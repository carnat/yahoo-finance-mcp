"""Bounded Yahoo/Quartr earnings-transcript client.

Yahoo renders transcript pages client-side.  The human page contains an event
identifier while the structured transcript is returned by Yahoo's internal
``/xhr/transcript`` endpoint.  This module keeps that undocumented integration
isolated, validates caller-provided page URLs, and exposes recoverable provider
statuses rather than leaking transport exceptions into MCP responses.
"""

from __future__ import annotations

import asyncio
import datetime
import http.cookiejar
import json
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

from yfmcp.cache import _tool_cache


_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
_QUOTE_TYPE_URL = "https://query1.finance.yahoo.com/v1/finance/quoteType/{ticker}"
_TRANSCRIPT_URL = "https://finance.yahoo.com/xhr/transcript"
_TRANSCRIPT_TTL_SECONDS = 30 * 24 * 60 * 60
_MAX_RESPONSE_BYTES = 10_000_000
_SOURCE_PATH_RE = re.compile(
    r"^/quote/([^/]+)/earnings/([^/]+)-Q([1-4])-(20\d{2})-earnings_call-(\d+)\.html/?$",
    flags=re.IGNORECASE,
)
_DISCOVERY_RE = re.compile(
    r"(?:https://finance\.yahoo\.com)?/quote/([^/\"'?]+)/earnings/"
    r"([^/\"'?]+)-Q([1-4])-(20\d{2})-earnings_call-(\d+)\.html",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class YahooTranscriptResult:
    payload: dict | None
    status: str
    source_url: str | None
    event_id: str | None
    provider_attempted: bool
    cache_status: str
    fetched_at: str | None = None
    http_status: int | None = None
    message: str | None = None
    retry_after: str | None = None


class _YahooHttpError(Exception):
    def __init__(self, status: int | None, message: str, retry_after: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _canonical_source_url(ticker: str, quarter: str, year: int, event_id: str) -> str:
    ticker_u = ticker.upper()
    return (
        f"https://finance.yahoo.com/quote/{urllib.parse.quote(ticker_u, safe='.^=-')}/earnings/"
        f"{urllib.parse.quote(ticker_u, safe='.^=-')}-{quarter.upper()}-{year}-earnings_call-{event_id}.html"
    )


def parse_yahoo_transcript_source_url(ticker: str, source_url: str) -> tuple[dict | None, str | None]:
    """Validate a Yahoo transcript page and extract its identity."""
    try:
        parsed = urllib.parse.urlparse(str(source_url or "").strip())
        port = parsed.port
    except ValueError:
        return None, "source_url is malformed"
    if parsed.scheme.lower() != "https" or (parsed.hostname or "").lower() != "finance.yahoo.com":
        return None, "source_url must be an https://finance.yahoo.com earnings-call URL"
    if parsed.username or parsed.password or port not in (None, 443):
        return None, "source_url must not contain credentials or a non-standard port"
    match = _SOURCE_PATH_RE.fullmatch(urllib.parse.unquote(parsed.path))
    if not match:
        return None, "source_url must match Yahoo's /quote/{ticker}/earnings/...-earnings_call-{eventId}.html format"
    requested = ticker.upper()
    path_ticker, slug_ticker, quarter_number, year_text, event_id = match.groups()
    if path_ticker.upper() != requested or slug_ticker.upper() != requested:
        return None, "source_url ticker does not match the requested ticker"
    year = int(year_text)
    quarter = f"Q{quarter_number}"
    return {
        "eventId": event_id,
        "fiscalQuarter": f"{year}Q{quarter_number}",
        "sourceUrl": _canonical_source_url(requested, quarter, year, event_id),
    }, None


def discover_yahoo_transcript_event(
    ticker: str,
    html: str,
    fiscal_quarter: str | None = None,
) -> dict | None:
    """Extract the newest matching Yahoo earnings-call URL from bounded HTML."""
    ticker_u = ticker.upper()
    requested = str(fiscal_quarter or "").upper()
    seen: set[str] = set()
    calls: list[dict] = []
    for match in _DISCOVERY_RE.finditer(html or ""):
        path_ticker, slug_ticker, quarter_number, year_text, event_id = match.groups()
        if path_ticker.upper() != ticker_u or slug_ticker.upper() != ticker_u or event_id in seen:
            continue
        seen.add(event_id)
        year = int(year_text)
        quarter = f"Q{quarter_number}"
        calls.append({
            "eventId": event_id,
            "fiscalQuarter": f"{year}Q{quarter_number}",
            "sourceUrl": _canonical_source_url(ticker_u, quarter, year, event_id),
            "year": year,
            "quarterNumber": int(quarter_number),
        })
    calls.sort(key=lambda item: (item["year"], item["quarterNumber"]), reverse=True)
    if requested:
        return next((item for item in calls if item["fiscalQuarter"] == requested), None)
    return calls[0] if calls else None


def _cache_key(ticker: str, event_id: str) -> str:
    return f"yahoo_quartr_transcript:{ticker.upper()}:{event_id}:en-US:US"


def validate_yahoo_transcript_payload_identity(
    payload: dict,
    expected_event_id: str,
    expected_fiscal_quarter: str | None = None,
) -> str | None:
    """Return a recoverable error when Yahoo metadata disagrees with the request."""
    metadata = payload.get("transcriptMetadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    content = payload.get("transcriptContent")
    content = content if isinstance(content, dict) else {}

    returned_event_ids = {
        str(value)
        for value in (metadata.get("eventId"), content.get("event_id"), content.get("eventId"))
        if value not in (None, "")
    }
    expected_event = str(expected_event_id)
    if not returned_event_ids:
        return f"Yahoo transcript metadata omitted the expected event ID {expected_event}."
    if returned_event_ids != {expected_event}:
        returned = ", ".join(sorted(returned_event_ids))
        return f"Yahoo returned event ID {returned}; expected {expected_event}."

    expected_quarter = str(expected_fiscal_quarter or "").strip().upper()
    if not expected_quarter:
        return None
    fiscal_period = str(metadata.get("fiscalPeriod") or "").strip().upper()
    fiscal_year = str(metadata.get("fiscalYear") or "").strip()
    returned_quarter = (
        f"{fiscal_year}{fiscal_period}"
        if re.fullmatch(r"20\d{2}", fiscal_year) and re.fullmatch(r"Q[1-4]", fiscal_period)
        else None
    )
    if returned_quarter != expected_quarter:
        return f"Yahoo returned fiscal quarter {returned_quarter or 'UNKNOWN'}; expected {expected_quarter}."
    return None


def _cached_payload(ticker: str, event_id: str) -> tuple[dict, str | None] | None:
    cached = _tool_cache.get(_cache_key(ticker, event_id))
    if cached is None:
        return None
    raw, _, cached_at = cached
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return (payload, cached_at) if isinstance(payload, dict) else None


def _store_payload(ticker: str, event_id: str, payload: dict) -> None:
    _tool_cache.set(
        _cache_key(ticker, event_id),
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
        _TRANSCRIPT_TTL_SECONDS,
    )


def _discard_cached_payload(ticker: str, event_id: str) -> None:
    _tool_cache._store.pop(_cache_key(ticker, event_id), None)


def _status_from_http(status: int | None) -> str:
    if status == 429:
        return "RATE_LIMIT"
    if status in (401, 403):
        return "AUTH_ERROR"
    if status == 404:
        return "NOT_FOUND"
    return "PROVIDER_ERROR"


def _response_bytes(opener: urllib.request.OpenerDirector, url: str, timeout: int = 20) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": _USER_AGENT,
            "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with opener.open(request, timeout=timeout) as response:  # noqa: S310
            content_length = response.headers.get("Content-Length")
            if content_length and int(content_length) > _MAX_RESPONSE_BYTES:
                raise _YahooHttpError(None, "Yahoo response exceeded the configured size limit.")
            data = response.read(_MAX_RESPONSE_BYTES + 1)
            if len(data) > _MAX_RESPONSE_BYTES:
                raise _YahooHttpError(None, "Yahoo response exceeded the configured size limit.")
            return data
    except urllib.error.HTTPError as error:
        retry_after = error.headers.get("Retry-After") if error.headers else None
        error.close()
        raise _YahooHttpError(error.code, f"Yahoo returned HTTP {error.code}.", retry_after) from error


def _blocking_fetch(
    ticker: str,
    event_id: str | None,
    source_url: str | None,
    fiscal_quarter: str | None,
) -> YahooTranscriptResult:
    ticker_u = ticker.upper()
    resolved: dict | None = None
    if source_url:
        resolved, error = parse_yahoo_transcript_source_url(ticker_u, source_url)
        if error:
            return YahooTranscriptResult(None, "INVALID_SOURCE_URL", source_url, event_id, False, "MISS", message=error)
    if event_id and resolved and event_id != resolved["eventId"]:
        return YahooTranscriptResult(
            None,
            "INVALID_SOURCE_URL",
            resolved["sourceUrl"],
            event_id,
            False,
            "MISS",
            message="event_id does not match source_url",
        )
    resolved_event_id = event_id or (str(resolved["eventId"]) if resolved else None)
    resolved_source_url = str(resolved["sourceUrl"]) if resolved else None
    expected_fiscal_quarter = str(fiscal_quarter or "").strip().upper() or None
    if resolved:
        expected_fiscal_quarter = expected_fiscal_quarter or str(resolved["fiscalQuarter"])
    if resolved_event_id:
        cached = _cached_payload(ticker_u, resolved_event_id)
        if cached:
            payload, cached_at = cached
            identity_error = validate_yahoo_transcript_payload_identity(
                payload, resolved_event_id, expected_fiscal_quarter
            )
            if not identity_error:
                return YahooTranscriptResult(
                    payload, "OK", resolved_source_url, resolved_event_id, False, "HIT_PROCESS", fetched_at=cached_at
                )
            _discard_cached_payload(ticker_u, resolved_event_id)

    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    try:
        # Yahoo commonly responds non-2xx at this bootstrap host while still
        # setting the session cookie.  The cookie jar processes that response.
        try:
            _response_bytes(opener, "https://fc.yahoo.com", timeout=10)
        except _YahooHttpError:
            pass
        crumb = _response_bytes(opener, "https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10).decode(
            "utf-8", errors="replace"
        ).strip()
        if not crumb:
            raise _YahooHttpError(None, "Yahoo did not return a session crumb.")

        if not resolved_event_id:
            quote_page = f"https://finance.yahoo.com/quote/{urllib.parse.quote(ticker_u, safe='.^=-')}"
            html = _response_bytes(opener, quote_page, timeout=20).decode("utf-8", errors="replace")
            discovered = discover_yahoo_transcript_event(ticker_u, html, fiscal_quarter)
            if not discovered:
                return YahooTranscriptResult(
                    None,
                    "NOT_FOUND",
                    None,
                    None,
                    True,
                    "MISS",
                    message=(
                        f"Yahoo did not expose an earnings-call page for {fiscal_quarter}."
                        if fiscal_quarter
                        else "Yahoo did not expose an earnings-call page for this ticker."
                    ),
                )
            resolved_event_id = str(discovered["eventId"])
            resolved_source_url = str(discovered["sourceUrl"])
            expected_fiscal_quarter = expected_fiscal_quarter or str(discovered["fiscalQuarter"])
            cached = _cached_payload(ticker_u, resolved_event_id)
            if cached:
                payload, cached_at = cached
                identity_error = validate_yahoo_transcript_payload_identity(
                    payload, resolved_event_id, expected_fiscal_quarter
                )
                if not identity_error:
                    return YahooTranscriptResult(
                        payload, "OK", resolved_source_url, resolved_event_id, False, "HIT_PROCESS", fetched_at=cached_at
                    )
                _discard_cached_payload(ticker_u, resolved_event_id)

        quote_query = urllib.parse.urlencode({"crumb": crumb, "lang": "en-US", "region": "US"})
        quote_url = f"{_QUOTE_TYPE_URL.format(ticker=urllib.parse.quote(ticker_u, safe='.^=-'))}?{quote_query}"
        quote_payload = json.loads(_response_bytes(opener, quote_url).decode("utf-8", errors="replace"))
        quote_results = quote_payload.get("quoteType", {}).get("result", []) if isinstance(quote_payload, dict) else []
        quartr_id = quote_results[0].get("quartrId") if quote_results and isinstance(quote_results[0], dict) else None
        if quartr_id in (None, ""):
            return YahooTranscriptResult(
                None,
                "NOT_FOUND",
                resolved_source_url,
                resolved_event_id,
                True,
                "MISS",
                message="Yahoo quote metadata did not expose a Quartr company identifier.",
            )

        transcript_query = urllib.parse.urlencode({
            "eventType": "earnings_call",
            "quartrId": str(quartr_id),
            "eventId": str(resolved_event_id),
            "lang": "en-US",
            "region": "US",
            "crumb": crumb,
        })
        payload = json.loads(
            _response_bytes(opener, f"{_TRANSCRIPT_URL}?{transcript_query}", timeout=30).decode(
                "utf-8", errors="replace"
            )
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("transcriptContent"), dict):
            return YahooTranscriptResult(
                None,
                "NOT_FOUND",
                resolved_source_url,
                resolved_event_id,
                True,
                "MISS",
                message="Yahoo returned no structured transcript content for this event.",
            )
        identity_error = validate_yahoo_transcript_payload_identity(
            payload, str(resolved_event_id), expected_fiscal_quarter
        )
        if identity_error:
            return YahooTranscriptResult(
                None,
                "YAHOO_METADATA_MISMATCH",
                resolved_source_url,
                str(resolved_event_id),
                True,
                "MISS",
                message=identity_error,
            )
        fetched_at = _utc_now_iso()
        _store_payload(ticker_u, str(resolved_event_id), payload)
        return YahooTranscriptResult(
            payload,
            "OK",
            resolved_source_url,
            str(resolved_event_id),
            True,
            "MISS",
            fetched_at=fetched_at,
        )
    except _YahooHttpError as error:
        return YahooTranscriptResult(
            None,
            _status_from_http(error.status),
            resolved_source_url,
            resolved_event_id,
            True,
            "MISS",
            http_status=error.status,
            message=str(error),
            retry_after=error.retry_after,
        )
    except (TimeoutError, socket.timeout):
        return YahooTranscriptResult(
            None, "TIMEOUT", resolved_source_url, resolved_event_id, True, "MISS", message="Yahoo request timed out."
        )
    except urllib.error.URLError as error:
        if isinstance(error.reason, (TimeoutError, socket.timeout)):
            return YahooTranscriptResult(
                None, "TIMEOUT", resolved_source_url, resolved_event_id, True, "MISS", message="Yahoo request timed out."
            )
        return YahooTranscriptResult(
            None, "PROVIDER_ERROR", resolved_source_url, resolved_event_id, True, "MISS", message=str(error.reason)
        )
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
        return YahooTranscriptResult(
            None,
            "PROVIDER_ERROR",
            resolved_source_url,
            resolved_event_id,
            True,
            "MISS",
            message=f"Yahoo returned an invalid transcript response: {error}",
        )
    except Exception as error:  # pragma: no cover - defensive provider boundary
        return YahooTranscriptResult(
            None, "PROVIDER_ERROR", resolved_source_url, resolved_event_id, True, "MISS", message=str(error)
        )


async def fetch_yahoo_quartr_transcript(
    ticker: str,
    *,
    event_id: str | None = None,
    source_url: str | None = None,
    fiscal_quarter: str | None = None,
) -> YahooTranscriptResult:
    """Fetch one structured Yahoo/Quartr transcript with no retry loop."""
    return await asyncio.get_running_loop().run_in_executor(
        None,
        _blocking_fetch,
        ticker,
        event_id,
        source_url,
        fiscal_quarter,
    )
