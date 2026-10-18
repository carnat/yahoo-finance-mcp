"""Marketaux provider: the API constants and _collect_marketaux_events.

Adapter contract (unchanged by the move): each ``_collect_*_events`` keeps its current signature and
return value. The return value is the items plus the per-source status/diagnostics; warnings are
appended to the caller's ``warnings`` list. There is no provider registry yet (2.15.1 is a pure
restructure; the registry and the off switch are part 2).
"""

import asyncio
import datetime
import json
import os
import urllib.parse as _urlparse
import urllib.request as _urlrequest
import urllib.error as _urlerror

from yfmcp import url_policy as _url_policy
from yfmcp.util import _to_iso_utc
from yfmcp.clients.market_providers import _PROVIDER_USER_AGENT
from yfmcp.news.shared import (
    _event_type_from_keywords,
    _make_duplicate_group_id,
    _normalize_exchange,
    _short_text,
    _within_date_window,
)


_MARKETAUX_NEWS_API = "https://api.marketaux.com/v1/news/all"


_MARKETAUX_WIRE_DOMAINS = ("businesswire.com", "prnewswire.com", "globenewswire.com")


_MARKETAUX_MAX_ITEMS = 3


async def _collect_marketaux_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
    fallback_reason: str = "EXPLICIT_SOURCE_SELECTION",
    search_query: str = "",
    expected_exchange: str | None = None,
) -> tuple[list[dict], list[dict], bool, dict]:
    """Fetch one bounded Marketaux page from major wire domains.

    Marketaux is a contextual aggregator, not official evidence. The free-tier
    request is deliberately capped at three rows and never paginated.
    """
    warnings: list[dict] = []
    items: list[dict] = []
    limit = min(_MARKETAUX_MAX_ITEMS, max(1, int(max_results)))
    diagnostics = {
        "status": "PROVIDER_ERROR",
        "rawCount": 0,
        "retrievedCount": 0,
        "acceptedCount": 0,
        "collectedCount": 0,
        "rejectedCount": 0,
        "rejectionCounts": {},
        "attempted": True,
        "completed": False,
        "fallbackReason": fallback_reason,
        "domains": list(_MARKETAUX_WIRE_DOMAINS),
        "quotaPolicy": {
            "dailyLimit": 100,
            "maxArticlesPerRequest": _MARKETAUX_MAX_ITEMS,
            "pagesRequested": 1,
            "automaticPagination": False,
        },
    }

    def _reject(reason: str) -> None:
        diagnostics["rejectedCount"] += 1
        diagnostics["rejectionCounts"][reason] = diagnostics["rejectionCounts"].get(reason, 0) + 1

    api_token = os.environ.get("MARKETAUX_API_TOKEN") or os.environ.get("MARKETAUX_API_KEY")
    if not api_token:
        diagnostics.update({"status": "UNCONFIGURED", "attempted": False})
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Marketaux wire fallback is not configured; set MARKETAUX_API_TOKEN to enable it.",
            "severity": "warning",
            "source": "marketaux",
        })
        return items, warnings, False, diagnostics

    now = datetime.datetime.now(datetime.timezone.utc)
    from_day = start_date or (now - datetime.timedelta(days=lookback_days or 14)).strftime("%Y-%m-%d")
    to_day = end_date or now.strftime("%Y-%m-%d")
    safe_params = {
        "symbols": ticker.upper(),
        "filter_entities": "true",
        "language": "en",
        "domains": ",".join(_MARKETAUX_WIRE_DOMAINS),
        "published_after": f"{from_day}T00:00:00",
        "published_before": f"{to_day}T23:59:59",
        "limit": str(limit),
        "page": "1",
    }
    if str(search_query or "").strip():
        safe_params["search"] = str(search_query).strip()
    public_url = f"{_MARKETAUX_NEWS_API}?{_urlparse.urlencode({**safe_params, 'api_token': 'REDACTED'})}"
    request_url = f"{_MARKETAUX_NEWS_API}?{_urlparse.urlencode({**safe_params, 'api_token': api_token})}"
    diagnostics["providerUrl"] = public_url
    loop = asyncio.get_event_loop()

    def _fetch() -> dict:
        req = _urlrequest.Request(request_url, headers={"User-Agent": _PROVIDER_USER_AGENT})
        with _url_policy.urlopen(req, timeout=12) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    try:
        payload = await loop.run_in_executor(None, _url_policy.in_context(_fetch))
    except _urlerror.HTTPError as exc:
        status = "AUTH_ERROR" if exc.code in (401, 403) else "RATE_LIMITED" if exc.code in (402, 429) else "PROVIDER_ERROR"
        diagnostics["status"] = status
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": f"Marketaux wire fallback returned HTTP {exc.code} ({status}).",
            "severity": "warning",
            "source": "marketaux",
        })
        return items, warnings, False, diagnostics
    except Exception as exc:
        message = str(exc).replace(api_token, "REDACTED")
        status = "TIMEOUT" if any(token in message.lower() for token in ("timeout", "timed out")) else "PROVIDER_ERROR"
        diagnostics["status"] = status
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": f"Marketaux wire fallback unavailable ({status}).",
            "severity": "warning",
            "source": "marketaux",
        })
        return items, warnings, False, diagnostics

    raw_items = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(raw_items, list):
        diagnostics["status"] = "PROVIDER_CHANGED"
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Marketaux provider changed: expected a data array.",
            "severity": "warning",
            "source": "marketaux",
        })
        return items, warnings, False, diagnostics

    diagnostics["rawCount"] = len(raw_items)
    diagnostics["retrievedCount"] = len(raw_items)
    ticker_u = ticker.upper()
    for row in raw_items:
        if not isinstance(row, dict):
            _reject("INVALID_ROW")
            continue
        title = str(row.get("title") or "").strip()
        item_url = str(row.get("url") or "").strip()
        if not title:
            _reject("MISSING_HEADLINE")
            continue
        if not item_url.startswith("https://"):
            _reject("INVALID_URL")
            continue
        host = (_urlparse.urlparse(item_url).hostname or "").lower()
        if not any(host == domain or host.endswith(f".{domain}") for domain in _MARKETAUX_WIRE_DOMAINS):
            _reject("DOMAIN_NOT_ALLOWED")
            continue
        entities = row.get("entities") if isinstance(row.get("entities"), list) else []
        matched_entity = next((
            entity for entity in entities
            if isinstance(entity, dict) and str(entity.get("symbol") or "").upper() == ticker_u
        ), None)
        if matched_entity is None:
            _reject("ENTITY_SYMBOL_MISMATCH")
            continue
        expected_exchange_norm = _normalize_exchange(expected_exchange)
        provider_exchange_norm = _normalize_exchange(str(matched_entity.get("exchange") or ""))
        if expected_exchange_norm and provider_exchange_norm and expected_exchange_norm != provider_exchange_norm:
            _reject("ENTITY_EXCHANGE_MISMATCH")
            continue
        published_at = _to_iso_utc(row.get("published_at"))
        if not _within_date_window(published_at, start_date=start_date, end_date=end_date, lookback_days=lookback_days):
            _reject("DATE_OUTSIDE_WINDOW")
            continue
        summary = str(row.get("snippet") or row.get("description") or title).strip()
        duplicate_group_id = _make_duplicate_group_id(ticker_u, title, published_at, None, item_url)
        item = {
            "title": title,
            "source": "marketaux",
            "originalSource": str(row.get("source") or host).strip() or host,
            "sourceType": "marketaux_wire",
            "provider": "marketaux",
            "discoveredVia": "marketaux_api",
            "publishedAt": published_at,
            "retrievedAt": retrieved_at,
            "url": item_url,
            "issuer": str(matched_entity.get("name") or "").strip() or None,
            "tickers": [ticker_u],
            "matchBasis": "PROVIDER_ENTITY_SYMBOL",
            "sourceTickerMatch": True,
            "eventType": _event_type_from_keywords(f"{title} {summary}"),
            "summary": _short_text(summary, 240),
            "evidenceText": _short_text(summary, 180),
            "confidence": "LOW",
            "tickerRelevance": "HIGH",
            "duplicateGroupId": duplicate_group_id,
            "providerArticleId": str(row.get("uuid") or "").strip() or None,
            "providerRelevanceScore": matched_entity.get("match_score"),
        }
        diagnostics["acceptedCount"] += 1
        if len(items) < limit:
            items.append(item)

    diagnostics.update({
        "status": "OK" if diagnostics["acceptedCount"] else "EMPTY_RESULT",
        "collectedCount": len(items),
        "completed": True,
    })
    return items, warnings, True, diagnostics
