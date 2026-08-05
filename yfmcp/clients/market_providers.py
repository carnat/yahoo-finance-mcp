"""Bounded JSON clients for optional market-data providers.

The helpers centralize credential redaction, provider-status mapping, and
process-local caching.  Callers still own response-shape validation and the
decision about whether a scarce provider request is justified.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from yfmcp.cache import _tool_cache


_ALPHA_URL = "https://www.alphavantage.co/query"
_FINNHUB_URL = "https://finnhub.io/api/v1"
_USER_AGENT = "yahoo-finance-mcp/1.0"


@dataclass(frozen=True)
class ProviderJsonResult:
    payload: object | None
    status: str
    public_url: str
    provider_attempted: bool
    cache_status: str
    fetched_at: str | None = None
    http_status: int | None = None
    message: str | None = None
    retry_after: str | None = None


def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _cache_key(provider: str, operation: str, params: dict[str, object]) -> str:
    stable = urllib.parse.urlencode(sorted((key, str(value)) for key, value in params.items()))
    return f"provider_json:{provider}:{operation}:{stable}"


def _cached_result(key: str) -> ProviderJsonResult | None:
    cached = _tool_cache.get(key)
    if cached is None:
        return None
    raw, _, cached_at = cached
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(value, dict) or "payload" not in value or "publicUrl" not in value:
        return None
    return ProviderJsonResult(
        payload=value.get("payload"),
        status="OK",
        public_url=str(value["publicUrl"]),
        provider_attempted=False,
        cache_status="HIT_PROCESS",
        fetched_at=str(value.get("fetchedAt") or cached_at or "") or None,
    )


def _store_result(
    key: str,
    *,
    payload: object,
    public_url: str,
    fetched_at: str,
    ttl_seconds: int,
) -> None:
    _tool_cache.set(
        key,
        json.dumps({"payload": payload, "publicUrl": public_url, "fetchedAt": fetched_at}),
        ttl_seconds,
    )


def _message_status(message: str) -> str:
    normalized = message.lower()
    if re.search(r"rate limit|frequency|call volume|standard api rate|too many requests", normalized):
        return "RATE_LIMIT"
    if re.search(r"premium|subscription|subscribe|entitlement|not available under your current plan|access to this resource", normalized):
        return "ENTITLEMENT_REQUIRED"
    if re.search(r"api key|apikey|authentication|unauthorized|invalid token|forbidden", normalized):
        return "AUTH_ERROR"
    return "PROVIDER_ERROR"


def _http_status(code: int, message: str) -> str:
    if code == 429:
        return "RATE_LIMIT"
    if code == 401:
        return "AUTH_ERROR"
    if code == 403:
        mapped = _message_status(message)
        return mapped if mapped == "ENTITLEMENT_REQUIRED" else "AUTH_ERROR"
    if code == 404:
        return "NOT_FOUND"
    return "PROVIDER_ERROR"


async def _request_json(
    url: str,
    *,
    headers: dict[str, str],
    public_url: str,
    secret: str,
    timeout_seconds: int,
) -> ProviderJsonResult:
    loop = asyncio.get_running_loop()

    def _fetch() -> tuple[object | None, int | None, str | None, str | None]:
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
                raw = response.read(5_000_000)
            return json.loads(raw.decode("utf-8", errors="replace")), None, None, None
        except urllib.error.HTTPError as error:
            try:
                raw = error.read(1_000_000)
                body = raw.decode("utf-8", errors="replace")
                parsed = json.loads(body)
                message = str(
                    parsed.get("error")
                    or parsed.get("message")
                    or parsed.get("Information")
                    or parsed.get("Note")
                    or body
                ) if isinstance(parsed, dict) else body
            except Exception:
                message = str(error)
            retry_after = error.headers.get("Retry-After") if error.headers else None
            return None, error.code, message, retry_after
        except (TimeoutError, socket.timeout):
            return None, -1, "Provider request timed out.", None
        except urllib.error.URLError as error:
            if isinstance(error.reason, (TimeoutError, socket.timeout)):
                return None, -1, "Provider request timed out.", None
            return None, None, str(error.reason), None
        except Exception as error:  # pragma: no cover - defensive boundary
            return None, None, str(error), None

    payload, http_code, message, retry_after = await loop.run_in_executor(None, _fetch)
    safe_message = (message or "").replace(secret, "REDACTED") or None
    if http_code == -1:
        return ProviderJsonResult(None, "TIMEOUT", public_url, True, "MISS", message=safe_message)
    if http_code is not None:
        return ProviderJsonResult(
            None,
            _http_status(http_code, safe_message or ""),
            public_url,
            True,
            "MISS",
            http_status=http_code,
            message=safe_message,
            retry_after=retry_after,
        )
    if payload is None:
        return ProviderJsonResult(None, "PROVIDER_ERROR", public_url, True, "MISS", message=safe_message)
    return ProviderJsonResult(payload, "OK", public_url, True, "MISS", fetched_at=_utc_now_iso())


async def fetch_alpha_vantage_json(
    function: str,
    params: dict[str, object],
    *,
    ttl_seconds: int,
    timeout_seconds: int = 30,
) -> ProviderJsonResult:
    """Fetch one Alpha Vantage JSON payload, with no retry."""
    safe_params = {"function": function, **params}
    public_query = urllib.parse.urlencode({**safe_params, "apikey": "REDACTED"})
    public_url = f"{_ALPHA_URL}?{public_query}"
    key = _cache_key("alpha_vantage", function, safe_params)
    cached = _cached_result(key)
    if cached is not None:
        return cached

    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY") or os.environ.get("ALPHAVANTAGE_API_KEY")
    if not api_key:
        return ProviderJsonResult(None, "SOURCE_UNCONFIGURED", public_url, False, "MISS")
    query = urllib.parse.urlencode({**safe_params, "apikey": api_key})
    result = await _request_json(
        f"{_ALPHA_URL}?{query}",
        headers={"User-Agent": _USER_AGENT},
        public_url=public_url,
        secret=api_key,
        timeout_seconds=timeout_seconds,
    )
    if result.status != "OK" or not isinstance(result.payload, dict):
        return result if result.status != "OK" else ProviderJsonResult(
            None, "PROVIDER_ERROR", public_url, result.provider_attempted, result.cache_status,
            message="Alpha Vantage returned a non-object payload.",
        )

    provider_message = str(
        result.payload.get("Note")
        or result.payload.get("Information")
        or result.payload.get("Error Message")
        or ""
    ).replace(api_key, "REDACTED")
    if provider_message:
        return ProviderJsonResult(
            None,
            _message_status(provider_message),
            public_url,
            result.provider_attempted,
            result.cache_status,
            message=provider_message,
        )
    fetched_at = result.fetched_at or _utc_now_iso()
    _store_result(
        key,
        payload=result.payload,
        public_url=public_url,
        fetched_at=fetched_at,
        ttl_seconds=ttl_seconds,
    )
    return ProviderJsonResult(
        result.payload, "OK", public_url, result.provider_attempted, result.cache_status,
        fetched_at=fetched_at,
    )


async def fetch_finnhub_json(
    path: str,
    params: dict[str, object],
    *,
    ttl_seconds: int,
    timeout_seconds: int = 20,
) -> ProviderJsonResult:
    """Fetch one Finnhub JSON payload through header authentication, with no retry."""
    clean_path = "/" + path.lstrip("/")
    public_query = urllib.parse.urlencode(params)
    public_url = f"{_FINNHUB_URL}{clean_path}?{public_query}"
    key = _cache_key("finnhub", clean_path, params)
    cached = _cached_result(key)
    if cached is not None:
        return cached

    token = os.environ.get("FINNHUB_API_KEY") or os.environ.get("FINNHUB_TOKEN")
    if not token:
        return ProviderJsonResult(None, "SOURCE_UNCONFIGURED", public_url, False, "MISS")
    result = await _request_json(
        public_url,
        headers={"User-Agent": _USER_AGENT, "X-Finnhub-Token": token},
        public_url=public_url,
        secret=token,
        timeout_seconds=timeout_seconds,
    )
    if result.status != "OK":
        return result
    fetched_at = result.fetched_at or _utc_now_iso()
    _store_result(
        key,
        payload=result.payload,
        public_url=public_url,
        fetched_at=fetched_at,
        ttl_seconds=ttl_seconds,
    )
    return ProviderJsonResult(
        result.payload, "OK", public_url, result.provider_attempted, result.cache_status,
        fetched_at=fetched_at,
    )
