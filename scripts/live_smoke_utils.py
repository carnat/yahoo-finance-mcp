"""Shared helpers for deployed/live MCP smoke tests.

These helpers keep live smokes focused on contract checks while avoiding
time-sensitive fixtures such as expired option dates.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

RETRY_STATUSES = {429, 502, 503, 504}
OPAQUE_PLATFORM_TEXT = {"no approval received."}


def rpc(
    url: str,
    method: str,
    params: dict[str, Any] | None = None,
    *,
    req_id: int = 1,
    user_agent: str,
    timeout: int = 60,
    retries: int = 3,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        payload["params"] = params
    data = json.dumps(payload).encode("utf-8")
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": user_agent},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code in RETRY_STATUSES and attempt < retries:
                time.sleep(5 * (2 ** (attempt - 1)))
                continue
            raise
        except urllib.error.URLError:
            raise
    raise last_exc or RuntimeError("RPC failed")


def content_text(response: dict[str, Any]) -> str:
    return str(((((response.get("result") or {}).get("content") or [{}])[0]) or {}).get("text", ""))


def parse_tool_text(text: str, tool: str) -> Any:
    if text.strip().lower() in OPAQUE_PLATFORM_TEXT:
        raise AssertionError(f"{tool} returned opaque platform text instead of JSON: {text!r}")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"_raw": text}


def extract_data(payload: Any) -> Any:
    if isinstance(payload, dict) and "ok" in payload and "data" in payload:
        return payload.get("data")
    return payload


def is_error_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    return payload.get("ok") is False or payload.get("error") is not None


def call_tool(
    url: str,
    tool: str,
    args: dict[str, Any],
    *,
    req_id: int,
    user_agent: str,
    timeout: int = 60,
    retries: int = 3,
) -> dict[str, Any]:
    response = rpc(
        url,
        "tools/call",
        {"name": tool, "arguments": args},
        req_id=req_id,
        user_agent=user_agent,
        timeout=timeout,
        retries=retries,
    )
    if response.get("error") is not None:
        return response
    parsed = parse_tool_text(content_text(response), tool)
    return parsed if isinstance(parsed, dict) else {"data": parsed}


def choose_stable_option_expiration(expirations: Any) -> str | None:
    if not isinstance(expirations, list):
        return None
    candidates = [item for item in expirations if isinstance(item, str) and item]
    if not candidates:
        return None
    # Prefer the second expiry when available; first can be same-day and vanish
    # during a deploy window.
    return candidates[1] if len(candidates) > 1 else candidates[0]


def resolve_option_expiration(
    url: str,
    ticker: str,
    *,
    user_agent: str,
    req_id: int = 900,
    timeout: int = 60,
    retries: int = 3,
) -> str | None:
    payload = call_tool(
        url,
        "get_option_expiration_dates",
        {"ticker": ticker},
        req_id=req_id,
        user_agent=user_agent,
        timeout=timeout,
        retries=retries,
    )
    if is_error_payload(payload):
        return None
    return choose_stable_option_expiration(extract_data(payload))
