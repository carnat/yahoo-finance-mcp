"""MCP response envelope helpers.

Extracted from server.py in Phase 1 of the refactoring plan.
server.py re-imports all public names from this module so that
``server._mcp_success``, ``server.ErrorCode``, etc. keep working.
"""

import datetime
import json
import os
from typing import TypedDict

# ---------------------------------------------------------------------------
# Server version and envelope feature flag
# ---------------------------------------------------------------------------
SERVER_VERSION = "0.2.0"
BUILD_DATE = "2026-06-14"  # date of this release; update on each deploy
_ENVELOPE_V2 = os.environ.get("MCP_ENVELOPE_V2", "").lower() == "true"


# ---------------------------------------------------------------------------
# Typed domain error codes
# ---------------------------------------------------------------------------
class ErrorCode:
    TICKER_NOT_FOUND = "TICKER_NOT_FOUND"
    NO_OPTIONS_DATA = "NO_OPTIONS_DATA"
    NO_FILING_DATA = "NO_FILING_DATA"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    PROVIDER_TIMEOUT = "PROVIDER_TIMEOUT"
    RATE_LIMIT = "RATE_LIMIT"
    INPUT_VALIDATION_ERROR = "INPUT_VALIDATION_ERROR"
    DEPRECATED_TOOL = "DEPRECATED_TOOL"


# ---------------------------------------------------------------------------
# McpResponse TypedDicts
# ---------------------------------------------------------------------------
class ToolMeta(TypedDict):
    tool: str
    canonicalTool: str | None
    deprecatedTool: bool
    useInstead: str
    source: str
    dataDate: str | None
    serverVersion: str
    cacheHit: bool
    warnings: list[object]


class ErrorDetail(TypedDict):
    code: str
    message: str


class McpResponse(TypedDict):
    ok: bool
    data: object
    meta: ToolMeta
    error: ErrorDetail | None


# ---------------------------------------------------------------------------
# McpResponse helpers
# ---------------------------------------------------------------------------
def _mcp_success(
    tool: str,
    data: object,
    *,
    canonical_tool: str | None = None,
    deprecated_tool: bool | None = None,
    use_instead: str | None = None,
    source: str = "yahoo_finance",
    data_date: str | None = None,
    cache_hit: bool = False,
    warnings: list[object] | None = None,
) -> str:
    if not _ENVELOPE_V2:
        return data if isinstance(data, str) else json.dumps(data)
    return json.dumps({
        "ok": True,
        "data": data if not isinstance(data, str) else json.loads(data),
        "meta": {
            "tool": tool,
            **({"canonicalTool": canonical_tool} if canonical_tool is not None else {}),
            **({"deprecatedTool": deprecated_tool} if deprecated_tool is not None else {}),
            **({"useInstead": use_instead} if use_instead is not None else {}),
            "source": source,
            "dataDate": data_date,
            "serverVersion": SERVER_VERSION,
            "cacheHit": cache_hit,
            "warnings": warnings or [],
        },
        "error": None,
    })


def _mcp_failure(
    tool: str,
    code: str,
    message: str,
    *,
    source: str = "yahoo_finance",
    data_date: str | None = None,
) -> str:
    payload = {
        "ok": False,
        "data": None,
        "meta": {
            "tool": tool,
            "source": source,
            "dataDate": data_date,
            "serverVersion": SERVER_VERSION,
            "cacheHit": False,
            "warnings": [],
        },
        "error": {"code": code, "message": message},
    }
    if not _ENVELOPE_V2:
        return json.dumps({"error": True, "code": code, "message": message})
    return json.dumps(payload)


def _mcp_warning(
    tool: str,
    data: object,
    message: str,
    *,
    canonical_tool: str | None = None,
    source: str = "yahoo_finance",
    data_date: str | None = None,
) -> str:
    if not _ENVELOPE_V2:
        return data if isinstance(data, str) else json.dumps(data)
    parsed = data if not isinstance(data, str) else json.loads(data)
    return json.dumps({
        "ok": True,
        "data": parsed,
        "meta": {
            "tool": tool,
            **({"canonicalTool": canonical_tool} if canonical_tool is not None else {}),
            "source": source,
            "dataDate": data_date,
            "serverVersion": SERVER_VERSION,
            "cacheHit": False,
            "warnings": [message],
        },
        "error": None,
    })


# ---------------------------------------------------------------------------
# Envelope V2 standardization helper
# ---------------------------------------------------------------------------
def _wrap_envelope_v2(
    tool_name: str,
    data: dict | list | None,
    *,
    warnings: list[dict] | None = None,
    error: str | None = None,
    error_code: str | None = None,
    meta_extra: dict | None = None,
) -> str:
    """
    Returns a JSON-encoded MCP Envelope V2 string.

    Shape:
    {
      "ok": true | false,
      "data": <payload>,          # present when ok=true
      "error": "<message>",       # present when ok=false
      "errorCode": "<CODE>",      # present when ok=false
      "meta": {
        "tool": "<tool_name>",
        "generatedAt": "<ISO-UTC>",
        "warnings": [ { "code": str, "message": str } ]
      }
    }
    """
    generated_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    meta: dict = {
        "tool": tool_name,
        "generatedAt": generated_at,
        "warnings": warnings or [],
    }
    if meta_extra:
        meta.update(meta_extra)

    if error is not None:
        return json.dumps({
            "ok": False,
            "error": error,
            "errorCode": error_code or "UNKNOWN_ERROR",
            "data": None,
            "meta": meta,
        })

    return json.dumps({
        "ok": True,
        "data": data,
        "error": None,
        "errorCode": None,
        "meta": meta,
    })
