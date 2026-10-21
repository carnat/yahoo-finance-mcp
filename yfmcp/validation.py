"""Input validation helpers for ticker symbols, accession numbers, and SEC URLs.

Extracted from server.py in Phase 1 of the refactoring plan.
"""

import json as _json
import re as _re
from pathlib import Path as _Path


# ---------------------------------------------------------------------------
# Input validation helpers
# ---------------------------------------------------------------------------
_TICKER_RE = _re.compile(r'^[A-Z0-9.\-\^=]{1,20}$')
# ASCII digits only (2.12.0): `\d` would also accept other Unicode decimal digits.
_ACCESSION_RE = _re.compile(r'^[0-9]{10}-[0-9]{2}-[0-9]{6}$')


def _validate_ticker(ticker: str) -> str | None:
    """Returns an error message if the ticker is invalid, else None."""
    t = ticker.strip().upper()
    if not _TICKER_RE.match(t):
        return f"Invalid ticker symbol: '{ticker}'. Must be 1-20 characters: uppercase letters, digits, or . - ^ ="
    return None


def _validate_accession(acc: str) -> str | None:
    """Returns an error message if the accession number is invalid, else None."""
    if not _ACCESSION_RE.match(acc.strip()):
        return f"Invalid accession number: '{acc}'. Expected format: XXXXXXXXXX-YY-ZZZZZZ."
    return None


# ---------------------------------------------------------------------------
# Tool-call gate (2.12.0, Q3b/Q3c): mirrors `validateToolArgs` in worker/src/tools.ts
# ---------------------------------------------------------------------------
# One check at the tool-call boundary (yfmcp.app wrapper for expanded mode, tool_groups router for grouped
# mode), before any handler code or fetch. Absent or null arguments, non-string values and empty or
# whitespace-only ticker values (and an empty accession) are skipped and left to the handler.
_CATALOG = _json.loads((_Path(__file__).resolve().parents[1] / "tool_catalog.json").read_text(encoding="utf-8"))
TICKER_CAPS: dict[str, int] = dict(_CATALOG.get("tickerCaps") or {})
_PUBLIC_ACTIONS = frozenset(
    action for group in _CATALOG["groups"].values() for action in group["actions"]
) | {"health_check"}

_TICKER_ARGS = ("ticker", "tickers", "subject", "peers")
_ACCESSION_ARGS = ("accession_number", "accessionNumber")
# get_company_news sources that read one issuer's own pages: one ticker per call.
_SINGLE_TICKER_NEWS_SOURCES = frozenset({"company_ir", "company_ir_page", "newswire"})
_SINGLE_TICKER_NEWS_CAP = 1


def ticker_cap_for(name: str, args: dict) -> int | None:
    """The tickerCaps entry that applies to this call, or None when the action is uncapped."""
    sources = args.get("sources")
    if name == "get_company_news" and isinstance(sources, list) and any(
        str(source).lower().strip() in _SINGLE_TICKER_NEWS_SOURCES for source in sources
    ):
        return _SINGLE_TICKER_NEWS_CAP
    # Mirrors the handler: only mode "full" selects full mode.
    mode = args.get("mode")
    full = name == "get_market_snapshot" and (mode if isinstance(mode, str) else "compact") == "full"
    return TICKER_CAPS.get("get_market_snapshot.full" if full else name)


def ticker_cap_failure(name: str, args: dict) -> str | None:
    """BATCH_LIMIT_EXCEEDED when the ticker array is longer than the action's cap."""
    ticker = args.get("ticker")
    if not isinstance(ticker, list):
        return None
    cap = ticker_cap_for(name, args)
    if cap is None or len(ticker) <= cap:
        return None
    from yfmcp.envelope import ErrorCode, _mcp_failure

    return _mcp_failure(
        name,
        ErrorCode.BATCH_LIMIT_EXCEEDED,
        f"Too many tickers: {len(ticker)}. Maximum is {cap} per call for {name}.",
        meta_extra={"retryable": False},
    )


# Earnings tools whose schema allows only period "latest": another value was silently read as the latest release
# (2.25.1, F28). The Worker checks this in _dispatchTool (LATEST_ONLY_PERIOD_TOOLS).
LATEST_ONLY_PERIOD_TOOLS = frozenset({
    "get_latest_earnings_release",
    "index_earnings_release",
    "extract_earnings_metrics",
    "extract_guidance",
    "extract_management_commentary",
    "compare_earnings_actual_vs_estimate",
    "get_earnings_call_transcript",
})


def latest_only_period_failure(name: str, args: dict) -> str | None:
    """INPUT_VALIDATION_ERROR when a latest-only earnings tool is given a period other than "latest"."""
    if name not in LATEST_ONLY_PERIOD_TOOLS or not isinstance(args, dict):
        return None
    period = args.get("period")
    if period is None:
        return None
    if isinstance(period, str) and _re.fullmatch(r"\s*latest\s*", period, _re.IGNORECASE):
        return None
    from yfmcp.envelope import ErrorCode, _mcp_failure
    from yfmcp.sec_facts import _js_string

    return _mcp_failure(
        name,
        ErrorCode.INPUT_VALIDATION_ERROR,
        f"period supports only \"latest\" for {name}; it reads the latest earnings release. Got '{_js_string(period)}'.",
    )


def validate_tool_args(name: str, args: dict) -> str | None:
    """INPUT_VALIDATION_ERROR (echoing the caller's raw value) or BATCH_LIMIT_EXCEEDED as failure-envelope
    JSON, else None. An unknown tool name is left to the dispatcher."""
    if name not in _PUBLIC_ACTIONS or not isinstance(args, dict):
        return None
    cap_failure = ticker_cap_failure(name, args)
    if cap_failure:
        return cap_failure
    from yfmcp.envelope import ErrorCode, _mcp_failure

    for arg in _TICKER_ARGS:
        value = args.get(arg)
        for item in (value if isinstance(value, list) else [value]):
            # Empty or whitespace-only values read as absent: the handler keeps its own empty-value behaviour.
            if not isinstance(item, str) or not item.strip():
                continue
            error = _validate_ticker(item)
            if error:
                return _mcp_failure(name, ErrorCode.INPUT_VALIDATION_ERROR, error)
    for arg in _ACCESSION_ARGS:
        value = args.get(arg)
        # An empty accession reads as absent, as the `if accession_number:` call sites do.
        if not isinstance(value, str) or value == "":
            continue
        error = _validate_accession(value)
        if error:
            return _mcp_failure(name, ErrorCode.INPUT_VALIDATION_ERROR, error)
    return latest_only_period_failure(name, args)


def _validate_sec_url(url: str) -> str | None:
    """Returns an error message if the SEC URL is not from sec.gov/Archives, else None."""
    if not url.startswith("https://www.sec.gov/Archives/"):
        return f"Invalid SEC URL: must start with 'https://www.sec.gov/Archives/'."
    return None


def _sanitize_sec_html(html: str) -> str:
    """Strip script/style tags and event handler attributes from SEC HTML."""
    html = _re.sub(r'<script[^>]*>.*?</script[^>]*>', '', html, flags=_re.DOTALL | _re.IGNORECASE)
    html = _re.sub(r'<style[^>]*>.*?</style[^>]*>', '', html, flags=_re.DOTALL | _re.IGNORECASE)
    html = _re.sub(r'\s+on\w+\s*=\s*(?:"[^"]*"|\'[^\']*\'|[^\s>]+)', '', html, flags=_re.IGNORECASE)
    return html
