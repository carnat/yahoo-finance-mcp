#!/usr/bin/env python3
"""
End-to-end test script for all 30 Yahoo Finance MCP tools.

Sends MCP JSON-RPC tool/call requests to the live Cloudflare Worker and
reports PASS / FAIL for each test case.

A test case consists of:
  - tool_name     : str
  - args          : dict passed to the tool
  - assertions    : optional list of (dot-path, expected_value) pairs that are
                    verified against the parsed JSON response.  Use the special
                    sentinel ``NOT_ZERO`` to assert a field is non-zero/non-null,
                    ``IS_NULL`` to assert it is null, and ``NOT_NULL`` to assert
                    it is present and not null.

Usage:
    python scripts/test_tools.py [--url https://your-worker.workers.dev/mcp]

Defaults to https://yahoo-finance-mcp.artinatw.workers.dev/mcp
"""

import argparse
import json
import sys
import traceback
import urllib.error
import urllib.request
from typing import Any

DEFAULT_URL = "https://yahoo-finance-mcp.artinatw.workers.dev/mcp"

# ── Assertion sentinels ───────────────────────────────────────────────────────

class _Sentinel:
    def __init__(self, name: str) -> None:
        self._name = name
    def __repr__(self) -> str:
        return self._name

NOT_ZERO = _Sentinel("NOT_ZERO")   # field exists and is not 0 / not null
IS_NULL  = _Sentinel("IS_NULL")    # field is JSON null
NOT_NULL = _Sentinel("NOT_NULL")   # field is present and not null


# ── Test case type ─────────────────────────────────────────────────────────────

# Each entry: (tool_name, args, assertions)
# assertions is a list of (dot-path-into-result, expected) tuples.
# Dot-path examples: "lastVolume", "AAPL.shares", "_note"
TestCase = tuple[str, dict[str, Any], list[tuple[str, Any]]]


# ── Test suite ─────────────────────────────────────────────────────────────────

TEST_CASES: list[TestCase] = [
    # ── single-ticker fundamentals ────────────────────────────────────────
    (
        "get_historical_stock_prices",
        {"ticker": "AAPL", "period": "5d", "interval": "1d"},
        [],
    ),
    (
        "get_stock_info",
        {"ticker": "MSFT"},
        [("shortName", NOT_NULL)],
    ),
    (
        "get_etf_info",
        {"ticker": "SPY"},
        [("shortName", NOT_NULL)],
    ),
    (
        "get_yahoo_finance_news",
        {"ticker": "NVDA"},
        [],
    ),
    (
        "get_stock_actions",
        {"ticker": "AAPL"},
        [],
    ),
    (
        "get_financial_statement",
        {"ticker": "AAPL", "financial_type": "income_stmt"},
        [],
    ),
    (
        "get_holder_info",
        {"ticker": "AAPL", "holder_type": "major_holders"},
        [],
    ),
    (
        "get_option_expiration_dates",
        {"ticker": "AAPL"},
        [],
    ),
    # options chain: the expiry must exist on Yahoo; if it doesn't Yahoo
    # returns a data error which is acceptable — the tool must not crash.
    (
        "get_option_chain",
        {"ticker": "AAPL", "expiration_date": "2025-06-20", "option_type": "calls"},
        [],
    ),
    (
        "get_recommendations",
        {"ticker": "AAPL", "recommendation_type": "recommendations"},
        [],
    ),
    # ── get_fast_info (stock) ─────────────────────────────────────────────
    (
        "get_fast_info",
        {"ticker": "AAPL"},
        [
            ("lastPrice",   NOT_NULL),
            ("quoteType",   "EQUITY"),
            # Normal equity: volume must be a real positive number
            ("lastVolume",  NOT_ZERO),
        ],
    ),
    # ── get_fast_info (index) — Issue 1 regression guard ─────────────────
    # ^VIX is an index: volume/shares/marketCap must be null, not 0.
    # A _note field must be present explaining the null values.
    (
        "get_fast_info",
        {"ticker": "^VIX"},
        [
            ("lastPrice",              NOT_NULL),
            ("quoteType",              "INDEX"),
            ("lastVolume",             IS_NULL),
            ("tenDayAverageVolume",    IS_NULL),
            ("threeMonthAverageVolume",IS_NULL),
            ("shares",                 IS_NULL),
            ("marketCap",              IS_NULL),
            ("_note",                  NOT_NULL),
        ],
    ),
    (
        "get_fast_info",
        {"ticker": "^VVIX"},
        [
            ("quoteType",  "INDEX"),
            ("lastVolume", IS_NULL),
        ],
    ),
    # ── get_fast_info (small-cap) — Issue 2 regression guard ─────────────
    # ASTS (AST SpaceMobile) had a missing sharesOutstanding in the price
    # module; the fix falls back to defaultKeyStatistics.sharesOutstanding.
    (
        "get_fast_info",
        {"ticker": "ASTS"},
        [("shares", NOT_NULL)],
    ),
    # ── more fast_info / price stats ──────────────────────────────────────
    (
        "get_price_stats",
        {"ticker": "AAPL"},
        [("lastPrice", NOT_NULL)],
    ),
    (
        "get_analyst_consensus",
        {"ticker": "AAPL"},
        [],
    ),
    (
        "get_earnings_analysis",
        {"ticker": "AAPL"},
        [],
    ),
    (
        "get_financial_ratios",
        {"ticker": "AAPL"},
        [],
    ),
    (
        "get_calendar",
        {"ticker": "AAPL"},
        [],
    ),
    # ── search / screen ───────────────────────────────────────────────────
    (
        "search_ticker",
        {"query": "Apple"},
        [],
    ),
    (
        "screen_stocks",
        {"screener_name": "day_gainers", "count": 5},
        [],
    ),
    # ── filings / short interest ──────────────────────────────────────────
    (
        "get_sec_filings",
        {"ticker": "AAPL"},
        [],
    ),
    (
        "get_short_interest",
        {"ticker": "AAPL"},
        [],
    ),
    # ── technical / momentum ──────────────────────────────────────────────
    (
        "get_technical_indicators",
        {"ticker": "AAPL"},
        [("rsi14", NOT_NULL)],
    ),
    (
        "get_price_slope",
        {"ticker": "AAPL", "days": 5},
        [("slopePct", NOT_NULL)],
    ),
    (
        "get_volume_ratio",
        {"ticker": "AAPL"},
        [("ratio10d", NOT_NULL)],
    ),
    (
        "get_ma_position",
        {"ticker": "AAPL"},
        [("trend", NOT_NULL)],
    ),
    # ── pre-computed alpha signals ────────────────────────────────────────
    (
        "get_credit_health",
        {"ticker": "AAPL"},
        [],
    ),
    (
        "get_short_momentum",
        {"ticker": "AAPL"},
        [],
    ),
    (
        "get_earnings_momentum",
        {"ticker": "AAPL"},
        [],
    ),
    (
        "get_options_flow_summary",
        {"ticker": "AAPL"},
        [],
    ),
    (
        "get_put_hedge_candidates",
        {"ticker": "AAPL", "otm_pct_min": 8, "otm_pct_max": 12},
        [],
    ),
    (
        "get_analyst_upgrade_radar",
        {"ticker": "AAPL", "days_back": 30},
        [],
    ),
    # ── batch variants: same tools with an array of tickers ───────────────
    # Intentional re-tests of single-ticker tools to verify batch dispatch.
    (
        "get_fast_info",
        {"ticker": ["AAPL", "MSFT"]},
        [
            ("AAPL.lastPrice", NOT_NULL),
            ("MSFT.lastPrice", NOT_NULL),
        ],
    ),
    (
        "get_etf_info",
        {"ticker": ["SPY", "QQQ"]},
        [
            ("SPY.shortName", NOT_NULL),
            ("QQQ.shortName", NOT_NULL),
        ],
    ),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _call(url: str, tool: str, args: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": args},
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def _get_path(obj: Any, path: str) -> Any:
    """Resolve a dot-separated path into a parsed JSON object."""
    parts = path.split(".")
    cur: Any = obj
    for p in parts:
        if not isinstance(cur, dict):
            raise KeyError(f"Expected dict at '{p}', got {type(cur).__name__}")
        if p not in cur:
            raise KeyError(f"Key '{p}' not found")
        cur = cur[p]
    return cur


def _check_assertions(
    inner: Any, assertions: list[tuple[str, Any]]
) -> list[str]:
    """Return a list of failure messages (empty = all passed)."""
    failures: list[str] = []
    for path, expected in assertions:
        try:
            actual = _get_path(inner, path)
        except KeyError as exc:
            failures.append(f"assertion '{path}': missing key — {exc}")
            continue

        if expected is IS_NULL:
            if actual is not None:
                failures.append(f"assertion '{path}': expected null, got {actual!r}")
        elif expected is NOT_NULL:
            if actual is None:
                failures.append(f"assertion '{path}': expected non-null, got null")
        elif expected is NOT_ZERO:
            if actual is None or actual == 0:
                failures.append(f"assertion '{path}': expected non-zero/non-null, got {actual!r}")
        else:
            if actual != expected:
                failures.append(f"assertion '{path}': expected {expected!r}, got {actual!r}")
    return failures


def _is_ok(
    response: dict[str, Any],
    assertions: list[tuple[str, Any]],
) -> tuple[bool, str]:
    """Return (passed, detail_message)."""
    if "error" in response:
        return False, f"JSON-RPC error: {response['error']}"

    result = response.get("result", {})
    content = result.get("content", [])
    if not content:
        return False, "empty content array"

    first = content[0] if isinstance(content, list) else {}
    text = first.get("text", "") if isinstance(first, dict) else ""
    if not text:
        return False, "empty text in content[0]"

    # Parse inner text for assertion checking
    inner: Any = None
    try:
        inner = json.loads(text)
        if isinstance(inner, dict) and "error" in inner:
            return False, f"tool returned error: {inner['error']}"
    except json.JSONDecodeError:
        pass  # non-JSON text is fine (e.g. "No data found")

    # Run encoded assertions
    if assertions and inner is not None:
        fails = _check_assertions(inner, assertions)
        if fails:
            return False, "; ".join(fails)

    snippet = text[:100].replace("\n", " ")
    return True, snippet


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Test all MCP tools end-to-end")
    parser.add_argument("--url", default=DEFAULT_URL, help="MCP endpoint URL")
    opts = parser.parse_args()

    url = opts.url
    print(f"\nTarget: {url}\n")
    print(f"{'Tool':<35} {'Args summary':<38} {'Result'}")
    print("-" * 112)

    passed = failed = 0
    failures: list[str] = []

    for tool, tool_args, assertions in TEST_CASES:
        args_summary = json.dumps(tool_args)[:36]
        try:
            response = _call(url, tool, tool_args)
            ok, detail = _is_ok(response, assertions)
        except urllib.error.URLError as exc:
            ok, detail = False, f"HTTP error: {exc}"
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, f"exception: {exc}\n{traceback.format_exc()}"

        status = "✅ PASS" if ok else "❌ FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
            failures.append(f"  {tool}({args_summary}): {detail}")

        print(f"{tool:<35} {args_summary:<38} {status}  {detail[:58]}")

    total = passed + failed
    print("-" * 112)
    print(f"\n{passed}/{total} tools passed")

    if failures:
        print("\nFailed tools:")
        for f in failures:
            print(f)
        sys.exit(1)
    else:
        print("\nAll tools passed ✅")


if __name__ == "__main__":
    main()

