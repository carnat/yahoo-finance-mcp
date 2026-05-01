#!/usr/bin/env python3
"""
End-to-end test script for all 30 Yahoo Finance MCP tools.

Sends MCP JSON-RPC tool/call requests to the live Cloudflare Worker and
reports PASS / FAIL for each tool. A tool PASSES when the response
contains a non-empty result text with no top-level "error" key.

Usage:
    python scripts/test_tools.py [--url https://your-worker.workers.dev/mcp]

Defaults to https://yahoo-finance-mcp.artinatw.workers.dev/mcp
"""

import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Any

DEFAULT_URL = "https://yahoo-finance-mcp.artinatw.workers.dev/mcp"

# ---------------------------------------------------------------------------
# Test cases: (tool_name, args_dict)
# Use well-known liquid symbols so Yahoo Finance almost always returns data.
# ---------------------------------------------------------------------------
TEST_CASES: list[tuple[str, dict[str, Any]]] = [
    # ── single-ticker fundamentals ────────────────────────────────────────
    ("get_historical_stock_prices", {"ticker": "AAPL", "period": "5d", "interval": "1d"}),
    ("get_stock_info",              {"ticker": "MSFT"}),
    ("get_etf_info",                {"ticker": "SPY"}),
    ("get_yahoo_finance_news",      {"ticker": "NVDA"}),
    ("get_stock_actions",           {"ticker": "AAPL"}),
    ("get_financial_statement",     {"ticker": "AAPL", "financial_type": "income_stmt"}),
    ("get_holder_info",             {"ticker": "AAPL", "holder_type": "major_holders"}),
    ("get_option_expiration_dates", {"ticker": "AAPL"}),
    # options chain: we rely on get_option_expiration_dates above; use a
    # known near-term date. If it no longer exists Yahoo returns an error,
    # but the tool itself must respond without crashing.
    ("get_option_chain",            {"ticker": "AAPL", "expiration_date": "2025-06-20", "option_type": "calls"}),
    ("get_recommendations",         {"ticker": "AAPL", "recommendation_type": "recommendations"}),
    ("get_fast_info",               {"ticker": "AAPL"}),
    ("get_price_stats",             {"ticker": "AAPL"}),
    ("get_analyst_consensus",       {"ticker": "AAPL"}),
    ("get_earnings_analysis",       {"ticker": "AAPL"}),
    ("get_financial_ratios",        {"ticker": "AAPL"}),
    ("get_calendar",                {"ticker": "AAPL"}),
    # ── search / screen ───────────────────────────────────────────────────
    ("search_ticker",               {"query": "Apple"}),
    ("screen_stocks",               {"screener_name": "day_gainers", "count": 5}),
    # ── filings / short interest ──────────────────────────────────────────
    ("get_sec_filings",             {"ticker": "AAPL"}),
    ("get_short_interest",          {"ticker": "AAPL"}),
    # ── technical / momentum ──────────────────────────────────────────────
    ("get_technical_indicators",    {"ticker": "AAPL"}),
    ("get_price_slope",             {"ticker": "AAPL", "days": 5}),
    ("get_volume_ratio",            {"ticker": "AAPL"}),
    ("get_ma_position",             {"ticker": "AAPL"}),
    # ── pre-computed alpha signals ────────────────────────────────────────
    ("get_credit_health",           {"ticker": "AAPL"}),
    ("get_short_momentum",          {"ticker": "AAPL"}),
    ("get_earnings_momentum",       {"ticker": "AAPL"}),
    ("get_options_flow_summary",    {"ticker": "AAPL"}),
    ("get_put_hedge_candidates",    {"ticker": "AAPL", "otm_pct_min": 8, "otm_pct_max": 12}),
    ("get_analyst_upgrade_radar",   {"ticker": "AAPL", "days_back": 30}),
    # ── batch variants (array of tickers) ─────────────────────────────────
    ("get_fast_info",               {"ticker": ["AAPL", "MSFT"]}),
    ("get_etf_info",                {"ticker": ["SPY", "QQQ"]}),
]


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


def _is_ok(response: dict[str, Any]) -> tuple[bool, str]:
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

    # Parse inner text as JSON to check for a top-level error field
    try:
        inner = json.loads(text)
        if isinstance(inner, dict) and "error" in inner:
            return False, f"tool returned error: {inner['error']}"
    except json.JSONDecodeError:
        pass  # non-JSON text is fine (e.g. "No data found")

    snippet = text[:120].replace("\n", " ")
    return True, snippet


def main() -> None:
    parser = argparse.ArgumentParser(description="Test all MCP tools end-to-end")
    parser.add_argument("--url", default=DEFAULT_URL, help="MCP endpoint URL")
    args = parser.parse_args()

    url = args.url
    print(f"\nTarget: {url}\n")
    print(f"{'Tool':<35} {'Args summary':<40} {'Result'}")
    print("-" * 110)

    passed = failed = 0
    failures: list[str] = []

    for tool, tool_args in TEST_CASES:
        args_summary = json.dumps(tool_args)[:38]
        try:
            response = _call(url, tool, tool_args)
            ok, detail = _is_ok(response)
        except urllib.error.URLError as exc:
            ok, detail = False, f"HTTP error: {exc}"
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, f"exception: {exc}"

        status = "✅ PASS" if ok else "❌ FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
            failures.append(f"  {tool}({args_summary}): {detail}")

        print(f"{tool:<35} {args_summary:<40} {status}  {detail[:60]}")

    total = passed + failed
    print("-" * 110)
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
