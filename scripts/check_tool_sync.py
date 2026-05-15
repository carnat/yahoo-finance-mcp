#!/usr/bin/env python3
"""Verify that the Cloudflare Worker tool list matches the Python MCP server.

Both server.py and worker/src/tools.ts define their own tool manifests.
This script extracts tool names from each and fails if they diverge,
preventing accidental desync when new tools are added.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SERVER_PY = ROOT / "server.py"
TOOLS_TS = ROOT / "worker" / "src" / "tools.ts"

EXPECTED_CANONICAL = {
    "get_market_quote",
    "get_historical_prices",
    "analyze_price_performance",
    "analyze_moving_average_position",
    "analyze_volume_ratio",
    "check_volume_liquidity_threshold",
    "get_company_profile",
    "get_fund_profile",
    "analyze_financial_ratios",
    "analyze_credit_health",
    "get_corporate_actions",
    "get_ownership_holders",
    "get_analyst_recommendations",
    "get_analyst_rating_changes",
    "analyze_earnings_momentum",
    "get_company_events_calendar",
    "summarize_options_flow",
    "analyze_options_flow_window",
    "find_put_hedge_candidates",
    "list_sec_company_filings",
    "get_sec_filing_outline",
    "get_sec_filing_section",
    "list_sec_filing_tables",
    "get_sec_filing_table",
    "extract_sec_filing_fact",
    "search_sec_filing_text",
    "analyze_position_signals",
    "calculate_price_target_distance",
    "get_company_news",
    "search_company_news",
    "get_company_press_releases",
    "get_sec_recent_events",
    "get_public_event_timeline",
    "verify_company_event",
    "health_check",
}

EXPECTED_ALIASES = {
    "get_tps_inputs",
    "get_eqf_bracket",
    "get_adv_gate",
    "get_dc134_options_scan",
}


def get_python_tools() -> set:
    """Extract tool names from @yfinance_server.tool(name=...) decorators."""
    source = SERVER_PY.read_text()
    names = set()
    for match in re.finditer(r'@yfinance_server\.tool\(\s*name\s*=\s*"([^"]+)"', source):
        names.add(match.group(1))
    return names


def get_worker_tools() -> set:
    """Extract tool names from the TOOLS array in tools.ts."""
    source = TOOLS_TS.read_text()
    names = set()
    for match in re.finditer(r'name:\s*"([^"]+)"', source):
        names.add(match.group(1))
    return names


def parse_alias_pairs(source: str) -> dict[str, str]:
    return {
        m.group(1): m.group(2)
        for m in re.finditer(r'"([^"]+)"\s*:\s*"([^"]+)"', source)
    }


def validate_alias_targets(aliases: dict[str, str], tools: set[str], source_name: str) -> tuple[bool, str | None]:
    for alias, canonical in aliases.items():
        if alias in tools and canonical not in tools:
            return False, f"ERROR: {source_name} alias '{alias}' maps to missing canonical '{canonical}'"
    return True, None


def main():
    py_source = SERVER_PY.read_text()
    ts_source = TOOLS_TS.read_text()
    py_tools = get_python_tools()
    ts_tools = get_worker_tools()
    py_aliases = parse_alias_pairs(py_source)
    ts_aliases = parse_alias_pairs(ts_source)

    if not py_tools:
        print("ERROR: found 0 tools in server.py — regex may need updating", file=sys.stderr)
        return 1
    if not ts_tools:
        print("ERROR: found 0 tools in worker/src/tools.ts — regex may need updating", file=sys.stderr)
        return 1

    only_py = py_tools - ts_tools
    only_ts = ts_tools - py_tools

    if only_py or only_ts:
        print("ERROR: Tool list mismatch between server.py and worker/src/tools.ts!\n", file=sys.stderr)
        if only_py:
            print("  In server.py but MISSING from worker/src/tools.ts:", file=sys.stderr)
            for name in sorted(only_py):
                print(f"    - {name}", file=sys.stderr)
        if only_ts:
            print("\n  In worker/src/tools.ts but MISSING from server.py:", file=sys.stderr)
            for name in sorted(only_ts):
                print(f"    - {name}", file=sys.stderr)
        print(
            "\n  When adding a new tool, update BOTH server.py AND worker/src/tools.ts.",
            file=sys.stderr,
        )
        return 1

    missing_py_canonical = sorted(EXPECTED_CANONICAL - py_tools)
    missing_ts_canonical = sorted(EXPECTED_CANONICAL - ts_tools)
    if missing_py_canonical or missing_ts_canonical:
        print("ERROR: Missing expected canonical tool(s).", file=sys.stderr)
        if missing_py_canonical:
            print("  Missing in server.py:", ", ".join(missing_py_canonical), file=sys.stderr)
        if missing_ts_canonical:
            print("  Missing in worker/src/tools.ts:", ", ".join(missing_ts_canonical), file=sys.stderr)
        return 1

    missing_py_alias = sorted(EXPECTED_ALIASES - py_tools)
    missing_ts_alias = sorted(EXPECTED_ALIASES - ts_tools)
    if missing_py_alias or missing_ts_alias:
        print("ERROR: Missing expected backward-compatible alias tool(s).", file=sys.stderr)
        if missing_py_alias:
            print("  Missing in server.py:", ", ".join(missing_py_alias), file=sys.stderr)
        if missing_ts_alias:
            print("  Missing in worker/src/tools.ts:", ", ".join(missing_ts_alias), file=sys.stderr)
        return 1

    ok, msg = validate_alias_targets(ts_aliases, ts_tools, "worker")
    if not ok:
        print(msg, file=sys.stderr)
        return 1
    ok, msg = validate_alias_targets(py_aliases, py_tools, "server")
    if not ok:
        print(msg, file=sys.stderr)
        return 1

    print(f"OK: {len(py_tools)} tools in sync with canonical/alias checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
