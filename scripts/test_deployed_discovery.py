#!/usr/bin/env python3
"""Deployed MCP discovery + canonical smoke checks."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

URL = "https://yahoo-finance-mcp.artinatw.workers.dev/mcp"
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-deployed-discovery/1.0)"
CANONICAL_TOOLS = {
    "get_market_quote",
    "analyze_position_signals",
    "calculate_price_target_distance",
    "check_volume_liquidity_threshold",
    "summarize_options_flow",
    "analyze_options_flow_window",
    "list_sec_company_filings",
    "get_sec_filing_outline",
    "get_sec_filing_section",
    "list_sec_filing_tables",
    "get_sec_filing_table",
    "extract_sec_filing_fact",
    "search_sec_filing_text",
    "health_check",
}


def rpc(method: str, params: dict | None = None, req_id: int = 1) -> dict:
    payload = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        payload["params"] = params
    data = json.dumps(payload).encode("utf-8")
    last_exc: Exception | None = None
    for i in range(3):
        req = urllib.request.Request(
            URL,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": UA},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                return json.loads(resp.read())
        except urllib.error.URLError as e:
            last_exc = e
            if i < 2:
                time.sleep(2 * (i + 1))
                continue
            raise
    raise last_exc or RuntimeError("RPC failed")


def call_tool(name: str, arguments: dict, req_id: int) -> dict:
    resp = rpc("tools/call", {"name": name, "arguments": arguments}, req_id=req_id)
    if "error" in resp:
        raise AssertionError(f"{name} JSON-RPC error: {resp['error']}")
    text = ((((resp.get("result") or {}).get("content") or [{}])[0]) or {}).get("text", "")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"_raw": text}


def extract_data(payload: dict) -> dict:
    if isinstance(payload, dict) and "ok" in payload and "data" in payload:
        return payload.get("data") or {}
    return payload


def assert_no_unknown_tool(payload: dict, tool: str) -> None:
    s = json.dumps(payload).lower()
    bad = ("unknown tool", "method not found", "unregistered dispatch")
    if any(b in s for b in bad):
        raise AssertionError(f"{tool} returned non-callable error: {payload}")


def main() -> int:
    try:
        listed = rpc("tools/list")
    except urllib.error.URLError as exc:
        # Live URL unreachable (e.g. sandboxed CI with no internet access).
        print(f"SKIP deployed discovery: live worker unreachable ({exc})")
        return 0
    tools = ((listed.get("result") or {}).get("tools")) or []
    names = {str(t.get("name")) for t in tools if isinstance(t, dict)}
    missing = sorted(CANONICAL_TOOLS - names)
    if missing:
        raise AssertionError(f"Missing canonical tools in discovery: {missing}")

    opt = next((t for t in tools if isinstance(t, dict) and t.get("name") == "get_option_chain"), None)
    if opt is None:
        raise AssertionError("get_option_chain missing in discovery")
    props = (((opt.get("inputSchema") or {}).get("properties")) or {})
    for f in ("max_contracts", "min_open_interest", "min_volume"):
        if f not in props:
            raise AssertionError(f"get_option_chain schema missing: {f}")

    filings = call_tool("list_sec_company_filings", {"ticker": "AAPL", "filing_type": "10-K", "limit": 3}, 20)
    assert_no_unknown_tool(filings, "list_sec_company_filings")
    filings_data = extract_data(filings)
    filing_list = filings_data.get("filings") if isinstance(filings_data, dict) else None
    doc_url = None
    if isinstance(filing_list, list) and filing_list:
        doc_url = (filing_list[0] or {}).get("primaryDocumentUrl")

    exp = call_tool("get_option_expiration_dates", {"ticker": "ASTS"}, 21)
    assert_no_unknown_tool(exp, "get_option_expiration_dates")
    expiry_dates = extract_data(exp)
    expiry = expiry_dates[0] if isinstance(expiry_dates, list) and expiry_dates else "2025-06-20"

    calls: list[tuple[str, dict]] = [
        ("health_check", {}),
        ("get_market_quote", {"ticker": "ASTS"}),
        ("get_fast_info", {"ticker": "ASTS"}),
        ("analyze_position_signals", {"ticker": "ASTS"}),
        ("calculate_price_target_distance", {"ticker": "ASTS", "io_pt": 95}),
        ("check_volume_liquidity_threshold", {"ticker": "ASTS"}),
        ("summarize_options_flow", {"ticker": "ASTS"}),
        ("analyze_options_flow_window", {"ticker": "ASTS", "window_label": "audit"}),
        ("get_option_chain", {"ticker": "ASTS", "expiration_date": expiry, "option_type": "calls", "max_contracts": 10}),
        ("get_sec_filing_outline", {"ticker": "AAPL", "filing_type": "10-K", "period": "latest"}),
        ("get_sec_filing_section", {"ticker": "AAPL", "filing_type": "10-K", "selector": {"item": "Item 1A"}}),
        ("list_sec_filing_tables", {"ticker": "AAPL", "filing_type": "10-K"}),
        ("get_sec_filing_table", {"ticker": "AAPL", "filing_type": "10-K", "table_index": 0}),
        ("extract_sec_filing_fact", {"ticker": "QCOM", "fact": "geographic_revenue", "region": "China"}),
        ("search_sec_filing_text", {"ticker": "AAPL", "search_terms": ["Greater China"], "filing_type": "10-K"}),
    ]
    if doc_url:
        calls.extend([
            ("get_sec_filing_outline", {"ticker": "AAPL", "document_url": doc_url}),
            ("list_sec_filing_tables", {"ticker": "AAPL", "document_url": doc_url}),
        ])

    for i, (name, args) in enumerate(calls, start=100):
        payload = call_tool(name, args, i)
        assert_no_unknown_tool(payload, name)
        if name == "health_check":
            health = extract_data(payload)
            print(f"  health_check response: {json.dumps(payload)}")
            if isinstance(health, dict) and health.get("envelopeV2") is not True:
                raise AssertionError(f"health_check envelopeV2 expected true, got: {health}")
        if name == "get_option_chain":
            data = extract_data(payload)
            if not isinstance(data, dict) or "filtersApplied" not in data:
                raise AssertionError("get_option_chain missing filtersApplied")
            contracts = data.get("contracts")
            if isinstance(contracts, list) and len(contracts) > 10:
                raise AssertionError("max_contracts=10 not honored")
        if name == "extract_sec_filing_fact":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError("extract_sec_filing_fact returned non-object")
            if "valuePct" not in data:
                raise AssertionError("extract_sec_filing_fact missing valuePct")

    for idx, t in enumerate(tools, start=1000):
        if not isinstance(t, dict):
            continue
        n = str(t.get("name", ""))
        if not n:
            continue
        payload = call_tool(n, {}, idx)
        assert_no_unknown_tool(payload, n)

    print(f"PASS deployed discovery + smoke ({len(names)} tools)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

