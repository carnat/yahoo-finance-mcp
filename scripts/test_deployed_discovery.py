#!/usr/bin/env python3
"""Deployed MCP discovery + canonical smoke checks.

Environment variables:
  ALLOW_NETWORK_SKIP  Set to '1' or 'true' to silently skip when the deployed
                      worker is unreachable (suitable for offline/sandbox CI).
                      Defaults to '1' so sandboxed CI passes automatically.
                      For post-deploy smoke tests, set ALLOW_NETWORK_SKIP=0 to
                      ensure network failures fail the job rather than silently skip.
"""

from __future__ import annotations

import json
import os
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
    "index_sec_filing",
    "get_sec_filing_index",
    # Phase 3 extractor tools
    "extract_geographic_revenue",
    "extract_segment_revenue",
    "extract_total_revenue",
    "extract_revenue_exposure",
    "extract_china_exposure",
    "extract_risk_factor_mentions",
    "extract_customer_concentration",
    "health_check",
}

# Safe args for tools that can be called generically during tool-scan loop.
# Tools not listed here are validated via discovery/schema only; no runtime call with {}.
SMOKE_ARGS: dict[str, dict] = {
    "health_check": {},
    "get_market_quote": {"ticker": "AAPL"},
    "get_historical_prices": {"ticker": "AAPL", "period": "5d", "interval": "1d"},
    "get_company_profile": {"ticker": "AAPL", "include_all": False},
    "get_fund_profile": {"ticker": "SPY"},
    "get_company_news": {"ticker": "AAPL"},
    "search_ticker": {"query": "Apple", "exchange": "US", "max_results": 3},
    "get_option_expiration_dates": {"ticker": "AAPL"},
    "extract_sec_filing_fact": {
        "ticker": "AAOI",
        "fact_type": "geographic_revenue",
        "region": "China",
        "filing_type": "10-K",
        "period": "latest",
    },
}

_ALLOW_SKIP = os.environ.get("ALLOW_NETWORK_SKIP", "1").lower() in ("1", "true", "yes")


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


def call_tool(name: str, arguments: dict, req_id: int, allow_jsonrpc_error: bool = False) -> dict:
    resp = rpc("tools/call", {"name": name, "arguments": arguments}, req_id=req_id)
    if "error" in resp and resp["error"]:
        if allow_jsonrpc_error:
            return resp
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


def _check_aaoi_geographic_revenue_schema(data: dict) -> None:
    """AAOI geographic revenue schema check: denominator/valueRatio/valuePct normalization."""
    if data.get("valuePct") is not None:
        if data.get("denominator") is None:
            raise AssertionError(f"AAOI: valuePct present but denominator is null: {data}")
    if data.get("valueRatio") is not None:
        ratio = float(data["valueRatio"])
        if not (0.0 <= ratio <= 1.0):
            raise AssertionError(f"AAOI: valueRatio {ratio} not in [0, 1] decimal range")
    if data.get("valuePct") is not None:
        pct = float(data["valuePct"])
        if not (0.0 <= pct <= 100.0):
            raise AssertionError(f"AAOI: valuePct {pct} not in [0, 100] percent range")
    if "extractionMethod" not in data:
        raise AssertionError(f"AAOI: extractionMethod missing in response: {data}")
    if "confidence" not in data:
        raise AssertionError(f"AAOI: confidence missing in response: {data}")
    if not (data.get("documentUrl") or data.get("primaryDocumentUrl")):
        raise AssertionError(f"AAOI: neither documentUrl nor primaryDocumentUrl present: {data}")


def _check_axti_not_disclosed_schema(data: dict) -> None:
    """AXTI NOT_DISCLOSED schema check: stable null keys for undisclosed geographic revenue."""
    stable_null_keys = ("value", "denominator", "valueRatio", "valuePct")
    for k in stable_null_keys:
        if k in data and data[k] is not None:
            raise AssertionError(f"AXTI: {k} should be null, got {data[k]!r}")
    extraction = data.get("extractionMethod")
    if extraction not in (None, "NONE", "NOT_DISCLOSED"):
        raise AssertionError(f"AXTI: extractionMethod should be NONE, got {extraction!r}")
    confidence = data.get("confidence")
    if confidence not in (None, "NOT_DISCLOSED"):
        raise AssertionError(f"AXTI: confidence should be NOT_DISCLOSED, got {confidence!r}")


def _check_yahoo_news_structured(data: dict) -> None:
    """Yahoo news structured response schema check."""
    if not isinstance(data, dict):
        raise AssertionError(f"get_company_news returned non-object: {type(data)}")
    if "items" not in data or not isinstance(data.get("items"), list):
        raise AssertionError("get_company_news missing items[]")
    meta = data.get("meta") or {}
    if not isinstance(meta, dict):
        raise AssertionError("get_company_news missing meta object")
    if meta.get("source") != "yahoo_finance":
        raise AssertionError(
            f"get_company_news meta.source expected 'yahoo_finance', got {meta.get('source')!r}"
        )
    required_item_fields = ("title", "publisher", "url", "publishedAt", "retrievedAt", "sourceType")
    for item in (data.get("items") or [])[:5]:
        if not isinstance(item, dict):
            raise AssertionError(f"get_company_news items[] entry is not an object: {item!r}")
        for field in required_item_fields:
            if field not in item:
                raise AssertionError(f"get_company_news item missing field '{field}': {item}")
    # Must not be a plain text blob
    if "_raw" in data:
        raise AssertionError("get_company_news returned raw text blob instead of structured JSON")


def _assert_sec_index_shape(data: dict, tool_name: str) -> None:
    if not isinstance(data, dict):
        raise AssertionError(f"{tool_name} returned non-object: {data!r}")
    doc_url = data.get("documentUrl")
    if not isinstance(doc_url, str) or not doc_url.startswith("https://www.sec.gov/Archives/"):
        raise AssertionError(f"{tool_name} invalid documentUrl: {doc_url!r}")
    idx = data.get("index")
    if not isinstance(idx, dict):
        raise AssertionError(f"{tool_name} missing index object")
    sections = idx.get("sections")
    tables = idx.get("tables")
    keyword_map = idx.get("keywordMap")
    if not isinstance(sections, list):
        raise AssertionError(f"{tool_name} index.sections must be list")
    if not isinstance(tables, list):
        raise AssertionError(f"{tool_name} index.tables must be list")
    if not isinstance(keyword_map, dict):
        raise AssertionError(f"{tool_name} index.keywordMap must be object")
    if not sections and not tables:
        raise AssertionError(f"{tool_name} expected sections or tables, got neither")
    if tables:
        scale_counts = {}
        for tbl in tables:
            if isinstance(tbl, dict):
                scale = str(tbl.get("unitScale", ""))
                scale_counts[scale] = scale_counts.get(scale, 0) + 1
        if scale_counts.get("millions", 0) == len(tables):
            raise AssertionError(
                f"{tool_name} all tables reported unitScale='millions'; expected unknown unless detected"
            )


def main() -> int:
    try:
        listed = rpc("tools/list")
    except urllib.error.URLError as exc:
        # Live URL unreachable (e.g. sandboxed CI with no internet access).
        if _ALLOW_SKIP:
            print(f"SKIP deployed discovery: live worker unreachable ({exc})")
            return 0
        raise AssertionError(
            f"Deployed worker unreachable and ALLOW_NETWORK_SKIP is not set: {exc}"
        ) from exc
    tools = ((listed.get("result") or {}).get("tools")) or []
    names = {str(t.get("name")) for t in tools if isinstance(t, dict)}
    missing = sorted(CANONICAL_TOOLS - names)
    if missing:
        raise AssertionError(f"Missing canonical tools in discovery: {missing}")

    opt = next((t for t in tools if isinstance(t, dict) and t.get("name") == "get_option_chain"), None)
    if opt is None:
        raise AssertionError("get_option_chain missing in discovery")
    props = (((opt.get("inputSchema") or {}).get("properties")) or {})
    for f in ("max_contracts", "min_open_interest", "min_volume",
              "moneyness_window_pct", "include_illiquid"):
        if f not in props:
            raise AssertionError(f"get_option_chain schema missing: {f}")

    # Confirm sort_by enum includes "relevance"
    sort_by_prop = props.get("sort_by") or {}
    sort_by_enum = sort_by_prop.get("enum") or []
    if "relevance" not in sort_by_enum:
        raise AssertionError(f"get_option_chain sort_by enum missing 'relevance': {sort_by_enum}")

    # Confirm default moneyness is "near_money"
    moneyness_default = (props.get("moneyness") or {}).get("default")
    if moneyness_default != "near_money":
        raise AssertionError(
            f"get_option_chain moneyness default must be 'near_money', got: {moneyness_default!r}"
        )

    # Confirm default sort_by is "relevance"
    sort_by_default = sort_by_prop.get("default")
    if sort_by_default != "relevance":
        raise AssertionError(
            f"get_option_chain sort_by default must be 'relevance', got: {sort_by_default!r}"
        )

    filings = call_tool("list_sec_company_filings", {"ticker": "AAPL", "filing_type": "10-K", "limit": 5}, 20)
    assert_no_unknown_tool(filings, "list_sec_company_filings")
    filings_data = extract_data(filings)
    filing_list = filings_data.get("filings") if isinstance(filings_data, dict) else None
    if not isinstance(filing_list, list) or not filing_list:
        raise AssertionError("list_sec_company_filings expected non-empty filings[]")
    first_filing = filing_list[0] if isinstance(filing_list[0], dict) else {}
    if not first_filing.get("accessionNumber"):
        raise AssertionError(f"list_sec_company_filings missing accessionNumber: {first_filing}")
    if not first_filing.get("filingDate"):
        raise AssertionError(f"list_sec_company_filings missing filingDate: {first_filing}")
    if not str(first_filing.get("documentUrl", "")).startswith("https://www.sec.gov/Archives/"):
        raise AssertionError(f"list_sec_company_filings invalid documentUrl: {first_filing}")
    doc_url = None
    if isinstance(filing_list, list) and filing_list:
        doc_url = (filing_list[0] or {}).get("documentUrl")

    aapl_index = call_tool("index_sec_filing", {"ticker": "AAPL", "filing_type": "10-K", "period": "latest"}, 22)
    assert_no_unknown_tool(aapl_index, "index_sec_filing")
    _assert_sec_index_shape(extract_data(aapl_index), "index_sec_filing")

    aapl_cached_index = call_tool("get_sec_filing_index", {"ticker": "AAPL", "filing_type": "10-K", "period": "latest"}, 23)
    assert_no_unknown_tool(aapl_cached_index, "get_sec_filing_index")
    _assert_sec_index_shape(extract_data(aapl_cached_index), "get_sec_filing_index")

    for req_id, ticker in ((24, "AAOI"), (25, "AXTI")):
        idx_payload = call_tool("get_sec_filing_index", {"ticker": ticker, "filing_type": "10-K", "period": "latest"}, req_id)
        assert_no_unknown_tool(idx_payload, "get_sec_filing_index")
        idx_data = extract_data(idx_payload)
        _assert_sec_index_shape(idx_data, f"get_sec_filing_index:{ticker}")
        if ticker == "AXTI":
            index = (idx_data or {}).get("index") if isinstance(idx_data, dict) else {}
            key_map = index.get("keywordMap") if isinstance(index, dict) else {}
            if isinstance(key_map, dict) and not any("china" in str(k).lower() for k in key_map):
                print("  WARN AXTI index keywordMap has no explicit China keyword")

    exp = call_tool("get_option_expiration_dates", {"ticker": "ASTS"}, 21)
    assert_no_unknown_tool(exp, "get_option_expiration_dates")
    expiry_dates = extract_data(exp)
    expiry = expiry_dates[0] if isinstance(expiry_dates, list) and expiry_dates else "2025-06-20"

    calls: list[tuple[str, dict]] = [
        ("health_check", {}),
        ("get_market_quote", {"ticker": "ASTS"}),
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
        ("get_company_news", {"ticker": "AAPL"}),
        # PR50 AAOI/AXTI schema smoke
        ("extract_sec_filing_fact", {"ticker": "AAOI", "fact_type": "geographic_revenue", "region": "China", "filing_type": "10-K", "period": "latest"}),
        ("extract_sec_filing_fact", {"ticker": "AXTI", "fact_type": "geographic_revenue", "region": "China", "filing_type": "10-K", "period": "latest"}),
        # Phase 3 extractor tools — dispatch smoke (schema-only, no deep value assertions)
        ("extract_geographic_revenue", {"ticker": "AAPL", "region": "Greater China", "filing_type": "10-K", "period": "latest"}),
        ("extract_segment_revenue", {"ticker": "AAPL", "filing_type": "10-K", "period": "latest"}),
        ("extract_total_revenue", {"ticker": "AAPL", "filing_type": "10-K", "period": "latest"}),
        ("extract_revenue_exposure", {"ticker": "AAOI", "exposure_query": "China", "filing_type": "10-K", "period": "latest"}),
        ("extract_china_exposure", {"ticker": "AXTI", "filing_type": "10-K", "period": "latest"}),
        ("extract_risk_factor_mentions", {"ticker": "AXTI", "terms": ["China", "tariff"], "filing_type": "10-K", "period": "latest"}),
        ("extract_customer_concentration", {"ticker": "AAOI", "filing_type": "10-K", "period": "latest"}),
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
            if "dataQuality" not in data:
                raise AssertionError("get_option_chain missing dataQuality block")
            fa = data.get("filtersApplied") or {}
            for key in ("sort_by", "moneyness"):
                if key not in fa:
                    raise AssertionError(f"get_option_chain filtersApplied missing: {key}")
        if name == "extract_sec_filing_fact" and args.get("ticker") == "QCOM":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError("extract_sec_filing_fact returned non-object")
            if "valuePct" not in data:
                raise AssertionError("extract_sec_filing_fact missing valuePct")
        if name == "extract_sec_filing_fact" and args.get("ticker") == "AAOI":
            data = extract_data(payload)
            if isinstance(data, dict):
                _check_aaoi_geographic_revenue_schema(data)
                print("  PASS AAOI geographic revenue schema check")
        if name == "extract_sec_filing_fact" and args.get("ticker") == "AXTI":
            data = extract_data(payload)
            if isinstance(data, dict):
                _check_axti_not_disclosed_schema(data)
                print("  PASS AXTI NOT_DISCLOSED schema check")
        if name == "get_company_news":
            data = extract_data(payload)
            _check_yahoo_news_structured(data)
            print("  PASS Yahoo news structured smoke")
        # Phase 3 extractor tool dispatch checks
        if name == "extract_geographic_revenue":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError(f"extract_geographic_revenue returned non-object: {data!r}")
            for field in ("value", "denominator", "valueRatio", "valuePct", "confidence", "extractionMethod"):
                if field not in data:
                    raise AssertionError(f"extract_geographic_revenue missing field: {field}")
            print(f"  PASS extract_geographic_revenue dispatch ({args.get('ticker')}/{args.get('region')})")
        if name == "extract_segment_revenue":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError(f"extract_segment_revenue returned non-object: {data!r}")
            if "status" not in data:
                raise AssertionError("extract_segment_revenue missing status")
            print(f"  PASS extract_segment_revenue dispatch ({args.get('ticker')})")
        if name == "extract_total_revenue":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError(f"extract_total_revenue returned non-object: {data!r}")
            if "status" not in data and "value" not in data:
                raise AssertionError("extract_total_revenue missing value/status")
            print(f"  PASS extract_total_revenue dispatch ({args.get('ticker')})")
        if name == "extract_revenue_exposure":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError(f"extract_revenue_exposure returned non-object: {data!r}")
            if "status" not in data:
                raise AssertionError("extract_revenue_exposure missing status")
            if "matches" not in data:
                raise AssertionError("extract_revenue_exposure missing matches")
            print(f"  PASS extract_revenue_exposure dispatch ({args.get('ticker')}/{args.get('exposure_query')})")
        if name == "extract_china_exposure":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError(f"extract_china_exposure returned non-object: {data!r}")
            for field in ("revenueExposure", "overallStatus"):
                if field not in data:
                    raise AssertionError(f"extract_china_exposure missing field: {field}")
            print(f"  PASS extract_china_exposure dispatch ({args.get('ticker')})")
        if name == "extract_risk_factor_mentions":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError(f"extract_risk_factor_mentions returned non-object: {data!r}")
            if "status" not in data or "matches" not in data:
                raise AssertionError("extract_risk_factor_mentions missing status/matches")
            print(f"  PASS extract_risk_factor_mentions dispatch ({args.get('ticker')})")
        if name == "extract_customer_concentration":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError(f"extract_customer_concentration returned non-object: {data!r}")
            if "status" not in data or "customers" not in data:
                raise AssertionError("extract_customer_concentration missing status/customers")
            print(f"  PASS extract_customer_concentration dispatch ({args.get('ticker')})")

    # Chained option-chain smoke (AAPL)
    aapl_exp_payload = call_tool("get_option_expiration_dates", {"ticker": "AAPL"}, 900)
    assert_no_unknown_tool(aapl_exp_payload, "get_option_expiration_dates")
    aapl_dates = extract_data(aapl_exp_payload)
    aapl_expiry = aapl_dates[0] if isinstance(aapl_dates, list) and aapl_dates else "2025-06-20"
    aapl_chain = call_tool(
        "get_option_chain",
        {"ticker": "AAPL", "expiration_date": aapl_expiry, "option_type": "calls"},
        901,
    )
    assert_no_unknown_tool(aapl_chain, "get_option_chain")
    chain_data = extract_data(aapl_chain)
    if not isinstance(chain_data, dict):
        raise AssertionError(f"get_option_chain (AAPL) returned non-object: {chain_data!r}")
    if "dataQuality" not in chain_data:
        raise AssertionError("get_option_chain (AAPL) missing dataQuality")
    if "filtersApplied" not in chain_data:
        raise AssertionError("get_option_chain (AAPL) missing filtersApplied")
    print("  PASS chained option-chain smoke (AAPL)")

    # Invalid-args validation test: get_historical_stock_prices({}) must not cause provider 404
    bad_payload = call_tool("get_historical_stock_prices", {}, 902, allow_jsonrpc_error=True)
    bad_str = json.dumps(bad_payload)
    # Must return a validation error. The specific check is whether we see a Yahoo chart 404
    # (which indicates the provider was called with no ticker before validation occurred).
    # We detect this by checking whether the error is a provider HTTP 404 on the chart endpoint,
    # NOT just any mention of those strings.
    is_provider_404 = (
        "chart" in bad_str.lower()
        and "404" in bad_str
        and ("yahoo" in bad_str.lower() or "finance" in bad_str.lower())
        and "INPUT_VALIDATION_ERROR" not in bad_str
    )
    if is_provider_404:
        raise AssertionError(
            f"get_historical_stock_prices({{}}) caused provider 404 instead of INPUT_VALIDATION_ERROR.\n"
            f"Response: {bad_str[:400]}"
        )
    if not (
        "INPUT_VALIDATION_ERROR" in bad_str
        or (isinstance(bad_payload, dict) and bad_payload.get("ok") is False)
        or (isinstance(bad_payload, dict) and bad_payload.get("error"))
    ):
        raise AssertionError(
            f"get_historical_stock_prices({{}}) expected validation error or ok=false, got: {bad_str[:400]}"
        )
    print("  PASS invalid-args test (empty ticker returns validation error, not provider 404)")

    # Tool-scan loop: use SMOKE_ARGS for known-safe tools; skip runtime call for others
    for idx, t in enumerate(tools, start=1000):
        if not isinstance(t, dict):
            continue
        n = str(t.get("name", ""))
        if not n:
            continue
        if n in SMOKE_ARGS:
            payload = call_tool(n, SMOKE_ARGS[n], idx)
            assert_no_unknown_tool(payload, n)
        # Tools not in SMOKE_ARGS: discovery/schema validated above, skip runtime call with {}

    print(f"PASS deployed discovery + smoke ({len(names)} tools)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
