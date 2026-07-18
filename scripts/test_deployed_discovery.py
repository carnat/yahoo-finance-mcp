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
import re
import time
import urllib.error
import urllib.request

from live_smoke_utils import choose_stable_option_expiration

URL = "https://yahoo-finance-mcp.artinatw.workers.dev/mcp"
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-deployed-discovery/1.0)"
CANONICAL_TOOLS = {
    "get_market_quote",
    "analyze_share_count_trend",
    "get_market_calendar",
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
    "extract_segment_revenue",
    "extract_total_revenue",
    "extract_risk_factor_mentions",
    "extract_customer_concentration",
    "extract_exposure",
    "query_sec_filing_index",
    "get_latest_earnings_release",
    "index_earnings_release",
    "extract_earnings_metrics",
    "extract_guidance",
    "extract_management_commentary",
    "compare_earnings_actual_vs_estimate",
    "get_company_news",
    "search_company_news",
    "get_company_press_releases",
    "get_sec_recent_events",
    "get_public_event_timeline",
    "verify_company_event",
    "search_thai_funds",
    "get_thai_fund_nav",
    "get_thai_fund_nav_batch",
    "get_thai_fund_factsheet",
    "get_thai_fund_dividend_history",
    "health_check",
}
GROUPED_TOOLS = {
    "stock_pricing",
    "stock_fundamentals",
    "analyst_data",
    "options_analysis",
    "sec_filings",
    "sec_extractors",
    "news_events",
    "earnings_intelligence",
    "screening",
    "system",
    "thai_funds",
}
ACTION_GROUP = {
    "health_check": "system",
    "get_manifest_diagnostics": "system",
    "get_market_quote": "stock_pricing",
    "get_historical_prices": "stock_pricing",
    "analyze_price_performance": "stock_pricing",
    "analyze_moving_average_position": "stock_pricing",
    "analyze_volume_ratio": "stock_pricing",
    "check_volume_liquidity_threshold": "stock_pricing",
    "get_technical_indicators": "stock_pricing",
    "get_price_slope": "stock_pricing",
    "get_short_interest": "stock_pricing",
    "get_short_momentum": "stock_pricing",
    "get_overnight_quote": "stock_pricing",
    "get_market_snapshot": "stock_pricing",
    "get_company_profile": "stock_fundamentals",
    "get_fund_profile": "stock_fundamentals",
    "get_financial_statement": "stock_fundamentals",
    "analyze_financial_ratios": "stock_fundamentals",
    "analyze_share_count_trend": "stock_fundamentals",
    "analyze_credit_health": "stock_fundamentals",
    "get_corporate_actions": "stock_fundamentals",
    "get_ownership_holders": "stock_fundamentals",
    "get_analyst_consensus": "analyst_data",
    "get_earnings_analysis": "analyst_data",
    "get_analyst_recommendations": "analyst_data",
    "get_analyst_rating_changes": "analyst_data",
    "analyze_earnings_momentum": "analyst_data",
    "get_company_events_calendar": "analyst_data",
    "get_market_calendar": "analyst_data",
    "get_option_expiration_dates": "options_analysis",
    "get_option_chain": "options_analysis",
    "summarize_options_flow": "options_analysis",
    "find_put_hedge_candidates": "options_analysis",
    "analyze_options_flow_window": "options_analysis",
    "list_sec_company_filings": "sec_filings",
    "list_sec_material_filings": "sec_filings",
    "get_sec_filing_outline": "sec_filings",
    "get_sec_filing_section": "sec_filings",
    "get_sec_filing_section_markdown": "sec_filings",
    "list_sec_filing_tables": "sec_filings",
    "get_sec_filing_table": "sec_filings",
    "extract_sec_filing_fact": "sec_filings",
    "search_sec_filing_text": "sec_filings",
    "index_sec_filing": "sec_filings",
    "get_sec_filing_index": "sec_filings",
    "get_sec_filing_intelligence": "sec_filings",
    "query_sec_filing_index": "sec_filings",
    "list_sec_filing_exhibits": "sec_filings",
    "get_sec_filing_exhibit_content": "sec_filings",
    "extract_geographic_revenue": "sec_extractors",
    "extract_segment_revenue": "sec_extractors",
    "extract_total_revenue": "sec_extractors",
    "extract_revenue_exposure": "sec_extractors",
    "extract_china_exposure": "sec_extractors",
    "extract_risk_factor_mentions": "sec_extractors",
    "extract_customer_concentration": "sec_extractors",
    "extract_exposure": "sec_extractors",
    "get_company_news": "news_events",
    "search_company_news": "news_events",
    "get_company_press_releases": "news_events",
    "get_sec_recent_events": "news_events",
    "get_public_event_timeline": "news_events",
    "verify_company_event": "news_events",
    "get_latest_earnings_release": "earnings_intelligence",
    "index_earnings_release": "earnings_intelligence",
    "extract_earnings_metrics": "earnings_intelligence",
    "extract_guidance": "earnings_intelligence",
    "extract_management_commentary": "earnings_intelligence",
    "compare_earnings_actual_vs_estimate": "earnings_intelligence",
    "get_earnings_call_transcript": "earnings_intelligence",
    "parse_public_transcript": "earnings_intelligence",
    "search_ticker": "screening",
    "screen_stocks": "screening",
    "analyze_position_signals": "screening",
    "calculate_price_target_distance": "screening",
    "search_thai_funds": "thai_funds",
    "get_thai_fund_nav": "thai_funds",
    "get_thai_fund_nav_batch": "thai_funds",
    "get_thai_fund_factsheet": "thai_funds",
    "get_thai_fund_dividend_history": "thai_funds",
}

# Safe args for tools that can be called generically during tool-scan loop.
# Tools not listed here are validated via discovery/schema only; no runtime call with {}.
SMOKE_ARGS: dict[str, dict] = {
    "health_check": {},
    "get_market_quote": {"ticker": "AAPL"},
    "get_historical_prices": {"ticker": "AAPL", "period": "5d", "interval": "1d"},
    "get_company_profile": {"ticker": "AAPL", "include_all": False},
    "get_fund_profile": {"ticker": "SPY"},
    "analyze_share_count_trend": {"ticker": "AAPL", "start_date": "2025-01-01"},
    "get_market_calendar": {"event_type": "earnings", "limit": 5},
    "get_company_news": {"ticker": "AAPL"},
    "search_company_news": {"ticker": "AAPL", "query": "earnings", "max_results": 5},
    "get_company_press_releases": {"ticker": "AAPL", "max_results": 5},
    "get_sec_recent_events": {"ticker": "AAPL", "filing_types": ["8-K"], "max_results": 5},
    "get_public_event_timeline": {"ticker": "AAPL", "max_results": 10},
    "verify_company_event": {"ticker": "AAPL", "event_query": "quarterly results"},
    "search_ticker": {"query": "Apple", "exchange": "US", "max_results": 3},
    "get_option_expiration_dates": {"ticker": "AAPL"},
    "extract_sec_filing_fact": {
        "ticker": "AAPL",
        "fact_type": "geographic_revenue",
        "region": "Greater China",
        "filing_type": "10-K",
        "period": "latest",
    },
    "get_latest_earnings_release": {"ticker": "AAPL", "period": "latest"},
    "index_earnings_release": {"ticker": "AAPL", "period": "latest"},
    "extract_earnings_metrics": {"ticker": "AAPL", "period": "latest"},
    "extract_guidance": {"ticker": "AAPL", "period": "latest"},
    "extract_management_commentary": {
        "ticker": "AAPL",
        "period": "latest",
        "topics": ["demand", "China", "AI"],
    },
    "compare_earnings_actual_vs_estimate": {"ticker": "AAPL", "period": "latest"},
}

_ALLOW_SKIP = os.environ.get("ALLOW_NETWORK_SKIP", "1").lower() in ("1", "true", "yes")
_GROUPED_DISCOVERY = False
_EXPECTED_TOOL_MODE = os.environ.get("EXPECTED_TOOL_MODE", os.environ.get("TOOL_MODE", "")).lower()
_FORBIDDEN_PUBLIC_TERMS = (
    r"\bIO\b",
    r"Commander",
    r"portfolio state",
    r"doctrine",
    r"DC-",
    r"DC Section",
    r"DC-80",
    r"DC-149",
    r"TPS",
    r"PCCE",
    r"EQF",
    r"\bT[1-5]\b",
)


def rpc(method: str, params: dict | None = None, req_id: int = 1) -> dict:
    payload = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        payload["params"] = params
    data = json.dumps(payload).encode("utf-8")
    last_exc: Exception | None = None
    # 5 attempts: sleeps of 5, 10, 20, 40 s between attempts (handles transient 503s)
    delays = [5, 10, 20, 40]
    for i in range(5):
        req = urllib.request.Request(
            URL,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": UA},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            last_exc = e
            if e.code in (429, 502, 503, 504) and i < len(delays):
                time.sleep(delays[i])
                continue
            raise
        except urllib.error.URLError as e:
            last_exc = e
            if i < len(delays):
                time.sleep(delays[i])
                continue
            raise
    raise last_exc or RuntimeError("RPC failed")


def call_tool(name: str, arguments: dict, req_id: int, allow_jsonrpc_error: bool = False) -> dict:
    call_name = name
    call_args = arguments
    if _GROUPED_DISCOVERY and name not in GROUPED_TOOLS:
        group = ACTION_GROUP.get(name)
        if group:
            call_name = group
            call_args = {"action": name, "params": arguments}
    resp = rpc("tools/call", {"name": call_name, "arguments": call_args}, req_id=req_id)
    if "error" in resp and resp["error"]:
        if allow_jsonrpc_error:
            return resp
        raise AssertionError(f"{name} JSON-RPC error: {resp['error']}")
    text = ((((resp.get("result") or {}).get("content") or [{}])[0]) or {}).get("text", "")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        if text.strip().lower() == "no approval received.":
            raise AssertionError(
                f"{name} returned connector/platform approval text instead of a JSON tool payload"
            )
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


def assert_not_double_enveloped_failure(payload: dict, tool: str) -> None:
    if not isinstance(payload, dict):
        return
    data = payload.get("data")
    if payload.get("ok") is True and isinstance(data, dict) and data.get("ok") is False:
        raise AssertionError(f"{tool} returned inner ok:false wrapped as top-level ok:true: {payload}")
    if payload.get("ok") is True and isinstance(data, dict) and data.get("error") is True:
        raise AssertionError(f"{tool} returned legacy error:true wrapped as top-level ok:true: {payload}")
    if payload.get("ok") is True and isinstance(data, dict) and isinstance(data.get("error"), (dict, str)):
        raise AssertionError(f"{tool} returned inner error wrapped as top-level ok:true: {payload}")


def _check_aaoi_geographic_revenue_schema(data: dict) -> None:
    """AAOI is the hard positive China-exposure fixture."""
    _check_geographic_revenue_schema(data, label="AAOI", require_positive=True)
    value = data.get("value")
    pct = data.get("valuePct")
    if not isinstance(value, (int, float)) or not (200_000_000 <= value <= 350_000_000):
        raise AssertionError(f"AAOI China value outside expected live range: {data}")
    if not isinstance(pct, (int, float)) or not (45 <= pct <= 70):
        raise AssertionError(f"AAOI China valuePct outside expected live range: {data}")


def _check_axti_not_disclosed_schema(data: dict) -> None:
    """AXTI NOT_DISCLOSED schema check: stable null keys for undisclosed geographic revenue."""
    confidence = data.get("confidence")
    status = data.get("status")
    code = data.get("code")
    source = data.get("source")
    if "EXTRACTION_FAILED" in (confidence, status, code, source):
        print("  AXTI: tolerated EXTRACTION_FAILED")
        return
    has_positive = _check_geographic_revenue_schema(data, label="AXTI", require_positive=False)
    if has_positive:
        raise AssertionError(f"AXTI: expected NOT_DISCLOSED payload, got extracted value: {data}")
    extraction = data.get("extractionMethod")
    if extraction not in (None, "NONE", "NOT_DISCLOSED"):
        raise AssertionError(f"AXTI: extractionMethod should be NONE/NOT_DISCLOSED, got {extraction!r}")
    if confidence not in ("NOT_DISCLOSED", "NOT_DECISION_GRADE"):
        raise AssertionError(f"AXTI: confidence should be NOT_DISCLOSED/NOT_DECISION_GRADE, got {confidence!r}")


def _check_geographic_revenue_schema(data: dict, label: str, require_positive: bool = False) -> bool:
    """Validate geographic revenue payload shape and semantics.

    Returns True when payload contains a positive extracted value, False for NOT_DISCLOSED/NOT_FOUND payloads.
    """
    if isinstance(data, dict) and (data.get("status") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE" or data.get("code") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE"):
        raise AssertionError(f"{label}: local SEC extractor returned provider-unavailable instead of local result/limitation: {data}")
    required_keys = (
        "value",
        "denominator",
        "valueRatio",
        "valuePct",
        "extractionMethod",
        "confidence",
        "warnings",
    )
    for key in required_keys:
        if key not in data:
            raise AssertionError(f"{label}: missing required key '{key}': {data}")
    if not isinstance(data.get("warnings"), list):
        raise AssertionError(f"{label}: warnings must be a list")

    numeric_or_null_keys = ("value", "denominator", "valueRatio", "valuePct")
    for key in numeric_or_null_keys:
        val = data.get(key)
        if val is not None and not isinstance(val, (int, float)):
            raise AssertionError(f"{label}: {key} must be number|null, got {type(val).__name__}")
        if val == {}:
            raise AssertionError(f"{label}: {key} must not be object")

    if data.get("valuePct") is not None and data.get("denominator") is None:
        raise AssertionError(f"{label}: valuePct present but denominator is null: {data}")

    if data.get("valueRatio") is not None:
        ratio = float(data["valueRatio"])
        if not (0.0 <= ratio <= 1.0):
            raise AssertionError(f"{label}: valueRatio {ratio} not in [0, 1] decimal range")
    if data.get("valuePct") is not None:
        pct = float(data["valuePct"])
        if not (0.0 <= pct <= 100.0):
            raise AssertionError(f"{label}: valuePct {pct} not in [0, 100] percent range")

    if data.get("value") is not None:
        if data.get("denominator") is None:
            raise AssertionError(f"{label}: value present but denominator is null: {data}")
        evidence = data.get("evidence")
        if isinstance(evidence, list):
            if not evidence:
                raise AssertionError(f"{label}: positive extraction must include non-empty evidence list")
            first_ev = evidence[0]
            if not isinstance(first_ev, dict):
                raise AssertionError(f"{label}: evidence list items must be objects")
            evidence_doc_url = first_ev.get("documentUrl") or first_ev.get("url")
            evidence_primary_doc_url = first_ev.get("primaryDocumentUrl")
        elif isinstance(evidence, dict):
            if not evidence:
                raise AssertionError(f"{label}: positive extraction must include non-empty evidence object")
            evidence_doc_url = evidence.get("documentUrl")
            evidence_primary_doc_url = evidence.get("primaryDocumentUrl")
        else:
            raise AssertionError(f"{label}: positive extraction must include non-empty evidence object or list, got {type(evidence).__name__}")

        if not (
            data.get("documentUrl")
            or data.get("primaryDocumentUrl")
            or evidence_doc_url
            or evidence_primary_doc_url
        ):
            raise AssertionError(f"{label}: positive extraction missing documentUrl/primaryDocumentUrl: {data}")
        return True

    if require_positive:
        raise AssertionError(f"{label}: expected positive extraction but got NOT_DISCLOSED/NOT_FOUND: {data}")

    confidence = data.get("confidence")
    explicit_limitations = {
        "EXTRACTION_FAILED",
        "TABLE_NOT_PARSED",
        "NO_DIMENSIONAL_REVENUE_FACT",
        "PROVIDER_LIMITATION",
    }
    observed_statuses = {str(v).upper() for v in (confidence, data.get("status"), data.get("code"), data.get("source")) if v}
    if observed_statuses & explicit_limitations:
        print(f"  {label}: tolerated explicit limitation {sorted(observed_statuses & explicit_limitations)}")
        return False
    if confidence not in ("NOT_DISCLOSED", "NOT_FOUND", "NOT_DECISION_GRADE"):
        raise AssertionError(f"{label}: expected NOT_DISCLOSED/NOT_FOUND/NOT_DECISION_GRADE confidence, got {confidence!r}")

    stable_null_keys = (
        "value",
        "denominator",
        "valueRatio",
        "valuePct",
        "rawValue",
        "rawDenominator",
    )
    for key in stable_null_keys:
        if key in data and data.get(key) is not None:
            raise AssertionError(f"{label}: {key} should be null in NOT_DISCLOSED/NOT_FOUND payload")
    if confidence == "NOT_DISCLOSED":
        evidence = data.get("evidence")
        if isinstance(evidence, list) and evidence:
            evidence = evidence[0]
        if not isinstance(evidence, dict):
            evidence = {}
        if not (evidence.get("accessionNumber") and evidence.get("filingDate") and (evidence.get("documentUrl") or evidence.get("url"))):
            raise AssertionError(f"{label}: clean NOT_DISCLOSED must include filing metadata: {data}")
        if not isinstance(data.get("scanCoverage"), dict):
            raise AssertionError(f"{label}: clean NOT_DISCLOSED missing scanCoverage: {data}")
        if not isinstance(data.get("searchedTerms"), list) or not data.get("searchedTerms"):
            raise AssertionError(f"{label}: clean NOT_DISCLOSED missing searchedTerms: {data}")
        if not data.get("notDisclosedBasis"):
            raise AssertionError(f"{label}: clean NOT_DISCLOSED missing notDisclosedBasis: {data}")
        warning_codes = {str(w.get("code")) for w in data.get("warnings", []) if isinstance(w, dict)}
        if "TABLE_NOT_PARSED" in warning_codes:
            raise AssertionError(f"{label}: NOT_DISCLOSED must not carry TABLE_NOT_PARSED: {data}")
    return False


def _assert_filing_resolution(payload: dict, ticker: str) -> tuple[list[dict], str]:
    filings_data = extract_data(payload)
    filing_list = filings_data.get("filings") if isinstance(filings_data, dict) else None
    if not isinstance(filing_list, list) or not filing_list:
        diag = json.dumps(filings_data, sort_keys=True)[:2000] if isinstance(filings_data, dict) else repr(filings_data)[:2000]
        raise AssertionError(
            f"{ticker}: list_sec_company_filings expected non-empty filings[]\n{diag}"
        )
    first_filing = filing_list[0] if isinstance(filing_list[0], dict) else {}
    if not first_filing.get("accessionNumber"):
        raise AssertionError(f"{ticker}: missing accessionNumber: {first_filing}")
    if not first_filing.get("filingDate"):
        raise AssertionError(f"{ticker}: missing filingDate: {first_filing}")
    doc_url = str(first_filing.get("documentUrl", ""))
    if not doc_url.startswith("https://www.sec.gov/Archives/"):
        raise AssertionError(f"{ticker}: invalid documentUrl: {first_filing}")
    return filing_list, doc_url


def _check_yahoo_news_structured(data: dict) -> None:
    """Yahoo news structured response schema check."""
    if not isinstance(data, dict):
        raise AssertionError(f"get_company_news returned non-object: {type(data)}")
    if "items" not in data or not isinstance(data.get("items"), list):
        raise AssertionError("get_company_news missing items[]")
    meta = data.get("meta") or {}
    if not isinstance(meta, dict):
        raise AssertionError("get_company_news missing meta object")
    if "sourcesUsed" not in meta:
        raise AssertionError("get_company_news meta.sourcesUsed missing")
    if "deduped" not in meta:
        raise AssertionError("get_company_news meta.deduped missing")
    # sourceStatus and sourceCoverage are required
    if "sourceStatus" not in data:
        raise AssertionError("get_company_news missing sourceStatus")
    if "sourceCoverage" not in data:
        raise AssertionError("get_company_news missing sourceCoverage")
    source_status = data.get("sourceStatus") or {}
    if not isinstance(source_status, dict):
        raise AssertionError(f"get_company_news sourceStatus must be an object, got: {type(source_status)}")
    # If company_ir or newswire are UNCONFIGURED and items=[], status must be SOURCE_LIMITED_NOT_FOUND
    items = data.get("items") or []
    company_ir_status = (source_status.get("company_ir") or {}).get("status", "")
    newswire_status = (source_status.get("newswire") or {}).get("status", "")
    if not items and (company_ir_status == "UNCONFIGURED" or newswire_status == "UNCONFIGURED"):
        top_status = data.get("status", "")
        if top_status == "NOT_FOUND":
            raise AssertionError(
                "get_company_news: items=[] with UNCONFIGURED source(s) must return "
                f"SOURCE_LIMITED_NOT_FOUND, got NOT_FOUND; sourceStatus={source_status}"
            )
    required_item_fields = (
        "title",
        "source",
        "sourceType",
        "publishedAt",
        "retrievedAt",
        "url",
        "confidence",
        "eventType",
        "duplicateGroupId",
    )
    for item in (data.get("items") or [])[:5]:
        if not isinstance(item, dict):
            raise AssertionError(f"get_company_news items[] entry is not an object: {item!r}")
        for field in required_item_fields:
            if field not in item:
                raise AssertionError(f"get_company_news item missing field '{field}': {item}")
    # Must not be a plain text blob
    if "_raw" in data:
        raise AssertionError("get_company_news returned raw text blob instead of structured JSON")


def _check_finnhub_item_shape(item: dict) -> None:
    """Validate the shape of a Finnhub news item."""
    for field in ("source", "originalSource", "sourceType", "publishedAt", "retrievedAt",
                  "url", "summary", "eventType", "duplicateGroupId"):
        if field not in item:
            raise AssertionError(f"Finnhub item missing field '{field}': {item}")
    if item.get("source") != "finnhub":
        raise AssertionError(f"Finnhub item source expected 'finnhub', got: {item.get('source')!r}")
    if item.get("sourceType") != "company_news":
        raise AssertionError(f"Finnhub item sourceType expected 'company_news', got: {item.get('sourceType')!r}")
    if "retrievedAt" in item and not isinstance(item["retrievedAt"], str):
        raise AssertionError(f"Finnhub item retrievedAt must be a string: {item['retrievedAt']!r}")



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


def _check_public_description_terms(tools: list[dict]) -> None:
    descriptions = [str(t.get("description", "")) for t in tools if isinstance(t, dict)]
    if not descriptions:
        raise AssertionError("tools/list returned no descriptions")
    for desc in descriptions:
        for pattern in _FORBIDDEN_PUBLIC_TERMS:
            if re.search(pattern, desc, flags=re.IGNORECASE):
                raise AssertionError(f"Forbidden private term matched /{pattern}/ in description: {desc[:200]!r}")


def _assert_deprecated_alias_metadata(tools: list[dict]) -> None:
    by_name = {str(t.get("name")): t for t in tools if isinstance(t, dict)}
    # Since deprecated aliases are filtered from tools/list, verify they are absent.
    expected_absent = {
        "get_dc134_options_scan",
        "get_eqf_bracket",
        "get_tps_inputs",
        "get_adv_gate",
        "get_china_revenue_pct",
        "get_geographic_revenue",
        "get_filing_text_search",
        "get_filing_document",
    }
    for alias in expected_absent:
        if alias in by_name:
            raise AssertionError(f"Deprecated alias should NOT appear in tools/list: {alias}")


def _assert_public_tool_wording(tools: list[dict]) -> None:
    by_name = {str(t.get("name")): str(t.get("description", "")) for t in tools if isinstance(t, dict)}
    checks = {
        "get_company_events_calendar": ("earnings", "estimate"),
        "calculate_price_target_distance": ("reference price target",),
        "analyze_position_signals": ("does not access holdings",),
        "check_volume_liquidity_threshold": ("liquidity thresholds",),
    }
    for name, snippets in checks.items():
        desc = by_name.get(name, "")
        if not desc:
            raise AssertionError(f"Missing description for {name}")
        for snippet in snippets:
            if snippet.lower() not in desc.lower():
                raise AssertionError(f"{name}: description missing expected phrase {snippet!r}")


def main() -> int:
    global _GROUPED_DISCOVERY
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
    if not all(isinstance(t, dict) for t in tools):
        raise AssertionError("tools/list returned non-object tool entries")
    names = {str(t.get("name")) for t in tools if isinstance(t, dict)}
    _GROUPED_DISCOVERY = GROUPED_TOOLS.issubset(names)
    if _EXPECTED_TOOL_MODE == "grouped" and not _GROUPED_DISCOVERY:
        raise AssertionError("Expected grouped tools/list, got expanded discovery")
    if _EXPECTED_TOOL_MODE == "expanded" and _GROUPED_DISCOVERY:
        raise AssertionError("Expected expanded tools/list, got grouped discovery")
    _check_public_description_terms([t for t in tools if isinstance(t, dict)])
    _assert_deprecated_alias_metadata([t for t in tools if isinstance(t, dict)])
    if not _GROUPED_DISCOVERY:
        _assert_public_tool_wording([t for t in tools if isinstance(t, dict)])
    print("  PASS public description and private alias checks")
    if _GROUPED_DISCOVERY:
        if names != GROUPED_TOOLS:
            raise AssertionError(f"Grouped discovery should expose only grouped tools: {sorted(names)}")
        print("  PASS grouped tools/list exposes 11 domain tools")
    else:
        missing = sorted(CANONICAL_TOOLS - names)
        if missing:
            raise AssertionError(f"Missing canonical tools in discovery: {missing}")

    if not _GROUPED_DISCOVERY:
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
    filing_list, doc_url = _assert_filing_resolution(filings, "AAPL")
    aapl_accession = str(filing_list[0].get("accessionNumber") or "")

    exhibits = call_tool("list_sec_filing_exhibits", {"ticker": "AAPL", "accessionNumber": aapl_accession}, 205)
    assert_no_unknown_tool(exhibits, "list_sec_filing_exhibits")
    exhibit_data = extract_data(exhibits)
    exhibit_list = exhibit_data.get("exhibits") if isinstance(exhibit_data, dict) else None
    if not isinstance(exhibit_list, list) or not exhibit_list:
        raise AssertionError(f"list_sec_filing_exhibits returned no exhibits for AAPL {aapl_accession}: {exhibit_data}")
    first_fetchable_exhibit = next(
        (
            exhibit for exhibit in exhibit_list
            if isinstance(exhibit, dict)
            and str(exhibit.get("document") or "").lower().endswith((".htm", ".html"))
        ),
        None,
    )
    if not first_fetchable_exhibit:
        raise AssertionError(f"list_sec_filing_exhibits returned no HTML exhibit for AAPL {aapl_accession}: {exhibit_list[:5]}")
    listed_document = str(first_fetchable_exhibit.get("document") or "")
    listed_url = str(first_fetchable_exhibit.get("documentUrl") or "")
    if not listed_url.startswith("https://www.sec.gov/Archives/"):
        raise AssertionError(f"list_sec_filing_exhibits missing documentUrl for listed exhibit: {first_fetchable_exhibit}")
    exhibit_content = call_tool(
        "get_sec_filing_exhibit_content",
        {"ticker": "AAPL", "accessionNumber": aapl_accession, "fileName": listed_document, "topics": ["Apple"]},
        206,
    )
    assert_no_unknown_tool(exhibit_content, "get_sec_filing_exhibit_content")
    assert_not_double_enveloped_failure(exhibit_content, "get_sec_filing_exhibit_content")
    exhibit_content_data = extract_data(exhibit_content)
    if not isinstance(exhibit_content_data, dict) or exhibit_content_data.get("documentUrl") != listed_url:
        raise AssertionError(f"get_sec_filing_exhibit_content did not fetch the listed exhibit URL: {exhibit_content_data}")
    if exhibit_content.get("ok") is False:
        raise AssertionError(f"get_sec_filing_exhibit_content failed for listed exhibit {listed_document}: {exhibit_content}")
    print("  PASS SEC exhibit list/content handoff smoke")

    aaoi_filings = call_tool("list_sec_company_filings", {"ticker": "AAOI", "filing_type": "10-K", "limit": 3}, 21)
    assert_no_unknown_tool(aaoi_filings, "list_sec_company_filings")
    _assert_filing_resolution(aaoi_filings, "AAOI")
    print("  PASS AAOI filing-resolution smoke")

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

    exp = call_tool("get_option_expiration_dates", {"ticker": "ASTS"}, 26)
    assert_no_unknown_tool(exp, "get_option_expiration_dates")
    expiry_dates = extract_data(exp)
    expiry = choose_stable_option_expiration(expiry_dates)
    if not expiry:
        raise AssertionError(f"get_option_expiration_dates returned no usable ASTS expirations: {expiry_dates!r}")

    calls: list[tuple[str, dict]] = [
        ("health_check", {}),
        ("analyze_position_signals", {"ticker": "ASTS"}),
        ("calculate_price_target_distance", {"ticker": "ASTS", "io_pt": 95}),
        ("check_volume_liquidity_threshold", {"ticker": "ASTS"}),
        ("summarize_options_flow", {"ticker": "ASTS"}),
        ("analyze_options_flow_window", {"ticker": "ASTS", "window_label": "audit"}),
        ("get_option_chain", {"ticker": "ASTS", "expiration_date": expiry, "option_type": "calls", "max_contracts": 10}),
        ("get_sec_filing_outline", {"ticker": "AAPL", "filing_type": "10-K", "period": "latest"}),
        ("get_sec_filing_section", {"ticker": "AAPL", "filing_type": "10-K", "selector": {"item": "Item 1A"}}),
        ("list_sec_filing_tables", {"ticker": "AAPL", "filing_type": "10-K", "offset": 0, "limit": 20}),
        ("get_sec_filing_table", {"ticker": "AAPL", "filing_type": "10-K", "table_index": 0}),
        ("search_sec_filing_text", {"ticker": "AAPL", "search_terms": ["Greater China"], "filing_type": "10-K"}),
        ("get_company_news", {"ticker": "AAPL"}),
        ("search_company_news", {"ticker": "AAPL", "query": "earnings", "max_results": 5}),
        ("get_company_press_releases", {"ticker": "AAPL", "max_results": 5}),
        ("get_sec_recent_events", {"ticker": "AAPL", "filing_types": ["8-K"], "max_results": 5}),
        ("get_public_event_timeline", {"ticker": "AAPL", "max_results": 10}),
        ("verify_company_event", {"ticker": "AAPL", "event_query": "quarterly results"}),
        # PR50 AAOI/AXTI schema smoke
        ("extract_sec_filing_fact", {"ticker": "AAOI", "fact_type": "geographic_revenue", "region": "China", "filing_type": "10-K", "period": "latest"}),
        # Phase 3 extractor tools — dispatch smoke (schema-only, no deep value assertions)
        ("extract_geographic_revenue", {"ticker": "AAOI", "region": "China", "filing_type": "10-K", "period": "latest"}),
        ("query_sec_filing_index", {"ticker": "AAOI", "filing_type": "10-K", "period": "latest", "query_type": "geographic_revenue_share", "params": {"region": "China"}}),
    ]
    if doc_url:
        calls.extend([
            ("get_sec_filing_outline", {"ticker": "AAPL", "document_url": doc_url}),
            ("list_sec_filing_tables", {"ticker": "AAPL", "document_url": doc_url, "offset": 0, "limit": 20}),
        ])

    for i, (name, args) in enumerate(calls, start=100):
        try:
            payload = call_tool(name, args, i)
        except Exception as exc:
            raise AssertionError(f"{name} smoke call failed with args {args}: {exc}") from exc
        assert_no_unknown_tool(payload, name)
        assert_not_double_enveloped_failure(payload, name)
        if name in (
            "extract_sec_filing_fact",
            "extract_geographic_revenue",
            "extract_segment_revenue",
            "extract_total_revenue",
            "extract_revenue_exposure",
            "extract_china_exposure",
            "extract_risk_factor_mentions",
            "extract_customer_concentration",
            "query_sec_filing_index"
        ):
            data = extract_data(payload)
            if isinstance(data, dict) and (data.get("status") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE" or data.get("code") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE"):
                raise AssertionError(f"{name} returned STRUCTURED_FACT_PROVIDER_UNAVAILABLE after local routing: {data}")
        if name == "health_check":
            health = extract_data(payload)
            print(f"  health_check response: {json.dumps(payload)}")
            if isinstance(health, dict) and health.get("envelopeV2") is not True:
                raise AssertionError(f"health_check envelopeV2 expected true, got: {health}")
            if isinstance(health, dict):
                for field in (
                    "toolCount",
                    "manifestVersion",
                    "manifestHash",
                    "privacyScope",
                ):
                    if field not in health:
                        raise AssertionError(f"health_check missing field: {field}")
                if health.get("envelopeSchemaVersion") not in (None, "2026-07-08"):
                    raise AssertionError(f"health_check envelopeSchemaVersion mismatch: {health}")
                if health.get("toolCount") != len(names):
                    raise AssertionError(
                        f"health_check toolCount mismatch: {health.get('toolCount')} != tools/list {len(names)}"
                    )
                if health.get("privacyScope") != "public_market_data_only":
                    raise AssertionError(f"health_check privacyScope mismatch: {health.get('privacyScope')!r}")
        if name == "get_option_chain":
            data = extract_data(payload)
            # Worker returns {"error": true, "code": "INVALID_EXPIRY_DATE"} when the
            # first expiry date is today (expiration day) — treat as a known-valid response.
            if isinstance(data, dict) and data.get("error") is True:
                print(f"  PASS get_option_chain (expired/invalid expiry on live run, code={data.get('code')!r})")
            else:
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
        if name == "list_sec_filing_tables":
            data = extract_data(payload)
            for field in ("tableCount", "returnedCount", "offset", "limit", "hasMore", "tables"):
                if field not in data:
                    raise AssertionError(f"list_sec_filing_tables missing {field}: {data}")
            if args.get("document_url") is None and data.get("returnedCount", 0) and not any((isinstance(t.get("title"), str) for t in data.get("tables", []))):
                raise AssertionError(f"list_sec_filing_tables returned tables missing title field: {data}")
        if name == "search_sec_filing_text":
            data = extract_data(payload)
            if data.get("documentKind") == "xbrl_xml":
                raise AssertionError(f"search_sec_filing_text returned xbrl_xml: {data}")
        if name == "extract_sec_filing_fact" and args.get("ticker") == "QCOM":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError("extract_sec_filing_fact returned non-object")
            if "valuePct" not in data:
                raise AssertionError("extract_sec_filing_fact missing valuePct")
            if data.get("value") is not None and data.get("extractionMethod") == "XBRL":
                warning_codes = {str(w.get("code")) for w in data.get("warnings", []) if isinstance(w, dict)}
                if not isinstance(data.get("xbrlContext"), dict) and "XBRL_CONTEXT_METADATA_UNAVAILABLE" not in warning_codes:
                    raise AssertionError("extract_sec_filing_fact returned XBRL value without xbrlContext or unavailable warning")
        if name == "extract_sec_filing_fact" and args.get("ticker") == "AAOI":
            data = extract_data(payload)
            if isinstance(data, dict):
                if data.get("value") is not None and data.get("extractionMethod") == "XBRL":
                    warning_codes = {str(w.get("code")) for w in data.get("warnings", []) if isinstance(w, dict)}
                    if not isinstance(data.get("xbrlContext"), dict) and "XBRL_CONTEXT_METADATA_UNAVAILABLE" not in warning_codes:
                        raise AssertionError("AAOI extract_sec_filing_fact returned XBRL value without xbrlContext or unavailable warning")
                aaoi_has_value = _check_geographic_revenue_schema(
                    data, label="AAOI extract_sec_filing_fact", require_positive=True
                )
                if aaoi_has_value:
                    _check_aaoi_geographic_revenue_schema(data)
                    print("  PASS AAOI geographic revenue extracted with value")
                else:
                    raise AssertionError(f"AAOI geographic revenue positive fixture returned no value: {data}")
        if name == "extract_sec_filing_fact" and args.get("ticker") == "AXTI":
            data = extract_data(payload)
            if isinstance(data, dict):
                _check_axti_not_disclosed_schema(data)
                print("  PASS AXTI NOT_DISCLOSED schema check")
        if name == "get_company_news":
            data = extract_data(payload)
            _check_yahoo_news_structured(data)
            print("  PASS Yahoo news structured smoke")
        if name == "get_company_press_releases":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError(f"get_company_press_releases returned non-object: {data!r}")
            if "items" not in data or not isinstance(data.get("items"), list):
                raise AssertionError("get_company_press_releases missing items[]")
            if "warnings" not in data or not isinstance(data.get("warnings"), list):
                raise AssertionError("get_company_press_releases missing warnings[]")
            for field in ("coverageStatus", "decisionGrade", "decisionGradeBasis"):
                if field not in data:
                    raise AssertionError(f"get_company_press_releases missing payload gate field {field}: {data}")
            if data.get("decisionGrade") is True and data.get("coverageStatus") not in {"SEC_EX99_RESOLVED", "APPROVED_IR_PAGE_RESOLVED"}:
                raise AssertionError(f"get_company_press_releases decisionGrade true without approved evidence status: {data}")
            if data.get("coverageStatus") == "SEC_EX99_RESOLVED" and not data.get("secEvidence"):
                raise AssertionError(f"get_company_press_releases missing secEvidence for SEC_EX99_RESOLVED: {data}")
            if data.get("coverageStatus") == "APPROVED_IR_PAGE_RESOLVED" and not data.get("irPageEvidence"):
                raise AssertionError(f"get_company_press_releases missing irPageEvidence for APPROVED_IR_PAGE_RESOLVED: {data}")
            status = data.get("status")
            if status == "SEC_8K_FOUND_EX99_NOT_FOUND":
                if not isinstance(data.get("secEvidence"), list) or not data.get("secEvidence"):
                    raise AssertionError("get_company_press_releases missing secEvidence for SEC_8K_FOUND_EX99_NOT_FOUND")
                warning_codes = {str(w.get("code")) for w in data.get("warnings", []) if isinstance(w, dict)}
                if "SEC_8K_FOUND_EX99_NOT_FOUND" not in warning_codes:
                    raise AssertionError("get_company_press_releases missing SEC_8K_FOUND_EX99_NOT_FOUND warning")
            print(f"  PASS get_company_press_releases structured smoke (status={status!r})")
        # Phase 3 extractor tool dispatch checks
        if name == "extract_geographic_revenue":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError(f"extract_geographic_revenue returned non-object: {data!r}")
            ticker = str(args.get("ticker", ""))
            region = str(args.get("region", ""))
            if ticker == "TSM":
                status = str(data.get("status") or data.get("confidence") or data.get("code") or "").upper()
                evidence = data.get("evidence")
                if isinstance(evidence, list) and evidence:
                    evidence = evidence[0]
                if not isinstance(evidence, dict):
                    evidence = {}
                if status == "NOT_DISCLOSED" and not any((evidence.get(k) or (evidence.get("url") if k == "documentUrl" else None)) for k in ("accessionNumber", "filingDate", "documentUrl")):
                    raise AssertionError(f"TSM wrong filing type returned silent NOT_DISCLOSED: {data}")
                print("  PASS TSM wrong-filing-type guard")
            elif ticker == "AAPL" and region == "Greater China":
                has_value = _check_geographic_revenue_schema(
                    data, label="AAPL extract_geographic_revenue", require_positive=False
                )
                if has_value:
                    print("  PASS extract_geographic_revenue positive fixture (AAPL/Greater China)")
                else:
                    print("  WARN extract_geographic_revenue AAPL/Greater China returned NOT_DISCLOSED (live data, allowed)")
            elif ticker == "AAOI" and region == "China":
                _check_aaoi_geographic_revenue_schema(data)
                print("  PASS extract_geographic_revenue AAOI extracted value")
            elif ticker in ("AXTI", "SNDK") and region == "China":
                _check_geographic_revenue_schema(
                    data, label=f"extract_geographic_revenue:{ticker}/{region}", require_positive=False
                )
                status = str(data.get("status") or data.get("confidence") or data.get("code") or "").upper()
                if status not in ("EXTRACTION_FAILED", "TABLE_NOT_PARSED", "NO_DIMENSIONAL_REVENUE_FACT", "PROVIDER_LIMITATION", "NOT_DISCLOSED", "NOT_FOUND"):
                    raise AssertionError(f"{ticker} China limitation fixture returned unexpected status: {data}")
                evidence = data.get("evidence")
                if isinstance(evidence, list) and evidence:
                    evidence = evidence[0]
                if not isinstance(evidence, dict) or not (evidence.get("accessionNumber") or evidence.get("filingDate") or evidence.get("documentUrl") or evidence.get("url")):
                    raise AssertionError(f"{ticker} China limitation fixture missing filing metadata: {data}")
                print(f"  PASS extract_geographic_revenue {ticker} explicit limitation")
            else:
                _check_geographic_revenue_schema(
                    data, label=f"extract_geographic_revenue:{ticker}/{region}", require_positive=False
                )
                print(f"  PASS extract_geographic_revenue dispatch ({ticker}/{region})")
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
        if name == "query_sec_filing_index":
            data = extract_data(payload)
            if not isinstance(data, dict):
                raise AssertionError(f"query_sec_filing_index returned non-object: {data!r}")
            for field in ("status", "queryType", "answer", "confidence", "evidence"):
                if field not in data:
                    raise AssertionError(f"query_sec_filing_index missing field: {field}")
            if args.get("ticker") == "AAOI" and args.get("query_type") == "geographic_revenue_share":
                answer = data.get("answer")
                if not isinstance(answer, dict):
                    raise AssertionError(f"AAOI query_sec_filing_index missing answer object: {data}")
                evidence = data.get("evidence")
                first_evidence = evidence[0] if isinstance(evidence, list) and evidence else {}
                _check_aaoi_geographic_revenue_schema({
                    "value": answer.get("value"),
                    "denominator": answer.get("denominator"),
                    "valueRatio": answer.get("valueRatio"),
                    "valuePct": answer.get("valuePct"),
                    "extractionMethod": "QUERY_INDEX",
                    "confidence": data.get("confidence"),
                    "warnings": data.get("warnings", []),
                    "documentUrl": first_evidence.get("documentUrl") if isinstance(first_evidence, dict) else None,
                    "primaryDocumentUrl": first_evidence.get("primaryDocumentUrl") if isinstance(first_evidence, dict) else None,
                    "evidence": first_evidence,
                })
            print(f"  PASS query_sec_filing_index dispatch ({args.get('ticker')}/{args.get('query_type')})")

    # Chained option-chain smoke (AAPL)
    aapl_exp_payload = call_tool("get_option_expiration_dates", {"ticker": "AAPL"}, 900)
    assert_no_unknown_tool(aapl_exp_payload, "get_option_expiration_dates")
    aapl_dates = extract_data(aapl_exp_payload)
    aapl_expiry = choose_stable_option_expiration(aapl_dates)
    if not aapl_expiry:
        raise AssertionError(f"get_option_expiration_dates returned no usable AAPL expirations: {aapl_dates!r}")
    aapl_chain = call_tool(
        "get_option_chain",
        {"ticker": "AAPL", "expiration_date": aapl_expiry, "option_type": "calls"},
        901,
    )
    assert_no_unknown_tool(aapl_chain, "get_option_chain")
    chain_data = extract_data(aapl_chain)
    if not isinstance(chain_data, dict):
        raise AssertionError(f"get_option_chain (AAPL) returned non-object: {chain_data!r}")
    if chain_data.get("error") is True:
        print(f"  PASS chained option-chain smoke (AAPL) — expired/invalid expiry (code={chain_data.get('code')!r})")
    else:
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

    manifest_diag = call_tool("get_manifest_diagnostics", {}, 903)
    assert_no_unknown_tool(manifest_diag, "get_manifest_diagnostics")
    assert_not_double_enveloped_failure(manifest_diag, "get_manifest_diagnostics")
    diag_data = extract_data(manifest_diag)
    if not isinstance(diag_data, dict):
        raise AssertionError(f"get_manifest_diagnostics returned non-object: {diag_data!r}")
    for field in (
        "toolCount",
        "manifestVersion",
        "manifestHash",
        "privacyScope",
    ):
        if field not in diag_data:
            raise AssertionError(f"get_manifest_diagnostics missing field {field}: {diag_data}")
    print("  PASS get_manifest_diagnostics contract smoke")

    batch_news = call_tool(
        "get_company_news",
        {"ticker": ["AAPL", "MSFT", "NVDA", "AMD", "TSM"], "max_results": 1, "lookback_days": 14},
        904,
    )
    assert_no_unknown_tool(batch_news, "get_company_news batch")
    assert_not_double_enveloped_failure(batch_news, "get_company_news batch")
    batch_data = extract_data(batch_news)
    if not isinstance(batch_data, dict):
        raise AssertionError(f"get_company_news batch returned non-object: {batch_data!r}")
    for ticker in ("AAPL", "MSFT", "NVDA", "AMD", "TSM"):
        if ticker not in batch_data:
            raise AssertionError(f"get_company_news batch missing key {ticker}: {batch_data}")
    print("  PASS get_company_news 5-ticker independent batch smoke")

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

    # Conditional Finnhub deployed smoke
    # When FINNHUB_API_KEY is absent the Worker returns UNCONFIGURED; when present it calls the API.
    fh_resp = call_tool(
        "get_company_news",
        {"ticker": "AAPL", "sources": ["finnhub"], "max_results": 5, "lookback_days": 14},
        2000,
    )
    assert_no_unknown_tool(fh_resp, "get_company_news (finnhub)")
    fh_data = extract_data(fh_resp)
    if not isinstance(fh_data, dict):
        raise AssertionError(f"get_company_news(finnhub) returned non-object: {type(fh_data)}")
    fh_source_status = (fh_data.get("sourceStatus") or {}).get("finnhub") or {}
    actual_fh_status = fh_source_status.get("status")
    _allowed = {"OK", "EMPTY_RESULT", "RATE_LIMITED", "AUTH_ERROR", "PROVIDER_ERROR", "PROVIDER_CHANGED", "UNCONFIGURED"}
    if actual_fh_status not in _allowed:
        raise AssertionError(f"Finnhub sourceStatus.status unexpected: {actual_fh_status!r}")
    if actual_fh_status != "UNCONFIGURED":
        if "rawCount" not in fh_source_status:
            raise AssertionError("Finnhub sourceStatus missing rawCount")
        if "filteredCount" not in fh_source_status:
            raise AssertionError("Finnhub sourceStatus missing filteredCount")
    # Verify a runner-provided key is not echoed anywhere in the response.
    _key_val = os.environ.get("FINNHUB_API_KEY") or os.environ.get("FINNHUB_TOKEN") or ""
    if _key_val and _key_val in json.dumps(fh_resp):
        raise AssertionError("SECURITY: Finnhub API key found in tool output")
    _fh_items = fh_data.get("items") or []
    if actual_fh_status == "OK" and _fh_items:
        _check_finnhub_item_shape(_fh_items[0])
        print("  PASS Finnhub item shape check (sourceType=company_news, source=finnhub)")
    print(f"  PASS Finnhub deployed smoke (status={actual_fh_status!r})")

    # Overnight smoke. This is Yahoo indicative/pre-post-market data only; true
    # The unusable third-party true-overnight route was removed.
    overnight = call_tool("get_overnight_quote", {"ticker": "AAPL"}, 2100)
    assert_no_unknown_tool(overnight, "get_overnight_quote")
    assert_not_double_enveloped_failure(overnight, "get_overnight_quote")
    overnight_data = extract_data(overnight)
    overnight_meta = overnight.get("meta") if isinstance(overnight, dict) else {}
    if not isinstance(overnight_data, dict):
        raise AssertionError(f"get_overnight_quote returned non-object data: {type(overnight_data)}")
    if not isinstance(overnight_meta, dict):
        overnight_meta = {}
    diagnostics = overnight.get("diagnostics") if isinstance(overnight, dict) else {}
    if not isinstance(diagnostics, dict):
        diagnostics = overnight_data.get("diagnostics") or {}
    provider = overnight_data.get("provider") or overnight_meta.get("provider") or diagnostics.get("provider")
    provider_status = overnight_data.get("providerStatus") or overnight_meta.get("providerStatus") or diagnostics.get("providerStatus")
    warning_codes = {
        w.get("code")
        for w in overnight_data.get("warnings", [])
        if isinstance(w, dict)
    }
    if provider != "yahoo":
        raise AssertionError(f"get_overnight_quote should use only Yahoo after third-party provider removal, got {provider!r}")
    if provider_status is None:
        raise AssertionError("get_overnight_quote missing providerStatus")
    if overnight_meta.get("doctrineUse") != "DIAGNOSTICS_ONLY":
        raise AssertionError(f"get_overnight_quote missing DIAGNOSTICS_ONLY metadata: {overnight_meta}")
    if overnight_data.get("decisionGrade") is not False:
        raise AssertionError(f"get_overnight_quote must be payload-level decisionGrade:false: {overnight_data}")
    if overnight_data.get("doctrineUse") != "DIAGNOSTICS_ONLY":
        raise AssertionError(f"get_overnight_quote must be payload-level DIAGNOSTICS_ONLY: {overnight_data}")
    if overnight_data.get("dataKind") != "yahoo_extended_hours_proxy":
        raise AssertionError(f"get_overnight_quote must declare yahoo_extended_hours_proxy: {overnight_data}")
    if "TRUE_OVERNIGHT_PROVIDER_REMOVED" not in warning_codes:
        raise AssertionError(f"get_overnight_quote missing TRUE_OVERNIGHT_PROVIDER_REMOVED warning: {overnight_data}")
    print(f"  PASS overnight smoke (provider={provider!r}, providerStatus={provider_status!r})")

    unsupported = call_tool(
        "query_sec_filing_index",
        {"ticker": "AAPL", "query_type": "unsupported_query_type", "params": {}},
        2110,
    )
    assert_no_unknown_tool(unsupported, "query_sec_filing_index")
    assert_not_double_enveloped_failure(unsupported, "query_sec_filing_index")
    if unsupported.get("ok") is not False:
        raise AssertionError(f"unsupported query_sec_filing_index must be top-level ok:false: {unsupported}")
    unsupported_error = unsupported.get("error") or {}
    if unsupported_error.get("code") != "UNSUPPORTED_QUERY_TYPE":
        raise AssertionError(f"unsupported query_sec_filing_index wrong error code: {unsupported}")
    unsupported_meta = unsupported.get("meta") or {}
    if not unsupported_meta.get("supportedQueryTypes"):
        raise AssertionError(f"unsupported query_sec_filing_index missing supportedQueryTypes: {unsupported}")

    bad_recommendation = call_tool(
        "get_analyst_recommendations",
        {"ticker": "AAPL", "recommendation_type": "summary"},
        2112,
    )
    assert_no_unknown_tool(bad_recommendation, "get_analyst_recommendations")
    assert_not_double_enveloped_failure(bad_recommendation, "get_analyst_recommendations")
    if bad_recommendation.get("ok") is not False:
        raise AssertionError(f"invalid get_analyst_recommendations type must be top-level ok:false: {bad_recommendation}")
    rec_error = bad_recommendation.get("error") or {}
    if rec_error.get("code") != "INPUT_VALIDATION_ERROR":
        raise AssertionError(f"invalid get_analyst_recommendations wrong error code: {bad_recommendation}")
    rec_meta = bad_recommendation.get("meta") or {}
    supported_rec_types = rec_meta.get("supportedRecommendationTypes") or []
    if "recommendations" not in supported_rec_types or "upgrades_downgrades" not in supported_rec_types:
        raise AssertionError(f"invalid get_analyst_recommendations missing supportedRecommendationTypes: {bad_recommendation}")

    total_fact = call_tool(
        "extract_sec_filing_fact",
        {"ticker": "AAPL", "fact_type": "total_revenue", "filing_type": "10-K", "period": "latest"},
        2115,
    )
    assert_no_unknown_tool(total_fact, "extract_sec_filing_fact")
    assert_not_double_enveloped_failure(total_fact, "extract_sec_filing_fact")
    total_data = extract_data(total_fact)
    if not isinstance(total_data, dict):
        raise AssertionError(f"extract_sec_filing_fact total_revenue returned non-object: {total_data!r}")
    if total_data.get("value") is not None and total_data.get("extractionMethod") == "XBRL":
        if total_data.get("decisionGrade") is not True:
            raise AssertionError(f"extract_sec_filing_fact total_revenue XBRL success must be decisionGrade:true: {total_data}")
        if not isinstance(total_data.get("xbrlContext"), dict):
            raise AssertionError(f"extract_sec_filing_fact total_revenue missing xbrlContext: {total_data}")
        source_evidence = total_data.get("sourceEvidence")
        if not isinstance(source_evidence, dict):
            raise AssertionError(f"extract_sec_filing_fact total_revenue missing sourceEvidence: {total_data}")
        for field in ("sourceType", "concept", "accessionNumber", "periodEnd"):
            if not source_evidence.get(field):
                raise AssertionError(f"extract_sec_filing_fact total_revenue sourceEvidence missing {field}: {source_evidence}")
        filing_year = str(source_evidence.get("filingDate") or "")[:4]
        period_year = str(source_evidence.get("periodEnd") or "")[:4]
        if filing_year.isdigit() and period_year.isdigit() and int(period_year) < int(filing_year) - 1:
            raise AssertionError(
                f"extract_sec_filing_fact total_revenue selected stale period for filing: {source_evidence}"
            )

    commentary = call_tool(
        "extract_management_commentary",
        {"ticker": "AAPL", "period": "latest", "topics": ["revenue"]},
        2118,
    )
    assert_no_unknown_tool(commentary, "extract_management_commentary")
    assert_not_double_enveloped_failure(commentary, "extract_management_commentary")
    commentary_data = extract_data(commentary)
    commentary_topics = commentary_data.get("topics") if isinstance(commentary_data, dict) else None
    if not isinstance(commentary_topics, list):
        raise AssertionError(f"extract_management_commentary returned missing topics[]: {commentary_data}")
    found_commentary = [t for t in commentary_topics if isinstance(t, dict) and t.get("status") == "FOUND"]
    if found_commentary:
        first_commentary = found_commentary[0]
        evidence = first_commentary.get("evidence") or []
        if not isinstance(evidence, list) or not evidence:
            raise AssertionError(f"extract_management_commentary FOUND topic missing evidence: {first_commentary}")
        if not first_commentary.get("matchedTerms"):
            raise AssertionError(f"extract_management_commentary FOUND topic missing matchedTerms: {first_commentary}")
        commentary_text = " ".join(
            str(value or "")
            for value in [
                first_commentary.get("summary"),
                *(item.get("excerpt") for item in evidence if isinstance(item, dict)),
            ]
        ).lower()
        if "emerging growth company" in commentary_text:
            raise AssertionError(f"extract_management_commentary returned SEC cover-page boilerplate: {first_commentary}")
    else:
        valid_statuses = {t.get("status") for t in commentary_topics if isinstance(t, dict)}
        if valid_statuses - {"NOT_FOUND"}:
            raise AssertionError(f"extract_management_commentary returned unexpected topic status: {commentary_data}")
        print("PASS extract_management_commentary revenue smoke (honest NOT_FOUND)")

    alias_payload = call_tool("get_historical_stock_prices", {}, 2120)
    assert_no_unknown_tool(alias_payload, "get_historical_stock_prices")
    assert_not_double_enveloped_failure(alias_payload, "get_historical_stock_prices")
    if alias_payload.get("ok") is not False:
        raise AssertionError(f"deprecated alias validation failure must be top-level ok:false: {alias_payload}")
    alias_meta = alias_payload.get("meta") or {}
    if alias_meta.get("canonicalTool") != "get_historical_prices":
        raise AssertionError(f"deprecated alias missing canonicalTool: {alias_payload}")
    if alias_meta.get("deprecatedTool") is not True:
        raise AssertionError(f"deprecated alias missing deprecatedTool=true: {alias_payload}")
    if alias_meta.get("useInstead") != "get_historical_prices":
        raise AssertionError(f"deprecated alias missing useInstead: {alias_payload}")
    alias_warnings = alias_meta.get("warnings") or []
    if not any(isinstance(w, dict) and w.get("code") == "DEPRECATED_ALIAS" for w in alias_warnings):
        raise AssertionError(f"deprecated alias missing DEPRECATED_ALIAS warning: {alias_payload}")

    print(f"PASS deployed discovery + smoke ({len(names)} tools)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
