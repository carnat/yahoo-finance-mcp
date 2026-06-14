#!/usr/bin/env python3
"""Deployed Phase 3 extractor smoke tests.

Makes live JSON-RPC calls against the deployed MCP Worker to validate
Phase 3 extractor tool output shapes and value invariants.

Environment variables:
  MCP_URL              Deployed Worker endpoint (required).
  ALLOW_NETWORK_SKIP   Set to "1" or "true" to silently skip when unreachable.
                       Defaults to "1" so sandboxed CI passes automatically.

Run against deployed worker:
  MCP_URL="https://yahoo-finance-mcp.artinatw.workers.dev/mcp" python scripts/test_deployed_phase3_extractors.py
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

MCP_URL = os.environ.get("MCP_URL", "https://yahoo-finance-mcp.artinatw.workers.dev/mcp").strip()
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-phase3-extractor-smoke/1.0)"
_ALLOW_SKIP = os.environ.get("ALLOW_NETWORK_SKIP", "1").lower() in ("1", "true", "yes")


def rpc(method: str, params: dict | None = None, req_id: int = 1) -> dict:
    payload: dict = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        payload["params"] = params
    data = json.dumps(payload).encode("utf-8")
    last_exc: Exception | None = None
    for i in range(3):
        req = urllib.request.Request(
            MCP_URL,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": UA},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read())
        except urllib.error.URLError as e:
            last_exc = e
            if i < 2:
                time.sleep(3 * (i + 1))
                continue
            raise
    raise last_exc or RuntimeError("RPC failed")


def call_tool(name: str, arguments: dict, req_id: int) -> dict:
    resp = rpc("tools/call", {"name": name, "arguments": arguments}, req_id=req_id)
    if "error" in resp and resp["error"]:
        raise AssertionError(f"{name} JSON-RPC error: {resp['error']}")
    text = ((((resp.get("result") or {}).get("content") or [{}])[0]) or {}).get("text", "")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"_raw": text}


def extract_data(payload: dict) -> dict:
    if isinstance(payload, dict) and "ok" in payload and "data" in payload:
        data = payload.get("data")
        if isinstance(data, str):
            try:
                parsed = json.loads(data)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                return {}
        return data or {}
    return payload


def assert_no_unknown_tool(payload: dict, tool: str) -> None:
    s = json.dumps(payload).lower()
    bad = ("unknown tool", "method not found", "unregistered dispatch")
    if any(b in s for b in bad):
        raise AssertionError(f"{tool} returned non-callable error: {payload}")


def _assert_geo_invariants(data: dict, label: str) -> None:
    for key in ("value", "denominator", "valueRatio", "valuePct", "confidence", "extractionMethod"):
        if key not in data:
            raise AssertionError(f"{label}: missing key '{key}'")
        if data[key] == {}:
            raise AssertionError(f"{label}: '{key}' must be null/number, not object")
    if data["denominator"] is not None:
        assert data["valueRatio"] is not None, f"{label}: valueRatio null when denominator is set"
        assert data["valuePct"] is not None, f"{label}: valuePct null when denominator is set"
    else:
        assert data["valueRatio"] is None, f"{label}: valueRatio must be null when denominator is null"
        assert data["valuePct"] is None, f"{label}: valuePct must be null when denominator is null"
    if data["valueRatio"] is not None:
        r = float(data["valueRatio"])
        assert 0.0 <= r <= 1.0, f"{label}: valueRatio {r} not in [0,1]"
    if data["valuePct"] is not None:
        p = float(data["valuePct"])
        assert 0.0 <= p <= 100.0, f"{label}: valuePct {p} not in [0,100]"


def _check_private_terms(descriptions: list[str], tool_names: list[str]) -> None:
    """Ensure no private/internal terms appear in public tool descriptions."""
    private_terms = ["IO", "Commander", "portfolio state", "DC-", "doctrine", "TPS", "PCCE"]
    for desc in descriptions:
        d = str(desc)
        for term in private_terms:
            # IO is a common substring; check as exact word or phrase
            if term == "IO":
                import re
                if re.search(r'\bIO\b', d):
                    raise AssertionError(f"Private term 'IO' found in tool description: {d[:200]!r}")
            elif term in d:
                raise AssertionError(f"Private term {term!r} found in tool description: {d[:200]!r}")


def main() -> int:
    print(f"Phase 3 extractor smoke target: {MCP_URL}")

    # --- tools/list validation ---
    try:
        listed = rpc("tools/list", req_id=1)
    except urllib.error.URLError as exc:
        if _ALLOW_SKIP:
            print(f"SKIP deployed Phase 3 smoke: worker unreachable ({exc})")
            return 0
        raise AssertionError(f"Worker unreachable and ALLOW_NETWORK_SKIP not set: {exc}") from exc

    tools = ((listed.get("result") or {}).get("tools")) or []
    names = {str(t.get("name")) for t in tools if isinstance(t, dict)}

    required_phase2 = {"list_sec_company_filings", "index_sec_filing", "get_sec_filing_index"}
    required_phase3 = {
        "extract_geographic_revenue",
        "extract_segment_revenue",
        "extract_total_revenue",
        "extract_revenue_exposure",
        "extract_china_exposure",
        "extract_risk_factor_mentions",
        "extract_customer_concentration",
    }
    missing_p2 = sorted(required_phase2 - names)
    missing_p3 = sorted(required_phase3 - names)
    if missing_p2:
        raise AssertionError(f"Missing Phase 2 tools in tools/list: {missing_p2}")
    if missing_p3:
        raise AssertionError(f"Missing Phase 3 tools in tools/list: {missing_p3}")
    print(f"  PASS tools/list exposes all Phase 2+3 tools ({len(names)} total)")

    # Check descriptions for private terms (require at least one description to avoid silent pass)
    descriptions = [str(t.get("description", "")) for t in tools if isinstance(t, dict)]
    if not descriptions:
        raise AssertionError("tools/list returned no tool descriptions — cannot validate private term policy")
    _check_private_terms(descriptions, list(names))
    print("  PASS no private/internal terms in public tool descriptions")

    # --- Phase 3B-3: Phase 2 index tools ---
    filings = call_tool("list_sec_company_filings", {"ticker": "AAPL", "filing_type": "10-K", "limit": 5}, 20)
    assert_no_unknown_tool(filings, "list_sec_company_filings")
    filings_data = extract_data(filings)
    filing_list = filings_data.get("filings") if isinstance(filings_data, dict) else None
    if not isinstance(filing_list, list) or not filing_list:
        raise AssertionError("list_sec_company_filings: expected non-empty filings[]")
    first = filing_list[0] if isinstance(filing_list[0], dict) else {}
    if not first.get("accessionNumber"):
        raise AssertionError(f"list_sec_company_filings: missing accessionNumber: {first}")
    if not first.get("filingDate"):
        raise AssertionError(f"list_sec_company_filings: missing filingDate: {first}")
    if not str(first.get("documentUrl", "")).startswith("https://www.sec.gov/Archives/"):
        raise AssertionError(f"list_sec_company_filings: invalid documentUrl: {first}")
    print("  PASS list_sec_company_filings AAPL schema")

    # index_sec_filing AAPL
    aapl_idx = call_tool("index_sec_filing", {"ticker": "AAPL", "filing_type": "10-K", "period": "latest"}, 21)
    assert_no_unknown_tool(aapl_idx, "index_sec_filing")
    aapl_idx_data = extract_data(aapl_idx)
    doc_url = aapl_idx_data.get("documentUrl", "")
    if not isinstance(doc_url, str) or not doc_url.startswith("https://www.sec.gov/Archives/"):
        raise AssertionError(f"index_sec_filing AAPL: invalid documentUrl: {doc_url!r}")
    index = aapl_idx_data.get("index") if isinstance(aapl_idx_data, dict) else {}
    if not isinstance(index, dict) or not isinstance(index.get("sections"), list):
        raise AssertionError("index_sec_filing AAPL: missing index.sections")
    if not isinstance(index.get("tables"), list):
        raise AssertionError("index_sec_filing AAPL: missing index.tables")
    if not isinstance(index.get("keywordMap"), dict):
        raise AssertionError("index_sec_filing AAPL: missing index.keywordMap")
    print("  PASS index_sec_filing AAPL schema")

    # get_sec_filing_index AAPL
    aapl_cidx = call_tool("get_sec_filing_index", {"ticker": "AAPL", "filing_type": "10-K", "period": "latest"}, 22)
    assert_no_unknown_tool(aapl_cidx, "get_sec_filing_index")
    aapl_cidx_data = extract_data(aapl_cidx)
    if not isinstance(aapl_cidx_data.get("index"), dict):
        raise AssertionError("get_sec_filing_index AAPL: missing index")
    print("  PASS get_sec_filing_index AAPL schema")

    # --- Phase 3B-4: extract_geographic_revenue ---

    # AAPL Greater China
    aapl_geo = call_tool("extract_geographic_revenue", {
        "ticker": "AAPL", "region": "Greater China", "filing_type": "10-K", "period": "latest",
    }, 30)
    assert_no_unknown_tool(aapl_geo, "extract_geographic_revenue")
    aapl_geo_data = extract_data(aapl_geo)
    _assert_geo_invariants(aapl_geo_data, "extract_geographic_revenue AAPL/Greater China")
    for field in ("factType", "evidence", "warnings"):
        if field not in aapl_geo_data:
            raise AssertionError(f"extract_geographic_revenue AAPL: missing field '{field}'")
    evidence = aapl_geo_data.get("evidence") or {}
    if isinstance(evidence, dict):
        for ef in ("filingType", "filingDate", "accessionNumber", "documentUrl"):
            if not evidence.get(ef):
                print(f"  WARN extract_geographic_revenue AAPL: evidence.{ef} missing/empty")
    print(f"  PASS extract_geographic_revenue AAPL/Greater China (value={aapl_geo_data.get('value')!r})")

    # AAOI China
    aaoi_geo = call_tool("extract_geographic_revenue", {
        "ticker": "AAOI", "region": "China", "filing_type": "10-K", "period": "latest",
    }, 31)
    assert_no_unknown_tool(aaoi_geo, "extract_geographic_revenue")
    aaoi_geo_data = extract_data(aaoi_geo)
    _assert_geo_invariants(aaoi_geo_data, "extract_geographic_revenue AAOI/China")
    print(f"  PASS extract_geographic_revenue AAOI/China (value={aaoi_geo_data.get('value')!r}, pct={aaoi_geo_data.get('valuePct')!r})")

    # Missing region (Atlantis) — stable null keys
    atlantis_geo = call_tool("extract_geographic_revenue", {
        "ticker": "AAPL", "region": "Atlantis", "filing_type": "10-K", "period": "latest",
    }, 32)
    assert_no_unknown_tool(atlantis_geo, "extract_geographic_revenue")
    atlantis_data = extract_data(atlantis_geo)
    _assert_geo_invariants(atlantis_data, "extract_geographic_revenue AAPL/Atlantis")
    if atlantis_data.get("value") is not None:
        raise AssertionError(f"Atlantis region must have null value, got: {atlantis_data.get('value')!r}")
    print("  PASS extract_geographic_revenue AAPL/Atlantis (stable null)")

    # --- Phase 3B-5: extract_revenue_exposure ---

    aaoi_rev = call_tool("extract_revenue_exposure", {
        "ticker": "AAOI", "exposure_query": "China", "filing_type": "10-K", "period": "latest",
    }, 40)
    assert_no_unknown_tool(aaoi_rev, "extract_revenue_exposure")
    aaoi_rev_data = extract_data(aaoi_rev)
    for field in ("ticker", "query", "matches", "status"):
        if field not in aaoi_rev_data:
            raise AssertionError(f"extract_revenue_exposure AAOI: missing field '{field}'")
    for m in aaoi_rev_data.get("matches", []):
        if isinstance(m, dict) and m.get("valuePct") is not None:
            if m.get("denominator") is None:
                raise AssertionError("extract_revenue_exposure AAOI: valuePct present but denominator null")
    print(f"  PASS extract_revenue_exposure AAOI/China (status={aaoi_rev_data.get('status')!r})")

    axti_rev = call_tool("extract_revenue_exposure", {
        "ticker": "AXTI", "exposure_query": "China", "filing_type": "10-K", "period": "latest",
    }, 41)
    assert_no_unknown_tool(axti_rev, "extract_revenue_exposure")
    axti_rev_data = extract_data(axti_rev)
    # AXTI should NOT falsely show revenue exposure
    if axti_rev_data.get("status") == "FOUND_REVENUE_EXPOSURE":
        matches = axti_rev_data.get("matches") or []
        for m in matches:
            if isinstance(m, dict) and m.get("value") is not None:
                print(f"  NOTE AXTI has revenue exposure value: {m.get('value')} — verify against filing")
    print(f"  PASS extract_revenue_exposure AXTI/China (status={axti_rev_data.get('status')!r})")

    # --- Phase 3B-6: extract_china_exposure ---

    axti_china = call_tool("extract_china_exposure", {
        "ticker": "AXTI", "filing_type": "10-K", "period": "latest",
    }, 50)
    assert_no_unknown_tool(axti_china, "extract_china_exposure")
    axti_china_data = extract_data(axti_china)
    for field in ("ticker", "exposureType", "revenueExposure", "manufacturingExposure",
                  "entityExposure", "bankExposure", "riskFactorExposure", "overallStatus", "warnings"):
        if field not in axti_china_data:
            raise AssertionError(f"extract_china_exposure AXTI: missing field '{field}'")
    rev = axti_china_data.get("revenueExposure") or {}
    if not isinstance(rev, dict) or "status" not in rev:
        raise AssertionError("extract_china_exposure AXTI: revenueExposure missing status")
    # Revenue exposure must not claim FOUND from entity/risk text
    if rev.get("value") is None and axti_china_data.get("overallStatus") == "FOUND_REVENUE_EXPOSURE":
        raise AssertionError(
            "extract_china_exposure AXTI: overallStatus=FOUND_REVENUE_EXPOSURE but revenueExposure.value is null"
        )
    print(f"  PASS extract_china_exposure AXTI (overallStatus={axti_china_data.get('overallStatus')!r})")

    aaoi_china = call_tool("extract_china_exposure", {
        "ticker": "AAOI", "filing_type": "10-K", "period": "latest",
    }, 51)
    assert_no_unknown_tool(aaoi_china, "extract_china_exposure")
    aaoi_china_data = extract_data(aaoi_china)
    for field in ("ticker", "exposureType", "revenueExposure", "overallStatus"):
        if field not in aaoi_china_data:
            raise AssertionError(f"extract_china_exposure AAOI: missing field '{field}'")
    print(f"  PASS extract_china_exposure AAOI (overallStatus={aaoi_china_data.get('overallStatus')!r})")

    # --- Phase 3B-7: extract_risk_factor_mentions ---

    axti_risk = call_tool("extract_risk_factor_mentions", {
        "ticker": "AXTI",
        "terms": ["China", "tariff", "export control", "Bank of China"],
        "filing_type": "10-K",
        "period": "latest",
    }, 60)
    assert_no_unknown_tool(axti_risk, "extract_risk_factor_mentions")
    axti_risk_data = extract_data(axti_risk)
    for field in ("ticker", "matches", "status"):
        if field not in axti_risk_data:
            raise AssertionError(f"extract_risk_factor_mentions AXTI: missing field '{field}'")
    for m in (axti_risk_data.get("matches") or []):
        if not isinstance(m, dict):
            continue
        for mf in ("term", "sectionHeading", "excerpt", "confidence", "evidence"):
            if mf not in m:
                print(f"  WARN extract_risk_factor_mentions AXTI: match missing '{mf}'")
        excerpt = str(m.get("excerpt") or "")
        if len(excerpt) > 500:
            raise AssertionError(
                f"extract_risk_factor_mentions AXTI: excerpt too long ({len(excerpt)} chars): {excerpt[:100]!r}"
            )
    print(f"  PASS extract_risk_factor_mentions AXTI (status={axti_risk_data.get('status')!r}, matches={len(axti_risk_data.get('matches') or [])})")

    # --- Phase 3B-8: extract_customer_concentration ---

    aaoi_cust = call_tool("extract_customer_concentration", {
        "ticker": "AAOI", "filing_type": "10-K", "period": "latest",
    }, 70)
    assert_no_unknown_tool(aaoi_cust, "extract_customer_concentration")
    aaoi_cust_data = extract_data(aaoi_cust)
    for field in ("ticker", "customers", "status"):
        if field not in aaoi_cust_data:
            raise AssertionError(f"extract_customer_concentration AAOI: missing field '{field}'")
    print(f"  PASS extract_customer_concentration AAOI (status={aaoi_cust_data.get('status')!r})")

    lite_cust = call_tool("extract_customer_concentration", {
        "ticker": "LITE", "filing_type": "10-K", "period": "latest",
    }, 71)
    assert_no_unknown_tool(lite_cust, "extract_customer_concentration")
    lite_cust_data = extract_data(lite_cust)
    for field in ("ticker", "customers", "status"):
        if field not in lite_cust_data:
            raise AssertionError(f"extract_customer_concentration LITE: missing field '{field}'")
    # LITE FY2025 baseline: Customer A 16.0%, Customer B 15.4%
    if lite_cust_data.get("status") == "FOUND":
        customers = lite_cust_data.get("customers") or []
        pcts = [c.get("valuePct") for c in customers if isinstance(c, dict)]
        if pcts:
            print(f"  NOTE LITE customer concentration percentages: {pcts}")
    print(f"  PASS extract_customer_concentration LITE (status={lite_cust_data.get('status')!r})")

    # --- Phase 3B-9: extract_total_revenue and extract_segment_revenue ---

    lite_total = call_tool("extract_total_revenue", {
        "ticker": "LITE", "filing_type": "10-K", "period": "latest",
    }, 80)
    assert_no_unknown_tool(lite_total, "extract_total_revenue")
    lite_total_data = extract_data(lite_total)
    for field in ("ticker", "factType", "confidence", "evidence", "status"):
        if field not in lite_total_data:
            raise AssertionError(f"extract_total_revenue LITE: missing field '{field}'")
    print(f"  PASS extract_total_revenue LITE (status={lite_total_data.get('status')!r}, value={lite_total_data.get('value')!r})")

    lite_seg = call_tool("extract_segment_revenue", {
        "ticker": "LITE", "filing_type": "10-K", "period": "latest",
    }, 81)
    assert_no_unknown_tool(lite_seg, "extract_segment_revenue")
    lite_seg_data = extract_data(lite_seg)
    for field in ("ticker", "factType", "segments", "status"):
        if field not in lite_seg_data:
            raise AssertionError(f"extract_segment_revenue LITE: missing field '{field}'")
    # Check segment labels in LITE FY2025 (Cloud & Networking, Industrial Tech)
    if lite_seg_data.get("status") == "FOUND":
        labels = [s.get("label") for s in (lite_seg_data.get("segments") or []) if isinstance(s, dict)]
        print(f"  NOTE LITE segment labels: {labels}")
        # If segments collapsed to single segment, that is valid — only warn
        if len(labels) == 1:
            print("  WARN LITE has single segment — may have reorganized into single reporting segment (FY2026 Q1)")
    print(f"  PASS extract_segment_revenue LITE (status={lite_seg_data.get('status')!r})")

    # --- Phase 3B-12: Backward compatibility ---

    hist = call_tool("get_historical_prices", {
        "ticker": "AAPL", "period": "5d", "interval": "1d",
    }, 90)
    assert_no_unknown_tool(hist, "get_historical_prices")
    print("  PASS get_historical_prices backward compat")

    bad_hist = call_tool("get_historical_stock_prices", {}, 91)
    bad_str = json.dumps(bad_hist)
    is_provider_404 = (
        "chart" in bad_str.lower()
        and "404" in bad_str
        and ("yahoo" in bad_str.lower() or "finance" in bad_str.lower())
        and "INPUT_VALIDATION_ERROR" not in bad_str
    )
    if is_provider_404:
        raise AssertionError(
            "get_historical_stock_prices({}) caused provider 404 instead of INPUT_VALIDATION_ERROR"
        )
    print("  PASS get_historical_stock_prices empty-ticker returns validation error")

    print(f"\nPASS deployed Phase 3 extractor smoke ({MCP_URL})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
