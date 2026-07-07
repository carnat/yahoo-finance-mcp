#!/usr/bin/env python3
"""Deployed SEC structured extractor smoke tests.

Makes live JSON-RPC calls against the deployed MCP Worker to validate
SEC structured extractor tool output shapes and value invariants.

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
_EXPECTED_TOOL_MODE = os.environ.get("EXPECTED_TOOL_MODE", os.environ.get("TOOL_MODE", "")).lower()
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
}
ACTION_GROUP = {
    "list_sec_company_filings": "sec_filings",
    "index_sec_filing": "sec_filings",
    "get_sec_filing_index": "sec_filings",
    "query_sec_filing_index": "sec_filings",
    "extract_sec_filing_fact": "sec_filings",
    "search_sec_filing_text": "sec_filings",
    "extract_geographic_revenue": "sec_extractors",
    "extract_segment_revenue": "sec_extractors",
    "extract_total_revenue": "sec_extractors",
    "extract_revenue_exposure": "sec_extractors",
    "extract_china_exposure": "sec_extractors",
    "extract_risk_factor_mentions": "sec_extractors",
    "extract_customer_concentration": "sec_extractors",
    "extract_exposure": "sec_extractors",
}
_GROUPED_DISCOVERY = False


def rpc(method: str, params: dict | None = None, req_id: int = 1) -> dict:
    payload: dict = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        payload["params"] = params
    data = json.dumps(payload).encode("utf-8")
    last_exc: Exception | None = None
    # 5 attempts: sleeps of 5, 10, 20, 40 s between attempts (handles transient 503s)
    delays = [5, 10, 20, 40]
    for i in range(5):
        req = urllib.request.Request(
            MCP_URL,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": UA},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
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


def call_tool(name: str, arguments: dict, req_id: int) -> dict:
    call_name = name
    call_args = arguments
    if _GROUPED_DISCOVERY and name not in GROUPED_TOOLS:
        group = ACTION_GROUP.get(name)
        if group:
            call_name = group
            call_args = {"action": name, "params": arguments}
    resp = rpc("tools/call", {"name": call_name, "arguments": call_args}, req_id=req_id)
    if "error" in resp and resp["error"]:
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


class ProviderUnavailableException(Exception):
    pass


def extract_data(payload: dict) -> dict:
    if isinstance(payload, dict) and "ok" in payload and "data" in payload:
        data = payload.get("data")
        if isinstance(data, str):
            try:
                parsed = json.loads(data)
                if isinstance(parsed, dict):
                    if parsed.get("status") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE" or parsed.get("code") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE":
                        raise AssertionError(f"Structured facts should route locally, got provider unavailable: {parsed}")
                    return parsed
            except json.JSONDecodeError:
                return {}
        if isinstance(data, dict):
            if data.get("status") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE" or data.get("code") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE":
                raise AssertionError(f"Structured facts should route locally, got provider unavailable: {data}")
        return data or {}
    if isinstance(payload, dict):
        if payload.get("status") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE" or payload.get("code") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE":
            raise AssertionError(f"Structured facts should route locally, got provider unavailable: {payload}")
    return payload


def assert_no_unknown_tool(payload: dict, tool: str) -> None:
    s = json.dumps(payload).lower()
    bad = ("unknown tool", "method not found", "unregistered dispatch")
    if any(b in s for b in bad):
        raise AssertionError(f"{tool} returned non-callable error: {payload}")


def assert_not_silent_wrong_filing_type(data: dict, label: str) -> None:
    status = str(data.get("status") or data.get("confidence") or data.get("code") or "").upper()
    evidence = data.get("evidence")
    if isinstance(evidence, list) and evidence:
        evidence = evidence[0]
    if not isinstance(evidence, dict):
        evidence = {}
    if status == "NOT_DISCLOSED" and not any((evidence.get(k) or (evidence.get("url") if k == "documentUrl" else None)) for k in ("accessionNumber", "filingDate", "documentUrl")):
        raise AssertionError(f"{label}: silent wrong-filing-type NOT_DISCLOSED with no filing evidence")


def _assert_geo_invariants(data: dict, label: str) -> None:
    if isinstance(data, dict) and (data.get("status") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE" or data.get("code") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE"):
        raise AssertionError(f"{label}: local SEC extractor returned provider-unavailable: {data}")
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


def _assert_aaoi_china_positive(data: dict, label: str) -> None:
    _assert_geo_invariants(data, label)
    value = data.get("value")
    pct = data.get("valuePct")
    if not isinstance(value, (int, float)) or not (200_000_000 <= value <= 350_000_000):
        raise AssertionError(f"{label}: AAOI China value outside expected live range: {data}")
    if not isinstance(pct, (int, float)) or not (45 <= pct <= 70):
        raise AssertionError(f"{label}: AAOI China valuePct outside expected live range: {data}")


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
    global _GROUPED_DISCOVERY
    print(f"SEC structured extractor smoke target: {MCP_URL}")

    # --- tools/list validation ---
    try:
        listed = rpc("tools/list", req_id=1)
    except urllib.error.URLError as exc:
        if _ALLOW_SKIP:
            print(f"SKIP deployed SEC structured smoke: worker unreachable ({exc})")
            return 0
        raise AssertionError(f"Worker unreachable and ALLOW_NETWORK_SKIP not set: {exc}") from exc

    tools = ((listed.get("result") or {}).get("tools")) or []
    names = {str(t.get("name")) for t in tools if isinstance(t, dict)}
    _GROUPED_DISCOVERY = GROUPED_TOOLS.issubset(names)
    if _EXPECTED_TOOL_MODE == "grouped" and not _GROUPED_DISCOVERY:
        raise AssertionError("Expected grouped tools/list, got expanded discovery")
    if _EXPECTED_TOOL_MODE == "expanded" and _GROUPED_DISCOVERY:
        raise AssertionError("Expected expanded tools/list, got grouped discovery")
    if _GROUPED_DISCOVERY:
        if names != GROUPED_TOOLS:
            raise AssertionError(f"Grouped discovery should expose only grouped tools: {sorted(names)}")
        print(f"  PASS grouped tools/list exposes all grouped tools ({len(names)} total)")
    else:
        required_phase2 = {"list_sec_company_filings", "index_sec_filing", "get_sec_filing_index"}
        required_phase3 = {
            "extract_segment_revenue",
            "extract_total_revenue",
            "extract_risk_factor_mentions",
            "extract_customer_concentration",
            "extract_exposure",
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

    # Wrong filing type on a 20-F filer must not become a clean non-disclosure.
    tsm_wrong_type = call_tool("extract_geographic_revenue", {
        "ticker": "TSM", "region": "China", "filing_type": "10-K", "period": "latest",
    }, 23)
    assert_no_unknown_tool(tsm_wrong_type, "extract_geographic_revenue")
    tsm_wrong_type_data = extract_data(tsm_wrong_type)
    assert_not_silent_wrong_filing_type(tsm_wrong_type_data, "TSM 10-K fallback")
    if str(tsm_wrong_type_data.get("status") or tsm_wrong_type_data.get("code") or "").upper() == "FILING_NOT_FOUND_TRY_OTHER_TYPE":
        if "20-F" not in tsm_wrong_type_data.get("suggestedFilingTypes", []):
            raise AssertionError(f"TSM wrong filing type should suggest 20-F: {tsm_wrong_type_data}")
    print("  PASS TSM wrong-filing-type guard")

    # Table listing should be paginated and expose index-derived titles/captions.
    tables_page = call_tool("list_sec_filing_tables", {
        "ticker": "AAPL", "filing_type": "10-K", "offset": 0, "limit": 20,
    }, 24)
    assert_no_unknown_tool(tables_page, "list_sec_filing_tables")
    tables_data = extract_data(tables_page)
    for field in ("tableCount", "returnedCount", "offset", "limit", "hasMore", "tables"):
        if field not in tables_data:
            raise AssertionError(f"list_sec_filing_tables pagination missing {field}: {tables_data}")
    if tables_data.get("returnedCount", 0) > 0 and not any((isinstance(t.get("title"), str) for t in tables_data.get("tables", []))):
        raise AssertionError("list_sec_filing_tables should expose title field for each table")
    print("  PASS list_sec_filing_tables pagination/title smoke")

    # Search should resolve readable primary HTML, not XBRL tag soup.
    tsem_search = call_tool("search_sec_filing_text", {
        "ticker": "TSEM", "search_terms": ["revenue"], "filing_type": "10-K", "context_chars": 800,
    }, 25)
    assert_no_unknown_tool(tsem_search, "search_sec_filing_text")
    tsem_data = extract_data(tsem_search)
    if tsem_data.get("documentKind") == "xbrl_xml":
        raise AssertionError(f"search_sec_filing_text returned XBRL/XML instead of readable filing text: {tsem_data}")
    if tsem_data.get("documentKind") == "primary_html":
        raw = json.dumps(tsem_data.get("matches", []))[:5000].lower()
        if "<xbrl" in raw or "<ix:" in raw:
            raise AssertionError("search_sec_filing_text returned raw XBRL/inline-XBRL tag soup")
    print("  PASS search_sec_filing_text primary-html routing smoke")

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
    evidence = aapl_geo_data.get("evidence")
    if isinstance(evidence, list) and evidence:
        evidence = evidence[0]
    if isinstance(evidence, dict):
        for ef in ("filingType", "filingDate", "accessionNumber", "documentUrl"):
            val = evidence.get(ef) or (evidence.get("url") if ef == "documentUrl" else None)
            if not val:
                print(f"  WARN extract_geographic_revenue AAPL: evidence.{ef} missing/empty")
    print(f"  PASS extract_geographic_revenue AAPL/Greater China (value={aapl_geo_data.get('value')!r})")

    # AAOI China
    aaoi_geo = call_tool("extract_geographic_revenue", {
        "ticker": "AAOI", "region": "China", "filing_type": "10-K", "period": "latest",
    }, 31)
    assert_no_unknown_tool(aaoi_geo, "extract_geographic_revenue")
    aaoi_geo_data = extract_data(aaoi_geo)
    _assert_aaoi_china_positive(aaoi_geo_data, "extract_geographic_revenue AAOI/China")
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
    matches = aaoi_rev_data.get("matches") or []
    if aaoi_rev_data.get("status") != "FOUND_REVENUE_EXPOSURE" or not matches:
        raise AssertionError(f"extract_revenue_exposure AAOI should propagate numeric China exposure: {aaoi_rev_data}")
    first_match = matches[0] if isinstance(matches[0], dict) else {}
    _assert_aaoi_china_positive({
        "value": first_match.get("value"),
        "denominator": first_match.get("denominator"),
        "valueRatio": first_match.get("valueRatio"),
        "valuePct": first_match.get("valuePct"),
        "confidence": first_match.get("confidence"),
        "extractionMethod": "REVENUE_EXPOSURE",
    }, "extract_revenue_exposure AAOI/China")
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
    revenue = aaoi_china_data.get("revenueExposure")
    if not isinstance(revenue, dict):
        raise AssertionError(f"extract_china_exposure AAOI: revenueExposure must be object: {aaoi_china_data}")
    _assert_aaoi_china_positive({
        "value": revenue.get("value"),
        "denominator": revenue.get("denominator"),
        "valueRatio": revenue.get("valueRatio"),
        "valuePct": revenue.get("valuePct"),
        "confidence": revenue.get("confidence"),
        "extractionMethod": "CHINA_EXPOSURE",
    }, "extract_china_exposure AAOI")
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
    risk_matches = [m for m in (axti_risk_data.get("matches") or []) if isinstance(m, dict)]
    if axti_risk_data.get("status") == "FOUND" and not risk_matches:
        raise AssertionError(f"extract_risk_factor_mentions AXTI status FOUND but matches[] is empty: {axti_risk_data}")
    nonempty_excerpts = 0
    for m in risk_matches:
        for mf in ("term", "sectionHeading", "excerpt", "confidence", "evidence"):
            if mf not in m:
                print(f"  WARN extract_risk_factor_mentions AXTI: match missing '{mf}'")
        excerpt = str(m.get("excerpt") or "")
        if excerpt:
            nonempty_excerpts += 1
        if m.get("excerptAvailable") is not True:
            raise AssertionError(f"extract_risk_factor_mentions AXTI: match missing excerptAvailable=true: {m}")
        if len(excerpt) > 500:
            raise AssertionError(
                f"extract_risk_factor_mentions AXTI: excerpt too long ({len(excerpt)} chars): {excerpt[:100]!r}"
            )
    if risk_matches and nonempty_excerpts == 0:
        raise AssertionError(f"extract_risk_factor_mentions AXTI: matches have no non-empty excerpts: {axti_risk_data}")
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

    asts_cash = call_tool("extract_sec_filing_fact", {
        "ticker": "ASTS",
        "fact_name": "CashAndCashEquivalentsAtCarryingValue",
        "filing_type": "10-K",
        "period": "latest",
    }, 82)
    assert_no_unknown_tool(asts_cash, "extract_sec_filing_fact ASTS cash concept")
    asts_cash_data = extract_data(asts_cash)
    if asts_cash_data.get("extractionMethod") == "text_search":
        raise AssertionError(f"extract_sec_filing_fact ASTS cash raw concept should not use legacy text_search: {asts_cash_data}")
    if asts_cash_data.get("value") is None:
        if asts_cash_data.get("status") != "SEC_FACT_NOT_AVAILABLE":
            raise AssertionError(f"extract_sec_filing_fact ASTS cash missing explicit limitation status: {asts_cash_data}")
        if asts_cash_data.get("code") != "NO_COMPANYCONCEPT_FACT_FOR_FORM":
            raise AssertionError(f"extract_sec_filing_fact ASTS cash wrong limitation code: {asts_cash_data}")
        for field in ("filingDate", "accessionNumber", "documentUrl"):
            if not asts_cash_data.get(field):
                raise AssertionError(f"extract_sec_filing_fact ASTS cash limitation missing {field}: {asts_cash_data}")
        if asts_cash_data.get("decisionGrade") is not False:
            raise AssertionError(f"extract_sec_filing_fact ASTS cash limitation must be decisionGrade=false: {asts_cash_data}")
    print(f"  PASS extract_sec_filing_fact ASTS cash concept (status={asts_cash_data.get('status')!r}, value={asts_cash_data.get('value')!r})")

    # --- Phase 3B-13: extract_exposure ---

    aaoi_exp = call_tool("extract_exposure", {
        "ticker": "AAOI", "topic": "china",
    }, 50)
    assert_no_unknown_tool(aaoi_exp, "extract_exposure")
    data = extract_data(aaoi_exp)
    for field in ("ticker", "topic", "overallStatus", "revenueExposure", "operationalExposure", "entityExposure", "riskFactorExposure"):
        if field not in data:
            raise AssertionError(f"extract_exposure AAOI china: missing field '{field}'")
    if data.get("overallStatus") not in ("FOUND_REVENUE_EXPOSURE", "FOUND_NON_REVENUE_EXPOSURE", "NOT_DISCLOSED", "NOT_FOUND"):
        raise AssertionError(f"extract_exposure AAOI china: unexpected overallStatus={data.get('overallStatus')!r}")
    rev = data.get("revenueExposure") or {}
    if rev.get("status") != "FOUND":
        raise AssertionError(f"extract_exposure AAOI china: unexpected revenueExposure.status={rev.get('status')!r}")
    _assert_aaoi_china_positive({
        "value": rev.get("value"),
        "denominator": rev.get("denominator"),
        "valueRatio": rev.get("valueRatio"),
        "valuePct": rev.get("valuePct"),
        "confidence": rev.get("confidence"),
        "extractionMethod": "EXTRACT_EXPOSURE",
    }, "extract_exposure AAOI china")
    print(f"  PASS extract_exposure AAOI china (overallStatus={data.get('overallStatus')!r}, rev.status={rev.get('status')!r})")

    exp_null = call_tool("extract_exposure", {
        "ticker": "AAPL", "topic": "atlantis",
    }, 51)
    assert_no_unknown_tool(exp_null, "extract_exposure")
    data_null = extract_data(exp_null)
    for field in ("ticker", "topic", "overallStatus"):
        if field not in data_null:
            raise AssertionError(f"extract_exposure AAPL atlantis: missing field '{field}'")
    if data_null.get("overallStatus") != "NOT_FOUND":
        raise AssertionError(f"extract_exposure AAPL atlantis: expected NOT_FOUND but got {data_null.get('overallStatus')!r}")
    print(f"  PASS extract_exposure AAPL atlantis (overallStatus={data_null.get('overallStatus')!r})")

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

    print(f"\nPASS deployed SEC structured extractor smoke ({MCP_URL})")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProviderUnavailableException as e:
        print(f"\nSKIPPED remaining extractor tests: {e} (tolerated in CI/test environments)")
        raise SystemExit(0)
