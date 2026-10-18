"""Parity cases: R4b-1 (2.14.0) evidence rows and the analyst mixed signal (R4B1_SPEC.md sections 4 and 5).

Section 4. Both enrichers (Python `yfmcp/envelope.py` `_enrich_facts`, Worker `worker/src/response.ts` `enrichFacts`) emit one
standardised evidence row of 14 keys, in this order:

    url, sourceType, filingType, accessionNumber, filingDate, publishedAt, retrievedAt, excerpt, concept, periodEnd,
    tableIndex, rowLabel, columnLabel, rawRow

with falsy values read as null. Table evidence (no tableIndex / rowLabel / rawRow of its own) fills tableIndex from
sourceTableId, rowLabel from the first element of sourceRows when it is a string, and rawRow from sourceRows joined
with " | " when it is a non-empty list of strings. `extract_sec_filing_fact`'s geographic PARSED_TABLE evidence names the
filing the table came from (documentUrl, filingType, accessionNumber, filingDate), so its row carries a url.

The enricher cases run a fact payload through each runtime's V2 envelope (Python `_envelope_tool_result`, Worker
`mcpSuccess`) and compare `data.evidence`.

Section 5. get_analyst_upgrade_radar: a mixed signal (an upgrade with a cut target, a downgrade with a raised target) is
MIXED in both directions; a downgrade without one is BEARISH.
"""

from __future__ import annotations

import json
import time
from unittest.mock import patch

import pandas as pd

from scripts.parity.cases_neutrality import _tool_case
from scripts.parity.cases_pricing_evidence_tools import YAHOO_AUTH_ROUTES
from scripts.parity.harness import Case

import server  # noqa: F401,E402
from yfmcp.envelope import _envelope_tool_result  # noqa: E402
from yfmcp.schemas import FilingFactType  # noqa: E402

CASES: list[Case] = []

ROW_KEYS = ["url", "sourceType", "filingType", "accessionNumber", "filingDate", "publishedAt", "retrievedAt", "excerpt",
            "concept", "periodEnd", "tableIndex", "rowLabel", "columnLabel", "rawRow"]
NULL_ROW = {k: None for k in ROW_KEYS}
SECTION_4 = "R4B1_SPEC section 4b: table evidence in the enricher (2.14.0), Worker first; Python follows in the same release"


def _row(**fields) -> dict:
    return {**NULL_ROW, **fields}


def _enrich_case(name: str, evidence, select=None, *, keys_only: bool = False, **kw) -> Case:
    """One fact payload carrying `evidence`, through each runtime's V2 success envelope.

    The comparison ignores key order, so `keys_only` returns each evidence row's key list, read inside each runtime.
    """
    payload = {"ticker": "TST", "value": 300000000, "confidence": "HIGH", "extractionMethod": "PARSED_TABLE", "evidence": evidence}
    raw = json.dumps(payload)

    def py():
        envelope = json.loads(_envelope_tool_result("extract_sec_filing_fact", raw))
        if not keys_only:
            return envelope
        rows = envelope["data"]["evidence"]
        return [list(row) for row in rows] if isinstance(rows, list) else rows

    js = ("m.response.setWorkerEnv({}); const v = JSON.parse(m.response.mcpSuccess(args[0], args[1]));"
          + (" const rows = v.data.evidence; return Array.isArray(rows) ? rows.map((r) => Object.keys(r)) : rows;"
             if keys_only else " return v;"))
    return Case(
        name=name, ids=["BR-EVD-001", "R4-AAOI-EVIDENCE"], py=py,
        ts={"js": js, "args": ["extract_sec_filing_fact", raw]},
        select=select, **kw)


def _evidence(v: dict):
    return (v.get("data") or {}).get("evidence")


GEO_TABLE_EVIDENCE = {"sectionHeading": "Geographic Information", "tableTitle": None, "sourceTableId": 1,
                      "sourceRows": [["China", "300"], ["Total revenue", "1,000"]], "sourceColumns": ["2025"]}

CASES += [
    # 4a: the row keys and their order, for an object and for a string.
    _enrich_case("evidence.row.object_evidence_has_the_fourteen_keys_in_order",
                 {"url": "https://www.sec.gov/Archives/edgar/data/1/x.htm", "sourceType": "sec_filing", "excerpt": "Revenue grew."},
                 keys_only=True, expect=[ROW_KEYS]),
    _enrich_case("evidence.row.string_evidence_gets_the_fourteen_keys_in_order",
                 "https://www.sec.gov/Archives/edgar/data/1/x.htm", keys_only=True, expect=[ROW_KEYS]),
    _enrich_case("evidence.row.string_evidence_row_is_url_and_raw_row",
                 "https://www.sec.gov/Archives/edgar/data/1/x.htm", _evidence,
                 expect=[_row(url="https://www.sec.gov/Archives/edgar/data/1/x.htm", rawRow="https://www.sec.gov/Archives/edgar/data/1/x.htm")]),
    _enrich_case("evidence.row.the_six_python_keys_are_kept",
                 {"sourceType": "earnings_release", "publishedAt": "2026-08-01T20:05:00Z", "retrievedAt": "2026-10-18T00:00:00Z",
                  "excerpt": "Revenue was $300 million.", "concept": "Revenues", "periodEnd": "2026-06-30"},
                 _evidence,
                 expect=[_row(sourceType="earnings_release", publishedAt="2026-08-01T20:05:00Z", retrievedAt="2026-10-18T00:00:00Z",
                              excerpt="Revenue was $300 million.", concept="Revenues", periodEnd="2026-06-30")]),
    # Falsy values read as null (`||` / `or`), and a row with nothing left is dropped.
    _enrich_case("evidence.row.falsy_values_read_as_null",
                 [{"url": "", "excerpt": "", "tableIndex": 0, "concept": "Revenues"}, {"rowLabel": "", "rawRow": None}],
                 _evidence, expect=[_row(concept="Revenues")]),
    # A raw object's own table fields win over the table-evidence fallbacks.
    _enrich_case("evidence.table.own_fields_win_over_source_rows",
                 {"tableIndex": 5, "rowLabel": "Own label", "rawRow": "own | raw", "sourceTableId": 1, "sourceRows": ["China", "300"]},
                 _evidence, expect=[_row(tableIndex=5, rowLabel="Own label", rawRow="own | raw")]),
]

CASES += [
    # 4b: table evidence survives standardisation.
    # TODO(python-mirror): flip to equal (R4B1_SPEC section 4b), expect=[_row(tableIndex=2, rowLabel="China",
    # rawRow="China | 300 | 1,000")].
    _enrich_case("evidence.table.source_rows_of_strings_fill_table_index_row_label_and_raw_row",
                 {"sectionHeading": "Revenue by geography", "sourceTableId": 2, "sourceRows": ["China", "300", "1,000"],
                  "sourceColumns": ["2025"]},
                 _evidence, mode="pinned", decision=SECTION_4,
                 expect_py=None, expect_ts=[_row(tableIndex=2, rowLabel="China", rawRow="China | 300 | 1,000")]),
    # The geographic producer's sourceRows are rows of cells (a list of lists), so only tableIndex is filled.
    # TODO(python-mirror): flip to equal (R4B1_SPEC section 4b), expect=[_row(tableIndex=1)].
    _enrich_case("evidence.table.nested_source_rows_keep_the_table_index",
                 GEO_TABLE_EVIDENCE, _evidence, mode="pinned", decision=SECTION_4,
                 expect_py=None, expect_ts=[_row(tableIndex=1)]),
    # TODO(python-mirror): flip to equal (R4B1_SPEC section 4b), expect=[_row(url=DOC, filingType="10-K",
    # accessionNumber=ACCN, filingDate="2026-02-15", tableIndex=1)] (the geographic evidence object of section 4c).
    _enrich_case("evidence.table.geographic_evidence_with_its_filing_names_document_and_table",
                 {**GEO_TABLE_EVIDENCE, "documentUrl": "https://www.sec.gov/Archives/edgar/data/1158114/000115811426000010/aaoi-20251231.htm",
                  "filingType": "10-K", "accessionNumber": "0001158114-26-000010", "filingDate": "2026-02-15"},
                 _evidence, mode="pinned", decision=SECTION_4,
                 expect_py=[_row(url="https://www.sec.gov/Archives/edgar/data/1158114/000115811426000010/aaoi-20251231.htm",
                                 filingType="10-K", accessionNumber="0001158114-26-000010", filingDate="2026-02-15")],
                 expect_ts=[_row(url="https://www.sec.gov/Archives/edgar/data/1158114/000115811426000010/aaoi-20251231.htm",
                                 filingType="10-K", accessionNumber="0001158114-26-000010", filingDate="2026-02-15", tableIndex=1)]),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# 4c: AAOI-shaped extract_sec_filing_fact on the PARSED_TABLE path. No XBRL concept facts (404), so the geographic read
# falls to the latest 10-K's HTML, whose table the parsers read; the HIGH figure's evidence row names its document.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

AAOI_CIK = "0001158114"
AAOI_ACCN = "0001158114-26-000010"
AAOI_DOC = "https://www.sec.gov/Archives/edgar/data/1158114/000115811426000010/aaoi-20251231.htm"
AAOI_HTML = ("<html><body><h2>Geographic Information</h2><p>Revenue by geographic area (in millions)</p>"
             "<table><tr><td></td><td>2025</td><td>2024</td></tr>"
             "<tr><td>China</td><td>300</td><td>250</td></tr><tr><td>United States</td><td>700</td><td>650</td></tr>"
             "<tr><td>Total revenue</td><td>1,000</td><td>900</td></tr></table></body></html>")
AAOI_ROUTES = {
    "https://www.sec.gov/files/company_tickers.json": {"json": {"0": {"cik_str": 1158114, "ticker": "AAOI", "title": "Applied Optoelectronics, Inc."}}},
    f"https://data.sec.gov/submissions/CIK{AAOI_CIK}.json": {"json": {"cik": "1158114", "name": "Applied Optoelectronics, Inc.", "filings": {"recent": {
        "form": ["10-K"], "accessionNumber": [AAOI_ACCN], "primaryDocument": ["aaoi-20251231.htm"],
        "filingDate": ["2026-02-15"], "reportDate": ["2025-12-31"], "acceptanceDateTime": ["2026-02-15T21:00:00.000Z"]}}}},
    "https://data.sec.gov/api/xbrl/*": {"status": 404, "json": {"message": "Not Found"}},
    AAOI_DOC: {"body": AAOI_HTML},
}
AAOI_ROW = _row(url=AAOI_DOC, filingType="10-K", accessionNumber=AAOI_ACCN, filingDate="2026-02-15", tableIndex=1)


def _fact_evidence_view(v: dict) -> dict:
    data = v.get("data") if isinstance(v.get("data"), dict) else {}
    return {"ok": v.get("ok"), "extractionMethod": data.get("extractionMethod"), "confidence": data.get("confidence"),
            "value": data.get("value"), "evidence": data.get("evidence")}


_AAOI_BASE = {"ok": True, "extractionMethod": "PARSED_TABLE", "confidence": "HIGH", "value": 300000000}
CASES += [
    # TODO(python-mirror): flip to equal (R4B1_SPEC section 4c), expect={**_AAOI_BASE, "evidence": [AAOI_ROW]}.
    _tool_case("evidence.aaoi.extract_sec_filing_fact_parsed_table_evidence_names_its_document",
               "extract_sec_filing_fact", {"ticker": "AAOI", "fact_type": "geographic_revenue", "region": "China"},
               lambda: server.extract_sec_filing_fact("AAOI", fact_type=FilingFactType.geographic_revenue, region="China"),
               _fact_evidence_view, routes=AAOI_ROUTES, ids=("BR-EVD-001", "R4-AAOI-EVIDENCE"),
               mode="pinned", decision="R4B1_SPEC section 4c: the geographic PARSED_TABLE evidence names its filing (2.14.0), Worker first",
               expect_py={**_AAOI_BASE, "evidence": None}, expect_ts={**_AAOI_BASE, "evidence": [AAOI_ROW]}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Section 5: get_analyst_upgrade_radar strengthFlag (BR-SHR-003/004). Python reads yfinance upgrades_downgrades (stubbed);
# the Worker reads Yahoo's upgradeDowngradeHistory (served). One rating change per case, one day old.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

_RECENT = int(time.time()) - 86_400


def _radar_case(name: str, action: str, from_grade: str, to_grade: str, prior: float, current: float, **kw) -> Case:
    row = {"Firm": "Firm A", "ToGrade": to_grade, "FromGrade": from_grade, "Action": action,
           "currentPriceTarget": current, "priorPriceTarget": prior, "priceTargetAction": ""}

    class _Ticker:
        upgrades_downgrades = pd.DataFrame([row], index=pd.DatetimeIndex([pd.Timestamp.now().normalize()], name="GradeDate"))

    def py():
        import asyncio
        with patch.object(server.yf, "Ticker", lambda _t: _Ticker()):
            return asyncio.run(server.get_analyst_upgrade_radar("RDR", days_back=30))

    history = {"quoteSummary": {"result": [{"upgradeDowngradeHistory": {"history": [{
        "epochGradeDate": _RECENT, "firm": "Firm A", "toGrade": to_grade, "fromGrade": from_grade, "action": action,
        "currentPriceTarget": current, "priorPriceTarget": prior, "priceTargetAction": ""}]}}]}}
    return Case(
        name=name, ids=["BR-SHR-003", "BR-SHR-004"], py=py,
        ts={"js": 'return JSON.parse(await m.yahooFinance.getAnalystUpgradeRadar("RDR", 30));', "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, "https://query1.finance.yahoo.com/v10/finance/quoteSummary/RDR?*": {"json": history}},
        select=lambda v: [{k: c.get(k) for k in ("signal", "ptDirection", "mixedSignal", "strengthFlag")} for c in v.get("changes") or []],
        **kw)


CASES += [
    _radar_case("shr.analyst.upgrade_with_a_cut_target_is_mixed", "up", "Hold", "Buy", 120.0, 100.0,
                expect=[{"signal": "UPGRADE", "ptDirection": "LOWERED", "mixedSignal": True, "strengthFlag": "MIXED"}]),
    # TODO(python-mirror): flip to equal (R4B1_SPEC section 5), expect=[{"signal": "DOWNGRADE", "ptDirection": "RAISED",
    # "mixedSignal": True, "strengthFlag": "MIXED"}].
    _radar_case("shr.analyst.downgrade_with_a_raised_target_is_mixed", "down", "Buy", "Hold", 100.0, 120.0,
                mode="pinned", decision="R4B1_SPEC section 5: mixedSignal is tested before DOWNGRADE (2.14.0), Worker first",
                expect_py=[{"signal": "DOWNGRADE", "ptDirection": "RAISED", "mixedSignal": True, "strengthFlag": "BEARISH"}],
                expect_ts=[{"signal": "DOWNGRADE", "ptDirection": "RAISED", "mixedSignal": True, "strengthFlag": "MIXED"}]),
    _radar_case("shr.analyst.downgrade_without_a_mixed_signal_is_bearish", "down", "Buy", "Hold", 120.0, 100.0,
                expect=[{"signal": "DOWNGRADE", "ptDirection": "LOWERED", "mixedSignal": False, "strengthFlag": "BEARISH"}]),
    _radar_case("shr.analyst.upgrade_without_a_mixed_signal_is_bullish", "up", "Hold", "Buy", 100.0, 120.0,
                expect=[{"signal": "UPGRADE", "ptDirection": "RAISED", "mixedSignal": False, "strengthFlag": "BULLISH"}]),
]
