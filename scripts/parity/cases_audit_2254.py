"""Parity cases for the 2.25.4 audit repairs (analysis/yahoo-finance-mcp/F_TRIAGE.md §5).

F35-F40, F43. F43: get_sec_recent_events' default forms include 8-K/A; an amendment keeps filingType "8-K/A" and the title
"8-K/A filed", and types as the form it amends.
"""

from __future__ import annotations

from scripts.parity.cases_news_a import sec_filing, sec_scn, tool_case

FILINGS = [sec_filing("8-K/A", "0000910201-26-000003", "a.htm", filed="2026-10-08"),
           sec_filing("8-K", "0000910201-26-000002", "b.htm", filed="2026-10-05"),
           sec_filing("4", "0000910201-26-000001", "c.xml", filed="2026-10-02")]


def _rows(v: dict) -> list:
    return [{"filingType": it["filingType"], "title": it["title"], "eventType": it["eventType"]} for it in v["items"]]


CASES = [
    tool_case("audit2254.sec_recent_events.default_forms_include_8k_amendments", ["F43"], "sec",
              ["SECA", [], 30, 10], sec_scn(910201, FILINGS), select=_rows,
              expect=[{"filingType": "8-K/A", "title": "8-K/A filed", "eventType": "regulatory"},
                      {"filingType": "8-K", "title": "8-K filed", "eventType": "regulatory"}]),
]


# F35: extract_exposure's revenue status for a region named only in the filing's text.
from scripts.parity.cases_neutrality import AAPL_DOC, SECTION_ROUTES, _tool_case  # noqa: E402

import server  # noqa: E402


def _geo_doc(extra_table: str = "") -> str:
    return (
        "<html><body><h2>Item 7. Management's Discussion and Analysis</h2>"
        "<p>Net sales grew in every region. The Company also sells products in India through partners, and net sales "
        "there are included in Rest of Asia Pacific.</p>"
        "<h2>Item 8. Financial Statements</h2><p>Net sales by reportable geographic segment (in millions):</p>"
        "<table><tr><td></td><td>2025</td><td>2024</td></tr>"
        "<tr><td>Americas</td><td>178,353</td><td>167,045</td></tr>"
        "<tr><td>Europe</td><td>111,032</td><td>101,328</td></tr>"
        "<tr><td>Greater China</td><td>64,377</td><td>66,952</td></tr>"
        "<tr><td>Rest of Asia Pacific</td><td>62,399</td><td>55,710</td></tr>"
        "<tr><td>Total net sales</td><td>416,161</td><td>391,035</td></tr></table>"
        f"{extra_table}</body></html>")


# A revenue table that names India but carries no amounts, so it cannot be parsed.
UNPARSED_INDIA = ("<p>Net sales by country (in millions):</p><table><tr><td>Country</td><td>Note</td></tr>"
                  "<tr><td>India</td><td>see above</td></tr><tr><td>Total</td><td>n/a</td></tr></table>")


def _exposure_view(v: dict) -> dict:
    data = v.get("data") if isinstance(v.get("data"), dict) else {}
    rev = data.get("revenueExposure") or {}
    return {"revenueStatus": rev.get("status"), "revenueCode": rev.get("code"), "code": data.get("code"),
            "warningCodes": sorted({w.get("code") for w in data.get("warnings") or []})}


for _name, _doc, _expect in [
    ("audit2254.extract_exposure.region_only_in_text_is_not_found", _geo_doc(),
     {"revenueStatus": "NOT_FOUND", "revenueCode": None, "code": None, "warningCodes": ["TABLE_NOT_PARSED"]}),
    ("audit2254.extract_exposure.region_in_an_unparsed_table_stays_extraction_failed", _geo_doc(UNPARSED_INDIA),
     {"revenueStatus": "EXTRACTION_FAILED", "revenueCode": "EXTRACTION_FAILED", "code": "EXTRACTION_FAILED",
      "warningCodes": ["TABLE_NOT_PARSED"]}),
]:
    CASES.append(_tool_case(_name, "extract_exposure", {"ticker": "AAPL", "topic": "India", "include_risk_factors": False},
                            (lambda: server.extract_exposure("AAPL", "India", include_risk_factors=False)), _exposure_view,
                            routes={**SECTION_ROUTES, AAPL_DOC: {"body": _doc}, "https://data.sec.gov/api/xbrl/*": {"status": 404, "body": "not found"}}, ids=("F35",), expect=_expect))


# F36: a management-commentary excerpt must show the topic. The long unpunctuated header names Greater China past
# the excerpt's cut; only the second text has a sentence that shows it.
import json  # noqa: E402

from scripts.parity.harness import Case  # noqa: E402

HEADER = ("CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS (Unaudited) (In millions, except number of shares) "
          + " ".join(f"Line item {i} 1,234 5,678" for i in range(30)) + " Greater China 15,033 14,728")
TEXTS = {"header_only": HEADER, "header_then_sentence": HEADER + ". Revenue in Greater China rose 4 percent."}


def _py_commentary(text: str):
    from yfmcp.tools.earnings import _first_sentence_for_topic
    excerpt = _first_sentence_for_topic(text, "Greater China")
    return {"found": excerpt is not None, "showsTopic": excerpt is not None and "greater china" in excerpt.lower()}


for _key, _text in TEXTS.items():
    CASES.append(Case(
        name=f"audit2254.management_commentary.excerpt_shows_topic.{_key}", ids=["F36"],
        py=(lambda t=_text: _py_commentary(t)),
        ts={"js": f"const r = m.yahooFinance.sentenceForTopic({json.dumps(_text)}, 'Greater China'); "
                  "return {found: r !== null, showsTopic: r !== null && r.excerpt.slice(0, 240).toLowerCase().includes('greater china')};"},
        expect={"found": _key != "header_only", "showsTopic": _key != "header_only"}))


# F37: debt with no maturity anywhere is PARTIAL with MATURITY_LADDER_INCOMPLETE; a laddered note stays COMPUTED.
from scripts.parity.cases_capital_structure import _N130, _capital, _debt_filing  # noqa: E402


def _cap_view(v: dict) -> dict:
    return {"status": v["status"], "ladderWarning": [w["message"] for w in v["warnings"] if w["code"] == "MATURITY_LADDER_INCOMPLETE"]}


CASES += [
    _capital("audit2254.capital_structure.debt_without_maturities_is_partial", ["F37"], _debt_filing([], 84_000_000), select=_cap_view,
             expect={"status": "PARTIAL", "ladderWarning": [
                 "The filing tags no debt maturities (no maturity ladder and no instrument maturity dates), so total debt 84000000 is not laddered; "
                 'an annual report (filing_type "10-K") usually tags the ladder.']}),
    # A 10-K that tags no ladder gets no "use the 10-K" hint.
    _capital("audit2254.capital_structure.unladdered_10k_has_no_annual_report_hint", ["F37"], _debt_filing([], 84_000_000), filing="10-K",
             select=_cap_view, expect={"status": "PARTIAL", "ladderWarning": [
                 "The filing tags no debt maturities (no maturity ladder and no instrument maturity dates), so total debt 84000000 is not laddered."]}),
    _capital("audit2254.capital_structure.laddered_note_stays_computed", ["F37"], _debt_filing([_N130], 130_000_000), select=_cap_view,
             expect={"status": "COMPUTED", "ladderWarning": []}),
]


# F38: manufacturing purchase obligations are a commitment, not capex; the bare verb "committed" with no amount or date
# is not a commitment; a non-cancelable commitment without an amount still is.
from scripts.parity.cases_capital_structure import _sched, _stmt  # noqa: E402

_F38_TEXT = ("As of September 27, 2025, the Company had manufacturing purchase obligations of $57.0 billion, with $56.2 billion payable within 12 months. "
             "The Company is committed to inclusion and diversity across its global workforce and supply chain. "
             "We have non-cancelable commitments to purchase equipment for the new facility. "
             "We also have purchase commitments of $2.0 billion for the construction of a new data center facility.")
CASES.append(_sched(
    "audit2254.funding_schedule.purchase_obligations_and_bare_committed", ["F38"], "2025-09-27", [], [_stmt(_F38_TEXT)],
    lambda v: [{k: i[k] for k in ("classification", "statedType", "category", "amountLow")} for i in v["textItems"]],
    expect=[{"classification": "COMPANY_DISCLOSED_COMMITTED", "statedType": "COMPANY_DISCLOSED_COMMITTED", "category": "commitment", "amountLow": 57_000_000_000},
            {"classification": "UNRESOLVED", "statedType": "UNRESOLVED", "category": "commitment", "amountLow": None},
            {"classification": "UNRESOLVED", "statedType": "COMPANY_DISCLOSED_COMMITTED", "category": "capital_expenditure", "amountLow": None},
            {"classification": "COMPANY_DISCLOSED_COMMITTED", "statedType": "COMPANY_DISCLOSED_COMMITTED", "category": "capital_expenditure", "amountLow": 2_000_000_000}]))


# F39: China manufacturing named as "manufactured by outsourcing partners located primarily in China mainland".
_F39_DOC = ("<html><body><h2>Item 1A. Risk Factors</h2><p>Substantially all of the Company's hardware products are manufactured by "
            "outsourcing partners located primarily in China mainland, India, Japan, South Korea, Taiwan and Vietnam. A disruption "
            "there could affect the Company.</p></body></html>")


def _china_view(v: dict) -> dict:
    data = v.get("data") if isinstance(v.get("data"), dict) else {}
    manu = data.get("manufacturingExposure") or {}
    return {"status": manu.get("status"), "terms": [e.get("term") for e in manu.get("evidence") or []]}


CASES.append(_tool_case(
    "audit2254.china_exposure.outsourcing_partners_in_china_is_manufacturing", "extract_china_exposure", {"ticker": "AAPL"},
    lambda: server.extract_china_exposure("AAPL"), _china_view,
    routes={**SECTION_ROUTES, AAPL_DOC: {"body": _F39_DOC}, "https://data.sec.gov/api/xbrl/*": {"status": 404, "body": "not found"}},
    ids=("F39",), expect={"status": "FOUND", "terms": ["manufactured"]}))


# F40: a value read from a "net sales by category" table is segment revenue; a region from the geographic table stays
# geographic revenue.
_CATEGORY_TABLE = ("<p>Net sales by category (in millions):</p><table><tr><td></td><td>2025</td><td>2024</td></tr>"
                   "<tr><td>iPhone</td><td>209,586</td><td>201,183</td></tr>"
                   "<tr><td>Mac</td><td>33,708</td><td>29,984</td></tr>"
                   "<tr><td>Services</td><td>109,158</td><td>96,169</td></tr>"
                   "<tr><td>Total net sales</td><td>416,161</td><td>391,035</td></tr></table>")


def _rev_view(v: dict) -> dict:
    data = v.get("data") if isinstance(v.get("data"), dict) else {}
    return {"status": data.get("status"), "types": [m.get("exposureType") for m in data.get("matches") or []]}


for _query, _type in [("iPhone", "segment_revenue"), ("Greater China", "geographic_revenue")]:
    CASES.append(_tool_case(
        f"audit2254.revenue_exposure.{_type}_label.{_query.replace(' ', '_').lower()}", "extract_revenue_exposure",
        {"ticker": "AAPL", "exposure_query": _query},
        (lambda q=_query: server.extract_revenue_exposure("AAPL", q)), _rev_view,
        routes={**SECTION_ROUTES, AAPL_DOC: {"body": _geo_doc(_CATEGORY_TABLE)}, "https://data.sec.gov/api/xbrl/*": {"status": 404, "body": "not found"}},
        ids=("F40",), expect={"status": "FOUND_REVENUE_EXPOSURE", "types": [_type]}))
