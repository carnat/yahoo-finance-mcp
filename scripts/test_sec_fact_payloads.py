#!/usr/bin/env python3
"""SEC fact-read payloads, the same in both runtimes (2.5.16).

Worker getFilingData/extractTotalRevenue (worker/src/yahoo-finance.ts) and Python
get_filing_data/extract_total_revenue (server.py) read the same mocked SEC
responses (one route table per case, served by URL to both) and are compared
field by field. Cases, for ticker XYZ (CIK 0000000001):

- a. found: one 10-K FY fact is a decision-grade XBRL fact with its concept;
- b. no facts for the requested form (10-Q): SEC_FACT_NOT_AVAILABLE with code
  NO_COMPANYCONCEPT_FACT_FOR_FORM, no unit, and the latest 10-Q as evidence;
- c. IFRS-only filer (20-F): SEC_FACTS_IFRS_ONLY, no unit;
- d. companyconcept returns an empty USD object: the value is read from
  companyfacts and SEC_COMPANYCONCEPT_MALFORMED is warned;
- e. named fiscal years (2.5.16), on BE-like annual facts where each year is
  reported again as a comparative carrying the later filing's fy: "FY2025",
  "2025" and "latest" give FY2025, a restated year takes the latest filed
  value, a missing year is PERIOD_NOT_FOUND, a quarterly filing type is
  INVALID_PERIOD, and "junk" is INVALID_PERIOD at get_filing_data and
  INPUT_VALIDATION_ERROR at the tool (the Worker's _dispatchTool).

- f. failed SEC reads (2.5.17): a case's "faults" map a URL to an HTTP status (429, 500) or "network" (the fetch
  throws) in both mocks. A 404 is absence (NO_COMPANYCONCEPT_FACT_FOR_FORM); any other failure the answer depends
  on is PROVIDER_ERROR / SEC_READ_FAILED with retryable and failedReads, and the same through
  extract_sec_filing_fact.

- g. unreadable ticker index (2.5.18): a ticker with no CIK is PROVIDER_ERROR / SEC_LOOKUP_UNAVAILABLE (retryable) when
  SEC's ticker index could not be read, and SEC_FACT_NOT_AVAILABLE / NO_SEC_REGISTRANT when it was read and the ticker is
  not in it. Case "faults" may name the ticker-index URL; "ticker" picks a ticker the index does not list.

- h. geographic tools (2.5.18): get_filing_data (whole payload), extract_geographic_revenue, extract_revenue_exposure and
  extract_china_exposure return the same JSON, key order included, on every path: XBRL found, HTML table found,
  NOT_DISCLOSED, EXTRACTION_FAILED (TABLE_NOT_PARSED / FILING_TEXT_NOT_AVAILABLE), SEC_READ_FAILED,
  SEC_LOOKUP_UNAVAILABLE, NO_SEC_REGISTRANT, the 20-F fallback and filing not found. A case route whose body is a
  string is a filing document (HTML); every other body is JSON.

The Worker runs in Node (esbuild bundle, stubbed fetch), one process per case
because it caches CIKs and submissions per isolate.
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))


def _ensure_mcp_available() -> None:
    try:
        import mcp.server  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _FastMCPStub:
        def __init__(self, *a: object, **kw: object) -> None:
            pass

        def tool(self, *a: object, **kw: object):  # type: ignore[return]
            if a and callable(a[0]):
                return a[0]
            return lambda fn: fn

    mcp_mod = types.ModuleType("mcp")
    server_mod = types.ModuleType("mcp.server")
    fastmcp_mod = types.ModuleType("mcp.server.fastmcp")
    fastmcp_mod.FastMCP = _FastMCPStub  # type: ignore[attr-defined]
    mcp_mod.server = server_mod  # type: ignore[attr-defined]
    server_mod.fastmcp = fastmcp_mod  # type: ignore[attr-defined]
    sys.modules.setdefault("mcp", mcp_mod)
    sys.modules.setdefault("mcp.server", server_mod)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_mod)


_ensure_mcp_available()
import server as srv  # noqa: E402
from yfmcp.clients.edgar import EdgarError  # noqa: E402
from yfmcp.parsing.extractors import extract_geo_revenue_from_html  # noqa: E402

TICKER = "XYZ"
CIK = "0000000001"
ACCN_10K = "0000000001-26-000001"
ACCN_10Q = "0000000001-26-000002"
ACCN_20F = "0000000001-26-000003"
CONCEPT_BASE = f"https://data.sec.gov/api/xbrl/companyconcept/CIK{CIK}/us-gaap/"
COMPANYFACTS = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json"
SUBMISSIONS = f"https://data.sec.gov/submissions/CIK{CIK}.json"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
DOC_10K = "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/xyz-10k.htm"
DOC_10Q = "https://www.sec.gov/Archives/edgar/data/1/000000000126000002/xyz-10q.htm"
DOC_20F = "https://www.sec.gov/Archives/edgar/data/1/000000000126000003/xyz-20f.htm"
REVENUE = "RevenueFromContractWithCustomerExcludingAssessedTax"
REVENUE_CONCEPTS = [REVENUE, "RevenueFromContractWithCustomerIncludingAssessedTax", "Revenues", "SalesRevenueNet"]

FACT = {"start": "2025-01-01", "end": "2025-12-31", "val": 1000000, "accn": ACCN_10K, "fy": 2025, "fp": "FY", "form": "10-K", "filed": "2026-02-01"}


def _submissions(*rows: tuple[str, str, str, str]) -> dict:
    """rows: (form, accession, primaryDocument, filingDate)."""
    return {"cik": "1", "filings": {"recent": {
        "form": [r[0] for r in rows], "accessionNumber": [r[1] for r in rows], "primaryDocument": [r[2] for r in rows],
        "filingDate": [r[3] for r in rows], "reportDate": [r[3] for r in rows], "acceptanceDateTime": ["" for _ in rows],
    }}}


_10K_ROW = ("10-K", ACCN_10K, "xyz-10k.htm", "2026-02-01")
_10Q_ROW = ("10-Q", ACCN_10Q, "xyz-10q.htm", "2026-05-05")
_20F_ROW = ("20-F", ACCN_20F, "xyz-20f.htm", "2026-03-15")
_US_GAAP_FACTS = {"facts": {"us-gaap": {REVENUE: {"units": {"USD": [FACT]}}}}}

ACCN_10K_2018 = "0000000001-19-000010"
ACCN_10K_2025 = "0000000001-26-000010"


def _annual(year: int, val: int, accn: str, filed: str, fy: int, *, end: str | None = None, start: str | None = None, fp: str = "FY") -> dict:
    return {"start": start or f"{year}-01-01", "end": end or f"{year}-12-31", "val": val, "accn": accn, "fy": fy, "fp": fp, "form": "10-K", "filed": filed}


# BE-like: every 10-K reports its year and the earlier ones again as comparatives, each carrying THAT filing's fy
# (the 2016 revenue arrives in the FY2018 10-K with fy 2018), and the FY2025 10-K restates 2024 and adds a
# fourth-quarter row. SEC lists them by period end, then filing.
BE_ROWS = [
    _annual(2016, 208540000, "0000000001-17-000010", "2017-03-01", 2016),
    _annual(2016, 208540000, ACCN_10K_2018, "2019-03-01", 2018),
    _annual(2017, 376000000, "0000000001-18-000010", "2018-03-01", 2017),
    _annual(2017, 376000000, ACCN_10K_2018, "2019-03-01", 2018),
    _annual(2018, 785000000, ACCN_10K_2018, "2019-03-01", 2018),
    _annual(2023, 1200000000, "0000000001-25-000010", "2025-02-27", 2024),
    _annual(2023, 1200000000, ACCN_10K_2025, "2026-02-26", 2025),
    _annual(2024, 1330000000, "0000000001-25-000010", "2025-02-27", 2024),
    _annual(2024, 1330500000, ACCN_10K_2025, "2026-02-26", 2025),
    _annual(2025, 600000000, ACCN_10K_2025, "2026-02-26", 2025, start="2025-10-01"),
    _annual(2025, 2001614000, ACCN_10K_2025, "2026-02-26", 2025),
]
# DG-like: a 52/53-week year ending 2026-01-30 is the issuer's fiscal 2025.
DG_ROWS = [
    _annual(2024, 40600000000, "0000000001-25-000020", "2025-03-20", 2024, start="2024-02-03", end="2025-01-31"),
    _annual(2025, 41000000000, ACCN_10K_2025, "2026-03-20", 2025, start="2025-02-01", end="2026-01-30"),
]
_QUARTER_ROWS = [
    {"start": "2026-04-01", "end": "2026-06-30", "val": 700000000, "accn": ACCN_10Q, "fy": 2026, "fp": "Q2", "form": "10-Q", "filed": "2026-08-14"},
    {"start": "2025-04-01", "end": "2025-06-30", "val": 500000000, "accn": ACCN_10Q, "fy": 2026, "fp": "Q2", "form": "10-Q", "filed": "2026-08-14"},
]


def _facts_case(rows: list[dict], filing_type: str, period: str, call: str = "total_revenue", *submissions_rows: tuple[str, str, str, str]) -> dict:
    return {
        "filingType": filing_type, "period": period, "call": call,
        "routes": {CONCEPT_BASE + f"{REVENUE}.json": {"units": {"USD": rows}}, COMPANYFACTS: {"facts": {"us-gaap": {REVENUE: {"units": {"USD": rows}}}}},
                   SUBMISSIONS: _submissions(*(submissions_rows or (_10K_ROW, _10Q_ROW)))},
    }


# Each case: the SEC responses (URL -> JSON body; unlisted URLs are 404) and the form asked for.
CASES: dict[str, dict] = {
    "found": {
        "filingType": "10-K",
        "routes": {CONCEPT_BASE + f"{REVENUE}.json": {"units": {"USD": [FACT]}}, COMPANYFACTS: _US_GAAP_FACTS,
                   SUBMISSIONS: _submissions(_10K_ROW, _10Q_ROW)},
    },
    "no_facts_for_form": {
        "filingType": "10-Q",
        "routes": {CONCEPT_BASE + f"{REVENUE}.json": {"units": {"USD": [FACT]}}, COMPANYFACTS: _US_GAAP_FACTS,
                   SUBMISSIONS: _submissions(_10Q_ROW, _10K_ROW)},
    },
    "ifrs_only": {
        "filingType": "20-F",
        "routes": {COMPANYFACTS: {"facts": {"ifrs-full": {"Revenue": {"units": {"USD": [{**FACT, "form": "20-F", "accn": ACCN_20F}]}}}}},
                   SUBMISSIONS: _submissions(_20F_ROW)},
    },
    "be_fy2025": _facts_case(BE_ROWS, "10-K", "FY2025"),
    "be_2025": _facts_case(BE_ROWS, "10-K", "2025"),
    "be_fy_lowercase_spaced": _facts_case(BE_ROWS, "10-K", "fy 2025"),
    "be_latest": _facts_case(BE_ROWS, "10-K", "latest"),
    "be_fy2016": _facts_case(BE_ROWS, "10-K", "FY2016"),
    "be_fy2024_restated": _facts_case(BE_ROWS, "10-K", "FY2024"),
    "be_fy2030": _facts_case(BE_ROWS, "10-K", "FY2030"),
    "be_fy2025_filing_data": _facts_case(BE_ROWS, "10-K", "FY2025", "get_filing_data"),
    "be_fy2030_filing_data": _facts_case(BE_ROWS, "10-K", "FY2030", "get_filing_data"),
    "be_junk_filing_data": _facts_case(BE_ROWS, "10-K", "junk", "get_filing_data"),
    "be_fy25_filing_data": _facts_case(BE_ROWS, "10-K", "FY25", "get_filing_data"),
    "dg_fy2025": _facts_case(DG_ROWS, "10-K", "FY2025"),
    "dg_fy2026": _facts_case(DG_ROWS, "10-K", "2026"),
    "q_fy2025": _facts_case(_QUARTER_ROWS, "10-Q", "FY2025", "total_revenue", _10Q_ROW, _10K_ROW),
    "q_latest": _facts_case(_QUARTER_ROWS, "10-Q", "latest", "total_revenue", _10Q_ROW, _10K_ROW),
    "q_fy2025_filing_data": _facts_case(_QUARTER_ROWS, "10-Q", "FY2025", "get_filing_data", _10Q_ROW, _10K_ROW),
    "malformed_concept": {
        "filingType": "10-K",
        "routes": {CONCEPT_BASE + "Revenues.json": {"units": {"USD": {}}},
                   COMPANYFACTS: {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [FACT]}}}}},
                   SUBMISSIONS: _submissions(_10K_ROW)},
    },
}

# Failed SEC reads (2.5.17). "faults": URL -> HTTP status, or "network" for a fetch that throws; unlisted URLs are 404.
_SUBMISSIONS_10K = {SUBMISSIONS: _submissions(_10K_ROW)}
_ALL_CONCEPTS_NETWORK = {CONCEPT_BASE + f"{name}.json": "network" for name in REVENUE_CONCEPTS}
_READ_FAILED_CASES: dict[str, dict] = {
    "read_429": {"filingType": "10-K", "routes": {COMPANYFACTS: _US_GAAP_FACTS, **_SUBMISSIONS_10K},
                 "faults": {CONCEPT_BASE + "Revenues.json": 429}},
    "absent_404_us_gaap": {"filingType": "10-K", "routes": {COMPANYFACTS: _US_GAAP_FACTS, **_SUBMISSIONS_10K}},
    "absent_404_facts_500": {"filingType": "10-K", "routes": dict(_SUBMISSIONS_10K), "faults": {COMPANYFACTS: 500}},
    "malformed_facts_500": {"filingType": "10-K", "routes": {CONCEPT_BASE + "Revenues.json": {"units": {"USD": {}}}, **_SUBMISSIONS_10K},
                            "faults": {COMPANYFACTS: 500}},
    "network_all_concepts": {"filingType": "10-K", "routes": {COMPANYFACTS: _US_GAAP_FACTS, **_SUBMISSIONS_10K}, "faults": _ALL_CONCEPTS_NETWORK},
    "mixed_status_and_network": {"filingType": "10-K", "routes": {COMPANYFACTS: _US_GAAP_FACTS, **_SUBMISSIONS_10K},
                                 "faults": {CONCEPT_BASE + f"{REVENUE}.json": 500, CONCEPT_BASE + "SalesRevenueNet.json": "network"}},
    "pinned_429": {"filingType": "10-K", "routes": {COMPANYFACTS: _US_GAAP_FACTS, **_SUBMISSIONS_10K}, "accession": ACCN_10K,
                   "faults": {CONCEPT_BASE + f"{REVENUE}.json": 429}},
}
for _name, _case in _READ_FAILED_CASES.items():
    CASES[_name] = _case
    CASES[f"{_name}_filing_data"] = {**_case, "call": "get_filing_data"}
CASES["read_429_dispatch"] = {**_READ_FAILED_CASES["read_429"], "call": "dispatch", "tool": "extract_sec_filing_fact",
                              "args": {"ticker": TICKER, "fact_type": "total_revenue"}}
# Geographic revenue (2.5.17): unavailable results are the Worker's unshaped payload, not the geographic shape.
_GEO = {"factType": "geographic_revenue", "region": "China"}
CASES["geo_read_429_filing_data"] = {**_READ_FAILED_CASES["read_429"], **_GEO, "call": "get_filing_data"}
CASES["geo_read_429_tool"] = {**_READ_FAILED_CASES["read_429"], **_GEO, "call": "extract_geographic_revenue"}
CASES["geo_pinned_no_fact_filing_data"] = {"filingType": "10-K", "routes": {COMPANYFACTS: _US_GAAP_FACTS, **_SUBMISSIONS_10K, CONCEPT_BASE + f"{REVENUE}.json": {"units": {"USD": [FACT]}}},
                                          "accession": ACCN_20F, **_GEO, "call": "get_filing_data"}
CASES["pinned_no_fact_filing_data"] = {**CASES["geo_pinned_no_fact_filing_data"], "factType": "total_revenue", "region": None}
CASES["found_filing_data"] = {**CASES["found"], "call": "get_filing_data"}

# Unreadable ticker index (2.5.18). Ticker ZZZZ has no CIK; the index (XYZ only) is unreachable when its URL has a fault.
_NO_CIK = {"ticker": "ZZZZ", "filingType": "10-K", "routes": {}}
_NO_CIK_CASES: dict[str, dict] = {
    "nocik_unreachable": {**_NO_CIK, "faults": {TICKERS_URL: 500}},
    "nocik_unreachable_network": {**_NO_CIK, "faults": {TICKERS_URL: "network"}},
    "nocik_no_registrant": dict(_NO_CIK),
    "nocik_unreachable_pinned": {**_NO_CIK, "faults": {TICKERS_URL: 500}, "accession": ACCN_10K},
    "nocik_no_registrant_pinned": {**_NO_CIK, "accession": ACCN_10K},
}
for _name, _case in _NO_CIK_CASES.items():
    CASES[f"{_name}_total_revenue"] = {**_case, "call": "total_revenue"}
    CASES[f"{_name}_filing_data"] = {**_case, "call": "get_filing_data"}
    CASES[f"{_name}_geo_filing_data"] = {**_case, **_GEO, "call": "get_filing_data"}

# Geographic tools (2.5.18): each scenario is read through get_filing_data (whole payload) and the three tools built on it.
_GEO_CHINA = {"factType": "geographic_revenue", "region": "China"}


def _geo_html(region: str = "China", value: str = "300", total: str = "1,000") -> str:
    """A filing with a geographic revenue table that the Worker's and Python's parsers read the same way."""
    return ("<html><body><h2>Geographic Information</h2><p>Revenue by geographic area (in millions)</p>"
            "<table><tr><td></td><td>2025</td><td>2024</td></tr>"
            f"<tr><td>{region}</td><td>{value}</td><td>250</td></tr><tr><td>United States</td><td>700</td><td>650</td></tr>"
            f"<tr><td>Total revenue</td><td>{total}</td><td>900</td></tr></table></body></html>")


_TEXT_ONLY_HTML = "<html><body><h2>Risk Factors</h2><p>Our sales in China depend on local demand and our revenue may fall.</p></body></html>"
_MANUFACTURING_HTML = ("<html><body><h2>Business</h2><p>Our products are assembled by contract manufacturing partners in China and shipped to customers worldwide "
                       "each quarter, and our revenue depends on them.</p></body></html>")
_BANK_RISK_HTML = ("<html><body><h2>Risk Factors</h2><p>We hold a working capital credit line with Bank of China to fund our operations in China, and our revenue "
                   "could fall if that lender withdrew the facility.</p></body></html>")
_SILENT_HTML = "<html><body><h2>Business</h2><p>We make widgets in Ohio and sell them to local retailers.</p></body></html>"
ACCN_10K_OLD = "0000000001-25-000001"
DOC_10K_OLD = "https://www.sec.gov/Archives/edgar/data/1/000000000125000001/xyz-10k-2024.htm"
_10K_OLD_ROW = ("10-K", ACCN_10K_OLD, "xyz-10k-2024.htm", "2025-02-01")
FACT_OLD = {**FACT, "accn": ACCN_10K_OLD, "fy": 2024, "filed": "2025-02-01", "start": "2024-01-01", "end": "2024-12-31"}
_SEGMENT_TOTAL = {**FACT, "val": 1000000}
_SEGMENT_CHINA = {**FACT, "val": 400000, "segment": "srt:ChinaMember"}


def _geo_scenario(docs: dict[str, str], *rows: tuple[str, str, str, str], facts: list[dict] | None = None, **extra: object) -> dict:
    """A geographic read of XYZ: its submissions (10-K and 10-Q unless rows say otherwise), filing documents by URL and,
    when `facts` is given, the revenue concept's facts (else the concept is 404, so the read falls to the filing's HTML)."""
    routes: dict = {SUBMISSIONS: _submissions(*(rows or (_10K_ROW, _10Q_ROW))), **docs}
    if facts is not None:
        routes[CONCEPT_BASE + f"{REVENUE}.json"] = {"units": {"USD": facts}}
    return {"filingType": "10-K", **_GEO_CHINA, "routes": routes, **extra}


GEO_SCENARIOS: dict[str, dict] = {
    "xbrl_found": _geo_scenario({}, facts=[_SEGMENT_TOTAL, _SEGMENT_CHINA]),
    "xbrl_no_denominator": _geo_scenario({}, facts=[_SEGMENT_CHINA]),
    # 31,250 / 1,000,000 is exactly 0.03125: JavaScript's Math.round reads 312.5 basis points as 313 (Python's round reads 312).
    "xbrl_half_up": _geo_scenario({}, facts=[_SEGMENT_TOTAL, {**_SEGMENT_CHINA, "val": 31250}]),
    "html_found": _geo_scenario({DOC_10K: _geo_html()}),
    "html_found_japan": _geo_scenario({DOC_10K: _geo_html("Japan")}, region="Japan"),
    "html_found_20f_only": _geo_scenario({DOC_20F: _geo_html()}, _20F_ROW),
    "html_not_disclosed": _geo_scenario({DOC_10K: _SILENT_HTML}),
    "html_table_not_parsed": _geo_scenario({DOC_10K: _TEXT_ONLY_HTML}),
    # The China tool's text reads: manufacturing named beside China (the fallback when no heading or table names it) and
    # Bank of China as a risk-factor term.
    "china_manufacturing_text": _geo_scenario({DOC_10K: _MANUFACTURING_HTML}),
    "china_bank_risk_text": _geo_scenario({DOC_10K: _BANK_RISK_HTML}),
    "html_text_unavailable": _geo_scenario({}),
    # A filing the fallback cannot resolve to a document: the submissions row has no primary document, or an XBRL one.
    "primary_document_missing": _geo_scenario({}, ("10-K", ACCN_10K, "", "2026-02-01")),
    "primary_document_is_xbrl": _geo_scenario({}, ("10-K", ACCN_10K, "xyz-20251231.xml", "2026-02-01")),
    # charsScanned counts UTF-16 code units, as the Worker's string length: an emoji is two.
    "html_astral_characters": _geo_scenario({DOC_10K: _SILENT_HTML.replace("Ohio", "Ohio \U0001F600")}),
    "html_text_fault": _geo_scenario({DOC_10K: _geo_html()}, faults={DOC_10K: 500}),
    "filing_not_found": _geo_scenario({}, _10Q_ROW),
    "html_20f_switch": _geo_scenario({DOC_10K: _SILENT_HTML, DOC_20F: _geo_html()}, _10K_ROW, _20F_ROW),
    "html_20f_also_silent": _geo_scenario({DOC_10K: _SILENT_HTML, DOC_20F: _SILENT_HTML}, _10K_ROW, _20F_ROW),
    "html_20f_also_text_only": _geo_scenario({DOC_10K: _SILENT_HTML, DOC_20F: _TEXT_ONLY_HTML}, _10K_ROW, _20F_ROW),
    "read_429": {**_READ_FAILED_CASES["read_429"], **_GEO_CHINA, "routes": {COMPANYFACTS: _US_GAAP_FACTS, **_SUBMISSIONS_10K, DOC_10K: _geo_html()}},
    "read_429_with_risk_text": {**_READ_FAILED_CASES["read_429"], **_GEO_CHINA, "routes": {COMPANYFACTS: _US_GAAP_FACTS, **_SUBMISSIONS_10K, DOC_10K: _TEXT_ONLY_HTML}},
    # A pinned read whose XBRL facts name no region falls to the HTML fallback, which reads the pinned filing's document
    # (2.5.18; it was the latest filing of the form): an older 10-K here, with its own table.
    "pinned_older_filing": _geo_scenario({DOC_10K: _geo_html(value="400"), DOC_10K_OLD: _geo_html(value="300")}, _10K_ROW, _10K_OLD_ROW, facts=[FACT_OLD], accession=ACCN_10K_OLD),
    "pinned_older_filing_no_doc": _geo_scenario({DOC_10K: _geo_html(value="400")}, _10K_ROW, _10K_OLD_ROW, facts=[FACT_OLD], accession=ACCN_10K_OLD),
    "pinned_filing_not_listed": _geo_scenario({DOC_10K: _geo_html()}, _10K_ROW, facts=[FACT_OLD], accession=ACCN_10K_OLD),
    "pinned_fallback_no_doc": _geo_scenario({}, facts=[FACT], accession=ACCN_10K),
    "pinned_fallback_doc": _geo_scenario({DOC_10K: _geo_html()}, facts=[FACT], accession=ACCN_10K),
    "lookup_unavailable": {**_NO_CIK, **_GEO_CHINA, "faults": {TICKERS_URL: 500}},
    "no_registrant": {**_NO_CIK, **_GEO_CHINA},
}
_GEO_CALLS = ("get_filing_data", "extract_geographic_revenue", "extract_revenue_exposure", "extract_china_exposure")
for _name, _case in GEO_SCENARIOS.items():
    for _call in _GEO_CALLS:
        if _call != "extract_china_exposure" or _case["region"] == "China":  # the China tool reads China only
            CASES[f"geo_{_name}__{_call}"] = {**_case, "call": _call}
# A pinned accession that SEC does not list: extract_china_exposure looks the pinned filing up before anything else.
CASES["geo_china_pinned_accession_missing"] = {**_geo_scenario({DOC_10K: _geo_html()}), "accession": "0000000001-26-000999", "call": "extract_china_exposure"}
CASES["geo_china_pinned_accession_listed"] = {**_geo_scenario({DOC_10K: _geo_html()}), "accession": ACCN_10K, "call": "extract_china_exposure"}

# The table reader (2.5.18): Python's extract_geo_revenue_from_html is a port of the Worker's extractGeoRevenueFromHtml.
GEO_FIXTURES = json.loads((ROOT / "fixtures" / "sec_geo_revenue_tables.json").read_text(encoding="utf-8"))


def _table(rows: str, lead: str = "<h3>Geographic Information</h3><p>Revenue by geographic area (in millions)</p>") -> str:
    return f"<html><body>{lead}<table>{rows}</table></body></html>"


def _tr(*cells: str) -> str:
    return "<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"


PARSE_CASES: dict[str, tuple[str, str]] = {
    # multi-year columns: the first amount column is the latest year
    "multi_year": (_table(_tr("", "2025", "2024", "2023") + _tr("United States", "$6,720", "$6,500", "$6,100") + _tr("China", "$2,840", "$2,600", "$2,100")
                          + _tr("Other", "1,000", "900", "800") + _tr("Total", "$10,560", "$10,000", "$9,000")), "China"),
    "multi_year_japan": (_table(_tr("", "2025", "2024") + _tr("Japan", "300", "250") + _tr("United States", "700", "650") + _tr("Total revenue", "1,000", "900")), "Japan"),
    # currency and percent signs in cells of their own; a "% of Total" header and the stated share
    "split_cells_with_share": ("<p>Net revenue by geographic area (in millions):</p><table>" + _tr("", "Three Months Ended") + _tr("", "August 1, 2026", "% of Total", "August 2, 2025", "% of Total")
                               + _tr("China", "$", "1,161.5", "42", "%", "$", "583.4", "29", "%") + _tr("Taiwan", "456.8", "17", "%", "541.2", "27", "%")
                               + _tr("Other", "1,121.0", "41", "%", "881.5", "44", "%") + _tr("$", "2,739.3", "$", "2,006.1") + "</table>", "China"),
    "stated_share_disagrees": ("<p>Net revenue by geographic area (in millions):</p><table>" + _tr("", "August 1, 2026", "% of Total") + _tr("China", "$", "1,161.5", "58", "%")
                               + _tr("Other", "1,577.8", "42", "%") + _tr("$", "2,739.3") + "</table>", "China"),
    "thousands": (_table(_tr("(in thousands)", "2025", "2024") + _tr("China", "55,076", "30,000") + _tr("Total revenue", "88,326", "60,000"), "<h3>Geographic Areas</h3>"), "China"),
    "billions": (_table(_tr("", "2025") + _tr("China", "$2.84") + _tr("Total", "$15.58"), "<h2>Revenue by Region</h2><p>(in billions)</p>"), "China"),
    "prc_row": (_table(_tr("", "2025") + _tr("PRC", "400") + _tr("United States", "600") + _tr("Total net sales", "1,000")), "China"),
    "peoples_republic_row": (_table(_tr("", "2025") + _tr("People's Republic of China", "400") + _tr("United States", "600") + _tr("Net sales", "1,000")), "China"),
    "mainland_china_row": (_table(_tr("", "2025") + _tr("Mainland China", "400") + _tr("United States", "600") + _tr("Total net revenues", "1,000")), "China"),
    # Hong Kong beside China: the Worker reads the first row that names the region and does not add Hong Kong
    "china_and_hong_kong": (_table(_tr("", "2025") + _tr("China", "300") + _tr("Hong Kong", "100") + _tr("United States", "600") + _tr("Total revenue", "1,000")), "China"),
    "mainland_and_hong_kong": (_table(_tr("", "2025") + _tr("Mainland China", "300") + _tr("Hong Kong", "100") + _tr("United States", "600") + _tr("Total revenue", "1,000")), "China"),
    "hong_kong_region": (_table(_tr("", "2025") + _tr("Hong Kong", "100") + _tr("China", "300") + _tr("United States", "600") + _tr("Total revenue", "1,000")), "Hong Kong"),
    "greater_china": (_table(_tr("", "2025") + _tr("Greater China", "300") + _tr("Americas", "700") + _tr("Total revenue", "1,000")), "Greater China"),
    "taiwan": (_table(_tr("", "2025") + _tr("Taiwan", "300") + _tr("Americas", "700") + _tr("Total revenue", "1,000")), "Taiwan"),
    "unlabeled_total_row": (_table(_tr("", "2025", "2024") + _tr("China", "300", "250") + _tr("Other", "700", "650") + _tr("", "$", "1,000", "$", "900")), "China"),
    "dash_placeholder": (_table(_tr("", "2025", "2024") + _tr("China", "300", "\u2014") + _tr("Total revenue", "1,000", "900")), "China"),
    "footnote_suffix_and_entities": (_table(_tr("Region", "2025&#160;") + _tr("China&nbsp;(1)", "300&nbsp;(2)") + _tr("Other&mdash;net", "700") + _tr("Total&nbsp;revenue", "1,000")), "China"),
    "total_is_last_numeric_row": (_table(_tr("", "2025") + _tr("China", "300") + _tr("Other", "700") + _tr("Sum", "1,000")), "China"),
    # tables the reader rejects
    "square_feet_list": (_table(_tr("Location", "Square feet") + _tr("China", "300") + _tr("Total", "1,000"), "<p>Revenue facilities</p>"), "China"),
    "no_revenue_lead_in": ("<html><body><h3>Properties</h3><p>Our plants</p><table>" + _tr("Location", "Plants") + _tr("China", "3") + _tr("Total", "10") + "</table></body></html>", "China"),
    "region_exceeds_total": (_table(_tr("", "2025") + _tr("China", "1,200") + _tr("Total revenue", "1,000")), "China"),
    "region_is_total_row": (_table(_tr("", "2025") + _tr("Total China", "1,000")), "China"),
    "region_not_in_table": (_table(_tr("", "2025") + _tr("Japan", "300") + _tr("Total revenue", "1,000")), "China"),
    "one_row_table": (_table(_tr("China", "300")), "China"),
    "no_table": ("<html><body><h3>Geographic Information</h3><p>China revenues were significant.</p></body></html>", "China"),
    "empty_html": ("", "China"),
    # the table introduced as a geographic breakdown wins over an earlier revenue table
    "geographic_table_first": ("<html><body><p>Segment revenue</p><table>" + _tr("", "2025") + _tr("China", "50") + _tr("Total revenue", "500") + "</table>"
                               "<p>Revenue by geographic area</p><table>" + _tr("", "2025") + _tr("China", "300") + _tr("Total revenue", "1,000") + "</table></body></html>", "China"),
    "second_candidate_when_first_fails": ("<html><body><p>Revenue by geographic area</p><table>" + _tr("", "2025") + _tr("China", "1,500") + _tr("Total revenue", "1,000") + "</table>"
                                          "<p>Net sales by region</p><table>" + _tr("", "2025") + _tr("China", "300") + _tr("Total revenue", "1,000") + "</table></body></html>", "China"),
    "nested_tables": ("<html><body><h3>Note 20 Geographic Information</h3><table><tr><td><table>" + _tr("Country", "Revenue ($M)") + _tr("China", "2,840") + _tr("Other", "6,070")
                      + _tr("Total", "15,630") + "</table></td></tr></table></body></html>", "China"),
    "section_heading_and_scripts": ("<html><body><h2><span>Note 12</span> &ndash; Geographic Information</h2><script>var China = 1;</script><p>Revenue by country</p><table onclick=\"x()\">"
                                    + _tr("", "2025") + _tr("China", "300") + _tr("Total revenue", "1,000") + "</table></body></html>", "China"),
    "negative_and_parentheses": (_table(_tr("", "2025") + _tr("China", "2,840") + _tr("Adjustments", "(100)") + _tr("Total", "15,630")), "China"),
    "header_one_cell_short": (_table("<tr><td>2025</td><td>2024</td></tr>" + _tr("China", "300", "250") + _tr("Total revenue", "1,000", "900")), "China"),
}
for _name, (_html, _region) in PARSE_CASES.items():
    CASES[f"parse_{_name}"] = {"call": "parse_geo_html", "html": _html, "region": _region, "routes": {}}
for _name, _html in GEO_FIXTURES.items():
    CASES[f"parse_live_{_name}"] = {"call": "parse_geo_html", "html": _html, "region": "China", "routes": {}}

# The tool-level check, through the Worker's callTool (_dispatchTool) and the Python tool functions.
_PERIOD_TOOLS = {
    "extract_sec_filing_fact": {"ticker": TICKER, "fact_type": "total_revenue"},
    "extract_geographic_revenue": {"ticker": TICKER, "region": "China"},
    "extract_segment_revenue": {"ticker": TICKER},
    "extract_total_revenue": {"ticker": TICKER},
    "extract_revenue_exposure": {"ticker": TICKER, "exposure_query": "China"},
    "extract_china_exposure": {"ticker": TICKER},
    "extract_exposure": {"ticker": TICKER, "topic": "China"},
    "query_sec_filing_index": {"ticker": TICKER, "query_type": "total_revenue"},
}
for _tool, _args in _PERIOD_TOOLS.items():
    CASES[f"dispatch_junk_{_tool}"] = {"call": "dispatch", "tool": _tool, "args": {**_args, "period": "junk"}, "filingType": "10-K", "routes": {}}
CASES["dispatch_fy25_extract_total_revenue"] = {"call": "dispatch", "tool": "extract_total_revenue", "args": {"ticker": TICKER, "period": "FY25"}, "filingType": "10-K", "routes": {}}

_ENTRY = """
export { extractTotalRevenue, extractGeographicRevenue, extractRevenueExposure, extractChinaExposure, extractGeoRevenueFromHtml, getFilingData } from "./src/yahoo-finance.ts";
export { callTool } from "./src/tools.ts";
export { setWorkerEnv } from "./src/response.ts";
"""

_HARNESS = r"""
import fs from "node:fs";
const [bundleUrl, casePath] = process.argv.slice(-2);
const m = await import(bundleUrl);
m.setWorkerEnv({});
const c = JSON.parse(fs.readFileSync(casePath, "utf8"));
const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
globalThis.fetch = async (req) => {
  const u = new URL(typeof req === "string" ? req : req.url);
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  if (u.pathname.includes("/quoteSummary/")) return json({ quoteSummary: { result: [{}], error: null } });
  const fault = c.faults?.[u.toString()];
  if (fault === "network") throw new TypeError("fetch failed");
  if (fault !== undefined) return new Response("fault", { status: fault });
  if (u.pathname.endsWith("company_tickers.json")) return json({ "0": { cik_str: 1, ticker: "XYZ", title: "XYZ Corp" } });
  const body = c.routes[u.toString()];
  if (body === undefined) return new Response("not found", { status: 404 });
  return typeof body === "string" ? new Response(body, { headers: { "content-type": "text/html" } }) : json(body);
};
const period = c.period ?? "latest";
const ticker = c.ticker ?? "XYZ";
let out;
if (c.call === "get_filing_data") out = await m.getFilingData(ticker, c.factType ?? "total_revenue", c.region ?? null, c.filingType, period, "auto", c.accession ?? null);
else if (c.call === "extract_geographic_revenue") out = await m.extractGeographicRevenue(ticker, c.region, c.filingType, period, c.accession ?? null);
else if (c.call === "extract_revenue_exposure") out = await m.extractRevenueExposure(ticker, c.region, c.filingType, period);
else if (c.call === "extract_china_exposure") out = await m.extractChinaExposure(ticker, c.filingType, period, c.accession ?? null);
else if (c.call === "parse_geo_html") { const r = m.extractGeoRevenueFromHtml(c.html, c.region); out = JSON.stringify(r ? { ...r, parsedTables: undefined } : null); }
else if (c.call === "dispatch") out = await m.callTool(c.tool, c.args);
else out = await m.extractTotalRevenue(ticker, c.filingType, period);
console.log(JSON.stringify(JSON.parse(out)));
"""


def _node() -> str:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    return node


@functools.cache
def _bundle_dir() -> Path:
    """Bundle the Worker once; the directory lives for the process."""
    _node()
    tmp = Path(tempfile.mkdtemp(prefix="sec-fact-payloads-"))
    entry = WORKER / ".sec-fact-payloads-entry.ts"
    entry.write_text(_ENTRY, encoding="utf-8")
    try:
        subprocess.run(
            [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral", "--main-fields=module,main",
             "--external:node:async_hooks", f"--outfile={tmp / 'bundle.mjs'}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
    finally:
        entry.unlink(missing_ok=True)
    (tmp / "harness.mjs").write_text(_HARNESS, encoding="utf-8")
    return tmp


def _worker(name: str) -> dict:
    tmp = _bundle_dir()
    case_path = tmp / f"{name}.json"
    case_path.write_text(json.dumps(CASES[name]), encoding="utf-8")
    result = subprocess.run([_node(), str(tmp / "harness.mjs"), (tmp / "bundle.mjs").as_uri(), str(case_path)],
                            check=True, capture_output=True, text=True, timeout=180)
    return json.loads(result.stdout.strip().splitlines()[-1])


def _python(name: str) -> dict:
    case = CASES[name]
    ticker = case.get("ticker", TICKER)

    async def edgar_get(url: str) -> dict:
        fault = case.get("faults", {}).get(url)
        if fault == "network":
            raise EdgarError("SEC API connection failed: fetch failed")
        if fault is not None:
            raise EdgarError(f"SEC API returned HTTP {fault}: fault", status_code=fault)
        if url not in case["routes"]:
            raise EdgarError("SEC API returned HTTP 404: Not Found", status_code=404)
        return json.loads(json.dumps(case["routes"][url]))

    async def edgar_html(url: str, max_bytes: int = 5_000_000) -> str | None:
        """A filing document: its route's string body, or None (a fault or a 404), as the Worker's edgarGetHtml."""
        body = None if case.get("faults", {}).get(url) is not None else case["routes"].get(url)
        return body if isinstance(body, str) else None

    async def submissions(_ticker: str) -> tuple[str | None, dict | None]:
        cik = CIK if ticker == TICKER else None
        return cik, (case["routes"].get(SUBMISSIONS) if cik else None)

    async def tickers() -> dict[str, int]:
        # The ticker index: unreadable when its URL has a fault, else it lists XYZ only (the Worker's harness fetch).
        return {} if TICKERS_URL in case.get("faults", {}) else {TICKER: 1}

    # The Python server caches filing text and filing indexes by URL and accession; cases reuse both.
    if case.get("call") == "parse_geo_html":
        return srv._js_json_numbers(extract_geo_revenue_from_html(case["html"], case["region"]))  # integral floats are ints in JSON, as the Worker's
    srv._tool_cache._store.clear()
    srv._FILING_TEXT_CACHE.clear()
    srv._FILING_TEXT_CACHE_CHARS = 0
    with patch("server._resolve_cik_for_ticker", new=AsyncMock(return_value=CIK if ticker == TICKER else None)), \
            patch("server._load_edgar_tickers", new=tickers), \
            patch("server._edgar_get", new=edgar_get), \
            patch("server._edgar_get_html", new=edgar_html), \
            patch("server._get_submissions_for_ticker", new=submissions):
        call = case.get("call", "total_revenue")
        period = case.get("period", "latest")
        if call == "dispatch":
            args = {**case["args"], **({"fact_type": srv.FilingFactType(case["args"]["fact_type"])} if "fact_type" in case["args"] else {})}
            return json.loads(asyncio.run(getattr(srv, case["tool"])(**args)))
        if call == "get_filing_data":
            return json.loads(asyncio.run(srv.get_filing_data(ticker, srv.FilingFactType(case.get("factType", "total_revenue")), case.get("region"), case["filingType"], period,
                                                              accession_number=case.get("accession"))))
        if call == "extract_geographic_revenue":
            return json.loads(asyncio.run(srv.extract_geographic_revenue(ticker, case["region"], case["filingType"], period, case.get("accession"))))
        if call == "extract_revenue_exposure":
            return json.loads(asyncio.run(srv.extract_revenue_exposure(ticker, case["region"], case["filingType"], period)))
        if call == "extract_china_exposure":
            return json.loads(asyncio.run(srv.extract_china_exposure(ticker, case["filingType"], period, case.get("accession"))))
        return json.loads(asyncio.run(srv.extract_total_revenue(ticker, case["filingType"], period)))


def _fields(payload: dict) -> dict:
    source = payload.get("sourceEvidence") or {}
    evidence = payload.get("evidence") or {}
    return {
        "status": payload.get("status"),
        "value": payload.get("value"),
        "unit": payload.get("unit"),
        "code": payload.get("code"),
        "decisionGrade": payload.get("decisionGrade"),
        "confidence": payload.get("confidence"),
        "sourceEvidence.concept": source.get("concept"),
        "evidence.accessionNumber": evidence.get("accessionNumber"),
        "evidence.filingType": evidence.get("filingType"),
        "warningCodes": [w.get("code") for w in payload.get("warnings") or []],
        "period": payload.get("period"),
        "requestedPeriod": payload.get("requestedPeriod"),
        "message": payload.get("message"),
        "warningMessages": [w.get("message") for w in payload.get("warnings") or []],
        "warningSeverities": [w.get("severity") for w in payload.get("warnings") or []],
        "error": payload.get("error"),
        "ok": payload.get("ok"),
        "retryable": payload.get("retryable"),
        "failedReads": payload.get("failedReads"),
    }


class TestSecFactPayloadsAgree(unittest.TestCase):
    """Python and the Worker return the same fields for the same SEC responses."""

    def _same(self, name: str) -> dict:
        worker, local = _fields(_worker(name)), _fields(_python(name))
        for key in worker:
            self.assertEqual(worker[key], local[key], f"{name}: {key}: worker={worker[key]!r} python={local[key]!r}")
        return local

    def _same_payload(self, name: str) -> dict:
        """The whole payload, sourceRows included, top-level key order included, and the same JSON number types."""
        worker, local = _worker(name), _python(name)
        self.assertEqual(worker, local, name)
        self.assertEqual(list(worker), list(local), f"{name}: key order")
        # 2023994000 and 2023994000.0 compare equal but are different JSON: value is an int in both runtimes.
        self.assertIs(type(worker.get("value")), type(local.get("value")), f"{name}: value type worker={worker.get('value')!r} python={local.get('value')!r}")
        self.assertEqual(json.dumps(worker), json.dumps(local), f"{name}: serialised payload")
        return local

    def test_found(self) -> None:
        got = self._same("found")
        self.assertEqual((got["status"], got["value"], got["decisionGrade"]), ("FOUND", 1000000, True))
        self.assertEqual(got["sourceEvidence.concept"], REVENUE)
        self.assertEqual((got["evidence.accessionNumber"], got["evidence.filingType"]), (ACCN_10K, "10-K"))
        self.assertEqual((got["unit"], got["code"], got["warningCodes"]), ("USD", None, []))

    def test_no_facts_for_the_form(self) -> None:
        got = self._same("no_facts_for_form")
        self.assertEqual((got["status"], got["code"], got["unit"], got["value"]), ("NOT_FOUND", "NO_COMPANYCONCEPT_FACT_FOR_FORM", None, None))
        self.assertEqual((got["evidence.accessionNumber"], got["evidence.filingType"]), (ACCN_10Q, "10-Q"))
        self.assertFalse(got["decisionGrade"])
        self.assertEqual(got["warningCodes"], ["NO_COMPANYCONCEPT_FACT_FOR_FORM"])

    def test_ifrs_only(self) -> None:
        got = self._same("ifrs_only")
        self.assertEqual((got["code"], got["unit"], got["value"]), ("SEC_FACTS_IFRS_ONLY", None, None))
        self.assertEqual((got["evidence.accessionNumber"], got["evidence.filingType"]), (ACCN_20F, "20-F"))
        self.assertEqual(got["warningCodes"], ["SEC_FACTS_IFRS_ONLY"])

    def test_malformed_concept_is_read_from_companyfacts(self) -> None:
        got = self._same("malformed_concept")
        self.assertEqual((got["status"], got["value"]), ("FOUND", 1000000))
        self.assertEqual(got["warningCodes"], ["SEC_COMPANYCONCEPT_MALFORMED"])
        self.assertEqual(got["sourceEvidence.concept"], "Revenues")


class TestFoundPayloadAgrees(unittest.TestCase):
    """A found non-geographic fact: value is an int, and sourceRows carries '' (not null) for the missing denominator."""

    _same_payload = TestSecFactPayloadsAgree._same_payload

    def test_found_get_filing_data(self) -> None:
        got = self._same_payload("found_filing_data")
        self.assertIsInstance(got["value"], int)
        self.assertEqual(got["value"], 1000000)
        self.assertEqual(got["evidence"]["sourceRows"], [["Region", "1,000,000"], ["Total revenue", ""]])
        self.assertIsNone(got["denominator"])


class TestFailedSecReads(unittest.TestCase):
    """2.5.17: a failed SEC read is SEC_READ_FAILED (retry), never a missing fact; a 404 is still absence."""

    _same = TestSecFactPayloadsAgree._same
    _same_payload = TestSecFactPayloadsAgree._same_payload

    def _failed(self, name: str, failed_reads: list[dict], message_reads: str) -> dict:
        """Both entry points agree, and carry the same SEC_READ_FAILED payload."""
        data = self._same_payload(f"{name}_filing_data")
        self.assertEqual((data["status"], data["code"], data["retryable"], data["value"], data["decisionGrade"]), ("PROVIDER_ERROR", "SEC_READ_FAILED", True, None, False), name)
        self.assertEqual(data["failedReads"], failed_reads, name)
        message = f"SEC could not be read for {TICKER}: {message_reads}. Whether the fact exists is unknown; retry."
        self.assertEqual(data["warnings"][-1], {"code": "SEC_READ_FAILED", "message": message, "severity": "warning"}, name)
        # Key order: status keeps its place; retryable and failedReads follow _manualLookup.
        keys = list(data)
        self.assertEqual(keys[keys.index("status") - 1: keys.index("status") + 2], ["confidence", "status", "code"], name)
        self.assertEqual(keys[-3:], ["_manualLookup", "retryable", "failedReads"], name)
        total = self._same(name)
        self.assertEqual((total["status"], total["code"], total["retryable"], total["failedReads"], total["message"]), ("PROVIDER_ERROR", "SEC_READ_FAILED", True, failed_reads, None), name)
        keys = list(_python(name))
        self.assertEqual(keys[keys.index("message"): keys.index("message") + 4], ["message", "retryable", "failedReads", "warnings"], name)
        self.assertEqual(keys, list(_worker(name)), f"{name}: extract_total_revenue key order")
        return data

    def test_a_throttled_companyconcept_read_is_not_a_missing_fact(self) -> None:
        data = self._failed("read_429", [{"endpoint": "companyconcept", "concept": "Revenues", "httpStatus": 429}],
                            "companyconcept Revenues (HTTP 429)")
        self.assertEqual(data["concept"], REVENUE)
        # The latest filing of the form it looked in is still named.
        self.assertEqual((data["evidence"]["accessionNumber"], data["accessionNumber"]), (ACCN_10K, ACCN_10K))
        self.assertNotIn("requestedAccession", data)

    def test_a_404_on_every_concept_is_absence(self) -> None:
        data = self._same_payload("absent_404_us_gaap_filing_data")
        self.assertEqual((data["status"], data["code"]), ("SEC_FACT_NOT_AVAILABLE", "NO_COMPANYCONCEPT_FACT_FOR_FORM"))
        self.assertNotIn("retryable", data)
        self.assertNotIn("failedReads", data)
        got = self._same("absent_404_us_gaap")
        self.assertEqual((got["status"], got["code"], got["retryable"], got["failedReads"]), ("NOT_FOUND", "NO_COMPANYCONCEPT_FACT_FOR_FORM", None, None))

    def test_the_ifrs_check_that_could_not_be_read_is_a_failed_read(self) -> None:
        self._failed("absent_404_facts_500", [{"endpoint": "companyfacts", "concept": None, "httpStatus": 500}], "companyfacts (HTTP 500)")

    def test_a_malformed_concept_with_unreadable_companyfacts_keeps_its_warning(self) -> None:
        data = self._failed("malformed_facts_500", [{"endpoint": "companyfacts", "concept": None, "httpStatus": 500}], "companyfacts (HTTP 500)")
        self.assertEqual([w["code"] for w in data["warnings"]], ["SEC_COMPANYCONCEPT_MALFORMED", "SEC_READ_FAILED"])
        self.assertEqual(self._same("malformed_facts_500")["warningCodes"], ["SEC_COMPANYCONCEPT_MALFORMED", "SEC_READ_FAILED"])

    def test_a_network_error_has_no_http_status(self) -> None:
        reads = [{"endpoint": "companyconcept", "concept": name, "httpStatus": None} for name in REVENUE_CONCEPTS]
        self._failed("network_all_concepts", reads, ", ".join(f"companyconcept {name} (no response)" for name in REVENUE_CONCEPTS))

    def test_a_pinned_accession_is_echoed_not_replaced_by_the_latest_filing(self) -> None:
        data = self._failed("pinned_429", [{"endpoint": "companyconcept", "concept": REVENUE, "httpStatus": 429}], f"companyconcept {REVENUE} (HTTP 429)")
        self.assertEqual((data["accessionNumber"], data["requestedAccession"], data["evidence"]), (ACCN_10K, ACCN_10K, {}))
        keys = list(data)
        self.assertEqual(keys[keys.index("accessionNumber") + 1], "requestedAccession")
        self.assertIsNone(data["filingDate"])

    def test_extract_sec_filing_fact_passes_failed_reads_through(self) -> None:
        """Through the tool: failedReads follows retryable. The Worker's callTool also runs the response envelope, which
        decorates nested objects (each failedReads entry and warning) with evidence fields; the Python tool's payload is
        the pre-envelope one, so only the keys this change adds are compared."""
        envelope, local = _worker("read_429_dispatch"), _python("read_429_dispatch")
        self.assertTrue(envelope["ok"])
        worker = json.loads(envelope["data"]) if isinstance(envelope["data"], str) else envelope["data"]
        reads = [{"endpoint": "companyconcept", "concept": "Revenues", "httpStatus": 429}]
        for name, got in (("worker", worker), ("python", local)):
            self.assertEqual((got["status"], got["code"], got["retryable"]), ("PROVIDER_ERROR", "SEC_READ_FAILED", True), name)
            self.assertEqual([{k: r[k] for k in ("endpoint", "concept", "httpStatus")} for r in got["failedReads"]], reads, name)
            keys = list(got)
            self.assertEqual(keys[keys.index("code"): keys.index("code") + 3], ["code", "retryable", "failedReads"], name)
        self.assertEqual(local["failedReads"], reads)


class TestFailedReadsOrder(unittest.TestCase):
    """The Worker lists failedReads in candidate order, companyfacts last (it sorts; its reads finish in network order)."""

    _same_payload = TestSecFactPayloadsAgree._same_payload

    def test_a_status_and_a_network_error_are_listed_in_concept_order(self) -> None:
        data = self._same_payload("mixed_status_and_network_filing_data")
        self.assertEqual(data["failedReads"], [{"endpoint": "companyconcept", "concept": REVENUE, "httpStatus": 500},
                                               {"endpoint": "companyconcept", "concept": "SalesRevenueNet", "httpStatus": None}])
        self.assertEqual(data["warnings"][-1]["message"],
                         f"SEC could not be read for {TICKER}: companyconcept {REVENUE} (HTTP 500), companyconcept SalesRevenueNet (no response). "
                         "Whether the fact exists is unknown; retry.")


class TestGeographicUnavailableAgrees(unittest.TestCase):
    """2.5.17: geographic_revenue returns the Worker's unshaped unavailable payload (status, code, retryable, failedReads kept)."""

    _same_payload = TestSecFactPayloadsAgree._same_payload

    def test_a_failed_read_keeps_status_and_code(self) -> None:
        data = self._same_payload("geo_read_429_filing_data")
        self.assertEqual((data["status"], data["code"], data["retryable"], data["factType"], data["concept"]), ("PROVIDER_ERROR", "SEC_READ_FAILED", True, "geographic_revenue", REVENUE))
        self.assertEqual(data["failedReads"], [{"endpoint": "companyconcept", "concept": "Revenues", "httpStatus": 429}])
        self.assertEqual(list(data)[-3:], ["_manualLookup", "retryable", "failedReads"])

    def test_a_pinned_accession_with_no_fact_is_the_full_unavailable_payload(self) -> None:
        for name in ("geo_pinned_no_fact_filing_data", "pinned_no_fact_filing_data"):
            data = self._same_payload(name)
            self.assertEqual((data["status"], data["code"], data["accessionNumber"], data["requestedAccession"], data["evidence"]), ("SEC_FACT_NOT_AVAILABLE", "NO_FACT_FOR_ACCESSION", ACCN_20F, ACCN_20F, {}), name)
            self.assertIn("_manualLookup", data)

    def test_extract_geographic_revenue_reads_a_failed_read_alike(self) -> None:
        """2.5.18: the tool returns the Worker's whole payload, status, code, scanCoverage, searchedTerms and
        notDisclosedBasis included (Python used to omit them, so a failed read showed only as the warning)."""
        local = self._same_payload("geo_read_429_tool")
        self.assertEqual((local["status"], local["code"]), ("PROVIDER_ERROR", "SEC_READ_FAILED"))
        self.assertEqual([w["code"] for w in local["warnings"]], ["SEC_READ_FAILED"])
        self.assertEqual(list(local)[-7:], ["calculation", "scanCoverage", "searchedTerms", "notDisclosedBasis", "status", "code", "warnings"])
        # Neither runtime starts the 20-F fallback (confidence is NOT_DECISION_GRADE, not NOT_DISCLOSED) or adds POSSIBLE_20F_FILER.
        self.assertEqual(local["confidence"], "NOT_DECISION_GRADE")


class TestUnreadableTickerIndexAgrees(unittest.TestCase):
    """2.5.18: a ticker with no CIK is PROVIDER_ERROR when SEC's ticker index could not be read, and a missing fact when it was."""

    _same_payload = TestSecFactPayloadsAgree._same_payload

    def test_an_unreadable_ticker_index_is_a_failed_read(self) -> None:
        for name in ("nocik_unreachable", "nocik_unreachable_network", "nocik_unreachable_pinned"):
            data = self._same_payload(f"{name}_filing_data")
            self.assertEqual((data["status"], data["code"], data["retryable"]), ("PROVIDER_ERROR", "SEC_LOOKUP_UNAVAILABLE", True), name)
            self.assertEqual((data["source"], data["confidence"], data["value"], data["evidence"]), ("NONE", "NOT_DECISION_GRADE", None, None), name)
            self.assertEqual([w["code"] for w in data["warnings"]], ["SEC_LOOKUP_UNAVAILABLE"], name)
            self.assertEqual(data["warnings"][0]["message"], "SEC's ticker index could not be read, so ZZZZ was not resolved to a CIK; retry.", name)
            total = self._same_payload(f"{name}_total_revenue")
            # extract_total_revenue keeps the failed read: it is not normalised to NOT_FOUND any more.
            self.assertEqual((total["status"], total["code"], total["retryable"], total["value"]), ("PROVIDER_ERROR", "SEC_LOOKUP_UNAVAILABLE", True, None), name)
            self.assertEqual(total["warnings"], data["warnings"], name)

    def test_a_ticker_the_index_does_not_list_is_no_registrant(self) -> None:
        for name in ("nocik_no_registrant", "nocik_no_registrant_pinned"):
            data = self._same_payload(f"{name}_filing_data")
            self.assertEqual((data["status"], data["code"], data["retryable"]), ("SEC_FACT_NOT_AVAILABLE", "NO_SEC_REGISTRANT", False), name)
            self.assertEqual(data["warnings"][0]["message"], "SEC has no registrant CIK for ticker ZZZZ.", name)
            total = self._same_payload(f"{name}_total_revenue")
            self.assertEqual((total["status"], total["code"]), ("NOT_FOUND", "NO_SEC_REGISTRANT"), name)
            self.assertNotIn("retryable", total, name)

    def test_the_pinned_accession_is_kept_on_both(self) -> None:
        for name in ("nocik_unreachable_pinned", "nocik_no_registrant_pinned"):
            data = self._same_payload(f"{name}_filing_data")
            self.assertEqual((data["accessionNumber"], data["requestedAccession"]), (ACCN_10K, ACCN_10K), name)

    def test_geographic_revenue_keeps_the_status_through_the_geographic_shape(self) -> None:
        """The geographic shape (withGeoShape / _geo_shape) keeps status and code, and reads the null evidence as {}."""
        down = self._same_payload("nocik_unreachable_geo_filing_data")
        self.assertEqual((down["status"], down["code"], down["evidence"], down["searchedTerms"], down["scanCoverage"]), ("PROVIDER_ERROR", "SEC_LOOKUP_UNAVAILABLE", {}, [], None))
        missing = self._same_payload("nocik_no_registrant_geo_filing_data")
        self.assertEqual((missing["status"], missing["code"]), ("SEC_FACT_NOT_AVAILABLE", "NO_SEC_REGISTRANT"))


# What each geographic scenario answers, by call: get_filing_data and extract_geographic_revenue (status, code),
# extract_revenue_exposure (status, code), extract_china_exposure (overallStatus, code). The Worker is the reference.
_FND = "FILING_NOT_FOUND_TRY_OTHER_TYPE"
_GEO_EXPECTED: dict[str, dict[str, tuple[str, str | None]]] = {
    "xbrl_found": {"get_filing_data": ("FOUND", None), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None), "extract_china_exposure": ("FOUND_REVENUE_EXPOSURE", None)},
    "xbrl_no_denominator": {"get_filing_data": ("FOUND", None), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None), "extract_china_exposure": ("FOUND_REVENUE_EXPOSURE", None)},
    "xbrl_half_up": {"get_filing_data": ("FOUND", None), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None), "extract_china_exposure": ("FOUND_REVENUE_EXPOSURE", None)},
    "primary_document_missing": {"get_filing_data": ("FILING_TEXT_NOT_AVAILABLE", "FILING_TEXT_NOT_AVAILABLE"), "extract_geographic_revenue": ("FILING_TEXT_NOT_AVAILABLE", "FILING_TEXT_NOT_AVAILABLE"), "extract_revenue_exposure": ("FILING_TEXT_NOT_AVAILABLE", "FILING_TEXT_NOT_AVAILABLE"), "extract_china_exposure": ("FILING_TEXT_NOT_AVAILABLE", "FILING_TEXT_NOT_AVAILABLE")},
    "primary_document_is_xbrl": {"get_filing_data": ("FILING_TEXT_NOT_AVAILABLE", "FILING_TEXT_NOT_AVAILABLE"), "extract_geographic_revenue": ("FILING_TEXT_NOT_AVAILABLE", "FILING_TEXT_NOT_AVAILABLE"), "extract_revenue_exposure": ("FILING_TEXT_NOT_AVAILABLE", "FILING_TEXT_NOT_AVAILABLE"), "extract_china_exposure": ("FILING_TEXT_NOT_AVAILABLE", "FILING_TEXT_NOT_AVAILABLE")},
    "html_astral_characters": {"get_filing_data": ("NOT_DISCLOSED", None), "extract_geographic_revenue": ("NOT_DISCLOSED", None), "extract_revenue_exposure": ("NOT_DISCLOSED", None), "extract_china_exposure": ("NOT_DISCLOSED", None)},
    "html_found": {"get_filing_data": ("FOUND", None), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None), "extract_china_exposure": ("FOUND_REVENUE_EXPOSURE", None)},
    "html_found_japan": {"get_filing_data": ("FOUND", None), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None)},
    "html_found_20f_only": {"get_filing_data": ("FOUND", None), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None), "extract_china_exposure": ("FOUND_REVENUE_EXPOSURE", None)},
    "html_not_disclosed": {"get_filing_data": ("NOT_DISCLOSED", None), "extract_geographic_revenue": ("NOT_DISCLOSED", None), "extract_revenue_exposure": ("NOT_DISCLOSED", None), "extract_china_exposure": ("NOT_DISCLOSED", None)},
    "html_table_not_parsed": {"get_filing_data": ("EXTRACTION_FAILED", "EXTRACTION_FAILED"), "extract_geographic_revenue": ("EXTRACTION_FAILED", "EXTRACTION_FAILED"), "extract_revenue_exposure": ("EXTRACTION_FAILED", "EXTRACTION_FAILED"), "extract_china_exposure": ("FOUND_NON_REVENUE_EXPOSURE", "EXTRACTION_FAILED")},
    "html_text_unavailable": {"get_filing_data": ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE"), "extract_geographic_revenue": ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE"), "extract_revenue_exposure": ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE"), "extract_china_exposure": ("EXTRACTION_FAILED", "EXTRACTION_FAILED")},
    "html_text_fault": {"get_filing_data": ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE"), "extract_geographic_revenue": ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE"), "extract_revenue_exposure": ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE"), "extract_china_exposure": ("EXTRACTION_FAILED", "EXTRACTION_FAILED")},
    "filing_not_found": {"get_filing_data": (_FND, _FND), "extract_geographic_revenue": (_FND, _FND), "extract_revenue_exposure": (_FND, _FND), "extract_china_exposure": (_FND, _FND)},
    "html_20f_switch": {"get_filing_data": ("NOT_DISCLOSED", None), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None), "extract_china_exposure": ("FOUND_REVENUE_EXPOSURE", None)},
    "html_20f_also_silent": {"get_filing_data": ("NOT_DISCLOSED", None), "extract_geographic_revenue": ("NOT_DISCLOSED", None), "extract_revenue_exposure": ("NOT_DISCLOSED", None), "extract_china_exposure": ("NOT_DISCLOSED", None)},
    "html_20f_also_text_only": {"get_filing_data": ("NOT_DISCLOSED", None), "extract_geographic_revenue": ("NOT_DISCLOSED", None), "extract_revenue_exposure": ("NOT_DISCLOSED", None), "extract_china_exposure": ("NOT_DISCLOSED", None)},
    "read_429": {"get_filing_data": ("PROVIDER_ERROR", "SEC_READ_FAILED"), "extract_geographic_revenue": ("PROVIDER_ERROR", "SEC_READ_FAILED"), "extract_revenue_exposure": ("PROVIDER_ERROR", "SEC_READ_FAILED"), "extract_china_exposure": ("PROVIDER_ERROR", "SEC_READ_FAILED")},
    "read_429_with_risk_text": {"get_filing_data": ("PROVIDER_ERROR", "SEC_READ_FAILED"), "extract_geographic_revenue": ("PROVIDER_ERROR", "SEC_READ_FAILED"), "extract_revenue_exposure": ("PROVIDER_ERROR", "SEC_READ_FAILED"), "extract_china_exposure": ("FOUND_NON_REVENUE_EXPOSURE", "SEC_READ_FAILED")},
    "pinned_older_filing": {"get_filing_data": ("FOUND", None), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None), "extract_china_exposure": ("FOUND_REVENUE_EXPOSURE", None)},
    # The tools do not pass the pin to get_filing_data, so they read the latest 10-K (which has its document here).
    "pinned_older_filing_no_doc": {"get_filing_data": ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE"), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None), "extract_china_exposure": ("FOUND_REVENUE_EXPOSURE", None)},
    "pinned_filing_not_listed": {"get_filing_data": (_FND, _FND), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None), "extract_china_exposure": (_FND, _FND)},
    "china_manufacturing_text": {"get_filing_data": ("EXTRACTION_FAILED", "EXTRACTION_FAILED"), "extract_geographic_revenue": ("EXTRACTION_FAILED", "EXTRACTION_FAILED"), "extract_revenue_exposure": ("EXTRACTION_FAILED", "EXTRACTION_FAILED"), "extract_china_exposure": ("FOUND_NON_REVENUE_EXPOSURE", "EXTRACTION_FAILED")},
    "china_bank_risk_text": {"get_filing_data": ("EXTRACTION_FAILED", "EXTRACTION_FAILED"), "extract_geographic_revenue": ("EXTRACTION_FAILED", "EXTRACTION_FAILED"), "extract_revenue_exposure": ("EXTRACTION_FAILED", "EXTRACTION_FAILED"), "extract_china_exposure": ("FOUND_NON_REVENUE_EXPOSURE", "EXTRACTION_FAILED")},
    "pinned_fallback_no_doc": {"get_filing_data": ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE"), "extract_geographic_revenue": ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE"), "extract_revenue_exposure": ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE"), "extract_china_exposure": ("EXTRACTION_FAILED", "EXTRACTION_FAILED")},
    "pinned_fallback_doc": {"get_filing_data": ("FOUND", None), "extract_geographic_revenue": ("FOUND", None), "extract_revenue_exposure": ("FOUND_REVENUE_EXPOSURE", None), "extract_china_exposure": ("FOUND_REVENUE_EXPOSURE", None)},
    "lookup_unavailable": {"get_filing_data": ("PROVIDER_ERROR", "SEC_LOOKUP_UNAVAILABLE"), "extract_geographic_revenue": ("PROVIDER_ERROR", "SEC_LOOKUP_UNAVAILABLE"), "extract_revenue_exposure": ("PROVIDER_ERROR", "SEC_LOOKUP_UNAVAILABLE"), "extract_china_exposure": ("PROVIDER_ERROR", "SEC_LOOKUP_UNAVAILABLE")},
    "no_registrant": {"get_filing_data": ("SEC_FACT_NOT_AVAILABLE", "NO_SEC_REGISTRANT"), "extract_geographic_revenue": ("SEC_FACT_NOT_AVAILABLE", "NO_SEC_REGISTRANT"), "extract_revenue_exposure": ("NOT_FOUND", "NO_SEC_REGISTRANT"), "extract_china_exposure": ("NOT_FOUND", None)},
}
_GEO_KEYS = ["ticker", "factType", "region", "period", "rawValue", "rawDenominator", "unit", "unitScale", "value", "denominator", "valueRatio", "valuePct",
             "extractionMethod", "source", "confidence", "filingType", "filingDate", "accessionNumber", "documentUrl", "indexUrl", "primaryDocumentUrl",
             "evidence", "calculation", "scanCoverage", "searchedTerms", "notDisclosedBasis", "status", "code", "xbrlContext", "warnings"]
_GEO_TOOL_KEYS = ["ticker", "factType", "region", "period", "rawValue", "rawDenominator", "unit", "unitScale", "value", "denominator", "valueRatio", "valuePct",
                  "extractionMethod", "confidence", "evidence", "calculation", "scanCoverage", "searchedTerms", "notDisclosedBasis", "status", "code", "warnings"]
_EXPOSURE_KEYS = ["ticker", "query", "matches", "status", "code", "requestedFilingType", "filingType", "filingDate", "accessionNumber", "documentUrl",
                  "availableFilingTypes", "suggestedFilingTypes", "warnings"]
_CHINA_KEYS = ["ticker", "exposureType", "filingType", "filingDate", "accessionNumber", "documentUrl", "revenueExposure", "manufacturingExposure", "entityExposure",
               "bankExposure", "riskFactorExposure", "overallStatus", "code", "warnings"]


class TestGeographicToolsAgree(unittest.TestCase):
    """2.5.18: get_filing_data (geographic_revenue), extract_geographic_revenue, extract_revenue_exposure and
    extract_china_exposure return the Worker's JSON on every path, key order included."""

    _same_payload = TestSecFactPayloadsAgree._same_payload

    def test_every_scenario_is_the_same_json_in_both_runtimes(self) -> None:
        for scenario, expected in _GEO_EXPECTED.items():
            for call, (status, code) in expected.items():
                name = f"geo_{scenario}__{call}"
                got = self._same_payload(name)
                self.assertEqual((got["overallStatus"] if call == "extract_china_exposure" else got["status"], got["code"]), (status, code), name)

    def test_every_scenario_is_listed(self) -> None:
        self.assertEqual(set(_GEO_EXPECTED), set(GEO_SCENARIOS))
        for scenario, expected in _GEO_EXPECTED.items():
            calls = [c for c in _GEO_CALLS if c != "extract_china_exposure" or GEO_SCENARIOS[scenario]["region"] == "China"]
            self.assertEqual(list(expected), calls, scenario)

    def test_key_order(self) -> None:
        """Each tool's keys, in the Worker's order, on paths that differ in which keys they fill."""
        for scenario in ("xbrl_found", "html_found", "html_not_disclosed", "html_table_not_parsed", "html_text_unavailable", "filing_not_found", "lookup_unavailable", "no_registrant"):
            self.assertEqual(list(_python(f"geo_{scenario}__extract_geographic_revenue")), _GEO_TOOL_KEYS, scenario)
            self.assertEqual(list(_python(f"geo_{scenario}__extract_revenue_exposure")), _EXPOSURE_KEYS, scenario)
            self.assertEqual(list(_python(f"geo_{scenario}__extract_china_exposure"))[:len(_CHINA_KEYS)], _CHINA_KEYS[:len(_CHINA_KEYS)] if scenario != "filing_not_found" else list(_python(f"geo_{scenario}__extract_china_exposure"))[:len(_CHINA_KEYS)], scenario)
        # get_filing_data's geographic shape: the same keys however the read ended (the no-CIK and unavailable payloads add
        # _manualLookup-style keys only on the unshaped paths, which are compared whole above).
        for scenario in ("xbrl_found", "html_found", "html_not_disclosed", "html_table_not_parsed", "html_text_unavailable", "filing_not_found", "lookup_unavailable", "no_registrant"):
            keys = [*_GEO_KEYS[:_GEO_KEYS.index("code") + 1], "retryable", *_GEO_KEYS[_GEO_KEYS.index("code") + 1:]] if scenario in ("lookup_unavailable", "no_registrant") else _GEO_KEYS
            self.assertEqual(list(_python(f"geo_{scenario}__get_filing_data")), keys, scenario)

    def test_xbrl_found_carries_its_context(self) -> None:
        got = self._same_payload("geo_xbrl_found__get_filing_data")
        self.assertEqual((got["value"], got["denominator"], got["valueRatio"], got["valuePct"], got["extractionMethod"], got["confidence"]), (400000, 1000000, 0.4, 40, "XBRL", "HIGH"))
        self.assertEqual((got["xbrlContext"]["concept"], got["scanCoverage"], got["searchedTerms"], got["notDisclosedBasis"]), (REVENUE, None, [], None))
        nothing = self._same_payload("geo_xbrl_no_denominator__get_filing_data")
        self.assertEqual((nothing["confidence"], nothing["valueRatio"], [w["code"] for w in nothing["warnings"]]), ("LOW", None, ["DENOMINATOR_NOT_FOUND"]))

    def test_html_table_found_has_the_full_shape(self) -> None:
        """The HTML path carries status, code and xbrlContext like every other path (Python's was narrower)."""
        got = self._same_payload("geo_html_found__get_filing_data")
        self.assertEqual((got["status"], got["code"], got["xbrlContext"], got["extractionMethod"], got["source"]), ("FOUND", None, None, "PARSED_TABLE", "PARSED_TABLE"))
        self.assertEqual((got["value"], got["denominator"], got["valuePct"], got["unitScale"], got["filingType"], got["accessionNumber"]), (300000000, 1000000000, 30, "millions", "10-K", ACCN_10K))
        self.assertEqual(got["evidence"]["sourceRows"], [["China", "300"], ["Total revenue", "1,000"]])
        self.assertEqual((got["scanCoverage"], got["searchedTerms"], got["notDisclosedBasis"]), (None, [], None))

    def test_not_disclosed_says_what_was_scanned(self) -> None:
        got = self._same_payload("geo_html_not_disclosed__get_filing_data")
        self.assertEqual((got["status"], got["source"], got["confidence"], got["code"]), ("NOT_DISCLOSED", "NOT_DISCLOSED", "NOT_DISCLOSED", None))
        self.assertEqual(got["evidence"], {"sourceType": "sec_filing", "filingType": "10-K", "filingDate": "2026-02-01", "accessionNumber": ACCN_10K, "documentUrl": DOC_10K})
        self.assertEqual(got["scanCoverage"], {"sourceType": "sec_primary_html", "documentUrl": DOC_10K, "charsScanned": len(_SILENT_HTML), "maxCharsRequested": 12000000,
                                               "filingReadTruncated": False, "relevantGeoTextFound": False, "searchedTerms": got["searchedTerms"]})
        self.assertEqual(got["searchedTerms"][:2], ["china", "mainland china"])
        self.assertTrue(got["notDisclosedBasis"].startswith("Resolved and scanned the primary SEC filing HTML"))

    def test_relevant_text_without_a_table_is_extraction_failed(self) -> None:
        got = self._same_payload("geo_html_table_not_parsed__get_filing_data")
        self.assertEqual((got["status"], got["code"], got["confidence"], got["scanCoverage"]["relevantGeoTextFound"]), ("EXTRACTION_FAILED", "EXTRACTION_FAILED", "EXTRACTION_FAILED", True))
        self.assertEqual([w["code"] for w in got["warnings"]], ["TABLE_NOT_PARSED"])

    def test_an_unreadable_filing_document_is_filing_text_not_available(self) -> None:
        """Python answered NOT_DISCLOSED with empty evidence when the document could not be read; the Worker says it could not be read."""
        for scenario in ("html_text_unavailable", "html_text_fault", "pinned_fallback_no_doc"):
            got = self._same_payload(f"geo_{scenario}__get_filing_data")
            self.assertEqual((got["status"], got["code"], got["source"], got["confidence"]), ("EXTRACTION_FAILED", "FILING_TEXT_NOT_AVAILABLE", "EXTRACTION_FAILED", "EXTRACTION_FAILED"), scenario)
            self.assertEqual(got["evidence"]["accessionNumber"], ACCN_10K, scenario)
            self.assertEqual([w["code"] for w in got["warnings"]], ["FILING_TEXT_NOT_AVAILABLE"], scenario)

    def test_a_pinned_read_falls_back_to_the_latest_filing_of_the_form(self) -> None:
        """The HTML fallback does not take the pin: a pinned accession whose facts name no region reads the latest 10-K's document."""
        got = self._same_payload("geo_pinned_fallback_doc__get_filing_data")
        self.assertEqual((got["status"], got["value"], got["accessionNumber"]), ("FOUND", 300000000, ACCN_10K))
        self.assertNotIn("requestedAccession", got)

    def test_filing_not_found(self) -> None:
        got = self._same_payload("geo_filing_not_found__get_filing_data")
        self.assertEqual((got["status"], got["code"], got["source"], got["confidence"], got["evidence"], got["filingType"]), (_FND, _FND, _FND, _FND, {}, "10-K"))
        self.assertEqual(got["warnings"], [{"code": _FND, "message": "No 10-K filing found for 'XYZ'.", "severity": "error"}])
        exposure = self._same_payload("geo_filing_not_found__extract_revenue_exposure")
        self.assertEqual((exposure["status"], exposure["code"], exposure["matches"]), (_FND, _FND, []))

    def test_a_10k_that_is_not_filed_reads_the_20f(self) -> None:
        got = self._same_payload("geo_html_found_20f_only__get_filing_data")
        self.assertEqual((got["status"], got["filingType"], got["accessionNumber"], [w["code"] for w in got["warnings"]]), ("FOUND", "20-F", ACCN_20F, ["AUTO_20F_FALLBACK"]))

    def test_the_20f_fallback_replaces_a_not_disclosed_10k(self) -> None:
        got = self._same_payload("geo_html_20f_switch__extract_geographic_revenue")
        self.assertEqual((got["status"], got["confidence"], got["value"], got["code"]), ("FOUND", "HIGH", 300000000, None))
        self.assertEqual((got["evidence"]["filingType"], got["evidence"]["accessionNumber"], got["evidence"]["sourceRows"]), ("20-F", ACCN_20F, [["China", "300"], ["Total revenue", "1,000"]]))
        self.assertEqual([w["code"] for w in got["warnings"]], ["AUTO_20F_FALLBACK"])
        self.assertEqual(self._same_payload("geo_html_20f_switch__get_filing_data")["status"], "NOT_DISCLOSED")

    def test_a_20f_that_does_not_help_adds_possible_20f_filer(self) -> None:
        for scenario in ("html_20f_also_silent", "html_20f_also_text_only"):
            got = self._same_payload(f"geo_{scenario}__extract_geographic_revenue")
            self.assertEqual((got["status"], [w["code"] for w in got["warnings"]]), ("NOT_DISCLOSED", ["POSSIBLE_20F_FILER"]), scenario)

    def test_a_failed_read_is_provider_error_through_every_tool(self) -> None:
        for call in ("get_filing_data", "extract_geographic_revenue", "extract_revenue_exposure"):
            got = self._same_payload(f"geo_read_429__{call}")
            self.assertEqual((got["status"], got["code"], [w["code"] for w in got["warnings"]]), ("PROVIDER_ERROR", "SEC_READ_FAILED", ["SEC_READ_FAILED"]), call)
        # Through extract_china_exposure a failed revenue read is the overall status and the code (2.5.18), unless other exposure was found.
        china = self._same_payload("geo_read_429__extract_china_exposure")
        self.assertEqual((china["revenueExposure"]["status"], china["revenueExposure"]["confidence"], china["overallStatus"], china["code"]), ("PROVIDER_ERROR", "LOW", "PROVIDER_ERROR", "SEC_READ_FAILED"))
        self.assertEqual([w["code"] for w in china["warnings"]], ["SEC_READ_FAILED"])

    def test_an_unreadable_ticker_index_is_provider_error_through_every_tool(self) -> None:
        for call in ("get_filing_data", "extract_geographic_revenue", "extract_revenue_exposure"):
            got = self._same_payload(f"geo_lookup_unavailable__{call}")
            self.assertEqual((got["status"], got["code"]), ("PROVIDER_ERROR", "SEC_LOOKUP_UNAVAILABLE"), call)
        # The geographic shape keeps retryable (2.5.18), right after code, and has no _manualLookup key.
        shaped = self._same_payload("geo_lookup_unavailable__get_filing_data")
        self.assertIs(shaped["retryable"], True)
        self.assertEqual(list(shaped)[list(shaped).index("code") + 1], "retryable")
        self.assertNotIn("_manualLookup", shaped)
        self.assertTrue(shaped["warnings"][0]["message"].endswith("retry."))

    def test_china_exposure_stops_at_a_missing_filing(self) -> None:
        for name in ("geo_filing_not_found__extract_china_exposure", "geo_china_pinned_accession_missing"):
            got = self._same_payload(name)
            self.assertEqual(list(got), _CHINA_KEYS[:13] + ["requestedFilingType", "availableFilingTypes", "suggestedFilingTypes", "warnings"], name)
            self.assertEqual((got["overallStatus"], got["code"], got["revenueExposure"]["status"], got["filingDate"], got["accessionNumber"], got["documentUrl"]), (_FND, _FND, _FND, None, None, None), name)
            self.assertEqual((got["requestedFilingType"], got["availableFilingTypes"], got["suggestedFilingTypes"]), ("10-K", ["10-K", "10-Q"] if "pinned" in name else ["10-Q"], []), name)
        # The pinned accession is listed: the read goes on.
        self.assertEqual(self._same_payload("geo_china_pinned_accession_listed")["overallStatus"], "FOUND_REVENUE_EXPOSURE")

    def test_javascript_rounding_and_string_lengths(self) -> None:
        half = self._same_payload("geo_xbrl_half_up__get_filing_data")
        self.assertEqual((half["valueRatio"], half["valuePct"], half["calculation"]["resultPct"]), (0.0313, 3.13, 3.13))  # Python's round gave 0.0312
        astral = self._same_payload("geo_html_astral_characters__get_filing_data")
        self.assertEqual(astral["scanCoverage"]["charsScanned"], len(_SILENT_HTML) + 3)  # a space and an emoji: 3 UTF-16 code units, 2 characters

    def test_an_unreadable_primary_document_is_the_filing_text_error_with_its_metadata(self) -> None:
        for scenario, code in (("primary_document_missing", "PRIMARY_DOCUMENT_MISSING"), ("primary_document_is_xbrl", "PRIMARY_HTML_NOT_FOUND")):
            got = self._same_payload(f"geo_{scenario}__get_filing_data")
            self.assertEqual((got["status"], got["code"], got["accessionNumber"], [w["code"] for w in got["warnings"]]), ("FILING_TEXT_NOT_AVAILABLE", "FILING_TEXT_NOT_AVAILABLE", ACCN_10K, [code]), scenario)
            china = self._same_payload(f"geo_{scenario}__extract_china_exposure")
            # The filing metadata is the lookup error's (the Worker reads the failed resolve as the index).
            self.assertEqual((china["filingType"], china["accessionNumber"], china["filingDate"]), ("10-K", ACCN_10K, "2026-02-01"), scenario)

    def test_a_pinned_read_falls_back_to_the_pinned_filings_table(self) -> None:
        """2.5.18: the HTML fallback reads the pinned accession's document, not the latest filing of the form."""
        got = self._same_payload("geo_pinned_older_filing__get_filing_data")
        self.assertEqual((got["status"], got["value"], got["accessionNumber"], got["documentUrl"]), ("FOUND", 300000000, ACCN_10K_OLD, DOC_10K_OLD))
        self.assertNotIn("requestedAccession", got)
        missing = self._same_payload("geo_pinned_older_filing_no_doc__get_filing_data")
        self.assertEqual((missing["code"], missing["accessionNumber"], missing["documentUrl"]), ("FILING_TEXT_NOT_AVAILABLE", ACCN_10K_OLD, DOC_10K_OLD))
        unlisted = self._same_payload("geo_pinned_filing_not_listed__get_filing_data")
        self.assertEqual((unlisted["status"], unlisted["code"]), (_FND, _FND))

    def test_china_exposure_reads_manufacturing_and_bank_of_china_from_the_text(self) -> None:
        manufacturing = self._same_payload("geo_china_manufacturing_text__extract_china_exposure")
        self.assertEqual(manufacturing["manufacturingExposure"]["status"], "FOUND")
        self.assertEqual([(e["source"], e["term"], e["sectionHeading"]) for e in manufacturing["manufacturingExposure"]["evidence"]], [("text", "manufacturing", "Business")])
        bank = self._same_payload("geo_china_bank_risk_text__extract_china_exposure")
        self.assertEqual([e["term"] for e in bank["riskFactorExposure"]["evidence"]], ["China", "Bank of China"])

    def test_a_failed_revenue_read_yields_to_other_exposure_but_keeps_its_code(self) -> None:
        got = self._same_payload("geo_read_429_with_risk_text__extract_china_exposure")
        self.assertEqual((got["overallStatus"], got["code"], got["revenueExposure"]["status"]), ("FOUND_NON_REVENUE_EXPOSURE", "SEC_READ_FAILED", "PROVIDER_ERROR"))

    def test_china_exposure_reports_a_named_revenue_limitation_as_its_code(self) -> None:
        for scenario, status in (("html_table_not_parsed", "FOUND_NON_REVENUE_EXPOSURE"), ("html_text_unavailable", "EXTRACTION_FAILED")):
            got = self._same_payload(f"geo_{scenario}__extract_china_exposure")
            self.assertEqual((got["overallStatus"], got["code"], got["revenueExposure"]["status"], got["revenueExposure"]["confidence"]), (status, "EXTRACTION_FAILED", "EXTRACTION_FAILED", "EXTRACTION_FAILED"), scenario)


_PARSE_REJECTED = {"stated_share_disagrees", "square_feet_list", "no_revenue_lead_in", "region_exceeds_total", "region_is_total_row", "region_not_in_table",
                   "one_row_table", "no_table", "empty_html"}


class TestGeoTableReaderAgrees(unittest.TestCase):
    """2.5.18: Python's table reader is the Worker's extractGeoRevenueFromHtml, so a filing's table reads the same in both."""

    def test_every_table_reads_the_same(self) -> None:
        for name in PARSE_CASES:
            worker, local = _worker(f"parse_{name}"), _python(f"parse_{name}")
            self.assertEqual(json.dumps(worker), json.dumps(local), name)
            self.assertEqual(worker is None, name in _PARSE_REJECTED, name)

    def test_trimmed_live_10k_tables(self) -> None:
        """AAOI, AXTI and QCOM 10-K tables (attributes stripped): the Worker reads them, and Python used to read none."""
        expected = {"aaoi": (0.5752, 262140000, 455715000, [["China", "262,140"], ["Total (unlabeled row)", "$455,715"]], ["Year ended December 31, 2025"]),
                    "axti": (0.6236, 55076000, 88326000, [["China", "$55,076"], ["Total revenue", "$88,326"]], ["($ in thousands)"]),
                    "qcom": (0.4593, 20340000000, 44284000000, [["China (including Hong Kong)", "$20,340"], ["Total (unlabeled row)", "$44,284"]], ["2025"])}
        for name, (pct, usd, total, rows, columns) in expected.items():
            for got in (_worker(f"parse_live_{name}"), _python(f"parse_live_{name}")):
                self.assertEqual((got["pct"], got["usd"], got["denominator"], got["sourceRows"], got["sourceColumns"]), (pct, usd, total, rows, columns), name)

    def test_hong_kong_is_not_added_to_china(self) -> None:
        for name, row in (("china_and_hong_kong", "China"), ("mainland_and_hong_kong", "Mainland China")):
            for got in (_worker(f"parse_{name}"), _python(f"parse_{name}")):
                self.assertEqual((got["pct"], got["usd"], got["sourceRows"][0]), (0.3, 300000000, [row, "300"]), name)

    def test_the_tuple_form_keeps_its_contract(self) -> None:
        from yfmcp.parsing.extractors import _extract_geo_revenue_from_html
        ratio, usd, total, heading, evidence = _extract_geo_revenue_from_html(PARSE_CASES["thousands"][0], "China")
        self.assertEqual((ratio, usd, total, heading), (0.6236, 55076000, 88326000, "Geographic Areas"))
        self.assertEqual(list(evidence), ["sectionHeading", "tableTitle", "sourceTableId", "sourceRows", "sourceColumns", "unitScale", "rawValue", "rawDenominator"])
        self.assertEqual(_extract_geo_revenue_from_html("", "China"), (None, None, None, "", None))


class TestAsStatusAgrees(unittest.TestCase):
    """_as_status is the Worker's normalizeStatus (not exported: its source is sliced from yahoo-finance.ts and run in Node)."""

    PAYLOADS = [
        {}, {"status": "FILING_NOT_FOUND_TRY_OTHER_TYPE"}, {"code": "filing_text_not_available"}, {"status": "EXTRACTION_FAILED"},
        {"status": "table_not_parsed"}, {"code": "PROVIDER_LIMITATION"}, {"status": "NO_DIMENSIONAL_REVENUE_FACT"}, {"status": "PROVIDER_ERROR"},
        {"code": "PROVIDER_ERROR"}, {"status": "FOUND", "code": "PROVIDER_ERROR"}, {"status": None, "code": "EXTRACTION_FAILED"}, {"status": "", "code": "EXTRACTION_FAILED"},
        {"source": "FILING_NOT_FOUND_TRY_OTHER_TYPE"}, {"confidence": "filing_not_found_try_other_type"}, {"source": "EXTRACTION_FAILED"},
        {"confidence": "TABLE_NOT_PARSED"}, {"source": "PROVIDER_LIMITATION"}, {"confidence": "NO_DIMENSIONAL_REVENUE_FACT"},
        {"source": "FILING_TEXT_NOT_AVAILABLE"}, {"confidence": "NOT_DISCLOSED"}, {"source": "NOT_DISCLOSED", "confidence": "CONFLICTING"},
        {"source": "CONFLICTING"}, {"source": "NOT_DISCLOSED", "confidence": "EXTRACTION_FAILED"}, {"confidence": "NOT_DECISION_GRADE"},
        {"status": "SEC_FACT_NOT_AVAILABLE", "code": "PROVIDER_ERROR"}, {"status": "SEC_FACT_NOT_AVAILABLE", "source": "SEC_COMPANYCONCEPT"},
        {"status": "PROVIDER_ERROR", "confidence": "NOT_DISCLOSED"}, {"status": "FOUND", "confidence": "HIGH"},
    ]

    def test_every_branch(self) -> None:
        node = _node()
        source = (WORKER / "src" / "yahoo-finance.ts").read_text(encoding="utf-8")
        start = source.index("function normalizeStatus(")
        body = source[start:source.index("\n}\n", start) + 3]
        js = subprocess.run([str(ESBUILD), "--loader=ts", "--log-level=error"], input=body, capture_output=True, text=True, check=True, timeout=60).stdout
        script = js + "\nconst cases = JSON.parse(process.argv[1]);\nconsole.log(JSON.stringify(cases.map(normalizeStatus)));\n"
        out = subprocess.run([node, "--input-type=module", "-e", script, json.dumps(self.PAYLOADS)], capture_output=True, text=True, check=True, timeout=60)
        expected = json.loads(out.stdout.strip().splitlines()[-1])
        self.assertEqual([srv._as_status(p) for p in self.PAYLOADS], expected)
        self.assertGreaterEqual(len(set(expected)), 8)


class TestNamedFiscalYearAgrees(unittest.TestCase):
    """2.5.16: period is "latest" or a fiscal year, and both runtimes answer the same for BE-like annual facts."""

    _same = TestSecFactPayloadsAgree._same
    _same_payload = TestSecFactPayloadsAgree._same_payload

    def test_a_named_year_selects_its_period_not_the_first_row(self) -> None:
        # The first companyconcept row is the 2016 revenue (208,540,000): "FY2025" once returned it labelled FY2018.
        for name in ("be_fy2025", "be_2025", "be_fy_lowercase_spaced", "be_latest"):
            got = self._same(name)
            self.assertEqual((got["status"], got["value"], got["period"], got["decisionGrade"]), ("FOUND", 2001614000, "FY2025", True), name)
            self.assertEqual((got["evidence.accessionNumber"], got["warningCodes"]), (ACCN_10K_2025, []), name)

    def test_an_earlier_year_keeps_its_own_label_when_a_later_filing_carries_it(self) -> None:
        got = self._same("be_fy2016")
        self.assertEqual((got["value"], got["period"]), (208540000, "FY2016"))
        self.assertEqual(got["evidence.accessionNumber"], ACCN_10K_2018)

    def test_a_restated_year_takes_the_latest_filed_value(self) -> None:
        got = self._same("be_fy2024_restated")
        self.assertEqual((got["value"], got["period"], got["evidence.accessionNumber"]), (1330500000, "FY2024", ACCN_10K_2025))

    def test_a_year_that_is_not_there_is_period_not_found(self) -> None:
        got = self._same("be_fy2030")
        self.assertEqual((got["status"], got["code"], got["value"], got["unit"], got["decisionGrade"]), ("NOT_FOUND", "PERIOD_NOT_FOUND", None, None, False))
        self.assertEqual(got["warningCodes"], ["PERIOD_NOT_FOUND"])
        self.assertIn("fiscal years found: 2016, 2017, 2018, 2023, 2024, 2025.", got["warningMessages"][0])
        self.assertIn(f"No annual {REVENUE} fact is fiscal year 2030", got["warningMessages"][0])
        self.assertEqual(got["evidence.filingType"], "10-K")

    def test_a_52_53_week_year_is_the_issuers_year(self) -> None:
        got = self._same("dg_fy2025")
        self.assertEqual((got["value"], got["period"]), (41000000000, "FY2025"))
        got = self._same("dg_fy2026")
        self.assertEqual((got["code"], got["value"]), ("PERIOD_NOT_FOUND", None))
        self.assertIn("fiscal years found: 2024, 2025.", got["warningMessages"][0])

    def test_a_quarterly_filing_type_cannot_name_a_fiscal_year(self) -> None:
        got = self._same("q_fy2025")
        self.assertEqual((got["status"], got["code"], got["value"], got["decisionGrade"]), ("NOT_FOUND", "INVALID_PERIOD", None, False))
        self.assertEqual(got["warningMessages"], ["period FY2025 names a fiscal year, an annual period; request filing_type 10-K or 20-F (or period_mode annual)."])
        self.assertEqual((got["evidence.accessionNumber"], got["evidence.filingType"]), (ACCN_10Q, "10-Q"))
        latest = self._same("q_latest")
        self.assertEqual((latest["value"], latest["code"]), (700000000, None))
        self._same_payload("q_fy2025_filing_data")

    def test_get_filing_data_returns_the_same_payload(self) -> None:
        got = self._same_payload("be_fy2025_filing_data")
        self.assertEqual((got["value"], got["period"]), (2001614000, "FY2025"))
        missing = self._same_payload("be_fy2030_filing_data")
        self.assertEqual(missing["code"], "PERIOD_NOT_FOUND")

    def test_an_unreadable_period_is_invalid_period(self) -> None:
        for name in ("be_junk_filing_data", "be_fy25_filing_data"):
            got = self._same_payload(name)
            self.assertEqual(list(got), ["ticker", "factType", "value", "unit", "period", "filingType", "extractionMethod", "source", "confidence",
                                         "status", "code", "decisionGrade", "requestedPeriod", "warnings"], name)
            self.assertEqual((got["status"], got["code"], got["value"], got["source"], got["extractionMethod"]), ("INVALID_PERIOD", "INVALID_PERIOD", None, "INPUT", "NONE"), name)
            self.assertEqual(got["warnings"], [{"code": "INVALID_PERIOD", "message": 'period must be "latest" or a fiscal year ("FY2025" or "2025").', "severity": "error"}], name)
        self.assertEqual(_python("be_junk_filing_data")["requestedPeriod"], "junk")

    def test_the_tools_refuse_an_unreadable_period(self) -> None:
        """Worker _dispatchTool and the Python tool functions return the same failure for each tool that reads a period."""
        for tool in _PERIOD_TOOLS:
            name = f"dispatch_junk_{tool}"
            worker, local = _worker(name), _python(name)
            for key in ("ok", "data", "error"):
                self.assertEqual(worker[key], local[key], f"{name}: {key}")
            self.assertEqual(worker["meta"]["tool"], local["meta"]["tool"], name)
            self.assertEqual((local["ok"], local["error"]["code"]), (False, "INPUT_VALIDATION_ERROR"), name)
            self.assertEqual(local["error"]["message"], """period must be "latest" or a fiscal year ("FY2025" or "2025"). Got 'junk'.""", name)
            self.assertEqual(local["meta"]["tool"], tool, name)
        worker, local = _worker("dispatch_fy25_extract_total_revenue"), _python("dispatch_fy25_extract_total_revenue")
        self.assertEqual(worker["error"], local["error"])
        self.assertEqual(local["error"]["message"], """period must be "latest" or a fiscal year ("FY2025" or "2025"). Got 'FY25'.""")


if __name__ == "__main__":
    unittest.main()
