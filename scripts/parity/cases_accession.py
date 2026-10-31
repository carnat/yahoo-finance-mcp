"""Parity cases: exact accession, fail-closed (2.13.0, R4 Q3a; spec analysis/yahoo-finance-mcp/R4_Q3A_Q4_SPEC.md §1, §3).

An explicit accession that is not among the company's recent SEC filings (submissions `filings.recent`) fails closed:

    {"ok": false, "error": {"code": "ACCESSION_NOT_FOUND",
                            "message": "Accession <acc> was not found in <TICKER>'s recent SEC filings.",
                            "requestedAccession": "<acc>", "retryable": false}}

with no fallback to the latest filing or to another form (the 20-F fallback). A found accession is unchanged.

Worker side: the action functions in worker/src/yahoo-finance.ts (one shared miss branch in resolveSecFiling).
Python side: the matching server.py functions (one shared miss branch in _resolve_sec_filing).

Each case serves the same canned SEC responses to both runtimes: SEC's ticker index (the CIK; Yahoo is unreachable in
both) and one submissions list holding a 10-Q, an 8-K and an older 10-K.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from unittest.mock import patch

from scripts.parity.harness import Case

import server  # noqa: E402
from yfmcp.clients import edgar  # noqa: E402
from yfmcp.tools import sec as sec_tools  # noqa: E402

CASES: list[Case] = []

CIK = "0000320193"
TEN_Q_ACC = "0000320193-25-000073"
EIGHT_K_ACC = "0000320193-25-000070"
TEN_K_ACC = "0000320193-24-000123"
MISSING_ACC = "0000320193-99-999999"
BASE = "https://www.sec.gov/Archives/edgar/data/320193"
TEN_Q_DOC = f"{BASE}/000032019325000073/aapl-20250628.htm"
TEN_K_DOC = f"{BASE}/000032019324000123/aapl-20240928.htm"
TEN_K_INDEX_PAGE = f"{BASE}/000032019324000123/{TEN_K_ACC}-index.htm"

TEN_K_HTML = ("<html><body><h2>Item 1. Business</h2><p>Annual revenue grew in fiscal 2024.</p>"
              "<h2>Item 1A. Risk Factors</h2><p>Supply risk.</p></body></html>")
TEN_Q_HTML = ("<html><body><h2>Item 2. Management's Discussion and Analysis</h2>"
              "<p>Quarterly revenue grew.</p></body></html>")
INDEX_PAGE_HTML = "<html><body><h2>Filing Index Page</h2></body></html>"

SUBMISSIONS = {"cik": "320193", "name": "Apple Inc.", "filings": {"recent": {
    "form": ["10-Q", "8-K", "10-K"],
    "accessionNumber": [TEN_Q_ACC, EIGHT_K_ACC, TEN_K_ACC],
    "primaryDocument": ["aapl-20250628.htm", "aapl-8k.htm", "aapl-20240928.htm"],
    "filingDate": ["2025-08-01", "2025-07-31", "2024-11-01"],
    "reportDate": ["2025-06-28", "2025-07-31", "2024-09-28"],
    "acceptanceDateTime": ["2025-08-01T16:30:00.000Z", "2025-07-31T16:30:00.000Z", "2024-11-01T16:30:00.000Z"],
}}}
ROUTES = {
    "https://www.sec.gov/files/company_tickers.json": {"json": {
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 999001, "ticker": "FPI", "title": "Foreign Private Issuer Co"},
    }},
    f"https://data.sec.gov/submissions/CIK{CIK}.json": {"json": SUBMISSIONS},
    # A foreign private issuer: a 20-F and no 10-K, so a 10-K request without an accession falls to the 20-F.
    "https://data.sec.gov/submissions/CIK0000999001.json": {"json": {"cik": "999001", "filings": {"recent": {
        "form": ["20-F"], "accessionNumber": ["0000999001-25-000001"], "primaryDocument": ["fpi-20f.htm"],
        "filingDate": ["2025-04-01"], "reportDate": ["2024-12-31"], "acceptanceDateTime": ["2025-04-01T10:00:00.000Z"],
    }}}},
    TEN_K_DOC: {"body": TEN_K_HTML},
    TEN_Q_DOC: {"body": TEN_Q_HTML},
    TEN_K_INDEX_PAGE: {"body": INDEX_PAGE_HTML},
}


def not_found(acc: str = MISSING_ACC, ticker: str = "AAPL") -> dict:
    """The ACCESSION_NOT_FOUND envelope view (spec §1)."""
    return {"ok": False, "error": {"code": "ACCESSION_NOT_FOUND",
                                   "message": f"Accession {acc} was not found in {ticker}'s recent SEC filings.",
                                   "requestedAccession": acc, "retryable": False}}


class _NoYahoo:
    def __init__(self, *_a, **_k):
        pass

    @property
    def info(self):
        raise RuntimeError("no yahoo in parity harness")


@contextlib.contextmanager
def _python_sec_state():
    """Empty SEC caches and no Yahoo, so Python resolves the CIK from the served ticker index, as the Worker does."""
    with contextlib.ExitStack() as stack:
        for cache in (edgar._FILING_CIK_CACHE, edgar._EDGAR_SUBS_CACHE, edgar._EDGAR_FACTS_CACHE, edgar._EDGAR_HTML_CACHE,
                      sec_tools._FILING_TEXT_CACHE, server._tool_cache._store):
            stack.enter_context(patch.dict(cache, {}, clear=True))
        saved = (edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT)
        edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = None, 0.0
        stack.callback(lambda: setattr(edgar, "_EDGAR_TICKERS", saved[0]))
        stack.callback(lambda: setattr(edgar, "_EDGAR_TICKERS_LOADED_AT", saved[1]))
        stack.enter_context(patch.object(edgar.yf, "Ticker", _NoYahoo))
        yield


def _py(fn, *args, **kwargs):
    def run():
        with _python_sec_state():
            return asyncio.run(fn(*args, **kwargs))
    return run


def _ts(fn: str, *args) -> dict:
    return {"module": "yahooFinance", "fn": fn, "args": list(args), "isolate": True}


def miss_view(v):
    """An error envelope as {ok, error}; any other payload as the fields that say which filing it answered from."""
    if not isinstance(v, dict):
        return v
    if v.get("ok") is False and isinstance(v.get("error"), dict):
        return {"ok": False, "error": v["error"]}
    if isinstance(v.get("result"), dict):  # extract_filing_fact wraps its text search
        v = v["result"]
    return {"ok": v.get("ok"), "code": v.get("code"), "status": v.get("status"), "accessionNumber": v.get("accessionNumber")}


def hit_view(v):
    """The filing a found accession answered from."""
    if not isinstance(v, dict):
        return v
    if v.get("ok") is False:
        return {"ok": False, "error": v.get("error")}
    return {"accessionNumber": v.get("accessionNumber"), "filingType": v.get("filingType"), "documentUrl": v.get("documentUrl")}


def outline_view(v):
    """The full outline payload (2.15.0: no masking); an error envelope as {ok, error}, without Python's meta."""
    if not isinstance(v, dict):
        return v
    if v.get("ok") is False and isinstance(v.get("error"), dict):
        return {"ok": False, "error": v["error"]}
    return v


def outline_keys(v):
    """The outline payload's key order, read before the harness canonicalises (it compares dicts without order)."""
    return list(v) if isinstance(v, dict) else v


def _case(name: str, py, ts: dict, select, **kw) -> Case:
    return Case(name=name, ids=["Q3a"], py=py, ts=ts, routes=ROUTES, select=select, **kw)


def _miss_case(name: str, py, ts: dict, expect_ts=None, select=miss_view) -> Case:
    """A miss or outline case both runtimes answer with the 2.13.0 rule (mode equal)."""
    return _case(name, py, ts, select, expect=expect_ts if expect_ts is not None else not_found())


SCENARIOS = [{"name": "base", "price": 100}]

# ── ACCESSION_NOT_FOUND for each listed action; the submissions hold a 10-K, which is never answered instead ─────
CASES += [
    _miss_case("q3a.miss.get_sec_filing_index",
             _py(server.get_sec_filing_index, "AAPL", "10-K", "latest", MISSING_ACC),
             _ts("getSecFilingIndex", "AAPL", "10-K", "latest", MISSING_ACC)),
    _miss_case("q3a.miss.search_sec_filing_text",
             _py(server.search_filing_text, "AAPL", ["revenue"], None, "10-K", MISSING_ACC),
             _ts("searchFilingText", "AAPL", ["revenue"], None, "10-K", MISSING_ACC)),
    _miss_case("q3a.miss.extract_sec_filing_fact_fact_name_path",
             _py(server.extract_filing_fact, "AAPL", "revenue", None, MISSING_ACC),
             _ts("extractFilingFact", "AAPL", "revenue", None, MISSING_ACC)),
    _miss_case("q3a.miss.extract_dilution_bridge",
             _py(server.extract_dilution_bridge, "AAPL", 100.0, None, "USD", "latest", MISSING_ACC, True),
             _ts("extractDilutionBridge", "AAPL", 100, None, "USD", "latest", MISSING_ACC, True)),
    _miss_case("q3a.miss.get_share_count_scenarios",
             _py(server.get_share_count_scenarios, "AAPL", SCENARIOS, "latest", MISSING_ACC, "USD"),
             _ts("getShareCountScenarios", "AAPL", SCENARIOS, "latest", MISSING_ACC, "USD")),
    _miss_case("q3a.miss.extract_capital_structure",
             _py(server.extract_capital_structure, "AAPL", "latest", MISSING_ACC, True),
             _ts("extractCapitalStructure", "AAPL", "latest", MISSING_ACC, True)),
    _miss_case("q3a.miss.extract_funding_capex_schedule",
             _py(server.extract_funding_capex_schedule, "AAPL", "latest", MISSING_ACC),
             _ts("extractFundingCapexSchedule", "AAPL", "latest", MISSING_ACC)),
    _miss_case("q3a.miss.extract_operating_driver_ledger",
             _py(server.extract_operating_driver_ledger, "AAPL", "latest", MISSING_ACC),
             _ts("extractOperatingDriverLedger", "AAPL", "latest", MISSING_ACC)),
    # A named periodic form with a miss: still no fallback to that form's latest filing.
    _miss_case("q3a.miss.extract_capital_structure_named_10k",
             _py(server.extract_capital_structure, "AAPL", "10-K", MISSING_ACC, True),
             _ts("extractCapitalStructure", "AAPL", "10-K", MISSING_ACC, True)),
    # No 20-F fallback when an accession was supplied: a 20-F filer asked for a 10-K with a missing accession.
    _miss_case("q3a.miss.no_20f_fallback_when_accession_given",
             _py(server.search_filing_text, "FPI", ["revenue"], None, "10-K", "0000999001-25-000999"),
             _ts("searchFilingText", "FPI", ["revenue"], None, "10-K", "0000999001-25-000999"),
             not_found("0000999001-25-000999", "FPI")),
    # The message names the ticker in upper case, whatever case the caller sent.
    _miss_case("q3a.miss.message_names_ticker_upper_case",
             _py(server.search_filing_text, "aapl", ["revenue"], None, "10-K", MISSING_ACC),
             _ts("searchFilingText", "aapl", ["revenue"], None, "10-K", MISSING_ACC)),
]

# ── A found accession is unchanged: the older 10-K is answered, not the newer 10-Q ─────────────────────────────────
HIT_10K = {"accessionNumber": TEN_K_ACC, "filingType": "10-K", "documentUrl": TEN_K_DOC}
CASES += [
    _case("q3a.hit.get_sec_filing_index_answers_the_requested_filing",
          _py(server.get_sec_filing_index, "AAPL", "10-K", "latest", TEN_K_ACC),
          _ts("getSecFilingIndex", "AAPL", "10-K", "latest", TEN_K_ACC), hit_view, expect=HIT_10K),
    _case("q3a.hit.search_sec_filing_text_answers_the_requested_filing",
          _py(server.search_filing_text, "AAPL", ["revenue"], None, "10-Q", TEN_K_ACC),
          _ts("searchFilingText", "AAPL", ["revenue"], None, "10-Q", TEN_K_ACC), hit_view, expect=HIT_10K),
    # The whole search payload, not only the filing it names, is the same in both runtimes.
    _case("q3a.hit.search_sec_filing_text_payload_agrees",
          _py(server.search_filing_text, "AAPL", ["revenue"], None, "10-K", TEN_K_ACC),
          _ts("searchFilingText", "AAPL", ["revenue"], None, "10-K", TEN_K_ACC), None),
    _case("q3a.hit.periodic_extractor_reads_the_requested_filing",
          _py(server.extract_capital_structure, "AAPL", "latest", TEN_K_ACC, False),
          _ts("extractCapitalStructure", "AAPL", "latest", TEN_K_ACC, False), lambda v: v.get("source"),
          expect={**HIT_10K, "filingDate": "2024-11-01", "periodEnd": None}),
]

# ── get_filing_outline with an accession and no document_url ────────────────────────────────────────────────────
# Both runtimes resolve the accession through the resolver and read that filing's primary document. The filing index
# page (<acc>-index.htm) is served here with different headings so a read of it would be told apart.
#
# 2.15.0 (R4B2_SPEC §2, R4-OUTLINE-SHAPE): one shape in both runtimes, keys in this order:
#     ticker, accessionNumber, documentUrl, outline, status, tableCount, warnings
# accessionNumber is the resolved filing's accession, the accession given alongside a document_url, or null when only a
# document_url was given; documentUrl is the document read; status is OK (headings found) / OUTLINE_NOT_PARSED (no
# headings, tables found) / EMPTY (neither); warnings carries TABLES_FOUND_OUTLINE_EMPTY with OUTLINE_NOT_PARSED. Python's
# sanitiser (_sanitize_sec_html) runs on the document before headings, the filingItemHeadings fallback and tableCount are
# read. Failures are the ok:false envelope with Python's codes. The Worker follows this today; Python after its port.
OUTLINE_TARGET_KEYS = ["ticker", "accessionNumber", "documentUrl", "outline", "status", "tableCount", "warnings"]
TEN_K_OUTLINE = [{"level": 2, "title": "Item 1. Business"}, {"level": 2, "title": "Item 1A. Risk Factors"}]
TEN_Q_OUTLINE = [{"level": 2, "title": "Item 2. Management's Discussion and Analysis"}]
TABLES_FOUND_OUTLINE_EMPTY = {"code": "TABLES_FOUND_OUTLINE_EMPTY",
                              "message": "Filing tables were detected but no section outline headings were parsed.",
                              "severity": "warning"}


def _outline_target(accession, document_url, outline, status="OK", table_count=0, warnings=None) -> dict:
    return {"ticker": "AAPL", "accessionNumber": accession, "documentUrl": document_url, "outline": outline,
            "status": status, "tableCount": table_count, "warnings": warnings or []}


DOC_URL_ONLY = f"{BASE}/000032019325000080/outline-cases.htm"
NO_HEADINGS_DOC = f"{BASE}/000032019325000081/bold-items.htm"
TABLES_ONLY_DOC = f"{BASE}/000032019325000082/tables-only.htm"
EMPTY_DOC = f"{BASE}/000032019325000083/empty.htm"
SCRIPT_DOC = f"{BASE}/000032019325000084/script-heading.htm"
MISSING_DOC = f"{BASE}/000032019325000085/missing.htm"
OUTLINE_ROUTES = {
    **ROUTES,
    DOC_URL_ONLY: {"body": TEN_K_HTML},
    # No <hN> headings: the item headings are bold paragraphs, read by the filingItemHeadings fallback.
    NO_HEADINGS_DOC: {"body": "<html><body><p><b>Item 1. Business</b></p><p>We sell devices and services worldwide.</p>"
                              "<p><b>Item 1A. Risk Factors</b></p><p>Supply risk.</p></body></html>"},
    TABLES_ONLY_DOC: {"body": "<html><body><p>Numbers</p><table><tr><td>1</td></tr></table>"
                              "<table><tr><td>2</td></tr></table></body></html>"},
    EMPTY_DOC: {"body": "<html><body><p>No headings and no tables.</p></body></html>"},
    # Script text inside a heading never reaches a title once the document is sanitised.
    SCRIPT_DOC: {"body": "<html><body><h2 onclick=\"x()\">Item 7. <script>var t = 1;</script>MD&amp;A</h2>"
                         "<style>h2 { color: red; }</style></body></html>"},
    MISSING_DOC: {"status": 404, "body": "not found"},
}


def _outline_pending(name: str, py, ts: dict, *, today_py, target, select=outline_view, routes=None) -> Case:
    """Both runtimes return the 2.15.0 outline shape (mode equal, expect=target); today_py is the pre-port Python value, kept for the record."""
    return Case(name=name, ids=["R4-OUTLINE-SHAPE"], py=py, ts=ts, routes=routes or OUTLINE_ROUTES, select=select,
                expect=target)


def _outline_equal(name: str, py, ts: dict, expect, select=outline_view) -> Case:
    return Case(name=name, ids=["R4-OUTLINE-SHAPE"], py=py, ts=ts, routes=OUTLINE_ROUTES, select=select, expect=expect)


CASES += [
    _outline_pending("q3a.outline.accession_only_hit_reads_the_primary_document",
                     _py(server.get_filing_outline, "AAPL", TEN_K_ACC, None),
                     _ts("getFilingOutline", "AAPL", TEN_K_ACC, None),
                     today_py={"ticker": "AAPL", "accessionNumber": TEN_K_ACC, "outline": TEN_K_OUTLINE},
                     target=_outline_target(TEN_K_ACC, TEN_K_DOC, TEN_K_OUTLINE)),
    _miss_case("q3a.outline.accession_only_miss",
               _py(server.get_filing_outline, "AAPL", MISSING_ACC, None),
               _ts("getFilingOutline", "AAPL", MISSING_ACC, None),
               select=outline_view),
    # document_url wins when given, as before; the accession given with it is echoed.
    _outline_pending("q3a.outline.document_url_wins_over_accession",
                     _py(server.get_filing_outline, "AAPL", MISSING_ACC, TEN_Q_DOC),
                     _ts("getFilingOutline", "AAPL", MISSING_ACC, TEN_Q_DOC),
                     today_py={"ticker": "AAPL", "accessionNumber": MISSING_ACC, "outline": TEN_Q_OUTLINE},
                     target=_outline_target(MISSING_ACC, TEN_Q_DOC, TEN_Q_OUTLINE)),
    _outline_pending("r4b2.outline.key_order",
                     lambda: outline_keys(json.loads(_py(server.get_filing_outline, "AAPL", TEN_K_ACC, None)())),
                     {"js": "return Object.keys(JSON.parse(await m.yahooFinance.getFilingOutline(...args)));",
                      "args": ["AAPL", TEN_K_ACC, None], "isolate": True},
                     today_py=["ticker", "accessionNumber", "outline"], target=OUTLINE_TARGET_KEYS, select=None),
    _outline_pending("r4b2.outline.document_url_only_has_null_accession",
                     _py(server.get_filing_outline, "AAPL", None, DOC_URL_ONLY),
                     _ts("getFilingOutline", "AAPL", None, DOC_URL_ONLY),
                     today_py={"ticker": "AAPL", "accessionNumber": None, "outline": TEN_K_OUTLINE},
                     target=_outline_target(None, DOC_URL_ONLY, TEN_K_OUTLINE)),
    _outline_pending("r4b2.outline.item_heading_fallback_without_hn_headings",
                     _py(server.get_filing_outline, "AAPL", None, NO_HEADINGS_DOC),
                     _ts("getFilingOutline", "AAPL", None, NO_HEADINGS_DOC),
                     today_py={"ticker": "AAPL", "accessionNumber": None, "outline": []},
                     target=_outline_target(None, NO_HEADINGS_DOC, TEN_K_OUTLINE)),
    _outline_pending("r4b2.outline.tables_without_headings_is_outline_not_parsed",
                     _py(server.get_filing_outline, "AAPL", None, TABLES_ONLY_DOC),
                     _ts("getFilingOutline", "AAPL", None, TABLES_ONLY_DOC),
                     today_py={"ticker": "AAPL", "accessionNumber": None, "outline": []},
                     target=_outline_target(None, TABLES_ONLY_DOC, [], "OUTLINE_NOT_PARSED", 2, [TABLES_FOUND_OUTLINE_EMPTY])),
    _outline_pending("r4b2.outline.neither_headings_nor_tables_is_empty",
                     _py(server.get_filing_outline, "AAPL", None, EMPTY_DOC),
                     _ts("getFilingOutline", "AAPL", None, EMPTY_DOC),
                     today_py={"ticker": "AAPL", "accessionNumber": None, "outline": []},
                     target=_outline_target(None, EMPTY_DOC, [], "EMPTY")),
    # Python's sanitiser, mirrored in the Worker: script/style blocks and on* attributes go before headings are read.
    _outline_pending("r4b2.outline.script_text_never_reaches_a_title",
                     _py(server.get_filing_outline, "AAPL", None, SCRIPT_DOC),
                     _ts("getFilingOutline", "AAPL", None, SCRIPT_DOC),
                     today_py={"ticker": "AAPL", "accessionNumber": None, "outline": [{"level": 2, "title": "Item 7. MD&amp;A"}]},
                     target=_outline_target(None, SCRIPT_DOC, [{"level": 2, "title": "Item 7. MD&amp;A"}])),
    # Errors: the standard ok:false envelope with Python's codes and messages; the Worker's bare {error, message} is gone.
    _outline_equal("r4b2.outline.invalid_sec_url_is_input_validation_error",
                   _py(server.get_filing_outline, "AAPL", None, "https://example.com/doc.htm"),
                   _ts("getFilingOutline", "AAPL", None, "https://example.com/doc.htm"),
                   {"ok": False, "error": {"code": "INPUT_VALIDATION_ERROR",
                                           "message": "Invalid SEC URL: must start with 'https://www.sec.gov/Archives/'."}}),
    _outline_equal("r4b2.outline.neither_accession_nor_document_url_is_input_validation_error",
                   _py(server.get_filing_outline, "AAPL", None, None),
                   _ts("getFilingOutline", "AAPL", None, None),
                   {"ok": False, "error": {"code": "INPUT_VALIDATION_ERROR",
                                           "message": "Either accession_number or document_url is required"}}),
    # A failed document read is PROVIDER_ERROR with the message "HTTP <status>" in both.
    Case(name="r4b2.outline.document_read_failure_is_provider_error", ids=["R4-OUTLINE-SHAPE"],
         py=_py(server.get_filing_outline, "AAPL", None, MISSING_DOC), ts=_ts("getFilingOutline", "AAPL", None, MISSING_DOC),
         routes=OUTLINE_ROUTES, select=outline_view,
         expect={"ok": False, "error": {"code": "PROVIDER_ERROR", "message": "HTTP 404"}}),
]

# The Worker's sanitiser is Python's _sanitize_sec_html, character for character.
from yfmcp.validation import _sanitize_sec_html  # noqa: E402

SANITIZE_INPUT = ('<p onclick="a()" class=x>One</p><SCRIPT type="t">bad()</script ><style>p{}</STYLE>'
                  "<a href='#' onmouseover='b()'>Two</a><div onload=c>Three</div><!-- kept -->")
CASES += [
    Case(name="r4b2.outline.worker_sanitiser_matches_python", ids=["R4-OUTLINE-SHAPE"],
         py=lambda: _sanitize_sec_html(SANITIZE_INPUT),
         ts={"module": "yahooFinance", "fn": "sanitizeSecHtml", "args": [SANITIZE_INPUT]},
         expect='<p class=x>One</p><a href=\'#\'>Two</a><div>Three</div><!-- kept -->'),
]

# ── Out of scope, unchanged (spec §1): extract_china_exposure is not an action that fails closed on a miss. ────────
# Its pinned-accession miss still reads FILING_NOT_FOUND_TRY_OTHER_TYPE, as before 2.13.0, rather than falling
# through to the latest filing's revenue and risk text. PLANNER: whether it should fail closed too is open.
CASES += [
    _case("q3a.out_of_scope.china_exposure_accession_miss_unchanged",
          _py(server.extract_china_exposure, "AAPL", "10-K", "latest", MISSING_ACC),
          _ts("extractChinaExposure", "AAPL", "10-K", "latest", MISSING_ACC), None),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# 2.15.0 (R4b-2; spec analysis/yahoo-finance-mcp/R4B2_SPEC.md §1): requested accessions honoured.
#
# §1a extract_geographic_revenue: a non-empty accession is passed into the filing read, so the figure, the evidence and
# the raw index all come from that filing; the 20-F retry stays pinned and is skipped when the pinned filing is not a
# 20-F; a miss is the ACCESSION_NOT_FOUND envelope above (not_found()), with no fallback to the latest filing.
# §1b query_sec_filing_index: geographic_revenue_share goes through §1a; china_exposure is unchanged (its miss reads
# FILING_NOT_FOUND_TRY_OTHER_TYPE, as 2.13.0 left it).
#
# GEOX files two 10-Ks and a 20-F. Its XBRL revenue facts carry no geographic segment, so the region is read from the
# filing's own table (the HTML fallback): the 2025 10-K says China 200 of 500, the 2024 10-K China 100 of 400, so the
# filing a figure came from is visible in the value. The 2023 20-F's table has no China row.
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════

GEOX_CIK = "0000777001"
GEOX_NEW_10K = "0000777001-25-000010"
GEOX_OLD_10K = "0000777001-24-000010"
GEOX_20F = "0000777001-23-000010"
GEOX_MISSING = "0000777001-99-999999"
GEOX_BASE = "https://www.sec.gov/Archives/edgar/data/777001"


def _geo_html(year: int, china: int | None, total: int, customer_pct: int | None = None) -> str:
    china_row = f"<tr><td>China</td><td>{china}</td></tr>" if china is not None else ""
    # 2.24.0 (F21): a year-specific customer sentence and a Risk Factors section, so the filing read shows in the value.
    f21_prose = (f"<h2>Item 1A. Risk Factors</h2><p>Our fiscal {year} results depend on suppliers located in China.</p>"
                 f"<h2>Item 7. Management's Discussion</h2><p>Customer A accounted for {customer_pct}% of net sales in fiscal {year}.</p>"
                 if customer_pct is not None else "")
    return (f"<html><body>{f21_prose}<h2>Item 8. Financial Statements</h2>"
            f"<p>Revenue by geographic area (in millions) for fiscal {year}:</p>"
            f"<table><tr><td>Geographic area</td><td>{year}</td></tr>"
            f"<tr><td>United States</td><td>{total - (china or 0)}</td></tr>{china_row}"
            f"<tr><td>Total revenue</td><td>{total}</td></tr></table></body></html>")


def _geox_fact(accn: str, form: str, fy: int, filed: str, val: int) -> dict:
    return {"start": f"{fy - 1}-10-01", "end": f"{fy}-09-30", "val": val, "accn": accn, "fy": fy, "fp": "FY",
            "form": form, "filed": filed}


GEOX_FACTS = {"units": {"USD": [
    _geox_fact(GEOX_NEW_10K, "10-K", 2025, "2025-11-01", 500_000_000),
    # 2.24.0 (F21): the FY2025 10-K's own prior-year comparative (FY2024, same accn), which a pinned read must not pick.
    _geox_fact(GEOX_NEW_10K, "10-K", 2024, "2025-11-01", 450_000_000),
    _geox_fact(GEOX_OLD_10K, "10-K", 2024, "2024-11-01", 400_000_000),
]}}
GEOX_ROUTES = {
    **ROUTES,
    "https://www.sec.gov/files/company_tickers.json": {"json": {
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 777001, "ticker": "GEOX", "title": "Geo Example Corp"},
    }},
    f"https://data.sec.gov/submissions/CIK{GEOX_CIK}.json": {"json": {"cik": "777001", "name": "Geo Example Corp",
        "filings": {"recent": {
            "form": ["10-K", "10-K", "20-F"],
            "accessionNumber": [GEOX_NEW_10K, GEOX_OLD_10K, GEOX_20F],
            "primaryDocument": ["geox-2025.htm", "geox-2024.htm", "geox-20f.htm"],
            "filingDate": ["2025-11-01", "2024-11-01", "2023-11-01"],
            "reportDate": ["2025-09-30", "2024-09-30", "2023-09-30"],
            "acceptanceDateTime": ["2025-11-01T16:30:00.000Z", "2024-11-01T16:30:00.000Z", "2023-11-01T16:30:00.000Z"],
        }}}},
    f"https://data.sec.gov/api/xbrl/companyconcept/CIK{GEOX_CIK}/us-gaap/RevenueFromContractWithCustomerExcludingAssessedTax.json":
        {"json": GEOX_FACTS},
    f"https://data.sec.gov/api/xbrl/companyconcept/CIK{GEOX_CIK}/*": {"status": 404, "body": "not found"},
    f"https://data.sec.gov/api/xbrl/companyfacts/CIK{GEOX_CIK}.json":
        {"json": {"facts": {"us-gaap": {"RevenueFromContractWithCustomerExcludingAssessedTax": GEOX_FACTS}}}},
    f"{GEOX_BASE}/000077700125000010/geox-2025.htm": {"body": _geo_html(2025, 200, 500, 12)},
    f"{GEOX_BASE}/000077700124000010/geox-2024.htm": {"body": _geo_html(2024, 100, 400, 15)},
    f"{GEOX_BASE}/000077700123000010/geox-20f.htm": {"body": _geo_html(2023, None, 300)},
}


def geo_view(v):
    """An error envelope as {ok, error}; a geographic read as the value and the filing its evidence names."""
    if not isinstance(v, dict):
        return v
    if v.get("ok") is False and isinstance(v.get("error"), dict):
        return {"ok": False, "error": v["error"]}
    ev = v.get("evidence") if isinstance(v.get("evidence"), dict) else {}
    return {"value": v.get("value"), "status": v.get("status"), "evidenceAccession": ev.get("accessionNumber"),
            "evidenceFilingType": ev.get("filingType"),
            "warnings": sorted(w.get("code") for w in v.get("warnings") or [] if isinstance(w, dict))}


def query_view(v):
    """An error envelope as {ok, error}; a query answer as its status, value and the filings its evidence names."""
    if not isinstance(v, dict):
        return v
    if v.get("ok") is False and isinstance(v.get("error"), dict):
        return {"ok": False, "error": v["error"]}
    answer = v.get("answer") if isinstance(v.get("answer"), dict) else {}
    return {"status": v.get("status"), "value": answer.get("value"),
            "evidenceAccessions": [e.get("accessionNumber") for e in v.get("evidence") or [] if isinstance(e, dict)]}


def _geo_case(name: str, py, ts: dict, select, **kw) -> Case:
    return Case(name=name, ids=["R4-ACC-GEO"], py=py, ts=ts, routes=GEOX_ROUTES, select=select, **kw)


def raw_view(v):
    """detailLevel=raw: the figure, and the filing named by the evidence and by the raw filing index."""
    if not isinstance(v, dict) or v.get("ok") is False:
        return geo_view(v)
    idx = (v.get("rawContext") or {}).get("filingIndex") or {}
    return {**geo_view(v), "period": v.get("period"), "rawIndexAccession": idx.get("accessionNumber")}


def _q(ticker, query_type, acc, params):
    return _py(server.query_sec_filing_index, ticker, query_type, "10-K", "latest", acc, params)


def _q_ts(ticker, query_type, acc, params):
    return _ts("querySecFilingIndex", ticker, "10-K", "latest", acc, query_type, params)


GEO = {"region": "China"}
NEW_CHINA = {"value": 200_000_000, "status": "FOUND", "evidenceAccession": GEOX_NEW_10K, "evidenceFilingType": "10-K", "warnings": []}
OLD_CHINA = {"value": 100_000_000, "status": "FOUND", "evidenceAccession": GEOX_OLD_10K, "evidenceFilingType": "10-K", "warnings": []}
GEOX_NOT_FOUND = not_found(GEOX_MISSING, "GEOX")


def _pending(name: str, py, ts: dict, select, *, today_py, target, ids=("R4-ACC-GEO",)) -> Case:
    """A 2.15.0 rule the Worker already follows and Python follows after its port: pinned to Python's value today and the
    Worker's new value (target). Ported in 2.15.0: mode equal with expect=target; today_py is the pre-port Python value."""
    return Case(name=name, ids=list(ids), py=py, ts=ts, routes=GEOX_ROUTES, select=select, expect=target)


CASES += [
    _pending("r4b2.geo.pinned_accession_is_the_filing_read",
             _py(server.extract_geographic_revenue, "GEOX", "China", "10-K", "latest", GEOX_OLD_10K),
             _ts("extractGeographicRevenue", "GEOX", "China", "10-K", "latest", GEOX_OLD_10K), geo_view,
             today_py=NEW_CHINA, target=OLD_CHINA),
    _geo_case("r4b2.geo.no_accession_reads_the_latest_filing_unchanged",
              _py(server.extract_geographic_revenue, "GEOX", "China", "10-K", "latest", None),
              _ts("extractGeographicRevenue", "GEOX", "China", "10-K", "latest", None), geo_view, expect=NEW_CHINA),
    _pending("r4b2.geo.accession_miss_fails_closed",
             _py(server.extract_geographic_revenue, "GEOX", "China", "10-K", "latest", GEOX_MISSING),
             _ts("extractGeographicRevenue", "GEOX", "China", "10-K", "latest", GEOX_MISSING), geo_view,
             today_py=NEW_CHINA, target=GEOX_NOT_FOUND),
    # Today Python's raw evidence and index name the requested 2024 filing while the figure is the 2025 filing's.
    _pending("r4b2.geo.raw_figure_evidence_and_index_name_one_filing",
             _py(server.extract_geographic_revenue, "GEOX", "China", "10-K", "latest", GEOX_OLD_10K, "raw"),
             _ts("extractGeographicRevenue", "GEOX", "China", "10-K", "latest", GEOX_OLD_10K, "raw"), raw_view,
             today_py={**NEW_CHINA, "evidenceAccession": GEOX_OLD_10K, "period": "FY2025", "rawIndexAccession": GEOX_OLD_10K},
             target={**OLD_CHINA, "period": "FY2024", "rawIndexAccession": GEOX_OLD_10K}),
    # The pinned filing is a 10-K, so no 20-F retry runs (no POSSIBLE_20F_FILER / AUTO_20F_FALLBACK), though GEOX files a 20-F.
    _pending("r4b2.geo.pinned_10k_skips_the_20f_retry",
             _py(server.extract_geographic_revenue, "GEOX", "Japan", "10-K", "latest", GEOX_OLD_10K),
             _ts("extractGeographicRevenue", "GEOX", "Japan", "10-K", "latest", GEOX_OLD_10K), geo_view,
             today_py={"value": None, "status": "NOT_DISCLOSED", "evidenceAccession": GEOX_NEW_10K, "evidenceFilingType": "10-K",
                       "warnings": ["POSSIBLE_20F_FILER"]},
             target={"value": None, "status": "NOT_DISCLOSED", "evidenceAccession": GEOX_OLD_10K, "evidenceFilingType": "10-K",
                     "warnings": []}),
    _geo_case("r4b2.geo.unpinned_not_disclosed_keeps_the_20f_retry",
              _py(server.extract_geographic_revenue, "GEOX", "Japan", "10-K", "latest", None),
              _ts("extractGeographicRevenue", "GEOX", "Japan", "10-K", "latest", None), geo_view,
              expect={"value": None, "status": "NOT_DISCLOSED", "evidenceAccession": GEOX_NEW_10K, "evidenceFilingType": "10-K",
                      "warnings": ["POSSIBLE_20F_FILER"]}),
    # 2.23.0 (F4): a pinned 20-F asked for as filing_type 10-K has no 10-K XBRL fact in that accession, so the pinned read falls
    # through to that accession's own HTML table (as the unpinned read does) instead of NO_FACT_FOR_ACCESSION. The 20-F has no
    # China row, so NOT_DISCLOSED; the existing pinned 20-F retry reads the same accession. evidenceFilingType is the 20-F's.
    _pending("r4b2.geo.pinned_20f_with_10k_filing_type_reads_that_accession",
             _py(server.extract_geographic_revenue, "GEOX", "China", "10-K", "latest", GEOX_20F),
             _ts("extractGeographicRevenue", "GEOX", "China", "10-K", "latest", GEOX_20F), geo_view,
             today_py=NEW_CHINA,
             target={"value": None, "status": "NOT_DISCLOSED", "evidenceAccession": GEOX_20F, "evidenceFilingType": "20-F",
                     "warnings": ["POSSIBLE_20F_FILER"]}),
    _geo_case("r5x.geo.pinned_20f_table_read",
              _py(server.extract_geographic_revenue, "GEOX", "United States", "10-K", "latest", GEOX_20F),
              _ts("extractGeographicRevenue", "GEOX", "United States", "10-K", "latest", GEOX_20F), geo_view,
              expect={"value": 300_000_000, "status": "FOUND", "evidenceAccession": GEOX_20F, "evidenceFilingType": "20-F", "warnings": []}),
    Case(name="r5x.query.revenue_exposure_pinned", ids=["R5-EXTRACTORS"], py=_q("GEOX", "revenue_exposure", GEOX_OLD_10K, {"exposure_query": "China"}),
         ts=_q_ts("GEOX", "revenue_exposure", GEOX_OLD_10K, {"exposure_query": "China"}), routes=GEOX_ROUTES, select=query_view,
         expect={"status": "ANSWERED", "value": 100_000_000, "evidenceAccessions": [GEOX_OLD_10K]}),
    # §1b: one hit and one miss per query type with an accession.
    _pending("r4b2.query.geographic_hit", _q("GEOX", "geographic_revenue_share", GEOX_OLD_10K, GEO),
             _q_ts("GEOX", "geographic_revenue_share", GEOX_OLD_10K, GEO), query_view, ids=("R4-ACC-QUERY",),
             today_py={"status": "ANSWERED", "value": 200_000_000, "evidenceAccessions": [GEOX_NEW_10K]},
             target={"status": "ANSWERED", "value": 100_000_000, "evidenceAccessions": [GEOX_OLD_10K]}),
    _pending("r4b2.query.geographic_miss", _q("GEOX", "geographic_revenue_share", GEOX_MISSING, GEO),
             _q_ts("GEOX", "geographic_revenue_share", GEOX_MISSING, GEO), query_view, ids=("R4-ACC-QUERY",),
             today_py={"status": "ANSWERED", "value": 200_000_000, "evidenceAccessions": [GEOX_NEW_10K]},
             target=GEOX_NOT_FOUND),
    # china_exposure was unchanged by 2.15.0. PLANNER (finding, closed): its revenue read was not pinned, so a hit named the
    # latest 10-K; 2.23.0 (F3) pins the revenue read, so the evidence now names the pinned filing.
    Case(name="r4b2.query.china_hit_reads_the_pinned_filing", ids=["R4-ACC-QUERY"], py=_q("GEOX", "china_exposure", GEOX_OLD_10K, {}),
         ts=_q_ts("GEOX", "china_exposure", GEOX_OLD_10K, {}), routes=GEOX_ROUTES, select=query_view,
         # 2.24.0 (F21): the risk-factor read is pinned too. GEOX's 10-Ks now carry a China risk sentence, so the risk block adds a
         # second evidence row: its outer row has no filing fields (shapeEvidence of the mention), which is why it reads None; the
         # accession the risk block names is checked in f21.query.china_risk_block_reads_the_pinned_filing.
         expect={"status": "ANSWERED", "value": None, "evidenceAccessions": [GEOX_OLD_10K, None]}),
    Case(name="r4b2.query.china_miss_unchanged", ids=["R4-ACC-QUERY"], py=_q("GEOX", "china_exposure", GEOX_MISSING, {}),
         ts=_q_ts("GEOX", "china_exposure", GEOX_MISSING, {}), routes=GEOX_ROUTES, select=query_view,
         expect={"status": "FILING_NOT_FOUND_TRY_OTHER_TYPE", "value": None, "evidenceAccessions": []}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# 2.24.0 (F21; spec analysis/yahoo-finance-mcp/F21_SPEC.md §2): every pinned query_sec_filing_index read uses the pinned
# filing. risk_factor_mentions, customer_concentration, total_revenue and segment_revenue pass the accession into their
# read, and an accession that is not among the company's filings fails closed with ACCESSION_NOT_FOUND (GEOX_NOT_FOUND),
# checked after the query's own params. The GEOX fixture carries a FY2024 comparative inside the 2025 10-K (450M), a
# customer sentence (2025: 12%, 2024: 15%) and a China risk-factor sentence in each 10-K.
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════

def f21_view(v):
    """An error envelope as {ok, error}; a query answer as status, value, period, customer percentages, segment count, the
    filings its evidence names and its warning codes."""
    if not isinstance(v, dict):
        return v
    if v.get("ok") is False and isinstance(v.get("error"), dict):
        return {"ok": False, "error": v["error"]}
    answer = v.get("answer") if isinstance(v.get("answer"), dict) else {}
    return {"status": v.get("status"), "value": answer.get("value"), "period": answer.get("period"),
            "customerPcts": [c.get("valuePct") for c in answer.get("customers") or [] if isinstance(c, dict)],
            "riskTerms": [m.get("term") for m in answer.get("matches") or [] if isinstance(m, dict)],
            "segmentCount": len(answer["segments"]) if isinstance(answer.get("segments"), list) else None,
            "evidenceAccessions": [e.get("accessionNumber") for e in v.get("evidence") or [] if isinstance(e, dict)],
            "warnings": sorted(w.get("code") for w in v.get("warnings") or [] if isinstance(w, dict))}


def f21_china_risk_view(v):
    """The accessions the china_exposure riskFactorExposure block names in its mentions' own evidence."""
    if not isinstance(v, dict) or v.get("ok") is False:
        return f21_view(v)
    risk = (v.get("answer") or {}).get("riskFactorExposure") or {}
    return {"status": risk.get("status"),
            "mentionAccessions": [(m.get("evidence") or {}).get("accessionNumber") for m in risk.get("evidence") or [] if isinstance(m, dict)]}


def _f21_case(name: str, query_type: str, acc, params: dict, expect: dict) -> Case:
    return Case(name=f"f21.query.{name}", ids=["F21"], py=_q("GEOX", query_type, acc, params),
                ts=_q_ts("GEOX", query_type, acc, params), routes=GEOX_ROUTES, select=f21_view, expect=expect)


F21_RISK = {"terms": ["China"]}

def _f21_expect(acc: str | None, *, status: str = "ANSWERED", value=None, period=None, pcts=(), terms=(), segments=None, warnings=()) -> dict:
    return {"status": status, "value": value, "period": period, "customerPcts": list(pcts), "riskTerms": list(terms),
            "segmentCount": segments, "evidenceAccessions": [acc] if acc else [], "warnings": list(warnings)}


CASES += [
    _f21_case("total_revenue_pinned", "total_revenue", GEOX_OLD_10K, {},
                _f21_expect(GEOX_OLD_10K, value=400_000_000, period="FY2024")),
    _f21_case("total_revenue_pinned_newest_period_of_that_filing", "total_revenue", GEOX_NEW_10K, {},
                _f21_expect(GEOX_NEW_10K, value=500_000_000, period="FY2025")),
    _f21_case("total_revenue_no_accession_unchanged", "total_revenue", None, {},
                _f21_expect(GEOX_NEW_10K, value=500_000_000, period="FY2025")),
    _f21_case("total_revenue_accession_miss_fails_closed", "total_revenue", GEOX_MISSING, {}, GEOX_NOT_FOUND),
    _f21_case("customer_concentration_pinned", "customer_concentration", GEOX_OLD_10K, {}, _f21_expect(GEOX_OLD_10K, pcts=[15])),
    _f21_case("customer_concentration_no_accession_unchanged", "customer_concentration", None, {}, _f21_expect(GEOX_NEW_10K, pcts=[12])),
    _f21_case("customer_concentration_accession_miss_fails_closed", "customer_concentration", GEOX_MISSING, {}, GEOX_NOT_FOUND),
    _f21_case("risk_factor_mentions_pinned", "risk_factor_mentions", GEOX_OLD_10K, F21_RISK,
                _f21_expect(GEOX_OLD_10K, terms=["China"])),
    _f21_case("risk_factor_mentions_no_accession_unchanged", "risk_factor_mentions", None, F21_RISK,
                _f21_expect(GEOX_NEW_10K, terms=["China"])),
    _f21_case("risk_factor_mentions_accession_miss_fails_closed", "risk_factor_mentions", GEOX_MISSING, F21_RISK, GEOX_NOT_FOUND),
    _f21_case("risk_factor_mentions_missing_terms_beats_accession_miss", "risk_factor_mentions", GEOX_MISSING, {},
                _f21_expect(None, status="INPUT_VALIDATION_ERROR", warnings=["INPUT_VALIDATION_ERROR"])),
    _f21_case("segment_revenue_pinned", "segment_revenue", GEOX_OLD_10K, {},
                _f21_expect(None, status="NOT_DISCLOSED", segments=0)),  # a pinned 10-K: no POSSIBLE_20F_FILER
    # Both runtimes' queries add the extractor's warnings to the answer (POSSIBLE_20F_FILER for an unpinned 10-K that
    # finds no segments, as GEOX files a 20-F); Python dropped them until 2.26.4 (F23, was pinned as
    # F21-SEGMENT-QUERY-WARNINGS). The pinned case above raises no such warning.
    Case(name="f21.query.segment_revenue_no_accession_unchanged", ids=["F21", "F23"], py=_q("GEOX", "segment_revenue", None, {}),
         ts=_q_ts("GEOX", "segment_revenue", None, {}), routes=GEOX_ROUTES, select=f21_view,
         expect=_f21_expect(None, status="NOT_DISCLOSED", segments=0, warnings=["POSSIBLE_20F_FILER"])),
    Case(name="f21.query.china_risk_block_reads_the_pinned_filing", ids=["F21"], py=_q("GEOX", "china_exposure", GEOX_OLD_10K, {}),
         ts=_q_ts("GEOX", "china_exposure", GEOX_OLD_10K, {}), routes=GEOX_ROUTES, select=f21_china_risk_view,
         expect={"status": "FOUND", "mentionAccessions": [GEOX_OLD_10K]}),
    Case(name="f21.query.china_risk_block_no_accession_unchanged", ids=["F21"], py=_q("GEOX", "china_exposure", None, {}),
         ts=_q_ts("GEOX", "china_exposure", None, {}), routes=GEOX_ROUTES, select=f21_china_risk_view,
         expect={"status": "FOUND", "mentionAccessions": [GEOX_NEW_10K]}),
    _f21_case("segment_revenue_accession_miss_fails_closed", "segment_revenue", GEOX_MISSING, {}, GEOX_NOT_FOUND),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# 2.15.0 §1c (R4-ACC-EXHIBITS): list_sec_filing_exhibits / get_sec_filing_exhibit_content. When SEC answers the filing
# index for (CIK, accession) with HTTP 404, the result is ACCESSION_NOT_FOUND ("...in <TICKER>'s SEC filings.", no
# "recent": the recent-filings resolver is not used, so an older real accession keeps working). Other index failures
# (5xx, timeouts) keep today's empty success.
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════

from yfmcp.tools import earnings as earnings_tools  # noqa: E402

OLD_ACC = "0000320193-10-000001"  # a real-looking accession older than the recent-filings list
OLD_INDEX = f"{BASE}/000032019310000001/{OLD_ACC}-index.htm"
SERVER_ERROR_ACC = "0000320193-11-000002"
OLD_INDEX_HTML = ("<html><body><table>"
                  "<tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>"
                  f'<tr><td>1</td><td>Annual report</td><td><a href="/Archives/edgar/data/320193/000032019310000001/aapl-2010.htm">aapl-2010.htm</a></td><td>10-K</td><td>100</td></tr>'
                  f'<tr><td>2</td><td>Press release</td><td><a href="/Archives/edgar/data/320193/000032019310000001/ex99.htm">ex99.htm</a></td><td>EX-99.1</td><td>10</td></tr>'
                  "</table></body></html>")
EXHIBIT_ROUTES = {
    **ROUTES,
    f"{BASE}/{MISSING_ACC.replace('-', '')}/*": {"status": 404, "body": "not found"},
    OLD_INDEX: {"body": OLD_INDEX_HTML},
    f"{BASE}/000032019311000002/*": {"status": 503, "body": "unavailable"},
}


def exhibits_view(v):
    """An error as {ok, error}; a listing as its accession and exhibit documents (Python's success data is enveloped)."""
    if not isinstance(v, dict):
        return v
    if v.get("ok") is False and isinstance(v.get("error"), dict):
        err = v["error"]
        return {"ok": False, "error": {k: err.get(k) for k in ("code", "message", "requestedAccession", "retryable") if k in err}}
    data = v.get("data") if isinstance(v.get("data"), dict) else v
    return {"accessionNumber": data.get("accessionNumber"), "documents": [e.get("document") for e in data.get("exhibits") or []]}


def exhibit_not_found(acc: str = MISSING_ACC, ticker: str = "AAPL") -> dict:
    return {"ok": False, "error": {"code": "ACCESSION_NOT_FOUND",
                                   "message": f"Accession {acc} was not found in {ticker}'s SEC filings.",
                                   "requestedAccession": acc, "retryable": False}}


def _exhibit_case(name: str, py, ts: dict, **kw) -> Case:
    return Case(name=name, ids=["R4-ACC-EXHIBITS"], py=py, ts=ts, routes=EXHIBIT_ROUTES, select=exhibits_view, **kw)


CASES += [
    _exhibit_case("r4b2.exhibits.list_index_404_is_accession_not_found",
                  _py(earnings_tools.list_sec_filing_exhibits, "AAPL", MISSING_ACC),
                  _ts("listSecFilingExhibits", "AAPL", MISSING_ACC), expect=exhibit_not_found()),
    _exhibit_case("r4b2.exhibits.content_index_404_is_accession_not_found",
                  _py(earnings_tools.get_sec_filing_exhibit_content, "AAPL", MISSING_ACC, "ex99.htm"),
                  _ts("getSecFilingExhibitContent", "AAPL", MISSING_ACC, "ex99.htm"), expect=exhibit_not_found()),
    # An older accession outside the recent-filings list still lists its exhibits (no resolver check).
    _exhibit_case("r4b2.exhibits.older_accession_outside_recent_filings_still_lists",
                  _py(earnings_tools.list_sec_filing_exhibits, "AAPL", OLD_ACC),
                  _ts("listSecFilingExhibits", "AAPL", OLD_ACC),
                  expect={"accessionNumber": OLD_ACC, "documents": ["aapl-2010.htm", "ex99.htm"]}),
    # A 5xx index read is not a miss: today's empty success, unchanged.
    _exhibit_case("r4b2.exhibits.index_server_error_keeps_empty_success",
                  _py(earnings_tools.list_sec_filing_exhibits, "AAPL", SERVER_ERROR_ACC),
                  _ts("listSecFilingExhibits", "AAPL", SERVER_ERROR_ACC),
                  expect={"accessionNumber": SERVER_ERROR_ACC, "documents": []}),
]
