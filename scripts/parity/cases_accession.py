"""Parity cases: exact accession, fail-closed (2.13.0, R4 Q3a; spec analysis/yahoo-finance-mcp/R4_Q3A_Q4_SPEC.md §1, §3).

An explicit accession that is not among the company's recent SEC filings (submissions `filings.recent`) fails closed:

    {"ok": false, "error": {"code": "ACCESSION_NOT_FOUND",
                            "message": "Accession <acc> was not found in <TICKER>'s recent SEC filings.",
                            "requestedAccession": "<acc>", "retryable": false}}

with no fallback to the latest filing or to another form (the 20-F fallback). A found accession is unchanged.

Worker side: the action functions in worker/src/yahoo-finance.ts (one shared miss branch in resolveSecFiling).
Python side: the matching server.py functions (one shared miss branch in _resolve_sec_filing).

TODO(python-mirror): every case here with mode "pinned" and decision "Q3a" records today's Python behaviour (the miss
reads as NO_FILING_DATA / FILING_NOT_FOUND_TRY_OTHER_TYPE, the outline reads the filing index page). When Python
ports the rule, flip each to mode "equal" with `expect` = its `expect_ts`.

Each case serves the same canned SEC responses to both runtimes: SEC's ticker index (the CIK; Yahoo is unreachable in
both) and one submissions list holding a 10-Q, an 8-K and an older 10-K.
"""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import patch

from scripts.parity.harness import Case

import server  # noqa: E402
from yfmcp.clients import edgar  # noqa: E402

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
                      server._FILING_TEXT_CACHE, server._tool_cache._store):
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
    if not isinstance(v, dict):
        return v
    if v.get("ok") is False and isinstance(v.get("error"), dict):
        return {"ok": False, "error": v["error"]}
    if v.get("error"):
        return {"error": True, "message": v.get("message")}
    return {"outline": [h["title"] for h in v.get("outline") or []]}


def _case(name: str, py, ts: dict, select, **kw) -> Case:
    return Case(name=name, ids=["Q3a"], py=py, ts=ts, routes=ROUTES, select=select, **kw)


def _pending(name: str, py, ts: dict, expect_py, expect_ts=None, select=miss_view) -> Case:
    """Worker has the 2.13.0 rule; Python's value today is pinned until it ports it. TODO(python-mirror)."""
    return _case(name, py, ts, select, mode="pinned", decision="Q3a", expect_py=expect_py,
                 expect_ts=expect_ts if expect_ts is not None else not_found())


# Python's values today for a miss (TODO(python-mirror): each becomes not_found()).
PY_INDEX_MISS = {"ok": False, "error": {"code": "NO_FILING_DATA", "message": "No 10-K filing found for 'AAPL'"}}
PY_SEARCH_MISS = {"ok": None, "code": "FILING_NOT_FOUND_TRY_OTHER_TYPE", "status": "FILING_NOT_FOUND_TRY_OTHER_TYPE",
                  "accessionNumber": MISSING_ACC}
PY_PERIODIC_MISS = {"ok": None, "code": "FILING_NOT_FOUND_TRY_OTHER_TYPE", "status": "FILING_NOT_FOUND_TRY_OTHER_TYPE",
                    "accessionNumber": None}

SCENARIOS = [{"name": "base", "price": 100}]

# ── ACCESSION_NOT_FOUND for each listed action; the submissions hold a 10-K, which is never answered instead ─────
CASES += [
    _pending("q3a.miss.get_sec_filing_index",
             _py(server.get_sec_filing_index, "AAPL", "10-K", "latest", MISSING_ACC),
             _ts("getSecFilingIndex", "AAPL", "10-K", "latest", MISSING_ACC), PY_INDEX_MISS),
    _pending("q3a.miss.search_sec_filing_text",
             _py(server.search_filing_text, "AAPL", ["revenue"], None, "10-K", MISSING_ACC),
             _ts("searchFilingText", "AAPL", ["revenue"], None, "10-K", MISSING_ACC), PY_SEARCH_MISS),
    _pending("q3a.miss.extract_sec_filing_fact_fact_name_path",
             _py(server.extract_filing_fact, "AAPL", "revenue", None, MISSING_ACC),
             _ts("extractFilingFact", "AAPL", "revenue", None, MISSING_ACC), PY_SEARCH_MISS),
    _pending("q3a.miss.extract_dilution_bridge",
             _py(server.extract_dilution_bridge, "AAPL", 100.0, None, "USD", "latest", MISSING_ACC, True),
             _ts("extractDilutionBridge", "AAPL", 100, None, "USD", "latest", MISSING_ACC, True), PY_PERIODIC_MISS),
    _pending("q3a.miss.get_share_count_scenarios",
             _py(server.get_share_count_scenarios, "AAPL", SCENARIOS, "latest", MISSING_ACC, "USD"),
             _ts("getShareCountScenarios", "AAPL", SCENARIOS, "latest", MISSING_ACC, "USD"), PY_PERIODIC_MISS),
    _pending("q3a.miss.extract_capital_structure",
             _py(server.extract_capital_structure, "AAPL", "latest", MISSING_ACC, True),
             _ts("extractCapitalStructure", "AAPL", "latest", MISSING_ACC, True), PY_PERIODIC_MISS),
    _pending("q3a.miss.extract_funding_capex_schedule",
             _py(server.extract_funding_capex_schedule, "AAPL", "latest", MISSING_ACC),
             _ts("extractFundingCapexSchedule", "AAPL", "latest", MISSING_ACC), PY_PERIODIC_MISS),
    _pending("q3a.miss.extract_operating_driver_ledger",
             _py(server.extract_operating_driver_ledger, "AAPL", "latest", MISSING_ACC),
             _ts("extractOperatingDriverLedger", "AAPL", "latest", MISSING_ACC), PY_PERIODIC_MISS),
    # A named periodic form with a miss: still no fallback to that form's latest filing.
    _pending("q3a.miss.extract_capital_structure_named_10k",
             _py(server.extract_capital_structure, "AAPL", "10-K", MISSING_ACC, True),
             _ts("extractCapitalStructure", "AAPL", "10-K", MISSING_ACC, True), PY_PERIODIC_MISS),
    # No 20-F fallback when an accession was supplied: a 20-F filer asked for a 10-K with a missing accession.
    _pending("q3a.miss.no_20f_fallback_when_accession_given",
             _py(server.search_filing_text, "FPI", ["revenue"], None, "10-K", "0000999001-25-000999"),
             _ts("searchFilingText", "FPI", ["revenue"], None, "10-K", "0000999001-25-000999"),
             {**PY_SEARCH_MISS, "accessionNumber": "0000999001-25-000999"},
             not_found("0000999001-25-000999", "FPI")),
    # The message names the ticker in upper case, whatever case the caller sent.
    _pending("q3a.miss.message_names_ticker_upper_case",
             _py(server.search_filing_text, "aapl", ["revenue"], None, "10-K", MISSING_ACC),
             _ts("searchFilingText", "aapl", ["revenue"], None, "10-K", MISSING_ACC), PY_SEARCH_MISS),
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
# The Worker resolves the accession through resolveSecFiling and reads that filing's primary document. Python today
# reads the filing index page (<acc>-index.htm), served here with different headings so the two are told apart.
CASES += [
    _pending("q3a.outline.accession_only_hit_reads_the_primary_document",
             _py(server.get_filing_outline, "AAPL", TEN_K_ACC, None),
             _ts("getFilingOutline", "AAPL", TEN_K_ACC, None),
             {"outline": ["Filing Index Page"]},
             {"outline": ["Item 1. Business", "Item 1A. Risk Factors"]}, select=outline_view),
    # Python today builds the index-page URL for any accession and fails reading it (unstubbed here).
    _pending("q3a.outline.accession_only_miss",
             _py(server.get_filing_outline, "AAPL", MISSING_ACC, None),
             _ts("getFilingOutline", "AAPL", MISSING_ACC, None),
             {"ok": False, "error": {"code": "PROVIDER_ERROR",
                                     "message": f"UNSTUBBED_FETCH {BASE}/000032019399999999/{MISSING_ACC}-index.htm"}},
             select=outline_view),
    # document_url wins when given, as before.
    _case("q3a.outline.document_url_wins_over_accession",
          _py(server.get_filing_outline, "AAPL", MISSING_ACC, TEN_Q_DOC),
          _ts("getFilingOutline", "AAPL", MISSING_ACC, TEN_Q_DOC), outline_view,
          expect={"outline": ["Item 2. Management's Discussion and Analysis"]}),
]

# ── Out of scope, unchanged (spec §1): extract_china_exposure is not an action that fails closed on a miss. ────────
# Its pinned-accession miss still reads FILING_NOT_FOUND_TRY_OTHER_TYPE, as before 2.13.0, rather than falling
# through to the latest filing's revenue and risk text. PLANNER: whether it should fail closed too is open.
CASES += [
    _case("q3a.out_of_scope.china_exposure_accession_miss_unchanged",
          _py(server.extract_china_exposure, "AAPL", "10-K", "latest", MISSING_ACC),
          _ts("extractChinaExposure", "AAPL", "10-K", "latest", MISSING_ACC), None),
]
