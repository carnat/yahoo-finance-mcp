"""Parity cases: a failed SEC read does not look like a missing record (3.8.0, swallowed-error audit F1-F6, S01-S03, S05, S06, S11-S14).

A 404 from SEC is absence and keeps today's output (TICKER_NOT_FOUND, NO_SEC_REGISTRANT, no exhibit, ...). Any other
outcome of a read (429, 5xx, a timeout) is a failed read and must not produce the absent-record output:

- ticker index or submissions unreadable when resolving a ticker: PROVIDER_ERROR, retryable (envelope tools:
  error {code PROVIDER_ERROR, message, retryable true}; payload tools: status PROVIDER_ERROR, code SEC_LOOKUP_UNAVAILABLE);
- companyfacts unreadable where a tool goes on without facts: a SEC_READ_FAILED warning (valuation: the code);
- an 8-K filing index unreadable: the release is NOT_READ, never the 8-K cover page labelled READ;
- a filing index unreadable: exhibitsCount null, a FILING_TEXT_NOT_AVAILABLE warning, a PROVIDER_ERROR exhibit list.

Each case serves the same canned SEC responses to both runtimes and compares the failure stubs (429, 500, timeout)
with the 404 stub. Not covered here (no stub can fail only the second read of a cached document): the return_tables
re-read warning (search_filing_text) and the transcript exhibit-index attempt.
"""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import patch

from scripts.parity.cases_accession import BASE, CIK, ROUTES, TEN_K_ACC, TEN_K_DOC, TEN_K_INDEX_PAGE, _NoYahoo
from scripts.parity.harness import _UNSET, Case

import server  # noqa: E402
from yfmcp.clients import edgar  # noqa: E402
from yfmcp.tools import sec as sec_tools  # noqa: E402

CASES: list[Case] = []
IDS = ["SEC-READS-380"]

TICKER_INDEX = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = f"https://data.sec.gov/submissions/CIK{CIK}.json"
FACTS_URL = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json"
EIGHT_K_ACC = "0000320193-25-000070"
EIGHT_K_BASE = f"{BASE}/000032019325000070"
EIGHT_K_DOC = f"{EIGHT_K_BASE}/aapl-8k.htm"
EX991_DOC = f"{EIGHT_K_BASE}/ex991.htm"
EIGHT_K_INDEX = f"{EIGHT_K_BASE}/{EIGHT_K_ACC}-index.htm"
NO_CIK_ACC = "0000000000-25-000001"  # an accession prefix that names no CIK, so the ticker's own lookup decides
NO_CIK_INDEX = f"{BASE}/000000000025000001/{NO_CIK_ACC}-index.htm"
NO_CIK_INDEX_AAPL = f"https://www.sec.gov/Archives/edgar/data/320193/000000000025000001/{NO_CIK_ACC}-index.htm"

SUBMISSIONS = {"cik": "320193", "name": "Apple Inc.", "filings": {"recent": {
    "form": ["8-K", "10-K"],
    "accessionNumber": [EIGHT_K_ACC, TEN_K_ACC],
    "primaryDocument": ["aapl-8k.htm", "aapl-20240928.htm"],
    "filingDate": ["2025-07-31", "2024-11-01"],
    "reportDate": ["2025-07-31", "2024-09-28"],
    "acceptanceDateTime": ["2025-07-31T16:30:00.000Z", "2024-11-01T16:30:00.000Z"],
    "items": ["2.02,9.01", ""],
}}}
FACTS = {"cik": 320193, "entityName": "Apple Inc.", "facts": {"us-gaap": {}}}
EXHIBIT_INDEX_HTML = (
    "<html><body><table>"
    "<tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>"
    f'<tr><td>1</td><td>8-K</td><td><a href="/Archives/edgar/data/320193/000032019325000070/aapl-8k.htm">aapl-8k.htm</a></td><td>8-K</td><td>10</td></tr>'
    f'<tr><td>2</td><td>Press release</td><td><a href="/Archives/edgar/data/320193/000032019325000070/ex991.htm">ex991.htm</a></td><td>EX-99.1</td><td>20</td></tr>'
    "</table></body></html>"
)
EX991_HTML = ("<html><body><p>Apple Inc. reports results for the fourth quarter of fiscal 2025.</p>"
              "<p>The company expects revenue between $10 billion and $11 billion for the first quarter of fiscal 2026.</p></body></html>")
EIGHT_K_HTML = "<html><body><p>Form 8-K cover page. Item 2.02 Results of Operations. Exhibit 99.1 is furnished.</p></body></html>"
BASE_ROUTES = {
    **ROUTES,
    SUBMISSIONS_URL: {"json": SUBMISSIONS},
    FACTS_URL: {"status": 404},
    EIGHT_K_DOC: {"body": EIGHT_K_HTML},
    EX991_DOC: {"body": EX991_HTML},
    EIGHT_K_INDEX: {"body": EXHIBIT_INDEX_HTML},
}

# The ways a read fails, and the one way it is absent.
FAILURES = {"429": {"status": 429}, "500": {"status": 500}, "timeout": {"error": "timeout"}}
ABSENT = {"status": 404}


def routes_with(**overrides: dict) -> dict:
    """BASE_ROUTES with the named routes replaced; keys are the constants above by name."""
    names = {"ticker_index": TICKER_INDEX, "submissions": SUBMISSIONS_URL, "facts": FACTS_URL, "eight_k_index": EIGHT_K_INDEX,
             "eight_k_doc": EIGHT_K_DOC, "ex991": EX991_DOC, "ten_k_index": TEN_K_INDEX_PAGE, "no_cik_index": NO_CIK_INDEX_AAPL}
    return {**BASE_ROUTES, **{names[k]: v for k, v in overrides.items()}}


@contextlib.contextmanager
def _python_state():
    """Empty SEC caches and read-status state, and no Yahoo, so Python resolves the CIK from the served ticker index."""
    with contextlib.ExitStack() as stack:
        for cache in (edgar._FILING_CIK_CACHE, edgar._EDGAR_SUBS_CACHE, edgar._EDGAR_FACTS_CACHE, edgar._EDGAR_HTML_CACHE,
                      edgar._EDGAR_FACTS_READ_FAILED, edgar._EDGAR_SUBS_READ_FAILED, edgar._EDGAR_HTML_READ_FAILED,
                      edgar._EDGAR_LOOKUP_FAILURE, sec_tools._FILING_TEXT_CACHE, server._tool_cache._store):
            stack.enter_context(patch.dict(cache, {}, clear=True))
        saved = (edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT)
        edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = None, 0.0
        stack.callback(lambda: setattr(edgar, "_EDGAR_TICKERS", saved[0]))
        stack.callback(lambda: setattr(edgar, "_EDGAR_TICKERS_LOADED_AT", saved[1]))
        stack.enter_context(patch.object(edgar.yf, "Ticker", _NoYahoo))
        yield


def _py(fn, *args, **kwargs):
    def run():
        with _python_state():
            return asyncio.run(fn(*args, **kwargs))
    return run


def _ts(module: str, fn: str, *args) -> dict:
    return {"module": module, "fn": fn, "args": list(args), "isolate": True}


def _error_view(v):
    """An error envelope as {ok, error}; any other payload as the failure fields it carries."""
    if not isinstance(v, dict):
        return v
    if v.get("ok") is False and isinstance(v.get("error"), dict):
        return {"ok": False, "error": {k: v["error"].get(k) for k in ("code", "message", "retryable")}}
    return {k: v.get(k) for k in ("status", "code", "retryable", "message", "confidence") if k in v}


def _warnings(*codes: str):
    def view(v):
        return [w for w in (v.get("warnings") or []) if w.get("code") in codes] if isinstance(v, dict) else v
    return view


def _case(name: str, py, ts: dict, routes: dict, select, expect=_UNSET) -> Case:
    return Case(name=f"sec_reads_380.{name}", ids=IDS, py=py, ts=ts, routes=routes, select=select, expect=expect)


INDEX_MSG = "SEC's ticker index could not be read, so AAPL was not resolved to a CIK; retry."
SUBS_MSG = "SEC submissions could not be read; retry."
INDEX_FILING_MSG = "SEC filing index could not be read; retry."


def unavailable(message: str) -> dict:
    return {"ok": False, "error": {"code": "PROVIDER_ERROR", "message": message, "retryable": True}}


# ── 1. Ticker index or submissions unreadable: PROVIDER_ERROR retryable; a 404 and an unknown ticker keep TICKER_NOT_FOUND ──
ENVELOPE_TOOLS = [
    ("list_sec_company_filings", server.list_sec_company_filings, "sec", "listSecCompanyFilings", ("10-K", 5),
     "Could not find EDGAR submissions for ticker 'AAPL'"),
    ("list_sec_material_filings", server.list_sec_material_filings, "sec", "listSecMaterialFilings", (None, 5),
     "Could not find EDGAR submissions for ticker 'AAPL'"),
    ("get_sec_filing_intelligence", server.get_sec_filing_intelligence, "sec", "getSecFilingIntelligence", ("10-K", 0),
     "Could not find EDGAR submissions for ticker 'AAPL'"),
    ("get_sec_filing_section_markdown", server.get_sec_filing_section_markdown, "sec", "getSecFilingSectionMarkdown", ("Item 1A", "10-K", 0),
     "Could not find EDGAR submissions for ticker 'AAPL'"),
]
for tool, py_fn, module, ts_fn, extra, not_found_message in ENVELOPE_TOOLS:
    for kind, stub in FAILURES.items():
        CASES.append(_case(f"{tool}.ticker_index_{kind}", _py(py_fn, "AAPL", *extra), _ts(module, ts_fn, "AAPL", *extra),
                           routes_with(ticker_index=stub), _error_view, unavailable(INDEX_MSG)))
        CASES.append(_case(f"{tool}.submissions_{kind}", _py(py_fn, "AAPL", *extra), _ts(module, ts_fn, "AAPL", *extra),
                           routes_with(submissions=stub), _error_view, unavailable(SUBS_MSG)))
    CASES.append(_case(f"{tool}.submissions_404_is_ticker_not_found", _py(py_fn, "AAPL", *extra), _ts(module, ts_fn, "AAPL", *extra),
                       routes_with(submissions=ABSENT), _error_view,
                       {"ok": False, "error": {"code": "TICKER_NOT_FOUND", "message": not_found_message, "retryable": None}}))
    CASES.append(_case(f"{tool}.unknown_ticker_is_ticker_not_found", _py(py_fn, "ZZZZ", *extra), _ts(module, ts_fn, "ZZZZ", *extra),
                       BASE_ROUTES, _error_view,
                       {"ok": False, "error": {"code": "TICKER_NOT_FOUND", "message": not_found_message.replace("AAPL", "ZZZZ"), "retryable": None}}))

# ── 2. The resolver payloads (search_filing_text, the index and the extractors that resolve a filing) ─────────────────────
SEARCH_UNAVAILABLE = {"status": "PROVIDER_ERROR", "code": "SEC_LOOKUP_UNAVAILABLE", "confidence": "SEC_LOOKUP_UNAVAILABLE", "retryable": True}
for kind, stub in FAILURES.items():
    for what, override, message in (("ticker_index", {"ticker_index": stub}, INDEX_MSG), ("submissions", {"submissions": stub}, SUBS_MSG)):
        CASES.append(_case(f"search_filing_text.{what}_{kind}",
                           _py(server.search_filing_text, "AAPL", ["revenue"], None, "10-K"),
                           _ts("sec", "searchFilingText", "AAPL", ["revenue"], None, "10-K"),
                           routes_with(**override), _error_view, {**SEARCH_UNAVAILABLE, "message": message}))
        CASES.append(_case(f"get_sec_filing_index.{what}_{kind}",
                           _py(server.get_sec_filing_index, "AAPL", "10-K", "latest"),
                           _ts("sec", "getSecFilingIndex", "AAPL", "10-K", "latest"),
                           routes_with(**override), _error_view))
CASES.append(_case("search_filing_text.submissions_404_is_ticker_not_found",
                   _py(server.search_filing_text, "AAPL", ["revenue"], None, "10-K"),
                   _ts("sec", "searchFilingText", "AAPL", ["revenue"], None, "10-K"),
                   routes_with(submissions=ABSENT), _error_view,
                   {"status": "TICKER_NOT_FOUND", "code": "TICKER_NOT_FOUND", "confidence": "TICKER_NOT_FOUND",
                    "message": "Could not resolve EDGAR submissions for ticker 'AAPL'"}))
CASES.append(_case("get_sec_filing_index.submissions_404_is_ticker_not_found",
                   _py(server.get_sec_filing_index, "AAPL", "10-K", "latest"),
                   _ts("sec", "getSecFilingIndex", "AAPL", "10-K", "latest"),
                   routes_with(submissions=ABSENT), _error_view))

# ── 3. Exhibit list and exhibit content: a CIK from the ticker, else from the accession ───────────────────────────────────
def EXHIBITS_VIEW(v):
    if isinstance(v, dict) and v.get("ok") is not False:
        v = v.get("data") if isinstance(v.get("data"), dict) else v  # Python wraps the payload in an envelope
        return {"exhibits": [[e.get("type"), e.get("document")] for e in v.get("exhibits", [])]}
    return _error_view(v)
for kind, stub in FAILURES.items():
    CASES.append(_case(f"list_sec_filing_exhibits.ticker_index_{kind}",
                       _py(server.list_sec_filing_exhibits, "AAPL", NO_CIK_ACC), _ts("sec", "listSecFilingExhibits", "AAPL", NO_CIK_ACC),
                       routes_with(ticker_index=stub), _error_view, unavailable(INDEX_MSG)))
    CASES.append(_case(f"list_sec_filing_exhibits.submissions_{kind}",
                       _py(server.list_sec_filing_exhibits, "AAPL", NO_CIK_ACC), _ts("sec", "listSecFilingExhibits", "AAPL", NO_CIK_ACC),
                       routes_with(submissions=stub), _error_view))
    CASES.append(_case(f"list_sec_filing_exhibits.index_{kind}",
                       _py(server.list_sec_filing_exhibits, "AAPL", EIGHT_K_ACC), _ts("sec", "listSecFilingExhibits", "AAPL", EIGHT_K_ACC),
                       routes_with(eight_k_index=stub), _error_view, unavailable(INDEX_FILING_MSG)))
CASES.append(_case("list_sec_filing_exhibits.index_404_is_accession_not_found",
                   _py(server.list_sec_filing_exhibits, "AAPL", EIGHT_K_ACC), _ts("sec", "listSecFilingExhibits", "AAPL", EIGHT_K_ACC),
                   routes_with(eight_k_index=ABSENT), _error_view,
                   {"ok": False, "error": {"code": "ACCESSION_NOT_FOUND",
                                           "message": f"Accession {EIGHT_K_ACC} was not found in AAPL's SEC filings.", "retryable": False}}))
CASES.append(_case("list_sec_filing_exhibits.index_read",
                   _py(server.list_sec_filing_exhibits, "AAPL", EIGHT_K_ACC), _ts("sec", "listSecFilingExhibits", "AAPL", EIGHT_K_ACC),
                   BASE_ROUTES, EXHIBITS_VIEW, {"exhibits": [["8-K", "aapl-8k.htm"], ["EX-99.1", "ex991.htm"]]}))

# ── 4. exhibitsCount (get_sec_filing_intelligence) and the exhibit expansion of search_filing_text ────────────────────────
EXHIBITS_COUNT = lambda v: v.get("index", {}).get("exhibits_count") if isinstance(v, dict) else v
TEN_K_INDEX_WITH_EXHIBITS = EXHIBIT_INDEX_HTML.replace("000032019325000070", "000032019324000123")
for kind, stub in FAILURES.items():
    CASES.append(_case(f"get_sec_filing_intelligence.exhibits_count_null_when_index_{kind}",
                       _py(server.get_sec_filing_intelligence, "AAPL", "10-K", 0), _ts("sec", "getSecFilingIntelligence", "AAPL", "10-K", 0),
                       routes_with(ten_k_index=stub), EXHIBITS_COUNT, None))
CASES.append(_case("get_sec_filing_intelligence.exhibits_count_0_when_index_404",
                   _py(server.get_sec_filing_intelligence, "AAPL", "10-K", 0), _ts("sec", "getSecFilingIntelligence", "AAPL", "10-K", 0),
                   routes_with(ten_k_index=ABSENT), EXHIBITS_COUNT))
CASES.append(_case("get_sec_filing_intelligence.exhibits_count_read",
                   _py(server.get_sec_filing_intelligence, "AAPL", "10-K", 0), _ts("sec", "getSecFilingIntelligence", "AAPL", "10-K", 0),
                   routes_with(ten_k_index={"body": TEN_K_INDEX_WITH_EXHIBITS}), EXHIBITS_COUNT, 2))

INDEX_WARNING = {"code": "FILING_TEXT_NOT_AVAILABLE", "message": INDEX_FILING_MSG, "severity": "warning", "accessionNumber": TEN_K_ACC}
for kind, stub in FAILURES.items():
    CASES.append(_case(f"search_filing_text.exhibit_index_{kind}_warns",
                       _py(server.search_filing_text, "AAPL", ["revenue"], None, "10-K", TEN_K_ACC, 1500, False, None, include_exhibits=True),
                       _ts("sec", "searchFilingText", "AAPL", ["revenue"], None, "10-K", TEN_K_ACC, 1500, False, None, {"includeExhibits": True}),
                       routes_with(ten_k_index=stub), _warnings("FILING_TEXT_NOT_AVAILABLE"), [INDEX_WARNING]))
CASES.append(_case("search_filing_text.exhibit_index_404_is_silent",
                   _py(server.search_filing_text, "AAPL", ["revenue"], None, "10-K", TEN_K_ACC, 1500, False, None, include_exhibits=True),
                   _ts("sec", "searchFilingText", "AAPL", ["revenue"], None, "10-K", TEN_K_ACC, 1500, False, None, {"includeExhibits": True}),
                   routes_with(ten_k_index=ABSENT), _warnings("FILING_TEXT_NOT_AVAILABLE"), []))

# ── 5. The earnings release (readReleaseText): the 8-K index unreadable is NOT_READ, never the cover page labelled READ ───────────
def _guidance_view(v):
    if not isinstance(v, dict):
        return v
    return {"status": v.get("status"), "retryable": v.get("retryable"),
            "warnings": [(w.get("code"), w.get("message")) for w in v.get("warnings", [])]}


RELEASE_UNREAD = [["RELEASE_TEXT_NOT_AVAILABLE", "1 of 1 earnings releases could not be read from SEC; their guidance is missing, not withdrawn. Retry."]]
ACTUALS_UNREAD = ["ACTUALS_NOT_READ", "SEC companyfacts could not be read, so outcomes were not evaluated; retry."]
FACTS_FAILED = ["SEC_READ_FAILED", "SEC companyfacts could not be read; figures that need them are missing; retry."]
for kind, stub in FAILURES.items():
    CASES.append(_case(f"get_guidance_history.eight_k_index_{kind}_is_not_read",
                       _py(server.get_guidance_history, "AAPL", 1), _ts("earnings", "getGuidanceHistory", "AAPL", 1),
                       routes_with(eight_k_index=stub), _guidance_view,
                       {"status": "RELEASE_TEXT_NOT_AVAILABLE", "retryable": True, "warnings": [*RELEASE_UNREAD, ACTUALS_UNREAD]}))
    CASES.append(_case(f"get_guidance_history.companyfacts_{kind}_adds_sec_read_failed",
                       _py(server.get_guidance_history, "AAPL", 1), _ts("earnings", "getGuidanceHistory", "AAPL", 1),
                       routes_with(facts=stub), _guidance_view,
                       {"status": "PARTIAL", "retryable": True, "warnings": [ACTUALS_UNREAD, FACTS_FAILED]}))
CASES.append(_case("get_guidance_history.eight_k_index_404_reads_the_8k",
                   _py(server.get_guidance_history, "AAPL", 1), _ts("earnings", "getGuidanceHistory", "AAPL", 1),
                   routes_with(eight_k_index=ABSENT), _guidance_view,
                   {"status": "PARTIAL", "retryable": True, "warnings": [ACTUALS_UNREAD]}))
CASES.append(_case("get_guidance_history.companyfacts_404_is_actuals_not_read_only",
                   _py(server.get_guidance_history, "AAPL", 1), _ts("earnings", "getGuidanceHistory", "AAPL", 1),
                   BASE_ROUTES, _guidance_view, {"status": "PARTIAL", "retryable": True, "warnings": [ACTUALS_UNREAD]}))

# ── 6. get_latest_earnings_release: the lookup and the period read ─────────────────────────────────────────────────────────
def _release_view(v):
    return {"warnings": [(w.get("code"), w.get("message")) for w in v.get("warnings", [])], "period": v.get("period")} if isinstance(v, dict) else v


# Yahoo answers the calendar with no earnings date, so a release with no SEC 8-K has no Yahoo date to fall back to.
NO_YAHOO = {
    "https://fc.yahoo.com": {"headers": {"set-cookie": "A3=fake-cookie; Path=/"}},
    "https://query1.finance.yahoo.com/v1/test/getcrumb": {"body": "fake-crumb"},
    "https://query2.finance.yahoo.com/v1/test/getcrumb": {"body": "fake-crumb"},
    "https://query1.finance.yahoo.com/v10/finance/quoteSummary/AAPL*": {"json": {"quoteSummary": {"result": [{"calendarEvents": {}}], "error": None}}},
}
for kind, stub in FAILURES.items():
    CASES.append(_case(f"get_latest_earnings_release.submissions_{kind}_warns",
                       _py(server.get_latest_earnings_release, "AAPL"), _ts("earnings", "getLatestEarningsRelease", "AAPL"),
                       {**routes_with(submissions=stub), **NO_YAHOO}, _release_view, {"period": "latest", "warnings": [["SEC_LOOKUP_UNAVAILABLE", SUBS_MSG]]}))
    CASES.append(_case(f"get_latest_earnings_release.ex991_{kind}_warns",
                       _py(server.get_latest_earnings_release, "AAPL"), _ts("earnings", "getLatestEarningsRelease", "AAPL"),
                       routes_with(ex991=stub), _release_view,
                       {"period": "latest", "warnings": [["RELEASE_TEXT_NOT_AVAILABLE", f"The earnings release {EX991_DOC} could not be read from SEC; retry."]]}))
CASES.append(_case("get_latest_earnings_release.submissions_404_has_no_lookup_warning",
                   _py(server.get_latest_earnings_release, "AAPL"), _ts("earnings", "getLatestEarningsRelease", "AAPL"),
                   {**routes_with(submissions=ABSENT), **NO_YAHOO}, _release_view, {"period": "latest", "warnings": []}))
CASES.append(_case("get_latest_earnings_release.ex991_404_has_no_warning",
                   _py(server.get_latest_earnings_release, "AAPL"), _ts("earnings", "getLatestEarningsRelease", "AAPL"),
                   routes_with(ex991=ABSENT), _release_view, {"period": "latest", "warnings": []}))

# ── 7. companyfacts unreadable in the valuation tools: the status stays, the code says the read failed ────────────────────
def _facts_view(v):
    return {k: v.get(k) for k in ("status", "code", "retryable")} if isinstance(v, dict) else v


for kind, stub in FAILURES.items():
    CASES.append(_case(f"reconcile_metric_sources.companyfacts_{kind}",
                       _py(server.reconcile_metric_sources, "AAPL", "revenue"), _ts("valuationTools", "reconcileMetricSources", "AAPL", "revenue"),
                       routes_with(facts=stub), _facts_view,
                       {"status": "COMPANYFACTS_NOT_AVAILABLE", "code": "SEC_READ_FAILED", "retryable": True}))
    CASES.append(_case(f"get_historical_valuation_context.companyfacts_{kind}",
                       _py(server.get_historical_valuation_context, "AAPL"), _ts("valuationTools", "getHistoricalValuationContext", "AAPL"),
                       routes_with(facts=stub), lambda v: _facts_view(v.get("subject")) if isinstance(v, dict) else v,
                       {"status": "COMPANYFACTS_NOT_AVAILABLE", "code": "SEC_READ_FAILED", "retryable": True}))
CASES.append(_case("reconcile_metric_sources.companyfacts_404",
                   _py(server.reconcile_metric_sources, "AAPL", "revenue"), _ts("valuationTools", "reconcileMetricSources", "AAPL", "revenue"),
                   BASE_ROUTES, _facts_view, {"status": "COMPANYFACTS_NOT_AVAILABLE", "code": "COMPANYFACTS_NOT_AVAILABLE", "retryable": True}))
CASES.append(_case("get_historical_valuation_context.companyfacts_404",
                   _py(server.get_historical_valuation_context, "AAPL"), _ts("valuationTools", "getHistoricalValuationContext", "AAPL"),
                   BASE_ROUTES, lambda v: _facts_view(v.get("subject")) if isinstance(v, dict) else v,
                   {"status": "COMPANYFACTS_NOT_AVAILABLE", "code": "COMPANYFACTS_NOT_AVAILABLE", "retryable": True}))

# ── 8. Python's public-page fetch: a timeout or socket error is a failed fetch (None), as the Worker's fetchPublicHtml ──────
# The Worker's fetchPublicHtml is not exported; its result for a timed-out page is null by construction (its catch returns
# null), so the Worker side is the literal and the case pins Python to it.
from yfmcp.tools import earnings as earnings_tools  # noqa: E402

PUBLIC_PAGE = "https://www.sec.gov/Archives/edgar/data/320193/000032019325000070/ir-release.htm"
for kind, stub in (("timeout", {"error": "timeout"}), ("500", {"status": 500}), ("404", {"status": 404})):
    CASES.append(Case(name=f"sec_reads_380.fetch_public_html_{kind}_is_none", ids=IDS,
                      py=lambda: earnings_tools._fetch_public_html(PUBLIC_PAGE), ts={"js": "return null;"},
                      routes={PUBLIC_PAGE: stub}, expect=None))
