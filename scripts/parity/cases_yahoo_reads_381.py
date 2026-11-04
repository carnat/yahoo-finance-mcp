"""Parity cases: a failed Yahoo enrichment read is reported (3.8.1, swallowed-error audit F13, F18-F20, S15, S16).

Per row a failing-read stub is compared with an empty-success stub. The empty success keeps today's output (no warning);
the failed read keeps the tool's answer and adds an entry to the tool's existing warnings list:

- get_uk_company_filings, issuer name read from Yahoo fails: PROVIDER_ERROR (retryable), not INPUT_VALIDATION_ERROR (F19);
- verify_company_event, the Yahoo issuer-name read or the SEC submissions read behind the relevance names fails:
  SOURCE_IDENTITY_UNAVAILABLE / SOURCE_UNAVAILABLE warnings, added after the source status is computed (F18);
- get_historical_valuation_context, the Yahoo FX series fails: FX_NOT_AVAILABLE naming the pair (S15); the Yahoo market
  inputs behind the ADR ratio fail for a foreign filer: SOURCE_UNAVAILABLE (F20, S16); status PARTIAL when it was OK;
- the consensus curve's fiscal-year naming, companyfacts read fails: COMPANYFACTS_NOT_AVAILABLE on the curve (F13).

Not covered here: the status change to PARTIAL on get_historical_valuation_context (the fixture's multiples are already
unusable, so its status is PARTIAL either way), and the wiring of F13 into the four curve tools (the naming read and the
curve helper are compared, each runtime's tools call them).
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

from scripts.parity.cases_news_b import (
    YAHOO_AUTH_ROUTES,
    GNW_FEEDS,
    _gitem,
    _py_sec,
    _rss,
    _scenario,
    _sec_routes,
)
from scripts.parity.cases_providers import CH_KEY, _python_env, _worker_js
from scripts.parity.cases_sec_reads_380 import ABSENT, FAILURES, _py, _ts, routes_with
from scripts.parity.harness import Case

import server  # noqa: E402
from yfmcp.tools import fundamentals as fundamentals_tools  # noqa: E402
from yfmcp.tools import valuation as valuation_tools  # noqa: E402

CASES: list[Case] = []
IDS = ["YAHOO-READS-381"]

QUOTE_SUMMARY = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/*"
QUOTE_SUMMARY2 = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/*"
YAHOO_DOWN = {"status": 500, "body": "{}"}
EMPTY_SUMMARY = {"json": {"quoteSummary": {"result": [{"price": {}, "summaryProfile": {}, "assetProfile": {}}], "error": None}}}


# ── 1. get_uk_company_filings: a failed issuer-name read is PROVIDER_ERROR, not INPUT_VALIDATION_ERROR (F19) ───────────────
class _InfoTicker:
    def __init__(self, info):
        self._info = info

    @property
    def info(self):
        if isinstance(self._info, Exception):
            raise self._info
        return dict(self._info)


def _uk_case(name: str, py_info, summary: dict, expect) -> Case:
    env = {"COMPANIES_HOUSE_API_KEY": CH_KEY}

    async def py():
        with _python_env(env), patch.object(server.yf, "Ticker", lambda *_a, **_k: _InfoTicker(py_info)):
            return json.loads(await server.get_uk_company_filings("ACME.L", None, None, None, 25, False))

    body = 'return JSON.parse(await m.yahooFinance.getUkCompanyFilings("ACME.L", null, null, null, 25, false));'
    routes = {**YAHOO_AUTH_ROUTES, QUOTE_SUMMARY: summary, QUOTE_SUMMARY2: summary,
              "https://www.sec.gov/files/company_tickers.json": ABSENT}
    return Case(name=f"yahoo_reads_381.uk_filings.{name}", ids=IDS, py=lambda: asyncio.run(py()), ts=_worker_js(env, body),
                routes=routes, select=lambda v: {k: v.get(k) for k in ("error", "code", "message", "retryable")}, expect=expect)


CASES += [
    _uk_case("yahoo_down_is_provider_error", RuntimeError("yahoo down"), YAHOO_DOWN,
             {"error": True, "code": "PROVIDER_ERROR", "message": "Yahoo issuer name could not be read; retry.", "retryable": True}),
    _uk_case("empty_profile_keeps_input_validation_error", {}, EMPTY_SUMMARY,
             {"error": True, "code": "INPUT_VALIDATION_ERROR",
              "message": "Provide company_number, company_name, or a ticker whose issuer name can be resolved.", "retryable": None}),
]


# ── 2. verify_company_event: failed alias reads are warnings, after the source status (F18) ───────────────────────────────
# A CIK and ticker no other parity case uses, so Python's process-wide SEC caches cannot serve the submissions read.
CIK, TICKER = 7777771, "ZQFX"
SEC_ROUTES = _sec_routes(CIK, TICKER, "Zqfx Corp", {})
ACME_INFO = {"shortName": "Zqfx", "longName": "Zqfx Corp", "exchange": "NMS"}
IDENTITY_WARNING = ["SOURCE_IDENTITY_UNAVAILABLE", "Yahoo issuer names could not be read; retry."]
SEC_WARNING = ["SOURCE_UNAVAILABLE", "SEC submissions could not be read; retry."]


def _verify_view(v):
    mine = {tuple(w) for w in (IDENTITY_WARNING, SEC_WARNING)}
    rows = [[w["code"], w.get("message")] for w in v["warnings"]]
    return {"status": v["status"], "ownWarnings": [r for r in rows if tuple(r) in mine]}


WIRE = _rss([_gitem(1, "Zqfx Corp announces acquisition of Beta Corp", ["Nasdaq:ZQFX"], desc="Beta Corp holders approve.")])
SUBS_URL = f"https://data.sec.gov/submissions/CIK{CIK:010d}.json"


def _verify_case(name: str, expect_warnings: list, *, info, identity_fails: bool = False, sec_stub: int | None = None) -> Case:
    """verify_company_event over one GlobeNewswire release (no Yahoo identity needed); the SEC submissions read answers sec_stub."""
    routes = {**SEC_ROUTES, GNW_FEEDS: {"body": WIRE, "headers": {"content-type": "application/rss+xml"}}}
    if sec_stub is not None:
        routes[SUBS_URL] = {"status": sec_stub, "body": "{}"}
    return _scenario(
        f"yahoo_reads_381.verify.{name}", IDS,
        lambda: server.verify_company_event("ZQFX", "acquisition", "", "", ["newswire"]),
        'm.yahooFinance.verifyCompanyEvent("ZQFX", "acquisition", "", "", ["newswire"])',
        info=info, identity_fails=identity_fails, submissions=_py_sec(CIK, "Zqfx Corp", {}) if sec_stub is None else None,
        routes=routes, select=_verify_view,
        expect={"status": "CONFIRMED", "ownWarnings": expect_warnings})


CASES += [
    _verify_case("yahoo_name_read_failed", [IDENTITY_WARNING], info=RuntimeError("yahoo down"), identity_fails=True),
    _verify_case("yahoo_name_empty_success_has_no_warning", [], info={}),
    _verify_case("sec_submissions_read_failed", [SEC_WARNING], info=ACME_INFO, sec_stub=500),
    _verify_case("sec_submissions_404_has_no_warning", [], info=ACME_INFO, sec_stub=404),
]


# ── 3. get_historical_valuation_context: FX series and ADR market inputs (S15, F20, S16) ─────────────────────────────────
def _row(end: str, val, form="20-F", start="2023-01-01", filed="2024-03-01", **extra) -> dict:
    return {"start": start, "end": end, "val": val, "filed": filed, "form": form, "accn": "0000320193-24-000001", "fy": 2023, "fp": "FY", **extra}


ADR_FACTS = {"cik": 320193, "entityName": "Acme Foreign", "facts": {
    "us-gaap": {"Revenues": {"units": {"RUB": [_row("2023-12-31", 1_000_000)]}}},
    "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [{"end": "2024-02-20", "val": 1_000_000, "filed": "2024-03-01", "form": "20-F"}]}}}}}
DATES = ["2024-06-28"]
USD_BARS = {"bars": [{"date": "2024-06-27", "close": 100.0}, {"date": "2024-06-28", "close": 101.0}], "splits": [], "currency": "USD"}
CHART = "https://query1.finance.yahoo.com/v8/finance/chart/"
CHART2 = "https://query2.finance.yahoo.com/v8/finance/chart/"


def _chart(timestamps: list[int], closes: list[float]) -> dict:
    return {"json": {"chart": {"result": [{"meta": {"currency": "USD", "exchangeTimezoneName": "UTC"}, "timestamp": timestamps,
                                           "indicators": {"quote": [{"close": closes}]}, "events": {}}], "error": None}}}


STOCK_CHART = _chart([1719446400, 1719532800], [100.0, 101.0])
EMPTY_CHART = {"json": {"chart": {"result": None, "error": None}}}
SUMMARY_OK = {"json": {"quoteSummary": {"result": [{"price": {"currency": "USD", "regularMarketPrice": {"raw": 100.0}},
                                                   "defaultKeyStatistics": {}, "financialData": {}, "earningsTrend": {}}], "error": None}}}


def _valuation_case(name: str, *, fx: str, market: str, expect_warnings: list) -> Case:
    """fx: "failed" (the Yahoo read raises) or "empty" (answers with no bars); market: "failed" or "empty" (no share count)."""
    def fake_history(ticker: str, from_date: str):
        if ticker == "AAPL":
            return dict(USD_BARS)
        if fx == "failed":
            raise RuntimeError("yahoo down")
        return None

    async def fake_market(ticker: str) -> dict:
        if market == "failed":
            raise RuntimeError("yahoo down")
        return {"ticker": ticker, "sharesOutstanding": None}

    def py():
        with patch.object(valuation_tools, "_daily_history_sync", fake_history), patch.object(valuation_tools, "_valuation_market_inputs", fake_market):
            return json.loads(_py(server.get_historical_valuation_context, "AAPL", DATES)())

    routes = {**routes_with(facts={"json": ADR_FACTS}), **YAHOO_AUTH_ROUTES,
              CHART + "AAPL?*": STOCK_CHART, CHART2 + "AAPL?*": STOCK_CHART,
              CHART + "RUBUSD*": YAHOO_DOWN if fx == "failed" else EMPTY_CHART, CHART2 + "RUBUSD*": YAHOO_DOWN if fx == "failed" else EMPTY_CHART,
              QUOTE_SUMMARY: YAHOO_DOWN if market == "failed" else SUMMARY_OK, QUOTE_SUMMARY2: YAHOO_DOWN if market == "failed" else SUMMARY_OK}

    def view(v):
        subject = v["subject"]
        keep = {"FX_NOT_AVAILABLE", "SOURCE_UNAVAILABLE", "ADR_RATIO_APPLIED"}
        return {"status": subject["status"], "warnings": [[w["code"], w["message"], w["severity"]] for w in subject.get("warnings") or [] if w["code"] in keep]}

    case = Case(name=f"yahoo_reads_381.valuation.{name}", ids=IDS, py=py,
                ts=_ts("valuationTools", "getHistoricalValuationContext", "AAPL", DATES), routes=routes, select=view)
    if expect_warnings is not None:
        case.expect = {"status": "PARTIAL", "warnings": expect_warnings}
    return case


FX_WARNING = ["FX_NOT_AVAILABLE", "Yahoo FX series RUBUSD=X could not be read; retry.", "warning"]
ADR_WARNING = ["SOURCE_UNAVAILABLE", "Yahoo market inputs could not be read, so no ADR ratio was applied; retry.", "warning"]

CASES += [
    _valuation_case("fx_read_failed", fx="failed", market="empty", expect_warnings=[FX_WARNING]),
    _valuation_case("fx_empty_success_has_no_read_warning", fx="empty", market="empty", expect_warnings=[]),
    _valuation_case("adr_market_read_failed", fx="empty", market="failed", expect_warnings=[ADR_WARNING]),
    _valuation_case("adr_market_empty_success_has_no_warning", fx="empty", market="empty", expect_warnings=[]),
]


# ── 4. consensus curve fiscal-year naming: a failed companyfacts read is a warning on the curve (F13) ─────────────────────
def _naming_case(name: str, facts_stub: dict, expect: dict) -> Case:
    def view(v):
        return {"basis": v["naming"]["basis"], "readFailed": v["readFailed"]}

    def py():
        naming, failed = _py(fundamentals_tools._consensus_naming, "AAPL")()
        return {"naming": naming, "readFailed": failed}

    return Case(name=f"yahoo_reads_381.consensus_naming.{name}", ids=IDS, py=py,
                ts={"js": 'const r = await m.yahooFinance.secFiscalYearNaming("AAPL"); return r;', "isolate": True},
                routes=routes_with(facts=facts_stub), select=view, expect=expect)


CASES += [
    *[_naming_case(f"facts_{kind}", stub, {"basis": "PERIOD_END_RULE_SEC_NOT_READ", "readFailed": True}) for kind, stub in FAILURES.items()],
    _naming_case("facts_404_is_not_a_failed_read", ABSENT, {"basis": "PERIOD_END_RULE_SEC_NOT_READ", "readFailed": False}),
    _naming_case("facts_read_is_not_a_failed_read", {"json": ADR_FACTS}, {"basis": "PERIOD_END_RULE_NO_STATED_YEAR", "readFailed": False}),
]

CURVE = {"ticker": "AAPL", "warnings": [{"code": "EXISTING", "message": "kept", "severity": "info"}]}
NAMING_WARNING = {"code": "COMPANYFACTS_NOT_AVAILABLE", "severity": "warning",
                  "message": "SEC companyfacts could not be read, so fiscal years use the period-end rule; retry."}
for _failed, _warnings in ((True, [CURVE["warnings"][0], NAMING_WARNING]), (False, CURVE["warnings"])):
    CASES.append(Case(
        name=f"yahoo_reads_381.consensus_curve.naming_read_failed_{str(_failed).lower()}", ids=IDS,
        py=lambda _f=_failed: fundamentals_tools._curve_with_naming_read({**CURVE, "warnings": list(CURVE["warnings"])}, _f),
        ts={"js": f"return m.yahooFinance.curveWithNamingRead({json.dumps(CURVE)}, {json.dumps(_failed)});"},
        expect={**CURVE, "warnings": _warnings}))
