"""Parity cases for the 2.26.5 audit repairs (audit 2026-10-27; analysis/yahoo-finance-mcp/F_TRIAGE.md §7).

F54: capital_structure's ladderCoverage counts the tagged principal ladder when no instrument carries its own maturity;
     coveragePct is null (not measurable at carrying amounts), not 0.
F55: management commentary matches a topic word's simple singular or plural ("tariffs" finds "tariff").
F56: get_market_snapshot of an unknown ticker is TICKER_NOT_FOUND, not retried; component messages carry no URLs.
F57: compare_peer_valuations error entries carry a code and no URL.
F60: reconcile_metric_sources refuses a period it cannot read before any fetch; a 10-Q geographic table whose column
     header has no year ("Three Months Ended") gives a null period, not the header.
F62: extract_exposure drops TABLE_NOT_PARSED when the parsed tables simply have no row for the region (the F35 case
     in cases_audit_2254.py now expects no warning); the manufacturing phrases apply to every region.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

import server  # noqa: E402
from scripts.parity import cases_capital_structure as C
from scripts.parity.cases_accession import ROUTES as SEC_ROUTES, _py, _ts
from scripts.parity.cases_capital_structure import T, _capital, _debt_filing, _filing, _N130
from scripts.parity.cases_neutrality import AAPL_DOC, SECTION_ROUTES, _tool_case
from scripts.parity.cases_pricing_evidence_tools import YAHOO_AUTH_ROUTES
from scripts.parity.harness import Case
from yfmcp import metric_reconciliation as _mr
from yfmcp.envelope import _envelope_tool_result
from yfmcp.tools import earnings as _earnings
from yfmcp.tools import pricing as _pricing

CASES: list[Case] = []


# ── F54: the tagged ladder is what is placed ───────────────────────────────────────────────────────────────────
_DEBT = "LongTermDebtMaturitiesRepaymentsOfPrincipal"


def _ladder_filing(buckets: dict[str, float], total: float, rows=()) -> str:
    """A 10-K with total debt `total`, optional instrument rows, and a tagged principal ladder."""
    base = _debt_filing(list(rows), total)
    ladder = "<p>" + "".join(T._num(f"us-gaap:{_DEBT}{b}", "end", "usd", f"{v:,.0f}") for b, v in buckets.items()) + "</p>"
    return base.replace("</body>", ladder + "</body>")


def _coverage_view(v: dict) -> dict:
    cov = v.get("ladderCoverage") or {}
    return {"status": v["status"], "ladderedAmount": cov.get("ladderedAmount"), "ladderedAmountBasis": cov.get("ladderedAmountBasis"),
            "coveragePct": cov.get("coveragePct"),
            "ladderWarning": [w["code"] for w in v["warnings"] if w["code"] == "MATURITY_LADDER_INCOMPLETE"]}


_AAPL_LIKE = {"InNextTwelveMonths": 10_000_000_000, "InYearTwo": 12_000_000_000, "AfterYearFive": 69_281_000_000}
CASES += [
    # AAPL's 10-K: no per-note maturities, a tagged ladder of 91,281M. Before 2.26.5 ladderedAmount was 0, coveragePct 0.
    _capital("audit2265.capital_structure.tagged_ladder_without_instrument_maturities_is_placed", ["F54"],
             _ladder_filing(_AAPL_LIKE, 91_281_000_000), filing="10-K", select=_coverage_view,
             expect={"status": "COMPUTED", "ladderedAmount": 91_281_000_000, "ladderedAmountBasis": "TAGGED", "coveragePct": None,
                     "ladderWarning": []}),
    # A note with its own maturity keeps the carrying-amount coverage it had.
    _capital("audit2265.capital_structure.laddered_note_keeps_carrying_coverage", ["F54"], _debt_filing([_N130], 130_000_000),
             select=_coverage_view,
             expect={"status": "COMPUTED", "ladderedAmount": 130_000_000, "ladderedAmountBasis": "CARRYING", "coveragePct": 100,
                     "ladderWarning": []}),
]


# ── F55: singular and plural topic words ──────────────────────────────────────────────────────────────────────
for _word, _singular in [("tariffs", "tariff"), ("supplies", "supply"), ("taxes", "tax"), ("businesses", "business"),
                         ("business", "business"), ("analysis", "analysis"), ("status", "status"), ("news", "news"),
                         ("margins", "margin"), ("rates", "rate"), ("AI", "ai"), ("H-1Bs", "h-1bs")]:
    CASES.append(Case(name=f"audit2265.commentary_singular.{_word.lower().replace('-', '_')}", ids=["F55"],
                      py=(lambda w=_word: _earnings._commentary_singular(w)),
                      ts={"js": "return m.yahooFinance.commentarySingular(args[0]);", "args": [_word]}, expect=_singular))

_RELEASE = ("Apple reported record revenue for the September quarter. "
            "Gross margin was affected by tariff costs of about $1.1 billion during the quarter, and the company expects tariff refunds to be limited. "
            "Supply of the newest products remained constrained.")
for _key, _topic, _expect in [
    ("plural_topic_finds_singular_text", "tariffs", {"found": True, "shows": "tariff costs", "matchedTerms": ["tariffs"]}),
    ("singular_topic_finds_singular_text", "tariff", {"found": True, "shows": "tariff costs", "matchedTerms": ["tariff"]}),
    ("plural_ies_topic_finds_singular", "supplies", {"found": True, "shows": "Supply of the newest", "matchedTerms": ["supplies"]}),
    ("unrelated_topic_not_found", "rates", {"found": False, "shows": None, "matchedTerms": None}),
]:
    CASES.append(Case(
        name=f"audit2265.sentence_for_topic.{_key}", ids=["F55"],
        py=(lambda t=_topic, e=_expect: (lambda r: {"found": r is not None, "shows": e["shows"] if r and e["shows"] and e["shows"] in r["excerpt"] else None,
                                                    "matchedTerms": r["matchedTerms"] if r else None})(_earnings._sentence_for_topic(_RELEASE, t))),
        ts={"js": "const r = m.yahooFinance.sentenceForTopic(args[0], args[1]); "
                  "return {found: r !== null, shows: r && args[2] && r.excerpt.includes(args[2]) ? args[2] : null, matchedTerms: r ? r.matchedTerms : null};",
            "args": [_RELEASE, _topic, _expect["shows"]]},
        expect=_expect))


# ── F56: an unknown ticker in the snapshot ────────────────────────────────────────────────────────────────────
for _key, _msg, _clean in [
    ("url_removed", "Yahoo API error 404 for https://query1.finance.yahoo.com/v8/finance/chart/ZZZZQQ?range=1y: Not Found",
     "Yahoo API error 404 for Not Found"),
    ("trailing_colon_trimmed", "fetch failed for https://query2.finance.yahoo.com/v10/finance/quoteSummary/ZZZZQQ:", "fetch failed for"),
    ("empty_after_cleaning", "https://example.com/x", "error in response"),
    ("plain_text_kept", "no price data found for ZZZZQQ", "no price data found for ZZZZQQ"),
]:
    CASES.append(Case(name=f"audit2265.snapshot_component_message.{_key}", ids=["F56"],
                      py=(lambda m=_msg: _pricing._snapshot_component_message(m)),
                      ts={"js": "return m.yahooFinance.snapshotComponentMessage(args[0]);", "args": [_msg]}, expect=_clean))
for _key, _msg, _hit in [("api_error_404", "Yahoo API error 404 for x", True), ("no_price_data", "No price data found", True),
                         ("code", "TICKER_NOT_FOUND: no data found for ticker ZZZZQQ", True), ("timeout", "Request timed out", False)]:
    CASES.append(Case(name=f"audit2265.snapshot_ticker_not_found.{_key}", ids=["F56"],
                      py=(lambda m=_msg: _pricing._snapshot_ticker_not_found(m)),
                      ts={"js": "return m.yahooFinance.snapshotTickerNotFound(args[0]);", "args": [_msg]}, expect=_hit))


class _UnknownFastInfo:
    def keys(self):
        return ["lastPrice", "currency", "quoteType", "yearHigh"]

    def __getitem__(self, key):
        raise KeyError(key)


class _UnknownTicker:
    """yfinance for a ticker that does not exist: every fast_info field fails, info and history are empty."""

    def __init__(self, *_a, **_k):
        self.fast_info = _UnknownFastInfo()
        self.info = {}

    def history(self, *_a, **_k):
        import pandas as pd
        return pd.DataFrame()


_NOT_FOUND_ROUTES = {
    **YAHOO_AUTH_ROUTES,
    "https://query1.finance.yahoo.com/v*": {"status": 404, "body": "Not Found"},
    "https://query2.finance.yahoo.com/v*": {"status": 404, "body": "Not Found"},
}


def _snapshot_view(v: dict) -> dict:
    if isinstance(v, dict) and "tickers" in v:
        return {"tickers": v["tickers"]}
    return {"ok": v.get("ok"), "code": (v.get("error") or {}).get("code"), "message": (v.get("error") or {}).get("message"),
            "retryable": (v.get("meta") or {}).get("retryable", (v.get("error") or {}).get("retryable"))}


def _py_snapshot(arg):
    def run():
        with patch.object(_pricing.yf, "Ticker", _UnknownTicker), patch.dict(_pricing._tool_cache._store, {}, clear=True):
            raw = asyncio.run(_pricing.get_market_snapshot(arg))
        return json.loads(raw) if isinstance(arg, list) else _envelope_tool_result("get_market_snapshot", raw)
    return run


CASES += [
    Case(name="audit2265.market_snapshot.unknown_ticker_is_ticker_not_found", ids=["F56"], py=_py_snapshot("ZZZZQQ"),
         ts={"js": 'm.response.setWorkerEnv({}); return (await m.tools.callToolResult("get_market_snapshot", {ticker: "ZZZZQQ"})).value;',
             "isolate": True},
         routes=_NOT_FOUND_ROUTES, select=_snapshot_view,
         expect={"ok": False, "code": "TICKER_NOT_FOUND", "message": "No provider data was found for the requested ticker.", "retryable": False}),
    Case(name="audit2265.market_snapshot.unknown_ticker_in_a_batch_is_ticker_not_found", ids=["F56"], py=_py_snapshot(["ZZZZQQ"]),
         ts={"js": 'm.response.setWorkerEnv({}); return JSON.parse(await m.yahooFinance.getMarketSnapshot(["ZZZZQQ"]));', "isolate": True},
         routes=_NOT_FOUND_ROUTES, select=_snapshot_view,
         expect={"tickers": {"ZZZZQQ": {"error": True, "code": "TICKER_NOT_FOUND",
                                        "message": "No provider data was found for the requested ticker.", "retryable": False}}}),
]


# ── F57: peer valuation error entries ─────────────────────────────────────────────────────────────────────────
_NF = "No provider data was found for the requested ticker."
for _key, _msg, _expect in [
    ("api_404", "Yahoo API error 404 for https://query1.finance.yahoo.com/v10/finance/quoteSummary/ZZZZQQ?modules=price",
     {"ticker": "ZZZZQQ", "code": "TICKER_NOT_FOUND", "message": _NF}),
    ("no_quote_summary", "No Yahoo quote summary for ZZZZQQ", {"ticker": "ZZZZQQ", "code": "TICKER_NOT_FOUND", "message": _NF}),
    ("provider_error_without_url", "Yahoo API error 500 for https://query1.finance.yahoo.com/v10/finance/quoteSummary/ZZZZQQ",
     {"ticker": "ZZZZQQ", "code": "PROVIDER_ERROR", "message": "Yahoo API error 500 for"}),
]:
    CASES.append(Case(name=f"audit2265.peer_valuation_error.{_key}", ids=["F57"],
                      py=(lambda m=_msg: server._peer_valuation_error("ZZZZQQ", m)),
                      ts={"js": 'return m.yahooFinance.peerValuationError("ZZZZQQ", args[0]);', "args": [_msg]}, expect=_expect))


# ── F60 part 1: the reconcile period is checked before any fetch (3.0.0: one vocabulary, latest | FY<yyyy> | FY<yyyy> Q<n>) ───
for _spec, _ok in [("latest", True), ("LATEST", True), ("FY2026", True), ("fy 2026", True), ("FY2026 Q3", True), ("fy2026 q3", True),
                   ("FY 2026  Q3", True), ("latest_quarter", False), ("latest_annual", False), ("Q3 2026", False), ("q32026", False),
                   ("FY2026 Q5", False), ("FY2026Q3", False), ("2026", False), ("", False)]:
    CASES.append(Case(name=f"audit2265.is_reconcile_period.{_spec.replace(' ', '_') or 'empty'}", ids=["F60"],
                      py=(lambda s=_spec: _mr.is_reconcile_period(s)),
                      ts={"js": "return m.metricReconciliation.isReconcilePeriod(args[0]);", "args": [_spec]}, expect=_ok))

for _spec in ("latest_quarter", "latest_annual", "Q3 2026", "FY2026 Q5"):
    CASES.append(Case(
        name=f"audit2265.reconcile_metric_sources.unreadable_period_{_spec.replace(' ', '_')}_is_input_validation_error", ids=["F60"],
        py=lambda s=_spec: json.loads(asyncio.run(server.reconcile_metric_sources("AAPL", "revenue", s))),
        ts={"js": "return JSON.parse(await m.yahooFinance.reconcileMetricSources('AAPL', 'revenue', args[0]));", "args": [_spec], "isolate": True},
        # No routes: a fetch would fail the case instead of returning the validation error.
        routes={}, select=lambda v: {"error": v.get("error"), "code": v.get("code"), "message": v.get("message")},
        expect={"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "period must be latest, FY<yyyy> or FY<yyyy> Q<n>."}))


# ── F60 part 2: a 10-Q column header with no year is not a period ─────────────────────────────────────────────
GEOQ_CIK = "0000777002"
GEOQ_BASE = "https://www.sec.gov/Archives/edgar/data/777002"
GEOQ_BARE = "0000777002-26-000020"
GEOQ_DATED = "0000777002-26-000010"


def _geoq_html(header: str) -> str:
    return ("<html><body><h2>Item 1. Financial Statements</h2><p>Revenue by geographic area (in millions):</p>"
            f"<table><tr><td>Geographic area</td><td>{header}</td></tr>"
            "<tr><td>United States</td><td>300</td></tr><tr><td>China</td><td>200</td></tr>"
            "<tr><td>Total revenue</td><td>500</td></tr></table></body></html>")


GEOQ_ROUTES = {
    **SEC_ROUTES,
    "https://www.sec.gov/files/company_tickers.json": {"json": {
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 777002, "ticker": "GEOQ", "title": "Geo Quarter Corp"}}},
    f"https://data.sec.gov/submissions/CIK{GEOQ_CIK}.json": {"json": {"cik": "777002", "name": "Geo Quarter Corp", "filings": {"recent": {
        "form": ["10-Q", "10-Q"], "accessionNumber": [GEOQ_BARE, GEOQ_DATED], "primaryDocument": ["geoq-q3.htm", "geoq-q2.htm"],
        "filingDate": ["2026-08-10", "2026-05-10"], "reportDate": ["2026-06-30", "2026-03-31"],
        "acceptanceDateTime": ["2026-08-10T16:30:00.000Z", "2026-05-10T16:30:00.000Z"]}}}},
    f"https://data.sec.gov/api/xbrl/companyconcept/CIK{GEOQ_CIK}/*": {"status": 404, "body": "not found"},
    f"https://data.sec.gov/api/xbrl/companyfacts/CIK{GEOQ_CIK}.json": {"status": 404, "body": "not found"},
    f"{GEOQ_BASE}/000077700226000020/geoq-q3.htm": {"body": _geoq_html("Three Months Ended")},
    f"{GEOQ_BASE}/000077700226000010/geoq-q2.htm": {"body": _geoq_html("Three Months Ended March 31, 2026")},
}


def _period_view(v: dict) -> dict:
    return {"value": v.get("value"), "period": v.get("period")}


for _key, _acc, _period in [("header_without_year_is_null", GEOQ_BARE, None),
                            ("header_with_date_is_kept", GEOQ_DATED, "Three Months Ended March 31, 2026")]:
    CASES.append(Case(name=f"audit2265.geographic_revenue_10q.{_key}", ids=["F60"],
                      py=_py(server.extract_geographic_revenue, "GEOQ", "China", "10-Q", "latest", _acc),
                      ts=_ts("extractGeographicRevenue", "GEOQ", "China", "10-Q", "latest", _acc),
                      routes=GEOQ_ROUTES, select=_period_view, expect={"value": 200_000_000, "period": _period}))


# ── F62: manufacturing phrases for every region ───────────────────────────────────────────────────────────────
_F62_DOC = ("<html><body><h2>Item 1A. Risk Factors</h2><p>Substantially all of the Company's hardware products are manufactured by "
            "outsourcing partners located primarily in China mainland, India, Japan, South Korea, Taiwan and Vietnam. A disruption "
            "there could affect the Company.</p></body></html>")


def _ops_view(v: dict) -> dict:
    data = v.get("data") if isinstance(v.get("data"), dict) else {}
    ops = data.get("operationalExposure") or {}
    return {"status": ops.get("status"), "terms": ops.get("terms")}


CASES.append(_tool_case(
    "audit2265.extract_exposure.outsourcing_partners_in_india_is_operational", "extract_exposure", {"ticker": "AAPL", "topic": "India"},
    lambda: server.extract_exposure("AAPL", "India"), _ops_view,
    routes={**SECTION_ROUTES, AAPL_DOC: {"body": _F62_DOC}, "https://data.sec.gov/api/xbrl/*": {"status": 404, "body": "not found"}},
    ids=("F62",), expect={"status": "FOUND", "terms": ["manufactured"]}))
