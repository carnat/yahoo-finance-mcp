"""Parity cases for the 2.26.4 audit repairs (audit 2026-10-27; analysis/yahoo-finance-mcp/F_TRIAGE.md §5).

F51: extract_earnings_metrics reads the SEC XBRL quarter when it ended within 120 days before the release was filed,
     not within 120 days of today; the age from today applies only when the release date is unknown.
F52: get_thai_fund_nav without proj_id keeps only the resolved share class's rows when the provider returns several.
F53: Python's dividendYield is a decimal ratio, the Worker's summaryDetail value (yfinance overlays the v7 quote's
     percent).
F22: compare_earnings_actual_vs_estimate rates the Yahoo actual EPS LOW, as the Yahoo estimates are (F16).
F23 is the flipped case f21.query.segment_revenue_no_accession_unchanged in cases_accession.py.
F24 is closed: on 2.26.3 the tool's estimate rows already have the same 9 keys in both runtimes (audit 2026-10-27).
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import json
import os
from types import SimpleNamespace
from unittest.mock import patch

import server  # noqa: E402
from scripts.parity.cases_audit_2255 import _view as _metrics_view
from scripts.parity.cases_pricing_evidence_tools import YAHOO_AUTH_ROUTES
from scripts.parity.cases_sec_earnings import _REL_Q2, _REVEST, _ROW, _compare_case, _doc_url, _run, _sec_routes, _srv, _unwrap
from scripts.parity.harness import Case
from yfmcp.tools import earnings as _earnings
from yfmcp.tools import thai_funds as _thai

CASES: list[Case] = []

TODAY = datetime.date.today()


def _d(days_ago: int) -> str:
    return (TODAY - datetime.timedelta(days=days_ago)).isoformat()


# ── F51: the XBRL window is measured from the release ──────────────────────────────────────────────────────────
def _fact(val, start: str, end: str, filed: str) -> dict:
    return {"val": val, "start": start, "end": end, "filed": filed, "form": "10-Q"}


def _concepts(q_end_ago: int, filed: str) -> dict:
    """A second fiscal quarter that ended q_end_ago days ago, with nine months to date, as in cases_audit_2255."""
    q_end, q_start, prior_end, ytd_start = _d(q_end_ago), _d(q_end_ago + 90), _d(q_end_ago + 91), _d(q_end_ago + 272)
    prior_filed = _d(q_end_ago + 80)
    return {
        "RevenueFromContractWithCustomerExcludingAssessedTax": ("USD", [
            _fact(1_100_000_000, q_start, q_end, filed), _fact(3_000_000_000, ytd_start, q_end, filed)]),
        "EarningsPerShareDiluted": ("USD/shares", [_fact(0.55, q_start, q_end, filed), _fact(1.5, ytd_start, q_end, filed)]),
        "GrossProfit": ("USD", [_fact(440_000_000, q_start, q_end, filed)]),
        "OperatingIncomeLoss": ("USD", [_fact(200_000_000, q_start, q_end, filed)]),
        "PaymentsToAcquirePropertyPlantAndEquipment": ("USD", [
            _fact(-90_000_000, ytd_start, q_end, filed), _fact(-60_000_000, ytd_start, prior_end, prior_filed)]),
        "NetCashProvidedByUsedInOperatingActivities": ("USD", [
            _fact(900_000_000, ytd_start, q_end, filed), _fact(600_000_000, ytd_start, prior_end, prior_filed)]),
    }


def _metrics_case(name: str, tkr: str, cik: int, q_end_ago: int, release_ago: int, filed_ago: int, expect: dict) -> Case:
    accn = f"0000{cik:06d}-26-000001"
    doc = _doc_url(cik, accn, "r8k.htm")
    release = "Acme Reports Second Quarter Fiscal 2026 Results. Revenue was $1.0 billion for the quarter."
    routes = _sec_routes(tkr, cik, [{"accession": accn, "date": _d(release_ago), "primary": "r8k.htm"}],
                         {doc: f"<html><body><p>{release}</p></body></html>"})
    routes["https://data.sec.gov/api/xbrl/companyconcept/*"] = {"status": 404}
    for concept, (unit, facts) in _concepts(q_end_ago, _d(filed_ago)).items():
        routes[f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/us-gaap/{concept}.json"] = {"json": {"units": {unit: facts}}}
    return Case(name=name, ids=["F51"], py=lambda: _run(lambda: _srv().extract_earnings_metrics(tkr))(),
                ts={"js": "m.response.setWorkerEnv({}); return m.yahooFinance.extractEarningsMetrics(args[0]);", "args": [tkr], "isolate": True},
                routes=routes, select=_metrics_view, expect=expect)


XBRL = {"revenue": [1_100_000_000, "HIGH", "DIRECT_QUARTER"], "epsDiluted": [0.55, "HIGH", "DIRECT_QUARTER"],
        "grossMargin": [40.0, "HIGH", "DIRECT_QUARTER"], "operatingIncome": [200_000_000, "HIGH", "DIRECT_QUARTER"],
        "freeCashFlow": [270_000_000, "HIGH", "YTD_DIFFERENCE"], "capex": [30_000_000, "HIGH", "YTD_DIFFERENCE"]}
TEXT_ONLY = {"revenue": [1_000_000_000, "LOW", None]}

CASES += [
    # AAPL on 2026-10-27: the quarter ended 125 days ago and its release (95 days ago) is still the latest. Before
    # 2.26.4 the 120 days were counted to today and every XBRL metric was dropped.
    _metrics_case("audit2264.earnings_metrics.quarter_125_days_old_with_its_release_still_latest_reads_xbrl", "ZWA", 840201,
                  125, 95, 94, {"metrics": XBRL, "confidence": "HIGH", "evidence": ["sec_xbrl_10q"] * 6}),
    # The quarter ended 135 days before the release: not the release's quarter, so the release text stands.
    _metrics_case("audit2264.earnings_metrics.quarter_135_days_before_the_release_is_not_used", "ZWB", 840202,
                  155, 20, 10, {"metrics": TEXT_ONLY, "confidence": "LOW", "evidence": ["sec_8k"]}),
]

# The window on its own, with a fixed clock: release-relative when the release date is known, from now otherwise.
_NOW = "2026-10-27T12:00:00Z"
_DAY = 86_400
for _key, _end, _release, _expect in [
    ("119_days_before_release", "2026-06-27", "2026-10-24", True),
    ("120_days_before_release", "2026-06-27", "2026-10-25", False),
    ("release_before_period_end", "2026-06-27", "2026-06-20", False),
    ("release_timestamp_counts_its_date", "2026-06-27", "2026-07-30T16:30:00.000Z", True),
    ("no_release_date_119_days_ago", "2026-06-30", None, True),
    ("no_release_date_125_days_ago", "2026-06-24", None, False),
    ("no_period_end", None, "2026-07-30", False),
]:
    CASES.append(Case(
        name=f"audit2264.xbrl_quarter_in_window.{_key}", ids=["F51"],
        py=(lambda e=_end, r=_release: _earnings._xbrl_quarter_in_window(
            e, r, 120 * _DAY, datetime.datetime.fromisoformat(_NOW.replace("Z", "+00:00")))),
        ts={"js": "return m.yahooFinance.xbrlQuarterInWindow(args[0], args[1], 120 * 86400000, Date.parse(args[2]));",
            "args": [_end, _release, _NOW]},
        expect=_expect))


# ── F52: Thai NAV keeps the resolved class ─────────────────────────────────────────────────────────────────────
_THAI = "https://api.sec.or.th/v2/fund"
_PROFILE = {"message": "success", "page_size": 100, "next_cursor": "", "items": [{
    "unique_id": "C0000000239", "comp_name_th": "Acme AM", "comp_name_en": "Acme Asset Management Co., Ltd.",
    "proj_id": "M0452_2546", "proj_name_th": "Acme Dividend", "proj_name_en": "Acme Dividend Fund", "fund_class_name": "ACMEDV"}]}


def _nav_row(cls: str, date: str, last_val: float, net_asset: float) -> dict:
    return {"proj_id": "M0452_2546", "fund_class_name": cls, "nav_date": date, "last_val": last_val, "net_asset": net_asset}


def _nav_page(rows: list[dict]) -> dict:
    return {"message": "success", "page_size": 100, "next_cursor": "", "items": rows}


def _nav_view(v):
    v = json.loads(v) if isinstance(v, str) else v
    d = v.get("data") if isinstance(v, dict) and isinstance(v.get("data"), dict) else v
    nav = d.get("nav") or {}
    return {"status": d.get("status"), "dataDate": d.get("dataDate"), "navDate": nav.get("navDate"),
            "lastValue": nav.get("lastValue"), "netAsset": nav.get("netAsset"),
            "fundClassName": (d.get("identity") or {}).get("fundClassName")}


def _thai_py():
    previous = os.environ.get("SEC_OPEN_DATA_API_KEY")
    os.environ["SEC_OPEN_DATA_API_KEY"] = "fixture-key"
    try:
        return asyncio.run(_thai.get_thai_fund_nav("ACMEDV", None, "2026-10-22", 5))
    finally:
        if previous is None:
            os.environ.pop("SEC_OPEN_DATA_API_KEY", None)
        else:
            os.environ["SEC_OPEN_DATA_API_KEY"] = previous


def _nav_case(name: str, rows: list[dict], expect: dict) -> Case:
    routes = {f"{_THAI}/general-info/profiles*": {"json": _PROFILE}, f"{_THAI}/daily-info/nav*": {"json": _nav_page(rows)}}
    return Case(name=name, ids=["F52"], py=_thai_py, routes=routes, select=_nav_view,
                ts={"js": 'm.response.setWorkerEnv({ SEC_OPEN_DATA_API_KEY: "fixture-key" });'
                          ' return await m.secThailand.getThaiFundNav("ACMEDV", null, "2026-10-22", 5, null);', "isolate": True},
                expect=expect)


_OTHER = [_nav_row("ACMEDV-SSF", "2026-10-22", 10.7558, 6_036_061.0), _nav_row("ACMEDV-SSF", "2026-10-21", 10.7401, 6_030_000.0)]
_MINE = [_nav_row("ACMEDV", "2026-10-21", 8.3603, 6_711_444_550.0), _nav_row("ACMEDV", "2026-10-20", 8.3511, 6_700_000_000.0)]
CASES += [
    # SCBDV on 2026-10-27: another class's newer row was returned as SCBDV's NAV (10.7558 for 8.3603).
    _nav_case("audit2264.thai_nav.multi_class_page_keeps_the_resolved_class", _OTHER + _MINE,
              {"status": "OK", "dataDate": "2026-10-21", "navDate": "2026-10-21", "lastValue": 8.3603,
               "netAsset": 6_711_444_550, "fundClassName": "ACMEDV"}),
    _nav_case("audit2264.thai_nav.multi_class_page_without_the_class_is_not_found", _OTHER + [_nav_row("ACMEDV-A", "2026-10-22", 9.1, 1.0)],
              {"status": "NAV_NOT_FOUND_IN_WINDOW", "dataDate": None, "navDate": None, "lastValue": None, "netAsset": None,
               "fundClassName": "ACMEDV"}),
    _nav_case("audit2264.thai_nav.single_class_page_is_unchanged", _MINE,
              {"status": "OK", "dataDate": "2026-10-21", "navDate": "2026-10-21", "lastValue": 8.3603,
               "netAsset": 6_711_444_550, "fundClassName": "ACMEDV"}),
]


# ── F53: dividendYield is a decimal ratio ──────────────────────────────────────────────────────────────────────
def _yahoo_summary(tkr: str, dividend_yield: float) -> dict:
    summary = {"quoteSummary": {"result": [{
        "summaryDetail": {"dividendYield": {"raw": dividend_yield, "fmt": f"{dividend_yield * 100:.2f}%"},
                          "dividendRate": {"raw": 1.04, "fmt": "1.04"}, "payoutRatio": {"raw": 0.1373, "fmt": "13.73%"}},
        "financialData": {"currentPrice": {"raw": 335.0, "fmt": "335.00"}, "financialCurrency": "USD"},
        "defaultKeyStatistics": {},
        "price": {"shortName": "Test", "currency": "USD", "quoteType": "EQUITY"},
    }]}}
    return {**YAHOO_AUTH_ROUTES, f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{tkr}?*": {"json": summary}}


def _py_with_info(call, info: dict):
    def go():
        company = SimpleNamespace(info=info, fast_info=SimpleNamespace(currency="USD"))
        with patch.object(server.yf, "Ticker", lambda *_a, **_k: company):
            return asyncio.run(call())
    return go


def _dividend_view(v):
    v = json.loads(v) if isinstance(v, str) else v
    d = v.get("data") if isinstance(v, dict) and isinstance(v.get("data"), dict) else v
    return {"dividendYield": d.get("dividendYield"), "payoutRatio": d.get("payoutRatio")}


# yfinance's info as it is today: the v7 quote's percent over summaryDetail's ratio.
_INFO = {"quoteType": "EQUITY", "shortName": "Test", "currency": "USD", "financialCurrency": "USD", "currentPrice": 335.0,
         "dividendRate": 1.04, "dividendYield": 0.31, "payoutRatio": 0.1373}
CASES += [
    Case(name="audit2264.profile.dividend_yield_is_a_decimal_ratio", ids=["F53"],
         py=_py_with_info(lambda: server.get_stock_info("ZDY", ["dividends"]), _INFO),
         ts={"js": 'return await m.yahooFinance.getStockInfo("ZDY");', "isolate": True},
         routes=_yahoo_summary("ZDY", 0.0031), select=_dividend_view,
         expect={"dividendYield": 0.0031, "payoutRatio": 0.1373}),
    Case(name="audit2264.ratios.dividend_yield_is_a_decimal_ratio", ids=["F53"],
         py=_py_with_info(lambda: server.get_financial_ratios("ZDR"), _INFO),
         ts={"js": 'return await m.yahooFinance.getFinancialRatios("ZDR");', "isolate": True},
         routes=_yahoo_summary("ZDR", 0.0031), select=_dividend_view,
         expect={"dividendYield": 0.0031, "payoutRatio": 0.1373}),
]
# The scale check on its own (Python only; the Worker reads summaryDetail, which is always a ratio).
for _key, _info, _expect in [
    ("quote_percent_is_divided", _INFO, 0.0031),
    ("ratio_kept_when_the_quote_call_failed", {**_INFO, "dividendYield": 0.0031}, 0.0031),
    ("percent_assumed_with_no_rate_or_price", {"dividendYield": 2.29}, 0.0229),
    ("trailing_yield_decides_without_a_price", {"dividendYield": 0.0229, "trailingAnnualDividendYield": 0.0225}, 0.0229),
    ("no_yield_stays_none", {"dividendYield": None}, None),
]:
    CASES.append(Case(name=f"audit2264.dividend_yield_ratio.{_key}", ids=["F53"],
                      py=(lambda i=_info: server._dividend_yield_ratio(i)), ts={"js": "return args[0];", "args": [_expect]},
                      expect=_expect, note="Python-only normalisation; the Worker side states the expected value."))


# ── F22: the comparison's Yahoo rows ───────────────────────────────────────────────────────────────────────────
def _yahoo_rows_view(v):
    d = _unwrap(v)
    actual, est = d.get("actual") or {}, d.get("estimate") or {}
    return {"actualEpsConfidence": (actual.get("eps") or {}).get("confidence"),
            "estimateEpsConfidence": (est.get("eps") or {}).get("confidence"),
            "confidence": d.get("confidence")}


CASES.append(dataclasses.replace(
    _compare_case("audit2264.compare.yahoo_actual_eps_is_low_while_comparison_confidence_is_high", ["F22", "F16"], "ZCM", 820013,
                  _REL_Q2, _ROW(0.55, 0.50), _REVEST, None),
    select=_yahoo_rows_view,
    expect={"actualEpsConfidence": "LOW", "estimateEpsConfidence": "LOW", "confidence": "HIGH"}))
