"""Parity cases for the 3.0.0 major release (the 2.8.0 and 2.27.0 deprecations removed).

Both runtimes must return the same payloads after the removals, so every case runs the tool in Python and in the Worker:

- F17: get_ownership_holders returns the Worker's quoteSummary module, unwrapped (Python used to return yfinance frames).
- F47: get_company_events_calendar (upcoming) returns the Worker's ``calendar`` shape.
- calculate_price_target_distance has no bracket, inferredTag, tag or tagNote.
- check_volume_liquidity_threshold has no gatePass or gateThresholdUsd; its note is "20d average traded value ..." with an
  optional "(>= | < min_adv_usd ...)" clause.
- analyze_position_signals has no t1/t2/t4/t5_inputs; analystInputs takes the radar's plain counts.
- analyze_options_flow_window has no ivPctile, putVolVs10dAvg or fieldNotes.
- get_analyst_rating_changes (the radar) has no upgrades30d, downgrades30d or initiations30d.
- The Worker's meta for the three status-table tools has no doctrineUse or decisionGrade (Python's meta carries
  reliabilityClass only: pinned).

Python reads Yahoo's quoteSummary through yfinance's own fetcher (``Ticker._data.get_raw_json``), which these cases replace
with a fake that checks the URL and parameters; the Worker is served the same JSON by route.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pandas as pd

from scripts.parity.cases_neutrality import (
    FACT, POSITION_ROUTES, PRESS, SECTION, SECTION_ROUTES, _position_case, _worker,
)
from scripts.parity.cases_pricing_evidence_tools import (
    GATE_NOW, QUOTE_TIME, YAHOO_AUTH_ROUTES, _bracket_case, _gate_case, _ts_with_clock,
)
from scripts.parity.harness import Case

import server  # noqa: E402
from scripts.parity.cases_accession import _python_sec_state  # noqa: E402
from yfmcp.envelope import _envelope_tool_result  # noqa: E402
from yfmcp.news.providers import yahoo as yahoo_provider  # noqa: E402

CASES: list[Case] = []

QS = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/"


def _parse(value):
    """A tool's JSON text parsed; a plain-text result (such as an error line) as is."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


# ═══════════════════════════════════════════════════════════════════════════════════════
# F17 / F47: quoteSummary modules through yfinance's fetcher (Python) and Yahoo's JSON (Worker)
# ═══════════════════════════════════════════════════════════════════════════════════════

def _summary(module: str, value) -> dict:
    return {"quoteSummary": {"result": [{module: value}], "error": None}}


NO_RESULT = {"quoteSummary": {"result": None, "error": {"code": "Not Found", "description": "Quote not found"}}}


def _fake_yf(ticker: str, module: str, payload: dict):
    """yfinance.Ticker stand-in whose crumb-aware fetcher serves `payload` and checks what the tool asked for."""
    def get_raw_json(url, params=None, timeout=30):
        assert url.endswith("/v10/finance/quoteSummary/" + ticker), url
        assert params == {"modules": module, "corsDomain": "finance.yahoo.com", "formatted": "true"}, params
        return payload

    company = SimpleNamespace(fast_info=SimpleNamespace(currency="USD"), _data=SimpleNamespace(get_raw_json=get_raw_json))
    return patch.object(server.yf, "Ticker", lambda _t: company)


def _qs_case(name: str, ids: list[str], ticker: str, module: str, payload: dict, py_call, ts_call: str, **kw) -> Case:
    def py():
        with _fake_yf(ticker, module, payload):
            return asyncio.run(py_call())

    return Case(
        name=name, ids=ids, py=py, ts={"js": f"return {ts_call};", "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, f"{QS}{ticker}?*": {"json": payload}}, select=_parse, **kw)


def _holders_case(label: str, holder_type: str, module: str, value, *, ticker: str, payload: dict | None = None, expect=None) -> Case:
    return _qs_case(
        f"major300.f17.{label}", ["F17"], ticker, module, payload if payload is not None else _summary(module, value),
        lambda: server.get_ownership_holders(ticker, holder_type),
        f'await m.yahooFinance.getHolderInfo("{ticker}", "{holder_type}")', expect=expect)


def _wrapped(raw, fmt, long_fmt=None) -> dict:
    return {"raw": raw, "fmt": fmt, **({"longFmt": long_fmt} if long_fmt else {})}


_REPORT = _wrapped(1782777600, "2026-06-30")

CASES += [
    _holders_case("institutional_rows_unwrap_numbers_and_dates", "institutional_holders", "institutionOwnership", {
        "maxAge": 1, "ownershipList": [
            {"maxAge": 1, "reportDate": _REPORT, "organization": "Fund One", "pctHeld": _wrapped(0.08, "8%"),
             "position": _wrapped(1_000_000, "1M", "1,000,000"), "value": _wrapped(120_000_000, "120M", "120,000,000"),
             "pctChange": _wrapped(0.01, "1%")},
            {"maxAge": 1, "reportDate": _REPORT, "organization": "Fund Two", "pctHeld": _wrapped(0.05, "5%"),
             "position": _wrapped(600_000, "600k"), "value": {}, "pctChange": _wrapped(-0.02, "-2%")}]},
        ticker="MJRHLD1", expect={"ownershipList": [
            {"reportDate": "2026-06-30", "organization": "Fund One", "pctHeld": 0.08, "position": 1000000, "value": 120000000, "pctChange": 0.01},
            {"reportDate": "2026-06-30", "organization": "Fund Two", "pctHeld": 0.05, "position": 600000, "value": None, "pctChange": -0.02}]}),
    _holders_case("major_holders_breakdown", "major_holders", "majorHoldersBreakdown", {
        "maxAge": 1, "insidersPercentHeld": _wrapped(0.0215, "2.15%"), "institutionsPercentHeld": _wrapped(0.61, "61%"),
        "institutionsCount": _wrapped(5123, "5.12k", "5,123")}, ticker="MJRHLD2",
        expect={"insidersPercentHeld": 0.0215, "institutionsPercentHeld": 0.61, "institutionsCount": 5123}),
    _holders_case("insider_transactions_with_a_non_date_fmt_keeps_the_raw", "insider_transactions", "insiderTransactions", {
        "maxAge": 1, "transactions": [{
            "maxAge": 1, "shares": _wrapped(100, "100", "100"), "filerUrl": "", "transactionText": "Sale at price 10.00 per share.",
            "filerName": "A PERSON", "filerRelation": "CEO", "moneyText": "", "startDate": _REPORT, "ownership": "D",
            "value": _wrapped(1000, "1k"), "odd": {"raw": 5, "fmt": "Jun 30, 2026"}, "extra": {"raw": 1, "other": 2}}]},
        ticker="MJRHLD3", expect={"transactions": [{
            "shares": 100, "filerUrl": "", "transactionText": "Sale at price 10.00 per share.", "filerName": "A PERSON",
            "filerRelation": "CEO", "moneyText": "", "startDate": "2026-06-30", "ownership": "D", "value": 1000, "odd": 5,
            "extra": {"raw": 1, "other": 2}}]}),
    _holders_case("insider_purchases_net_activity", "insider_purchases", "netSharePurchaseActivity", {
        "maxAge": 1, "period": "6m", "buyInfoShares": _wrapped(10, "10"), "sellInfoShares": {}, "netInfoCount": _wrapped(0, "-")},
        ticker="MJRHLD4", expect={"period": "6m", "buyInfoShares": 10, "sellInfoShares": None, "netInfoCount": 0}),
    _holders_case("insider_roster_holders", "insider_roster_holders", "insiderHolders", {
        "maxAge": 1, "holders": [{"maxAge": 1, "name": "A PERSON", "relation": "Director", "url": "",
                                   "transactionDescription": "Sale", "latestTransDate": _REPORT, "positionDirect": _wrapped(5, "5")}]},
        ticker="MJRHLD5", expect={"holders": [{"name": "A PERSON", "relation": "Director", "url": "", "transactionDescription": "Sale",
                                               "latestTransDate": "2026-06-30", "positionDirect": 5}]}),
    _holders_case("mutualfund_holders", "mutualfund_holders", "fundOwnership", {"maxAge": 1, "ownershipList": []},
                  ticker="MJRHLD6", expect={"ownershipList": []}),
    _holders_case("an_empty_module_is_null", "major_holders", "majorHoldersBreakdown", {}, ticker="MJRHLD7", expect=None),
    _holders_case("no_quote_summary_result_is_the_no_data_line", "major_holders", "majorHoldersBreakdown", None, ticker="MJRHLD8",
                  payload=NO_RESULT, expect="Error: no data found for ticker MJRHLD8"),
]

_CAL_FULL = {
    "maxAge": 1,
    "earnings": {
        "earningsDate": [_wrapped(1793920800, "2026-11-05"), _wrapped(1794266400, "2026-11-09")],
        "earningsAverage": _wrapped(1.94, "1.94"), "earningsLow": _wrapped(1.8, "1.8"), "earningsHigh": _wrapped(2.1, "2.1"),
        "revenueAverage": _wrapped(95_000_000_000, "95B", "95,000,000,000"), "revenueLow": _wrapped(91_000_000_000, "91B"),
        "revenueHigh": {}, "isEarningsDateEstimate": True},
    "exDividendDate": _wrapped(1793750400, "2026-11-04"), "dividendDate": _wrapped(1794355200, "2026-11-11"),
}


def _calendar_case(label: str, ticker: str, payload: dict, expect) -> Case:
    return _qs_case(
        f"major300.f47.{label}", ["F47"], ticker, "calendarEvents", payload,
        lambda: server.get_calendar(ticker, "upcoming"),
        f'await m.yahooFinance.getCalendar("{ticker}", "upcoming")', expect=expect)


_CAL_COMMON = {"mode": "upcoming", "earningsDateConfirmed": False, "confirmationStatus": "UNVERIFIED", "source": "yahoo_finance",
               "sourceType": "provider_calendar", "evidenceClass": "CONTEXTUAL_MARKET_DATA", "decisionGrade": False}

CASES += [
    _calendar_case("upcoming_calendar_has_the_worker_shape", "MJRCAL1", _summary("calendarEvents", _CAL_FULL), {
        "ticker": "MJRCAL1", **_CAL_COMMON, "earningsDateSource": "YAHOO_CALENDAR", "recommendedNextAction": "CHECK_OFFICIAL_RELEASES",
        "calendar": {
            "earnings": {"earningsDate": ["2026-11-05T23:20:00.000Z", "2026-11-09T23:20:00.000Z"], "earningsAverage": 1.94,
                         "earningsLow": 1.8, "earningsHigh": 2.1, "revenueAverage": 95000000000, "revenueLow": 91000000000,
                         "revenueHigh": None},
            "exDividendDate": "2026-11-04T00:00:00.000Z", "dividendDate": "2026-11-11T00:00:00.000Z"}}),
    _calendar_case("empty_dates_are_unknown_with_null_dividend_dates", "MJRCAL2", _summary("calendarEvents", {
        "maxAge": 1, "earnings": {"earningsDate": []}, "exDividendDate": {}, "dividendDate": {}}), {
        "ticker": "MJRCAL2", **_CAL_COMMON, "earningsDateSource": "UNKNOWN", "recommendedNextAction": "NONE",
        "calendar": {"earnings": {"earningsDate": [], "earningsAverage": None, "earningsLow": None, "earningsHigh": None,
                                  "revenueAverage": None, "revenueLow": None, "revenueHigh": None},
                     "exDividendDate": None, "dividendDate": None}}),
    _calendar_case("a_null_earnings_date_entry_stays_null", "MJRCAL3", _summary("calendarEvents", {
        "earnings": {"earningsDate": [None, _wrapped(1793920800, "2026-11-05")]}}), {
        "ticker": "MJRCAL3", **_CAL_COMMON, "earningsDateSource": "YAHOO_CALENDAR", "recommendedNextAction": "CHECK_OFFICIAL_RELEASES",
        "calendar": {"earnings": {"earningsDate": [None, "2026-11-05T23:20:00.000Z"], "earningsAverage": None, "earningsLow": None,
                                  "earningsHigh": None, "revenueAverage": None, "revenueLow": None, "revenueHigh": None},
                     "exDividendDate": None, "dividendDate": None}}),
    _calendar_case("a_result_without_calendar_events_is_the_unknown_shape", "MJRCAL4", _summary("price", {"maxAge": 1}), {
        "ticker": "MJRCAL4", **_CAL_COMMON, "earningsDateSource": "UNKNOWN", "recommendedNextAction": "NONE",
        "calendar": {"earnings": {"earningsDate": [], "earningsAverage": None, "earningsLow": None, "earningsHigh": None,
                                  "revenueAverage": None, "revenueLow": None, "revenueHigh": None},
                     "exDividendDate": None, "dividendDate": None}}),
    _calendar_case("no_quote_summary_result_is_the_no_data_line", "MJRCAL5", NO_RESULT, "Error: no data found for ticker MJRCAL5"),
]


# ═══════════════════════════════════════════════════════════════════════════════════════
# calculate_price_target_distance: no strategy labels
# ═══════════════════════════════════════════════════════════════════════════════════════

PRICE_TARGET_KEYS = sorted([
    "ticker", "currentPrice", "priceBasis", "observationType", "priceTimestamp", "marketState", "marketOpen", "referenceTargetPrice",
    "referenceTargetPct", "currentToTargetRatioPct", "distanceToTargetPct", "distanceConvention", "ioPt", "eqfPct", "invertedFlag",
    "priceAtOrAboveTarget", "dataDate", "recommendedNextAction"])

CASES += [
    _bracket_case("major300.price_target.key_set_has_no_bracket_inferred_tag_tag_or_tag_note", ["BR-PRC-022"], 70.0, 100.0,
                  market_time=QUOTE_TIME, select=lambda v: sorted(v), expect=PRICE_TARGET_KEYS),
    _bracket_case("major300.price_target.envelope_key_set_and_no_warnings", ["BR-PRC-022"], 120.0, 100.0,
                  market_time=QUOTE_TIME, via="envelope",
                  select=lambda v: {"keys": sorted(v["data"]), "warnings": v["meta"]["warnings"]},
                  expect={"keys": PRICE_TARGET_KEYS, "warnings": []}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════
# check_volume_liquidity_threshold: no fixed gate, and the new note
# Fixture listings (cases_pricing_evidence_tools): EURX.PA trades $200,000,000 a day, USDX $500,000; NORATE.L has no FX rate.
# ═══════════════════════════════════════════════════════════════════════════════════════

EURX = dict(symbol="EURX.PA", currency="EUR", close=100.0, volume=1_000_000, fx=("EUR=X", 0.5))
USDX = dict(symbol="USDX", currency="USD", close=5.0, volume=100_000, fx=None)
NORATE = dict(symbol="NORATE.L", currency="GBp", close=250.0, volume=2_000_000, fx=("GBP=X", None))
_EUR_FX = " [EUR→USD at 0.5000 EUR/USD]"


def _gate_view(v: dict) -> dict:
    return {"goneKeys": sorted(k for k in ("gatePass", "gateThresholdUsd") if k in v), "note": v.get("note"),
            "meetsMinimum": v.get("meetsMinimum"), "minAdvUsd": v.get("minAdvUsd")}


def _gate(name: str, base: dict, expect_view: dict, **kw) -> Case:
    return _gate_case(name, ["BR-PRC-003"], **base, select=_gate_view, expect={"goneKeys": [], **expect_view}, **kw)


CASES += [
    _gate("major300.volume_gate.note_without_min_adv_usd_has_no_threshold_clause", EURX,
          {"note": "20d average traded value $200.0M; latest session 1.00x 20d ADV" + _EUR_FX, "meetsMinimum": None, "minAdvUsd": None}),
    _gate("major300.volume_gate.note_names_a_met_minimum", EURX,
          {"note": "20d average traded value $200.0M (≥ min_adv_usd $150.0M); latest session 1.00x 20d ADV" + _EUR_FX,
           "meetsMinimum": True, "minAdvUsd": 150000000}, min_adv=150_000_000),
    _gate("major300.volume_gate.note_names_an_unmet_minimum", EURX,
          {"note": "20d average traded value $200.0M (< min_adv_usd $250.0M); latest session 1.00x 20d ADV" + _EUR_FX,
           "meetsMinimum": False, "minAdvUsd": 250000000}, min_adv=250_000_000),
    _gate("major300.volume_gate.note_formats_a_billion_minimum", USDX,
          {"note": "20d average traded value $0.5M (< min_adv_usd $1.50B); latest session 1.00x 20d ADV",
           "meetsMinimum": False, "minAdvUsd": 1500000000}, min_adv=1_500_000_000),
    _gate("major300.volume_gate.note_with_a_minimum_equal_to_the_average", USDX,
          {"note": "20d average traded value $0.5M (≥ min_adv_usd $0.5M); latest session 1.00x 20d ADV",
           "meetsMinimum": True, "minAdvUsd": 500000}, min_adv=500_000),
    _gate("major300.volume_gate.unknown_note_is_unchanged_with_a_minimum", NORATE,
          {"note": "Volume gate UNKNOWN — GBP=X rate unavailable", "meetsMinimum": None, "minAdvUsd": 1000000}, min_adv=1_000_000),
    _gate("major300.volume_gate.unknown_note_is_unchanged_without_a_minimum", NORATE,
          {"note": "Volume gate UNKNOWN — GBP=X rate unavailable", "meetsMinimum": None, "minAdvUsd": None}),
    _gate_case("major300.volume_gate.snapshot_volume_block_has_no_liquidity_gate_pass", ["BR-PRC-003"], **EURX, min_adv=150_000_000,
               via="snapshot", select=lambda v: "liquidityGatePass" in v["volume"], expect=False),
]


# ═══════════════════════════════════════════════════════════════════════════════════════
# analyze_position_signals: no tier-numbered groups, analystInputs counts from the radar's plain fields
# ═══════════════════════════════════════════════════════════════════════════════════════

CASES += [
    _position_case("major300.position.no_tier_numbered_groups", lambda v: sorted(k for k in v["data"] if k.endswith("_inputs")),
                   ids=("Q14-N3",), expect=[]),
]


# One upgrade a day before the Worker's fixed clock: the Worker's radar counts it, and the Python radar stub says the same.
_POS_HISTORY_SUMMARY = json.loads(json.dumps(POSITION_ROUTES["https://query1.finance.yahoo.com/v10/finance/quoteSummary/*"]["json"]))
_POS_HISTORY_SUMMARY["quoteSummary"]["result"][0]["upgradeDowngradeHistory"] = {"history": [{
    "epochGradeDate": GATE_NOW - 86_400, "firm": "Firm A", "toGrade": "Buy", "fromGrade": "Hold", "action": "up",
    "currentPriceTarget": 120.0, "priorPriceTarget": 100.0, "priceTargetAction": ""}]}
_counts_case = _position_case(
    "major300.position.analyst_inputs_take_the_radars_plain_counts",
    lambda v: {k: v["data"]["analystInputs"][k] for k in ("upgrades30d", "downgrades30d", "initiations30d")},
    components={"get_analyst_upgrade_radar": {"netSentiment": 1, "upgrades": 1, "downgrades": 0, "initiations": 0}},
    ids=("Q14-N3",), expect={"upgrades30d": 1, "downgrades30d": 0, "initiations30d": 0})
_counts_case.routes = {**POSITION_ROUTES, "https://query1.finance.yahoo.com/v10/finance/quoteSummary/*": {"json": _POS_HISTORY_SUMMARY}}
CASES.append(_counts_case)


# ═══════════════════════════════════════════════════════════════════════════════════════
# get_analyst_rating_changes (get_analyst_upgrade_radar): no *30d copies, whatever the window
# ═══════════════════════════════════════════════════════════════════════════════════════

_RECENT = int(time.time()) - 86_400


def _radar_case(name: str, days_back: int, **kw) -> Case:
    row = {"Firm": "Firm A", "ToGrade": "Buy", "FromGrade": "Hold", "Action": "up",
           "currentPriceTarget": 120.0, "priorPriceTarget": 100.0, "priceTargetAction": ""}

    class _Ticker:
        upgrades_downgrades = pd.DataFrame([row], index=pd.DatetimeIndex([pd.Timestamp.now().normalize()], name="GradeDate"))

    def py():
        with patch.object(server.yf, "Ticker", lambda _t: _Ticker()):
            return asyncio.run(server.get_analyst_upgrade_radar("MJRRDR", days_back=days_back))

    history = {"quoteSummary": {"result": [{"upgradeDowngradeHistory": {"history": [{
        "epochGradeDate": _RECENT, "firm": "Firm A", "toGrade": "Buy", "fromGrade": "Hold", "action": "up",
        "currentPriceTarget": 120.0, "priorPriceTarget": 100.0, "priceTargetAction": ""}]}}]}}
    return Case(
        name=name, ids=["BR-SHR-003"], py=py,
        ts={"js": f'return JSON.parse(await m.yahooFinance.getAnalystUpgradeRadar("MJRRDR", {days_back}));', "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, f"{QS}MJRRDR?*": {"json": history}}, **kw)


def _radar_view(v: dict) -> dict:
    return {"goneKeys": sorted(k for k in ("upgrades30d", "downgrades30d", "initiations30d") if k in v), "windowDays": v["windowDays"],
            "counts": [v["upgrades"], v["downgrades"], v["initiations"]]}


CASES += [
    _radar_case("major300.rating_changes.30_day_window_has_no_30d_copies", 30, select=_radar_view,
                expect={"goneKeys": [], "windowDays": 30, "counts": [1, 0, 0]}),
    _radar_case("major300.rating_changes.90_day_window_has_no_30d_copies", 90, select=_radar_view,
                expect={"goneKeys": [], "windowDays": 90, "counts": [1, 0, 0]}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════
# analyze_options_flow_window: no ivPctile / putVolVs10dAvg aliases and no fieldNotes
# A far-future expiry is the next expiry whatever today is; the daily history is unavailable in both runtimes (no chart route in
# the Worker, a raising loader in Python), so the scan reports IV_PERCENTILE_UNAVAILABLE_DAILY_BARS.
# ═══════════════════════════════════════════════════════════════════════════════════════

_FLOW_EXPIRY = "2099-01-15"
_FLOW_EXPIRY_TS = int(pd.Timestamp(_FLOW_EXPIRY, tz="UTC").timestamp())


def _contract(strike: float, kind: str) -> dict:
    return {"contractSymbol": f"FLOW{kind}{int(strike)}", "strike": strike, "lastPrice": 2.0, "bid": 1.9, "ask": 2.1,
            "volume": 300 if kind == "P" else 500, "openInterest": 1000, "impliedVolatility": 0.4,
            "lastTradeDate": 1_790_000_000, "inTheMoney": False, "currency": "USD", "expiration": _FLOW_EXPIRY_TS}


def _flow_case(name: str, select, **kw) -> Case:
    strikes = [95.0, 100.0, 105.0]
    calls = [_contract(s, "C") for s in strikes]
    puts = [_contract(s, "P") for s in strikes]

    class _Ticker:
        fast_info = {"lastPrice": 100.0, "tenDayAverageVolume": 1_000_000}
        options = (_FLOW_EXPIRY,)

        def option_chain(self, expiry):
            return SimpleNamespace(calls=pd.DataFrame(calls), puts=pd.DataFrame(puts))

    def py():
        with patch.object(server.yf, "Ticker", lambda _t: _Ticker()), \
                patch.object(server, "_load_completed_daily_history", AsyncMock(side_effect=RuntimeError("no history"))), \
                patch.object(server, "get_last_trading_date", lambda: "2026-10-08"):
            return json.loads(asyncio.run(server.get_options_flow_scan("MJRFLOW", "audit")))

    chain = {"optionChain": {"result": [{"expirationDates": [_FLOW_EXPIRY_TS], "options": [{"expirationDate": _FLOW_EXPIRY_TS,
                                                                                           "calls": calls, "puts": puts}]}]}}
    summary = {"quoteSummary": {"result": [{"price": {"regularMarketPrice": {"raw": 100.0}}}]}}
    return Case(
        name=name, ids=["BR-OPT-001"], py=py,
        ts={"js": _ts_with_clock(GATE_NOW, 'return JSON.parse(await m.yahooFinance.getOptionsFlowScan("MJRFLOW", "audit"));'),
            "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, "https://query2.finance.yahoo.com/v7/finance/options/MJRFLOW?*": {"json": chain},
                f"{QS}MJRFLOW?*": {"json": summary}},
        select=select, **kw)


CASES += [
    _flow_case("major300.options_flow.no_aliases_and_no_field_notes",
               lambda v: {"goneKeys": sorted(k for k in ("ivPctile", "putVolVs10dAvg", "fieldNotes") if k in v),
                          "named": sorted(k for k in ("ivVsRealizedRangePct", "putVolPer1PctStockAdv") if k in v)},
               expect={"goneKeys": [], "named": ["ivVsRealizedRangePct", "putVolPer1PctStockAdv"]}),
    _flow_case("major300.options_flow.result_key_set", lambda v: sorted(v), expect=sorted([
        "ticker", "windowLabel", "dataDate", "pcRatio", "ivVsRealizedRangePct", "putVolPer1PctStockAdv", "putVolTrend", "expiry",
        "expirySelection", "maxPainStrike", "bracket", "formattedBlock", "realizedVolPriceBasis", "historicalObservationType",
        "historicalBarStatus", "freshnessStatus", "excludedIncompleteBar", "retryAttempted", "fallbackUsed",
        "recommendedNextAction", "dataQuality", "warnings"])),
]


# ═══════════════════════════════════════════════════════════════════════════════════════
# Worker meta for the three status-table tools: reliabilityClass first after capabilityStatus, no doctrineUse or decisionGrade
# Python's meta carries reliabilityClass only (it never emitted the rest of the status table).
# ═══════════════════════════════════════════════════════════════════════════════════════

_STATUS_KEYS = ["capabilityStatus", "reliabilityClass", "failureMode", "evidenceRequired", "sourceType", "successCriteria",
                "limitationStatuses", "note"]
_FULL_STATUS_KEYS = list(_STATUS_KEYS)
_META_DECISION = "FINDING 3.0.0: Python meta carries reliabilityClass only from the Worker's status table (it never emitted capabilityStatus, failureMode, evidenceRequired or sourceType)"


def _meta_order_case(name: str, tool, routes: dict | None, expect_ts_keys: list[str]) -> Case:
    """The envelope meta's own keys, in order, filtered to the status table's and to the retired doctrineUse / decisionGrade.
    (The harness sorts dict keys before `select` runs, so each runtime returns the key list itself.)"""
    name_, args, py_call = tool
    watched = [*_STATUS_KEYS, "doctrineUse", "decisionGrade"]

    def py():
        async def call():
            return _envelope_tool_result(name_, await py_call())
        with patch.object(server.yf, "Ticker", side_effect=RuntimeError("offline")), \
                patch.object(yahoo_provider, "_yahoo_search_news", return_value=[]), \
                (_python_sec_state() if routes else contextlib.nullcontext()):
            out = asyncio.run(call())
        return [k for k in json.loads(out)["meta"] if k in watched]

    body = (f'const r = await m.tools.callToolResult({json.dumps(name_)}, {json.dumps(args)}); '
            f'return Object.keys(r.value.meta).filter((k) => {json.dumps(watched)}.includes(k));')
    return Case(name=name, ids=["Q14-N1"], py=py, ts={"js": _worker(body), "isolate": True}, routes=routes or {},
                mode="pinned", decision=_META_DECISION,
                expect_py=["reliabilityClass"], expect_ts=expect_ts_keys)


CASES += [
    _meta_order_case("major300.meta.press_releases_status_table_order_without_doctrine_use_or_decision_grade", PRESS, None,
                     _FULL_STATUS_KEYS),
    _meta_order_case("major300.meta.extract_sec_filing_fact_status_table_order_without_doctrine_use_or_decision_grade", FACT, None,
                     _FULL_STATUS_KEYS),
    _meta_order_case("major300.meta.section_markdown_status_table_order_without_doctrine_use_or_decision_grade", SECTION, SECTION_ROUTES,
                     ["capabilityStatus", "reliabilityClass", "failureMode", "evidenceRequired", "sourceType"]),
]
