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


# ═══════════════════════════════════════════════════════════════════════════════════════
# F20: confidence holds only HIGH, MEDIUM or LOW, or null; a status string moves to status
# Both runtimes apply the rule at the tool boundary (Python `_envelope_tool_result`, Worker `mcpSuccess`) after enrichFacts:
# a fact with no confidence and no value is null (was NOT_DECISION_GRADE); a fact's own non-level string moves to its status
# when it has none; a fact passes its own resolved confidence to its children, a non-fact passes `confidence || inherited`.
# ═══════════════════════════════════════════════════════════════════════════════════════

import os  # noqa: E402

from scripts.parity.cases_neutrality import _worker as _worker_env  # noqa: E402,F401


def _confidence_case(name: str, payload, expect, *, tool: str = "extract_sec_filing_fact", select=None, **kw) -> Case:
    text = json.dumps(payload)
    return Case(
        name=name, ids=["F20"],
        py=lambda: json.loads(_envelope_tool_result(tool, text)),
        ts={"js": _worker("return JSON.parse(m.response.mcpSuccess(args[0], args[1]));"), "args": [tool, text], "isolate": True},
        select=select or (lambda v: v["data"]), expect=expect, **kw)


def _conf_status(node: dict) -> list:
    return [node.get("confidence"), node.get("status", "<no status>")]


CASES += [
    # A fact whose confidence is a status string and that has no status: confidence null, the string becomes status.
    _confidence_case(
        "major300.f20.fact_with_not_disclosed_and_no_status_gets_null_and_status",
        {"revenue": {"value": None, "confidence": "NOT_DISCLOSED"}},
        {"revenue": [None, "NOT_DISCLOSED"]},
        select=lambda v: {"revenue": _conf_status(v["data"]["revenue"])}),
    # A status the fact already has is kept; the confidence string is dropped.
    _confidence_case(
        "major300.f20.fact_with_an_existing_status_keeps_it",
        {"revenue": {"value": None, "status": "NO_DATA", "confidence": "NOT_DISCLOSED"},
         "eps": {"value": 3, "status": None, "confidence": "EXTRACTION_FAILED"}},
        {"revenue": [None, "NO_DATA"], "eps": [None, None]},
        select=lambda v: {k: _conf_status(v["data"][k]) for k in ("revenue", "eps")}),
    # The string is moved as written (case kept), while the fact check upper-cases it.
    _confidence_case(
        "major300.f20.status_keeps_the_strings_own_case",
        {"revenue": {"value": 5, "confidence": "not_disclosed"}},
        {"revenue": [None, "not_disclosed"]},
        select=lambda v: {"revenue": _conf_status(v["data"]["revenue"])}),
    # Lower-case levels are levels after the fact check upper-cases them.
    _confidence_case(
        "major300.f20.fact_levels_are_upper_cased",
        {"revenue": {"value": 5, "confidence": "high"}},
        {"revenue": ["HIGH", "<no status>"]},
        select=lambda v: {"revenue": _conf_status(v["data"]["revenue"])}),
    # A fact with no value and no confidence has none to state; a fact with a value is LOW.
    _confidence_case(
        "major300.f20.fact_without_value_or_confidence_is_null_and_with_a_value_is_low",
        {"empty": {"value": None}, "ratio": {"valueRatio": 0.5}, "range": {"low": 1, "high": 2}, "highOnly": {"high": 2}},
        {"empty": [None, "<no status>"], "ratio": ["LOW", "<no status>"], "range": ["LOW", "<no status>"], "highOnly": [None, "<no status>"]},
        select=lambda v: {k: _conf_status(v["data"][k]) for k in ("empty", "ratio", "range", "highOnly")}),
    # A non-fact object is not enriched; the walk after enrichment moves its status string.
    _confidence_case(
        "major300.f20.non_fact_object_gets_null_and_status",
        {"quality": {"label": "x", "confidence": "EXTRACTION_FAILED"}, "other": {"label": "y", "status": "OK", "confidence": "PARTIAL"},
         "levels": {"confidence": "MEDIUM"}, "blank": {"confidence": ""}},
        {"quality": [None, "EXTRACTION_FAILED"], "other": [None, "OK"], "levels": ["MEDIUM", "<no status>"], "blank": [None, "<no status>"]},
        select=lambda v: {k: _conf_status(v["data"][k]) for k in ("quality", "other", "levels", "blank")}),
]

# Nested inheritance. A fact passes its OWN resolved confidence (null included) to its children; a non-fact passes
# `own || inherited`, so a non-level string on a non-fact parent is inherited by its fact children, which then drop it
# (only a fact's own string moves to its status).
_NESTED = {
    "factParent": {"value": 5, "confidence": "NOT_DISCLOSED",
                   "withValue": {"value": 1}, "withoutValue": {"value": None}, "tagged": {"value": 2, "confidence": "HIGH"}},
    "levelParent": {"value": 5, "confidence": "MEDIUM", "child": {"value": 1}, "empty": {"value": None}},
    "plainParent": {"confidence": "MEDIUM", "child": {"value": 1}, "empty": {"value": None}},
    "stringParent": {"confidence": "PARTIAL", "child": {"value": 1}, "empty": {"value": None}},
    "nullFactParent": {"value": None, "confidence": "NOT_DISCLOSED", "child": {"value": 1}, "empty": {"value": None}},
}


def _nested_view(v: dict) -> dict:
    d = v["data"]
    out = {}
    for parent, node in d.items():
        out[parent] = {"self": _conf_status(node),
                       **{k: _conf_status(c) for k, c in node.items() if isinstance(c, dict)}}
    return out


CASES.append(_confidence_case(
    "major300.f20.nested_inheritance_a_fact_passes_its_own_resolved_confidence", _NESTED,
    {"factParent": {"self": [None, "NOT_DISCLOSED"], "withValue": ["LOW", "<no status>"], "withoutValue": [None, "<no status>"],
                    "tagged": ["HIGH", "<no status>"]},
     "levelParent": {"self": ["MEDIUM", "<no status>"], "child": ["MEDIUM", "<no status>"], "empty": ["MEDIUM", "<no status>"]},
     "plainParent": {"self": ["MEDIUM", "<no status>"], "child": ["MEDIUM", "<no status>"], "empty": ["MEDIUM", "<no status>"]},
     "stringParent": {"self": [None, "PARTIAL"], "child": [None, "<no status>"], "empty": [None, "<no status>"]},
     "nullFactParent": {"self": [None, "NOT_DISCLOSED"], "child": ["LOW", "<no status>"], "empty": [None, "<no status>"]}},
    select=_nested_view))

# A stored evidence cut is returned verbatim so its SHA-256 still verifies: get_evidence_cut skips the walk, any other
# tool applies it. (Evidence payloads with decisionUse EVIDENCE_ONLY are never enriched, but the walk still reaches them.)
_CUT = {"stored": {"confidence": "NOT_DISCLOSED", "inner": [{"confidence": "EXTRACTION_FAILED"}]}, "label": "cut"}
_CUT_VIEW = lambda v: {"stored": v["data"]["stored"], "label": v["data"]["label"]}  # noqa: E731 - Python adds decisionUse at the boundary
CASES += [
    _confidence_case("major300.f20.evidence_cut_is_returned_verbatim", _CUT, {"stored": _CUT["stored"], "label": "cut"},
                     tool="get_evidence_cut", select=_CUT_VIEW),
    _confidence_case(
        "major300.f20.other_evidence_tools_are_normalised", _CUT,
        {"stored": {"confidence": None, "status": "NOT_DISCLOSED", "inner": [{"confidence": None, "status": "EXTRACTION_FAILED"}]}, "label": "cut"},
        tool="get_evidence_quality", select=_CUT_VIEW),
    _confidence_case(
        "major300.f20.evidence_only_payload_is_not_enriched_but_is_normalised",
        {"decisionUse": "EVIDENCE_ONLY", "component": {"value": None, "confidence": "NOT_DISCLOSED"}},
        {"decisionUse": "EVIDENCE_ONLY", "component": {"value": None, "confidence": None, "status": "NOT_DISCLOSED"}},
        tool="extract_sec_filing_fact"),
    # The inner envelope a tool already wrapped (ok / data / meta) is walked as well.
    _confidence_case(
        "major300.f20.wrapped_inner_envelope_data_is_normalised",
        {"ok": True, "data": {"a": {"value": None, "confidence": "NOT_DISCLOSED"}, "b": {"confidence": "FAILED"}}, "meta": {"warnings": []}, "error": None},
        {"a": [None, "NOT_DISCLOSED"], "b": [None, "FAILED"]},
        select=lambda v: {k: _conf_status(v["data"][k]) for k in ("a", "b")}),
]

# The legacy envelope (MCP_ENVELOPE_V2 false) returns the payload text; it is rewritten only when the rule changes it.
_LEGACY_CHANGED = '{"a": {"value": null, "confidence": "NOT_DISCLOSED"}, "b": [{"confidence": "FAILED", "status": "X"}]}'
_LEGACY_LEVELS = '{ "a" : {"confidence":"HIGH"},  "b": [1, 2] }'
_LEGACY_NO_KEY = '{ "a" : "confidence",  "b": null }'
_LEGACY_NOT_JSON = 'Error: "confidence" is a plain line'


def _without_boundary(value):
    """Python's tool boundary adds decisionUse to evidence tools (the Worker adds it in callTool, not mcpSuccess)."""
    if isinstance(value, dict):
        return {k: v for k, v in value.items() if k != "decisionUse"}
    return value


def _legacy_case(name: str, raw: str, *, tool: str = "extract_sec_filing_fact", exact: bool = True) -> Case:
    def py():
        with patch.dict(os.environ, {"MCP_ENVELOPE_V2": "false"}):
            out = _envelope_tool_result(tool, raw)
        parsed = _without_boundary(_parse(out))
        # An evidence tool's text also gains decisionUse at Python's boundary, so its "unchanged" is compared as parsed JSON.
        return {"unchanged": out == raw if exact else parsed == _parse(raw), "parsed": parsed}

    js = ('m.response.setWorkerEnv({ MCP_ENVELOPE_V2: "false" }); const out = m.response.mcpSuccess(args[0], args[1]); '
          'let parsed; try { parsed = JSON.parse(out); } catch { parsed = out; } return { unchanged: out === args[1], parsed };')
    return Case(name=name, ids=["F20"], py=py, ts={"js": js, "args": [tool, raw], "isolate": True})


CASES += [
    _legacy_case("major300.f20.legacy_envelope_payload_text_is_rewritten_when_it_changes", _LEGACY_CHANGED),
    _legacy_case("major300.f20.legacy_envelope_levels_only_text_is_returned_as_given", _LEGACY_LEVELS),
    _legacy_case("major300.f20.legacy_envelope_text_without_a_confidence_key_is_returned_as_given", _LEGACY_NO_KEY),
    _legacy_case("major300.f20.legacy_envelope_non_json_text_is_returned_as_given", _LEGACY_NOT_JSON),
    _legacy_case("major300.f20.legacy_envelope_evidence_cut_is_verbatim", _LEGACY_CHANGED, tool="get_evidence_cut", exact=False),
]


# ═══════════════════════════════════════════════════════════════════════════════════════
# F59: get_latest_earnings_release returns the 8-K as url and the document that is read as contentUrl
# sources[0] = {sourceType: "sec_8k", url: <8-K primary document>, contentUrl: <EX-99.1 when found, else the 8-K>, filingDate,
# acceptedAt, accessionNumber, cikPadded, items, isInlineXBRL, confidence}; no primaryDocumentUrl, no errorCode, no generatedAt.
# ═══════════════════════════════════════════════════════════════════════════════════════

import datetime  # noqa: E402

from scripts.parity.cases_sec_earnings import _doc_url, _python_env, _run, _sec_routes, _srv  # noqa: E402

_EX99_INDEX = ("<html><body><table>"
               "<tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>"
               '<tr><td>1</td><td>8-K</td><td><a href="/Archives/edgar/data/{cik}/{nodash}/r8k.htm">r8k.htm</a></td><td>8-K</td><td>10</td></tr>'
               '<tr><td>2</td><td>Press release</td><td><a href="/Archives/edgar/data/{cik}/{nodash}/ex991.htm">ex991.htm</a></td><td>EX-99.1</td><td>10</td></tr>'
               "</table></body></html>")
_EIGHT_K_HTML = "<html><body><p>Item 2.02 Results of Operations. The press release is attached as Exhibit 99.1.</p></body></html>"
_EX99_HTML = "<html><body><p>Acme Reports Fourth Quarter Fiscal 2026 Results. Revenue was $1.0 billion.</p></body></html>"


def _seconds(stamp):
    """An ISO timestamp without its milliseconds: Python's _to_iso_utc writes 2026-07-14T21:05:00Z where the Worker's toIsoUtc writes
    2026-07-14T21:05:00.000Z (a general difference of the two helpers, open: see the 3.0.0 report); the instant is what is compared."""
    return stamp.replace(".000Z", "Z") if isinstance(stamp, str) else stamp


def _f59_view(v: dict) -> dict:
    d = v["data"] if isinstance(v, dict) and "meta" in v and isinstance(v.get("data"), dict) else v
    out = json.loads(json.dumps(d))
    out["reportedAt"] = _seconds(out.get("reportedAt"))
    for source in out.get("sources") or []:
        source.pop("retrievedAt", None)
        if "publishedAt" in source:
            source["publishedAt"] = _seconds(source["publishedAt"])
    return out


def _f59_case(name: str, tkr: str, cik: int, filings: list[dict], docs: dict, index: bool, expect, **kw) -> Case:
    nodash = filings[0]["accession"].replace("-", "") if filings else ""
    index_rows = _EX99_INDEX.format(cik=cik, nodash=nodash) if index else None
    routes = _sec_routes(tkr, cik, filings, docs, index_rows)
    return Case(name=name, ids=["F59"], py=_run(lambda: _srv().get_latest_earnings_release(tkr)),
                ts={"js": "m.response.setWorkerEnv({}); return m.yahooFinance.getLatestEarningsRelease(args[0]);", "args": [tkr], "isolate": True},
                routes=routes, select=_f59_view, expect=expect, **kw)


def _f59_expect(cik: int, accn: str, *, content: str, period: str, status: str, evidence, items: str = "2.02,9.01") -> dict:
    return {
        "ticker": None, "eventType": "earnings_release", "period": period, "periodStatus": status, "periodEvidence": evidence,
        "reportedAt": "2026-07-14T21:05:00Z",
        "sources": [{"sourceType": "sec_8k", "url": _doc_url(cik, accn, "r8k.htm"), "contentUrl": content, "filingDate": "2026-07-14",
                     "acceptedAt": "2026-07-14T21:05:00.000Z", "accessionNumber": accn, "cikPadded": f"{cik:010d}", "items": items,
                     "isInlineXBRL": False, "confidence": "HIGH"}],
        "confidence": "HIGH", "warnings": [],
    }


_ACC1, _ACC2, _ACC3 = "0000850001-26-000001", "0000850002-26-000001", "0000850003-26-000001"
_F59_WITH = _f59_expect(850001, _ACC1, content=_doc_url(850001, _ACC1, "ex991.htm"), period="FY2026 Q4", status="EX99_TEXT_RESOLVED",
                        evidence="Fourth Quarter Fiscal 2026")
_F59_WITH["ticker"] = "ZF59A"
_F59_WITHOUT = _f59_expect(850002, _ACC2, content=_doc_url(850002, _ACC2, "r8k.htm"), period="latest", status="UNRESOLVED", evidence=None)
_F59_WITHOUT["ticker"] = "ZF59B"

CASES += [
    _f59_case("major300.f59.ex99_found_is_contenturl_and_the_8k_stays_url", "ZF59A", 850001,
              [{"accession": _ACC1, "date": "2026-07-14", "primary": "r8k.htm"}],
              {_doc_url(850001, _ACC1, "r8k.htm"): _EIGHT_K_HTML, _doc_url(850001, _ACC1, "ex991.htm"): _EX99_HTML}, True, _F59_WITH),
    _f59_case("major300.f59.no_ex99_contenturl_is_the_8k", "ZF59B", 850002,
              [{"accession": _ACC2, "date": "2026-07-14", "primary": "r8k.htm"}],
              {_doc_url(850002, _ACC2, "r8k.htm"): _EIGHT_K_HTML}, False, _F59_WITHOUT),
]

# The 8-K reporting results (Item 2.02) is chosen over a newer 8-K for another item; with none, the newest 8-K.
_F59_ITEMS_ROWS = [{"accession": "0000850003-26-000009", "date": "2026-07-20", "primary": "other8k.htm", "items": "5.02,8.01"},
                   {"accession": _ACC3, "date": "2026-07-14", "primary": "r8k.htm", "items": "2.02,7.01,9.01"}]
_F59_ITEMS = _f59_expect(850003, _ACC3, content=_doc_url(850003, _ACC3, "ex991.htm"), period="FY2026 Q4", status="EX99_TEXT_RESOLVED",
                         evidence="Fourth Quarter Fiscal 2026", items="2.02,7.01,9.01")
_F59_ITEMS["ticker"] = "ZF59C"
_F59_NEWEST = _f59_expect(850004, "0000850004-26-000009", content=_doc_url(850004, "0000850004-26-000009", "other8k.htm"),
                          period="latest", status="UNRESOLVED", evidence=None, items="5.02,8.01")
_F59_NEWEST["ticker"] = "ZF59D"
_F59_NEWEST["sources"][0]["url"] = _doc_url(850004, "0000850004-26-000009", "other8k.htm")
_F59_NEWEST["sources"][0]["filingDate"] = "2026-07-20"
_F59_NEWEST["sources"][0]["acceptedAt"] = "2026-07-20T21:05:00.000Z"
_F59_NEWEST["reportedAt"] = "2026-07-20T21:05:00Z"
CASES += [
    _f59_case("major300.f59.item_202_8k_is_chosen_over_a_newer_8k_for_another_item", "ZF59C", 850003, _F59_ITEMS_ROWS,
              {_doc_url(850003, _ACC3, "r8k.htm"): _EIGHT_K_HTML, _doc_url(850003, _ACC3, "ex991.htm"): _EX99_HTML}, True, _F59_ITEMS),
    _f59_case("major300.f59.without_an_item_202_8k_the_newest_8k_is_the_source", "ZF59D", 850004,
              [{"accession": "0000850004-26-000009", "date": "2026-07-20", "primary": "other8k.htm", "items": "5.02,8.01"}],
              {_doc_url(850004, "0000850004-26-000009", "other8k.htm"): _EIGHT_K_HTML}, False, _F59_NEWEST),
]


def _f59_shape_case(name: str, tkr: str, cik: int, expect) -> Case:
    """Key order of the payload and of sources[0] (contentUrl right after url), and the envelope each runtime wraps it in."""
    accn = f"0000{cik:06d}-26-000001"
    routes = _sec_routes(tkr, cik, [{"accession": accn, "date": "2026-07-14", "primary": "r8k.htm"}],
                         {_doc_url(cik, accn, "r8k.htm"): _EIGHT_K_HTML, _doc_url(cik, accn, "ex991.htm"): _EX99_HTML},
                         _EX99_INDEX.format(cik=cik, nodash=accn.replace("-", "")))

    def py():
        raw = _run(lambda: _srv().get_latest_earnings_release(tkr))()
        payload = json.loads(raw)
        env = json.loads(_envelope_tool_result("get_latest_earnings_release", raw))
        return {"payloadKeys": list(payload), "sourceKeys": list(payload["sources"][0]), "envelopeKeys": sorted(env),
                "metaHasGeneratedAt": "generatedAt" in env["meta"], "dataKeys": list(env["data"]),
                "sourceKeysInEnvelope": list(env["data"]["sources"][0])}

    js = ('m.response.setWorkerEnv({}); const payload = JSON.parse(await m.yahooFinance.getLatestEarningsRelease(args[0])); '
          'const env = JSON.parse(m.response.mcpSuccess("get_latest_earnings_release", JSON.stringify(payload))); '
          'return { payloadKeys: Object.keys(payload), sourceKeys: Object.keys(payload.sources[0]), envelopeKeys: Object.keys(env).sort(), '
          'metaHasGeneratedAt: "generatedAt" in env.meta, dataKeys: Object.keys(env.data), sourceKeysInEnvelope: Object.keys(env.data.sources[0]) };')
    return Case(name=name, ids=["F59"], py=py, ts={"js": js, "args": [tkr], "isolate": True}, routes=routes, expect=expect)


_SOURCE_KEYS = ["sourceType", "url", "contentUrl", "filingDate", "acceptedAt", "accessionNumber", "cikPadded", "items", "isInlineXBRL", "confidence"]
_PAYLOAD_KEYS = ["ticker", "eventType", "period", "periodStatus", "periodEvidence", "reportedAt", "sources", "confidence", "warnings"]
CASES.append(_f59_shape_case(
    "major300.f59.key_order_and_no_error_code_or_generated_at", "ZF59E", 850005,
    {"payloadKeys": _PAYLOAD_KEYS, "sourceKeys": _SOURCE_KEYS, "envelopeKeys": ["data", "error", "meta", "ok"], "metaHasGeneratedAt": False,
     "dataKeys": _PAYLOAD_KEYS, "sourceKeysInEnvelope": _SOURCE_KEYS}))


def _f59_yahoo_case(name: str, tkr: str, cik: int, epochs: list[int], expect) -> Case:
    """No SEC 8-K: Yahoo's calendar date at or before now is the estimate source (no contentUrl); none is NOT_FOUND."""
    routes = _sec_routes(tkr, cik, [])
    routes[f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{tkr}?modules=calendarEvents*"] = {"json": {"quoteSummary": {"result": [
        {"calendarEvents": {"earnings": {"earningsDate": [{"raw": e} for e in epochs]}}}], "error": None}}}
    calendar = json.dumps({"calendar": {"earnings": {"earningsDate": [
        datetime.datetime.fromtimestamp(e, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z") for e in epochs]}}})

    def py():
        with _python_env(calendar=calendar):
            return asyncio.run(_srv().get_latest_earnings_release(tkr))

    return Case(name=name, ids=["F59"], py=py, routes=routes, select=_f59_view, expect=expect,
                ts={"js": "m.response.setWorkerEnv({}); return m.yahooFinance.getLatestEarningsRelease(args[0]);", "args": [tkr], "isolate": True},
                note="Python's get_calendar output has flat 'Earnings Date' keys, so its calendar path is stubbed with the Worker's shape.")


_PAST, _FUTURE = 1_577_836_800, 4_070_908_800   # 2020-01-01 and 2099-01-01 (UTC)
CASES += [
    _f59_yahoo_case("major300.f59.yahoo_estimate_fallback_has_no_content_url", "ZF59F", 850006, [_FUTURE, _PAST], {
        "ticker": "ZF59F", "eventType": "earnings_release", "period": "latest", "periodStatus": "UNRESOLVED", "periodEvidence": None,
        "reportedAt": "2020-01-01T00:00:00Z",
        "sources": [{"sourceType": "yahoo_estimate", "url": "https://finance.yahoo.com/quote/ZF59F/analysis",
                     "publishedAt": "2020-01-01T00:00:00Z", "confidence": "MEDIUM"}],
        "confidence": "MEDIUM", "warnings": [{"code": "SEC_8K_NOT_FOUND", "message": "SEC 8-K earnings release source not found"}]}),
    _f59_yahoo_case("major300.f59.nothing_found_has_no_sources", "ZF59G", 850007, [_FUTURE], {
        "ticker": "ZF59G", "eventType": "earnings_release", "period": "latest", "periodStatus": "UNRESOLVED", "periodEvidence": None,
        "reportedAt": None, "sources": [], "confidence": "NOT_FOUND", "warnings": []}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════
# F60: reconcile_metric_sources periods are latest, FY<yyyy> and FY<yyyy> Q<n>
# FY<yyyy> Q<n> is the issuer's fiscal quarter (SEC fy/fp, else counted from the prior fiscal year end), not the calendar
# quarter of the period end. latest_quarter, latest_annual and Q<n> <yyyy> are INPUT_VALIDATION_ERROR before any fetch.
# ═══════════════════════════════════════════════════════════════════════════════════════

_OFF_CIK = 1850060
_OFF_ROUTES = {
    "https://www.sec.gov/files/company_tickers.json": {"json": {"0": {"cik_str": _OFF_CIK, "ticker": "ZOFF", "title": "Off Calendar Co"}}},
    f"https://data.sec.gov/submissions/CIK{_OFF_CIK:010d}.json": {"json": {"cik": str(_OFF_CIK), "filings": {"recent": {
        "accessionNumber": [], "filingDate": [], "form": [], "primaryDocument": [], "items": [], "reportDate": []}}}},
}


def _off_fact(start: str, end: str, val: int, form: str, filed: str, accn: str, fy: int, fp: str) -> dict:
    return {"start": start, "end": end, "val": val, "form": form, "filed": filed, "accn": accn, "fy": fy, "fp": fp}


# A January fiscal year end: fiscal 2026 ended 2026-01-25, so its third quarter ended 2025-10-26 (calendar Q4 2025) and fiscal
# 2027's first quarter ended 2026-04-26 (calendar Q2 2026). The calendar quarter ending in 2026-07 (Q3 2026) is fiscal 2027 Q2.
_OFF_FACTS = {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [
    _off_fact("2024-01-29", "2025-01-26", 130_000, "10-K", "2025-02-26", "k25", 2025, "FY"),
    _off_fact("2025-01-27", "2025-04-27", 44_000, "10-Q", "2025-05-28", "q1f26", 2026, "Q1"),
    _off_fact("2025-04-28", "2025-07-27", 46_700, "10-Q", "2025-08-27", "q2f26", 2026, "Q2"),
    _off_fact("2025-07-28", "2025-10-26", 57_000, "10-Q", "2025-11-19", "q3f26", 2026, "Q3"),
    _off_fact("2025-01-27", "2026-01-25", 215_900, "10-K", "2026-02-25", "k26", 2026, "FY"),
    _off_fact("2026-01-26", "2026-04-26", 81_000, "10-Q", "2026-05-27", "q1f27", 2027, "Q1"),
    _off_fact("2026-04-27", "2026-07-26", 96_200, "10-Q", "2026-08-26", "q2f27", 2027, "Q2"),
]}}}}}
_OFF_ROUTES[f"https://data.sec.gov/api/xbrl/companyfacts/CIK{_OFF_CIK:010d}.json"] = {"json": _OFF_FACTS}


def _reconcile_py(spec: str | None):
    import asyncio as _asyncio
    from unittest.mock import patch as _patch
    from yfmcp.clients import edgar

    def no_yahoo(*_a, **_k):
        raise RuntimeError("Yahoo statements unavailable")

    args = ("ZOFF", "revenue") + (() if spec is None else (spec,))
    with _patch.dict(edgar._FILING_CIK_CACHE, {"ZOFF": f"{_OFF_CIK:010d}"}), _patch.object(server, "_yahoo_statement_rows_sync", no_yahoo):
        return json.loads(_asyncio.run(server.reconcile_metric_sources(*args)))


def _reconcile_view(v: dict) -> dict:
    period = v.get("period") if isinstance(v.get("period"), dict) else {}
    return {"error": v.get("error"), "code": v.get("code"), "message": v.get("message"), "status": v.get("status"),
            "spec": v.get("spec") or period.get("spec"), "periodType": v.get("periodType") or period.get("periodType"),
            "periodStart": period.get("periodStart"), "periodEnd": period.get("periodEnd"),
            "fiscalQuarter": period.get("fiscalQuarter"), "fiscalYears": period.get("fiscalYears"),
            "fiscalYearSource": period.get("fiscalYearSource"), "labelBasis": period.get("labelBasis"),
            "fiscalYearsAvailable": v.get("fiscalYearsAvailable")}


def _reconcile_case(name: str, spec: str | None, expect: dict, *, routes: dict | None = None, **kw) -> Case:
    js = ("m.response.setWorkerEnv({}); return JSON.parse(await m.yahooFinance.reconcileMetricSources('ZOFF', 'revenue'"
          + ("" if spec is None else ", args[0]") + "));")
    return Case(name=name, ids=["F60"], py=lambda: _reconcile_py(spec),
                ts={"js": js, "args": [spec] if spec is not None else [], "isolate": True},
                routes=_OFF_ROUTES if routes is None else routes, select=_reconcile_view, expect=expect, **kw)


_LABEL_BASIS = ("FY<yyyy> is the issuer's fiscal year (SEC fy/fp) when companyfacts gives it, else the fiscal year ending in that year; "
                "FY<yyyy> Q<n> is the issuer's fiscal quarter (SEC fy/fp of the filing that first reported it, else counted from the prior fiscal year end)")
_HELP = "period must be latest, FY<yyyy> or FY<yyyy> Q<n>."


def _ok_period(spec: str, ptype: str, start: str | None, end: str, years: list[int], source: str, quarter: int | None = None) -> dict:
    # Only SEC companyfacts is served, so the reconciliation itself is PARTIAL (one provider).
    return {"error": None, "code": None, "message": None, "status": "PARTIAL", "spec": spec, "periodType": ptype, "periodStart": start,
            "periodEnd": end, "fiscalQuarter": quarter, "fiscalYears": years, "fiscalYearSource": source, "labelBasis": _LABEL_BASIS,
            "fiscalYearsAvailable": None}


def _not_found(spec: str, ptype: str, message: str, years: list[int] | None = None) -> dict:
    return {**_invalid(spec), "error": None, "code": None, "message": message, "status": "PERIOD_NOT_FOUND", "spec": spec, "periodType": ptype,
            "fiscalYearsAvailable": years}


def _invalid(spec: str) -> dict:
    return {"error": True, "code": "INPUT_VALIDATION_ERROR", "message": _HELP, "status": None, "spec": None, "periodType": None,
            "periodStart": None, "periodEnd": None, "fiscalQuarter": None, "fiscalYears": None, "fiscalYearSource": None, "labelBasis": None,
            "fiscalYearsAvailable": None}


CASES += [
    # latest is the latest quarter and the default.
    _reconcile_case("major300.f60.latest_is_the_latest_quarter", "latest",
                    _ok_period("latest", "QUARTER", "2026-04-27", "2026-07-26", [2027], "SEC_FY_FP", 2)),
    _reconcile_case("major300.f60.period_defaults_to_latest", None,
                    _ok_period("latest", "QUARTER", "2026-04-27", "2026-07-26", [2027], "SEC_FY_FP", 2)),
    _reconcile_case("major300.f60.fy_names_the_issuers_fiscal_year", "FY2026",
                    _ok_period("FY2026", "ANNUAL", "2025-01-27", "2026-01-25", [2026], "SEC_FY_FP")),
    # FY2026 Q3 is the issuer's third quarter of fiscal 2026 (ended 2025-10-26), not the calendar quarter of any period end.
    _reconcile_case("major300.f60.fiscal_quarter_off_calendar_issuer_fy2026_q3", "FY2026 Q3",
                    _ok_period("FY2026 Q3", "QUARTER", "2025-07-28", "2025-10-26", [2026], "SEC_FY_FP", 3)),
    _reconcile_case("major300.f60.fiscal_quarter_is_case_and_space_tolerant", "fy 2027  q2",
                    _ok_period("fy 2027  q2", "QUARTER", "2026-04-27", "2026-07-26", [2027], "SEC_FY_FP", 2)),
    # Fiscal 2026 has no standalone Q4 quarter row, and fiscal 2027 has no Q3 yet.
    _reconcile_case("major300.f60.fiscal_quarter_without_a_quarter_row_is_period_not_found", "FY2026 Q4", _not_found(
        "FY2026 Q4", "QUARTER", "No quarter period in companyfacts matches FY2026 Q4 (20-F and 40-F filers tag annual periods only).")),
    _reconcile_case("major300.f60.fiscal_quarter_not_yet_reported_is_period_not_found", "FY2027 Q3", _not_found(
        "FY2027 Q3", "QUARTER", "No quarter period in companyfacts matches FY2027 Q3 (20-F and 40-F filers tag annual periods only).")),
    _reconcile_case("major300.f60.fiscal_year_not_filed_lists_the_years_available", "FY2031", _not_found(
        "FY2031", "ANNUAL", "No annual period in companyfacts is fiscal year 2031 (the issuer's SEC fy, else the year the period ends).", [2025, 2026])),
]
# The removed forms are input errors before any fetch (no routes: a fetch would fail the case).
for _spec in ("latest_quarter", "latest_annual", "Q3 2026", "q32026", "FY2026 Q5", "2026"):
    CASES.append(_reconcile_case(f"major300.f60.{_spec.lower().replace(' ', '_')}_is_rejected_before_any_fetch", _spec, _invalid(_spec), routes={}))


def _status_order_case() -> Case:
    """status is appended last (dict order = JS order) when the object has none; an existing status keeps its place."""
    text = json.dumps({"quality": {"label": "x", "confidence": "EXTRACTION_FAILED", "note": "n"},
                       "fact": {"value": None, "confidence": "NOT_DISCLOSED", "unit": "USD"},
                       "kept": {"confidence": "PARTIAL", "status": "OK", "label": "y"}})

    def py():
        data = json.loads(_envelope_tool_result("extract_sec_filing_fact", text))["data"]
        return {"quality": list(data["quality"]), "fact": [k for k in data["fact"] if k in ("value", "confidence", "unit", "status")],
                "kept": list(data["kept"])}

    js = ('m.response.setWorkerEnv({}); const data = JSON.parse(m.response.mcpSuccess("extract_sec_filing_fact", args[0])).data; '
          'return { quality: Object.keys(data.quality), fact: Object.keys(data.fact).filter((k) => ["value", "confidence", "unit", "status"].includes(k)), '
          'kept: Object.keys(data.kept) };')
    return Case(name="major300.f20.status_is_appended_last", ids=["F20"], py=py, ts={"js": js, "args": [text], "isolate": True},
                expect={"quality": ["label", "confidence", "note", "status"], "fact": ["value", "confidence", "unit", "status"],
                        "kept": ["confidence", "status", "label"]})


CASES.append(_status_order_case())
