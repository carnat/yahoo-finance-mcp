"""Parity cases for the 4.0.0 major release (the 3.11.0 neutrality-label deprecations removed, NL1-NL13).

Every case runs the tool in Python and in the Worker and expects the removed keys to be absent and the replacement to be
present:

- analyze_moving_average_position: no ``trend``; maAlignment stays.
- get_market_snapshot: no ``trend.maTrend``.
- analyze_position_signals: no analystNetSentiment, earningsInputs.momentumFlag or technicalInputs.maPosition; no
  DEPRECATED_FIELD row in meta.warnings (the componentStatus key "maPosition" stays).
- get_analyst_rating_changes: no netSentiment and no strengthFlag; netUpgrades stays.
- analyze_earnings_momentum: none of the six labels; compositeMethodNote has its new text; revisionSignalDriver stays.
- analyze_credit_health: no debtTier or creditStressFlag; leverageBand and meetsCreditStressThreshold stay, and the
  unavailable/computed metric lists name them.
- get_analyst_consensus: no targetLagSignal; the warning still fires.
- get_short_momentum: no squeezeRisk or flag.
- find_put_hedge_candidates: no ivFlag; decisionUse CANDIDATE_SCREEN_ONLY.
- analyze_options_flow_window: no bracket; formattedBlock has no Advisory segment and no "SCAN".
- calculate_price_target_distance: no ioPt, eqfPct or invertedFlag; no io_pt argument, and no DEPRECATED_FIELD notice.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from scripts.parity.cases_neutrality import _position_case
from scripts.parity.cases_major_300 import QS, _flow_case
from scripts.parity.cases_options_reads_382 import _case as _hedge_case
from scripts.parity.cases_pricing_evidence_tools import (
    BEAT, GATE_NOW, M, MISS, QUOTE_TIME, YAHOO_AUTH_ROUTES, _bracket_case, _credit_case, _gate_case,
    _momentum_case, _ts_with_clock,
)
from scripts.parity.harness import Case

import server  # noqa: E402
from yfmcp.tools import fundamentals as fundamentals_tools  # noqa: E402

CASES: list[Case] = []
IDS = ["NL400"]


def _retitle(case: Case, name: str, select, expect) -> Case:
    """An existing scenario with a new name, view and expectation (the stubs and routes stay)."""
    case.name = f"major400.{name}"
    case.ids = list(IDS)
    case.select = select
    case.expect = expect
    return case


# ═══════════════════════════════════════════════════════════════════════════════════════
# analyze_moving_average_position and get_market_snapshot: no trend / maTrend
# ═══════════════════════════════════════════════════════════════════════════════════════

def _ma_case() -> Case:
    company = SimpleNamespace(
        fast_info=SimpleNamespace(last_price=110.0, fifty_day_average=100.0, two_hundred_day_average=120.0), info={})
    summary = {"quoteSummary": {"result": [{"price": {
        "currency": "USD", "quoteType": "EQUITY", "marketState": "CLOSED", "regularMarketPrice": {"raw": 110.0}},
        "summaryDetail": {"fiftyDayAverage": {"raw": 100.0}, "twoHundredDayAverage": {"raw": 120.0}},
        "defaultKeyStatistics": {}}]}}

    def py():
        with patch.object(server.yf, "Ticker", lambda _t: company):
            return json.loads(asyncio.run(server.get_ma_position("MJRMA")))

    return Case(
        name="major400.ma_position.no_trend_and_alignment_stays", ids=list(IDS), py=py,
        ts={"js": 'return JSON.parse(await m.yahooFinance.getMaPosition("MJRMA"));', "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, f"{QS}MJRMA?*": {"json": summary}},
        select=lambda v: {"trendPresent": "trend" in v, "regime50": v["regime50"], "regime200": v["regime200"],
                          "maAlignment": v["maAlignment"]},
        expect={"trendPresent": False, "regime50": "ABOVE", "regime200": "BELOW", "maAlignment": "MIXED"})


CASES.append(_ma_case())

# The snapshot's other components fail, which leaves its trend block with only the keys it names.
CASES.append(_retitle(
    _gate_case("x", ["NL400"], symbol="MJRSNAP", currency="USD", close=10.0, volume=1_000_000, fx=None, expect=None,
               via="snapshot"),
    "snapshot.trend_block_has_no_ma_trend_and_keeps_ma_alignment",
    lambda v: {"maTrendPresent": "maTrend" in v["trend"], "maAlignmentPresent": "maAlignment" in v["trend"],
               "trendKeys": sorted(v["trend"])},
    {"maTrendPresent": False, "maAlignmentPresent": True,
     "trendKeys": sorted(["fiftyDayAverage", "twoHundredDayAverage", "pctFrom50dma", "pctFrom200dma", "maAlignment",
                          "rsi14", "macdHistogram"])}))


# ═══════════════════════════════════════════════════════════════════════════════════════
# analyze_position_signals: no label copies and no DEPRECATED_FIELD notice
# ═══════════════════════════════════════════════════════════════════════════════════════

CASES.append(_position_case(
    "major400.position.no_label_copies_and_no_deprecated_field_row",
    lambda v: {"analystNetSentiment": "analystNetSentiment" in v["data"]["analystInputs"],
               "analystNetUpgrades": "analystNetUpgrades" in v["data"]["analystInputs"],
               "momentumFlag": "momentumFlag" in v["data"]["earningsInputs"],
               "revisionBand": "revisionBand" in v["data"]["earningsInputs"],
               "technicalMaPosition": "maPosition" in v["data"]["technicalInputs"],
               "maAlignment": "maAlignment" in v["data"]["technicalInputs"],
               "componentStatusMaPosition": "maPosition" in v["data"]["componentStatus"],
               "deprecatedFieldRows": [w for w in v["meta"]["warnings"] if w.get("code") == "DEPRECATED_FIELD"]},
    ids=("NL400",),
    expect={"analystNetSentiment": False, "analystNetUpgrades": True, "momentumFlag": False, "revisionBand": True,
            "technicalMaPosition": False, "maAlignment": True, "componentStatusMaPosition": True,
            "deprecatedFieldRows": []}))


# ═══════════════════════════════════════════════════════════════════════════════════════
# get_analyst_rating_changes (get_analyst_upgrade_radar): no netSentiment and no strengthFlag
# ═══════════════════════════════════════════════════════════════════════════════════════

def _radar_case(name: str, history_rows: int) -> Case:
    now_row = {"Firm": "Firm A", "ToGrade": "Buy", "FromGrade": "Hold", "Action": "up",
               "currentPriceTarget": 120.0, "priorPriceTarget": 100.0, "priceTargetAction": ""}
    rows = [now_row] * history_rows

    class _Ticker:
        upgrades_downgrades = (pd.DataFrame(rows, index=pd.DatetimeIndex([pd.Timestamp.now().normalize()] * history_rows,
                                                                         name="GradeDate"))
                               if rows else pd.DataFrame())

    def py():
        with patch.object(server.yf, "Ticker", lambda _t: _Ticker()):
            return json.loads(asyncio.run(server.get_analyst_upgrade_radar("MJR4RDR", days_back=30)))

    import time
    recent = int(time.time()) - 86_400
    history = {"quoteSummary": {"result": [{"upgradeDowngradeHistory": {"history": [{
        "epochGradeDate": recent, "firm": "Firm A", "toGrade": "Buy", "fromGrade": "Hold", "action": "up",
        "currentPriceTarget": 120.0, "priorPriceTarget": 100.0, "priceTargetAction": ""} for _ in rows]}}]}}
    return Case(
        name=f"major400.rating_changes.{name}", ids=list(IDS), py=py,
        ts={"js": 'return JSON.parse(await m.yahooFinance.getAnalystUpgradeRadar("MJR4RDR", 30));', "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, f"{QS}MJR4RDR?*": {"json": history}},
        select=lambda v: {"netSentiment": "netSentiment" in v, "netUpgrades": v["netUpgrades"],
                          "strengthFlagCount": sum("strengthFlag" in c for c in v["changes"]),
                          "changeKeys": [sorted(c) for c in v["changes"]]},
        expect={"netSentiment": False, "netUpgrades": history_rows, "strengthFlagCount": 0,
                "changeKeys": [sorted(["date", "firm", "fromGrade", "toGrade", "signal", "ptFrom", "ptTo", "ptDirection",
                                       "mixedSignal"])] * history_rows})


CASES += [_radar_case("one_upgrade_has_net_upgrades_only", 1), _radar_case("no_changes_has_net_upgrades_only", 0)]


# ═══════════════════════════════════════════════════════════════════════════════════════
# analyze_earnings_momentum: the six labels are gone; the method note and the driver stay
# ═══════════════════════════════════════════════════════════════════════════════════════

_EARNINGS_REMOVED = ("momentumFlag", "revisionDirection", "historicalSurpriseSignal", "forwardRevisionSignal",
                     "compositeMomentumSignal", "interpretationNote")
_NEW_METHOD_NOTE = ("revisionBand uses the 30d revision as primary and the 7d when the 30d is inside the band; with no 30d "
                    "revision it uses the 7d, then the 90d as a fallback. A 90d revision never overrides a 30d or 7d one.")

CASES += [
    _retitle(_momentum_case("x", ["NL400"], [BEAT, BEAT, MISS, BEAT], ()), "earnings.labels_are_gone_and_replacements_stay",
             lambda v: {"removedKeysPresent": sorted(k for k in _EARNINGS_REMOVED if k in v),
                        "revisionBand": v["revisionBand"], "revisionSignalDriver": v["revisionSignalDriver"],
                        "beatRateBand": v["beatRateBand"], "compositeMethodNote": v["compositeMethodNote"],
                        "warningCodes": sorted(w["code"] for w in v["warnings"])},
             {"removedKeysPresent": [], "revisionBand": None, "revisionSignalDriver": "none", "beatRateBand": "75_TO_100PCT",
              "compositeMethodNote": _NEW_METHOD_NOTE, "warningCodes": []}),
    _retitle(_momentum_case("x", ["NL400"], None, ()), "earnings.no_history_has_no_momentum_flag",
             lambda v: {"removedKeysPresent": sorted(k for k in _EARNINGS_REMOVED if k in v), "preRevenue": v["preRevenue"],
                        "warningCodes": sorted(w["code"] for w in v["warnings"])},
             {"removedKeysPresent": [], "preRevenue": True, "warningCodes": ["PRE_REVENUE_NO_HISTORY"]}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════
# analyze_credit_health: no debtTier or creditStressFlag
# ═══════════════════════════════════════════════════════════════════════════════════════

def _credit_view(v: dict) -> dict:
    return {"removedKeysPresent": sorted(k for k in ("debtTier", "creditStressFlag") if k in v),
            "leverageBand": v["leverageBand"], "meetsCreditStressThreshold": v["meetsCreditStressThreshold"],
            "unavailableMetrics": v["unavailableMetrics"], "computedMetrics": v["computedMetrics"]}


CASES += [
    _retitle(_credit_case("x", ["NL400"], {"debt": [410 * M], "cash": [100 * M]},
                          {"op": [15 * M] * 4, "da": [10 * M] * 4, "int": [6.25 * M] * 4}, ()),
             "credit.stressed_card_example", _credit_view,
             {"removedKeysPresent": [], "leverageBand": "2_5X_TO_4X", "meetsCreditStressThreshold": True,
              "unavailableMetrics": [],
              "computedMetrics": ["netDebtUsd", "operationalEbitdaUsd", "netDebtToEbitda", "interestCoverage",
                                  "interestCoverageEbit", "interestCoverageEbitda", "meetsCreditStressThreshold",
                                  "leverageBand"]}),
    _retitle(_credit_case("x", ["NL400"], {"debt": [300 * M], "cash": [200 * M]},
                          {"op": [-10 * M] * 4, "da": [5 * M] * 4, "int": [2 * M] * 4}, ()),
             "credit.loss_maker_with_net_debt_is_negative_earnings_base", _credit_view,
             {"removedKeysPresent": [], "leverageBand": "NEGATIVE_EARNINGS_BASE", "meetsCreditStressThreshold": None,
              "unavailableMetrics": ["netDebtToEbitda", "meetsCreditStressThreshold"],
              "computedMetrics": ["netDebtUsd", "operationalEbitdaUsd", "interestCoverage", "interestCoverageEbit",
                                  "interestCoverageEbitda", "leverageBand"]}),
    _retitle(_credit_case("x", ["NL400"], {"debt": [500 * M], "cash": [100 * M]},
                          {"op": [10 * M] * 4, "da": [-10 * M] * 4, "int": [1 * M] * 4}, ()),
             "credit.zero_ebitda_has_no_band_and_no_threshold", _credit_view,
             {"removedKeysPresent": [], "leverageBand": None, "meetsCreditStressThreshold": None,
              "unavailableMetrics": ["netDebtToEbitda", "meetsCreditStressThreshold"],
              "computedMetrics": ["netDebtUsd", "operationalEbitdaUsd", "interestCoverage", "interestCoverageEbit",
                                  "interestCoverageEbitda"]}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════
# get_analyst_consensus: no targetLagSignal; the warning still fires
# ═══════════════════════════════════════════════════════════════════════════════════════

def _consensus_case(name: str, symbol: str, price: float, upgrades: int, expect: dict, age_days: int = 1) -> Case:
    trend_rows = [{"period": "0m", "strongBuy": 2, "buy": 3, "hold": 1, "sell": 1, "strongSell": 0}]
    now = pd.Timestamp.now().normalize()
    upgrade_rows = [{"Firm": "Firm A", "ToGrade": "Buy", "FromGrade": "Hold", "Action": "up",
                     "currentPriceTarget": 150.0, "priorPriceTarget": 140.0, "priceTargetAction": ""}] * upgrades
    company = SimpleNamespace(
        fast_info=SimpleNamespace(last_price=price),
        analyst_price_targets={"mean": 150.0, "low": 100.0, "high": 200.0, "median": 150.0},
        upgrades_downgrades=(pd.DataFrame(upgrade_rows, index=pd.DatetimeIndex([now - pd.Timedelta(days=age_days)]
                                                                               * upgrades, name="GradeDate"))
                             if upgrades else pd.DataFrame()),
        recommendations_summary=pd.DataFrame(trend_rows), info={"numberOfAnalystOpinions": 3})

    def py():
        with patch.object(server.yf, "Ticker", lambda _t: company), \
                patch.object(fundamentals_tools, "_target_analyst_count", lambda _c: 3):
            return json.loads(asyncio.run(server.get_analyst_consensus(symbol)))

    from scripts.parity.cases_neutrality import _POS_SUMMARY
    summary = json.loads(json.dumps(_POS_SUMMARY))
    result = summary["quoteSummary"]["result"][0]
    result["price"]["regularMarketPrice"] = {"raw": price}
    result["upgradeDowngradeHistory"] = {"history": [{
        "epochGradeDate": GATE_NOW - 86_400 * age_days, "firm": "Firm A", "toGrade": "Buy",
        "fromGrade": "Hold", "action": "up", "currentPriceTarget": 150.0, "priorPriceTarget": 140.0,
        "priceTargetAction": ""} for _ in range(upgrades)]}
    return Case(
        name=f"major400.consensus.{name}", ids=list(IDS), py=py,
        ts={"js": _ts_with_clock(GATE_NOW, f'return JSON.parse(await m.yahooFinance.getAnalystConsensus("{symbol}"));'),
            "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, f"{QS}{symbol}?*": {"json": summary}},
        select=lambda v: {"targetLagSignalPresent": "targetLagSignal" in v, "targetVsPrice": v["targetVsPrice"],
                          "recentUpgradeCount30d": v["recentUpgradeCount30d"],
                          "warningCodes": sorted(w["code"] for w in v["warnings"])},
        expect=expect)


CASES += [
    _consensus_case("target_below_price_with_upgrades_warns", "MJR4CA", 200.0, 1,
                    {"targetLagSignalPresent": False, "targetVsPrice": "BELOW", "recentUpgradeCount30d": 1,
                     "warningCodes": ["CONSENSUS_TARGET_BELOW_PRICE_DESPITE_UPGRADES"]}),
    # An upgrade 60 days old is outside the 30-day count.
    _consensus_case("target_below_price_with_only_an_old_upgrade_does_not_warn", "MJR4CB", 200.0, 1,
                    {"targetLagSignalPresent": False, "targetVsPrice": "BELOW", "recentUpgradeCount30d": 0,
                     "warningCodes": []}, age_days=60),
    # 4.0.1: no rating-history rows at all is no data, so the count is null in both runtimes (the Worker said 0).
    _consensus_case("no_rating_history_count_is_null", "MJR4CD", 200.0, 0,
                    {"targetLagSignalPresent": False, "targetVsPrice": "BELOW", "recentUpgradeCount30d": None,
                     "warningCodes": []}),
    _consensus_case("target_above_price_does_not_warn", "MJR4CC", 129.9, 1,
                    {"targetLagSignalPresent": False, "targetVsPrice": "AT_OR_ABOVE", "recentUpgradeCount30d": 1,
                     "warningCodes": []}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════
# get_short_momentum: no squeezeRisk and no flag
# ═══════════════════════════════════════════════════════════════════════════════════════

def _short_case(name: str, pct: float) -> Case:
    info = {"sharesShort": 1_000_000, "sharesShortPriorMonth": 900_000, "shortPercentOfFloat": pct, "shortRatio": 2.0,
            "dateShortInterest": 1_790_000_000}
    company = SimpleNamespace(info=info)
    summary = {"quoteSummary": {"result": [{"defaultKeyStatistics": {
        "sharesShort": {"raw": 1_000_000}, "sharesShortPriorMonth": {"raw": 900_000},
        "shortPercentOfFloat": {"raw": pct}, "shortRatio": {"raw": 2.0}, "dateShortInterest": {"raw": 1_790_000_000}},
        "price": {}}]}}

    def py():
        with patch.object(server.yf, "Ticker", lambda _t: company):
            return json.loads(asyncio.run(server.get_short_momentum("MJR4SHT")))

    return Case(
        name=f"major400.short_momentum.{name}", ids=list(IDS), py=py,
        ts={"js": 'return JSON.parse(await m.yahooFinance.getShortMomentum("MJR4SHT"));', "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, f"{QS}MJR4SHT?*": {"json": summary}},
        select=lambda v: {"removedKeysPresent": sorted(k for k in ("squeezeRisk", "flag") if k in v),
                          "keys": sorted(v)},
        expect={"removedKeysPresent": [],
                "keys": sorted(["ticker", "shortPctFloat", "daysToCover", "sharesShort", "sharesShortPriorMonth", "momDeltaPct",
                                 "momDirection", "dateShortInterest", "dataDate", "dataDateBasis"])})


CASES += [_short_case("above_30_pct_has_no_squeeze_risk_or_flag", 0.35), _short_case("below_20_pct_has_no_squeeze_risk_or_flag", 0.05)]


# ═══════════════════════════════════════════════════════════════════════════════════════
# find_put_hedge_candidates: no ivFlag; decisionUse CANDIDATE_SCREEN_ONLY
# ═══════════════════════════════════════════════════════════════════════════════════════

CASES.append(_retitle(
    _hedge_case("x", {}, {}), "put_hedge.candidates_have_no_iv_flag_and_decision_use_is_candidate_screen_only",
    lambda v: {"decisionUse": v["decisionUse"], "candidateCount": len(v["candidates"]),
               "ivFlagCount": sum("ivFlag" in c for c in v["candidates"]),
               "candidateKeys": sorted({k for c in v["candidates"] for k in c})},
    {"decisionUse": "CANDIDATE_SCREEN_ONLY", "candidateCount": 4, "ivFlagCount": 0,
     "candidateKeys": sorted(["expiry", "strike", "bid", "ask", "mid", "contractCost", "contractCostBasis", "withinBudget",
                              "openInterest", "volume", "quoteStatus", "ivPctile", "otmPct"])}))


# ═══════════════════════════════════════════════════════════════════════════════════════
# analyze_options_flow_window: no bracket; formattedBlock drops its Advisory segment
# ═══════════════════════════════════════════════════════════════════════════════════════

CASES.append(_retitle(
    _flow_case("x", None), "options_flow.low_quality_block_has_no_bracket_note",
    lambda v: {"bracketPresent": "bracket" in v, "quality": v["dataQuality"]["quality"], "formattedBlock": v["formattedBlock"]},
    {"bracketPresent": False, "quality": "LOW", "formattedBlock": "OPTIONS FLOW: DATA QUALITY LOW — raw chain unreliable."}))


def _fresh_flow_case() -> Case:
    """The same chain with last trades after the data date (never stale), so the chain quality is not LOW."""
    from unittest.mock import AsyncMock
    from scripts.parity.cases_major_300 import _FLOW_EXPIRY, _FLOW_EXPIRY_TS, _contract
    from yfmcp.tools import pricing

    def fresh(strike: float, kind: str) -> dict:
        return {**_contract(strike, kind), "lastTradeDate": 4_000_000_000}

    strikes = [95.0, 100.0, 105.0]
    calls = [fresh(x, "C") for x in strikes]
    puts = [fresh(x, "P") for x in strikes]

    class _Ticker:
        fast_info = {"lastPrice": 100.0, "tenDayAverageVolume": 1_000_000}
        options = (_FLOW_EXPIRY,)

        def option_chain(self, expiry):
            return SimpleNamespace(calls=pd.DataFrame(calls), puts=pd.DataFrame(puts))

    def py():
        with patch.object(server.yf, "Ticker", lambda _t: _Ticker()), \
                patch.object(pricing, "_load_completed_daily_history", AsyncMock(side_effect=RuntimeError("no history"))), \
                patch.object(pricing, "get_last_trading_date", lambda: "2026-10-08"):
            return json.loads(asyncio.run(server.get_options_flow_scan("MJRFLOW", "audit")))

    chain = {"optionChain": {"result": [{"expirationDates": [_FLOW_EXPIRY_TS], "options": [{"expirationDate": _FLOW_EXPIRY_TS,
                                                                                           "calls": calls, "puts": puts}]}]}}
    summary = {"quoteSummary": {"result": [{"price": {"regularMarketPrice": {"raw": 100.0}},
                                            "summaryDetail": {"averageVolume10days": {"raw": 1_000_000}}}]}}
    return Case(
        name="major400.options_flow.block_has_no_advisory_segment_and_no_scan_word", ids=list(IDS), py=py,
        ts={"js": _ts_with_clock(GATE_NOW, 'return JSON.parse(await m.yahooFinance.getOptionsFlowScan("MJRFLOW", "audit"));'),
            "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, "https://query2.finance.yahoo.com/v7/finance/options/MJRFLOW?*": {"json": chain},
                f"{QS}MJRFLOW?*": {"json": summary}},
        select=lambda v: {"bracketPresent": "bracket" in v, "quality": v["dataQuality"]["quality"],
                          "formattedBlock": v["formattedBlock"]},
        expect={"bracketPresent": False, "quality": "HIGH",
                "formattedBlock": "OPTIONS FLOW [audit] MJRFLOW | P/C: 0.60 | IV vs 1y RV range: N/A | "
                                  "Put vol / 1% stock ADV: 0.09x | Trend: STABLE"})


CASES.append(_fresh_flow_case())


# ═══════════════════════════════════════════════════════════════════════════════════════
# calculate_price_target_distance: no ioPt, eqfPct or invertedFlag; no io_pt argument
# ═══════════════════════════════════════════════════════════════════════════════════════

_PT_REMOVED = ("ioPt", "eqfPct", "invertedFlag")

CASES += [
    _retitle(_bracket_case("x", ["NL400"], 70.0, 100.0, market_time=QUOTE_TIME, via="envelope"),
             "price_target.envelope_has_no_copies_and_no_notice",
             lambda v: {"removedKeysPresent": sorted(k for k in _PT_REMOVED if k in v["data"]),
                        "referenceTargetPrice": v["data"]["referenceTargetPrice"],
                        "referenceTargetPct": v["data"]["referenceTargetPct"],
                        "priceAtOrAboveTarget": v["data"]["priceAtOrAboveTarget"], "warnings": v["meta"]["warnings"]},
             {"removedKeysPresent": [], "referenceTargetPrice": 100.0, "referenceTargetPct": 70.0,
              "priceAtOrAboveTarget": False, "warnings": []}),
    _retitle(_bracket_case("x", ["NL400"], 70.0, 0.0, market_time=QUOTE_TIME, via="envelope"),
             "price_target.validation_message_names_only_reference_target_price",
             lambda v: {"ok": v["ok"], "error": v["error"]},
             {"ok": False, "error": {"code": "INPUT_VALIDATION_ERROR",
                                     "message": "reference_target_price must be a positive number"}}),
]


# An io_pt argument is no longer part of the contract. Expanded mode: neither runtime validates unknown arguments, so the
# call runs with no reference_target_price and fails validation. Grouped mode: both runtimes refuse it as an unexpected
# parameter.
def _io_pt_expanded_case() -> Case:
    def py():
        async def call():
            result = await server.yfinance_server.call_tool("calculate_price_target_distance", {"ticker": "TST", "io_pt": 95})
            return json.loads(result.content[0].text)
        return asyncio.run(call())

    return Case(
        name="major400.price_target.io_pt_argument_in_expanded_mode", ids=list(IDS), py=py,
        ts={"js": 'm.response.setWorkerEnv({}); return (await m.tools.callToolResult("calculate_price_target_distance", '
                  '{ticker: "TST", io_pt: 95})).value;', "isolate": True},
        select=lambda v: {"ok": v["ok"], "error": v["error"]},
        expect={"ok": False, "error": {"code": "INPUT_VALIDATION_ERROR",
                                       "message": "reference_target_price must be a positive number"}})


CASES.append(_io_pt_expanded_case())


def _io_pt_grouped_case() -> Case:
    import tool_groups
    from yfmcp.app import build_handler_registry

    def py():
        return json.loads(asyncio.run(tool_groups._route_grouped_call(
            "screening", "calculate_price_target_distance", {"ticker": "TST", "reference_target_price": 100, "io_pt": 95},
            build_handler_registry(server.yfinance_server))))

    return Case(
        name="major400.price_target.io_pt_argument_in_grouped_mode", ids=list(IDS), py=py,
        ts={"js": 'm.response.setWorkerEnv({}); const r = await m.tools.callVisibleToolResult("screening", '
                  '{action: "calculate_price_target_distance", params: {ticker: "TST", reference_target_price: 100, io_pt: 95}}); '
                  'return r.value ?? JSON.parse(r.text);', "isolate": True},
        select=lambda v: {"ok": v["ok"], "code": v["error"]["code"], "message": v["error"]["message"],
                          "details": v["error"].get("details")},
        expect={"ok": False, "code": "INPUT_VALIDATION_ERROR",
                "message": "Invalid params for 'calculate_price_target_distance': unexpected parameter(s): io_pt.",
                "details": None})


CASES.append(_io_pt_grouped_case())
