"""Parity cases for 3.11.0, the neutrality label notices (audit NL1-NL13,
/mnt/project-files/audits/neutrality-labels-2026-11-12.md; docs/provider-runtime-guidance.md
"Neutrality Label Notices (3.11.0)").

- The replacement labels (yfmcp/labels.py, worker/src/labels.ts) at every band boundary.
- The DEPRECATED_FIELD notices (``deprecation_warnings_for`` / ``deprecationWarningsFor``): one per notice whose
  fields the payload carries, at the top level, one level down in a batch, under ``tickers``, or in an array element.
- End to end: the new fields in analyze_credit_health and analyze_earnings_momentum, and the notice reaching
  meta.warnings for analyze_position_signals. Nothing deprecated changes value.
"""

from __future__ import annotations

import json

from scripts.parity.harness import Case
from scripts.parity.cases_neutrality import _position_case
from scripts.parity.cases_pricing_evidence_tools import M, BEAT, MISS, _credit_case, _momentum_case
from yfmcp import labels
from yfmcp.deprecations import deprecation_warnings_for

CASES: list[Case] = []


def _fn(name: str, py_fn, ts_fn: str, args: list, expect=None) -> Case:
    return Case(name=f"labels3110.{name}", ids=["NL3110"], py=lambda: py_fn(*args),
                ts={"js": f"const r = m.labels.{ts_fn}(...args); return r === undefined ? null : r;", "args": args},
                **({"expect": expect} if expect is not None else {}))


# NL1
CASES += [_fn(f"ma_alignment.{a}_{b}", labels.ma_alignment, "maAlignment", [a, b], expect)
          for a, b, expect in [("ABOVE", "ABOVE", "ABOVE_BOTH"), ("BELOW", "BELOW", "BELOW_BOTH"),
                               ("ABOVE", "BELOW", "MIXED"), ("BELOW", "ABOVE", "MIXED")]]
CASES += [Case(name="labels3110.ma_alignment.unknown_regime_is_null", ids=["NL3110"],
               py=lambda: labels.ma_alignment(None, "ABOVE"),
               ts={"js": "return m.labels.maAlignment(null, 'ABOVE');"}, expect=None)]


# NL3: revision band. Python returns (band, driver); the Worker {band, driver}.
def _band_py(*args):
    band, driver = labels.revision_band(*args)
    return {"band": band, "driver": driver}


for label, args, expect in [
    ("30d_up_at_threshold", [3, None, None, 3], {"band": "UP", "driver": "30d"}),
    ("30d_down_at_threshold", [-3, None, None, 3], {"band": "DOWN", "driver": "30d"}),
    ("30d_inside_band_is_flat", [2.99, None, None, 3], {"band": "FLAT", "driver": "30d"}),
    ("30d_flat_7d_confirms_down", [0.2, -3.5, None, 3], {"band": "DOWN", "driver": "30d_neutral_7d"}),
    ("30d_flat_7d_inside_band_stays_flat", [0.2, -0.26, 0.11, 3], {"band": "FLAT", "driver": "30d"}),
    ("7d_only", [None, 4, None, 3], {"band": "UP", "driver": "7d"}),
    ("90d_fallback", [None, None, -5, 3], {"band": "DOWN", "driver": "90d_fallback"}),
    ("none_known", [None, None, None, 3], {"band": None, "driver": "none"}),
    ("caller_threshold_turns_flat_into_up", [0.2, None, None, 0.1], {"band": "UP", "driver": "30d"}),
    ("caller_threshold_widens_band", [5, None, None, 10], {"band": "FLAT", "driver": "30d"}),
]:
    CASES.append(Case(name=f"labels3110.revision_band.{label}", ids=["NL3110"], py=lambda a=args: _band_py(*a),
                      ts={"js": "return m.labels.revisionBand(...args);", "args": args}, expect=expect))

CASES += [_fn(f"beat_rate_band.{str(rate).replace('.', '_')}", labels.beat_rate_band, "beatRateBand", [rate], expect)
          for rate, expect in [(1, "75_TO_100PCT"), (0.75, "75_TO_100PCT"), (0.74, "55_TO_75PCT"), (0.55, "55_TO_75PCT"),
                               (0.4, "40_TO_55PCT"), (0.39, "0_TO_40PCT"), (0, "0_TO_40PCT")]]
CASES += [Case(name="labels3110.beat_rate_band.unknown_is_null", ids=["NL3110"], py=lambda: labels.beat_rate_band(None),
               ts={"js": "return m.labels.beatRateBand(null);"}, expect=None)]

# NL6
CASES += [_fn(f"leverage_band.{label}", labels.leverage_band, "leverageBand", [ratio, neg], expect)
          for label, ratio, neg, expect in [
              ("0_9x", 0.9, False, "BELOW_1X"), ("1_0x", 1.0, False, "1X_TO_2_5X"), ("2_5x", 2.5, False, "1X_TO_2_5X"),
              ("2_6x", 2.6, False, "2_5X_TO_4X"), ("4_0x", 4.0, False, "2_5X_TO_4X"), ("4_1x", 4.1, False, "ABOVE_4X"),
              ("negative_net_debt", -1.0, False, "BELOW_1X"), ("loss_maker", None, True, "NEGATIVE_EARNINGS_BASE")]]
CASES += [Case(name="labels3110.leverage_band.unknown_is_null", ids=["NL3110"], py=lambda: labels.leverage_band(None, False),
               ts={"js": "return m.labels.leverageBand(null, false);"}, expect=None)]
CASES += [_fn(f"credit_stress.{label}", labels.meets_credit_stress_threshold, "meetsCreditStressThreshold", args, expect)
          for label, args, expect in [
              ("default_stressed", [3.1, 2.4, 2.5, 3], True), ("default_ratio_at_max_is_not", [2.5, 2.4, 2.5, 3], False),
              ("default_coverage_at_min_is_not", [3.1, 3, 2.5, 3], False), ("caller_max_4", [3.1, 2.4, 4, 3], False),
              ("caller_min_coverage_2", [3.1, 2.4, 2.5, 2], False)]]
CASES += [Case(name="labels3110.credit_stress.unknown_coverage_is_null", ids=["NL3110"],
               py=lambda: labels.meets_credit_stress_threshold(3.1, None, 2.5, 3),
               ts={"js": "return m.labels.meetsCreditStressThreshold(3.1, null, 2.5, 3);"}, expect=None)]

# NL7
CASES += [_fn(f"target_vs_price.{label}", labels.target_vs_price, "targetVsPrice", [t, p], expect)
          for label, t, p, expect in [("above", 210, 200, "AT_OR_ABOVE"), ("equal", 200, 200, "AT_OR_ABOVE"),
                                      ("below", 190, 200, "BELOW")]]


# Argument parsing: Python returns (value, error); the Worker the value or {error}.
def _arg_py(name, value):
    parsed, error = labels.parse_positive_number_arg(name, value)
    return {"error": error} if error else parsed


for label, value, expect in [("absent", None, None), ("valid", 2.5, 2.5),
                             ("zero", 0, {"error": "max_net_debt_to_ebitda must be a positive number when supplied."}),
                             ("text", "3", {"error": "max_net_debt_to_ebitda must be a positive number when supplied."}),
                             ("bool", True, {"error": "max_net_debt_to_ebitda must be a positive number when supplied."})]:
    CASES.append(Case(name=f"labels3110.positive_arg.{label}", ids=["NL3110"],
                      py=lambda v=value: _arg_py("max_net_debt_to_ebitda", v),
                      ts={"js": "const r = m.labels.parsePositiveNumberArg('max_net_debt_to_ebitda', args[0]); "
                                "return r === undefined ? null : r;", "args": [value]},
                      expect=expect))


# Notices.
REMOVAL = "will be removed in the next major version"


def _notice_case(name: str, tool: str, data, expect_fields: list[list[str]]) -> Case:
    return Case(name=f"labels3110.notice.{name}", ids=["NL3110"],
                py=lambda: deprecation_warnings_for(tool, json.loads(json.dumps(data))),
                ts={"js": "return m.deprecations.deprecationWarningsFor(args[0], args[1]);", "args": [tool, data]},
                select=lambda v: [n["fields"] for n in v], expect=expect_fields)


CASES += [
    _notice_case("ma_trend", "analyze_moving_average_position", {"trend": "BULLISH", "maAlignment": "ABOVE_BOTH"}, [["trend"]]),
    _notice_case("ma_batch_one_level_down", "analyze_moving_average_position", {"AAPL": {"trend": "MIXED"}, "MSFT": {"error": True}},
                 [["trend"]]),
    _notice_case("snapshot_tickers", "get_market_snapshot", {"tickers": {"AAPL": {"trend": {"maTrend": "BULLISH"}}}},
                 [["trend.maTrend"]]),
    _notice_case("snapshot_without_trend_block", "get_market_snapshot", {"ticker": "AAPL", "quote": {}}, []),
    _notice_case("rating_strength_in_changes", "get_analyst_rating_changes",
                 {"netSentiment": 1, "changes": [{"signal": "UPGRADE", "strengthFlag": "BULLISH"}]},
                 [["netSentiment", "changes[].strengthFlag"]]),
    _notice_case("rating_strength_only_in_array", "get_analyst_rating_changes",
                 {"changes": [{"signal": "UPGRADE"}, {"strengthFlag": "NEUTRAL"}]}, [["netSentiment", "changes[].strengthFlag"]]),
    _notice_case("earnings", "analyze_earnings_momentum", {"momentumFlag": "POSITIVE", "revisionBand": "FLAT"},
                 [["momentumFlag", "revisionDirection", "forwardRevisionSignal", "historicalSurpriseSignal",
                   "compositeMomentumSignal", "interpretationNote"]]),
    _notice_case("short", "get_short_momentum", {"squeezeRisk": "LOW", "flag": None}, [["squeezeRisk", "flag"]]),
    _notice_case("options_bracket", "analyze_options_flow_window", {"bracket": None, "pcRatio": 0.9}, [["bracket"]]),
    _notice_case("credit", "analyze_credit_health", {"debtTier": "CLEAN"}, [["debtTier", "creditStressFlag"]]),
    _notice_case("consensus", "get_analyst_consensus", {"targetLagSignal": "CURRENT"}, [["targetLagSignal"]]),
    _notice_case("position", "analyze_position_signals", {"technicalInputs": {"maPosition": "BULLISH"}},
                 [["analystInputs.analystNetSentiment", "earningsInputs.momentumFlag", "technicalInputs.maPosition"]]),
    _notice_case("price_target", "calculate_price_target_distance", {"ioPt": 200, "eqfPct": 95.0}, [["ioPt", "eqfPct", "invertedFlag"]]),
    _notice_case("hedge_iv_flag", "find_put_hedge_candidates", {"candidates": [{"strike": 90, "ivFlag": None}]},
                 [["candidates[].ivFlag"]]),
    _notice_case("hedge_no_candidates", "find_put_hedge_candidates", {"candidates": []}, []),
    _notice_case("other_tool_with_same_key", "get_market_quote", {"trend": "x", "bracket": "y"}, []),
    # Messages and replacement text, verbatim in both runtimes.
    Case(name="labels3110.notice.full_rows", ids=["NL3110"],
         py=lambda: deprecation_warnings_for("analyze_credit_health", {"debtTier": "CLEAN"})
         + deprecation_warnings_for("get_short_momentum", {"flag": None}),
         ts={"js": "return [...m.deprecations.deprecationWarningsFor('analyze_credit_health', {debtTier: 'CLEAN'}), "
                   "...m.deprecations.deprecationWarningsFor('get_short_momentum', {flag: null})];"},
         expect=[
             {"code": "DEPRECATED_FIELD",
              "message": f"debtTier, creditStressFlag are deprecated and {REMOVAL}; use leverageBand (the netDebtToEbitda band) "
                         "and meetsCreditStressThreshold (set with max_net_debt_to_ebitda and min_interest_coverage).",
              "severity": "info", "fields": ["debtTier", "creditStressFlag"],
              "replacement": "leverageBand, meetsCreditStressThreshold"},
             {"code": "DEPRECATED_FIELD",
              "message": f"squeezeRisk, flag are deprecated and {REMOVAL}; they have no replacement label; read shortPctFloat and daysToCover.",
              "severity": "info", "fields": ["squeezeRisk", "flag"], "replacement": "shortPctFloat, daysToCover"},
         ]),
]


# End to end with the existing provider stubs (default thresholds).
CASES += [
    _credit_case("labels3110.credit.card_example_new_fields", ["NL3110", "BR-FND-007"],
                 {"debt": [410 * M], "cash": [100 * M]}, {"op": [15 * M] * 4, "da": [10 * M] * 4, "int": [6.25 * M] * 4},
                 ("creditStressFlag", "debtTier", "leverageBand", "meetsCreditStressThreshold", "creditStressThresholds"),
                 expect={"creditStressFlag": True, "debtTier": "ELEVATED", "leverageBand": "2_5X_TO_4X",
                         "meetsCreditStressThreshold": True,
                         "creditStressThresholds": {"maxNetDebtToEbitda": 2.5, "minInterestCoverage": 3}}),
    _momentum_case("labels3110.momentum.beat_rate_band", ["NL3110"], [BEAT, BEAT, MISS, BEAT],
                   ("beatRate", "beatRateBand", "historicalSurpriseSignal", "revisionThresholdPct"),
                   expect={"beatRate": 0.75, "beatRateBand": "75_TO_100PCT", "historicalSurpriseSignal": "STRONG",
                           "revisionThresholdPct": 3}),
    _position_case("labels3110.position.new_fields_and_notice",
                   lambda v: {"maAlignment": "maAlignment" in v["data"]["technicalInputs"],
                              "analystNetUpgrades": "analystNetUpgrades" in v["data"]["analystInputs"],
                              "revisionBand": "revisionBand" in v["data"]["earningsInputs"],
                              "notices": [w["fields"] for w in v["meta"]["warnings"] if w.get("code") == "DEPRECATED_FIELD"]},
                   ids=("NL3110",),
                   expect={"maAlignment": True, "analystNetUpgrades": True, "revisionBand": True,
                           "notices": [["analystInputs.analystNetSentiment", "earningsInputs.momentumFlag",
                                        "technicalInputs.maPosition"]]}),
]
