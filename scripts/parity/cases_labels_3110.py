"""Parity cases for the neutrality replacement labels (3.11.0, audit NL1-NL13,
/mnt/project-files/audits/neutrality-labels-2026-11-12.md; docs/provider-runtime-guidance.md
"Neutrality Label Notices (3.11.0)"). The DEPRECATED_FIELD notices and the deprecated fields went in 4.0.0
(cases_major_400.py).

- The replacement labels (yfmcp/labels.py, worker/src/labels.ts) at every band boundary.
- End to end: the new fields in analyze_credit_health and analyze_earnings_momentum, and the replacement fields of
  analyze_position_signals.
"""

from __future__ import annotations

from scripts.parity.harness import Case
from scripts.parity.cases_neutrality import _position_case
from scripts.parity.cases_pricing_evidence_tools import M, BEAT, MISS, _credit_case, _momentum_case
from yfmcp import labels

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


# End to end with the existing provider stubs (default thresholds).
CASES += [
    _credit_case("labels3110.credit.card_example_new_fields", ["NL3110", "BR-FND-007"],
                 {"debt": [410 * M], "cash": [100 * M]}, {"op": [15 * M] * 4, "da": [10 * M] * 4, "int": [6.25 * M] * 4},
                 ("leverageBand", "meetsCreditStressThreshold", "creditStressThresholds"),
                 expect={"leverageBand": "2_5X_TO_4X",
                         "meetsCreditStressThreshold": True,
                         "creditStressThresholds": {"maxNetDebtToEbitda": 2.5, "minInterestCoverage": 3}}),
    _momentum_case("labels3110.momentum.beat_rate_band", ["NL3110"], [BEAT, BEAT, MISS, BEAT],
                   ("beatRate", "beatRateBand", "revisionThresholdPct"),
                   expect={"beatRate": 0.75, "beatRateBand": "75_TO_100PCT", "revisionThresholdPct": 3}),
    _position_case("labels3110.position.new_fields",
                   lambda v: {"maAlignment": "maAlignment" in v["data"]["technicalInputs"],
                              "analystNetUpgrades": "analystNetUpgrades" in v["data"]["analystInputs"],
                              "revisionBand": "revisionBand" in v["data"]["earningsInputs"]},
                   ids=("NL3110",),
                   expect={"maAlignment": True, "analystNetUpgrades": True, "revisionBand": True}),
]
