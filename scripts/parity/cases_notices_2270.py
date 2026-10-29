"""Parity cases for confidence levels (3.0.0, F20; originally the 2.27.0 notice-only release, F61).

3.0.0 removed the DEPRECATED_FIELD notices. ``confidence`` now holds only HIGH, MEDIUM or LOW, or null: every other string
becomes null, and moves to ``status`` when the object has none. Both runtimes apply the same rule to a payload
(Python ``normalize_status_confidence``, Worker ``normalizeStatusConfidence``), at any depth up to 64.

- The direct cases give one payload to both functions and compare the rewritten payload.
- The boundary cases give a payload that used to draw a notice (F20, F61, F17, F47, F5/F45) to each runtime's
  tool-boundary entry point and expect no DEPRECATED_FIELD warning, with ``data`` as the rule leaves it.
"""

from __future__ import annotations

import json

from scripts.parity.harness import Case
from yfmcp.envelope import _envelope_tool_result, normalize_status_confidence

CASES: list[Case] = []


def _case(name: str, ids: list[str], data, expect) -> Case:
    return Case(
        name=name, ids=ids,
        py=lambda: normalize_status_confidence(json.loads(json.dumps(data))),
        ts={"js": "return m.response.normalizeStatusConfidence(args[0]);", "args": [data]},
        expect=expect)


def _deep(levels: int, leaf: dict) -> dict:
    node = leaf
    for _ in range(levels):
        node = {"child": node}
    return node


def _walk(node: dict, levels: int) -> dict:
    for _ in range(levels):
        node = node["child"]
    return node


CASES += [
    # A status string in confidence, nested as the extractors nest it: confidence is null, and the object's own status stays.
    _case("notices2270.confidence.status_string_nested", ["F20"],
          {"revenueExposure": {"status": "NOT_FOUND", "confidence": "FILING_NOT_FOUND_TRY_OTHER_TYPE"}},
          {"revenueExposure": {"status": "NOT_FOUND", "confidence": None}}),
    _case("notices2270.confidence.status_string_in_a_list_moves_to_status", ["F20"],
          {"metrics": [{"name": "revenue", "confidence": "NOT_DISCLOSED"}]},
          {"metrics": [{"name": "revenue", "confidence": None, "status": "NOT_DISCLOSED"}]}),
    _case("notices2270.confidence.top_level_status_string", ["F20"],
          {"value": None, "confidence": "EXTRACTION_FAILED"},
          {"value": None, "confidence": None, "status": "EXTRACTION_FAILED"}),
    _case("notices2270.confidence.existing_status_is_kept_even_when_null", ["F20"],
          {"status": None, "confidence": "NOT_DECISION_GRADE"}, {"status": None, "confidence": None}),
    _case("notices2270.confidence.empty_string_becomes_null_without_status", ["F20"],
          {"confidence": ""}, {"confidence": None}),
    _case("notices2270.confidence.lower_case_level_is_not_a_level", ["F20"],
          {"confidence": "high"}, {"confidence": None, "status": "high"}),
    _case("notices2270.confidence.levels_only_unchanged", ["F20"],
          {"value": 1, "confidence": "HIGH", "evidence": [{"confidence": "MEDIUM"}, {"confidence": "LOW"}]},
          {"value": 1, "confidence": "HIGH", "evidence": [{"confidence": "MEDIUM"}, {"confidence": "LOW"}]}),
    _case("notices2270.confidence.non_string_ignored", ["F20"],
          {"confidence": None, "x": {"confidence": 0.9}}, {"confidence": None, "x": {"confidence": 0.9}}),
    _case("notices2270.confidence.inner_object_of_a_status_string_object_is_walked", ["F20"],
          {"confidence": "PARTIAL", "inner": {"confidence": "NOT_FOUND"}},
          {"confidence": None, "status": "PARTIAL", "inner": {"confidence": None, "status": "NOT_FOUND"}}),
    _case("notices2270.confidence.depth_64_is_walked", ["F20"],
          _deep(64, {"confidence": "X"}), _deep(64, {"confidence": None, "status": "X"})),
    _case("notices2270.confidence.depth_65_is_left_alone", ["F20"],
          _deep(65, {"confidence": "X"}), _deep(65, {"confidence": "X"})),
]


def _boundary_case(name: str, ids: list[str], tool: str, data, expect_data) -> Case:
    return Case(
        name=name, ids=ids,
        py=lambda: json.loads(_envelope_tool_result(tool, json.dumps(data))),
        ts={"js": "m.response.setWorkerEnv({}); return JSON.parse(m.response.mcpSuccess(args[0], JSON.stringify(args[1])));",
            "args": [tool, data]},
        select=lambda v: {"ok": v["ok"], "data": v["data"], "warnings": v["meta"]["warnings"]},
        expect={"ok": True, "data": expect_data, "warnings": []})


_RATING = {"ticker": "AAPL", "windowDays": 30, "netSentiment": 1, "upgrades": 2, "upgrades30d": 2, "downgrades": 1,
           "downgrades30d": 1, "initiations": 0, "initiations30d": 0, "changes": [], "summary": "2 UPGRADE(s), 1 DOWNGRADE(s)"}

CASES += [
    _boundary_case("notices2270.boundary.status_string_gets_no_notice", ["F20", "F61"], "extract_exposure",
                   {"revenueExposure": {"status": "NOT_FOUND", "confidence": "FILING_NOT_FOUND_TRY_OTHER_TYPE"}},
                   {"revenueExposure": {"status": "NOT_FOUND", "confidence": None}}),
    _boundary_case("notices2270.boundary.status_string_in_a_list_gets_no_notice", ["F20", "F61"], "extract_geographic_revenue",
                   {"segments": [{"name": "US", "confidence": "NOT_DISCLOSED"}]},
                   {"segments": [{"name": "US", "confidence": None, "status": "NOT_DISCLOSED"}]}),
    _boundary_case("notices2270.boundary.options_flow_removed_aliases_get_no_notice", ["F61"], "analyze_options_flow_window",
                   {"ticker": "AAPL", "ivPctile": 40, "quality": {"confidence": "NOT_DECISION_GRADE"}},
                   {"ticker": "AAPL", "ivPctile": 40, "quality": {"confidence": None, "status": "NOT_DECISION_GRADE"}}),
    _boundary_case("notices2270.boundary.rating_window_copies_get_no_notice", ["F61"], "get_analyst_rating_changes", _RATING, _RATING),
    _boundary_case("notices2270.boundary.holders_yfinance_columns_get_no_notice", ["F17", "F61"], "get_ownership_holders",
                   {"rows": [{"Date Reported": "2026-06-30", "Holder": "Vanguard", "pctHeld": 0.09}]},
                   {"rows": [{"Date Reported": "2026-06-30", "Holder": "Vanguard", "pctHeld": 0.09}]}),
    _boundary_case("notices2270.boundary.calendar_yfinance_keys_get_no_notice", ["F47", "F61"], "get_company_events_calendar",
                   {"ticker": "AAPL", "calendar": {"Dividend Date": "2026-11-13", "Earnings Date": ["2027-01-29"]}},
                   {"ticker": "AAPL", "calendar": {"Dividend Date": "2026-11-13", "Earnings Date": ["2027-01-29"]}}),
    _boundary_case("notices2270.boundary.levels_only_no_notice", ["F20", "F61"], "get_market_quote",
                   {"confidence": "HIGH", "evidence": [{"confidence": "LOW"}]}, {"confidence": "HIGH", "evidence": [{"confidence": "LOW"}]}),
]
