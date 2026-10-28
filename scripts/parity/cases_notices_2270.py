"""Parity cases for the DEPRECATED_FIELD notices after 3.0.0 (originally the 2.27.0 notice-only release, F61).

Each case gives the same payload to the DEPRECATED_FIELD builder of both runtimes (Python
``deprecation_warnings_for``, Worker ``deprecationWarningsFor``). Nothing in ``data`` changes.

- F20: a ``confidence`` holding a status string, at any depth, gets one notice in both runtimes. It is the only
  notice left in 3.0.0.
- The 2.27.0 notices were removed with their fields: the options-flow aliases and rating-change ``*30d`` copies (F61),
  ``meta.decisionGrade`` (F5/F45), Python's yfinance holder columns (F17) and calendar keys (F47). A payload that still
  names them gets no notice in either runtime.
"""

from __future__ import annotations

import json

from scripts.parity.harness import Case
from yfmcp.deprecations import deprecation_warnings_for

CASES: list[Case] = []

REMOVAL = "will be removed in the next major version"


def _notice(fields: list[str], replacement: str, message: str) -> dict:
    return {"code": "DEPRECATED_FIELD", "message": message, "severity": "info", "fields": fields, "replacement": replacement}


CONFIDENCE_NOTICE = _notice(
    ["confidence"], "status",
    "confidence values other than HIGH, MEDIUM and LOW (status strings such as EXTRACTION_FAILED or NOT_DISCLOSED) are deprecated "
    f"and {REMOVAL}; confidence will hold only HIGH, MEDIUM or LOW, and a status field names the limitation.")


def _case(name: str, ids: list[str], tool: str, data, expect=None, *, evidence_only=False, **kw) -> Case:
    return Case(
        name=name, ids=ids,
        py=lambda: deprecation_warnings_for(tool, json.loads(json.dumps(data)), evidence_only=evidence_only),
        ts={"js": "return m.deprecations.deprecationWarningsFor(args[0], args[1]);",
            "args": [tool, data]},
        **({"expect": expect} if expect is not None else {}), **kw)


_RATING = {"ticker": "AAPL", "windowDays": 30, "netSentiment": 1, "upgrades": 2, "upgrades30d": 2, "downgrades": 1,
           "downgrades30d": 1, "initiations": 0, "initiations30d": 0, "changes": [], "summary": "2 UPGRADE(s), 1 DOWNGRADE(s)"}

CASES += [
    # F20: status strings in confidence, nested as the extractors nest them.
    _case("notices2270.confidence.status_string_nested", ["F20", "F61"], "extract_exposure",
          {"revenueExposure": {"status": "NOT_FOUND", "confidence": "FILING_NOT_FOUND_TRY_OTHER_TYPE"}}, [CONFIDENCE_NOTICE]),
    _case("notices2270.confidence.status_string_in_a_list", ["F20", "F61"], "extract_earnings_metrics",
          {"metrics": [{"name": "revenue", "confidence": "NOT_DISCLOSED"}]}, [CONFIDENCE_NOTICE]),
    _case("notices2270.confidence.top_level_status_string", ["F20", "F61"], "extract_geographic_revenue",
          {"value": None, "confidence": "EXTRACTION_FAILED"}, [CONFIDENCE_NOTICE]),
    _case("notices2270.confidence.levels_only_no_notice", ["F20", "F61"], "extract_geographic_revenue",
          {"value": 1, "confidence": "HIGH", "evidence": [{"confidence": "MEDIUM"}, {"confidence": "LOW"}]}, []),
    _case("notices2270.confidence.non_string_ignored", ["F20", "F61"], "get_market_quote", {"confidence": None, "x": {"confidence": 0.9}}, []),
    # 3.0.0: the F61 aliases and *30d copies are removed with their notices; a payload that still names them gets none.
    _case("notices2270.options_flow.removed_aliases_get_no_notice", ["F61"], "analyze_options_flow_window",
          {"ticker": "AAPL", "ivVsRealizedRangePct": 40, "putVolPer1PctStockAdv": 0.5, "ivPctile": 40, "putVolVs10dAvg": 0.5}, []),
    _case("notices2270.options_flow.other_tool_keeps_its_ivpctile", ["F61"], "get_put_hedge_candidates",
          {"candidates": [{"ivPctile": 40}], "ivPctile": 40}, []),
    _case("notices2270.rating_changes.window_copies_get_no_notice", ["F61"], "get_analyst_rating_changes", _RATING, []),
    _case("notices2270.rating_changes.batch_gets_no_notice", ["F61"], "get_analyst_rating_changes", {"AAPL": _RATING, "MSFT": _RATING}, []),
    # The confidence notice alone, whatever else the payload names.
    _case("notices2270.order.only_the_confidence_notice_remains", ["F20", "F61"], "analyze_options_flow_window",
          {"ivPctile": 1, "quality": {"confidence": "NOT_DECISION_GRADE"}}, [CONFIDENCE_NOTICE]),
    # F5/F45: meta.decisionGrade is gone from the Worker's meta; the builder takes no meta fields any more.
    _case("notices2270.meta_decision_grade.no_notice", ["F5", "F45", "F61"], "extract_sec_filing_fact",
          {"value": 1, "decisionGrade": True}, []),
    # F17 / F47: Python's yfinance-shaped payloads were replaced by the Worker's shapes; neither gets a notice.
    _case("notices2270.holders.yfinance_columns_get_no_notice", ["F17", "F61"], "get_ownership_holders",
          [{"Date Reported": "2026-06-30", "Holder": "Vanguard", "pctHeld": 0.09, "Shares": 1, "Value": 2}], []),
    _case("notices2270.holders.camel_case_rows_no_notice", ["F17", "F61"], "get_ownership_holders",
          {"ownershipList": [{"organization": "Vanguard", "value": 2}]}, []),
    _case("notices2270.calendar.yfinance_keys_get_no_notice", ["F47", "F61"], "get_company_events_calendar",
          {"ticker": "AAPL", "calendar": {"Dividend Date": "2026-11-13", "Earnings Date": ["2027-01-29"], "Earnings High": 2.6}}, []),
    _case("notices2270.calendar.worker_keys_no_notice", ["F47", "F61"], "get_company_events_calendar",
          {"ticker": "AAPL", "calendar": {"earnings": {"earningsDate": ["2027-01-29"]}, "dividendDate": None, "exDividendDate": None}}, []),
]
