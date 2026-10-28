"""Parity cases for the 2.27.0 notice-only release (F61; analysis/yahoo-finance-mcp/F_TRIAGE.md §7).

Each case gives the same payload to the DEPRECATED_FIELD builder of both runtimes (Python
``deprecation_warnings_for``, Worker ``deprecationWarningsFor``). Nothing in ``data`` changes.

- F20: a ``confidence`` holding a status string, at any depth, gets one notice in both runtimes.
- F61 aliases: analyze_options_flow_window ``ivPctile`` / ``putVolVs10dAvg``; get_analyst_rating_changes'
  ``*30d`` copies (null for any window other than 30 days).
- F5/F45: ``meta.decisionGrade`` is in the Worker's meta only, so only the Worker gives its notice (pinned).
- F17 / F47: Python's yfinance holder columns and calendar keys are in Python's payload only, so only Python gives
  those notices (pinned).
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
OPTIONS_NOTICE = _notice(
    ["ivPctile", "putVolVs10dAvg"], "ivVsRealizedRangePct, putVolPer1PctStockAdv",
    f"ivPctile, putVolVs10dAvg are deprecated and {REMOVAL}; use ivVsRealizedRangePct, putVolPer1PctStockAdv.")
RATING_NOTICE = _notice(
    ["upgrades30d", "downgrades30d", "initiations30d"], "upgrades, downgrades, initiations",
    f"upgrades30d, downgrades30d, initiations30d are deprecated and {REMOVAL}; use upgrades, downgrades, initiations, "
    "which count the windowDays window.")
META_GRADE_NOTICE = _notice(
    ["meta.decisionGrade"], "decisionGrade",
    f"meta.decisionGrade is deprecated and {REMOVAL}; use the payload's decisionGrade, which is set per call.")


def _case(name: str, ids: list[str], tool: str, data, expect=None, *, meta_fields=(), evidence_only=False, **kw) -> Case:
    return Case(
        name=name, ids=ids,
        py=lambda: deprecation_warnings_for(tool, json.loads(json.dumps(data)), evidence_only=evidence_only),
        ts={"js": "return m.deprecations.deprecationWarningsFor(args[0], args[1], {evidenceOnly: args[2], metaFields: args[3]});",
            "args": [tool, data, evidence_only, list(meta_fields)]},
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
    # F61 aliases.
    _case("notices2270.options_flow.aliases", ["F61"], "analyze_options_flow_window",
          {"ticker": "AAPL", "ivVsRealizedRangePct": 40, "putVolPer1PctStockAdv": 0.5, "ivPctile": 40, "putVolVs10dAvg": 0.5}, [OPTIONS_NOTICE]),
    _case("notices2270.options_flow.other_tool_keeps_its_ivpctile", ["F61"], "get_put_hedge_candidates",
          {"candidates": [{"ivPctile": 40}], "ivPctile": 40}, []),
    _case("notices2270.rating_changes.window_copies", ["F61"], "get_analyst_rating_changes", _RATING, [RATING_NOTICE]),
    _case("notices2270.rating_changes.batch", ["F61"], "get_analyst_rating_changes", {"AAPL": _RATING, "MSFT": _RATING}, [RATING_NOTICE]),
    # Both a tool notice and the confidence notice, in that order.
    _case("notices2270.order.tool_notice_then_confidence", ["F20", "F61"], "analyze_options_flow_window",
          {"ivPctile": 1, "quality": {"confidence": "NOT_DECISION_GRADE"}}, [OPTIONS_NOTICE, CONFIDENCE_NOTICE]),
    # F5/F45: the Worker's meta carries the status table's decisionGrade; Python's meta has none (pinned).
    _case("notices2270.meta_decision_grade.worker_meta_only", ["F5", "F45", "F61"], "extract_sec_filing_fact", {"value": 1, "decisionGrade": True},
          meta_fields=["capabilityStatus", "decisionGrade"], mode="pinned", decision="F5-WORKER-META-ONLY",
          expect_py=[], expect_ts=[META_GRADE_NOTICE]),
    # F17 / F47: Python's yfinance-shaped payloads (pinned; the Worker never returns these keys).
    _case("notices2270.holders.python_yfinance_columns", ["F17", "F61"], "get_ownership_holders",
          [{"Date Reported": "2026-06-30", "Holder": "Vanguard", "pctHeld": 0.09, "Shares": 1, "Value": 2}],
          mode="pinned", decision="F17-PYTHON-SHAPE-ONLY", expect_ts=[], expect_py=[_notice(
              ["Date Reported", "Holder", "Shares", "Value"], "the Worker's quoteSummary rows (camelCase keys)",
              f"Date Reported, Holder, Shares, Value are deprecated and {REMOVAL}; rows take the Worker's quoteSummary rows (camelCase keys).")]),
    _case("notices2270.holders.camel_case_rows_no_notice", ["F17", "F61"], "get_ownership_holders",
          {"ownershipList": [{"organization": "Vanguard", "value": 2}]}, []),
    _case("notices2270.calendar.python_yfinance_keys", ["F47", "F61"], "get_company_events_calendar",
          {"ticker": "AAPL", "calendar": {"Dividend Date": "2026-11-13", "Earnings Date": ["2027-01-29"], "Earnings High": 2.6}},
          mode="pinned", decision="F47-PYTHON-SHAPE-ONLY", expect_ts=[], expect_py=[_notice(
              ["calendar.Dividend Date", "calendar.Earnings Date", "calendar.Earnings High"],
              "calendar.earnings.earningsDate, calendar.earnings.*, calendar.dividendDate, calendar.exDividendDate",
              f"calendar.Dividend Date, calendar.Earnings Date, calendar.Earnings High are deprecated and {REMOVAL}; use "
              "calendar.earnings.earningsDate, calendar.earnings.*, calendar.dividendDate, calendar.exDividendDate.")]),
    _case("notices2270.calendar.worker_keys_no_notice", ["F47", "F61"], "get_company_events_calendar",
          {"ticker": "AAPL", "calendar": {"earnings": {"earningsDate": ["2027-01-29"]}, "dividendDate": None, "exDividendDate": None}}, []),
]
