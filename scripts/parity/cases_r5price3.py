"""Cases for R5-pricing part 3 (3.2.0): analyze_position_signals, Q3i calls 1 to 4 and G6.

- Call 4: a ticker list is refused with INPUT_VALIDATION_ERROR through the grouped route (as it already was) and through
  each runtime's single tool, where the Worker used to batch it and Python passed it to its components.
- Call 1: a component that fails makes the result PARTIAL, named in failedComponents under Python's six keys.
- Q3d: with no completed-bar date from the technicals or the moving averages, dataDate is null, never today.
- 3.1.0 rows: a component's notices reach meta.warnings once, even when two components raise the same row
  (uniqueNotices / unique_notices; the rows reaching meta.warnings end to end are in cases_neutrality.py).

Calls 2 and 3 and G6 (quoteTimestamp, macd, quoteDataDate) are pinned in cases_neutrality.py.
"""

from __future__ import annotations

import asyncio
import json

from scripts.parity.harness import Case
from scripts.parity.cases_neutrality import POSITION_ROUTES, _position_case
from scripts.parity.cases_pricing_evidence_tools import GATE_NOW, _ts_with_clock
import server

CASES: list[Case] = []
add = CASES.append

_LIST = ["AAA", "BBB"]


def _py_list(via: str):
    from yfmcp.app import build_handler_registry
    from yfmcp.envelope import _envelope_tool_result
    import tool_groups

    def run():
        if via == "grouped":
            return asyncio.run(tool_groups._route_grouped_call("screening", "analyze_position_signals", {"ticker": _LIST},
                                                               build_handler_registry(server.yfinance_server)))
        return _envelope_tool_result("analyze_position_signals", asyncio.run(server.analyze_position_signals(_LIST)))
    return run


def _ts_list(via: str) -> str:
    if via == "grouped":
        body = ('m.response.setWorkerEnv({}); const r = await m.tools.callVisibleToolResult("screening", '
                f'{{action: "analyze_position_signals", params: {{ticker: {json.dumps(_LIST)}}}}}); '
                'return r.value ?? JSON.parse(r.text);')
    else:
        body = (f'm.response.setWorkerEnv({{}}); return (await m.tools.callToolResult("analyze_position_signals", '
                f'{{ticker: {json.dumps(_LIST)}}})).value;')
    return _ts_with_clock(GATE_NOW, body)


_LIST_MESSAGES = {
    "grouped": "Invalid params for 'analyze_position_signals': invalid parameter value(s): ticker.",
    "single": "analyze_position_signals takes one ticker as a string, not a list.",
}
for _via in ("grouped", "single"):
    add(Case(name=f"r5price3.list.{_via}_ticker_list_is_refused", ids=["Q14-N3", "BR-ENV-007"],
             py=_py_list(_via), ts={"js": _ts_list(_via), "isolate": True}, routes=POSITION_ROUTES,
             select=lambda v: {"ok": v["ok"], "data": v["data"], "code": v["error"]["code"], "message": v["error"]["message"]},
             expect={"ok": False, "data": None, "code": "INPUT_VALIDATION_ERROR", "message": _LIST_MESSAGES[_via]},
             note="R5-pricing part 3 (Q3i call 4): analyze_position_signals takes one ticker"))

# ── Failed components ───────────────────────────────────────────────────────

_FAILED = {"error": True, "message": "stubbed failure"}
_CHART_DOWN = {**POSITION_ROUTES, "https://query1.finance.yahoo.com/v8/finance/chart/*": {"status": 500, "body": "down"}}


def _failed_view(v: dict) -> dict:
    d = v["data"]
    return {"status": d["status"], "componentStatus": d["componentStatus"], "failedComponents": d["failedComponents"],
            "limitedComponents": d["limitedComponents"], "recommendedNextAction": d["recommendedNextAction"],
            "dataDate": d["dataDate"], "completedBarDataDate": d["completedBarDataDate"], "quoteDataDate": d["quoteDataDate"],
            "currentPrice": d["pricePositionInputs"]["currentPrice"]}


add(_position_case(
    "r5price3.components.chart_outage_fails_the_bar_components", _failed_view, ids=("Q14-N3",),
    components={"get_price_stats": _FAILED, "get_technical_indicators": _FAILED},
    routes=_CHART_DOWN,
    expect={"status": "PARTIAL",
            "componentStatus": {"analystUpgrades": "OK", "analystConsensus": "OK", "priceStats": "FAILED",
                                "earningsMomentum": "OK", "technicalIndicators": "FAILED", "maPosition": "OK"},
            "failedComponents": ["priceStats", "technicalIndicators"], "limitedComponents": [],
            "recommendedNextAction": "RETRY", "dataDate": "2026-10-09", "completedBarDataDate": None,
            "quoteDataDate": None, "currentPrice": None},
    note="R5-pricing part 3: a chart outage fails two components; the Worker used to fail the whole tool"))

_ALL_DOWN = {**_CHART_DOWN, "https://query1.finance.yahoo.com/v10/finance/quoteSummary/*": {"status": 500, "body": "down"}}
_SIX = ["analystUpgrades", "analystConsensus", "priceStats", "earningsMomentum", "technicalIndicators", "maPosition"]
_PY_FUNCS = ["get_analyst_upgrade_radar", "get_analyst_consensus", "get_price_stats", "get_earnings_momentum",
             "get_technical_indicators", "get_ma_position"]

add(_position_case(
    "r5price3.components.total_outage_is_partial_with_every_component_failed_and_no_date", _failed_view,
    ids=("Q14-N3", "BR-PRC-040"), components={f: _FAILED for f in _PY_FUNCS}, routes=_ALL_DOWN,
    expect={"status": "PARTIAL", "componentStatus": {k: "FAILED" for k in _SIX}, "failedComponents": _SIX,
            "limitedComponents": [], "recommendedNextAction": "RETRY", "dataDate": None, "completedBarDataDate": None,
            "quoteDataDate": None, "currentPrice": None},
    note="R5-pricing part 3 (Q3d): with no bar date dataDate is null; before, Python gave the last weekday and the Worker today"))

# ── Component rows ──────────────────────────────────────────────────────────

_CAL = {"code": "SESSION_CALENDAR_UNKNOWN", "ticker": "TST",
        "message": "No holiday calendar covers this exchange and date, so the previous session could not be named and freshnessStatus is UNKNOWN."}


_ROWS = [_CAL, {"code": "INSUFFICIENT_HISTORY", "ticker": "TST", "message": "m3"}, dict(_CAL),
         {"code": "INSUFFICIENT_HISTORY", "ticker": "TST", "message": "m5"}, {**_CAL, "ticker": "OTHER"}]
add(Case(name="r5price3.notices.unique_notices_drop_repeats_in_first_seen_order", ids=["BR-ENV-001"],
         py=lambda: __import__("yfmcp.pricing_notices", fromlist=["unique_notices"]).unique_notices([dict(r) for r in _ROWS]),
         ts={"js": "return m.pricingNotices.uniqueNotices(args[0]);", "args": [_ROWS]},
         expect=[_ROWS[0], _ROWS[1], _ROWS[3], _ROWS[4]],
         note="R5-pricing part 3: a row repeated by two components is kept once; another ticker's row is not a repeat"))
