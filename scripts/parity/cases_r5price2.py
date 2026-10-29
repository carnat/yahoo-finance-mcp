"""Cases for R5-pricing-2 (3.1.0): the exchange holiday calendar and where the new notices reach the caller.

- Calendar property (BR-PRC-002): for every day of 2023 to 2029, both runtimes name the same previous US session as an
  independent oracle built from the NYSE holiday rules (observed New Year, MLK, Presidents, Good Friday, Memorial,
  Juneteenth, Independence, Labor, Thanksgiving, Christmas, plus the 2025-01-09 day of mourning). Dates whose previous
  session would fall outside 2024 to 2028, exchanges without a calendar and malformed dates give null.
- Envelope (owner card 2026-10-29): a tool's notices leave the payload and join meta.warnings, in the order the tool
  raised them, in both the V2 envelope and the grouped route. get_market_snapshot folds a component's notices into its
  own data.warnings with the component name.

The pricing rules themselves (zero volume, short-history CAGR, the open-session fallback) are pinned in
cases_pricing_evidence_tools.py.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import json
from unittest.mock import AsyncMock, MagicMock, patch

from scripts.parity.harness import Case
from scripts.parity.cases_pricing_evidence_tools import (
    PRIOR10,
    SERIES_NOW,
    _series_chart,
    _series_prepared,
    _ts_with_clock,
)
from yfmcp.exchange_calendars import previous_exchange_session
from yfmcp.tools import pricing

CASES: list[Case] = []
add = CASES.append

# ── Calendar oracle ─────────────────────────────────────────────────────────


def _easter(y: int) -> _dt.date:
    """Gregorian Easter Sunday (anonymous algorithm)."""
    a, b, c = y % 19, y // 100, y % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l_ = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l_) // 451
    return _dt.date(y, (h + l_ - 7 * m + 114) // 31, (h + l_ - 7 * m + 114) % 31 + 1)


def _nth(y: int, month: int, weekday: int, n: int) -> _dt.date:
    first = _dt.date(y, month, 1)
    return first + _dt.timedelta((weekday - first.weekday()) % 7 + 7 * (n - 1))


def _last(y: int, month: int, weekday: int) -> _dt.date:
    end = _dt.date(y, month + 1, 1) - _dt.timedelta(1)
    return end - _dt.timedelta((end.weekday() - weekday) % 7)


def _observed(day: _dt.date) -> _dt.date:
    return day - _dt.timedelta(1) if day.weekday() == 5 else day + _dt.timedelta(1) if day.weekday() == 6 else day


def _nyse_closures(y: int) -> set[_dt.date]:
    out = {_nth(y, 1, 0, 3), _nth(y, 2, 0, 3), _easter(y) - _dt.timedelta(2), _last(y, 5, 0),
           _observed(_dt.date(y, 6, 19)), _observed(_dt.date(y, 7, 4)), _nth(y, 9, 0, 1), _nth(y, 11, 3, 4),
           _observed(_dt.date(y, 12, 25))}
    if _dt.date(y, 1, 1).weekday() != 5:  # NYSE does not close the Friday before a Saturday New Year
        out.add(_observed(_dt.date(y, 1, 1)))
    if y == 2025:
        out.add(_dt.date(2025, 1, 9))  # national day of mourning
    return out


_CLOSED = set().union(*(_nyse_closures(y) for y in range(2024, 2029)))


def _oracle(iso: str) -> str | None:
    day = _dt.date.fromisoformat(iso)
    for _ in range(14):
        day -= _dt.timedelta(1)
        if not 2024 <= day.year <= 2028:
            return None
        if day.weekday() < 5 and day not in _CLOSED:
            return day.isoformat()
    return None


_DAYS = [(_dt.date(2023, 12, 1) + _dt.timedelta(n)).isoformat()
         for n in range((_dt.date(2029, 1, 31) - _dt.date(2023, 12, 1)).days + 1)]


def _calendar_mismatches(rows: list) -> dict:
    bad = [[d, got] for d, got in zip(_DAYS, rows) if got != _oracle(d)]
    return {"checked": len(rows), "mismatches": len(bad), "first": bad[:5],
            "named": sum(1 for r in rows if r is not None)}


_EXPECT_CAL = {"checked": len(_DAYS), "mismatches": 0, "first": [],
               "named": sum(1 for d in _DAYS if _oracle(d) is not None)}

for _code in ("NMS", "NYQ", "ASE", "PCX", "NGM", "NCM"):
    add(Case(name=f"r5price2.calendar.{_code.lower()}_previous_session_matches_the_nyse_rules", ids=["BR-PRC-002"],
             py=lambda code=_code: [previous_exchange_session(code, d) for d in _DAYS],
             ts={"js": "return args[1].map((d) => m.exchangeCalendars.previousExchangeSession(args[0], d));",
                 "args": [_code, _DAYS]},
             select=_calendar_mismatches, expect=_EXPECT_CAL,
             note="R5-pricing-2: a US listing's previous session skips weekends and NYSE full closures"))

# The walk back stops at the first day outside the covered years, even when a covered session lies beyond it.
_EDGE_INPUTS = [
    ["NMS", "2024-01-02"],   # previous weekday is the 2024 New Year closure; the session before is in 2023
    ["NMS", "2024-01-03"],   # 2024-01-02
    ["NMS", "2028-12-29"],   # 2028-12-28
    ["NMS", "2029-01-02"],   # stepping back reaches 2029-01-01, outside the covered years
    ["NMS", "2028-12-30"],   # Saturday -> 2028-12-29
    ["NMS", "2025-01-10"],   # day of mourning -> 2025-01-08
    ["NMS", "2026-09-08"],   # Labor Day Monday -> Friday 2026-09-04
    ["NMS", "2027-12-27"],   # Christmas observed Friday 2027-12-24 -> 2027-12-23
    ["NMS", "2028-01-03"],   # Saturday New Year 2028 is not observed -> Friday 2027-12-31
    ["JPX", "2026-10-09"],   # no calendar
    [None, "2026-10-09"],
    ["NMS", "2026-10-9"],    # not YYYY-MM-DD
    ["NMS", "2026-02-30"],   # not a date
    ["NMS", None],
]
add(Case(name="r5price2.calendar.boundaries_and_unknown_inputs", ids=["BR-PRC-002"],
         py=lambda: [previous_exchange_session(code, d) for code, d in _EDGE_INPUTS],
         ts={"js": "return args[0].map(([c, d]) => m.exchangeCalendars.previousExchangeSession(c, d));",
             "args": [_EDGE_INPUTS]},
         expect=[None, "2024-01-02", "2028-12-28", None, "2028-12-29", "2025-01-08", "2026-09-04", "2027-12-23",
                 "2027-12-31", None, None, None, None, None],
         note="R5-pricing-2: outside 2024-2028, without a calendar or with a malformed date there is no named session"))

# ── Notices in the envelope ─────────────────────────────────────────────────

_ZERO_VOLUME = {"code": "ZERO_VOLUME_NO_RATIO", "ticker": "ZV",
                "message": "The latest completed session traded zero volume, so ratio10d, ratio90d and volumeFlag are null."}
_CLOSES = [50.0] * 11
_VOLUMES = PRIOR10 + [0]
_ROUTES = {"https://query1.finance.yahoo.com/v8/finance/chart/*": {"json": _series_chart(_CLOSES, _VOLUMES)}}


def _py_volume_ratio(via: str):
    from yfmcp.app import build_handler_registry
    from yfmcp.envelope import _envelope_tool_result
    import server
    import tool_groups

    async def call():
        if via == "envelope":
            return _envelope_tool_result("analyze_volume_ratio", await server.analyze_volume_ratio("ZV", 10))
        return await tool_groups._route_grouped_call("stock_pricing", "analyze_volume_ratio", {"ticker": "ZV", "period": 10},
                                                     build_handler_registry(server.yfinance_server))

    def run():
        with patch.object(pricing.yf, "Ticker", MagicMock()), patch.dict(pricing._tool_cache._store, {}, clear=True), \
                patch.object(pricing, "_load_completed_daily_history",
                             AsyncMock(return_value=_series_prepared(_CLOSES, _VOLUMES))):
            return asyncio.run(call())
    return run


def _ts_volume_ratio(via: str) -> str:
    args = json.dumps({"ticker": "ZV", "period": 10})
    if via == "envelope":
        body = f'm.response.setWorkerEnv({{}}); return (await m.tools.callToolResult("analyze_volume_ratio", {args})).value;'
    else:
        body = (f'm.response.setWorkerEnv({{}}); const r = await m.tools.callVisibleToolResult("stock_pricing", '
                f'{{action: "analyze_volume_ratio", params: {args}}}); return r.value ?? JSON.parse(r.text);')
    return _ts_with_clock(SERIES_NOW, body)


def _envelope_view(v: dict) -> dict:
    meta = v.get("meta") or {}
    return {"ok": v.get("ok"), "ratio10d": (v.get("data") or {}).get("ratio10d"),
            "payloadKey": "_warnings" in json.dumps(v.get("data")),
            "notices": [w for w in meta.get("warnings") or [] if w.get("code") == "ZERO_VOLUME_NO_RATIO"]}


for _via in ("envelope", "grouped"):
    add(Case(name=f"r5price2.envelope.{_via}_zero_volume_notice_moves_to_meta_warnings", ids=["BR-PRC-033", "BR-ENV-001"],
             py=_py_volume_ratio(_via), ts={"js": _ts_volume_ratio(_via), "isolate": True}, routes=_ROUTES,
             select=_envelope_view,
             expect={"ok": True, "ratio10d": None, "payloadKey": False, "notices": [_ZERO_VOLUME]},
             note="R5-pricing-2: the caller reads the notice in meta.warnings; data carries no _warnings key"))


def _py_lift():
    from yfmcp.pricing_notices import lift_payload_warnings
    data = {"A": {"x": 1, "_warnings": [{"code": "C1"}]}, "B": {"_warnings": [{"code": "C2"}, {"code": "C3"}]},
            "_warnings": [{"code": "C0"}], "deep": {"one": {"two": {"_warnings": [{"code": "TOO_DEEP"}]}}}}
    return {"lifted": lift_payload_warnings(data), "left": data}


add(Case(name="r5price2.envelope.lift_takes_top_and_per_ticker_notices_in_payload_order", ids=["BR-ENV-001"],
         py=_py_lift,
         ts={"js": ('const data = {A: {x: 1, _warnings: [{code: "C1"}]}, B: {_warnings: [{code: "C2"}, {code: "C3"}]},'
                    ' _warnings: [{code: "C0"}], deep: {one: {two: {_warnings: [{code: "TOO_DEEP"}]}}}};'
                    ' return {lifted: m.pricingNotices.liftPayloadWarnings(data), left: data};')},
         expect={"lifted": [{"code": "C0"}, {"code": "C1"}, {"code": "C2"}, {"code": "C3"}],
                 "left": {"A": {"x": 1}, "B": {}, "deep": {"one": {"two": {"_warnings": [{"code": "TOO_DEEP"}]}}}}},
         note="R5-pricing-2: a batch result's per-ticker notices are lifted too; deeper keys are not payload notices"))


# ── Notices in get_market_snapshot ──────────────────────────────────────────


def _py_snapshot():
    async def call():
        failed = AsyncMock(return_value=json.dumps({"error": True, "message": "stubbed component"}))
        with patch.multiple(pricing, get_fast_info=failed, get_price_stats=failed, get_ma_position=failed,
                            get_technical_indicators=failed):
            return await pricing.get_market_snapshot("ZV", "compact", False, None)

    with patch.object(pricing.yf, "Ticker", MagicMock()), patch.dict(pricing._tool_cache._store, {}, clear=True), \
            patch.object(pricing, "_load_completed_daily_history",
                         AsyncMock(return_value=_series_prepared(_CLOSES, _VOLUMES))):
        return asyncio.run(call())


def _snapshot_view(v: dict) -> dict:
    return {"notices": [w for w in v.get("warnings") or [] if w.get("code") == "ZERO_VOLUME_NO_RATIO"],
            "payloadKey": "_warnings" in json.dumps(v)}


# Python stubs every component but the volume ratio; the Worker builds its components from the same chart, and the
# view keeps only the zero-volume notice, so the other components' outcome does not matter here.
add(Case(name="r5price2.snapshot.component_notice_joins_snapshot_warnings", ids=["BR-PRC-033", "BR-PRC-020"],
         py=_py_snapshot,
         ts={"js": _ts_with_clock(SERIES_NOW, 'return JSON.parse(await m.yahooFinance.getMarketSnapshot("ZV", "compact", false, null));'),
             "isolate": True},
         routes=_ROUTES, select=_snapshot_view,
         expect={"notices": [{"code": "ZERO_VOLUME_NO_RATIO", "component": "volumeRatio",
                              "message": _ZERO_VOLUME["message"]}], "payloadKey": False},
         note="R5-pricing-2: a snapshot names the component whose notice it carries"))
