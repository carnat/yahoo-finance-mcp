"""Parity cases: pricing, evidence, and tool-level behaviour (modernization Phase 2).

Covers the P0 rules BR-PRC-001/002/003, BR-EVD-001..005 and BR-SHR-001, every repair-scheduled rule in the
pricing / fundamentals / providers / news / evidence / envelope / shared domains (pinned to today's behaviour), and
the three owner-decision behaviours Q3a, Q3h and Q4.

Conventions
- `equal` cases carry `expect` taken from the rule card, or from the code where both runtimes agree and the card is
  wrong (the comment says so).
- `pinned` cases record today's behaviour for a scheduled repair; the repair release flips them to `equal`.
- "Finding (Phase 2, for Q3i triage)" comments mark behaviour both runtimes share that looks wrong.
- Worker tool-level cases fix the clock by overriding `Date.now` inside the case body and use `isolate` so that
  module-level Yahoo caches start empty.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import re
import zoneinfo
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

from scripts.parity.harness import Case

# Registers the tools (server.py imports the yfmcp modules) the way the existing suites do.
import server  # noqa: F401,E402
from yfmcp.tools import news as news_tools  # noqa: E402
from yfmcp.tools import pricing  # noqa: E402

CASES: list[Case] = []

NY = "America/New_York"

# Anonymous-looking Yahoo cookie/crumb handshake the Worker performs before authenticated GETs.
YAHOO_AUTH_ROUTES = {
    "https://fc.yahoo.com": {"headers": {"set-cookie": "A3=fake-cookie; Path=/"}},
    "https://query1.finance.yahoo.com/v1/test/getcrumb": {"body": "fake-crumb"},
    "https://query2.finance.yahoo.com/v1/test/getcrumb": {"body": "fake-crumb"},
}


def _epoch(day: str, clock: str, tz: str) -> int:
    local = dt.datetime.fromisoformat(f"{day}T{clock}").replace(tzinfo=zoneinfo.ZoneInfo(tz))
    return int(local.timestamp())


def _weekdays(end: str, count: int) -> list[str]:
    day = dt.date.fromisoformat(end)
    out: list[str] = []
    while len(out) < count:
        if day.weekday() < 5:
            out.append(day.isoformat())
        day -= dt.timedelta(days=1)
    return list(reversed(out))


def _ts_with_clock(now_epoch: int, body: str) -> str:
    """Worker case body run with Date.now fixed at `now_epoch` seconds (restored afterwards)."""
    return (
        f"const realNow = Date.now; Date.now = () => {now_epoch} * 1000;\n"
        f"try {{ {body} }} finally {{ Date.now = realNow; }}"
    )


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-PRC-001 / BR-PRC-002: completed-session bar and daily price freshness
# Python: pricing._prepare_completed_daily_history. Worker: prepareCompletedDailySeries is private, so the
# Worker side runs getPriceSlope (days=2) on a canned Yahoo chart with a fixed clock; the same four fields
# are compared on both sides.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

SLOPE_FIELDS = ("expectedCompletedDate", "freshnessStatus", "excludedIncompleteBar", "latestAvailableBarDate")


def _bars(days: list[str], tz: str, closes: dict | None = None, null_close_days: tuple = ()) -> list[dict]:
    open_clock = "09:00" if tz == "Asia/Tokyo" else "09:30"
    out = []
    for index, day in enumerate(days):
        close = None if day in null_close_days else (closes or {}).get(day, 100.0 + index)
        out.append({"day": day, "open": _epoch(day, open_clock, tz), "close": close})
    return out


def _session_case(name: str, ids: list[str], *, days: list[str], now: tuple[str, str], session: tuple | None,
                  market_time: tuple | None, expect: dict, tz: str = NY, previous_close: float | None = None,
                  closes: dict | None = None, null_close_days: tuple = (), **kw) -> Case:
    meta: dict = {"exchangeTimezoneName": tz}
    if market_time:
        meta["regularMarketTime"] = _epoch(market_time[0], market_time[1], tz)
    if session:
        day, start, end = session
        meta["currentTradingPeriod"] = {"regular": {"start": _epoch(day, start, tz), "end": _epoch(day, end, tz)}}
    if previous_close is not None:
        meta["previousClose"] = previous_close
    bars = _bars(days, tz, closes, null_close_days)
    now_epoch = _epoch(now[0], now[1], tz)

    def py():
        index = pd.DatetimeIndex([pd.Timestamp(b["day"], tz=tz) for b in bars], name="Date")
        close = [float("nan") if b["close"] is None else b["close"] for b in bars]
        hist = pd.DataFrame({"Close": close, "Adj Close": close, "Volume": [1000] * len(bars)}, index=index)
        prepared = pricing._prepare_completed_daily_history(meta, hist, now_epoch)
        completed = prepared["completed"]
        return {
            "expectedCompletedDate": prepared["expectedCompletedDate"],
            "freshnessStatus": prepared["freshnessStatus"],
            "excludedIncompleteBar": prepared["excludedIncompleteBar"],
            "latestAvailableBarDate": completed.index[-1].date().isoformat() if not completed.empty else None,
        }

    closes_json = [b["close"] for b in bars]
    chart = {"chart": {"result": [{
        "meta": {"symbol": "TEST", "currency": "USD", **meta},
        "timestamp": [b["open"] for b in bars],
        "indicators": {"quote": [{"open": closes_json, "high": closes_json, "low": closes_json,
                                  "close": closes_json, "volume": [1000] * len(bars)}],
                       "adjclose": [{"adjclose": closes_json}]},
    }]}}
    return Case(
        name=name, ids=ids, py=py,
        ts={"js": _ts_with_clock(now_epoch, 'return JSON.parse(await m.yahooFinance.getPriceSlope("TEST", 2));'),
            "isolate": True},
        routes={"https://query1.finance.yahoo.com/v8/finance/chart/TEST*": {"json": chart}},
        select=lambda v: {k: v.get(k) for k in SLOPE_FIELDS}, expect=expect, **kw)


US_SEP24 = ("2026-09-24", "09:30", "16:00")
WEEK = ["2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23"]

CASES += [
    # ── BR-PRC-001: today's unfinished bar is excluded while the session is open ─────────────────
    _session_case(
        "prc.completed_bar.open_session_drops_todays_row", ["BR-PRC-001"],
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "11:00"), session=US_SEP24, market_time=("2026-09-24", "10:59"),
        previous_close=104.0,
        expect={"expectedCompletedDate": "2026-09-23", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-23"}),
    _session_case(
        "prc.completed_bar.open_session_without_todays_row_excludes_nothing", ["BR-PRC-001"],
        days=WEEK, now=("2026-09-24", "11:00"), session=US_SEP24, market_time=("2026-09-24", "10:59"),
        previous_close=104.0,
        expect={"expectedCompletedDate": "2026-09-23", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": False, "latestAvailableBarDate": "2026-09-23"}),
    _session_case(
        "prc.completed_bar.session_end_is_exclusive_bar_is_kept", ["BR-PRC-001"],
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "16:00"), session=US_SEP24, market_time=("2026-09-24", "16:00"),
        expect={"expectedCompletedDate": "2026-09-24", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": False, "latestAvailableBarDate": "2026-09-24"}),
    _session_case(
        "prc.completed_bar.session_start_is_inclusive_bar_is_dropped", ["BR-PRC-001"],
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "09:30"), session=US_SEP24, market_time=("2026-09-24", "09:30"),
        previous_close=104.0,
        expect={"expectedCompletedDate": "2026-09-23", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-23"}),
    _session_case(
        "prc.completed_bar.before_open_keeps_last_session", ["BR-PRC-001"],
        days=WEEK, now=("2026-09-24", "08:00"), session=US_SEP24, market_time=("2026-09-23", "16:00"),
        expect={"expectedCompletedDate": "2026-09-23", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": False, "latestAvailableBarDate": "2026-09-23"}),
    _session_case(
        "prc.completed_bar.trailing_null_close_trimmed_after_close", ["BR-PRC-001"],
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "18:00"), session=US_SEP24, market_time=("2026-09-24", "16:00"),
        null_close_days=("2026-09-24",),
        # Trimmed row flags excludedIncompleteBar; the older bar is then a day behind regularMarketTime.
        expect={"expectedCompletedDate": "2026-09-24", "freshnessStatus": "STALE",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-23"}),
    _session_case(
        "prc.completed_bar.internal_null_close_gap_is_kept", ["BR-PRC-001"],
        # The gap (09-17) is older than the slope window, so only the series tail matters here.
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "18:00"), session=US_SEP24, market_time=("2026-09-24", "16:00"),
        null_close_days=("2026-09-17",),
        expect={"expectedCompletedDate": "2026-09-24", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": False, "latestAvailableBarDate": "2026-09-24"}),
    _session_case(
        "prc.completed_bar.missing_session_metadata_means_not_active", ["BR-PRC-001"],
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "11:00"), session=None, market_time=("2026-09-24", "10:59"),
        expect={"expectedCompletedDate": "2026-09-24", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": False, "latestAvailableBarDate": "2026-09-24"}),

    # ── BR-PRC-002: freshness classification ───────────────────────────────────────────────────
    _session_case(
        "prc.freshness.closed_market_stale_card_example", ["BR-PRC-002"],
        days=["2026-10-07", "2026-10-08", "2026-10-09", "2026-10-12"], now=("2026-10-13", "20:00"),
        session=("2026-10-13", "09:30", "16:00"), market_time=("2026-10-13", "16:00"),
        expect={"expectedCompletedDate": "2026-10-13", "freshnessStatus": "STALE",
                "excludedIncompleteBar": False, "latestAvailableBarDate": "2026-10-12"}),
    _session_case(
        "prc.freshness.closed_market_current", ["BR-PRC-002"],
        days=["2026-10-07", "2026-10-08", "2026-10-09", "2026-10-12", "2026-10-13"], now=("2026-10-13", "20:00"),
        session=("2026-10-13", "09:30", "16:00"), market_time=("2026-10-13", "16:00"),
        expect={"expectedCompletedDate": "2026-10-13", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": False, "latestAvailableBarDate": "2026-10-13"}),
    _session_case(
        "prc.freshness.missing_regular_market_time_is_unknown", ["BR-PRC-002"],
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "18:00"), session=US_SEP24, market_time=None,
        expect={"expectedCompletedDate": None, "freshnessStatus": "UNKNOWN",
                "excludedIncompleteBar": False, "latestAvailableBarDate": "2026-09-24"}),
    _session_case(
        "prc.freshness.open_session_close_within_tolerance_is_current", ["BR-PRC-002"],
        # Card: last close 100.00 against previousClose 100.005 (inside max(0.01, 1e-6 x previousClose)).
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "11:00"), session=US_SEP24, market_time=("2026-09-24", "10:59"),
        closes={"2026-09-23": 100.0}, previous_close=100.005,
        expect={"expectedCompletedDate": "2026-09-23", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-23"}),
    _session_case(
        "prc.freshness.open_session_close_outside_tolerance_is_stale", ["BR-PRC-002"],
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "11:00"), session=US_SEP24, market_time=("2026-09-24", "10:59"),
        closes={"2026-09-23": 100.0}, previous_close=100.02,
        expect={"expectedCompletedDate": None, "freshnessStatus": "STALE",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-23"}),
    _session_case(
        "prc.freshness.open_session_tolerance_scales_with_large_previous_close", ["BR-PRC-002"],
        # previousClose 20000 -> tolerance 0.02, so a 0.015 gap still matches (the 0.01 floor alone would not).
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "11:00"), session=US_SEP24, market_time=("2026-09-24", "10:59"),
        closes={"2026-09-23": 20000.0}, previous_close=20000.015,
        expect={"expectedCompletedDate": "2026-09-23", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-23"}),
    _session_case(
        "prc.freshness.open_session_weekday_fallback_without_previous_close", ["BR-PRC-002"],
        days=WEEK + ["2026-09-24"], now=("2026-09-24", "11:00"), session=US_SEP24, market_time=("2026-09-24", "10:59"),
        expect={"expectedCompletedDate": "2026-09-23", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-23"}),
    _session_case(
        "prc.freshness.open_monday_fallback_skips_weekend", ["BR-PRC-002"],
        days=["2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21"], now=("2026-09-21", "11:00"),
        session=("2026-09-21", "09:30", "16:00"), market_time=("2026-09-21", "10:59"),
        expect={"expectedCompletedDate": "2026-09-18", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-18"}),
    _session_case(
        "prc.freshness.open_session_missing_previous_session_without_previous_close_is_unknown", ["BR-PRC-002"],
        days=["2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-24"],
        now=("2026-09-24", "11:00"), session=US_SEP24, market_time=("2026-09-24", "10:59"),
        expect={"expectedCompletedDate": None, "freshnessStatus": "UNKNOWN",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-22"}),
    # Owner decision Q1 BR-PRC-002: exchange-session-aware freshness is scheduled. Today the weekday fallback
    # ignores exchange holidays: after Labor Day (Mon 2026-09-07) the Friday bar cannot be proven the prior session
    # without previousClose, so the result is UNKNOWN. With previousClose it is CURRENT (not pinned).
    _session_case(
        "prc.freshness.open_after_monday_holiday_weekday_fallback_unknown", ["BR-PRC-002"],
        days=["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08"], now=("2026-09-08", "11:00"),
        session=("2026-09-08", "09:30", "16:00"), market_time=("2026-09-08", "10:59"),
        mode="pinned", decision="Q1 BR-PRC-002",
        expect={"expectedCompletedDate": None, "freshnessStatus": "UNKNOWN",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-04"}),
    _session_case(
        "prc.freshness.open_after_monday_holiday_previous_close_proves_prior_session", ["BR-PRC-002"],
        days=["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08"], now=("2026-09-08", "11:00"),
        session=("2026-09-08", "09:30", "16:00"), market_time=("2026-09-08", "10:59"),
        closes={"2026-09-04": 103.0}, previous_close=103.0,
        expect={"expectedCompletedDate": "2026-09-04", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": True, "latestAvailableBarDate": "2026-09-04"}),
    _session_case(
        "prc.freshness.exchange_timezone_decides_the_calendar_date_tokyo", ["BR-PRC-001", "BR-PRC-002"],
        days=["2026-09-18", "2026-09-21", "2026-09-22", "2026-09-24"], now=("2026-09-24", "17:00"),
        session=("2026-09-24", "09:00", "15:30"), market_time=("2026-09-24", "15:30"), tz="Asia/Tokyo",
        expect={"expectedCompletedDate": "2026-09-24", "freshnessStatus": "CURRENT",
                "excludedIncompleteBar": False, "latestAvailableBarDate": "2026-09-24"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-PRC-003: listing currency -> USD with minor units (get_volume_gate in both runtimes)
# Python: pricing.get_volume_gate with yfinance and the completed-history loader stubbed.
# Worker: getVolumeGate on canned Yahoo quoteSummary + chart responses.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

GATE_DAYS = _weekdays("2026-10-09", 30)
GATE_NOW = _epoch("2026-10-09", "22:00", "UTC")
GATE_FIELDS = ("status", "gatePass", "meetsMinimum", "minAdvUsd", "adv20dTradedValueUsd", "notionalUsd", "fxRate", "fxCurrency", "gateBasis",
               "recommendedNextAction", "note")


class _FakeYfTicker:
    """yfinance.Ticker stand-in: a listing with `currency`, and an FX pair quote when asked for <ISO>=X."""

    listing_currency = "USD"
    fx_rates: dict = {}

    def __init__(self, symbol: str) -> None:
        if symbol.endswith("=X"):
            self.fast_info = type("FI", (), {"last_price": self.fx_rates.get(symbol)})()
            self.info = {}
        else:
            self.fast_info = {"currency": self.listing_currency}


def _quote_summary(symbol: str, currency: str, price: float | None) -> dict:
    return {"quoteSummary": {"result": [{
        "price": {"currency": currency, "quoteType": "EQUITY" if not symbol.endswith("=X") else "CURRENCY",
                  **({"regularMarketPrice": {"raw": price}} if price is not None else {})},
        "summaryDetail": {}, "defaultKeyStatistics": {}}]}}


def _python_gate_call(via: str | None, symbol: str, min_adv):
    """Python side of a gate scenario: the function itself, the tool through the V2 envelope, or the grouped route."""
    from yfmcp.app import build_handler_registry
    from yfmcp.envelope import _envelope_tool_result
    import tool_groups

    if via is None:
        return pricing.get_volume_gate(symbol, False, min_adv)
    if via in ("snapshot", "snapshot_envelope"):
        # Only the volume component is real; the others fail, which leaves volume.* untouched.
        async def snapshot():
            failed = AsyncMock(return_value=json.dumps({"error": True, "message": "stubbed component"}))
            with patch.multiple(pricing, get_fast_info=failed, get_price_stats=failed, get_ma_position=failed,
                                get_volume_ratio=failed, get_technical_indicators=failed):
                raw = await pricing.get_market_snapshot(symbol, "compact", False, min_adv)
            return _envelope_tool_result("get_market_snapshot", raw) if via == "snapshot_envelope" else raw
        return snapshot()
    if via == "envelope":
        async def call():
            return _envelope_tool_result("check_volume_liquidity_threshold",
                                         await server.check_volume_liquidity_threshold(symbol, False, min_adv))
        return call()
    params = {"ticker": symbol, **({} if min_adv is None else {"min_adv_usd": min_adv})}
    return tool_groups._route_grouped_call("stock_pricing", "check_volume_liquidity_threshold", params,
                                           build_handler_registry(server.yfinance_server))


def _worker_gate_call(via: str | None, symbol: str, min_adv) -> str:
    """Worker side of the same scenario (a JS function body, to be wrapped by _ts_with_clock)."""
    args = {"ticker": symbol, **({} if min_adv is None else {"min_adv_usd": min_adv})}
    min_js = "null" if min_adv is None else json.dumps(min_adv)
    if via is None:
        return f'return JSON.parse(await m.yahooFinance.getVolumeGate("{symbol}", false, {min_js}));'
    if via == "snapshot":
        return f'return JSON.parse(await m.yahooFinance.getMarketSnapshot("{symbol}", "compact", false, {min_js}));'
    if via == "snapshot_envelope":
        return (f'm.response.setWorkerEnv({{}}); '
                f'return (await m.tools.callToolResult("get_market_snapshot", {json.dumps(args)})).value;')
    if via == "envelope":
        return (f'm.response.setWorkerEnv({{}}); '
                f'return (await m.tools.callToolResult("check_volume_liquidity_threshold", {json.dumps(args)})).value;')
    return (f'm.response.setWorkerEnv({{}}); const r = await m.tools.callVisibleToolResult("stock_pricing", '
            f'{{action: "check_volume_liquidity_threshold", params: {json.dumps(args)}}}); return r.value ?? JSON.parse(r.text);')


def _gate_case(name: str, ids: list[str], *, symbol: str, currency: str, close: float, volume: int,
               fx: tuple[str, float | None] | None, expect: dict, min_adv=None, via: str | None = None,
               select=None, **kw) -> Case:
    """One get_volume_gate scenario: `volume` shares a day at `close` (listing units) for 30 sessions.

    `min_adv` is the caller's min_adv_usd. `via` None calls the gate function in each runtime; "envelope" calls the
    tool through the V2 envelope (Python `_envelope_tool_result`, Worker `callToolResult`); "grouped" calls it as an
    action of the stock_pricing group (Python `_route_grouped_call`, Worker `callVisibleToolResult`); "snapshot" and
    "snapshot_envelope" do the same for get_market_snapshot, whose volume block carries the gate fields.
    """
    fx_symbol, fx_rate = fx if fx else (None, None)

    def py():
        listing = type("Listing", (_FakeYfTicker,), {
            "listing_currency": currency, "fx_rates": {fx_symbol: fx_rate} if fx_symbol else {}})
        index = pd.DatetimeIndex([pd.Timestamp(d) for d in GATE_DAYS])
        completed = pd.DataFrame({"Close": [close] * len(index), "Volume": [volume] * len(index)}, index=index)
        prepared = {"completed": completed, "timezoneName": "UTC", "freshnessStatus": "CURRENT",
                    "expectedCompletedDate": GATE_DAYS[-1], "excludedIncompleteBar": False, "retryAttempted": False}
        with patch.object(pricing.yf, "Ticker", listing), \
                patch.object(pricing, "_load_completed_daily_history", AsyncMock(return_value=prepared)):
            return asyncio.run(_python_gate_call(via, symbol, min_adv))

    stamps = [_epoch(d, "20:00", "UTC") for d in GATE_DAYS]
    chart = {"chart": {"result": [{
        "meta": {"exchangeTimezoneName": "UTC", "regularMarketTime": stamps[-1]},
        "timestamp": stamps,
        "indicators": {"quote": [{"close": [close] * len(stamps), "volume": [volume] * len(stamps)}],
                       "adjclose": [{"adjclose": [close] * len(stamps)}]}}]}}
    routes = {
        **YAHOO_AUTH_ROUTES,
        f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?*": {"json": _quote_summary(symbol, currency, close)},
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?*": {"json": chart},
    }
    if fx_symbol:
        encoded = fx_symbol.replace("=", "%3D")
        routes[f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{encoded}?*"] = {
            "json": _quote_summary(fx_symbol, fx_symbol[:3], fx_rate)}
    return Case(
        name=name, ids=ids, py=py,
        ts={"js": _ts_with_clock(GATE_NOW, _worker_gate_call(via, symbol, min_adv)), "isolate": True},
        routes=routes, select=select or (lambda v: {k: v.get(k) for k in GATE_FIELDS}), expect=expect, **kw)


CASES += [
    # Card: LSE listing in GBp, close 250 GBp, 2,000,000 shares a day, GBP=X 0.79 -> 79 GBp per USD,
    # 500,000,000 GBp / 79 = $6.33M, gate FAIL.
    _gate_case("prc.currency.gbp_pence_listing_card_example", ["BR-PRC-003"], symbol="CARD.L", currency="GBp",
               close=250.0, volume=2_000_000, fx=("GBP=X", 0.79),
               expect={"status": "OK", "gatePass": False, "meetsMinimum": None, "minAdvUsd": None, "adv20dTradedValueUsd": 6329113.92,
                       "notionalUsd": 6329113.92, "fxRate": 0.79, "fxCurrency": "GBP",
                       "gateBasis": "ADV20_TRADED_VALUE_USD", "recommendedNextAction": "NONE",
                       "note": "Volume gate FAIL — 20d average traded value $6.3M (< $10M); latest session 1.00x 20d ADV"
                               " [GBp→USD at 0.7900 GBP/USD, 100 GBp per GBP]"}),
    _gate_case("prc.currency.gbx_alias_behaves_like_gbp_pence", ["BR-PRC-003"], symbol="CARX.L", currency="GBX",
               close=250.0, volume=2_000_000, fx=("GBP=X", 0.79),
               expect={"status": "OK", "gatePass": False, "meetsMinimum": None, "minAdvUsd": None, "adv20dTradedValueUsd": 6329113.92,
                       "notionalUsd": 6329113.92, "fxRate": 0.79, "fxCurrency": "GBP",
                       "gateBasis": "ADV20_TRADED_VALUE_USD", "recommendedNextAction": "NONE",
                       "note": "Volume gate FAIL — 20d average traded value $6.3M (< $10M); latest session 1.00x 20d ADV"
                               " [GBX→USD at 0.7900 GBP/USD, 100 GBX per GBP]"}),
    _gate_case("prc.currency.south_african_cents_zac", ["BR-PRC-003"], symbol="ZAC.JO", currency="ZAc",
               close=5000.0, volume=1_000_000, fx=("ZAR=X", 18.0),
               # 5,000 ZAc x 1,000,000 = 5e9 ZAc; 18 ZAR/USD x 100 = 1,800 ZAc per USD -> $2,777,777.78.
               expect={"status": "OK", "gatePass": False, "meetsMinimum": None, "minAdvUsd": None, "adv20dTradedValueUsd": 2777777.78,
                       "notionalUsd": 2777777.78, "fxRate": 18, "fxCurrency": "ZAR",
                       "gateBasis": "ADV20_TRADED_VALUE_USD", "recommendedNextAction": "NONE",
                       "note": "Volume gate FAIL — 20d average traded value $2.8M (< $10M); latest session 1.00x 20d ADV"
                               " [ZAc→USD at 18.0000 ZAR/USD, 100 ZAc per ZAR]"}),
    _gate_case("prc.currency.israeli_agorot_ila", ["BR-PRC-003"], symbol="ILA.TA", currency="ILA",
               close=10000.0, volume=1_000_000, fx=("ILS=X", 4.0),
               # 1e10 ILA / (4 x 100) = $25,000,000 -> PASS.
               expect={"status": "OK", "gatePass": True, "meetsMinimum": None, "minAdvUsd": None, "adv20dTradedValueUsd": 25000000,
                       "notionalUsd": 25000000, "fxRate": 4, "fxCurrency": "ILS",
                       "gateBasis": "ADV20_TRADED_VALUE_USD", "recommendedNextAction": "NONE",
                       "note": "Volume gate PASS — 20d average traded value $25.0M (≥ $10M); latest session 1.00x 20d ADV"
                               " [ILA→USD at 4.0000 ILS/USD, 100 ILA per ILS]"}),
    _gate_case("prc.currency.major_unit_currency_uses_rate_without_scaling", ["BR-PRC-003"], symbol="EURX.PA",
               currency="EUR", close=100.0, volume=1_000_000, fx=("EUR=X", 0.5),
               # 1e8 EUR / 0.5 EUR per USD = $200,000,000.
               expect={"status": "OK", "gatePass": True, "meetsMinimum": None, "minAdvUsd": None, "adv20dTradedValueUsd": 200000000,
                       "notionalUsd": 200000000, "fxRate": 0.5, "fxCurrency": "EUR",
                       "gateBasis": "ADV20_TRADED_VALUE_USD", "recommendedNextAction": "NONE",
                       "note": "Volume gate PASS — 20d average traded value $200.0M (≥ $10M); latest session 1.00x 20d ADV"
                               " [EUR→USD at 0.5000 EUR/USD]"}),
    _gate_case("prc.currency.usd_listing_needs_no_rate", ["BR-PRC-003"], symbol="USDX", currency="USD",
               close=5.0, volume=100_000, fx=None,
               expect={"status": "OK", "gatePass": False, "meetsMinimum": None, "minAdvUsd": None, "adv20dTradedValueUsd": 500000, "notionalUsd": 500000,
                       "fxRate": 1, "fxCurrency": "USD", "gateBasis": "ADV20_TRADED_VALUE_USD",
                       "recommendedNextAction": "NONE",
                       "note": "Volume gate FAIL — 20d average traded value $0.5M (< $10M); latest session 1.00x 20d ADV"}),
    _gate_case("prc.currency.missing_rate_makes_the_gate_unknown", ["BR-PRC-003"], symbol="NORATE.L", currency="GBp",
               close=250.0, volume=2_000_000, fx=("GBP=X", None),
               expect={"status": "OK", "gatePass": None, "meetsMinimum": None, "minAdvUsd": None, "adv20dTradedValueUsd": None, "notionalUsd": None,
                       "fxRate": None, "fxCurrency": None, "gateBasis": "ADV20_TRADED_VALUE_USD",
                       "recommendedNextAction": "RETRY", "note": "Volume gate UNKNOWN — GBP=X rate unavailable"}),
    _gate_case("prc.currency.zero_rate_makes_the_gate_unknown", ["BR-PRC-003"], symbol="ZERORATE.L", currency="GBp",
               close=250.0, volume=2_000_000, fx=("GBP=X", 0.0),
               expect={"status": "OK", "gatePass": None, "meetsMinimum": None, "minAdvUsd": None, "adv20dTradedValueUsd": None, "notionalUsd": None,
                       "fxRate": None, "fxCurrency": None, "gateBasis": "ADV20_TRADED_VALUE_USD",
                       "recommendedNextAction": "RETRY", "note": "Volume gate UNKNOWN — GBP=X rate unavailable"}),
    _gate_case("prc.currency.negative_rate_makes_the_gate_unknown", ["BR-PRC-003"], symbol="NEGRATE.L", currency="GBp",
               close=250.0, volume=2_000_000, fx=("GBP=X", -0.79),
               expect={"status": "OK", "gatePass": None, "meetsMinimum": None, "minAdvUsd": None, "adv20dTradedValueUsd": None, "notionalUsd": None,
                       "fxRate": None, "fxCurrency": None, "gateBasis": "ADV20_TRADED_VALUE_USD",
                       "recommendedNextAction": "RETRY", "note": "Volume gate UNKNOWN — GBP=X rate unavailable"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-EVD-001/002/003: consensus provider agreement (evidence.py build_consensus_curve <-> evidence.ts buildConsensusCurve)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from yfmcp import evidence as ev  # noqa: E402

CURVE_AS_OF = "2026-09-26T08:00:00.000Z"
DEFAULT_POLICY = {"horizonYears": 5, "minAnalystCount": 3, "conflictTolerancePct": 10, "epsAbsoluteTolerance": 0.02}


def _estimate(mean=None, high=None, low=None, count=None, currency="USD"):
    return {"mean": mean, "high": high, "low": low, "analystCount": count, "currency": currency,
            "currencyBasis": "PROVIDER_STATED" if currency else "NOT_STATED"}


def _provider(name: str, fiscal_year_end: str = "2026-12-31", eps=None, revenue=None, status: str = "OK") -> dict:
    """One provider input with a single fiscal-year period."""
    return {
        "provider": name, "status": status, "retrievedAt": CURVE_AS_OF, "providerTimestamp": None, "message": None,
        "periods": [{
            "providerPeriodLabel": "0y" if name == "yahoo_finance" else "fiscal year",
            "fiscalYearEnd": fiscal_year_end, "fiscalYearEndBasis": "PROVIDER_STATED",
            "eps": eps or _estimate(), "revenue": revenue or _estimate(), "epsTrend": None, "epsRevisions": None}],
    }


def _fy0_cell(metric: str, fields=("coverage", "agreement")):
    def pick(curve):
        cell = curve["periods"][0]["metrics"][metric]
        return {k: cell[k] for k in fields}
    return pick


def _curve_case(name: str, ids: list[str], inputs: list[dict], metric: str, expect, *, policy: dict | None = None,
                splits: dict | None = None, fields=("coverage", "agreement"), **kw) -> Case:
    policy = policy or DEFAULT_POLICY
    return Case(
        name=name, ids=ids,
        py=lambda: ev.build_consensus_curve("TST", json.loads(json.dumps(inputs)), CURVE_AS_OF,
                                            dict(policy), None, splits),
        ts={"module": "evidence", "fn": "buildConsensusCurve",
            "args": ["TST", inputs, CURVE_AS_OF, policy, None, splits]},
        select=_fy0_cell(metric, fields), expect=expect, **kw)


def _default_policy_curve_case(name: str, ids: list[str], inputs: list[dict], status: str) -> Case:
    """buildConsensusCurve with no policy argument, so each runtime applies its own DEFAULT_CONSENSUS_POLICY."""
    return Case(
        name=name, ids=ids,
        py=lambda: ev.build_consensus_curve("TST", json.loads(json.dumps(inputs)), CURVE_AS_OF),
        ts={"js": "return m.evidence.buildConsensusCurve('TST', args[0], args[1]);", "args": [inputs, CURVE_AS_OF]},
        select=lambda v: _fy0_cell("eps", ("agreement",))(v)["agreement"]["status"], expect=status)


_AGREE_IDS = ["BR-EVD-001", "BR-EVD-003"]

CASES += [
    # Card BR-EVD-001: Yahoo FY0 EPS 5.20 vs Alpha Vantage 5.00 -> 0.20 > 0.02, 0.20/5.20 x 100 = 3.85% <= 10% -> AGREED.
    _curve_case(
        "evd.agreement.eps_relative_within_tolerance_agreed", _AGREE_IDS + ["BR-EVD-002"],
        [_provider("yahoo_finance", eps=_estimate(5.20, 5.60, 4.80, 10)), _provider("alpha_vantage", eps=_estimate(5.00, 5.50, 4.70, 12))],
        "eps",
        {"coverage": "PROVIDER_COVERED", "agreement": {
            "status": "AGREED", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 3.85,
            "absoluteDiff": 0.2, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED"}}),
    # Card BR-EVD-003: 5.20 vs 5.75 -> 0.55 / 5.75 x 100 = 9.57% -> AGREED; 5.20 vs 5.90 -> 11.86% -> CONFLICT.
    _curve_case(
        "evd.agreement.eps_9_57_pct_agreed_denominator_is_larger_magnitude", _AGREE_IDS,
        [_provider("yahoo_finance", eps=_estimate(5.20, None, None, 10)), _provider("alpha_vantage", eps=_estimate(5.75, None, None, 12))],
        "eps",
        {"coverage": "PROVIDER_COVERED", "agreement": {
            "status": "AGREED", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 9.57,
            "absoluteDiff": 0.55, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED"}}),
    _curve_case(
        "evd.agreement.eps_11_86_pct_conflict", _AGREE_IDS,
        [_provider("yahoo_finance", eps=_estimate(5.20, None, None, 10)), _provider("alpha_vantage", eps=_estimate(5.90, None, None, 12))],
        "eps",
        {"coverage": "PROVIDER_CONFLICT", "agreement": {
            "status": "CONFLICT", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 11.86,
            "absoluteDiff": 0.7, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED"}}),
    # Card BR-EVD-002: 5.20 vs 5.21 is inside the $0.02 EPS band, so AGREED even when the percent tolerance is 0.1.
    _curve_case(
        "evd.agreement.eps_within_two_cents_agreed_despite_tight_pct_tolerance", ["BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(5.20, None, None, 10)), _provider("alpha_vantage", eps=_estimate(5.21, None, None, 12))],
        "eps",
        {"coverage": "PROVIDER_COVERED", "agreement": {
            "status": "AGREED", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 0.19,
            "absoluteDiff": 0.01, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED"}},
        policy={**DEFAULT_POLICY, "conflictTolerancePct": 0.1}),
    _curve_case(
        "evd.agreement.eps_three_cents_gap_conflicts_under_tight_pct_tolerance", ["BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(5.20, None, None, 10)), _provider("alpha_vantage", eps=_estimate(5.23, None, None, 12))],
        "eps",
        {"coverage": "PROVIDER_CONFLICT", "agreement": {
            "status": "CONFLICT", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 0.57,
            "absoluteDiff": 0.03, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED"}},
        policy={**DEFAULT_POLICY, "conflictTolerancePct": 0.1}),
    # The code's own default policy (what the tools use, not DEFAULT_POLICY above): 0.10 vs 0.12 is 16.7% apart, so only
    # the $0.02 band makes it AGREED; 0.10 vs 0.13 is outside the band and conflicts.
    _default_policy_curve_case(
        "evd.agreement.code_default_policy_two_cent_band_agreed", ["BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(0.10, None, None, 10)), _provider("alpha_vantage", eps=_estimate(0.12, None, None, 12))],
        "AGREED"),
    _default_policy_curve_case(
        "evd.agreement.code_default_policy_three_cent_gap_conflicts", ["BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(0.10, None, None, 10)), _provider("alpha_vantage", eps=_estimate(0.13, None, None, 12))],
        "CONFLICT"),
    # The $0.02 band is EPS only: a revenue gap of 0.01 on the same tight tolerance conflicts.
    _curve_case(
        "evd.agreement.revenue_has_no_absolute_band", ["BR-EVD-003"],
        [_provider("yahoo_finance", revenue=_estimate(5.20, None, None, 10)), _provider("alpha_vantage", revenue=_estimate(5.21, None, None, 12))],
        "revenue",
        {"coverage": "PROVIDER_CONFLICT", "agreement": {
            "status": "CONFLICT", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 0.19,
            "absoluteDiff": 0.01, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED"}},
        policy={**DEFAULT_POLICY, "conflictTolerancePct": 0.1}),
    _curve_case(
        "evd.agreement.single_provider", ["BR-EVD-001", "BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(5.20, None, None, 10)), {"provider": "alpha_vantage", "status": "NO_DATA", "retrievedAt": CURVE_AS_OF,
                                                                           "providerTimestamp": None, "message": None, "periods": []}],
        "eps",
        {"coverage": "PROVIDER_COVERED", "agreement": {
            "status": "SINGLE_PROVIDER", "providersCompared": ["yahoo_finance"], "relativeDiffPct": None, "absoluteDiff": None}}),
    _curve_case(
        "evd.agreement.no_provider_mean", ["BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(None, None, None, None)), _provider("alpha_vantage", eps=_estimate(None, None, None, None))],
        "eps",
        {"coverage": "PROVIDER_NOT_COVERED", "agreement": {
            "status": "NO_PROVIDER", "providersCompared": [], "relativeDiffPct": None, "absoluteDiff": None}}),
    _curve_case(
        "evd.agreement.fiscal_year_ends_eleven_days_apart_is_period_identity_mismatch", ["BR-EVD-001", "BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", "2026-12-31", eps=_estimate(5.20, None, None, 10)),
         _provider("alpha_vantage", "2026-12-20", eps=_estimate(5.20, None, None, 12))],
        "eps",
        {"coverage": "PROVIDER_CONFLICT", "agreement": {
            "status": "PERIOD_IDENTITY_MISMATCH", "providersCompared": ["yahoo_finance", "alpha_vantage"],
            "fiscalYearEnds": ["2026-12-31", "2026-12-20"], "relativeDiffPct": None, "absoluteDiff": None}}),
    _curve_case(
        "evd.agreement.fiscal_year_ends_ten_days_apart_still_compared", ["BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", "2026-12-31", eps=_estimate(5.20, None, None, 10)),
         _provider("alpha_vantage", "2026-12-21", eps=_estimate(5.00, None, None, 12))],
        "eps",
        {"coverage": "PROVIDER_COVERED", "agreement": {
            "status": "AGREED", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 3.85,
            "absoluteDiff": 0.2, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED"}}),
    _curve_case(
        "evd.agreement.differing_currencies_is_currency_mismatch", ["BR-EVD-001", "BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(60.1, None, None, 20, "TWD")),
         _provider("alpha_vantage", eps=_estimate(9.4, None, None, 20, "USD"))],
        "eps",
        {"coverage": "PROVIDER_CONFLICT", "agreement": {
            "status": "CURRENCY_MISMATCH", "providersCompared": ["yahoo_finance", "alpha_vantage"],
            "currencies": ["TWD", "USD"], "relativeDiffPct": None, "absoluteDiff": None}}),
    # Identical mean/high/low and equal analyst counts: one upstream feed carried twice.
    _curve_case(
        "evd.agreement.identical_feeds_are_not_independent", ["BR-EVD-001", "BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(5.20, 5.60, 4.80, 10)), _provider("alpha_vantage", eps=_estimate(5.20, 5.60, 4.80, 10))],
        "eps",
        {"coverage": "PROVIDER_COVERED", "agreement": {
            "status": "IDENTICAL", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 0,
            "absoluteDiff": 0, "currencyIdentity": "VERIFIED", "independence": "NOT_INDEPENDENT"}}),
    _curve_case(
        "evd.agreement.identical_means_with_different_analyst_counts_are_agreed", ["BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(5.20, 5.60, 4.80, 10)), _provider("alpha_vantage", eps=_estimate(5.20, 5.60, 4.80, 12))],
        "eps",
        {"coverage": "PROVIDER_COVERED", "agreement": {
            "status": "AGREED", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 0,
            "absoluteDiff": 0, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED"}}),
    # A mean outside its own high-low range is excluded and named; the other provider stands alone.
    _curve_case(
        "evd.agreement.inconsistent_row_is_excluded_and_named", ["BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(5.20, 5.60, 4.80, 10)), _provider("alpha_vantage", eps=_estimate(6.50, 5.60, 4.80, 12))],
        "eps",
        {"coverage": "PROVIDER_COVERED", "agreement": {
            "status": "SINGLE_PROVIDER", "providersCompared": ["yahoo_finance"], "relativeDiffPct": None, "absoluteDiff": None,
            "providersExcluded": [{"provider": "alpha_vantage", "reason": "MEAN_ABOVE_HIGH"}]}}),
    _curve_case(
        "evd.agreement.every_row_inconsistent_is_provider_inconsistent", ["BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(3.0, 5.60, 4.80, 10)), _provider("alpha_vantage", eps=_estimate(6.50, 5.60, 4.80, 12))],
        "eps",
        {"coverage": "PROVIDER_INCONSISTENT", "agreement": {
            "status": "PROVIDER_INCONSISTENT", "providersCompared": [], "relativeDiffPct": None, "absoluteDiff": None,
            "providersExcluded": [{"provider": "yahoo_finance", "reason": "MEAN_BELOW_LOW"},
                                  {"provider": "alpha_vantage", "reason": "MEAN_ABOVE_HIGH"}]}}),
    _curve_case(
        "evd.agreement.analyst_count_below_minimum_is_insufficient", ["BR-EVD-002"],
        [_provider("yahoo_finance", eps=_estimate(5.20, None, None, 2))],
        "eps",
        {"coverage": "INSUFFICIENT_ANALYST_COUNT", "agreement": {
            "status": "SINGLE_PROVIDER", "providersCompared": ["yahoo_finance"], "relativeDiffPct": None, "absoluteDiff": None}}),
    # EPS gap that equals a recent 4-for-1 split (within 15%): one provider is not split-adjusted.
    _curve_case(
        "evd.agreement.eps_gap_matching_recent_split_is_not_split_adjusted", ["BR-EVD-002", "BR-EVD-003", "BR-EVD-012"],
        [_provider("yahoo_finance", eps=_estimate(1.30, None, None, 10)), _provider("alpha_vantage", eps=_estimate(5.20, None, None, 12))],
        "eps",
        {"coverage": "PROVIDER_CONFLICT", "agreement": {
            "status": "NOT_SPLIT_ADJUSTED", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 75,
            "absoluteDiff": 3.9, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED",
            "split": {"date": "2026-09-10", "ratio": 4}, "providersNotAdjusted": ["alpha_vantage"]}},
        splits={"status": "OK", "splits": [{"date": "2026-09-10", "ratio": 4}]}),
    _curve_case(
        "evd.agreement.eps_gap_without_split_history_is_a_conflict", ["BR-EVD-002", "BR-EVD-003"],
        [_provider("yahoo_finance", eps=_estimate(1.30, None, None, 10)), _provider("alpha_vantage", eps=_estimate(5.20, None, None, 12))],
        "eps",
        {"coverage": "PROVIDER_CONFLICT", "agreement": {
            "status": "CONFLICT", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 75,
            "absoluteDiff": 3.9, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED"}}),
    _curve_case(
        "evd.agreement.eps_gap_outside_split_ratio_tolerance_is_a_conflict", ["BR-EVD-002"],
        # Gap factor 5.20 / 1.00 = 5.2 against a 4-for-1 split: 30% off, outside the 15% tolerance.
        [_provider("yahoo_finance", eps=_estimate(1.00, None, None, 10)), _provider("alpha_vantage", eps=_estimate(5.20, None, None, 12))],
        "eps",
        {"coverage": "PROVIDER_CONFLICT", "agreement": {
            "status": "CONFLICT", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 80.77,
            "absoluteDiff": 4.2, "currencyIdentity": "VERIFIED", "independence": "UNVERIFIED"}},
        splits={"status": "OK", "splits": [{"date": "2026-09-10", "ratio": 4}]}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-EVD-004 (cut integrity), BR-EVD-005 / BR-EVD-027 (write-once storage)
# Python: server.get_evidence_cut / yfmcp.evidence_store with an injected store.
# Worker: evidence-pack.getEvidenceCut / evidence-store.putOnce with an injected memory store.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

import contextlib  # noqa: E402

from yfmcp import evidence_store as es  # noqa: E402

CUT_BODY = '{"schema":"yfmcp.evidence-cut/1","ticker":"AAPL"}'
CUT_SHA = "7ade1fb748f88275e5bdc6ede48f0fd11b9b3267b5c5a5d7b1a786fd126faf17"
CUT_ID = f"ec1_AAPL_20261014T120000Z_{CUT_SHA}"
CUT_KEY = f"evidence-cuts/AAPL/20261014T120000Z/{CUT_SHA}.json"
ALTERED_BODY = '{"schema":"yfmcp.evidence-cut/1","ticker":"AAPX"}'
ALTERED_SHA = "10591cf27e06625138f55d5d561d2c55ff9718be64337f368bd343340469c60d"
NOT_JSON_BODY = "not json"
NOT_JSON_SHA = "7ccfa1fbf3940e6f0c0375d87c0f9235a50514e14cb427bdfaf5077987b26ccf"


@contextlib.contextmanager
def _python_store(store):
    es.set_store_for_tests(store)
    try:
        yield store
    finally:
        es.set_store_for_tests(es._UNSET)


class _PyFailingGet(es.MemoryStore):
    def get(self, key):
        raise RuntimeError("bucket offline")


class _PyFailingPut(es.MemoryStore):
    def put(self, key, body, metadata):
        raise RuntimeError("disk full")


def _cut_case(name: str, ids: list[str], cut_id: str, objects: dict, expect, store: str = "memory") -> Case:
    def py():
        impl = {"memory": es.MemoryStore, "none": lambda: None, "failing": _PyFailingGet}[store]()
        if store != "none":
            impl.objects.update(objects)
        with _python_store(impl):
            return asyncio.run(server.get_evidence_cut(cut_id))

    js = f"""
const store = {{
  memory: () => m.evidenceStore.memoryEvidenceStore(),
  none: () => null,
  failing: () => ({{ kind: "failing", get: async () => {{ throw new Error("bucket offline"); }} }}),
}}[args[0]]();
if (store && store.objects) for (const [k, v] of Object.entries(args[1])) store.objects.set(k, v);
m.evidenceStore.setEvidenceStoreForTests(store);
return await m.evidencePack.getEvidenceCut(args[2]);
"""
    return Case(name=name, ids=ids, py=py, ts={"js": js, "args": [store, objects, cut_id], "isolate": True}, expect=expect)


CASES += [
    _cut_case("evd.cut.stored_text_hashing_to_the_id_is_verified", ["BR-EVD-004"], CUT_ID, {CUT_KEY: CUT_BODY},
              {"evidenceCutId": CUT_ID, "storageKey": CUT_KEY, "integrity": "VERIFIED", "expectedSha256": CUT_SHA,
               "actualSha256": CUT_SHA, "document": {"schema": "yfmcp.evidence-cut/1", "ticker": "AAPL"}}),
    _cut_case("evd.cut.any_changed_byte_is_a_mismatch", ["BR-EVD-004"], CUT_ID, {CUT_KEY: ALTERED_BODY},
              {"evidenceCutId": CUT_ID, "storageKey": CUT_KEY, "integrity": "MISMATCH", "expectedSha256": CUT_SHA,
               "actualSha256": ALTERED_SHA, "document": {"schema": "yfmcp.evidence-cut/1", "ticker": "AAPX"}}),
    _cut_case("evd.cut.matching_hash_but_not_json_is_a_mismatch", ["BR-EVD-004"],
              f"ec1_AAPL_20261014T120000Z_{NOT_JSON_SHA}",
              {f"evidence-cuts/AAPL/20261014T120000Z/{NOT_JSON_SHA}.json": NOT_JSON_BODY},
              {"evidenceCutId": f"ec1_AAPL_20261014T120000Z_{NOT_JSON_SHA}",
               "storageKey": f"evidence-cuts/AAPL/20261014T120000Z/{NOT_JSON_SHA}.json", "integrity": "MISMATCH",
               "expectedSha256": NOT_JSON_SHA, "actualSha256": NOT_JSON_SHA, "document": None}),
    _cut_case("evd.cut.absent_key_is_not_found", ["BR-EVD-004"], CUT_ID, {},
              {"error": True, "code": "NOT_FOUND", "message": f"No evidence cut is stored under {CUT_ID}."}),
    _cut_case("evd.cut.no_store_is_storage_unavailable", ["BR-EVD-004", "BR-EVD-005"], CUT_ID, {}, 
              {"error": True, "code": "STORAGE_UNAVAILABLE",
               "message": "Durable evidence storage is not configured; evidence cuts cannot be retrieved."}, store="none"),
    _cut_case("evd.cut.store_read_failure_is_storage_error", ["BR-EVD-004"], CUT_ID, {},
              {"error": True, "code": "STORAGE_ERROR", "message": "bucket offline"}, store="failing"),
    _cut_case("evd.cut.malformed_id_is_rejected_before_storage", ["BR-EVD-004"], "../../etc/passwd", {},
              {"error": True, "code": "INPUT_VALIDATION_ERROR",
               "message": "evidence_cut_id must look like ec1_<TICKER>_<YYYYMMDDTHHMMSSZ>_<sha256>."}),
    _cut_case("evd.cut.id_with_surrounding_whitespace_is_trimmed_for_lookup", ["BR-EVD-004"], f"  {CUT_ID}  ", {CUT_KEY: CUT_BODY},
              {"evidenceCutId": f"  {CUT_ID}  ", "storageKey": CUT_KEY, "integrity": "VERIFIED", "expectedSha256": CUT_SHA,
               "actualSha256": CUT_SHA, "document": {"schema": "yfmcp.evidence-cut/1", "ticker": "AAPL"}}),
]


def _put_case(name: str, ids: list[str], steps: list[tuple[str, str]], expect, store: str = "memory") -> Case:
    """Write each (key, body) in order through put_once; report each status and the final stored text per key."""
    def py():
        impl = {"memory": es.MemoryStore, "none": lambda: None, "failing": _PyFailingPut}[store]()
        with _python_store(impl):
            results = [es.put_once(key, body, {"ticker": "AAPL"}) for key, body in steps]
        return {"results": results, "stored": dict(impl.objects) if impl is not None else None}

    js = """
const store = {
  memory: () => m.evidenceStore.memoryEvidenceStore(),
  none: () => null,
  failing: () => Object.assign(m.evidenceStore.memoryEvidenceStore(), { put: async () => { throw new Error("disk full"); } }),
}[args[0]]();
m.evidenceStore.setEvidenceStoreForTests(store);
const results = [];
for (const [key, body] of args[1]) results.push(await m.evidenceStore.putOnce(key, body, { ticker: "AAPL" }));
return { results, stored: store ? Object.fromEntries(store.objects) : null };
"""
    return Case(name=name, ids=ids, py=py, ts={"js": js, "args": [store, steps], "isolate": True}, expect=expect)


OBS_KEY = "consensus-history/AAPL/2026-10-14.json"
CASES += [
    _put_case("evd.store.first_write_is_stored", ["BR-EVD-005", "BR-EVD-027"], [(CUT_KEY, CUT_BODY)],
              {"results": [{"status": "STORED"}], "stored": {CUT_KEY: CUT_BODY}}),
    _put_case("evd.store.second_write_is_already_stored_and_file_unchanged", ["BR-EVD-005", "BR-EVD-027"],
              [(OBS_KEY, '{"observedAt":"2026-10-14T09:00:00.000Z"}'), (OBS_KEY, '{"observedAt":"2026-10-14T17:00:00.000Z"}')],
              {"results": [{"status": "STORED"}, {"status": "ALREADY_STORED"}],
               "stored": {OBS_KEY: '{"observedAt":"2026-10-14T09:00:00.000Z"}'}}),
    _put_case("evd.store.no_store_is_unavailable", ["BR-EVD-005", "BR-EVD-027"], [(CUT_KEY, CUT_BODY)],
              {"results": [{"status": "UNAVAILABLE"}], "stored": None}, store="none"),
    _put_case("evd.store.write_error_is_failed_with_message_and_never_raises", ["BR-EVD-005", "BR-EVD-027"], [(CUT_KEY, CUT_BODY)],
              {"results": [{"status": "FAILED", "message": "disk full"}], "stored": {}}, store="failing"),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-SHR-001: decision-grade SEC XBRL fact
# Python: server._is_decision_grade_sec_xbrl_fact. Worker: yahooFinance.isDecisionGradeXbrlFact.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

GOOD_PAYLOAD = {"value": 391035000000, "extractionMethod": "XBRL_COMPANYFACTS", "xbrlContext": {"concept": "Revenues"}}
GOOD_EVIDENCE = {"sourceType": "SEC_XBRL", "concept": "Revenues", "accessionNumber": "0000320193-24-000123", "periodEnd": "2024-09-28"}


def _grade_case(name: str, ids: list[str], payload: dict, evidence, status, expect=None, **kw) -> Case:
    return Case(
        name=name, ids=ids,
        py=lambda: server._is_decision_grade_sec_xbrl_fact(json.loads(json.dumps(payload)), json.loads(json.dumps(evidence)), status),
        ts={"module": "yahooFinance", "fn": "isDecisionGradeXbrlFact", "args": [payload, evidence, status]},
        **({"expect": expect} if expect is not None else {}), **kw)


CASES += [
    _grade_case("shr.decision_grade.complete_xbrl_fact_is_decision_grade", ["BR-SHR-001"], GOOD_PAYLOAD, GOOD_EVIDENCE, "FOUND", True),
    _grade_case("shr.decision_grade.status_comparison_ignores_case", ["BR-SHR-001"], GOOD_PAYLOAD, GOOD_EVIDENCE, "found", True),
    _grade_case("shr.decision_grade.zero_value_still_counts_as_a_value", ["BR-SHR-001"], {**GOOD_PAYLOAD, "value": 0}, GOOD_EVIDENCE, "FOUND", True),
    # Card example: revenue FOUND via XBRL whose evidence lacks an accession number.
    _grade_case("shr.decision_grade.missing_accession_number_is_not_decision_grade", ["BR-SHR-001"], GOOD_PAYLOAD,
                {k: v for k, v in GOOD_EVIDENCE.items() if k != "accessionNumber"}, "FOUND", False),
    _grade_case("shr.decision_grade.missing_period_end_is_not_decision_grade", ["BR-SHR-001"], GOOD_PAYLOAD,
                {**GOOD_EVIDENCE, "periodEnd": None}, "FOUND", False),
    _grade_case("shr.decision_grade.empty_concept_is_not_decision_grade", ["BR-SHR-001"], GOOD_PAYLOAD,
                {**GOOD_EVIDENCE, "concept": ""}, "FOUND", False),
    _grade_case("shr.decision_grade.missing_source_type_is_not_decision_grade", ["BR-SHR-001"], GOOD_PAYLOAD,
                {**GOOD_EVIDENCE, "sourceType": None}, "FOUND", False),
    _grade_case("shr.decision_grade.no_source_evidence_is_not_decision_grade", ["BR-SHR-001"], GOOD_PAYLOAD, None, "FOUND", False),
    _grade_case("shr.decision_grade.null_value_is_not_decision_grade", ["BR-SHR-001"], {**GOOD_PAYLOAD, "value": None}, GOOD_EVIDENCE, "FOUND", False),
    _grade_case("shr.decision_grade.not_found_status_is_not_decision_grade", ["BR-SHR-001"], GOOD_PAYLOAD, GOOD_EVIDENCE, "NOT_FOUND", False),
    _grade_case("shr.decision_grade.html_table_extraction_is_never_decision_grade", ["BR-SHR-001"],
                {**GOOD_PAYLOAD, "extractionMethod": "HTML_TABLE"}, GOOD_EVIDENCE, "FOUND", False),
    _grade_case("shr.decision_grade.text_extraction_is_never_decision_grade", ["BR-SHR-001"],
                {**GOOD_PAYLOAD, "extractionMethod": "TEXT_REGEX"}, GOOD_EVIDENCE, "FOUND", False),
    _grade_case("shr.decision_grade.missing_xbrl_context_is_not_decision_grade", ["BR-SHR-001"],
                {k: v for k, v in GOOD_PAYLOAD.items() if k != "xbrlContext"}, GOOD_EVIDENCE, "FOUND", False),
]

CASES += [
    # Finding (Phase 2, for Q3i triage): an empty xbrlContext object ({}) and a numeric 0 in an evidence field both
    # count as present in both runtimes, so a context with no content can make a fact decision-grade.
    _grade_case("shr.decision_grade.empty_xbrl_context_object_still_passes", ["BR-SHR-001"], {**GOOD_PAYLOAD, "xbrlContext": {}},
                GOOD_EVIDENCE, "FOUND", True),
    _grade_case("shr.decision_grade.numeric_zero_evidence_field_counts_as_present", ["BR-SHR-001"], GOOD_PAYLOAD,
                {**GOOD_EVIDENCE, "periodEnd": 0}, "FOUND", True),
    # Python/Worker divergences with no owner decision (FINDING): Python requires xbrlContext to be a dict while the
    # Worker accepts any object (an array passes); Python accepts a whitespace-only evidence field, the Worker trims.
    _grade_case("shr.decision_grade.array_xbrl_context_diverges", ["BR-SHR-001"], {**GOOD_PAYLOAD, "xbrlContext": []},
                GOOD_EVIDENCE, "FOUND", mode="pinned",
                decision="FINDING: Worker accepts an array xbrlContext as decision-grade, Python requires a dict",
                expect_py=False, expect_ts=True),
    _grade_case("shr.decision_grade.whitespace_only_evidence_field_diverges", ["BR-SHR-001"], GOOD_PAYLOAD,
                {**GOOD_EVIDENCE, "concept": "   "}, "FOUND", mode="pinned",
                decision="FINDING: Python accepts a whitespace-only evidence field, the Worker trims it and rejects",
                expect_py=True, expect_ts=False),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-ENV-005 / BR-ENV-007 (Q3b) and Q3a: ticker / accession validation and accession resolution
# in get_filing_outline. Python validates the ticker and accession and resolves an accession to a filing
# index; the Worker validates neither (it validates tickers only in get_historical_prices). Q3b's pinned cases record
# that. Q3a (2.13.0): both honour an accession without document_url.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from yfmcp.validation import _validate_ticker  # noqa: E402

OUTLINE_HTML = "<html><body><h2>Item 1. Business</h2><h2>Item 1A. Risk Factors</h2></body></html>"
AAPL_DOC = "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm"
AAPL_INDEX = "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/0000320193-24-000123-index.htm"
OUTLINE_ROUTES = {
    "https://www.sec.gov/files/company_tickers.json": {"json": {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}},
    # The Worker resolves an accession through SEC submissions (2.13.0, Q3a).
    "https://data.sec.gov/submissions/CIK0000320193.json": {"json": {"cik": "320193", "filings": {"recent": {
        "form": ["10-K"], "accessionNumber": ["0000320193-24-000123"], "primaryDocument": ["aapl-20240928.htm"],
        "filingDate": ["2024-11-01"], "reportDate": ["2024-09-28"], "acceptanceDateTime": ["2024-11-01T16:30:00.000Z"],
    }}}},
    AAPL_DOC: {"body": OUTLINE_HTML},
    AAPL_INDEX: {"body": OUTLINE_HTML},
}
VALID_ACCESSION = "0000320193-24-000123"
OUTLINE_OK = {"outline": ["Item 1. Business", "Item 1A. Risk Factors"]}


def _outline_view(v):
    """Success -> the outline titles; failure -> code / message (Python envelope or the Worker's error text)."""
    if not isinstance(v, dict):
        return v
    if v.get("outline") is not None and not v.get("error"):
        return {"outline": [h["title"] for h in v["outline"]]}
    err = v.get("error")
    if isinstance(err, dict):
        return {"error": True, "code": err.get("code"), "message": err.get("message")}
    return {"error": True, "code": None, "message": v.get("message")}


def _outline_py(ticker: str, accession, document_url):
    # An accession without document_url resolves through SEC's ticker index and submissions (Yahoo unreachable), as the Worker does.
    from scripts.parity.cases_accession import _python_sec_state
    with _python_sec_state():
        return asyncio.run(server.get_filing_outline(ticker, accession, document_url))


def _outline_case(name: str, ids: list[str], ticker: str, accession, document_url, **kw) -> Case:
    return Case(
        name=name, ids=ids,
        py=lambda: _outline_py(ticker, accession, document_url),
        ts={"module": "yahooFinance", "fn": "getFilingOutline", "args": [ticker, accession, document_url], "isolate": True},
        routes=OUTLINE_ROUTES, select=_outline_view, **kw)


CASES += [
    # Function level: the shared format check (BR-ENV-007) is identical where both runtimes run it.
    Case(name="tool.ticker_format.trimmed_lowercase_symbol_passes", ids=["BR-ENV-007"],
         py=lambda: _validate_ticker(" brk-b "), ts={"module": "validate", "fn": "validateTicker", "args": [" brk-b "]}, expect=None),
    Case(name="tool.ticker_format.symbol_with_dollar_is_rejected", ids=["BR-ENV-007"],
         py=lambda: _validate_ticker("AAPL$"), ts={"module": "validate", "fn": "validateTicker", "args": ["AAPL$"]},
         expect="Invalid ticker symbol: 'AAPL$'. Must be 1-20 characters: uppercase letters, digits, or . - ^ ="),
    Case(name="tool.ticker_format.empty_and_21_char_symbols_are_rejected", ids=["BR-ENV-007"],
         py=lambda: [_validate_ticker(""), _validate_ticker("A" * 21), _validate_ticker("A" * 20)],
         ts={"js": 'return [m.validate.validateTicker(""), m.validate.validateTicker("A".repeat(21)), m.validate.validateTicker("A".repeat(20))]'},
         expect=["Invalid ticker symbol: ''. Must be 1-20 characters: uppercase letters, digits, or . - ^ =",
                 "Invalid ticker symbol: '" + "A" * 21 + "'. Must be 1-20 characters: uppercase letters, digits, or . - ^ =", None]),

    # Q3b: the Worker's get_filing_outline never validates the ticker or the accession.
    _outline_case("tool.q3b.outline_invalid_ticker_python_rejects_worker_proceeds", ["BR-ENV-007"], "AAPL$", None, AAPL_DOC,
                  mode="pinned", decision="Q3b",
                  expect_py={"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "Invalid ticker symbol: 'AAPL$'. Must be 1-20 characters: uppercase letters, digits, or . - ^ ="},
                  expect_ts=OUTLINE_OK),
    _outline_case("tool.q3b.outline_invalid_accession_python_rejects_worker_proceeds", ["BR-ENV-005"], "AAPL", "320193-24-123", AAPL_DOC,
                  mode="pinned", decision="Q3b",
                  expect_py={"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "Invalid accession number: '320193-24-123'. Expected format: XXXXXXXXXX-YY-ZZZZZZ."},
                  expect_ts=OUTLINE_OK),
    _outline_case("tool.q3b.outline_valid_accession_and_ticker_with_document_url_agree", ["BR-ENV-005", "BR-ENV-007"],
                  "AAPL", VALID_ACCESSION, AAPL_DOC, expect=OUTLINE_OK),
    # Q3a (2.13.0): an accession without document_url is honoured in both runtimes. The Worker resolves it through
    # resolveSecFiling and reads the primary document; Python does the same. The filing index page is served here with
    # the same HTML, so this case cannot tell a primary-document read from an index-page read; cases_accession.py
    # (q3a.outline.accession_only_hit_reads_the_primary_document) does.
    _outline_case("tool.q3a.outline_accession_only_python_resolves_worker_ignores", ["Q3a"], "AAPL", VALID_ACCESSION, None,
                  expect=OUTLINE_OK),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Q4 (2.13.0): the smoke ticker -> CIK table is out of production lookup. The order is cache -> Yahoo -> SEC
# company_tickers.json -> EDGAR atom; the 11 former entries are a test fixture
# (scripts/parity/fixtures/ticker_cik_fixtures.json) injected at the old position.
# Python: edgar._resolve_cik_for_ticker with yfinance failing, fixtures through edgar._TEST_TICKER_CIK_FIXTURES.
# Worker: getSubmissionsForTicker (cikPadded), fixtures through setTickerCikFixturesForTests.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from pathlib import Path  # noqa: E402

from yfmcp.clients import edgar  # noqa: E402

TICKER_CIK_FIXTURES: dict[str, str] = json.loads(
    (Path(__file__).with_name("fixtures") / "ticker_cik_fixtures.json").read_text(encoding="utf-8"))


def _resolve_cik_py(ticker: str, fixtures: dict | None = None):
    saved = (edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT, dict(edgar._FILING_CIK_CACHE))
    edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = None, 0.0
    edgar._FILING_CIK_CACHE.clear()
    try:
        with patch.object(edgar.yf, "Ticker", side_effect=RuntimeError("yahoo unavailable")), \
                patch.dict(edgar._TEST_TICKER_CIK_FIXTURES, fixtures or {}, clear=True):
            return asyncio.run(edgar._resolve_cik_for_ticker(ticker))
    finally:
        edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = saved[0], saved[1]
        edgar._FILING_CIK_CACHE.clear()
        edgar._FILING_CIK_CACHE.update(saved[2])


_CIK_JS = """
if (args[1]) m.yahooFinance.setTickerCikFixturesForTests(new Map(Object.entries(args[1])));
return (await m.yahooFinance.getSubmissionsForTicker(args[0])).cikPadded;
"""


def _cik_case(name: str, ids: list[str], ticker: str, *expect, routes: dict | None = None,
              fixtures: dict | None = None, **kw) -> Case:
    """`expect` (positional, optional: a pinned case gives expect_py / expect_ts instead)."""
    return Case(
        name=name, ids=ids, py=lambda: _resolve_cik_py(ticker, fixtures),
        ts={"js": _CIK_JS, "args": [ticker, fixtures], "isolate": True},
        routes=routes or {}, **({"expect": expect[0]} if expect else {}), **kw)


ATOM_AAPL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=AAPL&type=&dateb=&owner=include&count=10&output=atom"

CASES += [
    # The injected fixture resolves a listed ticker when Yahoo and SEC's index both fail.
    _cik_case("tool.q4.injected_fixture_resolves_listed_ticker_when_yahoo_and_sec_fail", ["Q4"], "AAPL", "0000320193",
              fixtures=TICKER_CIK_FIXTURES),
    _cik_case("tool.q4.injected_fixture_covers_aaoi", ["Q4"], "AAOI", "0001158114", fixtures=TICKER_CIK_FIXTURES),
    # The seam itself: a ticker the old table never held.
    _cik_case("tool.q4.injected_fixture_resolves_a_ticker_outside_the_old_table", ["Q4"], "FIXT", "0000000042",
              fixtures={"FIXT": "0000000042"}),
    # Production lookup without fixtures: no Yahoo, no SEC index, no atom -> unresolved, even for a former table entry.
    _cik_case("tool.q4.no_fixture_and_no_index_falls_to_atom_and_is_unresolved", ["Q4"], "AAPL", None),
    # Without fixtures the EDGAR atom answers a former table entry.
    _cik_case("tool.q4.no_fixture_atom_answers_a_former_table_entry", ["Q4"], "AAPL", "0000111111",
              routes={ATOM_AAPL: {"body": '<feed><link href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0000111111"/></feed>'}}),
    _cik_case("tool.q4.unlisted_ticker_with_all_lookups_failing_is_unresolved", ["Q4"], "ZZZZ", None),
    _cik_case("tool.q4.sec_ticker_index_wins_over_the_smoke_table", ["Q4"], "AAPL", "0000999999", fixtures=TICKER_CIK_FIXTURES,
              routes={"https://www.sec.gov/files/company_tickers.json": {"json": {"0": {"cik_str": 999999, "ticker": "AAPL", "title": "Fake Co"}}}}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-NWS-007 (Q3i) news coverage state, and Q3h sources=[] in get_company_news
# ═══════════════════════════════════════════════════════════════════════════════════════════════

COMPANY_PROFILE_NO_WEBSITE = {"quoteSummary": {"result": [{"assetProfile": {}, "price": {"shortName": "Test Co", "longName": "Test Co"}}], "error": None}}
NEWS_ROUTES = {**YAHOO_AUTH_ROUTES, "https://query1.finance.yahoo.com/v10/finance/quoteSummary/*": {"json": COMPANY_PROFILE_NO_WEBSITE}}


def _coverage_view(v):
    cov = v["coverage"] if "coverage" in v else v
    return {"state": cov["state"], "failed": [(f["source"], f["status"]) for f in cov["failedSources"]],
            "skipped": [(f["source"], f["status"]) for f in cov["skippedSources"]],
            "action": cov["recommendedNextAction"]}


CASES += [
    # Unconfigured Finnhub is a failed source: coverage PARTIAL, next action CHECK_OFFICIAL_RELEASES.
    Case(name="tool.nws.coverage_unconfigured_source_is_partial_with_check_official_releases", ids=["BR-NWS-007"],
         py=lambda: server._build_coverage({"finnhub": {"status": "UNCONFIGURED"}}),
         ts={"js": """
const r = JSON.parse(await m.yahooFinance.getCompanyNews("TEST", 5, 14, ["finnhub"]));
return r.coverage;
""", "isolate": True},
         routes=NEWS_ROUTES, select=_coverage_view,
         expect={"state": "PARTIAL", "failed": [["finnhub", "UNCONFIGURED"]], "skipped": [], "action": "CHECK_OFFICIAL_RELEASES"}),
    # Q3i: the PARTIAL status set is the same in both runtimes, so a failed source never sits beside FULL.
    Case(name="tool.nws.coverage_website_not_available_failed_source", ids=["BR-NWS-007"],
         py=lambda: server._build_coverage({"company_ir": {"status": "WEBSITE_NOT_AVAILABLE"}}),
         # Both runtimes build the coverage from the same fixed input: since 2.10.0 the Worker's company_ir can no longer produce
         # WEBSITE_NOT_AVAILABLE, so the status cannot be reached through getCompanyNews; the builder is called directly.
         ts={"js": """
return m.news.buildCoverage({ company_ir: { status: "WEBSITE_NOT_AVAILABLE" } });
""", "isolate": True},
         routes=NEWS_ROUTES, select=_coverage_view,
         expect={"state": "PARTIAL", "failed": [["company_ir", "WEBSITE_NOT_AVAILABLE"]], "skipped": [], "action": "CHECK_OFFICIAL_RELEASES"}),

    # Q3h: an explicit sources list is used as given; [] (or only unsupported names) attempts no source.
]


def _q3h_view(payload):
    """The fields Q3h pins, identical in both runtimes."""
    return {
        "requestedSources": payload["requestedSources"],
        "defaultSourcesApplied": payload["defaultSourcesApplied"],
        "attemptedSources": payload["attemptedSources"],
        "sourceStatus": payload["sourceStatus"],
        "status": payload.get("status"),
        "sourceCoverage": payload["sourceCoverage"],
        "coverageState": payload["coverage"]["state"],
        "recommendedNextAction": payload["coverage"]["recommendedNextAction"],
        "warningCodes": [w["code"] for w in payload["warnings"]],
    }


# (name suffix, Python call, Worker call) for the five tools that take `sources`; %s is the sources argument.
_Q3H_TOOLS = [
    ("get_company_news", 'server.get_company_news("TEST", 5, 14, {src})',
     'm.yahooFinance.getCompanyNews("TEST", 5, 14, {src})'),
    ("search_company_news", 'server.search_company_news("TEST", "earnings", "", "", {src}, 5)',
     'm.yahooFinance.searchCompanyNews("TEST", "earnings", "", "", {src}, 5)'),
    ("get_company_press_releases", 'server.get_company_press_releases("TEST", 90, 20, {src})',
     'm.yahooFinance.getCompanyPressReleases("TEST", 90, 20, {src})'),
    ("get_public_event_timeline", 'server.get_public_event_timeline("TEST", "", "", {src}, 50, False)',
     'm.yahooFinance.getPublicEventTimeline("TEST", "", "", {src}, 50, false)'),
    ("verify_company_event", 'server.verify_company_event("TEST", "acquisition", "", "", {src})',
     'm.yahooFinance.verifyCompanyEvent("TEST", "acquisition", "", "", {src})'),
]


def _q3h_case(tool: str, py_call: str, ts_call: str, label: str, sources, expect: dict, ids: list[str], routes: dict | None = None) -> Case:
    src_py = repr(sources)
    src_ts = json.dumps(sources)
    return Case(
        name=f"tool.q3h.{tool}_{label}", ids=ids,
        py=lambda: json.loads(asyncio.run(eval(py_call.format(src=src_py), {"server": server}))),
        ts={"js": f"m.response.setWorkerEnv({{}}); return JSON.parse(await {ts_call.format(src=src_ts)});", "isolate": True},
        routes=routes or {}, select=_q3h_view, expect=expect)


def _q3h_none_selected(requested: list, warning_codes: list) -> dict:
    return {
        "requestedSources": requested, "defaultSourcesApplied": False, "attemptedSources": [], "sourceStatus": {},
        "status": "SOURCE_LIMITED_NOT_FOUND", "sourceCoverage": "PARTIAL", "coverageState": "PARTIAL",
        "recommendedNextAction": "CHECK_OFFICIAL_RELEASES", "warningCodes": warning_codes,
    }


for _tool, _py, _ts in _Q3H_TOOLS:
    # get_company_press_releases also reports that it found no official release source.
    _extra = ["NO_OFFICIAL_RELEASE_SOURCE"] if _tool == "get_company_press_releases" else []
    CASES.append(_q3h_case(_tool, _py, _ts, "empty_sources_attempts_none", [], _q3h_none_selected([], _extra), ["Q3h"]))
    CASES.append(_q3h_case(_tool, _py, _ts, "unsupported_sources_attempt_none", ["bogus"],
                           _q3h_none_selected(["bogus"], ["SOURCE_UNSUPPORTED"] + _extra), ["Q3h"]))


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-PRC-014 (slope), BR-PRC-033 (volume ratio), BR-SHR-006 (batch cap): daily-series tools
# Python: pricing tool with yfinance and the completed-history loader stubbed (a CURRENT, fully completed series).
# Worker: the same tool on a canned Yahoo chart with the clock fixed after the last session.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

SERIES_END = "2026-10-09"
SERIES_NOW = _epoch(SERIES_END, "22:00", "UTC")


def _series_prepared(closes: list, volumes: list, adj: list | None = None) -> dict:
    days = _weekdays(SERIES_END, len(closes))
    index = pd.DatetimeIndex([pd.Timestamp(d) for d in days])
    adj_closes = closes if adj is None else adj
    frame = pd.DataFrame({
        "Close": [float("nan") if c is None else c for c in closes],
        "Adj Close": [float("nan") if c is None else c for c in adj_closes],
        "Volume": volumes}, index=index)
    return {"completed": frame, "timezoneName": "UTC", "freshnessStatus": "CURRENT", "expectedCompletedDate": days[-1],
            "excludedIncompleteBar": False, "retryAttempted": False}


def _series_chart(closes: list, volumes: list, adj: list | None = None) -> dict:
    days = _weekdays(SERIES_END, len(closes))
    stamps = [_epoch(d, "20:00", "UTC") for d in days]
    return {"chart": {"result": [{
        "meta": {"exchangeTimezoneName": "UTC", "regularMarketTime": stamps[-1], "currency": "USD"},
        "timestamp": stamps,
        "indicators": {"quote": [{"close": closes, "volume": volumes}],
                       "adjclose": [{"adjclose": closes if adj is None else adj}]}}]}}


def _series_case(name: str, ids: list[str], tool: str, args: list, closes: list, volumes: list | None = None,
                 adj: list | None = None, **kw) -> Case:
    volumes = volumes if volumes is not None else [1000] * len(closes)
    py_fn, ts_fn = {"slope": (pricing.get_price_slope, "getPriceSlope"),
                    "volume_ratio": (pricing.get_volume_ratio, "getVolumeRatio")}[tool]

    def py():
        with patch.object(pricing.yf, "Ticker", MagicMock()), \
                patch.object(pricing, "_load_completed_daily_history", AsyncMock(return_value=_series_prepared(closes, volumes, adj))):
            return asyncio.run(py_fn(*args))

    body = f"return JSON.parse(await m.yahooFinance.{ts_fn}(...args));"
    return Case(
        name=name, ids=ids, py=py, ts={"js": _ts_with_clock(SERIES_NOW, body), "args": args, "isolate": True},
        routes={"https://query1.finance.yahoo.com/v8/finance/chart/*": {"json": _series_chart(closes, volumes, adj)}}, **kw)


def _pick(*keys):
    return lambda v: {k: v.get(k) for k in keys}


def _gated_slope_seven() -> str:
    """get_price_slope for seven tickers through the registered Python tool, i.e. through the tool-call gate."""
    closes = [100.00] + [100.0] * 4 + [103.25]
    tool = server.yfinance_server._tool_manager._tools["get_price_slope"].fn
    with patch.object(pricing.yf, "Ticker", MagicMock()), \
            patch.object(pricing, "_load_completed_daily_history", AsyncMock(return_value=_series_prepared(closes, [1000] * len(closes)))):
        return asyncio.run(tool(ticker=["T1", "T2", "T3", "T4", "T5", "T6", "T7"], days=5))


SLOPE_KEYS = ("status", "slopePct", "direction", "observationCount", "priceBasis", "startClose", "endClose", "fallbackUsed")
FLAT_SIX = [100.0, 100.0, 100.0, 100.0, 100.0]

CASES += [
    # ── BR-PRC-014 ───────────────────────────────────────────────────────────────────────────
    _series_case("prc.slope.card_example_up", ["BR-PRC-014"], "slope", ["TST", 5], [100.00] + FLAT_SIX[:4] + [103.25],
                 select=_pick(*SLOPE_KEYS),
                 expect={"status": "OK", "slopePct": 3.25, "direction": "UP", "observationCount": 6,
                         "priceBasis": "ADJUSTED_CLOSE", "startClose": 100.0, "endClose": 103.25, "fallbackUsed": False}),
    _series_case("prc.slope.move_under_half_percent_is_flat", ["BR-PRC-014"], "slope", ["TST", 5], [100.00] + FLAT_SIX[:4] + [100.40],
                 select=_pick("slopePct", "direction"), expect={"slopePct": 0.4, "direction": "FLAT"}),
    _series_case("prc.slope.exactly_half_percent_is_up", ["BR-PRC-014"], "slope", ["TST", 5], [100.00] + FLAT_SIX[:4] + [100.50],
                 select=_pick("slopePct", "direction"), expect={"slopePct": 0.5, "direction": "UP"}),
    _series_case("prc.slope.down", ["BR-PRC-014"], "slope", ["TST", 5], [100.00] + FLAT_SIX[:4] + [90.00],
                 select=_pick("slopePct", "direction"), expect={"slopePct": -10.0, "direction": "DOWN"}),
    _series_case("prc.slope.uses_unadjusted_closes_when_an_adjusted_one_is_missing", ["BR-PRC-014"], "slope", ["TST", 5],
                 [100.00, 100.0, 100.0, 100.0, 100.0, 105.0], adj=[100.0, 100.0, None, 100.0, 100.0, 105.0],
                 select=_pick("priceBasis", "fallbackUsed", "slopePct"),
                 expect={"priceBasis": "UNADJUSTED_CLOSE", "fallbackUsed": True, "slopePct": 5.0}),
    _series_case("prc.slope.too_few_completed_bars_is_an_error", ["BR-PRC-014"], "slope", ["TST", 5], [100.0, 101.0, 102.0, 103.0],
                 select=_pick("error", "message"),
                 expect={"error": True, "message": "Insufficient completed price data for TST: need 6 bars"}),
    _series_case("prc.slope.days_below_one_is_clamped_to_one", ["BR-PRC-014"], "slope", ["TST", 0], [100.0, 110.0],
                 select=_pick("observationCount", "slopePct"), expect={"observationCount": 2, "slopePct": 10.0}),
    # Q3b: Python does not validate the ticker in the single-ticker path and int(days) raises on a non-number;
    # the Worker also skips ticker validation here and falls back to 5 days.
    _series_case("prc.slope.q3b_invalid_ticker_is_not_rejected_by_either_runtime", ["BR-PRC-014", "BR-ENV-007"], "slope",
                 ["AAPL$", 5], [100.00] + FLAT_SIX[:4] + [103.25], select=_pick("status", "ticker", "slopePct"),
                 mode="pinned", decision="Q3b", expect={"status": "OK", "ticker": "AAPL$", "slopePct": 3.25}),
    _series_case("prc.slope.q3b_non_numeric_days_python_raises_worker_defaults_to_five", ["BR-PRC-014"], "slope",
                 ["TST", "abc"], [100.00] + FLAT_SIX[:4] + [103.25], select=_pick("status", "days", "slopePct", "__error__"),
                 mode="pinned", decision="Q3b",
                 expect_py={"__error__": "ValueError: invalid literal for int() with base 10: 'abc'"},
                 expect_ts={"status": "OK", "days": 5, "slopePct": 3.25, "__error__": None}),
]

VR_KEYS = ("status", "lastVolume", "avgVolume10d", "avgVolume90d", "ratio10d", "ratio90d", "volumeFlag")
PRIOR10 = [1_800_000] * 10

CASES += [
    # ── BR-PRC-033 ───────────────────────────────────────────────────────────────────────────
    _series_case("prc.volume_ratio.card_example_high", ["BR-PRC-033"], "volume_ratio", ["TST", 10], [50.0] * 11,
                 volumes=PRIOR10 + [3_000_000], select=_pick(*VR_KEYS),
                 expect={"status": "OK", "lastVolume": 3000000, "avgVolume10d": 1800000, "avgVolume90d": None,
                         "ratio10d": 1.667, "ratio90d": None, "volumeFlag": "HIGH"}),
    _series_case("prc.volume_ratio.below_point_seven_is_low", ["BR-PRC-033"], "volume_ratio", ["TST", 10], [50.0] * 11,
                 volumes=[2_000_000] * 10 + [1_000_000], select=_pick("ratio10d", "volumeFlag"),
                 expect={"ratio10d": 0.5, "volumeFlag": "LOW"}),
    _series_case("prc.volume_ratio.exactly_one_point_five_is_normal", ["BR-PRC-033"], "volume_ratio", ["TST", 10], [50.0] * 11,
                 volumes=[2_000_000] * 10 + [3_000_000], select=_pick("ratio10d", "volumeFlag"),
                 expect={"ratio10d": 1.5, "volumeFlag": "NORMAL"}),
    _series_case("prc.volume_ratio.exactly_point_seven_is_normal", ["BR-PRC-033"], "volume_ratio", ["TST", 10], [50.0] * 11,
                 volumes=[2_000_000] * 10 + [1_400_000], select=_pick("ratio10d", "volumeFlag"),
                 expect={"ratio10d": 0.7, "volumeFlag": "NORMAL"}),
    _series_case("prc.volume_ratio.fewer_than_ten_prior_sessions_gives_no_ratio", ["BR-PRC-033"], "volume_ratio", ["TST", 10], [50.0] * 10,
                 volumes=[2_000_000] * 9 + [3_000_000], select=_pick("status", "avgVolume10d", "ratio10d", "volumeFlag"),
                 expect={"status": "OK", "avgVolume10d": None, "ratio10d": None, "volumeFlag": None}),
    _series_case("prc.volume_ratio.ninety_day_window_when_enough_history", ["BR-PRC-033"], "volume_ratio", ["TST", 10], [50.0] * 101,
                 volumes=[1_000_000] * 100 + [2_000_000], select=_pick("avgVolume90d", "ratio90d", "ratio10d", "volumeFlag"),
                 expect={"avgVolume90d": 1000000, "ratio90d": 2.0, "ratio10d": 2.0, "volumeFlag": "HIGH"}),
    # Q3c: a zero-volume completed session. Python's `if last_vol and avg` gives null ratio and flag; the Worker
    # divides and reports ratio 0 with flag LOW.
    _series_case("prc.volume_ratio.q3c_zero_volume_session_python_null_worker_zero_low", ["BR-PRC-033"], "volume_ratio", ["TST", 10], [50.0] * 11,
                 volumes=PRIOR10 + [0], select=_pick("status", "lastVolume", "ratio10d", "volumeFlag"),
                 mode="pinned", decision="Q3c",
                 expect_py={"status": "OK", "lastVolume": 0, "ratio10d": None, "volumeFlag": None},
                 expect_ts={"status": "OK", "lastVolume": 0, "ratio10d": 0, "volumeFlag": "LOW"}),
    # Q3c (card, second defect): the Worker divides by the integer-rounded average, Python by the unrounded one;
    # Python round() is half-even (1000.5 -> 1000) and JS Math.round half-up (1000.5 -> 1001).
    _series_case("prc.volume_ratio.q3c_average_rounding_differs_between_runtimes", ["BR-PRC-033"], "volume_ratio", ["TST", 10], [50.0] * 11,
                 volumes=[1000] * 5 + [1001] * 5 + [1000], select=_pick("avgVolume10d", "ratio10d", "volumeFlag"),
                 mode="pinned", decision="Q3c",
                 expect_py={"avgVolume10d": 1000, "ratio10d": 1.0, "volumeFlag": "NORMAL"},
                 expect_ts={"avgVolume10d": 1001, "ratio10d": 0.999, "volumeFlag": "NORMAL"}),

    # ── BR-SHR-006 (Q3c, 2.12.0): seven tickers is over get_price_slope's cap of 5 ──────────────
    # Both runtimes' tool-call gate rejects the call with BATCH_LIMIT_EXCEEDED before any fetch (no more truncation
    # to five with _truncated). Python goes through the registered, gated tool wrapper.
    Case(name="prc.batch.q3c_seven_tickers_are_rejected_over_the_cap_in_both_runtimes", ids=["BR-SHR-006"],
         py=lambda: _gated_slope_seven(),
         ts={"js": _ts_with_clock(SERIES_NOW, 'return JSON.parse(await m.tools.callTool("get_price_slope", { ticker: ["T1", "T2", "T3", "T4", "T5", "T6", "T7"], days: 5 }));'),
             "isolate": True},
         select=lambda v: ({"error": v["error"]["code"], "message": v["error"]["message"]} if v.get("ok") is False
                           else {"tickers": sorted(k for k in v if not k.startswith("_"))}),
         expect={"error": "BATCH_LIMIT_EXCEEDED", "message": "Too many tickers: 7. Maximum is 5 per call for get_price_slope."}),
    _series_case("prc.batch.five_tickers_are_processed_by_both", ["BR-SHR-006"], "slope",
                 [["T1", "T2", "T3", "T4", "T5"], 5], [100.00] + FLAT_SIX[:4] + [103.25],
                 select=lambda v: {"tickers": sorted(k for k in v if not k.startswith("_")),
                                   "slopes": sorted({v[k]["slopePct"] for k in v if not k.startswith("_")}),
                                   "truncated": "_truncated" in v},
                 expect={"tickers": ["T1", "T2", "T3", "T4", "T5"], "slopes": [3.25], "truncated": False}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-PRC-019 / BR-PRC-021: price CAGR over 1, 3 and 5 years (get_price_stats)
# A sparse completed-close series is enough: only the first bar on or after the horizon start and the last bar
# matter. Python: pricing.get_price_stats with yfinance and the history loader stubbed. Worker: getPriceStats on
# canned quoteSummary + chart responses.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from types import SimpleNamespace  # noqa: E402

STATS_END = "2026-10-09"
STATS_NOW = _epoch(STATS_END, "22:00", "UTC")


def _stats_case(name: str, ids: list[str], ticker: str, bars: list[tuple[str, float]], keys: tuple, **kw) -> Case:
    days = [d for d, _ in bars]
    closes = [c for _, c in bars]

    def py():
        index = pd.DatetimeIndex([pd.Timestamp(d) for d in days])
        frame = pd.DataFrame({"Close": closes, "Adj Close": closes, "Volume": [1000] * len(days)}, index=index)
        prepared = {"completed": frame, "timezoneName": "UTC", "freshnessStatus": "CURRENT", "expectedCompletedDate": days[-1],
                    "excludedIncompleteBar": False, "retryAttempted": False}
        fast = SimpleNamespace(last_price=closes[-1], previous_close=closes[-1], currency="USD", year_high=closes[-1],
                               year_low=closes[0], fifty_day_average=closes[-1], two_hundred_day_average=closes[-1])
        company = SimpleNamespace(fast_info=fast, info={"regularMarketTime": _epoch(days[-1], "20:00", "UTC"), "marketState": "CLOSED"})
        with patch.object(pricing.yf, "Ticker", lambda _t: company), \
                patch.object(pricing, "_load_completed_daily_history", AsyncMock(return_value=prepared)):
            return asyncio.run(pricing.get_price_stats(ticker))

    stamps = [_epoch(d, "20:00", "UTC") for d in days]
    chart = {"chart": {"result": [{
        "meta": {"exchangeTimezoneName": "UTC", "regularMarketTime": stamps[-1], "currency": "USD"},
        "timestamp": stamps,
        "indicators": {"quote": [{"close": closes, "volume": [1000] * len(days)}], "adjclose": [{"adjclose": closes}]}}]}}
    summary = {"quoteSummary": {"result": [{
        "price": {"currency": "USD", "regularMarketPrice": {"raw": closes[-1]}, "regularMarketPreviousClose": {"raw": closes[-1]},
                  "regularMarketTime": {"raw": stamps[-1]}, "marketState": "CLOSED"},
        "summaryDetail": {}}]}}
    return Case(
        name=name, ids=ids, py=py,
        ts={"js": _ts_with_clock(STATS_NOW, f'return JSON.parse(await m.yahooFinance.getPriceStats("{ticker}"));'), "isolate": True},
        routes={**YAHOO_AUTH_ROUTES,
                f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?*": {"json": summary},
                f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?*": {"json": chart}},
        select=_pick(*keys), **kw)


CAGR_KEYS = ("cagr1y", "cagr3y", "cagr5y")

CASES += [
    # Card BR-PRC-021: 5 years ago $50, latest $100 -> (100/50)^(1/5) - 1 = 14.8698%. Shorter horizons have no
    # earlier bar than the last one, so they are null.
    _stats_case("prc.cagr.five_year_card_example", ["BR-PRC-021"], "CGA", [("2021-10-11", 50.0), (STATS_END, 100.0)], CAGR_KEYS,
                expect={"cagr1y": None, "cagr3y": None, "cagr5y": 14.8698}),
    # Card BR-PRC-019: close 100 three years ago and 133.10 now -> cagr3y = 10.0000.
    _stats_case("prc.cagr.three_year_card_example", ["BR-PRC-019"], "CGB", [("2023-10-11", 100.0), (STATS_END, 133.10)], ("cagr3y",),
                expect={"cagr3y": 10.0}),
    _stats_case("prc.cagr.start_close_not_positive_is_null", ["BR-PRC-019", "BR-PRC-021"], "CGC", [("2023-10-11", 0.0), (STATS_END, 133.10)], ("cagr3y",),
                expect={"cagr3y": None}),
    # Q3i: with only ~3 years of history the whole series is the "5 year" window, but the exponent is still 1/5,
    # so a 33.1% total gain reports as a 5.9% five-year CAGR instead of null (BR-PRC-019 / BR-PRC-021 suspected defect).
    _stats_case("prc.cagr.q3i_short_history_five_year_cagr_uses_nominal_five_years", ["BR-PRC-019", "BR-PRC-021"], "CGD",
                [("2023-10-11", 100.0), (STATS_END, 133.10)], ("cagr3y", "cagr5y"),
                mode="pinned", decision="Q3i", expect={"cagr3y": 10.0, "cagr5y": 5.8853}),
    # Finding (Phase 2, for Q3i triage): the horizon start differs. Python uses a calendar offset
    # (2026-10-09 - 3y = 2023-10-09, bar included); the Worker uses 3 x 365.25 days (cutoff 2023-10-10 ~06:00Z, the
    # 2023-10-09 20:00Z bar excluded), so the two pick different start bars (yahoo-finance.ts:2764-2774).
    _stats_case("prc.cagr.start_bar_selection_differs_on_the_horizon_boundary", ["BR-PRC-021"], "CGE",
                [("2023-10-09", 75.0), ("2025-10-09", 90.0), (STATS_END, 100.0)], ("cagr3y",),
                mode="pinned", decision="FINDING: Python (calendar DateOffset) and Worker (365.25-day years) pick different CAGR start bars at the horizon boundary",
                expect_py={"cagr3y": 10.0642}, expect_ts={"cagr3y": 3.5744}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-PRC-022 (Q3d): price target bracket and tag (get_price_target_bracket)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

BRACKET_NOW = _epoch("2026-10-09", "12:00", "UTC")
BRACKET_KEYS = ("referenceTargetPct", "bracket", "inferredTag", "distanceToTargetPct", "invertedFlag", "priceAtOrAboveTarget",
                "dataDate", "error", "message")


def _bracket_case(name: str, ids: list[str], price: float | None, target: float, keys=BRACKET_KEYS, market_time: int | None = None,
                  post=None, via: str | None = None, select=None, **kw) -> Case:
    """One get_price_target_bracket scenario. `via` "envelope" calls calculate_price_target_distance through the V2 envelope
    (Python `_envelope_tool_result`, Worker `callToolResult`); None calls the bracket function in each runtime."""
    def py():
        from yfmcp.envelope import _envelope_tool_result

        info = {"regularMarketTime": market_time, "marketState": "CLOSED"} if market_time else {"marketState": "CLOSED"}
        company = SimpleNamespace(fast_info={"lastPrice": price}, info=info)

        async def call():
            if via == "envelope":
                return _envelope_tool_result("calculate_price_target_distance",
                                             await server.calculate_price_target_distance("TST", reference_target_price=target))
            return await server.get_price_target_bracket("TST", target)

        with patch.object(server.yf, "Ticker", lambda _t: company):
            return asyncio.run(call())

    summary = {"quoteSummary": {"result": [{"price": {
        "currency": "USD", "quoteType": "EQUITY", "marketState": "CLOSED",
        **({"regularMarketPrice": {"raw": price}} if price is not None else {}),
        **({"regularMarketTime": {"raw": market_time}} if market_time else {})}, "summaryDetail": {}, "defaultKeyStatistics": {}}]}}
    if via == "envelope":
        body = (f'm.response.setWorkerEnv({{}}); return (await m.tools.callToolResult("calculate_price_target_distance", '
                f'{{ticker: "TST", reference_target_price: {target}}})).value;')
    else:
        body = f'return JSON.parse(await m.yahooFinance.getPriceTargetBracket("TST", {target}));'
    return Case(
        name=name, ids=ids, py=py,
        ts={"js": _ts_with_clock(BRACKET_NOW, body), "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, "https://query1.finance.yahoo.com/v10/finance/quoteSummary/TST?*": {"json": summary}},
        select=select or ((lambda v: post(_pick(*keys)(v))) if post else _pick(*keys)), **kw)


QUOTE_TIME = _epoch("2026-10-08", "20:00", "UTC")
CASES += [
    # Card: price $70, reference target $100 -> 70.0, STRONG_BUY, LONG, distance 42.9.
    _bracket_case("prc.bracket.card_example_strong_buy_long", ["BR-PRC-022"], 70.0, 100.0, market_time=QUOTE_TIME,
                  expect={"referenceTargetPct": 70.0, "bracket": "STRONG_BUY", "inferredTag": "LONG", "distanceToTargetPct": 42.9,
                          "invertedFlag": False, "priceAtOrAboveTarget": False, "dataDate": "2026-10-08", "error": None, "message": None}),
    _bracket_case("prc.bracket.eighty_five_pct_is_acceptable_long_boundary", ["BR-PRC-022"], 85.0, 100.0, market_time=QUOTE_TIME,
                  keys=("referenceTargetPct", "bracket", "inferredTag"),
                  expect={"referenceTargetPct": 85.0, "bracket": "ACCEPTABLE", "inferredTag": "NEAR"}),
    _bracket_case("prc.bracket.exactly_75_is_strong_buy", ["BR-PRC-022"], 75.0, 100.0, market_time=QUOTE_TIME,
                  keys=("referenceTargetPct", "bracket", "inferredTag"),
                  expect={"referenceTargetPct": 75.0, "bracket": "STRONG_BUY", "inferredTag": "LONG"}),
    _bracket_case("prc.bracket.exactly_100_is_caution_and_inverted", ["BR-PRC-022"], 100.0, 100.0, market_time=QUOTE_TIME,
                  keys=("referenceTargetPct", "bracket", "inferredTag", "invertedFlag"),
                  expect={"referenceTargetPct": 100.0, "bracket": "CAUTION", "inferredTag": "INVERTED", "invertedFlag": True}),
    # 2.8.0 (Q14-N3): priceAtOrAboveTarget = currentPrice >= referenceTargetPrice on the raw prices.
    _bracket_case("prc.bracket.price_at_or_above_target_false_below_the_target", ["BR-PRC-022", "Q14-N3"], 99.0, 100.0,
                  market_time=QUOTE_TIME, keys=("referenceTargetPct", "priceAtOrAboveTarget"),
                  expect={"referenceTargetPct": 99.0, "priceAtOrAboveTarget": False}),
    _bracket_case("prc.bracket.price_at_or_above_target_true_when_equal", ["BR-PRC-022", "Q14-N3"], 100.0, 100.0,
                  market_time=QUOTE_TIME, keys=("referenceTargetPct", "priceAtOrAboveTarget"),
                  expect={"referenceTargetPct": 100.0, "priceAtOrAboveTarget": True}),
    _bracket_case("prc.bracket.price_at_or_above_target_true_above_the_target", ["BR-PRC-022", "Q14-N3"], 120.0, 100.0,
                  market_time=QUOTE_TIME, keys=("referenceTargetPct", "priceAtOrAboveTarget"),
                  expect={"referenceTargetPct": 120.0, "priceAtOrAboveTarget": True}),
    # The flag reads the raw prices: 99.96 rounds to 100.0% (invertedFlag true) but is still below the target.
    _bracket_case("prc.bracket.price_at_or_above_target_uses_raw_prices_not_the_rounded_pct_below", ["BR-PRC-022", "Q14-N3"], 99.96, 100.0,
                  market_time=QUOTE_TIME, keys=("referenceTargetPct", "invertedFlag", "priceAtOrAboveTarget"),
                  expect={"referenceTargetPct": 100.0, "invertedFlag": True, "priceAtOrAboveTarget": False}),
    _bracket_case("prc.bracket.price_at_or_above_target_uses_raw_prices_not_the_rounded_pct_above", ["BR-PRC-022", "Q14-N3"], 100.04, 100.0,
                  market_time=QUOTE_TIME, keys=("referenceTargetPct", "invertedFlag", "priceAtOrAboveTarget"),
                  expect={"referenceTargetPct": 100.0, "invertedFlag": True, "priceAtOrAboveTarget": True}),
    # A price that is not positive gives null (the target is validated positive before this point).
    _bracket_case("prc.bracket.price_at_or_above_target_null_for_a_non_positive_price", ["BR-PRC-022", "Q14-N3"], -5.0, 100.0,
                  market_time=QUOTE_TIME, keys=("referenceTargetPct", "priceAtOrAboveTarget"),
                  expect={"referenceTargetPct": -5.0, "priceAtOrAboveTarget": None}),
    # No price at all is an error payload in both runtimes, so there is no flag to read.
    _bracket_case("prc.bracket.price_at_or_above_target_missing_price_is_an_error_without_the_flag", ["BR-PRC-022", "Q14-N3"], None, 100.0,
                  market_time=QUOTE_TIME, keys=("error", "message", "priceAtOrAboveTarget"),
                  expect={"error": True, "message": "No price data for TST", "priceAtOrAboveTarget": None}),
    _bracket_case("prc.bracket.above_target_is_avoid", ["BR-PRC-022"], 120.0, 100.0, market_time=QUOTE_TIME,
                  keys=("referenceTargetPct", "bracket", "inferredTag", "distanceToTargetPct"),
                  expect={"referenceTargetPct": 120.0, "bracket": "AVOID", "inferredTag": "INVERTED", "distanceToTargetPct": -16.7}),
    _bracket_case("prc.bracket.below_forty_pct_is_speculative", ["BR-PRC-022"], 39.9, 100.0, market_time=QUOTE_TIME,
                  keys=("referenceTargetPct", "bracket", "inferredTag"),
                  expect={"referenceTargetPct": 39.9, "bracket": "STRONG_BUY", "inferredTag": "SPECULATIVE"}),
    _bracket_case("prc.bracket.non_positive_target_is_rejected", ["BR-PRC-022"], 70.0, 0.0, market_time=QUOTE_TIME,
                  keys=("error", "message"),
                  expect={"error": True, "message": "reference_target_price (or io_pt alias) must be a positive number"}),
    # Q3d: thresholds compare the value rounded to 1 dp: 75.04 reads as 75.0 and is STRONG_BUY.
    _bracket_case("prc.bracket.q3d_threshold_uses_the_rounded_percentage", ["BR-PRC-022"], 75.04, 100.0, market_time=QUOTE_TIME,
                  keys=("referenceTargetPct", "bracket"), mode="pinned", decision="Q3d",
                  expect={"referenceTargetPct": 75.0, "bracket": "STRONG_BUY"}),
    # Q3d: Python round() is half-even, JS toFixed(1) rounds an exact tie up: 301/400 x 100 = 75.25 exactly.
    _bracket_case("prc.bracket.q3d_rounding_differs_python_round_vs_ts_tofixed", ["BR-PRC-022"], 301.0, 400.0, market_time=QUOTE_TIME,
                  keys=("referenceTargetPct", "bracket"), mode="pinned", decision="Q3d",
                  expect_py={"referenceTargetPct": 75.2, "bracket": "ACCEPTABLE"},
                  expect_ts={"referenceTargetPct": 75.3, "bracket": "ACCEPTABLE"}),
    # Q3d: with no quote time the Worker reports today as dataDate; Python reports null.
    _bracket_case("prc.bracket.q3d_missing_quote_time_python_null_worker_today", ["BR-PRC-022"], 70.0, 100.0,
                  keys=("dataDate", "bracket"), mode="pinned", decision="Q3d",
                  # `new Date()` ignores the Date.now override, so the Worker's date is the real current date: only its
                  # presence (a YYYY-MM-DD string) is pinned.
                  post=lambda v: {"dataDate": None if v["dataDate"] is None else ("<today YYYY-MM-DD>" if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v["dataDate"]) else v["dataDate"]),
                                  "bracket": v["bracket"]},
                  expect_py={"dataDate": None, "bracket": "STRONG_BUY"}, expect_ts={"dataDate": "<today YYYY-MM-DD>", "bracket": "STRONG_BUY"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-FND-004 / BR-FND-006 / BR-FND-007: credit health (get_credit_health)
# Python: server.get_credit_health on stubbed yfinance quarterly statements.
# Worker: getCreditHealth on a canned fundamentals-timeseries response.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

M = 1_000_000
QUARTERS = ["2026-06-30", "2026-03-31", "2025-12-31", "2025-09-30"]  # newest first
CREDIT_NOW = _epoch("2026-10-09", "12:00", "UTC")

# Python statement row names and the Worker timeseries types for each synthetic field.
_PY_ROWS = {"debt": "Total Debt", "long_term_debt": "Long Term Debt", "cash": "Cash And Cash Equivalents",
            "op": "Operating Income", "da": "Reconciled Depreciation", "int": "Interest Expense Non Operating", "ebitda": "EBITDA"}
_TS_TYPES = {"debt": "quarterlyTotalDebt", "cash": "quarterlyCashAndCashEquivalents", "op": "quarterlyOperatingIncome",
             "da": "quarterlyReconciledDepreciation", "int": "quarterlyInterestExpenseNonOperating", "ebitda": "quarterlyEBITDA"}


def _frame(rows: dict, dates: list[str]) -> pd.DataFrame:
    data = {name: [vals[i] if i < len(vals) else float("nan") for i in range(len(dates))] for name, vals in rows.items()}
    return pd.DataFrame(data, index=[pd.Timestamp(d) for d in dates]).T


def _timeseries(rows: dict, dates: list[str]) -> dict:
    result = []
    for field, vals in rows.items():
        if field not in _TS_TYPES:  # e.g. "Long Term Debt": the Worker never requests it
            continue
        t = _TS_TYPES[field]
        result.append({"meta": {"type": [t]}, t: [{"asOfDate": dates[i], "reportedValue": {"raw": v}}
                                                  for i, v in enumerate(vals) if v is not None and i < len(dates)]})
    return {"timeseries": {"result": result}}


def _credit_case(name: str, ids: list[str], balance: dict, income: dict, keys: tuple, income_dates: list[str] | None = None,
                 **kw) -> Case:
    """`balance` / `income`: field -> values newest first (None = missing)."""
    income_dates = income_dates or QUARTERS[: max(len(v) for v in income.values())]
    bs_dates = QUARTERS[:1]

    def py():
        bs_rows = {_PY_ROWS[k]: [float("nan") if x is None else x for x in v] for k, v in balance.items()}
        inc_rows = {_PY_ROWS[k]: [float("nan") if x is None else x for x in v] for k, v in income.items()}
        company = SimpleNamespace(quarterly_balance_sheet=_frame(bs_rows, bs_dates), quarterly_income_stmt=_frame(inc_rows, income_dates))
        with patch.object(server.yf, "Ticker", lambda _t: company):
            return asyncio.run(server.get_credit_health("TST"))

    base = "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/TST?type="
    routes = {
        base + "quarterlyTotalDebt*": {"json": _timeseries(balance, bs_dates)},
        base + "quarterlyEBITDA*": {"json": _timeseries(income, income_dates)},
    }
    return Case(
        name=name, ids=ids, py=py,
        ts={"js": _ts_with_clock(CREDIT_NOW, 'return JSON.parse(await m.yahooFinance.getCreditHealth("TST"));'), "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, **routes},
        select=lambda v: {**{k: v.get(k) for k in keys}, **({"warningCodes": sorted(w["code"] for w in v["warnings"])} if "warningCodes" in keys else {})},
        **kw)


RATIO_KEYS = ("netDebtUsd", "operationalEbitdaUsd", "operationalEbitdaSource", "netDebtToEbitda",
              "interestCoverageEbit", "interestCoverageEbitda", "creditStressFlag", "debtTier")


def _credit_inputs(debt, cash, op_q, da_q, int_q, quarters=4):
    return ({"debt": [debt], "cash": [cash]}, {"op": [op_q] * quarters, "da": [da_q] * quarters, "int": [int_q] * quarters})


CASES += [
    # Card BR-FND-004: debt $500M, cash $20M, TTM op income $120M, D&A $40M, interest $50M.
    _credit_case("fnd.credit.card_ttm_example_leverage_and_coverage", ["BR-FND-004", "BR-FND-006"],
                 *_credit_inputs(500 * M, 20 * M, 30 * M, 10 * M, 12.5 * M), RATIO_KEYS,
                 expect={"netDebtUsd": 480 * M, "operationalEbitdaUsd": 160 * M, "operationalEbitdaSource": "ttm_operating_income_plus_da",
                         "netDebtToEbitda": 3.0, "interestCoverageEbit": 2.4, "interestCoverageEbitda": 3.2,
                         "creditStressFlag": True, "debtTier": "ELEVATED"}),
    # Card BR-FND-006: debt $500M, cash $100M; 4 quarters of op income 40M, D&A 10M, interest 8M.
    _credit_case("fnd.credit.card_net_debt_and_coverage_example", ["BR-FND-006"],
                 *_credit_inputs(500 * M, 100 * M, 40 * M, 10 * M, 8 * M), RATIO_KEYS,
                 expect={"netDebtUsd": 400 * M, "operationalEbitdaUsd": 200 * M, "operationalEbitdaSource": "ttm_operating_income_plus_da",
                         "netDebtToEbitda": 2.0, "interestCoverageEbit": 5.0, "interestCoverageEbitda": 6.25,
                         "creditStressFlag": False, "debtTier": "MODERATE"}),
    # Card BR-FND-007: leverage 3.10x and EBIT coverage 2.40x -> stressed flag, ELEVATED.
    _credit_case("fnd.credit.card_stress_flag_and_elevated_tier", ["BR-FND-007"],
                 {"debt": [410 * M], "cash": [100 * M]}, {"op": [15 * M] * 4, "da": [10 * M] * 4, "int": [6.25 * M] * 4},
                 ("netDebtToEbitda", "interestCoverageEbit", "creditStressFlag", "debtTier"),
                 expect={"netDebtToEbitda": 3.1, "interestCoverageEbit": 2.4, "creditStressFlag": True, "debtTier": "ELEVATED"}),
    # Tier boundaries (net debt / operational EBITDA of 100M): <1 CLEAN, <=2.5 MODERATE, <=4 ELEVATED, >4 STRESSED.
    *[_credit_case(f"fnd.credit.tier_boundary_{label}", ["BR-FND-007"],
                   {"debt": [int(net * M) + 10 * M], "cash": [10 * M]}, {"op": [20 * M] * 4, "da": [5 * M] * 4, "int": [1 * M] * 4},
                   ("netDebtToEbitda", "debtTier"), expect={"netDebtToEbitda": ratio, "debtTier": tier})
      for label, net, ratio, tier in [("0_9x_is_clean", 90, 0.9, "CLEAN"), ("1_0x_is_moderate", 100, 1.0, "MODERATE"),
                                     ("2_5x_is_moderate", 250, 2.5, "MODERATE"), ("4_0x_is_elevated", 400, 4.0, "ELEVATED"),
                                     ("4_5x_is_stressed", 450, 4.5, "STRESSED")]],
    # Stress needs both leverage above 2.5x and EBIT coverage under 3x: coverage of exactly 3.00 is not stressed.
    _credit_case("fnd.credit.coverage_of_exactly_three_is_not_stressed", ["BR-FND-007"],
                 {"debt": [410 * M], "cash": [100 * M]}, {"op": [15 * M] * 4, "da": [10 * M] * 4, "int": [5 * M] * 4},
                 ("netDebtToEbitda", "interestCoverageEbit", "creditStressFlag"),
                 expect={"netDebtToEbitda": 3.1, "interestCoverageEbit": 3.0, "creditStressFlag": False}),
    _credit_case("fnd.credit.net_cash_gives_negative_leverage_and_clean_tier", ["BR-FND-007"],
                 {"debt": [50 * M], "cash": [250 * M]}, {"op": [40 * M] * 4, "da": [10 * M] * 4, "int": [1 * M] * 4},
                 ("netDebtUsd", "netDebtToEbitda", "debtTier", "creditStressFlag"),
                 expect={"netDebtUsd": -200 * M, "netDebtToEbitda": -1.0, "debtTier": "CLEAN", "creditStressFlag": False}),
    _credit_case("fnd.credit.missing_interest_leaves_coverage_and_stress_flag_null", ["BR-FND-006", "BR-FND-007"],
                 {"debt": [500 * M], "cash": [100 * M]}, {"op": [40 * M] * 4, "da": [10 * M] * 4},
                 ("netDebtToEbitda", "interestCoverageEbit", "interestCoverageEbitda", "creditStressFlag", "debtTier", "dataQuality"),
                 expect={"netDebtToEbitda": 2.0, "interestCoverageEbit": None, "interestCoverageEbitda": None,
                         "creditStressFlag": None, "debtTier": "MODERATE", "dataQuality": "PARTIAL"}),
    _credit_case("fnd.credit.provider_ebitda_is_the_fallback_when_da_is_missing", ["BR-FND-004", "BR-FND-006"],
                 {"debt": [500 * M], "cash": [100 * M]}, {"op": [40 * M] * 4, "ebitda": [50 * M] * 4},
                 ("operationalEbitdaUsd", "operationalEbitdaSource", "netDebtToEbitda"),
                 expect={"operationalEbitdaUsd": 200 * M, "operationalEbitdaSource": "provider_ebitda_fallback", "netDebtToEbitda": 2.0}),
    _credit_case("fnd.credit.zero_operational_ebitda_gives_no_leverage", ["BR-FND-006"],
                 {"debt": [500 * M], "cash": [100 * M]}, {"op": [10 * M] * 4, "da": [-10 * M] * 4, "int": [1 * M] * 4},
                 ("operationalEbitdaUsd", "netDebtToEbitda", "debtTier", "creditStressFlag"),
                 expect={"operationalEbitdaUsd": 0, "netDebtToEbitda": None, "debtTier": None, "creditStressFlag": None}),

    # Q3i: a loss-maker with net debt gets a negative leverage ratio and the CLEAN tier; only the warning flags it.
    _credit_case("fnd.credit.q3i_loss_maker_with_net_debt_is_tiered_clean", ["BR-FND-007"],
                 {"debt": [300 * M], "cash": [200 * M]}, {"op": [-10 * M] * 4, "da": [5 * M] * 4, "int": [2 * M] * 4},
                 ("operationalEbitdaUsd", "netDebtToEbitda", "debtTier", "creditStressFlag", "warningCodes"),
                 mode="pinned", decision="Q3i",
                 expect={"operationalEbitdaUsd": -20 * M, "netDebtToEbitda": -5.0, "debtTier": "CLEAN", "creditStressFlag": False,
                         "warningCodes": ["NEGATIVE_EARNINGS_BASE"]}),
    # Q3i: three income quarters are summed as if they were a year. Python marks dataQuality PARTIAL; the Worker
    # reports OK (it flags only missing components) -- the figures themselves are identical.
    _credit_case("fnd.credit.q3i_three_quarter_ttm_python_partial_worker_ok", ["BR-FND-004", "BR-FND-006"],
                 {"debt": [500 * M], "cash": [100 * M]},
                 {"op": [40 * M] * 3, "da": [10 * M] * 3, "int": [8 * M] * 3, "ebitda": [50 * M] * 3},
                 ("operationalEbitdaUsd", "netDebtToEbitda", "interestCoverageEbit", "dataQuality"),
                 mode="pinned", decision="Q3i",
                 expect_py={"operationalEbitdaUsd": 150 * M, "netDebtToEbitda": 2.67, "interestCoverageEbit": 5.0, "dataQuality": "PARTIAL"},
                 expect_ts={"operationalEbitdaUsd": 150 * M, "netDebtToEbitda": 2.67, "interestCoverageEbit": 5.0, "dataQuality": "OK"}),
    # Python falls back to "Long Term Debt" (which omits current debt) when no total debt row exists; the Worker
    # reads total debt only, so net debt is unavailable.
    _credit_case("fnd.credit.q3i_long_term_debt_fallback_exists_only_in_python", ["BR-FND-004", "BR-FND-006"],
                 {"long_term_debt": [300 * M], "cash": [100 * M]}, {"op": [40 * M] * 4, "da": [10 * M] * 4, "int": [8 * M] * 4},
                 ("totalDebtUsd", "netDebtUsd", "netDebtToEbitda"),
                 mode="pinned", decision="Q3i",
                 expect_py={"totalDebtUsd": 300 * M, "netDebtUsd": 200 * M, "netDebtToEbitda": 1.0},
                 expect_ts={"totalDebtUsd": None, "netDebtUsd": None, "netDebtToEbitda": None}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-SHR-010 / BR-SHR-012: earnings beat rate, surprise, beat streak, pre-revenue rule (get_earnings_momentum)
# Python: server.get_earnings_momentum on stubbed yfinance tables. Worker: getEarningsMomentum on a canned
# quoteSummary earningsHistory.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _momentum_case(name: str, ids: list[str], history: list | None, keys: tuple, **kw) -> Case:
    """`history`: (epsActual, epsEstimate, surprisePercent) rows in provider order (None = missing)."""
    def py():
        frame = None
        if history:
            frame = pd.DataFrame(
                {"epsActual": [r[0] for r in history], "epsEstimate": [r[1] for r in history],
                 "surprisePercent": [r[2] for r in history]},
                index=pd.DatetimeIndex([f"2025-{3 * (i + 1):02d}-28" for i in range(len(history))], name="quarter"), dtype=object)
        company = SimpleNamespace(fast_info=SimpleNamespace(currency="USD"), eps_trend=None, earnings_history=frame)
        with patch.object(server.yf, "Ticker", lambda _t: company):
            return asyncio.run(server.get_earnings_momentum("TST"))

    def cell(v):
        return None if v is None else {"raw": v, "fmt": str(v)}

    hist_json = [{"epsActual": cell(a), "epsEstimate": cell(e), "surprisePercent": cell(s)} for a, e, s in (history or [])]
    summary = {"quoteSummary": {"result": [{"earningsTrend": {"trend": []},
                                            "earningsHistory": {"history": hist_json} if hist_json else {}}], "error": None}}
    return Case(
        name=name, ids=ids, py=py,
        ts={"js": _ts_with_clock(CREDIT_NOW, 'return JSON.parse(await m.yahooFinance.getEarningsMomentum("TST"));'), "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, "https://query1.finance.yahoo.com/v10/finance/quoteSummary/TST?*": {"json": summary}},
        select=lambda v: {**{k: v.get(k) for k in keys}, **({"warningCodes": sorted(w["code"] for w in v["warnings"])} if "warningCodes" in keys else {})},
        **kw)


BEAT, MISS = (1.05, 1.00, 0.05), (0.95, 1.00, -0.05)
MOMENTUM_KEYS = ("beatRate", "beatCount", "totalQuarters", "avgSurprisePct", "currentBeatStreak", "preRevenue", "momentumFlag")

CASES += [
    # Card BR-SHR-012: 4 quarters, 3 beats, surprise ratios 0.05, 0.02, -0.01, 0.04 -> beatRate 0.75, avgSurprisePct 2.50.
    _momentum_case("shr.momentum.card_beat_rate_and_average_surprise", ["BR-SHR-012"],
                   [(1.05, 1.00, 0.05), (1.02, 1.00, 0.02), (0.99, 1.00, -0.01), (1.04, 1.00, 0.04)],
                   ("beatRate", "beatCount", "totalQuarters", "avgSurprisePct", "preRevenue"),
                   expect={"beatRate": 0.75, "beatCount": 3, "totalQuarters": 4, "avgSurprisePct": 2.5, "preRevenue": False}),
    _momentum_case("shr.momentum.equal_actual_and_estimate_is_not_a_beat", ["BR-SHR-012"],
                   [(1.00, 1.00, 0.0), (1.00, 1.00, 0.0)], ("beatRate", "beatCount", "currentBeatStreak"),
                   expect={"beatRate": 0, "beatCount": 0, "currentBeatStreak": 0}),
    _momentum_case("shr.momentum.rows_missing_actual_or_estimate_are_skipped", ["BR-SHR-010", "BR-SHR-012"],
                   [BEAT, (None, 1.00, None), BEAT, MISS], ("beatRate", "totalQuarters", "currentBeatStreak"),
                   expect={"beatRate": 0.67, "totalQuarters": 3, "currentBeatStreak": 2}),
    # Q3i (BR-SHR-010): the streak counts from the FIRST provider row. Rows beat, beat, miss, beat -> 2, although
    # the most recent quarter (the last row, if the provider lists oldest first) gives 1.
    _momentum_case("shr.momentum.q3i_beat_streak_counts_from_the_first_provider_row", ["BR-SHR-010", "BR-SHR-012"],
                   [BEAT, BEAT, MISS, BEAT], ("currentBeatStreak", "beatRate"),
                   mode="pinned", decision="Q3i", expect={"currentBeatStreak": 2, "beatRate": 0.75}),
    _momentum_case("shr.momentum.q3i_beat_streak_reversed_rows_give_a_different_streak", ["BR-SHR-010"],
                   [BEAT, MISS, BEAT, BEAT], ("currentBeatStreak", "beatRate"),
                   mode="pinned", decision="Q3i", expect={"currentBeatStreak": 1, "beatRate": 0.75}),
    # Pre-revenue rule: no history, or every actual EPS effectively zero (< 1e-9).
    _momentum_case("shr.momentum.no_history_is_pre_revenue", ["BR-SHR-012"], None, MOMENTUM_KEYS + ("warningCodes", "dataQuality"),
                   expect={"beatRate": None, "beatCount": 0, "totalQuarters": 0, "avgSurprisePct": None, "currentBeatStreak": 0,
                           "preRevenue": True, "momentumFlag": "NO_HISTORY", "warningCodes": ["PRE_REVENUE_NO_HISTORY"], "dataQuality": "PARTIAL"}),
    _momentum_case("shr.momentum.all_zero_actuals_are_pre_revenue_and_hide_average_surprise", ["BR-SHR-012"],
                   [(0.0, 0.05, -1.0), (5e-10, 0.04, -0.99)], MOMENTUM_KEYS,
                   expect={"beatRate": 0, "beatCount": 0, "totalQuarters": 2, "avgSurprisePct": None, "currentBeatStreak": 0,
                           "preRevenue": True, "momentumFlag": "NO_HISTORY"}),
    _momentum_case("shr.momentum.actual_of_exactly_epsilon_is_not_pre_revenue", ["BR-SHR-012"],
                   [(1e-9, 0.05, -1.0)], ("preRevenue", "momentumFlag", "avgSurprisePct"),
                   expect={"preRevenue": False, "momentumFlag": None, "avgSurprisePct": -100}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-PRV-002 / BR-PRV-003: Finnhub market eligibility in get_expanded_institutional_ownership
# Neither runtime has a Finnhub key here, so an eligible ticker stops at SOURCE_UNCONFIGURED (attempted=false) without any HTTP call.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

import os  # noqa: E402
from pathlib import Path  # noqa: E402

from yfmcp.tools import provider_gaps  # noqa: E402


def _ownership_case(name: str, ids: list[str], ticker: str, *, allow_fallback: bool = False, max_holders: int = 50,
                    policy_missing: bool = False, **kw) -> Case:
    def py():
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {}, clear=False))  # restores any key removed below
            for var in ("FINNHUB_API_KEY", "FINNHUB_TOKEN", "ALPHA_VANTAGE_API_KEY"):
                os.environ.pop(var, None)
            if policy_missing:
                stack.enter_context(patch.object(provider_gaps, "_SOURCE_POLICY_PATH", Path("/nonexistent/news-source-capabilities.json")))
            return asyncio.run(provider_gaps.get_expanded_institutional_ownership(ticker, allow_fallback, max_holders))

    js = f'return JSON.parse(await m.yahooFinance.getExpandedInstitutionalOwnership({json.dumps(ticker)}, {json.dumps(allow_fallback)}, {max_holders}));'

    def view(v):
        data = v.get("data") if isinstance(v.get("data"), dict) else v
        err = v.get("error")
        if isinstance(err, dict):
            return {"error": True, "code": err.get("code"), "message": err.get("message")}
        return {"status": data.get("status"), "action": data.get("recommendedNextAction"),
                "attempts": [(a.get("provider"), a.get("status"), a.get("attempted"), a.get("reasonCode")) for a in data.get("providerAttempts", [])]}

    return Case(name=name, ids=ids, py=py, ts={"js": js, "isolate": True}, select=view, **kw)


NOT_ELIGIBLE = {"status": "NOT_ELIGIBLE", "action": "USE_YAHOO_OR_OFFICIAL_FILINGS",
                "attempts": [["finnhub", "NOT_ELIGIBLE", False, "MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT"]]}
CASES += [
    # Card BR-PRV-002: an ineligible suffix (.L) is not sent to Finnhub, no Alpha call, NOT_ELIGIBLE.
    _ownership_case("prv.eligibility.ineligible_suffix_is_not_attempted", ["BR-PRV-002", "BR-PRV-003"], "VOD.L", expect=NOT_ELIGIBLE),
    _ownership_case("prv.eligibility.suffix_match_is_case_insensitive", ["BR-PRV-003"], "vod.l", expect=NOT_ELIGIBLE),
    _ownership_case("prv.eligibility.listed_ineligible_ticker", ["BR-PRV-003"], "FOCI", expect=NOT_ELIGIBLE),
    _ownership_case("prv.eligibility.ineligible_ticker_stays_ineligible_with_scarce_fallback_off_and_no_alpha_call", ["BR-PRV-002"], "ABB.ST",
                    allow_fallback=False, expect=NOT_ELIGIBLE),
    _ownership_case("prv.eligibility.max_holders_above_100_is_rejected", ["BR-PRV-002"], "AAPL", max_holders=101,
                    expect={"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "max_holders must be an integer from 1 through 100."}),
    _ownership_case("prv.eligibility.eligible_ticker_is_attempted", ["BR-PRV-002", "BR-PRV-003"], "AAPL",
                    expect={"status": "SOURCE_UNCONFIGURED", "action": "CONFIGURE_PROVIDER",
                            "attempts": [["finnhub", "SOURCE_UNCONFIGURED", False, None]]}),
    # Q3i: Python reads the policy file at call time and fails open on any read error, so every ticker, including
    # the ineligible VOD.L, is sent to Finnhub. The Worker's policy is compiled in and cannot be unreadable.
    _ownership_case("prv.eligibility.q3i_unreadable_policy_fails_open_in_python", ["BR-PRV-002", "BR-PRV-003"], "VOD.L",
                    policy_missing=True, mode="pinned", decision="Q3i",
                    expect_py={"status": "SOURCE_UNCONFIGURED", "action": "CONFIGURE_PROVIDER",
                               "attempts": [["finnhub", "SOURCE_UNCONFIGURED", False, None]]},
                    expect_ts=NOT_ELIGIBLE),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-SHR-016: Form 4 insider transaction value (get_sec_recent_events for Form 4)
# A Form 4 filing in the SEC submissions feed has its primary document parsed into item.insiderTransaction.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

FORM4_TEMPLATE = """<?xml version="1.0"?><ownershipDocument><reportingOwner><reportingOwnerId><rptOwnerName>Doe Jane</rptOwnerName></reportingOwnerId>
<reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>Chief Financial Officer</officerTitle></reportingOwnerRelationship></reportingOwner>
<nonDerivativeTable>{rows}</nonDerivativeTable></ownershipDocument>"""


def _f4_row(code: str, shares: str, price: str | None, date: str = "2026-10-01") -> str:
    price_xml = f"<transactionPricePerShare><value>{price}</value></transactionPricePerShare>" if price is not None else ""
    return (f"<nonDerivativeTransaction><transactionDate><value>{date}</value></transactionDate>"
            f"<transactionCoding><transactionCode>{code}</transactionCode></transactionCoding>"
            f"<transactionAmounts><transactionShares><value>{shares}</value></transactionShares>{price_xml}</transactionAmounts>"
            f"<ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature></nonDerivativeTransaction>")


def _form4_case(name: str, ids: list[str], ticker: str, cik: int, rows: str, expect=None, **kw) -> Case:
    accession = f"0001{cik:06d}-26-000001"
    nodash = accession.replace("-", "")
    doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{nodash}/form4.xml"
    routes = {
        "https://www.sec.gov/files/company_tickers.json": {"json": {"0": {"cik_str": cik, "ticker": ticker, "title": "Form Four Co"}}},
        f"https://data.sec.gov/submissions/CIK{cik:010d}.json": {"json": {"name": "Form Four Co", "filings": {"recent": {
            "form": ["4"], "filingDate": ["2026-10-01"], "acceptanceDateTime": ["2026-10-01T16:00:00.000Z"],
            "accessionNumber": [accession], "primaryDocument": ["form4.xml"]}}}},
        doc_url: {"body": FORM4_TEMPLATE.format(rows=rows)},
        "https://query1.finance.yahoo.com/*": {"status": 404, "body": "{}"},
    }

    def py():
        saved = (edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT)
        edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = None, 0.0
        try:
            with patch.object(edgar.yf, "Ticker", side_effect=RuntimeError("yahoo unavailable")):
                return asyncio.run(server.get_sec_recent_events(ticker, ["4"], 36500, 10))
        finally:
            edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = saved

    def tx(v):
        for item in v.get("items") or []:
            if item.get("insiderTransaction"):
                t = item["insiderTransaction"]
                return {k: t.get(k) for k in ("owner", "role", "transactionCode", "transactionLabel", "shares", "price", "value", "transactionDate")}
        return {"items": len(v.get("items") or []), "warnings": [w.get("code") for w in v.get("warnings") or []]}

    return Case(
        name=name, ids=ids, py=py,
        ts={"js": _ts_with_clock(CREDIT_NOW, f'return JSON.parse(await m.yahooFinance.getSecRecentEvents({json.dumps(ticker)}, ["4"], 36500, 10));'),
            "isolate": True},
        routes=routes, select=tx, **({"expect": expect} if expect is not None else {}), **kw)


SALE_ROW = _f4_row("S", "10000", "25.50")
CASES += [
    # Card: non-derivative row, code S, 10,000 shares at $25.50 -> Sale, value 255,000.00.
    _form4_case("shr.form4.sale_value_is_shares_times_price", ["BR-SHR-016"], "FRA", 910001, SALE_ROW,
                {"owner": "Doe Jane", "role": "Chief Financial Officer", "transactionCode": "S", "transactionLabel": "Sale",
                 "shares": 10000, "price": 25.5, "value": 255000, "transactionDate": "2026-10-01"}),
    _form4_case("shr.form4.no_price_gives_no_value", ["BR-SHR-016"], "FRB", 910002, _f4_row("A", "500", None),
                {"owner": "Doe Jane", "role": "Chief Financial Officer", "transactionCode": "A", "transactionLabel": "Grant/Award",
                 "shares": 500, "price": None, "value": None, "transactionDate": "2026-10-01"}),
    _form4_case("shr.form4.unknown_code_is_other_unclassified", ["BR-SHR-016"], "FRC", 910003, _f4_row("X", "100", "10.00"),
                {"owner": "Doe Jane", "role": "Chief Financial Officer", "transactionCode": "X", "transactionLabel": "Other/Unclassified",
                 "shares": 100, "price": 10, "value": 1000, "transactionDate": "2026-10-01"}),
    # Q3i: only the first transaction row is read. An option exercise (M, 10,000 at $5.00) followed by the sale of
    # those shares (S, 10,000 at $25.50) reports the exercise alone.
    _form4_case("shr.form4.q3i_multi_line_filing_reports_only_the_first_transaction", ["BR-SHR-016"], "FRD", 910004,
                _f4_row("M", "10000", "5.00") + _f4_row("S", "10000", "25.50"),
                mode="pinned", decision="Q3i",
                expect={"owner": "Doe Jane", "role": "Chief Financial Officer", "transactionCode": "M",
                        "transactionLabel": "Option exercise/conversion", "shares": 10000, "price": 5, "value": 50000,
                        "transactionDate": "2026-10-01"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# BR-EVD-005 (card example): the day's consensus observation is written once; a second call the same day reports
# ALREADY_STORED and leaves the stored file unchanged. Calls are six hours apart, so an overwrite would change observedAt.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

OBS_T1, OBS_T2 = "2026-10-14T09:00:00.000Z", "2026-10-14T15:00:00.000Z"
OBS_TREND = [{"period": "0y", "endDate": "2026-12-31",
              "earningsEstimate": {"avg": 5.2, "high": 5.6, "low": 4.8, "numberOfAnalysts": 10},
              "revenueEstimate": {"avg": 1e9, "high": 1.1e9, "low": 9e8, "numberOfAnalysts": 10}}]


def _observation_py():
    store = es.MemoryStore()
    times = iter([OBS_T1, OBS_T2])
    yahoo = ev.yahoo_consensus_input(OBS_TREND, retrieved_at=OBS_T1, financial_currency="USD")
    providers = AsyncMock(return_value=([yahoo], {"price": None}, {"status": "NO_DATA", "splits": []}))
    with patch.object(server, "_consensus_providers", providers), \
            patch.object(server, "_consensus_naming", AsyncMock(return_value=server.fiscal_year_naming(None))), \
            patch.object(server, "_now_iso", lambda: next(times)), _python_store(store):
        first = json.loads(asyncio.run(server.get_consensus_forecast_curve("TST")))["storage"]["consensusObservation"]
        key = first["key"]
        body = store.objects[key]
        second = json.loads(asyncio.run(server.get_consensus_forecast_curve("TST")))["storage"]["consensusObservation"]
        return {"first": first, "second": second, "unchanged": store.objects[key] == body,
                "observedAt": json.loads(store.objects[key])["observedAt"]}


OBSERVATION_JS = """
let cur = Date.UTC(2026, 9, 14, 9, 0, 0);
const RealDate = Date;
globalThis.Date = class extends RealDate {
  constructor(...a) { if (a.length === 0) super(cur); else super(...a); }
  static now() { return cur; }
};
try {
  const store = m.evidenceStore.memoryEvidenceStore();
  m.evidenceStore.setEvidenceStoreForTests(store);
  const one = JSON.parse(await m.evidencePack.getConsensusForecastCurve("TST")).storage.consensusObservation;
  const body = store.objects.get(one.key);
  cur += 6 * 3600 * 1000;
  const two = JSON.parse(await m.evidencePack.getConsensusForecastCurve("TST")).storage.consensusObservation;
  return { first: one, second: two, unchanged: store.objects.get(one.key) === body, observedAt: JSON.parse(store.objects.get(one.key)).observedAt };
} finally { globalThis.Date = RealDate; }
"""

CASES += [
    Case(name="evd.store.consensus_observation_first_call_wins_the_day", ids=["BR-EVD-005", "BR-EVD-027"],
         py=_observation_py, ts={"js": OBSERVATION_JS, "isolate": True},
         routes={**YAHOO_AUTH_ROUTES, "https://query1.finance.yahoo.com/v10/finance/quoteSummary/TST?*": {"json": {"quoteSummary": {"result": [{
             "price": {"currency": "USD"}, "financialData": {"financialCurrency": "USD"},
             "earningsTrend": {"trend": OBS_TREND}}], "error": None}}}},
         expect={"first": {"status": "STORED", "key": "consensus-history/TST/2026-10-14.json"},
                 "second": {"status": "ALREADY_STORED", "key": "consensus-history/TST/2026-10-14.json"},
                 "unchanged": True, "observedAt": OBS_T1}),
]


# BR-EVD-005: build_valuation_evidence_pack storage status per mode (persist true / false / no store). Every
# SEC and provider component fails visibly in both runtimes; only the evidenceCut storage fields are compared.
_HEX64 = re.compile(r"[0-9a-f]{64}")
_PACK_FAIL = json.dumps({"ticker": "TST", "status": "FILING_TEXT_NOT_AVAILABLE", "code": "FILING_TEXT_NOT_AVAILABLE", "message": "stub"})


def _pack_py(persist: bool, with_store: bool):
    store = es.MemoryStore() if with_store else None
    failing = AsyncMock(return_value=_PACK_FAIL)
    providers = AsyncMock(return_value=([ev.yahoo_consensus_input([], retrieved_at=OBS_T1, status="NO_DATA")],
                                        {"price": None, "currency": None, "priceTime": None, "status": "NO_DATA", "message": "stub"},
                                        {"status": "NO_DATA", "splits": []}))
    with patch.object(server, "_consensus_providers", providers), \
            patch.object(server, "_consensus_naming", AsyncMock(return_value=server.fiscal_year_naming(None))), \
            patch.object(server, "_sec_filing_rows", AsyncMock(return_value=(None, "TICKER_NOT_FOUND"))), \
            patch.object(server, "_warm_sec_submissions", AsyncMock(return_value=None)), \
            patch.object(server, "extract_capital_structure", failing), patch.object(server, "extract_guidance", failing), \
            patch.object(server, "list_sec_material_filings", failing), \
            patch.object(server, "_now_iso", lambda: OBS_T1), _python_store(store):
        pack = json.loads(asyncio.run(server.build_valuation_evidence_pack("TST", 5, persist)))
        return {"cut": pack["evidenceCut"], "keys": sorted(store.objects) if store else None}


def _pack_case(name: str, ids: list[str], persist: bool, with_store: bool, expect) -> Case:
    js = f"""
const RealDate = Date;
const fixed = Date.UTC(2026, 9, 14, 9, 0, 0);
globalThis.Date = class extends RealDate {{
  constructor(...a) {{ if (a.length === 0) super(fixed); else super(...a); }}
  static now() {{ return fixed; }}
}};
try {{
  const store = {'m.evidenceStore.memoryEvidenceStore()' if with_store else 'null'};
  m.evidenceStore.setEvidenceStoreForTests(store);
  const pack = JSON.parse(await m.evidencePack.buildValuationEvidencePack("TST", 5, {json.dumps(persist)}));
  return {{ cut: pack.evidenceCut, keys: store ? [...store.objects.keys()].sort() : null }};
}} finally {{ globalThis.Date = RealDate; }}
"""

    def view(v):
        cut = v["cut"]
        mask = lambda s: None if s is None else _HEX64.sub("<sha256>", s)  # noqa: E731 - the two runtimes hash different receipts
        return {"storageStatus": cut["storageStatus"], "storageKey": mask(cut["storageKey"]),
                "consensusObservation": cut["consensusObservation"],
                "keys": None if v["keys"] is None else [mask(k) for k in v["keys"]]}

    return Case(name=name, ids=ids, py=lambda: _pack_py(persist, with_store), ts={"js": js, "isolate": True},
                routes=YAHOO_AUTH_ROUTES, select=view, expect=expect)


CUT_KEY_MASKED = "evidence-cuts/TST/20261014T090000Z/<sha256>.json"
CASES += [
    _pack_case("evd.store.pack_persisted_stores_cut_and_observation", ["BR-EVD-005"], True, True,
               {"storageStatus": "STORED", "storageKey": CUT_KEY_MASKED,
                "consensusObservation": {"status": "STORED", "key": "consensus-history/TST/2026-10-14.json"},
                "keys": ["consensus-history/TST/2026-10-14.json", CUT_KEY_MASKED]}),
    _pack_case("evd.store.pack_persist_false_is_skipped_and_writes_nothing", ["BR-EVD-005"], False, True,
               {"storageStatus": "SKIPPED", "storageKey": None, "consensusObservation": {"status": "SKIPPED", "key": None}, "keys": []}),
    _pack_case("evd.store.pack_without_store_is_unavailable_but_returns_the_cut", ["BR-EVD-005"], True, False,
               {"storageStatus": "UNAVAILABLE", "storageKey": None, "consensusObservation": {"status": "UNAVAILABLE", "key": None}, "keys": None}),
]


# Q3h (c): sources omitted -> the tool's default set, reported as defaultSourcesApplied with requestedSources null.
# The stubbed Yahoo path differs between the runtimes below the source level (see the helper case that follows
# for attemptedSources), so only the request fields are compared here.
def _q3h_omitted_view(payload):
    return {
        "requestedSources": payload["requestedSources"],
        "defaultSourcesApplied": payload["defaultSourcesApplied"],
        "attemptedSourcesAreSourceStatusKeys": all(a["sourceType"] in payload["sourceStatus"] for a in payload["attemptedSources"]),
    }


_Q3H_STATUS_FIXTURE = {
    "yahoo_finance_news": {"status": "OK"},
    "yahoo_finance_press_releases": {"status": "EMPTY_RESULT", "attempted": True},
    "finnhub": {"status": "UNCONFIGURED"},
    "sec": {"status": "PROVIDER_ERROR"},
    "company_ir_page": {"status": "NOT_NEEDED"},
    "newswire": {"status": "NOT_ELIGIBLE"},
    "marketaux": {"status": "EMPTY_RESULT", "attempted": False},
    "company_ir": {"status": "WEBSITE_NOT_AVAILABLE"},
}


def _python_news_sources_omitted():
    with patch.object(news_tools, "_collect_company_events", AsyncMock(return_value=([], [], [], "2026-10-15T00:00:00Z", {}))):
        return json.loads(asyncio.run(server.get_company_news("TEST", 5, 14)))


CASES += [
    Case(name="tool.q3h.get_company_news_omitted_sources_applies_defaults", ids=["Q3h", "BR-NWS-007"],
         py=_python_news_sources_omitted,
         ts={"js": 'm.response.setWorkerEnv({}); return JSON.parse(await m.yahooFinance.getCompanyNews("TEST", 5, 14));',
             "isolate": True},
         routes=NEWS_ROUTES, select=_q3h_omitted_view,
         expect={"requestedSources": None, "defaultSourcesApplied": True, "attemptedSourcesAreSourceStatusKeys": True}),
    # attemptedSources leaves out sources that were not tried: attempted false, NOT_ELIGIBLE, NOT_NEEDED, UNCONFIGURED.
    Case(name="tool.q3h.attempted_sources_exclude_not_eligible_not_needed_unconfigured", ids=["Q3h", "BR-NWS-007"],
         py=lambda: server._source_request_fields(None, _Q3H_STATUS_FIXTURE),
         ts={"js": "return m.yahooFinance.sourceRequestFields(null, args[0]);", "args": [_Q3H_STATUS_FIXTURE]},
         expect={"requestedSources": None, "defaultSourcesApplied": True,
                 "attemptedSources": [{"sourceType": "yahoo_finance_news", "status": "OK"},
                                      {"sourceType": "yahoo_finance_press_releases", "status": "EMPTY_RESULT"},
                                      {"sourceType": "sec", "status": "PROVIDER_ERROR"},
                                      {"sourceType": "company_ir", "status": "WEBSITE_NOT_AVAILABLE"}]}),
    Case(name="tool.q3h.requested_sources_are_reported_as_strings", ids=["Q3h"],
         py=lambda: server._source_request_fields(["finnhub", 7], {}),
         ts={"js": "return m.yahooFinance.sourceRequestFields(args[0], {});", "args": [["finnhub", "7"]]},
         expect={"requestedSources": ["finnhub", "7"], "defaultSourcesApplied": False, "attemptedSources": []}),
    Case(name="tool.q3h.get_company_news_batch_empty_sources_attempts_none", ids=["Q3h"],
         py=lambda: json.loads(asyncio.run(server.get_company_news(["AAPL", "MSFT"], 5, 14, []))),
         ts={"js": 'm.response.setWorkerEnv({}); return JSON.parse(await m.yahooFinance.getCompanyNews(["AAPL", "MSFT"], 5, 14, []));',
             "isolate": True},
         select=lambda batch: {t: _q3h_view(batch[t]) for t in ("AAPL", "MSFT")},
         expect={t: _q3h_none_selected([], []) for t in ("AAPL", "MSFT")}),
]
