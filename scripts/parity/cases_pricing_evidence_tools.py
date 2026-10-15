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
import zoneinfo
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

from scripts.parity.harness import Case

# Registers the tools (server.py imports the yfmcp modules) the way the existing suites do.
import server  # noqa: F401,E402
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
GATE_FIELDS = ("status", "gatePass", "adv20dTradedValueUsd", "notionalUsd", "fxRate", "fxCurrency", "gateBasis",
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


def _gate_case(name: str, ids: list[str], *, symbol: str, currency: str, close: float, volume: int,
               fx: tuple[str, float | None] | None, expect: dict, **kw) -> Case:
    """One get_volume_gate scenario: `volume` shares a day at `close` (listing units) for 30 sessions."""
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
            return asyncio.run(pricing.get_volume_gate(symbol, False))

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
        ts={"js": _ts_with_clock(GATE_NOW, f'return JSON.parse(await m.yahooFinance.getVolumeGate("{symbol}", false));'),
            "isolate": True},
        routes=routes, select=lambda v: {k: v.get(k) for k in GATE_FIELDS}, expect=expect, **kw)


CASES += [
    # Card: LSE listing in GBp, close 250 GBp, 2,000,000 shares a day, GBP=X 0.79 -> 79 GBp per USD,
    # 500,000,000 GBp / 79 = $6.33M, gate FAIL.
    _gate_case("prc.currency.gbp_pence_listing_card_example", ["BR-PRC-003"], symbol="CARD.L", currency="GBp",
               close=250.0, volume=2_000_000, fx=("GBP=X", 0.79),
               expect={"status": "OK", "gatePass": False, "adv20dTradedValueUsd": 6329113.92,
                       "notionalUsd": 6329113.92, "fxRate": 0.79, "fxCurrency": "GBP",
                       "gateBasis": "ADV20_TRADED_VALUE_USD", "recommendedNextAction": "NONE",
                       "note": "Volume gate FAIL — 20d average traded value $6.3M (< $10M); latest session 1.00x 20d ADV"
                               " [GBp→USD at 0.7900 GBP/USD, 100 GBp per GBP]"}),
    _gate_case("prc.currency.gbx_alias_behaves_like_gbp_pence", ["BR-PRC-003"], symbol="CARX.L", currency="GBX",
               close=250.0, volume=2_000_000, fx=("GBP=X", 0.79),
               expect={"status": "OK", "gatePass": False, "adv20dTradedValueUsd": 6329113.92,
                       "notionalUsd": 6329113.92, "fxRate": 0.79, "fxCurrency": "GBP",
                       "gateBasis": "ADV20_TRADED_VALUE_USD", "recommendedNextAction": "NONE",
                       "note": "Volume gate FAIL — 20d average traded value $6.3M (< $10M); latest session 1.00x 20d ADV"
                               " [GBX→USD at 0.7900 GBP/USD, 100 GBX per GBP]"}),
    _gate_case("prc.currency.south_african_cents_zac", ["BR-PRC-003"], symbol="ZAC.JO", currency="ZAc",
               close=5000.0, volume=1_000_000, fx=("ZAR=X", 18.0),
               # 5,000 ZAc x 1,000,000 = 5e9 ZAc; 18 ZAR/USD x 100 = 1,800 ZAc per USD -> $2,777,777.78.
               expect={"status": "OK", "gatePass": False, "adv20dTradedValueUsd": 2777777.78,
                       "notionalUsd": 2777777.78, "fxRate": 18, "fxCurrency": "ZAR",
                       "gateBasis": "ADV20_TRADED_VALUE_USD", "recommendedNextAction": "NONE",
                       "note": "Volume gate FAIL — 20d average traded value $2.8M (< $10M); latest session 1.00x 20d ADV"
                               " [ZAc→USD at 18.0000 ZAR/USD, 100 ZAc per ZAR]"}),
    _gate_case("prc.currency.israeli_agorot_ila", ["BR-PRC-003"], symbol="ILA.TA", currency="ILA",
               close=10000.0, volume=1_000_000, fx=("ILS=X", 4.0),
               # 1e10 ILA / (4 x 100) = $25,000,000 -> PASS.
               expect={"status": "OK", "gatePass": True, "adv20dTradedValueUsd": 25000000,
                       "notionalUsd": 25000000, "fxRate": 4, "fxCurrency": "ILS",
                       "gateBasis": "ADV20_TRADED_VALUE_USD", "recommendedNextAction": "NONE",
                       "note": "Volume gate PASS — 20d average traded value $25.0M (≥ $10M); latest session 1.00x 20d ADV"
                               " [ILA→USD at 4.0000 ILS/USD, 100 ILA per ILS]"}),
    _gate_case("prc.currency.major_unit_currency_uses_rate_without_scaling", ["BR-PRC-003"], symbol="EURX.PA",
               currency="EUR", close=100.0, volume=1_000_000, fx=("EUR=X", 0.5),
               # 1e8 EUR / 0.5 EUR per USD = $200,000,000.
               expect={"status": "OK", "gatePass": True, "adv20dTradedValueUsd": 200000000,
                       "notionalUsd": 200000000, "fxRate": 0.5, "fxCurrency": "EUR",
                       "gateBasis": "ADV20_TRADED_VALUE_USD", "recommendedNextAction": "NONE",
                       "note": "Volume gate PASS — 20d average traded value $200.0M (≥ $10M); latest session 1.00x 20d ADV"
                               " [EUR→USD at 0.5000 EUR/USD]"}),
    _gate_case("prc.currency.usd_listing_needs_no_rate", ["BR-PRC-003"], symbol="USDX", currency="USD",
               close=5.0, volume=100_000, fx=None,
               expect={"status": "OK", "gatePass": False, "adv20dTradedValueUsd": 500000, "notionalUsd": 500000,
                       "fxRate": 1, "fxCurrency": "USD", "gateBasis": "ADV20_TRADED_VALUE_USD",
                       "recommendedNextAction": "NONE",
                       "note": "Volume gate FAIL — 20d average traded value $0.5M (< $10M); latest session 1.00x 20d ADV"}),
    _gate_case("prc.currency.missing_rate_makes_the_gate_unknown", ["BR-PRC-003"], symbol="NORATE.L", currency="GBp",
               close=250.0, volume=2_000_000, fx=("GBP=X", None),
               expect={"status": "OK", "gatePass": None, "adv20dTradedValueUsd": None, "notionalUsd": None,
                       "fxRate": None, "fxCurrency": None, "gateBasis": "ADV20_TRADED_VALUE_USD",
                       "recommendedNextAction": "RETRY", "note": "Volume gate UNKNOWN — GBP=X rate unavailable"}),
    _gate_case("prc.currency.zero_rate_makes_the_gate_unknown", ["BR-PRC-003"], symbol="ZERORATE.L", currency="GBp",
               close=250.0, volume=2_000_000, fx=("GBP=X", 0.0),
               expect={"status": "OK", "gatePass": None, "adv20dTradedValueUsd": None, "notionalUsd": None,
                       "fxRate": None, "fxCurrency": None, "gateBasis": "ADV20_TRADED_VALUE_USD",
                       "recommendedNextAction": "RETRY", "note": "Volume gate UNKNOWN — GBP=X rate unavailable"}),
    _gate_case("prc.currency.negative_rate_makes_the_gate_unknown", ["BR-PRC-003"], symbol="NEGRATE.L", currency="GBp",
               close=250.0, volume=2_000_000, fx=("GBP=X", -0.79),
               expect={"status": "OK", "gatePass": None, "adv20dTradedValueUsd": None, "notionalUsd": None,
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
    _grade_case("shr.probe.whitespace", ["BR-SHR-001"], GOOD_PAYLOAD, {**GOOD_EVIDENCE, "concept": "   "}, "FOUND", False),
    _grade_case("shr.probe.ctxlist", ["BR-SHR-001"], {**GOOD_PAYLOAD, "xbrlContext": []}, GOOD_EVIDENCE, "FOUND", False),
    _grade_case("shr.probe.ctxempty", ["BR-SHR-001"], {**GOOD_PAYLOAD, "xbrlContext": {}}, GOOD_EVIDENCE, "FOUND", False),
    _grade_case("shr.probe.evidence_num", ["BR-SHR-001"], GOOD_PAYLOAD, {**GOOD_EVIDENCE, "periodEnd": 0}, "FOUND", False),
]
