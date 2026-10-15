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
# index; the Worker validates neither (it validates tickers only in get_historical_prices) and ignores the
# accession (Q3a). Both are scheduled to change; today's behaviour is pinned.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from yfmcp.validation import _validate_ticker  # noqa: E402

OUTLINE_HTML = "<html><body><h2>Item 1. Business</h2><h2>Item 1A. Risk Factors</h2></body></html>"
AAPL_DOC = "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm"
AAPL_INDEX = "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/0000320193-24-000123-index.htm"
OUTLINE_ROUTES = {
    "https://www.sec.gov/files/company_tickers.json": {"json": {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}},
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


def _outline_case(name: str, ids: list[str], ticker: str, accession, document_url, **kw) -> Case:
    return Case(
        name=name, ids=ids,
        py=lambda: asyncio.run(server.get_filing_outline(ticker, accession, document_url)),
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
    # Q3a: Python resolves the accession to the filing index; the Worker ignores accession_number.
    _outline_case("tool.q3a.outline_accession_only_python_resolves_worker_ignores", ["Q3a"], "AAPL", VALID_ACCESSION, None,
                  mode="pinned", decision="Q3a", expect_py=OUTLINE_OK,
                  expect_ts={"error": True, "code": None, "message": "document_url is required"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Q4: SMOKE_TICKER_CIK_FALLBACKS resolves a listed ticker's CIK when the Yahoo and SEC lookups fail.
# Python: edgar._resolve_cik_for_ticker with yfinance failing. Worker: getSubmissionsForTicker (cikPadded).
# The repair removes the table from production lookup; today both runtimes still use it.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from yfmcp.clients import edgar  # noqa: E402


def _resolve_cik_py(ticker: str):
    saved = (edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT, dict(edgar._FILING_CIK_CACHE))
    edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = None, 0.0
    edgar._FILING_CIK_CACHE.clear()
    try:
        with patch.object(edgar.yf, "Ticker", side_effect=RuntimeError("yahoo unavailable")):
            return asyncio.run(edgar._resolve_cik_for_ticker(ticker))
    finally:
        edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = saved[0], saved[1]
        edgar._FILING_CIK_CACHE.clear()
        edgar._FILING_CIK_CACHE.update(saved[2])


def _cik_case(name: str, ids: list[str], ticker: str, expect, routes: dict | None = None, **kw) -> Case:
    return Case(
        name=name, ids=ids, py=lambda: _resolve_cik_py(ticker),
        ts={"js": "return (await m.yahooFinance.getSubmissionsForTicker(args[0])).cikPadded", "args": [ticker], "isolate": True},
        routes=routes or {}, expect=expect, **kw)


CASES += [
    _cik_case("tool.q4.smoke_table_resolves_listed_ticker_when_yahoo_and_sec_fail", ["Q4"], "AAPL", "0000320193",
              mode="pinned", decision="Q4"),
    _cik_case("tool.q4.smoke_table_covers_aaoi", ["Q4"], "AAOI", "0001158114", mode="pinned", decision="Q4"),
    _cik_case("tool.q4.unlisted_ticker_with_all_lookups_failing_is_unresolved", ["Q4"], "ZZZZ", None),
    _cik_case("tool.q4.sec_ticker_index_wins_over_the_smoke_table", ["Q4"], "AAPL", "0000999999",
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


def _python_news_sources_empty():
    """get_company_news(sources=[]) with the collector stubbed to find nothing; the sources it was asked for are recorded."""
    collector = AsyncMock(return_value=([], [], [], "2026-10-15T00:00:00Z", {}))
    with patch.object(server, "_collect_company_events", collector):
        payload = json.loads(asyncio.run(server.get_company_news("TEST", 5, 14, [])))
    return {"sourceStatusKeys": sorted(payload["sourceStatus"]), "coverageState": payload["coverage"]["state"],
            "sourcesUsed": payload["meta"]["sourcesUsed"]}


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
    Case(name="tool.nws.coverage_website_not_available_failed_source", ids=["BR-NWS-007"],
         py=lambda: server._build_coverage({"company_ir": {"status": "WEBSITE_NOT_AVAILABLE"}}),
         ts={"js": """
const r = JSON.parse(await m.yahooFinance.getCompanyNews("TEST", 5, 14, ["company_ir"]));
return r.coverage;
""", "isolate": True},
         routes=NEWS_ROUTES, select=_coverage_view, mode="pinned", decision="Q3i",
         # Finding (Phase 2, for Q3i triage): Python's _compute_source_coverage (server.py:2402-2406) omits the
         # WEBSITE_NOT_AVAILABLE / DISCOVERY_* / FEED_NOT_FOUND / PARSE_ERROR states that its own _build_coverage
         # (server.py:315-320) lists as failed, so a failed source sits beside coverage FULL. The Worker's
         # computeSourceCoverage (yahoo-finance.ts:10992) includes them and reports PARTIAL.
         expect_py={"state": "FULL", "failed": [["company_ir", "WEBSITE_NOT_AVAILABLE"]], "skipped": [], "action": "CHECK_OFFICIAL_RELEASES"},
         expect_ts={"state": "PARTIAL", "failed": [["company_ir", "WEBSITE_NOT_AVAILABLE"]], "skipped": [], "action": "CHECK_OFFICIAL_RELEASES"}),

    # Q3h: sources=[] -- Python substitutes the four default sources, the Worker passes [] through (no source attempted).
    Case(name="tool.q3h.empty_sources_python_substitutes_defaults_worker_attempts_none", ids=["Q3h"],
         py=lambda: _python_news_sources_empty(),
         ts={"js": """
const r = JSON.parse(await m.yahooFinance.getCompanyNews("TEST", 5, 14, []));
return { sourceStatusKeys: Object.keys(r.sourceStatus).sort(), coverageState: r.coverage.state, sourcesUsed: r.meta.sourcesUsed };
""", "isolate": True},
         routes=NEWS_ROUTES, mode="pinned", decision="Q3h",
         expect_py={"sourceStatusKeys": ["finnhub", "marketaux", "yahoo_finance_news", "yahoo_finance_press_releases"],
                    "coverageState": "FULL", "sourcesUsed": []},
         expect_ts={"sourceStatusKeys": [], "coverageState": "FULL", "sourcesUsed": []}),
]


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

    # ── BR-SHR-006 (Q3c): Worker batch truncation at 5, Python unlimited ───────────────────────
    _series_case("prc.batch.q3c_seven_tickers_python_processes_all_worker_truncates_to_five", ["BR-SHR-006"], "slope",
                 [["T1", "T2", "T3", "T4", "T5", "T6", "T7"], 5], [100.00] + FLAT_SIX[:4] + [103.25],
                 select=lambda v: {"tickers": sorted(k for k in v if not k.startswith("_")),
                                   "remaining": (v.get("_truncated") or {}).get("remaining")},
                 mode="pinned", decision="Q3c",
                 expect_py={"tickers": ["T1", "T2", "T3", "T4", "T5", "T6", "T7"], "remaining": None},
                 expect_ts={"tickers": ["T1", "T2", "T3", "T4", "T5"], "remaining": ["T6", "T7"]}),
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
