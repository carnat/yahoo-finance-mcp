#!/usr/bin/env python3
"""Offline regressions for PR2 deterministic data-source fixes."""

import asyncio
import datetime
import json
import os
import pathlib
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402

_orig_tool = _FastMCP.tool


def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
    return _orig_tool(self, name=name, **kwargs)


_FastMCP.tool = _patched_tool  # type: ignore[method-assign]

import server as srv  # noqa: E402
from scripts import test_deployed_canaries as deployed_canaries  # noqa: E402
from yfmcp.tools import pricing  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


class _FastInfo(dict):
    currency = "USD"
    last_price = 50.0


class TestPr2DataQuality(unittest.TestCase):
    def tearDown(self):
        srv.yf.Ticker = self._server_ticker  # type: ignore[attr-defined]
        pricing.yf.Ticker = self._pricing_ticker  # type: ignore[attr-defined]

    def setUp(self):
        self._server_ticker = srv.yf.Ticker
        self._pricing_ticker = pricing.yf.Ticker

    def test_historical_prices_use_camel_case_rows(self):
        class FakeTicker:
            fast_info = _FastInfo()

            def history(self, period, interval, prepost=False):
                return pd.DataFrame(
                    {
                        "Open": [1.0],
                        "High": [2.0],
                        "Low": [0.5],
                        "Close": [1.5],
                        "Volume": [100],
                        "Adj Close": [1.4],
                    },
                    index=pd.DatetimeIndex(["2026-06-12"]),
                )

            def get_history_metadata(self):
                return {}

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        rows = json.loads(_run(pricing.get_historical_stock_prices("TSTPR2HIST")))
        self.assertEqual(
            set(rows[0]),
            {
                "date", "open", "high", "low", "close", "volume",
                "adjClose", "barStatus", "isFinal",
            },
        )
        self.assertEqual(rows[0]["barStatus"], "COMPLETE")
        self.assertTrue(rows[0]["isFinal"])

    def test_historical_prices_label_active_daily_row_incomplete(self):
        now = pd.Timestamp.now(tz="UTC")
        index = pd.DatetimeIndex([
            now.normalize() - pd.Timedelta(days=1),
            now.normalize(),
        ])

        class FakeTicker:
            fast_info = _FastInfo()

            def history(self, period, interval, prepost=False):
                return pd.DataFrame(
                    {
                        "Open": [10.0, 11.0],
                        "High": [11.0, 12.0],
                        "Low": [9.0, 10.0],
                        "Close": [10.5, 11.5],
                        "Volume": [1_000, 100],
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "previousClose": 10.5,
                    "currentTradingPeriod": {
                        "regular": {
                            "start": int((now - pd.Timedelta(hours=1)).timestamp()),
                            "end": int((now + pd.Timedelta(hours=1)).timestamp()),
                        },
                    },
                }

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        rows = json.loads(_run(pricing.get_historical_stock_prices("TSTACTIVEHIST")))
        self.assertEqual(rows[-2]["barStatus"], "COMPLETE")
        self.assertTrue(rows[-2]["isFinal"])
        self.assertEqual(rows[-1]["barStatus"], "INCOMPLETE")
        self.assertFalse(rows[-1]["isFinal"])

    def test_completed_null_close_row_is_incomplete_and_daily_derivatives_fail_closed(self):
        index = pd.date_range(
            end=pd.Timestamp("2026-07-28T00:00:00Z"),
            periods=40,
            freq="D",
        )
        closes = [100.0 + i for i in range(39)] + [float("nan")]

        class FastInfo(dict):
            currency = "USD"
            last_price = 140.0
            previous_close = 139.0
            year_high = 150.0
            year_low = 90.0
            fifty_day_average = 130.0
            two_hundred_day_average = 120.0

            def __init__(self):
                super().__init__(currency="USD")

        class FakeTicker:
            fast_info = FastInfo()

            def history(self, *args, **kwargs):
                return pd.DataFrame(
                    {
                        "Open": [99.0 + i for i in range(40)],
                        "High": [101.0 + i for i in range(40)],
                        "Low": [98.0 + i for i in range(40)],
                        "Close": closes,
                        "Adj Close": closes,
                        "Volume": [1_000 + i for i in range(40)],
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "regularMarketTime": int(
                        pd.Timestamp("2026-07-28T20:00:00Z").timestamp()
                    ),
                }

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]

        rows = json.loads(_run(pricing.get_historical_stock_prices("TSTNULLCLOSE")))
        self.assertEqual(rows[-2]["barStatus"], "COMPLETE")
        self.assertTrue(rows[-2]["isFinal"])
        self.assertIsNone(rows[-1]["close"])
        self.assertEqual(rows[-1]["barStatus"], "INCOMPLETE")
        self.assertFalse(rows[-1]["isFinal"])

        for result in (
            json.loads(_run(pricing.get_technical_indicators("TSTNULLTECH"))),
            json.loads(_run(pricing.get_price_slope("TSTNULLSLOPE", days=5))),
            json.loads(_run(pricing.get_volume_ratio("TSTNULLVOLUME"))),
            json.loads(_run(pricing.get_volume_gate("TSTNULLGATE"))),
        ):
            self.assertEqual(result["status"], "STALE_BAR")
            self.assertEqual(result["barStatus"], "STALE")
            self.assertEqual(result["latestAvailableBarDate"], "2026-07-27")
            self.assertEqual(result["dataDate"], "2026-07-27")
            self.assertTrue(result["excludedIncompleteBar"])
            self.assertTrue(result["retryAttempted"])
            self.assertEqual(result["recommendedNextAction"], "RETRY")

        price_stats = json.loads(_run(pricing.get_price_stats("TSTNULLSTATS")))
        self.assertEqual(price_stats["status"], "PARTIAL")
        self.assertEqual(price_stats["historicalBarStatus"], "STALE")
        self.assertEqual(price_stats["dataDate"], "2026-07-27")
        self.assertTrue(price_stats["excludedIncompleteBar"])
        self.assertEqual(price_stats["recommendedNextAction"], "RETRY")

        snapshot = json.loads(
            _run(pricing.get_market_snapshot("TSTNULLSNAPSHOT", mode="compact"))
        )
        self.assertEqual(snapshot["status"], "PARTIAL")
        self.assertFalse(snapshot["failedComponents"])
        self.assertIn("priceStats", snapshot["limitedComponents"])
        self.assertIn("technicalIndicators", snapshot["limitedComponents"])
        self.assertIn("volumeRatio", snapshot["limitedComponents"])
        self.assertIn("volumeGate", snapshot["limitedComponents"])
        self.assertEqual(snapshot["recommendedNextAction"], "RETRY")

    def test_missing_latest_volume_limits_only_volume_derivatives(self):
        index = pd.date_range(
            end=pd.Timestamp("2026-07-28T00:00:00Z"),
            periods=40,
            freq="D",
        )
        closes = [100.0 + i for i in range(40)]
        volumes = [1_000.0 + i for i in range(39)] + [float("nan")]

        class FastInfo(dict):
            currency = "USD"
            last_price = 140.0
            previous_close = 139.0
            year_high = 150.0
            year_low = 90.0
            fifty_day_average = 130.0
            two_hundred_day_average = 120.0

            def __init__(self):
                super().__init__(currency="USD")

        class FakeTicker:
            fast_info = FastInfo()

            def history(self, *args, **kwargs):
                return pd.DataFrame(
                    {
                        "Open": [99.0 + i for i in range(40)],
                        "High": [101.0 + i for i in range(40)],
                        "Low": [98.0 + i for i in range(40)],
                        "Close": closes,
                        "Adj Close": closes,
                        "Volume": volumes,
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "regularMarketTime": int(
                        pd.Timestamp("2026-07-28T20:00:00Z").timestamp()
                    ),
                }

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]

        technical = json.loads(
            _run(pricing.get_technical_indicators("TSTMISSINGVOLTECH"))
        )
        self.assertEqual(technical["status"], "OK")
        self.assertEqual(technical["barStatus"], "COMPLETE")
        self.assertEqual(technical["dataDate"], "2026-07-28")

        for result in (
            json.loads(_run(pricing.get_volume_ratio("TSTMISSINGVOLRATIO"))),
            json.loads(_run(pricing.get_volume_gate("TSTMISSINGVOLGATE"))),
        ):
            self.assertEqual(result["status"], "PARTIAL")
            self.assertEqual(result["barStatus"], "INCOMPLETE")
            self.assertEqual(result["freshnessStatus"], "CURRENT")
            self.assertEqual(result["latestAvailableBarDate"], "2026-07-28")
            self.assertEqual(result["dataDate"], "2026-07-28")
            self.assertTrue(result["retryAttempted"])
            self.assertEqual(result["recommendedNextAction"], "RETRY")

    def test_technical_indicators_exclude_active_daily_bar(self):
        now = pd.Timestamp.now(tz="UTC")
        index = pd.date_range(end=now.normalize(), periods=40, freq="D", tz="UTC")
        completed_closes = [100.0 + i for i in range(39)]
        raw_closes = completed_closes + [999.0]

        class FakeTicker:
            def history(self, period, interval, auto_adjust=False, keepna=False):
                return pd.DataFrame(
                    {
                        "Close": raw_closes,
                        "Adj Close": raw_closes,
                        "Volume": [1_000] * 40,
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "previousClose": completed_closes[-1],
                    "currentTradingPeriod": {
                        "regular": {
                            "start": int((now - pd.Timedelta(hours=1)).timestamp()),
                            "end": int((now + pd.Timedelta(hours=1)).timestamp()),
                        },
                    },
                }

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(pricing.get_technical_indicators("TSTACTIVETECH")))
        self.assertEqual(data["status"], "OK")
        self.assertEqual(data["lastClose"], completed_closes[-1])
        self.assertTrue(data["excludedIncompleteBar"])
        self.assertEqual(data["barStatus"], "COMPLETE")
        self.assertEqual(data["dataDate"], index[-2].date().isoformat())

    def test_price_stats_separates_live_quote_from_completed_history(self):
        now = pd.Timestamp.now(tz="UTC")
        index = pd.date_range(end=now.normalize(), periods=40, freq="D", tz="UTC")
        closes = [100.0 + i for i in range(39)] + [999.0]

        class FastInfo:
            last_price = 150.0
            previous_close = 149.0
            currency = "EUR"
            year_high = 180.0
            year_low = 80.0
            fifty_day_average = 140.0
            two_hundred_day_average = 120.0

        class FakeTicker:
            fast_info = FastInfo()
            info = {
                "marketState": "REGULAR",
                "regularMarketTime": int(now.timestamp()),
            }

            def history(self, period, interval, auto_adjust=False, keepna=False):
                return pd.DataFrame(
                    {
                        "Close": closes,
                        "Adj Close": closes,
                        "Volume": [1_000] * 40,
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "previousClose": closes[-2],
                    "currentTradingPeriod": {
                        "regular": {
                            "start": int((now - pd.Timedelta(hours=1)).timestamp()),
                            "end": int((now + pd.Timedelta(hours=1)).timestamp()),
                        },
                    },
                }

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(pricing.get_price_stats("TSTACTIVESTATS")))
        self.assertEqual(data["lastPrice"], 150.0)
        self.assertEqual(data["priceBasis"], "REGULAR_MARKET_PRICE")
        self.assertEqual(data["dataDate"], index[-2].date().isoformat())
        self.assertEqual(data["historicalBarStatus"], "COMPLETE")
        self.assertTrue(data["excludedIncompleteBar"])
        self.assertIsNotNone(data["priceTimestamp"])

    def test_volume_ratio_uses_completed_numerator_and_prior_denominators(self):
        now = pd.Timestamp.now(tz="UTC")
        index = pd.date_range(end=now.normalize(), periods=92, freq="D", tz="UTC")
        volumes = [100] * 90 + [200, 10_000]
        closes = [50.0] * 92

        class FakeTicker:
            def history(self, period, interval, auto_adjust=False, keepna=False):
                return pd.DataFrame(
                    {"Close": closes, "Adj Close": closes, "Volume": volumes},
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "previousClose": 50.0,
                    "currentTradingPeriod": {
                        "regular": {
                            "start": int((now - pd.Timedelta(hours=1)).timestamp()),
                            "end": int((now + pd.Timedelta(hours=1)).timestamp()),
                        },
                    },
                }

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(pricing.get_volume_ratio("TSTACTIVEVOL")))
        self.assertEqual(data["lastVolume"], 200)
        self.assertEqual(data["avgVolume10d"], 100)
        self.assertEqual(data["avgVolume90d"], 100)
        self.assertEqual(data["ratio10d"], 2.0)
        self.assertEqual(data["volumeFlag"], "HIGH")
        self.assertTrue(data["excludedIncompleteBar"])

    def test_volume_ratio_does_not_shift_past_missing_latest_completed_volume(self):
        now = pd.Timestamp.now(tz="UTC")
        index = pd.date_range(end=now.normalize(), periods=12, freq="D", tz="UTC")
        volumes = [100] * 10 + [float("nan"), 10_000]
        closes = [50.0] * 12

        class FakeTicker:
            def history(self, period, interval, auto_adjust=False, keepna=False):
                return pd.DataFrame(
                    {"Close": closes, "Adj Close": closes, "Volume": volumes},
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "previousClose": 50.0,
                    "currentTradingPeriod": {
                        "regular": {
                            "start": int((now - pd.Timedelta(hours=1)).timestamp()),
                            "end": int((now + pd.Timedelta(hours=1)).timestamp()),
                        },
                    },
                }

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(pricing.get_volume_ratio("TSTMISSINGLATESTVOL")))
        self.assertEqual(data["status"], "PARTIAL")
        self.assertEqual(data["barStatus"], "INCOMPLETE")
        self.assertIsNone(data["lastVolume"])
        self.assertTrue(data["retryAttempted"])
        self.assertEqual(data["recommendedNextAction"], "RETRY")

    def test_volume_gate_uses_last_completed_session(self):
        now = pd.Timestamp.now(tz="UTC")
        index = pd.date_range(end=now.normalize(), periods=22, freq="D", tz="UTC")
        volumes = [200] * 20 + [100, 1]
        closes = [25.0] * 22

        class FakeTicker:
            fast_info = {"currency": "USD"}

            def history(self, period, interval, auto_adjust=False, keepna=False):
                return pd.DataFrame(
                    {"Close": closes, "Adj Close": closes, "Volume": volumes},
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "previousClose": 25.0,
                    "currentTradingPeriod": {
                        "regular": {
                            "start": int((now - pd.Timedelta(hours=1)).timestamp()),
                            "end": int((now + pd.Timedelta(hours=1)).timestamp()),
                        },
                    },
                }

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(pricing.get_volume_gate("TSTACTIVEGATE")))
        self.assertEqual(data["lastVolume"], 100)
        self.assertEqual(data["adv20d"], 200)
        self.assertEqual(data["ratio20d"], 0.5)
        self.assertTrue(data["gatePass"])
        self.assertTrue(data["excludedIncompleteBar"])
        self.assertEqual(data["dataDate"], index[-2].date().isoformat())

    def test_market_quote_labels_regular_market_price_observation(self):
        timestamp = int(pd.Timestamp("2026-06-23T15:20:00Z").timestamp())

        class FakeTicker:
            fast_info = {
                "currency": "USD",
                "exchange": "NMS",
                "quoteType": "EQUITY",
                "lastPrice": 101.25,
            }
            info = {
                "marketState": "REGULAR",
                "regularMarketTime": timestamp,
            }

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        result = json.loads(_run(pricing.get_fast_info("TSTPRICEOBS")))

        self.assertEqual(result["priceBasis"], "REGULAR_MARKET_PRICE")
        self.assertEqual(result["observationType"], "REGULAR_MARKET_QUOTE")
        self.assertEqual(result["marketState"], "REGULAR")
        self.assertEqual(result["priceTimestamp"], "2026-06-23T15:20:00Z")

    def test_price_slope_keeps_adjusted_and_raw_close_semantics_aligned(self):
        index = pd.DatetimeIndex(["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"])

        class FakeTicker:
            def history(self, period, interval, auto_adjust=False, keepna=False):
                self.request = (period, interval, auto_adjust, keepna)
                return pd.DataFrame(
                    {
                        "Close": [98.0, 100.0, 102.0, 104.0],
                        "Adj Close": [49.0, 50.0, 51.0, 52.0],
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {}

        ticker = FakeTicker()
        pricing.yf.Ticker = lambda symbol: ticker  # type: ignore[assignment]
        result = json.loads(_run(pricing.get_price_slope("TSTSLOPEOBS", days=3)))

        self.assertEqual(ticker.request, ("1mo", "1d", False, True))
        self.assertEqual(result["startClose"], 49.0)
        self.assertEqual(result["endClose"], 52.0)
        self.assertEqual(result["endRawClose"], 104.0)
        self.assertEqual(result["priceBasis"], "ADJUSTED_CLOSE")
        self.assertEqual(result["observationType"], "DAILY_PRICE_BAR")
        self.assertEqual(result["calculationBasis"], "COMPLETED_SESSION_CLOSE_TO_CLOSE")
        self.assertEqual(result["observationCount"], 4)
        self.assertEqual(result["barStatus"], "COMPLETE")
        self.assertEqual(result["dataDate"], "2026-06-23")

    def test_price_slope_retries_then_uses_one_consistent_raw_series(self):
        index = pd.DatetimeIndex(
            ["2026-07-17", "2026-07-20", "2026-07-21", "2026-07-22", "2026-07-23", "2026-07-24"],
            tz="UTC",
        )

        class FakeTicker:
            def __init__(self):
                self.history_calls = 0

            def history(self, period, interval, auto_adjust=False, keepna=False):
                self.history_calls += 1
                return pd.DataFrame(
                    {
                        "Close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                        "Adj Close": [50.0, 50.5, 51.0, 51.5, 52.0, float("nan")],
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "regularMarketTime": int(pd.Timestamp("2026-07-24T20:00:00Z").timestamp()),
                }

        ticker = FakeTicker()
        pricing.yf.Ticker = lambda symbol: ticker  # type: ignore[assignment]
        result = json.loads(_run(pricing.get_price_slope("TSTRAWFALLBACK", days=5)))

        self.assertEqual(ticker.history_calls, 2)
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["startClose"], 100.0)
        self.assertEqual(result["endClose"], 105.0)
        self.assertEqual(result["endRawClose"], 105.0)
        self.assertEqual(result["priceBasis"], "UNADJUSTED_CLOSE")
        self.assertEqual(result["dataDate"], "2026-07-24")
        self.assertTrue(result["retryAttempted"])
        self.assertTrue(result["fallbackUsed"])

    def test_daily_bar_canary_rejects_close_incomplete_final_row(self):
        deployed_canaries.daily_bar_finality_contract(
            {
                "ok": True,
                "data": [
                    {
                        "date": "2026-07-27",
                        "close": 100.0,
                        "adjClose": 100.0,
                        "barStatus": "COMPLETE",
                        "isFinal": True,
                    },
                    {
                        "date": "2026-07-28",
                        "close": None,
                        "adjClose": None,
                        "barStatus": "INCOMPLETE",
                        "isFinal": False,
                    },
                ],
            },
            {},
        )
        with self.assertRaisesRegex(AssertionError, "marked final"):
            deployed_canaries.daily_bar_finality_contract(
                {
                    "ok": True,
                    "data": [
                        {
                            "date": "2026-07-28",
                            "close": None,
                            "adjClose": None,
                            "barStatus": "COMPLETE",
                            "isFinal": True,
                        }
                    ],
                },
                {},
            )

    def test_price_slope_retries_a_stale_completed_bar(self):
        stale_index = pd.DatetimeIndex(
            ["2026-07-16", "2026-07-17", "2026-07-20", "2026-07-21", "2026-07-22", "2026-07-23"],
            tz="UTC",
        )
        fresh_index = pd.DatetimeIndex(
            ["2026-07-17", "2026-07-20", "2026-07-21", "2026-07-22", "2026-07-23", "2026-07-24"],
            tz="UTC",
        )

        class FakeTicker:
            def __init__(self):
                self.history_calls = 0

            def history(self, period, interval, auto_adjust=False, keepna=False):
                self.history_calls += 1
                index = stale_index if self.history_calls == 1 else fresh_index
                return pd.DataFrame(
                    {
                        "Close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                        "Adj Close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "regularMarketTime": int(pd.Timestamp("2026-07-24T20:00:00Z").timestamp()),
                }

        ticker = FakeTicker()
        pricing.yf.Ticker = lambda symbol: ticker  # type: ignore[assignment]
        result = json.loads(_run(pricing.get_price_slope("TSTSTALERETRY", days=5)))

        self.assertEqual(ticker.history_calls, 2)
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["dataDate"], "2026-07-24")
        self.assertEqual(result["freshnessStatus"], "CURRENT")
        self.assertTrue(result["retryAttempted"])

    def test_price_slope_does_not_publish_a_persistently_stale_slope(self):
        index = pd.DatetimeIndex(
            ["2026-07-16", "2026-07-17", "2026-07-20", "2026-07-21", "2026-07-22", "2026-07-23"],
            tz="UTC",
        )

        class FakeTicker:
            def __init__(self):
                self.history_calls = 0

            def history(self, period, interval, auto_adjust=False, keepna=False):
                self.history_calls += 1
                return pd.DataFrame(
                    {
                        "Close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                        "Adj Close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "regularMarketTime": int(pd.Timestamp("2026-07-24T20:00:00Z").timestamp()),
                }

        ticker = FakeTicker()
        pricing.yf.Ticker = lambda symbol: ticker  # type: ignore[assignment]
        result = json.loads(_run(pricing.get_price_slope("TSTPERSISTENTSTALE", days=5)))

        self.assertEqual(ticker.history_calls, 2)
        self.assertEqual(result["status"], "STALE_BAR")
        self.assertIsNone(result["slopePct"])
        self.assertEqual(result["freshnessStatus"], "STALE")
        self.assertEqual(result["recommendedNextAction"], "RETRY")

    def test_price_slope_excludes_the_active_daily_bar(self):
        today = pd.Timestamp.now(tz="UTC").normalize()
        index = pd.date_range(end=today, periods=7, freq="D")
        now_epoch = int(pd.Timestamp.now(tz="UTC").timestamp())

        class FakeTicker:
            def history(self, period, interval, auto_adjust=False, keepna=False):
                return pd.DataFrame(
                    {
                        "Close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 999.0],
                        "Adj Close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 999.0],
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "regularMarketTime": now_epoch,
                    "previousClose": 105.0,
                    "currentTradingPeriod": {
                        "regular": {
                            "start": now_epoch - 3600,
                            "end": now_epoch + 3600,
                        }
                    },
                }

        pricing.yf.Ticker = lambda symbol: FakeTicker()  # type: ignore[assignment]
        result = json.loads(_run(pricing.get_price_slope("TSTACTIVEBAR", days=5)))

        self.assertEqual(result["endClose"], 105.0)
        self.assertEqual(result["dataDate"], index[-2].date().isoformat())
        self.assertTrue(result["excludedIncompleteBar"])
        self.assertEqual(result["barStatus"], "COMPLETE")

    def test_price_slope_detects_a_stale_previous_close_during_market_hours(self):
        today = pd.Timestamp.now(tz="UTC").normalize()
        index = pd.date_range(end=today, periods=7, freq="D")
        now_epoch = int(pd.Timestamp.now(tz="UTC").timestamp())

        class FakeTicker:
            def __init__(self):
                self.history_calls = 0

            def history(self, period, interval, auto_adjust=False, keepna=False):
                self.history_calls += 1
                return pd.DataFrame(
                    {
                        "Close": [99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 999.0],
                        "Adj Close": [99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 999.0],
                    },
                    index=index,
                )

            def get_history_metadata(self):
                return {
                    "exchangeTimezoneName": "UTC",
                    "regularMarketTime": now_epoch,
                    "previousClose": 105.0,
                    "currentTradingPeriod": {
                        "regular": {
                            "start": now_epoch - 3600,
                            "end": now_epoch + 3600,
                        }
                    },
                }

        ticker = FakeTicker()
        pricing.yf.Ticker = lambda symbol: ticker  # type: ignore[assignment]
        result = json.loads(_run(pricing.get_price_slope("TSTACTIVESTALE", days=5)))

        self.assertEqual(ticker.history_calls, 2)
        self.assertEqual(result["status"], "STALE_BAR")
        self.assertIsNone(result["slopePct"])
        self.assertEqual(result["recommendedNextAction"], "RETRY")

    def test_worker_price_slope_pairs_each_close_with_its_timestamp(self):
        worker_source = (
            pathlib.Path(__file__).resolve().parents[1] / "worker" / "src" / "yahoo-finance.ts"
        ).read_text(encoding="utf-8")
        slope_source = worker_source.split("export async function getPriceSlope", 1)[1].split(
            "export async function getVolumeRatio", 1
        )[0]

        self.assertIn("timestamp: parsedTimestamp", worker_source)
        self.assertIn("endRawClose: endObservation.rawClose", slope_source)
        self.assertIn("normalizedDays + 1", slope_source)
        self.assertIn("excludedIncompleteBar", slope_source)
        self.assertIn("retryAttempted", slope_source)
        self.assertIn("COMPLETED_SESSION_CLOSE_TO_CLOSE", slope_source)
        self.assertIn('priceBasis: "REGULAR_MARKET_PRICE"', worker_source)
        self.assertNotIn("const closes = adjclose.filter", slope_source)
        self.assertNotIn("const priceSeries =", slope_source)

    def test_worker_daily_derived_tools_share_completed_bar_selector(self):
        worker_source = (
            pathlib.Path(__file__).resolve().parents[1] / "worker" / "src" / "yahoo-finance.ts"
        ).read_text(encoding="utf-8")
        for start, end in (
            ("export async function getTechnicalIndicators", "export async function getPriceSlope"),
            ("export async function getVolumeRatio", "export async function getMaPosition"),
            ("export async function getVolumeGate", "export async function getMarketSnapshot"),
        ):
            section = worker_source.split(start, 1)[1].split(end, 1)[0]
            self.assertIn("prepareCompletedDailySeries", section)
            self.assertIn("excludedIncompleteBar", section)
        volume_section = worker_source.split(
            "export async function getVolumeRatio", 1
        )[1].split("export async function getMaPosition", 1)[0]
        self.assertNotIn("fastInfo.lastVolume", volume_section)
        self.assertIn("initialLatestRow?.volume == null", volume_section)
        self.assertIn('status: "PARTIAL"', volume_section)
        self.assertIn('barStatus: "INCOMPLETE"', volume_section)
        gate_section = worker_source.split(
            "export async function getVolumeGate", 1
        )[1].split("export async function getMarketSnapshot", 1)[0]
        self.assertIn("initialLatestRow?.volume == null", gate_section)
        self.assertIn("initialLatestRow?.rawClose == null", gate_section)
        self.assertIn('status: "PARTIAL"', gate_section)
        self.assertIn('gatePass: null', gate_section)
        self.assertIn("completedTimestamps.has(t) && hasUsableClose", worker_source)
        self.assertIn(
            "completedRows[completedRows.length - 1].adjustedClose == null",
            worker_source,
        )

    def test_options_realized_volatility_uses_completed_bar_selector_in_both_runtimes(self):
        root = pathlib.Path(__file__).resolve().parents[1]
        worker_source = (root / "worker" / "src" / "yahoo-finance.ts").read_text(
            encoding="utf-8"
        )
        python_source = (root / "server.py").read_text(encoding="utf-8")
        worker_section = worker_source.split(
            "export async function getOptionsFlowScan", 1
        )[1].split("export async function getPriceTargetBracket", 1)[0]
        python_section = python_source.split(
            "async def get_options_flow_scan", 1
        )[1].split("async def get_price_target_bracket", 1)[0]
        self.assertIn("prepareCompletedDailySeries", worker_section)
        self.assertIn("historicalBarStatus", worker_section)
        self.assertIn("_load_completed_daily_history", python_section)
        self.assertIn("historical_bar_status", python_section)

    def test_options_summary_rejects_invalid_expiry_hint(self):
        class FakeTicker:
            options = ["2026-06-18"]

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        previous = os.environ.get("MCP_ENVELOPE_V2")
        os.environ["MCP_ENVELOPE_V2"] = "true"
        try:
            payload = json.loads(_run(srv.get_options_summary("ASTS", expiry_hint="2026-06-19")))
        finally:
            if previous is None:
                os.environ.pop("MCP_ENVELOPE_V2", None)
            else:
                os.environ["MCP_ENVELOPE_V2"] = previous

        self.assertFalse(payload["ok"])
        self.assertIsNone(payload["data"])
        error = payload["error"]
        self.assertEqual(error["code"], "INVALID_EXPIRY_DATE")
        self.assertEqual(error["nearestExpiration"], "2026-06-18")

    def test_credit_health_splits_ebit_and_ebitda_coverage(self):
        col = pd.Timestamp("2026-03-31")

        class FakeTicker:
            quarterly_balance_sheet = pd.DataFrame(
                {col: [100.0, 20.0]},
                index=["Total Debt", "Cash And Cash Equivalents"],
            )
            quarterly_income_stmt = pd.DataFrame(
                {col: [40.0, 20.0, -5.0]},
                index=["EBITDA", "EBIT", "Interest Expense"],
            )

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_credit_health("TST")))
        self.assertEqual(data["interestCoverage"], 4.0)
        self.assertEqual(data["interestCoverageEbit"], 4.0)
        self.assertEqual(data["interestCoverageEbitda"], 8.0)

    def test_credit_health_annualizes_latest_quarter_interest_once_and_uses_operational_ebitda(self):
        col1 = pd.Timestamp("2026-03-31")
        col2 = pd.Timestamp("2025-12-31")
        col3 = pd.Timestamp("2025-09-30")
        col4 = pd.Timestamp("2025-06-30")

        class FakeTicker:
            quarterly_balance_sheet = pd.DataFrame(
                {col1: [1_000_000_000.0, 100_000_000.0]},
                index=["Total Debt", "Cash And Cash Equivalents"],
            )
            quarterly_income_stmt = pd.DataFrame(
                {
                    col1: [920_000_000.0, 100_000_000.0, 40_000_000.0, 52_800_000.0, 256_000_000.0],
                    col2: [920_000_000.0, 100_000_000.0, 40_000_000.0, 52_800_000.0, 256_000_000.0],
                    col3: [920_000_000.0, 100_000_000.0, 40_000_000.0, 52_800_000.0, 256_000_000.0],
                    col4: [920_000_000.0, 100_000_000.0, 40_000_000.0, 52_800_000.0, 256_000_000.0],
                },
                index=[
                    "EBITDA",
                    "EBIT",
                    "Depreciation And Amortization",
                    "Interest Expense Non Operating",
                    "Interest Expense",
                ],
            )

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_credit_health("MRVL")))
        self.assertEqual(data["interestExpenseUsd"], 211_200_000.0)
        self.assertEqual(data["ebitdaUsd"], 3_680_000_000.0)
        self.assertEqual(data["operationalEbitdaUsd"], 560_000_000.0)
        self.assertEqual(data["netDebtToEbitda"], 1.61)
        self.assertEqual(data["interestCoverageEbitda"], 2.65)
        self.assertEqual(data["operationalEbitdaSource"], "ttm_operating_income_plus_da")
        warning_codes = [w.get("code") for w in data.get("warnings", []) if isinstance(w, dict)]
        self.assertIn("NON_OPERATING_EBITDA_DIVERGENCE", warning_codes)

    def test_credit_health_falls_back_to_provider_ebitda_when_da_missing(self):
        col1 = pd.Timestamp("2026-03-31")
        col2 = pd.Timestamp("2025-12-31")
        col3 = pd.Timestamp("2025-09-30")
        col4 = pd.Timestamp("2025-06-30")

        class FakeTicker:
            quarterly_balance_sheet = pd.DataFrame(
                {col1: [500.0, 100.0]},
                index=["Total Debt", "Cash And Cash Equivalents"],
            )
            quarterly_income_stmt = pd.DataFrame(
                {
                    col1: [150.0, 100.0, 10.0],
                    col2: [150.0, 100.0, 10.0],
                    col3: [150.0, 100.0, 10.0],
                    col4: [150.0, 100.0, 10.0],
                },
                index=["EBITDA", "EBIT", "Interest Expense Non Operating"],
            )

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_credit_health("NBIS")))
        self.assertEqual(data["operationalEbitdaUsd"], 600.0)
        self.assertEqual(data["operationalEbitdaSource"], "provider_ebitda_fallback")
        warning_codes = [w.get("code") for w in data.get("warnings", []) if isinstance(w, dict)]
        self.assertIn("OPERATIONAL_EBITDA_UNAVAILABLE", warning_codes)

    def test_earnings_compare_skips_upcoming_null_actual_quarter(self):
        async def fake_metrics(**kwargs):
            return json.dumps({
                "period": "FY2026 Q2",
                "reportedAt": "2026-06-10",
                "metrics": {"revenue": {"value": 123_000_000_000}, "epsDiluted": {"value": 2.31}},
            })

        async def fake_ea(**kwargs):
            return json.dumps({
                "revenueEstimate": [{"period": "FY2026 Q2", "avg": 121_000_000_000}],
                "earningsHistory": [
                    {"quarter": "2026-09-30", "epsActual": None, "epsEstimate": 2.40},
                    {"quarter": "2026-06-30", "period": "FY2026 Q2", "epsActual": 2.31, "epsEstimate": 2.25},
                ],
            })

        old_metrics = srv.extract_earnings_metrics
        old_ea = srv.get_earnings_analysis
        try:
            srv.extract_earnings_metrics = fake_metrics
            srv.get_earnings_analysis = fake_ea
            parsed = json.loads(_run(srv.compare_earnings_actual_vs_estimate("AAPL")))
        finally:
            srv.extract_earnings_metrics = old_metrics
            srv.get_earnings_analysis = old_ea
        data = parsed["data"] if parsed.get("ok") else parsed
        self.assertEqual(data["reportedPeriod"], "FY2026 Q2")
        self.assertEqual(data["reportedDate"], "2026-06-30")
        self.assertEqual(data["actual"]["eps"]["value"], 2.31)
        self.assertEqual(data["estimate"]["eps"]["value"], 2.25)
        self.assertAlmostEqual(data["surprise"]["epsSurprisePct"], 2.67, places=2)

    def test_earnings_compare_no_reported_quarter_warning(self):
        async def fake_metrics(**kwargs):
            return json.dumps({"period": "FY2026 Q3", "metrics": {"revenue": {"value": 100.0}, "epsDiluted": {"value": None}}})

        async def fake_ea(**kwargs):
            return json.dumps({"earningsHistory": [{"quarter": "2026-09-30", "epsActual": None, "epsEstimate": 1.2}]})

        old_metrics = srv.extract_earnings_metrics
        old_ea = srv.get_earnings_analysis
        try:
            srv.extract_earnings_metrics = fake_metrics
            srv.get_earnings_analysis = fake_ea
            parsed = json.loads(_run(srv.compare_earnings_actual_vs_estimate("AAPL")))
        finally:
            srv.extract_earnings_metrics = old_metrics
            srv.get_earnings_analysis = old_ea
        data = parsed["data"] if parsed.get("ok") else parsed
        warnings = (parsed.get("meta") or {}).get("warnings") or data.get("warnings") or []
        self.assertEqual(data["confidence"], "NOT_DISCLOSED")
        self.assertIn("NO_REPORTED_QUARTER", [w.get("code") for w in warnings])

    def test_upgrade_counts_exclude_initiations_and_position_uses_canonical_radar(self):
        now = pd.Timestamp.now()

        class FakeTicker:
            upgrades_downgrades = pd.DataFrame(
                [
                    {"GradeDate": now, "Action": "initiated", "ToGrade": "Buy", "FromGrade": "", "Firm": "A"},
                    {"GradeDate": now, "Action": "up", "ToGrade": "Buy", "FromGrade": "Hold", "Firm": "B"},
                    {"GradeDate": now, "Action": "down", "ToGrade": "Hold", "FromGrade": "Buy", "Firm": "C"},
                    {"GradeDate": now, "Action": "main", "ToGrade": "Hold", "FromGrade": "Hold", "Firm": "D"},
                ]
            )

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        radar = json.loads(_run(srv.get_analyst_upgrade_radar("TST", days_back=30)))
        self.assertEqual(radar["upgrades30d"], 1)
        self.assertEqual(radar["downgrades30d"], 1)
        self.assertEqual(radar["initiations30d"], 1)

        async def fake_radar(ticker, days_back=30):
            return json.dumps(radar)

        async def fake_consensus(ticker):
            return json.dumps({"dominantRating": "buy", "totalAnalysts": 10})

        async def fake_price(ticker):
            return json.dumps({})

        async def fake_earnings(ticker):
            return json.dumps({})

        async def fake_tech(ticker, period="3mo"):
            return json.dumps({})

        async def fake_ma(ticker):
            return json.dumps({})

        originals = (
            srv.get_analyst_upgrade_radar,
            srv.get_analyst_consensus,
            srv.get_price_stats,
            srv.get_earnings_momentum,
            srv.get_technical_indicators,
            srv.get_ma_position,
        )
        try:
            srv.get_analyst_upgrade_radar = fake_radar
            srv.get_analyst_consensus = fake_consensus
            srv.get_price_stats = fake_price
            srv.get_earnings_momentum = fake_earnings
            srv.get_technical_indicators = fake_tech
            srv.get_ma_position = fake_ma
            position = json.loads(_run(srv.analyze_position_signals("TST")))
        finally:
            (
                srv.get_analyst_upgrade_radar,
                srv.get_analyst_consensus,
                srv.get_price_stats,
                srv.get_earnings_momentum,
                srv.get_technical_indicators,
                srv.get_ma_position,
            ) = originals
        self.assertEqual(position["t1_inputs"]["upgrades30d"], 1)
        self.assertEqual(position["t1_inputs"]["initiations30d"], 1)

    def test_recent_revision_signal_beats_negative_90d_context(self):
        class FakeTicker:
            fast_info = _FastInfo()
            eps_trend = pd.DataFrame(
                {
                    "period": ["0q"],
                    "current": [1.05],
                    "7daysAgo": [1.0],
                    "30daysAgo": [1.0],
                    "90daysAgo": [1.8],
                }
            )
            earnings_history = pd.DataFrame(
                {
                    "epsActual": [1.2, 1.1, 1.0, 0.9],
                    "epsEstimate": [1.0, 1.0, 0.9, 0.8],
                    "surprisePercent": [0.2, 0.1, 0.11, 0.12],
                }
            )

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_earnings_momentum("NBIS")))
        self.assertEqual(data["forwardRevisionSignal"], "POSITIVE")
        self.assertNotEqual(data["compositeMomentumSignal"], "NEGATIVE")
        self.assertIn("90d only as fallback/context", data["compositeMethodNote"])

    def test_price_target_distance_exposes_inferred_tag_and_deprecated_tag(self):
        class FakeTicker:
            fast_info = {"lastPrice": 100.0}

            def history(self, period, interval):
                return pd.DataFrame(index=pd.DatetimeIndex(["2026-06-12"]))

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.calculate_price_target_distance("ASTS", reference_target_price=120.0)))
        self.assertEqual(data["inferredTag"], "NEAR")
        self.assertEqual(data["tag"], "NEAR")
        self.assertIn("Deprecated", data["tagNote"])
        self.assertEqual(data["currentToTargetRatioPct"], 83.3)
        self.assertEqual(data["distanceToTargetPct"], 20.0)
        self.assertEqual(data["distanceConvention"], "(referenceTargetPrice-currentPrice)/currentPrice*100")

    def test_short_momentum_uses_observation_date(self):
        observation = datetime.datetime(2026, 7, 15, tzinfo=datetime.UTC)

        class FakeTicker:
            info = {
                "sharesShort": 120,
                "sharesShortPriorMonth": 100,
                "shortPercentOfFloat": 0.05,
                "shortRatio": 2.5,
                "dateShortInterest": int(observation.timestamp()),
            }

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_short_momentum("AEHR")))
        self.assertEqual(data["dateShortInterest"], "2026-07-15")
        self.assertEqual(data["dataDate"], "2026-07-15")
        self.assertEqual(data["dataDateBasis"], "SHORT_INTEREST_OBSERVATION")

    def test_risk_mentions_reject_xbrl_identifier_noise(self):
        async def fake_search(**kwargs):
            return json.dumps({
                "matchCount": 2,
                "matches": [
                    {"term": "China", "sectionHeading": "Risk Factors", "context": "srt:ChinaMember"},
                    {
                        "term": "China",
                        "sectionHeading": "Risk Factors",
                        "context": "Our operations in China may be adversely affected by tariffs and export controls.",
                    },
                ],
            })

        old_search = srv.search_sec_filing_text
        try:
            srv.search_sec_filing_text = fake_search
            data = json.loads(_run(srv.extract_risk_factor_mentions("AAOI", ["China"])))
        finally:
            srv.search_sec_filing_text = old_search
        self.assertEqual(data["status"], "FOUND")
        self.assertEqual(len(data["matches"]), 1)
        self.assertIn("operations in China", data["matches"][0]["excerpt"])
        self.assertEqual(data["rejectedNoiseCount"], 1)

    def test_entity_exposure_rejects_competitor_list(self):
        async def fake_index(**kwargs):
            return json.dumps({"filingType": "10-K", "index": {"sections": [], "tables": []}})

        async def fake_geo(**kwargs):
            return json.dumps({"status": "NOT_FOUND", "value": None, "confidence": "LOW", "evidence": {}})

        async def fake_search(**kwargs):
            terms = kwargs.get("search_terms") or []
            if "foxconn" in terms:
                return json.dumps({
                    "matches": [{
                        "term": "foxconn",
                        "sectionHeading": "Competition",
                        "context": "Our competitors include Foxconn, TSMC, and other large manufacturers.",
                    }],
                })
            return json.dumps({"matches": []})

        async def fake_risk(**kwargs):
            return json.dumps({"status": "NOT_FOUND", "matches": []})

        old_index = srv.get_sec_filing_index
        old_geo = srv.extract_geographic_revenue
        old_search = srv.search_sec_filing_text
        old_risk = srv.extract_risk_factor_mentions
        try:
            srv.get_sec_filing_index = fake_index
            srv.extract_geographic_revenue = fake_geo
            srv.search_sec_filing_text = fake_search
            srv.extract_risk_factor_mentions = fake_risk
            data = json.loads(_run(srv.extract_exposure("AAOI", "china")))
        finally:
            srv.get_sec_filing_index = old_index
            srv.extract_geographic_revenue = old_geo
            srv.search_sec_filing_text = old_search
            srv.extract_risk_factor_mentions = old_risk
        self.assertEqual(data["entityExposure"]["status"], "NOT_FOUND")
        self.assertEqual(data["entityExposure"]["entities"], [])
        self.assertEqual(data["entityExposure"]["rejectedNoiseCount"], 1)

    def test_event_dedupe_ignores_source_url_and_keeps_refs(self):
        title = "Coherent and Lumentum Stocks Have Tumbled. Why Now Is the Time to Buy the Dip"
        gid1 = srv._make_duplicate_group_id("COHR", title, "2026-06-10T12:00:00Z", None, "https://a.example/x")
        gid2 = srv._make_duplicate_group_id("COHR", title, "2026-06-10T12:30:00Z", None, "https://b.example/y")
        self.assertEqual(gid1, gid2)
        deduped = srv._dedupe_event_items([
            {"title": title, "publishedAt": "2026-06-10T12:00:00Z", "sourceType": "company_news", "source": "finnhub", "duplicateGroupId": gid1, "url": "https://a.example/x"},
            {"title": title, "publishedAt": "2026-06-10T12:30:00Z", "sourceType": "yahoo_finance_news", "source": "yahoo_finance_news", "duplicateGroupId": gid2, "url": "https://b.example/y"},
        ], [])
        self.assertEqual(len(deduped), 1)
        self.assertEqual(len(deduped[0]["sourceRefs"]), 1)

    def test_form4_xml_parser_extracts_transaction(self):
        xml = """
        <ownershipDocument>
          <reportingOwner><reportingOwnerId><rptOwnerName>Jane Doe</rptOwnerName></reportingOwnerId>
          <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship></reportingOwner>
          <nonDerivativeTransaction>
            <transactionDate><value>2026-06-10</value></transactionDate>
            <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
            <transactionAmounts>
              <transactionShares><value>1000</value></transactionShares>
              <transactionPricePerShare><value>12.50</value></transactionPricePerShare>
            </transactionAmounts>
            <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
          </nonDerivativeTransaction>
        </ownershipDocument>
        """
        parsed = srv._parse_form4_transaction(xml)
        self.assertEqual(parsed["owner"], "Jane Doe")
        self.assertEqual(parsed["transactionLabel"], "Purchase")
        self.assertEqual(parsed["value"], 12500.0)

    def test_form4_transformed_html_parser_extracts_transaction(self):
        html = """
        <html><body>
          <table width="100%" border="1">
            <tr>
              <td rowspan="4">
                <span class="MedSmallFormText">1. Name and Address of Reporting Person</span>
                <table><tr><td><a href="/cgi-bin/browse-edgar?action=getcompany&amp;CIK=1">LEVINSON ARTHUR D</a></td></tr></table>
              </td>
              <td>
                <span class="MedSmallFormText">2. Issuer Name and Ticker or Trading Symbol</span>
                <br>Apple Inc. [ <span class="FormData">AAPL</span> ]
              </td>
              <td rowspan="2">
                <span class="MedSmallFormText">5. Relationship of Reporting Person(s) to Issuer</span>
                <table>
                  <tr><td><span class="FormData">X</span></td><td>Director</td><td></td><td>10% Owner</td></tr>
                  <tr><td></td><td>Officer (give title below)</td><td></td><td>Other (specify below)</td></tr>
                </table>
              </td>
            </tr>
            <tr><td><span class="MedSmallFormText">6. Individual or Joint/Group Filing</span></td></tr>
          </table>
          <table width="100%" border="1" cellspacing="0" cellpadding="4">
            <thead>
              <tr><th colspan="11">Table I - Non-Derivative Securities Acquired, Disposed of, or Beneficially Owned</th></tr>
              <tr><th>1. Title of Security</th><th>2. Transaction Date</th><th>2A.</th><th colspan="2">3. Transaction Code</th><th colspan="3">4. Securities Acquired (A) or Disposed Of (D)</th><th>5. Amount Owned</th><th>6. Ownership Form</th><th>7. Nature</th></tr>
              <tr><th></th><th></th><th></th><th>Code</th><th>V</th><th>Amount</th><th>(A) or (D)</th><th>Price</th><th></th><th></th><th></th></tr>
            </thead>
            <tbody>
              <tr>
                <td><span class="FormData">Common Stock</span></td>
                <td><span class="FormData">05/27/2026</span></td>
                <td></td>
                <td><span class="SmallFormData">S</span></td>
                <td></td>
                <td><span class="FormData">50,000</span></td>
                <td><span class="FormData">D</span></td>
                <td><span class="FormText">$</span><span class="FormData">311.02</span><span class="FootnoteData"><sup>(1)</sup></span></td>
                <td><span class="FormData">3,764,576</span></td>
                <td><span class="FormData">D</span></td>
                <td></td>
              </tr>
            </tbody>
          </table>
        </body></html>
        """
        parsed = srv._parse_form4_transaction(html)
        self.assertEqual(parsed["owner"], "LEVINSON ARTHUR D")
        self.assertEqual(parsed["role"], "director")
        self.assertEqual(parsed["transactionCode"], "S")
        self.assertEqual(parsed["transactionLabel"], "Sale")
        self.assertEqual(parsed["shares"], 50000.0)
        self.assertEqual(parsed["price"], 311.02)
        self.assertEqual(parsed["value"], 15551000.0)
        self.assertEqual(parsed["ownershipForm"], "D")
        self.assertEqual(parsed["transactionDate"], "2026-05-27")

    def test_china_exposure_evidence_has_excerpt_availability(self):
        async def fake_index(**kwargs):
            return json.dumps({
                "filingType": "10-K",
                "index": {
                    "sections": [{"heading": "China supply chain and manufacturing risks"}],
                    "tables": [{"title": "Bank of China credit facility", "rowLabels": ["Bank of China"], "tableId": "t1"}],
                },
            })

        async def fake_revenue(**kwargs):
            return json.dumps({"status": "NOT_FOUND", "matches": []})

        async def fake_risk(**kwargs):
            return json.dumps({"status": "FOUND", "matches": [{"term": "China", "excerpt": ""}]})

        old_index = srv.get_sec_filing_index
        old_revenue = srv.extract_revenue_exposure
        old_risk = srv.extract_risk_factor_mentions
        try:
            srv.get_sec_filing_index = fake_index
            srv.extract_revenue_exposure = fake_revenue
            srv.extract_risk_factor_mentions = fake_risk
            data = json.loads(_run(srv.extract_china_exposure("MRVL")))
        finally:
            srv.get_sec_filing_index = old_index
            srv.extract_revenue_exposure = old_revenue
            srv.extract_risk_factor_mentions = old_risk
        entity_evidence = data["manufacturingExposure"]["evidence"][0]
        self.assertTrue(entity_evidence["excerptAvailable"])
        self.assertEqual(data["riskFactorExposure"]["status"], "NOT_FOUND")
        self.assertEqual(data["riskFactorExposure"]["evidence"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
