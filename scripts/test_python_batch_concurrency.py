#!/usr/bin/env python3
"""Local batch tools run three tickers at a time, like the Worker.

Each per-ticker call is replaced by a stub that blocks for 0.3 s (as the
real tools block on yfinance). Five tickers take about 0.6 s when three run
at once and 1.5 s one at a time. Results must keep the request order and a
failing ticker must keep the per-ticker error shape.
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from yfmcp.tools import pricing  # noqa: E402

TICKERS = ["AAA", "BBB", "CCC", "DDD", "FAIL"]
BLOCK_SECONDS = 0.3

# (tool, extra positional args after ticker)
BATCH_TOOLS = [
    ("get_fast_info", ()),
    ("get_price_stats", ()),
    ("get_technical_indicators", ("3mo",)),
    ("get_price_slope", (5,)),
    ("get_volume_ratio", ("1mo",)),
    ("get_ma_position", ()),
    ("get_short_momentum", ()),
]


class _Probe:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.calls: list[tuple] = []

    def stub(self):
        async def per_ticker(ticker, *args):
            with self.lock:
                self.active += 1
                self.peak = max(self.peak, self.active)
                self.calls.append((ticker, *args))
            try:
                time.sleep(BLOCK_SECONDS)  # blocking, as yfinance calls are
                if ticker == "FAIL":
                    raise RuntimeError("provider exploded")
                return json.dumps({"ticker": ticker})
            finally:
                with self.lock:
                    self.active -= 1

        return per_ticker


class TestPythonBatchConcurrency(unittest.TestCase):
    def _run_batch(self, name: str, extra: tuple, tickers: list[str]):
        original = getattr(pricing, name)
        probe = _Probe()
        with patch.object(pricing, name, probe.stub()):
            started = time.monotonic()
            raw = asyncio.run(original(tickers, *extra))
            elapsed = time.monotonic() - started
        return json.loads(raw), probe, elapsed

    def test_batch_tools_run_three_tickers_at_a_time(self) -> None:
        for name, extra in BATCH_TOOLS:
            with self.subTest(tool=name):
                result, probe, elapsed = self._run_batch(name, extra, TICKERS)
                self.assertEqual(probe.peak, 3)
                self.assertLess(elapsed, 1.2)
                self.assertEqual(list(result), TICKERS)
                self.assertEqual(result["AAA"], {"ticker": "AAA"})
                self.assertEqual(result["FAIL"], {"error": True, "message": "provider exploded", "ticker": "FAIL"})
                self.assertEqual(sorted(call[1:] for call in probe.calls), [extra] * len(TICKERS))

    def test_market_snapshot_batch(self) -> None:
        result, probe, elapsed = self._run_batch("get_market_snapshot", ("compact", False), TICKERS + ["EXTRA"])
        self.assertEqual(probe.peak, 3)
        self.assertLess(elapsed, 1.2)
        self.assertEqual(list(result["tickers"]), TICKERS)
        self.assertEqual(result["tickers"]["FAIL"], {"error": True, "message": "provider exploded"})
        self.assertEqual(result["droppedTickers"], ["EXTRA"])
        self.assertTrue(result["truncated"])


# Batch tools defined in server.py (tool, extra positional args after ticker).
SERVER_BATCH_TOOLS = [
    ("get_credit_health", ()),
    ("get_earnings_momentum", ()),
    ("get_analyst_upgrade_radar", (30,)),
    ("get_etf_info", (None,)),
    ("get_stock_info", (None, False)),
    ("get_analyst_consensus", ()),
    ("get_financial_ratios", (0, "quarterly")),
]


class TestServerBatchConcurrency(unittest.TestCase):
    def test_server_batch_tools_run_three_tickers_at_a_time(self) -> None:
        import server

        for name, extra in SERVER_BATCH_TOOLS:
            with self.subTest(tool=name):
                original = getattr(server, name)
                probe = _Probe()
                with patch.object(server, name, probe.stub()):
                    started = time.monotonic()
                    result = json.loads(asyncio.run(original(TICKERS, *extra)))
                    elapsed = time.monotonic() - started
                self.assertEqual(probe.peak, 3)
                self.assertLess(elapsed, 1.2)
                self.assertEqual(list(result), TICKERS)
                self.assertEqual(result["FAIL"], {"error": True, "message": "provider exploded", "ticker": "FAIL"})


if __name__ == "__main__":
    unittest.main()
