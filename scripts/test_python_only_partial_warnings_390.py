#!/usr/bin/env python3
"""Python-only partial-result warnings (3.9.0, swallowed-error leftovers item 4, plus the Python-only branches of items 2 and 3).

Python makes several upstream reads for these tools and fills null when one fails; the Worker makes one upstream call, so a
failure there fails the whole tool. The difference is intentional (3.9.0), so there is nothing to compare with the Worker.
A failed read adds COMPONENT_FAILED "<part> could not be read; retry." (severity "warning"); an empty read adds nothing.
Run: python scripts/test_python_only_partial_warnings_390.py -v
"""

from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import server  # noqa: E402
from yfmcp import pricing_notices  # noqa: E402
from yfmcp.clients import edgar  # noqa: E402
from yfmcp.tools import fundamentals, pricing  # noqa: E402
from yfmcp.tools import sec as sec_tools  # noqa: E402


def _failed(part: str) -> dict:
    return {"code": "COMPONENT_FAILED", "message": f"{part} could not be read; retry.", "severity": "warning"}


def _raises(*_a, **_k):
    raise RuntimeError("upstream read failed")


class _Raising:
    """A yfinance attribute that raises when read."""

    def __init__(self, name: str):
        self.name = name


def _ticker(**attrs):
    """A yfinance Ticker stand-in: an attribute given as a _Raising raises, any other value is returned."""
    class T:
        fast_info = SimpleNamespace(last_price=100.0, currency="USD")

        def __getattr__(self, key):
            value = attrs.get(key)
            if isinstance(value, _Raising):
                raise RuntimeError(f"{key} failed")
            return value

    return T()


class TestPartialWarnings(unittest.TestCase):
    def setUp(self):
        server._tool_cache._store.clear()

    def test_analyst_consensus_names_each_failed_part(self):
        t = _ticker(analyst_price_targets=_Raising("p"), upgrades_downgrades=_Raising("u"), recommendations_summary=_Raising("r"),
                    info=_Raising("i"))
        with patch.object(server.yf, "Ticker", lambda _s: t):
            out = json.loads(asyncio.run(server.get_analyst_consensus("PWT")))
        self.assertEqual([w for w in out["warnings"] if w["code"] == "COMPONENT_FAILED"],
                         [_failed("Price targets"), _failed("Upgrades and downgrades"), _failed("Recommendation summary")])
        self.assertIsNone(out["priceTargets"])

    def test_analyst_consensus_analyst_count_failure_and_empty_reads(self):
        t = _ticker(analyst_price_targets={"low": 1, "high": 3, "mean": 2, "median": 2}, upgrades_downgrades=None,
                    recommendations_summary=None, info=_Raising("i"))
        with patch.object(server.yf, "Ticker", lambda _s: t):
            out = json.loads(asyncio.run(server.get_analyst_consensus("PWC")))
        self.assertEqual([w for w in out["warnings"] if w["code"] == "COMPONENT_FAILED"], [_failed("Price target analyst count")])
        server._tool_cache._store.clear()
        t = _ticker(analyst_price_targets=None, upgrades_downgrades=None, recommendations_summary=None, info={})
        with patch.object(server.yf, "Ticker", lambda _s: t):
            out = json.loads(asyncio.run(server.get_analyst_consensus("PWE")))
        self.assertEqual(out["warnings"], [])

    def test_earnings_momentum_names_the_failed_inputs(self):
        t = _ticker(eps_trend=_Raising("e"), earnings_history=_Raising("h"))
        with patch.object(server.yf, "Ticker", lambda _s: t):
            out = json.loads(asyncio.run(server.get_earnings_momentum("PWM")))
        self.assertEqual([w for w in out["warnings"] if w["code"] == "COMPONENT_FAILED"], [_failed("EPS trend"), _failed("Earnings history")])
        server._tool_cache._store.clear()
        t = _ticker(eps_trend=None, earnings_history=None)
        with patch.object(server.yf, "Ticker", lambda _s: t):
            out = json.loads(asyncio.run(server.get_earnings_momentum("PWN")))
        self.assertEqual([w for w in out["warnings"] if w["code"] == "COMPONENT_FAILED"], [])

    def test_issuer_names_failure_is_component_failed(self):
        quiet = AsyncMock(return_value=json.dumps({"ok": True, "data": {"items": [], "changes": []}, "meta": {}}))
        with patch.object(fundamentals, "get_company_news", quiet), patch.object(fundamentals, "get_analyst_upgrade_radar", quiet):
            with patch.object(server.yf, "Ticker", lambda _s: _ticker(info=_Raising("i"))):
                out = json.loads(asyncio.run(server.extract_analyst_valuation_methods("PWI")))
            codes = [w["code"] for w in out["warnings"]]
            self.assertIn(_failed("Issuer names")["message"], [w["message"] for w in out["warnings"]])
            self.assertEqual(codes[:2], ["COMPONENT_FAILED", "ATTRIBUTION_UNCHECKED"])
            with patch.object(server.yf, "Ticker", lambda _s: _ticker(info={"longName": "Acme Corp"})):
                out = json.loads(asyncio.run(server.extract_analyst_valuation_methods("PWJ")))
            self.assertNotIn("COMPONENT_FAILED", [w["code"] for w in out["warnings"]])

    def test_history_metadata_failure_is_a_notice(self):
        hist = pd.DataFrame({"Close": [1.0], "Adj Close": [1.0], "Volume": [10]}, index=pd.DatetimeIndex(["2026-10-07"], tz="UTC"))

        class C:
            def history(self, **_k):
                return hist

            def get_history_metadata(self):
                raise RuntimeError("metadata failed")

        prepared = asyncio.run(pricing._load_completed_daily_history(C(), "1y"))
        self.assertTrue(prepared["metadataFailed"])
        self.assertEqual(pricing_notices.freshness_notices("PWH", prepared.get("freshnessReason"), prepared["metadataFailed"]),
                         [_failed("Price history metadata")] if prepared.get("freshnessReason") != pricing_notices.SESSION_CALENDAR_UNKNOWN
                         else [pricing_notices.session_calendar_notice("PWH"), _failed("Price history metadata")])
        C.get_history_metadata = lambda self: {"exchangeTimezoneName": "UTC"}
        self.assertFalse(asyncio.run(pricing._load_completed_daily_history(C(), "1y"))["metadataFailed"])
        self.assertEqual(pricing_notices.freshness_notices("PWH", None), [])


class TestOptionsFlowMaxPain(unittest.TestCase):
    def test_max_pain_computation_failure_warns(self):
        expiry = "2099-01-15"

        def contract(strike, kind):
            return {"strike": strike, "lastPrice": 2.0, "bid": 1.9, "ask": 2.1, "volume": 10, "openInterest": 1000,
                    "impliedVolatility": 0.4, "inTheMoney": False}

        class T:
            fast_info = {"lastPrice": 100.0, "tenDayAverageVolume": 1_000_000}
            options = (expiry,)

            def option_chain(self, _e):
                return SimpleNamespace(calls=pd.DataFrame([contract(s, "C") for s in (95.0, 100.0)]),
                                       puts=pd.DataFrame([contract(s, "P") for s in (95.0, 100.0)]))

        def scan():
            with patch.object(server.yf, "Ticker", lambda _t: T()), \
                    patch.object(pricing, "_load_completed_daily_history", AsyncMock(side_effect=RuntimeError("no history"))), \
                    patch.object(pricing, "get_last_trading_date", lambda: "2026-10-08"):
                return json.loads(asyncio.run(server.get_options_flow_scan("PWX", "audit")))

        ok = scan()
        self.assertNotIn("MAX_PAIN_UNAVAILABLE", ok["warnings"])
        self.assertIsNotNone(ok["maxPainStrike"])
        with patch.object(pd.Series, "clip", _raises):
            bad = scan()
        self.assertIsNone(bad["maxPainStrike"])
        self.assertEqual(bad["warnings"].count("MAX_PAIN_UNAVAILABLE"), 1)
        # Appended in the order the failure points are reached: max pain before the daily-bars and 10-day volume reads.
        self.assertLess(bad["warnings"].index("MAX_PAIN_UNAVAILABLE"), bad["warnings"].index("IV_PERCENTILE_UNAVAILABLE_DAILY_BARS"))


class TestAnnualCoverageSubmissions(unittest.TestCase):
    PAYLOAD = {"value": None}
    EXPECTED = {"code": "SEC_READ_FAILED", "severity": "info",
                "message": "SEC data could not be read, so the latest-annual coverage check did not run; retry.",
                "failedReads": [{"endpoint": "submissions", "concept": None, "httpStatus": None}]}

    def test_submissions_exception_and_failed_lookup_warn(self):
        with patch.object(sec_tools, "_get_submissions_for_ticker", AsyncMock(side_effect=RuntimeError("down"))):
            self.assertEqual(asyncio.run(sec_tools._annual_coverage_warning_for("PWS", self.PAYLOAD)), self.EXPECTED)
        with patch.object(sec_tools, "_get_submissions_for_ticker", AsyncMock(return_value=(None, None))), \
                patch.dict(edgar._EDGAR_LOOKUP_FAILURE, {"PWS": "SUBMISSIONS"}):
            self.assertEqual(asyncio.run(sec_tools._annual_coverage_warning_for("PWS", self.PAYLOAD)), self.EXPECTED)

    def test_absent_submissions_are_silent(self):
        with patch.object(sec_tools, "_get_submissions_for_ticker", AsyncMock(return_value=(None, None))), \
                patch.dict(edgar._EDGAR_LOOKUP_FAILURE, {}, clear=True):
            self.assertIsNone(asyncio.run(sec_tools._annual_coverage_warning_for("PWS", self.PAYLOAD)))


if __name__ == "__main__":
    unittest.main()
