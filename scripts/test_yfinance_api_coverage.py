from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
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


def _run(coro):
    return asyncio.run(coro)


class _FastInfo:
    currency = "USD"


class TestYfinanceApiCoverage(unittest.TestCase):
    def setUp(self):
        self.ticker = srv.yf.Ticker
        self.had_calendars = hasattr(srv.yf, "Calendars")
        self.calendars = getattr(srv.yf, "Calendars", None)

    def tearDown(self):
        srv.yf.Ticker = self.ticker  # type: ignore[assignment]
        if self.had_calendars:
            srv.yf.Calendars = self.calendars  # type: ignore[attr-defined]
        else:
            delattr(srv.yf, "Calendars")

    def test_fund_profile_uses_supported_funds_data_sections(self):
        class Funds:
            description = "Index fund"
            fund_overview = {"categoryName": "Large Blend"}
            top_holdings = pd.DataFrame({"Name": ["Alpha"], "Holding Percent": [0.1]}, index=["AAA"])
            equity_holdings = pd.DataFrame({"Fund": [20.0]}, index=["Price/Earnings"])
            asset_classes = {"stockPosition": 0.95, "cashPosition": 0.05}
            sector_weightings = {"technology": 0.3}
            fund_operations = pd.DataFrame({"Fund": [0.001]}, index=["Annual Report Expense Ratio"])
            bond_holdings = pd.DataFrame({"Fund": [5.0]}, index=["Duration"])
            bond_ratings = {"aaa": 0.8}

        class FakeTicker:
            info = {"shortName": "Test Fund"}
            funds_data = Funds()

            @property
            def funds_top_holdings(self):
                raise AssertionError("unsupported legacy property used")

            @property
            def funds_sector_weightings(self):
                raise AssertionError("unsupported legacy property used")

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_fund_profile("TSTFUND", ["overview", "holdings", "allocation", "operations", "fixed_income"])))
        self.assertEqual(data["description"], "Index fund")
        self.assertEqual(data["topHoldings"][0]["index"], "AAA")
        self.assertEqual(data["assetClasses"]["stockPosition"], 0.95)
        self.assertEqual(data["bondRatings"]["aaa"], 0.8)
        self.assertTrue(all(value == "OK" for value in data["sectionStatus"].values()))
        self.assertFalse(data["decisionGrade"])

    def test_company_calendar_never_infers_official_confirmation(self):
        class FakeTicker:
            fast_info = _FastInfo()
            calendar = {"Earnings Date": [pd.Timestamp("2026-08-01")]}

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_company_events_calendar("TSTCAL")))
        self.assertEqual(data["earningsDateSource"], "YAHOO_CALENDAR")
        self.assertEqual(data["confirmationStatus"], "UNVERIFIED")
        self.assertFalse(data["earningsDateConfirmed"])
        self.assertFalse(data["decisionGrade"])

    def test_fund_profile_preserves_other_sections_on_partial_failure(self):
        class Funds:
            description = "Index fund"
            fund_overview = {"categoryName": "Blend"}

            @property
            def top_holdings(self):
                raise RuntimeError("holdings unavailable")

            equity_holdings = None

        class FakeTicker:
            info = {"shortName": "Partial Fund"}
            funds_data = Funds()

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_fund_profile("TSTPART", ["overview", "holdings"])))
        self.assertEqual(data["sectionStatus"]["overview"], "OK")
        self.assertEqual(data["sectionStatus"]["holdings"], "PROVIDER_ERROR")
        self.assertEqual(data["description"], "Index fund")

    def test_company_calendar_history_is_paginated(self):
        class FakeTicker:
            fast_info = _FastInfo()

            def get_earnings_dates(self, limit, offset):
                self.called = (limit, offset)
                return pd.DataFrame(
                    {"EPS Estimate": [1.0], "Reported EPS": [1.1], "Surprise(%)": [10.0]},
                    index=pd.DatetimeIndex(["2026-07-01"], name="Earnings Date"),
                )

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_company_events_calendar("TSTHIST", "history", 5, 2)))
        self.assertEqual(data["mode"], "history")
        self.assertEqual(data["limit"], 5)
        self.assertEqual(data["offset"], 2)
        self.assertEqual(len(data["items"]), 1)
        self.assertEqual(data["providerMethod"], "YAHOO_CALENDAR_HTML")
        self.assertEqual(data["status"], "OK")

    def test_earnings_analysis_exposes_revision_counts(self):
        revisions = pd.DataFrame(
            {"upLast7days": [2], "upLast30days": [3], "downLast7days": [0], "downLast30days": [1]},
            index=["0q"],
        )

        class FakeTicker:
            fast_info = _FastInfo()
            earnings_estimate = None
            revenue_estimate = None
            eps_trend = None
            eps_revisions = revisions
            earnings_history = None
            growth_estimates = None

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_earnings_analysis("TSTEPREV")))
        self.assertEqual(data["epsRevisions"][0]["upLast30days"], 3)

    def test_share_count_trend_sorts_and_compacts(self):
        class FakeTicker:
            def get_shares_full(self, start, end):
                return pd.Series(
                    [110.0, 100.0, 105.0, 110.0],
                    index=pd.DatetimeIndex(["2026-06-01", "2026-01-01", "2026-03-01", "2026-03-01"]),
                )

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.analyze_share_count_trend("TSTDIL", "2026-01-01", "2026-07-01")))
        self.assertEqual(data["firstShares"], 100.0)
        self.assertEqual(data["currentShares"], 110.0)
        self.assertEqual(data["changePct"], 10.0)
        self.assertEqual(data["sampleCount"], 2)
        self.assertEqual(data["recommendedNextAction"], "CHECK_SEC_FILINGS")

    def test_financial_ratios_can_add_bounded_valuation_history(self):
        class FakeTicker:
            info = {"financialCurrency": "USD", "marketCap": 100.0, "freeCashflow": 10.0}

            def get_valuation_measures(self, freq, periods):
                self.request = (freq, periods)
                return pd.DataFrame(
                    {"2026-06-30": [15.0, 4.0], "2026-03-31": [14.0, 3.5]},
                    index=["Trailing P/E", "Price/Sales"],
                )

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.analyze_financial_ratios("TSTVAL", 2, "quarterly")))
        self.assertEqual(data["valuationFrequency"], "quarterly")
        self.assertEqual(data["valuationHistory"][0]["trailingPE"], 15.0)
        self.assertEqual(data["valuationHistory"][1]["priceToSales"], 3.5)
        self.assertFalse(data["decisionGrade"])

    def test_market_calendar_uses_unfiltered_earnings_and_bounds(self):
        class FakeCalendars:
            def __init__(self, start, end):
                self.start = start
                self.end = end

            def get_earnings_calendar(self, **kwargs):
                self.kwargs = kwargs
                if kwargs.get("filter_most_active") is not False:
                    raise AssertionError("hidden most-active filter must be disabled")
                return pd.DataFrame({"Ticker": ["AAA"], "Event Start Date": ["2026-07-20"]})

        srv.yf.Calendars = FakeCalendars  # type: ignore[attr-defined]
        data = json.loads(_run(srv.get_market_calendar("earnings", "2026-07-20", "2026-07-21", 25, 0)))
        self.assertEqual(data["status"], "OK")
        self.assertEqual(data["items"][0]["Ticker"], "AAA")
        self.assertEqual(data["confirmationStatus"], "UNVERIFIED")

    def test_worker_contains_parity_guards(self):
        root = Path(__file__).resolve().parents[1]
        worker = (root / "worker" / "src" / "yahoo-finance.ts").read_text(encoding="utf-8")
        self.assertIn("events=div%2Csplit%2CcapitalGains", worker)
        self.assertIn("ttm_income_stmt", worker)
        self.assertIn('lineItem: humanize(baseType)', worker)
        self.assertIn("epsRevisions", worker)
        self.assertIn('earningsDateSource: earningsDates.filter(Boolean).length ? "YAHOO_CALENDAR"', worker)
        self.assertIn("export async function analyzeShareCountTrend", worker)
        self.assertIn("export async function getMarketCalendar", worker)
        self.assertIn('operator: "GTELT"', worker)
        self.assertIn("YAHOO_CALENDAR_HTML", worker)
        self.assertIn("https://finance.yahoo.com/calendar/earnings", worker)
        self.assertNotIn('entityIdType: "earnings"', worker)

    def test_llm_visible_descriptions_route_common_intents(self):
        tools = {tool.name: tool for tool in _run(srv.yfinance_server.list_tools())}
        self.assertIn("dilution", (tools["analyze_share_count_trend"].description or "").lower())
        self.assertIn("market-wide", (tools["get_market_calendar"].description or "").lower())
        self.assertIn("unverified", (tools["get_company_events_calendar"].description or "").lower())


if __name__ == "__main__":
    unittest.main()
