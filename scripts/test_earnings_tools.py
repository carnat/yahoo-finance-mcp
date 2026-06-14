#!/usr/bin/env python3
"""Phase 5 tests: earnings/report checking tools."""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import re
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _ensure_mcp_available() -> None:
    try:
        from mcp.server.fastmcp import FastMCP  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _FastMCPStub:
        def __init__(self, *a: object, **kw: object) -> None:
            pass

        def tool(self, *a: object, **kw: object):  # type: ignore[return]
            if a and callable(a[0]):
                return a[0]

            def _decorator(fn):  # type: ignore[return]
                return fn

            return _decorator

    mcp_mod = types.ModuleType("mcp")
    server_mod = types.ModuleType("mcp.server")
    fastmcp_mod = types.ModuleType("mcp.server.fastmcp")
    fastmcp_mod.FastMCP = _FastMCPStub  # type: ignore[attr-defined]
    mcp_mod.server = server_mod  # type: ignore[attr-defined]
    server_mod.fastmcp = fastmcp_mod  # type: ignore[attr-defined]
    sys.modules.setdefault("mcp", mcp_mod)
    sys.modules.setdefault("mcp.server", server_mod)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_mod)


_ensure_mcp_available()

from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402

if not getattr(_FastMCP, "_output_schema_patched", False):
    _orig_tool = _FastMCP.tool

    def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
        return _orig_tool(self, name=name, **kwargs)

    _FastMCP.tool = _patched_tool  # type: ignore[method-assign]
    _FastMCP._output_schema_patched = True  # type: ignore[attr-defined]

import server as srv  # noqa: E402


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


def _parse(raw: str) -> dict:
    parsed = json.loads(raw)
    # Unwrap Envelope V2 responses
    if isinstance(parsed, dict) and "ok" in parsed and "data" in parsed and "meta" in parsed:
        if parsed.get("ok") and isinstance(parsed.get("data"), dict):
            result = dict(parsed["data"])
            # Expose meta.warnings at top level for test assertions
            meta = parsed.get("meta") or {}
            if "warnings" not in result and meta.get("warnings"):
                result["warnings"] = meta["warnings"]
            elif "warnings" not in result:
                result["warnings"] = []
            return result
    return parsed


class TestGetLatestEarningsRelease(unittest.TestCase):
    def test_not_found_stable_shape(self):
        with patch("server._resolve_latest_earnings_release", new_callable=AsyncMock) as mocked:
            mocked.return_value = {
                "ticker": "AAPL",
                "eventType": "earnings_release",
                "period": "latest",
                "reportedAt": None,
                "sources": [],
                "confidence": "NOT_FOUND",
                "warnings": [],
            }
            data = _parse(_run(srv.get_latest_earnings_release("AAPL")))
            self.assertEqual(data.get("confidence"), "NOT_FOUND")
            self.assertEqual(data.get("sources"), [])
            self.assertIn("warnings", data)

    def test_found_has_url_and_timestamps(self):
        with patch("server._resolve_latest_earnings_release", new_callable=AsyncMock) as mocked:
            mocked.return_value = {
                "ticker": "AAPL",
                "eventType": "earnings_release",
                "period": "FY2026 Q2",
                "reportedAt": "2026-05-01T20:05:00Z",
                "sources": [{
                    "sourceType": "sec_8k",
                    "url": "https://www.sec.gov/Archives/example.htm",
                    "filingDate": "2026-05-01",
                    "acceptedAt": "2026-05-01T20:05:00Z",
                    "accessionNumber": "0000000000-26-000001",
                    "confidence": "HIGH",
                }],
                "confidence": "HIGH",
                "warnings": [],
            }
            data = _parse(_run(srv.get_latest_earnings_release("AAPL")))
            src = data["sources"][0]
            self.assertTrue(str(src.get("url", "")).startswith("https://"))
            self.assertIsNotNone(src.get("filingDate"))
            self.assertIsNotNone(src.get("acceptedAt"))


class TestIndexEarningsRelease(unittest.TestCase):
    def test_index_shape_and_unknown_units(self):
        fake_html = """
        <h2>Financial Highlights</h2>
        <table><tr><th>Three Months Ended</th></tr><tr><td>Net sales</td><td>10,000</td></tr></table>
        """
        with patch("server._resolve_latest_earnings_release", new_callable=AsyncMock) as mocked_release, patch(
            "server._edgar_get_html", new_callable=AsyncMock
        ) as mocked_html:
            mocked_release.return_value = {
                "ticker": "AAPL",
                "period": "FY2026 Q2",
                "sources": [{
                    "sourceType": "sec_8k",
                    "url": "https://www.sec.gov/Archives/example.htm",
                    "filingDate": "2026-05-01",
                    "acceptedAt": "2026-05-01T20:05:00Z",
                    "accessionNumber": "0000000000-26-000001",
                }],
            }
            mocked_html.return_value = fake_html
            data = _parse(_run(srv.index_earnings_release("AAPL")))
            self.assertIn("sections", data.get("index", {}))
            self.assertIn("tables", data.get("index", {}))
            self.assertIn("keywordMap", data.get("index", {}))
            tables = data.get("index", {}).get("tables", [])
            if tables:
                self.assertEqual(tables[0].get("unitScale"), "unknown")
            self.assertNotIn("rawHtml", json.dumps(data))


class TestExtractEarningsMetrics(unittest.TestCase):
    def test_stable_keys_and_evidence(self):
        html = "Revenue was $123.0 billion. Diluted earnings per share $2.31. Gross margin 47.8%."
        with patch("server._resolve_latest_earnings_release", new_callable=AsyncMock) as mocked_release, patch(
            "server._edgar_get_html", new_callable=AsyncMock
        ) as mocked_html:
            mocked_release.return_value = {
                "ticker": "AAPL",
                "period": "FY2026 Q2",
                "reportedAt": "2026-05-01T20:05:00Z",
                "sources": [{"sourceType": "sec_8k", "url": "https://www.sec.gov/Archives/example.htm", "filingDate": "2026-05-01"}],
                "confidence": "HIGH",
            }
            mocked_html.return_value = html
            data = _parse(_run(srv.extract_earnings_metrics("AAPL")))
            metrics = data.get("metrics", {})
            for key in ("revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex"):
                self.assertIn(key, metrics)
            self.assertEqual(metrics["freeCashFlow"].get("confidence"), "NOT_DISCLOSED")
            self.assertIsNone(metrics["freeCashFlow"].get("value"))
            self.assertEqual(metrics["revenue"].get("confidence"), "HIGH")
            self.assertIsNotNone(metrics["revenue"].get("evidence"))


class TestExtractGuidance(unittest.TestCase):
    def test_not_disclosed_when_absent(self):
        with patch("server._resolve_latest_earnings_release", new_callable=AsyncMock) as mocked_release, patch(
            "server._edgar_get_html", new_callable=AsyncMock
        ) as mocked_html:
            mocked_release.return_value = {
                "ticker": "AAPL",
                "period": "FY2026 Q2",
                "sources": [{"sourceType": "sec_8k", "url": "https://www.sec.gov/Archives/example.htm", "filingDate": "2026-05-01"}],
            }
            mocked_html.return_value = "No guidance provided in this release."
            data = _parse(_run(srv.extract_guidance("AAPL")))
            self.assertEqual(data.get("confidence"), "NOT_DISCLOSED")
            self.assertEqual(data["guidance"]["revenue"]["status"], "NOT_DISCLOSED")

    def test_does_not_infer_from_estimates(self):
        with patch("server._resolve_latest_earnings_release", new_callable=AsyncMock) as mocked_release:
            mocked_release.return_value = {
                "ticker": "AAPL",
                "period": "latest",
                "sources": [{"sourceType": "yahoo_estimate", "url": "https://finance.yahoo.com/quote/AAPL/analysis"}],
            }
            data = _parse(_run(srv.extract_guidance("AAPL")))
            self.assertEqual(data["guidance"]["eps"]["status"], "NOT_DISCLOSED")
            self.assertEqual(data.get("confidence"), "NOT_DISCLOSED")


class TestExtractManagementCommentary(unittest.TestCase):
    def test_topic_summary_has_evidence_and_short_excerpt(self):
        html = "<p>Management said demand in China improved sequentially.</p><p>AI features expanded in iPhone.</p>"
        with patch("server._resolve_latest_earnings_release", new_callable=AsyncMock) as mocked_release, patch(
            "server._edgar_get_html", new_callable=AsyncMock
        ) as mocked_html:
            mocked_release.return_value = {
                "ticker": "AAPL",
                "period": "FY2026 Q2",
                "sources": [{"sourceType": "sec_8k", "url": "https://www.sec.gov/Archives/example.htm", "filingDate": "2026-05-01"}],
            }
            mocked_html.return_value = html
            data = _parse(_run(srv.extract_management_commentary("AAPL", topics=["China", "AI"])))
            found = [t for t in data.get("topics", []) if t.get("status") == "FOUND"]
            self.assertTrue(found)
            for topic in found:
                ev = topic.get("evidence", [])
                self.assertTrue(ev)
                self.assertLessEqual(len(str(ev[0].get("excerpt", ""))), 240)


class TestCompareActualVsEstimate(unittest.TestCase):
    def test_surprise_calc_and_source(self):
        metrics_payload = {
            "period": "FY2026 Q2",
            "metrics": {"revenue": {"value": 123_000_000_000}, "epsDiluted": {"value": 2.31}},
        }
        ea_payload = {
            "revenueEstimate": [{"period": "FY2026 Q2", "avg": 121_000_000_000}],
            "earningsHistory": [
                {"quarter": "2026-09-30", "epsActual": None, "epsEstimate": 2.40},
                {"period": "FY2026 Q2", "quarter": "2026-06-30", "epsActual": 2.31, "epsEstimate": 2.25},
            ],
        }
        with patch("server.extract_earnings_metrics", new_callable=AsyncMock) as mocked_metrics, patch(
            "server.get_earnings_analysis", new_callable=AsyncMock
        ) as mocked_ea:
            mocked_metrics.return_value = json.dumps(metrics_payload)
            mocked_ea.return_value = json.dumps(ea_payload)
            data = _parse(_run(srv.compare_earnings_actual_vs_estimate("AAPL")))
            self.assertEqual(data["reportedPeriod"], "FY2026 Q2")
            self.assertEqual(data["reportedDate"], "2026-06-30")
            self.assertEqual(data["estimate"]["revenue"]["source"], "yahoo")
            self.assertEqual(data["estimate"]["eps"]["source"], "yahoo")
            self.assertAlmostEqual(data["surprise"]["revenueSurprisePct"], 1.65, places=2)
            self.assertAlmostEqual(data["surprise"]["epsSurprisePct"], 2.67, places=2)

    def test_no_reported_quarter_warning(self):
        with patch("server.extract_earnings_metrics", new_callable=AsyncMock) as mocked_metrics, patch(
            "server.get_earnings_analysis", new_callable=AsyncMock
        ) as mocked_ea:
            mocked_metrics.return_value = json.dumps({"period": None, "metrics": {"revenue": {"value": 100.0}, "epsDiluted": {"value": None}}})
            mocked_ea.return_value = json.dumps({"revenueEstimate": [{"period": "0q", "avg": 90.0}], "earningsHistory": [{"epsActual": None, "epsEstimate": 0.9}]})
            data = _parse(_run(srv.compare_earnings_actual_vs_estimate("AAPL")))
            codes = [w.get("code") for w in data.get("warnings", []) if isinstance(w, dict)]
            self.assertIn("NO_REPORTED_QUARTER", codes)
            self.assertEqual(data.get("confidence"), "NOT_DISCLOSED")


class TestPublicWording(unittest.TestCase):
    def test_no_private_terms_in_new_tool_descriptions(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        server_text = open(os.path.join(root, "server.py"), encoding="utf-8").read()
        tools_text = open(os.path.join(root, "worker", "src", "tools.ts"), encoding="utf-8").read()
        private_terms = ("Commander", "portfolio state", "doctrine", "DC-", "TPS", "PCCE")
        names = [
            "get_latest_earnings_release",
            "index_earnings_release",
            "extract_earnings_metrics",
            "extract_guidance",
            "extract_management_commentary",
            "compare_earnings_actual_vs_estimate",
        ]
        def _window(text: str, marker: str, span: int = 400) -> str:
            idx = text.find(marker)
            self.assertGreaterEqual(idx, 0)
            return text[max(0, idx - span): idx + span]
        for name in names:
            s_win = _window(server_text, f'name="{name}"')
            t_win = _window(tools_text, f'name: "{name}"')
            for term in private_terms:
                self.assertNotRegex(s_win, re.escape(term))
                self.assertNotRegex(t_win, re.escape(term))


class TestSemanticQualityFixes(unittest.TestCase):
    def test_get_earnings_momentum_mixed_signal_metadata(self):
        class _FastInfo:
            currency = "USD"

        class _Ticker:
            def __init__(self):
                self.fast_info = _FastInfo()
                self.eps_trend = srv.pd.DataFrame(
                    [{"current": 1.0, "7daysAgo": 1.0, "30daysAgo": 1.2, "90daysAgo": 1.4}],
                    index=["0q"],
                )
                self.earnings_history = srv.pd.DataFrame(
                    [
                        {"epsActual": 1.1, "epsEstimate": 1.0, "surprisePercent": 0.10},
                        {"epsActual": 1.0, "epsEstimate": 0.9, "surprisePercent": 0.11},
                        {"epsActual": 0.9, "epsEstimate": 0.8, "surprisePercent": 0.12},
                        {"epsActual": 0.8, "epsEstimate": 0.7, "surprisePercent": 0.13},
                    ]
                )

        with patch("server.yf.Ticker", return_value=_Ticker()):
            data = _parse(_run(srv.get_earnings_momentum("VRT")))
        self.assertEqual(data.get("historicalSurpriseSignal"), "STRONG")
        self.assertEqual(data.get("forwardRevisionSignal"), "NEGATIVE")
        self.assertEqual(data.get("compositeMomentumSignal"), "MIXED_NEGATIVE_REVISION")
        self.assertEqual(data.get("beatSample"), 4)
        warning_codes = [w.get("code") for w in data.get("warnings", []) if isinstance(w, dict)]
        self.assertIn("MIXED_EARNINGS_SIGNAL", warning_codes)

    def test_get_credit_health_partial_data_metadata(self):
        class _Ticker:
            def __init__(self):
                col = srv.pd.Timestamp("2026-03-31")
                self.quarterly_balance_sheet = srv.pd.DataFrame(
                    {col: {"Total Debt": 510.0, "Cash And Cash Equivalents": 10.0}}
                )
                self.quarterly_income_stmt = srv.pd.DataFrame(
                    {col: {"EBITDA": 250.0, "EBIT": 200.0}}
                )

        with patch("server.yf.Ticker", return_value=_Ticker()):
            data = _parse(_run(srv.get_credit_health("VRT")))
        self.assertEqual(data.get("dataQuality"), "PARTIAL")
        self.assertIn("interestExpenseUsd", data.get("missingComponents", []))
        self.assertIn("interestCoverage", data.get("unavailableMetrics", []))
        self.assertIn("netDebtToEbitda", data.get("computedMetrics", []))
        warning_codes = [w.get("code") for w in data.get("warnings", []) if isinstance(w, dict)]
        self.assertIn("INTEREST_EXPENSE_UNAVAILABLE", warning_codes)

    def test_get_analyst_consensus_target_lag_metadata(self):
        class _FastInfo:
            last_price = 370.94

        class _Ticker:
            def __init__(self):
                now = datetime.datetime.now(datetime.UTC)
                self.fast_info = _FastInfo()
                self.analyst_price_targets = {
                    "current": 355.52,
                    "low": 320.0,
                    "high": 410.0,
                    "mean": 355.52,
                    "median": 360.0,
                }
                self.recommendations_summary = srv.pd.DataFrame(
                    [{"strongBuy": 4, "buy": 7, "hold": 2, "sell": 0, "strongSell": 0}],
                    index=["0m"],
                )
                self.upgrades_downgrades = srv.pd.DataFrame(
                    [{"Action": "up", "ToGrade": "Buy", "FromGrade": "Hold"}],
                    index=[now - datetime.timedelta(days=3)],
                )

        with patch("server.yf.Ticker", return_value=_Ticker()):
            data = _parse(_run(srv.get_analyst_consensus("VRT")))
        self.assertEqual(data.get("targetLagSignal"), "LIKELY_STALE_OR_LAGGING")
        self.assertEqual(data.get("recentUpgradeCount30d"), 1)
        self.assertEqual(data.get("priceTargets", {}).get("mean"), 355.52)
        self.assertAlmostEqual(data.get("priceTargets", {}).get("pctUpsideFromLastPrice"), -4.16, places=2)
        warning_codes = [w.get("code") for w in data.get("warnings", []) if isinstance(w, dict)]
        self.assertIn("CONSENSUS_TARGET_BELOW_PRICE_DESPITE_UPGRADES", warning_codes)


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    classes = [
        TestGetLatestEarningsRelease,
        TestIndexEarningsRelease,
        TestExtractEarningsMetrics,
        TestExtractGuidance,
        TestExtractManagementCommentary,
        TestCompareActualVsEstimate,
        TestPublicWording,
        TestSemanticQualityFixes,
    ]
    for cls in classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
