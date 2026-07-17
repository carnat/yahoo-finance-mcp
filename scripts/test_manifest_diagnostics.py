#!/usr/bin/env python3
"""Phase 8 tests: manifest diagnostics, get_market_snapshot, freshness classifier, wording smoke.

These are offline/unit tests — no live network calls required.
Run: PYTHONPATH=. python scripts/test_phase8.py
"""

import asyncio
import json
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["MCP_ENVELOPE_V2"] = "true"

# Patch FastMCP.tool to accept output_schema (not in mcp>=1.9)
from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402
_orig_tool = _FastMCP.tool
def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
    return _orig_tool(self, name=name, **kwargs)
_FastMCP.tool = _patched_tool  # type: ignore[method-assign]


def _reload_server():
    import importlib
    import sys
    # Trigger the first import so all submodules are in sys.modules.
    import server as srv  # noqa: F401 (may be first import)
    # Reload in dependency order: app → domain modules → server,
    # so @yfinance_server.tool decorators always re-register on a fresh instance.
    for _mod in ("yfmcp.app", "yfmcp.tools.system", "yfmcp.tools.pricing"):
        if _mod in sys.modules:
            importlib.reload(sys.modules[_mod])
    importlib.reload(srv)
    return srv


def _run(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# ---------------------------------------------------------------------------
# 1. Manifest diagnostics smoke
# ---------------------------------------------------------------------------

class TestManifestDiagnostics(unittest.TestCase):
    def setUp(self):
        self.srv = _reload_server()

    def test_get_manifest_diagnostics_shape(self):
        result = json.loads(_run(self.srv.get_manifest_diagnostics()))
        expected = {
            "status", "serverVersion", "toolCount", "manifestVersion", "manifestHash",
            "schemaHash", "runtimeHash", "toolMode", "envelopeSchemaVersion",
            "generatedAt", "privacyScope",
        }
        self.assertEqual(set(result), expected)

    def test_privacy_scope_value(self):
        result = json.loads(_run(self.srv.get_manifest_diagnostics()))
        self.assertEqual(result["privacyScope"], "public_market_data_only")

    def test_tool_count_positive(self):
        result = json.loads(_run(self.srv.get_manifest_diagnostics()))
        self.assertIsInstance(result["toolCount"], int)
        self.assertGreater(result["toolCount"], 0)

    def test_schema_hash_is_distinct_manifest_identity(self):
        result = json.loads(_run(self.srv.get_manifest_diagnostics()))
        self.assertIsInstance(result["schemaHash"], str)
        self.assertGreater(len(result["schemaHash"]), 0)
        self.assertNotEqual(result["schemaHash"], result["manifestHash"])

    def test_manifest_hash_is_string(self):
        result = json.loads(_run(self.srv.get_manifest_diagnostics()))
        self.assertIsInstance(result["manifestHash"], str)
        self.assertGreater(len(result["manifestHash"]), 0)

    def test_public_diagnostics_omit_operational_details(self):
        result = json.loads(_run(self.srv.get_manifest_diagnostics()))
        forbidden = {
            "buildSha", "deployedAt", "hiddenAliases", "batchContracts",
            "responseFieldContract", "doctrineToolStatus", "structuredFactProvider",
        }
        self.assertTrue(forbidden.isdisjoint(result))


# ---------------------------------------------------------------------------
# 2. Freshness classifier unit tests
# ---------------------------------------------------------------------------

class TestFreshnessClassifier(unittest.TestCase):
    def setUp(self):
        self.srv = _reload_server()

    def _classify(self, data_date, retrieved_at):
        return self.srv._classify_freshness(data_date, retrieved_at)

    def test_none_data_date_returns_unknown(self):
        self.assertEqual(self._classify(None, "2026-05-17T10:00:00Z"), "UNKNOWN")

    def test_fresh_same_day(self):
        # Data from 16:05 UTC close, retrieved 18:00 UTC same day
        result = self._classify("2026-05-15", "2026-05-15T22:00:00Z")
        self.assertEqual(result, "FRESH")

    def test_weekend_expected_stale_sunday_friday_data(self):
        # Sunday 2026-05-17 (Python weekday=6), data from Friday 2026-05-15 (Python weekday=4)
        result = self._classify("2026-05-15", "2026-05-17T12:00:00Z")
        self.assertEqual(result, "WEEKEND_EXPECTED_STALE")

    def test_weekend_expected_stale_saturday_friday_data(self):
        # Saturday 2026-05-16 (Python weekday=5), data from Friday 2026-05-15 (Python weekday=4)
        result = self._classify("2026-05-15", "2026-05-16T12:00:00Z")
        self.assertEqual(result, "WEEKEND_EXPECTED_STALE")

    def test_market_closed_expected_stale_overnight(self):
        # Data from Friday close, retrieved Monday morning within 56h
        # Friday 2026-05-15 21:00 UTC approx close → Monday 2026-05-18 04:00 UTC = ~55h
        result = self._classify("2026-05-15", "2026-05-18T04:00:00Z")
        self.assertEqual(result, "MARKET_CLOSED_EXPECTED_STALE")

    def test_stale_3_5_days(self):
        # Data 4 days old (~96h)
        result = self._classify("2026-05-13", "2026-05-17T10:00:00Z")
        self.assertEqual(result, "STALE")

    def test_very_stale_over_7_days(self):
        # Data 10 days old
        result = self._classify("2026-05-07", "2026-05-17T10:00:00Z")
        self.assertEqual(result, "VERY_STALE")

    def test_future_date_returns_unknown(self):
        result = self._classify("2026-05-20", "2026-05-17T10:00:00Z")
        self.assertEqual(result, "UNKNOWN")


# ---------------------------------------------------------------------------
# 3. Overnight guardrails unit tests
# ---------------------------------------------------------------------------

class TestOvernightSessionGuardrails(unittest.TestCase):
    def setUp(self):
        self.srv = _reload_server()

    def test_overnight_session_status_classifier(self):
        self.assertEqual(
            self.srv._classify_overnight_session(self.srv.pd.Timestamp("2026-06-08T07:00:00Z")),
            "ACTIVE",
        )
        self.assertEqual(
            self.srv._classify_overnight_session(self.srv.pd.Timestamp("2026-06-08T15:00:00Z")),
            "ENDED",
        )
        self.assertEqual(
            self.srv._classify_overnight_session(self.srv.pd.Timestamp("2026-06-08T23:00:00Z")),
            "NOT_STARTED",
        )

    def test_dst_window_shift_winter_vs_summer(self):
        summer_start, summer_end = self.srv._overnight_window_utc(
            self.srv.pd.Timestamp("2026-06-08T07:00:00Z")
        )
        winter_start, winter_end = self.srv._overnight_window_utc(
            self.srv.pd.Timestamp("2026-12-08T08:00:00Z")
        )

        self.assertEqual(str(summer_start), "2026-06-08 00:00:00+00:00")  # EDT => 00:00–08:00 UTC
        self.assertEqual(str(summer_end), "2026-06-08 08:00:00+00:00")
        self.assertEqual(str(winter_start), "2026-12-08 01:00:00+00:00")  # EST => 01:00–09:00 UTC
        self.assertEqual(str(winter_end), "2026-12-08 09:00:00+00:00")

    def test_get_overnight_quote_includes_session_fields(self):
        import unittest.mock as mock
        import yfmcp.tools.pricing as pricing_tools

        class _FastInfo:
            timezone = "America/New_York"

            def __getitem__(self, key):
                if key == "previousClose":
                    return 100.0
                raise KeyError(key)

            def __getattr__(self, name):
                raise AttributeError(name)

        class _Company:
            def __init__(self):
                self.fast_info = _FastInfo()
                self.info = {}

            def history(self, *args, **kwargs):
                raise RuntimeError("history should be mocked")

        idx = self.srv.pd.to_datetime(
            ["2026-06-08T01:00:00Z", "2026-06-08T02:00:00Z"], utc=True
        )
        hist = self.srv.pd.DataFrame(
            {
                "Open": [101.0, 102.0],
                "High": [103.0, 104.0],
                "Low": [100.0, 101.0],
                "Close": [102.0, 103.0],
                "Volume": [10, 20],
            },
            index=idx,
        )

        async def _fake_fetch(*args, **kwargs):
            return hist

        with (
            mock.patch.object(self.srv.yf, "Ticker", return_value=_Company()),
            mock.patch.object(pricing_tools, "_fetch_with_retry", _fake_fetch),
            mock.patch.object(
                self.srv.pd.Timestamp,
                "now",
                return_value=self.srv.pd.Timestamp("2026-06-08T07:00:00Z"),
            ),
        ):
            out = json.loads(_run(self.srv.get_overnight_quote("ZZTEST")))

        self.assertIn("sessionStatus", out)
        self.assertIn("requestedAt", out)
        self.assertEqual(out["sessionStatus"], "ACTIVE")


# ---------------------------------------------------------------------------
# 4. get_market_snapshot schema smoke (offline, mocked components)
# ---------------------------------------------------------------------------

class TestMarketSnapshotSchema(unittest.TestCase):
    """Test snapshot output shape by patching atomic tool functions."""

    def setUp(self):
        self.srv = _reload_server()

    def _mock_all_components(self, srv):
        """Replace atomic tool functions with lightweight stubs."""
        import unittest.mock as mock
        import yfmcp.tools.pricing as pricing_tools

        async def _fast_info(ticker, *a, **kw):
            return json.dumps({
                "ticker": ticker,
                "lastPrice": 83.67,
                "previousClose": 83.01,
                "yearHigh": 129.89,
                "yearLow": 22.47,
                "fiftyDayAverage": 83.71,
                "twoHundredDayAverage": 74.68,
                "lastVolume": 21579066,
                "tenDayAverageVolume": 22959070,
                "threeMonthAverageVolume": 15802664,
                "marketOpen": False,
                "lastTradeDate": "2026-05-15",
                "currency": "USD",
            })

        async def _price_stats(ticker, *a, **kw):
            return json.dumps({
                "ticker": ticker,
                "pctChangeTodayVsPrevClose": 0.80,
                "pctFromYearHigh": -35.58,
                "pctFromYearLow": 272.36,
                "annualizedVolatility30d": 103.46,
                "dataDate": "2026-05-15",
            })

        async def _ma_position(ticker, *a, **kw):
            return json.dumps({
                "ticker": ticker,
                "pctVs50dma": -0.05,
                "pctVs200dma": 12.04,
                "trend": "MIXED",
                "dataDate": "2026-05-15",
            })

        async def _volume_ratio(ticker, period=10, *a, **kw):
            return json.dumps({
                "ticker": ticker,
                "ratio10d": 0.94,
                "ratio90d": 1.366,
                "volumeFlag": "NORMAL",
            })

        async def _volume_gate(ticker, fx=False, *a, **kw):
            return json.dumps({
                "ticker": ticker,
                "adv20d": 20516600,
                "ratio20d": 1.05,
                "gatePass": True,
            })

        async def _tech_indicators(ticker, period="3mo", *a, **kw):
            return json.dumps({
                "ticker": ticker,
                "rsi14": 54.15,
                "macdHistogram": 1.7079,
            })

        patcher = mock.patch.multiple(
            pricing_tools,
            get_fast_info=_fast_info,
            get_price_stats=_price_stats,
            get_ma_position=_ma_position,
            get_volume_ratio=_volume_ratio,
            get_volume_gate=_volume_gate,
            get_technical_indicators=_tech_indicators,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_compact_snapshot_shape(self):
        self._mock_all_components(self.srv)
        result = json.loads(_run(self.srv.get_market_snapshot("ASTS")))

        # top-level required fields
        self.assertEqual(result["ticker"], "ASTS")
        self.assertIn("price", result)
        self.assertIn("range", result)
        self.assertIn("trend", result)
        self.assertIn("volume", result)
        self.assertIn("risk", result)
        self.assertIn("freshness", result)
        self.assertIn("componentStatus", result)
        self.assertIn("partialSuccess", result)
        self.assertIn("failedComponents", result)
        self.assertIn("warnings", result)

    def test_price_fields_populated(self):
        self._mock_all_components(self.srv)
        result = json.loads(_run(self.srv.get_market_snapshot("ASTS")))
        price = result["price"]
        self.assertIsNotNone(price["last"])
        self.assertIsNotNone(price["lastTradeDate"])
        self.assertIsInstance(price["marketOpen"], bool)

    def test_trend_fields_populated(self):
        self._mock_all_components(self.srv)
        result = json.loads(_run(self.srv.get_market_snapshot("ASTS")))
        trend = result["trend"]
        self.assertIsNotNone(trend["maTrend"])
        self.assertIsNotNone(trend["rsi14"])
        self.assertIsNotNone(trend["macdHistogram"])

    def test_volume_liquidity_gate(self):
        self._mock_all_components(self.srv)
        result = json.loads(_run(self.srv.get_market_snapshot("ASTS")))
        self.assertIsNotNone(result["volume"]["liquidityGatePass"])
        self.assertIsInstance(result["volume"]["liquidityGatePass"], bool)

    def test_freshness_class_present(self):
        self._mock_all_components(self.srv)
        result = json.loads(_run(self.srv.get_market_snapshot("ASTS")))
        freshness = result["freshness"]
        self.assertIn("freshnessClass", freshness)
        valid_classes = {"FRESH", "MARKET_CLOSED_EXPECTED_STALE", "WEEKEND_EXPECTED_STALE", "STALE", "VERY_STALE", "UNKNOWN"}
        self.assertIn(freshness["freshnessClass"], valid_classes)

    def test_partial_success_false_when_all_ok(self):
        self._mock_all_components(self.srv)
        result = json.loads(_run(self.srv.get_market_snapshot("ASTS")))
        self.assertFalse(result["partialSuccess"])
        self.assertEqual(result["failedComponents"], [])
        self.assertEqual(result["warnings"], [])

    def test_partial_success_true_when_one_fails(self):
        import unittest.mock as mock
        self._mock_all_components(self.srv)

        async def _failing_tech(ticker, period="3mo", *a, **kw):
            raise RuntimeError("simulated failure")

        with mock.patch.object(__import__("yfmcp.tools.pricing", fromlist=["get_technical_indicators"]), "get_technical_indicators", _failing_tech):
            result = json.loads(_run(self.srv.get_market_snapshot("ASTS")))

        self.assertTrue(result["partialSuccess"])
        self.assertIn("technicalIndicators", result["failedComponents"])
        self.assertTrue(any(w["code"] == "COMPONENT_FAILED" for w in result["warnings"]))

    def test_full_mode_has_components_key(self):
        self._mock_all_components(self.srv)
        result = json.loads(_run(self.srv.get_market_snapshot("ASTS", mode="full")))
        self.assertIn("_components", result)

    def test_compact_mode_no_components_key(self):
        self._mock_all_components(self.srv)
        result = json.loads(_run(self.srv.get_market_snapshot("ASTS", mode="compact")))
        self.assertNotIn("_components", result)

    def test_batch_compact_respects_cap(self):
        self._mock_all_components(self.srv)
        tickers = ["ASTS", "VRT", "AAPL", "MSFT", "GOOG", "TSLA"]
        result = json.loads(_run(self.srv.get_market_snapshot(tickers, mode="compact")))
        self.assertIn("tickers", result)
        self.assertLessEqual(len(result["tickers"]), 5)
        self.assertTrue(result.get("truncated", False))

    def test_batch_full_respects_cap(self):
        self._mock_all_components(self.srv)
        tickers = ["ASTS", "VRT", "AAPL"]
        result = json.loads(_run(self.srv.get_market_snapshot(tickers, mode="full")))
        self.assertIn("tickers", result)
        self.assertLessEqual(len(result["tickers"]), 2)
        self.assertTrue(result.get("truncated", False))


# ---------------------------------------------------------------------------
# 4. Public wording smoke — tool descriptions must not contain private language
# ---------------------------------------------------------------------------

class TestPublicWording(unittest.TestCase):
    """Tool descriptions exposed via tools/list must use neutral public language."""

    FORBIDDEN_TERMS = [
        r"\bIO\b",
        r"\bCommander\b",
        r"\bdoctrine\b",
        r"\bDC-\d",
        r"\bEQF\b",
        r"\bTPS\b",
        r"IO price target",
        r"DC Section",
        r"DC-149",
        r"PCCE Rule",
        r"portfolio state",
    ]

    def setUp(self):
        self.srv = _reload_server()

    def _get_canonical_tool_names(self):
        import glob
        import re
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sources = [os.path.join(root, "server.py")]
        tools_dir = os.path.join(root, "yfmcp", "tools")
        if os.path.isdir(tools_dir):
            sources.extend(sorted(glob.glob(os.path.join(tools_dir, "*.py"))))
        names = set()
        for path in sources:
            with open(path, encoding="utf-8") as fh:
                names.update(
                    m.group(1)
                    for m in re.finditer(r'@yfinance_server\.tool\(\s*name\s*=\s*"([^"]+)"', fh.read())
                )
        return names

    def _get_tool_descriptions(self):
        """Extract all tool descriptions from @yfinance_server.tool decorators."""
        import glob
        import re
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sources = [os.path.join(root, "server.py")]
        tools_dir = os.path.join(root, "yfmcp", "tools")
        if os.path.isdir(tools_dir):
            sources.extend(sorted(glob.glob(os.path.join(tools_dir, "*.py"))))
        descs = []
        for path in sources:
            with open(path, encoding="utf-8") as fh:
                source = fh.read()
            descs += re.findall(r'description\s*=\s*"""(.*?)"""', source, re.DOTALL)
            descs += re.findall(r'description\s*=\s*"([^"]+)"', source)
        return descs

    def test_no_private_wording_in_descriptions(self):
        descs = self._get_tool_descriptions()
        self.assertGreater(len(descs), 0, "No descriptions found — regex may need updating")
        violations = []
        for term in self.FORBIDDEN_TERMS:
            pattern = re.compile(term, re.IGNORECASE)
            for desc in descs:
                if pattern.search(desc):
                    violations.append(f"Term '{term}' found in description: {desc[:100]!r}")
        self.assertEqual(violations, [], "\n".join(violations))


# ---------------------------------------------------------------------------
# 5. Tool sync smoke — new canonical tools are registered
# ---------------------------------------------------------------------------

class TestToolRegistration(unittest.TestCase):
    def setUp(self):
        self.srv = _reload_server()

    def _get_registered_tool_names(self):
        import glob
        import re
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sources = [os.path.join(root, "server.py")]
        tools_dir = os.path.join(root, "yfmcp", "tools")
        if os.path.isdir(tools_dir):
            sources.extend(sorted(glob.glob(os.path.join(tools_dir, "*.py"))))
        names = set()
        for path in sources:
            with open(path, encoding="utf-8") as fh:
                names.update(
                    m.group(1)
                    for m in re.finditer(r'@yfinance_server\.tool\(\s*name\s*=\s*"([^"]+)"', fh.read())
                )
        return names

    def test_get_manifest_diagnostics_registered(self):
        self.assertIn("get_manifest_diagnostics", self._get_registered_tool_names())

    def test_get_market_snapshot_registered(self):
        self.assertIn("get_market_snapshot", self._get_registered_tool_names())

    def test_health_check_still_registered(self):
        self.assertIn("health_check", self._get_registered_tool_names())

    def test_atomic_tools_preserved(self):
        tools = self._get_registered_tool_names()
        for atomic in [
            "get_market_quote",
            "analyze_price_performance",
            "analyze_moving_average_position",
            "analyze_volume_ratio",
            "check_volume_liquidity_threshold",
            "get_technical_indicators",
        ]:
            self.assertIn(atomic, tools, f"Atomic tool {atomic!r} must not be removed")

    def test_private_aliases_removed(self):
        tools = self._get_registered_tool_names()
        for alias in [
            "get_adv_gate",
            "get_eqf_bracket",
            "get_tps_inputs",
            "get_dc134_options_scan",
            "get_china_revenue_pct",
            "get_geographic_revenue",
            "get_filing_text_search",
            "get_filing_document",
        ]:
            self.assertNotIn(alias, tools, f"Private alias {alias!r} should not be registered")


# ---------------------------------------------------------------------------
# 6. V2 envelope contract
# ---------------------------------------------------------------------------

class TestV2Envelope(unittest.TestCase):
    def setUp(self):
        self.srv = _reload_server()

    def test_ok_true_error_null_is_success(self):
        raw = json.dumps({"ticker": "AAPL", "lastPrice": 150.0})
        result = json.loads(self.srv._mcp_success("get_market_quote", raw))
        self.assertTrue(result["ok"])
        self.assertIsNone(result["error"])
        self.assertIn("data", result)
        self.assertIn("meta", result)

    def test_ok_false_has_error_code(self):
        result = json.loads(self.srv._mcp_failure("some_tool", "TICKER_NOT_FOUND", "not found"))
        self.assertFalse(result["ok"])
        self.assertIsNone(result["data"])
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["error"]["code"], "TICKER_NOT_FOUND")

    def test_tool_success_with_not_found_status_is_still_ok_true(self):
        """ok=true with data.status=NOT_FOUND is still tool success, not transport failure."""
        raw = json.dumps({"status": "NOT_FOUND", "ticker": "AAPL"})
        result = json.loads(self.srv._mcp_success("search_sec_filing_text", raw))
        self.assertTrue(result["ok"])
        self.assertIsNone(result["error"])
        self.assertEqual(result["data"]["status"], "NOT_FOUND")

    def test_warnings_preserved_in_envelope(self):
        raw = json.dumps({"ticker": "AAPL"})
        result = json.loads(self.srv._mcp_success(
            "get_market_quote", raw,
            warnings=[{"code": "TARGET_LAG", "message": "stale"}]
        ))
        self.assertTrue(result["ok"])
        self.assertIsInstance(result["meta"]["warnings"], list)
        self.assertEqual(len(result["meta"]["warnings"]), 1)


if __name__ == "__main__":
    unittest.main()
