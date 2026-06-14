#!/usr/bin/env python3
"""Phase 4 tests: Centralized TTL cache (ToolCache class).

These are offline/unit tests — no network calls required.
Run: PYTHONPATH=. python scripts/test_phase4.py
"""

import os
import sys
import time
import unittest
import asyncio
import json
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Patch FastMCP.tool to accept output_schema (not in mcp>=1.9)
from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402
_orig_tool = _FastMCP.tool
def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
    return _orig_tool(self, name=name, **kwargs)
_FastMCP.tool = _patched_tool  # type: ignore[method-assign]

import server as srv


class TestToolCache(unittest.TestCase):
    def setUp(self):
        self.cache = srv.ToolCache()

    def test_miss_on_empty(self):
        self.assertIsNone(self.cache.get("missing_key"))

    def test_set_and_get(self):
        self.cache.set("k", '{"v":1}', 60.0)
        result = self.cache.get("k")
        self.assertIsNotNone(result)
        value, cache_hit, cached_at = result  # type: ignore[misc]
        self.assertEqual(value, '{"v":1}')
        self.assertTrue(cache_hit)
        self.assertIsNotNone(cached_at)

    def test_expiry(self):
        """Entry should be expired after TTL seconds."""
        self.cache.set("exp_key", "x", 0.01)  # 10ms TTL
        time.sleep(0.05)
        self.assertIsNone(self.cache.get("exp_key"))

    def test_not_stale_fresh(self):
        self.cache.set("fresh", "x", 3600.0)
        self.assertFalse(self.cache.is_stale("fresh"))

    def test_stale_after_2x_ttl(self):
        """is_stale returns True when age > 2× TTL."""
        self.cache.set("stale", "x", 0.01)
        time.sleep(0.05)  # 5× TTL
        self.assertTrue(self.cache.is_stale("stale"))

    def test_stale_unknown_key(self):
        self.assertFalse(self.cache.is_stale("does_not_exist"))

    def test_ttl_tiers_defined(self):
        self.assertGreater(srv.TTL_PRICE, 0)
        self.assertGreater(srv.TTL_ANALYST, srv.TTL_PRICE)
        self.assertGreater(srv.TTL_FINANCIALS, srv.TTL_ANALYST)
        self.assertGreater(srv.TTL_EDGAR, srv.TTL_FINANCIALS)
        self.assertEqual(srv.TTL_OPTIONS, srv.TTL_ANALYST)

    def test_legacy_price_ttl_alias(self):
        self.assertEqual(srv._PRICE_TTL, srv.TTL_PRICE)

    def test_legacy_stmt_ttl_alias(self):
        self.assertEqual(srv._STMT_TTL, srv.TTL_FINANCIALS)

    def test_legacy_cache_get_set(self):
        """Legacy _cache_get / _cache_set should still work."""
        # Use a fresh cache state by using an unused key
        srv._cache_set("legacy_test", '{"x":2}', ttl=300.0)
        val = srv._cache_get("legacy_test", 300.0)
        self.assertEqual(val, '{"x":2}')

    def test_legacy_cache_miss_expired(self):
        srv._cache_set("exp_legacy", "v", ttl=0.01)
        time.sleep(0.05)
        self.assertIsNone(srv._cache_get("exp_legacy", 0.01))


class TestQuerySecFilingIndex(unittest.TestCase):
    def _call(self, **kwargs):
        return json.loads(asyncio.run(srv.query_sec_filing_index(**kwargs)))

    def test_unsupported_query_type(self):
        res = self._call(ticker="AAPL", query_type="is_this_investable", params={})
        self.assertEqual(res.get("status"), "UNSUPPORTED_BY_INDEX")
        self.assertEqual(res.get("queryType"), "is_this_investable")

    def test_missing_required_param_region(self):
        with patch("server.extract_geographic_revenue", new_callable=AsyncMock) as mocked:
            res = self._call(
                ticker="AAPL",
                query_type="geographic_revenue_share",
                params={},
            )
            self.assertEqual(res.get("status"), "INPUT_VALIDATION_ERROR")
            codes = [w.get("code") for w in res.get("warnings", []) if isinstance(w, dict)]
            self.assertIn("INPUT_VALIDATION_ERROR", codes)
            mocked.assert_not_called()

    def test_answered_requires_evidence(self):
        geo = {
            "value": 1,
            "denominator": 10,
            "valueRatio": 0.1,
            "valuePct": 10.0,
            "confidence": "HIGH",
            "evidence": {},
            "unit": "USD",
        }
        with patch("server.extract_geographic_revenue", new_callable=AsyncMock, return_value=json.dumps(geo)):
            res = self._call(
                ticker="AAPL",
                query_type="geographic_revenue_share",
                params={"region": "Greater China"},
            )
            self.assertNotEqual(res.get("status"), "ANSWERED")
            codes = [w.get("code") for w in res.get("warnings", []) if isinstance(w, dict)]
            self.assertIn("EVIDENCE_REQUIRED", codes)

    def test_geographic_answered_shape(self):
        geo = {
            "value": 64377000000,
            "denominator": 416161000000,
            "valueRatio": 0.1547,
            "valuePct": 15.47,
            "confidence": "HIGH",
            "unit": "USD",
            "evidence": {
                "filingDate": "2025-10-31",
                "accessionNumber": "0000320193-25-000079",
                "documentUrl": "https://www.sec.gov/Archives/example",
                "sourceRows": [["Greater China", "64,377"], ["Total net sales", "416,161"]],
            },
        }
        with patch("server.extract_geographic_revenue", new_callable=AsyncMock, return_value=json.dumps(geo)):
            res = self._call(
                ticker="AAPL",
                query_type="geographic_revenue_share",
                params={"region": "Greater China"},
            )
            self.assertEqual(res.get("status"), "ANSWERED")
            ans = res.get("answer") or {}
            self.assertEqual(ans.get("valuePct"), 15.47)
            self.assertEqual(ans.get("denominator"), 416161000000)
            self.assertTrue(isinstance(res.get("evidence"), list) and len(res.get("evidence")) > 0)

    def test_not_disclosed_preserved(self):
        geo = {
            "value": None,
            "denominator": None,
            "valueRatio": None,
            "valuePct": None,
            "confidence": "NOT_DISCLOSED",
            "evidence": {},
            "unit": "USD",
        }
        with patch("server.extract_geographic_revenue", new_callable=AsyncMock, return_value=json.dumps(geo)):
            res = self._call(
                ticker="AXTI",
                query_type="geographic_revenue_share",
                params={"region": "China"},
            )
            self.assertEqual(res.get("status"), "NOT_DISCLOSED")


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestToolCache))
    suite.addTests(loader.loadTestsFromTestCase(TestQuerySecFilingIndex))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
