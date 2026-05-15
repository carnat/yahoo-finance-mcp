#!/usr/bin/env python3
"""Phase 4 tests: Centralized TTL cache (ToolCache class).

These are offline/unit tests — no network calls required.
Run: PYTHONPATH=. python scripts/test_phase4.py
"""

import os
import sys
import time
import unittest

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


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestToolCache)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
