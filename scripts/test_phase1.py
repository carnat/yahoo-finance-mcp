#!/usr/bin/env python3
"""Phase 1 tests: McpResponse envelope (ok/data/meta/error), helpers, SERVER_VERSION.

These are offline/unit tests — no network calls required.
Run: PYTHONPATH=. python scripts/test_phase1.py
"""

import json
import os
import sys
import unittest

# Ensure we import from the repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Activate the envelope for these tests
os.environ["MCP_ENVELOPE_V2"] = "true"

# Patch FastMCP.tool to accept output_schema (not in mcp>=1.9)
from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402
_orig_tool = _FastMCP.tool
def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
    return _orig_tool(self, name=name, **kwargs)
_FastMCP.tool = _patched_tool  # type: ignore[method-assign]


def _reload_server():
    """Reload server module so the env var is picked up."""
    import importlib
    import server as srv
    importlib.reload(srv)
    return srv


class TestPhase1Envelope(unittest.TestCase):
    def setUp(self):
        self.srv = _reload_server()

    # ── _mcp_success ────────────────────────────────────────────────────────

    def test_success_envelope_shape(self):
        raw = json.dumps({"ticker": "AAPL", "lastPrice": 150.0})
        result = json.loads(self.srv._mcp_success("get_fast_info", raw))
        self.assertTrue(result["ok"])
        self.assertIsNone(result["error"])
        self.assertIn("meta", result)
        self.assertIn("data", result)

    def test_success_meta_fields(self):
        raw = json.dumps({"x": 1})
        result = json.loads(self.srv._mcp_success("my_tool", raw, source="yahoo_finance"))
        meta = result["meta"]
        self.assertEqual(meta["tool"], "my_tool")
        self.assertEqual(meta["source"], "yahoo_finance")
        self.assertEqual(meta["serverVersion"], self.srv.SERVER_VERSION)
        self.assertIsInstance(meta["warnings"], list)
        self.assertIsInstance(meta["cacheHit"], bool)

    def test_success_data_parsed(self):
        """data should be the parsed JSON object, not a raw string."""
        raw = json.dumps({"k": "v"})
        result = json.loads(self.srv._mcp_success("t", raw))
        self.assertIsInstance(result["data"], dict)
        self.assertEqual(result["data"]["k"], "v")

    def test_success_cache_hit(self):
        raw = json.dumps({})
        result = json.loads(self.srv._mcp_success("t", raw, cache_hit=True))
        self.assertTrue(result["meta"]["cacheHit"])

    def test_success_warnings(self):
        raw = json.dumps({})
        result = json.loads(self.srv._mcp_success("t", raw, warnings=["data may be stale"]))
        self.assertIn("data may be stale", result["meta"]["warnings"])

    # ── _mcp_failure ────────────────────────────────────────────────────────

    def test_failure_envelope_shape(self):
        result = json.loads(self.srv._mcp_failure("get_fast_info", "TICKER_NOT_FOUND", "No data"))
        self.assertFalse(result["ok"])
        self.assertIsNone(result["data"])
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["error"]["code"], "TICKER_NOT_FOUND")
        self.assertIn("No data", result["error"]["message"])

    def test_failure_meta_tool_name(self):
        result = json.loads(self.srv._mcp_failure("my_tool", "PROVIDER_ERROR", "timeout"))
        self.assertEqual(result["meta"]["tool"], "my_tool")

    # ── _mcp_warning ────────────────────────────────────────────────────────

    def test_warning_ok_true(self):
        raw = json.dumps({"data": 42})
        result = json.loads(self.srv._mcp_warning("t", raw, "stale data"))
        self.assertTrue(result["ok"])
        self.assertIn("stale data", result["meta"]["warnings"])

    # ── Feature flag off ────────────────────────────────────────────────────

    def test_flag_off_returns_raw(self):
        """When _ENVELOPE_V2 is False, _mcp_success returns the raw JSON string."""
        import yfmcp.envelope as _env
        original_env = _env._ENVELOPE_V2
        _env._ENVELOPE_V2 = False
        try:
            raw = json.dumps({"k": "v"})
            result = self.srv._mcp_success("t", raw)
            self.assertEqual(result, raw)
        finally:
            _env._ENVELOPE_V2 = original_env

    def test_flag_off_failure_returns_compat_format(self):
        import yfmcp.envelope as _env
        original_env = _env._ENVELOPE_V2
        _env._ENVELOPE_V2 = False
        try:
            result = json.loads(self.srv._mcp_failure("t", "TICKER_NOT_FOUND", "msg"))
            self.assertTrue(result.get("error"))
            self.assertEqual(result["code"], "TICKER_NOT_FOUND")
        finally:
            _env._ENVELOPE_V2 = original_env

    # ── SERVER_VERSION ──────────────────────────────────────────────────────

    def test_server_version_present(self):
        self.assertIsInstance(self.srv.SERVER_VERSION, str)
        self.assertTrue(len(self.srv.SERVER_VERSION) > 0)

    # ── ErrorCode ───────────────────────────────────────────────────────────

    def test_error_codes_defined(self):
        ec = self.srv.ErrorCode
        self.assertEqual(ec.TICKER_NOT_FOUND, "TICKER_NOT_FOUND")
        self.assertEqual(ec.NO_OPTIONS_DATA, "NO_OPTIONS_DATA")
        self.assertEqual(ec.NO_FILING_DATA, "NO_FILING_DATA")
        self.assertEqual(ec.PROVIDER_ERROR, "PROVIDER_ERROR")
        self.assertEqual(ec.PROVIDER_TIMEOUT, "PROVIDER_TIMEOUT")
        self.assertEqual(ec.RATE_LIMIT, "RATE_LIMIT")
        self.assertEqual(ec.INPUT_VALIDATION_ERROR, "INPUT_VALIDATION_ERROR")
        self.assertEqual(ec.DEPRECATED_TOOL, "DEPRECATED_TOOL")


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestPhase1Envelope)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
