#!/usr/bin/env python3
"""Offline response-contract tests for Python options-summary failures."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from unittest.mock import patch


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# FastMCP versions used by the mirror differ over output_schema support.
from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402

_original_tool = _FastMCP.tool


def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
    return _original_tool(self, name=name, **kwargs)


_FastMCP.tool = _patched_tool  # type: ignore[method-assign]

import server as srv  # noqa: E402


class TestOptionsSummaryFailureEnvelope(unittest.TestCase):
    def call_with_envelope(self, ticker: str, expiry_hint: str | None = None) -> dict:
        previous = os.environ.get("MCP_ENVELOPE_V2")
        os.environ["MCP_ENVELOPE_V2"] = "true"
        try:
            raw = asyncio.run(srv.get_options_summary(ticker, expiry_hint))
        finally:
            if previous is None:
                os.environ.pop("MCP_ENVELOPE_V2", None)
            else:
                os.environ["MCP_ENVELOPE_V2"] = previous
        return json.loads(raw)

    def test_no_options_data_is_top_level_failure(self) -> None:
        class NoOptionsTicker:
            options = []

        with patch.object(srv.yf, "Ticker", return_value=NoOptionsTicker()):
            result = self.call_with_envelope("asts")

        self.assertFalse(result["ok"])
        self.assertIsNone(result["data"])
        self.assertEqual(result["error"]["code"], srv.ErrorCode.NO_OPTIONS_DATA)
        self.assertEqual(result["error"]["ticker"], "ASTS")

    def test_invalid_expiry_is_top_level_failure_with_recovery_fields(self) -> None:
        class CalendarTicker:
            options = ["2026-08-21"]

        with patch.object(srv.yf, "Ticker", return_value=CalendarTicker()):
            result = self.call_with_envelope("ASTS", "2026-09-18")

        self.assertFalse(result["ok"])
        self.assertIsNone(result["data"])
        self.assertEqual(result["error"]["code"], "INVALID_EXPIRY_DATE")
        self.assertEqual(result["error"]["nearestExpiration"], "2026-08-21")
        self.assertEqual(result["error"]["validExpirations"], ["2026-08-21"])

    def test_upstream_failure_is_top_level_provider_error(self) -> None:
        class FailingTicker:
            @property
            def options(self):
                raise RuntimeError("Yahoo Finance API error 500")

        with patch.object(srv.yf, "Ticker", return_value=FailingTicker()):
            result = self.call_with_envelope("ASTS")

        self.assertFalse(result["ok"])
        self.assertIsNone(result["data"])
        self.assertEqual(result["error"]["code"], srv.ErrorCode.PROVIDER_ERROR)
        self.assertEqual(result["error"]["ticker"], "ASTS")
        self.assertIn("Yahoo Finance API error 500", result["error"]["message"])


if __name__ == "__main__":
    unittest.main()
