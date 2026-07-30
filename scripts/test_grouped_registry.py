#!/usr/bin/env python3
"""Tests for grouped-mode handler resolution via the FastMCP-derived registry.

Phase 2 moved grouped-mode handler resolution off ``server.py`` module globals
and onto ``yfmcp.app.build_handler_registry``, which reads the live FastMCP tool
manager. These tests guard the plan's top risk for the split: that grouped mode
silently loses handlers as tools migrate into yfmcp.tools.* modules.

Offline — no network calls required.
Run:
    PYTHONPATH=. python scripts/test_grouped_registry.py
    pytest scripts/test_grouped_registry.py -v
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import types
import unittest
from typing import Literal
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _ensure_mcp_available() -> None:
    try:
        from mcp.server.fastmcp import FastMCP  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _ToolEntry:
        def __init__(self, fn):
            self.fn = fn

    class _ToolManager:
        def __init__(self):
            self._tools: dict[str, _ToolEntry] = {}

    class _FastMCPStub:
        def __init__(self, *a: object, **kw: object) -> None:
            self._tool_manager = _ToolManager()

        def tool(self, *a: object, name: str | None = None, **kw: object):
            def _decorator(fn):
                self._tool_manager._tools[name or fn.__name__] = _ToolEntry(fn)
                return fn
            if a and callable(a[0]):
                return _decorator(a[0])
            return _decorator

        async def list_tools(self):
            return list(self._tool_manager._tools.values())

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
import tool_groups  # noqa: E402
from yfmcp.app import build_handler_registry, yfinance_server  # noqa: E402
from yfmcp.tools.system import _public_metadata  # noqa: E402


class TestHandlerRegistry(unittest.TestCase):
    def setUp(self):
        self.registry = build_handler_registry(yfinance_server)

    def test_registry_nonempty(self):
        self.assertTrue(self.registry, "registry should not be empty")

    def test_every_grouped_handler_is_resolvable(self):
        """Every handler referenced by TOOL_GROUPS must resolve to a callable.

        This is the regression guard: if a tool fails to register (or a domain
        module is not imported), the handler drops out of the registry and the
        grouped action becomes a runtime error. Catch it at test time instead.
        """
        missing = []
        for group_name, group_def in tool_groups.TOOL_GROUPS.items():
            for action, handler_name in group_def["actions"].items():
                fn = self.registry.get(handler_name)
                if fn is None or not callable(fn):
                    missing.append(f"{group_name}.{action} -> {handler_name}")
        self.assertEqual(missing, [], f"unresolvable grouped handlers: {missing}")

    def test_registry_keyed_by_function_name(self):
        for name, fn in self.registry.items():
            self.assertEqual(name, fn.__name__)


class TestGroupedServer(unittest.TestCase):
    def test_manifest_count_matches_visible_mode(self):
        with patch.dict(os.environ, {"TOOL_MODE": "grouped"}):
            self.assertEqual(_public_metadata()["toolCount"], len(tool_groups.TOOL_GROUPS))
        with patch.dict(os.environ, {"TOOL_MODE": "expanded"}):
            self.assertEqual(_public_metadata()["toolCount"], 111)

    def test_grouped_server_exposes_one_tool_per_group(self):
        original = os.environ.get("TOOL_MODE")
        os.environ["TOOL_MODE"] = "grouped"
        try:
            # _build_grouped_server reads TOOL_GROUPS, not the env directly,
            # so build it explicitly to avoid depending on module-load order.
            grouped = srv._build_grouped_server()
            tools = asyncio.run(grouped.list_tools())
            names = {getattr(t, "name", getattr(getattr(t, "fn", None), "__name__", None)) for t in tools}
            self.assertEqual(len(tools), len(tool_groups.TOOL_GROUPS))
            self.assertTrue(set(tool_groups.TOOL_GROUPS).issubset(names) or len(tools) == len(tool_groups.TOOL_GROUPS))
        finally:
            if original is None:
                os.environ.pop("TOOL_MODE", None)
            else:
                os.environ["TOOL_MODE"] = original


class TestGroupedRouting(unittest.TestCase):
    def setUp(self):
        self.registry = build_handler_registry(yfinance_server)

    def call(self, group: str, action: str, params: dict | None) -> dict:
        with patch.dict(os.environ, {"MCP_ENVELOPE_V2": "true"}):
            raw = asyncio.run(
                tool_groups._route_grouped_call(group, action, params, self.registry)
            )
        return json.loads(raw)

    def test_registered_grouped_handler_invokes_router(self):
        grouped = srv._build_grouped_server()
        entry = grouped._tool_manager._tools["system"]
        with patch.dict(os.environ, {"MCP_ENVELOPE_V2": "true"}):
            payload = json.loads(asyncio.run(entry.fn("health_check", {})))
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["toolMode"], "grouped")

    def test_missing_required_param_fails_before_provider_call(self):
        payload = self.call("stock_pricing", "get_market_quote", {})
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "INPUT_VALIDATION_ERROR")
        self.assertEqual(payload["error"]["missingParams"], ["ticker"])
        self.assertEqual(payload["error"]["recommendedNextAction"], "CORRECT_TOOL_PARAMS")

    def test_compat_error_keeps_recovery_diagnostics(self):
        with patch.dict(os.environ, {"MCP_ENVELOPE_V2": ""}):
            raw = asyncio.run(
                tool_groups._route_grouped_call(
                    "stock_pricing",
                    "get_market_quote",
                    {},
                    self.registry,
                )
            )
        payload = json.loads(raw)
        self.assertTrue(payload["error"])
        self.assertEqual(payload["missingParams"], ["ticker"])
        self.assertEqual(payload["recommendedNextAction"], "CORRECT_TOOL_PARAMS")

    def test_empty_required_param_fails_before_provider_call(self):
        payload = self.call("news_events", "get_company_news", {"ticker": "   "})
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["invalidParams"], ["ticker"])

    def test_unexpected_param_is_not_silently_dropped(self):
        payload = self.call(
            "sec_filings",
            "list_sec_material_filings",
            {"ticker": "AAPL", "form_types": ["8-K"]},
        )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["unexpectedParams"], ["form_types"])

    def test_invalid_nested_param_type_is_rejected(self):
        payload = self.call(
            "news_events",
            "get_company_news",
            {"ticker": "AAPL", "max_results": "ten"},
        )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["invalidParams"], ["max_results"])

    def test_semantic_literal_value_reaches_action_handler(self):
        async def semantic_handler(
            ticker: str,
            query_type: Literal["supported"],
            params: dict | None = None,
        ) -> str:
            return json.dumps(
                {
                    "ok": False,
                    "data": None,
                    "meta": {
                        "tool": "query_sec_filing_index",
                        "supportedQueryTypes": ["supported"],
                    },
                    "error": {
                        "code": "UNSUPPORTED_QUERY_TYPE",
                        "message": f"Unsupported query type '{query_type}' for {ticker}.",
                    },
                }
            )

        with patch.dict(os.environ, {"MCP_ENVELOPE_V2": "true"}):
            raw = asyncio.run(
                tool_groups._route_grouped_call(
                    "sec_filings",
                    "query_sec_filing_index",
                    {
                        "ticker": "AAPL",
                        "query_type": "unsupported",
                        "params": {},
                    },
                    {"query_sec_filing_index": semantic_handler},
                )
            )
        payload = json.loads(raw)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "UNSUPPORTED_QUERY_TYPE", payload)
        self.assertEqual(payload["meta"]["supportedQueryTypes"], ["supported"])

    def test_legacy_error_string_becomes_failure_envelope(self):
        async def fake_expirations(ticker: str) -> str:
            return f"Error: getting option expiration dates for {ticker}: provider unavailable"

        registry = {"get_option_expiration_dates": fake_expirations}
        with patch.dict(os.environ, {"MCP_ENVELOPE_V2": "true"}):
            raw = asyncio.run(
                tool_groups._route_grouped_call(
                    "options_analysis",
                    "get_option_expiration_dates",
                    {"ticker": "AAPL"},
                    registry,
                )
            )
        payload = json.loads(raw)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "PROVIDER_ERROR")


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(__import__(__name__))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
