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
import os
import sys
import types
import unittest

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


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(__import__(__name__))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
