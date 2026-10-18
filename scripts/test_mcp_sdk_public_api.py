#!/usr/bin/env python3
"""Guard (Q8, 2.13.1): the Python server uses only public MCP SDK APIs.

Fails when ``yfmcp/``, ``server.py``, ``tool_groups.py`` or ``scripts/`` touch the SDK's private attributes
(``_tool_manager``, ``_mcp_server``, ``_lowlevel_server``) or patch ``MCPServer``/``FastMCP`` class attributes.
It also checks the public seams that replaced them: the ``YfmcpServer`` subclass, its ordered registry, and
a listing that is the same through the protocol and through ``list_tools()``.

Offline. Run: python scripts/test_mcp_sdk_public_api.py
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.append(str(ROOT / "scripts"))
from source_text import python_runtime_files  # noqa: E402

THIS_FILE = Path(__file__).resolve()
# Every Python runtime module (server.py, tool_groups.py, yfmcp/**, via scripts/source_text.py) plus scripts/.
SCAN_ROOTS = [ROOT / "scripts"]

PRIVATE_NAMES = re.compile(r"\b(_tool_manager|_mcp_server|_lowlevel_server)\b")
SDK_CLASSES = r"(?:FastMCP|MCPServer|YfmcpServer)"
CLASS_PATCHING = [
    re.compile(rf"\b{SDK_CLASSES}\.\w+\s*=(?!=)"),  # FastMCP.tool = ...
    re.compile(rf"\bsetattr\(\s*{SDK_CLASSES}\b"),
    re.compile(rf"\bpatch(?:\.object|\.multiple)?\(\s*(?:[\w.]*\.)?{SDK_CLASSES}\b"),
    re.compile(r"\b_yfmcp_original_tool\b"),
    re.compile(r"mcp\.server\.fastmcp\b"),  # a stubbed 1.x module
]


def _python_files() -> list[Path]:
    files = list(python_runtime_files())
    for root in SCAN_ROOTS:
        files.extend(sorted(root.rglob("*.py")))
    return [f for f in files if f.resolve() != THIS_FILE and "__pycache__" not in f.parts]


class TestNoPrivateSdkAccess(unittest.TestCase):
    def test_files_are_scanned(self):
        names = {f.name for f in _python_files()}
        for expected in ("server.py", "tool_groups.py", "app.py", "system.py", "test_grouped_registry.py"):
            self.assertIn(expected, names)

    def test_no_private_sdk_attribute_access(self):
        hits = []
        for path in _python_files():
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if PRIVATE_NAMES.search(line):
                    hits.append(f"{path.relative_to(ROOT)}:{number}: {line.strip()}")
        self.assertEqual(hits, [], "private MCP SDK attributes are off limits; use YfmcpServer's public seams")

    def test_no_sdk_class_patching(self):
        hits = []
        for path in _python_files():
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if any(pattern.search(line) for pattern in CLASS_PATCHING):
                    hits.append(f"{path.relative_to(ROOT)}:{number}: {line.strip()}")
        self.assertEqual(hits, [], "do not patch MCPServer/FastMCP; subclass it (yfmcp.app.YfmcpServer)")


class TestPublicSeams(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("TOOL_MODE", "expanded")
        import server

        cls.server = server

    def test_servers_are_yfmcp_servers_overriding_public_methods(self):
        from mcp.server.mcpserver import MCPServer
        from yfmcp.app import YfmcpServer, yfinance_server

        self.assertIsInstance(yfinance_server, YfmcpServer)
        self.assertIsInstance(self.server._build_grouped_server(), YfmcpServer)
        self.assertIsNot(YfmcpServer.tool, MCPServer.tool)
        self.assertIsNot(YfmcpServer.list_tools, MCPServer.list_tools)
        for name in ("tool", "list_tools"):
            self.assertEqual(getattr(MCPServer, name).__qualname__, f"MCPServer.{name}", "SDK class was patched")

    def test_listing_follows_registry_order_with_after_position(self):
        from yfmcp.app import yfinance_server

        listed = [tool.name for tool in asyncio.run(yfinance_server.list_tools())]
        self.assertEqual(listed, [spec["name"] for spec in yfinance_server.tool_specs()])
        self.assertEqual(listed[listed.index("get_company_events_calendar") + 1], "get_company_news")
        self.assertEqual(len(listed), len(set(listed)))

    def test_listing_applies_ticker_caps_to_input_schema_only(self):
        from yfmcp.app import yfinance_server
        from yfmcp.validation import TICKER_CAPS

        tools = {tool.name: tool for tool in asyncio.run(yfinance_server.list_tools())}
        for name, cap in TICKER_CAPS.items():
            if "." in name:
                continue
            ticker = tools[name].input_schema["properties"]["ticker"]
            branches = [b for b in ticker.get("anyOf", []) + ticker.get("oneOf", []) if b.get("type") == "array"]
            self.assertEqual([b.get("maxItems") for b in branches], [cap], name)
        # Listing a second time must not accumulate or leak into later schemas.
        again = {tool.name: tool for tool in asyncio.run(yfinance_server.list_tools())}
        self.assertEqual(
            {n: t.input_schema for n, t in tools.items()},
            {n: t.input_schema for n, t in again.items()},
        )

    def test_protocol_listing_equals_list_tools(self):
        from mcp import Client
        from yfmcp.app import yfinance_server

        async def over_protocol():
            async with Client(yfinance_server) as client:
                return (await client.list_tools()).tools

        protocol = [t.model_dump(mode="json", by_alias=True) for t in asyncio.run(over_protocol())]
        direct = [t.model_dump(mode="json", by_alias=True) for t in asyncio.run(yfinance_server.list_tools())]
        self.assertEqual(protocol, direct)

    def test_registered_handler_seams(self):
        import inspect

        from yfmcp.app import registered_handler, registered_tool_fn, yfinance_server

        self.assertIs(registered_handler("get_market_quote"), self.server.get_market_quote)
        self.assertTrue(inspect.iscoroutinefunction(registered_tool_fn("get_market_quote")))
        self.assertIsNot(registered_tool_fn("get_market_quote"), registered_handler("get_market_quote"))
        with self.assertRaises(RuntimeError):
            registered_handler("no_such_tool")
        self.assertIs(yfinance_server.registration("health_check").fn, self.server.health_check)

    def test_registries_fail_loudly_instead_of_returning_empty(self):
        from yfmcp.app import build_handler_registry, build_tool_contract_registry, create_server

        empty = create_server("empty", "")
        with self.assertRaises(RuntimeError):
            build_handler_registry(empty)
        with self.assertRaises(RuntimeError):
            build_tool_contract_registry(empty)
        with self.assertRaises(RuntimeError):
            empty.set_input_schema("missing", {})

        orphan = create_server("orphan", "")

        @orphan.tool(name="orphan_tool", after="not_registered")
        async def orphan_tool() -> str:
            return "{}"

        with self.assertRaises(RuntimeError):
            asyncio.run(orphan.list_tools())

    def test_decorator_rejects_output_schema_kwarg(self):
        from yfmcp.app import create_server

        with self.assertRaises(TypeError):
            create_server("x", "").tool(name="x", output_schema={})  # type: ignore[call-arg]

    def test_version_property_reports_build_version(self):
        from yfmcp.app import yfinance_server
        from yfmcp.build_info import BUILD_VERSION

        self.assertEqual(yfinance_server.version, BUILD_VERSION)


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False)
    sys.exit(0 if result.result.wasSuccessful() else 1)
