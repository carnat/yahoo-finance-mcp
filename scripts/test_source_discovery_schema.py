#!/usr/bin/env python3
"""Offline source-level schema checks for get_option_chain.

Reads worker/src/tools.ts and server.py directly (no network, no imports)
to assert that the schema for get_option_chain contains all required fields
and correct defaults introduced in PR #48.

Run: python scripts/test_source_discovery_schema.py
"""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TOOLS_TS = ROOT / "worker" / "src" / "tools.ts"
SERVER_PY = ROOT / "server.py"


def _ts_source() -> str:
    return TOOLS_TS.read_text()


def _py_source() -> str:
    return SERVER_PY.read_text()


class TestGetOptionChainWorkerSchema(unittest.TestCase):
    """Verify worker/src/tools.ts get_option_chain schema fields and defaults."""

    def setUp(self) -> None:
        self.src = _ts_source()
        # Locate the get_option_chain tool block (from its name: line to the
        # next top-level TOOLS entry or end of TOOLS array).
        m = re.search(r'name:\s*"get_option_chain"', self.src)
        self.assertIsNotNone(m, "get_option_chain not found in tools.ts")
        # Capture a generous slice of source after the tool name declaration
        self.block = self.src[m.start() : m.start() + 4000]

    def test_moneyness_window_pct_present(self) -> None:
        self.assertIn("moneyness_window_pct", self.block,
                      "tools.ts: get_option_chain schema missing moneyness_window_pct")

    def test_include_illiquid_present(self) -> None:
        self.assertIn("include_illiquid", self.block,
                      "tools.ts: get_option_chain schema missing include_illiquid")

    def test_sort_by_enum_includes_relevance(self) -> None:
        # Find sort_by property block
        m = re.search(r'sort_by\s*:\s*\{[^}]*enum\s*:\s*\[([^\]]+)\]', self.block, re.S)
        self.assertIsNotNone(m, "tools.ts: get_option_chain sort_by enum not found")
        enum_str = m.group(1)
        self.assertIn("relevance", enum_str,
                      f"tools.ts: sort_by enum missing 'relevance': {enum_str!r}")

    def test_sort_by_default_is_relevance(self) -> None:
        # Expect: default: "relevance" within the sort_by block
        m = re.search(r'sort_by\s*:\s*\{.*?default\s*:\s*"([^"]+)"', self.block, re.S)
        self.assertIsNotNone(m, "tools.ts: get_option_chain sort_by default not found")
        self.assertEqual(m.group(1), "relevance",
                         f"tools.ts: sort_by default must be 'relevance', got {m.group(1)!r}")

    def test_moneyness_default_is_near_money(self) -> None:
        m = re.search(r'moneyness\s*:\s*\{.*?default\s*:\s*"([^"]+)"', self.block, re.S)
        self.assertIsNotNone(m, "tools.ts: get_option_chain moneyness default not found")
        self.assertEqual(m.group(1), "near_money",
                         f"tools.ts: moneyness default must be 'near_money', got {m.group(1)!r}")

    def test_moneyness_window_pct_default_is_20(self) -> None:
        m = re.search(
            r'moneyness_window_pct\s*:\s*\{.*?default\s*:\s*(\d+)', self.block, re.S
        )
        self.assertIsNotNone(m, "tools.ts: moneyness_window_pct default not found")
        self.assertEqual(int(m.group(1)), 20,
                         f"tools.ts: moneyness_window_pct default must be 20, got {m.group(1)!r}")

    def test_max_contracts_present(self) -> None:
        self.assertIn("max_contracts", self.block,
                      "tools.ts: get_option_chain schema missing max_contracts")

    def test_min_open_interest_present(self) -> None:
        self.assertIn("min_open_interest", self.block,
                      "tools.ts: get_option_chain schema missing min_open_interest")

    def test_min_volume_present(self) -> None:
        self.assertIn("min_volume", self.block,
                      "tools.ts: get_option_chain schema missing min_volume")


class TestGetOptionChainServerPySchema(unittest.TestCase):
    """Verify server.py get_option_chain function signature and defaults."""

    def setUp(self) -> None:
        self.src = _py_source()
        # Locate the async def get_option_chain function
        m = re.search(r'async def get_option_chain\s*\(', self.src)
        self.assertIsNotNone(m, "get_option_chain not found in server.py")
        self.block = self.src[m.start() : m.start() + 1000]

    def test_moneyness_default_near_money(self) -> None:
        m = re.search(r'moneyness\s*:\s*str\s*=\s*"([^"]+)"', self.block)
        self.assertIsNotNone(m, "server.py: get_option_chain moneyness default not found")
        self.assertEqual(m.group(1), "near_money",
                         f"server.py: moneyness default must be 'near_money', got {m.group(1)!r}")

    def test_moneyness_window_pct_default_20(self) -> None:
        m = re.search(r'moneyness_window_pct\s*:\s*float\s*=\s*([\d.]+)', self.block)
        self.assertIsNotNone(m, "server.py: moneyness_window_pct default not found")
        self.assertAlmostEqual(float(m.group(1)), 20.0,
                               msg=f"server.py: moneyness_window_pct default must be 20.0, got {m.group(1)!r}")

    def test_sort_by_default_relevance(self) -> None:
        m = re.search(r'sort_by\s*:\s*str\s*=\s*"([^"]+)"', self.block)
        self.assertIsNotNone(m, "server.py: get_option_chain sort_by default not found")
        self.assertEqual(m.group(1), "relevance",
                         f"server.py: sort_by default must be 'relevance', got {m.group(1)!r}")

    def test_include_illiquid_default_false(self) -> None:
        m = re.search(r'include_illiquid\s*:\s*bool\s*=\s*(False|True)', self.block)
        self.assertIsNotNone(m, "server.py: include_illiquid default not found")
        self.assertEqual(m.group(1), "False",
                         f"server.py: include_illiquid default must be False, got {m.group(1)!r}")


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False)
    sys.exit(0 if result.result.wasSuccessful() else 1)
