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


# Generous slice of source after the tool name declaration; 4000 chars is
# sufficient to cover the entire inputSchema block for any single tool.
_TOOL_BLOCK_SIZE = 4000


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
        self.block = self.src[m.start() : m.start() + _TOOL_BLOCK_SIZE]

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


class TestPublicConnectorSurfaceSource(unittest.TestCase):
    def setUp(self) -> None:
        self.ts = _ts_source()
        self.py = _py_source()

    def _window(self, text: str, marker: str, span: int = 500) -> str:
        idx = text.find(marker)
        self.assertGreaterEqual(idx, 0, f"missing marker: {marker}")
        return text[idx: idx + span]

    def test_key_public_descriptions_are_clean(self) -> None:
        forbidden = (
            r"\bIO\b",
            r"Commander",
            r"portfolio state",
            r"doctrine",
            r"DC-",
            r"DC Section",
            r"DC-80",
            r"DC-149",
            r"TPS",
            r"PCCE",
            r"EQF",
            r"\bT[1-5]\b",
        )
        names = [
            "get_calendar",
            "get_company_events_calendar",
            "get_price_target_bracket",
            "calculate_price_target_distance",
            "get_position_score_inputs",
            "analyze_position_signals",
            "get_volume_gate",
            "check_volume_liquidity_threshold",
        ]
        for name in names:
            py_win = self._window(self.py, f'name="{name}"')
            ts_win = self._window(self.ts, f'name: "{name}"')
            for pattern in forbidden:
                self.assertIsNone(re.search(pattern, py_win, re.IGNORECASE), f"{name} server.py hit /{pattern}/")
                self.assertIsNone(re.search(pattern, ts_win, re.IGNORECASE), f"{name} tools.ts hit /{pattern}/")

    def test_reference_target_parameter_is_exposed(self) -> None:
        ts_win = self._window(self.ts, 'name: "calculate_price_target_distance"', span=900)
        self.assertIn("reference_target_price", ts_win)
        self.assertIn("io_pt", ts_win)
        py_sig_win = self._window(self.py, "async def calculate_price_target_distance", span=260)
        self.assertIn("reference_target_price", py_sig_win)
        self.assertIn("io_pt", py_sig_win)

    def test_deprecated_aliases_have_standard_metadata(self) -> None:
        expected_aliases = [
            "get_dc134_options_scan",
            "get_eqf_bracket",
            "get_tps_inputs",
            "get_adv_gate",
        ]
        for alias in expected_aliases:
            win = self._window(self.ts, f'name: "{alias}"', span=700)
            self.assertIn("deprecated: true", win, f"{alias}: missing deprecated=true")
            self.assertIn("useInstead:", win, f"{alias}: missing useInstead")
            self.assertIn('deprecationReason: "Use the canonical public tool name."', win, f"{alias}: missing deprecationReason")

    def test_health_check_manifest_fields_exist(self) -> None:
        ts_health = self._window(self.ts, 'case "health_check"', span=900)
        for field in ("manifestVersion", "manifestHash", "deployedAt", "canonicalToolCount", "deprecatedAliasCount"):
            self.assertIn(field, ts_health, f"worker health_check missing {field}")
        py_health = self._window(self.py, "async def health_check", span=1600)
        for field in ("manifestVersion", "manifestHash", "deployedAt", "canonicalToolCount", "deprecatedAliasCount"):
            self.assertIn(field, py_health, f"server health_check missing {field}")


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False)
    sys.exit(0 if result.result.wasSuccessful() else 1)
