#!/usr/bin/env python3
"""Static guards for Worker grouped-mode discovery and routing.

These are intentionally cheap: TypeScript still gets type-checked separately,
while this test pins the production Worker to the grouped-mode contract:
`tools/list` uses the visible-tool filter and grouped `tools/call` delegates to
the existing expanded `callTool(action, params)` path.
"""

from __future__ import annotations

import json
import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
CATALOG = ROOT / "tool_catalog.json"
TOOLS_TS = ROOT / "worker" / "src" / "tools.ts"
MCP_TS = ROOT / "worker" / "src" / "mcp.ts"
CATALOG_TS = ROOT / "worker" / "src" / "tool-catalog.ts"
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy-worker.yml"


class TestWorkerGroupedMode(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
        cls.tools_ts = TOOLS_TS.read_text(encoding="utf-8")
        cls.mcp_ts = MCP_TS.read_text(encoding="utf-8")
        cls.catalog_ts = CATALOG_TS.read_text(encoding="utf-8")
        cls.deploy_workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")

    def test_catalog_has_expected_group_surface(self) -> None:
        groups = self.catalog["groups"]
        self.assertEqual(len(groups), 11)
        self.assertEqual(
            set(groups),
            {
                "stock_pricing",
                "stock_fundamentals",
                "options_analysis",
                "analyst_data",
                "news_events",
                "sec_filings",
                "sec_extractors",
                "earnings_intelligence",
                "screening",
                "system",
                "thai_funds",
            },
        )
        self.assertIn("get_market_quote", groups["stock_pricing"]["actions"])
        self.assertIn("health_check", groups["system"]["actions"])
        self.assertIn("extract_exposure", groups["sec_extractors"]["actions"])
        self.assertIn("get_thai_fund_nav", groups["thai_funds"]["actions"])

    def test_worker_generated_catalog_matches_json(self) -> None:
        for group_name, group_def in self.catalog["groups"].items():
            self.assertIn(f'"name": "{group_name}"', self.catalog_ts)
            for action in group_def["actions"]:
                self.assertIn(f'"{action}"', self.catalog_ts)

    def test_mcp_uses_visible_tools_for_list_and_call(self) -> None:
        self.assertIn("import { callVisibleTool, listVisibleTools }", self.mcp_ts)
        self.assertRegex(self.mcp_ts, r"tools/list[\s\S]*listVisibleTools\(\)")
        self.assertRegex(self.mcp_ts, r"tools/call[\s\S]*callVisibleTool\(p\.name, p\.arguments \?\? \{\}\)")

    def test_grouped_call_delegates_to_expanded_action(self) -> None:
        self.assertIn("export function isGroupedMode()", self.tools_ts)
        self.assertIn("const GROUPED_TOOLS", self.tools_ts)
        self.assertIn("const GROUPED_ACTIONS", self.tools_ts)
        match = re.search(
            r"export async function callVisibleTool[\s\S]+?return callTool\(action, \(params as Record<string, unknown> \| undefined\) \?\? \{\}\);",
            self.tools_ts,
        )
        self.assertIsNotNone(match)

    def test_deploy_can_wire_grouped_mode_to_worker(self) -> None:
        self.assertIn("DEPLOY_TOOL_MODE: ${{ vars.TOOL_MODE || 'expanded' }}", self.deploy_workflow)
        self.assertIn("Build candidate secrets file", self.deploy_workflow)
        self.assertIn("worker_version_promotion.py write-secrets", self.deploy_workflow)
        self.assertIn("TOOL_MODE: ${{ env.DEPLOY_TOOL_MODE }}", self.deploy_workflow)
        self.assertIn('--secrets-file "$RUNNER_TEMP/worker-secrets.json"', self.deploy_workflow)
        self.assertNotIn("wrangler secret put TOOL_MODE", self.deploy_workflow)
        self.assertIn("DEPLOY_GROUPED_SMOKE", self.deploy_workflow)


if __name__ == "__main__":
    unittest.main(verbosity=2)
