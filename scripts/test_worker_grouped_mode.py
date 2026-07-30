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
YAHOO_TS = ROOT / "worker" / "src" / "yahoo-finance.ts"
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy-worker.yml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
SERVER = ROOT / "server.py"


class TestWorkerGroupedMode(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
        cls.tools_ts = TOOLS_TS.read_text(encoding="utf-8")
        cls.mcp_ts = MCP_TS.read_text(encoding="utf-8")
        cls.catalog_ts = CATALOG_TS.read_text(encoding="utf-8")
        cls.yahoo_ts = YAHOO_TS.read_text(encoding="utf-8")
        cls.deploy_workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")
        cls.ci_workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        cls.server = SERVER.read_text(encoding="utf-8")

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

    def test_worker_imports_shared_catalog_without_generated_mirror(self) -> None:
        self.assertIn('import catalog from "../../tool_catalog.json"', self.catalog_ts)
        self.assertIn("Object.entries(catalog.groups)", self.catalog_ts)
        self.assertNotIn('"name": "sec_filings"', self.catalog_ts)

    def test_sec_catalog_documents_canonical_params(self) -> None:
        description = self.catalog["groups"]["sec_filings"]["description"]
        self.assertIn(
            "list_sec_material_filings: Material only. Params: ticker, forms, limit",
            description,
        )
        self.assertIn(
            "get_sec_filing_section_markdown: Unverified degraded section Markdown. "
            "Params: ticker, section, filing_type, filing_index, max_chars",
            description,
        )
        self.assertIn(
            "list_sec_filing_exhibits: Exhibits list. Params: ticker, accessionNumber",
            description,
        )
        self.assertNotIn("list_sec_material_filings: Material only. Params: ticker, form_types", description)
        self.assertNotIn("list_sec_filing_exhibits: Exhibits list. Params: ticker, accession_number", description)

    def test_mcp_uses_visible_tools_for_list_and_call(self) -> None:
        self.assertIn("import { callVisibleTool, listVisibleTools }", self.mcp_ts)
        self.assertRegex(self.mcp_ts, r"tools/list[\s\S]*listVisibleTools\(\)")
        self.assertRegex(self.mcp_ts, r"tools/call[\s\S]*callVisibleTool\(p\.name, p\.arguments \?\? \{\}\)")

    def test_grouped_call_validates_then_delegates_to_expanded_action(self) -> None:
        self.assertIn("export function isGroupedMode()", self.tools_ts)
        self.assertIn("const GROUPED_TOOLS", self.tools_ts)
        self.assertIn("const GROUPED_ACTIONS", self.tools_ts)
        call_start = self.tools_ts.index("export async function callVisibleTool")
        call_end = self.tools_ts.index("async function _dispatchTool", call_start)
        body = self.tools_ts[call_start:call_end]
        self.assertIn("validateGroupedActionParams(action, actionParams)", body)
        self.assertIn("if (validationFailure) return validationFailure", body)
        self.assertIn("return callTool(action, actionParams)", body)

    def test_grouped_validation_is_action_specific_and_recoverable(self) -> None:
        validation_start = self.tools_ts.index("function validateGroupedActionParams")
        validation_end = self.tools_ts.index("function legacyToolFailure", validation_start)
        body = self.tools_ts[validation_start:validation_end]
        self.assertIn("TOOLS.find((tool) => tool.name === action)", body)
        for field in (
            "missingParams",
            "invalidParams",
            "unexpectedParams",
            "expectedParams",
            "CORRECT_TOOL_PARAMS",
        ):
            self.assertIn(field, body)

    def test_grouped_validation_leaves_semantic_enums_to_handlers(self) -> None:
        validation_start = self.tools_ts.index("function matchesParamSchema")
        validation_end = self.tools_ts.index("function validateGroupedActionParams", validation_start)
        body = self.tools_ts[validation_start:validation_end]
        self.assertNotIn("schema.enum", body)
        self.assertIn("schema.type", body)
        self.assertIn('"UNSUPPORTED_QUERY_TYPE"', self.yahoo_ts)

    def test_legacy_text_errors_are_not_wrapped_as_success(self) -> None:
        call_start = self.tools_ts.index("export async function callTool")
        call_end = self.tools_ts.index("export async function callVisibleTool", call_start)
        body = self.tools_ts[call_start:call_end]
        self.assertIn("legacyToolFailure(raw)", body)
        self.assertIn("return mcpFailure(name, legacyFailure.code, legacyFailure.message)", body)

    def test_company_news_rejects_empty_symbols_before_collection(self) -> None:
        start = self.yahoo_ts.index("export async function getCompanyNews")
        end = self.yahoo_ts.index("export async function searchCompanyNews", start)
        body = self.yahoo_ts[start:end]
        guard = body.index('code: "INPUT_VALIDATION_ERROR"')
        collect = body.index("collectCompanyEvents(")
        self.assertLess(guard, collect)
        self.assertIn("ticker.length === 0", body)
        self.assertIn('item.trim() === ""', body)

    def test_grouped_mode_is_the_universal_default(self) -> None:
        self.assertIn('getWorkerVar("TOOL_MODE") ?? "grouped"', self.tools_ts)
        self.assertIn('os.environ.get("TOOL_MODE", "grouped")', self.server)
        self.assertIn("DEPLOY_TOOL_MODE: ${{ vars.TOOL_MODE || 'grouped' }}", self.deploy_workflow)
        self.assertIn("Build candidate secrets file", self.deploy_workflow)
        self.assertIn("worker_version_promotion.py write-secrets", self.deploy_workflow)
        self.assertIn("TOOL_MODE: ${{ env.DEPLOY_TOOL_MODE }}", self.deploy_workflow)
        self.assertIn('--secrets-file "$RUNNER_TEMP/worker-secrets.json"', self.deploy_workflow)
        self.assertNotIn("wrangler secret put TOOL_MODE", self.deploy_workflow)
        self.assertNotIn("DEPLOY_GROUPED_SMOKE", self.deploy_workflow)
        self.assertRegex(
            self.ci_workflow,
            r"(?ms)- name: Universal alias tests\n\s+if: \$\{\{ vars\.TOOL_MODE == 'expanded' \}\}",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
