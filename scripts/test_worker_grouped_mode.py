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
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from source_text import definition_source, python_runtime_source, source_between, worker_runtime_source  # noqa: E402


ROOT = pathlib.Path(__file__).resolve().parents[1]
CATALOG = ROOT / "tool_catalog.json"
MCP_TS = ROOT / "worker" / "src" / "mcp.ts"
CATALOG_TS = ROOT / "worker" / "src" / "tool-catalog.ts"
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy-worker.yml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


class TestWorkerGroupedMode(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
        # Every Worker / Python runtime module (scripts/source_text.py): markers are
        # found wherever the code lives, and "not present" checks cover all modules.
        cls.tools_ts = worker_runtime_source()
        cls.mcp_ts = MCP_TS.read_text(encoding="utf-8")
        cls.catalog_ts = CATALOG_TS.read_text(encoding="utf-8")
        cls.yahoo_ts = worker_runtime_source()
        cls.deploy_workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")
        cls.ci_workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        cls.server = python_runtime_source()

    def test_catalog_has_expected_group_surface(self) -> None:
        groups = self.catalog["groups"]
        self.assertEqual(len(groups), 12)
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
                "evidence",
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
        self.assertIn("import { callVisibleToolResult, listVisibleTools }", self.mcp_ts)
        self.assertRegex(self.mcp_ts, r"tools/list[\s\S]*listVisibleTools\(\)")
        self.assertRegex(self.mcp_ts, r"tools/call[\s\S]*callVisibleToolResult\(p\.name, p\.arguments \?\? \{\}\)")

    def test_grouped_call_validates_then_delegates_to_expanded_action(self) -> None:
        self.assertIn("export function isGroupedMode()", self.tools_ts)
        self.assertIn("const GROUPED_TOOLS", self.tools_ts)
        self.assertIn("const GROUPED_ACTIONS", self.tools_ts)
        body = source_between("worker", "export async function callVisibleTool", "async function _dispatchTool")
        self.assertIn("validateGroupedActionParams(action, actionParams)", body)
        self.assertIn("if (validationFailure) return { text: validationFailure }", body)
        self.assertIn("return callToolResult(action, actionParams)", body)

    def test_grouped_discovery_has_action_specific_schemas_and_read_only_hints(self) -> None:
        self.assertIn("function groupedInputSchema(", self.tools_ts)
        self.assertIn("oneOf,", self.tools_ts)
        self.assertIn("const: action", self.tools_ts)
        self.assertIn("additionalProperties: false", self.tools_ts)
        self.assertIn('requiredParams.length > 0 ? ["action", "params"]', self.tools_ts)
        for field, value in (
            ("readOnlyHint", "true"),
            ("destructiveHint", "false"),
            ("idempotentHint", "true"),
            ("openWorldHint", "true"),
        ):
            self.assertIn(f"{field}: {value}", self.tools_ts)
        self.assertIn("function annotationsForTool(", self.tools_ts)
        self.assertIn('["system", "health_check"].includes(name)', self.tools_ts)
        self.assertNotIn('"get_manifest_diagnostics"', self.tools_ts)
        self.assertIn("annotations: annotationsForTool(group.name)", self.tools_ts)

    def test_grouped_validation_is_action_specific_and_recoverable(self) -> None:
        body = definition_source("worker", "validateGroupedActionParams")
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
        body = definition_source("worker", "matchesParamSchema")
        self.assertNotIn("schema.enum", body)
        self.assertIn("schema.type", body)
        self.assertIn('"UNSUPPORTED_QUERY_TYPE"', self.yahoo_ts)

    def test_legacy_text_errors_are_not_wrapped_as_success(self) -> None:
        body = source_between("worker", "export async function callTool(", "export async function callVisibleTool")
        self.assertIn("legacyToolFailure(raw)", body)
        self.assertIn("return mcpFailureResult(name, legacyFailure.code, legacyFailure.message", body)

    def test_company_news_rejects_empty_symbols_before_collection(self) -> None:
        body = definition_source("worker", "getCompanyNews")
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
