#!/usr/bin/env python3
"""Behavior guards for Worker doctrine safety envelope semantics."""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
TOOLS_TS = ROOT / "worker" / "src" / "tools.ts"
YAHOO_FINANCE_TS = ROOT / "worker" / "src" / "yahoo-finance.ts"
DEPLOYED_DISCOVERY = ROOT / "scripts" / "test_deployed_discovery.py"


def _node_json(source: str) -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None:
        raise unittest.SkipTest("node executable not found")
    result = subprocess.run(
        [node, "--experimental-strip-types", "--input-type=module", "-e", source],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr or result.stdout)
    return json.loads(result.stdout)


class TestWorkerDoctrineSafety(unittest.TestCase):
    def test_mcp_success_propagates_inner_failure_envelopes(self) -> None:
        payload = _node_json(
            """
            import assert from "node:assert/strict";
            import { mcpSuccess, setWorkerEnv } from "./worker/src/response.ts";

            setWorkerEnv({ MCP_ENVELOPE_V2: "true" });
            const raw = JSON.stringify({
              ok: false,
              data: null,
              meta: { source: "provider", providerStatus: "PROVIDER_BLOCKED", warnings: [] },
              error: { code: "PROVIDER_BLOCKED", message: "provider blocked" },
              diagnostics: { provider: "yahoo", dataSource: "INDICATIVE_ONLY" },
            });
            const out = JSON.parse(mcpSuccess("get_overnight_quote", raw, {
              warnings: [{ code: "DEPRECATED_ALIAS", message: "Use canonical tool instead." }],
              metaExtra: { doctrineUse: "DIAGNOSTICS_ONLY" },
            }));

            assert.equal(out.ok, false);
            assert.equal(out.error.code, "PROVIDER_BLOCKED");
            assert.equal(out.meta.tool, "get_overnight_quote");
            assert.equal(out.meta.providerStatus, "PROVIDER_BLOCKED");
            assert.equal(out.meta.doctrineUse, "DIAGNOSTICS_ONLY");
            assert.equal(out.meta.warnings[0].code, "DEPRECATED_ALIAS");
            assert.equal(out.diagnostics.dataSource, "INDICATIVE_ONLY");
            console.log(JSON.stringify(out));
            """
        )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "PROVIDER_BLOCKED")

    def test_mcp_success_propagates_inner_failure_without_data_field(self) -> None:
        payload = _node_json(
            """
            import assert from "node:assert/strict";
            import { mcpSuccess, setWorkerEnv } from "./worker/src/response.ts";

            setWorkerEnv({ MCP_ENVELOPE_V2: "true" });
            const raw = JSON.stringify({
              ok: false,
              error: { code: "UNSUPPORTED_QUERY_TYPE", message: "unsupported query" },
            });
            const out = JSON.parse(mcpSuccess("query_sec_filing_index", raw));

            assert.equal(out.ok, false);
            assert.equal(out.data, null);
            assert.equal(out.error.code, "UNSUPPORTED_QUERY_TYPE");
            console.log(JSON.stringify(out));
            """
        )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "UNSUPPORTED_QUERY_TYPE")

    def test_mcp_success_converts_legacy_error_true_payloads(self) -> None:
        payload = _node_json(
            """
            import assert from "node:assert/strict";
            import { mcpSuccess, setWorkerEnv } from "./worker/src/response.ts";

            setWorkerEnv({ MCP_ENVELOPE_V2: "true" });
            const raw = JSON.stringify({
              error: true,
              code: "NO_OPTIONS_DATA",
              message: "No option expirations",
              ticker: "AAPL",
            });
            const out = JSON.parse(mcpSuccess("get_option_chain", raw));

            assert.equal(out.ok, false);
            assert.equal(out.data, null);
            assert.equal(out.error.code, "NO_OPTIONS_DATA");
            console.log(JSON.stringify(out));
            """
        )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "NO_OPTIONS_DATA")

    def test_deprecated_alias_set_is_derived_from_alias_map(self) -> None:
        tools = TOOLS_TS.read_text(encoding="utf-8")
        self.assertIn('get_historical_stock_prices: "get_historical_prices"', tools)
        self.assertIn("const DEPRECATED_ALIAS_NAMES = new Set(Object.keys(TOOL_ALIASES));", tools)
        self.assertNotIn("const DEPRECATED_ALIAS_NAMES = new Set<string>();", tools)

    def test_deployed_smoke_covers_alias_and_doctrine_status_behavior(self) -> None:
        smoke = DEPLOYED_DISCOVERY.read_text(encoding="utf-8")
        self.assertIn("get_historical_stock_prices", smoke)
        self.assertIn("deprecatedTool", smoke)
        self.assertIn("DEPRECATED_ALIAS", smoke)
        self.assertIn("doctrineToolStatus", smoke)

    def test_press_releases_exposes_8k_without_ex99_status(self) -> None:
        worker = YAHOO_FINANCE_TS.read_text(encoding="utf-8")
        self.assertIn("SEC_8K_FOUND_EX99_NOT_FOUND", worker)
        self.assertIn("secEvidence", worker)
        self.assertIn("sec8kWithoutEx99Count", worker)
        self.assertIn("filingsSearched", worker)

    def test_press_release_ex99_resolver_uses_index_exhibits(self) -> None:
        worker = YAHOO_FINANCE_TS.read_text(encoding="utf-8")
        start = worker.index("async function resolveEx991Url")
        end = worker.index("/** Parse inline XBRL", start)
        resolver = worker[start:end]
        self.assertIn("edgarListExhibitsFromIndex(indexUrl)", resolver)
        self.assertIn('type.startsWith("EX-99")', resolver)
        self.assertIn("PRESS RELEASE|EARNINGS RELEASE|RESULTS RELEASE", resolver)
        self.assertIn("edgarBuildFilingUrls(cikInt, accessionNumber, String(ex991.document))", resolver)

    def test_section_markdown_success_is_not_decision_grade(self) -> None:
        worker = YAHOO_FINANCE_TS.read_text(encoding="utf-8")
        self.assertIn("SECTION_MARKDOWN_UNVERIFIED", worker)
        self.assertIn("LIVE_SECTION_EXTRACTION_UNRELIABLE", worker)
        self.assertIn('confidence: "NOT_DECISION_GRADE"', worker)
        self.assertIn("decisionGrade: false", worker)
        self.assertIn('doctrineUse: "BLOCKED"', worker)

    def test_sec_filing_fact_preserves_xbrl_context_metadata(self) -> None:
        worker = YAHOO_FINANCE_TS.read_text(encoding="utf-8")
        tools = TOOLS_TS.read_text(encoding="utf-8")
        self.assertIn("xbrlContext: payload.xbrlContext ?? null", worker)
        self.assertIn("XBRL_CONTEXT_METADATA_UNAVAILABLE", worker)
        self.assertIn("function buildXbrlFactContext", worker)
        for field in ("periodStart", "periodEnd", "instant", "durationDays", "fiscalPeriod", "fiscalYear", "form", "frame", "dimensions"):
            self.assertIn(field, worker)
        self.assertIn("function xbrlSourceEvidence", tools)
        self.assertIn("sourceEvidence", tools)
        self.assertIn("xbrlContext: parsed.xbrlContext ?? null", tools)
        self.assertIn("const sourceEvidence = xbrlSourceEvidence(parsed)", tools)
        self.assertIn('String(b.end ?? "").localeCompare(String(a.end ?? ""))', worker)

    def test_overnight_quote_is_explicitly_diagnostics_only(self) -> None:
        tools = TOOLS_TS.read_text(encoding="utf-8")
        worker = YAHOO_FINANCE_TS.read_text(encoding="utf-8")
        smoke = DEPLOYED_DISCOVERY.read_text(encoding="utf-8")
        self.assertIn("Deprecated diagnostics-only Yahoo extended-hours proxy", tools)
        self.assertIn('failureMode: "YAHOO_EXTENDED_HOURS_PROXY_ONLY"', tools)
        self.assertIn("OVERNIGHT_DIAGNOSTIC_FIELDS", worker)
        self.assertIn('dataKind: "yahoo_extended_hours_proxy"', worker)
        self.assertIn('decisionGrade: false', worker)
        self.assertIn('doctrineUse: "DIAGNOSTICS_ONLY"', worker)
        self.assertIn("TRUE_OVERNIGHT_PROVIDER_REMOVED", worker)
        self.assertIn("yahoo_extended_hours_proxy", smoke)

    def test_query_sec_filing_index_no_longer_quarantined(self) -> None:
        tools = TOOLS_TS.read_text(encoding="utf-8")
        smoke = DEPLOYED_DISCOVERY.read_text(encoding="utf-8")
        self.assertNotIn("ENVELOPE_SEMANTICS_UNDER_VERIFICATION", tools)
        self.assertNotIn('"query_sec_filing_index": ("DEGRADED", "VERIFY_ONLY")', smoke)
        self.assertIn("UNSUPPORTED_QUERY_TYPE", smoke)


if __name__ == "__main__":
    unittest.main(verbosity=2)
