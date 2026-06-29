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
              meta: { source: "alpaca", providerStatus: "PROVIDER_FORBIDDEN" },
              error: { code: "PROVIDER_FORBIDDEN", message: "subscription blocked" },
              diagnostics: { provider: "yahoo", dataSource: "INDICATIVE_ONLY" },
            });
            const out = JSON.parse(mcpSuccess("get_overnight_quote", raw, {
              metaExtra: { doctrineUse: "DIAGNOSTICS_ONLY" },
            }));

            assert.equal(out.ok, false);
            assert.equal(out.error.code, "PROVIDER_FORBIDDEN");
            assert.equal(out.meta.tool, "get_overnight_quote");
            assert.equal(out.meta.providerStatus, "PROVIDER_FORBIDDEN");
            assert.equal(out.meta.doctrineUse, "DIAGNOSTICS_ONLY");
            assert.equal(out.diagnostics.dataSource, "INDICATIVE_ONLY");
            console.log(JSON.stringify(out));
            """
        )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "PROVIDER_FORBIDDEN")

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
