#!/usr/bin/env python3
"""Grouped-mode contract parity between the Worker and the local server.

Both runtimes publish the same grouped actions from tool_catalog.json, but
each derives action parameters from its own tool definitions and wraps tool
results with its own envelope code. These tests require every action to
expose the same parameters locally and every raw tool result to be wrapped
into the same V2 envelope (ok, data, error code/message) as on the Worker.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

_LIST_GROUPED_SCHEMAS = """
const worker = (await import(process.argv[1])).default;
const response = await worker.fetch(
  new Request("https://parity.invalid/mcp", {
    method: "POST",
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }),
  }),
  { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true" },
);
const out = {};
for (const tool of (await response.json()).result.tools) {
  for (const branch of tool.inputSchema.oneOf ?? []) {
    const params = branch.properties.params;
    out[`${tool.name}.${branch.properties.action.const}`] = {
      params: Object.keys(params.properties ?? {}).sort(),
      required: [...(params.required ?? [])].sort(),
    };
  }
}
console.log(JSON.stringify(out));
"""


def _worker_grouped_schemas() -> dict[str, dict[str, list[str]]]:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry = WORKER / ".parity-entry.ts"
        bundle = Path(tmp) / "worker.mjs"
        entry.write_text('export { default } from "./src/index.ts";\n', encoding="utf-8")
        try:
            subprocess.run(
                [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
                 "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
                cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
            )
        finally:
            entry.unlink(missing_ok=True)
        result = subprocess.run(
            [node, "--input-type=module", "-e", _LIST_GROUPED_SCHEMAS, bundle.as_uri()],
            check=True, capture_output=True, text=True, timeout=60,
        )
    return json.loads(result.stdout)


def _python_grouped_schemas() -> dict[str, dict[str, list[str]]]:
    import server

    out: dict[str, dict[str, list[str]]] = {}
    for tool in asyncio.run(server._build_grouped_server().list_tools()):
        # mcp 2.x exposes snake_case model attributes; 1.x uses camelCase.
        schema = getattr(tool, "input_schema", None) or getattr(tool, "inputSchema", None) or {}
        for branch in schema.get("oneOf", []):
            params = branch["properties"]["params"]
            out[f'{tool.name}.{branch["properties"]["action"]["const"]}'] = {
                "params": sorted(params.get("properties", {})),
                "required": sorted(params.get("required", [])),
            }
    return out


class TestGroupedContractParity(unittest.TestCase):
    def test_every_grouped_action_has_the_same_parameters(self) -> None:
        worker = _worker_grouped_schemas()
        local = _python_grouped_schemas()
        self.assertEqual(sorted(worker), sorted(local), "grouped action sets differ")
        mismatches = {
            action: {"worker": worker[action], "local": local[action]}
            for action in worker
            if worker[action] != local[action]
        }
        self.assertEqual(mismatches, {}, json.dumps(mismatches, indent=2))


_ENVELOPE_SAMPLES = {
    "object": {"lastPrice": 101.5, "currency": "USD"},
    "list": ["2026-09-25", "2026-10-02"],
    "fact": {"revenue": {"value": 391035000000, "unit": "USD"}, "status": "FOUND"},
    "price_bars": [{"date": "2026-09-24T00:00:00.000Z", "open": 60.5, "high": 61.1, "low": 60.3, "close": 61.0, "volume": 7648430}],
    "range": {"targetPrice": {"low": 250.0, "high": 400.0}},
    "legacy_error": {"error": True, "code": "RATE_LIMIT", "message": "slow down"},
    "envelope": {"ok": True, "data": {"x": 1}, "meta": {"tool": "inner"}, "error": None},
}

_WORKER_ENVELOPES = """
import { mcpSuccess, setWorkerEnv } from "./worker/src/response.ts";
setWorkerEnv({ MCP_ENVELOPE_V2: "true" });
const samples = JSON.parse(process.argv[1]);
const out = {};
for (const [name, raw] of Object.entries(samples)) out[name] = JSON.parse(mcpSuccess("sample_tool", JSON.stringify(raw)));
console.log(JSON.stringify(out));
"""


def _comparable(envelope: dict) -> dict:
    error = envelope.get("error") if isinstance(envelope.get("error"), dict) else None
    return {
        "ok": envelope.get("ok"),
        "data": envelope.get("data"),
        "error": {"code": error.get("code"), "message": error.get("message")} if error else None,
    }


class TestEnvelopeParity(unittest.TestCase):
    def test_raw_tool_results_are_wrapped_like_the_worker(self) -> None:
        node = os.environ.get("NODE_BINARY") or shutil.which("node")
        if node is None:
            raise unittest.SkipTest("node is required")
        result = subprocess.run(
            [node, "--experimental-strip-types", "--no-warnings", "--input-type=module",
             "-e", _WORKER_ENVELOPES, json.dumps(_ENVELOPE_SAMPLES)],
            cwd=ROOT, check=True, capture_output=True, text=True, timeout=60,
        )
        worker = json.loads(result.stdout)

        from unittest.mock import patch
        from yfmcp.envelope import _envelope_tool_result

        with patch.dict(os.environ, {"MCP_ENVELOPE_V2": "true"}):
            local = {
                name: json.loads(_envelope_tool_result("sample_tool", json.dumps(raw)))
                for name, raw in _ENVELOPE_SAMPLES.items()
            }
        for name in _ENVELOPE_SAMPLES:
            with self.subTest(sample=name):
                self.assertEqual(_comparable(local[name]), _comparable(worker[name]))


if __name__ == "__main__":
    unittest.main()
