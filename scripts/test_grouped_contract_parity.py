#!/usr/bin/env python3
"""Grouped-mode parameter parity between the Worker and the local server.

Both runtimes publish the same grouped actions from tool_catalog.json, but
each derives action parameters from its own tool definitions. This test
bundles the Worker, reads its grouped tools/list, and requires every action
to expose the same parameter names and required parameters locally.
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
                 "--main-fields=module,main", f"--outfile={bundle}", "--log-level=error"],
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


if __name__ == "__main__":
    unittest.main()
