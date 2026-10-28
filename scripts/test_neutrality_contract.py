#!/usr/bin/env python3
"""Neutrality contract (3.0.0, Phase 6 exit).

Both runtimes' tool listings, in expanded and grouped mode, must not carry
doctrine vocabulary in any name, description, enum or schema key, and must not
advertise a field that 3.0.0 removed (the 2.8.0 N1-N3 and 2.27.0 F5/F45/F61
deprecations).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
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

DOCTRINE = re.compile(r"doctrine", re.IGNORECASE)

# Keys removed everywhere in 3.0.0.
REMOVED_KEYS = {
    "doctrineUse", "g2", "opportunity", "scenarioWeights", "selectedMethod", "selectedMultiple",
    "inferredTag", "tagNote", "gatePass", "gateThresholdUsd", "liquidityGatePass",
    "t1_inputs", "t2_inputs", "t4_inputs", "t5_inputs", "putVolVs10dAvg",
}
# Keys removed from one tool's output only (other tools keep a field of the same name).
REMOVED_KEYS_BY_TOOL = {
    "calculate_price_target_distance": {"bracket", "tag", "priceTarget", "action"},
    "analyze_options_flow_window": {"ivPctile", "fieldNotes"},
    "get_analyst_rating_changes": {"upgrades30d", "downgrades30d", "initiations30d"},
}

_LIST_TOOLS = """
const worker = (await import(process.argv[1])).default;
const out = {};
for (const mode of ["expanded", "grouped"]) {
  const response = await worker.fetch(
    new Request("https://parity.invalid/mcp", {
      method: "POST",
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }),
    }),
    { TOOL_MODE: mode, MCP_ENVELOPE_V2: "true" },
  );
  out[mode] = (await response.json()).result.tools;
}
console.log(JSON.stringify(out));
"""


def _worker_listings() -> dict[str, list[dict]]:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry = WORKER / ".neutrality-entry.ts"
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
            [node, "--input-type=module", "-e", _LIST_TOOLS, bundle.as_uri()],
            check=True, capture_output=True, text=True, timeout=60,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


def _python_tool(tool) -> dict:
    dump = tool.model_dump(mode="json", by_alias=True, exclude_none=True) if hasattr(tool, "model_dump") else dict(tool)
    return dump


def _python_listings() -> dict[str, list[dict]]:
    import server

    expanded = asyncio.run(server.yfinance_server.list_tools())
    grouped = asyncio.run(server._build_grouped_server().list_tools())
    return {"expanded": [_python_tool(t) for t in expanded], "grouped": [_python_tool(t) for t in grouped]}


def _keys(node, path="") -> list[tuple[str, str]]:
    """Every (path, key) of every dict in the tree."""
    out: list[tuple[str, str]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            out.append((path, key))
            out.extend(_keys(value, f"{path}.{key}"))
    elif isinstance(node, list):
        for i, value in enumerate(node):
            out.extend(_keys(value, f"{path}[{i}]"))
    return out


def _output_schema(tool: dict) -> dict:
    return tool.get("outputSchema") or tool.get("output_schema") or {}


class NeutralityContract(unittest.TestCase):
    listings: dict[str, dict[str, list[dict]]] = {}

    @classmethod
    def setUpClass(cls) -> None:
        cls.listings = {"worker": _worker_listings(), "python": _python_listings()}

    def _each(self):
        for runtime, modes in self.listings.items():
            for mode, tools in modes.items():
                yield runtime, mode, tools

    def test_no_doctrine_term_in_any_listing(self) -> None:
        for runtime, mode, tools in self._each():
            hits = [t.get("name") for t in tools if DOCTRINE.search(json.dumps(t))]
            self.assertEqual(hits, [], f"{runtime}/{mode}: doctrine vocabulary in tools {hits}")

    def test_no_removed_field_in_any_output_schema(self) -> None:
        for runtime, mode, tools in self._each():
            for tool in tools:
                schema = _output_schema(tool)
                found = sorted({key for _, key in _keys(schema) if key in REMOVED_KEYS})
                self.assertEqual(found, [], f"{runtime}/{mode}/{tool.get('name')}: removed fields {found}")

    def test_no_tool_specific_removed_field(self) -> None:
        for runtime, mode, tools in self._each():
            by_name = {t.get("name"): t for t in tools}
            for name, removed in REMOVED_KEYS_BY_TOOL.items():
                tool = by_name.get(name)
                if tool is None:
                    continue  # grouped mode lists groups, not these names
                props = (_output_schema(tool).get("properties") or {})
                self.assertEqual(sorted(set(props) & removed), [], f"{runtime}/{mode}/{name}")

    def test_no_removed_field_named_in_descriptions(self) -> None:
        pattern = re.compile(r"\b(" + "|".join(sorted(REMOVED_KEYS - {"g2", "opportunity"})) + r")\b")
        for runtime, mode, tools in self._each():
            for tool in tools:
                text = json.dumps(tool)
                self.assertIsNone(pattern.search(text), f"{runtime}/{mode}/{tool.get('name')}: {pattern.search(text)}")


if __name__ == "__main__":
    unittest.main()
