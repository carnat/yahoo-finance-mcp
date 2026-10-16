#!/usr/bin/env python3
"""health_check reports whether an /mcp request carried an Authorization header (2.6.0).

Drives the bundled Worker entry point (worker/src/index.ts) with real Request
objects in Node, so the header-to-health_check path is exercised end to end.
The header value itself must never appear in the response.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"

_HARNESS = r"""
const worker = (await import(process.argv[2])).default;
const SECRET = "Bearer test-secret-value-123";

async function health(headers) {
  const response = await worker.fetch(
    new Request("https://example.invalid/mcp", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call",
        params: { name: "system", arguments: { action: "health_check", params: {} } } }),
    }),
    // MCP_TOKEN matches SECRET so the Bearer header is a valid credential (a header
    // presented while MCP_TOKEN is unset is rejected with 401 since 2.9.0).
    { TOOL_MODE: "grouped", MCP_TOKEN: "test-secret-value-123" },
    { waitUntil() {}, passThroughOnException() {} },
  );
  const text = await response.text();
  const rpc = JSON.parse(text);
  const envelope = JSON.parse(rpc.result.content[0].text);
  return { raw: text, data: envelope.data };
}

const absent = await health({});
const present = await health({ Authorization: SECRET });
const blank = await health({ Authorization: "   " });
console.log(JSON.stringify({
  absent: absent.data.authorizationHeaderPresent,
  present: present.data.authorizationHeaderPresent,
  blank: blank.data.authorizationHeaderPresent,
  leaked: [absent, present, blank].some((r) => r.raw.includes("test-secret-value-123")),
  privacyScope: present.data.privacyScope,
}));
"""


def _run_harness() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".auth-header-test-") as tmp:
        bundle = Path(tmp) / "index.mjs"
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        subprocess.run(
            [str(ESBUILD), str(WORKER / "src" / "index.ts"), "--bundle", "--format=esm", "--platform=node",
             f"--outfile={bundle}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
        result = subprocess.run(
            [node, str(harness), bundle.as_uri()],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


class TestWorkerHealthAuthorizationHeader(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.result = _run_harness()

    def test_absent_header_reports_false(self) -> None:
        self.assertIs(self.result["absent"], False)

    def test_present_header_reports_true(self) -> None:
        self.assertIs(self.result["present"], True)

    def test_blank_header_reports_false(self) -> None:
        self.assertIs(self.result["blank"], False)

    def test_header_value_never_echoed(self) -> None:
        self.assertFalse(self.result["leaked"])

    def test_existing_contract_unchanged(self) -> None:
        self.assertEqual(self.result["privacyScope"], "public_market_data_only")


if __name__ == "__main__":
    unittest.main()
