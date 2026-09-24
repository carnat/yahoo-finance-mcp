#!/usr/bin/env python3
"""Worker handling of Yahoo throttling (HTTP 429), modelled on yfinance.

- A throttled data request is retried once on Yahoo's other API host.
- A throttled crumb request falls back to the other host, then lets the call
  proceed without a crumb and pauses crumb requests for a minute.
- When an endpoint needs the missing crumb, or both hosts throttle, the tool
  reports RATE_LIMIT with retryable:true rather than a provider error.
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
const [bundleUrl] = process.argv.slice(-1);
const now = 1790000000;
const chart = { chart: { result: [{ meta: { symbol: "AAPL", currency: "USD", exchangeTimezoneName: "America/New_York", regularMarketTime: now },
  timestamp: [now - 86400, now], indicators: { quote: [{ open: [1, 2], high: [1, 2], low: [1, 2], close: [1, 2], volume: [1, 1] }], adjclose: [{ adjclose: [1, 2] }] } }] } };
const holders = { quoteSummary: { error: null, result: [{ institutionOwnership: { ownershipList: [{ organization: "Vanguard", pctHeld: { raw: 0.08 }, position: { raw: 100 }, value: { raw: 1000 }, reportDate: { raw: now, fmt: "2026-06-30" } }] } }] } };

// Each scenario maps a request to a status; 200 answers with the fixture.
const scenarios = {
  chartFallsBackToOtherHost: (u) => u.pathname.includes("/chart/") && u.hostname.startsWith("query1") ? 429 : 200,
  chartThrottledOnBothHosts: (u) => u.pathname.includes("/chart/") ? 429 : 200,
  crumbThrottledEndpointWorks: (u) => u.pathname.includes("getcrumb") ? 429 : 200,
  crumbThrottledEndpointNeedsCrumb: (u) => u.pathname.includes("getcrumb") ? 429 : (u.searchParams.has("crumb") ? 200 : 401),
};

async function run(name) {
  const log = [];
  globalThis.fetch = async (req) => {
    const u = new URL(typeof req === "string" ? req : req.url);
    log.push(`${u.hostname}${u.pathname.split("/").slice(0, 4).join("/")}${u.searchParams.has("crumb") ? "+crumb" : ""}`);
    if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
    const status = scenarios[name](u);
    if (status !== 200) return new Response("Too Many Requests", { status });
    if (u.pathname.includes("getcrumb")) return new Response("crumb123");
    if (u.pathname.includes("/chart/")) return Response.json(chart);
    return Response.json(holders);
  };
  // A fresh module instance per scenario: crumb state starts empty.
  const worker = (await import(`${bundleUrl}?scenario=${name}`)).default;
  const env = { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true" };
  let id = 0;
  const call = async (group, action, params) => {
    const res = await worker.fetch(new Request("https://t/mcp", { method: "POST", body: JSON.stringify({
      jsonrpc: "2.0", id: ++id, method: "tools/call", params: { name: group, arguments: { action, params } },
    }) }), env);
    return (await res.json()).result.structuredContent;
  };
  const out = { log };
  if (name.startsWith("chart")) {
    out.result = await call("stock_pricing", "get_historical_prices", { ticker: "AAPL", period: "5d", interval: "1d" });
  } else {
    const args = { ticker: "AAPL", holder_type: "institutional_holders" };
    out.result = await call("stock_fundamentals", "get_ownership_holders", args);
    out.logAfterFirst = log.length;
    out.again = await call("stock_fundamentals", "get_ownership_holders", { ...args, ticker: "MSFT" });
  }
  return out;
}

const results = {};
for (const name of Object.keys(scenarios)) results[name] = await run(name);
console.log(JSON.stringify(results));
"""


def _run() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry = WORKER / ".throttling-test-entry.ts"
        bundle = Path(tmp) / "worker.mjs"
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        entry.write_text('export { default } from "./src/index.ts";\n', encoding="utf-8")
        try:
            subprocess.run(
                [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
                 "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
                cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
            )
        finally:
            entry.unlink(missing_ok=True)
        result = subprocess.run([node, str(harness), bundle.as_uri()], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout)


class TestWorkerYahooThrottling(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _run()

    def test_throttled_request_is_retried_on_the_other_host(self) -> None:
        out = self.out["chartFallsBackToOtherHost"]
        self.assertTrue(out["result"]["ok"], out["result"].get("error"))
        chart_hosts = [entry.split("/")[0] for entry in out["log"] if "/chart" in entry]
        self.assertEqual(chart_hosts, ["query1.finance.yahoo.com", "query2.finance.yahoo.com"])

    def test_throttling_on_both_hosts_is_a_retryable_rate_limit(self) -> None:
        result = self.out["chartThrottledOnBothHosts"]["result"]
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "RATE_LIMIT")
        self.assertIs(result["error"].get("retryable"), True)

    def test_throttled_crumb_lets_the_request_proceed_without_one(self) -> None:
        out = self.out["crumbThrottledEndpointWorks"]
        self.assertTrue(out["result"]["ok"], out["result"].get("error"))
        self.assertTrue(out["again"]["ok"], out["again"].get("error"))
        crumb_requests = [entry for entry in out["log"] if "getcrumb" in entry]
        # Both hosts were asked once; the second call skipped crumbs (cool-down).
        self.assertEqual([entry.split("/")[0] for entry in crumb_requests],
                         ["query2.finance.yahoo.com", "query1.finance.yahoo.com"])
        self.assertFalse(any("getcrumb" in entry for entry in out["log"][out["logAfterFirst"]:]))
        self.assertFalse(any(entry.endswith("+crumb") for entry in out["log"]))

    def test_endpoint_needing_the_missing_crumb_reports_a_rate_limit(self) -> None:
        result = self.out["crumbThrottledEndpointNeedsCrumb"]["result"]
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "RATE_LIMIT")
        self.assertIs(result["error"].get("retryable"), True)


if __name__ == "__main__":
    unittest.main()
