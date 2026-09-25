#!/usr/bin/env python3
"""Worker edge cache for slow-changing Yahoo data.

Runs the bundled Worker as two isolates in one Miniflare instance, so they
share the Cache API but not module state, against a mocked Yahoo. The second
isolate must reuse cached statements, holders, and analyst recommendations,
and must refetch live quotes and empty answers.
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
MINIFLARE = WORKER / "node_modules" / "miniflare" / "dist" / "src" / "index.js"

_HARNESS = r"""
const [miniflareUrl, scriptPath] = process.argv.slice(-2);
const { Miniflare, convertV4MiniflareOptions } = await import(miniflareUrl);
const now = 1790000000;
const counts = {};
const count = (key) => { counts[key] = (counts[key] ?? 0) + 1; };

async function outbound(req) {
  const u = new URL(req.url);
  const sym = decodeURIComponent(u.pathname.split("/").pop());
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  if (u.pathname.includes("/fundamentals-timeseries/")) {
    count(`timeseries:${sym}`);
    if (sym === "EMPTY") {
      return Response.json({ timeseries: { result: [{ meta: { symbol: [sym], type: ["annualTotalRevenue"] }, timestamp: [] }], error: null } });
    }
    const types = (u.searchParams.get("type") ?? "").split(",").slice(0, 3);
    return Response.json({ timeseries: { error: null, result: types.map((t) => ({
      meta: { symbol: [sym], type: [t] },
      timestamp: [now],
      [t]: [{ asOfDate: "2025-09-27", periodType: "12M", currencyCode: "USD", reportedValue: { raw: 1000, fmt: "1k" } }],
    })) } });
  }
  if (u.pathname.includes("/quoteSummary/")) {
    const modules = u.searchParams.get("modules") ?? "";
    count(`quoteSummary:${modules}:${sym}`);
    const result = {};
    for (const m of modules.split(",")) {
      result[m] = m === "price" ? { regularMarketPrice: { raw: 101 }, currency: "USD", regularMarketTime: now }
        : m === "institutionOwnership" ? { ownershipList: [{ organization: "Vanguard", pctHeld: { raw: 0.08 }, position: { raw: 100 }, value: { raw: 1000 }, reportDate: { raw: now, fmt: "2026-06-30" } }] }
        : m === "recommendationTrend" ? { trend: [{ period: "0m", strongBuy: 5, buy: 10, hold: 3, sell: 0, strongSell: 0 }] }
        : {};
    }
    return Response.json({ quoteSummary: { result: [result], error: null } });
  }
  return new Response("{}", { status: 404 });
}

const worker = (name) => ({
  name, modules: true, scriptPath, compatibilityDate: "2026-09-21", compatibilityFlags: ["nodejs_als"],
  bindings: { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true" }, outboundService: outbound,
});
const mf = new Miniflare(convertV4MiniflareOptions({ workers: [worker("first"), worker("second")] }));
let id = 0;
const headers = { first: {}, second: {} };
let current = "first";
async function call(isolate, group, action, params, label) {
  const res = await isolate.fetch("https://x/mcp", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: ++id, method: "tools/call", params: { name: group, arguments: { action, params } } }),
  });
  if (label) headers[current][label] = res.headers.get("x-yahoo-cache");
  return (await res.json()).result.structuredContent;
}
async function run(isolate) {
  const out = {};
  out.statement = await call(isolate, "stock_fundamentals", "get_financial_statement", { ticker: "AAPL", financial_type: "income_stmt" }, "statement");
  await new Promise((r) => setTimeout(r, 1100));  // the timeseries window end moves to the next second
  out.statementAgain = await call(isolate, "stock_fundamentals", "get_financial_statement", { ticker: "AAPL", financial_type: "income_stmt" }, "statementAgain");
  out.empty = await call(isolate, "stock_fundamentals", "get_financial_statement", { ticker: "EMPTY", financial_type: "income_stmt" }, "empty");
  // Two concurrent requests in one isolate: each header counts only its own reads.
  [out.holders, out.recommendations] = await Promise.all([
    call(isolate, "stock_fundamentals", "get_ownership_holders", { ticker: "AAPL", holder_type: "institutional_holders" }, "holders"),
    call(isolate, "analyst_data", "get_analyst_recommendations", { ticker: "AAPL", recommendation_type: "recommendations" }, "recommendations"),
  ]);
  out.quote = await call(isolate, "stock_pricing", "get_market_quote", { ticker: "AAPL" }, "quote");
  out.health = await (await isolate.fetch("https://x/health")).json();
  return out;
}
const first = await run(await mf.getWorker("first"));
const afterFirst = { ...counts };
current = "second";
const second = await run(await mf.getWorker("second"));
await mf.dispose();
console.log(JSON.stringify({ first, second, afterFirst, afterSecond: counts, headers }));
"""


def _run_harness() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists() or not MINIFLARE.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    # workerd only loads scripts below the working directory.
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".edge-cache-test-") as tmp:
        entry = Path(tmp) / "entry.ts"
        bundle = Path(tmp) / "index.js"
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        entry.write_text(f'export {{ default }} from "{(WORKER / "src" / "index.ts").as_posix()}";\n', encoding="utf-8")
        subprocess.run(
            [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
             "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
        result = subprocess.run(
            [node, str(harness), MINIFLARE.as_uri(), str(bundle.relative_to(WORKER))],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=180,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


class TestWorkerYahooEdgeCache(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.result = _run_harness()

    def test_all_calls_succeed_in_both_isolates(self) -> None:
        for isolate in ("first", "second"):
            for call, payload in self.result[isolate].items():
                if call == "health":
                    continue
                with self.subTest(isolate=isolate, call=call):
                    self.assertTrue(payload.get("ok"), payload.get("error"))

    def test_timeseries_key_ignores_the_moving_window_end(self) -> None:
        # Two statement calls a second apart in one isolate make one Yahoo request.
        self.assertEqual(self.result["afterFirst"]["timeseries:AAPL"], 1)

    def test_slow_data_is_shared_through_the_edge_cache(self) -> None:
        after = self.result["afterSecond"]
        self.assertEqual(after["timeseries:AAPL"], 1)
        self.assertEqual(after["quoteSummary:institutionOwnership:AAPL"], 1)
        self.assertEqual(after["quoteSummary:recommendationTrend:AAPL"], 1)
        self.assertEqual(self.result["first"]["statement"]["data"], self.result["second"]["statement"]["data"])

    def test_live_quotes_and_empty_answers_are_not_edge_cached(self) -> None:
        after = self.result["afterSecond"]
        self.assertEqual(after["timeseries:EMPTY"], 2)
        quote_keys = [key for key in after if key.startswith("quoteSummary:") and "price" in key.split(":")[1].split(",")]
        self.assertTrue(quote_keys)
        for key in quote_keys:
            self.assertEqual(after[key], 2, key)

    def test_meta_reports_where_each_call_got_its_data(self) -> None:
        def summary(isolate: str, call: str) -> tuple[bool, str | None]:
            meta = self.result[isolate][call]["meta"]
            return meta["cacheHit"], meta["cacheSource"]

        self.assertEqual(summary("first", "statement"), (False, "upstream"))
        self.assertEqual(summary("first", "statementAgain"), (True, "memory"))
        self.assertEqual(summary("second", "statement"), (True, "edge"))
        self.assertEqual(summary("second", "holders"), (True, "edge"))
        self.assertIs(summary("second", "quote")[0], False)
        self.assertIn(summary("second", "quote")[1], ("upstream", "mixed"))

    def test_health_reports_cache_counters(self) -> None:
        first = self.result["first"]["health"]["yahooCache"]
        second = self.result["second"]["health"]["yahooCache"]
        self.assertEqual(first["scope"], "isolate")
        self.assertFalse(str(first["since"]).startswith("1970"), first["since"])
        # First isolate: statement, holders and recommendations are written to
        # the edge cache; the repeated statement call is a memory hit.
        self.assertEqual(first["edgeHits"], 0)
        self.assertEqual(first["edgeWrites"], 3)
        self.assertGreaterEqual(first["memoryHits"], 1)
        # Second isolate: the same three reads come from the edge cache.
        self.assertEqual(second["edgeHits"], 3)
        self.assertEqual(second["edgeWrites"], 0)
        self.assertGreater(second["bodyCacheChars"], 0)


_CACHE_UNIT = r"""
const { BoundedTtlCache } = await import(process.argv.at(-1));
const cache = new BoundedTtlCache(10, 25, (v) => v.length);
for (const key of ["a", "b", "c"]) cache.set(key, "x".repeat(10), 60_000);
const afterBudget = { size: cache.size, weight: cache.weight, a: cache.get("a") ?? null, c: cache.get("c") ?? null };
cache.set("big", "y".repeat(26), 60_000);
const oversized = { size: cache.size, big: cache.get("big") ?? null };
cache.delete("c");
console.log(JSON.stringify({ afterBudget, oversized, afterDelete: { size: cache.size, weight: cache.weight } }));
"""


class TestYahooCacheHeader(unittest.TestCase):
    """X-Yahoo-Cache reports where each request's Yahoo data came from."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.headers = _run_harness()["headers"]

    @staticmethod
    def _parse(header: str) -> dict[str, int]:
        return {key: int(value) for key, value in (part.split("=") for part in header.split(", "))}

    def test_first_isolate_fetches_then_reuses_memory(self) -> None:
        first = {label: self._parse(value) for label, value in self.headers["first"].items()}
        self.assertEqual(first["statement"]["upstream"], 1)
        self.assertEqual(first["statement"]["edge-miss"], 1)
        self.assertEqual(first["statement"]["edge-write"], 1)
        self.assertEqual(first["statementAgain"], {**first["statementAgain"], "memory": 1, "upstream": 0, "edge-hit": 0})

    def test_second_isolate_reads_the_edge_cache(self) -> None:
        second = {label: self._parse(value) for label, value in self.headers["second"].items()}
        for label in ("statement", "holders", "recommendations"):
            with self.subTest(label=label):
                self.assertEqual(second[label]["edge-hit"], 1)
                self.assertEqual(second[label]["upstream"], 0)
        self.assertGreaterEqual(second["quote"]["upstream"], 1)
        self.assertEqual(second["quote"]["edge-hit"], 0)

    def test_concurrent_requests_are_counted_separately(self) -> None:
        # holders and recommendations ran concurrently; one edge read each.
        second = {label: self._parse(value) for label, value in self.headers["second"].items()}
        self.assertEqual(sum(second["holders"].values()), 1)
        self.assertEqual(sum(second["recommendations"].values()), 1)


class TestBoundedTtlCacheWeight(unittest.TestCase):
    def test_total_weight_limit(self) -> None:
        node = os.environ.get("NODE_BINARY") or shutil.which("node")
        if node is None:
            raise unittest.SkipTest("node is required")
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "cache_unit.mjs"
            script.write_text(_CACHE_UNIT, encoding="utf-8")
            result = subprocess.run(
                [node, "--experimental-strip-types", "--no-warnings", str(script), (WORKER / "src" / "cache.ts").as_uri()],
                check=True, capture_output=True, text=True, timeout=60,
            )
        out = json.loads(result.stdout)
        # Three 10-char bodies exceed the 25-char budget; the oldest goes.
        self.assertEqual(out["afterBudget"], {"size": 2, "weight": 20, "a": None, "c": "x" * 10})
        # A value larger than the whole budget is not stored.
        self.assertEqual(out["oversized"], {"size": 2, "big": None})
        self.assertEqual(out["afterDelete"], {"size": 1, "weight": 10})


if __name__ == "__main__":
    unittest.main()
