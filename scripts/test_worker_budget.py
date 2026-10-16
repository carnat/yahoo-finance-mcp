#!/usr/bin/env python3
"""Worker per-invocation subrequest budget guard (2.11.0, Phase 1 release 3c).

Bundles worker/src with esbuild and drives request-context.ts, the edge-cache
wrappers and the MCP layer in one Node process, with a fetch spy and a fake
Cache API (`caches.default`). Nothing reaches the network.

- 45 provider fetches succeed in a request scope; the 46th throws
  BudgetExhaustedError without calling fetch.
- Every redirect hop counts.
- Cache match/put count, and once the budget is spent are skipped (a miss, a
  no-op) without consuming budget or flagging the invocation.
- Nested scopes share the root scope's budget; a refusal is charged to the
  scope that asked.
- Outside a scope nothing is counted and nothing is refused.
- Cloudflare's own "Too many subrequests" error counts as a budget refusal, and
  in news maps a source to PROVIDER_BUDGET_EXHAUSTED (failureMode
  WORKER_SUBREQUEST_LIMIT unchanged).
- One meta.warnings row (exact text) when the guard fired, none otherwise;
  a starved single tool call fails with PROVIDER_BUDGET_EXHAUSTED; a starved
  batch ticker carries the per-ticker code.
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

WARNING = {
    "code": "PROVIDER_BUDGET_EXHAUSTED",
    "message": "Stopped starting new upstream requests at the per-request subrequest budget (45 of 50); some sources were not queried.",
    "severity": "warning",
}

_ENTRY = """
export * as requestContext from "{src}/request-context.ts";
export * as yahooFinance from "{src}/yahoo-finance.ts";
export * as response from "{src}/response.ts";
export * as mcp from "{src}/mcp.ts";
"""

_HARNESS = r"""
const [bundleUrl] = process.argv.slice(-1);
const m = await import(bundleUrl);
const rc = m.requestContext;
const out = {};

let fetchCalls = 0;
let fetchImpl = async (url) => new Response("{}", { status: 200 });
globalThis.fetch = async (input, init) => { fetchCalls++; return fetchImpl(String(input), init); };
const cacheCalls = { match: 0, put: 0, delete: 0 };
const fakeCache = {
  async match() { cacheCalls.match++; return undefined; },
  async put() { cacheCalls.put++; },
  async delete() { cacheCalls.delete++; return true; },
};
delete globalThis.caches;

const URL_OK = "https://query1.finance.yahoo.com/v1/test";
async function tryFetch(url = URL_OK) {
  try { const r = await rc.providerFetch(url); await r.body?.cancel(); return "ok"; }
  catch (e) { return e.name + ": " + e.message; }
}

// 1. 45 fetches succeed, the 46th throws without calling fetch.
{
  fetchCalls = 0;
  const { result, scope } = await rc.withCacheScope(async () => {
    const results = [];
    for (let i = 0; i < 46; i++) results.push(await tryFetch());
    return results;
  });
  out.fortyFive = {
    okCount: result.filter((r) => r === "ok").length,
    last: result[45],
    fetchCalls,
    subrequests: scope.subrequests,
    budgetExhausted: scope.budgetExhausted,
    budgetRefusals: scope.budgetRefusals,
    upstreamRequests: scope.upstreamRequests,
  };
}

// 2. Redirect hops count, one unit each.
{
  fetchCalls = 0;
  fetchImpl = async (url) => url.endsWith("/a")
    ? new Response(null, { status: 302, headers: { Location: "/b" } })
    : new Response("{}", { status: 200 });
  const { result, scope } = await rc.withCacheScope(async () => {
    const first = await tryFetch("https://query1.finance.yahoo.com/a");
    for (let i = 0; i < 42; i++) await tryFetch();   // 2 + 42 = 44 used
    const second = await tryFetch("https://query1.finance.yahoo.com/a"); // hop 1 = 45th, hop 2 refused
    return { first, second };
  });
  out.redirect = { ...result, fetchCalls, subrequests: scope.subrequests, budgetExhausted: scope.budgetExhausted };
  fetchImpl = async () => new Response("{}", { status: 200 });
}

// 3. Cache calls count, and are skipped once the budget is spent.
{
  globalThis.caches = { default: fakeCache };
  fetchCalls = 0;
  for (const k of Object.keys(cacheCalls)) cacheCalls[k] = 0;
  const yf = m.yahooFinance;
  const { result, scope } = await rc.withCacheScope(async () => {
    const miss = await yf.getProviderCache("finnhub:budget-test:a", 60_000);
    await yf.setProviderCache("finnhub:budget-test:b", { x: 1 }, "https://finnhub.io/x", "2026-10-16T00:00:00Z", 60_000);
    const afterTwo = { ...cacheCalls };
    for (let i = 0; i < 43; i++) rc.consumeSubrequest("fetch");
    const spentCache = rc.consumeSubrequest("cache");
    const skippedRead = await yf.getProviderCache("finnhub:budget-test:c", 60_000);
    await yf.setProviderCache("finnhub:budget-test:d", { x: 2 }, "https://finnhub.io/x", "2026-10-16T00:00:00Z", 60_000);
    return { miss, afterTwo, spentCache, skippedRead, afterSpent: { ...cacheCalls } };
  });
  out.cache = { ...result, subrequests: scope.subrequests, budgetExhausted: scope.budgetExhausted, budgetRefusals: scope.budgetRefusals };
  delete globalThis.caches;
}

// 4. Nested scopes share the root budget; a refusal is charged to the asking scope.
{
  const { result, scope } = await rc.withCacheScope(async () => {
    const a = await rc.withCacheScope(async () => { for (let i = 0; i < 30; i++) await tryFetch(); });
    const b = await rc.withCacheScope(async () => { for (let i = 0; i < 15; i++) await tryFetch(); });
    const c = await rc.withBudgetWatch(async () => tryFetch());
    const d = await rc.withBudgetWatch(async () => "no fetch");
    return {
      aRefusals: a.scope.budgetRefusals, bRefusals: b.scope.budgetRefusals,
      aSubrequests: a.scope.subrequests ?? null,
      cStarved: c.starved, cResult: c.result, dStarved: d.starved,
      exhausted: rc.subrequestBudgetExhausted(),
    };
  });
  out.nested = { ...result, rootSubrequests: scope.subrequests, rootRefusals: scope.budgetRefusals };
}

// 5. Outside a scope: no counting, never refused.
{
  fetchCalls = 0;
  let refused = 0;
  for (let i = 0; i < 60; i++) if ((await tryFetch()) !== "ok") refused++;
  out.outside = { fetchCalls, refused, consume: rc.consumeSubrequest("fetch"), exhausted: rc.subrequestBudgetExhausted() };
}

// 6. Cloudflare's own "Too many subrequests" error is a budget refusal (no guard warning).
{
  fetchImpl = async () => { throw new Error("Too many subrequests."); };
  const { result, scope } = await rc.withCacheScope(() => rc.withBudgetWatch(() => tryFetch()));
  out.cloudflare = { starved: result.starved, result: result.result, budgetExhausted: scope.budgetExhausted,
    isBudget: rc.isBudgetExhaustedError(new Error("Too many subrequests by single Worker invocation")) };
  fetchImpl = async () => new Response("{}", { status: 200 });
}

// 7. The MCP layer: warning row, single-tool failure, batch per-ticker code.
{
  m.response.setWorkerEnv({ TOOL_MODE: "flat", MCP_ENVELOPE_V2: "true" });
  const call = (name, args) => ({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args } });
  // Within budget: Yahoo answers 404 for everything; no warning row.
  fetchImpl = async () => new Response("{}", { status: 404 });
  const within = await rc.withCacheScope(() => m.mcp.handleMcp(call("get_company_profile", { ticker: "AAPL" })));
  // Budget already spent before the call: the main fetch is refused.
  const starvedSingle = await rc.withCacheScope(async () => {
    for (let i = 0; i < 45; i++) rc.consumeSubrequest("fetch");
    return m.mcp.handleMcp(call("get_company_profile", { ticker: "AAPL" }));
  });
  const starvedBatch = await rc.withCacheScope(async () => {
    for (let i = 0; i < 45; i++) rc.consumeSubrequest("fetch");
    return m.mcp.handleMcp(call("get_company_profile", { ticker: ["AAPL", "MSFT"] }));
  });
  const sc = (r) => r.result.structuredContent;
  out.mcp = {
    withinWarnings: sc(within.result).meta.warnings,
    single: { ok: sc(starvedSingle.result).ok, error: sc(starvedSingle.result).error,
      retryable: sc(starvedSingle.result).meta.retryable, recoveryAction: sc(starvedSingle.result).meta.recoveryAction,
      warnings: sc(starvedSingle.result).meta.warnings, isError: starvedSingle.result.result.isError ?? false },
    batch: { ok: sc(starvedBatch.result).ok, data: sc(starvedBatch.result).data, warnings: sc(starvedBatch.result).meta.warnings },
  };
  m.response.setWorkerEnv({});
}

// 8. News: Cloudflare's "Too many subrequests" text on a source fetch maps to PROVIDER_BUDGET_EXHAUSTED,
//    with failureMode WORKER_SUBREQUEST_LIMIT kept (frozen value) and retryable true.
{
  m.response.setWorkerEnv({ FINNHUB_API_KEY: "fake-finnhub-key" });
  fetchImpl = async () => { throw new Error("Too many subrequests by single Worker invocation"); };
  const { result } = await rc.withCacheScope(() => m.yahooFinance.verifyCompanyEvent("ACME", "acquisition", "", "", ["finnhub"]));
  const v = JSON.parse(result);
  out.cloudflareNews = {
    status: v.status, failureMode: v.failureMode, retryable: v.retryable,
    finnhub: v.sourceStatus.finnhub.status, coverage: v.coverage.state,
    failed: v.coverage.failedSources, action: v.coverage.recommendedNextAction,
    attempted: v.attemptedSources,
  };
  fetchImpl = async () => new Response("{}", { status: 200 });
  m.response.setWorkerEnv({});
}

process.stdout.write(JSON.stringify(out) + "\n");
"""


def _node() -> str | None:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    return node if node and ESBUILD.exists() else None


_RESULT: dict = {}


def _run() -> dict:
    if _RESULT:
        return _RESULT
    node = _node()
    with tempfile.TemporaryDirectory(prefix="worker-budget-") as tmp:
        tmp_path = Path(tmp)
        entry = tmp_path / "entry.ts"
        entry.write_text(_ENTRY.format(src=(WORKER / "src").as_posix()), encoding="utf-8")
        bundle = tmp_path / "bundle.mjs"
        subprocess.run(
            [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=node",
             f"--outfile={bundle}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=180,
        )
        harness = tmp_path / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        proc = subprocess.run([node, str(harness), bundle.as_uri()], cwd=WORKER,
                              capture_output=True, text=True, timeout=300)
        if proc.returncode != 0:
            raise AssertionError(f"harness failed:\n{proc.stderr[-4000:]}")
        _RESULT.update(json.loads(proc.stdout.strip().splitlines()[-1]))
    return _RESULT


@unittest.skipIf(_node() is None, "node and worker/node_modules (npm ci) are required")
class TestWorkerSubrequestBudget(unittest.TestCase):
    def test_45_fetches_then_refusal_without_fetch(self):
        r = _run()["fortyFive"]
        self.assertEqual(r["okCount"], 45)
        self.assertTrue(r["last"].startswith("BudgetExhaustedError: PROVIDER_BUDGET_EXHAUSTED"), r["last"])
        self.assertEqual(r["fetchCalls"], 45)
        self.assertEqual(r["subrequests"], 45)
        self.assertEqual(r["upstreamRequests"], 45)
        self.assertTrue(r["budgetExhausted"])
        self.assertEqual(r["budgetRefusals"], 1)

    def test_redirect_hops_count(self):
        r = _run()["redirect"]
        self.assertEqual(r["first"], "ok")
        self.assertTrue(r["second"].startswith("BudgetExhaustedError"), r["second"])
        # 2 hops + 42 + 1 hop allowed; the second hop of the last request never reached fetch.
        self.assertEqual(r["fetchCalls"], 45)
        self.assertEqual(r["subrequests"], 45)
        self.assertTrue(r["budgetExhausted"])

    def test_cache_calls_count_and_are_skipped_when_spent(self):
        r = _run()["cache"]
        self.assertIsNone(r["miss"])
        self.assertEqual(r["afterTwo"], {"match": 1, "put": 1, "delete": 0})
        self.assertFalse(r["spentCache"])
        self.assertIsNone(r["skippedRead"])
        self.assertEqual(r["afterSpent"], {"match": 1, "put": 1, "delete": 0})
        self.assertEqual(r["subrequests"], 45)  # skipped cache calls consume nothing
        self.assertFalse(r["budgetExhausted"])  # and do not flag the invocation
        self.assertEqual(r["budgetRefusals"], 0)

    def test_nested_scopes_share_the_root_budget(self):
        r = _run()["nested"]
        self.assertEqual(r["rootSubrequests"], 45)
        self.assertIsNone(r["aSubrequests"])  # only the root carries the counter
        self.assertEqual((r["aRefusals"], r["bRefusals"]), (0, 0))
        self.assertTrue(r["cStarved"])
        self.assertTrue(r["cResult"].startswith("BudgetExhaustedError"))
        self.assertFalse(r["dStarved"])
        self.assertTrue(r["exhausted"])
        self.assertEqual(r["rootRefusals"], 1)

    def test_no_counting_outside_a_scope(self):
        r = _run()["outside"]
        self.assertEqual(r["fetchCalls"], 60)
        self.assertEqual(r["refused"], 0)
        self.assertTrue(r["consume"])
        self.assertFalse(r["exhausted"])

    def test_cloudflare_too_many_subrequests_is_a_budget_refusal(self):
        r = _run()["cloudflare"]
        self.assertTrue(r["starved"])
        self.assertIn("Too many subrequests", r["result"])
        self.assertFalse(r["budgetExhausted"])  # the guard did not refuse; no guard warning
        self.assertTrue(r["isBudget"])

    def test_meta_warning_row_only_when_the_guard_fired(self):
        r = _run()["mcp"]
        self.assertNotIn(WARNING["code"], [w.get("code") for w in r["withinWarnings"]])
        self.assertEqual([w for w in r["single"]["warnings"] if w.get("code") == WARNING["code"]], [WARNING])
        self.assertEqual([w for w in r["batch"]["warnings"] if w.get("code") == WARNING["code"]], [WARNING])

    def test_starved_single_tool_fails_with_budget_code(self):
        r = _run()["mcp"]["single"]
        self.assertFalse(r["ok"])
        self.assertTrue(r["isError"])
        self.assertEqual(r["error"]["code"], "PROVIDER_BUDGET_EXHAUSTED")
        self.assertEqual(r["error"]["recoveryAction"], "RETRY_LATER")
        self.assertTrue(r["error"]["retryable"])
        self.assertEqual((r["recoveryAction"], r["retryable"]), ("RETRY_LATER", True))

    def test_starved_batch_ticker_carries_the_per_ticker_code(self):
        r = _run()["mcp"]["batch"]
        self.assertTrue(r["ok"])
        for ticker in ("AAPL", "MSFT"):
            row = r["data"][ticker]
            err = row.get("error") if isinstance(row.get("error"), dict) else row
            self.assertEqual(err["code"], "PROVIDER_BUDGET_EXHAUSTED", row)
            self.assertTrue(err["retryable"])
            self.assertEqual(err["recoveryAction"], "RETRY_LATER")

    def test_cloudflare_subrequest_error_in_news_maps_to_budget_status(self):
        r = _run()["cloudflareNews"]
        self.assertEqual(r["finnhub"], "PROVIDER_BUDGET_EXHAUSTED")
        self.assertEqual(r["status"], "SOURCE_LIMITED_NOT_FOUND")
        self.assertEqual(r["failureMode"], "WORKER_SUBREQUEST_LIMIT")
        self.assertTrue(r["retryable"])
        self.assertEqual(r["coverage"], "PARTIAL")
        self.assertEqual(r["failed"], [{"source": "finnhub", "status": "PROVIDER_BUDGET_EXHAUSTED", "attempted": True, "reasonCode": None}])
        self.assertEqual(r["action"], "RETRY_RETRYABLE_SOURCES")
        self.assertEqual(r["attempted"], [{"sourceType": "finnhub", "status": "PROVIDER_BUDGET_EXHAUSTED"}])


if __name__ == "__main__":
    unittest.main()
