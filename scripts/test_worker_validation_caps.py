#!/usr/bin/env python3
"""Worker tool-call gate: central validation and per-tool ticker caps (2.12.0, R4 Q3b/Q3c).

Bundles worker/src with esbuild and drives the real dispatch paths in one Node process with a fetch spy (every
fetch answers 404; nothing reaches the network):

- The same table-driven cases as the parity harness (scripts/parity/cases_validation.py, cases_caps.py) through a
  full tool call in expanded mode (callToolResult) and grouped mode (callVisibleToolResult): exact error envelope
  (code, message, public action name, retryable) and no fetch on rejection.
- Every capped action: cap + 1 is BATCH_LIMIT_EXCEEDED with no fetch in both modes; the cap passes the gate and
  reaches its handler. The get_company_news conditional cap and both get_market_snapshot caps.
- The grouped-mode checker still enforces schema length, and an oversize ticker array is BATCH_LIMIT_EXCEEDED in
  grouped mode too (the cap is checked before the shape checker).
- Schema maxItems in both listings equal tool_catalog.json tickerCaps; descriptions that state a cap state it.
- No truncation: in-cap batch results carry no _truncated / truncated / droppedTickers key.
- Through /mcp: the rejection is a tool error and the telemetry data point carries errorCode BATCH_LIMIT_EXCEEDED.
"""

from __future__ import annotations

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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.parity.cases_caps import CAP_EXPECT, CAP_ROWS, COND_ROWS, POOL  # noqa: E402
from scripts.parity.cases_caps import CASES as CAP_CASES  # noqa: E402
from scripts.parity.cases_validation import ACCESSION_ROWS, PASS, TICKER_ROWS  # noqa: E402
from scripts.parity.cases_validation import CASES as VALIDATION_CASES  # noqa: E402

CATALOG = json.loads((ROOT / "tool_catalog.json").read_text(encoding="utf-8"))
TICKER_CAPS: dict[str, int] = CATALOG["tickerCaps"]
GROUP_OF = {action: group for group, spec in CATALOG["groups"].items() for action in spec["actions"]}


def _expectations() -> dict[str, object]:
    """label -> expected gate view, from the parity cases' Worker expectations."""
    out: dict[str, object] = {}
    for case in VALIDATION_CASES + CAP_CASES:
        if isinstance(case.expect_ts, dict) and case.name.startswith(("validation.gate.", "caps.every", "caps.company")):
            out.update(case.expect_ts)
    return out


EXPECT = _expectations()
ROWS = [list(r) for r in TICKER_ROWS + ACCESSION_ROWS + COND_ROWS + CAP_ROWS]
REJECT_ROWS = [r for r in ROWS if EXPECT[r[0]] != PASS]
AT_CAP_ROWS = [[label, action, args] for label, action, args in CAP_ROWS if CAP_EXPECT[label] == PASS]

_HARNESS = r"""
import fs from "node:fs";
const [bundleUrl, inputPath] = process.argv.slice(-2);
const input = JSON.parse(fs.readFileSync(inputPath, "utf8"));
const m = await import(bundleUrl);

// Yahoo chart requests get a canned daily series (so batch tools produce data); everything else is a 404.
function chart(url) {
  const symbol = decodeURIComponent(url.split("/v8/finance/chart/")[1].split("?")[0]);
  const day = 86400, end = Math.floor(Date.UTC(2026, 9, 9, 20, 0, 0) / 1000);
  const timestamp = [];
  for (let t = end - 400 * day; t <= end; t += day) {
    const wd = new Date(t * 1000).getUTCDay();
    if (wd !== 0 && wd !== 6) timestamp.push(t);
  }
  const close = timestamp.map((_, i) => 100 + Math.sin(i / 7) * 5 + i * 0.05);
  return { chart: { error: null, result: [{
    meta: { symbol, currency: "USD", instrumentType: "EQUITY", exchangeTimezoneName: "America/New_York", gmtoffset: -14400,
      regularMarketPrice: close[close.length - 1], regularMarketTime: end, chartPreviousClose: close[close.length - 2] },
    timestamp,
    indicators: { quote: [{ open: close, high: close.map((c) => c + 1), low: close.map((c) => c - 1), close,
      volume: close.map((_, i) => 1_000_000 + i * 10) }], adjclose: [{ adjclose: close }] },
  }] } };
}
let fetches = 0;
globalThis.fetch = async (input) => {
  fetches++;
  const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
  if (url.includes("/v8/finance/chart/")) return new Response(JSON.stringify(chart(url)), { status: 200, headers: { "Content-Type": "application/json" } });
  return new Response("not found", { status: 404 });
};
delete globalThis.caches;
const logLines = [];
console.log = (...a) => { logLines.push(a.map(String).join(" ")); };
console.warn = () => {};
console.error = () => {};

const BASE_ENV = { MCP_ENVELOPE_V2: "true" };
const setMode = (mode) => m.response.setWorkerEnv({ ...BASE_ENV, TOOL_MODE: mode });

async function run(mode, action, args, mod = m) {
  mod.response.setWorkerEnv({ ...BASE_ENV, TOOL_MODE: mode });
  const before = fetches;
  const result = mode === "expanded"
    ? await mod.tools.callToolResult(action, args)
    : await mod.tools.callVisibleToolResult(input.groupOf[action], { action, params: args });
  return { envelope: JSON.parse(result.text), fetches: fetches - before };
}

const out = { rejected: {}, atCap: {}, noTruncation: {}, listing: {}, groupedLength: {}, mcp: {} };
let isolate = 0;
for (const mode of ["expanded", "grouped"]) {
  out.rejected[mode] = {};
  for (const [label, action, args] of input.rejectRows) out.rejected[mode][label] = await run(mode, action, args);
  out.atCap[mode] = {};
  for (const [label, action, args] of input.atCapRows) {
    // A fresh bundle copy: module caches start empty, so a call that reached its handler fetches.
    const r = await run(mode, action, args, await import(`${bundleUrl}?isolate=${++isolate}`));
    out.atCap[mode][label] = { code: r.envelope.ok ? null : r.envelope.error?.code, message: r.envelope.error?.message ?? null, fetches: r.fetches };
  }
}

// In-cap batch results: the top-level keys (the truncation keys are gone).
for (const [label, action, args] of input.noTruncationRows) {
  const r = await run("expanded", action, args);
  out.noTruncation[label] = { ok: r.envelope.ok, keys: r.envelope.ok ? Object.keys(r.envelope.data).sort() : null };
}

// The grouped shape checker still enforces schema length (peers maxItems 5).
out.groupedLength.peers6 = await run("grouped", "get_historical_valuation_context", { ticker: "AAPL", peers: input.pool.slice(1, 7) });

// Listings: ticker maxItems, required params and the ticker descriptions, in both modes.
for (const mode of ["expanded", "grouped"]) {
  setMode(mode);
  const caps = {}, required = {}, descriptions = {};
  for (const tool of m.tools.listVisibleTools()) {
    const entries = mode === "expanded"
      ? [[tool.name, tool.inputSchema]]
      : (tool.inputSchema.oneOf ?? []).map((b) => [b.properties.action.const, b.properties.params]);
    for (const [action, schema] of entries) {
      required[action] = schema.required ?? [];
      const ticker = schema.properties?.ticker;
      const branch = (ticker?.oneOf ?? []).find((b) => b.type === "array");
      if (branch && typeof branch.maxItems === "number") caps[action] = branch.maxItems;
      if (ticker?.description) descriptions[action] = ticker.description;
    }
  }
  out.listing[mode] = { caps, required, descriptions };
}
out.errorCodes = m.response.ErrorCode;

// Through /mcp (grouped, credential-free grace mode): a tool error with a telemetry data point.
const points = [];
const env = { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true", MCP_AUTH_MODE: "grace", EDGAR_CONTACT_EMAIL: "diagnostic@example.com",
  TELEMETRY: { writeDataPoint(p) { points.push(JSON.parse(JSON.stringify(p))); } } };
const body = { jsonrpc: "2.0", id: 1, method: "tools/call",
  params: { name: "stock_pricing", arguments: { action: "get_market_quote", params: { ticker: input.pool.slice(0, 11) } } } };
const before = fetches;
const response = await m.default.fetch(new Request("https://gate.invalid/mcp", {
  method: "POST", headers: { "Content-Type": "application/json", Accept: "application/json, text/event-stream" }, body: JSON.stringify(body),
}), env, { waitUntil() {}, passThroughOnException() {} });
const rpc = await response.json();
out.mcp = { status: response.status, isError: rpc.result?.isError ?? null, text: rpc.result?.content?.[0]?.text ?? null,
  fetches: fetches - before, errorCodeBlob: points.length === 1 ? points[0].blobs[4] : null, outcomeBlob: points.length === 1 ? points[0].blobs[3] : null };

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
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".gate-test-") as tmp:
        tmp_path = Path(tmp)
        entry = tmp_path / "entry.ts"
        entry.write_text(
            'export { default } from "../src/index.ts";\n'
            'export * as tools from "../src/tools.ts";\n'
            'export * as response from "../src/response.ts";\n',
            encoding="utf-8",
        )
        bundle = tmp_path / "worker.mjs"
        subprocess.run(
            [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=node",
             f"--outfile={bundle}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=180,
        )
        harness = tmp_path / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        payload = {
            "groupOf": GROUP_OF,
            "rejectRows": REJECT_ROWS,
            "atCapRows": AT_CAP_ROWS,
            "noTruncationRows": [
                ["get_price_slope@5", "get_price_slope", {"ticker": POOL[:5], "days": 5}],
                ["get_technical_indicators@5", "get_technical_indicators", {"ticker": POOL[:5]}],
                ["get_market_snapshot@2", "get_market_snapshot", {"ticker": POOL[:2]}],
                ["get_market_snapshot.full@2", "get_market_snapshot", {"ticker": POOL[:2], "mode": "full"}],
            ],
            "pool": POOL,
        }
        input_path = tmp_path / "input.json"
        input_path.write_text(json.dumps(payload), encoding="utf-8")
        proc = subprocess.run([node, str(harness), bundle.as_uri(), str(input_path)], cwd=WORKER,
                              capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            raise AssertionError(f"harness failed:\n{proc.stderr[-4000:]}")
        _RESULT.update(json.loads(proc.stdout.strip().splitlines()[-1]))
    return _RESULT


def _view(envelope: dict) -> dict:
    err, meta = envelope.get("error") or {}, envelope.get("meta") or {}
    return {"code": err.get("code"), "message": err.get("message"), "tool": meta.get("tool"), "retryable": meta.get("retryable")}


def _grouped_preempted(label: str, action: str, args: dict) -> bool:
    """The grouped shape checker runs before the gate's format checks: an empty required string or a missing
    required parameter is reported there (unchanged behaviour). Caps are checked before the shape checker."""
    if EXPECT[label] != PASS and EXPECT[label]["code"] == "BATCH_LIMIT_EXCEEDED":
        return False
    required = _run()["listing"]["grouped"]["required"].get(action, [])
    return any(name not in args or (isinstance(args[name], str) and args[name].strip() == "") for name in required)


@unittest.skipIf(_node() is None, "node and worker/node_modules (npm ci) are required")
class TestGateRejections(unittest.TestCase):
    def test_expanded_mode_envelope_is_exact_and_nothing_is_fetched(self):
        r = _run()["rejected"]["expanded"]
        for label, _action, _args in REJECT_ROWS:
            with self.subTest(label=label):
                self.assertEqual(_view(r[label]["envelope"]), EXPECT[label])
                self.assertIs(r[label]["envelope"]["ok"], False)
                self.assertEqual(r[label]["fetches"], 0)

    def test_grouped_mode_envelope_is_exact_and_nothing_is_fetched(self):
        r = _run()["rejected"]["grouped"]
        checked = 0
        for label, action, args in REJECT_ROWS:
            with self.subTest(label=label):
                self.assertEqual(r[label]["fetches"], 0)
                if _grouped_preempted(label, action, args):
                    self.assertEqual(r[label]["envelope"]["error"]["code"], "INPUT_VALIDATION_ERROR")
                    self.assertTrue(r[label]["envelope"]["error"]["message"].startswith(f"Invalid params for '{action}'"))
                    continue
                checked += 1
                self.assertEqual(_view(r[label]["envelope"]), EXPECT[label])
        self.assertGreater(checked, len(REJECT_ROWS) - 6)

    def test_empty_required_ticker_in_grouped_mode_keeps_the_shape_checker_message(self):
        r = _run()["rejected"]["grouped"]
        self.assertEqual(r["empty_string"]["envelope"]["error"]["message"],
                         "Invalid params for 'get_historical_prices': empty required parameter(s): ticker.")

    def test_batch_limit_error_shape(self):
        env = _run()["rejected"]["expanded"]["get_market_quote@11"]["envelope"]
        self.assertEqual(env["error"], {"code": "BATCH_LIMIT_EXCEEDED",
                                        "message": "Too many tickers: 11. Maximum is 10 per call for get_market_quote.",
                                        "retryable": False})
        self.assertIs(env["meta"]["retryable"], False)
        self.assertIsNone(env["data"])

    def test_every_capped_action_rejects_cap_plus_one_in_both_modes(self):
        rejected = _run()["rejected"]
        for key, cap in TICKER_CAPS.items():
            label = f"{key}@{cap + 1}"
            action = key.partition(".")[0]
            for mode in ("expanded", "grouped"):
                with self.subTest(label=label, mode=mode):
                    env = rejected[mode][label]["envelope"]
                    self.assertEqual(env["error"]["code"], "BATCH_LIMIT_EXCEEDED")
                    self.assertEqual(env["error"]["message"], f"Too many tickers: {cap + 1}. Maximum is {cap} per call for {action}.")
                    self.assertEqual(rejected[mode][label]["fetches"], 0)


@unittest.skipIf(_node() is None, "node and worker/node_modules (npm ci) are required")
class TestCapsPass(unittest.TestCase):
    def test_at_cap_reaches_the_handler_in_both_modes(self):
        at_cap = _run()["atCap"]
        for label, _action, _args in AT_CAP_ROWS:
            for mode in ("expanded", "grouped"):
                with self.subTest(label=label, mode=mode):
                    row = at_cap[mode][label]
                    self.assertNotIn(row["code"], ("BATCH_LIMIT_EXCEEDED", "INPUT_VALIDATION_ERROR"), row)
                    self.assertGreater(row["fetches"], 0, "the handler ran and asked upstream")

    def test_company_news_conditional_cap(self):
        r = _run()["rejected"]["expanded"]
        for label in ("company_ir_two", "company_ir_page_two", "newswire_two", "newswire_mixed_case_two"):
            with self.subTest(label=label):
                self.assertEqual(r[label]["envelope"]["error"]["message"],
                                 "Too many tickers: 2. Maximum is 1 per call for get_company_news.")

    def test_snapshot_caps_compact_and_full(self):
        r = _run()["rejected"]["expanded"]
        compact, full = TICKER_CAPS["get_market_snapshot"], TICKER_CAPS["get_market_snapshot.full"]
        self.assertEqual(r[f"get_market_snapshot@{compact + 1}"]["envelope"]["error"]["code"], "BATCH_LIMIT_EXCEEDED")
        self.assertEqual(r[f"get_market_snapshot.full@{full + 1}"]["envelope"]["error"]["message"],
                         f"Too many tickers: {full + 1}. Maximum is {full} per call for get_market_snapshot.")

    def test_in_cap_batches_carry_no_truncation_keys(self):
        r = _run()["noTruncation"]
        for label, row in r.items():
            with self.subTest(label=label):
                self.assertTrue(row["ok"], row)
                self.assertFalse({"_truncated", "truncated", "droppedTickers"} & set(row["keys"]), row)
        self.assertEqual(r["get_market_snapshot@2"]["keys"], ["tickers"])
        self.assertEqual(r["get_market_snapshot.full@2"]["keys"], ["tickers"])

    def test_grouped_checker_still_enforces_schema_length(self):
        env = _run()["groupedLength"]["peers6"]
        self.assertEqual(env["fetches"], 0)
        self.assertEqual(env["envelope"]["error"]["code"], "INPUT_VALIDATION_ERROR")
        self.assertIn("invalid parameter value(s): peers", env["envelope"]["error"]["message"])


@unittest.skipIf(_node() is None, "node and worker/node_modules (npm ci) are required")
class TestSchemasAndCodes(unittest.TestCase):
    def test_schema_max_items_match_ticker_caps_in_both_listings(self):
        expected = {k: v for k, v in TICKER_CAPS.items() if "." not in k}
        for mode in ("expanded", "grouped"):
            with self.subTest(mode=mode):
                self.assertEqual(_run()["listing"][mode]["caps"], expected)

    def test_full_snapshot_cap_is_not_larger_than_the_advertised_cap(self):
        self.assertLessEqual(TICKER_CAPS["get_market_snapshot.full"], TICKER_CAPS["get_market_snapshot"])

    def test_descriptions_state_the_cap_and_no_truncation(self):
        descriptions = _run()["listing"]["expanded"]["descriptions"]
        for action, text in descriptions.items():
            with self.subTest(action=action):
                self.assertNotIn("only the first", text)
                match = re.search(r"up to (\d+) symbols", text)
                if match and action in TICKER_CAPS:
                    self.assertEqual(int(match.group(1)), TICKER_CAPS[action])
        self.assertIn("only one symbol when sources includes company_ir, company_ir_page or newswire",
                      descriptions["get_company_news"])
        self.assertIn("BATCH_LIMIT_EXCEEDED", descriptions["get_analyst_consensus"])

    def test_batch_limit_exceeded_is_an_error_code(self):
        self.assertEqual(_run()["errorCodes"]["BATCH_LIMIT_EXCEEDED"], "BATCH_LIMIT_EXCEEDED")

    def test_ticker_caps_cover_exactly_the_array_ticker_actions(self):
        caps = _run()["listing"]["expanded"]["caps"]
        self.assertEqual(set(caps), {k for k in TICKER_CAPS if "." not in k})


@unittest.skipIf(_node() is None, "node and worker/node_modules (npm ci) are required")
class TestThroughMcp(unittest.TestCase):
    def test_rejection_is_a_tool_error_with_telemetry_error_code(self):
        r = _run()["mcp"]
        self.assertEqual(r["status"], 200)
        self.assertIs(r["isError"], True)
        self.assertEqual(json.loads(r["text"])["error"]["code"], "BATCH_LIMIT_EXCEEDED")
        self.assertEqual(r["fetches"], 0)
        self.assertEqual(r["outcomeBlob"], "tool_error")
        self.assertEqual(r["errorCodeBlob"], "BATCH_LIMIT_EXCEEDED")


if __name__ == "__main__":
    unittest.main()
