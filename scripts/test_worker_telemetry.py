#!/usr/bin/env python3
"""Worker operational telemetry (2.11.1, R4 Q10).

Bundles worker/src/index.ts and drives it in Node with real Request objects, a
fake env holding a fake MCP_TOKEN, a fake TELEMETRY binding that records every
writeDataPoint call, a fetch spy (nothing reaches the network) and a captured
console.log.

- One data point per /mcp request, for every shape: an OK tool call, tool
  errors, tools/list, 401, 405, 413, batch 400, invalid request, unknown
  tools and methods, notifications.
- The exact index/blob/double layout, with the expected values.
- latencyMs >= 0; budgetExhausted, providerTimeouts and providerRetries set
  when the guard, a provider timeout or a retry happened.
- Privacy (Q10, Q13): every recorded string is in an allow-set or matches
  ^[A-Za-z0-9_./:<>=-]{0,64}$; no blob, index or log line carries the token,
  an argument value, a caller-supplied tool name or method, or the path.
- One dispatch-start log line per dispatched request, logged as an object (not
  a JSON string, 2.14.0) with exactly the keys t, v, action, domain, build, and
  none for rejections.
- Without the binding (and with a binding that throws) nothing throws and the
  responses (status, headers, body) are byte-identical.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"

TOKEN = "tok-telemetry-7c1e9b44d02a-SECRET"
WRONG = "tok-telemetry-7c1e9b44d02a-WRONG"
BUILD_SHA = "0123456789abcdef0123456789abcdef01234567"
SERVER_VERSION = "2.11.0"
# Argument values the requests carry; none may reach a data point or log line.
FAKE_TICKER = "QZXQV"
FAKE_TOOL = "evil_tool<script>"
FAKE_METHOD = "x/evil-method"
FAKE_ACTION = "drop_tables_now"
FAKE_ARG = "arg-value-5d1f0e"

SAFE_STRING = re.compile(r"^[A-Za-z0-9_./:<>=-]{0,64}$")

_HARNESS = r"""
const [bundleUrl] = process.argv.slice(-1);
const TOKEN = process.env.TEST_TOKEN;
const WRONG = process.env.TEST_WRONG;
const FAKE = JSON.parse(process.env.TEST_FAKE);
const ctx = { waitUntil() {}, passThroughOnException() {} };

// Captured console.log: the dispatch-start lines. A single logged object is kept as {object}, so the test can tell
// an object from a JSON string; anything else is kept as {text}.
const logLines = [];
const realLog = console.log;
console.log = (...args) => {
  const one = args.length === 1 && args[0] !== null && typeof args[0] === "object" && !Array.isArray(args[0]);
  logLines.push(one ? { object: JSON.parse(JSON.stringify(args[0])) } : { text: args.map(String).join(" ") });
};

// A fixed clock, so responses from separate runs compare byte for byte.
const RealDate = Date;
const FIXED = RealDate.parse("2026-10-16T12:00:00Z");
class FixedDate extends RealDate {
  constructor(...args) { if (args.length) super(...args); else super(FIXED); }
  static now() { return FIXED; }
}
globalThis.Date = FixedDate;

// Fetch spy: the behaviour is chosen per request.
let fetchMode = "unavailable";
let rc = null;
const realSetTimeout = globalThis.setTimeout;
globalThis.fetch = async (input, init = {}) => {
  if (fetchMode === "budget") {
    // Runs inside the request's scope: spend the whole budget on the first fetch.
    for (let i = 0; i < 60; i++) rc.consumeSubrequest("fetch");
    return new Response("unavailable", { status: 503 });
  }
  if (fetchMode === "hang") {
    return new Promise((_, reject) => {
      init.signal?.addEventListener("abort", () => reject(new DOMException("The operation was aborted.", "AbortError")));
    });
  }
  if (fetchMode === "throttled") {
    const u = new URL(String(input));
    if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
    if (u.pathname.includes("getcrumb")) return new Response("crumb123");
    return new Response("Too Many Requests", { status: 429 });
  }
  return new Response("unavailable", { status: 503 });
};

const rpc = (method, id = 1, params) => ({ jsonrpc: "2.0", id, method, ...(params ? { params } : {}) });
const toolCall = (name, args, id = 1) => rpc("tools/call", id, { name, arguments: args });
const bearer = { Authorization: `Bearer ${TOKEN}` };
const priceCall = (id = 1) => toolCall("stock_pricing", {
  action: "get_historical_prices", params: { ticker: FAKE.ticker, period: "5d", interval: "1d" },
}, id);

// name, request, fetch mode
const CASES = [
  ["okToolCall", { body: toolCall("system", { action: "health_check", params: {} }), headers: bearer }],
  ["toolErrorUnknownAction", { body: toolCall("stock_pricing", { action: FAKE.action, params: { ticker: FAKE.ticker, note: FAKE.arg } }), headers: bearer }],
  ["toolErrorProvider", { body: priceCall(), headers: bearer }],
  ["toolsList", { body: rpc("tools/list"), headers: bearer }],
  ["initialize", { body: rpc("initialize", 1, { protocolVersion: "2025-06-18" }), headers: bearer }],
  ["pathCredential", { path: `/mcp/${TOKEN}`, body: rpc("tools/list") }],
  ["unknownTool", { body: toolCall(FAKE.tool, { action: "x", params: { ticker: FAKE.ticker } }), headers: bearer }],
  ["nonStringToolName", { body: rpc("tools/call", 1, { name: 42 }), headers: bearer }],
  ["unknownMethod", { body: rpc(FAKE.method), headers: bearer }],
  ["nonStringMethod", { body: { jsonrpc: "2.0", id: 1, method: { x: FAKE.arg } }, headers: bearer }],
  ["missingToolName", { body: rpc("tools/call", 1, {}), headers: bearer }],
  ["notification", { body: { jsonrpc: "2.0", method: "notifications/initialized" }, headers: bearer }],
  ["batchOneCall", { body: [rpc("initialize", 1), priceCall(2)], headers: bearer }],
  ["auth401", { body: priceCall() }],
  ["auth401WrongPath", { path: `/mcp/${WRONG}`, body: priceCall() }],
  ["auth401WrongHeader", { body: priceCall(), headers: { Authorization: `Bearer ${WRONG}` } }],
  ["method405", { method: "GET", headers: bearer }],
  ["size413", { body: priceCall(), headers: bearer, contentLength: 2_000_000 }],
  ["batch400", { body: [priceCall(1), priceCall(2)], headers: bearer }],
  ["invalidJson400", { raw: `{"not json ${FAKE.arg}`, headers: bearer }],
  ["emptyBatch400", { raw: "[]", headers: bearer }],
  ["budget", { body: priceCall(), headers: bearer }, "budget"],
  ["timeout", { body: priceCall(), headers: bearer }, "hang"],
  ["retry", { body: priceCall(), headers: bearer }, "throttled"],
  ["health", { path: "/health", method: "GET" }],
];

function makeEnv(telemetry) {
  const env = {
    TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true", MCP_AUTH_MODE: "enforce", MCP_TOKEN: TOKEN,
    SERVER_VERSION: FAKE.serverVersion, BUILD_SHA: FAKE.buildSha,
  };
  if (telemetry !== undefined) env.TELEMETRY = telemetry;
  return env;
}

async function runAll(label, telemetry) {
  const mod = await import(`${bundleUrl}?run=${label}`);
  const worker = mod.default;
  rc = mod.requestContext;
  const env = makeEnv(telemetry);
  const results = {};
  for (const [name, req, mode] of CASES) {
    fetchMode = mode ?? "unavailable";
    globalThis.setTimeout = mode === "hang"
      ? (cb, ms, ...a) => realSetTimeout(cb, ms >= 10_000 ? 0 : ms, ...a)
      : realSetTimeout;
    const method = req.method ?? "POST";
    const headers = { ...(req.headers ?? {}) };
    const init = { method, headers };
    if (method === "POST") {
      headers["Content-Type"] = "application/json";
      init.body = req.raw ?? JSON.stringify(req.body);
    }
    if (req.contentLength !== undefined) headers["Content-Length"] = String(req.contentLength);
    const pointsBefore = telemetry?.points?.length ?? 0;
    const logsBefore = logLines.length;
    const response = await worker.fetch(new Request(`https://telemetry.invalid${req.path ?? "/mcp"}`, init), env, ctx);
    const text = await response.text();
    results[name] = {
      status: response.status,
      headers: [...response.headers.entries()],
      text,
      points: (telemetry?.points ?? []).slice(pointsBefore),
      logs: logLines.slice(logsBefore),
    };
  }
  globalThis.setTimeout = realSetTimeout;
  return results;
}

const recording = { points: [], writeDataPoint(p) { this.points.push(JSON.parse(JSON.stringify(p))); } };
const withBinding = await runAll("binding", recording);
const withoutBinding = await runAll("none", undefined);
const throwing = { writeDataPoint() { throw new Error("binding down"); } };
const withThrowingBinding = await runAll("throwing", throwing);

const strip = (results) => Object.fromEntries(Object.entries(results).map(([k, v]) => [k, { status: v.status, headers: v.headers, text: v.text }]));
process.stdout.write(JSON.stringify({
  withBinding,
  withoutBinding: strip(withoutBinding),
  withThrowingBinding: strip(withThrowingBinding),
  logsWithoutBinding: Object.fromEntries(Object.entries(withoutBinding).map(([k, v]) => [k, v.logs])),
}) + "\n");
"""


def _node() -> str | None:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    return node if node and ESBUILD.exists() else None


_RESULT: dict = {}


def _dispatch_objects(logs: list[dict]) -> list[dict]:
    """The dispatch-start lines logged as objects."""
    return [e["object"] for e in logs if isinstance(e.get("object"), dict) and e["object"].get("t") == "yfmcp"]


def _dispatch_strings(logs: list[dict]) -> list[str]:
    """Dispatch-start lines logged as a JSON string (the pre-2.14.0 form)."""
    return [e["text"] for e in logs if isinstance(e.get("text"), str) and '"t":"yfmcp"' in e["text"]]


def _run() -> dict:
    if _RESULT:
        return _RESULT
    node = _node()
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".telemetry-test-") as tmp:
        tmp_path = Path(tmp)
        entry = tmp_path / "entry.ts"
        entry.write_text(
            'export { default } from "../src/index.ts";\n'
            'export * as requestContext from "../src/request-context.ts";\n',
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
        fake = {
            "ticker": FAKE_TICKER, "tool": FAKE_TOOL, "method": FAKE_METHOD, "action": FAKE_ACTION,
            "arg": FAKE_ARG, "buildSha": BUILD_SHA, "serverVersion": SERVER_VERSION,
        }
        env = {**os.environ, "TEST_TOKEN": TOKEN, "TEST_WRONG": WRONG, "TEST_FAKE": json.dumps(fake)}
        proc = subprocess.run([node, str(harness), bundle.as_uri()], cwd=WORKER,
                              capture_output=True, text=True, timeout=300, env=env)
        if proc.returncode != 0:
            raise AssertionError(f"harness failed:\n{proc.stderr[-4000:]}")
        _RESULT.update(json.loads(proc.stdout.strip().splitlines()[-1]))
    return _RESULT


def _catalog_actions() -> set[str]:
    catalog = json.loads((ROOT / "tool_catalog.json").read_text(encoding="utf-8"))
    return {f"tools/call:{group}.{action}" for group, spec in catalog["groups"].items() for action in spec["actions"]}


def _catalog_groups() -> set[str]:
    return set(json.loads((ROOT / "tool_catalog.json").read_text(encoding="utf-8"))["groups"])


ACTION_ALLOW = _catalog_actions() | {
    "initialize", "ping", "tools/list", "tools/call:unknown", "rpc:other",
    "rejected:auth", "rejected:method", "rejected:body_size", "rejected:batch", "rejected:invalid",
    "rejected:key_limit",
}
DOMAIN_ALLOW = _catalog_groups() | {"-"}
OUTCOMES = {"ok", "tool_error", "rpc_error", "rejected", "exception"}
SIZE_BUCKETS = {"<1K", "1-10K", "10-100K", "100K-1M", ">=1M"}
CACHE_SOURCES = {"", "memory", "edge", "upstream", "mixed"}

SHORT_SHA = BUILD_SHA[:7]
DISPATCHED = [
    "okToolCall", "toolErrorUnknownAction", "toolErrorProvider", "toolsList", "initialize", "pathCredential",
    "unknownTool", "nonStringToolName", "unknownMethod", "nonStringMethod", "missingToolName", "notification",
    "batchOneCall", "budget", "timeout", "retry",
]
REJECTED = ["auth401", "auth401WrongPath", "auth401WrongHeader", "method405", "size413", "batch400",
            "invalidJson400", "emptyBatch400"]

# name: (index1, domain, outcome, errorCode, credentialTransport, httpStatus)
EXPECTED = {
    "okToolCall": ("tools/call:system.health_check", "system", "ok", "", "header", 200),
    "toolErrorUnknownAction": ("tools/call:unknown", "stock_pricing", "tool_error", "INPUT_VALIDATION_ERROR", "header", 200),
    "toolErrorProvider": ("tools/call:stock_pricing.get_historical_prices", "stock_pricing", "tool_error", None, "header", 200),
    "toolsList": ("tools/list", "-", "ok", "", "header", 200),
    "initialize": ("initialize", "-", "ok", "", "header", 200),
    "pathCredential": ("tools/list", "-", "ok", "", "path", 200),
    "unknownTool": ("tools/call:unknown", "-", "tool_error", "INPUT_VALIDATION_ERROR", "header", 200),
    "nonStringToolName": ("tools/call:unknown", "-", None, None, "header", 200),
    "unknownMethod": ("rpc:other", "-", "rpc_error", "-32601", "header", 200),
    "nonStringMethod": ("rpc:other", "-", "rpc_error", "-32601", "header", 200),
    "missingToolName": ("tools/call:unknown", "-", "rpc_error", "-32602", "header", 200),
    "notification": ("rpc:other", "-", "ok", "", "header", 202),
    "batchOneCall": ("tools/call:stock_pricing.get_historical_prices", "stock_pricing", "tool_error", None, "header", 200),
    "auth401": ("rejected:auth", "-", "rejected", "AUTH_REQUIRED", "none", 401),
    "auth401WrongPath": ("rejected:auth", "-", "rejected", "AUTH_REQUIRED", "path", 401),
    "auth401WrongHeader": ("rejected:auth", "-", "rejected", "AUTH_REQUIRED", "header", 401),
    "method405": ("rejected:method", "-", "rejected", "", "header", 405),
    "size413": ("rejected:body_size", "-", "rejected", "PAYLOAD_TOO_LARGE", "header", 413),
    "batch400": ("rejected:batch", "-", "rejected", "BATCH_LIMIT_EXCEEDED", "header", 400),
    "invalidJson400": ("rejected:invalid", "-", "rejected", "-32700", "header", 400),
    "emptyBatch400": ("rejected:invalid", "-", "rejected", "-32600", "header", 400),
    "budget": ("tools/call:stock_pricing.get_historical_prices", "stock_pricing", "tool_error", "PROVIDER_BUDGET_EXHAUSTED", "header", 200),
    "timeout": ("tools/call:stock_pricing.get_historical_prices", "stock_pricing", "tool_error", None, "header", 200),
    "retry": ("tools/call:stock_pricing.get_historical_prices", "stock_pricing", "tool_error", None, "header", 200),
}


def _point(name: str) -> dict:
    points = _run()["withBinding"][name]["points"]
    assert len(points) == 1, (name, points)
    return points[0]


def _fields(point: dict) -> dict:
    blobs, doubles = point["blobs"], point["doubles"]
    return {
        "action": point["indexes"][0],
        "domain": blobs[0], "serverVersion": blobs[1], "buildShaShort": blobs[2], "outcome": blobs[3],
        "errorCode": blobs[4], "sizeBucket": blobs[5], "credentialTransport": blobs[6],
        "budgetExhausted": blobs[7], "cacheSource": blobs[8], "toolMode": blobs[9], "timeoutClass": blobs[10],
        "keyId": blobs[11],
        "latencyMs": doubles[0], "responseBytes": doubles[1], "httpStatus": doubles[2],
        "upstreamRequests": doubles[3], "subrequestsUsed": doubles[4], "budgetRefusals": doubles[5],
        "providerTimeouts": doubles[6], "edgeCacheHits": doubles[7], "edgeCacheMisses": doubles[8],
        "providerRetries": doubles[9],
    }


def _size_bucket(n: int) -> str:
    if n < 1024:
        return "<1K"
    if n < 10 * 1024:
        return "1-10K"
    if n < 100 * 1024:
        return "10-100K"
    if n < 1024 * 1024:
        return "100K-1M"
    return ">=1M"


@unittest.skipIf(_node() is None, "node and worker/node_modules (npm ci) are required")
class TestWorkerTelemetry(unittest.TestCase):
    def test_one_data_point_per_mcp_request(self):
        r = _run()["withBinding"]
        for name in DISPATCHED + REJECTED:
            self.assertEqual(len(r[name]["points"]), 1, name)
        self.assertEqual(r["health"]["points"], [], "non-/mcp routes write nothing")

    def test_layout_is_one_index_twelve_blobs_ten_doubles(self):
        for name in DISPATCHED + REJECTED:
            point = _point(name)
            self.assertEqual(sorted(point), ["blobs", "doubles", "indexes"], name)
            self.assertEqual(len(point["indexes"]), 1, name)
            self.assertEqual(len(point["blobs"]), 12, name)
            self.assertEqual(len(point["doubles"]), 10, name)
            for value in point["blobs"] + point["indexes"]:
                self.assertIsInstance(value, str, name)
            for value in point["doubles"]:
                self.assertIsInstance(value, (int, float), name)

    def test_expected_values(self):
        r = _run()["withBinding"]
        for name, (action, domain, outcome, error_code, transport, status) in EXPECTED.items():
            f = _fields(_point(name))
            self.assertEqual(f["action"], action, name)
            self.assertEqual(f["domain"], domain, name)
            if outcome is not None:
                self.assertEqual(f["outcome"], outcome, name)
            if error_code is not None:
                self.assertEqual(f["errorCode"], error_code, name)
            self.assertEqual(f["credentialTransport"], transport, name)
            self.assertEqual(f["httpStatus"], status, name)
            self.assertEqual(r[name]["status"], status, name)
            self.assertEqual(f["serverVersion"], SERVER_VERSION, name)
            self.assertEqual(f["buildShaShort"], SHORT_SHA, name)
            self.assertEqual(f["toolMode"], "grouped", name)
            body_bytes = len(r[name]["text"].encode("utf-8"))
            self.assertEqual(f["responseBytes"], body_bytes, name)
            self.assertEqual(f["sizeBucket"], _size_bucket(body_bytes), name)
            self.assertGreaterEqual(f["latencyMs"], 0, name)
            self.assertIn(f["outcome"], OUTCOMES, name)
            self.assertIn(f["sizeBucket"], SIZE_BUCKETS, name)
            self.assertIn(f["cacheSource"], CACHE_SOURCES, name)
            self.assertIn(f["timeoutClass"], {"none", "some", "all"}, name)
            self.assertIn(f["budgetExhausted"], {"0", "1"}, name)

    def test_key_id_is_owner_once_identified_else_dash(self):
        """2.18.0: blob 12 keyId. The owner key records `owner` on every outcome; no identified key records `-`."""
        unidentified = {"auth401", "auth401WrongPath", "auth401WrongHeader"}
        for name in DISPATCHED + REJECTED:
            self.assertEqual(_fields(_point(name))["keyId"], "-" if name in unidentified else "owner", name)

    def test_rejections_carry_no_counters(self):
        for name in REJECTED:
            f = _fields(_point(name))
            self.assertEqual(f["outcome"], "rejected", name)
            self.assertEqual(
                (f["upstreamRequests"], f["subrequestsUsed"], f["budgetRefusals"], f["providerTimeouts"],
                 f["edgeCacheHits"], f["edgeCacheMisses"], f["providerRetries"]),
                (0, 0, 0, 0, 0, 0, 0), name)
            self.assertEqual((f["budgetExhausted"], f["cacheSource"], f["timeoutClass"]), ("0", "", "none"), name)

    def test_ok_tool_call_without_provider_traffic(self):
        f = _fields(_point("okToolCall"))
        self.assertEqual((f["upstreamRequests"], f["providerTimeouts"], f["providerRetries"]), (0, 0, 0))
        self.assertEqual((f["budgetExhausted"], f["timeoutClass"]), ("0", "none"))

    def test_provider_failure_counts_upstream_requests(self):
        f = _fields(_point("toolErrorProvider"))
        self.assertGreater(f["upstreamRequests"], 0)
        self.assertEqual(f["subrequestsUsed"], f["upstreamRequests"])  # no cache in Node
        self.assertEqual(f["cacheSource"], "upstream")
        self.assertEqual((f["budgetExhausted"], f["providerTimeouts"], f["timeoutClass"]), ("0", 0, "none"))

    def test_budget_exhausted_when_the_guard_fired(self):
        f = _fields(_point("budget"))
        self.assertEqual(f["budgetExhausted"], "1")
        self.assertGreaterEqual(f["budgetRefusals"], 1)
        self.assertEqual(f["subrequestsUsed"], 45)
        self.assertEqual(f["errorCode"], "PROVIDER_BUDGET_EXHAUSTED")

    def test_provider_timeouts_counted(self):
        f = _fields(_point("timeout"))
        self.assertGreaterEqual(f["providerTimeouts"], 1)
        self.assertEqual(f["providerTimeouts"], f["upstreamRequests"])
        self.assertEqual(f["timeoutClass"], "all")
        self.assertEqual(f["errorCode"], "PROVIDER_TIMEOUT")

    def test_provider_retries_counted(self):
        f = _fields(_point("retry"))
        # The throttled chart request is retried once on the other Yahoo host.
        self.assertEqual((f["providerRetries"], f["upstreamRequests"]), (1, 2))
        self.assertEqual((f["providerTimeouts"], f["timeoutClass"]), (0, "none"))
        self.assertEqual(f["errorCode"], "RATE_LIMIT")

    def test_privacy_every_string_is_allowed(self):
        for name in DISPATCHED + REJECTED:
            point = _point(name)
            f = _fields(point)
            self.assertIn(f["action"], ACTION_ALLOW, name)
            self.assertIn(f["domain"], DOMAIN_ALLOW, name)
            for value in point["blobs"]:
                self.assertRegex(value, SAFE_STRING, name)

    def test_privacy_no_token_argument_or_path_recorded(self):
        r = _run()["withBinding"]
        forbidden = [TOKEN, WRONG, FAKE_TICKER, FAKE_TOOL, "evil_tool", FAKE_METHOD, "evil-method", FAKE_ACTION, FAKE_ARG,
                     "telemetry.invalid", "/mcp/", "Bearer", "5d"]
        for name in DISPATCHED + REJECTED:
            recorded = json.dumps(r[name]["points"]) + json.dumps(r[name]["logs"])
            for value in forbidden:
                self.assertNotIn(value, recorded, f"{name}: {value!r} recorded")

    def test_one_dispatch_log_line_per_dispatched_request(self):
        r = _run()["withBinding"]
        for name in DISPATCHED:
            lines = _dispatch_objects(r[name]["logs"])
            self.assertEqual(len(lines), 1, (name, r[name]["logs"]))
            line = lines[0]
            self.assertEqual(list(line), ["t", "v", "action", "domain", "build"], name)
            f = _fields(_point(name))
            self.assertEqual(line, {"t": "yfmcp", "v": 1, "action": f["action"], "domain": f["domain"], "build": SHORT_SHA}, name)
            for value in line.values():
                if isinstance(value, str):
                    self.assertRegex(value, SAFE_STRING, name)
        for name in REJECTED:
            self.assertEqual(_dispatch_objects(r[name]["logs"]), [], name)
            self.assertEqual(_dispatch_strings(r[name]["logs"]), [], name)

    def test_dispatch_log_line_is_an_object_not_a_json_string(self):
        """OPS-TEL-1 (2.14.0): Workers Logs extracts the fields of a logged object only."""
        r = _run()["withBinding"]
        for name in DISPATCHED + REJECTED:
            self.assertEqual(_dispatch_strings(r[name]["logs"]), [], name)
        for name in DISPATCHED:
            self.assertEqual([type(e) for e in r[name]["logs"] if "object" in e and e["object"].get("t") == "yfmcp"],
                             [dict], name)

    def test_dispatch_log_line_does_not_depend_on_the_binding(self):
        r = _run()
        for name in DISPATCHED + REJECTED:
            self.assertEqual(r["logsWithoutBinding"][name], r["withBinding"][name]["logs"], name)

    def test_responses_byte_identical_without_the_binding(self):
        r = _run()
        for name, with_binding in r["withBinding"].items():
            stripped = {"status": with_binding["status"], "headers": with_binding["headers"], "text": with_binding["text"]}
            self.assertEqual(r["withoutBinding"][name], stripped, name)

    def test_a_throwing_binding_changes_nothing(self):
        r = _run()
        self.assertEqual(r["withThrowingBinding"], r["withoutBinding"])


if __name__ == "__main__":
    unittest.main()
