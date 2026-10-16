#!/usr/bin/env python3
"""Worker credential support (grace and enforce modes) and request limits (2.9.0).

Drives the bundled Worker entry point (worker/src/index.ts) in Node with real
Request objects and a fake env, so the whole path from request to response is
exercised: credential resolution (Authorization: Bearer or /mcp/<token>),
grace versus enforce, the 401 response, the 1 MiB body cap, the 1 tools/call
batch cap, and the health_check credentialTransport field.

The token must never appear in any response body or header.
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

TOKEN = "tok-3f9a1c7e5b2d4086-SECRET"
WRONG = "tok-3f9a1c7e5b2d4086-WRONG"
MAX_BODY = 1_048_576

_HARNESS = r"""
const mod = await import(process.argv[2]);
const worker = mod.default;
const TOKEN = process.env.TEST_TOKEN;
const WRONG = process.env.TEST_WRONG;
const MAX_BODY = 1048576;
const ctx = { waitUntil() {}, passThroughOnException() {} };

// Any provider call goes through globalThis.fetch. The spy counts them and
// answers 503 so a tool that does run fails fast without touching the network.
let fetchCalls = 0;
globalThis.fetch = async () => { fetchCalls += 1; return new Response("unavailable", { status: 503 }); };

const GRACE = { TOOL_MODE: "grouped", MCP_TOKEN: TOKEN };
const GRACE_EXPLICIT = { ...GRACE, MCP_AUTH_MODE: "grace" };
const ENFORCE = { ...GRACE, MCP_AUTH_MODE: "enforce" };

const everything = [];   // every response seen: the token must appear in none

async function send(env, { path = "/mcp", method = "POST", headers = {}, body, contentLength } = {}) {
  const h = { ...headers };
  if (body !== undefined && method === "POST") h["Content-Type"] = "application/json";
  if (contentLength !== undefined) h["Content-Length"] = String(contentLength);
  const init = { method, headers: h };
  if (body !== undefined && method === "POST") init.body = body;
  const before = fetchCalls;
  const response = await worker.fetch(new Request(`https://example.invalid${path}`, init), env, ctx);
  const text = await response.text();
  const out = {
    status: response.status,
    headers: Object.fromEntries(response.headers.entries()),
    text,
    fetches: fetchCalls - before,
  };
  try { out.json = JSON.parse(text); } catch { out.json = null; }
  everything.push({ status: out.status, headers: out.headers, text });
  return out;
}

const rpc = (method, id = 1, params) => ({ jsonrpc: "2.0", id, method, ...(params ? { params } : {}) });
const ping = JSON.stringify(rpc("ping"));
const healthCall = (id = 1) => rpc("tools/call", id, { name: "system", arguments: { action: "health_check", params: {} } });
const quoteCall = (id, ticker) => rpc("tools/call", id, { name: "stock_pricing", arguments: { action: "get_historical_prices", params: { ticker, period: "5d", interval: "1d" } } });
const bearer = (t) => ({ Authorization: `Bearer ${t}` });

// A valid ping request of exactly `bytes` bytes.
function pingOfSize(bytes) {
  const base = JSON.stringify({ ...rpc("ping"), pad: "" });
  return base.slice(0, -2) + "x".repeat(bytes - base.length) + '"}';
}

const r = {};

// ── grace mode ──────────────────────────────────────────────────────────────
r.graceAbsent = await send(GRACE, { body: ping });
r.graceAbsentDefaultMode = await send({ TOOL_MODE: "grouped", MCP_TOKEN: TOKEN }, { body: ping });
r.graceExplicitAbsent = await send(GRACE_EXPLICIT, { body: ping });
r.graceHeaderValid = await send(GRACE, { body: ping, headers: bearer(TOKEN) });
r.gracePathValid = await send(GRACE, { path: `/mcp/${TOKEN}`, body: ping });
r.graceHeaderInvalid = await send(GRACE, { body: ping, headers: bearer(WRONG) });
r.gracePathInvalid = await send(GRACE, { path: `/mcp/${WRONG}`, body: ping });
r.graceHeaderWinsOverBadPath = await send(GRACE, { path: `/mcp/${WRONG}`, body: ping, headers: bearer(TOKEN) });
r.graceBadHeaderBeatsGoodPath = await send(GRACE, { path: `/mcp/${TOKEN}`, body: ping, headers: bearer(WRONG) });
r.graceSchemeCaseInsensitive = await send(GRACE, { body: ping, headers: { Authorization: `bEaReR ${TOKEN}` } });
r.graceHeaderWhitespaceTrimmed = await send(GRACE, { body: ping, headers: { Authorization: `  Bearer   ${TOKEN}  ` } });
r.graceNonBearerIsNotACredential = await send(GRACE, { body: ping, headers: { Authorization: "Basic dXNlcjpwYXNz" } });
r.graceTokenPrefixInvalid = await send(GRACE, { body: ping, headers: bearer(TOKEN.slice(0, -1)) });
r.graceTokenSuffixedInvalid = await send(GRACE, { body: ping, headers: bearer(TOKEN + "x") });
r.graceNoTrailingSegments = await send(GRACE, { path: `/mcp/${TOKEN}/extra`, body: ping });
r.graceEmptySegment = await send(GRACE, { path: "/mcp/", body: ping });

// ── MCP_TOKEN unset or empty ────────────────────────────────────────────────
for (const [name, env] of [["unset", { TOOL_MODE: "grouped" }], ["empty", { TOOL_MODE: "grouped", MCP_TOKEN: "" }]]) {
  r[`${name}PathForm`] = await send(env, { path: `/mcp/${WRONG}`, body: ping });
  r[`${name}PathFormWithHeader`] = await send(env, { path: `/mcp/${WRONG}`, body: ping, headers: bearer(WRONG) });
  r[`${name}HeaderInvalid`] = await send(env, { body: ping, headers: bearer(WRONG) });
  r[`${name}Absent`] = await send(env, { body: ping });
}
r.unsetEnforceAbsent = await send({ TOOL_MODE: "grouped", MCP_AUTH_MODE: "enforce" }, { body: ping });

// ── enforce mode ────────────────────────────────────────────────────────────
r.enforceAbsent = await send(ENFORCE, { body: ping });
r.enforceHeaderValid = await send(ENFORCE, { body: ping, headers: bearer(TOKEN) });
r.enforcePathValid = await send(ENFORCE, { path: `/mcp/${TOKEN}`, body: ping });
r.enforceHeaderInvalid = await send(ENFORCE, { body: ping, headers: bearer(WRONG) });
r.enforcePathInvalid = await send(ENFORCE, { path: `/mcp/${WRONG}`, body: ping });
r.enforceNonBearer = await send(ENFORCE, { body: ping, headers: { Authorization: "Basic dXNlcjpwYXNz" } });
r.enforceModeSpelledOddly = await send({ ...GRACE, MCP_AUTH_MODE: " Enforce " }, { body: ping });
r.enforceGetNoCredential = await send(ENFORCE, { method: "GET" });
r.graceGetNoCredential = await send(GRACE, { method: "GET" });

// ── unchanged routes ────────────────────────────────────────────────────────
r.healthGrace = await send(GRACE, { path: "/health", method: "GET" });
r.healthEnforce = await send(ENFORCE, { path: "/health", method: "GET" });
r.rootEnforce = await send(ENFORCE, { path: "/", method: "GET" });
r.healthWithTokenHeader = await send(ENFORCE, { path: "/health", method: "GET", headers: bearer(TOKEN) });
r.optionsMcp = await send(ENFORCE, { path: "/mcp", method: "OPTIONS" });
r.optionsMcpPath = await send(ENFORCE, { path: `/mcp/${WRONG}`, method: "OPTIONS" });
r.optionsMcpPathUnset = await send({ TOOL_MODE: "grouped" }, { path: `/mcp/${WRONG}`, method: "OPTIONS" });
r.auditNoToken = await send({ ...ENFORCE, AUDIT_TOKEN: "audit-token-xyz" }, { path: "/audit/mcp", method: "GET" });
r.auditMcpTokenDoesNotOpenIt = await send({ ...ENFORCE, AUDIT_TOKEN: "audit-token-xyz" }, { path: "/audit/mcp", method: "GET", headers: bearer(TOKEN) });

// ── body cap ────────────────────────────────────────────────────────────────
const justUnder = pingOfSize(MAX_BODY - 1);
const exactly = pingOfSize(MAX_BODY);
const justOver = pingOfSize(MAX_BODY + 1);
const sizes = { under: justUnder.length, exact: exactly.length, over: justOver.length };
r.bodyUnderNoLength = await send(GRACE, { body: justUnder });
r.bodyExactNoLength = await send(GRACE, { body: exactly });
r.bodyOverNoLength = await send(GRACE, { body: justOver });
r.bodyUnderWithLength = await send(GRACE, { body: justUnder, contentLength: justUnder.length });
r.bodyExactWithLength = await send(GRACE, { body: exactly, contentLength: exactly.length });
r.bodyOverWithLength = await send(GRACE, { body: justOver, contentLength: justOver.length });
r.bodyOverUnderstatedLength = await send(GRACE, { body: justOver, contentLength: 10 });
r.bodySmallOverstatedLength = await send(GRACE, { body: ping, contentLength: MAX_BODY + 1 });
r.bodyOverNotJson = await send(GRACE, { body: "x".repeat(MAX_BODY + 1) });
r.bodyOverEnforceNoCredential = await send(ENFORCE, { body: justOver, contentLength: justOver.length });
r.bodyOverEnforceValid = await send(ENFORCE, { body: justOver, headers: bearer(TOKEN) });
r.bodyMalformedJson = await send(GRACE, { body: "{not json" });
r.bodyEmpty = await send(GRACE, { body: "" });

// ── batch cap ───────────────────────────────────────────────────────────────
r.batchTwoCalls = await send(GRACE, { body: JSON.stringify([quoteCall(1, "AAPL"), quoteCall(2, "MSFT")]) });
r.batchTwoCallsAndOthers = await send(GRACE, { body: JSON.stringify([
  rpc("initialize", 1), healthCall(2), rpc("tools/list", 3), healthCall(4), { jsonrpc: "2.0", method: "notifications/initialized" },
]) });
r.batchTwoCallsEnforceNoCredential = await send(ENFORCE, { body: JSON.stringify([quoteCall(1, "AAPL"), quoteCall(2, "MSFT")]) });
r.batchOneCallWithNotifications = await send(GRACE, { body: JSON.stringify([
  { jsonrpc: "2.0", method: "notifications/initialized" },
  healthCall(7),
  { jsonrpc: "2.0", method: "notifications/cancelled", params: { requestId: 1 } },
]) });
r.batchNoCallsManyOthers = await send(GRACE, { body: JSON.stringify([rpc("initialize", 1), rpc("tools/list", 2), rpc("ping", 3), rpc("ping", 4)]) });
r.batchNotificationsOnly = await send(GRACE, { body: JSON.stringify([{ jsonrpc: "2.0", method: "notifications/initialized" }]) });
r.batchEmpty = await send(GRACE, { body: "[]" });
r.batchEmptyEnforceNoCredential = await send(ENFORCE, { body: "[]" });
// Control: a single call to a tool that reaches the provider is seen by the fetch spy,
// so "zero fetches" for the rejected batch above means nothing was dispatched.
r.controlSingleFetchingCall = await send(GRACE, { body: JSON.stringify(quoteCall(1, "AAPL")) });
r.controlBatchOfOneFetchingCall = await send(GRACE, { body: JSON.stringify([quoteCall(1, "NVDA")]) });

// ── health_check credentialTransport ────────────────────────────────────────
const transport = async (env, opts) => {
  const res = await send(env, { ...opts, body: JSON.stringify(healthCall()) });
  const envelope = JSON.parse(res.json.result.content[0].text);
  return { status: res.status, data: envelope.data, raw: res.text };
};
r.transportHeader = await transport(GRACE, { headers: bearer(TOKEN) });
r.transportPath = await transport(GRACE, { path: `/mcp/${TOKEN}` });
r.transportNone = await transport(GRACE, {});
r.transportHeaderWins = await transport(GRACE, { path: `/mcp/${TOKEN}`, headers: bearer(TOKEN) });
r.transportEnforcePath = await transport(ENFORCE, { path: `/mcp/${TOKEN}` });

// ── /health makes no provider call ──────────────────────────────────────────
const beforeHealth = fetchCalls;
await send(GRACE, { path: "/health", method: "GET" });
await send(GRACE, { path: "/", method: "GET" });
await send(ENFORCE, { path: "/health", method: "GET" });
await send(ENFORCE, { path: "/", method: "GET", headers: bearer(TOKEN) });
r.healthFetches = fetchCalls - beforeHealth;

// ── unit-level: resolveCredential and redactPath ────────────────────────────
const resolve = (path, headers = {}, env = { MCP_TOKEN: TOKEN }) =>
  mod.resolveCredential(new Request(`https://example.invalid${path}`, { headers }), path, env);
r.unit = {
  absent: resolve("/mcp"),
  header: resolve("/mcp", bearer(TOKEN)),
  headerBad: resolve("/mcp", bearer(WRONG)),
  path: resolve(`/mcp/${TOKEN}`),
  pathBad: resolve(`/mcp/${WRONG}`),
  headerWins: resolve(`/mcp/${WRONG}`, bearer(TOKEN)),
  pathUnsetToken: resolve(`/mcp/${WRONG}`, {}, {}),
  headerUnsetToken: resolve("/mcp", bearer(WRONG), {}),
  absentUnsetToken: resolve("/mcp", {}, {}),
  trailing: resolve(`/mcp/${TOKEN}/x`),
  emptySegment: resolve("/mcp/"),
  health: resolve("/health"),
  redacted: mod.redactPath(`/mcp/${TOKEN}`),
  redactedPlain: mod.redactPath("/mcp"),
  redactedOther: mod.redactPath("/health"),
  redactedTrailing: mod.redactPath(`/mcp/${TOKEN}/x`),
  timingEqual: mod.timingSafeEqual(TOKEN, TOKEN),
  timingDifferent: mod.timingSafeEqual(TOKEN, WRONG),
  timingPrefix: mod.timingSafeEqual(TOKEN, TOKEN.slice(0, -1)),
  timingEmpty: mod.timingSafeEqual("", ""),
};

// ── leak scan ───────────────────────────────────────────────────────────────
r.leaks = everything.filter((x) => {
  const blob = x.text + JSON.stringify(x.headers);
  return blob.includes(TOKEN) || blob.includes(WRONG);
}).length;
r.responsesScanned = everything.length;
r.sizes = sizes;

console.log(JSON.stringify(r));
"""

AUTH_REQUIRED_MESSAGE = (
    "AUTH_REQUIRED: a valid credential is required (Authorization: Bearer <token> or /mcp/<token>)."
)
AUTH_REQUIRED_BODY = (
    '{"jsonrpc":"2.0","id":null,"error":{"code":-32001,"message":"'
    + AUTH_REQUIRED_MESSAGE
    + '","data":{"code":"AUTH_REQUIRED"}}}'
)


def _run_harness() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".auth-test-") as tmp:
        entry = Path(tmp) / "entry.ts"
        bundle = Path(tmp) / "worker.mjs"
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        entry.write_text(
            'export { default } from "../src/index.ts";\nexport * from "../src/auth.ts";\n',
            encoding="utf-8",
        )
        subprocess.run(
            [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=node",
             f"--outfile={bundle}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
        env = {**os.environ, "TEST_TOKEN": TOKEN, "TEST_WRONG": WRONG}
        result = subprocess.run(
            [node, str(harness), bundle.as_uri()],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=300, env=env,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


class WorkerAuthCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.r = _run_harness()

    def served(self, name: str) -> None:
        res = self.r[name]
        self.assertEqual(res["status"], 200, f"{name}: {res['text'][:200]}")
        self.assertEqual(res["json"], {"jsonrpc": "2.0", "id": 1, "result": {}}, name)

    def unauthorized(self, name: str) -> None:
        res = self.r[name]
        self.assertEqual(res["status"], 401, f"{name}: {res['text'][:200]}")
        self.assertEqual(res["text"], AUTH_REQUIRED_BODY, name)

    def error_code(self, name: str) -> str | None:
        return (((self.r[name]["json"] or {}).get("error") or {}).get("data") or {}).get("code")


class TestGraceMode(WorkerAuthCase):
    def test_absent_credential_is_served(self) -> None:
        self.served("graceAbsent")
        self.served("graceAbsentDefaultMode")  # MCP_AUTH_MODE unset means grace
        self.served("graceExplicitAbsent")

    def test_valid_header_is_served(self) -> None:
        self.served("graceHeaderValid")

    def test_valid_path_is_served(self) -> None:
        self.served("gracePathValid")

    def test_invalid_header_is_rejected(self) -> None:
        self.unauthorized("graceHeaderInvalid")

    def test_invalid_path_is_rejected(self) -> None:
        self.unauthorized("gracePathInvalid")

    def test_header_wins_when_both_are_present(self) -> None:
        self.served("graceHeaderWinsOverBadPath")
        self.unauthorized("graceBadHeaderBeatsGoodPath")

    def test_scheme_is_case_insensitive_and_whitespace_is_trimmed(self) -> None:
        self.served("graceSchemeCaseInsensitive")
        self.served("graceHeaderWhitespaceTrimmed")

    def test_other_authorization_schemes_are_not_credentials(self) -> None:
        self.served("graceNonBearerIsNotACredential")

    def test_near_misses_are_rejected(self) -> None:
        self.unauthorized("graceTokenPrefixInvalid")
        self.unauthorized("graceTokenSuffixedInvalid")

    def test_path_form_takes_exactly_one_segment(self) -> None:
        self.assertEqual(self.r["graceNoTrailingSegments"]["status"], 404)
        self.assertEqual(self.r["graceEmptySegment"]["status"], 404)


class TestTokenNotConfigured(WorkerAuthCase):
    def test_path_form_is_not_found(self) -> None:
        for name in ("unsetPathForm", "unsetPathFormWithHeader", "emptyPathForm", "emptyPathFormWithHeader"):
            self.assertEqual(self.r[name]["status"], 404, name)
            self.assertEqual(self.r[name]["text"], "Not Found", name)

    def test_header_credential_is_invalid(self) -> None:
        self.unauthorized("unsetHeaderInvalid")
        self.unauthorized("emptyHeaderInvalid")

    def test_absent_credential_is_served_in_grace(self) -> None:
        self.served("unsetAbsent")
        self.served("emptyAbsent")

    def test_enforce_without_a_token_rejects_everything(self) -> None:
        self.unauthorized("unsetEnforceAbsent")


class TestEnforceMode(WorkerAuthCase):
    def test_absent_credential_is_rejected(self) -> None:
        self.unauthorized("enforceAbsent")
        self.unauthorized("enforceNonBearer")

    def test_valid_header_or_path_is_served(self) -> None:
        self.served("enforceHeaderValid")
        self.served("enforcePathValid")

    def test_invalid_credential_is_rejected(self) -> None:
        self.unauthorized("enforceHeaderInvalid")
        self.unauthorized("enforcePathInvalid")

    def test_mode_value_is_trimmed_and_case_insensitive(self) -> None:
        self.unauthorized("enforceModeSpelledOddly")

    def test_auth_is_checked_before_the_method(self) -> None:
        self.unauthorized("enforceGetNoCredential")
        self.assertEqual(self.r["graceGetNoCredential"]["status"], 405)


class TestUnauthorizedResponse(WorkerAuthCase):
    def test_body_and_headers_exactly(self) -> None:
        res = self.r["enforceAbsent"]
        self.assertEqual(res["status"], 401)
        self.assertEqual(res["text"], AUTH_REQUIRED_BODY)
        headers = res["headers"]
        self.assertEqual(headers["www-authenticate"], 'Bearer realm="yahoo-finance-mcp"')
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(headers["access-control-allow-origin"], "*")
        self.assertEqual(headers["access-control-allow-methods"], "GET, POST, OPTIONS")
        self.assertEqual(headers["access-control-allow-headers"], "Content-Type, Mcp-Session-Id")
        self.assertEqual(headers["access-control-max-age"], "86400")
        self.assertEqual(headers["access-control-expose-headers"], "X-Yahoo-Cache, X-Sec-Cache")

    def test_body_parses_to_the_specified_error(self) -> None:
        self.assertEqual(self.r["enforceAbsent"]["json"], {
            "jsonrpc": "2.0", "id": None,
            "error": {"code": -32001, "message": AUTH_REQUIRED_MESSAGE, "data": {"code": "AUTH_REQUIRED"}},
        })

    def test_every_rejection_is_identical(self) -> None:
        names = ("graceHeaderInvalid", "gracePathInvalid", "graceTokenPrefixInvalid", "enforceAbsent",
                 "enforceHeaderInvalid", "enforcePathInvalid", "unsetHeaderInvalid")
        for name in names:
            self.assertEqual(self.r[name]["text"], AUTH_REQUIRED_BODY, name)
            self.assertEqual(self.r[name]["headers"], self.r["enforceAbsent"]["headers"], name)

    def test_no_provider_call_was_made(self) -> None:
        for name in ("graceHeaderInvalid", "enforceAbsent", "enforcePathInvalid"):
            self.assertEqual(self.r[name]["fetches"], 0, name)


class TestTokenNeverLeaks(WorkerAuthCase):
    def test_token_is_in_no_response_body_or_header(self) -> None:
        self.assertGreater(self.r["responsesScanned"], 60)
        self.assertEqual(self.r["leaks"], 0)

    def test_health_output_does_not_mention_credentials(self) -> None:
        for name in ("healthGrace", "healthEnforce", "rootEnforce", "healthWithTokenHeader"):
            body = self.r[name]["json"]
            self.assertEqual(self.r[name]["status"], 200, name)
            blob = json.dumps(body).lower()
            for word in ("token", "credential", "auth", "secret"):
                self.assertNotIn(word, blob, f"{name}: {word}")

    def test_health_check_tool_does_not_echo_the_credential(self) -> None:
        for name in ("transportHeader", "transportPath", "transportNone"):
            self.assertNotIn(TOKEN, self.r[name]["raw"])


class TestUnchangedRoutes(WorkerAuthCase):
    def test_health_and_root_are_unauthenticated(self) -> None:
        for name in ("healthGrace", "healthEnforce", "rootEnforce", "healthWithTokenHeader"):
            self.assertEqual(self.r[name]["status"], 200, name)
            self.assertEqual(self.r[name]["json"]["status"], "ok", name)

    def test_health_makes_no_provider_call(self) -> None:
        self.assertEqual(self.r["healthFetches"], 0)
        for name in ("healthGrace", "healthEnforce", "rootEnforce"):
            self.assertEqual(self.r[name]["fetches"], 0, name)

    def test_preflight_is_answered_for_the_path_form(self) -> None:
        for name in ("optionsMcp", "optionsMcpPath", "optionsMcpPathUnset"):
            res = self.r[name]
            self.assertEqual(res["status"], 204, name)
            self.assertEqual(res["headers"]["access-control-allow-origin"], "*", name)
            self.assertEqual(res["headers"]["access-control-allow-methods"], "GET, POST, OPTIONS", name)
        self.assertEqual(self.r["optionsMcp"]["headers"], self.r["optionsMcpPath"]["headers"])

    def test_audit_keeps_its_own_token(self) -> None:
        self.assertEqual(self.r["auditNoToken"]["status"], 401)
        self.assertEqual(self.r["auditNoToken"]["text"], "Unauthorized")
        self.assertEqual(self.r["auditMcpTokenDoesNotOpenIt"]["status"], 401)


class TestBodyCap(WorkerAuthCase):
    def test_test_payloads_have_the_intended_sizes(self) -> None:
        self.assertEqual(self.r["sizes"], {"under": MAX_BODY - 1, "exact": MAX_BODY, "over": MAX_BODY + 1})

    def test_at_or_under_the_cap_is_served(self) -> None:
        for name in ("bodyUnderNoLength", "bodyExactNoLength", "bodyUnderWithLength", "bodyExactWithLength"):
            self.served(name)

    def test_over_the_cap_is_rejected_with_and_without_content_length(self) -> None:
        for name in ("bodyOverNoLength", "bodyOverWithLength", "bodyOverUnderstatedLength",
                     "bodySmallOverstatedLength", "bodyOverNotJson"):
            res = self.r[name]
            self.assertEqual(res["status"], 413, name)
            self.assertEqual(res["json"], {
                "jsonrpc": "2.0", "id": None,
                "error": {"code": -32600, "message": f"PAYLOAD_TOO_LARGE: request body exceeds {MAX_BODY} bytes.",
                          "data": {"code": "PAYLOAD_TOO_LARGE"}},
            }, name)
            self.assertEqual(res["headers"]["access-control-allow-origin"], "*", name)

    def test_declared_length_alone_is_enough_to_reject(self) -> None:
        self.assertEqual(self.error_code("bodySmallOverstatedLength"), "PAYLOAD_TOO_LARGE")

    def test_auth_comes_before_the_cap(self) -> None:
        self.unauthorized("bodyOverEnforceNoCredential")
        self.assertEqual(self.r["bodyOverEnforceValid"]["status"], 413)

    def test_cap_comes_before_parsing(self) -> None:
        self.assertEqual(self.r["bodyOverNotJson"]["status"], 413)

    def test_parse_errors_are_unchanged(self) -> None:
        for name in ("bodyMalformedJson", "bodyEmpty"):
            self.assertEqual(self.r[name]["status"], 400, name)
            self.assertEqual(self.r[name]["json"]["error"], {"code": -32700, "message": "Parse error"}, name)


class TestBatchCap(WorkerAuthCase):
    def test_two_tool_calls_are_rejected_and_nothing_executes(self) -> None:
        for name in ("batchTwoCalls", "batchTwoCallsAndOthers"):
            res = self.r[name]
            self.assertEqual(res["status"], 400, name)
            self.assertEqual(res["json"], {
                "jsonrpc": "2.0", "id": None,
                "error": {"code": -32600, "message": "BATCH_LIMIT_EXCEEDED: at most 1 tools/call per request.",
                          "data": {"code": "BATCH_LIMIT_EXCEEDED"}},
            }, name)
            self.assertEqual(res["fetches"], 0, f"{name}: a tool was dispatched")

    def test_the_spy_does_see_a_dispatched_call(self) -> None:
        # Control for the test above: one call to the same tool reaches the provider.
        self.assertGreater(self.r["controlSingleFetchingCall"]["fetches"], 0)
        self.assertGreater(self.r["controlBatchOfOneFetchingCall"]["fetches"], 0)

    def test_one_tool_call_with_notifications_is_served(self) -> None:
        res = self.r["batchOneCallWithNotifications"]
        self.assertEqual(res["status"], 200)
        self.assertEqual([item["id"] for item in res["json"]], [7])
        self.assertNotIn("error", res["json"][0])

    def test_other_messages_in_a_batch_are_unrestricted(self) -> None:
        res = self.r["batchNoCallsManyOthers"]
        self.assertEqual(res["status"], 200)
        self.assertEqual([item["id"] for item in res["json"]], [1, 2, 3, 4])

    def test_notification_only_batch_is_still_accepted(self) -> None:
        self.assertEqual(self.r["batchNotificationsOnly"]["status"], 202)

    def test_empty_batch_is_an_invalid_request(self) -> None:
        res = self.r["batchEmpty"]
        self.assertEqual(res["status"], 400)
        self.assertEqual(res["json"]["id"], None)
        self.assertEqual(res["json"]["error"]["code"], -32600)

    def test_auth_comes_before_the_batch_cap(self) -> None:
        self.unauthorized("batchTwoCallsEnforceNoCredential")
        self.unauthorized("batchEmptyEnforceNoCredential")


class TestCredentialTransport(WorkerAuthCase):
    def transport(self, name: str) -> object:
        res = self.r[name]
        self.assertEqual(res["status"], 200, name)
        return res["data"]["credentialTransport"]

    def test_header(self) -> None:
        self.assertEqual(self.transport("transportHeader"), "header")

    def test_path(self) -> None:
        self.assertEqual(self.transport("transportPath"), "path")
        self.assertEqual(self.transport("transportEnforcePath"), "path")

    def test_none(self) -> None:
        self.assertEqual(self.transport("transportNone"), "none")

    def test_header_wins(self) -> None:
        self.assertEqual(self.transport("transportHeaderWins"), "header")

    def test_authorization_header_presence_is_unchanged(self) -> None:
        self.assertIs(self.r["transportHeader"]["data"]["authorizationHeaderPresent"], True)
        self.assertIs(self.r["transportPath"]["data"]["authorizationHeaderPresent"], False)
        self.assertIs(self.r["transportNone"]["data"]["authorizationHeaderPresent"], False)


class TestAuthModule(WorkerAuthCase):
    def test_resolve_credential_results(self) -> None:
        u = self.r["unit"]
        mcp = lambda transport, status: {"route": "mcp", "transport": transport, "status": status}  # noqa: E731
        self.assertEqual(u["absent"], mcp("none", "absent"))
        self.assertEqual(u["header"], mcp("header", "valid"))
        self.assertEqual(u["headerBad"], mcp("header", "invalid"))
        self.assertEqual(u["path"], mcp("path", "valid"))
        self.assertEqual(u["pathBad"], mcp("path", "invalid"))
        self.assertEqual(u["headerWins"], mcp("header", "valid"))
        self.assertEqual(u["headerUnsetToken"], mcp("header", "invalid"))
        self.assertEqual(u["absentUnsetToken"], mcp("none", "absent"))
        for name in ("pathUnsetToken", "trailing", "emptySegment", "health"):
            self.assertEqual(u[name], {"route": "other"}, name)

    def test_redact_path(self) -> None:
        u = self.r["unit"]
        self.assertEqual(u["redacted"], "/mcp/<redacted>")
        self.assertEqual(u["redactedTrailing"], "/mcp/<redacted>/x")
        self.assertEqual(u["redactedPlain"], "/mcp")
        self.assertEqual(u["redactedOther"], "/health")
        self.assertNotIn(TOKEN, json.dumps(u["redacted"]))

    def test_timing_safe_equal(self) -> None:
        u = self.r["unit"]
        self.assertIs(u["timingEqual"], True)
        self.assertIs(u["timingDifferent"], False)
        self.assertIs(u["timingPrefix"], False)
        self.assertIs(u["timingEmpty"], True)

    def test_comparison_is_constant_time_by_construction(self) -> None:
        source = (WORKER / "src" / "auth.ts").read_text(encoding="utf-8")
        body = source[source.index("export function timingSafeEqual"):source.index("export function redactPath")]
        self.assertIn("diff |=", body)
        self.assertNotIn("===", body.replace("diff === 0", ""))  # no early-exit comparison
        self.assertNotIn("return false", body)


if __name__ == "__main__":
    unittest.main()
