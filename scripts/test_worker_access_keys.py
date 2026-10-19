#!/usr/bin/env python3
"""Per-person access keys (2.18.0, Phase 1b), Worker only.

Bundles worker/src/index.ts and drives it in Node with real Request objects,
a fake MCP_TOKEN, a fake ACCESS_KEYS KV binding (records keyed by the SHA-256
of obviously fake keys), a fake USAGE Durable Object namespace that runs the
real exported KeyUsage class over in-memory storage, a controllable clock, a
fetch spy (nothing reaches the network), a recording TELEMETRY binding and a
captured console.

- The owner key works in header and path form with no store or counter call.
- A valid personal key works in header and path form.
- Unknown and revoked keys get the same 401 as a wrong token.
- Over the per-minute or daily limit: 429 KEY_RATE_LIMITED with Retry-After.
- Revoking one key leaves another working.
- paidProviders false: Finnhub reads as unconfigured; true: Finnhub is attempted.
- Without either binding only the owner key works.
- The 60-second isolate cache serves repeat lookups (found and not found).
- Telemetry blob 12 carries keyId (owner, the 8-char id, or -) and never a
  key, hash or label; neither does any response or log line.
- A failing store or counter, or a malformed record, fails closed through the
  exception path (500, outcome exception); the owner key is unaffected.
"""

from __future__ import annotations

import hashlib
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

# Obviously fake credentials: never real keys.
OWNER = "tok-owner-fake-0000-NOT-A-REAL-TOKEN"
WRONG = "tok-owner-fake-0000-WRONG"
KEYS = {name: "yfk_" + ch * 43 for name, ch in {
    "alice": "A", "bob": "B", "revoked": "C", "unknown": "D", "minute": "E", "daily": "F",
    "paidOff": "G", "paidOn": "H", "cache": "J", "malformed": "K", "telemetry": "L", "injected": "M",
}.items()}
LABEL_PREFIX = "label-PRIVATE-"


def _hash(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def _record(name: str, **overrides) -> dict:
    record = {
        "id": _hash(KEYS[name])[:8], "label": LABEL_PREFIX + name, "createdAt": "2026-10-18T00:00:00Z",
        "status": "active", "dailyLimit": 2000, "perMinuteLimit": 30, "paidProviders": False,
    }
    record.update(overrides)
    return record


RECORDS = {
    "alice": _record("alice"),
    "bob": _record("bob"),
    "revoked": _record("revoked", status="revoked"),
    "minute": _record("minute", perMinuteLimit=2, dailyLimit=100),
    "daily": _record("daily", perMinuteLimit=100, dailyLimit=3),
    "paidOff": _record("paidOff", paidProviders=False),
    "paidOn": _record("paidOn", paidProviders=True),
    "cache": _record("cache"),
    "malformed": {"id": "not-hex", "label": LABEL_PREFIX + "malformed"},
    "telemetry": _record("telemetry", perMinuteLimit=1, dailyLimit=100),
    "injected": _record("injected", paidProviders=True),
}

_HARNESS = r"""
const [bundleUrl] = process.argv.slice(-1);
const mod = await import(bundleUrl);
const worker = mod.default;
const ak = mod.accessKeys;
const T = JSON.parse(process.env.TEST_DATA);
const ctx = { waitUntil() {}, passThroughOnException() {} };

// Controllable clock for the counter windows and the isolate cache.
let NOW = Date.parse("2026-10-18T10:00:10Z");
Date.now = () => NOW;

const logs = [];
for (const level of ["log", "info", "warn", "error", "debug"]) {
  console[level] = (...args) => logs.push(args.map((a) => typeof a === "string" ? a : JSON.stringify(a)).join(" "));
}

const fetched = [];
globalThis.fetch = async (input) => { fetched.push(new URL(String(input)).hostname); return new Response("unavailable", { status: 503 }); };

function fakeKv(records) {
  const kv = {
    gets: 0,
    mode: "ok",
    data: new Map(Object.entries(records)),
    async get(key, type) {
      kv.gets += 1;
      if (kv.mode === "throw") throw new Error("kv down");
      const v = kv.data.get(key);
      if (v === undefined) return null;
      return type === "json" ? JSON.parse(v) : v;
    },
  };
  return kv;
}

function fakeUsage() {
  const instances = new Map();
  const ns = {
    calls: 0,
    mode: "ok",
    idFromName(name) { ns.calls += 1; return name; },
    get(id) {
      if (!instances.has(id)) {
        const data = new Map();
        const storage = { async get(k) { return data.get(k); }, async put(k, v) { data.set(k, structuredClone(v)); } };
        instances.set(id, new mod.KeyUsage({ storage }, {}));
      }
      const obj = instances.get(id);
      return {
        fetch: async (url, init) => {
          if (ns.mode === "throw") throw new Error("do down");
          return obj.fetch(new Request(url, init));
        },
      };
    },
  };
  return ns;
}

const kvRecords = Object.fromEntries(Object.entries(T.records).map(([name, rec]) => [`key:${T.hashes[name]}`, JSON.stringify(rec)]));
const KV = fakeKv(kvRecords);
const USAGE = fakeUsage();
const telemetry = { points: [], writeDataPoint(p) { this.points.push(JSON.parse(JSON.stringify(p))); } };
const BASE = {
  TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true", MCP_AUTH_MODE: "enforce", MCP_TOKEN: T.owner,
  FINNHUB_API_KEY: "fake-finnhub-key-not-real", SERVER_VERSION: "2.18.0",
};
const FULL = { ...BASE, ACCESS_KEYS: KV, USAGE, TELEMETRY: telemetry };

const everything = [];
async function send(env, { path = "/mcp", method = "POST", headers = {}, body = { jsonrpc: "2.0", id: 1, method: "ping" } } = {}) {
  const h = { ...headers };
  const init = { method, headers: h };
  if (method === "POST") { h["Content-Type"] = "application/json"; init.body = JSON.stringify(body); }
  const points = telemetry.points.length;
  const kvBefore = KV.gets;
  const usageBefore = USAGE.calls;
  const fetchedBefore = fetched.length;
  let response;
  try {
    response = await worker.fetch(new Request(`https://keys.invalid${path}`, init), env, ctx);
  } catch (error) {
    const out = { threw: true, message: String(error && error.message), points: telemetry.points.slice(points), kvGets: KV.gets - kvBefore, usageCalls: USAGE.calls - usageBefore };
    everything.push(out.message);
    return out;
  }
  const text = await response.text();
  const out = {
    status: response.status,
    headers: Object.fromEntries(response.headers.entries()),
    text,
    points: telemetry.points.slice(points),
    kvGets: KV.gets - kvBefore,
    usageCalls: USAGE.calls - usageBefore,
    fetched: fetched.slice(fetchedBefore),
  };
  everything.push(text + JSON.stringify(out.headers));
  return out;
}
const bearer = (t) => ({ Authorization: `Bearer ${t}` });
const K = T.keys;
const ownership = (ticker) => ({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "stock_fundamentals", arguments: { action: "get_expanded_institutional_ownership", params: { ticker } } } });
const r = {};

// ── owner key: no store or counter call ─────────────────────────────────────
r.ownerHeader = await send(FULL, { headers: bearer(T.owner) });
r.ownerPath = await send(FULL, { path: `/mcp/${T.owner}` });

// ── personal key: header and path ───────────────────────────────────────────
r.aliceHeader = await send(FULL, { headers: bearer(K.alice) });
r.alicePath = await send(FULL, { path: `/mcp/${K.alice}` });

// ── unknown, revoked and wrong all get the same 401 ─────────────────────────
r.wrongToken = await send(FULL, { headers: bearer(T.wrong) });
r.unknownKey = await send(FULL, { headers: bearer(K.unknown) });
r.revokedKey = await send(FULL, { headers: bearer(K.revoked) });
r.unknownKeyPath = await send(FULL, { path: `/mcp/${K.unknown}` });

// ── per-minute limit ────────────────────────────────────────────────────────
NOW = Date.parse("2026-10-18T10:00:10Z");
r.minute1 = await send(FULL, { headers: bearer(K.minute) });
r.minute2 = await send(FULL, { headers: bearer(K.minute) });
r.minute3 = await send(FULL, { headers: bearer(K.minute) });
r.minute3Path = await send(FULL, { path: `/mcp/${K.minute}` });
NOW = Date.parse("2026-10-18T10:01:00Z");
r.minuteNext = await send(FULL, { headers: bearer(K.minute) });

// ── daily limit ─────────────────────────────────────────────────────────────
NOW = Date.parse("2026-10-18T23:59:30Z");
r.daily1 = await send(FULL, { headers: bearer(K.daily) });
NOW = Date.parse("2026-10-18T23:59:31Z");
r.daily2 = await send(FULL, { headers: bearer(K.daily) });
r.daily3 = await send(FULL, { headers: bearer(K.daily) });
r.daily4 = await send(FULL, { headers: bearer(K.daily) });
NOW = Date.parse("2026-10-19T00:00:01Z");
r.dailyNextDay = await send(FULL, { headers: bearer(K.daily) });

// ── revoking one key leaves another working ─────────────────────────────────
NOW = Date.parse("2026-10-19T09:00:00Z");
r.bobBefore = await send(FULL, { headers: bearer(K.bob) });
r.aliceBefore = await send(FULL, { headers: bearer(K.alice) });
const aliceRecord = JSON.parse(KV.data.get(`key:${T.hashes.alice}`));
KV.data.set(`key:${T.hashes.alice}`, JSON.stringify({ ...aliceRecord, status: "revoked" }));
r.aliceCachedStillActive = await send(FULL, { headers: bearer(K.alice) });
NOW += 61_000;  // past the 60-second isolate cache
r.aliceRevoked = await send(FULL, { headers: bearer(K.alice) });
r.bobAfter = await send(FULL, { headers: bearer(K.bob) });

// ── paid providers ──────────────────────────────────────────────────────────
r.paidOff = await send(FULL, { headers: bearer(K.paidOff), body: ownership("QZXA") });
r.paidOn = await send(FULL, { headers: bearer(K.paidOn), body: ownership("QZXB") });

// ── without the bindings only the owner key works ───────────────────────────
const NO_KV = { ...BASE, USAGE };
const NO_DO = { ...BASE, ACCESS_KEYS: KV };
const NEITHER = { ...BASE };
for (const [name, env] of [["noKv", NO_KV], ["noDo", NO_DO], ["neither", NEITHER]]) {
  r[`${name}Key`] = await send(env, { headers: bearer(K.bob) });
  r[`${name}KeyPath`] = await send(env, { path: `/mcp/${K.bob}` });
  r[`${name}Owner`] = await send(env, { headers: bearer(T.owner) });
}

// ── the isolate cache ───────────────────────────────────────────────────────
NOW = Date.parse("2026-10-19T12:00:00Z");
ak.clearAccessKeyCacheForTests();
r.cacheFirst = await send(FULL, { headers: bearer(K.cache) });
r.cacheSecond = await send(FULL, { headers: bearer(K.cache) });
NOW += 59_000;
r.cacheWithinTtl = await send(FULL, { headers: bearer(K.cache) });
NOW += 2_000;
r.cacheExpired = await send(FULL, { headers: bearer(K.cache) });
r.negativeFirst = await send(FULL, { headers: bearer(K.unknown) });
r.negativeSecond = await send(FULL, { headers: bearer(K.unknown) });

// ── telemetry keyId ─────────────────────────────────────────────────────────
NOW = Date.parse("2026-10-19T13:00:05Z");
r.telOwner = await send(FULL, { headers: bearer(T.owner) });
r.telKey = await send(FULL, { headers: bearer(K.telemetry) });
r.telKeyLimited = await send(FULL, { headers: bearer(K.telemetry) });
NOW += 60_000;
r.telKeyMethod = await send(FULL, { method: "GET", headers: bearer(K.telemetry) });
r.telWrong = await send(FULL, { headers: bearer(T.wrong) });
r.telAnonymous = await send(FULL, {});
r.telGraceAnonymous = await send({ ...FULL, MCP_AUTH_MODE: "grace" }, {});

// ── fail closed ─────────────────────────────────────────────────────────────
ak.clearAccessKeyCacheForTests();
r.malformed = await send(FULL, { headers: bearer(K.malformed) });
KV.mode = "throw";
r.kvThrows = await send(FULL, { headers: bearer(K.bob) });
r.kvThrowsOwner = await send(FULL, { headers: bearer(T.owner) });
KV.mode = "ok";
USAGE.mode = "throw";
r.doThrows = await send(FULL, { headers: bearer(K.bob) });
r.doThrowsOwner = await send(FULL, { headers: bearer(T.owner) });
USAGE.mode = "ok";

// ── injected store and counter (setAccessKeyStoreForTests / setUsageCounterForTests) ──
const spy = { store: 0, counter: 0 };
ak.setAccessKeyStoreForTests({ async get(hash) { spy.store += 1; return hash === T.hashes.injected ? T.records.injected : null; } });
ak.setUsageCounterForTests({ async checkAndIncrement() { spy.counter += 1; return { allowed: true, retryAfterSeconds: 0 }; } });
r.injectedKey = await send(NEITHER, { headers: bearer(K.injected) });
r.injectedOwner = await send(NEITHER, { headers: bearer(T.owner) });
r.injectedSpy = { ...spy };
ak.setAccessKeyStoreForTests(null);
r.injectedForcedAbsent = await send(FULL, { headers: bearer(K.bob) });
ak.setAccessKeyStoreForTests(undefined);
ak.setUsageCounterForTests(undefined);

// ── unit level ──────────────────────────────────────────────────────────────
const w = ak.emptyUsageWindow();
const t0 = Date.parse("2026-10-18T10:00:59.500Z");
r.unit = {
  shape: [K.alice, "yfk_short", "yfk_" + "A".repeat(44), "xfk_" + "A".repeat(43), "yfk_" + "+".repeat(43)].map((k) => ak.ACCESS_KEY_SHAPE.test(k)),
  window: [
    ak.checkAndIncrementWindow(w, t0, 1, 10),
    ak.checkAndIncrementWindow(w, t0, 1, 10),
    ak.checkAndIncrementWindow(w, t0 + 600, 1, 10),
  ],
  zeroDaily: ak.checkAndIncrementWindow(ak.emptyUsageWindow(), Date.parse("2026-10-18T12:00:00Z"), 5, 0),
  sameClass: mod.KeyUsage === ak.KeyUsage,
  ttl: ak.ACCESS_KEY_CACHE_TTL_MS,
  hash: await ak.sha256Hex(K.alice),
};

r.logs = logs;
r.everything = everything;
process.stdout.write(JSON.stringify(r) + "\n");
"""

AUTH_REQUIRED_BODY = (
    '{"jsonrpc":"2.0","id":null,"error":{"code":-32001,"message":"AUTH_REQUIRED: a valid credential is required '
    '(Authorization: Bearer <token> or /mcp/<token>).","data":{"code":"AUTH_REQUIRED"}}}'
)
PING_OK = {"jsonrpc": "2.0", "id": 1, "result": {}}


def _rate_limited_body(seconds: int) -> str:
    return ('{"jsonrpc":"2.0","id":null,"error":{"code":-32029,"message":"Key rate limit reached.",'
            '"data":{"code":"KEY_RATE_LIMITED","retryAfterSeconds":' + str(seconds) + '}}}')


def _node() -> str | None:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    return node if node and ESBUILD.exists() else None


_RESULT: dict = {}


def _run() -> dict:
    if _RESULT:
        return _RESULT
    node = _node()
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".access-keys-test-") as tmp:
        tmp_path = Path(tmp)
        entry = tmp_path / "entry.ts"
        entry.write_text(
            'export { default, KeyUsage } from "../src/index.ts";\n'
            'export * as accessKeys from "../src/access-keys.ts";\n',
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
        data = {
            "owner": OWNER, "wrong": WRONG, "keys": KEYS, "records": RECORDS,
            "hashes": {name: _hash(key) for name, key in KEYS.items()},
        }
        env = {**os.environ, "TEST_DATA": json.dumps(data)}
        proc = subprocess.run([node, str(harness), bundle.as_uri()], cwd=WORKER,
                              capture_output=True, text=True, timeout=300, env=env)
        if proc.returncode != 0:
            raise AssertionError(f"harness failed:\n{proc.stderr[-4000:]}")
        _RESULT.update(json.loads(proc.stdout.strip().splitlines()[-1]))
    return _RESULT


def _blobs(res: dict) -> list[str]:
    assert len(res["points"]) == 1, res["points"]
    return res["points"][0]["blobs"]


@unittest.skipIf(_node() is None, "node and worker/node_modules (npm ci) are required")
class TestAccessKeys(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.r = _run()

    def served(self, name: str) -> None:
        res = self.r[name]
        self.assertEqual(res.get("status"), 200, f"{name}: {res}")
        self.assertEqual(json.loads(res["text"]), PING_OK, name)

    def unauthorized(self, name: str) -> None:
        res = self.r[name]
        self.assertEqual(res.get("status"), 401, f"{name}: {res}")
        self.assertEqual(res["text"], AUTH_REQUIRED_BODY, name)
        self.assertEqual(res["headers"]["www-authenticate"], 'Bearer realm="yahoo-finance-mcp"', name)

    def rate_limited(self, name: str, seconds: int) -> None:
        res = self.r[name]
        self.assertEqual(res.get("status"), 429, f"{name}: {res}")
        self.assertEqual(res["text"], _rate_limited_body(seconds), name)
        self.assertEqual(res["headers"]["retry-after"], str(seconds), name)
        self.assertNotIn("www-authenticate", res["headers"], name)

    def test_owner_key_makes_no_store_or_counter_call(self):
        for name in ("ownerHeader", "ownerPath"):
            self.served(name)
            self.assertEqual((self.r[name]["kvGets"], self.r[name]["usageCalls"]), (0, 0), name)

    def test_valid_personal_key_header_and_path(self):
        self.served("aliceHeader")
        self.served("alicePath")
        self.assertEqual(self.r["aliceHeader"]["usageCalls"], 1)
        self.assertEqual(self.r["alicePath"]["usageCalls"], 1)

    def test_unknown_and_revoked_keys_get_the_wrong_token_401(self):
        for name in ("wrongToken", "unknownKey", "revokedKey", "unknownKeyPath"):
            self.unauthorized(name)
        self.assertEqual(self.r["unknownKey"]["headers"], self.r["wrongToken"]["headers"])
        self.assertEqual(self.r["revokedKey"]["headers"], self.r["wrongToken"]["headers"])
        # A credential without the yfk_ shape never reaches the store; a revoked key never reaches the counter.
        self.assertEqual(self.r["wrongToken"]["kvGets"], 0)
        self.assertEqual(self.r["revokedKey"]["usageCalls"], 0)

    def test_per_minute_limit(self):
        self.served("minute1")
        self.served("minute2")
        self.rate_limited("minute3", 50)       # 10:00:10 → the minute ends at 10:01:00
        self.rate_limited("minute3Path", 50)
        self.served("minuteNext")

    def test_daily_limit(self):
        self.served("daily1")
        self.served("daily2")
        self.served("daily3")
        self.rate_limited("daily4", 29)        # 23:59:31 → UTC midnight
        self.served("dailyNextDay")

    def test_revoking_one_key_leaves_another_working(self):
        self.served("bobBefore")
        self.served("aliceBefore")
        self.served("aliceCachedStillActive")  # within the 60-second cache
        self.unauthorized("aliceRevoked")
        self.served("bobAfter")

    def test_paid_providers_flag(self):
        off, on = self.r["paidOff"], self.r["paidOn"]
        self.assertEqual((off["status"], on["status"]), (200, 200))
        self.assertNotIn("finnhub.io", off["fetched"])
        self.assertIn("SOURCE_UNCONFIGURED", off["text"])
        self.assertIn("finnhub.io", on["fetched"])

    def test_without_both_bindings_only_the_owner_key_works(self):
        for prefix in ("noKv", "noDo", "neither"):
            self.unauthorized(f"{prefix}Key")
            self.unauthorized(f"{prefix}KeyPath")
            self.served(f"{prefix}Owner")
            self.assertEqual(self.r[f"{prefix}Key"]["kvGets"], 0, prefix)
            self.assertEqual(self.r[f"{prefix}Key"]["usageCalls"], 0, prefix)

    def test_isolate_cache_serves_repeat_lookups(self):
        self.assertEqual([self.r[n]["kvGets"] for n in ("cacheFirst", "cacheSecond", "cacheWithinTtl", "cacheExpired")],
                         [1, 0, 0, 1])
        for name in ("cacheFirst", "cacheSecond", "cacheWithinTtl", "cacheExpired"):
            self.served(name)
            self.assertEqual(self.r[name]["usageCalls"], 1, name)  # every request is counted
        self.assertEqual([self.r[n]["kvGets"] for n in ("negativeFirst", "negativeSecond")], [1, 0])
        self.assertEqual(self.r["unit"]["ttl"], 60_000)

    def test_telemetry_key_id(self):
        key_id = RECORDS["telemetry"]["id"]
        expected = {
            "telOwner": ("owner", "ok", "", 200, "ping"),
            "telKey": (key_id, "ok", "", 200, "ping"),
            "telKeyLimited": (key_id, "rejected", "KEY_RATE_LIMITED", 429, "rejected:key_limit"),
            "telKeyMethod": (key_id, "rejected", "", 405, "rejected:method"),
            "telWrong": ("-", "rejected", "AUTH_REQUIRED", 401, "rejected:auth"),
            "telAnonymous": ("-", "rejected", "AUTH_REQUIRED", 401, "rejected:auth"),
            "telGraceAnonymous": ("-", "ok", "", 200, "ping"),
        }
        for name, (kid, outcome, code, status, action) in expected.items():
            res = self.r[name]
            blobs = _blobs(res)
            self.assertEqual(len(blobs), 12, name)
            self.assertEqual(blobs[11], kid, name)
            self.assertEqual((blobs[3], blobs[4]), (outcome, code), name)
            self.assertEqual(res["points"][0]["indexes"], [action], name)
            self.assertEqual(res["status"], status, name)
        self.rate_limited("telKeyLimited", 55)

    def test_fail_closed_through_the_exception_path(self):
        for name in ("malformed", "kvThrows", "doThrows"):
            res = self.r[name]
            self.assertTrue(res.get("threw"), f"{name}: {res}")
            self.assertTrue(res["message"].startswith("ACCESS_KEY_BACKEND_ERROR"), name)
            blobs = _blobs(res)
            self.assertEqual(blobs[3], "exception", name)
            self.assertEqual(res["points"][0]["doubles"][2], 500, name)
        for name in ("kvThrowsOwner", "doThrowsOwner"):
            self.served(name)

    def test_injected_store_and_counter(self):
        self.served("injectedKey")
        self.served("injectedOwner")
        self.assertEqual(self.r["injectedSpy"], {"store": 1, "counter": 1})
        self.unauthorized("injectedForcedAbsent")

    def test_unit_shape_window_and_export(self):
        unit = self.r["unit"]
        self.assertEqual(unit["shape"], [True, False, False, False, False])
        self.assertEqual(unit["window"], [
            {"allowed": True, "retryAfterSeconds": 0},
            {"allowed": False, "retryAfterSeconds": 1},   # 0.5 s left in the minute rounds up
            {"allowed": True, "retryAfterSeconds": 0},    # the next minute
        ])
        self.assertEqual(unit["zeroDaily"], {"allowed": False, "retryAfterSeconds": 43_200})
        self.assertTrue(unit["sameClass"], "KeyUsage is exported from the Worker entry module")
        self.assertEqual(unit["hash"], hashlib.sha256(KEYS["alice"].encode()).hexdigest())

    def test_no_key_hash_or_label_anywhere(self):
        secrets = [OWNER, WRONG, *KEYS.values(), *(_hash(k) for k in KEYS.values()), LABEL_PREFIX]
        recorded = {
            "responses": json.dumps(self.r["everything"]),
            "logs": json.dumps(self.r["logs"]),
            "telemetry": json.dumps([p for v in self.r.values() if isinstance(v, dict) for p in v.get("points", [])]),
        }
        for where, text in recorded.items():
            for secret in secrets:
                self.assertNotIn(secret, text, f"{where} leaks a credential, hash or label")


if __name__ == "__main__":
    unittest.main()
