#!/usr/bin/env python3
"""Fetch allow-list, redirect checks, CORS and the evidence-store guard (2.10.0).

Worker cases drive the bundled Worker (worker/src) in Node with a fake fetch, so
nothing reaches the network:

- checkFetchUrl: every rule of the URL policy, table-driven (CHECK_CASES);
- providerFetch: manual redirects, each hop re-checked, at most 5 hops, 303 to
  GET, each hop one upstream request;
- the SSRF exit criteria: http redirect, non-allow-listed host, loopback,
  link-local, IP literal, and raw_text never fetched;
- parse_public_transcript / index_earnings_release refusal envelopes;
- company_ir: approved registry only, NOT_REGISTERED with no approved entry;
- CORS on /mcp, /health and /audit/mcp, through the Worker entry point;
- evidence tools: invalid tickers rejected before list(), list() prefix rules.

The Python half (yfmcp/url_policy.py) is ported later by python-mirror; its
cases are the TODO placeholders at the end, which must use the same CHECK_CASES
table and reason strings.
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

HOST = "URL host not allowed"

# (url, expected reason or None when allowed). Shared by both runtimes: the reason strings are identical.
CHECK_CASES: list[tuple[str, str | None]] = [
    # Rule 1: absolute URL
    ("not a url", "invalid URL"),
    ("/Archives/edgar/data/1/x.htm", "invalid URL"),
    ("", "invalid URL"),
    # Rule 2: https only
    ("http://www.sec.gov/Archives/edgar/data/1/x.htm", "URL must use https"),
    ("ftp://www.sec.gov/", "URL must use https"),
    ("http://user:pw@127.0.0.1:8080/", "URL must use https"),   # the first failing rule wins
    # Rule 3: no credentials
    ("https://user:pw@www.sec.gov/Archives/", "URL must not contain credentials"),
    ("https://user@www.sec.gov/Archives/", "URL must not contain credentials"),
    # Rule 4: port empty or 443
    ("https://www.sec.gov:8443/Archives/", "URL port not allowed"),
    ("https://www.sec.gov:80/Archives/", "URL port not allowed"),
    ("https://www.sec.gov:443/Archives/", None),
    # Rule 5: IP literals and local names
    ("https://127.0.0.1/", HOST),
    ("https://169.254.169.254/latest/meta-data/", HOST),
    ("https://10.0.0.1/", HOST),
    ("https://[::1]/", HOST),
    ("https://[fd00::1]/", HOST),
    ("https://[::ffff:169.254.169.254]/", HOST),
    ("https://2130706433/", HOST),          # decimal 127.0.0.1
    ("https://0x7f000001/", HOST),          # hex 127.0.0.1
    ("https://0x7f.1/", HOST),              # mixed hex form
    ("https://017700000001/", HOST),        # octal form
    ("https://localhost/", HOST),
    ("https://LOCALHOST./", HOST),
    ("https://api.localhost/", HOST),
    ("https://printer.local/", HOST),
    ("https://metadata.google.internal/computeMetadata/v1/", HOST),
    ("https://www.sec.gov.invalid/", HOST),
    # Rule 6: exact allow-list
    ("https://example.com/", HOST),
    ("https://sec.gov/Archives/", HOST),
    ("https://evil.www.sec.gov/", HOST),
    ("https://www.sec.gov.evil.com/", HOST),
    ("https://yahoo.com/", HOST),
    ("https://www.iqep.com/investors/", HOST),   # a candidate registry entry is not approved
    ("https://www.sec.gov/Archives/edgar/data/1/x.htm", None),
    ("https://www.sec.gov./Archives/edgar/data/1/x.htm", None),    # trailing dot
    ("https://WWW.SEC.GOV/Archives/edgar/data/1/x.htm", None),     # uppercase host
    ("https://data.sec.gov/submissions/CIK0000320193.json", None),
    ("https://efts.sec.gov/LATEST/search-index?q=x", None),
    ("https://finance.yahoo.com/quote/AAPL/", None),
    ("https://query1.finance.yahoo.com/v8/finance/chart/AAPL", None),
    ("https://query2.finance.yahoo.com/v8/finance/chart/AAPL", None),
    ("https://fc.yahoo.com", None),
    ("https://www.globenewswire.com/RssFeed/x", None),
    ("https://finnhub.io/api/v1/company-news", None),
    ("https://www.alphavantage.co/query", None),
    ("https://api.marketaux.com/v1/news/all", None),
    ("https://api.sec.or.th/FundFactsheet/fund", None),
    ("https://api.company-information.service.gov.uk/company/1", None),
]

TRANSCRIPT_REFUSAL = (
    "url is not on the fetch allow-list ({reason}). Allowed: SEC Archives, finance.yahoo.com transcript pages, "
    "and approved company IR pages. Paste the text as raw_text instead."
)

_HARNESS = r"""
const mod = await import(process.argv[2]);
const CHECK_CASES = JSON.parse(process.env.CHECK_CASES);
const { urlPolicy, requestContext, yahooFinance, response, evidenceStore, evidencePack } = mod;
const worker = mod.default;
const ctx = { waitUntil() {}, passThroughOnException() {} };
delete globalThis.caches;   // no edge cache: every read goes to the fake fetch
response.setWorkerEnv({});

// ── fake fetch ──────────────────────────────────────────────────────────────
let routes = {};
let calls = [];
globalThis.fetch = async (input, init = {}) => {
  const url = typeof input === "string" ? input : input.url;
  const headers = new Headers(init.headers || {});
  calls.push({ url, method: init.method || "GET", redirect: init.redirect ?? null, body: init.body ?? null,
               authorization: headers.get("Authorization"), custom: headers.get("X-Custom") });
  const route = routes[url];
  if (!route) return new Response("unavailable", { status: 503 });
  const status = route.status ?? 200;
  return new Response([204, 304].includes(status) ? null : route.body ?? "", { status, headers: route.headers ?? {} });
};
const hostsOf = (list) => [...new Set(list.map((c) => new URL(c.url).hostname))];

const r = {};

// ── checkFetchUrl table ─────────────────────────────────────────────────────
r.check = CHECK_CASES.map(([url]) => {
  const out = urlPolicy.checkFetchUrl(url);
  return out.ok ? null : out.reason;
});
r.checkOkUrl = urlPolicy.checkFetchUrl("https://WWW.SEC.GOV./Archives/x").url?.hostname ?? null;
r.registryHosts = [...urlPolicy.REGISTRY_HOSTS];

// ── providerFetch redirects ─────────────────────────────────────────────────
async function follow(name, start, table, init) {
  routes = table; calls = [];
  let status = null, error = null, errorName = null, finalText = null;
  const { scope } = await requestContext.withCacheScope(async () => {
    try {
      const res = await requestContext.providerFetch(start, init);
      status = res.status; finalText = await res.text();
    } catch (e) { error = e.message; errorName = e.name; }
  });
  r[name] = { status, error, errorName, finalText, upstream: scope.upstreamRequests, calls };
}
const redirect = (status, location) => ({ status, headers: location === null ? {} : { Location: location } });

await follow("allowedToAllowed", "https://www.sec.gov/a", {
  "https://www.sec.gov/a": redirect(302, "https://data.sec.gov/b"),
  "https://data.sec.gov/b": { body: "done" },
});
await follow("relativeLocation", "https://www.sec.gov/a", {
  "https://www.sec.gov/a": redirect(301, "/Archives/c"),
  "https://www.sec.gov/Archives/c": { body: "rel" },
});
await follow("allowedToDisallowed", "https://www.sec.gov/a", { "https://www.sec.gov/a": redirect(302, "https://evil.example/x") });
await follow("allowedToIpLiteral", "https://www.sec.gov/a", { "https://www.sec.gov/a": redirect(307, "https://169.254.169.254/latest/meta-data/") });
await follow("allowedToLoopback", "https://www.sec.gov/a", { "https://www.sec.gov/a": redirect(308, "https://127.0.0.1/admin") });
await follow("allowedToIpv6Loopback", "https://www.sec.gov/a", { "https://www.sec.gov/a": redirect(302, "https://[::1]/") });
await follow("allowedToLocalhost", "https://www.sec.gov/a", { "https://www.sec.gov/a": redirect(302, "https://localhost/") });
await follow("httpRedirect", "https://www.sec.gov/a", { "https://www.sec.gov/a": redirect(301, "http://www.sec.gov/a") });
await follow("credentialRedirect", "https://www.sec.gov/a", { "https://www.sec.gov/a": redirect(302, "https://u:p@www.sec.gov/a") });
await follow("portRedirect", "https://www.sec.gov/a", { "https://www.sec.gov/a": redirect(302, "https://www.sec.gov:8080/a") });
const chain = (n) => {
  const t = {};
  for (let i = 0; i < n; i++) t[`https://www.sec.gov/h${i}`] = redirect(302, `https://www.sec.gov/h${i + 1}`);
  t[`https://www.sec.gov/h${n}`] = { body: `end${n}` };
  return t;
};
await follow("fiveHops", "https://www.sec.gov/h0", chain(5));
await follow("sixHops", "https://www.sec.gov/h0", chain(6));
await follow("seeOtherBecomesGet", "https://efts.sec.gov/post", {
  "https://efts.sec.gov/post": redirect(303, "https://efts.sec.gov/result"),
  "https://efts.sec.gov/result": { body: "got" },
}, { method: "POST", body: "q=1", headers: { "Content-Type": "application/x-www-form-urlencoded" } });
await follow("temporaryRedirectKeepsPost", "https://efts.sec.gov/post", {
  "https://efts.sec.gov/post": redirect(307, "https://efts.sec.gov/again"),
  "https://efts.sec.gov/again": { body: "kept" },
}, { method: "POST", body: "q=1" });
await follow("crossOriginDropsAuthorization", "https://www.sec.gov/a", {
  "https://www.sec.gov/a": redirect(302, "https://www.sec.gov/b"),
  "https://www.sec.gov/b": redirect(302, "https://data.sec.gov/c"),
  "https://data.sec.gov/c": { body: "x" },
}, { headers: { Authorization: "Bearer provider-key", "X-Custom": "kept" } });
await follow("disallowedStart", "https://evil.example/", {});
await follow("ipLiteralStart", "https://169.254.169.254/latest/meta-data/", {});
await follow("redirectWithoutLocation", "https://www.sec.gov/a", { "https://www.sec.gov/a": redirect(302, null) });
await follow("notModifiedIsNotARedirect", "https://www.sec.gov/a", { "https://www.sec.gov/a": { status: 304, headers: { Location: "https://evil.example/" } } });
await follow("callerSpellingKept", "https://fc.yahoo.com", { "https://fc.yahoo.com": { status: 404, body: "nf" } });

// ── parse_public_transcript ─────────────────────────────────────────────────
const ptr = async (url, rawText = null) => {
  routes = {}; calls = [];
  const out = JSON.parse(await yahooFinance.parsePublicTranscript(url, null, rawText));
  return { out, fetches: calls.length, hosts: hostsOf(calls) };
};
r.ptrExample = await ptr("https://example.com/");
r.ptrMetadata = await ptr("https://169.254.169.254/latest/meta-data/");
r.ptrLoopback = await ptr("https://127.0.0.1/");
r.ptrDecimalIp = await ptr("https://2130706433/");
r.ptrPort = await ptr("https://www.sec.gov:8443/Archives/edgar/data/1/x.htm");
r.ptrCredentials = await ptr("https://u:p@www.sec.gov/Archives/edgar/data/1/x.htm");
r.ptrSecOutsideArchives = await ptr("https://www.sec.gov/cgi-bin/browse-edgar");
r.ptrDataSec = await ptr("https://data.sec.gov/submissions/CIK0000320193.json");
r.ptrYahooNotTranscript = await ptr("https://finance.yahoo.com/quote/AAPL/");
r.ptrYahooApiHost = await ptr("https://query1.finance.yahoo.com/v8/finance/chart/AAPL");
r.ptrProviderHost = await ptr("https://finnhub.io/api/v1/company-news");
r.ptrHttp = await ptr("http://example.com/");
r.ptrSecArchives = await ptr("https://www.sec.gov/Archives/edgar/data/1/x.htm");
r.ptrRawTextWithUrl = await ptr("https://example.com/", "Revenue rose. See https://169.254.169.254/latest/meta-data/ and http://localhost/.");
r.ptrRawTextOnly = await ptr("", "Plain transcript text about revenue.");

// ── index_earnings_release source_url ───────────────────────────────────────
const ier = async (url) => {
  routes = {}; calls = [];
  const out = JSON.parse(await yahooFinance.indexEarningsRelease("ZQI", "latest", url));
  return { out, fetches: calls.length };
};
r.ierExample = await ier("https://example.com/ir/release");
r.ierWsjSubdomain = await ier("https://markets.wsj.com/a");
r.ierPaywalled = await ier("https://www.wsj.com/articles/x");
r.ierMetadata = await ier("https://169.254.169.254/latest/meta-data/");
r.ierSecOutsideArchives = await ier("https://www.sec.gov/cgi-bin/browse-edgar");
r.ierYahoo = await ier("https://finance.yahoo.com/quote/AAPL/");
r.ierHttp = await ier("http://example.com/release");
r.ierInvalid = await ier("not a url");
r.ierSecArchives = await ier("https://www.sec.gov/Archives/edgar/data/1/2/r.htm");

// ── company_ir: approved registry only ──────────────────────────────────────
const news = async (sources, fn = "getCompanyNews") => {
  routes = {}; calls = [];
  const out = fn === "getCompanyNews"
    ? JSON.parse(await yahooFinance.getCompanyNews("MSFT", 10, 14, sources))
    : JSON.parse(await yahooFinance.getPublicEventTimeline("MSFT", "", "", sources, 20, false));
  return { companyIr: out.sourceStatus?.company_ir ?? null, coverage: out.coverage ?? null, fetches: calls.length,
           hosts: hostsOf(calls), sourcesUsed: out.meta?.sourcesUsed ?? null };
};
r.companyIrOnly = await news(["company_ir"]);
r.newsDefaults = await news(null);
r.timelineDefaults = await news(null, "timeline");
r.allowedHostCheck = Object.fromEntries([...r.newsDefaults.hosts, ...r.timelineDefaults.hosts]
  .map((h) => [h, urlPolicy.checkFetchUrl(`https://${h}/`).ok]));

// ── CORS through the Worker entry point ─────────────────────────────────────
async function send(path, { method = "POST", origin, headers = {}, body } = {}) {
  routes = {}; calls = [];
  const h = { ...headers };
  if (origin !== undefined) h.Origin = origin;
  if (body !== undefined) h["Content-Type"] = "application/json";
  const init = { method, headers: h };
  if (body !== undefined) init.body = body;
  const res = await worker.fetch(new Request(`https://example.invalid${path}`, init), { TOOL_MODE: "grouped" }, ctx);
  await res.text();
  return { status: res.status, headers: Object.fromEntries(res.headers.entries()) };
}
const ping = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "ping" });
r.corsNoOrigin = await send("/mcp", { body: ping });
r.corsClaude = await send("/mcp", { body: ping, origin: "https://claude.ai" });
r.corsEvil = await send("/mcp", { body: ping, origin: "https://evil.example" });
r.corsClaudeLookalike = await send("/mcp", { body: ping, origin: "https://claude.ai.evil.example" });
r.corsNullOrigin = await send("/mcp", { body: ping, origin: "null" });
r.optionsNoOrigin = await send("/mcp", { method: "OPTIONS" });
r.optionsClaude = await send("/mcp", { method: "OPTIONS", origin: "https://claude.ai" });
r.optionsEvil = await send("/mcp", { method: "OPTIONS", origin: "https://evil.example" });
r.optionsPathClaude = await send("/mcp/some-token", { method: "OPTIONS", origin: "https://claude.ai" });
r.optionsPathEvil = await send("/mcp/some-token", { method: "OPTIONS", origin: "https://evil.example" });
r.mcpGetClaude = await send("/mcp", { method: "GET", origin: "https://claude.ai" });
r.mcpGetEvil = await send("/mcp", { method: "GET", origin: "https://evil.example" });
r.health = await send("/health", { method: "GET" });
r.healthEvil = await send("/health", { method: "GET", origin: "https://evil.example" });
r.root = await send("/", { method: "GET" });
r.optionsHealth = await send("/health", { method: "OPTIONS", origin: "https://evil.example" });
r.audit = await send("/audit/mcp", { method: "GET", origin: "https://claude.ai" });
r.optionsAudit = await send("/audit/mcp", { method: "OPTIONS", origin: "https://claude.ai" });
r.unknown = await send("/nope", { method: "GET", origin: "https://claude.ai" });

// ── evidence guard ──────────────────────────────────────────────────────────
const listed = [];
const spyStore = {
  ...evidenceStore.memoryEvidenceStore(),
  kind: "spy",
  async list(prefix, limit) { listed.push(prefix); return []; },
  async get() { return null; },
};
evidenceStore.setEvidenceStoreForTests(spyStore);
const parsed = (text) => { const v = JSON.parse(text); return { code: v.error?.code ?? v.code ?? null, message: v.error?.message ?? v.message ?? null, ok: v.ok ?? null }; };
routes = {}; calls = [];
r.evInvalidEps = parsed(await evidencePack.getEpsRevisions("BAD/TICKER"));
r.evInvalidCuts = parsed(await evidencePack.listEvidenceCuts("../ETC"));
r.evInvalidObservation = parsed(await evidencePack.getConsensusForecastCurve("A/B", 5, 3, 10, "2026-10-01"));
r.evInvalidFetches = calls.length;
r.evInvalidListed = [...listed];
r.evValidCuts = JSON.parse(await evidencePack.listEvidenceCuts("aapl"));
r.evValidListed = [...listed];
const mem = evidenceStore.memoryEvidenceStore();
mem.objects.set("evidence-cuts/AAPL/x.json", "{}");
mem.objects.set("evidence-cuts/AAPLX/y.json", "{}");
const tryList = async (store, prefix) => { try { return { keys: await store.list(prefix, 10) }; } catch (e) { return { error: e.message }; } };
r.memPrefixes = {};
for (const p of ["evidence-cuts/AAPL/", "", "evidence-cuts/AAPL", "evidence-cuts/../", "evidence-cuts//", "evidence-cuts\\AAPL/", "evidence-cuts/A..B/"]) {
  r.memPrefixes[p] = await tryList(mem, p);
}
const r2Calls = [];
const fakeBucket = { put() {}, async list(opts) { r2Calls.push(opts.prefix); return { objects: [], truncated: false }; } };
evidenceStore.setEvidenceStoreForTests(undefined);
evidenceStore.setEvidenceBucket(fakeBucket);
const r2 = evidenceStore.getEvidenceStore();
r.r2Prefixes = {};
for (const p of ["consensus-history/AAPL/", "", "consensus-history", "consensus-history/../", "a//b/", "a\\b/"]) {
  r.r2Prefixes[p] = await tryList(r2, p);
}
r.r2Calls = r2Calls;
evidenceStore.setEvidenceStoreForTests(evidenceStore.memoryEvidenceStore());
r.evDotDotTicker = JSON.parse(await evidencePack.listEvidenceCuts("A..B"));

console.log(JSON.stringify(r));
"""


def _run_harness() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".url-policy-test-") as tmp:
        entry = Path(tmp) / "entry.ts"
        bundle = Path(tmp) / "worker.mjs"
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        entry.write_text(
            'export { default } from "../src/index.ts";\n'
            'export * as urlPolicy from "../src/url-policy.ts";\n'
            'export * as requestContext from "../src/request-context.ts";\n'
            'export * as yahooFinance from "../src/yahoo-finance.ts";\n'
            'export * as response from "../src/response.ts";\n'
            'export * as evidenceStore from "../src/evidence-store.ts";\n'
            'export * as evidencePack from "../src/evidence-pack.ts";\n',
            encoding="utf-8",
        )
        subprocess.run(
            [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=node",
             f"--outfile={bundle}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
        env = {**os.environ, "CHECK_CASES": json.dumps(CHECK_CASES)}
        result = subprocess.run(
            [node, str(harness), bundle.as_uri()],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=300, env=env,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


class WorkerCase(unittest.TestCase):
    r: dict

    @classmethod
    def setUpClass(cls) -> None:
        cls.r = _run_harness()


class TestWorkerCheckFetchUrl(WorkerCase):
    def test_every_rule_in_the_table(self) -> None:
        for (url, expected), actual in zip(CHECK_CASES, self.r["check"]):
            with self.subTest(url=url):
                self.assertEqual(actual, expected)

    def test_allowed_url_is_returned_parsed(self) -> None:
        self.assertEqual(self.r["checkOkUrl"], "www.sec.gov.")

    def test_registry_contributes_no_host_while_nothing_is_approved(self) -> None:
        self.assertEqual(self.r["registryHosts"], [])


class TestWorkerRedirects(WorkerCase):
    def refused(self, name: str, reason: str, fetches: int = 1) -> None:
        res = self.r[name]
        self.assertEqual(res["errorName"], "UrlPolicyError", name)
        self.assertEqual(res["error"], reason, name)
        self.assertEqual(len(res["calls"]), fetches, name)
        self.assertEqual(res["upstream"], fetches, name)

    def test_allowed_to_allowed_hop_is_followed(self) -> None:
        res = self.r["allowedToAllowed"]
        self.assertEqual((res["status"], res["finalText"]), (200, "done"))
        self.assertEqual([c["url"] for c in res["calls"]], ["https://www.sec.gov/a", "https://data.sec.gov/b"])
        self.assertTrue(all(c["redirect"] == "manual" for c in res["calls"]))

    def test_relative_location_resolves_against_the_current_url(self) -> None:
        res = self.r["relativeLocation"]
        self.assertEqual(res["finalText"], "rel")
        self.assertEqual(res["calls"][1]["url"], "https://www.sec.gov/Archives/c")

    def test_each_hop_counts_as_one_upstream_request(self) -> None:
        self.assertEqual(self.r["allowedToAllowed"]["upstream"], 2)
        self.assertEqual(self.r["fiveHops"]["upstream"], 6)

    def test_allowed_to_disallowed_hop_is_refused(self) -> None:
        self.refused("allowedToDisallowed", "URL host not allowed")

    def test_allowed_to_ip_literal_and_local_hops_are_refused(self) -> None:
        for name in ("allowedToIpLiteral", "allowedToLoopback", "allowedToIpv6Loopback", "allowedToLocalhost"):
            self.refused(name, "URL host not allowed")

    def test_http_credential_and_port_hops_are_refused(self) -> None:
        self.refused("httpRedirect", "URL must use https")
        self.refused("credentialRedirect", "URL must not contain credentials")
        self.refused("portRedirect", "URL port not allowed")

    def test_five_hops_are_followed_and_a_sixth_is_refused(self) -> None:
        self.assertEqual(self.r["fiveHops"]["finalText"], "end5")
        self.refused("sixHops", "too many redirects", fetches=6)

    def test_303_becomes_get_without_a_body(self) -> None:
        calls = self.r["seeOtherBecomesGet"]["calls"]
        self.assertEqual([(c["method"], c["body"]) for c in calls], [("POST", "q=1"), ("GET", None)])
        self.assertEqual(self.r["seeOtherBecomesGet"]["finalText"], "got")

    def test_307_keeps_the_method_and_body(self) -> None:
        calls = self.r["temporaryRedirectKeepsPost"]["calls"]
        self.assertEqual([(c["method"], c["body"]) for c in calls], [("POST", "q=1"), ("POST", "q=1")])

    def test_authorization_stays_with_its_origin(self) -> None:
        calls = self.r["crossOriginDropsAuthorization"]["calls"]
        self.assertEqual([c["authorization"] for c in calls], ["Bearer provider-key", "Bearer provider-key", None])
        self.assertEqual([c["custom"] for c in calls], ["kept", "kept", "kept"])

    def test_disallowed_first_url_is_refused_before_any_fetch(self) -> None:
        self.refused("disallowedStart", "URL host not allowed", fetches=0)
        self.refused("ipLiteralStart", "URL host not allowed", fetches=0)

    def test_non_redirect_responses_are_returned_as_they_are(self) -> None:
        self.assertEqual(self.r["redirectWithoutLocation"]["status"], 302)
        self.assertEqual(self.r["notModifiedIsNotARedirect"]["status"], 304)
        self.assertEqual(len(self.r["notModifiedIsNotARedirect"]["calls"]), 1)

    def test_first_request_keeps_the_callers_spelling(self) -> None:
        self.assertEqual(self.r["callerSpellingKept"]["calls"][0]["url"], "https://fc.yahoo.com")


class TestWorkerCallerUrls(WorkerCase):
    def assert_refused(self, name: str, reason: str) -> None:
        res = self.r[name]
        self.assertEqual(res["out"], {"ok": False, "error": {"code": "INPUT_VALIDATION_ERROR",
                                                            "message": TRANSCRIPT_REFUSAL.format(reason=reason)}}, name)
        self.assertEqual(res["fetches"], 0, name)

    def test_transcript_url_off_the_allow_list_is_refused_without_fetching(self) -> None:
        for name in ("ptrExample", "ptrMetadata", "ptrLoopback", "ptrDecimalIp"):
            self.assert_refused(name, "URL host not allowed")
        self.assert_refused("ptrPort", "URL port not allowed")
        self.assert_refused("ptrCredentials", "URL must not contain credentials")

    def test_transcript_path_rules(self) -> None:
        for name in ("ptrSecOutsideArchives", "ptrDataSec", "ptrYahooNotTranscript", "ptrYahooApiHost", "ptrProviderHost"):
            self.assert_refused(name, "URL path not allowed")

    def test_transcript_http_keeps_the_existing_message(self) -> None:
        self.assertEqual(self.r["ptrHttp"]["out"]["error"],
                         {"code": "INPUT_VALIDATION_ERROR", "message": "A valid https:// URL or raw_text is required."})

    def test_sec_archives_transcript_url_is_fetched(self) -> None:
        res = self.r["ptrSecArchives"]
        self.assertEqual(res["hosts"], ["www.sec.gov"])
        self.assertEqual(res["out"]["error"]["code"], "FETCH_ERROR")   # the fake fetch answers 503

    def test_raw_text_is_never_fetched_even_with_urls_in_it(self) -> None:
        for name in ("ptrRawTextWithUrl", "ptrRawTextOnly"):
            res = self.r[name]
            self.assertEqual(res["fetches"], 0, name)
            self.assertEqual(res["out"]["source"], "raw_text", name)
            self.assertIsNone(res["out"]["url"], name)

    def test_earnings_release_source_url_refusals(self) -> None:
        def err(name: str) -> dict:
            return self.r[name]["out"]["error"]
        for name in ("ierExample", "ierWsjSubdomain", "ierMetadata"):
            self.assertEqual(err(name), {"code": "INPUT_VALIDATION_ERROR",
                                         "message": "source_url is not on the fetch allow-list (URL host not allowed)"}, name)
            self.assertEqual(self.r[name]["fetches"], 0, name)
        for name in ("ierSecOutsideArchives", "ierYahoo"):
            self.assertEqual(err(name), {"code": "INPUT_VALIDATION_ERROR",
                                         "message": "source_url is not on the fetch allow-list (URL path not allowed)"}, name)

    def test_earnings_release_existing_checks_come_first(self) -> None:
        self.assertEqual(self.r["ierPaywalled"]["out"]["error"]["message"], "source_url appears paywalled and is not allowed")
        self.assertEqual(self.r["ierHttp"]["out"]["error"]["message"], "source_url must use https")
        self.assertEqual(self.r["ierInvalid"]["out"]["error"]["message"], "source_url must be a valid URL")

    def test_earnings_release_sec_archives_is_still_sec_8k(self) -> None:
        res = self.r["ierSecArchives"]
        self.assertEqual(res["out"]["error"]["code"], "PROVIDER_ERROR")   # fetched as sec_8k; the fake fetch answers 503
        self.assertGreater(res["fetches"], 0)


class TestWorkerCompanyIr(WorkerCase):
    def test_company_ir_without_an_approved_entry_is_not_registered(self) -> None:
        res = self.r["companyIrOnly"]
        self.assertEqual(res["companyIr"]["status"], "NOT_REGISTERED")
        self.assertEqual(res["companyIr"]["allowedAction"], "registry_review_required")
        self.assertEqual(res["fetches"], 0)
        self.assertEqual(res["sourcesUsed"], [])
        self.assertIn({"source": "company_ir", "status": "NOT_REGISTERED", "attempted": True, "reasonCode": None},
                      res["coverage"]["failedSources"])

    def test_default_news_and_timeline_fetch_only_allow_listed_hosts(self) -> None:
        self.assertTrue(self.r["allowedHostCheck"])
        self.assertTrue(all(self.r["allowedHostCheck"].values()), self.r["allowedHostCheck"])
        self.assertEqual(self.r["timelineDefaults"]["companyIr"]["status"], "NOT_REGISTERED")


class TestWorkerCors(WorkerCase):
    def no_cors(self, name: str) -> None:
        headers = self.r[name]["headers"]
        self.assertFalse([h for h in headers if h.startswith("access-control-")], name)
        self.assertNotIn("vary", headers, name)

    def claude_cors(self, name: str) -> None:
        headers = self.r[name]["headers"]
        self.assertEqual(headers["access-control-allow-origin"], "https://claude.ai", name)
        self.assertEqual(headers["vary"], "Origin", name)
        self.assertEqual(headers["access-control-allow-headers"], "Content-Type, Mcp-Session-Id, Authorization", name)
        self.assertEqual(headers["access-control-allow-methods"], "GET, POST, OPTIONS", name)

    def test_mcp_without_origin_has_no_cors_headers(self) -> None:
        self.assertEqual(self.r["corsNoOrigin"]["status"], 200)
        self.no_cors("corsNoOrigin")

    def test_mcp_reflects_only_claude_ai(self) -> None:
        self.claude_cors("corsClaude")
        for name in ("corsEvil", "corsClaudeLookalike", "corsNullOrigin"):
            self.assertEqual(self.r[name]["status"], 200, name)
            self.no_cors(name)

    def test_options_on_mcp(self) -> None:
        for name in ("optionsNoOrigin", "optionsClaude", "optionsEvil", "optionsPathClaude", "optionsPathEvil"):
            self.assertEqual(self.r[name]["status"], 204, name)
        self.claude_cors("optionsClaude")
        self.claude_cors("optionsPathClaude")
        for name in ("optionsNoOrigin", "optionsEvil", "optionsPathEvil"):
            self.no_cors(name)

    def test_mcp_errors_follow_the_same_rule(self) -> None:
        self.assertEqual(self.r["mcpGetClaude"]["status"], 405)
        self.claude_cors("mcpGetClaude")
        self.no_cors("mcpGetEvil")

    def test_health_and_root_stay_public(self) -> None:
        for name in ("health", "healthEvil", "root", "optionsHealth"):
            self.assertEqual(self.r[name]["headers"]["access-control-allow-origin"], "*", name)
            self.assertEqual(self.r[name]["headers"]["access-control-allow-headers"], "Content-Type, Mcp-Session-Id", name)

    def test_audit_and_unknown_paths_have_no_cors(self) -> None:
        self.assertEqual(self.r["audit"]["status"], 404)   # no AUDIT_TOKEN configured
        for name in ("audit", "optionsAudit", "unknown"):
            self.no_cors(name)
        self.assertEqual(self.r["unknown"]["status"], 404)


class TestWorkerEvidenceGuard(WorkerCase):
    def test_invalid_ticker_is_rejected_before_list(self) -> None:
        for name in ("evInvalidEps", "evInvalidCuts", "evInvalidObservation"):
            self.assertEqual(self.r[name]["code"], "INPUT_VALIDATION_ERROR", name)
            self.assertIn("Invalid ticker symbol", self.r[name]["message"], name)
        self.assertEqual(self.r["evInvalidListed"], [])
        self.assertEqual(self.r["evInvalidFetches"], 0)

    def test_valid_ticker_lists_its_own_prefix(self) -> None:
        self.assertEqual(self.r["evValidCuts"]["storageStatus"], "AVAILABLE")
        self.assertEqual(self.r["evValidListed"], ["evidence-cuts/AAPL/"])

    def test_memory_store_prefix_rules(self) -> None:
        p = self.r["memPrefixes"]
        self.assertEqual(p["evidence-cuts/AAPL/"], {"keys": ["evidence-cuts/AAPL/x.json"]})
        for bad in ("", "evidence-cuts/AAPL", "evidence-cuts/../", "evidence-cuts//", "evidence-cuts\\AAPL/", "evidence-cuts/A..B/"):
            self.assertEqual(p[bad], {"error": "invalid evidence list prefix"}, bad)

    def test_r2_store_prefix_rules(self) -> None:
        p = self.r["r2Prefixes"]
        self.assertEqual(p["consensus-history/AAPL/"], {"keys": []})
        for bad in ("", "consensus-history", "consensus-history/../", "a//b/", "a\\b/"):
            self.assertEqual(p[bad], {"error": "invalid evidence list prefix"}, bad)
        self.assertEqual(self.r["r2Calls"], ["consensus-history/AAPL/"])

    def test_dot_dot_ticker_passes_validation_but_not_the_list_guard(self) -> None:
        self.assertEqual(self.r["evDotDotTicker"]["storageStatus"], "FAILED")
        self.assertEqual(self.r["evDotDotTicker"]["message"], "invalid evidence list prefix")


# ── Python (yfmcp/url_policy.py) ────────────────────────────────────────────
# TODO(python-mirror, 2.10.0): port these once yfmcp/url_policy.py exists. They must use CHECK_CASES and
# the same reason strings as the Worker cases above.
_PY_TODO = "TODO(python-mirror, 2.10.0): Python url policy not ported yet"


class TestPythonUrlPolicy(unittest.TestCase):
    @unittest.skip(_PY_TODO)
    def test_check_fetch_url_table(self) -> None:
        """for url, expected in CHECK_CASES: check_fetch_url(url) == (expected is None, expected or '')."""

    @unittest.skip(_PY_TODO)
    def test_redirect_handler_refuses_a_disallowed_location(self) -> None:
        """The HTTPRedirectHandler subclass raises UrlPolicyError for http, off-list, loopback, link-local and IP-literal hops."""

    @unittest.skip(_PY_TODO)
    def test_parse_public_transcript_refusal_and_raw_text_not_fetched(self) -> None:
        """Same refusal envelope (code and message) as TestWorkerCallerUrls; raw_text with URLs makes zero urlopen calls."""

    @unittest.skip(_PY_TODO)
    def test_index_earnings_release_refusal(self) -> None:
        """source_url off the allow-list -> INPUT_VALIDATION_ERROR 'source_url is not on the fetch allow-list (<reason>)'."""

    @unittest.skip(_PY_TODO)
    def test_evidence_store_prefix_rules_and_path_containment(self) -> None:
        """LocalDirStore.list / MemoryStore.list reject the prefixes in TestWorkerEvidenceGuard; LocalDirStore stays under root."""

    @unittest.skip(_PY_TODO)
    def test_evidence_tools_validate_the_ticker_first(self) -> None:
        """get_eps_revisions, list_evidence_cuts and the consensus observation read return the invalid-ticker error first."""


if __name__ == "__main__":
    unittest.main()
