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

The Python half (yfmcp/url_policy.py) is tested at the end of this file against
the same CHECK_CASES table and reason strings, offline (a fake HTTPS handler and
a urlopen spy).
"""

from __future__ import annotations

import email.message
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
import urllib.response
from pathlib import Path
from unittest.mock import patch


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
# Offline: the redirect tests use a fake HTTPS handler, the tool tests spy on the urlopen seam.
sys.path.insert(0, str(ROOT))

from yfmcp import evidence_store as es  # noqa: E402
from yfmcp import url_policy as up  # noqa: E402


class _FakeHttps(urllib.request.HTTPSHandler):
    """Serves `routes` {url: (status, location, body)} without a socket and records each request."""

    def __init__(self, routes: dict) -> None:
        super().__init__()
        self.routes = routes
        self.seen: list[dict] = []

    def https_open(self, req):  # noqa: D102
        self.seen.append({"url": req.full_url, "method": req.get_method(), "data": req.data,
                          "authorization": req.get_header("Authorization"), "custom": req.get_header("X-custom")})
        status, location, body = self.routes[req.full_url]
        headers = email.message.Message()
        if location:
            headers["Location"] = location
        response = urllib.response.addinfourl(io.BytesIO(body), headers, req.full_url, status)
        response.msg = "stub"
        return response


def _redir(status: int, location: str) -> tuple:
    return (status, location, b"")


def _follow(start: str, routes: dict, **request_kwargs):
    handler = _FakeHttps(routes)
    opener = up.build_opener(handler)
    request = urllib.request.Request(start, **request_kwargs)
    try:
        with up.open_checked(opener, request, timeout=5) as resp:
            return handler, resp.read(), None
    except up.UrlPolicyError as error:
        return handler, None, error


class TestPythonUrlPolicy(unittest.TestCase):
    def test_check_fetch_url_table(self) -> None:
        for url, expected in CHECK_CASES:
            self.assertEqual(up.check_fetch_url(url), (expected is None, expected or ""), url)

    def test_registry_hosts_are_approved_entries_only(self) -> None:
        self.assertEqual(up.REGISTRY_HOSTS, ())   # the shipped registry holds candidates only
        registry = {"sources": [
            {"ticker": "A", "status": "approved", "allowedHosts": ["IR.Example.com"], "allowedPathPrefixes": ["/investors/"]},
            {"ticker": "B", "status": "candidate", "allowedHosts": ["cand.example.com"], "allowedPathPrefixes": ["/"]},
            {"ticker": "C", "status": "rejected", "allowedHosts": ["rej.example.com"], "allowedPathPrefixes": ["/"]},
        ]}
        with up.registry_override(registry):
            self.assertEqual(up.REGISTRY_HOSTS, ("ir.example.com",))
            self.assertEqual(up.check_fetch_url("https://ir.example.com/investors/x"), (True, ""))
            self.assertEqual(up.check_fetch_url("https://cand.example.com/"), (False, "URL host not allowed"))
            self.assertEqual(up.check_fetch_url("https://rej.example.com/"), (False, "URL host not allowed"))
            self.assertEqual(up.allowed_host_kind("https://ir.example.com/x"), "registry")
            self.assertTrue(up.approved_registry_url_allowed("https://ir.example.com/investors/news"))
            self.assertFalse(up.approved_registry_url_allowed("https://ir.example.com/other"))
        self.assertEqual(up.REGISTRY_HOSTS, ())

    def test_redirect_allowed_to_allowed_follows_one_request_per_hop(self) -> None:
        handler, body, error = _follow("https://www.sec.gov/a", {
            "https://www.sec.gov/a": _redir(302, "https://data.sec.gov/b"),
            "https://data.sec.gov/b": (200, None, b"done"),
        })
        self.assertIsNone(error)
        self.assertEqual(body, b"done")
        self.assertEqual([c["url"] for c in handler.seen], ["https://www.sec.gov/a", "https://data.sec.gov/b"])

    def test_redirect_relative_location_follows(self) -> None:
        handler, body, error = _follow("https://www.sec.gov/a", {
            "https://www.sec.gov/a": _redir(301, "/Archives/c"),
            "https://www.sec.gov/Archives/c": (200, None, b"rel"),
        })
        self.assertIsNone(error)
        self.assertEqual(body, b"rel")

    def test_redirect_handler_refuses_a_disallowed_location(self) -> None:
        cases = [
            ("https://evil.example/x", "URL host not allowed"),
            ("https://169.254.169.254/latest/meta-data/", "URL host not allowed"),
            ("https://127.0.0.1/admin", "URL host not allowed"),
            ("https://[::1]/", "URL host not allowed"),
            ("https://localhost/", "URL host not allowed"),
            ("http://www.sec.gov/a", "URL must use https"),
            ("https://u:p@www.sec.gov/a", "URL must not contain credentials"),
            ("https://www.sec.gov:8080/a", "URL port not allowed"),
        ]
        for location, reason in cases:
            handler, body, error = _follow("https://www.sec.gov/a", {"https://www.sec.gov/a": _redir(302, location)})
            self.assertIsNotNone(error, location)
            self.assertEqual(error.reason, reason, location)
            self.assertEqual(str(error), reason, location)
            self.assertIsInstance(error, urllib.error.URLError)
            self.assertEqual([c["url"] for c in handler.seen], ["https://www.sec.gov/a"], location)   # the target was never requested

    def test_redirect_hop_limit_is_five(self) -> None:
        def chain(n: int) -> dict:
            routes = {f"https://www.sec.gov/h{i}": _redir(302, f"https://www.sec.gov/h{i + 1}") for i in range(n)}
            routes[f"https://www.sec.gov/h{n}"] = (200, None, f"end{n}".encode())
            return routes

        handler, body, error = _follow("https://www.sec.gov/h0", chain(5))
        self.assertIsNone(error)
        self.assertEqual(body, b"end5")
        self.assertEqual(len(handler.seen), 6)
        handler, body, error = _follow("https://www.sec.gov/h0", chain(6))
        self.assertIsNone(body)
        self.assertEqual(error.reason, "too many redirects")
        self.assertEqual(len(handler.seen), 6)   # the sixth redirect was not followed

    def test_see_other_becomes_get_and_authorization_stays_with_its_origin(self) -> None:
        handler, body, error = _follow("https://efts.sec.gov/post", {
            "https://efts.sec.gov/post": _redir(303, "https://efts.sec.gov/result"),
            "https://efts.sec.gov/result": (200, None, b"got"),
        }, data=b"q=1", method="POST", headers={"Content-Type": "application/x-www-form-urlencoded"})
        self.assertIsNone(error)
        self.assertEqual([(c["method"], c["data"]) for c in handler.seen], [("POST", b"q=1"), ("GET", None)])
        handler, body, error = _follow("https://www.sec.gov/a", {
            "https://www.sec.gov/a": _redir(302, "https://www.sec.gov/b"),
            "https://www.sec.gov/b": _redir(302, "https://data.sec.gov/c"),
            "https://data.sec.gov/c": (200, None, b"x"),
        }, headers={"Authorization": "Bearer provider-key", "X-Custom": "kept"})
        self.assertIsNone(error)
        self.assertEqual([c["authorization"] for c in handler.seen], ["Bearer provider-key", "Bearer provider-key", None])
        self.assertEqual([c["custom"] for c in handler.seen], ["kept", "kept", "kept"])

    def test_first_url_is_checked_before_any_request(self) -> None:
        for start in ("https://evil.example/", "https://169.254.169.254/latest/meta-data/", "http://www.sec.gov/a"):
            handler, body, error = _follow(start, {})
            self.assertIsNotNone(error, start)
            self.assertEqual(handler.seen, [], start)

    def test_a_fetch_site_reports_a_refused_hop_as_a_fetch_failure(self) -> None:
        import server
        import asyncio
        refused = up.UrlPolicyError("URL host not allowed")
        with patch.object(up, "urlopen", side_effect=refused):
            self.assertIsNone(server._fetch_public_html("https://www.sec.gov/Archives/edgar/data/1/x.htm"))
            out = json.loads(asyncio.run(server.parse_public_transcript("https://www.sec.gov/Archives/edgar/data/1/x.htm")))
        self.assertEqual(out["error"]["code"], "FETCH_ERROR")
        self.assertIn("URL host not allowed", out["error"]["message"])

    def _spy(self):
        import server  # noqa: F401  (registers the tools)
        return patch.object(up, "urlopen", side_effect=AssertionError("urlopen must not be called"))

    def test_parse_public_transcript_refusal_and_raw_text_not_fetched(self) -> None:
        import asyncio
        import server

        def ptr(url: str, raw_text: str | None = None) -> dict:
            return json.loads(asyncio.run(server.parse_public_transcript(url, None, raw_text)))

        with self._spy():
            for url in ("https://example.com/", "https://169.254.169.254/latest/meta-data/", "https://127.0.0.1/", "https://2130706433/"):
                self.assertEqual(ptr(url)["error"], {"code": "INPUT_VALIDATION_ERROR", "message": TRANSCRIPT_REFUSAL.format(reason=HOST)}, url)
            self.assertEqual(ptr("https://www.sec.gov:8443/Archives/edgar/data/1/x.htm")["error"]["message"],
                             TRANSCRIPT_REFUSAL.format(reason="URL port not allowed"))
            self.assertEqual(ptr("https://u:p@www.sec.gov/Archives/edgar/data/1/x.htm")["error"]["message"],
                             TRANSCRIPT_REFUSAL.format(reason="URL must not contain credentials"))
            for url in ("https://www.sec.gov/cgi-bin/browse-edgar", "https://data.sec.gov/submissions/CIK0000320193.json",
                        "https://finance.yahoo.com/quote/AAPL/", "https://query1.finance.yahoo.com/v8/finance/chart/AAPL",
                        "https://finnhub.io/api/v1/company-news"):
                self.assertEqual(ptr(url)["error"], {"code": "INPUT_VALIDATION_ERROR",
                                                     "message": TRANSCRIPT_REFUSAL.format(reason="URL path not allowed")}, url)
            self.assertEqual(ptr("http://example.com/")["error"],
                             {"code": "INPUT_VALIDATION_ERROR", "message": "A valid https:// URL or raw_text is required."})
            for url, raw in (("https://example.com/", "Revenue rose. See https://169.254.169.254/latest/meta-data/ and http://localhost/."),
                             ("", "Plain transcript text about revenue.")):
                out = ptr(url, raw)
                self.assertEqual(out["data"]["source"], "raw_text")
                self.assertIsNone(out["data"]["url"])

    def test_index_earnings_release_refusal(self) -> None:
        import asyncio
        import server

        def ier(url: str) -> dict:
            return json.loads(asyncio.run(server.index_earnings_release("ZQI", "latest", url)))["error"]

        with self._spy():
            for url in ("https://example.com/ir/release", "https://markets.wsj.com/a", "https://169.254.169.254/latest/meta-data/"):
                self.assertEqual(ier(url), {"code": "INPUT_VALIDATION_ERROR",
                                            "message": "source_url is not on the fetch allow-list (URL host not allowed)"}, url)
            for url in ("https://www.sec.gov/cgi-bin/browse-edgar", "https://finance.yahoo.com/quote/AAPL/"):
                self.assertEqual(ier(url), {"code": "INPUT_VALIDATION_ERROR",
                                            "message": "source_url is not on the fetch allow-list (URL path not allowed)"}, url)
            self.assertEqual(ier("https://www.wsj.com/articles/x")["message"], "source_url appears paywalled and is not allowed")
            self.assertEqual(ier("http://example.com/release")["message"], "source_url must use https")

    def test_earnings_release_sec_archives_and_registry_classification(self) -> None:
        from yfmcp.tools import earnings
        self.assertEqual(earnings._classify_earnings_source_url("https://www.sec.gov/Archives/edgar/data/1/2/r.htm"), ("sec_8k", None))
        self.assertEqual(earnings._classify_earnings_source_url("https://www.sec.gov/Archives/../cgi-bin/x")[1],
                         "source_url is not on the fetch allow-list (URL path not allowed)")
        registry = {"sources": [{"ticker": "A", "status": "approved", "allowedHosts": ["ir.example.com"], "allowedPathPrefixes": ["/investors/"]}]}
        with up.registry_override(registry):
            self.assertEqual(earnings._classify_earnings_source_url("https://ir.example.com/investors/q3"), ("company_ir", None))
            self.assertEqual(earnings._classify_earnings_source_url("https://ir.example.com/other")[1],
                             "source_url is not on the fetch allow-list (URL path not allowed)")

    def test_evidence_store_prefix_rules_and_path_containment(self) -> None:
        bad = ("", "evidence-cuts/AAPL", "evidence-cuts/../", "evidence-cuts//", "evidence-cuts\\AAPL/", "evidence-cuts/A..B/")
        mem = es.MemoryStore()
        mem.objects["evidence-cuts/AAPL/x.json"] = "{}"
        mem.objects["evidence-cuts/AAPLX/y.json"] = "{}"
        self.assertEqual(mem.list("evidence-cuts/AAPL/", 10), ["evidence-cuts/AAPL/x.json"])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "store"
            (root / "evidence-cuts" / "AAPL").mkdir(parents=True)
            (root / "evidence-cuts" / "AAPL" / "x.json").write_text("{}", encoding="utf-8")
            (Path(tmp) / "secret.json").write_text("{}", encoding="utf-8")
            local = es.LocalDirStore(str(root))
            self.assertEqual(local.list("evidence-cuts/AAPL/", 10), ["evidence-cuts/AAPL/x.json"])
            self.assertEqual(local.list("evidence-cuts/NONE/", 10), [])
            for store in (mem, local):
                for prefix in bad + ("../",):
                    with self.assertRaises(ValueError, msg=prefix) as caught:
                        store.list(prefix, 10)
                    self.assertEqual(str(caught.exception), "invalid evidence list prefix", prefix)
            # A prefix that passes the string rules but resolves outside the root (absolute, or through a symlink) is refused.
            with self.assertRaises(ValueError) as caught:
                local.list("/etc/", 10)
            self.assertEqual(str(caught.exception), "invalid evidence list prefix")
            link = root / "evidence-cuts" / "LINK"
            try:
                link.symlink_to(tmp, target_is_directory=True)
            except OSError:
                self.skipTest("symlinks unavailable")
            with self.assertRaises(ValueError) as caught:
                local.list("evidence-cuts/LINK/", 10)
            self.assertEqual(str(caught.exception), "invalid evidence list prefix")

    def test_evidence_tools_validate_the_ticker_first(self) -> None:
        import asyncio
        import server

        class _SpyStore(es.MemoryStore):
            listed: list[str] = []

            def list(self, prefix: str, limit: int) -> list[str]:
                self.listed.append(prefix)
                return super().list(prefix, limit)

        store = _SpyStore()
        es.set_store_for_tests(store)
        try:
            with patch.object(server, "_consensus_providers", side_effect=AssertionError("providers must not be called")):
                results = [
                    json.loads(asyncio.run(server.get_eps_revisions("BAD/TICKER"))),
                    json.loads(asyncio.run(server.list_evidence_cuts("../ETC"))),
                    json.loads(asyncio.run(server.get_consensus_forecast_curve("A/B", observation_date="2026-10-01"))),
                ]
            for out in results:
                self.assertEqual(out["error"]["code"], "INPUT_VALIDATION_ERROR")
                self.assertIn("Invalid ticker symbol", out["error"]["message"])
            self.assertEqual(store.listed, [])
            valid = json.loads(asyncio.run(server.list_evidence_cuts("aapl")))
            self.assertEqual(valid["storageStatus"], "AVAILABLE")
            self.assertEqual(store.listed, ["evidence-cuts/AAPL/"])
            dotdot = json.loads(asyncio.run(server.list_evidence_cuts("A..B")))
            self.assertEqual((dotdot["storageStatus"], dotdot["message"]), ("FAILED", "invalid evidence list prefix"))
        finally:
            es.set_store_for_tests(es._UNSET)


if __name__ == "__main__":
    unittest.main()
