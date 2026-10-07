#!/usr/bin/env python3
"""Alpha Vantage estimate snapshots and stale fallback (2.5.29), end to end in both runtimes.

A good EARNINGS_ESTIMATES payload is written once a day to the evidence store; a refused request (here the daily
quota) reads the newest stored payload back and marks it stale. Offline: the providers are mocked.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from yfmcp import evidence as ev  # noqa: E402
from yfmcp import evidence_store as es  # noqa: E402
from yfmcp.clients.market_providers import ProviderJsonResult  # noqa: E402

WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
AV = {"symbol": "ASTS", "estimates": [
    {"date": "2026-12-31", "horizon": "fiscal year", "eps_estimate_average": "-1.2020", "eps_estimate_high": "-0.9000",
     "eps_estimate_low": "-1.5000", "eps_estimate_analyst_count": "8"},
]}
QUOTA = "We have detected your API key as ABC and our standard API rate limit is 25 requests per day."
AGE_HOURS = 30


def _ago(hours: float) -> str:
    t = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours)
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _python_outputs() -> dict:
    import server as srv

    store = es.MemoryStore()
    es.set_store_for_tests(store)
    try:
        fetched = _ago(0)
        ok = ProviderJsonResult(AV, "OK", "https://www.alphavantage.co/query", True, "MISS", fetched_at=fetched)
        with patch("server._fetch_alpha_vantage_json", new=AsyncMock(return_value=ok)):
            live = asyncio.run(srv._alpha_vantage_estimates("ASTS"))
            again = asyncio.run(srv._alpha_vantage_estimates("ASTS"))
        written = sorted(store.objects)
        # A snapshot from 30 hours ago, and the quota refused.
        old = _ago(AGE_HOURS)
        store.objects.clear()
        store.objects[ev.provider_snapshot_key("alpha_vantage", "EARNINGS_ESTIMATES", "ASTS", old)] = ev.provider_snapshot_body(
            "alpha_vantage", "EARNINGS_ESTIMATES", "ASTS", old, AV)
        refused = ProviderJsonResult(None, "RATE_LIMIT", "https://www.alphavantage.co/query", True, "MISS", message=QUOTA)
        denied = ProviderJsonResult(None, "AUTH_ERROR", "https://www.alphavantage.co/query", True, "MISS", message="Invalid API key.")
        with patch("server._fetch_alpha_vantage_json", new=AsyncMock(return_value=refused)):
            stale = asyncio.run(srv._alpha_vantage_estimates("ASTS"))
            other = asyncio.run(srv._alpha_vantage_estimates("MSFT"))
        with patch("server._fetch_alpha_vantage_json", new=AsyncMock(return_value=denied)):
            auth = asyncio.run(srv._alpha_vantage_estimates("ASTS"))
        es.set_store_for_tests(None)
        with patch("server._fetch_alpha_vantage_json", new=AsyncMock(return_value=refused)):
            no_store = asyncio.run(srv._alpha_vantage_estimates("ASTS"))
    finally:
        es.set_store_for_tests(es._UNSET)
    return {"live": live, "again": again, "written": written, "old": old,
            "stale": stale, "other": other, "auth": auth, "noStore": no_store}


_ENTRY = r"""
export { getEpsRevisions } from "{SRC}/evidence-pack.ts";
export { setEvidenceStoreForTests, memoryEvidenceStore } from "{SRC}/evidence-store.ts";
export { setWorkerEnv } from "{SRC}/response.ts";
export { providerSnapshotKey, providerSnapshotBody } from "{SRC}/evidence.ts";
"""

_HARNESS = r"""
const [bundleUrl, fixturesPath] = process.argv.slice(-2);
const m = await import(bundleUrl);
const { readFileSync } = await import("node:fs");
const f = JSON.parse(readFileSync(fixturesPath, "utf8"));
const av = new Map([["ASTS", f.av], ["RKLB", { Information: f.quota }], ["MSFT", { Information: f.quota }]]);
globalThis.fetch = async (input) => {
  const u = new URL(typeof input === "string" ? input : input.url);
  if (u.hostname === "www.alphavantage.co") return Response.json(av.get(u.searchParams.get("symbol")));
  return new Response("not found", { status: 404 });
};
m.setWorkerEnv({ ALPHA_VANTAGE_API_KEY: "test-av-key" });
const store = m.memoryEvidenceStore();
m.setEvidenceStoreForTests(store);
const old = new Date(Date.now() - f.ageHours * 3_600_000).toISOString();
store.objects.set(m.providerSnapshotKey("alpha_vantage", "EARNINGS_ESTIMATES", "RKLB", old),
  m.providerSnapshotBody("alpha_vantage", "EARNINGS_ESTIMATES", "RKLB", old, { ...f.av, symbol: "RKLB" }));
const provider = (text) => JSON.parse(text).providers.find((p) => p.provider === "alpha_vantage");
const warnings = (text) => JSON.parse(text).warnings.map((w) => w.code);
const live = await m.getEpsRevisions("ASTS");
const written = [...store.objects.keys()].filter((k) => k.startsWith("provider-snapshots/")).sort();
const stale = await m.getEpsRevisions("RKLB");
const other = await m.getEpsRevisions("MSFT");
console.log(JSON.stringify({
  live: provider(live), written, writtenBody: JSON.parse(store.objects.get(written[0]) ?? "null"),
  old, stale: provider(stale), staleWarnings: warnings(stale), other: provider(other), otherWarnings: warnings(other),
}));
"""


def _worker_outputs() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry = Path(tmp) / "entry.ts"
        bundle = Path(tmp) / "pack.mjs"
        harness = Path(tmp) / "harness.mjs"
        fx = Path(tmp) / "fixtures.json"
        entry.write_text(_ENTRY.replace("{SRC}", (WORKER / "src").as_posix()), encoding="utf-8")
        fx.write_text(json.dumps({"av": AV, "quota": QUOTA, "ageHours": AGE_HOURS}), encoding="utf-8")
        harness.write_text(_HARNESS, encoding="utf-8")
        subprocess.run([str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=node",
                        f"--outfile={bundle}", "--log-level=error"], cwd=WORKER, check=True, capture_output=True, text=True, timeout=120)
        result = subprocess.run([node, str(harness), bundle.as_uri(), str(fx)], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


class TestPythonSnapshots(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()

    def test_good_payload_is_kept_once_a_day(self) -> None:
        self.assertEqual(self.out["live"]["status"], "OK")
        self.assertNotIn("staleFallback", self.out["live"])
        day = self.out["live"]["retrievedAt"][:10]
        self.assertEqual(self.out["written"], [f"provider-snapshots/alpha_vantage/EARNINGS_ESTIMATES/ASTS/{day}.json"])
        self.assertEqual(self.out["again"]["periods"], self.out["live"]["periods"])

    def test_quota_refusal_reads_the_stored_payload(self) -> None:
        stale = self.out["stale"]
        self.assertEqual((stale["status"], stale["retrievedAt"]), ("OK", self.out["old"]))
        self.assertEqual(stale["periods"][0]["eps"]["mean"], -1.202)
        fb = stale["staleFallback"]
        self.assertEqual((fb["liveStatus"], fb["liveMessage"]), ("RATE_LIMIT", QUOTA))
        self.assertAlmostEqual(fb["ageHours"], AGE_HOURS, delta=0.2)
        self.assertEqual(fb["snapshotKey"], f"provider-snapshots/alpha_vantage/EARNINGS_ESTIMATES/ASTS/{self.out['old'][:10]}.json")

    def test_no_fallback_without_a_snapshot_or_for_a_bad_key(self) -> None:
        for name, status in (("other", "RATE_LIMIT"), ("auth", "AUTH_ERROR"), ("noStore", "RATE_LIMIT")):
            out = self.out[name]
            self.assertEqual((out["status"], out["periods"]), (status, []), name)
            self.assertNotIn("staleFallback", out)


class TestWorkerSnapshots(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _worker_outputs()

    def test_good_payload_is_kept_once_a_day(self) -> None:
        live = self.out["live"]
        self.assertEqual(live["status"], "OK")
        self.assertNotIn("staleFallback", live)
        self.assertEqual(self.out["written"][0], f"provider-snapshots/alpha_vantage/EARNINGS_ESTIMATES/ASTS/{live['retrievedAt'][:10]}.json")
        body = self.out["writtenBody"]
        self.assertEqual((body["schema"], body["ticker"], body["fetchedAt"], body["payload"]), ("yfmcp.provider-snapshot/1", "ASTS", live["retrievedAt"], AV))

    def test_quota_refusal_reads_the_stored_payload(self) -> None:
        stale = self.out["stale"]
        self.assertEqual((stale["status"], stale["retrievedAt"]), ("OK", self.out["old"]))
        fb = stale["staleFallback"]
        self.assertEqual((fb["liveStatus"], fb["liveMessage"]), ("RATE_LIMIT", QUOTA))
        self.assertAlmostEqual(fb["ageHours"], AGE_HOURS, delta=0.2)
        self.assertIn("PROVIDER_DATA_STALE", self.out["staleWarnings"])

    def test_no_fallback_without_a_snapshot(self) -> None:
        self.assertEqual(self.out["other"]["status"], "RATE_LIMIT")
        self.assertNotIn("staleFallback", self.out["other"])
        self.assertNotIn("PROVIDER_DATA_STALE", self.out["otherWarnings"])


if __name__ == "__main__":
    unittest.main()
