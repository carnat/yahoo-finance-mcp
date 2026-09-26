#!/usr/bin/env python3
"""Evidence tools end to end (2.5.0), both runtimes, offline.

- get_consensus_forecast_curve writes the day's consensus observation once;
- build_valuation_evidence_pack returns the authority boundary (always null),
  every component with its status, and a receipt; contentSha256 re-derives
  from the payload with the other runtime's canonical JSON;
- the cut is stored content-addressed, retrieved with integrity VERIFIED,
  reported MISMATCH when altered, and listed;
- without storage the full payload and receipt still return, with
  storageStatus UNAVAILABLE, and retrieval says STORAGE_UNAVAILABLE;
- the response envelope passes evidence payloads through verbatim.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

from yfmcp import evidence as ev  # noqa: E402

AUTHORITY = ("selectedMethod", "selectedMultiple", "scenarioWeights", "priceTarget", "g2", "opportunity", "action")
COMPONENTS = ("quote", "evidenceQuality", "consensus", "epsRevisions", "currentCapitalStructure", "currentDilution", "latestGuidance", "materialEvents")


def _verify_hash(pack: dict) -> str:
    body = {k: v for k, v in pack.items() if k != "evidenceCut"}
    return hashlib.sha256(ev.canonical_json(body).encode("utf-8")).hexdigest()


_ENTRY = """
export { buildValuationEvidencePack, getConsensusForecastCurve, getEpsRevisions, getEvidenceCut, getEvidenceQuality, listEvidenceCuts } from "./src/evidence-pack.ts";
export { memoryEvidenceStore, setEvidenceStoreForTests } from "./src/evidence-store.ts";
export { mcpSuccess, setWorkerEnv } from "./src/response.ts";
"""

_HARNESS = r"""
const [bundleUrl] = process.argv.slice(-1);
const m = await import(bundleUrl);
m.setWorkerEnv({ MCP_ENVELOPE_V2: "true", ALPHA_VANTAGE_API_KEY: "test-key", SERVER_VERSION: "2.5.0", BUILD_SHA: "abc123" });
const r = (v) => ({ raw: v, fmt: String(v) });
const now = Math.floor(Date.now() / 1000);
const summary = {
  price: { regularMarketPrice: r(81.2), currency: "USD", regularMarketTime: r(now - 3600) },
  financialData: { financialCurrency: "USD" },
  earningsTrend: { trend: [
    { period: "0y", endDate: "2026-12-31", earningsEstimate: { avg: r(-2.27), low: r(-2.49), high: r(-2.04), numberOfAnalysts: r(8) },
      revenueEstimate: { avg: r(168000000), low: r(150980000), high: r(189000000), numberOfAnalysts: r(12) },
      epsTrend: { current: r(-2.27), "7daysAgo": r(-2.26), "30daysAgo": r(-1.39), "60daysAgo": r(-1.5), "90daysAgo": r(-1.5) },
      epsRevisions: { upLast7days: r(0), upLast30days: r(0), downLast30days: r(4), downLast7Days: r(1) } },
    { period: "+1y", endDate: "2027-12-31", earningsEstimate: { avg: r(-1.1), numberOfAnalysts: r(9) }, revenueEstimate: { avg: r(640000000), numberOfAnalysts: r(11) } },
  ] },
};
const av = { symbol: "ASTS", estimates: [
  { date: "2027-12-31", horizon: "fiscal year", eps_estimate_average: "-1.1016", eps_estimate_analyst_count: "9", revenue_estimate_average: "650775060.00", revenue_estimate_analyst_count: "11" },
  { date: "2026-12-31", horizon: "fiscal year", eps_estimate_average: "-2.2839", eps_estimate_analyst_count: "8", revenue_estimate_average: "168843670.00", revenue_estimate_analyst_count: "12" },
] };
const today = new Date().toISOString().slice(0, 10);
const daysAgo = (n) => new Date(Date.now() - n * 86400000).toISOString().slice(0, 10);
const submissions = { cik: "1780312", filings: { recent: {
  form: ["10-Q", "8-K", "10-K"], filingDate: [daysAgo(40), daysAgo(40), daysAgo(200)], reportDate: [daysAgo(80), daysAgo(40), daysAgo(260)],
  items: ["", "2.02,9.01", ""], isInlineXBRL: [1, 1, 1], accessionNumber: ["0001193125-26-342550", "0001193125-26-342551", "0001780312-26-000011"],
  primaryDocument: ["asts-10q.htm", "ex99.htm", "asts-10k.htm"], acceptanceDateTime: ["", "", ""], isXBRL: [1, 1, 1],
} } };
globalThis.fetch = async (req) => {
  const u = new URL(typeof req === "string" ? req : req.url);
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  if (u.pathname.includes("/quoteSummary/")) return Response.json({ quoteSummary: { result: [summary], error: null } });
  if (u.hostname === "www.alphavantage.co") return Response.json(av);
  if (u.pathname.endsWith("company_tickers.json")) return Response.json({ "0": { cik_str: 1780312, ticker: "ASTS", title: "AST SpaceMobile, Inc." } });
  if (u.pathname.includes("/submissions/")) return Response.json(submissions);
  return new Response("not found", { status: 404 });
};

const out = {};
const store = m.memoryEvidenceStore();
m.setEvidenceStoreForTests(store);
out.curve1 = JSON.parse(await m.getConsensusForecastCurve("ASTS"));
out.curve2 = JSON.parse(await m.getConsensusForecastCurve("ASTS"));
out.badHorizon = JSON.parse(await m.getConsensusForecastCurve("ASTS", 9));
out.revisions = JSON.parse(await m.getEpsRevisions("ASTS"));
out.quality = JSON.parse(await m.getEvidenceQuality("ASTS"));
const packText = await m.buildValuationEvidencePack("ASTS");
out.pack = JSON.parse(packText);
out.envelope = JSON.parse(m.mcpSuccess("build_valuation_evidence_pack", packText));
out.fetched = JSON.parse(await m.getEvidenceCut(out.pack.evidenceCut.evidenceCutId));
out.listed = JSON.parse(await m.listEvidenceCuts("asts"));
const key = out.pack.evidenceCut.storageKey;
store.objects.set(key, store.objects.get(key).replace('"ASTS"', '"ASTX"'));
out.tampered = JSON.parse(await m.getEvidenceCut(out.pack.evidenceCut.evidenceCutId));
out.badId = JSON.parse(await m.getEvidenceCut("../../etc/passwd"));
out.keys = [...store.objects.keys()].sort();
m.setEvidenceStoreForTests(null);
out.noStorePack = JSON.parse(await m.buildValuationEvidencePack("ASTS", 2));
out.noStoreGet = JSON.parse(await m.getEvidenceCut(out.pack.evidenceCut.evidenceCutId));
out.noStoreList = JSON.parse(await m.listEvidenceCuts("ASTS"));
console.log(JSON.stringify(out));
"""


def _run_worker() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry = WORKER / ".evidence-tools-entry.ts"
        bundle = Path(tmp) / "bundle.mjs"
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        entry.write_text(_ENTRY, encoding="utf-8")
        try:
            subprocess.run(
                [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
                 "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
                cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
            )
        finally:
            entry.unlink(missing_ok=True)
        result = subprocess.run([node, str(harness), bundle.as_uri()], check=True, capture_output=True, text=True, timeout=180)
    return json.loads(result.stdout.strip().splitlines()[-1])


class _PackAssertions:
    out: dict

    def test_consensus_observation_is_written_once_a_day(self) -> None:
        first, second = self.out["curve1"]["storage"]["consensusObservation"], self.out["curve2"]["storage"]["consensusObservation"]
        self.assertEqual((first["status"], second["status"]), ("STORED", "ALREADY_STORED"))
        self.assertRegex(first["key"], r"^consensus-history/ASTS/\d{4}-\d{2}-\d{2}\.json$")
        fy0 = self.out["curve1"]["periods"][0]["metrics"]["eps"]
        self.assertEqual((fy0["coverage"], len(fy0["providers"])), ("PROVIDER_COVERED", 2))
        self.assertEqual(self.out["badHorizon"]["code"], "INPUT_VALIDATION_ERROR")
        self.assertEqual(len(self.out["revisions"]["storedConsensusObservations"]["observationDates"]), 1)

    def test_pack_authority_boundary_and_components(self) -> None:
        pack = self.out["pack"]
        self.assertEqual(pack["schema"], "yfmcp.evidence-cut/1")
        self.assertEqual(pack["decisionUse"], "EVIDENCE_ONLY")
        for field in AUTHORITY:
            self.assertIn(field, pack)
            self.assertIsNone(pack[field])
        for name in COMPONENTS:
            self.assertIn(pack[name]["status"], {"OK", "LIMITED", "FAILED", "NOT_APPLICABLE"}, name)
            self.assertIn("sourceTool", pack[name])
        self.assertEqual(pack["quote"]["data"]["price"], 81.2)
        self.assertEqual(pack["consensus"]["status"], "OK")
        # SEC document fetches are not stubbed: those components fail visibly, they are not dropped.
        self.assertIn(pack["currentCapitalStructure"]["status"], {"FAILED", "LIMITED"})
        receipt = pack["provenance"]
        self.assertEqual(sorted(c["name"] for c in receipt["components"]), sorted(COMPONENTS))
        self.assertEqual(receipt["coverage"]["state"], "PARTIAL")
        self.assertTrue(all(len(c["sha256"]) == 64 for c in receipt["components"]))

    def test_content_hash_rederives_across_runtimes(self) -> None:
        pack = self.out["pack"]
        self.assertEqual(_verify_hash(pack), pack["evidenceCut"]["contentSha256"])
        self.assertTrue(pack["evidenceCut"]["evidenceCutId"].endswith("_" + pack["evidenceCut"]["contentSha256"]))

    def test_store_retrieve_verify_list(self) -> None:
        cut = self.out["pack"]["evidenceCut"]
        self.assertEqual(cut["storageStatus"], "STORED")
        self.assertIn(cut["storageKey"], self.out["keys"])
        fetched = self.out["fetched"]
        self.assertEqual(fetched["integrity"], "VERIFIED")
        self.assertEqual(fetched["document"], {k: v for k, v in self.out["pack"].items() if k != "evidenceCut"})
        self.assertEqual(self.out["tampered"]["integrity"], "MISMATCH")
        self.assertEqual(self.out["badId"]["code"], "INPUT_VALIDATION_ERROR")
        self.assertEqual([c["evidenceCutId"] for c in self.out["listed"]["cuts"]], [cut["evidenceCutId"]])

    def test_no_storage_still_returns_the_pack(self) -> None:
        pack = self.out["noStorePack"]
        self.assertEqual(pack["evidenceCut"]["storageStatus"], "UNAVAILABLE")
        self.assertIsNone(pack["evidenceCut"]["storageKey"])
        self.assertEqual(_verify_hash(pack), pack["evidenceCut"]["contentSha256"])
        self.assertEqual(len(pack["consensus"]["data"]["periods"]), 3)
        self.assertEqual(pack["evidenceQuality"]["data"]["families"]["evidenceStorage"]["state"], "UNAVAILABLE")
        self.assertEqual(self.out["noStoreGet"]["code"], "STORAGE_UNAVAILABLE")
        self.assertEqual(self.out["noStoreList"]["storageStatus"], "UNAVAILABLE")

    def test_quality_preflight(self) -> None:
        q = self.out["quality"]
        self.assertEqual(q["families"]["quote"]["state"], "READY")
        self.assertEqual(q["families"]["secPeriodicFiling"]["state"], "READY")
        self.assertEqual(q["families"]["guidance"]["state"], "READY_TO_EXTRACT")


class TestWorkerEvidenceTools(_PackAssertions, unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _run_worker()

    def test_envelope_passes_evidence_through_verbatim(self) -> None:
        env = self.out["envelope"]
        self.assertTrue(env["ok"])
        self.assertEqual(env["data"], self.out["pack"])


# ── Python runtime ───────────────────────────────────────────────────────────

def _ensure_mcp_available() -> None:
    try:
        import mcp.server  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _FastMCPStub:
        def __init__(self, *a: object, **kw: object) -> None:
            pass

        def tool(self, *a: object, **kw: object):  # type: ignore[return]
            if a and callable(a[0]):
                return a[0]
            return lambda fn: fn

    mcp_mod = types.ModuleType("mcp")
    server_mod = types.ModuleType("mcp.server")
    fastmcp_mod = types.ModuleType("mcp.server.fastmcp")
    fastmcp_mod.FastMCP = _FastMCPStub  # type: ignore[attr-defined]
    mcp_mod.server = server_mod  # type: ignore[attr-defined]
    server_mod.fastmcp = fastmcp_mod  # type: ignore[attr-defined]
    sys.modules.setdefault("mcp", mcp_mod)
    sys.modules.setdefault("mcp.server", server_mod)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_mod)


def _python_outputs() -> dict:
    _ensure_mcp_available()
    import datetime as dt

    import server as srv
    from yfmcp import envelope as envelope_mod
    from yfmcp import evidence_store as es
    from yfmcp.clients.market_providers import ProviderJsonResult

    now = dt.datetime.now(dt.timezone.utc)
    retrieved = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    trend = [
        {"period": "0y", "endDate": "2026-12-31", "earningsEstimate": {"avg": -2.27, "low": -2.49, "high": -2.04, "numberOfAnalysts": 8},
         "revenueEstimate": {"avg": 168000000, "low": 150980000, "high": 189000000, "numberOfAnalysts": 12},
         "epsTrend": {"current": -2.27, "7daysAgo": -2.26, "30daysAgo": -1.39, "60daysAgo": -1.5, "90daysAgo": -1.5},
         "epsRevisions": {"upLast7days": 0, "upLast30days": 0, "downLast30days": 4, "downLast7Days": 1}},
        {"period": "+1y", "endDate": "2027-12-31", "earningsEstimate": {"avg": -1.1, "numberOfAnalysts": 9}, "revenueEstimate": {"avg": 640000000, "numberOfAnalysts": 11}},
    ]
    yahoo = ev.yahoo_consensus_input(trend, retrieved_at=retrieved, financial_currency="USD")
    quote = {"price": 81.2, "currency": "USD", "priceTime": (now - dt.timedelta(hours=1)).isoformat(timespec="milliseconds").replace("+00:00", "Z"), "status": "OK", "message": None}
    av = {"symbol": "ASTS", "estimates": [
        {"date": "2027-12-31", "horizon": "fiscal year", "eps_estimate_average": "-1.1016", "eps_estimate_analyst_count": "9", "revenue_estimate_average": "650775060.00", "revenue_estimate_analyst_count": "11"},
        {"date": "2026-12-31", "horizon": "fiscal year", "eps_estimate_average": "-2.2839", "eps_estimate_analyst_count": "8", "revenue_estimate_average": "168843670.00", "revenue_estimate_analyst_count": "12"},
    ]}

    def days_ago(n: int) -> str:
        return (now - dt.timedelta(days=n)).date().isoformat()

    subs = {"filings": {"recent": {
        "form": ["10-Q", "8-K", "10-K"], "filingDate": [days_ago(40), days_ago(40), days_ago(200)], "reportDate": [days_ago(80), days_ago(40), days_ago(260)],
        "items": ["", "2.02,9.01", ""], "isInlineXBRL": [1, 1, 1],
    }}}
    failed = json.dumps({"ticker": "ASTS", "status": "FILING_TEXT_NOT_AVAILABLE", "code": "FILING_TEXT_NOT_AVAILABLE", "message": "stub"})
    out: dict = {}

    def run(coro):  # type: ignore[no-untyped-def]
        return json.loads(asyncio.run(coro))

    store = es.MemoryStore()
    with patch("server._yahoo_consensus_snapshot", return_value=(yahoo, quote)), \
            patch("server._fetch_alpha_vantage_json", new=AsyncMock(return_value=ProviderJsonResult(av, "OK", "https://www.alphavantage.co/query", True, "MISS", fetched_at=retrieved))), \
            patch("server._get_submissions_for_ticker", new=AsyncMock(return_value=("0001780312", subs))), \
            patch("server.extract_capital_structure", new=AsyncMock(return_value=failed)), \
            patch("server.extract_dilution_bridge", new=AsyncMock(return_value=failed)), \
            patch("server.extract_guidance", new=AsyncMock(return_value=json.dumps({"ticker": "ASTS", "revenue": {"status": "NOT_DISCLOSED"}}))), \
            patch("server.list_sec_material_filings", new=AsyncMock(return_value=json.dumps({"ticker": "ASTS", "filings": []}))):
        es.set_store_for_tests(store)
        try:
            out["curve1"] = run(srv.get_consensus_forecast_curve("ASTS"))
            out["curve2"] = run(srv.get_consensus_forecast_curve("ASTS"))
            out["badHorizon"] = run(srv.get_consensus_forecast_curve("ASTS", horizon_years=9))
            out["revisions"] = run(srv.get_eps_revisions("ASTS"))
            out["quality"] = run(srv.get_evidence_quality("ASTS"))
            pack_text = asyncio.run(srv.build_valuation_evidence_pack("ASTS"))
            out["pack"] = json.loads(pack_text)
            out["envelopeData"] = envelope_mod._enrich_facts(copy.deepcopy(out["pack"]))
            cut_id = out["pack"]["evidenceCut"]["evidenceCutId"]
            out["fetched"] = run(srv.get_evidence_cut(cut_id))
            out["listed"] = run(srv.list_evidence_cuts("asts"))
            key = out["pack"]["evidenceCut"]["storageKey"]
            store.objects[key] = store.objects[key].replace('"ASTS"', '"ASTX"', 1)
            out["tampered"] = run(srv.get_evidence_cut(cut_id))
            out["badId"] = run(srv.get_evidence_cut("../../etc/passwd"))
            out["keys"] = sorted(store.objects)
            es.set_store_for_tests(None)
            out["noStorePack"] = run(srv.build_valuation_evidence_pack("ASTS", horizon_years=2))
            out["noStoreGet"] = run(srv.get_evidence_cut(cut_id))
            out["noStoreList"] = run(srv.list_evidence_cuts("ASTS"))
        finally:
            es.set_store_for_tests(es._UNSET)
    return out


class TestPythonEvidenceTools(_PackAssertions, unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()

    def test_envelope_passes_evidence_through_verbatim(self) -> None:
        self.assertEqual(self.out["envelopeData"], self.out["pack"])

    def test_local_dir_store_round_trip(self) -> None:
        from yfmcp import evidence_store as es

        with tempfile.TemporaryDirectory() as tmp:
            store = es.LocalDirStore(tmp)
            key = "evidence-cuts/ASTS/20260926T080000Z/" + "a" * 64 + ".json"
            store.put(key, "{}", {})
            self.assertEqual((store.get(key), store.exists(key), store.list("evidence-cuts/ASTS/", 10)), ("{}", True, [key]))
            with self.assertRaises(ValueError):
                store.get("evidence-cuts/../../x.json")


if __name__ == "__main__":
    unittest.main()
