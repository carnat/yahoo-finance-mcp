#!/usr/bin/env python3
"""Worker exact accession (R4 Q3a) and test-only ticker -> CIK fixtures (R4 Q4), 2.13.0.

Bundles worker/src with esbuild and drives the real dispatch paths in one Node process. Fetch serves the canned SEC
responses of scripts/parity/cases_accession.py (ticker index, a submissions list with a 10-Q, an 8-K and an older 10-K,
the filing documents); anything else answers 404, so nothing reaches the network.

Q3a, through a full tool call in expanded mode (callToolResult) and grouped mode (callVisibleToolResult):
- every action that honours an accession returns the exact ACCESSION_NOT_FOUND envelope on a miss (code, message,
  requestedAccession, retryable false; ok false, data null, meta.tool the public action);
- no fallback: on a miss no filing document is read (neither the latest 10-K nor the 10-Q);
- a found accession is unchanged: get_sec_filing_index and get_sec_filing_outline answer from that filing;
- extract_china_exposure (out of scope) still reads FILING_NOT_FOUND_TRY_OTHER_TYPE on a miss (the structured
  geographic_revenue HTML fallback keeps it too; scripts/test_sec_fact_payloads.py covers that path).

Q4:
- the smoke table is gone from worker/src; production lookup has no fixtures (Yahoo, SEC index and atom all failing
  leaves a former table entry unresolved);
- setTickerCikFixturesForTests injects scripts/parity/fixtures/ticker_cik_fixtures.json (the 11 former entries), keys
  case-insensitive, read after SEC's ticker index and before the atom; null empties it.
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

from scripts.parity import cases_accession as ca  # noqa: E402
from scripts.source_text import worker_runtime_source  # noqa: E402

CATALOG = json.loads((ROOT / "tool_catalog.json").read_text(encoding="utf-8"))
GROUP_OF = {action: group for group, spec in CATALOG["groups"].items() for action in spec["actions"]}
FIXTURES = json.loads((ROOT / "scripts" / "parity" / "fixtures" / "ticker_cik_fixtures.json").read_text(encoding="utf-8"))

MISS = ca.MISSING_ACC
MISS_ROWS = [
    ["get_sec_filing_index", {"ticker": "AAPL", "accession_number": MISS}],
    ["search_sec_filing_text", {"ticker": "AAPL", "search_terms": ["revenue"], "accession_number": MISS}],
    ["extract_sec_filing_fact", {"ticker": "AAPL", "fact_name": "backlog", "accession_number": MISS}],
    ["extract_dilution_bridge", {"ticker": "AAPL", "price": 100, "accession_number": MISS}],
    ["get_share_count_scenarios", {"ticker": "AAPL", "scenarios": [{"name": "base", "price": 100}], "accession_number": MISS}],
    ["extract_capital_structure", {"ticker": "AAPL", "accession_number": MISS}],
    ["extract_funding_capex_schedule", {"ticker": "AAPL", "accession_number": MISS}],
    ["extract_operating_driver_ledger", {"ticker": "AAPL", "accession_number": MISS}],
    ["get_sec_filing_outline", {"ticker": "AAPL", "accession_number": MISS}],
]

_HARNESS = r"""
import fs from "node:fs";
const [bundleUrl, inputPath] = process.argv.slice(-2);
const input = JSON.parse(fs.readFileSync(inputPath, "utf8"));

let fetched = [];
let routes = input.routes;
globalThis.fetch = async (req) => {
  const url = typeof req === "string" ? req : req instanceof URL ? req.href : req.url;
  fetched.push(url);
  const route = routes[url];
  if (!route) return new Response("not found", { status: 404 });
  const body = route.json !== undefined ? JSON.stringify(route.json) : route.body ?? "";
  return new Response(body, { status: route.status ?? 200 });
};
delete globalThis.caches;
console.log = () => {};
console.warn = () => {};
console.error = () => {};

let isolate = 0;
const fresh = () => import(`${bundleUrl}?isolate=${++isolate}`);
const BASE_ENV = { MCP_ENVELOPE_V2: "true" };

async function call(mode, action, args) {
  const m = await fresh();
  m.response.setWorkerEnv({ ...BASE_ENV, TOOL_MODE: mode });
  fetched = [];
  const result = mode === "expanded"
    ? await m.tools.callToolResult(action, args)
    : await m.tools.callVisibleToolResult(input.groupOf[action], { action, params: args });
  return { envelope: JSON.parse(result.text), fetched: [...fetched] };
}

const out = { miss: {}, hit: {}, q4: {} };
for (const mode of ["expanded", "grouped"]) {
  out.miss[mode] = {};
  for (const [action, args] of input.missRows) out.miss[mode][action] = await call(mode, action, args);
}
out.hit.index = await call("expanded", "get_sec_filing_index", { ticker: "AAPL", accession_number: input.tenKAcc });
out.hit.outline = await call("expanded", "get_sec_filing_outline", { ticker: "AAPL", accession_number: input.tenKAcc });
out.hit.outlineFn = JSON.parse(await (await fresh()).yahooFinance.getFilingOutline("AAPL", input.tenKAcc, null));
out.china = await call("expanded", "extract_china_exposure", { ticker: "AAPL", accession_number: input.missAcc });

// Q4: Yahoo, SEC's ticker index and the atom all fail (404) unless a route says otherwise.
async function cik(ticker, fixtures, extraRoutes = {}) {
  const m = await fresh();
  routes = extraRoutes;
  if (fixtures !== undefined) m.yahooFinance.setTickerCikFixturesForTests(fixtures === null ? null : new Map(Object.entries(fixtures)));
  try {
    return (await m.yahooFinance.getSubmissionsForTicker(ticker)).cikPadded;
  } finally {
    routes = input.routes;
  }
}
out.q4.production = {};
for (const ticker of Object.keys(input.fixtures)) out.q4.production[ticker] = await cik(ticker);
out.q4.injected = {};
for (const ticker of Object.keys(input.fixtures)) out.q4.injected[ticker] = await cik(ticker, input.fixtures);
out.q4.lowercaseKey = await cik("FIXT", { fixt: "0000000042" });
out.q4.cleared = await (async () => {
  const m = await fresh();
  routes = {};
  m.yahooFinance.setTickerCikFixturesForTests(new Map(Object.entries(input.fixtures)));
  m.yahooFinance.setTickerCikFixturesForTests(null);
  try { return (await m.yahooFinance.getSubmissionsForTicker("AAPL")).cikPadded; } finally { routes = input.routes; }
})();
out.q4.secIndexWins = await cik("AAPL", input.fixtures, {
  "https://www.sec.gov/files/company_tickers.json": { json: { "0": { cik_str: 999999, ticker: "AAPL", title: "Fake" } } },
});
out.q4.fixtureBeforeAtom = await cik("AAPL", input.fixtures, { [input.atomUrl]: { body: input.atomBody } });
out.q4.atomWithoutFixture = await cik("AAPL", undefined, { [input.atomUrl]: { body: input.atomBody } });
out.errorCodes = (await fresh()).response.ErrorCode;

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
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".accession-test-") as tmp:
        tmp_path = Path(tmp)
        entry = tmp_path / "entry.ts"
        entry.write_text(
            'export * as tools from "../src/tools.ts";\n'
            'export * as response from "../src/response.ts";\n'
            'export * as yahooFinance from "../src/yahoo-finance.ts";\n',
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
            "routes": ca.ROUTES,
            "groupOf": GROUP_OF,
            "missRows": MISS_ROWS,
            "missAcc": MISS,
            "tenKAcc": ca.TEN_K_ACC,
            "fixtures": FIXTURES,
            "atomUrl": ca_atom_url("AAPL"),
            "atomBody": '<feed><link href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0000111111"/></feed>',
        }
        input_path = tmp_path / "input.json"
        input_path.write_text(json.dumps(payload), encoding="utf-8")
        proc = subprocess.run([node, str(harness), bundle.as_uri(), str(input_path)], cwd=WORKER,
                              capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            raise AssertionError(f"harness failed:\n{proc.stderr[-4000:]}")
        _RESULT.update(json.loads(proc.stdout.strip().splitlines()[-1]))
    return _RESULT


def ca_atom_url(ticker: str) -> str:
    return (f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={ticker}"
            "&type=&dateb=&owner=include&count=10&output=atom")


DOCUMENTS = {ca.TEN_K_DOC, ca.TEN_Q_DOC, ca.TEN_K_INDEX_PAGE}


@unittest.skipIf(_node() is None, "node and worker/node_modules (npm ci) are required")
class TestAccessionNotFound(unittest.TestCase):
    def test_error_code_is_public(self):
        self.assertEqual(_run()["errorCodes"].get("ACCESSION_NOT_FOUND"), "ACCESSION_NOT_FOUND")

    def test_every_action_returns_the_exact_envelope_in_both_modes(self):
        expected_error = ca.not_found()["error"]
        for mode in ("expanded", "grouped"):
            for action, _args in MISS_ROWS:
                with self.subTest(mode=mode, action=action):
                    env = _run()["miss"][mode][action]["envelope"]
                    self.assertIs(env["ok"], False)
                    self.assertIsNone(env["data"])
                    self.assertEqual(env["error"], expected_error)
                    self.assertEqual(env["meta"]["tool"], action)

    def test_no_fallback_reads_no_filing_document(self):
        for mode in ("expanded", "grouped"):
            for action, _args in MISS_ROWS:
                with self.subTest(mode=mode, action=action):
                    fetched = set(_run()["miss"][mode][action]["fetched"])
                    self.assertFalse(fetched & DOCUMENTS, f"{action} read {sorted(fetched & DOCUMENTS)}")
                    self.assertFalse(any("/Archives/" in url for url in fetched), sorted(fetched))

    def test_found_accession_answers_from_that_filing(self):
        index = _run()["hit"]["index"]["envelope"]
        self.assertIs(index["ok"], True)
        self.assertEqual(index["data"]["accessionNumber"], ca.TEN_K_ACC)
        self.assertEqual(index["data"]["documentUrl"], ca.TEN_K_DOC)
        outline = _run()["hit"]["outline"]["envelope"]
        self.assertIs(outline["ok"], True)
        self.assertEqual(outline["data"]["accessionNumber"], ca.TEN_K_ACC)
        self.assertNotIn(ca.TEN_Q_DOC, _run()["hit"]["outline"]["fetched"])

    def test_get_filing_outline_function_resolves_the_accession_without_document_url(self):
        out = _run()["hit"]["outlineFn"]
        self.assertEqual(out["documentUrl"], ca.TEN_K_DOC)
        self.assertEqual([h["title"] for h in out["outline"]], ["Item 1. Business", "Item 1A. Risk Factors"])
        self.assertEqual(out["status"], "OK")

    def test_china_exposure_miss_is_unchanged(self):
        env = _run()["china"]["envelope"]
        self.assertIs(env["ok"], True)
        self.assertEqual(env["data"]["code"], "FILING_NOT_FOUND_TRY_OTHER_TYPE")
        self.assertEqual(env["data"]["overallStatus"], "FILING_NOT_FOUND_TRY_OTHER_TYPE")


@unittest.skipIf(_node() is None, "node and worker/node_modules (npm ci) are required")
class TestTickerCikFixtures(unittest.TestCase):
    def test_smoke_table_is_gone_from_worker_source(self):
        for path in (WORKER / "src").rglob("*.ts"):
            with self.subTest(path=path.relative_to(WORKER / "src").as_posix()):
                self.assertNotIn("SMOKE_TICKER_CIK", path.read_text(encoding="utf-8"))
        # Every Worker runtime module (scripts/source_text.py): no module may carry a fixture entry.
        source = worker_runtime_source()
        self.assertRegex(source, r"export function setTickerCikFixturesForTests\(")
        # Production holds no entries: the module-level map starts empty.
        self.assertRegex(source, r"let tickerCikFixtures: ReadonlyMap<string, string> = new Map\(\);")
        for ticker, cik in FIXTURES.items():
            self.assertIsNone(re.search(rf'\b{ticker}: "{cik}"', source), ticker)

    def test_shared_fixture_holds_the_eleven_former_entries(self):
        self.assertEqual(len(FIXTURES), 11)
        self.assertEqual(FIXTURES["AAPL"], "0000320193")
        self.assertEqual(FIXTURES["AAOI"], "0001158114")
        for ticker, cik in FIXTURES.items():
            self.assertRegex(cik, r"^\d{10}$", ticker)

    def test_production_lookup_has_no_fixture(self):
        self.assertEqual(_run()["q4"]["production"], {t: None for t in FIXTURES})

    def test_injected_fixture_resolves_each_entry(self):
        self.assertEqual(_run()["q4"]["injected"], FIXTURES)

    def test_fixture_keys_are_case_insensitive(self):
        self.assertEqual(_run()["q4"]["lowercaseKey"], "0000000042")

    def test_null_empties_the_fixtures(self):
        self.assertIsNone(_run()["q4"]["cleared"])

    def test_order_sec_index_then_fixture_then_atom(self):
        self.assertEqual(_run()["q4"]["secIndexWins"], "0000999999")
        self.assertEqual(_run()["q4"]["fixtureBeforeAtom"], "0000320193")
        self.assertEqual(_run()["q4"]["atomWithoutFixture"], "0000111111")


if __name__ == "__main__":
    unittest.main()
