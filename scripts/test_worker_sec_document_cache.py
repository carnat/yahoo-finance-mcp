#!/usr/bin/env python3
"""Worker cache for SEC archive documents, and per-call meta.cacheHit/cacheSource.

Runs the bundled Worker as two isolates in one Miniflare instance, so they
share the Cache API but not module state, against a mocked SEC archive. The
filing tools that read a document by URL (outline, section, table list,
table) must fetch each document once per data center, share concurrent reads,
never cache failures, and report where each call's data came from.
"""

from __future__ import annotations

import functools
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
MINIFLARE = WORKER / "node_modules" / "miniflare" / "dist" / "src" / "index.js"

_HARNESS = r"""
const [miniflareUrl, scriptPath] = process.argv.slice(-2);
const { Miniflare, convertV4MiniflareOptions } = await import(miniflareUrl);
const counts = {};
const DOC = "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/aapl-20250927.htm";
const DOC2 = "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/second.htm";
const MISSING = "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/missing.htm";
const html = `<html><body>
<h2>Item 1. Business</h2><p>We design and sell devices.</p>
<h2>Item 1A. Risk Factors</h2><p>Supply chains can fail.</p>
<table><tr><th>Region</th><th>Net sales</th></tr><tr><td>Americas</td><td>100</td></tr><tr><td>Europe</td><td>50</td></tr></table>
</body></html>`;

async function outbound(req) {
  const u = new URL(req.url);
  counts[u.pathname] = (counts[u.pathname] ?? 0) + 1;
  if (u.hostname !== "www.sec.gov") return new Response("{}", { status: 404 });
  // Slow enough that the two concurrent reads of DOC2 overlap.
  await new Promise((r) => setTimeout(r, 200));
  if (u.pathname.endsWith("missing.htm")) return new Response("Not Found", { status: 404 });
  return new Response(html, { headers: { "content-type": "text/html" } });
}

const worker = (name) => ({
  name, modules: true, scriptPath, compatibilityDate: "2026-09-21", compatibilityFlags: ["nodejs_als"],
  bindings: { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true" }, outboundService: outbound,
});
const mf = new Miniflare(convertV4MiniflareOptions({ workers: [worker("first"), worker("second")] }));
let id = 0;
async function call(isolate, name, args) {
  const res = await isolate.fetch("https://x/mcp", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: ++id, method: "tools/call", params: { name: "sec_filings", arguments: { action: name, params: { ticker: "AAPL", ...args } } } }),
  });
  const envelope = (await res.json()).result.structuredContent;
  return { ok: envelope.ok, error: envelope.error, meta: envelope.meta, secCache: res.headers.get("x-sec-cache"), yahooCache: res.headers.get("x-yahoo-cache") };
}
const path = (url) => new URL(url).pathname;
async function run(isolate) {
  const out = {};
  out.outline = await call(isolate, "get_sec_filing_outline", { document_url: DOC });
  out.tables = await call(isolate, "list_sec_filing_tables", { document_url: DOC });
  [out.section, out.table] = await Promise.all([
    call(isolate, "get_sec_filing_section", { document_url: DOC2, section_name: "Item 1A" }),
    call(isolate, "get_sec_filing_table", { document_url: DOC2, table_index: 0 }),
  ]);
  out.missing = await call(isolate, "get_sec_filing_outline", { document_url: MISSING });
  out.missingAgain = await call(isolate, "get_sec_filing_outline", { document_url: MISSING });
  out.health = (await (await isolate.fetch("https://x/health")).json()).secDocumentCache;
  return out;
}
const first = await run(await mf.getWorker("first"));
const afterFirst = { doc: counts[path(DOC)], doc2: counts[path(DOC2)], missing: counts[path(MISSING)] };
const second = await run(await mf.getWorker("second"));
const afterSecond = { doc: counts[path(DOC)], doc2: counts[path(DOC2)], missing: counts[path(MISSING)] };
await mf.dispose();
console.log(JSON.stringify({ first, second, afterFirst, afterSecond }));
"""


@functools.cache
def _run_harness() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists() or not MINIFLARE.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    # workerd only loads scripts below the working directory.
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".sec-cache-test-") as tmp:
        entry = Path(tmp) / "entry.ts"
        bundle = Path(tmp) / "index.js"
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        entry.write_text(f'export {{ default }} from "{(WORKER / "src" / "index.ts").as_posix()}";\n', encoding="utf-8")
        subprocess.run(
            [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
             "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
        result = subprocess.run(
            [node, str(harness), MINIFLARE.as_uri(), str(bundle.relative_to(WORKER))],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=180,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


def _parse(header: str) -> dict[str, int]:
    return {key: int(value) for key, value in (part.split("=") for part in header.split(", "))}


class TestWorkerSecDocumentCache(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.result = _run_harness()

    def test_document_tools_succeed(self) -> None:
        for isolate in ("first", "second"):
            for call in ("outline", "tables"):
                with self.subTest(isolate=isolate, call=call):
                    self.assertTrue(self.result[isolate][call]["ok"])

    def test_each_document_is_fetched_once_per_data_center(self) -> None:
        # Outline then table list in one isolate: one SEC request; the second
        # isolate reads the edge cache.
        self.assertEqual(self.result["afterFirst"]["doc"], 1)
        self.assertEqual(self.result["afterSecond"]["doc"], 1)
        self.assertEqual(self.result["afterSecond"]["doc2"], 1)

    def test_concurrent_reads_share_one_request(self) -> None:
        self.assertEqual(self.result["afterFirst"]["doc2"], 1)
        section, table = _parse(self.result["first"]["section"]["secCache"]), _parse(self.result["first"]["table"]["secCache"])
        self.assertEqual(sorted([section["upstream"], table["upstream"]]), [0, 1])
        self.assertEqual(sorted([section["shared"], table["shared"]]), [0, 1])

    def test_failed_fetches_are_not_cached(self) -> None:
        self.assertFalse(self.result["first"]["missing"]["ok"])
        self.assertEqual(self.result["afterFirst"]["missing"], 2)
        self.assertEqual(self.result["afterSecond"]["missing"], 4)

    def test_sec_cache_header(self) -> None:
        first = _parse(self.result["first"]["outline"]["secCache"])
        self.assertEqual((first["edge-miss"], first["upstream"], first["edge-write"]), (1, 1, 1))
        self.assertEqual(_parse(self.result["first"]["tables"]["secCache"])["memory"], 1)
        second = _parse(self.result["second"]["outline"]["secCache"])
        self.assertEqual((second["edge-hit"], second["upstream"]), (1, 0))
        self.assertEqual(_parse(self.result["second"]["outline"]["yahooCache"])["upstream"], 0)

    def test_health_reports_sec_document_counters(self) -> None:
        first, second = self.result["first"]["health"], self.result["second"]["health"]
        self.assertEqual(first["scope"], "isolate")
        self.assertEqual((first["edgeWrites"], first["upstreamFetches"]), (2, 4))
        self.assertEqual((second["edgeHits"], second["edgeWrites"]), (2, 0))
        self.assertGreater(second["bodyCacheChars"], 0)


class TestMetaCacheSource(unittest.TestCase):
    """meta.cacheHit and meta.cacheSource describe each tool call's own reads."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.result = _run_harness()

    def _meta(self, isolate: str, call: str) -> tuple[bool, str | None]:
        meta = self.result[isolate][call]["meta"]
        return meta["cacheHit"], meta["cacheSource"]

    def test_provider_fetch_is_not_a_cache_hit(self) -> None:
        self.assertEqual(self._meta("first", "outline"), (False, "upstream"))

    def test_memory_and_edge_reads_are_cache_hits(self) -> None:
        self.assertEqual(self._meta("first", "tables"), (True, "memory"))
        self.assertEqual(self._meta("second", "outline"), (True, "edge"))
        self.assertEqual(self._meta("second", "tables"), (True, "memory"))

    def test_shared_in_flight_read_counts_as_memory(self) -> None:
        sources = sorted([self._meta("first", "section"), self._meta("first", "table")], key=str)
        self.assertEqual(sources, [(False, "upstream"), (True, "memory")])

    def test_failures_carry_the_summary_too(self) -> None:
        self.assertEqual(self._meta("first", "missing"), (False, "upstream"))


if __name__ == "__main__":
    unittest.main()
