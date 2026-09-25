#!/usr/bin/env python3
"""search_sec_filing_text: display-text search, the same in both runtimes (2.2.7).

The 2.2.6 search matched substrings of the raw HTML: "AI" hit hidden inline
XBRL data and the inside of words, a straight apostrophe missed the filing's
curly one, the first term filled all ten slots, context stopped at tags, and
a second term near the first was dropped. Filings are now projected once to
the text a reader sees and searched there.

Part one runs worker/src/filing-search.ts (bundled with esbuild) and
yfmcp/filing_search.py on the same fixtures and requires identical output.
Part two drives the whole tool in both runtimes against a mocked SEC: the
Worker in Miniflare, the local server with its EDGAR helpers patched.
"""

from __future__ import annotations

import asyncio
import copy
import functools
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
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
MINIFLARE = WORKER / "node_modules" / "miniflare" / "dist" / "src" / "index.js"
sys.path.insert(0, str(ROOT))

from yfmcp import filing_search as fs  # noqa: E402

# An inline-XBRL 10-K: hidden ix:header data, a linked table of contents,
# bold-span headings with quoted font names, curly apostrophes, a
# non-breaking hyphen, and a segment table with "$" and "%" in own cells.
IXBRL_10K = """<html><head><title>fstc-20250927</title><style>.x{font-weight:700}</style></head><body>
<div style="display:none"><ix:header><ix:hidden><ix:nonNumeric name="dei:DocumentType">10-K</ix:nonNumeric><ix:nonNumeric name="x">fstc:WidgetMember AI tariff</ix:nonNumeric></ix:hidden></ix:header></div>
<table><tr><td><a href="#i1"><span style="font-weight:700">Item 1.</span></a></td><td><a href="#i1">Business</a></td><td>1</td></tr>
<tr><td><a href="#i1a"><span style="font-weight:700">Item 1A.</span></a></td><td><a href="#i1a">Risk Factors</a></td><td>5</td></tr></table>
<div><span style="font-weight:700">PART I</span></div>
<div id="i1"><span style="font-family:'Helvetica',sans-serif;font-weight:700">Item 1.&#160;&#160;&#160;&#160;Business</span></div>
<div><span style="font-weight:700">Widgets</span></div>
<div><span>Widgets are the Company&#8217;s line of devices. The Widget line includes Widget 17 Pro and Widget Air.</span></div>
<div id="i1a"><span style="font-weight:700">Item 1A.&#160;&#160;&#160;&#160;Risk Factors</span></div>
<div><span>The Company&#8217;s products depend on supply&#8209;chain partners in China mainland, India and Vietnam. Restrictions on international trade, such as tariffs and other controls on imports, can materially adversely affect the Company&#8217;s business. Competitors in China are aggressive. The Company maintains certain AI features in the U.S. and elsewhere.</span></div>
<div><span>Beginning in 2025, new tariffs were announced on imports to the U.S. from China, India and Vietnam.</span></div>
<div><span style="font-weight:700">PART II</span></div>
<div><span style="font-weight:700">Item 7.&#160;&#160;&#160;&#160;Management&#8217;s Discussion and Analysis of Financial Condition and Results of Operations</span></div>
<div><span>Net sales by reportable segment (dollars in millions):</span></div>
<table><tr><td></td><td colspan="3"><span>2025</span></td><td colspan="3"><span>Change</span></td></tr>
<tr><td><span>Greater China</span></td><td><span>$</span></td><td><span>64,377</span></td><td><span>(4</span></td><td><span>)%</span></td></tr>
<tr><td><span>Japan</span></td><td></td><td><span>28,703</span></td><td><span>15</span></td><td><span>%</span></td></tr></table>
<div><span>Vision Pro net sales declined during 2025 compared to 2024.</span></div>
<div><span style="font-weight:700">Item 8.&#160;&#160;&#160;&#160;Financial Statements and Supplementary Data</span></div>
<div><span>Note 13 &#8211; Segment Information. The Company manages its business primarily on a geographic basis, including Greater China.</span></div>
</body></html>"""

# An older filing with <h2> headings and a display:none paragraph.
HTAG_10K = """<html><body>
<h2>PART I</h2>
<h2>Item 1. Business</h2><p>We sell widgets &amp; gadgets worldwide.</p>
<h2>Item 1A. Risk Factors</h2><p>Our customers include distributors. One customer accounted for 24% of net sales.</p>
<p style="display:none">hidden customer text</p>
<h2>Item 7. Management's Discussion and Analysis</h2><p>Revenue grew because customers bought more widgets.</p>
</body></html>"""

# Text before Part I: the cover and forward-looking statements (2.2.8).
PREAMBLE_10K = """<html><body>
<p>We expect results to vary.</p>
<p>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</p>
<p>FORM 10-K</p>
<p><b>Forward-Looking Statements</b></p>
<p>This report contains forward-looking statements about tariffs and supply chains.</p>
<p><span style="font-weight:700">PART I</span></p>
<p><span style="font-weight:700">Item 1. Business</span></p><p>The Company sells widgets despite tariffs.</p>
</body></html>"""

FIXTURES = {"ixbrl": IXBRL_10K, "htag": HTAG_10K, "preamble": PREAMBLE_10K}

# (name, fixture, spec) — spec: terms, query, exclude, mode, budget, hint.
SCENARIOS = [
    ("ai", "ixbrl", {"terms": ["AI"]}),
    ("apostrophe", "ixbrl", {"terms": ["Company's products"]}),
    ("two_terms", "ixbrl", {"terms": ["Widget", "Vision Pro"]}),
    ("hyphen", "ixbrl", {"terms": ["supply chain"]}),
    ("near", "ixbrl", {"query": "China NEAR/5 partners"}),
    ("exclude", "ixbrl", {"query": "China -competitors", "budget": 300}),
    ("table", "ixbrl", {"terms": ["Greater China"]}),
    ("partial_word", "ixbrl", {"terms": ["tari"]}),
    ("substring", "ixbrl", {"terms": ["tari"], "mode": "substring"}),
    ("hint_item", "ixbrl", {"terms": ["China"], "hint": "Item 1A"}),
    ("hint_title", "ixbrl", {"terms": ["China"], "hint": "Risk Factors"}),
    ("exact", "ixbrl", {"query": '"tariff"'}),
    ("htag", "htag", {"terms": ["customer"]}),
    ("preamble", "preamble", {"terms": ["tariffs", "results"]}),
]

_WORKER_PURE = r"""
import fs from "node:fs";
const [bundleUrl, fixturesPath, scenariosPath] = process.argv.slice(-3);
const m = await import(bundleUrl);
const fixtures = JSON.parse(fs.readFileSync(fixturesPath, "utf8"));
const scenarios = JSON.parse(fs.readFileSync(scenariosPath, "utf8"));
const out = { projections: {}, scenarios: {} };
const projections = {};
for (const [name, html] of Object.entries(fixtures)) {
  const p = m.projectFilingText(html, m.searchHeadings(html), m.filingTableSpans(html));
  projections[name] = p;
  out.projections[name] = { text: p.text, sections: p.sections, tables: p.tables.map((t) => [t.tableIndex, t.textStart, t.textEnd]) };
}
for (const [name, fixture, spec] of scenarios) {
  const p = projections[fixture];
  const parsed = spec.query ? m.parseSearchQuery(spec.query) : { terms: [], near: [], exclude: [] };
  let scopeStart = 0, scopeEnd = p.text.length, scope = null;
  if (spec.hint) {
    scope = m.sectionHintScope(p, spec.hint, (h, t) => m.sectionHeadingMatches(h, t) || t.toLowerCase().includes(h.toLowerCase()));
    if (scope) { scopeStart = scope.start; scopeEnd = scope.end; }
  }
  const doc = { key: "d", documentType: "primary", defaultSection: null, projection: p, scopeStart, scopeEnd };
  const result = m.searchDocument(doc, {
    terms: [...m.termsFromList(spec.terms ?? []), ...parsed.terms], near: parsed.near, exclude: parsed.exclude,
    mode: spec.mode ?? "word", budget: spec.budget ?? 1500,
  });
  out.scenarios[name] = {
    scope: scope ? scope.title : null,
    termHits: Object.fromEntries([...result.termHits].map(([k, v]) => [k, v.count])),
    excluded: result.excludedCount,
    matches: m.orderMatches(result.matches, "relevance", new Map([["d", 0]])).map((x) => m.matchPayload(x, false)),
    hitsBySection: m.hitsBySection(result.matches),
  };
}
console.log(JSON.stringify(out));
"""


def _node() -> str:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    return node


@functools.cache
def _worker_pure() -> dict:
    node = _node()
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        entry = WORKER / ".filing-search-test-entry.ts"
        bundle = tmp_path / "bundle.mjs"
        entry.write_text(
            'export * from "./src/filing-search.ts";\n'
            'export { searchHeadings, sectionHeadingMatches } from "./src/yahoo-finance.ts";\n',
            encoding="utf-8",
        )
        try:
            subprocess.run(
                [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
                 "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
                cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
            )
        finally:
            entry.unlink(missing_ok=True)
        (tmp_path / "fixtures.json").write_text(json.dumps(FIXTURES), encoding="utf-8")
        (tmp_path / "scenarios.json").write_text(json.dumps(SCENARIOS), encoding="utf-8")
        (tmp_path / "harness.mjs").write_text(_WORKER_PURE, encoding="utf-8")
        result = subprocess.run(
            [node, str(tmp_path / "harness.mjs"), bundle.as_uri(), str(tmp_path / "fixtures.json"), str(tmp_path / "scenarios.json")],
            check=True, capture_output=True, text=True, timeout=120,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


def _python_pure() -> dict:
    out: dict = {"projections": {}, "scenarios": {}}
    projections = {}
    for name, html in FIXTURES.items():
        p = fs.project_filing_text(html, fs.search_headings(html), fs.filing_table_spans(html))
        projections[name] = p
        out["projections"][name] = {
            "text": p.text,
            "sections": [{"title": s.title, "level": s.level, "textStart": s.text_start} for s in p.sections],
            "tables": [[t.table_index, t.text_start, t.text_end] for t in p.tables],
        }
    for name, fixture, spec in SCENARIOS:
        p = projections[fixture]
        parsed = fs.parse_search_query(spec["query"]) if spec.get("query") else fs.ParsedQuery()
        scope = fs.section_hint_scope(p, spec["hint"]) if spec.get("hint") else None
        doc = fs.SearchDocument("d", "primary", None, p, scope[0] if scope else 0, scope[1] if scope else len(p.text))
        result = fs.search_document(doc, fs.SearchSpec(
            [*fs.terms_from_list(spec.get("terms", [])), *parsed.terms], parsed.near, parsed.exclude,
            spec.get("mode", "word"), spec.get("budget", 1500),
        ))
        out["scenarios"][name] = {
            "scope": scope[2] if scope else None,
            "termHits": {k: v[0] for k, v in result.term_hits.items()},
            "excluded": result.excluded_count,
            "matches": [fs.match_payload(m) for m in fs.order_matches(result.matches, "relevance", {"d": 0})],
            "hitsBySection": fs.hits_by_section(result.matches),
        }
    return out


class TestFilingSearchParity(unittest.TestCase):
    """Both runtimes project and search the same HTML identically."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.worker = _worker_pure()
        cls.local = _python_pure()

    def test_projections_match(self) -> None:
        for name in FIXTURES:
            with self.subTest(fixture=name):
                self.assertEqual(self.worker["projections"][name], self.local["projections"][name])

    def test_scenarios_match(self) -> None:
        for name, _fixture, _spec in SCENARIOS:
            with self.subTest(scenario=name):
                self.assertEqual(self.worker["scenarios"][name], self.local["scenarios"][name])


class TestFilingSearchBehaviour(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_pure()

    def scenario(self, name: str) -> dict:
        return self.out["scenarios"][name]

    def test_projection_is_the_visible_text(self) -> None:
        p = self.out["projections"]["ixbrl"]
        for hidden in ("fstc:WidgetMember", "Helvetica", "font-weight", "fstc-20250927"):
            self.assertNotIn(hidden, p["text"])
        self.assertIn("The Company’s products depend on supply‑chain partners", p["text"])
        self.assertIn("\nGreater China | $64,377 | (4)%\n", p["text"])
        self.assertEqual([s["title"] for s in p["sections"]], [
            "PART I", "Item 1. Business", "Item 1A. Risk Factors", "PART II",
            "Item 7. Management’s Discussion and Analysis of Financial Condition and Results of Operations",
            "Item 8. Financial Statements and Supplementary Data",
        ])
        self.assertEqual([t[0] for t in p["tables"]], [0, 1])

    def test_words_not_markup_or_word_parts(self) -> None:
        ai = self.scenario("ai")
        self.assertEqual(ai["termHits"], {"AI": 1})
        self.assertEqual(ai["matches"][0]["sectionHeading"], "Item 1A. Risk Factors")
        self.assertEqual(self.scenario("partial_word")["termHits"], {"tari": 0})
        self.assertEqual(self.scenario("substring")["termHits"], {"tari": 2})

    def test_quotes_and_dashes_are_folded(self) -> None:
        self.assertEqual(self.scenario("apostrophe")["termHits"], {"Company's products": 1})
        self.assertEqual(self.scenario("hyphen")["termHits"], {"supply chain": 1})

    def test_plurals_unless_exact(self) -> None:
        self.assertEqual(self.scenario("exact")["termHits"], {'"tariff"': 0})
        self.assertEqual(self.scenario("substring")["matches"][0]["terms"], ["tari"])

    def test_every_term_is_represented(self) -> None:
        matches = self.scenario("two_terms")["matches"]
        self.assertEqual([m["term"] for m in matches[:2]], ["Widget", "Vision Pro"])

    def test_heading_line_takes_the_following_paragraph(self) -> None:
        first = self.scenario("two_terms")["matches"][0]
        self.assertTrue(first["contextText"].startswith("Widgets Widgets are the Company’s line of devices."))
        self.assertEqual(first["hitCount"], 5)  # the heading and four mentions below it

    def test_near_and_exclusion(self) -> None:
        near = self.scenario("near")
        self.assertEqual(near["termHits"], {"China NEAR/5 partners": 1})
        excluded = self.scenario("exclude")
        self.assertEqual(excluded["excluded"], 1)
        self.assertEqual(excluded["termHits"], {"China": 5})
        texts = [m["contextText"] for m in excluded["matches"]]
        self.assertTrue(any("China mainland" in t for t in texts))
        self.assertFalse(any(t.startswith("Competitors") for t in texts))

    def test_table_rows(self) -> None:
        table = next(m for m in self.scenario("table")["matches"] if m["inTable"])
        self.assertEqual(table["contextText"], "Greater China | $64,377 | (4)%")
        self.assertEqual((table["tableIndex"], table["rowLabel"]), (1, "Greater China"))
        self.assertEqual(table["tableTitle"], "Net sales by reportable segment (dollars in millions):")

    def test_section_hint_scopes_the_search(self) -> None:
        for name in ("hint_item", "hint_title"):
            with self.subTest(hint=name):
                scoped = self.scenario(name)
                self.assertEqual(scoped["scope"], "Item 1A. Risk Factors")
                self.assertEqual(scoped["termHits"], {"China": 3})
                self.assertEqual({m["sectionHeading"] for m in scoped["matches"]}, {"Item 1A. Risk Factors"})

    def test_text_before_part_one_takes_the_heading_above_it(self) -> None:
        labels = {m["contextText"][:20]: m["sectionHeading"] for m in self.scenario("preamble")["matches"]}
        self.assertEqual(labels, {
            "We expect results to": "Front matter",
            "This report contains": "Forward-Looking Statements",
            "The Company sells wi": "Item 1. Business",
        })

    def test_h_tag_headings_and_hidden_paragraphs(self) -> None:
        htag = self.scenario("htag")
        self.assertEqual(htag["termHits"], {"customer": 3})
        self.assertEqual([s["section"] for s in htag["hitsBySection"]], [
            "Item 1A. Risk Factors", "Item 7. Management's Discussion and Analysis",
        ])


# ── End to end ────────────────────────────────────────────────────────────────

CIK = 1234567
ARCHIVE = f"https://www.sec.gov/Archives/edgar/data/{CIK}"
FILINGS = [
    # form, accession, primary document, filing date, report date
    ("8-K", "0001234567-25-000090", "fstc-8k.htm", "2025-11-05", "2025-11-05"),
    ("10-K", "0001234567-25-000079", "fstc-20250927.htm", "2025-10-31", "2025-09-27"),
    ("10-K", "0001234567-24-000070", "fstc-20240928.htm", "2024-11-01", "2024-09-28"),
    ("10-K", "0001234567-23-000060", "fstc-20230930.htm", "2023-11-03", "2023-09-30"),
]
SUBMISSIONS = {"filings": {"recent": {
    "form": [f[0] for f in FILINGS],
    "accessionNumber": [f[1] for f in FILINGS],
    "primaryDocument": [f[2] for f in FILINGS],
    "filingDate": [f[3] for f in FILINGS],
    "reportDate": [f[4] for f in FILINGS],
    "acceptanceDateTime": ["" for _ in FILINGS],
}}}
EX991 = "<html><body><p>FSTC Reports Fourth Quarter Results</p><p>Revenue from Greater China rose 12%. Tariffs reduced gross margin by 50 basis points.</p></body></html>"
EX992 = "<html><body><p>Supplemental Information</p><p>Greater China unit shipments are disclosed quarterly.</p></body></html>"
DOCUMENTS = {
    f"{ARCHIVE}/000123456725000090/fstc-8k.htm": "<html><body><p>Item 2.02 Results of Operations and Financial Condition. See Exhibit 99.1.</p></body></html>",
    f"{ARCHIVE}/000123456725000090/ex991.htm": EX991,
    f"{ARCHIVE}/000123456725000090/ex992.htm": EX992,
    f"{ARCHIVE}/000123456725000079/fstc-20250927.htm": IXBRL_10K,
    f"{ARCHIVE}/000123456724000070/fstc-20240928.htm": IXBRL_10K.replace("tariffs", "duties").replace("tariff", "duty"),
    f"{ARCHIVE}/000123456723000060/fstc-20230930.htm": HTAG_10K,
}
INDEX_8K_URL = f"{ARCHIVE}/000123456725000090/0001234567-25-000090-index.htm"
INDEX_8K = """<html><body><table>
<tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>
<tr><td>1</td><td>8-K</td><td><a href="/Archives/edgar/data/1234567/000123456725000090/fstc-8k.htm">fstc-8k.htm</a></td><td>8-K</td><td>10000</td></tr>
<tr><td>2</td><td>Press release</td><td><a href="/Archives/edgar/data/1234567/000123456725000090/ex991.htm">ex991.htm</a></td><td>EX-99.1</td><td>20000</td></tr>
<tr><td>3</td><td>EX-99.2</td><td><a href="/Archives/edgar/data/1234567/000123456725000090/ex992.htm">ex992.htm</a></td><td>EX-99.2</td><td>5000</td></tr>
</table></body></html>"""

CALLS = {
    "three_years": {"ticker": "FSTC", "search_terms": ["tariff"], "filing_count": 3},
    "since": {"ticker": "FSTC", "search_terms": ["tariff"], "since": "2024-01-01"},
    "exhibits": {"ticker": "FSTC", "filing_type": "8-K", "search_terms": ["Greater China"], "include_exhibits": True},
    "page_one": {"ticker": "FSTC", "search_terms": ["China"], "max_matches": 2},
    "page_two": {"ticker": "FSTC", "search_terms": ["China"], "max_matches": 2, "cursor": "2"},
    "tables": {"ticker": "FSTC", "search_terms": ["Greater China"], "return_tables": True, "order": "document"},
    "structured": {"ticker": "FSTC", "search_terms": ["China"], "exclude_terms": ["competitors"], "near": [{"terms": ["tariffs", "Vietnam"], "within_words": 12}]},
    "hint": {"ticker": "FSTC", "search_query": "China", "section_hint": "Item 1A"},
    "no_terms": {"ticker": "FSTC"},
}

_WORKER_E2E = r"""
const [miniflareUrl, scriptPath, dataPath] = process.argv.slice(-3);
const { Miniflare, convertV4MiniflareOptions } = await import(miniflareUrl);
const fs = await import("node:fs");
const data = JSON.parse(fs.readFileSync(dataPath, "utf8"));
const archiveFetches = [];
async function outbound(req) {
  const u = new URL(req.url);
  const url = `${u.origin}${u.pathname}`;
  if (u.hostname === "www.sec.gov" && u.pathname === "/files/company_tickers.json") {
    return Response.json({ "0": { cik_str: data.cik, ticker: "FSTC", title: "FS Test Co" } });
  }
  if (u.hostname === "data.sec.gov" && u.pathname === `/submissions/CIK${String(data.cik).padStart(10, "0")}.json`) {
    return Response.json(data.submissions);
  }
  if (url === data.indexUrl) return new Response(data.index, { headers: { "content-type": "text/html" } });
  if (data.documents[url] !== undefined) {
    archiveFetches.push(url);
    return new Response(data.documents[url], { headers: { "content-type": "text/html" } });
  }
  return new Response("Not Found", { status: 404 });
}
const mf = new Miniflare(convertV4MiniflareOptions({ workers: [{
  name: "w", modules: true, scriptPath, compatibilityDate: "2026-09-21", compatibilityFlags: ["nodejs_als"],
  bindings: { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true" }, outboundService: outbound,
}] }));
const worker = await mf.getWorker("w");
let id = 0;
const out = {};
for (const [name, params] of Object.entries(data.calls)) {
  const before = archiveFetches.length;
  const res = await worker.fetch("https://x/mcp", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: ++id, method: "tools/call", params: { name: "sec_filings", arguments: { action: "search_sec_filing_text", params } } }),
  });
  const envelope = (await res.json()).result.structuredContent;
  out[name] = { data: envelope.data, error: envelope.error, archiveFetches: archiveFetches.length - before };
}
await mf.dispose();
console.log(JSON.stringify(out));
"""


@functools.cache
def _worker_e2e() -> dict:
    node = _node()
    if not MINIFLARE.exists():
        raise unittest.SkipTest("worker/node_modules (npm ci) is required")
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".filing-search-test-") as tmp:
        tmp_path = Path(tmp)
        entry = tmp_path / "entry.ts"
        bundle = tmp_path / "index.js"
        entry.write_text(f'export {{ default }} from "{(WORKER / "src" / "index.ts").as_posix()}";\n', encoding="utf-8")
        subprocess.run(
            [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
             "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
        (tmp_path / "data.json").write_text(json.dumps({
            "cik": CIK, "submissions": SUBMISSIONS, "documents": DOCUMENTS,
            "indexUrl": INDEX_8K_URL, "index": INDEX_8K, "calls": CALLS,
        }), encoding="utf-8")
        (tmp_path / "harness.mjs").write_text(_WORKER_E2E, encoding="utf-8")
        result = subprocess.run(
            [node, str(tmp_path / "harness.mjs"), MINIFLARE.as_uri(), str(bundle.relative_to(WORKER)), str(tmp_path / "data.json")],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=240,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


def _python_e2e() -> dict:
    import server as srv

    srv._FILING_TEXT_CACHE.clear()
    srv._FILING_TEXT_CACHE_CHARS = 0
    fetches: list[str] = []

    async def fake_html(url: str, max_bytes: int = 5_000_000) -> str | None:
        fetches.append(url)
        return DOCUMENTS.get(url)

    exhibits = [
        {"sequence": "1", "description": "8-K", "document": "fstc-8k.htm", "type": "8-K", "size": "10000"},
        {"sequence": "2", "description": "Press release", "document": "ex991.htm", "type": "EX-99.1", "size": "20000"},
        {"sequence": "3", "description": "EX-99.2", "document": "ex992.htm", "type": "EX-99.2", "size": "5000"},
    ]
    out = {}
    with patch.object(srv, "_get_submissions_for_ticker", AsyncMock(return_value=(f"{CIK:010d}", copy.deepcopy(SUBMISSIONS)))), \
            patch.object(srv, "_edgar_get_html", fake_html), \
            patch.object(srv, "_edgar_list_exhibits_from_index", AsyncMock(return_value=exhibits)):
        for name, params in CALLS.items():
            before = len([u for u in fetches if u in DOCUMENTS])
            raw = json.loads(asyncio.run(srv.search_sec_filing_text(**params)))
            data = raw["data"] if isinstance(raw, dict) and "ok" in raw else raw
            out[name] = {"data": data, "archiveFetches": len([u for u in fetches if u in DOCUMENTS]) - before}
    return out


class TestFilingSearchEndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.worker = _worker_e2e()
        cls.local = _python_e2e()

    def data(self, name: str) -> dict:
        result = self.worker[name]
        self.assertIsNone(result["error"], result["error"])
        return result["data"]

    def test_runtimes_agree(self) -> None:
        for name in CALLS:
            with self.subTest(call=name):
                worker = self.worker[name]["data"]
                local = self.local[name]["data"]
                self.assertEqual(worker, local)

    def test_several_filings(self) -> None:
        data = self.data("three_years")
        self.assertEqual([f["filingDate"] for f in data["filings"]], ["2025-10-31", "2024-11-01", "2023-11-03"])
        self.assertEqual([f["termHits"]["tariff"] for f in data["filings"]], [2, 0, 0])
        self.assertEqual(len(data["documentsSearched"]), 3)
        self.assertTrue(all(m["accessionNumber"] == "0001234567-25-000079" for m in data["matches"]))
        since = self.data("since")
        self.assertEqual([f["filingDate"] for f in since["filings"]], ["2025-10-31", "2024-11-01"])

    def test_exhibits(self) -> None:
        data = self.data("exhibits")
        by_type = {m["documentType"]: m for m in data["matches"]}
        self.assertEqual(sorted(by_type), ["EX-99.1", "EX-99.2"])
        self.assertEqual(by_type["EX-99.1"]["sectionHeading"], "EX-99.1: Press release")
        self.assertIn("Revenue from Greater China rose 12%.", by_type["EX-99.1"]["contextText"])
        # An index description that only repeats the type is not repeated in the label.
        self.assertEqual(by_type["EX-99.2"]["sectionHeading"], "EX-99.2")

    def test_pagination_and_term_stats(self) -> None:
        first, second = self.data("page_one"), self.data("page_two")
        self.assertEqual(first["totalMatches"], 4)
        self.assertEqual(first["termStats"], [{"term": "China", "hitCount": 5, "hitCountCapped": False, "matchCount": 4, "returnedCount": 2}])
        self.assertEqual((first["pagination"]["nextCursor"], second["pagination"]["nextCursor"]), ("2", None))
        offsets = [m["textOffset"] for m in first["matches"] + second["matches"]]
        self.assertEqual(len(set(offsets)), 4)
        self.assertEqual([s["section"] for s in first["hitsBySection"]], [
            "Item 1A. Risk Factors",
            "Item 7. Management’s Discussion and Analysis of Financial Condition and Results of Operations",
            "Item 8. Financial Statements and Supplementary Data",
        ])

    def test_containing_table_only(self) -> None:
        data = self.data("tables")
        table_match = next(m for m in data["matches"] if m["inTable"])
        self.assertEqual(table_match["tableParsed"], [{"tableIndex": 1, "rows": [["", "2025", "Change"], ["Greater China", "$64,377", "(4)%"], ["Japan", "28,703", "15%"]], "rowsTruncated": False}])
        text_match = next(m for m in data["matches"] if not m["inTable"])
        self.assertEqual(text_match["tableParsed"], [])

    def test_structured_near_and_exclude(self) -> None:
        data = self.data("structured")
        self.assertEqual(data["query"]["near"], ["tariffs NEAR/12 Vietnam"])
        self.assertEqual(data["excludedHitCount"], 1)
        stats = {s["term"]: s["hitCount"] for s in data["termStats"]}
        # Both risk paragraphs put tariffs within 12 words of Vietnam.
        self.assertEqual(stats, {"China": 5, "tariffs NEAR/12 Vietnam": 2})

    def test_section_hint(self) -> None:
        data = self.data("hint")
        self.assertEqual(data["sectionScope"], "Item 1A. Risk Factors")
        self.assertEqual({m["sectionHeading"] for m in data["matches"]}, {"Item 1A. Risk Factors"})

    def test_no_terms_reads_nothing(self) -> None:
        data = self.data("no_terms")
        self.assertEqual(data["accessionNumber"], "0001234567-25-000079")
        self.assertEqual(data["warnings"][-1]["code"], "NO_SEARCH_TERMS")
        self.assertEqual((self.worker["no_terms"]["archiveFetches"], self.local["no_terms"]["archiveFetches"]), (0, 0))


if __name__ == "__main__":
    unittest.main()
