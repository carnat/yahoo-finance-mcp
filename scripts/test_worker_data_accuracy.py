#!/usr/bin/env python3
"""Worker data-accuracy regressions found by the 2.2.4 functional audit.

Each case is a fixture shaped like the live data that produced a wrong
answer on production:

- 10-Q XBRL facts for the quarter and the year to date share a period end;
  quarterly metrics must use the three-month value (or derive it).
- analyze_credit_health read statement rows by date after the fetcher began
  returning rows by line item, so every value was null.
- Inline-XBRL 10-Ks mark Item headings with bold spans whose style quotes
  font names; the outline, sections and risk factors found none.
- Financial tables put "$" and "%" in separate cells, so values sit in
  different columns per row; segment and geographic extraction failed.
- Table unit scale came from any "billion" in nearby prose.
- Rating changes ignored Yahoo's price targets; the flow window's max pain
  was the strike with the most open interest; news items that only mention
  a company were labelled as issued by it.
- Risk factor excerpts kept the first 240 characters of each search window,
  which rarely held the term, and "tariff" did not match "tariffs".
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"

_ENTRY = """
export {
  selectQuarterFact, getCreditHealth, filingItemHeadings, findSectionBounds, mergeFinancialCells,
  extractSegmentTableFromHtml, extractGeoRevenueFromHtml, tableUnitScale, analystPriceTargetChange,
  computeMaxPainStrike, rankNewsItemsByRelevance, limitCorporateActions, unwrapYahooValues,
  yahooItemIssuer, customerConcentrationFromMatches, readableExposureExcerpt, _stripHtmlTagsIdx,
} from "./src/yahoo-finance.ts";
"""

# A release fragment with the shapes the release readers depend on (2.5.20).
BLOCK_HTML = (
    "<html><head><style>p{x}</style><script>var a=1;</script></head><body><!-- note -->"
    "<p>Fiscal Q4 2026 Highlights</p><div><span>&#8226;</span><span>Revenue of $54.23&nbsp;billion</span></div>"
    "<div>&#8226;GAAP net income of $37.70&#160;billion, or $32.87 per diluted share</div>"
    "<p onclick=\"x()\">FINANC</span><span>IAL Outlook &amp; more</p>"
    "<table><tr><td>GAAP</td></tr><tr><td>Non-GAAP<sup>(1)</sup></td></tr><tr><td>Revenue</td><td>$10,300 - $10,800</td></tr></table>"
    "<h2>Tail&nbsp;&nbsp; heading</h2><br/>last</body></html>"
)

_HARNESS = r"""
const [bundleUrl] = process.argv.slice(-1);
const m = await import(bundleUrl);
const out = {};
const BLOCK_HTML = __BLOCK_HTML__;

// ── Quarterly XBRL facts ─────────────────────────────────────────────────────
const fact = (start, end, val, form, filed) => ({ start, end, val, form, filed });
const q3Revenue = [
  fact("2025-09-28", "2026-06-27", 364357e6, "10-Q", "2026-07-31"),   // nine months
  fact("2026-03-29", "2026-06-27", 102500e6, "10-Q", "2026-07-31"),   // three months
  fact("2025-09-28", "2026-03-28", 261857e6, "10-Q", "2026-05-01"),
];
out.q3Revenue = m.selectQuarterFact(q3Revenue, { minFiled: "2026-07-30", derive: true });
const q3CashFlow = [
  fact("2025-09-28", "2026-06-27", 117000e6, "10-Q", "2026-07-31"),   // year to date only
  fact("2025-09-28", "2026-03-28", 80000e6, "10-Q", "2026-05-01"),
];
out.q3CashFlow = m.selectQuarterFact(q3CashFlow, { minFiled: "2026-07-30", derive: true });
const q4Revenue = [
  fact("2025-07-01", "2026-06-30", 300000e6, "10-K", "2026-07-29"),   // fiscal year
  fact("2025-07-01", "2026-03-31", 220000e6, "10-Q", "2026-04-29"),   // nine months
];
out.q4Revenue = m.selectQuarterFact(q4Revenue, { minFiled: "2026-07-28", derive: true });
out.q4Eps = m.selectQuarterFact([
  fact("2025-07-01", "2026-06-30", 13.5, "10-K", "2026-07-29"),
  fact("2025-07-01", "2026-03-31", 10.0, "10-Q", "2026-04-29"),
], { minFiled: "2026-07-28", derive: false });
out.staleOnly = m.selectQuarterFact(q3Revenue, { minFiled: "2026-08-15", derive: true });

// ── Credit health from line-item timeseries ──────────────────────────────────
const quarters = ["2026-06-30", "2026-03-31", "2025-12-31", "2025-09-30"];
globalThis.fetch = async (req) => {
  const u = new URL(typeof req === "string" ? req : req.url);
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  const types = (u.searchParams.get("type") ?? "").split(",");
  const values = { quarterlyTotalDebt: 1000, quarterlyCashAndCashEquivalents: 400, quarterlyEBITDA: 150,
    quarterlyOperatingIncome: 100, quarterlyInterestExpense: 10, quarterlyReconciledDepreciation: 25 };
  const result = types.filter((t) => t in values).map((t) => ({
    meta: { type: [t] },
    [t]: quarters.map((asOfDate) => ({ asOfDate, reportedValue: { raw: values[t] } })),
  }));
  return Response.json({ timeseries: { result, error: null } });
};
out.credit = JSON.parse(await m.getCreditHealth("TEST"));

// ── Inline-XBRL Item headings ────────────────────────────────────────────────
const bold = (text) => `<div><span style="color:#000000;font-family:'Helvetica',sans-serif;font-size:9pt;font-weight:700;line-height:120%">${text}</span></div>`;
const filing = [
  `<table><tr><td><a href="#i1a"><span>Item 1A.</span></a></td><td><a href="#i1a">Risk Factors</a></td><td>5</td></tr></table>`,
  bold("PART I"),
  bold("Item 1A.&#160;&#160;&#160;&#160;Risk Factors"),
  `<div><span>Tariffs and export controls can adversely affect the Company.</span></div>`,
  bold("Item 1B.&#160;&#160;&#160;&#160;Unresolved Staff Comments"),
  `<div><span>None.</span></div>`,
].join("\n");
out.headings = m.filingItemHeadings(filing).map((h) => ({ level: h.level, title: h.title, token: h.token }));
const bounds = m.findSectionBounds(filing, "Item 1A");
out.section = bounds.startIdx == null ? null : filing.slice(bounds.startIdx, bounds.endIdx);

// ── Financial tables ─────────────────────────────────────────────────────────
const td = (v) => `<td><span>${v}</span></td>`;
const row = (...cells) => `<tr>${cells.map(td).join("")}</tr>`;
const segmentTable = [
  `<div><span>The following table shows net sales by reportable segment for 2025, 2024 and 2023 (dollars in millions):</span></div>`,
  "<table>",
  row("", "2025", "", "Change", "", "2024", "", "Change", "", "2023"),
  row("Americas", "$", "178,353", "7", "%", "$", "167,045", "3", "%", "$", "162,560"),
  row("Europe", "111,032", "10", "%", "101,328", "7", "%", "94,294"),
  row("Greater China", "64,377", "(4)", "%", "66,952", "(8)", "%", "72,559"),
  row("Japan", "28,703", "15", "%", "25,052", "3", "%", "24,257"),
  row("Rest of Asia Pacific", "33,696", "10", "%", "30,658", "4", "%", "29,615"),
  row("Total net sales", "$", "416,161", "6", "%", "$", "391,035", "2", "%", "$", "383,285"),
  "</table>",
].join("");
out.merged = m.mergeFinancialCells(["Americas", "$", "178,353", "7", "%", "", "( 95,699 )"]);
out.unlabeledTotal = m.mergeFinancialCells(["", "$", "455,715", "", "$", "250,000"]);
out.segments = m.extractSegmentTableFromHtml(`<p>Revenue grew in fiscal 2025, reaching $416 billion.</p>${segmentTable}`);
out.geo = m.extractGeoRevenueFromHtml(segmentTable, "Greater China");
// AAOI: an Item 2 properties table (square footage by location) names China
// before the revenue table, whose total row has no label.
const aaoi = [
  `<p>Item 2. Properties. Our principal facilities are:</p><table>`,
  row("Location", "Owned or leased", "Approximate", "Square Footage", "Use"),
  row("Ningbo, China", "Owned", "1,203,740", "Administration, sales, manufacturing"),
  row("Taipei, Taiwan", "Leased", "705,760", "Administration, sales, manufacturing"),
  `</table><p>The following table presents revenue by geographic region (in thousands):</p><table>`,
  row("", "2025", "", "2024"),
  row("United States", "$", "150,000", "$", "120,000"),
  row("China", "262,140", "100,000"),
  row("Other", "43,575", "30,000"),
  row("", "$", "455,715", "$", "250,000"),
  "</table>",
].join("");
out.aaoi = m.extractGeoRevenueFromHtml(aaoi, "China");
// MRVL (2.5.9): "% of Total" beside each value and an unlabeled total row
// without them; the cell index of China's value is the prior year's total there.
const mrvl = [
  `<p>Net revenue by geographic area (in millions):</p><table>`,
  row("", "Three Months Ended"),
  row("", "August 1, 2026", "% of Total", "August 2, 2025", "% of Total"),
  row("Net revenue based on destination of shipment:"),
  row("China", "$", "1,161.5", "42", "%", "$", "583.4", "29", "%"),
  row("Taiwan", "456.8", "17", "%", "541.2", "27", "%"),
  row("Other", "1,121.0", "41", "%", "881.5", "44", "%"),
  row("", "$", "2,739.3", "", "$", "2,006.1"),
  "</table>",
].join("");
out.mrvl = m.extractGeoRevenueFromHtml(mrvl, "China");
// A table whose stated share disagrees with the computed one is not read.
out.mrvlMisread = m.extractGeoRevenueFromHtml(mrvl.replace("42", "58"), "China");
const msftStyle = [
  `<p>Segment revenue and operating income were as follows during the periods presented:</p>`,
  "<table>",
  row("(In millions)", "2026", "2025"),
  row("Revenue"),
  row("Productivity and Business Processes", "$", "130,000", "$", "120,810"),
  row("Intelligent Cloud", "120,000", "105,362"),
  row("More Personal Computing", "60,000", "55,000"),
  row("Total", "$", "310,000", "$", "281,172"),
  row("Operating income"),
  row("Productivity and Business Processes", "$", "70,000", "$", "60,000"),
  "</table>",
].join("");
out.msftSegments = m.extractSegmentTableFromHtml(msftStyle);
// COHR (2.5.20): "($000)" is thousands, the "Revenues" caption row is not the total, and the long-lived
// assets table after a revenue paragraph is not revenue.
const cohrAssets = [
  `<p>Major customers accounted for 20% of consolidated revenue. Geographic information for long-lived assets by country ($000):</p><table>`,
  row("Long-Lived Assets", "June 30, 2026", "2025"),
  row("China", "968,796", "402,960"),
  row("Total", "$", "3,432,316", "$", "2,147,530"),
  "</table>",
].join("");
const cohr = [
  `<p>Geographic information for revenues, by location of the customer's headquarters, were as follows ($000):</p><table>`,
  row("Revenues"),
  row("Year Ended June 30,", "2026", "2025"),
  row("North America", "$", "4,633,696", "$", "3,564,846"),
  row("China", "813,377", "680,110"),
  row("Rest of World", "1,671,108", "1,565,159"),
  row("Total", "$", "7,118,181", "$", "5,810,115"),
  "</table>",
].join("") + cohrAssets;
out.cohrGeo = m.extractGeoRevenueFromHtml(cohr, "China");
out.cohrAssetsOnly = m.extractGeoRevenueFromHtml(cohrAssets, "China");
// VRT (2.5.20): a disaggregation table by region with a Total column and no year column.
const vrt = [
  `<p>Disaggregation of Revenues. The following table disaggregates revenue by business segment and product and service offering (in millions):</p><table>`,
  row("Year Ended December 31, 2025", "Americas", "Asia Pacific", "EMEA", "Total"),
  row("Products", "$", "5,270.1", "1,510.9", "1,426.0", "$", "8,207.0"),
  row("Services & spares", "1,116.2", "508.3", "398.4", "2,022.9"),
  row("Total", "$", "6,386.3", "2,019.2", "1,824.4", "$", "10,229.9"),
  "</table>",
].join("");
out.vrtSegments = m.extractSegmentTableFromHtml(vrt);
// No scale stated anywhere: read in millions but marked as assumed (2.5.21).
out.unscaledSegments = m.extractSegmentTableFromHtml(vrt.replace(" (in millions)", ""));
// The release text both runtimes read (2.5.20): one line per block, inline tags joined, no-break spaces as spaces.
out.blockText = m._stripHtmlTagsIdx(BLOCK_HTML);
out.unitScale = {
  captionMillions: m.tableUnitScale("<table><tr><td>(In millions)</td></tr></table>", "<p>Net sales rose to $416.2 billion.</p>"),
  proseOnly: m.tableUnitScale("<table><tr><td>2025</td></tr></table>", "<p>Net sales rose to $416.2 billion.</p>"),
  sentence: m.tableUnitScale("<table><tr><td>2025</td></tr></table>", "<p>Net sales by category (dollars in millions):</p>"),
};

// ── Analyst targets, options, news, holders ──────────────────────────────────
out.targets = {
  raised: m.analystPriceTargetChange({ currentPriceTarget: 380, priorPriceTarget: 365, priceTargetAction: "Raises" }, "MAINTAIN"),
  announced: m.analystPriceTargetChange({ currentPriceTarget: 280, priorPriceTarget: 0, priceTargetAction: "Announces" }, "DOWNGRADE"),
  none: m.analystPriceTargetChange({ currentPriceTarget: 0, priorPriceTarget: 0, priceTargetAction: "" }, "MAINTAIN"),
};
const calls = [{ strike: 330, openInterest: 100 }, { strike: 335, openInterest: 50 }, { strike: 350, openInterest: 5000 }];
const puts = [{ strike: 330, openInterest: 3000 }, { strike: 335, openInterest: 100 }, { strike: 350, openInterest: 10 }];
out.maxPain = m.computeMaxPainStrike(calls, puts);
out.ranked = m.rankNewsItemsByRelevance([
  { title: "garmin", tickerRelevance: "LOW" }, { title: "apple a", tickerRelevance: "HIGH" }, { title: "apple b", tickerRelevance: "HIGH" },
]).map((item) => item.title);
out.actions = m.limitCorporateActions([
  { Date: "1987-06-16", "Stock Splits": 2 }, { Date: "2024-01-01", Dividends: 1, "Stock Splits": 0 },
  { Date: "2025-01-01", Dividends: 1, "Stock Splits": 0 }, { Date: "2026-01-01", Dividends: 1, "Stock Splits": 0 },
], "", 2).map((r) => r.Date);
out.unwrapped = m.unwrapYahooValues({ maxAge: 1, pctHeld: { raw: 0.08, fmt: "8%" }, reportDate: { raw: 1782777600, fmt: "2026-06-30" }, list: [{ value: { raw: 5, fmt: "5", longFmt: "5" } }] });
const identity = { aliases: ["apple", "apple inc"], companyName: "Apple Inc.", acronyms: [], status: "RESOLVED", exchange: null };
out.issuer = {
  mentioned: m.yahooItemIssuer({ source: "yahoo_finance_press_releases", title: "Qualcomm Announces Renewal of Global Patent License Agreement with Apple", tickers: ["AAPL"] }, identity),
  own: m.yahooItemIssuer({ source: "yahoo_finance_press_releases", title: "Apple announces quarterly dividend", tickers: ["AAPL"] }, identity),
  news: m.yahooItemIssuer({ source: "yahoo_finance_news", title: "Apple announces quarterly dividend", tickers: ["AAPL"] }, identity),
};
out.customers = m.customerConcentrationFromMatches([
  { contextText: "Net sales are diversified. No single customer accounted for more than 10% of net sales in 2025." },
  { contextText: "One customer accounted for 24% of revenue. Customers include distributors." },
], { documentUrl: "u" }, "FY2025");
// AAPL FY2025 risk factor search window: the term sits past the first 240
// characters and appears only in plural form.
const tariffWindow = "rk carriers and other channel partners. The Company has a large, global business with sales outside the U.S. representing a majority of the Company’s total net sales, and the Company believes that it generally benefits from growth in international trade. A significant majority of the Company’s manufacturing is performed in whole or in part by outsourcing partners located primarily in China mainland, India, Japan, South Korea, Taiwan and Vietnam. Restrictions on international trade, such as tariffs and other controls on imports or exports of goods, technology or data, can materially adversely affect the Company’s business and supply chain. The impact can be particularly significant if these restrictive measures apply to countries and regions where the Company derives a significant portion of its revenues.";
out.riskExcerpt = {
  tariff: m.readableExposureExcerpt(tariffWindow, ["tariff"]),
  absent: m.readableExposureExcerpt(tariffWindow, ["semiconductor"]),
  partialWord: m.readableExposureExcerpt(tariffWindow, ["tari"]),
};
console.log(JSON.stringify(out));
"""


def _run() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry = WORKER / ".data-accuracy-test-entry.ts"
        bundle = Path(tmp) / "bundle.mjs"
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS.replace("__BLOCK_HTML__", json.dumps(BLOCK_HTML)), encoding="utf-8")
        entry.write_text(_ENTRY, encoding="utf-8")
        try:
            subprocess.run(
                [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
                 "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
                cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
            )
        finally:
            entry.unlink(missing_ok=True)
        result = subprocess.run([node, str(harness), bundle.as_uri()], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


class TestWorkerDataAccuracy(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _run()

    def test_quarter_uses_three_month_fact_not_year_to_date(self) -> None:
        q3 = self.out["q3Revenue"]
        self.assertEqual((q3["value"], q3["start"], q3["end"], q3["method"]), (102500e6, "2026-03-29", "2026-06-27", "DIRECT_QUARTER"))

    def test_year_to_date_only_concepts_are_differenced(self) -> None:
        cash = self.out["q3CashFlow"]
        self.assertEqual((cash["value"], cash["method"], cash["start"]), (37000e6, "YTD_DIFFERENCE", "2026-03-29"))
        q4 = self.out["q4Revenue"]
        self.assertEqual((q4["value"], q4["method"], q4["form"]), (80000e6, "YTD_DIFFERENCE", "10-K"))

    def test_per_share_amounts_are_never_derived_and_stale_facts_are_ignored(self) -> None:
        self.assertIsNone(self.out["q4Eps"])
        self.assertIsNone(self.out["staleOnly"])

    def test_credit_health_reads_line_item_timeseries(self) -> None:
        credit = self.out["credit"]
        self.assertEqual((credit["totalDebtUsd"], credit["cashUsd"], credit["netDebtUsd"]), (1000, 400, 600))
        self.assertEqual(credit["quarterDate"], "2026-06-30")
        self.assertEqual(credit["ebitUsd"], 400)
        self.assertEqual(credit["operationalEbitdaUsd"], 500)
        self.assertEqual(credit["interestCoverageEbit"], 10)
        self.assertEqual(credit["dataQuality"], "OK")

    def test_bold_item_headings_with_quoted_font_names(self) -> None:
        self.assertEqual(self.out["headings"], [
            {"level": 1, "title": "PART I", "token": "part i"},
            {"level": 2, "title": "Item 1A. Risk Factors", "token": "item 1a"},
            {"level": 2, "title": "Item 1B. Unresolved Staff Comments", "token": "item 1b"},
        ])
        section = self.out["section"]
        self.assertIsNotNone(section)
        self.assertIn("Tariffs and export controls", section)
        self.assertNotIn("None.", section)

    def test_financial_cells_are_merged(self) -> None:
        self.assertEqual(self.out["merged"], ["Americas", "$178,353", "7%", "(95,699)"])
        self.assertEqual(self.out["unlabeledTotal"], ["", "$455,715", "$250,000"])

    def test_segment_table_sums_to_total(self) -> None:
        result = self.out["segments"]["result"]
        self.assertEqual([row["label"] for row in result["segments"]],
                         ["Americas", "Europe", "Greater China", "Japan", "Rest of Asia Pacific"])
        self.assertEqual(result["total"]["value"], 416161)
        self.assertEqual((result["column"], result["unitScale"], result["tableIndex"]), ("2025", "millions", 0))
        msft = self.out["msftSegments"]["result"]
        self.assertEqual([row["value"] for row in msft["segments"]], [130000, 120000, 60000])
        self.assertEqual(msft["total"]["value"], 310000)

    def test_geographic_row_parses_despite_split_cells(self) -> None:
        geo = self.out["geo"]
        self.assertEqual(geo["usd"], 64377e6)
        self.assertEqual(geo["denominator"], 416161e6)
        self.assertEqual(geo["sourceColumns"], ["2025"])

    def test_geographic_value_and_total_share_a_column(self) -> None:
        mrvl = self.out["mrvl"]
        self.assertEqual((mrvl["pct"], mrvl["usd"], mrvl["denominator"], mrvl["statedPct"]), (0.424, 1_161_500_000, 2_739_300_000, 42))
        self.assertEqual(mrvl["sourceRows"][1], ["Total (unlabeled row)", "$2,739.3"])
        self.assertEqual(mrvl["sourceColumns"], ["Three Months Ended August 1, 2026"])
        self.assertIsNone(self.out["mrvlMisread"], "a computed share 16 points from the stated 58% is a misread column")

    def test_geographic_table_must_be_about_revenue(self) -> None:
        aaoi = self.out["aaoi"]
        self.assertEqual((aaoi["usd"], aaoi["denominator"], aaoi["unitScale"]), (262140e3, 455715e3, "thousands"))
        self.assertAlmostEqual(aaoi["pct"], 0.5752, places=4)

    def test_geographic_table_scale_caption_and_asset_tables(self) -> None:
        cohr = self.out["cohrGeo"]
        self.assertEqual((cohr["pct"], cohr["usd"], cohr["denominator"], cohr["unitScale"]), (0.1143, 813_377_000, 7_118_181_000, "thousands"))
        self.assertIsNone(self.out["cohrAssetsOnly"], "long-lived assets by country are not revenue")

    def test_segment_table_without_a_year_column_reads_the_total_column(self) -> None:
        vrt = self.out["vrtSegments"]["result"]
        self.assertEqual([row["value"] for row in vrt["segments"]], [8207.0, 2022.9])
        self.assertEqual((vrt["total"]["value"], vrt["column"], vrt["unitScale"]), (10229.9, "Total", "millions"))
        self.assertEqual(vrt["unitScaleSource"], "STATED_BEFORE_TABLE")
        unscaled = self.out["unscaledSegments"]["result"]
        self.assertEqual((unscaled["unitScale"], unscaled["unitScaleSource"]), ("millions", "ASSUMED_MILLIONS"))

    def test_release_text_matches_python(self) -> None:
        from yfmcp.parsing.html import _strip_html_blocks
        self.assertEqual(self.out["blockText"], _strip_html_blocks(BLOCK_HTML))
        self.assertIn("\u2022Revenue of $54.23 billion", self.out["blockText"].split("\n"))
        self.assertIn("Non-GAAP(1)", self.out["blockText"].split("\n"))

    def test_unit_scale_comes_from_the_unit_statement(self) -> None:
        self.assertEqual(self.out["unitScale"], {"captionMillions": "millions", "proseOnly": "unknown", "sentence": "millions"})

    def test_price_targets_from_yahoo(self) -> None:
        targets = self.out["targets"]
        self.assertEqual(targets["raised"], {"ptFrom": 365, "ptTo": 380, "ptDirection": "RAISED"})
        self.assertEqual(targets["announced"], {"ptFrom": None, "ptTo": 280, "ptDirection": "INITIATED"})
        self.assertEqual(targets["none"], {"ptFrom": None, "ptTo": None, "ptDirection": None})

    def test_max_pain_is_minimum_payout_not_most_open_interest(self) -> None:
        # 350 has the most open interest; payout is smallest at 335.
        self.assertEqual(self.out["maxPain"], 335)

    def test_news_relevance_ranking_and_issuer(self) -> None:
        self.assertEqual(self.out["ranked"], ["apple a", "apple b", "garmin"])
        self.assertEqual(self.out["issuer"], {"mentioned": None, "own": "Apple Inc.", "news": None})

    def test_corporate_actions_keep_splits(self) -> None:
        self.assertEqual(self.out["actions"], ["1987-06-16", "2025-01-01", "2026-01-01"])

    def test_yahoo_values_are_unwrapped(self) -> None:
        self.assertEqual(self.out["unwrapped"], {"pctHeld": 0.08, "reportDate": "2026-06-30", "list": [{"value": 5}]})

    def test_customer_negation_is_not_a_customer(self) -> None:
        customers = self.out["customers"]
        self.assertEqual([c["valuePct"] for c in customers["customers"]], [24])
        self.assertIn("No single customer", customers["negation"]["excerpt"])

    def test_risk_excerpt_is_centered_on_the_term(self) -> None:
        excerpt = self.out["riskExcerpt"]
        self.assertTrue(excerpt["tariff"].startswith("...Restrictions on international trade, such as tariffs"))
        self.assertLessEqual(len(excerpt["tariff"]), 246)
        self.assertEqual((excerpt["absent"], excerpt["partialWord"]), ("", ""))


if __name__ == "__main__":
    unittest.main()
