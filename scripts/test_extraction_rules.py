#!/usr/bin/env python3
"""Extraction rules and SEC concept selection, the same in both runtimes (2.4.4).

worker/src/extraction-rules.ts and yfmcp/extraction_rules.py hold the text
rules for customer concentration, guidance ranges, reported release metrics
and event query terms; worker/src/sec-facts.ts and yfmcp/sec_facts.py pick
among equivalent XBRL concepts. Fixtures are the live filing text that
exposed each defect: AAOI's 10-K customer disclosures (receivables and
aggregates had been read as customers), ASTS's Q2 2026 release (a $125M
award read as revenue, guidance missed) and ASTS's revenue concept switch
(a 2022 quarter returned as latest).
"""

from __future__ import annotations

import functools
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

from yfmcp import extraction_rules as er  # noqa: E402
from yfmcp import sec_facts as sf  # noqa: E402

AAOI_MATCHES = [
    {"sectionHeading": "Overview", "contextText": "In 2025, our key customer in the CATV market was Digicomm. In 2025, 2024, and 2023, Digicomm accounted for 53.1%, 34.1% and 11.3% of our revenue, respectively, and in 2023, ATX Networks accounted for 15.6% of our revenue. In 2025, our key customer in the internet data center market was Microsoft. In 2025, 2024 and 2023, Microsoft accounted for 28.8%, 43.7% and 46.6% of our revenue, respectively, and in 2024, Oracle accounted for 12.4% of our revenue."},
    {"sectionHeading": "Risks Related to Operating Our Business", "contextText": "We generate much of our revenue from a limited number of customers. For each year ended 2025, 2024 and 2023, our top ten customers represented 96.6%, 95% and 92.7% of our revenue, respectively. In 2025, Digicomm represented 53.1% of our revenue and Microsoft represented 28.8% of our revenue."},
    {"sectionHeading": "Customers", "contextText": "We had two customers that accounted for more than 10% of our revenue in 2025 and three customers that accounted for more than 10% of our revenue in 2024. A distributor represented 10% or more of net sales."},
    {"sectionHeading": "F- 9", "contextText": "The five largest receivable balances for customers represented an aggregate of 94.0% and 95.5% of total accounts receivable at December 31, 2025 and 2024, respectively. As of December 31, 2025, Digicomm represented 71.6% of total accounts receivable and Microsoft represented 13.9% of total accounts receivable."},
]
NEGATION_MATCHES = [
    {"contextText": "Net sales are diversified. No single customer accounted for more than 10% of net sales in 2025."},
    {"contextText": "One customer accounted for 24% of revenue. Customers include distributors."},
]
ASTS_RELEASE = (
    "HIGHLIGHTS o Continued to build out global gateway footprint with nearly 50 gateways in various stages of completion "
    "o Second quarter revenue was $31.5 million, consistent with plans for quarterly revenue ramp during 2026 "
    "o Received multiple awards from the U.S. Government with an aggregate value of over $125 million supporting multiple national-security applications "
    "o On track to achieve full year 2026 revenue guidance of $150.0 million to $200.0 million, supported by additional contract awards from the U.S. Government "
    "o Revenue backlog increased to approximately $1.30 billion in aggregate contracted revenue agreements"
)
AWARD_FIRST = "Received awards with an aggregate value of over $125 million; revenue was $31.5 million."
KEYWORD_FIRST = "The Company expects revenue of between $40 million and $45 million for the third quarter. Gross margin of 38% to 40% is expected."
GUIDANCE_ONLY = "For the fourth quarter we expect revenue to be $500 million."
STEM_WORDS = ["launch", "launches", "launched", "launching", "BlueBirds", "release", "released", "offering", "class", "guidance"]
EVIDENCE = [
    {"id": "finnhub", "confidence": "LOW", "publishedAt": "2026-09-17"},
    {"id": "press", "confidence": "MEDIUM", "publishedAt": "2026-08-05"},
    {"id": "old-press", "confidence": "MEDIUM", "publishedAt": "2026-07-01"},
    {"id": "sec", "confidence": "HIGH", "publishedAt": "2026-08-10"},
]
# ASTS: ExcludingAssessedTax last filed in 2023; IncludingAssessedTax since.
CONCEPTS = [
    {"concept": "RevenueFromContractWithCustomerExcludingAssessedTax", "facts": [
        {"form": "10-Q", "accn": "0000950170-23-063737", "filed": "2023-11-14", "start": "2022-07-01", "end": "2022-09-30", "val": 4168000},
    ]},
    {"concept": "RevenueFromContractWithCustomerIncludingAssessedTax", "facts": [
        {"form": "10-Q", "accn": "0001193125-26-342550", "filed": "2026-08-10", "start": "2026-04-01", "end": "2026-06-30", "val": 31520000},
        {"form": "10-K", "accn": "0001193125-26-100000", "filed": "2026-03-01", "start": "2025-01-01", "end": "2025-12-31", "val": 70000000},
    ]},
    {"concept": "Revenues", "facts": []},
]

_HARNESS = r"""
import fs from "node:fs";
const [rulesUrl, factsUrl, dataPath] = process.argv.slice(-3);
const r = await import(rulesUrl);
const f = await import(factsUrl);
const d = JSON.parse(fs.readFileSync(dataPath, "utf8"));
const out = {
  aaoi: r.customerConcentration(d.aaoi),
  negation: r.customerConcentration(d.negation),
  guidanceAsts: r.guidanceRanges(d.astsRelease),
  guidanceKeyword: r.guidanceRanges(d.keywordFirst),
  revenueAsts: r.reportedTextMetric(d.astsRelease, r.REVENUE_LABEL, r.USD_AMOUNT),
  revenueAwardFirst: r.reportedTextMetric(d.awardFirst, r.REVENUE_LABEL, r.USD_AMOUNT),
  revenueGuidanceOnly: r.reportedTextMetric(d.guidanceOnly, r.REVENUE_LABEL, r.USD_AMOUNT),
  stems: d.stemWords.map(r.stemWord),
  ranked: r.rankEvidence(d.evidence, (e) => e.confidence).map((e) => e.id),
  pickLatest10q: f.pickConceptFacts(d.concepts, "10-Q", null),
  pickLatest10k: f.pickConceptFacts(d.concepts, "10-K", null),
  pickPinned: f.pickConceptFacts(d.concepts, "10-Q", "0000950170-23-063737"),
  pickPinnedMissing: f.pickConceptFacts(d.concepts, "10-Q", "0001193125-26-999999"),
};
console.log(JSON.stringify(out));
"""


def _node() -> str:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    return node


def _data() -> dict:
    return {
        "aaoi": AAOI_MATCHES, "negation": NEGATION_MATCHES, "astsRelease": ASTS_RELEASE, "keywordFirst": KEYWORD_FIRST,
        "awardFirst": AWARD_FIRST, "guidanceOnly": GUIDANCE_ONLY, "stemWords": STEM_WORDS, "evidence": EVIDENCE, "concepts": CONCEPTS,
    }


@functools.cache
def _worker() -> dict:
    node = _node()
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        bundles = []
        for name in ("extraction-rules", "sec-facts"):
            bundle = tmp_path / f"{name}.mjs"
            subprocess.run([str(ESBUILD), str(WORKER / "src" / f"{name}.ts"), "--bundle", "--format=esm", "--platform=neutral",
                            f"--outfile={bundle}", "--log-level=error"], cwd=WORKER, check=True, capture_output=True, text=True, timeout=120)
            bundles.append(bundle.as_uri())
        (tmp_path / "data.json").write_text(json.dumps(_data()), encoding="utf-8")
        (tmp_path / "harness.mjs").write_text(_HARNESS, encoding="utf-8")
        result = subprocess.run([node, str(tmp_path / "harness.mjs"), *bundles, str(tmp_path / "data.json")],
                                check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


def _python() -> dict:
    d = _data()
    return {
        "aaoi": er.customer_concentration(d["aaoi"]),
        "negation": er.customer_concentration(d["negation"]),
        "guidanceAsts": er.guidance_ranges(d["astsRelease"]),
        "guidanceKeyword": er.guidance_ranges(d["keywordFirst"]),
        "revenueAsts": er.reported_text_metric(d["astsRelease"], er.REVENUE_LABEL, er.USD_AMOUNT),
        "revenueAwardFirst": er.reported_text_metric(d["awardFirst"], er.REVENUE_LABEL, er.USD_AMOUNT),
        "revenueGuidanceOnly": er.reported_text_metric(d["guidanceOnly"], er.REVENUE_LABEL, er.USD_AMOUNT),
        "stems": [er.stem_word(w) for w in d["stemWords"]],
        "ranked": [e["id"] for e in er.rank_evidence(d["evidence"], lambda e: e["confidence"])],
        "pickLatest10q": sf.pick_concept_facts(d["concepts"], "10-Q", None),
        "pickLatest10k": sf.pick_concept_facts(d["concepts"], "10-K", None),
        "pickPinned": sf.pick_concept_facts(d["concepts"], "10-Q", "0000950170-23-063737"),
        "pickPinnedMissing": sf.pick_concept_facts(d["concepts"], "10-Q", "0001193125-26-999999"),
    }


class TestRuntimesAgree(unittest.TestCase):
    def test_outputs_match(self) -> None:
        worker, local = _worker(), _python()
        for key in local:
            self.assertEqual(worker[key], local[key], key)


class TestRules(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python()

    def test_aaoi_customers_are_revenue_shares_for_the_latest_year(self) -> None:
        found = [(f["kind"], f["name"], f["valuePct"], f["year"]) for f in self.out["aaoi"]["findings"]]
        # Receivables (94%, 71.6%), prior years and the prior-year-only ATX/Oracle rows are all excluded.
        self.assertEqual(found, [("customer", "Digicomm", 53.1, 2025), ("customer", "Microsoft", 28.8, 2025), ("aggregate", None, 96.6, 2025)])
        self.assertEqual(self.out["aaoi"]["findings"][2]["description"], "our top ten customers")

    def test_negation_and_unnamed_customer(self) -> None:
        n = self.out["negation"]
        self.assertEqual([(f["name"], f["description"], f["valuePct"]) for f in n["findings"]], [(None, "One customer", 24)])
        self.assertIn("No single customer", n["negation"]["sentence"])

    def test_guidance_after_the_metric_word(self) -> None:
        self.assertEqual(self.out["guidanceAsts"]["revenue"], {"excerpt": "revenue guidance of $150.0 million to $200.0 million", "low": "150.0 million", "high": "200.0 million"})
        self.assertEqual((self.out["guidanceKeyword"]["revenue"]["low"], self.out["guidanceKeyword"]["revenue"]["high"]), ("40 million", "45 million"))
        self.assertEqual((self.out["guidanceKeyword"]["grossMargin"]["low"], self.out["guidanceKeyword"]["grossMargin"]["high"]), ("38", "40"))

    def test_reported_revenue_skips_awards_backlog_and_guidance(self) -> None:
        self.assertEqual(self.out["revenueAsts"]["rawValue"], "$31.5 million")
        self.assertIsNone(self.out["revenueAwardFirst"], "a sentence with award wording is never a revenue result")
        self.assertIsNone(self.out["revenueGuidanceOnly"])

    def test_stems_and_ranking(self) -> None:
        self.assertEqual(self.out["stems"], ["launch", "launch", "launch", "launch", "bluebird", "releas", "releas", "offer", "class", "guidanc"])
        self.assertEqual(self.out["ranked"], ["sec", "press", "old-press", "finnhub"])

    def test_newest_concept_wins_and_pins_are_honoured(self) -> None:
        latest = self.out["pickLatest10q"]
        self.assertEqual((latest["concept"], [f["val"] for f in latest["facts"]]), ("RevenueFromContractWithCustomerIncludingAssessedTax", [31520000]))
        self.assertEqual(self.out["pickLatest10k"]["concept"], "RevenueFromContractWithCustomerIncludingAssessedTax")
        self.assertEqual(self.out["pickPinned"]["concept"], "RevenueFromContractWithCustomerExcludingAssessedTax")
        self.assertIsNone(self.out["pickPinnedMissing"])


if __name__ == "__main__":
    unittest.main()
