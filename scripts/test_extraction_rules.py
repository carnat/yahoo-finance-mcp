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
# MU FQ4 2026 EX-99.1 (2.5.20): the highlights bullet has no result verb; the statement table is in millions.
MU_HIGHLIGHTS = (
    "Fourth Quarter Highlights • Revenue of $54.23 billion versus $37.38 billion for the prior year "
    "• GAAP net income of $37.70 billion, or $32.87 per diluted share "
    "• Non-GAAP net income of $38.10 billion, or $33.20 per diluted share "
    "• Operating cash flow of $41.20 billion versus $28.60 billion for the prior year"
)
RELEASE_CASES = [
    {"name": "muRevenue", "metric": "revenue", "text": MU_HIGHLIGHTS},
    {"name": "muEps", "metric": "epsDiluted", "text": MU_HIGHLIGHTS},
    {"name": "tableInMillions", "metric": "revenue", "text": "(in millions, except per share amounts) Quarter ended August 27, 2026 Revenue was $ 54,229."},
    {"name": "twoTableScales", "metric": "revenue", "text": "(in millions) Revenue was $ 54,229. (in thousands) Other income was $ 3,000."},
    {"name": "unscaledAsWritten", "metric": "revenue", "text": "Revenue was $ 1,234 for the period."},
    {"name": "nonGaapRevenue", "metric": "revenue", "text": "Non-GAAP revenue was $5.0 billion."},
    {"name": "adjustedEps", "metric": "epsDiluted", "text": "Adjusted diluted EPS was $1.20."},
    {"name": "plusMinus", "metric": "revenue", "text": "Revenue was $5.0 billion ± $200 million."},
    {"name": "netLossPerShare", "metric": "epsDiluted", "text": "GAAP net loss was $5.0 million, or $0.12 per diluted share."},
    {"name": "lossLabelParen", "metric": "epsDiluted", "text": "Diluted net loss per share was $(0.12)."},
    {"name": "incomeLossOuterParen", "metric": "epsDiluted", "text": "Diluted net income (loss) per share was ($0.40)."},
    {"name": "grossMargin", "metric": "grossMargin", "text": "GAAP gross margin was 56.0%, operating income was $1.2 billion and free cash flow was $800 million."},
    {"name": "operatingIncome", "metric": "operatingIncome", "text": "GAAP gross margin was 56.0%, operating income was $1.2 billion and free cash flow was $800 million."},
    {"name": "freeCashFlow", "metric": "freeCashFlow", "text": "GAAP gross margin was 56.0%, operating income was $1.2 billion and free cash flow was $800 million."},
    {"name": "revenuePerShare", "metric": "revenue", "text": "Revenue was $2.10 per share."},
    {"name": "capexParen", "metric": "capex", "text": "Capital expenditures were $(1,234) million."},
    {"name": "asts", "metric": "revenue", "text": ASTS_RELEASE},
    {"name": "awardFirst", "metric": "revenue", "text": AWARD_FIRST},
    {"name": "guidanceOnly", "metric": "revenue", "text": "For the fourth quarter we expect revenue to be $500 million."},
    # Real release forms (2.5.20 corpus): segments, annual figures, change verbs, attribution, closing quotes.
    {"name": "segmentRevenue", "metric": "revenue", "text": "Gaming revenue was $2.04 billion, down 44% sequentially. Services revenues were $1.4 billion, up 5%."},
    {"name": "annualRevenue", "metric": "revenue", "text": "Revenue of $2.02 billion in 2025, an increase of 37.3% compared to $1.47 billion in 2024."},
    {"name": "annualFiscalRevenue", "metric": "revenue", "text": "Marvell delivered record fiscal 2026 revenue of $8.195 billion, growing 42% year-over-year."},
    {"name": "yearInGap", "metric": "revenue", "text": "Revenue for the fourth quarter of fiscal 2026 was $2.05 billion, with GAAP gross margin of 38.5%."},
    {"name": "changeTo", "metric": "revenue", "text": "Net Sales Increased 5.2% to $11.3 Billion"},
    {"name": "changeBy", "metric": "revenue", "text": "Revenue increased $500 million, or 10%, to $5.5 billion in the quarter."},
    {"name": "attributedElsewhere", "metric": "capex", "text": "The Company generated $14.2 billion in cash from operations and spent $0.5 billion on capital expenditures, resulting in $13.7 billion of free cash flow."},
    {"name": "negativeWord", "metric": "freeCashFlow", "text": "Free cash flow was negative $5 billion for Q1 as Oracle continued to invest."},
    {"name": "closingQuote", "metric": "epsDiluted",
     "text": "\u201cOur Q1 revenue guidance midpoint is $1.25 billion.\u201d Net revenue for the fourth quarter of fiscal year 2026 was $1.01 billion, with GAAP net loss of $7.2 billion, or $84.65 per diluted share."},
]
# SNDK FQ4 2026 EX-99.1 outlook table (2.5.20): a stated table scale and GAAP / Non-GAAP columns.
SNDK_OUTLOOK = (
    "Business Outlook for Fiscal First Quarter of 2027 (in millions, except per share amounts) GAAP Non-GAAP (1) "
    "Revenue $10,300 - $10,800 $10,300 - $10,800 Gross Margin 83.0% - 84.9% 83.0% - 85.0% Operating Expenses $574 - $614 $520 - $540 "
    "Tax Expense (2) N/A 15.0% Diluted Net Income Per Share N/A $44.00 - $46.00 Diluted Shares Outstanding ~ 155 ~ 155"
)
# MU FQ1-27 outlook table (2.5.21): "GAAP(1) Outlook" / "Non-GAAP(2) Outlook" columns, midpoint and tolerance cells, an approximate margin.
MU_OUTLOOK = (
    "Business Outlook The following table presents Micron\u2019s guidance for the first quarter of 2027:\nFQ1-27\nGAAP(1) Outlook\nNon-GAAP(2) Outlook\n"
    "Revenue\n$61.5 billion \u00b1 $1.5 billion $61.5 billion \u00b1 $1.5 billion\nGross margin\nApproximately 85.95% Approximately 86.25%\n"
    "Operating expenses\nApproximately $2.31 billion Approximately $2.06 billion\nDiluted earnings per share\n$37.84 \u00b1 $1.00 $38.15 \u00b1 $1.00\n"
    "Further information regarding Micron\u2019s business outlook is included in the prepared remarks."
)
# BE (2.5.21): a "GAAP to Non-GAAP" sentence before the guidance heading does not make the revenue range non-GAAP.
BE_CLAUSE = (
    "A reconciliation of GAAP to Non-GAAP financial measures is provided at the end of this press release. 2 Guidance Bloom Energy increases "
    "financial guidance for the full-year 2026: \u2022 Revenue: $3.4B - $3.8B \u2022 Non-GAAP Gross Margin: ~34% \u2022 Non-GAAP EPS: $1.85 - $2.25"
)
UNIT_FORMS = "Guidance Bloom Energy increases financial guidance for the full-year 2026: • Revenue: $3.4B - $3.8B • Revenue was $ 1,234 more than last year."
KEYWORD_FIRST = "The Company expects revenue of between $40 million and $45 million for the third quarter. Gross margin of 38% to 40% is expected."
GUIDANCE_ONLY = "For the fourth quarter we expect revenue to be $500 million."
# COHR Q4 FY26 EX-99.1: metric first, forward verb after it, basis after the range (2.5.9).
COHR_OUTLOOK = (
    "Revenue for the fourth quarter of fiscal 2026 was $1.83 billion. GAAP gross margin was 36.1%. "
    "Business Outlook – First Quarter Fiscal 2027 (1) • Revenue for the first quarter of fiscal 2027 is expected to be between $2.2 billion and $2.4 billion. "
    "• Gross margin percentage for the first quarter of fiscal 2027 is expected to be between 39.5% and 41.5% on a non-GAAP basis. "
    "• Total operating expenses for the first quarter of fiscal 2027 are expected to be between $400 million and $420 million on a non-GAAP basis. "
    "• EPS for the first quarter of fiscal 2027 is expected to be between $1.85 and $2.05 on a non-GAAP basis."
)
# MRVL Q2 FY27 EX-99.1: a midpoint and a tolerance, GAAP and non-GAAP (2.5.9).
MRVL_OUTLOOK = (
    "Third Quarter of Fiscal 2027 Financial Outlook \u2022 Net revenue is expected to be $3.150 billion +/- 5%. "
    "\u2022 GAAP gross margin is expected to be 52.9% to 53.9%. \u2022 Non-GAAP gross margin is expected to be 57.5% to 58.5%. "
    "\u2022 Diluted weighted-average shares outstanding are expected to be 921 million. "
    "\u2022 GAAP diluted net income per share is expected to be $0.53 +/- $0.05 per share. "
    "\u2022 Non-GAAP diluted net income per share is expected to be $1.10 +/- $0.05 per share."
)
# VRT Q2 2026 EX-99.1 (2.5.10): guidance only in release tables, and a second EPS range in the same sentence.
VRT_OUTLOOK = (
    "Full Year 2026 Guidance \u2022 Expects full year 2026 net sales of $14,000 million and organic sales growth of 31%, each at the midpoint of guidance. "
    "\u2022 Expects full year 2026 diluted EPS of $5.82 to $5.92 and adjusted diluted EPS of $6.65 to $6.75, a midpoint increase of 72% and 60%. "
    "Second quarter net sales were $3,274 million. Updated Full Year and Third Quarter 2026 Guidance The data center market continues to demonstrate strong momentum. "
    "Third Quarter 2026 Guidance Net sales $3,650M - $3,850M Organic net sales growth (2) 34% - 36% Adjusted operating margin (2) 24.0% - 25.0% "
    "Adjusted diluted EPS (1) $1.77 - $1.83 Full Year 2026 Guidance Net sales $13,800M - $14,200M Adjusted diluted EPS (1) $6.65 - $6.75"
)
# NVDA Q2 FY27: a comma before the tolerance, and a margin tolerance in basis points for both bases.
NVDA_OUTLOOK = (
    "Outlook NVIDIA\u2019s outlook for the third quarter of fiscal 2027 is as follows: \u2022 Revenue is expected to be $108.0 billion, plus or minus 2%. "
    "\u2022 GAAP and non-GAAP gross margins are expected to be 74.0%, plus or minus 50 basis points."
)
# LITE Q4 FY26: outlook bullets under a lead-in, no verb per bullet.
LITE_OUTLOOK = (
    "Business Outlook Lumentum expects the following for the first quarter of fiscal year 2027: \u2022 Net revenue in the range of $1.225 billion to $1.275 billion "
    "\u2022 Non-GAAP operating margin of 39.5% - 40.5% \u2022 Non-GAAP diluted net income per share of $4.05 to $4.35 We have not provided reconciliations."
)
# A results table is not guidance: no guidance heading above it.
RESULTS_TABLE = "Second Quarter 2026 Results Net sales $3,100M - $3,274M reported. Diluted EPS $1.20 - $1.27 in the prior periods."
MIXED_BASIS = "GAAP EPS for the quarter is expected to be between $1.00 and $1.20 and non-GAAP EPS between $1.85 and $2.05."
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

# A 10-Q's own facts (2.4.5): quarter, year to date and the prior-year
# comparative in one accession, plus an instant balance.
FILING_CONCEPTS = [
    {"concept": "Revenues", "facts": []},
    {"concept": "RevenueFromContractWithCustomerIncludingAssessedTax", "facts": [
        {"form": "10-Q", "accn": "0001193125-26-342550", "filed": "2026-08-10", "start": "2026-01-01", "end": "2026-06-30", "val": 46255000},
        {"form": "10-Q", "accn": "0001193125-26-342550", "filed": "2026-08-10", "start": "2026-04-01", "end": "2026-06-30", "val": 31520000},
        {"form": "10-Q", "accn": "0001193125-26-342550", "filed": "2026-08-10", "start": "2025-04-01", "end": "2025-06-30", "val": 1156000},
        {"form": "10-Q", "accn": "0001193125-26-200000", "filed": "2026-05-10", "start": "2026-01-01", "end": "2026-03-31", "val": 14735000},
    ]},
]
# BE FY2025 (2.5.15): both revenue concepts tagged in one 10-K; contract revenue is a part of the total. The
# earlier-listed concept is the smaller. A malformed companyconcept leaves `facts` an object, not a list.
BE_TIE_CONCEPTS = [
    {"concept": "RevenueFromContractWithCustomerExcludingAssessedTax", "facts": [
        {"form": "10-K", "accn": "0001664703-26-000010", "filed": "2026-02-26", "start": "2025-01-01", "end": "2025-12-31", "val": 2001614000},
    ]},
    {"concept": "Revenues", "facts": [
        {"form": "10-K", "accn": "0001664703-26-000010", "filed": "2026-02-26", "start": "2025-01-01", "end": "2025-12-31", "val": 2023994000},
    ]},
]
MALFORMED_CONCEPTS = [
    {"concept": "RevenueFromContractWithCustomerIncludingAssessedTax", "facts": {}},
    *BE_TIE_CONCEPTS,
]
CASH_CONCEPTS = [{"concept": "CashAndCashEquivalentsAtCarryingValue", "facts": [
    {"form": "10-Q", "accn": "0001193125-26-342550", "filed": "2026-08-10", "end": "2025-12-31", "val": 1000},
    {"form": "10-Q", "accn": "0001193125-26-342550", "filed": "2026-08-10", "end": "2026-06-30", "val": 2288253000},
]}]



def _row(start, end, val, filed, fy, fp="FY", form="10-K"):
    return {"start": start, "end": end, "val": val, "accn": f"0000000001-{filed[2:4]}-{filed[5:7]}{filed[8:10]}", "fy": fy, "fp": fp, "form": form, "filed": filed}


# Named fiscal years (2.5.16). BE's 10-K reports each year again as a comparative in the next filing, carrying THAT
# filing's fy: the 2016 revenue arrives with fy 2018 and the 2016 period must still be FY2016.
BE_YEARS = [
    _row("2016-01-01", "2016-12-31", 208540000, "2017-03-01", 2016),
    _row("2018-01-01", "2018-12-31", 785000000, "2019-03-01", 2018),
    _row("2016-01-01", "2016-12-31", 208540000, "2019-03-01", 2018),
    _row("2017-01-01", "2017-12-31", 376000000, "2018-03-01", 2017),
    _row("2017-01-01", "2017-12-31", 376000000, "2019-03-01", 2018),
    _row("2024-01-01", "2024-12-31", 1330000000, "2025-02-27", 2024),
    _row("2024-01-01", "2024-12-31", 1330000000, "2026-02-26", 2025),
    _row("2025-01-01", "2025-12-31", 2001614000, "2026-02-26", 2025),
    _row("2025-01-01", "2025-12-31", 2001614000, "2026-08-14", 2026, "Q2", "10-Q"),
]
# DG: a 52/53-week year ending 2026-01-30 is its fiscal 2025 (the filing's fy), not 2026.
DG_YEAR = [_row("2025-02-01", "2026-01-30", 41000000000, "2026-03-20", 2025), _row("2024-02-03", "2025-01-31", 40600000000, "2025-03-20", 2024)]
# A year ending 2026-01-02 (end less 7 days is 2025-12-26) is 2025 when the row states no year.
JAN_2_YEAR = [{"start": "2025-01-04", "end": "2026-01-02", "val": 5, "filed": "2026-02-10", "form": "10-K"}]
FY_EDGE_ROWS = [
    _row("2025-01-01", "2025-12-31", 1, "2026-02-10", "2025"),
    _row("2024-01-01", "2024-12-31", 2, "2025-02-10", "2027"),
    _row("2023-01-01", "2023-12-31", 3, "2024-02-10", None),
    _row("2022-01-01", "2022-12-31", 4, "2023-02-10", "junk"),
    {"start": "2021-01-01", "end": None, "val": 5, "filed": "2022-02-10", "fy": 2021, "fp": "FY"},
    {"start": "2020-01-01", "val": 6, "filed": "2021-02-10", "fy": 2020, "fp": "FY"},
    _row("2019-01-01", "not-a-date", 7, "2020-02-10", 2019),
    _row("2018-01-01", "2018-12-31", 8, "2019-02-10", 2018, "fy"),
]
Q4_ROW = [_row("2025-10-01", "2025-12-31", 9, "2026-02-10", 2026, "Q4", "10-K")]
FILED_TIES = [
    _row("2025-01-01", "2025-12-31", 10, "2026-02-10", 2025),
    _row("2025-01-01", "2025-12-31", 11, "2026-02-10", 2025),
    _row("2024-01-01", "2024-12-31", 12, "2026-02-10", 2025),
]
FILING_PERIODS = [None, "", "  ", "latest", "LATEST", " Latest ", "FY2025", "fy2025", "fy 2025", "FY 2025", "FY  2025", "2025", " 2025 ",
                  "FY25", "25", "20255", "FY-2025", "2025Q1", "FY2025.0", "junk", "next", "FY0999", "0000"]
FY_CASES = [
    {"name": "be2025", "rows": BE_YEARS, "year": 2025},
    {"name": "be2016", "rows": BE_YEARS, "year": 2016},
    {"name": "be2018", "rows": BE_YEARS, "year": 2018},
    {"name": "be2026", "rows": BE_YEARS, "year": 2026},
    {"name": "be2030", "rows": BE_YEARS, "year": 2030},
    {"name": "dg2025", "rows": DG_YEAR, "year": 2025},
    {"name": "dg2026", "rows": DG_YEAR, "year": 2026},
    {"name": "dg2024", "rows": DG_YEAR, "year": 2024},
    {"name": "jan2_2025", "rows": JAN_2_YEAR, "year": 2025},
    {"name": "jan2_2026", "rows": JAN_2_YEAR, "year": 2026},
    {"name": "q4_2025", "rows": Q4_ROW, "year": 2025},
    {"name": "q4_2026", "rows": Q4_ROW, "year": 2026},
    {"name": "edge2025", "rows": FY_EDGE_ROWS, "year": 2025},
    {"name": "edge2024", "rows": FY_EDGE_ROWS, "year": 2024},
    {"name": "edge2023", "rows": FY_EDGE_ROWS, "year": 2023},
    {"name": "edge2022", "rows": FY_EDGE_ROWS, "year": 2022},
    {"name": "edge2018", "rows": FY_EDGE_ROWS, "year": 2018},
    {"name": "edge2027", "rows": FY_EDGE_ROWS, "year": 2027},
    {"name": "ties2025", "rows": FILED_TIES, "year": 2025},
    {"name": "ties2024", "rows": FILED_TIES, "year": 2024},
    {"name": "empty", "rows": [], "year": 2025},
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
  guidanceCohr: r.guidanceRanges(d.cohrOutlook),
  guidanceMixed: r.guidanceRanges(d.mixedBasis),
  guidanceMrvl: r.guidanceRanges(d.mrvlOutlook),
  guidanceVrt: r.guidanceRanges(d.vrtOutlook),
  guidanceNvda: r.guidanceRanges(d.nvdaOutlook),
  guidanceLite: r.guidanceRanges(d.liteOutlook),
  guidanceResults: r.guidanceRanges(d.resultsTable),
  guidanceSndk: r.guidanceRanges(d.sndkOutlook),
  guidanceUnitForms: r.guidanceRanges(d.unitForms),
  guidanceMu: r.guidanceRanges(d.muOutlook),
  guidanceBe: r.guidanceRanges(d.beClause),
  releaseMetrics: Object.fromEntries(d.releaseCases.map((c) => [c.name, r.releaseTextMetric(c.text, c.metric)])),
  stems: d.stemWords.map(r.stemWord),
  ranked: r.rankEvidence(d.evidence, (e) => e.confidence).map((e) => e.id),
  pickLatest10q: f.pickConceptFacts(d.concepts, "10-Q", null),
  pickLatest10k: f.pickConceptFacts(d.concepts, "10-K", null),
  pickPinned: f.pickConceptFacts(d.concepts, "10-Q", "0000950170-23-063737"),
  pickPinnedMissing: f.pickConceptFacts(d.concepts, "10-Q", "0001193125-26-999999"),
  tieFirst: f.pickConceptFacts(d.beTie, "10-K", null),
  tieFirstExplicit: f.pickConceptFacts(d.beTie, "10-K", null, "first"),
  tieLarger: f.pickConceptFacts(d.beTie, "10-K", null, "larger"),
  tieLargerReversed: f.pickConceptFacts([...d.beTie].reverse(), "10-K", null, "larger"),
  malformedSkipped: f.pickConceptFacts(d.malformed, "10-K", null, "larger"),
  malformedAlone: f.pickConceptFacts([d.malformed[0]], "10-K", null),
  malformedFiling: f.filingFactInAccession(d.malformed, "0001664703-26-000010"),
  filingRevenue: f.filingFactInAccession(d.filingConcepts, "0001193125-26-342550"),
  filingCash: f.filingFactInAccession(d.cashConcepts, "0001193125-26-342550"),
  filingMissing: f.filingFactInAccession(d.filingConcepts, "0001193125-26-999999"),
  periodHelp: f.FILING_PERIOD_HELP,
  periods: d.filingPeriods.map((p) => f.parseFilingPeriod(p)),
  periodsDefault: f.parseFilingPeriod(undefined),
  fiscalYears: Object.fromEntries(d.fyCases.map((c) => [c.name, f.selectFiscalYearRows(c.rows, c.year)])),
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
        "awardFirst": AWARD_FIRST, "guidanceOnly": GUIDANCE_ONLY, "releaseCases": RELEASE_CASES, "sndkOutlook": SNDK_OUTLOOK, "unitForms": UNIT_FORMS, "muOutlook": MU_OUTLOOK, "beClause": BE_CLAUSE, "stemWords": STEM_WORDS, "evidence": EVIDENCE, "concepts": CONCEPTS,
        "filingConcepts": FILING_CONCEPTS, "cashConcepts": CASH_CONCEPTS, "beTie": BE_TIE_CONCEPTS, "malformed": MALFORMED_CONCEPTS,
        "cohrOutlook": COHR_OUTLOOK, "mixedBasis": MIXED_BASIS, "mrvlOutlook": MRVL_OUTLOOK, "vrtOutlook": VRT_OUTLOOK,
        "nvdaOutlook": NVDA_OUTLOOK, "liteOutlook": LITE_OUTLOOK, "resultsTable": RESULTS_TABLE,
        "filingPeriods": FILING_PERIODS, "fyCases": FY_CASES,
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
        "guidanceCohr": er.guidance_ranges(d["cohrOutlook"]),
        "guidanceMixed": er.guidance_ranges(d["mixedBasis"]),
        "guidanceMrvl": er.guidance_ranges(d["mrvlOutlook"]),
        "guidanceVrt": er.guidance_ranges(d["vrtOutlook"]),
        "guidanceNvda": er.guidance_ranges(d["nvdaOutlook"]),
        "guidanceLite": er.guidance_ranges(d["liteOutlook"]),
        "guidanceResults": er.guidance_ranges(d["resultsTable"]),
        "guidanceSndk": er.guidance_ranges(d["sndkOutlook"]),
        "guidanceUnitForms": er.guidance_ranges(d["unitForms"]),
        "guidanceMu": er.guidance_ranges(d["muOutlook"]),
        "guidanceBe": er.guidance_ranges(d["beClause"]),
        "releaseMetrics": {c["name"]: er.release_text_metric(c["text"], c["metric"]) for c in d["releaseCases"]},
        "stems": [er.stem_word(w) for w in d["stemWords"]],
        "ranked": [e["id"] for e in er.rank_evidence(d["evidence"], lambda e: e["confidence"])],
        "pickLatest10q": sf.pick_concept_facts(d["concepts"], "10-Q", None),
        "pickLatest10k": sf.pick_concept_facts(d["concepts"], "10-K", None),
        "pickPinned": sf.pick_concept_facts(d["concepts"], "10-Q", "0000950170-23-063737"),
        "pickPinnedMissing": sf.pick_concept_facts(d["concepts"], "10-Q", "0001193125-26-999999"),
        "tieFirst": sf.pick_concept_facts(d["beTie"], "10-K", None),
        "tieFirstExplicit": sf.pick_concept_facts(d["beTie"], "10-K", None, "first"),
        "tieLarger": sf.pick_concept_facts(d["beTie"], "10-K", None, "larger"),
        "tieLargerReversed": sf.pick_concept_facts(list(reversed(d["beTie"])), "10-K", None, "larger"),
        "malformedSkipped": sf.pick_concept_facts(d["malformed"], "10-K", None, "larger"),
        "malformedAlone": sf.pick_concept_facts([d["malformed"][0]], "10-K", None),
        "malformedFiling": sf.filing_fact_in_accession(d["malformed"], "0001664703-26-000010"),
        "filingRevenue": sf.filing_fact_in_accession(d["filingConcepts"], "0001193125-26-342550"),
        "filingCash": sf.filing_fact_in_accession(d["cashConcepts"], "0001193125-26-342550"),
        "filingMissing": sf.filing_fact_in_accession(d["filingConcepts"], "0001193125-26-999999"),
        "periodHelp": sf.FILING_PERIOD_HELP,
        "periods": [sf.parse_filing_period(p) for p in d["filingPeriods"]],
        "periodsDefault": sf.parse_filing_period(None),
        "fiscalYears": {c["name"]: sf.select_fiscal_year_rows(c["rows"], c["year"]) for c in d["fyCases"]},
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
        self.assertEqual(self.out["guidanceAsts"]["revenue"], {"excerpt": "revenue guidance of $150.0 million to $200.0 million", "low": "150.0 million", "high": "200.0 million", "basis": "NOT_STATED", "statedAs": "RANGE", "alternates": []})
        self.assertEqual((self.out["guidanceKeyword"]["revenue"]["low"], self.out["guidanceKeyword"]["revenue"]["high"]), ("40 million", "45 million"))
        self.assertEqual((self.out["guidanceKeyword"]["grossMargin"]["low"], self.out["guidanceKeyword"]["grossMargin"]["high"]), ("38", "40"))

    def test_metric_first_guidance_and_its_basis(self) -> None:
        cohr = self.out["guidanceCohr"]
        self.assertEqual((cohr["revenue"]["low"], cohr["revenue"]["high"], cohr["revenue"]["basis"]), ("2.2 billion", "2.4 billion", "NOT_STATED"))
        self.assertTrue(cohr["revenue"]["excerpt"].startswith("Revenue for the first quarter of fiscal 2027"), "the reported fourth-quarter revenue is never the range")
        self.assertEqual((cohr["grossMargin"]["low"], cohr["grossMargin"]["high"], cohr["grossMargin"]["basis"]), ("39.5", "41.5", "NON_GAAP"))
        self.assertEqual((cohr["eps"]["low"], cohr["eps"]["high"], cohr["eps"]["basis"]), ("1.85", "2.05", "NON_GAAP"))
        mixed = self.out["guidanceMixed"]["eps"]
        self.assertEqual((mixed["low"], mixed["high"], mixed["basis"]), ("1.00", "1.20", "GAAP"), "the next clause's non-GAAP range does not relabel this one")

    def test_midpoint_plus_minus_and_alternate_basis(self) -> None:
        mrvl = self.out["guidanceMrvl"]
        rev, gm, eps = mrvl["revenue"], mrvl["grossMargin"], mrvl["eps"]
        # $3.150 billion +/- 5%, exactly: never 2.9924999999999997.
        self.assertEqual((rev["low"], rev["high"], rev["statedAs"]), ("2.9925 billion", "3.3075 billion", "MIDPOINT_PLUS_MINUS"))
        self.assertEqual((gm["low"], gm["high"], gm["basis"]), ("52.9", "53.9", "GAAP"))
        self.assertEqual([(a["low"], a["high"], a["basis"]) for a in gm["alternates"]], [("57.5", "58.5", "NON_GAAP")])
        self.assertEqual((eps["low"], eps["high"], eps["basis"], eps["statedAs"]), ("0.48", "0.58", "GAAP", "MIDPOINT_PLUS_MINUS"))
        self.assertEqual([(a["low"], a["high"], a["basis"]) for a in eps["alternates"]], [("1.05", "1.15", "NON_GAAP")])
        # A share count is not a revenue or EPS range.
        self.assertNotIn("921", rev["excerpt"] + eps["excerpt"])

    def test_release_tables_bullets_and_point_tolerances(self) -> None:
        vrt = self.out["guidanceVrt"]
        # VRT (2.5.10): the text states only a revenue midpoint; the table row carries the range.
        self.assertEqual((vrt["revenue"]["low"], vrt["revenue"]["high"], vrt["revenue"]["statedAs"], vrt["revenue"]["basis"]),
                         ("3,650M", "3,850M", "OUTLOOK_ROW", "NOT_STATED"))
        self.assertEqual((vrt["eps"]["low"], vrt["eps"]["high"]), ("5.82", "5.92"))
        self.assertEqual([(a["low"], a["high"], a["basis"]) for a in vrt["eps"]["alternates"]][0], ("6.65", "6.75", "NON_GAAP"),
                         "the adjusted range in the same sentence")
        self.assertNotIn("organic", vrt["revenue"]["excerpt"].lower())
        nvda = self.out["guidanceNvda"]
        self.assertEqual((nvda["revenue"]["low"], nvda["revenue"]["high"]), ("105.84 billion", "110.16 billion"))
        self.assertEqual((nvda["grossMargin"]["low"], nvda["grossMargin"]["high"], nvda["grossMargin"]["basis"]), ("73.5", "74.5", "GAAP_AND_NON_GAAP"))
        lite = self.out["guidanceLite"]
        self.assertEqual((lite["eps"]["low"], lite["eps"]["high"], lite["eps"]["basis"]), ("4.05", "4.35", "NON_GAAP"))
        self.assertIsNone(lite["grossMargin"], "an operating margin is not a gross margin")
        results = self.out["guidanceResults"]
        self.assertEqual((results["revenue"], results["eps"]), (None, None), "a table without a guidance heading is not guidance")

    def test_alternates_share_the_target_period(self) -> None:
        from yfmcp.guidance_history import period_for_excerpt
        text = VRT_OUTLOOK.replace("and adjusted diluted EPS of $6.65 to $6.75", "")
        ranges = er.guidance_ranges(text, lambda at, n: period_for_excerpt(text, at, n).get("label"))
        # Without the resolver the Q3 table row would stand in for the full year's non-GAAP range.
        self.assertEqual([(a["low"], a["high"]) for a in ranges["eps"]["alternates"]], [("6.65", "6.75")])
        nvda_at = NVDA_OUTLOOK.find("gross margins")
        self.assertEqual(period_for_excerpt(NVDA_OUTLOOK, nvda_at, 40)["label"], "Q3 2027", "not the FY2027 a cut look-back reads")

    def test_reported_revenue_skips_awards_backlog_and_guidance(self) -> None:
        m = self.out["releaseMetrics"]
        self.assertEqual((m["asts"]["value"], m["asts"]["rawValue"]), (31_500_000, "$31.5 million"))
        self.assertIsNone(m["awardFirst"], "a sentence with award wording is never a revenue result")
        self.assertIsNone(m["guidanceOnly"])
        self.assertIsNone(m["plusMinus"], "a midpoint ± tolerance is guidance wording")

    def test_outlook_table_scale_and_columns(self) -> None:
        g = self.out["guidanceSndk"]
        # The table's "(in millions" scales unscaled amounts; each column carries its own basis.
        self.assertEqual((g["revenue"]["low"], g["revenue"]["high"], g["revenue"]["basis"]), ("10,300 million", "10,800 million", "GAAP"))
        self.assertEqual([(a["low"], a["basis"]) for a in g["revenue"]["alternates"]], [("10,300 million", "NON_GAAP")])
        self.assertEqual((g["grossMargin"]["low"], g["grossMargin"]["high"], g["grossMargin"]["basis"]), ("83.0", "84.9", "GAAP"))
        self.assertEqual([(a["low"], a["high"], a["basis"]) for a in g["grossMargin"]["alternates"]], [("83.0", "85.0", "NON_GAAP")])
        self.assertEqual((g["eps"]["low"], g["eps"]["high"], g["eps"]["basis"]), ("44.00", "46.00", "NON_GAAP"), "an N/A GAAP cell puts the range in the Non-GAAP column")

    def test_outlook_rows_stated_as_midpoint_tolerance_or_point(self) -> None:
        mu = self.out["guidanceMu"]
        rev, gm, eps = mu["revenue"], mu["grossMargin"], mu["eps"]
        self.assertEqual((rev["low"], rev["high"], rev["basis"], rev["statedAs"]), ("60 billion", "63 billion", "GAAP", "MIDPOINT_PLUS_MINUS"))
        self.assertEqual([(a["low"], a["high"], a["basis"]) for a in rev["alternates"]], [("60 billion", "63 billion", "NON_GAAP")])
        self.assertEqual((gm["low"], gm["high"], gm["basis"], gm["statedAs"]), ("85.95", "85.95", "GAAP", "POINT_ESTIMATE"))
        self.assertEqual([(a["low"], a["high"], a["basis"]) for a in gm["alternates"]], [("86.25", "86.25", "NON_GAAP")])
        self.assertEqual((eps["low"], eps["high"], eps["basis"], eps["statedAs"]), ("36.84", "38.84", "GAAP", "MIDPOINT_PLUS_MINUS"))
        self.assertEqual([(a["low"], a["high"], a["basis"]) for a in eps["alternates"]], [("37.15", "39.15", "NON_GAAP")])

    def test_a_range_takes_its_basis_from_the_clause_holding_its_first_amount(self) -> None:
        be = self.out["guidanceBe"]
        self.assertEqual((be["revenue"]["basis"], be["revenue"]["low"], be["revenue"]["high"], be["revenue"]["alternates"]), ("NOT_STATED", "3.4B", "3.8B", []))
        gm = be["grossMargin"]
        self.assertEqual((gm["low"], gm["high"], gm["basis"], gm["statedAs"], gm["alternates"]), ("34", "34", "NON_GAAP", "POINT_ESTIMATE", []))

    def test_abbreviated_units_and_unit_boundaries(self) -> None:
        g = self.out["guidanceUnitForms"]
        self.assertEqual((g["revenue"]["low"], g["revenue"]["high"]), ("3.4B", "3.8B"))
        self.assertNotIn("1,234", g["revenue"]["excerpt"])

    def test_release_metrics_take_scale_basis_and_gaap_figures(self) -> None:
        m = self.out["releaseMetrics"]
        # MU FQ4 2026: a label-led bullet counts without a result verb; the per-diluted-share form is EPS.
        self.assertEqual((m["muRevenue"]["value"], m["muRevenue"]["scaleBasis"]), (54_230_000_000, "AS_WRITTEN"))
        self.assertEqual((m["muEps"]["value"], m["muEps"]["rawValue"]), (32.87, "$32.87 per diluted share"))
        self.assertEqual((m["tableInMillions"]["value"], m["tableInMillions"]["scaleBasis"]), (54_229_000_000, "RELEASE_TABLE_IN_MILLIONS"))
        self.assertIsNone(m["twoTableScales"], "an unscaled figure is never guessed between two declared scales")
        self.assertEqual((m["unscaledAsWritten"]["value"], m["unscaledAsWritten"]["scaleBasis"]), (1234, None))
        self.assertIsNone(m["nonGaapRevenue"])
        self.assertIsNone(m["adjustedEps"])
        self.assertIsNone(m["revenuePerShare"], "a per-share figure is not a total")
        self.assertEqual([m[k]["value"] for k in ("netLossPerShare", "lossLabelParen", "incomeLossOuterParen")], [-0.12, -0.12, -0.4])
        self.assertEqual((m["grossMargin"]["value"], m["operatingIncome"]["value"], m["freeCashFlow"]["value"]), (56, 1_200_000_000, 800_000_000))
        self.assertEqual(m["capexParen"]["value"], -1_234_000_000)

    def test_release_metrics_refuse_segments_annual_and_attributed_figures(self) -> None:
        m = self.out["releaseMetrics"]
        for name in ("segmentRevenue", "annualRevenue", "annualFiscalRevenue", "changeBy", "attributedElsewhere"):
            self.assertIsNone(m[name], name)
        self.assertEqual(m["yearInGap"]["value"], 2_050_000_000, "a fiscal year inside the quarter phrase is not another figure")
        self.assertEqual(m["changeTo"]["value"], 11_300_000_000)
        self.assertEqual(m["negativeWord"]["value"], -5_000_000_000)
        self.assertEqual(m["closingQuote"]["value"], -84.65, "a quote's closing mark ends its sentence")

    def test_stems_and_ranking(self) -> None:
        self.assertEqual(self.out["stems"], ["launch", "launch", "launch", "launch", "bluebird", "releas", "releas", "offer", "class", "guidanc"])
        self.assertEqual(self.out["ranked"], ["sec", "press", "old-press", "finnhub"])

    def test_newest_concept_wins_and_pins_are_honoured(self) -> None:
        latest = self.out["pickLatest10q"]
        self.assertEqual((latest["concept"], [f["val"] for f in latest["facts"]]), ("RevenueFromContractWithCustomerIncludingAssessedTax", [31520000]))
        self.assertEqual(self.out["pickLatest10k"]["concept"], "RevenueFromContractWithCustomerIncludingAssessedTax")
        self.assertEqual(self.out["pickPinned"]["concept"], "RevenueFromContractWithCustomerExcludingAssessedTax")
        self.assertIsNone(self.out["pickPinnedMissing"])


class TestRevenueTieAndMalformedFacts(unittest.TestCase):
    """2.5.15: BE tags Revenues and contract revenue in one filing, and SEC has served an object where a fact list belongs."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python()

    def test_first_keeps_the_earlier_listed_concept_on_a_tie(self) -> None:
        for key in ("tieFirst", "tieFirstExplicit"):
            picked = self.out[key]
            self.assertEqual((picked["concept"], [f["val"] for f in picked["facts"]]),
                             ("RevenueFromContractWithCustomerExcludingAssessedTax", [2001614000]), key)

    def test_larger_takes_the_bigger_newest_period_on_a_tie(self) -> None:
        for key in ("tieLarger", "tieLargerReversed"):
            picked = self.out[key]
            self.assertEqual((picked["concept"], [f["val"] for f in picked["facts"]]), ("Revenues", [2023994000]), key)

    def test_a_concept_without_a_fact_list_is_skipped(self) -> None:
        self.assertEqual(self.out["malformedSkipped"]["concept"], "Revenues")
        self.assertIsNone(self.out["malformedAlone"])
        self.assertEqual(self.out["malformedFiling"]["concept"], "RevenueFromContractWithCustomerExcludingAssessedTax")
        self.assertEqual(self.out["malformedFiling"]["fact"]["val"], 2001614000)


class TestNamedFiscalYear(unittest.TestCase):
    """2.5.16: period is "latest" or a fiscal year; a year selects the annual period the issuer calls that year."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python()
        cls.years = cls.out["fiscalYears"]

    def _vals(self, name: str) -> list:
        return [f["val"] for f in self.years[name]["rows"]]

    def test_period_parsing(self) -> None:
        latest, year = {"kind": "latest"}, lambda y: {"kind": "fiscalYear", "year": y}
        got = dict(zip(map(repr, FILING_PERIODS), self.out["periods"]))
        self.assertEqual(got, {
            "None": latest, "''": latest, "'  '": latest, "'latest'": latest, "'LATEST'": latest, "' Latest '": latest,
            "'FY2025'": year(2025), "'fy2025'": year(2025), "'fy 2025'": year(2025), "'FY 2025'": year(2025), "'FY  2025'": None,
            "'2025'": year(2025), "' 2025 '": year(2025), "'FY25'": None, "'25'": None, "'20255'": None, "'FY-2025'": None,
            "'2025Q1'": None, "'FY2025.0'": None, "'junk'": None, "'next'": None, "'FY0999'": year(999), "'0000'": year(0),
        })
        self.assertEqual(self.out["periodsDefault"], latest)
        self.assertEqual(self.out["periodHelp"], 'period must be "latest" or a fiscal year ("FY2025" or "2025").')

    def test_a_comparative_carrying_a_later_fy_stays_in_its_own_year(self) -> None:
        # BE: the 2016 revenue arrives again in the FY2018 10-K with fy 2018; "FY2025" once returned it labelled FY2018.
        rows = self.years["be2025"]["rows"]
        self.assertEqual({f["end"] for f in rows}, {"2025-12-31"})
        self.assertEqual([f["filed"] for f in rows], ["2026-08-14", "2026-02-26"])
        self.assertEqual({f["end"] for f in self.years["be2016"]["rows"]}, {"2016-12-31"})
        self.assertEqual([f["filed"] for f in self.years["be2016"]["rows"]], ["2019-03-01", "2017-03-01"])
        self.assertEqual({f["end"] for f in self.years["be2018"]["rows"]}, {"2018-12-31"})
        self.assertEqual({f["end"] for f in self.years["be2025"]["rows"]}, {"2025-12-31"})

    def test_a_miss_lists_the_fiscal_years_found(self) -> None:
        for name in ("be2026", "be2030"):
            self.assertEqual(self.years[name], {"rows": [], "fiscalYears": [2016, 2017, 2018, 2024, 2025]}, name)
        self.assertEqual(self.years["empty"], {"rows": [], "fiscalYears": []})

    def test_a_52_53_week_year_belongs_to_the_year_the_issuer_states(self) -> None:
        self.assertEqual(self._vals("dg2025"), [41000000000])
        self.assertEqual(self._vals("dg2024"), [40600000000])
        self.assertEqual(self.years["dg2026"], {"rows": [], "fiscalYears": [2024, 2025]})

    def test_a_year_ending_in_the_first_week_of_january_belongs_to_the_year_before(self) -> None:
        self.assertEqual(self._vals("jan2_2025"), [5])
        self.assertEqual(self.years["jan2_2026"], {"rows": [], "fiscalYears": [2025]})

    def test_a_fourth_quarter_row_uses_the_year_its_period_ends_in(self) -> None:
        self.assertEqual(self._vals("q4_2025"), [9])
        self.assertEqual(self.years["q4_2026"], {"rows": [], "fiscalYears": [2025]})

    def test_fy_edge_cases(self) -> None:
        # fy as a numeric string counts; a year more than one away from the period end, a missing or non-numeric fy,
        # a fp other than FY, a row without an end string or with an unparseable end are decided by the period end.
        self.assertEqual(self._vals("edge2025"), [1])
        self.assertEqual(self._vals("edge2024"), [2])
        self.assertEqual(self._vals("edge2023"), [3])
        self.assertEqual(self._vals("edge2022"), [4])
        self.assertEqual(self._vals("edge2018"), [8])
        self.assertEqual(self.years["edge2027"], {"rows": [], "fiscalYears": [2018, 2022, 2023, 2024, 2025]})

    def test_ties_keep_the_order_the_rows_came_in(self) -> None:
        self.assertEqual(self._vals("ties2025"), [10, 11])
        self.assertEqual(self._vals("ties2024"), [12])


class TestFilingSnapshot(unittest.TestCase):
    def test_a_10q_gives_its_quarter(self) -> None:
        out = _python()
        rev = out["filingRevenue"]
        self.assertEqual((rev["concept"], rev["fact"]["val"], rev["fact"]["start"]), ("RevenueFromContractWithCustomerIncludingAssessedTax", 31520000, "2026-04-01"))
        self.assertEqual(out["filingCash"]["fact"]["val"], 2288253000)
        self.assertIsNone(out["filingMissing"])

    def test_inline_tags_join_without_a_break(self) -> None:
        from yfmcp import filing_search
        from yfmcp.parsing.html import _strip_html_tags
        html = '<p><span style="a">PART I - FINANC</span><span style="b">IAL INFORMATION</span></p><td>Net</td><td>31.5</td> <b>Item</b> <i>2</i>'
        self.assertEqual(_strip_html_tags(html), "PART I - FINANCIAL INFORMATION Net 31.5 Item 2")
        self.assertEqual(filing_search.strip_html_tags(html), "PART I - FINANCIAL INFORMATION Net 31.5 Item 2")


if __name__ == "__main__":
    unittest.main()
