#!/usr/bin/env python3
"""Guidance history and the operating-driver ledger (2.5.3): Worker vs local parity and semantics.

- Guidance ranges are read per release with their stated target period;
  revisions compare consecutive releases for the same metric and period;
  outcomes compare the later reported XBRL actual; unread releases and
  unread companyfacts are reported as unread, never as withdrawn or
  unreported.
- The driver ledger lists standard XBRL driver series, the filer's own
  tagged KPIs and operating statements with figures, each as disclosed,
  with nothing derived.
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
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

from yfmcp import driver_ledger as dl  # noqa: E402
from yfmcp import fiscal_calendar as fc  # noqa: E402
from yfmcp import guidance_history as gh  # noqa: E402
from yfmcp.evidence import AUTHORITY_BOUNDARY  # noqa: E402

RELEASES = [
    {"filingDate": "2026-08-10", "accessionNumber": "0001-26-000300", "url": "https://www.sec.gov/r3.htm", "status": "READ", "text": (
        "AST SpaceMobile Provides Business Update. The company now expects full year 2026 revenue guidance of $150.0 million to $200.0 million. "
        "The company expects EPS of $0.10 to $0.20 for fiscal 2027. Revenue was $14.7 million in the second quarter of 2026.")},
    {"filingDate": "2025-11-10", "accessionNumber": "0001-25-000100", "url": "https://www.sec.gov/r1.htm", "status": "READ", "text": (
        "Third quarter results. The company is initiating full year 2026 revenue guidance of $150.0 to $250.0 million. "
        "For the first quarter of 2026, the company expects gross margin of 40% to 44%.")},
    {"filingDate": "2026-03-02", "accessionNumber": "0001-26-000200", "url": "https://www.sec.gov/r2.htm", "status": "READ", "text": (
        "Fourth quarter results. The company reaffirms its full year 2026 revenue guidance of $150.0 million to $250.0 million. "
        "For the first quarter of 2026, the company expects gross margin of 41% to 43%.")},
    {"filingDate": "2026-05-11", "accessionNumber": "0001-26-000250", "url": "https://www.sec.gov/r25.htm", "status": "NOT_READ", "text": None},
    {"filingDate": "2025-08-11", "accessionNumber": "0001-25-000050", "url": "https://www.sec.gov/r0.htm", "status": "READ", "text": (
        "Second quarter results. Given the uncertainty, the company is withdrawing its full year 2025 guidance. "
        "The company expects revenue between $5 million and $7 million, subject to launch timing.")},
]


def _fact(start, end, val, form="10-Q", filed="2026-08-10", accn="0001-26-000301"):
    out = {"end": end, "val": val, "form": form, "filed": filed, "accn": accn}
    if start is not None:
        out["start"] = start
    return out


COMPANYFACTS = {"facts": {"us-gaap": {
    "Revenues": {"units": {"USD": [
        _fact("2025-01-01", "2025-12-31", 4_400_000, "10-K", "2026-03-02", "0001-26-000201"),
        _fact("2026-01-01", "2026-03-31", 9_000_000, "10-Q", "2026-05-11", "0001-26-000251"),
        _fact("2026-04-01", "2026-06-30", 14_700_000),
        _fact("2026-01-01", "2026-06-30", 23_700_000),
        # An amended filing of the same quarter wins by filed date.
        _fact("2026-01-01", "2026-03-31", 9_100_000, "10-Q/A", "2026-06-01", "0001-26-000260"),
        # Not a periodic report: ignored.
        _fact("2026-04-01", "2026-06-30", 99, "8-K", "2026-08-11", "0001-26-000302"),
    ]}},
    "GrossProfit": {"units": {"USD": [
        _fact("2026-01-01", "2026-03-31", 3_900_000, "10-Q", "2026-05-11", "0001-26-000251"),
    ]}},
    "PaymentsToAcquirePropertyPlantAndEquipment": {"units": {"USD": [
        _fact("2026-01-01", "2026-03-31", 250_000_000, "10-Q", "2026-05-11", "0001-26-000251"),
        _fact("2026-01-01", "2026-06-30", 530_000_000),
    ]}},
    "RevenueRemainingPerformanceObligation": {"units": {"USD": [
        _fact(None, "2026-06-30", 1_000_000_000),
        # A duration fact under an instant concept is not a balance.
        _fact("2026-01-01", "2026-06-30", 5),
    ]}},
    "EarningsPerShareDiluted": {"units": {"USD/shares": [
        _fact("2025-01-01", "2025-12-31", -1.2, "10-K", "2026-03-02", "0001-26-000201"),
    ]}},
}}}

# SEC-rendered releases: " o " bullets, and a period stated after the range.
BULLET_RELEASES = [
    {"filingDate": "2025-08-11", "accessionNumber": "b1", "url": None, "status": "READ", "text": (
        "Business Update \u2022 Commercial service in the United States and Canada in Q1 2026 o Continued expectations for revenue "
        "of $50.0 million to $75.0 million in the second half 2025, from government and commercial customers \u2022 Completed assembly")},
    {"filingDate": "2025-11-10", "accessionNumber": "b2", "url": None, "status": "READ", "text": (
        "o GAAP revenue of $14.7 million in Q3 of 2025 driven by U.S. Government contract milestones o Company reiterates its "
        "second-half 2025 revenue guidance of $50.0 million to $75.0 million \u2022 Started launch campaign")},
]

# Non-GAAP guidance is never scored against a GAAP actual (2.5.9).
NON_GAAP_RELEASES = [
    {"filingDate": "2025-03-03", "accessionNumber": "n1", "url": None, "status": "READ", "text": (
        "Business Outlook \u2022 EPS for fiscal 2025 is expected to be between $0.10 and $0.20 on a non-GAAP basis. "
        "\u2022 Revenue for fiscal 2025 is expected to be between $4.0 million and $5.0 million.")},
]

FISCAL_FACTS = {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [
    _fact("2024-10-01", "2025-09-30", 1_000_000, "10-K", "2025-11-20"),
    _fact("2025-10-01", "2025-12-31", 300_000, "10-Q", "2026-02-05"),
]}}}}}

INLINE_FACTS = [
    {"name": "asts:NumberOfSatellitesLaunched", "unit": "satellite", "value": 5, "periodStart": "2026-01-01", "periodEnd": "2026-06-30", "dims": {}},
    {"name": "asts:NumberOfSatellitesLaunched", "unit": "satellite", "value": 5, "periodStart": "2026-01-01", "periodEnd": "2026-06-30", "dims": {}},
    {"name": "asts:NumberOfSatellitesLaunched", "unit": "satellite", "value": 0, "periodStart": "2025-01-01", "periodEnd": "2025-06-30", "dims": {}},
    {"name": "asts:MNOAgreementsCount", "unit": "agreement", "value": 50, "periodStart": None, "periodEnd": "2026-06-30", "dims": {}},
    {"name": "asts:PrepaymentsFromPartners", "unit": "USD", "value": 43_500_000, "periodStart": None, "periodEnd": "2026-06-30", "dims": {}},
    {"name": "asts:NumberOfSatellitesLaunched", "unit": "satellite", "value": 3, "periodStart": "2026-01-01", "periodEnd": "2026-06-30",
     "dims": {"srt:ProductOrServiceAxis": "asts:BlockTwoMember"}},
    {"name": "us-gaap:Revenues", "unit": "USD", "value": 14_700_000, "periodStart": "2026-04-01", "periodEnd": "2026-06-30", "dims": {}},
    {"name": "dei:EntityCommonStockSharesOutstanding", "unit": "shares", "value": 300_000_000, "periodStart": None, "periodEnd": "2026-08-01", "dims": {}},
    {"name": "asts:WarrantExerciseDuringPeriodShares", "unit": "shares", "value": 4_823_170, "periodStart": "2026-01-01", "periodEnd": "2026-06-30", "dims": {}},
    {"name": "asts:LineOfCreditUpfrontFeePercentage", "unit": "pure", "value": 0.03, "periodStart": None, "periodEnd": "2026-06-30", "dims": {}},
    {"name": "asts:TextOnly", "unit": None, "value": None, "periodStart": None, "periodEnd": "2026-06-30", "dims": {}},
]

STATEMENTS = [
    {"contextText": ("We launched five BlueBird satellites during the second quarter of 2026. "
                     "We expect to have 45 to 60 satellites in orbit by the end of 2026. "
                     "Revenue was $14.7 million for the quarter. "
                     "We had 1,020 full-time employees as of June 30, 2026. "
                     "Our production facilities in Midland are designed for a capacity of 6 satellites per month."),
     "sectionHeading": None, "documentUrl": "https://www.sec.gov/r3.htm", "filingDate": "2026-08-10", "accessionNumber": "0001-26-000300",
     "source": "EARNINGS_RELEASE"},
    {"contextText": ("deployed in orbit with 4 satellites working. We launched five BlueBird satellites during the second quarter of 2026. "
                     "As of June 30, 2026, we had agreements with approximately 50 mobile network operators customers. "
                     "Our backlog of committed contracts was $1.0 billion. "
                     "In 2025 we signed agreements in Europe. "
                     "We shipped 12 gateway units to partners and the remaining"),
     "sectionHeading": "Item 2. MD&A", "documentUrl": "https://www.sec.gov/q2.htm", "filingDate": "2026-08-11", "accessionNumber": "0001-26-000301",
     "source": "PERIODIC_FILING"},
]

PERIOD_CASES = [
    ["For fiscal 2027, the company expects", None],
    ["full year 2026", None],
    ["FY'27 outlook", None],
    ["2026 full-year outlook", None],
    ["third quarter of fiscal 2026", None],
    ["Q3 2026", None],
    ["Q4", None],
    ["first quarter of fiscal 2026 and Q1 FY2026", None],
    ["2H25 and H1 2026 and first half of fiscal 2027", None],
    ["second-half 2025", None],
    ["Q3 of 2025", None],
    ["no period here", None],
    ["For the first quarter of 2026 guidance of X compared with the first quarter of 2025", 45],
    ["guidance of X for fiscal 2027", 0],
    # 2.5.11: a year named by its end date (AEHR), a stated quarter end, and an impossible date.
    ["For the fiscal year ending June 25, 2027, Aehr expects total company revenue to be between $130 million and $150 million", None],
    ["For the third quarter of fiscal 2027 ending October 30, 2026, revenue guidance of X", None],
    ["For the fiscal year ending January 2, 2027, guidance of X", None],
    ["For the fiscal year ended February 30, 2027, guidance of X", None],
]

# 2.5.11: a 52/53-week year the annual reports state (AEHR-like, ending near May 31), and a calendar
# 52/53-week year ending on January 3 with no stated fiscal year.
def _stated(start, end, val, form, filed, accn, fy, fp):
    return {**_fact(start, end, val, form, filed, accn), "fy": fy, "fp": fp}


STATED_FACTS = {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [
    _stated("2024-06-01", "2025-05-30", 59_000_000, "10-K", "2025-08-01", "a25", 2025, "FY"),
    _stated("2025-05-31", "2026-05-29", 50_000_000, "10-K", "2026-07-28", "a26", 2026, "FY"),
    # The FY2025 year again in the FY2026 10-K: that filing's fy (2026) is not this year's.
    _stated("2024-06-01", "2025-05-30", 59_000_000, "10-K", "2026-07-28", "a26", 2026, "FY"),
    _stated("2025-08-30", "2025-11-28", 9_000_000, "10-Q", "2026-01-08", "q2", 2026, "Q2"),
    _stated("2026-05-30", "2026-08-28", 30_000_000, "10-Q", "2026-10-08", "q1", 2027, "Q1"),
]}}}}}
WEEK53_FACTS = {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [
    _fact("2024-12-29", "2026-01-03", 5_000_000, "10-K", "2026-02-20", "w25"),
    _fact("2024-12-29", "2025-04-05", 1_000_000, "10-Q", "2025-05-05", "w1"),
    _fact("2024-12-29", "2025-07-05", 2_100_000, "10-Q", "2025-08-05", "w2"),
]}}}}}
AEHR_RELEASES = [
    {"filingDate": "2026-07-14", "accessionNumber": "0001-26-000700", "url": "https://www.sec.gov/aehr.htm", "status": "READ", "text": (
        "Financial Guidance: For the fiscal year ending June 25, 2027, Aehr expects total company revenue to be between $130 million and $150 million.")},
    {"filingDate": "2026-06-02", "accessionNumber": "0001-26-000650", "url": "https://www.sec.gov/aehr-q.htm", "status": "READ", "text": (
        "Business update. For the first quarter of fiscal 2027, Aehr expects revenue between $25 million and $35 million.")},
]
# 2.5.13: DG names its years for the year they start in (the year ending January 30, 2026 is its fiscal 2025).
DG_FACTS = {"facts": {"us-gaap": {"NetIncomeLoss": {"units": {"USD": [
    _stated("2023-02-04", "2024-02-02", 1_600_000_000, "10-K", "2024-03-22", "d23", 2023, "FY"),
    _stated("2024-02-03", "2025-01-31", 1_100_000_000, "10-K", "2025-03-21", "d24", 2024, "FY"),
    _stated("2025-02-01", "2026-01-30", 1_300_000_000, "10-K", "2026-03-20", "d25", 2025, "FY"),
    _stated("2025-02-01", "2026-01-30", 1_300_000_000, "10-K", "2026-03-20", "d25", 2025, "FY"),
]}}}}}
MIXED_FACTS = {"facts": {"us-gaap": {"NetIncomeLoss": {"units": {"USD": [
    _stated("2024-02-03", "2025-01-31", 1, "10-K", "2025-03-21", "m24", 2024, "FY"),
    _stated("2025-02-01", "2026-01-30", 1, "10-K", "2026-03-20", "m25", 2026, "FY"),
]}}}}}
DG_RELEASES = [
    {"filingDate": "2026-03-12", "accessionNumber": "0001-26-000800", "url": "https://www.sec.gov/dg.htm", "status": "READ", "text": (
        "Outlook: For the fiscal year ending January 29, 2027, the company expects revenue between $42.0 billion and $43.0 billion.")},
]
FOCUS_HTML = ['<ix:nonNumeric name="dei:DocumentFiscalYearFocus" contextRef="c-1">2025</ix:nonNumeric>',
              '<ix:nonNumeric contextRef="c" name="dei:DocumentFiscalYearFocus" id="f"><span>2026</span></ix:nonNumeric>', "<p>no tag</p>", None]

# 2.5.14: (tagged DocumentFiscalYearFocus, period of report) -> filing label. DG's 10-K, AAPL's December-quarter
# 10-Q, a mistag two years off, untagged, no date.
FILING_LABEL_CASES = [[2025, "2026-01-30"], [2025, "2024-12-28"], [2019, "2026-01-30"], [None, "2026-01-30"], [2025, None], [None, None]]

# 2.5.16: (annual period ends newest first, provider year end). DG, MRVL, FN, LITE with three ends (both rules fit and
# disagree), LITE settled by a fourth, ANET (month end), a provider month the calendar does not have, one end, a changed calendar.
CALENDAR_CASES = [
    [["2026-01-30", "2025-01-31", "2024-02-02"], "2027-01-31"],
    [["2026-01-31", "2025-02-01", "2024-02-03"], "2027-01-31"],
    [["2026-06-26", "2025-06-27", "2024-06-28"], "2027-06-30"],
    [["2026-06-27", "2025-06-28", "2024-06-29"], "2027-06-30"],
    [["2026-06-27", "2025-06-28", "2024-06-29", "2021-07-03"], "2027-06-30"],
    [["2025-12-31", "2024-12-31"], "2026-12-31"],
    [["2026-01-30", "2025-01-31", "2024-02-02"], "2027-06-30"],
    [["2026-01-30"], "2027-01-31"],
    [["2026-06-30", "2025-12-31"], "2027-06-30"],
]


def _calendar_cases_py() -> list:
    return [[fc.fiscal_calendar(ends), fc.settled_fiscal_calendar(ends), fc.company_fiscal_year_end(fc.settled_fiscal_calendar(ends), provider)]
            for ends, provider in CALENDAR_CASES]


FISCAL_DATES = ["2026-01-03", "2026-01-08", "2025-12-27", "2026-05-29", "2027-06-25", None, "bad"]
FISCAL_QUARTERS = [["2025-11-28", "2026-05-29"], ["2025-08-29", "2026-05-29"], ["2026-05-29", "2026-05-29"], ["2025-04-05", "2026-01-03"],
                   ["2025-01-15", "2026-05-29"], ["2025-10-15", "2026-05-29"]]


def _python_outputs() -> dict:
    return {
        "history": gh.guidance_history("asts", RELEASES, COMPANYFACTS),
        "historyUnreadFacts": gh.guidance_history("asts", RELEASES[:3], None),
        "bullets": gh.guidance_history("asts", BULLET_RELEASES, COMPANYFACTS),
        "nonGaap": gh.guidance_history("x", NON_GAAP_RELEASES, COMPANYFACTS),
        "fiscalActuals": gh.actuals_from_company_facts(FISCAL_FACTS),
        "statedActuals": gh.actuals_from_company_facts(STATED_FACTS),
        "week53Actuals": gh.actuals_from_company_facts(WEEK53_FACTS),
        "aehr": gh.guidance_history("aehr", AEHR_RELEASES, STATED_FACTS),
        "fiscal": [[fc.fiscal_year_of_period_end(d), fc.nominal_period_end(d), fc.fiscal_year_label(d)] for d in FISCAL_DATES],
        "fiscalQuarters": [fc.fiscal_quarter_of(a, b) for a, b in FISCAL_QUARTERS],
        "textDates": [fc.text_date("Sept.", "30", "2026"), fc.text_date("Jan", "2", "2027"), fc.text_date("February", "30", "2027"), fc.text_date("Foo", "1", "2026")],
        "naming": [fc.fiscal_year_naming(DG_FACTS), fc.fiscal_year_naming(MIXED_FACTS), fc.fiscal_year_naming(STATED_FACTS), fc.fiscal_year_naming(None),
                   fc.fiscal_year_naming({"facts": {}})],
        "calendars": _calendar_cases_py(),
        "focus": [fc.document_fiscal_year_focus(h) for h in FOCUS_HTML],
        "filingLabels": [fc.filing_fiscal_year_label(f, d) for f, d in FILING_LABEL_CASES],
        "dg": gh.guidance_history("dg", DG_RELEASES, DG_FACTS),
        "periods": [gh.guidance_target_period(c) if a is None else gh.guidance_target_period(c, a) for c, a in PERIOD_CASES],
        "amounts": [gh.parse_amount("150.0 million"), gh.parse_amount("250.0", "million"), gh.parse_amount("1,250"), gh.parse_amount("abc")],
        "ledger": dl.operating_driver_ledger(ticker="asts", companyfacts=COMPANYFACTS, inline_facts=INLINE_FACTS,
                                             filing={"filingType": "10-Q", "status": "READ"}, release={"status": "READ"}, statements=STATEMENTS),
        "ledgerUnread": dl.operating_driver_ledger(ticker="asts", companyfacts=None, inline_facts=None, filing=None, release=None, statements=[]),
        "figures": [dl.figures_in(s) for s in ("Revenue was $14.7 million in 2026.", "We had 1,020 full-time employees.", "In 2025 and 2026 we grew 12%.",
                                                "The 10-Q lists 3 of 4 items.", "Capacity reached 1.5 GW and 300 MW.",
                                                "Orbital launch of BlueBird 8-13 marks six spacecraft, with Block 2 satellites and 45 satellites planned.",
                                                "Revenue: $3.4B - $3.8B")],
    }


_HARNESS = r"""
const [ghUrl, dlUrl, fcUrl, fixturesPath] = process.argv.slice(-4);
const gh = await import(ghUrl);
const dl = await import(dlUrl);
const fc = await import(fcUrl);
const { readFileSync } = await import("node:fs");
const f = JSON.parse(readFileSync(fixturesPath, "utf8"));
const out = {
  history: gh.guidanceHistory("asts", f.releases, f.companyfacts),
  historyUnreadFacts: gh.guidanceHistory("asts", f.releases.slice(0, 3), null),
  bullets: gh.guidanceHistory("asts", f.bulletReleases, f.companyfacts),
  nonGaap: gh.guidanceHistory("x", f.nonGaapReleases, f.companyfacts),
  fiscalActuals: gh.actualsFromCompanyFacts(f.fiscalFacts),
  statedActuals: gh.actualsFromCompanyFacts(f.statedFacts),
  week53Actuals: gh.actualsFromCompanyFacts(f.week53Facts),
  aehr: gh.guidanceHistory("aehr", f.aehrReleases, f.statedFacts),
  fiscal: f.fiscalDates.map((d) => [fc.fiscalYearOfPeriodEnd(d), fc.nominalPeriodEnd(d), fc.fiscalYearLabel(d)]),
  fiscalQuarters: f.fiscalQuarters.map(([a, b]) => fc.fiscalQuarterOf(a, b)),
  textDates: [fc.textDate("Sept.", "30", "2026"), fc.textDate("Jan", "2", "2027"), fc.textDate("February", "30", "2027"), fc.textDate("Foo", "1", "2026")],
  naming: [fc.fiscalYearNaming(f.dgFacts), fc.fiscalYearNaming(f.mixedFacts), fc.fiscalYearNaming(f.statedFacts), fc.fiscalYearNaming(null), fc.fiscalYearNaming({ facts: {} })],
  calendars: f.calendarCases.map(([ends, provider]) => [fc.fiscalCalendar(ends), fc.settledFiscalCalendar(ends), fc.companyFiscalYearEnd(fc.settledFiscalCalendar(ends), provider)]),
  focus: f.focusHtml.map((h) => fc.documentFiscalYearFocus(h)),
  filingLabels: f.filingLabelCases.map(([focus, d]) => fc.filingFiscalYearLabel(focus, d)),
  dg: gh.guidanceHistory("dg", f.dgReleases, f.dgFacts),
  periods: f.periodCases.map(([c, a]) => (a == null ? gh.guidanceTargetPeriod(c) : gh.guidanceTargetPeriod(c, a))),
  amounts: [gh.parseAmount("150.0 million"), gh.parseAmount("250.0", "million"), gh.parseAmount("1,250"), gh.parseAmount("abc")],
  ledger: dl.operatingDriverLedger({ ticker: "asts", companyfacts: f.companyfacts, inlineFacts: f.inlineFacts,
    filing: { filingType: "10-Q", status: "READ" }, release: { status: "READ" }, statements: f.statements }),
  ledgerUnread: dl.operatingDriverLedger({ ticker: "asts", companyfacts: null, inlineFacts: null, filing: null, release: null, statements: [] }),
  figures: ["Revenue was $14.7 million in 2026.", "We had 1,020 full-time employees.", "In 2025 and 2026 we grew 12%.",
    "The 10-Q lists 3 of 4 items.", "Capacity reached 1.5 GW and 300 MW.",
    "Orbital launch of BlueBird 8-13 marks six spacecraft, with Block 2 satellites and 45 satellites planned.",
    "Revenue: $3.4B - $3.8B"].map((s) => dl.figuresIn(s)),
};
console.log(JSON.stringify(out));
"""


def _worker_outputs() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        bundles = []
        for name in ("guidance-history", "driver-ledger", "fiscal-calendar"):
            out = Path(tmp) / f"{name}.mjs"
            subprocess.run([str(ESBUILD), str(WORKER / "src" / f"{name}.ts"), "--bundle", "--format=esm", "--platform=neutral", f"--outfile={out}", "--log-level=error"],
                           cwd=WORKER, check=True, capture_output=True, text=True, timeout=120)
            bundles.append(out.as_uri())
        fx = Path(tmp) / "fixtures.json"
        fx.write_text(json.dumps({"releases": RELEASES, "companyfacts": COMPANYFACTS, "fiscalFacts": FISCAL_FACTS, "inlineFacts": INLINE_FACTS,
                                  "statements": STATEMENTS, "periodCases": PERIOD_CASES, "bulletReleases": BULLET_RELEASES,
                                  "nonGaapReleases": NON_GAAP_RELEASES, "statedFacts": STATED_FACTS, "week53Facts": WEEK53_FACTS,
                                  "aehrReleases": AEHR_RELEASES, "fiscalDates": FISCAL_DATES, "fiscalQuarters": FISCAL_QUARTERS,
                                  "dgFacts": DG_FACTS, "mixedFacts": MIXED_FACTS, "dgReleases": DG_RELEASES, "focusHtml": FOCUS_HTML,
                                  "filingLabelCases": FILING_LABEL_CASES, "calendarCases": CALENDAR_CASES}), encoding="utf-8")
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        result = subprocess.run([node, str(harness), *bundles, str(fx)], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


class TestParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.py = _python_outputs()
        cls.ts = _worker_outputs()

    def test_outputs_match(self) -> None:
        for key in self.py:
            self.assertEqual(json.loads(json.dumps(self.py[key])), self.ts[key], key)


def _revisions(history: dict, metric: str, period: str) -> list[str]:
    return [r["change"] for r in history["revisions"] if r["metric"] == metric and r["targetPeriod"] == period]


def _outcome(history: dict, metric: str, period: str) -> dict:
    return next(o for o in history["outcomes"] if o["metric"] == metric and o["targetPeriod"] == period)


class TestGuidanceHistory(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.h = gh.guidance_history("asts", RELEASES, COMPANYFACTS)

    def test_entries_carry_period_and_source(self) -> None:
        latest = next(g for g in self.h["guidance"] if g["releaseDate"] == "2026-08-10" and g["metric"] == "revenue")
        self.assertEqual((latest["low"], latest["high"], latest["midpoint"]), (150_000_000, 200_000_000, 175_000_000))
        self.assertEqual(latest["targetPeriod"]["label"], "FY2026")
        self.assertEqual(latest["targetPeriod"]["scope"], "SENTENCE")
        self.assertEqual(latest["sourceUrl"], "https://www.sec.gov/r3.htm")
        # "$150.0 to $250.0 million": the stated scale applies to both ends.
        first = next(g for g in self.h["guidance"] if g["releaseDate"] == "2025-11-10" and g["metric"] == "revenue")
        self.assertEqual((first["low"], first["high"]), (150_000_000, 250_000_000))

    def test_period_stated_after_the_range(self) -> None:
        eps = next(g for g in self.h["guidance"] if g["metric"] == "eps")
        # Not the FY2026 of the preceding sentence.
        self.assertEqual(eps["targetPeriod"]["label"], "FY2027")

    def test_revisions(self) -> None:
        self.assertEqual(_revisions(self.h, "revenue", "FY2026"), ["INITIATED", "REAFFIRMED", "LOWERED"])
        self.assertEqual(_revisions(self.h, "grossMargin", "Q1 2026"), ["INITIATED", "NARROWED"])
        withdrawn = [r for r in self.h["revisions"] if r["change"] == "WITHDRAWN"]
        self.assertEqual(len(withdrawn), 1)
        self.assertEqual(withdrawn[0]["targetPeriod"], "FY2025")
        reaffirmed = next(g for g in self.h["guidance"] if g["releaseDate"] == "2026-03-02" and g["metric"] == "revenue")
        self.assertEqual(reaffirmed["statedAction"], "REAFFIRMED_IN_TEXT")

    def test_range_without_period_is_kept_not_compared(self) -> None:
        stray = next(g for g in self.h["guidance"] if g["releaseDate"] == "2025-08-11" and g["metric"] == "revenue")
        self.assertEqual(stray["targetPeriod"]["label"], "FY2025")
        self.assertEqual(stray["targetPeriod"]["scope"], "GUIDANCE_MENTION", "the nearest guidance mention names the year")

    def test_unread_release_is_visible(self) -> None:
        unread = next(r for r in self.h["releases"] if r["status"] == "NOT_READ")
        self.assertEqual(unread["guidanceFound"], 0)
        self.assertFalse(any(g["releaseDate"] == "2026-05-11" for g in self.h["guidance"]))
        self.assertFalse(any(r["releaseDate"] == "2026-05-11" for r in self.h["revisions"]))

    def test_outcomes(self) -> None:
        fy26 = _outcome(self.h, "revenue", "FY2026")
        self.assertEqual(fy26["status"], "ACTUAL_NOT_YET_REPORTED")
        self.assertEqual(fy26["initialGuidance"]["high"], 250_000_000)
        self.assertEqual(fy26["lastGuidance"]["high"], 200_000_000)
        fy25 = _outcome(self.h, "revenue", "FY2025")
        self.assertEqual(fy25["status"], "EVALUATED")
        self.assertEqual(fy25["actual"]["value"], 4_400_000)
        self.assertEqual(fy25["positionVsLast"], "BELOW")
        gm = _outcome(self.h, "grossMargin", "Q1 2026")
        self.assertEqual(gm["status"], "NOT_EVALUATED_METRIC")
        self.assertIsNone(gm["positionVsLast"])

    def test_non_gaap_guidance_is_not_scored(self) -> None:
        h = gh.guidance_history("x", NON_GAAP_RELEASES, COMPANYFACTS)
        eps = _outcome(h, "eps", "FY2025")
        self.assertEqual((eps["status"], eps["basis"]), ("NOT_EVALUATED_NON_GAAP_BASIS", "NON_GAAP"))
        self.assertIsNone(eps["positionVsLast"])
        self.assertEqual(eps["actual"]["value"], -1.2, "the GAAP actual stays visible")
        rev = _outcome(h, "revenue", "FY2025")
        self.assertEqual((rev["status"], rev["basis"], rev["positionVsLast"]), ("EVALUATED", "NOT_STATED", "WITHIN"))

    def test_unread_companyfacts(self) -> None:
        h = gh.guidance_history("asts", RELEASES[:3], None)
        self.assertEqual(_outcome(h, "revenue", "FY2026")["status"], "ACTUALS_NOT_READ")
        self.assertFalse(h["actualsBasis"]["read"])

    def test_actual_labels(self) -> None:
        a = gh.actuals_from_company_facts(COMPANYFACTS)
        self.assertTrue(a["calendarFiscalYear"])
        self.assertEqual(a["revenue"]["Q1 2026"]["value"], 9_100_000)
        self.assertEqual(a["revenue"]["Q2 2026"]["value"], 14_700_000)
        # The filed six-month period is the first half; no second half is derived.
        self.assertEqual(a["revenue"]["H1 2026"]["value"], 23_700_000)
        self.assertFalse(any(k.startswith("H2") for k in a["revenue"]))
        fiscal = gh.actuals_from_company_facts(FISCAL_FACTS)
        self.assertFalse(fiscal["calendarFiscalYear"])
        self.assertEqual(list(fiscal["revenue"]), ["FY2025"])
        h = gh.guidance_history("x", [{"filingDate": "2025-11-01", "accessionNumber": "a", "url": None, "status": "READ",
                                        "text": "For the first quarter of fiscal 2026, the company expects revenue between $1 million and $2 million."}], FISCAL_FACTS)
        self.assertEqual(h["outcomes"][0]["status"], "NOT_EVALUATED_FISCAL_QUARTER_MAPPING")

    def test_52_53_week_fiscal_identity(self) -> None:
        # 2.5.11 (AEHR): a year named only by its end date is that date's fiscal year.
        aehr = gh.guidance_target_period("For the fiscal year ending June 25, 2027, Aehr expects revenue between $130 million and $150 million")
        self.assertEqual((aehr["label"], aehr["periodEnd"], aehr["basis"]), ("FY2027", "2027-06-25", "TEXT_PERIOD_END"))
        q3 = gh.guidance_target_period("For the third quarter of fiscal 2027 ending October 30, 2026, revenue guidance of X")
        self.assertEqual((q3["label"], q3["periodEnd"], q3["basis"]), ("Q3 2027", "2026-10-30", "TEXT"))
        self.assertEqual(gh.guidance_target_period("For the fiscal year ending January 2, 2027, guidance of X")["label"], "FY2026")
        self.assertEqual(gh.guidance_target_period("For the fiscal year ended February 30, 2027, guidance of X")["basis"], "NOT_STATED")
        stated = gh.actuals_from_company_facts(STATED_FACTS)
        # The stated fiscal years label the years; quarters count back from the year end (Q1 FY2027 from the projected end).
        self.assertEqual(stated["statedFiscalYears"], {"2025-05-30": 2025, "2026-05-29": 2026})
        self.assertEqual(stated["fiscalQuarterMapping"], "FILING_STATED_FISCAL_YEAR")
        self.assertEqual(sorted(stated["revenue"]), ["FY2025", "FY2026", "Q1 2027", "Q2 2026"])
        week = gh.actuals_from_company_facts(WEEK53_FACTS)
        # A year ending January 3, 2026 is fiscal 2025; its quarter ending April 5 is Q1 and July 5 is Q2.
        self.assertTrue(week["calendarFiscalYear"])
        self.assertEqual(sorted(week["revenue"]), ["FY2025", "H1 2025", "Q1 2025"])
        h = gh.guidance_history("aehr", AEHR_RELEASES, STATED_FACTS)
        outcomes = {o["targetPeriod"]: o for o in h["outcomes"]}
        self.assertEqual(outcomes["FY2027"]["status"], "ACTUAL_NOT_YET_REPORTED")
        self.assertEqual((outcomes["Q1 2027"]["status"], outcomes["Q1 2027"]["positionVsLast"]), ("EVALUATED", "WITHIN"))
        self.assertEqual(fc.fiscal_year_of_period_end("2026-01-03"), 2025)
        self.assertEqual(fc.fiscal_year_of_period_end("2026-01-08"), 2026)
        self.assertEqual([fc.fiscal_quarter_of(a, b) for a, b in FISCAL_QUARTERS], [2, 1, 4, 1, None, None])

    def test_fiscal_year_naming(self) -> None:
        # 2.5.13: DG's stated years sit one below the period-end rule; mixed statements give no offset.
        # 2.5.16: with the calendar its year ends fit, settled by the third year (2024-02-02 is the Friday nearest
        # January 31, not the last Friday of January).
        self.assertEqual(fc.fiscal_year_naming(DG_FACTS), {"offset": -1, "basis": "SEC_STATED_FISCAL_YEAR", "periodEnd": "2026-01-30", "statedFiscalYear": 2025,
                                                           "calendar": {"patterns": ["WEEKDAY_NEAREST_MONTH_END"], "month": 1, "weekday": "Friday",
                                                                        "basis": "SEC_ANNUAL_PERIOD_ENDS", "periodEnds": ["2026-01-30", "2025-01-31", "2024-02-02"]}})
        self.assertEqual(fc.fiscal_year_naming(MIXED_FACTS)["basis"], "PERIOD_END_RULE_STATED_YEARS_INCONSISTENT")
        self.assertEqual(fc.fiscal_year_naming(MIXED_FACTS)["offset"], 0)
        self.assertEqual(fc.fiscal_year_naming(None)["basis"], "PERIOD_END_RULE_SEC_NOT_READ")
        self.assertEqual([fc.document_fiscal_year_focus(h) for h in FOCUS_HTML], [2025, 2026, None, None])
        # 2.5.14: a filing's label is the year it tags for itself, unless that is a mistag beyond one year.
        self.assertEqual([fc.filing_fiscal_year_label(f, d) for f, d in FILING_LABEL_CASES],
                         ["FY2025", "FY2025", "FY2026", "FY2026", "FY2025", None])
        # "the fiscal year ending January 29, 2027" is DG's fiscal 2026, not 2027.
        g = gh.guidance_history("dg", DG_RELEASES, DG_FACTS)["guidance"][0]["targetPeriod"]
        self.assertEqual((g["label"], g["fiscalYear"], g["basis"], g["namingBasis"]), ("FY2026", 2026, "TEXT_PERIOD_END", "SEC_STATED_FISCAL_YEAR"))

    def test_fiscal_calendar(self) -> None:
        # 2.5.16: the company's year end from its SEC annual period ends, projected only when the rules that fit agree.
        def year_end(ends: list, provider: str | None):
            return fc.company_fiscal_year_end(fc.settled_fiscal_calendar(ends), provider)

        dg = fc.fiscal_calendar(["2026-01-30", "2025-01-31", "2024-02-02"])
        self.assertEqual(dg, {"patterns": ["WEEKDAY_NEAREST_MONTH_END"], "month": 1, "weekday": "Friday",
                              "basis": "SEC_ANNUAL_PERIOD_ENDS", "periodEnds": ["2026-01-30", "2025-01-31", "2024-02-02"]})
        self.assertEqual(fc.company_fiscal_year_end(dg, "2027-01-31"), "2027-01-29")
        self.assertEqual(year_end(["2026-01-31", "2025-02-01", "2024-02-03"], "2027-01-31"), "2027-01-30")
        fn = fc.fiscal_calendar(["2026-06-26", "2025-06-27", "2024-06-28"])
        self.assertEqual(fn["patterns"], ["LAST_WEEKDAY_OF_MONTH"])
        self.assertEqual(fc.company_fiscal_year_end(fn, "2027-06-30"), "2027-06-25")
        # LITE: three ends fit two rules that disagree for 2027, so no date; a fourth end settles it.
        lite = ["2026-06-27", "2025-06-28", "2024-06-29"]
        self.assertEqual(fc.fiscal_calendar(lite)["patterns"], ["WEEKDAY_NEAREST_MONTH_END", "LAST_WEEKDAY_OF_MONTH"])
        self.assertIsNone(year_end(lite, "2027-06-30"))
        settled = fc.settled_fiscal_calendar([*lite, "2021-07-03"])
        self.assertEqual(settled["patterns"], ["WEEKDAY_NEAREST_MONTH_END"])
        self.assertEqual(fc.company_fiscal_year_end(settled, "2027-06-30"), "2027-07-03")
        anet = fc.fiscal_calendar(["2025-12-31", "2024-12-31"])
        self.assertEqual((anet["patterns"], anet["weekday"]), (["MONTH_END"], None))
        self.assertEqual(fc.company_fiscal_year_end(anet, "2026-12-31"), "2026-12-31")
        # No date for another month, a single end, a changed calendar, or no calendar / provider end.
        self.assertIsNone(fc.company_fiscal_year_end(dg, "2027-06-30"))
        self.assertIsNone(fc.fiscal_calendar(["2026-01-30"]))
        self.assertIsNone(fc.fiscal_calendar(["2026-06-30", "2025-12-31"]))
        self.assertIsNone(fc.company_fiscal_year_end(None, "2027-01-31"))
        self.assertIsNone(fc.company_fiscal_year_end(dg, None))
        # Naming carries the calendar; not read or no stated year gives none.
        self.assertIsNone(fc.fiscal_year_naming(None)["calendar"])
        self.assertIsNone(fc.fiscal_year_naming({"facts": {}})["calendar"])

    def test_bullets_and_half_years(self) -> None:
        h = gh.guidance_history("asts", BULLET_RELEASES, COMPANYFACTS)
        # Not the Q1 2026 or Q3 2025 of the neighbouring bullets.
        self.assertEqual([(g["releaseDate"], g["targetPeriod"]["label"], g["targetPeriod"]["scope"]) for g in h["guidance"]],
                         [("2025-08-11", "H2 2025", "SENTENCE"), ("2025-11-10", "H2 2025", "SENTENCE")])
        self.assertEqual(_revisions(h, "revenue", "H2 2025"), ["INITIATED", "REAFFIRMED"])
        self.assertEqual(h["guidance"][1]["statedAction"], "REAFFIRMED_IN_TEXT")
        self.assertIsNone(h["guidance"][0]["statedAction"])
        self.assertEqual(_outcome(h, "revenue", "H2 2025")["status"], "NOT_EVALUATED_HALF_YEAR")

    def test_authority_boundary(self) -> None:
        for key, value in AUTHORITY_BOUNDARY.items():
            self.assertEqual(self.h[key], value)


class TestDriverLedger(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.l = dl.operating_driver_ledger(ticker="asts", companyfacts=COMPANYFACTS, inline_facts=INLINE_FACTS,
                                           filing={"status": "READ"}, release={"status": "READ"}, statements=STATEMENTS)

    def _series(self, driver: str) -> dict:
        return next(s for s in self.l["xbrlSeries"] if s["driver"] == driver)

    def test_xbrl_series(self) -> None:
        revenue = self._series("revenue")
        self.assertEqual(revenue["status"], "REPORTED")
        by_period = {(p["periodStart"], p["periodEnd"]): p for p in revenue["points"]}
        self.assertEqual(by_period[("2026-01-01", "2026-03-31")]["value"], 9_100_000)
        self.assertEqual(by_period[("2026-01-01", "2026-06-30")]["periodType"], "YEAR_TO_DATE")
        self.assertEqual(by_period[("2026-04-01", "2026-06-30")]["value"], 14_700_000)
        capex = self._series("capital_expenditure")
        # Year-to-date capex stays year-to-date; no quarter is derived.
        self.assertEqual([p["periodType"] for p in capex["points"]], ["QUARTER", "YEAR_TO_DATE"])
        rpo = self._series("remaining_performance_obligation")
        self.assertEqual([(p["periodType"], p["value"]) for p in rpo["points"]], [("INSTANT", 1_000_000_000)])
        self.assertIn("research_and_development", self.l["notReported"])
        self.assertEqual(self._series("gross_profit")["latestPeriodEnd"], "2026-03-31")
        self.assertIsNone(self._series("research_and_development")["latestPeriodEnd"])
        self.assertEqual(self.l["notRead"], [])

    def test_company_specific_series(self) -> None:
        series = self.l["companySpecificSeries"]
        self.assertEqual(self.l["companySpecificStatus"], "READ")
        self.assertEqual([s["concept"] for s in series], ["asts:MNOAgreementsCount", "asts:NumberOfSatellitesLaunched", "asts:NumberOfSatellitesLaunched"])
        launched = series[1]
        self.assertEqual(launched["label"], "Number Of Satellites Launched")
        self.assertEqual([p["value"] for p in launched["points"]], [0, 5])
        self.assertEqual(series[2]["dims"], {"srt:ProductOrServiceAxis": "asts:BlockTwoMember"})
        self.assertEqual(series[0]["label"], "MNO Agreements Count")
        # Monetary extension concepts are accounting line items: counted, not listed.
        self.assertEqual(self.l["excludedMonetaryConcepts"], 1)
        # Warrant exercises and credit fees are capital structure, not operating drivers.
        self.assertEqual(self.l["excludedCapitalStructureConcepts"], 2)
        self.assertFalse(any("Warrant" in s["concept"] or "Fee" in s["concept"] for s in series))

    def test_text_drivers(self) -> None:
        text = self.l["textDrivers"]
        sentences = [t["sentence"] for t in text]
        self.assertEqual(sentences.count("We launched five BlueBird satellites during the second quarter of 2026."), 0)  # no figure
        planned = next(t for t in text if t["sentence"].startswith("We expect to have 45 to 60 satellites"))
        self.assertEqual(planned["basis"], "TARGET_OR_PLAN")
        self.assertIn("launches_deployments", planned["categories"])
        self.assertEqual(planned["timing"]["horizon"], "through_year")
        staff = next(t for t in text if "employees" in t["sentence"])
        self.assertEqual(staff["basis"], "REPORTED_ACTUAL")
        self.assertEqual(staff["figures"][0]["asWritten"], "1,020 full-time")
        backlog = next(t for t in text if "backlog" in t["sentence"])
        self.assertEqual(backlog["source"], "PERIODIC_FILING")
        self.assertEqual(backlog["figures"][0]["currency"], "USD")
        self.assertFalse(any(t["sentence"].startswith("Revenue was") for t in text))  # no driver category
        self.assertFalse(any(t["sentence"].startswith("In 2025") for t in text))  # a year is not a figure
        self.assertEqual(self.l["summary"]["textDriverCount"], len(text))
        # A search window's cut-off first and last pieces are not sentences.
        self.assertFalse(any(t["sentence"].startswith("deployed in orbit") for t in text))
        self.assertFalse(any(t["sentence"].startswith("We shipped 12 gateway") for t in text))

    def test_bullets_split(self) -> None:
        text = dl.text_drivers([{"contextText": "Business Update \u2022 Reported 5 satellites launched in the quarter o Revenue backlog increased to $1.30 billion o Next",
                                 "source": "EARNINGS_RELEASE"}])
        self.assertEqual([t["sentence"] for t in text], ["Reported 5 satellites launched in the quarter", "Revenue backlog increased to $1.30 billion"])

    def test_unread_sources(self) -> None:
        unread = dl.operating_driver_ledger(ticker="asts", companyfacts=None, inline_facts=None, filing=None, release=None, statements=[])
        self.assertEqual(unread["notReported"], [])
        self.assertEqual(len(unread["notRead"]), len(dl.DRIVER_SERIES))
        self.assertEqual(unread["companySpecificStatus"], "NOT_READ")

    def test_figures(self) -> None:
        self.assertEqual([f["asWritten"] for f in dl.figures_in("Revenue was $14.7 million in 2026.")], ["$14.7 million"])
        self.assertEqual([f["asWritten"] for f in dl.figures_in("In 2025 and 2026 we grew 12%.")], ["12%"])
        self.assertEqual([(f["asWritten"], f["unitAsWritten"]) for f in dl.figures_in("Revenue: $3.4B - $3.8B")], [("$3.4B", "B"), ("$3.8B", "B")], "2.5.20")
        self.assertEqual(dl.figures_in("The 10-Q lists 3 of 4 items."), [{"asWritten": "4 items", "number": 4.0, "currency": None, "unitAsWritten": "items"}])
        self.assertEqual([f["unitAsWritten"] for f in dl.figures_in("Capacity reached 1.5 GW and 300 MW.")], ["GW", "MW"])
        # Numbers inside names are not figures.
        self.assertEqual([f["asWritten"] for f in dl.figures_in(
            "Orbital launch of BlueBird 8-13 marks six spacecraft, with Block 2 satellites and 45 satellites planned.")], ["45 satellites"])
        self.assertEqual([f["asWritten"] for f in dl.figures_in("Reported 5 satellites launched.")], ["5 satellites"])
        # Product/model names are not figures even when the sentence starts with the name (live ASTS residual in 2.5.4).
        self.assertEqual(dl.figures_in("Block 2 satellites expected to deliver peak data rates."), [])
        self.assertEqual(dl.figures_in("New Glenn 3 launch vehicle is scheduled."), [])
        # A year before a word is not a figure (live ASTS release, 2.5.3).
        self.assertEqual([f["asWritten"] for f in dl.figures_in("Reaffirmed 2026 revenue of $150.0 million to $200.0 million.")],
                         ["$150.0 million", "$200.0 million"])

    def test_authority_boundary(self) -> None:
        for key, value in AUTHORITY_BOUNDARY.items():
            self.assertEqual(self.l[key], value)


if __name__ == "__main__":
    unittest.main()
