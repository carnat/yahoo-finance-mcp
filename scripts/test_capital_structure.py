#!/usr/bin/env python3
"""Dilution bridge, capital structure, analyst methods and UK filings (2.3.0).

companyfacts drops dimensional XBRL facts, so per-instrument debt terms and
per-class warrant counts were unreachable. worker/src/capital-structure.ts and
yfmcp/capital_structure.py parse the filing's own inline XBRL and build the
bridge and timeline from it; they must agree exactly.

Part one runs both pure modules on the same fixtures. Part two drives the
tools end to end: the Worker in Miniflare against a mocked SEC and Companies
House, the local server with its EDGAR and HTTP helpers patched.
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

from yfmcp import capital_structure as cs  # noqa: E402

CIK = 1234568
ARCHIVE = f"https://www.sec.gov/Archives/edgar/data/{CIK}"


def _context(cid: str, period: str, dims: dict[str, str] | None = None) -> str:
    segment = ""
    if dims:
        members = "".join(f'<xbrldi:explicitMember dimension="{axis}">{member}</xbrldi:explicitMember>' for axis, member in dims.items())
        segment = f"<xbrli:segment>{members}</xbrli:segment>"
    if ".." in period:
        start, end = period.split("..")
        when = f"<xbrli:startDate>{start}</xbrli:startDate><xbrli:endDate>{end}</xbrli:endDate>"
    else:
        when = f"<xbrli:instant>{period}</xbrli:instant>"
    return (f'<xbrli:context id="{cid}"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">000{CIK}</xbrli:identifier>'
            f"{segment}</xbrli:entity><xbrli:period>{when}</xbrli:period></xbrli:context>")


UNITS = (
    '<xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>'
    '<xbrli:unit id="shares"><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unit>'
    '<xbrli:unit id="pure"><xbrli:measure>xbrli:pure</xbrli:measure></xbrli:unit>'
    '<xbrli:unit id="usdPerShare"><xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unitNumerator>'
    '<xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitDenominator></xbrli:divide></xbrli:unit>'
)


def _num(name: str, ctx: str, unit: str, text: str, scale: int = 0, fmt: str = "ixt:num-dot-decimal", sign: str = "", decimals: str = "-3") -> str:
    extra = f' sign="{sign}"' if sign else ""
    return f'<ix:nonFraction name="{name}" contextRef="{ctx}" unitRef="{unit}" decimals="{decimals}" scale="{scale}" format="{fmt}"{extra}>{text}</ix:nonFraction>'


def _text(name: str, ctx: str, text: str, fmt: str = "") -> str:
    extra = f' format="{fmt}"' if fmt else ""
    return f'<ix:nonNumeric name="{name}" contextRef="{ctx}"{extra}>{text}</ix:nonNumeric>'


DEBT = "us-gaap:DebtInstrumentAxis"
CONV = "cstc:ConvertibleSeniorNotesDue2029Member"
TERM = "cstc:TermLoanMember"
OLD = "cstc:SeniorNotesDue2023Member"
# Tagged the ASTS way (2.3.1): principal only as a carrying amount on the issue date.
CONV31 = "cstc:ConvertibleNotesDue2031Member"
# A coupon-only duplicate member of the 2029 notes.
DUP = "cstc:ConvertibleSeniorNotesDue2029PercentageMember"

K_CONTEXTS = "".join([
    _context("fy24", "2024-01-01..2024-12-31"),
    _context("i24", "2024-12-31"),
    _context("i23", "2023-12-31"),
    _context("cover", "2025-02-10"),
    _context("coverA", "2025-02-10", {"dei:EntityListingsExchangeAxis": "exch:XNAS", "us-gaap:StatementClassOfStockAxis": "us-gaap:CommonClassAMember"}),
    _context("coverA1", "2025-02-10", {"us-gaap:StatementClassOfStockAxis": "us-gaap:CommonClassAMember"}),
    _context("coverB1", "2025-02-10", {"us-gaap:StatementClassOfStockAxis": "us-gaap:CommonClassBMember"}),
    _context("r1", "2024-12-31", {"us-gaap:ExercisePriceRangeAxis": "cstc:RangeOneMember"}),
    _context("r2", "2024-12-31", {"us-gaap:ExercisePriceRangeAxis": "cstc:RangeTwoMember"}),
    _context("rsu", "2024-12-31", {"us-gaap:AwardTypeAxis": "us-gaap:RestrictedStockUnitsRSUMember"}),
    _context("psu", "2024-12-31", {"us-gaap:AwardTypeAxis": "us-gaap:PerformanceSharesMember"}),
    _context("wpub", "2024-12-31", {"us-gaap:ClassOfWarrantOrRightAxis": "cstc:PublicWarrantsMember"}),
    _context("wpre", "2024-12-31", {"us-gaap:ClassOfWarrantOrRightAxis": "cstc:PrefundedWarrantsMember"}),
    _context("wb", "2024-12-31", {"us-gaap:ClassOfWarrantOrRightAxis": "cstc:SeriesBWarrantsMember"}),
    _context("conv", "2024-12-31", {DEBT: CONV}),
    _context("convIssue", "2024-03-15", {DEBT: CONV}),
    _context("convD", "2024-01-01..2024-12-31", {DEBT: CONV}),
    _context("term", "2024-12-31", {DEBT: TERM}),
    _context("termD", "2024-01-01..2024-12-31", {DEBT: TERM}),
    _context("old", "2023-06-01", {DEBT: OLD}),
    _context("c31Issue", "2024-06-01", {DEBT: CONV31}),
    _context("c31D", "2024-01-01..2024-12-31", {DEBT: CONV31}),
    _context("dupD", "2024-01-01..2024-12-31", {DEBT: DUP}),
])

TEN_K = f"""<html><head><title>cstc-20241231</title></head><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "fy24", "10-K")}
{_text("dei:DocumentPeriodEndDate", "fy24", "December 31, 2024", "ixt:date-monthname-day-year-en")}
{_num("us-gaap:SegmentReportingNumberOfSegments", "fy24", "pure", "two", fmt="ixt-sec:numwordsen")}
<ix:nonFraction name="us-gaap:Goodwill" contextRef="i24" unitRef="usd" xsi:nil="true"/>
</ix:hidden><ix:resources>{K_CONTEXTS}{UNITS}</ix:resources></ix:header></div>
<p>Class A shares outstanding at February 10, 2025: {_num("dei:EntityCommonStockSharesOutstanding", "coverA1", "shares", "90,000,000")}; Class B: {_num("dei:EntityCommonStockSharesOutstanding", "coverB1", "shares", "10,000,000")}</p>
<p><span style="font-weight:700">PART II</span></p>
<p><span style="font-weight:700">Item 7. Management&#8217;s Discussion and Analysis of Financial Condition and Results of Operations</span></p>
<p><span style="font-weight:700">Liquidity and Capital Resources</span></p>
<p>We believe our existing cash, cash equivalents and short-term investments will be sufficient to fund our operations for at least the next twelve months.</p>
<p>In May 2024, we entered into a sales agreement for an at-the-market offering program to sell up to $200.0 million of our common stock. During 2024, we sold 2,000,000 shares for net proceeds of $48.5 million, and $150.0 million remained available under the ATM program as of December 31, 2024.</p>
<p>We expect capital expenditures of approximately $40 million in 2025, which we expect to be fully funded by cash on hand.</p>
<p><span style="font-weight:700">Item 8. Financial Statements and Supplementary Data</span></p>
<table>
<tr><td>Cash and cash equivalents</td><td>$</td><td>{_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "i24", "usd", "150.0", 6)}</td><td>{_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "i23", "usd", "90.0", 6)}</td></tr>
<tr><td>Short-term investments</td><td>{_num("us-gaap:ShortTermInvestments", "i24", "usd", "50.0", 6)}</td></tr>
<tr><td>Long-term debt</td><td>{_num("us-gaap:LongTermDebt", "i24", "usd", "385.0", 6)}</td></tr>
<tr><td>Common stock outstanding</td><td>{_num("us-gaap:CommonStockSharesOutstanding", "i24", "shares", "99,500,000")}</td></tr>
</table>
<p>Options outstanding: {_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingNumber", "i24", "shares", "5,000", 3)} at a weighted-average exercise price of ${_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingWeightedAverageExercisePrice", "i24", "usdPerShare", "12.80")}; exercisable {_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsExercisableNumber", "i24", "shares", "3,000", 3)}.</p>
<table>
<tr><td>$0.00 - $10.00</td><td>{_num("us-gaap:ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeOutstandingOptions", "r1", "shares", "2,000", 3)}</td><td>{_num("us-gaap:ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeOutstandingOptionsWeightedAverageExercisePrice", "r1", "usdPerShare", "5.00")}</td></tr>
<tr><td>$10.01 - $30.00</td><td>{_num("us-gaap:ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeOutstandingOptions", "r2", "shares", "3,000", 3)}</td><td>{_num("us-gaap:ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeOutstandingOptionsWeightedAverageExercisePrice", "r2", "usdPerShare", "18.00")}</td></tr>
</table>
<p>Unvested RSUs {_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber", "rsu", "shares", "1,500,000")} and PSUs {_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber", "psu", "shares", "500,000")}.</p>
<p>Public warrants {_num("us-gaap:ClassOfWarrantOrRightOutstanding", "wpub", "shares", "4,000,000")} exercisable at ${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "wpub", "usdPerShare", "11.50")};
pre-funded warrants {_num("us-gaap:ClassOfWarrantOrRightOutstanding", "wpre", "shares", "1,000,000")} at ${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "wpre", "usdPerShare", "0.0001")};
Series B warrants {_num("us-gaap:ClassOfWarrantOrRightOutstanding", "wb", "shares", "500,000")}.</p>
<p>In March 2024 we issued ${_num("us-gaap:DebtInstrumentFaceAmount", "convIssue", "usd", "300.0", 6)} million of {_num("us-gaap:DebtInstrumentInterestRateStatedPercentage", "convIssue", "pure", "0.375", -2)}% convertible senior notes due {_text("us-gaap:DebtInstrumentMaturityDate", "convD", "March 15, 2029", "ixt:date-monthname-day-year-en")}, convertible at {_num("us-gaap:DebtInstrumentConvertibleConversionRatio1", "convD", "pure", "50.0000")} shares per $1,000 principal. Carrying amount {_num("us-gaap:LongTermDebt", "conv", "usd", "290.0", 6)}.</p>
<p>Term loan of ${_num("us-gaap:DebtInstrumentFaceAmount", "termD", "usd", "100.0", 6)} million bearing {_num("us-gaap:DebtInstrumentInterestRateStatedPercentage", "termD", "pure", "7.25", -2)}% and maturing {_text("us-gaap:DebtInstrumentMaturityDate", "termD", "June&#160;30,&#160;2027", "ixt:date-monthname-day-year-en")}; carrying {_num("us-gaap:LongTermDebt", "term", "usd", "95.0", 6)}.</p>
<p>In June 2024 we issued {_num("us-gaap:LongTermDebt", "c31Issue", "usd", "200.0", 6)} of convertible notes due {_text("us-gaap:DebtInstrumentMaturityDate", "c31D", "June 1, 2031", "ixt:date-monthname-day-year-en")}, convertible at ${_num("us-gaap:DebtInstrumentConvertibleConversionPrice1", "c31D", "usdPerShare", "40.00")} per share; the {_num("us-gaap:DebtInstrumentInterestRateStatedPercentage", "dupD", "pure", "0.375", -2)}% notes are described above.</p>
<p>The ${_num("us-gaap:DebtInstrumentFaceAmount", "old", "usd", "50.0", 6)} million senior notes matured on {_text("us-gaap:DebtInstrumentMaturityDate", "old", "June 1, 2023", "ixt:date-monthname-day-year-en")}.</p>
<table>
<tr><td>2025</td><td>{_num("us-gaap:LongTermDebtMaturitiesRepaymentsOfPrincipalInNextTwelveMonths", "i24", "usd", "5.0", 6)}</td></tr>
<tr><td>2026</td><td>{_num("us-gaap:LongTermDebtMaturitiesRepaymentsOfPrincipalInYearTwo", "i24", "usd", "5.0", 6)}</td></tr>
<tr><td>2027</td><td>{_num("us-gaap:LongTermDebtMaturitiesRepaymentsOfPrincipalInYearThree", "i24", "usd", "90.0", 6)}</td></tr>
<tr><td>2028</td><td>{_num("us-gaap:LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFour", "i24", "usd", "&#8212;", 6, "ixt:fixed-zero")}</td></tr>
<tr><td>Thereafter</td><td>{_num("us-gaap:LongTermDebtMaturitiesRepaymentsOfPrincipalAfterYearFive", "i24", "usd", "300.0", 6)}</td></tr>
</table>
</body></html>"""

Q_CONTEXTS = "".join([
    _context("q1", "2025-01-01..2025-03-31"),
    _context("iq1", "2025-03-31"),
    _context("coverQ", "2025-05-01"),
])

TEN_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "q1", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "q1", "March 31, 2025", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{Q_CONTEXTS}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding at May 1, 2025: {_num("dei:EntityCommonStockSharesOutstanding", "coverQ", "shares", "101,000,000")}</p>
<p><span style="font-weight:700">Item 2. Management&#8217;s Discussion and Analysis of Financial Condition and Results of Operations</span></p>
<p>As of March 31, 2025, $140.0 million remained available for sale under the ATM program.</p>
<p>Cash {_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "iq1", "usd", "140.0", 6)}; long-term debt {_num("us-gaap:LongTermDebt", "iq1", "usd", "380.0", 6)}; unvested RSUs {_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber", "iq1", "shares", "1,800,000")}.</p>
</body></html>"""

AWARD = "us-gaap:AwardTypeAxis"
PLAN = "us-gaap:PlanNameAxis"
A_CONTEXTS = "".join([
    _context("aq", "2025-04-01..2025-06-30"),
    _context("aytd", "2025-01-01..2025-06-30"),
    _context("ai", "2025-06-30"),
    _context("acover", "2025-08-01"),
    _context("aPlan", "2025-06-30", {PLAN: "cstc:Plan2020Member"}),
    _context("aRsuPlan", "2025-06-30", {AWARD: "us-gaap:RestrictedStockUnitsRSUMember", PLAN: "cstc:Plan2020Member"}),
    _context("aPsuPlan", "2025-06-30", {AWARD: "us-gaap:PerformanceSharesMember", PLAN: "cstc:Plan2020Member"}),
    _context("aWarrant", "2025-06-30", {"us-gaap:ClassOfWarrantOrRightAxis": "cstc:CustomerWarrantMember"}),
    _context("aAntiQ", "2025-04-01..2025-06-30", {"us-gaap:AntidilutiveSecuritiesAxis": "us-gaap:RestrictedStockUnitsRSUMember"}),
    _context("aAntiYtd", "2025-01-01..2025-06-30", {"us-gaap:AntidilutiveSecuritiesAxis": "us-gaap:RestrictedStockUnitsRSUMember"}),
])
# Tagged the AAOI way (2.3.1): awards only by type x plan and by plan, the
# warrant as the shares it calls for, and EPS exclusions by security.
AWARDS_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "aq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "aq", "June 30, 2025", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{A_CONTEXTS}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "acover", "shares", "50,000,000")}</p>
<p>Unvested: RSUs {_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber", "aRsuPlan", "shares", "700,000")},
PSUs {_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber", "aPsuPlan", "shares", "300,000")},
2020 plan total {_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber", "aPlan", "shares", "1,000,000")}.</p>
<p>The customer warrant is exercisable for {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "aWarrant", "shares", "2,000,000")} shares at ${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "aWarrant", "usdPerShare", "10.00")}.</p>
<p>Weighted basic {_num("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "aq", "shares", "49,000,000")} (six months {_num("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "aytd", "shares", "48,500,000")});
diluted {_num("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding", "aq", "shares", "49,000,000")}; antidilutive RSUs excluded {_num("us-gaap:AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount", "aAntiQ", "shares", "900,000")}
(six months {_num("us-gaap:AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount", "aAntiYtd", "shares", "950,000")}).</p>
</body></html>"""

AMZN_DIMS = {"us-gaap:ClassOfWarrantOrRightAxis": "aaoi:CustomerWarrantMember", "srt:CounterpartyNameAxis": "aaoi:SubsidiaryOfAmazonMember"}
AAOI_CONTEXTS = "".join([
    _context("xq", "2026-04-01..2026-06-30"),
    _context("xi", "2026-06-30"),
    _context("xcover", "2026-08-03"),
    _context("xIssue", "2025-03-13", AMZN_DIMS),
    _context("xUnvested", "2026-06-30", AMZN_DIMS),
    _context("xAnti", "2026-04-01..2026-06-30", {"aaoi:AntidilutiveSecurityTypeAxis": "us-gaap:RestrictedStockUnitsRSUMember"}),
])
# Tagged the way AAOI's live 10-Q is (2.4.0): awards only under a company
# concept, an Amazon warrant with an unvested tranche, exclusions on a custom axis.
AAOI_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "xq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "xq", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{AAOI_CONTEXTS}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "xcover", "shares", "84,000,000")}</p>
<p>RSUs vested and expected to vest: {_num("aaoi:SharebasedCompensationArrangementBySharebasedPaymentAwardNonoptionEquityInstrumentsVestedAndExpectedToVest", "xi", "shares", "3,000,000")}</p>
<p>The warrant is exercisable for {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "xIssue", "shares", "7,945,399")} shares at ${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "xIssue", "usdPerShare", "23.6956")};
{_num("aaoi:ClassOfWarrantOrRightUnvestedNumberOfSecuritiesCalledByWarrantsOrRights", "xUnvested", "shares", "5,000,000")} remain unvested.</p>
<p>Cash {_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "xi", "usd", "499,737", 3)}; bank loans current {_num("us-gaap:LongTermDebtCurrent", "xi", "usd", "57,258", 3)} and noncurrent {_num("us-gaap:LongTermDebtNoncurrent", "xi", "usd", "1,657", 3)};
convertible senior notes, net {_num("us-gaap:ConvertibleNotesPayable", "xi", "usd", "124,900", 3)} (a separate balance-sheet line).</p>
<p>Weighted basic {_num("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "xq", "shares", "81,568,000")}; excluded RSUs {_num("us-gaap:AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount", "xAnti", "shares", "1,100,000")}.</p>
</body></html>"""
BARE_Q = f"""<html><body><div style="display:none"><ix:header><ix:resources>{_context("bcover", "2026-08-03")}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "bcover", "shares", "50,000,000")}</p></body></html>"""
# Tagged the way ASTS's live 10-Q is (2.4.2): a note sentence tags the
# money-market part of cash, "approximately $2.3 billion", as short-term
# investments, and a rounded cash figure precedes the balance-sheet line.
ASTS_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "sq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "sq", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("sq", "2026-01-01..2026-06-30")}{_context("si", "2026-06-30")}{_context("sprior", "2025-12-31")}{UNITS}</ix:resources></ix:header></div>
<p>We ended the quarter with about ${_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "si", "usd", "2.3", 9, decimals="-8")} billion of cash.</p>
<p>Cash and cash equivalents {_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "si", "usd", "2,288,253", 3)}; current portion of long-term debt {_num("us-gaap:LongTermDebtCurrent", "si", "usd", "8,494", 3)}; long-term debt {_num("us-gaap:LongTermDebtNoncurrent", "si", "usd", "2,963,422", 3)}.</p>
<p>Of which approximately ${_num("us-gaap:ShortTermInvestments", "si", "usd", "2.3", 9, decimals="-8")} billion and ${_num("us-gaap:ShortTermInvestments", "sprior", "usd", "2.0", 9, decimals="-8")} billion, respectively, is classified as cash equivalents.</p>
</body></html>"""
# A debt-free filer (AEHR): cash and a lease liability, no borrowing concepts.
DEBT_FREE_K = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "dy", "10-K")}
{_text("dei:DocumentPeriodEndDate", "dy", "May 29, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("dy", "2025-05-31..2026-05-29")}{_context("di", "2026-05-29")}{UNITS}</ix:resources></ix:header></div>
<p>Cash and cash equivalents {_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "di", "usd", "116,358", 3)}; operating lease liabilities {_num("us-gaap:OperatingLeaseLiability", "di", "usd", "9,882", 3)}.</p>
</body></html>"""
# A precise investments figure in a sentence that calls it cash equivalents,
# ahead of the balance-sheet table whose own investments line must survive.
OVERLAP_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "oq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "oq", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("oq", "2026-01-01..2026-06-30")}{_context("oi", "2026-06-30")}{UNITS}</ix:resources></ix:header></div>
<p>Liquidity. As of June 30, 2026, <span>{_num("us-gaap:ShortTermInvestments", "oi", "usd", "180,000", 3)}</span> of our cash was held in money market funds classified as cash equivalents. We have no other investments.</p>
<table><tr><td>Cash and cash equivalents</td><td>{_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "oi", "usd", "400,000", 3)}</td></tr>
<tr><td>Short-term investments</td><td>{_num("us-gaap:ShortTermInvestments", "oi", "usd", "120,000", 3)}</td></tr></table>
<p>Our {_num("us-gaap:LongTermDebtNoncurrent", "oi", "usd", "50,000", 3)} term loan matures in 2029.</p>
</body></html>"""
# The filing's own cash-plus-investments total equals cash, so the investments are inside it.
AGGREGATE_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "gq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "gq", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("gq", "2026-01-01..2026-06-30")}{_context("gi", "2026-06-30")}{UNITS}</ix:resources></ix:header></div>
<table><tr><td>Cash and cash equivalents</td><td>{_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "gi", "usd", "500,000", 3)}</td></tr>
<tr><td>Short-term investments</td><td>{_num("us-gaap:ShortTermInvestments", "gi", "usd", "200,000", 3)}</td></tr>
<tr><td>Cash, cash equivalents and short-term investments</td><td>{_num("us-gaap:CashCashEquivalentsAndShortTermInvestments", "gi", "usd", "500,000", 3)}</td></tr>
<tr><td>Long-term debt</td><td>{_num("us-gaap:LongTermDebt", "gi", "usd", "10,000", 3)}</td></tr></table>
</body></html>"""
# Tagged the way VRT's live 10-Q is (2.4.3): Treasury bills held to maturity
# under the post-CECL concept, in millions, with a fair-value twin not read.
VRT_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "vq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "vq", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("vq", "2026-01-01..2026-06-30")}{_context("vi", "2026-06-30")}{UNITS}</ix:resources></ix:header></div>
<table><tr><td>Cash and cash equivalents</td><td>{_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "vi", "usd", "2,810.6", 6, decimals="-5")}</td></tr>
<tr><td>Short-term investments</td><td>{_num("us-gaap:DebtSecuritiesHeldToMaturityAmortizedCostAfterAllowanceForCreditLossCurrent", "vi", "usd", "300.0", 6, decimals="-5")}</td></tr>
<tr><td>Long-term debt, net</td><td>{_num("us-gaap:LongTermDebt", "vi", "usd", "2,939.8", 6, decimals="-5")}</td></tr></table>
<p>The short-term investments had a fair value of ${_num("us-gaap:DebtSecuritiesHeldToMaturityFairValueCurrent", "vi", "usd", "300.0", 6, decimals="-5")}.</p>
</body></html>"""
# Available-for-sale and held-to-maturity lines with no total: they add up.
PARTS_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "pq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "pq", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("pq", "2026-01-01..2026-06-30")}{_context("pi", "2026-06-30")}{UNITS}</ix:resources></ix:header></div>
<table><tr><td>Cash</td><td>{_num("us-gaap:CashAndCashEquivalentsAtCarryingValue", "pi", "usd", "200,000", 3)}</td></tr>
<tr><td>Available-for-sale</td><td>{_num("us-gaap:AvailableForSaleSecuritiesDebtSecuritiesCurrent", "pi", "usd", "40,000", 3)}</td></tr>
<tr><td>Held to maturity</td><td>{_num("us-gaap:HeldToMaturitySecuritiesCurrent", "pi", "usd", "60,000", 3)}</td></tr>
<tr><td>Debt</td><td>{_num("us-gaap:LongTermDebt", "pi", "usd", "10,000", 3)}</td></tr></table>
</body></html>"""
TABLE_MATCHES = [
    {"contextText": "Unvested at December 31, 2025 | 2,500,000 | $ 14.10", "sectionHeading": "Stock-Based Compensation", "documentUrl": "https://www.sec.gov/Archives/x.htm", "filingDate": "2026-08-06", "accessionNumber": None, "inTable": True, "tableTitle": "Restricted stock unit activity", "rowLabel": "Unvested at December 31, 2025"},
    {"contextText": "Unvested at June 30, 2026 | 2,750,000 | $ 15.20", "sectionHeading": "Stock-Based Compensation", "documentUrl": "https://www.sec.gov/Archives/x.htm", "filingDate": "2026-08-06", "accessionNumber": None, "inTable": True, "tableTitle": "Restricted stock unit activity", "rowLabel": "Unvested at June 30, 2026"},
    {"contextText": "Outstanding at June 30, 2026 | 9,999,999 | $ 3.00", "sectionHeading": "Stock-Based Compensation", "documentUrl": "https://www.sec.gov/Archives/x.htm", "filingDate": "2026-08-06", "accessionNumber": None, "inTable": True, "tableTitle": "Stock option activity", "rowLabel": "Outstanding at June 30, 2026"},
    {"contextText": "Unvested at June 30, 2026, restricted stock units totalled 8,888,888 shares.", "sectionHeading": "Stock-Based Compensation", "documentUrl": "https://www.sec.gov/Archives/x.htm", "filingDate": "2026-08-06", "accessionNumber": None, "inTable": False, "tableTitle": None, "rowLabel": None},
]

FIXTURES = {"ten_k": TEN_K, "ten_q": TEN_Q, "awards_q": AWARDS_Q, "aaoi_q": AAOI_Q, "bare_q": BARE_Q, "asts_q": ASTS_Q, "debt_free_k": DEBT_FREE_K, "overlap_q": OVERLAP_Q, "aggregate_q": AGGREGATE_Q, "vrt_q": VRT_Q, "parts_q": PARTS_Q}

NEWS_ITEMS = [
    {"title": "Needham raises IQE price target to 45p from 38p", "summary": "Needham values IQE at 12x 2027 EV/EBITDA, citing gallium nitride demand.", "url": "https://news.example/1", "publishedAt": "2026-09-20T08:00:00Z", "source": "yahoo_finance_news"},
    {"title": "Sivers Semiconductors: Carnegie sees upside", "summary": "Carnegie's DCF uses a WACC of 11.5% and 2% terminal growth, giving a target price of SEK 12.", "url": "https://news.example/2", "publishedAt": "2026-09-18T08:00:00Z", "originalSource": "Placera"},
    {"title": "MACOM price target raised to $210 from $180 at Morgan Stanley", "summary": "", "url": "https://news.example/3", "publishedAt": "2026-09-17T08:00:00Z"},
    {"title": "Rosenblatt's $60 target is based on 41x 2H27-1H28 EBITDA", "summary": "The benchmark index fell. Revenue grew 3 times faster than peers.", "url": "https://news.example/4", "publishedAt": "2026-09-16T08:00:00Z"},
    {"title": "Needham raises IQE price target to 45p from 38p", "summary": "duplicate", "url": "https://news.example/1"},
    {"title": "Analysts see a sum-of-the-parts discount at Liberum", "summary": "Liberum uses a sum-of-the-parts valuation.", "url": "https://news.example/5", "publishedAt": "2026-09-15T08:00:00Z"},
    # 2.3.0 read this as a $5 target (2.3.1).
    {"title": "AST SpaceMobile Soars 12% on Berenberg\u2019s $92 Price Target, Planet Labs Climbs 5%", "summary": "", "url": "https://news.example/6", "publishedAt": "2026-09-02T14:20:15Z"},
]
RATING_CHANGES = [
    {"date": "2026-09-20", "firm": "Needham", "toGrade": "Buy", "ptTo": 45, "ptFrom": 38},
    {"date": "2026-09-01", "firm": "Jefferies", "toGrade": "Hold", "ptTo": 52, "ptFrom": 0},
    {"date": "2026-08-30", "firm": "Nomura", "toGrade": "Buy", "ptTo": 0, "ptFrom": 0},
]

K_URL = f"{ARCHIVE}/000123456825000010/cstc-20241231.htm"
Q_URL = f"{ARCHIVE}/000123456825000020/cstc-20250331.htm"


def _source(role: str, filing_type: str, filing_date: str, accession: str, url: str, doc):
    return {"role": role, "filingType": filing_type, "filingDate": filing_date, "accessionNumber": accession, "documentUrl": url, "doc": doc}


_WORKER_PURE = r"""
import fs from "node:fs";
const [bundleUrl, dataPath] = process.argv.slice(-2);
const m = await import(bundleUrl);
const data = JSON.parse(fs.readFileSync(dataPath, "utf8"));
const docs = {};
const out = { documents: {} };
for (const [name, html] of Object.entries(data.fixtures)) {
  const doc = m.parseIxbrl(html);
  docs[name] = doc;
  out.documents[name] = doc;
}
const src = (role, type, date, acc, url, name) => ({ role, filingType: type, filingDate: date, accessionNumber: acc, documentUrl: url, doc: docs[name] });
const kSource = src("primary", "10-K", "2025-02-20", "0001234568-25-000010", data.kUrl, "ten_k");
const qSource = src("primary", "10-Q", "2025-05-08", "0001234568-25-000020", data.qUrl, "ten_q");
const kFallback = { ...kSource, role: "latest_annual_fallback" };
const atm = data.atm.map((t) => ({ contextText: t, sectionHeading: "Liquidity", documentUrl: data.qUrl, filingDate: "2025-05-08", accessionNumber: null }));
out.bridge = m.dilutionBridge({ ticker: "CSTC", price: 25, priceCurrency: "USD", asOfDate: "2025-05-09", sources: [qSource, kFallback], atmMatches: atm });
out.bridgeLow = m.dilutionBridge({ ticker: "CSTC", price: 10, priceCurrency: "USD", asOfDate: null, sources: [kSource], atmMatches: [] });
out.bridgeAaoi = m.dilutionBridge({ ticker: "AAOX", price: 30, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-Q", "2026-08-06", "0001234568-26-000040", data.qUrl, "aaoi_q")], atmMatches: [] });
out.bridgeTable = m.dilutionBridge({ ticker: "BARE", price: 30, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-Q", "2026-08-06", "0001234568-26-000050", data.qUrl, "bare_q")], atmMatches: [], awardTableMatches: data.tableMatches });
out.bridgeAwards = m.dilutionBridge({ ticker: "CSTC", price: 25, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-Q", "2025-08-06", "0001234568-25-000030", data.qUrl, "awards_q")], atmMatches: [] });
out.capital = m.capitalStructure({ ticker: "CSTC", source: kSource, fundingMatches: atm });
out.capitalAaoi = m.capitalStructure({ ticker: "AAOX", source: src("primary", "10-Q", "2026-08-06", "0001234568-26-000040", data.qUrl, "aaoi_q"), fundingMatches: [] });
out.capitalAsts = m.capitalStructure({ ticker: "ASTX", source: src("primary", "10-Q", "2026-08-10", "0001234568-26-000060", data.qUrl, "asts_q"), fundingMatches: [] });
out.capitalDebtFree = m.capitalStructure({ ticker: "AEHX", source: src("primary", "10-K", "2026-07-27", "0001234568-26-000070", data.kUrl, "debt_free_k"), fundingMatches: [] });
out.capitalOverlap = m.capitalStructure({ ticker: "OVLP", source: src("primary", "10-Q", "2026-08-10", "0001234568-26-000080", data.qUrl, "overlap_q"), fundingMatches: [] });
out.capitalAggregate = m.capitalStructure({ ticker: "AGGR", source: src("primary", "10-Q", "2026-08-10", "0001234568-26-000090", data.qUrl, "aggregate_q"), fundingMatches: [] });
out.capitalVrt = m.capitalStructure({ ticker: "VRTX", source: src("primary", "10-Q", "2026-07-29", "0001234568-26-000100", data.qUrl, "vrt_q"), fundingMatches: [] });
out.capitalParts = m.capitalStructure({ ticker: "PRTS", source: src("primary", "10-Q", "2026-07-29", "0001234568-26-000110", data.qUrl, "parts_q"), fundingMatches: [] });
out.labels = ["us-gaap:ClassBCommonStockMember", "us-gaap:RestrictedStockUnitsRSUMember", "cstc:ConvertibleSeniorNotesDue2029Member", "aaoi:SubsidiaryOfAmazonMember"].map(m.memberLabel);
out.analyst = m.analystValuationMethods("IQE.L", data.news, data.changes);
out.ch = m.companiesHouseFilings("01234567", data.chHistory);
out.chPick = m.pickCompaniesHouseMatch("IQE plc", data.chSearch);
console.log(JSON.stringify(out));
"""

ATM_TEXT = [
    "In May 2024, we entered into a sales agreement for an at-the-market offering program to sell up to $200.0 million of our common stock. During 2024, we sold 2,000,000 shares for net proceeds of $48.5 million, and $150.0 million remained available under the ATM program as of December 31, 2024.",
    "We believe our existing cash will be sufficient to fund our operations for at least the next twelve months. We expect capital expenditures of approximately $40 million in 2025, which we expect to be fully funded by cash on hand.",
    # Not funding statements (2.3.1): revenue recognition, stock-award valuation, a boilerplate list item.
    "The Company expects to recognize approximately 6.6% of its remaining performance obligations as revenue over the next 12 months. For these analyses, the Company selects companies with historical share price information sufficient to meet the expected life of the stock-based awards. Other factors include our ability to raise funds to finance operating expenses and capital expenditures;",
]

CH_HISTORY = {"total_count": 2, "items": [
    {"date": "2026-06-30", "category": "capital", "type": "SH01", "description": "capital-allotment-shares", "description_values": {"date": "2026-06-20", "capital": [{"figure": "1,200,000", "currency": "GBP"}]}, "pages": 3, "transaction_id": "MzQ1Njc4OTAx", "links": {"document_metadata": "https://document-api.company-information.service.gov.uk/document/abc123"}},
    {"date": "2026-05-12", "category": "mortgage", "type": "MR01", "description": "mortgage-create-with-deed-with-charge-number-charge-creation-date", "description_values": {"charge_number": "026013160005"}, "transaction_id": "MzQ1Njc4OTAy", "links": {}},
]}
CH_SEARCH = {"items": [
    {"title": "IQE HOLDINGS LIMITED", "company_number": "09999999", "company_status": "dissolved", "address_snippet": "Cardiff"},
    {"title": "IQE PLC", "company_number": "01234567", "company_status": "active", "address_snippet": "Pascal Close, Cardiff"},
    {"title": "IQE SILICON COMPOUNDS LIMITED", "company_number": "02222222", "company_status": "active", "address_snippet": "Cardiff"},
]}
CH_CHARGES = {"items": [{"charge_code": "026013160005", "status": "outstanding", "created_on": "2026-05-01", "delivered_on": "2026-05-12", "classification": {"type": "charge-description", "description": "A registered charge"}, "persons_entitled": [{"name": "Lender Bank PLC"}], "particulars": {"description": "Fixed and floating charge over all assets."}}]}


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
        bundle = tmp_path / "bundle.mjs"
        subprocess.run(
            [str(ESBUILD), str(WORKER / "src" / "capital-structure.ts"), "--bundle", "--format=esm", "--platform=neutral",
             f"--outfile={bundle}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
        (tmp_path / "data.json").write_text(json.dumps({
            "fixtures": FIXTURES, "kUrl": K_URL, "qUrl": Q_URL, "atm": ATM_TEXT, "news": NEWS_ITEMS, "changes": RATING_CHANGES,
            "chHistory": CH_HISTORY, "chSearch": CH_SEARCH, "tableMatches": TABLE_MATCHES,
        }), encoding="utf-8")
        (tmp_path / "harness.mjs").write_text(_WORKER_PURE, encoding="utf-8")
        result = subprocess.run(
            [node, str(tmp_path / "harness.mjs"), bundle.as_uri(), str(tmp_path / "data.json")],
            check=True, capture_output=True, text=True, timeout=120,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


def _doc_json(doc: cs.IxDocument) -> dict:
    return {
        "facts": [{
            "name": f.name, "local": f.local, "contextRef": f.context_ref, "unit": f.unit, "value": f.value, "text": f.text,
            "periodEnd": f.period_end, "periodStart": f.period_start, "dims": f.dims, "decimals": f.decimals, "sentence": f.sentence, "order": f.order,
        } for f in doc.facts],
        "contextCount": doc.context_count,
        "documentPeriodEnd": doc.document_period_end,
        "documentType": doc.document_type,
    }


def _python_pure() -> dict:
    docs = {name: cs.parse_ixbrl(html) for name, html in FIXTURES.items()}
    k_source = cs.IxSource("primary", "10-K", "2025-02-20", "0001234568-25-000010", K_URL, docs["ten_k"])
    q_source = cs.IxSource("primary", "10-Q", "2025-05-08", "0001234568-25-000020", Q_URL, docs["ten_q"])
    k_fallback = cs.IxSource("latest_annual_fallback", "10-K", "2025-02-20", "0001234568-25-000010", K_URL, docs["ten_k"])
    atm = [cs.TextMatch(t, "Liquidity", Q_URL, "2025-05-08", None) for t in ATM_TEXT]
    return {
        "documents": {name: _doc_json(doc) for name, doc in docs.items()},
        "bridge": cs.dilution_bridge("CSTC", 25, "USD", "2025-05-09", [q_source, k_fallback], atm),
        "bridgeLow": cs.dilution_bridge("CSTC", 10, "USD", None, [k_source], []),
        "bridgeAaoi": cs.dilution_bridge("AAOX", 30, "USD", None, [cs.IxSource("primary", "10-Q", "2026-08-06", "0001234568-26-000040", Q_URL, docs["aaoi_q"])], []),
        "bridgeTable": cs.dilution_bridge("BARE", 30, "USD", None, [cs.IxSource("primary", "10-Q", "2026-08-06", "0001234568-26-000050", Q_URL, docs["bare_q"])], [], [
            cs.TextMatch(t["contextText"], t["sectionHeading"], t["documentUrl"], t["filingDate"], t["accessionNumber"], t["inTable"], t["tableTitle"], t["rowLabel"]) for t in TABLE_MATCHES
        ]),
        "bridgeAwards": cs.dilution_bridge("CSTC", 25, "USD", None, [cs.IxSource("primary", "10-Q", "2025-08-06", "0001234568-25-000030", Q_URL, docs["awards_q"])], []),
        "capital": cs.capital_structure("CSTC", k_source, atm),
        "capitalAaoi": cs.capital_structure("AAOX", cs.IxSource("primary", "10-Q", "2026-08-06", "0001234568-26-000040", Q_URL, docs["aaoi_q"]), []),
        "capitalAsts": cs.capital_structure("ASTX", cs.IxSource("primary", "10-Q", "2026-08-10", "0001234568-26-000060", Q_URL, docs["asts_q"]), []),
        "capitalDebtFree": cs.capital_structure("AEHX", cs.IxSource("primary", "10-K", "2026-07-27", "0001234568-26-000070", K_URL, docs["debt_free_k"]), []),
        "capitalOverlap": cs.capital_structure("OVLP", cs.IxSource("primary", "10-Q", "2026-08-10", "0001234568-26-000080", Q_URL, docs["overlap_q"]), []),
        "capitalAggregate": cs.capital_structure("AGGR", cs.IxSource("primary", "10-Q", "2026-08-10", "0001234568-26-000090", Q_URL, docs["aggregate_q"]), []),
        "capitalVrt": cs.capital_structure("VRTX", cs.IxSource("primary", "10-Q", "2026-07-29", "0001234568-26-000100", Q_URL, docs["vrt_q"]), []),
        "capitalParts": cs.capital_structure("PRTS", cs.IxSource("primary", "10-Q", "2026-07-29", "0001234568-26-000110", Q_URL, docs["parts_q"]), []),
        "labels": [cs.member_label(m) for m in ("us-gaap:ClassBCommonStockMember", "us-gaap:RestrictedStockUnitsRSUMember", "cstc:ConvertibleSeniorNotesDue2029Member", "aaoi:SubsidiaryOfAmazonMember")],
        "analyst": cs.analyst_valuation_methods("IQE.L", copy.deepcopy(NEWS_ITEMS), copy.deepcopy(RATING_CHANGES)),
        "ch": cs.companies_house_filings("01234567", CH_HISTORY),
        "chPick": cs.pick_companies_house_match("IQE plc", CH_SEARCH),
    }


class TestCapitalStructureParity(unittest.TestCase):
    """Both runtimes parse and compute the same results."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.worker = _worker_pure()
        cls.local = _python_pure()

    def test_documents_match(self) -> None:
        for name in FIXTURES:
            self.assertEqual(self.worker["documents"][name], self.local["documents"][name], name)

    def test_outputs_match(self) -> None:
        for key in ("bridge", "bridgeLow", "bridgeAwards", "bridgeAaoi", "bridgeTable", "capital", "capitalAaoi", "capitalAsts", "capitalDebtFree", "capitalOverlap", "capitalAggregate", "capitalVrt", "capitalParts", "labels", "analyst", "ch", "chPick"):
            self.assertEqual(self.worker[key], self.local[key], key)


class TestCapitalStructureValues(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_pure()

    def fact(self, doc: str, local: str, **dims):
        return [f for f in self.out["documents"][doc]["facts"] if f["local"] == local and all(f["dims"].get(k) == v for k, v in dims.items())]

    def test_parser_reads_scale_sign_formats_dimensions_and_dates(self) -> None:
        k = self.out["documents"]["ten_k"]
        self.assertEqual(k["documentPeriodEnd"], "2024-12-31")
        self.assertEqual(k["documentType"], "10-K")
        self.assertEqual(self.fact("ten_k", "CashAndCashEquivalentsAtCarryingValue")[0]["value"], 150_000_000)
        self.assertEqual(self.fact("ten_k", "SegmentReportingNumberOfSegments")[0]["value"], 2)
        self.assertEqual(self.fact("ten_k", "LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFour")[0]["value"], 0)
        self.assertEqual(self.fact("ten_k", "Goodwill"), [])
        coupon = self.fact("ten_k", "DebtInstrumentInterestRateStatedPercentage", DebtInstrumentAxis=TERM)[0]
        self.assertAlmostEqual(coupon["value"], 0.0725)
        self.assertEqual(coupon["unit"], "pure")
        strike = self.fact("ten_k", "ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", ClassOfWarrantOrRightAxis="cstc:PublicWarrantsMember")[0]
        self.assertEqual(strike["unit"], "USD/shares")
        maturity = self.fact("ten_k", "DebtInstrumentMaturityDate", DebtInstrumentAxis=TERM)[0]
        self.assertEqual(maturity["text"], "June 30, 2027")
        self.assertEqual(maturity["periodStart"], "2024-01-01")

    def test_bridge_at_supplied_price(self) -> None:
        b = self.out["bridge"]
        self.assertEqual(b["status"], "PARTIAL")
        self.assertEqual(b["decisionUse"], "MECHANICAL_NOT_CONSENSUS")
        self.assertEqual(b["price"], {"amount": 25, "currency": "USD", "asOfDate": "2025-05-09", "source": "caller_supplied"})
        self.assertEqual(b["basicShares"]["shares"], 101_000_000)
        self.assertEqual(b["basicShares"]["source"]["filingType"], "10-Q")
        bridge = b["bridge"]
        # Options by exercise-price range: 2.0M x (1 - 5/25) + 3.0M x (1 - 18/25).
        self.assertEqual(bridge["stockOptions"], 2_440_000)
        # RSUs come from the 10-Q; everything else falls back to the 10-K.
        self.assertEqual(bridge["unvestedShareAwards"], 1_800_000)
        # 4.0M x (1 - 11.5/25) + 1.0M x (1 - 0.0001/25); Series B has no tagged strike.
        self.assertEqual(bridge["warrants"], 3_159_996)
        # $300M / 1000 x 50 = 15M shares; implied conversion price $20 <= $25.
        self.assertEqual(bridge["convertibleDebt"], 15_000_000)
        self.assertEqual(bridge["dilutedSharesAtPrice"], 123_399_996)
        self.assertEqual(bridge["dilutionPctAtPrice"], 22.18)
        self.assertEqual(bridge["atmPotentialShares"], 6_000_000)
        self.assertEqual(b["partiallyResolved"], ["warrants"])
        self.assertEqual(b["notDisclosed"], [])
        convs = next(c for c in b["components"] if c["component"] == "convertible_debt")["instruments"]
        conv = convs[0]
        self.assertEqual(conv["conversionPrice"], 20)
        self.assertEqual(conv["maturityDate"], "2029-03-15")
        self.assertEqual((conv["principal"], conv["principalBasis"]), (300_000_000, "face_amount"))
        # No face amount: the issue-date carrying amount stands in, and says so.
        conv31 = convs[1]
        self.assertEqual((conv31["faceAmount"], conv31["principal"], conv31["principalConcept"], conv31["principalDate"], conv31["principalBasis"]),
                         (None, 200_000_000, "us-gaap:LongTermDebt", "2024-06-01", "tagged_amount_fallback"))
        self.assertEqual((conv31["ifConvertedShares"], conv31["inTheMoney"], conv31["incrementalShares"]), (5_000_000, False, 0))
        self.assertEqual(len(convs), 2, "the coupon-only duplicate member has no conversion terms")
        self.assertNotIn("taggedDilutionConcepts", b)
        atm = b["atmProgram"]
        self.assertEqual(atm["remainingCapacityUsd"], 150_000_000)
        self.assertEqual(atm["programSizeUsd"], 200_000_000)

    def test_out_of_the_money_instruments_add_nothing(self) -> None:
        b = self.out["bridgeLow"]
        self.assertEqual(b["basicShares"]["shares"], 100_000_000)
        self.assertEqual([c["class"] for c in b["basicShares"]["classes"]], ["Common Class A", "Common Class B"])
        bridge = b["bridge"]
        self.assertEqual(bridge["stockOptions"], 1_000_000)  # only the $5 range is in the money at $10
        self.assertEqual(bridge["unvestedShareAwards"], 2_000_000)  # 10-K RSU + PSU by award type
        self.assertEqual(bridge["convertibleDebt"], 0)
        self.assertEqual(bridge["atmPotentialShares"], None)
        self.assertEqual(b["notDisclosed"], ["atm_program"])

    def test_rounded_narrative_facts_do_not_become_balances(self) -> None:
        # 2.4.2: ASTS tagged "approximately $2.3 billion ... classified as cash
        # equivalents" as ShortTermInvestments; counting it doubled cash.
        cash = self.fact("asts_q", "CashAndCashEquivalentsAtCarryingValue")
        self.assertEqual([f["decimals"] for f in cash], [-8, -3])
        c = self.out["capitalAsts"]
        bal = c["balances"]
        self.assertEqual(bal["cashAndEquivalents"], 2_288_253_000)
        self.assertIsNone(bal["shortTermInvestments"])
        self.assertEqual(bal["totalDebt"], 2_971_916_000)
        self.assertEqual(bal["netCash"], -683_663_000)
        codes = [w["code"] for w in c["warnings"]]
        self.assertEqual(codes, ["ROUNDED_FACT_IGNORED"])
        self.assertIn("ShortTermInvestments 2300000000", c["warnings"][0]["message"])
        # Precise facts are unaffected: the base fixture keeps its investments.
        self.assertEqual(self.out["capital"]["warnings"], [])

    def test_investments_described_as_cash_equivalents_are_not_added(self) -> None:
        sti = self.fact("overlap_q", "ShortTermInvestments")
        self.assertEqual(sti[0]["sentence"], "As of June 30, 2026, 180,000 of our cash was held in money market funds classified as cash equivalents.")
        self.assertIsNone(sti[1]["sentence"])  # a balance-sheet table row
        self.assertIsNone(self.fact("overlap_q", "LongTermDebtNoncurrent")[0]["sentence"])  # only investment concepts keep a sentence
        c = self.out["capitalOverlap"]
        self.assertEqual(c["balances"]["shortTermInvestments"], 120_000_000)
        self.assertEqual(c["balances"]["netCash"], 470_000_000)
        self.assertEqual([w["code"] for w in c["warnings"]], ["OVERLAPS_CASH_EQUIVALENTS"])
        self.assertIn("money market funds", c["warnings"][0]["sentence"])

    def test_filing_aggregate_equal_to_cash_drops_investments(self) -> None:
        c = self.out["capitalAggregate"]
        self.assertIsNone(c["balances"]["shortTermInvestments"])
        self.assertEqual(c["balances"]["netCash"], 490_000_000)
        self.assertEqual([(w["code"], w["severity"]) for w in c["warnings"]], [("CASH_AGGREGATE_MISMATCH", "info")])

    def test_held_to_maturity_treasury_bills_are_short_term_investments(self) -> None:
        bal = self.out["capitalVrt"]["balances"]
        self.assertEqual((bal["cashAndEquivalents"], bal["shortTermInvestments"], bal["currency"]), (2_810_600_000, 300_000_000, "USD"))
        self.assertEqual(bal["shortTermInvestmentsConcept"], "us-gaap:DebtSecuritiesHeldToMaturityAmortizedCostAfterAllowanceForCreditLossCurrent")
        self.assertEqual(bal["netCash"], 2_810_600_000 + 300_000_000 - 2_939_800_000)
        self.assertEqual(self.out["capitalVrt"]["warnings"], [])
        parts = self.out["capitalParts"]["balances"]
        self.assertEqual(parts["shortTermInvestments"], 100_000_000)
        self.assertEqual(parts["shortTermInvestmentsConcept"], "us-gaap:AvailableForSaleSecuritiesDebtSecuritiesCurrent + us-gaap:HeldToMaturitySecuritiesCurrent")

    def test_filing_without_borrowings_reports_zero_debt(self) -> None:
        c = self.out["capitalDebtFree"]
        self.assertEqual(c["status"], "COMPUTED")
        bal = c["balances"]
        self.assertEqual(bal["totalDebt"], 0)
        self.assertEqual(bal["totalDebtBasis"], "No borrowing concepts tagged in the filing")
        self.assertEqual(bal["netCash"], 116_358_000)
        self.assertEqual([w["code"] for w in c["warnings"]], ["NO_BORROWINGS_TAGGED"])
        # A filing whose only tagged facts are shares says nothing about debt.
        self.assertTrue(cs._tags_no_borrowings(cs.parse_ixbrl(BARE_Q)))
        self.assertFalse(cs._tags_no_borrowings(cs.parse_ixbrl(AAOI_Q)))

    def test_capital_structure(self) -> None:
        c = self.out["capital"]
        self.assertEqual(c["status"], "COMPUTED")
        self.assertEqual(c["periodEnd"], "2024-12-31")
        bal = c["balances"]
        self.assertEqual(bal["cashAndEquivalents"], 150_000_000)
        self.assertEqual(bal["totalDebt"], 385_000_000)
        self.assertEqual(bal["netCash"], -185_000_000)
        rows = {r["member"]: r for r in c["instruments"]}
        self.assertEqual(rows[TERM]["couponPct"], 7.25)
        self.assertEqual(rows[TERM]["maturityDate"], "2027-06-30")
        self.assertEqual(rows[TERM]["status"], "outstanding_at_period_end")
        self.assertEqual(rows[CONV]["couponPct"], 0.375)
        self.assertTrue(rows[CONV]["convertible"])
        self.assertEqual(rows[OLD]["status"], "matured_before_period_end")
        # The coupon-only duplicate member is dropped (2.3.1).
        self.assertEqual([r["member"] for r in c["instruments"]], [OLD, TERM, CONV, CONV31])
        self.assertEqual((rows[CONV31]["carryingAmount"], rows[CONV31]["taggedAmount"], rows[CONV31]["taggedAmountDate"]), (None, 200_000_000, "2024-06-01"))
        self.assertEqual(rows[TERM]["carryingAmountConcept"], "us-gaap:LongTermDebt")
        self.assertEqual([(y["year"], y["faceAmount"]) for y in c["instrumentMaturitiesByYear"]],
                         [("2027", 100_000_000), ("2029", 300_000_000), ("2031", 200_000_000)])
        ladder = {r["bucket"]: r for r in c["maturityLadder"]}
        self.assertEqual(ladder["year_3"]["periodThrough"], "2027-12-31")
        self.assertEqual(ladder["year_4"]["amount"], 0)
        categories = [s["categories"] for s in c["fundingStatements"]]
        self.assertIn(["liquidity_sufficiency"], categories)
        self.assertIn(["atm_program"], categories)
        self.assertTrue(any("capital_expenditure" in cats and "liquidity_sufficiency" in cats for cats in categories))
        statements = " ".join(s["statement"] for s in c["fundingStatements"])
        for noise in ("performance obligations", "expected life", "operating expenses and capital expenditures;"):
            self.assertNotIn(noise, statements)

    def test_awards_warrants_and_eps_tagged_the_aaoi_way(self) -> None:
        b = self.out["bridgeAwards"]
        awards = next(c for c in b["components"] if c["component"] == "unvested_share_awards")
        # The plan total (one axis) is used, not added to its type x plan split.
        self.assertEqual((awards["unvested"], awards["breakdown"]), (1_000_000, [{"awardType": "Plan 2020", "unvested": 1_000_000}]))
        warrants = next(c for c in b["components"] if c["component"] == "warrants")
        self.assertEqual(warrants["classes"][0]["concept"], "us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights")
        self.assertEqual(warrants["incrementalShares"], 1_200_000)  # 2.0M x (1 - 10/25)
        self.assertEqual(b["bridge"]["dilutedSharesAtPrice"], 52_200_000)
        eps = b["reportedEpsDilution"]
        self.assertEqual((eps["periodStart"], eps["periodEnd"], eps["weightedBasicShares"], eps["weightedDilutedShares"]),
                         ("2025-04-01", "2025-06-30", 49_000_000, 49_000_000))
        self.assertEqual(eps["antidilutiveExcluded"], [{"security": "Restricted Stock Units RSU", "shares": 900_000}])
        self.assertEqual(b["notDisclosed"], ["stock_options", "convertible_debt", "atm_program"])
        self.assertIn("us-gaap:AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount [AntidilutiveSecuritiesAxis]", b["taggedDilutionConcepts"])

    def test_company_prefixed_awards_vesting_warrant_and_other_antidilutive_axis(self) -> None:
        b = self.out["bridgeAaoi"]
        awards = next(c for c in b["components"] if c["component"] == "unvested_share_awards")
        self.assertEqual((awards["unvested"], awards["countBasis"]), (3_000_000, "vested_and_expected_to_vest"))
        self.assertTrue(awards["concept"].startswith("aaoi:"))
        warrant = next(c for c in b["components"] if c["component"] == "warrants")["classes"][0]
        # 7,945,399 called for, 5,000,000 still unvested: only 2,945,399 are exercisable.
        self.assertEqual((warrant["outstanding"], warrant["unvested"], warrant["exercisable"]), (7_945_399, 5_000_000, 2_945_399))
        self.assertEqual(warrant["incrementalShares"], cs.round_half_up(2_945_399 * (1 - 23.6956 / 30)))
        self.assertEqual(b["reportedEpsDilution"]["antidilutiveExcluded"], [{"security": "Restricted Stock Units RSU", "shares": 1_100_000}])
        self.assertEqual(b["bridge"]["grossSharesAllInstruments"], 84_000_000 + 3_000_000 + 7_945_399)

    def test_separately_reported_convertible_notes_are_debt(self) -> None:
        bal = self.out["capitalAaoi"]["balances"]
        # Bank loans $58.9M plus the $124.9M convertible line AAOI reports on its own (2.4.1).
        self.assertEqual(bal["totalDebt"], 57_258_000 + 1_657_000 + 124_900_000)
        self.assertIn("separately reported convertible notes", bal["totalDebtBasis"])
        self.assertEqual([c["concept"] for c in bal["totalDebtComponents"]],
                         ["us-gaap:LongTermDebtCurrent", "us-gaap:LongTermDebtNoncurrent", "us-gaap:ConvertibleNotesPayable"])
        # A convertible line smaller than the long-term debt it sits inside is not added again.
        self.assertEqual(self.out["capital"]["balances"]["totalDebt"], 385_000_000)

    def test_member_labels_split_single_letter_classes(self) -> None:
        self.assertEqual(self.out["labels"], ["Class B Common Stock", "Restricted Stock Units RSU", "Convertible Senior Notes Due 2029", "Subsidiary Of Amazon"])

    def test_award_count_from_the_filing_table(self) -> None:
        awards = next(c for c in self.out["bridgeTable"]["components"] if c["component"] == "unvested_share_awards")
        # The latest "Unvested at" row of the RSU table; the option table and prose are ignored.
        self.assertEqual((awards["unvested"], awards["countBasis"], awards["source"]["periodEnd"]), (2_750_000, "filing_table_text", "2026-06-30"))
        self.assertIn("2,750,000", awards["evidence"]["row"])

    def test_analyst_methods(self) -> None:
        a = self.out["analyst"]
        self.assertEqual(a["status"], "FOUND")
        self.assertEqual(a["decisionUse"], "CONTEXT_ONLY")
        self.assertEqual(a["itemsScanned"], 6)
        by_url = {}
        for e in a["evidence"]:
            by_url.setdefault(e["url"], []).append(e)
        needham = by_url["https://news.example/1"]
        self.assertEqual(needham[0]["priceTarget"], {"target": 45, "prior": 38, "currency": "GBp"})
        self.assertEqual(needham[1]["methods"], [{"method": "multiple", "multiple": 12, "metric": "EBITDA", "enterpriseValue": True, "periods": ["2027"]}])
        carnegie = by_url["https://news.example/2"][0]
        self.assertEqual(carnegie["firm"], "Carnegie")
        self.assertEqual(carnegie["methods"][0], {"method": "DCF", "waccPct": 11.5, "discountRatePct": None, "terminalGrowthPct": 2, "exitMultiple": None})
        self.assertEqual(carnegie["priceTarget"], {"target": 12, "prior": None, "currency": "SEK"})
        rosenblatt = by_url["https://news.example/4"]
        self.assertEqual(len(rosenblatt), 1, "lowercase 'benchmark' and '3 times faster' are not evidence")
        self.assertEqual(rosenblatt[0]["methods"][0]["periods"], ["2H27", "1H28"])
        self.assertEqual(rosenblatt[0]["priceTarget"], {"target": 60, "prior": None, "currency": "USD"})
        # "$92 Price Target, ... Climbs 5%" is a $92 target, not $5 (2.3.1).
        self.assertEqual(by_url["https://news.example/6"][0]["priceTarget"], {"target": 92, "prior": None, "currency": "USD"})
        self.assertEqual({(m["firm"], m["source"]) for m in a["methodNotDisclosed"]},
                         {("Morgan Stanley", "news"), ("Berenberg", "news"), ("Jefferies", "rating_changes")})
        self.assertEqual(a["methodCounts"], {"multiple": 2, "DCF": 1, "sum_of_the_parts": 2})

    def test_companies_house_rows(self) -> None:
        rows = self.out["ch"]
        self.assertEqual(rows[0]["type"], "SH01")
        self.assertEqual(rows[0]["categoryLabel"], "Share capital (allotments, buybacks)")
        self.assertEqual(rows[0]["documentContentUrl"], "https://document-api.company-information.service.gov.uk/document/abc123/content")
        self.assertIn("/company/01234567/filing-history/MzQ1Njc4OTAx/document", rows[0]["viewerUrl"])
        self.assertIsNone(rows[1]["documentMetadataUrl"])
        self.assertEqual(self.out["chPick"]["company_number"], "01234567")


# ── End to end ──────────────────────────────────────────────────────────────

SUBMISSIONS = {"cik": str(CIK), "filings": {"recent": {
    "form": ["10-Q", "8-K", "10-K"],
    "accessionNumber": ["0001234568-25-000020", "0001234568-25-000015", "0001234568-25-000010"],
    "primaryDocument": ["cstc-20250331.htm", "cstc-8k.htm", "cstc-20241231.htm"],
    "filingDate": ["2025-05-08", "2025-03-01", "2025-02-20"],
    "reportDate": ["2025-03-31", "", "2024-12-31"],
    "acceptanceDateTime": ["", "", ""],
}}}
DOCUMENTS = {K_URL: TEN_K, Q_URL: TEN_Q}
CH_KEY = "test-ch-key"
CH_RESPONSES = {
    "/search/companies": CH_SEARCH,
    "/company/01234567": {"company_name": "IQE PLC", "company_status": "active"},
    "/company/01234567/filing-history": CH_HISTORY,
    "/company/01234567/charges": CH_CHARGES,
}

CALLS = {
    "bridge": ("sec_extractors", "extract_dilution_bridge", {"ticker": "CSTC", "price": 25, "as_of_date": "2025-05-09"}),
    "bridge_annual": ("sec_extractors", "extract_dilution_bridge", {"ticker": "CSTC", "price": 25, "filing_type": "10-K", "include_atm": False}),
    "capital": ("sec_extractors", "extract_capital_structure", {"ticker": "CSTC", "filing_type": "10-K"}),
    "capital_latest": ("sec_extractors", "extract_capital_structure", {"ticker": "CSTC", "include_funding_statements": False}),
    "uk_by_name": ("news_events", "get_uk_company_filings", {"company_name": "IQE plc", "category": "capital", "limit": 10}),
    "uk_by_number": ("news_events", "get_uk_company_filings", {"company_number": "1234567", "include_charges": True}),
}

_WORKER_E2E = r"""
const [miniflareUrl, scriptPath, dataPath] = process.argv.slice(-3);
const { Miniflare, convertV4MiniflareOptions } = await import(miniflareUrl);
const fs = await import("node:fs");
const data = JSON.parse(fs.readFileSync(dataPath, "utf8"));
const chRequests = [];
async function outbound(req) {
  const u = new URL(req.url);
  const url = `${u.origin}${u.pathname}`;
  if (u.hostname === "www.sec.gov" && u.pathname === "/files/company_tickers.json") {
    return Response.json({ "0": { cik_str: data.cik, ticker: "CSTC", title: "CS Test Co" } });
  }
  if (u.hostname === "data.sec.gov" && u.pathname === `/submissions/CIK${String(data.cik).padStart(10, "0")}.json`) {
    return Response.json(data.submissions);
  }
  if (data.documents[url] !== undefined) return new Response(data.documents[url], { headers: { "content-type": "text/html" } });
  if (u.hostname === "api.company-information.service.gov.uk") {
    chRequests.push({ path: u.pathname, query: u.search, auth: req.headers.get("authorization") });
    const body = data.ch[u.pathname];
    return body ? Response.json(body) : new Response("{}", { status: 404 });
  }
  return new Response("Not Found", { status: 404 });
}
async function run(bindings, calls) {
  const mf = new Miniflare(convertV4MiniflareOptions({ workers: [{
    name: "w", modules: true, scriptPath, compatibilityDate: "2026-09-21", compatibilityFlags: ["nodejs_als"],
    bindings: { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true", ...bindings }, outboundService: outbound,
  }] }));
  const worker = await mf.getWorker("w");
  const out = {};
  let id = 0;
  for (const [name, [group, action, params]] of Object.entries(calls)) {
    const res = await worker.fetch("https://x/mcp", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: ++id, method: "tools/call", params: { name: group, arguments: { action, params } } }),
    });
    const envelope = (await res.json()).result.structuredContent;
    out[name] = { data: envelope.data, error: envelope.error };
  }
  await mf.dispose();
  return out;
}
const out = await run({ COMPANIES_HOUSE_API_KEY: data.chKey }, data.calls);
const bare = await run({}, { uk_unconfigured: ["news_events", "get_uk_company_filings", { company_number: "01234567" }] });
out.uk_unconfigured = bare.uk_unconfigured;
out.chRequests = chRequests;
console.log(JSON.stringify(out));
"""


@functools.cache
def _worker_e2e() -> dict:
    node = _node()
    if not MINIFLARE.exists():
        raise unittest.SkipTest("worker/node_modules (npm ci) is required")
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".capital-structure-test-") as tmp:
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
            "cik": CIK, "submissions": SUBMISSIONS, "documents": DOCUMENTS, "calls": CALLS, "ch": CH_RESPONSES, "chKey": CH_KEY,
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
    srv._IXBRL_DOCUMENTS.clear()
    ch_requests: list[str] = []

    async def fake_html(url: str, max_bytes: int = 5_000_000) -> str | None:
        return DOCUMENTS.get(url)

    async def fake_ch(path: str, key: str):
        ch_requests.append(path)
        body = CH_RESPONSES.get(path.split("?", 1)[0])
        return (200, copy.deepcopy(body)) if body is not None else (404, None)

    tools = {
        "extract_dilution_bridge": srv.extract_dilution_bridge,
        "extract_capital_structure": srv.extract_capital_structure,
        "get_uk_company_filings": srv.get_uk_company_filings,
    }
    out: dict = {}
    with patch.object(srv, "_get_submissions_for_ticker", AsyncMock(return_value=(f"{CIK:010d}", copy.deepcopy(SUBMISSIONS)))), \
            patch.object(srv, "_edgar_get_html", fake_html), \
            patch.object(srv, "_companies_house_get", fake_ch):
        with patch.dict(os.environ, {"COMPANIES_HOUSE_API_KEY": CH_KEY}):
            for name, (_, action, params) in CALLS.items():
                raw = json.loads(asyncio.run(tools[action](**params)))
                out[name] = {"data": raw["data"] if isinstance(raw, dict) and "ok" in raw else raw}
        with patch.dict(os.environ, {"COMPANIES_HOUSE_API_KEY": ""}):
            raw = json.loads(asyncio.run(srv.get_uk_company_filings(company_number="01234567")))
            out["uk_unconfigured"] = {"data": raw["data"] if isinstance(raw, dict) and "ok" in raw else raw}
    out["chRequests"] = ch_requests
    return out


class TestCapitalStructureEndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.worker = _worker_e2e()
        cls.local = _python_e2e()

    def data(self, name: str) -> dict:
        result = self.worker[name]
        self.assertIsNone(result["error"], result["error"])
        return result["data"]

    def test_runtimes_agree(self) -> None:
        for name in [*CALLS, "uk_unconfigured"]:
            self.assertEqual(self.data(name), self.local[name]["data"], name)

    def test_latest_uses_the_quarter_with_annual_fallback(self) -> None:
        b = self.data("bridge")
        self.assertEqual([(s["role"], s["filingType"]) for s in b["sources"]], [("primary", "10-Q"), ("latest_annual_fallback", "10-K")])
        self.assertEqual(b["bridge"]["dilutedSharesAtPrice"], 123_399_996)
        # The 10-Q states its own ATM remainder, which wins over the 10-K's.
        self.assertEqual(b["atmProgram"]["remainingCapacityUsd"], 140_000_000)
        self.assertEqual(b["bridge"]["atmPotentialShares"], 5_600_000)
        annual = self.data("bridge_annual")
        self.assertEqual(annual["basicShares"]["shares"], 100_000_000)
        self.assertIsNone(annual["atmProgram"])

    def test_capital_structure_from_filing(self) -> None:
        c = self.data("capital")
        self.assertEqual(c["balances"]["netCash"], -185_000_000)
        self.assertTrue(any("going_concern" not in s["categories"] for s in c["fundingStatements"]))
        self.assertTrue(c["fundingStatements"])
        latest = self.data("capital_latest")
        self.assertEqual(latest["periodEnd"], "2025-03-31")
        self.assertEqual(latest["balances"]["cashAndEquivalents"], 140_000_000)
        self.assertEqual(latest["fundingStatements"], [])

    def test_companies_house(self) -> None:
        by_name = self.data("uk_by_name")
        self.assertEqual(by_name["company"], {"companyNumber": "01234567", "name": "IQE PLC", "status": "active", "matchedBy": "company_name"})
        self.assertEqual(by_name["filings"][0]["type"], "SH01")
        by_number = self.data("uk_by_number")
        self.assertEqual(by_number["company"]["name"], "IQE PLC")
        self.assertEqual(by_number["charges"][0]["personsEntitled"], ["Lender Bank PLC"])
        requests = self.worker["chRequests"]
        self.assertTrue(all(r["auth"] == "Basic dGVzdC1jaC1rZXk6" for r in requests))
        history = next(r for r in requests if r["path"].endswith("/filing-history") and "category" in r["query"])
        self.assertEqual(history["query"], "?items_per_page=10&category=capital")
        self.assertEqual(self.data("uk_unconfigured")["status"], "SOURCE_UNCONFIGURED")

    def test_price_is_required(self) -> None:
        import server as srv

        raw = json.loads(asyncio.run(srv.extract_dilution_bridge(ticker="CSTC", price=0)))
        payload = raw.get("error") if isinstance(raw.get("error"), dict) else raw
        self.assertIn("price", json.dumps(payload))


if __name__ == "__main__":
    unittest.main()
