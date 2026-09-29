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
# Tagged the way VRT's 2025 10-K is (2.5.9): the private placement warrants' cashless exercise on 2024-12-06
# as the shares issued (on the equity-statement axis) and the warrants exercised; a public warrant class whose
# only count is an exercise-date figure with a warrants-exercised twin. None is outstanding at 2025-12-31.
PPW = {"us-gaap:ClassOfWarrantOrRightAxis": "vrt:PrivatePlacementWarrantMember", "us-gaap:StatementClassOfStockAxis": "us-gaap:CommonClassAMember"}
PUBW = {"us-gaap:ClassOfWarrantOrRightAxis": "vrt:PublicWarrantMember"}
VRT_WARRANT_K = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "wy", "10-K")}
{_text("dei:DocumentPeriodEndDate", "wy", "December 31, 2025", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("wy", "2025-01-01..2025-12-31")}{_context("wcover", "2026-02-06")}
{_context("wEx", "2024-12-06", PPW)}{_context("wExEq", "2024-12-06", {**PPW, "us-gaap:StatementEquityComponentsAxis": "us-gaap:CommonStockMember"})}
{_context("wPub", "2024-06-03", PUBW)}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "wcover", "shares", "382,000,000")}</p>
<p>On December 6, 2024, holders exercised {_num("us-gaap:ClassOfWarrantOrRightNumberOfWarrantsExercised", "wEx", "shares", "5,266,667")} private placement warrants on a cashless basis
for {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "wExEq", "shares", "4,812,521")} shares. There were no outstanding Private Placement Warrants as of December 31, 2025.</p>
<p>On June 3, 2024, {_num("us-gaap:ClassOfWarrantOrRightNumberOfWarrantsExercised", "wPub", "shares", "3,000,000")} public warrants were exercised for
{_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "wPub", "shares", "1,000,000")} shares.</p>
</body></html>"""
# Tagged the way BE's Q2 2026 10-Q is (2.5.9): notes mostly converted since issue, their tagged
# ratios the make-whole increases, a redemption after the period end, and a freestanding warrant tagged at
# issuance whose holder exercised it in May.
N28 = {"us-gaap:DebtInstrumentAxis": "be:GreenConvertibleSeniorNotesDueJune2028Member", "us-gaap:LongtermDebtTypeAxis": "us-gaap:SeniorNotesMember"}
N30 = {"us-gaap:DebtInstrumentAxis": "be:ConvertibleSeniorNotesDueNovember2030Member", "us-gaap:LongtermDebtTypeAxis": "us-gaap:SeniorNotesMember"}
FSW = {"us-gaap:ClassOfWarrantOrRightAxis": "be:FreestandingWarrantMember"}
BE_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "eq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "eq", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("eq", "2026-01-01..2026-06-30")}{_context("ecover", "2026-07-24")}
{_context("e28issue", "2023-05-16", N28)}{_context("e28end", "2026-06-30", N28)}{_context("e28sub", "2026-07-10..2026-07-10", {**N28, "us-gaap:SubsequentEventTypeAxis": "us-gaap:SubsequentEventMember"})}
{_context("e30issue", "2025-11-04", N30)}{_context("e30end", "2026-06-30", N30)}
{_context("ewIssue", "2025-10-28", FSW)}{_context("ewEx", "2026-05-01..2026-05-01", FSW)}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "ecover", "shares", "294,527,346")}</p>
<p>2028 notes: issued {_num("us-gaap:DebtInstrumentFaceAmount", "e28issue", "usd", "632.5", 6)}, conversion price ${_num("us-gaap:DebtInstrumentConvertibleConversionPrice1", "e28issue", "usdPerShare", "18.85")},
make-whole increase {_num("us-gaap:DebtInstrumentConvertibleConversionRatio1", "e28issue", "pure", "22.543")}; principal outstanding {_num("us-gaap:DebtInstrumentCarryingAmount", "e28end", "usd", "0.787", 6)};
shares issuable {_num("us-gaap:DebtInstrumentConvertibleNumberOfSharesAvailableForConversion", "e28end", "shares", "59,486")}; redeemed at {_num("us-gaap:DebtInstrumentRedemptionPricePercentage", "e28sub", "pure", "100", -2)}% in July.</p>
<p>2030 notes: issued {_num("us-gaap:DebtInstrumentFaceAmount", "e30issue", "usd", "2,500", 6)}, conversion price ${_num("us-gaap:DebtInstrumentConvertibleConversionPrice1", "e30issue", "usdPerShare", "194.97")},
make-whole increase {_num("us-gaap:DebtInstrumentConvertibleConversionRatio1", "e30issue", "pure", "2.6926")}; principal {_num("us-gaap:DebtInstrumentCarryingAmount", "e30end", "usd", "2,500", 6)}.</p>
<p>The warrant was issued for {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "ewIssue", "shares", "3,531,073")} shares at ${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "ewIssue", "usdPerShare", "113.28")};
on May 1, 2026 it was exercised on a cashless basis for {_num("us-gaap:StockIssuedDuringPeriodSharesExerciseOfWarrants", "ewEx", "shares", "1,905,433")} shares.</p>
</body></html>"""
# BE's 2025 10-K, the bridge's annual fallback: the warrant is still outstanding at its year end.
BE_K = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "ey", "10-K")}
{_text("dei:DocumentPeriodEndDate", "ey", "December 31, 2025", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("ey", "2025-01-01..2025-12-31")}{_context("eyw", "2025-10-28", FSW)}{UNITS}</ix:resources></ix:header></div>
<p>The warrant is exercisable for {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "eyw", "shares", "3,531,073")} shares at ${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "eyw", "usdPerShare", "113.28")}.</p>
</body></html>"""
# COHR's FY2026 10-K (2.5.9): no warrant or convertible is tagged, the NVIDIA share sale carries a
# price-protection provision stated only in text, and the Series B preferred is tagged at zero.
COHR_K = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "cy", "10-K")}
{_text("dei:DocumentPeriodEndDate", "cy", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("cy", "2025-07-01..2026-06-30")}{_context("ccover", "2026-08-10")}{_context("cend", "2026-06-30")}
{_context("cendB", "2026-06-30", {"us-gaap:StatementClassOfStockAxis": "iivi:SeriesBConvertiblePreferredStockMember"})}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "ccover", "shares", "195,832,246")}</p>
<p>Temporary equity shares outstanding {_num("us-gaap:TemporaryEquitySharesOutstanding", "cend", "shares", "0")}; Series B {_num("us-gaap:TemporaryEquitySharesOutstanding", "cendB", "shares", "0")}.</p>
</body></html>"""
# A preferred series still outstanding at the period end.
PREF_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "pq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "pq", "March 31, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("pq", "2026-01-01..2026-03-31")}{_context("pcover", "2026-05-01")}{_context("pend", "2026-03-31")}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "pcover", "shares", "150,000,000")}</p>
<p>Temporary equity shares outstanding {_num("us-gaap:TemporaryEquitySharesOutstanding", "pend", "shares", "215,000")}.</p>
</body></html>"""
COHR_CLAIM_TEXT = [
    # The window opens mid-sentence; that fragment is never quoted.
    "ansion. On March 2, 2026, the Company entered into a Securities Purchase Agreement (the \u201cPurchase Agreement\u201d) with NVIDIA Corporation, pursuant to which the Company issued and sold 7,788,161 shares of Common Stock at a price of $ 256.80 per share. "
    "The Purchase Agreement includes a price protection provision that is effective for a period of six months following execution of the Purchase Agreement. "
    "The Company evaluated the price protection provision under applicable U.S. GAAP and determined that the provision is indexed to the Company\u2019s own stock and meets the criteria for equity classification. "
    "Any potential issuance of additional shares or cash settlement pursuant to the price protection provision, if triggered, will be accounted for as an adjustment to equity.",
    # Revenue recognition, not a claim on shares.
    "We determine variable consideration, which primarily consists of product returns and distributor sales price reductions resulting from price protection agreements, by estimating the impact of such reductions.",
    # An instrument's own adjustment terms: the warrant component reads the warrant.
    "The warrants contain customary anti-dilution provisions that adjust the exercise price upon stock splits.",
    "All outstanding shares of Series B Convertible Preferred Stock were converted to Company Common Stock in the quarter ended December 31, 2025, and no shares of Series B Convertible Preferred Stock are currently issued and outstanding.",
]

# MRVL's Q2 FY2027 10-Q (2.5.9): customer warrants with vested counts tagged, one vesting on untagged
# conditions, a 59.0M warrant issued after the quarter (a subsequent event), and NVIDIA's Series A
# convertible preferred (2.0M shares, 21.8M common shares at $91.84).
W25 = {"us-gaap:ClassOfWarrantOrRightAxis": "mrvl:Fiscal2025WarrantSharesMember"}
W26 = {"us-gaap:ClassOfWarrantOrRightAxis": "mrvl:Fiscal2026WarrantSharesMember"}
WC = {"us-gaap:ClassOfWarrantOrRightAxis": "mrvl:CustomerWarrantCMember"}
SUB = {"us-gaap:SubsequentEventTypeAxis": "us-gaap:SubsequentEventMember"}
MRVL_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "mq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "mq", "August 1, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("mq", "2026-05-03..2026-08-01")}{_context("mcover", "2026-08-25")}{_context("mend", "2026-08-01")}
{_context("m25", "2025-02-01", W25)}{_context("m25end", "2026-08-01", W25)}{_context("m26", "2026-01-31", W26)}{_context("m26end", "2026-08-01", W26)}
{_context("mc", "2026-06-01", WC)}{_context("msub", "2026-08-28", SUB)}{_context("missue", "2026-03-31")}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "mcover", "shares", "876.9", 6)}</p>
<p>Fiscal 2025 warrant: {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "m25", "shares", "4.2", 6)} shares at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "m25", "usdPerShare", "87.77")}, vesting over {_text("us-gaap:WarrantsAndRightsOutstandingVestingTerm", "m25", "five years")};
{_num("us-gaap:ClassOfWarrantOrRightSharesVested", "m25end", "shares", "1.2", 6)} vested.</p>
<p>Fiscal 2026 warrant: {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "m26", "shares", "1.0", 6)} shares at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "m26", "usdPerShare", "87.00")}, vesting over {_text("us-gaap:WarrantsAndRightsOutstandingVestingTerm", "m26", "five years")};
{_num("us-gaap:ClassOfWarrantOrRightSharesVested", "m26end", "shares", "0", 6)} vested.</p>
<p>Customer warrant C: {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "mc", "shares", "0.5", 6)} shares at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "mc", "usdPerShare", "50.00")}, vesting over {_text("us-gaap:WarrantsAndRightsOutstandingVestingTerm", "mc", "four years")}.</p>
<p>Subsequent to quarter end, a warrant for {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "msub", "shares", "59.0", 6)} shares at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "msub", "usdPerShare", "206.58")}.</p>
<p>Preferred outstanding {_num("us-gaap:PreferredStockSharesOutstanding", "mend", "shares", "2.0", 6)}, convertible into up to
{_num("us-gaap:PreferredStockConvertibleSharesIssuable", "missue", "shares", "21.8", 6)} shares at ${_num("us-gaap:PreferredStockConvertibleConversionPrice", "missue", "usdPerShare", "91.84")}.</p>
</body></html>"""
MRVL_CLAIM_TEXT = [
    "Subsequent to quarter end, the Company issued a warrant to a customer to purchase an aggregate of up to 59.0 million of the Company\u2019s common stock at an exercise price of $ 206.58 per share over a seven year term expiring in August 2033. "
    "The warrant is eligible for vesting from the Company's third quarter of fiscal 2027 through the end of fiscal 2033, upon meeting certain revenue milestone conditions or time-based conditions.",
    "On March 31, 2026, we completed the issuance and sale of 2.0 million shares of our Series A Convertible Preferred Stock to NVIDIA for an aggregate purchase price of $2.0 billion in cash.",
]

# Warrant lifecycle (2.5.10): a class past its tagged expiration date, a class whose tagged term from an
# old count has elapsed, and a live class.
WA = {"us-gaap:ClassOfWarrantOrRightAxis": "lcx:SeriesAWarrantsMember"}
WB = {"us-gaap:ClassOfWarrantOrRightAxis": "lcx:SeriesBWarrantsMember"}
WL = {"us-gaap:ClassOfWarrantOrRightAxis": "lcx:PublicWarrantsMember"}
LIFE_K = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "ly", "10-K")}
{_text("dei:DocumentPeriodEndDate", "ly", "December 31, 2025", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("ly", "2025-01-01..2025-12-31")}{_context("lcover", "2026-02-20")}
{_context("la", "2021-06-30", WA)}{_context("lb", "2019-03-31", WB)}{_context("ll", "2025-12-31", WL)}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "lcover", "shares", "50,000,000")}</p>
<p>Series A: {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "la", "shares", "2,000,000")} at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "la", "usdPerShare", "5.00")}, expiring {_text("us-gaap:WarrantsAndRightsOutstandingMaturityDate", "la", "June 30, 2025", "ixt:date-monthname-day-year-en")}.</p>
<p>Series B: {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "lb", "shares", "1,000,000")} at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "lb", "usdPerShare", "4.00")}, term {_text("us-gaap:WarrantsAndRightsOutstandingTerm", "lb", "five years")}.</p>
<p>Public: {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "ll", "shares", "3,000,000")} at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "ll", "usdPerShare", "11.50")}.</p>
</body></html>"""

# AEHR FY2026 10-K (2.5.10): options tagged only under a company member on AwardTypeAxis.
AOPT = {"us-gaap:AwardTypeAxis": "aehr:OutstandingOptionsStockOptionTransactionsMember"}
AEHR_K = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "ay", "10-K")}
{_text("dei:DocumentPeriodEndDate", "ay", "May 29, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("ay", "2025-05-31..2026-05-29")}{_context("acover", "2026-07-20")}{_context("aend", "2026-05-29", AOPT)}{_context("aprior", "2025-05-30", AOPT)}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "acover", "shares", "30,000,000")}</p>
<p>Options outstanding {_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingNumber", "aprior", "shares", "645")} thousand then
{_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingNumber", "aend", "shares", "316", 3)} at
${_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingWeightedAverageExercisePrice", "aend", "usdPerShare", "5.11")};
exercisable {_num("us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsExercisableNumber", "aend", "shares", "310", 3)}.</p>
</body></html>"""
# LITE FY2026 10-K (2.5.10): conversion ratios tagged per $1 of principal, and a one-for-one Series A
# preferred tagged under both preferred and temporary-equity counts, with no conversion tags.
L28 = {"us-gaap:DebtInstrumentAxis": "lite:ConvertibleSeniorNotesDue2028Member"}
LITE_K = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "ty", "10-K")}
{_text("dei:DocumentPeriodEndDate", "ty", "June 27, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("ty", "2025-06-29..2026-06-27")}{_context("tcover", "2026-08-15")}{_context("tend", "2026-06-27")}{_context("t28", "2026-06-27", L28)}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "tcover", "shares", "74,000,000")}</p>
<p>2028 notes {_num("us-gaap:DebtInstrumentFaceAmount", "t28", "usd", "179.6", 6)} at ${_num("us-gaap:DebtInstrumentConvertibleConversionPrice1", "t28", "usdPerShare", "131.03")},
{_num("us-gaap:DebtInstrumentConvertibleConversionRatio1", "t28", "pure", "0.0076319", 0, decimals="INF")} shares per dollar.</p>
<p>Preferred {_num("us-gaap:PreferredStockSharesOutstanding", "tend", "shares", "2.9", 6)}; temporary equity {_num("us-gaap:TemporaryEquitySharesOutstanding", "tend", "shares", "2.9", 6)}.</p>
</body></html>"""
LITE_CLAIM_TEXT = [
    "Conversion. The Preferred Stock will convert on a one-for-one basis into shares of our common stock (i) at the option of the holder, subject to the waiting period under the Hart-Scott-Rodino Act. "
    "Dividends. Each holder of Preferred Stock will be entitled to receive dividends in the same manner as holders of our common stock.",
]
# Claim kinds added in 2.5.10, and the sentences that must not count.
CENSUS_CLAIM_TEXT = [
    "Holdings LLC units held by the continuing members are exchangeable for shares of our Class A common stock on a one-for-one basis at the holder's election.",
    "In 2025 we issued simple agreements for future equity to two investors, which convert at the next equity financing.",
    "The deferred acquisition consideration of $40.0 million is payable in shares of our common stock in 2027 at our election.",
    "We entered into a standby equity purchase agreement under which we may sell up to $100.0 million of common stock to the investor.",
    # Not claims: award settlement, note settlement, past settlement.
    "Restricted stock units are settled in shares of our common stock upon vesting.",
    "The convertible notes may be settled in shares of our common stock, cash or a combination.",
    "The earnout was settled in shares of common stock in 2024.",
]
# A preferred series with a tagged liquidation preference and dividend rate, not convertible at $10.
PREFLIQ_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "fq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "fq", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("fq", "2026-04-01..2026-06-30")}{_context("fcover", "2026-08-01")}{_context("fend", "2026-06-30")}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "fcover", "shares", "100,000,000")}</p>
<p>Preferred outstanding {_num("us-gaap:PreferredStockSharesOutstanding", "fend", "shares", "1,000,000")}, convertible into
{_num("us-gaap:PreferredStockConvertibleSharesIssuable", "fend", "shares", "5,000,000")} shares at ${_num("us-gaap:PreferredStockConvertibleConversionPrice", "fend", "usdPerShare", "20.00")};
liquidation preference ${_num("us-gaap:PreferredStockLiquidationPreference", "fend", "usdPerShare", "100.00")} per share; dividend {_num("us-gaap:PreferredStockDividendRatePercentage", "fend", "pure", "6", -2)}%.</p>
</body></html>"""

# Warrant lifecycle stated in text (2.5.11): counts tagged before the period end that the text says were
# exercised (ASTS's 122,000 private placement warrants; RKLB's 728,835 lender warrants), one it does not.
SPW = {"us-gaap:ClassOfWarrantOrRightAxis": "stl:PrivatePlacementWarrantsMember"}
SLW = {"us-gaap:ClassOfWarrantOrRightAxis": "stl:LenderWarrantMember"}
SDW = {"us-gaap:ClassOfWarrantOrRightAxis": "stl:DeltaWarrantsMember"}
SPUB = {"us-gaap:ClassOfWarrantOrRightAxis": "stl:PublicWarrantsMember"}
STALE_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "sq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "sq", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("sq", "2026-04-01..2026-06-30")}{_context("scover", "2026-08-01")}
{_context("spw", "2025-12-31", SPW)}{_context("slw", "2023-12-29", SLW)}{_context("sdw", "2022-10-07", SDW)}{_context("spub", "2026-06-30", SPUB)}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "scover", "shares", "100,000,000")}</p>
<p>Private placement: {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "spw", "shares", "122,000")} at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "spw", "usdPerShare", "11.50")}.</p>
<p>Lender: {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "slw", "shares", "728,835")} at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "slw", "usdPerShare", "4.87")}.</p>
<p>Delta: {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "sdw", "shares", "7,000,000")} at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "sdw", "usdPerShare", "0.01")}.</p>
<p>Public: {_num("us-gaap:ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights", "spub", "shares", "3,000,000")} at
${_num("us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "spub", "usdPerShare", "11.50")}.</p>
</body></html>"""
LIFECYCLE_TEXT = [
    "As of December 31, 2025, there were 122,000 Private Placement Warrants that remained outstanding. During the three months ended March 31, 2026, "
    "the remaining 122,000 Private Placement Warrants were exercised for 109,499 shares of Class A Common Stock on a cashless basis. "
    "The Private Placement Warrants expired on April 6, 2026, five years after the closing of the Business Combination.",
    "In connection with the Loan Agreement, the Company issued a warrant, dated December 29, 2023, to purchase up to 728,835 shares. "
    "On November 14, 2024, all 728,835 common stock warrants were exercised on a cashless basis, which resulted in the holder receiving 540,336 shares of common stock.",
    # Not events that retire the Delta Warrants: negated, partial, and after the period end.
    "No Delta Warrants were exercised during the three months ended June 30, 2025. On May 5, 2026, 300,000 of the Delta Warrants were exercised. "
    "All of the Delta Warrants were exercised on August 5, 2026.",
]
# Convertible principal settled in cash (2.5.11): the 2029 notes by name, and every note (LITE).
SETTLEMENT_TEXT = [
    "Upon conversion of the 2029 Notes, we will pay cash up to the aggregate principal amount of the notes being converted and pay or deliver shares of our common stock for the remainder of our conversion obligation. "
    "For the 2031 Notes, we may settle conversions in cash, shares of our common stock or a combination, at our election.",
]
LITE_SETTLEMENT_TEXT = [
    "The principal amounts of all of our outstanding convertible notes must be settled in cash. The actual cash settlement may be higher if we decide to settle the conversion value in excess of the principal amounts in cash.",
]
# Capped calls (2.5.12): the 2029 notes' capped call by its passage (strike, cap, the notes' underlying shares);
# BE's stated count that outlasted conversions; RKLB's stated count above the notes' shares; LITE's missing strike.
CAPPED_TEXT = [
    "In connection with the 2029 Notes, we entered into capped call transactions. The capped calls have an initial strike price of $20.00 per share, "
    "which corresponds to the initial conversion price of the 2029 Notes, and an initial cap price of $35.00 per share. "
    "The capped calls cover, subject to anti-dilution adjustments, the number of shares of common stock that initially underlie the 2029 Notes.",
]
BE_CAPPED_TEXT = [
    "The Capped Calls have an initial strike price of approximately $18.85 per share of Class A common stock, subject to certain adjustments. "
    "The strike price of $18.85 corresponds to the initial conversion price of the 3.0% Green Notes due June 2028. "
    "The number of shares underlying the Capped Calls is 33,549,508 shares of Class A common stock. The cap price of the Capped Calls is initially $26.46 per share of Class A common stock.",
    "The Capped Calls were not impacted by the induced conversion of 3.0% Green Notes due June 2028 in the fourth quarter of fiscal year 2025.",
]
RKLB_CAPPED_TEXT = [
    "The Capped Call Transactions have a strike price of $5.1255 per share with a cap price of $8.04 per share, covering approximately 69.3 million shares of common stock.",
]
LITE_CAPPED_TEXT = [
    "The 2032 Capped Call Options cover, subject to anti-dilution adjustments, the number of shares of our common stock that initially underlie the 2032 Notes. "
    "The cap price of the 2032 Capped Call Options was initially $268.24 per share, and is subject to certain adjustments.",
]

# Nested warrant counts and the antidilutive-securities table (2.5.13): a class tagged as a total and in
# tranches that sum to it (JOBY), a class with an "of which" part (LUNR), and the company's own table
# listing warrants, escrow shares, Class C units and a forward the bridge does not model.
NDW = {"us-gaap:ClassOfWarrantOrRightAxis": "nst:DeltaWarrantsMember"}
NDW1 = {**NDW, "nst:TrancheAxis": "nst:TrancheOneMember"}
NDW2 = {**NDW, "nst:TrancheAxis": "nst:TrancheTwoMember"}
NPW = {"us-gaap:ClassOfWarrantOrRightAxis": "nst:PreferredInvestorWarrantsMember"}
NPW1 = {**NPW, "srt:CounterpartyNameAxis": "nst:GhaffarianEnterprisesMember"}
ANTI = "us-gaap:AntidilutiveSecuritiesAxis"
NEST_Q = f"""<html><body>
<div style="display:none"><ix:header><ix:hidden>
{_text("dei:DocumentType", "nq", "10-Q")}
{_text("dei:DocumentPeriodEndDate", "nq", "June 30, 2026", "ixt:date-monthname-day-year-en")}
</ix:hidden><ix:resources>{_context("nq", "2026-04-01..2026-06-30")}{_context("ncover", "2026-08-01")}
{_context("ndw", "2026-06-30", NDW)}{_context("ndw1", "2026-06-30", NDW1)}{_context("ndw2", "2026-06-30", NDW2)}{_context("npw", "2026-06-30", NPW)}{_context("npw1", "2026-06-30", NPW1)}
{_context("aw", "2026-04-01..2026-06-30", {ANTI: "us-gaap:WarrantMember"})}{_context("ae", "2026-04-01..2026-06-30", {ANTI: "nst:EscrowSharesMember"})}
{_context("ac", "2026-04-01..2026-06-30", {ANTI: "us-gaap:CommonClassCMember"})}{_context("af", "2026-04-01..2026-06-30", {ANTI: "nst:CollaredForwardTransactionsMember"})}
{_context("ao", "2026-04-01..2026-06-30", {ANTI: "us-gaap:EmployeeStockOptionMember"})}{UNITS}</ix:resources></ix:header></div>
<p>Shares outstanding: {_num("dei:EntityCommonStockSharesOutstanding", "ncover", "shares", "100,000,000")}</p>
<p>Delta: {_num("us-gaap:ClassOfWarrantOrRightOutstanding", "ndw", "shares", "12,833,333")}, tranche one {_num("us-gaap:ClassOfWarrantOrRightOutstanding", "ndw1", "shares", "7,000,000")},
tranche two {_num("us-gaap:ClassOfWarrantOrRightOutstanding", "ndw2", "shares", "5,833,333")}.</p>
<p>Preferred investor: {_num("us-gaap:ClassOfWarrantOrRightOutstanding", "npw", "shares", "541,667")}, of which a related party holds {_num("us-gaap:ClassOfWarrantOrRightOutstanding", "npw1", "shares", "104,157")}.</p>
<p>Antidilutive: warrants {_num("us-gaap:AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount", "aw", "shares", "20,000,000")};
escrow shares {_num("us-gaap:AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount", "ae", "shares", "316,237")};
Class C common stock {_num("us-gaap:AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount", "ac", "shares", "78,163,078")};
collared forward {_num("us-gaap:AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount", "af", "shares", "7,451,200")};
options {_num("us-gaap:AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount", "ao", "shares", "0")}.</p>
</body></html>"""
NEST_CLAIM_TEXT = [
    "We entered into a forward sale agreement with the bank covering 7,451,200 shares of common stock, which will settle in 2027.",
]

# Claim kinds and preferred wordings added in 2.5.11, and the sentences that must not count.
CENSUS2_CLAIM_TEXT = [
    "In connection with the acquisition, 1,200,000 holdback shares are held by the Company and will be released to the sellers in 2027.",
    "Each contingent value right entitles the holder to receive shares of our common stock upon regulatory approval of the product.",
    "We are obligated to issue 500,000 shares of common stock to the licensor upon the first commercial sale.",
    # Not claims: a cash-only CVR and an award commitment.
    "Each contingent value right entitles the holder to a cash payment of $1.00 upon approval.",
    "We are required to issue shares of common stock to employees under the ESPP.",
]
PREFERRED_WORDINGS = [
    ["The Series B Preferred Stock has a conversion rate of 12.5 shares of common stock for each share of Series B Preferred Stock."],
    ["The Series C Preferred Stock is convertible into an aggregate of 4.2 million shares of our common stock."],
    ["Each share of Series D Preferred Stock is convertible, at the option of the holder, into 3 shares of our common stock."],
    ["The Series E Preferred Stock converts on a one-to-one basis into shares of common stock."],
]

TABLE_MATCHES = [
    {"contextText": "Unvested at December 31, 2025 | 2,500,000 | $ 14.10", "sectionHeading": "Stock-Based Compensation", "documentUrl": "https://www.sec.gov/Archives/x.htm", "filingDate": "2026-08-06", "accessionNumber": None, "inTable": True, "tableTitle": "Restricted stock unit activity", "rowLabel": "Unvested at December 31, 2025"},
    {"contextText": "Unvested at June 30, 2026 | 2,750,000 | $ 15.20", "sectionHeading": "Stock-Based Compensation", "documentUrl": "https://www.sec.gov/Archives/x.htm", "filingDate": "2026-08-06", "accessionNumber": None, "inTable": True, "tableTitle": "Restricted stock unit activity", "rowLabel": "Unvested at June 30, 2026"},
    {"contextText": "Outstanding at June 30, 2026 | 9,999,999 | $ 3.00", "sectionHeading": "Stock-Based Compensation", "documentUrl": "https://www.sec.gov/Archives/x.htm", "filingDate": "2026-08-06", "accessionNumber": None, "inTable": True, "tableTitle": "Stock option activity", "rowLabel": "Outstanding at June 30, 2026"},
    {"contextText": "Unvested at June 30, 2026, restricted stock units totalled 8,888,888 shares.", "sectionHeading": "Stock-Based Compensation", "documentUrl": "https://www.sec.gov/Archives/x.htm", "filingDate": "2026-08-06", "accessionNumber": None, "inTable": False, "tableTitle": None, "rowLabel": None},
]

FIXTURES = {"ten_k": TEN_K, "ten_q": TEN_Q, "awards_q": AWARDS_Q, "aaoi_q": AAOI_Q, "bare_q": BARE_Q, "asts_q": ASTS_Q, "debt_free_k": DEBT_FREE_K, "overlap_q": OVERLAP_Q, "aggregate_q": AGGREGATE_Q, "vrt_q": VRT_Q, "parts_q": PARTS_Q, "vrt_warrant_k": VRT_WARRANT_K, "be_q": BE_Q, "be_k": BE_K, "cohr_k": COHR_K, "pref_q": PREF_Q, "mrvl_q": MRVL_Q, "life_k": LIFE_K, "aehr_k": AEHR_K, "lite_k": LITE_K, "prefliq_q": PREFLIQ_Q, "stale_q": STALE_Q, "nest_q": NEST_Q}

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
out.bridgeBe = m.dilutionBridge({ ticker: "BEX", price: 30, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-Q", "2026-07-28", "0001234568-26-000130", data.qUrl, "be_q"), src("latest_annual_fallback", "10-K", "2026-02-27", "0001234568-26-000140", data.kUrl, "be_k")], atmMatches: [] });
out.bridgeVrtWarrants = m.dilutionBridge({ ticker: "VRTW", price: 150, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-K", "2026-02-13", "0001234568-26-000120", data.kUrl, "vrt_warrant_k")], atmMatches: [] });
const claims = data.cohrClaims.map((t) => ({ contextText: t, sectionHeading: "Equity", documentUrl: data.kUrl, filingDate: "2026-08-14", accessionNumber: null }));
const cohrSrc = src("primary", "10-K", "2026-08-14", "0001234568-26-000150", data.kUrl, "cohr_k");
out.bridgeCohr = m.dilutionBridge({ ticker: "COHX", price: 300, priceCurrency: "USD", asOfDate: null, sources: [cohrSrc], atmMatches: [], claimMatches: claims });
out.bridgeCohrUnread = m.dilutionBridge({ ticker: "COHX", price: 300, priceCurrency: "USD", asOfDate: null, sources: [cohrSrc], atmMatches: [], claimMatches: null });
out.bridgeCohrClean = m.dilutionBridge({ ticker: "COHX", price: 300, priceCurrency: "USD", asOfDate: null, sources: [cohrSrc], atmMatches: [], claimMatches: claims.slice(1) });
out.bridgePrefOpen = m.dilutionBridge({ ticker: "PREF", price: 30, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-Q", "2026-05-08", "0001234568-26-000160", data.qUrl, "pref_q")], atmMatches: [], claimMatches: claims.slice(3) });
const mrvlClaims = data.mrvlClaims.map((t) => ({ contextText: t, sectionHeading: "Subsequent Event", documentUrl: data.qUrl, filingDate: "2026-08-28", accessionNumber: null }));
const mrvlSrc = src("primary", "10-Q", "2026-08-28", "0001234568-26-000170", data.qUrl, "mrvl_q");
out.bridgeMrvl = m.dilutionBridge({ ticker: "MRVX", price: 80, priceCurrency: "USD", asOfDate: null, sources: [mrvlSrc], atmMatches: [], claimMatches: mrvlClaims });
out.bridgeMrvlHigh = m.dilutionBridge({ ticker: "MRVX", price: 100, priceCurrency: "USD", asOfDate: null, sources: [mrvlSrc], atmMatches: [], claimMatches: mrvlClaims });
out.bridgeLife = m.dilutionBridge({ ticker: "LCX", price: 20, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-K", "2026-02-27", "0001234568-26-000180", data.kUrl, "life_k")], atmMatches: [], claimMatches: [] });
const tm = (list) => list.map((t) => ({ contextText: t, sectionHeading: "Equity", documentUrl: data.kUrl, filingDate: "2026-08-20", accessionNumber: null }));
out.bridgeAehr = m.dilutionBridge({ ticker: "AEHX", price: 20, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-K", "2026-07-27", "0001234568-26-000190", data.kUrl, "aehr_k")], atmMatches: [], claimMatches: [] });
out.bridgeLite = m.dilutionBridge({ ticker: "LITX", price: 700, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-K", "2026-08-20", "0001234568-26-000200", data.kUrl, "lite_k")], atmMatches: [], claimMatches: tm(data.liteClaims) });
out.bridgeCensus = m.dilutionBridge({ ticker: "AEHX", price: 20, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-K", "2026-07-27", "0001234568-26-000190", data.kUrl, "aehr_k")], atmMatches: [], claimMatches: tm(data.censusClaims) });
out.bridgePrefLiq = m.dilutionBridge({ ticker: "PLQ", price: 10, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-Q", "2026-08-05", "0001234568-26-000210", data.qUrl, "prefliq_q")], atmMatches: [], claimMatches: [] });
const staleSrc = src("primary", "10-Q", "2026-08-10", "0001234568-26-000220", data.qUrl, "stale_q");
out.bridgeStale = m.dilutionBridge({ ticker: "STLX", price: 20, priceCurrency: "USD", asOfDate: null, sources: [staleSrc], atmMatches: [], claimMatches: [], warrantLifecycleMatches: tm(data.lifecycle) });
out.bridgeStaleUnread = m.dilutionBridge({ ticker: "STLX", price: 20, priceCurrency: "USD", asOfDate: null, sources: [staleSrc], atmMatches: [], claimMatches: [] });
out.bridgeSettled = m.dilutionBridge({ ticker: "CSTC", price: 25, priceCurrency: "USD", asOfDate: "2025-05-09", sources: [qSource, kFallback], atmMatches: atm, convertibleSettlementMatches: tm(data.settlement) });
out.bridgeLiteSettled = m.dilutionBridge({ ticker: "LITX", price: 700, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-K", "2026-08-20", "0001234568-26-000200", data.kUrl, "lite_k")], atmMatches: [], claimMatches: tm(data.liteClaims), convertibleSettlementMatches: tm(data.liteSettlement) });
out.bridgeCensus2 = m.dilutionBridge({ ticker: "AEHX", price: 20, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-K", "2026-07-27", "0001234568-26-000190", data.kUrl, "aehr_k")], atmMatches: [], claimMatches: tm(data.census2Claims) });
out.preferredWordings = data.preferredWordings.map((texts) => m.preferredConversionTerms(tm(texts)));
out.bridgeCapped = m.dilutionBridge({ ticker: "CSTC", price: 25, priceCurrency: "USD", asOfDate: "2025-05-09", sources: [qSource, kFallback], atmMatches: atm, cappedCallMatches: tm(data.capped) });
out.bridgeCappedHigh = m.dilutionBridge({ ticker: "CSTC", price: 50, priceCurrency: "USD", asOfDate: "2025-05-09", sources: [qSource, kFallback], atmMatches: atm, convertibleSettlementMatches: tm(data.settlement), cappedCallMatches: tm(data.capped) });
out.bridgeLiteCapped = m.dilutionBridge({ ticker: "LITX", price: 700, priceCurrency: "USD", asOfDate: null, sources: [src("primary", "10-K", "2026-08-20", "0001234568-26-000200", data.kUrl, "lite_k")], atmMatches: [], claimMatches: tm(data.liteClaims), cappedCallMatches: tm(data.liteCapped) });
const nestSrc = src("primary", "10-Q", "2026-08-10", "0001234568-26-000230", data.qUrl, "nest_q");
out.bridgeNest = m.dilutionBridge({ ticker: "NSTX", price: 20, priceCurrency: "USD", asOfDate: null, sources: [nestSrc], atmMatches: [], claimMatches: tm(data.nestClaims) });
out.bridgeNestUnread = m.dilutionBridge({ ticker: "NSTX", price: 20, priceCurrency: "USD", asOfDate: null, sources: [nestSrc], atmMatches: [] });
out.cappedTerms = [data.capped, data.beCapped, data.rklbCapped, data.liteCapped].map((texts) => m.cappedCallTerms(tm(texts)));
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
out.analystAsts = m.analystValuationMethods("ASTS", data.astsNews, [], ["AST SpaceMobile, Inc."]);
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
# Live ASTS items (2.4.4): Cantor's $122 target is Rocket Lab's, in a
# headline that also names AST SpaceMobile; Berenberg's $92 is ASTS's.
ASTS_NEWS = [
    {"title": "Rocket Lab Climbs 7% as Cantor Reiterates $122 Target on Launch Record; Intuitive Machines Jumps 10%, AST SpaceMobile Rises 6%", "summary": "Cantor Fitzgerald just restated its bull case for Rocket Lab with a $122 target, yet the stock is trailing a space peer that received no catalyst at all.", "url": "https://news.example/asts1"},
    {"title": "AST SpaceMobile Soars 12% on Berenberg\u2019s $92 Price Target, Planet Labs Climbs 5%", "summary": "", "url": "https://news.example/asts2"},
    {"title": "Why Is AST SpaceMobile Stock Up 13% Today?", "summary": "AST SpaceMobile stock jumped as much as 13% Wednesday after Berenberg initiated coverage with a Buy rating and a $92 price target.", "url": "https://news.example/asts3"},
    {"title": "AST SpaceMobile draws a new bull", "summary": "Berenberg set a $92 price target.", "url": "https://news.example/asts4"},
    {"title": "Rocket Lab price target raised to $130 at Needham", "summary": "", "url": "https://news.example/asts5"},
]
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
            "chHistory": CH_HISTORY, "chSearch": CH_SEARCH, "tableMatches": TABLE_MATCHES, "astsNews": ASTS_NEWS,
            "cohrClaims": COHR_CLAIM_TEXT, "mrvlClaims": MRVL_CLAIM_TEXT, "liteClaims": LITE_CLAIM_TEXT, "censusClaims": CENSUS_CLAIM_TEXT,
            "lifecycle": LIFECYCLE_TEXT, "settlement": SETTLEMENT_TEXT, "liteSettlement": LITE_SETTLEMENT_TEXT, "census2Claims": CENSUS2_CLAIM_TEXT,
            "preferredWordings": PREFERRED_WORDINGS, "capped": CAPPED_TEXT, "beCapped": BE_CAPPED_TEXT, "rklbCapped": RKLB_CAPPED_TEXT,
            "liteCapped": LITE_CAPPED_TEXT, "nestClaims": NEST_CLAIM_TEXT,
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
    claims = [cs.TextMatch(t, "Equity", K_URL, "2026-08-14", None) for t in COHR_CLAIM_TEXT]
    cohr_src = cs.IxSource("primary", "10-K", "2026-08-14", "0001234568-26-000150", K_URL, docs["cohr_k"])
    pref_src = cs.IxSource("primary", "10-Q", "2026-05-08", "0001234568-26-000160", Q_URL, docs["pref_q"])
    mrvl_claims = [cs.TextMatch(t, "Subsequent Event", Q_URL, "2026-08-28", None) for t in MRVL_CLAIM_TEXT]
    mrvl_src = cs.IxSource("primary", "10-Q", "2026-08-28", "0001234568-26-000170", Q_URL, docs["mrvl_q"])
    tm = lambda texts: [cs.TextMatch(t, "Equity", K_URL, "2026-08-20", None) for t in texts]
    aehr_src = cs.IxSource("primary", "10-K", "2026-07-27", "0001234568-26-000190", K_URL, docs["aehr_k"])
    stale_src = cs.IxSource("primary", "10-Q", "2026-08-10", "0001234568-26-000220", Q_URL, docs["stale_q"])
    lite_src = cs.IxSource("primary", "10-K", "2026-08-20", "0001234568-26-000200", K_URL, docs["lite_k"])
    nest_src = cs.IxSource("primary", "10-Q", "2026-08-10", "0001234568-26-000230", Q_URL, docs["nest_q"])
    return {
        "bridgeStale": cs.dilution_bridge("STLX", 20, "USD", None, [stale_src], [], None, [], tm(LIFECYCLE_TEXT)),
        "bridgeStaleUnread": cs.dilution_bridge("STLX", 20, "USD", None, [stale_src], [], None, []),
        "bridgeSettled": cs.dilution_bridge("CSTC", 25, "USD", "2025-05-09", [q_source, k_fallback], atm, None, None, None, tm(SETTLEMENT_TEXT)),
        "bridgeLiteSettled": cs.dilution_bridge("LITX", 700, "USD", None, [lite_src], [], None, tm(LITE_CLAIM_TEXT), None, tm(LITE_SETTLEMENT_TEXT)),
        "bridgeCensus2": cs.dilution_bridge("AEHX", 20, "USD", None, [aehr_src], [], None, tm(CENSUS2_CLAIM_TEXT)),
        "preferredWordings": [cs.preferred_conversion_terms(tm(texts)) for texts in PREFERRED_WORDINGS],
        "bridgeCapped": cs.dilution_bridge("CSTC", 25, "USD", "2025-05-09", [q_source, k_fallback], atm, None, None, None, None, tm(CAPPED_TEXT)),
        "bridgeCappedHigh": cs.dilution_bridge("CSTC", 50, "USD", "2025-05-09", [q_source, k_fallback], atm, None, None, None, tm(SETTLEMENT_TEXT), tm(CAPPED_TEXT)),
        "bridgeLiteCapped": cs.dilution_bridge("LITX", 700, "USD", None, [lite_src], [], None, tm(LITE_CLAIM_TEXT), None, None, tm(LITE_CAPPED_TEXT)),
        "cappedTerms": [cs.capped_call_terms(tm(texts)) for texts in (CAPPED_TEXT, BE_CAPPED_TEXT, RKLB_CAPPED_TEXT, LITE_CAPPED_TEXT)],
        "bridgeNest": cs.dilution_bridge("NSTX", 20, "USD", None, [nest_src], [], None, tm(NEST_CLAIM_TEXT)),
        "bridgeNestUnread": cs.dilution_bridge("NSTX", 20, "USD", None, [nest_src], []),
        "bridgeAehr": cs.dilution_bridge("AEHX", 20, "USD", None, [aehr_src], [], None, []),
        "bridgeLite": cs.dilution_bridge("LITX", 700, "USD", None, [cs.IxSource("primary", "10-K", "2026-08-20", "0001234568-26-000200", K_URL, docs["lite_k"])], [], None, tm(LITE_CLAIM_TEXT)),
        "bridgeCensus": cs.dilution_bridge("AEHX", 20, "USD", None, [aehr_src], [], None, tm(CENSUS_CLAIM_TEXT)),
        "bridgePrefLiq": cs.dilution_bridge("PLQ", 10, "USD", None, [cs.IxSource("primary", "10-Q", "2026-08-05", "0001234568-26-000210", Q_URL, docs["prefliq_q"])], [], None, []),
        "bridgeLife": cs.dilution_bridge("LCX", 20, "USD", None, [cs.IxSource("primary", "10-K", "2026-02-27", "0001234568-26-000180", K_URL, docs["life_k"])], [], None, []),
        "bridgeMrvl": cs.dilution_bridge("MRVX", 80, "USD", None, [mrvl_src], [], None, mrvl_claims),
        "bridgeMrvlHigh": cs.dilution_bridge("MRVX", 100, "USD", None, [mrvl_src], [], None, mrvl_claims),
        "bridgeCohr": cs.dilution_bridge("COHX", 300, "USD", None, [cohr_src], [], None, claims),
        "bridgeCohrUnread": cs.dilution_bridge("COHX", 300, "USD", None, [cohr_src], [], None, None),
        "bridgeCohrClean": cs.dilution_bridge("COHX", 300, "USD", None, [cohr_src], [], None, claims[1:]),
        "bridgePrefOpen": cs.dilution_bridge("PREF", 30, "USD", None, [pref_src], [], None, claims[3:]),
        "documents": {name: _doc_json(doc) for name, doc in docs.items()},
        "bridge": cs.dilution_bridge("CSTC", 25, "USD", "2025-05-09", [q_source, k_fallback], atm),
        "bridgeLow": cs.dilution_bridge("CSTC", 10, "USD", None, [k_source], []),
        "bridgeAaoi": cs.dilution_bridge("AAOX", 30, "USD", None, [cs.IxSource("primary", "10-Q", "2026-08-06", "0001234568-26-000040", Q_URL, docs["aaoi_q"])], []),
        "bridgeTable": cs.dilution_bridge("BARE", 30, "USD", None, [cs.IxSource("primary", "10-Q", "2026-08-06", "0001234568-26-000050", Q_URL, docs["bare_q"])], [], [
            cs.TextMatch(t["contextText"], t["sectionHeading"], t["documentUrl"], t["filingDate"], t["accessionNumber"], t["inTable"], t["tableTitle"], t["rowLabel"]) for t in TABLE_MATCHES
        ]),
        "bridgeAwards": cs.dilution_bridge("CSTC", 25, "USD", None, [cs.IxSource("primary", "10-Q", "2025-08-06", "0001234568-25-000030", Q_URL, docs["awards_q"])], []),
        "bridgeBe": cs.dilution_bridge("BEX", 30, "USD", None, [cs.IxSource("primary", "10-Q", "2026-07-28", "0001234568-26-000130", Q_URL, docs["be_q"]),
                                                               cs.IxSource("latest_annual_fallback", "10-K", "2026-02-27", "0001234568-26-000140", K_URL, docs["be_k"])], []),
        "bridgeVrtWarrants": cs.dilution_bridge("VRTW", 150, "USD", None, [cs.IxSource("primary", "10-K", "2026-02-13", "0001234568-26-000120", K_URL, docs["vrt_warrant_k"])], []),
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
        "analystAsts": cs.analyst_valuation_methods("ASTS", copy.deepcopy(ASTS_NEWS), [], ["AST SpaceMobile, Inc."]),
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
        for key in ("bridge", "bridgeLow", "bridgeAwards", "bridgeAaoi", "bridgeTable", "bridgeBe", "bridgeVrtWarrants", "bridgeCohr", "bridgeCohrUnread", "bridgeCohrClean", "bridgePrefOpen", "bridgeMrvl", "bridgeMrvlHigh", "bridgeLife", "bridgeAehr", "bridgeLite", "bridgeCensus", "bridgePrefLiq", "bridgeStale", "bridgeStaleUnread", "bridgeSettled", "bridgeLiteSettled", "bridgeCensus2", "preferredWordings", "bridgeCapped", "bridgeCappedHigh", "bridgeLiteCapped", "cappedTerms", "bridgeNest", "bridgeNestUnread", "capital", "capitalAaoi", "capitalAsts", "capitalDebtFree", "capitalOverlap", "capitalAggregate", "capitalVrt", "capitalParts", "labels", "analyst", "analystAsts", "ch", "chPick"):
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
        self.assertEqual(b["notDisclosed"], ["convertible_preferred"])
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
        self.assertEqual(b["notDisclosed"], ["convertible_preferred", "atm_program"])

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

    def test_convertibles_at_period_end_and_warrant_exercised_after_count(self) -> None:
        b = _python_pure()["bridgeBe"]
        notes = {i["instrument"]: i for i in next(c for c in b["components"] if c["component"] == "convertible_debt")["instruments"]}
        n28 = next(v for k, v in notes.items() if "2028" in k)
        n30 = next(v for k, v in notes.items() if "2030" in k)
        # BE (2.5.9): the 2028 notes had $0.787M left of $632.5M; the filing counts 59,486 shares issuable.
        self.assertEqual((n28["principal"], n28["principalBasis"], n28["ifConvertedShares"], n28["ifConvertedBasis"]),
                         (787_000, "outstanding_at_period_end", 59_486, "shares_issuable_tagged_at_period_end"))
        self.assertEqual(n28["afterPeriodEnd"]["date"], "2026-07-10")
        # The tagged 2.6926 is a make-whole increase, not the rate: $2.5B / $194.97, out of the money at $30.
        self.assertEqual((n30["principalBasis"], n30["conversionRatioNote"], n30["ifConvertedBasis"], n30["inTheMoney"]),
                         ("face_amount", "RATIO_INCONSISTENT_WITH_PRICE", "principal / conversion_price", False))
        self.assertEqual(n30["ifConvertedShares"], cs.round_half_up(2_500_000_000 / 194.97))
        self.assertEqual(b["bridge"]["convertibleDebt"], 59_486)
        codes = [w["code"] for w in b["warnings"]]
        self.assertIn("CONVERSION_RATIO_INCONSISTENT", codes)
        self.assertIn("CONVERTIBLE_REDEMPTION_AFTER_PERIOD_END", codes)
        # Oracle's warrant, tagged at issuance, was exercised in May: not outstanding, and the 2025 10-K fallback,
        # where it was still outstanding, does not restore it.
        self.assertIn("warrants", b["notDisclosed"])
        event = next(w for w in b["warnings"] if w["code"] == "WARRANT_EXERCISE_NOT_OUTSTANDING")["counts"][0]
        self.assertEqual((event["value"], event["reason"], event["exercise"]["value"], event["exercise"]["date"]),
                         (3_531_073, "EXERCISED_AFTER_COUNT", 1_905_433, "2026-05-01"))

    def test_untagged_share_claims_leave_the_count_partial(self) -> None:
        b = _python_pure()["bridgeCohr"]
        # COHR (2.5.9): nothing beyond the cover count is tagged, but the NVIDIA price protection is a claim on shares.
        self.assertEqual(b["status"], "PARTIAL")
        self.assertEqual(b["bridge"]["dilutedSharesAtPrice"], 195_832_246, "an unquantified claim is never added to the count")
        claims = {c["kind"]: c for c in b["unquantifiedShareClaims"]}
        self.assertEqual(sorted(claims), ["CONVERTIBLE_PREFERRED", "PRICE_PROTECTION"], "revenue price protection and warrant terms are not claims")
        pp = claims["PRICE_PROTECTION"]
        self.assertEqual(pp["status"], "UNQUANTIFIED")
        self.assertEqual(len(pp["sentences"]), 3)
        self.assertTrue(pp["sentences"][1].startswith("The Company evaluated") and "U.S. GAAP" in pp["sentences"][1], "an initialism does not end a sentence")
        self.assertIn("Any potential issuance of additional shares", pp["sentences"][2])
        self.assertTrue(pp["leadIn"].startswith("On March 2, 2026"), "the window's cut first sentence is dropped")
        pref = claims["CONVERTIBLE_PREFERRED"]
        self.assertEqual((pref["status"], pref["extinguishmentStated"]), ("TAGGED_NONE_OUTSTANDING", True))
        self.assertEqual([r["shares"] for r in pref["taggedOutstanding"]], [0, 0])
        cov = b["claimCoverage"]
        self.assertEqual((cov["scope"], cov["completeClaimInventory"], cov["textScan"], cov["unquantifiedClaims"]), ("TAGGED_INSTRUMENTS", False, "READ", 1))
        self.assertIn("UNQUANTIFIED_SHARE_CLAIMS", [w["code"] for w in b["warnings"]])
        # Without the NVIDIA text the tagged instruments are all resolved: COMPUTED, still not a full claim inventory.
        clean = _python_pure()["bridgeCohrClean"]
        self.assertEqual((clean["status"], clean["claimCoverage"]["completeClaimInventory"], clean["claimCoverage"]["unquantifiedClaims"]), ("COMPUTED", False, 0))
        unread = _python_pure()["bridgeCohrUnread"]
        self.assertEqual((unread["claimCoverage"]["textScan"], unread["unquantifiedShareClaims"]), ("NOT_READ", []))
        self.assertIn("SHARE_CLAIM_TEXT_NOT_READ", [w["code"] for w in unread["warnings"]])
        # A preferred series tagged outstanding stays open, whatever the text says about another series.
        open_pref = _python_pure()["bridgePrefOpen"]
        pref = open_pref["unquantifiedShareClaims"][0]
        self.assertEqual((pref["kind"], pref["status"], pref["taggedOutstanding"][0]["shares"]), ("CONVERTIBLE_PREFERRED", "UNQUANTIFIED", 215_000))
        self.assertEqual(open_pref["status"], "PARTIAL")

    def test_vested_warrants_post_period_warrants_and_convertible_preferred(self) -> None:
        b = _python_pure()["bridgeMrvl"]
        classes = {c["class"]: c for c in next(c for c in b["components"] if c["component"] == "warrants")["classes"]}
        # MRVL (2.5.9): the tagged vested counts, not the full grants, are exercisable.
        self.assertEqual((classes["Fiscal 2025 Warrant Shares"]["exercisable"], classes["Fiscal 2025 Warrant Shares"]["exercisableBasis"]), (1_200_000, "vested_count_tagged"))
        self.assertEqual(classes["Fiscal 2026 Warrant Shares"]["exercisable"], 0)
        # Vesting terms with no vested count: unresolved, never all 500,000 (in the money at $50).
        c = classes["Customer Warrant C"]
        self.assertEqual((c["exercisable"], c["exercisableBasis"], c["incrementalShares"]), (None, "vesting_terms_without_vested_count", None))
        self.assertIn("WARRANT_VESTING_NOT_TAGGED", [w["code"] for w in b["warnings"]])
        # The 59.0M warrant issued after the quarter is a quoted claim, not a period-end class.
        self.assertNotIn("Subsequent Event", classes)
        post = next(x for x in b["unquantifiedShareClaims"] if x["kind"] == "WARRANT_AFTER_PERIOD_END")
        self.assertEqual((post["status"], post["shares"], post["exercisePrice"], post["asOf"]), ("UNQUANTIFIED", 59_000_000, 206.58, "2026-08-28"))
        self.assertTrue(post["sentences"][0].startswith("Subsequent to quarter end"))
        self.assertIn("eligible for vesting", post["leadOut"])
        self.assertEqual(len([x for x in b["unquantifiedShareClaims"] if x["kind"] == "WARRANT_AFTER_PERIOD_END"]), 1, "the text sentence joins the tagged claim")
        # NVIDIA's Series A preferred: 21.8M common shares at $91.84, out of the money at $80.
        pref = next(c for c in b["components"] if c["component"] == "convertible_preferred")["instruments"][0]
        self.assertEqual((pref["preferredSharesOutstanding"], pref["ifConvertedShares"], pref["conversionPrice"], pref["inTheMoney"], pref["incrementalShares"], pref["countBeforePeriodEnd"]),
                         (2_000_000, 21_800_000, 91.84, False, 0, True))
        self.assertEqual(next(x for x in b["unquantifiedShareClaims"] if x["kind"] == "CONVERTIBLE_PREFERRED")["status"], "MODELED_IN_BRIDGE")
        self.assertEqual(b["bridge"]["grossSharesAllInstruments"], 876_900_000 + 4_200_000 + 1_000_000 + 500_000 + 21_800_000, "the post-period 59.0M is not in the count")
        self.assertEqual(b["status"], "PARTIAL")
        high = _python_pure()["bridgeMrvlHigh"]
        self.assertEqual(high["bridge"]["convertiblePreferred"], 21_800_000)

    def test_award_axis_options_ratio_units_and_stated_preferred(self) -> None:
        out = _python_pure()
        opts = next(c for c in out["bridgeAehr"]["components"] if c["component"] == "stock_options")
        # AEHR (2.5.10): 316,000 options at $5.11 tagged only on AwardTypeAxis, at the latest date.
        self.assertEqual((opts["outstanding"], opts["exercisable"], opts["weightedAverageExercisePrice"], opts["countBasis"]), (316_000, 310_000, 5.11, "award_axis_members"))
        self.assertEqual(opts["incrementalShares"], cs.round_half_up(316_000 - 316_000 * 5.11 / 20))
        self.assertNotIn("stock_options", out["bridgeAehr"]["notDisclosed"])
        lite = out["bridgeLite"]
        note = next(c for c in lite["components"] if c["component"] == "convertible_debt")["instruments"][0]
        # LITE: 0.0076319 per $1 is 7.6319 per $1,000.
        self.assertEqual((note["conversionRatioPer1000"], note["conversionRatioTagged"], note["conversionRatioNote"], note["ifConvertedBasis"]),
                         (7.6319, 0.0076319, "RATIO_PER_1_PRINCIPAL_SCALED", "principal / 1000 * conversion_ratio"))
        pref = next(c for c in lite["components"] if c["component"] == "convertible_preferred")["instruments"][0]
        # 2.9M tagged twice is 2.9M, one-for-one as the filing states, common-equivalent at any price.
        self.assertEqual((pref["preferredSharesOutstanding"], pref["ifConvertedShares"], pref["ifConvertedBasis"], pref["incrementalShares"], pref["method"]),
                         (2_900_000, 2_900_000, "ratio_stated_in_text", 2_900_000, "as_converted_no_conversion_price"))
        self.assertIn("one-for-one", pref["statedConversion"]["sentence"])
        self.assertEqual(cs.normalized_ratio(0.5, None)["note"], "RATIO_UNIT_UNCERTAIN")
        liq = next(c for c in out["bridgePrefLiq"]["components"] if c["component"] == "convertible_preferred")["instruments"][0]
        self.assertEqual((liq["liquidationPreference"]["amount"], liq["liquidationPreference"]["basis"], liq["dividendRatePct"], liq["incrementalShares"]),
                         (100_000_000, "per_share_tagged_x_shares_outstanding", 6, 0))

    def test_claim_census_breadth(self) -> None:
        claims = {c["kind"]: c for c in _python_pure()["bridgeCensus"]["unquantifiedShareClaims"]}
        self.assertEqual(sorted(claims), ["EQUITY_LINE", "EXCHANGEABLE_INTERESTS", "SAFE", "SHARE_SETTLED_OBLIGATION"])
        quoted = claims["SHARE_SETTLED_OBLIGATION"]["sentences"]
        self.assertEqual(len(quoted), 1, "award, note and past settlements are not claims")
        self.assertIn("deferred acquisition consideration", quoted[0])

    def test_warrant_lifecycle_stated_in_text(self) -> None:
        out = _python_pure()
        b = out["bridgeStale"]
        w = next(c for c in b["components"] if c["component"] == "warrants")
        # 2.5.11: the private placement warrants (exercised by March 31, before their April expiry) and the lender
        # warrants (all 728,835 exercised on November 14, 2024) are retired; the Delta and public warrants remain.
        self.assertEqual([c["class"] for c in w["classes"]], ["Delta Warrants", "Public Warrants"])
        retired = {r["class"]: r for r in w["retiredInText"]}
        self.assertEqual((retired["Private Placement Warrants"]["event"], retired["Private Placement Warrants"]["eventDate"], retired["Private Placement Warrants"]["matchedBy"]),
                         ("EXERCISED", "2026-03-31", "STATED_COUNT"))
        self.assertEqual((retired["Lender Warrant"]["event"], retired["Lender Warrant"]["eventDate"], retired["Lender Warrant"]["count"]),
                         ("EXERCISED", "2024-11-14", 728_835))
        self.assertIn("all 728,835 common stock warrants were exercised", retired["Lender Warrant"]["sentence"])
        # The negated, partial and post-period Delta sentences leave it counted, read.
        self.assertEqual(w["classes"][0]["lifecycleText"], "NO_EVENT_STATED")
        self.assertNotIn("lifecycleText", w["classes"][1], "a count at the period end needs no lifecycle read")
        self.assertEqual(w["outstanding"], 10_000_000)
        codes = [x["code"] for x in b["warnings"]]
        self.assertIn("WARRANT_RETIRED_IN_TEXT", codes)
        early = next(x for x in b["warnings"] if x["code"] == "WARRANT_COUNT_BEFORE_PERIOD_END")
        self.assertIn("states no exercise, expiry or redemption", early["message"])
        unread = next(c for c in out["bridgeStaleUnread"]["components"] if c["component"] == "warrants")
        self.assertEqual(len(unread["classes"]), 4)
        self.assertEqual({c.get("lifecycleText") for c in unread["classes"]}, {"NOT_READ", None})
        self.assertNotIn("retiredInText", unread)
        sentences = cs.warrant_lifecycle_sentences([cs.TextMatch(t, None, None, None, None) for t in LIFECYCLE_TEXT])
        self.assertEqual([s["event"] for s in sentences], ["EXERCISED", "EXPIRED", "EXERCISED", "EXERCISED", "EXERCISED"])

    def test_principal_settled_in_cash(self) -> None:
        out = _python_pure()
        conv = next(c for c in out["bridgeSettled"]["components"] if c["component"] == "convertible_debt")
        n29, n31 = conv["instruments"]
        # 2029 notes, principal in cash as stated: 15,000,000 if-converted less $300M / $25 = 3,000,000 net shares.
        self.assertEqual((n29["principalSettlement"]["scope"], n29["netShareSettlementShares"], n29["incrementalShares"]), ("NAMED_NOTES", 3_000_000, 15_000_000))
        # The 2031 notes settle at the issuer's election: not a stated cash settlement.
        self.assertIsNone(n31["principalSettlement"])
        self.assertIsNone(n31["netShareSettlementShares"])
        self.assertEqual((conv["settlementText"], conv["incrementalSharesNetShareSettlement"]), ("READ", 3_000_000))
        bridge = out["bridgeSettled"]["bridge"]
        self.assertEqual(bridge["dilutedSharesAtPrice"] - bridge["dilutedSharesAtPriceNetShareSettlement"], 12_000_000)
        self.assertEqual(bridge["convertibleDebtNetShareSettlement"], 3_000_000)
        self.assertIn("CONVERTIBLE_PRINCIPAL_SETTLED_IN_CASH", [w["code"] for w in out["bridgeSettled"]["warnings"]])
        # Unread: the if-converted count only.
        plain = next(c for c in out["bridge"]["components"] if c["component"] == "convertible_debt")
        self.assertEqual((plain["settlementText"], plain["incrementalSharesNetShareSettlement"]), ("NOT_READ", None))
        self.assertNotIn("principalSettlement", plain["instruments"][0])
        self.assertIsNone(out["bridge"]["bridge"]["dilutedSharesAtPriceNetShareSettlement"])
        # LITE: every note; 1,370,689 if-converted less $179.6M / $700.
        lite = next(c for c in out["bridgeLiteSettled"]["components"] if c["component"] == "convertible_debt")["instruments"][0]
        self.assertEqual((lite["principalSettlement"]["scope"], lite["netShareSettlementShares"]), ("ALL_NOTES", 1_114_118))

    def test_nested_warrants_and_antidilutive_table(self) -> None:
        out = _python_pure()
        b = out["bridgeNest"]
        w = next(c for c in b["components"] if c["component"] == "warrants")
        # 2.5.13: the Delta tranches replace their total; the related party's 104,157 is part of the 541,667.
        self.assertEqual(sorted(c["class"] for c in w["classes"]), ["Delta Warrants / Tranche One", "Delta Warrants / Tranche Two", "Preferred Investor Warrants"])
        self.assertEqual(w["outstanding"], 7_000_000 + 5_833_333 + 541_667)
        self.assertEqual(sorted((n["class"], n["reason"]) for n in w["nestedCounts"]),
                         [("Delta Warrants", "SUM_OF_COUNTED_PARTS"), ("Preferred Investor Warrants / Ghaffarian Enterprises", "PART_OF_COUNTED_CLASS")])
        codes = [x["code"] for x in b["warnings"]]
        self.assertIn("WARRANT_NESTED_COUNT", codes)
        # The company reports 20,000,000 warrants: the gap is flagged, never added.
        self.assertIn("WARRANT_COUNT_DIFFERS_FROM_REPORTED", codes)
        rows = {r["security"]: r for r in b["antidilutiveReconciliation"]["rows"]}
        self.assertEqual((rows["Warrant"]["category"], rows["Warrant"]["modeledBy"]), ("warrants", "warrants"))
        self.assertNotIn("Employee Stock Option", rows, "a zero row is not a security")
        claims = {c["kind"]: c for c in b["unquantifiedShareClaims"]}
        # The text's forward sale takes the table's count; escrow shares and Class C units are claims from the table alone.
        self.assertEqual((claims["FORWARD_SALE"]["status"], claims["FORWARD_SALE"]["reportedShares"], len(claims["FORWARD_SALE"]["sentences"])),
                         ("REPORTED_NOT_MODELED", 7_451_200, 1))
        self.assertEqual((claims["CONTINGENT_SHARES"]["reportedShares"], claims["CONTINGENT_SHARES"]["evidence"]), (316_237, "ANTIDILUTIVE_TABLE"))
        self.assertEqual(claims["EXCHANGEABLE_INTERESTS"]["reportedShares"], 78_163_078)
        self.assertEqual(b["status"], "PARTIAL")
        self.assertEqual(b["claimCoverage"]["antidilutiveTable"], "READ")
        self.assertTrue(cs.is_open_claim(claims["FORWARD_SALE"]))
        # Without the text read, the table's rows are still claims.
        unread = {c["kind"]: c for c in out["bridgeNestUnread"]["unquantifiedShareClaims"]}
        self.assertEqual((unread["FORWARD_SALE"]["sentences"], unread["FORWARD_SALE"]["reportedShares"]), ([], 7_451_200))

    def test_capped_calls(self) -> None:
        out = _python_pure()
        conv = next(c for c in out["bridgeCapped"]["components"] if c["component"] == "convertible_debt")
        n29, n31 = conv["instruments"]
        call = n29["cappedCall"]
        # 2.5.12: strike $20, cap $35, the 2029 notes' 15,000,000 shares: 15,000,000 x (25 - 20) / 25 = 3,000,000 back at $25.
        self.assertEqual((call["strikePrice"], call["strikeBasis"], call["capPrice"], call["coveredShares"], call["coverageBasis"], call["offsetSharesAtPrice"]),
                         (20.0, "STATED", 35.0, 15_000_000, "SHARES_UNDERLYING_NOTES_AT_PERIOD_END", 3_000_000))
        self.assertIsNone(n31["cappedCall"], "the passage names only the 2029 notes")
        bridge = out["bridgeCapped"]["bridge"]
        self.assertEqual((bridge["cappedCallOffsetShares"], bridge["dilutedSharesAtPrice"] - bridge["dilutedSharesAtPriceNetOfCappedCalls"]), (3_000_000, 3_000_000))
        self.assertIsNone(bridge["dilutedSharesAtPriceNetShareSettlementNetOfCappedCalls"], "no stated cash settlement in this read")
        self.assertIn("CAPPED_CALL_OFFSET", [w["code"] for w in out["bridgeCapped"]["warnings"]])
        # At $50 the cap binds: 15,000,000 x (35 - 20) / 50 = 4,500,000; with the 2029 notes net-share settled too.
        high = out["bridgeCappedHigh"]["bridge"]
        self.assertEqual(high["cappedCallOffsetShares"], 4_500_000)
        self.assertEqual(high["dilutedSharesAtPriceNetShareSettlement"] - high["dilutedSharesAtPriceNetShareSettlementNetOfCappedCalls"], 4_500_000)
        # Unread: no capped call fields on the notes, no offset.
        plain = next(c for c in out["bridge"]["components"] if c["component"] == "convertible_debt")
        self.assertEqual((plain["cappedCallText"], plain["cappedCallOffsetShares"]), ("NOT_READ", None))
        self.assertNotIn("cappedCall", plain["instruments"][0])
        # LITE: the cap and coverage are stated, the strike is not: reported, not netted.
        lite = next(c for c in out["bridgeLiteCapped"]["components"] if c["component"] == "convertible_debt")["instruments"][0]
        self.assertIsNone(lite["cappedCall"], "the fixture's LITE note is the 2028 notes; the 2032 capped call names another note")
        terms = out["cappedTerms"]
        self.assertEqual((terms[1][0]["strikePrice"], terms[1][0]["capPrice"], terms[1][0]["coveredShares"], terms[1][0]["survivesConversions"], terms[1][0]["years"]),
                         (18.85, 26.46, 33_549_508, True, ["2028"]))
        self.assertEqual((terms[2][0]["coveredShares"], terms[2][0]["strikePrice"], terms[2][0]["capPrice"]), (69_300_000, 5.1255, 8.04))
        self.assertEqual((terms[3][0]["strikePrice"], terms[3][0]["capPrice"], terms[3][0]["coversNoteShares"], terms[3][0]["years"]), (None, 268.24, True, ["2032"]))
        # A stated count above the notes' shares: used when the capped calls outlasted conversions, bounded otherwise.
        note = {"conversionPrice": 18.85, "ifConvertedShares": 59_486}
        be = cs._capped_call_at(terms[1][0], note, 30)
        self.assertEqual((be["coveredShares"], be["coverageBasis"], be["offsetSharesAtPrice"]), (33_549_508, "STATED_COUNT_SURVIVES_CONVERSIONS", 8_510_392))
        rklb = cs._capped_call_at(terms[2][0], {"conversionPrice": 5.13, "ifConvertedShares": 27_736_452}, 20)
        self.assertEqual((rklb["coveredShares"], rklb["coverageBasis"], rklb["statedCoveredShares"], rklb["offsetSharesAtPrice"]),
                         (27_736_452, "SHARES_UNDERLYING_NOTES_AT_PERIOD_END_BELOW_STATED_COUNT", 69_300_000, 4_041_894))
        lite_call = cs._capped_call_at(terms[3][0], {"conversionPrice": 187.77, "ifConvertedShares": 6_737_011}, 700)
        self.assertEqual((lite_call["strikeBasis"], lite_call["offsetSharesAtPrice"], lite_call["unresolvedReason"], lite_call["missingTerms"]),
                         ("NOT_STATED", None, "CAPPED_CALL_TERMS_INCOMPLETE", ["STRIKE_PRICE"]))

    def test_claim_census_2_5_11(self) -> None:
        out = _python_pure()
        claims = {c["kind"]: c for c in out["bridgeCensus2"]["unquantifiedShareClaims"]}
        self.assertEqual(sorted(claims), ["CONTINGENT_SHARES", "CONTINGENT_VALUE_RIGHT", "SHARE_ISSUANCE_COMMITMENT"])
        self.assertEqual(len(claims["CONTINGENT_VALUE_RIGHT"]["sentences"]), 1, "a cash-only CVR is not a share claim")
        self.assertEqual(len(claims["SHARE_ISSUANCE_COMMITMENT"]["sentences"]), 1, "an ESPP commitment is an award")
        self.assertEqual([(w or {}).get("ratio") for w in out["preferredWordings"]], [12.5, None, 3, 1])
        self.assertEqual(out["preferredWordings"][1]["aggregateShares"], 4_200_000)

    def test_warrant_expiry_and_elapsed_terms(self) -> None:
        b = _python_pure()["bridgeLife"]
        w = next(c for c in b["components"] if c["component"] == "warrants")
        # 2.5.10: Series A expired on 2025-06-30 by its tagged date; it is closed, and listed.
        self.assertEqual([c["class"] for c in w["classes"]], ["Series B Warrants", "Public Warrants"])
        self.assertEqual(w["expiredClasses"], [{"class": "Series A Warrants", "count": 2_000_000, "asOf": "2021-06-30", "expirationDate": "2025-06-30"}])
        # Series B's five-year term from its 2019-03-31 count ended 2024-03-31: flagged, still counted.
        series_b = w["classes"][0]
        self.assertEqual((series_b["termElapsedBy"], series_b["outstanding"]), ("2024-03-31", 1_000_000))
        self.assertNotIn("termElapsedBy", w["classes"][1])
        codes = [x["code"] for x in b["warnings"]]
        self.assertIn("WARRANT_EXPIRED_BEFORE_PERIOD_END", codes)
        self.assertIn("WARRANT_TERM_ELAPSED", codes)
        self.assertEqual(b["bridge"]["grossSharesAllInstruments"], 50_000_000 + 1_000_000 + 3_000_000)
        self.assertEqual((cs.term_years("P7Y"), cs.term_years("5 years"), cs.term_years("six years"), cs.term_years("18 months")), (7, 5, 6, None))

    def test_exercised_warrants_are_not_outstanding(self) -> None:
        bridge = _python_pure()["bridgeVrtWarrants"]
        # VRT (2.5.9): the 4,812,521 shares issued on exercise are not 4,812,521 warrants outstanding.
        self.assertNotIn("warrants", [c["component"] for c in bridge["components"]])
        self.assertIn("warrants", bridge["notDisclosed"])
        self.assertIsNone(bridge["bridge"]["warrants"])
        self.assertEqual(bridge["bridge"]["grossSharesAllInstruments"], 382_000_000)
        events = next(w for w in bridge["warnings"] if w["code"] == "WARRANT_EXERCISE_NOT_OUTSTANDING")
        self.assertEqual(sorted((c["value"], c["reason"]) for c in events["counts"]),
                         [(1_000_000, "WARRANT_EXERCISE"), (4_812_521, "EQUITY_STATEMENT_MOVEMENT")])

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
        self.assertEqual(b["notDisclosed"], ["stock_options", "convertible_debt", "convertible_preferred", "atm_program"])
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
        # Tagged only at issuance (2025-03-13), before the 2026-06-30 period end: still counted, and flagged (2.5.9).
        self.assertTrue(warrant["countBeforePeriodEnd"])
        self.assertIn("WARRANT_COUNT_BEFORE_PERIOD_END", [w["code"] for w in b["warnings"]])
        self.assertNotIn("WARRANT_EXERCISE_NOT_OUTSTANDING", [w["code"] for w in b["warnings"]])

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

    def test_targets_are_attributed_to_the_subject(self) -> None:
        a = self.out["analystAsts"]
        kept = [(e["url"][-5:], e["firm"], e["priceTarget"]["target"], e["subjectMatch"]) for e in a["evidence"]]
        # Rocket Lab's $122 (twice) and $130 are rejected; Berenberg's $92 is kept in all three items.
        self.assertEqual(kept, [("asts2", "Berenberg", 92, "CLAUSE"), ("asts3", "Berenberg", 92, "CLAUSE"), ("asts4", "Berenberg", 92, "ITEM")])
        self.assertEqual(a["attribution"], {"checked": True, "subjectAliases": ["ast spacemobile", "ast spacemobile inc", "asts"], "rejectedForOtherCompany": 3})
        self.assertNotIn("Cantor Fitzgerald", [m["firm"] for m in a["methodNotDisclosed"]])
        # Without issuer names nothing is checked (the parsing fixture above).
        self.assertEqual(self.out["analyst"]["attribution"]["checked"], False)

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
