#!/usr/bin/env python3
"""Historical valuation context and metric reconciliation (2.5.4): Worker vs local parity and semantics.

- Valuation at a date uses only SEC facts filed by then, the close with later
  split adjustments undone, balances at the latest balance date, LTM and LFY
  denominators with their components, FX and ADS normalization; untagged or
  unfiled figures leave multiples null with a status.
- Reconciliation compares one metric and period across SEC as first and last
  filed, period-scoped release sentences and Yahoo, with AGREED / PARTIAL /
  CONFLICT / NOT_FOUND and restatements flagged.
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

from yfmcp import metric_reconciliation as mr  # noqa: E402
from yfmcp import valuation_history as vh  # noqa: E402
from yfmcp.evidence import AUTHORITY_BOUNDARY  # noqa: E402


def _f(start, end, val, form, filed, accn):
    row = {"end": end, "val": val, "form": form, "filed": filed, "accn": accn}
    if start is not None:
        row["start"] = start
    return row


K24 = ("10-K", "2025-02-15", "k24")
Q125 = ("10-Q", "2025-05-05", "q125")
Q225 = ("10-Q", "2025-08-05", "q225")
Q224 = ("10-Q", "2024-08-05", "q224")


def _usd(*rows):
    return {"units": {"USD": list(rows)}}


SYN = {"facts": {
    "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [
        _f(None, "2025-02-10", 10, *K24),
        _f(None, "2025-07-30", 20, *Q225),
    ]}}},
    "us-gaap": {
        "Revenues": _usd(
            _f("2023-01-01", "2023-12-31", 800, "10-K", "2024-02-15", "k23"),
            _f("2023-01-01", "2023-12-31", 800, *K24),
            _f("2024-01-01", "2024-12-31", 1000, *K24),
            _f("2024-01-01", "2024-06-30", 450, *Q224),
            _f("2024-01-01", "2024-06-30", 450, *Q225),
            _f("2024-04-01", "2024-06-30", 240, *Q225),
            _f("2025-01-01", "2025-03-31", 260, *Q125),
            _f("2025-01-01", "2025-06-30", 560, *Q225),
            _f("2025-04-01", "2025-06-30", 300, *Q225),
        ),
        "OperatingIncomeLoss": _usd(
            _f("2024-01-01", "2024-12-31", 200, *K24),
            _f("2024-01-01", "2024-06-30", 90, *Q225),
            _f("2025-01-01", "2025-06-30", 120, *Q225),
        ),
        "NetIncomeLoss": _usd(
            _f("2024-01-01", "2024-12-31", 100, *K24),
            _f("2024-01-01", "2024-06-30", 40, *Q225),
            _f("2025-01-01", "2025-06-30", 70, *Q225),
        ),
        "ProfitLoss": _usd(_f("2024-01-01", "2024-12-31", 110, *K24)),
        # The total only in the 10-K; depreciation and amortization separately in 10-Qs (VRT).
        "DepreciationDepletionAndAmortization": _usd(_f("2024-01-01", "2024-12-31", 50, *K24)),
        "Depreciation": _usd(
            _f("2024-01-01", "2024-12-31", 35, *K24),
            _f("2024-01-01", "2024-06-30", 18, *Q225),
            _f("2025-01-01", "2025-06-30", 20, *Q225),
        ),
        "AmortizationOfIntangibleAssets": _usd(
            _f("2024-01-01", "2024-12-31", 15, *K24),
            _f("2024-01-01", "2024-06-30", 9, *Q225),
            _f("2025-01-01", "2025-06-30", 10, *Q225),
        ),
        "CashAndCashEquivalentsAtCarryingValue": _usd(
            _f(None, "2024-12-31", 100, *K24),
            _f(None, "2025-06-30", 150, *Q225),
        ),
        "ShortTermInvestments": _usd(_f(None, "2025-06-30", 20, *Q225)),
        # LongTermDebt last tagged at 2024-12-31 is not carried to 2025-06-30 (ASTS).
        "LongTermDebt": _usd(_f(None, "2024-12-31", 300, *K24)),
        "LongTermDebtCurrent": _usd(_f(None, "2025-06-30", 30, *Q225)),
        "LongTermDebtNoncurrent": _usd(_f(None, "2025-06-30", 280, *Q225)),
        "CommercialPaper": _usd(_f(None, "2025-06-30", 5, *Q225)),
    },
}}

BARS = [
    {"date": "2025-02-28", "close": 49.0},
    {"date": "2025-03-03", "close": 50.0},
    {"date": "2025-07-01", "close": 60.0},
    {"date": "2025-09-02", "close": 70.0},
]
SPLITS = [{"date": "2025-06-01", "ratio": 2.0}]
DATES = ["2025-03-03", "2025-07-01", "2025-09-02", "2021-01-04"]

F20 = ("20-F", "2025-04-15", "f24")
IFRS_FACTS = {"facts": {
    "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [_f(None, "2024-12-31", 500, *F20)]}}},
    "ifrs-full": {
        "Revenue": {"units": {"TWD": [_f("2024-01-01", "2024-12-31", 20000, *F20)], "USD": [_f("2024-01-01", "2024-12-31", 610, *F20)]}},
        "ProfitLossFromOperatingActivities": {"units": {"TWD": [_f("2024-01-01", "2024-12-31", 8000, *F20)]}},
        "ProfitLossAttributableToOwnersOfParent": {"units": {"TWD": [_f("2024-01-01", "2024-12-31", 6000, *F20)]}},
        "DepreciationExpense": {"units": {"TWD": [_f("2024-01-01", "2024-12-31", 3000, *F20)]}},
        "AmortisationExpense": {"units": {"TWD": [_f("2024-01-01", "2024-12-31", 100, *F20)]}},
        "CashAndCashEquivalents": {"units": {"TWD": [_f(None, "2024-12-31", 3000, *F20)]}},
        "Borrowings": {"units": {"TWD": [_f(None, "2024-12-31", 1000, *F20)]}},
    },
}}
IFRS_BARS = [{"date": "2025-06-02", "close": 10.0}]
FX = {"pair": "TWDUSD=X", "bars": [{"date": "2025-05-30", "close": 0.03}]}

# Fresh-share / stale-balance and fresh-balance / stale-results fixtures exercise
# the 2.5.4 live residual where warnings did not invalidate multiples.
STALE_BAL_FACTS = json.loads(json.dumps(SYN))
STALE_BAL_FACTS["facts"]["dei"]["EntityCommonStockSharesOutstanding"]["units"]["shares"].append(
    _f(None, "2026-08-15", 21, "10-Q", "2026-08-20", "q326")
)
STALE_BAL_BARS = [{"date": "2026-09-02", "close": 80.0}]

STALE_RESULTS_FACTS = json.loads(json.dumps(SYN))
STALE_RESULTS_FACTS["facts"]["dei"]["EntityCommonStockSharesOutstanding"]["units"]["shares"].append(
    _f(None, "2027-01-10", 22, "10-Q", "2027-01-15", "q427")
)
for concept, value in (
    ("CashAndCashEquivalentsAtCarryingValue", 200),
    ("ShortTermInvestments", 25),
    ("LongTermDebtCurrent", 40),
    ("LongTermDebtNoncurrent", 300),
):
    STALE_RESULTS_FACTS["facts"]["us-gaap"][concept]["units"]["USD"].append(
        _f(None, "2026-12-31", value, "10-Q", "2027-01-15", "q427")
    )
STALE_RESULTS_BARS = [{"date": "2027-01-20", "close": 90.0}]


def _hv(ticker, facts, bars, dates, splits=None, fx=None, ads=None, currency="USD", covers=None):
    out = {"ticker": ticker, "dates": dates, "companyfacts": facts, "bars": bars, "priceCurrency": currency, "splits": splits or [], "fx": fx, "adsRatio": ads}
    if covers is not None:
        out["coverCounts"] = covers
        out["periodicFilings"] = FILINGS
    return out


# The periodic reports SEC submissions list for the synthetic filer.
FILINGS = [
    {"accessionNumber": "k24", "form": "10-K", "filed": "2025-02-15", "reportDate": "2024-12-31"},
    {"accessionNumber": "q125", "form": "10-Q", "filed": "2025-05-05", "reportDate": "2025-03-31"},
    {"accessionNumber": "q225", "form": "10-Q", "filed": "2025-08-05", "reportDate": "2025-06-30"},
]


# A multi-class filer (ASTS): no undimensioned cover or balance-sheet count, only a weighted-average basic count.
MULTI = json.loads(json.dumps(SYN))
del MULTI["facts"]["dei"]
MULTI["facts"]["us-gaap"]["WeightedAverageNumberOfSharesOutstandingBasic"] = {"units": {"shares": [
    _f("2025-01-01", "2025-06-30", 14, *Q225),
    _f("2025-04-01", "2025-06-30", 15, *Q225),
]}}
CLASS_SUM = {"q225": {"status": "OK", "value": 25, "asOf": "2025-08-01", "basis": "COVER_PAGE_CLASS_SUM", "documentUrl": "https://www.sec.gov/q225.htm",
                      "classes": [{"class": "Common Class A", "member": "us-gaap:CommonClassAMember", "shares": 20},
                                  {"class": "Common Class B", "member": "us-gaap:CommonClassBMember", "shares": 5}]}}
COVER_FAILED = {"q225": {"status": "CONTEXT_NOT_READ", "value": None, "documentUrl": "https://www.sec.gov/q225.htm"}}

# A filer that moved from 20-F to 10-K: each date takes the cadence of the annual report filed by then.
REGIME = {"facts": {"us-gaap": {"Revenues": _usd(
    _f("2022-01-01", "2022-12-31", 100, "20-F", "2023-04-01", "f22"),
    _f("2023-01-01", "2023-12-31", 110, "20-F", "2024-04-01", "f23"),
    _f("2024-01-01", "2024-12-31", 120, "10-K", "2025-03-01", "k24r"),
)}}}
REGIME_BARS = [{"date": "2024-06-03", "close": 10.0}, {"date": "2025-06-02", "close": 11.0}]


def _ctx(cid, date, members=()):
    segment = "".join(f'<xbrldi:explicitMember dimension="{d}">{m}</xbrldi:explicitMember>' for d, m in members)
    entity = f"<xbrli:entity><xbrli:identifier>1</xbrli:identifier>{f'<xbrli:segment>{segment}</xbrli:segment>' if segment else ''}</xbrli:entity>"
    return f'<xbrli:context id="{cid}">{entity}<xbrli:period><xbrli:instant>{date}</xbrli:instant></xbrli:period></xbrli:context>'


def _cover(cid, text, fmt="ixt:num-dot-decimal", scale=None):
    scale_attr = f' scale="{scale}"' if scale is not None else ""
    return (f'<ix:nonFraction contextRef="{cid}" name="dei:EntityCommonStockSharesOutstanding" unitRef="shares" decimals="INF" format="{fmt}"{scale_attr}>'
            f"{text}</ix:nonFraction>")


CLASS_A = ("us-gaap:StatementClassOfStockAxis", "us-gaap:CommonClassAMember")
CLASS_B = ("us-gaap:StatementClassOfStockAxis", "us-gaap:CommonClassBMember")
COVER_HTML = [
    # Two classes, the Class A fact repeated (hidden and visible), and an older cover date ignored.
    "<ix:header><ix:resources>" + _ctx("a", "2026-08-06", [CLASS_A]) + _ctx("b", "2026-08-06", [CLASS_B]) + _ctx("old", "2025-08-06", [CLASS_A])
    + "</ix:resources></ix:header><p>" + _cover("a", "299,789,305") + _cover("b", "<span>11,215,111</span>") + _cover("a", "299,789,305")
    + _cover("old", "1") + "</p>",
    # An undimensioned count is the total.
    _ctx("t", "2026-08-06") + _ctx("a", "2026-08-06", [CLASS_A]) + _cover("t", "311,004,416") + _cover("a", "299,789,305"),
    # A second dimension (co-registrants) is not summed.
    _ctx("a", "2026-08-06", [CLASS_A, ("dei:LegalEntityAxis", "x:SubMember")]) + _cover("a", "5"),
    # A context outside the read leaves every class unresolved.
    _ctx("a", "2026-08-06", [CLASS_A]) + _cover("a", "10") + _cover("missing", "3"),
    "<p>No cover count.</p>",
    # Scale and comma-decimal formats; a dash is zero.
    _ctx("a", "2026-08-06", [CLASS_A]) + _ctx("b", "2026-08-06", [CLASS_B]) + _cover("a", "1.234,00", fmt="ixt:num-comma-decimal", scale=3)
    + _cover("b", "—", fmt="ixt:fixed-zero"),
    # An unparsed value fails the read.
    _ctx("a", "2026-08-06", [CLASS_A]) + _cover("a", "n/a"),
]


HV_INPUTS = {
    "syn": _hv("syn", SYN, BARS, DATES, SPLITS),
    "ifrs": _hv("tsmx", IFRS_FACTS, IFRS_BARS, ["2025-06-02"], fx=FX, ads=5),
    "ifrsNoFx": _hv("tsmx", IFRS_FACTS, IFRS_BARS, ["2025-06-02"], fx={"pair": "TWDUSD=X", "bars": []}, ads=5),
    "gbp": _hv("lse", SYN, BARS, ["2025-09-02"], currency="GBp"),
    "staleBalance": _hv("stale-bal", STALE_BAL_FACTS, STALE_BAL_BARS, ["2026-09-02"], SPLITS),
    "staleResults": _hv("stale-results", STALE_RESULTS_FACTS, STALE_RESULTS_BARS, ["2027-01-20"], SPLITS),
    "staleShares": _hv("stale-shares", SYN, STALE_RESULTS_BARS, ["2027-01-20"], SPLITS),
    "empty": _hv("none", {"facts": {}}, BARS, ["2025-09-02"]),
    # An annual (20-F) filer's balances 244 days old are its latest: EV stays computed.
    "ifrsLate": _hv("tsmx", IFRS_FACTS, [{"date": "2025-09-02", "close": 10.0}], ["2025-09-02"],
                    fx={"pair": "TWDUSD=X", "bars": [{"date": "2025-09-01", "close": 0.03}]}, ads=5),
    "multiWeighted": _hv("multi", MULTI, BARS, ["2025-09-02"], SPLITS),
    "multiCoverFailed": _hv("multi", MULTI, BARS, ["2025-09-02"], SPLITS, covers=COVER_FAILED),
    "multiClassSum": _hv("multi", MULTI, BARS, ["2025-09-02"], SPLITS, covers=CLASS_SUM),
    "regime": _hv("regime", REGIME, REGIME_BARS, ["2024-06-03", "2025-06-02"]),
    # TSM-like (2.5.30, F-015/F-018): the FY2025 20-F filed 2026-04-16 is in submissions but not in companyfacts.
    "ifrsMissingFy": {**_hv("tsmx", IFRS_FACTS, [{"date": "2025-06-02", "close": 10.0}, {"date": "2026-06-30", "close": 12.0}], ["2025-06-02", "2026-06-30"],
                            fx={"pair": "TWDUSD=X", "bars": [{"date": "2025-05-30", "close": 0.03}, {"date": "2026-06-30", "close": 0.031}]}, ads=5),
                      "periodicFilings": [{"accessionNumber": "f24", "form": "20-F", "filed": "2025-04-15", "reportDate": "2024-12-31"},
                                          {"accessionNumber": "f25", "form": "20-F", "filed": "2026-04-16", "reportDate": "2025-12-31"}]},
}

# NVDA: fiscal year ending late January, so its fiscal Q2 ends in calendar Q3.
FISCAL_RECON = {"facts": {"us-gaap": {"Revenues": _usd(
    _f("2025-01-27", "2026-01-25", 215_938_000_000, "10-K", "2026-02-26", "k26"),
    _f("2026-04-27", "2026-07-26", 96_221_000_000, "10-Q", "2026-08-27", "q227"),
)}}}
FISCAL_TEXTS = [
    "NVIDIA today reported revenue for the second quarter ended July 26, 2026, of $96.2 billion, up 56% from a year ago.",
    "Revenue for the second quarter of fiscal 2027 was $96.2 billion.",
    "Second-quarter revenue was $96.2 billion.",
    "Revenue for the first quarter of fiscal 2027 was $80.1 billion.",
    "Revenue for the fiscal 2027 second quarter was $96.2 billion.",
    "Full fiscal year 2026 revenue was $215.9 billion.",
]


def _fy(row, fy, fp):
    return {**row, "fy": fy, "fp": fp}


# The same periods with the issuer's fiscal metadata (companyfacts fy/fp), and Dollar General, which names a year
# ending in late January by the year it began.
FISCAL_META = {"facts": {"us-gaap": {"Revenues": _usd(
    _fy(_f("2025-01-27", "2026-01-25", 215_938_000_000, "10-K", "2026-02-26", "k26"), 2026, "FY"),
    _fy(_f("2026-04-27", "2026-07-26", 96_221_000_000, "10-Q", "2026-08-27", "q227"), 2027, "Q2"),
)}}}
DG_META = {"facts": {"us-gaap": {"RevenueFromContractWithCustomerExcludingAssessedTax": _usd(
    _fy(_f("2024-02-03", "2025-01-31", 40_612_308_000, "10-K", "2025-03-24", "dgk24"), 2024, "FY"),
    # The next 10-K's comparative carries that filing's fy (2025); the period stays fiscal 2024.
    _fy(_f("2024-02-03", "2025-01-31", 40_612_308_000, "10-K", "2026-03-20", "dgk"), 2025, "FY"),
    _fy(_f("2025-02-01", "2026-01-30", 42_000_000_000, "10-K", "2026-03-20", "dgk"), 2025, "FY"),
    _fy(_f("2026-05-02", "2026-07-31", 11_000_000_000, "10-Q", "2026-08-27", "dgq"), 2026, "Q2"),
)}}}
FISCAL_META_TEXTS = [
    "Revenue for the second quarter of fiscal 2027 was $96.2 billion.",
    "Second quarter fiscal 2026 revenue was $46.7 billion.",
    "Q2 fiscal 2027 revenue was $96.2 billion.",
    "Q2 FY27 revenue was $96.2 billion.",
    "FY2027 Q2 revenue was $96.2 billion.",
    "Fiscal 2026 second quarter revenue was $46.7 billion.",
    "Revenue for the fiscal second quarter was $96.2 billion.",
    "Revenue in the second fiscal quarter of fiscal 2027 was $96.2 billion.",
    "Q2'27 revenue was $96.2 billion.",
    "Full-year FY2026 revenue was $215.9 billion.",
    "Third quarter 2026 revenue was $96.2 billion.",
]
# Explicit FY<yyyy> selects by the issuer's fiscal year where companyfacts names it.
FY_SELECT = [
    [DG_META, "FY2025"], [DG_META, "FY2026"], [DG_META, "FY2024"],
    [FISCAL_META, "FY2026"], [FISCAL_META, "FY2027"],
    [FISCAL_RECON, "FY2026"],
]
DG_TEXTS = [
    ["latest_annual", "Net sales for fiscal 2025 were $42.0 billion."],
    ["latest_annual", "Net sales for fiscal 2026 were $44.0 billion."],
    ["latest_quarter", "Second quarter fiscal 2026 net sales were $11.0 billion."],
]

DATE_CASES = [[None, "2026-09-25"], [None, "2028-02-29"], [["2025-03-03", "2024-01-02", "2025-03-03"], None], [["2025-13-01"], None],
              [[f"2025-01-{d:02d}" for d in range(1, 14)], None], [None, None]]

# ── Reconciliation fixtures ──
Q226 = ("10-Q", "2026-08-10", "q226")
RECON = {"facts": {"us-gaap": {
    "RevenueFromContractWithCustomerIncludingAssessedTax": _usd(
        _f("2025-04-01", "2025-06-30", 20_000_000, "10-Q", "2025-08-11", "q225"),
        _f("2025-04-01", "2025-06-30", 21_000_000, *Q226),
        _f("2026-04-01", "2026-06-30", 31_520_000, *Q226),
        _f("2025-01-01", "2025-12-31", 70_918_000, "10-K", "2026-03-02", "k25"),
    ),
    "NetIncomeLoss": _usd(_f("2026-04-01", "2026-06-30", -230_909_000, *Q226)),
    "ProfitLoss": _usd(_f("2026-04-01", "2026-06-30", -299_919_000, *Q226)),
    "EarningsPerShareDiluted": {"units": {"USD/shares": [_f("2022-07-01", "2022-09-30", -0.18, "10-Q", "2022-11-14", "q322")]}},
    "CashAndCashEquivalentsAtCarryingValue": _usd(_f(None, "2026-06-30", 2_288_253_000, *Q226)),
}}}

RELEASES = [
    {"status": "READ", "url": "https://www.sec.gov/early.htm", "filingDate": "2026-07-15", "accessionNumber": "e1",
     "text": "Business update. The company announced a partnership with an operator."},
    {"status": "READ", "url": "https://www.sec.gov/q2.htm", "filingDate": "2026-08-10", "accessionNumber": "r2",
     "text": ("Second Quarter 2026 Results • Second quarter revenue was $31.5 million from government and commercial customers "
              "• As of June 30, 2026, we had cash, cash equivalents, and restricted cash of approximately $2.7 billion "
              "• Net loss of $230.9 million for the second quarter • Full year 2025 revenue was $70.9 million "
              "• The company expects revenue of $150 million to $200 million for the full year")},
]
YAHOO_Q = [{"date": "2026-06-30", "totalRevenue": 31_520_000.0, "netIncome": -230_909_000.0, "dilutedEPS": -0.77, "cashAndCashEquivalents": 2_288_253_000.0},
           {"date": "2025-06-30", "totalRevenue": 20_000_000.0}]
YAHOO_A = [{"date": "2025-12-31", "totalRevenue": 70_918_000.0}]

RECON_CASES = [
    ["revenue", "latest_quarter", RELEASES, YAHOO_Q, 0.5],
    ["net_income", "latest_quarter", RELEASES, YAHOO_Q, 0.5],
    ["eps_diluted", "latest_quarter", RELEASES, YAHOO_Q, 0.5],
    ["cash_and_equivalents", "Q2 2026", RELEASES, YAHOO_Q, 0.5],
    ["revenue", "Q2 2025", [], YAHOO_Q, 0.5],
    ["revenue", "FY2025", RELEASES, YAHOO_A, 0.5],
    ["revenue", "latest_quarter", [RELEASES[0]], None, 0.5],
    ["revenue", "latest_quarter", [{"status": "NOT_READ", "url": "u", "filingDate": "2026-08-10", "accessionNumber": "x", "text": None}], YAHOO_Q, 0.0],
]
# 2.5.15 fixtures. NBIS reported in RUB as Yandex to FY2023 and in USD from FY2024: the longer RUB history is not its unit.
NBIS_FACTS = {"facts": {"us-gaap": {"Revenues": {"units": {
    "RUB": [_f("2021-01-01", "2021-12-31", 1_000, "10-K", "2022-03-01", "n21"),
            _f("2022-01-01", "2022-12-31", 2_000, "10-K", "2023-03-01", "n22"),
            _f("2023-01-01", "2023-12-31", 3_000, "10-K", "2024-03-01", "n23")],
    "USD": [_f("2024-01-01", "2024-12-31", 117_500_000, "10-K", "2026-03-01", "n24"),
            _f("2025-01-01", "2025-12-31", 530_000_000, "10-K", "2026-03-01", "n25")],
}}}}}
# Same newest filing: the unit with more rows, then the name.
UNIT_TIE_FACTS = {"facts": {"us-gaap": {"Revenues": {"units": {
    "USD": [_f("2025-01-01", "2025-12-31", 10, "10-K", "2026-03-01", "u25")],
    "EUR": [_f("2024-01-01", "2024-12-31", 8, "10-K", "2026-03-01", "u25"), _f("2025-01-01", "2025-12-31", 9, "10-K", "2026-03-01", "u25")],
}}}}}
# BE FY2025: Revenues and contract revenue in one 10-K, and net income tagged only as ProfitLoss with its NCI share.
BE_FILED = ("10-K", "2026-02-26", "be26")
BE_FACTS = {"facts": {"us-gaap": {
    "RevenueFromContractWithCustomerExcludingAssessedTax": _usd(_f("2025-01-01", "2025-12-31", 2_001_614_000, *BE_FILED)),
    "Revenues": _usd(_f("2025-01-01", "2025-12-31", 2_023_994_000, *BE_FILED)),
    "ProfitLoss": _usd(_f("2025-01-01", "2025-12-31", -87_140_000, *BE_FILED)),
    "NetIncomeLossAttributableToNoncontrollingInterest": _usd(_f("2025-01-01", "2025-12-31", 1_294_000, *BE_FILED)),
}}}
BE_NO_NCI = json.loads(json.dumps(BE_FACTS))
del BE_NO_NCI["facts"]["us-gaap"]["NetIncomeLossAttributableToNoncontrollingInterest"]
BE_NCI_OTHER_FILING = json.loads(json.dumps(BE_FACTS))
BE_NCI_OTHER_FILING["facts"]["us-gaap"]["NetIncomeLossAttributableToNoncontrollingInterest"] = _usd(_f("2025-01-01", "2025-12-31", 1_294_000, "10-K", "2026-02-26", "other"))
BE_CASES = [["revenue", BE_FACTS], ["net_income", BE_FACTS], ["net_income", BE_NO_NCI], ["net_income", BE_NCI_OTHER_FILING]]

# Release text parsing (2.5.15), read against the file's Q2 2026 period (ended 2026-06-30).
RELEASE_TEXTS_2515 = [
    ["revenue", "Q2 revenue of $1.81B, up 12% year over year."],
    ["revenue", "Second quarter revenue was $808.4M and $2.5 bn a year ago."],
    ["net_income", "GAAP net income of $0.97 per diluted share for the second quarter."],
    ["net_income", "Second quarter GAAP net income was $12.0 million, or $0.97 per diluted share."],
    ["net_income", "Non-GAAP net income was $5.0 million; GAAP net income was $4.0 million for the second quarter."],
    ["net_income", "Adjusted net income of $5.0 million for the second quarter."],
    ["eps_diluted", "Diluted EPS was $0.77 per diluted share for the second quarter."],
    ["revenue", "Consolidated Statements of Operations (in thousands, except per share data) Three months ended June 30, 2026 Revenues $ 1,214,293"],
    ["revenue", "Consolidated Statements of Operations (in thousands) Segment table (in millions) Three months ended June 30, 2026 Revenues $ 1,214,293"],
    ["revenue", "Consolidated Statements of Operations (in millions) Three months ended June 30, 2026 Revenues $ 1,214"],
    ["revenue", "Statements (in thousands) Second quarter revenue was $31.5 million."],
    ["eps_diluted", "Statements (in thousands) Diluted net income per share was $0.77 for the second quarter."],
    ["revenue", "Second quarter revenue was $31,520,000."],
]
PERIOD_SPECS = ["latest_quarter", "latest_annual", "FY2025", "fy 2025", "Q2 2026", "Q4 2026", "2026", "Q2 2025"]
RELEASE_TEXTS = [
    ["eps_diluted", "Diluted loss per share of $(0.77) for the second quarter."],
    ["eps_diluted", "Net loss per diluted share was $0.77 in the second quarter."],
    ["revenue", "Vertiv reported second quarter net sales of $3,274 million, an increase of 24%."],
    ["revenue", "Fourth quarter and full year revenue were $20.1 million and $70.9 million."],
    ["revenue", "Revenue backlog increased to approximately $1.30 billion in the second quarter."],
    ["operating_income", "Operating loss for the second quarter was $(118.2) million."],
    ["revenue", "First quarter revenue was $99.0 million."],
    ["revenue", "Second quarter revenue was $31.5 million compared to $20.0 million in second quarter 2025."],
]


def _python_outputs() -> dict:
    periods = {spec: mr.resolve_period(RECON, "revenue", spec) for spec in PERIOD_SPECS}
    quarter = mr.resolve_period(RECON, "revenue", "Q2 2026")
    peers = [vh.historical_valuation({**HV_INPUTS["syn"], "ticker": t}) for t in ("p1", "p2")]
    peers[1]["points"][2]["multiples"]["LTM"]["evToRevenue"] = {"value": 3.0, "status": "OK"}
    return {
        "hv": {k: vh.historical_valuation(v) for k, v in HV_INPUTS.items()},
        "dates": [list(vh.valuation_dates(r, latest)) for r, latest in DATE_CASES],
        "medians": vh.peer_medians(DATES, peers),
        "foreign": [vh.foreign_filer(SYN), vh.foreign_filer(IFRS_FACTS), vh.foreign_filer({})],
        "latestShares": [vh.latest_share_count(SYN), vh.latest_share_count(IFRS_FACTS), vh.latest_share_count({})],
        "periods": periods,
        "recon": [mr.metric_reconciliation(ticker="asts", metric=m, period=mr.resolve_period(RECON, m, p), companyfacts=RECON, releases=r, yahoo_rows=y,
                                           tolerance_pct=t) for m, p, r, y, t in RECON_CASES],
        "releaseObs": [mr.release_observation({"status": "READ", "text": text, "url": None, "filingDate": None, "accessionNumber": None}, m, quarter, "USD")
                       for m, text in RELEASE_TEXTS],
        "currency": mr.release_observation(RELEASES[1], "revenue", quarter, "TWD"),
        "unitChoice": [_mf(NBIS_FACTS), _mf(UNIT_TIE_FACTS)],
        "beSecObs": [mr.sec_observations(facts, m, mr.resolve_period(facts, m, "latest_annual")) for m, facts in BE_CASES],
        "beRecon": mr.metric_reconciliation(ticker="be", metric="net_income", period=mr.resolve_period(BE_FACTS, "net_income", "latest_annual"),
                                            companyfacts=BE_FACTS, releases=[], yahoo_rows=None, tolerance_pct=0.5),
        "releaseObs2515": [mr.release_observation({"status": "READ", "text": text, "url": None, "filingDate": None, "accessionNumber": None}, m, quarter, "USD")
                           for m, text in RELEASE_TEXTS_2515],
        "noSecAgreement": mr.reconcile_observations([
            {"source": "ISSUER_RELEASE", "provider": "ISSUER_RELEASE", "status": "FOUND", "value": 100.0, "precision": 0},
            {"source": "YAHOO", "provider": "YAHOO", "status": "FOUND", "value": 100.0, "precision": 0},
        ], 0.5),
        "noSecConflict": mr.reconcile_observations([
            {"source": "ISSUER_RELEASE", "provider": "ISSUER_RELEASE", "status": "FOUND", "value": 100.0, "precision": 0},
            {"source": "YAHOO", "provider": "YAHOO", "status": "FOUND", "value": 120.0, "precision": 0},
        ], 0.5),
        "fiscalPeriod": mr.resolve_period(FISCAL_RECON, "revenue", "latest_quarter"),
        "fiscalObs": [mr.release_observation({"status": "READ", "text": t, "url": None, "filingDate": None, "accessionNumber": None}, "revenue",
                                             mr.resolve_period(FISCAL_RECON, "revenue", "latest_quarter"), "USD") for t in FISCAL_TEXTS],
        "latestRelease": mr.pick_release_observation([
            {"source": "ISSUER_RELEASE", "provider": "ISSUER_RELEASE", "status": "FOUND", "value": 31_500_000, "filingDate": "2026-08-10"},
            {"source": "ISSUER_RELEASE", "provider": "ISSUER_RELEASE", "status": "FOUND", "value": 31_600_000, "filingDate": "2026-08-20"},
        ]),
        "cover": [vh.cover_share_counts(h) for h in COVER_HTML],
        "coverNeeded": [vh.cover_reads_needed(MULTI, FILINGS, ["2025-03-03", "2025-09-02"]), vh.cover_reads_needed(SYN, FILINGS, DATES),
                        vh.cover_reads_needed(MULTI, None, DATES), vh.cover_reads_needed({}, FILINGS, DATES)],
        "foreignAsOf": [vh.foreign_filer(REGIME, "2024-06-03"), vh.foreign_filer(REGIME, "2025-06-02"), vh.foreign_filer(REGIME)],
        "fiscalMetaPeriod": mr.resolve_period(FISCAL_META, "revenue", "latest_quarter"),
        "fiscalMetaObs": [_read_obs(t, mr.resolve_period(FISCAL_META, "revenue", "latest_quarter")) for t in FISCAL_META_TEXTS],
        "fiscalDerivedObs": [_read_obs(t, mr.resolve_period(FISCAL_RECON, "revenue", "latest_quarter")) for t in FISCAL_META_TEXTS],
        "dgPeriods": [mr.resolve_period(DG_META, "revenue", s) for s in ("latest_quarter", "latest_annual")],
        "dgObs": [_read_obs(t, mr.resolve_period(DG_META, "revenue", s)) for s, t in DG_TEXTS],
        "normalized": [mr.normalize_fiscal_tokens(t) for t in FISCAL_META_TEXTS],
        "fySelect": [mr.resolve_period(f, "revenue", spec) for f, spec in FY_SELECT],
    }


def _mf(facts: dict) -> dict:
    taxonomy, unit, rows = mr.metric_facts(facts, "revenue")
    return {"taxonomy": taxonomy, "unit": unit, "rows": rows}


def _read_obs(text: str, period: dict) -> dict:
    return mr.release_observation({"status": "READ", "text": text, "url": None, "filingDate": None, "accessionNumber": None}, "revenue", period, "USD")


_HARNESS = r"""
const [vhUrl, mrUrl, fixturesPath] = process.argv.slice(-3);
const vh = await import(vhUrl);
const mr = await import(mrUrl);
const { readFileSync } = await import("node:fs");
const f = JSON.parse(readFileSync(fixturesPath, "utf8"));
const readObs = (text, period) => mr.releaseObservation({ status: "READ", text, url: null, filingDate: null, accessionNumber: null }, "revenue", period, "USD");
const periods = {};
for (const spec of f.periodSpecs) periods[spec] = mr.resolvePeriod(f.recon, "revenue", spec);
const quarter = mr.resolvePeriod(f.recon, "revenue", "Q2 2026");
const peers = ["p1", "p2"].map((t) => vh.historicalValuation({ ...f.hv.syn, ticker: t }));
peers[1].points[2].multiples.LTM.evToRevenue = { value: 3.0, status: "OK" };
const hv = {};
for (const [k, v] of Object.entries(f.hv)) hv[k] = vh.historicalValuation(v);
const out = {
  hv,
  dates: f.dateCases.map(([r, latest]) => { const d = vh.valuationDates(r, latest); return [d.dates, d.error]; }),
  medians: vh.peerMedians(f.dates, peers),
  foreign: [vh.foreignFiler(f.syn), vh.foreignFiler(f.ifrsFacts), vh.foreignFiler({})],
  latestShares: [vh.latestShareCount(f.syn), vh.latestShareCount(f.ifrsFacts), vh.latestShareCount({})],
  periods,
  recon: f.reconCases.map(([m, p, r, y, t]) => mr.metricReconciliation({ ticker: "asts", metric: m, period: mr.resolvePeriod(f.recon, m, p), companyfacts: f.recon, releases: r, yahooRows: y, tolerancePct: t })),
  releaseObs: f.releaseTexts.map(([m, text]) => mr.releaseObservation({ status: "READ", text, url: null, filingDate: null, accessionNumber: null }, m, quarter, "USD")),
  currency: mr.releaseObservation(f.releases[1], "revenue", quarter, "TWD"),
  unitChoice: [f.nbisFacts, f.unitTieFacts].map((facts) => mr.metricFacts(facts, "revenue")),
  beSecObs: f.beCases.map(([m, facts]) => mr.secObservations(facts, m, mr.resolvePeriod(facts, m, "latest_annual"))),
  beRecon: mr.metricReconciliation({ ticker: "be", metric: "net_income", period: mr.resolvePeriod(f.beFacts, "net_income", "latest_annual"), companyfacts: f.beFacts, releases: [], yahooRows: null, tolerancePct: 0.5 }),
  releaseObs2515: f.releaseTexts2515.map(([m, text]) => mr.releaseObservation({ status: "READ", text, url: null, filingDate: null, accessionNumber: null }, m, quarter, "USD")),
  noSecAgreement: (() => {
    const r = mr.reconcileObservations([
      { source: "ISSUER_RELEASE", provider: "ISSUER_RELEASE", status: "FOUND", value: 100.0, precision: 0 },
      { source: "YAHOO", provider: "YAHOO", status: "FOUND", value: 100.0, precision: 0 },
    ], 0.5);
    return [r.comparisons, r.status, r.restated];
  })(),
  noSecConflict: (() => {
    const r = mr.reconcileObservations([
      { source: "ISSUER_RELEASE", provider: "ISSUER_RELEASE", status: "FOUND", value: 100.0, precision: 0 },
      { source: "YAHOO", provider: "YAHOO", status: "FOUND", value: 120.0, precision: 0 },
    ], 0.5);
    return [r.comparisons, r.status, r.restated];
  })(),
  fiscalPeriod: mr.resolvePeriod(f.fiscalRecon, "revenue", "latest_quarter"),
  fiscalObs: f.fiscalTexts.map((text) => mr.releaseObservation({ status: "READ", text, url: null, filingDate: null, accessionNumber: null }, "revenue",
    mr.resolvePeriod(f.fiscalRecon, "revenue", "latest_quarter"), "USD")),
  latestRelease: mr.pickReleaseObservation([
    { source: "ISSUER_RELEASE", provider: "ISSUER_RELEASE", status: "FOUND", value: 31_500_000, filingDate: "2026-08-10" },
    { source: "ISSUER_RELEASE", provider: "ISSUER_RELEASE", status: "FOUND", value: 31_600_000, filingDate: "2026-08-20" },
  ]),
  cover: f.coverHtml.map((h) => vh.coverShareCounts(h)),
  coverNeeded: [vh.coverReadsNeeded(f.multi, f.filings, ["2025-03-03", "2025-09-02"]), vh.coverReadsNeeded(f.syn, f.filings, f.dates),
    vh.coverReadsNeeded(f.multi, null, f.dates), vh.coverReadsNeeded({}, f.filings, f.dates)],
  foreignAsOf: [vh.foreignFiler(f.regime, "2024-06-03"), vh.foreignFiler(f.regime, "2025-06-02"), vh.foreignFiler(f.regime)],
  fiscalMetaPeriod: mr.resolvePeriod(f.fiscalMeta, "revenue", "latest_quarter"),
  fiscalMetaObs: f.fiscalMetaTexts.map((t) => readObs(t, mr.resolvePeriod(f.fiscalMeta, "revenue", "latest_quarter"))),
  fiscalDerivedObs: f.fiscalMetaTexts.map((t) => readObs(t, mr.resolvePeriod(f.fiscalRecon, "revenue", "latest_quarter"))),
  dgPeriods: ["latest_quarter", "latest_annual"].map((s) => mr.resolvePeriod(f.dgMeta, "revenue", s)),
  dgObs: f.dgTexts.map(([s, t]) => readObs(t, mr.resolvePeriod(f.dgMeta, "revenue", s))),
  normalized: f.fiscalMetaTexts.map((t) => mr.normalizeFiscalTokens(t)),
  fySelect: f.fySelect.map(([facts, spec]) => mr.resolvePeriod(facts, "revenue", spec)),
};
console.log(JSON.stringify(out));
"""


def _worker_outputs() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        bundles = []
        for name in ("valuation-history", "metric-reconciliation"):
            out = Path(tmp) / f"{name}.mjs"
            subprocess.run([str(ESBUILD), str(WORKER / "src" / f"{name}.ts"), "--bundle", "--format=esm", "--platform=neutral", f"--outfile={out}", "--log-level=error"],
                           cwd=WORKER, check=True, capture_output=True, text=True, timeout=120)
            bundles.append(out.as_uri())
        fx = Path(tmp) / "fixtures.json"
        fx.write_text(json.dumps({"hv": HV_INPUTS, "dateCases": DATE_CASES, "dates": DATES, "syn": SYN, "ifrsFacts": IFRS_FACTS, "periodSpecs": PERIOD_SPECS,
                                  "recon": RECON, "reconCases": RECON_CASES, "releaseTexts": RELEASE_TEXTS, "releases": RELEASES, "fiscalRecon": FISCAL_RECON, "fiscalTexts": FISCAL_TEXTS,
                                  "coverHtml": COVER_HTML, "multi": MULTI, "filings": FILINGS, "regime": REGIME, "fiscalMeta": FISCAL_META, "fiscalMetaTexts": FISCAL_META_TEXTS,
                                  "dgMeta": DG_META, "dgTexts": DG_TEXTS, "fySelect": FY_SELECT,
                                  "nbisFacts": NBIS_FACTS, "unitTieFacts": UNIT_TIE_FACTS, "beFacts": BE_FACTS, "beCases": BE_CASES,
                                  "releaseTexts2515": RELEASE_TEXTS_2515}), encoding="utf-8")
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


def _point(result: dict, date: str) -> dict:
    return next(p for p in result["points"] if p["date"] == date)


class TestHistoricalValuation(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.r = vh.historical_valuation(HV_INPUTS["syn"])

    def test_point_in_time_and_split(self) -> None:
        p = _point(self.r, "2025-03-03")
        # Yahoo's 50 is adjusted for the later 2:1 split; the price then was 100.
        self.assertEqual((p["price"]["close"], p["price"]["laterSplitFactor"]), (100, 2))
        self.assertEqual((p["shares"]["value"], p["shares"]["basis"]), (10, "COVER_PAGE"))
        self.assertEqual(p["marketCap"]["value"], 1000)
        # Only the 10-K filed 2025-02-15 is known: LTM is the fiscal year.
        self.assertEqual(p["denominators"]["LTM"]["revenue"]["method"], "LAST_FISCAL_YEAR_IS_LATEST")
        self.assertEqual(p["balances"]["balanceDate"], "2024-12-31")
        self.assertEqual(p["enterpriseValue"]["value"], 1200)
        self.assertEqual(p["multiples"]["LFY"]["evToRevenue"]["value"], 1.2)

    def test_split_after_share_count(self) -> None:
        p = _point(self.r, "2025-07-01")
        self.assertEqual(p["marketCap"]["status"], "SPLIT_AFTER_SHARE_COUNT")
        self.assertEqual(p["multiples"]["LTM"]["priceToSales"]["status"], "MARKETCAP_NOT_AVAILABLE")

    def test_ltm_components_and_balances(self) -> None:
        p = _point(self.r, "2025-09-02")
        ltm = p["denominators"]["LTM"]
        self.assertEqual((ltm["revenue"]["value"], ltm["revenue"]["method"], ltm["revenue"]["periodEnd"]), (1110, "LFY_PLUS_YTD_MINUS_PRIOR_YTD", "2025-06-30"))
        self.assertEqual([c["sign"] for c in ltm["revenue"]["components"]], [1, 1, -1])
        # D&A: the 10-K total has no year-to-date, so the separately tagged parts are used for LTM (VRT).
        self.assertEqual(ltm["ebitda"]["value"], 230 + 53)
        self.assertIn("Depreciation + AmortizationOfIntangibleAssets", ltm["ebitda"]["method"])
        self.assertEqual(p["denominators"]["LFY"]["ebitda"]["value"], 250)
        debt = p["balances"]["debt"]
        self.assertEqual((debt["value"], [c["concept"] for c in debt["components"]]), (315, ["LongTermDebtCurrent", "LongTermDebtNoncurrent", "CommercialPaper"]))
        self.assertEqual(p["enterpriseValue"]["value"], 1400 + 315 - 150 - 20)
        self.assertEqual(p["multiples"]["LTM"]["priceToEarnings"]["value"], round(1400 / 130, 2))

    def test_missing_price_and_fundamentals(self) -> None:
        self.assertEqual(_point(self.r, "2021-01-04")["status"], "PRICE_UNAVAILABLE")
        self.assertEqual(vh.historical_valuation(HV_INPUTS["empty"])["status"], "FUNDAMENTALS_NOT_AVAILABLE")

    def test_ifrs_fx_and_ads(self) -> None:
        r = vh.historical_valuation(HV_INPUTS["ifrs"])
        self.assertEqual((r["taxonomy"], r["reportingCurrency"]), ("ifrs-full", "TWD"))
        p = r["points"][0]
        self.assertEqual(p["shares"]["quotedShareEquivalent"], 100)
        self.assertEqual(p["marketCap"]["value"], 1000)
        self.assertEqual(p["fx"]["rate"], 0.03)
        self.assertEqual(p["enterpriseValue"]["value"], 1000 + (1000 - 3000) * 0.03)
        self.assertEqual(p["multiples"]["LFY"]["priceToSales"]["value"], round(1000 / 600, 2))
        self.assertEqual(p["balances"]["shortTermInvestments"]["status"], "NOT_MAPPED")
        no_fx = vh.historical_valuation(HV_INPUTS["ifrsNoFx"])["points"][0]
        self.assertEqual(no_fx["enterpriseValue"]["status"], "FX_NOT_AVAILABLE")
        self.assertEqual(no_fx["multiples"]["LFY"]["priceToSales"]["status"], "FX_NOT_AVAILABLE")

    def test_minor_currency_units(self) -> None:
        p = vh.historical_valuation(HV_INPUTS["gbp"])["points"][0]
        self.assertEqual((p["price"]["close"], p["price"]["currency"]), (0.7, "GBP"))
        self.assertEqual(p["fx"]["status"], "NOT_AVAILABLE")

    def test_nonpositive_numerator(self) -> None:
        facts = json.loads(json.dumps(SYN))
        facts["facts"]["us-gaap"]["CashAndCashEquivalentsAtCarryingValue"]["units"]["USD"][1]["val"] = 5000
        p = _point(vh.historical_valuation({**HV_INPUTS["syn"], "companyfacts": facts}), "2025-09-02")
        self.assertLess(p["enterpriseValue"]["value"], 0)
        self.assertEqual(p["multiples"]["LTM"]["evToRevenue"]["status"], "NOT_MEANINGFUL_NONPOSITIVE_NUMERATOR")

    def test_dates_and_medians(self) -> None:
        self.assertEqual(vh.valuation_dates(None, "2028-02-29")[0], ["2023-02-28", "2024-02-29", "2025-02-28", "2026-02-28", "2027-02-28", "2028-02-29"])
        self.assertEqual(vh.valuation_dates(["2025-03-03", "2024-01-02", "2025-03-03"], None)[0], ["2024-01-02", "2025-03-03"])
        self.assertIsNotNone(vh.valuation_dates(["2025-13-01"], None)[1])
        medians = next(m for m in _python_outputs()["medians"] if m["date"] == "2025-09-02")
        self.assertEqual(medians["LTM"]["evToRevenue"], {"median": vh._median([1.39, 3.0]), "count": 2, "tickers": ["P1", "P2"]})

    def test_stale_inputs_fail_closed(self) -> None:
        balance = vh.historical_valuation(HV_INPUTS["staleBalance"])["points"][0]
        self.assertEqual(balance["enterpriseValue"]["status"], "BALANCES_STALE")
        self.assertIsNone(balance["multiples"]["LTM"]["evToRevenue"]["value"])
        results = vh.historical_valuation(HV_INPUTS["staleResults"])["points"][0]
        self.assertEqual(results["multiples"]["LTM"]["priceToSales"]["status"], "RESULTS_STALE")
        self.assertIsNone(results["multiples"]["LTM"]["priceToSales"]["value"])
        shares = vh.historical_valuation(HV_INPUTS["staleShares"])["points"][0]
        self.assertEqual(shares["marketCap"]["status"], "SHARE_COUNT_STALE")
        self.assertIsNone(shares["multiples"]["LTM"]["priceToSales"]["value"])

    def test_preferred_concept_is_stable_across_later_alternate_filing(self) -> None:
        facts = json.loads(json.dumps(SYN))
        facts["facts"]["us-gaap"]["ProfitLoss"]["units"]["USD"].append(
            _f("2024-01-01", "2024-12-31", 999, "10-K/A", "2025-08-20", "alt")
        )
        p = _point(vh.historical_valuation({**HV_INPUTS["syn"], "companyfacts": facts}), "2025-09-02")
        self.assertEqual(p["denominators"]["LFY"]["netIncome"]["components"][0]["concept"], "NetIncomeLoss")
        self.assertEqual(p["denominators"]["LFY"]["netIncome"]["value"], 100)

    def test_stale_peer_multiple_is_excluded(self) -> None:
        peers = [
            {"ticker": "GOOD", "points": [{"date": "2026-09-02", "multiples": {"LTM": {"evToRevenue": {"status": "OK", "value": 5.0}}, "LFY": {}}}]},
            {"ticker": "STALE", "points": [{"date": "2026-09-02", "multiples": {"LTM": {"evToRevenue": {"status": "RESULTS_STALE", "value": None}}, "LFY": {}}}]},
        ]
        m = vh.peer_medians(["2026-09-02"], peers)[0]["LTM"]["evToRevenue"]
        self.assertEqual(m, {"median": 5.0, "count": 1, "tickers": ["GOOD"]})

    def test_weighted_average_never_sets_market_cap(self) -> None:
        for key, cover_status in (("multiWeighted", "NO_PERIODIC_FILING"), ("multiCoverFailed", "CONTEXT_NOT_READ")):
            p = vh.historical_valuation(HV_INPUTS[key])["points"][0]
            self.assertEqual((p["shares"]["basis"], p["shares"]["value"], p["shares"]["pointInTime"]), ("WEIGHTED_AVERAGE_BASIC", 15, False))
            self.assertEqual(p["shares"]["coverPageRead"]["status"], cover_status)
            self.assertEqual(p["shares"]["coverPageRead"]["accessionNumber"], "q225" if key == "multiCoverFailed" else None)
            self.assertEqual((p["marketCap"]["status"], p["marketCap"]["value"]), ("POINT_IN_TIME_SHARES_UNRESOLVED", None))
            self.assertEqual(p["enterpriseValue"]["status"], "MARKET_CAP_NOT_AVAILABLE")
            self.assertEqual((p["status"], p["coreStatus"]), ("PARTIAL", "PARTIAL"))
            self.assertIn("POINT_IN_TIME_SHARES_UNRESOLVED", [w["code"] for w in p["warnings"]])

    def test_cover_page_classes_are_summed(self) -> None:
        p = vh.historical_valuation(HV_INPUTS["multiClassSum"])["points"][0]
        self.assertEqual((p["shares"]["basis"], p["shares"]["value"], p["shares"]["asOf"], p["shares"]["accessionNumber"]),
                         ("COVER_PAGE_CLASS_SUM", 25, "2025-08-01", "q225"))
        self.assertEqual(p["marketCap"]["value"], 70 * 25)
        self.assertEqual(p["enterpriseValue"]["value"], 1750 + 315 - 150 - 20)
        self.assertIn("Common Class A: 20, Common Class B: 5", next(w["message"] for w in p["warnings"] if w["code"] == "SHARE_CLASSES_SUMMED"))
        needed = _python_outputs()["coverNeeded"]
        # The latest report filed by each date, when companyfacts has no undimensioned count as new as its period:
        # every date for the multi-class filer; for the other, only where its cover count lags a later 10-Q.
        self.assertEqual([[f["accessionNumber"] for f in n] for n in needed], [["k24", "q225"], ["q125"], [], []])
        # A multi-class filer whose weighted-average count stopped (ASTS after 2022) still gets the current cover page.
        facts = json.loads(json.dumps(MULTI))
        facts["facts"]["us-gaap"]["WeightedAverageNumberOfSharesOutstandingBasic"]["units"]["shares"] = [_f("2024-07-01", "2024-09-30", 9, "10-Q", "2024-11-05", "q324")]
        p = vh.historical_valuation({**HV_INPUTS["multiClassSum"], "companyfacts": facts})["points"][0]
        self.assertEqual((p["shares"]["basis"], p["shares"]["accessionNumber"], p["shares"]["value"]), ("COVER_PAGE_CLASS_SUM", "q225", 25))

    def test_cover_share_counts(self) -> None:
        out = _python_outputs()["cover"]
        self.assertEqual((out[0]["status"], out[0]["basis"], out[0]["value"], out[0]["asOf"]), ("OK", "COVER_PAGE_CLASS_SUM", 311_004_416, "2026-08-06"))
        self.assertEqual([c["class"] for c in out[0]["classes"]], ["Common Class A", "Common Class B"])
        self.assertEqual((out[1]["basis"], out[1]["value"]), ("COVER_PAGE", 311_004_416))
        self.assertEqual([o["status"] for o in out[2:5]], ["OTHER_DIMENSIONS", "CONTEXT_NOT_READ", "NOT_TAGGED"])
        self.assertEqual(out[5]["value"], 1_234_000)
        self.assertEqual(out[6]["status"], "VALUE_NOT_PARSED")

    def test_status_requires_every_multiple(self) -> None:
        full = _point(self.r, "2025-09-02")
        self.assertEqual((full["status"], full["coreStatus"], full["coverage"]["multiplesAvailable"]), ("OK", "OK", 8))
        facts = json.loads(json.dumps(SYN))
        for concept in ("DepreciationDepletionAndAmortization", "Depreciation", "AmortizationOfIntangibleAssets"):
            del facts["facts"]["us-gaap"][concept]
        r = vh.historical_valuation({**HV_INPUTS["syn"], "companyfacts": facts})
        p = _point(r, "2025-09-02")
        # EV/EBITDA without D&A: the core values stand, the point does not claim completeness.
        self.assertEqual((p["status"], p["coreStatus"]), ("PARTIAL", "OK"))
        self.assertEqual(p["coverage"]["multiplesAvailable"], 6)
        self.assertEqual({(u["basis"], u["multiple"], u["status"]) for u in p["coverage"]["unavailable"]},
                         {("LTM", "evToEbitda", "DENOMINATOR_NOT_AVAILABLE"), ("LFY", "evToEbitda", "DENOMINATOR_NOT_AVAILABLE")})
        self.assertEqual(r["status"], "PARTIAL")

    def test_cadence_is_point_in_time(self) -> None:
        self.assertEqual(_python_outputs()["foreignAsOf"], [True, False, False])
        r = vh.historical_valuation(HV_INPUTS["regime"])
        self.assertEqual([p["secCompanyfactsCadence"] for p in r["points"]], ["ANNUAL", "QUARTERLY"])
        self.assertEqual(r["secCompanyfactsCadence"], "MIXED")
        self.assertEqual((r["stalenessLimitsDays"]["ANNUAL"]["balances"], r["stalenessLimitsDays"]["QUARTERLY"]["balances"]), (500, 200))
        self.assertNotIn("reportingCadence", r)

    def test_authority_boundary(self) -> None:
        for key, value in AUTHORITY_BOUNDARY.items():
            self.assertEqual(self.r[key], value)


def _obs(result: dict, source: str) -> dict:
    return next(o for o in result["observations"] if o["source"] == source)


class TestStaleFiguresAndCoverage(unittest.TestCase):
    """2.5.30 (F-018, F-015): each denominator and the balances carry their age and a stale flag; a latest annual
    report missing from companyfacts is warned at the dates it was already filed."""

    def test_a_stale_denominator_says_so_on_itself(self) -> None:
        late = _point(vh.historical_valuation(HV_INPUTS["ifrsMissingFy"]), "2026-06-30")
        revenue = late["denominators"]["LTM"]["revenue"]
        # Status and value unchanged: the age and the flag are added, so a caller reading only this block sees it.
        self.assertEqual((revenue["status"], revenue["periodEnd"], revenue["method"], revenue["periodAgeDays"], revenue["stale"]),
                         ("OK", "2024-12-31", "LAST_FISCAL_YEAR_IS_LATEST", 546, True))
        self.assertEqual(list(revenue)[-2:], ["periodAgeDays", "stale"])
        for basis in ("LTM", "LFY"):
            for name, d in late["denominators"][basis].items():
                if d["status"] == "OK":
                    self.assertEqual((d["periodAgeDays"], d["stale"]), (546, True), (basis, name))
        self.assertEqual((late["balances"]["status"], late["balances"]["balanceAgeDays"], late["balances"]["stale"]), ("OK", 546, True))
        # The multiples stay null, as before.
        self.assertEqual(late["coverage"]["multiplesAvailable"], 0)

    def test_a_fresh_denominator_is_not_stale(self) -> None:
        early = _point(vh.historical_valuation(HV_INPUTS["ifrsMissingFy"]), "2025-06-02")
        revenue = early["denominators"]["LTM"]["revenue"]
        self.assertEqual((revenue["periodAgeDays"], revenue["stale"]), (153, False))
        self.assertEqual((early["balances"]["balanceAgeDays"], early["balances"]["stale"]), (153, False))
        # A figure that is not OK carries no age.
        unavailable = [d for inp in HV_INPUTS.values() for pt in vh.historical_valuation(inp)["points"]
                       for basis in pt.get("denominators", {}).values() for d in basis.values() if d["status"] != "OK"]
        self.assertTrue(unavailable)
        self.assertFalse([d for d in unavailable if "periodAgeDays" in d or "stale" in d])

    def test_the_missing_annual_report_is_warned_once_filed(self) -> None:
        result = vh.historical_valuation(HV_INPUTS["ifrsMissingFy"])
        late, early = _point(result, "2026-06-30"), _point(result, "2025-06-02")
        warning = next(w for w in late["warnings"] if w["code"] == "LATEST_ANNUAL_NOT_IN_COMPANYFACTS")
        self.assertEqual((warning["accessionNumber"], warning["form"], warning["filed"], warning["latestCompanyfactsAnnualPeriodEnd"]), ("f25", "20-F", "2026-04-16", "2024-12-31"))
        self.assertTrue(warning["message"].startswith("The latest annual report filed by 2026-06-30 (20-F f25, filed 2026-04-16, period 2025-12-31) has no us-gaap or ifrs-full fact"))
        # Before the FY2025 20-F was filed, the latest annual report (f24) is in companyfacts: no warning.
        self.assertNotIn("LATEST_ANNUAL_NOT_IN_COMPANYFACTS", [w["code"] for w in early["warnings"]])
        # Without submissions there is nothing to compare: no warning.
        bare = _point(vh.historical_valuation(HV_INPUTS["ifrsLate"]), "2025-09-02")
        self.assertNotIn("LATEST_ANNUAL_NOT_IN_COMPANYFACTS", [w["code"] for w in bare["warnings"]])


class TestReconciliation(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()["recon"]

    def test_revenue_agreed_with_release_precision(self) -> None:
        r = self.out[0]
        self.assertEqual((r["status"], r["period"]["periodEnd"]), ("AGREED", "2026-06-30"))
        rel = _obs(r, "ISSUER_RELEASE")
        # The first Item 2.02 8-K after the quarter is not the results release; the next one is read.
        self.assertEqual((rel["value"], rel["filingDate"], len(rel["releasesConsidered"])), (31_500_000, "2026-08-10", 2))
        cmp = next(c for c in r["comparisons"] if c["source"] == "ISSUER_RELEASE")
        # The larger of 0.5% of the SEC value and half the release's last stated digit ($0.1 million).
        self.assertEqual((cmp["result"], cmp["tolerance"]), ("MATCH", 157_600))
        sec = {"source": "SEC_XBRL_LATEST", "provider": "SEC", "status": "FOUND", "value": 31_520_000, "precision": 0}
        comparisons, status, _ = mr.reconcile_observations([sec, {**rel, "provider": "ISSUER_RELEASE"}], 0)
        self.assertEqual((comparisons[0]["result"], comparisons[0]["tolerance"], status), ("MATCH", 50_000, "AGREED"))
        comparisons, status, _ = mr.reconcile_observations([sec, {**rel, "value": 31_400_000}], 0)
        self.assertEqual((comparisons[0]["result"], status), ("MISMATCH", "CONFLICT"))

    def test_agreement_basis_names_what_agreed_rests_on(self) -> None:
        # 2.5.22 (F-004): AGREED is SEC plus one or two of the release and Yahoo; Alpha Vantage is never read.
        read = ["SEC", "ISSUER_RELEASE", "YAHOO"]
        full = self.out[0]["agreementBasis"]
        self.assertEqual(full, {"providersRead": read, "providersFound": read, "providersAgreeingWithSec": ["ISSUER_RELEASE", "YAHOO"],
                                "independentChecks": 2, "notRead": ["ALPHA_VANTAGE"]})
        self.assertEqual(list(self.out[0])[5:7], ["status", "agreementBasis"])
        # The release was not found: Yahoo alone agrees with SEC (the as-first-filed SEC row is not an independent check).
        one = self.out[3]
        self.assertEqual((one["status"], one["agreementBasis"]["providersFound"], one["agreementBasis"]["providersAgreeingWithSec"], one["agreementBasis"]["independentChecks"]),
                         ("AGREED", ["SEC", "YAHOO"], ["YAHOO"], 1))
        # A provider that differs from SEC is not counted; PARTIAL and CONFLICT carry the basis too.
        conflict = self.out[4]["agreementBasis"]
        self.assertEqual((conflict["providersAgreeingWithSec"], conflict["independentChecks"]), ([], 0))
        self.assertEqual(self.out[2]["agreementBasis"]["providersFound"], ["YAHOO"])
        self.assertEqual(self.out[6]["agreementBasis"]["independentChecks"], 0)
        for r in self.out:
            self.assertEqual(r["agreementBasis"]["independentChecks"], len(r["agreementBasis"]["providersAgreeingWithSec"]))
            self.assertEqual(r["agreementBasis"]["notRead"], ["ALPHA_VANTAGE"])
            self.assertIn("agreementBasis names the providers that matched SEC", r["notes"][0])

    def test_concept_precedence(self) -> None:
        r = self.out[1]
        sec = _obs(r, "SEC_XBRL_LATEST")
        self.assertEqual((sec["value"], sec["evidence"]["concept"]), (-230_909_000, "NetIncomeLoss"))
        self.assertEqual(sec["otherConcepts"][0]["concept"], "ProfitLoss")
        self.assertFalse(r["restated"])
        self.assertEqual(_obs(r, "ISSUER_RELEASE")["value"], -230_900_000)
        self.assertEqual(r["status"], "AGREED")

    def test_periods_come_from_revenue(self) -> None:
        r = self.out[2]
        self.assertEqual(r["period"]["periodEnd"], "2026-06-30")
        self.assertEqual(_obs(r, "SEC_XBRL_LATEST")["status"], "NOT_FOUND")
        self.assertEqual(r["status"], "PARTIAL")

    def test_different_aggregate_not_read(self) -> None:
        r = self.out[3]
        self.assertEqual(_obs(r, "ISSUER_RELEASE")["status"], "NOT_FOUND_IN_TEXT")
        self.assertEqual(r["status"], "AGREED")

    def test_restatement_and_conflict(self) -> None:
        r = self.out[4]
        self.assertTrue(r["restated"])
        self.assertEqual(_obs(r, "SEC_XBRL_LATEST")["filedValues"], [20_000_000, 21_000_000])
        self.assertEqual(r["status"], "CONFLICT")
        self.assertEqual(_obs(r, "ISSUER_RELEASE")["status"], "NOT_RESOLVED")

    def test_annual_scope(self) -> None:
        r = self.out[5]
        self.assertEqual(_obs(r, "ISSUER_RELEASE")["value"], 70_900_000)
        self.assertEqual(r["status"], "AGREED")

    def test_partial_and_unread(self) -> None:
        self.assertEqual(self.out[6]["status"], "PARTIAL")
        self.assertEqual(_obs(self.out[6], "YAHOO")["status"], "NOT_READ")
        self.assertEqual(_obs(self.out[7], "ISSUER_RELEASE")["status"], "NOT_READ")

    def test_release_rules(self) -> None:
        obs = _python_outputs()["releaseObs"]
        self.assertEqual([o.get("value") for o in obs[:3]], [-0.77, -0.77, 3_274_000_000])
        self.assertEqual(obs[2]["precision"], 500_000)
        # "Fourth quarter and full year" is not scoped to a quarter alone; backlog is not revenue.
        self.assertEqual([o["status"] for o in obs[3:5]], ["NOT_FOUND_IN_TEXT", "NOT_FOUND_IN_TEXT"])
        self.assertEqual(obs[3]["unscopedCandidates"], 1)
        self.assertEqual(obs[5]["value"], -118_200_000)
        self.assertEqual(_python_outputs()["currency"]["status"], "NOT_COMPARED_CURRENCY")

    def test_reporting_unit_is_the_one_filed_most_recently(self) -> None:
        nbis, tie = _python_outputs()["unitChoice"]
        # NBIS: three RUB rows filed to 2024 against two USD rows filed 2026: USD.
        self.assertEqual((nbis["taxonomy"], nbis["unit"], [r["val"] for r in nbis["rows"]]), ("us-gaap", "USD", [117_500_000, 530_000_000]))
        # The same newest filing: the unit with more rows.
        self.assertEqual((tie["unit"], len(tie["rows"])), ("EUR", 2))

    def test_revenue_takes_the_larger_of_two_concepts_in_one_filing(self) -> None:
        latest, first = _python_outputs()["beSecObs"][0]
        self.assertEqual((latest["value"], latest["evidence"]["concept"]), (2_023_994_000, "Revenues"))
        self.assertEqual(latest["otherConcepts"], [{"concept": "RevenueFromContractWithCustomerExcludingAssessedTax", "value": 2_001_614_000,
                                                    "form": "10-K", "filed": "2026-02-26", "accessionNumber": "be26"}])
        self.assertEqual(first["value"], 2_023_994_000)

    def test_profit_loss_is_reduced_by_the_noncontrolling_share(self) -> None:
        latest, first = _python_outputs()["beSecObs"][1]
        self.assertEqual((latest["value"], latest["basis"]), (-88_434_000, "PARENT_DERIVED_FROM_PROFITLOSS"))
        self.assertEqual((first["value"], first["basis"]), (-88_434_000, "PARENT_DERIVED_FROM_PROFITLOSS"))
        self.assertEqual({k: latest["evidence"][k] for k in ("concept", "value", "profitLoss", "noncontrollingInterest", "derivation")},
                         {"concept": "ProfitLoss", "value": -88_434_000, "profitLoss": -87_140_000, "noncontrollingInterest": 1_294_000,
                          "derivation": "ProfitLoss - NetIncomeLossAttributableToNoncontrollingInterest"})
        self.assertEqual((latest["filedValues"], latest["otherConcepts"]), ([-87_140_000], []))
        # Without the NCI share in the same accession, ProfitLoss stands as tagged and carries no basis.
        for plain in _python_outputs()["beSecObs"][2:]:
            self.assertEqual(plain[0]["value"], -87_140_000)
            self.assertNotIn("basis", plain[0])
        # NetIncomeLoss, when tagged, is never adjusted (asserted by test_concept_precedence).
        recon = _python_outputs()["beRecon"]
        self.assertEqual(_obs(recon, "SEC_XBRL_LATEST")["value"], -88_434_000)

    def test_release_abbreviations_per_share_and_non_gaap(self) -> None:
        obs = _python_outputs()["releaseObs2515"]
        self.assertEqual((obs[0]["status"], obs[0]["value"], obs[0]["precision"]), ("FOUND", 1_810_000_000, 5_000_000))
        self.assertEqual(obs[1]["value"], 808_400_000, "the first amount, scaled by M; the bn amount after it is not read")
        # A per-share figure is not net income; the rejection is counted.
        self.assertEqual((obs[2]["status"], obs[2]["value"], obs[2]["rejectedCandidates"]), ("NOT_FOUND_IN_TEXT", None, 1))
        self.assertEqual(obs[3]["value"], 12_000_000)
        # The GAAP figure after a non-GAAP one is read; an adjusted-only sentence is not.
        self.assertEqual((obs[4]["status"], obs[4]["value"]), ("FOUND", 4_000_000))
        self.assertEqual((obs[5]["status"], obs[5]["rejectedCandidates"]), ("NOT_FOUND_IN_TEXT", 1))
        # A per-share metric keeps its per-share figure.
        self.assertEqual((obs[6]["status"], obs[6]["value"]), ("FOUND", 0.77))
        self.assertNotIn("rejectedCandidates", obs[4])

    def test_release_table_scale(self) -> None:
        obs = _python_outputs()["releaseObs2515"]
        # "(in thousands, except per share data)" scales an unscaled table figure, and says so.
        self.assertEqual((obs[7]["status"], obs[7]["value"], obs[7]["scaleBasis"]), ("FOUND", 1_214_293_000, "RELEASE_TABLE_IN_THOUSANDS"))
        self.assertEqual(obs[7]["asWritten"], "$ 1,214,293")
        # Two declared scales: an unscaled amount is not read.
        self.assertEqual((obs[8]["status"], obs[8]["value"], obs[8]["rejectedCandidates"]), ("NOT_FOUND_IN_TEXT", None, 1))
        self.assertEqual((obs[9]["value"], obs[9]["scaleBasis"]), (1_214_000_000, "RELEASE_TABLE_IN_MILLIONS"))
        # A written scale beats the table's, and per-share figures are never table-scaled.
        self.assertEqual(obs[10]["value"], 31_500_000)
        self.assertNotIn("scaleBasis", obs[10])
        self.assertEqual(obs[11]["value"], 0.77)
        self.assertNotIn("scaleBasis", obs[11])
        # No declared scale: as written.
        self.assertEqual(obs[12]["value"], 31_520_000)
        self.assertNotIn("scaleBasis", obs[12])

    def test_periods(self) -> None:
        p = _python_outputs()["periods"]
        self.assertEqual(p["latest_annual"]["periodEnd"], "2025-12-31")
        self.assertEqual(p["fy 2025"]["periodEnd"], "2025-12-31")
        self.assertEqual(p["Q4 2026"]["status"], "PERIOD_NOT_FOUND")
        self.assertEqual(p["2026"]["status"], "INVALID_PERIOD")

    def test_agreement_requires_sec_baseline(self) -> None:
        comparisons, status, _ = _python_outputs()["noSecAgreement"]
        self.assertEqual(status, "PARTIAL")
        # Without SEC the other sources are still compared, so agreement is visible but never AGREED...
        self.assertEqual([(c["source"], c["against"], c["result"]) for c in comparisons], [("YAHOO", "ISSUER_RELEASE", "MATCH")])
        # ...and a disagreement between them is a CONFLICT, not a quiet PARTIAL.
        comparisons, status, _ = _python_outputs()["noSecConflict"]
        self.assertEqual((status, comparisons[0]["result"]), ("CONFLICT", "MISMATCH"))

    def test_fiscal_quarter_naming(self) -> None:
        out = _python_outputs()
        period = out["fiscalPeriod"]
        self.assertEqual((period["periodEnd"], period["fiscalQuarter"], period["fiscalYears"]), ("2026-07-26", 2, [2027, 2026]))
        obs = out["fiscalObs"]
        # The exact end date, the fiscal quarter and the hyphenated form all name NVDA's fiscal Q2 (calendar Q3).
        self.assertEqual([o["status"] for o in obs[:3]], ["FOUND", "FOUND", "FOUND"])
        self.assertEqual(obs[0]["value"], 96_200_000_000)
        # A different fiscal quarter is still rejected.
        self.assertEqual((obs[3]["status"], obs[3]["unscopedCandidates"]), ("NOT_FOUND_IN_TEXT", 1))
        self.assertEqual(obs[4]["status"], "FOUND")
        # A full-year figure is still not a quarter's.
        self.assertEqual(obs[5]["status"], "NOT_FOUND_IN_TEXT")

    def test_fy_selector_uses_issuer_fiscal_year(self) -> None:
        dg25, dg26, dg24, nv26, nv27, derived = _python_outputs()["fySelect"]
        # Dollar General's fiscal 2025 is the year ended 2026-01-30, not the one ending in calendar 2025.
        self.assertEqual((dg25["periodStart"], dg25["periodEnd"], dg25["fiscalYears"], dg25["fiscalYearSource"]),
                         ("2025-02-01", "2026-01-30", [2025], "SEC_FY_FP"))
        # Fiscal 2026 is not filed yet: not found, rather than fiscal 2025 by its end year.
        self.assertEqual((dg26["status"], dg26["fiscalYearsAvailable"]), ("PERIOD_NOT_FOUND", [2024, 2025]))
        # The first-reported fy names the period; a later 10-K's comparative tagged fy 2025 does not.
        self.assertEqual((dg24["periodEnd"], dg24["fiscalYears"]), ("2025-01-31", [2024]))
        # NVDA names a year by its end: fiscal 2026 ended 2026-01-25; fiscal 2027 is not filed yet.
        self.assertEqual((nv26["periodEnd"], nv26["fiscalYears"]), ("2026-01-25", [2026]))
        self.assertEqual(nv27["status"], "PERIOD_NOT_FOUND")
        # Without fy/fp the year the period ends is used, as before.
        self.assertEqual((derived["periodEnd"], derived["fiscalYearSource"]), ("2026-01-25", "PERIOD_END_YEAR"))
        self.assertIn("issuer's fiscal year", dg25["labelBasis"])
        # Calendar filers are unchanged.
        cal = _python_outputs()["periods"]["FY2025"]
        self.assertEqual((cal["periodEnd"], cal["fiscalYears"]), ("2025-12-31", [2025]))

    def test_annual_filer_staleness_limits(self) -> None:
        r = vh.historical_valuation(HV_INPUTS["ifrsLate"])
        self.assertEqual((r["secCompanyfactsCadence"], r["stalenessLimitsDays"]["ANNUAL"]["balances"]), ("ANNUAL", 500))
        p = r["points"][0]
        self.assertEqual((p["balances"]["balanceDate"], p["enterpriseValue"]["status"]), ("2024-12-31", "OK"))
        self.assertEqual(vh.historical_valuation(HV_INPUTS["syn"])["secCompanyfactsCadence"], "QUARTERLY")

    def test_fiscal_identity_from_issuer_metadata(self) -> None:
        out = _python_outputs()
        period = out["fiscalMetaPeriod"]
        self.assertEqual((period["fiscalQuarter"], period["fiscalYears"], period["fiscalYearSource"]), (2, [2027], "SEC_FY_FP"))
        self.assertNotIn("fiscalYearAmbiguous", period)
        found = [o["status"] == "FOUND" for o in out["fiscalMetaObs"]]
        # Q2 fiscal 2027 in every wording; never fiscal 2026's second quarter; calendar Q3 2026 is the same period.
        self.assertEqual(found, [True, False, True, True, True, False, True, True, True, False, True])
        # Without metadata, both adjacent years remain possible for a January year end, and that is flagged.
        derived = mr.resolve_period(FISCAL_RECON, "revenue", "latest_quarter")
        self.assertEqual((derived["fiscalYears"], derived["fiscalYearAmbiguous"]), ([2027, 2026], True))
        self.assertEqual(out["fiscalDerivedObs"][1]["status"], "FOUND")
        dg_quarter, dg_annual = out["dgPeriods"]
        self.assertEqual((dg_quarter["fiscalQuarter"], dg_quarter["fiscalYears"], dg_annual["fiscalYears"]), (2, [2026], [2025]))
        self.assertEqual([o["status"] for o in out["dgObs"]], ["FOUND", "NOT_FOUND_IN_TEXT", "FOUND"])
        self.assertEqual(out["normalized"][3], "Q2 fiscal 2027 revenue was $96.2 billion.")
        # The evidence keeps the sentence as written.
        self.assertEqual(out["fiscalMetaObs"][3]["sentence"], "Q2 FY27 revenue was $96.2 billion.")

    def test_latest_parseable_release_wins(self) -> None:
        latest = _python_outputs()["latestRelease"]
        self.assertEqual((latest["filingDate"], latest["value"]), ("2026-08-20", 31_600_000))

    def test_exact_quarter_scope(self) -> None:
        obs = _python_outputs()["releaseObs"]
        self.assertEqual(obs[6]["status"], "NOT_FOUND_IN_TEXT")
        self.assertEqual(obs[7]["status"], "FOUND")
        self.assertEqual(obs[7]["value"], 31_500_000)

    def test_authority_boundary(self) -> None:
        for key, value in AUTHORITY_BOUNDARY.items():
            self.assertEqual(self.out[0][key], value)


if __name__ == "__main__":
    unittest.main()
