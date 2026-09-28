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


def _hv(ticker, facts, bars, dates, splits=None, fx=None, ads=None, currency="USD"):
    return {"ticker": ticker, "dates": dates, "companyfacts": facts, "bars": bars, "priceCurrency": currency, "splits": splits or [], "fx": fx, "adsRatio": ads}


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
    }


_HARNESS = r"""
const [vhUrl, mrUrl, fixturesPath] = process.argv.slice(-3);
const vh = await import(vhUrl);
const mr = await import(mrUrl);
const { readFileSync } = await import("node:fs");
const f = JSON.parse(readFileSync(fixturesPath, "utf8"));
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
                                  "recon": RECON, "reconCases": RECON_CASES, "releaseTexts": RELEASE_TEXTS, "releases": RELEASES, "fiscalRecon": FISCAL_RECON, "fiscalTexts": FISCAL_TEXTS}), encoding="utf-8")
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

    def test_authority_boundary(self) -> None:
        for key, value in AUTHORITY_BOUNDARY.items():
            self.assertEqual(self.r[key], value)


def _obs(result: dict, source: str) -> dict:
    return next(o for o in result["observations"] if o["source"] == source)


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

    def test_annual_filer_staleness_limits(self) -> None:
        r = vh.historical_valuation(HV_INPUTS["ifrsLate"])
        self.assertEqual((r["reportingCadence"], r["stalenessLimitsDays"]["balances"]), ("ANNUAL", 500))
        p = r["points"][0]
        self.assertEqual((p["balances"]["balanceDate"], p["enterpriseValue"]["status"]), ("2024-12-31", "OK"))
        self.assertEqual(vh.historical_valuation(HV_INPUTS["syn"])["reportingCadence"], "QUARTERLY")

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
