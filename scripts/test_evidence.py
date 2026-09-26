#!/usr/bin/env python3
"""Evidence composition parity (2.5.0): worker/src/evidence.ts vs yfmcp/evidence.py.

Covers canonical JSON (the bytes an evidence cut is hashed over), the FY0-FY+5
consensus curve with per-provider, per-period coverage states, EPS revision
windows, evidence-quality preflight, the receipt, and the authority boundary
every payload carries.
"""

from __future__ import annotations

import hashlib
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

from yfmcp import evidence as ev  # noqa: E402

AS_OF = "2026-09-26T08:00:00.000Z"


def _r(v):
    return {"raw": v, "fmt": str(v)}


YAHOO_TREND = [
    {"period": "0q", "endDate": "2026-09-30", "earningsEstimate": {"avg": _r(-0.44), "numberOfAnalysts": _r(6)}},
    {
        "period": "0y", "endDate": "2026-12-31",
        "earningsEstimate": {"avg": _r(-2.27), "low": _r(-2.49), "high": _r(-2.04), "numberOfAnalysts": _r(8)},
        "revenueEstimate": {"avg": _r(168_000_000), "low": _r(150_980_000), "high": _r(189_000_000), "numberOfAnalysts": _r(12)},
        "epsTrend": {"current": _r(-2.27), "7daysAgo": _r(-2.26), "30daysAgo": _r(-1.39), "60daysAgo": _r(-1.5), "90daysAgo": _r(-1.5)},
        "epsRevisions": {"upLast7days": _r(0), "upLast30days": _r(0), "downLast30days": _r(4), "downLast7Days": _r(1)},
    },
    {
        "period": "+1y", "endDate": "2027-12-31",
        "earningsEstimate": {"avg": _r(-1.1), "low": _r(-1.98), "high": _r(1.48), "numberOfAnalysts": _r(9)},
        "revenueEstimate": {"avg": _r(520_000_000), "low": _r(345_057_500), "high": _r(834_100_000), "numberOfAnalysts": _r(11)},
        "epsTrend": {"current": _r(-1.1), "7daysAgo": _r(-1.05), "30daysAgo": _r(-0.69), "60daysAgo": None, "90daysAgo": _r(-0.65)},
        "epsRevisions": {"upLast7days": _r(1), "upLast30days": _r(1), "downLast30days": _r(2)},
    },
]

AV = {"symbol": "ASTS", "estimates": [
    {"date": "2027-12-31", "horizon": "fiscal year", "eps_estimate_average": "-1.1016", "eps_estimate_high": "1.4800", "eps_estimate_low": "-1.9800",
     "eps_estimate_analyst_count": "9.0000", "eps_estimate_average_7_days_ago": "-1.0493", "eps_estimate_average_30_days_ago": "-0.6863",
     "eps_estimate_average_60_days_ago": "-0.6476", "eps_estimate_average_90_days_ago": "-0.6476", "eps_estimate_revision_up_trailing_7_days": "1.0000",
     "eps_estimate_revision_down_trailing_7_days": None, "eps_estimate_revision_up_trailing_30_days": "1.0000", "eps_estimate_revision_down_trailing_30_days": "2.0000",
     "revenue_estimate_average": "650775060.00", "revenue_estimate_high": "834100000.00", "revenue_estimate_low": "345057500.00", "revenue_estimate_analyst_count": "11.00"},
    {"date": "2026-12-31", "horizon": "fiscal year", "eps_estimate_average": "-2.2839", "eps_estimate_high": "-2.0400", "eps_estimate_low": "-2.4900",
     "eps_estimate_analyst_count": "8.0000", "eps_estimate_average_7_days_ago": "-2.2815", "eps_estimate_average_30_days_ago": "-1.3862",
     "eps_estimate_average_60_days_ago": "-1.5051", "eps_estimate_average_90_days_ago": "-1.5051", "eps_estimate_revision_up_trailing_7_days": "0.0000",
     "eps_estimate_revision_down_trailing_7_days": None, "eps_estimate_revision_up_trailing_30_days": "0.0000", "eps_estimate_revision_down_trailing_30_days": "4.0000",
     "revenue_estimate_average": "168843670.00", "revenue_estimate_high": "189000000.00", "revenue_estimate_low": "150980000.00", "revenue_estimate_analyst_count": "12.00"},
    {"date": "2026-09-30", "horizon": "fiscal quarter", "eps_estimate_average": "-0.4463", "eps_estimate_analyst_count": "6.0000"},
    {"date": "2021-12-31", "horizon": "fiscal year", "eps_estimate_average": "0.0000", "eps_estimate_analyst_count": "0.0000",
     "revenue_estimate_average": "0.00", "revenue_estimate_analyst_count": "0.00"},
]}

# A 52/53-week filer with two thinly covered years, a TWD ADR, and a provider fiscal-year mismatch.
THIN_TREND = [
    {"period": "0y", "endDate": "2026-09-26", "earningsEstimate": {"avg": 1.2, "numberOfAnalysts": 2}, "revenueEstimate": {"avg": 5e8, "numberOfAnalysts": 2}},
    {"period": "+1y", "endDate": "2027-09-25", "earningsEstimate": {"avg": 1.5, "numberOfAnalysts": 3}, "revenueEstimate": {}},
]
ADR_TREND = [{"period": "0y", "endDate": "2026-12-31", "earningsEstimate": {"avg": 60.1, "numberOfAnalysts": 20, "earningsCurrency": "TWD"},
              "revenueEstimate": {"avg": 3.6e12, "numberOfAnalysts": 20, "revenueCurrency": "TWD"}}]
ADR_OTHER = {"provider": "other", "status": "OK", "retrievedAt": AS_OF, "providerTimestamp": None, "message": None, "periods": [
    {"providerPeriodLabel": "fiscal year", "fiscalYearEnd": "2026-12-31", "fiscalYearEndBasis": "PROVIDER_STATED",
     "eps": {"mean": 9.4, "high": None, "low": None, "analystCount": 20, "currency": "USD", "currencyBasis": "PROVIDER_STATED"},
     "revenue": {"mean": None, "high": None, "low": None, "analystCount": None, "currency": None, "currencyBasis": "NOT_STATED"},
     "epsTrend": None, "epsRevisions": None},
    {"providerPeriodLabel": "fiscal year", "fiscalYearEnd": "2027-09-30", "fiscalYearEndBasis": "PROVIDER_STATED",
     "eps": {"mean": 11.0, "high": None, "low": None, "analystCount": 20, "currency": "USD", "currencyBasis": "PROVIDER_STATED"},
     "revenue": {"mean": None, "high": None, "low": None, "analystCount": None, "currency": None, "currencyBasis": "NOT_STATED"},
     "epsTrend": None, "epsRevisions": None},
]}
ADR_TREND_NEXT = ADR_TREND + [{"period": "+1y", "endDate": "2027-12-31", "earningsEstimate": {"avg": 70.0, "numberOfAnalysts": 20, "earningsCurrency": "TWD"}}]
# yfinance DataFrames carry no endDate; the local server derives it.
DERIVED_TREND = [{"period": "0y", "earningsEstimate": {"avg": 3.0, "numberOfAnalysts": 12}, "revenueEstimate": {"avg": 1e9, "numberOfAnalysts": 12}}]

FILINGS = [
    {"form": "10-Q", "filingDate": "2026-08-10", "reportDate": "2026-06-30", "items": "", "isInlineXBRL": True},
    {"form": "8-K", "filingDate": "2026-08-10", "reportDate": "2026-08-10", "items": "2.02,9.01", "isInlineXBRL": True},
    {"form": "8-K", "filingDate": "2026-08-05", "reportDate": "2026-08-05", "items": "7.01,9.01", "isInlineXBRL": False},
    {"form": "10-K", "filingDate": "2026-03-01", "reportDate": "2025-12-31", "items": "", "isInlineXBRL": True},
    {"form": "8-K", "filingDate": "2026-03-01", "reportDate": "2026-03-01", "items": "2.02", "isInlineXBRL": True},
]
STALE_FILINGS = [{"form": "10-K", "filingDate": "2025-03-01", "reportDate": "2024-12-31", "items": "", "isInlineXBRL": False}]

CANONICAL_CASES = [
    {"b": [1, 2.5, -0.0, 1e21, 1.5e21, 1e-7, 0.000001, 123.456, 31520000.0, -2.2839, 1e16, 5e-324], "a": "é\n\"q\"\u0001", "c": None, "d": True},
    {"z": {"y": [], "x": {}}, "m": 0.1, "n": 100, "k": "ASTS"},
]

COMPONENT_TEXTS = [
    ("extract_capital_structure", json.dumps({"basis": "COMPANY_DISCLOSED", "source": {"filingDate": "2026-08-10", "periodEnd": "2026-06-30", "form": "10-Q"}, "warnings": [{"code": "X"}]})),
    ("extract_guidance", json.dumps({"status": "GUIDANCE_NOT_AVAILABLE", "reason": "No guidance found"})),
    ("extract_dilution_bridge", json.dumps({"error": True, "code": "TICKER_NOT_FOUND", "message": "no CIK"})),
    ("list_sec_material_filings", json.dumps({"ok": False, "data": None, "error": {"code": "RATE_LIMIT", "message": "slow"}})),
    ("get_quote", "not json"),
]


def _python_outputs() -> dict:
    yahoo = ev.yahoo_consensus_input(YAHOO_TREND, retrieved_at=AS_OF, financial_currency="USD")
    av = ev.alpha_vantage_consensus_input(AV, retrieved_at=AS_OF)
    thin = ev.yahoo_consensus_input(THIN_TREND, retrieved_at=AS_OF, financial_currency="USD")
    adr = ev.yahoo_consensus_input(ADR_TREND_NEXT, retrieved_at=AS_OF)
    derived = ev.yahoo_consensus_input(DERIVED_TREND, retrieved_at=AS_OF, financial_currency="EUR",
                                       fiscal_year_ends={"0y": "2026-12-31"}, fiscal_year_end_basis="DERIVED_FROM_NEXT_FISCAL_YEAR_END")
    conflict = ev.alpha_vantage_consensus_input({"estimates": [dict(AV["estimates"][1], revenue_estimate_average="120000000")]}, retrieved_at=AS_OF)
    curve = ev.build_consensus_curve("asts", [yahoo, av], AS_OF)
    quality = ev.evidence_quality(ticker="ASTS", as_of=AS_OF, quote={"price": 81.2, "currency": "USD", "priceTime": "2026-09-25T20:00:00.000Z", "status": "OK"},
                                  filings=FILINGS, filings_status="OK", consensus=curve, storage_available=True)
    components = {name: ev.component_from_tool_text(name, text, AS_OF) for name, text in COMPONENT_TEXTS}
    hashes = {name: hashlib.sha256(ev.canonical_json(c).encode()).hexdigest() for name, c in components.items()}
    receipt = ev.build_receipt(ticker="asts", evidence_cutoff=AS_OF, server_version="2.5.0", build_sha="abc", runtime="test",
                               components=components, component_hashes=hashes, components_sha256="0" * 64)
    return {
        "canonical": [ev.canonical_json(c) for c in CANONICAL_CASES],
        "curve": curve,
        "curveOneYear": ev.build_consensus_curve("asts", [yahoo, av], AS_OF, {**ev.DEFAULT_CONSENSUS_POLICY, "horizonYears": 1}),
        "curveThin": ev.build_consensus_curve("thin", [thin], AS_OF),
        "curveAdr": ev.build_consensus_curve("tsm", [adr, ADR_OTHER], AS_OF),
        "curveDerived": ev.build_consensus_curve("sap", [derived], AS_OF),
        "curveConflict": ev.build_consensus_curve("asts", [yahoo, conflict], AS_OF),
        "curveAvOnly": ev.build_consensus_curve("asts", [av], AS_OF),
        "curveNone": ev.build_consensus_curve("none", [ev.yahoo_consensus_input([], retrieved_at=AS_OF, status="PROVIDER_ERROR", message="down")], AS_OF),
        "revisions": ev.build_eps_revisions("asts", [yahoo, av], AS_OF),
        "quality": quality,
        "qualityStale": ev.evidence_quality(ticker="old", as_of=AS_OF, quote=None, filings=STALE_FILINGS, filings_status="OK", consensus=None, storage_available=False),
        "qualityNoSec": ev.evidence_quality(ticker="iqe.l", as_of=AS_OF, quote={"price": 12.5, "currency": "GBp", "priceTime": "2026-09-10T16:00:00.000Z", "status": "OK"},
                                            filings=None, filings_status="TICKER_NOT_FOUND", consensus=curve, storage_available=True),
        "components": components,
        "receipt": receipt,
        "cutId": ev.evidence_cut_id("asts", AS_OF, "a" * 64),
        "parsed": ev.parse_evidence_cut_id(ev.evidence_cut_id("brk.b", AS_OF, "b" * 64)),
        "parsedBad": ev.parse_evidence_cut_id("ec1_../x_20260926T080000Z_" + "c" * 64),
        "observationKey": ev.consensus_observation_key("asts", AS_OF),
    }


_HARNESS = r"""
const [bundleUrl, fixturesPath] = process.argv.slice(-2);
const m = await import(bundleUrl);
const { readFileSync } = await import("node:fs");
const f = JSON.parse(readFileSync(fixturesPath, "utf8"));
const AS_OF = f.asOf;
const yahoo = m.yahooConsensusInput(f.yahooTrend, { retrievedAt: AS_OF, financialCurrency: "USD" });
const av = m.alphaVantageConsensusInput(f.av, { retrievedAt: AS_OF });
const thin = m.yahooConsensusInput(f.thinTrend, { retrievedAt: AS_OF, financialCurrency: "USD" });
const adr = m.yahooConsensusInput(f.adrTrend, { retrievedAt: AS_OF });
const derived = m.yahooConsensusInput(f.derivedTrend, { retrievedAt: AS_OF, financialCurrency: "EUR", fiscalYearEnds: { "0y": "2026-12-31" }, fiscalYearEndBasis: "DERIVED_FROM_NEXT_FISCAL_YEAR_END" });
const conflict = m.alphaVantageConsensusInput({ estimates: [{ ...f.av.estimates[1], revenue_estimate_average: "120000000" }] }, { retrievedAt: AS_OF });
const curve = m.buildConsensusCurve("asts", [yahoo, av], AS_OF);
const quality = m.evidenceQuality({ ticker: "ASTS", asOf: AS_OF, quote: { price: 81.2, currency: "USD", priceTime: "2026-09-25T20:00:00.000Z", status: "OK" }, filings: f.filings, filingsStatus: "OK", consensus: curve, storageAvailable: true });
const components = Object.fromEntries(f.componentTexts.map(([name, text]) => [name, m.componentFromToolText(name, text, AS_OF)]));
const { createHash } = await import("node:crypto");
const hashes = Object.fromEntries(Object.entries(components).map(([k, c]) => [k, createHash("sha256").update(m.canonicalJson(c)).digest("hex")]));
const receipt = m.buildReceipt({ ticker: "asts", evidenceCutoff: AS_OF, serverVersion: "2.5.0", buildSha: "abc", runtime: "test", components, componentHashes: hashes, componentsSha256: "0".repeat(64) });
const out = {
  canonical: f.canonical.map((c) => m.canonicalJson(c)),
  curve,
  curveOneYear: m.buildConsensusCurve("asts", [yahoo, av], AS_OF, { ...m.DEFAULT_CONSENSUS_POLICY, horizonYears: 1 }),
  curveThin: m.buildConsensusCurve("thin", [thin], AS_OF),
  curveAdr: m.buildConsensusCurve("tsm", [adr, f.adrOther], AS_OF),
  curveDerived: m.buildConsensusCurve("sap", [derived], AS_OF),
  curveConflict: m.buildConsensusCurve("asts", [yahoo, conflict], AS_OF),
  curveAvOnly: m.buildConsensusCurve("asts", [av], AS_OF),
  curveNone: m.buildConsensusCurve("none", [m.yahooConsensusInput([], { retrievedAt: AS_OF, status: "PROVIDER_ERROR", message: "down" })], AS_OF),
  revisions: m.buildEpsRevisions("asts", [yahoo, av], AS_OF),
  quality,
  qualityStale: m.evidenceQuality({ ticker: "old", asOf: AS_OF, quote: null, filings: f.staleFilings, filingsStatus: "OK", consensus: null, storageAvailable: false }),
  qualityNoSec: m.evidenceQuality({ ticker: "iqe.l", asOf: AS_OF, quote: { price: 12.5, currency: "GBp", priceTime: "2026-09-10T16:00:00.000Z", status: "OK" }, filings: null, filingsStatus: "TICKER_NOT_FOUND", consensus: curve, storageAvailable: true }),
  components,
  receipt,
  cutId: m.evidenceCutId("asts", AS_OF, "a".repeat(64)),
  parsed: m.parseEvidenceCutId(m.evidenceCutId("brk.b", AS_OF, "b".repeat(64))),
  parsedBad: m.parseEvidenceCutId("ec1_../x_20260926T080000Z_" + "c".repeat(64)),
  observationKey: m.consensusObservationKey("asts", AS_OF),
};
console.log(JSON.stringify(out));
"""


def _worker_outputs() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    fixtures = {
        "asOf": AS_OF, "yahooTrend": YAHOO_TREND, "av": AV, "thinTrend": THIN_TREND, "adrTrend": ADR_TREND_NEXT, "adrOther": ADR_OTHER,
        "derivedTrend": DERIVED_TREND, "filings": FILINGS, "staleFilings": STALE_FILINGS, "componentTexts": COMPONENT_TEXTS,
        # JSON cannot carry -0.0 distinctly from 0 in every parser; both runtimes format it as 0.
        "canonical": CANONICAL_CASES,
    }
    with tempfile.TemporaryDirectory() as tmp:
        bundle = Path(tmp) / "evidence.mjs"
        harness = Path(tmp) / "harness.mjs"
        fx = Path(tmp) / "fixtures.json"
        fx.write_text(json.dumps(fixtures), encoding="utf-8")
        harness.write_text(_HARNESS, encoding="utf-8")
        subprocess.run([str(ESBUILD), str(WORKER / "src" / "evidence.ts"), "--bundle", "--format=esm", "--platform=neutral", f"--outfile={bundle}", "--log-level=error"],
                       cwd=WORKER, check=True, capture_output=True, text=True, timeout=120)
        result = subprocess.run([node, str(harness), bundle.as_uri(), str(fx)], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


def _cell(curve: dict, label: str, metric: str) -> dict:
    return next(p for p in curve["periods"] if p["label"] == label)["metrics"][metric]


class TestEvidenceParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.py = _python_outputs()
        cls.ts = _worker_outputs()

    def test_outputs_match(self) -> None:
        for key in self.py:
            self.assertEqual(json.loads(json.dumps(self.py[key])), self.ts[key], key)

    def test_canonical_bytes_match(self) -> None:
        self.assertEqual(self.py["canonical"], self.ts["canonical"])
        self.assertTrue(self.py["canonical"][0].startswith('{"a":"é\\n\\"q\\"\\u0001","b":[1,2.5,0,1e+21,1.5e+21,1e-7,0.000001,123.456,31520000,-2.2839,10000000000000000,5e-324]'))


class TestConsensusCurve(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()

    def test_fy0_fy1_are_covered_by_both_providers(self) -> None:
        curve = self.out["curve"]
        self.assertEqual(curve["fiscalYearBasis"], {"fiscalYearEnd": "2026-12-31", "basis": "yahoo_finance:0y"})
        eps = _cell(curve, "FY0", "eps")
        self.assertEqual(eps["coverage"], "PROVIDER_COVERED")
        self.assertEqual([p["provider"] for p in eps["providers"]], ["yahoo_finance", "alpha_vantage"])
        self.assertEqual(eps["agreement"]["status"], "AGREED")
        self.assertEqual(eps["agreement"]["currencyIdentity"], "UNVERIFIED")  # Alpha Vantage states no currency
        rev = _cell(curve, "FY+1", "revenue")
        self.assertEqual(rev["coverage"], "PROVIDER_CONFLICT")  # 520M vs 650.8M
        self.assertEqual(rev["agreement"]["relativeDiffPct"], 20.1)

    def test_years_beyond_fy1_stay_visibly_missing(self) -> None:
        curve = self.out["curve"]
        self.assertEqual([p["label"] for p in curve["periods"]], ["FY0", "FY+1", "FY+2", "FY+3", "FY+4", "FY+5"])
        for label in ("FY+2", "FY+3", "FY+4", "FY+5"):
            for metric in ("eps", "revenue"):
                cell = _cell(curve, label, metric)
                self.assertEqual((cell["coverage"], cell["providers"]), ("PROVIDER_NOT_COVERED", []))
        self.assertEqual(next(p for p in curve["periods"] if p["label"] == "FY+3")["fiscalYear"], 2029)
        self.assertEqual({m["metric"] for m in curve["metricsNotCoveredByAnyProvider"]}, {"ebitda", "ebit", "freeCashFlow", "capex", "grossMargin", "ebitdaMargin"})
        self.assertEqual(_cell(curve, "FY0", "eps")["dispersionStatistics"]["median"], {"value": None, "state": "PROVIDER_NOT_COVERED"})
        self.assertEqual(len(self.out["curveOneYear"]["periods"]), 2)

    def test_no_selected_value_and_authority_boundary(self) -> None:
        for key in ("curve", "revisions", "quality"):
            payload = self.out[key]
            self.assertEqual(payload["decisionUse"], "EVIDENCE_ONLY")
            for field in ("selectedMethod", "selectedMultiple", "scenarioWeights", "priceTarget", "g2", "opportunity", "action"):
                self.assertIn(field, payload)
                self.assertIsNone(payload[field])
        eps = _cell(self.out["curve"], "FY0", "eps")
        self.assertNotIn("mean", eps)
        self.assertNotIn("value", eps)

    def test_thin_coverage_and_52_week_years(self) -> None:
        thin = self.out["curveThin"]
        self.assertEqual(_cell(thin, "FY0", "eps")["coverage"], "INSUFFICIENT_ANALYST_COUNT")
        self.assertEqual(_cell(thin, "FY+1", "eps")["coverage"], "PROVIDER_COVERED")  # 2027-09-25 is FY+1 of 2026-09-26
        self.assertEqual(_cell(thin, "FY+1", "revenue")["coverage"], "PROVIDER_NOT_COVERED")

    def test_currency_and_period_identity_conflicts(self) -> None:
        adr = self.out["curveAdr"]
        fy0 = _cell(adr, "FY0", "eps")
        self.assertEqual((fy0["coverage"], fy0["agreement"]["status"], fy0["agreement"]["currencies"]), ("PROVIDER_CONFLICT", "CURRENCY_MISMATCH", ["TWD", "USD"]))
        fy1 = _cell(adr, "FY+1", "eps")
        self.assertEqual((fy1["coverage"], fy1["agreement"]["status"]), ("PROVIDER_CONFLICT", "PERIOD_IDENTITY_MISMATCH"))

    def test_derived_fiscal_year_is_labelled(self) -> None:
        eps = _cell(self.out["curveDerived"], "FY0", "eps")["providers"][0]
        self.assertEqual((eps["fiscalYearEndBasis"], eps["currency"], eps["currencyBasis"]), ("DERIVED_FROM_NEXT_FISCAL_YEAR_END", "EUR", "FINANCIAL_CURRENCY"))

    def test_av_zero_count_rows_are_not_coverage(self) -> None:
        av = self.out["curveAvOnly"]
        self.assertEqual(av["fiscalYearBasis"]["basis"], "earliest_provider_fiscal_year_on_or_after_as_of")
        self.assertNotIn("2021-12-31", json.dumps(av["periods"]))
        none = self.out["curveNone"]
        self.assertEqual(none["coverageSummary"], {"PROVIDER_NOT_COVERED": 12})
        self.assertEqual(none["providers"][0]["status"], "PROVIDER_ERROR")

    def test_eps_revision_windows(self) -> None:
        rev = self.out["revisions"]
        fy0 = rev["periods"][0]["providers"]
        yahoo = next(p for p in fy0 if p["provider"] == "yahoo_finance")
        self.assertEqual(yahoo["windows"]["30d"], {"mean": -1.39, "change": -0.88, "changePct": -63.31})
        self.assertEqual(yahoo["revisionCounts"], {"up7d": 0, "down7d": 1, "up30d": 0, "down30d": 4})
        av1 = next(p for p in rev["periods"][1]["providers"] if p["provider"] == "alpha_vantage")
        self.assertIn("revisionCounts.down7d", av1["notReported"])
        y1 = next(p for p in rev["periods"][1]["providers"] if p["provider"] == "yahoo_finance")
        self.assertEqual(y1["windows"]["60d"], {"mean": None, "change": None, "changePct": None})
        self.assertEqual(rev["revenueRevisions"]["coverage"], "PROVIDER_NOT_COVERED")


class TestEvidenceQualityAndReceipt(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()

    def test_quality_families(self) -> None:
        q = self.out["quality"]["families"]
        self.assertEqual(q["secPeriodicFiling"]["state"], "READY")
        self.assertEqual((q["guidance"]["state"], q["guidance"]["latestEarningsRelease8k"]), ("READY_TO_EXTRACT", "2026-08-10"))
        self.assertEqual(q["materialEvents"]["eightKCount90d"], 2)
        self.assertEqual(q["consensus"]["state"], "PARTIAL")
        self.assertIn({"family": "consensus", "code": "PROVIDER_CONFLICT", "message": "FY+1.revenue is PROVIDER_CONFLICT."}, self.out["quality"]["blockers"])
        stale = self.out["qualityStale"]["families"]
        self.assertEqual((stale["quote"]["state"], stale["secPeriodicFiling"]["state"], stale["capitalStructure"]["state"], stale["evidenceStorage"]["state"]),
                         ("MISSING", "STALE", "PARTIAL", "UNAVAILABLE"))
        no_sec = self.out["qualityNoSec"]["families"]
        self.assertEqual((no_sec["quote"]["state"], no_sec["secPeriodicFiling"]["state"], no_sec["dilution"]["state"]), ("STALE", "UNAVAILABLE", "UNAVAILABLE"))

    def test_components_and_receipt(self) -> None:
        c = self.out["components"]
        self.assertEqual({k: v["status"] for k, v in c.items()}, {
            "extract_capital_structure": "OK", "extract_guidance": "LIMITED", "extract_dilution_bridge": "FAILED",
            "list_sec_material_filings": "FAILED", "get_quote": "FAILED"})
        receipt = self.out["receipt"]
        self.assertEqual(receipt["coverage"]["state"], "PARTIAL")
        cap = next(r for r in receipt["components"] if r["name"] == "extract_capital_structure")
        self.assertEqual(cap["providerTimestamps"], {"source.filingDate": "2026-08-10", "source.periodEnd": "2026-06-30", "source.form": "10-Q"})
        self.assertEqual(len(cap["sha256"]), 64)

    def test_cut_ids(self) -> None:
        self.assertEqual(self.out["cutId"], "ec1_ASTS_20260926T080000Z_" + "a" * 64)
        self.assertEqual(self.out["parsed"]["key"], "evidence-cuts/BRK.B/20260926T080000Z/" + "b" * 64 + ".json")
        self.assertIsNone(self.out["parsedBad"])
        self.assertEqual(self.out["observationKey"], "consensus-history/ASTS/2026-09-26.json")


if __name__ == "__main__":
    unittest.main()
