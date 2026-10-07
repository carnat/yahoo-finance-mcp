#!/usr/bin/env python3
"""Latest annual report missing from SEC companyfacts (2.5.30, F-015): Worker vs local parity and semantics.

- worker/src/companyfacts-coverage.ts and yfmcp/companyfacts_coverage.py give the same answers on shared cases:
  which annual report is the latest as of a date, whether companyfacts holds any us-gaap or ifrs-full fact from
  it, and the LATEST_ANNUAL_NOT_IN_COMPANYFACTS warning.
- reconcile_metric_sources carries the warning in both runtimes, also on PERIOD_NOT_FOUND (TSM FY2025), with
  the providers mocked.
"""

from __future__ import annotations

import asyncio
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
sys.path.insert(0, str(ROOT))

from yfmcp import companyfacts_coverage as cfc  # noqa: E402

CIK = "0000000001"
A24 = "0001628280-25-000024"
A25 = "0001628280-26-025362"


def _row(start, end, val, accn, form, filed, fy=2024):
    return {**({"start": start} if start else {}), "end": end, "val": val, "accn": accn, "fy": fy, "fp": "FY", "form": form, "filed": filed}


# TSM-like: the FY2025 20-F contributes one dei fact and no ifrs-full fact; the newest IFRS annual period is FY2024.
TSM_FACTS = {"facts": {
    "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [_row(None, "2026-03-31", 25932000000, A25, "20-F", "2026-04-16", 2025)]}}},
    "srt": {"Something": {"units": {"pure": [_row(None, "2025-12-31", 1, A25, "20-F", "2026-04-16", 2025)]}}},
    "ifrs-full": {"Revenue": {"units": {"TWD": [
        _row("2023-01-01", "2023-12-31", 2161735841000, "0001628280-24-000023", "20-F", "2024-04-18", 2023),
        _row("2024-01-01", "2024-12-31", 2894307699000, A24.replace("-", ""), "20-F", "2025-04-17"),
    ]}}},
}}
COVERED_FACTS = json.loads(json.dumps(TSM_FACTS))
COVERED_FACTS["facts"]["ifrs-full"]["Revenue"]["units"]["TWD"].append(_row("2025-01-01", "2025-12-31", 3809054000000, A25, "20-F", "2026-04-16", 2025))
# An amendment's facts do not count for the original report; an amendment is never the latest annual report.
AMENDED_FACTS = json.loads(json.dumps(TSM_FACTS))
AMENDED_FACTS["facts"]["ifrs-full"]["Revenue"]["units"]["TWD"].append(_row("2025-01-01", "2025-12-31", 3809054000000, "0001628280-26-099999", "20-F/A", "2026-06-01", 2025))


def _submissions(*rows):
    """rows: (form, accession, filingDate, reportDate)."""
    return {"cik": "1", "filings": {"recent": {"form": [r[0] for r in rows], "accessionNumber": [r[1] for r in rows],
                                                 "filingDate": [r[2] for r in rows], "reportDate": [r[3] for r in rows],
                                                 "primaryDocument": ["d.htm" for _ in rows]}}}


SUBS = _submissions(("6-K", "0001628280-26-030000", "2026-07-16", ""), ("20-F/A", "0001628280-26-099999", "2026-06-01", "2025-12-31"),
                    ("20-F", A25, "2026-04-16", "2025-12-31"), ("20-F", A24, "2025-04-17", "2024-12-31"))

# [label, submissions, companyfacts, asOf]
CASES = [
    ["tsm-missing", SUBS, TSM_FACTS, "9999-12-31"],
    ["tsm-covered", SUBS, COVERED_FACTS, "9999-12-31"],
    ["tsm-amended-not-original", SUBS, AMENDED_FACTS, "9999-12-31"],
    ["before-fy2025-filed", SUBS, TSM_FACTS, "2026-04-15"],
    ["on-the-filing-day", SUBS, TSM_FACTS, "2026-04-16T09:00:00.000Z"],
    ["no-annual-report", _submissions(("10-Q", "0000000001-26-000002", "2026-05-01", "2026-03-31")), TSM_FACTS, "9999-12-31"],
    ["no-submissions", None, TSM_FACTS, "9999-12-31"],
    ["no-companyfacts", SUBS, None, "9999-12-31"],
    ["us-gaap-10k", _submissions(("10-K", "0000320193-25-000079", "2025-10-31", "2025-09-27")),
     {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [_row("2024-09-29", "2025-09-27", 416161000000, "0000320193-25-000079", "10-K", "2025-10-31", 2025)]}}}}}, "9999-12-31"],
]


def _python_outputs() -> dict:
    out = {}
    for label, subs, facts, as_of in CASES:
        filings = cfc.annual_filings_from_submissions(subs)
        latest = cfc.latest_annual_filing(filings, as_of)
        out[label] = {
            "filings": filings,
            "latest": latest,
            "inCompanyfacts": cfc.accession_in_companyfacts(facts, latest["accessionNumber"]) if latest else None,
            "warning": cfc.latest_annual_coverage_warning(filings, facts, as_of),
        }
    return out


_ENTRY = """
export * from "{SRC}/companyfacts-coverage.ts";
export { reconcileMetricSources } from "{SRC}/yahoo-finance.ts";
export { setWorkerEnv } from "{SRC}/response.ts";
"""

_HARNESS = r"""
const [bundleUrl, fixturesPath] = process.argv.slice(-2);
const m = await import(bundleUrl);
const { readFileSync } = await import("node:fs");
const f = JSON.parse(readFileSync(fixturesPath, "utf8"));
const out = {};
for (const [label, subs, facts, asOf] of f.cases) {
  const filings = m.annualFilingsFromSubmissions(subs);
  const latest = m.latestAnnualFiling(filings, asOf);
  out[label] = {
    filings,
    latest,
    inCompanyfacts: latest ? m.accessionInCompanyfacts(facts, latest.accessionNumber) : null,
    warning: m.latestAnnualCoverageWarning(filings, facts, asOf),
  };
}
if (f.reconcile) {
  m.setWorkerEnv({});
  const routes = f.reconcile.routes;
  globalThis.fetch = async (req) => {
    const u = new URL(typeof req === "string" ? req : req.url);
    if (u.pathname.endsWith("company_tickers.json")) return Response.json({ "0": { cik_str: 1, ticker: "XYZ", title: "XYZ Corp" } });
    const body = routes[u.toString()];
    return body === undefined ? new Response("not found", { status: 404 }) : Response.json(body);
  };
  out.reconcile = JSON.parse(await m.reconcileMetricSources("XYZ", "revenue", f.reconcile.period));
}
console.log(JSON.stringify(out));
"""

COMPANYFACTS_URL = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json"
SUBMISSIONS_URL = f"https://data.sec.gov/submissions/CIK{CIK}.json"


def _worker_outputs(reconcile: dict | None = None) -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry, bundle, harness, fx = (Path(tmp) / n for n in ("entry.ts", "bundle.mjs", "harness.mjs", "fixtures.json"))
        entry.write_text(_ENTRY.replace("{SRC}", (WORKER / "src").as_posix()), encoding="utf-8")
        harness.write_text(_HARNESS, encoding="utf-8")
        fx.write_text(json.dumps({"cases": CASES, "reconcile": reconcile}), encoding="utf-8")
        subprocess.run([str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=node", f"--outfile={bundle}", "--log-level=error"],
                       cwd=WORKER, check=True, capture_output=True, text=True, timeout=120)
        result = subprocess.run([node, str(harness), bundle.as_uri(), str(fx)], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


def _python_reconcile(facts: dict, subs: dict, period: str) -> dict:
    import server as srv

    with patch("server._get_submissions_for_ticker", new=AsyncMock(return_value=(CIK, subs))), \
            patch("server._edgar_get_company_facts", new=AsyncMock(return_value=facts)):
        return json.loads(asyncio.run(srv.reconcile_metric_sources("XYZ", "revenue", period)))


class TestCoverageParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.py = _python_outputs()
        cls.ts = _worker_outputs()

    def test_outputs_match(self) -> None:
        for label, *_ in CASES:
            self.assertEqual(self.py[label], self.ts[label], label)


class TestCoverageRule(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()

    def test_tsm_fy2025_is_missing(self) -> None:
        got = self.out["tsm-missing"]
        self.assertEqual([f["form"] for f in got["filings"]], ["20-F", "20-F"])
        self.assertEqual((got["latest"]["accessionNumber"], got["inCompanyfacts"]), (A25, False))
        w = got["warning"]
        self.assertEqual((w["code"], w["severity"], w["accessionNumber"], w["filed"], w["reportDate"], w["latestCompanyfactsAnnualPeriodEnd"]),
                         ("LATEST_ANNUAL_NOT_IN_COMPANYFACTS", "warning", A25, "2026-04-16", "2025-12-31", "2024-12-31"))
        self.assertEqual(w["message"], f"The latest annual report (20-F {A25}, filed 2026-04-16, period 2025-12-31) has no us-gaap or ifrs-full fact in SEC "
                         "companyfacts; the newest annual period companyfacts holds ends 2024-12-31. Figures here come from earlier reports, not from that filing.")

    def test_dei_and_srt_facts_do_not_count(self) -> None:
        # The FY2025 20-F's dei and srt facts are administrative: they are not the figures the paths read.
        self.assertIsNotNone(self.out["tsm-missing"]["warning"])

    def test_covered_reports_and_accession_formats(self) -> None:
        self.assertIsNone(self.out["tsm-covered"]["warning"])
        self.assertIsNone(self.out["us-gaap-10k"]["warning"])
        # The FY2024 accession is stored without dashes in the fixture; it still matches before the FY2025 filing.
        before = self.out["before-fy2025-filed"]
        self.assertEqual((before["latest"]["accessionNumber"], before["inCompanyfacts"], before["warning"]), (A24, True, None))

    def test_an_amendment_that_carries_the_year_covers_it(self) -> None:
        # The 20-F/A is never the latest annual report, and its accession is not the 20-F's; but it put FY2025
        # into companyfacts, so the year is there and no warning is raised.
        got = self.out["tsm-amended-not-original"]
        self.assertEqual((got["latest"]["accessionNumber"], got["inCompanyfacts"], got["warning"]), (A25, False, None))

    def test_a_report_counts_from_its_filing_day(self) -> None:
        got = self.out["on-the-filing-day"]
        self.assertEqual(got["latest"]["accessionNumber"], A25)
        self.assertTrue(got["warning"]["message"].startswith(f"The latest annual report filed by 2026-04-16 (20-F {A25}"))

    def test_nothing_to_compare_gives_no_warning(self) -> None:
        for label in ("no-annual-report", "no-submissions"):
            self.assertEqual((self.out[label]["latest"], self.out[label]["warning"]), (None, None), label)
        # No companyfacts at all: the latest report is not in it.
        self.assertEqual(self.out["no-companyfacts"]["warning"]["latestCompanyfactsAnnualPeriodEnd"], None)
        self.assertIn("companyfacts holds no annual period.", self.out["no-companyfacts"]["warning"]["message"])


class TestReconcileCarriesTheWarning(unittest.TestCase):
    """TSM FY2025 through reconcile_metric_sources: PERIOD_NOT_FOUND, now with the reason companyfacts lacks it."""

    def _both(self, facts: dict, period: str) -> dict:
        py = _python_reconcile(facts, SUBS, period)
        ts = _worker_outputs({"period": period, "routes": {COMPANYFACTS_URL: facts, SUBMISSIONS_URL: SUBS}})["reconcile"]
        self.assertEqual(py, ts)
        self.assertEqual(list(py), list(ts), "key order")
        return py

    def test_period_not_found_names_the_missing_report(self) -> None:
        got = self._both(TSM_FACTS, "FY2025")
        self.assertEqual((got["status"], got["fiscalYearsAvailable"]), ("PERIOD_NOT_FOUND", [2023, 2024]))
        self.assertEqual([w["code"] for w in got["warnings"]], ["LATEST_ANNUAL_NOT_IN_COMPANYFACTS"])
        self.assertEqual(got["warnings"][0]["accessionNumber"], A25)

    def test_no_warning_when_companyfacts_has_it(self) -> None:
        got = self._both(COVERED_FACTS, "FY2030")
        self.assertEqual(got["status"], "PERIOD_NOT_FOUND")
        self.assertNotIn("warnings", got)


if __name__ == "__main__":
    unittest.main()
