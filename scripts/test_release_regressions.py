#!/usr/bin/env python3
"""Regression locks for the PR232-235 fixes (2.4.2-2.4.5), in both runtimes.

The shared pure modules (capital structure, valuation, SEC facts, extraction
rules) are covered by their own parity tests. These cases lock the fixes that
live in the provider layer, with fetch stubbed so they run offline:

- a failed accession pin names the requested accession, skips the latest
  filing lookup, and carries no evidence row (2.4.5, 2.4.6);
- the envelope drops evidence rows with no populated field (2.4.6);
- quote/financial currency mismatch withholds value-over-results multiples
  and keeps P/E (2.4.4, TSM);
- Yahoo's surprisePercent is a decimal ratio at every size (2.4.4, ASTS);
- exhibits resolve through the issuer's CIK, not the filer agent's accession
  prefix (2.4.4);
- event verification matches every collected item before the display cap,
  and the options flow window names its fields for what they measure
  (2.4.4, 2.4.5).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

REQUESTED = "0001193125-26-999999"

_ENTRY = """
export { getFilingData, getFinancialRatios, getEarningsMomentum, listSecFilingExhibits } from "./src/yahoo-finance.ts";
export { mcpSuccess, setWorkerEnv } from "./src/response.ts";
"""

_HARNESS = r"""
const [bundleUrl] = process.argv.slice(-1);
const m = await import(bundleUrl);
m.setWorkerEnv({ MCP_ENVELOPE_V2: "true" });
const out = {};
const fetched = [];
const yahoo = {};
globalThis.fetch = async (req) => {
  const u = new URL(typeof req === "string" ? req : req.url);
  fetched.push(u.toString());
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  if (u.pathname.endsWith("company_tickers.json")) {
    return Response.json({ "0": { cik_str: 1780312, ticker: "ASTS", title: "AST SpaceMobile, Inc." } });
  }
  if (u.pathname.includes("/companyconcept/")) {
    return Response.json({ units: { USD: [
      { start: "2026-04-01", end: "2026-06-30", val: 31520000, accn: "0001193125-26-342550", fy: 2026, fp: "Q2", form: "10-Q", filed: "2026-08-10" },
      { start: "2025-01-01", end: "2025-12-31", val: 70000000, accn: "0001780312-26-000011", fy: 2025, fp: "FY", form: "10-K", filed: "2026-03-01" },
    ] } });
  }
  if (u.pathname.includes("/submissions/")) return Response.json({ cik: "1780312", filings: { recent: {} } });
  if (u.pathname.includes("/Archives/edgar/data/")) return new Response("<table></table>", { headers: { "content-type": "text/html" } });
  if (u.pathname.includes("/quoteSummary/")) return Response.json({ quoteSummary: { result: [yahoo.summary], error: null } });
  return new Response("not found", { status: 404 });
};

// Failed accession pin (2.4.5, 2.4.6).
const pin = JSON.parse(await m.getFilingData("ASTS", "total_revenue", null, "10-K", "latest", "auto", "0001193125-26-999999"));
out.pin = pin;
out.pinFetchedSubmissions = fetched.some((f) => f.includes("/submissions/"));
out.pinEnvelope = JSON.parse(m.mcpSuccess("extract_sec_filing_fact", JSON.stringify(pin)));
out.emptyRowEnvelope = JSON.parse(m.mcpSuccess("extract_sec_filing_fact", JSON.stringify({ value: null, evidence: [{}, { url: null }] })));

// Currency-mixed multiples (2.4.4, TSM).
const r = (v) => ({ raw: v, fmt: String(v) });
yahoo.summary = {
  summaryDetail: { currency: "USD", trailingPE: r(24), forwardPE: r(20), priceToSalesTrailing12Months: r(0.3), marketCap: r(1e12) },
  financialData: { financialCurrency: "TWD", freeCashflow: r(9e11) },
  defaultKeyStatistics: { priceToBook: r(1.1), enterpriseToEbitda: r(0.5), enterpriseToRevenue: r(0.3) },
};
out.ratios = JSON.parse(await m.getFinancialRatios("TSM"));

// Surprise decimal ratio at every size (2.4.4, ASTS).
yahoo.summary = {
  earningsTrend: { trend: [] },
  earningsHistory: { history: [
    { quarter: r(1), epsActual: r(-0.61), epsEstimate: r(-0.18), surprisePercent: r(-2.337) },
    { quarter: r(2), epsActual: r(-0.2), epsEstimate: r(-0.2), surprisePercent: r(-0.013) },
  ] },
};
out.momentum = JSON.parse(await m.getEarningsMomentum("ASTS"));

// Issuer CIK before accession prefix (2.4.4).
out.exhibits = JSON.parse(await m.listSecFilingExhibits("ASTS", "0001193125-26-342550"));
console.log(JSON.stringify(out));
"""


def _run_worker() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry = WORKER / ".release-regressions-entry.ts"
        bundle = Path(tmp) / "bundle.mjs"
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
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


class TestWorkerRegressions(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _run_worker()

    def test_failed_pin_names_the_requested_accession(self) -> None:
        pin = self.out["pin"]
        self.assertEqual(pin["code"], "NO_FACT_FOR_ACCESSION")
        self.assertEqual((pin["accessionNumber"], pin["requestedAccession"]), (REQUESTED, REQUESTED))
        self.assertIsNone(pin["value"])
        self.assertFalse(self.out["pinFetchedSubmissions"], "a failed pin must not look up the latest filing")

    def test_failed_pin_carries_no_evidence_row(self) -> None:
        self.assertIsNone(self.out["pinEnvelope"]["data"]["evidence"])
        self.assertIsNone(self.out["emptyRowEnvelope"]["data"]["evidence"])

    def test_currency_mismatch_withholds_multiples(self) -> None:
        ratios = self.out["ratios"]
        for key in ("priceToSales", "priceToBook", "enterpriseToEbitda", "enterpriseToRevenue", "freeCashflowYield"):
            self.assertIsNone(ratios[key], key)
        self.assertEqual(ratios["withheldMultiples"]["priceToSales"], 0.3)
        self.assertEqual(ratios["trailingPE"], 24)
        self.assertEqual(ratios["quoteCurrency"], "USD")
        self.assertIn("CURRENCY_MISMATCH_MULTIPLES_WITHHELD", [w["code"] for w in ratios["warnings"]])

    def test_surprise_is_scaled_at_every_size(self) -> None:
        self.assertAlmostEqual(self.out["momentum"]["avgSurprisePct"], (-233.7 - 1.3) / 2, places=1)

    def test_exhibits_use_the_issuer_cik(self) -> None:
        index_url = self.out["exhibits"]["indexUrl"]
        self.assertIn("/data/1780312/", index_url)
        self.assertNotIn("/data/1193125/", index_url)


class TestSourceContracts(unittest.TestCase):
    """Wiring that needs every news provider to exercise end to end."""

    def test_event_verification_matches_before_the_display_cap(self) -> None:
        ts = (WORKER / "src" / "yahoo-finance.ts").read_text(encoding="utf-8")
        self.assertRegex(ts, r"collectCompanyEvents\(ticker, \{[^}]*keepAll: true")
        self.assertIn("keepAll ? allDeduped : allDeduped.slice(0, safeMax)", ts)
        py = (ROOT / "server.py").read_text(encoding="utf-8")
        self.assertRegex(py, r"keep_all=True")
        self.assertRegex(py, r"if not keep_all:\s*\n\s*deduped = deduped\[:max_cap\]")

    def test_options_flow_fields_name_what_they_measure(self) -> None:
        for path in (WORKER / "src" / "yahoo-finance.ts", ROOT / "server.py"):
            text = path.read_text(encoding="utf-8")
            for field in ("ivVsRealizedRangePct", "putVolPer1PctStockAdv", "fieldNotes"):
                self.assertIn(field, text, f"{field} missing from {path.name}")


# ── Python runtime ───────────────────────────────────────────────────────────

def _ensure_mcp_available() -> None:
    try:
        import mcp.server  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _FastMCPStub:
        def __init__(self, *a: object, **kw: object) -> None:
            pass

        def tool(self, *a: object, **kw: object):  # type: ignore[return]
            if a and callable(a[0]):
                return a[0]
            return lambda fn: fn

    mcp_mod = types.ModuleType("mcp")
    server_mod = types.ModuleType("mcp.server")
    fastmcp_mod = types.ModuleType("mcp.server.fastmcp")
    fastmcp_mod.FastMCP = _FastMCPStub  # type: ignore[attr-defined]
    mcp_mod.server = server_mod  # type: ignore[attr-defined]
    server_mod.fastmcp = fastmcp_mod  # type: ignore[attr-defined]
    sys.modules.setdefault("mcp", mcp_mod)
    sys.modules.setdefault("mcp.server", server_mod)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_mod)


_ensure_mcp_available()
import server as srv  # noqa: E402
from yfmcp import envelope as _envelope  # noqa: E402

_CONCEPT = {"units": {"USD": [
    {"start": "2026-04-01", "end": "2026-06-30", "val": 31520000, "accn": "0001193125-26-342550", "fy": 2026, "fp": "Q2", "form": "10-Q", "filed": "2026-08-10"},
]}}


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


class TestPythonRegressions(unittest.TestCase):
    def test_failed_pin_names_the_requested_accession(self) -> None:
        with patch("server._resolve_cik_for_ticker", new=AsyncMock(return_value="0001780312")), \
                patch("server._edgar_get", new=AsyncMock(return_value=_CONCEPT)):
            data = json.loads(_run(srv.extract_sec_filing_fact("ASTS", fact="total_revenue", accession_number=REQUESTED)))
        if "data" in data and "ok" in data:
            data = data["data"]
        self.assertEqual(data["code"], "NO_FACT_FOR_ACCESSION")
        self.assertEqual((data["accessionNumber"], data["requestedAccession"]), (REQUESTED, REQUESTED))
        self.assertFalse(data["evidence"])

    def test_envelope_drops_empty_evidence_rows(self) -> None:
        enriched = _envelope._enrich_facts({"value": None, "evidence": [{}, {"url": None}]})
        self.assertIsNone(enriched["evidence"])

    def test_currency_mismatch_withholds_multiples(self) -> None:
        class _Ticker:
            info = {"currency": "USD", "financialCurrency": "TWD", "trailingPE": 24, "priceToSalesTrailing12Months": 0.3,
                    "priceToBook": 1.1, "enterpriseToEbitda": 0.5, "enterpriseToRevenue": 0.3, "marketCap": 1e12, "freeCashflow": 9e11}

        with patch("server.yf.Ticker", return_value=_Ticker()):
            ratios = json.loads(_run(srv.get_financial_ratios("TSMREG")))
        for key in ("priceToSales", "priceToBook", "enterpriseToEbitda", "enterpriseToRevenue", "freeCashflowYield"):
            self.assertIsNone(ratios[key], key)
        self.assertEqual(ratios["withheldMultiples"]["priceToSales"], 0.3)
        self.assertEqual(ratios["trailingPE"], 24)

    def test_surprise_is_scaled_at_every_size(self) -> None:
        class _FastInfo:
            currency = "USD"

        class _Ticker:
            fast_info = _FastInfo()
            eps_trend = srv.pd.DataFrame()
            earnings_history = srv.pd.DataFrame([
                {"epsActual": -0.61, "epsEstimate": -0.18, "surprisePercent": -2.337},
                {"epsActual": -0.2, "epsEstimate": -0.2, "surprisePercent": -0.013},
            ])

        with patch("server.yf.Ticker", return_value=_Ticker()):
            data = json.loads(_run(srv.get_earnings_momentum("ASTS")))
        self.assertAlmostEqual(data["avgSurprisePct"], (-233.7 - 1.3) / 2, places=1)


if __name__ == "__main__":
    unittest.main()
