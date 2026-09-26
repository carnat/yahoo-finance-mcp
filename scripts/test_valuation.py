#!/usr/bin/env python3
"""Valuation snapshot and peer multiples, the same in both runtimes (2.4.0).

Part one runs worker/src/valuation.ts and yfmcp/valuation.py on the same
Yahoo quoteSummary fixtures and requires identical output. Part two drives
get_valuation_snapshot and compare_peer_valuations end to end: the Worker in
Miniflare against mocked Yahoo and SEC, the local server with its Yahoo and
EDGAR helpers patched, reusing the filings of test_capital_structure.
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
sys.path.insert(0, str(ROOT / "scripts"))

import test_capital_structure as csfx  # noqa: E402
from yfmcp import valuation as vl  # noqa: E402


def _r(value):
    return {"raw": value, "fmt": str(value)}


def _qs(*, name, currency, price, fin_currency, shares, cash, debt, revenue, ebitda, gross, eps, fy0, fy1, market_cap=None, implied=None):
    return {
        "price": {"longName": name, "currency": currency, "regularMarketPrice": _r(price), "regularMarketTime": _r(1790000000),
                  **({"marketCap": _r(market_cap)} if market_cap is not None else {})},
        "financialData": {"financialCurrency": fin_currency, "totalCash": _r(cash), "totalDebt": _r(debt), "totalRevenue": _r(revenue),
                          "ebitda": _r(ebitda), "grossMargins": _r(gross)},
        "defaultKeyStatistics": {"sharesOutstanding": _r(shares), "trailingEps": _r(eps), **({"impliedSharesOutstanding": _r(implied)} if implied is not None else {})},
        "earningsTrend": {"trend": [
            {"period": "0q", "revenueEstimate": {"avg": _r(fy0[0] / 4)}, "earningsEstimate": {"avg": _r(0.01)}},
            {"period": "0y", "revenueEstimate": {"avg": _r(fy0[0]), "numberOfAnalysts": _r(fy0[1])}, "earningsEstimate": {"avg": _r(fy0[2]), "numberOfAnalysts": _r(fy0[3])}},
            {"period": "+1y", "revenueEstimate": {"avg": _r(fy1[0]), "numberOfAnalysts": _r(fy1[1])}, "earningsEstimate": {"avg": _r(fy1[2]), "numberOfAnalysts": _r(fy1[3])}},
        ]},
    }


QUOTE_SUMMARIES = {
    # The capital-structure fixture company, loss-making on EBITDA and trailing EPS.
    "CSTC": _qs(name="CS Test Co", currency="USD", price=25.0, fin_currency="USD", shares=101_000_000, cash=190_000_000, debt=400_000_000,
                revenue=500_000_000, ebitda=-20_000_000, gross=0.4123, eps=-0.5, fy0=(600_000_000, 8, 0.25, 7), fy1=(800_000_000, 6, 1.10, 5)),
    "PEER1": _qs(name="Peer One", currency="USD", price=50.0, fin_currency="USD", shares=20_000_000, cash=100_000_000, debt=50_000_000,
                 revenue=400_000_000, ebitda=80_000_000, gross=0.5, eps=1.5, fy0=(450_000_000, 10, 2.0, 10), fy1=(540_000_000, 9, 2.5, 9)),
    # A pence listing with sterling financials.
    "PEER2.L": _qs(name="Peer Two plc", currency="GBp", price=20.5, fin_currency="GBP", shares=970_000_000, cash=30_000_000, debt=60_000_000,
                   revenue=110_000_000, ebitda=5_000_000, gross=0.2, eps=-0.01, fy0=(120_000_000, 4, 0.001, 3), fy1=(140_000_000, 4, 0.01, 3)),
    # Up-C (ASTS-like): Yahoo lists the Class A count; the implied count includes exchangeable classes (2.4.1).
    "UPC": _qs(name="Up-C Co", currency="USD", price=60.0, fin_currency="USD", shares=300_000_000, implied=390_000_000, cash=4_000_000_000, debt=3_000_000_000,
               revenue=100_000_000, ebitda=-400_000_000, gross=0.4, eps=-2.0, fy0=(170_000_000, 12, -2.3, 8), fy1=(650_000_000, 11, -1.1, 9)),
    # ADR-style mismatch: USD quote, TWD financials.
    "PEER3": _qs(name="Peer Three", currency="USD", price=10.0, fin_currency="TWD", shares=50_000_000, cash=900_000_000, debt=100_000_000,
                 revenue=3_000_000_000, ebitda=300_000_000, gross=0.3, eps=10.0, fy0=(3_300_000_000, 2, 11.0, 2), fy1=(3_600_000_000, 2, 12.0, 2)),
}

# A bridge and capital structure as the SEC tools return them (the parts the snapshot reads).
BRIDGE = {
    "basis": "MECHANICAL_COMPANY_DISCLOSED", "status": "PARTIAL",
    "basicShares": {"shares": 101_000_000},
    "bridge": {"dilutedSharesAtPrice": 123_399_996, "dilutionPctAtPrice": 22.18},
    "components": [{"component": "convertible_debt", "instruments": [
        {"principal": 300_000_000, "incrementalShares": 15_000_000},
        {"principal": 200_000_000, "incrementalShares": 0},
    ]}],
    "atmProgram": {"remainingCapacityUsd": 140_000_000, "programSizeUsd": None, "potentialShares": 5_600_000},
}
CAPITAL = {"basis": "COMPANY_DISCLOSED", "periodEnd": "2025-03-31", "balances": {"cashAndEquivalents": 140_000_000, "shortTermInvestments": None, "totalDebt": 380_000_000}}
# A filing that tags no borrowings (AEHR, 2.4.2): debt is zero, not Yahoo's lease-inclusive total.
# Filing cash equals Yahoo's total cash, yet the filing adds investments on top (ASTS-like).
CAPITAL_CASH_MISMATCH = {"basis": "COMPANY_DISCLOSED", "periodEnd": "2025-03-31", "balances": {"cashAndEquivalents": 190_000_000, "shortTermInvestments": 150_000_000, "totalDebt": 380_000_000}}
CAPITAL_DEBT_FREE = {"basis": "COMPANY_DISCLOSED", "periodEnd": "2025-03-31", "balances": {"cashAndEquivalents": 140_000_000, "shortTermInvestments": None, "totalDebt": 0}}

_WORKER_PURE = r"""
import fs from "node:fs";
const [bundleUrl, dataPath] = process.argv.slice(-2);
const m = await import(bundleUrl);
const data = JSON.parse(fs.readFileSync(dataPath, "utf8"));
const markets = Object.fromEntries(Object.entries(data.qs).map(([t, r]) => [t, m.marketInputsFromQuoteSummary(t, r)]));
const out = { markets };
out.snapshot = m.valuationSnapshot({ ticker: "CSTC", market: markets.CSTC, suppliedPrice: null, bridge: data.bridge, capital: data.capital, secWarnings: [] });
out.snapshotCashMismatch = m.valuationSnapshot({ ticker: "CSTC", market: markets.CSTC, suppliedPrice: null, bridge: null, capital: data.capitalCashMismatch, secWarnings: [] });
out.snapshotDebtFree = m.valuationSnapshot({ ticker: "CSTC", market: markets.CSTC, suppliedPrice: null, bridge: null, capital: data.capitalDebtFree, secWarnings: [] });
out.snapshotYahoo = m.valuationSnapshot({ ticker: "CSTC", market: markets.CSTC, suppliedPrice: 30, bridge: null, capital: null, secWarnings: [] });
out.upcRow = m.peerRow(markets.UPC);
out.snapshotPence = m.valuationSnapshot({ ticker: "PEER2.L", market: markets["PEER2.L"], suppliedPrice: null, bridge: null, capital: null, secWarnings: [] });
out.peers = m.peerValuations("CSTC", [markets.CSTC, markets.PEER1, markets["PEER2.L"], markets.PEER3], [{ ticker: "PEER4", message: "No Yahoo quote summary for PEER4" }]);
console.log(JSON.stringify(out));
"""


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
            [str(ESBUILD), str(WORKER / "src" / "valuation.ts"), "--bundle", "--format=esm", "--platform=neutral", f"--outfile={bundle}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
        (tmp_path / "data.json").write_text(json.dumps({"qs": QUOTE_SUMMARIES, "bridge": BRIDGE, "capital": CAPITAL, "capitalDebtFree": CAPITAL_DEBT_FREE, "capitalCashMismatch": CAPITAL_CASH_MISMATCH}), encoding="utf-8")
        (tmp_path / "harness.mjs").write_text(_WORKER_PURE, encoding="utf-8")
        result = subprocess.run([node, str(tmp_path / "harness.mjs"), bundle.as_uri(), str(tmp_path / "data.json")],
                                check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


def _python_pure() -> dict:
    markets = {t: vl.market_inputs_from_quote_summary(t, r) for t, r in QUOTE_SUMMARIES.items()}
    return {
        "markets": markets,
        "snapshot": vl.valuation_snapshot("CSTC", markets["CSTC"], None, copy.deepcopy(BRIDGE), copy.deepcopy(CAPITAL), []),
        "snapshotCashMismatch": vl.valuation_snapshot("CSTC", markets["CSTC"], None, None, copy.deepcopy(CAPITAL_CASH_MISMATCH), []),
        "snapshotDebtFree": vl.valuation_snapshot("CSTC", markets["CSTC"], None, None, copy.deepcopy(CAPITAL_DEBT_FREE), []),
        "snapshotYahoo": vl.valuation_snapshot("CSTC", markets["CSTC"], 30, None, None, []),
        "snapshotPence": vl.valuation_snapshot("PEER2.L", markets["PEER2.L"], None, None, None, []),
        "upcRow": vl.peer_row(markets["UPC"]),
        "peers": vl.peer_valuations("CSTC", [markets["CSTC"], markets["PEER1"], markets["PEER2.L"], markets["PEER3"]],
                                    [{"ticker": "PEER4", "message": "No Yahoo quote summary for PEER4"}]),
    }


def _multiple(snapshot: dict, metric: str, basis: str):
    return next(r for r in snapshot["multiples"] if r["metric"] == metric and r["basis"] == basis)


class TestValuationParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.worker = _worker_pure()
        cls.local = _python_pure()

    def test_runtimes_agree(self) -> None:
        for key in ("markets", "snapshot", "snapshotCashMismatch", "snapshotDebtFree", "snapshotYahoo", "snapshotPence", "upcRow", "peers"):
            self.assertEqual(self.worker[key], self.local[key], key)

    def test_yfinance_adapter_matches_quote_summary_parsing(self) -> None:
        qs = QUOTE_SUMMARIES["PEER1"]
        info = {
            "longName": "Peer One", "currency": "USD", "financialCurrency": "USD", "currentPrice": 50.0, "regularMarketTime": 1790000000,
            "sharesOutstanding": 20_000_000, "totalCash": 100_000_000, "totalDebt": 50_000_000, "totalRevenue": 400_000_000,
            "ebitda": 80_000_000, "grossMargins": 0.5, "trailingEps": 1.5, "marketCap": None,
        }
        revenue = [{"period": "0q", "avg": 112_500_000.0, "numberOfAnalysts": float("nan")}, {"period": "0y", "avg": 450_000_000, "numberOfAnalysts": 10}, {"period": "+1y", "avg": 540_000_000, "numberOfAnalysts": 9}]
        earnings = [{"period": "0q", "avg": 0.01, "numberOfAnalysts": float("nan")}, {"period": "0y", "avg": 2.0, "numberOfAnalysts": 10}, {"period": "+1y", "avg": 2.5, "numberOfAnalysts": 9}]
        self.assertEqual(vl.market_inputs_from_yfinance("PEER1", info, revenue, earnings), vl.market_inputs_from_quote_summary("PEER1", qs))


class TestValuationValues(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_pure()

    def test_snapshot_uses_the_bridge_and_period_end_balances(self) -> None:
        s = self.out["snapshot"]
        self.assertEqual((s["status"], s["decisionUse"]), ("COMPUTED", "CONTEXT_ONLY_NOT_A_PRICE_TARGET"))
        self.assertEqual((s["shares"]["diluted"], s["shares"]["basis"], s["shares"]["secVsYahooSharesDiffPct"]), (123_399_996, "sec_cover_page", 0))
        # The in-the-money $300M convertible is counted as shares, so it leaves the debt.
        self.assertEqual(s["balances"]["convertibleDebtCountedAsShares"], 300_000_000)
        self.assertEqual(s["equityValue"], 3_084_999_900)
        self.assertEqual(s["enterpriseValue"], 3_084_999_900 + 380_000_000 - 300_000_000 - 140_000_000)
        self.assertEqual(_multiple(s, "EV/Revenue", "trailing_12_months")["multiple"], 6.05)
        self.assertEqual(_multiple(s, "EV/Revenue", "current_fiscal_year")["multiple"], 5.04)
        self.assertEqual((_multiple(s, "EV/Revenue", "next_fiscal_year")["multiple"], _multiple(s, "EV/Revenue", "next_fiscal_year")["analysts"]), (3.78, 6))
        self.assertIsNone(_multiple(s, "EV/EBITDA", "trailing_12_months")["multiple"])
        self.assertIn("negative", _multiple(s, "EV/EBITDA", "trailing_12_months")["note"])
        self.assertEqual(_multiple(s, "P/E", "current_fiscal_year")["multiple"], 100)
        self.assertEqual(_multiple(s, "P/E", "next_fiscal_year")["multiple"], 22.73)
        self.assertEqual(s["revenueGrowth"], {"currentFiscalYearVsTtmPct": 20, "nextVsCurrentFiscalYearPct": 33.33})
        self.assertEqual(s["atmCapacity"]["potentialSharesAtPrice"], 5_600_000)
        self.assertEqual(s["peerComparableBasis"]["enterpriseValue"], 25 * 101_000_000 + 400_000_000 - 190_000_000)

    def test_filing_investments_above_yahoo_cash_are_flagged(self) -> None:
        s = self.out["snapshotCashMismatch"]
        w = next(w for w in s["warnings"] if w["code"] == "SEC_YAHOO_CASH_MISMATCH")
        self.assertEqual((w["severity"], w["secCash"], w["secShortTermInvestments"], w["yahooTotalCash"]), ("warning", 190_000_000, 150_000_000, 190_000_000))
        # Flagged, not overridden: the filing's balances still set enterprise value.
        self.assertEqual(s["enterpriseValue"], 25 * 101_000_000 + 380_000_000 - 190_000_000 - 150_000_000)
        self.assertNotIn("SEC_YAHOO_CASH_MISMATCH", [w["code"] for w in self.out["snapshot"]["warnings"]])

    def test_debt_free_filing_keeps_filing_balances(self) -> None:
        s = self.out["snapshotDebtFree"]
        self.assertEqual((s["balances"]["basis"], s["balances"]["totalDebt"], s["balances"]["cash"]), ("sec_filing_period_end", 0, 140_000_000))
        self.assertEqual(s["enterpriseValue"], 25 * 101_000_000 - 140_000_000)
        self.assertNotIn("YAHOO_BALANCES", [w["code"] for w in s["warnings"]])

    def test_snapshot_without_filings_uses_yahoo(self) -> None:
        s = self.out["snapshotYahoo"]
        self.assertEqual((s["price"]["source"], s["shares"]["basis"], s["balances"]["basis"]), ("caller_supplied", "yahoo_shares_outstanding", "yahoo_financial_data"))
        self.assertEqual(s["enterpriseValue"], 30 * 101_000_000 + 400_000_000 - 190_000_000)
        self.assertIn("YAHOO_BALANCES", [w["code"] for w in s["warnings"]])

    def test_pence_listing_is_valued_in_pounds(self) -> None:
        s = self.out["snapshotPence"]
        self.assertEqual((s["price"]["majorUnitAmount"], s["price"]["majorCurrency"]), (0.205, "GBP"))
        self.assertEqual(s["enterpriseValue"], round(0.205 * 970_000_000 + 60_000_000 - 30_000_000))
        self.assertEqual(_multiple(s, "EV/Revenue", "trailing_12_months")["multiple"], 2.08)

    def test_implied_all_class_shares_replace_the_listed_class(self) -> None:
        row = self.out["upcRow"]
        self.assertEqual((row["sharesUsed"], row["shareBasis"]), (390_000_000, "yahoo_implied_shares_outstanding"))
        self.assertEqual(row["enterpriseValue"], 60 * 390_000_000 + 3_000_000_000 - 4_000_000_000)
        rows = {r["ticker"]: r for r in self.out["peers"]["rows"]}
        self.assertEqual(rows["PEER1"]["shareBasis"], "yahoo_shares_outstanding")

    def test_peer_medians_exclude_the_subject(self) -> None:
        p = self.out["peers"]
        rows = {r["ticker"]: r for r in p["rows"]}
        self.assertEqual((rows["PEER1"]["enterpriseValue"], rows["PEER1"]["evToRevenueTtm"], rows["PEER1"]["evToEbitdaTtm"]), (950_000_000, 2.38, 11.88))
        # TWD financials against a USD quote: no multiples rather than wrong ones.
        self.assertEqual((rows["PEER3"]["comparableCurrency"], rows["PEER3"]["evToRevenueTtm"]), (False, None))
        summary = p["peerSummary"]["evToRevenueTtm"]
        self.assertEqual((summary["count"], summary["median"]), (2, round((2.38 + 2.08) / 2, 2)))
        versus = p["subjectVsPeerMedian"]["evToRevenueTtm"]
        self.assertEqual(versus["subject"], rows["CSTC"]["evToRevenueTtm"])
        self.assertEqual(versus["premiumPct"], round((rows["CSTC"]["evToRevenueTtm"] / 2.23 - 1) * 100, 2))
        self.assertIsNone(p["subjectVsPeerMedian"]["grossMarginPct"]["premiumPct"])
        self.assertEqual(p["errors"], [{"ticker": "PEER4", "message": "No Yahoo quote summary for PEER4"}])


# ── End to end ──────────────────────────────────────────────────────────────

CALLS = {
    "snapshot": ("stock_fundamentals", "get_valuation_snapshot", {"ticker": "CSTC"}),
    "snapshot_at_40": ("stock_fundamentals", "get_valuation_snapshot", {"ticker": "CSTC", "price": 40}),
    "snapshot_yahoo": ("stock_fundamentals", "get_valuation_snapshot", {"ticker": "CSTC", "include_sec_filings": False}),
    "snapshot_pence": ("stock_fundamentals", "get_valuation_snapshot", {"ticker": "PEER2.L"}),
    "peers": ("stock_fundamentals", "compare_peer_valuations", {"tickers": ["PEER1", "PEER2.L", "PEER4"], "subject": "CSTC"}),
}

_WORKER_E2E = r"""
const [miniflareUrl, scriptPath, dataPath] = process.argv.slice(-3);
const { Miniflare, convertV4MiniflareOptions } = await import(miniflareUrl);
const fs = await import("node:fs");
const data = JSON.parse(fs.readFileSync(dataPath, "utf8"));
async function outbound(req) {
  const u = new URL(req.url);
  const url = `${u.origin}${u.pathname}`;
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/; Domain=.yahoo.com" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  if (u.pathname.startsWith("/v10/finance/quoteSummary/")) {
    const symbol = decodeURIComponent(u.pathname.split("/").pop());
    const result = data.qs[symbol];
    return result ? Response.json({ quoteSummary: { result: [result], error: null } }) : Response.json({ quoteSummary: { result: [], error: null } });
  }
  if (u.hostname === "www.sec.gov" && u.pathname === "/files/company_tickers.json") {
    return Response.json({ "0": { cik_str: data.cik, ticker: "CSTC", title: "CS Test Co" } });
  }
  if (u.hostname === "data.sec.gov" && u.pathname === `/submissions/CIK${String(data.cik).padStart(10, "0")}.json`) return Response.json(data.submissions);
  if (data.documents[url] !== undefined) return new Response(data.documents[url], { headers: { "content-type": "text/html" } });
  return new Response("Not Found", { status: 404 });
}
const mf = new Miniflare(convertV4MiniflareOptions({ workers: [{
  name: "w", modules: true, scriptPath, compatibilityDate: "2026-09-21", compatibilityFlags: ["nodejs_als"],
  bindings: { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true" }, outboundService: outbound,
}] }));
const worker = await mf.getWorker("w");
const out = {};
let id = 0;
for (const [name, [group, action, params]] of Object.entries(data.calls)) {
  const res = await worker.fetch("https://x/mcp", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: ++id, method: "tools/call", params: { name: group, arguments: { action, params } } }),
  });
  const envelope = (await res.json()).result.structuredContent;
  out[name] = { data: envelope.data, error: envelope.error };
}
await mf.dispose();
console.log(JSON.stringify(out));
"""


@functools.cache
def _worker_e2e() -> dict:
    node = _node()
    if not MINIFLARE.exists():
        raise unittest.SkipTest("worker/node_modules (npm ci) is required")
    with tempfile.TemporaryDirectory(dir=WORKER, prefix=".valuation-test-") as tmp:
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
            "qs": QUOTE_SUMMARIES, "cik": csfx.CIK, "submissions": csfx.SUBMISSIONS, "documents": csfx.DOCUMENTS, "calls": CALLS,
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

    async def fake_html(url: str, max_bytes: int = 5_000_000) -> str | None:
        return csfx.DOCUMENTS.get(url)

    async def fake_market(ticker: str) -> dict:
        result = QUOTE_SUMMARIES.get(ticker.upper())
        if result is None:
            raise ValueError(f"No Yahoo quote summary for {ticker.upper()}")
        return vl.market_inputs_from_quote_summary(ticker, copy.deepcopy(result))

    tools = {"get_valuation_snapshot": srv.get_valuation_snapshot, "compare_peer_valuations": srv.compare_peer_valuations}
    out = {}
    with patch.object(srv, "_get_submissions_for_ticker", AsyncMock(return_value=(f"{csfx.CIK:010d}", copy.deepcopy(csfx.SUBMISSIONS)))), \
            patch.object(srv, "_edgar_get_html", fake_html), \
            patch.object(srv, "_valuation_market_inputs", fake_market):
        for name, (_, action, params) in CALLS.items():
            raw = json.loads(asyncio.run(tools[action](**params)))
            out[name] = {"data": raw["data"] if isinstance(raw, dict) and "ok" in raw else raw}
    return out


class TestValuationEndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.worker = _worker_e2e()
        cls.local = _python_e2e()

    def data(self, name: str) -> dict:
        result = self.worker[name]
        self.assertIsNone(result["error"], result["error"])
        return result["data"]

    def test_runtimes_agree(self) -> None:
        for name in CALLS:
            self.assertEqual(self.data(name), self.local[name]["data"], name)

    def test_snapshot_reads_the_filings(self) -> None:
        s = self.data("snapshot")
        self.assertEqual((s["shares"]["diluted"], s["shares"]["bridgeStatus"], s["balances"]["basis"], s["balances"]["periodEnd"]),
                         (123_399_996, "PARTIAL", "sec_filing_period_end", "2025-03-31"))
        self.assertEqual(s["enterpriseValue"], 25 * 123_399_996 + 380_000_000 - 300_000_000 - 140_000_000)
        self.assertEqual(s["warnings"], [])

    def test_supplied_price_reruns_the_bridge(self) -> None:
        s = self.data("snapshot_at_40")
        # At $40 the $40-strike 2031 notes are in the money too: both principals leave the debt, capped at total debt.
        self.assertEqual((s["price"]["source"], s["balances"]["convertibleDebtCountedAsShares"]), ("caller_supplied", 380_000_000))
        self.assertGreater(s["shares"]["diluted"], 123_399_996)

    def test_fallbacks(self) -> None:
        self.assertEqual(self.data("snapshot_yahoo")["balances"]["basis"], "yahoo_financial_data")
        pence = self.data("snapshot_pence")
        self.assertEqual([w["code"] for w in pence["warnings"]], ["NON_USD_LISTING", "YAHOO_BALANCES"])
        peers = self.data("peers")
        self.assertEqual([r["ticker"] for r in peers["rows"]], ["CSTC", "PEER1", "PEER2.L"])
        self.assertEqual([e["ticker"] for e in peers["errors"]], ["PEER4"])


if __name__ == "__main__":
    unittest.main()
