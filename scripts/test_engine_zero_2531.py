#!/usr/bin/env python3
"""Engine Zero findings fixed in 2.5.31, in both runtimes with the providers mocked.

- F-027: when Yahoo and SEC's ticker index are both unreadable, a ticker is resolved by EDGAR's ticker lookup
  only, never by its company-name search ("BE" matched "BE 2023 Irrevocable Trust", CIK 0001986183).
- F-028: list_sec_material_filings states its 20-filing cap, whether more filings match, and warns
  LIMIT_CAPPED when a larger limit is asked for.
- F-022: get_earnings_analysis carries epsBasis UNKNOWN / NOT_DISCLOSED_BY_PROVIDER.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

TRUST_CIK = "0001986183"
BLOOM_CIK = "0001664703"
# EDGAR's atom feeds: the ticker lookup for an unknown symbol names no company; the company-name search lists
# "BE 2023 Irrevocable Trust" first.
TICKER_FEED_EMPTY = "<feed><title>Company Information:</title></feed>"
TICKER_FEED_BLOOM = f'<feed><company-info><cik>{BLOOM_CIK}</cik></company-info><link href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK={BLOOM_CIK}"/></feed>'
NAME_FEED_TRUST = f'<feed><entry><link href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK={TRUST_CIK}"/></entry></feed>'

FORMS = ["8-K"] * 30 + ["4"] * 5 + ["10-Q"] * 3
SUBS = {"cik": "1664703", "filings": {"recent": {
    "form": FORMS,
    "accessionNumber": [f"0001664703-26-{i:06d}" for i in range(len(FORMS))],
    "filingDate": [f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}" for i in range(len(FORMS))],
    "primaryDocument": ["d.htm"] * len(FORMS),
    "acceptanceDateTime": [""] * len(FORMS),
    "isXBRL": [0] * len(FORMS), "isInlineXBRL": [0] * len(FORMS),
}}}

_ENTRY = """
export { getSubmissionsForTicker, listSecMaterialFilings, getEarningsAnalysis } from "{SRC}/yahoo-finance.ts";
export { setWorkerEnv } from "{SRC}/response.ts";
"""

_HARNESS = r"""
const [bundleUrl, fixturesPath] = process.argv.slice(-2);
const m = await import(bundleUrl);
const { readFileSync } = await import("node:fs");
const f = JSON.parse(readFileSync(fixturesPath, "utf8"));
m.setWorkerEnv({});
const requested = [];
let tickerFeed = f.tickerFeedEmpty;
globalThis.fetch = async (req) => {
  const u = new URL(typeof req === "string" ? req : req.url);
  requested.push(u.toString());
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  if (u.pathname.includes("/quoteSummary/") && u.searchParams.get("modules") === "secFilings") return new Response("busy", { status: 500 });
  if (u.pathname.includes("/quoteSummary/")) return Response.json({ quoteSummary: { result: [{ earningsTrend: { trend: [] }, earningsHistory: { history: [] } }], error: null } });
  if (u.pathname.endsWith("company_tickers.json")) return new Response("busy", { status: 503 });
  if (u.pathname.includes("browse-edgar")) {
    if (u.searchParams.get("company")) return new Response(f.nameFeedTrust);
    return new Response(tickerFeed);
  }
  if (u.pathname.startsWith("/submissions/")) return Response.json(f.subs);
  return new Response("not found", { status: 404 });
};
const out = {};
out.unknown = await m.getSubmissionsForTicker("BE");
out.unknownAskedCompanySearch = requested.some((r) => r.includes("company=BE"));
tickerFeed = f.tickerFeedBloom;
out.known = (await m.getSubmissionsForTicker("BE")).cikPadded;
out.capped = JSON.parse(await m.listSecMaterialFilings("BE", ["8-K"], 120));
out.small = JSON.parse(await m.listSecMaterialFilings("BE", ["10-Q"], 5));
out.earnings = JSON.parse(await m.getEarningsAnalysis("BE"));
console.log(JSON.stringify(out));
"""


def _worker_outputs() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry, bundle, harness, fx = (Path(tmp) / n for n in ("entry.ts", "bundle.mjs", "harness.mjs", "fixtures.json"))
        entry.write_text(_ENTRY.replace("{SRC}", (WORKER / "src").as_posix()), encoding="utf-8")
        harness.write_text(_HARNESS, encoding="utf-8")
        fx.write_text(json.dumps({"tickerFeedEmpty": TICKER_FEED_EMPTY, "tickerFeedBloom": TICKER_FEED_BLOOM, "nameFeedTrust": NAME_FEED_TRUST, "subs": SUBS}), encoding="utf-8")
        subprocess.run([str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=node", f"--outfile={bundle}", "--log-level=error"],
                       cwd=WORKER, check=True, capture_output=True, text=True, timeout=120)
        result = subprocess.run([node, str(harness), bundle.as_uri(), str(fx)], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


def _python_cik(ticker_feed: str) -> tuple[str | None, list[str]]:
    from yfmcp.clients import edgar

    requested: list[str] = []

    def urlopen(req, timeout=None):  # noqa: ARG001
        url = req.full_url
        requested.append(url)
        body = NAME_FEED_TRUST if "company=" in url and "company=&" not in url else ticker_feed
        resp = MagicMock()
        resp.__enter__.return_value = io.BytesIO(body.encode())
        resp.__exit__.return_value = False
        return resp

    failing = MagicMock()
    type(failing).info = property(lambda self: (_ for _ in ()).throw(RuntimeError("busy")))
    edgar._FILING_CIK_CACHE.clear()
    with patch.object(edgar.yf, "Ticker", return_value=failing), \
            patch.object(edgar, "_load_edgar_tickers", new=AsyncMock(side_effect=RuntimeError("busy"))), \
            patch.object(edgar._urlreq, "urlopen", new=urlopen):
        cik = asyncio.run(edgar._resolve_cik_for_ticker("BE"))
    edgar._FILING_CIK_CACHE.clear()
    return cik, requested


def _python_material(forms: list[str], limit: int) -> dict:
    import server as srv

    with patch("server._get_submissions_for_ticker", new=AsyncMock(return_value=(BLOOM_CIK, SUBS))):
        return json.loads(asyncio.run(srv.list_sec_material_filings("BE", forms, limit)))


class TestCikResolution(unittest.TestCase):
    """F-027: the company-name search is never asked."""

    def test_worker(self) -> None:
        out = _worker_outputs()
        self.assertEqual(out["unknown"], {"cikPadded": None, "submissions": None})
        self.assertFalse(out["unknownAskedCompanySearch"])
        self.assertEqual(out["known"], BLOOM_CIK)

    def test_python(self) -> None:
        cik, requested = _python_cik(TICKER_FEED_EMPTY)
        self.assertIsNone(cik)
        self.assertEqual(len(requested), 1)
        self.assertIn("CIK=BE", requested[0])
        self.assertEqual(_python_cik(TICKER_FEED_BLOOM)[0], BLOOM_CIK)


class TestMaterialFilingsCap(unittest.TestCase):
    """F-028: the cap is stated and a larger limit is warned."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.ts = _worker_outputs()

    def _drop_time(self, d: dict) -> dict:
        return {**d, "meta": {k: v for k, v in d["meta"].items() if k != "retrievedAt"}}

    def test_both_runtimes_agree(self) -> None:
        for py, ts in ((_python_material(["8-K"], 120), self.ts["capped"]), (_python_material(["10-Q"], 5), self.ts["small"])):
            self.assertEqual(self._drop_time(py), self._drop_time(ts))
            self.assertEqual(list(py), list(ts))

    def test_a_capped_limit_says_so(self) -> None:
        got = self.ts["capped"]
        self.assertEqual(len(got["filings"]), 20)
        self.assertEqual((got["limit"], got["moreAvailable"]), ({"requested": 120, "applied": 20, "maximum": 20}, True))
        self.assertEqual([w["code"] for w in got["warnings"]], ["LIMIT_CAPPED"])
        self.assertEqual(got["warnings"][0]["message"], "limit 120 is above the maximum of 20; at most 20 filings are returned, "
                         "and more matching filings exist in SEC's recent submissions.")

    def test_a_limit_within_the_cap_has_no_warning(self) -> None:
        got = self.ts["small"]
        self.assertEqual((len(got["filings"]), got["moreAvailable"]), (3, False))
        self.assertNotIn("warnings", got)


class TestEpsBasis(unittest.TestCase):
    """F-022: provider EPS is labelled as of unknown basis."""

    def test_worker_earnings_analysis(self) -> None:
        basis = _worker_outputs()["earnings"]["epsBasis"]
        self.assertEqual((basis["basis"], basis["determination"]), ("UNKNOWN", "NOT_DISCLOSED_BY_PROVIDER"))

    def test_python_earnings_analysis(self) -> None:
        import pandas as pd

        import server as srv

        company = MagicMock()
        company.fast_info.currency = "USD"
        for attr in ("earnings_estimate", "revenue_estimate", "eps_trend", "eps_revisions", "earnings_history", "growth_estimates"):
            setattr(company, attr, pd.DataFrame())
        srv._tool_cache._store.clear()
        with patch("server.yf.Ticker", return_value=company):
            out = json.loads(asyncio.run(srv.get_earnings_analysis("ZZEPS")))
        from yfmcp.evidence import EPS_BASIS
        self.assertEqual(out["epsBasis"], EPS_BASIS)
        self.assertEqual(_worker_outputs()["earnings"]["epsBasis"], EPS_BASIS)


if __name__ == "__main__":
    unittest.main()
