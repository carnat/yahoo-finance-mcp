#!/usr/bin/env python3
"""Endpoint builder exact-literal tests (Phase 4 unit 4, decision D8).

Every builder in worker/src/clients/endpoints.ts and yfmcp/clients/endpoints.py must return exactly the URL its call
sites wrote by hand before the unit-4 rewrite. The expected strings below are those hand-written templates, filled
with fixture inputs; both runtimes are checked against them.
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
sys.path.insert(0, str(ROOT))

from yfmcp.clients import endpoints as ep  # noqa: E402

WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"

CIK_PADDED = "0000320193"
CIK = 320193
ACC = "0000320193-25-000079"
ACC_ND = ACC.replace("-", "")
DOC = "aapl-20250927.htm"
CONCEPT = "Revenues"
SYM = "BRK-B"
SYM_ENC = "%5EGSPC"

# The hand-written templates the call sites used before unit 4.
WORKER_EXPECTED = {
    "quoteSummaryLiteral": f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{SYM}?modules=price,summaryDetail",
    "quoteSummaryEncoded": f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{SYM_ENC}?modules=price",
    "chart": f"https://query1.finance.yahoo.com/v8/finance/chart/{SYM}?range=5y&interval=1d",
    "chartBuster": f"https://query1.finance.yahoo.com/v8/finance/chart/{SYM}?range=6mo&interval=1d&_=1",
    "options": f"https://query2.finance.yahoo.com/v7/finance/options/{SYM}",
    "optionsDate": f"https://query2.finance.yahoo.com/v7/finance/options/{SYM}?date=1790000000",
    "timeseries2": f"https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{SYM}",
    "timeseries1": f"https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{SYM}",
    "search": "https://query1.finance.yahoo.com/v1/finance/search?q=AAPL&quotesCount=5&newsCount=0&enableFuzzyQuery=false",
    "screener": "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_gainers&count=2&lang=en-US&region=US&corsDomain=finance.yahoo.com",
    "quoteType": f"https://query1.finance.yahoo.com/v1/finance/quoteType/{SYM}?lang=en-US&region=US",
    "visualization": "https://query1.finance.yahoo.com/v1/finance/visualization?lang=en-US&region=US",
    "submissions": f"https://data.sec.gov/submissions/CIK{CIK_PADDED}.json",
    "companyFacts": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK_PADDED}.json",
    "companyConcept": f"https://data.sec.gov/api/xbrl/companyconcept/CIK{CIK_PADDED}/us-gaap/{CONCEPT}.json",
    "archiveDocument": f"https://www.sec.gov/Archives/edgar/data/{CIK}/{ACC_ND}/{DOC}",
    "filingIndex": f"https://www.sec.gov/Archives/edgar/data/{CIK}/{ACC_ND}/{ACC}-index.htm",
    "browseAtom": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=BRK.B&type=&dateb=&owner=include&count=10&output=atom",
    "archivesPrefix": "https://www.sec.gov/Archives/",
    "tickerIndex": "https://www.sec.gov/files/company_tickers.json",
    "secWww": "https://www.sec.gov",
    "manualWithCik": {
        "edgarIndexUrl": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={CIK_PADDED}&type=10-K&owner=include&count=10",
        "eftsSearchUrl": "https://efts.sec.gov/LATEST/search-index?q=BRK%2FB&forms=10-K",
        "note": "Fact not XBRL-tagged. Use search_filing_text instead.",
    },
    "manualWithoutCik": {
        "edgarIndexUrl": "https://www.sec.gov/cgi-bin/browse-edgar?company=BRK%2FB&action=getcompany&type=10-K&owner=include&count=10",
        "eftsSearchUrl": "https://efts.sec.gov/LATEST/search-index?q=BRK%2FB&forms=10-K",
        "note": "Fact not XBRL-tagged. Use search_filing_text instead.",
    },
}

_HARNESS = r"""
const m = await import(process.argv[2]);
const out = {
  quoteSummaryLiteral: m.yahooQuoteSummaryUrl("BRK-B", "price,summaryDetail"),
  quoteSummaryEncoded: m.yahooQuoteSummaryUrl("%5EGSPC", "price"),
  chart: m.yahooChartUrl("BRK-B", "range=5y&interval=1d"),
  chartBuster: m.yahooChartUrl("BRK-B", `range=6mo&interval=1d${"&_=1"}`),
  options: m.yahooOptionsUrl("BRK-B"),
  optionsDate: `${m.yahooOptionsUrl("BRK-B")}?date=${1790000000}`,
  timeseries2: m.yahooTimeseriesUrl("BRK-B"),
  timeseries1: m.yahooTimeseriesUrl("BRK-B", m.YAHOO_QUERY1),
  search: m.yahooSearchUrl("q=AAPL&quotesCount=5&newsCount=0&enableFuzzyQuery=false"),
  screener: m.yahooScreenerUrl("scrIds=day_gainers&count=2&lang=en-US&region=US&corsDomain=finance.yahoo.com"),
  quoteType: m.yahooQuoteTypeUrl("BRK-B"),
  visualization: m.YAHOO_VISUALIZATION_URL,
  submissions: m.secSubmissionsUrl("0000320193"),
  companyFacts: m.secCompanyFactsUrl("0000320193"),
  companyConcept: m.secCompanyConceptUrl("0000320193", "Revenues"),
  archiveDocument: m.secArchiveDocumentUrl(320193, "000032019325000079", "aapl-20250927.htm"),
  filingIndex: m.secFilingIndexUrl(320193, "000032019325000079", "0000320193-25-000079"),
  browseAtom: m.secBrowseEdgarAtomUrl("BRK.B"),
  archivesPrefix: m.SEC_ARCHIVES_PREFIX,
  tickerIndex: m.SEC_TICKER_INDEX_URL,
  secWww: m.SEC_WWW,
  manualWithCik: m.filingManualLookup("BRK/B", "0000320193", "10-K"),
  manualWithoutCik: m.filingManualLookup("BRK/B", null, "10-K"),
};
console.log(JSON.stringify(out));
"""


def _worker() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        bundle = Path(tmp) / "endpoints.mjs"
        harness = Path(tmp) / "harness.mjs"
        subprocess.run([str(ESBUILD), str(WORKER / "src" / "clients" / "endpoints.ts"), "--bundle", "--format=esm",
                        "--platform=neutral", f"--outfile={bundle}", "--log-level=error"], check=True)
        harness.write_text(_HARNESS, encoding="utf-8")
        result = subprocess.run([node, str(harness), bundle.as_uri()], check=True, capture_output=True, text=True, timeout=60)
    return json.loads(result.stdout)


class TestWorkerEndpoints(unittest.TestCase):
    def test_every_builder_returns_the_hand_written_literal(self) -> None:
        got = _worker()
        for key, expected in WORKER_EXPECTED.items():
            self.assertEqual(got[key], expected, key)


class TestPythonEndpoints(unittest.TestCase):
    def test_every_builder_returns_the_hand_written_literal(self) -> None:
        self.assertEqual(ep.yahoo_quote_summary_url(SYM, "price,summaryDetail"), WORKER_EXPECTED["quoteSummaryLiteral"])
        self.assertEqual(ep.YAHOO_QUOTE_SUMMARY_PREFIX + SYM, f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{SYM}")
        self.assertEqual(ep.YAHOO_QUOTE_SUMMARY_PREFIX_QUERY2 + SYM, f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{SYM}")
        self.assertEqual(ep.sec_submissions_url(CIK_PADDED), WORKER_EXPECTED["submissions"])
        self.assertEqual(ep.sec_company_facts_url(CIK_PADDED), WORKER_EXPECTED["companyFacts"])
        self.assertEqual(ep.sec_company_concept_url(CIK_PADDED, CONCEPT), WORKER_EXPECTED["companyConcept"])
        self.assertEqual(ep.sec_archive_document_url(CIK, ACC_ND, DOC), WORKER_EXPECTED["archiveDocument"])
        self.assertEqual(ep.sec_archive_document_url(str(CIK), ACC_ND, DOC), WORKER_EXPECTED["archiveDocument"])
        self.assertEqual(ep.sec_filing_index_url(CIK, ACC_ND, ACC), WORKER_EXPECTED["filingIndex"])
        self.assertEqual(ep.sec_browse_edgar_atom_url("BRK.B"), WORKER_EXPECTED["browseAtom"])
        self.assertEqual(ep.SEC_ARCHIVES_PREFIX, WORKER_EXPECTED["archivesPrefix"])
        self.assertEqual(ep.SEC_TICKER_INDEX_URL, WORKER_EXPECTED["tickerIndex"])
        self.assertEqual(ep.SEC_WWW, WORKER_EXPECTED["secWww"])

    def test_manual_lookup_keeps_its_python_encoding(self) -> None:
        # Python's company= parameter is not encoded and quote() keeps "/" (the Worker encodes both); unchanged by the move.
        self.assertEqual(ep._manual_lookup_payload("BRK/B", CIK_PADDED, "10-K", "n"), {
            "edgarIndexUrl": WORKER_EXPECTED["manualWithCik"]["edgarIndexUrl"],
            "eftsSearchUrl": "https://efts.sec.gov/LATEST/search-index?q=BRK/B&forms=10-K",
            "note": "n",
        })
        self.assertEqual(ep._manual_lookup_payload("BRK/B", None, "10-K", "n")["edgarIndexUrl"],
                         "https://www.sec.gov/cgi-bin/browse-edgar?company=BRK/B&action=getcompany&type=10-K&owner=include&count=10")


if __name__ == "__main__":
    unittest.main()
