#!/usr/bin/env python3
"""SEC Filing Intelligence Layer tests.

Offline/unit tests for the new SEC intelligence tools:
- list_sec_material_filings
- get_sec_filing_intelligence
- get_sec_filing_section_markdown
- extract_sec_filing_fact enhancements (retrieval_path, alternative_queries)

Run: PYTHONPATH=. python scripts/test_sec_intelligence.py
"""

import asyncio
import json
import os
import sys
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Patch FastMCP.tool to accept output_schema (not in mcp>=1.9)
from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402
_orig_tool = _FastMCP.tool


def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
    return _orig_tool(self, name=name, **kwargs)


_FastMCP.tool = _patched_tool  # type: ignore[method-assign]

import server as srv


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MOCK_SUBMISSIONS = {
    "filings": {
        "recent": {
            "form": ["10-K", "10-Q", "8-K", "4", "4", "10-K", "SC 13G", "DEF 14A", "144"],
            "filingDate": [
                "2024-11-01", "2024-08-01", "2024-07-15",
                "2024-06-01", "2024-05-01", "2023-11-01",
                "2024-04-01", "2024-03-15", "2024-02-01",
            ],
            "accessionNumber": [
                "0000320193-24-000006", "0000320193-24-000005", "0000320193-24-000004",
                "0000320193-24-000003", "0000320193-24-000002", "0000320193-23-000006",
                "0000320193-24-000001", "0000320193-24-000007", "0000320193-24-000008",
            ],
            "primaryDocument": [
                "aapl-20240928.htm", "aapl-20240629.htm", "aapl-8k-2024.htm",
                "form4.xml", "form4.xml", "aapl-20230930.htm",
                "sc13g.htm", "defproxy.htm", "form144.htm",
            ],
            "acceptanceDateTime": [
                "2024-11-01T16:30:00.000Z", "2024-08-01T16:30:00.000Z", "2024-07-15T16:30:00.000Z",
                "2024-06-01T16:30:00.000Z", "2024-05-01T16:30:00.000Z", "2023-11-01T16:30:00.000Z",
                "2024-04-01T16:30:00.000Z", "2024-03-15T16:30:00.000Z", "2024-02-01T16:30:00.000Z",
            ],
        }
    }
}

MOCK_CIK = "0000320193"

MOCK_HTML_WITH_SECTIONS = """
<html><body>
<h1>PART I</h1>
<h2>Item 1. Business</h2>
<p>Apple Inc. designs, manufactures, and markets smartphones.</p>
<h2>Item 1A. Risk Factors</h2>
<p>The Company's business is subject to various risks.</p>
<p>Global economic conditions may adversely affect demand.</p>
<table><tr><th>Risk Category</th><th>Impact</th></tr>
<tr><td>Supply Chain</td><td>High</td></tr>
<tr><td>Currency</td><td>Medium</td></tr></table>
<p>Competition in the technology industry is intense.</p>
<h2>Item 2. Properties</h2>
<p>The Company's principal executive offices are in Cupertino.</p>
</body></html>
"""

MOCK_COMPANY_FACTS = {
    "facts": {
        "us-gaap": {
            "Revenues": {
                "units": {
                    "USD": [
                        {"val": 391035000000, "end": "2024-09-28", "form": "10-K", "filed": "2024-11-01"},
                        {"val": 383285000000, "end": "2023-09-30", "form": "10-K", "filed": "2023-11-01"},
                    ]
                }
            },
            "NetIncomeLoss": {
                "units": {
                    "USD": [
                        {"val": 93736000000, "end": "2024-09-28", "form": "10-K", "filed": "2024-11-01"},
                    ]
                }
            },
            "CashAndCashEquivalentsAtCarryingValue": {
                "units": {
                    "USD": [
                        {"val": 29965000000, "end": "2024-09-28", "form": "10-K", "filed": "2024-11-01"},
                    ]
                }
            },
        }
    }
}


# ---------------------------------------------------------------------------
# Tests: list_sec_material_filings
# ---------------------------------------------------------------------------

class TestListSecMaterialFilings(unittest.TestCase):
    def _run(self, coro):
        return asyncio.run(coro)

    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_filters_noisy_forms(self, mock_subs):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        result = self._run(srv.list_sec_material_filings("AAPL"))
        data = json.loads(result)
        self.assertIn("filings", data)
        filing_types = [f["filingType"] for f in data["filings"]]
        # Should NOT include Form 4, SC 13G, or 144
        for noisy in ("4", "SC 13G", "144"):
            self.assertNotIn(noisy, filing_types)
        # Should include 10-K, 10-Q, 8-K, DEF 14A
        self.assertIn("10-K", filing_types)
        self.assertIn("10-Q", filing_types)
        self.assertIn("8-K", filing_types)
        self.assertIn("DEF 14A", filing_types)

    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_custom_forms_filter(self, mock_subs):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        result = self._run(srv.list_sec_material_filings("AAPL", forms=["10-K"]))
        data = json.loads(result)
        filing_types = [f["filingType"] for f in data["filings"]]
        for ft in filing_types:
            self.assertEqual(ft, "10-K")

    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_limit_respected(self, mock_subs):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        result = self._run(srv.list_sec_material_filings("AAPL", limit=2))
        data = json.loads(result)
        self.assertLessEqual(len(data["filings"]), 2)

    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_ticker_not_found(self, mock_subs):
        mock_subs.return_value = (None, None)
        result = self._run(srv.list_sec_material_filings("FAKE"))
        data = json.loads(result)
        # Should indicate error
        self.assertTrue(
            data.get("error") is not None or data.get("ok") is False,
            "Expected error for missing ticker",
        )

    def test_invalid_ticker(self):
        result = self._run(srv.list_sec_material_filings("<script>"))
        data = json.loads(result)
        self.assertTrue(
            data.get("error") is not None or data.get("ok") is False,
        )

    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_output_shape(self, mock_subs):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        result = self._run(srv.list_sec_material_filings("AAPL"))
        data = json.loads(result)
        self.assertEqual(data["ticker"], "AAPL")
        self.assertEqual(data["cik"], MOCK_CIK)
        self.assertIn("meta", data)
        self.assertIn("materialFormsFilter", data["meta"])
        if data["filings"]:
            f = data["filings"][0]
            self.assertIn("filingType", f)
            self.assertIn("filingDate", f)
            self.assertIn("accessionNumber", f)
            self.assertIn("documentUrl", f)


# ---------------------------------------------------------------------------
# Tests: get_sec_filing_intelligence
# ---------------------------------------------------------------------------

class TestGetSecFilingIntelligence(unittest.TestCase):
    def _run(self, coro):
        return asyncio.run(coro)

    @patch.object(srv, "_index_sec_filing_impl", new_callable=AsyncMock)
    @patch.object(srv, "_edgar_get_company_facts", new_callable=AsyncMock)
    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_returns_intelligence_map(self, mock_subs, mock_facts, mock_index):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        mock_facts.return_value = MOCK_COMPANY_FACTS
        mock_index.return_value = json.dumps({
            "ticker": "AAPL",
            "index": {
                "sections": [{"heading": "Business"}, {"heading": "Risk Factors"}],
                "tables": [{"tableId": 0}, {"tableId": 1}],
                "keywordMap": {},
            }
        })
        result = self._run(srv.get_sec_filing_intelligence("AAPL"))
        data = json.loads(result)
        self.assertEqual(data["ticker"], "AAPL")
        self.assertTrue(data["xbrl_available"])
        self.assertIn("revenue", data["xbrl_facts"])
        self.assertEqual(data["xbrl_facts"]["revenue"]["value"], 391035000000)
        self.assertEqual(data["xbrl_facts"]["revenue"]["confidence"], "HIGH")
        self.assertEqual(data["index"]["sections_count"], 2)
        self.assertEqual(data["index"]["tables_count"], 2)
        self.assertIn("recommended_queries", data)
        self.assertEqual(data["status"]["xbrl"], "OK")
        self.assertEqual(data["status"]["index"], "OK")

    @patch.object(srv, "_index_sec_filing_impl", new_callable=AsyncMock)
    @patch.object(srv, "_edgar_get_company_facts", new_callable=AsyncMock)
    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_filing_index_parameter(self, mock_subs, mock_facts, mock_index):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        mock_facts.return_value = None
        mock_index.return_value = json.dumps({"ticker": "AAPL", "index": {"sections": [], "tables": []}})
        result = self._run(srv.get_sec_filing_intelligence("AAPL", filing_index=1))
        data = json.loads(result)
        # Should get the second 10-K (2023)
        self.assertEqual(data["filing"]["accessionNumber"], "0000320193-23-000006")
        self.assertFalse(data["xbrl_available"])

    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_filing_not_found(self, mock_subs):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        result = self._run(srv.get_sec_filing_intelligence("AAPL", filing_type="S-1"))
        data = json.loads(result)
        self.assertTrue(data.get("error") is not None or data.get("ok") is False)


# ---------------------------------------------------------------------------
# Tests: get_sec_filing_section_markdown
# ---------------------------------------------------------------------------

class TestGetSecFilingSectionMarkdown(unittest.TestCase):
    def _run(self, coro):
        return asyncio.run(coro)

    @patch.object(srv, "_edgar_get_html", new_callable=AsyncMock)
    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_extracts_section_as_markdown(self, mock_subs, mock_html):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        mock_html.return_value = MOCK_HTML_WITH_SECTIONS
        result = self._run(srv.get_sec_filing_section_markdown("AAPL", section="Item 1A"))
        data = json.loads(result)
        self.assertIn("markdown", data)
        self.assertIn("Risk Factors", data["section"])
        self.assertGreater(data["word_count"], 0)
        self.assertGreater(data["tables_in_section"], 0)
        self.assertEqual(data["source"], "html_parser_fallback")
        self.assertFalse(data["truncated"])
        # Should contain table content in markdown format
        self.assertIn("Supply Chain", data["markdown"])

    @patch.object(srv, "_edgar_get_html", new_callable=AsyncMock)
    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_section_not_found(self, mock_subs, mock_html):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        mock_html.return_value = MOCK_HTML_WITH_SECTIONS
        result = self._run(srv.get_sec_filing_section_markdown("AAPL", section="Item 99"))
        data = json.loads(result)
        self.assertTrue(data.get("error") is not None or data.get("ok") is False)

    @patch.object(srv, "_edgar_get_html", new_callable=AsyncMock)
    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_truncation(self, mock_subs, mock_html):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        mock_html.return_value = MOCK_HTML_WITH_SECTIONS
        result = self._run(srv.get_sec_filing_section_markdown("AAPL", section="Item 1A", max_chars=1000))
        data = json.loads(result)
        # Content is short enough to not truncate with 1000 char limit here
        self.assertIn("markdown", data)

    @patch.object(srv, "_edgar_get_html", new_callable=AsyncMock)
    @patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock)
    def test_mu_sec_filing_section_toc_skip(self, mock_subs, mock_html):
        mock_subs.return_value = (MOCK_CIK, MOCK_SUBMISSIONS)
        mu_html = """
        <html><body>
        <div id="table_of_contents">
            <h2>Table of Contents</h2>
            <h3><a href="#item1a">Item 1A. Risk Factors</a></h3>
            <h3><a href="#item2">Item 2. Properties</a></h3>
        </div>
        <hr/>
        <div id="item1a">
            <h2>Item 1A. Risk Factors</h2>
            <p>This is the actual risk factors content for Micron Technology (MU).</p>
            <p>We face risks relating to DRAM and NAND price fluctuations.</p>
        </div>
        <div id="item2">
            <h2>Item 2. Properties</h2>
            <p>Actual properties content.</p>
        </div>
        </body></html>
        """
        mock_html.return_value = mu_html
        result = self._run(srv.get_sec_filing_section_markdown("MU", section="Item 1A", max_chars=2500))
        data = json.loads(result)
        self.assertNotIn("error", data)
        self.assertIn("markdown", data)
        self.assertIn("matchedHeading", data)
        self.assertIn("tocSkipped", data)
        self.assertIn("sectionStartOffset", data)
        self.assertIn("sectionEndOffset", data)
        self.assertTrue(data["tocSkipped"])
        self.assertEqual(data["matchedHeading"], "Item 1A. Risk Factors")
        self.assertIn("Micron Technology (MU)", data["markdown"])
        self.assertNotIn("Table of Contents", data["markdown"])


# ---------------------------------------------------------------------------
# Tests: extract_sec_filing_fact enhancements
# ---------------------------------------------------------------------------

class TestExtractSecFilingFactEnhancements(unittest.TestCase):
    def _run(self, coro):
        return asyncio.run(coro)

    def test_retrieval_path_mapping(self):
        self.assertEqual(srv._map_extraction_to_retrieval_path("XBRL"), "XBRL")
        self.assertEqual(srv._map_extraction_to_retrieval_path("COMPANYFACTS"), "XBRL")
        self.assertEqual(srv._map_extraction_to_retrieval_path("HTML_TABLE"), "INDEXED_TABLE")
        self.assertEqual(srv._map_extraction_to_retrieval_path("TEXT_SEARCH"), "SECTION_TEXT")
        self.assertEqual(srv._map_extraction_to_retrieval_path("FULL_DOC_SEARCH"), "FULL_DOC_SEARCH")
        self.assertEqual(srv._map_extraction_to_retrieval_path("NONE"), "NONE")
        self.assertEqual(srv._map_extraction_to_retrieval_path("something_else"), "UNKNOWN")

    def test_alternative_queries_on_not_disclosed(self):
        payload = {"source": "NOT_DISCLOSED", "confidence": "NOT_DISCLOSED"}
        suggestions = srv._suggest_alternative_queries("geographic_revenue", "China", payload)
        self.assertGreater(len(suggestions), 0)
        self.assertTrue(any("China" in s for s in suggestions))

    def test_no_alternative_queries_on_success(self):
        payload = {"source": "XBRL", "confidence": "HIGH"}
        suggestions = srv._suggest_alternative_queries("total_revenue", None, payload)
        self.assertEqual(len(suggestions), 0)

    @patch("server.get_filing_data", new_callable=AsyncMock)
    def test_response_includes_retrieval_path(self, mock_filing_data):
        mock_filing_data.return_value = json.dumps({
            "value": "391,035",
            "extractionMethod": "XBRL",
            "source": "companyfacts",
            "confidence": "PARSED_HTML",
            "warnings": [],
        })
        result = self._run(srv.extract_sec_filing_fact(
            ticker="AAPL", fact_type=srv.FilingFactType.total_revenue
        ))
        data = json.loads(result)
        self.assertIn("retrieval_path", data)
        self.assertEqual(data["retrieval_path"], "XBRL")
        self.assertIn("alternative_queries", data)


# ---------------------------------------------------------------------------
# Tests: HTML to Markdown conversion
# ---------------------------------------------------------------------------

class TestHtmlToMarkdownFallback(unittest.TestCase):
    def test_basic_conversion(self):
        html = """<h2>Section Title</h2>
<p>Some paragraph text here.</p>
<table><tr><th>Col A</th><th>Col B</th></tr>
<tr><td>Value 1</td><td>Value 2</td></tr></table>
<p>More text after table.</p>
<h2>Next Section</h2>"""
        md = srv._html_to_markdown_fallback(html, 0, html.index("<h2>Next Section</h2>"))
        self.assertIn("## Section Title", md)
        self.assertIn("Col A", md)
        self.assertIn("Value 1", md)
        self.assertIn("|", md)  # pipe table format

    def test_table_to_markdown(self):
        table_html = "<table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>2</td></tr></table>"
        md = srv._html_table_to_markdown(table_html)
        self.assertIn("| A | B |", md)
        self.assertIn("| --- | --- |", md)
        self.assertIn("| 1 | 2 |", md)


# ---------------------------------------------------------------------------
# Tests: XBRL extraction helper
# ---------------------------------------------------------------------------

class TestXbrlLatestAnnualExtraction(unittest.TestCase):
    def test_extracts_latest_value(self):
        result = srv._extract_xbrl_latest_annual(
            MOCK_COMPANY_FACTS, ["Revenues"]
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["value"], 391035000000)
        self.assertEqual(result["confidence"], "HIGH")
        self.assertEqual(result["unit"], "USD")

    def test_returns_none_for_missing_concept(self):
        result = srv._extract_xbrl_latest_annual(
            MOCK_COMPANY_FACTS, ["NonExistentConcept"]
        )
        self.assertIsNone(result)

    def test_fallback_to_second_concept(self):
        result = srv._extract_xbrl_latest_annual(
            MOCK_COMPANY_FACTS, ["NonExistent", "NetIncomeLoss"]
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["value"], 93736000000)


if __name__ == "__main__":
    unittest.main()
