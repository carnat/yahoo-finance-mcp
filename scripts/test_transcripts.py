#!/usr/bin/env python3
"""Unit tests for Phase 5B: Earnings Call Transcript & SEC Exhibit tools."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _ensure_mcp_available() -> None:
    try:
        from mcp.server.fastmcp import FastMCP  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _FastMCPStub:
        def __init__(self, *a: object, **kw: object) -> None:
            pass

        def tool(self, *a: object, **kw: object):  # type: ignore[return]
            if a and callable(a[0]):
                return a[0]

            def _decorator(fn):  # type: ignore[return]
                return fn

            return _decorator

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

from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402

if not getattr(_FastMCP, "_output_schema_patched", False):
    _orig_tool = _FastMCP.tool

    def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
        return _orig_tool(self, name=name, **kwargs)

    _FastMCP.tool = _patched_tool  # type: ignore[method-assign]
    _FastMCP._output_schema_patched = True  # type: ignore[attr-defined]

import server as srv  # noqa: E402
import yfmcp.clients.edgar as edgar_client  # noqa: E402


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


def _parse(raw: str) -> dict:
    parsed = json.loads(raw)
    if isinstance(parsed, dict) and "ok" in parsed and "data" in parsed and "meta" in parsed:
        if parsed.get("ok") and isinstance(parsed.get("data"), dict):
            result = dict(parsed["data"])
            meta = parsed.get("meta") or {}
            if "warnings" not in result and meta.get("warnings"):
                result["warnings"] = meta["warnings"]
            elif "warnings" not in result:
                result["warnings"] = []
            return result
    return parsed


def _parse_full(raw: str) -> dict:
    """Return the full envelope without unwrapping."""
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Test: _edgar_list_exhibits_from_index
# ---------------------------------------------------------------------------

MOCK_INDEX_HTML = """
<html><body>
<table>
<tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>
<tr><td>1</td><td>EARNINGS RELEASE</td><td><a href="ex99-1.htm">ex99-1.htm</a></td><td>EX-99.1</td><td>50000</td></tr>
<tr><td>2</td><td>CONFERENCE CALL TRANSCRIPT</td><td><a href="ex99-2.htm">ex99-2.htm</a></td><td>EX-99.2</td><td>120000</td></tr>
<tr><td>3</td><td>ADDITIONAL EXHIBITS</td><td><a href="ex99-3.htm">ex99-3.htm</a></td><td>EX-99.3</td><td>30000</td></tr>
</table>
</body></html>
"""


class TestEdgarListExhibitsFromIndex(unittest.TestCase):
    def test_parses_exhibit_table(self):
        with patch("yfmcp.clients.edgar._edgar_get_html", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = MOCK_INDEX_HTML
            exhibits = _run(edgar_client._edgar_list_exhibits_from_index("https://www.sec.gov/Archives/edgar/data/12345/0000123450-24-000001-index.htm"))
        self.assertEqual(len(exhibits), 3)
        self.assertEqual(exhibits[0]["sequence"], "1")
        self.assertEqual(exhibits[0]["type"], "EX-99.1")
        self.assertEqual(exhibits[0]["document"], "ex99-1.htm")
        self.assertEqual(exhibits[0]["description"], "EARNINGS RELEASE")
        self.assertEqual(exhibits[1]["type"], "EX-99.2")
        self.assertEqual(exhibits[1]["document"], "ex99-2.htm")
        self.assertEqual(exhibits[1]["description"], "CONFERENCE CALL TRANSCRIPT")

    def test_empty_on_fetch_failure(self):
        with patch("yfmcp.clients.edgar._edgar_get_html", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None
            exhibits = _run(edgar_client._edgar_list_exhibits_from_index("https://www.sec.gov/invalid"))
        self.assertEqual(exhibits, [])


# ---------------------------------------------------------------------------
# Test: _filter_paragraphs_by_topics
# ---------------------------------------------------------------------------

class TestFilterParagraphsByTopics(unittest.TestCase):
    def test_filters_matching_paragraphs(self):
        text = (
            "We are excited about our AI investments this quarter.\n\n"
            "Revenue grew 15% year over year driven by cloud services.\n\n"
            "Our supply chain remains resilient despite global challenges.\n\n"
            "Short unrelated line."
        )
        results = srv._filter_paragraphs_by_topics(text, ["AI", "supply chain"])
        self.assertEqual(len(results), 2)
        self.assertIn("AI", results[0]["matchedTopics"])
        self.assertIn("supply chain", results[1]["matchedTopics"])

    def test_empty_topics_returns_nothing(self):
        text = "Some paragraph about AI.\n\nAnother paragraph about revenue."
        results = srv._filter_paragraphs_by_topics(text, [])
        self.assertEqual(results, [])

    def test_case_insensitive_matching(self):
        text = "The company discussed artificial intelligence developments extensively this quarter."
        results = srv._filter_paragraphs_by_topics(text, ["ARTIFICIAL INTELLIGENCE"])
        self.assertEqual(len(results), 1)


# ---------------------------------------------------------------------------
# Test: list_sec_filing_exhibits
# ---------------------------------------------------------------------------

class TestListSecFilingExhibits(unittest.TestCase):
    def test_returns_exhibits(self):
        with patch("yfmcp.tools.earnings._edgar_cik_from_accession", return_value=12345), \
             patch("yfmcp.tools.earnings._edgar_list_exhibits_from_index", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = [
                {"sequence": "1", "description": "Press Release", "document": "ex99-1.htm", "type": "EX-99.1", "size": "50000"},
            ]
            raw = _run(srv.list_sec_filing_exhibits(ticker="AAPL", accessionNumber="0000320193-24-000081"))
        data = _parse(raw)
        self.assertEqual(data["ticker"], "AAPL")
        self.assertEqual(data["accessionNumber"], "0000320193-24-000081")
        self.assertEqual(len(data["exhibits"]), 1)
        self.assertEqual(data["exhibits"][0]["type"], "EX-99.1")

    def test_validation_error_missing_accession(self):
        raw = _run(srv.list_sec_filing_exhibits(ticker="AAPL", accessionNumber=""))
        envelope = _parse_full(raw)
        self.assertFalse(envelope["ok"])
        self.assertIn("accessionNumber", envelope["error"])


# ---------------------------------------------------------------------------
# Test: get_sec_filing_exhibit_content
# ---------------------------------------------------------------------------

class TestGetSecFilingExhibitContent(unittest.TestCase):
    def test_returns_full_text_without_topics(self):
        mock_html = "<html><body><p>Revenue grew 20% in the quarter.</p><p>Net income was $1.5 billion.</p></body></html>"
        with patch("yfmcp.tools.earnings._edgar_cik_from_accession", return_value=12345), \
             patch("yfmcp.tools.earnings._edgar_get_html", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_html
            raw = _run(srv.get_sec_filing_exhibit_content(
                ticker="AAPL", accessionNumber="0000320193-24-000081", fileName="ex99-1.htm"
            ))
        data = _parse(raw)
        self.assertIn("Revenue grew", data["text"])
        self.assertFalse(data["truncated"])
        self.assertIsNone(data["filteredByTopics"])

    def test_returns_filtered_paragraphs_with_topics(self):
        mock_html = "<html><body><p>Revenue grew 20% in the quarter due to AI services.</p><p>Net income was $1.5 billion from operations.</p></body></html>"
        with patch("yfmcp.tools.earnings._edgar_cik_from_accession", return_value=12345), \
             patch("yfmcp.tools.earnings._edgar_get_html", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_html
            raw = _run(srv.get_sec_filing_exhibit_content(
                ticker="AAPL", accessionNumber="0000320193-24-000081", fileName="ex99-1.htm", topics=["AI"]
            ))
        data = _parse(raw)
        self.assertEqual(data["filteredByTopics"], ["AI"])
        self.assertIsInstance(data["matchedParagraphs"], list)


# ---------------------------------------------------------------------------
# Test: get_earnings_call_transcript (integration with mocks)
# ---------------------------------------------------------------------------

class TestGetEarningsCallTranscript(unittest.TestCase):
    def test_returns_sec_not_found_when_no_8k(self):
        with patch("yfmcp.tools.earnings._resolve_latest_earnings_sec_source", new_callable=AsyncMock) as mock_src:
            mock_src.return_value = None
            raw = _run(srv.get_earnings_call_transcript(ticker="AAPL"))
        data = _parse(raw)
        self.assertEqual(data["status"], "SEC_8K_NOT_FOUND")
        self.assertIsNone(data["content"])
        self.assertEqual(data["attemptedSources"][0]["sourceType"], "sec_8k_exhibit")
        self.assertEqual(data["attemptedSources"][0]["status"], "NOT_FOUND")
        self.assertEqual(data["attemptedSources"][1]["sourceType"], "company_ir")
        self.assertEqual(data["attemptedSources"][2]["sourceType"], "public_transcript_url")
        self.assertEqual(data["attemptedSources"][3]["sourceType"], "alpha_vantage")
        self.assertIsInstance(data["nextRecommendedFallback"], dict)

    def test_returns_exhibit_not_found_when_no_transcript(self):
        mock_sec = {"accessionNumber": "0000320193-24-000081", "filingDate": "2024-01-25", "acceptedAt": "2024-01-25T16:00:00"}
        with patch("yfmcp.tools.earnings._resolve_latest_earnings_sec_source", new_callable=AsyncMock, return_value=mock_sec), \
             patch("yfmcp.tools.earnings._edgar_cik_from_accession", return_value=320193), \
             patch("yfmcp.tools.earnings._edgar_list_exhibits_from_index", new_callable=AsyncMock) as mock_list:
            # Only EX-99.1 (press release), no transcript
            mock_list.return_value = [
                {"sequence": "1", "description": "PRESS RELEASE", "document": "ex99-1.htm", "type": "EX-99.1", "size": "50000"},
            ]
            raw = _run(srv.get_earnings_call_transcript(ticker="AAPL"))
        data = _parse(raw)
        self.assertEqual(data["status"], "SEC_EXHIBIT_NOT_FOUND")
        self.assertIn("availableExhibits", data)
        self.assertEqual(data["attemptedSources"][0]["sourceType"], "sec_8k_exhibit")
        self.assertEqual(data["attemptedSources"][0]["status"], "NOT_FOUND")
        self.assertEqual(data["attemptedSources"][0]["exhibitsSearched"], 1)
        self.assertEqual(data["attemptedSources"][-1]["sourceType"], "alpha_vantage")
        self.assertIsInstance(data["nextRecommendedFallback"], dict)

    def test_returns_structured_metadata_when_sec_fetch_fails(self):
        mock_sec = {"accessionNumber": "0000320193-24-000081", "filingDate": "2024-01-25", "acceptedAt": "2024-01-25T16:00:00"}
        with patch("yfmcp.tools.earnings._resolve_latest_earnings_sec_source", new_callable=AsyncMock, return_value=mock_sec), \
             patch("yfmcp.tools.earnings._edgar_cik_from_accession", return_value=320193), \
             patch("yfmcp.tools.earnings._edgar_list_exhibits_from_index", new_callable=AsyncMock) as mock_list, \
             patch("yfmcp.tools.earnings._edgar_get_html", new_callable=AsyncMock) as mock_get:
            mock_list.return_value = [
                {"sequence": "2", "description": "EARNINGS CALL TRANSCRIPT", "document": "ex99-2.htm", "type": "EX-99.2", "size": "120000"},
            ]
            mock_get.return_value = None
            raw = _run(srv.get_earnings_call_transcript(ticker="AAPL"))
        data = _parse(raw)
        self.assertEqual(data["status"], "FETCH_ERROR")
        self.assertEqual(data["attemptedSources"][0]["status"], "FETCH_ERROR")
        self.assertIn("documentUrl", data)
        self.assertIsInstance(data["nextRecommendedFallback"], dict)

    def test_alpha_vantage_fallback_success_when_sec_exhibit_missing(self):
        mock_sec = {"accessionNumber": "0000320193-24-000081", "filingDate": "2024-01-25", "acceptedAt": "2024-01-25T16:00:00"}
        alpha_payload = {
            "sourceType": "alpha_vantage",
            "status": "OK",
            "filteredByTopics": None,
            "content": "Operator: Welcome to the call.",
            "totalTextLength": 30,
            "truncated": False,
            "warnings": [],
        }
        alpha_attempt = {"sourceType": "alpha_vantage", "status": "SUCCESS", "quarter": "2024Q1", "rateLimit": {"provider": "alpha_vantage", "used": True}}
        with patch("yfmcp.tools.earnings._resolve_latest_earnings_sec_source", new_callable=AsyncMock, return_value=mock_sec), \
             patch("yfmcp.tools.earnings._edgar_cik_from_accession", return_value=320193), \
             patch("yfmcp.tools.earnings._edgar_list_exhibits_from_index", new_callable=AsyncMock) as mock_list, \
             patch("yfmcp.tools.earnings._fetch_alpha_vantage_transcript", new_callable=AsyncMock) as mock_alpha:
            mock_list.return_value = [
                {"sequence": "1", "description": "PRESS RELEASE", "document": "ex99-1.htm", "type": "EX-99.1", "size": "50000"},
            ]
            mock_alpha.return_value = (alpha_payload, alpha_attempt)
            raw = _run(srv.get_earnings_call_transcript(ticker="AAPL"))
        data = _parse(raw)
        self.assertEqual(data["status"], "OK")
        self.assertEqual(data["sourceType"], "alpha_vantage")
        self.assertIn("Welcome to the call", data["content"])
        self.assertIsNone(data["nextRecommendedFallback"])
        self.assertEqual(data["attemptedSources"][-1]["status"], "SUCCESS")

    def test_returns_transcript_content_when_found(self):
        mock_sec = {"accessionNumber": "0000320193-24-000081", "filingDate": "2024-01-25", "acceptedAt": "2024-01-25T16:00:00"}
        mock_html = "<html><body><p>Good afternoon everyone, welcome to the earnings call.</p><p>We had a fantastic quarter driven by AI.</p></body></html>"
        with patch("yfmcp.tools.earnings._resolve_latest_earnings_sec_source", new_callable=AsyncMock, return_value=mock_sec), \
             patch("yfmcp.tools.earnings._edgar_cik_from_accession", return_value=320193), \
             patch("yfmcp.tools.earnings._edgar_list_exhibits_from_index", new_callable=AsyncMock) as mock_list, \
             patch("yfmcp.tools.earnings._edgar_get_html", new_callable=AsyncMock) as mock_get:
            mock_list.return_value = [
                {"sequence": "1", "description": "PRESS RELEASE", "document": "ex99-1.htm", "type": "EX-99.1", "size": "50000"},
                {"sequence": "2", "description": "EARNINGS CALL TRANSCRIPT", "document": "ex99-2.htm", "type": "EX-99.2", "size": "120000"},
            ]
            mock_get.return_value = mock_html
            raw = _run(srv.get_earnings_call_transcript(ticker="AAPL"))
        data = _parse(raw)
        self.assertEqual(data["status"], "OK")
        self.assertIn("earnings call", data["content"])
        self.assertEqual(data["exhibitType"], "EX-99.2")
        self.assertEqual(data["attemptedSources"][0]["status"], "SUCCESS")
        self.assertIsNone(data["nextRecommendedFallback"])

    def test_topic_filtering(self):
        mock_sec = {"accessionNumber": "0000320193-24-000081", "filingDate": "2024-01-25", "acceptedAt": "2024-01-25T16:00:00"}
        mock_html = "<html><body><p>Good afternoon everyone, welcome to the earnings call for Apple Inc.</p><p>Our AI investments drove significant growth this quarter with new model deployments.</p><p>Supply chain improvements reduced costs by 8% globally.</p></body></html>"
        with patch("yfmcp.tools.earnings._resolve_latest_earnings_sec_source", new_callable=AsyncMock, return_value=mock_sec), \
             patch("yfmcp.tools.earnings._edgar_cik_from_accession", return_value=320193), \
             patch("yfmcp.tools.earnings._edgar_list_exhibits_from_index", new_callable=AsyncMock) as mock_list, \
             patch("yfmcp.tools.earnings._edgar_get_html", new_callable=AsyncMock) as mock_get:
            mock_list.return_value = [
                {"sequence": "2", "description": "TRANSCRIPT", "document": "ex99-2.htm", "type": "EX-99.2", "size": "120000"},
            ]
            mock_get.return_value = mock_html
            raw = _run(srv.get_earnings_call_transcript(ticker="AAPL", topics=["AI"]))
        data = _parse(raw)
        self.assertEqual(data["status"], "OK")
        self.assertEqual(data["filteredByTopics"], ["AI"])
        self.assertIsInstance(data["matchedParagraphs"], list)
        # Content should be None when filtering by topics
        self.assertIsNone(data["content"])
        self.assertEqual(data["attemptedSources"][0]["status"], "SUCCESS")
        self.assertIsNone(data["nextRecommendedFallback"])


if __name__ == "__main__":
    unittest.main()
