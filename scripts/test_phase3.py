#!/usr/bin/env python3
"""Phase 3 tests: Input validation and security hardening.

These are offline/unit tests — no network calls required.
Run: PYTHONPATH=. python scripts/test_phase3.py
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Patch FastMCP.tool to accept output_schema (not in mcp>=1.9)
from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402
_orig_tool = _FastMCP.tool
def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
    return _orig_tool(self, name=name, **kwargs)
_FastMCP.tool = _patched_tool  # type: ignore[method-assign]

import server as srv


class TestTickerValidation(unittest.TestCase):
    def test_valid_tickers(self):
        for t in ["AAPL", "MSFT", "BTC-USD", "^VIX", "^GSPC", "SPY", "AAPL.L", "BRK.B", "BRK=B"]:
            self.assertIsNone(srv._validate_ticker(t), f"Expected '{t}' to be valid")

    def test_invalid_tickers_empty(self):
        self.assertIsNotNone(srv._validate_ticker(""))

    def test_invalid_ticker_too_long(self):
        self.assertIsNotNone(srv._validate_ticker("A" * 21))

    def test_invalid_ticker_special_chars(self):
        for bad in ["AAPL@", "A B", "A!B", "<script>", "'; DROP TABLE--"]:
            self.assertIsNotNone(srv._validate_ticker(bad), f"Expected '{bad}' to be invalid")

    def test_lowercase_auto_normalized(self):
        # validate_ticker should accept lowercase (normalizes to upper internally)
        self.assertIsNone(srv._validate_ticker("aapl"))


class TestAccessionValidation(unittest.TestCase):
    def test_valid_accession(self):
        self.assertIsNone(srv._validate_accession("0000024741-26-000124"))

    def test_invalid_accession_format(self):
        for bad in ["bad-accession", "1234567890-12-123456-extra", "AAPL", ""]:
            self.assertIsNotNone(srv._validate_accession(bad), f"Expected '{bad}' to be invalid")


class TestBatchValidation(unittest.TestCase):
    def test_valid_batch(self):
        self.assertIsNone(srv._validate_batch_tickers(["AAPL", "MSFT"]))
        self.assertIsNone(srv._validate_batch_tickers(["A", "B", "C", "D", "E"]))

    def test_batch_too_large(self):
        self.assertIsNotNone(srv._validate_batch_tickers(["A"] * 6))

    def test_empty_batch_ok(self):
        self.assertIsNone(srv._validate_batch_tickers([]))


class TestSecUrlValidation(unittest.TestCase):
    def test_valid_sec_url(self):
        url = "https://www.sec.gov/Archives/edgar/data/320193/000032019323000077/aapl-20230930.htm"
        self.assertIsNone(srv._validate_sec_url(url))

    def test_invalid_urls(self):
        bad_urls = [
            "https://evil.com/Archives/aapl.htm",
            "http://www.sec.gov/Archives/aapl.htm",  # http, not https
            "https://www.sec.gov/cgi-bin/browse-edgar",  # not /Archives/
            "javascript:alert(1)",
            "",
            "file:///etc/passwd",
        ]
        for url in bad_urls:
            self.assertIsNotNone(srv._validate_sec_url(url), f"Expected '{url}' to be invalid")


class TestHtmlSanitization(unittest.TestCase):
    def test_strips_script_tags(self):
        html = '<div>text</div><script>alert("xss")</script><p>safe</p>'
        result = srv._sanitize_sec_html(html)
        self.assertNotIn("<script>", result)
        self.assertNotIn("alert", result)
        self.assertIn("safe", result)

    def test_strips_style_tags(self):
        html = "<style>.evil{background:url(x)}</style><p>ok</p>"
        result = srv._sanitize_sec_html(html)
        self.assertNotIn("<style>", result)
        self.assertIn("ok", result)

    def test_strips_event_handlers(self):
        html = '<img src="x" onerror="alert(1)"><a onclick="bad()">link</a>'
        result = srv._sanitize_sec_html(html)
        self.assertNotIn("onerror", result)
        self.assertNotIn("onclick", result)

    def test_preserves_content(self):
        html = "<p>Revenue: $1.2B</p><table><tr><td>Net Income</td></tr></table>"
        result = srv._sanitize_sec_html(html)
        self.assertIn("Revenue", result)
        self.assertIn("Net Income", result)


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [TestTickerValidation, TestAccessionValidation, TestBatchValidation,
                TestSecUrlValidation, TestHtmlSanitization]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
