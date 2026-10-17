#!/usr/bin/env python3
"""Phase 3 tests: Input validation and security hardening.

These are offline/unit tests — no network calls required.
Run: PYTHONPATH=. python scripts/test_phase3.py
"""

import asyncio
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Patch FastMCP.tool to accept output_schema (not in mcp>=1.9)
from yfmcp.app import FastMCP as _FastMCP  # noqa: E402
_orig_tool = _FastMCP.tool
def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
    return _orig_tool(self, name=name, **kwargs)
_FastMCP.tool = _patched_tool  # type: ignore[method-assign]

import server as srv
from yfmcp.clients import edgar


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


    def test_accession_digits_are_ascii_only(self):
        self.assertIsNotNone(srv._validate_accession("٠" * 10 + "-" + "٠" * 2 + "-" + "٠" * 6))


class TestToolCallGate(unittest.TestCase):
    """The tool-call gate (2.12.0, Q3b/Q3c): rejects before any handler code or fetch, in both tool modes."""

    @staticmethod
    def _expanded(name, **args):
        fn = srv.yfinance_server._tool_manager._tools[name].fn
        with patch.object(srv.yf, "Ticker", side_effect=AssertionError("fetched")) as ticker:
            out = json.loads(asyncio.run(fn(**args)))
        return out, ticker

    @staticmethod
    def _grouped(group, action, params):
        from tool_groups import _route_grouped_call
        from yfmcp.app import build_handler_registry

        registry = build_handler_registry(srv.yfinance_server)
        with patch.object(srv.yf, "Ticker", side_effect=AssertionError("fetched")) as ticker:
            out = json.loads(asyncio.run(_route_grouped_call(group, action, params, registry)))
        return out, ticker

    def test_expanded_rejects_bad_symbol_with_raw_echo_before_any_fetch(self):
        out, ticker = self._expanded("get_market_quote", ticker=["AAPL", " brk/b "])
        self.assertEqual(out["error"]["code"], "INPUT_VALIDATION_ERROR")
        self.assertEqual(out["meta"]["tool"], "get_market_quote")
        self.assertIn("' brk/b '", out["error"]["message"])
        ticker.assert_not_called()

    def test_expanded_cap_plus_one_is_batch_limit_exceeded_before_any_fetch(self):
        out, ticker = self._expanded("get_market_quote", ticker=[f"T{i}" for i in range(11)])
        self.assertEqual(out["error"], {"code": "BATCH_LIMIT_EXCEEDED", "message": "Too many tickers: 11. Maximum is 10 per call for get_market_quote."})
        self.assertIs(out["meta"]["retryable"], False)
        ticker.assert_not_called()

    def test_cap_is_checked_before_the_symbol_format(self):
        out, _ = self._expanded("get_market_quote", ticker=["BAD$"] * 11)
        self.assertEqual(out["error"]["code"], "BATCH_LIMIT_EXCEEDED")

    def test_grouped_mode_runs_the_same_gate(self):
        out, ticker = self._grouped("stock_pricing", "get_market_quote", {"ticker": [f"T{i}" for i in range(11)]})
        self.assertEqual(out["error"]["code"], "BATCH_LIMIT_EXCEEDED")
        self.assertEqual(out["meta"]["tool"], "get_market_quote")
        out, ticker = self._grouped("stock_pricing", "get_market_quote", {"ticker": "BRK/B"})
        self.assertEqual(out["error"]["code"], "INPUT_VALIDATION_ERROR")
        ticker.assert_not_called()

    def test_grouped_accession_is_checked_at_the_gate(self):
        out, ticker = self._grouped("sec_filings", "get_sec_filing_index", {"ticker": "AAPL", "accession_number": "000032019324000123"})
        self.assertEqual(out["error"]["code"], "INPUT_VALIDATION_ERROR")
        self.assertIn("Invalid accession number", out["error"]["message"])

    def test_news_with_single_issuer_sources_is_capped_at_one(self):
        out, _ = self._expanded("get_company_news", ticker=["AAPL", "MSFT"], sources=[" NewsWire "])
        self.assertEqual(out["error"]["message"], "Too many tickers: 2. Maximum is 1 per call for get_company_news.")

    def test_snapshot_full_mode_uses_its_own_cap_entry(self):
        from yfmcp.validation import TICKER_CAPS, validate_tool_args

        over = TICKER_CAPS["get_market_snapshot.full"] + 1
        failure = json.loads(validate_tool_args("get_market_snapshot", {"ticker": ["A"] * over, "mode": "full"}))
        self.assertEqual(failure["error"]["code"], "BATCH_LIMIT_EXCEEDED")
        self.assertIsNone(validate_tool_args("get_market_snapshot", {"ticker": ["A"] * TICKER_CAPS["get_market_snapshot.full"], "mode": "full"}))

    def test_blank_and_non_string_values_are_left_to_the_handler(self):
        from yfmcp.validation import validate_tool_args

        for args in ({"ticker": ""}, {"ticker": "  "}, {"ticker": ["AAPL", ""]}, {"ticker": 123}, {"ticker": None}, {}):
            self.assertIsNone(validate_tool_args("get_market_quote", args), args)
        self.assertIsNone(validate_tool_args("get_sec_filing_index", {"ticker": "AAPL", "accession_number": ""}))

    def test_listing_advertises_ticker_caps_as_max_items(self):
        from yfmcp.validation import TICKER_CAPS

        tools = {t.name: t for t in asyncio.run(srv.yfinance_server.list_tools())}
        for key, cap in TICKER_CAPS.items():
            if "." in key:
                continue
            schema = getattr(tools[key], "input_schema", None) or getattr(tools[key], "inputSchema", None)
            branches = schema["properties"]["ticker"]["anyOf"]
            self.assertEqual(next(b for b in branches if b.get("type") == "array")["maxItems"], cap, key)

    def test_grouped_shape_checker_enforces_array_length(self):
        from tool_groups import _matches_annotation

        self.assertTrue(_matches_annotation(["A"] * 5, str | list[str], 5))
        self.assertFalse(_matches_annotation(["A"] * 6, str | list[str], 5))
        self.assertTrue(_matches_annotation("A", str | list[str], 5))


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


class TestSecCikResolutionFallbacks(unittest.TestCase):
    """The ticker -> CIK fixtures are empty in production (2.13.0, R4 Q4); tests inject entries with patch.dict."""

    FIXTURES = json.loads((Path(__file__).parent / "parity" / "fixtures" / "ticker_cik_fixtures.json").read_text(encoding="utf-8"))

    def _resolve(self, ticker, fixtures):
        with patch("server.yf.Ticker") as mock_ticker, patch("server._load_edgar_tickers", new_callable=AsyncMock) as mock_load, \
                patch.dict(edgar._FILING_CIK_CACHE, {}, clear=True), patch.dict(edgar._TEST_TICKER_CIK_FIXTURES, fixtures, clear=True):
            mock_ticker.return_value.info = {}
            mock_load.return_value = {}
            return asyncio.run(srv._resolve_cik_for_ticker(ticker))

    def test_fixture_cik_applies_without_network_sources(self):
        self.assertEqual(self._resolve("AAPL", self.FIXTURES), "0000320193")

    def test_fixture_cik_handles_lowercase(self):
        self.assertEqual(self._resolve("axti", self.FIXTURES), "0001051627")

    def test_fixtures_are_empty_in_production(self):
        self.assertEqual(edgar._TEST_TICKER_CIK_FIXTURES, {})


class TestSecDocumentUrlGuard(unittest.TestCase):
    """search_sec_filing_text only reads caller-supplied SEC Archives documents."""

    def _search(self, **kwargs) -> dict:
        import json
        with patch.object(srv, "_edgar_get_html", new_callable=AsyncMock) as fetch, \
                patch.object(srv, "_get_submissions_for_ticker", new_callable=AsyncMock) as subs:
            subs.return_value = (None, None)
            fetch.return_value = "<html><body>Revenue in China grew.</body></html>"
            result = json.loads(asyncio.run(srv.search_sec_filing_text(ticker="AAPL", search_query="China", **kwargs)))
        return {"result": result, "fetched": [c.args[0] for c in fetch.call_args_list], "resolved": subs.await_count}

    def test_non_sec_url_is_rejected_without_fetching(self):
        out = self._search(document_url="https://attacker.example/filing.htm")
        self.assertEqual(out["fetched"], [])
        self.assertIsNone(out["result"]["documentUrl"])
        self.assertIn("sec.gov/Archives", out["result"]["_note"])

    def test_sec_archives_url_is_read_directly(self):
        url = "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/aapl-20250927.htm"
        out = self._search(document_url=url)
        self.assertEqual(out["fetched"], [url])
        self.assertEqual(out["resolved"], 0)
        self.assertEqual(out["result"]["documentUrl"], url)
        self.assertEqual(out["result"]["matchCount"], 1)

    def test_xbrl_url_without_accession_is_not_available(self):
        out = self._search(document_url="https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/aapl-20250927_htm.xml")
        self.assertEqual(out["fetched"], [])
        self.assertEqual(out["result"]["status"], "FILING_TEXT_NOT_AVAILABLE")


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [TestTickerValidation, TestAccessionValidation, TestToolCallGate,
                TestSecUrlValidation, TestHtmlSanitization, TestSecCikResolutionFallbacks]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
