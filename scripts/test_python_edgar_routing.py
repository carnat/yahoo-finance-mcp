#!/usr/bin/env python3
"""The Python EDGAR routing release (2.26.0; PHASE4_SPEC.md §3.6, decisions D2b, D2c, D9; F_TRIAGE.md F7).

- The SEC filing tools (list_sec_filings and the document reads of get_filing_outline, get_filing_section,
  list_filing_tables, get_filing_table) go through yfmcp/clients/edgar.py: SEC's User-Agent, the Worker's full CIK
  resolution, the 15-minute submissions cache and the archive document cache.
- Document reads keep the HTTP status and have no size cap; messages are unchanged where the status is unchanged.
- F7: a failed ticker-index load is not cached; the next call retries.
- D9: parse_public_transcript fetches SEC pages with SEC's User-Agent.

No network: urllib is stubbed.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import sys
import unittest
import urllib.error
from email.message import Message
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import server  # noqa: E402
from source_text import definition_source, python_runtime_source  # noqa: E402
from yfmcp import url_policy  # noqa: E402
from yfmcp.clients import edgar  # noqa: E402

CIK = "0000320193"
DOC = "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBS_URL = f"https://data.sec.gov/submissions/CIK{CIK}.json"
SUBMISSIONS = {"filings": {"recent": {
    "form": ["10-Q", "10-K"],
    "accessionNumber": ["0000320193-25-000073", "0000320193-24-000123"],
    "primaryDocument": ["aapl-20250628.htm", "aapl-20240928.htm"],
    "filingDate": ["2025-08-01", "2024-11-01"],
}}}


class _Resp(io.BytesIO):
    def __init__(self, body: bytes, status: int = 200):
        super().__init__(body)
        self.status = status

    def getcode(self) -> int:
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Net:
    """A urlopen stand-in: serves routes, records (url, User-Agent, timeout) for every request."""

    def __init__(self, routes: dict):
        self.routes = routes
        self.calls: list[tuple[str, str | None, object]] = []

    def __call__(self, req, timeout=None):
        url = req if isinstance(req, str) else req.full_url
        ua = None if isinstance(req, str) else req.get_header("User-agent")
        self.calls.append((url, ua, timeout))
        route = self.routes.get(url)
        if route is None:
            raise urllib.error.URLError("unrouted")
        if isinstance(route, Exception):
            raise route
        status, body = route
        if status >= 400:
            raise urllib.error.HTTPError(url, status, "Service Unavailable" if status == 503 else "Not Found",
                                         Message(), io.BytesIO(b""))
        return _Resp(body if isinstance(body, bytes) else json.dumps(body).encode())

    def urls(self) -> list[str]:
        return [c[0] for c in self.calls]


class _NoYahoo:
    def __init__(self, *_a, **_k):
        pass

    @property
    def info(self):
        raise RuntimeError("no yahoo")


def _reset_state() -> None:
    edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = None, 0.0
    edgar._FILING_CIK_CACHE.clear()
    edgar._EDGAR_SUBS_CACHE.clear()
    with edgar._EDGAR_HTML_LOCK:
        edgar._EDGAR_HTML_CACHE.clear()
        edgar._EDGAR_HTML_INFLIGHT.clear()
        edgar._EDGAR_HTML_CACHE_CHARS = 0


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        _reset_state()
        self.addCleanup(_reset_state)
        p = patch.object(edgar.yf, "Ticker", _NoYahoo)
        p.start()
        self.addCleanup(p.stop)

    def net(self, routes: dict) -> _Net:
        net = _Net(routes)
        p = patch.object(url_policy, "urlopen", net)
        p.start()
        self.addCleanup(p.stop)
        return net


class TestTickerIndexF7(_Base):
    def test_failed_load_is_not_cached_and_the_next_call_retries(self) -> None:
        net = self.net({TICKERS_URL: urllib.error.URLError("down")})
        self.assertEqual(asyncio.run(edgar._load_edgar_tickers()), {})
        self.assertFalse(edgar._edgar_ticker_map_loaded())
        net.routes[TICKERS_URL] = (200, {"0": {"cik_str": 320193, "ticker": "AAPL"}})
        self.assertEqual(asyncio.run(edgar._load_edgar_tickers()), {"AAPL": 320193})
        self.assertEqual(net.urls().count(TICKERS_URL), 2)
        self.assertTrue(edgar._edgar_ticker_map_loaded())
        # A loaded map is served from cache.
        asyncio.run(edgar._load_edgar_tickers())
        self.assertEqual(net.urls().count(TICKERS_URL), 2)

    def test_failed_refresh_after_expiry_returns_empty_as_the_worker(self) -> None:
        net = self.net({TICKERS_URL: (200, {"0": {"cik_str": 320193, "ticker": "AAPL"}})})
        asyncio.run(edgar._load_edgar_tickers())
        edgar._EDGAR_TICKERS_LOADED_AT -= edgar._EDGAR_TTL + 1
        net.routes[TICKERS_URL] = urllib.error.URLError("down")
        self.assertEqual(asyncio.run(edgar._load_edgar_tickers()), {})
        self.assertFalse(edgar._edgar_ticker_map_loaded())


class TestListSecFilings(_Base):
    def test_uses_the_sec_user_agent_and_the_client_caches(self) -> None:
        net = self.net({TICKERS_URL: (200, {"0": {"cik_str": 320193, "ticker": "AAPL"}}), SUBS_URL: (200, SUBMISSIONS)})
        first = json.loads(asyncio.run(server.list_sec_filings("AAPL", "10-K", 5)))
        self.assertEqual(first["filings"][0]["primaryDocumentUrl"], DOC)
        self.assertEqual(first["filings"][0]["edgarIndexUrl"],
                         "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/0000320193-24-000123-index.htm")
        asyncio.run(server.list_sec_filings("AAPL", "10-Q", 5))
        # One ticker-index read and one submissions read for both calls; every SEC request carries SEC's User-Agent.
        self.assertEqual(net.urls(), [TICKERS_URL, SUBS_URL])
        self.assertEqual({ua for _, ua, _ in net.calls}, {edgar._SEC_REQUIRED_UA})

    def test_submissions_expire_after_15_minutes(self) -> None:
        net = self.net({TICKERS_URL: (200, {"0": {"cik_str": 320193, "ticker": "AAPL"}}), SUBS_URL: (200, SUBMISSIONS)})
        asyncio.run(server.list_sec_filings("AAPL"))
        data, stored = edgar._EDGAR_SUBS_CACHE[CIK]
        edgar._EDGAR_SUBS_CACHE[CIK] = (data, stored - 15 * 60 - 1)
        asyncio.run(server.list_sec_filings("AAPL"))
        self.assertEqual(net.urls().count(SUBS_URL), 2)

    def test_cik_miss_keeps_its_message(self) -> None:
        self.net({TICKERS_URL: (200, {"0": {"cik_str": 320193, "ticker": "AAPL"}})})
        with patch.object(edgar, "_TEST_TICKER_CIK_FIXTURES", {}):
            out = json.loads(asyncio.run(server.list_sec_filings("ZZZZ")))
        self.assertEqual(out["error"]["code"], "TICKER_NOT_FOUND")
        self.assertEqual(out["error"]["message"], "Could not find EDGAR CIK for ticker 'ZZZZ'")

    def test_failed_submissions_read_keeps_urllibs_message(self) -> None:
        self.net({TICKERS_URL: (200, {"0": {"cik_str": 320193, "ticker": "AAPL"}}), SUBS_URL: (503, b"")})
        out = json.loads(asyncio.run(server.list_sec_filings("AAPL")))
        self.assertEqual(out["error"]["code"], "PROVIDER_ERROR")
        self.assertEqual(out["error"]["message"], "HTTP Error 503: Service Unavailable")

    def test_full_resolution_finds_a_cik_the_index_lacks(self) -> None:
        """D2b: full client resolution (the Worker's), so the test fixtures resolve a ticker missing from the index."""
        self.net({TICKERS_URL: (200, {"0": {"cik_str": 320193, "ticker": "AAPL"}}),
                  "https://data.sec.gov/submissions/CIK0000999001.json": (200, {"filings": {"recent": {}}})})
        with patch.dict(edgar._TEST_TICKER_CIK_FIXTURES, {"NEWCO": "0000999001"}):
            out = json.loads(asyncio.run(server.list_sec_filings("NEWCO")))
        self.assertEqual(out, {"ticker": "NEWCO", "formType": "10-K", "filings": []})


HTML = (b"<html><body><h2>Item 1. Business</h2><p>Revenue grew.</p>"
        b"<table><tr><td>Revenue</td><td>100</td></tr></table></body></html>")


class TestDocumentReads(_Base):
    def test_every_document_tool_reads_through_the_client_with_the_sec_user_agent(self) -> None:
        net = self.net({DOC: (200, HTML)})
        calls = [
            server.get_filing_outline("AAPL", None, DOC),
            server.get_filing_section("AAPL", "Item 1", DOC),
            server.list_filing_tables("AAPL", DOC),
            server.get_filing_table("AAPL", DOC, 0),
        ]
        for coro in calls:
            out = json.loads(asyncio.run(coro))
            self.assertNotEqual(out.get("ok"), False, out)
        # One SEC request for four tools (the archive cache), with SEC's User-Agent and the 15 s timeout.
        self.assertEqual(net.calls, [(DOC, edgar._SEC_REQUIRED_UA, 15)])

    def test_no_size_cap(self) -> None:
        big = b"<html><body>" + b"x" * 6_000_000 + b"<table><tr><td>a</td><td>b</td></tr></table></body></html>"
        self.net({DOC: (200, big)})
        out = json.loads(asyncio.run(server.list_filing_tables("AAPL", DOC)))
        self.assertEqual(out["usableTableCount"], 1)

    def test_invalid_utf8_is_replaced_as_before(self) -> None:
        self.net({DOC: (200, b"<html><h2>Item 1. Caf\xe9</h2></html>")})
        out = json.loads(asyncio.run(server.get_filing_outline("AAPL", None, DOC)))
        self.assertEqual(out["outline"][0]["title"], "Item 1. Caf�")

    def test_failures_keep_their_status_and_message_and_are_not_cached(self) -> None:
        net = self.net({DOC: (404, b"")})
        outline = json.loads(asyncio.run(server.get_filing_outline("AAPL", None, DOC)))
        self.assertEqual(outline["error"], {"code": "PROVIDER_ERROR", "message": "HTTP 404"})
        for coro in (server.get_filing_section("AAPL", "Item 1", DOC), server.list_filing_tables("AAPL", DOC),
                     server.get_filing_table("AAPL", DOC, 0)):
            out = json.loads(asyncio.run(coro))
            self.assertEqual(out["error"]["code"], "PROVIDER_ERROR")
            self.assertEqual(out["error"]["message"], "HTTP Error 404: Not Found")
        self.assertEqual(net.urls().count(DOC), 4)

    def test_document_cache_is_separate_from_capped_reads(self) -> None:
        self.net({DOC: (200, HTML)})
        asyncio.run(server.get_filing_outline("AAPL", None, DOC))
        self.assertIn((DOC, edgar._EDGAR_DOCUMENT_KEY), edgar._EDGAR_HTML_CACHE)
        self.assertNotIn((DOC, 5_000_000), edgar._EDGAR_HTML_CACHE)


class TestPublicTranscriptD9(_Base):
    def test_sec_page_uses_the_sec_user_agent(self) -> None:
        net = self.net({DOC: (200, b"<p>Operator: welcome.</p>")})
        out = json.loads(asyncio.run(server.parse_public_transcript(DOC)))
        self.assertNotEqual(out.get("ok"), False, out)
        self.assertEqual(net.calls[-1][1], edgar._SEC_REQUIRED_UA)
        self.assertEqual(net.calls[-1][2], 30)

    def test_sec_failure_details_are_unchanged(self) -> None:
        self.net({DOC: (503, b"")})
        out = json.loads(asyncio.run(server.parse_public_transcript(DOC)))
        self.assertEqual(out["error"]["code"], "FETCH_SERVER_ERROR")
        self.assertEqual(out["error"]["message"], "HTTP 503: Service Unavailable")


class TestNoSecFetchOutsideTheClient(unittest.TestCase):
    def test_no_runtime_module_sends_the_old_user_agent(self) -> None:
        self.assertNotIn("admin@example.com", python_runtime_source())

    def test_routed_functions_build_no_raw_request(self) -> None:
        for name in ("list_sec_filings", "get_filing_outline", "get_filing_section", "list_filing_tables",
                     "get_filing_table", "parse_public_transcript"):
            body = definition_source("python", name)
            self.assertNotIn("sec.gov/files", body, name)
            self.assertNotIn("data.sec.gov", body, name)
        self.assertIn("_edgar_fetch_page_detail", definition_source("python", "parse_public_transcript"))
        for name in ("list_sec_filings", "get_filing_outline", "get_filing_section", "list_filing_tables",
                     "get_filing_table"):
            self.assertNotIn("Request(", definition_source("python", name), name)


if __name__ == "__main__":
    unittest.main()
