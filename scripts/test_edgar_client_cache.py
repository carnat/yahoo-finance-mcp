#!/usr/bin/env python3
"""In-process EDGAR client caches: expiry, size limits, and shared fetches.

No network: the HTTP helpers are patched.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server  # noqa: E402,F401  (registers tools; mirrors the other suites)
from yfmcp.clients import edgar  # noqa: E402

ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/aapl-20250927.htm"


def _clear_html_cache() -> None:
    with edgar._EDGAR_HTML_LOCK:
        edgar._EDGAR_HTML_CACHE.clear()
        edgar._EDGAR_HTML_INFLIGHT.clear()
        edgar._EDGAR_HTML_CACHE_CHARS = 0


class TestSubmissionsExpiry(unittest.TestCase):
    def setUp(self) -> None:
        edgar._EDGAR_SUBS_CACHE.clear()
        edgar._FILING_CIK_CACHE.clear()
        edgar._FILING_CIK_CACHE["AAPL"] = "0000320193"

    def test_submissions_are_cached_then_refetched_after_24h(self) -> None:
        with patch.object(edgar, "_edgar_get", new_callable=AsyncMock) as fetch:
            fetch.side_effect = [{"filings": {"version": 1}}, {"filings": {"version": 2}}]
            cik, first = asyncio.run(edgar._get_submissions_for_ticker("aapl"))
            _, again = asyncio.run(edgar._get_submissions_for_ticker("AAPL"))
            self.assertEqual(cik, "0000320193")
            self.assertEqual(fetch.await_count, 1)
            self.assertIs(first, again)

            data, stored_at = edgar._EDGAR_SUBS_CACHE["0000320193"]
            edgar._EDGAR_SUBS_CACHE["0000320193"] = (data, stored_at - edgar._EDGAR_TTL - 1)
            _, refreshed = asyncio.run(edgar._get_submissions_for_ticker("AAPL"))
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual(refreshed, {"filings": {"version": 2}})


class TestBoundedCaches(unittest.TestCase):
    def test_bounded_set_evicts_oldest_and_refreshes_order(self) -> None:
        cache: dict[str, int] = {}
        for key in ("a", "b", "c"):
            edgar._bounded_set(cache, key, 1, 3)
        edgar._bounded_set(cache, "a", 2, 3)
        edgar._bounded_set(cache, "d", 1, 3)
        self.assertEqual(list(cache), ["c", "a", "d"])

    def test_company_facts_cache_is_limited(self) -> None:
        edgar._EDGAR_FACTS_CACHE.clear()
        with patch.object(edgar, "_edgar_get", new_callable=AsyncMock, return_value={"facts": {}}):
            for i in range(edgar._EDGAR_FACTS_CACHE_MAX + 5):
                asyncio.run(edgar._edgar_get_company_facts(f"{i:010d}"))
        self.assertEqual(len(edgar._EDGAR_FACTS_CACHE), edgar._EDGAR_FACTS_CACHE_MAX)


class TestEdgarHtmlCache(unittest.TestCase):
    def setUp(self) -> None:
        _clear_html_cache()
        self.calls = 0
        self.lock = threading.Lock()

    def tearDown(self) -> None:
        _clear_html_cache()

    def _slow_fetch(self, result: str | None = "<html>filing</html>"):
        def _fetch(url: str, max_bytes: int) -> str | None:
            with self.lock:
                self.calls += 1
            time.sleep(0.2)
            return result
        return _fetch

    def test_concurrent_reads_in_one_call_share_one_fetch(self) -> None:
        async def _five_reads():
            return await asyncio.gather(*(edgar._edgar_get_html(ARCHIVE_URL) for _ in range(5)))

        with patch.object(edgar, "_fetch_edgar_html", side_effect=self._slow_fetch()):
            results = asyncio.run(_five_reads())
            again = asyncio.run(edgar._edgar_get_html(ARCHIVE_URL))
        self.assertEqual(self.calls, 1)
        self.assertEqual(set(results), {"<html>filing</html>"})
        self.assertEqual(again, "<html>filing</html>")

    def test_reads_from_separate_tool_calls_share_one_fetch(self) -> None:
        # Each MCP tool call runs asyncio.run on its own thread.
        results: list[str | None] = []

        def _tool_call() -> None:
            results.append(asyncio.run(edgar._edgar_get_html(ARCHIVE_URL)))

        with patch.object(edgar, "_fetch_edgar_html", side_effect=self._slow_fetch()):
            threads = [threading.Thread(target=_tool_call) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual(self.calls, 1)
        self.assertEqual(results, ["<html>filing</html>"] * 4)

    def test_failed_fetches_are_not_cached(self) -> None:
        with patch.object(edgar, "_fetch_edgar_html", side_effect=self._slow_fetch(None)):
            self.assertIsNone(asyncio.run(edgar._edgar_get_html(ARCHIVE_URL)))
            self.assertIsNone(asyncio.run(edgar._edgar_get_html(ARCHIVE_URL)))
        self.assertEqual(self.calls, 2)

    def test_non_archive_urls_are_not_cached(self) -> None:
        url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
        with patch.object(edgar, "_fetch_edgar_html", side_effect=self._slow_fetch()):
            asyncio.run(edgar._edgar_get_html(url))
            asyncio.run(edgar._edgar_get_html(url))
        self.assertEqual(self.calls, 2)

    def test_cache_is_limited_by_total_size(self) -> None:
        with patch.object(edgar, "_EDGAR_HTML_CACHE_MAX_CHARS", 25), \
             patch.object(edgar, "_fetch_edgar_html", side_effect=lambda url, max_bytes: "x" * 10):
            for i in range(4):
                asyncio.run(edgar._edgar_get_html(f"{ARCHIVE_URL}?doc={i}"))
        self.assertEqual(len(edgar._EDGAR_HTML_CACHE), 2)
        self.assertEqual(edgar._EDGAR_HTML_CACHE_CHARS, 20)

    def test_entries_expire(self) -> None:
        with patch.object(edgar, "_fetch_edgar_html", side_effect=self._slow_fetch()):
            asyncio.run(edgar._edgar_get_html(ARCHIVE_URL))
            key = (ARCHIVE_URL, 5_000_000)
            text, stored_at = edgar._EDGAR_HTML_CACHE[key]
            edgar._EDGAR_HTML_CACHE[key] = (text, stored_at - edgar._EDGAR_HTML_TTL - 1)
            asyncio.run(edgar._edgar_get_html(ARCHIVE_URL))
        self.assertEqual(self.calls, 2)


class TestFinnhubPolicy(unittest.TestCase):
    def test_policy_file_is_read_once(self) -> None:
        server._finnhub_policy.cache_clear()
        for ticker in ("AAPL", "SIVE.ST", "MSFT"):
            server._finnhub_eligibility(ticker)
        info = server._finnhub_policy.cache_info()
        self.assertEqual(info.misses, 1)
        self.assertEqual(info.hits, 2)


class TestSecUserAgent(unittest.TestCase):
    def test_contact_comes_from_environment(self) -> None:
        with patch.dict(os.environ, {"EDGAR_CONTACT_EMAIL": " ops@example.org "}):
            self.assertEqual(edgar._sec_user_agent(), "yahoo-finance-mcp ops@example.org")
        with patch.dict(os.environ, {"EDGAR_CONTACT_EMAIL": ""}):
            self.assertEqual(edgar._sec_user_agent(), "yahoo-finance-mcp contact@example.com")

    def test_non_sec_providers_do_not_receive_the_contact(self) -> None:
        import inspect

        self.assertNotIn("@", server._PROVIDER_USER_AGENT)
        for fn in (server._collect_globenewswire_events, server._collect_finnhub_events, server._collect_marketaux_events):
            with self.subTest(fn=fn.__name__):
                source = inspect.getsource(fn)
                self.assertNotIn("_SEC_REQUIRED_UA", source)
                self.assertIn("_PROVIDER_USER_AGENT", source)


if __name__ == "__main__":
    unittest.main()
