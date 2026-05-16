#!/usr/bin/env python3
"""Phase 6B tests: multi-source company news / event layer."""

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


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


def _parse(raw: str) -> dict:
    return json.loads(raw)


class TestPhase6BCompanyNews(unittest.TestCase):
    def test_get_company_news_structured_shape(self):
        fake_items = [{
            "title": "Macro round-up",
            "source": "Yahoo Finance",
            "sourceType": "yahoo_finance",
            "publishedAt": "2026-05-15T12:30:00Z",
            "retrievedAt": "2026-05-15T13:01:22Z",
            "url": "https://example.com/news",
            "issuer": "Apple Inc.",
            "tickers": ["AAPL"],
            "eventType": "macro",
            "summary": "short neutral summary",
            "evidenceText": "short excerpt only",
            "confidence": "LOW",
            "tickerRelevance": "LOW",
            "duplicateGroupId": "abc123",
        }]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (fake_items, ["yahoo_finance"], [], "2026-05-15T13:01:22Z")
            data = _parse(_run(srv.get_company_news("AAPL")))
            self.assertEqual(data["ticker"], "AAPL")
            self.assertTrue(data["items"])
            item = data["items"][0]
            for field in ("source", "sourceType", "publishedAt", "retrievedAt", "url", "confidence"):
                self.assertIn(field, item)
            self.assertIn(item.get("tickerRelevance"), ("LOW", "UNKNOWN", "HIGH", "MEDIUM"))
            self.assertEqual(data.get("meta", {}).get("sourcesUsed"), ["yahoo_finance"])
            self.assertTrue(data.get("meta", {}).get("deduped"))

    def test_search_company_news_query_required(self):
        payload = _parse(_run(srv.search_company_news("AAPL", "")))
        self.assertTrue(payload.get("error"))
        self.assertEqual(payload.get("code"), "INPUT_VALIDATION_ERROR")

    def test_search_company_news_returns_query(self):
        fake_items = [{
            "title": "China revenue update",
            "source": "SEC",
            "sourceType": "sec_filing",
            "publishedAt": "2026-05-05T13:00:00Z",
            "retrievedAt": "2026-05-05T13:01:00Z",
            "url": "https://www.sec.gov/Archives/example.htm",
            "eventType": "earnings",
            "summary": "China revenue discussion",
            "evidenceText": "China revenue discussion",
            "confidence": "HIGH",
            "tickerRelevance": "HIGH",
            "duplicateGroupId": "g1",
        }]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (fake_items, ["sec"], [], "2026-05-05T13:01:00Z")
            data = _parse(_run(srv.search_company_news("AAPL", "China revenue")))
            self.assertEqual(data.get("query"), "China revenue")
            self.assertEqual(len(data.get("items", [])), 1)
            self.assertNotIn("articleBody", json.dumps(data))

    def test_press_release_no_official_source_warning(self):
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = ([], ["yahoo_finance"], [], "2026-05-05T13:01:00Z")
            data = _parse(_run(srv.get_company_press_releases("AAPL")))
            codes = [w.get("code") for w in data.get("warnings", []) if isinstance(w, dict)]
            self.assertIn("NO_OFFICIAL_RELEASE_SOURCE", codes)


class TestPhase6BSecEvents(unittest.TestCase):
    def test_get_sec_recent_events_accepted_at_used_as_published(self):
        subs = {
            "name": "Apple Inc.",
            "filings": {
                "recent": {
                    "form": ["8-K", "10-Q"],
                    "filingDate": ["2026-05-01", "2026-04-30"],
                    "acceptanceDateTime": ["2026-05-01T20:05:00Z", ""],
                    "accessionNumber": ["0000000000-26-000001", "0000000000-26-000002"],
                    "primaryDocument": ["a.htm", "b.htm"],
                }
            },
        }
        with patch("server._get_submissions_for_ticker", new_callable=AsyncMock) as mocked:
            mocked.return_value = ("0000000000", subs)
            data = _parse(_run(srv.get_sec_recent_events("AAPL", filing_types=["8-K", "10-Q"], max_results=2)))
            self.assertEqual(len(data.get("items", [])), 2)
            first = data["items"][0]
            self.assertEqual(first.get("publishedAt"), "2026-05-01T20:05:00Z")
            self.assertTrue(str(first.get("url", "")).startswith("https://www.sec.gov/Archives/"))
            self.assertIn("accessionNumber", first)
            warnings = [w.get("code") for w in data.get("warnings", []) if isinstance(w, dict)]
            self.assertIn("PUBLISHED_AT_ESTIMATED", warnings)

    def test_timeline_sorted_and_max_results(self):
        fake_items = [
            {"title": "B", "publishedAt": "2026-05-03T00:00:00Z", "source": "SEC", "sourceType": "sec_filing", "url": "https://www.sec.gov/Archives/x", "eventType": "regulatory", "confidence": "HIGH", "duplicateGroupId": "2"},
            {"title": "A", "publishedAt": "2026-05-01T00:00:00Z", "source": "SEC", "sourceType": "sec_filing", "url": "https://www.sec.gov/Archives/y", "eventType": "regulatory", "confidence": "HIGH", "duplicateGroupId": "1"},
        ]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (fake_items, ["sec"], [], "2026-05-03T01:00:00Z")
            data = _parse(_run(srv.get_public_event_timeline("AAPL", max_results=1)))
            self.assertEqual(len(data.get("timeline", [])), 1)
            self.assertEqual(data["timeline"][0]["timestamp"], "2026-05-01T00:00:00Z")


class TestPhase6BVerifyEvent(unittest.TestCase):
    def test_verify_statuses(self):
        confirmed = [{
            "title": "8-K filed",
            "source": "SEC",
            "sourceType": "sec_filing",
            "publishedAt": "2026-05-10T10:00:00Z",
            "retrievedAt": "2026-05-10T10:01:00Z",
            "url": "https://www.sec.gov/Archives/test",
            "confidence": "HIGH",
            "summary": "guidance",
            "evidenceText": "guidance",
            "eventType": "guidance",
        }]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (confirmed, ["sec"], [], "2026-05-10T10:01:00Z")
            data = _parse(_run(srv.verify_company_event("AAPL", "guidance")))
            self.assertEqual(data.get("status"), "CONFIRMED")

        partial = [dict(confirmed[0], source="Yahoo Finance", sourceType="yahoo_finance", confidence="LOW")]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (partial, ["yahoo_finance"], [], "2026-05-10T10:01:00Z")
            data = _parse(_run(srv.verify_company_event("AAPL", "guidance")))
            self.assertEqual(data.get("status"), "PARTIAL")

        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = ([], ["sec"], [], "2026-05-10T10:01:00Z")
            data = _parse(_run(srv.verify_company_event("AAPL", "guidance")))
            self.assertEqual(data.get("status"), "NOT_FOUND")

    def test_verify_stale_and_conflicting(self):
        stale_item = [{
            "title": "Old guidance",
            "source": "SEC",
            "sourceType": "sec_filing",
            "publishedAt": "2020-01-01T00:00:00Z",
            "retrievedAt": "2026-05-10T10:01:00Z",
            "url": "https://www.sec.gov/Archives/test",
            "confidence": "HIGH",
            "summary": "guidance",
            "evidenceText": "guidance",
            "eventType": "guidance",
        }]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (stale_item, ["sec"], [], "2026-05-10T10:01:00Z")
            data = _parse(_run(srv.verify_company_event("AAPL", "guidance", start_date="2026-05-01", end_date="2026-05-15")))
            self.assertEqual(data.get("status"), "STALE")

        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (stale_item, ["sec"], [{"code": "TIMESTAMP_CONFLICT", "message": "x"}], "2026-05-10T10:01:00Z")
            data = _parse(_run(srv.verify_company_event("AAPL", "guidance")))
            self.assertEqual(data.get("status"), "CONFLICTING")


class TestPhase6BPublicWording(unittest.TestCase):
    def test_no_private_terms_in_public_descriptions(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        server_text = open(os.path.join(root, "server.py"), encoding="utf-8").read()
        tools_text = open(os.path.join(root, "worker", "src", "tools.ts"), encoding="utf-8").read()
        private_terms = ("Commander", "portfolio state", "doctrine", "DC-", "TPS", "PCCE")
        names = [
            "get_company_news",
            "search_company_news",
            "get_company_press_releases",
            "get_sec_recent_events",
            "get_public_event_timeline",
            "verify_company_event",
        ]
        for name in names:
            self.assertIn(name, server_text)
            self.assertIn(name, tools_text)
        blob = server_text + "\n" + tools_text
        for term in private_terms:
            self.assertNotIn(term, blob)


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False)
    sys.exit(0 if result.result.wasSuccessful() else 1)
