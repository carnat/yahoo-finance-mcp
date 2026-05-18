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

    def test_get_company_news_finnhub_source_status_unconfigured(self):
        finnhub_warning = [{
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub company-news source is not configured; skipped.",
            "severity": "warning",
        }]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = ([], [], finnhub_warning, "2026-05-15T13:01:22Z")
            data = _parse(_run(srv.get_company_news("AAPL", sources=["finnhub"])))
            self.assertEqual(data.get("sourceStatus", {}).get("finnhub", {}).get("status"), "UNCONFIGURED")

    def test_get_company_news_finnhub_source_status_ok(self):
        finnhub_items = [{
            "title": "Apple announces update",
            "source": "finnhub",
            "originalSource": "Reuters",
            "sourceType": "company_news",
            "publishedAt": "2026-05-15T12:30:00Z",
            "retrievedAt": "2026-05-15T13:01:22Z",
            "url": "https://example.com/finnhub-news",
            "tickers": ["AAPL"],
            "eventType": "product",
            "summary": "summary",
            "evidenceText": "summary",
            "confidence": "MEDIUM",
            "tickerRelevance": "HIGH",
            "duplicateGroupId": "fh1",
        }]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (finnhub_items, ["finnhub"], [], "2026-05-15T13:01:22Z")
            data = _parse(_run(srv.get_company_news("AAPL", sources=["finnhub"])))
            self.assertEqual(data.get("sourceStatus", {}).get("finnhub", {}).get("status"), "OK")

    def test_get_company_news_default_sources_yahoo_and_finnhub_only(self):
        """Regression: default sources must be yahoo_finance + finnhub only, not sec/company_ir/newswire."""
        yf_items = [
            {
                "title": "Apple reports record sales",
                "source": "Yahoo Finance",
                "sourceType": "yahoo_finance",
                "publishedAt": "2026-05-15T12:30:00Z",
                "retrievedAt": "2026-05-15T13:01:22Z",
                "url": "https://finance.yahoo.com/news/apple-record-sales",
                "tickers": ["AAPL"],
                "eventType": "earnings",
                "summary": "Apple record sales summary",
                "evidenceText": "record sales",
                "confidence": "MEDIUM",
                "tickerRelevance": "HIGH",
                "duplicateGroupId": "yf1",
            },
            {
                "title": "Apple launches new iPhone",
                "source": "Yahoo Finance",
                "sourceType": "yahoo_finance",
                "publishedAt": "2026-05-14T10:00:00Z",
                "retrievedAt": "2026-05-14T10:01:00Z",
                "url": "https://finance.yahoo.com/news/apple-new-iphone",
                "tickers": ["AAPL"],
                "eventType": "product",
                "summary": "new iPhone launched",
                "evidenceText": "new iPhone",
                "confidence": "MEDIUM",
                "tickerRelevance": "HIGH",
                "duplicateGroupId": "yf2",
            },
        ]
        finnhub_unconfigured_warning = [{
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub company-news source is not configured; skipped.",
            "severity": "warning",
        }]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (yf_items, ["yahoo_finance"], finnhub_unconfigured_warning, "2026-05-15T13:01:22Z")
            data = _parse(_run(srv.get_company_news("AAPL")))
            # Must return 2 items from Yahoo even though Finnhub is absent
            self.assertEqual(len(data.get("items", [])), 2)
            # sourceStatus must include yahoo_finance and finnhub only
            source_status = data.get("sourceStatus", {})
            self.assertIn("yahoo_finance", source_status)
            self.assertIn("finnhub", source_status)
            # sec, company_ir, newswire must NOT appear in default sourceStatus
            self.assertNotIn("sec", source_status)
            self.assertNotIn("company_ir", source_status)
            self.assertNotIn("newswire", source_status)
            # Yahoo returned items so its status is OK; Finnhub is UNCONFIGURED
            self.assertEqual(source_status.get("yahoo_finance", {}).get("status"), "OK")
            self.assertEqual(source_status.get("finnhub", {}).get("status"), "UNCONFIGURED")
            # Top-level status: items present + SOURCE_UNAVAILABLE warning → PARTIAL (not SOURCE_LIMITED_NOT_FOUND)
            self.assertIn(data.get("status"), ("PARTIAL", None))

    def test_get_yahoo_finance_news_returns_yahoo_items_when_finnhub_absent(self):
        """Regression: get_yahoo_finance_news must return Yahoo items even when Finnhub key is absent."""
        yf_items = [
            {
                "title": "Apple hits all-time high",
                "source": "Yahoo Finance",
                "sourceType": "yahoo_finance",
                "publishedAt": "2026-05-16T08:00:00Z",
                "retrievedAt": "2026-05-16T08:01:00Z",
                "url": "https://finance.yahoo.com/news/apple-ath",
                "tickers": ["AAPL"],
                "eventType": "analyst",
                "summary": "analyst upgrade",
                "evidenceText": "all-time high",
                "confidence": "MEDIUM",
                "tickerRelevance": "HIGH",
                "duplicateGroupId": "yf10",
            },
            {
                "title": "Apple dividend increase",
                "source": "Yahoo Finance",
                "sourceType": "yahoo_finance",
                "publishedAt": "2026-05-15T09:00:00Z",
                "retrievedAt": "2026-05-15T09:01:00Z",
                "url": "https://finance.yahoo.com/news/apple-dividend",
                "tickers": ["AAPL"],
                "eventType": "financing",
                "summary": "dividend raise",
                "evidenceText": "dividend increase",
                "confidence": "MEDIUM",
                "tickerRelevance": "HIGH",
                "duplicateGroupId": "yf11",
            },
        ]
        finnhub_unconfigured_warning = [{
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub company-news source is not configured; skipped.",
            "severity": "warning",
        }]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (yf_items, ["yahoo_finance"], finnhub_unconfigured_warning, "2026-05-16T08:01:00Z")
            data = _parse(_run(srv.get_yahoo_finance_news("AAPL")))
            # Must return 2 Yahoo items even with no Finnhub key
            self.assertEqual(len(data.get("items", [])), 2)
            source_status = data.get("sourceStatus", {})
            # sec/company_ir/newswire must not appear
            self.assertNotIn("sec", source_status)
            self.assertNotIn("company_ir", source_status)
            self.assertNotIn("newswire", source_status)
            # _collect_company_events must have been called with sources containing only news sources
            call_kwargs = mocked.call_args
            called_sources = call_kwargs.kwargs.get("sources") or (call_kwargs.args[1] if len(call_kwargs.args) > 1 else None)
            if called_sources is not None:
                self.assertNotIn("sec", called_sources)
                self.assertNotIn("company_ir", called_sources)
                self.assertNotIn("newswire", called_sources)
                self.assertIn("yahoo_finance", called_sources)

    def test_yahoo_newswire_items_included_when_yahoo_finance_selected(self):
        """Regression: newswire-subtype items from Yahoo feed must be returned when yahoo_finance is selected."""
        # Simulate what _collect_company_events returns after the fix:
        # items from Yahoo Finance feed that were sub-classified as newswire/company_ir
        yf_feed_items = [
            {
                "title": "Apple Q2 Results Press Release",
                "source": "Business Wire",
                "sourceType": "newswire",
                "publishedAt": "2026-05-16T16:00:00Z",
                "retrievedAt": "2026-05-16T16:01:00Z",
                "url": "https://www.businesswire.com/news/apple-q2",
                "tickers": ["AAPL"],
                "eventType": "earnings",
                "summary": "Apple reports Q2 results",
                "evidenceText": "Q2 results",
                "confidence": "MEDIUM",
                "tickerRelevance": "HIGH",
                "duplicateGroupId": "nw1",
            },
            {
                "title": "Apple IR: Share Buyback Program",
                "source": "Apple Investor Relations",
                "sourceType": "company_ir",
                "publishedAt": "2026-05-15T10:00:00Z",
                "retrievedAt": "2026-05-15T10:01:00Z",
                "url": "https://investor.apple.com/news/detail",
                "tickers": ["AAPL"],
                "eventType": "financing",
                "summary": "buyback",
                "evidenceText": "buyback",
                "confidence": "MEDIUM",
                "tickerRelevance": "HIGH",
                "duplicateGroupId": "ir1",
            },
            {
                "title": "AAPL analyst upgrade",
                "source": "Yahoo Finance",
                "sourceType": "yahoo_finance",
                "publishedAt": "2026-05-14T08:00:00Z",
                "retrievedAt": "2026-05-14T08:01:00Z",
                "url": "https://finance.yahoo.com/news/aapl-upgrade",
                "tickers": ["AAPL"],
                "eventType": "analyst",
                "summary": "upgrade",
                "evidenceText": "upgrade",
                "confidence": "LOW",
                "tickerRelevance": "HIGH",
                "duplicateGroupId": "yf3",
            },
        ]
        finnhub_warning = [{
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub company-news source is not configured; skipped.",
            "severity": "warning",
        }]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (yf_feed_items, ["yahoo_finance"], finnhub_warning, "2026-05-16T16:01:00Z")
            data = _parse(_run(srv.get_company_news("AAPL")))
            # All 3 items (including newswire + company_ir sub-types) must appear
            self.assertEqual(len(data.get("items", [])), 3)
            # sourceStatus.yahoo_finance must report OK with rawCount=3
            yf_status = data.get("sourceStatus", {}).get("yahoo_finance", {})
            self.assertEqual(yf_status.get("status"), "OK")
            self.assertEqual(yf_status.get("rawCount"), 3)
            # sec/company_ir/newswire must NOT appear as separate top-level status keys
            source_status = data.get("sourceStatus", {})
            self.assertNotIn("sec", source_status)
            self.assertNotIn("company_ir", source_status)
            self.assertNotIn("newswire", source_status)

    def test_collect_company_events_includes_newswire_when_yahoo_selected(self):
        """Unit test: _collect_company_events includes newswire items from Yahoo feed when yahoo_finance is selected."""
        import datetime

        newswire_item = {
            "title": "Tesla raises guidance",
            "source": "PR Newswire",
            "sourceType": "newswire",
            "publishedAt": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "retrievedAt": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": "https://www.prnewswire.com/news/tesla-guidance",
            "tickers": ["TSLA"],
            "eventType": "guidance",
            "summary": "guidance raise",
            "evidenceText": "guidance raise",
            "confidence": "MEDIUM",
            "tickerRelevance": "HIGH",
            "duplicateGroupId": "nw2",
        }
        company_ir_item = {
            "title": "Tesla investor update",
            "source": "Tesla IR",
            "sourceType": "company_ir",
            "publishedAt": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "retrievedAt": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": "https://investor.tesla.com/update",
            "tickers": ["TSLA"],
            "eventType": "other",
            "summary": "investor update",
            "evidenceText": "investor update",
            "confidence": "MEDIUM",
            "tickerRelevance": "HIGH",
            "duplicateGroupId": "ir2",
        }

        with patch("server._collect_yahoo_events", new_callable=AsyncMock) as mock_yf, \
             patch("server._collect_finnhub_events", new_callable=AsyncMock) as mock_fh:
            mock_yf.return_value = ([newswire_item, company_ir_item], [], True)
            mock_fh.return_value = ([], [{"code": "SOURCE_UNAVAILABLE", "message": "Finnhub company-news source is not configured; skipped.", "severity": "warning"}], False)
            items, sources_used, warnings, _ = _run(
                srv._collect_company_events("TSLA", max_results=10, lookback_days=14, sources=["yahoo_finance", "finnhub"])
            )
            # Both newswire and company_ir items from Yahoo feed must be included
            self.assertEqual(len(items), 2)
            source_types = {it["sourceType"] for it in items}
            self.assertIn("newswire", source_types)
            self.assertIn("company_ir", source_types)

    def test_compute_source_status_counts_all_yahoo_feed_types(self):
        """Unit test: _compute_source_status counts newswire/company_ir items toward yahoo_finance when not separately selected."""
        items = [
            {"sourceType": "newswire", "source": "Business Wire"},
            {"sourceType": "company_ir", "source": "Apple IR"},
            {"sourceType": "yahoo_finance", "source": "Yahoo Finance"},
        ]
        # Only yahoo_finance and finnhub are selected (default path)
        status = srv._compute_source_status(
            sources_used=["yahoo_finance"],
            warnings=[{"code": "SOURCE_UNAVAILABLE", "message": "Finnhub company-news source is not configured; skipped.", "severity": "warning"}],
            items=items,
            selected_sources=["yahoo_finance", "finnhub"],
        )
        # All 3 items must count toward yahoo_finance
        self.assertEqual(status.get("yahoo_finance", {}).get("status"), "OK")
        self.assertEqual(status.get("yahoo_finance", {}).get("rawCount"), 3)
        # company_ir and newswire must NOT appear as separate keys
        self.assertNotIn("company_ir", status)
        self.assertNotIn("newswire", status)
        # finnhub must be UNCONFIGURED
        self.assertEqual(status.get("finnhub", {}).get("status"), "UNCONFIGURED")

    def test_yahoo_empty_finnhub_unavailable_no_sec_default_behavior(self):
        """Regression: Yahoo empty + Finnhub unavailable must NOT surface SEC-only defaults."""
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (
                [],
                [],
                [{"code": "SOURCE_UNAVAILABLE", "message": "Finnhub company-news source is not configured; skipped.", "severity": "warning"}],
                "2026-05-16T10:00:00Z",
            )
            data = _parse(_run(srv.get_yahoo_finance_news("NVDA")))
            source_status = data.get("sourceStatus", {})
            # sec must not appear in source status
            self.assertNotIn("sec", source_status)
            # company_ir and newswire must not appear
            self.assertNotIn("company_ir", source_status)
            self.assertNotIn("newswire", source_status)
            # yahoo_finance must appear with EMPTY_RESULT (not OK)
            self.assertIn("yahoo_finance", source_status)
            self.assertEqual(source_status["yahoo_finance"].get("status"), "EMPTY_RESULT")
            # finnhub must be UNCONFIGURED
            self.assertEqual(source_status.get("finnhub", {}).get("status"), "UNCONFIGURED")


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
        with open(os.path.join(root, "server.py"), encoding="utf-8") as f:
            server_text = f.read()
        with open(os.path.join(root, "worker", "src", "tools.ts"), encoding="utf-8") as f:
            tools_text = f.read()
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
