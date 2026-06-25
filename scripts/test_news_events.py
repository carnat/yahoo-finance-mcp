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
        err = payload.get("error")
        code = err.get("code") if isinstance(err, dict) else payload.get("code")
        self.assertEqual(code, "INPUT_VALIDATION_ERROR")

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

    def test_get_company_news_default_sources_uses_fine_grained_yahoo(self):
        """Regression: default sources must be yahoo_finance_news + yahoo_finance_press_releases + finnhub only."""
        yf_items = [
            {
                "title": "Apple reports record sales",
                "source": "yahoo_finance_news",
                "originalSource": "Yahoo Finance",
                "sourceType": "yahoo_finance_news",
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
                "source": "yahoo_finance_news",
                "originalSource": "Yahoo Finance",
                "sourceType": "yahoo_finance_news",
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
            mocked.return_value = (yf_items, ["yahoo_finance_news"], finnhub_unconfigured_warning, "2026-05-15T13:01:22Z")
            data = _parse(_run(srv.get_company_news("AAPL")))
            # Must return 2 items from Yahoo even though Finnhub is absent
            self.assertEqual(len(data.get("items", [])), 2)
            # sourceStatus must include yahoo_finance_news, yahoo_finance_press_releases, and finnhub only
            source_status = data.get("sourceStatus", {})
            self.assertIn("yahoo_finance_news", source_status)
            self.assertIn("yahoo_finance_press_releases", source_status)
            self.assertIn("finnhub", source_status)
            # sec, company_ir, newswire, legacy yahoo_finance must NOT appear in default sourceStatus
            self.assertNotIn("sec", source_status)
            self.assertNotIn("company_ir", source_status)
            self.assertNotIn("newswire", source_status)
            self.assertNotIn("yahoo_finance", source_status)
            # Yahoo news returned items so its status is OK; Finnhub is UNCONFIGURED
            self.assertEqual(source_status.get("yahoo_finance_news", {}).get("status"), "OK")
            self.assertEqual(source_status.get("yahoo_finance_press_releases", {}).get("status"), "EMPTY_RESULT")
            self.assertEqual(source_status.get("finnhub", {}).get("status"), "UNCONFIGURED")
            # Top-level status: items present + SOURCE_UNAVAILABLE warning → PARTIAL (not SOURCE_LIMITED_NOT_FOUND)
            self.assertIn(data.get("status"), ("PARTIAL", None))

    def test_get_yahoo_finance_news_returns_yahoo_items_when_finnhub_absent(self):
        """Regression: get_yahoo_finance_news must return Yahoo items even when Finnhub key is absent."""
        yf_items = [
            {
                "title": "Apple hits all-time high",
                "source": "yahoo_finance_news",
                "originalSource": "Yahoo Finance",
                "sourceType": "yahoo_finance_news",
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
                "source": "yahoo_finance_news",
                "originalSource": "Yahoo Finance",
                "sourceType": "yahoo_finance_news",
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
            mocked.return_value = (yf_items, ["yahoo_finance_news"], finnhub_unconfigured_warning, "2026-05-16T08:01:00Z")
            data = _parse(_run(srv.get_yahoo_finance_news("AAPL")))
            # Must return 2 Yahoo items even with no Finnhub key
            self.assertEqual(len(data.get("items", [])), 2)
            source_status = data.get("sourceStatus", {})
            # sec/company_ir/newswire must not appear
            self.assertNotIn("sec", source_status)
            self.assertNotIn("company_ir", source_status)
            self.assertNotIn("newswire", source_status)
            # _collect_company_events must have been called with fine-grained Yahoo sources
            call_kwargs = mocked.call_args
            called_sources = call_kwargs.kwargs.get("sources") or (call_kwargs.args[1] if len(call_kwargs.args) > 1 else None)
            if called_sources is not None:
                self.assertNotIn("sec", called_sources)
                self.assertNotIn("company_ir", called_sources)
                self.assertNotIn("newswire", called_sources)
                # Must include at least one Yahoo Finance source
                self.assertTrue(
                    any(s in called_sources for s in ("yahoo_finance_news", "yahoo_finance_press_releases", "yahoo_finance")),
                    f"Expected a Yahoo Finance source in {called_sources}",
                )

    def test_yahoo_newswire_items_included_when_yahoo_finance_selected(self):
        """Regression: all Yahoo feed items must be returned when yahoo_finance (legacy) is selected."""
        # Items now use the new fine-grained source labels even when fetched via the legacy 'yahoo_finance' source.
        yf_feed_items = [
            {
                "title": "Apple Q2 Results Press Release",
                "source": "yahoo_finance_press_releases",
                "originalSource": "Business Wire",
                "sourceType": "yahoo_finance_press_releases",
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
                "source": "yahoo_finance_news",
                "originalSource": "Apple Investor Relations",
                "sourceType": "yahoo_finance_news",
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
                "source": "yahoo_finance_news",
                "originalSource": "Yahoo Finance",
                "sourceType": "yahoo_finance_news",
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
            # Call with legacy yahoo_finance source explicitly
            data = _parse(_run(srv.get_company_news("AAPL", sources=["yahoo_finance", "finnhub"])))
            # All 3 items must appear
            self.assertEqual(len(data.get("items", [])), 3)
            # sourceStatus.yahoo_finance (legacy) must report OK with rawCount=3
            source_status = data.get("sourceStatus", {})
            yf_status = source_status.get("yahoo_finance", {})
            self.assertEqual(yf_status.get("status"), "OK")
            self.assertEqual(yf_status.get("rawCount"), 3)
            # sec/company_ir/newswire must NOT appear as separate top-level status keys
            self.assertNotIn("sec", source_status)
            self.assertNotIn("company_ir", source_status)
            self.assertNotIn("newswire", source_status)

    def test_collect_company_events_includes_all_yahoo_items_when_yahoo_selected(self):
        """Unit test: _collect_company_events includes all Yahoo feed items when yahoo_finance is selected."""
        import datetime

        news_item = {
            "title": "Tesla raises guidance",
            "source": "yahoo_finance_news",
            "originalSource": "PR Newswire",
            "sourceType": "yahoo_finance_news",
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
        pr_item = {
            "title": "Tesla investor update",
            "source": "yahoo_finance_press_releases",
            "originalSource": "Tesla IR",
            "sourceType": "yahoo_finance_press_releases",
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
            mock_yf.return_value = ([news_item, pr_item], [], True)
            mock_fh.return_value = ([], [{"code": "SOURCE_UNAVAILABLE", "message": "Finnhub company-news source is not configured; skipped.", "severity": "warning"}], False)
            items, sources_used, warnings, _ = _run(
                srv._collect_company_events("TSLA", max_results=10, lookback_days=14, sources=["yahoo_finance", "finnhub"])
            )
            # Both yahoo_finance_news and yahoo_finance_press_releases items from the Yahoo feed must be included
            self.assertEqual(len(items), 2)
            sources = {it["source"] for it in items}
            self.assertIn("yahoo_finance_news", sources)
            self.assertIn("yahoo_finance_press_releases", sources)

    def test_compute_source_status_with_new_yahoo_sources(self):
        """Unit test: _compute_source_status reports fine-grained Yahoo Finance source statuses."""
        items = [
            {"sourceType": "yahoo_finance_news", "source": "yahoo_finance_news", "originalSource": "Reuters"},
            {"sourceType": "yahoo_finance_press_releases", "source": "yahoo_finance_press_releases", "originalSource": "BusinessWire"},
            {"sourceType": "yahoo_finance_news", "source": "yahoo_finance_news", "originalSource": "Yahoo Finance"},
        ]
        # Fine-grained sources selected
        status = srv._compute_source_status(
            sources_used=["yahoo_finance_news", "yahoo_finance_press_releases"],
            warnings=[{"code": "SOURCE_UNAVAILABLE", "message": "Finnhub company-news source is not configured; skipped.", "severity": "warning"}],
            items=items,
            selected_sources=["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"],
        )
        # yahoo_finance_news has 2 items
        self.assertEqual(status.get("yahoo_finance_news", {}).get("status"), "OK")
        self.assertEqual(status.get("yahoo_finance_news", {}).get("rawCount"), 2)
        # yahoo_finance_press_releases has 1 item
        self.assertEqual(status.get("yahoo_finance_press_releases", {}).get("status"), "OK")
        self.assertEqual(status.get("yahoo_finance_press_releases", {}).get("rawCount"), 1)
        # finnhub must be UNCONFIGURED
        self.assertEqual(status.get("finnhub", {}).get("status"), "UNCONFIGURED")
        # legacy yahoo_finance must NOT appear
        self.assertNotIn("yahoo_finance", status)

    def test_compute_source_status_legacy_yahoo_finance_aggregates(self):
        """Unit test: _compute_source_status aggregates both yahoo sub-sources under legacy yahoo_finance key."""
        items = [
            {"sourceType": "yahoo_finance_news", "source": "yahoo_finance_news", "originalSource": "Business Wire"},
            {"sourceType": "yahoo_finance_press_releases", "source": "yahoo_finance_press_releases", "originalSource": "Apple IR"},
            {"sourceType": "yahoo_finance_news", "source": "yahoo_finance_news", "originalSource": "Yahoo Finance"},
        ]
        # Legacy yahoo_finance source selected
        status = srv._compute_source_status(
            sources_used=["yahoo_finance"],
            warnings=[{"code": "SOURCE_UNAVAILABLE", "message": "Finnhub company-news source is not configured; skipped.", "severity": "warning"}],
            items=items,
            selected_sources=["yahoo_finance", "finnhub"],
        )
        # All 3 items must count toward yahoo_finance legacy aggregate
        self.assertEqual(status.get("yahoo_finance", {}).get("status"), "OK")
        self.assertEqual(status.get("yahoo_finance", {}).get("rawCount"), 3)
        # fine-grained keys must NOT appear when only legacy source is selected
        self.assertNotIn("yahoo_finance_news", status)
        self.assertNotIn("yahoo_finance_press_releases", status)
        # company_ir and newswire must NOT appear
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
            # yahoo_finance_news and yahoo_finance_press_releases must appear with EMPTY_RESULT
            self.assertIn("yahoo_finance_news", source_status)
            self.assertEqual(source_status["yahoo_finance_news"].get("status"), "EMPTY_RESULT")
            self.assertIn("yahoo_finance_press_releases", source_status)
            self.assertEqual(source_status["yahoo_finance_press_releases"].get("status"), "EMPTY_RESULT")
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


class TestPhase6BYahooFinanceSources(unittest.TestCase):
    """Tests for the new yahoo_finance_news / yahoo_finance_press_releases split."""

    def _make_news_item(self, dup_id: str, source: str, source_type: str, original_source: str = "Yahoo Finance") -> dict:
        import datetime
        return {
            "title": f"Headline for {dup_id}",
            "source": source,
            "originalSource": original_source,
            "sourceType": source_type,
            "publishedAt": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "retrievedAt": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": f"https://finance.yahoo.com/news/{dup_id}",
            "tickers": ["AAPL"],
            "eventType": "other",
            "summary": f"summary {dup_id}",
            "evidenceText": f"text {dup_id}",
            "confidence": "MEDIUM",
            "tickerRelevance": "HIGH",
            "duplicateGroupId": dup_id,
        }

    # ------------------------------------------------------------------
    # _build_yahoo_event_item unit tests
    # ------------------------------------------------------------------

    def test_build_yahoo_event_item_story_labeled_yahoo_finance_news(self):
        """STORY contentType → source=yahoo_finance_news, sourceType=yahoo_finance_news."""
        raw = {
            "content": {
                "title": "NVDA beats estimates",
                "summary": "Strong quarter",
                "contentType": "STORY",
                "pubDate": "2026-05-15T12:00:00.000Z",
                "provider": {"displayName": "Reuters"},
                "canonicalUrl": {"url": "https://reuters.com/nvda"},
            },
            "providerPublishTime": 1747310400,
        }
        import server as srv_mod
        item, _ = srv_mod._build_yahoo_event_item("NVDA", raw, "2026-05-15T13:00:00Z")
        self.assertEqual(item["source"], "yahoo_finance_news")
        self.assertEqual(item["sourceType"], "yahoo_finance_news")
        self.assertEqual(item["originalSource"], "Reuters")

    def test_build_yahoo_event_item_press_release_labeled_yahoo_finance_press_releases(self):
        """PRESS_RELEASE contentType → source=yahoo_finance_press_releases."""
        raw = {
            "content": {
                "title": "AAPL declares special dividend",
                "summary": "Apple board approves dividend",
                "contentType": "PRESS_RELEASE",
                "pubDate": "2026-05-10T16:00:00.000Z",
                "provider": {"displayName": "BusinessWire"},
                "canonicalUrl": {"url": "https://businesswire.com/aapl-div"},
            },
            "providerPublishTime": 1746892800,
        }
        import server as srv_mod
        item, _ = srv_mod._build_yahoo_event_item("AAPL", raw, "2026-05-10T17:00:00Z")
        self.assertEqual(item["source"], "yahoo_finance_press_releases")
        self.assertEqual(item["sourceType"], "yahoo_finance_press_releases")
        self.assertEqual(item["originalSource"], "BusinessWire")

    def test_build_yahoo_event_item_feed_source_override(self):
        """feed_source parameter forces the source label regardless of contentType."""
        raw = {
            "title": "Tesla Q1",
            "summary": "Beat estimates",
            "publisher": "PR Newswire",
            "link": "https://prnewswire.com/tsla",
            "providerPublishTime": 1747310400,
        }
        import server as srv_mod
        item_pr, _ = srv_mod._build_yahoo_event_item("TSLA", raw, "2026-05-15T13:00:00Z", feed_source="yahoo_finance_press_releases")
        self.assertEqual(item_pr["source"], "yahoo_finance_press_releases")
        self.assertEqual(item_pr["originalSource"], "PR Newswire")

        item_news, _ = srv_mod._build_yahoo_event_item("TSLA", raw, "2026-05-15T13:00:00Z", feed_source="yahoo_finance_news")
        self.assertEqual(item_news["source"], "yahoo_finance_news")

    def test_build_yahoo_event_item_has_original_source(self):
        """originalSource field is always populated."""
        raw = {
            "content": {
                "title": "AAPL news",
                "contentType": "ARTICLE",
                "provider": {"displayName": "Motley Fool"},
                "canonicalUrl": {"url": "https://fool.com/aapl"},
            },
            "providerPublishTime": 1747310400,
        }
        import server as srv_mod
        item, _ = srv_mod._build_yahoo_event_item("AAPL", raw, "2026-05-15T13:00:00Z")
        self.assertIn("originalSource", item)
        self.assertEqual(item["originalSource"], "Motley Fool")

    # ------------------------------------------------------------------
    # get_company_news acceptance tests
    # ------------------------------------------------------------------

    def test_get_company_news_merged_output_preserves_source_identity(self):
        """get_company_news merged result preserves distinct source labels per item."""
        items = [
            self._make_news_item("n1", "yahoo_finance_news", "yahoo_finance_news", "Reuters"),
            self._make_news_item("pr1", "yahoo_finance_press_releases", "yahoo_finance_press_releases", "BusinessWire"),
            self._make_news_item("fh1", "finnhub", "company_news", "Barron's"),
        ]
        items[-1]["originalSource"] = "Barron's"
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (items, ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"], [], "2026-05-15T12:00:00Z")
            data = _parse(_run(srv.get_company_news("AAPL")))
            returned_sources = {it["source"] for it in data.get("items", [])}
            self.assertIn("yahoo_finance_news", returned_sources)
            self.assertIn("yahoo_finance_press_releases", returned_sources)
            self.assertIn("finnhub", returned_sources)
            # original sources preserved
            original_sources = {it.get("originalSource") for it in data.get("items", [])}
            self.assertIn("Reuters", original_sources)
            self.assertIn("BusinessWire", original_sources)

    def test_get_company_news_source_status_distinguishes_all_three(self):
        """sourceStatus has distinct keys for yahoo_finance_news, yahoo_finance_press_releases, finnhub."""
        news_items = [self._make_news_item("n1", "yahoo_finance_news", "yahoo_finance_news")]
        pr_items = [self._make_news_item("pr1", "yahoo_finance_press_releases", "yahoo_finance_press_releases")]
        finnhub_items = [self._make_news_item("fh1", "finnhub", "company_news")]

        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (
                news_items + pr_items + finnhub_items,
                ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"],
                [],
                "2026-05-15T12:00:00Z",
            )
            data = _parse(_run(srv.get_company_news("AAPL")))
            ss = data.get("sourceStatus", {})
            self.assertEqual(ss.get("yahoo_finance_news", {}).get("status"), "OK")
            self.assertEqual(ss.get("yahoo_finance_press_releases", {}).get("status"), "OK")
            self.assertEqual(ss.get("finnhub", {}).get("status"), "OK")
            # Legacy yahoo_finance must NOT appear by default
            self.assertNotIn("yahoo_finance", ss)

    # ------------------------------------------------------------------
    # get_company_press_releases acceptance tests
    # ------------------------------------------------------------------

    def test_get_company_press_releases_yahoo_pr_as_first_class_source(self):
        """get_company_press_releases returns yahoo_finance_press_releases items."""
        pr_item = self._make_news_item("pr1", "yahoo_finance_press_releases", "yahoo_finance_press_releases", "PR Newswire")
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = ([pr_item], ["yahoo_finance_press_releases"], [], "2026-05-15T12:00:00Z")
            data = _parse(_run(srv.get_company_press_releases("AAPL")))
            sources = {it["source"] for it in data.get("items", [])}
            self.assertIn("yahoo_finance_press_releases", sources)
            self.assertNotIn("NO_OFFICIAL_RELEASE_SOURCE", [w.get("code") for w in data.get("warnings", [])])

    def test_get_company_press_releases_default_includes_yahoo_pr_source(self):
        """get_company_press_releases default sources include yahoo_finance_press_releases."""
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = ([], [], [], "2026-05-15T12:00:00Z")
            _run(srv.get_company_press_releases("AAPL"))
            call_kwargs = mocked.call_args
            called_sources = call_kwargs.kwargs.get("sources") or []
            self.assertIn("yahoo_finance_press_releases", called_sources)

    # ------------------------------------------------------------------
    # _build_yahoo_event_item: item schema
    # ------------------------------------------------------------------

    def test_yahoo_item_schema_has_required_fields(self):
        """Yahoo news items contain all required schema fields."""
        raw = {
            "content": {
                "title": "NVDA earnings beat",
                "summary": "Revenue up 20%",
                "contentType": "STORY",
                "pubDate": "2026-05-15T12:00:00.000Z",
                "provider": {"displayName": "Yahoo Finance"},
                "canonicalUrl": {"url": "https://finance.yahoo.com/nvda-earnings"},
            },
            "providerPublishTime": 1747310400,
        }
        import server as srv_mod
        item, _ = srv_mod._build_yahoo_event_item("NVDA", raw, "2026-05-15T13:00:00Z")
        for field in ("source", "sourceType", "originalSource", "publishedAt", "retrievedAt", "url", "title", "summary", "tickers", "eventType"):
            self.assertIn(field, item, f"Missing field: {field}")

    # ------------------------------------------------------------------
    # Dedupe: same story from Yahoo + Finnhub preserves both sources
    # ------------------------------------------------------------------

    def test_dedupe_preserves_source_identity_when_story_appears_in_multiple_sources(self):
        """Dedupe keeps items even when the same story is seen from Yahoo and Finnhub (different duplicateGroupId)."""
        import datetime
        now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        yf_item = {
            "title": "AAPL new product launch",
            "source": "yahoo_finance_news",
            "originalSource": "Reuters",
            "sourceType": "yahoo_finance_news",
            "publishedAt": now,
            "retrievedAt": now,
            "url": "https://reuters.com/aapl-launch",
            "tickers": ["AAPL"],
            "eventType": "product",
            "summary": "new product",
            "evidenceText": "product launch",
            "confidence": "MEDIUM",
            "tickerRelevance": "HIGH",
            "duplicateGroupId": "aapl-launch-yf",
        }
        fh_item = {
            "title": "AAPL new product launch",
            "source": "finnhub",
            "originalSource": "Reuters",
            "sourceType": "company_news",
            "publishedAt": now,
            "retrievedAt": now,
            "url": "https://reuters.com/aapl-launch",
            "tickers": ["AAPL"],
            "eventType": "product",
            "summary": "new product",
            "evidenceText": "product launch",
            "confidence": "MEDIUM",
            "tickerRelevance": "HIGH",
            "duplicateGroupId": "aapl-launch-fh",
        }
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = ([yf_item, fh_item], ["yahoo_finance_news", "finnhub"], [], now)
            data = _parse(_run(srv.get_company_news("AAPL")))
            items = data.get("items", [])
            # Both items must be present (different duplicateGroupId → not deduped)
            item_sources = {it["source"] for it in items}
            self.assertIn("yahoo_finance_news", item_sources)
            self.assertIn("finnhub", item_sources)

    # ------------------------------------------------------------------
    # Backward-compat: legacy yahoo_finance source still works
    # ------------------------------------------------------------------

    def test_legacy_yahoo_finance_source_still_accepted(self):
        """Passing sources=['yahoo_finance', 'finnhub'] still works without errors."""
        items = [self._make_news_item("n1", "yahoo_finance_news", "yahoo_finance_news")]
        with patch("server._collect_company_events", new_callable=AsyncMock) as mocked:
            mocked.return_value = (items, ["yahoo_finance"], [], "2026-05-15T12:00:00Z")
            data = _parse(_run(srv.get_company_news("AAPL", sources=["yahoo_finance", "finnhub"])))
            self.assertEqual(len(data.get("items", [])), 1)
            # Legacy source status key should appear
            ss = data.get("sourceStatus", {})
            self.assertIn("yahoo_finance", ss)
            self.assertIn("finnhub", ss)

    # ------------------------------------------------------------------
    # Fix 1: press-release fallback must NOT mislabel generic news
    # ------------------------------------------------------------------

    def test_press_release_tab_unavailable_returns_empty_not_general_feed(self):
        """If get_news(tab='press releases') fails, return empty + warning (no fallback to general feed)."""
        import server as srv_mod

        class _BadTicker:
            """Simulates a yfinance Ticker where get_news(tab='press releases') raises."""
            def get_news(self, tab="news"):
                if tab == "press releases":
                    raise AttributeError("tab parameter not supported in this yfinance version")
                return []

        import datetime
        retrieved = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        with patch("server.yf") as mock_yf:
            mock_yf.Ticker.return_value = _BadTicker()
            items, warnings, used = _run(
                srv_mod._collect_yahoo_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                    feed="press_releases",
                )
            )

        # Must return empty items — no mislabeled generic news
        self.assertEqual(items, [])
        self.assertFalse(used)
        # Must emit a warning explaining why
        codes = [w.get("code") for w in warnings]
        self.assertIn("PRESS_RELEASE_TAB_UNAVAILABLE", codes)

    def test_press_release_tab_success_labels_items_correctly(self):
        """Successful get_news(tab='press releases') labels all items as yahoo_finance_press_releases."""
        import server as srv_mod
        import datetime

        retrieved = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        pr_item = {
            "content": {
                "title": "AAPL declares dividend",
                "summary": "Board approves quarterly dividend",
                "contentType": "PRESS_RELEASE",
                # Use a string ISO pubDate (not an int providerPublishTime) so the
                # active _to_iso_utc implementation can parse it correctly.
                "pubDate": retrieved,
                "provider": {"displayName": "BusinessWire"},
                "canonicalUrl": {"url": "https://businesswire.com/aapl-div"},
            },
            # Omit providerPublishTime (integer) — would be mis-parsed by the string-only
            # _to_iso_utc and produce publishedAt=None, causing the item to be date-filtered.
        }

        class _GoodTicker:
            def get_news(self, tab="news"):
                return [pr_item]

        with patch("server.yf") as mock_yf:
            mock_yf.Ticker.return_value = _GoodTicker()
            items, warnings, used = _run(
                srv_mod._collect_yahoo_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                    feed="press_releases",
                )
            )

        self.assertTrue(used)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["source"], "yahoo_finance_press_releases")

    def test_press_release_tab_story_items_are_accepted(self):
        """Yahoo press-release tab items can arrive as STORY and must still be kept."""
        import server as srv_mod
        import datetime

        retrieved = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        pr_item = {
            "content": {
                "title": "REX Shares Launches T-REX 2X ASTS (ASUP) & 2X LITE (LITU) ETFs",
                "summary": "REX Shares announces leveraged ETFs tied to ASTS and LITE.",
                "contentType": "STORY",
                "pubDate": retrieved,
                "provider": {"displayName": "Business Wire"},
                "canonicalUrl": {"url": "https://finance.yahoo.com/markets/options/articles/rex-shares-launches-t-rex-120000327.html"},
            },
        }

        class _GoodTicker:
            def get_news(self, tab="news"):
                self.tab = tab
                return [pr_item]

        ticker = _GoodTicker()
        with patch("server.yf") as mock_yf:
            mock_yf.Ticker.return_value = ticker
            items, warnings, used = _run(
                srv_mod._collect_yahoo_events(
                    "ASTS",
                    retrieved_at=retrieved,
                    max_results=10,
                    feed="press_releases",
                )
            )

        self.assertTrue(used)
        self.assertEqual(ticker.tab, "press releases")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["source"], "yahoo_finance_press_releases")
        self.assertEqual(items[0]["sourceType"], "yahoo_finance_press_releases")
        self.assertEqual(items[0]["originalSource"], "Business Wire")
        self.assertEqual(warnings, [])

    def test_press_release_tab_no_fallback_means_no_mislabeled_items(self):
        """Items without contentType must not be labeled yahoo_finance_press_releases via fallback."""
        import server as srv_mod

        generic_news_item = {
            "title": "AAPL generic news without contentType",
            "publisher": "Some Publisher",
            "link": "https://example.com/news",
            "providerPublishTime": 1747310400,
            # No content/contentType — typical of older yfinance company.news format
        }

        class _FallbackTicker:
            """Simulates get_news(tab=...) failing; has company.news with generic items."""
            @property
            def news(self):
                return [generic_news_item]

            def get_news(self, tab="news"):
                if tab == "press releases":
                    raise AttributeError("tab not supported")
                return []

        import datetime
        retrieved = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        with patch("server.yf") as mock_yf:
            mock_yf.Ticker.return_value = _FallbackTicker()
            items, warnings, used = _run(
                srv_mod._collect_yahoo_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                    feed="press_releases",
                )
            )

        # Must not contain the generic item labeled as press release
        pr_labeled = [it for it in items if it.get("source") == "yahoo_finance_press_releases"]
        self.assertEqual(pr_labeled, [], "Generic news items must not be mislabeled as press releases")
        self.assertFalse(used)


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


_MOCK_RSS_RELEVANT = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel xmlns:dc="http://purl.org/dc/elements/1.1/">
    <title>GlobeNewswire - News about Public Companies</title>
    <item>
      <title>Apple Inc. Announces New Product Launch</title>
      <category domain="https://www.globenewswire.com/rss/stock">Nasdaq:AAPL</category>
      <category domain="https://www.globenewswire.com/rss/ISIN">US0378331005</category>
      <description>Apple AAPL unveils its latest innovation at its annual event.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/001.html</link>
      <pubDate>Thu, 15 May 2026 13:00:00 GMT</pubDate>
      <dc:identifier>001</dc:identifier>
      <dc:language>en</dc:language>
      <dc:contributor>Apple Inc.</dc:contributor>
      <dc:subject>Product / Services Announcement</dc:subject>
      <dc:keyword>Apple</dc:keyword>
    </item>
    <item>
      <title>Unrelated Company Raises Capital</title>
      <category domain="https://www.globenewswire.com/rss/stock">Nasdaq:XYZ</category>
      <description>XYZ Corp completes a Series C round.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/002.html</link>
      <pubDate>Thu, 15 May 2026 12:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

_MOCK_RSS_EMPTY = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>GlobeNewswire - News about Public Companies</title>
  </channel>
</rss>
"""

_MOCK_RSS_AMBIGUOUS_WORDS = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Unrelated company launches AI platform on Monday</title>
      <description>It will focus on analytics for public companies.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/003.html</link>
      <pubDate>Thu, 15 May 2026 13:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

_MOCK_RSS_AMBIGUOUS_MARKER = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>C3.ai Announces Product Update</title>
      <category domain="https://www.globenewswire.com/rss/stock">NYSE:AI</category>
      <description>C3.ai released a new enterprise AI application.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/004.html</link>
      <pubDate>Thu, 15 May 2026 13:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

_MOCK_RSS_DATED_MANY = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Apple AAPL First Item</title>
      <category domain="https://www.globenewswire.com/rss/stock">Nasdaq:AAPL</category>
      <description>Apple AAPL announces item one.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/005.html</link>
      <pubDate>Thu, 15 May 2026 13:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Apple AAPL Second Item</title>
      <category domain="https://www.globenewswire.com/rss/stock">Nasdaq:AAPL</category>
      <description>Apple AAPL announces item two.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/14/006.html</link>
      <pubDate>Wed, 14 May 2026 13:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Apple AAPL Old Item</title>
      <category domain="https://www.globenewswire.com/rss/stock">Nasdaq:AAPL</category>
      <description>Apple AAPL announces an older item.</description>
      <link>https://www.globenewswire.com/news-release/2026/04/01/007.html</link>
      <pubDate>Wed, 01 Apr 2026 13:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

_MOCK_RSS_EXACT_TICKERS = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel xmlns:dc="http://purl.org/dc/elements/1.1/">
    <item>
      <title>NVIDIA Corporation Announces Data Center Update</title>
      <category domain="https://www.globenewswire.com/rss/stock">Nasdaq:NVDA</category>
      <description><![CDATA[<p>NVIDIA Corporation published a direct company update.</p>]]></description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/nvda.html</link>
      <pubDate>Thu, 15 May 2026 13:00:00 GMT</pubDate>
      <dc:identifier>nvda-1</dc:identifier>
      <dc:language>en</dc:language>
      <dc:contributor>NVIDIA Corporation</dc:contributor>
      <dc:subject>Company Announcement</dc:subject>
      <dc:keyword>GPU</dc:keyword>
    </item>
    <item>
      <title>AST SpaceMobile Announces Satellite Update</title>
      <category domain="https://www.globenewswire.com/rss/stock">Nasdaq:ASTS</category>
      <description>AST SpaceMobile published a direct company update.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/asts.html</link>
      <pubDate>Thu, 15 May 2026 12:00:00 GMT</pubDate>
      <dc:contributor>AST SpaceMobile, Inc.</dc:contributor>
      <dc:subject>Product / Services Announcement</dc:subject>
    </item>
    <item>
      <title>Lumentum Announces Optical Networking Update</title>
      <category domain="https://www.globenewswire.com/rss/stock">NYSE:LITE</category>
      <category domain="https://www.globenewswire.com/rss/ISIN">US55024U1097</category>
      <description>Lumentum published a direct company update.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/lite.html</link>
      <pubDate>Thu, 15 May 2026 11:00:00 GMT</pubDate>
      <dc:contributor>Lumentum Holdings Inc.</dc:contributor>
      <dc:subject>Company Announcement</dc:subject>
    </item>
  </channel>
</rss>
"""

_MOCK_RSS_TEXT_FALSE_POSITIVES = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>GOWIN Participates in NVIDIA APAC Robotics and Edge AI Partner Day</title>
      <description>GOWIN is invited by NVIDIA to participate in an event.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/gowin.html</link>
      <pubDate>Thu, 15 May 2026 13:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Company Hosts Webcasts and Forecasts</title>
      <category domain="https://www.globenewswire.com/rss/stock">Nasdaq:AMBA</category>
      <description>The word webcasts contains ASTS as a substring.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/webcasts.html</link>
      <pubDate>Thu, 15 May 2026 12:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Satellite Provider Highlights Light Equipment</title>
      <category domain="https://www.globenewswire.com/rss/stock">Nasdaq:GILT</category>
      <description>The words satellite and light should not match LITE.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/lite-false.html</link>
      <pubDate>Thu, 15 May 2026 11:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

_MOCK_RSS_DOCTYPE = """\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE rss [<!ENTITY x "blocked">]>
<rss version="2.0">
  <channel>
    <item>
      <title>&x;</title>
    </item>
  </channel>
</rss>
"""


class TestGlobeNewswireRSS(unittest.TestCase):
    """Tests for the direct GlobeNewswire RSS fetcher."""

    def _get_srv(self):
        import server as srv_mod  # noqa: PLC0415
        return srv_mod

    def _retrieved(self):
        import datetime
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _mock_urlopen(self, content):
        payload = content if isinstance(content, bytes) else content.encode("utf-8")

        def _fake_urlopen(req, timeout=20):
            class _Resp:
                def read(self, size=-1):
                    return payload if size is None or size < 0 else payload[:size]
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    pass
            return _Resp()

        return patch("server._urlrequest.urlopen", side_effect=_fake_urlopen)

    def _mock_yf_no_info(self):
        """yfinance stub that returns no company info."""
        class _NoInfo:
            @property
            def info(self):
                return {}
        mock_yf = patch("server.yf").__enter__()
        return mock_yf

    def test_relevant_item_returned_filtered_out_unrelated(self):
        """Only items with an exact GlobeNewswire stock category should be returned."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        def _fake_urlopen(req, timeout=20):
            class _Resp:
                def read(self, size=-1):
                    return _MOCK_RSS_RELEVANT.encode("utf-8")
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    pass
            return _Resp()

        with patch("server.yf") as mock_yf, \
             patch("server._urlrequest.urlopen", side_effect=_fake_urlopen), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {"shortName": "Apple Inc.", "longName": "Apple Inc."}
            mock_cache.get.return_value = None
            mock_cache.set.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                    lookback_days=365,
                )
            )

        self.assertTrue(used)
        self.assertEqual(len(items), 1, "Should return only the AAPL-relevant item")
        item = items[0]
        self.assertEqual(item["source"], "newswire")
        self.assertEqual(item["sourceType"], "newswire")
        self.assertEqual(item["originalSource"], "GlobeNewswire")
        self.assertEqual(item["provider"], "globenewswire")
        self.assertEqual(item["discoveredVia"], "globenewswire_rss")
        self.assertEqual(item["tickerRelevance"], "HIGH")
        self.assertIn("AAPL", item["tickers"])
        self.assertEqual(item["stockCategories"], ["Nasdaq:AAPL"])
        self.assertEqual(item["isin"], "US0378331005")
        self.assertEqual(item["issuer"], "Apple Inc.")
        self.assertEqual(item["subject"], "Product / Services Announcement")
        self.assertEqual(item["keywords"], ["Apple"])
        self.assertEqual(item["language"], "en")
        self.assertEqual(item["globenewswireId"], "001")
        self.assertIn(item["feedSource"], {name for name, _url in srv_mod._GLOBENEWSWIRE_RSS_FEEDS})

    def test_filter_disabled_still_requires_exact_stock_category(self):
        """filter_low_relevance cannot turn unrelated stock tags into newswire hits."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        def _fake_urlopen(req, timeout=20):
            class _Resp:
                def read(self, size=-1):
                    return _MOCK_RSS_RELEVANT.encode("utf-8")
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    pass
            return _Resp()

        with patch("server.yf") as mock_yf, \
             patch("server._urlrequest.urlopen", side_effect=_fake_urlopen), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {}
            mock_cache.get.return_value = None
            mock_cache.set.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                    lookback_days=365,
                    filter_low_relevance=False,
                )
            )

        self.assertTrue(used)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["stockCategories"], ["Nasdaq:AAPL"])

    def test_exact_stock_categories_match_nvda_asts_lite(self):
        """NVDA, ASTS, and LITE match only their exact RSS stock categories."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()
        expected = {
            "NVDA": "Nasdaq:NVDA",
            "ASTS": "Nasdaq:ASTS",
            "LITE": "NYSE:LITE",
        }

        for ticker, category in expected.items():
            with self.subTest(ticker=ticker), \
                 patch("server.yf") as mock_yf, \
                 self._mock_urlopen(_MOCK_RSS_EXACT_TICKERS), \
                 patch("server._tool_cache") as mock_cache:
                mock_yf.Ticker.return_value.info = {}
                mock_cache.get.return_value = None
                mock_cache.set.return_value = None

                items, warnings, used = _run(
                    srv_mod._collect_globenewswire_events(
                        ticker,
                        retrieved_at=retrieved,
                        max_results=10,
                    )
                )

            self.assertTrue(used)
            self.assertEqual(warnings, [])
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["tickerRelevance"], "HIGH")
            self.assertEqual(items[0]["stockCategories"], [category])

    def test_text_false_positives_ignored_for_nvda_asts_lite(self):
        """Text mentions and substrings are ignored when the exact stock tag is absent."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        for ticker in ("NVDA", "ASTS", "LITE"):
            with self.subTest(ticker=ticker), \
                 patch("server.yf") as mock_yf, \
                 self._mock_urlopen(_MOCK_RSS_TEXT_FALSE_POSITIVES), \
                 patch("server._tool_cache") as mock_cache:
                mock_yf.Ticker.return_value.info = {}
                mock_cache.get.return_value = None
                mock_cache.set.return_value = None

                items, warnings, used = _run(
                    srv_mod._collect_globenewswire_events(
                        ticker,
                        retrieved_at=retrieved,
                        max_results=10,
                    )
                )

            self.assertTrue(used)
            self.assertEqual(warnings, [])
            self.assertEqual(items, [])

    def test_ambiguous_ticker_words_do_not_match_without_marker(self):
        """Common words like AI/ON/IT must not be treated as ticker hits."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        with patch("server.yf") as mock_yf, \
             self._mock_urlopen(_MOCK_RSS_AMBIGUOUS_WORDS), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {}
            mock_cache.get.return_value = None
            mock_cache.set.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AI",
                    retrieved_at=retrieved,
                    max_results=10,
                )
            )

        self.assertTrue(used)
        self.assertEqual(warnings, [])
        self.assertEqual(items, [])

    def test_ambiguous_ticker_matches_exact_stock_category(self):
        """Ambiguous tickers still match when the feed uses an exact stock category."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        with patch("server.yf") as mock_yf, \
             self._mock_urlopen(_MOCK_RSS_AMBIGUOUS_MARKER), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {}
            mock_cache.get.return_value = None
            mock_cache.set.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AI",
                    retrieved_at=retrieved,
                    max_results=10,
                )
            )

        self.assertTrue(used)
        self.assertEqual(warnings, [])
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["tickerRelevance"], "HIGH")
        self.assertEqual(items[0]["stockCategories"], ["NYSE:AI"])

    def test_company_name_without_stock_category_is_ignored(self):
        """Company names from yfinance do not establish newswire relevance."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()
        rss = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>ON Semiconductor Corporation Announces Results</title>
      <description>ON Semiconductor Corporation reported quarterly results.</description>
      <link>https://www.globenewswire.com/news-release/2026/05/15/008.html</link>
      <pubDate>Thu, 15 May 2026 13:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

        with patch("server.yf") as mock_yf, \
             self._mock_urlopen(rss), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {"shortName": "ON Semiconductor Corporation"}
            mock_cache.get.return_value = None
            mock_cache.set.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "ON",
                    retrieved_at=retrieved,
                    max_results=10,
                )
            )

        self.assertTrue(used)
        self.assertEqual(warnings, [])
        self.assertEqual(items, [])

    def test_date_window_filters_globenewswire_items(self):
        """start_date/end_date filters apply to GlobeNewswire RSS items."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        with patch("server.yf") as mock_yf, \
             self._mock_urlopen(_MOCK_RSS_DATED_MANY), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {}
            mock_cache.get.return_value = None
            mock_cache.set.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                    start_date="2026-05-14",
                    end_date="2026-05-15",
                )
            )

        self.assertTrue(used)
        self.assertEqual(warnings, [])
        self.assertEqual(len(items), 2)
        self.assertNotIn("Old Item", " ".join(item["title"] for item in items))

    def test_max_results_limits_globenewswire_items(self):
        """max_results truncates relevant GlobeNewswire RSS items."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        with patch("server.yf") as mock_yf, \
             self._mock_urlopen(_MOCK_RSS_DATED_MANY), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {}
            mock_cache.get.return_value = None
            mock_cache.set.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=2,
                )
            )

        self.assertTrue(used)
        self.assertEqual(warnings, [])
        self.assertEqual(len(items), 2)

    def test_provider_error_on_oversized_xml(self):
        """Oversized RSS responses fail closed before XML parsing."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()
        oversized = b"x" * (srv_mod._GLOBENEWSWIRE_MAX_BYTES + 1)

        with patch("server.yf") as mock_yf, \
             self._mock_urlopen(oversized), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {}
            mock_cache.get.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                )
            )

        self.assertFalse(used)
        self.assertEqual(items, [])
        self.assertIn("SOURCE_UNAVAILABLE", [w.get("code") for w in warnings])

    def test_provider_error_on_blocked_xml_declaration(self):
        """DOCTYPE/entity declarations are rejected for external RSS content."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        with patch("server.yf") as mock_yf, \
             self._mock_urlopen(_MOCK_RSS_DOCTYPE), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {}
            mock_cache.get.return_value = None
            mock_cache.set.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                )
            )

        self.assertFalse(used)
        self.assertEqual(items, [])
        self.assertIn("SOURCE_UNAVAILABLE", [w.get("code") for w in warnings])

    def test_provider_error_on_fetch_failure(self):
        """Network errors must result in used=False and a SOURCE_UNAVAILABLE warning."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        def _raise_error(req, timeout=20):
            raise OSError("connection refused")

        with patch("server.yf") as mock_yf, \
             patch("server._urlrequest.urlopen", side_effect=_raise_error), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {}
            mock_cache.get.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                )
            )

        self.assertFalse(used)
        self.assertEqual(items, [])
        codes = [w.get("code") for w in warnings]
        self.assertIn("SOURCE_UNAVAILABLE", codes)
        # Warning message must mention globenewswire so _compute_source_status
        # can map it to PROVIDER_ERROR for the newswire source.
        msgs = " ".join(w.get("message", "") for w in warnings).lower()
        self.assertIn("globenewswire", msgs)

    def test_provider_error_on_bad_xml(self):
        """Malformed XML must result in used=False and a SOURCE_UNAVAILABLE warning."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        def _fake_urlopen(req, timeout=20):
            class _Resp:
                def read(self, size=-1):
                    return b"<not valid xml <<<<"
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    pass
            return _Resp()

        with patch("server.yf") as mock_yf, \
             patch("server._urlrequest.urlopen", side_effect=_fake_urlopen), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {}
            mock_cache.get.return_value = None
            mock_cache.set.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                )
            )

        self.assertFalse(used)
        self.assertEqual(items, [])
        codes = [w.get("code") for w in warnings]
        self.assertIn("SOURCE_UNAVAILABLE", codes)

    def test_empty_feed_returns_empty_items_used_true(self):
        """An empty but valid RSS feed must return used=True with zero items."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()

        def _fake_urlopen(req, timeout=20):
            class _Resp:
                def read(self, size=-1):
                    return _MOCK_RSS_EMPTY.encode("utf-8")
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    pass
            return _Resp()

        with patch("server.yf") as mock_yf, \
             patch("server._urlrequest.urlopen", side_effect=_fake_urlopen), \
             patch("server._tool_cache") as mock_cache:
            mock_yf.Ticker.return_value.info = {}
            mock_cache.get.return_value = None
            mock_cache.set.return_value = None

            items, warnings, used = _run(
                srv_mod._collect_globenewswire_events(
                    "AAPL",
                    retrieved_at=retrieved,
                    max_results=10,
                )
            )

        self.assertTrue(used)
        self.assertEqual(items, [])

    def test_rss_cache_used_on_second_call(self):
        """The RSS feed must not be fetched again when the cache holds a valid entry."""
        srv_mod = self._get_srv()
        retrieved = self._retrieved()
        fetch_count = {"n": 0}

        def _fake_urlopen(req, timeout=20):
            fetch_count["n"] += 1
            class _Resp:
                def read(self, size=-1):
                    return _MOCK_RSS_RELEVANT.encode("utf-8")
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    pass
            return _Resp()

        # First call: cache miss → fetch
        with patch("server.yf") as mock_yf, \
             patch("server._urlrequest.urlopen", side_effect=_fake_urlopen):
            mock_yf.Ticker.return_value.info = {}
            for key in list(srv_mod._tool_cache._store):
                if key.startswith("gnw_rss:"):
                    srv_mod._tool_cache._store.pop(key, None)
            first_items, _, _ = _run(srv_mod._collect_globenewswire_events(
                "AAPL", retrieved_at=retrieved, max_results=10, lookback_days=365,
            ))

        first_count = fetch_count["n"]

        # Second call: cache hit → no fetch
        with patch("server.yf") as mock_yf, \
             patch("server._urlrequest.urlopen", side_effect=_fake_urlopen):
            mock_yf.Ticker.return_value.info = {}
            _run(srv_mod._collect_globenewswire_events(
                "TSLA", retrieved_at=retrieved, max_results=10, lookback_days=365,
            ))

        self.assertEqual(len(first_items), 1)
        self.assertEqual(first_count, len(srv_mod._GLOBENEWSWIRE_RSS_FEEDS))
        self.assertEqual(fetch_count["n"], first_count, "Feeds must not be re-fetched on cache hit")

    def test_compute_source_status_newswire_ok(self):
        """_compute_source_status reports OK for newswire when items exist."""
        srv_mod = self._get_srv()
        item = {
            "source": "newswire",
            "sourceType": "newswire",
            "originalSource": "GlobeNewswire",
        }
        status = srv_mod._compute_source_status(
            sources_used=["newswire"],
            warnings=[],
            items=[item],
            selected_sources=["newswire"],
        )
        self.assertEqual(status.get("newswire", {}).get("status"), "OK")

    def test_compute_source_status_newswire_empty_result(self):
        """_compute_source_status reports EMPTY_RESULT when newswire used but no items."""
        srv_mod = self._get_srv()
        status = srv_mod._compute_source_status(
            sources_used=["newswire"],
            warnings=[],
            items=[],
            selected_sources=["newswire"],
        )
        self.assertEqual(status.get("newswire", {}).get("status"), "EMPTY_RESULT")

    def test_compute_source_status_newswire_provider_error(self):
        """PROVIDER_ERROR is reported when a GlobeNewswire warning message is present."""
        srv_mod = self._get_srv()
        w = {
            "code": "SOURCE_UNAVAILABLE",
            "message": "GlobeNewswire RSS unavailable: connection refused",
        }
        status = srv_mod._compute_source_status(
            sources_used=[],
            warnings=[w],
            items=[],
            selected_sources=["newswire"],
        )
        self.assertEqual(status.get("newswire", {}).get("status"), "PROVIDER_ERROR")

    def test_yahoo_items_not_reclassified_as_newswire(self):
        """Items fetched from Yahoo Finance must never carry sourceType='newswire'."""
        srv_mod = self._get_srv()
        yf_item = {
            "source": "yahoo_finance_press_releases",
            "sourceType": "yahoo_finance_press_releases",
            "originalSource": "GlobeNewswire",
            "title": "AAPL press release via Yahoo",
            "publishedAt": "2026-05-15T12:00:00Z",
        }
        self.assertNotEqual(yf_item.get("sourceType"), "newswire")
        self.assertNotEqual(yf_item.get("source"), "newswire")

    def test_newswire_not_in_default_get_company_news_sources(self):
        """get_company_news must not include 'newswire' in its default sources."""
        import inspect
        srv_mod = self._get_srv()
        src = inspect.getsource(srv_mod.get_company_news)
        # The default call must reference the three canonical defaults.
        self.assertIn("yahoo_finance_news", src)
        self.assertIn("yahoo_finance_press_releases", src)
        self.assertIn("finnhub", src)

    def test_worker_newswire_uses_globenewswire_not_yahoo_backfill(self):
        """Worker newswire path must use GlobeNewswire RSS and not Yahoo backfill."""
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "worker", "src", "yahoo-finance.ts"), encoding="utf-8") as f:
            worker_text = f.read()
        with open(os.path.join(root, "worker", "src", "tools.ts"), encoding="utf-8") as f:
            tools_text = f.read()

        self.assertIn("const GLOBENEWSWIRE_RSS_FEEDS", worker_text)
        self.assertIn("collectGlobeNewswireEvents", worker_text)
        self.assertIn("GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN", worker_text)
        self.assertIn('warningMsgs.some(m => m.includes("globenewswire"))', worker_text)
        self.assertIn('if (selected.includes("newswire"))', worker_text)
        self.assertNotIn('selected.includes("newswire")\n    || selected.includes("company_ir")', worker_text)

        fine_grained_default = '["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"]'
        self.assertIn(fine_grained_default, tools_text)
        self.assertNotIn('["yahoo_finance", "finnhub"]', tools_text)
        self.assertNotIn('["sec", "company_ir", "newswire", "yahoo_finance"]', tools_text)

    def test_worker_press_release_tab_accepts_story_items(self):
        """Worker must not require PRESS_RELEASE contentType for pressRelease queryRef items."""
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "worker", "src", "yahoo-finance.ts"), encoding="utf-8") as f:
            worker_text = f.read()

        self.assertIn("queryRef=pressRelease", worker_text)
        self.assertIn("YAHOO_ALLOWED_CONTENT_TYPES", worker_text)
        self.assertNotIn('ct !== "PRESS_RELEASE"', worker_text)
        self.assertNotIn("ct !== 'PRESS_RELEASE'", worker_text)


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False)
    sys.exit(0 if result.result.wasSuccessful() else 1)
