#!/usr/bin/env python3
"""Regression tests for quota-aware Marketaux news fallback routing."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import types
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _ensure_mcp_available() -> None:
    try:
        from mcp.server.fastmcp import FastMCP  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _FastMCPStub:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def tool(self, *args: object, **kwargs: object):  # type: ignore[return]
            if args and callable(args[0]):
                return args[0]

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
    _original_tool = _FastMCP.tool

    def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
        return _original_tool(self, name=name, **kwargs)

    _FastMCP.tool = _patched_tool  # type: ignore[method-assign]
    _FastMCP._output_schema_patched = True  # type: ignore[attr-defined]

import server as srv  # noqa: E402


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


class _JsonResponse:
    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False

    def read(self) -> bytes:
        return self._body


def _event(source: str = "yahoo_finance_news", source_type: str | None = None) -> dict:
    return {
        "title": "AEHR announces quarterly results",
        "source": source,
        "originalSource": "Example Publisher",
        "sourceType": source_type or source,
        "provider": source,
        "publishedAt": "2026-08-09T12:00:00Z",
        "retrievedAt": "2026-08-10T00:00:00Z",
        "url": "https://example.com/aehr-results",
        "issuer": "Aehr Test Systems",
        "tickers": ["AEHR"],
        "matchBasis": "TICKER_TOKEN",
        "eventType": "earnings",
        "summary": "Quarterly results",
        "evidenceText": "Quarterly results",
        "confidence": "LOW",
        "tickerRelevance": "HIGH",
        "duplicateGroupId": f"{source}-aehr",
    }


class TestMarketauxCollector(unittest.TestCase):
    def test_exact_entity_and_allowed_wire_domain_are_required(self):
        payload = {
            "data": [
                {
                    "uuid": "valid-1",
                    "title": "AEHR announces quarterly results",
                    "url": "https://www.businesswire.com/news/home/aehr-results",
                    "published_at": "2026-08-09T12:00:00Z",
                    "snippet": "Aehr Test Systems reported quarterly results.",
                    "source": "Business Wire",
                    "entities": [{"symbol": "AEHR", "name": "Aehr Test Systems", "exchange": "NASDAQ", "match_score": 98}],
                },
                {
                    "uuid": "bad-domain",
                    "title": "AEHR commentary",
                    "url": "https://example.com/aehr",
                    "published_at": "2026-08-09T11:00:00Z",
                    "entities": [{"symbol": "AEHR", "name": "Aehr Test Systems"}],
                },
                {
                    "uuid": "bad-symbol",
                    "title": "Different issuer",
                    "url": "https://www.prnewswire.com/news-releases/different-issuer.html",
                    "published_at": "2026-08-09T10:00:00Z",
                    "entities": [{"symbol": "AAPL", "name": "Apple Inc."}],
                },
                {
                    "uuid": "bad-exchange",
                    "title": "Wrong exchange issuer",
                    "url": "https://www.globenewswire.com/news-release/wrong-exchange.html",
                    "published_at": "2026-08-09T09:00:00Z",
                    "entities": [{"symbol": "AEHR", "name": "Different issuer", "exchange": "TSXV"}],
                },
            ]
        }
        requested_urls: list[str] = []

        def _urlopen(request, timeout=0):  # type: ignore[no-untyped-def]
            requested_urls.append(request.full_url)
            self.assertEqual(timeout, 12)
            return _JsonResponse(payload)

        with patch.dict(
            os.environ,
            {"MARKETAUX_API_TOKEN": "super-secret-token", "MARKETAUX_API_KEY": ""},
        ), patch("server._urlrequest.urlopen", side_effect=_urlopen):
            items, warnings, used, diagnostics = _run(srv._collect_marketaux_events(
                "AEHR",
                retrieved_at="2026-08-10T00:00:00Z",
                max_results=20,
                start_date="2026-08-01",
                end_date="2026-08-10",
                search_query="quarterly results",
                expected_exchange="NMS",
            ))

        self.assertTrue(used)
        self.assertEqual(warnings, [])
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["source"], "marketaux")
        self.assertEqual(items[0]["sourceType"], "marketaux_wire")
        self.assertEqual(items[0]["matchBasis"], "PROVIDER_ENTITY_SYMBOL")
        self.assertEqual(diagnostics["status"], "OK")
        self.assertEqual(diagnostics["rejectionCounts"], {
            "DOMAIN_NOT_ALLOWED": 1,
            "ENTITY_SYMBOL_MISMATCH": 1,
            "ENTITY_EXCHANGE_MISMATCH": 1,
        })
        self.assertNotIn("super-secret-token", diagnostics["providerUrl"])
        self.assertIn("REDACTED", diagnostics["providerUrl"])
        query = parse_qs(urlparse(requested_urls[0]).query)
        self.assertEqual(query["symbols"], ["AEHR"])
        self.assertEqual(query["filter_entities"], ["true"])
        self.assertEqual(query["limit"], ["3"])
        self.assertEqual(query["page"], ["1"])
        self.assertEqual(query["search"], ["quarterly results"])
        self.assertEqual(query["api_token"], ["super-secret-token"])
        self.assertEqual(
            query["domains"],
            ["businesswire.com,prnewswire.com,globenewswire.com"],
        )

    def test_missing_secret_is_unconfigured_without_network_call(self):
        with patch.dict(
            os.environ,
            {"MARKETAUX_API_TOKEN": "", "MARKETAUX_API_KEY": ""},
        ), patch("server._urlrequest.urlopen") as mocked_urlopen:
            items, warnings, used, diagnostics = _run(srv._collect_marketaux_events(
                "AEHR",
                retrieved_at="2026-08-10T00:00:00Z",
                max_results=10,
            ))

        self.assertEqual(items, [])
        self.assertFalse(used)
        self.assertEqual(diagnostics["status"], "UNCONFIGURED")
        self.assertFalse(diagnostics["attempted"])
        self.assertEqual(warnings[0]["source"], "marketaux")
        mocked_urlopen.assert_not_called()

    def test_provider_errors_do_not_leak_the_token(self):
        secret = "must-not-appear"
        with patch.dict(
            os.environ,
            {"MARKETAUX_API_TOKEN": secret, "MARKETAUX_API_KEY": ""},
        ), patch("server._urlrequest.urlopen", side_effect=RuntimeError(f"failure: {secret}")):
            _items, warnings, used, diagnostics = _run(srv._collect_marketaux_events(
                "AEHR",
                retrieved_at="2026-08-10T00:00:00Z",
                max_results=10,
            ))

        self.assertFalse(used)
        self.assertEqual(diagnostics["status"], "PROVIDER_ERROR")
        self.assertNotIn(secret, json.dumps({"warnings": warnings, "diagnostics": diagnostics}))

    def test_daily_quota_http_402_is_rate_limited(self):
        with patch.dict(
            os.environ,
            {"MARKETAUX_API_TOKEN": "quota-token", "MARKETAUX_API_KEY": ""},
        ), patch(
            "server._urlrequest.urlopen",
            side_effect=HTTPError("https://api.marketaux.com", 402, "quota", None, None),
        ):
            _items, warnings, used, diagnostics = _run(srv._collect_marketaux_events(
                "AEHR",
                retrieved_at="2026-08-10T00:00:00Z",
                max_results=10,
            ))

        self.assertFalse(used)
        self.assertEqual(diagnostics["status"], "RATE_LIMITED")
        self.assertIn("HTTP 402 (RATE_LIMITED)", warnings[0]["message"])


class TestMarketauxPriorityRouting(unittest.TestCase):
    def test_fallback_reason_distinguishes_normal_empty_and_limited(self):
        selected = ["yahoo_finance_news", "finnhub", "marketaux"]
        healthy = {
            "yahoo_finance_news": {"status": "OK", "completed": True},
            "finnhub": {"status": "OK", "completed": True},
        }
        self.assertIsNone(srv._marketaux_fallback_reason(
            selected,
            [_event()],
            [],
            healthy,
        ))
        self.assertEqual(srv._marketaux_fallback_reason(
            selected,
            [],
            [],
            healthy,
        ), "HIGHER_PRIORITY_EMPTY")
        self.assertIsNone(srv._marketaux_fallback_reason(
            selected,
            [_event()],
            [],
            healthy,
            "quarterly results",
        ))
        self.assertEqual(srv._marketaux_fallback_reason(
            selected,
            [_event()],
            [],
            healthy,
            "acquisition",
        ), "HIGHER_PRIORITY_EMPTY")
        self.assertEqual(srv._marketaux_fallback_reason(
            selected,
            [_event()],
            [],
            {**healthy, "finnhub": {"status": "NOT_ELIGIBLE", "completed": False}},
        ), "HIGHER_PRIORITY_COVERAGE_LIMITED")
        self.assertEqual(srv._marketaux_fallback_reason(
            ["marketaux"],
            [],
            [],
            {},
        ), "EXPLICIT_SOURCE_SELECTION")

    def test_healthy_priority_evidence_skips_marketaux(self):
        yahoo = AsyncMock(return_value=(
            [_event()],
            [],
            True,
            {"rawCount": 1, "acceptedCount": 1, "completed": True},
        ))
        finnhub = AsyncMock(return_value=(
            [],
            [],
            True,
            {"rawCount": 0, "acceptedCount": 0, "completed": True, "status": "EMPTY_RESULT"},
        ))
        marketaux = AsyncMock()
        with patch("server.yf.Ticker") as ticker_factory, patch(
            "server._collect_yahoo_events", yahoo,
        ), patch("server._collect_finnhub_events", finnhub), patch(
            "server._collect_marketaux_events", marketaux,
        ):
            ticker_factory.return_value.info = {"longName": "Aehr Test Systems"}
            result = _run(srv._collect_company_events(
                "AEHR",
                max_results=10,
                lookback_days=14,
                sources=["yahoo_finance_news", "finnhub", "marketaux"],
                include_diagnostics=True,
            ))

        items, _sources_used, warnings, _retrieved_at, diagnostics = result
        self.assertEqual(len(items), 1)
        self.assertEqual(warnings, [])
        marketaux.assert_not_awaited()
        self.assertEqual(diagnostics["marketaux"]["status"], "NOT_NEEDED")
        self.assertFalse(diagnostics["marketaux"]["attempted"])
        source_status = srv._compute_source_status(
            ["yahoo_finance_news", "finnhub"],
            [],
            items,
            ["yahoo_finance_news", "finnhub", "marketaux"],
            diagnostics,
        )
        coverage = srv._build_coverage(source_status)
        self.assertEqual(coverage["state"], "FULL")
        self.assertEqual(coverage["recommendedNextAction"], "USE_RETURNED_CONTEXT")
        self.assertEqual(coverage["skippedSources"][0]["status"], "NOT_NEEDED")

    def test_empty_priority_sources_trigger_one_marketaux_request(self):
        empty = AsyncMock(return_value=(
            [],
            [],
            True,
            {"rawCount": 0, "acceptedCount": 0, "completed": True, "status": "EMPTY_RESULT"},
        ))
        marketaux_item = _event("marketaux", "marketaux_wire")
        marketaux = AsyncMock(return_value=(
            [marketaux_item],
            [],
            True,
            {"status": "OK", "acceptedCount": 1, "completed": True},
        ))
        with patch("server.yf.Ticker") as ticker_factory, patch(
            "server._collect_yahoo_events", empty,
        ), patch("server._collect_finnhub_events", empty), patch(
            "server._collect_marketaux_events", marketaux,
        ):
            ticker_factory.return_value.info = {"longName": "Aehr Test Systems"}
            result = _run(srv._collect_company_events(
                "AEHR",
                max_results=10,
                lookback_days=14,
                sources=["yahoo_finance_news", "finnhub", "marketaux"],
                include_diagnostics=True,
            ))

        items, sources_used, _warnings, _retrieved_at, diagnostics = result
        self.assertEqual([item["source"] for item in items], ["marketaux"])
        self.assertIn("marketaux", sources_used)
        marketaux.assert_awaited_once()
        self.assertEqual(
            marketaux.await_args.kwargs["fallback_reason"],
            "HIGHER_PRIORITY_EMPTY",
        )
        self.assertEqual(diagnostics["marketaux"]["status"], "OK")

    def test_worker_has_matching_bounded_fallback_contract(self):
        worker_source = (
            Path(__file__).resolve().parents[1] / "worker" / "src" / "yahoo-finance.ts"
        ).read_text(encoding="utf-8")
        self.assertIn('const MARKETAUX_MAX_ITEMS = 3;', worker_source)
        self.assertIn('const MARKETAUX_WIRE_DOMAINS = ["businesswire.com", "prnewswire.com", "globenewswire.com"]', worker_source)
        self.assertIn('api_token: "REDACTED"', worker_source)
        self.assertIn('response.status === 402 || response.status === 429', worker_source)
        self.assertIn('reject("ENTITY_EXCHANGE_MISMATCH")', worker_source)
        self.assertIn('function marketauxFallbackReason(', worker_source)
        self.assertIn('status: "NOT_NEEDED"', worker_source)
        self.assertIn('automaticPagination: false', worker_source)


if __name__ == "__main__":
    unittest.main()
