#!/usr/bin/env python3
"""Regression tests for explicit provider-gap routing and Finnhub diagnostics."""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server as srv  # noqa: E402
from yfmcp.cache import _tool_cache  # noqa: E402
from yfmcp.clients.market_providers import ProviderJsonResult, fetch_alpha_vantage_json  # noqa: E402


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


def _parse(raw: str) -> dict:
    parsed = json.loads(raw)
    if parsed.get("ok") is True and isinstance(parsed.get("data"), dict):
        return parsed["data"]
    return parsed


class _JsonResponse:
    def __init__(self, payload: object):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit: int | None = None) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


class ProviderGapRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        _tool_cache._store.clear()

    def test_historical_ratio_requires_explicit_valid_date_before_provider(self):
        with patch("yfmcp.tools.provider_gaps.fetch_alpha_vantage_json", new_callable=AsyncMock) as provider:
            raw = _run(srv.get_historical_put_call_ratio(ticker="IBM", date="not-a-date"))
        envelope = json.loads(raw)
        self.assertTrue(envelope["error"])
        self.assertEqual(envelope["code"], "INPUT_VALIDATION_ERROR")
        self.assertIn("summarize_options_flow", envelope["message"])
        provider.assert_not_awaited()

    def test_historical_ratio_parses_verified_alpha_shape(self):
        result = ProviderJsonResult(
            payload={
                "symbol": "IBM",
                "date": "2026-03-12",
                "put_call_ratio_full_chain": "0.73",
                "put_call_ratio_by_expiration": [
                    {"date": "2026-03-20", "value": "0.44"},
                    {"date": "2026-04-17", "value": "1.2"},
                ],
            },
            status="OK",
            public_url="https://www.alphavantage.co/query?function=HISTORICAL_PUT_CALL_RATIO&apikey=REDACTED",
            provider_attempted=True,
            cache_status="MISS",
            fetched_at="2026-08-04T00:00:00+00:00",
        )
        with patch("yfmcp.tools.provider_gaps.fetch_alpha_vantage_json", new_callable=AsyncMock, return_value=result) as provider:
            data = _parse(_run(srv.get_historical_put_call_ratio(ticker="IBM", date="2026-03-12")))
        self.assertEqual(data["status"], "OK")
        self.assertEqual(data["putCallRatioFullChain"], 0.73)
        self.assertEqual(data["byExpiration"][0], {"expirationDate": "2026-03-20", "putCallRatio": 0.44})
        self.assertEqual(data["capacityClass"], "SCARCE_SHARED_QUOTA")
        self.assertFalse(data["decisionGrade"])
        provider.assert_awaited_once()

    def test_historical_ratio_rejects_provider_identity_or_date_mismatch(self):
        result = ProviderJsonResult(
            payload={
                "symbol": "MSFT",
                "date": "2026-03-11",
                "put_call_ratio_full_chain": "0.73",
            },
            status="OK",
            public_url="https://www.alphavantage.co/query?apikey=REDACTED",
            provider_attempted=True,
            cache_status="MISS",
        )
        with patch("yfmcp.tools.provider_gaps.fetch_alpha_vantage_json", new_callable=AsyncMock, return_value=result):
            data = _parse(_run(srv.get_historical_put_call_ratio(ticker="IBM", date="2026-03-12")))
        self.assertEqual(data["status"], "PROVIDER_IDENTITY_MISMATCH")
        self.assertFalse(data["providerGapFilled"])
        self.assertFalse(data["decisionGrade"])

    def test_expanded_ownership_uses_finnhub_without_alpha(self):
        finnhub = ProviderJsonResult(
            payload={"data": [{"name": "Example Capital", "share": 125000, "filingDate": "2026-06-30"}]},
            status="OK",
            public_url="https://finnhub.io/api/v1/stock/ownership?symbol=AEHR&limit=50",
            provider_attempted=True,
            cache_status="MISS",
            fetched_at="2026-08-04T00:00:00+00:00",
        )
        with patch("yfmcp.tools.provider_gaps.fetch_finnhub_json", new_callable=AsyncMock, return_value=finnhub) as finnhub_fetch, \
             patch("yfmcp.tools.provider_gaps.fetch_alpha_vantage_json", new_callable=AsyncMock) as alpha_fetch:
            data = _parse(_run(srv.get_expanded_institutional_ownership(ticker="AEHR")))
        self.assertEqual(data["source"], "finnhub")
        self.assertEqual(data["holders"][0]["holderName"], "Example Capital")
        self.assertEqual(data["dataDate"], "2026-06-30")
        self.assertFalse(data["decisionGrade"])
        finnhub_fetch.assert_awaited_once()
        alpha_fetch.assert_not_awaited()

    def test_expanded_ownership_never_spends_alpha_without_opt_in(self):
        unavailable = ProviderJsonResult(
            payload=None,
            status="ENTITLEMENT_REQUIRED",
            public_url="https://finnhub.io/api/v1/stock/ownership?symbol=AEHR&limit=50",
            provider_attempted=True,
            cache_status="MISS",
        )
        with patch("yfmcp.tools.provider_gaps.fetch_finnhub_json", new_callable=AsyncMock, return_value=unavailable), \
             patch("yfmcp.tools.provider_gaps.fetch_alpha_vantage_json", new_callable=AsyncMock) as alpha_fetch:
            data = _parse(_run(srv.get_expanded_institutional_ownership(ticker="AEHR")))
        self.assertEqual(data["status"], "ENTITLEMENT_REQUIRED")
        self.assertEqual(data["recommendedNextAction"], "USE_ALPHA_VANTAGE_EXPLICITLY")
        alpha_fetch.assert_not_awaited()

    def test_expanded_ownership_alpha_fallback_is_explicit_and_compact(self):
        finnhub = ProviderJsonResult(None, "ENTITLEMENT_REQUIRED", "https://finnhub.io/api/v1/stock/ownership", True, "MISS")
        alpha = ProviderJsonResult(
            payload={
                "symbol": "AEHR",
                "total_institutional_holders": "220",
                "total_institutional_ownership_percentage": "39.5",
                "holdings": [
                    {
                        "holder_name": "Example Fund",
                        "shares_held": "321000",
                        "shares_changed": "12000",
                        "shares_changed_percentage": "3.88",
                        "change_type": "increased",
                        "last_reported": "2026-06-30",
                    }
                ],
            },
            status="OK",
            public_url="https://www.alphavantage.co/query?function=INSTITUTIONAL_HOLDINGS&apikey=REDACTED",
            provider_attempted=True,
            cache_status="MISS",
            fetched_at="2026-08-04T00:00:00+00:00",
        )
        with patch("yfmcp.tools.provider_gaps.fetch_finnhub_json", new_callable=AsyncMock, return_value=finnhub), \
             patch("yfmcp.tools.provider_gaps.fetch_alpha_vantage_json", new_callable=AsyncMock, return_value=alpha) as alpha_fetch:
            data = _parse(_run(srv.get_expanded_institutional_ownership(ticker="AEHR", allow_scarce_fallback=True)))
        self.assertEqual(data["source"], "alpha_vantage")
        self.assertEqual(data["capacityClass"], "SCARCE_SHARED_QUOTA")
        self.assertEqual(data["aggregate"]["totalInstitutionalHolders"], 220)
        self.assertEqual(data["holders"][0]["sharesHeld"], 321000)
        self.assertEqual(len(data["providerAttempts"]), 2)
        alpha_fetch.assert_awaited_once()

    def test_alpha_client_redacts_key_and_maps_auth_error(self):
        secret = "provider-gap-secret"
        error = urllib.error.HTTPError("https://www.alphavantage.co/query", 403, "Forbidden", {}, None)
        with patch.dict(os.environ, {"ALPHA_VANTAGE_API_KEY": secret}, clear=True), \
             patch("yfmcp.clients.market_providers.urllib.request.urlopen", side_effect=error):
            result = _run(fetch_alpha_vantage_json("TEST_ONLY", {"symbol": "ZZTEST"}, ttl_seconds=60))
        self.assertEqual(result.status, "AUTH_ERROR")
        self.assertNotIn(secret, result.public_url)
        self.assertIn("REDACTED", result.public_url)

    def test_finnhub_news_counts_raw_accepted_returned_and_truncated(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        recent = int(now.timestamp())
        old = int((now - datetime.timedelta(days=60)).timestamp())
        rows = [
            {"headline": f"AEHR update {index}", "summary": "AEHR company update", "source": "Wire", "url": f"https://example.com/{index}", "datetime": recent - index}
            for index in range(5)
        ]
        rows.extend([
            {"headline": "", "summary": "missing title", "datetime": recent},
            {"headline": "AEHR old item", "summary": "old", "datetime": old},
        ])
        with patch.dict(os.environ, {"FINNHUB_API_KEY": "test-finnhub"}, clear=True), \
             patch("server._urlrequest.urlopen", return_value=_JsonResponse(rows)):
            items, warnings, used, diagnostics = _run(
                srv._collect_finnhub_events(
                    "AEHR",
                    retrieved_at=now.isoformat(),
                    max_results=2,
                    lookback_days=14,
                )
            )
        self.assertTrue(used)
        self.assertEqual(warnings, [])
        self.assertEqual(len(items), 2)
        self.assertEqual(diagnostics["rawCount"], 7)
        self.assertEqual(diagnostics["acceptedCount"], 5)
        self.assertEqual(diagnostics["collectedCount"], 2)
        self.assertEqual(diagnostics["rejectedCount"], 2)

        source_status = srv._compute_source_status(
            ["finnhub"],
            [],
            items,
            ["finnhub"],
            {"finnhub": diagnostics},
        )
        finnhub = source_status["finnhub"]
        self.assertEqual(finnhub["rawCount"], 7)
        self.assertEqual(finnhub["acceptedCount"], 5)
        self.assertEqual(finnhub["returnedCount"], 2)
        self.assertEqual(finnhub["truncatedCount"], 3)
        self.assertTrue(finnhub["hasMoreAccepted"])
        coverage = srv._build_coverage(source_status)
        self.assertEqual(coverage["state"], "FULL")
        self.assertEqual(coverage["recommendedNextAction"], "RETRY_TRUNCATED_SOURCE")
        self.assertEqual(coverage["truncatedSources"][0]["source"], "finnhub")

    def test_worker_contract_contains_equivalent_routing_and_diagnostics(self):
        root = Path(__file__).resolve().parent.parent
        worker = (root / "worker" / "src" / "yahoo-finance.ts").read_text(encoding="utf-8")
        tools = (root / "worker" / "src" / "tools.ts").read_text(encoding="utf-8")
        for marker in (
            "getHistoricalPutCallRatio",
            "getExpandedInstitutionalOwnership",
            'capacityClass: "SCARCE_SHARED_QUOTA"',
            "diagnostics.rawCount = news.length",
            "diagnostics.acceptedCount",
            "hasMoreAccepted",
            "RETRY_TRUNCATED_SOURCE",
        ):
            self.assertIn(marker, worker)
        self.assertIn('name: "get_historical_put_call_ratio"', tools)
        self.assertIn('name: "get_expanded_institutional_ownership"', tools)


if __name__ == "__main__":
    unittest.main()
