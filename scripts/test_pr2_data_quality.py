#!/usr/bin/env python3
"""Offline regressions for PR2 deterministic data-source fixes."""

import asyncio
import json
import os
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402

_orig_tool = _FastMCP.tool


def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
    return _orig_tool(self, name=name, **kwargs)


_FastMCP.tool = _patched_tool  # type: ignore[method-assign]

import server as srv  # noqa: E402
from yfmcp.tools import pricing  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


class _FastInfo(dict):
    currency = "USD"
    last_price = 50.0


class TestPr2DataQuality(unittest.TestCase):
    def tearDown(self):
        srv.yf.Ticker = self._server_ticker  # type: ignore[attr-defined]
        pricing.yf.Ticker = self._pricing_ticker  # type: ignore[attr-defined]

    def setUp(self):
        self._server_ticker = srv.yf.Ticker
        self._pricing_ticker = pricing.yf.Ticker

    def test_historical_prices_use_camel_case_rows(self):
        class FakeTicker:
            fast_info = _FastInfo()

            def history(self, period, interval, prepost=False):
                return pd.DataFrame(
                    {
                        "Open": [1.0],
                        "High": [2.0],
                        "Low": [0.5],
                        "Close": [1.5],
                        "Volume": [100],
                        "Adj Close": [1.4],
                    },
                    index=pd.DatetimeIndex(["2026-06-12"]),
                )

        pricing.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        rows = json.loads(_run(pricing.get_historical_stock_prices("TSTPR2HIST")))
        self.assertEqual(
            set(rows[0]),
            {"date", "open", "high", "low", "close", "volume", "adjClose"},
        )

    def test_options_summary_rejects_invalid_expiry_hint(self):
        class FakeTicker:
            options = ["2026-06-18"]

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_options_summary("ASTS", expiry_hint="2026-06-19")))
        self.assertTrue(data["error"])
        self.assertEqual(data["code"], "INVALID_EXPIRY_DATE")
        self.assertEqual(data["nearestExpiration"], "2026-06-18")

    def test_credit_health_splits_ebit_and_ebitda_coverage(self):
        col = pd.Timestamp("2026-03-31")

        class FakeTicker:
            quarterly_balance_sheet = pd.DataFrame(
                {col: [100.0, 20.0]},
                index=["Total Debt", "Cash And Cash Equivalents"],
            )
            quarterly_income_stmt = pd.DataFrame(
                {col: [40.0, 20.0, -5.0]},
                index=["EBITDA", "EBIT", "Interest Expense"],
            )

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_credit_health("TST")))
        self.assertEqual(data["interestCoverage"], 4.0)
        self.assertEqual(data["interestCoverageEbit"], 4.0)
        self.assertEqual(data["interestCoverageEbitda"], 8.0)

    def test_recent_revision_signal_beats_negative_90d_context(self):
        class FakeTicker:
            fast_info = _FastInfo()
            eps_trend = pd.DataFrame(
                {
                    "period": ["0q"],
                    "current": [1.05],
                    "7daysAgo": [1.0],
                    "30daysAgo": [1.0],
                    "90daysAgo": [1.8],
                }
            )
            earnings_history = pd.DataFrame(
                {
                    "epsActual": [1.2, 1.1, 1.0, 0.9],
                    "epsEstimate": [1.0, 1.0, 0.9, 0.8],
                    "surprisePercent": [0.2, 0.1, 0.11, 0.12],
                }
            )

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.get_earnings_momentum("NBIS")))
        self.assertEqual(data["forwardRevisionSignal"], "POSITIVE")
        self.assertNotEqual(data["compositeMomentumSignal"], "NEGATIVE")
        self.assertIn("90d only as fallback/context", data["compositeMethodNote"])

    def test_price_target_distance_exposes_inferred_tag_and_deprecated_tag(self):
        class FakeTicker:
            fast_info = {"lastPrice": 100.0}

            def history(self, period, interval):
                return pd.DataFrame(index=pd.DatetimeIndex(["2026-06-12"]))

        srv.yf.Ticker = lambda ticker: FakeTicker()  # type: ignore[assignment]
        data = json.loads(_run(srv.calculate_price_target_distance("ASTS", reference_target_price=120.0)))
        self.assertEqual(data["inferredTag"], "NEAR")
        self.assertEqual(data["tag"], "NEAR")
        self.assertIn("Deprecated", data["tagNote"])

    def test_event_dedupe_ignores_source_url_and_keeps_refs(self):
        title = "Coherent and Lumentum Stocks Have Tumbled. Why Now Is the Time to Buy the Dip"
        gid1 = srv._make_duplicate_group_id("COHR", title, "2026-06-10T12:00:00Z", None, "https://a.example/x")
        gid2 = srv._make_duplicate_group_id("COHR", title, "2026-06-10T12:30:00Z", None, "https://b.example/y")
        self.assertEqual(gid1, gid2)
        deduped = srv._dedupe_event_items([
            {"title": title, "publishedAt": "2026-06-10T12:00:00Z", "sourceType": "company_news", "source": "finnhub", "duplicateGroupId": gid1, "url": "https://a.example/x"},
            {"title": title, "publishedAt": "2026-06-10T12:30:00Z", "sourceType": "yahoo_finance_news", "source": "yahoo_finance_news", "duplicateGroupId": gid2, "url": "https://b.example/y"},
        ], [])
        self.assertEqual(len(deduped), 1)
        self.assertEqual(len(deduped[0]["sourceRefs"]), 1)

    def test_form4_xml_parser_extracts_transaction(self):
        xml = """
        <ownershipDocument>
          <reportingOwner><reportingOwnerId><rptOwnerName>Jane Doe</rptOwnerName></reportingOwnerId>
          <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship></reportingOwner>
          <nonDerivativeTransaction>
            <transactionDate><value>2026-06-10</value></transactionDate>
            <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
            <transactionAmounts>
              <transactionShares><value>1000</value></transactionShares>
              <transactionPricePerShare><value>12.50</value></transactionPricePerShare>
            </transactionAmounts>
            <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
          </nonDerivativeTransaction>
        </ownershipDocument>
        """
        parsed = srv._parse_form4_transaction(xml)
        self.assertEqual(parsed["owner"], "Jane Doe")
        self.assertEqual(parsed["transactionLabel"], "Purchase")
        self.assertEqual(parsed["value"], 12500.0)

    def test_form4_transformed_html_parser_extracts_transaction(self):
        html = """
        <html><body>
          <table width="100%" border="1">
            <tr>
              <td rowspan="4">
                <span class="MedSmallFormText">1. Name and Address of Reporting Person</span>
                <table><tr><td><a href="/cgi-bin/browse-edgar?action=getcompany&amp;CIK=1">LEVINSON ARTHUR D</a></td></tr></table>
              </td>
              <td>
                <span class="MedSmallFormText">2. Issuer Name and Ticker or Trading Symbol</span>
                <br>Apple Inc. [ <span class="FormData">AAPL</span> ]
              </td>
              <td rowspan="2">
                <span class="MedSmallFormText">5. Relationship of Reporting Person(s) to Issuer</span>
                <table>
                  <tr><td><span class="FormData">X</span></td><td>Director</td><td></td><td>10% Owner</td></tr>
                  <tr><td></td><td>Officer (give title below)</td><td></td><td>Other (specify below)</td></tr>
                </table>
              </td>
            </tr>
            <tr><td><span class="MedSmallFormText">6. Individual or Joint/Group Filing</span></td></tr>
          </table>
          <table width="100%" border="1" cellspacing="0" cellpadding="4">
            <thead>
              <tr><th colspan="11">Table I - Non-Derivative Securities Acquired, Disposed of, or Beneficially Owned</th></tr>
              <tr><th>1. Title of Security</th><th>2. Transaction Date</th><th>2A.</th><th colspan="2">3. Transaction Code</th><th colspan="3">4. Securities Acquired (A) or Disposed Of (D)</th><th>5. Amount Owned</th><th>6. Ownership Form</th><th>7. Nature</th></tr>
              <tr><th></th><th></th><th></th><th>Code</th><th>V</th><th>Amount</th><th>(A) or (D)</th><th>Price</th><th></th><th></th><th></th></tr>
            </thead>
            <tbody>
              <tr>
                <td><span class="FormData">Common Stock</span></td>
                <td><span class="FormData">05/27/2026</span></td>
                <td></td>
                <td><span class="SmallFormData">S</span></td>
                <td></td>
                <td><span class="FormData">50,000</span></td>
                <td><span class="FormData">D</span></td>
                <td><span class="FormText">$</span><span class="FormData">311.02</span><span class="FootnoteData"><sup>(1)</sup></span></td>
                <td><span class="FormData">3,764,576</span></td>
                <td><span class="FormData">D</span></td>
                <td></td>
              </tr>
            </tbody>
          </table>
        </body></html>
        """
        parsed = srv._parse_form4_transaction(html)
        self.assertEqual(parsed["owner"], "LEVINSON ARTHUR D")
        self.assertEqual(parsed["role"], "director")
        self.assertEqual(parsed["transactionCode"], "S")
        self.assertEqual(parsed["transactionLabel"], "Sale")
        self.assertEqual(parsed["shares"], 50000.0)
        self.assertEqual(parsed["price"], 311.02)
        self.assertEqual(parsed["value"], 15551000.0)
        self.assertEqual(parsed["ownershipForm"], "D")
        self.assertEqual(parsed["transactionDate"], "2026-05-27")

    def test_china_exposure_evidence_has_excerpt_availability(self):
        async def fake_index(**kwargs):
            return json.dumps({
                "filingType": "10-K",
                "index": {
                    "sections": [{"heading": "China supply chain and manufacturing risks"}],
                    "tables": [{"title": "Bank of China credit facility", "rowLabels": ["Bank of China"], "tableId": "t1"}],
                },
            })

        async def fake_revenue(**kwargs):
            return json.dumps({"status": "NOT_FOUND", "matches": []})

        async def fake_risk(**kwargs):
            return json.dumps({"status": "FOUND", "matches": [{"term": "China", "excerpt": ""}]})

        old_index = srv.get_sec_filing_index
        old_revenue = srv.extract_revenue_exposure
        old_risk = srv.extract_risk_factor_mentions
        try:
            srv.get_sec_filing_index = fake_index
            srv.extract_revenue_exposure = fake_revenue
            srv.extract_risk_factor_mentions = fake_risk
            data = json.loads(_run(srv.extract_china_exposure("MRVL")))
        finally:
            srv.get_sec_filing_index = old_index
            srv.extract_revenue_exposure = old_revenue
            srv.extract_risk_factor_mentions = old_risk
        entity_evidence = data["manufacturingExposure"]["evidence"][0]
        risk_evidence = data["riskFactorExposure"]["evidence"][0]
        self.assertTrue(entity_evidence["excerptAvailable"])
        self.assertFalse(risk_evidence["excerptAvailable"])
        self.assertNotIn("excerpt", risk_evidence)


if __name__ == "__main__":
    unittest.main(verbosity=2)
