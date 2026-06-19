#!/usr/bin/env python3
"""Offline tests for the EdgarTools SEC facts sidecar normalizer."""

from __future__ import annotations

import unittest

from yfmcp.sec_facts_sidecar import ExposureRequest, normalize_exposure_facts


class TestSecFactsSidecar(unittest.TestCase):
    def test_normalizes_dimensional_revenue_pct(self) -> None:
        req = ExposureRequest(ticker="AAPL", topic="Greater China")
        rows = [
            {
                "concept": "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
                "label": "Greater China net sales",
                "value": 70_000_000_000,
                "units": "USD",
                "period_end": "2025-09-27",
                "srt:StatementGeographicalAxis": "GreaterChinaMember",
            },
            {
                "concept": "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
                "label": "Net sales",
                "value": 400_000_000_000,
                "units": "USD",
                "period_end": "2025-09-27",
            },
        ]
        meta = {
            "filingType": "10-K",
            "filingDate": "2025-10-31",
            "fiscalYear": "FY2025",
            "accessionNumber": "0000320193-25-000079",
            "documentUrl": "https://www.sec.gov/Archives/edgar/data/320193/x/aapl.htm",
        }
        result = normalize_exposure_facts(req, rows, meta)
        self.assertEqual(result["status"], "FOUND")
        self.assertEqual(result["value"], 70_000_000_000)
        self.assertEqual(result["valuePct"], 17.5)
        self.assertEqual(result["evidence"]["accessionNumber"], meta["accessionNumber"])

    def test_no_dimensional_match_is_explicit(self) -> None:
        req = ExposureRequest(ticker="AAPL", topic="Atlantis")
        result = normalize_exposure_facts(req, [], {"filingType": "10-K"})
        self.assertEqual(result["status"], "NO_DIMENSIONAL_REVENUE_FACT")
        self.assertIsNone(result["value"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

