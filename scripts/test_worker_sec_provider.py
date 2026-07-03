#!/usr/bin/env python3
"""Static guards for Worker official SEC structured-facts routing."""

from __future__ import annotations

import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
TOOLS_TS = ROOT / "worker" / "src" / "tools.ts"
YAHOO_TS = ROOT / "worker" / "src" / "yahoo-finance.ts"


class TestWorkerSecProvider(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tools = TOOLS_TS.read_text(encoding="utf-8")
        cls.worker = YAHOO_TS.read_text(encoding="utf-8")

    def test_structured_extractors_route_to_worker_local_sec_extractors(self) -> None:
        self.assertIn("official_sec_data_api", self.tools)
        self.assertIn("data.sec.gov", self.tools)
        self.assertIn("companyfacts", self.tools)
        self.assertIn("STRUCTURED_FACT_PROVIDER_UNCONFIGURED", self.tools)
        self.assertIn("STRUCTURED_FACT_PROVIDER_UNAVAILABLE", self.tools)
        expected_routes = {
            "extract_geographic_revenue": "extractGeographicRevenue(",
            "extract_segment_revenue": "extractSegmentRevenue(",
            "extract_total_revenue": "extractTotalRevenue(",
            "extract_revenue_exposure": "extractRevenueExposure(",
            "extract_china_exposure": "extractChinaExposure(",
            "extract_exposure": "extractExposure(",
        }
        for tool, target in expected_routes.items():
            self.assertRegex(self.tools, rf'case "{tool}":[\s\S]*?return {re.escape(target)}')

    def test_diagnostics_are_exposed_for_live_gate(self) -> None:
        for field in (
            "structuredFactProvider",
            "structuredFactProviderConfigured",
            "structuredFactProviderHealth",
            "structuredFactProviderLastSmokeStatus",
            "structuredFactProviderLastErrorCode",
        ):
            self.assertIn(field, self.tools)

    def test_no_external_python_service_dependency(self) -> None:
        self.assertNotIn("EDGAR_FACTS_URL", self.tools)
        self.assertNotIn("/sec/facts/exposure", self.tools)

    def test_public_tools_do_not_route_through_sidecar_style_gate(self) -> None:
        dispatch = re.search(
            r'case "extract_geographic_revenue":[\s\S]*?case "extract_risk_factor_mentions"',
            self.tools,
        )
        self.assertIsNotNone(dispatch)
        self.assertNotIn("callStructuredFactsProvider(name, args)", dispatch.group(0))

    def test_extract_exposure_uses_shared_revenue_exposure_path(self) -> None:
        match = re.search(
            r"export async function extractExposure\([\s\S]*?// 2\. Operational/entity scan",
            self.worker,
        )
        self.assertIsNotNone(match)
        section = match.group(0)
        self.assertIn("extractRevenueExposure(ticker, topic", section)
        self.assertNotIn("extractGeographicRevenue(ticker, regionLabel", section)


if __name__ == "__main__":
    unittest.main(verbosity=2)
