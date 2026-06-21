#!/usr/bin/env python3
"""Static guards for Worker official SEC structured-facts routing."""

from __future__ import annotations

import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
TOOLS_TS = ROOT / "worker" / "src" / "tools.ts"


class TestWorkerSecSidecar(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tools = TOOLS_TS.read_text(encoding="utf-8")

    def test_structured_extractors_route_to_official_sec_provider(self) -> None:
        self.assertIn("official_sec_data_api", self.tools)
        self.assertIn("data.sec.gov", self.tools)
        self.assertIn("companyfacts", self.tools)
        self.assertIn("STRUCTURED_FACT_PROVIDER_UNCONFIGURED", self.tools)
        self.assertIn("STRUCTURED_FACT_PROVIDER_UNAVAILABLE", self.tools)
        for tool in (
            "extract_geographic_revenue",
            "extract_segment_revenue",
            "extract_total_revenue",
            "extract_revenue_exposure",
            "extract_china_exposure",
            "extract_exposure",
        ):
            self.assertRegex(
                self.tools,
                rf'case "{tool}":[\s\S]*?return callStructuredFactsProvider\(name, args\);',
            )

    def test_diagnostics_are_exposed_for_live_gate(self) -> None:
        for field in (
            "structuredFactProvider",
            "structuredFactProviderConfigured",
            "structuredFactProviderHealth",
            "structuredFactProviderLastSmokeStatus",
            "structuredFactProviderLastErrorCode",
        ):
            self.assertIn(field, self.tools)

    def test_no_sidecar_url_dependency(self) -> None:
        self.assertNotIn("EDGAR_FACTS_URL", self.tools)
        self.assertNotIn("/sec/facts/exposure", self.tools)

    def test_structured_tools_do_not_fall_back_to_legacy_parser(self) -> None:
        self.assertNotRegex(
            self.tools,
            r'case "extract_geographic_revenue":[\s\S]*?extractGeographicRevenue\(',
        )
        self.assertNotRegex(
            self.tools,
            r'case "extract_revenue_exposure":[\s\S]*?extractRevenueExposure\(',
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
