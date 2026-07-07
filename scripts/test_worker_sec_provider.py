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

    def test_extract_exposure_ignores_empty_risk_evidence(self) -> None:
        match = re.search(
            r"// --- Process risk factor exposure ---[\s\S]*?// --- Determine overallStatus ---",
            self.worker,
        )
        self.assertIsNotNone(match)
        section = match.group(0)
        self.assertIn(".filter((m) => m.excerpt.length > 0)", section)

    def test_compact_geo_extraction_does_not_build_full_filing_index(self) -> None:
        match = re.search(
            r"export async function extractGeographicRevenue\([\s\S]*?return JSON\.stringify\(out\);",
            self.worker,
        )
        self.assertIsNotNone(match)
        section = match.group(0)
        self.assertIn('const needsIndex = detail === "raw"', section)
        self.assertIn("needsIndex ? parseObjectJson(await getSecFilingIndex", section)

    def test_sec_text_search_is_bounded_for_worker_cpu(self) -> None:
        match = re.search(
            r"export async function searchFilingText\([\s\S]*?return JSON\.stringify\(\{[\s\S]*?\n  \}\);",
            self.worker,
        )
        self.assertIsNotNone(match)
        section = match.group(0)
        self.assertIn("full-filing text conversion can exhaust Worker CPU", section)
        self.assertIn("const htmlLower = html.toLowerCase()", section)
        self.assertIn("isHtmlTagPosition(html, pos)", section)
        self.assertIn("htmlWindowAtTagBoundaries(html", section)
        self.assertNotIn("const readableText = cleanFilingDisplayText(htmlToReadableText", section)

    def test_geo_fallback_avoids_full_document_strip(self) -> None:
        self.assertIn("function filingHasRelevantGeoText", self.worker)
        self.assertNotIn("stripHtmlTags(htmlText).toLowerCase()", self.worker)

    def test_ownership_holder_schema_advertises_supported_types(self) -> None:
        self.assertIn("export const SUPPORTED_HOLDER_TYPES", self.worker)
        self.assertIn("supportedHolderTypes: SUPPORTED_HOLDER_TYPES", self.worker)
        self.assertIn('"INPUT_VALIDATION_ERROR"', self.worker)
        match = re.search(
            r'name: "get_ownership_holders"[\s\S]*?required: \["ticker", "holder_type"\]',
            self.tools,
        )
        self.assertIsNotNone(match)
        schema = match.group(0)
        self.assertIn("enum: SUPPORTED_HOLDER_TYPES", schema)
        for holder_type in (
            "major_holders",
            "institutional_holders",
            "mutualfund_holders",
            "insider_transactions",
            "insider_purchases",
            "insider_roster_holders",
        ):
            self.assertIn(holder_type, schema)

    def test_sec_filing_outline_uses_index_fallback_and_explicit_empty_status(self) -> None:
        self.assertIn("function secIndexOutlinePayload", self.tools)
        self.assertIn('"OUTLINE_NOT_PARSED"', self.tools)
        self.assertIn('"TABLES_FOUND_OUTLINE_EMPTY"', self.tools)
        dispatch = re.search(
            r'case "get_sec_filing_outline":[\s\S]*?case "get_sec_filing_section"',
            self.tools,
        )
        self.assertIsNotNone(dispatch)
        section = dispatch.group(0)
        self.assertIn("getSecFilingIndex(", section)
        self.assertIn("secIndexOutlinePayload(idx)", section)
        self.assertIn("getFilingOutline(", section)
        self.assertIn('"OUTLINE_NOT_PARSED"', self.worker)
        self.assertIn('"TABLES_FOUND_OUTLINE_EMPTY"', self.worker)


if __name__ == "__main__":
    unittest.main(verbosity=2)
