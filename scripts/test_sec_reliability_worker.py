#!/usr/bin/env python3
"""Static guards for SEC reliability fixes in the production Worker."""

from __future__ import annotations

import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
TOOLS_TS = ROOT / "worker" / "src" / "tools.ts"
YF_TS = ROOT / "worker" / "src" / "yahoo-finance.ts"


class TestSecReliabilityWorker(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tools = TOOLS_TS.read_text(encoding="utf-8")
        cls.worker = YF_TS.read_text(encoding="utf-8")

    def test_shared_filing_resolver_has_explicit_wrong_type_status(self) -> None:
        self.assertIn("async function resolveSecFiling", self.worker)
        self.assertIn("FILING_NOT_FOUND_TRY_OTHER_TYPE", self.worker)
        self.assertIn('"AUTO_20F_FALLBACK"', self.worker)
        self.assertRegex(self.worker, r"requested === \"10-K\"[\s\S]+?forms\.findIndex\(\(f\).*?\"20-F\"")

    def test_geo_extraction_distinguishes_parser_failure(self) -> None:
        self.assertIn('"EXTRACTION_FAILED"', self.worker)
        self.assertIn('"TABLE_NOT_PARSED"', self.worker)
        self.assertRegex(self.worker, r"filingHasRelevantGeoText\(htmlText, regionText\)[\s\S]+?EXTRACTION_FAILED")
        self.assertRegex(self.worker, r"function normalizeStatus[\s\S]+?FILING_NOT_FOUND_TRY_OTHER_TYPE[\s\S]+?EXTRACTION_FAILED")

    def test_table_listing_is_paginated_and_index_backed(self) -> None:
        self.assertRegex(self.tools, r'name: "list_sec_filing_tables"[\s\S]+offset[\s\S]+limit')
        self.assertIn("function secIndexTablesPayload", self.tools)
        self.assertRegex(self.tools, r"case \"list_sec_filing_tables\"[\s\S]+getSecFilingIndex[\s\S]+secIndexTablesPayload")
        self.assertRegex(self.worker, r"export async function listFilingTables\(ticker: string, documentUrl: string, offset: number = 0, limit: number = 50\)")
        self.assertNotRegex(self.worker, r"while \(\(tm = tableRe\.exec\(html\)\) !== null && tables\.length < 50\)")

    def test_search_uses_primary_html_not_xbrl(self) -> None:
        self.assertRegex(self.tools, r'name: "search_sec_filing_text"[\s\S]+document_url')
        self.assertIn("isLikelyXbrlDocumentUrl", self.worker)
        self.assertIn("DOCUMENT_URL_REPLACED_WITH_PRIMARY_HTML", self.worker)
        self.assertIn('documentKind: "primary_html"', self.worker)
        self.assertIn("FILING_TEXT_NOT_AVAILABLE", self.worker)

    def test_earnings_ex99_text_is_period_resolved_and_non_decision_grade(self) -> None:
        self.assertIn("function extractEarningsPeriodFromText", self.worker)
        self.assertIn('periodStatus: "EX99_TEXT_RESOLVED"', self.worker)
        self.assertIn("async function resolveEarningsPeriodFromSource", self.worker)
        self.assertIn("function extractReportedTextMetric", self.worker)
        self.assertIn("EX99_TEXT_CONTEXT", self.worker)
        self.assertIn("EX99_IXBRL_UNSCOPED", self.worker)
        self.assertIn("TEXT_METRIC_VERIFY_REQUIRED", self.worker)
        self.assertIn("EPS_NEAR_ZERO_ESTIMATE_BASE", self.worker)
        self.assertRegex(
            self.worker,
            r"String\(f\.filed \?\? \"\"\) >= releaseFilingDate",
        )
        self.assertNotIn("function deriveFiscalPeriod", self.worker)


if __name__ == "__main__":
    unittest.main(verbosity=2)
