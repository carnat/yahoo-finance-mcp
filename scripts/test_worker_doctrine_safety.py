#!/usr/bin/env python3
"""Static guards for Worker doctrine safety envelope semantics."""

from __future__ import annotations

import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
RESPONSE_TS = ROOT / "worker" / "src" / "response.ts"
TOOLS_TS = ROOT / "worker" / "src" / "tools.ts"
YF_TS = ROOT / "worker" / "src" / "yahoo-finance.ts"
DEPLOYED_DISCOVERY = ROOT / "scripts" / "test_deployed_discovery.py"


class TestWorkerDoctrineSafety(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.response = RESPONSE_TS.read_text(encoding="utf-8")
        cls.tools = TOOLS_TS.read_text(encoding="utf-8")
        cls.worker = YF_TS.read_text(encoding="utf-8")
        cls.discovery = DEPLOYED_DISCOVERY.read_text(encoding="utf-8")

    def test_mcp_success_propagates_inner_failure_envelopes(self) -> None:
        self.assertIn('"ok" in parsed', self.response)
        self.assertIn('"data" in parsed', self.response)
        self.assertIn("ok: inner.ok === true", self.response)
        self.assertIn("error: inner.ok === true ? null", self.response)
        self.assertRegex(self.response, r"for \(const key of \[\"diagnostics\"\]\)")

    def test_doctrine_quarantine_manifest_is_exposed(self) -> None:
        for tool in (
            "get_overnight_quote",
            "get_sec_filing_section_markdown",
            "get_company_press_releases",
            "query_sec_filing_index",
            "extract_sec_filing_fact",
        ):
            self.assertIn(tool, self.tools)
        for field in (
            "capabilityStatus",
            "decisionGrade",
            "doctrineUse",
            "failureMode",
            "evidenceRequired",
            "sourceType",
            "doctrineToolStatus",
        ):
            self.assertIn(field, self.tools)

    def test_overnight_provider_failures_are_failure_envelopes(self) -> None:
        self.assertIn('mcpFailure("get_overnight_quote"', self.worker)
        self.assertIn("PROVIDER_FORBIDDEN", self.worker)
        self.assertIn("DIAGNOSTICS_ONLY", self.tools)
        self.assertIn("provider_diagnostic", self.tools)

    def test_deployed_smoke_blocks_double_envelope_regression(self) -> None:
        self.assertIn("assert_not_double_enveloped_failure", self.discovery)
        self.assertIn("UNSUPPORTED_QUERY_TYPE", self.discovery)
        self.assertIn("doctrineToolStatus", self.discovery)
        self.assertIn("no approval received", self.discovery)


if __name__ == "__main__":
    unittest.main(verbosity=2)
