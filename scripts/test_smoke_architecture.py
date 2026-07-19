#!/usr/bin/env python3
"""Regression guards for the deployed smoke-test gate architecture."""

from __future__ import annotations

import copy
import io
import pathlib
import re
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts import test_deployed_canaries as deployed_canaries


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy-worker.yml"
REMOVED_SEC_SMOKE = ROOT / "scripts" / "test_deployed_sec_facts_provider.py"


class TestSmokeArchitecture(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")

    def _step_body(self, name: str) -> str:
        match = re.search(
            rf"(?ms)^      - name: {re.escape(name)}\n(?P<body>.*?)(?=^      - name:|\Z)",
            self.workflow,
        )
        self.assertIsNotNone(match, f"missing workflow step: {name}")
        return match.group("body")

    def test_registry_canaries_are_the_only_blocking_live_suite(self) -> None:
        contract = self._step_body("Verify deployed MCP contract canaries")
        self.assertNotIn("continue-on-error: true", contract)

        advisory_steps = (
            "Audit deployed MCP live discovery",
            "Audit alias behavior",
            "Audit geographic revenue schema",
            "Audit deployed extractor tools",
            "Audit deployed MCP grouped discovery",
            "Audit deployed grouped extractor tools",
            "Audit end-to-end tool tests",
        )
        for name in advisory_steps:
            with self.subTest(step=name):
                body = self._step_body(name)
                self.assertIn("continue-on-error: true", body)
                self.assertIn("always()", body)

    def test_obsolete_provider_configuration_probe_is_removed(self) -> None:
        self.assertFalse(REMOVED_SEC_SMOKE.exists())
        self.assertNotIn("test_deployed_sec_facts_provider.py", self.workflow)
        self.assertNotIn("EDGAR_FACTS_LAST_SMOKE_STATUS", self.workflow)

    def test_production_deploys_are_serialized_and_bounded(self) -> None:
        self.assertIn("group: deploy-worker-production", self.workflow)
        self.assertIn("cancel-in-progress: false", self.workflow)
        self.assertRegex(self.workflow, r"(?ms)^  deploy:\n.*?^    timeout-minutes: 20$")
        self.assertRegex(self.workflow, r"(?ms)^  smoke-test:\n.*?^    timeout-minutes: 45$")

    def test_registry_contains_decision_grade_xbrl_canary(self) -> None:
        canaries = deployed_canaries.validate_registry(deployed_canaries.load_registry())
        by_id = {item["id"]: item for item in canaries}
        self.assertIn("sec-xbrl-decision-grade", by_id)
        canary = by_id["sec-xbrl-decision-grade"]
        self.assertEqual(canary["tool"], "extract_sec_filing_fact")
        self.assertEqual(canary["assertion"], "sec_xbrl_decision_grade")

    def test_xbrl_canary_requires_source_evidence_not_private_health(self) -> None:
        payload = {
            "ok": True,
            "data": {
                "status": "FOUND",
                "value": 1000,
                "extractionMethod": "XBRL",
                "decisionGrade": True,
                "documentUrl": "https://www.sec.gov/Archives/example.htm",
                "xbrlContext": {
                    "concept": "RevenueFromContractWithCustomerExcludingAssessedTax",
                    "periodEnd": "2025-09-27",
                },
                "sourceEvidence": {
                    "sourceType": "sec_xbrl_companyconcept",
                    "concept": "RevenueFromContractWithCustomerExcludingAssessedTax",
                    "accessionNumber": "0000320193-25-000079",
                    "periodEnd": "2025-09-27",
                    "documentUrl": "https://www.sec.gov/Archives/example.htm",
                },
            },
        }
        deployed_canaries.sec_xbrl_decision_grade(payload, {})

        missing_evidence = copy.deepcopy(payload)
        missing_evidence["data"]["sourceEvidence"] = None
        with self.assertRaisesRegex(AssertionError, "context/source evidence"):
            deployed_canaries.sec_xbrl_decision_grade(missing_evidence, {})

    def test_canary_runner_reports_all_contract_failures(self) -> None:
        def always_fail(_payload: dict, canary: dict) -> None:
            raise AssertionError(f"failed {canary['id']}")

        canaries = [
            {"id": "first", "tool": "one", "args": {}, "assertion": "always_fail"},
            {"id": "second", "tool": "two", "args": {}, "assertion": "always_fail"},
        ]
        with redirect_stdout(io.StringIO()):
            with (
                patch.object(deployed_canaries, "call_tool", return_value={}) as call,
                patch.dict(deployed_canaries.ASSERTIONS, {"always_fail": always_fail}),
                self.assertRaises(AssertionError) as raised,
            ):
                deployed_canaries.run_canaries(canaries)

        self.assertEqual(call.call_count, 2)
        self.assertIn("first: failed first", str(raised.exception))
        self.assertIn("second: failed second", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
