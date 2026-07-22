#!/usr/bin/env python3
"""Regression guards for the deployed smoke-test gate architecture."""

from __future__ import annotations

import copy
import io
import json
import pathlib
import re
import tempfile
import tomllib
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts import test_deployed_canaries as deployed_canaries
from scripts import worker_version_promotion as promotion


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy-worker.yml"
REMOVED_SEC_SMOKE = ROOT / "scripts" / "test_deployed_sec_facts_provider.py"
WORKER_CONFIG = ROOT / "worker" / "wrangler.toml"
WORKER_ENTRY = ROOT / "worker" / "src" / "index.ts"


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

    def test_only_identity_and_registry_canaries_block_live_workflow(self) -> None:
        blocking_steps = (
            "Verify candidate version identity",
            "Verify candidate MCP contract canaries",
            "Verify production version identity",
            "Verify production MCP contract canaries",
        )
        for name in blocking_steps:
            with self.subTest(step=name):
                self.assertNotIn("continue-on-error: true", self._step_body(name))

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
        expected_timeouts = {
            "upload_version": 20,
            "candidate_smoke": 30,
            "promote_version": 10,
            "production_smoke": 45,
        }
        for job, timeout in expected_timeouts.items():
            with self.subTest(job=job):
                self.assertRegex(
                    self.workflow,
                    rf"(?ms)^  {job}:\n.*?^    timeout-minutes: {timeout}$",
                )

    def test_candidate_is_verified_before_exact_version_promotion(self) -> None:
        self.assertNotIn("npx wrangler deploy", self.workflow)
        self.assertNotIn("wrangler secret put", self.workflow)
        self.assertIn("wrangler versions upload", self.workflow)
        self.assertIn('--secrets-file "$RUNNER_TEMP/worker-secrets.json"', self.workflow)
        self.assertIn("WRANGLER_OUTPUT_FILE_PATH:", self.workflow)
        self.assertRegex(
            self.workflow,
            r"(?ms)^  promote_version:\n.*?^    needs: \[upload_version, candidate_smoke\]$",
        )
        self.assertRegex(
            self.workflow,
            r"(?ms)^  production_smoke:\n.*?^    needs: \[upload_version, promote_version\]$",
        )
        self.assertNotIn("sleep 30", self.workflow)
        promote = self._step_body("Promote exact candidate version")
        self.assertIn('"${VERSION_ID}@100%"', promote)
        self.assertIn("needs.upload_version.outputs.version_id", promote)

        candidate = self._step_body("Verify candidate MCP contract canaries")
        self.assertIn("needs.upload_version.outputs.preview_url", candidate)

    def test_version_upload_uses_only_supported_wrangler_flags(self) -> None:
        upload_step = self._step_body("Upload immutable candidate version")
        wrangler_command = upload_step.split("python ../scripts/worker_version_promotion.py", 1)[0]
        self.assertEqual(
            set(re.findall(r"--[a-z-]+", wrangler_command)),
            {"--secrets-file", "--preview-alias", "--tag", "--message"},
        )

    def test_candidate_secrets_are_atomic_and_ephemeral(self) -> None:
        secrets = self._step_body("Build candidate secrets file")
        self.assertIn("worker_version_promotion.py write-secrets", secrets)
        cleanup = self._step_body("Remove candidate secrets file")
        self.assertIn("always()", cleanup)
        self.assertIn("rm -f", cleanup)

    def test_runtime_exposes_immutable_version_identity(self) -> None:
        config = WORKER_CONFIG.read_text(encoding="utf-8")
        entry = WORKER_ENTRY.read_text(encoding="utf-8")
        parsed_config = tomllib.loads(config)
        self.assertIn("preview_urls = true", config)
        self.assertIn("[version_metadata]", config)
        self.assertIn('binding = "CF_VERSION_METADATA"', config)
        self.assertEqual(parsed_config["version_metadata"]["binding"], "CF_VERSION_METADATA")
        self.assertIn("workerVersionId: env.CF_VERSION_METADATA?.id ?? null", entry)

    def test_version_upload_parser_uses_structured_wrangler_output(self) -> None:
        events = [
            {"type": "wrangler-session", "version": 1},
            {
                "type": "version-upload",
                "version_id": "11111111-2222-3333-4444-555555555555",
                "preview_urls": [
                    "https://11111111-yahoo-finance-mcp.artinatw.workers.dev",
                    "https://candidate-123-1-yahoo-finance-mcp.artinatw.workers.dev",
                ],
            },
        ]
        version_id, preview_url = promotion.parse_version_upload(
            events, "candidate-123-1", "yahoo-finance-mcp"
        )
        self.assertEqual(version_id, "11111111-2222-3333-4444-555555555555")
        self.assertEqual(
            preview_url,
            "https://candidate-123-1-yahoo-finance-mcp.artinatw.workers.dev",
        )

    def test_version_upload_parser_fails_closed_on_alias_mismatch(self) -> None:
        events = [
            {
                "type": "version-upload",
                "version_id": "11111111-2222-3333-4444-555555555555",
                "preview_urls": ["https://other-unrelated-worker.example.workers.dev"],
            }
        ]
        with self.assertRaisesRegex(ValueError, "could not resolve one preview hostname"):
            promotion.parse_version_upload(events, "candidate-123-1", "yahoo-finance-mcp")

    def test_version_upload_parser_rejects_unsafe_version_id(self) -> None:
        events = [
            {
                "type": "version-upload",
                "version_id": "valid-id\nforged_output=value",
                "preview_url": "https://a1b2c3d4-yahoo-finance-mcp.artinatw.workers.dev",
            }
        ]
        with self.assertRaisesRegex(ValueError, "valid version_id"):
            promotion.parse_version_upload(events, "candidate-123-1", "yahoo-finance-mcp")

    def test_version_upload_parser_derives_alias_from_versioned_url(self) -> None:
        events = [
            {
                "type": "version-upload",
                "version_id": "11111111-2222-3333-4444-555555555555",
                "preview_url": "https://a1b2c3d4-yahoo-finance-mcp.artinatw.workers.dev",
            }
        ]
        _version_id, preview_url = promotion.parse_version_upload(
            events, "candidate-456-1", "yahoo-finance-mcp"
        )
        self.assertEqual(
            preview_url,
            "https://candidate-456-1-yahoo-finance-mcp.artinatw.workers.dev",
        )

    def test_secrets_file_omits_unconfigured_optional_values(self) -> None:
        payload = promotion.build_secrets(
            {
                "TOOL_MODE": "expanded",
                "FINNHUB_API_KEY": "finnhub-secret",
                "SEC_OPEN_DATA_API_KEY": "",
            }
        )
        self.assertEqual(payload, {"TOOL_MODE": "expanded", "FINNHUB_API_KEY": "finnhub-secret"})
        with tempfile.TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "secrets.json"
            promotion.write_secrets(path, {"TOOL_MODE": "grouped"})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"TOOL_MODE": "grouped"})

    def test_version_identity_poll_retries_until_exact_version_is_live(self) -> None:
        responses = iter(
            [
                {"status": "ok", "workerVersionId": "old-version"},
                {"status": "ok", "workerVersionId": "expected-version"},
            ]
        )
        sleeps: list[float] = []

        payload = promotion.wait_for_worker_version(
            "https://example.workers.dev/health",
            "expected-version",
            attempts=2,
            delay_seconds=0.25,
            timeout=1,
            fetch=lambda _url, _timeout: next(responses),
            sleep=sleeps.append,
        )

        self.assertEqual(payload["workerVersionId"], "expected-version")
        self.assertEqual(sleeps, [0.25])

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
