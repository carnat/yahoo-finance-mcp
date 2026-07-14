#!/usr/bin/env python3
"""Static regression checks for Worker options-summary failure normalization."""

from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKER_YAHOO = ROOT / "worker" / "src" / "yahoo-finance.ts"
WORKER_RESPONSE = ROOT / "worker" / "src" / "response.ts"


class TestWorkerOptionsSummaryEnvelope(unittest.TestCase):
    def setUp(self) -> None:
        source = WORKER_YAHOO.read_text(encoding="utf-8")
        start = source.index("export async function getOptionsSummary")
        end = source.index("export async function listSecFilings", start)
        self.summary_source = source[start:end]

    def test_failure_paths_are_typed_legacy_envelopes(self) -> None:
        self.assertIn("code: ErrorCode.NO_OPTIONS_DATA", self.summary_source)
        self.assertIn("code: ErrorCode.PROVIDER_ERROR", self.summary_source)
        self.assertNotIn('JSON.stringify({ ticker, error:', self.summary_source)

    def test_dispatcher_promotes_legacy_failure_to_v2_not_ok_true(self) -> None:
        response_source = WORKER_RESPONSE.read_text(encoding="utf-8")
        self.assertIn('parsed as Record<string, unknown>).error === true', response_source)
        self.assertIn("ok: false", response_source)


if __name__ == "__main__":
    unittest.main()
