#!/usr/bin/env python3
"""Static guards for Worker-only Alpaca overnight quote support."""

from __future__ import annotations

import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
YF_TS = ROOT / "worker" / "src" / "yahoo-finance.ts"
TOOLS_TS = ROOT / "worker" / "src" / "tools.ts"
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy-worker.yml"
DEPLOYED_DISCOVERY = ROOT / "scripts" / "test_deployed_discovery.py"


class TestWorkerAlpacaOvernight(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.worker = YF_TS.read_text(encoding="utf-8")
        cls.tools = TOOLS_TS.read_text(encoding="utf-8")
        cls.workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")
        cls.discovery = DEPLOYED_DISCOVERY.read_text(encoding="utf-8")

    def test_worker_uses_alpaca_boats_feed_without_sidecar(self) -> None:
        self.assertIn("OVERNIGHT_PROVIDER", self.worker)
        self.assertIn("ALPACA_API_KEY", self.worker)
        self.assertIn("ALPACA_SECRET_KEY", self.worker)
        self.assertIn("ALPACA_DATA_BASE_URL", self.worker)
        self.assertIn("https://data.alpaca.markets", self.worker)
        self.assertIn('feed: ALPACA_OVERNIGHT_FEED', self.worker)
        self.assertIn('"boats"', self.worker)
        self.assertIn('"APCA-API-KEY-ID"', self.worker)
        self.assertIn('"APCA-API-SECRET-KEY"', self.worker)
        self.assertRegex(self.worker, r"fetch\(url,[\s\S]+APCA-API-KEY-ID")
        self.assertNotIn("alpaca-py", self.worker)

    def test_response_statuses_are_explicit(self) -> None:
        for status in (
            "FOUND_TRUE_OVERNIGHT",
            "PROVIDER_UNCONFIGURED",
            "PROVIDER_FORBIDDEN",
            "PROVIDER_RATE_LIMITED",
            "PROVIDER_UNAVAILABLE",
            "NO_OVERNIGHT_BARS",
            "FALLBACK_EXTENDED_HOURS",
        ):
            self.assertIn(status, self.worker)
        self.assertIn('dataSource: "BLUE_OCEAN_ATS"', self.worker)
        self.assertIn('provider: "alpaca"', self.worker)
        self.assertIn('provider: "yahoo"', self.worker)

    def test_public_description_mentions_provider_fields(self) -> None:
        self.assertRegex(
            self.tools,
            r'name: "get_overnight_quote"[\s\S]+providerStatus[\s\S]+requestedFeed',
        )
        self.assertIn("Alpaca BOATS/Blue Ocean", self.tools)

    def test_deploy_wires_alpaca_runtime_secrets(self) -> None:
        self.assertIn("Wire Alpaca overnight provider secrets", self.workflow)
        for name in (
            "ALPACA_API_KEY",
            "ALPACA_SECRET_KEY",
            "OVERNIGHT_PROVIDER",
            "ALPACA_DATA_BASE_URL",
        ):
            self.assertRegex(self.workflow, rf"wrangler secret put {name}")

    def test_deployed_smoke_checks_honest_status(self) -> None:
        self.assertIn("_ALPACA_KEY_CONFIGURED", self.discovery)
        self.assertIn("FOUND_TRUE_OVERNIGHT", self.discovery)
        self.assertIn("PROVIDER_FORBIDDEN", self.discovery)
        self.assertIn("get_overnight_quote", self.discovery)
        self.assertIn('for secret_name in ("ALPACA_API_KEY", "ALPACA_SECRET_KEY")', self.discovery)
        self.assertIn("found in overnight tool output", self.discovery)


if __name__ == "__main__":
    unittest.main(verbosity=2)
