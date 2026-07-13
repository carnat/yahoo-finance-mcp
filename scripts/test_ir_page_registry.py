#!/usr/bin/env python3
"""Regression tests for the approved company IR-page registry workflow."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
DISCOVERY_SCRIPT = ROOT / "scripts" / "discover_ir_page_candidates.py"
SCOPE_PATH = ROOT / "scripts" / "ir_page_scope.json"
REGISTRY_PATH = ROOT / "worker" / "src" / "company-ir-page-registry.json"
ARSENAL_TICKERS = {
    "FN", "VRT", "MU", "ANET", "TSM", "COHR", "LITE", "TSEM", "ASTS", "SIVE.ST",
    "MRVL", "SNDK", "FLNC", "LPK.DE", "BE", "NBIS", "3363.TWO", "IQE.L", "AAOI", "AEHR",
}


def _load_discovery_module():
    spec = importlib.util.spec_from_file_location("discover_ir_page_candidates", DISCOVERY_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load discovery script")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestIrPageRegistry(unittest.TestCase):
    def test_registry_schema_validates(self):
        module = _load_discovery_module()
        registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        module.validate_registry(registry)
        self.assertEqual(registry.get("schemaVersion"), "2026-07-10")
        self.assertIn("registryVersion", registry)
        self.assertIsInstance(registry.get("sources"), list)

    def test_arsenal_scope_is_exact_minimum_cover(self):
        module = _load_discovery_module()
        scope = json.loads(SCOPE_PATH.read_text(encoding="utf-8"))
        tickers = module.validate_scope(scope)
        self.assertEqual(set(tickers), ARSENAL_TICKERS)
        self.assertEqual(len(tickers), 20)

    def test_candidate_records_keep_review_metadata_as_json_null(self):
        module = _load_discovery_module()
        with patch.object(module, "public_company_identity", return_value=("Micron Technology", "https://www.micron.com/")):
            with patch.object(module, "fetch_text", return_value=("https://investors.micron.com/news", "text/html", "<title>Investor Relations newsroom</title>")):
                candidate = module.candidate_for("MU")
        self.assertEqual(candidate["status"], "candidate")
        self.assertEqual(candidate["issuerName"], "Micron Technology")
        self.assertIsNone(candidate["reviewedBy"])
        self.assertIsNone(candidate["revalidateAfter"])
        module.validate_registry({"schemaVersion": "2026-07-10", "sources": [candidate]})


if __name__ == "__main__":
    unittest.main()
