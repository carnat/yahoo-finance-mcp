#!/usr/bin/env python3
"""Regression tests for the non-blocking Arsenal news-quality audit."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "audit_arsenal_news.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("audit_arsenal_news", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load audit script")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestArsenalNewsAudit(unittest.TestCase):
    def test_build_ticker_result_compares_source_isolated_titles(self):
        module = _load_module()
        yahoo = {
            "ok": True,
            "data": {
                "status": "OK",
                "coverage": {"state": "FULL"},
                "sourceStatus": {"yahoo_finance_news": {"status": "OK"}},
                "items": [
                    {"title": "Issuer expands factory", "evidenceClass": "CONTEXTUAL_NEWS"},
                    {"title": "Issuer reports results", "evidenceClass": "CONTEXTUAL_NEWS"},
                ],
            },
        }
        finnhub = {
            "ok": True,
            "data": {
                "status": "PARTIAL",
                "coverage": {"state": "PARTIAL"},
                "sourceStatus": {"finnhub": {"status": "AUTH_ERROR"}},
                "items": [{"title": "Issuer expands factory", "evidenceClass": "CONTEXTUAL_NEWS"}],
            },
        }
        result = module.build_ticker_result("MU", {"yahoo_finance_news": yahoo, "finnhub": finnhub})
        self.assertEqual(result["providers"]["yahoo_finance_news"]["itemCount"], 2)
        self.assertEqual(result["providers"]["finnhub"]["sourceStatus"], "AUTH_ERROR")
        self.assertEqual(result["comparison"], {
            "sharedTitleCount": 1,
            "combinedUniqueTitleCount": 2,
            "titleOverlapRatio": 0.5,
        })

    def test_call_failure_is_retained_as_a_nonblocking_audit_finding(self):
        module = _load_module()
        row = module.summarize_response("SIVE.ST", "finnhub", {"ok": False})
        self.assertEqual(row["callState"], "CALL_FAILED")
        self.assertEqual(row["itemCount"], 0)


if __name__ == "__main__":
    unittest.main()
