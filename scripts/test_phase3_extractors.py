#!/usr/bin/env python3
"""Phase 3B extractor schema and helper unit tests.

These tests validate extractor helper logic and output schema invariants
without making live network calls. They import server.py directly.

Run: python scripts/test_phase3_extractors.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _ensure_mcp_available() -> None:
    """Install a minimal FastMCP stub when the real mcp package is absent."""
    try:
        from mcp.server.fastmcp import FastMCP  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _FastMCPStub:
        def __init__(self, *a: object, **kw: object) -> None:
            pass

        def tool(self, *a: object, **kw: object):  # type: ignore[return]
            if a and callable(a[0]):
                return a[0]

            def _decorator(fn):  # type: ignore[return]
                return fn

            return _decorator

    mcp_mod = types.ModuleType("mcp")
    server_mod = types.ModuleType("mcp.server")
    fastmcp_mod = types.ModuleType("mcp.server.fastmcp")
    fastmcp_mod.FastMCP = _FastMCPStub  # type: ignore[attr-defined]
    mcp_mod.server = server_mod  # type: ignore[attr-defined]
    server_mod.fastmcp = fastmcp_mod  # type: ignore[attr-defined]
    sys.modules.setdefault("mcp", mcp_mod)
    sys.modules.setdefault("mcp.server", server_mod)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_mod)


_ensure_mcp_available()

# Patch FastMCP.tool to accept output_schema before importing server
from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402

if not getattr(_FastMCP, "_output_schema_patched", False):
    _orig_tool = _FastMCP.tool

    def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
        return _orig_tool(self, name=name, **kwargs)

    _FastMCP.tool = _patched_tool  # type: ignore[method-assign]
    _FastMCP._output_schema_patched = True  # type: ignore[attr-defined]

import server as srv  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


def _parse(raw: str) -> dict:
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Tests for helper functions
# ---------------------------------------------------------------------------

class TestSafeJsonLoads(unittest.TestCase):
    def test_valid_dict(self):
        result = srv._safe_json_loads('{"ok": true, "value": 1}')
        self.assertEqual(result, {"ok": True, "value": 1})

    def test_invalid_json_returns_empty(self):
        self.assertEqual(srv._safe_json_loads("not-json"), {})

    def test_non_dict_returns_empty(self):
        self.assertEqual(srv._safe_json_loads("[1,2,3]"), {})

    def test_empty_string_returns_empty(self):
        self.assertEqual(srv._safe_json_loads(""), {})


class TestCompactExcerpt(unittest.TestCase):
    def test_short_text_unchanged(self):
        t = "Short text"
        self.assertEqual(srv._compact_excerpt(t), t)

    def test_long_text_truncated(self):
        t = "x" * 300
        result = srv._compact_excerpt(t)
        self.assertLessEqual(len(result), 244)
        self.assertTrue(result.endswith("..."))

    def test_whitespace_collapsed(self):
        t = "Revenue:   $1.2B   (FY2024)"
        result = srv._compact_excerpt(t)
        self.assertNotIn("   ", result)

    def test_custom_max_len(self):
        t = "a" * 100
        result = srv._compact_excerpt(t, max_len=50)
        self.assertLessEqual(len(result), 54)


class TestAsStatus(unittest.TestCase):
    def test_not_disclosed_from_confidence(self):
        self.assertEqual(srv._as_status({"confidence": "NOT_DISCLOSED"}), "NOT_DISCLOSED")

    def test_not_disclosed_from_source(self):
        self.assertEqual(srv._as_status({"source": "NOT_DISCLOSED"}), "NOT_DISCLOSED")

    def test_conflicting(self):
        self.assertEqual(srv._as_status({"confidence": "CONFLICTING"}), "CONFLICTING")

    def test_not_found_default(self):
        self.assertEqual(srv._as_status({}), "NOT_FOUND")
        self.assertEqual(srv._as_status({"confidence": "LOW"}), "NOT_FOUND")


# ---------------------------------------------------------------------------
# Extractor output schema invariants
# ---------------------------------------------------------------------------

_GEO_REQUIRED = ("value", "denominator", "valueRatio", "valuePct", "extractionMethod", "confidence")
_GEO_PHASE3_REQUIRED = ("factType", "region", "evidence", "warnings", "calculation")


def _assert_geo_schema_invariants(data: dict, label: str) -> None:
    """Check that geographic revenue schema invariants hold."""
    for key in _GEO_REQUIRED:
        assert key in data, f"{label}: missing key '{key}'"
        assert data[key] != {}, f"{label}: key '{key}' must be null/number, not object"

    if data["denominator"] is not None:
        assert isinstance(data["denominator"], (int, float)), f"{label}: denominator must be number"
        assert data["valueRatio"] is not None, f"{label}: valueRatio must be non-null when denominator is set"
        assert data["valuePct"] is not None, f"{label}: valuePct must be non-null when denominator is set"
    else:
        assert data["valueRatio"] is None, f"{label}: valueRatio must be null when denominator is null"
        assert data["valuePct"] is None, f"{label}: valuePct must be null when denominator is null"

    if data["valueRatio"] is not None:
        r = float(data["valueRatio"])
        assert 0.0 <= r <= 1.0, f"{label}: valueRatio {r} not in [0,1] decimal range"

    if data["valuePct"] is not None:
        p = float(data["valuePct"])
        assert 0.0 <= p <= 100.0, f"{label}: valuePct {p} not in [0,100] percent range"


class TestExtractGeographicRevenueShape(unittest.TestCase):
    """Validate extract_geographic_revenue output shape using an invalid ticker (no network needed)."""

    def _call(self, ticker: str, region: str) -> dict:
        raw = _run(srv.extract_geographic_revenue(
            ticker=ticker,
            region=region,
            filing_type="10-K",
            period="latest",
        ))
        return _parse(raw)

    def test_empty_region_returns_input_validation_error(self):
        result = self._call("AAPL", "")
        self.assertIn("warnings", result)
        codes = [w.get("code") for w in result.get("warnings", []) if isinstance(w, dict)]
        self.assertIn("INPUT_VALIDATION_ERROR", codes)
        # All schema keys must still be present
        _assert_geo_schema_invariants(result, "empty-region")

    def test_output_has_phase3_required_keys(self):
        result = self._call("AAPL", "Greater China")
        for key in _GEO_PHASE3_REQUIRED:
            self.assertIn(key, result, f"Missing key: {key}")

    def test_fact_type_is_geographic_revenue(self):
        result = self._call("AAPL", "Greater China")
        self.assertEqual(result.get("factType"), "geographic_revenue")

    def test_evidence_is_dict(self):
        result = self._call("AAPL", "Greater China")
        self.assertIsInstance(result.get("evidence"), dict, "evidence must be a dict")

    def test_warnings_is_list(self):
        result = self._call("AAPL", "Greater China")
        self.assertIsInstance(result.get("warnings"), list, "warnings must be a list")

    def test_invariants_no_denominator(self):
        # For an invalid ticker, we expect null values — invariants should still hold
        result = self._call("ZZZZINVALID99", "China")
        _assert_geo_schema_invariants(result, "invalid-ticker")

    def test_region_preserved_in_output(self):
        result = self._call("AAPL", "Atlantis")
        self.assertEqual(result.get("region"), "Atlantis")

    def test_ticker_preserved_in_output(self):
        result = self._call("AAPL", "Atlantis")
        self.assertEqual(result.get("ticker"), "AAPL")


class TestExtractSegmentRevenueShape(unittest.TestCase):
    def _call(self, ticker: str) -> dict:
        raw = _run(srv.extract_segment_revenue(ticker=ticker))
        return _parse(raw)

    def test_output_has_required_keys(self):
        result = self._call("AAPL")
        self.assertIn("ticker", result)
        self.assertIn("factType", result)
        self.assertIn("segments", result)
        self.assertIn("status", result)

    def test_segments_is_list(self):
        result = self._call("AAPL")
        self.assertIsInstance(result.get("segments"), list)

    def test_fact_type_is_segment_revenue(self):
        result = self._call("AAPL")
        self.assertEqual(result.get("factType"), "segment_revenue")

    def test_invalid_ticker_stable_output(self):
        result = self._call("ZZZZINVALID99")
        self.assertIn("status", result)
        self.assertIn("segments", result)
        self.assertIsInstance(result["segments"], list)


class TestExtractTotalRevenueShape(unittest.TestCase):
    def _call(self, ticker: str) -> dict:
        raw = _run(srv.extract_total_revenue(ticker=ticker))
        return _parse(raw)

    def test_output_has_required_keys(self):
        result = self._call("AAPL")
        for key in ("ticker", "factType", "confidence", "evidence", "status"):
            self.assertIn(key, result, f"Missing key: {key}")

    def test_fact_type(self):
        result = self._call("AAPL")
        self.assertEqual(result.get("factType"), "total_revenue")

    def test_evidence_is_dict(self):
        result = self._call("AAPL")
        self.assertIsInstance(result.get("evidence"), dict)

    def test_invalid_ticker_returns_status(self):
        result = self._call("ZZZZINVALID99")
        self.assertIn("status", result)


class TestExtractRevenueExposureShape(unittest.TestCase):
    def _call(self, ticker: str, query: str) -> dict:
        raw = _run(srv.extract_revenue_exposure(ticker=ticker, exposure_query=query))
        return _parse(raw)

    def test_output_has_required_keys(self):
        result = self._call("AAPL", "Greater China")
        for key in ("ticker", "query", "matches", "status"):
            self.assertIn(key, result)

    def test_matches_is_list(self):
        result = self._call("AAPL", "Atlantis")
        self.assertIsInstance(result.get("matches"), list)

    def test_not_found_status_when_no_match(self):
        result = self._call("AAPL", "Atlantis99XYZ")
        self.assertIn(result.get("status"), (
            "NOT_FOUND", "NOT_DISCLOSED", "CONFLICTING", "FOUND_REVENUE_EXPOSURE",
        ))

    def test_no_valuepct_without_denominator(self):
        result = self._call("AAPL", "Greater China")
        for m in result.get("matches", []):
            if isinstance(m, dict) and m.get("valuePct") is not None:
                self.assertIsNotNone(m.get("denominator"), "valuePct present but denominator is null")


class TestExtractRiskFactorMentionsShape(unittest.TestCase):
    def _call(self, ticker: str, terms: list) -> dict:
        raw = _run(srv.extract_risk_factor_mentions(ticker=ticker, terms=terms))
        return _parse(raw)

    def test_output_has_required_keys(self):
        result = self._call("AAPL", ["China"])
        for key in ("ticker", "matches", "status"):
            self.assertIn(key, result)

    def test_matches_is_list(self):
        result = self._call("AAPL", ["China"])
        self.assertIsInstance(result.get("matches"), list)

    def test_status_values(self):
        result = self._call("AAPL", ["Zxyz99NonexistentTerm"])
        self.assertIn(result.get("status"), ("FOUND", "NOT_FOUND"))

    def test_empty_terms_returns_not_found(self):
        result = self._call("AAPL", [])
        self.assertEqual(result.get("status"), "NOT_FOUND")
        self.assertEqual(result.get("matches"), [])


class TestExtractCustomerConcentrationShape(unittest.TestCase):
    def _call(self, ticker: str) -> dict:
        raw = _run(srv.extract_customer_concentration(ticker=ticker))
        return _parse(raw)

    def test_output_has_required_keys(self):
        result = self._call("AAPL")
        for key in ("ticker", "customers", "status"):
            self.assertIn(key, result)

    def test_customers_is_list(self):
        result = self._call("AAPL")
        self.assertIsInstance(result.get("customers"), list)

    def test_customer_fields_present(self):
        result = self._call("AAPL")
        for c in result.get("customers", []):
            self.assertIsInstance(c, dict)
            self.assertIn("label", c)
            self.assertIn("valuePct", c)

    def test_status_values(self):
        result = self._call("ZZZZINVALID99")
        self.assertIn(result.get("status"), ("FOUND", "NOT_DISCLOSED", "NOT_FOUND"))


class TestExtractChinaExposureShape(unittest.TestCase):
    def _call(self, ticker: str) -> dict:
        raw = _run(srv.extract_china_exposure(ticker=ticker))
        return _parse(raw)

    def test_output_has_required_keys(self):
        result = self._call("AAPL")
        for key in ("ticker", "exposureType", "revenueExposure", "manufacturingExposure",
                    "entityExposure", "bankExposure", "riskFactorExposure",
                    "overallStatus", "warnings"):
            self.assertIn(key, result, f"Missing key: {key}")

    def test_exposure_type(self):
        result = self._call("AAPL")
        self.assertEqual(result.get("exposureType"), "china_exposure")

    def test_revenue_exposure_has_status(self):
        result = self._call("AAPL")
        rev = result.get("revenueExposure") or {}
        self.assertIsInstance(rev, dict)
        self.assertIn("status", rev)

    def test_overall_status_is_valid(self):
        result = self._call("AAPL")
        valid = {
            "FOUND_REVENUE_EXPOSURE",
            "FOUND_NON_REVENUE_EXPOSURE",
            "NOT_DISCLOSED",
            "NOT_FOUND",
            "CONFLICTING",
        }
        self.assertIn(result.get("overallStatus"), valid,
                      f"Unexpected overallStatus: {result.get('overallStatus')!r}")

    def test_warnings_is_list(self):
        result = self._call("AAPL")
        self.assertIsInstance(result.get("warnings"), list)

    def test_revenue_not_collapsed_from_entity_exposure(self):
        """Revenue exposure must not be derived from entity text."""
        result = self._call("AXTI")
        rev = result.get("revenueExposure") or {}
        # revenueExposure should reflect the revenue fact, not non-revenue entity evidence
        entity = result.get("entityExposure") or {}
        if rev.get("value") is not None and entity.get("status") == "FOUND":
            # If revenue was found AND entity was found, overall should be FOUND_REVENUE_EXPOSURE
            self.assertEqual(result.get("overallStatus"), "FOUND_REVENUE_EXPOSURE")
        # If revenue is null, overall must not claim FOUND_REVENUE_EXPOSURE
        if rev.get("value") is None and rev.get("status") != "FOUND":
            self.assertNotEqual(
                result.get("overallStatus"),
                "FOUND_REVENUE_EXPOSURE",
                "overallStatus should not be FOUND_REVENUE_EXPOSURE when revenue value is null",
            )


# ---------------------------------------------------------------------------
# Token efficiency: default outputs must not contain raw HTML / huge context
# ---------------------------------------------------------------------------

class TestTokenEfficiency(unittest.TestCase):
    def test_geo_default_no_raw_context(self):
        raw = _run(srv.extract_geographic_revenue(
            ticker="AAPL", region="Greater China", filing_type="10-K", period="latest",
        ))
        result = _parse(raw)
        self.assertNotIn("rawContext", result, "rawContext should not appear in default (compact) mode")

    def test_segment_default_no_raw_context(self):
        raw = _run(srv.extract_segment_revenue(ticker="AAPL"))
        result = _parse(raw)
        self.assertNotIn("rawContext", result)

    def test_customer_default_no_raw_context(self):
        raw = _run(srv.extract_customer_concentration(ticker="AAPL"))
        result = _parse(raw)
        self.assertNotIn("rawMatchCount", result)

    def test_china_default_no_raw_context(self):
        raw = _run(srv.extract_china_exposure(ticker="AAPL"))
        result = _parse(raw)
        self.assertNotIn("rawContext", result)

    def test_risk_mentions_default_no_raw_terms(self):
        raw = _run(srv.extract_risk_factor_mentions(ticker="AAPL", terms=["China"]))
        result = _parse(raw)
        self.assertNotIn("rawTerms", result)


# ---------------------------------------------------------------------------
# Status semantics
# ---------------------------------------------------------------------------

class TestStatusSemantics(unittest.TestCase):
    def test_as_status_consistent_mapping(self):
        """_as_status must map known inputs to correct status codes."""
        cases = [
            ({"confidence": "NOT_DISCLOSED"}, "NOT_DISCLOSED"),
            ({"source": "NOT_DISCLOSED"}, "NOT_DISCLOSED"),
            ({"confidence": "CONFLICTING"}, "CONFLICTING"),
            ({"source": "CONFLICTING"}, "CONFLICTING"),
            ({}, "NOT_FOUND"),
            ({"confidence": "HIGH"}, "NOT_FOUND"),
            ({"confidence": "LOW"}, "NOT_FOUND"),
        ]
        for payload, expected in cases:
            result = srv._as_status(payload)
            self.assertEqual(result, expected, f"_as_status({payload!r}) = {result!r}, want {expected!r}")


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [
        TestSafeJsonLoads,
        TestCompactExcerpt,
        TestAsStatus,
        TestExtractGeographicRevenueShape,
        TestExtractSegmentRevenueShape,
        TestExtractTotalRevenueShape,
        TestExtractRevenueExposureShape,
        TestExtractRiskFactorMentionsShape,
        TestExtractCustomerConcentrationShape,
        TestExtractChinaExposureShape,
        TestTokenEfficiency,
        TestStatusSemantics,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
