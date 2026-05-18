#!/usr/bin/env python3
"""Comprehensive unit tests for untested helper functions in server.py.

Covers ~30 pure/offline helper functions that were missing test coverage:
  - Date/time utilities: get_last_trading_date, _to_iso_utc, _derive_fiscal_period_from_date
  - Coercion helpers: _coerce_max_results, _coerce_lookback_days
  - Event-layer helpers: _normalize_event_sources, _event_type_from_keywords,
      _event_type_from_form, _short_text, _canonicalize_event_url,
      _make_duplicate_group_id, _source_rank, _safe_sec_url, _within_date_window,
      _dedupe_event_items, _build_collection_status, _compute_source_status,
      _compute_source_coverage
  - EDGAR helpers: _edgar_cik_from_accession, _edgar_build_filing_urls
  - Filing / XBRL helpers: _region_matches, _normalize_segment_label, _as_status
  - JSON / text helpers: _safe_parse, _safe_json_loads, _compact_excerpt
  - Earnings helpers: _is_paywalled_url, _classify_earnings_source_url,
      _scale_number_from_text, _first_sentence_for_topic, _extract_metric_number
  - Alias helpers: _deprecated_alias_response
  - SEC filing index: _build_filing_index_from_html (including XSS sanitization)

All tests are offline — no network calls required.
Run:
    PYTHONPATH=. python scripts/test_helpers.py
    # or via pytest:
    pytest scripts/test_helpers.py -v
"""

from __future__ import annotations

import datetime
import json
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _ensure_mcp_available() -> None:
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

from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402

if not getattr(_FastMCP, "_output_schema_patched", False):
    _orig_tool = _FastMCP.tool

    def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
        return _orig_tool(self, name=name, **kwargs)

    _FastMCP.tool = _patched_tool  # type: ignore[method-assign]
    _FastMCP._output_schema_patched = True  # type: ignore[attr-defined]

import server as srv  # noqa: E402


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_event(
    title: str = "Event",
    source_type: str = "yahoo_finance",
    source: str = "Yahoo Finance",
    published_at: str = "2026-05-15T12:00:00Z",
    group_id: str | None = "g1",
) -> dict:
    return {
        "title": title,
        "source": source,
        "sourceType": source_type,
        "publishedAt": published_at,
        "duplicateGroupId": group_id,
        "sourceRefs": [],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Date / Time utilities
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetLastTradingDate(unittest.TestCase):
    def test_returns_string(self):
        result = srv.get_last_trading_date()
        self.assertIsInstance(result, str)
        self.assertRegex(result, r"^\d{4}-\d{2}-\d{2}$")

    def test_never_returns_weekend(self):
        result = srv.get_last_trading_date()
        d = datetime.date.fromisoformat(result)
        self.assertLessEqual(d.weekday(), 4, "Result must be Mon–Fri")

    def test_df_branch_uses_last_index(self):
        import pandas as pd
        idx = pd.to_datetime(["2026-05-12", "2026-05-13"])
        df = pd.DataFrame({"close": [100, 101]}, index=idx)
        self.assertEqual(srv.get_last_trading_date(df), "2026-05-13")

    def test_empty_df_falls_back_to_weekday(self):
        import pandas as pd
        df = pd.DataFrame()
        result = srv.get_last_trading_date(df)
        self.assertRegex(result, r"^\d{4}-\d{2}-\d{2}$")


class TestToIsoUtc(unittest.TestCase):
    """Tests for the active _to_iso_utc (the earnings-layer version at line ~8783)."""

    def test_none_returns_none(self):
        self.assertIsNone(srv._to_iso_utc(None))

    def test_empty_string_returns_none(self):
        self.assertIsNone(srv._to_iso_utc(""))

    def test_whitespace_only_returns_none(self):
        self.assertIsNone(srv._to_iso_utc("   "))

    def test_date_only_string(self):
        result = srv._to_iso_utc("2026-05-15")
        self.assertEqual(result, "2026-05-15T00:00:00Z")

    def test_iso_string_with_z(self):
        result = srv._to_iso_utc("2026-05-15T12:30:00Z")
        self.assertEqual(result, "2026-05-15T12:30:00Z")

    def test_iso_string_with_offset(self):
        result = srv._to_iso_utc("2026-05-15T08:30:00-04:00")
        self.assertEqual(result, "2026-05-15T12:30:00Z")

    def test_invalid_string_returns_none(self):
        self.assertIsNone(srv._to_iso_utc("not-a-date"))


class TestDeriveFiscalPeriod(unittest.TestCase):
    def test_q1_january(self):
        self.assertEqual(srv._derive_fiscal_period_from_date("2026-01-15"), "FY2026 Q1")

    def test_q1_march(self):
        self.assertEqual(srv._derive_fiscal_period_from_date("2026-03-31"), "FY2026 Q1")

    def test_q2_april(self):
        self.assertEqual(srv._derive_fiscal_period_from_date("2026-04-01"), "FY2026 Q2")

    def test_q3_july(self):
        self.assertEqual(srv._derive_fiscal_period_from_date("2026-07-10"), "FY2026 Q3")

    def test_q4_december(self):
        self.assertEqual(srv._derive_fiscal_period_from_date("2026-12-31"), "FY2026 Q4")

    def test_none_returns_none(self):
        self.assertIsNone(srv._derive_fiscal_period_from_date(None))

    def test_invalid_date_returns_none(self):
        self.assertIsNone(srv._derive_fiscal_period_from_date("not-a-date"))


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Coercion helpers
# ═══════════════════════════════════════════════════════════════════════════════

class TestCoerceMaxResults(unittest.TestCase):
    def test_normal_value(self):
        self.assertEqual(srv._coerce_max_results(10, 20), 10)

    def test_zero_uses_default(self):
        self.assertEqual(srv._coerce_max_results(0, 25), 25)

    def test_clamps_to_minimum_one(self):
        self.assertEqual(srv._coerce_max_results(-5, 10), 1)

    def test_clamps_to_maximum_100(self):
        self.assertEqual(srv._coerce_max_results(999, 10), 100)

    def test_exact_boundary_100(self):
        self.assertEqual(srv._coerce_max_results(100, 10), 100)


class TestCoerceLookbackDays(unittest.TestCase):
    def test_normal_value(self):
        self.assertEqual(srv._coerce_lookback_days(30, 90), 30)

    def test_zero_uses_default(self):
        self.assertEqual(srv._coerce_lookback_days(0, 60), 60)

    def test_clamps_to_minimum_one(self):
        self.assertEqual(srv._coerce_lookback_days(-1, 30), 1)

    def test_clamps_to_maximum_3650(self):
        self.assertEqual(srv._coerce_lookback_days(10000, 30), 3650)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Event-layer helpers
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizeEventSources(unittest.TestCase):
    def test_valid_sources_pass_through(self):
        sources, warnings = srv._normalize_event_sources(["yahoo_finance", "sec"], ["yahoo_finance"])
        self.assertEqual(sources, ["yahoo_finance", "sec"])
        self.assertEqual(warnings, [])

    def test_none_uses_default(self):
        sources, warnings = srv._normalize_event_sources(None, ["yahoo_finance"])
        self.assertEqual(sources, ["yahoo_finance"])

    def test_empty_list_uses_default(self):
        sources, warnings = srv._normalize_event_sources([], ["yahoo_finance", "finnhub"])
        self.assertIn("yahoo_finance", sources)

    def test_unsupported_source_emits_warning(self):
        sources, warnings = srv._normalize_event_sources(["unknown_source"], ["yahoo_finance"])
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["code"], "SOURCE_UNSUPPORTED")

    def test_duplicates_deduplicated(self):
        sources, _ = srv._normalize_event_sources(["yahoo_finance", "yahoo_finance"], ["finnhub"])
        self.assertEqual(sources.count("yahoo_finance"), 1)

    def test_case_insensitive_normalization(self):
        sources, _ = srv._normalize_event_sources(["Yahoo_Finance"], ["finnhub"])
        self.assertIn("yahoo_finance", sources)


class TestEventTypeFromKeywords(unittest.TestCase):
    def test_earnings_keywords(self):
        self.assertEqual(srv._event_type_from_keywords("Apple Q3 earnings beat"), "earnings")

    def test_guidance_keywords(self):
        self.assertEqual(srv._event_type_from_keywords("Company raises guidance"), "guidance")

    def test_contract_keywords(self):
        self.assertEqual(srv._event_type_from_keywords("New defense contract signed"), "contract")

    def test_financing_keywords(self):
        self.assertEqual(srv._event_type_from_keywords("Company prices senior note offering"), "financing")

    def test_product_keywords(self):
        self.assertEqual(srv._event_type_from_keywords("Apple announces new iPhone launch"), "product")

    def test_analyst_keywords(self):
        self.assertEqual(srv._event_type_from_keywords("Analyst upgrade and new price target"), "analyst")

    def test_macro_keywords(self):
        self.assertEqual(srv._event_type_from_keywords("FOMC rate decision and inflation data"), "macro")

    def test_litigation_keywords(self):
        self.assertEqual(srv._event_type_from_keywords("Patent lawsuit court settlement"), "litigation")

    def test_insider_keywords(self):
        self.assertEqual(srv._event_type_from_keywords("Director files Form 4 insider"), "insider")

    def test_regulatory_keywords(self):
        self.assertEqual(srv._event_type_from_keywords("SEC 8-K regulatory filing"), "regulatory")

    def test_other_fallback(self):
        self.assertEqual(srv._event_type_from_keywords("Random unrelated headline"), "other")

    def test_empty_string_returns_other(self):
        self.assertEqual(srv._event_type_from_keywords(""), "other")


class TestEventTypeFromForm(unittest.TestCase):
    def test_10q_is_earnings(self):
        self.assertEqual(srv._event_type_from_form("10-Q"), "earnings")

    def test_10k_is_earnings(self):
        self.assertEqual(srv._event_type_from_form("10-K"), "earnings")

    def test_s3_is_financing(self):
        self.assertEqual(srv._event_type_from_form("S-3"), "financing")

    def test_8k_is_regulatory(self):
        self.assertEqual(srv._event_type_from_form("8-K"), "regulatory")

    def test_form4_is_insider(self):
        self.assertEqual(srv._event_type_from_form("4"), "insider")

    def test_unknown_form_is_other(self):
        self.assertEqual(srv._event_type_from_form("SC-TO"), "other")

    def test_case_insensitive(self):
        self.assertEqual(srv._event_type_from_form("10-k"), "earnings")


class TestShortText(unittest.TestCase):
    def test_none_returns_none(self):
        self.assertIsNone(srv._short_text(None))

    def test_empty_returns_none(self):
        self.assertIsNone(srv._short_text(""))

    def test_whitespace_only_returns_none(self):
        self.assertIsNone(srv._short_text("   "))

    def test_normalizes_whitespace(self):
        result = srv._short_text("hello   world\n\nfoo")
        self.assertEqual(result, "hello world foo")

    def test_truncates_to_max_chars(self):
        long_text = "A" * 300
        result = srv._short_text(long_text, 50)
        self.assertIsNotNone(result)
        self.assertLessEqual(len(result), 50)

    def test_default_max_220(self):
        long_text = "B" * 300
        result = srv._short_text(long_text)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 220)


class TestCanonicalizeEventUrl(unittest.TestCase):
    def test_valid_https_url_stripped(self):
        result = srv._canonicalize_event_url("https://Example.com/path?q=1#frag")
        self.assertEqual(result, "https://example.com/path")

    def test_valid_http_url_preserved(self):
        result = srv._canonicalize_event_url("http://example.com/news")
        self.assertEqual(result, "http://example.com/news")

    def test_javascript_scheme_returns_none(self):
        self.assertIsNone(srv._canonicalize_event_url("javascript:alert(1)"))

    def test_none_returns_none(self):
        self.assertIsNone(srv._canonicalize_event_url(None))

    def test_empty_string_returns_none(self):
        self.assertIsNone(srv._canonicalize_event_url(""))

    def test_file_scheme_returns_none(self):
        self.assertIsNone(srv._canonicalize_event_url("file:///etc/passwd"))

    def test_query_and_fragment_stripped(self):
        result = srv._canonicalize_event_url("https://news.com/article?utm_source=x&ref=y#top")
        self.assertEqual(result, "https://news.com/article")

    def test_scheme_lowercased(self):
        result = srv._canonicalize_event_url("HTTPS://Example.COM/path")
        self.assertIsNotNone(result)
        self.assertTrue(result.startswith("https://"))


class TestMakeDuplicateGroupId(unittest.TestCase):
    def test_returns_hex_string(self):
        gid = srv._make_duplicate_group_id("AAPL", "Earnings beat", "2026-05-01T12:00:00Z", "Apple Inc", "https://example.com/1")
        self.assertIsNotNone(gid)
        self.assertEqual(len(gid), 16)

    def test_same_inputs_same_id(self):
        g1 = srv._make_duplicate_group_id("AAPL", "Title", "2026-05-01", "Apple", "https://x.com")
        g2 = srv._make_duplicate_group_id("AAPL", "Title", "2026-05-01", "Apple", "https://x.com")
        self.assertEqual(g1, g2)

    def test_different_title_different_id(self):
        g1 = srv._make_duplicate_group_id("AAPL", "Title A", "2026-05-01", None, None)
        g2 = srv._make_duplicate_group_id("AAPL", "Title B", "2026-05-01", None, None)
        self.assertNotEqual(g1, g2)

    def test_empty_inputs_returns_none(self):
        self.assertIsNone(srv._make_duplicate_group_id("AAPL", "", "", None, ""))

    def test_url_canonicalized(self):
        g1 = srv._make_duplicate_group_id("AAPL", "T", "2026-05-01", None, "https://News.com/a?x=1")
        g2 = srv._make_duplicate_group_id("AAPL", "T", "2026-05-01", None, "https://news.com/a?y=2")
        self.assertEqual(g1, g2)


class TestSourceRank(unittest.TestCase):
    def test_sec_filing_highest_priority(self):
        self.assertEqual(srv._source_rank("sec_filing"), 0)

    def test_yahoo_finance_lower_priority_than_company_ir(self):
        self.assertGreater(srv._source_rank("yahoo_finance"), srv._source_rank("company_ir"))

    def test_unknown_source_maps_to_other(self):
        self.assertEqual(srv._source_rank("unknown_xyz"), srv._source_rank("other"))

    def test_none_maps_to_other(self):
        self.assertEqual(srv._source_rank(None), srv._source_rank("other"))

    def test_priority_ordering(self):
        priorities = [
            srv._source_rank("sec_filing"),
            srv._source_rank("company_ir"),
            srv._source_rank("press_release"),
            srv._source_rank("newswire"),
            srv._source_rank("company_news"),
            srv._source_rank("yahoo_finance"),
        ]
        self.assertEqual(priorities, sorted(priorities))


class TestSafeSecUrl(unittest.TestCase):
    def test_valid_sec_archives_url(self):
        url = "https://www.sec.gov/Archives/edgar/data/320193/000032019326000054/aapl-20260328.htm"
        self.assertEqual(srv._safe_sec_url(url), url)

    def test_non_sec_url_returns_none(self):
        self.assertIsNone(srv._safe_sec_url("https://example.com/Archives/file.htm"))

    def test_empty_returns_none(self):
        self.assertIsNone(srv._safe_sec_url(""))

    def test_none_returns_none(self):
        self.assertIsNone(srv._safe_sec_url(None))

    def test_sec_cgi_url_returns_none(self):
        self.assertIsNone(srv._safe_sec_url("https://www.sec.gov/cgi-bin/browse-edgar"))


class TestWithinDateWindow(unittest.TestCase):
    def test_none_iso_ts_returns_false(self):
        self.assertFalse(srv._within_date_window(None))

    def test_no_constraints_returns_true(self):
        self.assertTrue(srv._within_date_window("2026-05-15T12:00:00Z"))

    def test_start_date_before_event_returns_true(self):
        self.assertTrue(srv._within_date_window("2026-05-15T12:00:00Z", start_date="2026-05-01"))

    def test_start_date_after_event_returns_false(self):
        self.assertFalse(srv._within_date_window("2026-05-01T12:00:00Z", start_date="2026-05-10"))

    def test_end_date_after_event_returns_true(self):
        self.assertTrue(srv._within_date_window("2026-05-01T12:00:00Z", end_date="2026-05-31"))

    def test_end_date_before_event_returns_false(self):
        self.assertFalse(srv._within_date_window("2026-05-15T12:00:00Z", end_date="2026-05-01"))

    def test_lookback_days_recent_event_returns_true(self):
        today = datetime.datetime.now(datetime.timezone.utc)
        yesterday = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.assertTrue(srv._within_date_window(yesterday, lookback_days=30))

    def test_lookback_days_old_event_returns_false(self):
        old = "2020-01-01T00:00:00Z"
        self.assertFalse(srv._within_date_window(old, lookback_days=30))


class TestDedupeEventItems(unittest.TestCase):
    def test_passthrough_items_without_group_id(self):
        items = [_make_event(group_id=None), _make_event(group_id=None)]
        result = srv._dedupe_event_items(items, [])
        self.assertEqual(len(result), 2)

    def test_duplicate_group_id_keeps_one(self):
        items = [_make_event(group_id="g1"), _make_event(group_id="g1")]
        result = srv._dedupe_event_items(items, [])
        self.assertEqual(len(result), 1)

    def test_higher_priority_source_wins(self):
        sec_item = _make_event(source_type="sec_filing", group_id="g1")
        yf_item = _make_event(source_type="yahoo_finance", group_id="g1")
        result = srv._dedupe_event_items([yf_item, sec_item], [])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["sourceType"], "sec_filing")

    def test_timestamp_conflict_warning_emitted(self):
        a = _make_event(published_at="2026-05-15T10:00:00Z", group_id="g1")
        b = _make_event(published_at="2026-05-15T11:00:00Z", group_id="g1")
        warnings: list[dict] = []
        srv._dedupe_event_items([a, b], warnings)
        self.assertTrue(any(w.get("code") == "TIMESTAMP_CONFLICT" for w in warnings))

    def test_source_refs_merged(self):
        a = _make_event(source_type="sec_filing", group_id="g1")
        b = _make_event(source_type="yahoo_finance", group_id="g1")
        result = srv._dedupe_event_items([a, b], [])
        self.assertEqual(len(result[0]["sourceRefs"]), 1)

    def test_result_sorted_by_published_at_desc(self):
        items = [
            _make_event(published_at="2026-05-10T00:00:00Z", group_id=None),
            _make_event(published_at="2026-05-15T00:00:00Z", group_id=None),
            _make_event(published_at="2026-05-12T00:00:00Z", group_id=None),
        ]
        result = srv._dedupe_event_items(items, [])
        dates = [r["publishedAt"] for r in result]
        self.assertEqual(dates, sorted(dates, reverse=True))


class TestBuildCollectionStatus(unittest.TestCase):
    def test_items_and_no_unavailable_returns_none(self):
        self.assertIsNone(srv._build_collection_status([{"x": 1}], ["yahoo_finance"], []))

    def test_items_with_unavailable_returns_partial(self):
        warnings = [{"code": "SOURCE_UNAVAILABLE", "message": "Yahoo Finance unavailable."}]
        result = srv._build_collection_status([{"x": 1}], ["yahoo_finance"], warnings)
        self.assertEqual(result, "PARTIAL")

    def test_no_items_no_warnings_with_sources_returns_not_found(self):
        result = srv._build_collection_status([], ["yahoo_finance"], [])
        self.assertEqual(result, "NOT_FOUND")

    def test_no_items_unavailable_warning_returns_source_limited(self):
        warnings = [{"code": "SOURCE_UNAVAILABLE", "message": "Yahoo Finance unavailable."}]
        result = srv._build_collection_status([], [], warnings)
        self.assertEqual(result, "SOURCE_LIMITED_NOT_FOUND")

    def test_no_items_no_sources_returns_provider_error(self):
        result = srv._build_collection_status([], [], [])
        self.assertEqual(result, "PROVIDER_ERROR")


class TestComputeSourceStatus(unittest.TestCase):
    def test_yahoo_finance_ok_when_items_present(self):
        items = [{"sourceType": "yahoo_finance"}]
        result = srv._compute_source_status(["yahoo_finance"], [], items, ["yahoo_finance"])
        self.assertEqual(result["yahoo_finance"]["status"], "OK")
        self.assertEqual(result["yahoo_finance"]["rawCount"], 1)

    def test_yahoo_finance_empty_when_no_items(self):
        result = srv._compute_source_status(["yahoo_finance"], [], [], ["yahoo_finance"])
        self.assertEqual(result["yahoo_finance"]["status"], "EMPTY_RESULT")

    def test_finnhub_unconfigured_via_warning(self):
        warnings = [{"code": "SOURCE_UNAVAILABLE", "message": "Finnhub company-news source is not configured."}]
        result = srv._compute_source_status([], warnings, [], ["finnhub"])
        self.assertEqual(result["finnhub"]["status"], "UNCONFIGURED")

    def test_finnhub_ok_when_used(self):
        items = [{"source": "finnhub", "sourceType": "company_news"}]
        result = srv._compute_source_status(["finnhub"], [], items, ["finnhub"])
        self.assertEqual(result["finnhub"]["status"], "OK")

    def test_sec_ok_when_items_present(self):
        items = [{"sourceType": "sec_filing"}]
        result = srv._compute_source_status(["sec"], [], items, ["sec"])
        self.assertEqual(result["sec"]["status"], "OK")

    def test_only_requested_sources_in_result(self):
        result = srv._compute_source_status([], [], [], ["yahoo_finance"])
        self.assertIn("yahoo_finance", result)
        self.assertNotIn("finnhub", result)


class TestComputeSourceCoverage(unittest.TestCase):
    def test_full_when_all_ok(self):
        status = {
            "yahoo_finance": {"status": "OK"},
            "finnhub": {"status": "EMPTY_RESULT"},
        }
        self.assertEqual(srv._compute_source_coverage(status), "FULL")

    def test_partial_when_unconfigured(self):
        status = {"finnhub": {"status": "UNCONFIGURED"}}
        self.assertEqual(srv._compute_source_coverage(status), "PARTIAL")

    def test_partial_when_provider_error(self):
        status = {"yahoo_finance": {"status": "PROVIDER_ERROR"}}
        self.assertEqual(srv._compute_source_coverage(status), "PARTIAL")

    def test_partial_when_rate_limited(self):
        status = {"finnhub": {"status": "RATE_LIMITED"}}
        self.assertEqual(srv._compute_source_coverage(status), "PARTIAL")

    def test_full_empty_dict(self):
        self.assertEqual(srv._compute_source_coverage({}), "FULL")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. EDGAR helpers
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgarCikFromAccession(unittest.TestCase):
    def test_standard_accession(self):
        self.assertEqual(srv._edgar_cik_from_accession("0000024741-26-000124"), 24741)

    def test_apple_cik(self):
        self.assertEqual(srv._edgar_cik_from_accession("0000320193-23-000077"), 320193)

    def test_all_zeros_returns_none(self):
        self.assertIsNone(srv._edgar_cik_from_accession("0000000000-23-000001"))

    def test_empty_string_returns_none(self):
        self.assertIsNone(srv._edgar_cik_from_accession(""))

    def test_malformed_returns_none(self):
        self.assertIsNone(srv._edgar_cik_from_accession("not-an-accession"))


class TestEdgarBuildFilingUrls(unittest.TestCase):
    def test_index_url_format(self):
        index_url, _ = srv._edgar_build_filing_urls(320193, "0000320193-23-000077", None)
        self.assertIn("320193", index_url)
        self.assertIn("0000320193-23-000077-index.htm", index_url)

    def test_primary_url_when_doc_provided(self):
        _, primary_url = srv._edgar_build_filing_urls(320193, "0000320193-23-000077", "aapl-20230930.htm")
        self.assertIsNotNone(primary_url)
        self.assertIn("aapl-20230930.htm", primary_url)
        self.assertIn("000032019323000077", primary_url)

    def test_primary_url_none_when_no_doc(self):
        _, primary_url = srv._edgar_build_filing_urls(320193, "0000320193-23-000077", None)
        self.assertIsNone(primary_url)

    def test_dashes_removed_in_path(self):
        index_url, _ = srv._edgar_build_filing_urls(24741, "0000024741-26-000124", None)
        self.assertIn("000002474126000124", index_url)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Filing / XBRL helpers
# ═══════════════════════════════════════════════════════════════════════════════

class TestRegionMatches(unittest.TestCase):
    def test_simple_substring_match(self):
        self.assertTrue(srv._region_matches("United States", "United States"))

    def test_china_matches_greater_china(self):
        self.assertTrue(srv._region_matches("Greater China", "china"))

    def test_china_matches_xbrl_member(self):
        self.assertTrue(srv._region_matches("srt:ChinaMember", "china"))

    def test_china_matches_country_cn(self):
        self.assertTrue(srv._region_matches("country:CN", "china"))

    def test_asia_fallback_not_triggered_by_default(self):
        self.assertFalse(srv._region_matches("AsiaPacificMember", "china"))

    def test_asia_fallback_triggered_when_enabled(self):
        self.assertTrue(srv._region_matches("AsiaPacificMember", "china", include_asia_fallback=True))

    def test_greater_china_compact_form(self):
        self.assertTrue(srv._region_matches("GreaterChinaMember", "greater china"))

    def test_no_match_returns_false(self):
        self.assertFalse(srv._region_matches("Europe", "china"))

    def test_region_compact_no_space(self):
        self.assertTrue(srv._region_matches("northamerica", "North America"))


class TestNormalizeSegmentLabel(unittest.TestCase):
    def test_string_input(self):
        self.assertEqual(srv._normalize_segment_label("China"), "China")

    def test_dict_input_joins_values(self):
        result = srv._normalize_segment_label({"dim": "GreaterChina", "val": "Member"})
        self.assertIn("GreaterChina", result)
        self.assertIn("Member", result)

    def test_dict_skips_none_values(self):
        result = srv._normalize_segment_label({"a": "China", "b": None})
        self.assertNotIn("None", result)

    def test_list_input_joins_elements(self):
        result = srv._normalize_segment_label(["US", "Canada"])
        self.assertIn("US", result)
        self.assertIn("Canada", result)

    def test_none_returns_empty_string(self):
        self.assertEqual(srv._normalize_segment_label(None), "")


class TestAsStatus(unittest.TestCase):
    def test_not_disclosed_source(self):
        self.assertEqual(srv._as_status({"source": "NOT_DISCLOSED"}), "NOT_DISCLOSED")

    def test_not_disclosed_confidence(self):
        self.assertEqual(srv._as_status({"confidence": "NOT_DISCLOSED"}), "NOT_DISCLOSED")

    def test_conflicting_source(self):
        self.assertEqual(srv._as_status({"source": "CONFLICTING"}), "CONFLICTING")

    def test_conflicting_confidence(self):
        self.assertEqual(srv._as_status({"confidence": "CONFLICTING"}), "CONFLICTING")

    def test_empty_dict_returns_not_found(self):
        self.assertEqual(srv._as_status({}), "NOT_FOUND")


# ═══════════════════════════════════════════════════════════════════════════════
# 6. JSON / text helpers
# ═══════════════════════════════════════════════════════════════════════════════

class TestSafeParse(unittest.TestCase):
    def test_valid_json_string(self):
        result = srv._safe_parse('{"ticker": "AAPL", "price": 150}', "AAPL")
        self.assertEqual(result["ticker"], "AAPL")

    def test_exception_input_returns_error_dict(self):
        err = ValueError("fetch failed")
        result = srv._safe_parse(err, "MSFT")
        self.assertTrue(result["error"])
        self.assertIn("fetch failed", result["message"])
        self.assertEqual(result["ticker"], "MSFT")

    def test_non_json_string_returns_error_dict(self):
        result = srv._safe_parse("not json at all", "TSLA")
        self.assertTrue(result["error"])
        self.assertEqual(result["ticker"], "TSLA")


class TestSafeJsonLoads(unittest.TestCase):
    def test_valid_dict_json(self):
        result = srv._safe_json_loads('{"key": "value"}')
        self.assertEqual(result["key"], "value")

    def test_invalid_json_returns_empty_dict(self):
        self.assertEqual(srv._safe_json_loads("not json"), {})

    def test_non_dict_json_returns_empty_dict(self):
        self.assertEqual(srv._safe_json_loads("[1, 2, 3]"), {})

    def test_empty_string_returns_empty_dict(self):
        self.assertEqual(srv._safe_json_loads(""), {})


class TestCompactExcerpt(unittest.TestCase):
    def test_short_text_unchanged(self):
        self.assertEqual(srv._compact_excerpt("Hello world", 240), "Hello world")

    def test_long_text_truncated_with_ellipsis(self):
        result = srv._compact_excerpt("A" * 300, 240)
        self.assertTrue(result.endswith("..."))
        self.assertLessEqual(len(result), 243)

    def test_whitespace_normalized(self):
        result = srv._compact_excerpt("hello\n  world\t\tfoo")
        self.assertEqual(result, "hello world foo")

    def test_none_returns_empty_string(self):
        self.assertEqual(srv._compact_excerpt(None), "")


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Alias helper
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeprecatedAliasResponse(unittest.TestCase):
    def setUp(self):
        self._orig = srv._ENVELOPE_V2
        srv._ENVELOPE_V2 = True

    def tearDown(self):
        srv._ENVELOPE_V2 = self._orig

    def test_injects_deprecated_alias_warning(self):
        inner = json.dumps(srv._mcp_success("canonical_tool", json.dumps({"x": 1})))
        result = json.loads(srv._deprecated_alias_response("alias_tool", "canonical_tool", inner))
        warnings = result["meta"]["warnings"]
        self.assertTrue(any(w.get("code") == "DEPRECATED_ALIAS" for w in warnings))

    def test_meta_tool_set_to_alias(self):
        inner = json.dumps(srv._mcp_success("canonical_tool", json.dumps({"x": 1})))
        result = json.loads(srv._deprecated_alias_response("alias_tool", "canonical_tool", inner))
        self.assertEqual(result["meta"]["tool"], "alias_tool")

    def test_meta_canonical_tool_set(self):
        inner = json.dumps(srv._mcp_success("canonical_tool", json.dumps({"x": 1})))
        result = json.loads(srv._deprecated_alias_response("alias_tool", "canonical_tool", inner))
        self.assertEqual(result["meta"]["canonicalTool"], "canonical_tool")

    def test_deprecated_tool_flag_true(self):
        inner = json.dumps(srv._mcp_success("canonical_tool", json.dumps({"x": 1})))
        result = json.loads(srv._deprecated_alias_response("alias_tool", "canonical_tool", inner))
        self.assertTrue(result["meta"]["deprecatedTool"])

    def test_use_instead_set(self):
        inner = json.dumps(srv._mcp_success("canonical_tool", json.dumps({"x": 1})))
        result = json.loads(srv._deprecated_alias_response("alias_tool", "canonical_tool", inner))
        self.assertEqual(result["meta"]["useInstead"], "canonical_tool")

    def test_envelope_off_returns_raw(self):
        srv._ENVELOPE_V2 = False
        raw = '{"raw": "data"}'
        result = srv._deprecated_alias_response("alias_tool", "canonical_tool", raw)
        self.assertEqual(result, raw)


# ═══════════════════════════════════════════════════════════════════════════════
# 8. Earnings helpers
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsPaywalledUrl(unittest.TestCase):
    def test_seeking_alpha_is_paywalled(self):
        self.assertTrue(srv._is_paywalled_url("https://seekingalpha.com/article/12345"))

    def test_wsj_is_paywalled(self):
        self.assertTrue(srv._is_paywalled_url("https://www.wsj.com/articles/story"))

    def test_bloomberg_is_paywalled(self):
        self.assertTrue(srv._is_paywalled_url("https://www.bloomberg.com/news/article"))

    def test_sec_gov_not_paywalled(self):
        self.assertFalse(srv._is_paywalled_url("https://www.sec.gov/Archives/edgar/data/320193/aapl.htm"))

    def test_company_ir_not_paywalled(self):
        self.assertFalse(srv._is_paywalled_url("https://investor.apple.com/news-releases/2026"))


class TestClassifyEarningsSourceUrl(unittest.TestCase):
    def test_sec_archives_classified_as_sec_8k(self):
        url = "https://www.sec.gov/Archives/edgar/data/320193/000032019326000001/aapl-8k.htm"
        source_type, error = srv._classify_earnings_source_url(url)
        self.assertEqual(source_type, "sec_8k")
        self.assertIsNone(error)

    def test_company_ir_url_classified(self):
        url = "https://investor.apple.com/news-releases/2026/earnings.html"
        source_type, error = srv._classify_earnings_source_url(url)
        self.assertEqual(source_type, "company_ir")
        self.assertIsNone(error)

    def test_paywalled_url_rejected(self):
        url = "https://seekingalpha.com/article/12345"
        source_type, error = srv._classify_earnings_source_url(url)
        self.assertIsNone(source_type)
        self.assertIsNotNone(error)
        self.assertIn("paywalled", error)

    def test_non_https_rejected(self):
        url = "http://investor.apple.com/news"
        source_type, error = srv._classify_earnings_source_url(url)
        self.assertIsNone(source_type)
        self.assertIn("https", error)

    def test_empty_string_rejected(self):
        source_type, error = srv._classify_earnings_source_url("")
        self.assertIsNone(source_type)
        self.assertIsNotNone(error)

    def test_none_like_input_rejected(self):
        source_type, error = srv._classify_earnings_source_url("   ")
        self.assertIsNone(source_type)


class TestScaleNumberFromText(unittest.TestCase):
    def test_plain_number(self):
        self.assertAlmostEqual(srv._scale_number_from_text("42.5"), 42.5)

    def test_billion_suffix(self):
        self.assertAlmostEqual(srv._scale_number_from_text("2.5 billion"), 2_500_000_000)

    def test_million_suffix(self):
        self.assertAlmostEqual(srv._scale_number_from_text("500 million"), 500_000_000)

    def test_thousand_suffix(self):
        self.assertAlmostEqual(srv._scale_number_from_text("10 thousand"), 10_000)

    def test_comma_separated(self):
        self.assertAlmostEqual(srv._scale_number_from_text("1,234"), 1234)

    def test_none_returns_none(self):
        self.assertIsNone(srv._scale_number_from_text(None))

    def test_empty_string_returns_none(self):
        self.assertIsNone(srv._scale_number_from_text(""))

    def test_no_number_returns_none(self):
        self.assertIsNone(srv._scale_number_from_text("no numbers here"))

    def test_negative_number(self):
        val = srv._scale_number_from_text("-3.2 million")
        self.assertAlmostEqual(val, -3_200_000)


class TestFirstSentenceForTopic(unittest.TestCase):
    def test_returns_matching_sentence(self):
        text = "Revenue grew 10%. China revenue was $5B. Margins improved."
        result = srv._first_sentence_for_topic(text, "China")
        self.assertIsNotNone(result)
        self.assertIn("China", result)

    def test_returns_none_when_no_match(self):
        text = "Revenue grew 10%. Margins improved greatly."
        self.assertIsNone(srv._first_sentence_for_topic(text, "China"))

    def test_case_insensitive_match(self):
        text = "CHINA REVENUE was significant."
        result = srv._first_sentence_for_topic(text, "china")
        self.assertIsNotNone(result)


class TestExtractMetricNumber(unittest.TestCase):
    def test_matches_first_pattern(self):
        val, raw, excerpt = srv._extract_metric_number(
            "Revenue was $2.5 billion in Q3",
            [r"\$(\d+(?:\.\d+)?\s*billion)"],
        )
        self.assertIsNotNone(val)
        self.assertAlmostEqual(val, 2_500_000_000)

    def test_returns_none_when_no_match(self):
        val, raw, excerpt = srv._extract_metric_number("No numbers here", [r"\$(\d+ billion)"])
        self.assertIsNone(val)
        self.assertIsNone(raw)
        self.assertIsNone(excerpt)

    def test_tries_multiple_patterns(self):
        val, raw, excerpt = srv._extract_metric_number(
            "EPS was 1.23",
            [r"no_match_here", r"EPS was (\d+\.\d+)"],
        )
        self.assertIsNotNone(val)
        self.assertAlmostEqual(val, 1.23)


# ═══════════════════════════════════════════════════════════════════════════════
# 9. SEC filing index builder (includes XSS sanitization coverage)
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildFilingIndexFromHtml(unittest.TestCase):
    _SIMPLE_HTML = """
    <html><body>
    <h1>Annual Report</h1>
    <h2>Risk Factors</h2>
    <p>Some risk discussion (in millions).</p>
    <table>
      <tr><th>Region</th><th>2025</th><th>2024</th></tr>
      <tr><td>United States</td><td>6,720</td><td>6,500</td></tr>
      <tr><td>China</td><td>2,840</td><td>2,600</td></tr>
      <tr><td>Total</td><td>9,560</td><td>9,100</td></tr>
    </table>
    </body></html>
    """

    def test_sections_extracted(self):
        result = srv._build_filing_index_from_html(self._SIMPLE_HTML)
        headings = [s["heading"] for s in result["sections"]]
        self.assertIn("Annual Report", headings)
        self.assertIn("Risk Factors", headings)

    def test_section_level_correct(self):
        result = srv._build_filing_index_from_html(self._SIMPLE_HTML)
        h1 = next(s for s in result["sections"] if s["heading"] == "Annual Report")
        h2 = next(s for s in result["sections"] if s["heading"] == "Risk Factors")
        self.assertEqual(h1["level"], 1)
        self.assertEqual(h2["level"], 2)

    def test_tables_extracted(self):
        result = srv._build_filing_index_from_html(self._SIMPLE_HTML)
        self.assertGreaterEqual(len(result["tables"]), 1)

    def test_table_headers_extracted(self):
        result = srv._build_filing_index_from_html(self._SIMPLE_HTML)
        headers = result["tables"][0]["headers"]
        self.assertIn("Region", headers)

    def test_table_row_labels_extracted(self):
        result = srv._build_filing_index_from_html(self._SIMPLE_HTML)
        row_labels = result["tables"][0]["rowLabels"]
        self.assertTrue(any("China" in lbl for lbl in row_labels))

    def test_keyword_map_includes_risk_factors(self):
        result = srv._build_filing_index_from_html(self._SIMPLE_HTML)
        self.assertIn("risk factors", result["keywordMap"])

    def test_unit_scale_detected_millions(self):
        result = srv._build_filing_index_from_html(self._SIMPLE_HTML)
        self.assertEqual(result["tables"][0]["unitScale"], "millions")

    def test_script_tags_removed(self):
        html = """
        <h1>Report</h1>
        <script>alert('xss')</script>
        <table><tr><th>2025</th></tr><tr><td>100</td></tr></table>
        """
        result = srv._build_filing_index_from_html(html)
        # Script content must not appear in any extracted text
        all_text = json.dumps(result)
        self.assertNotIn("alert", all_text)

    def test_style_tags_removed(self):
        html = """
        <style>body { color: red; }</style>
        <h2>Revenue</h2>
        """
        result = srv._build_filing_index_from_html(html)
        all_text = json.dumps(result)
        self.assertNotIn("color: red", all_text)

    def test_event_handlers_removed(self):
        html = '<h1 onclick="evil()">Revenue</h1>'
        result = srv._build_filing_index_from_html(html)
        all_text = json.dumps(result)
        self.assertNotIn("evil()", all_text)

    def test_empty_html_returns_empty_structures(self):
        result = srv._build_filing_index_from_html("")
        self.assertEqual(result["sections"], [])
        self.assertEqual(result["tables"], [])
        self.assertEqual(result["keywordMap"], {})

    def test_nested_script_not_bypassed(self):
        html = "<h1>Report</h1><scr<script>ipt>alert('nested')</script>"
        result = srv._build_filing_index_from_html(html)
        all_text = json.dumps(result)
        self.assertNotIn("alert", all_text)


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(__import__(__name__))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
