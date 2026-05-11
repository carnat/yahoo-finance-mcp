#!/usr/bin/env python3
"""
Unit tests for the EDGAR HTML parsing helpers introduced by PR #36.

These tests exercise _extract_geo_revenue_from_html and its supporting
helpers (_parse_html_table, _parse_numeric_cell, _detect_unit_multiplier,
_strip_html_tags) with synthetic HTML that mirrors the structure of a real
Corning (GLW) 10-K Note 20 – Geographic Information section.

No external network calls are made.  All test input is embedded below.

Run:
    python scripts/test_edgar_html_parse.py
    # or via pytest:
    pytest scripts/test_edgar_html_parse.py -v
"""

import sys
import os
import unittest
import asyncio
from unittest import mock

# Allow importing from repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from server import (  # type: ignore[import]
    _strip_html_tags,
    _parse_html_table,
    _parse_numeric_cell,
    _detect_unit_multiplier,
    _extract_geo_revenue_from_html,
    _edgar_primary_doc_from_index,
)


# ── Synthetic 10-K HTML fixtures ─────────────────────────────────────────────

# Mirrors GLW's Note 20 – Geographic Information (values in $ millions)
MOCK_GLW_NOTE_20 = """\
<html><body>
<h3>Note 20 – Geographic Information</h3>
<p>The following table presents information about our revenues disaggregated
by geography (in millions):</p>
<table>
  <tr><th>Geography</th><th>2025</th><th>2024</th></tr>
  <tr><td>United States</td><td>$6,720</td><td>$6,500</td></tr>
  <tr><td>China</td><td>$2,840</td><td>$2,600</td></tr>
  <tr><td>Europe</td><td>$1,980</td><td>$1,900</td></tr>
  <tr><td>Asia Pacific</td><td>$2,590</td><td>$2,400</td></tr>
  <tr><td>Other</td><td>$1,500</td><td>$1,400</td></tr>
  <tr><td><b>Total</b></td><td>$15,630</td><td>$14,800</td></tr>
</table>
<p>China revenues include sales to customers whose headquarters are in mainland China.</p>
</body></html>
"""

# Variant: "revenues" spelled as "Revenues" in the total row label
MOCK_TOTAL_LABEL_VARIANT = """\
<html><body>
<h4>Geographic Information</h4>
<p>Net revenues by geography (in millions):</p>
<table>
  <tr><th>Region</th><th>FY2025</th></tr>
  <tr><td>Americas</td><td>7,100</td></tr>
  <tr><td>China</td><td>3,200</td></tr>
  <tr><td>EMEA</td><td>2,100</td></tr>
  <tr><td>Revenues</td><td>13,400</td></tr>
</table>
</body></html>
"""

# Variant: parentheses notation for values, billions unit
MOCK_BILLIONS_UNIT = """\
<html><body>
<p>The following summarizes revenues by geography (in billions):</p>
<h2>Revenue by Region</h2>
<table>
  <tr><th>Area</th><th>2025</th></tr>
  <tr><td>China</td><td>$2.84</td></tr>
  <tr><td>United States</td><td>$6.72</td></tr>
  <tr><td>Other</td><td>$6.02</td></tr>
  <tr><td>Total</td><td>$15.58</td></tr>
</table>
</body></html>
"""

# Variant: no "geographic" keyword, just the region name in a table
MOCK_NO_GEO_KEYWORD = """\
<html><body>
<h5>Segment Revenue Table</h5>
<table>
  <tr><th>Segment</th><th>Revenue</th></tr>
  <tr><td>North America</td><td>8,000</td></tr>
  <tr><td>China</td><td>1,600</td></tr>
  <tr><td>Total</td><td>12,000</td></tr>
</table>
</body></html>
"""

# Variant: China not in HTML at all
MOCK_NO_CHINA = """\
<html><body>
<h3>Geographic Information</h3>
<table>
  <tr><th>Region</th><th>Revenue</th></tr>
  <tr><td>Americas</td><td>10,000</td></tr>
  <tr><td>EMEA</td><td>3,000</td></tr>
  <tr><td>APAC</td><td>2,000</td></tr>
  <tr><td>Total</td><td>15,000</td></tr>
</table>
</body></html>
"""

# Variant: nested tables (outer layout table + inner data table)
MOCK_NESTED_TABLES = """\
<html><body>
<h3>Note 20 Geographic Information</h3>
<table class="layout">
  <tr><td>
    <table class="data">
      <tr><th>Country</th><th>Revenue ($M)</th></tr>
      <tr><td>United States</td><td>6,720</td></tr>
      <tr><td>China</td><td>2,840</td></tr>
      <tr><td>Other</td><td>6,070</td></tr>
      <tr><td>Total</td><td>15,630</td></tr>
    </table>
  </td></tr>
</table>
</body></html>
"""

MOCK_FILING_INDEX_SEQ1 = """\
<html><body>
<table>
  <tr>
    <td>1</td>
    <td>10-K</td>
    <td><a href="glw-20251231.htm">glw-20251231.htm</a></td>
  </tr>
  <tr>
    <td>2</td>
    <td>EX-21</td>
    <td><a href="glw-ex21.htm">glw-ex21.htm</a></td>
  </tr>
</table>
</body></html>
"""

MOCK_FILING_INDEX_IXVIEWER = """\
<html><body>
<table>
  <tr>
    <td>8</td>
    <td>EX-31.1</td>
    <td><a href="ex31.htm">ex31.htm</a></td>
  </tr>
  <tr>
    <td>9</td>
    <td>10-K</td>
    <td><a href="/ixviewer/ix.html?doc=/Archives/edgar/data/24741/000002474126000124/glw-20251231.htm">view</a></td>
  </tr>
</table>
</body></html>
"""


# ── Test class ────────────────────────────────────────────────────────────────

class TestStripHtmlTags(unittest.TestCase):
    def test_removes_tags(self):
        self.assertEqual(_strip_html_tags("<b>Hello</b>"), "Hello")

    def test_decodes_entities(self):
        self.assertEqual(_strip_html_tags("A &amp; B"), "A & B")
        self.assertEqual(_strip_html_tags("&lt;tag&gt;"), "<tag>")
        self.assertEqual(_strip_html_tags("&nbsp;"), "")

    def test_collapses_whitespace(self):
        self.assertEqual(_strip_html_tags("  foo   bar  "), "foo bar")

    def test_empty_string(self):
        self.assertEqual(_strip_html_tags(""), "")


class TestParseNumericCell(unittest.TestCase):
    def test_integer(self):
        self.assertEqual(_parse_numeric_cell("1234"), 1234.0)

    def test_comma_separated(self):
        self.assertEqual(_parse_numeric_cell("1,234,567"), 1234567.0)

    def test_dollar_sign(self):
        self.assertEqual(_parse_numeric_cell("$6,720"), 6720.0)

    def test_parentheses_negative(self):
        self.assertEqual(_parse_numeric_cell("(500)"), -500.0)

    def test_millions_suffix(self):
        self.assertAlmostEqual(_parse_numeric_cell("2.84M"), 2_840_000.0)

    def test_billions_suffix(self):
        self.assertAlmostEqual(_parse_numeric_cell("15.63B"), 15_630_000_000.0)

    def test_non_numeric(self):
        self.assertIsNone(_parse_numeric_cell("China"))
        self.assertIsNone(_parse_numeric_cell(""))
        self.assertIsNone(_parse_numeric_cell("N/A"))

    def test_decimal(self):
        self.assertAlmostEqual(_parse_numeric_cell("$2,840.5"), 2840.5)


class TestDetectUnitMultiplier(unittest.TestCase):
    def test_millions(self):
        self.assertEqual(_detect_unit_multiplier("in millions", ""), 1_000_000.0)

    def test_billions(self):
        self.assertEqual(_detect_unit_multiplier("in billions", ""), 1_000_000_000.0)

    def test_thousands(self):
        self.assertEqual(_detect_unit_multiplier("(in thousands)", ""), 1_000.0)

    def test_default_millions(self):
        # No unit indicator → default to millions
        self.assertEqual(_detect_unit_multiplier("", ""), 1_000_000.0)

    def test_context_wins(self):
        # Unit in context text (before table) rather than table header
        self.assertEqual(_detect_unit_multiplier("", "expressed in billions of dollars"), 1_000_000_000.0)


class TestParseHtmlTable(unittest.TestCase):
    def test_basic_table(self):
        html = "<table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>2</td></tr></table>"
        rows = _parse_html_table(html)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], ["A", "B"])
        self.assertEqual(rows[1], ["1", "2"])

    def test_strips_tags_in_cells(self):
        html = "<table><tr><td><b>China</b></td><td>$2,840</td></tr></table>"
        rows = _parse_html_table(html)
        self.assertEqual(rows[0][0], "China")
        self.assertEqual(rows[0][1], "$2,840")

    def test_empty_table(self):
        rows = _parse_html_table("<table></table>")
        self.assertEqual(rows, [])

    def test_entities_decoded(self):
        html = "<table><tr><td>US&amp;Canada</td><td>100</td></tr></table>"
        rows = _parse_html_table(html)
        self.assertEqual(rows[0][0], "US&Canada")


class TestExtractGeoRevenueFromHtml(unittest.TestCase):
    """Integration tests for the full extraction pipeline."""

    # ── Basic GLW-like fixture ────────────────────────────────────────────

    def test_glw_note20_china_pct(self):
        pct, usd, heading, _ = _extract_geo_revenue_from_html(MOCK_GLW_NOTE_20, "China")
        self.assertIsNotNone(pct, "regionRevenuePct must not be None")
        self.assertAlmostEqual(pct, 2840 / 15630, places=3,
                               msg="China pct should be ~18.2% of total")
        self.assertIsNotNone(usd, "regionRevenueUSD must not be None")
        # usd = 2840 * 1_000_000 (millions default)
        self.assertAlmostEqual(usd, 2840 * 1_000_000, delta=1,
                               msg="USD value should be 2840M when unit is millions")

    def test_glw_note20_heading_detected(self):
        _, _, heading, _ = _extract_geo_revenue_from_html(MOCK_GLW_NOTE_20, "China")
        self.assertIn("geographic", heading.lower(),
                      msg=f"Section heading should mention geographic, got: {heading!r}")

    def test_glw_note20_tables_returned(self):
        _, _, _, tables = _extract_geo_revenue_from_html(MOCK_GLW_NOTE_20, "China")
        self.assertTrue(len(tables) > 0, "At least one parsed table should be returned")
        rows = tables[0]["rows"]
        self.assertTrue(len(rows) >= 3, f"Table must have at least 3 rows, got {len(rows)}")

    def test_china_not_present_returns_none(self):
        pct, usd, _, _ = _extract_geo_revenue_from_html(MOCK_NO_CHINA, "China")
        self.assertIsNone(pct, "Should return None when China not in table")
        self.assertIsNone(usd)

    # ── Alternative label for total row ──────────────────────────────────

    def test_total_label_revenues(self):
        pct, usd, _, _ = _extract_geo_revenue_from_html(MOCK_TOTAL_LABEL_VARIANT, "China")
        self.assertIsNotNone(pct, "Should find China with 'Revenues' total row label")
        expected = 3200 / 13400
        self.assertAlmostEqual(pct, expected, places=3,
                               msg=f"China pct should be ~{expected*100:.1f}%")

    # ── Billions unit scale ───────────────────────────────────────────────

    def test_billions_unit_scale(self):
        pct, usd, _, _ = _extract_geo_revenue_from_html(MOCK_BILLIONS_UNIT, "China")
        self.assertIsNotNone(pct)
        expected_pct = 2.84 / 15.58
        self.assertAlmostEqual(pct, expected_pct, places=2,
                               msg=f"China pct should be ~{expected_pct*100:.1f}% (billions)")
        # 2.84 * 1e9 = 2,840,000,000
        self.assertAlmostEqual(usd, 2.84 * 1_000_000_000, delta=1_000,
                               msg="USD must use billions multiplier")

    # ── Fallback: no 'geographic' keyword, just the region in a table ────

    def test_no_geo_keyword_falls_back_to_region_name(self):
        pct, _, _, _ = _extract_geo_revenue_from_html(MOCK_NO_GEO_KEYWORD, "China")
        self.assertIsNotNone(pct, "Should find China even without 'geographic' keyword")
        expected = 1600 / 12000
        self.assertAlmostEqual(pct, expected, places=3)

    # ── Nested tables ─────────────────────────────────────────────────────

    def test_nested_tables(self):
        pct, _, heading, _ = _extract_geo_revenue_from_html(MOCK_NESTED_TABLES, "China")
        self.assertIsNotNone(pct, "Should parse inner data table within nested layout table")
        expected = 2840 / 15630
        self.assertAlmostEqual(pct, expected, places=3)

    # ── Edge cases ────────────────────────────────────────────────────────

    def test_empty_html(self):
        pct, usd, heading, tables = _extract_geo_revenue_from_html("", "China")
        self.assertIsNone(pct)
        self.assertIsNone(usd)
        self.assertEqual(heading, "")
        self.assertEqual(tables, [])

    def test_html_with_no_tables(self):
        html = "<html><body><h3>Geographic Information</h3><p>China revenues were significant.</p></body></html>"
        pct, usd, _, _ = _extract_geo_revenue_from_html(html, "China")
        self.assertIsNone(pct, "No table → should return None")

    def test_case_insensitive_region_match(self):
        # Table has mixed-case "CHINA" — should still match region="China"
        html = """\
<html><body>
<h3>Geographic Areas</h3>
<table>
  <tr><th>Region</th><th>Revenue</th></tr>
  <tr><td>CHINA</td><td>2,840</td></tr>
  <tr><td>Total</td><td>15,630</td></tr>
</table>
</body></html>
"""
        pct, _, _, _ = _extract_geo_revenue_from_html(html, "China")
        self.assertIsNotNone(pct)

    def test_region_with_parentheses_negative_value(self):
        # Verify parentheses values in non-total cells don't break extraction
        html = """\
<html><body>
<h3>Geographic Information</h3>
<table>
  <tr><th>Region</th><th>Revenue</th></tr>
  <tr><td>China</td><td>2,840</td></tr>
  <tr><td>Adjustments</td><td>(100)</td></tr>
  <tr><td>Total</td><td>15,630</td></tr>
</table>
</body></html>
"""
        pct, _, _, _ = _extract_geo_revenue_from_html(html, "China")
        self.assertIsNotNone(pct)
        self.assertAlmostEqual(pct, 2840 / 15630, places=3)


class TestEdgarPrimaryDocFromIndex(unittest.TestCase):
    def _run_with_html(self, html: str) -> str | None:
        with mock.patch("server._edgar_get_html", new=mock.AsyncMock(return_value=html)):
            return asyncio.run(_edgar_primary_doc_from_index("https://example.com/index.htm"))

    def test_prefers_seq1_or_10k_row(self):
        fname = self._run_with_html(MOCK_FILING_INDEX_SEQ1)
        self.assertEqual(fname, "glw-20251231.htm")

    def test_parses_ixviewer_doc_query_param(self):
        fname = self._run_with_html(MOCK_FILING_INDEX_IXVIEWER)
        self.assertEqual(fname, "glw-20251231.htm")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Run with verbose output
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
