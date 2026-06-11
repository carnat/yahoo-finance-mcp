"""Geographic revenue and XBRL extraction helpers.

Extracted from server.py in Phase 1 of the refactoring plan.
"""

import datetime
import re as _re

from yfmcp.parsing.html import (
    _parse_html_table,
    _parse_numeric_cell,
    _detect_unit_multiplier,
    _strip_html_tags,
)


# ---------------------------------------------------------------------------
# Segment / region label helpers
# ---------------------------------------------------------------------------

def _normalize_segment_label(segment: object) -> str:
    if isinstance(segment, dict):
        return " ".join(str(v) for v in segment.values() if v is not None)
    if isinstance(segment, list):
        return " ".join(_normalize_segment_label(s) for s in segment)
    return str(segment or "")


def _region_matches(label: str, region: str, include_asia_fallback: bool = False) -> bool:
    label_low = label.lower()
    region_low = region.lower()
    if region_low in label_low:
        return True
    # Also try compact (no-space) region for XBRL member names like "GreaterChinaMember"
    region_compact = region_low.replace(" ", "")
    if region_compact and region_compact in label_low:
        return True
    if region_low == "china":
        base_tokens = ("country:cn", "greater china", "srt:chinamember", "greaterchina")
        if any(token in label_low for token in base_tokens):
            return True
        return include_asia_fallback and "asiapacificmember" in label_low
    if region_low == "greater china":
        if "greaterchina" in label_low or "greater china" in label_low:
            return True
    return False


# ---------------------------------------------------------------------------
# HTML geographic revenue extractor
# ---------------------------------------------------------------------------

def _extract_geo_revenue_from_html(
    html_text: str,
    region: str,
) -> tuple[float | None, float | None, float | None, str, dict | None]:
    """Search an SEC filing HTML document for a geographic revenue table.

    Returns (regionRevenueRatio, regionRevenueUSD, totalRevenueUSD, sectionHeading, evidence).
    Parses the first table that contains the target region and a numeric total row.
    """
    region_lower = region.lower()
    html_lower = html_text.lower()

    # Candidate search terms ordered by specificity
    search_terms = [
        "geographic information",
        "geographic areas",
        "geographic segment",
        "revenue by region",
        "revenues by geography",
        region_lower,
    ]

    # Collect positions of all search-term matches (cap to keep runtime bounded)
    term_positions: list[int] = []
    for term in search_terms:
        idx = 0
        while len(term_positions) < 30:
            pos = html_lower.find(term, idx)
            if pos == -1:
                break
            term_positions.append(pos)
            idx = pos + 1

    if not term_positions:
        return None, None, None, "", None

    # For each match, find the nearest enclosing or following <table>
    checked_tables: set[int] = set()
    candidate_tables: list[dict] = []

    for pos in sorted(set(term_positions))[:20]:
        # Search window: 1 000 chars before match → 60 000 chars after
        search_start = max(0, pos - 1_000)
        search_end = min(len(html_text), pos + 60_000)
        chunk = html_text[search_start:search_end]

        for tbl_m in _re.finditer(r"<table[^>]*>", chunk, _re.IGNORECASE):
            abs_start = search_start + tbl_m.start()
            if abs_start in checked_tables:
                continue
            checked_tables.add(abs_start)

            # Walk forward tracking nested table depth to find matching </table>
            depth = 0
            i = abs_start
            table_end = abs_start
            while i < min(len(html_text), abs_start + 200_000):
                o = html_lower.find("<table", i)
                c = html_lower.find("</table>", i)
                if o == -1 and c == -1:
                    break
                if o != -1 and (c == -1 or o < c):
                    depth += 1
                    i = o + 6
                else:
                    depth -= 1
                    if depth == 0:
                        table_end = c + 8
                        break
                    i = c + 8

            table_html = html_text[abs_start:table_end]
            if region_lower not in table_html.lower():
                continue

            parsed = _parse_html_table(table_html)
            if len(parsed) < 2:
                continue

            candidate_tables.append({
                "pos": abs_start,
                "table_html": table_html,
                "rows": parsed,
            })

    if not candidate_tables:
        return None, None, None, "", None

    _TOTAL_LABELS = frozenset({
        "total", "consolidated", "total revenues", "total net revenues",
        "net revenues", "revenues", "total revenue",
    })

    for tbl in candidate_tables:
        rows: list[list[str]] = tbl["rows"]

        # Find the row index for the target region
        region_row_idx: int | None = None
        for i, row in enumerate(rows):
            if any(region_lower in cell.lower() for cell in row):
                region_row_idx = i
                break
        if region_row_idx is None:
            continue

        # Find a "Total" row
        total_row_idx: int | None = None
        for i, row in enumerate(rows):
            if any(cell.strip().lower() in _TOTAL_LABELS for cell in row):
                total_row_idx = i
                break
        if total_row_idx is None:
            # Fall back: last row that has any numeric value
            for i in range(len(rows) - 1, -1, -1):
                if any(_parse_numeric_cell(c) is not None for c in rows[i]):
                    total_row_idx = i
                    break

        if total_row_idx is None or total_row_idx == region_row_idx:
            continue

        # Find the first numeric column in the region row (skip label column)
        region_row = rows[region_row_idx]
        value_col: int | None = None
        for j, cell in enumerate(region_row):
            v = _parse_numeric_cell(cell)
            if v is not None and v > 0:
                value_col = j
                break
        if value_col is None:
            continue

        region_val = _parse_numeric_cell(
            rows[region_row_idx][value_col] if value_col < len(rows[region_row_idx]) else ""
        )
        total_val = _parse_numeric_cell(
            rows[total_row_idx][value_col] if value_col < len(rows[total_row_idx]) else ""
        )

        if region_val is None or total_val is None or total_val <= 0:
            continue

        ratio = round(region_val / total_val, 4)

        # Detect unit scale for USD conversion
        context_html = html_text[max(0, tbl["pos"] - 3_000): tbl["pos"]]
        unit_mult = _detect_unit_multiplier(tbl["table_html"], context_html)
        region_usd = region_val * unit_mult
        total_usd = total_val * unit_mult

        # Extract nearest section heading (last <h*> tag before the table)
        heading = ""
        pre_html = html_text[max(0, tbl["pos"] - 6_000): tbl["pos"]]
        h_matches = _re.findall(r"<h[1-6][^>]*>(.*?)</h[1-6]>", pre_html, _re.IGNORECASE | _re.DOTALL)
        if h_matches:
            heading = _strip_html_tags(h_matches[-1])

        header_row = rows[0] if rows else []
        source_col = str(header_row[value_col]).strip() if value_col < len(header_row) else ""
        unit_scale = (
            "thousands" if unit_mult == 1_000.0
            else "millions" if unit_mult == 1_000_000.0
            else "actual" if unit_mult == 1.0
            else "actual"
        )
        evidence = {
            "sectionHeading": heading or None,
            "tableTitle": None,
            "sourceTableId": 1,
            "sourceRows": [
                [
                    str(rows[region_row_idx][0] if rows[region_row_idx] else region),
                    str(rows[region_row_idx][value_col]) if value_col < len(rows[region_row_idx]) else "",
                ],
                [
                    str(rows[total_row_idx][0] if rows[total_row_idx] else "Total revenue"),
                    str(rows[total_row_idx][value_col]) if value_col < len(rows[total_row_idx]) else "",
                ],
            ],
            "sourceColumns": [source_col] if source_col else [],
            "unitScale": unit_scale,
            "rawValue": str(rows[region_row_idx][value_col]) if value_col < len(rows[region_row_idx]) else None,
            "rawDenominator": str(rows[total_row_idx][value_col]) if value_col < len(rows[total_row_idx]) else None,
        }
        return ratio, region_usd, total_usd, heading, evidence

    return None, None, None, "", None


# ---------------------------------------------------------------------------
# Region → XBRL segment-member mapping for geographic revenue extraction.
# Keys are lowercase region names; values are ordered candidate member strings.
# Substring fallback (region_lower in member.lower()) handles custom prefixes
# such as "aapl:GreaterChinaMember".
# ---------------------------------------------------------------------------
_REGION_XBRL_MEMBERS: dict[str, list[str]] = {
    "china": ["country:CN", "srt:ChinaMember"],
    "united states": ["country:US", "srt:UnitedStatesMember"],
    "europe": ["srt:EuropeMember", "srt:EuropeMiddleEastAndAfricaMember"],
    "japan": ["country:JP", "srt:JapanMember"],
    "asia pacific": ["srt:AsiaPacificMember", "srt:AsiaMember"],
    "rest of world": ["srt:NonUsMember", "srt:OtherGeographicAreasMember"],
}

# Revenue concept names to probe, in priority order.
_GEO_REVENUE_CONCEPTS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomer",
]

_GEO_AXIS = "srt:StatementGeographicalAxis"


def _extract_geographic_pct(
    facts_data: dict,
    region: str,
    filing_date: str | None,
) -> tuple[float | None, float | None, str, str, str]:
    """Extract geographic revenue from EDGAR XBRL company-facts JSON.

    Returns (regionRevenuePct, regionRevenueUSD, segmentLabel, source, confidence).
    All-None/NOT_DISCLOSED on failure.
    """
    region_lower = region.lower()
    candidate_members: list[str] = _REGION_XBRL_MEMBERS.get(region_lower, [])

    def _member_matches(member_val: str) -> bool:
        if any(m.lower() == member_val.lower() for m in candidate_members):
            return True
        # Substring fallback — catches custom prefixes like "aapl:GreaterChinaMember"
        return region_lower in member_val.lower()

    us_gaap: dict = facts_data.get("facts", {}).get("us-gaap", {})

    for concept in _GEO_REVENUE_CONCEPTS:
        concept_data = us_gaap.get(concept)
        if not concept_data:
            continue

        usd_units: list[dict] = concept_data.get("units", {}).get("USD", [])
        if not usd_units:
            continue

        # Pin facts to the specific 10-K by filing date (±10 days) or, when
        # filing_date is unknown, accept any 10-K annual fact.
        def _is_target_filing(fact: dict) -> bool:
            if fact.get("form") not in ("10-K", "10-K405", "10-KSB"):
                return False
            if filing_date is None:
                return True
            try:
                fd = datetime.date.fromisoformat(filing_date)
                ff = datetime.date.fromisoformat(fact["filed"])
                return abs((ff - fd).days) <= 10
            except Exception:
                return True

        target_facts = [f for f in usd_units if _is_target_filing(f)]
        if not target_facts:
            # Relax: accept any 10-K fact for this concept if none match the date
            target_facts = [
                f for f in usd_units
                if f.get("form") in ("10-K", "10-K405", "10-KSB")
            ]
        if not target_facts:
            continue

        # Group facts by period end-date to align regional vs. total rows
        by_period: dict[str, list[dict]] = {}
        for fact in target_facts:
            end = fact.get("end", "")
            by_period.setdefault(end, []).append(fact)

        # Try each period (most-recent first)
        for period_end in sorted(by_period.keys(), reverse=True):
            period_facts = by_period[period_end]

            regional_fact: dict | None = None
            total_fact: dict | None = None

            for fact in period_facts:
                seg = fact.get("segment")
                if seg is None:
                    # No segment dimension → consolidated total
                    total_fact = fact
                elif (
                    isinstance(seg, dict)
                    and seg.get("dimension") == _GEO_AXIS
                    and _member_matches(str(seg.get("member", "")))
                ):
                    regional_fact = fact
                elif isinstance(seg, list):
                    # Some filers encode segment as a list of {dimension, member} objects
                    for dim_entry in seg:
                        if (
                            isinstance(dim_entry, dict)
                            and dim_entry.get("dimension") == _GEO_AXIS
                            and _member_matches(str(dim_entry.get("member", "")))
                        ):
                            regional_fact = fact
                            break

            if regional_fact is not None and total_fact is not None:
                r_val = float(regional_fact["val"])
                t_val = float(total_fact["val"])
                if t_val > 0:
                    pct = round(r_val / t_val, 4)
                    seg_member = (
                        regional_fact["segment"]["member"]
                        if isinstance(regional_fact.get("segment"), dict)
                        else region
                    )
                    return pct, r_val, seg_member, "edgar_xbrl", "CONFIRMED"

    return None, None, region, "not_available", "NOT_DISCLOSED"


# ---------------------------------------------------------------------------
# XBRL annual fact extractor (used by get_sec_filing_intelligence)
# ---------------------------------------------------------------------------

def _extract_xbrl_latest_annual(facts_data: dict, concept_names: list[str]) -> dict | None:
    """Extract the most recent annual (10-K/20-F) value for a set of XBRL concept names.

    Tries each concept name in order, returning the first match found.
    Returns a dict with keys: value, unit, period, form, filed, confidence.
    Returns None if no matching XBRL concept has annual data.
    """
    us_gaap: dict = facts_data.get("facts", {}).get("us-gaap", {})
    for concept in concept_names:
        concept_data = us_gaap.get(concept)
        if not concept_data:
            continue
        usd_units: list[dict] = concept_data.get("units", {}).get("USD", [])
        if not usd_units:
            continue
        # Find most recent 10-K fact
        annual_facts = [
            f for f in usd_units
            if f.get("form") in ("10-K", "10-K405", "10-KSB", "20-F")
            and f.get("end")
            and f.get("val") is not None
        ]
        if not annual_facts:
            continue
        latest = max(annual_facts, key=lambda f: f.get("end", ""))
        return {
            "value": latest.get("val"),
            "unit": "USD",
            "period": latest.get("end"),
            "form": latest.get("form"),
            "filed": latest.get("filed"),
            "confidence": "HIGH",
        }
    return None
