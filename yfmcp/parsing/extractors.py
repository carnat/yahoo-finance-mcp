"""Geographic revenue and XBRL extraction helpers.

Extracted from server.py in Phase 1 of the refactoring plan.
"""

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

# Amount cells of a table row: numbers and dash placeholders, never percent
# cells. A total row often carries no label and no "% of total" cells, so a
# cell index taken from a region row can land on another period's total
# (MRVL: China $1,161.5 against the prior year's $2,006.1 read 57.9%; the
# table states 42%) (2.5.9). Mirrors alignedGeoAmounts in the Worker.
_DASH_CELL_RE = _re.compile(r"^[\s$]*[-\u2013\u2014]+\s*$")
# A computed share more than this many points from the table's own percentage is a misread column.
_STATED_PCT_TOLERANCE = 1.0
# A percent beside a value is a share only under a "% of total" header; a "Change" column is not.
_SHARE_HEADER_RE = _re.compile(r"%\s*of\b|\bpercent(?:age)?\s+of\b", _re.IGNORECASE)


def _has_share_header(rows: list[list[str]], before: int) -> bool:
    return any(_SHARE_HEADER_RE.search(c) for r in rows[:before] for c in r)


def _is_pct_cell(row: list[str], col: int) -> bool:
    """A percent cell, or a number whose "%" sign sits in the next cell."""
    return "%" in row[col] or (col + 1 < len(row) and row[col + 1].strip() == "%")


def _amount_cells(row: list[str]) -> list[tuple[int, float]]:
    out: list[tuple[int, float]] = []
    for col, cell in enumerate(row):
        if _is_pct_cell(row, col):
            continue
        if _DASH_CELL_RE.match(cell):
            out.append((col, 0.0))
            continue
        v = _parse_numeric_cell(cell)
        if v is not None:
            out.append((col, v))
    return out


def _first_positive_ordinal(row: list[str]) -> int | None:
    return next((k for k, (_, v) in enumerate(_amount_cells(row)) if v > 0), None)


def _amount_at(row: list[str], k: int) -> tuple[int, float] | None:
    cells = _amount_cells(row)
    return cells[k] if k < len(cells) else None


def _stated_pct_after(row: list[str], col: int) -> float | None:
    nxt = col + 1
    return _parse_numeric_cell(row[nxt]) if nxt < len(row) and _is_pct_cell(row, nxt) and row[nxt].strip() != "%" else None


def _geo_column_header(rows: list[list[str]], region_row_idx: int, k: int) -> str:
    """The header of the k-th amount column: the header row with one label per amount column
    ("% of Total" headers dropped), prefixed by a period group ("Three Months Ended") when a row
    above spans the columns evenly (2.5.9). Mirrors geoColumnHeader in the Worker."""
    n = len(_amount_cells(rows[region_row_idx]))
    label = ""
    group = ""
    for i in range(region_row_idx):
        cells = [c.strip() for c in rows[i] if c.strip() and not _SHARE_HEADER_RE.search(c)]
        if not cells or any(_parse_numeric_cell(c) is not None and not _re.search(r"\d{4}", c) for c in cells):
            continue
        if len(cells) == n and not label:
            label = cells[k]
        elif 0 < len(cells) < n and n % len(cells) == 0 and not group and not label:
            group = cells[k // (n // len(cells))]
    return " ".join(x for x in (group, label) if x)


def _row_label(row: list[str], fallback: str) -> str:
    """A row's label; an unlabeled total row's first cell is a number."""
    if not row:
        return fallback
    return str(row[0]) if _re.search(r"[A-Za-z]", str(row[0])) else "Total (unlabeled row)"


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
        starts = [search_start + tbl_m.start() for tbl_m in _re.finditer(r"<table[^>]*>", chunk, _re.IGNORECASE)]
        # The table the match sits in, however far back its tag starts: inline
        # styles put MRVL's region table tag ~5,000 characters before "China" (2.5.9).
        enclosing = html_lower.rfind("<table", 0, pos)
        if enclosing != -1 and enclosing < search_start and html_lower.find("</table>", enclosing) > pos:
            starts.insert(0, enclosing)

        for abs_start in starts:
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

    def _local_format_raw_number(n: float | int | None) -> str | None:
        if n is None:
            return None
        try:
            f = float(n)
            if abs(f - round(f)) < 1e-9:
                return f"{int(round(f)):,}"
            return f"{f:,.2f}"
        except Exception:
            return None

    is_china_query = region_lower in ("china", "greater china")

    for tbl in candidate_tables:
        rows: list[list[str]] = tbl["rows"]

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

        if total_row_idx is None:
            continue

        if is_china_query:
            # China query: extract and sum Mainland China / Hong Kong rows
            mainland_idx = None
            hongkong_idx = None
            greater_china_idx = None
            generic_china_idx = None

            for i, row in enumerate(rows):
                if not row or i == total_row_idx:
                    continue
                label = str(row[0]).lower()
                if any(t in label for t in ["total", "consolidated"]) and not "china" in label:
                    continue
                if "hong kong" in label or "hongkong" in label:
                    hongkong_idx = i
                elif "mainland" in label or "excluding hong kong" in label or "exclude hong kong" in label:
                    mainland_idx = i
                elif "greater china" in label:
                    greater_china_idx = i
                elif "china" in label:
                    generic_china_idx = i

            # One column for every row, paired by position among amount cells:
            # the first positive amount of the main China row.
            lead_idx = next((i for i in (generic_china_idx, greater_china_idx, mainland_idx, hongkong_idx) if i is not None), None)
            ordinal = _first_positive_ordinal(rows[lead_idx]) if lead_idx is not None else None
            if ordinal is None:
                continue
            total_cell = _amount_at(rows[total_row_idx], ordinal)
            if total_cell is None:
                continue
            value_col, total_val = total_cell

            def _val(idx: int | None) -> float | None:
                cell = _amount_at(rows[idx], ordinal) if idx is not None else None
                return cell[1] if cell else None

            def _raw(idx: int) -> str:
                cell = _amount_at(rows[idx], ordinal)
                return str(rows[idx][cell[0]]) if cell else ""

            mainland_val = _val(mainland_idx)
            hongkong_val = _val(hongkong_idx)
            greater_china_val = _val(greater_china_idx)
            generic_china_val = _val(generic_china_idx)

            region_val = None
            interpretation_warning = None
            if mainland_val is not None and hongkong_val is not None:
                region_val = mainland_val + hongkong_val
                interpretation_warning = "Combined Mainland China and Hong Kong revenue to represent total China exposure."
            elif mainland_val is not None:
                region_val = mainland_val
                interpretation_warning = "Mainland China revenue only; Hong Kong revenue was not separately identified."
            elif greater_china_val is not None:
                region_val = greater_china_val
            elif generic_china_val is not None:
                region_val = generic_china_val
            elif hongkong_val is not None:
                region_val = hongkong_val
                interpretation_warning = "Hong Kong revenue only; Mainland China revenue was not separately identified."

            if region_val is None or total_val is None or total_val <= 0:
                continue

            ratio = round(region_val / total_val, 4)
            # A single row's own "% of total" must agree with the computed share.
            single_idx = None if (mainland_idx is not None and hongkong_idx is not None) else lead_idx
            if single_idx is not None and _has_share_header(rows, single_idx):
                cell = _amount_at(rows[single_idx], ordinal)
                stated = _stated_pct_after(rows[single_idx], cell[0]) if cell else None
                if stated is not None and abs(ratio * 100 - stated) > _STATED_PCT_TOLERANCE:
                    continue

            # Detect unit scale for USD conversion
            context_html = html_text[max(0, tbl["pos"] - 3_000): tbl["pos"]]
            unit_mult = _detect_unit_multiplier(tbl["table_html"], context_html)
            region_usd = region_val * unit_mult
            total_usd = total_val * unit_mult

            # Extract nearest section heading
            heading = ""
            pre_html = html_text[max(0, tbl["pos"] - 6_000): tbl["pos"]]
            h_matches = _re.findall(r"<h[1-6][^>]*>(.*?)</h[1-6]>", pre_html, _re.IGNORECASE | _re.DOTALL)
            if h_matches:
                heading = _strip_html_tags(h_matches[-1])

            header_row = rows[0] if rows else []
            source_col = _geo_column_header(rows, lead_idx, ordinal) or (str(header_row[value_col]).strip() if value_col < len(header_row) else "")
            unit_scale = (
                "thousands" if unit_mult == 1_000.0
                else "millions" if unit_mult == 1_000_000.0
                else "actual" if unit_mult == 1.0
                else "actual"
            )

            source_rows = []
            if mainland_idx is not None:
                source_rows.append([str(rows[mainland_idx][0]), _raw(mainland_idx)])
            if hongkong_idx is not None:
                source_rows.append([str(rows[hongkong_idx][0]), _raw(hongkong_idx)])
            if greater_china_idx is not None:
                source_rows.append([str(rows[greater_china_idx][0]), _raw(greater_china_idx)])
            if generic_china_idx is not None and mainland_idx is None and greater_china_idx is None:
                source_rows.append([str(rows[generic_china_idx][0]), _raw(generic_china_idx)])
            source_rows.append([_row_label(rows[total_row_idx], "Total revenue"), str(rows[total_row_idx][value_col])])

            breakdown = {}
            if total_val > 0:
                if mainland_val is not None:
                    breakdown["mainlandPct"] = round(mainland_val / total_val * 100, 2)
                    breakdown["mainlandUSD"] = mainland_val * unit_mult
                if hongkong_val is not None:
                    breakdown["hongKongPct"] = round(hongkong_val / total_val * 100, 2)
                    breakdown["hongKongUSD"] = hongkong_val * unit_mult
                if mainland_val is not None and hongkong_val is not None:
                    breakdown["combinedPct"] = round((mainland_val + hongkong_val) / total_val * 100, 2)
                    breakdown["combinedUSD"] = (mainland_val + hongkong_val) * unit_mult

            evidence = {
                "sectionHeading": heading or None,
                "tableTitle": None,
                "sourceTableId": 1,
                "sourceRows": source_rows,
                "sourceColumns": [source_col] if source_col else [],
                "unitScale": unit_scale,
                "rawValue": _local_format_raw_number(region_val),
                "rawDenominator": _local_format_raw_number(total_val),
                "breakdown": breakdown,
                "allChinaInterpretationWarning": interpretation_warning,
            }
            return ratio, region_usd, total_usd, heading, evidence

        else:
            # Standard single-row matching
            region_row_idx: int | None = None
            for i, row in enumerate(rows):
                if any(region_lower in cell.lower() for cell in row):
                    region_row_idx = i
                    break
            if region_row_idx is None or total_row_idx == region_row_idx:
                continue

            # The value and the total in the same column, paired among amount cells.
            ordinal = _first_positive_ordinal(rows[region_row_idx])
            if ordinal is None:
                continue
            region_cell = _amount_at(rows[region_row_idx], ordinal)
            total_cell = _amount_at(rows[total_row_idx], ordinal)
            if region_cell is None or total_cell is None:
                continue
            (value_col, region_val), (total_col, total_val) = region_cell, total_cell
            if total_val <= 0:
                continue

            ratio = round(region_val / total_val, 4)
            stated = _stated_pct_after(rows[region_row_idx], value_col) if _has_share_header(rows, region_row_idx) else None
            if stated is not None and abs(ratio * 100 - stated) > _STATED_PCT_TOLERANCE:
                continue

            # Detect unit scale for USD conversion
            context_html = html_text[max(0, tbl["pos"] - 3_000): tbl["pos"]]
            unit_mult = _detect_unit_multiplier(tbl["table_html"], context_html)
            region_usd = region_val * unit_mult
            total_usd = total_val * unit_mult

            # Extract nearest section heading
            heading = ""
            pre_html = html_text[max(0, tbl["pos"] - 6_000): tbl["pos"]]
            h_matches = _re.findall(r"<h[1-6][^>]*>(.*?)</h[1-6]>", pre_html, _re.IGNORECASE | _re.DOTALL)
            if h_matches:
                heading = _strip_html_tags(h_matches[-1])

            header_row = rows[0] if rows else []
            source_col = _geo_column_header(rows, region_row_idx, ordinal) or (str(header_row[value_col]).strip() if value_col < len(header_row) else "")
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
                        _row_label(rows[total_row_idx], "Total revenue"),
                        str(rows[total_row_idx][total_col]),
                    ],
                ],
                "sourceColumns": [source_col] if source_col else [],
                "unitScale": unit_scale,
                "rawValue": str(rows[region_row_idx][value_col]) if value_col < len(rows[region_row_idx]) else None,
                "rawDenominator": str(rows[total_row_idx][total_col]),
            }
            return ratio, region_usd, total_usd, heading, evidence

    return None, None, None, "", None


# ---------------------------------------------------------------------------
# XBRL annual fact extractor (used by get_sec_filing_intelligence).
# ---------------------------------------------------------------------------
def _extract_xbrl_latest_annual(
    facts_data: dict,
    concept_names: list[str],
    accession_number: str | None = None,
    document_url: str | None = None,
) -> dict | None:
    """Extract the most recent annual (10-K/20-F) value for a set of XBRL concept names.

    Tries each concept name in order, returning the first match found.
    Returns a dict with keys: value, unit, period, form, filed, confidence.
    Returns None if no matching XBRL concept has annual data.
    """
    us_gaap: dict = facts_data.get("facts", {}).get("us-gaap", {})
    if accession_number:
        # The selected filing's own value, any form (yfmcp/sec_facts.py, 2.4.5):
        # a 10-Q gives its quarter, not nothing.
        from yfmcp.sec_facts import filing_fact_in_accession
        candidates = [{"concept": c, "facts": ((us_gaap.get(c) or {}).get("units") or {}).get("USD") or []} for c in concept_names]
        hit = filing_fact_in_accession(candidates, accession_number)
        if hit is None:
            return None
        latest = hit["fact"]
        source_evidence = {
            "sourceType": "sec_xbrl_companyfacts",
            "concept": hit["concept"],
            "taxonomy": "us-gaap",
            "unit": "USD",
            "accessionNumber": latest.get("accn"),
            "filingType": latest.get("form"),
            "filingDate": latest.get("filed"),
            "periodStart": latest.get("start"),
            "periodEnd": latest.get("end"),
            "documentUrl": document_url,
        }
        decision_grade = bool(latest.get("accn") == accession_number and latest.get("end") and document_url)
        return {
            "value": latest.get("val"),
            "unit": "USD",
            "period": latest.get("end"),
            "periodStart": latest.get("start"),
            "form": latest.get("form"),
            "filed": latest.get("filed"),
            "confidence": "HIGH",
            "decisionGrade": decision_grade,
            "evidence": source_evidence if decision_grade else None,
            "sourceEvidence": source_evidence,
        }
    for concept in concept_names:
        concept_data = us_gaap.get(concept)
        if not concept_data:
            continue
        usd_units: list[dict] = concept_data.get("units", {}).get("USD", [])
        if not usd_units:
            continue
        # Bind the snapshot to the selected filing when an accession is known.
        annual_facts = [
            f for f in usd_units
            if f.get("form") in ("10-K", "10-K405", "10-KSB", "20-F")
            and f.get("end")
            and f.get("val") is not None
            and (accession_number is None or f.get("accn") == accession_number)
        ]
        if not annual_facts:
            continue
        latest = max(annual_facts, key=lambda f: f.get("end", ""))
        source_evidence = {
            "sourceType": "sec_xbrl_companyfacts",
            "concept": concept,
            "taxonomy": "us-gaap",
            "unit": "USD",
            "accessionNumber": latest.get("accn"),
            "filingType": latest.get("form"),
            "filingDate": latest.get("filed"),
            "periodEnd": latest.get("end"),
            "documentUrl": document_url,
        }
        decision_grade = bool(
            accession_number
            and latest.get("accn") == accession_number
            and source_evidence["periodEnd"]
            and document_url
        )
        return {
            "value": latest.get("val"),
            "unit": "USD",
            "period": latest.get("end"),
            "form": latest.get("form"),
            "filed": latest.get("filed"),
            "confidence": "HIGH",
            "decisionGrade": decision_grade,
            "evidence": source_evidence if decision_grade else None,
            "sourceEvidence": source_evidence,
        }
    return None
