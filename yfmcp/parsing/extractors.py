"""Geographic revenue and XBRL extraction helpers.

Extracted from server.py in Phase 1 of the refactoring plan.
"""

import math
import re as _re

from yfmcp.parsing.html import _detect_unit_scale, _lazy_document_scale


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
# A port of the Worker's extractGeoRevenueFromHtml and the helpers it calls (stripHtmlTags, parseFinancialTableRows,
# parseNumericCell, alignedGeoAmounts, geoColumnHeader), so both runtimes read a filing's tables the same way (2.5.18).
# The shared helpers of yfmcp.parsing.html (_strip_html_tags, _parse_numeric_cell) differ from the Worker's in small
# ways (entities, footnote suffixes), so the geographic reader has its own.

_HTML_ENTITY_MAP = {
    "&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"', "&apos;": "'",
    "&bull;": "•", "&middot;": "·", "&rsquo;": "’", "&lsquo;": "‘", "&ldquo;": "“", "&rdquo;": "”",
    "&ndash;": "–", "&mdash;": "—", "&hellip;": "…",
}
_ENTITY_RE = _re.compile(r"&(?:nbsp|amp|lt|gt|quot|apos|#\d+|#x[0-9a-f]+|[a-z]+);", _re.IGNORECASE)
_INLINE_TAG_RE = _re.compile(r"</?(?:span|font|b|i|u|em|strong|a|sup|sub|small|ix:[a-z]+)\b[^>]*>", _re.IGNORECASE)


def _decode_entity(match: "_re.Match[str]") -> str:
    entity = match.group(0)
    if entity in _HTML_ENTITY_MAP:
        return _HTML_ENTITY_MAP[entity]
    if entity[:3].lower() == "&#x":
        code = int(entity[3:-1], 16)
        return chr(code) if 0 < code <= 0x10FFFF else " "
    if entity.startswith("&#"):
        return chr(int(entity[2:-1], 10) % 65536)  # String.fromCharCode
    return " "


def _geo_strip_html_tags(html: str) -> str:
    """The Worker's stripHtmlTags."""
    text = _re.sub(r"<!--[\s\S]*?-->", " ", html)
    text = _re.sub(r"<script\b[^>]*>[\s\S]*?</script[^>]*>", " ", text, flags=_re.IGNORECASE)
    text = _re.sub(r"<style\b[^>]*>[\s\S]*?</style[^>]*>", " ", text, flags=_re.IGNORECASE)
    text = _re.sub(r"""\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)""", " ", text, flags=_re.IGNORECASE)
    text = _INLINE_TAG_RE.sub("", text)
    text = _re.sub(r"<[^>]+>", " ", text)
    text = _ENTITY_RE.sub(_decode_entity, text)
    return _re.sub(r"\s+", " ", _re.sub(r"<[^>]+>", " ", text)).strip()


_JS_FLOAT_RE = _re.compile(r"[+-]?(?:Infinity|\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?)")


def _geo_parse_numeric_cell(text: str) -> float | None:
    """The Worker's parseNumericCell: commas, parentheses, currency signs, %, b/m/k suffixes and JavaScript's lenient parseFloat."""
    s = _re.sub(r"[$€£¥]", "", _re.sub(r"\s", "", text.replace(",", ""))).replace("%", "")
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    mult = 1.0
    if s[-1:] in ("b", "B"):
        mult, s = 1e9, s[:-1]
    elif s[-1:] in ("m", "M"):
        mult, s = 1e6, s[:-1]
    elif s[-1:] in ("k", "K"):
        mult, s = 1e3, s[:-1]
    m = _JS_FLOAT_RE.match(s)
    if m is None:
        return None
    value = float(m.group(0).replace("Infinity", "inf"))
    return value * mult


def _merge_financial_cells(cells: list[str]) -> list[str]:
    """The Worker's mergeFinancialCells: "$", "%" and ")" cells join the adjacent value; empty cells go."""
    out: list[str] = []
    prefix = ""
    for index, raw in enumerate(cells):
        cell = _re.sub(r"\s+", " ", raw).strip()
        if not cell:
            if index == 0:
                out.append("")
            continue
        if _re.fullmatch(r"[$€£¥]", cell):
            prefix += cell
            continue
        if _re.fullmatch(r"%|\)|\)%|%\)", cell) and out:
            out[-1] += cell
            continue
        cell = _re.sub(r"\s+\)$", ")", _re.sub(r"^\(\s+", "(", cell))
        out.append(prefix + cell)
        prefix = ""
    if prefix:
        out.append(prefix)
    return out


def _financial_table_rows(table_html: str) -> list[list[str]]:
    """The Worker's parseFinancialTableRows."""
    rows: list[list[str]] = []
    for tr in _re.finditer(r"<tr[^>]*>([\s\S]*?)</tr>", table_html, _re.IGNORECASE):
        cells = [_geo_strip_html_tags(td.group(1)) for td in _re.finditer(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", tr.group(1), _re.IGNORECASE)]
        if cells:
            merged = _merge_financial_cells(cells)
            if any(c != "" for c in merged):
                rows.append(merged)
    return rows


# Amount cells of a table row: numbers and dash placeholders, never percent cells (MRVL, 2.5.9).
_DASH_CELL_RE = _re.compile(r"[\s$]*[-–—]+\s*")
# A computed share more than this many points from the table's own percentage is a misread column.
_STATED_PCT_TOLERANCE = 1.0
# A percent beside a value is a share only under a "% of total" header; a "Change" column is not.
_SHARE_HEADER_RE = _re.compile(r"%\s*of\b|\bpercent(?:age)?\s+of\b", _re.IGNORECASE)
_TOTAL_LABELS = frozenset({
    "total", "consolidated", "total revenues", "total net revenues", "net revenues", "revenues", "total revenue",
    "total net sales", "net sales", "total net revenue",
})


def _is_pct_cell(row: list[str], col: int) -> bool:
    """A percent cell, or a number whose "%" sign sits in the next cell."""
    return "%" in row[col] or (col + 1 < len(row) and row[col + 1].strip() == "%")


def _amount_cells(row: list[str]) -> list[tuple[int, float]]:
    out: list[tuple[int, float]] = []
    for col, cell in enumerate(row):
        if _is_pct_cell(row, col):
            continue
        if _DASH_CELL_RE.fullmatch(cell):
            out.append((col, 0.0))
            continue
        v = _geo_parse_numeric_cell(cell)
        if v is not None:
            out.append((col, v))
    return out


def _aligned_geo_amounts(region_row: list[str], total_row: list[str]) -> tuple[float, float, int, int, float | None] | None:
    """The Worker's alignedGeoAmounts: (regionVal, totalVal, valueCol, totalCol, statedPct), paired by position among amount cells."""
    region = _amount_cells(region_row)
    k = next((i for i, (_, v) in enumerate(region) if v > 0), None)
    if k is None:
        return None
    totals = _amount_cells(total_row)
    if k >= len(totals):
        return None
    next_col = region[k][0] + 1
    stated = (
        _geo_parse_numeric_cell(region_row[next_col])
        if next_col < len(region_row) and _is_pct_cell(region_row, next_col) and region_row[next_col].strip() != "%" else None
    )
    return region[k][1], totals[k][1], region[k][0], totals[k][0], stated


def _geo_column_header(rows: list[list[str]], region_row_idx: int, k: int) -> str:
    """The Worker's geoColumnHeader: the header of the k-th amount column, prefixed by a period group when a row above spans them evenly."""
    n = len(_amount_cells(rows[region_row_idx]))
    label = ""
    group = ""
    for i in range(region_row_idx):
        cells = [c.strip() for c in rows[i] if c.strip() and not _SHARE_HEADER_RE.search(c.strip())]
        if not cells or any(_geo_parse_numeric_cell(c) is not None and not _re.search(r"\d{4}", c) for c in cells):
            continue
        if len(cells) == n and not label:
            label = cells[k]
        elif 0 < len(cells) < n and n % len(cells) == 0 and not group and not label:
            group = cells[k // (n // len(cells))]
    return " ".join(x for x in (group, label) if x)


def _geo_region_aliases(region: str) -> list[str]:
    """The Worker's geoRegionAliases: the spellings of a region a filing may use."""
    lower = str(region).lower().strip()
    compact = _re.sub(r"\s+", "", lower)
    aliases = [lower]
    if lower in ("china", "prc", "mainland china"):
        aliases += ["china", "mainland china", "people's republic of china", "peoples republic of china", "prc",
                    "country:cn", "srt:chinamember", "chinamember", "mainlandchinamember"]
    elif lower == "greater china" or compact == "greaterchina":
        aliases += ["greater china", "greaterchinamember"]
    elif lower == "hong kong" or compact == "hongkong":
        aliases += ["hong kong", "hongkong", "country:hk", "srt:hongkongmember", "hongkongmember"]
    elif lower == "taiwan":
        aliases += ["taiwan", "country:tw", "srt:taiwanmember", "taiwanmember"]
    return [a for a in dict.fromkeys(aliases) if a]


def _text_contains_geo_region(text: str, region: str) -> bool:
    lower = text.lower()
    for term in _geo_region_aliases(region):
        if _re.fullmatch(r"[a-z0-9 ]+", term):
            pattern = r"\b" + r"\s+".join(_re.escape(part) for part in _re.split(r"\s+", term)) + r"\b"
            if _re.search(pattern, lower, _re.IGNORECASE | _re.ASCII):
                return True
        elif term in lower:
            return True
    return False


def _js_round(value: float) -> int:
    """JavaScript's Math.round: halves round up."""
    return math.floor(value + 0.5)


def extract_geo_revenue_from_html(html: str, region: str) -> dict | None:
    """The Worker's extractGeoRevenueFromHtml: a geographic revenue table that names the region, or None.

    Every table that names the region and is about revenue is a candidate; tables introduced as a geographic breakdown
    come first, and a table without revenue or sales in it or its lead-in (a properties list) never counts.
    """
    document_scale = _lazy_document_scale(html)
    candidates: list[tuple[int, int, str, list[list[str]]]] = []
    for scanned, m in enumerate(_re.finditer(r"<table[^>]*>[\s\S]*?</table>", html, _re.IGNORECASE), start=1):
        if scanned > 800:
            break
        table_html = m.group(0)
        if not _text_contains_geo_region(table_html, region):
            continue
        pos = m.start()
        # The lead-in stops at the previous table.
        before = html[max(0, pos - 1_500): pos]
        lead = _geo_strip_html_tags(before[before.lower().rfind("</table>") + 1:])[-800:]
        table_text = _geo_strip_html_tags(table_html)[:600]
        context = f"{lead} {table_text}".lower()
        if not _re.search(r"revenue|net sales|\bsales\b", context, _re.ASCII) or _re.search(r"square f(?:oo|ee)t", table_text, _re.IGNORECASE):
            continue
        # An asset table after a revenue paragraph is not revenue: COHR's long-lived assets by country (2.5.20).
        if _re.search(r"long-lived assets|property,? plant,? and equipment|\btotal assets\b|identifiable assets", table_text, _re.IGNORECASE) and not _re.search(r"revenue|net sales|\bsales\b", table_text, _re.IGNORECASE):
            continue
        rows = _financial_table_rows(table_html)
        if len(rows) < 2:
            continue
        score = 2 if _re.search(r"geograph|by region|by country|by location|region of|country of|location of (?:the )?customer", context) else 1
        candidates.append((-score, pos, table_html, rows))
    candidates.sort(key=lambda c: (c[0], c[1]))

    for neg_score, pos, table_html, rows in candidates:
        region_row = next((i for i, row in enumerate(rows) if any(_text_contains_geo_region(cell, region) for cell in row)), None)
        if region_row is None:
            continue
        # A total row carries amounts: COHR's "Revenues" header row is a caption, not the total (2.5.20).
        total_row = next((i for i, row in enumerate(rows)
                          if any(c.strip().lower() in _TOTAL_LABELS for c in row) and any(_geo_parse_numeric_cell(c) is not None for c in row)), None)
        if total_row is None:
            total_row = next((i for i in range(len(rows) - 1, -1, -1) if any(_geo_parse_numeric_cell(c) is not None for c in rows[i])), None)
        if total_row is None or total_row == region_row:
            continue
        aligned = _aligned_geo_amounts(rows[region_row], rows[total_row])
        if aligned is None:
            continue
        region_val, total_val, value_col, total_col, stated_pct = aligned
        # A region cannot exceed the total it is part of.
        if total_val <= 0 or region_val <= 0 or region_val > total_val * 1.001:
            continue
        pct = _js_round(region_val / total_val * 10000) / 10000
        # The table's own share for the region must agree with the computed one, under a "% of total" header only.
        share_header = any(_SHARE_HEADER_RE.search(c) for r in rows[:region_row] for c in r)
        stated_share = stated_pct if share_header else None
        if stated_share is not None and abs(pct * 100 - stated_share) > _STATED_PCT_TOLERANCE:
            continue
        unit_mult, unit_scale_source = _detect_unit_scale(table_html, html[max(0, pos - 3_000): pos], document_scale)
        unit_scale = "thousands" if unit_mult == 1e3 else "millions" if unit_mult == 1e6 else "billions" if unit_mult == 1e9 else "actual"
        headings = _re.findall(r"<h[1-6][^>]*>([\s\S]*?)</h[1-6]>", html[max(0, pos - 6_000): pos], _re.IGNORECASE)
        section_heading = _geo_strip_html_tags(headings[-1]) if headings else ""
        header_row = rows[0]
        # A header row without a label cell is one cell shorter than the data rows.
        header_col = value_col - max(0, len(rows[region_row]) - len(header_row))
        ordinal = next((k for k, (col, _) in enumerate(_amount_cells(rows[region_row])) if col == value_col), -1)
        source_column = _geo_column_header(rows, region_row, ordinal) or (
            str(header_row[header_col]).strip() if 0 <= header_col < len(header_row) else "")
        raw_value = str(rows[region_row][value_col]) if value_col < len(rows[region_row]) else None
        raw_denominator = str(rows[total_row][total_col])
        total_label = str(rows[total_row][0]) if rows[total_row] else ""
        return {
            "pct": pct,
            "usd": region_val * unit_mult,
            "denominator": total_val * unit_mult,
            "sectionHeading": section_heading,
            "unitScale": unit_scale,
            "unitScaleSource": unit_scale_source,
            "rawValue": raw_value,
            "rawDenominator": raw_denominator,
            "sourceRows": [
                [str(rows[region_row][0]) if rows[region_row] else region, raw_value if raw_value is not None else ""],
                [total_label if _re.search(r"[A-Za-z]", total_label) else "Total (unlabeled row)", raw_denominator],
            ],
            "sourceColumns": [source_column] if source_column else [],
            "statedPct": stated_share,
        }
    return None


def _extract_geo_revenue_from_html(
    html_text: str,
    region: str,
) -> tuple[float | None, float | None, float | None, str, dict | None]:
    """Search an SEC filing HTML document for a geographic revenue table.

    Returns (regionRevenueRatio, regionRevenueUSD, totalRevenueUSD, sectionHeading, evidence), as the Worker's
    extractGeoRevenueFromHtml reads it (extract_geo_revenue_from_html); all None (heading "") when there is no table.
    """
    geo = extract_geo_revenue_from_html(html_text, region)
    if geo is None:
        return None, None, None, "", None
    evidence = {
        "sectionHeading": geo["sectionHeading"] or None,
        "tableTitle": None,
        "sourceTableId": 1,
        "sourceRows": geo["sourceRows"],
        "sourceColumns": geo["sourceColumns"],
        "unitScale": geo["unitScale"],
        "unitScaleSource": geo["unitScaleSource"],
        "rawValue": geo["rawValue"],
        "rawDenominator": geo["rawDenominator"],
    }
    return geo["pct"], geo["usd"], geo["denominator"], geo["sectionHeading"], evidence


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
