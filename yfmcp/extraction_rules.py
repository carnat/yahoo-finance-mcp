"""Text extraction rules shared by both runtimes (2.4.4).

Customer concentration, guidance ranges and reported release metrics read
filing and press-release prose. Each rule proves what a number belongs to
before returning it: a customer percentage must be a share of revenue for a
named or explicitly unnamed customer, a guidance range must follow a guidance
keyword, and a reported metric must follow its own label outside award,
backlog and outlook wording.

Mirrors worker/src/extraction-rules.ts; scripts/test_extraction_rules.py
requires identical output from both.
"""

from __future__ import annotations

import math
import re

_F = re.I | re.A
# One whitespace class for both runtimes (JavaScript's \s); text is collapsed
# with it first, so every later pattern sees plain spaces.
_WS_RUN_RE = re.compile("[\t\n\x0b\x0c\r    -     　﻿]+")


def _collapse_ws(text: str) -> str:
    return _WS_RUN_RE.sub(" ", text).strip(" ")


# ── Customer concentration ──────────────────────────────────────────────────

_CUSTOMER_NEGATION_RE = re.compile(r"\bno (?:single |one )?customer\b|\bnone of (?:the|its|our) customers\b|did not have any (?:single )?customer|no customers? (?:that )?(?:individually )?accounted", _F)
# "<subject> accounted for 53.1%, 34.1% and 11.3% of our revenue": the first
# percentage is the first period listed. Receivables and other bases never match.
# A threshold ("more than 10%", "10% or more") states the disclosure rule, not
# a value, and is skipped.
_CONCENTRATION_RE = re.compile(
    r"\b(?:accounted\s+for|represented|comprised|contributed)\s+(?:approximately\s+|about\s+|nearly\s+|roughly\s+|(more\s+than|over|greater\s+than|at\s+least|in\s+excess\s+of|exceeding)\s+)?"
    r"(\d{1,3}(?:\.\d+)?)\s*%(\s*or\s+(?:more|greater|higher))?(?:(?:\s*,\s*|\s*,?\s*and\s+)\d{1,3}(?:\.\d+)?\s*%)*\s+of\s+"
    r"(?:(?:our|its|the\s+company'?s|the|total|net|consolidated)\s+){0,3}(?:revenues?|net\s+(?:sales|revenues?)|sales)\b", _F)
_YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b", _F)
# "... of our revenue in 2025": a year straight after a match belongs to it.
_TRAILING_YEAR_RE = re.compile(r"\s*(?:in|for|during)\s+(?:fiscal\s+(?:year\s+)?)?((?:19|20)\d{2})\b", _F)
_SUBJECT_BREAK_RE = re.compile(r",\s+|;\s+|\band\s+", _F)
_AGGREGATE_SUBJECT_RE = re.compile(r"\b(?:customers|clients|distributors)\b", _F)
_UNNAMED_SUBJECT_RE = re.compile(r"^(?:(?:our|the|its)\s+)?(?:one|a|single|a single|another|largest)\b.*\b(?:customer|client|distributor)$", _F)
_LEADING_YEAR_RE = re.compile(r"^(?:in|during|for)\s+(?:fiscal\s+)?(?:19|20)\d{2}\s*", _F)
# A lower-case subject naming no customer is a revenue category, not a customer (2.5.32, F-024: AEHR's "EV and power
# semiconductor revenues accounted for 17%").
_CUSTOMER_WORD_RE = re.compile(r"\b(?:customers?|clients?|distributors?|resellers?)\b", _F)
# "three customers accounted for approximately 26%, 14% and 11%": as many shares as customers counted, in a sentence
# naming fewer years, are one share per customer, not a total (2.5.32, F-024: AEHR, ANET).
_COUNT_WORDS = {"two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10}
_COUNTED_SUBJECT_RE = re.compile(
    r"^(?:(?:these|the|our|its)\s+)?(two|three|four|five|six|seven|eight|nine|ten|\d{1,2})\s+(?:of\s+(?:our|its|the)\s+)?(?:\w+\s+)?(?:customers|clients|distributors)$", _F)
_RANKED_SUBJECT_RE = re.compile(r"\b(?:largest|top|biggest|major)\b", _F)
_PCT_RE = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*%", _F)
# A significant-customer table row (2.5.32, F-024: MRVL's "Distributor A | 37% | 34% | 24%" under a title stating
# the 10%-of-net-revenue rule); the first column is the latest period.
_TABLE_CUSTOMER_LABEL_RE = re.compile(r"(?:(?:direct|end)\s+)?(?:customer|distributor|client|reseller)\s+(?:[A-Z]|\d{1,2})", _F)
_TABLE_PCT_CELL_RE = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*%", _F)
_AGGREGATE_DETERMINER_RE = re.compile(r"^(?:our|its|the company['’]s|the)\s+", re.A)
_INCLUSIVE_CLAUSE_RE = re.compile(r",\s*(?:inclusive\s+of|including|excluding)\b[^,]*,?\s*$", _F)
_REVENUE_FROM_RE = re.compile(r"^(?:net\s+)?(?:revenues?|sales)\s+(?:from|to)\s+", _F)
_TRAILING_PUNCT_RE = re.compile(r"[\s,;:]+$", _F)
_LEADING_ARTICLE_RE = re.compile(r"^(?:our|the|its)\s+", _F)
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.;])\s+", _F)


def _concentration_subject(segment: str) -> str:
    # An "inclusive of ..." clause and trailing punctuation are no subject break (MRVL's "net revenue from our ten (10)
    # largest customers, inclusive of our distributor and direct customers, represented 82%"), and "sales to" or
    # "revenue from" is not part of the customer (ANET's "Sales to one end customer") (2.5.32, F-024).
    trimmed = _TRAILING_PUNCT_RE.sub("", _INCLUSIVE_CLAUSE_RE.sub("", segment, count=1), count=1)
    start = 0
    for m in _SUBJECT_BREAK_RE.finditer(trimmed):
        start = m.end()
    subject = _LEADING_YEAR_RE.sub("", trimmed[start:].strip(), count=1)
    subject = _REVENUE_FROM_RE.sub("", subject, count=1)
    return _TRAILING_PUNCT_RE.sub("", subject, count=1)


def _table_row_finding(item: dict, section: str | None) -> dict | None:
    label = _collapse_ws(item["rowLabel"]) if isinstance(item.get("rowLabel"), str) else ""
    title = _collapse_ws(item["tableTitle"]) if isinstance(item.get("tableTitle"), str) else ""
    if not _TABLE_CUSTOMER_LABEL_RE.fullmatch(label) or not re.search(r"\b(?:revenues?|sales)\b", title, _F) or re.search(r"receivable", title, _F):
        return None
    raw = item.get("contextText")
    cells = [c.strip(" ") for c in _collapse_ws(str(raw if raw is not None else "")).split("|")]
    m = _TABLE_PCT_CELL_RE.fullmatch(cells[1] if len(cells) > 1 else "") if cells[0] == label else None
    if not m:
        return None
    pct = float(m.group(1))
    if not (0 < pct <= 100):
        return None
    return {"kind": "customer", "name": None, "description": label, "valuePct": pct, "year": None, "sectionHeading": section, "sentence": f"{title} {' | '.join(cells)}"}


def customer_concentration(matches: list[dict]) -> dict:
    """Revenue-concentration statements in filing text matches.

    Named or unnamed customers, and aggregates such as "our top ten
    customers", each for the first period its sentence lists. Only the latest
    year found is kept.
    """
    findings: list[dict] = []
    negation = None
    for item in matches:
        raw_ctx = item.get("contextText") if item.get("contextText") is not None else item.get("context")
        ctx = _collapse_ws(str(raw_ctx if raw_ctx is not None else ""))
        section = item.get("sectionHeading") if isinstance(item.get("sectionHeading"), str) else None
        if item.get("inTable") is True:
            row = _table_row_finding(item, section)
            if row:
                findings.append(row)
            continue
        for sentence in _SENTENCE_SPLIT_RE.split(ctx):
            if not re.search(r"customer|client|distributor", sentence, _F) and not re.search(r"\b(?:accounted\s+for|represented)\b", sentence, _F):
                continue
            if _CUSTOMER_NEGATION_RE.search(sentence):
                if negation is None:
                    negation = {"sectionHeading": section, "sentence": sentence}
                continue
            prev_end = 0
            last_year = None
            for m in _CONCENTRATION_RE.finditer(sentence):
                segment = sentence[prev_end:m.start()]
                prev_end = m.end()
                trailing = _TRAILING_YEAR_RE.match(sentence, prev_end)
                if trailing:
                    prev_end = trailing.end()
                years = [int(y.group(0)) for y in _YEAR_RE.finditer(segment)]
                year = int(trailing.group(1)) if trailing else (years[0] if years else last_year)
                last_year = year
                if m.group(1) or m.group(3):
                    continue
                pct = float(m.group(2))
                if not (0 < pct <= 100):
                    continue
                subject = _concentration_subject(segment)
                if not subject:
                    continue
                counted = _COUNTED_SUBJECT_RE.match(subject)
                shares = [float(p) for p in _PCT_RE.findall(m.group(0))]
                count = (_COUNT_WORDS.get(counted.group(1).lower()) or int(counted.group(1))) if counted else 0
                if counted and not _RANKED_SUBJECT_RE.search(subject) and len(shares) == count and len(set(_YEAR_RE.findall(sentence))) < count:
                    for share in shares:
                        if 0 < share <= 100:
                            findings.append({"kind": "customer", "name": None, "description": f"one of {subject}", "valuePct": share, "year": year, "sectionHeading": section, "sentence": sentence})
                    continue
                if _AGGREGATE_SUBJECT_RE.search(subject):
                    findings.append({"kind": "aggregate", "name": None, "description": subject, "valuePct": pct, "year": year, "sectionHeading": section, "sentence": sentence})
                elif not re.match(r"[A-Z]", subject) and not _CUSTOMER_WORD_RE.search(subject):
                    continue
                elif _UNNAMED_SUBJECT_RE.search(subject) or not re.match(r"[A-Z]", subject) or len(subject) > 60:
                    findings.append({"kind": "customer", "name": None, "description": subject, "valuePct": pct, "year": year, "sectionHeading": section, "sentence": sentence})
                else:
                    findings.append({"kind": "customer", "name": _LEADING_ARTICLE_RE.sub("", subject, count=1), "description": subject, "valuePct": pct, "year": year, "sectionHeading": section, "sentence": sentence})
    years = [f["year"] for f in findings if f["year"] is not None]
    latest = max(years) if years else None
    # A significant-customer table states the same customers more precisely than the prose ("three customers accounted
    # for approximately 26%, 14% and 11%" against AEHR's Customer A-C at 26.3%, 14.2% and 10.9%): the prose share
    # within half a point of a table row is that row (2.5.32, F-024).
    table_shares = [f["valuePct"] for f in findings if f["kind"] == "customer" and f["name"] is None and _TABLE_CUSTOMER_LABEL_RE.fullmatch(f["description"])]
    seen: set[str] = set()
    kept: list[dict] = []
    for f in findings:
        if latest is not None and f["year"] is not None and f["year"] != latest:
            continue
        if (f["kind"] == "customer" and f["name"] is None and not _TABLE_CUSTOMER_LABEL_RE.fullmatch(f["description"])
                and any(abs(t - f["valuePct"]) <= 0.5 for t in table_shares)):
            continue
        if f["kind"] == "aggregate":
            # "our five largest customers" and "the Company's five largest customers" are one aggregate.
            key = f"a|{_AGGREGATE_DETERMINER_RE.sub('', f['description'].lower(), count=1)}"
        elif f["name"]:
            key = f"n|{f['name'].lower()}"
        elif _TABLE_CUSTOMER_LABEL_RE.fullmatch(f["description"]):
            key = f"l|{f['description'].lower()}"
        else:
            key = f"u|{_js_num(f['valuePct'])}"
        if key in seen:
            continue
        seen.add(key)
        kept.append(f)
    return {"findings": kept[:12], "negation": negation}


def _js_num(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else repr(value)


# ── Guidance ranges ─────────────────────────────────────────────────────────

# "$3.9B" carries its unit; "$1,234 more" carries none (2.5.20).
_AMOUNT = r"([0-9][0-9.,]*(?:\s*(?:billion|million|thousand|bn|mn|b|m|k)\b)?)"
_RANGE_SEP = r"\s*(?:to|and|-|–|—)\s*"
# "full year 2026 revenue guidance of $150.0 million to $200.0 million" (ASTS)
_REVENUE_FIRST_RE = re.compile(rf"\brevenues?\s+(?:guidance|outlook|forecast)\b[^$.]{{0,40}}\$\s*{_AMOUNT}{_RANGE_SEP}\$?\s*{_AMOUNT}", _F)
# "expects revenue between $X and $Y" / "guidance: revenue of $X to $Y" / "expectations for revenue of $X to $Y" (ASTS)
_KEYWORD_FIRST_RE = re.compile(rf"(?:expects|expectations?|guidance|outlook)[^.\n]{{0,120}}revenue[^$]{{0,25}}\$?\s*{_AMOUNT}{_RANGE_SEP}\$?\s*{_AMOUNT}", _F)
_GROSS_MARGIN_RE = re.compile(r"gross margin[^0-9]{0,20}([0-9]{1,2}(?:\.[0-9]+)?)\s*%\s*(?:to|and|-|–|—)\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%", _F)
# "net income per share" is EPS too (2.5.10, LITE: "Non-GAAP diluted net income per share of $4.05 to $4.35").
_EPS_RE = re.compile(r"(?:expects|guidance|outlook)[^.\n]{0,120}(?:eps|earnings per share|net (?:income|earnings|loss) per share)[^$]{0,25}\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|and|-|–|—)\s*\$?\s*([0-9]+(?:\.[0-9]+)?)", _F)


# Metric first, forward verb after it (2.5.9, COHR): "Revenue for the first
# quarter of fiscal 2027 is expected to be between $2.2 billion and $2.4
# billion." No period, dollar sign or percent sign may sit between the metric
# and the verb, so a reported value is never read as the range.
_FORWARD_VERB = r"\b(?:expected|projected|forecast(?:ed)?|anticipated|estimated)\s+to\s+(?:be|range|total)\b"
_METRIC_FIRST_REVENUE_RE = re.compile(rf"\brevenues?\b[^.$%]{{0,120}}?{_FORWARD_VERB}[^$.%]{{0,30}}\$\s*{_AMOUNT}{_RANGE_SEP}\$?\s*{_AMOUNT}", _F)
_METRIC_FIRST_GROSS_MARGIN_RE = re.compile(rf"\bgross margins?\b[^.$%]{{0,120}}?{_FORWARD_VERB}[^$.%0-9]{{0,30}}([0-9]{{1,2}}(?:\.[0-9]+)?)\s*%{_RANGE_SEP}([0-9]{{1,2}}(?:\.[0-9]+)?)\s*%", _F)
_EPS_LABEL = r"\b(?:eps|earnings per share|net (?:income|earnings|loss) per share)\b"
_METRIC_FIRST_EPS_RE = re.compile(rf"{_EPS_LABEL}[^.$%]{{0,120}}?{_FORWARD_VERB}[^$.%]{{0,30}}\$\s*([0-9]+(?:\.[0-9]+)?){_RANGE_SEP}\$?\s*([0-9]+(?:\.[0-9]+)?)", _F)
# A midpoint and a tolerance (2.5.9, MRVL): "Net revenue is expected to be
# $3.150 billion +/- 5%", "... net income per share is expected to be $0.53
# +/- $0.05 per share". The bounds are computed exactly (_pm_bounds).
_PLUS_MINUS = r"(?:\+\s*/\s*[-−]|±|plus or minus)"
_PM_NUMBER = r"([0-9][0-9,]*(?:\.[0-9]+)?)"
_PM_UNIT = r"(?:\s*(billion|million|thousand|bn|mn|b|m|k)\b)?"
# A comma may precede the tolerance (NVDA: "$108.0 billion, plus or minus 2%").
_PM_TOLERANCE = rf"\s*,?\s*{_PLUS_MINUS}\s*(\$)?\s*{_PM_NUMBER}\s*(%|(?:billion|million|thousand|bn|mn|b|m|k)\b)?"
_METRIC_FIRST_REVENUE_PM_RE = re.compile(rf"\brevenues?\b[^.$%]{{0,120}}?{_FORWARD_VERB}[^$.%]{{0,30}}\$\s*{_PM_NUMBER}{_PM_UNIT}{_PM_TOLERANCE}", _F)
_METRIC_FIRST_EPS_PM_RE = re.compile(rf"{_EPS_LABEL}[^.$%]{{0,120}}?{_FORWARD_VERB}[^$.%]{{0,30}}\$\s*{_PM_NUMBER}{_PM_UNIT}{_PM_TOLERANCE}", _F)
_UNIT_EXP = {"billion": 9, "bn": 9, "b": 9, "million": 6, "mn": 6, "m": 6, "thousand": 3, "k": 3}
# A margin and a tolerance in points (NVDA: "gross margins are expected to be 74.0%, plus or minus 50 basis points").
_METRIC_FIRST_GROSS_MARGIN_PM_RE = re.compile(rf"\bgross margins?\b[^.$%]{{0,120}}?{_FORWARD_VERB}[^$.%0-9]{{0,30}}([0-9]{{1,2}}(?:\.[0-9]+)?)\s*%\s*,?\s*{_PLUS_MINUS}\s*([0-9]+(?:\.[0-9]+)?)\s*(basis points?|bps|percentage points?|%)", _F)
# Release tables, read as flattened text (2.5.10, VRT): "Third Quarter 2026 Guidance Net sales $3,650M -
# $3,850M ... Adjusted diluted EPS (1) $1.77 - $1.83". A row is a label, an optional footnote marker and
# the range, under a guidance or outlook heading with no sentence break between them.
_TABLE_FOOTNOTE = r"(?:\s*\(\d\))?"
# An outlook bullet puts "of" or "in the range of" between label and range (LITE: "Non-GAAP diluted net
# income per share of $4.05 to $4.35"); a two-column table may put an "N/A" first cell there (SNDK, 2.5.20).
_ROW_LEAD = r"\s*(?:of\s+|in the range of\s+|:\s*)?(?:N/A\s+)?"
_TABLE_REVENUE_RE = re.compile(rf"\b(?:net sales|(?:total )?(?:net )?revenues?){_TABLE_FOOTNOTE}{_ROW_LEAD}\$\s*{_AMOUNT}{_RANGE_SEP}\$?\s*{_AMOUNT}", _F)
_TABLE_GROSS_MARGIN_RE = re.compile(rf"\b(?:(?:adjusted|non-GAAP|GAAP)\s+)?gross margins?{_TABLE_FOOTNOTE}{_ROW_LEAD}([0-9]{{1,2}}(?:\.[0-9]+)?)\s*%{_RANGE_SEP}([0-9]{{1,2}}(?:\.[0-9]+)?)\s*%", _F)
_TABLE_EPS_RE = re.compile(rf"\b(?:(?:adjusted|non-GAAP|GAAP)\s+)?(?:diluted\s+)?(?:eps|earnings per share|net income per share){_TABLE_FOOTNOTE}{_ROW_LEAD}\$\s*([0-9]+(?:\.[0-9]+)?){_RANGE_SEP}\$?\s*([0-9]+(?:\.[0-9]+)?)", _F)
# An outlook row stated as a midpoint and tolerance, or as an approximate point (2.5.21, MU: "Revenue $61.5 billion
# ± $1.5 billion", "Diluted earnings per share $37.84 ± $1.00", "Gross margin Approximately 85.95%").
_TABLE_REVENUE_PM_RE = re.compile(rf"\b(?:net sales|(?:total )?(?:net )?revenues?){_TABLE_FOOTNOTE}{_ROW_LEAD}\$\s*{_PM_NUMBER}{_PM_UNIT}{_PM_TOLERANCE}", _F)
_TABLE_EPS_PM_RE = re.compile(rf"\b(?:(?:adjusted|non-GAAP|GAAP)\s+)?(?:diluted\s+)?(?:eps|earnings per share|net income per share){_TABLE_FOOTNOTE}{_ROW_LEAD}\$\s*{_PM_NUMBER}{_PM_UNIT}{_PM_TOLERANCE}", _F)
_APPROX = r"(?:approximately|about|~)\s*"
_TABLE_GROSS_MARGIN_POINT_RE = re.compile(rf"\b(?:(?:adjusted|non-GAAP|GAAP)\s+)?gross margins?{_TABLE_FOOTNOTE}{_ROW_LEAD}{_APPROX}([0-9]{{1,2}}(?:\.[0-9]+)?)\s*%", _F)
_TABLE_HEADING_RE = re.compile(r"\b(?:guidance|outlook)\b", _F)
# "... diluted EPS of $5.82 to $5.92 and adjusted diluted EPS of $6.65 to $6.75" (VRT): the second range.
_EPS_CONTINUATION_RE = re.compile(r"\band (?:adjusted|non-GAAP|GAAP) (?:diluted )?(?:eps|earnings per share|net income per share) of \$\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|-|–|—)\s*\$?\s*([0-9]+(?:\.[0-9]+)?)", _F)


# An outlook table's scale and columns (2.5.20, SNDK: "Business Outlook ... (in millions, except per share
# amounts) GAAP Non-GAAP (1) Revenue $10,300 - $10,800 $10,300 - $10,800 Gross Margin 83.0% - 84.9% 83.0% -
# 85.0% ... Diluted Net Income Per Share N/A $44.00 - $46.00"). Both are read only between the outlook heading
# and the row, with no sentence break after them.
_OUTLOOK_SCALE_RE = re.compile(r"\(\s*(?:\$|US\$|dollars|amounts)?\s*in\s+(thousands|millions|billions)\b", _F)
# A header may name its columns "GAAP(1) Outlook Non-GAAP(2) Outlook", with an "Adjustments" column between (MU, 2.5.21).
_OUTLOOK_COLUMNS_RE = re.compile(r"\b(Non-?\s?GAAP|GAAP)(?:\s*\(\d\))?(?:\s+Outlook)?(?:\s+Adjustments)?\s+(Non-?\s?GAAP|GAAP)(?:\s*\(\d\))?(?:\s+Outlook)?(?=\s+[A-Z])", _F)
_SENTENCE_BREAK_RE = re.compile(r"[.!?]\s+[A-Z]")
_SECOND_AMOUNT_CELL_RE = re.compile(rf"^\s*\$\s*{_AMOUNT}{_RANGE_SEP}\$?\s*{_AMOUNT}", _F)
_SECOND_PCT_CELL_RE = re.compile(r"^\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%\s*(?:to|and|-|–|—)\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%", _F)
_SECOND_PM_CELL_RE = re.compile(rf"^\s*\$\s*{_PM_NUMBER}{_PM_UNIT}{_PM_TOLERANCE}", _F)
_SECOND_POINT_CELL_RE = re.compile(rf"^\s*{_APPROX}([0-9]{{1,2}}(?:\.[0-9]+)?)\s*%", _F)


def _outlook_statement(text: str, at: int, pattern: re.Pattern) -> re.Match | None:
    """The last statement a pattern finds in the 400 characters before `at`, if no sentence break follows it."""
    before = text[max(0, at - 400):at]
    last = None
    for m in pattern.finditer(before):
        last = m
    return last if last is not None and not _SENTENCE_BREAK_RE.search(before[last.end():]) else None


def _under_guidance_heading(text: str, at: int) -> bool:
    """A table row sits under a guidance or outlook heading within 400 characters, with no sentence break between."""
    before = text[max(0, at - 400):at]
    last = -1
    for m in _TABLE_HEADING_RE.finditer(before):
        last = m.end()
    return last >= 0 and not re.search(r"[.!?]\s+[A-Z]", before[last:])


def _dec(text: str) -> tuple[int, int]:
    t = text.replace(",", "")
    dot = t.find(".")
    return (int(t), 0) if dot < 0 else (int(t.replace(".", "")), -(len(t) - dot - 1))


def _dec_text(n: int, exp: int, unit_exp: int) -> str:
    """An exact decimal n x 10^exp written in units of 10^unit_exp, trailing zeros dropped."""
    places = unit_exp - exp
    neg = n < 0
    digits = str(-n if neg else n)
    if places > 0:
        digits = digits.rjust(places + 1, "0")
        digits = re.sub(r"\.?0+$", "", f"{digits[:-places]}.{digits[-places:]}")
    elif places < 0:
        digits = digits + "0" * (-places)
    return f"{'-' if neg else ''}{digits}"


def _point_bounds(mid: str, tol: str, unit: str) -> dict:
    """The low and high of a margin "N% +/- M basis points" (or percentage points), exactly."""
    m_n, m_exp = _dec(mid)
    t_n, t_exp = _dec(tol)
    t_exp = t_exp - 2 if re.search(r"basis|bps", unit, re.I) else t_exp
    exp = min(m_exp, t_exp)
    mn = m_n * 10 ** (m_exp - exp)
    tn = t_n * 10 ** (t_exp - exp)
    return {"low": _dec_text(mn - tn, exp, 0), "high": _dec_text(mn + tn, exp, 0)}


def _pm_bounds(mid: str, mid_unit: str | None, dollar: str | None, tol: str, tol_unit: str | None) -> dict | None:
    """The low and high of "midpoint +/- tolerance", in the midpoint's unit: a
    percentage of the midpoint, or an amount in its own unit (the midpoint's
    when it names none). A bare tolerance with no $, % or unit is not read."""
    m_n, m_exp = _dec(mid)
    t_n, t_exp = _dec(tol)
    mid_exp = _UNIT_EXP.get((mid_unit or "").lower(), 0)
    suffix = f" {mid_unit}" if mid_unit else ""
    if tol_unit == "%":
        # mid x (100 -/+ p) / 100
        hundred = 100 * 10 ** (-t_exp)
        exp = m_exp + t_exp - 2
        return {"low": _dec_text(m_n * (hundred - t_n), exp, 0) + suffix, "high": _dec_text(m_n * (hundred + t_n), exp, 0) + suffix}
    if not dollar and not tol_unit:
        return None
    tol_exp = _UNIT_EXP.get(tol_unit.lower(), 0) if tol_unit else mid_exp
    exp = min(m_exp + mid_exp, t_exp + tol_exp)
    mn = m_n * 10 ** (m_exp + mid_exp - exp)
    tn = t_n * 10 ** (t_exp + tol_exp - exp)
    return {"low": _dec_text(mn - tn, exp, mid_exp) + suffix, "high": _dec_text(mn + tn, exp, mid_exp) + suffix}


# The basis a range is stated on, read from its own clause: the sentence up
# to the range, and the words after it up to the next value or clause break
# ("... between $1.85 and $2.05 on a non-GAAP basis.").
_CLAUSE_START_RE = re.compile(r"(?:[.;!?]\s|•)", _F)
_CLAUSE_TAIL_RE = re.compile(r"^[^.;,$%•]{0,80}?(?=[.;,$%•]|\sand\s|$)", _F)
_NON_GAAP_RE = re.compile(r"\bnon-?\s?GAAP\b|\badjusted\b", _F)
_GAAP_RE = re.compile(r"\bGAAP\b", _F)
_BOTH_BASES_RE = re.compile(r"\bGAAP and non-?\s?GAAP\b|\bnon-?\s?GAAP and GAAP\b", _F)


def _clause_basis(clause: str) -> str:
    # One range for both (NVDA: "GAAP and non-GAAP gross margins are expected to be 74.0% ...") (2.5.10).
    if _BOTH_BASES_RE.search(clause):
        return "GAAP_AND_NON_GAAP"
    if _NON_GAAP_RE.search(clause):
        return "NON_GAAP"
    return "GAAP" if _GAAP_RE.search(clause) else "NOT_STATED"


def _column_basis(word: str) -> str:
    return "NON_GAAP" if _NON_GAAP_RE.search(word) else "GAAP"


def _range_basis(text: str, at: int, length: int, anchor: int | None = None) -> str:
    """The basis of the clause holding a range: from the last clause break before its first amount (`anchor`) to the
    words after it. A break inside the match ends an earlier clause: BE's "...GAAP to Non-GAAP financial measures
    ... Guidance ... • Revenue: $3.4B - $3.8B" is not a non-GAAP range (2.5.21)."""
    if anchor is None:
        anchor = at
    before = text[max(0, anchor - 200):anchor]
    start = 0
    for m in _CLAUSE_START_RE.finditer(before):
        start = m.end()
    tail = _CLAUSE_TAIL_RE.match(text[at + length:])
    return _clause_basis(before[start:] + text[anchor:at + length] + (tail.group(0) if tail else ""))


def guidance_ranges(text: str, period_of=None) -> dict:
    """Guidance ranges stated in release text; low and high are the number text as
    written (computed exactly for a midpoint and tolerance). Keyword-first wording
    wins; metric-first wording ("revenue ... is expected to be between") is read
    when there is none, then release-table rows under a guidance heading. Ranges
    for the same metric on another basis are kept as alternates."""
    def pick(money, *patterns):
        found = []
        periods = []
        for pattern, kind in patterns:
            for m in pattern.finditer(text):
                at = m.start()
                table_row = kind in ("table", "table_pm", "table_point")
                if table_row and not _under_guidance_heading(text, at):
                    continue
                if kind in ("pm_amount", "table_pm"):
                    bounds = _pm_bounds(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5))
                elif kind == "pm_points":
                    bounds = _point_bounds(m.group(1), m.group(2), m.group(3))
                elif kind == "table_point":
                    bounds = {"low": m.group(1), "high": m.group(1)}
                else:
                    bounds = {"low": m.group(1), "high": m.group(2)}
                if not bounds:
                    continue
                first_at = m.start(1) if m.start(1) >= 0 else at
                # An unscaled amount takes the outlook table's stated scale: "$10,300 - $10,800" (in millions).
                scale_word = None
                if money and not re.search(r"[a-z]\s*$", bounds["low"], _F) and not re.search(r"[a-z]\s*$", bounds["high"], _F):
                    stated = _outlook_statement(text, first_at, _OUTLOOK_SCALE_RE)
                    scale_word = re.sub(r"s$", "", stated.group(1).lower()) if stated else None

                def scaled(t: str, word=scale_word) -> str:
                    return f"{t} {word}" if word else t

                # Under a "GAAP Non-GAAP" header a range takes its column's basis; an "N/A" first cell puts it in the second.
                columns = _outlook_statement(text, first_at, _OUTLOOK_COLUMNS_RE)
                two_columns = None
                if columns and _column_basis(columns.group(1)) != _column_basis(columns.group(2)):
                    two_columns = [_column_basis(columns.group(1)), _column_basis(columns.group(2))]
                column = 1 if two_columns and re.search(r"\bN/A\s*\$?\s*$", text[max(0, first_at - 12):first_at], _F) else 0
                stated_as = "OUTLOOK_ROW" if kind == "table" else "RANGE" if kind == "range" else "POINT_ESTIMATE" if kind == "table_point" else "MIDPOINT_PLUS_MINUS"
                found.append({
                    "excerpt": m.group(0), "low": scaled(bounds["low"]), "high": scaled(bounds["high"]),
                    # A table row's basis is its own label: the rows above it belong to other metrics.
                    "basis": two_columns[column] if two_columns else _clause_basis(m.group(0)) if table_row else _range_basis(text, at, len(m.group(0)), first_at),
                    "statedAs": stated_as,
                })
                periods.append(period_of(at, len(m.group(0))) if period_of else None)
                if two_columns and column == 0 and kind not in ("pm_amount", "pm_points"):
                    rest = text[m.end():]
                    second = None
                    if kind == "table_pm":
                        c = _SECOND_PM_CELL_RE.search(rest)
                        b = _pm_bounds(c.group(1), c.group(2), c.group(3), c.group(4), c.group(5)) if c else None
                        second = {"text": c.group(0), **b} if c and b else None
                    elif kind == "table_point":
                        c = _SECOND_POINT_CELL_RE.search(rest)
                        second = {"text": c.group(0), "low": c.group(1), "high": c.group(1)} if c else None
                    else:
                        c = (_SECOND_PCT_CELL_RE if re.search(r"%\s*$", m.group(0)) else _SECOND_AMOUNT_CELL_RE).search(rest)
                        second = {"text": c.group(0), "low": c.group(1), "high": c.group(2)} if c else None
                    if second:
                        found.append({"excerpt": m.group(0) + second["text"], "low": scaled(second["low"]), "high": scaled(second["high"]),
                                      "basis": two_columns[1], "statedAs": stated_as})
                        periods.append(period_of(at, len(m.group(0))) if period_of else None)
        if not found:
            return None
        primary, rest = found[0], found[1:]
        alternates: list[dict] = []
        # Another basis for the same target period only: a quarter's row is not a year's alternate (2.5.10).
        for i, r in enumerate(rest):
            same_period = periods[0] is None or periods[i + 1] is None or periods[i + 1] == periods[0]
            if same_period and r["basis"] != primary["basis"] and not any(a["basis"] == r["basis"] for a in alternates):
                alternates.append(r)
        return {**primary, "alternates": alternates}
    return {
        "revenue": pick(True, (_REVENUE_FIRST_RE, "range"), (_KEYWORD_FIRST_RE, "range"), (_METRIC_FIRST_REVENUE_RE, "range"),
                        (_METRIC_FIRST_REVENUE_PM_RE, "pm_amount"), (_TABLE_REVENUE_RE, "table"), (_TABLE_REVENUE_PM_RE, "table_pm")),
        "grossMargin": pick(False, (_GROSS_MARGIN_RE, "range"), (_METRIC_FIRST_GROSS_MARGIN_RE, "range"),
                            (_METRIC_FIRST_GROSS_MARGIN_PM_RE, "pm_points"), (_TABLE_GROSS_MARGIN_RE, "table"), (_TABLE_GROSS_MARGIN_POINT_RE, "table_point")),
        "eps": pick(False, (_EPS_RE, "range"), (_EPS_CONTINUATION_RE, "range"), (_METRIC_FIRST_EPS_RE, "range"),
                    (_METRIC_FIRST_EPS_PM_RE, "pm_amount"), (_TABLE_EPS_RE, "table"), (_TABLE_EPS_PM_RE, "table_pm")),
    }


# ── Reported release metrics (2.5.20) ───────────────────────────────────────
#
# extract_earnings_metrics' text fallback read MU's FQ4 2026 revenue as 54229 USD: the highlights bullet
# "Revenue of $54.23 billion versus…" has no result verb, so the statement table's "$ 54,229" (in millions)
# was read as written. A figure is read for a metric only when the sentence proves it is that metric's
# result for the quarter:
# - a sentence with guidance, award, backlog or ± wording is never read, nor one that names only an annual
#   period ("in 2025", "fiscal 2026 revenue", "full year");
# - the label leads the sentence or follows a period or GAAP qualifier: "Gaming revenue" and "Services
#   revenues" are segments, not the total;
# - a change verb ("increased 5.2% to") reads the figure after "to", never the change itself;
# - a non-GAAP or adjusted figure, a per-share figure for a total, and a figure attributed to another metric
#   ("$13.7 billion of free cash flow" after "capital expenditures") are never read;
# - an unscaled figure takes the release's table scale only when the release declares exactly one.

# Sentences end at . ! ? (also before a closing quote) and at bullet markers ("•Revenue" needs no space after
# the bullet, MU 2.5.20), including the " o " bullets
# SEC-rendered press releases carry, so one bullet's value is never read for another's label.
_RELEASE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|(?<=[.!?][\"”’])\s+|\s+[•●▪◦]\s*|\s+·\s+|\s+o\s+(?=[A-Z])", re.A)
_GUIDANCE_CONTEXT_RE = re.compile(r"\b(?:guidance|outlook|expect(?:s|ed|ation)?|forecast(?:s|ing)?|project(?:s|ed)?|target|range)\b", _F)
_REPORTED_CONTEXT_RE = re.compile(r"\b(?:reported|was|were|totaled|totalled|generated|delivered|achieved|increased|decreased|grew|rose|declined|fell)\b", _F)
# "$125 million" of government awards is not revenue (ASTS Q2 2026).
_NON_RESULT_CONTEXT_RE = re.compile(r"\b(?:awards?|awarded|contract value|aggregate value|backlog|bookings|orders?|pipeline|contracted)\b", _F)
_ANNUAL_CONTEXT_RE = re.compile(
    r"\b(?:full[\s-]year|(?:full )?fiscal year|year[\s-]to[\s-]date|twelve months|12 months|annual|in (?:fiscal )?(?:19|20)[0-9]{2}|fiscal (?:19|20)[0-9]{2}|FY ?(?:19|20)?[0-9]{2})\b", _F)
_QUARTER_CONTEXT_RE = re.compile(r"\b(?:quarter(?:ly)?|Q[1-4]|three months|13 weeks)\b", _F)

_RELEASE_LABELS = {
    "revenue": r"\b(?:net sales|net revenues?|total revenues?|revenues?)\b",
    "epsDiluted": r"\b(?:diluted (?:earnings|net income|net loss|income|loss)(?: \(loss\))? per (?:common )?share|diluted eps|eps \(diluted\))(?![\w(])",
    "grossMargin": r"\bgross margin\b",
    "operatingIncome": r"\boperating income\b",
    "freeCashFlow": r"\bfree cash flow\b",
    "capex": r"\b(?:capital expenditures|capex)\b",
}
# Between a label and its figure: words, and the period tokens a release puts there ("for the fourth quarter
# of fiscal 2026 was", "ended July 27, 2025, of", "in Q1 FY27") or a change ("increased 5.2% to"). Never another figure.
_RELEASE_GAP = r"(?:[^$0-9%]|\b(?:19|20)[0-9]{2}\b|\b[0-3]?[0-9], (?:19|20)[0-9]{2}\b|\bQ[1-4]\b|\bFY ?[0-9]{2,4}\b|\b[0-9]{1,3}(?:\.[0-9]+)?\s*(?:%|percent)(?=\s+to\b))"
# Groups: "(" before the $, "(" after it, "-", integer, fraction, scale word. "($0.12)" and "$ (0.12)" are negative.
_RELEASE_MONEY_BASE = r"(\(\s*)?\$\s*(\()?\s*(-)?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(\.[0-9]+)?\s*\)?"
_RELEASE_MONEY = _RELEASE_MONEY_BASE + r"(?:\s*(billion|million|thousand|bn|mn|mm|b|m|k)\b)?"
_RELEASE_PCT = r"()()(-)?([0-9]{1,2})(\.[0-9]+)?\s*%()"
_RELEASE_SCALE = {"billion": 1e9, "bn": 1e9, "b": 1e9, "million": 1e6, "mn": 1e6, "mm": 1e6, "m": 1e6, "thousand": 1e3, "k": 1e3}
_RELEASE_TABLE_SCALE_RE = re.compile(r"\(\s*(?:\$|US\$|dollars|amounts)?\s*in\s+(thousands|millions|billions)\b", _F)
_RELEASE_NON_GAAP_RE = re.compile(r"\bnon-?\s?GAAP\b|\badjusted\b", _F)
_RELEASE_PLUS_MINUS_RE = re.compile(r"±|\+/-|\bplus or minus\b", _F)
_RELEASE_PER_SHARE_AFTER_RE = re.compile(r"^\s*(?:per|a)\s+(?:diluted\s+|basic\s+)?(?:common\s+)?(?:share|ADS|ADR)\b", _F)
_RELEASE_PER_DILUTED_WORDING_RE = re.compile(r"\bper\s+diluted\s+(?:common\s+)?share\b", _F)
_RELEASE_PER_DILUTED_SHARE_RE = re.compile(_RELEASE_MONEY_BASE + r"()\s+per\s+diluted\s+(?:common\s+)?share\b", _F)
_RELEASE_CHANGE_RE = re.compile(r"\b(?:increase[sd]?|decrease[sd]?|grew|grow(?:s|th)?|rose|declined?|fell|up|down|improved?)\b", _F)
_RELEASE_CHANGE_TO_RE = re.compile(r"\bto\s*$", _F)
_RELEASE_ATTRIBUTED_RE = re.compile(
    r"^\s*(?:of|in)\s+(?:[A-Za-z-]+\s+){0,2}?(free cash flow|operating cash flow|cash|revenues?|net sales|net income|net loss|operating income|capital expenditures|gross margin|earnings)\b", _F)
# The word before a label: a period or GAAP qualifier, a result verb, or a sentence, clause or heading boundary.
_RELEASE_QUALIFIER_RE = re.compile(
    r"^(?:gaap|total|record|quarterly|consolidated|reported|delivered|achieved|generated|posted|preliminary|the|our|its|with|and|of|in|for|q[1-4]|fy[0-9]{2,4}|(?:19|20)[0-9]{2}|first|second|third|fourth|(?:first|second|third|fourth)-quarter|quarter|fiscal)$", _F)
_RELEASE_NEGATIVE_RE = re.compile(r"\bnegative\s*$", _F)
_RELEASE_LOSS_RE = re.compile(r"\bloss\b", _F)
_RELEASE_INCOME_RE = re.compile(r"\b(?:income|earnings)\b", _F)
_RELEASE_NET_RESULT_RE = re.compile(r"\bnet (?:income|earnings|loss)\b", _F)
_RELEASE_NET_LOSS_RE = re.compile(r"\bnet loss\b", _F)


def _release_scales(text: str) -> list[str]:
    scales: list[str] = []
    for m in _RELEASE_TABLE_SCALE_RE.finditer(text):
        unit = re.sub(r"s$", "", m.group(1).lower())
        if unit not in scales:
            scales.append(unit)
    return scales


def _qualified_label(before: str) -> bool:
    found = re.search(r"(\S+)\s*$", before, re.A)
    raw = found.group(1) if found else ""
    if raw.endswith((":", ";", ",")):
        return True
    token = re.sub(r"^\W+|\W+$", "", raw, flags=re.A)
    return token == "" or bool(_RELEASE_QUALIFIER_RE.match(token))


def _whole(n: float) -> float | int:
    return int(n) if n == int(n) else n


def release_text_metric(text: str, metric: str) -> dict | None:
    """The first reported release figure for a metric under the 2.5.20 rules, or None."""
    collapsed = _collapse_ws(text or "")
    table_scales = _release_scales(collapsed)
    label_source = _RELEASE_LABELS[metric]
    money = metric not in ("grossMargin", "epsDiluted")
    amount_source = _RELEASE_PCT if metric == "grossMargin" else _RELEASE_MONEY
    label = re.compile(label_source, _F)
    result_lead = re.compile(rf"^(?:GAAP\s+|total\s+)?(?:{label_source})\s*(?:of|:|was|were|totaled|totalled)\b", _F)
    label_first = re.compile(rf"({label_source})({_RELEASE_GAP}{{0,100}}?)({amount_source})", _F)
    for raw in _RELEASE_SPLIT_RE.split(collapsed):
        piece = raw.strip(" ")
        if not piece or len(piece) > 600:
            continue
        if _GUIDANCE_CONTEXT_RE.search(piece) or _NON_RESULT_CONTEXT_RE.search(piece) or _RELEASE_PLUS_MINUS_RE.search(piece):
            continue
        if _ANNUAL_CONTEXT_RE.search(piece) and not _QUARTER_CONTEXT_RE.search(piece):
            continue
        labelled = bool(label.search(piece))
        per_share_form = metric == "epsDiluted" and bool(_RELEASE_PER_DILUTED_WORDING_RE.search(piece))
        if not labelled and not per_share_form:
            continue
        if not _REPORTED_CONTEXT_RE.search(piece) and not result_lead.search(piece) and not per_share_form:
            continue
        candidates: list[tuple[str, re.Match]] = []
        if labelled:
            candidates.extend(("label", m) for m in label_first.finditer(piece))
        if metric == "epsDiluted":
            candidates.extend(("perShare", m) for m in _RELEASE_PER_DILUTED_SHARE_RE.finditer(piece))
        candidates.sort(key=lambda c: c[1].start())
        for kind, m in candidates:
            outer_paren, inner_paren, minus, int_part, frac, word = m.groups()[3:9] if kind == "label" else m.groups()[0:6]
            amount_start = m.end() - len(m.group(3)) if kind == "label" else m.start()
            gap = m.group(2) if kind == "label" else ""
            if kind == "label":
                if not _qualified_label(piece[:m.start()]):
                    continue
                if _RELEASE_CHANGE_RE.search(gap) and not _RELEASE_CHANGE_TO_RE.search(gap):
                    continue
            # A label-first figure: its label and the 40 characters before it; a per-share figure: the piece before it.
            lead = piece[max(0, m.start() - 40) if kind == "label" else 0:amount_start]
            if _RELEASE_NON_GAAP_RE.search(lead):
                continue
            after = piece[m.end():]
            if money and _RELEASE_PER_SHARE_AFTER_RE.search(after):
                continue
            attributed = _RELEASE_ATTRIBUTED_RE.search(after)
            if kind == "label" and attributed and not label.search(attributed.group(1)):
                continue
            scale = 1.0
            scale_basis = None
            if money:
                if word:
                    scale = _RELEASE_SCALE[word.lower()]
                    scale_basis = "AS_WRITTEN"
                elif len(table_scales) > 1:
                    continue
                elif len(table_scales) == 1:
                    scale = _RELEASE_SCALE[table_scales[0]]
                    scale_basis = f"RELEASE_TABLE_IN_{table_scales[0].upper()}S"
            magnitude = float(int_part.replace(",", "") + (frac or ""))
            scaled = _whole(magnitude) if scale == 1 else int(math.floor(magnitude * scale + 0.5))
            # An unsigned figure stated as a loss is negative: "diluted net loss per share of $0.12", "net loss of $0.12 per
            # diluted share", "free cash flow was negative $5 billion".
            stated_loss = bool(_RELEASE_NEGATIVE_RE.search(gap))
            if not stated_loss and metric == "epsDiluted":
                if kind == "label":
                    stated_loss = bool(_RELEASE_LOSS_RE.search(m.group(1))) and not _RELEASE_INCOME_RE.search(m.group(1))
                else:
                    results = _RELEASE_NET_RESULT_RE.findall(lead)
                    stated_loss = bool(results) and bool(_RELEASE_NET_LOSS_RE.search(results[-1]))
            negative = bool(outer_paren) or inner_paren == "(" or minus == "-" or stated_loss
            value = -scaled if negative and scaled != 0 else scaled
            raw_value = (m.group(3) if kind == "label" else m.group(0)).strip(" ")
            return {"value": value, "rawValue": raw_value, "scaleBasis": scale_basis, "sentence": piece}
    return None


# ── Event query terms ───────────────────────────────────────────────────────

def stem_word(word: str) -> str:
    """A light suffix stem for matching event query words.

    "launch", "launches", "launched" and "launching" all become "launch";
    "release" and "released" become "releas". Applied to both the query and
    the evidence words.
    """
    w = word.lower()
    if len(w) > 5 and w.endswith("ing"):
        w = w[:-3]
    elif len(w) > 4 and w.endswith("ed"):
        w = w[:-2]
    elif len(w) > 4 and w.endswith("es"):
        w = w[:-2]
    elif len(w) > 3 and w.endswith("s") and not w.endswith("ss"):
        w = w[:-1]
    if len(w) > 4 and w.endswith("e"):
        w = w[:-1]
    return w


_CONFIDENCE_RANK = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


def rank_evidence(items: list[dict], confidence_of) -> list[dict]:
    """Evidence ordered by confidence (HIGH first), then newest first; stable otherwise."""
    indexed = list(enumerate(items))
    indexed.sort(key=lambda pair: (
        _CONFIDENCE_RANK.get(confidence_of(pair[1]), 3),
        _Desc(str(pair[1].get("publishedAt") or "")),
        pair[0],
    ))
    return [item for _, item in indexed]


class _Desc:
    """Reverses string order inside a sort key."""

    __slots__ = ("value",)

    def __init__(self, value: str) -> None:
        self.value = value

    def __lt__(self, other: "_Desc") -> bool:
        return self.value > other.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Desc) and self.value == other.value
