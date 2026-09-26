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
_TRAILING_PUNCT_RE = re.compile(r"[\s,;:]+$", _F)
_LEADING_ARTICLE_RE = re.compile(r"^(?:our|the|its)\s+", _F)
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.;])\s+", _F)


def _concentration_subject(segment: str) -> str:
    start = 0
    for m in _SUBJECT_BREAK_RE.finditer(segment):
        start = m.end()
    subject = _LEADING_YEAR_RE.sub("", segment[start:].strip(), count=1)
    return _TRAILING_PUNCT_RE.sub("", subject, count=1)


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
                if _AGGREGATE_SUBJECT_RE.search(subject):
                    findings.append({"kind": "aggregate", "name": None, "description": subject, "valuePct": pct, "year": year, "sectionHeading": section, "sentence": sentence})
                elif _UNNAMED_SUBJECT_RE.search(subject) or not re.match(r"[A-Z]", subject) or len(subject) > 60:
                    findings.append({"kind": "customer", "name": None, "description": subject, "valuePct": pct, "year": year, "sectionHeading": section, "sentence": sentence})
                else:
                    findings.append({"kind": "customer", "name": _LEADING_ARTICLE_RE.sub("", subject, count=1), "description": subject, "valuePct": pct, "year": year, "sectionHeading": section, "sentence": sentence})
    years = [f["year"] for f in findings if f["year"] is not None]
    latest = max(years) if years else None
    seen: set[str] = set()
    kept: list[dict] = []
    for f in findings:
        if latest is not None and f["year"] is not None and f["year"] != latest:
            continue
        if f["kind"] == "aggregate":
            key = f"a|{f['description'].lower()}"
        elif f["name"]:
            key = f"n|{f['name'].lower()}"
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

_AMOUNT = r"([0-9][0-9.,]*(?:\s*(?:billion|million|thousand|bn|m|k))?)"
_RANGE_SEP = r"\s*(?:to|and|-|–|—)\s*"
# "full year 2026 revenue guidance of $150.0 million to $200.0 million" (ASTS)
_REVENUE_FIRST_RE = re.compile(rf"\brevenues?\s+(?:guidance|outlook|forecast)\b[^$.]{{0,40}}\$\s*{_AMOUNT}{_RANGE_SEP}\$?\s*{_AMOUNT}", _F)
# "expects revenue between $X and $Y" / "guidance: revenue of $X to $Y"
_KEYWORD_FIRST_RE = re.compile(rf"(?:expects|guidance|outlook)[^.\n]{{0,120}}revenue[^$]{{0,25}}\$?\s*{_AMOUNT}{_RANGE_SEP}\$?\s*{_AMOUNT}", _F)
_GROSS_MARGIN_RE = re.compile(r"gross margin[^0-9]{0,20}([0-9]{1,2}(?:\.[0-9]+)?)\s*%\s*(?:to|and|-|–|—)\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%", _F)
_EPS_RE = re.compile(r"(?:expects|guidance|outlook)[^.\n]{0,120}(?:eps|earnings per share)[^$]{0,25}\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|and|-|–|—)\s*\$?\s*([0-9]+(?:\.[0-9]+)?)", _F)


def guidance_ranges(text: str) -> dict:
    """Guidance ranges stated in release text; low and high are the number text as written."""
    def pick(m):
        return {"excerpt": m.group(0), "low": m.group(1), "high": m.group(2)} if m else None
    return {
        "revenue": pick(_REVENUE_FIRST_RE.search(text)) or pick(_KEYWORD_FIRST_RE.search(text)),
        "grossMargin": pick(_GROSS_MARGIN_RE.search(text)),
        "eps": pick(_EPS_RE.search(text)),
    }


# ── Reported release metrics ────────────────────────────────────────────────

# Sentences end at . ! ? and at bullet markers, including the " o " bullets
# SEC-rendered press releases carry, so one bullet's value is never read for
# another's label.
_METRIC_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\s+[•●▪◦·]\s+|\s+o\s+(?=[A-Z])", re.A)
_GUIDANCE_CONTEXT_RE = re.compile(r"\b(?:guidance|outlook|expect(?:s|ed|ation)?|forecast|project(?:s|ed)?|target|range)\b", _F)
_REPORTED_CONTEXT_RE = re.compile(r"\b(?:reported|was|were|totaled|totalled|generated|delivered|achieved)\b", _F)
# "$125 million" of government awards is not revenue (ASTS Q2 2026).
_NON_RESULT_CONTEXT_RE = re.compile(r"\b(?:awards?|awarded|contract value|aggregate value|backlog|bookings|orders?|pipeline|contracted)\b", _F)

REVENUE_LABEL = r"\b(?:net sales|revenues?)\b"
USD_AMOUNT = r"(\$\s*[-+]?[0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)"
EPS_LABEL = r"\b(?:diluted (?:earnings per share|eps)|eps \(diluted\))\b"
EPS_AMOUNT = r"(\$\s*\(?[-+]?[0-9]+(?:\.[0-9]+)?\)?)"
PCT_AMOUNT = r"([0-9]{1,2}(?:\.[0-9]+)?\s*%)"


def reported_text_metric(text: str, label_source: str, value_source: str) -> dict | None:
    """The first explicitly reported value for a label.

    A sentence with a result verb, no guidance or award wording, and the value
    within 100 non-digit characters after the label.
    """
    label = re.compile(label_source, _F)
    anchored = re.compile(rf"(?:{label_source})\D{{0,100}}?{value_source}", _F)
    for raw in _METRIC_SPLIT_RE.split(_collapse_ws(text)):
        sentence = raw.strip(" ")
        if not sentence or not label.search(sentence):
            continue
        if _GUIDANCE_CONTEXT_RE.search(sentence) or _NON_RESULT_CONTEXT_RE.search(sentence) or not _REPORTED_CONTEXT_RE.search(sentence):
            continue
        m = anchored.search(sentence)
        if m and m.group(1):
            return {"rawValue": m.group(1).strip(" "), "sentence": sentence}
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
