"""The fiscal period an earnings release states for itself (2.5.19).

A period is read only from one phrase that joins a quarter to a year: "fourth quarter and full year of
fiscal 2026", "fiscal 2026 fourth quarter", "Q4 FY2026", "FOURTH-QUARTER AND FULL-YEAR 2026". The quarter and
the year may not be joined across a sentence, headline or dash: MU's release headline "...position Micron for
a record fiscal 2027" sits 140 characters before "results for its fourth quarter". The first phrase in the
release wins (comparative prior-year figures come later; AEHR is a concrete case).

Mirrors worker/src/earnings-period.ts; scripts/test_earnings_period.py requires identical output from both.
"""

from __future__ import annotations

import re

_QUARTER_WORDS = {
    "first": "Q1", "1st": "Q1", "second": "Q2", "2nd": "Q2", "third": "Q3", "3rd": "Q3", "fourth": "Q4", "4th": "Q4",
}
_QW = r"(first|1st|second|2nd|third|3rd|fourth|4th)"

# (pattern, quarter group, year group, numeric quarter); JS \s, \b and [^...] semantics on ASCII text.
_PATTERNS = [
    (
        re.compile(
            rf"\b{_QW}[\s-]+quarter(?:\s+(?:and|&)\s+(?:full[\s-]+)?(?:fiscal[\s-]+)?year)?(?:\s+of)?"
            r"(?:\s+(?:fiscal(?:\s+year)?|FY))?\s*(20\d{2})\b",
            re.IGNORECASE | re.ASCII,
        ),
        1, 2, False,
    ),
    (
        re.compile(
            r"\b(?:fiscal\s+(?:year\s+)?|FY\s*)(20\d{2})[^.!?;:•–—|]{0,40}?\b" + _QW + r"[\s-]+quarter\b",
            re.IGNORECASE | re.ASCII,
        ),
        2, 1, False,
    ),
    (re.compile(r"\bQ([1-4])\s*(?:of\s+)?(?:fiscal\s+(?:year\s+)?|FY\s*)(20\d{2})\b", re.IGNORECASE | re.ASCII), 1, 2, True),
    (re.compile(r"\bfiscal\s+Q([1-4])\s*(?:of\s+)?(20\d{2})\b", re.IGNORECASE | re.ASCII), 1, 2, True),
]


def _compact_excerpt(text: str, max_len: int) -> str:
    value = re.sub(r"\s+", " ", text).strip()
    return value if len(value) <= max_len else value[:max_len].rstrip() + "..."


def extract_earnings_period_from_text(text: str) -> dict[str, str | None]:
    """The issuer fiscal period a release states, or UNRESOLVED; never inferred from a filing date."""
    normalized = re.sub(r"\s+", " ", text or "").strip()
    best = None
    for pattern, quarter_group, year_group, numeric in _PATTERNS:
        match = pattern.search(normalized)
        if not match:
            continue
        if best is not None and best[0].start() <= match.start():
            continue
        quarter = f"Q{match.group(quarter_group)}" if numeric else _QUARTER_WORDS[match.group(quarter_group).lower()]
        best = (match, quarter, match.group(year_group))
    if best is None:
        return {"period": None, "periodStatus": "UNRESOLVED", "periodEvidence": None}
    match, quarter, year = best
    return {
        "period": f"FY{year} {quarter}",
        "periodStatus": "EX99_TEXT_RESOLVED",
        "periodEvidence": _compact_excerpt(match.group(0), 220),
    }
