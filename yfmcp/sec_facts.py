"""SEC companyconcept fact selection across equivalent concepts (2.4.4).

Filers move between equivalent us-gaap concepts: ASTS reported revenue as
RevenueFromContractWithCustomerExcludingAssessedTax until 2023 and as
...IncludingAssessedTax since. Reading the first concept that has any facts
returned a 2022 quarter as "latest". The concept with the newest filing of
the requested form wins instead, and a pinned accession must be matched.

Mirrors worker/src/sec-facts.ts; scripts/test_sec_facts.py requires identical
output from both.
"""

from __future__ import annotations

import datetime
import re

REVENUE_CONCEPTS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
]


def pick_concept_facts(candidates: list[dict], form: str, accession: str | None = None, on_tie: str = "first") -> dict | None:
    """Of several concepts for one fact, the one whose facts for the form were filed most recently.

    The earlier-listed concept wins a tie. With a pinned accession only facts
    from that filing count, so the first concept tagged in it wins. The
    returned facts are already limited to the form and accession.

    With on_tie="larger" (2.5.15), a candidate filed on the same date as the
    best replaces it when its newest-period magnitude is larger. Candidates
    whose facts are not a list are skipped.
    """
    want_form = form.upper()
    want_accession = accession.strip() if accession else ""
    best = None
    best_filed = ""
    best_size = -1
    for candidate in candidates:
        if not isinstance(candidate.get("facts"), list):
            continue
        rows = [f for f in candidate["facts"]
                if str(f.get("form") or "").upper() == want_form
                and (not want_accession or str(f.get("accn") or "") == want_accession)]
        if not rows:
            continue
        filed = max(str(f.get("filed") or "") for f in rows)
        size = _newest_period_magnitude(rows, filed)
        if best is None or filed > best_filed or (on_tie == "larger" and filed == best_filed and size > best_size):
            best = {"concept": candidate["concept"], "facts": rows}
            best_filed = filed
            best_size = size
    return best


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _newest_period_magnitude(rows: list[dict], filed: str) -> float:
    """The largest magnitude a concept reports for the latest period end in its newest filing (2.5.15)."""
    in_filing = [f for f in rows if str(f.get("filed") or "") == filed and _is_number(f.get("val"))]
    end = max((str(f.get("end") or "") for f in in_filing), default="")
    return max((abs(f["val"]) for f in in_filing if str(f.get("end") or "") == end), default=-1)


def _duration_days(fact: dict) -> float:
    import datetime
    start = fact.get("start") if isinstance(fact.get("start"), str) else ""
    end = fact.get("end") if isinstance(fact.get("end"), str) else ""
    if not start:
        return 0
    try:
        return (datetime.date.fromisoformat(end) - datetime.date.fromisoformat(start)).days
    except ValueError:
        return 1e9


def filing_fact_in_accession(candidates: list[dict], accession: str) -> dict | None:
    """A filing's own value for a fact (2.4.5).

    The first concept tagged in the accession, at the latest period end it
    reports, and the shortest period there, so a 10-Q gives its quarter rather
    than the year to date or the prior-year comparative. Any form counts; the
    old snapshot read 10-K forms only.
    """
    want = accession.strip()
    for candidate in candidates:
        if not isinstance(candidate.get("facts"), list):
            continue
        rows = [f for f in candidate["facts"]
                if str(f.get("accn") or "") == want and isinstance(f.get("end"), str) and f.get("end") and f.get("val") is not None]
        if not rows:
            continue
        latest_end = max(str(f["end"]) for f in rows)
        best = None
        for f in rows:
            if str(f["end"]) != latest_end:
                continue
            if best is None or _duration_days(f) < _duration_days(best):
                best = f
        if best is not None:
            return {"concept": candidate["concept"], "fact": best}
    return None


# ── Named fiscal-year periods (2.5.16) ───────────────────────────────────────
#
# `period` was honoured only as "latest"; any other string ("FY2025", "2025", a typo) left the rows
# unselected, so the first fact came back: BE's 2016 revenue labelled FY2018 for "FY2025". A period is now
# "latest" or the issuer's fiscal year ("FY2025" or "2025"); anything else is refused.

FILING_PERIOD_HELP = 'period must be "latest" or a fiscal year ("FY2025" or "2025").'

_FISCAL_YEAR_PERIOD = re.compile(r"(?:FY\s?)?([0-9]{4})", re.IGNORECASE)
_ISO_DAY = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})")


def parse_filing_period(raw) -> dict | None:
    """The period a caller named, or None when it is not one this reader resolves."""
    text = str("latest" if raw is None else raw).strip()
    if text == "" or text.lower() == "latest":
        return {"kind": "latest"}
    m = _FISCAL_YEAR_PERIOD.fullmatch(text)
    return {"kind": "fiscalYear", "year": int(m.group(1))} if m else None


def _period_end_year(end: str) -> int | None:
    """The year of the period end less a week: a 52/53-week year ending in the first week of January belongs to the year before."""
    m = _ISO_DAY.match(end[:10])
    if not m:
        return None
    year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
    # Date.parse takes any day 1-31 of a valid month and rolls it over (Feb 30 is Mar 2).
    if year < 1 or not 1 <= month <= 12 or not 1 <= day <= 31:
        return None
    try:
        return (datetime.date(year, month, 1) + datetime.timedelta(days=day - 1 - 7)).year
    except OverflowError:
        return None


def _js_string(value) -> str:
    """String(value ?? "") for the scalars SEC facts carry."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _js_number(value) -> float:
    """Number(value): a number as is, a numeric string parsed, null/"" zero, anything else NaN."""
    nan = float("nan")
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    if not isinstance(value, str):
        return nan
    text = value.strip()
    if text == "":
        return 0
    try:
        if re.fullmatch(r"0[xXoObB][0-9a-fA-F]+", text):
            return int(text, 0)
        if "_" in text or text.lower().lstrip("+-") in ("nan", "inf", "infinity"):
            return nan
        return float(text)
    except ValueError:
        return nan


def select_fiscal_year_rows(rows: list[dict], year: int) -> dict:
    """The rows for the annual period the issuer calls fiscal `year`.

    Its fy (fp FY) as the filing that first reported the period states it, within a year of the period end,
    else the year the period ends in (the rule reconcile_metric_sources uses). The latest filed rows come
    first (a restated value wins). The fiscal years found are listed for a miss.
    """
    groups: dict[str, list[dict]] = {}
    for f in rows:
        if not isinstance(f.get("end"), str):
            continue
        groups.setdefault(f"{_js_string(f.get('start'))}|{f['end']}", []).append(f)
    years: set[int] = set()
    matched: list[dict] = []
    for group in groups.values():
        first = min(group, key=lambda f: _js_string(f.get("filed")))
        end_year = _period_end_year(first["end"])
        if end_year is None:
            continue
        fy = _js_number(first.get("fy"))
        finite = fy == fy and fy not in (float("inf"), float("-inf"))
        issuer_year = int(fy) if (_js_string(first.get("fp")).upper() == "FY" and finite and fy in (end_year, end_year - 1)) else end_year
        years.add(issuer_year)
        if issuer_year == year:
            matched.extend(group)
    matched.sort(key=lambda f: (_js_string(f.get("filed")), _js_string(f.get("end"))), reverse=True)
    return {"rows": matched, "fiscalYears": sorted(years)}
