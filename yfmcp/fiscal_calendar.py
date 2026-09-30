"""Fiscal-period identity for 52/53-week calendars (2.5.11), mirroring worker/src/fiscal-calendar.ts.

A 52/53-week fiscal year ends on a weekday nearest a month end, so its end
date drifts up to a week past that month end: AMD-style years "ending on the
last Saturday nearest December 31" can end on January 2, and AEHR's quarters
end on the Friday nearest the month end (May 29, 2026). The fiscal year and
quarter a period belongs to are read from the date a week before its end,
never from the raw end date's year or month.
"""

from __future__ import annotations

import datetime as _dt
import re

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3, "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6,
    "jul": 7, "july": 7, "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10, "october": 10, "nov": 11,
    "november": 11, "dec": 12, "december": 12,
}

# A 52/53-week period can end up to this many days after its nominal month end.
WEEK_DRIFT_DAYS = 7

_ISO_RE = re.compile(r"\d{4}-\d{2}-\d{2}", re.A)


def nominal_period_end(end: str | None) -> str | None:
    """The period end moved back a week: its nominal month end for a 52/53-week period, the same month otherwise."""
    if not isinstance(end, str) or not _ISO_RE.match(end):
        return None
    try:
        day = _dt.date.fromisoformat(end[:10])
    except ValueError:
        return None
    return (day - _dt.timedelta(days=WEEK_DRIFT_DAYS)).isoformat()


def fiscal_year_of_period_end(end: str | None) -> int | None:
    """The fiscal year a period ending on `end` is named for by the period-end rule.

    The calendar year it ends in, except that a year ending in the first week
    of January (a 52/53-week year nearest December 31) belongs to the year
    before. This rule alone cannot see a company that names its years
    otherwise (DG's "fiscal 2025" ended January 30, 2026);
    `filing_fiscal_year_label` (a filing's own tagged year) and
    `fiscal_year_naming` (the company's stated years) correct it where they
    are read.
    """
    nominal = nominal_period_end(end)
    return int(nominal[:4]) if nominal else None


def fiscal_year_label(end: str | None) -> str | None:
    """"FY2025" for a fiscal year ending on `end`; None without a date."""
    year = fiscal_year_of_period_end(end)
    return None if year is None else f"FY{year}"


def text_date(month: str, day: str, year: str) -> str | None:
    """"June 25, 2027" / "Jan. 2, 2027" -> "2027-06-25"; None when it is not a whole date."""
    m = _MONTHS.get(month.lower().rstrip("."))
    try:
        return _dt.date(int(year), m, int(day)).isoformat() if m else None
    except ValueError:
        return None


TEXT_DATE_SOURCE = (r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sept?(?:ember)?|Oct(?:ober)?"
                    r"|Nov(?:ember)?|Dec(?:ember)?)\.?\s+(\d{1,2}),?\s+(20\d\d)")


def _day(iso: str) -> int:
    return _dt.date.fromisoformat(iso[:10]).toordinal()


def fiscal_quarter_of(end: str, year_end: str) -> int | None:
    """The quarter (1-4) of a fiscal year ending on `year_end` that a period ending on `end` closes.

    Counted from the weeks between them; None when `end` is not within that
    year or is not within a week of a quarter boundary.
    """
    gap = _day(year_end) - _day(end)
    if gap < -WEEK_DRIFT_DAYS or gap > 280:
        return None
    quarters_left = round(gap / 91)
    if abs(gap - quarters_left * 91) > WEEK_DRIFT_DAYS + 3:
        return None
    q = 4 - quarters_left
    return q if 1 <= q <= 4 else None


# ── Fiscal-year naming (2.5.13) ─────────────────────────────────────────────
#
# Companies name a year that ends early in a calendar year differently: DG's year ending January 30,
# 2026 is its fiscal 2025 (named for the year it starts in), WMT's year ending January 31, 2026 its
# fiscal 2026. Only the company says which. Its annual reports state it (companyfacts fy of each 10-K's
# own year); the offset between that stated year and the period-end rule, when every recent annual
# report agrees, names the company's other fiscal years.

_NAMING_CONCEPTS = ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet", "NetIncomeLoss", "EarningsPerShareDiluted")
_NAMING_REPORTS = 3


def stated_fiscal_years(companyfacts) -> list[dict]:
    """Each 10-K's own year: its latest annual period end and the fiscal year it states (companyfacts fy), newest first."""
    facts = companyfacts.get("facts") if isinstance(companyfacts, dict) else None
    usgaap = (facts or {}).get("us-gaap") or {}
    by_accession: dict[str, dict] = {}
    for concept in _NAMING_CONCEPTS:
        units = (usgaap.get(concept) or {}).get("units") or {}
        for rows in units.values():
            for f in rows or []:
                fy = f.get("fy")
                if (not str(f.get("form") or "").startswith("10-K") or f.get("fp") != "FY" or isinstance(fy, bool) or not isinstance(fy, int)
                        or not isinstance(f.get("accn"), str) or not isinstance(f.get("end"), str) or not isinstance(f.get("start"), str)):
                    continue
                try:
                    days = (_dt.date.fromisoformat(f["end"][:10]) - _dt.date.fromisoformat(f["start"][:10])).days
                except ValueError:
                    continue
                if not 350 <= days <= 380:
                    continue
                prev = by_accession.get(f["accn"])
                if prev is None or f["end"] > prev["periodEnd"]:
                    by_accession[f["accn"]] = {"periodEnd": f["end"], "fiscalYear": fy}
    by_end: dict[str, int] = {}
    for v in by_accession.values():
        by_end[v["periodEnd"]] = v["fiscalYear"]
    return [{"periodEnd": e, "fiscalYear": y} for e, y in sorted(by_end.items(), reverse=True)]


def fiscal_year_naming(companyfacts) -> dict:
    """The company's fiscal-year naming against the period-end rule, from its latest annual reports; offset 0 when they are not read or disagree."""
    if companyfacts is None:
        return {"offset": 0, "basis": "PERIOD_END_RULE_SEC_NOT_READ", "periodEnd": None, "statedFiscalYear": None, "calendar": None}
    stated = stated_fiscal_years(companyfacts)
    recent = stated[:_NAMING_REPORTS]
    if not recent:
        return {"offset": 0, "basis": "PERIOD_END_RULE_NO_STATED_YEAR", "periodEnd": None, "statedFiscalYear": None, "calendar": None}
    calendar = settled_fiscal_calendar([r["periodEnd"] for r in stated])
    offsets = [r["fiscalYear"] - (fiscal_year_of_period_end(r["periodEnd"]) or r["fiscalYear"]) for r in recent]
    if any(o != offsets[0] for o in offsets) or abs(offsets[0]) > 1:
        return {"offset": 0, "basis": "PERIOD_END_RULE_STATED_YEARS_INCONSISTENT", "periodEnd": recent[0]["periodEnd"], "statedFiscalYear": recent[0]["fiscalYear"],
                "calendar": calendar}
    return {"offset": offsets[0], "basis": "SEC_STATED_FISCAL_YEAR", "periodEnd": recent[0]["periodEnd"], "statedFiscalYear": recent[0]["fiscalYear"], "calendar": calendar}


# ── Fiscal calendar rule (2.5.16) ───────────────────────────────────────────
#
# A provider states a fiscal year's end as a nominal month end (Yahoo: DG's year ends 2027-01-31); the
# company's own year ends on the Friday nearest January 31, 2027-01-29. The rule is read from the company's
# recent annual period ends and kept only if it reproduces every one of them: the month end, the weekday
# nearest the month end, or the last such weekday of the month. When two rules fit the past but part ways
# for the year asked about, no date is projected.

_WEEKDAYS = ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")


def _last_day_of_month(year: int, month: int) -> int:
    """Day number (proleptic ordinal) of the last day of `month` (1-12) in `year`."""
    first_of_next = _dt.date(year + 1, 1, 1) if month == 12 else _dt.date(year, month + 1, 1)
    return first_of_next.toordinal() - 1


def _weekday_of_day(day: int) -> int:
    """0 = Sunday .. 6 = Saturday, as JavaScript's getUTCDay."""
    return day % 7


def _iso_of_day(day: int) -> str:
    return _dt.date.fromordinal(day).isoformat()


def _year_end_for(pattern: str, year: int, month: int, weekday: int | None) -> str | None:
    """The year end a pattern gives for a nominal month and year; None when the pattern needs a weekday and has none."""
    last = _last_day_of_month(year, month)
    if pattern == "MONTH_END":
        return _iso_of_day(last)
    if weekday is None:
        return None
    if pattern == "LAST_WEEKDAY_OF_MONTH":
        return _iso_of_day(last - ((_weekday_of_day(last) - weekday + 7) % 7))
    for d in range(-3, 4):
        if _weekday_of_day(last + d) == weekday:
            return _iso_of_day(last + d)
    return None


def fiscal_calendar(period_ends: list[str]) -> dict | None:
    """The company's fiscal calendar from its annual period ends (newest first; two or more), or None when no rule fits them all."""
    ends = [e for e in period_ends if nominal_period_end(e) is not None]
    if len(ends) < 2:
        return None
    nominal = [nominal_period_end(e) for e in ends]
    month = int(nominal[0][5:7])
    if any(int(n[5:7]) != month for n in nominal):
        return None
    weekday = _weekday_of_day(_day(ends[0]))

    def fits(pattern: str) -> bool:
        return all(_year_end_for(pattern, int(nominal[i][:4]), month, weekday) == e[:10] for i, e in enumerate(ends))

    if fits("MONTH_END"):
        patterns = ["MONTH_END"]
    else:
        patterns = [p for p in ("WEEKDAY_NEAREST_MONTH_END", "LAST_WEEKDAY_OF_MONTH") if fits(p)]
    if not patterns:
        return None
    return {
        "patterns": patterns,
        "month": month,
        "weekday": None if patterns[0] == "MONTH_END" else _WEEKDAYS[weekday],
        "basis": "SEC_ANNUAL_PERIOD_ENDS",
        "periodEnds": [e[:10] for e in ends],
    }


def settled_fiscal_calendar(period_ends: list[str]) -> dict | None:
    """The calendar read from as few recent year ends as settle it: older ends are added one at a time while a
    rule still fits them all, until one rule is left (MU's Thursday nearest August 31 and its last Thursday of
    August agree for 2023-2025; 2020's September 3 settles it). An end no rule fits (a changed calendar) stops
    the look-back."""
    settled = None
    for n in range(2, len(period_ends) + 1):
        cal = fiscal_calendar(period_ends[:n])
        if not cal:
            break
        settled = cal
        if len(cal["patterns"]) == 1:
            break
    return settled


def company_fiscal_year_end(calendar: dict | None, provider_end: str | None) -> str | None:
    """The company's year end for the fiscal year a provider dates `provider_end`; None without a rule, in another month, or when the fitting rules disagree."""
    if not calendar or not provider_end:
        return None
    nominal = nominal_period_end(provider_end)
    if not nominal or int(nominal[5:7]) != calendar["month"]:
        return None
    weekday = None if calendar["weekday"] is None else _WEEKDAYS.index(calendar["weekday"])
    dates = {_year_end_for(p, int(nominal[:4]), calendar["month"], weekday) for p in calendar["patterns"]}
    return next(iter(dates)) if len(dates) == 1 else None


_FY_FOCUS_RE = re.compile(r'name="dei:DocumentFiscalYearFocus"[^>]*>\s*(?:<[^>]+>\s*)*(\d{4})\s*<')


def document_fiscal_year_focus(html: str | None) -> int | None:
    """The fiscal year an inline XBRL filing states for itself (dei:DocumentFiscalYearFocus); None when it is not tagged."""
    if not html:
        return None
    m = _FY_FOCUS_RE.search(html)
    return int(m.group(1)) if m else None


def filing_fiscal_year_label(focus: int | None, report_date: str | None) -> str | None:
    """A filing's fiscal-year label (2.5.14): the year it tags for itself (dei:DocumentFiscalYearFocus).

    DG's 10-K for the year ended January 30, 2026 reads FY2025 and AAPL's
    December-quarter 10-Q reads its next fiscal year. A tagged year more than
    one year from the period-end rule is taken as a mistag and the rule is
    used; so is an untagged filing (8-Ks, older HTML filings).
    """
    rule_year = fiscal_year_of_period_end(report_date)
    if focus is not None and (rule_year is None or abs(focus - rule_year) <= 1):
        return f"FY{focus}"
    return None if rule_year is None else f"FY{rule_year}"
