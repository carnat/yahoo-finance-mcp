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
    """The fiscal year a period ending on `end` is named for.

    The calendar year it ends in, except that a year ending in the first week
    of January (a 52/53-week year nearest December 31) belongs to the year
    before. A company that names its years otherwise (a retailer's "fiscal
    2025" ending February 2026) is not detected; a fiscal year the filing
    states wins wherever one is read.
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
