// Fiscal-period identity for 52/53-week calendars (2.5.11), mirrored by yfmcp/fiscal_calendar.py.
//
// A 52/53-week fiscal year ends on a weekday nearest a month end, so its end
// date drifts up to a week past that month end: AMD-style years "ending on the
// last Saturday nearest December 31" can end on January 2, and AEHR's quarters
// end on the Friday nearest the month end (May 29, 2026). The fiscal year and
// quarter a period belongs to are read from the date a week before its end,
// never from the raw end date's year or month.

const MONTHS: Record<string, number> = {
  jan: 1, january: 1, feb: 2, february: 2, mar: 3, march: 3, apr: 4, april: 4, may: 5, jun: 6, june: 6,
  jul: 7, july: 7, aug: 8, august: 8, sep: 9, sept: 9, september: 9, oct: 10, october: 10, nov: 11, november: 11, dec: 12, december: 12,
};

// A 52/53-week period can end up to this many days after its nominal month end.
export const WEEK_DRIFT_DAYS = 7;

function dayNumber(iso: string): number {
  return Math.floor(Date.parse(`${iso.slice(0, 10)}T00:00:00Z`) / 86_400_000);
}

function isoOfDay(day: number): string | null {
  return Number.isFinite(day) ? new Date(day * 86_400_000).toISOString().slice(0, 10) : null;
}

/** The period end moved back a week: its nominal month end for a 52/53-week period, the same month otherwise. */
export function nominalPeriodEnd(end: string | null | undefined): string | null {
  if (typeof end !== "string" || !/^\d{4}-\d{2}-\d{2}/.test(end)) return null;
  // Not a calendar date ("2026-02-30"): no period end.
  if (isoOfDay(dayNumber(end)) !== end.slice(0, 10)) return null;
  return isoOfDay(dayNumber(end) - WEEK_DRIFT_DAYS);
}

/**
 * The fiscal year a period ending on `end` is named for: the calendar year it
 * ends in, except that a year ending in the first week of January (a 52/53-week
 * year nearest December 31) belongs to the year before. A company that names its
 * years otherwise (a retailer's "fiscal 2025" ending February 2026) is not
 * detected; a fiscal year the filing states wins wherever one is read.
 */
export function fiscalYearOfPeriodEnd(end: string | null | undefined): number | null {
  const nominal = nominalPeriodEnd(end);
  return nominal ? Number(nominal.slice(0, 4)) : null;
}

/** "FY2025" for a fiscal year ending on `end`; null without a date. */
export function fiscalYearLabel(end: string | null | undefined): string | null {
  const year = fiscalYearOfPeriodEnd(end);
  return year == null ? null : `FY${year}`;
}

/** "June 25, 2027" / "Jan. 2, 2027" -> "2027-06-25"; null when it is not a whole date. */
export function textDate(month: string, day: string, year: string): string | null {
  const m = MONTHS[month.toLowerCase().replace(/\.$/, "")];
  const d = Number(day);
  if (!m || !(d >= 1 && d <= 31)) return null;
  const iso = `${year}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  return isoOfDay(dayNumber(iso)) === iso ? iso : null;
}

export const TEXT_DATE_SOURCE = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sept?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\\.?\\s+(\\d{1,2}),?\\s+(20\\d\\d)";

/**
 * The quarter (1-4) of a fiscal year ending on `yearEnd` that a period ending on
 * `end` closes, from the weeks between them; null when `end` is not within
 * that year or is not within a week of a quarter boundary.
 */
export function fiscalQuarterOf(end: string, yearEnd: string): number | null {
  const gap = dayNumber(yearEnd) - dayNumber(end);
  if (gap < -WEEK_DRIFT_DAYS || gap > 280) return null;
  const quartersLeft = Math.round(gap / 91);
  if (Math.abs(gap - quartersLeft * 91) > WEEK_DRIFT_DAYS + 3) return null;
  const q = 4 - quartersLeft;
  return q >= 1 && q <= 4 ? q : null;
}
