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
 * The fiscal year a period ending on `end` is named for by the period-end rule: the
 * calendar year it ends in, except that a year ending in the first week of January
 * (a 52/53-week year nearest December 31) belongs to the year before. This rule alone
 * cannot see a company that names its years otherwise (DG's "fiscal 2025" ended
 * January 30, 2026); `filingFiscalYearLabel` (a filing's own tagged year) and
 * `fiscalYearNaming` (the company's stated years) correct it where they are read.
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

// ── Fiscal-year naming (2.5.13) ─────────────────────────────────────────────
//
// Companies name a year that ends early in a calendar year differently: DG's year ending January 30,
// 2026 is its fiscal 2025 (named for the year it starts in), WMT's year ending January 31, 2026 its
// fiscal 2026. Only the company says which. Its annual reports state it (companyfacts fy of each 10-K's
// own year); the offset between that stated year and the period-end rule, when every recent annual
// report agrees, names the company's other fiscal years.

const NAMING_CONCEPTS = ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet", "NetIncomeLoss", "EarningsPerShareDiluted"];
const NAMING_REPORTS = 3;

export type FiscalYearNaming = { offset: number; basis: string; periodEnd: string | null; statedFiscalYear: number | null; calendar: FiscalCalendar | null };

/** Each 10-K's own year: its latest annual period end and the fiscal year it states (companyfacts fy), newest first. */
export function statedFiscalYears(companyfacts: unknown): { periodEnd: string; fiscalYear: number }[] {
  const usgaap = ((((companyfacts ?? {}) as Record<string, unknown>).facts ?? {}) as Record<string, unknown>)["us-gaap"] as Record<string, unknown> | undefined;
  const byAccession = new Map<string, { periodEnd: string; fiscalYear: number }>();
  for (const concept of NAMING_CONCEPTS) {
    const units = (((usgaap?.[concept] ?? {}) as Record<string, unknown>).units ?? {}) as Record<string, Record<string, unknown>[]>;
    for (const rows of Object.values(units)) {
      for (const f of rows) {
        if (!/^10-K/.test(String(f.form ?? "")) || f.fp !== "FY" || typeof f.fy !== "number" || typeof f.accn !== "string" || typeof f.end !== "string" || typeof f.start !== "string") continue;
        const days = (Date.parse(`${f.end}T00:00:00Z`) - Date.parse(`${f.start}T00:00:00Z`)) / 86_400_000;
        if (!(days >= 350 && days <= 380)) continue;
        const prev = byAccession.get(f.accn);
        if (!prev || f.end > prev.periodEnd) byAccession.set(f.accn, { periodEnd: f.end, fiscalYear: f.fy });
      }
    }
  }
  const byEnd = new Map<string, number>();
  for (const v of byAccession.values()) byEnd.set(v.periodEnd, v.fiscalYear);
  return [...byEnd.entries()].map(([periodEnd, fiscalYear]) => ({ periodEnd, fiscalYear })).sort((a, b) => (a.periodEnd < b.periodEnd ? 1 : -1));
}

/** The company's fiscal-year naming against the period-end rule, from its latest annual reports; offset 0 when they are not read or disagree. */
export function fiscalYearNaming(companyfacts: unknown): FiscalYearNaming {
  if (companyfacts == null) return { offset: 0, basis: "PERIOD_END_RULE_SEC_NOT_READ", periodEnd: null, statedFiscalYear: null, calendar: null };
  const recent = statedFiscalYears(companyfacts).slice(0, NAMING_REPORTS);
  if (recent.length === 0) return { offset: 0, basis: "PERIOD_END_RULE_NO_STATED_YEAR", periodEnd: null, statedFiscalYear: null, calendar: null };
  const calendar = settledFiscalCalendar(statedFiscalYears(companyfacts).map((r) => r.periodEnd));
  const offsets = recent.map((r) => r.fiscalYear - (fiscalYearOfPeriodEnd(r.periodEnd) ?? r.fiscalYear));
  if (offsets.some((o) => o !== offsets[0]) || Math.abs(offsets[0]) > 1) {
    return { offset: 0, basis: "PERIOD_END_RULE_STATED_YEARS_INCONSISTENT", periodEnd: recent[0].periodEnd, statedFiscalYear: recent[0].fiscalYear, calendar };
  }
  return { offset: offsets[0], basis: "SEC_STATED_FISCAL_YEAR", periodEnd: recent[0].periodEnd, statedFiscalYear: recent[0].fiscalYear, calendar };
}

// ── Fiscal calendar rule (2.5.16) ───────────────────────────────────────────
//
// A provider states a fiscal year's end as a nominal month end (Yahoo: DG's year ends 2027-01-31); the
// company's own year ends on the Friday nearest January 31, 2027-01-29. The rule is read from the company's
// recent annual period ends and kept only if it reproduces every one of them: the month end, the weekday
// nearest the month end, or the last such weekday of the month. When two rules fit the past but part ways
// for the year asked about, no date is projected.

const WEEKDAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
export type FiscalCalendarPattern = "MONTH_END" | "WEEKDAY_NEAREST_MONTH_END" | "LAST_WEEKDAY_OF_MONTH";
export type FiscalCalendar = { patterns: FiscalCalendarPattern[]; month: number; weekday: string | null; basis: string; periodEnds: string[] };

function lastDayOfMonth(year: number, month: number): number {
  return Math.floor(Date.UTC(year, month, 1) / 86_400_000) - 1;
}

function weekdayOfDay(day: number): number {
  return new Date(day * 86_400_000).getUTCDay();
}

/** The year end a pattern gives for a nominal month and year; null when the pattern needs a weekday and has none. */
function yearEndFor(pattern: FiscalCalendarPattern, year: number, month: number, weekday: number | null): string | null {
  const last = lastDayOfMonth(year, month);
  if (pattern === "MONTH_END") return isoOfDay(last);
  if (weekday == null) return null;
  if (pattern === "LAST_WEEKDAY_OF_MONTH") return isoOfDay(last - ((weekdayOfDay(last) - weekday + 7) % 7));
  for (let d = -3; d <= 3; d++) if (weekdayOfDay(last + d) === weekday) return isoOfDay(last + d);
  return null;
}

/** The company's fiscal calendar from its annual period ends (newest first; two or more), or null when no rule fits them all. */
export function fiscalCalendar(periodEnds: string[]): FiscalCalendar | null {
  const ends = periodEnds.filter((e) => nominalPeriodEnd(e) != null);
  if (ends.length < 2) return null;
  const nominal = ends.map((e) => nominalPeriodEnd(e) as string);
  const month = Number(nominal[0].slice(5, 7));
  if (nominal.some((n) => Number(n.slice(5, 7)) !== month)) return null;
  const weekday = weekdayOfDay(dayNumber(ends[0]));
  const fits = (pattern: FiscalCalendarPattern) => ends.every((e, i) => yearEndFor(pattern, Number(nominal[i].slice(0, 4)), month, weekday) === e.slice(0, 10));
  const patterns = fits("MONTH_END")
    ? ["MONTH_END" as const]
    : (["WEEKDAY_NEAREST_MONTH_END", "LAST_WEEKDAY_OF_MONTH"] as const).filter(fits);
  if (patterns.length === 0) return null;
  return {
    patterns: [...patterns],
    month,
    weekday: patterns[0] === "MONTH_END" ? null : WEEKDAYS[weekday],
    basis: "SEC_ANNUAL_PERIOD_ENDS",
    periodEnds: ends.map((e) => e.slice(0, 10)),
  };
}

/**
 * The calendar read from as few recent year ends as settle it: older ends are added one at a time while a
 * rule still fits them all, until one rule is left (MU's Thursday nearest August 31 and its last Thursday of
 * August agree for 2023-2025; 2020's September 3 settles it). An end no rule fits (a changed calendar) stops
 * the look-back.
 */
export function settledFiscalCalendar(periodEnds: string[]): FiscalCalendar | null {
  let settled: FiscalCalendar | null = null;
  for (let n = 2; n <= periodEnds.length; n++) {
    const cal = fiscalCalendar(periodEnds.slice(0, n));
    if (!cal) break;
    settled = cal;
    if (cal.patterns.length === 1) break;
  }
  return settled;
}

/** The company's year end for the fiscal year a provider dates `providerEnd`; null without a rule, in another month, or when the fitting rules disagree. */
export function companyFiscalYearEnd(calendar: FiscalCalendar | null, providerEnd: string | null | undefined): string | null {
  if (!calendar || !providerEnd) return null;
  const nominal = nominalPeriodEnd(providerEnd);
  if (!nominal || Number(nominal.slice(5, 7)) !== calendar.month) return null;
  const weekday = calendar.weekday == null ? null : WEEKDAYS.indexOf(calendar.weekday);
  const dates = [...new Set(calendar.patterns.map((p) => yearEndFor(p, Number(nominal.slice(0, 4)), calendar.month, weekday)))];
  return dates.length === 1 ? dates[0] : null;
}

/** The fiscal year an inline XBRL filing states for itself (dei:DocumentFiscalYearFocus); null when it is not tagged. */
export function documentFiscalYearFocus(html: string | null | undefined): number | null {
  if (!html) return null;
  const m = /name="dei:DocumentFiscalYearFocus"[^>]*>\s*(?:<[^>]+>\s*)*(\d{4})\s*</.exec(html);
  return m ? Number(m[1]) : null;
}

/**
 * A filing's fiscal-year label (2.5.14): the year it tags for itself (dei:DocumentFiscalYearFocus), so DG's
 * 10-K for the year ended January 30, 2026 reads FY2025 and AAPL's December-quarter 10-Q reads its next
 * fiscal year. A tagged year more than one year from the period-end rule is taken as a mistag and the rule
 * is used; so is an untagged filing (8-Ks, older HTML filings).
 */
export function filingFiscalYearLabel(focus: number | null, reportDate: string | null | undefined): string | null {
  const ruleYear = fiscalYearOfPeriodEnd(reportDate);
  if (focus != null && (ruleYear == null || Math.abs(focus - ruleYear) <= 1)) return `FY${focus}`;
  return ruleYear == null ? null : `FY${ruleYear}`;
}
