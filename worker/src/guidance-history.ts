/**
 * Guidance history (2.5.3), shared with yfmcp/guidance_history.py (parity in
 * scripts/test_guidance_and_drivers.py). Pure.
 *
 * Every guidance range is read from an earnings-release exhibit with its
 * target period and source; revisions compare consecutive releases for the
 * same metric and target period; outcomes compare the company's later
 * reported actual (XBRL) with the first and last range. Nothing is inferred:
 * a range whose target period the release does not state is kept but not
 * compared, and an actual that cannot be matched to the period is not
 * evaluated.
 */

import { AUTHORITY_BOUNDARY } from "./evidence.js";
import { guidanceRanges } from "./extraction-rules.js";
import { fiscalQuarterOf, fiscalYearOfPeriodEnd, nominalPeriodEnd, TEXT_DATE_SOURCE, textDate } from "./fiscal-calendar.js";
import { REVENUE_CONCEPTS } from "./sec-facts.js";

type Rec = Record<string, unknown>;

export interface ReleaseText {
  filingDate: string;
  accessionNumber: string;
  url: string | null;
  status: string;
  text: string | null;
}

const SCALE: Record<string, number> = { billion: 1e9, bn: 1e9, million: 1e6, m: 1e6, thousand: 1e3, k: 1e3 };

/** "150.0 million" -> 150000000; the scale of the other end of a range applies when this end has none. */
export function parseAmount(text: string, fallbackUnit: string | null = null): { value: number | null; unit: string | null } {
  const m = /^\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(billion|million|thousand|bn|m|k)?\s*$/i.exec(text);
  if (!m) return { value: null, unit: null };
  const unit = (m[2] ?? fallbackUnit ?? "").toLowerCase() || null;
  const base = parseFloat(m[1].replace(/,/g, ""));
  // Whole units once scaled: 2.05 billion is 2050000000, not 2049999999.9999998.
  return { value: unit ? Math.floor(base * (SCALE[unit] ?? 1) + 0.5) : base, unit };
}

function unitOf(text: string): string | null {
  const m = /(billion|million|thousand|bn|m|k)\s*$/i.exec(text.trim());
  return m ? m[1].toLowerCase() : null;
}

const ORDINAL: Record<string, number> = { first: 1, second: 2, third: 3, fourth: 4 };

function year4(text: string | undefined): number | null {
  if (!text) return null;
  const n = parseInt(text, 10);
  return text.length === 2 ? 2000 + n : n;
}

/**
 * The target period a context names for the guidance ending at `anchor`: the
 * closest named before the anchor, else the first after it. Quarters and
 * halves ("first quarter of fiscal 2026", "second-half 2025", "2H25") win
 * over the fiscal year inside them.
 */
export function guidanceTargetPeriod(context: string, anchor: number = context.length): Rec {
  type Hit = { index: number; end: number; fiscalYear: number | null; quarter: number | null; half: number | null; periodEnd: string | null; fromDate: boolean };
  const found: Hit[] = [];
  const add = (m: RegExpMatchArray, fiscalYear: number | null, quarter: number | null, half: number | null) => {
    const index = m.index ?? 0;
    const end = index + m[0].length;
    // "fiscal 2027 ending June 25, 2027", "third quarter ended October 31, 2026": the stated end date (2.5.11).
    const d = PERIOD_END_AFTER_RE.exec(context.slice(end, end + 60));
    found.push({ index, end, fiscalYear, quarter, half, periodEnd: d ? textDate(d[1], d[2], d[3]) : null, fromDate: false });
  };
  // "the fiscal year ending June 25, 2027" names a 52/53-week year by its end date only: its year is the
  // fiscal year of that date (fiscal-calendar.ts) (2.5.11, AEHR).
  for (const m of context.matchAll(FISCAL_YEAR_ENDING_RE)) {
    const periodEnd = textDate(m[1], m[2], m[3]);
    const index = m.index ?? 0;
    if (periodEnd) found.push({ index, end: index + m[0].length, fiscalYear: fiscalYearOfPeriodEnd(periodEnd), quarter: null, half: null, periodEnd, fromDate: true });
  }
  for (const m of context.matchAll(/\b(?:full[- ]year|fiscal(?: year)?|FY)\s*'?(20\d\d|\d\d)\b/gi)) add(m, year4(m[1]), null, null);
  for (const m of context.matchAll(/\b(20\d\d)\s+(?:full[- ]year|annual)\b/gi)) add(m, year4(m[1]), null, null);
  for (const m of context.matchAll(/\b(first|second|third|fourth) quarter(?: of)?(?: fiscal)?(?: year)?\s*(20\d\d)?/gi)) {
    add(m, year4(m[2]), ORDINAL[m[1].toLowerCase()], null);
  }
  for (const m of context.matchAll(/\bQ([1-4])\s*(?:of\s+)?(?:FY)?\s*'?(20\d\d)?\b/g)) add(m, year4(m[2]), Number(m[1]), null);
  for (const m of context.matchAll(/\b(first|second)[- ]half(?: of)?(?: fiscal)?(?: year)?\s*'?(20\d\d)?\b/gi)) {
    add(m, year4(m[2]), null, ORDINAL[m[1].toLowerCase()]);
  }
  for (const m of context.matchAll(/\b([12])H\s?'?(20\d\d|\d\d)\b/g)) add(m, year4(m[2]), null, Number(m[1]));
  for (const m of context.matchAll(/\bH([12])\s*'?(20\d\d|\d\d)?\b/g)) add(m, year4(m[2]), null, Number(m[1]));
  const parts = found.filter((h) => h.quarter != null || h.half != null);
  const hits = found.filter((h) => h.quarter != null || h.half != null || !parts.some((q) => h.index >= q.index && h.index < q.end));
  if (hits.length === 0) return { label: null, fiscalYear: null, quarter: null, half: null, periodEnd: null, basis: "NOT_STATED" };
  const before = hits.filter((h) => h.index < anchor);
  const best = before.length > 0
    ? before.reduce((a, b) => (b.index >= a.index ? b : a))
    : hits.reduce((a, b) => (b.index < a.index ? b : a));
  const year = best.fiscalYear != null ? ` ${best.fiscalYear}` : "";
  const label = best.quarter != null ? `Q${best.quarter}${year}` : best.half != null ? `H${best.half}${year}` : `FY${best.fiscalYear}`;
  return {
    label,
    fiscalYear: best.fiscalYear,
    quarter: best.quarter,
    half: best.half,
    periodEnd: best.periodEnd,
    basis: best.fiscalYear == null ? "TEXT_YEAR_NOT_STATED" : best.fromDate ? "TEXT_PERIOD_END" : "TEXT",
  };
}

const FISCAL_YEAR_ENDING_RE = new RegExp(`\\bfiscal year (?:ending|ended|that (?:ends|ended)|which (?:ends|ended))(?: on)?\\s+${TEXT_DATE_SOURCE}\\b`, "gi");
const PERIOD_END_AFTER_RE = new RegExp(`^\\s*,?\\s*(?:ending|ended)(?: on)?\\s+${TEXT_DATE_SOURCE}\\b`, "i");

// Sentence and bullet boundaries, including the " o " bullets SEC-rendered releases carry.
const WS = "[\\t\\n\\v\\f\\r \\u00a0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000\\ufeff]";
const BOUNDARY_SOURCE = `[.!?]${WS}|${WS}[\u2022\u25cf\u25aa\u25e6\u00b7]${WS}|${WS}o${WS}(?=[A-Z])`;

/** [start, end) of the sentence or bullet around an excerpt at [at, at + len). */
export function sentenceBounds(text: string, at: number, len: number): [number, number] {
  const windowStart = Math.max(0, at - 300);
  let start = windowStart;
  for (const m of text.slice(windowStart, at).matchAll(new RegExp(BOUNDARY_SOURCE, "g"))) start = windowStart + (m.index ?? 0) + m[0].length;
  const tail = text.slice(at + len, at + len + 200);
  const m = new RegExp(BOUNDARY_SOURCE).exec(tail);
  return [start, at + len + (m ? m.index : tail.length)];
}

/**
 * The period for an excerpt at [at, at + len): from its own sentence first
 * ("expects EPS of $0.10 to $0.20 for fiscal 2027"), else from the 200
 * characters before it (a heading or lead-in). scope says which.
 */
export function periodForExcerpt(text: string, at: number, len: number): Rec {
  const [start, end] = sentenceBounds(text, at, len);
  const inSentence = guidanceTargetPeriod(text.slice(start, end), at + len - start);
  if (inSentence.basis !== "NOT_STATED") return { ...inSentence, scope: "SENTENCE" };
  // The nearest guidance or outlook heading above, read whole: a fixed look-back can cut "third
  // quarter of fiscal 2027" to "fiscal 2027" (NVDA), and bullets can sit far below it (MRVL) (2.5.10).
  // Nearest mention first; one that names no period ("... in its outlook") gives way to the next.
  const base = Math.max(0, at - HEADING_LOOKBACK);
  const mentions = [...text.slice(base, at).matchAll(HEADING_WORD_RE)].map((m) => base + (m.index ?? 0)).reverse();
  for (const heading of mentions) {
    const from = Math.max(0, heading - 100);
    const fromHeading = guidanceTargetPeriod(text.slice(from, Math.min(at, heading + 100)), heading - from);
    if (fromHeading.basis !== "NOT_STATED") return { ...fromHeading, scope: "GUIDANCE_MENTION" };
  }
  const preceding = guidanceTargetPeriod(text.slice(Math.max(0, at - 200), at + len));
  return { ...preceding, scope: preceding.basis === "NOT_STATED" ? null : "PRECEDING_TEXT" };
}

const HEADING_LOOKBACK = 1500;
const HEADING_WORD_RE = /\b(?:outlook|guidance)\b/gi;

const WITHDRAWN_RE = /\bwithdr[ae]w(?:s|n|ing)?\b[^.]{0,60}\b(?:guidance|outlook)\b|\b(?:guidance|outlook)\b[^.]{0,60}\bwithdrawn\b|\bsuspend(?:s|ed|ing)?\b[^.]{0,40}\b(?:guidance|outlook)\b/i;
const REAFFIRM_RE = /\breaffirm(?:s|ed|ing)?\b|\breiterat(?:e|es|ed|ing)\b|\bmaintain(?:s|ed|ing)?\b[^.]{0,30}\b(?:guidance|outlook)\b/i;

/** Guidance ranges stated in one release, each with its target period. */
export function guidanceEntries(release: ReleaseText): Rec[] {
  if (release.status !== "READ" || !release.text) return [];
  const text = release.text;
  const ranges = guidanceRanges(text, (at, len) => (periodForExcerpt(text, at, len).label as string | null) ?? null);
  const out: Rec[] = [];
  for (const metric of ["revenue", "grossMargin", "eps"] as const) {
    const r = ranges[metric];
    if (!r) continue;
    const at = text.indexOf(r.excerpt);
    const [sStart, sEnd] = sentenceBounds(text, at, r.excerpt.length);
    const sentence = text.slice(sStart, sEnd);
    let low: number | null;
    let high: number | null;
    let unit: string;
    if (metric === "revenue") {
      const fallback = unitOf(r.high) ?? unitOf(r.low);
      low = parseAmount(r.low, fallback).value;
      high = parseAmount(r.high, fallback).value;
      unit = "USD";
    } else {
      low = Number(r.low);
      high = Number(r.high);
      unit = metric === "eps" ? "USD/share" : "percent";
    }
    if (low == null || high == null || !Number.isFinite(low) || !Number.isFinite(high)) continue;
    out.push({
      metric,
      targetPeriod: periodForExcerpt(text, at, r.excerpt.length),
      low,
      high,
      midpoint: (low + high) / 2,
      unit,
      basis: r.basis,
      statedAction: REAFFIRM_RE.test(sentence) ? "REAFFIRMED_IN_TEXT" : null,
      releaseDate: release.filingDate,
      accessionNumber: release.accessionNumber,
      sourceUrl: release.url,
      excerpt: r.excerpt.slice(0, 300),
    });
  }
  const withdrawn = WITHDRAWN_RE.exec(text);
  if (withdrawn) {
    const at = withdrawn.index;
    out.push({
      metric: "any",
      targetPeriod: periodForExcerpt(text, at, withdrawn[0].length),
      event: "WITHDRAWN",
      releaseDate: release.filingDate,
      accessionNumber: release.accessionNumber,
      sourceUrl: release.url,
      excerpt: withdrawn[0].slice(0, 300),
    });
  }
  return out;
}

/** Code-unit order, as Python compares strings (localeCompare is locale-dependent). */
function cmp(a: unknown, b: unknown): number {
  const x = String(a);
  const y = String(b);
  return x < y ? -1 : x > y ? 1 : 0;
}

function compare(prev: Rec, cur: Rec): string {
  const eq = (a: unknown, b: unknown) => Math.abs(Number(a) - Number(b)) <= 1e-9 * Math.max(1, Math.abs(Number(a)));
  if (eq(prev.low, cur.low) && eq(prev.high, cur.high)) return "REAFFIRMED";
  const pm = Number(prev.midpoint);
  const cm = Number(cur.midpoint);
  if (!eq(pm, cm)) return cm > pm ? "RAISED" : "LOWERED";
  const pw = Number(prev.high) - Number(prev.low);
  const cw = Number(cur.high) - Number(cur.low);
  return cw < pw ? "NARROWED" : "WIDENED";
}

/** Changes between consecutive releases for the same metric and target period. */
export function guidanceRevisions(entries: Rec[]): Rec[] {
  const groups = new Map<string, Rec[]>();
  for (const e of entries) {
    const tp = e.targetPeriod as Rec;
    if (e.event || tp.label == null || tp.fiscalYear == null) continue;
    const key = `${e.metric}|${tp.label}`;
    groups.set(key, [...(groups.get(key) ?? []), e]);
  }
  const out: Rec[] = [];
  for (const [key, list] of groups) {
    const sorted = [...list].sort((a, b) => cmp(a.releaseDate, b.releaseDate));
    const [metric, label] = key.split("|");
    sorted.forEach((cur, i) => {
      const prev = i > 0 ? sorted[i - 1] : null;
      out.push({
        metric,
        targetPeriod: label,
        releaseDate: cur.releaseDate,
        change: prev ? compare(prev, cur) : "INITIATED",
        from: prev ? { low: prev.low, high: prev.high, releaseDate: prev.releaseDate } : null,
        to: { low: cur.low, high: cur.high },
        accessionNumber: cur.accessionNumber,
      });
    });
  }
  for (const e of entries) {
    if (e.event !== "WITHDRAWN") continue;
    out.push({ metric: "any", targetPeriod: (e.targetPeriod as Rec).label, releaseDate: e.releaseDate, change: "WITHDRAWN", from: null, to: null, accessionNumber: e.accessionNumber });
  }
  return out.sort((a, b) => cmp(a.releaseDate, b.releaseDate) || cmp(a.metric, b.metric));
}

function days(start: string, end: string): number {
  return Math.round((Date.parse(`${end.slice(0, 10)}T00:00:00Z`) - Date.parse(`${start.slice(0, 10)}T00:00:00Z`)) / 86_400_000);
}

/**
 * Reported actuals by period label from companyfacts: FY<fiscal year> for
 * ~1-year durations, the fiscal year the annual report states (companyfacts fy
 * of the 10-K whose own year it is) else the fiscal year of the period end
 * (fiscal-calendar.ts). Quarters: calendar quarters when every annual period
 * ends in December (a 52/53-week year ending in the first week of January
 * counts); otherwise fiscal quarters of a year whose annual report states its
 * fiscal year, counted back from that year's end. The newest filing of each
 * period wins.
 */
export function actualsFromCompanyFacts(companyfacts: unknown): Rec {
  const usgaap = (((companyfacts ?? {}) as Rec).facts as Rec | undefined)?.["us-gaap"] as Rec | undefined;
  const raw = (concepts: string[], unit: string): Rec[] => concepts.flatMap((concept) => {
    const units = ((usgaap?.[concept] as Rec | undefined)?.units ?? {}) as Record<string, Rec[]>;
    return (units[unit] ?? [])
      .filter((f) => typeof f.start === "string" && typeof f.end === "string" && typeof f.val === "number" && /^10-[KQ]/.test(String(f.form ?? "")))
      .map((f) => ({ ...f, concept }));
  });
  const collect = (facts: Rec[]) => {
    const byPeriod = new Map<string, Rec>();
    for (const f of facts) {
      const key = `${f.start}|${f.end}`;
      const prev = byPeriod.get(key);
      if (!prev || String(f.filed ?? "") > String(prev.filed ?? "")) byPeriod.set(key, f);
    }
    return [...byPeriod.values()];
  };
  const rawRevenue = raw([...REVENUE_CONCEPTS], "USD");
  const rawEps = raw(["EarningsPerShareDiluted"], "USD/shares");
  const revenue = collect(rawRevenue);
  const eps = collect(rawEps);
  const isAnnual = (f: Rec) => { const d = days(String(f.start), String(f.end)); return d >= 350 && d <= 380; };
  // The fiscal year an annual report states for its own year: the fy of the 10-K's latest annual period (2.5.11).
  const statedFy = new Map<string, number>();
  const newestByAccession = new Map<string, Rec>();
  for (const f of [...rawRevenue, ...rawEps]) {
    if (!isAnnual(f) || !/^10-K/.test(String(f.form ?? "")) || f.fp !== "FY" || typeof f.fy !== "number" || typeof f.accn !== "string") continue;
    const prev = newestByAccession.get(f.accn);
    if (!prev || String(f.end) > String(prev.end)) newestByAccession.set(f.accn, f);
  }
  for (const f of [...newestByAccession.values()].sort((a, b) => cmp(a.filed, b.filed))) statedFy.set(String(f.end), f.fy as number);
  const annualEnds = [...new Set([...revenue, ...eps].filter(isAnnual).map((f) => String(f.end)))].sort();
  const calendarFy = annualEnds.length > 0 && annualEnds.every((e) => (nominalPeriodEnd(e) ?? "").slice(5, 7) === "12");
  const fiscalQuarterMapping = calendarFy ? "CALENDAR" : statedFy.size > 0 ? "FILING_STATED_FISCAL_YEAR" : "NONE";
  const fiscalYear = (end: string) => statedFy.get(end) ?? fiscalYearOfPeriodEnd(end);
  // The fiscal year a non-calendar quarter falls in: the first annual end on or after it, else (the year in
  // progress) a year after the latest, only when an annual report states that year's number.
  const fiscalQuarter = (end: string): string | null => {
    const yearEnd = annualEnds.find((a) => days(end, a) >= -7 && days(end, a) <= 280);
    if (yearEnd) {
      const fy = statedFy.get(yearEnd);
      const q = fiscalQuarterOf(end, yearEnd);
      return fy != null && q != null ? `Q${q} ${fy}` : null;
    }
    const latest = annualEnds[annualEnds.length - 1];
    const fy = latest ? statedFy.get(latest) : undefined;
    if (!latest || fy == null || end <= latest) return null;
    const projected = new Date(Date.parse(`${latest}T00:00:00Z`) + 364 * 86_400_000).toISOString().slice(0, 10);
    const q = fiscalQuarterOf(end, projected);
    return q != null ? `Q${q} ${fy + 1}` : null;
  };
  const label = (f: Rec): string | null => {
    const d = days(String(f.start), String(f.end));
    const end = String(f.end);
    const nominal = nominalPeriodEnd(end) ?? end;
    if (d >= 350 && d <= 380) return `FY${fiscalYear(end)}`;
    if (d >= 80 && d <= 100) {
      if (calendarFy) return `Q${Math.ceil(Number(nominal.slice(5, 7)) / 3)} ${nominal.slice(0, 4)}`;
      return fiscalQuarterMapping === "FILING_STATED_FISCAL_YEAR" ? fiscalQuarter(end) : null;
    }
    // A first half is filed as the six-month year-to-date period; a second half is never filed as a period.
    if (d >= 170 && d <= 190 && calendarFy && nominal.slice(5, 7) === "06") return `H1 ${nominal.slice(0, 4)}`;
    return null;
  };
  const table = (facts: Rec[]) => {
    const out: Rec = {};
    for (const f of facts) {
      const l = label(f);
      if (l) out[l] = { value: f.val, concept: f.concept, periodStart: f.start, periodEnd: f.end, form: f.form ?? null, filed: f.filed ?? null, accessionNumber: f.accn ?? null };
    }
    return out;
  };
  // An unread companyfacts is not an unreported actual.
  return {
    read: companyfacts != null,
    revenue: table(revenue),
    eps: table(eps),
    calendarFiscalYear: calendarFy,
    fiscalQuarterMapping,
    statedFiscalYears: Object.fromEntries([...statedFy.entries()].sort()),
  };
}

/** First and last guidance for each metric and target period against the reported actual. */
export function guidanceOutcomes(entries: Rec[], actuals: Rec): Rec[] {
  const out: Rec[] = [];
  const groups = new Map<string, Rec[]>();
  for (const e of entries) {
    const tp = e.targetPeriod as Rec;
    if (e.event || tp.label == null || tp.fiscalYear == null) continue;
    const key = `${e.metric}|${tp.label}`;
    groups.set(key, [...(groups.get(key) ?? []), e]);
  }
  for (const [key, list] of groups) {
    const [metric, label] = key.split("|");
    const sorted = [...list].sort((a, b) => cmp(a.releaseDate, b.releaseDate));
    const first = sorted[0];
    const last = sorted[sorted.length - 1];
    const table = (actuals[metric] ?? null) as Rec | null;
    const actual = table ? (table[label] as Rec | undefined) ?? null : null;
    const position = (g: Rec) => {
      if (!actual || g.basis === "NON_GAAP") return null;
      const v = Number(actual.value);
      return v < Number(g.low) ? "BELOW" : v > Number(g.high) ? "ABOVE" : "WITHIN";
    };
    // Reported actuals are GAAP: a non-GAAP range is never scored against them (2.5.9).
    const nonGaap = sorted.some((g) => g.basis === "NON_GAAP");
    let status = "EVALUATED";
    if (actuals.read === false && metric !== "grossMargin") status = "ACTUALS_NOT_READ";
    else if (!table) status = "NOT_EVALUATED_METRIC";
    else if (nonGaap) status = "NOT_EVALUATED_NON_GAAP_BASIS";
    else if (!actual) {
      if (/^H/.test(label) && (label.startsWith("H2") || actuals.calendarFiscalYear !== true)) status = "NOT_EVALUATED_HALF_YEAR";
      else if (/^Q/.test(label) && (actuals.fiscalQuarterMapping ?? "NONE") === "NONE") status = "NOT_EVALUATED_FISCAL_QUARTER_MAPPING";
      else status = "ACTUAL_NOT_YET_REPORTED";
    }
    out.push({
      metric,
      targetPeriod: label,
      status,
      actual,
      initialGuidance: { low: first.low, high: first.high, releaseDate: first.releaseDate },
      lastGuidance: { low: last.low, high: last.high, releaseDate: last.releaseDate },
      basis: last.basis ?? "NOT_STATED",
      positionVsInitial: position(first),
      positionVsLast: position(last),
    });
  }
  return out.sort((a, b) => cmp(a.targetPeriod, b.targetPeriod) || cmp(a.metric, b.metric));
}

export function guidanceHistory(ticker: string, releases: ReleaseText[], companyfacts: unknown): Rec {
  const entries = releases.flatMap(guidanceEntries);
  const actuals = actualsFromCompanyFacts(companyfacts);
  return {
    ticker: ticker.toUpperCase(),
    basis: "COMPANY_DISCLOSED",
    releases: releases.map((r) => ({
      filingDate: r.filingDate,
      accessionNumber: r.accessionNumber,
      url: r.url,
      status: r.status,
      guidanceFound: guidanceEntries(r).filter((e) => !e.event).length,
    })),
    guidance: entries.filter((e) => !e.event),
    withdrawals: entries.filter((e) => e.event === "WITHDRAWN"),
    revisions: guidanceRevisions(entries),
    outcomes: guidanceOutcomes(entries, actuals),
    actualsBasis: {
      revenue: "SEC XBRL revenue concepts, newest filing per period",
      eps: "us-gaap:EarningsPerShareDiluted, newest filing per period",
      grossMargin: "not evaluated",
      periodLabels: "FY<the fiscal year the annual report states, else the year of the period end less a week (a 52/53-week year ending in early January is the prior year's)>; calendar quarters and first halves for calendar fiscal years; fiscal quarters counted back from a year end whose fiscal year an annual report states; second halves are not filed as a period and are not derived",
      calendarFiscalYear: actuals.calendarFiscalYear,
      fiscalQuarterMapping: actuals.fiscalQuarterMapping,
      read: actuals.read,
    },
    notes: [
      "Guidance is read from earnings-release exhibits as written; a range whose target period the release does not state is listed but not compared.",
      "Revisions compare consecutive releases for the same metric and target period by midpoint, then width.",
      "Outcomes compare the reported actual with the first and last guidance; nothing is estimated.",
      "guidanceFound counts ranges stated in text; guidance given only in a table is not read, and a release with none found is not a withdrawal.",
    ],
    ...AUTHORITY_BOUNDARY,
  };
}
