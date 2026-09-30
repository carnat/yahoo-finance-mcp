/**
 * Metric source reconciliation (2.5.4), shared with
 * yfmcp/metric_reconciliation.py (parity in
 * scripts/test_valuation_history_and_reconcile.py). Pure.
 *
 * One metric for one period, as each source states it:
 * - SEC XBRL as first filed and as most recently filed for that period (a
 *   difference is a restatement, reported as such);
 * - the issuer's earnings release, read only from sentences that name the
 *   metric, a dollar amount and the period's scope (quarter or full year);
 * - Yahoo's quarterly or annual statement row for the same period end.
 * Each found value is compared with the latest SEC value (the baseline) with
 * its difference and percentage; a release figure's stated precision widens
 * the tolerance ("$31.5 million" is +/- $50,000). Nothing is averaged or
 * chosen: the comparison is the output.
 */

import { AUTHORITY_BOUNDARY } from "./evidence.js";
import { collapse, round } from "./capital-structure.js";
import { REVENUE_CONCEPTS } from "./sec-facts.js";

type Rec = Record<string, unknown>;

export interface MetricSpec {
  kind: "duration" | "instant";
  unit: "money" | "per_share";
  gaap: string[];
  ifrs: string[];
  yahoo: string[];
  releaseLabel: string;
}

export const METRICS: Record<string, MetricSpec> = {
  revenue: { kind: "duration", unit: "money", gaap: [...REVENUE_CONCEPTS], ifrs: ["Revenue", "RevenueFromContractsWithCustomers"], yahoo: ["totalRevenue"], releaseLabel: "\\b(?:net sales|revenues?)\\b" },
  net_income: { kind: "duration", unit: "money", gaap: ["NetIncomeLoss", "ProfitLoss"], ifrs: ["ProfitLossAttributableToOwnersOfParent", "ProfitLoss"], yahoo: ["netIncome", "netIncomeCommonStockholders"], releaseLabel: "\\bnet (?:income|loss)\\b" },
  operating_income: { kind: "duration", unit: "money", gaap: ["OperatingIncomeLoss"], ifrs: ["ProfitLossFromOperatingActivities"], yahoo: ["operatingIncome"], releaseLabel: "\\b(?:operating (?:income|loss)|(?:income|loss) from operations)\\b" },
  eps_diluted: { kind: "duration", unit: "per_share", gaap: ["EarningsPerShareDiluted"], ifrs: ["DilutedEarningsLossPerShare"], yahoo: ["dilutedEPS"], releaseLabel: "\\b(?:diluted (?:earnings|loss|net loss|net income) per share|diluted eps|(?:earnings|loss|net loss|net income) per diluted share)\\b" },
  // "cash, cash equivalents, and restricted cash" (ASTS) is a different total.
  cash_and_equivalents: { kind: "instant", unit: "money", gaap: ["CashAndCashEquivalentsAtCarryingValue"], ifrs: ["CashAndCashEquivalents"], yahoo: ["cashAndCashEquivalents"], releaseLabel: "\\bcash,? (?:and )?cash equivalents\\b(?!,? (?:and )?(?:restricted cash|short-term investments|marketable securities|investments))" },
};

export const DEFAULT_TOLERANCE_PCT = 0.5;
const PERIODIC_FORM_RE = /^(?:10-K|10-Q|20-F|40-F)/;

function days(start: string, end: string): number {
  return Math.round((Date.parse(`${end.slice(0, 10)}T00:00:00Z`) - Date.parse(`${start.slice(0, 10)}T00:00:00Z`)) / 86_400_000);
}

function cmp(a: unknown, b: unknown): number {
  const x = String(a ?? "");
  const y = String(b ?? "");
  return x < y ? -1 : x > y ? 1 : 0;
}

interface Row { concept: string; start: string | null; end: string; val: number; filed: string; form: string; accn: string | null; fy: number | null; fp: string | null }

/** The taxonomy, reporting unit and facts for a metric (all periodic filings, every filing kept). */
export function metricFacts(companyfacts: unknown, metric: string): { taxonomy: string | null; unit: string | null; rows: Row[] } {
  const spec = METRICS[metric];
  const facts = ((companyfacts ?? {}) as Rec).facts as Rec | undefined;
  for (const [taxonomy, concepts] of [["us-gaap", spec.gaap], ["ifrs-full", spec.ifrs]] as const) {
    const tax = (facts?.[taxonomy] ?? null) as Rec | null;
    if (!tax) continue;
    const byUnit = new Map<string, Row[]>();
    for (const concept of concepts) {
      const units = (((tax[concept] as Rec | undefined)?.units ?? {}) as Record<string, Rec[]>);
      for (const [unit, list] of Object.entries(units)) {
        const ok = spec.unit === "per_share" ? /^[A-Z]{3}\/shares$/.test(unit) : /^[A-Z]{3}$/.test(unit);
        if (!ok) continue;
        for (const f of list) {
          if (typeof f.end !== "string" || typeof f.val !== "number" || typeof f.filed !== "string" || !PERIODIC_FORM_RE.test(String(f.form ?? ""))) continue;
          const start = typeof f.start === "string" ? f.start : null;
          if ((spec.kind === "instant") !== (start == null)) continue;
          const rows = byUnit.get(unit) ?? [];
          rows.push({ concept, start, end: f.end, val: f.val, filed: f.filed, form: String(f.form), accn: typeof f.accn === "string" ? f.accn : null, fy: typeof f.fy === "number" ? f.fy : null, fp: typeof f.fp === "string" ? f.fp : null });
          byUnit.set(unit, rows);
        }
      }
    }
    if (byUnit.size === 0) continue;
    // The unit the issuer reports in now: the one filed most recently (NBIS reported in RUB as Yandex to
    // FY2023 and in USD from FY2024; the longer RUB history is not its reporting unit), then the most facts (2.5.15).
    const newest = (rows: Row[]) => rows.reduce((m, r) => (r.filed > m ? r.filed : m), "");
    const [unit, rows] = [...byUnit.entries()].sort((a, b) => cmp(newest(b[1]), newest(a[1])) || b[1].length - a[1].length || cmp(a[0], b[0]))[0];
    return { taxonomy, unit, rows };
  }
  return { taxonomy: null, unit: null, rows: [] };
}

const isQuarter = (r: { start: string | null; end: string }) => r.start != null && days(r.start, r.end) >= 80 && days(r.start, r.end) <= 100;
const isAnnual = (r: { start: string | null; end: string }) => r.start != null && days(r.start, r.end) >= 350 && days(r.start, r.end) <= 380;

/** The row that first reported a period: its fy/fp are the issuer's names for that period. */
function firstReported(rows: Row[], r: Row): Row {
  return rows
    .filter((x) => x.end === r.end && x.start === r.start)
    .sort((a, b) => cmp(a.filed, b.filed) || cmp(a.accn, b.accn))[0] ?? r;
}

/**
 * An annual period's fiscal year: the issuer's (companyfacts fy with fp FY, from the filing that first
 * reported it, within a year of the end), else the year it ends in. Dollar General's year ended
 * 2026-01-30 is fiscal 2025; NVDA's ended 2026-01-25 is fiscal 2026.
 */
function annualFiscalYear(rows: Row[], r: Row): { year: number; source: string; first: Row } {
  const first = firstReported(rows, r);
  const endYear = Number(r.end.slice(0, 4));
  return first.fy != null && first.fp === "FY" && (first.fy === endYear || first.fy === endYear - 1)
    ? { year: first.fy, source: "SEC_FY_FP", first }
    : { year: endYear, source: "PERIOD_END_YEAR", first };
}

const LABEL_BASIS = "FY<yyyy> is the issuer's fiscal year (SEC fy/fp) when companyfacts gives it, else the fiscal year ending in that year; Q<n> <yyyy> is the calendar quarter of the period end (fiscalQuarter gives the issuer's)";

/**
 * The period a spec names: latest_quarter, latest_annual, FY<year> (the
 * issuer's fiscal year, else the fiscal year ending in that year) or Q<n>
 * <year> (the quarter ending in that calendar quarter). Instants take the end
 * of the matching revenue period.
 */
export function resolvePeriod(companyfacts: unknown, metric: string, spec: string): Rec {
  // The company's reporting periods come from its revenue (ASTS stopped tagging undimensioned EPS in 2022,
  // so its own latest EPS quarter is not the latest quarter); without revenue, the metric's own or net income's.
  const revenue = metricFacts(companyfacts, "revenue").rows;
  const rows = revenue.length > 0 ? revenue : metricFacts(companyfacts, METRICS[metric].kind === "duration" ? metric : "net_income").rows;
  const text = spec.trim();
  let candidates: Row[] = [];
  let periodType: string;
  let m: RegExpExecArray | null;
  if (/^latest_quarter$/i.test(text)) {
    periodType = "QUARTER";
    candidates = rows.filter(isQuarter);
  } else if (/^latest_annual$/i.test(text)) {
    periodType = "ANNUAL";
    candidates = rows.filter(isAnnual);
  } else if ((m = /^FY\s?(\d{4})$/i.exec(text))) {
    periodType = "ANNUAL";
    // By the issuer's fiscal year where companyfacts names it; a period the issuer names is never chosen
    // for a different year because of the calendar year it ends in.
    const year = Number(m[1]);
    const named = rows.filter(isAnnual).map((r) => ({ r, id: annualFiscalYear(rows, r) })).filter((x) => x.id.year === year);
    const bySec = named.filter((x) => x.id.source === "SEC_FY_FP");
    candidates = (bySec.length > 0 ? bySec : named).map((x) => x.r);
  } else if ((m = /^Q([1-4])\s?(\d{4})$/i.exec(text))) {
    periodType = "QUARTER";
    const q = Number(m[1]);
    const year = m[2];
    candidates = rows.filter((r) => isQuarter(r) && r.end.slice(0, 4) === year && Math.ceil(Number(r.end.slice(5, 7)) / 3) === q);
  } else {
    return { status: "INVALID_PERIOD", message: "period must be latest_quarter, latest_annual, FY<yyyy> or Q<n> <yyyy>." };
  }
  if (candidates.length === 0) {
    const fyYears = [...new Set(rows.filter(isAnnual).map((r) => annualFiscalYear(rows, r).year))].sort((a, b) => a - b);
    return /^FY/i.test(text)
      ? { status: "PERIOD_NOT_FOUND", spec: text, periodType, message: `No annual period in companyfacts is fiscal year ${text.replace(/^FY\s?/i, "")} (the issuer's SEC fy, else the year the period ends).`, fiscalYearsAvailable: fyYears.slice(-5), labelBasis: LABEL_BASIS }
      : { status: "PERIOD_NOT_FOUND", spec: text, periodType, message: `No ${periodType.toLowerCase()} period in companyfacts matches ${text} (20-F and 40-F filers tag annual periods only).` };
  }
  const best = [...candidates].sort((a, b) => cmp(a.end, b.end) || cmp(a.start, b.start))[candidates.length - 1];
  const kind = METRICS[metric].kind;
  // The fiscal year and quarter the issuer gives the period: companyfacts' fy/fp of the filing that first
  // reported it (NVDA's quarter ended 2026-07-26 is fy 2027 Q2; Dollar General's ended 2026-07-31 is fy 2026 Q2).
  // Without usable metadata the quarter is counted from the prior fiscal year end and, for a year ending in
  // January to March, either naming convention is accepted and flagged as ambiguous.
  const first = firstReported(rows, best);
  const fy = first?.fy ?? null;
  const fp = first?.fp ?? null;
  const endYear = Number(best.end.slice(0, 4));
  let fiscal: Rec = {};
  if (periodType === "QUARTER") {
    const priorEnds = rows.filter(isAnnual).map((r) => r.end).filter((e) => e < best.end).sort();
    const prior = priorEnds.length > 0 ? priorEnds[priorEnds.length - 1] : null;
    const priorYear = prior ? Number(prior.slice(0, 4)) : null;
    const counted = prior ? Math.min(4, Math.max(1, Math.floor(days(prior, best.end) / 91.25 + 0.5))) : null;
    const fpQuarter = fp && /^Q[1-3]$/.test(fp) ? Number(fp[1]) : fp === "FY" ? 4 : null;
    const fyFits = fy != null && (priorYear == null ? fy === endYear || fy === endYear + 1 || fy === endYear - 1 : fy === priorYear || fy === priorYear + 1);
    if (fy != null && fpQuarter != null && fyFits && (counted == null || counted === fpQuarter)) {
      fiscal = { fiscalQuarter: fpQuarter, fiscalYears: [fy], fiscalYearSource: "SEC_FY_FP", fiscalBasis: `companyfacts fy ${fy} fp ${fp} of the filing that first reported the period (${first.accn})` };
    } else if (prior && counted != null && priorYear != null) {
      const fiscalYears = Number(prior.slice(5, 7)) <= 3 ? [priorYear + 1, priorYear] : [priorYear + 1];
      fiscal = {
        fiscalQuarter: counted,
        fiscalYears,
        fiscalYearSource: "DERIVED_FROM_PRIOR_YEAR_END",
        fiscalBasis: `counted from the prior fiscal year end ${prior}`,
        ...(fiscalYears.length > 1 ? { fiscalYearAmbiguous: true } : {}),
        ...(fy != null || fp != null ? { fiscalMetadataNotUsed: { fy, fp } } : {}),
      };
    }
  } else {
    const id = annualFiscalYear(rows, best);
    fiscal = id.source === "SEC_FY_FP"
      ? { fiscalYears: [id.year], fiscalYearSource: "SEC_FY_FP", fiscalBasis: `companyfacts fy ${fy} fp ${fp} of the filing that first reported the period (${first.accn})` }
      : { fiscalYears: [id.year], fiscalYearSource: "PERIOD_END_YEAR", fiscalBasis: "the calendar year the fiscal year ends in" };
  }
  return {
    status: "OK",
    spec: text,
    periodType,
    periodStart: kind === "instant" ? null : best.start,
    periodEnd: best.end,
    ...fiscal,
    labelBasis: LABEL_BASIS,
  };
}

/** NetIncomeLossAttributableToNoncontrollingInterest for a ProfitLoss row's period, in the same filing; null when not tagged. */
function nciShare(companyfacts: unknown, unit: string | null, r: Row): number | null {
  const list = ((((((companyfacts ?? {}) as Rec).facts as Rec | undefined)?.["us-gaap"] as Rec | undefined)?.NetIncomeLossAttributableToNoncontrollingInterest as Rec | undefined)?.units as Record<string, Rec[]> | undefined)?.[unit ?? ""];
  if (!Array.isArray(list)) return null;
  const hit = list.find((f) => f.accn === r.accn && f.end === r.end && f.start === r.start && typeof f.val === "number");
  return hit ? (hit.val as number) : null;
}

function rowRef(r: Row): Rec {
  return { concept: r.concept, value: r.val, form: r.form, filed: r.filed, accessionNumber: r.accn };
}

/** SEC values for the period: as first filed and as most recently filed, with every distinct value filed. */
export function secObservations(companyfacts: unknown, metric: string, period: Rec): Rec[] {
  const { taxonomy, unit, rows } = metricFacts(companyfacts, metric);
  const spec = METRICS[metric];
  const all = rows.filter((r) => r.end === period.periodEnd && (spec.kind === "instant" || r.start === period.periodStart));
  if (all.length === 0) {
    return [{ source: "SEC_XBRL_LATEST", provider: "SEC", status: "NOT_FOUND", value: null, taxonomy, unit }];
  }
  // One concept: the one filed most recently for the period (the earlier-listed on a tie), so NetIncomeLoss
  // is not compared with ProfitLoss (which includes noncontrolling interests) from the same filing.
  const concepts = taxonomy === "ifrs-full" ? spec.ifrs : spec.gaap;
  const newestFiled = (c: string) => all.filter((r) => r.concept === c).reduce((m, r) => (r.filed > m ? r.filed : m), "");
  // Revenue tagged under two concepts in the same filing is its total under the larger: contract revenue is a
  // part of Revenues (BE FY2025: 2,001,614,000 of 2,023,994,000) (2.5.15).
  const newestMagnitude = (c: string) => Math.max(...all.filter((r) => r.concept === c && r.filed === newestFiled(c)).map((r) => Math.abs(r.val)));
  const concept = concepts.filter((c) => newestFiled(c) !== "").reduce((best, c) =>
    newestFiled(c) > newestFiled(best) || (metric === "revenue" && newestFiled(c) === newestFiled(best) && newestMagnitude(c) > newestMagnitude(best)) ? c : best);
  const matching = all.filter((r) => r.concept === concept).sort((a, b) => cmp(a.filed, b.filed) || cmp(a.accn, b.accn));
  const first = matching[0];
  const last = matching[matching.length - 1];
  // Net income is the parent's: a filer that tags only ProfitLoss (BE) includes noncontrolling interests, so
  // the NCI share tagged for the same period in the same filing is taken off and the derivation shown (2.5.15).
  if (metric === "net_income" && taxonomy === "us-gaap" && concept === "ProfitLoss") {
    const nci = (r: Row) => nciShare(companyfacts, unit, r);
    const lastNci = nci(last);
    const firstNci = nci(first);
    if (lastNci != null && firstNci != null) {
      const derived = (r: Row, share: number) => ({ ...rowRef(r), value: r.val - share, profitLoss: r.val, noncontrollingInterest: share, derivation: "ProfitLoss - NetIncomeLossAttributableToNoncontrollingInterest" });
      return [
        { source: "SEC_XBRL_LATEST", provider: "SEC", status: "FOUND", value: last.val - lastNci, unit, taxonomy, precision: 0, basis: "PARENT_DERIVED_FROM_PROFITLOSS", evidence: derived(last, lastNci), filedValues: [...new Set(matching.map((r) => r.val))], otherConcepts: [] },
        { source: "SEC_XBRL_AS_FIRST_FILED", provider: "SEC", status: "FOUND", value: first.val - firstNci, unit, taxonomy, precision: 0, basis: "PARENT_DERIVED_FROM_PROFITLOSS", evidence: derived(first, firstNci) },
      ];
    }
  }
  const distinct = [...new Set(matching.map((r) => r.val))];
  const otherConcepts = concepts
    .filter((c) => c !== concept && newestFiled(c) !== "")
    .map((c) => {
      const rowsFor = all.filter((r) => r.concept === c).sort((a, b) => cmp(a.filed, b.filed) || cmp(a.accn, b.accn));
      return rowRef(rowsFor[rowsFor.length - 1]);
    });
  return [
    { source: "SEC_XBRL_LATEST", provider: "SEC", status: "FOUND", value: last.val, unit, taxonomy, precision: 0, evidence: rowRef(last), filedValues: distinct, otherConcepts },
    { source: "SEC_XBRL_AS_FIRST_FILED", provider: "SEC", status: "FOUND", value: first.val, unit, taxonomy, precision: 0, evidence: rowRef(first) },
  ];
}

// Sentences and bullets, including the " o " bullets SEC-rendered releases carry.
const PIECE_SPLIT_RE = /(?<=[.!?])\s+|\s+[•●▪◦·]\s+|\s+o\s+(?=[A-Z])/;
const GUIDANCE_RE = /\b(?:guidance|outlook|expect(?:s|ed|ation|ations)?|forecast|project(?:s|ed|ion|ions)?|target|range)\b/i;
const NON_RESULT_RE = /\b(?:awards?|awarded|contract value|aggregate value|backlog|bookings|orders?|pipeline|contracted)\b/i;
const QUARTER_SCOPE_RE = /\b(?:quarter(?:ly)?|three months|Q[1-4])\b/i;
const ANNUAL_SCOPE_RE = /\b(?:full[- ]year|fiscal (?:year )?20\d\d|years? ended|twelve months|for (?:the )?(?:fiscal )?year|annual)\b/i;
const INSTANT_SCOPE_RE = /\b(?:as of|ended (?:the )?(?:quarter|year|period)|at (?:the )?(?:end|close) of|balance)\b/i;
// Scale words and their abbreviations ("$1.81B", "$808.4M", "$2.5 bn") (2.5.15: COHR's "$1.81B" read as 1.81).
const MONEY_RE = /(\(?)\s?(-?)\s?\$\s?(\(?)(-?)([0-9][0-9,]*)(\.[0-9]+)?\)?(?:\s?(billion|million|thousand|bn|mn|mm|[bmk])\b)?/i;
const QUARTER_ORDINAL = ["first", "second", "third", "fourth"];
// "quarter of fiscal 2027", "Q2 fiscal 2027", "fiscal 2027 second quarter", "fiscal year 2027 Q2": a fiscal year
// naming the quarter it qualifies (the quarter word is kept, the year dropped), not a full-year scope.
const FISCAL_YEAR_OF_QUARTER_RE = /\b(?:(quarter(?:ly)?|Q[1-4])(?: of)?(?: the)? fiscal (?:year )?20\d\d|fiscal (?:year )?20\d\d(?=,? (?:(?:first|second|third|fourth)[- ](?:fiscal[- ])?quarter|Q[1-4])\b))\b/gi;

const QUARTER_MENTION_SOURCE = "\\b(?:Q([1-4])|(first|second|third|fourth)[- ](?:fiscal[- ])?quarter)\\b";

/**
 * Fiscal shorthand spelled out so one set of patterns reads it: "FY27" and "FY2027" -> "fiscal 2027",
 * "Q2FY27" -> "Q2 fiscal 2027", "Q2'27" -> "Q2 2027", "2Q26" -> "Q2 2026". Dollar amounts are untouched.
 */
export function normalizeFiscalTokens(text: string): string {
  return text
    .replace(/\b(Q[1-4])\s?FY\s?'?(\d{2})\b/gi, "$1 fiscal 20$2")
    .replace(/\b(Q[1-4])\s?FY\s?(20\d{2})\b/gi, "$1 fiscal $2")
    .replace(/\bFY\s?'?(\d{2})\b/gi, "fiscal 20$1")
    .replace(/\bFY\s?(20\d{2})\b/gi, "fiscal $1")
    .replace(/\b(Q[1-4])\s?'(\d{2})\b/gi, "$1 20$2")
    .replace(/\b([1-4])Q'?(\d{2})\b/gi, "Q$1 20$2")
    .replace(/\b([1-4])Q\b/gi, "Q$1");
}
const MONTH_NAMES = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
const COMPARATOR_RE = /\b(?:compared (?:with|to)|versus|vs\.?|from)\b/i;

/** "2026-07-26" -> "July 26, 2026". */
function periodEndPhrase(end: string): string {
  return `${MONTH_NAMES[Number(end.slice(5, 7)) - 1]} ${Number(end.slice(8, 10))}, ${end.slice(0, 4)}`;
}

/**
 * Whether the quarters and years a sentence names match the period. A quarter
 * may be named by its calendar or its fiscal number ("second quarter of fiscal
 * 2027" is NVDA's quarter ended July 26, 2026, calendar Q3); a sentence naming
 * the period's exact end date is scoped to it.
 */
function explicitPeriodMatches(sentence: string, amountStart: number, period: Rec): boolean {
  const end = String(period.periodEnd ?? "");
  if (end.length === 10 && sentence.includes(periodEndPhrase(end))) return true;
  const beforeAmount = sentence.slice(0, amountStart);
  if (period.periodType === "QUARTER") {
    // A quarter and year name the period as a pair: its calendar quarter in the calendar year, or its fiscal
    // quarter in its fiscal year. NVDA's quarter ended 2026-07-26 is Q3 2026 or Q2 fiscal 2027, never
    // "second quarter fiscal 2026" (the prior year's quarter).
    const pairs: [number, string][] = [[Math.ceil(Number(end.slice(5, 7)) / 3), end.slice(0, 4)]];
    if (typeof period.fiscalQuarter === "number") {
      for (const y of (period.fiscalYears ?? []) as unknown[]) pairs.push([period.fiscalQuarter, String(y)]);
    }
    const quarterOf = (m: RegExpMatchArray) => (m[1] ? Number(m[1]) : QUARTER_ORDINAL.indexOf(m[2].toLowerCase()) + 1);
    // The year written just before the quarter ("fiscal 2026 second quarter") or after it ("Q2 fiscal 2026").
    const matches = (text: string, m: RegExpMatchArray) => {
      const q = quarterOf(m);
      const at = m.index ?? 0;
      const before = /\b(20\d{2}),?\s+$/.exec(text.slice(Math.max(0, at - 24), at));
      const after = /\b(20\d{2})\b/.exec(text.slice(at, Math.min(text.length, at + 60)));
      const named = [before?.[1], after?.[1]].filter((y): y is string => y != null);
      return pairs.some(([pq, py]) => pq === q && named.every((y) => y === py));
    };
    const quarterMentions = [...beforeAmount.matchAll(new RegExp(QUARTER_MENTION_SOURCE, "gi"))];
    for (const m of quarterMentions) {
      if (!matches(beforeAmount, m)) return false;
    }
    // If the only explicit period is after the amount in a comparison clause,
    // it describes the comparator rather than the current-period amount.
    if (quarterMentions.length === 0) {
      const after = sentence.slice(amountStart);
      const firstPeriod = new RegExp(QUARTER_MENTION_SOURCE, "i").exec(after);
      if (firstPeriod && !COMPARATOR_RE.test(after.slice(0, firstPeriod.index)) && !matches(after, firstPeriod)) return false;
    }
    return true;
  }
  if (period.periodType === "ANNUAL") {
    // The issuer's name for the year (Dollar General's year ended 2026-01-30 is fiscal 2025), else the end year.
    const years = new Set<string>(((period.fiscalYears ?? [end.slice(0, 4)]) as unknown[]).map(String));
    const beforeYears = [...beforeAmount.matchAll(/\b(20\d{2})\b/g)].map((m) => m[1]);
    if (beforeYears.some((y) => !years.has(y))) return false;
    const after = sentence.slice(amountStart);
    const laterYear = /\b(20\d{2})\b/.exec(after);
    if (beforeYears.length === 0 && laterYear) {
      const prefix = after.slice(0, laterYear.index);
      if (!COMPARATOR_RE.test(prefix) && !years.has(laterYear[1])) return false;
    }
    return true;
  }
  return true;
}
const SCALE: Record<string, number> = { billion: 1e9, million: 1e6, thousand: 1e3, bn: 1e9, b: 1e9, mn: 1e6, mm: 1e6, m: 1e6, k: 1e3 };
// A figure that is a per-share amount, or a non-GAAP/adjusted one, is never a GAAP total (2.5.15: "GAAP net
// income of $0.97 per diluted share" read as COHR's net income; SNDK's "Non-GAAP diluted net income per share").
const PER_SHARE_RE = /\bper\s+(?:diluted\s+|basic\s+)?(?:common\s+)?(?:share|ADS|ADR)\b|\bEPS\b/i;
const NON_GAAP_RE = /\bnon-?\s?GAAP\b|\badjusted\b/i;
// "(in thousands, except per share data)": the scale of a release's statement tables.
const TABLE_SCALE_RE = /\(\s*(?:\$|US\$|dollars|amounts)?\s*in\s+(thousands|millions|billions)\b/gi;

/** A release sentence's figure for the metric and period, or why none was read. */
export function releaseObservation(release: Rec | null, metric: string, period: Rec, reportingUnit: string | null): Rec {
  const base = { source: "ISSUER_RELEASE", provider: "ISSUER_RELEASE", url: release?.url ?? null, filingDate: release?.filingDate ?? null, accessionNumber: release?.accessionNumber ?? null };
  if (!release || release.status === "NOT_RESOLVED") return { ...base, status: "NOT_RESOLVED", value: null };
  if (release.status !== "READ" || typeof release.text !== "string") return { ...base, status: "NOT_READ", value: null };
  if (reportingUnit && !/^USD/.test(reportingUnit)) return { ...base, status: "NOT_COMPARED_CURRENCY", value: null, reportingUnit };
  const spec = METRICS[metric];
  const label = new RegExp(spec.releaseLabel, "i");
  const anchored = new RegExp(`(${spec.releaseLabel})[^$]{0,80}?${MONEY_RE.source}`, "gi");
  // An unscaled table figure takes the release's table scale only when the release declares exactly one;
  // with none it is read as written, with several it is not read (2.5.15: FN's "$ 1,214,293" in thousands).
  const tableScales = [...new Set([...collapse(release.text).matchAll(TABLE_SCALE_RE)].map((x) => x[1].toLowerCase().replace(/s$/, "")))];
  let unscoped = 0;
  let rejected = 0;
  for (const raw of collapse(release.text).split(PIECE_SPLIT_RE)) {
    const sentence = raw.trim();
    if (!sentence || sentence.length > 600 || !label.test(sentence)) continue;
    if (GUIDANCE_RE.test(sentence) || NON_RESULT_RE.test(sentence)) continue;
    // Scope is read on the sentence with fiscal shorthand spelled out; the evidence is the sentence as written.
    const text = normalizeFiscalTokens(sentence);
    // Every label-and-amount pair in the sentence, not only the first: "Non-GAAP net income was $X; GAAP net
    // income was $Y" holds a GAAP figure after a rejected one.
    anchored.lastIndex = 0;
    let m: RegExpExecArray | null = null;
    let scale = 1;
    let scaleBasis: string | null = null;
    for (let c = anchored.exec(text); c; c = anchored.exec(text)) {
      const amountEnd = c.index + c[0].length;
      const lead = text.slice(Math.max(0, c.index - 40), c.index + c[0].indexOf("$"));
      const tail = text.slice(amountEnd, amountEnd + 30);
      if (NON_GAAP_RE.test(lead)) { rejected += 1; continue; }
      if (spec.unit === "money" && (PER_SHARE_RE.test(c[0].slice(c[1].length)) || /^\s*(?:per|a)\s+(?:diluted\s+|basic\s+)?(?:common\s+)?(?:share|ADS|ADR)\b/i.test(tail))) { rejected += 1; continue; }
      const word = c[8];
      if (word) {
        scale = SCALE[word.toLowerCase()];
        scaleBasis = "AS_WRITTEN";
      } else if (spec.unit === "money" && tableScales.length > 1) {
        rejected += 1;
        continue;
      } else if (spec.unit === "money" && tableScales.length === 1) {
        scale = SCALE[tableScales[0]];
        scaleBasis = `RELEASE_TABLE_IN_${tableScales[0].toUpperCase()}S`;
      } else {
        scale = 1;
        scaleBasis = null;
      }
      m = c;
      break;
    }
    if (!m) continue;
    const quarter = QUARTER_SCOPE_RE.test(text);
    // "second quarter of fiscal 2027" and "fiscal 2026 third quarter" name a quarter, not a year.
    const annual = ANNUAL_SCOPE_RE.test(text.replace(FISCAL_YEAR_OF_QUARTER_RE, "$1"));
    const amountStart = m.index + m[0].indexOf("$");
    const scoped = spec.kind === "instant"
      ? INSTANT_SCOPE_RE.test(text)
      : period.periodType === "QUARTER"
        ? quarter && !annual && explicitPeriodMatches(text, amountStart, period)
        : annual && !quarter && explicitPeriodMatches(text, amountStart, period);
    if (!scoped) {
      unscoped += 1;
      continue;
    }
    const [, labelText, openParen, minus, innerParen, innerMinus, intPart, frac] = m;
    const magnitude = parseFloat(`${intPart.replace(/,/g, "")}${frac ?? ""}`) * scale;
    const negative = openParen === "(" || innerParen === "(" || minus === "-" || innerMinus === "-" || /\bloss\b/i.test(labelText);
    const decimals = frac ? frac.length - 1 : 0;
    const precision = 0.5 * 10 ** -decimals * scale;
    const asWritten = m[0].slice(labelText.length).replace(/^[^$(-]*/, "").trim();
    return { ...base, status: "FOUND", value: negative ? -magnitude : magnitude, precision, asWritten, ...(scaleBasis && scaleBasis !== "AS_WRITTEN" ? { scaleBasis } : {}), sentence: sentence.slice(0, 400) };
  }
  return { ...base, status: "NOT_FOUND_IN_TEXT", value: null, unscopedCandidates: unscoped, ...(rejected ? { rejectedCandidates: rejected } : {}) };
}

/**
 * Of the releases read for the period (oldest first), the first with a scoped
 * figure; else the first read. Every release considered is listed.
 */
export function pickReleaseObservation(observations: Rec[]): Rec {
  const considered = observations.map((o) => ({ filingDate: o.filingDate ?? null, accessionNumber: o.accessionNumber ?? null, status: o.status }));
  const found = observations.filter((o) => o.status === "FOUND");
  const chosen = found.length > 0 ? found[found.length - 1] : observations[0] ?? { source: "ISSUER_RELEASE", provider: "ISSUER_RELEASE", status: "NOT_RESOLVED", value: null };
  return { ...chosen, releasesConsidered: considered };
}

/** Yahoo's statement row for the period end (within 7 days). */
export function yahooObservation(rows: Rec[] | null, metric: string, period: Rec, frequency: string): Rec {
  const base = { source: "YAHOO", provider: "YAHOO", frequency };
  if (rows == null) return { ...base, status: "NOT_READ", value: null };
  const end = String(period.periodEnd);
  const row = rows.find((r) => typeof r.date === "string" && Math.abs(days(r.date as string, end)) <= 7);
  if (!row) return { ...base, status: "NOT_FOUND", value: null, datesAvailable: rows.map((r) => r.date).slice(0, 8) };
  for (const field of METRICS[metric].yahoo) {
    const v = row[field];
    if (typeof v === "number" && Number.isFinite(v)) return { ...base, status: "FOUND", value: v, precision: 0, field, periodEnd: row.date };
  }
  return { ...base, status: "FIELD_EMPTY", value: null, periodEnd: row.date, fields: METRICS[metric].yahoo };
}

/** Each found value against the baseline, and the overall agreement. */
export function reconcileObservations(observations: Rec[], tolerancePct: number): { comparisons: Rec[]; status: string; restated: boolean } {
  const found = observations.filter((o) => o.status === "FOUND" && typeof o.value === "number");
  const baseline = found.find((o) => o.source === "SEC_XBRL_LATEST") ?? null;
  // Without SEC, the other sources are still compared with each other so a disagreement is visible;
  // they can never be AGREED.
  const reference = baseline ?? found[0] ?? null;
  const comparisons: Rec[] = [];
  let conflict = false;
  let restated = false;
  if (reference) {
    const b = reference.value as number;
    for (const o of found) {
      if (o === reference) continue;
      const v = o.value as number;
      const difference = v - b;
      const tolerance = Math.max((Math.abs(b) * tolerancePct) / 100, Number(o.precision ?? 0), Number(reference.precision ?? 0));
      const match = Math.abs(difference) <= tolerance + 1e-9 * Math.max(1, Math.abs(b));
      const row: Rec = {
        source: o.source,
        against: reference.source,
        value: v,
        baselineValue: b,
        difference: round(difference, 6),
        differencePct: b !== 0 ? round((difference / Math.abs(b)) * 100, 4) : null,
        tolerance: round(tolerance, 6),
        result: match ? "MATCH" : "MISMATCH",
      };
      if (o.source === "SEC_XBRL_AS_FIRST_FILED") {
        row.result = match ? "MATCH" : "RESTATED";
        restated = !match;
      } else if (!match) {
        conflict = true;
      }
      comparisons.push(row);
    }
  }
  const providers = new Set(found.map((o) => o.provider));
  const status = found.length === 0 ? "NOT_FOUND"
    : conflict ? "CONFLICT"
    : !baseline ? "PARTIAL"
    : providers.size >= 2 ? "AGREED"
    : "PARTIAL";
  return { comparisons, status, restated };
}

export interface ReconcileInput {
  ticker: string;
  metric: string;
  period: Rec;
  companyfacts: unknown;
  /** Releases read for the period, oldest first; empty when none was filed in the window. */
  releases: Rec[];
  yahooRows: Rec[] | null;
  tolerancePct: number;
}

export function metricReconciliation(input: ReconcileInput): Rec {
  const { taxonomy, unit } = metricFacts(input.companyfacts, input.metric);
  const frequency = input.period.periodType === "ANNUAL" ? "annual" : "quarterly";
  const observations = [
    ...secObservations(input.companyfacts, input.metric, input.period),
    pickReleaseObservation(input.releases.map((r) => releaseObservation(r, input.metric, input.period, unit))),
    yahooObservation(input.yahooRows, input.metric, input.period, frequency),
  ];
  const result = reconcileObservations(observations, input.tolerancePct);
  const found = new Set(observations.filter((o) => o.status === "FOUND").map((o) => o.provider));
  return {
    ticker: input.ticker.toUpperCase(),
    metric: input.metric,
    period: input.period,
    taxonomy,
    unit,
    status: result.status,
    restated: result.restated,
    tolerancePct: input.tolerancePct,
    observations,
    comparisons: result.comparisons,
    missingProviders: ["SEC", "ISSUER_RELEASE", "YAHOO"].filter((p) => !found.has(p)),
    sourcesNotCompared: [
      { source: "SEC_FILING_TABLES", reason: "The filing's statement tables are the inline XBRL values companyfacts carries per filing; both as-first-filed and latest are compared." },
      { source: "ALPHA_VANTAGE", reason: "Not read: its free-tier quota is reserved for consensus estimates." },
      { source: "COMPANIES_HOUSE", reason: "UK statutory filings; not applicable to SEC registrants and not tagged for these metrics." },
    ],
    notes: [
      "AGREED: latest SEC plus at least one independent provider found the value and all agree within tolerance. PARTIAL: SEC is absent or only one provider found it. CONFLICT: a provider differs from the latest SEC value (or, without SEC, from another provider) beyond tolerance. NOT_FOUND: none found it.",
      "A release quarter may be named by its calendar or fiscal number; a sentence naming the period's exact end date is scoped to it.",
      "A difference between the SEC value as first filed and as latest filed is a restatement (restated: true), not a conflict.",
      "The tolerance is the larger of tolerancePct of the SEC value and half the last stated digit of a release figure.",
      "Release figures are read only from sentences scoped to the period (quarter or full year); unscopedCandidates counts sentences skipped for scope.",
    ],
    ...AUTHORITY_BOUNDARY,
  };
}
