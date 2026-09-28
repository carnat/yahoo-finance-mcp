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

interface Row { concept: string; start: string | null; end: string; val: number; filed: string; form: string; accn: string | null }

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
          rows.push({ concept, start, end: f.end, val: f.val, filed: f.filed, form: String(f.form), accn: typeof f.accn === "string" ? f.accn : null });
          byUnit.set(unit, rows);
        }
      }
    }
    if (byUnit.size === 0) continue;
    const [unit, rows] = [...byUnit.entries()].sort((a, b) => b[1].length - a[1].length || cmp(a[0], b[0]))[0];
    return { taxonomy, unit, rows };
  }
  return { taxonomy: null, unit: null, rows: [] };
}

const isQuarter = (r: { start: string | null; end: string }) => r.start != null && days(r.start, r.end) >= 80 && days(r.start, r.end) <= 100;
const isAnnual = (r: { start: string | null; end: string }) => r.start != null && days(r.start, r.end) >= 350 && days(r.start, r.end) <= 380;

/**
 * The period a spec names: latest_quarter, latest_annual, FY<year> (the
 * fiscal year ending in that year) or Q<n> <year> (the quarter ending in that
 * calendar quarter). Instants take the end of the matching revenue period.
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
    const year = m[1];
    candidates = rows.filter((r) => isAnnual(r) && r.end.slice(0, 4) === year);
  } else if ((m = /^Q([1-4])\s?(\d{4})$/i.exec(text))) {
    periodType = "QUARTER";
    const q = Number(m[1]);
    const year = m[2];
    candidates = rows.filter((r) => isQuarter(r) && r.end.slice(0, 4) === year && Math.ceil(Number(r.end.slice(5, 7)) / 3) === q);
  } else {
    return { status: "INVALID_PERIOD", message: "period must be latest_quarter, latest_annual, FY<yyyy> or Q<n> <yyyy>." };
  }
  if (candidates.length === 0) {
    return { status: "PERIOD_NOT_FOUND", spec: text, periodType, message: `No ${periodType.toLowerCase()} period in companyfacts matches ${text} (20-F and 40-F filers tag annual periods only).` };
  }
  const best = [...candidates].sort((a, b) => cmp(a.end, b.end) || cmp(a.start, b.start))[candidates.length - 1];
  const kind = METRICS[metric].kind;
  return {
    status: "OK",
    spec: text,
    periodType,
    periodStart: kind === "instant" ? null : best.start,
    periodEnd: best.end,
    labelBasis: "Q<n> is the calendar quarter of the period end; FY<yyyy> is the fiscal year ending in that year",
  };
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
  const concept = concepts.filter((c) => newestFiled(c) !== "").reduce((best, c) => (newestFiled(c) > newestFiled(best) ? c : best));
  const matching = all.filter((r) => r.concept === concept).sort((a, b) => cmp(a.filed, b.filed) || cmp(a.accn, b.accn));
  const first = matching[0];
  const last = matching[matching.length - 1];
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
const MONEY_RE = /(\(?)\s?(-?)\s?\$\s?(\(?)(-?)([0-9][0-9,]*)(\.[0-9]+)?\)?(?:\s?(billion|million|thousand)\b)?/i;
const QUARTER_ORDINAL = ["first", "second", "third", "fourth"];

function explicitPeriodMatches(sentence: string, amountStart: number, period: Rec): boolean {
  const beforeAmount = sentence.slice(0, amountStart);
  if (period.periodType === "QUARTER") {
    const end = String(period.periodEnd ?? "");
    const month = Number(end.slice(5, 7));
    const q = Math.ceil(month / 3);
    const year = end.slice(0, 4);
    const quarterMentions = [...beforeAmount.matchAll(/\b(?:Q([1-4])|(?:first|second|third|fourth) quarter)\b/gi)];
    for (const m of quarterMentions) {
      const token = m[0].toLowerCase();
      const observedQ = m[1] ? Number(m[1]) : QUARTER_ORDINAL.findIndex((x) => token.startsWith(x)) + 1;
      if (observedQ !== q) return false;
      const nearby = beforeAmount.slice(m.index ?? 0, Math.min(beforeAmount.length, (m.index ?? 0) + 60));
      const ym = /\b(20\d{2})\b/.exec(nearby);
      if (ym && ym[1] !== year) return false;
    }
    // If the only explicit period is after the amount in a comparison clause,
    // it describes the comparator rather than the current-period amount.
    if (quarterMentions.length === 0) {
      const after = sentence.slice(amountStart);
      const firstPeriod = /\b(?:Q([1-4])|(?:first|second|third|fourth) quarter)\b/i.exec(after);
      if (firstPeriod) {
        const prefix = after.slice(0, firstPeriod.index);
        if (!/\b(?:compared (?:with|to)|versus|vs\.?|from)\b/i.test(prefix)) {
          const token = firstPeriod[0].toLowerCase();
          const observedQ = firstPeriod[1] ? Number(firstPeriod[1]) : QUARTER_ORDINAL.findIndex((x) => token.startsWith(x)) + 1;
          if (observedQ !== q) return false;
          const nearby = after.slice(firstPeriod.index, Math.min(after.length, firstPeriod.index + 60));
          const ym = /\b(20\d{2})\b/.exec(nearby);
          if (ym && ym[1] !== year) return false;
        }
      }
    }
    return true;
  }
  if (period.periodType === "ANNUAL") {
    const requestedYear = String(period.periodEnd ?? "").slice(0, 4);
    const beforeYears = [...beforeAmount.matchAll(/\b(20\d{2})\b/g)].map((m) => m[1]);
    if (beforeYears.some((y) => y !== requestedYear)) return false;
    const after = sentence.slice(amountStart);
    const laterYear = /\b(20\d{2})\b/.exec(after);
    if (beforeYears.length === 0 && laterYear) {
      const prefix = after.slice(0, laterYear.index);
      if (!/\b(?:compared (?:with|to)|versus|vs\.?|from)\b/i.test(prefix) && laterYear[1] !== requestedYear) return false;
    }
    return true;
  }
  return true;
}
const SCALE: Record<string, number> = { billion: 1e9, million: 1e6, thousand: 1e3 };

/** A release sentence's figure for the metric and period, or why none was read. */
export function releaseObservation(release: Rec | null, metric: string, period: Rec, reportingUnit: string | null): Rec {
  const base = { source: "ISSUER_RELEASE", provider: "ISSUER_RELEASE", url: release?.url ?? null, filingDate: release?.filingDate ?? null, accessionNumber: release?.accessionNumber ?? null };
  if (!release || release.status === "NOT_RESOLVED") return { ...base, status: "NOT_RESOLVED", value: null };
  if (release.status !== "READ" || typeof release.text !== "string") return { ...base, status: "NOT_READ", value: null };
  if (reportingUnit && !/^USD/.test(reportingUnit)) return { ...base, status: "NOT_COMPARED_CURRENCY", value: null, reportingUnit };
  const spec = METRICS[metric];
  const label = new RegExp(spec.releaseLabel, "i");
  const anchored = new RegExp(`(${spec.releaseLabel})[^$]{0,80}?${MONEY_RE.source}`, "i");
  let unscoped = 0;
  for (const raw of collapse(release.text).split(PIECE_SPLIT_RE)) {
    const sentence = raw.trim();
    if (!sentence || sentence.length > 600 || !label.test(sentence)) continue;
    if (GUIDANCE_RE.test(sentence) || NON_RESULT_RE.test(sentence)) continue;
    const m = anchored.exec(sentence);
    if (!m) continue;
    const quarter = QUARTER_SCOPE_RE.test(sentence);
    const annual = ANNUAL_SCOPE_RE.test(sentence);
    const amountStart = m.index + m[0].indexOf("$");
    const scoped = spec.kind === "instant"
      ? INSTANT_SCOPE_RE.test(sentence)
      : period.periodType === "QUARTER"
        ? quarter && !annual && explicitPeriodMatches(sentence, amountStart, period)
        : annual && !quarter && explicitPeriodMatches(sentence, amountStart, period);
    if (!scoped) {
      unscoped += 1;
      continue;
    }
    const [, labelText, openParen, minus, innerParen, innerMinus, intPart, frac, scaleWord] = m;
    const scale = scaleWord ? SCALE[scaleWord.toLowerCase()] : 1;
    const magnitude = parseFloat(`${intPart.replace(/,/g, "")}${frac ?? ""}`) * scale;
    const negative = openParen === "(" || innerParen === "(" || minus === "-" || innerMinus === "-" || /\bloss\b/i.test(labelText);
    const decimals = frac ? frac.length - 1 : 0;
    const precision = 0.5 * 10 ** -decimals * scale;
    const asWritten = m[0].slice(labelText.length).replace(/^[^$(-]*/, "").trim();
    return { ...base, status: "FOUND", value: negative ? -magnitude : magnitude, precision, asWritten, sentence: sentence.slice(0, 400) };
  }
  return { ...base, status: "NOT_FOUND_IN_TEXT", value: null, unscopedCandidates: unscoped };
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
  const comparisons: Rec[] = [];
  let conflict = false;
  let restated = false;
  if (baseline) {
    const b = baseline.value as number;
    for (const o of found) {
      if (o === baseline) continue;
      const v = o.value as number;
      const difference = v - b;
      const tolerance = Math.max((Math.abs(b) * tolerancePct) / 100, Number(o.precision ?? 0), Number(baseline.precision ?? 0));
      const match = Math.abs(difference) <= tolerance + 1e-9 * Math.max(1, Math.abs(b));
      const row: Rec = {
        source: o.source,
        against: baseline.source,
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
    : !baseline ? "PARTIAL"
    : conflict ? "CONFLICT"
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
      "AGREED: at least two providers found the value and all agree within tolerance. PARTIAL: one provider found it. CONFLICT: a provider differs from the latest SEC value beyond tolerance. NOT_FOUND: none found it.",
      "A difference between the SEC value as first filed and as latest filed is a restatement (restated: true), not a conflict.",
      "The tolerance is the larger of tolerancePct of the SEC value and half the last stated digit of a release figure.",
      "Release figures are read only from sentences scoped to the period (quarter or full year); unscopedCandidates counts sentences skipped for scope.",
    ],
    ...AUTHORITY_BOUNDARY,
  };
}
