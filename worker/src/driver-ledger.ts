/**
 * Operating-driver ledger (2.5.3), shared with yfmcp/driver_ledger.py (parity
 * in scripts/test_guidance_and_drivers.py). Pure.
 *
 * Three sources, each reported as disclosed:
 * - standard driver series from SEC companyfacts (revenue, gross profit, R&D,
 *   capex, remaining performance obligations, contract liabilities), newest
 *   filing per period; year-to-date cash-flow periods stay year-to-date;
 * - company-specific inline XBRL facts (the filer's own taxonomy) from the
 *   latest periodic filing (non-monetary, financing concepts left out);
 * - operating statements from the filing and the latest earnings release
 *   with a figure, a driver category, whether the sentence reports an actual
 *   or states a target, timing, and the quoted source.
 * A series the company does not tag is NOT_REPORTED; nothing is derived,
 * annualized or filled.
 */

import { AUTHORITY_BOUNDARY } from "./evidence.js";
import { collapse } from "./capital-structure.js";
import { timingOf } from "./funding-schedule.js";
import { REVENUE_CONCEPTS } from "./sec-facts.js";

type Rec = Record<string, unknown>;

export const DRIVER_SERIES: { driver: string; concepts: string[]; unit: string; periodKind: "duration" | "instant" }[] = [
  { driver: "revenue", concepts: [...REVENUE_CONCEPTS], unit: "USD", periodKind: "duration" },
  { driver: "gross_profit", concepts: ["GrossProfit"], unit: "USD", periodKind: "duration" },
  { driver: "research_and_development", concepts: ["ResearchAndDevelopmentExpense"], unit: "USD", periodKind: "duration" },
  { driver: "capital_expenditure", concepts: ["PaymentsToAcquirePropertyPlantAndEquipment"], unit: "USD", periodKind: "duration" },
  { driver: "remaining_performance_obligation", concepts: ["RevenueRemainingPerformanceObligation"], unit: "USD", periodKind: "instant" },
  { driver: "contract_liabilities", concepts: ["ContractWithCustomerLiability"], unit: "USD", periodKind: "instant" },
  { driver: "contract_liabilities_current", concepts: ["ContractWithCustomerLiabilityCurrent"], unit: "USD", periodKind: "instant" },
  { driver: "contract_liabilities_noncurrent", concepts: ["ContractWithCustomerLiabilityNoncurrent"], unit: "USD", periodKind: "instant" },
];

export const SERIES_POINT_LIMIT = 16;
export const COMPANY_SERIES_LIMIT = 40;
export const TEXT_DRIVER_LIMIT = 40;

/** Code-unit order, as Python compares strings. */
function cmp(a: unknown, b: unknown): number {
  const x = String(a ?? "");
  const y = String(b ?? "");
  return x < y ? -1 : x > y ? 1 : 0;
}

function days(start: string, end: string): number {
  return Math.round((Date.parse(`${end.slice(0, 10)}T00:00:00Z`) - Date.parse(`${start.slice(0, 10)}T00:00:00Z`)) / 86_400_000);
}

export function periodType(start: string | null, end: string): string {
  if (start == null) return "INSTANT";
  const d = days(start, end);
  if (d >= 80 && d <= 100) return "QUARTER";
  if (d >= 350 && d <= 380) return "ANNUAL";
  if (d > 100 && d < 350) return "YEAR_TO_DATE";
  return "OTHER";
}

/** Standard driver series from companyfacts, newest filing per period, last points by period end. */
export function xbrlDriverSeries(companyfacts: unknown): Rec[] {
  const usgaap = (((companyfacts ?? {}) as Rec).facts as Rec | undefined)?.["us-gaap"] as Rec | undefined;
  return DRIVER_SERIES.map((spec) => {
    const byPeriod = new Map<string, Rec>();
    for (const concept of spec.concepts) {
      const units = ((usgaap?.[concept] as Rec | undefined)?.units ?? {}) as Record<string, Rec[]>;
      for (const f of units[spec.unit] ?? []) {
        if (typeof f.end !== "string" || typeof f.val !== "number") continue;
        const start = typeof f.start === "string" ? f.start : null;
        if ((spec.periodKind === "instant") !== (start == null)) continue;
        if (!/^10-[KQ]/.test(String(f.form ?? ""))) continue;
        const key = `${start ?? ""}|${f.end}`;
        const prev = byPeriod.get(key);
        if (!prev || String(f.filed ?? "") > String(prev.filed ?? "")) byPeriod.set(key, { ...f, start, concept });
      }
    }
    const points = [...byPeriod.values()]
      .sort((a, b) => cmp(a.end, b.end) || cmp(a.start, b.start))
      .slice(-SERIES_POINT_LIMIT)
      .map((f) => ({
        periodStart: f.start,
        periodEnd: f.end,
        periodType: periodType(f.start as string | null, String(f.end)),
        value: f.val,
        concept: f.concept,
        form: f.form ?? null,
        filed: f.filed ?? null,
        accessionNumber: f.accn ?? null,
      }));
    return {
      driver: spec.driver,
      concepts: spec.concepts.map((c) => `us-gaap:${c}`),
      unit: spec.unit,
      // A series the company stopped tagging shows its last period here.
      latestPeriodEnd: points.length > 0 ? points[points.length - 1].periodEnd : null,
      // An unread companyfacts is not an untagged driver.
      status: companyfacts == null ? "NOT_READ" : points.length > 0 ? "REPORTED" : "NOT_REPORTED",
      points,
    };
  });
}

const STANDARD_PREFIXES = new Set(["us-gaap", "dei", "srt", "ifrs-full", "country", "currency", "exch", "stpr", "naics", "sic", "ecd", "cyd", "invest", "xbrli", "iso4217", "utr"]);

export interface InlineFact {
  name: string;
  unit: string | null;
  value: number | null;
  periodStart: string | null;
  periodEnd: string | null;
  dims: Record<string, string>;
}

function isMonetary(unit: string | null): boolean {
  return unit != null && /^[A-Z]{3}(?:\/|$)/.test(unit);
}

// Financing, equity and acquisition terms belong to the capital-structure and dilution tools, not the operating ledger.
export const CAPITAL_CONCEPT_RE = /Stock|Share|Warrant|Convertible|Note|LineOfCredit|Credit|Loan|Debt|Borrowing|Interest|Fee|Ownership|Voting|Equity|Tax|Lease|Principal|Installment|Acquisition|BusinessCombination|Dividend|Option|Award|Vesting|Compensation|Seller|Commission|Premium|Restructuring/;
const CAPITAL_AXIS_RE = /ClassOfStock|Warrant|Debt|LineOfCredit|CreditFacility|Equity|Stock/;

/** Custom units (satellites, patents, customers) first, then ratios and counts. */
function unitRank(unit: string | null): number {
  if (unit != null && /^shares$/i.test(unit)) return 2;
  if (unit == null || /^pure$/i.test(unit)) return 1;
  return 0;
}

export function conceptLabel(local: string): string {
  return local.replace(/([a-z0-9])([A-Z])/g, "$1 $2").replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2");
}

/**
 * The filer's own tagged non-monetary operating figures (its extension
 * taxonomy), grouped by concept and dimensions. Monetary extension concepts
 * are accounting line items, and financing concepts belong to the capital
 * tools; both are counted, not listed.
 */
export function companySpecificSeries(facts: InlineFact[]): { series: Rec[]; excludedCapitalConcepts: number; excludedMonetaryConcepts: number } {
  const groups = new Map<string, Rec>();
  const capital = new Set<string>();
  const monetary = new Set<string>();
  for (const f of facts) {
    if (f.value == null || f.periodEnd == null) continue;
    const colon = f.name.indexOf(":");
    const prefix = colon > 0 ? f.name.slice(0, colon) : "";
    if (!prefix || STANDARD_PREFIXES.has(prefix.toLowerCase())) continue;
    const local = f.name.slice(colon + 1);
    const dimKeys = Object.keys(f.dims).sort();
    if (CAPITAL_CONCEPT_RE.test(local) || dimKeys.some((k) => CAPITAL_AXIS_RE.test(k.slice(k.indexOf(":") + 1)))) {
      capital.add(f.name);
      continue;
    }
    if (isMonetary(f.unit)) {
      monetary.add(f.name);
      continue;
    }
    const dims: Record<string, string> = {};
    for (const k of dimKeys) dims[k] = f.dims[k];
    const key = `${f.name}|${f.unit ?? ""}|${dimKeys.map((k) => `${k}=${f.dims[k]}`).join(",")}`;
    let g = groups.get(key);
    if (!g) {
      g = { concept: f.name, label: conceptLabel(local), unit: f.unit, dims, points: [] as Rec[], seen: new Set<string>() };
      groups.set(key, g);
    }
    const periodKey = `${f.periodStart ?? ""}|${f.periodEnd}`;
    const seen = g.seen as Set<string>;
    if (seen.has(periodKey)) continue;
    seen.add(periodKey);
    (g.points as Rec[]).push({ periodStart: f.periodStart, periodEnd: f.periodEnd, periodType: periodType(f.periodStart, f.periodEnd), value: f.value });
  }
  const out = [...groups.values()].map((g) => ({
    concept: g.concept,
    label: g.label,
    unit: g.unit,
    dims: g.dims,
    points: [...(g.points as Rec[])].sort((a, b) => cmp(a.periodEnd, b.periodEnd) || cmp(a.periodStart, b.periodStart)),
  }));
  // Custom units first; within each, undimensioned before dimensioned, then by concept.
  out.sort((a, b) => unitRank(a.unit as string | null) - unitRank(b.unit as string | null)
    || Object.keys(a.dims as Rec).length - Object.keys(b.dims as Rec).length
    || cmp(a.concept, b.concept)
    || cmp(JSON.stringify(a.dims), JSON.stringify(b.dims)));
  return { series: out.slice(0, COMPANY_SERIES_LIMIT), excludedCapitalConcepts: capital.size, excludedMonetaryConcepts: monetary.size };
}

export const DRIVER_CATEGORIES: [string, RegExp][] = [
  ["capacity", /\bcapacity\b|\bmegawatts?\b|\bgigawatts?\b|\b[0-9.,]+\s?(?:MW|GW)\b/i],
  ["production", /\bproduc(?:e|ed|es|ing|tion)\b|\bmanufactur(?:e|ed|es|ing)\b|\boutput\b/i],
  ["deliveries", /\bdeliver(?:ed|ies|y)\b|\bshipments?\b|\bshipped\b/i],
  ["launches_deployments", /\blaunch(?:es|ed|ing)?\b|\bdeploy(?:s|ed|ing|ment|ments)?\b|\bin orbit\b|\bsatellites?\b/i],
  ["customers", /\bcustomers?\b|\bsubscribers?\b|\busers\b|\bmembers\b/i],
  ["backlog_bookings", /\bbacklog\b|\bbookings?\b|\bremaining performance obligations?\b|\bbook-to-bill\b|\border(?:s| book)\b/i],
  ["utilization", /\butiliz(?:ation|ed)\b|\boccupancy\b|\bload factor\b/i],
  ["pricing", /\baverage (?:selling |sales )?prices?\b|\bASPs?\b|\bpricing\b|\bprice increases?\b|\bARPU\b/i],
  ["yield", /\byields?\b/i],
  ["headcount", /\bemployees\b|\bheadcount\b|\bfull-time\b/i],
];

// A figure: a dollar amount, a percentage, or a number with a scale or unit; bare years are not figures.
const FIGURE_RE = /(\$\s?)?\b([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(\.[0-9]+)?(?:\s?(%|percent\b|billion\b|million\b|thousand\b|bn\b|MW\b|GW\b|megawatts?\b|gigawatts?\b|square (?:feet|foot|meters?)\b|[a-z][a-z-]{1,19}[a-z]\b))?/g;
const NON_UNIT_WORDS = new Set(["and", "or", "to", "of", "in", "the", "for", "from", "with", "at", "on", "as", "by", "per", "compared", "versus", "vs", "year", "years",
  "quarter", "quarters", "month", "months", "days", "day", "was", "were", "is", "are", "will", "would", "through", "into", "over", "under", "within", "than", "more",
  "less", "higher", "lower", "increase", "increases", "decrease", "decreases", "respectively", "marks", "which", "that", "this", "these", "each", "both", "after",
  "before", "during", "since", "until", "including"]);

export function figuresIn(sentence: string): Rec[] {
  const out: Rec[] = [];
  for (const m of sentence.matchAll(FIGURE_RE)) {
    const dollar = m[1] != null;
    if (!dollar) {
      // "Block 2", "BlueBird 8-13", "New Glenn 3": a number in a name is not a figure.
      const pre = sentence.slice(0, m.index ?? 0);
      const name = /([A-Z][A-Za-z-]*)\s?$/.exec(pre);
      if ((name && name.index > 0) || /[0-9]-$/.test(pre)) continue;
    }
    const unitWord = m[4] ?? null;
    const unit = unitWord != null && NON_UNIT_WORDS.has(unitWord.toLowerCase()) ? null : unitWord;
    const intPart = m[2];
    const number = parseFloat(`${intPart.replace(/,/g, "")}${m[3] ?? ""}`);
    // A year is not a figure, bare or before a word ("2026 revenue"); "2025 MW" or "2025%" still is.
    if (!dollar && m[3] == null && /^(?:19|20)[0-9]{2}$/.test(intPart)
      && !(unit != null && /^(?:%|percent|billion|million|thousand|bn|MW|GW|megawatts?|gigawatts?)$/.test(unit))) continue;
    if (!dollar && unit == null) continue;
    const asWritten = unit != null ? m[0] : `${m[1] ?? ""}${intPart}${m[3] ?? ""}`;
    out.push({ asWritten: asWritten.trim(), number, currency: dollar ? "USD" : null, unitAsWritten: unit });
    if (out.length >= 6) break;
  }
  return out;
}

const PLAN_RE = /\b(?:expects?|expected to|plans?|planned|planning|targets?|targeted|targeting|anticipates?|anticipated|aims?|intends?|will|goal|guidance|outlook|forecasts?|projects?|projected|on track to|scheduled to|by (?:the )?end of)\b/i;
const ACTUAL_RE = /\b(?:delivered|shipped|launched|deployed|produced|completed|reached|achieved|ended|added|installed|signed|totaled|totalled|grew|increased|decreased|declined|had|was|were|reported|recorded|generated)\b/i;

export function driverBasis(sentence: string): string {
  const plan = PLAN_RE.test(sentence);
  const actual = ACTUAL_RE.test(sentence);
  if (plan && !actual) return "TARGET_OR_PLAN";
  if (actual && !plan) return "REPORTED_ACTUAL";
  return "UNCLEAR";
}

export interface DriverStatement {
  contextText: string;
  sectionHeading: string | null;
  documentUrl: string | null;
  filingDate: string | null;
  accessionNumber: string | null;
  source: string;
}

// Sentences and bullets, including the " o " bullets SEC-rendered releases carry.
const PIECE_SPLIT_RE = /(?<=[.!?])\s+|\s+[\u2022\u25cf\u25aa\u25e6\u00b7]\s+|\s+o\s+(?=[A-Z])/;

/** Whole sentences of a context: a search window's cut-off first and last pieces are dropped. */
export function pieces(text: string): string[] {
  // SEC exhibit headers ("EX-99.1 2 asts-ex99_1.htm") are not sentences.
  const parts = collapse(text).split(PIECE_SPLIT_RE).map((p) => p.trim()).filter((p) => p && !/\.htm/i.test(p));
  return parts.filter((p, i) => !(i === 0 && /^[a-z,;:)]/.test(p)) && !(i === parts.length - 1 && !/[.!?]["\u201d\u2019)]?$/.test(p)));
}

/** Operating statements with a figure and a driver category, deduplicated, release first. */
export function textDrivers(statements: DriverStatement[]): Rec[] {
  const out: Rec[] = [];
  const seen = new Set<string>();
  for (const st of statements) {
    for (const sentence of pieces(st.contextText)) {
      if (sentence.length < 30 || sentence.length > 600) continue;
      const categories = DRIVER_CATEGORIES.filter(([, re]) => re.test(sentence)).map(([c]) => c);
      if (categories.length === 0) continue;
      const figures = figuresIn(sentence);
      if (figures.length === 0) continue;
      // Overlapping search windows repeat a sentence; its opening identifies it.
      const key = sentence.toLowerCase().slice(0, 120);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        categories,
        basis: driverBasis(sentence),
        figures,
        timing: timingOf(sentence),
        sentence,
        source: st.source,
        sectionHeading: st.sectionHeading,
        documentUrl: st.documentUrl,
        filingDate: st.filingDate,
        accessionNumber: st.accessionNumber,
      });
      if (out.length >= TEXT_DRIVER_LIMIT) return out;
    }
  }
  return out;
}

export interface DriverLedgerInput {
  ticker: string;
  companyfacts: unknown;
  /** null when the periodic filing could not be read. */
  inlineFacts: InlineFact[] | null;
  filing: Rec | null;
  release: Rec | null;
  statements: DriverStatement[];
}

export function operatingDriverLedger(input: DriverLedgerInput): Rec {
  const series = xbrlDriverSeries(input.companyfacts);
  const company = input.inlineFacts == null
    ? { series: [], excludedCapitalConcepts: 0, excludedMonetaryConcepts: 0 }
    : companySpecificSeries(input.inlineFacts);
  const text = textDrivers(input.statements);
  const byCategory: Record<string, number> = {};
  for (const [c] of DRIVER_CATEGORIES) byCategory[c] = 0;
  for (const t of text) for (const c of t.categories as string[]) byCategory[c] += 1;
  const byBasis: Record<string, number> = { REPORTED_ACTUAL: 0, TARGET_OR_PLAN: 0, UNCLEAR: 0 };
  for (const t of text) byBasis[t.basis as string] += 1;
  return {
    ticker: input.ticker.toUpperCase(),
    basis: "COMPANY_DISCLOSED",
    xbrlSeries: series,
    notReported: series.filter((s) => s.status === "NOT_REPORTED").map((s) => s.driver),
    notRead: series.filter((s) => s.status === "NOT_READ").map((s) => s.driver),
    companySpecificStatus: input.inlineFacts == null ? "NOT_READ" : "READ",
    companySpecificSeries: company.series,
    excludedCapitalStructureConcepts: company.excludedCapitalConcepts,
    excludedMonetaryConcepts: company.excludedMonetaryConcepts,
    textDrivers: text,
    summary: { byCategory, byBasis, textDriverCount: text.length, companySpecificSeriesCount: company.series.length },
    sources: { periodicFiling: input.filing, earningsRelease: input.release },
    notes: [
      "Standard series are SEC companyfacts as filed (10-K/10-Q), newest filing per period; year-to-date periods are not converted to quarters.",
      "Company-specific series are the filer's own non-monetary inline XBRL concepts in the latest periodic filing, custom units first; monetary extension line items and financing or equity concepts are counted, not listed.",
      "Text drivers are sentences with a figure; basis says whether the sentence reports an actual or states a target or plan, and UNCLEAR when it does both or neither.",
      "A driver the company does not tag or state is listed as not reported; nothing is derived, annualized or filled.",
    ],
    ...AUTHORITY_BOUNDARY,
  };
}
