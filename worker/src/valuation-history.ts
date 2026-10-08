/**
 * Historical valuation context (2.5.4), shared with yfmcp/valuation_history.py
 * (parity in scripts/test_valuation_history_and_reconcile.py). Pure.
 *
 * For each requested date, market capitalization, enterprise value and
 * multiples as they could have been computed on that date:
 * - the price is the close on or before the date, with Yahoo's later split
 *   adjustments undone so it matches the share count reported at the time;
 * - shares, balances and results are SEC companyfacts filed on or before the
 *   date (point in time: no later amendment or restatement is used);
 * - denominators are the last fiscal year (LFY) and the last twelve months
 *   (LTM = LFY + current year-to-date - prior year-to-date), each naming its
 *   periods, concepts and filings;
 * - reporting-currency figures are converted at the date's FX close, and an
 *   ADR's ordinary shares are divided by the ordinary shares per ADS.
 * A figure that is not tagged or not filed yet is reported as such; the
 * multiples it feeds are null. No multiple is selected or weighted.
 */

import { AUTHORITY_BOUNDARY } from "./evidence.js";
import { BORROWING_CONCEPTS, round } from "./capital-structure.js";
import { REVENUE_CONCEPTS } from "./sec-facts.js";
import { majorPrice } from "./valuation.js";
import { latestAnnualCoverageWarning } from "./companyfacts-coverage.js";

type Rec = Record<string, unknown>;

export interface Bar { date: string; close: number }
export interface Split { date: string; ratio: number }

export interface ValuationHistoryInput {
  ticker: string;
  dates: string[];
  companyfacts: unknown;
  bars: Bar[];
  priceCurrency: string | null;
  splits: Split[];
  /** Converts the reporting currency into the price currency; null when not needed or not read. */
  fx: { pair: string; bars: Bar[] } | null;
  /**
   * The same for each other reporting currency the dates use (2.5.32, F-029: Nebius reported in RUB until its
   * FY2024 20-F, then in USD), keyed by currency.
   */
  fxByCurrency?: Record<string, { pair: string; bars: Bar[] }> | null;
  /** Ordinary shares per quoted share (ADS), when the quote is for depositary shares. */
  adsRatio: number | null;
  /**
   * Cover-page share counts read from periodic filings' inline XBRL (coverShareCounts), keyed by
   * accession number: the counts companyfacts leaves out when a filer tags them per share class.
   */
  coverCounts?: Record<string, Rec> | null;
  /** The filer's 10-K, 10-Q, 20-F and 40-F filings (SEC submissions), whose cover pages coverCounts holds. */
  periodicFilings?: PeriodicFiling[] | null;
}

export interface PeriodicFiling { accessionNumber: string; form: string; filed: string; reportDate: string | null }

/** The periodic report filed most recently on or before asOf. */
function latestFiling(filings: PeriodicFiling[] | null | undefined, asOf: string): PeriodicFiling | null {
  let best: PeriodicFiling | null = null;
  for (const f of filings ?? []) {
    if (f.filed <= asOf && (best == null || cmp(f.filed, best.filed) > 0 || (f.filed === best.filed && cmp(f.accessionNumber, best.accessionNumber) > 0))) best = f;
  }
  return best;
}

interface Fact { concept: string; start: string | null; end: string; val: number; filed: string; form: string; accn: string | null }

interface TaxonomyMap {
  revenue: string[];
  operatingIncome: string[];
  netIncome: string[];
  depreciationAmortization: string[][];
  cash: string[];
  shortTermInvestments: string[];
  debtGroups: string[][];
  debtAdditions: string[];
  sharesInstant: string[];
  sharesWeighted: string[];
  // The concepts that would show the filing has borrowings; empty where no such rule applies (2.5.32, F-026).
  borrowingConcepts: string[];
  // Depreciation alone stands in for D&A when the amortization concept was never tagged (2.5.32, F-026).
  depreciationOnly: { depreciation: string; amortization: string } | null;
}

const US_GAAP: TaxonomyMap = {
  revenue: [...REVENUE_CONCEPTS],
  operatingIncome: ["OperatingIncomeLoss"],
  netIncome: ["NetIncomeLoss", "ProfitLoss"],
  depreciationAmortization: [["DepreciationDepletionAndAmortization"], ["DepreciationAndAmortization"], ["DepreciationAmortizationAndAccretionNet"], ["Depreciation", "AmortizationOfIntangibleAssets"]],
  cash: ["CashAndCashEquivalentsAtCarryingValue", "Cash"],
  shortTermInvestments: ["ShortTermInvestments", "MarketableSecuritiesCurrent", "AvailableForSaleSecuritiesDebtSecuritiesCurrent"],
  debtGroups: [["LongTermDebt"], ["LongTermDebtCurrent", "LongTermDebtNoncurrent"], ["ConvertibleNotesPayableCurrent", "ConvertibleNotesPayable", "ConvertibleLongTermNotesPayable"], ["NotesPayableCurrent", "NotesPayable"]],
  debtAdditions: ["ShortTermBorrowings", "CommercialPaper"],
  borrowingConcepts: [...BORROWING_CONCEPTS],
  depreciationOnly: { depreciation: "Depreciation", amortization: "AmortizationOfIntangibleAssets" },
  sharesInstant: ["CommonStockSharesOutstanding"],
  sharesWeighted: ["WeightedAverageNumberOfSharesOutstandingBasic"],
};

const IFRS: TaxonomyMap = {
  revenue: ["Revenue", "RevenueFromContractsWithCustomers"],
  operatingIncome: ["ProfitLossFromOperatingActivities"],
  netIncome: ["ProfitLossAttributableToOwnersOfParent", "ProfitLoss"],
  depreciationAmortization: [["DepreciationAndAmortisationExpense"], ["DepreciationExpense", "AmortisationExpense"]],
  cash: ["CashAndCashEquivalents"],
  shortTermInvestments: [],
  debtGroups: [["Borrowings"], ["CurrentBorrowingsAndCurrentPortionOfNoncurrentBorrowings", "NoncurrentPortionOfNoncurrentBorrowings"], ["ShorttermBorrowings", "CurrentPortionOfLongtermBorrowings", "LongtermBorrowings"]],
  debtAdditions: [],
  borrowingConcepts: [],
  depreciationOnly: null,
  sharesInstant: [],
  sharesWeighted: ["WeightedAverageShares"],
};

const PERIODIC_FORM_RE = /^(?:10-K|10-Q|20-F|40-F)/;

export function days(start: string, end: string): number {
  return Math.round((Date.parse(`${end.slice(0, 10)}T00:00:00Z`) - Date.parse(`${start.slice(0, 10)}T00:00:00Z`)) / 86_400_000);
}

function cmp(a: unknown, b: unknown): number {
  const x = String(a ?? "");
  const y = String(b ?? "");
  return x < y ? -1 : x > y ? 1 : 0;
}

/** us-gaap when it carries revenue, else ifrs-full; the reporting currency is the revenue unit with the most facts. */
export function taxonomyOf(companyfacts: unknown): { taxonomy: string | null; currency: string | null } {
  const facts = ((companyfacts ?? {}) as Rec).facts as Rec | undefined;
  for (const [taxonomy, map] of [["us-gaap", US_GAAP], ["ifrs-full", IFRS]] as const) {
    const tax = (facts?.[taxonomy] ?? null) as Rec | null;
    if (!tax) continue;
    const counts = new Map<string, number>();
    for (const concept of map.revenue) {
      const units = (((tax[concept] as Rec | undefined)?.units ?? {}) as Record<string, unknown[]>);
      for (const [unit, rows] of Object.entries(units)) {
        if (/^[A-Z]{3}$/.test(unit)) counts.set(unit, (counts.get(unit) ?? 0) + rows.length);
      }
    }
    if (counts.size === 0) continue;
    const currency = [...counts.entries()].sort((a, b) => b[1] - a[1] || cmp(a[0], b[0]))[0][0];
    return { taxonomy, currency };
  }
  return { taxonomy: null, currency: null };
}

/**
 * The reporting currency as of a date: the currency of the newest annual revenue fact filed by then (2.5.32,
 * F-029). Where that filing tags more than one currency (a convenience translation), the filer's main currency
 * wins. Falls back to the main currency when no annual revenue is filed by then.
 */
export function reportingCurrencyAt(companyfacts: unknown, asOf: string): string | null {
  const { taxonomy, currency } = taxonomyOf(companyfacts);
  if (!taxonomy) return null;
  const tax = ((((companyfacts ?? {}) as Rec).facts as Rec)[taxonomy] ?? {}) as Rec;
  const day = asOf.slice(0, 10);
  let best: { key: string; units: Set<string> } | null = null;
  for (const concept of (taxonomy === "ifrs-full" ? IFRS : US_GAAP).revenue) {
    for (const [unit, rows] of Object.entries((((tax[concept] as Rec | undefined)?.units ?? {}) as Record<string, Rec[]>))) {
      if (!/^[A-Z]{3}$/.test(unit) || !Array.isArray(rows)) continue;
      for (const r of rows) {
        if (typeof r.start !== "string" || typeof r.end !== "string" || typeof r.filed !== "string" || r.filed > day) continue;
        const span = days(r.start, r.end);
        if (span < 350 || span > 380) continue;
        const key = `${r.filed}|${r.end}`;
        if (!best || key > best.key) best = { key, units: new Set([unit]) };
        else if (key === best.key) best.units.add(unit);
      }
    }
  }
  if (!best) return currency;
  return currency && best.units.has(currency) ? currency : [...best.units].sort()[0];
}

/** The distinct reporting currencies the dates use, in date order. */
export function reportingCurrencies(companyfacts: unknown, dates: string[]): string[] {
  return [...new Set(dates.map((d) => reportingCurrencyAt(companyfacts, d)).filter((c): c is string => c != null))];
}

/** Periodic-report facts for concepts in one unit, filed on or before asOf; the newest filing of each period wins. */
function factsFor(tax: Rec | null, concepts: string[], unit: string, asOf: string): Fact[] {
  const byPeriod = new Map<string, Fact>();
  for (const concept of concepts) {
    const rows = ((((tax?.[concept] as Rec | undefined)?.units ?? {}) as Record<string, Rec[]>)[unit]) ?? [];
    for (const f of rows) {
      if (typeof f.end !== "string" || typeof f.val !== "number" || typeof f.filed !== "string") continue;
      if (f.filed > asOf || !PERIODIC_FORM_RE.test(String(f.form ?? ""))) continue;
      const start = typeof f.start === "string" ? f.start : null;
      const key = `${start ?? ""}|${f.end}`;
      const prev = byPeriod.get(key);
      const candidate = { concept, start, end: f.end, val: f.val, filed: f.filed, form: String(f.form), accn: typeof f.accn === "string" ? f.accn : null };
      const candidatePriority = concepts.indexOf(concept);
      const previousPriority = prev ? concepts.indexOf(prev.concept) : Number.POSITIVE_INFINITY;
      // Concept order is semantic precedence. A later filing may update the same
      // concept, but a lower-priority alternate concept must not silently replace it.
      if (!prev || candidatePriority < previousPriority || (candidatePriority === previousPriority && f.filed > prev.filed)) {
        byPeriod.set(key, candidate);
      }
    }
  }
  return [...byPeriod.values()];
}

function component(f: Fact, sign = 1): Rec {
  return { concept: f.concept, periodStart: f.start, periodEnd: f.end, value: f.val, sign, form: f.form, filed: f.filed, accessionNumber: f.accn };
}

const isAnnual = (f: Fact) => f.start != null && days(f.start, f.end) >= 350 && days(f.start, f.end) <= 380;

/**
 * A flow metric on the LFY and LTM bases as known at asOf. LTM is the last
 * fiscal year when it is the latest period filed, else LFY + current
 * year-to-date - prior year-to-date.
 */
export function flowBases(facts: Fact[]): { LFY: Rec; LTM: Rec } {
  const durations = facts.filter((f) => f.start != null);
  const annual = durations.filter(isAnnual).sort((a, b) => cmp(a.end, b.end));
  const lfy = annual.length > 0 ? annual[annual.length - 1] : null;
  if (!lfy) {
    const missing = { status: "NOT_AVAILABLE", value: null, reason: "NO_ANNUAL_PERIOD_FILED" };
    return { LFY: missing, LTM: missing };
  }
  const lfyOut = { status: "OK", value: lfy.val, periodStart: lfy.start, periodEnd: lfy.end, method: "REPORTED_FISCAL_YEAR", components: [component(lfy)] };
  const later = durations.filter((f) => f.end > lfy.end);
  if (later.length === 0) return { LFY: lfyOut, LTM: { ...lfyOut, method: "LAST_FISCAL_YEAR_IS_LATEST" } };
  const latestEnd = later.reduce((a, b) => (b.end > a ? b.end : a), "");
  // The year-to-date period that starts the day after the fiscal year ended.
  const ytd = later
    .filter((f) => f.end === latestEnd && days(lfy.end, f.start as string) >= 0 && days(lfy.end, f.start as string) <= 4)
    .sort((a, b) => days(b.start as string, b.end) - days(a.start as string, a.end))[0];
  if (!ytd) return { LFY: lfyOut, LTM: { status: "NOT_AVAILABLE", value: null, reason: "YEAR_TO_DATE_NOT_FILED", latestPeriodEnd: latestEnd } };
  const span = days(ytd.start as string, ytd.end);
  const prior = durations.find((f) =>
    Math.abs(days(lfy.start as string, f.start as string)) <= 4
    && days(f.end, ytd.end) >= 358 && days(f.end, ytd.end) <= 372
    && Math.abs(days(f.start as string, f.end) - span) <= 7);
  if (!prior) return { LFY: lfyOut, LTM: { status: "NOT_AVAILABLE", value: null, reason: "PRIOR_YEAR_TO_DATE_NOT_FILED", latestPeriodEnd: latestEnd } };
  return {
    LFY: lfyOut,
    LTM: {
      status: "OK",
      value: lfy.val + ytd.val - prior.val,
      periodStart: null,
      periodEnd: ytd.end,
      method: "LFY_PLUS_YTD_MINUS_PRIOR_YTD",
      components: [component(lfy), component(ytd), component(prior, -1)],
    },
  };
}

export function rateAt(bars: Bar[], date: string): Bar | null {
  let best: Bar | null = null;
  for (const b of bars) {
    if (b.date <= date && days(b.date, date) <= 7 && (best == null || b.date > best.date)) best = b;
  }
  return best;
}

const latestFact = (fs: Fact[]): Fact | null => {
  const sorted = [...fs].sort((a, b) => cmp(a.end, b.end) || days(b.start ?? b.end, b.end) - days(a.start ?? a.end, a.end));
  return sorted.length > 0 ? sorted[sorted.length - 1] : null;
};

/** The undimensioned point-in-time count (cover page, else balance sheet) and the latest weighted-average basic count filed by asOf. */
function shareFactsAt(dei: Rec | null, tax: Rec | null, map: TaxonomyMap, asOf: string): { pointInTime: Rec | null; weighted: Fact | null } {
  const cover = latestFact(factsFor(dei, ["EntityCommonStockSharesOutstanding"], "shares", asOf).filter((f) => f.start == null));
  const balance = cover ? null : latestFact(factsFor(tax, map.sharesInstant, "shares", asOf).filter((f) => f.start == null));
  const weighted = latestFact(factsFor(tax, map.sharesWeighted, "shares", asOf).filter((f) => f.start != null));
  const pointInTime = cover
    ? { value: cover.val, asOf: cover.end, basis: "COVER_PAGE", ...component(cover) }
    : balance
      ? { value: balance.val, asOf: balance.end, basis: "BALANCE_SHEET", ...component(balance) }
      : null;
  return { pointInTime, weighted };
}

/**
 * The share count at asOf. An undimensioned cover-page or balance-sheet count comes from companyfacts; when
 * there is none, or the latest periodic report filed by asOf has a newer cover page, that report's per-class
 * cover counts are summed. A weighted-average count is returned only as context: it is an average over a
 * period, may cover one class, and never sets a point-in-time market value.
 */
function sharesAt(dei: Rec | null, tax: Rec | null, map: TaxonomyMap, asOf: string, coverCounts: Record<string, Rec> | null = null, filings: PeriodicFiling[] | null = null): Rec | null {
  const { pointInTime, weighted } = shareFactsAt(dei, tax, map, asOf);
  const filing = latestFiling(filings, asOf);
  const read = filing ? coverCounts?.[filing.accessionNumber] ?? null : null;
  if (filing && read && read.status === "OK" && (!pointInTime || String(read.asOf) > String(pointInTime.asOf))) {
    return {
      value: read.value,
      asOf: read.asOf,
      basis: read.basis,
      classes: read.classes,
      form: filing.form,
      filed: filing.filed,
      accessionNumber: filing.accessionNumber,
      documentUrl: read.documentUrl ?? null,
    };
  }
  if (pointInTime) return pointInTime;
  if (weighted) {
    return {
      value: weighted.val,
      asOf: weighted.end,
      basis: "WEIGHTED_AVERAGE_BASIC",
      pointInTime: false,
      coverPageRead: read
        ? { status: read.status, accessionNumber: filing?.accessionNumber ?? null, documentUrl: read.documentUrl ?? null }
        : { status: filing ? "NOT_READ" : "NO_PERIODIC_FILING", accessionNumber: filing?.accessionNumber ?? null },
      ...component(weighted),
    };
  }
  return null;
}

/**
 * The periodic reports whose cover pages are needed: at each date, the latest report filed by then when
 * companyfacts has no undimensioned count as new as that report's period.
 */
export function coverReadsNeeded(companyfacts: unknown, filings: PeriodicFiling[] | null, dates: string[]): PeriodicFiling[] {
  const { taxonomy } = taxonomyOf(companyfacts);
  if (!taxonomy) return [];
  const facts = (((companyfacts ?? {}) as Rec).facts ?? {}) as Rec;
  const map = taxonomy === "ifrs-full" ? IFRS : US_GAAP;
  const out = new Map<string, PeriodicFiling>();
  for (const date of dates) {
    const filing = latestFiling(filings, date);
    if (!filing) continue;
    const { pointInTime } = shareFactsAt((facts.dei ?? null) as Rec | null, (facts[taxonomy] ?? null) as Rec | null, map, date);
    if (pointInTime && String(pointInTime.asOf) >= (filing.reportDate ?? filing.filed)) continue;
    out.set(filing.accessionNumber, filing);
  }
  return [...out.values()].sort((a, b) => cmp(a.filed, b.filed) || cmp(a.accessionNumber, b.accessionNumber));
}

const COVER_FACT_RE = /<ix:nonfraction\b([^>]*\bname="dei:EntityCommonStockSharesOutstanding"[^>]*)>([\s\S]*?)<\/ix:nonfraction>/gi;
const CLASS_AXIS_RE = /ClassOfStockAxis$/;

function attr(attrs: string, name: string): string | null {
  const m = new RegExp(`\\b${name}="([^"]*)"`, "i").exec(attrs);
  return m ? m[1] : null;
}

/** "us-gaap:CommonClassAMember" -> "Common Class A". */
function memberName(member: string): string {
  return member.replace(/^[^:]*:/, "").replace(/Member$/, "").replace(/([a-z])([A-Z])/g, "$1 $2").replace(/([A-Z])([A-Z][a-z])/g, "$1 $2");
}

/**
 * dei:EntityCommonStockSharesOutstanding from a periodic filing's inline XBRL (its first part: the cover page
 * and the header contexts). At the latest cover date: the undimensioned count, else the sum of the per-class
 * counts. Counts carrying any dimension other than the class-of-stock axis are not summed.
 */
export function coverShareCounts(html: string): Rec {
  const facts: { value: number; date: string; members: { dimension: string; member: string }[]; context: string }[] = [];
  let unresolved = 0;
  let unparsed = 0;
  const contexts = new Map<string, { date: string; members: { dimension: string; member: string }[] } | null>();
  const contextOf = (id: string) => {
    if (contexts.has(id)) return contexts.get(id) ?? null;
    const escaped = id.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const m = new RegExp(`<(?:[a-z]+:)?context\\b[^>]*\\bid="${escaped}"[^>]*>([\\s\\S]*?)</(?:[a-z]+:)?context>`, "i").exec(html);
    const instant = m ? /<(?:[a-z]+:)?instant>\s*(\d{4}-\d{2}-\d{2})\s*</i.exec(m[1]) : null;
    const value = m && instant
      ? {
          date: instant[1],
          members: [...m[1].matchAll(/<(?:[a-z]+:)?explicitMember\b[^>]*\bdimension="([^"]+)"[^>]*>\s*([^<\s]+)\s*</gi)].map((x) => ({ dimension: x[1], member: x[2] })),
        }
      : null;
    contexts.set(id, value);
    return value;
  };
  const seen = new Set<string>();
  for (const m of html.matchAll(COVER_FACT_RE)) {
    const attrs = m[1];
    const contextRef = attr(attrs, "contextRef");
    if (!contextRef) continue;
    const ctx = contextOf(contextRef);
    if (!ctx) {
      unresolved += 1;
      continue;
    }
    if (seen.has(contextRef)) continue;
    seen.add(contextRef);
    const text = m[2].replace(/<[^>]*>/g, "").trim();
    const format = attr(attrs, "format") ?? "";
    let value: number;
    if (/zero|dash/i.test(format) || /^[-\u2013\u2014]$/.test(text)) {
      value = 0;
    } else {
      const digits = /comma-?decimal|numcommadecimal/i.test(format) ? text.replace(/[.\s\u00a0]/g, "").replace(/,\d*$/, "") : text.replace(/[,\s\u00a0]/g, "").replace(/\.\d*$/, "");
      if (!/^\d+$/.test(digits)) {
        unparsed += 1;
        continue;
      }
      value = Number(digits) * 10 ** Number(attr(attrs, "scale") ?? 0);
    }
    facts.push({ value, date: ctx.date, members: ctx.members, context: contextRef });
  }
  // A class whose context or value was not read would make any sum partial.
  if (unresolved > 0) return { status: "CONTEXT_NOT_READ", value: null };
  if (unparsed > 0) return { status: "VALUE_NOT_PARSED", value: null };
  if (facts.length === 0) return { status: "NOT_TAGGED", value: null };
  const date = facts.reduce((d, f) => (f.date > d ? f.date : d), "");
  const atDate = facts.filter((f) => f.date === date);
  const plain = atDate.find((f) => f.members.length === 0);
  if (plain) return { status: "OK", value: plain.value, asOf: date, basis: "COVER_PAGE", classes: [] };
  if (atDate.some((f) => f.members.length !== 1 || !CLASS_AXIS_RE.test(f.members[0].dimension))) {
    return { status: "OTHER_DIMENSIONS", value: null, asOf: date, dimensions: [...new Set(atDate.flatMap((f) => f.members.map((x) => x.dimension)))].sort() };
  }
  const classes = atDate
    .map((f) => ({ class: memberName(f.members[0].member), member: f.members[0].member, shares: f.value }))
    .sort((a, b) => cmp(a.member, b.member));
  return { status: "OK", value: classes.reduce((sum, c) => sum + c.shares, 0), asOf: date, basis: "COVER_PAGE_CLASS_SUM", classes };
}

/** Whether companyfacts holds a fact of any of the concepts, in any unit and at any date, from the accession. */
function accessionTagsAny(tax: Rec | null, concepts: string[], accn: string): boolean {
  return concepts.some((c) => Object.values(((((tax ?? {}) as Rec)[c] as Rec | undefined)?.units ?? {}) as Record<string, unknown>)
    .some((rows) => Array.isArray(rows) && rows.some((r) => (r as Rec)?.accn === accn)));
}

function balancesAt(tax: Rec | null, map: TaxonomyMap, currency: string, asOf: string): Rec {
  const cashFacts = factsFor(tax, map.cash, currency, asOf).filter((f) => f.start == null).sort((a, b) => cmp(a.end, b.end));
  if (cashFacts.length === 0) return { status: "NOT_AVAILABLE", reason: "CASH_NOT_TAGGED" };
  const at = cashFacts[cashFacts.length - 1].end;
  // Every balance is read at the cash balance's date; a concept last tagged earlier is not carried forward.
  const tagged = (concepts: string[]) => factsFor(tax, concepts, currency, asOf).filter((f) => f.start == null && f.end === at);
  const cash = cashFacts.filter((f) => f.end === at)[0];
  const sti = map.shortTermInvestments.map((c) => tagged([c])[0]).find((f) => f != null) ?? null;
  let debt: Rec = { status: "NOT_TAGGED", value: null, concepts: map.debtGroups.flat() };
  for (const group of map.debtGroups) {
    const parts = group.map((c) => tagged([c])[0]).filter((f): f is Fact => f != null);
    if (parts.length === 0) continue;
    const additions = map.debtAdditions.map((c) => tagged([c])[0]).filter((f): f is Fact => f != null);
    const all = [...parts, ...additions];
    debt = { status: "OK", value: all.reduce((s, f) => s + f.val, 0), components: all.map((f) => component(f)) };
    break;
  }
  // The filing that reported this balance sheet tags no borrowing concept at any date: debt is zero, as
  // get_valuation_snapshot reads the same filing (2.5.32, F-026: AEHR's EV was DEBT_NOT_TAGGED here and computed
  // there). Companyfacts holds undimensioned facts only, so a filing tagging debt only by member is not seen.
  if (debt.status === "NOT_TAGGED" && cash.accn && map.borrowingConcepts.length > 0 && !accessionTagsAny(tax, map.borrowingConcepts, cash.accn)) {
    debt = { status: "OK", value: 0, basis: "NO_BORROWINGS_TAGGED", components: [], accessionNumber: cash.accn };
  }
  return {
    status: "OK",
    balanceDate: at,
    currency,
    cash: { value: cash.val, ...component(cash) },
    shortTermInvestments: sti
      ? { status: "OK", value: sti.val, ...component(sti) }
      : { status: map.shortTermInvestments.length > 0 ? "NOT_TAGGED" : "NOT_MAPPED", value: null, concepts: map.shortTermInvestments },
    debt,
  };
}

/** The concepts a denominator was read from, in component order: EBITDA's operating income and D&A included. */
function denominatorConcepts(d: Rec | null): string[] {
  if (!d || d.status !== "OK") return [];
  const parts = [d, d.operatingIncome as Rec | undefined, d.depreciationAmortization as Rec | undefined].filter((x): x is Rec => !!x);
  const names = parts.flatMap((x) => ((x.components ?? []) as Rec[]).map((c) => String(c.concept)));
  return [...new Set(names)];
}

function multiple(numerator: number | null, denominator: Rec | null, fx: number | null, numeratorName: string, basis: string, metric: string): Rec {
  const den = denominator && denominator.status === "OK" ? (denominator.value as number) : null;
  // The denominator's own concepts, on the multiple (2.5.31, F-030: BE's EV/Revenue was read from
  // RevenueFromContractWithCustomerExcludingAssessedTax, 1,135,346,000, not its 1,199,125,000 total revenue).
  const ref = { numerator: numeratorName, denominator: metric, basis, denominatorPeriodEnd: denominator?.periodEnd ?? null, denominatorConcepts: denominatorConcepts(denominator) };
  if (numerator == null) return { value: null, status: `${numeratorName.toUpperCase()}_NOT_AVAILABLE`, ...ref };
  if (den == null) return { value: null, status: "DENOMINATOR_NOT_AVAILABLE", ...ref };
  if (fx == null) return { value: null, status: "FX_NOT_AVAILABLE", ...ref };
  // A negative enterprise value (net cash above market cap) has no meaningful multiple.
  if (!(numerator > 0)) return { value: null, status: "NOT_MEANINGFUL_NONPOSITIVE_NUMERATOR", ...ref };
  const converted = den * fx;
  if (!(converted > 0)) return { value: null, status: "NOT_MEANINGFUL_NONPOSITIVE_DENOMINATOR", ...ref };
  return { value: round(numerator / converted, 2), status: "OK", ...ref };
}

/** EBITDA as operating income plus depreciation and amortization on the same basis and periods. */
function ebitda(oi: Rec, da: Rec, daConcepts: string[] | null): Rec {
  if (oi.status !== "OK") return { status: "NOT_AVAILABLE", value: null, reason: "OPERATING_INCOME_NOT_AVAILABLE" };
  if (da.status !== "OK") return { status: "NOT_AVAILABLE", value: null, reason: "DEPRECIATION_AMORTIZATION_NOT_AVAILABLE" };
  if (oi.periodEnd !== da.periodEnd) return { status: "NOT_AVAILABLE", value: null, reason: "PERIODS_DIFFER", operatingIncomePeriodEnd: oi.periodEnd, depreciationPeriodEnd: da.periodEnd };
  return {
    status: "OK",
    value: (oi.value as number) + (da.value as number),
    periodStart: oi.periodStart,
    periodEnd: oi.periodEnd,
    method: `operating income + ${(daConcepts ?? []).join(" + ")} (computed, not a reported figure)`,
    operatingIncome: oi,
    depreciationAmortization: da,
  };
}

/**
 * Depreciation and amortization per basis: the first concept group with that
 * basis for operating income's period (VRT tags the total only in 10-Ks and
 * depreciation and amortization separately in 10-Qs). Separate parts are
 * summed when all are there for the same periods.
 */
function daFor(tax: Rec | null, map: TaxonomyMap, currency: string, asOf: string, oi: { LFY: Rec; LTM: Rec }): Record<"LFY" | "LTM", { value: Rec; concepts: string[] | null }> {
  const groups = map.depreciationAmortization.map((group) => {
    const parts = group.map((c) => flowBases(factsFor(tax, [c], currency, asOf)));
    if (parts.length === 1) return { group, bases: parts[0] };
    const sum = (key: "LFY" | "LTM"): Rec => {
      const rows = parts.map((p) => p[key]);
      if (rows.some((r) => r.status !== "OK") || rows.some((r) => r.periodEnd !== rows[0].periodEnd)) return { status: "NOT_AVAILABLE", value: null, reason: "PARTS_NOT_ALIGNED" };
      return { ...rows[0], value: rows.reduce((s, r) => s + (r.value as number), 0), components: rows.flatMap((r) => r.components as Rec[]) };
    };
    return { group, bases: { LFY: sum("LFY"), LTM: sum("LTM") } };
  });
  // A filer that never tagged the amortization concept by asOf (BE) has depreciation only.
  const only = map.depreciationOnly;
  if (only && factsFor(tax, [only.amortization], currency, asOf).length === 0) {
    groups.push({ group: [only.depreciation], bases: flowBases(factsFor(tax, [only.depreciation], currency, asOf)) });
  }
  const pick = (key: "LFY" | "LTM") => {
    const ok = groups.filter((g) => g.bases[key].status === "OK");
    const aligned = ok.find((g) => g.bases[key].periodEnd === oi[key].periodEnd) ?? ok[0];
    return aligned ? { value: aligned.bases[key], concepts: aligned.group } : { value: { status: "NOT_AVAILABLE", value: null, reason: "NOT_TAGGED" }, concepts: null };
  };
  return { LFY: pick("LFY"), LTM: pick("LTM") };
}

/**
 * Staleness limits in days, by the cadence of the periodic facts SEC companyfacts holds as of a date:
 * annual-only when the latest annual report filed by then is a 20-F or 40-F (those filers tag annual periods
 * only; their interim 6-K, IR or exchange disclosures are not in companyfacts). Balances and share counts then
 * arrive once a year, about four months after year end, so a quarterly filer's 200-day balance limit would
 * leave their EV null for most of each year. This is not the issuer's disclosure cadence.
 */
export function stalenessLimits(companyfacts: unknown, asOf = "9999-12-31"): { cadence: string; shareCountDays: number; balancesDays: number; resultsDays: number } {
  return foreignFiler(companyfacts, asOf)
    ? { cadence: "ANNUAL", shareCountDays: 500, balancesDays: 500, resultsDays: 500 }
    : { cadence: "QUARTERLY", shareCountDays: 400, balancesDays: 200, resultsDays: 500 };
}

// Past this age an annual filer's share count is flagged, though still used (2.5.32, F-029).
export const SHARE_COUNT_AGED_DAYS = 183;

const MULTIPLE_NAMES = ["evToRevenue", "evToEbitda", "priceToEarnings", "priceToSales"];

/** Market value, balances, denominators and multiples as they stood on one date. */
export function valuationAtDate(input: ValuationHistoryInput, date: string, taxonomy: string, currency: string): Rec {
  const limits = stalenessLimits(input.companyfacts, date);
  const cadence = { reportingCurrency: currency, secCompanyfactsCadence: limits.cadence, stalenessLimitsDays: { shareCount: limits.shareCountDays, balances: limits.balancesDays, results: limits.resultsDays } };
  const facts = ((input.companyfacts ?? {}) as Rec).facts as Rec;
  const tax = (facts[taxonomy] ?? null) as Rec | null;
  const dei = (facts.dei ?? null) as Rec | null;
  const map = taxonomy === "ifrs-full" ? IFRS : US_GAAP;
  const warnings: Rec[] = [];

  const bar = rateAt(input.bars, date);
  if (!bar) return { date, status: "PRICE_UNAVAILABLE", coreStatus: "PRICE_UNAVAILABLE", ...cadence, warnings: [{ code: "PRICE_UNAVAILABLE", message: `No close within 7 days on or before ${date}.`, severity: "warning" }] };
  // Yahoo's closes are adjusted for every later split; undo that so the price matches the share count reported then.
  const laterSplits = input.splits.filter((s) => s.date > bar.date && s.ratio > 0);
  const splitFactor = laterSplits.reduce((f, s) => f * s.ratio, 1);
  const major = majorPrice(bar.close * splitFactor, input.priceCurrency);
  const priceCurrency = major.currency;
  const price = { tradingDate: bar.date, closeAsAdjusted: bar.close, laterSplitFactor: splitFactor, close: round(major.price, 4), currency: priceCurrency, laterSplits };

  const shares = sharesAt(dei, tax, map, date, input.coverCounts ?? null, input.periodicFilings ?? null);
  // The count's age at the date, on the count itself (2.5.32, F-029).
  if (shares && typeof shares.asOf === "string") shares.ageDays = days(shares.asOf, date);
  let marketCap: Rec;
  if (!shares) {
    marketCap = { status: "SHARES_NOT_AVAILABLE", value: null };
  } else if (!(typeof shares.value === "number" && shares.value > 0)) {
    // A count of zero or less is not a share count: BE's 2019-06-28 count of 0 gave market cap OK 0 and EV equal
    // to debt less cash (2.5.31, F-025). The count is shown; market cap, EV and the multiples are null.
    marketCap = { status: "SHARE_COUNT_NOT_POSITIVE", value: null };
    warnings.push({ code: "SHARE_COUNT_NOT_POSITIVE", message: `The share count filed by ${date} (${shares.basis}, as of ${shares.asOf}) is ${shares.value}; no market value is computed from it.`, severity: "warning" });
  } else if (shares.basis === "WEIGHTED_AVERAGE_BASIC") {
    // An average over a period, possibly of one class: not a count of the shares outstanding on the date.
    marketCap = { status: "POINT_IN_TIME_SHARES_UNRESOLVED", value: null };
    const coverStatus = String(((shares.coverPageRead ?? {}) as Rec).status ?? "NOT_READ");
    warnings.push({ code: "POINT_IN_TIME_SHARES_UNRESOLVED", message: `No undimensioned cover-page or balance-sheet share count is in companyfacts and the filing's cover page gave no usable count (${coverStatus}); the weighted-average basic count ${shares.value} (period ending ${shares.asOf}) is shown but not used, so market cap and enterprise value are null.`, severity: "warning" });
  } else {
    if (shares.basis === "COVER_PAGE_CLASS_SUM") {
      const classes = (shares.classes ?? []) as Rec[];
      warnings.push({ code: "SHARE_CLASSES_SUMMED", message: `The cover page of ${shares.form} ${shares.accessionNumber} counts ${classes.length} share classes as of ${shares.asOf} (${classes.map((c) => `${c.class}: ${c.shares}`).join(", ")}); market cap values every class at the quoted class's close.`, severity: "info" });
    }
    const shareCountStale = days(String(shares.asOf), date) > limits.shareCountDays;
    if (shareCountStale) {
      warnings.push({ code: "SHARE_COUNT_STALE", message: `The latest share count filed by ${date} is as of ${shares.asOf}.`, severity: "warning" });
    } else if (limits.cadence === "ANNUAL" && (shares.ageDays as number) > SHARE_COUNT_AGED_DAYS) {
      // An annual filer's count is used for up to 500 days; past half a year, say so (2.5.32, F-029: NBIS's
      // 2025-12-31 market cap used the 2024-12-31 count, before 17M shares issued in 2025).
      warnings.push({ code: "SHARE_COUNT_AGED", message: `The latest share count filed by ${date} is as of ${shares.asOf}, ${shares.ageDays} days earlier; SEC companyfacts holds this filer's counts once a year, so shares issued or repurchased since are not reflected.`, severity: "info" });
    }
    const splitAfterCount = input.splits.find((s) => s.date > String(shares.asOf) && s.date <= bar.date);
    const quoted = input.adsRatio != null && input.adsRatio > 0 ? (shares.value as number) / input.adsRatio : (shares.value as number);
    if (input.adsRatio != null) (shares as Rec).quotedShareEquivalent = round(quoted);
    marketCap = splitAfterCount
      ? { status: "SPLIT_AFTER_SHARE_COUNT", value: null, split: splitAfterCount }
      : shareCountStale
        ? { status: "SHARE_COUNT_STALE", value: null, asOf: shares.asOf }
        : { status: "OK", value: round(major.price * quoted), currency: priceCurrency };
  }

  let fxRate: number | null = null;
  let fx: Rec | null = null;
  if (priceCurrency == null || priceCurrency === currency) {
    fxRate = 1;
  } else {
    // This date's currency: its own series, else the main series when it is for this currency.
    const series = input.fxByCurrency?.[currency] ?? (input.fx && input.fx.pair.startsWith(currency) ? input.fx : null);
    const r = series ? rateAt(series.bars, date) : null;
    if (r) {
      fxRate = r.close;
      fx = { pair: series?.pair ?? null, date: r.date, rate: r.close };
    } else {
      fx = { pair: series?.pair ?? `${currency}${priceCurrency}=X`, status: "NOT_AVAILABLE" };
      warnings.push({ code: "FX_NOT_AVAILABLE", message: `No ${currency} to ${priceCurrency} rate within 7 days on or before ${date}; figures in ${currency} are not converted and the multiples are null.`, severity: "warning" });
    }
  }

  const balances = balancesAt(tax, map, currency, date);
  if (balances.status === "OK" && (balances.debt as Rec).basis === "NO_BORROWINGS_TAGGED") {
    warnings.push({ code: "NO_BORROWINGS_TAGGED", message: `The filing that reported the ${balances.balanceDate} balance sheet (${(balances.debt as Rec).accessionNumber}) tags no borrowings at any date, so debt is taken as zero (leases excluded).`, severity: "info" });
  }
  let enterpriseValue: Rec;
  const mcap = marketCap.status === "OK" ? (marketCap.value as number) : null;
  if (mcap == null) enterpriseValue = { status: "MARKET_CAP_NOT_AVAILABLE", value: null };
  else if (balances.status !== "OK") enterpriseValue = { status: "BALANCES_NOT_AVAILABLE", value: null };
  else if (((balances.debt as Rec).status) !== "OK") enterpriseValue = { status: "DEBT_NOT_TAGGED", value: null };
  else if (fxRate == null) enterpriseValue = { status: "FX_NOT_AVAILABLE", value: null };
  else {
    const cash = ((balances.cash as Rec).value as number);
    const sti = ((balances.shortTermInvestments as Rec).value as number | null) ?? 0;
    const debt = ((balances.debt as Rec).value as number);
    const balancesStale = days(String(balances.balanceDate), date) > limits.balancesDays;
    enterpriseValue = balancesStale
      ? { status: "BALANCES_STALE", value: null, balanceDate: balances.balanceDate, currency: priceCurrency }
      : {
          status: "OK",
          value: round(mcap + (debt - cash - sti) * fxRate),
          currency: priceCurrency,
          formula: "market cap + debt - cash - short-term investments (untagged short-term investments are left out)",
          balanceDate: balances.balanceDate,
        };
    if (((balances.shortTermInvestments as Rec).status) !== "OK") {
      warnings.push({ code: "SHORT_TERM_INVESTMENTS_NOT_TAGGED", message: "No short-term investment concept is tagged at the balance date; enterprise value subtracts cash only.", severity: "info" });
    }
    if (balancesStale) {
      warnings.push({ code: "BALANCES_STALE", message: `The latest balance sheet filed by ${date} is as of ${balances.balanceDate}.`, severity: "warning" });
    }
  }

  const revenue = flowBases(factsFor(tax, map.revenue, currency, date));
  const oi = flowBases(factsFor(tax, map.operatingIncome, currency, date));
  const ni = flowBases(factsFor(tax, map.netIncome, currency, date));
  const da = daFor(tax, map, currency, date, oi);
  const only = map.depreciationOnly;
  if (only && (["LTM", "LFY"] as const).some((k) => da[k].value.status === "OK" && da[k].concepts?.length === 1 && da[k].concepts?.[0] === only.depreciation)) {
    warnings.push({ code: "EBITDA_DEPRECIATION_ONLY", message: `No ${only.amortization} is tagged by ${date}, so EBITDA adds ${only.depreciation} alone to operating income.`, severity: "info" });
  }
  const ltmEnd = revenue.LTM.status === "OK" ? String(revenue.LTM.periodEnd) : null;
  if (ltmEnd && days(ltmEnd, date) > limits.resultsDays) {
    warnings.push({ code: "RESULTS_STALE", message: `The latest results in SEC companyfacts filed by ${date} end ${ltmEnd}.`, severity: "warning" });
  }
  const denominators: Rec = {};
  const multiples: Rec = {};
  for (const basis of ["LTM", "LFY"] as const) {
    const e = ebitda(oi[basis], da[basis].value, da[basis].concepts);
    denominators[basis] = { revenue: revenue[basis], ebitda: e, netIncome: ni[basis] };
    const ev = enterpriseValue.status === "OK" ? (enterpriseValue.value as number) : null;
    const withFreshness = (m: Rec, denominator: Rec): Rec => {
      const periodEnd = typeof denominator.periodEnd === "string" ? denominator.periodEnd : null;
      return periodEnd && days(periodEnd, date) > limits.resultsDays
        ? { ...m, value: null, status: "RESULTS_STALE", denominatorPeriodEnd: periodEnd }
        : m;
    };
    multiples[basis] = {
      evToRevenue: withFreshness(multiple(ev, revenue[basis], fxRate, "enterpriseValue", basis, "revenue"), revenue[basis]),
      evToEbitda: withFreshness(multiple(ev, e, fxRate, "enterpriseValue", basis, "ebitda"), e),
      priceToEarnings: withFreshness(multiple(mcap, ni[basis], fxRate, "marketCap", basis, "netIncome"), ni[basis]),
      priceToSales: withFreshness(multiple(mcap, revenue[basis], fxRate, "marketCap", basis, "revenue"), revenue[basis]),
    };
  }
  // The latest annual report filed by the date may be missing from companyfacts (2.5.30, F-015).
  const coverageWarning = latestAnnualCoverageWarning(input.periodicFilings ?? [], input.companyfacts, date);
  if (coverageWarning) warnings.push(coverageWarning);
  // Each figure's age at the date, and whether it is past the point's limit, on the figure itself (2.5.30, F-018):
  // a stale denominator keeps its status, but no longer reads as current to a caller that looks only at it.
  const aged = (r: Rec, field: string, limit: number): Rec =>
    r.status === "OK" && typeof r[field] === "string" ? { ...r, [`${field === "balanceDate" ? "balance" : "period"}AgeDays`]: days(r[field] as string, date), stale: days(r[field] as string, date) > limit } : r;
  for (const basis of ["LTM", "LFY"] as const) {
    const d = denominators[basis] as Rec;
    denominators[basis] = Object.fromEntries(Object.entries(d).map(([k, v]) => [k, aged(v as Rec, "periodEnd", limits.resultsDays)]));
  }
  const coreStatus = marketCap.status === "OK" && enterpriseValue.status === "OK" ? "OK" : "PARTIAL";
  const unavailable: Rec[] = [];
  for (const basis of ["LTM", "LFY"]) {
    for (const name of MULTIPLE_NAMES) {
      const m = (multiples[basis] as Rec)[name] as Rec;
      if (m.status !== "OK") unavailable.push({ basis, multiple: name, status: m.status });
    }
  }
  const requested = 2 * MULTIPLE_NAMES.length;
  return {
    date,
    // OK only when market cap, enterprise value and all eight multiples are usable; coreStatus covers the first two.
    status: coreStatus === "OK" && unavailable.length === 0 ? "OK" : "PARTIAL",
    coreStatus,
    coverage: { multiplesRequested: requested, multiplesAvailable: requested - unavailable.length, unavailable },
    ...cadence,
    price,
    shares,
    marketCap,
    fx,
    balances: aged(balances, "balanceDate", limits.balancesDays),
    enterpriseValue,
    denominators,
    multiples,
    warnings,
  };
}

/**
 * Whether the latest annual report filed by asOf is a 20-F or 40-F: a foreign private issuer, whose quote may
 * be for ADSs. Judged per date, so a filer that changed regime keeps its earlier cadence at earlier dates.
 */
export function foreignFiler(companyfacts: unknown, asOf = "9999-12-31"): boolean {
  const { taxonomy } = taxonomyOf(companyfacts);
  const currency = reportingCurrencyAt(companyfacts, asOf);
  if (!taxonomy || !currency) return false;
  const tax = ((((companyfacts ?? {}) as Rec).facts as Rec)[taxonomy] ?? null) as Rec | null;
  const annual = factsFor(tax, (taxonomy === "ifrs-full" ? IFRS : US_GAAP).revenue, currency, asOf).filter(isAnnual).sort((a, b) => cmp(a.end, b.end) || cmp(a.filed, b.filed));
  return annual.length > 0 && /^(?:20|40)-F/.test(annual[annual.length - 1].form);
}

/** The newest share count companyfacts holds (cover page, balance sheet or weighted average), for the ADS ratio. */
export function latestShareCount(companyfacts: unknown): number | null {
  const { taxonomy } = taxonomyOf(companyfacts);
  if (!taxonomy) return null;
  const facts = (((companyfacts ?? {}) as Rec).facts ?? {}) as Rec;
  const shares = sharesAt((facts.dei ?? null) as Rec | null, (facts[taxonomy] ?? null) as Rec | null, taxonomy === "ifrs-full" ? IFRS : US_GAAP, "9999-12-31");
  return shares ? (shares.value as number) : null;
}

export const MAX_DATES = 12;

/** Requested dates, or the latest close and its anniversaries over five years. */
export function valuationDates(requested: string[] | null, latestTradingDate: string | null): { dates: string[]; error: string | null } {
  if (requested && requested.length > 0) {
    if (requested.length > MAX_DATES) return { dates: [], error: `At most ${MAX_DATES} dates.` };
    for (const d of requested) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(d) || Number.isNaN(Date.parse(`${d}T00:00:00Z`))) return { dates: [], error: `Invalid date ${d}; use YYYY-MM-DD.` };
    }
    return { dates: [...new Set(requested)].sort(), error: null };
  }
  if (!latestTradingDate) return { dates: [], error: "No latest close to anchor default dates." };
  const y = Number(latestTradingDate.slice(0, 4));
  const md = latestTradingDate.slice(4);
  const leap = (n: number) => (n % 4 === 0 && n % 100 !== 0) || n % 400 === 0;
  const out: string[] = [];
  for (let k = 5; k >= 1; k--) {
    // 29 February falls back to the 28th in years without it.
    out.push(md === "-02-29" && !leap(y - k) ? `${y - k}-02-28` : `${y - k}${md}`);
  }
  out.push(latestTradingDate);
  return { dates: out, error: null };
}

export function historicalValuation(input: ValuationHistoryInput): Rec {
  const { taxonomy, currency } = taxonomyOf(input.companyfacts);
  const base = {
    ticker: input.ticker.toUpperCase(),
    basis: "POINT_IN_TIME_SEC_FACTS",
    taxonomy,
    reportingCurrency: currency,
    priceCurrency: input.priceCurrency ? majorPrice(1, input.priceCurrency).currency : null,
    adsRatio: input.adsRatio,
  };
  if (!taxonomy || !currency) {
    return { ...base, status: "FUNDAMENTALS_NOT_AVAILABLE", points: [], notes: ["No us-gaap or ifrs-full revenue facts are in companyfacts."], ...AUTHORITY_BOUNDARY };
  }
  const points = input.dates.map((d) => valuationAtDate(input, d, taxonomy, reportingCurrencyAt(input.companyfacts, d) ?? currency));
  const currencies = [...new Set(points.map((p) => String(p.reportingCurrency)))];
  const cadences = [...new Set(points.map((p) => String(p.secCompanyfactsCadence)))].sort();
  const limitsByCadence: Rec = {};
  for (const p of points) limitsByCadence[String(p.secCompanyfactsCadence)] = p.stalenessLimitsDays;
  return {
    ...base,
    secCompanyfactsCadence: cadences.length === 1 ? cadences[0] : "MIXED",
    stalenessLimitsDays: limitsByCadence,
    ...(currencies.length > 1 ? { reportingCurrencies: currencies } : {}),
    status: points.every((p) => p.status === "OK") ? "OK" : "PARTIAL",
    coreStatus: points.every((p) => p.coreStatus === "OK") ? "OK" : "PARTIAL",
    points,
    notes: [
      "Each date uses only SEC facts filed on or before it; the price is that day's close (or the last within 7 days) with later split adjustments undone.",
      "Shares are a point-in-time count: the undimensioned cover-page or balance-sheet count, else the per-class cover-page counts of the latest periodic filing summed. A weighted-average count is shown but never sets market cap.",
      "LTM = last fiscal year + current year-to-date - prior year-to-date, from the filings named in components; LFY is the last reported fiscal year.",
      "EBITDA is computed as operating income plus depreciation and amortization; it is not a reported figure.",
      "Balances are read at the latest cash balance date filed by the date; a concept not tagged at that date is not carried forward.",
      "Multiples with a missing or non-positive denominator are null with a status; none is selected or weighted.",
      "status is OK only when market cap, enterprise value and all eight multiples are usable; coreStatus covers market cap and enterprise value, and coverage lists each unavailable multiple.",
      "secCompanyfactsCadence is the cadence of the periodic facts SEC companyfacts holds as of each date (ANNUAL for 20-F/40-F filers), not the issuer's disclosure cadence; interim 6-K, IR and exchange disclosures are outside this engine.",
    ],
    ...AUTHORITY_BOUNDARY,
  };
}

const MULTIPLE_KEYS = MULTIPLE_NAMES;

function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const s = [...values].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return round(s.length % 2 === 1 ? s[mid] : (s[mid - 1] + s[mid]) / 2, 2);
}

/** Unweighted medians of the peers' multiples on each date and basis, with the peers counted. */
export function peerMedians(dates: string[], peers: Rec[]): Rec[] {
  return dates.map((date) => {
    const out: Rec = { date };
    for (const basis of ["LTM", "LFY"]) {
      const row: Rec = {};
      for (const key of MULTIPLE_KEYS) {
        const values: number[] = [];
        const tickers: string[] = [];
        for (const peer of peers) {
          const point = ((peer.points ?? []) as Rec[]).find((p) => p.date === date);
          const m = (((point?.multiples as Rec | undefined)?.[basis] as Rec | undefined)?.[key] as Rec | undefined);
          if (m && m.status === "OK" && typeof m.value === "number") {
            values.push(m.value);
            tickers.push(String(peer.ticker));
          }
        }
        row[key] = { median: median(values), count: values.length, tickers };
      }
      out[basis] = row;
    }
    return out;
  });
}
