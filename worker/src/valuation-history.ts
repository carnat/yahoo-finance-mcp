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
import { round } from "./capital-structure.js";
import { REVENUE_CONCEPTS } from "./sec-facts.js";
import { majorPrice } from "./valuation.js";

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
  /** Ordinary shares per quoted share (ADS), when the quote is for depositary shares. */
  adsRatio: number | null;
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

function sharesAt(dei: Rec | null, tax: Rec | null, map: TaxonomyMap, asOf: string): Rec | null {
  const latest = (fs: Fact[]): Fact | null => {
    const sorted = [...fs].sort((a, b) => cmp(a.end, b.end) || days(b.start ?? b.end, b.end) - days(a.start ?? a.end, a.end));
    return sorted.length > 0 ? sorted[sorted.length - 1] : null;
  };
  const cover = latest(factsFor(dei, ["EntityCommonStockSharesOutstanding"], "shares", asOf).filter((f) => f.start == null));
  if (cover) return { value: cover.val, asOf: cover.end, basis: "COVER_PAGE", ...component(cover) };
  const balance = latest(factsFor(tax, map.sharesInstant, "shares", asOf).filter((f) => f.start == null));
  if (balance) return { value: balance.val, asOf: balance.end, basis: "BALANCE_SHEET", ...component(balance) };
  const weighted = latest(factsFor(tax, map.sharesWeighted, "shares", asOf).filter((f) => f.start != null));
  if (weighted) return { value: weighted.val, asOf: weighted.end, basis: "WEIGHTED_AVERAGE_BASIC", ...component(weighted) };
  return null;
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

function multiple(numerator: number | null, denominator: Rec | null, fx: number | null, numeratorName: string, basis: string, metric: string): Rec {
  const den = denominator && denominator.status === "OK" ? (denominator.value as number) : null;
  const ref = { numerator: numeratorName, denominator: metric, basis, denominatorPeriodEnd: denominator?.periodEnd ?? null };
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
  const pick = (key: "LFY" | "LTM") => {
    const ok = groups.filter((g) => g.bases[key].status === "OK");
    const aligned = ok.find((g) => g.bases[key].periodEnd === oi[key].periodEnd) ?? ok[0];
    return aligned ? { value: aligned.bases[key], concepts: aligned.group } : { value: { status: "NOT_AVAILABLE", value: null, reason: "NOT_TAGGED" }, concepts: null };
  };
  return { LFY: pick("LFY"), LTM: pick("LTM") };
}

/** Market value, balances, denominators and multiples as they stood on one date. */
/**
 * Staleness limits in days. Annual-only (20-F/40-F) filers publish balances and
 * share counts once a year, about four months after year end, so a quarterly
 * filer's 200-day balance limit would leave their EV null for most of each year.
 */
export function stalenessLimits(companyfacts: unknown): { cadence: string; shareCountDays: number; balancesDays: number; resultsDays: number } {
  return foreignFiler(companyfacts)
    ? { cadence: "ANNUAL", shareCountDays: 500, balancesDays: 500, resultsDays: 500 }
    : { cadence: "QUARTERLY", shareCountDays: 400, balancesDays: 200, resultsDays: 500 };
}

export function valuationAtDate(input: ValuationHistoryInput, date: string, taxonomy: string, currency: string): Rec {
  const limits = stalenessLimits(input.companyfacts);
  const facts = ((input.companyfacts ?? {}) as Rec).facts as Rec;
  const tax = (facts[taxonomy] ?? null) as Rec | null;
  const dei = (facts.dei ?? null) as Rec | null;
  const map = taxonomy === "ifrs-full" ? IFRS : US_GAAP;
  const warnings: Rec[] = [];

  const bar = rateAt(input.bars, date);
  if (!bar) return { date, status: "PRICE_UNAVAILABLE", warnings: [{ code: "PRICE_UNAVAILABLE", message: `No close within 7 days on or before ${date}.`, severity: "warning" }] };
  // Yahoo's closes are adjusted for every later split; undo that so the price matches the share count reported then.
  const laterSplits = input.splits.filter((s) => s.date > bar.date && s.ratio > 0);
  const splitFactor = laterSplits.reduce((f, s) => f * s.ratio, 1);
  const major = majorPrice(bar.close * splitFactor, input.priceCurrency);
  const priceCurrency = major.currency;
  const price = { tradingDate: bar.date, closeAsAdjusted: bar.close, laterSplitFactor: splitFactor, close: round(major.price, 4), currency: priceCurrency, laterSplits };

  const shares = sharesAt(dei, tax, map, date);
  let marketCap: Rec;
  if (!shares) {
    marketCap = { status: "SHARES_NOT_AVAILABLE", value: null };
  } else {
    if (shares.basis === "WEIGHTED_AVERAGE_BASIC") {
      warnings.push({ code: "SHARE_COUNT_WEIGHTED_AVERAGE", message: "No undimensioned cover-page or balance-sheet share count is in companyfacts (companies with several share classes tag them per class), so the latest weighted-average basic count is used; it may cover only the listed class.", severity: "warning" });
    }
    const shareCountStale = days(String(shares.asOf), date) > limits.shareCountDays;
    if (shareCountStale) {
      warnings.push({ code: "SHARE_COUNT_STALE", message: `The latest share count filed by ${date} is as of ${shares.asOf}.`, severity: "warning" });
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
    const r = input.fx ? rateAt(input.fx.bars, date) : null;
    if (r) {
      fxRate = r.close;
      fx = { pair: input.fx?.pair ?? null, date: r.date, rate: r.close };
    } else {
      fx = { pair: input.fx?.pair ?? null, status: "NOT_AVAILABLE" };
      warnings.push({ code: "FX_NOT_AVAILABLE", message: `No ${currency} to ${priceCurrency} rate within 7 days on or before ${date}; figures in ${currency} are not converted and the multiples are null.`, severity: "warning" });
    }
  }

  const balances = balancesAt(tax, map, currency, date);
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
  const allOk = marketCap.status === "OK" && enterpriseValue.status === "OK";
  return {
    date,
    status: allOk ? "OK" : "PARTIAL",
    price,
    shares,
    marketCap,
    fx,
    balances,
    enterpriseValue,
    denominators,
    multiples,
    warnings,
  };
}

/** Whether the latest annual report is a 20-F or 40-F: a foreign private issuer, whose quote may be for ADSs. */
export function foreignFiler(companyfacts: unknown): boolean {
  const { taxonomy, currency } = taxonomyOf(companyfacts);
  if (!taxonomy || !currency) return false;
  const tax = ((((companyfacts ?? {}) as Rec).facts as Rec)[taxonomy] ?? null) as Rec | null;
  const annual = factsFor(tax, (taxonomy === "ifrs-full" ? IFRS : US_GAAP).revenue, currency, "9999-12-31").filter(isAnnual).sort((a, b) => cmp(a.end, b.end));
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
  const points = input.dates.map((d) => valuationAtDate(input, d, taxonomy, currency));
  const limits = stalenessLimits(input.companyfacts);
  return {
    ...base,
    reportingCadence: limits.cadence,
    stalenessLimitsDays: { shareCount: limits.shareCountDays, balances: limits.balancesDays, results: limits.resultsDays },
    status: points.every((p) => p.status === "OK") ? "OK" : "PARTIAL",
    points,
    notes: [
      "Each date uses only SEC facts filed on or before it; the price is that day's close (or the last within 7 days) with later split adjustments undone.",
      "LTM = last fiscal year + current year-to-date - prior year-to-date, from the filings named in components; LFY is the last reported fiscal year.",
      "EBITDA is computed as operating income plus depreciation and amortization; it is not a reported figure.",
      "Balances are read at the latest cash balance date filed by the date; a concept not tagged at that date is not carried forward.",
      "Multiples with a missing or non-positive denominator are null with a status; none is selected or weighted.",
    ],
    ...AUTHORITY_BOUNDARY,
  };
}

const MULTIPLE_KEYS = ["evToRevenue", "evToEbitda", "priceToEarnings", "priceToSales"];

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
