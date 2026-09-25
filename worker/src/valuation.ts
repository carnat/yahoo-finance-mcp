// Valuation snapshot and peer multiples (2.4.0).
//
// Mechanical context only: enterprise value from a price, a share count and
// disclosed balances, and multiples of that value over reported results and
// Yahoo's consensus estimates. Nothing here produces an implied price or a
// forecast, and no figure is back-solved from a price target.
//
// yfmcp/valuation.py mirrors this file; scripts/test_valuation.py requires
// identical output from both.

import { round } from "./capital-structure.js";

export type Estimate = {
  period: string;
  revenueAvg: number | null;
  revenueAnalysts: number | null;
  epsAvg: number | null;
  epsAnalysts: number | null;
};

export type MarketInputs = {
  ticker: string;
  name: string | null;
  currency: string | null;
  financialCurrency: string | null;
  price: number | null;
  priceTime: string | null;
  sharesOutstanding: number | null;
  impliedSharesOutstanding: number | null;
  marketCap: number | null;
  totalCash: number | null;
  totalDebt: number | null;
  ttmRevenue: number | null;
  ttmEbitda: number | null;
  grossMarginPct: number | null;
  trailingEps: number | null;
  estimates: Estimate[];
};

function rawNum(value: unknown): number | null {
  const v = value && typeof value === "object" ? (value as Record<string, unknown>).raw : value;
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

/** Yahoo quoteSummary (price, financialData, defaultKeyStatistics, earningsTrend) as valuation inputs. */
export function marketInputsFromQuoteSummary(ticker: string, result: Record<string, unknown>): MarketInputs {
  const price = (result.price ?? {}) as Record<string, unknown>;
  const fd = (result.financialData ?? {}) as Record<string, unknown>;
  const ks = (result.defaultKeyStatistics ?? {}) as Record<string, unknown>;
  const trend = (((result.earningsTrend ?? {}) as Record<string, unknown>).trend ?? []) as Record<string, unknown>[];
  const time = rawNum(price.regularMarketTime);
  const gross = rawNum(fd.grossMargins);
  return {
    ticker: ticker.toUpperCase(),
    name: text(price.longName) ?? text(price.shortName),
    currency: text(price.currency),
    financialCurrency: text(fd.financialCurrency),
    price: rawNum(price.regularMarketPrice),
    priceTime: time != null ? new Date(time * 1000).toISOString() : null,
    sharesOutstanding: rawNum(ks.sharesOutstanding),
    impliedSharesOutstanding: rawNum(ks.impliedSharesOutstanding),
    marketCap: rawNum(price.marketCap),
    totalCash: rawNum(fd.totalCash),
    totalDebt: rawNum(fd.totalDebt),
    ttmRevenue: rawNum(fd.totalRevenue),
    ttmEbitda: rawNum(fd.ebitda),
    grossMarginPct: gross != null ? round(gross * 100, 2) : null,
    trailingEps: rawNum(ks.trailingEps),
    estimates: trend
      .filter((t) => typeof t.period === "string")
      .map((t) => {
        const rev = (t.revenueEstimate ?? {}) as Record<string, unknown>;
        const eps = (t.earningsEstimate ?? {}) as Record<string, unknown>;
        return {
          period: String(t.period),
          revenueAvg: rawNum(rev.avg),
          revenueAnalysts: rawNum(rev.numberOfAnalysts),
          epsAvg: rawNum(eps.avg),
          epsAnalysts: rawNum(eps.numberOfAnalysts),
        };
      }),
  };
}

// Listings quoted in a minor unit, with the major currency their financials use.
const MINOR_UNITS: Record<string, [string, number]> = { GBp: ["GBP", 100], GBX: ["GBP", 100], ZAc: ["ZAR", 100], ILA: ["ILS", 100] };

export function majorPrice(price: number, currency: string | null): { price: number; currency: string | null } {
  const unit = currency ? MINOR_UNITS[currency] : undefined;
  return unit ? { price: price / unit[1], currency: unit[0] } : { price, currency };
}

const PERIOD_LABELS: Record<string, string> = { "0y": "current_fiscal_year", "+1y": "next_fiscal_year" };

function estimate(market: MarketInputs, period: string): Estimate | null {
  return market.estimates.find((e) => e.period === period) ?? null;
}

function ratio(numerator: number | null, denominator: number | null, digits = 2): number | null {
  return numerator != null && denominator != null && denominator > 0 ? round(numerator / denominator, digits) : null;
}

type MultipleRow = Record<string, unknown>;

/** EV/Revenue, EV/EBITDA and P/E over trailing results and the current and next fiscal-year consensus. */
function multiples(ev: number | null, price: number | null, market: MarketInputs, comparable: boolean): MultipleRow[] {
  const rows: MultipleRow[] = [];
  const add = (metric: string, basis: string, numerator: number | null, denominator: number | null, source: string, analysts: number | null) => {
    let multiple: number | null = null;
    let note: string | null = null;
    if (!comparable) note = "Financials are reported in a different currency from the quote; not computed.";
    else if (numerator == null) note = "Numerator unavailable.";
    else if (denominator == null) note = "Denominator unavailable.";
    else if (denominator <= 0) note = "Denominator is zero or negative; a multiple is not meaningful.";
    else multiple = ratio(numerator, denominator);
    rows.push({ metric, basis, multiple, denominator, denominatorSource: source, analysts, note });
  };
  add("EV/Revenue", "trailing_12_months", ev, market.ttmRevenue, "yahoo_financial_data_ttm", null);
  for (const period of ["0y", "+1y"]) {
    const e = estimate(market, period);
    add("EV/Revenue", PERIOD_LABELS[period], ev, e ? e.revenueAvg : null, "yahoo_consensus_estimate", e ? e.revenueAnalysts : null);
  }
  add("EV/EBITDA", "trailing_12_months", ev, market.ttmEbitda, "yahoo_financial_data_ttm", null);
  add("P/E", "trailing_12_months", price, market.trailingEps, "yahoo_trailing_eps", null);
  for (const period of ["0y", "+1y"]) {
    const e = estimate(market, period);
    add("P/E", PERIOD_LABELS[period], price, e ? e.epsAvg : null, "yahoo_consensus_estimate", e ? e.epsAnalysts : null);
  }
  return rows;
}

function growth(market: MarketInputs): Record<string, number | null> {
  const fy0 = estimate(market, "0y");
  const fy1 = estimate(market, "+1y");
  const pct = (a: number | null, b: number | null) => (a != null && b != null && b > 0 ? round((a / b - 1) * 100, 2) : null);
  return {
    currentFiscalYearVsTtmPct: pct(fy0 ? fy0.revenueAvg : null, market.ttmRevenue),
    nextVsCurrentFiscalYearPct: pct(fy1 ? fy1.revenueAvg : null, fy0 ? fy0.revenueAvg : null),
  };
}

// Yahoo's implied count covers every class (e.g. ASTS's exchangeable Class B/C);
// it replaces the listed-class count when it is materially larger.
const IMPLIED_SHARES_MIN_RATIO = 1.02;

function yahooShares(market: MarketInputs): { shares: number | null; basis: string | null } {
  const listed = market.sharesOutstanding;
  const implied = market.impliedSharesOutstanding;
  if (implied != null && (listed == null || implied > listed * IMPLIED_SHARES_MIN_RATIO)) return { shares: implied, basis: "yahoo_implied_shares_outstanding" };
  if (listed != null) return { shares: listed, basis: "yahoo_shares_outstanding" };
  return { shares: null, basis: market.marketCap != null ? "yahoo_market_cap" : null };
}

/** Yahoo-basis equity and enterprise value, as every peer row is computed. */
function yahooBasis(market: MarketInputs, price: number | null): { marketCap: number | null; enterpriseValue: number | null; shares: number | null; shareBasis: string | null } {
  const { shares, basis } = yahooShares(market);
  if (price == null) return { marketCap: null, enterpriseValue: null, shares, shareBasis: basis };
  const major = majorPrice(price, market.currency);
  const marketCap = shares != null ? major.price * shares : market.marketCap;
  const enterpriseValue = marketCap != null && market.totalDebt != null && market.totalCash != null
    ? marketCap + market.totalDebt - market.totalCash
    : null;
  return { marketCap: marketCap != null ? round(marketCap) : null, enterpriseValue: enterpriseValue != null ? round(enterpriseValue) : null, shares, shareBasis: basis };
}

function comparableCurrency(market: MarketInputs): boolean {
  const quote = market.currency ? (MINOR_UNITS[market.currency]?.[0] ?? market.currency) : null;
  return !market.financialCurrency || !quote || market.financialCurrency === quote;
}

export type SnapshotInput = {
  ticker: string;
  market: MarketInputs;
  suppliedPrice: number | null;
  bridge: Record<string, unknown> | null;
  capital: Record<string, unknown> | null;
  secWarnings: Record<string, unknown>[];
};

function num(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/** Price, diluted shares, disclosed balances and multiples for one ticker. */
export function valuationSnapshot(input: SnapshotInput): Record<string, unknown> {
  const { market, bridge, capital } = input;
  const warnings: Record<string, unknown>[] = [...input.secWarnings];
  const quote = input.suppliedPrice ?? market.price;
  if (quote == null) {
    return { ticker: input.ticker, status: "PRICE_UNAVAILABLE", code: "PRICE_UNAVAILABLE", message: "No price was supplied and Yahoo returned none.", warnings };
  }
  const major = majorPrice(quote, market.currency);
  const bridgeCore = (bridge?.bridge ?? null) as Record<string, unknown> | null;
  const bridgeBasic = (bridge?.basicShares ?? null) as Record<string, unknown> | null;
  const basic = num(bridgeBasic?.shares) ?? market.sharesOutstanding;
  const diluted = num(bridgeCore?.dilutedSharesAtPrice);
  const shareBasis = num(bridgeBasic?.shares) != null ? "sec_cover_page" : (market.sharesOutstanding != null ? "yahoo_shares_outstanding" : null);
  const valueShares = diluted ?? basic;

  // Balances: the filing's period-end values when read, else Yahoo's totals.
  const balances = (capital?.balances ?? null) as Record<string, unknown> | null;
  const secBalances = balances != null && num(balances.totalDebt) != null && num(balances.cashAndEquivalents) != null;
  const cash = secBalances ? num(balances!.cashAndEquivalents)! : market.totalCash;
  const shortTerm = secBalances ? (num(balances!.shortTermInvestments) ?? 0) : 0;
  const debt = secBalances ? num(balances!.totalDebt)! : market.totalDebt;
  const balanceBasis = secBalances ? "sec_filing_period_end" : (market.totalDebt != null ? "yahoo_financial_data" : null);
  if (!secBalances) {
    warnings.push({ code: "YAHOO_BALANCES", message: "Cash and debt are Yahoo's totals (debt can include leases), not the filing's period-end balances.", severity: "info" });
  }

  // Convertibles counted as shares leave the debt, so they are not counted twice.
  let convertibleAdjustment = 0;
  const convertibles = ((bridge?.components ?? []) as Record<string, unknown>[]).find((c) => c.component === "convertible_debt");
  for (const inst of ((convertibles?.instruments ?? []) as Record<string, unknown>[])) {
    if ((num(inst.incrementalShares) ?? 0) > 0) convertibleAdjustment += num(inst.principal) ?? 0;
  }
  if (debt != null) convertibleAdjustment = Math.min(convertibleAdjustment, debt);

  const equityValue = valueShares != null ? major.price * valueShares : null;
  const enterpriseValue = equityValue != null && debt != null && cash != null
    ? equityValue + debt - convertibleAdjustment - cash - shortTerm
    : null;
  if (enterpriseValue == null) warnings.push({ code: "ENTERPRISE_VALUE_INCOMPLETE", message: "Shares, cash or debt were unavailable, so enterprise value was not computed.", severity: "warning" });
  const comparable = comparableCurrency(market);
  if (!comparable) warnings.push({ code: "FINANCIAL_CURRENCY_MISMATCH", message: `Financials are in ${market.financialCurrency}; the quote is in ${market.currency}.`, severity: "warning" });

  const atm = (bridge?.atmProgram ?? null) as Record<string, unknown> | null;
  const yahoo = yahooBasis(market, quote);
  const sharesDiffPct = basic != null && market.sharesOutstanding != null && market.sharesOutstanding > 0 && shareBasis === "sec_cover_page"
    ? round((basic / market.sharesOutstanding - 1) * 100, 2)
    : null;
  return {
    ticker: input.ticker,
    name: market.name,
    status: enterpriseValue != null ? "COMPUTED" : "PARTIAL",
    basis: "MECHANICAL_VALUATION_CONTEXT",
    decisionUse: "CONTEXT_ONLY_NOT_A_PRICE_TARGET",
    price: {
      amount: quote,
      currency: market.currency,
      majorUnitAmount: major.price,
      majorCurrency: major.currency,
      source: input.suppliedPrice != null ? "caller_supplied" : "yahoo_regular_market",
      asOf: input.suppliedPrice != null ? null : market.priceTime,
    },
    shares: {
      basic,
      diluted,
      usedForValue: valueShares,
      basis: shareBasis,
      dilutionPctAtPrice: num(bridgeCore?.dilutionPctAtPrice),
      bridgeStatus: bridge?.status ?? null,
      yahooSharesOutstanding: market.sharesOutstanding,
      yahooImpliedSharesOutstanding: market.impliedSharesOutstanding,
      secVsYahooSharesDiffPct: sharesDiffPct,
    },
    balances: {
      cash,
      shortTermInvestments: secBalances ? shortTerm : null,
      totalDebt: debt,
      convertibleDebtCountedAsShares: round(convertibleAdjustment),
      basis: balanceBasis,
      periodEnd: secBalances ? (capital?.periodEnd ?? null) : null,
    },
    equityValue: equityValue != null ? round(equityValue) : null,
    enterpriseValue: enterpriseValue != null ? round(enterpriseValue) : null,
    enterpriseValueFormula: "price x diluted shares + total debt - convertible debt counted as shares - cash - short-term investments",
    peerComparableBasis: { ...yahoo, note: "Price x Yahoo shares (the implied all-class count when larger) + Yahoo total debt - Yahoo total cash: the basis compare_peer_valuations uses for every peer." },
    multiples: multiples(enterpriseValue, major.price, market, comparable),
    revenueGrowth: growth(market),
    grossMarginPct: market.grossMarginPct,
    atmCapacity: atm ? {
      remainingCapacity: atm.remainingCapacityUsd ?? null,
      programSize: atm.programSizeUsd ?? null,
      potentialSharesAtPrice: atm.potentialShares ?? null,
      note: "Possible issuance at the company's discretion; not included in the share count above.",
    } : null,
    methodology: [
      "Enterprise value uses the dilution bridge's share count at this price and the filing's period-end cash and debt, falling back to Yahoo when no SEC filing is read.",
      "Multiples divide by Yahoo's trailing results and consensus estimates as published; the analyst counts say how many estimates stand behind each.",
      "This is context for a valuation, not one: no implied price, no forecast, and nothing back-solved from price targets.",
    ],
    warnings,
  };
}

function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

/** One peer row on the Yahoo basis. */
export function peerRow(market: MarketInputs): Record<string, unknown> {
  const yahoo = yahooBasis(market, market.price);
  const comparable = comparableCurrency(market);
  const major = market.price != null ? majorPrice(market.price, market.currency) : null;
  const rows = multiples(yahoo.enterpriseValue, major ? major.price : null, market, comparable);
  const value = (metric: string, basis: string) => rows.find((r) => r.metric === metric && r.basis === basis)?.multiple ?? null;
  return {
    ticker: market.ticker,
    name: market.name,
    price: market.price,
    currency: market.currency,
    marketCap: yahoo.marketCap,
    enterpriseValue: yahoo.enterpriseValue,
    sharesUsed: yahoo.shares,
    shareBasis: yahoo.shareBasis,
    evToRevenueTtm: value("EV/Revenue", "trailing_12_months"),
    evToRevenueCurrentFy: value("EV/Revenue", "current_fiscal_year"),
    evToRevenueNextFy: value("EV/Revenue", "next_fiscal_year"),
    evToEbitdaTtm: value("EV/EBITDA", "trailing_12_months"),
    peCurrentFy: value("P/E", "current_fiscal_year"),
    peNextFy: value("P/E", "next_fiscal_year"),
    revenueGrowthNextFyPct: growth(market).nextVsCurrentFiscalYearPct,
    grossMarginPct: market.grossMarginPct,
    comparableCurrency: comparable,
  };
}

const PEER_METRICS = ["evToRevenueTtm", "evToRevenueCurrentFy", "evToRevenueNextFy", "evToEbitdaTtm", "peCurrentFy", "peNextFy", "revenueGrowthNextFyPct", "grossMarginPct"];

/** Multiples for a peer set on one basis, with peer medians and the subject against them. */
export function peerValuations(subject: string | null, markets: MarketInputs[], errors: Record<string, unknown>[]): Record<string, unknown> {
  const rows = markets.map(peerRow);
  const subjectU = subject ? subject.toUpperCase() : null;
  const peers = rows.filter((r) => r.ticker !== subjectU);
  const summary: Record<string, unknown> = {};
  const versus: Record<string, unknown> = {};
  const subjectRow = rows.find((r) => r.ticker === subjectU) ?? null;
  for (const metric of PEER_METRICS) {
    const values = peers.map((r) => num(r[metric])).filter((v): v is number => v != null);
    const med = median(values);
    summary[metric] = {
      median: med != null ? round(med, 2) : null,
      min: values.length > 0 ? Math.min(...values) : null,
      max: values.length > 0 ? Math.max(...values) : null,
      count: values.length,
    };
    const own = subjectRow ? num(subjectRow[metric]) : null;
    if (subjectRow) {
      versus[metric] = {
        subject: own,
        peerMedian: med != null ? round(med, 2) : null,
        premiumPct: own != null && med != null && med > 0 && !metric.endsWith("Pct") ? round((own / med - 1) * 100, 2) : null,
      };
    }
  }
  return {
    subject: subjectU,
    basis: "YAHOO_MARKET_AND_CONSENSUS",
    decisionUse: "CONTEXT_ONLY_NOT_A_PRICE_TARGET",
    rows,
    peerSummary: summary,
    subjectVsPeerMedian: subjectRow ? versus : null,
    notes: [
      "Every row uses the same basis: Yahoo price x shares + total debt - total cash, over Yahoo's trailing results and consensus. Shares are Yahoo's implied all-class count when it is materially larger than the listed class (shareBasis says which).",
      "Peer medians exclude the subject. A premium or discount is context, not a signal; peers differ in growth, margins and risk.",
      "Multiples on zero or negative denominators are left empty rather than shown as meaningless numbers.",
    ],
    errors,
  };
}
