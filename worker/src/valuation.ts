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
  /** Yahoo's latest balance-sheet quarter end (YYYY-MM-DD), for the freshness check. */
  mostRecentQuarter: string | null;
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
  const quarter = rawNum(ks.mostRecentQuarter);
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
    mostRecentQuarter: quarter != null ? new Date(quarter * 1000).toISOString().slice(0, 10) : null,
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
  // Cash and debt are in the financial currency; adding them to a quote in
  // another currency is dimensionally invalid (TSM: USD ADR, TWD balances).
  const enterpriseValue = marketCap != null && market.totalDebt != null && market.totalCash != null && comparableCurrency(market)
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

// Cash plus investments more than 10% above Yahoo's total, with cash alone within 2% of it.
const SEC_YAHOO_EXCESS = 0.1;
const SEC_YAHOO_CASH_MATCH = 0.02;
// Yahoo's cash and short-term investments more than 5% above the filing's.
const SEC_YAHOO_SHORTFALL = 0.05;
// Filing balances more than 45 days older than Yahoo's latest quarter are stale.
const STALE_BALANCE_DAYS = 45;
// Yahoo's totals are compared with the filing's only for the same quarter end.
const SAME_QUARTER_DAYS = 7;
// SEC ordinary shares within 2% of a whole multiple (2x or more) of Yahoo's
// count are ADR-quoted: Yahoo counts depositary shares (TSM: 5 per ADS).
const ADS_MIN_RATIO = 1.9;
const ADS_RATIO_TOLERANCE = 0.02;
// Otherwise counts more than 1.5x apart are on different bases; Yahoo's is used.
const SHARE_BASIS_MAX_RATIO = 1.5;

/** Ordinary shares per quoted share when the two counts are a whole multiple apart, else null. */
export function adsRatio(secShares: number, quotedShares: number): number | null {
  if (!(secShares > 0) || !(quotedShares > 0)) return null;
  const r = secShares / quotedShares;
  if (r >= ADS_MIN_RATIO) {
    const n = Math.floor(r + 0.5);
    return Math.abs(r - n) <= ADS_RATIO_TOLERANCE * n ? n : null;
  }
  if (r <= 1 / ADS_MIN_RATIO) {
    const k = Math.floor(1 / r + 0.5);
    return Math.abs(1 / r - k) <= ADS_RATIO_TOLERANCE * k ? 1 / k : null;
  }
  return null;
}

function daysBetween(earlier: string, later: string): number {
  return Math.round((Date.parse(`${later}T00:00:00Z`) - Date.parse(`${earlier}T00:00:00Z`)) / 86_400_000);
}

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
  const comparable = comparableCurrency(market);
  const bridgeCore = (bridge?.bridge ?? null) as Record<string, unknown> | null;
  const bridgeBasic = (bridge?.basicShares ?? null) as Record<string, unknown> | null;
  const secBasic = num(bridgeBasic?.shares);
  const quoted = yahooShares(market);
  // SEC counts ordinary shares; the quote may be for depositary shares.
  let ratio: number | null = null;
  let secUsable = secBasic != null;
  // Only foreign issuers (20-F, 40-F) have depositary shares; a domestic
  // dual-class filer near 2x is not read as an ADR.
  const basicSource = (bridgeBasic?.source ?? null) as Record<string, unknown> | null;
  const foreignFiler = typeof basicSource?.filingType === "string" && /^(?:20|40)-F/.test(basicSource.filingType);
  if (secBasic != null && quoted.shares != null && quoted.shares > 0) {
    ratio = foreignFiler ? adsRatio(secBasic, quoted.shares) : null;
    const r = secBasic / quoted.shares;
    if (ratio != null) {
      warnings.push({ code: "ADR_RATIO_APPLIED", message: `The filing counts ${secBasic} ordinary shares; Yahoo counts ${quoted.shares} quoted shares, ${ratio} ordinary shares each. SEC share counts are divided by ${ratio} before the price is applied.`, severity: "info" });
    } else if (r > SHARE_BASIS_MAX_RATIO || r < 1 / SHARE_BASIS_MAX_RATIO) {
      secUsable = false;
      warnings.push({ code: "SHARE_BASIS_MISMATCH", message: `The filing's ${secBasic} shares and Yahoo's ${quoted.shares} differ by more than 1.5x without a whole-number ratio, so Yahoo's count is used.`, severity: "warning" });
    }
  }
  const perQuoted = (shares: number | null) => (shares != null && ratio != null ? round(shares / ratio) : shares);
  const basic = secUsable ? perQuoted(secBasic) : (quoted.shares ?? market.sharesOutstanding);
  const diluted = secUsable ? perQuoted(num(bridgeCore?.dilutedSharesAtPrice)) : null;
  const shareBasis = secUsable ? "sec_cover_page" : (quoted.shares != null ? quoted.basis : null);
  const valueShares = diluted ?? basic;

  // Balances: the filing's period-end values when read, else Yahoo's totals.
  const balances = (capital?.balances ?? null) as Record<string, unknown> | null;
  let secBalances = balances != null && num(balances.totalDebt) != null && num(balances.cashAndEquivalents) != null;
  const secCurrency = balances && typeof balances.currency === "string" ? balances.currency : null;
  if (secBalances && secCurrency && major.currency && secCurrency !== major.currency) {
    secBalances = false;
    warnings.push({ code: "SEC_BALANCES_CURRENCY", message: `The filing's balances are in ${secCurrency}; the quote is in ${major.currency}. They are not used.`, severity: "info" });
  }
  // A filing older than Yahoo's latest quarter (20-F filers such as TSEM and
  // NBIS tag only annual reports) gives way to Yahoo's newer balances.
  const secPeriodEnd = typeof capital?.periodEnd === "string" ? capital.periodEnd : null;
  const staleDays = secBalances && secPeriodEnd && market.mostRecentQuarter ? daysBetween(secPeriodEnd, market.mostRecentQuarter) : null;
  if (staleDays != null && staleDays > STALE_BALANCE_DAYS) {
    if (market.totalCash != null && market.totalDebt != null) {
      secBalances = false;
      warnings.push({ code: "SEC_BALANCES_STALE", message: `The filing's balances are from ${secPeriodEnd}; Yahoo has ${market.mostRecentQuarter}, so Yahoo's newer cash and debt are used (its debt can include leases).`, severity: "warning", secPeriodEnd, yahooMostRecentQuarter: market.mostRecentQuarter });
    } else {
      warnings.push({ code: "SEC_BALANCES_STALE", message: `The filing's balances are from ${secPeriodEnd}, older than Yahoo's latest quarter ${market.mostRecentQuarter}, and Yahoo has no newer cash and debt; enterprise value uses the older balances.`, severity: "warning", secPeriodEnd, yahooMostRecentQuarter: market.mostRecentQuarter });
    }
  }
  const cash = secBalances ? num(balances!.cashAndEquivalents)! : market.totalCash;
  const shortTerm = secBalances ? (num(balances!.shortTermInvestments) ?? 0) : 0;
  const debt = secBalances ? num(balances!.totalDebt)! : market.totalDebt;
  const balanceBasis = secBalances ? "sec_filing_period_end" : (market.totalDebt != null ? "yahoo_financial_data" : null);
  if (!secBalances) {
    warnings.push({ code: "YAHOO_BALANCES", message: "Cash and debt are Yahoo's totals (debt can include leases), not the filing's period-end balances.", severity: "info" });
  }

  // Yahoo's total cash includes short-term investments. When the filing's cash
  // alone matches it but cash plus investments runs well above it, the
  // investments are probably part of cash already (ASTS, 2.4.2). Flagged, not overridden.
  const yahooCash = comparable ? market.totalCash : null;
  if (secBalances && cash != null && shortTerm > 0 && yahooCash != null && yahooCash > 0
    && cash + shortTerm > (1 + SEC_YAHOO_EXCESS) * yahooCash && Math.abs(cash - yahooCash) <= SEC_YAHOO_CASH_MATCH * yahooCash) {
    warnings.push({
      code: "SEC_YAHOO_CASH_MISMATCH",
      message: "The filing's cash alone matches Yahoo's total cash and short-term investments, but cash plus the filing's short-term investments is well above it; the investments may already be inside cash. Check the filing before relying on enterprise value.",
      severity: "warning",
      secCash: cash,
      secShortTermInvestments: shortTerm,
      yahooTotalCash: yahooCash,
    });
  }
  // The opposite: Yahoo's cash and short-term investments well above the
  // filing's, for the same quarter, suggests investments the filing tags under
  // a concept not read (VRT).
  const sameQuarter = secPeriodEnd != null && market.mostRecentQuarter != null && Math.abs(daysBetween(secPeriodEnd, market.mostRecentQuarter)) <= SAME_QUARTER_DAYS;
  if (secBalances && sameQuarter && cash != null && yahooCash != null && yahooCash > (1 + SEC_YAHOO_SHORTFALL) * (cash + shortTerm)) {
    warnings.push({
      code: "SEC_YAHOO_CASH_SHORTFALL",
      message: "Yahoo's cash and short-term investments are more than 5% above the filing's cash plus the investments read from it; an investment line may be tagged under a concept not read. Check the balance sheet before relying on enterprise value.",
      severity: "warning",
      secCash: cash,
      secShortTermInvestments: shortTerm,
      yahooTotalCash: yahooCash,
    });
  }

  // Convertibles counted as shares leave the debt, so they are not counted twice.
  let convertibleAdjustment = 0;
  const convertibles = ((bridge?.components ?? []) as Record<string, unknown>[]).find((c) => c.component === "convertible_debt");
  for (const inst of ((convertibles?.instruments ?? []) as Record<string, unknown>[])) {
    if ((num(inst.incrementalShares) ?? 0) > 0) convertibleAdjustment += num(inst.principal) ?? 0;
  }
  if (debt != null) convertibleAdjustment = Math.min(convertibleAdjustment, debt);

  const equityValue = valueShares != null ? major.price * valueShares : null;
  // Yahoo's balances are in the financial currency; never add them to equity in another.
  const balancesComparable = secBalances || comparable;
  const enterpriseValue = equityValue != null && debt != null && cash != null && balancesComparable
    ? equityValue + debt - convertibleAdjustment - cash - shortTerm
    : null;
  if (!balancesComparable) {
    warnings.push({ code: "ENTERPRISE_VALUE_CURRENCY_MISMATCH", message: `Cash and debt are in ${market.financialCurrency} and the quote is in ${major.currency}; enterprise value is not computed without an exchange rate.`, severity: "warning" });
  } else if (enterpriseValue == null) {
    warnings.push({ code: "ENTERPRISE_VALUE_INCOMPLETE", message: "Shares, cash or debt were unavailable, so enterprise value was not computed.", severity: "warning" });
  }
  if (!comparable) warnings.push({ code: "FINANCIAL_CURRENCY_MISMATCH", message: `Financials are in ${market.financialCurrency}; the quote is in ${market.currency}.`, severity: "warning" });

  const atm = (bridge?.atmProgram ?? null) as Record<string, unknown> | null;
  const yahoo = yahooBasis(market, quote);
  const sharesDiffPct = secBasic != null && market.sharesOutstanding != null && market.sharesOutstanding > 0
    ? round((secBasic / market.sharesOutstanding - 1) * 100, 2)
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
      secOrdinaryShares: secBasic,
      ordinarySharesPerQuotedShare: ratio,
      dilutionPctAtPrice: secUsable ? num(bridgeCore?.dilutionPctAtPrice) : null,
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
