/**
 * Fundamentals and analyst tools: company profile and stock info, statements and ratios, calendars and corporate actions,
 * holders and ownership, ETF and fund profiles, analyst consensus, recommendations, rating changes and upgrade radar, EPS
 * revisions, earnings analysis and momentum, credit health, share-count trend, peer valuations and analyst valuation
 * methods (Phase 5 move out of yahoo-finance.ts). Helpers shared with other domains still live in yahoo-finance.ts and are
 * imported back; yahoo-finance.ts re-exports every moved name, so tools.ts and the parity bundle are unchanged.
 */

import { providerFetch } from "./request-context.js";
import { mcpFailure, ErrorCode } from "./response.js";
import { tickerCap } from "./tool-catalog.js";
import { analystValuationMethods } from "./capital-structure.js";
import { type MarketInputs, peerValuations, majorPrice } from "./valuation.js";
import { EPS_BASIS } from "./evidence.js";
import { type FiscalYearNaming, fiscalYearNaming } from "./fiscal-calendar.js";
import { resolveCikForTicker, edgarGetJson } from "./clients/edgar.js";
import {
  yahooTimeseriesUrl,
  YAHOO_QUERY1,
  yahooQuoteSummaryUrl,
  YAHOO_VISUALIZATION_URL,
  yahooChartUrl,
  secCompanyFactsUrl,
} from "./clients/endpoints.js";
import { UA } from "./clients/provider-http.js";
import { yGet, enc, iso, valuationMarketInputs, fetchTimeseriesByDate, noData, raw, yPost } from "./clients/yahoo.js";
import { stripHtmlTags } from "./clients/html.js";
import { clampInt, parseObjectJson, getLastTradingDate } from "./util.js";
import { runPartialBatch } from "./batch.js";
import { resolveNewsCompanyIdentity } from "./news/identity.js";
import { getCompanyNews } from "./news.js";
import { snapshotComponentMessage } from "./pricing.js";
import { HOLDER_MOD, REC_MOD, SUPPORTED_HOLDER_TYPES, SUPPORTED_RECOMMENDATION_TYPES } from "./yahoo-finance.js";

const ANALYST_UPGRADE_GRADES = new Set([
  "buy", "outperform", "overweight", "strong buy", "positive",
  "market outperform", "top pick",
]);
const ANALYST_DOWNGRADE_GRADES = new Set([
  "sell", "underperform", "underweight", "strong sell", "negative",
  "market underperform", "reduce",
]);
const ANALYST_INITIATION_ACTIONS = new Set(["initiated", "init", "initiation", "new coverage"]);

function classifyAnalystChange(action: unknown, fromGrade: unknown, toGrade: unknown): string {
  const actionLower = String(action ?? "").trim().toLowerCase();
  const fromLower = String(fromGrade ?? "").trim().toLowerCase();
  const toLower = String(toGrade ?? "").trim().toLowerCase();
  if (ANALYST_INITIATION_ACTIONS.has(actionLower) || actionLower.startsWith("init")) return "INITIATED";
  if (actionLower.includes("downgrade") || actionLower === "down") return "DOWNGRADE";
  if (actionLower.includes("upgrade") || actionLower === "up") return "UPGRADE";
  if (ANALYST_DOWNGRADE_GRADES.has(toLower) && !ANALYST_DOWNGRADE_GRADES.has(fromLower)) return "DOWNGRADE";
  if (ANALYST_UPGRADE_GRADES.has(toLower) && fromLower && !ANALYST_UPGRADE_GRADES.has(fromLower)) return "UPGRADE";
  if (toLower && !fromLower) return "INITIATED";
  return "MAINTAIN";
}

const FUND_INVERSE_VALUATION_FIELDS = new Set([
  "priceToEarnings",
  "priceToBook",
  "priceToSales",
  "priceToCashflow",
  "priceToEarningsCat",
  "priceToBookCat",
  "priceToSalesCat",
  "priceToCashflowCat",
]);

function normalizeFundEquityHoldings(
  source: Record<string, unknown>
): {
  values: Record<string, unknown>;
  providerRaw: Record<string, unknown> | null;
  methods: Record<string, string>;
} {
  const values: Record<string, unknown> = {};
  const providerRaw: Record<string, unknown> = {};
  const methods: Record<string, string> = {};
  let changed = false;
  for (const [key, wrapped] of Object.entries(source)) {
    const value = raw(wrapped);
    providerRaw[key] = value;
    if (FUND_INVERSE_VALUATION_FIELDS.has(key) && typeof value === "number" && value > 0) {
      if (value < 1) {
        values[key] = +(1 / value).toFixed(4);
        methods[key] = "RECIPROCAL_FROM_PROVIDER_YIELD";
        changed = true;
      } else {
        values[key] = value;
        methods[key] = "PASSTHROUGH_CONVENTIONAL_MULTIPLE";
      }
    } else {
      values[key] = value;
    }
  }
  return { values, providerRaw: changed ? providerRaw : null, methods };
}

const FUND_QUOTE_TYPES = new Set(["ETF", "MUTUALFUND", "MONEYMARKET"]);

export function notAFundMessage(ticker: string, quoteType: string): string {
  return `${ticker} is quote type ${quoteType}, not an ETF or fund; get_fund_profile reads fund data only. Use get_company_profile for a company.`;
}

export async function getEtfInfo(ticker: string | string[], sections?: string[] | null): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getEtfInfo(t, sections), tickerCap("get_fund_profile"));
  }

  try {
    const requested = sections?.length ? sections : ["overview", "holdings", "allocation"];
    const allowed = new Set(["overview", "holdings", "allocation", "operations", "fixed_income"]);
    const invalid = requested.filter((section) => !allowed.has(section));
    if (invalid.length) {
      return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: `Unsupported sections: ${invalid.join(", ")}`, supportedSections: [...allowed] });
    }
    const modules = [
      "summaryDetail",
      "summaryProfile",
      "defaultKeyStatistics",
      "topHoldings",
      "fundProfile",
      "fundPerformance",
      "price",
    ].join(",");

    const d = (await yGet(
      yahooQuoteSummaryUrl(enc(ticker), modules)
    )) as Record<string, unknown>;

    const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
      | Record<string, unknown>
      | undefined;
    if (!result) return noData(ticker);

    const priceData = result.price as Record<string, unknown> | undefined;
    const summary = result.summaryDetail as Record<string, unknown> | undefined;
    const keyStats = result.defaultKeyStatistics as Record<string, unknown> | undefined;
    const topHoldingsData = result.topHoldings as Record<string, unknown> | undefined;
    const summaryProfile = result.summaryProfile as Record<string, unknown> | undefined;
    const fundProfile = result.fundProfile as Record<string, unknown> | undefined;
    const fundPerf = result.fundPerformance as Record<string, unknown> | undefined;

    const pick = (src: Record<string, unknown> | undefined, key: string): unknown =>
      src ? raw(src[key]) : null;

    // A stock is not a fund: say so instead of empty sections that recommend a retry (2.25.2, F33).
    const fundQuoteType = pick(priceData, "quoteType");
    if (typeof fundQuoteType === "string" && fundQuoteType && !FUND_QUOTE_TYPES.has(fundQuoteType.toUpperCase())) {
      return mcpFailure("get_fund_profile", ErrorCode.INPUT_VALIDATION_ERROR, notAFundMessage(ticker, fundQuoteType));
    }

    const data: Record<string, unknown> = {
      ticker,
      source: "yahoo_finance",
      evidenceClass: "CONTEXTUAL_MARKET_DATA",
      decisionGrade: false,
      sectionsRequested: requested,
      sectionStatus: {},
      sectionDates: {},
      warnings: [],
      // Identity
      shortName: pick(priceData, "shortName"),
      quoteType: pick(priceData, "quoteType"),
      category: pick(summary, "category"),
      fundFamily: pick(summary, "fundFamily"),
      legalType: pick(keyStats, "legalType"),
      fundInceptionDate: pick(keyStats, "fundInceptionDate"),
      // Pricing
      navPrice: pick(summary, "navPrice"),
      previousClose: pick(summary, "previousClose"),
      open: pick(summary, "open"),
      dayHigh: pick(summary, "dayHigh"),
      dayLow: pick(summary, "dayLow"),
      volume: pick(summary, "volume"),
      averageVolume: pick(summary, "averageVolume"),
      // AUM / costs
      totalAssets: pick(summary, "totalAssets"),
      yield: pick(summary, "yield"),
      annualReportExpenseRatio: pick(summary, "annualReportExpenseRatio"),
      ytdReturn: pick(summary, "ytdReturn"),
      beta3Year: pick(summary, "beta3Year"),
      // 52-week
      fiftyTwoWeekHigh: pick(summary, "fiftyTwoWeekHigh"),
      fiftyTwoWeekLow: pick(summary, "fiftyTwoWeekLow"),
      fiftyTwoWeekChange: pick(keyStats, "52WeekChange"),
      // Moving averages
      fiftyDayAverage: pick(summary, "fiftyDayAverage"),
      twoHundredDayAverage: pick(summary, "twoHundredDayAverage"),
    };

    if (requested.includes("overview")) {
      data.description = pick(summaryProfile, "longBusinessSummary");
      data.fundOverview = {
        categoryName: pick(fundProfile, "categoryName") ?? pick(summary, "category"),
        family: pick(fundProfile, "family") ?? pick(summary, "fundFamily"),
        legalType: pick(fundProfile, "legalType") ?? pick(keyStats, "legalType"),
      };
      (data.sectionStatus as Record<string, string>).overview = summaryProfile || fundProfile ? "OK" : "EMPTY_RESULT";
    }

    // Top holdings (up to 10)
    if (topHoldingsData && requested.includes("holdings")) {
      const holdings = (topHoldingsData.holdings as Array<Record<string, unknown>> | undefined) ?? [];
      data.topHoldings = holdings.slice(0, 10).map((h) => ({
        symbol: h.symbol,
        holdingName: h.holdingName,
        pct: raw(h.holdingPercent),
      }));

      const equity = (topHoldingsData.equityHoldings as Record<string, unknown> | undefined) ?? {};
      const normalizedEquity = normalizeFundEquityHoldings(equity);
      data.equityHoldings = normalizedEquity.values;
      data.equityHoldingsProviderRaw = normalizedEquity.providerRaw;
      data.valuationBasis = "CONVENTIONAL_MULTIPLE";
      data.valuationNormalization = normalizedEquity.methods;
      (data.sectionDates as Record<string, unknown>).holdings = {
        asOfDate: null,
        status: "NOT_EXPOSED_BY_PROVIDER",
      };
      (data.sectionStatus as Record<string, string>).holdings = "OK";
    } else if (requested.includes("holdings")) {
      (data.sectionStatus as Record<string, string>).holdings = "EMPTY_RESULT";
    }

    if (topHoldingsData && requested.includes("allocation")) {
      data.assetClasses = {
        cashPosition: raw(topHoldingsData.cashPosition),
        stockPosition: raw(topHoldingsData.stockPosition),
        bondPosition: raw(topHoldingsData.bondPosition),
        preferredPosition: raw(topHoldingsData.preferredPosition),
        convertiblePosition: raw(topHoldingsData.convertiblePosition),
        otherPosition: raw(topHoldingsData.otherPosition),
      };
      const sw = (topHoldingsData.sectorWeightings as Array<Record<string, unknown>> | undefined) ?? [];
      data.sectorWeights = sw
        .map((s) => {
          const entries = Object.entries(s);
          if (entries.length === 0) return null;
          const [sector, rawWeight] = entries[0];
          return { sector, weight: raw(rawWeight) };
        })
        .filter(Boolean);
      (data.sectionDates as Record<string, unknown>).allocation = {
        asOfDate: null,
        status: "NOT_EXPOSED_BY_PROVIDER",
      };
      (data.sectionStatus as Record<string, string>).allocation = "OK";
    } else if (requested.includes("allocation")) {
      (data.sectionStatus as Record<string, string>).allocation = "EMPTY_RESULT";
    }

    if (requested.includes("operations")) {
      const fees = (fundProfile?.feesExpensesInvestment as Record<string, unknown> | undefined) ?? {};
      const categoryFees = (fundProfile?.feesExpensesInvestmentCat as Record<string, unknown> | undefined) ?? {};
      data.fundOperations = Object.fromEntries(
        [...new Set([...Object.keys(fees), ...Object.keys(categoryFees)])].map((key) => [key, {
          fund: raw(fees[key]), categoryAverage: raw(categoryFees[key]),
        }])
      );
      (data.sectionStatus as Record<string, string>).operations = fundProfile ? "OK" : "EMPTY_RESULT";
    }

    if (topHoldingsData && requested.includes("fixed_income")) {
      const bond = (topHoldingsData.bondHoldings as Record<string, unknown> | undefined) ?? {};
      data.bondHoldings = Object.fromEntries(Object.entries(bond).map(([key, value]) => [key, raw(value)]));
      const ratings = (topHoldingsData.bondRatings as Array<Record<string, unknown>> | undefined) ?? [];
      data.bondRatings = Object.fromEntries(ratings.flatMap((rating) => Object.entries(rating).map(([key, value]) => [key, raw(value)])));
      (data.sectionStatus as Record<string, string>).fixed_income = "OK";
    } else if (requested.includes("fixed_income")) {
      (data.sectionStatus as Record<string, string>).fixed_income = "EMPTY_RESULT";
    }

    // Annual returns from fundPerformance (most recent 5 years)
    if (fundPerf) {
      const annual = fundPerf.annualTotalReturns as Record<string, unknown> | undefined;
      if (annual) {
        const returns = (annual.returns as Array<Record<string, unknown>> | undefined) ?? [];
        data.annualReturns = returns.slice(0, 5).map((r) => ({
          year: r.year,
          annualValue: raw(r.annualValue),
        }));
      }
    }

    const statuses = Object.values(data.sectionStatus as Record<string, string>);
    const datedSections = ["holdings", "allocation"].filter((section) => requested.includes(section));
    if (datedSections.some((section) => (data.sectionStatus as Record<string, string>)[section] === "OK")) {
      (data.warnings as string[]).push("FUND_CHARACTERISTICS_AS_OF_DATE_NOT_EXPOSED");
    }
    data.recommendedNextAction = !statuses.every((status) => status === "OK")
      ? "RETRY_OR_REQUEST_AVAILABLE_SECTION"
      : (data.warnings as string[]).length
        ? "CHECK_OFFICIAL_FUND_SOURCE"
        : "NONE";
    return JSON.stringify(data);
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

export async function getStockInfo(ticker: string | string[], includeAll = false): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getStockInfo(t, includeAll), tickerCap("get_company_profile"));
  }
  const modules = [
    "assetProfile",
    "summaryProfile",
    "summaryDetail",
    "financialData",
    "defaultKeyStatistics",
    "calendarEvents",
    "earningsTrend",
    "price",
  ].join(",");

  const d = (await yGet(
    yahooQuoteSummaryUrl(enc(ticker), modules)
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  // Flatten all module fields into a single info object, unwrapping {raw, fmt} values
  const info: Record<string, unknown> = {};
  for (const mod of Object.values(result)) {
    if (mod && typeof mod === "object") {
      for (const [k, v] of Object.entries(mod as Record<string, unknown>)) {
        // A later module's empty value never replaces an earlier one's (2.25.2, F34: AAPL's
        // priceToSalesTrailing12Months read null; Python's merged info keeps it).
        const value = raw(v);
        if (value != null || !(k in info)) info[k] = value;
      }
    }
  }

  if (!includeAll) {
    const defaults = [
      "shortName", "longName", "sector", "industry", "country", "website", "fullTimeEmployees",
      "currentPrice", "previousClose", "marketCap", "enterpriseValue", "currency",
      "trailingPE", "forwardPE", "priceToBook", "priceToSalesTrailing12Months", "enterpriseToEbitda",
      "trailingEps", "forwardEps", "revenueGrowth", "earningsGrowth",
      "grossMargins", "operatingMargins", "profitMargins", "returnOnEquity", "returnOnAssets",
      "dividendYield", "payoutRatio",
      "recommendationMean", "numberOfAnalystOpinions", "targetMeanPrice",
      "longBusinessSummary",
    ];
    const filtered: Record<string, unknown> = {};
    for (const k of defaults) if (k in info) filtered[k] = info[k];
    return JSON.stringify(filtered);
  }
  return JSON.stringify(info);
}

export async function getStockActions(ticker: string, startDate = "", limit = 40): Promise<string> {
  const d = (await yGet(
    yahooChartUrl(enc(ticker), "range=max&interval=1d&events=div%2Csplit%2CcapitalGains"),
    false
  )) as Record<string, unknown>;

  const result = (d?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  const events = (result?.events as Record<string, Record<string, Record<string, number>>>) ?? {};

  type Row = { Date: string; Dividends: number; "Stock Splits": number; "Capital Gains": number };
  const rows: Row[] = [];

  for (const v of Object.values(events.dividends ?? {})) {
    rows.push({ Date: iso(v.date), Dividends: v.amount, "Stock Splits": 0, "Capital Gains": 0 });
  }
  for (const v of Object.values(events.splits ?? {})) {
    rows.push({
      Date: iso(v.date),
      Dividends: 0,
      "Stock Splits": v.numerator / v.denominator,
      "Capital Gains": 0,
    });
  }
  for (const v of Object.values(events.capitalGains ?? {})) {
    rows.push({ Date: iso(v.date), Dividends: 0, "Stock Splits": 0, "Capital Gains": v.amount });
  }

  rows.sort((a, b) => a.Date.localeCompare(b.Date));
  return JSON.stringify(limitCorporateActions(rows, startDate, limit));
}

/**
 * Every split, plus the most recent `limit` dividend/capital-gain rows on or
 * after startDate; oldest first. A full dividend history runs to hundreds of
 * rows, while splits are rare and needed to read old prices.
 */
export function limitCorporateActions<T extends { Date: string; "Stock Splits": number }>(rows: T[], startDate: string, limit: number): T[] {
  const cap = Math.max(1, Math.min(1000, Math.floor(Number(limit) || 40)));
  const isSplit = (row: T): boolean => Number(row["Stock Splits"]) > 0;
  const payouts = rows.filter((row) => !isSplit(row) && (!startDate || row.Date.slice(0, 10) >= startDate));
  const kept = new Set<T>([...rows.filter(isSplit), ...payouts.slice(-cap)]);
  return rows.filter((row) => kept.has(row));
}

const INCOME_BASE_TYPES = [
  "TotalRevenue", "OperatingRevenue", "GrossProfit", "CostOfRevenue",
  "OperatingExpense", "ResearchAndDevelopment", "SellingGeneralAndAdministration",
  "OperatingIncome", "PretaxIncome", "TaxProvision", "NetIncome",
  "EBITDA", "EBIT", "InterestExpense", "InterestIncome", "NetInterestIncome",
  "DilutedEPS", "BasicEPS", "DilutedAverageShares", "BasicAverageShares",
  "NormalizedEBITDA", "NormalizedIncome", "ReconciledCostOfRevenue",
  "ReconciledDepreciation", "NetIncomeContinuousOperations",
  "TotalExpenses", "NetIncomeIncludingNoncontrollingInterests",
  "NetIncomeCommonStockholders", "MinorityInterests",
  "TotalOperatingIncomeAsReported", "SpecialIncomeCharges",
  "InterestExpenseNonOperating", "InterestIncomeNonOperating",
  "OtherIncomeExpense", "OtherNonOperatingIncomeExpenses",
];

const BALANCE_BASE_TYPES = [
  "TotalAssets", "CurrentAssets", "CashAndCashEquivalents",
  "CashCashEquivalentsAndShortTermInvestments", "OtherShortTermInvestments",
  "NetReceivables", "Inventory", "OtherCurrentAssets",
  "TotalNonCurrentAssets", "NetPPE", "Goodwill", "OtherIntangibleAssets",
  "OtherNonCurrentAssets", "TotalLiabilitiesNetMinorityInterest",
  "CurrentLiabilities", "AccountsPayable", "CurrentDebt",
  "OtherCurrentLiabilities", "LongTermDebt",
  "OtherNonCurrentLiabilities", "MinorityInterest",
  "StockholdersEquity", "CommonStock", "RetainedEarnings",
  "TotalEquityGrossMinorityInterest", "WorkingCapital",
  "TangibleBookValue", "TotalDebt", "NetDebt",
  "CapitalLeaseObligations", "CommonStockEquity",
];

const CASHFLOW_BASE_TYPES = [
  "OperatingCashFlow", "InvestingCashFlow", "FinancingCashFlow",
  "EndCashPosition", "CapitalExpenditure", "FreeCashFlow",
  "RepurchaseOfCapitalStock", "RepaymentOfDebt", "IssuanceOfDebt",
  "DepreciationAndAmortization", "ChangeInWorkingCapital", "NetIncome",
  "DeferredIncomeTax", "StockBasedCompensation",
  "IssuanceOfCapitalStock", "ChangeInReceivables", "ChangeInInventory",
  "ChangeInPayablesAndAccruedExpense", "OtherNonCashItems",
];

const TIMESERIES_FS_CONFIG: Record<string, { prefix: string; baseTypes: string[] }> = {
  income_stmt:             { prefix: "annual",    baseTypes: INCOME_BASE_TYPES },
  quarterly_income_stmt:   { prefix: "quarterly", baseTypes: INCOME_BASE_TYPES },
  balance_sheet:           { prefix: "annual",    baseTypes: BALANCE_BASE_TYPES },
  quarterly_balance_sheet: { prefix: "quarterly", baseTypes: BALANCE_BASE_TYPES },
  cashflow:                { prefix: "annual",    baseTypes: CASHFLOW_BASE_TYPES },
  quarterly_cashflow:      { prefix: "quarterly", baseTypes: CASHFLOW_BASE_TYPES },
  ttm_income_stmt:         { prefix: "trailing",  baseTypes: INCOME_BASE_TYPES },
  ttm_cashflow:            { prefix: "trailing",  baseTypes: CASHFLOW_BASE_TYPES },
};

async function fetchTimeseries(
  ticker: string,
  prefix: string,
  baseTypes: string[],
  lineItems?: string[] | null,
): Promise<string> {
  const normalized = new Set((lineItems ?? []).map((item) => item.toLowerCase().replace(/[^a-z0-9]/g, "")));
  const selectedTypes = normalized.size
    ? baseTypes.filter((type) => normalized.has(type.toLowerCase()) || normalized.has(type.replace(/([a-z])([A-Z])/g, "$1 $2").toLowerCase().replace(/[^a-z0-9]/g, "")))
    : baseTypes;
  if (normalized.size && !selectedTypes.length) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "No requested line_items matched this statement type", requestedLineItems: lineItems });
  }
  const byDateRows = await fetchTimeseriesByDate(ticker, prefix, selectedTypes);
  if (byDateRows == null) return noData(ticker);
  const byDate = Object.fromEntries(byDateRows.map((row) => [String(row.date), row]));
  const dates = byDateRows.map((row) => String(row.date)).slice(0, prefix === "trailing" ? 1 : 4);
  if (!dates.length) return JSON.stringify([]);

  // Match Python/yfinance's line-item-oriented DataFrame serialization:
  // [{lineItem: "Total Revenue", "2026-03-31": 123, ...}, ...]
  const humanize = (value: string): string => value
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2");
  const records = selectedTypes.map((baseType) => {
    const key = baseType.charAt(0).toLowerCase() + baseType.slice(1);
    const row: Record<string, unknown> = { lineItem: humanize(baseType) };
    for (const date of dates) row[date] = byDate[date][key] ?? null;
    return row;
  }).filter((row) => dates.some((date) => row[date] != null));

  return JSON.stringify(records);
}

export async function getFinancialStatement(ticker: string, type: string, lineItems?: string[] | null): Promise<string> {
  const cfg = TIMESERIES_FS_CONFIG[type];
  if (!cfg) return `Error: invalid financial type '${type}'`;

  try {
    return await fetchTimeseries(ticker, cfg.prefix, cfg.baseTypes, lineItems);
  } catch (e) {
    return `Error fetching financial statement for ${ticker}: ${e instanceof Error ? e.message : String(e)}`;
  }
}

export async function getHolderInfo(ticker: string, type: string): Promise<string> {
  const mod = HOLDER_MOD[type];
  if (!mod) {
    return mcpFailure(
      "get_ownership_holders",
      "INPUT_VALIDATION_ERROR",
      `Invalid holder_type '${type}'. Supported holder_type values: ${SUPPORTED_HOLDER_TYPES.join(", ")}`,
      { metaExtra: { supportedHolderTypes: SUPPORTED_HOLDER_TYPES } },
    );
  }

  const d = (await yGet(
    yahooQuoteSummaryUrl(enc(ticker), mod)
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  return JSON.stringify(unwrapYahooValues(result[mod]));
}

/**
 * Yahoo quoteSummary values arrive as {raw, fmt, longFmt} objects. Keep the
 * number (or the formatted date, for date values) and drop Yahoo's maxAge
 * bookkeeping: {pctHeld: {raw: 0.08, fmt: "8%"}} -> {pctHeld: 0.08}.
 */
export function unwrapYahooValues(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(unwrapYahooValues);
  if (value == null || typeof value !== "object") return value;
  const obj = value as Record<string, unknown>;
  if ("raw" in obj && Object.keys(obj).every((key) => key === "raw" || key === "fmt" || key === "longFmt")) {
    if (typeof obj.fmt === "string" && /^\d{4}-\d{2}-\d{2}$/.test(obj.fmt)) return obj.fmt;
    return obj.raw ?? null;
  }
  if (Object.keys(obj).length === 0) return null;
  const out: Record<string, unknown> = {};
  for (const [key, inner] of Object.entries(obj)) {
    if (key === "maxAge") continue;
    out[key] = unwrapYahooValues(inner);
  }
  return out;
}

export async function getAnalystConsensus(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getAnalystConsensus(t), tickerCap("get_analyst_consensus"));
  }
  const modules = "financialData,recommendationTrend,price,upgradeDowngradeHistory";
  const d = (await yGet(
    yahooQuoteSummaryUrl(enc(ticker), modules)
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  const fd = (result.financialData as Record<string, unknown>) ?? {};
  const rt = (result.recommendationTrend as Record<string, unknown>) ?? {};
  const price = (result.price as Record<string, unknown>) ?? {};

  const lastPrice = raw(price.regularMarketPrice) as number | null;
  const targetMean = raw(fd.targetMeanPrice) as number | null;
  const warnings: Array<{ code: string; message: string }> = [];

  const output: Record<string, unknown> = {
    ticker,
    // The same provenance Python sets: Yahoo consensus is contextual market data (2.25.0, F19).
    source: "yahoo_finance",
    evidenceClass: "CONTEXTUAL_MARKET_DATA",
    decisionGrade: false,
    recommendedNextAction: "NONE",
    priceTargets: {
      current: lastPrice,
      low: raw(fd.targetLowPrice),
      high: raw(fd.targetHighPrice),
      mean: targetMean,
      median: raw(fd.targetMedianPrice),
      // Analysts behind the price targets (not the rating counts below).
      numberOfAnalysts: raw(fd.numberOfAnalystOpinions),
      pctUpsideFromLastPrice:
        targetMean != null && lastPrice != null && lastPrice !== 0
          ? +((targetMean - lastPrice) / lastPrice * 100).toFixed(2)
          : null,
    },
  };

  const trend = (rt.trend as Record<string, unknown>[]) ?? [];
  if (trend.length > 0) {
    const latest = trend[0];
    const cols = ["strongBuy", "buy", "hold", "sell", "strongSell"] as const;
    const counts: Record<string, number> = Object.fromEntries(
      cols.map((c) => [c, (raw(latest[c]) as number | null) ?? 0])
    );
    const dominant = cols.reduce((a, b) => ((counts[a] ?? 0) >= (counts[b] ?? 0) ? a : b));
    output.recommendationSummary = trend.map((t) =>
      Object.fromEntries(Object.entries(t).map(([k, v]) => [k, raw(v)]))
    );
    output.dominantRating = dominant;
    output.ratingCounts = counts;
    output.ratingCount = Object.values(counts).reduce((a, b) => a + b, 0);
    // Deprecated: the rating count; priceTargets.numberOfAnalysts counts target contributors.
    output.numberOfAnalysts = output.ratingCount;
  } else {
    output.recommendationSummary = null;
  }

  const cutoffSec = Date.now() / 1000 - 30 * 86400;
  const upgradeHistory = (result.upgradeDowngradeHistory as Record<string, unknown>) ?? {};
  const history = (upgradeHistory.history as Record<string, unknown>[]) ?? [];
  const recentUpgradeCount30d = history.filter((h) => {
    const tsRaw = raw(h.epochGradeDate) as number | null;
    if (tsRaw == null || tsRaw < cutoffSec) return false;
    const action = h.action ?? h.Action ?? "";
    const toGrade = h.toGrade ?? h.ToGrade ?? "";
    const fromGrade = h.fromGrade ?? h.FromGrade ?? "";
    return classifyAnalystChange(action, fromGrade, toGrade) === "UPGRADE";
  }).length;

  let targetLagSignal = "UNKNOWN";
  if (targetMean != null && lastPrice != null) {
    if (targetMean >= lastPrice) {
      targetLagSignal = "CURRENT";
    } else if (recentUpgradeCount30d > 0) {
      targetLagSignal = "LIKELY_STALE_OR_LAGGING";
      warnings.push({
        code: "CONSENSUS_TARGET_BELOW_PRICE_DESPITE_UPGRADES",
        message: "Consensus price target may lag recent market or analyst sentiment changes.",
      });
    } else {
      targetLagSignal = "POSSIBLY_STALE";
    }
  }

  output.currentPrice = lastPrice;
  output.pctBelowCurrentPrice =
    targetMean != null && lastPrice != null && lastPrice > 0
      ? +((lastPrice - targetMean) / lastPrice * 100).toFixed(2)
      : null;
  output.recentUpgradeCount30d = recentUpgradeCount30d;
  output.targetLagSignal = targetLagSignal;
  output.warnings = warnings;

  return JSON.stringify(output);
}

export async function getEarningsAnalysis(ticker: string): Promise<string> {
  const d = (await yGet(
    yahooQuoteSummaryUrl(enc(ticker), "earningsTrend,earningsHistory")
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  const et = (result.earningsTrend as Record<string, unknown>) ?? {};
  const eh = (result.earningsHistory as Record<string, unknown>) ?? {};

  const flatRaw = (obj: Record<string, unknown>): Record<string, unknown> =>
    Object.fromEntries(Object.entries(obj).map(([k, v]) => [k, raw(v)]));

  const output: Record<string, unknown> = {
    ticker,
    earningsEstimate: null,
    revenueEstimate: null,
    epsTrend: null,
    epsRevisions: null,
    earningsHistory: null,
    growthEstimates: null,
    source: "yahoo_finance",
    evidenceClass: "CONTEXTUAL_MARKET_DATA",
    decisionGrade: false,
    recommendedNextAction: "NONE",
  };

  const trendArr = (et.trend as Record<string, unknown>[]) ?? [];
  if (trendArr.length > 0) {
    output.earningsEstimate = trendArr.map((p) => ({
      period: p.period,
      ...flatRaw((p.earningsEstimate as Record<string, unknown>) ?? {}),
    }));
    output.revenueEstimate = trendArr.map((p) => ({
      period: p.period,
      ...flatRaw((p.revenueEstimate as Record<string, unknown>) ?? {}),
    }));
    output.epsTrend = trendArr.map((p) => ({
      period: p.period,
      ...flatRaw((p.epsTrend as Record<string, unknown>) ?? {}),
    }));
    output.epsRevisions = trendArr.map((p) => ({
      period: p.period,
      ...flatRaw((p.epsRevisions as Record<string, unknown>) ?? {}),
    }));
    output.growthEstimates = trendArr.map((p) => ({
      period: p.period,
      stockGrowth: raw((p.growth as Record<string, unknown> | undefined)?.estimate ?? null),
    }));
  }

  const histArr = (eh.history as Record<string, unknown>[]) ?? [];
  if (histArr.length > 0) {
    output.earningsHistory = histArr.map((h) => ({
      // Fiscal quarter end as YYYY-MM-DD (Yahoo sends {raw: epoch, fmt}).
      quarter: unwrapYahooValues(h.quarter),
      epsActual: raw(h.epsActual),
      epsEstimate: raw(h.epsEstimate),
      epsDifference: raw(h.epsDifference),
      // Yahoo's surprisePercent is a decimal ratio (0.0452 = 4.52%); surprisePct is in percent.
      surprisePercent: raw(h.surprisePercent),
      surprisePct: typeof raw(h.surprisePercent) === "number" ? +((raw(h.surprisePercent) as number) * 100).toFixed(2) : null,
    }));
  }
  output.unitSemantics = EARNINGS_ANALYSIS_UNITS;
  output.epsBasis = EPS_BASIS;

  return JSON.stringify(output);
}

const EARNINGS_ANALYSIS_UNITS = {
  decimalRatios: ["earningsEstimate[].growth", "revenueEstimate[].growth", "earningsHistory[].surprisePercent"],
  percentValues: ["earningsHistory[].surprisePct"],
};

const VALUATION_TYPES: Record<string, string> = {
  MarketCap: "marketCap",
  EnterpriseValue: "enterpriseValue",
  PeRatio: "trailingPE",
  ForwardPeRatio: "forwardPE",
  PegRatio: "pegRatio",
  PsRatio: "priceToSales",
  PbRatio: "priceToBook",
  EnterprisesValueRevenueRatio: "enterpriseToRevenue",
  EnterprisesValueEBITDARatio: "enterpriseToEbitda",
};

async function fetchValuationHistory(ticker: string, periods: number, frequency: string): Promise<Record<string, unknown>[]> {
  const prefix = frequency === "yearly" ? "annual" : frequency;
  const types = Object.keys(VALUATION_TYPES).map((type) => `${prefix}${type}`);
  const p2 = Math.floor(Date.now() / 1000);
  const d = (await yGet(
    yahooTimeseriesUrl(enc(ticker), YAHOO_QUERY1) +
      `?type=${types.join(",")}&period1=1483142400&period2=${p2}`
  )) as Record<string, unknown>;
  const results = ((d.timeseries as Record<string, unknown> | undefined)?.result as Record<string, unknown>[] | undefined) ?? [];
  const byDate: Record<string, Record<string, unknown>> = {};
  for (const item of results) {
    const rawType = ((item.meta as Record<string, unknown> | undefined)?.type ?? "") as string | string[];
    const typeName = String(Array.isArray(rawType) ? rawType[0] ?? "" : rawType);
    const baseType = typeName.slice(prefix.length);
    const outputKey = VALUATION_TYPES[baseType];
    if (!outputKey) continue;
    const values = (item[typeName] as Array<{ asOfDate?: string; reportedValue?: { raw?: unknown } }> | undefined) ?? [];
    for (const value of values) {
      if (!value.asOfDate) continue;
      byDate[value.asOfDate] ??= { period: value.asOfDate };
      byDate[value.asOfDate][outputKey] = value.reportedValue?.raw ?? null;
    }
  }
  return Object.values(byDate).sort((a, b) => String(b.period).localeCompare(String(a.period))).slice(0, periods);
}

export async function getFinancialRatios(
  ticker: string | string[],
  historyPeriods: number = 0,
  frequency: string = "quarterly",
): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getFinancialRatios(t, historyPeriods, frequency), tickerCap("analyze_financial_ratios"));
  }
  historyPeriods = Math.max(0, Math.min(20, Math.trunc(historyPeriods)));
  if (!["quarterly", "monthly", "yearly", "trailing"].includes(frequency)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "frequency must be quarterly, monthly, yearly, or trailing" });
  }
  const d = (await yGet(
    yahooQuoteSummaryUrl(enc(ticker), "summaryDetail,financialData,defaultKeyStatistics")
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  const sd = (result.summaryDetail as Record<string, unknown>) ?? {};
  const fd = (result.financialData as Record<string, unknown>) ?? {};
  const ks = (result.defaultKeyStatistics as Record<string, unknown>) ?? {};

  const freeCashflow = raw(fd.freeCashflow) as number | null;
  const marketCap = raw(sd.marketCap) as number | null;
  // Multiples of a quote-currency value over financial-currency results.
  const CURRENCY_MIXED_RATIOS = ["priceToSales", "priceToBook", "enterpriseToEbitda", "enterpriseToRevenue", "freeCashflowYield"];

  const rawRatios: Record<string, unknown> = {
    ticker,
    source: "yahoo_finance",
    evidenceClass: "CONTEXTUAL_MARKET_DATA",
    decisionGrade: false,
    recommendedNextAction: "NONE",
    currency: raw(fd.financialCurrency),
    trailingPE: raw(sd.trailingPE),
    forwardPE: raw(sd.forwardPE),
    pegRatio: raw(ks.pegRatio),
    priceToSales: raw(sd.priceToSalesTrailing12Months),
    priceToBook: raw(ks.priceToBook),
    enterpriseToEbitda: raw(ks.enterpriseToEbitda),
    enterpriseToRevenue: raw(ks.enterpriseToRevenue),
    grossMargins: raw(fd.grossMargins),
    operatingMargins: raw(fd.operatingMargins),
    profitMargins: raw(ks.profitMargins),
    returnOnEquity: raw(fd.returnOnEquity),
    returnOnAssets: raw(fd.returnOnAssets),
    debtToEquity: raw(fd.debtToEquity),
    currentRatio: raw(fd.currentRatio),
    quickRatio: raw(fd.quickRatio),
    freeCashflow,
    freeCashflowYield:
      freeCashflow != null && marketCap != null && marketCap !== 0
        ? +((freeCashflow / marketCap) * 100).toFixed(4)
        : null,
    dividendYield: raw(sd.dividendYield),
    payoutRatio: raw(sd.payoutRatio),
    earningsGrowth: raw(fd.earningsGrowth),
    revenueGrowth: raw(fd.revenueGrowth),
  };

  // Post-filter: replace any plain object values (e.g. empty {} wrappers from
  // unavailable fields) with null. raw() only unwraps {raw,fmt}; a bare {}
  // has no "raw" key and slips through as-is.
  const filtered = Object.fromEntries(
    Object.entries(rawRatios).map(([k, v]) => [
      k,
      v !== null && typeof v === "object" && !Array.isArray(v) ? null : v,
    ])
  );
  // Quote and financial currencies must match for value-over-results multiples.
  const quoteCurrency = typeof sd.currency === "string" ? majorPrice(1, sd.currency).currency : null;
  const financialCurrency = typeof filtered.currency === "string" ? filtered.currency : null;
  if (quoteCurrency && financialCurrency && quoteCurrency !== financialCurrency) {
    const withheld: Record<string, unknown> = {};
    for (const key of CURRENCY_MIXED_RATIOS) {
      withheld[key] = filtered[key] ?? null;
      filtered[key] = null;
    }
    filtered.quoteCurrency = quoteCurrency;
    filtered.withheldMultiples = withheld;
    filtered.warnings = [{ code: "CURRENCY_MISMATCH_MULTIPLES_WITHHELD", message: "Yahoo's price/sales, price/book and EV multiples, and the free-cash-flow yield, divide a quote-currency market value by financial-currency results; they are withheld when the two currencies differ (TSM: USD ADR, TWD financials). P/E stays, since Yahoo reports EPS per quoted share.", severity: "warning" }];
  }
  filtered.unitSemantics = {
    currency: filtered.currency ?? null,
    multiples: [
      "trailingPE", "forwardPE", "pegRatio", "priceToSales", "priceToBook",
      "enterpriseToEbitda", "enterpriseToRevenue", "currentRatio", "quickRatio",
    ],
    decimalRatios: [
      "grossMargins", "operatingMargins", "profitMargins", "returnOnEquity",
      "returnOnAssets", "dividendYield", "payoutRatio", "earningsGrowth", "revenueGrowth",
    ],
    percentValues: ["debtToEquity", "freeCashflowYield"],
    currencyValues: ["freeCashflow"],
  };

  if (historyPeriods > 0) {
    try {
      const current: Record<string, unknown> = { period: "Current" };
      for (const outputKey of Object.values(VALUATION_TYPES)) current[outputKey] = filtered[outputKey] ?? null;
      const dated = historyPeriods > 1
        ? await fetchValuationHistory(ticker, historyPeriods - 1, frequency)
        : [];
      filtered.valuationHistory = [current, ...dated];
      filtered.valuationFrequency = frequency;
      filtered.historyPeriodsRequested = historyPeriods;
    } catch (e) {
      filtered.valuationHistory = null;
      filtered.valuationHistoryStatus = "PROVIDER_ERROR";
      filtered.valuationHistoryWarning = e instanceof Error ? e.message : String(e);
    }
  }
  return JSON.stringify(filtered);
}

function visualizationRows(d: Record<string, unknown>): Record<string, unknown>[] {
  const result = ((d.finance as Record<string, unknown> | undefined)?.result as Record<string, unknown>[] | undefined)?.[0];
  const document = (result?.documents as Record<string, unknown>[] | undefined)?.[0];
  const columns = (document?.columns as Array<{ label?: string; id?: string; type?: string }> | undefined) ?? [];
  const seen = new Set<string>();
  const labels = columns.map((column, index) => {
    const base = column.label === "Event Start Date" && column.type === "STRING"
      ? "Timing"
      : column.label ?? column.id ?? `field${index + 1}`;
    if (!seen.has(base)) {
      seen.add(base);
      return base;
    }
    const unique = `${base} ${index + 1}`;
    seen.add(unique);
    return unique;
  });
  const rows = (document?.rows as unknown[][] | undefined) ?? [];
  return rows.map((row) => Object.fromEntries(labels.map((label, index) => [label, row[index] ?? null])));
}

function yahooCalendarNumber(value: string | undefined): number | null {
  const normalized = (value ?? "").trim().replace(/,/g, "").replace(/%$/, "");
  if (!normalized || normalized === "-" || /^n\/?a$/i.test(normalized)) return null;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

async function fetchTickerEarningsHistory(
  ticker: string,
  limit: number,
  offset: number,
): Promise<Record<string, unknown>[]> {
  // yfinance's current Ticker.get_earnings_dates implementation scrapes this
  // bounded Yahoo calendar table because the older visualization feed stopped
  // receiving updates. Keep the Worker on the same source and pagination model.
  const size = limit <= 25 ? 25 : limit <= 50 ? 50 : 100;
  const url = `https://finance.yahoo.com/calendar/earnings?symbol=${enc(ticker)}&offset=${offset}&size=${size}`;
  const response = await providerFetch(url, { headers: { "User-Agent": UA } });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`Yahoo Finance calendar error ${response.status} for: ${ticker}`);
  }
  const html = await response.text();
  const table = html.match(/<table\b[^>]*>[\s\S]*?<\/table>/i)?.[0];
  if (!table) return [];

  const rowHtml = [...table.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)].map((match) => match[1]);
  if (!rowHtml.length) return [];
  const headers = [...rowHtml[0].matchAll(/<th\b[^>]*>([\s\S]*?)<\/th>/gi)]
    .map((match) => stripHtmlTags(match[1]));
  if (!headers.length) return [];

  const rows: Record<string, unknown>[] = [];
  for (const rawRow of rowHtml.slice(1)) {
    const cells = [...rawRow.matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)]
      .map((match) => stripHtmlTags(match[1]));
    if (!cells.length) continue;
    const row = Object.fromEntries(headers.map((header, index) => [header, cells[index] ?? ""]));
    const symbol = String(row.Symbol ?? "").trim();
    if (symbol && symbol.toUpperCase() !== ticker.toUpperCase()) continue;
    rows.push({
      "Earnings Date": row["Earnings Date"] || null,
      "EPS Estimate": yahooCalendarNumber(String(row["EPS Estimate"] ?? "")),
      "Reported EPS": yahooCalendarNumber(String(row["Reported EPS"] ?? "")),
      "Surprise(%)": yahooCalendarNumber(String(row["Surprise(%)"] ?? row["Surprise (%)"] ?? "")),
    });
    if (rows.length >= limit) break;
  }
  return rows;
}

export async function analyzeShareCountTrend(
  ticker: string,
  startDate?: string | null,
  endDate?: string | null,
): Promise<string> {
  const end = endDate ? new Date(`${endDate}T00:00:00Z`) : new Date();
  const start = startDate ? new Date(`${startDate}T00:00:00Z`) : new Date(end.getTime() - 5 * 365 * 86_400_000);
  if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime()) || start >= end) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "start_date must be before end_date" });
  }
  const d = (await yGet(
    yahooTimeseriesUrl(enc(ticker)) +
      `?symbol=${enc(ticker)}&period1=${Math.floor(start.getTime() / 1000)}&period2=${Math.floor(end.getTime() / 1000)}`
  )) as Record<string, unknown>;
  const results = ((d.timeseries as Record<string, unknown> | undefined)?.result as Record<string, unknown>[] | undefined) ?? [];
  const shares = results.find((item) => Array.isArray(item.shares_out));
  const values = (shares?.shares_out as number[] | undefined) ?? [];
  const timestamps = (shares?.timestamp as number[] | undefined) ?? [];
  const ordered = timestamps.map((timestamp, index) => ({ date: iso(timestamp), shares: values[index] ?? null }))
    .filter((item) => item.shares != null)
    .sort((a, b) => a.date.localeCompare(b.date));
  const byTimestamp = new Map<string, { date: string; shares: number | null }>();
  for (const item of ordered) byTimestamp.set(item.date, item);
  const deduped = [...byTimestamp.values()];
  const compact = deduped.filter((item, index) => index === 0 || item.shares !== deduped[index - 1].shares);
  if (!compact.length) {
    return JSON.stringify({ ticker, status: "EMPTY_RESULT", observations: [], source: "yahoo_finance", evidenceClass: "CONTEXTUAL_MARKET_DATA", decisionGrade: false, recommendedNextAction: "CHECK_SEC_FILINGS" });
  }
  const first = compact[0].shares as number;
  const current = compact[compact.length - 1].shares as number;
  const change = current - first;
  const observations = compact.slice(-60);
  return JSON.stringify({
    ticker, status: "OK", startDate: start.toISOString().slice(0, 10), endDate: end.toISOString().slice(0, 10),
    dataDate: compact[compact.length - 1].date, firstShares: first, currentShares: current,
    changeShares: change, changePct: first ? +((change / first) * 100).toFixed(4) : null,
    sampleCount: compact.length, returnedSampleCount: observations.length,
    truncated: compact.length > observations.length, observations, source: "yahoo_finance",
    evidenceClass: "CONTEXTUAL_MARKET_DATA", decisionGrade: false,
    interpretation: "Share-count changes may reflect issuance, buybacks, splits, or provider revisions; confirm material changes in company filings.",
    recommendedNextAction: change ? "CHECK_SEC_FILINGS" : "NONE",
  });
}

export async function getCalendar(ticker: string, mode: string = "upcoming", limit: number = 12, offset: number = 0): Promise<string> {
  limit = Math.max(1, Math.min(100, Math.trunc(limit)));
  offset = Math.max(0, Math.trunc(offset));
  if (!['upcoming', 'history'].includes(mode)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "mode must be upcoming or history" });
  }
  if (mode === "history") {
    const items = await fetchTickerEarningsHistory(ticker, limit, offset);
    return JSON.stringify({
      ticker, mode, status: items.length ? "OK" : "EMPTY_RESULT", items, limit, offset, hasMore: items.length === limit,
      source: "yahoo_finance", sourceType: "provider_calendar", evidenceClass: "CONTEXTUAL_MARKET_DATA",
      providerMethod: "YAHOO_CALENDAR_HTML",
      confirmationStatus: "UNVERIFIED", decisionGrade: false, recommendedNextAction: "NONE",
    });
  }
  const d = (await yGet(
    yahooQuoteSummaryUrl(enc(ticker), "calendarEvents")
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  const cal = (result.calendarEvents as Record<string, unknown>) ?? {};
  const earnings = (cal.earnings as Record<string, unknown>) ?? {};

  const earningsDates = (
    (earnings.earningsDate as Array<{ raw?: number } | null>) ?? []
  ).map((d) => (d?.raw != null ? iso(d.raw) : null));

  const exDiv = cal.exDividendDate as { raw?: number } | null | undefined;
  const divDate = cal.dividendDate as { raw?: number } | null | undefined;

  return JSON.stringify({
    ticker,
    mode,
    earningsDateConfirmed: false,
    earningsDateSource: earningsDates.filter(Boolean).length ? "YAHOO_CALENDAR" : "UNKNOWN",
    confirmationStatus: "UNVERIFIED",
    source: "yahoo_finance",
    sourceType: "provider_calendar",
    evidenceClass: "CONTEXTUAL_MARKET_DATA",
    decisionGrade: false,
    recommendedNextAction: earningsDates.filter(Boolean).length ? "CHECK_OFFICIAL_RELEASES" : "NONE",
    calendar: {
      earnings: {
        earningsDate: earningsDates,
        earningsAverage: raw(earnings.earningsAverage),
        earningsLow: raw(earnings.earningsLow),
        earningsHigh: raw(earnings.earningsHigh),
        revenueAverage: raw(earnings.revenueAverage),
        revenueLow: raw(earnings.revenueLow),
        revenueHigh: raw(earnings.revenueHigh),
      },
      exDividendDate: exDiv?.raw != null ? iso(exDiv.raw) : null,
      dividendDate: divDate?.raw != null ? iso(divDate.raw) : null,
    },
  });
}

export async function getMarketCalendar(
  eventType: string = "earnings",
  startDate?: string | null,
  endDate?: string | null,
  limit: number = 25,
  offset: number = 0,
): Promise<string> {
  const start = startDate ?? new Date().toISOString().slice(0, 10);
  const startMs = new Date(`${start}T00:00:00Z`).getTime();
  const end = endDate ?? new Date(startMs + 7 * 86_400_000).toISOString().slice(0, 10);
  const endMs = new Date(`${end}T23:59:59Z`).getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || startMs > endMs || endMs - startMs > 31 * 86_400_000) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "date range must be 0 to 31 days" });
  }
  limit = Math.max(1, Math.min(100, Math.trunc(limit)));
  offset = Math.max(0, Math.trunc(offset));
  const config: Record<string, { entity: string; fields: string[]; sortField: string }> = {
    earnings: { entity: "sp_earnings", sortField: "intradaymarketcap", fields: ["ticker", "companyshortname", "intradaymarketcap", "eventname", "startdatetime", "startdatetimetype", "epsestimate", "epsactual", "epssurprisepct"] },
    economic: { entity: "economic_event", sortField: "startdatetime", fields: ["econ_release", "country_code", "startdatetime", "period", "after_release_actual", "consensus_estimate", "prior_release_actual", "originally_reported_actual"] },
    ipo: { entity: "ipo_info", sortField: "startdatetime", fields: ["ticker", "companyshortname", "exchange_short_name", "filingdate", "startdatetime", "amendeddate", "pricefrom", "priceto", "offerprice", "currencyname", "shares", "dealtype"] },
    splits: { entity: "splits", sortField: "startdatetime", fields: ["ticker", "companyshortname", "startdatetime", "optionable", "old_share_worth", "share_worth"] },
  };
  const selected = config[eventType];
  if (!selected) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "event_type must be earnings, economic, ipo, or splits" });
  }
  const dateQuery = eventType === "ipo"
    ? { operator: "OR", operands: [
        { operator: "GTELT", operands: ["startdatetime", start, end] },
        { operator: "GTELT", operands: ["filingdate", start, end] },
        { operator: "GTELT", operands: ["amendeddate", start, end] },
      ] }
    : eventType === "earnings"
      ? { operator: "AND", operands: [
          { operator: "EQ", operands: ["region", "us"] },
          { operator: "OR", operands: [
            { operator: "EQ", operands: ["eventtype", "EAD"] },
            { operator: "EQ", operands: ["eventtype", "ERA"] },
          ] },
          { operator: "GTE", operands: ["startdatetime", start] },
          { operator: "LTE", operands: ["startdatetime", end] },
        ] }
      : { operator: "AND", operands: [
          { operator: "GTE", operands: ["startdatetime", start] },
          { operator: "LTE", operands: ["startdatetime", end] },
        ] };
  const d = (await yPost(YAHOO_VISUALIZATION_URL, {
    size: limit,
    offset,
    query: dateQuery,
    sortField: selected.sortField,
    sortType: "DESC",
    entityIdType: selected.entity,
    includeFields: selected.fields,
  })) as Record<string, unknown>;
  const items = visualizationRows(d);
  for (const item of items) {
    const normalized = Object.fromEntries(
      Object.keys(item).map((key) => [key.toLowerCase().replace(/[^a-z]/g, ""), key])
    );
    const actualKey = normalized.epsactual ?? normalized.reportedeps;
    if (actualKey != null && item[actualKey] == null) {
      for (const [normalizedKey, originalKey] of Object.entries(normalized)) {
        if (normalizedKey.includes("surprise")) item[originalKey] = null;
      }
    }
  }
  return JSON.stringify({
    status: items.length ? "OK" : "EMPTY_RESULT", eventType, startDate: start, endDate: end,
    limit, offset, itemCount: items.length, hasMore: items.length === limit,
    coverage: { state: items.length === limit ? "PARTIAL" : "FULL", truncated: items.length === limit },
    items,
    source: "yahoo_finance", sourceType: "provider_calendar", evidenceClass: "CONTEXTUAL_MARKET_DATA",
    confirmationStatus: "UNVERIFIED", decisionGrade: false,
    recommendedNextAction: ["earnings", "ipo", "splits"].includes(eventType) ? "CHECK_OFFICIAL_RELEASES" : "NONE",
  });
}

export async function getRecommendations(
  ticker: string,
  type: string,
  monthsBack: number
): Promise<string> {
  const mod = REC_MOD[type];
  if (!mod) {
    return mcpFailure(
      "get_analyst_recommendations",
      "INPUT_VALIDATION_ERROR",
      `Invalid recommendation_type '${type}'. Supported recommendation_type values: ${SUPPORTED_RECOMMENDATION_TYPES.join(", ")}`,
      { metaExtra: { supportedRecommendationTypes: SUPPORTED_RECOMMENDATION_TYPES } },
    );
  }

  const d = (await yGet(
    yahooQuoteSummaryUrl(enc(ticker), mod)
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  if (type === "recommendations") {
    return JSON.stringify(
      ((result.recommendationTrend as Record<string, unknown>)?.trend as unknown[]) ?? []
    );
  }

  // upgrades_downgrades: filter to monthsBack, dedupe by firm (most recent per firm)
  const cutoffSec = Date.now() / 1000 - monthsBack * 30 * 86400;
  const history = (
    ((result.upgradeDowngradeHistory as Record<string, unknown>)?.history as Record<
      string,
      unknown
    >[]) ?? []
  )
    .filter((h) => ((h.epochGradeDate as number) ?? 0) >= cutoffSec)
    .sort((a, b) => (b.epochGradeDate as number) - (a.epochGradeDate as number));

  const seen = new Set<string>();
  return JSON.stringify(
    history
      .filter((h) => !seen.has(h.firm as string) && seen.add(h.firm as string))
      .map((h) => ({
        ...h,
        GradeDate: h.epochGradeDate ? iso(h.epochGradeDate as number) : null,
      }))
  );
}

export async function getCreditHealth(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getCreditHealth(t), tickerCap("analyze_credit_health"));
  }
  try {
    // One row per quarter, newest first (fetchTimeseries itself returns one
    // row per line item for the statement tool).
    const [bsRows, incRows4] = await Promise.all([
      fetchTimeseriesByDate(ticker, "quarterly", ["TotalDebt", "CashAndCashEquivalents"]),
      fetchTimeseriesByDate(ticker, "quarterly", [
        "EBITDA",
        "NormalizedEBITDA",
        "EBIT",
        "OperatingIncome",
        "InterestExpenseNonOperating",
        "InterestExpense",
        "ReconciledDepreciation",
        "DepreciationAndAmortization",
        "DepreciationAmortizationDepletionIncomeStatement",
      ]),
    ]);

    const bs = bsRows ?? [];
    const inc = incRows4 ?? [];
    if (!bs.length || !inc.length) {
      return JSON.stringify({ error: true, message: "Insufficient financial data", ticker });
    }

    // The newest quarter that reports debt or cash.
    const bsLatest = bs.find((row) => row.totalDebt != null || row.cashAndCashEquivalents != null) ?? bs[0];
    // TTM: sum up to 4 most-recent quarterly income rows (newest first)
    const incRows = inc.slice(0, 4);

    // TTM sum of the first matching key. A trailing-twelve-month figure needs a value in each of the 4 newest
    // quarters (R5-fundamentals): a key with fewer is not a year, so the value is null and the component missing.
    const ttmSum = (keys: string[]): { value: number | null; source: string | null; values: number[] } => {
      for (const key of keys) {
        const vals: number[] = [];
        for (const row of incRows) {
          const v = row[key];
          if (typeof v === "number" && Number.isFinite(v)) vals.push(v);
        }
        if (vals.length > 0) {
          if (incRows.length < 4 || vals.length < incRows.length) return { value: null, source: null, values: vals };
          return { value: vals.reduce((a, b) => a + b, 0), source: key, values: vals };
        }
      }
      return { value: null, source: null, values: [] };
    };

    const totalDebt = (bsLatest.totalDebt as number | null) ?? null;
    const cash = (bsLatest.cashAndCashEquivalents as number | null) ?? null;
    const providerEbitda = ttmSum(["eBITDA", "ebitda", "normalizedEBITDA", "normalizedEbitda"]);
    // BUG-03: prefer operatingIncome over Yahoo's EBIT to avoid non-cash fair-value items
    const ebit = ttmSum(["operatingIncome", "eBIT", "ebit"]);
    const da = ttmSum([
      "reconciledDepreciation",
      "depreciationAndAmortization",
      "depreciationAmortizationDepletionIncomeStatement",
    ]);
    const interest = ttmSum(["interestExpenseNonOperating", "interestExpense"]);

    // TTM sums are already annualized — no × 4 needed
    const ebitdaAnnual = providerEbitda.value;
    const ebitAnnual = ebit.value;
    const depreciationAmortizationAnnual = da.value;
    const netDebt = totalDebt != null && cash != null ? totalDebt - cash : null;
    let operationalEbitdaAnnual: number | null = null;
    let operationalEbitdaSource: string | null = null;
    if (ebitAnnual != null && depreciationAmortizationAnnual != null) {
      operationalEbitdaAnnual = ebitAnnual + depreciationAmortizationAnnual;
      operationalEbitdaSource = "ttm_operating_income_plus_da";
    } else if (ebitdaAnnual != null) {
      operationalEbitdaAnnual = ebitdaAnnual;
      operationalEbitdaSource = "provider_ebitda_fallback";
    }
    const interestAnnual = interest.value;

    // Leverage needs a positive earnings base: over zero or negative EBITDA the ratio has no meaning (a loss-maker
    // with net debt would read as a negative ratio and the CLEAN tier), so it is withheld with the tier and the
    // stress flag; NEGATIVE_EARNINGS_BASE says why (R5-fundamentals).
    const netDebtToEbitda = netDebt != null && operationalEbitdaAnnual != null && operationalEbitdaAnnual > 0
      ? +(netDebt / operationalEbitdaAnnual).toFixed(2) : null;
    const interestCoverageEbit = ebitAnnual != null && interestAnnual != null && interestAnnual !== 0
      ? +(ebitAnnual / Math.abs(interestAnnual)).toFixed(2) : null;
    const interestCoverageEbitda = operationalEbitdaAnnual != null && interestAnnual != null && interestAnnual !== 0
      ? +(operationalEbitdaAnnual / Math.abs(interestAnnual)).toFixed(2) : null;
    const interestCoverage = interestCoverageEbit;

    let creditStressFlag: boolean | null = null;
    if (netDebtToEbitda != null && interestCoverageEbit != null) {
      creditStressFlag = netDebtToEbitda > 2.5 && interestCoverageEbit < 3;
    }

    let debtTier: string | null = null;
    if (netDebtToEbitda != null) {
      if (netDebtToEbitda < 1) debtTier = "CLEAN";
      else if (netDebtToEbitda <= 2.5) debtTier = "MODERATE";
      else if (netDebtToEbitda <= 4) debtTier = "ELEVATED";
      else debtTier = "STRESSED";
    } else if (netDebt != null && operationalEbitdaAnnual != null && operationalEbitdaAnnual < 0) {
      // A loss-maker has no leverage tier: the state is named instead of reading as CLEAN (R5-fundamentals).
      debtTier = "NEGATIVE_EARNINGS_BASE";
    }

    const missingComponents: string[] = [];
    if (totalDebt == null) missingComponents.push("totalDebtUsd");
    if (cash == null) missingComponents.push("cashUsd");
    if (ebitdaAnnual == null) missingComponents.push("ebitdaUsd");
    if (operationalEbitdaAnnual == null) missingComponents.push("operationalEbitdaUsd");
    if (ebitAnnual == null) missingComponents.push("ebitUsd");
    if (interestAnnual == null) missingComponents.push("interestExpenseUsd");

    const unavailableMetrics: string[] = [];
    if (netDebtToEbitda == null) unavailableMetrics.push("netDebtToEbitda");
    if (interestCoverage == null) unavailableMetrics.push("interestCoverage");
    if (interestCoverageEbit == null) unavailableMetrics.push("interestCoverageEbit");
    if (interestCoverageEbitda == null) unavailableMetrics.push("interestCoverageEbitda");
    if (creditStressFlag == null) unavailableMetrics.push("creditStressFlag");

    const computedMetrics: string[] = [];
    if (netDebt != null) computedMetrics.push("netDebtUsd");
    if (operationalEbitdaAnnual != null) computedMetrics.push("operationalEbitdaUsd");
    if (netDebtToEbitda != null) computedMetrics.push("netDebtToEbitda");
    if (interestCoverage != null) computedMetrics.push("interestCoverage");
    if (interestCoverageEbit != null) computedMetrics.push("interestCoverageEbit");
    if (interestCoverageEbitda != null) computedMetrics.push("interestCoverageEbitda");
    if (creditStressFlag != null) computedMetrics.push("creditStressFlag");
    if (debtTier != null) computedMetrics.push("debtTier");

    const warnings: Record<string, unknown>[] = [];
    if (interestAnnual == null) {
      warnings.push({
        code: "INTEREST_EXPENSE_UNAVAILABLE",
        message: "Interest coverage cannot be computed from available provider data.",
      });
    }
    if (operationalEbitdaSource === "provider_ebitda_fallback") {
      warnings.push({
        code: "OPERATIONAL_EBITDA_UNAVAILABLE",
        message: "Operational EBITDA could not be computed from EBIT plus depreciation and amortization; provider EBITDA is used as a fallback.",
      });
    }
    if (ebitdaAnnual != null && operationalEbitdaAnnual != null) {
      const basis = Math.max(Math.abs(operationalEbitdaAnnual), 1);
      if (Math.abs(ebitdaAnnual - operationalEbitdaAnnual) / basis >= 0.25 && Math.abs(ebitdaAnnual - operationalEbitdaAnnual) >= 100_000_000) {
        warnings.push({
          code: "NON_OPERATING_EBITDA_DIVERGENCE",
          message: "Provider EBITDA materially differs from EBIT plus depreciation and amortization; leverage metrics use operational EBITDA.",
        });
      }
    }
    if ((operationalEbitdaAnnual != null && operationalEbitdaAnnual < 0) || (ebitAnnual != null && ebitAnnual < 0)) {
      warnings.push({
        code: "NEGATIVE_EARNINGS_BASE",
        message: "Company has negative EBIT/EBITDA; leverage metrics may understate operating credit risk despite net cash or low net debt.",
      });
    }
    // BUG-02: flag anomalous interest spike in most-recent quarter vs prior-quarter average
    if (interest.values.length >= 2) {
      const mostRecentQ = Math.abs(interest.values[0]);
      const priorAbs = interest.values.slice(1).map(v => Math.abs(v));
      const priorAvg = priorAbs.reduce((a, b) => a + b, 0) / priorAbs.length;
      if (priorAvg > 0 && mostRecentQ > priorAvg * 2.0) {
        const ratio = +(mostRecentQ / priorAvg).toFixed(1);
        warnings.push({
          code: "INTEREST_EXPENSE_ANOMALY",
          message: `Most recent quarter interest expense (${Math.round(mostRecentQ).toLocaleString()}) is ${ratio}× prior ${priorAbs.length}-quarter average (${Math.round(priorAvg).toLocaleString()}). May include one-time items. Coverage ratios may not reflect ongoing debt service capacity.`,
          mostRecentQuarter: mostRecentQ,
          prior3QAverage: priorAvg,
        });
      }
    }

    const dataQuality = missingComponents.length > 0 ? "PARTIAL" : "OK";
    const quarterDate = (bsLatest.date as string) ?? (incRows[0]?.date as string) ?? null;

    return JSON.stringify({
      ticker,
      quarterDate,
      totalDebtUsd: totalDebt,
      cashUsd: cash,
      netDebtUsd: netDebt,
      ebitdaUsd: ebitdaAnnual,
      ebitdaSource: providerEbitda.source,
      operationalEbitdaUsd: operationalEbitdaAnnual,
      operationalEbitdaSource,
      depreciationAmortizationUsd: depreciationAmortizationAnnual,
      depreciationAmortizationSource: da.source,
      ebitUsd: ebitAnnual,
      interestExpenseUsd: interestAnnual,
      interestExpenseSource: interest.source,
      netDebtToEbitda,
      interestCoverage,
      interestCoverageEbit,
      interestCoverageEbitda,
      creditStressFlag,
      debtTier,
      dataQuality,
      missingComponents,
      unavailableMetrics,
      computedMetrics,
      warnings,
      dataDate: getLastTradingDate(),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

/** A Yahoo earnings-history quarter as YYYY-MM-DD (from the date string or epoch seconds), or null. */
function beatStreakQuarter(value: unknown): string | null {
  if (typeof value === "string") {
    const m = /^(\d{4}-\d{2}-\d{2})/.exec(value);
    return m ? m[1] : null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const d = new Date(value * 1000);
    return Number.isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
  }
  return null;
}

export async function getEarningsMomentum(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getEarningsMomentum(t), tickerCap("analyze_earnings_momentum"));
  }
  try {
    const ea = JSON.parse(await getEarningsAnalysis(ticker));
    if (typeof ea === "string" && ea.startsWith("Error")) {
      return JSON.stringify({ error: true, message: ea, ticker });
    }

    const epsTrend = ea.epsTrend as Record<string, unknown>[] | null;
    const earningsHistory = ea.earningsHistory as Record<string, unknown>[] | null;

    let currentQtrEps: number | null = null;
    let revision7d: number | null = null;
    let revision30d: number | null = null;
    let revision90d: number | null = null;

    if (epsTrend && epsTrend.length > 0) {
      // Find 0q (current quarter)
      const q0 = epsTrend.find((p) => p.period === "0q") ?? epsTrend[0];
      const current = q0.current as number | null;
      const ago7d = q0["7daysAgo"] as number | null;
      const ago30d = q0["30daysAgo"] as number | null;
      const ago90d = q0["90daysAgo"] as number | null;
      currentQtrEps = current;

      // Math.abs() in denominator is intentional: when EPS goes from negative
      // to less-negative (e.g. -0.50→-0.30), the revision is positive.
      // Without abs(), (-0.30-(-0.50))/-0.50 = -40%, incorrectly signaling a downgrade.
      if (current != null && ago7d != null && ago7d !== 0)
        revision7d = +((current - ago7d) / Math.abs(ago7d) * 100).toFixed(2);
      if (current != null && ago30d != null && ago30d !== 0)
        revision30d = +((current - ago30d) / Math.abs(ago30d) * 100).toFixed(2);
      if (current != null && ago90d != null && ago90d !== 0)
        revision90d = +((current - ago90d) / Math.abs(ago90d) * 100).toFixed(2);
    }

    let revisionDirection: string | null = null;
    if (revision30d != null) {
      if (Math.abs(revision30d) < 3) revisionDirection = "STABLE";
      else if (revision30d > 0) revisionDirection = "UPGRADING";
      else revisionDirection = "DOWNGRADING";
    }

    let momentumFlag: string | null = null;
    if (revision30d != null) {
      if (revision30d > 10) momentumFlag = "STRONG";
      else if (revision30d >= 0) momentumFlag = "POSITIVE";
      else if (revision30d > -10) momentumFlag = "NEGATIVE";
      else momentumFlag = "COLLAPSING";
    }

    let beatCount = 0;
    let totalQuarters = 0;
    const surprises: number[] = [];
    let beatStreak: number | null = 0;
    const actualEpsValues: number[] = [];

    if (earningsHistory && earningsHistory.length > 0) {
      for (const h of earningsHistory) {
        const actual = h.epsActual as number | null;
        const estimate = h.epsEstimate as number | null;
        const surprise = h.surprisePercent as number | null;
        if (actual != null && estimate != null) {
          actualEpsValues.push(actual);
          totalQuarters++;
          if (actual > estimate) beatCount++;
          // Yahoo's surprisePercent is a decimal ratio at every size (-2.337 is
          // -233.7%); scaling only values under 1 turned ASTS's -117.5% into -27.0%.
          if (surprise != null) surprises.push(surprise * 100);
        }
      }
      // Beat streak: consecutive beats counted from the most recent quarter. Yahoo lists quarters oldest first,
      // so rows are ordered by their quarter date, newest first; when a counted row has no quarter date the
      // order is unknown and the streak is withheld (R5-fundamentals, BR-SHR-010).
      const dated = earningsHistory
        .filter((h) => h.epsActual != null && h.epsEstimate != null)
        .map((h) => ({ quarter: beatStreakQuarter(h.quarter), actual: h.epsActual as number, estimate: h.epsEstimate as number }));
      if (dated.some((row) => row.quarter == null)) {
        beatStreak = null;
      } else {
        dated.sort((a, b) => ((a.quarter as string) < (b.quarter as string) ? 1 : (a.quarter as string) > (b.quarter as string) ? -1 : 0));
        for (const row of dated) {
          if (row.actual > row.estimate) beatStreak++;
          else break;
        }
      }
    }

    const beatRate = totalQuarters > 0 ? +(beatCount / totalQuarters).toFixed(2) : null;
    const avgSurprise = surprises.length > 0
      ? +(surprises.reduce((a, b) => a + b, 0) / surprises.length).toFixed(2) : null;
    const warnings: Array<{ code: string; message: string }> = [];
    const preRevenue = (totalQuarters === 0 && (!earningsHistory || earningsHistory.length === 0))
      || (actualEpsValues.length > 0 && actualEpsValues.every(v => Math.abs(v) < PRE_REVENUE_EPS_EPSILON));
    if (preRevenue) {
      momentumFlag = "NO_HISTORY";
      warnings.push({
        code: "PRE_REVENUE_NO_HISTORY",
        message: "Earnings history appears pre-revenue or unavailable; momentum fields are not reliable.",
      });
    }

    let historicalSurpriseSignal = "UNKNOWN";
    if (beatRate != null) {
      if (beatRate >= 0.75) historicalSurpriseSignal = "STRONG";
      else if (beatRate >= 0.55) historicalSurpriseSignal = "POSITIVE";
      else if (beatRate >= 0.4) historicalSurpriseSignal = "NEUTRAL";
      else historicalSurpriseSignal = "NEGATIVE";
    }

    let forwardRevisionSignal = "UNKNOWN";
    let revisionSignalDriver = "none";
    if (revision30d != null) {
      revisionSignalDriver = "30d";
      if (revision30d <= -3) forwardRevisionSignal = "NEGATIVE";
      else if (revision30d >= 3) forwardRevisionSignal = "POSITIVE";
      else if (revision7d != null && revision7d <= -3) {
        revisionSignalDriver = "30d_neutral_7d";
        forwardRevisionSignal = "NEGATIVE";
      } else if (revision7d != null && revision7d >= 3) {
        revisionSignalDriver = "30d_neutral_7d";
        forwardRevisionSignal = "POSITIVE";
      } else forwardRevisionSignal = "NEUTRAL";
    } else if (revision7d != null) {
      revisionSignalDriver = "7d";
      if (revision7d <= -3) forwardRevisionSignal = "NEGATIVE";
      else if (revision7d >= 3) forwardRevisionSignal = "POSITIVE";
      else forwardRevisionSignal = "NEUTRAL";
    } else if (revision90d != null) {
      revisionSignalDriver = "90d_fallback";
      if (revision90d <= -3) forwardRevisionSignal = "NEGATIVE";
      else if (revision90d >= 3) forwardRevisionSignal = "POSITIVE";
      else forwardRevisionSignal = "NEUTRAL";
    }
    const compositeMethodNote = "Forward revision signal uses 30d revision as primary, 7d as confirmation when 30d is neutral or missing, and 90d only as fallback/context; a negative 90d revision does not override positive recent revisions.";
    if (revision30d != null && revision30d >= 3 && revision90d != null && revision90d <= -3) {
      warnings.push({
        code: "LONGER_LOOKBACK_REVISION_DIVERGENCE",
        message: "Recent EPS revisions are positive while the 90d revision remains negative.",
      });
    }

    const mixedNegativeRevision = beatRate != null && beatRate >= 0.75
      && [revision30d, revision7d].some((r) => r != null && r <= -3);
    if (mixedNegativeRevision) {
      warnings.push({
        code: "MIXED_EARNINGS_SIGNAL",
        message: "Historical beat streak is positive, but forward estimates were revised down.",
      });
    }

    let compositeMomentumSignal = "UNKNOWN";
    if (historicalSurpriseSignal === "UNKNOWN" && forwardRevisionSignal === "UNKNOWN") {
      compositeMomentumSignal = "UNKNOWN";
    } else if (
      forwardRevisionSignal === "NEGATIVE"
      && (historicalSurpriseSignal === "STRONG" || historicalSurpriseSignal === "POSITIVE")
    ) {
      compositeMomentumSignal = "MIXED_NEGATIVE_REVISION";
    } else if (forwardRevisionSignal === "POSITIVE" && historicalSurpriseSignal === "NEGATIVE") {
      compositeMomentumSignal = "MIXED_POSITIVE_REVISION";
    } else if (
      forwardRevisionSignal === "POSITIVE"
      && (historicalSurpriseSignal === "STRONG" || historicalSurpriseSignal === "POSITIVE")
    ) {
      compositeMomentumSignal = "STRONG_POSITIVE";
    } else if (forwardRevisionSignal === "NEGATIVE") {
      compositeMomentumSignal = "NEGATIVE";
    } else {
      compositeMomentumSignal = "NEUTRAL";
    }

    const interpretationNoteBySignal: Record<string, string> = {
      STRONG_POSITIVE: "Historical earnings surprises and forward estimate revisions are both supportive.",
      MIXED_NEGATIVE_REVISION: "Historical beat performance is strong, but forward revisions are negative.",
      MIXED_POSITIVE_REVISION: "Historical surprise trend is weak, but forward revisions are improving.",
      NEGATIVE: "Both historical surprise trend and forward revisions indicate weakness.",
      NEUTRAL: "Signals are mixed or modest without a strong directional bias.",
      UNKNOWN: "Insufficient data to classify both historical and forward signals.",
    };

    const dataQuality = revision30d == null || beatRate == null ? "PARTIAL" : "OK";

    return JSON.stringify({
      ticker,
      currentQtrEpsEstimate: currentQtrEps,
      revision7d,
      revision30d,
      revision90d,
      revisionDirection,
      momentumFlag,
      beatRate,
      beatCount,
      beatSample: totalQuarters,
      totalQuarters,
      avgSurprisePct: preRevenue ? null : avgSurprise,
      preRevenue,
      currentBeatStreak: beatStreak,
      historicalSurpriseSignal,
      forwardRevisionSignal,
      compositeMomentumSignal,
      interpretationNote: interpretationNoteBySignal[compositeMomentumSignal],
      compositeMethodNote,
      revisionSignalDriver,
      warnings,
      dataQuality,
      dataDate: getLastTradingDate(),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

/** Price-target change of one upgradeDowngradeHistory row. */
export function analystPriceTargetChange(
  entry: Record<string, unknown>,
  signal: string,
): { ptFrom: number | null; ptTo: number | null; ptDirection: string | null } {
  const target = (value: unknown): number | null =>
    typeof value === "number" && Number.isFinite(value) && value > 0 ? value : null;
  const ptTo = target(entry.currentPriceTarget);
  const ptFrom = target(entry.priorPriceTarget);
  const action = String(entry.priceTargetAction ?? "").toLowerCase();
  if (ptFrom != null && ptTo != null) {
    return { ptFrom, ptTo, ptDirection: ptTo > ptFrom ? "RAISED" : ptTo < ptFrom ? "LOWERED" : "UNCHANGED" };
  }
  let ptDirection: string | null = null;
  if (/raise/.test(action)) ptDirection = "RAISED";
  else if (/lower/.test(action)) ptDirection = "LOWERED";
  else if ((/announce|initiat/.test(action) || signal === "INITIATED") && ptTo != null) ptDirection = "INITIATED";
  else if (/maintain/.test(action)) ptDirection = "UNCHANGED";
  return { ptFrom, ptTo, ptDirection };
}

export async function getAnalystUpgradeRadar(ticker: string | string[], daysBack: number): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getAnalystUpgradeRadar(t, daysBack), tickerCap("get_analyst_rating_changes"));
  }

  try {
    // Fetch upgrades/downgrades with enough history
    const monthsBack = Math.max(Math.ceil(daysBack / 30), 2);
    const udRaw = await getRecommendations(ticker, "upgrades_downgrades", monthsBack);
    const ud = JSON.parse(udRaw) as Record<string, unknown>[];

    if (!Array.isArray(ud) || !ud.length) {
      return JSON.stringify({
        ticker,
        windowDays: daysBack,
        netSentiment: 0,
        upgrades: 0,
        downgrades: 0,
        initiations: 0,
        changes: [],
        summary: "NO CHANGES",
        dataDate: getLastTradingDate(),
      });
    }

    const cutoffMs = Date.now() - daysBack * 86400 * 1000;

    const changes: Record<string, unknown>[] = [];
    let upgradeCount = 0;
    let downgradeCount = 0;
    let initiationCount = 0;

    for (const entry of ud) {
      const gradeDate = entry.GradeDate as string | undefined;
      const epochDate = entry.epochGradeDate as number | undefined;

      // Filter by date
      if (epochDate != null && epochDate * 1000 < cutoffMs) continue;
      if (gradeDate != null && new Date(gradeDate).getTime() < cutoffMs) continue;

      const toGrade = (entry.toGrade ?? entry.ToGrade ?? "") as string;
      const fromGrade = (entry.fromGrade ?? entry.FromGrade ?? "") as string;
      const firm = (entry.firm ?? entry.Firm ?? "") as string;
      const action = (entry.action ?? entry.Action ?? "") as string;

      const signal = classifyAnalystChange(action, fromGrade, toGrade);
      if (signal === "UPGRADE") upgradeCount++;
      else if (signal === "DOWNGRADE") downgradeCount++;
      else if (signal === "INITIATED") initiationCount++;

      // Price targets: Yahoo reports currentPriceTarget/priorPriceTarget and a
      // priceTargetAction; 0 means no target. Direction comes from the
      // targets when both exist, else from the action wording.
      const { ptFrom, ptTo, ptDirection } = analystPriceTargetChange(entry, signal);
      const mixedSignal = (signal === "UPGRADE" && ptDirection === "LOWERED") || (signal === "DOWNGRADE" && ptDirection === "RAISED");

      let strengthFlag: string;
      // A mixed signal wins over the rating direction in both directions (2.14.0).
      if (mixedSignal) strengthFlag = "MIXED";
      else if (signal === "UPGRADE") strengthFlag = "BULLISH";
      else if (signal === "DOWNGRADE") strengthFlag = "BEARISH";
      else strengthFlag = "NEUTRAL";

      changes.push({
        date: gradeDate ?? (epochDate ? new Date(epochDate * 1000).toISOString().slice(0, 10) : null),
        firm,
        fromGrade,
        toGrade,
        signal,
        ptFrom,
        ptTo,
        ptDirection,
        mixedSignal,
        strengthFlag,
      });
    }

    const netSentiment = upgradeCount - downgradeCount;
    const parts: string[] = [];
    if (upgradeCount) parts.push(`${upgradeCount} UPGRADE(s)`);
    if (downgradeCount) parts.push(`${downgradeCount} DOWNGRADE(s)`);
    if (initiationCount) parts.push(`${initiationCount} INITIATION(s)`);
    const summary = parts.length > 0 ? parts.join(", ") : "NO CHANGES";

    return JSON.stringify({
      ticker,
      windowDays: daysBack,
      netSentiment,
      upgrades: upgradeCount,
      downgrades: downgradeCount,
      initiations: initiationCount,
      changes,
      summary,
      dataDate: getLastTradingDate(),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

/** How the company names its fiscal years, from its annual reports' stated fy (2.5.13); the period-end rule when SEC is not read. */
export async function secFiscalYearNaming(ticker: string): Promise<FiscalYearNaming> {
  try {
    const cikPadded = await resolveCikForTicker(ticker);
    const facts = cikPadded ? await edgarGetJson(secCompanyFactsUrl(cikPadded)) : null;
    return fiscalYearNaming(facts);
  } catch {
    return fiscalYearNaming(null);
  }
}

const PRE_REVENUE_EPS_EPSILON = 1e-9;

export async function extractAnalystValuationMethods(ticker: string, daysBack = 30): Promise<string> {
  const days = clampInt(daysBack, 30, 1, 365);
  let newsFailed = false;
  let radarFailed = false;
  const [news, radar, identity] = await Promise.all([
    getCompanyNews(ticker, 100, days).then(parseObjectJson).catch(() => { newsFailed = true; return {} as Record<string, unknown>; }),
    getAnalystUpgradeRadar(ticker, days).then(parseObjectJson).catch(() => { radarFailed = true; return {} as Record<string, unknown>; }),
    resolveNewsCompanyIdentity(ticker).catch(() => null),
  ]);
  const items = Array.isArray(news.items) ? news.items as Record<string, unknown>[] : [];
  const changes = Array.isArray(radar.changes) ? radar.changes as Record<string, unknown>[] : [];
  // Targets are attributed to the subject by name; without a name nothing can be checked. A name equal to the ticker is
  // not issuer identity (2.19.0, B-G11 / call #16).
  const names = [identity?.longName, identity?.shortName, identity?.companyName]
    .filter((n): n is string => typeof n === "string" && n.trim() !== "" && n.trim().toLowerCase() !== ticker.trim().toLowerCase());
  const out = analystValuationMethods(ticker.toUpperCase(), items, changes, names.length > 0 ? names : null);
  const warnings: Record<string, unknown>[] = [];
  if (names.length === 0) warnings.push({ code: "ATTRIBUTION_UNCHECKED", message: "The company name could not be resolved, so targets were not checked against the subject.", severity: "warning" });
  // A news error payload or a failed news call is disclosed (2.19.0, B-G11; Python behaviour).
  if (newsFailed || news.error || news.code === "INPUT_VALIDATION_ERROR") warnings.push({ code: "NEWS_UNAVAILABLE", message: String(news.message ?? "News could not be read."), severity: "warning" });
  // A failed radar call is disclosed like a radar error payload (2.20.0, F8; Python behaviour).
  if (radarFailed || radar.error) warnings.push({ code: "RATING_CHANGES_UNAVAILABLE", message: String(radar.message ?? "Rating changes could not be read."), severity: "warning" });
  out.windowDays = days;
  out.newsSourcesUsed = (news.meta as Record<string, unknown> | undefined)?.sourcesUsed ?? [];
  out.warnings = warnings;
  return JSON.stringify(out);
}

const PEER_VALUATION_MAX_TICKERS = 10;

/**
 * A peer that failed, with an existing error code and no provider URL (2.26.5, F57): an unknown ticker (HTTP 404, or
 * no quote summary) is TICKER_NOT_FOUND, anything else PROVIDER_ERROR.
 */
export function peerValuationError(ticker: string, message: string): { ticker: string; code: string; message: string } {
  if (/api error 404|no yahoo quote summary|not found/i.test(message)) {
    return { ticker, code: "TICKER_NOT_FOUND", message: "No provider data was found for the requested ticker." };
  }
  return { ticker, code: "PROVIDER_ERROR", message: snapshotComponentMessage(message) };
}

export async function comparePeerValuations(tickers: string[], subject: string | null = null): Promise<string> {
  const list: string[] = [];
  for (const t of [subject, ...tickers]) {
    const u = typeof t === "string" ? t.trim().toUpperCase() : "";
    if (u && !list.includes(u)) list.push(u);
  }
  if (list.length < 2 || list.length > PEER_VALUATION_MAX_TICKERS) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: `tickers must name 2 to ${PEER_VALUATION_MAX_TICKERS} distinct symbols, including the subject.` });
  }
  const results = await Promise.all(list.map((t) => valuationMarketInputs(t).then(
    (m) => ({ ok: true as const, m }),
    (e) => ({ ok: false as const, error: peerValuationError(t, e instanceof Error ? e.message : String(e)) }),
  )));
  const markets = results.filter((r): r is { ok: true; m: MarketInputs } => r.ok).map((r) => r.m);
  const errors = results.filter((r): r is { ok: false; error: { ticker: string; code: string; message: string } } => !r.ok).map((r) => r.error);
  if (markets.length === 0) {
    return JSON.stringify({ error: true, message: `Yahoo quote summary failed for every ticker: ${errors.map((e) => e.message).join("; ")}` });
  }
  return JSON.stringify(peerValuations(subject, markets, errors));
}
