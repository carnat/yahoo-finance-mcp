/**
 * Yahoo Finance API client for Cloudflare Workers.
 * Calls Yahoo Finance HTTP endpoints directly (replacing yfinance + pandas).
 *
 * Endpoints that need a session crumb:   /v10/finance/quoteSummary/, /v7/finance/options/,
 *                                        /ws/fundamentals-timeseries/
 * Endpoints that work without auth:      /v8/finance/chart/, /v1/finance/search
 */

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

/**
 * Maximum number of tickers allowed in a single batch call.
 * Cloudflare Workers have a subrequest limit (50 on free, 1000 on paid).
 * Each ticker requires 1-3 subrequests (crumb + API call + possible retry),
 * so capping at 5 keeps a single tool invocation well within limits.
 */
const MAX_TICKERS = 5;

interface LimitResult {
  tickers: string[];
  truncatedFrom?: number;
  dropped?: string[];
}

function limitTickers(tickers: string[]): LimitResult {
  if (tickers.length <= MAX_TICKERS) return { tickers };
  return {
    tickers: tickers.slice(0, MAX_TICKERS),
    truncatedFrom: tickers.length,
    dropped: tickers.slice(MAX_TICKERS),
  };
}

/** Adds a _truncated warning to the batch response object when tickers were dropped. */
function wrapBatchResult(
  result: Record<string, unknown>,
  limit: LimitResult
): string {
  if (!limit.truncatedFrom) return JSON.stringify(result);
  return JSON.stringify({
    ...result,
    _truncated: {
      message: `Only the first ${MAX_TICKERS} of ${limit.truncatedFrom} tickers were processed. Call again with the remaining tickers: ${limit.dropped!.join(", ")}`,
      processed: limit.tickers,
      remaining: limit.dropped,
    },
  });
}

/**
 * Returns the last trading date as a YYYY-MM-DD string.
 * If a unix-seconds timestamps array is provided, uses the last element.
 * Falls back to the last weekday from the UTC system clock.
 * Note: does not account for market holidays — weekday fallback only.
 */
function getLastTradingDate(timestamps?: number[]): string {
  if (timestamps && timestamps.length > 0) {
    return new Date(timestamps[timestamps.length - 1] * 1000).toISOString().slice(0, 10);
  }
  const d = new Date();
  let safety = 0;
  while ((d.getUTCDay() === 0 || d.getUTCDay() === 6) && safety++ < 7) {
    d.setUTCDate(d.getUTCDate() - 1);
  }
  return d.toISOString().slice(0, 10);
}

// Module-level crumb cache — shared within a Cloudflare isolate session
let _crumb: { value: string; cookie: string; exp: number } | null = null;

async function refreshCrumb(): Promise<{ value: string; cookie: string }> {
  // fc.yahoo.com sets the consent cookie used by Yahoo Finance APIs
  const init = await fetch("https://fc.yahoo.com", {
    headers: { "User-Agent": UA },
    redirect: "follow",
  });

  // Collect all Set-Cookie headers (Workers Headers.entries() exposes them)
  const cookies: string[] = [];
  for (const [name, value] of init.headers.entries()) {
    if (name.toLowerCase() === "set-cookie") {
      cookies.push(value.split(";")[0].trim());
    }
  }
  const cookie = cookies.join("; ");

  // Cancel the response body — we only needed headers.
  // Leaving it unconsumed stalls a Cloudflare HTTP slot and can trigger
  // "stalled response canceled to prevent deadlock" warnings.
  await init.body?.cancel();

  const crumbRes = await fetch("https://query2.finance.yahoo.com/v1/test/getcrumb", {
    headers: { "User-Agent": UA, Cookie: cookie },
  });
  if (!crumbRes.ok) {
    await crumbRes.body?.cancel();
    throw new Error(`Crumb fetch failed: ${crumbRes.status}`);
  }

  return { value: await crumbRes.text(), cookie };
}

async function getCrumb(): Promise<{ value: string; cookie: string }> {
  if (_crumb && Date.now() < _crumb.exp) return _crumb;
  const { value, cookie } = await refreshCrumb();
  _crumb = { value, cookie, exp: Date.now() + 3_600_000 }; // 1-hour cache
  return _crumb;
}

async function yGet(url: string, auth = true): Promise<unknown> {
  const makeReq = async (c?: { value: string; cookie: string }): Promise<Response> => {
    const headers: Record<string, string> = { "User-Agent": UA };
    let u = url;
    if (c) {
      headers.Cookie = c.cookie;
      u += `${u.includes("?") ? "&" : "?"}crumb=${encodeURIComponent(c.value)}`;
    }
    return fetch(u, { headers });
  };

  let res = await makeReq(auth ? await getCrumb() : undefined);

  // Retry once with a fresh crumb on 401
  if (res.status === 401 && auth) {
    // Cancel the unconsumed response body to free the HTTP slot
    await res.body?.cancel();
    _crumb = null;
    res = await makeReq(await getCrumb());
  }

  if (!res.ok) {
    await res.body?.cancel();
    throw new Error(`Yahoo Finance API error ${res.status} for: ${url}`);
  }
  return res.json();
}

// Extract the `raw` numeric value from Yahoo Finance's {raw, fmt} wrapper objects
function raw(v: unknown): unknown {
  if (v !== null && v !== undefined && typeof v === "object" && "raw" in (v as object)) {
    return (v as { raw: unknown }).raw;
  }
  return v ?? null;
}

const enc = encodeURIComponent;
const iso = (ts: number) => new Date(ts * 1000).toISOString();
const noData = (t: string) => `Error: no data found for ticker ${t}`;

/** Parse a JSON string returned by a single-ticker handler, falling back to a
 *  structured error object if the string is not valid JSON (e.g. plain error messages). */
function safeJsonParse(s: string, ticker: string): Record<string, unknown> {
  try {
    return JSON.parse(s) as Record<string, unknown>;
  } catch {
    return { error: true, message: s, ticker };
  }
}


// ── get_etf_info ─────────────────────────────────────────────────────────────

export async function getEtfInfo(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) {
      results.push(await getEtfInfo(t));
    }
    return wrapBatchResult(
      Object.fromEntries(limit.tickers.map((t, i) => [t, safeJsonParse(results[i], t)])),
      limit
    );
  }

  try {
    const modules = [
      "summaryDetail",
      "defaultKeyStatistics",
      "topHoldings",
      "fundPerformance",
      "price",
    ].join(",");

    const d = (await yGet(
      `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=${modules}`
    )) as Record<string, unknown>;

    const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
      | Record<string, unknown>
      | undefined;
    if (!result) return noData(ticker);

    const priceData = result.price as Record<string, unknown> | undefined;
    const summary = result.summaryDetail as Record<string, unknown> | undefined;
    const keyStats = result.defaultKeyStatistics as Record<string, unknown> | undefined;
    const topHoldingsData = result.topHoldings as Record<string, unknown> | undefined;
    const fundPerf = result.fundPerformance as Record<string, unknown> | undefined;

    const pick = (src: Record<string, unknown> | undefined, key: string): unknown =>
      src ? raw(src[key]) : null;

    const data: Record<string, unknown> = {
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

    // Top holdings (up to 10)
    if (topHoldingsData) {
      const holdings = (topHoldingsData.holdings as Array<Record<string, unknown>> | undefined) ?? [];
      data.topHoldings = holdings.slice(0, 10).map((h) => ({
        symbol: h.symbol,
        holdingName: h.holdingName,
        pct: raw(h.holdingPercent),
      }));

      const sw = (topHoldingsData.sectorWeightings as Array<Record<string, unknown>> | undefined) ?? [];
      data.sectorWeights = sw
        .map((s) => {
          const entries = Object.entries(s);
          if (entries.length === 0) return null;
          const [sector, rawWeight] = entries[0];
          return { sector, weight: raw(rawWeight) };
        })
        .filter(Boolean);
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

    return JSON.stringify(data);
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}


export async function getHistoricalPrices(
  ticker: string,
  period: string,
  interval: string,
  prepost: boolean = false
): Promise<string> {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${enc(ticker)}?range=${period}&interval=${interval}${prepost ? "&includePrePost=true" : ""}`;
  const d = (await yGet(url, false)) as Record<string, unknown>;

  const result = (d?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  const timestamps = (result.timestamp as number[]) ?? [];
  const quote = ((result.indicators as Record<string, unknown[]>)?.quote?.[0] as Record<
    string,
    (number | null)[]
  >) ?? {};
  const adjclose =
    ((result.indicators as Record<string, unknown[]>)?.adjclose?.[0] as Record<
      string,
      (number | null)[]
    >)?.adjclose ?? [];

  return JSON.stringify(
    timestamps.map((t, i) => ({
      Date: iso(t),
      Open: quote.open?.[i] ?? null,
      High: quote.high?.[i] ?? null,
      Low: quote.low?.[i] ?? null,
      Close: quote.close?.[i] ?? null,
      Volume: quote.volume?.[i] ?? null,
      "Adj Close": adjclose[i] ?? null,
    }))
  );
}

export async function getStockInfo(ticker: string | string[], includeAll = false): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) results.push(await getStockInfo(t, includeAll));
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, JSON.parse(results[i])])), limit);
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
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=${modules}`
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
        info[k] = raw(v);
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

export async function getNews(ticker: string): Promise<string> {
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v1/finance/search?q=${enc(ticker)}&quotesCount=0&newsCount=20&enableFuzzyQuery=false`,
    false
  )) as Record<string, unknown>;

  const news = (d?.news as Record<string, unknown>[]) ?? [];
  const items = news.map(
    (n) =>
      `Title: ${n.title ?? ""}\nPublisher: ${n.publisher ?? ""}\nURL: ${n.link ?? ""}\nDate: ${n.providerPublishTime ? iso(n.providerPublishTime as number) : ""}`
  );
  return items.length > 0 ? items.join("\n\n") : `No news found for ${ticker}`;
}

export async function getStockActions(ticker: string): Promise<string> {
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v8/finance/chart/${enc(ticker)}?range=max&interval=1d&events=div%2Csplit`,
    false
  )) as Record<string, unknown>;

  const result = (d?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  const events = (result?.events as Record<string, Record<string, Record<string, number>>>) ?? {};

  type Row = { Date: string; Dividends: number; "Stock Splits": number };
  const rows: Row[] = [];

  for (const v of Object.values(events.dividends ?? {})) {
    rows.push({ Date: iso(v.date), Dividends: v.amount, "Stock Splits": 0 });
  }
  for (const v of Object.values(events.splits ?? {})) {
    rows.push({
      Date: iso(v.date),
      Dividends: 0,
      "Stock Splits": v.numerator / v.denominator,
    });
  }

  rows.sort((a, b) => a.Date.localeCompare(b.Date));
  return JSON.stringify(rows);
}

// ── Financial statements via fundamentals timeseries API ────────────────────
//
// The quoteSummary v10 API returns only a small subset of income/balance/cash
// statement fields and frequently returns {raw:0} for fields it doesn't fully
// populate (e.g. grossProfit, costOfRevenue, all balance sheet numerics).
// Balance sheet results contain only the endDate — no numeric data.
//
// yfinance >= 0.2.x switched to the fundamentals-timeseries API for this
// reason. We do the same here.

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
};

async function fetchTimeseries(
  ticker: string,
  prefix: string,
  baseTypes: string[]
): Promise<string> {
  const types = baseTypes.map((t) => `${prefix}${t}`);
  // period1: 1985-08-20 (yfinance default); period2: now
  const p2 = Math.floor(Date.now() / 1000);
  const d = (await yGet(
    `https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/${enc(ticker)}` +
      `?type=${types.join(",")}&period1=493590046&period2=${p2}` +
      `&lang=en-US&region=US&corsDomain=finance.yahoo.com`
  )) as Record<string, unknown>;

  const results =
    ((d?.timeseries as Record<string, unknown> | undefined)?.result as Record<
      string,
      unknown
    >[]) ?? [];

  if (!results.length) return noData(ticker);

  // Merge all type arrays into a {date → {field: value}} map
  const byDate: Record<string, Record<string, unknown>> = {};

  for (const item of results) {
    // meta.type is a string in some API versions and an array in others
    const rawType = ((item.meta as Record<string, unknown>) ?? {}).type;
    const typeName = (Array.isArray(rawType) ? (rawType[0] ?? "") : (rawType ?? "")) as string;
    // Strip the prefix (e.g. "annual" / "quarterly") → camelCase field key
    // "annualGrossProfit" → "grossProfit"
    const stripped = typeName.slice(prefix.length);
    const key = stripped.charAt(0).toLowerCase() + stripped.slice(1);

    const values = (
      item[typeName] as
        | Array<{ asOfDate: string; reportedValue?: { raw: unknown } } | null>
        | undefined
    ) ?? [];

    for (const val of values) {
      if (!val) continue;
      const { asOfDate } = val;
      if (!byDate[asOfDate]) byDate[asOfDate] = { date: asOfDate };
      byDate[asOfDate][key] = val.reportedValue?.raw ?? null;
    }
  }

  // Sort most-recent first
  const records = Object.values(byDate).sort((a, b) =>
    (b.date as string).localeCompare(a.date as string)
  );

  // DEBUG: if byDate ended up empty, surface what the API actually returned
  if (records.length === 0) {
    const firstItem = results[0] as Record<string, unknown> | undefined;
    const firstMeta = firstItem?.meta as Record<string, unknown> | undefined;
    const firstType = firstMeta?.type;
    const firstKey = Array.isArray(firstType) ? firstType[0] : firstType;
    // Include raw response snippet so we can see the actual shape
    const rawSnippet = JSON.stringify(d).slice(0, 1500);
    const debug = {
      resultsCount: results.length,
      firstItemType: firstType,
      firstItemKeys: firstItem ? Object.keys(firstItem) : null,
      firstItemRaw: firstItem,  // full first item for structure inspection
      firstItemDataSample: firstItem && firstKey ? firstItem[firstKey as string] : null,
      rawResponseSnippet: rawSnippet,
    };
    return JSON.stringify({ debug, data: [] });
  }

  return JSON.stringify(records);
}

export async function getFinancialStatement(ticker: string, type: string): Promise<string> {
  const cfg = TIMESERIES_FS_CONFIG[type];
  if (!cfg) return `Error: invalid financial type '${type}'`;

  try {
    return await fetchTimeseries(ticker, cfg.prefix, cfg.baseTypes);
  } catch (e) {
    return `Error fetching financial statement for ${ticker}: ${e instanceof Error ? e.message : String(e)}`;
  }
}

const HOLDER_MOD: Record<string, string> = {
  major_holders: "majorHoldersBreakdown",
  institutional_holders: "institutionOwnership",
  mutualfund_holders: "fundOwnership",
  insider_transactions: "insiderTransactions",
  insider_purchases: "netSharePurchaseActivity",
  insider_roster_holders: "insiderHolders",
};

export async function getHolderInfo(ticker: string, type: string): Promise<string> {
  const mod = HOLDER_MOD[type];
  if (!mod) return `Error: invalid holder type '${type}'`;

  const d = (await yGet(
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=${mod}`
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  return JSON.stringify(result[mod]);
}

export async function getOptionExpirationDates(ticker: string): Promise<string> {
  // auth=true (default): the v7 options endpoint now requires a crumb;
  // passing false caused 401 → uncaught throw → "Error occurred during tool execution".
  try {
    const d = (await yGet(
      `https://query2.finance.yahoo.com/v7/finance/options/${enc(ticker)}`
    )) as Record<string, unknown>;

    const result = (d?.optionChain as Record<string, unknown[]> | undefined)?.result?.[0] as
      | Record<string, unknown>
      | undefined;
    if (!result) return noData(ticker);

    const dates = ((result.expirationDates as number[]) ?? []).map((ts) =>
      new Date(ts * 1000).toISOString().split("T")[0]
    );
    return JSON.stringify(dates);
  } catch (e) {
    return `Error fetching option expiration dates for ${ticker}: ${e instanceof Error ? e.message : String(e)}`;
  }
}

export async function getOptionChain(
  ticker: string,
  expDate: string,
  optType: string
): Promise<string> {
  if (!["calls", "puts"].includes(optType)) {
    return `Error: option_type must be 'calls' or 'puts'`;
  }

  // auth=true (default): same crumb requirement as getOptionExpirationDates.
  try {
    const [y, m, day] = expDate.split("-").map(Number);
    const ts = Math.floor(Date.UTC(y, m - 1, day) / 1000);

    const d = (await yGet(
      `https://query2.finance.yahoo.com/v7/finance/options/${enc(ticker)}?date=${ts}`
    )) as Record<string, unknown>;

    const result = (d?.optionChain as Record<string, unknown[]> | undefined)?.result?.[0] as
      | Record<string, unknown>
      | undefined;
    const opts = (result?.options as Record<string, unknown[]>[])?.[0];
    if (!opts) return `Error: no options found for ${ticker} on ${expDate}`;

    // Derive dataDate from the quote's regularMarketTime (most recent trading session)
    const quote = (result?.quote as Record<string, unknown>) ?? {};
    const regMktTime = typeof quote.regularMarketTime === "number" ? quote.regularMarketTime : null;
    const dataDate = regMktTime
      ? new Date(regMktTime * 1000).toISOString().slice(0, 10)
      : new Date().toISOString().slice(0, 10);

    return JSON.stringify({
      ticker,
      expiration: expDate,
      optionType: optType,
      dataDate,
      contracts: opts[optType] ?? [],
    });
  } catch (e) {
    return `Error fetching option chain for ${ticker} on ${expDate}: ${e instanceof Error ? e.message : String(e)}`;
  }
}

/**
 * Internal helper: fetch full options response in a single subrequest.
 * Returns { dates: string[], calls: Record[], puts: Record[] }.
 * The Yahoo Finance options endpoint returns expiration dates AND the full
 * chain (calls + puts) for the specified date in one response, so we avoid
 * making 3 separate calls (dates + calls + puts) which wastes subrequests.
 */
async function yGetFullOptions(
  ticker: string,
  expDate?: string
): Promise<{ dates: string[]; calls: Record<string, unknown>[]; puts: Record<string, unknown>[] }> {
  let url = `https://query2.finance.yahoo.com/v7/finance/options/${enc(ticker)}`;
  if (expDate) {
    const [y, m, day] = expDate.split("-").map(Number);
    const ts = Math.floor(Date.UTC(y, m - 1, day) / 1000);
    url += `?date=${ts}`;
  }
  const d = (await yGet(url)) as Record<string, unknown>;
  const result = (d?.optionChain as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) throw new Error(`No options data for ${ticker}`);

  const dates = ((result.expirationDates as number[]) ?? []).map((ts) =>
    new Date(ts * 1000).toISOString().split("T")[0]
  );
  const opts = (result.options as Record<string, unknown[]>[])?.[0] ?? {};
  return {
    dates,
    calls: (opts.calls ?? []) as Record<string, unknown>[],
    puts: (opts.puts ?? []) as Record<string, unknown>[],
  };
}

const REC_MOD: Record<string, string> = {
  recommendations: "recommendationTrend",
  upgrades_downgrades: "upgradeDowngradeHistory",
};

// ── New tools ────────────────────────────────────────────────────────────────

export async function getFastInfo(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) results.push(await getFastInfo(t));
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, JSON.parse(results[i])])), limit);
  }
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=price,summaryDetail,defaultKeyStatistics`
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  const price = (result.price as Record<string, unknown>) ?? {};
  const detail = (result.summaryDetail as Record<string, unknown>) ?? {};
  const ks = (result.defaultKeyStatistics as Record<string, unknown>) ?? {};

  const quoteType = raw(price.quoteType) as string | null;
  // Indices (^VIX, ^VVIX, ^SPX …) have no volume or outstanding shares.
  // Yahoo Finance fills these fields with 0 rather than null, so we
  // normalise them back to null to avoid misleading zero values.
  const isIndex = quoteType === "INDEX";
  const volOrNull = (v: unknown): number | null => {
    const val = raw(v) as number | null;
    return isIndex ? null : val;
  };

  // Shares: prefer price module; fall back to defaultKeyStatistics (fixes
  // small-caps like ASTS where price.sharesOutstanding is missing).
  const shares = isIndex
    ? null
    : ((raw(price.sharesOutstanding) as number | null) ??
       (raw(ks.sharesOutstanding) as number | null));

  const regMktTime = raw(price.regularMarketTime) as number | null;
  const postMktTime = raw(price.postMarketTime) as number | null;

  return JSON.stringify({
    currency: raw(price.currency),
    exchange: raw(price.exchangeName),
    quoteType,
    lastPrice: raw(price.regularMarketPrice),
    open: raw(price.regularMarketOpen),
    previousClose: raw(price.regularMarketPreviousClose),
    dayHigh: raw(price.regularMarketDayHigh),
    dayLow: raw(price.regularMarketDayLow),
    yearHigh: raw(detail.fiftyTwoWeekHigh),
    yearLow: raw(detail.fiftyTwoWeekLow),
    yearChange: raw(ks["52WeekChange" as keyof typeof ks]),
    marketCap: isIndex ? null : (raw(price.marketCap) as number | null),
    shares,
    lastVolume: volOrNull(price.regularMarketVolume),
    tenDayAverageVolume: volOrNull(detail.averageVolume10days),
    threeMonthAverageVolume: volOrNull(detail.averageVolume),
    fiftyDayAverage: raw(detail.fiftyDayAverage),
    twoHundredDayAverage: raw(detail.twoHundredDayAverage),
    preMarketPrice: raw(price.preMarketPrice),
    postMarketPrice: raw(price.postMarketPrice),
    marketOpen: (price.marketState as string | null) === "REGULAR",
    lastTradeDate: regMktTime != null ? new Date(regMktTime * 1000).toISOString().slice(0, 10) : null,
    postMarketTimestamp: postMktTime != null ? new Date(postMktTime * 1000).toISOString() : null,
    ...(isIndex ? { _note: "Index ticker — volume, shares, and marketCap are not applicable and are returned as null" } : {}),
  });
}

// ── get_overnight_quote helpers ───────────────────────────────────────────────

function dataAgeHoursFromTs(tsSeconds: number, nowMs: number): number {
  return Math.round((nowMs - tsSeconds * 1000) / 3_600_000 * 10) / 10;
}

function gapPctFromClose(price: number, prevClose: number): number {
  return Math.round(((price - prevClose) / prevClose * 100) * 100) / 100;
}

// ── get_overnight_quote ───────────────────────────────────────────────────────

export async function getOvernightQuote(ticker: string): Promise<string> {
  try {
    // Fetch chart data and quoteSummary price in parallel.
    // The chart meta.regularMarketPreviousClose is often null for equities;
    // the quoteSummary `price` module reliably has regularMarketPreviousClose.
    const [d, priceD] = await Promise.all([
      yGet(
        `https://query1.finance.yahoo.com/v8/finance/chart/${enc(ticker)}?range=5d&interval=1h&includePrePost=true`,
        false
      ),
      yGet(
        `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=price`
      ).catch(() => null),
    ]) as [Record<string, unknown>, Record<string, unknown> | null];

    const result = (d?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
      | Record<string, unknown>
      | undefined;
    if (!result) return noData(ticker);

    const meta = (result.meta as Record<string, unknown>) ?? {};
    const tzName = (meta.exchangeTimezoneName as string | undefined) ?? "UTC";

    // prevClose: chart meta first, then quoteSummary price module fallback
    const chartPrevClose = typeof meta.regularMarketPreviousClose === "number"
      ? (meta.regularMarketPreviousClose as number)
      : null;
    const priceResult = (priceD?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
      | Record<string, unknown>
      | undefined;
    const priceMod = (priceResult?.price as Record<string, unknown>) ?? {};
    const qsPrevClose = raw(priceMod.regularMarketPreviousClose) as number | null;
    const prevClose = chartPrevClose ?? qsPrevClose;

    const timestamps = (result.timestamp as number[]) ?? [];
    const quoteBlock = ((result.indicators as Record<string, unknown[]>)?.quote?.[0] as
      | Record<string, (number | null)[]>
      | undefined) ?? {};

    const opens   = quoteBlock.open   ?? [];
    const highs   = quoteBlock.high   ?? [];
    const lows    = quoteBlock.low    ?? [];
    const closes  = quoteBlock.close  ?? [];
    const volumes = quoteBlock.volume ?? [];

    // TRUE overnight window: 00:00–08:00 UTC (= 20:00–04:00 ET / Blue Ocean ATS).
    // Group bars by UTC calendar date, keeping only bars whose UTC hour is 0–7.
    const overnightByDate = new Map<string, {
      opens: number[];
      highs: number[];
      lows: number[];
      closes: number[];
      volumes: number[];
      times: number[];
    }>();

    for (let i = 0; i < timestamps.length; i++) {
      const ts = timestamps[i];
      const d = new Date(ts * 1000);
      const utcHour = d.getUTCHours();
      if (utcHour >= 8) continue; // outside 00:00–08:00 UTC overnight window

      // UTC date key "YYYY-MM-DD"
      const utcDateStr = d.toISOString().slice(0, 10);

      if (!overnightByDate.has(utcDateStr)) {
        overnightByDate.set(utcDateStr, { opens: [], highs: [], lows: [], closes: [], volumes: [], times: [] });
      }
      const bucket = overnightByDate.get(utcDateStr)!;
      if (opens[i] != null)   bucket.opens.push(opens[i]!);
      if (highs[i] != null)   bucket.highs.push(highs[i]!);
      if (lows[i] != null)    bucket.lows.push(lows[i]!);
      if (closes[i] != null)  bucket.closes.push(closes[i]!);
      if (volumes[i] != null) bucket.volumes.push(volumes[i]!);
      bucket.times.push(ts);
    }

    const nowMs = Date.now();

    if (overnightByDate.size === 0) {
      // Fallback: most recent prepost bar before 13:30 UTC (09:30 ET market open)
      let fallbackIdx = -1;
      for (let i = timestamps.length - 1; i >= 0; i--) {
        const utcHour = new Date(timestamps[i] * 1000).getUTCHours();
        if (utcHour < 13) {
          fallbackIdx = i;
          break;
        }
      }

      if (fallbackIdx === -1) {
        return JSON.stringify({
          ticker,
          overnightPrice: null,
          overnightTime: null,
          overnightHigh: null,
          overnightLow: null,
          overnightOpen: null,
          overnightVolume: null,
          _note: "No overnight or pre-market data found for this ticker",
        });
      }

      const fbTs = timestamps[fallbackIdx];
      const fbClose = closes[fallbackIdx] ?? null;
      const fbVol = volumes[fallbackIdx] ?? 0;
      const fbDataAgeHours = dataAgeHoursFromTs(fbTs, nowMs);
      const fbGapPct = prevClose && fbClose != null
        ? gapPctFromClose(fbClose, prevClose)
        : null;

      return JSON.stringify({
        ticker,
        overnightPrice: fbClose,
        overnightTime: iso(fbTs),
        overnightHigh: highs[fallbackIdx] ?? null,
        overnightLow: lows[fallbackIdx] ?? null,
        overnightOpen: opens[fallbackIdx] ?? null,
        overnightVolume: typeof fbVol === "number" ? Math.round(fbVol) : null,
        sessionDate: new Date(fbTs * 1000).toISOString().slice(0, 10),
        timezone: tzName,
        previousClose: prevClose,
        gapPct: fbGapPct,
        gapDirection: fbGapPct === null ? null : fbGapPct > 0.1 ? "UP" : fbGapPct < -0.1 ? "DOWN" : "FLAT",
        dataSource: (typeof fbVol === "number" && fbVol > 0) ? "EXCHANGE" : "OTC_INDICATIVE",
        isBlueOceanWindow: false,
        isStale: fbDataAgeHours > 6,
        dataAgeHours: fbDataAgeHours,
        fallback: true,
        note: "True overnight window (20:00–04:00 ET) unavailable via Yahoo Finance API. Returning last pre-market OTC indicative quote as proxy.",
      });
    }

    // Use the most recent UTC date with overnight data
    const latestDate = [...overnightByDate.keys()].sort().at(-1)!;
    const bars = overnightByDate.get(latestDate)!;

    const overnightOpen   = bars.opens.length   > 0 ? bars.opens[0]                               : null;
    const overnightHigh   = bars.highs.length   > 0 ? Math.max(...bars.highs)                     : null;
    const overnightLow    = bars.lows.length    > 0 ? Math.min(...bars.lows)                      : null;
    const overnightPrice  = bars.closes.length  > 0 ? bars.closes[bars.closes.length - 1]         : null;
    const overnightTime   = bars.times.length   > 0 ? iso(bars.times[bars.times.length - 1])      : null;
    const overnightVolume = bars.volumes.length > 0 ? bars.volumes.reduce((a, b) => a + b, 0)     : null;

    const lastTs = bars.times.length > 0 ? bars.times[bars.times.length - 1] : null;
    const dataAgeHours = lastTs != null ? dataAgeHoursFromTs(lastTs, nowMs) : null;
    const lastUtcHour = lastTs != null ? new Date(lastTs * 1000).getUTCHours() : null;
    const isBlueOceanWindow = lastUtcHour != null ? lastUtcHour < 8 : false;
    const dataSource = (overnightVolume ?? 0) > 0 ? "EXCHANGE" : "OTC_INDICATIVE";
    const gapPct = prevClose && overnightPrice != null
      ? gapPctFromClose(overnightPrice, prevClose)
      : null;

    return JSON.stringify({
      ticker,
      overnightPrice,
      overnightTime,
      overnightHigh,
      overnightLow,
      overnightOpen,
      overnightVolume,
      sessionDate: latestDate,
      timezone: tzName,
      previousClose: prevClose,
      gapPct,
      gapDirection: gapPct === null ? null : gapPct > 0.1 ? "UP" : gapPct < -0.1 ? "DOWN" : "FLAT",
      dataSource,
      isBlueOceanWindow,
      isStale: dataAgeHours != null ? dataAgeHours > 6 : null,
      dataAgeHours,
      fallback: false,
      note: null,
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

export async function getPriceStats(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) results.push(await getPriceStats(t));
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, JSON.parse(results[i])])), limit);
  }
  const [metaRaw, histRaw] = await Promise.all([
    yGet(
      `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=price,summaryDetail`
    ),
    yGet(
      `https://query1.finance.yahoo.com/v8/finance/chart/${enc(ticker)}?range=5y&interval=1d`,
      false
    ),
  ]);

  const meta = metaRaw as Record<string, unknown>;
  const result = (meta?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  const price = (result.price as Record<string, unknown>) ?? {};
  const detail = (result.summaryDetail as Record<string, unknown>) ?? {};

  const lastPrice = raw(price.regularMarketPrice) as number | null;
  const prevClose = raw(price.regularMarketPreviousClose) as number | null;
  const yearHigh = raw(detail.fiftyTwoWeekHigh) as number | null;
  const yearLow = raw(detail.fiftyTwoWeekLow) as number | null;
  const fiftyDayAvg = raw(detail.fiftyDayAverage) as number | null;
  const twoHundredDayAvg = raw(detail.twoHundredDayAverage) as number | null;

  const pct = (v: number | null, ref: number | null): number | null =>
    v != null && ref != null && ref !== 0 ? +((v - ref) / ref * 100).toFixed(4) : null;

  const stats: Record<string, unknown> = {
    ticker,
    currency: raw(price.currency),
    lastPrice,
    previousClose: prevClose,
    pctChangeTodayVsPrevClose: pct(lastPrice, prevClose),
    yearHigh,
    yearLow,
    pctFromYearHigh: pct(lastPrice, yearHigh),
    pctFromYearLow: pct(lastPrice, yearLow),
    fiftyDayAverage: fiftyDayAvg,
    twoHundredDayAverage: twoHundredDayAvg,
    pctFromFiftyDayAvg: pct(lastPrice, fiftyDayAvg),
    pctFromTwoHundredDayAvg: pct(lastPrice, twoHundredDayAvg),
  };

  let chartTimestamps: number[] = [];
  try {
    const hist = histRaw as Record<string, unknown>;
    const chartResult = (hist?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
      | Record<string, unknown>
      | undefined;
    if (chartResult) {
      const timestamps = (chartResult.timestamp as number[]) ?? [];
      chartTimestamps = timestamps;
      const adjClose =
        ((chartResult.indicators as Record<string, unknown[]>)?.adjclose?.[0] as Record<
          string,
          (number | null)[]
        >)?.adjclose ?? [];
      const closes = adjClose.filter((v): v is number => v != null);

      if (closes.length >= 20) {
        const last31 = closes.slice(-31);
        const returns = last31.slice(1).map((c, i) => Math.log(c / last31[i]));
        const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
        const variance = returns.reduce((a, b) => a + (b - mean) ** 2, 0) / returns.length;
        stats.annualizedVolatility30d = +(Math.sqrt(variance * 252) * 100).toFixed(4);
      }

      const cagr = (years: number): number | null => {
        const cutoffMs = Date.now() - years * 365.25 * 86400 * 1000;
        const idx = timestamps.findIndex((t) => t * 1000 >= cutoffMs);
        if (idx < 0 || idx >= closes.length - 1) return null;
        const start = adjClose[idx];
        const end = closes[closes.length - 1];
        if (start == null || start <= 0) return null;
        return +((Math.pow(end / start, 1 / years) - 1) * 100).toFixed(4);
      };

      stats.cagr1y = cagr(1);
      stats.cagr3y = cagr(3);
      stats.cagr5y = cagr(5);
    }
  } catch {
    // partial stats from fast_info are still returned
  }

  stats.dataDate = getLastTradingDate(chartTimestamps);
  return JSON.stringify(stats);
}

export async function getAnalystConsensus(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) results.push(await getAnalystConsensus(t));
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, JSON.parse(results[i])])), limit);
  }
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=financialData,recommendationTrend,price`
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

  const output: Record<string, unknown> = {
    ticker,
    priceTargets: {
      current: targetMean,
      low: raw(fd.targetLowPrice),
      high: raw(fd.targetHighPrice),
      mean: targetMean,
      median: raw(fd.targetMedianPrice),
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
    output.numberOfAnalysts = Object.values(counts).reduce((a, b) => a + b, 0);
  } else {
    output.recommendationSummary = null;
  }

  return JSON.stringify(output);
}

export async function getEarningsAnalysis(ticker: string): Promise<string> {
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=earningsTrend,earningsHistory`
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
    earningsHistory: null,
    growthEstimates: null,
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
    output.growthEstimates = trendArr.map((p) => ({
      period: p.period,
      stockGrowth: raw((p.growth as Record<string, unknown> | undefined)?.estimate ?? null),
    }));
  }

  const histArr = (eh.history as Record<string, unknown>[]) ?? [];
  if (histArr.length > 0) {
    output.earningsHistory = histArr.map((h) => ({
      quarter: h.quarter,
      epsActual: raw(h.epsActual),
      epsEstimate: raw(h.epsEstimate),
      epsDifference: raw(h.epsDifference),
      surprisePercent: raw(h.surprisePercent),
    }));
  }

  return JSON.stringify(output);
}

export async function getFinancialRatios(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) results.push(await getFinancialRatios(t));
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, JSON.parse(results[i])])), limit);
  }
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=summaryDetail,financialData,defaultKeyStatistics`
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

  const rawRatios: Record<string, unknown> = {
    ticker,
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

  return JSON.stringify(filtered);
}

export async function getCalendar(ticker: string): Promise<string> {
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=calendarEvents`
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
    earningsDateConfirmed: earningsDates.filter(Boolean).length === new Set(earningsDates.filter(Boolean)).size
      && new Set(earningsDates.filter(Boolean)).size === 1,
    earningsDateSource: earningsDates.filter(Boolean).length === 0
      ? "UNKNOWN"
      : new Set(earningsDates.filter(Boolean)).size === 1
        ? "IR_FILING"
        : "ESTIMATE",
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

export async function searchTicker(query: string, maxResults: number, exchange?: string | null): Promise<string> {
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v1/finance/search?q=${enc(query)}&quotesCount=${maxResults}&newsCount=0&enableFuzzyQuery=false`,
    false
  )) as Record<string, unknown>;

  const quotes = (d?.quotes as Record<string, unknown>[]) ?? [];
  let trimmed = quotes
    .filter((q) => q.symbol)
    .map((q) => ({
      symbol: q.symbol ?? null,
      shortname: (q.shortname ?? q.longname ?? null) as unknown,
      exchange: q.exchange ?? null,
      quoteType: q.quoteType ?? null,
      score: q.score ?? null,
    }));

  if (exchange) {
    const exch = exchange.toUpperCase();
    if (exch === "US") {
      const usExchanges = new Set(["NMS", "NYQ"]);
      trimmed = trimmed.filter((r) => usExchanges.has(r.exchange as string));
    } else {
      trimmed = trimmed.filter((r) => r.exchange === exch);
    }
  }

  return JSON.stringify(trimmed);
}

const VALID_SCREENERS = [
  "aggressive_small_caps",
  "day_gainers",
  "day_losers",
  "growth_technology_stocks",
  "most_actives",
  "most_shorted_stocks",
  "small_cap_gainers",
  "undervalued_growth_stocks",
  "undervalued_large_caps",
  "conservative_foreign_funds",
  "high_yield_bond",
  "portfolio_anchors",
  "solid_large_growth_funds",
  "solid_midcap_growth_funds",
  "top_mutual_funds",
] as const;

type ScreenerName = (typeof VALID_SCREENERS)[number];

export async function screenStocks(screenerName: string, count: number): Promise<string> {
  if (!VALID_SCREENERS.includes(screenerName as ScreenerName)) {
    return `Error: unknown screener '${screenerName}'. Valid options: ${VALID_SCREENERS.join(", ")}`;
  }
  const safeCount = Math.min(count, 250);
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=${enc(screenerName)}&count=${safeCount}&lang=en-US&region=US&corsDomain=finance.yahoo.com`
  )) as Record<string, unknown>;

  const financeResult = (d?.finance as Record<string, unknown>)?.result as
    | Record<string, unknown>[]
    | undefined;
  const quotes = (financeResult?.[0]?.quotes as Record<string, unknown>[]) ?? [];

  if (!quotes.length) return `Error: no results for screener '${screenerName}'`;

  const trimmed = quotes.map((q) => ({
    symbol: q.symbol ?? null,
    shortName: q.shortName ?? null,
    regularMarketPrice: q.regularMarketPrice ?? null,
    regularMarketChangePercent: q.regularMarketChangePercent ?? null,
    marketCap: q.marketCap ?? null,
    regularMarketVolume: q.regularMarketVolume ?? null,
    exchange: q.exchange ?? null,
  }));
  return JSON.stringify({ screener: screenerName, count: trimmed.length, results: trimmed });
}

export async function getSecFilings(ticker: string): Promise<string> {
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=secFilings`
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  const sec = (result.secFilings as Record<string, unknown>) ?? {};
  const filings = (sec.filings as Record<string, unknown>[]) ?? [];

  if (!filings.length) {
    return JSON.stringify({ ticker, filings: [] });
  }

  const out = filings.map((f) => ({
    date: f.epochDate != null ? iso(f.epochDate as number) : null,
    type: f.type ?? null,
    title: f.title ?? null,
    edgarUrl: f.edgarUrl ?? null,
  }));
  return JSON.stringify({ ticker, filings: out });
}

export async function getRecommendations(
  ticker: string,
  type: string,
  monthsBack: number
): Promise<string> {
  const mod = REC_MOD[type];
  if (!mod) return `Error: invalid recommendation type '${type}'`;

  const d = (await yGet(
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=${mod}`
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

// ── get_short_interest ───────────────────────────────────────────────────────

const SHORT_FIELDS = [
  "sharesShort",
  "sharesShortPriorMonth",
  "shortRatio",
  "shortPercentOfFloat",
  "sharesPercentSharesOut",
  "floatShares",
  "sharesOutstanding",
  "dateShortInterest",
  "sharesShortPreviousMonthDate",
] as const;

export async function getShortInterest(ticker: string): Promise<string> {
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=defaultKeyStatistics,price`
  )) as Record<string, unknown>;

  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!result) return noData(ticker);

  const ks = (result.defaultKeyStatistics as Record<string, unknown>) ?? {};
  const price = (result.price as Record<string, unknown>) ?? {};

  const data: Record<string, unknown> = { ticker };
  for (const key of SHORT_FIELDS) {
    const val = raw(ks[key]) ?? raw(price[key]);
    if (val != null) data[key] = val;
  }

  return JSON.stringify(data);
}

// ── get_technical_indicators ─────────────────────────────────────────────────

export async function getTechnicalIndicators(
  ticker: string | string[],
  period: string
): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) {
      results.push(await getTechnicalIndicators(t, period));
    }
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, safeJsonParse(results[i], t)])), limit);
  }
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v8/finance/chart/${enc(ticker)}?range=${period}&interval=1d`,
    false
  )) as Record<string, unknown>;

  const chartResult = (d?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  if (!chartResult) return noData(ticker);

  const adjCloseArr =
    ((chartResult.indicators as Record<string, unknown[]>)?.adjclose?.[0] as Record<
      string,
      (number | null)[]
    >)?.adjclose ??
    ((chartResult.indicators as Record<string, unknown[]>)?.quote?.[0] as Record<
      string,
      (number | null)[]
    >)?.close ??
    [];

  const timestamps = (chartResult.timestamp as number[]) ?? [];
  const closes = adjCloseArr.filter((v): v is number => v != null);

  if (closes.length < 26) {
    return `Error: insufficient price history for ${ticker} (need ≥26 data points, got ${closes.length})`;
  }

  const output: Record<string, unknown> = { ticker };

  // RSI-14 (Wilder smoothing via EWM with alpha=1/14)
  try {
    const deltas = closes.slice(1).map((c, i) => c - closes[i]);
    const gains = deltas.map((d) => (d > 0 ? d : 0));
    const losses = deltas.map((d) => (d < 0 ? -d : 0));
    const alpha = 1 / 14;
    let avgGain = gains.slice(0, 14).reduce((a, b) => a + b, 0) / 14;
    let avgLoss = losses.slice(0, 14).reduce((a, b) => a + b, 0) / 14;
    for (let i = 14; i < gains.length; i++) {
      avgGain = alpha * gains[i] + (1 - alpha) * avgGain;
      avgLoss = alpha * losses[i] + (1 - alpha) * avgLoss;
    }
    const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
    output.rsi14 = +(100 - 100 / (1 + rs)).toFixed(2);
  } catch {
    output.rsi14 = null;
  }

  // MACD (12, 26, 9)
  try {
    const ema = (data: number[], span: number): number[] => {
      const k = 2 / (span + 1);
      const result = [data[0]];
      for (let i = 1; i < data.length; i++) {
        result.push(data[i] * k + result[i - 1] * (1 - k));
      }
      return result;
    };

    const ema12 = ema(closes, 12);
    const ema26 = ema(closes, 26);
    const macdLine = ema12.map((v, i) => v - ema26[i]);
    const signalLine = ema(macdLine, 9);
    const last = macdLine.length - 1;

    output.macd = +macdLine[last].toFixed(4);
    output.macdSignal = +signalLine[last].toFixed(4);
    output.macdHistogram = +(macdLine[last] - signalLine[last]).toFixed(4);
  } catch {
    output.macd = null;
    output.macdSignal = null;
    output.macdHistogram = null;
  }

  output.lastClose = +closes[closes.length - 1].toFixed(2);
  const lastTs = timestamps[timestamps.length - 1];
  output.dataDate = lastTs ? new Date(lastTs * 1000).toISOString().slice(0, 10) : null;

  return JSON.stringify(output);
}

// ── get_price_slope ──────────────────────────────────────────────────────────

export async function getPriceSlope(ticker: string | string[], days: number): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) {
      results.push(await getPriceSlope(t, days));
    }
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, JSON.parse(results[i])])), limit);
  }

  const range = `${days + 10}d`;
  try {
    const d = (await yGet(
      `https://query1.finance.yahoo.com/v8/finance/chart/${enc(ticker)}?range=${range}&interval=1d`,
      false
    )) as Record<string, unknown>;

    const result = (d?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
      | Record<string, unknown>
      | undefined;
    if (!result) return JSON.stringify({ error: true, message: `No data for ${ticker}`, ticker });

    const timestamps = (result.timestamp as number[]) ?? [];
    const adjclose =
      ((result.indicators as Record<string, unknown[]>)?.adjclose?.[0] as Record<string, (number | null)[]>)?.adjclose ??
      ((result.indicators as Record<string, unknown[]>)?.quote?.[0] as Record<string, (number | null)[]>)?.close ??
      [];

    const closes = adjclose.filter((v): v is number => v != null);
    if (closes.length < 2) return JSON.stringify({ error: true, message: `Insufficient data for ${ticker}`, ticker });

    const tail = closes.slice(-days);
    const startClose = tail[0];
    const endClose = tail[tail.length - 1];
    const slopePct = startClose !== 0 ? +((endClose - startClose) / startClose * 100).toFixed(2) : null;

    let direction: string;
    if (slopePct == null || Math.abs(slopePct) < 0.5) direction = "FLAT";
    else if (slopePct > 0) direction = "UP";
    else direction = "DOWN";

    const lastTsVal = timestamps[timestamps.length - 1];
    return JSON.stringify({
      ticker,
      days,
      startClose: +startClose.toFixed(2),
      endClose: +endClose.toFixed(2),
      slopePct,
      direction,
      dataDate: lastTsVal ? new Date(lastTsVal * 1000).toISOString().slice(0, 10) : null,
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_volume_ratio ─────────────────────────────────────────────────────────

export async function getVolumeRatio(ticker: string | string[], _period: number): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) {
      results.push(await getVolumeRatio(t, _period));
    }
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, JSON.parse(results[i])])), limit);
  }

  try {
    const fi = JSON.parse(await getFastInfo(ticker));
    const lastVolume = fi.lastVolume as number | null;
    const avg10d = fi.tenDayAverageVolume as number | null;
    const avg90d = fi.threeMonthAverageVolume as number | null;

    const ratio10d = lastVolume != null && avg10d != null && avg10d !== 0 ? +(lastVolume / avg10d).toFixed(3) : null;
    const ratio90d = lastVolume != null && avg90d != null && avg90d !== 0 ? +(lastVolume / avg90d).toFixed(3) : null;

    let volumeFlag: string | null = null;
    if (ratio10d != null) {
      if (ratio10d > 1.5) volumeFlag = "HIGH";
      else if (ratio10d < 0.7) volumeFlag = "LOW";
      else volumeFlag = "NORMAL";
    }

    return JSON.stringify({
      ticker,
      lastVolume,
      avgVolume10d: avg10d,
      avgVolume90d: avg90d,
      ratio10d,
      ratio90d,
      volumeFlag,
      dataDate: (fi.lastTradeDate as string | null) ?? new Date().toISOString().slice(0, 10),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_ma_position ──────────────────────────────────────────────────────────

export async function getMaPosition(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) {
      results.push(await getMaPosition(t));
    }
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, JSON.parse(results[i])])), limit);
  }

  try {
    const fi = JSON.parse(await getFastInfo(ticker));
    const lastPrice = fi.lastPrice as number | null;
    const fiftyDma = fi.fiftyDayAverage as number | null;
    const twoHundredDma = fi.twoHundredDayAverage as number | null;

    const pctVs50 = lastPrice != null && fiftyDma != null && fiftyDma !== 0
      ? +((lastPrice - fiftyDma) / fiftyDma * 100).toFixed(2) : null;
    const pctVs200 = lastPrice != null && twoHundredDma != null && twoHundredDma !== 0
      ? +((lastPrice - twoHundredDma) / twoHundredDma * 100).toFixed(2) : null;

    const regime50 = pctVs50 != null ? (pctVs50 >= 0 ? "ABOVE" : "BELOW") : null;
    const regime200 = pctVs200 != null ? (pctVs200 >= 0 ? "ABOVE" : "BELOW") : null;

    let trend: string | null = null;
    if (regime50 != null && regime200 != null) {
      if (regime50 === "ABOVE" && regime200 === "ABOVE") trend = "BULLISH";
      else if (regime50 === "BELOW" && regime200 === "BELOW") trend = "BEARISH";
      else trend = "MIXED";
    }

    return JSON.stringify({
      ticker,
      lastPrice,
      fiftyDayAverage: fiftyDma,
      twoHundredDayAverage: twoHundredDma,
      pctVs50dma: pctVs50,
      pctVs200dma: pctVs200,
      regime50,
      regime200,
      trend,
      dataDate: (fi.lastTradeDate as string | null) ?? new Date().toISOString().slice(0, 10),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_credit_health ────────────────────────────────────────────────────────

export async function getCreditHealth(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) {
      results.push(await getCreditHealth(t));
    }
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, safeJsonParse(results[i], t)])), limit);
  }
  try {
    const [bsRaw, incRaw] = await Promise.all([
      fetchTimeseries(ticker, "quarterly", ["TotalDebt", "CashAndCashEquivalents"]),
      fetchTimeseries(ticker, "quarterly", ["EBITDA", "EBIT", "InterestExpense"]),
    ]);

    const bs = JSON.parse(bsRaw) as Record<string, unknown>[];
    const inc = JSON.parse(incRaw) as Record<string, unknown>[];

    if (!Array.isArray(bs) || !bs.length || !Array.isArray(inc) || !inc.length) {
      return JSON.stringify({ error: true, message: "Insufficient financial data", ticker });
    }

    const bsLatest = bs[0];
    const incLatest = inc[0];

    const totalDebt = (bsLatest.totalDebt as number | null) ?? null;
    const cash = (bsLatest.cashAndCashEquivalents as number | null) ?? null;
    const ebitdaQ = (incLatest.eBITDA as number | null) ?? (incLatest.ebitda as number | null) ?? null;
    const ebitQ = (incLatest.eBIT as number | null) ?? (incLatest.ebit as number | null) ?? null;
    const interestQ = (incLatest.interestExpense as number | null) ?? null;

    const netDebt = totalDebt != null && cash != null ? totalDebt - cash : null;
    const ebitdaAnnual = ebitdaQ != null ? ebitdaQ * 4 : null;
    const ebitAnnual = ebitQ != null ? ebitQ * 4 : null;
    const interestAnnual = interestQ != null ? interestQ * 4 : null;

    const netDebtToEbitda = netDebt != null && ebitdaAnnual != null && ebitdaAnnual !== 0
      ? +(netDebt / ebitdaAnnual).toFixed(2) : null;
    const interestCoverage = ebitAnnual != null && interestAnnual != null && interestAnnual !== 0
      ? +(ebitAnnual / Math.abs(interestAnnual)).toFixed(2) : null;

    let creditStressFlag: boolean | null = null;
    if (netDebtToEbitda != null && interestCoverage != null) {
      creditStressFlag = netDebtToEbitda > 2.5 && interestCoverage < 3;
    }

    let debtTier: string | null = null;
    if (netDebtToEbitda != null) {
      if (netDebtToEbitda < 1) debtTier = "CLEAN";
      else if (netDebtToEbitda <= 2.5) debtTier = "MODERATE";
      else if (netDebtToEbitda <= 4) debtTier = "ELEVATED";
      else debtTier = "STRESSED";
    }

    const dataQuality = [totalDebt, cash, ebitdaQ, ebitQ, interestQ].some((v) => v == null) ? "PARTIAL" : "OK";
    const quarterDate = (bsLatest.date as string) ?? (incLatest.date as string) ?? null;

    return JSON.stringify({
      ticker,
      quarterDate,
      totalDebtUsd: totalDebt,
      cashUsd: cash,
      netDebtUsd: netDebt,
      ebitdaUsd: ebitdaAnnual,
      ebitUsd: ebitAnnual,
      interestExpenseUsd: interestAnnual,
      netDebtToEbitda,
      interestCoverage,
      creditStressFlag,
      debtTier,
      dataQuality,
      dataDate: getLastTradingDate(),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_short_momentum ───────────────────────────────────────────────────────

export async function getShortMomentum(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) {
      results.push(await getShortMomentum(t));
    }
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, safeJsonParse(results[i], t)])), limit);
  }
  try {
    const si = JSON.parse(await getShortInterest(ticker));
    if (si.error) return JSON.stringify(si);

    const sharesShort = si.sharesShort as number | null;
    const sharesShortPrior = si.sharesShortPriorMonth as number | null;
    const shortPctFloatRaw = si.shortPercentOfFloat as number | null;
    const shortRatio = si.shortRatio as number | null;
    const dateShort = si.dateShortInterest;

    const shortPctFloat = shortPctFloatRaw != null ? +(shortPctFloatRaw * 100).toFixed(2) : null;

    let momDeltaPct: number | null = null;
    if (sharesShort != null && sharesShortPrior != null && sharesShortPrior !== 0) {
      momDeltaPct = +((sharesShort - sharesShortPrior) / sharesShortPrior * 100).toFixed(2);
    }

    let momDirection: string | null = null;
    if (momDeltaPct != null) {
      if (Math.abs(momDeltaPct) < 2) momDirection = "FLAT";
      else if (momDeltaPct > 0) momDirection = "RISING";
      else momDirection = "FALLING";
    }

    let squeezeRisk: string | null = null;
    if (shortPctFloat != null) {
      if (shortPctFloat > 30 && shortRatio != null && shortRatio < 3) squeezeRisk = "HIGH";
      else if (shortPctFloat > 20) squeezeRisk = "MODERATE";
      else squeezeRisk = "LOW";
    }

    let flag: string | null = null;
    if (shortPctFloat != null && shortPctFloat > 30) flag = "🔴 CRITICAL SHORT";
    else if (shortPctFloat != null && shortPctFloat > 20) flag = "⚠️ HIGH SHORT";

    return JSON.stringify({
      ticker,
      shortPctFloat,
      daysToCover: shortRatio,
      sharesShort,
      sharesShortPriorMonth: sharesShortPrior,
      momDeltaPct,
      momDirection,
      squeezeRisk,
      flag,
      dateShortInterest: dateShort,
      dataDate: getLastTradingDate(),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_earnings_momentum ────────────────────────────────────────────────────

export async function getEarningsMomentum(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) {
      results.push(await getEarningsMomentum(t));
    }
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, safeJsonParse(results[i], t)])), limit);
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
    let beatStreak = 0;

    if (earningsHistory && earningsHistory.length > 0) {
      for (const h of earningsHistory) {
        const actual = h.epsActual as number | null;
        const estimate = h.epsEstimate as number | null;
        const surprise = h.surprisePercent as number | null;
        if (actual != null && estimate != null) {
          totalQuarters++;
          if (actual > estimate) beatCount++;
          if (surprise != null) surprises.push(Math.abs(surprise) < 1 ? surprise * 100 : surprise);
        }
      }
      for (const h of earningsHistory) {
        const actual = h.epsActual as number | null;
        const estimate = h.epsEstimate as number | null;
        if (actual != null && estimate != null) {
          if (actual > estimate) beatStreak++;
          else break;
        }
      }
    }

    const beatRate = totalQuarters > 0 ? +(beatCount / totalQuarters).toFixed(2) : null;
    const avgSurprise = surprises.length > 0
      ? +(surprises.reduce((a, b) => a + b, 0) / surprises.length).toFixed(2) : null;

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
      totalQuarters,
      avgSurprisePct: avgSurprise,
      currentBeatStreak: beatStreak,
      dataQuality,
      dataDate: getLastTradingDate(),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_options_flow_summary ─────────────────────────────────────────────────

export async function getOptionsFlowSummary(ticker: string, expiryHint?: string): Promise<string> {
  try {
    // First fetch: get all expiry dates (and the default chain) in one subrequest
    const firstFetch = await yGetFullOptions(ticker);
    const dates = firstFetch.dates;
    if (!dates || !dates.length) {
      return JSON.stringify({ error: true, message: "No option expirations", ticker });
    }

    // Get last price (1 subrequest)
    const fi = JSON.parse(await getFastInfo(ticker));
    const lastPrice = fi.lastPrice as number | null;

    // Select expiry
    const selectedExpiry = expiryHint && dates.includes(expiryHint) ? expiryHint : dates[0];

    // If the selected expiry is the default (first), reuse firstFetch data;
    // otherwise make one more subrequest for the specific date's chain
    let calls: Record<string, unknown>[];
    let puts: Record<string, unknown>[];
    if (selectedExpiry === dates[0]) {
      calls = firstFetch.calls;
      puts = firstFetch.puts;
    } else {
      const specific = await yGetFullOptions(ticker, selectedExpiry);
      calls = specific.calls;
      puts = specific.puts;
    }

    if (!Array.isArray(calls) || !Array.isArray(puts)) {
      return JSON.stringify({ error: true, message: "Failed to parse option chain", ticker });
    }

    const totalCallOI = calls.reduce((s, c) => s + ((c.openInterest as number) ?? 0), 0);
    const totalPutOI = puts.reduce((s, p) => s + ((p.openInterest as number) ?? 0), 0);
    const pcRatio = totalCallOI > 0 ? +(totalPutOI / totalCallOI).toFixed(3) : null;

    let pcSentiment: string | null = null;
    if (pcRatio != null) {
      if (pcRatio > 1.5) pcSentiment = "PUT_HEAVY";
      else if (pcRatio < 0.7) pcSentiment = "CALL_HEAVY";
      else pcSentiment = "NEUTRAL";
    }

    // ATM strike
    let atmStrike: number | null = null;
    let atmIV: number | null = null;
    if (lastPrice != null && calls.length > 0) {
      let minDist = Infinity;
      for (const c of calls) {
        const strike = c.strike as number;
        const iv = c.impliedVolatility as number | null;
        if (strike != null && iv != null) {
          const dist = Math.abs(strike - lastPrice);
          if (dist < minDist) {
            minDist = dist;
            atmStrike = strike;
            atmIV = +(iv).toFixed(3);
          }
        }
      }
    }

    // IV percentile
    const allIVs: number[] = [];
    for (const c of calls) { const iv = c.impliedVolatility as number | null; if (iv != null) allIVs.push(iv); }
    for (const p of puts) { const iv = p.impliedVolatility as number | null; if (iv != null) allIVs.push(iv); }

    let ivPctile: number | null = null;
    if (atmIV != null && allIVs.length > 0) {
      const below = allIVs.filter((iv) => iv <= atmIV!).length;
      ivPctile = Math.round((below / allIVs.length) * 100);
    }
    const ivFlag = ivPctile != null && ivPctile > 70 ? "⚠️ HIGH IV" : null;

    // Max pain
    let maxPainStrike: number | null = null;
    const allStrikes = [...new Set([
      ...calls.map((c) => c.strike as number).filter(Boolean),
      ...puts.map((p) => p.strike as number).filter(Boolean),
    ])].sort((a, b) => a - b);

    if (allStrikes.length > 0) {
      let minPain = Infinity;
      for (const strike of allStrikes) {
        let pain = 0;
        for (const c of calls) {
          pain += Math.max(0, strike - (c.strike as number)) * ((c.openInterest as number) ?? 0);
        }
        for (const p of puts) {
          pain += Math.max(0, (p.strike as number) - strike) * ((p.openInterest as number) ?? 0);
        }
        if (pain < minPain) { minPain = pain; maxPainStrike = strike; }
      }
    }

    // Highest OI strikes
    let highestOICallStrike: number | null = null;
    let highestOIPutStrike: number | null = null;
    let maxCallOI = 0;
    for (const c of calls) {
      const oi = (c.openInterest as number) ?? 0;
      if (oi > maxCallOI) { maxCallOI = oi; highestOICallStrike = c.strike as number; }
    }
    let maxPutOI = 0;
    for (const p of puts) {
      const oi = (p.openInterest as number) ?? 0;
      if (oi > maxPutOI) { maxPutOI = oi; highestOIPutStrike = p.strike as number; }
    }

    return JSON.stringify({
      ticker,
      expiryDate: selectedExpiry,
      totalCallOI,
      totalPutOI,
      pcRatio,
      pcSentiment,
      atmStrike,
      atmIV,
      ivPctile,
      ivFlag,
      maxPainStrike,
      highestOICallStrike,
      highestOIPutStrike,
      dataDate: new Date().toISOString().slice(0, 10),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_put_hedge_candidates ─────────────────────────────────────────────────

export async function getPutHedgeCandidates(
  ticker: string,
  otmPctMin: number,
  otmPctMax: number,
  budgetUsd: number,
  expiryAfter: string
): Promise<string> {
  try {
    // Get last price (1 subrequest)
    const fi = JSON.parse(await getFastInfo(ticker));
    const currentPrice = fi.lastPrice as number | null;
    if (currentPrice == null) {
      return JSON.stringify({ error: true, message: `No price for ${ticker}`, ticker });
    }

    // Get expiration dates + default chain in one subrequest
    const firstFetch = await yGetFullOptions(ticker);
    const dates = firstFetch.dates;
    if (!dates || !dates.length) {
      return JSON.stringify({ error: true, message: "No option expirations", ticker });
    }

    // Filter and select nearest 2
    const qualifying = expiryAfter ? dates.filter((d) => d >= expiryAfter).slice(0, 2) : dates.slice(0, 2);
    if (!qualifying.length) {
      return JSON.stringify({ error: true, message: "No qualifying expiry dates", ticker });
    }

    const strikeMin = currentPrice * (1 - otmPctMax / 100);
    const strikeMax = currentPrice * (1 - otmPctMin / 100);

    interface Candidate {
      expiry: string;
      strike: number;
      bid: number;
      ask: number;
      mid: number;
      contractCost: number;
      withinBudget: boolean;
      openInterest: number;
      ivPctile: number | null;
      ivFlag: string | null;
      otmPct: number;
    }
    const candidates: Candidate[] = [];

    for (const exp of qualifying) {
      try {
        // Reuse firstFetch data if this is the default expiry, otherwise fetch
        let putsRaw: Record<string, unknown>[];
        if (exp === dates[0]) {
          putsRaw = firstFetch.puts;
        } else {
          const specific = await yGetFullOptions(ticker, exp);
          putsRaw = specific.puts;
        }
        if (!Array.isArray(putsRaw)) continue;

        const allIVs = putsRaw
          .map((p: Record<string, unknown>) => p.impliedVolatility as number | null)
          .filter((v): v is number => v != null);

        for (const p of putsRaw as Record<string, unknown>[]) {
          const strike = p.strike as number;
          if (strike == null || strike < strikeMin || strike > strikeMax) continue;

          const bid = (p.bid as number) ?? 0;
          const ask = (p.ask as number) ?? 0;
          const mid = +((bid + ask) / 2).toFixed(2);
          const contractCost = +(mid * 100).toFixed(2);
          const oi = (p.openInterest as number) ?? 0;
          const iv = (p.impliedVolatility as number) ?? 0;

          let ivPctile: number | null = null;
          if (allIVs.length > 0 && iv > 0) {
            ivPctile = Math.round((allIVs.filter((v) => v <= iv).length / allIVs.length) * 100);
          }

          candidates.push({
            expiry: exp,
            strike,
            bid,
            ask,
            mid,
            contractCost,
            withinBudget: contractCost <= budgetUsd,
            openInterest: oi,
            ivPctile,
            ivFlag: ivPctile != null && ivPctile > 70 ? "⚠️ HIGH IV" : null,
            otmPct: +((currentPrice - strike) / currentPrice * 100).toFixed(2),
          });
        }
      } catch {
        continue;
      }
    }

    candidates.sort((a, b) => a.expiry.localeCompare(b.expiry) || a.strike - b.strike);
    const budgetFeasible = candidates.some((c) => c.withinBudget);

    let note: string;
    let budgetGapUsd: number | null = null;
    if (!candidates.length) {
      note = "No put options found in the specified OTM range.";
    } else if (!budgetFeasible) {
      const nearest = candidates.reduce((a, b) => (a.contractCost < b.contractCost ? a : b));
      budgetGapUsd = +(nearest.contractCost - budgetUsd).toFixed(2);
      note = `No candidates within budget. Nearest: $${nearest.strike} put at $${nearest.contractCost}/contract vs $${budgetUsd} budget.`;
    } else {
      const count = candidates.filter((c) => c.withinBudget).length;
      note = `${count} candidate(s) within $${budgetUsd} budget.`;
    }

    return JSON.stringify({
      ticker,
      currentPrice: +currentPrice.toFixed(2),
      strikeRangeMin: +strikeMin.toFixed(2),
      strikeRangeMax: +strikeMax.toFixed(2),
      budgetUsd,
      candidates,
      budgetFeasible,
      budgetGapUsd,
      note,
      dataDate: getLastTradingDate(),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_analyst_upgrade_radar ────────────────────────────────────────────────

export async function getAnalystUpgradeRadar(ticker: string | string[], daysBack: number): Promise<string> {
  if (Array.isArray(ticker)) {
    const limit = limitTickers(ticker);
    const results: string[] = [];
    for (const t of limit.tickers) {
      results.push(await getAnalystUpgradeRadar(t, daysBack));
    }
    return wrapBatchResult(Object.fromEntries(limit.tickers.map((t, i) => [t, JSON.parse(results[i])])), limit);
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
        changes: [],
        summary: "NO CHANGES",
        dataDate: getLastTradingDate(),
      });
    }

    const cutoffMs = Date.now() - daysBack * 86400 * 1000;
    const upgradeGrades = new Set(["Buy", "Outperform", "Overweight", "Strong Buy", "Positive", "Market Outperform", "Top Pick"]);
    const downgradeGrades = new Set(["Sell", "Underperform", "Underweight", "Strong Sell", "Negative", "Market Underperform", "Reduce"]);

    const changes: Record<string, unknown>[] = [];
    let upgradeCount = 0;
    let downgradeCount = 0;

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

      let signal: string;
      if (["up", "upgrade", "Up", "Upgrade"].includes(action) || upgradeGrades.has(toGrade)) {
        signal = "UPGRADE";
        upgradeCount++;
      } else if (["down", "downgrade", "Down", "Downgrade"].includes(action) || downgradeGrades.has(toGrade)) {
        signal = "DOWNGRADE";
        downgradeCount++;
      } else {
        signal = "MAINTAIN";
      }

      // Price target direction.
      // yfinance/upgrades_downgrades doesn't expose numeric price targets, so ptFrom/ptTo are
      // structural stubs (null). ptDirection is derived from action semantics:
      //   INITIATED — new coverage (initiated/init action)
      //   UNCHANGED — reiteration/maintain with no rating change
      //   null     — signal genuinely unknown
      const ptFrom: null = null;
      const ptTo: null = null;
      const ptDirection: string | null =
        ["initiated", "Initiated", "init"].includes(action) ? "INITIATED" :
        signal === "MAINTAIN" ? "UNCHANGED" : null;
      const mixedSignal = signal === "UPGRADE" && ptDirection === "LOWERED";

      let strengthFlag: string;
      if (signal === "UPGRADE" && !mixedSignal) strengthFlag = "BULLISH";
      else if (signal === "DOWNGRADE") strengthFlag = "BEARISH";
      else if (mixedSignal) strengthFlag = "MIXED";
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
    const summary = parts.length > 0 ? parts.join(", ") : "NO CHANGES";

    return JSON.stringify({
      ticker,
      windowDays: daysBack,
      netSentiment,
      changes,
      summary,
      dataDate: getLastTradingDate(),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// Module-level in-memory cache for options flow window-label readings.
// Persists within a single Worker instance lifetime.
const _optionsFlowCache = new Map<string, { data: Record<string, unknown>; storedAt: number }>();

// ── get_geographic_revenue ────────────────────────────────────────────────────

export async function getGeographicRevenue(ticker: string, region: string = "China"): Promise<string> {
  let cik: number | null = null;
  let filingDate: string | null = null;
  let fiscalYear: string | null = null;

  try {
    const tickersResp = await fetch("https://www.sec.gov/files/company_tickers.json", {
      headers: { "User-Agent": "yahoo-finance-mcp/1.0 (contact@example.com)" },
    });
    if (tickersResp.ok) {
      const tickersData = await tickersResp.json() as Record<string, { ticker: string; cik_str: number }>;
      for (const entry of Object.values(tickersData)) {
        if (entry.ticker.toUpperCase() === ticker.toUpperCase()) {
          cik = entry.cik_str;
          break;
        }
      }
    } else {
      await tickersResp.body?.cancel();
    }
  } catch { /* EDGAR fetch failed */ }

  if (cik != null) {
    try {
      const cikPadded = String(cik).padStart(10, "0");
      const subsResp = await fetch(`https://data.sec.gov/submissions/CIK${cikPadded}.json`, {
        headers: { "User-Agent": "yahoo-finance-mcp/1.0 (contact@example.com)" },
      });
      if (subsResp.ok) {
        const subs = await subsResp.json() as Record<string, unknown>;
        const filings = ((subs.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
        const forms = (filings.form as string[]) ?? [];
        const dates = (filings.filingDate as string[]) ?? [];
        const periods = (filings.reportDate as string[]) ?? [];
        for (let i = 0; i < forms.length; i++) {
          if (["10-K", "10-K405", "10-KSB"].includes(forms[i])) {
            filingDate = dates[i] ?? null;
            const period = periods[i];
            if (period) fiscalYear = `FY${period.slice(0, 4)}`;
            break;
          }
        }
      } else {
        await subsResp.body?.cancel();
      }
    } catch { /* submissions fetch failed */ }
  }

  // ── Region → XBRL member mapping ──────────────────────────────────────────
  const REGION_XBRL_MEMBERS: Record<string, string[]> = {
    "china":         ["country:CN", "srt:ChinaMember"],
    "united states": ["country:US", "srt:UnitedStatesMember"],
    "europe":        ["srt:EuropeMember", "srt:EuropeMiddleEastAndAfricaMember"],
    "japan":         ["country:JP", "srt:JapanMember"],
    "asia pacific":  ["srt:AsiaPacificMember", "srt:AsiaMember"],
    "rest of world": ["srt:NonUsMember", "srt:OtherGeographicAreasMember"],
  };
  const GEO_AXIS = "srt:StatementGeographicalAxis";
  const GEO_CONCEPTS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomer",
  ];

  const regionLower = region.toLowerCase();
  const candidateMembers: string[] = REGION_XBRL_MEMBERS[regionLower] ?? [];

  function memberMatches(memberVal: string): boolean {
    if (candidateMembers.some(m => m.toLowerCase() === memberVal.toLowerCase())) return true;
    return memberVal.toLowerCase().includes(regionLower);
  }

  // ── Step 2: EDGAR XBRL company-facts extraction ───────────────────────────
  let regionRevenuePct: number | null = null;
  let regionRevenueUSD: number | null = null;
  let segmentLabel: string = region;
  let source = "not_available";
  let confidence = "NOT_DISCLOSED";

  if (cik != null) {
    try {
      const cikPadded = String(cik).padStart(10, "0");
      const factsResp = await fetch(
        `https://data.sec.gov/api/xbrl/companyfacts/CIK${cikPadded}.json`,
        { headers: { "User-Agent": "yahoo-finance-mcp/1.0 (contact@example.com)" } },
      );
      if (factsResp.ok) {
        type XbrlSegment = { dimension: string; member: string } | Array<{ dimension: string; member: string }>;
        type XbrlFact = { val: number; end: string; filed: string; form: string; segment?: XbrlSegment };
        type ConceptData = { units?: { USD?: XbrlFact[] } };
        const factsJson = await factsResp.json() as { facts?: { "us-gaap"?: Record<string, ConceptData> } };
        const usGaap = factsJson.facts?.["us-gaap"] ?? {};

        const annualForms = new Set(["10-K", "10-K405", "10-KSB"]);

        function isTargetFiling(fact: XbrlFact): boolean {
          if (!annualForms.has(fact.form)) return false;
          if (!filingDate) return true;
          try {
            const fd = new Date(filingDate).getTime();
            const ff = new Date(fact.filed).getTime();
            return Math.abs(ff - fd) <= 10 * 86400 * 1000;
          } catch { return true; }
        }

        outer: for (const concept of GEO_CONCEPTS) {
          const conceptData = usGaap[concept];
          if (!conceptData) continue;
          let facts = (conceptData.units?.USD ?? []).filter(isTargetFiling);
          if (!facts.length) {
            // Relax: accept any annual fact for this concept
            facts = (conceptData.units?.USD ?? []).filter(f => annualForms.has(f.form));
          }
          if (!facts.length) continue;

          // Group by period end-date
          const byPeriod = new Map<string, XbrlFact[]>();
          for (const fact of facts) {
            const arr = byPeriod.get(fact.end) ?? [];
            arr.push(fact);
            byPeriod.set(fact.end, arr);
          }

          // Try periods most-recent first
          for (const periodEnd of [...byPeriod.keys()].sort().reverse()) {
            const periodFacts = byPeriod.get(periodEnd)!;
            let regionalFact: XbrlFact | null = null;
            let totalFact: XbrlFact | null = null;

            for (const fact of periodFacts) {
              if (!fact.segment) {
                totalFact = fact;
              } else if (Array.isArray(fact.segment)) {
                for (const dimEntry of fact.segment) {
                  if (dimEntry.dimension === GEO_AXIS && memberMatches(dimEntry.member)) {
                    regionalFact = fact;
                    break;
                  }
                }
              } else if (
                fact.segment.dimension === GEO_AXIS &&
                memberMatches((fact.segment as { dimension: string; member: string }).member)
              ) {
                regionalFact = fact;
              }
            }

            if (regionalFact != null && totalFact != null && totalFact.val > 0) {
              regionRevenuePct = Math.round((regionalFact.val / totalFact.val) * 10000) / 10000;
              regionRevenueUSD = regionalFact.val;
              const seg = regionalFact.segment;
              segmentLabel = (seg && !Array.isArray(seg) && seg.member)
                ? seg.member
                : region;
              source = "edgar_xbrl";
              confidence = "CONFIRMED";
              break outer;
            }
          }
        }
      } else {
        await factsResp.body?.cancel();
      }
    } catch { /* company-facts fetch/parse failed — fall through to manual lookup */ }
  }

  // ── Manual-lookup pointer when XBRL extraction did not succeed ────────────
  type ManualLookup = {
    reason: string;
    action: string;
    edgarFullTextSearchUrl: string;
    edgarFilingsPageUrl: string;
    cik: string | null;
    filingDate: string | null;
    fiscalYear: string | null;
  } | null;

  let manualLookup: ManualLookup = null;
  if (confidence === "NOT_DISCLOSED") {
    const tUpper = ticker.toUpperCase();
    const cikPadded = cik != null ? String(cik).padStart(10, "0") : null;
    const edgarSearchUrl =
      `https://efts.sec.gov/LATEST/search-index?q=%22${encodeURIComponent(region)}%22&forms=10-K&entity=${tUpper}`;
    const edgarFilingsUrl = cikPadded
      ? `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cikPadded}&type=10-K&dateb=&owner=include&count=5`
      : `https://www.sec.gov/cgi-bin/browse-edgar?company=${tUpper}&CIK=&type=10-K&dateb=&owner=include&count=5&search_text=&action=getcompany`;
    manualLookup = {
      reason:
        `Geographic revenue breakdown for '${region}' is not available in machine-readable form via this data pipeline. ` +
        "NOT_DISCLOSED does NOT satisfy DC-151 Rule 1 annual confirmation requirement.",
      action:
        `Open the most recent 10-K for ${tUpper} and search for the section titled ` +
        "'Geographic Information', 'Geographic Areas', or 'Segment Information'. " +
        `Look for a table that lists revenue by region including '${region}'. ` +
        "Divide that figure by Total Revenue to compute regionRevenuePct.",
      edgarFullTextSearchUrl: edgarSearchUrl,
      edgarFilingsPageUrl: edgarFilingsUrl,
      cik: cikPadded,
      filingDate,
      fiscalYear,
    };
  }

  return JSON.stringify({
    ticker,
    region,
    regionRevenuePct,
    regionRevenueUSD,
    fiscalYear,
    filingType: "10-K",
    filingDate,
    segmentLabel,
    source,
    confidence,
    _manualLookup: manualLookup,
  });
}

// ── get_options_flow_scan ─────────────────────────────────────────────────────

export async function getOptionsFlowScan(ticker: string, windowLabel: string): Promise<string> {
  try {
    const { calls, puts } = await yGetFullOptions(ticker);
    if (!calls.length && !puts.length) {
      return JSON.stringify({ error: true, message: `No options data for ${ticker}`, ticker });
    }

    let currentPrice: number | null = null;
    try {
      const pd = await yGet(`https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=price`) as Record<string, unknown>;
      const pr = ((pd?.quoteSummary as Record<string, unknown[]>)?.result?.[0] as Record<string, unknown>)?.price as Record<string, unknown>;
      currentPrice = raw(pr?.regularMarketPrice) as number | null;
    } catch { /* ignore */ }

    const callVol = calls.reduce((s, c) => s + ((c.volume as number) || 0), 0);
    const putVol = puts.reduce((s, p) => s + ((p.volume as number) || 0), 0);
    const pcRatio = callVol > 0 ? +(putVol / callVol).toFixed(2) : null;

    let maxPainStrike: number | null = null;
    const oiByStrike = new Map<number, number>();
    for (const c of [...calls, ...puts]) {
      const strike = c.strike as number;
      const oi = (c.openInterest as number) || 0;
      oiByStrike.set(strike, (oiByStrike.get(strike) ?? 0) + oi);
    }
    if (oiByStrike.size > 0) {
      let maxOi = -1;
      for (const [strike, oi] of oiByStrike) {
        if (oi > maxOi) { maxOi = oi; maxPainStrike = strike; }
      }
    }

    let atmIv: number | null = null;
    if (currentPrice != null) {
      let minDist = Infinity;
      for (const c of calls) {
        const dist = Math.abs((c.strike as number) - currentPrice);
        if (dist < minDist) { minDist = dist; atmIv = (c.impliedVolatility as number | null) ?? null; }
      }
    }

    let ivPctile: number | null = null;
    let chartTimestamps: number[] = [];
    try {
      const chartD = await yGet(
        `https://query1.finance.yahoo.com/v8/finance/chart/${enc(ticker)}?range=1y&interval=1d`,
        false
      ) as Record<string, unknown>;
      const chartRes = (chartD?.chart as Record<string, unknown[]>)?.result?.[0] as Record<string, unknown>;
      chartTimestamps = (chartRes?.timestamp as number[]) ?? [];
      const adjclose =
        ((chartRes?.indicators as Record<string, unknown[]>)?.adjclose?.[0] as Record<string, (number | null)[]>)?.adjclose ??
        ((chartRes?.indicators as Record<string, unknown[]>)?.quote?.[0] as Record<string, (number | null)[]>)?.close ?? [];
      const closes = adjclose.filter((v): v is number => v != null);
      if (closes.length >= 30 && atmIv != null) {
        const rets: number[] = [];
        for (let i = 1; i < closes.length; i++) rets.push((closes[i] - closes[i - 1]) / closes[i - 1]);
        const rollVols: number[] = [];
        for (let i = 29; i < rets.length; i++) {
          const window = rets.slice(i - 29, i + 1);
          const mean = window.reduce((a, b) => a + b, 0) / window.length;
          const variance = window.reduce((a, b) => a + (b - mean) ** 2, 0) / window.length;
          rollVols.push(Math.sqrt(variance * 252));
        }
        if (rollVols.length >= 5) {
          const rvMin = Math.min(...rollVols);
          const rvMax = Math.max(...rollVols);
          if (rvMax > rvMin) {
            ivPctile = Math.max(0, Math.min(100, Math.round((atmIv - rvMin) / (rvMax - rvMin) * 100)));
          }
        }
      }
    } catch { /* ignore */ }

    const dataDate = getLastTradingDate(chartTimestamps);

    const prevWindowMap: Record<string, string> = { "T-7": "T-14", "T-2": "T-7" };
    const prevWindow = prevWindowMap[windowLabel];
    let prevData: Record<string, unknown> | null = null;
    if (prevWindow) {
      const cacheEntry = _optionsFlowCache.get(`${ticker}:${prevWindow}`);
      if (cacheEntry && Date.now() - cacheEntry.storedAt < 72 * 3600 * 1000) {
        prevData = cacheEntry.data;
      }
    }

    let putVolVs10d: number | null = null;
    try {
      const fi = JSON.parse(await getFastInfo(ticker)) as Record<string, unknown>;
      const adv10 = fi.tenDayAverageVolume as number | null;
      if (adv10 && adv10 > 0 && putVol > 0) {
        putVolVs10d = +(putVol / (adv10 * 0.01)).toFixed(2);
      }
    } catch { /* ignore */ }

    let putVolTrend = "STABLE";
    const cmpCurr = putVolVs10d ?? pcRatio;
    const cmpPrev = prevData
      ? ((prevData.putVolVs10dAvg as number | null) ?? (prevData.pcRatio as number | null))
      : null;
    if (cmpCurr != null && cmpPrev != null && cmpPrev > 0) {
      const ratioChange = cmpCurr / cmpPrev;
      if (ratioChange > 1.1) putVolTrend = "INCREASING";
      else if (ratioChange < 0.9) putVolTrend = "DECREASING";
    }

    let bracket: string | null = null;
    if (pcRatio != null) {
      if (pcRatio >= 1.3 || (pcRatio >= 1.0 && putVolTrend === "INCREASING")) bracket = "UPPER";
      else if (pcRatio <= 0.8 && putVolTrend !== "INCREASING") bracket = "LOWER";
      else bracket = "MID";
    }

    const ivStr = ivPctile != null ? `${ivPctile}th%ile` : "N/A";
    const pvStr = putVolVs10d != null ? `${putVolVs10d.toFixed(2)}x` : "N/A";
    const pcStr = pcRatio != null ? pcRatio.toFixed(2) : "N/A";
    const formattedBlock = `OPTIONS FLOW SCAN [${windowLabel}] ${ticker} | P/C: ${pcStr} | IV: ${ivStr} | Put vol vs 10d avg: ${pvStr} | Trend: ${putVolTrend} | Advisory: ${bracket ?? "N/A"} bracket`;

    const resultData: Record<string, unknown> = {
      ticker, windowLabel, dataDate,
      pcRatio, ivPctile, putVolVs10dAvg: putVolVs10d, putVolTrend,
      maxPainStrike, bracket, formattedBlock,
    };

    _optionsFlowCache.set(`${ticker}:${windowLabel}`, { data: resultData, storedAt: Date.now() });
    return JSON.stringify(resultData);
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_price_target_bracket ──────────────────────────────────────────────────

export async function getPriceTargetBracket(ticker: string, ioPt: number): Promise<string> {
  if (ioPt <= 0) {
    return JSON.stringify({ error: true, message: "io_pt must be a positive number", ticker });
  }
  try {
    const fi = JSON.parse(await getFastInfo(ticker)) as Record<string, unknown>;
    const currentPrice = fi.lastPrice as number | null;
    if (currentPrice == null) {
      return JSON.stringify({ error: true, message: `No price data for ${ticker}`, ticker });
    }

    const eqfPct = +(currentPrice / ioPt * 100).toFixed(1);

    const bracket =
      eqfPct <= 75 ? "STRONG_BUY" :
      eqfPct <= 90 ? "ACCEPTABLE" :
      eqfPct <= 100 ? "CAUTION" : "AVOID";

    const tag =
      eqfPct < 40 ? "SPECULATIVE" :
      eqfPct < 80 ? "LONG" :
      eqfPct < 100 ? "NEAR" : "INVERTED";

    return JSON.stringify({
      ticker,
      currentPrice: +currentPrice.toFixed(4),
      ioPt,
      eqfPct,
      bracket,
      tag,
      invertedFlag: eqfPct >= 100,
      dataDate: (fi.lastTradeDate as string | null) ?? new Date().toISOString().slice(0, 10),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_position_score_inputs ─────────────────────────────────────────────────

export async function getPositionScoreInputs(ticker: string): Promise<string> {
  try {
    const [upgradeRaw, consensusRaw, priceRaw, earningsRaw, techRaw, maRaw] = await Promise.all([
      getAnalystUpgradeRadar(ticker, 30),
      getAnalystConsensus(ticker),
      getPriceStats(ticker),
      getEarningsMomentum(ticker),
      getTechnicalIndicators(ticker, "3mo"),
      getMaPosition(ticker),
    ]);

    const parse = (s: string): Record<string, unknown> => {
      try { return JSON.parse(s) as Record<string, unknown>; } catch { return {}; }
    };

    const upgrade = parse(upgradeRaw);
    const consensus = parse(consensusRaw);
    const price = parse(priceRaw);
    const earnings = parse(earningsRaw);
    const tech = parse(techRaw);
    const ma = parse(maRaw);

    const changes = (upgrade.changes as Record<string, unknown>[]) ?? [];
    const t1 = {
      analystNetSentiment: upgrade.netSentiment ?? null,
      upgrades30d: changes.filter((c) => c.signal === "UPGRADE").length,
      downgrades30d: changes.filter((c) => c.signal === "DOWNGRADE").length,
      dominantRating: consensus.dominantRating ?? null,
      analystCount: consensus.numberOfAnalysts ?? null,
    };

    const t2 = {
      currentPrice: price.lastPrice ?? null,
      fiftyTwoWeekHigh: price.yearHigh ?? null,
      fiftyTwoWeekLow: price.yearLow ?? null,
      pctFromYearHigh: price.pctFromYearHigh ?? null,
      pctFromYearLow: price.pctFromYearLow ?? null,
    };

    const t4 = {
      beatRate: earnings.beatRate ?? null,
      currentBeatStreak: earnings.currentBeatStreak ?? null,
      avgSurprisePct: earnings.avgSurprisePct ?? null,
      momentumFlag: earnings.momentumFlag ?? null,
    };

    const t5 = {
      rsi14: tech.rsi14 ?? null,
      macdHistogram: tech.macdHistogram ?? null,
      maPosition: ma.trend ?? null,
      pctFrom50dma: ma.pctVs50dma ?? null,
      pctFrom200dma: ma.pctVs200dma ?? null,
      lastClose: tech.lastClose ?? null,
    };

    return JSON.stringify({
      ticker,
      dataDate: (tech.dataDate as string | null) ?? (ma.dataDate as string | null) ?? new Date().toISOString().slice(0, 10),
      t1_inputs: t1,
      t2_inputs: t2,
      t4_inputs: t4,
      t5_inputs: t5,
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_volume_gate ───────────────────────────────────────────────────────────

export async function getVolumeGate(ticker: string, foreignExchange: boolean): Promise<string> {
  try {
    const fi = JSON.parse(await getFastInfo(ticker)) as Record<string, unknown>;
    const lastVolume = fi.lastVolume as number | null;
    const adv10d = fi.tenDayAverageVolume as number | null;
    const adv90d = fi.threeMonthAverageVolume as number | null;
    const lastPrice = fi.lastPrice as number | null;

    let adv20d: number | null = null;
    let dataDate = new Date().toISOString().slice(0, 10);
    try {
      const chartD = await yGet(
        `https://query1.finance.yahoo.com/v8/finance/chart/${enc(ticker)}?range=1mo&interval=1d`,
        false
      ) as Record<string, unknown>;
      const chartRes = (chartD?.chart as Record<string, unknown[]>)?.result?.[0] as Record<string, unknown>;
      const timestamps = (chartRes?.timestamp as number[]) ?? [];
      const volumes = ((chartRes?.indicators as Record<string, unknown[]>)?.quote?.[0] as Record<string, (number | null)[]>)?.volume ?? [];
      const vols = volumes.filter((v): v is number => v != null);
      if (vols.length >= 5) {
        const tail20 = vols.slice(-20);
        adv20d = Math.round(tail20.reduce((a, b) => a + b, 0) / tail20.length);
      }
      if (timestamps.length > 0) {
        dataDate = new Date(timestamps[timestamps.length - 1] * 1000).toISOString().slice(0, 10);
      }
    } catch { /* ignore */ }

    let gatePass: boolean | null = null;
    let ratio20d: number | null = null;
    let note: string;

    if (foreignExchange) {
      if (lastVolume != null && lastPrice != null && lastPrice > 0) {
        const dailyNotional = lastVolume * lastPrice;
        gatePass = dailyNotional >= 10_000_000;
        note = `Volume gate ${gatePass ? "PASS" : "FAIL"} (DC-80 FX) — $${(dailyNotional / 1_000_000).toFixed(1)}M daily notional (${gatePass ? "≥" : "<"} $10M threshold)`;
      } else {
        note = "Volume gate UNKNOWN — insufficient price/volume data for DC-80 FX check";
      }
    } else {
      if (lastVolume != null && adv20d != null && adv20d > 0) {
        ratio20d = +(lastVolume / adv20d).toFixed(2);
        gatePass = ratio20d >= 0.5;
        note = `Volume gate ${gatePass ? "PASS" : "FAIL"} — ${ratio20d.toFixed(2)}x 20d ADV`;
      } else {
        note = "Volume gate UNKNOWN — insufficient volume data for 20d ADV calculation";
      }
    }

    return JSON.stringify({
      ticker, lastVolume, adv10d, adv20d, adv90d, ratio20d, gatePass, dataDate, note,
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}
