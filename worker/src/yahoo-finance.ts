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

  const crumbRes = await fetch("https://query2.finance.yahoo.com/v1/test/getcrumb", {
    headers: { "User-Agent": UA, Cookie: cookie },
  });
  if (!crumbRes.ok) throw new Error(`Crumb fetch failed: ${crumbRes.status}`);

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
    _crumb = null;
    res = await makeReq(await getCrumb());
  }

  if (!res.ok) throw new Error(`Yahoo Finance API error ${res.status} for: ${url}`);
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

// ── Tool implementations ─────────────────────────────────────────────────────

export async function getHistoricalPrices(
  ticker: string,
  period: string,
  interval: string
): Promise<string> {
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v8/finance/chart/${enc(ticker)}?range=${period}&interval=${interval}`,
    false
  )) as Record<string, unknown>;

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

export async function getStockInfo(ticker: string): Promise<string> {
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

    return JSON.stringify(opts[optType] ?? []);
  } catch (e) {
    return `Error fetching option chain for ${ticker} on ${expDate}: ${e instanceof Error ? e.message : String(e)}`;
  }
}

const REC_MOD: Record<string, string> = {
  recommendations: "recommendationTrend",
  upgrades_downgrades: "upgradeDowngradeHistory",
};

// ── New tools ────────────────────────────────────────────────────────────────

export async function getFastInfo(ticker: string): Promise<string> {
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

  return JSON.stringify({
    currency: raw(price.currency),
    exchange: raw(price.exchangeName),
    quoteType: raw(price.quoteType),
    timezone: raw(price.exchangeTimezoneShortName),
    lastPrice: raw(price.regularMarketPrice),
    open: raw(price.regularMarketOpen),
    previousClose: raw(price.regularMarketPreviousClose),
    dayHigh: raw(price.regularMarketDayHigh),
    dayLow: raw(price.regularMarketDayLow),
    yearHigh: raw(detail.fiftyTwoWeekHigh),
    yearLow: raw(detail.fiftyTwoWeekLow),
    yearChange: raw(ks["52WeekChange" as keyof typeof ks]),
    marketCap: raw(price.marketCap),
    shares: raw(price.sharesOutstanding),
    lastVolume: raw(price.regularMarketVolume),
    tenDayAverageVolume: raw(detail.averageVolume10days),
    threeMonthAverageVolume: raw(detail.averageVolume),
    fiftyDayAverage: raw(detail.fiftyDayAverage),
    twoHundredDayAverage: raw(detail.twoHundredDayAverage),
  });
}

export async function getPriceStats(ticker: string): Promise<string> {
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

  try {
    const hist = histRaw as Record<string, unknown>;
    const chartResult = (hist?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
      | Record<string, unknown>
      | undefined;
    if (chartResult) {
      const timestamps = (chartResult.timestamp as number[]) ?? [];
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

  return JSON.stringify(stats);
}

export async function getAnalystConsensus(ticker: string): Promise<string> {
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

export async function getFinancialRatios(ticker: string): Promise<string> {
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

  return JSON.stringify({
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
  });
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

export async function searchTicker(query: string, maxResults: number): Promise<string> {
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v1/finance/search?q=${enc(query)}&quotesCount=${maxResults}&newsCount=0&enableFuzzyQuery=false`,
    false
  )) as Record<string, unknown>;

  const quotes = (d?.quotes as Record<string, unknown>[]) ?? [];
  const trimmed = quotes
    .filter((q) => q.symbol)
    .map((q) => ({
      symbol: q.symbol ?? null,
      shortname: (q.shortname ?? q.longname ?? null) as unknown,
      exchange: q.exchange ?? null,
      quoteType: q.quoteType ?? null,
      score: q.score ?? null,
    }));
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
