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

const PLACEHOLDER_IV_THRESHOLD = 0.0001;

interface DataQuality {
  zeroBidAskCount: number;
  zeroOpenInterestCount: number;
  placeholderIvCount: number;
  staleLastTradeCount: number;
  returnedContracts: number;
  quality: "HIGH" | "MEDIUM" | "LOW";
  warnings: string[];
}

function computeDataQuality(
  contracts: Record<string, unknown>[],
  dataDate: string,
  staleDaysThreshold: number = 5
): DataQuality {
  const n = contracts.length;
  if (n === 0) {
    return {
      zeroBidAskCount: 0, zeroOpenInterestCount: 0, placeholderIvCount: 0,
      staleLastTradeCount: 0, returnedContracts: 0, quality: "LOW",
      warnings: ["NO_CONTRACTS_RETURNED"],
    };
  }

  let dataDateMs: number | null = null;
  try {
    dataDateMs = new Date(dataDate).getTime();
    if (isNaN(dataDateMs)) dataDateMs = null;
  } catch { dataDateMs = null; }

  let zeroBidAsk = 0, zeroOI = 0, placeholderIv = 0, staleTrade = 0;

  for (const c of contracts) {
    const bid = Number(c.bid ?? 0);
    const ask = Number(c.ask ?? 0);
    if (bid <= 0 || ask <= 0) zeroBidAsk++;

    const oi = Number(c.openInterest ?? 0);
    if (oi <= 0) zeroOI++;

    const iv = Number(c.impliedVolatility ?? 0);
    if (iv <= PLACEHOLDER_IV_THRESHOLD) placeholderIv++;

    if (dataDateMs != null) {
      const ltd = c.lastTradeDate;
      if (ltd != null) {
        let ltdMs: number | null = null;
        if (typeof ltd === "number") {
          // Yahoo Finance returns epoch seconds (×1000 to get ms) or epoch ms
          ltdMs = ltd > 1e10 ? ltd : ltd * 1000;
        } else if (typeof ltd === "string") {
          ltdMs = new Date(ltd).getTime();
        }
        if (ltdMs != null && !isNaN(ltdMs)) {
          const ageDays = (dataDateMs - ltdMs) / 86400000;
          if (ageDays > staleDaysThreshold) staleTrade++;
        }
      }
    }
  }

  const warnings: string[] = [];

  // Per-dimension thresholds (any single dimension can trigger LOW/MEDIUM)
  let quality: "HIGH" | "MEDIUM" | "LOW";
  const zeroBAFrac = zeroBidAsk / n;
  const zeroOIFrac = zeroOI / n;
  const placeholderIvFrac = placeholderIv / n;
  const staleFrac = staleTrade / n;

  if (
    zeroBAFrac > 0.50 ||
    zeroOIFrac > 0.80 ||
    placeholderIvFrac > 0.50 ||
    staleFrac > 0.50
  ) {
    quality = "LOW";
  } else if (
    zeroBAFrac > 0.30 ||
    zeroOIFrac > 0.50 ||
    placeholderIvFrac > 0.30 ||
    staleFrac > 0.30
  ) {
    quality = "MEDIUM";
  } else {
    quality = "HIGH";
  }

  if (zeroBidAsk > n * 0.5) warnings.push("MAJORITY_ZERO_BID_ASK");
  if (zeroOI > n * 0.5) warnings.push("MAJORITY_ZERO_OPEN_INTEREST");
  if (placeholderIv > n * 0.5) warnings.push("MAJORITY_PLACEHOLDER_IV");
  if (staleTrade > n * 0.5) warnings.push("MAJORITY_STALE_LAST_TRADE");

  return { zeroBidAskCount: zeroBidAsk, zeroOpenInterestCount: zeroOI, placeholderIvCount: placeholderIv,
           staleLastTradeCount: staleTrade, returnedContracts: n, quality, warnings };
}

function sortByRelevance(
  contracts: Record<string, unknown>[],
  underlyingPrice: number | null
): Record<string, unknown>[] {
  return [...contracts].sort((a, b) => {
    const aBid = Number(a.bid ?? 0), aAsk = Number(a.ask ?? 0);
    const bBid = Number(b.bid ?? 0), bAsk = Number(b.ask ?? 0);
    const aOI = Number(a.openInterest ?? 0), aVol = Number(a.volume ?? 0);
    const bOI = Number(b.openInterest ?? 0), bVol = Number(b.volume ?? 0);
    const aIV = Number(a.impliedVolatility ?? 0);
    const bIV = Number(b.impliedVolatility ?? 0);
    const aStrike = Number(a.strike ?? 0), bStrike = Number(b.strike ?? 0);

    const aValidQuote = aBid > 0 && aAsk > 0 ? 1 : 0;
    const bValidQuote = bBid > 0 && bAsk > 0 ? 1 : 0;
    if (bValidQuote !== aValidQuote) return bValidQuote - aValidQuote;

    const aLiquidity = (aOI > 0 || aVol > 0) ? 1 : 0;
    const bLiquidity = (bOI > 0 || bVol > 0) ? 1 : 0;
    if (bLiquidity !== aLiquidity) return bLiquidity - aLiquidity;

    const aValidIv = aIV > PLACEHOLDER_IV_THRESHOLD ? 1 : 0;
    const bValidIv = bIV > PLACEHOLDER_IV_THRESHOLD ? 1 : 0;
    if (bValidIv !== aValidIv) return bValidIv - aValidIv;

    if (underlyingPrice != null && underlyingPrice > 0) {
      const aDist = Math.abs(aStrike - underlyingPrice) / underlyingPrice;
      const bDist = Math.abs(bStrike - underlyingPrice) / underlyingPrice;
      if (Math.abs(aDist - bDist) > 1e-9) return aDist - bDist;
    }

    if (bOI !== aOI) return bOI - aOI;
    if (bVol !== aVol) return bVol - aVol;

    const aSpread = aBid > 0 && aAsk > 0 ? (aAsk - aBid) / ((aBid + aAsk) / 2) : 9999;
    const bSpread = bBid > 0 && bAsk > 0 ? (bAsk - bBid) / ((bBid + bAsk) / 2) : 9999;
    return aSpread - bSpread;
  });
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

// Extract the `raw` numeric value from Yahoo Finance's {raw, fmt} wrapper objects.
// Empty objects {} are treated as missing values and normalized to null.
function raw(v: unknown): unknown {
  if (v !== null && v !== undefined && typeof v === "object") {
    if ("raw" in (v as object)) {
      return (v as { raw: unknown }).raw;
    }
    // Yahoo Finance sometimes returns {} for fields that have no value — treat as null.
    if (Object.keys(v as object).length === 0) return null;
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

function toBatchError(message: string, code = "PROVIDER_ERROR", retryable = false): Record<string, unknown> {
  return { code, message, retryable };
}

function normalizeBatchSymbolResult(parsed: unknown, ticker: string): Record<string, unknown> {
  if (parsed != null && typeof parsed === "object") {
    const p = parsed as Record<string, unknown>;
    if (p.error === true || p.error != null) {
      const message = typeof p.message === "string" ? p.message : (typeof p.error === "string" ? p.error : `Error for ${ticker}`);
      const lower = message.toLowerCase();
      if (lower.includes("no data found for ticker") || lower.includes("not found") || lower.includes("api error 404")) {
        return { ok: false, data: null, error: toBatchError(message, "TICKER_NOT_FOUND", false) };
      }
      if (lower.includes("timeout")) {
        return { ok: false, data: null, error: toBatchError(message, "PROVIDER_TIMEOUT", true) };
      }
      return { ok: false, data: null, error: toBatchError(message, "PROVIDER_ERROR", false) };
    }
    return { ok: true, data: p, error: null };
  }
  return { ok: false, data: null, error: toBatchError(`Malformed response for ${ticker}`, "PROVIDER_ERROR", false) };
}

async function runPartialBatch(
  tickers: string[],
  perTicker: (ticker: string) => Promise<string>
): Promise<string> {
  if (tickers.length > MAX_TICKERS) {
    return JSON.stringify({
      error: true,
      code: "INPUT_VALIDATION_ERROR",
      message: `Too many tickers: ${tickers.length}. Maximum is ${MAX_TICKERS} per call.`,
    });
  }
  const out: Record<string, unknown> = {};
  let successCount = 0;
  let errorCount = 0;
  for (const t of tickers) {
    try {
      const raw = await perTicker(t);
      const parsed = safeJsonParse(raw, t);
      const shaped = normalizeBatchSymbolResult(parsed, t);
      if (shaped.ok === true) successCount += 1;
      else errorCount += 1;
      out[t] = shaped;
    } catch (e) {
      errorCount += 1;
      const message = e instanceof Error ? e.message : String(e);
      const lower = message.toLowerCase();
      const code = lower.includes("timeout") ? "PROVIDER_TIMEOUT"
        : lower.includes("api error 404") ? "TICKER_NOT_FOUND"
        : "PROVIDER_ERROR";
      out[t] = {
        ok: false,
        data: null,
        error: toBatchError(message, code, lower.includes("timeout")),
      };
    }
  }
  out.__batchMeta = {
    partialSuccess: successCount > 0 && errorCount > 0,
    successCount,
    errorCount,
  };
  return JSON.stringify(out);
}


// ── get_etf_info ─────────────────────────────────────────────────────────────

export async function getEtfInfo(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getEtfInfo(t));
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
    return runPartialBatch(ticker, (t) => getStockInfo(t, includeAll));
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
  const retrievedAt = new Date().toISOString();
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v1/finance/search?q=${enc(ticker)}&quotesCount=0&newsCount=20&enableFuzzyQuery=false`,
    false
  )) as Record<string, unknown>;

  const news = (d?.news as Record<string, unknown>[]) ?? [];
  const tickerUpper = ticker.toUpperCase();
  const items = news.map((n) => {
    const title = String(n.title ?? "");
    return {
      title,
      publisher: String(n.publisher ?? ""),
      url: String(n.link ?? ""),
      publishedAt: n.providerPublishTime ? iso(Number(n.providerPublishTime)) : null,
      retrievedAt,
      sourceType: "yahoo_finance",
      tickerRelevance: title.toUpperCase().includes(tickerUpper) ? "HIGH" : "UNKNOWN",
    };
  });
  return JSON.stringify({
    ticker,
    items,
    meta: {
      source: "yahoo_finance",
      watermark: retrievedAt,
      itemCount: items.length,
    },
  });
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
  optType: string,
  maxContracts: number = 50,
  minOpenInterest: number = 0,
  minVolume: number = 0,
  strikeMin: number | null = null,
  strikeMax: number | null = null,
  moneyness: string = "near_money",
  sortBy: string = "relevance",
  moneynessWindowPct: number = 20,
  includeIlliquid: boolean = false,
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
      : getLastTradingDate();

    const underlyingPrice = typeof quote.regularMarketPrice === "number" ? quote.regularMarketPrice : null;

    let contracts = (opts[optType] ?? []) as Record<string, unknown>[];
    if (strikeMin != null) {
      contracts = contracts.filter(c => ((c.strike as number) || 0) >= strikeMin);
    }
    if (strikeMax != null) {
      contracts = contracts.filter(c => ((c.strike as number) || 0) <= strikeMax);
    }
    if (moneyness !== "all") {
      contracts = contracts.filter(c => {
        const itm = c.inTheMoney === true;
        if (moneyness === "itm") return itm;
        if (moneyness === "otm") return !itm;
        if (moneyness === "near_money") {
          const strike = (c.strike as number) || 0;
          const underlying = underlyingPrice ?? ((quote.regularMarketPrice as number) || strike);
          if (underlying <= 0) return false;
          const windowFraction = moneynessWindowPct / 100;
          return Math.abs(strike - underlying) / underlying <= windowFraction;
        }
        return true;
      });
    }
    if (minOpenInterest > 0) {
      contracts = contracts.filter(c => ((c.openInterest as number) || 0) >= minOpenInterest);
    }
    if (minVolume > 0) {
      contracts = contracts.filter(c => ((c.volume as number) || 0) >= minVolume);
    }

    // include_illiquid=false: drop contracts with zero bid/ask AND zero OI
    if (!includeIlliquid) {
      contracts = contracts.filter(c => {
        const bid = Number(c.bid ?? 0);
        const ask = Number(c.ask ?? 0);
        const oi = Number(c.openInterest ?? 0);
        return bid > 0 || ask > 0 || oi > 0;
      });
    }

    if (sortBy === "relevance") {
      contracts = sortByRelevance(contracts, underlyingPrice);
    } else if (sortBy === "volume") {
      contracts = contracts.sort((a, b) => (((b.volume as number) || 0) - ((a.volume as number) || 0)));
    } else if (sortBy === "openInterest") {
      contracts = contracts.sort((a, b) => (((b.openInterest as number) || 0) - ((a.openInterest as number) || 0)));
    } else {
      contracts = contracts.sort((a, b) => (((a.strike as number) || 0) - ((b.strike as number) || 0)));
    }
    const totalContracts = contracts.length;
    if (maxContracts > 0) {
      contracts = contracts.slice(0, maxContracts);
    }
    const returnedContracts = contracts.length;
    const dataQuality = computeDataQuality(contracts, dataDate);

    return JSON.stringify({
      ticker,
      expiration: expDate,
      optionType: optType,
      dataDate,
      totalContracts,
      returnedContracts,
      truncated: returnedContracts < totalContracts,
      dataQuality,
      filtersApplied: {
        max_contracts: maxContracts,
        min_open_interest: minOpenInterest,
        min_volume: minVolume,
        strike_min: strikeMin,
        strike_max: strikeMax,
        moneyness,
        moneyness_window_pct: moneynessWindowPct,
        sort_by: sortBy,
        include_illiquid: includeIlliquid,
      },
      contracts,
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
    return runPartialBatch(ticker, (t) => getFastInfo(t));
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
    return runPartialBatch(ticker, (t) => getPriceStats(t));
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
    return runPartialBatch(ticker, (t) => getAnalystConsensus(t));
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
    return runPartialBatch(ticker, (t) => getFinancialRatios(t));
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

  // Fetch EDGAR submissions to enrich filings with direct EDGAR URLs
  const edgarUrlMap = new Map<string, { accessionNumber: string; edgarIndexUrl: string; edgarPrimaryDocumentUrl: string | null }>();
  try {
    const cik = await edgarResolveCik(ticker);
    if (cik != null) {
      const cikPadded = String(cik).padStart(10, "0");
      const subs = await edgarGetJson(`https://data.sec.gov/submissions/CIK${cikPadded}.json`);
      if (subs) {
        const recent = ((subs.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
        const forms = (recent.form as string[]) ?? [];
        const dates = (recent.filingDate as string[]) ?? [];
        const accessions = (recent.accessionNumber as string[]) ?? [];
        const primaryDocs = (recent.primaryDocument as string[]) ?? [];
        for (let i = 0; i < forms.length; i++) {
          const acc = accessions[i];
          const date = dates[i];
          if (acc && date) {
            const { edgarIndexUrl, edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(cik, acc, primaryDocs[i] ?? null);
            edgarUrlMap.set(`${forms[i]}:${date}`, { accessionNumber: acc, edgarIndexUrl, edgarPrimaryDocumentUrl });
          }
        }
      }
    }
  } catch { /* non-fatal */ }

  const out = await Promise.all(filings.map(async (f) => {
    const date = f.epochDate != null ? iso(f.epochDate as number) : null;
    const type = (f.type as string) ?? null;
    // EDGAR filingDate is "YYYY-MM-DD"; Yahoo epochDate converts to a full ISO
    // string — use only the date portion for the map lookup.
    const edgarInfo = date && type ? edgarUrlMap.get(`${type}:${date.slice(0, 10)}`) : undefined;

    // Fallback: extract accession + CIK from Yahoo's edgarUrl when not found via EDGAR submissions.
    let accessionNumber: string | null = edgarInfo?.accessionNumber ?? null;
    let edgarIndexUrl: string | null = edgarInfo?.edgarIndexUrl ?? null;
    let edgarPrimaryDocumentUrl: string | null = edgarInfo?.edgarPrimaryDocumentUrl ?? null;
    if (!accessionNumber) {
      const eu = (f.edgarUrl as string) ?? "";
      // Pattern 1: direct EDGAR Archives URL
      // e.g. https://www.sec.gov/Archives/edgar/data/1234/000123456724000001/...
      const m = eu.match(/\/Archives\/edgar\/data\/(\d+)\/(\d{18})\//);
      if (m) {
        const cikFromUrl = parseInt(m[1], 10);
        const noDash = m[2];
        accessionNumber = `${noDash.slice(0, 10)}-${noDash.slice(10, 12)}-${noDash.slice(12)}`;
        const fname = eu.split("/").pop()?.split("?")[0] ?? null;
        const urls = edgarBuildFilingUrls(cikFromUrl, accessionNumber, fname);
        edgarIndexUrl = urls.edgarIndexUrl;
        edgarPrimaryDocumentUrl = urls.edgarPrimaryDocumentUrl;
      } else {
        // Pattern 2: Yahoo Finance sec-filing proxy URL
        // e.g. https://finance.yahoo.com/sec-filing/GLW/0000024741-26-000124_24741
        const m2 = eu.match(/\/sec-filing\/[^/]+\/(\d{10}-\d{2}-\d{6})_(\d+)/);
        if (m2) {
          accessionNumber = m2[1];
          const cikFromUrl = parseInt(m2[2], 10);
          const urls = edgarBuildFilingUrls(cikFromUrl, accessionNumber, null);
          edgarIndexUrl = urls.edgarIndexUrl;
          edgarPrimaryDocumentUrl = null;
        }
      }
    }

    // For annual (10-K) filings: eagerly resolve the primary document URL from the
    // EDGAR index page when it could not be determined from the submissions JSON or the
    // Yahoo URL.  This avoids a second round-trip inside getFilingDocument / getFilingTextSearch.
    if (!edgarPrimaryDocumentUrl && edgarIndexUrl && type === "10-K") {
      try {
        const fname = await edgarPrimaryDocFromIndex(edgarIndexUrl);
        if (fname && accessionNumber) {
          const cikFromIndex = edgarCikFromAccession(accessionNumber);
          if (cikFromIndex != null) {
            const { edgarPrimaryDocumentUrl: resolved } = edgarBuildFilingUrls(cikFromIndex, accessionNumber, fname);
            edgarPrimaryDocumentUrl = resolved;
          }
        }
      } catch { /* non-fatal */ }
    }

    return {
      date,
      type,
      title: f.title ?? null,
      edgarUrl: f.edgarUrl ?? null,
      accessionNumber,
      edgarIndexUrl,
      edgarPrimaryDocumentUrl,
    };
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
    return runPartialBatch(ticker, (t) => getVolumeRatio(t, _period));
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
    return runPartialBatch(ticker, (t) => getMaPosition(t));
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
    return runPartialBatch(ticker, (t) => getCreditHealth(t));
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
    return runPartialBatch(ticker, (t) => getShortMomentum(t));
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
    return runPartialBatch(ticker, (t) => getEarningsMomentum(t));
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

    // ATM strike — reject placeholder IV
    let atmStrike: number | null = null;
    let atmIV: number | null = null;
    const flowWarnings: string[] = [];
    if (lastPrice == null) {
      flowWarnings.push("ATM_IV_UNAVAILABLE_NO_PRICE");
    } else if (calls.length === 0) {
      flowWarnings.push("ATM_IV_UNAVAILABLE_NO_CALLS");
    } else {
      let minDist = Infinity;
      let rawAtmIV: number | null = null;
      for (const c of calls) {
        const strike = c.strike as number;
        const iv = c.impliedVolatility as number | null;
        if (strike != null) {
          const dist = Math.abs(strike - lastPrice);
          if (dist < minDist) {
            minDist = dist;
            atmStrike = strike;
            rawAtmIV = iv ?? null;
          }
        }
      }
      if (rawAtmIV != null && rawAtmIV > PLACEHOLDER_IV_THRESHOLD) {
        atmIV = +rawAtmIV.toFixed(3);
      } else {
        flowWarnings.push("ATM_IV_PLACEHOLDER");
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

    // Max pain — suppress when OI is all-zero
    let maxPainStrike: number | null = null;
    if (totalCallOI + totalPutOI <= 0) {
      flowWarnings.push("MAX_PAIN_UNAVAILABLE_ZERO_OI");
    } else {
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

    const dataDate = getLastTradingDate();
    const dataQuality = computeDataQuality([...calls, ...puts], dataDate);

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
      dataDate,
      dataQuality,
      warnings: flowWarnings,
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
    return runPartialBatch(ticker, (t) => getAnalystUpgradeRadar(t, daysBack));
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

// EDGAR fair-access policy requires a reachable contact in the User-Agent.
// Replace the URL/contact below with one owned by the operator, or inject
// via the EDGAR_CONTACT_EMAIL Cloudflare secret / var.
const EDGAR_UA = "yahoo-finance-mcp contact@example.com";

const filingCikCache = new Map<string, string>();
const filingSubmissionsCache = new Map<string, Record<string, unknown>>();
const filingIndexCache = new Map<string, { value: string; storedAt: number }>();
const FILING_INDEX_TTL_MS = 24 * 60 * 60 * 1000;

const FILING_FACT_CONCEPTS: Record<string, { primary: string; fallback?: string }> = {
  geographic_revenue: { primary: "RevenueFromContractWithCustomerExcludingAssessedTax", fallback: "Revenues" },
  segment_revenue: { primary: "RevenueFromContractWithCustomerExcludingAssessedTax", fallback: "Revenues" },
  capex: { primary: "PaymentsToAcquirePropertyPlantAndEquipment" },
  rd_expense: { primary: "ResearchAndDevelopmentExpense" },
  operating_income: { primary: "OperatingIncomeLoss" },
  net_income: { primary: "NetIncomeLoss" },
  total_revenue: { primary: "RevenueFromContractWithCustomerExcludingAssessedTax", fallback: "Revenues" },
  long_term_debt: { primary: "LongTermDebt" },
  cash: { primary: "CashAndCashEquivalentsAtCarryingValue" },
};

// Confirmed China revenue figures from manually verified 10-K filings.
// Mirrors the _CHINA_REVENUE_CONFIRMED lookup in server.py.
// pct is the decimal fraction (e.g. 0.620 = 62%).
const CHINA_REVENUE_CONFIRMED: Record<string, { pct: number; fiscalYear: string; filingDate: string }> = {
  "MU":   { pct: 0.117, fiscalYear: "FY2025", filingDate: "2025-10-03" },
  "AAPL": { pct: 0.170, fiscalYear: "FY2025", filingDate: "2025-10-31" },
  "ANET": { pct: 0.140, fiscalYear: "FY2024", filingDate: "2025-02-14" },
  "QCOM": { pct: 0.620, fiscalYear: "FY2024", filingDate: "2024-11-06" },
  "NVDA": { pct: 0.170, fiscalYear: "FY2025", filingDate: "2025-02-26" },
  "AMD":  { pct: 0.220, fiscalYear: "FY2024", filingDate: "2025-02-04" },
  "AVGO": { pct: 0.350, fiscalYear: "FY2024", filingDate: "2024-12-19" },
  "SWKS": { pct: 0.580, fiscalYear: "FY2024", filingDate: "2024-11-20" },
  "MRVL": { pct: 0.550, fiscalYear: "FY2025", filingDate: "2025-03-13" },
  "ON":   { pct: 0.330, fiscalYear: "FY2024", filingDate: "2025-02-10" },
};

// ── EDGAR helper utilities ────────────────────────────────────────────────────

/** Fetch an EDGAR JSON endpoint using the required User-Agent header. */
async function edgarGetJson(url: string): Promise<Record<string, unknown> | null> {
  try {
    const resp = await fetch(url, { headers: { "User-Agent": EDGAR_UA } });
    if (!resp.ok) { await resp.body?.cancel(); return null; }
    return await resp.json() as Record<string, unknown>;
  } catch {
    return null;
  }
}

/** Fetch an EDGAR HTML/text document. Reads at most maxBytes from the response
 *  stream and cancels the rest, avoiding large memory allocations for big filings. */
async function edgarGetHtml(url: string, maxBytes = 5_000_000): Promise<string | null> {
  try {
    const resp = await fetch(url, { headers: { "User-Agent": EDGAR_UA } });
    if (!resp.ok || !resp.body) { await resp.body?.cancel(); return null; }
    const reader = resp.body.getReader();
    const chunks: Uint8Array[] = [];
    let totalBytes = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) {
        const remaining = maxBytes - totalBytes;
        if (value.byteLength <= remaining) {
          chunks.push(value);
          totalBytes += value.byteLength;
        } else {
          if (remaining > 0) {
            chunks.push(value.slice(0, remaining));
            totalBytes = maxBytes;
          }
          await reader.cancel();
          break;
        }
      }
    }
    const combined = new Uint8Array(totalBytes);
    let offset = 0;
    for (const chunk of chunks) { combined.set(chunk, offset); offset += chunk.byteLength; }
    return new TextDecoder("utf-8", { fatal: false, ignoreBOM: true }).decode(combined);
  } catch {
    return null;
  }
}

/** Construct EDGAR filing index URL and primary document URL from CIK + accession + primaryDoc. */
function edgarBuildFilingUrls(
  cik: number,
  accessionNumber: string,
  primaryDoc: string | null
): { edgarIndexUrl: string; edgarPrimaryDocumentUrl: string | null } {
  const noDash = accessionNumber.replace(/-/g, "");
  const edgarIndexUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${noDash}/${accessionNumber}-index.htm`;
  const edgarPrimaryDocumentUrl = primaryDoc
    ? `https://www.sec.gov/Archives/edgar/data/${cik}/${noDash}/${primaryDoc}`
    : null;
  return { edgarIndexUrl, edgarPrimaryDocumentUrl };
}

/**
 * Fetch the EDGAR filing index HTM and return the primary document filename.
 *
 * The EDGAR filing index page (e.g. ``0000024741-26-000124-index.htm``) lists all
 * documents for a filing. The sequence-1 entry is the primary document (e.g.
 * ``glw-20251231.htm``). This function is ticker- and naming-convention-agnostic and
 * works regardless of the EDGAR submissions window size.
 *
 * Returns the bare filename (suitable for passing to edgarBuildFilingUrls), or null on
 * failure.
 */
async function edgarPrimaryDocFromIndex(indexUrl: string): Promise<string | null> {
  const html = await edgarGetHtml(indexUrl);
  if (!html) return null;
  const normalizeHref = (rawHref: string): string | null => {
    let href = rawHref.trim();
    if (!href) return null;
    // SEC often wraps filing docs as /ixviewer/ix.html?doc=/Archives/.../file.htm
    const docParam = href.match(/[?&]doc=([^&#]+)/i);
    if (docParam) {
      try {
        href = decodeURIComponent(docParam[1]);
      } catch {
        href = docParam[1];
      }
    }
    href = href.split("#", 1)[0].split("?", 1)[0];
    if (!href) return null;
    const fname = href.includes("/") ? (href.split("/").pop() ?? "") : href;
    return fname.trim() || null;
  };

  // Prefer the first row matching Sequence=1 OR Type=10-K.
  for (const rowM of html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const rowHtml = rowM[1];
    const cells = [...rowHtml.matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)].map(m => stripHtmlTags(m[1]));
    if (cells.length === 0) continue;
    const seq = (cells[0] ?? "").trim();
    const documentType = (cells[1] ?? "").trim().toUpperCase();
    if (seq === "1" || documentType.startsWith("10-K")) {
      const hrefM = rowHtml.match(/<a[^>]+href=["']([^"']+)["']/i);
      if (hrefM) {
        const fname = normalizeHref(hrefM[1]);
        if (fname && !fname.toLowerCase().endsWith("-index.htm") && !fname.toLowerCase().endsWith("-index.html")) {
          return fname;
        }
      }
    }
  }

  // Fallback: return the first document-like link that is not the index file itself.
  const allHrefs = [...html.matchAll(/href=["']([^"']+)["']/gi)].map(m => m[1]);
  for (const href of allHrefs) {
    const fname = normalizeHref(href);
    if (fname && /\.(html?)$/i.test(fname) && !fname.toLowerCase().endsWith("-index.htm") && !fname.toLowerCase().endsWith("-index.html")) return fname;
  }
  return null;
}

/**
 * Derive the EDGAR CIK from an accession number.
 * The first 10 digits of an accession number are the zero-padded filer CIK.
 * e.g. "0000024741-26-000124" → 24741
 */
function edgarCikFromAccession(accessionNumber: string): number | null {
  try {
    // Strip leading zeros; empty string ("") is falsy so "|| '0'" handles the
    // all-zeros edge case without producing NaN from parseInt("", 10).
    const stripped = accessionNumber.split("-")[0].replace(/^0+/, "") || "0";
    const n = parseInt(stripped, 10);
    return isNaN(n) || n === 0 ? null : n;
  } catch {
    return null;
  }
}

/** Resolve CIK for a ticker using the EDGAR company_tickers.json index. */
async function edgarResolveCik(ticker: string): Promise<number | null> {
  const data = await edgarGetJson("https://www.sec.gov/files/company_tickers.json");
  if (!data) return null;
  const upper = ticker.toUpperCase();
  for (const entry of Object.values(data) as { ticker: string; cik_str: number }[]) {
    if (entry.ticker.toUpperCase() === upper) return entry.cik_str;
  }
  return null;
}

async function resolveCikForTicker(ticker: string): Promise<string | null> {
  const key = ticker.toUpperCase();
  const cached = filingCikCache.get(key);
  if (cached) return cached;

  try {
    const sec = await yGet(
      `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=secFilings`
    ) as Record<string, unknown>;
    const result = (sec?.quoteSummary as Record<string, unknown[]>)?.result?.[0] as Record<string, unknown> | undefined;
    const secFilings = result?.secFilings as Record<string, unknown> | undefined;
    const cikFromYahoo = secFilings?.cik as string | number | undefined;
    if (cikFromYahoo != null) {
      const cik = String(cikFromYahoo).replace(/\D/g, "").padStart(10, "0");
      filingCikCache.set(key, cik);
      return cik;
    }
  } catch { /* non-fatal */ }

  const cikFromTickerFile = await edgarResolveCik(ticker);
  if (cikFromTickerFile != null) {
    const cik = String(cikFromTickerFile).padStart(10, "0");
    filingCikCache.set(key, cik);
    return cik;
  }

  try {
    const atom = await fetch(
      `https://www.sec.gov/cgi-bin/browse-edgar?company=${encodeURIComponent(ticker)}&action=getcompany&output=atom`,
      { headers: { "User-Agent": EDGAR_UA } }
    );
    if (atom.ok) {
      const text = await atom.text();
      const m = text.match(/CIK=(\d{1,10})/);
      if (m) {
        const cik = m[1].padStart(10, "0");
        filingCikCache.set(key, cik);
        return cik;
      }
    } else {
      await atom.body?.cancel();
    }
  } catch { /* non-fatal */ }

  return null;
}

async function getSubmissionsForTicker(ticker: string): Promise<{ cikPadded: string | null; submissions: Record<string, unknown> | null }> {
  const key = ticker.toUpperCase();
  const cachedSubmissions = filingSubmissionsCache.get(key) ?? null;
  const cikPadded = await resolveCikForTicker(ticker);
  if (!cikPadded) return { cikPadded: null, submissions: null };
  if (cachedSubmissions) return { cikPadded, submissions: cachedSubmissions };
  const submissions = await edgarGetJson(`https://data.sec.gov/submissions/CIK${cikPadded}.json`);
  if (submissions) filingSubmissionsCache.set(key, submissions);
  return { cikPadded, submissions };
}

/** Look up the most recent 10-K filing info for a CIK from EDGAR submissions. */
async function edgarGetLatest10K(cik: number): Promise<{
  filingDate: string | null;
  fiscalYear: string | null;
  accessionNumber: string | null;
  primaryDocument: string | null;
  edgarIndexUrl: string | null;
  edgarPrimaryDocumentUrl: string | null;
} | null> {
  const cikPadded = String(cik).padStart(10, "0");
  const subs = await edgarGetJson(`https://data.sec.gov/submissions/CIK${cikPadded}.json`);
  if (!subs) return null;
  const filings = ((subs.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (filings.form as string[]) ?? [];
  const dates = (filings.filingDate as string[]) ?? [];
  const periods = (filings.reportDate as string[]) ?? [];
  const accessions = (filings.accessionNumber as string[]) ?? [];
  const primaryDocs = (filings.primaryDocument as string[]) ?? [];
  for (let i = 0; i < forms.length; i++) {
    if (["10-K", "10-K405", "10-KSB"].includes(forms[i])) {
      const acc = accessions[i] ?? null;
      const pdoc = primaryDocs[i] ?? null;
      const { edgarIndexUrl, edgarPrimaryDocumentUrl } = acc
        ? edgarBuildFilingUrls(cik, acc, pdoc)
        : { edgarIndexUrl: null, edgarPrimaryDocumentUrl: null };
      const period = periods[i];
      return {
        filingDate: dates[i] ?? null,
        fiscalYear: period ? `FY${period.slice(0, 4)}` : null,
        accessionNumber: acc,
        primaryDocument: pdoc,
        edgarIndexUrl,
        edgarPrimaryDocumentUrl,
      };
    }
  }
  return null;
}

// ── HTML table parsing helpers ────────────────────────────────────────────────

function stripHtmlTags(html: string): string {
  const sanitizedHtml = html
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script[^>]*>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style[^>]*>/gi, " ")
    .replace(/\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, " ");
  // Remove all HTML tags first, then decode entities in a single pass to avoid double-unescaping.
  const noTags = sanitizedHtml.replace(/<[^>]+>/g, " ");
  const ENTITY_MAP: Record<string, string> = {
    "&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"', "&apos;": "'",
  };
  const decoded = noTags.replace(/&(?:nbsp|amp|lt|gt|quot|apos|#\d+|[a-z]+);/gi, (entity) => {
    if (entity in ENTITY_MAP) return ENTITY_MAP[entity];
    if (entity.startsWith("&#")) {
      const code = parseInt(entity.slice(2, -1), 10);
      return isNaN(code) ? " " : String.fromCharCode(code);
    }
    return " ";
  });
  return decoded.replace(/\s+/g, " ").trim();
}

function sanitizeFilingHtml(html: string): string {
  return html
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script[^>]*>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style[^>]*>/gi, " ")
    .replace(/\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, " ");
}

/** Parse a single HTML <table> into a list of rows (each row is a list of plain-text cells). */
function parseHtmlTable(tableHtml: string): string[][] {
  const rows: string[][] = [];
  const trRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
  let trMatch: RegExpExecArray | null;
  while ((trMatch = trRe.exec(tableHtml)) !== null) {
    const row: string[] = [];
    let tdMatch: RegExpExecArray | null;
    while ((tdMatch = tdRe.exec(trMatch[1])) !== null) {
      row.push(stripHtmlTags(tdMatch[1]));
    }
    if (row.length > 0) rows.push(row);
  }
  return rows;
}

/** Parse a numeric cell value, handling commas, parens, and $/%/scale suffixes. */
function parseNumericCell(text: string): number | null {
  let s = text.replace(/,/g, "").replace(/\s/g, "").replace(/\$/g, "").replace(/%/g, "");
  if (s.startsWith("(") && s.endsWith(")")) s = "-" + s.slice(1, -1);
  let mult = 1;
  if (/b$/i.test(s)) { mult = 1e9; s = s.slice(0, -1); }
  else if (/m$/i.test(s)) { mult = 1e6; s = s.slice(0, -1); }
  else if (/k$/i.test(s)) { mult = 1e3; s = s.slice(0, -1); }
  const n = parseFloat(s);
  return isNaN(n) ? null : n * mult;
}

/** Detect the monetary unit multiplier from table HTML and surrounding context. */
function detectUnitMultiplier(tableHtml: string, contextHtml: string): number {
  const combined = (tableHtml + contextHtml).toLowerCase();
  if (/in billions|\$ billions/.test(combined)) return 1e9;
  if (/in thousands|\$ thousands/.test(combined)) return 1e3;
  if (/in millions|\$ millions/.test(combined)) return 1e6;
  return 1e6; // most common 10-K scale
}

const TOTAL_LABELS = new Set([
  "total", "consolidated", "total revenues", "total net revenues",
  "net revenues", "revenues", "total revenue",
]);

/**
 * Search an SEC filing HTML document for a geographic revenue table.
 * Returns { pct, usd, sectionHeading, parsedTables } or null if not found.
 */
function extractGeoRevenueFromHtml(
  html: string,
  region: string
): {
  pct: number;
  usd: number | null;
  denominator: number | null;
  sectionHeading: string;
  parsedTables: unknown[];
  unitScale: "thousands" | "millions" | "actual";
  rawValue: string | null;
  rawDenominator: string | null;
  sourceRows: string[][];
  sourceColumns: string[];
} | null {
  const regionLower = region.toLowerCase();
  const htmlLower = html.toLowerCase();

  const searchTerms = [
    "geographic information",
    "geographic areas",
    "geographic segment",
    "revenue by region",
    "revenues by geography",
    regionLower,
  ];

  // Collect match positions (capped)
  const positions: number[] = [];
  for (const term of searchTerms) {
    let idx = 0;
    while (positions.length < 30) {
      const pos = htmlLower.indexOf(term, idx);
      if (pos === -1) break;
      positions.push(pos);
      idx = pos + 1;
    }
  }
  if (positions.length === 0) return null;

  const checkedTables = new Set<number>();
  const candidateTables: { pos: number; tableHtml: string; rows: string[][] }[] = [];

  for (const pos of [...new Set(positions)].slice(0, 20)) {
    const searchStart = Math.max(0, pos - 1_000);
    const searchEnd = Math.min(html.length, pos + 60_000);
    const chunk = html.slice(searchStart, searchEnd);

    const tblRe = /<table[^>]*>/gi;
    let tblM: RegExpExecArray | null;
    while ((tblM = tblRe.exec(chunk)) !== null) {
      const absStart = searchStart + tblM.index;
      if (checkedTables.has(absStart)) continue;
      checkedTables.add(absStart);

      // Walk forward tracking nesting depth to find matching </table>
      let depth = 0;
      let i = absStart;
      let tableEnd = absStart;
      while (i < Math.min(html.length, absStart + 200_000)) {
        const o = htmlLower.indexOf("<table", i);
        const c = htmlLower.indexOf("</table>", i);
        if (o === -1 && c === -1) break;
        if (o !== -1 && (c === -1 || o < c)) { depth++; i = o + 6; }
        else { depth--; if (depth === 0) { tableEnd = c + 8; break; } i = c + 8; }
      }

      const tableHtml = html.slice(absStart, tableEnd);
      if (!tableHtml.toLowerCase().includes(regionLower)) continue;
      const rows = parseHtmlTable(tableHtml);
      if (rows.length < 2) continue;
      candidateTables.push({ pos: absStart, tableHtml, rows });
    }
  }

  for (const tbl of candidateTables) {
    const { rows, tableHtml } = tbl;

    // Find region row
    let regionRowIdx: number | null = null;
    for (let i = 0; i < rows.length; i++) {
      if (rows[i].some(c => c.toLowerCase().includes(regionLower))) { regionRowIdx = i; break; }
    }
    if (regionRowIdx === null) continue;

    // Find total row
    let totalRowIdx: number | null = null;
    for (let i = 0; i < rows.length; i++) {
      if (rows[i].some(c => TOTAL_LABELS.has(c.trim().toLowerCase()))) { totalRowIdx = i; break; }
    }
    if (totalRowIdx === null) {
      for (let i = rows.length - 1; i >= 0; i--) {
        if (rows[i].some(c => parseNumericCell(c) !== null)) { totalRowIdx = i; break; }
      }
    }
    if (totalRowIdx === null || totalRowIdx === regionRowIdx) continue;

    // Find value column
    let valueCol: number | null = null;
    for (let j = 0; j < rows[regionRowIdx].length; j++) {
      const v = parseNumericCell(rows[regionRowIdx][j]);
      if (v !== null && v > 0) { valueCol = j; break; }
    }
    if (valueCol === null) continue;

    const regionVal = valueCol < rows[regionRowIdx].length
      ? parseNumericCell(rows[regionRowIdx][valueCol])
      : null;
    const totalVal = valueCol < rows[totalRowIdx].length
      ? parseNumericCell(rows[totalRowIdx][valueCol])
      : null;
    if (regionVal === null || totalVal === null || totalVal <= 0) continue;

    const pct = Math.round((regionVal / totalVal) * 10000) / 10000;
    const contextHtml = html.slice(Math.max(0, tbl.pos - 3_000), tbl.pos);
    const unitMult = detectUnitMultiplier(tableHtml, contextHtml);
    const usd = regionVal * unitMult;
    const denominator = totalVal * unitMult;
    const unitScale = unitMult === 1e3 ? "thousands" : unitMult === 1e6 ? "millions" : "actual";

    // Find nearest section heading
    const preHtml = html.slice(Math.max(0, tbl.pos - 6_000), tbl.pos);
    const hMatches = [...preHtml.matchAll(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/gi)];
    const sectionHeading = hMatches.length > 0
      ? stripHtmlTags(hMatches[hMatches.length - 1][1])
      : "";

    const headerRow = rows[0] ?? [];
    const sourceColumn = valueCol < headerRow.length ? String(headerRow[valueCol]).trim() : "";
    const rawValue = valueCol < rows[regionRowIdx].length ? String(rows[regionRowIdx][valueCol]) : null;
    const rawDenominator = valueCol < rows[totalRowIdx].length ? String(rows[totalRowIdx][valueCol]) : null;
    const sourceRows = [
      [String(rows[regionRowIdx][0] ?? region), rawValue ?? ""],
      [String(rows[totalRowIdx][0] ?? "Total revenue"), rawDenominator ?? ""],
    ];
    return {
      pct,
      usd,
      denominator,
      sectionHeading,
      parsedTables: [{ rows }],
      unitScale,
      rawValue,
      rawDenominator,
      sourceRows,
      sourceColumns: sourceColumn ? [sourceColumn] : [],
    };
  }

  return null;
}

function normalizeSegmentLabel(segment: unknown): string {
  if (segment == null) return "";
  if (Array.isArray(segment)) return segment.map((s) => normalizeSegmentLabel(s)).join(" ").trim();
  if (typeof segment === "object") return Object.values(segment as Record<string, unknown>).map(String).join(" ").trim();
  return String(segment).trim();
}

function regionMatches(segmentLabel: string, region: string, includeAsiaFallback = false): boolean {
  const label = segmentLabel.toLowerCase();
  const regionLower = region.toLowerCase();
  if (label.includes(regionLower)) return true;
  if (regionLower === "china") {
    if (["country:cn", "greater china", "srt:chinamember"].some((t) => label.includes(t))) return true;
    return includeAsiaFallback && label.includes("asiapacificmember");
  }
  return false;
}

function filingManualLookup(ticker: string, cikPadded: string | null, filingType: string): Record<string, unknown> {
  const edgarIndexUrl = cikPadded
    ? `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cikPadded}&type=${filingType}&owner=include&count=10`
    : `https://www.sec.gov/cgi-bin/browse-edgar?company=${encodeURIComponent(ticker)}&action=getcompany&type=${filingType}&owner=include&count=10`;
  const eftsSearchUrl = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(ticker)}&forms=${encodeURIComponent(filingType)}`;
  return {
    edgarIndexUrl,
    eftsSearchUrl,
    note: "Fact not XBRL-tagged. Use search_filing_text instead.",
  };
}

export async function getFilingData(
  ticker: string,
  factType: string,
  region: string | null = null,
  filingType = "10-K",
  period = "latest",
): Promise<string> {
  const FLOATING_POINT_EPSILON = 1e-9;
  const RATIO_SCALE = 10000;
  const PCT_SCALE = 100;
  const PCT_MULTIPLIER = 100;
  const ratioToPct = (ratio: number): number => Math.round(ratio * PCT_MULTIPLIER * PCT_SCALE) / PCT_SCALE;
  const formatRawNumber = (n: number | null | undefined): string | null => {
    if (n == null || !Number.isFinite(n)) return null;
    if (Math.abs(n - Math.round(n)) < FLOATING_POINT_EPSILON) return Math.round(n).toLocaleString("en-US");
    return n.toLocaleString("en-US", { maximumFractionDigits: 2, minimumFractionDigits: 2 });
  };
  const withGeoShape = (payload: Record<string, unknown>, addDenominatorWarning = false): string => {
    if (factType !== "geographic_revenue") return JSON.stringify(payload);
    const warnings = Array.isArray(payload.warnings) ? [...payload.warnings] : [];
    const hasDenominator = payload.denominator != null;
    const shaped: Record<string, unknown> = {
      ticker: payload.ticker ?? ticker,
      factType: payload.factType ?? "geographic_revenue",
      region: payload.region ?? region,
      period: payload.period ?? null,
      rawValue: payload.rawValue ?? null,
      rawDenominator: payload.rawDenominator ?? null,
      unit: payload.unit ?? "USD",
      unitScale: payload.unitScale ?? "actual",
      value: payload.value ?? null,
      denominator: payload.denominator ?? null,
      valueRatio: hasDenominator ? (payload.valueRatio ?? null) : null,
      valuePct: hasDenominator ? (payload.valuePct ?? null) : null,
      extractionMethod: payload.extractionMethod ?? "NONE",
      source: payload.source ?? "NOT_DISCLOSED",
      confidence: payload.confidence ?? "NOT_DISCLOSED",
      filingType: payload.filingType ?? filingType,
      filingDate: payload.filingDate ?? null,
      accessionNumber: payload.accessionNumber ?? null,
      documentUrl: payload.documentUrl ?? null,
      indexUrl: payload.indexUrl ?? null,
      primaryDocumentUrl: payload.primaryDocumentUrl ?? null,
      evidence: payload.evidence ?? {},
      calculation: payload.calculation ?? null,
      warnings,
    };
    if (addDenominatorWarning && shaped.value != null && shaped.denominator == null) {
      (shaped.warnings as unknown[]).push({
        code: "DENOMINATOR_NOT_FOUND",
        message: "Could not compute geographic revenue percentage due to missing denominator.",
        severity: "warning",
      });
    }
    return JSON.stringify(shaped);
  };

  const config = FILING_FACT_CONCEPTS[factType];
  if (!config) return JSON.stringify({ error: true, message: `Unsupported fact_type '${factType}'`, ticker });
  if (factType === "geographic_revenue" && !region) {
    return JSON.stringify({ error: true, message: "region is required for fact_type='geographic_revenue'", ticker });
  }

  const cikPadded = await resolveCikForTicker(ticker);
  if (!cikPadded) {
    return withGeoShape({
      ticker,
      factType,
      value: null,
      denominator: null,
      valueRatio: null,
      valuePct: null,
      extractionMethod: "NONE",
      source: "NOT_DISCLOSED",
      confidence: "NOT_DISCLOSED",
      evidence: {},
      warnings: [],
      _manualLookup: filingManualLookup(ticker, null, filingType),
    });
  }

  const fetchConcept = async (concept: string): Promise<Record<string, unknown> | null> =>
    edgarGetJson(`https://data.sec.gov/api/xbrl/companyconcept/CIK${cikPadded}/us-gaap/${concept}.json`);

  let concept = config.primary;
  let conceptData = await fetchConcept(config.primary);
  let facts = (((conceptData?.units as Record<string, unknown>)?.USD as Record<string, unknown>[]) ?? []);
  if (!facts.length && config.fallback) {
    const fb = await fetchConcept(config.fallback);
    const fbFacts = (((fb?.units as Record<string, unknown>)?.USD as Record<string, unknown>[]) ?? []);
    if (fbFacts.length) {
      concept = config.fallback;
      conceptData = fb;
      facts = fbFacts;
    }
  }

  let filtered = facts.filter((f) => String(f.form ?? "").toUpperCase() === filingType.toUpperCase());
  if (!filtered.length) {
    return withGeoShape({
      ticker,
      factType,
      value: null,
      denominator: null,
      valueRatio: null,
      valuePct: null,
      extractionMethod: "NONE",
      source: "NOT_DISCLOSED",
      confidence: "NOT_DISCLOSED",
      evidence: {},
      warnings: [],
      _manualLookup: filingManualLookup(ticker, cikPadded, filingType),
    });
  }
  if (period === "latest") {
    const latestFiled = filtered.map((f) => String(f.filed ?? "")).sort().slice(-1)[0];
    filtered = filtered.filter((f) => String(f.filed ?? "") === latestFiled);
  }

  if (factType === "segment_revenue") {
    const allSegments = filtered
      .map((f) => {
        const segmentLabel = normalizeSegmentLabel(f.segment);
        if (!segmentLabel) return null;
        return {
          segmentLabel,
          value: f.val ?? null,
          fiscalYear: String(f.fy ?? ""),
          fiscalPeriod: String(f.fp ?? ""),
          filingDate: String(f.filed ?? ""),
          accessionNumber: String(f.accn ?? ""),
        };
      })
      .filter((v) => v != null);
    return JSON.stringify({
      ticker,
      factType,
      concept,
      value: allSegments[0]?.value ?? null,
      fiscalYear: allSegments[0]?.fiscalYear ?? null,
      fiscalPeriod: allSegments[0]?.fiscalPeriod ?? null,
      filingType,
      filingDate: allSegments[0]?.filingDate ?? null,
      accessionNumber: allSegments[0]?.accessionNumber ?? null,
      extractionMethod: "XBRL",
      source: "XBRL",
      confidence: "HIGH",
      allSegments,
    });
  }

  let picked: Record<string, unknown> | null = null;
  let valueRatio: number | null = null;
  let valuePct: number | null = null;
  let denominator: number | null = null;
  let segmentLabel: string | null = null;
  if (factType === "geographic_revenue") {
    picked = filtered.find((f) => {
      const label = normalizeSegmentLabel(f.segment);
      if (!label) return false;
      if (regionMatches(label, region ?? "", false)) {
        segmentLabel = label;
        return true;
      }
      return false;
    }) ?? null;
    if (!picked && (region ?? "").toLowerCase() === "china") {
      picked = filtered.find((f) => {
        const label = normalizeSegmentLabel(f.segment);
        if (!label) return false;
        if (regionMatches(label, region ?? "", true)) {
          segmentLabel = label;
          return true;
        }
        return false;
      }) ?? null;
    }
    if (picked) {
      const accn = String(picked.accn ?? "");
      const total = filtered.find((f) => String(f.accn ?? "") === accn && f.segment == null) ?? null;
      const totalVal = total ? Number(total.val ?? 0) : 0;
      const partVal = picked.val != null ? Number(picked.val) : null;
      if (partVal != null && totalVal > 0) {
        denominator = totalVal;
        valueRatio = Math.round((partVal / totalVal) * RATIO_SCALE) / RATIO_SCALE;
        valuePct = ratioToPct(valueRatio);
      }
    }
  } else {
    picked = filtered.find((f) => f.segment == null) ?? filtered[0] ?? null;
  }

  if (!picked) {
    // ── HTML fallback for geographic_revenue ────────────────────────────────
    // Some companies (e.g. GLW) do not XBRL-tag geographic-revenue segments.
    // Fall through to the same HTML-parsing path used by searchFilingText.
    if (factType === "geographic_revenue") {
      const { cikPadded: subsCik, submissions } = await getSubmissionsForTicker(ticker);
      if (subsCik && submissions) {
        const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
        const sforms = (recent.form as string[]) ?? [];
        const saccessions = (recent.accessionNumber as string[]) ?? [];
        const sprimaryDocs = (recent.primaryDocument as string[]) ?? [];
        const sfilingDates = (recent.filingDate as string[]) ?? [];
        const sreportDates = (recent.reportDate as string[]) ?? [];
        const idx = sforms.findIndex((f) => String(f).toUpperCase() === filingType.toUpperCase());
        if (idx >= 0) {
          const primaryDoc = sprimaryDocs[idx] ?? null;
          if (primaryDoc) {
            const cikInt = parseInt(subsCik, 10);
            const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(cikInt, saccessions[idx], primaryDoc);
            if (edgarPrimaryDocumentUrl) {
              const htmlText = await edgarGetHtml(edgarPrimaryDocumentUrl, 5_000_000);
              if (htmlText) {
                const geo = extractGeoRevenueFromHtml(htmlText, region ?? "");
                if (geo) {
                  const reportDate = sreportDates[idx] ?? "";
                  const fiscalYear = reportDate ? `FY${String(reportDate).slice(0, 4)}` : "";
                  const warnings = geo.denominator == null && geo.usd != null
                    ? [{
                        code: "DENOMINATOR_NOT_FOUND",
                        message: "Could not compute geographic revenue percentage due to missing denominator.",
                        severity: "warning",
                      }]
                    : [];
                  return withGeoShape({
                    ticker,
                    factType,
                    region,
                    period: fiscalYear || null,
                    rawValue: geo.rawValue ?? formatRawNumber(geo.usd ?? null),
                    rawDenominator: geo.rawDenominator ?? formatRawNumber(geo.denominator ?? null),
                    unit: "USD",
                    unitScale: geo.unitScale,
                    value: geo.usd ?? null,
                    denominator: geo.denominator ?? null,
                    valueRatio: geo.pct,
                    valuePct: geo.denominator != null ? ratioToPct(geo.pct) : null,
                    extractionMethod: "PARSED_TABLE",
                    source: "PARSED_TABLE",
                    confidence: geo.denominator != null ? "HIGH" : "LOW",
                    filingType,
                    filingDate: sfilingDates[idx] ?? null,
                    accessionNumber: saccessions[idx] ?? null,
                    documentUrl: edgarPrimaryDocumentUrl,
                    indexUrl: null,
                    primaryDocumentUrl: edgarPrimaryDocumentUrl,
                    evidence: {
                      sectionHeading: geo.sectionHeading || null,
                      tableTitle: null,
                      sourceTableId: 1,
                      sourceRows: geo.sourceRows,
                      sourceColumns: geo.sourceColumns.length > 0 ? geo.sourceColumns : [fiscalYear],
                    },
                    calculation: geo.denominator != null
                      ? {
                          formula: "value / denominator * 100",
                          valueSource: "sourceRows[0]",
                          denominatorSource: "sourceRows[1]",
                          resultPct: ratioToPct(geo.pct),
                        }
                      : null,
                    warnings,
                  });
                }
              }
            }
          }
        }
      }
    }
    return withGeoShape({
      ticker,
      factType,
      value: null,
      denominator: null,
      valueRatio: null,
      valuePct: null,
      extractionMethod: "NONE",
      source: "NOT_DISCLOSED",
      confidence: "NOT_DISCLOSED",
      evidence: {},
      warnings: [],
      _manualLookup: filingManualLookup(ticker, cikPadded, filingType),
    });
  }

  const accessionNumber = String(picked.accn ?? "") || null;
  let indexUrl: string | null = null;
  let primaryDocumentUrl: string | null = null;
  if (accessionNumber) {
    const baseUrls = edgarBuildFilingUrls(parseInt(cikPadded, 10), accessionNumber, null);
    indexUrl = baseUrls.edgarIndexUrl;
    const { submissions } = await getSubmissionsForTicker(ticker);
    const recent = (((submissions?.filings as Record<string, unknown>)?.recent) as Record<string, unknown[]> | undefined) ?? {};
    const accessions = (recent.accessionNumber as string[]) ?? [];
    const primaryDocs = (recent.primaryDocument as string[]) ?? [];
    const idx = accessions.findIndex((a) => String(a) === accessionNumber);
    const primaryDoc = idx >= 0 && idx < primaryDocs.length ? primaryDocs[idx] : null;
    if (primaryDoc) {
      primaryDocumentUrl = edgarBuildFilingUrls(parseInt(cikPadded, 10), accessionNumber, primaryDoc).edgarPrimaryDocumentUrl;
    }
  }
  const documentUrl = primaryDocumentUrl ?? indexUrl;
  const periodLabelRaw = String(picked.fy ?? "");
  const periodLabel = periodLabelRaw && !periodLabelRaw.startsWith("FY") ? `FY${periodLabelRaw}` : periodLabelRaw;
  const valueNum = picked.val != null ? Number(picked.val) : null;
  const rawValue = formatRawNumber(valueNum);
  const rawDenominator = formatRawNumber(denominator);

  return withGeoShape({
    ticker,
    factType,
    region,
    period: periodLabel || null,
    rawValue,
    rawDenominator,
    unit: "USD",
    unitScale: "actual",
    value: valueNum,
    denominator: factType === "geographic_revenue" ? denominator : null,
    valueRatio: factType === "geographic_revenue" ? valueRatio : null,
    valuePct: factType === "geographic_revenue" ? valuePct : null,
    extractionMethod: "XBRL",
    source: "XBRL",
    confidence: factType !== "geographic_revenue" || denominator != null ? "HIGH" : "LOW",
    filingType,
    filingDate: String(picked.filed ?? ""),
    accessionNumber,
    documentUrl,
    indexUrl,
    primaryDocumentUrl,
    evidence: {
      sectionHeading: segmentLabel,
      tableTitle: null,
      sourceTableId: null,
      sourceRows: [
        [segmentLabel ?? (region ?? "Region"), rawValue ?? ""],
        ["Total revenue", rawDenominator ?? ""],
      ],
      sourceColumns: [periodLabel || String(picked.fp ?? "")],
    },
    calculation: factType === "geographic_revenue" && denominator != null
      ? {
          formula: "value / denominator * 100",
          valueSource: "sourceRows[0]",
          denominatorSource: "sourceRows[1]",
          resultPct: valuePct,
        }
      : null,
    warnings: [],
  }, factType === "geographic_revenue" && denominator == null);
}

export async function searchFilingText(
  ticker: string,
  searchTerms: string[] = [],
  sectionHint: string | null = null,
  filingType = "10-K",
  accessionNumber: string | null = null,
  contextChars = 1500,
  returnTables = true,
): Promise<string> {
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: null,
      fiscalYear: null,
      filingType,
      filingDate: null,
      matches: [],
      matchCount: 0,
      confidence: "PARSED_HTML",
      _note: "Could not resolve SEC submissions for ticker.",
    });
  }

  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form as string[]) ?? [];
  const accessions = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs = (recent.primaryDocument as string[]) ?? [];
  const filingDates = (recent.filingDate as string[]) ?? [];
  const reportDates = (recent.reportDate as string[]) ?? [];

  let targetIndex = -1;
  if (accessionNumber) {
    targetIndex = accessions.findIndex((a) => a === accessionNumber);
  } else {
    targetIndex = forms.findIndex((f) => String(f).toUpperCase() === filingType.toUpperCase());
    if (targetIndex >= 0) accessionNumber = accessions[targetIndex] ?? null;
  }
  if (targetIndex < 0 || !accessionNumber) {
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: null,
      fiscalYear: null,
      filingType,
      filingDate: null,
      matches: [],
      matchCount: 0,
      confidence: "PARSED_HTML",
      _note: `No ${filingType} filing found in submissions JSON.`,
    });
  }

  const primaryDocument = primaryDocs[targetIndex] ?? null;
  if (!primaryDocument) {
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: null,
      fiscalYear: null,
      filingType,
      filingDate: filingDates[targetIndex] ?? null,
      matches: [],
      matchCount: 0,
      confidence: "PARSED_HTML",
      _note: "primaryDocument missing in submissions JSON.",
    });
  }

  const cik = parseInt(cikPadded, 10);
  const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(cik, accessionNumber, primaryDocument);
  if (!edgarPrimaryDocumentUrl) {
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: null,
      fiscalYear: null,
      filingType,
      filingDate: filingDates[targetIndex] ?? null,
      matches: [],
      matchCount: 0,
      confidence: "PARSED_HTML",
      _note: "Failed constructing filing document URL.",
    });
  }

  const html = await edgarGetHtml(edgarPrimaryDocumentUrl, 5_000_000);
  if (!html) {
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: edgarPrimaryDocumentUrl,
      fiscalYear: reportDates[targetIndex] ? `FY${String(reportDates[targetIndex]).slice(0, 4)}` : null,
      filingType,
      filingDate: filingDates[targetIndex] ?? null,
      matches: [],
      matchCount: 0,
      confidence: "PARSED_HTML",
      _note: "Unable to fetch filing HTML.",
    });
  }

  const cleanedHtml = sanitizeFilingHtml(html);
  const htmlLower = cleanedHtml.toLowerCase();
  const size = Math.max(200, Math.min(Math.floor(contextChars), 4000));
  const matches: Record<string, unknown>[] = [];
  const seen = new Set<number>();

  const addMatch = (term: string, pos: number) => {
    if ([...seen].some((p) => Math.abs(p - pos) < 150)) return;
    seen.add(pos);
    const start = Math.max(0, pos - Math.floor(size / 2));
    const end = Math.min(cleanedHtml.length, pos + Math.floor(size / 2));
    const contextHtml = cleanedHtml.slice(start, end);
    const preHtml = cleanedHtml.slice(Math.max(0, pos - 8_000), pos);
    const headingMatches = [...preHtml.matchAll(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/gi)];
    const sectionHeading = headingMatches.length ? stripHtmlTags(headingMatches[headingMatches.length - 1][1]) : "";
    const match: Record<string, unknown> = {
      term,
      sectionHeading,
      contextText: stripHtmlTags(contextHtml),
      confidence: "LOW",
    };
    if (returnTables) {
      const tableParsed: Record<string, unknown>[] = [];
      const tableWindow = cleanedHtml.slice(Math.max(0, pos - 12_000), Math.min(cleanedHtml.length, pos + 12_000));
      for (const m of tableWindow.matchAll(/<table[^>]*>([\s\S]*?)<\/table>/gi)) {
        const rows = parseHtmlTable(m[0]);
        if (rows.length >= 2 && rows.some(r => r.length > 0 && r.some(c => c.length > 0))) {
          tableParsed.push({ rows });
        }
        if (tableParsed.length >= 3) break;
      }
      match.tableParsed = tableParsed;
      if (tableParsed.length > 0) {
        match.confidence = "HIGH";
      } else if ((match.contextText as string).length > 0) {
        match.confidence = "MEDIUM";
      }
    }
    matches.push(match);
  };

  if (sectionHint) {
    const pos = htmlLower.indexOf(sectionHint.toLowerCase());
    if (pos >= 0) addMatch(sectionHint, pos);
  }
  for (const term of searchTerms) {
    let idx = 0;
    const termLower = term.toLowerCase();
    while (matches.length < 10) {
      const pos = htmlLower.indexOf(termLower, idx);
      if (pos < 0) break;
      addMatch(term, pos);
      idx = pos + 1;
    }
  }

  return JSON.stringify({
    ticker,
    accessionNumber,
    documentUrl: edgarPrimaryDocumentUrl,
    fiscalYear: reportDates[targetIndex] ? `FY${String(reportDates[targetIndex]).slice(0, 4)}` : null,
    filingType,
    filingDate: filingDates[targetIndex] ?? null,
    matches,
    matchCount: matches.length,
    confidence: matches.length === 0 ? "NOT_DISCLOSED" : (matches.some(m => ((m.tableParsed as unknown[]) ?? []).length > 0) ? "HIGH" : "MEDIUM"),
    warnings: matches.length > 0 ? [{
      code: "RAW_FILING_TEXT",
      message: "Returned text is sanitized filing context, not structured fact extraction.",
      severity: "info",
    }] : [],
  });
}

export async function getGeographicRevenue(ticker: string, region: string = "China"): Promise<string> {
  // ── Confirmed lookup table for China (fast path, no network call) ─────────
  if (region.toLowerCase() === "china") {
    const confirmedEntry = CHINA_REVENUE_CONFIRMED[ticker.toUpperCase()];
    if (confirmedEntry) {
      return JSON.stringify({
        ticker,
        region,
        regionRevenuePct: confirmedEntry.pct,
        regionRevenueUSD: null,
        fiscalYear: confirmedEntry.fiscalYear,
        filingType: "10-K",
        filingDate: confirmedEntry.filingDate,
        segmentLabel: "China",
        source: "confirmed_lookup_table",
        confidence: "CONFIRMED",
        sectionHeading: null,
        primaryDocumentUrl: null,
        edgarError: null,
        _manualLookup: null,
      });
    }
  }

  const edgarErrors: string[] = [];

  // ── Step 1: Resolve CIK and latest 10-K filing metadata ──────────────────
  let cik: number | null = null;
  let filingDate: string | null = null;
  let fiscalYear: string | null = null;
  let primaryDocumentUrl: string | null = null;

  try {
    cik = await edgarResolveCik(ticker);
  } catch (e) {
    edgarErrors.push(`tickers_fetch: ${e instanceof Error ? e.message : String(e)}`);
  }

  if (cik != null) {
    try {
      const info = await edgarGetLatest10K(cik);
      if (info) {
        filingDate = info.filingDate;
        fiscalYear = info.fiscalYear;
        primaryDocumentUrl = info.edgarPrimaryDocumentUrl;
      }
    } catch (e) {
      edgarErrors.push(`submissions_fetch: ${e instanceof Error ? e.message : String(e)}`);
    }
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
        { headers: { "User-Agent": EDGAR_UA } },
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
    } catch (e) {
      edgarErrors.push(`xbrl_fetch: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  // ── Step 3 (fallback): Parse primary 10-K HTML when XBRL is NOT_DISCLOSED ─
  let htmlSectionHeading: string | null = null;
  if (confidence === "NOT_DISCLOSED" && primaryDocumentUrl != null) {
    try {
      const htmlText = await edgarGetHtml(primaryDocumentUrl);
      if (htmlText) {
        const parsed = extractGeoRevenueFromHtml(htmlText, region);
        if (parsed != null) {
          regionRevenuePct = parsed.pct;
          regionRevenueUSD = parsed.usd;
          htmlSectionHeading = parsed.sectionHeading || null;
          source = "edgar_html";
          confidence = "PARSED_HTML";
        }
      }
    } catch (e) {
      edgarErrors.push(`html_fallback: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  // Promote confidence to FETCH_ERROR when a network/parse failure prevented
  // a definitive lookup — distinguishes infrastructure failures from genuine
  // non-disclosures so downstream DC-151 logic can treat it as indeterminate.
  const edgarError = edgarErrors.length > 0 ? edgarErrors.join("; ") : null;
  if (edgarError != null && confidence === "NOT_DISCLOSED") {
    confidence = "FETCH_ERROR";
  }

  // ── Manual-lookup pointer when neither XBRL nor HTML extraction succeeded ─
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
    sectionHeading: htmlSectionHeading,
    primaryDocumentUrl,
    edgarError,
    _manualLookup: manualLookup,
  });
}

// ── get_filing_text_search ────────────────────────────────────────────────────

type FilingTextSearchMatch = {
  term: string;
  sectionHeading: string | null;
  contextText: string;
  tableParsed: unknown[] | null;
};

function filingTextOnlyMatches(text: string, searchTerms: string[], contextChars: number): FilingTextSearchMatch[] {
  const textLower = text.toLowerCase();
  const matches: FilingTextSearchMatch[] = [];
  const seenPositions = new Set<number>();
  const half = Math.floor(contextChars / 2);
  for (const term of searchTerms) {
    const termLower = term.toLowerCase();
    let idx = 0;
    while (matches.length < 5) {
      const pos = textLower.indexOf(termLower, idx);
      if (pos === -1) break;
      idx = pos + 1;
      if ([...seenPositions].some(sp => Math.abs(pos - sp) < 200)) continue;
      seenPositions.add(pos);
      const ctxStart = Math.max(0, pos - half);
      const ctxEnd = Math.min(text.length, pos + half);
      matches.push({
        term,
        sectionHeading: null,
        contextText: text.slice(ctxStart, ctxEnd).trim(),
        tableParsed: null,
      });
    }
  }
  return matches;
}

export async function getFilingTextSearch(
  ticker: string,
  accessionNumber: string,
  searchTerms: string[],
  contextChars: number = 1500,
  returnTables: boolean = true,
  textOnly: boolean = false,
  documentUrl: string | null = null,
): Promise<string> {
  // Resolve primary document URL from EDGAR submissions
  // When the caller already provides the resolved document URL (e.g. from
  // get_sec_filings edgarPrimaryDocumentUrl), skip all EDGAR resolution calls
  // and use it directly.  This is the fast path that avoids EDGAR API failures.
  let primaryDocUrl: string | null = documentUrl;
  let fiscalYear: string | null = null;
  let fallbackIndexUrl: string | null = null;
  if (!primaryDocUrl) {
  try {
    const cik = await edgarResolveCik(ticker);
    if (cik != null) {
      const cikPadded = String(cik).padStart(10, "0");
      const subs = await edgarGetJson(`https://data.sec.gov/submissions/CIK${cikPadded}.json`);
      if (subs) {
        const recent = ((subs.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
        const accessions = (recent.accessionNumber as string[]) ?? [];
        const primaryDocs = (recent.primaryDocument as string[]) ?? [];
        const periods = (recent.reportDate as string[]) ?? [];
        for (let i = 0; i < accessions.length; i++) {
          if (accessions[i] === accessionNumber) {
            const period = periods[i];
            if (period) fiscalYear = `FY${period.slice(0, 4)}`;
            const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(cik, accessions[i], primaryDocs[i] ?? null);
            primaryDocUrl = edgarPrimaryDocumentUrl;
            break;
          }
        }
      }
    }
  } catch { /* non-fatal */ }

  // Fallback: derive CIK from the accession number prefix when ticker→CIK lookup failed
  // or when the accession is not present in the most-recent submissions window.
  if (!primaryDocUrl) {
    const derivedCik = edgarCikFromAccession(accessionNumber);
    if (derivedCik != null) {
      try {
        const cikPadded = String(derivedCik).padStart(10, "0");
        const subs = await edgarGetJson(`https://data.sec.gov/submissions/CIK${cikPadded}.json`);
        if (subs) {
          const recent = ((subs.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
          const accessions = (recent.accessionNumber as string[]) ?? [];
          const primaryDocs = (recent.primaryDocument as string[]) ?? [];
          const periods = (recent.reportDate as string[]) ?? [];
          for (let i = 0; i < accessions.length; i++) {
            if (accessions[i] === accessionNumber) {
              const period = periods[i];
              if (period && !fiscalYear) fiscalYear = `FY${period.slice(0, 4)}`;
              const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(derivedCik, accessions[i], primaryDocs[i] ?? null);
              primaryDocUrl = edgarPrimaryDocumentUrl;
              break;
            }
          }
        }
      } catch { /* non-fatal */ }
    }
  }

  // Third fallback: derive CIK directly from the accession number and parse the primary
  // document from the EDGAR filing index HTM. This approach is ticker-agnostic and works
  // even when the filing falls outside the EDGAR submissions window or when the submissions
  // JSON has an empty/missing primaryDocument field.
  if (!primaryDocUrl) {
    const fbCik = edgarCikFromAccession(accessionNumber);
    if (fbCik != null) {
      try {
        const { edgarIndexUrl } = edgarBuildFilingUrls(fbCik, accessionNumber, null);
        fallbackIndexUrl = edgarIndexUrl;
        const pdocFname = await edgarPrimaryDocFromIndex(edgarIndexUrl);
        if (pdocFname) {
          const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(fbCik, accessionNumber, pdocFname);
          primaryDocUrl = edgarPrimaryDocumentUrl;
        }
      } catch { /* non-fatal */ }
    }
  }
  } // end if (!primaryDocUrl) — EDGAR resolution block

  if (!primaryDocUrl) {
    const indexHtml = fallbackIndexUrl ? await edgarGetHtml(fallbackIndexUrl, 500_000) : null;
    const indexText = indexHtml ? stripHtmlTags(indexHtml) : null;
    const fallbackMatches = indexText
      ? filingTextOnlyMatches(indexText, searchTerms, Math.max(contextChars, 1200))
      : [];
    return JSON.stringify({
      ticker,
      accessionNumber,
      filingUrl: null,
      indexUrl: fallbackIndexUrl,
      fiscalYear,
      searchTerms,
      matches: fallbackMatches,
      matchCount: fallbackMatches.length,
      searchMode: "index_text_fallback",
      _note: `Primary document URL could not be resolved for accession '${accessionNumber}'. Returned text-only fallback from filing index page when available.`,
    });
  }

  const html = await edgarGetHtml(primaryDocUrl);
  if (!html) {
    const indexHtml = fallbackIndexUrl ? await edgarGetHtml(fallbackIndexUrl, 500_000) : null;
    const indexText = indexHtml ? stripHtmlTags(indexHtml) : null;
    const fallbackMatches = indexText
      ? filingTextOnlyMatches(indexText, searchTerms, Math.max(contextChars, 1200))
      : [];
    return JSON.stringify({
      ticker,
      accessionNumber,
      filingUrl: primaryDocUrl,
      indexUrl: fallbackIndexUrl,
      fiscalYear,
      searchTerms,
      matches: fallbackMatches,
      matchCount: fallbackMatches.length,
      searchMode: "index_text_fallback",
      _note: `Could not fetch filing document from ${primaryDocUrl}. Returned text-only fallback from filing index page when available.`,
    });
  }

  if (textOnly) {
    const plainText = stripHtmlTags(html);
    const matches = filingTextOnlyMatches(plainText, searchTerms, contextChars);
    return JSON.stringify({
      ticker,
      accessionNumber,
      filingUrl: primaryDocUrl,
      indexUrl: fallbackIndexUrl,
      fiscalYear,
      searchTerms,
      matches,
      matchCount: matches.length,
      searchMode: "text_only",
      _note: "Keyword search executed over stripped filing text (table parsing disabled).",
    });
  }

  const htmlLower = html.toLowerCase();
  const matches: unknown[] = [];
  const seenPositions = new Set<number>();
  const half = Math.floor(contextChars / 2);

  for (const term of searchTerms) {
    const termLower = term.toLowerCase();
    let idx = 0;
    while (matches.length < 5) {
      const pos = htmlLower.indexOf(termLower, idx);
      if (pos === -1) break;
      idx = pos + 1;

      // Deduplicate nearby matches
      if ([...seenPositions].some(sp => Math.abs(pos - sp) < 200)) continue;
      seenPositions.add(pos);

      const ctxStart = Math.max(0, pos - half);
      const ctxEnd = Math.min(html.length, pos + half);
      const contextText = stripHtmlTags(html.slice(ctxStart, ctxEnd));

      // Nearest section heading
      const preHtml = html.slice(Math.max(0, pos - 8_000), pos);
      const hMatches = [...preHtml.matchAll(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/gi)];
      const sectionHeading = hMatches.length > 0
        ? stripHtmlTags(hMatches[hMatches.length - 1][1])
        : null;

      // Parse nearby tables
      const tableParsed: unknown[] = [];
      if (returnTables) {
        const tblSearch = html.slice(Math.max(0, pos - 2_000), Math.min(html.length, pos + 60_000));
        const tblRe = /<table[^>]*>/gi;
        let tblM: RegExpExecArray | null;
        while ((tblM = tblRe.exec(tblSearch)) !== null && tableParsed.length < 3) {
          const absStart = Math.max(0, pos - 2_000) + tblM.index;
          let depth = 0;
          let i = absStart;
          let tableEnd = absStart;
          while (i < Math.min(html.length, absStart + 200_000)) {
            const o = htmlLower.indexOf("<table", i);
            const c = htmlLower.indexOf("</table>", i);
            if (o === -1 && c === -1) break;
            if (o !== -1 && (c === -1 || o < c)) { depth++; i = o + 6; }
            else { depth--; if (depth === 0) { tableEnd = c + 8; break; } i = c + 8; }
          }
          const rows = parseHtmlTable(html.slice(absStart, tableEnd));
          if (rows.length >= 2) tableParsed.push({ rows });
        }
      }

      matches.push({ term, sectionHeading, contextText, tableParsed: returnTables ? tableParsed : null });
    }
  }

  return JSON.stringify({
    ticker,
    accessionNumber,
    filingUrl: primaryDocUrl,
    indexUrl: fallbackIndexUrl,
    fiscalYear,
    searchTerms,
    matches,
    matchCount: matches.length,
    searchMode: "html_context",
  });
}

// ── get_filing_document ───────────────────────────────────────────────────────

export async function getFilingDocument(
  ticker: string,
  accessionNumber: string,
  sectionHint: string | null = null,
  filingType: string = "10-K",
  documentUrl: string | null = null,
): Promise<string> {
  // When the caller already provides the resolved document URL (e.g. from
  // get_sec_filings edgarPrimaryDocumentUrl), skip all EDGAR resolution calls.
  let primaryDocUrl: string | null = documentUrl;
  let fiscalYear: string | null = null;
  if (!primaryDocUrl) {
  try {
    const cik = await edgarResolveCik(ticker);
    if (cik != null) {
      const cikPadded = String(cik).padStart(10, "0");
      const subs = await edgarGetJson(`https://data.sec.gov/submissions/CIK${cikPadded}.json`);
      if (subs) {
        const recent = ((subs.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
        const accessions = (recent.accessionNumber as string[]) ?? [];
        const primaryDocs = (recent.primaryDocument as string[]) ?? [];
        const periods = (recent.reportDate as string[]) ?? [];
        for (let i = 0; i < accessions.length; i++) {
          if (accessions[i] === accessionNumber) {
            const period = periods[i];
            if (period) fiscalYear = `FY${period.slice(0, 4)}`;
            const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(cik, accessions[i], primaryDocs[i] ?? null);
            primaryDocUrl = edgarPrimaryDocumentUrl;
            break;
          }
        }
      }
    }
  } catch { /* non-fatal */ }

  // Fallback: derive CIK from the accession number prefix when ticker→CIK lookup failed
  // or when the accession is not present in the most-recent submissions window.
  if (!primaryDocUrl) {
    const derivedCik = edgarCikFromAccession(accessionNumber);
    if (derivedCik != null) {
      try {
        const cikPadded = String(derivedCik).padStart(10, "0");
        const subs = await edgarGetJson(`https://data.sec.gov/submissions/CIK${cikPadded}.json`);
        if (subs) {
          const recent = ((subs.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
          const accessions = (recent.accessionNumber as string[]) ?? [];
          const primaryDocs = (recent.primaryDocument as string[]) ?? [];
          const periods = (recent.reportDate as string[]) ?? [];
          for (let i = 0; i < accessions.length; i++) {
            if (accessions[i] === accessionNumber) {
              const period = periods[i];
              if (period && !fiscalYear) fiscalYear = `FY${period.slice(0, 4)}`;
              const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(derivedCik, accessions[i], primaryDocs[i] ?? null);
              primaryDocUrl = edgarPrimaryDocumentUrl;
              break;
            }
          }
        }
      } catch { /* non-fatal */ }
    }
  }

  // Third fallback: derive CIK directly from the accession number and parse the primary
  // document from the EDGAR filing index HTM. This approach is ticker-agnostic and works
  // even when the filing falls outside the EDGAR submissions window or when the submissions
  // JSON has an empty/missing primaryDocument field.
  if (!primaryDocUrl) {
    const fbCik = edgarCikFromAccession(accessionNumber);
    if (fbCik != null) {
      try {
        const { edgarIndexUrl } = edgarBuildFilingUrls(fbCik, accessionNumber, null);
        const pdocFname = await edgarPrimaryDocFromIndex(edgarIndexUrl);
        if (pdocFname) {
          const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(fbCik, accessionNumber, pdocFname);
          primaryDocUrl = edgarPrimaryDocumentUrl;
        }
      } catch { /* non-fatal */ }
    }
  }
  } // end if (!primaryDocUrl) — EDGAR resolution block

  if (!primaryDocUrl) {
    const fbCik = edgarCikFromAccession(accessionNumber);
    const indexUrl = fbCik != null
      ? edgarBuildFilingUrls(fbCik, accessionNumber, null).edgarIndexUrl
      : null;
    const indexHtml = indexUrl ? await edgarGetHtml(indexUrl, 500_000) : null;
    const indexText = indexHtml ? stripHtmlTags(indexHtml) : null;
    const fallbackContent = sectionHint && indexText
      ? filingTextOnlyMatches(indexText, [sectionHint], 5_000)[0]?.contextText ?? null
      : null;
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: null,
      indexUrl,
      fiscalYear,
      filingType,
      sectionsFound: [],
      sectionContent: fallbackContent,
      tablesInSection: null,
      _note: `Primary document URL could not be resolved for accession '${accessionNumber}'. Returned text-only fallback from filing index page when available.`,
    });
  }

  const html = await edgarGetHtml(primaryDocUrl);
  if (!html) {
    const fbCik = edgarCikFromAccession(accessionNumber);
    const indexUrl = fbCik != null
      ? edgarBuildFilingUrls(fbCik, accessionNumber, null).edgarIndexUrl
      : null;
    const indexHtml = indexUrl ? await edgarGetHtml(indexUrl, 500_000) : null;
    const indexText = indexHtml ? stripHtmlTags(indexHtml) : null;
    const fallbackContent = sectionHint && indexText
      ? filingTextOnlyMatches(indexText, [sectionHint], 5_000)[0]?.contextText ?? null
      : null;
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: primaryDocUrl,
      indexUrl,
      fiscalYear,
      filingType,
      sectionsFound: [],
      sectionContent: fallbackContent,
      tablesInSection: null,
      _note: `Could not fetch filing document from ${primaryDocUrl}. Returned text-only fallback from filing index page when available.`,
    });
  }

  // Extract section headings
  const sectionsFound = [...html.matchAll(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/gi)]
    .map(m => stripHtmlTags(m[1]))
    .filter(h => h.length > 0);

  if (!sectionHint) {
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: primaryDocUrl,
      fiscalYear,
      filingType,
      sectionsFound,
      sectionContent: null,
      tablesInSection: null,
      _note: "Provide section_hint to retrieve specific section content.",
    });
  }

  const hintLower = sectionHint.toLowerCase();
  const htmlLower = html.toLowerCase();
  let matchPos: number | null = null;

  // Try heading tags first
  for (const m of html.matchAll(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/gi)) {
    if (stripHtmlTags(m[1]).toLowerCase().includes(hintLower)) {
      matchPos = m.index ?? null;
      break;
    }
  }

  // Fallback: any occurrence
  if (matchPos === null) {
    const p = htmlLower.indexOf(hintLower);
    if (p !== -1) matchPos = p;
  }

  if (matchPos === null) {
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: primaryDocUrl,
      fiscalYear,
      filingType,
      sectionsFound,
      sectionContent: null,
      tablesInSection: null,
      _note: `Section hint '${sectionHint}' not found in the filing document.`,
    });
  }

  const sectionContent = stripHtmlTags(html.slice(matchPos, Math.min(html.length, matchPos + 5_000)));

  const tablesInSection: unknown[] = [];
  const wideHtml = html.slice(matchPos, Math.min(html.length, matchPos + 60_000));
  const tblRe = /<table[^>]*>/gi;
  let tblM: RegExpExecArray | null;
  while ((tblM = tblRe.exec(wideHtml)) !== null && tablesInSection.length < 5) {
    const absStart = matchPos + tblM.index;
    let depth = 0;
    let i = absStart;
    let tableEnd = absStart;
    while (i < Math.min(html.length, absStart + 200_000)) {
      const o = htmlLower.indexOf("<table", i);
      const c = htmlLower.indexOf("</table>", i);
      if (o === -1 && c === -1) break;
      if (o !== -1 && (c === -1 || o < c)) { depth++; i = o + 6; }
      else { depth--; if (depth === 0) { tableEnd = c + 8; break; } i = c + 8; }
    }
    const rows = parseHtmlTable(html.slice(absStart, tableEnd));
    if (rows.length >= 2) tablesInSection.push({ rows });
  }

  return JSON.stringify({
    ticker,
    accessionNumber,
    documentUrl: primaryDocUrl,
    fiscalYear,
    filingType,
    sectionsFound,
    sectionContent,
    tablesInSection,
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

    // Total OI guard for max pain
    const totalCallOI = calls.reduce((s, c) => s + ((c.openInterest as number) || 0), 0);
    const totalPutOI = puts.reduce((s, p) => s + ((p.openInterest as number) || 0), 0);

    let maxPainStrike: number | null = null;
    const scanWarnings: string[] = [];
    if (totalCallOI + totalPutOI <= 0) {
      scanWarnings.push("MAX_PAIN_UNAVAILABLE_ZERO_OI");
    } else {
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
    }

    // ATM IV — reject placeholder
    let atmIv: number | null = null;
    if (currentPrice == null) {
      scanWarnings.push("ATM_IV_UNAVAILABLE_NO_PRICE");
    } else if (calls.length === 0) {
      scanWarnings.push("ATM_IV_UNAVAILABLE_NO_CALLS");
    } else {
      let minDist = Infinity;
      let rawAtmIv: number | null = null;
      for (const c of calls) {
        const dist = Math.abs((c.strike as number) - currentPrice);
        const iv = (c.impliedVolatility as number | null) ?? null;
        if (dist < minDist) {
          minDist = dist;
          rawAtmIv = iv;
        }
      }
      if (rawAtmIv != null && rawAtmIv > PLACEHOLDER_IV_THRESHOLD) {
        atmIv = rawAtmIv;
      } else {
        scanWarnings.push("ATM_IV_PLACEHOLDER");
      }
    }

    // dataQuality
    const dataQuality = computeDataQuality([...calls, ...puts], getLastTradingDate());
    const quality = dataQuality.quality;
    const allContracts = calls.length + puts.length;
    const placeholderIvCount = dataQuality.placeholderIvCount;

    let ivPctile: number | null = null;
    let chartTimestamps: number[] = [];
    if (quality === "LOW" && placeholderIvCount > allContracts * 0.5) {
      scanWarnings.push("IV_PERCENTILE_UNAVAILABLE_PLACEHOLDER_IV");
    } else {
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
            const windowSlice = rets.slice(i - 29, i + 1);
            const mean = windowSlice.reduce((a, b) => a + b, 0) / windowSlice.length;
            const variance = windowSlice.reduce((a, b) => a + (b - mean) ** 2, 0) / windowSlice.length;
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
    }

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

    // Bracket suppressed when data quality is LOW
    let bracket: string | null = null;
    if (quality !== "LOW" && pcRatio != null) {
      if (pcRatio >= 1.3 || (pcRatio >= 1.0 && putVolTrend === "INCREASING")) bracket = "UPPER";
      else if (pcRatio <= 0.8 && putVolTrend !== "INCREASING") bracket = "LOWER";
      else bracket = "MID";
    }

    let formattedBlock: string;
    if (quality === "LOW") {
      formattedBlock = "OPTIONS FLOW: DATA QUALITY LOW — raw chain unreliable; no doctrine weight.";
    } else {
      const ivStr = ivPctile != null ? `${ivPctile}th%ile` : "N/A";
      const pvStr = putVolVs10d != null ? `${putVolVs10d.toFixed(2)}x` : "N/A";
      const pcStr = pcRatio != null ? pcRatio.toFixed(2) : "N/A";
      formattedBlock = `OPTIONS FLOW SCAN [${windowLabel}] ${ticker} | P/C: ${pcStr} | IV: ${ivStr} | Put vol vs 10d avg: ${pvStr} | Trend: ${putVolTrend} | Advisory: ${bracket ?? "N/A"} bracket`;
    }

    const resultData: Record<string, unknown> = {
      ticker, windowLabel, dataDate,
      pcRatio, ivPctile, putVolVs10dAvg: putVolVs10d, putVolTrend,
      maxPainStrike, bracket, formattedBlock,
      dataQuality, warnings: scanWarnings,
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

export async function getPositionScoreInputs(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getPositionScoreInputs(t));
  }
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
    const currency = fi.currency as string | null;

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
    let fxRate: number | null = null;
    let note: string;

    if (foreignExchange) {
      if (lastVolume != null && lastPrice != null && lastPrice > 0) {
        const localNotional = lastVolume * lastPrice;
        let appliedFxRate = 1.0;
        let fxConversionNote = "";

        if (currency && currency !== "USD") {
          try {
            const fxFi = JSON.parse(await getFastInfo(`${currency}=X`)) as Record<string, unknown>;
            const rate = fxFi.lastPrice as number | null;
            if (rate != null && rate > 0) {
              appliedFxRate = rate;
              fxRate = +rate.toFixed(4);
              fxConversionNote = ` [${currency}→USD at ${rate.toFixed(2)}]`;
            } else {
              fxConversionNote = ` [${currency}=X rate unavailable — notional in local currency]`;
            }
          } catch {
            fxConversionNote = ` [${currency}=X fetch failed — notional in local currency]`;
          }
        } else if (currency === "USD") {
          fxRate = 1.0;
        }

        const dailyNotionalUSD = localNotional / appliedFxRate;
        gatePass = dailyNotionalUSD >= 10_000_000;
        note = `Volume gate ${gatePass ? "PASS" : "FAIL"} (DC-80 FX) — $${(dailyNotionalUSD / 1_000_000).toFixed(1)}M daily notional (${gatePass ? "≥" : "<"} $10M threshold)${fxConversionNote}`;
      } else {
        note = "Volume gate UNKNOWN — insufficient price/volume data for DC-80 FX check";
      }
      // Bug 5: also compute ratio20d in the FX branch
      if (lastVolume != null && adv20d != null && adv20d > 0) {
        ratio20d = +(lastVolume / adv20d).toFixed(2);
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
      ticker, currency, lastVolume, adv10d, adv20d, adv90d, ratio20d, fxRate, gatePass, dataDate, note,
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// ── get_options_summary ───────────────────────────────────────────────────────

export async function getOptionsSummary(ticker: string): Promise<string> {
  try {
    const expData = JSON.parse(await getOptionExpirationDates(ticker)) as string[];
    if (!expData || expData.length === 0) {
      return JSON.stringify({ ticker, error: "No options data available" });
    }
    const expiry = expData[0];
    // Fetch all contracts without illiquid filtering and with strike sort so we get the full chain
    const callsRaw = JSON.parse(await getOptionChain(ticker, expiry, "calls", 200, 0, 0, null, null, "all", "strike", 20, true)) as Record<string, unknown>;
    const putsRaw = JSON.parse(await getOptionChain(ticker, expiry, "puts", 200, 0, 0, null, null, "all", "strike", 20, true)) as Record<string, unknown>;
    const calls = (callsRaw.contracts ?? []) as Record<string, unknown>[];
    const puts = (putsRaw.contracts ?? []) as Record<string, unknown>[];

    const fi = JSON.parse(await getFastInfo(ticker)) as Record<string, unknown>;
    const currentPrice = fi.lastPrice as number | null;

    const summaryWarnings: string[] = [];

    let atmIV: number | null = null;
    if (currentPrice == null) {
      summaryWarnings.push("ATM_IV_UNAVAILABLE_NO_PRICE");
    } else if (calls.length === 0) {
      summaryWarnings.push("ATM_IV_UNAVAILABLE_NO_CALLS");
    } else {
      let minDist = Infinity;
      let rawAtmIV: number | null = null;
      for (const c of calls) {
        const strike = c.strike as number;
        const dist = Math.abs(strike - currentPrice);
        if (dist < minDist) {
          minDist = dist;
          rawAtmIV = (c.impliedVolatility as number | null) ?? null;
        }
      }
      if (rawAtmIV != null && rawAtmIV > PLACEHOLDER_IV_THRESHOLD) {
        atmIV = +rawAtmIV.toFixed(4);
      } else {
        summaryWarnings.push("ATM_IV_PLACEHOLDER");
      }
    }

    const callVol = calls.reduce((s, c) => s + ((c.volume as number) || 0), 0);
    const putVol = puts.reduce((s, c) => s + ((c.volume as number) || 0), 0);
    const callOI = calls.reduce((s, c) => s + ((c.openInterest as number) || 0), 0);
    const putOI = puts.reduce((s, c) => s + ((c.openInterest as number) || 0), 0);
    const pcRatioVolume = callVol > 0 ? +(putVol / callVol).toFixed(3) : null;
    const pcRatioOI = callOI > 0 ? +(putOI / callOI).toFixed(3) : null;

    let maxPainStrike: number | null = null;
    if (callOI + putOI <= 0) {
      summaryWarnings.push("MAX_PAIN_UNAVAILABLE_ZERO_OI");
    } else {
      const strikeSet = new Set([...calls.map(c => c.strike as number), ...puts.map(p => p.strike as number)]);
      const allStrikes = Array.from(strikeSet).sort((a, b) => a - b);
      let minPain = Infinity;
      for (const s of allStrikes) {
        const callPain = calls.reduce((sum, c) => sum + Math.max(0, s - (c.strike as number)) * ((c.openInterest as number) || 0), 0);
        const putPain = puts.reduce((sum, p) => sum + Math.max(0, (p.strike as number) - s) * ((p.openInterest as number) || 0), 0);
        const total = callPain + putPain;
        if (total < minPain) { minPain = total; maxPainStrike = s; }
      }
    }

    const dataDate = getLastTradingDate();
    const dataQuality = computeDataQuality([...calls, ...puts], dataDate);

    return JSON.stringify({
      ticker, nearestExpiry: expiry, currentPrice,
      atmIV, pcRatioVolume, pcRatioOI,
      callVolume: callVol, putVolume: putVol, callOI, putOI,
      maxPainStrike, dataDate,
      dataQuality,
      warnings: summaryWarnings,
    });
  } catch (e) {
    return JSON.stringify({ ticker, error: `${e instanceof Error ? e.message : String(e)}` });
  }
}

// ── list_sec_filings ──────────────────────────────────────────────────────────

export async function listSecFilings(ticker: string, formType: string = "10-K", maxFilings: number = 5): Promise<string> {
  try {
    const tickersResp = await fetch("https://www.sec.gov/files/company_tickers.json", {
      headers: { "User-Agent": "yahoo-finance-mcp/1.0 admin@example.com" }
    });
    if (!tickersResp.ok) return JSON.stringify({ error: true, message: "Failed to fetch EDGAR tickers" });
    const tickersData = await tickersResp.json() as Record<string, { ticker: string; cik_str: string }>;

    let cik: number | null = null;
    for (const entry of Object.values(tickersData)) {
      if (entry.ticker.toUpperCase() === ticker.toUpperCase()) {
        cik = parseInt(entry.cik_str, 10);
        break;
      }
    }
    if (!cik) return JSON.stringify({ error: true, message: `Could not find EDGAR CIK for ticker '${ticker}'` });

    const cikPadded = String(cik).padStart(10, "0");
    const subResp = await fetch(`https://data.sec.gov/submissions/CIK${cikPadded}.json`, {
      headers: { "User-Agent": "yahoo-finance-mcp/1.0 admin@example.com" }
    });
    if (!subResp.ok) return JSON.stringify({ error: true, message: "Failed to fetch EDGAR submissions" });
    const subData = await subResp.json() as Record<string, unknown>;
    const recent = (subData.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>;

    const forms = (recent?.form ?? []) as string[];
    const dates = (recent?.filingDate ?? []) as string[];
    const accessions = (recent?.accessionNumber ?? []) as string[];
    const primaryDocs = (recent?.primaryDocument ?? []) as string[];

    const results: unknown[] = [];
    const limit = Math.min(Math.max(1, maxFilings), 20);
    for (let i = 0; i < forms.length && results.length < limit; i++) {
      if (forms[i] === formType) {
        const acc = accessions[i] ?? "";
        const accClean = acc.replace(/-/g, "");
        const docUrl = primaryDocs[i] ? `https://www.sec.gov/Archives/edgar/data/${cik}/${accClean}/${primaryDocs[i]}` : null;
        const indexUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accClean}/${acc}-index.htm`;
        results.push({ accessionNumber: acc, filingDate: dates[i] ?? "", formType: forms[i], primaryDocumentUrl: docUrl, edgarIndexUrl: indexUrl });
      }
    }
    return JSON.stringify({ ticker, formType, filings: results });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

// ── get_filing_outline ────────────────────────────────────────────────────────

export async function getFilingOutline(ticker: string, _accessionNumber: string | null, documentUrl: string | null): Promise<string> {
  try {
    if (!documentUrl) return JSON.stringify({ error: true, message: "document_url is required" });
    if (!documentUrl.startsWith("https://www.sec.gov/Archives/")) {
      return JSON.stringify({ error: true, message: "Invalid SEC URL" });
    }
    const resp = await fetch(documentUrl, { headers: { "User-Agent": "yahoo-finance-mcp/1.0 admin@example.com" } });
    if (!resp.ok) return JSON.stringify({ error: true, message: `HTTP ${resp.status}` });
    const html = await resp.text();

    const outline: { level: number; title: string }[] = [];
    const headingRe = /<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi;
    const itemRe = /Part\s+[IVX]+|Item\s+\d+[A-Z]?|Note\s+\d+/i;
    let m: RegExpExecArray | null;
    while ((m = headingRe.exec(html)) !== null && outline.length < 100) {
      const level = parseInt(m[1], 10);
      const text = m[2].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
      if (text && (itemRe.test(text) || text.length < 100)) {
        outline.push({ level, title: text });
      }
    }
    return JSON.stringify({ ticker, documentUrl, outline });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

// ── get_filing_section ────────────────────────────────────────────────────────

export async function getFilingSection(ticker: string, sectionName: string, documentUrl: string, contextChars: number = 3000): Promise<string> {
  try {
    if (!documentUrl.startsWith("https://www.sec.gov/Archives/")) {
      return JSON.stringify({ error: true, message: "Invalid SEC URL" });
    }
    const resp = await fetch(documentUrl, { headers: { "User-Agent": "yahoo-finance-mcp/1.0 admin@example.com" } });
    if (!resp.ok) return JSON.stringify({ error: true, message: `HTTP ${resp.status}` });
    const html = await resp.text();
    const text = html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
    const idx = text.toLowerCase().indexOf(sectionName.toLowerCase());
    if (idx === -1) return JSON.stringify({ ticker, sectionName, found: false, text: null });
    return JSON.stringify({ ticker, sectionName, found: true, text: text.slice(idx, idx + contextChars) });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

// ── list_filing_tables ────────────────────────────────────────────────────────

export async function listFilingTables(ticker: string, documentUrl: string): Promise<string> {
  try {
    if (!documentUrl.startsWith("https://www.sec.gov/Archives/")) {
      return JSON.stringify({ error: true, message: "Invalid SEC URL" });
    }
    const resp = await fetch(documentUrl, { headers: { "User-Agent": "yahoo-finance-mcp/1.0 admin@example.com" } });
    if (!resp.ok) return JSON.stringify({ error: true, message: `HTTP ${resp.status}` });
    const html = await resp.text();

    const tables: { tableIndex: number; rowCount: number; headers: string[] }[] = [];
    const tableRe = /<table[^>]*>([\s\S]*?)<\/table>/gi;
    const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
    let tm: RegExpExecArray | null;
    let idx = 0;
    while ((tm = tableRe.exec(html)) !== null && tables.length < 50) {
      const tableHtml = tm[1];
      const rows = tableHtml.match(/<tr[^>]*>[\s\S]*?<\/tr>/gi) ?? [];
      const headers: string[] = [];
      if (rows.length > 0) {
        const firstRow = rows[0] ?? "";
        let hm: RegExpExecArray | null;
        tdRe.lastIndex = 0;
        while ((hm = tdRe.exec(firstRow)) !== null && headers.length < 6) {
          headers.push(hm[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim());
        }
      }
      tables.push({ tableIndex: idx++, rowCount: rows.length, headers });
    }
    return JSON.stringify({ ticker, documentUrl, tableCount: tables.length, tables });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

// ── get_filing_table ──────────────────────────────────────────────────────────

export async function getFilingTable(ticker: string, documentUrl: string, tableIndex: number, maxRows: number = 30): Promise<string> {
  try {
    if (!documentUrl.startsWith("https://www.sec.gov/Archives/")) {
      return JSON.stringify({ error: true, message: "Invalid SEC URL" });
    }
    const resp = await fetch(documentUrl, { headers: { "User-Agent": "yahoo-finance-mcp/1.0 admin@example.com" } });
    if (!resp.ok) return JSON.stringify({ error: true, message: `HTTP ${resp.status}` });
    const html = await resp.text();

    const tableRe = /<table[^>]*>([\s\S]*?)<\/table>/gi;
    const tables: string[] = [];
    let tm: RegExpExecArray | null;
    while ((tm = tableRe.exec(html)) !== null) tables.push(tm[1]);

    if (tableIndex >= tables.length) {
      return JSON.stringify({ error: true, message: `Table index ${tableIndex} not found. Document has ${tables.length} tables.` });
    }

    const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
    const tableHtml = tables[tableIndex];
    const rowMatches = tableHtml.match(/<tr[^>]*>[\s\S]*?<\/tr>/gi) ?? [];
    const parsedRows: string[][] = [];

    for (let r = 0; r < Math.min(rowMatches.length, maxRows + 1); r++) {
      const cells: string[] = [];
      let cm: RegExpExecArray | null;
      tdRe.lastIndex = 0;
      while ((cm = tdRe.exec(rowMatches[r])) !== null && cells.length < 10) {
        cells.push(cm[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim());
      }
      parsedRows.push(cells);
    }

    return JSON.stringify({ ticker, tableIndex, totalRows: rowMatches.length, returnedRows: parsedRows.length, rows: parsedRows });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

// ── extract_filing_fact ───────────────────────────────────────────────────────

export async function extractFilingFact(ticker: string, factName: string, _documentUrl: string | null, _accessionNumber: string | null): Promise<string> {
  try {
    const searchResult = JSON.parse(await searchFilingText(
      ticker,
      [factName],
      null,
      "10-K",
      _accessionNumber,
      1000,
      true,
    )) as Record<string, unknown>;
    return JSON.stringify({
      ticker, factName,
      extractionMethod: "text_search",
      result: searchResult,
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

// ─── Public news / event helpers ──────────────────────────────────────────────

const _str = (v: unknown, fallback = ""): string => (typeof v === "string" ? v : fallback);

function eventTypeFromKeywords(title: string, summary: string): string {
  const text = `${title} ${summary}`.toLowerCase();
  if (/earnings|quarterly results|q[1-4] \d{4}/.test(text)) return "EARNINGS";
  if (/dividend|ex-div/.test(text)) return "DIVIDEND";
  if (/merger|acquisition|acqui/.test(text)) return "M&A";
  if (/guidance|outlook|forecast/.test(text)) return "GUIDANCE";
  if (/press release|announcement/.test(text)) return "PRESS_RELEASE";
  if (/sec filing|form 8-k|form 10/.test(text)) return "SEC_FILING";
  return "NEWS";
}

function makeDupGroupId(title: string, dateStr: string): string {
  // djb2-variant hash: h = h * 33 - h + char, accumulated over title+date string.
  // Yields a 32-bit unsigned integer → 8 hex digits, used as a stable dedup key.
  let h = 0;
  const s = `${dateStr}::${title.toLowerCase().replace(/\s+/g, " ").trim()}`;
  for (let i = 0; i < s.length; i++) { h = ((h << 5) - h + s.charCodeAt(i)) | 0; }
  return (h >>> 0).toString(16).padStart(8, "0");
}

function buildYfEventItem(item: Record<string, unknown>): Record<string, unknown> {
  const title = _str(item.title);
  const rawPublishedAt = typeof item.providerPublishTime === "number"
    ? iso(item.providerPublishTime as number)
    : _str(item.publishedAt);
  // Preserve null when publishedAt is genuinely unknown — do not substitute epoch sentinel.
  const publishedAt = rawPublishedAt || null;
  return {
    source: "Yahoo Finance",
    sourceType: "yahoo_finance",
    title,
    summary: _str(item.summary),
    url: _str(item.link),
    publishedAt,
    retrievedAt: new Date().toISOString(),
    eventType: eventTypeFromKeywords(title, _str(item.summary)),
    confidence: "MEDIUM",
    duplicateGroupId: makeDupGroupId(title, (publishedAt ?? "").slice(0, 10)),
  };
}

function buildSecEventItem(filing: Record<string, unknown>): Record<string, unknown> {
  const title = _str(filing.description || filing.formType || "SEC Filing");
  const filingDate = _str(filing.filingDate || filing.date || "");
  return {
    source: "SEC EDGAR",
    sourceType: "sec_filing",
    title,
    accessionNumber: _str(filing.accessionNumber),
    url: _str(filing.primaryDocumentUrl || filing.filingUrl),
    publishedAt: filingDate,
    retrievedAt: new Date().toISOString(),
    eventType: eventTypeFromKeywords(title, _str(filing.formType)),
    confidence: "HIGH",
    formType: _str(filing.formType),
    duplicateGroupId: makeDupGroupId(title, filingDate.slice(0, 10)),
  };
}

// ─── Public event / news tools ─────────────────────────────────────────────────

export async function searchCompanyNews(ticker: string, query = "", maxResults = 20): Promise<string> {
  try {
    const raw = JSON.parse(await getNews(ticker)) as Record<string, unknown>;
    let items = (raw.news as Record<string, unknown>[]) ?? [];
    if (query) {
      const q = query.toLowerCase();
      items = items.filter(n =>
        _str(n.title).toLowerCase().includes(q) ||
        _str(n.summary).toLowerCase().includes(q)
      );
    }
    items = items.slice(0, maxResults);
    return JSON.stringify({
      ticker,
      query,
      count: items.length,
      items: items.map(buildYfEventItem),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

export async function getCompanyPressReleases(ticker: string, maxResults = 10, startDate = ""): Promise<string> {
  try {
    const raw = JSON.parse(await listSecFilings(ticker, "8-K", maxResults + 5)) as Record<string, unknown>;
    let filings = (raw.filings as Record<string, unknown>[]) ?? [];
    if (startDate) {
      filings = filings.filter(f => _str(f.filingDate || f.date) >= startDate);
    }
    filings = filings.slice(0, maxResults);
    return JSON.stringify({
      ticker,
      count: filings.length,
      items: filings.map(buildSecEventItem),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

export async function getSecRecentEvents(ticker: string, filingType = "8-K", maxResults = 10, startDate = ""): Promise<string> {
  try {
    const raw = JSON.parse(await listSecFilings(ticker, filingType === "all" ? "8-K" : filingType, maxResults + 5)) as Record<string, unknown>;
    let filings = (raw.filings as Record<string, unknown>[]) ?? [];
    if (startDate) {
      filings = filings.filter(f => _str(f.filingDate || f.date) >= startDate);
    }
    filings = filings.slice(0, maxResults);
    return JSON.stringify({
      ticker,
      filingType,
      count: filings.length,
      items: filings.map(buildSecEventItem),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

export async function getPublicEventTimeline(ticker: string, maxResults = 20, startDate = "", sources: string[] = ["sec", "yahoo_finance"]): Promise<string> {
  try {
    const items: Record<string, unknown>[] = [];
    const seen = new Set<string>();

    if (sources.includes("sec")) {
      const raw = JSON.parse(await listSecFilings(ticker, "8-K", maxResults)) as Record<string, unknown>;
      for (const f of (raw.filings as Record<string, unknown>[]) ?? []) {
        if (startDate && _str(f.filingDate || f.date) < startDate) continue;
        const item = buildSecEventItem(f);
        const key = _str(item.duplicateGroupId);
        if (!seen.has(key)) { seen.add(key); items.push(item); }
      }
    }

    if (sources.includes("yahoo_finance")) {
      const raw = JSON.parse(await getNews(ticker)) as Record<string, unknown>;
      for (const n of (raw.news as Record<string, unknown>[]) ?? []) {
        const item = buildYfEventItem(n);
        if (startDate && _str(item.publishedAt).slice(0, 10) < startDate) continue;
        const key = _str(item.duplicateGroupId);
        if (!seen.has(key)) { seen.add(key); items.push(item); }
      }
    }

    items.sort((a, b) => _str(b.publishedAt).localeCompare(_str(a.publishedAt)));
    return JSON.stringify({
      ticker,
      count: Math.min(items.length, maxResults),
      items: items.slice(0, maxResults),
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

export async function verifyCompanyEvent(ticker: string, eventQuery: string, startDate = "", endDate = "", sources: string[] = ["sec", "yahoo_finance"]): Promise<string> {
  try {
    const q = eventQuery.toLowerCase();
    const secItems: Record<string, unknown>[] = [];
    const newsItems: Record<string, unknown>[] = [];

    if (sources.includes("sec")) {
      const raw = JSON.parse(await listSecFilings(ticker, "8-K", 20)) as Record<string, unknown>;
      for (const f of (raw.filings as Record<string, unknown>[]) ?? []) {
        const date = _str(f.filingDate || f.date);
        if (startDate && date < startDate) continue;
        if (endDate && date > endDate) continue;
        const title = _str(f.description || f.formType);
        if (!q || title.toLowerCase().includes(q)) secItems.push(buildSecEventItem(f));
      }
    }

    if (sources.includes("yahoo_finance")) {
      const raw = JSON.parse(await getNews(ticker)) as Record<string, unknown>;
      for (const n of (raw.news as Record<string, unknown>[]) ?? []) {
        const item = buildYfEventItem(n);
        const date = _str(item.publishedAt).slice(0, 10);
        if (startDate && date < startDate) continue;
        if (endDate && date > endDate) continue;
        const title = _str(item.title).toLowerCase();
        const summary = _str(item.summary).toLowerCase();
        if (!q || title.includes(q) || summary.includes(q)) newsItems.push(item);
      }
    }

    let verificationStatus: string;
    if (secItems.length > 0) verificationStatus = "CONFIRMED";
    else if (newsItems.length > 0) verificationStatus = "PARTIAL";
    else verificationStatus = "NOT_FOUND";

    return JSON.stringify({
      ticker,
      eventQuery,
      verificationStatus,
      secEvidenceCount: secItems.length,
      newsEvidenceCount: newsItems.length,
      bestEvidence: [...secItems.slice(0, 3), ...newsItems.slice(0, 3)],
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

// ── index_sec_filing / get_sec_filing_index ────────────────────────────────────

const _INDEX_KEYWORDS = [
  "china", "greater china", "prc", "geographic", "segment", "revenue",
  "customers", "long-lived assets", "risk factors", "americas", "europe",
  "japan", "asia", "rest of asia",
];

function _stripHtmlTagsIdx(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/&[^;]+;/g, " ").replace(/\s+/g, " ").trim();
}

function _buildFilingIndexFromHtml(
  html: string,
): { sections: Record<string, unknown>[]; tables: Record<string, unknown>[]; keywordMap: Record<string, string[]> } {
  // Remove scripts/styles/event handlers to reduce noise.
  // Use \s* before element names in closing tags to handle whitespace variants (</script >, </style >).
  let sanitized = html.replace(/<script\b[^>]*>[\s\S]*?<\/\s*script[^>]*>/gi, "");
  sanitized = sanitized.replace(/<style\b[^>]*>[\s\S]*?<\/\s*style[^>]*>/gi, "");
  sanitized = sanitized.replace(/\s+on\w+=(?:"[^"]*"|'[^']*'|[^\s>]+)/gi, " ");

  // --- Section extraction ---
  const sections: Record<string, unknown>[] = [];
  const headingRe = /<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi;
  let hm: RegExpExecArray | null;
  while ((hm = headingRe.exec(sanitized)) !== null && sections.length < 50) {
    const level = parseInt(hm[1], 10);
    const rawText = _stripHtmlTagsIdx(hm[2]);
    if (!rawText || rawText.length > 200) continue;
    const normalized = rawText.toLowerCase().trim();
    const keywords = _INDEX_KEYWORDS.filter(kw => normalized.includes(kw));
    const sectionId = normalized.replace(/[^a-z0-9]+/g, "_").slice(0, 60);
    sections.push({ sectionId, heading: rawText, normalizedHeading: normalized, level, keywords, startChar: hm.index, endChar: hm.index + hm[0].length });
  }

  // --- Table extraction ---
  const tables: Record<string, unknown>[] = [];
  const tableRe = /<table[^>]*>([\s\S]*?)<\/table>/gi;
  const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
  let tm: RegExpExecArray | null;
  let tableIdx = 0;
  while ((tm = tableRe.exec(sanitized)) !== null && tableIdx < 100) {
    const tableStart = tm.index;
    const tableHtml = tm[0];

    // Nearby section (last section starting before this table)
    let nearbySectionId: string | null = null;
    let nearbyHeading = "";
    for (let si = sections.length - 1; si >= 0; si--) {
      if ((sections[si]!.startChar as number) <= tableStart) {
        nearbySectionId = sections[si]!.sectionId as string;
        nearbyHeading = sections[si]!.heading as string;
        break;
      }
    }

    // Parse rows
    const rowMatches = tableHtml.match(/<tr[^>]*>[\s\S]*?<\/tr>/gi) ?? [];
    if (rowMatches.length === 0) { tableIdx++; continue; }

    // Headers from first row
    const headers: string[] = [];
    tdRe.lastIndex = 0;
    let hd: RegExpExecArray | null;
    while ((hd = tdRe.exec(rowMatches[0] ?? "")) !== null && headers.length < 10) {
      headers.push(_stripHtmlTagsIdx(hd[1]));
    }

    // Row labels from first column of subsequent rows (up to 19 data rows)
    const rowLabels: string[] = [];
    for (let r = 1; r < Math.min(rowMatches.length, 20); r++) {
      tdRe.lastIndex = 0;
      const cellMatch = tdRe.exec(rowMatches[r] ?? "");
      if (cellMatch) {
        const label = _stripHtmlTagsIdx(cellMatch[1]);
        if (label && label.length < 100) rowLabels.push(label);
      }
    }

    // Detect unit scale
    const preContext = sanitized.slice(Math.max(0, tableStart - 2000), tableStart).toLowerCase();
    const tableContext = (tableHtml + preContext).toLowerCase();
    let unitScale = "millions";
    if (/billion|in billions/.test(tableContext)) unitScale = "billions";
    else if (/thousand|in thousands/.test(tableContext)) unitScale = "thousands";

    // Confidence
    const hasYearHeaders = headers.some(h => /\b20\d\d\b/.test(h));
    const hasRowLabels = rowLabels.length > 0;
    const confidence = hasYearHeaders && hasRowLabels ? "HIGH" : (hasYearHeaders || hasRowLabels ? "MEDIUM" : "LOW");

    // Infer title from pre-context
    const preText = _stripHtmlTagsIdx(sanitized.slice(Math.max(0, tableStart - 500), tableStart));
    const lines = preText.split("\n").map(l => l.trim()).filter(Boolean);
    let title = "";
    const candidate = lines[lines.length - 1] ?? "";
    if (candidate.length > 10 && candidate.length < 200) title = candidate;

    tables.push({ tableId: tableIdx, sectionId: nearbySectionId, title: title || nearbyHeading, headers, rowLabels, unit: "USD", unitScale, confidence });
    tableIdx++;
  }

  // --- Keyword map ---
  const keywordMap: Record<string, string[]> = {};
  for (const kw of _INDEX_KEYWORDS) {
    const refs: string[] = [];
    for (const sec of sections) {
      if ((sec.normalizedHeading as string).includes(kw)) {
        const ref = `sectionId:${sec.sectionId}`;
        if (!refs.includes(ref)) refs.push(ref);
      }
    }
    for (const tbl of tables) {
      const haystack = [
        ...(tbl.rowLabels as string[]),
        ...(tbl.headers as string[]),
        tbl.title as string,
      ].join(" ").toLowerCase();
      if (haystack.includes(kw)) {
        const ref = `tableId:${tbl.tableId}`;
        if (!refs.includes(ref)) refs.push(ref);
      }
    }
    if (refs.length > 0) keywordMap[kw] = refs;
  }

  return { sections, tables, keywordMap };
}

async function _indexSecFilingImpl(
  ticker: string,
  filingType: string,
  // `period` is reserved for future multi-period support (e.g. "2024", "prior").
  // Currently only "latest" is supported; the field is kept for API stability.
  _period: string,
  accessionNumber: string | null,
): Promise<string> {
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not resolve EDGAR submissions for ticker '${ticker}'` } });
  }

  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form as string[]) ?? [];
  const accessions = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs = (recent.primaryDocument as string[]) ?? [];
  const filingDates = (recent.filingDate as string[]) ?? [];
  const acceptedDts = (recent.acceptanceDateTime as string[]) ?? [];

  // Find target filing
  let targetIdx: number | null = null;
  if (accessionNumber) {
    for (let i = 0; i < accessions.length; i++) {
      if (accessions[i] === accessionNumber) { targetIdx = i; break; }
    }
  } else {
    for (let i = 0; i < forms.length; i++) {
      if (forms[i]!.toUpperCase() === filingType.toUpperCase()) {
        targetIdx = i;
        accessionNumber = accessions[i] ?? null;
        break;
      }
    }
  }

  if (targetIdx === null || !accessionNumber) {
    return JSON.stringify({ ok: false, error: { code: "NO_FILING_DATA", message: `No ${filingType} filing found for '${ticker}'` } });
  }

  const filingDate = filingDates[targetIdx] ?? "";
  const acceptedAt = acceptedDts[targetIdx] ?? null;
  const primaryDoc = primaryDocs[targetIdx] ?? null;

  if (!primaryDoc) {
    return JSON.stringify({ ok: false, error: { code: "NO_FILING_DATA", message: `primaryDocument missing for ${accessionNumber}` } });
  }

  const cikInt = parseInt(cikPadded, 10);
  const accClean = accessionNumber.replace(/-/g, "");
  const documentUrl = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${primaryDoc}`;

  // Check cache
  const cacheKey = `secidx:${ticker.toUpperCase()}:${accessionNumber}:${filingType}`;
  const cached = filingIndexCache.get(cacheKey);
  if (cached && Date.now() - cached.storedAt < FILING_INDEX_TTL_MS) {
    return cached.value;
  }

  // Fetch filing HTML
  const resp = await fetch(documentUrl, { headers: { "User-Agent": EDGAR_UA } });
  if (!resp.ok) {
    return JSON.stringify({ ok: false, error: { code: "PROVIDER_ERROR", message: `Failed to fetch filing: HTTP ${resp.status}` } });
  }

  // Read up to 5 MB to avoid memory issues on large filings
  const reader = resp.body?.getReader();
  if (!reader) {
    return JSON.stringify({ ok: false, error: { code: "PROVIDER_ERROR", message: "Failed to read filing response body" } });
  }
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  const MAX_BYTES = 5_000_000;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) {
      chunks.push(value);
      totalBytes += value.byteLength;
      if (totalBytes >= MAX_BYTES) { await reader.cancel(); break; }
    }
  }
  // Pre-allocate a single buffer and copy chunks at correct offsets (avoids O(n²) reduce).
  const merged = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) { merged.set(chunk, offset); offset += chunk.byteLength; }
  const html = new TextDecoder().decode(merged);

  const index = _buildFilingIndexFromHtml(html);
  const indexedAt = new Date().toISOString();

  const result = JSON.stringify({
    ticker,
    cik: cikPadded,
    filingType,
    filingDate,
    acceptedAt,
    accessionNumber,
    documentUrl,
    index,
    meta: {
      indexedAt,
      source: "sec",
      cacheKey: `${ticker.toUpperCase()}:${accessionNumber}`,
      cacheTtlHours: 24,
    },
  });

  filingIndexCache.set(cacheKey, { value: result, storedAt: Date.now() });
  return result;
}

// indexSecFiling and getSecFilingIndex are separate exports for API symmetry:
// index_sec_filing always (re-)builds and caches; get_sec_filing_index returns
// cached results and is the preferred read path. Both currently delegate to the
// same impl, but may diverge in future (e.g. force-refresh vs cache-first).
export async function indexSecFiling(
  ticker: string,
  filingType: string = "10-K",
  period: string = "latest",
  accessionNumber: string | null = null,
): Promise<string> {
  try {
    return await _indexSecFilingImpl(ticker, filingType, period, accessionNumber);
  } catch (e) {
    return JSON.stringify({ ok: false, error: { code: "PROVIDER_ERROR", message: `${e instanceof Error ? e.message : String(e)}` } });
  }
}

export async function getSecFilingIndex(
  ticker: string,
  filingType: string = "10-K",
  period: string = "latest",
  accessionNumber: string | null = null,
): Promise<string> {
  try {
    return await _indexSecFilingImpl(ticker, filingType, period, accessionNumber);
  } catch (e) {
    return JSON.stringify({ ok: false, error: { code: "PROVIDER_ERROR", message: `${e instanceof Error ? e.message : String(e)}` } });
  }
}
