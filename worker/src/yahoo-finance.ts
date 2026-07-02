import { getWorkerVar, mcpFailure } from "./response.js";

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
const FINNHUB_COMPANY_NEWS_API = "https://finnhub.io/api/v1/company-news";
const GLOBENEWSWIRE_RSS_FEEDS: Array<{ name: string; url: string }> = [
  {
    name: "public_companies",
    url: "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
  },
  {
    name: "press_releases",
    url: "https://www.globenewswire.com/RssFeed/subjectcode/72-Press%20Releases/feedTitle/GlobeNewswire%20-%20Press%20Releases",
  },
  {
    name: "earnings",
    url: "https://www.globenewswire.com/RssFeed/subjectcode/13-Earnings%20Releases%20And%20Operating%20Results/feedTitle/GlobeNewswire%20-%20Earnings%20Releases%20And%20Operating%20Results",
  },
  {
    name: "stock_market_news",
    url: "https://www.globenewswire.com/RssFeed/subjectcode/39-Stock%20Market%20News/feedTitle/GlobeNewswire%20-%20Stock%20Market%20News",
  },
  {
    name: "technology",
    url: "https://www.globenewswire.com/RssFeed/industry/9000-Technology/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Technology",
  },
  {
    name: "semiconductors",
    url: "https://www.globenewswire.com/RssFeed/industry/9576-Semiconductors/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Semiconductors",
  },
  {
    name: "telecommunications",
    url: "https://www.globenewswire.com/RssFeed/industry/6000-Telecommunications/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Telecommunications",
  },
  {
    name: "mobile_telecommunications",
    url: "https://www.globenewswire.com/RssFeed/industry/6575-Mobile%20Telecommunications/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Mobile%20Telecommunications",
  },
  {
    name: "telecommunications_equipment",
    url: "https://www.globenewswire.com/RssFeed/industry/9578-Telecommunications%20Equipment/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Telecommunications%20Equipment",
  },
  {
    name: "electronic_equipment",
    url: "https://www.globenewswire.com/RssFeed/industry/2737-Electronic%20Equipment/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Electronic%20Equipment",
  },
];
const GLOBENEWSWIRE_MAX_BYTES = 2 * 1024 * 1024;
const GLOBENEWSWIRE_TTL_MS = 15 * 60 * 1000;
const GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN = "https://www.globenewswire.com/rss/stock";
const GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN = "https://www.globenewswire.com/rss/ISIN";
const globenewswireCache = new Map<string, { value: string; storedAt: number }>();
const YAHOO_ALLOWED_CONTENT_TYPES = new Set(["STORY", "ARTICLE", "PRESS_RELEASE"]);
const SMOKE_TICKER_CIK_FALLBACKS: Record<string, string> = {
  AAPL: "0000320193",
  MSFT: "0000789019",
  AMZN: "0001018724",
  GOOGL: "0001652044",
  GOOG: "0001652044",
  NVDA: "0001045810",
  TSLA: "0001318605",
  META: "0001326801",
  VRT: "0001674101",
  AAOI: "0001158114",
  AXTI: "0001051627",
};

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

const PLACEHOLDER_IV_THRESHOLD = 0.10;

interface DataQuality {
  zeroBidAskCount: number;
  zeroOpenInterestCount: number;
  placeholderIvCount: number;
  staleLastTradeCount: number;
  returnedContracts: number;
  overall: "HIGH" | "MEDIUM" | "LOW" | "PARTIAL";
  quality: "HIGH" | "MEDIUM" | "LOW";
  volumeQuality: "OK" | "UNAVAILABLE";
  oiQuality: "OK" | "STALE" | "UNAVAILABLE";
  ivQuality: "OK" | "PARTIAL" | "UNAVAILABLE";
  priceQuality: "OK";
  warnings: string[];
}

function isPlaceholderIv(v: unknown): boolean {
  if (v == null) return true;
  const n = Number(v);
  return !Number.isFinite(n) || n <= PLACEHOLDER_IV_THRESHOLD;
}

function normalizeContractIv(c: Record<string, unknown>): Record<string, unknown> {
  if ("impliedVolatility" in c && isPlaceholderIv(c.impliedVolatility)) {
    return { ...c, impliedVolatility: null };
  }
  return c;
}

function invalidExpiryPayload(ticker: string, requested: string, expirations: string[]): Record<string, unknown> {
  let nearest: string | null = expirations[0] ?? null;
  const requestedMs = Date.parse(`${requested}T00:00:00Z`);
  if (Number.isFinite(requestedMs) && expirations.length) {
    nearest = [...expirations].sort((a, b) =>
      Math.abs(Date.parse(`${a}T00:00:00Z`) - requestedMs)
      - Math.abs(Date.parse(`${b}T00:00:00Z`) - requestedMs)
    )[0] ?? null;
  }
  return {
    error: true,
    code: "INVALID_EXPIRY_DATE",
    message: `${requested} is not in the options calendar for ${ticker.toUpperCase()}`,
    ticker: ticker.toUpperCase(),
    requestedExpiration: requested,
    nearestExpiration: nearest,
    validExpirations: expirations,
    hint: "Call get_option_expiration_dates first and pass one of the returned dates.",
  };
}

function zeroOpenInterestRatio(contracts: Record<string, unknown>[]): number {
  if (contracts.length === 0) return 1;
  return contracts.filter(c => Number(c.openInterest ?? 0) <= 0).length / contracts.length;
}

function majorityZeroOpenInterest(contracts: Record<string, unknown>[]): boolean {
  return contracts.length > 0 && zeroOpenInterestRatio(contracts) > 0.5;
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
      overall: "LOW",
      volumeQuality: "UNAVAILABLE",
      oiQuality: "UNAVAILABLE",
      ivQuality: "UNAVAILABLE",
      priceQuality: "OK",
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

    if (isPlaceholderIv(c.impliedVolatility)) placeholderIv++;

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
           staleLastTradeCount: staleTrade, returnedContracts: n,
           overall: quality === "LOW" ? "PARTIAL" : quality,
           quality,
           volumeQuality: n > 0 ? "OK" : "UNAVAILABLE",
           oiQuality: zeroOI > n * 0.5 ? "STALE" : "OK",
           ivQuality: placeholderIv > n * 0.5 ? "UNAVAILABLE" : (placeholderIv > 0 ? "PARTIAL" : "OK"),
           priceQuality: "OK",
           warnings };
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
    const aStrike = Number(a.strike ?? 0), bStrike = Number(b.strike ?? 0);

    const aValidQuote = aBid > 0 && aAsk > 0 ? 1 : 0;
    const bValidQuote = bBid > 0 && bAsk > 0 ? 1 : 0;
    if (bValidQuote !== aValidQuote) return bValidQuote - aValidQuote;

    const aLiquidity = (aOI > 0 || aVol > 0) ? 1 : 0;
    const bLiquidity = (bOI > 0 || bVol > 0) ? 1 : 0;
    if (bLiquidity !== aLiquidity) return bLiquidity - aLiquidity;

    const aValidIv = !isPlaceholderIv(a.impliedVolatility) ? 1 : 0;
    const bValidIv = !isPlaceholderIv(b.impliedVolatility) ? 1 : 0;
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
    if (typeof p.ok === "boolean" && ("data" in p || "error" in p)) {
      if (p.ok === true) {
        return { ok: true, data: p.data ?? null };
      }
      const errObj = p.error as Record<string, unknown> | null | undefined;
      const code = typeof errObj?.code === "string" ? errObj.code : "PROVIDER_ERROR";
      const message = typeof errObj?.message === "string" ? errObj.message : `Error for ${ticker}`;
      const retryable = errObj?.retryable === true || code === "PROVIDER_TIMEOUT";
      return { ok: false, data: null, error: toBatchError(message, code, retryable) };
    }
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
      out[t] = shaped.ok === true
        ? (shaped.data ?? null)
        : {
            error: true,
            code: typeof (shaped.error as Record<string, unknown> | undefined)?.code === "string"
              ? (shaped.error as Record<string, unknown>).code
              : "PROVIDER_ERROR",
            message: typeof (shaped.error as Record<string, unknown> | undefined)?.message === "string"
              ? (shaped.error as Record<string, unknown>).message
              : `Error for ${t}`,
            retryable: (shaped.error as Record<string, unknown> | undefined)?.retryable === true,
          };
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
      date: iso(t),
      open: quote.open?.[i] ?? null,
      high: quote.high?.[i] ?? null,
      low: quote.low?.[i] ?? null,
      close: quote.close?.[i] ?? null,
      volume: quote.volume?.[i] ?? null,
      adjClose: adjclose[i] ?? null,
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
    const expirationDates = JSON.parse(await getOptionExpirationDates(ticker)) as string[];
    if (!Array.isArray(expirationDates) || expirationDates.length === 0) {
      return JSON.stringify({
        error: true,
        code: "NO_OPTIONS_DATA",
        message: `No options calendar available for ${ticker}`,
        ticker,
      });
    }
    if (!expirationDates.includes(expDate)) {
      const targetMs = new Date(`${expDate}T00:00:00.000Z`).getTime();
      const nearestExpiration = Number.isFinite(targetMs)
        ? [...expirationDates].sort((a, b) =>
            Math.abs(new Date(`${a}T00:00:00.000Z`).getTime() - targetMs) -
            Math.abs(new Date(`${b}T00:00:00.000Z`).getTime() - targetMs)
          )[0]
        : null;
      return JSON.stringify({
        error: true,
        code: "INVALID_EXPIRY_DATE",
        message: `${expDate} is not in the options calendar for ${ticker}`,
        hint: "Call get_option_expiration_dates first and pass one of the returned dates.",
        ticker,
        requestedExpiration: expDate,
        nearestExpiration,
        validExpirations: expirationDates,
      });
    }

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

    const warnings: string[] = [];
    let contracts = ((opts[optType] ?? []) as Record<string, unknown>[]).map(normalizeContractIv);
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
      const beforeLiquidityFilter = contracts.length;
      contracts = contracts.filter(c => {
        const bid = Number(c.bid ?? 0);
        const ask = Number(c.ask ?? 0);
        const oi = Number(c.openInterest ?? 0);
        return bid > 0 || ask > 0 || oi > 0;
      });
      if (beforeLiquidityFilter > 0 && contracts.length === 0) {
        warnings.push("ALL_CONTRACTS_EXCLUDED_BY_LIQUIDITY_FILTER");
      }
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
      warnings,
      ...(warnings.includes("ALL_CONTRACTS_EXCLUDED_BY_LIQUIDITY_FILTER")
        ? { note: "Zero contracts returned after liquidity filtering. During pre-market or T+1 OI lag windows, retry with include_illiquid=true." }
        : {}),
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
    calls: ((opts.calls ?? []) as Record<string, unknown>[]).map(normalizeContractIv),
    puts: ((opts.puts ?? []) as Record<string, unknown>[]).map(normalizeContractIv),
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

const OVERNIGHT_SUSPENSION_REASON =
  "True overnight window (20:00-04:00 ET) unavailable via Yahoo Finance API";
const OVERNIGHT_RELIABILITY_WARNING =
  "OTC_INDICATIVE data may lag actual price materially; use with extreme caution.";
const OVERNIGHT_DEPRECATION_WARNING = {
  code: "TRUE_OVERNIGHT_PROVIDER_REMOVED",
  message: "This tool is diagnostics-only and does not provide true 20:00-04:00 ET overnight venue data.",
  severity: "warning",
};
const OVERNIGHT_DIAGNOSTIC_FIELDS = {
  dataKind: "yahoo_extended_hours_proxy",
  decisionGrade: false,
  doctrineUse: "DIAGNOSTICS_ONLY",
  warnings: [OVERNIGHT_DEPRECATION_WARNING],
};

// ── get_overnight_quote ───────────────────────────────────────────────────────

export async function getOvernightQuote(ticker: string): Promise<string> {
  return getYahooOvernightQuote(ticker);
}

async function getYahooOvernightQuote(ticker: string): Promise<string> {
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

    // True overnight window: 00:00–08:00 UTC (= 20:00–04:00 ET).
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
          ...OVERNIGHT_DIAGNOSTIC_FIELDS,
          status: "SUSPENDED",
          provider: "yahoo",
          providerStatus: "FALLBACK_EXTENDED_HOURS",
          requestedFeed: null,
          suspensionReason: OVERNIGHT_SUSPENSION_REASON,
          reliabilityWarning: OVERNIGHT_RELIABILITY_WARNING,
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
        ...OVERNIGHT_DIAGNOSTIC_FIELDS,
        status: "SUSPENDED",
        provider: "yahoo",
        providerStatus: "FALLBACK_EXTENDED_HOURS",
        requestedFeed: null,
        suspensionReason: OVERNIGHT_SUSPENSION_REASON,
        reliabilityWarning: OVERNIGHT_RELIABILITY_WARNING,
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
    const suspendedOvernight = dataSource === "OTC_INDICATIVE" && dataAgeHours != null && dataAgeHours > 6;

    return JSON.stringify({
      ticker,
      ...OVERNIGHT_DIAGNOSTIC_FIELDS,
      provider: "yahoo",
      providerStatus: "FALLBACK_EXTENDED_HOURS",
      requestedFeed: null,
      ...(suspendedOvernight
        ? {
            status: "SUSPENDED",
            suspensionReason: OVERNIGHT_SUSPENSION_REASON,
            reliabilityWarning: OVERNIGHT_RELIABILITY_WARNING,
          }
        : { status: "OK" }),
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
    return JSON.stringify({
      error: true,
      message: `${e instanceof Error ? e.message : String(e)}`,
      ticker,
      provider: "yahoo",
      providerStatus: "PROVIDER_UNAVAILABLE",
    });
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
  const modules = "financialData,recommendationTrend,price,upgradeDowngradeHistory";
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=${modules}`
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
      fetchTimeseries(ticker, "quarterly", [
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

    const bs = JSON.parse(bsRaw) as Record<string, unknown>[];
    const inc = JSON.parse(incRaw) as Record<string, unknown>[];

    if (!Array.isArray(bs) || !bs.length || !Array.isArray(inc) || !inc.length) {
      return JSON.stringify({ error: true, message: "Insufficient financial data", ticker });
    }

    const bsLatest = bs[0];
    // TTM: sum up to 4 most-recent quarterly income rows (newest first)
    const incRows = inc.slice(0, 4);

    // Sum the first matching key across quarterly rows for TTM
    const ttmSum = (keys: string[]): { value: number | null; source: string | null; values: number[] } => {
      for (const key of keys) {
        const vals: number[] = [];
        for (const row of incRows) {
          const v = row[key];
          if (typeof v === "number" && Number.isFinite(v)) vals.push(v);
        }
        if (vals.length > 0) {
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

    const netDebtToEbitda = netDebt != null && operationalEbitdaAnnual != null && operationalEbitdaAnnual !== 0
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
    const selectedExpiry = expiryHint || dates[0];
    if (!dates.includes(selectedExpiry)) {
      return JSON.stringify(invalidExpiryPayload(ticker, selectedExpiry, dates));
    }

    // If the selected expiry is the default (first), reuse firstFetch data;
    // otherwise make one more subrequest for the specific date's chain
    let calls: Record<string, unknown>[];
    let puts: Record<string, unknown>[];
    if (selectedExpiry === dates[0]) {
      calls = firstFetch.calls.map(normalizeContractIv);
      puts = firstFetch.puts.map(normalizeContractIv);
    } else {
      const specific = await yGetFullOptions(ticker, selectedExpiry);
      calls = specific.calls.map(normalizeContractIv);
      puts = specific.puts.map(normalizeContractIv);
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
      if (!isPlaceholderIv(rawAtmIV)) {
        atmIV = +Number(rawAtmIV).toFixed(3);
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
    const allContracts = [...calls, ...puts];
    if (totalCallOI + totalPutOI <= 0 || majorityZeroOpenInterest(allContracts)) {
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
    const dataQuality = computeDataQuality(allContracts, dataDate);

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
        upgrades: 0,
        upgrades30d: daysBack === 30 ? 0 : null,
        downgrades: 0,
        downgrades30d: daysBack === 30 ? 0 : null,
        initiations: 0,
        initiations30d: daysBack === 30 ? 0 : null,
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

      // Price target direction.
      // yfinance/upgrades_downgrades doesn't expose numeric price targets, so ptFrom/ptTo are
      // structural stubs (null). ptDirection is derived from action semantics:
      //   INITIATED — new coverage (initiated/init action)
      //   UNCHANGED — reiteration/maintain with no rating change
      //   null     — signal genuinely unknown
      const ptFrom: null = null;
      const ptTo: null = null;
      const ptDirection: string | null =
        signal === "INITIATED" ? "INITIATED" :
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
    if (initiationCount) parts.push(`${initiationCount} INITIATION(s)`);
    const summary = parts.length > 0 ? parts.join(", ") : "NO CHANGES";

    return JSON.stringify({
      ticker,
      windowDays: daysBack,
      netSentiment,
      upgrades: upgradeCount,
      upgrades30d: daysBack === 30 ? upgradeCount : null,
      downgrades: downgradeCount,
      downgrades30d: daysBack === 30 ? downgradeCount : null,
      initiations: initiationCount,
      initiations30d: daysBack === 30 ? initiationCount : null,
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

// ── SEC geographic revenue extraction ─────────────────────────────────────────

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

async function edgarListExhibitsFromIndex(indexUrl: string): Promise<Record<string, unknown>[]> {
  const html = await edgarGetHtml(indexUrl, 500_000);
  if (!html) return [];
  const exhibits: Record<string, unknown>[] = [];
  for (const rowM of html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const rowHtml = rowM[1];
    const cells = [...rowHtml.matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)].map((m) => m[1]);
    if (cells.length < 3) continue;
    const sequence = stripHtmlTags(cells[0]).trim();
    if (!/^\d/.test(sequence)) continue;
    const hrefMatch = cells[2].match(/href=["']([^"']+)["']/i);
    const document = hrefMatch
      ? hrefMatch[1].trim().split("/").pop()?.split("?", 1)[0].split("#", 1)[0] ?? ""
      : stripHtmlTags(cells[2] ?? "").trim();
    exhibits.push({
      sequence,
      description: stripHtmlTags(cells[1] ?? "").trim(),
      document,
      type: stripHtmlTags(cells[3] ?? "").trim(),
      size: stripHtmlTags(cells[4] ?? "").trim(),
    });
  }
  return exhibits;
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

  const fixtureCik = SMOKE_TICKER_CIK_FALLBACKS[key];
  if (fixtureCik) {
    filingCikCache.set(key, fixtureCik);
    return fixtureCik;
  }

  const extractCik = (text: string): string | null => {
    const patterns = [/CIK=(\d{1,10})/i, /\/CIK0*([1-9]\d{0,9})\.json/i, /\/edgar\/data\/0*([1-9]\d{0,9})\//i];
    for (const p of patterns) {
      const m = text.match(p);
      if (m) return m[1].padStart(10, "0");
    }
    return null;
  };

  const atomUrls = [
    `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${encodeURIComponent(ticker)}&type=&dateb=&owner=include&count=10&output=atom`,
    `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=${encodeURIComponent(ticker)}&CIK=&type=&dateb=&owner=include&count=10&output=atom`,
  ];

  try {
    for (const atomUrl of atomUrls) {
      const atom = await fetch(atomUrl, { headers: { "User-Agent": EDGAR_UA } });
      if (!atom.ok) {
        await atom.body?.cancel();
        continue;
      }
      const text = await atom.text();
      const cik = extractCik(text);
      if (cik) {
        filingCikCache.set(key, cik);
        return cik;
      }
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

type ResolvedSecFiling = {
  ticker: string;
  cikPadded: string;
  cikInt: number;
  requestedFilingType: string;
  filingType: string;
  filingDate: string | null;
  acceptedAt: string | null;
  accessionNumber: string;
  primaryDocument: string;
  documentUrl: string;
  availableFilingTypes: string[];
  suggestedFilingTypes: string[];
  warnings: Record<string, unknown>[];
};

function uniqueRecentForms(forms: unknown[], limit = 12): string[] {
  const out: string[] = [];
  for (const form of forms) {
    const value = String(form ?? "").toUpperCase();
    if (value && !out.includes(value)) out.push(value);
    if (out.length >= limit) break;
  }
  return out;
}

function filingNotFoundPayload(ticker: string, requestedFilingType: string, availableFilingTypes: string[]): Record<string, unknown> {
  const suggestedFilingTypes = requestedFilingType.toUpperCase() === "10-K" && availableFilingTypes.includes("20-F") ? ["20-F"] : [];
  return {
    status: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
    code: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
    ticker,
    requestedFilingType,
    availableFilingTypes,
    suggestedFilingTypes,
    accessionNumber: null,
    filingDate: null,
    documentUrl: null,
    warnings: [{
      code: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
      message: `No ${requestedFilingType} filing found for '${ticker}'.`,
      severity: "error",
    }],
  };
}

function isLikelyXbrlDocumentUrl(url: string): boolean {
  const lower = url.split("?", 1)[0].toLowerCase();
  return lower.endsWith(".xml") || /\/xbrl\//.test(lower) || /(_htm|xbrl|cal|def|lab|pre)\.xml$/.test(lower);
}

async function resolveSecFiling(
  ticker: string,
  requestedFilingType: string = "10-K",
  accessionNumber: string | null = null,
): Promise<{ ok: true; filing: ResolvedSecFiling } | { ok: false; error: Record<string, unknown> }> {
  const requested = (requestedFilingType || "10-K").toUpperCase();
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return { ok: false, error: { status: "TICKER_NOT_FOUND", code: "TICKER_NOT_FOUND", ticker, message: `Could not resolve EDGAR submissions for ticker '${ticker}'` } };
  }

  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form as string[]) ?? [];
  const accessions = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs = (recent.primaryDocument as string[]) ?? [];
  const filingDates = (recent.filingDate as string[]) ?? [];
  const acceptedDts = (recent.acceptanceDateTime as string[]) ?? [];
  const availableFilingTypes = uniqueRecentForms(forms);

  let targetIdx = -1;
  const warnings: Record<string, unknown>[] = [];
  if (accessionNumber) {
    targetIdx = accessions.findIndex((a) => a === accessionNumber);
  } else {
    targetIdx = forms.findIndex((f) => String(f).toUpperCase() === requested);
    if (targetIdx < 0 && requested === "10-K") {
      const fallbackIdx = forms.findIndex((f) => String(f).toUpperCase() === "20-F");
      if (fallbackIdx >= 0) {
        targetIdx = fallbackIdx;
        warnings.push({
          code: "AUTO_20F_FALLBACK",
          message: "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
          severity: "info",
        });
      }
    }
  }

  if (targetIdx < 0 || !accessions[targetIdx]) {
    return { ok: false, error: filingNotFoundPayload(ticker, requested, availableFilingTypes) };
  }

  const primaryDocument = String(primaryDocs[targetIdx] ?? "");
  if (!primaryDocument) {
    return {
      ok: false,
      error: {
        status: "FILING_TEXT_NOT_AVAILABLE",
        code: "FILING_TEXT_NOT_AVAILABLE",
        ticker,
        requestedFilingType: requested,
        filingType: String(forms[targetIdx] ?? requested),
        filingDate: filingDates[targetIdx] ?? null,
        accessionNumber: accessions[targetIdx],
        documentUrl: null,
        availableFilingTypes,
        suggestedFilingTypes: [],
        warnings: [{ code: "PRIMARY_DOCUMENT_MISSING", message: "SEC submissions entry has no primaryDocument.", severity: "error" }],
      },
    };
  }

  const cikInt = parseInt(cikPadded, 10);
  const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(cikInt, accessions[targetIdx], primaryDocument);
  if (!edgarPrimaryDocumentUrl || isLikelyXbrlDocumentUrl(edgarPrimaryDocumentUrl)) {
    return {
      ok: false,
      error: {
        status: "FILING_TEXT_NOT_AVAILABLE",
        code: "FILING_TEXT_NOT_AVAILABLE",
        ticker,
        requestedFilingType: requested,
        filingType: String(forms[targetIdx] ?? requested),
        filingDate: filingDates[targetIdx] ?? null,
        accessionNumber: accessions[targetIdx],
        documentUrl: edgarPrimaryDocumentUrl,
        availableFilingTypes,
        suggestedFilingTypes: [],
        warnings: [{ code: "PRIMARY_HTML_NOT_FOUND", message: "Could not resolve a human-readable primary filing HTML document.", severity: "error" }],
      },
    };
  }

  return {
    ok: true,
    filing: {
      ticker,
      cikPadded,
      cikInt,
      requestedFilingType: requested,
      filingType: String(forms[targetIdx] ?? requested),
      filingDate: filingDates[targetIdx] ?? null,
      acceptedAt: acceptedDts[targetIdx] ?? null,
      accessionNumber: accessions[targetIdx],
      primaryDocument,
      documentUrl: edgarPrimaryDocumentUrl,
      availableFilingTypes,
      suggestedFilingTypes: requested === "10-K" && String(forms[targetIdx]).toUpperCase() === "20-F" ? ["20-F"] : [],
      warnings,
    },
  };
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
  return decoded.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function sanitizeFilingHtml(html: string): string {
  return html
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script[^>]*>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style[^>]*>/gi, " ")
    .replace(/\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, " ");
}

function htmlToReadableText(html: string): string {
  const blockBroken = sanitizeFilingHtml(html)
    .replace(/<(?:br|\/p|\/div|\/li|\/tr|\/h[1-6]|\/section)\b[^>]*>/gi, "\n")
    .replace(/<(?:p|div|li|tr|h[1-6]|section)\b[^>]*>/gi, "\n");
  const withoutTags = blockBroken.replace(/<[^>]+>/g, " ");
  const entityMap: Record<string, string> = {
    "&nbsp;": " ",
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&apos;": "'",
  };
  const decoded = withoutTags.replace(/&(?:nbsp|amp|lt|gt|quot|apos|#\d+|[a-z]+);/gi, (entity) => {
    if (entity in entityMap) return entityMap[entity];
    if (entity.startsWith("&#")) {
      const code = parseInt(entity.slice(2, -1), 10);
      return Number.isNaN(code) ? " " : String.fromCharCode(code);
    }
    return " ";
  });
  return decoded
    .replace(/[^\S\n]+/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function filterParagraphsByTopics(text: string, topics: string[], maxChars = 1000): Record<string, unknown>[] {
  if (!topics.length) return [];
  const paragraphs = text
    .split(/\n\s*\n|\n/)
    .map((paragraph) => paragraph.trim())
    .filter((paragraph) => paragraph.length > 20);
  const topicPatterns = topics.map((topic) => ({
    topic,
    pattern: new RegExp(topic.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i"),
  }));
  const results: Record<string, unknown>[] = [];
  for (const paragraph of paragraphs) {
    const matchedTopics = topicPatterns
      .filter(({ pattern }) => pattern.test(paragraph))
      .map(({ topic }) => topic);
    if (matchedTopics.length) {
      results.push({
        paragraph: paragraph.length > maxChars ? `${paragraph.slice(0, maxChars).trimEnd()}...` : paragraph,
        matchedTopics,
      });
    }
  }
  return results;
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

function buildXbrlFactContext(fact: Record<string, unknown>, concept: string, periodMode: string): Record<string, unknown> {
  let durationDays: number | null = null;
  if (fact.start && fact.end) {
    try {
      const d0 = new Date(String(fact.start));
      const d1 = new Date(String(fact.end));
      if (!isNaN(d0.getTime()) && !isNaN(d1.getTime())) {
        durationDays = Math.round((d1.getTime() - d0.getTime()) / (1000 * 60 * 60 * 24));
      }
    } catch { /* ignore */ }
  }
  const segmentLabel = normalizeSegmentLabel(fact.segment);
  const dimensions: Record<string, unknown> = {};
  if (segmentLabel) dimensions.segment = segmentLabel;
  if (fact.segment != null) dimensions.rawSegment = fact.segment;
  return {
    concept,
    taxonomy: "us-gaap",
    unit: "USD",
    periodStart: fact.start ?? null,
    periodEnd: fact.end ?? null,
    instant: fact.start ? null : (fact.end ?? null),
    durationDays,
    fiscalPeriod: String(fact.fp ?? ""),
    fiscalYear: String(fact.fy ?? ""),
    form: String(fact.form ?? ""),
    frame: fact.frame ?? null,
    accessionNumber: fact.accn ?? null,
    filedAt: fact.filed ?? null,
    periodMode,
    dimensions,
  };
}

function regionMatches(segmentLabel: string, region: string, includeAsiaFallback = false): boolean {
  const label = segmentLabel.toLowerCase();
  const regionLower = region.toLowerCase();
  if (label.includes(regionLower)) return true;
  // Also try compact (no-space) matching for XBRL member names like "GreaterChinaMember"
  const regionCompact = regionLower.replace(/\s+/g, "");
  if (regionCompact && label.includes(regionCompact)) return true;
  if (regionLower === "china") {
    if (["country:cn", "greater china", "srt:chinamember", "greaterchina"].some((t) => label.includes(t))) return true;
    return includeAsiaFallback && label.includes("asiapacificmember");
  }
  if (regionLower === "greater china") {
    if (label.includes("greaterchina") || label.includes("greater china")) return true;
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
  periodMode = "auto",
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
      status: payload.status ?? (payload.value != null ? "FOUND" : (payload.confidence ?? "NOT_DISCLOSED")),
      code: payload.code ?? null,
      xbrlContext: payload.xbrlContext ?? null,
      warnings,
    };
    if (
      shaped.value != null
      && shaped.extractionMethod === "XBRL"
      && shaped.xbrlContext == null
      && !(shaped.warnings as Record<string, unknown>[]).some((w) => w.code === "XBRL_CONTEXT_METADATA_UNAVAILABLE")
    ) {
      (shaped.warnings as unknown[]).push({
        code: "XBRL_CONTEXT_METADATA_UNAVAILABLE",
        message: "XBRL fact value was found, but SEC context metadata was unavailable.",
        severity: "warning",
      });
    }
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
    if (factType !== "geographic_revenue") {
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
    // For geographic_revenue, fall through to HTML fallback below (picked remains null)
  }
  if (filtered.length && period === "latest") {
    const latestFiled = filtered.map((f) => String(f.filed ?? "")).sort().slice(-1)[0];
    filtered = filtered.filter((f) => String(f.filed ?? "") === latestFiled);
  }

  // ── period_mode filtering ─────────────────────────────────────────────
  // XBRL facts include both quarterly and YTD figures for the same filing.
  // Filter by duration to avoid returning 6-month revenue as quarterly.
  let resolvedMode = (periodMode || "auto").toLowerCase().trim();
  if (resolvedMode === "auto") {
    resolvedMode = filingType.toUpperCase() === "10-Q" ? "quarter" : "annual";
  }
  if ((resolvedMode === "quarter" || resolvedMode === "annual" || resolvedMode === "ytd") && filtered.length) {
    const durationDays = (f: Record<string, unknown>): number | null => {
      const s = f.start as string | undefined;
      const e = f.end as string | undefined;
      if (!s || !e) return null;
      try {
        const d0 = new Date(s);
        const d1 = new Date(e);
        if (isNaN(d0.getTime()) || isNaN(d1.getTime())) return null;
        return Math.round((d1.getTime() - d0.getTime()) / (1000 * 60 * 60 * 24));
      } catch {
        return null;
      }
    };
    const tagged = filtered.map((f) => ({ f, d: durationDays(f) }));
    let modeFiltered: Record<string, unknown>[];
    if (resolvedMode === "quarter") {
      modeFiltered = tagged.filter(({ d }) => d == null || (d >= 60 && d <= 110)).map(({ f }) => f);
    } else if (resolvedMode === "ytd") {
      modeFiltered = tagged.filter(({ f, d }) => {
        if (d == null) return true;
        const fp = String(f.fp ?? "").toUpperCase().trim();
        if (fp === "Q1") return d >= 60 && d <= 110;
        if (fp === "Q2") return d >= 150 && d <= 200;
        if (fp === "Q3") return d >= 240 && d <= 290;
        const form = String(f.form ?? "").toUpperCase().trim();
        if (form === "10-K") return d >= 340 && d <= 400;
        return d >= 60;
      }).map(({ f }) => f);
    } else {
      modeFiltered = tagged.filter(({ d }) => d == null || (d >= 340 && d <= 400)).map(({ f }) => f);
    }
    if (modeFiltered.length) {
      filtered = modeFiltered;
    }
  }
  if (period === "latest" && filtered.length) {
    filtered = [...filtered].sort((a, b) => {
      const endCmp = String(b.end ?? "").localeCompare(String(a.end ?? ""));
      if (endCmp !== 0) return endCmp;
      const fyCmp = Number(b.fy ?? 0) - Number(a.fy ?? 0);
      if (fyCmp !== 0) return fyCmp;
      return String(b.filed ?? "").localeCompare(String(a.filed ?? ""));
    });
  }

  if (factType === "segment_revenue") {
    const allSegments = filtered
      .map((f) => {
        const segmentLabel = normalizeSegmentLabel(f.segment);
        if (!segmentLabel) return null;
        const xbrlContext = buildXbrlFactContext(f, concept, resolvedMode);
        return {
          segmentLabel,
          value: f.val ?? null,
          fiscalYear: String(f.fy ?? ""),
          fiscalPeriod: String(f.fp ?? ""),
          filingDate: String(f.filed ?? ""),
          accessionNumber: String(f.accn ?? ""),
          xbrlContext,
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
      xbrlContext: allSegments[0]?.xbrlContext ?? null,
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
      const resolved = await resolveSecFiling(ticker, filingType, null);
      if (!resolved.ok) {
        return withGeoShape({
          ...resolved.error,
          factType,
          region,
          value: null,
          denominator: null,
          valueRatio: null,
          valuePct: null,
          extractionMethod: "NONE",
          source: resolved.error.code,
          confidence: resolved.error.code,
          filingType,
          evidence: {},
        });
      }
      const filing = resolved.filing;
      const htmlText = await edgarGetHtml(filing.documentUrl, 5_000_000);
      if (htmlText) {
        const geo = extractGeoRevenueFromHtml(htmlText, region ?? "");
        if (geo) {
          const reportDate = filing.filingDate ?? "";
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
            filingType: filing.filingType,
            filingDate: filing.filingDate,
            accessionNumber: filing.accessionNumber,
            documentUrl: filing.documentUrl,
            indexUrl: null,
            primaryDocumentUrl: filing.documentUrl,
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
            warnings: [...warnings, ...filing.warnings],
          });
        }
        const readable = stripHtmlTags(htmlText).toLowerCase();
        const regionText = String(region ?? "").toLowerCase();
        if (regionText && readable.includes(regionText) && /revenue|sales|geographic|segment/.test(readable)) {
          return withGeoShape({
            ticker,
            factType,
            region,
            value: null,
            denominator: null,
            valueRatio: null,
            valuePct: null,
            extractionMethod: "NONE",
            source: "EXTRACTION_FAILED",
            confidence: "EXTRACTION_FAILED",
            status: "EXTRACTION_FAILED",
            code: "EXTRACTION_FAILED",
            filingType: filing.filingType,
            filingDate: filing.filingDate,
            accessionNumber: filing.accessionNumber,
            documentUrl: filing.documentUrl,
            evidence: {},
            warnings: [...filing.warnings, { code: "TABLE_NOT_PARSED", message: "Relevant filing text exists, but no geographic revenue table was parsed.", severity: "warning" }],
          });
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

  // ── XBRL context metadata ─────────────────────────────────────────────
  let durationDaysVal: number | null = null;
  if (picked.start && picked.end) {
    try {
      const d0 = new Date(String(picked.start));
      const d1 = new Date(String(picked.end));
      if (!isNaN(d0.getTime()) && !isNaN(d1.getTime())) {
        durationDaysVal = Math.round((d1.getTime() - d0.getTime()) / (1000 * 60 * 60 * 24));
      }
    } catch { /* ignore */ }
  }
  const xbrlContext = {
    concept,
    taxonomy: "us-gaap",
    unit: "USD",
    periodStart: picked.start ?? null,
    periodEnd: picked.end ?? null,
    instant: picked.start ? null : (picked.end ?? null),
    durationDays: durationDaysVal,
    fiscalPeriod: String(picked.fp ?? ""),
    fiscalYear: String(picked.fy ?? ""),
    form: String(picked.form ?? ""),
    frame: picked.frame ?? null,
    accessionNumber: picked.accn ?? null,
    filedAt: picked.filed ?? null,
    periodMode: resolvedMode,
    dimensions: {
      ...(segmentLabel ? { segment: segmentLabel } : {}),
      ...(picked.segment != null ? { rawSegment: picked.segment } : {}),
    },
  };

  const resultWarnings: Record<string, unknown>[] = [];
  if (resolvedMode === "quarter" && durationDaysVal != null && durationDaysVal > 110) {
    resultWarnings.push({
      code: "PERIOD_MODE_MISMATCH",
      message: `Requested quarter but picked fact has ${durationDaysVal}-day duration. No quarterly fact available.`,
      severity: "warning",
    });
  }

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
    xbrlContext,
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
    warnings: resultWarnings,
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
  documentUrl: string | null = null,
): Promise<string> {
  const warnings: Record<string, unknown>[] = [];
  let edgarPrimaryDocumentUrl: string | null = documentUrl;
  let filingDate: string | null = null;
  let fiscalYear: string | null = null;
  let actualFilingType = filingType;

  if (documentUrl && isLikelyXbrlDocumentUrl(documentUrl)) {
    if (!accessionNumber) {
      return JSON.stringify({
        ticker,
        accessionNumber,
        documentUrl,
        fiscalYear: null,
        filingType,
        filingDate: null,
        documentKind: "xbrl_xml",
        matches: [],
        matchCount: 0,
        status: "FILING_TEXT_NOT_AVAILABLE",
        code: "FILING_TEXT_NOT_AVAILABLE",
        confidence: "FILING_TEXT_NOT_AVAILABLE",
        warnings: [{ code: "FILING_TEXT_NOT_AVAILABLE", message: "Provided document_url appears to be XBRL/XML and no accession_number was supplied to resolve primary HTML.", severity: "error" }],
      });
    }
    edgarPrimaryDocumentUrl = null;
    warnings.push({ code: "DOCUMENT_URL_REPLACED_WITH_PRIMARY_HTML", message: "Provided document_url was XBRL/XML; resolved the accession primary HTML document instead.", severity: "warning" });
  }

  if (!edgarPrimaryDocumentUrl) {
    const resolved = await resolveSecFiling(ticker, filingType, accessionNumber);
    if (!resolved.ok) {
      return JSON.stringify({
        ...resolved.error,
        accessionNumber: resolved.error.accessionNumber ?? accessionNumber,
        documentUrl: resolved.error.documentUrl ?? null,
        fiscalYear: null,
        filingType,
        filingDate: resolved.error.filingDate ?? null,
        documentKind: "unavailable",
        matches: [],
        matchCount: 0,
        confidence: resolved.error.code,
      });
    }
    const filing = resolved.filing;
    edgarPrimaryDocumentUrl = filing.documentUrl;
    accessionNumber = filing.accessionNumber;
    filingDate = filing.filingDate;
    fiscalYear = filing.filingDate ? `FY${String(filing.filingDate).slice(0, 4)}` : null;
    actualFilingType = filing.filingType;
    warnings.push(...filing.warnings);
  }

  if (!edgarPrimaryDocumentUrl) {
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: null,
      fiscalYear,
      filingType: actualFilingType,
      filingDate,
      documentKind: "unavailable",
      matches: [],
      matchCount: 0,
      status: "FILING_TEXT_NOT_AVAILABLE",
      code: "FILING_TEXT_NOT_AVAILABLE",
      confidence: "FILING_TEXT_NOT_AVAILABLE",
      warnings: [...warnings, { code: "FILING_TEXT_NOT_AVAILABLE", message: "Could not resolve primary filing HTML.", severity: "error" }],
    });
  }

  if (isLikelyXbrlDocumentUrl(edgarPrimaryDocumentUrl)) {
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: edgarPrimaryDocumentUrl,
      fiscalYear: null,
      filingType: actualFilingType,
      filingDate: null,
      documentKind: "xbrl_xml",
      matches: [],
      matchCount: 0,
      status: "FILING_TEXT_NOT_AVAILABLE",
      code: "FILING_TEXT_NOT_AVAILABLE",
      confidence: "FILING_TEXT_NOT_AVAILABLE",
      warnings: [{ code: "FILING_TEXT_NOT_AVAILABLE", message: "Only an XBRL/XML document could be resolved; refusing to return tag soup as filing text.", severity: "error" }],
    });
  }

  const html = await edgarGetHtml(edgarPrimaryDocumentUrl, 5_000_000);
  if (!html) {
    return JSON.stringify({
      ticker,
      accessionNumber,
      documentUrl: edgarPrimaryDocumentUrl,
      fiscalYear,
      filingType: actualFilingType,
      filingDate,
      documentKind: "primary_html",
      matches: [],
      matchCount: 0,
      status: "FILING_TEXT_NOT_AVAILABLE",
      code: "FILING_TEXT_NOT_AVAILABLE",
      confidence: "FILING_TEXT_NOT_AVAILABLE",
      warnings: [...warnings, { code: "FILING_TEXT_NOT_AVAILABLE", message: "Unable to fetch primary filing HTML.", severity: "error" }],
    });
  }

  const cleanedHtml = sanitizeFilingHtml(html);
  const readableText = cleanFilingDisplayText(htmlToReadableText(cleanedHtml));
  const textLower = readableText.toLowerCase();
  const htmlLower = cleanedHtml.toLowerCase();
  const size = Math.max(200, Math.min(Math.floor(contextChars), 4000));
  const matches: Record<string, unknown>[] = [];
  const seen = new Set<number>();

  const addMatch = (term: string, pos: number) => {
    if ([...seen].some((p) => Math.abs(p - pos) < 150)) return;
    seen.add(pos);
    const start = Math.max(0, pos - Math.floor(size / 2));
    const end = Math.min(readableText.length, pos + Math.floor(size / 2));
    const contextText = cleanFilingDisplayText(readableText.slice(start, end));
    const preText = readableText.slice(Math.max(0, pos - 2_000), pos);
    const sectionHeading = (preText.match(/(?:Item\s+\d+[A-Z]?\.?\s+[^.]{3,120}|[A-Z][A-Z0-9 ,&/-]{12,120})\s*$/) ?? [""])[0].trim();
    const match: Record<string, unknown> = {
      term,
      sectionHeading,
      contextText,
      confidence: "LOW",
    };
    if (returnTables) {
      const tableParsed: Record<string, unknown>[] = [];
      const htmlPos = htmlLower.indexOf(term.toLowerCase());
      const tableWindow = htmlPos >= 0
        ? cleanedHtml.slice(Math.max(0, htmlPos - 12_000), Math.min(cleanedHtml.length, htmlPos + 12_000))
        : "";
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
    const pos = textLower.indexOf(sectionHint.toLowerCase());
    if (pos >= 0) addMatch(sectionHint, pos);
  }
  for (const term of searchTerms) {
    let idx = 0;
    const termLower = term.toLowerCase();
    while (matches.length < 10) {
      const pos = textLower.indexOf(termLower, idx);
      if (pos < 0) break;
      addMatch(term, pos);
      idx = pos + 1;
    }
  }

  return JSON.stringify({
    ticker,
    accessionNumber,
    documentUrl: edgarPrimaryDocumentUrl,
    fiscalYear,
    filingType: actualFilingType,
    filingDate,
    documentKind: "primary_html",
    matches,
    matchCount: matches.length,
    confidence: matches.length === 0 ? "NOT_DISCLOSED" : (matches.some(m => ((m.tableParsed as unknown[]) ?? []).length > 0) ? "HIGH" : "MEDIUM"),
    warnings: [...warnings, ...(matches.length > 0 ? [{
      code: "RAW_FILING_TEXT",
      message: "Returned text is sanitized filing context, not structured fact extraction.",
      severity: "info",
    }] : [])],
  });
}

function cleanFilingDisplayText(text: string): string {
  return text
    .replace(/<[^>]+>/g, " ")
    .replace(/\b(?:contextRef|unitRef|decimals|id|name|class|style|href)=("[^"]*"|'[^']*'|[^\s]+)/gi, " ")
    .replace(/\b(?:xbrli|ix|dei|us-gaap|srt|country):[A-Za-z0-9_.:-]+\b/g, " ")
    .replace(/https?:\/\/\S+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function looksLikeFilingMarkupText(text: string): boolean {
  return /[<>]=?|(?:^|\s)(?:contextRef|unitRef|decimals|style|class|id|name)=|(?:xbrli|ix|dei|us-gaap|srt|country):|https?:\/\//i.test(text);
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

// ── SEC filing text search ────────────────────────────────────────────────────

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

// ── SEC filing document/section retrieval ─────────────────────────────────────

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
    const fullOptions = await yGetFullOptions(ticker);
    const calls = fullOptions.calls.map(normalizeContractIv);
    const puts = fullOptions.puts.map(normalizeContractIv);
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
    const allContracts = [...calls, ...puts];

    let maxPainStrike: number | null = null;
    const scanWarnings: string[] = [];
    if (totalCallOI + totalPutOI <= 0 || majorityZeroOpenInterest(allContracts)) {
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
      if (!isPlaceholderIv(rawAtmIv)) {
        atmIv = rawAtmIv;
      } else {
        scanWarnings.push("ATM_IV_PLACEHOLDER");
      }
    }

    // dataQuality
    const dataQuality = computeDataQuality(allContracts, getLastTradingDate());
    const quality = dataQuality.quality;
    const allContractCount = calls.length + puts.length;
    const placeholderIvCount = dataQuality.placeholderIvCount;

    let ivPctile: number | null = null;
    let chartTimestamps: number[] = [];
    if (quality === "LOW" && placeholderIvCount > allContractCount * 0.5) {
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
      formattedBlock = "OPTIONS FLOW: DATA QUALITY LOW — raw chain unreliable; bracket not assigned.";
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
    return JSON.stringify({
      error: true,
      message: "reference_target_price (or io_pt alias) must be a positive number",
      ticker,
    });
  }
  try {
    const fi = JSON.parse(await getFastInfo(ticker)) as Record<string, unknown>;
    const currentPrice = fi.lastPrice as number | null;
    if (currentPrice == null) {
      return JSON.stringify({ error: true, message: `No price data for ${ticker}`, ticker });
    }

    const referenceTargetPct = +(currentPrice / ioPt * 100).toFixed(1);

    const bracket =
      referenceTargetPct <= 75 ? "STRONG_BUY" :
      referenceTargetPct <= 90 ? "ACCEPTABLE" :
      referenceTargetPct <= 100 ? "CAUTION" : "AVOID";

    const inferredTag =
      referenceTargetPct < 40 ? "SPECULATIVE" :
      referenceTargetPct < 80 ? "LONG" :
      referenceTargetPct < 100 ? "NEAR" : "INVERTED";

    return JSON.stringify({
      ticker,
      currentPrice: +currentPrice.toFixed(4),
      referenceTargetPrice: ioPt,
      referenceTargetPct,
      ioPt,
      eqfPct: referenceTargetPct,
      bracket,
      inferredTag,
      tag: inferredTag,
      tagNote: "Deprecated: tag is inferred from currentPrice/referenceTargetPrice distance. Use inferredTag.",
      invertedFlag: referenceTargetPct >= 100,
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

    const t1 = {
      analystNetSentiment: upgrade.netSentiment ?? null,
      upgrades30d: upgrade.upgrades30d ?? upgrade.upgrades ?? null,
      downgrades30d: upgrade.downgrades30d ?? upgrade.downgrades ?? null,
      initiations30d: upgrade.initiations30d ?? upgrade.initiations ?? null,
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
      avgSurprisePct: earnings.preRevenue ? null : (earnings.avgSurprisePct ?? null),
      momentumFlag: earnings.momentumFlag ?? null,
      preRevenue: Boolean(earnings.preRevenue),
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

// ── freshness classifier ──────────────────────────────────────────────────────

function classifyFreshness(dataDate: string | null, retrievedAt: string): string {
  if (!dataDate) return "UNKNOWN";
  try {
    const now = new Date(retrievedAt);
    // Approximate US market close as 21:00 UTC (4pm ET / 5pm EDT)
    const data = new Date(dataDate + "T21:00:00Z");
    const diffMs = now.getTime() - data.getTime();
    if (diffMs < 0) return "UNKNOWN"; // future date
    const diffHours = diffMs / (1000 * 60 * 60);
    // JS getUTCDay(): 0=Sunday, 1=Monday, ..., 5=Friday, 6=Saturday
    const nowDay = now.getUTCDay();  // 0=Sun, 6=Sat
    const dataDay = data.getUTCDay(); // 5=Fri
    // Weekend: current day is Saturday(6) or Sunday(0), data is from last Friday(5)
    if ((nowDay === 6 || nowDay === 0) && dataDay === 5 && diffHours <= 72) {
      return "WEEKEND_EXPECTED_STALE";
    }
    if (diffHours <= 28) return "FRESH";
    if (diffHours <= 56) return "MARKET_CLOSED_EXPECTED_STALE";
    if (diffHours <= 168) return "STALE";
    return "VERY_STALE";
  } catch {
    return "UNKNOWN";
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
        note = `Volume gate ${gatePass ? "PASS" : "FAIL"} (FX notional) — $${(dailyNotionalUSD / 1_000_000).toFixed(1)}M daily notional (${gatePass ? "≥" : "<"} $10M threshold)${fxConversionNote}`;
      } else {
        note = "Volume gate UNKNOWN — insufficient price/volume data for FX notional check";
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

// ── get_market_snapshot ───────────────────────────────────────────────────────

export async function getMarketSnapshot(
  ticker: string | string[],
  mode: "compact" | "full",
  foreignExchange: boolean
): Promise<string> {
  if (Array.isArray(ticker)) {
    const cap = mode === "full" ? 2 : 5;
    const limited = ticker.slice(0, cap);
    const results: Record<string, unknown> = {};
    for (const t of limited) {
      try {
        results[t] = JSON.parse(await getMarketSnapshot(t, mode, foreignExchange));
      } catch (e) {
        results[t] = { error: true, message: e instanceof Error ? e.message : String(e) };
      }
    }
    return JSON.stringify({
      tickers: results,
      truncated: ticker.length > cap,
      ...(ticker.length > cap ? { droppedTickers: ticker.slice(cap) } : {}),
    });
  }

  const retrievedAt = new Date().toISOString();

  const [quoteResult, priceStatsResult, maResult, volumeRatioResult, volumeGateResult, techResult] =
    await Promise.allSettled([
      getFastInfo(ticker).then((r) => JSON.parse(r) as Record<string, unknown>),
      getPriceStats(ticker).then((r) => JSON.parse(r) as Record<string, unknown>),
      getMaPosition(ticker).then((r) => JSON.parse(r) as Record<string, unknown>),
      getVolumeRatio(ticker, 10).then((r) => JSON.parse(r) as Record<string, unknown>),
      getVolumeGate(ticker, foreignExchange).then((r) => JSON.parse(r) as Record<string, unknown>),
      getTechnicalIndicators(ticker, "3mo").then((r) => JSON.parse(r) as Record<string, unknown>),
    ]);

  const componentStatus: Record<string, string> = {};
  const failedComponents: string[] = [];
  const warnings: Record<string, unknown>[] = [];

  const getVal = (
    result: PromiseSettledResult<Record<string, unknown>>,
    name: string
  ): Record<string, unknown> | null => {
    if (result.status === "fulfilled" && !result.value?.error) {
      componentStatus[name] = "OK";
      return result.value;
    }
    componentStatus[name] = "FAILED";
    failedComponents.push(name);
    const msg =
      result.status === "rejected"
        ? String(result.reason)
        : String((result.value as Record<string, unknown>)?.message ?? "error in response");
    warnings.push({ code: "COMPONENT_FAILED", component: name, message: msg });
    return null;
  };

  const quote = getVal(quoteResult, "quote");
  const priceStats = getVal(priceStatsResult, "priceStats");
  const ma = getVal(maResult, "maPosition");
  const volumeRatio = getVal(volumeRatioResult, "volumeRatio");
  const volumeGate = getVal(volumeGateResult, "volumeGate");
  const tech = getVal(techResult, "technicalIndicators");

  const dataDate =
    (quote?.lastTradeDate as string | null) ??
    (priceStats?.dataDate as string | null) ??
    null;

  const lastPrice = (quote?.lastPrice as number | null) ?? null;
  const prevClose = (quote?.previousClose as number | null) ?? null;
  const changePct =
    (priceStats?.pctChangeTodayVsPrevClose as number | null) ??
    (lastPrice != null && prevClose != null && prevClose !== 0
      ? +((lastPrice - prevClose) / prevClose * 100).toFixed(2)
      : null);

  const snapshot: Record<string, unknown> = {
    ticker,
    price: {
      last: lastPrice,
      previousClose: prevClose,
      changePct,
      lastTradeDate: dataDate,
      marketOpen: (quote?.marketOpen as boolean | null) ?? null,
    },
    range: {
      yearHigh: (quote?.yearHigh as number | null) ?? null,
      yearLow: (quote?.yearLow as number | null) ?? null,
      pctFromYearHigh: (priceStats?.pctFromYearHigh as number | null) ?? null,
      pctFromYearLow: (priceStats?.pctFromYearLow as number | null) ?? null,
    },
    trend: {
      fiftyDayAverage: (quote?.fiftyDayAverage as number | null) ?? null,
      twoHundredDayAverage: (quote?.twoHundredDayAverage as number | null) ?? null,
      pctFrom50dma: (ma?.pctVs50dma as number | null) ?? null,
      pctFrom200dma: (ma?.pctVs200dma as number | null) ?? null,
      maTrend: (ma?.trend as string | null) ?? null,
      rsi14: (tech?.rsi14 as number | null) ?? null,
      macdHistogram: (tech?.macdHistogram as number | null) ?? null,
    },
    volume: {
      lastVolume: (quote?.lastVolume as number | null) ?? null,
      avgVolume10d: (quote?.tenDayAverageVolume as number | null) ?? null,
      avgVolume20d: (volumeGate?.adv20d as number | null) ?? null,
      avgVolume90d: (quote?.threeMonthAverageVolume as number | null) ?? null,
      ratio10d: (volumeRatio?.ratio10d as number | null) ?? null,
      ratio20d: (volumeGate?.ratio20d as number | null) ?? null,
      ratio90d: (volumeRatio?.ratio90d as number | null) ?? null,
      volumeFlag: (volumeRatio?.volumeFlag as string | null) ?? null,
      liquidityGatePass: (volumeGate?.gatePass as boolean | null) ?? null,
    },
    risk: {
      annualizedVolatility30d: (priceStats?.annualizedVolatility30d as number | null) ?? null,
    },
    freshness: {
      dataDate,
      retrievedAt,
      marketSessionAware: true,
      freshnessClass: classifyFreshness(dataDate, retrievedAt),
    },
    componentStatus,
    partialSuccess: failedComponents.length > 0 && failedComponents.length < 6,
    failedComponents,
    warnings,
  };

  if (mode === "full") {
    snapshot._components = {
      quote,
      priceStats,
      maPosition: ma,
      volumeRatio,
      volumeGate,
      technicalIndicators: tech,
    };
  }

  return JSON.stringify(snapshot);
}

// ── get_options_summary ───────────────────────────────────────────────────────

export async function getOptionsSummary(ticker: string, expiryHint?: string): Promise<string> {
  try {
    const expData = JSON.parse(await getOptionExpirationDates(ticker)) as string[];
    if (!expData || expData.length === 0) {
      return JSON.stringify({ ticker, error: "No options data available" });
    }
    const expiry = expiryHint || expData[0];
    if (!expData.includes(expiry)) {
      return JSON.stringify(invalidExpiryPayload(ticker, expiry, expData));
    }
    // Fetch all contracts without illiquid filtering and with strike sort so we get the full chain
    const callsRaw = JSON.parse(await getOptionChain(ticker, expiry, "calls", 200, 0, 0, null, null, "all", "strike", 20, true)) as Record<string, unknown>;
    const putsRaw = JSON.parse(await getOptionChain(ticker, expiry, "puts", 200, 0, 0, null, null, "all", "strike", 20, true)) as Record<string, unknown>;
    const calls = ((callsRaw.contracts ?? []) as Record<string, unknown>[]).map(normalizeContractIv);
    const puts = ((putsRaw.contracts ?? []) as Record<string, unknown>[]).map(normalizeContractIv);

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
      if (!isPlaceholderIv(rawAtmIV)) {
        atmIV = +Number(rawAtmIV).toFixed(4);
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
    const allContracts = [...calls, ...puts];
    const dataDate = getLastTradingDate();
    const dataQuality = computeDataQuality(allContracts, dataDate);

    let maxPainStrike: number | null = null;
    if (callOI + putOI <= 0 || majorityZeroOpenInterest(allContracts)) {
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

// ── list_sec_company_filings ──────────────────────────────────────────────────

/** Phase 2 canonical listing: returns cik, filingType, acceptedAt, primaryDocument, documentUrl, meta. */
export async function listSecCompanyFilings(ticker: string, filingType: string = "10-K", limit: number = 5): Promise<string> {
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not find EDGAR submissions for ticker '${ticker}'` } });
  }

  const cikInt = parseInt(cikPadded, 10);
  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form as string[]) ?? [];
  const dates = (recent.filingDate as string[]) ?? [];
  const accessions = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs = (recent.primaryDocument as string[]) ?? [];
  const acceptedDts = (recent.acceptanceDateTime as string[]) ?? [];

  const cap = Math.min(Math.max(1, limit), 20);
  const results: Record<string, unknown>[] = [];
  for (let i = 0; i < forms.length && results.length < cap; i++) {
    if ((forms[i] ?? "").toUpperCase() !== filingType.toUpperCase()) continue;
    const acc = accessions[i] ?? "";
    const primaryDoc = primaryDocs[i] ?? "";
    const accClean = acc.replace(/-/g, "");
    const documentUrl = primaryDoc
      ? `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${primaryDoc}`
      : null;
    results.push({
      filingType: forms[i],
      filingDate: dates[i] ?? "",
      acceptedAt: acceptedDts[i] ?? null,
      accessionNumber: acc,
      primaryDocument: primaryDoc,
      documentUrl,
    });
  }

  return JSON.stringify({
    ticker,
    cik: cikPadded,
    filings: results,
    meta: { source: "sec_submissions", retrievedAt: new Date().toISOString() },
  });
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

function isTocMatch(html: string, matchStart: number, matchEnd: number): boolean {
  const slice = html.slice(Math.max(0, matchStart - 30), Math.min(html.length, matchEnd + 30));
  if (/<a\b[^>]*\bhref\s*=\s*['"]#[^'"]*['"]/i.test(slice)) {
    return true;
  }
  const surrBefore = html.slice(Math.max(0, matchStart - 100), matchStart);
  if (/<a\b[^>]*\bhref\s*=\s*['"]#[^'"]*['"][^>]*>\s*$/i.test(surrBefore)) {
    return true;
  }
  const context = html.slice(Math.max(0, matchStart - 150), Math.min(html.length, matchEnd + 150));
  if (context.includes("....") || context.includes(". . .") || context.includes("&#183;") || context.includes("&middot;")) {
    return true;
  }
  const plainContext = context.replace(/<[^>]+>/g, " ");
  if (/\.{3,}\s*\d+|\.\s*\.\s*\.\s*\d+/.test(plainContext)) {
    return true;
  }
  return false;
}

interface SectionBounds {
  startIdx: number | null;
  endIdx: number | null;
  foundHeading: string;
  tocSkipped: boolean;
  errCode: string | null;
}

function findSectionBounds(html: string, section: string, maxChars: number = 50000): SectionBounds {
  const sectionLower = section.toLowerCase().trim();
  const headingRe = /<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi;
  let tocSkipped = false;
  
  const allHeadings: { text: string; start: number; end: number; level: number; rawText: string }[] = [];
  let hMatch: RegExpExecArray | null;
  while ((hMatch = headingRe.exec(html)) !== null) {
    const text = hMatch[2].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().toLowerCase();
    allHeadings.push({
      text,
      start: hMatch.index,
      end: headingRe.lastIndex,
      level: parseInt(hMatch[1], 10),
      rawText: hMatch[2].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim(),
    });
  }
  
  const candidates: { start: number; level: number; rawText: string; hIdx: number }[] = [];
  for (let i = 0; i < allHeadings.length; i++) {
    const h = allHeadings[i];
    if (sectionLower.includes(h.text) || h.text.includes(sectionLower)) {
      if (isTocMatch(html, h.start, h.end)) {
        tocSkipped = true;
        continue;
      }
      candidates.push({ start: h.start, level: h.level, rawText: h.rawText, hIdx: i });
    }
  }
  
  const itemMatches: { start: number; end: number }[] = [];
  if (candidates.length === 0) {
    const escapeRegex = (s: string) => s.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&");
    const itemRe = new RegExp(`(?:<b[^>]*>|<span[^>]*font-weight:\\s*bold[^>]*>)\\s*${escapeRegex(section)}\\b`, "gi");
    let itemMatch: RegExpExecArray | null;
    while ((itemMatch = itemRe.exec(html)) !== null) {
      if (isTocMatch(html, itemMatch.index, itemRe.lastIndex)) {
        tocSkipped = true;
        continue;
      }
      itemMatches.push({ start: itemMatch.index, end: itemRe.lastIndex });
    }
  }
  
  if (candidates.length === 0 && itemMatches.length === 0) {
    return { startIdx: null, endIdx: null, foundHeading: "", tocSkipped, errCode: null };
  }
  
  if (candidates.length > 1 || itemMatches.length > 1) {
    return { startIdx: null, endIdx: null, foundHeading: "", tocSkipped, errCode: "SECTION_AMBIGUOUS" };
  }
  
  if (candidates.length === 1) {
    const c = candidates[0];
    let endPos: number | null = null;
    for (let i = c.hIdx + 1; i < allHeadings.length; i++) {
      if (allHeadings[i].level <= c.level) {
        endPos = allHeadings[i].start;
        break;
      }
    }
    if (endPos === null) {
      endPos = Math.min(c.start + maxChars * 3, html.length);
    }
    return { startIdx: c.start, endIdx: endPos, foundHeading: c.rawText, tocSkipped, errCode: null };
  } else {
    const match = itemMatches[0];
    const escapeRegex = (s: string) => s.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&");
    const itemRe = new RegExp(`(?:<b[^>]*>|<span[^>]*font-weight:\\s*bold[^>]*>)\\s*${escapeRegex(section)}\\b`, "gi");
    const minSpacing = 100;
    const nextStart = match.end + minSpacing;
    let endPos: number | null = null;
    
    itemRe.lastIndex = nextStart;
    let nextMatch: RegExpExecArray | null;
    while ((nextMatch = itemRe.exec(html)) !== null) {
      if (isTocMatch(html, nextMatch.index, itemRe.lastIndex)) {
        itemRe.lastIndex = nextMatch.index + 1;
        continue;
      }
      endPos = nextMatch.index;
      break;
    }
    if (endPos === null) {
      endPos = Math.min(match.start + maxChars * 3, html.length);
    }
    return { startIdx: match.start, endIdx: endPos, foundHeading: section, tocSkipped, errCode: null };
  }
}

export async function getFilingSection(ticker: string, sectionName: string, documentUrl: string, contextChars: number = 3000): Promise<string> {
  try {
    if (!documentUrl.startsWith("https://www.sec.gov/Archives/")) {
      return JSON.stringify({ error: true, message: "Invalid SEC URL" });
    }
    const resp = await fetch(documentUrl, { headers: { "User-Agent": "yahoo-finance-mcp/1.0 admin@example.com" } });
    if (!resp.ok) return JSON.stringify({ error: true, message: `HTTP ${resp.status}` });
    const html = await resp.text();

    const bounds = findSectionBounds(html, sectionName, contextChars);
    if (bounds.errCode === "SECTION_AMBIGUOUS") {
      return mcpFailure("get_filing_section", "SECTION_AMBIGUOUS", "The section heading could not be resolved unambiguously.");
    }

    if (bounds.startIdx !== null && bounds.endIdx !== null) {
      const sectionHtml = html.slice(bounds.startIdx, bounds.endIdx);
      const plainSection = sectionHtml.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
      return JSON.stringify({
        ticker,
        sectionName,
        found: true,
        text: plainSection.slice(0, contextChars),
        sectionStartOffset: bounds.startIdx,
        sectionEndOffset: bounds.endIdx,
        matchedHeading: bounds.foundHeading,
        tocSkipped: bounds.tocSkipped,
      });
    }

    // Fallback to text search
    const text = html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
    const idx = text.toLowerCase().indexOf(sectionName.toLowerCase());
    if (idx === -1) {
      return JSON.stringify({
        ticker,
        sectionName,
        found: false,
        text: null,
        sectionStartOffset: null,
        sectionEndOffset: null,
        matchedHeading: "",
        tocSkipped: bounds.tocSkipped,
      });
    }
    return JSON.stringify({
      ticker,
      sectionName,
      found: true,
      text: text.slice(idx, idx + contextChars),
      sectionStartOffset: idx,
      sectionEndOffset: idx + contextChars,
      matchedHeading: sectionName,
      tocSkipped: bounds.tocSkipped,
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

// ── list_filing_tables ────────────────────────────────────────────────────────

export async function listFilingTables(ticker: string, documentUrl: string, offset: number = 0, limit: number = 50): Promise<string> {
  try {
    if (!documentUrl.startsWith("https://www.sec.gov/Archives/")) {
      return JSON.stringify({ error: true, message: "Invalid SEC URL" });
    }
    const resp = await fetch(documentUrl, { headers: { "User-Agent": "yahoo-finance-mcp/1.0 admin@example.com" } });
    if (!resp.ok) return JSON.stringify({ error: true, message: `HTTP ${resp.status}` });
    const html = await resp.text();

    const tables: { tableIndex: number; rowCount: number; title: string | null; headers: string[] }[] = [];
    const tableRe = /<table[^>]*>([\s\S]*?)<\/table>/gi;
    const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
    let tm: RegExpExecArray | null;
    let idx = 0;
    while ((tm = tableRe.exec(html)) !== null) {
      const tableStart = tm.index;
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
      const preText = stripHtmlTags(html.slice(Math.max(0, tableStart - 500), tableStart));
      const lines = preText.split(/\n| {2,}/).map(l => cleanFilingDisplayText(l.trim())).filter(Boolean);
      const candidate = lines[lines.length - 1] ?? "";
      const title = candidate.length > 10 && candidate.length < 200 && !looksLikeFilingMarkupText(candidate) ? candidate : null;
      tables.push({ tableIndex: idx++, rowCount: rows.length, title, headers });
    }
    const safeOffset = Math.max(0, Math.trunc(offset));
    const safeLimit = Math.min(100, Math.max(1, Math.trunc(limit || 50)));
    const page = tables.slice(safeOffset, safeOffset + safeLimit);
    return JSON.stringify({
      ticker,
      documentUrl,
      tableCount: tables.length,
      returnedCount: page.length,
      offset: safeOffset,
      limit: safeLimit,
      hasMore: safeOffset + page.length < tables.length,
      tables: page,
    });
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
const OFFICIAL_SOURCE_TYPES = new Set(["sec_filing", "company_ir", "press_release", "newswire", "yahoo_finance_press_releases"]);
const SUPPORTED_EVENT_SOURCES = new Set([
  "sec", "company_ir", "newswire",
  "yahoo_finance",                  // legacy: aggregates news + press releases
  "yahoo_finance_news",             // Yahoo Finance general news tab
  "yahoo_finance_press_releases",   // Yahoo Finance press releases tab
  "finnhub",
]);
const PRE_REVENUE_EPS_EPSILON = 1e-9;
const SOURCE_PRIORITY: Record<string, number> = {
  sec_filing: 0,
  company_ir: 1,
  press_release: 2,
  yahoo_finance_press_releases: 2,
  newswire: 3,
  company_news: 4,
  yahoo_finance: 5,
  yahoo_finance_news: 5,
  other: 6,
};

function confidenceForSourceType(sourceType: unknown, fallback: unknown = "LOW"): string {
  const src = _str(sourceType);
  if (src === "sec_filing" || src === "company_ir") return "HIGH";
  if (src === "press_release" || src === "yahoo_finance_press_releases" || src === "newswire") return "MEDIUM";
  if (src === "company_news" || src === "yahoo_finance_news" || src === "yahoo_finance" || src === "finnhub") return "LOW";
  return _str(fallback, "LOW");
}

function clampInt(value: number, fallback: number, min: number, max: number): number {
  const n = Number.isFinite(value) ? Math.trunc(value) : fallback;
  return Math.min(max, Math.max(min, n));
}

function normalizeSources(sources: string[] | undefined, defaults: string[]): { selected: string[]; warnings: Record<string, unknown>[] } {
  const warnings: Record<string, unknown>[] = [];
  const input = (sources && sources.length > 0 ? sources : defaults).map(s => _str(s).toLowerCase().trim()).filter(Boolean);
  const seen = new Set<string>();
  const selected: string[] = [];
  for (const src of input) {
    if (!SUPPORTED_EVENT_SOURCES.has(src)) {
      warnings.push({ code: "SOURCE_UNSUPPORTED", message: `Source '${src}' is not supported.`, severity: "warning" });
      continue;
    }
    if (!seen.has(src)) {
      seen.add(src);
      selected.push(src);
    }
  }
  return { selected: selected.length ? selected : defaults, warnings };
}

function normalizeIso(raw: unknown): string | null {
  if (raw == null) return null;
  if (typeof raw === "number" && Number.isFinite(raw)) return iso(raw);
  if (typeof raw !== "string") return null;
  const v = raw.trim();
  if (!v) return null;
  if (/^\d{14}$/.test(v)) return `${v.slice(0, 4)}-${v.slice(4, 6)}-${v.slice(6, 8)}T${v.slice(8, 10)}:${v.slice(10, 12)}:${v.slice(12, 14)}Z`;
  if (/^\d{8}$/.test(v)) return `${v.slice(0, 4)}-${v.slice(4, 6)}-${v.slice(6, 8)}T00:00:00Z`;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function shortText(value: unknown, maxLen = 220): string | null {
  const s = _str(value).replace(/\s+/g, " ").trim();
  return s ? s.slice(0, maxLen) : null;
}

function eventTypeFromKeywords(title: string, summary: string): string {
  const text = `${title} ${summary}`.toLowerCase();
  if (/earnings|eps|quarterly|10-q|10-k/.test(text)) return "earnings";
  if (/guidance|outlook|forecast|reaffirm|raises|lowers/.test(text)) return "guidance";
  if (/contract|agreement|deal|partnership/.test(text)) return "contract";
  if (/offering|financing|debt|notes?/.test(text)) return "financing";
  if (/launch|product|introduce/.test(text)) return "product";
  if (/analyst|rating|upgrade|downgrade|price target/.test(text)) return "analyst";
  if (/macro|inflation|rates|fomc|cpi/.test(text)) return "macro";
  if (/lawsuit|litigation|court|settlement/.test(text)) return "litigation";
  if (/insider|director|officer|form 4/.test(text)) return "insider";
  if (/sec|regulatory|8-k|filing/.test(text)) return "regulatory";
  return "other";
}

function normalizedEventTitleStem(ticker: string, title: string | null): string {
  const stop = new Set(["yahoo", "finance", "finnhub", "globenewswire", "press", "release", "inc", "corp", "corporation", "company", "plc", "ltd", "llc"]);
  const tickerLower = ticker.toLowerCase();
  return _str(title)
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter((word) => word && word !== tickerLower && !stop.has(word))
    .join(" ")
    .slice(0, 80);
}

function eventTypeFromForm(formType: string): string {
  const f = formType.toUpperCase();
  if (f === "10-Q" || f === "10-K") return "earnings";
  if (f === "S-3" || f === "S-1" || f === "424B") return "financing";
  if (f === "8-K" || f === "DEF14A" || f === "PRE14A") return "regulatory";
  if (f === "4") return "insider";
  return "other";
}

function makeDupGroupId(ticker: string, title: string | null, publishedAt: string | null, issuer: string | null): string | null {
  const normTitle = normalizedEventTitleStem(ticker, title);
  const day = _str(publishedAt).slice(0, 10);
  const entity = (_str(issuer) || ticker).toUpperCase();
  if (!normTitle && !day) return null;
  let h = 0;
  const s = `${normTitle}|${day}|${entity}`;
  for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  return (h >>> 0).toString(16).padStart(8, "0");
}

function xmlTag(xml: string, path: string[]): string | null {
  let scope = xml;
  for (const tag of path) {
    const re = new RegExp(`<(?:\\w+:)?${tag}\\b[^>]*>([\\s\\S]*?)<\\/(?:\\w+:)?${tag}>`, "i");
    const m = scope.match(re);
    if (!m) return null;
    scope = m[1];
  }
  return scope.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim() || null;
}

function xmlBlock(xml: string, tag: string): string | null {
  const m = xml.match(new RegExp(`<(?:\\w+:)?${tag}\\b[^>]*>([\\s\\S]*?)<\\/(?:\\w+:)?${tag}>`, "i"));
  return m ? m[1] : null;
}

const FORM4_TRANSACTION_LABELS: Record<string, string> = {
  P: "Purchase",
  S: "Sale",
  A: "Grant/Award",
  D: "Disposition",
  M: "Option exercise/conversion",
  F: "Tax withholding/payment",
  G: "Gift",
};

function form4Num(value: string | null): number | null {
  if (value == null) return null;
  const text = stripHtmlTags(value);
  const m = text.match(/-?\$?\s*\(?\d[\d,]*(?:\.\d+)?\)?/);
  if (!m) return null;
  let s = m[0].replace(/\$/g, "").replace(/,/g, "").replace(/\s+/g, "");
  const negative = s.startsWith("(") && s.endsWith(")");
  s = s.replace(/[()]/g, "");
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return negative ? -n : n;
}

function form4Date(value: string | null): string | null {
  const text = stripHtmlTags(value ?? "");
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const m = text.match(/\b(\d{1,2})\/(\d{1,2})\/(\d{4})\b/);
  if (!m) return text;
  return `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
}

function form4Payload(
  owner: string | null,
  role: string | null,
  code: string | null,
  shares: number | null,
  price: number | null,
  ownershipForm: string | null,
  transactionDate: string | null,
): Record<string, unknown> {
  return {
    owner,
    role,
    transactionCode: code,
    transactionLabel: FORM4_TRANSACTION_LABELS[_str(code).toUpperCase()] ?? "Other/Unclassified",
    shares,
    price,
    value: shares != null && price != null ? Math.round(shares * price * 100) / 100 : null,
    ownershipForm,
    transactionDate,
  };
}

function form4OwnerFromHtml(html: string): string | null {
  const m = html.match(/Name and Address of Reporting Person[\s\S]*?<a\b[^>]*>([\s\S]*?)<\/a>/i);
  return m ? stripHtmlTags(m[1]) || null : null;
}

function form4RoleFromHtml(html: string): string | null {
  const m = html.match(/Relationship of Reporting Person\(s\) to Issuer([\s\S]*?)Individual or Joint\/Group Filing/i);
  if (!m) return null;
  const roles: string[] = [];
  for (const row of parseHtmlTable(m[1])) {
    for (let i = 0; i < row.length - 1; i++) {
      if (row[i].trim().toUpperCase() !== "X") continue;
      const label = row[i + 1].toLowerCase();
      if (label.includes("director")) roles.push("director");
      else if (label.includes("officer")) roles.push("officer");
      else if (label.includes("10%") || label.includes("owner")) roles.push("ten_percent_owner");
      else if (label.includes("other")) roles.push("other");
    }
  }
  return [...new Set(roles)].join(", ") || null;
}

function parseForm4HtmlTransaction(html: string): Record<string, unknown> | null {
  const owner = form4OwnerFromHtml(html);
  const role = form4RoleFromHtml(html);
  const tableRe = /<table[^>]*>[\s\S]*?<\/table>/gi;
  let tableM: RegExpExecArray | null;
  while ((tableM = tableRe.exec(html)) !== null) {
    const tableHtml = tableM[0];
    const tableText = stripHtmlTags(tableHtml).toLowerCase();
    if (tableText.includes("non-derivative securities")) {
      for (const row of parseHtmlTable(tableHtml)) {
        if (row.length < 10) continue;
        const code = row[3].trim().toUpperCase();
        const shares = form4Num(row[5]);
        if (!code || shares == null) continue;
        return form4Payload(
          owner,
          role,
          code,
          shares,
          form4Num(row[7]),
          row[9].trim() || null,
          form4Date(row[1]),
        );
      }
    }
    if (tableText.includes("derivative securities")) {
      for (const row of parseHtmlTable(tableHtml)) {
        if (row.length < 15) continue;
        const code = row[4].trim().toUpperCase();
        const shares = form4Num(row[6]);
        if (!code || shares == null) continue;
        return form4Payload(
          owner,
          role,
          code,
          shares,
          form4Num(row[1]),
          row[14].trim() || null,
          form4Date(row[2]),
        );
      }
    }
  }
  return null;
}

function parseForm4Transaction(xml: string): Record<string, unknown> | null {
  const tx = xmlBlock(xml, "nonDerivativeTransaction") ?? xmlBlock(xml, "derivativeTransaction");
  if (!tx) return parseForm4HtmlTransaction(xml);
  const owner = xmlTag(xml, ["reportingOwnerId", "rptOwnerName"]);
  const officerTitle = xmlTag(xml, ["reportingOwnerRelationship", "officerTitle"]);
  const roles: string[] = [];
  for (const [tag, label] of [["isDirector", "director"], ["isOfficer", "officer"], ["isTenPercentOwner", "ten_percent_owner"], ["isOther", "other"]]) {
    if (["1", "true"].includes(_str(xmlTag(xml, ["reportingOwnerRelationship", tag])).toLowerCase())) roles.push(label);
  }
  const code = xmlTag(tx, ["transactionCoding", "transactionCode"]);
  const shares = form4Num(xmlTag(tx, ["transactionAmounts", "transactionShares", "value"]));
  const price = form4Num(xmlTag(tx, ["transactionAmounts", "transactionPricePerShare", "value"]));
  return form4Payload(
    owner,
    officerTitle || roles.join(", ") || null,
    code,
    shares,
    price,
    xmlTag(tx, ["ownershipNature", "directOrIndirectOwnership", "value"]),
    form4Date(xmlTag(tx, ["transactionDate", "value"])),
  );
}

async function tryAttachForm4Transaction(item: Record<string, unknown>, filing: Record<string, unknown>, warnings: Record<string, unknown>[]): Promise<void> {
  if (_str(filing.filingType).toUpperCase() !== "4") return;
  const url = _str(item.url);
  if (!url.startsWith("https://www.sec.gov/Archives/")) {
    warnings.push({ code: "FORM4_PARSE_UNAVAILABLE", message: "Form 4 primary document URL is unavailable.", severity: "warning" });
    return;
  }
  const xml = await edgarGetHtml(url, 2_000_000);
  const parsed = parseForm4Transaction(xml ?? "");
  if (!parsed) {
    warnings.push({ code: "FORM4_PARSE_UNAVAILABLE", message: "Form 4 transaction details could not be parsed from the primary document.", severity: "warning" });
    return;
  }
  item.insiderTransaction = parsed;
  const label = _str(parsed.transactionLabel) || "Insider transaction";
  const owner = _str(parsed.owner) || "reporting owner";
  const shares = typeof parsed.shares === "number" ? `${Math.round(parsed.shares).toLocaleString("en-US")} shares` : "shares unavailable";
  const value = typeof parsed.value === "number" ? `, value $${Math.round(parsed.value).toLocaleString("en-US")}` : "";
  item.title = `Form 4: ${label} by ${owner}`;
  item.summary = shortText(`${label} by ${owner}: ${shares}${value}.`);
  item.evidenceText = shortText(`SEC Form 4 transaction code ${_str(parsed.transactionCode) || "unknown"} on ${_str(parsed.transactionDate) || _str(item.filingDate)}.`);
}

function withinDateWindow(publishedAt: string | null, startDate = "", endDate = "", lookbackDays?: number): boolean {
  if (!publishedAt) return false;
  const day = publishedAt.slice(0, 10);
  if (startDate && day < startDate) return false;
  if (endDate && day > endDate) return false;
  if (lookbackDays != null) {
    const cutoff = new Date(Date.now() - lookbackDays * 86400000).toISOString().slice(0, 10);
    if (day < cutoff) return false;
  }
  return true;
}

function decodeXmlText(value: string): string {
  return value
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/gi, "$1")
    .replace(/&(?:amp|#38);/gi, "&")
    .replace(/&(?:lt|#60);/gi, "<")
    .replace(/&(?:gt|#62);/gi, ">")
    .replace(/&(?:quot|#34);/gi, "\"")
    .replace(/&(?:apos|#39);/gi, "'")
    .replace(/&#(\d+);/g, (_m, code) => String.fromCharCode(Number(code)));
}

function extractXmlTag(xml: string, tagName: string): string {
  const escaped = tagName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const m = xml.match(new RegExp(`<${escaped}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${escaped}>`, "i"));
  return m ? decodeXmlText(m[1]).trim() : "";
}

function extractXmlTagValues(xml: string, tagName: string): string[] {
  const escaped = tagName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const out: string[] = [];
  const re = new RegExp(`<${escaped}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${escaped}>`, "gi");
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    const value = decodeXmlText(m[1]).trim();
    if (value) out.push(value);
  }
  return out;
}

function extractGlobeNewswireCategories(xml: string, domain: string): string[] {
  const out: string[] = [];
  const domainLower = domain.toLowerCase();
  const re = /<category\b([^>]*)>([\s\S]*?)<\/category>/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    const attrs = m[1].toLowerCase();
    if (!attrs.includes(`domain="${domainLower}"`) && !attrs.includes(`domain='${domainLower}'`)) continue;
    const text = decodeXmlText(m[2]).trim();
    for (const part of text.split(",")) {
      const value = part.trim();
      if (value) out.push(value);
    }
  }
  return out;
}

function globenewswireStockCategoryMatches(ticker: string, stockCategories: string[]): boolean {
  const tickerU = ticker.toUpperCase();
  return stockCategories.some(category => category.split(":").pop()?.trim().toUpperCase() === tickerU);
}

function globenewswirePlainText(value: string): string {
  return stripHtmlTags(decodeXmlText(value));
}

function buildYfEventItem(
  ticker: string,
  raw: Record<string, unknown>,
  retrievedAt: string,
  feedSource?: string,
): { item: Record<string, unknown>; warnings: Record<string, unknown>[] } {
  const warnings: Record<string, unknown>[] = [];
  const content = (raw.content && typeof raw.content === "object") ? raw.content as Record<string, unknown> : {};
  const title = _str(content.title || raw.title).trim();
  const summary = _str(content.summary || raw.summary).trim();
  const originalSource = _str((content.provider as Record<string, unknown> | undefined)?.displayName || raw.publisher || "Yahoo Finance");
  const url = _str((content.canonicalUrl as Record<string, unknown> | undefined)?.url || raw.link || raw.url) || null;
  // Prefer native Yahoo publish timestamps, then normalized fallback from getNews().
  const publishedAt = normalizeIso(raw.providerPublishTime ?? content.pubDate ?? raw.publishedAt);
  if (!publishedAt) {
    warnings.push({ code: "PUBLISHED_AT_UNAVAILABLE", message: `Published timestamp unavailable for source '${originalSource}'.`, severity: "warning" });
  }

  // Determine the precise source label.
  let sourceKey: string;
  if (feedSource === "yahoo_finance_news" || feedSource === "yahoo_finance_press_releases") {
    sourceKey = feedSource;
  } else {
    const contentType = _str(content.contentType || raw.contentType).toUpperCase();
    sourceKey = contentType === "PRESS_RELEASE" ? "yahoo_finance_press_releases" : "yahoo_finance_news";
  }

  const relevance = `${title} ${summary}`.toUpperCase().includes(ticker.toUpperCase()) ? "HIGH" : "LOW";
  const confidence = !url || relevance !== "HIGH" ? "LOW" : "MEDIUM";
  const duplicateGroupId = makeDupGroupId(ticker, title, publishedAt, null);
  if (!duplicateGroupId) {
    warnings.push({ code: "DEDUPE_WEAK_KEY", message: "Weak dedupe key for at least one item.", severity: "warning" });
  }
  return {
    item: {
      title,
      source: sourceKey,
      originalSource,
      sourceType: sourceKey,
      publishedAt,
      retrievedAt,
      url,
      issuer: null,
      tickers: [ticker.toUpperCase()],
      eventType: eventTypeFromKeywords(title, summary),
      summary: shortText(summary || title, 240),
      evidenceText: shortText(summary || title, 180),
      confidence,
      tickerRelevance: relevance,
      duplicateGroupId,
    },
    warnings,
  };
}

function buildSecEventItem(
  ticker: string,
  filing: Record<string, unknown>,
  retrievedAt: string,
  issuer: string | null
): { item: Record<string, unknown>; warnings: Record<string, unknown>[] } {
  const warnings: Record<string, unknown>[] = [];
  const filingType = _str(filing.filingType || filing.formType || filing.form).toUpperCase();
  const filingDate = _str(filing.filingDate);
  const acceptedAt = normalizeIso(filing.acceptedAt ?? filing.acceptanceDateTime);
  let publishedAt = acceptedAt;
  if (!publishedAt && filingDate) {
    publishedAt = `${filingDate}T00:00:00Z`;
    warnings.push({
      code: "PUBLISHED_AT_ESTIMATED",
      message: `acceptedAt unavailable for ${filingType || "SEC filing"}; filingDate used.`,
      severity: "warning",
    });
  }
  const accessionNumber = _str(filing.accessionNumber);
  const cikInt = _str(filing.cikInt || filing.cik);
  const accClean = accessionNumber.replace(/-/g, "");
  const primaryDocument = _str(filing.primaryDocument);
  let url = _str(filing.documentUrl || filing.primaryDocumentUrl);
  if (!url && cikInt && accessionNumber && primaryDocument) {
    url = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${primaryDocument}`;
  }
  if (!url && cikInt && accessionNumber) {
    url = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${accessionNumber}-index.htm`;
  }
  if (!url.startsWith("https://www.sec.gov/Archives/")) {
    url = "";
    warnings.push({ code: "SEC_URL_INVALID", message: "SEC event URL missing or invalid SEC Archives URL.", severity: "warning" });
  }
  const duplicateGroupId = makeDupGroupId(ticker, `${filingType} filed`, publishedAt, issuer);
  if (!duplicateGroupId) {
    warnings.push({ code: "DEDUPE_WEAK_KEY", message: "Weak dedupe key for at least one item.", severity: "warning" });
  }
  const confidence = accessionNumber && acceptedAt && url ? "HIGH" : (accessionNumber && url ? "MEDIUM" : "LOW");
  return {
    item: {
      title: `${filingType || "SEC"} filed`,
      source: "SEC",
      sourceType: "sec_filing",
      filingType: filingType || null,
      filingDate: filingDate || null,
      acceptedAt: acceptedAt || null,
      accessionNumber: accessionNumber || null,
      url: url || null,
      publishedAt,
      retrievedAt,
      issuer,
      tickers: [ticker.toUpperCase()],
      eventType: eventTypeFromForm(filingType),
      summary: shortText(`SEC ${filingType} filing for ${ticker.toUpperCase()}`),
      evidenceText: shortText(`${filingType} accepted by SEC on ${acceptedAt || filingDate}`),
      confidence,
      tickerRelevance: "HIGH",
      duplicateGroupId,
    },
    warnings,
  };
}

function dedupeEventItems(items: Record<string, unknown>[], warnings: Record<string, unknown>[]): Record<string, unknown>[] {
  const grouped = new Map<string, Record<string, unknown>>();
  const passthrough: Record<string, unknown>[] = [];
  for (const item of items) {
    const gid = _str(item.duplicateGroupId);
    if (!gid) {
      passthrough.push(item);
      continue;
    }
    const existing = grouped.get(gid);
    if (!existing) {
      grouped.set(gid, item);
      continue;
    }
    const existingRank = SOURCE_PRIORITY[_str(existing.sourceType, "other")] ?? SOURCE_PRIORITY.other;
    const currentRank = SOURCE_PRIORITY[_str(item.sourceType, "other")] ?? SOURCE_PRIORITY.other;
    let keepNew = currentRank < existingRank;
    if (currentRank === existingRank) {
      keepNew = _str(item.publishedAt) > _str(existing.publishedAt);
    }
    if (_str(existing.publishedAt) !== _str(item.publishedAt)) {
      warnings.push({ code: "TIMESTAMP_CONFLICT", message: `Conflicting source timestamps observed for duplicateGroupId=${gid}.`, severity: "warning" });
    }
    const preferred = keepNew ? item : existing;
    const alternate = keepNew ? existing : item;
    const refs = Array.isArray(preferred.sourceRefs) ? preferred.sourceRefs as Record<string, unknown>[] : [];
    refs.push({
      source: alternate.source,
      sourceType: alternate.sourceType,
      publishedAt: alternate.publishedAt,
      url: alternate.url,
    });
    preferred.sourceRefs = refs;
    grouped.set(gid, preferred);
  }
  const deduped = [...grouped.values(), ...passthrough];
  deduped.sort((a, b) => _str(b.publishedAt).localeCompare(_str(a.publishedAt)));
  return deduped;
}

function collectionStatus(items: Record<string, unknown>[], sourcesUsed: string[], warnings: Record<string, unknown>[]): string | null {
  if (items.length > 0 && warnings.some(w => _str(w.code) === "SOURCE_UNAVAILABLE")) return "PARTIAL";
  if (items.length === 0) {
    if (warnings.some(w => _str(w.code) === "SOURCE_UNAVAILABLE")) return "SOURCE_LIMITED_NOT_FOUND";
    if (sourcesUsed.length > 0) return "NOT_FOUND";
    return "PROVIDER_ERROR";
  }
  return null;
}

function computeSourceStatus(
  sourcesUsed: string[],
  warnings: Record<string, unknown>[],
  items: Record<string, unknown>[],
  selectedSources: string[]
): Record<string, unknown> {
  const warningMsgs = warnings
    .filter(w => _str(w.code) === "SOURCE_UNAVAILABLE")
    .map(w => _str(w.message).toLowerCase());
  const isYfError = warningMsgs.some(m => m.includes("yahoo finance"));
  const secItems = items.filter(it => _str(it.sourceType).includes("sec"));
  const getItemSource = (it: Record<string, unknown>): string => _str(it.source);
  const getItemSourceType = (it: Record<string, unknown>): string => _str(it.sourceType);
  // Fine-grained Yahoo Finance items
  const yfNewsItems = items.filter(
    it => getItemSource(it) === "yahoo_finance_news" || getItemSourceType(it) === "yahoo_finance_news"
  );
  const yfPrItems = items.filter(
    it =>
      getItemSource(it) === "yahoo_finance_press_releases"
      || getItemSourceType(it) === "yahoo_finance_press_releases"
  );
  // Legacy yahoo_finance aggregates both fine-grained sources plus legacy-tagged items
  const yfLegacyItems = items.filter(
    it =>
      ["yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases"].includes(getItemSource(it))
      || ["yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases"].includes(getItemSourceType(it))
  );
  const newswireItems = items.filter(it => _str(it.sourceType) === "newswire");
  const companyIrItems = items.filter(it => ["company_ir", "press_release"].includes(_str(it.sourceType)));
  const finnhubItems = items.filter(it => _str(it.source) === "finnhub");
  const result: Record<string, unknown> = {};
  if (selectedSources.includes("sec")) {
    if (sourcesUsed.includes("sec")) {
      result.sec = { status: secItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: secItems.length, filteredCount: secItems.length };
    } else if (warningMsgs.some(m => m.includes("sec submissions"))) {
      result.sec = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.sec = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  // Fine-grained Yahoo Finance sources
  if (selectedSources.includes("yahoo_finance_news")) {
    if (sourcesUsed.includes("yahoo_finance_news")) {
      result.yahoo_finance_news = { status: yfNewsItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: yfNewsItems.length, filteredCount: yfNewsItems.length };
    } else if (isYfError) {
      result.yahoo_finance_news = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.yahoo_finance_news = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  if (selectedSources.includes("yahoo_finance_press_releases")) {
    if (sourcesUsed.includes("yahoo_finance_press_releases")) {
      result.yahoo_finance_press_releases = { status: yfPrItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: yfPrItems.length, filteredCount: yfPrItems.length };
    } else if (isYfError) {
      result.yahoo_finance_press_releases = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.yahoo_finance_press_releases = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  // Legacy yahoo_finance aggregate
  if (selectedSources.includes("yahoo_finance")) {
    if (sourcesUsed.includes("yahoo_finance")) {
      result.yahoo_finance = { status: yfLegacyItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: yfLegacyItems.length, filteredCount: yfLegacyItems.length };
    } else if (isYfError) {
      result.yahoo_finance = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.yahoo_finance = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  if (selectedSources.includes("finnhub")) {
    if (sourcesUsed.includes("finnhub")) {
      result.finnhub = { status: finnhubItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: finnhubItems.length, filteredCount: finnhubItems.length };
    } else if (warningMsgs.some(m => m.includes("finnhub company-news source is not configured"))) {
      result.finnhub = { status: "UNCONFIGURED" };
    } else if (warningMsgs.some(m => m.includes("finnhub auth error"))) {
      result.finnhub = { status: "AUTH_ERROR", rawCount: 0, filteredCount: 0 };
    } else if (warningMsgs.some(m => m.includes("finnhub rate limited"))) {
      result.finnhub = { status: "RATE_LIMITED", rawCount: 0, filteredCount: 0 };
    } else if (warningMsgs.some(m => m.includes("finnhub provider changed"))) {
      result.finnhub = { status: "PROVIDER_CHANGED", rawCount: 0, filteredCount: 0 };
    } else if (warningMsgs.some(m => m.includes("finnhub"))) {
      result.finnhub = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.finnhub = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  if (selectedSources.includes("company_ir")) {
    if (sourcesUsed.includes("company_ir")) {
      result.company_ir = { status: companyIrItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: companyIrItems.length, filteredCount: companyIrItems.length };
    } else if (isYfError) {
      result.company_ir = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.company_ir = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  if (selectedSources.includes("newswire")) {
    if (sourcesUsed.includes("newswire")) {
      result.newswire = { status: newswireItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: newswireItems.length, filteredCount: newswireItems.length };
    } else if (warningMsgs.some(m => m.includes("globenewswire"))) {
      result.newswire = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.newswire = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  return result;
}

function computeSourceCoverage(sourceStatus: Record<string, unknown>): string {
  const limitedStatuses = new Set(["UNCONFIGURED", "PROVIDER_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_CHANGED"]);
  for (const info of Object.values(sourceStatus)) {
    if (info && typeof info === "object" && limitedStatuses.has(_str((info as Record<string, unknown>).status))) {
      return "PARTIAL";
    }
  }
  return "FULL";
}

async function collectSecEvents(
  ticker: string,
  filingTypes: string[],
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean }> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    warnings.push({ code: "SOURCE_UNAVAILABLE", message: "SEC submissions source unavailable.", severity: "warning" });
    return { items, warnings, used: false };
  }
  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form ?? []) as string[];
  const filingDates = (recent.filingDate ?? []) as string[];
  const accepted = (recent.acceptanceDateTime ?? []) as string[];
  const accessions = (recent.accessionNumber ?? []) as string[];
  const primaryDocs = (recent.primaryDocument ?? []) as string[];
  const issuer = _str(submissions.name) || null;
  const wanted = new Set(filingTypes.map(f => f.toUpperCase()));
  const cikInt = String(parseInt(cikPadded, 10));
  for (let i = 0; i < forms.length && items.length < maxResults; i++) {
    const form = _str(forms[i]).toUpperCase();
    if (wanted.size > 0 && !wanted.has(form)) continue;
    const filing = {
      filingType: form,
      filingDate: filingDates[i] ?? "",
      acceptedAt: accepted[i] ?? "",
      accessionNumber: accessions[i] ?? "",
      primaryDocument: primaryDocs[i] ?? "",
      cikInt,
    };
    const { item, warnings: w } = buildSecEventItem(ticker, filing, retrievedAt, issuer);
    await tryAttachForm4Transaction(item, filing, w);
    if (!withinDateWindow(_str(item.publishedAt) || null, startDate, endDate, lookbackDays)) continue;
    items.push(item);
    warnings.push(...w);
  }
  return { items, warnings, used: true };
}

async function collectYahooEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number,
  feed: "news" | "press_releases" = "news",
  nameTokens: string[] = [],
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean }> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const feedSource = feed === "press_releases" ? "yahoo_finance_press_releases" : "yahoo_finance_news";
  try {
    let newsRaw: Record<string, unknown>[];
    if (feed === "press_releases") {
      // Mirror yfinance Ticker.get_news(tab="press releases"):
      // POST https://finance.yahoo.com/xhr/ncp?queryRef=pressRelease&serviceKey=ncp_fin
      // Body: { serviceConfig: { snippetCount: count, s: [ticker] } }
      // Response: data.data.tickerStream.stream (filter out ad items)
      try {
        const { cookie } = await getCrumb();
        const count = Math.min(Math.max(1, maxResults), 100);
        const prUrl = "https://finance.yahoo.com/xhr/ncp?queryRef=pressRelease&serviceKey=ncp_fin";
        const resp = await fetch(prUrl, {
          method: "POST",
          headers: {
            "User-Agent": UA,
            "Content-Type": "application/json",
            Cookie: cookie,
          },
          body: JSON.stringify({ serviceConfig: { snippetCount: count, s: [ticker.toUpperCase()] } }),
        });
        if (!resp.ok) {
          await resp.body?.cancel();
          newsRaw = [];
        } else {
          const prJson = await resp.json() as Record<string, unknown>;
          const stream = (
            (prJson.data as Record<string, unknown> | undefined)?.tickerStream as Record<string, unknown> | undefined
          )?.stream;
          const raw = Array.isArray(stream) ? (stream as Record<string, unknown>[]) : [];
          // Filter out ad items, exactly as yfinance does
          newsRaw = raw.filter(item => !item.ad || (Array.isArray(item.ad) && item.ad.length === 0));
        }
      } catch {
        newsRaw = [];
      }
    } else {
      const raw = JSON.parse(await getNews(ticker)) as Record<string, unknown>;
      newsRaw = (raw.items as Record<string, unknown>[]) ?? [];
    }
    for (const n of newsRaw) {
      const content = (n.content && typeof n.content === "object") ? n.content as Record<string, unknown> : {};
      const ct = _str(content.contentType || n.contentType).toUpperCase();
      if (ct && !YAHOO_ALLOWED_CONTENT_TYPES.has(ct)) continue;
      // For the press-releases feed, Yahoo tab membership is authoritative:
      // valid press-release tab items may still arrive as STORY/ARTICLE.
      const { item, warnings: w } = buildYfEventItem(ticker, n, retrievedAt, feedSource);
      if (!withinDateWindow(_str(item.publishedAt) || null, startDate, endDate, lookbackDays)) continue;
      // BUG-08: drop articles that don't mention the ticker or company name.
      // Only filter when nameTokens is populated; if company name lookup failed,
      // fall back to permissive (no filtering) to avoid over-dropping.
      if (feed === "news" && nameTokens.length > 0) {
        const hay = `${_str(item.title)} ${_str(item.evidenceText)}`;
        const tickerEsc = ticker.toUpperCase().replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const tickerFound = new RegExp(`\\b${tickerEsc}\\b`).test(hay.toUpperCase());
        const nameFound = nameTokens.some(tok =>
          new RegExp(`\\b${tok.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(hay)
        );
        if (!tickerFound && !nameFound) continue;
        item.sourceTickerMatch = true;
      }
      items.push(item);
      warnings.push(...w);
      if (items.length >= maxResults) break;
    }
  } catch (e) {
    warnings.push({ code: "SOURCE_UNAVAILABLE", message: `Yahoo Finance source unavailable: ${e instanceof Error ? e.message : String(e)}`, severity: "warning" });
    return { items, warnings, used: false };
  }
  return { items, warnings, used: true };
}

async function fetchGlobeNewswireFeed(feed: { name: string; url: string }): Promise<string> {
  const cacheKey = `gnw_rss:${feed.name}`;
  const cached = globenewswireCache.get(cacheKey);
  if (cached && Date.now() - cached.storedAt < GLOBENEWSWIRE_TTL_MS) return cached.value;

  const resp = await fetch(feed.url, { headers: { "User-Agent": UA } });
  if (!resp.ok) {
    await resp.body?.cancel();
    throw new Error(`HTTP ${resp.status}`);
  }
  const declaredLen = Number(resp.headers.get("content-length") || "0");
  if (declaredLen > GLOBENEWSWIRE_MAX_BYTES) {
    await resp.body?.cancel();
    throw new Error("GlobeNewswire RSS response exceeded size limit");
  }
  const buf = await resp.arrayBuffer();
  if (buf.byteLength > GLOBENEWSWIRE_MAX_BYTES) {
    throw new Error("GlobeNewswire RSS response exceeded size limit");
  }
  const xml = new TextDecoder("utf-8").decode(buf);
  globenewswireCache.set(cacheKey, { value: xml, storedAt: Date.now() });
  return xml;
}

async function collectGlobeNewswireEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean }> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  let parsedAnyFeed = false;
  const tickerU = ticker.toUpperCase();

  for (const feed of GLOBENEWSWIRE_RSS_FEEDS) {
    let xml = "";
    try {
      xml = await fetchGlobeNewswireFeed(feed);
    } catch (e) {
      warnings.push({
        code: "SOURCE_UNAVAILABLE",
        message: `GlobeNewswire RSS feed '${feed.name}' unavailable: ${e instanceof Error ? e.message : String(e)}`,
        severity: "warning",
      });
      continue;
    }

    try {
      if (/<!doctype|<!entity/i.test(xml)) {
        throw new Error("unsupported XML declaration in GlobeNewswire RSS");
      }
      if (!/<rss\b/i.test(xml) || !/<channel\b/i.test(xml) || !/<\/rss>/i.test(xml)) {
        throw new Error("invalid GlobeNewswire RSS XML");
      }
      const openItems = xml.match(/<item\b/gi)?.length ?? 0;
      const closeItems = xml.match(/<\/item>/gi)?.length ?? 0;
      if (openItems !== closeItems) {
        throw new Error("malformed GlobeNewswire RSS item XML");
      }
      const itemRe = /<item\b[^>]*>([\s\S]*?)<\/item>/gi;
      let m: RegExpExecArray | null;
      parsedAnyFeed = true;
      while ((m = itemRe.exec(xml)) !== null) {
        const itemXml = m[1];
        const stockCategories = extractGlobeNewswireCategories(itemXml, GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN);
        if (!globenewswireStockCategoryMatches(tickerU, stockCategories)) continue;

        const title = extractXmlTag(itemXml, "title");
        const descriptionRaw = extractXmlTag(itemXml, "description");
        const description = globenewswirePlainText(descriptionRaw);
        const link = extractXmlTag(itemXml, "link") || null;
        const guid = extractXmlTag(itemXml, "guid") || null;
        const publishedAt = normalizeIso(extractXmlTag(itemXml, "pubDate"));
        if (!withinDateWindow(publishedAt, startDate, endDate, lookbackDays)) continue;

        const issuer = extractXmlTag(itemXml, "dc:contributor") || null;
        const subject = extractXmlTag(itemXml, "dc:subject") || null;
        const language = extractXmlTag(itemXml, "dc:language") || null;
        const globenewswireId = extractXmlTag(itemXml, "dc:identifier") || null;
        const keywords = extractXmlTagValues(itemXml, "dc:keyword");
        const isinValues = extractGlobeNewswireCategories(itemXml, GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN);

        if (!publishedAt) {
          warnings.push({
            code: "PUBLISHED_AT_UNAVAILABLE",
            message: "Published timestamp unavailable for GlobeNewswire item.",
            severity: "warning",
          });
        }

        const duplicateGroupId = makeDupGroupId(tickerU, title, publishedAt, issuer);
        if (!duplicateGroupId) {
          warnings.push({ code: "DEDUPE_WEAK_KEY", message: "Weak dedupe key for at least one GlobeNewswire item.", severity: "warning" });
        }
        const dedupeKey = duplicateGroupId || `${feed.name}:${link || guid}:${title}:${publishedAt}`;
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);

        const item: Record<string, unknown> = {
          title,
          source: "newswire",
          originalSource: "GlobeNewswire",
          sourceType: "newswire",
          provider: "globenewswire",
          discoveredVia: "globenewswire_rss",
          publishedAt,
          retrievedAt,
          url: link || guid,
          issuer,
          tickers: [tickerU],
          eventType: eventTypeFromKeywords(title, `${description} ${subject ?? ""}`),
          summary: shortText(description || title, 240),
          evidenceText: shortText(description || title, 180),
          confidence: link || guid ? "HIGH" : "MEDIUM",
          tickerRelevance: "HIGH",
          duplicateGroupId,
          stockCategories,
          feedSource: feed.name,
        };
        if (isinValues.length > 0) item.isin = isinValues[0];
        if (subject) item.subject = subject;
        if (keywords.length > 0) item.keywords = keywords;
        if (language) item.language = language;
        if (globenewswireId) item.globenewswireId = globenewswireId;
        items.push(item);
        if (items.length >= maxResults) return { items, warnings, used: true };
      }
    } catch (e) {
      warnings.push({
        code: "SOURCE_UNAVAILABLE",
        message: `GlobeNewswire RSS feed '${feed.name}' parse error: ${e instanceof Error ? e.message : String(e)}`,
        severity: "warning",
      });
    }
  }

  return { items, warnings, used: warnings.length > 0 && items.length === 0 ? false : parsedAnyFeed };
}

async function collectFinnhubEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean }> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const token = getWorkerVar("FINNHUB_API_KEY") ?? getWorkerVar("FINNHUB_TOKEN");
  if (!token) {
    warnings.push({ code: "SOURCE_UNAVAILABLE", message: "Finnhub company-news source is not configured; skipped.", severity: "warning" });
    return { items, warnings, used: false };
  }
  try {
    const now = new Date();
    const from = new Date(now.getTime() - (lookbackDays ?? 14) * 86400000).toISOString().slice(0, 10);
    const to = now.toISOString().slice(0, 10);
    const url = `${FINNHUB_COMPANY_NEWS_API}?symbol=${encodeURIComponent(ticker.toUpperCase())}&from=${from}&to=${to}`;
    const resp = await fetch(url, { headers: { "User-Agent": UA, "X-Finnhub-Token": token } });
    if (!resp.ok) {
      await resp.body?.cancel();
      if (resp.status === 401 || resp.status === 403) {
        throw new Error(`FINNHUB_AUTH_ERROR:${resp.status}`);
      } else if (resp.status === 429) {
        throw new Error("FINNHUB_RATE_LIMITED");
      } else {
        throw new Error(`HTTP ${resp.status}`);
      }
    }
    const rawJson = await resp.json();
    if (!Array.isArray(rawJson)) {
      throw new Error(`FINNHUB_PROVIDER_CHANGED: expected array, got ${typeof rawJson}`);
    }
    const news = rawJson as Record<string, unknown>[];
    const tickerU = ticker.toUpperCase();
    for (const n of news) {
      const title = _str(n.headline).trim();
      const summary = _str(n.summary).trim();
      const originalSource = _str(n.source).trim() || null;
      const urlStr = _str(n.url).trim() || null;
      const publishedAt = normalizeIso(n.datetime);
      const duplicateGroupId = makeDupGroupId(tickerU, title, publishedAt, null);
      if (!duplicateGroupId) {
        warnings.push({ code: "DEDUPE_WEAK_KEY", message: "Weak dedupe key for at least one item.", severity: "warning" });
      }
      const relevance = `${title} ${summary}`.toUpperCase().includes(tickerU) ? "HIGH" : "LOW";
      const item: Record<string, unknown> = {
        title,
        source: "finnhub",
        originalSource,
        sourceType: "company_news",
        publishedAt,
        retrievedAt,
        url: urlStr,
        issuer: null,
        tickers: [tickerU],
        eventType: eventTypeFromKeywords(title, summary),
        summary: shortText(summary || title, 240),
        evidenceText: shortText(summary || title, 180),
        confidence: urlStr ? "MEDIUM" : "LOW",
        tickerRelevance: relevance,
        duplicateGroupId,
      };
      if (!withinDateWindow(_str(item.publishedAt) || null, startDate, endDate, lookbackDays)) continue;
      items.push(item);
      if (items.length >= maxResults) break;
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (msg.startsWith("FINNHUB_AUTH_ERROR")) {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: `Finnhub auth error: ${msg}`, severity: "warning" });
    } else if (msg.startsWith("FINNHUB_RATE_LIMITED")) {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: "Finnhub rate limited: HTTP 429", severity: "warning" });
    } else if (msg.startsWith("FINNHUB_PROVIDER_CHANGED")) {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: `Finnhub provider changed: ${msg}`, severity: "warning" });
    } else {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: `Finnhub source unavailable: ${msg}`, severity: "warning" });
    }
    return { items, warnings, used: false };
  }
  return { items, warnings, used: true };
}

async function collectCompanyEvents(
  ticker: string,
  {
    maxResults = 10,
    lookbackDays = 14,
    startDate = "",
    endDate = "",
    sources,
    secFilingTypes = ["8-K", "10-Q", "10-K", "S-3", "DEF14A"],
  }: {
    maxResults?: number;
    lookbackDays?: number;
    startDate?: string;
    endDate?: string;
    sources?: string[];
    secFilingTypes?: string[];
  } = {}
): Promise<{ items: Record<string, unknown>[]; sourcesUsed: string[]; warnings: Record<string, unknown>[]; watermark: string }> {
  const safeMax = clampInt(maxResults, 10, 1, 100);
  const safeLookback = clampInt(lookbackDays, 14, 1, 3650);
  const watermark = new Date().toISOString();
  const { selected, warnings: sourceWarnings } = normalizeSources(sources, [
    "sec", "company_ir", "newswire",
    "yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases",
    "finnhub",
  ]);
  const warnings: Record<string, unknown>[] = [...sourceWarnings];
  const items: Record<string, unknown>[] = [];
  const sourcesUsed: string[] = [];

  if (selected.includes("sec")) {
    const sec = await collectSecEvents(ticker, secFilingTypes, safeMax, watermark, startDate, endDate, safeLookback);
    if (sec.used) sourcesUsed.push("sec");
    items.push(...sec.items);
    warnings.push(...sec.warnings);
  }

  // Fetch company short name once for news relevance filtering (BUG-08)
  const _yfHeadlineStopwords = new Set([
    "corp","corporation","inc","ltd","llc","plc","co","group","holdings",
    "technology","technologies","solutions","services","systems",
    "international","global","energy","capital","financial","finance","resources",
  ]);
  let companyNameTokens: string[] = [];
  try {
    const priceD = await yGet(
      `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=price`
    ) as Record<string, unknown>;
    const priceResult = ((priceD?.quoteSummary as Record<string, unknown> | undefined)
      ?.result as Record<string, unknown>[] | undefined)?.[0]?.price as Record<string, unknown> | undefined;
    const shortName = _str(priceResult?.shortName || priceResult?.longName);
    if (shortName) {
      companyNameTokens = shortName.toLowerCase()
        .replace(/[^a-z0-9]/g, " ")
        .split(/\s+/)
        .filter(w => w.length >= 4 && !_yfHeadlineStopwords.has(w));
    }
  } catch { /* non-fatal */ }

  // Yahoo Finance news tab (explicit or via legacy yahoo_finance or company_ir)
  const needYfNews = selected.includes("yahoo_finance_news")
    || selected.includes("yahoo_finance")
    || selected.includes("company_ir");
  if (needYfNews) {
    const yf = await collectYahooEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback, "news", companyNameTokens);
    if (yf.used) {
      if (selected.includes("yahoo_finance_news")) sourcesUsed.push("yahoo_finance_news");
      if (selected.includes("yahoo_finance") && !sourcesUsed.includes("yahoo_finance")) sourcesUsed.push("yahoo_finance");
      if (selected.includes("company_ir") && !sourcesUsed.includes("company_ir")) {
        sourcesUsed.push("company_ir");
      }
    }
    for (const item of yf.items) {
      const src = _str(item.source);
      if (selected.includes("yahoo_finance_news") && src === "yahoo_finance_news") items.push(item);
      else if (selected.includes("yahoo_finance") && ["yahoo_finance_news", "yahoo_finance_press_releases"].includes(src)) items.push(item);
      else if (selected.includes("company_ir") && ["yahoo_finance_news", "yahoo_finance_press_releases"].includes(src)) items.push(item);
    }
    warnings.push(...yf.warnings);
  }

  // Yahoo Finance press releases tab (explicit or via legacy yahoo_finance)
  const needYfPr = selected.includes("yahoo_finance_press_releases") || selected.includes("yahoo_finance");
  if (needYfPr) {
    const pr = await collectYahooEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback, "press_releases");
    if (pr.used && selected.includes("yahoo_finance_press_releases") && !sourcesUsed.includes("yahoo_finance_press_releases")) {
      sourcesUsed.push("yahoo_finance_press_releases");
    }
    items.push(...pr.items);
    warnings.push(...pr.warnings);
  }

  if (selected.includes("newswire")) {
    const gnw = await collectGlobeNewswireEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback);
    if (gnw.used) sourcesUsed.push("newswire");
    items.push(...gnw.items);
    warnings.push(...gnw.warnings);
  }

  if (selected.includes("finnhub")) {
    const finnhub = await collectFinnhubEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback);
    if (finnhub.used) sourcesUsed.push("finnhub");
    items.push(...finnhub.items);
    warnings.push(...finnhub.warnings);
  }

  const deduped = dedupeEventItems(items, warnings).slice(0, safeMax);
  const uniqueWarnings: Record<string, unknown>[] = [];
  const warningKeys = new Set<string>();
  for (const w of warnings) {
    const key = `${_str(w.code)}|${_str(w.message)}`;
    if (warningKeys.has(key)) continue;
    warningKeys.add(key);
    uniqueWarnings.push(w);
  }
  return { items: deduped, sourcesUsed, warnings: uniqueWarnings, watermark };
}

// ─── Public event / news tools ─────────────────────────────────────────────────

export async function getCompanyNews(
  ticker: string | string[],
  maxResults = 10,
  lookbackDays = 14,
  sources: string[] = ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"],
): Promise<string> {
  // Batch path: fetch each ticker independently and return a per-ticker keyed
  // object (a union of results), matching the other multi-ticker tools. News is
  // fetched per ticker; there is no combined query that could zero out the
  // whole batch under low-news conditions.
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getCompanyNews(t, maxResults, lookbackDays, sources));
  }
  const out = await collectCompanyEvents(ticker, { maxResults, lookbackDays, sources });
  const status = collectionStatus(out.items, out.sourcesUsed, out.warnings);
  const sourceStatus = computeSourceStatus(out.sourcesUsed, out.warnings, out.items, sources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    items: out.items,
    meta: { sourcesUsed: out.sourcesUsed, deduped: true, watermark: out.watermark },
    warnings: out.warnings,
    sourceCoverage,
    sourceStatus,
  };
  if (status) payload.status = status;
  return JSON.stringify(payload);
}

export async function searchCompanyNews(
  ticker: string,
  query: string,
  startDate = "",
  endDate = "",
  sources: string[] = ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"],
  maxResults = 10
): Promise<string> {
  const out = await collectCompanyEvents(ticker, { maxResults, lookbackDays: 14, startDate, endDate, sources });
  const q = query.toLowerCase().trim();
  const matched = q
    ? out.items.filter(item => `${_str(item.title)} ${_str(item.summary)} ${_str(item.source)} ${_str(item.eventType)} ${_str(item.evidenceText)}`.toLowerCase().includes(q))
    : out.items;
  const status = collectionStatus(matched, out.sourcesUsed, out.warnings);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    query,
    items: matched.slice(0, clampInt(maxResults, 10, 1, 100)),
    meta: { sourcesUsed: out.sourcesUsed, deduped: true, watermark: out.watermark },
    warnings: out.warnings,
  };
  if (status) payload.status = status;
  return JSON.stringify(payload);
}

export async function getCompanyPressReleases(
  ticker: string,
  lookbackDays = 90,
  maxResults = 20,
  sources: string[] = ["yahoo_finance_press_releases", "company_ir", "newswire", "sec"]
): Promise<string> {
  const out = await collectCompanyEvents(ticker, {
    maxResults,
    lookbackDays,
    sources,
    secFilingTypes: ["8-K"],
  });
  const releaseTypes = new Set(["company_ir", "press_release", "newswire", "sec_filing", "yahoo_finance_press_releases"]);
  const items = out.items.filter(it => releaseTypes.has(_str(it.sourceType)));
  
  let hasSecEx99Found = false;
  const sec8kEvidence: Record<string, unknown>[] = [];
  let sec8kWithoutEx99Count = 0;
  const processedItems = await Promise.all(items.map(async (it) => {
    if (_str(it.sourceType) === "sec_filing" && _str(it.filingType) === "8-K") {
      const accession = _str(it.accessionNumber);
      const url = _str(it.url);
      sec8kEvidence.push({
        filingType: "8-K",
        filingDate: it.filingDate ?? null,
        acceptedAt: it.acceptedAt ?? null,
        accessionNumber: accession || null,
        documentUrl: url || null,
      });
      const cikMatch = /\/data\/(\d+)\//.exec(url);
      const cik = cikMatch ? parseInt(cikMatch[1], 10) : null;
      if (accession && cik !== null) {
        const ex991Url = await resolveEx991Url(cik, accession);
        if (ex991Url) {
          hasSecEx99Found = true;
          return {
            ...it,
            sourceType: "sec_ex99_found",
            url: ex991Url,
            title: "EX-99.1 exhibit found in 8-K",
          };
        }
      }
      sec8kWithoutEx99Count += 1;
    }
    return it;
  }));

  const warnings = [...out.warnings];
  if (processedItems.length === 0) {
    warnings.push({
      code: "NO_OFFICIAL_RELEASE_SOURCE",
      message: "No company-originated or official release source found in requested window.",
      severity: "warning",
    });
  }
  if (!hasSecEx99Found && sec8kWithoutEx99Count > 0) {
    warnings.push({
      code: "SEC_8K_FOUND_EX99_NOT_FOUND",
      message: "SEC 8-K filing(s) were found, but no EX-99.1 press-release exhibit was resolved.",
      severity: "warning",
      filingsSearched: sec8kWithoutEx99Count,
    });
  }

  let status: string | null = null;
  if (hasSecEx99Found) {
    status = "SEC_EX99_FOUND";
  } else if (sec8kWithoutEx99Count > 0) {
    status = "SEC_8K_FOUND_EX99_NOT_FOUND";
  } else if (processedItems.length === 0) {
    if (sources.includes("yahoo_finance_press_releases")) {
      status = "NO_YAHOO_PRESS_RELEASE";
    } else if (sources.includes("company_ir")) {
      status = "COMPANY_IR_NOT_FOUND";
    } else {
      status = "NOT_FOUND";
    }
  } else {
    status = collectionStatus(processedItems, out.sourcesUsed, warnings);
  }

  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    items: processedItems.slice(0, clampInt(maxResults, 20, 1, 100)),
    meta: { sourcesUsed: out.sourcesUsed, deduped: true, watermark: out.watermark },
    warnings,
  };
  if (sec8kEvidence.length > 0) payload.secEvidence = sec8kEvidence.slice(0, 10);
  if (status) payload.status = status;
  return JSON.stringify(payload);
}

export async function getSecRecentEvents(
  ticker: string,
  filingTypes: string[] = ["8-K", "10-Q", "10-K"],
  lookbackDays = 90,
  maxResults = 20
): Promise<string> {
  const watermark = new Date().toISOString();
  const sec = await collectSecEvents(ticker, filingTypes, clampInt(maxResults, 20, 1, 100), watermark, "", "", clampInt(lookbackDays, 90, 1, 3650));
  const status = collectionStatus(sec.items, sec.used ? ["sec"] : [], sec.warnings);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    items: sec.items,
    meta: { sourcesUsed: sec.used ? ["sec"] : [], watermark },
    warnings: sec.warnings,
  };
  if (status) payload.status = status;
  return JSON.stringify(payload);
}

export async function getPublicEventTimeline(
  ticker: string,
  startDate = "",
  endDate = "",
  sources: string[] = ["sec", "company_ir", "newswire", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"],
  maxResults = 50,
  newestFirst = false
): Promise<string> {
  const out = await collectCompanyEvents(ticker, { maxResults, lookbackDays: 365, startDate, endDate, sources });
  const timeline = out.items
    .filter(item => _str(item.publishedAt))
    .map(item => ({
      timestamp: item.publishedAt,
      eventType: item.eventType,
      title: item.title,
      source: item.source,
      sourceType: item.sourceType,
      url: item.url,
      confidence: item.confidence,
      duplicateGroupId: item.duplicateGroupId,
      sourceRefs: item.sourceRefs ?? [],
    }))
    .sort((a, b) => newestFirst
      ? _str(b.timestamp).localeCompare(_str(a.timestamp))
      : _str(a.timestamp).localeCompare(_str(b.timestamp)))
    .slice(0, clampInt(maxResults, 50, 1, 100));
  const status = collectionStatus(out.items, out.sourcesUsed, out.warnings);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    timeline,
    meta: { sourcesUsed: out.sourcesUsed, deduped: true, watermark: out.watermark },
    warnings: out.warnings,
  };
  if (status) payload.status = status;
  return JSON.stringify(payload);
}

export async function verifyCompanyEvent(
  ticker: string,
  eventQuery: string,
  startDate = "",
  endDate = "",
  sources: string[] = ["sec", "company_ir", "newswire", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"]
): Promise<string> {
  const out = await collectCompanyEvents(ticker, { maxResults: 50, lookbackDays: 365, sources });
  const q = eventQuery.toLowerCase().trim();
  const tokens = q.split(/\s+/).filter(Boolean);
  const isMatch = (item: Record<string, unknown>): boolean => {
    const text = `${_str(item.title)} ${_str(item.summary)} ${_str(item.evidenceText)} ${_str(item.eventType)} ${_str(item.source)}`.toLowerCase();
    if (q && text.includes(q)) return true;
    return tokens.some(t => t.length >= 4 && text.includes(t));
  };
  const matched = out.items.filter(isMatch);
  const inRange = matched.filter(item => {
    if (!startDate && !endDate) return true;
    return withinDateWindow(_str(item.publishedAt) || null, startDate, endDate);
  });
  const official = inRange.filter(item =>
    OFFICIAL_SOURCE_TYPES.has(_str(item.sourceType)) &&
    !!_str(item.url) &&
    ["HIGH", "MEDIUM"].includes(confidenceForSourceType(item.sourceType, item.confidence))
  );
  const staleCutoff = new Date(Date.now() - 90 * 86400000).toISOString().slice(0, 10);
  const staleOnly = matched.length > 0 && matched.every(item => _str(item.publishedAt).slice(0, 10) < staleCutoff);
  const conflicts: Record<string, unknown>[] = out.warnings.some(w => _str(w.code) === "TIMESTAMP_CONFLICT")
    ? [{ type: "timestamp", message: "Conflicting timestamps observed across sources for related events." }]
    : [];
  let status = "NOT_FOUND";
  if (conflicts.length > 0) status = "CONFLICTING";
  else if (official.length > 0) status = "CONFIRMED";
  else if (inRange.length > 0) status = "PARTIAL";
  else if (staleOnly) status = "STALE";

  let companyName = "";
  try {
    const priceD = await yGet(
      `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=price`
    ) as Record<string, unknown>;
    const priceResult = ((priceD?.quoteSummary as Record<string, unknown> | undefined)
      ?.result as Record<string, unknown>[] | undefined)?.[0]?.price as Record<string, unknown> | undefined;
    companyName = _str(priceResult?.shortName || priceResult?.longName);
  } catch { /* non-fatal */ }

  const extractBaseCompanyName = (name: string): string => {
    if (!name) return "";
    let nameClean = name.toUpperCase();
    const suffixes = [
      " INC", " CORP", " CORPORATION", " LTD", " LIMITED", " PLC",
      " CO", " COMPANY", " S.A.", " AG", " GMBH", " S.A.B. DE C.V.",
      " GROUP", " HOLDINGS", " TRUST"
    ];
    for (const p of [".", ",", "/"]) {
      nameClean = nameClean.split(p).join("");
    }
    for (const suff of suffixes) {
      if (nameClean.endsWith(suff)) {
        nameClean = nameClean.slice(0, -suff.length);
      }
    }
    return nameClean.trim();
  };

  const baseCompanyName = extractBaseCompanyName(companyName);
  const tickerUpper = ticker.toUpperCase();
  const tickerPattern = new RegExp(`\\b${tickerUpper}\\b`, "i");

  const relevanceScore = (ev: Record<string, unknown>): string => {
    // 1. Check explicit tickers list
    const evTickers = Array.isArray(ev.tickers) ? ev.tickers.map(String) : [];
    if (evTickers.some(t => t.toUpperCase() === tickerUpper)) {
      return "HIGH";
    }

    // 2. Check source type
    const sourceType = _str(ev.sourceType);
    if (["sec_filing", "sec", "company_ir", "sec_ex99_found"].includes(sourceType)) {
      return "HIGH";
    }

    // 3. Check text content
    const hay = `${_str(ev.title)} ${_str(ev.summary)} ${_str(ev.evidenceText)} ${_str(ev.issuer)}`.toLowerCase();
    if (tickerPattern.test(hay)) {
      return "HIGH";
    }

    if (baseCompanyName && hay.includes(baseCompanyName.toLowerCase())) {
      return "HIGH";
    }
    if (companyName && hay.includes(companyName.toLowerCase())) {
      return "HIGH";
    }

    // Check issuer field explicitly
    const evIssuer = _str(ev.issuer).toUpperCase();
    if (evIssuer.includes(tickerUpper)) {
      return "HIGH";
    }

    return "LOW";
  };

  const best = (official.length > 0 ? official : (inRange.length > 0 ? inRange : matched)).slice(0, 5).map(ev => {
    const relevance = relevanceScore(ev);
    return {
      source: ev.source,
      sourceType: ev.sourceType,
      publishedAt: ev.publishedAt,
      retrievedAt: ev.retrievedAt,
      url: ev.url,
      confidence: confidenceForSourceType(ev.sourceType, ev.confidence),
      relevance,
      evidenceText: shortText(ev.evidenceText || ev.summary || ev.title, 180),
    };
  });

  // If all best evidence is LOW relevance, downgrade status
  if (best.length > 0 && best.every(e => e.relevance === "LOW")) {
    if (status === "CONFIRMED") {
      status = "PARTIAL";
      out.warnings.push({
        code: "LOW_RELEVANCE_EVIDENCE",
        message: `Evidence found but none contain word-boundary match for ticker '${tickerUpper}'. Confidence downgraded.`,
        severity: "warning",
      });
    }
  }
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    query: eventQuery,
    status,
    bestEvidence: best,
    conflicts,
    meta: { sourcesChecked: out.sourcesUsed, watermark: out.watermark },
    warnings: out.warnings,
  });
}

// ── index_sec_filing / get_sec_filing_index ────────────────────────────────────

const _INDEX_KEYWORDS = [
  "china", "greater china", "prc", "geographic", "segment", "revenue",
  "customers", "long-lived assets", "risk factors", "americas", "europe",
  "japan", "asia", "rest of asia",
];

function _stripHtmlTagsIdx(html: string): string {
  const sanitizedHtml = html
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script[^>]*>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style[^>]*>/gi, " ")
    .replace(/\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, " ");
  const blockBroken = sanitizedHtml
    .replace(/<(?:br|\/p|\/div|\/li|\/tr|\/h[1-6]|\/section)\b[^>]*>/gi, "\n")
    .replace(/<(?:p|div|li|tr|h[1-6]|section)\b[^>]*>/gi, "\n");
  const noTags = blockBroken.replace(/<[^>]+>/g, " ");
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
  const lines = decoded.split("\n").map(line => line.replace(/[ \t]+/g, " ").trim());
  return lines.filter(Boolean).join("\n");
}

function _sanitizeFilingHtml(html: string): string {
  return sanitizeFilingHtml(html);
}

function _buildFilingIndexFromHtml(
  html: string,
): { sections: Record<string, unknown>[]; tables: Record<string, unknown>[]; keywordMap: Record<string, string[]> } {
  // Remove scripts/styles/event handlers to reduce noise.
  const sanitized = _sanitizeFilingHtml(html);

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

    // Detect unit scale: default to "unknown"; detect explicitly from context.
    // Lowercase tableHtml separately since preContext is already lowercased,
    // then concatenate to build the search context.
    const preContext = sanitized.slice(Math.max(0, tableStart - 2000), tableStart).toLowerCase();
    const tableContext = tableHtml.toLowerCase() + preContext;
    let unitScale: string;
    if (/billion/.test(tableContext)) unitScale = "billions";
    else if (/million/.test(tableContext)) unitScale = "millions";
    else if (/thousand/.test(tableContext)) unitScale = "thousands";
    else unitScale = "unknown";

    // Confidence: also lower when unitScale is unknown
    const hasYearHeaders = headers.some(h => /\b20\d\d\b/.test(h));
    const hasRowLabels = rowLabels.length > 0;
    const confidence = (hasYearHeaders && hasRowLabels && unitScale !== "unknown") ? "HIGH"
      : (hasYearHeaders || hasRowLabels) ? "MEDIUM"
      : "LOW";

    // Infer title from pre-context
    const preText = _stripHtmlTagsIdx(sanitized.slice(Math.max(0, tableStart - 500), tableStart));
    const lines = preText.split("\n").map(l => cleanFilingDisplayText(l.trim())).filter(Boolean);
    let title = "";
    const candidate = lines[lines.length - 1] ?? "";
    if (candidate.length > 10 && candidate.length < 200 && !looksLikeFilingMarkupText(candidate)) title = candidate;

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
  const resolved = await resolveSecFiling(ticker, filingType, accessionNumber);
  if (!resolved.ok) {
    return JSON.stringify({ ok: false, error: resolved.error, ...resolved.error });
  }
  const filing = resolved.filing;
  accessionNumber = filing.accessionNumber;

  // Check cache
  const cacheKey = `secidx:${ticker.toUpperCase()}:${accessionNumber}:${filing.filingType}`;
  const cached = filingIndexCache.get(cacheKey);
  if (cached && Date.now() - cached.storedAt < FILING_INDEX_TTL_MS) {
    return cached.value;
  }

  // Fetch filing HTML
  const resp = await fetch(filing.documentUrl, { headers: { "User-Agent": EDGAR_UA } });
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
    cik: filing.cikPadded,
    requestedFilingType: filing.requestedFilingType,
    filingType: filing.filingType,
    filingDate: filing.filingDate,
    acceptedAt: filing.acceptedAt,
    accessionNumber: filing.accessionNumber,
    documentUrl: filing.documentUrl,
    index,
    meta: {
      indexedAt,
      source: "sec",
      cacheKey: `${ticker.toUpperCase()}:${accessionNumber}`,
      cacheTtlHours: 24,
    },
    warnings: filing.warnings,
  });

  filingIndexCache.set(cacheKey, { value: result, storedAt: Date.now() });
  return result;
}

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
  return indexSecFiling(ticker, filingType, period, accessionNumber);
}


const SEC_MATERIAL_FORMS_DEFAULT = ["10-K", "10-Q", "8-K", "S-1", "424B", "DEF 14A", "20-F", "6-K"];
const SEC_NOISY_FORMS = new Set(["4", "3", "5", "SC 13G", "SC 13G/A", "SC 13D", "SC 13D/A", "144", "SD", "CORRESP", "UPLOAD", "CT ORDER"]);

export async function listSecMaterialFilings(
  ticker: string,
  forms: string[] | null = null,
  limit: number = 5,
): Promise<string> {
  const resolvedLimit = Math.min(Math.max(1, limit), 20);
  const allowedForms = new Set((forms ?? SEC_MATERIAL_FORMS_DEFAULT).map(f => f.toUpperCase()));

  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not find EDGAR submissions for ticker '${ticker}'` } });
  }

  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const formsList: string[] = (recent.form as string[]) ?? [];
  const dates: string[] = (recent.filingDate as string[]) ?? [];
  const accessions: string[] = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs: string[] = (recent.primaryDocument as string[]) ?? [];
  const acceptedDts: string[] = (recent.acceptanceDateTime as string[]) ?? [];

  const results: Record<string, unknown>[] = [];
  const cikInt = parseInt(cikPadded, 10);
  for (let i = 0; i < formsList.length && results.length < resolvedLimit; i++) {
    const formUpper = String(formsList[i]).toUpperCase();
    if (SEC_NOISY_FORMS.has(formUpper)) continue;
    const matched = Array.from(allowedForms).some(af => formUpper === af || formUpper.startsWith(af));
    if (!matched) continue;

    const acc = accessions[i] ?? "";
    const accClean = acc.replace(/-/g, "");
    const primaryDoc = primaryDocs[i] ?? "";
    const docUrl = primaryDoc ? `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${primaryDoc}` : null;

    results.push({
      filingType: formsList[i],
      filingDate: dates[i] ?? "",
      acceptedAt: acceptedDts[i] ?? null,
      accessionNumber: acc,
      primaryDocument: primaryDoc,
      documentUrl: docUrl,
      xbrl_available: false,
    });
  }

  return JSON.stringify({
    ticker,
    cik: cikPadded,
    filings: results,
    meta: { source: "sec_submissions", materialFormsFilter: Array.from(allowedForms).sort(), retrievedAt: new Date().toISOString() },
  });
}

export async function getSecFilingIntelligence(
  ticker: string,
  filingType: string = "10-K",
  filingIndex: number = 0,
): Promise<string> {
  filingIndex = Math.max(0, Math.min(filingIndex, 9));

  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not find EDGAR submissions for ticker '${ticker}'` } });
  }

  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const formsList: string[] = (recent.form as string[]) ?? [];
  const dates: string[] = (recent.filingDate as string[]) ?? [];
  const accessions: string[] = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs: string[] = (recent.primaryDocument as string[]) ?? [];
  const acceptedDts: string[] = (recent.acceptanceDateTime as string[]) ?? [];

  let matchCount = 0;
  let targetIdx: number | null = null;
  for (let i = 0; i < formsList.length; i++) {
    if (String(formsList[i]).toUpperCase() === filingType.toUpperCase()) {
      if (matchCount === filingIndex) { targetIdx = i; break; }
      matchCount++;
    }
  }

  if (targetIdx === null) {
    return JSON.stringify({ ok: false, error: { code: "NO_FILING_DATA", message: `No ${filingType} filing at index ${filingIndex} for '${ticker}'` } });
  }

  const accessionNumber = accessions[targetIdx] ?? "";
  const filingDate = dates[targetIdx] ?? "";
  const acceptedAt = acceptedDts[targetIdx] ?? null;
  const primaryDoc = primaryDocs[targetIdx] ?? "";
  const cikInt = parseInt(cikPadded, 10);
  const accClean = accessionNumber.replace(/-/g, "");
  const documentUrl = primaryDoc ? `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${primaryDoc}` : null;

  // Attempt to get filing index
  let indexStatus = "UNAVAILABLE";
  let sectionsCount = 0;
  let tablesCount = 0;
  const sectionsList: string[] = [];
  try {
    const indexRaw = await _indexSecFilingImpl(ticker, filingType, "latest", accessionNumber);
    const indexData = JSON.parse(indexRaw) as Record<string, unknown>;
    if (indexData.index) {
      const idx = indexData.index as Record<string, unknown[]>;
      sectionsCount = (idx.sections ?? []).length;
      tablesCount = (idx.tables ?? []).length;
      for (const s of (idx.sections ?? []).slice(0, 20)) {
        const sec = s as Record<string, unknown>;
        if (sec.heading) sectionsList.push(String(sec.heading));
      }
      indexStatus = "OK";
    }
  } catch { indexStatus = "ERROR"; }

  let recommendedQueries = ["revenue by segment", "risk factors", "liquidity and capital resources", "customer concentration", "long-term debt"];
  if (filingType.toUpperCase() === "10-K" || filingType.toUpperCase() === "20-F") {
    recommendedQueries.push("geographic revenue", "R&D expense", "guidance");
  } else if (filingType.toUpperCase() === "10-Q") {
    recommendedQueries.push("quarter-over-quarter revenue", "material events");
  } else if (filingType.toUpperCase() === "8-K") {
    recommendedQueries = ["material event", "exhibit content", "financial results"];
  }

  return JSON.stringify({
    ticker,
    filing: { type: filingType, accessionNumber, filedAt: filingDate, acceptedAt, documentUrl },
    xbrl_available: false,
    xbrl_facts: {},
    index: { sections_count: sectionsCount, tables_count: tablesCount, sections: sectionsList, exhibits_count: 0 },
    recommended_queries: recommendedQueries,
    status: { xbrl: "UNAVAILABLE", index: indexStatus, sections: sectionsCount > 0 ? "AVAILABLE" : "EMPTY" },
  });
}

export async function getSecFilingSectionMarkdown(
  ticker: string,
  section: string = "Item 1A",
  filingType: string = "10-K",
  filingIndex: number = 0,
  maxChars: number = 50000,
): Promise<string> {
  filingIndex = Math.max(0, Math.min(filingIndex, 9));
  maxChars = Math.min(Math.max(1000, maxChars), 100000);

  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not find EDGAR submissions for ticker '${ticker}'` } });
  }

  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const formsList: string[] = (recent.form as string[]) ?? [];
  const accessions: string[] = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs: string[] = (recent.primaryDocument as string[]) ?? [];

  let matchCount = 0;
  let targetIdx: number | null = null;
  for (let i = 0; i < formsList.length; i++) {
    if (String(formsList[i]).toUpperCase() === filingType.toUpperCase()) {
      if (matchCount === filingIndex) { targetIdx = i; break; }
      matchCount++;
    }
  }

  if (targetIdx === null) {
    return JSON.stringify({ ok: false, error: { code: "NO_FILING_DATA", message: `No ${filingType} filing at index ${filingIndex} for '${ticker}'` } });
  }

  const accessionNumber = accessions[targetIdx] ?? "";
  const primaryDoc = primaryDocs[targetIdx] ?? "";
  const cikInt = parseInt(cikPadded, 10);
  const accClean = accessionNumber.replace(/-/g, "");
  const documentUrl = primaryDoc ? `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${primaryDoc}` : null;

  if (!documentUrl) {
    return JSON.stringify({ ok: false, error: { code: "NO_FILING_DATA", message: `No document URL for ${accessionNumber}` } });
  }

  // Fetch filing HTML
  const html = await edgarGetHtml(documentUrl);
  if (!html) {
    return JSON.stringify({ ok: false, error: { code: "PROVIDER_ERROR", message: `Failed to fetch filing document: ${documentUrl}` } });
  }

  // Find section boundaries
  const bounds = findSectionBounds(html, section, maxChars);
  if (bounds.errCode === "SECTION_AMBIGUOUS") {
    return JSON.stringify({ ok: false, error: { code: "SECTION_AMBIGUOUS", message: "The section heading could not be resolved unambiguously." } });
  }

  if (bounds.startIdx === null || bounds.endIdx === null) {
    return JSON.stringify({ ok: false, error: { code: "NO_FILING_DATA", message: `Section '${section}' not found in filing` } });
  }

  const sectionStart = bounds.startIdx;
  const sectionEnd = bounds.endIdx;
  const foundHeading = bounds.foundHeading;

  // Convert to markdown (basic fallback — no sec2md in worker)
  let sectionHtml = html.slice(sectionStart, sectionEnd);
  // Strip scripts/styles iteratively to handle nested/malformed patterns
  const MAX_CELL_CHARS = 60;
  let prevHtml = "";
  while (prevHtml !== sectionHtml) {
    prevHtml = sectionHtml;
    sectionHtml = sectionHtml.replace(/<script\b[^>]*>[\s\S]*?<\/script[^>]*>/gi, "");
    sectionHtml = sectionHtml.replace(/<style\b[^>]*>[\s\S]*?<\/style[^>]*>/gi, "");
  }
  // Convert headers
  for (let lvl = 1; lvl <= 6; lvl++) {
    const prefix = "#".repeat(lvl);
    sectionHtml = sectionHtml.replace(new RegExp(`<h${lvl}[^>]*>([\\s\\S]*?)<\\/h${lvl}>`, "gi"), (_, content) => `\n${prefix} ${(content as string).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim()}\n`);
  }
  // Convert tables to pipe-delimited
  sectionHtml = sectionHtml.replace(/<table[^>]*>[\s\S]*?<\/table>/gi, (tableHtml) => {
    const rows: string[][] = [];
    const trRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
    let trMatch: RegExpExecArray | null;
    while ((trMatch = trRe.exec(tableHtml)) !== null) {
      const cells: string[] = [];
      const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
      let tdMatch: RegExpExecArray | null;
      while ((tdMatch = tdRe.exec(trMatch[1])) !== null) {
        cells.push(tdMatch[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().slice(0, MAX_CELL_CHARS));
      }
      if (cells.length > 0) rows.push(cells);
      if (rows.length >= 50) break;
    }
    if (rows.length === 0) return "";
    const lines: string[] = [];
    for (let i = 0; i < rows.length; i++) {
      lines.push("| " + rows[i].join(" | ") + " |");
      if (i === 0) lines.push("| " + rows[i].map(() => "---").join(" | ") + " |");
    }
    return "\n" + lines.join("\n") + "\n";
  });
  // Convert paragraphs/divs
  sectionHtml = sectionHtml.replace(/<(?:p|div|br)[^>]*\/?>/gi, "\n");
  sectionHtml = sectionHtml.replace(/<\/(?:p|div)>/gi, "\n");
  // Strip remaining tags
  let markdown = sectionHtml.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  markdown = markdown.replace(/\n{3,}/g, "\n\n");

  // Count tables
  const tablesInSection = (html.slice(sectionStart, sectionEnd).match(/<table[^>]*>/gi) ?? []).length;

  let truncated = false;
  if (markdown.length > maxChars) {
    markdown = markdown.slice(0, maxChars).trimEnd();
    truncated = true;
  }

  const wordCount = markdown ? markdown.split(/\s+/).length : 0;
  const warnings: Record<string, unknown>[] = [{
    code: "LIVE_SECTION_EXTRACTION_UNRELIABLE",
    message: "Section markdown is produced by a lossy Worker HTML fallback and is blocked from decision-grade use.",
    severity: "warning",
  }];
  if (wordCount < 50) {
    warnings.push({
      code: "SECTION_MARKDOWN_LOW_CONTENT",
      message: "Extracted section markdown is unusually short; verify with the source filing.",
      severity: "warning",
    });
  }

  return JSON.stringify({
    ticker,
    section: foundHeading || section,
    filingType,
    accessionNumber,
    markdown,
    tables_in_section: tablesInSection,
    word_count: wordCount,
    status: "SECTION_MARKDOWN_UNVERIFIED",
    confidence: "NOT_DECISION_GRADE",
    decisionGrade: false,
    doctrineUse: "BLOCKED",
    warnings,
    source: "html_parser_fallback",
    truncated,
    sectionStartOffset: sectionStart,
    sectionEndOffset: sectionEnd,
    matchedHeading: foundHeading,
    tocSkipped: bounds.tocSkipped,
  });
}

export async function listSecFilingExhibits(ticker: string, accessionNumber: string): Promise<string> {
  if (!accessionNumber.trim()) {
    return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: "accessionNumber is required." } });
  }

  let cik = edgarCikFromAccession(accessionNumber);
  if (!cik) {
    const { cikPadded } = await getSubmissionsForTicker(ticker);
    cik = cikPadded ? parseInt(cikPadded, 10) : null;
  }
  if (!cik) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not resolve CIK for ticker '${ticker}'.` } });
  }

  const { edgarIndexUrl } = edgarBuildFilingUrls(cik, accessionNumber, null);
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    accessionNumber,
    indexUrl: edgarIndexUrl,
    exhibits: await edgarListExhibitsFromIndex(edgarIndexUrl),
  });
}

export async function getSecFilingExhibitContent(
  ticker: string,
  accessionNumber: string,
  fileName: string,
  topics: string[] | null = null,
): Promise<string> {
  if (!accessionNumber.trim()) {
    return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: "accessionNumber is required." } });
  }
  if (!fileName.trim()) {
    return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: "fileName is required." } });
  }

  let cik = edgarCikFromAccession(accessionNumber);
  if (!cik) {
    const { cikPadded } = await getSubmissionsForTicker(ticker);
    cik = cikPadded ? parseInt(cikPadded, 10) : null;
  }
  if (!cik) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not resolve CIK for ticker '${ticker}'.` } });
  }

  const documentUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accessionNumber.replace(/-/g, "")}/${fileName}`;
  const html = await edgarGetHtml(documentUrl, 5_000_000);
  if (!html) {
    return JSON.stringify({ ok: false, error: { code: "FETCH_ERROR", message: `Could not fetch exhibit '${fileName}'.` } });
  }

  const cleanText = htmlToReadableText(html);
  const warnings: Record<string, unknown>[] = [];
  const topicList = (topics ?? []).filter((topic) => typeof topic === "string" && topic.trim()).map((topic) => topic.trim());

  if (topicList.length) {
    const matchedParagraphs = filterParagraphsByTopics(cleanText, topicList);
    if (!matchedParagraphs.length) {
      warnings.push({ code: "NO_TOPIC_MATCHES", message: `No paragraphs matched the provided topics: ${topicList.join(", ")}` });
    }
    return JSON.stringify({
      ticker: ticker.toUpperCase(),
      accessionNumber,
      fileName,
      documentUrl,
      filteredByTopics: topicList,
      matchedParagraphs,
      totalTextLength: cleanText.length,
      warnings,
    });
  }

  const maxChars = 50_000;
  const truncated = cleanText.length > maxChars;
  if (truncated) {
    warnings.push({ code: "TEXT_TRUNCATED", message: `Text truncated from ${cleanText.length} to ${maxChars} characters.` });
  }
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    accessionNumber,
    fileName,
    documentUrl,
    filteredByTopics: null,
    text: cleanText.slice(0, maxChars),
    totalTextLength: cleanText.length,
    truncated,
    warnings,
  });
}

export async function parsePublicTranscript(url: string, topics: string[] | null = null): Promise<string> {
  if (!url.startsWith("https://")) {
    return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: "A valid https:// URL is required." } });
  }

  const html = await fetchPublicHtml(url, 5_000_000);
  if (!html) {
    return JSON.stringify({ ok: false, error: { code: "FETCH_ERROR", message: `Could not fetch URL: ${url}` } });
  }

  const cleanText = htmlToReadableText(html);
  const warnings: Record<string, unknown>[] = [];
  const topicList = (topics ?? []).filter((topic) => typeof topic === "string" && topic.trim()).map((topic) => topic.trim());

  if (topicList.length) {
    const matchedParagraphs = filterParagraphsByTopics(cleanText, topicList);
    if (!matchedParagraphs.length) {
      warnings.push({ code: "NO_TOPIC_MATCHES", message: `No paragraphs matched the provided topics: ${topicList.join(", ")}` });
    }
    return JSON.stringify({
      url,
      filteredByTopics: topicList,
      matchedParagraphs,
      totalTextLength: cleanText.length,
      warnings,
    });
  }

  const maxChars = 50_000;
  const truncated = cleanText.length > maxChars;
  if (truncated) {
    warnings.push({ code: "TEXT_TRUNCATED", message: `Text truncated from ${cleanText.length} to ${maxChars} characters.` });
  }
  return JSON.stringify({
    url,
    filteredByTopics: null,
    text: cleanText.slice(0, maxChars),
    totalTextLength: cleanText.length,
    truncated,
    warnings,
  });
}

function transcriptAttempt(sourceType: string, status: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
  return Object.fromEntries(Object.entries({ sourceType, status, ...extra }).filter(([, v]) => v != null));
}

function nextTranscriptFallback(attemptedSources: Record<string, unknown>[]): Record<string, unknown> | null {
  const statusBySource = new Map(attemptedSources.map((a) => [String(a.sourceType), String(a.status)]));
  if (!statusBySource.has("company_ir") || statusBySource.get("company_ir") === "SKIPPED") {
    return { sourceType: "company_ir", action: "Provide or discover a company IR earnings-call/transcript URL, then call parse_public_transcript." };
  }
  if (!statusBySource.has("public_transcript_url") || statusBySource.get("public_transcript_url") === "SKIPPED") {
    return { sourceType: "public_transcript_url", action: "Call parse_public_transcript with a verified public transcript URL." };
  }
  if (!statusBySource.has("alpha_vantage") || statusBySource.get("alpha_vantage") === "SKIPPED") {
    return { sourceType: "alpha_vantage", action: "Configure ALPHA_VANTAGE_API_KEY and retry when a fiscal quarter is known." };
  }
  return null;
}

function alphaVantageQuarter(period: string, filingDate: unknown = null): string | null {
  const match = String(period ?? "").match(/(\d{4})\s*Q([1-4])/i);
  if (match) return `${match[1]}Q${match[2]}`;
  if (typeof filingDate === "string" && filingDate.trim()) {
    const d = new Date(filingDate.slice(0, 10));
    if (Number.isFinite(d.getTime())) return `${d.getUTCFullYear()}Q${Math.floor(d.getUTCMonth() / 3) + 1}`;
  }
  return null;
}

async function fetchAlphaVantageTranscript(
  ticker: string,
  quarter: string,
  topics: string[] | null = null,
): Promise<{ payload: Record<string, unknown> | null; attempt: Record<string, unknown> }> {
  const apiKey = getWorkerVar("ALPHA_VANTAGE_API_KEY") ?? getWorkerVar("ALPHAVANTAGE_API_KEY");
  if (!apiKey) {
    return {
      payload: null,
      attempt: transcriptAttempt("alpha_vantage", "SKIPPED", {
        reason: "ALPHA_VANTAGE_API_KEY not configured.",
        rateLimit: { provider: "alpha_vantage", used: false },
      }),
    };
  }
  const params = new URLSearchParams({
    function: "EARNINGS_CALL_TRANSCRIPT",
    symbol: ticker.toUpperCase(),
    quarter,
    apikey: apiKey,
  });
  const url = `https://www.alphavantage.co/query?${params.toString()}`;
  const safeUrl = url.replace(apiKey, "REDACTED");
  const rateLimit = { provider: "alpha_vantage", used: true, note: "Alpha Vantage free-tier rate limits may apply." };
  let json: Record<string, unknown> | null = null;
  try {
    const resp = await fetch(url, { headers: { "User-Agent": UA } });
    if (!resp.ok) {
      await resp.body?.cancel();
      return { payload: null, attempt: transcriptAttempt("alpha_vantage", "FETCH_ERROR", { url: safeUrl, quarter, httpStatus: resp.status, rateLimit }) };
    }
    json = await resp.json() as Record<string, unknown>;
  } catch {
    return { payload: null, attempt: transcriptAttempt("alpha_vantage", "FETCH_ERROR", { url: safeUrl, quarter, rateLimit }) };
  }
  if (json.Note || json.Information) {
    return { payload: null, attempt: transcriptAttempt("alpha_vantage", "RATE_LIMITED_OR_UNAVAILABLE", { url: safeUrl, quarter, message: json.Note ?? json.Information, rateLimit }) };
  }
  const rows = Array.isArray(json.transcript) ? json.transcript as Record<string, unknown>[] : [];
  const paragraphs = rows
    .map((row) => {
      const speaker = String(row.speaker ?? row.speaker_name ?? "").trim();
      const content = String(row.content ?? row.text ?? "").trim();
      return content ? (speaker ? `${speaker}: ${content}` : content) : "";
    })
    .filter(Boolean);
  if (!paragraphs.length) {
    return { payload: null, attempt: transcriptAttempt("alpha_vantage", "NOT_FOUND", { url: safeUrl, quarter, rateLimit }) };
  }
  const cleanText = paragraphs.join("\n\n");
  const warnings: Record<string, unknown>[] = [];
  const topicList = (topics ?? []).filter((topic) => typeof topic === "string" && topic.trim()).map((topic) => topic.trim());
  if (topicList.length) {
    const matchedParagraphs = filterParagraphsByTopics(cleanText, topicList);
    if (!matchedParagraphs.length) warnings.push({ code: "NO_TOPIC_MATCHES", message: `No paragraphs matched the provided topics: ${topicList.join(", ")}` });
    return {
      payload: {
        sourceType: "alpha_vantage",
        status: "OK",
        filteredByTopics: topicList,
        matchedParagraphs,
        content: null,
        totalTextLength: cleanText.length,
        truncated: false,
        warnings,
      },
      attempt: transcriptAttempt("alpha_vantage", "SUCCESS", { url: safeUrl, quarter, rateLimit }),
    };
  }
  const maxChars = 50_000;
  const truncated = cleanText.length > maxChars;
  if (truncated) warnings.push({ code: "TEXT_TRUNCATED", message: `Text truncated from ${cleanText.length} to ${maxChars} characters.` });
  return {
    payload: {
      sourceType: "alpha_vantage",
      status: "OK",
      filteredByTopics: null,
      content: cleanText.slice(0, maxChars),
      totalTextLength: cleanText.length,
      truncated,
      warnings,
    },
    attempt: transcriptAttempt("alpha_vantage", "SUCCESS", { url: safeUrl, quarter, rateLimit }),
  };
}

export async function getEarningsCallTranscript(
  ticker: string,
  period: string = "latest",
  topics: string[] | null = null,
): Promise<string> {
  const attemptedSources: Record<string, unknown>[] = [];
  const secSource = await resolveLatestEarningsSecSource(ticker);
  if (!secSource) {
    attemptedSources.push(transcriptAttempt("sec_8k_exhibit", "NOT_FOUND"));
    attemptedSources.push(transcriptAttempt("company_ir", "SKIPPED", { reason: "No company IR transcript/call page URL was discoverable." }));
    attemptedSources.push(transcriptAttempt("public_transcript_url", "SKIPPED", { reason: "No public transcript URL was provided to parse_public_transcript." }));
    const quarter = alphaVantageQuarter(period);
    if (quarter) {
      const alpha = await fetchAlphaVantageTranscript(ticker, quarter, topics);
      attemptedSources.push(alpha.attempt);
      if (alpha.payload) {
        return JSON.stringify({ ticker: ticker.toUpperCase(), period, ...alpha.payload, attemptedSources, nextRecommendedFallback: null });
      }
    } else {
      attemptedSources.push(transcriptAttempt("alpha_vantage", "SKIPPED", { reason: "No fiscal quarter available for Alpha Vantage transcript lookup." }));
    }
    return JSON.stringify({
      ticker: ticker.toUpperCase(),
      period,
      status: "SEC_8K_NOT_FOUND",
      message: "No recent SEC 8-K filing found for this ticker.",
      content: null,
      attemptedSources,
      nextRecommendedFallback: nextTranscriptFallback(attemptedSources),
    });
  }

  const accessionNumber = String(secSource.accessionNumber ?? "");
  let cik = edgarCikFromAccession(accessionNumber);
  if (!cik) {
    const { cikPadded } = await getSubmissionsForTicker(ticker);
    cik = cikPadded ? parseInt(cikPadded, 10) : null;
  }
  if (!cik) {
    attemptedSources.push(transcriptAttempt("sec_8k_exhibit", "FAILED", { accessionNumber, reason: "CIK resolution failed." }));
    return JSON.stringify({
      ticker: ticker.toUpperCase(),
      period,
      status: "CIK_RESOLUTION_FAILED",
      message: "Could not resolve CIK for ticker.",
      content: null,
      attemptedSources,
      nextRecommendedFallback: nextTranscriptFallback(attemptedSources),
    });
  }

  const { edgarIndexUrl } = edgarBuildFilingUrls(cik, accessionNumber, null);
  const exhibits = await edgarListExhibitsFromIndex(edgarIndexUrl);
  const transcriptKeywords = ["TRANSCRIPT", "CONFERENCE CALL", "PROCEEDINGS", "EARNINGS CALL"];
  const transcriptExhibit = exhibits.find((exhibit) => {
    const exhibitType = String(exhibit.type ?? "").toUpperCase();
    const description = String(exhibit.description ?? "").toUpperCase();
    return exhibitType === "EX-99.2" || exhibitType === "EX-99.3" || transcriptKeywords.some((keyword) => description.includes(keyword));
  });

  if (!transcriptExhibit) {
    attemptedSources.push(transcriptAttempt("sec_8k_exhibit", "NOT_FOUND", {
      url: edgarIndexUrl,
      accessionNumber,
      filingDate: secSource.filingDate ?? null,
      exhibitsSearched: exhibits.length,
    }));
    attemptedSources.push(transcriptAttempt("company_ir", "SKIPPED", { reason: "No company IR transcript/call page URL was discoverable." }));
    attemptedSources.push(transcriptAttempt("public_transcript_url", "SKIPPED", { reason: "No public transcript URL was provided to parse_public_transcript." }));
    const quarter = alphaVantageQuarter(period, secSource.filingDate ?? null);
    if (quarter) {
      const alpha = await fetchAlphaVantageTranscript(ticker, quarter, topics);
      attemptedSources.push(alpha.attempt);
      if (alpha.payload) {
        return JSON.stringify({
          ticker: ticker.toUpperCase(),
          period,
          accessionNumber,
          filingDate: secSource.filingDate ?? null,
          ...alpha.payload,
          attemptedSources,
          nextRecommendedFallback: null,
        });
      }
    } else {
      attemptedSources.push(transcriptAttempt("alpha_vantage", "SKIPPED", { reason: "No fiscal quarter available for Alpha Vantage transcript lookup." }));
    }
    return JSON.stringify({
      ticker: ticker.toUpperCase(),
      period,
      status: "SEC_EXHIBIT_NOT_FOUND",
      accessionNumber,
      filingDate: secSource.filingDate ?? null,
      availableExhibits: exhibits.map((exhibit) => ({
        type: exhibit.type ?? "",
        description: exhibit.description ?? "",
        document: exhibit.document ?? "",
      })),
      message: "8-K filing found but no transcript exhibit detected.",
      content: null,
      attemptedSources,
      nextRecommendedFallback: nextTranscriptFallback(attemptedSources),
    });
  }

  const fileName = String(transcriptExhibit.document ?? "");
  const documentUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accessionNumber.replace(/-/g, "")}/${fileName}`;
  const html = await edgarGetHtml(documentUrl, 5_000_000);
  if (!html) {
    attemptedSources.push(transcriptAttempt("sec_8k_exhibit", "FETCH_ERROR", { url: documentUrl, accessionNumber, filingDate: secSource.filingDate ?? null }));
    attemptedSources.push(transcriptAttempt("company_ir", "SKIPPED", { reason: "No company IR transcript/call page URL was discoverable." }));
    attemptedSources.push(transcriptAttempt("public_transcript_url", "SKIPPED", { reason: "No public transcript URL was provided to parse_public_transcript." }));
    const quarter = alphaVantageQuarter(period, secSource.filingDate ?? null);
    if (quarter) {
      const alpha = await fetchAlphaVantageTranscript(ticker, quarter, topics);
      attemptedSources.push(alpha.attempt);
      if (alpha.payload) {
        return JSON.stringify({ ticker: ticker.toUpperCase(), period, ...alpha.payload, attemptedSources, nextRecommendedFallback: null });
      }
    } else {
      attemptedSources.push(transcriptAttempt("alpha_vantage", "SKIPPED", { reason: "No fiscal quarter available for Alpha Vantage transcript lookup." }));
    }
    return JSON.stringify({
      ticker: ticker.toUpperCase(),
      period,
      status: "FETCH_ERROR",
      documentUrl,
      message: `Could not fetch exhibit document '${fileName}'.`,
      content: null,
      attemptedSources,
      nextRecommendedFallback: nextTranscriptFallback(attemptedSources),
    });
  }

  const cleanText = htmlToReadableText(html);
  const warnings: Record<string, unknown>[] = [];
  attemptedSources.push(transcriptAttempt("sec_8k_exhibit", "SUCCESS", { url: documentUrl, accessionNumber, filingDate: secSource.filingDate ?? null }));
  const topicList = (topics ?? []).filter((topic) => typeof topic === "string" && topic.trim()).map((topic) => topic.trim());
  if (topicList.length) {
    const matchedParagraphs = filterParagraphsByTopics(cleanText, topicList);
    if (!matchedParagraphs.length) {
      warnings.push({ code: "NO_TOPIC_MATCHES", message: `No paragraphs matched the provided topics: ${topicList.join(", ")}` });
    }
    return JSON.stringify({
      ticker: ticker.toUpperCase(),
      period,
      status: "OK",
      accessionNumber,
      filingDate: secSource.filingDate ?? null,
      exhibitType: transcriptExhibit.type ?? null,
      documentUrl,
      filteredByTopics: topicList,
      matchedParagraphs,
      totalTextLength: cleanText.length,
      content: null,
      attemptedSources,
      nextRecommendedFallback: null,
      warnings,
    });
  }

  const maxChars = 50_000;
  const truncated = cleanText.length > maxChars;
  if (truncated) {
    warnings.push({ code: "TEXT_TRUNCATED", message: `Text truncated from ${cleanText.length} to ${maxChars} characters.` });
  }
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    period,
    status: "OK",
    accessionNumber,
    filingDate: secSource.filingDate ?? null,
    exhibitType: transcriptExhibit.type ?? null,
    documentUrl,
    filteredByTopics: null,
    content: cleanText.slice(0, maxChars),
    totalTextLength: cleanText.length,
    truncated,
    attemptedSources,
    nextRecommendedFallback: null,
    warnings,
  });
}

function parseObjectJson(raw: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(raw) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function compactExcerpt(text: unknown, maxLen = 240): string {
  const cleaned = String(text ?? "").replace(/\s+/g, " ").trim();
  return cleaned.length <= maxLen ? cleaned : `${cleaned.slice(0, maxLen).trimEnd()}...`;
}

function normalizeStatus(payload: Record<string, unknown>): string {
  const status = String(payload.status ?? payload.code ?? "").toUpperCase();
  if (status === "FILING_NOT_FOUND_TRY_OTHER_TYPE") return "FILING_NOT_FOUND_TRY_OTHER_TYPE";
  if (status === "FILING_TEXT_NOT_AVAILABLE") return "FILING_TEXT_NOT_AVAILABLE";
  if (status === "EXTRACTION_FAILED") return "EXTRACTION_FAILED";
  const source = String(payload.source ?? "").toUpperCase();
  const confidence = String(payload.confidence ?? "").toUpperCase();
  if (source === "FILING_NOT_FOUND_TRY_OTHER_TYPE" || confidence === "FILING_NOT_FOUND_TRY_OTHER_TYPE") return "FILING_NOT_FOUND_TRY_OTHER_TYPE";
  if (source === "EXTRACTION_FAILED" || confidence === "EXTRACTION_FAILED") return "EXTRACTION_FAILED";
  if (source === "NOT_DISCLOSED" || confidence === "NOT_DISCLOSED") return "NOT_DISCLOSED";
  if (source === "CONFLICTING" || confidence === "CONFLICTING") return "CONFLICTING";
  return "NOT_FOUND";
}

function hasWarningCode(warnings: unknown[], code: string): boolean {
  return warnings.some((w) => typeof w === "object" && w != null && (w as Record<string, unknown>).code === code);
}

async function mayBe20FFiler(ticker: string): Promise<boolean> {
  const { submissions } = await getSubmissionsForTicker(ticker);
  if (!submissions || typeof submissions !== "object") return false;
  const filings = (submissions.filings as Record<string, unknown>) ?? {};
  const recent = (filings.recent as Record<string, unknown>) ?? {};
  const forms = Array.isArray(recent.form) ? recent.form : [];
  return forms.some((form: unknown) => String(form ?? "").toUpperCase() === "20-F");
}

export async function extractGeographicRevenue(
  ticker: string,
  region: string,
  filingType = "10-K",
  period = "latest",
  accessionNumber: string | null = null,
  detailLevel = "compact",
): Promise<string> {
  if (!region || !region.trim()) {
    return JSON.stringify({
      ticker,
      factType: "geographic_revenue",
      region,
      period: null,
      rawValue: null,
      rawDenominator: null,
      unit: "USD",
      unitScale: "unknown",
      value: null,
      denominator: null,
      valueRatio: null,
      valuePct: null,
      extractionMethod: "NONE",
      confidence: "NOT_DISCLOSED",
      evidence: {},
      calculation: null,
      warnings: [{ code: "INPUT_VALIDATION_ERROR", message: "region is required", severity: "error" }],
    });
  }
  const payload = parseObjectJson(await getFilingData(ticker, "geographic_revenue", region, filingType, period));
  const idx = parseObjectJson(await getSecFilingIndex(ticker, filingType, period, accessionNumber));
  const evidence = payload.evidence && typeof payload.evidence === "object" ? payload.evidence as Record<string, unknown> : {};
  const warnings = Array.isArray(payload.warnings) ? [...payload.warnings] : [];
  if (payload.value != null && payload.denominator == null && !hasWarningCode(warnings, "DENOMINATOR_NOT_FOUND")) {
    warnings.push({ code: "DENOMINATOR_NOT_FOUND", message: "Could not compute geographic revenue percentage due to missing denominator.", severity: "warning" });
  }
  const out: Record<string, unknown> = {
    ticker,
    factType: "geographic_revenue",
    region,
    period: payload.period ?? null,
    rawValue: payload.rawValue ?? null,
    rawDenominator: payload.rawDenominator ?? null,
    unit: payload.unit ?? "USD",
    unitScale: payload.unitScale ?? "unknown",
    value: payload.value ?? null,
    denominator: payload.denominator ?? null,
    valueRatio: payload.denominator != null ? (payload.valueRatio ?? null) : null,
    valuePct: payload.denominator != null ? (payload.valuePct ?? null) : null,
    extractionMethod: payload.extractionMethod ?? "NONE",
    confidence: payload.confidence ?? "NOT_DISCLOSED",
    evidence: {
      filingType: idx.filingType ?? payload.filingType ?? filingType,
      filingDate: idx.filingDate ?? payload.filingDate ?? null,
      acceptedAt: idx.acceptedAt ?? null,
      accessionNumber: idx.accessionNumber ?? payload.accessionNumber ?? null,
      documentUrl: idx.documentUrl ?? payload.documentUrl ?? null,
      sectionHeading: evidence.sectionHeading ?? null,
      tableTitle: evidence.tableTitle ?? null,
      sourceTableId: evidence.sourceTableId ?? null,
      sourceRows: Array.isArray(evidence.sourceRows) ? evidence.sourceRows : [],
      sourceColumns: Array.isArray(evidence.sourceColumns) ? evidence.sourceColumns : [],
    },
    calculation: payload.calculation ?? null,
    warnings,
  };
  if (filingType.toUpperCase() === "10-K" && String(out.confidence ?? "").toUpperCase() === "NOT_DISCLOSED") {
    const evidenceObj = out.evidence as Record<string, unknown>;
    const evidenceFilingType = String(evidenceObj?.filingType ?? "").toUpperCase();
    let possible20F = evidenceFilingType === "20-F";
    if (!possible20F && (evidenceFilingType === "" || evidenceFilingType === "10-K")) {
      possible20F = await mayBe20FFiler(ticker);
    }
    if (possible20F) {
      // Automatic 20-F fallback: retry extraction with 20-F filing type
      const fallbackPayload = parseObjectJson(await getFilingData(ticker, "geographic_revenue", region, "20-F", period));
      const fallbackIdx = parseObjectJson(await getSecFilingIndex(ticker, "20-F", period, accessionNumber));
      if (fallbackPayload.value != null) {
        // 20-F extraction succeeded — replace output with fallback data
        const fbEvidence = fallbackPayload.evidence && typeof fallbackPayload.evidence === "object"
          ? fallbackPayload.evidence as Record<string, unknown> : {};
        const fbWarnings = Array.isArray(fallbackPayload.warnings) ? [...fallbackPayload.warnings] : [];
        out.period = fallbackPayload.period ?? null;
        out.rawValue = fallbackPayload.rawValue ?? null;
        out.rawDenominator = fallbackPayload.rawDenominator ?? null;
        out.unit = fallbackPayload.unit ?? "USD";
        out.unitScale = fallbackPayload.unitScale ?? "unknown";
        out.value = fallbackPayload.value ?? null;
        out.denominator = fallbackPayload.denominator ?? null;
        out.valueRatio = fallbackPayload.denominator != null ? (fallbackPayload.valueRatio ?? null) : null;
        out.valuePct = fallbackPayload.denominator != null ? (fallbackPayload.valuePct ?? null) : null;
        out.extractionMethod = fallbackPayload.extractionMethod ?? "NONE";
        out.confidence = fallbackPayload.confidence ?? "HIGH";
        out.evidence = {
          filingType: fallbackIdx.filingType ?? fallbackPayload.filingType ?? "20-F",
          filingDate: fallbackIdx.filingDate ?? fallbackPayload.filingDate ?? null,
          acceptedAt: fallbackIdx.acceptedAt ?? null,
          accessionNumber: fallbackIdx.accessionNumber ?? fallbackPayload.accessionNumber ?? null,
          documentUrl: fallbackIdx.documentUrl ?? fallbackPayload.documentUrl ?? null,
          sectionHeading: fbEvidence.sectionHeading ?? null,
          tableTitle: fbEvidence.tableTitle ?? null,
          sourceTableId: fbEvidence.sourceTableId ?? null,
          sourceRows: Array.isArray(fbEvidence.sourceRows) ? fbEvidence.sourceRows : [],
          sourceColumns: Array.isArray(fbEvidence.sourceColumns) ? fbEvidence.sourceColumns : [],
        };
        out.calculation = fallbackPayload.calculation ?? null;
        // Append advisory warning noting automatic 20-F selection
        fbWarnings.push({
          code: "AUTO_20F_FALLBACK",
          message: "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
          severity: "info",
        });
        out.warnings = fbWarnings;
      } else {
        // 20-F extraction also failed — add advisory warning
        if (!hasWarningCode(warnings, "POSSIBLE_20F_FILER")) {
          warnings.push({
            code: "POSSIBLE_20F_FILER",
            message: "POSSIBLE_20F_FILER: Ticker may file 20-F. Retry with filing_type='20-F' or use IR web search.",
            severity: "warning",
          });
        }
      }
    }
  }
  if (String(detailLevel).toLowerCase() === "raw") out.rawContext = { filingIndex: idx };
  return JSON.stringify(out);
}

export async function extractSegmentRevenue(ticker: string, filingType = "10-K", period = "latest", detailLevel = "compact"): Promise<string> {
  const payload = parseObjectJson(await getFilingData(ticker, "segment_revenue", null, filingType, period));
  const segsRaw = Array.isArray(payload.allSegments) ? payload.allSegments : [];
  let segments = segsRaw
    .filter((s) => typeof s === "object" && s != null)
    .map((s) => {
      const row = s as Record<string, unknown>;
      return {
        label: row.segmentLabel ?? null,
        value: row.value ?? null,
        period: row.fiscalYear ? `FY${String(row.fiscalYear)}` : null,
        confidence: "HIGH",
        evidence: {
          filingDate: row.filingDate ?? null,
          accessionNumber: row.accessionNumber ?? null,
        },
      };
    });
  const warnings: Array<Record<string, unknown>> = [];
  // Automatic 20-F fallback for foreign private issuers
  if (segments.length === 0 && filingType.toUpperCase() === "10-K") {
    const possible20F = await mayBe20FFiler(ticker);
    if (possible20F) {
      const fbPayload = parseObjectJson(await getFilingData(ticker, "segment_revenue", null, "20-F", period));
      const fbSegsRaw = Array.isArray(fbPayload.allSegments) ? fbPayload.allSegments : [];
      const fbSegments = fbSegsRaw
        .filter((s) => typeof s === "object" && s != null)
        .map((s) => {
          const row = s as Record<string, unknown>;
          return {
            label: row.segmentLabel ?? null,
            value: row.value ?? null,
            period: row.fiscalYear ? `FY${String(row.fiscalYear)}` : null,
            confidence: "HIGH",
            evidence: {
              filingDate: row.filingDate ?? null,
              accessionNumber: row.accessionNumber ?? null,
            },
          };
        });
      if (fbSegments.length > 0) {
        segments = fbSegments;
        warnings.push({
          code: "AUTO_20F_FALLBACK",
          message: "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
          severity: "info",
        });
      } else {
        warnings.push({
          code: "POSSIBLE_20F_FILER",
          message: "POSSIBLE_20F_FILER: Ticker may file 20-F. Retry with filing_type='20-F' or use IR web search.",
          severity: "warning",
        });
      }
    }
  }
  const out: Record<string, unknown> = { ticker, factType: "segment_revenue", segments, status: segments.length > 0 ? "FOUND" : "NOT_DISCLOSED" };
  if (warnings.length > 0) out.warnings = warnings;
  if (String(detailLevel).toLowerCase() === "raw") out.rawContext = payload;
  return JSON.stringify(out);
}

export async function extractTotalRevenue(ticker: string, filingType = "10-K", period = "latest"): Promise<string> {
  const payload = parseObjectJson(await getFilingData(ticker, "total_revenue", null, filingType, period));
  const value = payload.value ?? null;
  return JSON.stringify({
    ticker,
    factType: "total_revenue",
    value,
    period: payload.period ?? null,
    confidence: payload.confidence ?? (value != null ? "HIGH" : "NOT_DISCLOSED"),
    evidence: {
      filingType: payload.filingType ?? filingType,
      filingDate: payload.filingDate ?? null,
      accessionNumber: payload.accessionNumber ?? null,
      documentUrl: payload.documentUrl ?? null,
    },
    status: value != null ? "FOUND" : normalizeStatus(payload),
  });
}

export async function extractRevenueExposure(
  ticker: string,
  exposureQuery: string,
  filingType = "10-K",
  period = "latest",
  detailLevel = "compact",
): Promise<string> {
  const geo = parseObjectJson(await extractGeographicRevenue(ticker, exposureQuery, filingType, period, null, detailLevel));
  const found = geo.value != null;
  const status = found ? "FOUND_REVENUE_EXPOSURE" : normalizeStatus(geo);
  const warnings = Array.isArray(geo.warnings) ? geo.warnings : [];
  const matches = found
    ? [{
        exposureType: "geographic_revenue",
        label: exposureQuery,
        value: geo.value ?? null,
        denominator: geo.denominator ?? null,
        valueRatio: geo.valueRatio ?? null,
        valuePct: geo.valuePct ?? null,
        period: geo.period ?? null,
        confidence: geo.confidence ?? "HIGH",
        evidence: geo.evidence ?? {},
      }]
    : [];
  return JSON.stringify({
    ticker,
    query: exposureQuery,
    matches,
    status,
    code: found ? null : (geo.code ?? (status === "NOT_DISCLOSED" ? null : status)),
    requestedFilingType: geo.requestedFilingType ?? filingType,
    filingType: (geo.evidence && typeof geo.evidence === "object" ? (geo.evidence as Record<string, unknown>).filingType : null) ?? geo.filingType ?? filingType,
    filingDate: (geo.evidence && typeof geo.evidence === "object" ? (geo.evidence as Record<string, unknown>).filingDate : null) ?? geo.filingDate ?? null,
    accessionNumber: (geo.evidence && typeof geo.evidence === "object" ? (geo.evidence as Record<string, unknown>).accessionNumber : null) ?? geo.accessionNumber ?? null,
    documentUrl: (geo.evidence && typeof geo.evidence === "object" ? (geo.evidence as Record<string, unknown>).documentUrl : null) ?? geo.documentUrl ?? null,
    availableFilingTypes: geo.availableFilingTypes ?? [],
    suggestedFilingTypes: geo.suggestedFilingTypes ?? [],
    warnings,
  });
}

export async function extractRiskFactorMentions(
  ticker: string,
  terms: string[],
  filingType = "10-K",
  _period = "latest",
  detailLevel = "compact",
): Promise<string> {
  const matches: Record<string, unknown>[] = [];
  for (const term of terms ?? []) {
    const search = parseObjectJson(await searchFilingText(ticker, [term], "Risk Factors", filingType, null, 1200, false));
    const list = Array.isArray(search.matches) ? search.matches : [];
    for (const row of list.slice(0, 3)) {
      if (!row || typeof row !== "object") continue;
      const item = row as Record<string, unknown>;
      const excerpt = compactExcerpt(item.context ?? item.excerpt ?? "");
      matches.push({
        term,
        sectionHeading: item.sectionHeading ?? "Risk Factors",
        excerpt,
        excerptAvailable: excerpt.length > 0,
        confidence: "MEDIUM",
        evidence: {
          filingDate: search.filingDate ?? null,
          accessionNumber: search.accessionNumber ?? null,
          documentUrl: search.documentUrl ?? null,
        },
      });
    }
  }
  const out: Record<string, unknown> = { ticker, matches, status: matches.length > 0 ? "FOUND" : "NOT_FOUND" };
  if (String(detailLevel).toLowerCase() === "raw") out.rawTerms = terms ?? [];
  return JSON.stringify(out);
}

export async function extractCustomerConcentration(
  ticker: string,
  filingType = "10-K",
  _period = "latest",
  detailLevel = "compact",
): Promise<string> {
  const search = parseObjectJson(await searchFilingText(ticker, ["major customer", "customers", "customer accounted", "percent of revenue"], null, filingType, null, 1200, false));
  const list = Array.isArray(search.matches) ? search.matches : [];
  const customers: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  for (const row of list) {
    if (!row || typeof row !== "object") continue;
    const item = row as Record<string, unknown>;
    const ctx = String(item.context ?? "");
    const m = ctx.match(/(\d{1,2}(?:\.\d+)?)\s*%/);
    if (!m) continue;
    const pct = Number(m[1]);
    if (!Number.isFinite(pct)) continue;
    const key = pct.toFixed(2);
    if (seen.has(key)) continue;
    seen.add(key);
    customers.push({
      label: `Customer ${String.fromCharCode(65 + customers.length)}`,
      valuePct: pct,
      period: search.fiscalYear ? `FY${String(search.fiscalYear)}` : null,
      confidence: "HIGH",
      evidence: {
        sectionHeading: item.sectionHeading ?? null,
        excerpt: compactExcerpt(ctx),
        filingDate: search.filingDate ?? null,
        accessionNumber: search.accessionNumber ?? null,
        documentUrl: search.documentUrl ?? null,
      },
    });
    if (customers.length >= 5) break;
  }
  const status = customers.length > 0 ? "FOUND" : ((Number(search.matchCount ?? 0) > 0) ? "NOT_DISCLOSED" : "NOT_FOUND");
  const out: Record<string, unknown> = { ticker, customers, status };
  if (String(detailLevel).toLowerCase() === "raw") out.rawMatchCount = search.matchCount ?? 0;
  return JSON.stringify(out);
}

export async function extractChinaExposure(
  ticker: string,
  filingType = "10-K",
  period = "latest",
  accessionNumber: string | null = null,
  detailLevel = "compact",
): Promise<string> {
  const idx = parseObjectJson(await getSecFilingIndex(ticker, filingType, period, accessionNumber));
  const revenue = parseObjectJson(await extractRevenueExposure(ticker, "China", filingType, period, "compact"));
  const idxError = idx.error && typeof idx.error === "object" ? idx.error as Record<string, unknown> : {};
  const idxStatus = String(idx.status ?? idx.code ?? idxError.code ?? "").toUpperCase();
  const revenueStatus = String(revenue.status ?? revenue.code ?? "").toUpperCase();
  if (idxStatus === "FILING_NOT_FOUND_TRY_OTHER_TYPE" || revenueStatus === "FILING_NOT_FOUND_TRY_OTHER_TYPE") {
    const source = idxStatus === "FILING_NOT_FOUND_TRY_OTHER_TYPE" ? idx : revenue;
    return JSON.stringify({
      ticker,
      exposureType: "china_exposure",
      filingType,
      filingDate: null,
      accessionNumber: null,
      documentUrl: null,
      revenueExposure: { status: "FILING_NOT_FOUND_TRY_OTHER_TYPE", value: null, denominator: null, valueRatio: null, valuePct: null, confidence: "FILING_NOT_FOUND_TRY_OTHER_TYPE", evidence: [] },
      manufacturingExposure: { status: "NOT_FOUND", confidence: "LOW", evidence: [] },
      entityExposure: { status: "NOT_FOUND", entities: [], confidence: "LOW", evidence: [] },
      bankExposure: { status: "NOT_FOUND", entities: [], confidence: "LOW", evidence: [] },
      riskFactorExposure: { status: "NOT_FOUND", confidence: "LOW", evidence: [] },
      overallStatus: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
      code: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
      requestedFilingType: source.requestedFilingType ?? filingType,
      availableFilingTypes: source.availableFilingTypes ?? [],
      suggestedFilingTypes: source.suggestedFilingTypes ?? [],
      warnings: source.warnings ?? [],
    });
  }
  const index = idx.index && typeof idx.index === "object" ? idx.index as Record<string, unknown> : {};
  const sections = Array.isArray(index.sections) ? index.sections : [];
  const tables = Array.isArray(index.tables) ? index.tables : [];

  const collect = (terms: string[]): Record<string, unknown>[] => {
    const found: Record<string, unknown>[] = [];
    for (const sec of sections) {
      if (!sec || typeof sec !== "object") continue;
      const s = sec as Record<string, unknown>;
      const heading = String(s.heading ?? "");
      const low = heading.toLowerCase();
      for (const term of terms) {
        if (low.includes(term.toLowerCase())) {
          const excerpt = compactExcerpt(heading);
          found.push({ source: "section", term, sectionHeading: heading, excerpt, excerptAvailable: excerpt.length > 0 });
        }
      }
    }
    for (const tbl of tables) {
      if (!tbl || typeof tbl !== "object") continue;
      const t = tbl as Record<string, unknown>;
      const rowLabels = Array.isArray(t.rowLabels) ? t.rowLabels.map((v) => String(v)) : [];
      const haystackRaw = `${String(t.title ?? "")} ${rowLabels.join(" ")}`;
      const haystack = haystackRaw.toLowerCase();
      for (const term of terms) {
        if (haystack.includes(term.toLowerCase())) {
          const excerpt = compactExcerpt(haystackRaw);
          found.push({ source: "table", term, tableTitle: t.title ?? null, sourceTableId: t.tableId ?? null, sectionId: t.sectionId ?? null, excerpt, excerptAvailable: excerpt.length > 0 });
        }
      }
    }
    return found;
  };

  const entityTerms = ["Tongmei", "JinMei", "BoYu"];
  const bankTerms = ["Bank of China"];
  const manuTerms = ["manufacturing", "production", "supply chain", "fab"];
  const riskMentions = parseObjectJson(await extractRiskFactorMentions(ticker, ["China", "tariff", "export control", "Bank of China"], filingType, period, "compact"));

  const entityEvidence = collect(entityTerms);
  const bankEvidence = collect(bankTerms);
  const manuEvidence = collect(manuTerms);
  const riskEvidence = Array.isArray(riskMentions.matches) ? riskMentions.matches : [];
  for (const ev of riskEvidence) {
    if (ev && typeof ev === "object") {
      const item = ev as Record<string, unknown>;
      const excerpt = compactExcerpt(item.excerpt ?? item.context ?? "");
      if (excerpt) {
        item.excerpt = excerpt;
        item.excerptAvailable = true;
      } else {
        delete item.excerpt;
        item.excerptAvailable = false;
      }
    }
  }
  const nonRevenueFound = entityEvidence.length > 0 || bankEvidence.length > 0 || manuEvidence.length > 0 || riskEvidence.length > 0;
  const revStatus = String(revenue.status ?? "NOT_FOUND");
  const revFound = revStatus === "FOUND_REVENUE_EXPOSURE";

  const revenueMatches = Array.isArray(revenue.matches) ? revenue.matches : [];
  const firstRevenue = revenueMatches.length > 0 && typeof revenueMatches[0] === "object" ? revenueMatches[0] as Record<string, unknown> : {};

  const overallStatus = revFound
    ? "FOUND_REVENUE_EXPOSURE"
    : nonRevenueFound
      ? "FOUND_NON_REVENUE_EXPOSURE"
      : revStatus === "NOT_DISCLOSED"
        ? "NOT_DISCLOSED"
        : revStatus === "CONFLICTING"
          ? "CONFLICTING"
          : "NOT_FOUND";

  const out: Record<string, unknown> = {
    ticker,
    exposureType: "china_exposure",
    filingType: idx.filingType ?? filingType,
    filingDate: idx.filingDate ?? null,
    accessionNumber: idx.accessionNumber ?? null,
    documentUrl: idx.documentUrl ?? null,
    revenueExposure: {
      status: revFound ? "FOUND" : (revStatus as string),
      value: firstRevenue.value ?? null,
      denominator: firstRevenue.denominator ?? null,
      valueRatio: firstRevenue.valueRatio ?? null,
      valuePct: firstRevenue.valuePct ?? null,
      confidence: revFound ? "HIGH" : (revStatus === "NOT_DISCLOSED" ? "NOT_DISCLOSED" : "LOW"),
      evidence: firstRevenue.evidence ?? [],
    },
    manufacturingExposure: { status: manuEvidence.length > 0 ? "FOUND" : "NOT_FOUND", confidence: "MEDIUM", evidence: manuEvidence },
    entityExposure: { status: entityEvidence.length > 0 ? "FOUND" : "NOT_FOUND", entities: entityEvidence.length > 0 ? entityTerms : [], confidence: "MEDIUM", evidence: entityEvidence },
    bankExposure: { status: bankEvidence.length > 0 ? "FOUND" : "NOT_FOUND", entities: bankEvidence.length > 0 ? bankTerms : [], confidence: "MEDIUM", evidence: bankEvidence },
    riskFactorExposure: { status: riskEvidence.length > 0 ? "FOUND" : "NOT_FOUND", confidence: "MEDIUM", evidence: riskEvidence },
    overallStatus,
    warnings: [],
  };
  if (String(detailLevel).toLowerCase() === "raw") out.rawContext = { filingIndex: idx };
  return JSON.stringify(out);
}

// XBRL synonym lookup for extractExposure
const EXPOSURE_SYNONYMS: Record<string, string[]> = {
  china:          ["china", "greaterchinese", "greaterchina", "prc", "hongkong", "taiwan", "chinesesimplified"],
  "greater china":["greaterchina", "greaterchinese", "china", "hongkong", "taiwan"],
  europe:         ["europe", "emea", "europeanunion"],
  japan:          ["japan"],
  americas:       ["americas", "northamerica", "unitedstates", "us"],
  russia:         ["russia", "russianfederation"],
  india:          ["india"],
  "rest of asia": ["restofasia", "asiapacific", "asiaxjapan"],
};

// Named entities known to be associated with China manufacturing / supply chain
const CHINA_NAMED_ENTITIES = ["foxconn", "tsmc", "luxshare", "catl", "byd", "tongmei", "catcher", "pegatron"];

// Operational terms for geographic exposure scanning
const OPERATIONAL_TERMS = ["manufacturing", "assembly", "supply chain", "factory", "production facility"];

export async function extractExposure(
  ticker: string,
  topic: string,
  filingType = "10-K",
  period = "latest",
  includeRiskFactors = true,
): Promise<string> {
  if (!ticker || !ticker.trim()) {
    return JSON.stringify({ ticker, topic, overallStatus: "NOT_FOUND", warnings: [{ code: "INPUT_VALIDATION_ERROR", message: "ticker is required" }] });
  }
  if (!topic || !topic.trim()) {
    return JSON.stringify({ ticker, topic, overallStatus: "NOT_FOUND", warnings: [{ code: "INPUT_VALIDATION_ERROR", message: "topic is required" }] });
  }

  const topicLower = topic.trim().toLowerCase();
  const warnings: Record<string, unknown>[] = [];

  // Resolve filing metadata via index
  const idx = parseObjectJson(await getSecFilingIndex(ticker, filingType, period, null));

  const filingDate: string | null = typeof idx.filingDate === "string" ? idx.filingDate : null;
  const accessionNumber: string | null = typeof idx.accessionNumber === "string" ? idx.accessionNumber : null;
  const documentUrl: string | null = typeof idx.documentUrl === "string" ? idx.documentUrl : null;

  // Determine the region name to use for XBRL lookup (try synonym table, otherwise use raw topic)
  const synonymEntry = Object.entries(EXPOSURE_SYNONYMS).find(([key]) => key === topicLower);
  const regionLabel = synonymEntry ? (topicLower === "china" ? "Greater China" : topic) : topic;

  // --- Fan-out: revenue extraction, operational scan, risk factor scan (all parallel) ---

  // 1. Revenue extraction via existing geographic revenue logic
  const revenuePromise = extractGeographicRevenue(ticker, regionLabel, filingType, period, null, "compact");

  // 2. Operational/entity scan via searchFilingText
  const operationalPromise = searchFilingText(ticker, [topicLower], null, filingType, null, 600, false);

  // 3. Entity scan — only for China topic
  const isChina = synonymEntry != null && topicLower === "china" || (synonymEntry?.includes("china") ?? false);
  const entityPromise = isChina
    ? searchFilingText(ticker, CHINA_NAMED_ENTITIES.slice(0, 3), null, filingType, null, 400, false)
    : Promise.resolve(null as string | null);

  // 4. Risk factor scan
  const riskPromise = includeRiskFactors
    ? extractRiskFactorMentions(ticker, [topicLower], filingType, period, "compact")
    : Promise.resolve(null as string | null);

  const [revenueResult, operationalResult, entityResult, riskResult] = await Promise.allSettled([
    revenuePromise,
    operationalPromise,
    entityPromise,
    riskPromise,
  ]);

  // --- Process revenue ---
  const geo = revenueResult.status === "fulfilled" ? parseObjectJson(revenueResult.value) : {};
  const geoValue: number | null = typeof geo.value === "number" ? geo.value : null;
  const geoDenominator: number | null = typeof geo.denominator === "number" ? geo.denominator : null;
  const geoConf = String(geo.confidence ?? "LOW").toUpperCase();
  const geoMethod = String(geo.extractionMethod ?? "NONE").toUpperCase();
  const geoEvidence = geo.evidence && typeof geo.evidence === "object" ? geo.evidence as Record<string, unknown> : {};
  let revStatus: string;
  if (geoValue != null) {
    revStatus = "FOUND";
  } else if (geoConf === "NOT_DISCLOSED") {
    revStatus = "NOT_DISCLOSED";
  } else {
    revStatus = "NOT_FOUND";
  }

  const revenueExposure: Record<string, unknown> = {
    status: revStatus,
    value: geoValue,
    denominator: geoDenominator,
    valuePct: geoDenominator != null ? (geo.valuePct ?? null) : null,
    valueRatio: geoDenominator != null ? (geo.valueRatio ?? null) : null,
    unit: geo.unit ?? "USD",
    region: geoEvidence.sectionHeading ?? regionLabel,
    period: geo.period ?? null,
    extractionMethod: geoMethod === "NONE" ? "NONE" : geoMethod,
    confidence: geoConf === "NOT_DISCLOSED" ? "LOW" : (geoValue != null ? (geoConf || "MEDIUM") : "LOW"),
    evidence: {
      sectionHeading: geoEvidence.sectionHeading ?? null,
      sourceRows: Array.isArray(geoEvidence.sourceRows) ? geoEvidence.sourceRows : [],
      sourceColumns: Array.isArray(geoEvidence.sourceColumns) ? geoEvidence.sourceColumns : [],
    },
  };

  // --- Process operational exposure ---
  const opsRaw = operationalResult.status === "fulfilled" && operationalResult.value
    ? parseObjectJson(operationalResult.value)
    : {};
  const opsMatches: Record<string, unknown>[] = Array.isArray(opsRaw.matches)
    ? (opsRaw.matches as Record<string, unknown>[]).filter((m) => m && typeof m === "object")
    : [];

  // Scan for operational keyword co-occurrence in text excerpts
  const opsEvidence: Record<string, unknown>[] = [];
  const foundOpTerms = new Set<string>();
  for (const m of opsMatches) {
    const contextText = String(m.contextText ?? m.context ?? "").toLowerCase();
    for (const opTerm of OPERATIONAL_TERMS) {
      if (contextText.includes(opTerm)) {
        foundOpTerms.add(opTerm);
        if (opsEvidence.length < 5) {
          opsEvidence.push({
            term: opTerm,
            excerpt: compactExcerpt(m.contextText ?? m.context ?? "", 200),
            section: String(m.sectionHeading ?? ""),
          });
        }
        break;
      }
    }
    if (opsEvidence.length >= 5) break;
  }

  const operationalExposure: Record<string, unknown> = {
    status: opsEvidence.length > 0 ? "FOUND" : "NOT_FOUND",
    terms: Array.from(foundOpTerms),
    evidence: opsEvidence,
  };

  // --- Process entity exposure (China only) ---
  let entityExposure: Record<string, unknown>;
  if (isChina && entityResult.status === "fulfilled" && entityResult.value) {
    const entRaw = parseObjectJson(entityResult.value);
    const entMatches: Record<string, unknown>[] = Array.isArray(entRaw.matches)
      ? (entRaw.matches as Record<string, unknown>[]).filter((m) => m && typeof m === "object")
      : [];
    const foundEntities = new Set<string>();
    const entEvidence: Record<string, unknown>[] = [];
    for (const m of entMatches) {
      const termLow = String(m.term ?? "").toLowerCase();
      if (termLow && !foundEntities.has(termLow)) {
        foundEntities.add(termLow);
        if (entEvidence.length < 5) {
          entEvidence.push({
            entity: String(m.term ?? ""),
            excerpt: compactExcerpt(m.contextText ?? m.context ?? "", 200),
            section: String(m.sectionHeading ?? ""),
          });
        }
      }
      if (entEvidence.length >= 5) break;
    }
    entityExposure = {
      status: entEvidence.length > 0 ? "FOUND" : "NOT_FOUND",
      entities: Array.from(foundEntities),
      evidence: entEvidence,
    };
  } else {
    entityExposure = { status: "NOT_FOUND", entities: [], evidence: [] };
  }

  // --- Process risk factor exposure ---
  let riskFactorExposure: Record<string, unknown>;
  if (includeRiskFactors && riskResult.status === "fulfilled" && riskResult.value) {
    const riskRaw = parseObjectJson(riskResult.value);
    const riskMatches: Record<string, unknown>[] = Array.isArray(riskRaw.matches)
      ? (riskRaw.matches as Record<string, unknown>[]).filter((m) => m && typeof m === "object")
      : [];
    const riskEvidence = riskMatches.slice(0, 5).map((m) => ({
      excerpt: compactExcerpt(m.excerpt ?? m.context ?? m.contextText ?? "", 200),
      section: String(m.sectionHeading ?? "Risk Factors"),
    }));
    riskFactorExposure = {
      status: riskEvidence.length > 0 ? "FOUND" : "NOT_FOUND",
      mentionCount: riskMatches.length,
      evidence: riskEvidence,
    };
  } else {
    riskFactorExposure = { status: "NOT_FOUND", mentionCount: 0, evidence: [] };
  }

  // --- Determine overallStatus ---
  const nonRevenueFound = (opsEvidence.length > 0) ||
    (Array.isArray(entityExposure.evidence) && (entityExposure.evidence as unknown[]).length > 0) ||
    (Array.isArray(riskFactorExposure.evidence) && (riskFactorExposure.evidence as unknown[]).length > 0);

  let overallStatus: string;
  if (revStatus === "FOUND") {
    overallStatus = "FOUND_REVENUE_EXPOSURE";
  } else if (nonRevenueFound) {
    overallStatus = "FOUND_NON_REVENUE_EXPOSURE";
  } else if (revStatus === "NOT_DISCLOSED") {
    overallStatus = "NOT_DISCLOSED";
  } else {
    overallStatus = "NOT_FOUND";
  }

  // Propagate any errors as warnings
  if (revenueResult.status === "rejected") warnings.push({ code: "REVENUE_EXTRACTION_ERROR", message: String(revenueResult.reason), severity: "warning" });
  if (operationalResult.status === "rejected") warnings.push({ code: "OPERATIONAL_SCAN_ERROR", message: String(operationalResult.reason), severity: "warning" });

  return JSON.stringify({
    ticker,
    topic: topicLower,
    filingType: idx.filingType ?? filingType,
    filingDate,
    accessionNumber,
    documentUrl,
    revenueExposure,
    operationalExposure,
    entityExposure,
    riskFactorExposure,
    overallStatus,
    warnings,
  });
}

export async function querySecFilingIndex(
  ticker: string,
  filingType = "10-K",
  period = "latest",
  accessionNumber: string | null = null,
  queryType = "",
  params: Record<string, unknown> = {},
  returnEvidence = true,
  detailLevel = "compact",
): Promise<string> {
  const detail = String(detailLevel || "compact").toLowerCase();
  if (!["compact", "evidence", "raw"].includes(detail)) {
    return JSON.stringify({
      status: "INPUT_VALIDATION_ERROR",
      queryType,
      ticker,
      filingType,
      period,
      answer: null,
      confidence: "NOT_DISCLOSED",
      evidence: [],
      warnings: [{ code: "INPUT_VALIDATION_ERROR", message: "detailLevel must be one of: compact, evidence, raw" }],
    });
  }

  const query = String(queryType || "").trim();
  const warnings: Record<string, unknown>[] = [];

  const shapeEvidence = (ev: Record<string, unknown>): Record<string, unknown> => {
    const out: Record<string, unknown> = {
      filingDate: ev.filingDate ?? null,
      acceptedAt: ev.acceptedAt ?? null,
      accessionNumber: ev.accessionNumber ?? null,
      documentUrl: ev.documentUrl ?? null,
      sectionHeading: ev.sectionHeading ?? null,
      tableTitle: ev.tableTitle ?? null,
      sourceTableId: ev.sourceTableId ?? null,
    };
    if (detail === "evidence" || detail === "raw") {
      out.sourceRows = Array.isArray(ev.sourceRows) ? ev.sourceRows : [];
      out.sourceColumns = Array.isArray(ev.sourceColumns) ? ev.sourceColumns : [];
      if (ev.excerpt != null) out.excerpt = ev.excerpt;
    }
    return out;
  };

  const result = (
    status: string,
    answer: Record<string, unknown> | null,
    confidence: string,
    evidenceItems: Record<string, unknown>[] = [],
    warnItems: Record<string, unknown>[] = [],
  ): string => JSON.stringify({
    status,
    queryType: query,
    ticker,
    filingType,
    period,
    answer,
    confidence,
    evidence: returnEvidence ? evidenceItems : [],
    warnings: warnItems,
  });

  const missingParam = (name: string): string => result(
    "INPUT_VALIDATION_ERROR",
    null,
    "NOT_DISCLOSED",
    [],
    [{ code: "INPUT_VALIDATION_ERROR", message: `Missing required params.${name} for query_type=${query}` }],
  );

  const supported = new Set([
    "geographic_revenue_share",
    "revenue_exposure",
    "china_exposure",
    "risk_factor_mentions",
    "customer_concentration",
    "total_revenue",
    "segment_revenue",
  ]);
  if (!supported.has(query)) {
    return mcpFailure(
      "query_sec_filing_index",
      "UNSUPPORTED_QUERY_TYPE",
      `Unsupported query type '${query}'. Supported types are: ${Array.from(supported).sort().join(", ")}`,
      {
        metaExtra: {
          supportedQueryTypes: Array.from(supported),
        }
      }
    );
  }

  if (query === "geographic_revenue_share") {
    const region = String(params.region ?? "").trim();
    if (!region) return missingParam("region");
    const geo = parseObjectJson(await extractGeographicRevenue(ticker, region, filingType, period, accessionNumber, detail));
    const evidenceObj = geo.evidence && typeof geo.evidence === "object" ? geo.evidence as Record<string, unknown> : {};
    const evidence = Object.keys(evidenceObj).length > 0 ? [shapeEvidence(evidenceObj)] : [];
    let status = geo.value != null ? "ANSWERED" : normalizeStatus(geo);
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    const answer = {
      region,
      value: geo.value ?? null,
      denominator: geo.denominator ?? null,
      valueRatio: geo.valueRatio ?? null,
      valuePct: geo.valuePct ?? null,
      unit: geo.unit ?? "USD",
    };
    const confidence = String(geo.confidence ?? (status === "ANSWERED" ? "HIGH" : "NOT_DISCLOSED"));
    return result(status, answer, confidence, evidence, warnings);
  }

  if (query === "revenue_exposure") {
    const exposureQuery = String(params.exposure_query ?? "").trim();
    if (!exposureQuery) return missingParam("exposure_query");
    const rex = parseObjectJson(await extractRevenueExposure(ticker, exposureQuery, filingType, period, detail));
    const matches = Array.isArray(rex.matches) ? rex.matches.filter((m) => m && typeof m === "object") as Record<string, unknown>[] : [];
    const first = matches.length > 0 ? matches[0] : {};
    const evidenceObj = first.evidence && typeof first.evidence === "object" ? first.evidence as Record<string, unknown> : {};
    const evidence = Object.keys(evidenceObj).length > 0 ? [shapeEvidence(evidenceObj)] : [];
    let status = matches.length > 0 && String(rex.status ?? "") === "FOUND_REVENUE_EXPOSURE" ? "ANSWERED" : String(rex.status ?? "NOT_FOUND");
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    return result(status, {
      exposureQuery,
      value: first.value ?? null,
      denominator: first.denominator ?? null,
      valueRatio: first.valueRatio ?? null,
      valuePct: first.valuePct ?? null,
      period: first.period ?? null,
    }, String(first.confidence ?? (status === "ANSWERED" ? "HIGH" : "NOT_DISCLOSED")), evidence, warnings);
  }

  if (query === "china_exposure") {
    const china = parseObjectJson(await extractChinaExposure(ticker, filingType, period, accessionNumber, detail));
    const overall = String(china.overallStatus ?? "NOT_FOUND");
    const answer = {
      revenueExposure: china.revenueExposure ?? null,
      manufacturingExposure: china.manufacturingExposure ?? null,
      entityExposure: china.entityExposure ?? null,
      bankExposure: china.bankExposure ?? null,
      riskFactorExposure: china.riskFactorExposure ?? null,
      overallStatus: overall,
    };
    const evidence: Record<string, unknown>[] = [];
    for (const key of ["revenueExposure", "manufacturingExposure", "entityExposure", "bankExposure", "riskFactorExposure"]) {
      const block = china[key];
      if (!block || typeof block !== "object") continue;
      const b = block as Record<string, unknown>;
      if (b.evidence && typeof b.evidence === "object" && !Array.isArray(b.evidence)) evidence.push(shapeEvidence(b.evidence as Record<string, unknown>));
      if (Array.isArray(b.evidence)) {
        for (const item of b.evidence) {
          if (item && typeof item === "object") evidence.push(shapeEvidence(item as Record<string, unknown>));
        }
      }
    }
    let status = (overall === "FOUND_REVENUE_EXPOSURE" || overall === "FOUND_NON_REVENUE_EXPOSURE") ? "ANSWERED" : overall;
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    const confidence = status === "ANSWERED" ? "HIGH" : (overall === "NOT_DISCLOSED" ? "NOT_DISCLOSED" : "LOW");
    return result(status, answer, confidence, evidence, warnings);
  }

  if (query === "risk_factor_mentions") {
    const terms = Array.isArray(params.terms) ? params.terms.map((t) => String(t)) : [];
    if (terms.length === 0) return missingParam("terms");
    const risk = parseObjectJson(await extractRiskFactorMentions(ticker, terms, filingType, period, detail));
    const matches = Array.isArray(risk.matches) ? risk.matches.filter((m) => m && typeof m === "object") as Record<string, unknown>[] : [];
    const evidence = matches
      .map((m) => (m.evidence && typeof m.evidence === "object" && !Array.isArray(m.evidence)) ? shapeEvidence(m.evidence as Record<string, unknown>) : null)
      .filter((m): m is Record<string, unknown> => m != null);
    let status = matches.length > 0 ? "ANSWERED" : String(risk.status ?? "NOT_FOUND");
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    return result(status, { terms, matches }, status === "ANSWERED" ? "MEDIUM" : "LOW", evidence, warnings);
  }

  if (query === "customer_concentration") {
    const customerLabel = String(params.customer_label ?? "").trim();
    const cust = parseObjectJson(await extractCustomerConcentration(ticker, filingType, period, detail));
    let customers = Array.isArray(cust.customers) ? cust.customers.filter((c) => c && typeof c === "object") as Record<string, unknown>[] : [];
    if (customerLabel) customers = customers.filter((c) => String(c.label ?? "").toLowerCase() === customerLabel.toLowerCase());
    const evidence = customers
      .map((c) => (c.evidence && typeof c.evidence === "object" && !Array.isArray(c.evidence)) ? shapeEvidence(c.evidence as Record<string, unknown>) : null)
      .filter((m): m is Record<string, unknown> => m != null);
    let status = customers.length > 0 ? "ANSWERED" : String(cust.status ?? "NOT_FOUND");
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    const confidence = status === "ANSWERED" ? "HIGH" : (status === "NOT_DISCLOSED" ? "NOT_DISCLOSED" : "LOW");
    return result(status, { customerLabel: customerLabel || null, customers }, confidence, evidence, warnings);
  }

  if (query === "total_revenue") {
    const total = parseObjectJson(await extractTotalRevenue(ticker, filingType, period));
    const evidenceObj = total.evidence && typeof total.evidence === "object" ? total.evidence as Record<string, unknown> : {};
    const evidence = Object.keys(evidenceObj).length > 0 ? [shapeEvidence(evidenceObj)] : [];
    let status = total.value != null ? "ANSWERED" : String(total.status ?? "NOT_FOUND");
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    return result(
      status,
      { value: total.value ?? null, period: total.period ?? null, unit: "USD" },
      String(total.confidence ?? (status === "ANSWERED" ? "HIGH" : "NOT_DISCLOSED")),
      evidence,
      warnings,
    );
  }

  const segmentName = String(params.segment ?? "").trim();
  const seg = parseObjectJson(await extractSegmentRevenue(ticker, filingType, period, detail));
  let segments = Array.isArray(seg.segments) ? seg.segments.filter((s) => s && typeof s === "object") as Record<string, unknown>[] : [];
  if (segmentName) segments = segments.filter((s) => String(s.label ?? "").toLowerCase() === segmentName.toLowerCase());
  const evidence = segments
    .map((s) => (s.evidence && typeof s.evidence === "object" && !Array.isArray(s.evidence)) ? shapeEvidence(s.evidence as Record<string, unknown>) : null)
    .filter((m): m is Record<string, unknown> => m != null);
  let status = segments.length > 0 ? "ANSWERED" : (segmentName ? "NOT_FOUND" : String(seg.status ?? "NOT_FOUND"));
  if (status === "ANSWERED" && evidence.length === 0) {
    status = "NOT_FOUND";
    warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
  }
  const confidence = status === "ANSWERED" ? "HIGH" : (status === "NOT_DISCLOSED" ? "NOT_DISCLOSED" : "LOW");
  return result(status, { segment: segmentName || null, segments }, confidence, evidence, warnings);
}

function nowIsoUtc(): string {
  return new Date().toISOString();
}

function toIsoUtc(ts: unknown): string | null {
  if (typeof ts !== "string" || !ts.trim()) return null;
  const s = ts.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return `${s}T00:00:00Z`;
  const d = new Date(s);
  return Number.isFinite(d.getTime()) ? d.toISOString() : null;
}

function deriveFiscalPeriod(dateStr: unknown): string | null {
  if (typeof dateStr !== "string" || !dateStr.trim()) return null;
  const d = new Date(dateStr);
  if (!Number.isFinite(d.getTime())) return null;
  const q = Math.floor(d.getUTCMonth() / 3) + 1;
  return `FY${d.getUTCFullYear()} Q${q}`;
}

function isPaywalledUrl(url: string): boolean {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return new Set([
      "seekingalpha.com",
      "www.seekingalpha.com",
      "wsj.com",
      "www.wsj.com",
      "bloomberg.com",
      "www.bloomberg.com",
    ]).has(host);
  } catch {
    return false;
  }
}

function classifyEarningsSourceUrl(url: string): { sourceType: "sec_8k" | "company_ir" | null; error: string | null } {
  if (!url || !url.trim()) return { sourceType: null, error: "source_url must be a non-empty string" };
  let u: URL;
  try {
    u = new URL(url.trim());
  } catch {
    return { sourceType: null, error: "source_url must be a valid URL" };
  }
  if (u.protocol !== "https:") return { sourceType: null, error: "source_url must use https" };
  if (isPaywalledUrl(u.toString())) return { sourceType: null, error: "source_url appears paywalled and is not allowed" };
  if (u.toString().startsWith("https://www.sec.gov/Archives/")) return { sourceType: "sec_8k", error: null };
  return { sourceType: "company_ir", error: null };
}

async function fetchPublicHtml(url: string, maxBytes = 3_000_000): Promise<string | null> {
  try {
    const resp = await fetch(url, { headers: { "User-Agent": UA } });
    if (!resp.ok) return null;
    const reader = resp.body?.getReader();
    if (!reader) return await resp.text();
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      chunks.push(value);
      total += value.byteLength;
      if (total >= maxBytes) { await reader.cancel(); break; }
    }
    const merged = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) { merged.set(c, off); off += c.byteLength; }
    return new TextDecoder().decode(merged);
  } catch {
    return null;
  }
}

function scaleNumberFromText(raw: unknown): number | null {
  if (typeof raw !== "string" && typeof raw !== "number") return null;
  const s = String(raw).trim().replace(/,/g, "");
  const m = s.match(/[-+]?\d+(?:\.\d+)?/);
  if (!m) return null;
  let n = Number(m[0]);
  if (!Number.isFinite(n)) return null;
  const low = s.toLowerCase();
  if (low.includes("billion") || /\bbn\b/.test(low) || /b$/.test(low)) n *= 1_000_000_000;
  else if (low.includes("million") || /m$/.test(low)) n *= 1_000_000;
  else if (low.includes("thousand") || /k$/.test(low)) n *= 1_000;
  return n;
}

function extractMetricNumber(
  text: string,
  patterns: RegExp[],
): { value: number | null; rawValue: string | null; excerpt: string | null } {
  for (const re of patterns) {
    const m = text.match(re);
    if (!m) continue;
    const rawValue = m[1] ?? null;
    const value = scaleNumberFromText(rawValue);
    if (value != null) {
      return { value, rawValue, excerpt: compactExcerpt(m[0] ?? "", 220) };
    }
  }
  return { value: null, rawValue: null, excerpt: null };
}

function sentenceForTopic(text: string, topic: string): string | null {
  const topicLower = topic.toLowerCase();
  for (const s of text.split(/(?<=[.!?])\s+/)) {
    if (s.toLowerCase().includes(topicLower)) return compactExcerpt(s, 220);
  }
  return null;
}

/** Resolve the EX-99.1 exhibit URL from an 8-K filing index page. */
async function resolveEx991Url(cikInt: number, accessionNumber: string): Promise<string | null> {
  const { edgarIndexUrl: indexUrl } = edgarBuildFilingUrls(cikInt, accessionNumber, null);
  const exhibits = await edgarListExhibitsFromIndex(indexUrl);
  const ex991 = exhibits.find((exhibit) => {
    const type = String(exhibit.type ?? "").toUpperCase().replace(/\s+/g, "");
    const description = String(exhibit.description ?? "").toUpperCase();
    const document = String(exhibit.document ?? "").toLowerCase();
    if (/^EX-99\.0?1\b/.test(type)) return true;
    if (type.startsWith("EX-99") && /PRESS RELEASE|EARNINGS RELEASE|RESULTS RELEASE/.test(description)) return true;
    return /(?:^|[-_])ex(?:hibit)?[-_]?99[-_.]?0?1\b/.test(document);
  });
  if (ex991?.document) {
    const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(cikInt, accessionNumber, String(ex991.document));
    if (edgarPrimaryDocumentUrl) return edgarPrimaryDocumentUrl;
  }

  const html = await edgarGetHtml(indexUrl, 100_000);
  if (!html) return null;
  // Match an EX-99.1 row in the filing index and capture its document link
  const re = /EX-99\.1[^<]{0,300}<[^>]+href="(\/Archives\/edgar\/data\/[^"]+\.(?:htm|html))"/i;
  const m = re.exec(html);
  if (m) return `https://www.sec.gov${m[1]}`;
  return null;
}

/** Parse inline XBRL (iXBRL) numeric tags from an HTML document.
 *  Returns a Map of lowercased concept name → scaled numeric value. */
function parseIxbrlConceptValues(html: string): Map<string, number> {
  const result = new Map<string, number>();
  // Match ix:nonFraction and ix:nonfraction (case-insensitive tag name)
  const re = /<ix:non[Ff]raction\b([^>]*)>([\s\S]*?)<\/ix:non[Ff]raction>/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(html)) !== null) {
    const attrStr = m[1];
    const rawText = m[2].replace(/,/g, "").replace(/\s/g, "").trim();
    if (!rawText) continue;
    const nameMx = /\bname="([^"]+)"/i.exec(attrStr);
    if (!nameMx) continue;
    const conceptKey = nameMx[1].toLowerCase();
    let num = parseFloat(rawText);
    if (!Number.isFinite(num)) continue;
    const scaleMx = /\bscale="(-?\d+)"/i.exec(attrStr);
    if (scaleMx) num *= Math.pow(10, parseInt(scaleMx[1], 10));
    if (/\bsign="-"/i.test(attrStr)) num = -num;
    // Keep first occurrence per concept (consolidated figure appears first)
    if (!result.has(conceptKey)) result.set(conceptKey, num);
  }
  return result;
}

async function resolveLatestEarningsSecSource(ticker: string): Promise<Record<string, unknown> | null> {
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) return null;

  const cikInt = parseInt(cikPadded, 10);
  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form as string[]) ?? [];
  const dates = (recent.filingDate as string[]) ?? [];
  const accessions = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs = (recent.primaryDocument as string[]) ?? [];
  const acceptedDts = (recent.acceptanceDateTime as string[]) ?? [];
  const items = (recent.items as string[]) ?? [];
  const isInlineXBRL = (recent.isInlineXBRL as (number | boolean)[]) ?? [];

  // Two passes: prefer item-2.02 (earnings) 8-Ks; fall back to any 8-K
  for (const requireItem202 of [true, false]) {
    let checked = 0;
    for (let i = 0; i < forms.length && checked < 20; i++) {
      if ((forms[i] ?? "").toUpperCase() !== "8-K") continue;
      checked++;
      const itemStr = String(items[i] ?? "");
      if (requireItem202 && !itemStr.includes("2.02")) continue;
      const acc = accessions[i] ?? "";
      const primaryDoc = primaryDocs[i] ?? "";
      if (!acc || !primaryDoc) continue;
      const accClean = acc.replace(/-/g, "");
      const documentUrl = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${primaryDoc}`;
      return {
        sourceType: "sec_8k",
        url: documentUrl,
        filingDate: dates[i] ?? null,
        acceptedAt: acceptedDts[i] ?? null,
        accessionNumber: acc,
        cikPadded,
        items: itemStr,
        isInlineXBRL: !!(isInlineXBRL[i]),
        confidence: "HIGH",
      };
    }
  }
  return null;
}

async function resolveLatestEarningsRelease(ticker: string): Promise<Record<string, unknown>> {
  const sec = await resolveLatestEarningsSecSource(ticker);
  if (sec) {
    const reportingTs = toIsoUtc((sec.acceptedAt as string | null) ?? null) ?? toIsoUtc((sec.filingDate as string | null) ?? null);
    return {
      ticker: ticker.toUpperCase(),
      eventType: "earnings_release",
      period: deriveFiscalPeriod(sec.filingDate) ?? "latest",
      reportedAt: reportingTs,
      sources: [sec],
      confidence: "HIGH",
      warnings: [],
    };
  }

  const calendar = parseObjectJson(await getCalendar(ticker));
  const earningsDates = ((((calendar.calendar as Record<string, unknown> | undefined)?.earnings as Record<string, unknown> | undefined)?.earningsDate as unknown[]) ?? [])
    .filter((d) => typeof d === "string") as string[];
  const first = earningsDates[0] ?? null;
  if (first) {
    return {
      ticker: ticker.toUpperCase(),
      eventType: "earnings_release",
      period: deriveFiscalPeriod(first) ?? "latest",
      reportedAt: toIsoUtc(first),
      sources: [
        {
          sourceType: "yahoo_estimate",
          url: `https://finance.yahoo.com/quote/${ticker.toUpperCase()}/analysis`,
          publishedAt: toIsoUtc(first),
          retrievedAt: nowIsoUtc(),
          confidence: "MEDIUM",
        },
      ],
      confidence: "MEDIUM",
      warnings: [{ code: "SEC_8K_NOT_FOUND", message: "SEC 8-K earnings release source not found" }],
    };
  }

  return {
    ticker: ticker.toUpperCase(),
    eventType: "earnings_release",
    period: "latest",
    reportedAt: null,
    sources: [],
    confidence: "NOT_FOUND",
    warnings: [],
  };
}

export async function getLatestEarningsRelease(ticker: string, _period = "latest"): Promise<string> {
  return JSON.stringify(await resolveLatestEarningsRelease(ticker));
}

export async function indexEarningsRelease(ticker: string, period = "latest", sourceUrl: string | null = null): Promise<string> {
  let sourceType: string | null = null;
  let sourceMeta: Record<string, unknown> = {};
  if (sourceUrl) {
    const classified = classifyEarningsSourceUrl(sourceUrl);
    if (classified.error) return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: classified.error } });
    sourceType = classified.sourceType;
    sourceMeta = { sourceType, url: sourceUrl };
  } else {
    const latest = await resolveLatestEarningsRelease(ticker);
    const sources = Array.isArray(latest.sources) ? latest.sources : [];
    const src = (sources[0] && typeof sources[0] === "object") ? sources[0] as Record<string, unknown> : {};
    sourceType = String(src.sourceType ?? "");
    sourceUrl = String(src.url ?? "");
    sourceMeta = src;
  }

  if (!sourceUrl) {
    return JSON.stringify({
      ticker: ticker.toUpperCase(),
      period,
      source: { sourceType: sourceType ?? "unknown", url: null },
      index: { sections: [], tables: [], keywordMap: {} },
      meta: { indexedAt: nowIsoUtc(), cacheKey: `earnidx:${ticker.toUpperCase()}:none`, cacheTtlHours: 24 },
      warnings: [{ code: "SOURCE_NOT_FOUND", message: "No public earnings release source found" }],
    });
  }

  const cacheId = String(sourceMeta.accessionNumber ?? sourceUrl);
  const cacheKey = `earnidx:${ticker.toUpperCase()}:${cacheId}`;
  const cached = filingIndexCache.get(cacheKey);
  if (cached && Date.now() - cached.storedAt < FILING_INDEX_TTL_MS) return cached.value;

  const html = sourceUrl.startsWith("https://www.sec.gov/Archives/")
    ? await edgarGetHtml(sourceUrl, 5_000_000)
    : await fetchPublicHtml(sourceUrl);
  if (!html) return JSON.stringify({ ok: false, error: { code: "PROVIDER_ERROR", message: `Failed to fetch source: ${sourceUrl}` } });

  const index = _buildFilingIndexFromHtml(_sanitizeFilingHtml(html));
  const out = {
    ticker: ticker.toUpperCase(),
    period: deriveFiscalPeriod(sourceMeta.filingDate ?? sourceMeta.publishedAt) ?? period,
    source: {
      sourceType: sourceType ?? sourceMeta.sourceType ?? "company_ir",
      url: sourceUrl,
      publishedAt: sourceMeta.publishedAt ?? null,
      retrievedAt: nowIsoUtc(),
      filingDate: sourceMeta.filingDate ?? null,
      acceptedAt: sourceMeta.acceptedAt ?? null,
      accessionNumber: sourceMeta.accessionNumber ?? null,
    },
    index,
    meta: { indexedAt: nowIsoUtc(), cacheKey, cacheTtlHours: 24 },
  };
  const encoded = JSON.stringify(out);
  filingIndexCache.set(cacheKey, { value: encoded, storedAt: Date.now() });
  return encoded;
}

export async function extractEarningsMetrics(
  ticker: string,
  period = "latest",
  _sourcePreference: string[] = ["sec_8k", "company_ir", "10-q", "yahoo"],
): Promise<string> {
  const release = await resolveLatestEarningsRelease(ticker);
  const defaultMetric = (unit: string): Record<string, unknown> => ({ value: null, unit, confidence: "NOT_DISCLOSED", evidence: null });
  const metrics: Record<string, unknown> = {
    revenue: defaultMetric("USD"),
    epsDiluted: defaultMetric("USD/share"),
    grossMargin: { valueRatio: null, valuePct: null, rawValue: null, confidence: "NOT_DISCLOSED", evidence: null },
    operatingIncome: defaultMetric("USD"),
    freeCashFlow: defaultMetric("USD"),
    capex: defaultMetric("USD"),
  };
  const evidence: Record<string, unknown>[] = [];
  const warnings: Record<string, unknown>[] = [];
  const src = Array.isArray(release.sources) && release.sources[0] && typeof release.sources[0] === "object"
    ? release.sources[0] as Record<string, unknown>
    : {};
  const srcUrl = String(src.url ?? "");
  const srcType = String(src.sourceType ?? "yahoo");
  const srcAccession = String(src.accessionNumber ?? "");
  const srcCikPadded = String(src.cikPadded ?? "");
  const publishedAt = toIsoUtc((src.filingDate as string | null) ?? (src.publishedAt as string | null) ?? null);
  const retrievedAt = nowIsoUtc();

  const hasHighConfidence = (): boolean =>
    Object.values(metrics).some((m) => (m as Record<string, unknown>).confidence === "HIGH");

  // ── Tier 1: XBRL CompanyConcept API (confirmed 10-Q data) ────────────────
  // Fetches the most recent 10-Q fact for each concept. Covers periods where
  // the 10-Q is already filed (~40 days after earnings).
  const cikPadded = srcCikPadded || (await resolveCikForTicker(ticker)) || "";
  if (cikPadded) {
    const fetchQuarterlyFact = async (
      primary: string,
      fallback?: string,
      unitType: "USD" | "USD/shares" = "USD",
    ): Promise<{ value: number; end: string; concept: string } | null> => {
      for (const concept of [primary, ...(fallback ? [fallback] : [])]) {
        const d = await edgarGetJson(
          `https://data.sec.gov/api/xbrl/companyconcept/CIK${cikPadded}/us-gaap/${concept}.json`,
        );
        const facts =
          (((d?.units as Record<string, unknown>) ?? {})[unitType] as Record<string, unknown>[] | undefined) ?? [];
        // Exclude segment-level (non-consolidated) facts; take most recent 10-Q
        const quarterly = facts
          .filter((f) => String(f.form ?? "").toUpperCase() === "10-Q" && !f.segment)
          .sort((a, b) => String(b.end ?? "").localeCompare(String(a.end ?? "")));
        const latest = quarterly[0];
        if (latest?.val != null && typeof latest.val === "number") {
          return { value: latest.val, end: String(latest.end ?? ""), concept };
        }
      }
      return null;
    };

    const [xRev, xEps, xGp, xOi, xCapex, xOcf] = await Promise.all([
      fetchQuarterlyFact("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"),
      fetchQuarterlyFact("EarningsPerShareDiluted", undefined, "USD/shares"),
      fetchQuarterlyFact("GrossProfit"),
      fetchQuarterlyFact("OperatingIncomeLoss"),
      fetchQuarterlyFact("PaymentsToAcquirePropertyPlantAndEquipment", "CapitalExpenditures"),
      fetchQuarterlyFact("NetCashProvidedByUsedInOperatingActivities"),
    ]);

    // Only apply Tier 1 if the most recent period is within 4 months (10-Q lag window)
    const XBRL_MAX_AGE_MS = 120 * 24 * 60 * 60 * 1000;
    const xbrlPeriodEnd = xRev?.end ?? xEps?.end ?? null;
    const xbrlIsRecent = xbrlPeriodEnd
      ? Date.now() - new Date(xbrlPeriodEnd).getTime() < XBRL_MAX_AGE_MS
      : false;

    if (xbrlIsRecent) {
      const xbrlEv = (concept: string, end: string): Record<string, unknown> => ({
        url: `https://data.sec.gov/api/xbrl/companyconcept/CIK${cikPadded}/us-gaap/${concept}.json`,
        sourceType: "sec_xbrl_10q",
        publishedAt: null,
        retrievedAt,
        excerpt: `XBRL ${concept} period ending ${end}`,
      });
      const setM = (key: string, val: Record<string, unknown>): void => {
        metrics[key] = val;
        if (val.evidence) evidence.push(val.evidence as Record<string, unknown>);
      };
      if (xRev) setM("revenue", { value: xRev.value, unit: "USD", rawValue: null, confidence: "HIGH", evidence: xbrlEv(xRev.concept, xRev.end) });
      if (xEps) setM("epsDiluted", { value: xEps.value, unit: "USD/share", rawValue: null, confidence: "HIGH", evidence: xbrlEv(xEps.concept, xEps.end) });
      if (xGp && xRev && xRev.value !== 0) {
        const pct = Number(((xGp.value / xRev.value) * 100).toFixed(2));
        setM("grossMargin", { valueRatio: Number((xGp.value / xRev.value).toFixed(6)), valuePct: pct, rawValue: null, confidence: "HIGH", evidence: xbrlEv(xGp.concept, xGp.end) });
      }
      if (xOi) setM("operatingIncome", { value: xOi.value, unit: "USD", rawValue: null, confidence: "HIGH", evidence: xbrlEv(xOi.concept, xOi.end) });
      if (xCapex) setM("capex", { value: Math.abs(xCapex.value), unit: "USD", rawValue: null, confidence: "HIGH", evidence: xbrlEv(xCapex.concept, xCapex.end) });
      if (xOcf && xCapex) setM("freeCashFlow", { value: xOcf.value - Math.abs(xCapex.value), unit: "USD", rawValue: null, confidence: "HIGH", evidence: xbrlEv(xOcf.concept, xOcf.end) });
    }
  }

  // ── Tier 2 + 3: SEC 8-K press release (iXBRL → regex fallback) ───────────
  if (!hasHighConfidence() && srcUrl.startsWith("https://www.sec.gov/Archives/")) {
    const cikInt = parseInt(cikPadded || "0", 10);

    // Resolve EX-99.1 exhibit URL — the actual earnings press release document.
    // Modern 8-Ks put financial details in EX-99.1, not in the primary 8-K wrapper.
    let contentUrl = srcUrl;
    if (srcAccession && cikInt) {
      const ex991 = await resolveEx991Url(cikInt, srcAccession);
      if (ex991) contentUrl = ex991;
    }
    const contentSourceType = contentUrl !== srcUrl ? "sec_8k_ex991" : "sec_8k";

    const html = await edgarGetHtml(contentUrl, 5_000_000);
    if (html) {
      const contentEv = (excerpt: string | null): Record<string, unknown> | null =>
        excerpt ? { url: contentUrl, sourceType: contentSourceType, publishedAt, retrievedAt, excerpt } : null;

      // Tier 2: iXBRL — structured data embedded in modern press releases
      const conceptValues = parseIxbrlConceptValues(html);
      if (conceptValues.size > 0) {
        const getConcept = (...names: string[]): number | undefined => {
          for (const n of names) {
            const v = conceptValues.get(n);
            if (v != null) return v;
          }
          return undefined;
        };
        const ixEv = (label: string): Record<string, unknown> => ({
          url: contentUrl,
          sourceType: "sec_8k_ixbrl",
          publishedAt,
          retrievedAt,
          excerpt: `iXBRL: ${label}`,
        });
        const setIx = (key: string, val: Record<string, unknown>): void => {
          metrics[key] = val;
          evidence.push(val.evidence as Record<string, unknown>);
        };

        const ixRev = getConcept(
          "us-gaap:revenuesfromcontractwithcustomerexcludingassessedtax",
          "us-gaap:revenues",
        );
        const ixEps = getConcept("us-gaap:earningspersharediluted");
        const ixGp = getConcept("us-gaap:grossprofit");
        const ixOi = getConcept("us-gaap:operatingincomeloss");
        const ixCapex = getConcept(
          "us-gaap:paymentstoacquirepropertyplantandequipment",
          "us-gaap:capitalexpenditures",
        );
        const ixOcf = getConcept("us-gaap:netcashprovidedbyusedinoperatingactivities");

        if (ixRev != null) setIx("revenue", { value: ixRev, unit: "USD", rawValue: null, confidence: "HIGH", evidence: ixEv("Revenues") });
        if (ixEps != null) setIx("epsDiluted", { value: ixEps, unit: "USD/share", rawValue: null, confidence: "HIGH", evidence: ixEv("EarningsPerShareDiluted") });
        const gmRev = ixRev ?? (typeof (metrics.revenue as Record<string, unknown>).value === "number"
          ? (metrics.revenue as Record<string, unknown>).value as number : null);
        if (ixGp != null && gmRev != null && gmRev !== 0) {
          const pct = Number(((ixGp / gmRev) * 100).toFixed(2));
          setIx("grossMargin", { valueRatio: Number((ixGp / gmRev).toFixed(6)), valuePct: pct, rawValue: null, confidence: "HIGH", evidence: ixEv("GrossProfit") });
        }
        if (ixOi != null) setIx("operatingIncome", { value: ixOi, unit: "USD", rawValue: null, confidence: "HIGH", evidence: ixEv("OperatingIncomeLoss") });
        if (ixCapex != null) setIx("capex", { value: Math.abs(ixCapex), unit: "USD", rawValue: null, confidence: "HIGH", evidence: ixEv("CapEx") });
        if (ixOcf != null && ixCapex != null) setIx("freeCashFlow", { value: ixOcf - Math.abs(ixCapex), unit: "USD", rawValue: null, confidence: "HIGH", evidence: ixEv("FreeCashFlow") });
      }

      // Tier 3 (regex fallback): plain-text extraction for non-iXBRL press releases
      if (!hasHighConfidence()) {
        const text = _stripHtmlTagsIdx(_sanitizeFilingHtml(html));
        const rev = extractMetricNumber(text, [/(?:net sales|revenue(?:s)?)\D{0,20}\$?\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)/i]);
        const eps = extractMetricNumber(text, [/(?:diluted (?:earnings per share|eps)|eps \(diluted\))\D{0,20}\$?\s*([0-9]+(?:\.[0-9]+)?)/i]);
        const gm = extractMetricNumber(text, [/gross margin\D{0,15}([0-9]{1,2}(?:\.[0-9]+)?)\s*%/i]);
        const op = extractMetricNumber(text, [/operating income\D{0,20}\$?\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)/i]);
        const fcf = extractMetricNumber(text, [/free cash flow\D{0,20}\$?\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)/i]);
        const capex = extractMetricNumber(text, [/(?:capital expenditures|capex)\D{0,20}\$?\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)/i]);
        const setFallbackMetric = (key: string, val: Record<string, unknown>): void => {
          metrics[key] = val;
          const ev = val.evidence;
          if (ev && typeof ev === "object") evidence.push(ev as Record<string, unknown>);
        };

        if (rev.value != null) setFallbackMetric("revenue", { value: rev.value, unit: "USD", rawValue: rev.rawValue, confidence: "HIGH", evidence: contentEv(rev.excerpt) });
        if (eps.value != null) setFallbackMetric("epsDiluted", { value: eps.value, unit: "USD/share", rawValue: eps.rawValue ? `$${eps.rawValue}` : null, confidence: "HIGH", evidence: contentEv(eps.excerpt) });
        if (gm.value != null) {
          const pct = Number(gm.value);
          setFallbackMetric("grossMargin", { valueRatio: Number((pct / 100).toFixed(6)), valuePct: pct, rawValue: gm.rawValue, confidence: "HIGH", evidence: contentEv(gm.excerpt) });
        }
        if (op.value != null) setFallbackMetric("operatingIncome", { value: op.value, unit: "USD", rawValue: op.rawValue, confidence: "HIGH", evidence: contentEv(op.excerpt) });
        if (fcf.value != null) setFallbackMetric("freeCashFlow", { value: fcf.value, unit: "USD", rawValue: fcf.rawValue, confidence: "HIGH", evidence: contentEv(fcf.excerpt) });
        if (capex.value != null) setFallbackMetric("capex", { value: capex.value, unit: "USD", rawValue: capex.rawValue, confidence: "HIGH", evidence: contentEv(capex.excerpt) });
      }
    }
  } else if (!hasHighConfidence() && !srcUrl.startsWith("https://www.sec.gov/Archives/")) {
    warnings.push({ code: "PUBLIC_RELEASE_NOT_FOUND", message: "No SEC 8-K earnings release source available" });
  }

  let confidence = "NOT_DISCLOSED";
  for (const key of ["revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex"]) {
    const conf = String((metrics[key] as Record<string, unknown>).confidence ?? "");
    if (conf === "HIGH") { confidence = "HIGH"; break; }
  }
  if (confidence !== "HIGH" && (release.confidence === "MEDIUM" || release.confidence === "LOW")) confidence = String(release.confidence);

  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    eventType: "earnings_release",
    period: release.period ?? period,
    reportedAt: release.reportedAt ?? null,
    source: srcType || "yahoo",
    metrics,
    evidence,
    confidence,
    warnings,
  });
}

export async function extractGuidance(ticker: string, period = "latest"): Promise<string> {
  const release = await resolveLatestEarningsRelease(ticker);
  const guidance: Record<string, unknown> = {
    revenue: { status: "NOT_DISCLOSED", low: null, high: null, midpoint: null, unit: "USD", evidence: [] },
    grossMargin: { status: "NOT_DISCLOSED", lowPct: null, highPct: null, midpointPct: null, evidence: [] },
    eps: { status: "NOT_DISCLOSED", low: null, high: null, midpoint: null, unit: "USD/share", evidence: [] },
  };
  const src = Array.isArray(release.sources) && release.sources[0] && typeof release.sources[0] === "object"
    ? release.sources[0] as Record<string, unknown>
    : {};
  const srcUrl = String(src.url ?? "");
  if (!srcUrl.startsWith("https://www.sec.gov/Archives/")) {
    return JSON.stringify({ ticker: ticker.toUpperCase(), period: release.period ?? period, guidance, confidence: "NOT_DISCLOSED", warnings: [] });
  }
  const html = await edgarGetHtml(srcUrl, 5_000_000);
  const text = _stripHtmlTagsIdx(_sanitizeFilingHtml(html ?? ""));
  const rev = text.match(/(?:expects|guidance|outlook)[^.\n]{0,120}revenue[^$]{0,25}\$?\s*([0-9.,]+(?:\s*(?:billion|million|thousand|bn|m|k))?)\s*(?:to|-)\s*\$?\s*([0-9.,]+(?:\s*(?:billion|million|thousand|bn|m|k))?)/i);
  const gm = text.match(/gross margin[^0-9]{0,20}([0-9]{1,2}(?:\.[0-9]+)?)\s*%\s*(?:to|-)\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%/i);
  const eps = text.match(/(?:expects|guidance|outlook)[^.\n]{0,120}(?:eps|earnings per share)[^$]{0,25}\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|-)\s*\$?\s*([0-9]+(?:\.[0-9]+)?)/i);

  const ev = (excerpt: string): Record<string, unknown> => ({
    url: srcUrl,
    sourceType: src.sourceType ?? "sec_8k",
    publishedAt: toIsoUtc((src.filingDate as string | null) ?? null),
    retrievedAt: nowIsoUtc(),
    excerpt: compactExcerpt(excerpt),
  });

  if (rev) {
    const low = scaleNumberFromText(rev[1]);
    const high = scaleNumberFromText(rev[2]);
    if (low != null && high != null) {
      guidance.revenue = { status: "FOUND", low, high, midpoint: (low + high) / 2, unit: "USD", evidence: [ev(rev[0])] };
    }
  }
  if (gm) {
    const lowPct = Number(gm[1]);
    const highPct = Number(gm[2]);
    guidance.grossMargin = { status: "FOUND", lowPct, highPct, midpointPct: (lowPct + highPct) / 2, evidence: [ev(gm[0])] };
  }
  if (eps) {
    const low = Number(eps[1]);
    const high = Number(eps[2]);
    guidance.eps = { status: "FOUND", low, high, midpoint: (low + high) / 2, unit: "USD/share", evidence: [ev(eps[0])] };
  }
  const found = ["revenue", "grossMargin", "eps"].some((k) => ((guidance[k] as Record<string, unknown>).status === "FOUND"));
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    period: release.period ?? period,
    guidance,
    confidence: found ? "HIGH" : "NOT_DISCLOSED",
    warnings: [],
  });
}

export async function extractManagementCommentary(
  ticker: string,
  period = "latest",
  topics: string[] = [],
): Promise<string> {
  const release = await resolveLatestEarningsRelease(ticker);
  const src = Array.isArray(release.sources) && release.sources[0] && typeof release.sources[0] === "object"
    ? release.sources[0] as Record<string, unknown>
    : {};
  const srcUrl = String(src.url ?? "");
  let text = "";
  if (srcUrl.startsWith("https://www.sec.gov/Archives/")) {
    text = _stripHtmlTagsIdx(_sanitizeFilingHtml((await edgarGetHtml(srcUrl, 5_000_000)) ?? ""));
  } else if (srcUrl) {
    text = _stripHtmlTagsIdx(_sanitizeFilingHtml((await fetchPublicHtml(srcUrl)) ?? ""));
  }
  const outTopics = (topics ?? []).map((topic) => {
    const excerpt = text ? sentenceForTopic(text, topic) : null;
    if (!excerpt) {
      return { topic, status: "NOT_FOUND", summary: "", evidence: [], confidence: "LOW" };
    }
    return {
      topic,
      status: "FOUND",
      summary: excerpt,
      evidence: [{
        sourceType: src.sourceType ?? "company_ir",
        url: srcUrl,
        publishedAt: toIsoUtc((src.filingDate as string | null) ?? (src.publishedAt as string | null) ?? null),
        retrievedAt: nowIsoUtc(),
        excerpt: excerpt.slice(0, 240),
      }],
      confidence: src.sourceType === "sec_8k" ? "HIGH" : "MEDIUM",
    };
  });
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    period: release.period ?? period,
    topics: outTopics,
    warnings: [],
  });
}

export async function compareEarningsActualVsEstimate(ticker: string, period = "latest"): Promise<string> {
  const metrics = parseObjectJson(await extractEarningsMetrics(ticker, period));
  const ea = parseObjectJson(await getEarningsAnalysis(ticker));
  const m = (metrics.metrics && typeof metrics.metrics === "object") ? metrics.metrics as Record<string, unknown> : {};
  const revenueMetric = (m.revenue && typeof m.revenue === "object") ? m.revenue as Record<string, unknown> : {};
  const epsMetric = (m.epsDiluted && typeof m.epsDiluted === "object") ? m.epsDiluted as Record<string, unknown> : {};
  const actualRevenue = typeof revenueMetric.value === "number" ? revenueMetric.value : null;

  const toNumber = (value: unknown): number | null =>
    typeof value === "number" && Number.isFinite(value) ? value : null;
  const rowPeriod = (row: Record<string, unknown> | null): string | null => {
    if (!row) return null;
    for (const key of ["reportedPeriod", "period", "quarter", "index"]) {
      let value: unknown = row[key];
      if (value == null) continue;
      // Unwrap Yahoo Finance {raw, fmt} nested objects (e.g. quarter: {raw: 1617148800, fmt: "3/31/2021"})
      if (typeof value === "object" && value !== null) {
        const obj = value as Record<string, unknown>;
        value = obj.fmt ?? obj.raw ?? obj.label ?? null;
        if (value == null) continue;
      }
      const str = String(value).trim();
      if (str) return str;
    }
    return null;
  };
  const rowDate = (row: Record<string, unknown> | null): string | null => {
    if (!row) return null;
    for (const key of ["reportedDate", "quarter", "date", "earningsDate", "index"]) {
      let value: unknown = row[key];
      if (value == null) continue;
      // Unwrap Yahoo Finance {raw, fmt} nested objects; prefer raw (often a Unix timestamp number)
      if (typeof value === "object" && value !== null) {
        const obj = value as Record<string, unknown>;
        value = obj.raw ?? obj.fmt ?? null;
        if (value == null) continue;
      }
      if (typeof value === "number" && Number.isFinite(value)) {
        const ts = value > 1_000_000_000_000 ? value : value * 1000;
        return new Date(ts).toISOString().slice(0, 10);
      }
      const parsed = new Date(String(value));
      if (!Number.isNaN(parsed.getTime())) return parsed.toISOString().slice(0, 10);
      const text = String(value).trim();
      if (text) return text;
    }
    return null;
  };

  const earningsHistory = Array.isArray(ea.earningsHistory) ? ea.earningsHistory as Record<string, unknown>[] : [];
  const reportedRows = earningsHistory.filter((row) => toNumber(row.epsActual) != null);
  const selected = reportedRows.sort((a, b) => String(rowDate(b) ?? "").localeCompare(String(rowDate(a) ?? "")))[0] ?? null;
  const reportedPeriod = rowPeriod(selected);
  const reportedDate = rowDate(selected) ?? (metrics.reportedAt as string | null) ?? null;
  const actualEps = toNumber(selected?.epsActual) ?? (typeof epsMetric.value === "number" ? epsMetric.value : null);
  const estEps = toNumber(selected?.epsEstimate);

  let estRevenue: number | null = null;
  const revenueEstimate = Array.isArray(ea.revenueEstimate) ? ea.revenueEstimate as Record<string, unknown>[] : [];
  for (const row of revenueEstimate) {
    const candidatePeriod = String(row.period ?? row.reportedPeriod ?? "");
    if (
      ((reportedPeriod != null && candidatePeriod === reportedPeriod)
        || (reportedDate != null && rowDate(row) === reportedDate))
      && typeof row.avg === "number"
    ) {
      estRevenue = row.avg as number;
      break;
    }
  }

  const out: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    period: reportedPeriod ?? metrics.period ?? period,
    reportedPeriod,
    reportedDate,
    actual: {
      revenue: { value: actualRevenue, unit: "USD" },
      eps: { value: actualEps, unit: "USD/share" },
    },
    estimate: {
      revenue: { value: estRevenue, unit: "USD", source: "yahoo" },
      eps: { value: estEps, unit: "USD/share", source: "yahoo" },
    },
    surprise: {
      revenueSurprisePct: null,
      epsSurprisePct: null,
    },
    confidence: "NOT_DISCLOSED",
    warnings: [] as Record<string, unknown>[],
  };

  if (!selected) {
    (out.warnings as Record<string, unknown>[]).push({
      code: "NO_REPORTED_QUARTER",
      message: "Yahoo earningsHistory did not include a quarter with non-null actual EPS.",
    });
    return JSON.stringify(out);
  }

  if (actualRevenue != null && estRevenue != null && estRevenue !== 0) {
    (out.surprise as Record<string, unknown>).revenueSurprisePct =
      Number((((actualRevenue - estRevenue) / Math.abs(estRevenue)) * 100).toFixed(2));
  } else if (actualRevenue != null) {
    (out.warnings as Record<string, unknown>[]).push({
      code: "REVENUE_ESTIMATE_UNAVAILABLE",
      message: "No Yahoo revenue estimate was available for the selected reported quarter.",
    });
  }

  if (actualEps == null || estEps == null || estEps === 0) {
    (out.warnings as Record<string, unknown>[]).push({
      code: "EPS_ESTIMATE_UNAVAILABLE",
      message: "No Yahoo EPS estimate was available for the selected reported quarter.",
    });
    return JSON.stringify(out);
  }

  out.surprise = {
    revenueSurprisePct: (out.surprise as Record<string, unknown>).revenueSurprisePct ?? null,
    epsSurprisePct: Number((((actualEps - estEps) / Math.abs(estEps)) * 100).toFixed(2)),
  };
  out.confidence = (out.surprise as Record<string, unknown>).revenueSurprisePct != null ? "HIGH" : "MEDIUM";
  return JSON.stringify(out);
}
