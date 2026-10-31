/**
 * Pricing and options: quotes, historical prices, price and volume analysis, position signals, price targets, option
 * expiries and chains, options flow, put hedges, the screener and ticker search (Phase 5 move out of yahoo-finance.ts).
 * Helpers shared with other domains still live in yahoo-finance.ts and are imported back; yahoo-finance.ts re-exports
 * every moved name, so tools.ts and the parity bundle are unchanged.
 */

import { setBounded } from "./cache.js";
import { ErrorCode, classifyErrorMessage, mcpFailure } from "./response.js";
import { tickerCap } from "./tool-catalog.js";
import { meetsMinimum } from "./deprecations.js";
import { yahooQuoteSummaryUrl, yahooChartUrl, yahooOptionsUrl, yahooScreenerUrl, yahooSearchUrl } from "./clients/endpoints.js";
import { raw, yGet, enc, noData, iso, evictYahooGet } from "./clients/yahoo.js";
import { dailyBarDate, roundHalfAwayFromZero, getLastTradingDate, clampInt, addIsoYears } from "./util.js";
import { runPartialBatch, mapWithConcurrency, BATCH_TICKER_CONCURRENCY, safeJsonParse } from "./batch.js";
import { previousExchangeSession } from "./exchange-calendars.js";
import {
  takeNotices,
  withNotices,
  freshnessNotices,
  uniqueNotices,
  insufficientHistoryNotice,
  zeroVolumeNotice,
  SESSION_CALENDAR_UNKNOWN,
} from "./pricing-notices.js";
import {
  HISTORICAL_INTERVALS,
  HISTORICAL_PERIODS,
  INDICATOR_MIN_LOOKBACK,
  POSITION_SINGLE_TICKER_MESSAGE,
  getAnalystConsensus,
  getAnalystUpgradeRadar,
  getEarningsMomentum,
} from "./yahoo-finance.js";

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

export function historicalRangeError(period: string, interval: string): string | null {
  if (!(HISTORICAL_PERIODS as readonly string[]).includes(period)) {
    return `period must be one of: ${HISTORICAL_PERIODS.join(", ")}`;
  }
  if (!(HISTORICAL_INTERVALS as readonly string[]).includes(interval)) {
    return `interval must be one of: ${HISTORICAL_INTERVALS.join(", ")}`;
  }
  return null;
}

export async function getHistoricalPrices(
  ticker: string,
  period: string,
  interval: string,
  prepost: boolean = false
): Promise<string> {
  const url = yahooChartUrl(enc(ticker), `range=${period}&interval=${interval}${prepost ? "&includePrePost=true" : ""}`);
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
  const dailyState = interval === "1d"
    ? prepareCompletedDailySeries(result, Math.floor(Date.now() / 1000))
    : null;
  const completedTimestamps = new Set(
    dailyState?.completedRows.map((row) => row.timestamp) ?? []
  );
  // `date` is the bar's UTC instant; for exchanges east of UTC the session
  // open can fall on the previous UTC day, so the exchange-local calendar
  // date is reported separately.
  const meta = (result.meta as Record<string, unknown> | undefined) ?? {};
  const exchangeTimezone = typeof meta.exchangeTimezoneName === "string" ? meta.exchangeTimezoneName : "UTC";

  return JSON.stringify(
    timestamps.map((t, i) => {
      const hasUsableClose =
        dailyBarNumber(adjclose[i]) != null || dailyBarNumber(quote.close?.[i]) != null;
      const isFinal = interval === "1d"
        ? completedTimestamps.has(t) && hasUsableClose
        : null;
      return {
        date: iso(t),
        tradingDate: dailyBarDate(t, exchangeTimezone),
        open: quote.open?.[i] ?? null,
        high: quote.high?.[i] ?? null,
        low: quote.low?.[i] ?? null,
        close: quote.close?.[i] ?? null,
        volume: quote.volume?.[i] ?? null,
        adjClose: adjclose[i] ?? null,
        ...(interval === "1d"
          ? {
            barStatus: isFinal ? "COMPLETE" : "INCOMPLETE",
            isFinal,
          }
          : {}),
      };
    })
  );
}

export async function getOptionExpirationDates(ticker: string): Promise<string> {
  // auth=true (default): the v7 options endpoint now requires a crumb;
  // passing false caused 401 → uncaught throw → "Error occurred during tool execution".
  try {
    const url = yahooOptionsUrl(enc(ticker));
    const read = (d: unknown): { result: Record<string, unknown> | undefined; dates: string[] } => {
      const result = ((d as Record<string, unknown> | undefined)?.optionChain as Record<string, unknown[]> | undefined)
        ?.result?.[0] as Record<string, unknown> | undefined;
      const dates = ((result?.expirationDates as number[] | undefined) ?? []).map((ts) =>
        new Date(ts * 1000).toISOString().split("T")[0]
      );
      return { result, dates };
    };

    let { result, dates } = read(await yGet(url));
    if (dates.length === 0) {
      // Yahoo intermittently answers with no result or no expirations (seen
      // right after deploys, on fresh isolates). Keep the 30s body cache from
      // replaying that answer and retry once, uncached.
      evictYahooGet(url);
      ({ result, dates } = read(await yGet(`${url}?_=${Date.now()}`)));
    }
    if (!result) return noData(ticker);
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
      `${yahooOptionsUrl(enc(ticker))}?date=${ts}`
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
  let url = yahooOptionsUrl(enc(ticker));
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

export async function getFastInfo(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getFastInfo(t), tickerCap("get_market_quote"));
  }
  const d = (await yGet(
    yahooQuoteSummaryUrl(enc(ticker), "price,summaryDetail,defaultKeyStatistics")
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
  const marketState = typeof price.marketState === "string" ? price.marketState : null;

  return JSON.stringify({
    currency: raw(price.currency),
    exchange: raw(price.exchangeName),
    quoteType,
    lastPrice: raw(price.regularMarketPrice),
    priceBasis: "REGULAR_MARKET_PRICE",
    observationType: "REGULAR_MARKET_QUOTE",
    priceTimestamp: regMktTime != null ? new Date(regMktTime * 1000).toISOString() : null,
    marketState,
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
    marketOpen: marketState === "REGULAR",
    lastTradeDate: regMktTime != null ? new Date(regMktTime * 1000).toISOString().slice(0, 10) : null,
    postMarketTimestamp: postMktTime != null ? new Date(postMktTime * 1000).toISOString() : null,
    ...(isIndex ? { _note: "Index ticker — volume, shares, and marketCap are not applicable and are returned as null" } : {}),
  });
}

export async function getPriceStats(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getPriceStats(t), tickerCap("analyze_price_performance"));
  }
  const [metaRaw, initialHistRaw] = await Promise.all([
    yGet(
      yahooQuoteSummaryUrl(enc(ticker), "price,summaryDetail")
    ),
    yGet(
      yahooChartUrl(enc(ticker), "range=5y&interval=1d"),
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
  const regularMarketTime = raw(price.regularMarketTime) as number | null;

  const pct = (v: number | null, ref: number | null): number | null =>
    v != null && ref != null && ref !== 0 ? +((v - ref) / ref * 100).toFixed(4) : null;

  const stats: Record<string, unknown> = {
    ticker,
    status: "OK",
    currency: raw(price.currency),
    lastPrice,
    priceBasis: "REGULAR_MARKET_PRICE",
    priceObservationType: "REGULAR_MARKET_QUOTE",
    priceTimestamp: regularMarketTime != null ? iso(regularMarketTime) : null,
    marketState: raw(price.marketState),
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

  let histRaw = initialHistRaw as Record<string, unknown>;
  let chartResult = (histRaw?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
    | Record<string, unknown>
    | undefined;
  let retryAttempted = false;
  let prepared = chartResult
    ? prepareCompletedDailySeries(chartResult, Math.floor(Date.now() / 1000))
    : null;
  if (
    chartResult
    && prepared
    && (
      prepared.freshnessStatus === "STALE"
      || completedAdjustedWindowIncomplete(prepared.completedRows)
    )
  ) {
    retryAttempted = true;
    try {
      histRaw = await yGet(
        yahooChartUrl(enc(ticker), `range=5y&interval=1d&events=div%2Csplits&_=${Date.now()}`),
        false
      ) as Record<string, unknown>;
      const retryResult = (histRaw?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
        | Record<string, unknown>
        | undefined;
      if (retryResult) {
        chartResult = retryResult;
        const retried = prepareCompletedDailySeries(
          chartResult,
          Math.floor(Date.now() / 1000)
        );
        retried.excludedIncompleteBar =
          prepared.excludedIncompleteBar || retried.excludedIncompleteBar;
        prepared = retried;
      }
    } catch {
      // Keep quote-derived fields and report historical coverage below.
    }
  }

  const chartMeta = (chartResult?.meta as Record<string, unknown> | undefined) ?? {};
  const timezone = typeof chartMeta.exchangeTimezoneName === "string"
    ? chartMeta.exchangeTimezoneName
    : "UTC";
  const historicalDataDate = prepared && prepared.completedRows.length > 0
    ? dailyBarDate(prepared.completedRows[prepared.completedRows.length - 1].timestamp, timezone)
    : null;

  stats.historicalObservationType = "COMPLETED_DAILY_PRICE_SERIES";
  stats.historicalBarStatus = !prepared
    ? "UNAVAILABLE"
    : prepared.freshnessStatus === "STALE"
      ? "STALE"
      : "COMPLETE";
  stats.freshnessStatus = prepared?.freshnessStatus ?? "UNKNOWN";
  stats.expectedCompletedDate = prepared?.expectedCompletedDate ?? null;
  stats.latestAvailableBarDate = historicalDataDate;
  stats.excludedIncompleteBar = prepared?.excludedIncompleteBar ?? false;
  stats.retryAttempted = retryAttempted;
  stats.dataDate = historicalDataDate;

  if (!prepared || prepared.freshnessStatus === "STALE") {
    stats.status = "PARTIAL";
    stats.historicalPriceBasis = null;
    stats.fallbackUsed = false;
    stats.annualizedVolatility30d = null;
    stats.cagr1y = null;
    stats.cagr3y = null;
    stats.cagr5y = null;
    stats.recommendedNextAction = "RETRY";
    return JSON.stringify(withNotices(stats, freshnessNotices(ticker, prepared?.freshnessReason)));
  }

  const selected = selectCompletedPriceSeries(prepared.completedRows);
  stats.historicalPriceBasis = selected?.priceBasis ?? null;
  stats.fallbackUsed = selected?.fallbackUsed ?? false;
  stats.recommendedNextAction = selected ? "NONE" : "RETRY";
  if (!selected) {
    stats.status = "PARTIAL";
    stats.historicalBarStatus = "INCOMPLETE";
    stats.annualizedVolatility30d = null;
    stats.cagr1y = null;
    stats.cagr3y = null;
    stats.cagr5y = null;
    return JSON.stringify(withNotices(stats, freshnessNotices(ticker, prepared.freshnessReason)));
  }

  const notices = freshnessNotices(ticker, prepared.freshnessReason);
  try {
    const observations = selected.observations;
    const closes = observations.map((row) => row.close);
    const volatility = annualizedVolatility(closes, 30);
    if (volatility != null) stats.annualizedVolatility30d = +volatility.toFixed(4);

    // The horizon starts at the first bar on or after the same exchange-local date N years before the last completed
    // bar (29 February -> 28 February), in both runtimes (R5-pricing-1, BR-PRC-021); it was N x 365.25 days.
    const barDates = observations.map((row) => dailyBarDate(row.timestamp, timezone));
    const cagr = (years: number): number | null => {
      const endObservation = observations[observations.length - 1];
      const endDate = barDates[barDates.length - 1];
      if (endDate == null) return null;
      const cutoffDate = addIsoYears(endDate, -years);
      // A history that starts after the window does not cover it: null with INSUFFICIENT_HISTORY instead of dividing
      // a shorter run by the nominal years (R5-pricing-2, 3.1.0).
      if (!historyCovers(barDates[0] ?? null, cutoffDate)) {
        notices.push(insufficientHistoryNotice(ticker, years));
        return null;
      }
      const startIndex = barDates.findIndex((d) => d != null && d >= cutoffDate);
      const startObservation = startIndex >= 0 ? observations[startIndex] : undefined;
      if (!startObservation || startObservation === endObservation || startObservation.close <= 0) {
        return null;
      }
      return +(
        (Math.pow(endObservation.close / startObservation.close, 1 / years) - 1) * 100
      ).toFixed(4);
    };

    stats.cagr1y = cagr(1);
    stats.cagr3y = cagr(3);
    stats.cagr5y = cagr(5);
  } catch {
    stats.status = "PARTIAL";
    stats.recommendedNextAction = "RETRY";
  }

  return JSON.stringify(withNotices(stats, notices));
}

/**
 * A CAGR window counts as covered when the history's first bar is no more than this many calendar days after the
 * window's start date: the 5y fetch starts about five years before today, not before the last completed bar, and the
 * first bar after that start can follow a weekend and a holiday (R5-pricing-2, 3.1.0).
 */
const HISTORY_START_TOLERANCE_DAYS = 10;

/** Whether a history whose first bar is on firstBarDate reaches back to a window starting on cutoffDate. */
function historyCovers(firstBarDate: string | null, cutoffDate: string): boolean {
  if (firstBarDate == null) return false;
  const limit = new Date(`${cutoffDate}T00:00:00Z`);
  limit.setUTCDate(limit.getUTCDate() + HISTORY_START_TOLERANCE_DAYS);
  return firstBarDate <= limit.toISOString().slice(0, 10);
}

export async function searchTicker(query: string, maxResults: number, exchange?: string | null): Promise<string> {
  const d = (await yGet(
    yahooSearchUrl(`q=${enc(query)}&quotesCount=${maxResults}&newsCount=0&enableFuzzyQuery=false`),
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
      // Every US listing venue, the same set in both runtimes (R5-pricing-1, Q3i call 24): Nasdaq Global Select/Global/Capital Market,
      // NYSE, NYSE American, NYSE Arca.
      const usExchanges = new Set(["NMS", "NGM", "NCM", "NYQ", "ASE", "PCX"]);
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
    yahooScreenerUrl(`scrIds=${enc(screenerName)}&count=${safeCount}&lang=en-US&region=US&corsDomain=finance.yahoo.com`)
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
    yahooQuoteSummaryUrl(enc(ticker), "defaultKeyStatistics,price")
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
  // Observation dates as YYYY-MM-DD, as get_short_momentum reports them.
  for (const key of ["dateShortInterest", "sharesShortPreviousMonthDate"] as const) {
    const value = data[key];
    if (typeof value === "number" && value > 0) data[key] = new Date(value * 1000).toISOString().slice(0, 10);
  }
  data.dataDate = typeof data.dateShortInterest === "string" ? data.dateShortInterest : null;
  data.dataDateBasis = "SHORT_INTEREST_OBSERVATION";
  data.unitSemantics = SHORT_INTEREST_UNITS;

  return JSON.stringify(data);
}

const SHORT_INTEREST_UNITS = {
  decimalRatios: ["shortPercentOfFloat", "sharesPercentSharesOut"],
  days: ["shortRatio"],
  shares: ["sharesShort", "sharesShortPriorMonth", "floatShares", "sharesOutstanding"],
};

const INDICATOR_LONG_PERIODS = new Set(["1y", "2y", "5y", "10y", "max"]);

export function indicatorLookback(period: string | null | undefined): string {
  return period && INDICATOR_LONG_PERIODS.has(period) ? period : INDICATOR_MIN_LOOKBACK;
}

/** Wilder RSI: gains and losses averaged over the first `period` changes, then smoothed by 1/period. */
export function wilderRsi(closes: number[], period = 14): number | null {
  if (closes.length <= period) return null;
  let avgGain = 0;
  let avgLoss = 0;
  for (let i = 1; i <= period; i++) {
    const change = closes[i] - closes[i - 1];
    if (change > 0) avgGain += change;
    else avgLoss -= change;
  }
  avgGain /= period;
  avgLoss /= period;
  for (let i = period + 1; i < closes.length; i++) {
    const change = closes[i] - closes[i - 1];
    avgGain = (avgGain * (period - 1) + (change > 0 ? change : 0)) / period;
    avgLoss = (avgLoss * (period - 1) + (change < 0 ? -change : 0)) / period;
  }
  if (avgLoss === 0) return avgGain === 0 ? 50 : 100;
  return 100 - 100 / (1 + avgGain / avgLoss);
}

function emaSeries(values: number[], span: number): number[] {
  const k = 2 / (span + 1);
  const out = [values[0]];
  for (let i = 1; i < values.length; i++) out.push(values[i] * k + out[i - 1] * (1 - k));
  return out;
}

/** MACD(12, 26, 9) with each EMA seeded by its first value. */
export function macd(closes: number[], fast = 12, slow = 26, signalSpan = 9): { macd: number; signal: number; histogram: number } | null {
  if (closes.length < slow) return null;
  const fastEma = emaSeries(closes, fast);
  const slowEma = emaSeries(closes, slow);
  const line = fastEma.map((v, i) => v - slowEma[i]);
  const signal = emaSeries(line, signalSpan);
  const last = line.length - 1;
  return { macd: line[last], signal: signal[last], histogram: line[last] - signal[last] };
}

/** Annualized volatility in percent: sample standard deviation of the last `window` daily log returns, times sqrt(252). */
export function annualizedVolatility(closes: number[], window = 30): number | null {
  if (closes.length < window + 1) return null;
  const tail = closes.slice(-(window + 1));
  const returns = tail.slice(1).map((c, i) => Math.log(c / tail[i]));
  const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((a, b) => a + (b - mean) ** 2, 0) / (returns.length - 1);
  return Math.sqrt(variance * 252) * 100;
}

export async function getTechnicalIndicators(
  ticker: string | string[],
  period: string
): Promise<string> {
  if (Array.isArray(ticker)) {
    const results = await mapWithConcurrency(ticker, BATCH_TICKER_CONCURRENCY, (t) => getTechnicalIndicators(t, period));
    return JSON.stringify(Object.fromEntries(ticker.map((t, i) => [t, safeJsonParse(results[i], t)])));
  }
  const lookback = indicatorLookback(period);
  const fetchChart = async (retry: boolean): Promise<Record<string, unknown> | undefined> => {
    const cacheBuster = retry ? `&events=div%2Csplits&_=${Date.now()}` : "";
    const d = (await yGet(
      yahooChartUrl(enc(ticker), `range=${lookback}&interval=1d${cacheBuster}`),
      false
    )) as Record<string, unknown>;
    return (d?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
      | Record<string, unknown>
      | undefined;
  };

  let chartResult = await fetchChart(false);
  if (!chartResult) return noData(ticker);
  const nowEpoch = Math.floor(Date.now() / 1000);
  let prepared = prepareCompletedDailySeries(chartResult, nowEpoch);
  let retryAttempted = false;
  if (
    prepared.freshnessStatus === "STALE"
    || completedAdjustedWindowIncomplete(prepared.completedRows)
  ) {
    retryAttempted = true;
    const retryResult = await fetchChart(true);
    if (retryResult) {
      chartResult = retryResult;
      const retried = prepareCompletedDailySeries(chartResult, nowEpoch);
      retried.excludedIncompleteBar =
        prepared.excludedIncompleteBar || retried.excludedIncompleteBar;
      prepared = retried;
    }
  }

  const meta = (chartResult.meta as Record<string, unknown> | undefined) ?? {};
  const timezone = typeof meta.exchangeTimezoneName === "string" ? meta.exchangeTimezoneName : "UTC";
  const latestAvailableBarDate = prepared.completedRows.length > 0
    ? dailyBarDate(prepared.completedRows[prepared.completedRows.length - 1].timestamp, timezone)
    : null;
  if (prepared.freshnessStatus === "STALE") {
    return JSON.stringify({
      ticker,
      status: "STALE_BAR",
      rsi14: null,
      macd: null,
      macdSignal: null,
      macdHistogram: null,
      lastClose: null,
      priceBasis: null,
      observationType: "COMPLETED_DAILY_PRICE_SERIES",
      barStatus: "STALE",
      freshnessStatus: "STALE",
      expectedCompletedDate: prepared.expectedCompletedDate,
      latestAvailableBarDate,
      excludedIncompleteBar: prepared.excludedIncompleteBar,
      retryAttempted,
      fallbackUsed: false,
      dataDate: latestAvailableBarDate,
      recommendedNextAction: "RETRY",
    });
  }

  const selected = selectCompletedPriceSeries(prepared.completedRows);
  if (!selected) {
    return JSON.stringify({
      error: true,
      message: `Incomplete completed-close series for ${ticker}`,
      ticker,
    });
  }
  const closes = selected.observations.map((row) => row.close);

  if (closes.length < 26) {
    return JSON.stringify({
      error: true,
      message: `Insufficient completed price history for ${ticker} (need >=26 data points, got ${closes.length})`,
      ticker,
    });
  }

  const output: Record<string, unknown> = {
    ticker,
    status: "OK",
    priceBasis: selected.priceBasis,
    observationType: "COMPLETED_DAILY_PRICE_SERIES",
    barStatus: "COMPLETE",
    freshnessStatus: prepared.freshnessStatus,
    expectedCompletedDate: prepared.expectedCompletedDate,
    latestAvailableBarDate,
    excludedIncompleteBar: prepared.excludedIncompleteBar,
    retryAttempted,
    fallbackUsed: selected.fallbackUsed,
    recommendedNextAction: "NONE",
  };

  const rsi = wilderRsi(closes);
  output.rsi14 = rsi == null ? null : +rsi.toFixed(2);
  const macdValues = macd(closes);
  output.macd = macdValues ? +macdValues.macd.toFixed(4) : null;
  output.macdSignal = macdValues ? +macdValues.signal.toFixed(4) : null;
  output.macdHistogram = macdValues ? +macdValues.histogram.toFixed(4) : null;
  output.lookbackPeriod = lookback;
  output.lookbackBars = closes.length;

  output.lastClose = +closes[closes.length - 1].toFixed(2);
  output.dataDate = latestAvailableBarDate;

  return JSON.stringify(withNotices(output, freshnessNotices(ticker, prepared.freshnessReason)));
}

type DailyBarObservation = {
  timestamp: number;
  adjustedClose: number | null;
  rawClose: number | null;
  volume: number | null;
};

type PreparedCompletedDailySeries = {
  completedRows: DailyBarObservation[];
  activeRow: DailyBarObservation | null;
  activeSession: boolean;
  excludedIncompleteBar: boolean;
  expectedCompletedDate: string | null;
  freshnessStatus: "CURRENT" | "STALE" | "UNKNOWN";
  /** SESSION_CALENDAR_UNKNOWN when the previous session could not be named for want of a calendar (3.1.0). */
  freshnessReason: string | null;
};

function dailyBarNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function priceSlopeRange(days: number): string {
  if (days <= 20) return "1mo";
  if (days <= 60) return "3mo";
  if (days <= 120) return "6mo";
  if (days <= 250) return "1y";
  return "2y";
}

function prepareCompletedDailySeries(
  result: Record<string, unknown>,
  nowEpoch: number,
): PreparedCompletedDailySeries {
  const timestamps = (result.timestamp as unknown[]) ?? [];
  const indicators = (result.indicators as Record<string, unknown[]> | undefined) ?? {};
  const adjustedCloses =
    ((indicators.adjclose?.[0] as Record<string, unknown[]> | undefined)?.adjclose ?? []);
  const quote =
    (indicators.quote?.[0] as Record<string, unknown[]> | undefined) ?? {};
  const rawCloses = quote.close ?? [];
  const volumes = quote.volume ?? [];
  const rows = timestamps
    .map((timestamp, index): DailyBarObservation | null => {
      const parsedTimestamp = dailyBarNumber(timestamp);
      const adjustedClose = dailyBarNumber(adjustedCloses[index]);
      const rawClose = dailyBarNumber(rawCloses[index]);
      const volume = dailyBarNumber(volumes[index]);
      if (
        parsedTimestamp == null
        || (adjustedClose == null && rawClose == null && volume == null)
      ) return null;
      return { timestamp: parsedTimestamp, adjustedClose, rawClose, volume };
    })
    .filter((row): row is DailyBarObservation => row != null);

  const meta = (result.meta as Record<string, unknown> | undefined) ?? {};
  const timezone = typeof meta.exchangeTimezoneName === "string" ? meta.exchangeTimezoneName : "UTC";
  const currentTradingPeriod =
    (meta.currentTradingPeriod as Record<string, unknown> | undefined) ?? {};
  const regular =
    (currentTradingPeriod.regular as Record<string, unknown> | undefined) ?? {};
  const regularStart = dailyBarNumber(regular.start);
  const regularEnd = dailyBarNumber(regular.end);
  const activeSession =
    regularStart != null
    && regularEnd != null
    && regularStart <= nowEpoch
    && nowEpoch < regularEnd;

  let completedRows = [...rows];
  let activeRow: DailyBarObservation | null = null;
  let excludedIncompleteBar = false;
  if (activeSession && regularStart != null && completedRows.length > 0) {
    const sessionDate = dailyBarDate(regularStart, timezone);
    const lastDate = dailyBarDate(completedRows[completedRows.length - 1].timestamp, timezone);
    if (sessionDate != null && lastDate === sessionDate) {
      activeRow = completedRows.pop() ?? null;
      excludedIncompleteBar = true;
    }
  }

  // A session boundary alone does not make a row usable price evidence.
  // Yahoo can briefly publish a finished daily row with volume/OHLC fields
  // populated while both close series are null. Trim only trailing
  // close-incomplete rows so consumers can report the latest valid completed
  // date as stale; internal gaps remain visible to the price-series selector.
  while (
    completedRows.length > 0
    && completedRows[completedRows.length - 1].adjustedClose == null
    && completedRows[completedRows.length - 1].rawClose == null
  ) {
    completedRows.pop();
    excludedIncompleteBar = true;
  }

  const lastCompletedDate = completedRows.length > 0
    ? dailyBarDate(completedRows[completedRows.length - 1].timestamp, timezone)
    : null;
  const lastCompletedRawClose = completedRows.length > 0
    ? completedRows[completedRows.length - 1].rawClose
    : null;

  // expectedCompletedDate is the most recent session that should already be
  // complete. Outside a regular session that is the session of
  // regularMarketTime. During one it is the previous session, which is only
  // named when proven: Yahoo's previousClose equals the last completed close,
  // or (without previousClose) the exchange's holiday calendar names it.
  // Without a calendar it is never inferred from weekday arithmetic: the
  // status is UNKNOWN with SESSION_CALENDAR_UNKNOWN (BR-PRC-002, 3.1.0).
  let expectedCompletedDate: string | null = null;
  let freshnessStatus: PreparedCompletedDailySeries["freshnessStatus"];
  let freshnessReason: string | null = null;
  if (!activeSession) {
    expectedCompletedDate = dailyBarDate(dailyBarNumber(meta.regularMarketTime), timezone);
    freshnessStatus = expectedCompletedDate == null || lastCompletedDate == null
      ? "UNKNOWN"
      : lastCompletedDate < expectedCompletedDate
        ? "STALE"
        : "CURRENT";
  } else {
    const previousClose = dailyBarNumber(meta.previousClose);
    const sessionDate = dailyBarDate(regularStart, timezone);
    if (previousClose != null && lastCompletedRawClose != null) {
      const matches = Math.abs(lastCompletedRawClose - previousClose)
        <= Math.max(0.01, Math.abs(previousClose) * 1e-6);
      freshnessStatus = matches ? "CURRENT" : "STALE";
      expectedCompletedDate = matches ? lastCompletedDate : null;
    } else {
      const previousSession = sessionDate != null ? previousExchangeSession(meta.exchangeName, sessionDate) : null;
      if (previousSession == null) {
        freshnessStatus = "UNKNOWN";
        freshnessReason = sessionDate != null ? SESSION_CALENDAR_UNKNOWN : null;
      } else if (lastCompletedDate == null || lastCompletedDate > previousSession) {
        freshnessStatus = "UNKNOWN";
      } else {
        freshnessStatus = lastCompletedDate === previousSession ? "CURRENT" : "STALE";
        expectedCompletedDate = previousSession;
      }
    }
  }

  return {
    completedRows,
    activeRow,
    activeSession,
    excludedIncompleteBar,
    expectedCompletedDate,
    freshnessStatus,
    freshnessReason,
  };
}

function completedAdjustedWindowIncomplete(
  completedRows: DailyBarObservation[],
  requiredBars?: number,
): boolean {
  const window = requiredBars == null ? completedRows : completedRows.slice(-requiredBars);
  const hasAnyAdjusted = window.some((row) => row.adjustedClose != null);
  const hasAllAdjusted =
    window.length > 0 && window.every((row) => row.adjustedClose != null);
  const hasAllRaw =
    window.length > 0 && window.every((row) => row.rawClose != null);
  return hasAnyAdjusted && !hasAllAdjusted && hasAllRaw;
}

function selectCompletedPriceSeries(completedRows: DailyBarObservation[]): {
  observations: Array<{ close: number; rawClose: number | null; timestamp: number }>;
  priceBasis: "ADJUSTED_CLOSE" | "UNADJUSTED_CLOSE";
  fallbackUsed: boolean;
} | null {
  const adjustedComplete =
    completedRows.length > 0 && completedRows.every((row) => row.adjustedClose != null);
  const rawComplete =
    completedRows.length > 0 && completedRows.every((row) => row.rawClose != null);
  if (!adjustedComplete && !rawComplete) return null;
  return {
    observations: completedRows.map((row) => ({
      close: adjustedComplete ? row.adjustedClose as number : row.rawClose as number,
      rawClose: row.rawClose,
      timestamp: row.timestamp,
    })),
    priceBasis: adjustedComplete ? "ADJUSTED_CLOSE" : "UNADJUSTED_CLOSE",
    fallbackUsed: !adjustedComplete,
  };
}

export async function getPriceSlope(ticker: string | string[], days: number): Promise<string> {
  if (Array.isArray(ticker)) {
    const results = await mapWithConcurrency(ticker, BATCH_TICKER_CONCURRENCY, (t) => getPriceSlope(t, days));
    return JSON.stringify(Object.fromEntries(ticker.map((t, i) => [t, JSON.parse(results[i])])));
  }

  const normalizedDays = clampInt(days, 5, 1, 500);
  const requiredBars = normalizedDays + 1;
  const range = priceSlopeRange(normalizedDays);
  try {
    const fetchResult = async (retry: boolean): Promise<Record<string, unknown> | undefined> => {
      const cacheBuster = retry ? `&events=div%2Csplits&_=${Date.now()}` : "";
      const payload = (await yGet(
        yahooChartUrl(enc(ticker), `range=${range}&interval=1d${cacheBuster}`),
        false
      )) as Record<string, unknown>;
      return (payload?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
        | Record<string, unknown>
        | undefined;
    };

    let result = await fetchResult(false);
    if (!result) return JSON.stringify({ error: true, message: `No data for ${ticker}`, ticker });

    const nowEpoch = Math.floor(Date.now() / 1000);
    let prepared = prepareCompletedDailySeries(result, nowEpoch);
    let retryAttempted = false;
    if (
      prepared.freshnessStatus === "STALE"
      || completedAdjustedWindowIncomplete(prepared.completedRows, requiredBars)
    ) {
      retryAttempted = true;
      const retryResult = await fetchResult(true);
      if (retryResult) {
        result = retryResult;
        const retried = prepareCompletedDailySeries(result, nowEpoch);
        retried.excludedIncompleteBar =
          prepared.excludedIncompleteBar || retried.excludedIncompleteBar;
        prepared = retried;
      }
    }

    const meta = (result.meta as Record<string, unknown> | undefined) ?? {};
    const timezone = typeof meta.exchangeTimezoneName === "string" ? meta.exchangeTimezoneName : "UTC";
    const latestAvailableBarDate = prepared.completedRows.length > 0
      ? dailyBarDate(prepared.completedRows[prepared.completedRows.length - 1].timestamp, timezone)
      : null;
    if (prepared.freshnessStatus === "STALE") {
      return JSON.stringify({
        ticker,
        days: normalizedDays,
        status: "STALE_BAR",
        startClose: null,
        endClose: null,
        endRawClose: null,
        priceBasis: null,
        observationType: "DAILY_PRICE_BAR",
        calculationBasis: "COMPLETED_SESSION_CLOSE_TO_CLOSE",
        observationCount: 0,
        barStatus: "STALE",
        freshnessStatus: "STALE",
        expectedCompletedDate: prepared.expectedCompletedDate,
        latestAvailableBarDate,
        excludedIncompleteBar: prepared.excludedIncompleteBar,
        retryAttempted,
        fallbackUsed: false,
        slopePct: null,
        direction: "UNKNOWN",
        dataDate: latestAvailableBarDate,
        recommendedNextAction: "RETRY",
      });
    }

    if (prepared.completedRows.length < requiredBars) {
      return JSON.stringify({
        error: true,
        message: `Insufficient completed price data for ${ticker}: need ${requiredBars} bars`,
        ticker,
      });
    }

    const window = prepared.completedRows.slice(-requiredBars);
    const selected = selectCompletedPriceSeries(window);
    if (!selected) {
      return JSON.stringify({
        error: true,
        message: `Incomplete completed-close series for ${ticker}`,
        ticker,
      });
    }

    const { observations, priceBasis, fallbackUsed } = selected;
    const startClose = observations[0].close;
    const endObservation = observations[observations.length - 1];
    const endClose = endObservation.close;
    const slopePct = startClose !== 0 ? +((endClose - startClose) / startClose * 100).toFixed(2) : null;

    let direction: string;
    if (slopePct == null || Math.abs(slopePct) < 0.5) direction = "FLAT";
    else if (slopePct > 0) direction = "UP";
    else direction = "DOWN";

    return JSON.stringify(withNotices({
      ticker,
      days: normalizedDays,
      status: "OK",
      startClose: +startClose.toFixed(2),
      endClose: +endClose.toFixed(2),
      endRawClose: endObservation.rawClose != null ? +endObservation.rawClose.toFixed(2) : null,
      priceBasis,
      observationType: "DAILY_PRICE_BAR",
      calculationBasis: "COMPLETED_SESSION_CLOSE_TO_CLOSE",
      observationCount: requiredBars,
      barStatus: "COMPLETE",
      freshnessStatus: prepared.freshnessStatus,
      expectedCompletedDate: prepared.expectedCompletedDate,
      latestAvailableBarDate,
      excludedIncompleteBar: prepared.excludedIncompleteBar,
      retryAttempted,
      fallbackUsed,
      slopePct,
      direction,
      dataDate: dailyBarDate(endObservation.timestamp, timezone),
      recommendedNextAction: "NONE",
    }, freshnessNotices(ticker, prepared.freshnessReason)));
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

export async function getVolumeRatio(ticker: string | string[], _period: number): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getVolumeRatio(t, _period), tickerCap("analyze_volume_ratio"));
  }

  try {
    const fetchChart = async (retry: boolean): Promise<Record<string, unknown> | undefined> => {
      const cacheBuster = retry ? `&events=div%2Csplits&_=${Date.now()}` : "";
      const payload = await yGet(
        yahooChartUrl(enc(ticker), `range=6mo&interval=1d${cacheBuster}`),
        false
      ) as Record<string, unknown>;
      return (payload?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
        | Record<string, unknown>
        | undefined;
    };
    let chartResult = await fetchChart(false);
    if (!chartResult) return noData(ticker);
    const nowEpoch = Math.floor(Date.now() / 1000);
    let prepared = prepareCompletedDailySeries(chartResult, nowEpoch);
    let retryAttempted = false;
    const initialLatestRow = prepared.completedRows[prepared.completedRows.length - 1];
    if (
      prepared.freshnessStatus === "STALE"
      || initialLatestRow?.volume == null
    ) {
      retryAttempted = true;
      const retryResult = await fetchChart(true);
      if (retryResult) {
        chartResult = retryResult;
        const retried = prepareCompletedDailySeries(chartResult, nowEpoch);
        retried.excludedIncompleteBar =
          prepared.excludedIncompleteBar || retried.excludedIncompleteBar;
        prepared = retried;
      }
    }
    const meta = (chartResult.meta as Record<string, unknown> | undefined) ?? {};
    const timezone = typeof meta.exchangeTimezoneName === "string" ? meta.exchangeTimezoneName : "UTC";
    const dataDate = prepared.completedRows.length > 0
      ? dailyBarDate(prepared.completedRows[prepared.completedRows.length - 1].timestamp, timezone)
      : null;
    if (prepared.freshnessStatus === "STALE") {
      return JSON.stringify({
        ticker,
        status: "STALE_BAR",
        lastVolume: null,
        avgVolume10d: null,
        avgVolume90d: null,
        ratio10d: null,
        ratio90d: null,
        volumeFlag: null,
        observationType: "COMPLETED_DAILY_VOLUME",
        barStatus: "STALE",
        freshnessStatus: "STALE",
        expectedCompletedDate: prepared.expectedCompletedDate,
        latestAvailableBarDate: dataDate,
        excludedIncompleteBar: prepared.excludedIncompleteBar,
        retryAttempted,
        dataDate,
        recommendedNextAction: "RETRY",
      });
    }

    const completedRows = prepared.completedRows;
    const lastRow = completedRows[completedRows.length - 1];
    if (!lastRow || lastRow.volume == null) {
      return JSON.stringify(withNotices({
        ticker,
        status: "PARTIAL",
        lastVolume: null,
        avgVolume10d: null,
        avgVolume90d: null,
        ratio10d: null,
        ratio90d: null,
        volumeFlag: null,
        observationType: "COMPLETED_DAILY_VOLUME",
        barStatus: "INCOMPLETE",
        freshnessStatus: prepared.freshnessStatus,
        expectedCompletedDate: prepared.expectedCompletedDate,
        latestAvailableBarDate: dataDate,
        excludedIncompleteBar: prepared.excludedIncompleteBar,
        retryAttempted,
        dataDate,
        recommendedNextAction: "RETRY",
      }, freshnessNotices(ticker, prepared.freshnessReason)));
    }
    // The ratio uses the unrounded average; only the published averages and ratios are rounded, half away from zero,
    // as in Python (R5-pricing-1, Q3c/Q3d).
    const average = (count: number): number | null => {
      const window = completedRows.slice(-(count + 1), -1);
      if (window.length < count || window.some((row) => row.volume == null)) return null;
      return window.reduce((sum, row) => sum + (row.volume as number), 0) / window.length;
    };
    const lastVolume = lastRow.volume as number;
    const rawAvg10d = average(10);
    const rawAvg90d = average(90);
    const avg10d = rawAvg10d != null ? roundHalfAwayFromZero(rawAvg10d, 0) : null;
    const avg90d = rawAvg90d != null ? roundHalfAwayFromZero(rawAvg90d, 0) : null;
    // A zero-volume session has no ratio and no flag, with an explicit notice (Q3c, R5-pricing-2, 3.1.0).
    const zeroVolume = lastVolume === 0;
    const ratio10d = !zeroVolume && rawAvg10d != null && rawAvg10d !== 0
      ? roundHalfAwayFromZero(lastVolume / rawAvg10d, 3)
      : null;
    const ratio90d = !zeroVolume && rawAvg90d != null && rawAvg90d !== 0
      ? roundHalfAwayFromZero(lastVolume / rawAvg90d, 3)
      : null;

    let volumeFlag: string | null = null;
    if (ratio10d != null) {
      if (ratio10d > 1.5) volumeFlag = "HIGH";
      else if (ratio10d < 0.7) volumeFlag = "LOW";
      else volumeFlag = "NORMAL";
    }

    return JSON.stringify(withNotices({
      ticker,
      status: "OK",
      lastVolume,
      avgVolume10d: avg10d,
      avgVolume90d: avg90d,
      ratio10d,
      ratio90d,
      volumeFlag,
      observationType: "COMPLETED_DAILY_VOLUME",
      barStatus: "COMPLETE",
      freshnessStatus: prepared.freshnessStatus,
      expectedCompletedDate: prepared.expectedCompletedDate,
      latestAvailableBarDate: dataDate,
      excludedIncompleteBar: prepared.excludedIncompleteBar,
      retryAttempted,
      dataDate,
      recommendedNextAction: "NONE",
    }, [...(zeroVolume ? [zeroVolumeNotice(ticker)] : []), ...freshnessNotices(ticker, prepared.freshnessReason)]));
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

export async function getMaPosition(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getMaPosition(t), tickerCap("analyze_moving_average_position"));
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
      priceBasis: "REGULAR_MARKET_PRICE",
      observationType: "REGULAR_MARKET_QUOTE_VS_TRAILING_AVERAGES",
      priceTimestamp: (fi.priceTimestamp as string | null) ?? null,
      marketState: (fi.marketState as string | null) ?? null,
      marketOpen: (fi.marketOpen as boolean | null) ?? null,
      fiftyDayAverage: fiftyDma,
      twoHundredDayAverage: twoHundredDma,
      pctVs50dma: pctVs50,
      pctVs200dma: pctVs200,
      regime50,
      regime200,
      trend,
      // The quote's own date, never today's (R5-pricing-1, Q3d).
      dataDate: (fi.lastTradeDate as string | null) ?? null,
      recommendedNextAction: "NONE",
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

export async function getShortMomentum(ticker: string | string[]): Promise<string> {
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getShortMomentum(t), tickerCap("get_short_momentum"));
  }
  try {
    const si = JSON.parse(await getShortInterest(ticker));
    if (si.error) return JSON.stringify(si);

    const sharesShort = si.sharesShort as number | null;
    const sharesShortPrior = si.sharesShortPriorMonth as number | null;
    const shortPctFloatRaw = si.shortPercentOfFloat as number | null;
    const shortRatio = si.shortRatio as number | null;
    const dateShort = si.dateShortInterest;
    let observationDate: string | null = null;
    if (typeof dateShort === "number" && Number.isFinite(dateShort)) {
      const timestampMs = dateShort > 1_000_000_000_000 ? dateShort : dateShort * 1000;
      observationDate = new Date(timestampMs).toISOString().slice(0, 10);
    } else if (typeof dateShort === "string" && dateShort.trim()) {
      const numeric = Number(dateShort);
      const parsed = Number.isFinite(numeric)
        ? new Date(numeric > 1_000_000_000_000 ? numeric : numeric * 1000)
        : new Date(dateShort);
      if (!Number.isNaN(parsed.getTime())) observationDate = parsed.toISOString().slice(0, 10);
    }

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
      dateShortInterest: observationDate,
      dataDate: observationDate,
      dataDateBasis: observationDate ? "SHORT_INTEREST_OBSERVATION" : "UNAVAILABLE",
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

/** Without expiry_after, the hedge screen starts at expiries this many days out. */
const HEDGE_DEFAULT_MIN_DAYS = 7;

export async function getPutHedgeCandidates(
  ticker: string,
  otmPctMin: number,
  otmPctMax: number,
  budgetUsd: number,
  expiryAfter: string
): Promise<string> {
  try {
    if (budgetUsd <= 0) {
      return JSON.stringify({
        error: true,
        code: "INPUT_VALIDATION_ERROR",
        message: "budget_usd must be greater than zero",
        ticker,
      });
    }
    if (otmPctMin < 0 || otmPctMax < otmPctMin) {
      return JSON.stringify({
        error: true,
        code: "INPUT_VALIDATION_ERROR",
        message: "otm_pct_min and otm_pct_max must define a non-negative ascending range",
        ticker,
      });
    }
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

    // Filter and select nearest 2. Without expiry_after, skip expiries less
    // than a week out: they rarely suit a hedge and often have no quotes. When
    // every expiry is under a week out the screen fails closed instead of
    // returning the contracts it says it skips (R5-pricing-1, Q3i call 23).
    const defaultAfter = new Date(Date.now() + HEDGE_DEFAULT_MIN_DAYS * 86_400_000).toISOString().slice(0, 10);
    const qualifying = dates.filter((d) => d >= (expiryAfter || defaultAfter)).slice(0, 2);
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
      contractCostBasis: "ASK";
      withinBudget: boolean;
      openInterest: number;
      volume: number;
      quoteStatus: "TWO_SIDED";
      ivPctile: number | null;
      ivFlag: string | null;
      otmPct: number;
    }
    const candidates: Candidate[] = [];
    const consideredContracts: Record<string, unknown>[] = [];
    const excludedContracts = {
      noTwoSidedQuote: 0,
      noLiquidityEvidence: 0,
    };

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
          consideredContracts.push(p);

          const bid = Number(p.bid ?? 0);
          const ask = Number(p.ask ?? 0);
          const oi = Number(p.openInterest ?? 0);
          const volume = Number(p.volume ?? 0);
          if (bid <= 0 || ask <= 0) {
            excludedContracts.noTwoSidedQuote += 1;
            continue;
          }
          if (oi <= 0 && volume <= 0) {
            excludedContracts.noLiquidityEvidence += 1;
            continue;
          }
          const mid = +((bid + ask) / 2).toFixed(2);
          const contractCost = +(ask * 100).toFixed(2);
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
            contractCostBasis: "ASK",
            withinBudget: contractCost <= budgetUsd,
            openInterest: oi,
            volume,
            quoteStatus: "TWO_SIDED",
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
    const dataDate = getLastTradingDate();
    const dataQuality = computeDataQuality(consideredContracts, dataDate);
    const warnings = [...dataQuality.warnings];
    if (excludedContracts.noTwoSidedQuote) warnings.push("UNQUOTED_CONTRACTS_EXCLUDED");
    if (excludedContracts.noLiquidityEvidence) warnings.push("NO_LIQUIDITY_EVIDENCE_EXCLUDED");
    const uniqueWarnings = [...new Set(warnings)];
    const budgetFeasible: boolean | null = dataQuality.quality === "LOW"
      ? null
      : candidates.length
      ? candidates.some((c) => c.withinBudget)
      : consideredContracts.length ? null : false;
    const status = dataQuality.quality === "LOW" || (consideredContracts.length > 0 && candidates.length === 0)
      ? "PARTIAL"
      : candidates.length ? "OK" : "EMPTY_RESULT";

    let note: string;
    let budgetGapUsd: number | null = null;
    if (status === "PARTIAL") {
      note = "Option-chain quality is insufficient for a hedge or budget conclusion; retry before use.";
    } else if (!candidates.length) {
      note = "No put options found in the specified OTM range.";
    } else if (budgetFeasible === false) {
      const nearest = candidates.reduce((a, b) => (a.contractCost < b.contractCost ? a : b));
      budgetGapUsd = +(nearest.contractCost - budgetUsd).toFixed(2);
      note = `No candidates within budget. Nearest: $${nearest.strike} put at $${nearest.contractCost}/contract ask cost vs $${budgetUsd} budget.`;
    } else {
      const count = candidates.filter((c) => c.withinBudget).length;
      note = `${count} candidate(s) within $${budgetUsd} budget using executable ask cost.`;
    }
    if (consideredContracts.length && !candidates.length) {
      note = "No decision-usable hedge candidates: contracts lacked a two-sided quote or liquidity evidence.";
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
      status,
      decisionGrade: false,
      decisionUse: "HEDGE_SCREEN_ONLY",
      consideredContracts: consideredContracts.length,
      excludedContracts,
      dataQuality,
      warnings: uniqueWarnings,
      recommendedNextAction: status === "PARTIAL" ? "RETRY" : "NONE",
      note,
      dataDate,
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

// Module-level in-memory cache for options flow window-label readings.
// Persists within a single Worker instance lifetime.
const OPTIONS_FLOW_CACHE_MAX = 500;
const _optionsFlowCache = new Map<string, { data: Record<string, unknown>; storedAt: number }>();

/** Max-pain strike: where the total intrinsic payout to call and put holders at expiry is smallest. */
export function computeMaxPainStrike(calls: Record<string, unknown>[], puts: Record<string, unknown>[]): number | null {
  const strikes = [...new Set([...calls, ...puts].map((c) => c.strike as number).filter((v) => Number.isFinite(v)))].sort((a, b) => a - b);
  let best: number | null = null;
  let minPain = Infinity;
  for (const s of strikes) {
    const callPain = calls.reduce((sum, c) => sum + Math.max(0, s - (c.strike as number)) * ((c.openInterest as number) || 0), 0);
    const putPain = puts.reduce((sum, p) => sum + Math.max(0, (p.strike as number) - s) * ((p.openInterest as number) || 0), 0);
    if (callPain + putPain < minPain) {
      minPain = callPain + putPain;
      best = s;
    }
  }
  return best;
}

export async function getOptionsFlowScan(ticker: string, windowLabel: string): Promise<string> {
  try {
    const nearest = await yGetFullOptions(ticker);
    const scanExpiry = nextOptionExpiry(nearest.dates, usMarketDate());
    const fullOptions = scanExpiry && scanExpiry !== nearest.dates[0] ? await yGetFullOptions(ticker, scanExpiry) : nearest;
    const calls = fullOptions.calls.map(normalizeContractIv);
    const puts = fullOptions.puts.map(normalizeContractIv);
    if (!calls.length && !puts.length) {
      return JSON.stringify({ error: true, message: `No options data for ${ticker}`, ticker });
    }

    let currentPrice: number | null = null;
    try {
      const pd = await yGet(yahooQuoteSummaryUrl(enc(ticker), "price")) as Record<string, unknown>;
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
      // The same max-pain definition as summarize_options_flow: the strike
      // where option holders' total payout is smallest (not the strike with
      // the most open interest).
      maxPainStrike = computeMaxPainStrike(calls, puts);
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
    if (quality === "LOW") {
      scanWarnings.push("OPTIONS_CHAIN_LOW_QUALITY");
    }
    const allContractCount = calls.length + puts.length;
    const placeholderIvCount = dataQuality.placeholderIvCount;

    let ivPctile: number | null = null;
    let dataDate: string | null = null;
    let realizedVolPriceBasis: string | null = null;
    let historicalBarStatus = "NOT_USED";
    let freshnessStatus = "UNKNOWN";
    let freshnessReason: string | null = null;
    let excludedIncompleteBar = false;
    let retryAttempted = false;
    let fallbackUsed = false;
    if (quality === "LOW" && placeholderIvCount > allContractCount * 0.5) {
      scanWarnings.push("IV_PERCENTILE_UNAVAILABLE_PLACEHOLDER_IV");
    } else {
      try {
        const fetchChart = async (retry: boolean): Promise<Record<string, unknown> | undefined> => {
          const cacheBuster = retry ? `&events=div%2Csplits&_=${Date.now()}` : "";
          const chartD = await yGet(
            yahooChartUrl(enc(ticker), `range=1y&interval=1d${cacheBuster}`),
            false
          ) as Record<string, unknown>;
          return (chartD?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
            | Record<string, unknown>
            | undefined;
        };
        let chartRes = await fetchChart(false);
        if (!chartRes) throw new Error("daily chart unavailable");
        let prepared = prepareCompletedDailySeries(
          chartRes,
          Math.floor(Date.now() / 1000)
        );
        if (
          prepared.freshnessStatus === "STALE"
          || completedAdjustedWindowIncomplete(prepared.completedRows)
        ) {
          retryAttempted = true;
          const retryResult = await fetchChart(true);
          if (retryResult) {
            chartRes = retryResult;
            const retried = prepareCompletedDailySeries(
              chartRes,
              Math.floor(Date.now() / 1000)
            );
            retried.excludedIncompleteBar =
              prepared.excludedIncompleteBar || retried.excludedIncompleteBar;
            prepared = retried;
          }
        }
        const chartMeta = (chartRes.meta as Record<string, unknown> | undefined) ?? {};
        const timezone = typeof chartMeta.exchangeTimezoneName === "string"
          ? chartMeta.exchangeTimezoneName
          : "UTC";
        dataDate = prepared.completedRows.length > 0
          ? dailyBarDate(
            prepared.completedRows[prepared.completedRows.length - 1].timestamp,
            timezone
          )
          : null;
        freshnessStatus = prepared.freshnessStatus;
        freshnessReason = prepared.freshnessReason;
        excludedIncompleteBar = prepared.excludedIncompleteBar;
        if (prepared.freshnessStatus === "STALE") {
          historicalBarStatus = "STALE";
          scanWarnings.push("IV_PERCENTILE_UNAVAILABLE_STALE_DAILY_BARS");
        } else {
          const selected = selectCompletedPriceSeries(prepared.completedRows);
          if (!selected) {
            historicalBarStatus = "INCOMPLETE";
            scanWarnings.push("IV_PERCENTILE_UNAVAILABLE_INCOMPLETE_DAILY_BARS");
          } else {
            historicalBarStatus = "COMPLETE";
            realizedVolPriceBasis = selected.priceBasis;
            fallbackUsed = selected.fallbackUsed;
            const closes = selected.observations.map((row) => row.close);
            if (closes.length >= 31 && atmIv != null) {
              const rets: number[] = [];
              for (let i = 1; i < closes.length; i++) {
                rets.push((closes[i] - closes[i - 1]) / closes[i - 1]);
              }
              const rollVols: number[] = [];
              for (let i = 29; i < rets.length; i++) {
                const windowSlice = rets.slice(i - 29, i + 1);
                const mean = windowSlice.reduce((a, b) => a + b, 0) / windowSlice.length;
                const variance =
                  windowSlice.reduce((a, b) => a + (b - mean) ** 2, 0)
                  / windowSlice.length;
                rollVols.push(Math.sqrt(variance * 252));
              }
              if (rollVols.length >= 5) {
                const rvMin = Math.min(...rollVols);
                const rvMax = Math.max(...rollVols);
                if (rvMax > rvMin) {
                  ivPctile = Math.max(
                    0,
                    Math.min(
                      100,
                      Math.round((atmIv - rvMin) / (rvMax - rvMin) * 100)
                    )
                  );
                }
              }
            }
          }
        }
      } catch {
        historicalBarStatus = "UNAVAILABLE";
        scanWarnings.push("IV_PERCENTILE_UNAVAILABLE_DAILY_BARS");
      }
    }

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
      ? ((prevData.putVolPer1PctStockAdv as number | null) ?? (prevData.putVolVs10dAvg as number | null) ?? (prevData.pcRatio as number | null))
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
      const ivStr = ivPctile != null ? `${ivPctile}%` : "N/A";
      const pvStr = putVolVs10d != null ? `${putVolVs10d.toFixed(2)}x` : "N/A";
      const pcStr = pcRatio != null ? pcRatio.toFixed(2) : "N/A";
      formattedBlock = `OPTIONS FLOW SCAN [${windowLabel}] ${ticker} | P/C: ${pcStr} | IV vs 1y RV range: ${ivStr} | Put vol / 1% stock ADV: ${pvStr} | Trend: ${putVolTrend} | Advisory: ${bracket ?? "N/A"} bracket`;
    }

    const resultData: Record<string, unknown> = {
      ticker, windowLabel, dataDate,
      pcRatio,
      // Named for what they measure (2.4.4); the old aliases were removed in 3.0.0.
      ivVsRealizedRangePct: ivPctile,
      putVolPer1PctStockAdv: putVolVs10d,
      putVolTrend,
      expiry: scanExpiry ?? null,
      expirySelection: "NEXT_AFTER_TODAY",
      maxPainStrike, bracket, formattedBlock,
      realizedVolPriceBasis,
      historicalObservationType: "COMPLETED_DAILY_PRICE_SERIES",
      historicalBarStatus,
      freshnessStatus,
      excludedIncompleteBar,
      retryAttempted,
      fallbackUsed,
      recommendedNextAction:
        quality === "LOW"
          || historicalBarStatus === "STALE"
          || historicalBarStatus === "UNAVAILABLE"
          || historicalBarStatus === "INCOMPLETE"
          ? "RETRY"
          : "NONE",
      dataQuality, warnings: scanWarnings,
    };

    setBounded(_optionsFlowCache, `${ticker}:${windowLabel}`, { data: resultData, storedAt: Date.now() }, OPTIONS_FLOW_CACHE_MAX);
    return JSON.stringify(withNotices({ ...resultData }, freshnessNotices(ticker, freshnessReason)));
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

export async function getPriceTargetBracket(ticker: string, ioPt: number): Promise<string> {
  if (!(ioPt > 0)) {
    // A caller's bad input, not a provider failure (2.25.2, F42).
    return mcpFailure("calculate_price_target_distance", ErrorCode.INPUT_VALIDATION_ERROR, "reference_target_price (or io_pt alias) must be a positive number");
  }
  try {
    const fi = JSON.parse(await getFastInfo(ticker)) as Record<string, unknown>;
    const currentPrice = fi.lastPrice as number | null;
    if (currentPrice == null) {
      return JSON.stringify({ error: true, message: `No price data for ${ticker}`, ticker });
    }

    // Display rounding is half away from zero on the decimal value, as in Python (R5-pricing-1, Q3d); a zero price
    // has no distance to the target (G1).
    const referenceTargetPct = roundHalfAwayFromZero(currentPrice / ioPt * 100, 1);
    const distanceToTargetPct = currentPrice !== 0 ? roundHalfAwayFromZero((ioPt - currentPrice) / currentPrice * 100, 1) : null;

    return JSON.stringify({
      ticker,
      currentPrice: roundHalfAwayFromZero(currentPrice, 4),
      priceBasis: "REGULAR_MARKET_PRICE",
      observationType: "REGULAR_MARKET_QUOTE_VS_USER_REFERENCE",
      priceTimestamp: (fi.priceTimestamp as string | null) ?? null,
      marketState: (fi.marketState as string | null) ?? null,
      marketOpen: (fi.marketOpen as boolean | null) ?? null,
      referenceTargetPrice: ioPt,
      referenceTargetPct,
      currentToTargetRatioPct: referenceTargetPct,
      distanceToTargetPct,
      distanceConvention: "(referenceTargetPrice-currentPrice)/currentPrice*100",
      ioPt,
      eqfPct: referenceTargetPct,
      invertedFlag: referenceTargetPct >= 100,
      // From the raw prices, not the rounded referenceTargetPct (2.8.0).
      priceAtOrAboveTarget: currentPrice > 0 && ioPt > 0 ? currentPrice >= ioPt : null,
      // The quote's own date, never today's (R5-pricing-1, Q3d).
      dataDate: (fi.lastTradeDate as string | null) ?? null,
      recommendedNextAction: "NONE",
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

export async function getPositionScoreInputs(ticker: string | string[]): Promise<string> {
  // Q3i call 4 (3.2.0): one ticker, as the declared schema says; a list is refused, not batched.
  if (Array.isArray(ticker)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: POSITION_SINGLE_TICKER_MESSAGE });
  }
  try {
    // A component that throws is FAILED, as in Python's gather(return_exceptions=True) (Q3i call 1, 3.2.0); one
    // provider outage no longer fails the whole tool.
    const [upgradeRaw, consensusRaw, priceRaw, earningsRaw, techRaw, maRaw] = await Promise.allSettled([
      getAnalystUpgradeRadar(ticker, 30),
      getAnalystConsensus(ticker),
      getPriceStats(ticker),
      getEarningsMomentum(ticker),
      getTechnicalIndicators(ticker, INDICATOR_MIN_LOOKBACK),
      getMaPosition(ticker),
    ]);

    const parse = (result: PromiseSettledResult<string>): Record<string, unknown> => {
      if (result.status === "rejected") return {};
      try {
        const value = JSON.parse(result.value) as unknown;
        return value != null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
      } catch { return {}; }
    };

    const upgrade = parse(upgradeRaw);
    const consensus = parse(consensusRaw);
    const price = parse(priceRaw);
    const earnings = parse(earningsRaw);
    const tech = parse(techRaw);
    const ma = parse(maRaw);

    const t1 = {
      analystNetSentiment: upgrade.netSentiment ?? null,
      upgrades30d: upgrade.upgrades ?? null,
      downgrades30d: upgrade.downgrades ?? null,
      initiations30d: upgrade.initiations ?? null,
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
      macd: tech.macd ?? null,
      macdHistogram: tech.macdHistogram ?? null,
      maPosition: ma.trend ?? null,
      pctFrom50dma: ma.pctVs50dma ?? null,
      pctFrom200dma: ma.pctVs200dma ?? null,
      lastClose: tech.lastClose ?? null,
    };
    // Q3i call 1 (3.2.0): Python's six component keys; an empty or error payload is FAILED, STALE_BAR/PARTIAL limited.
    const componentPayloads: Record<string, Record<string, unknown>> = {
      analystUpgrades: upgrade,
      analystConsensus: consensus,
      priceStats: price,
      earningsMomentum: earnings,
      technicalIndicators: tech,
      maPosition: ma,
    };
    const componentStatus: Record<string, string> = {};
    const failedComponents: string[] = [];
    const limitedComponents: string[] = [];
    for (const [name, payload] of Object.entries(componentPayloads)) {
      if (Object.keys(payload).length === 0 || payload.error) {
        componentStatus[name] = "FAILED";
        failedComponents.push(name);
      } else if (payload.status === "STALE_BAR" || payload.status === "PARTIAL") {
        componentStatus[name] = payload.status;
        limitedComponents.push(name);
      } else {
        componentStatus[name] = "OK";
      }
    }
    const degraded = failedComponents.length > 0 || limitedComponents.length > 0;
    // The components' notices (3.1.0) reach this tool's meta.warnings, once each.
    const notices = uniqueNotices(Object.values(componentPayloads).flatMap((payload) => takeNotices(payload)));
    // G6 / Q3d (3.2.0): the quote date is the date of the quote behind currentPrice; no date falls back to today.
    const quoteTimestamp = typeof price.priceTimestamp === "string" && price.priceTimestamp ? price.priceTimestamp : null;

    return JSON.stringify(withNotices({
      ticker,
      status: degraded ? "PARTIAL" : "OK",
      dataDate: (tech.dataDate as string | null) ?? (ma.dataDate as string | null) ?? null,
      quoteDataDate: quoteTimestamp ? quoteTimestamp.slice(0, 10) : null,
      quoteTimestamp,
      completedBarDataDate: (tech.dataDate as string | null) ?? null,
      analystInputs: t1,
      pricePositionInputs: t2,
      earningsInputs: t4,
      technicalInputs: t5,
      componentStatus,
      failedComponents,
      limitedComponents,
      recommendedNextAction: degraded ? "RETRY" : "NONE",
    }, notices));
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

function classifyQuoteFreshness(
  quoteTimestamp: string | null,
  retrievedAt: string,
  marketOpen: boolean | null,
): string {
  if (!quoteTimestamp) return "UNKNOWN";
  try {
    const now = new Date(retrievedAt);
    const observed = new Date(quoteTimestamp);
    if (!Number.isFinite(now.getTime()) || !Number.isFinite(observed.getTime())) {
      return "UNKNOWN";
    }
    const diffMs = now.getTime() - observed.getTime();
    // Allow small provider/host clock skew, but reject genuinely future quotes.
    if (diffMs < -5 * 60 * 1000) return "UNKNOWN";
    const diffHours = diffMs / (1000 * 60 * 60);
    if (diffHours <= 0.5) return "FRESH";
    if (marketOpen === true) return "STALE";
    if (
      (now.getUTCDay() === 6 || now.getUTCDay() === 0)
      && observed.getUTCDay() === 5
      && diffHours <= 96
    ) {
      return "WEEKEND_EXPECTED_STALE";
    }
    if (diffHours <= 96) return "MARKET_CLOSED_EXPECTED_STALE";
    if (diffHours <= 168) return "STALE";
    return "VERY_STALE";
  } catch {
    return "UNKNOWN";
  }
}

// Listings quoted in a currency's minor unit: [ISO currency, minor units per unit].
const MINOR_UNIT_CURRENCIES: Record<string, [string, number]> = {
  GBp: ["GBP", 100], GBX: ["GBP", 100], ZAc: ["ZAR", 100], ILA: ["ILS", 100],
};

/** The ISO currency behind a listing currency and how many listing units make one of it. */
export function listingCurrencyUnit(currency: string): { iso: string; perUnit: number } {
  const minor = MINOR_UNIT_CURRENCIES[currency];
  return minor ? { iso: minor[0], perUnit: minor[1] } : { iso: currency.toUpperCase(), perUnit: 1 };
}

/** Mean daily volume x close over exactly `count` sessions, or null when any is missing. */
export function averageTradedValue(rows: { volume: number | null; rawClose: number | null }[], count: number): number | null {
  if (rows.length !== count || rows.some((row) => row.volume == null || row.rawClose == null)) return null;
  return rows.reduce((sum, row) => sum + (row.volume as number) * (row.rawClose as number), 0) / count;
}

type UsdConversion =
  | { ok: true; localPerUsd: number; fxRate: number; fxCurrency: string; fxPriceTimestamp: string | null; note: string }
  | { ok: false; note: string };

/** Listing-currency units per US dollar, from Yahoo's <ISO>=X quote (ISO units per USD). */
async function usdConversion(currency: string | null): Promise<UsdConversion> {
  if (!currency) return { ok: false, note: "listing currency unknown" };
  const { iso, perUnit } = listingCurrencyUnit(currency);
  if (iso === "USD") return { ok: true, localPerUsd: perUnit, fxRate: 1, fxCurrency: "USD", fxPriceTimestamp: null, note: perUnit === 1 ? "" : ` [${currency} = USD/${perUnit}]` };
  try {
    const fxFi = JSON.parse(await getFastInfo(`${iso}=X`)) as Record<string, unknown>;
    const rate = fxFi.lastPrice as number | null;
    if (rate == null || !(rate > 0)) return { ok: false, note: `${iso}=X rate unavailable` };
    return {
      ok: true,
      localPerUsd: rate * perUnit,
      fxRate: +rate.toFixed(4),
      fxCurrency: iso,
      fxPriceTimestamp: (fxFi.priceTimestamp as string | null) ?? null,
      note: ` [${currency}→USD at ${rate.toFixed(4)} ${iso}/USD${perUnit === 1 ? "" : `, ${perUnit} ${currency} per ${iso}`}]`,
    };
  } catch {
    return { ok: false, note: `${iso}=X fetch failed` };
  }
}

/** $14.22B above a billion, else $12.3M. */
function usdCompact(value: number): string {
  return value >= 1e9 ? `$${(value / 1e9).toFixed(2)}B` : `$${(value / 1e6).toFixed(1)}M`;
}

export async function getVolumeGate(ticker: string, foreignExchange: boolean, minAdvUsd: number | null = null): Promise<string> {
  try {
    const fi = JSON.parse(await getFastInfo(ticker)) as Record<string, unknown>;
    const currency = fi.currency as string | null;
    const fetchChart = async (retry: boolean): Promise<Record<string, unknown> | undefined> => {
      const cacheBuster = retry ? `&events=div%2Csplits&_=${Date.now()}` : "";
      const payload = await yGet(
        yahooChartUrl(enc(ticker), `range=6mo&interval=1d${cacheBuster}`),
        false
      ) as Record<string, unknown>;
      return (payload?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as
        | Record<string, unknown>
        | undefined;
    };
    let chartResult = await fetchChart(false);
    if (!chartResult) return noData(ticker);
    const nowEpoch = Math.floor(Date.now() / 1000);
    let prepared = prepareCompletedDailySeries(chartResult, nowEpoch);
    let retryAttempted = false;
    const initialLatestRow = prepared.completedRows[prepared.completedRows.length - 1];
    if (
      prepared.freshnessStatus === "STALE"
      || initialLatestRow?.volume == null
      || initialLatestRow?.rawClose == null
    ) {
      retryAttempted = true;
      const retryResult = await fetchChart(true);
      if (retryResult) {
        chartResult = retryResult;
        const retried = prepareCompletedDailySeries(chartResult, nowEpoch);
        retried.excludedIncompleteBar =
          prepared.excludedIncompleteBar || retried.excludedIncompleteBar;
        prepared = retried;
      }
    }
    const chartMeta = (chartResult.meta as Record<string, unknown> | undefined) ?? {};
    const timezone = typeof chartMeta.exchangeTimezoneName === "string"
      ? chartMeta.exchangeTimezoneName
      : "UTC";
    const dataDate = prepared.completedRows.length > 0
      ? dailyBarDate(prepared.completedRows[prepared.completedRows.length - 1].timestamp, timezone)
      : null;
    if (prepared.freshnessStatus === "STALE") {
      return JSON.stringify({
        ticker,
        status: "STALE_BAR",
        currency,
        lastVolume: null,
        lastClose: null,
        priceBasis: "UNADJUSTED_CLOSE",
        adv10d: null,
        adv20d: null,
        adv90d: null,
        ratio20d: null,
        fxRate: null,
        fxPriceTimestamp: null,
        notionalUsd: null,
        meetsMinimum: null,
        minAdvUsd,
        observationType: "COMPLETED_DAILY_VOLUME_NOTIONAL",
        barStatus: "STALE",
        freshnessStatus: "STALE",
        expectedCompletedDate: prepared.expectedCompletedDate,
        latestAvailableBarDate: dataDate,
        excludedIncompleteBar: prepared.excludedIncompleteBar,
        retryAttempted,
        dataDate,
        note: "Volume gate UNKNOWN — latest completed daily bar is stale",
        recommendedNextAction: "RETRY",
      });
    }

    const rows = prepared.completedRows;
    const lastRow = rows[rows.length - 1];
    if (!lastRow || lastRow.volume == null || lastRow.rawClose == null) {
      return JSON.stringify(withNotices({
        ticker,
        status: "PARTIAL",
        currency,
        lastVolume: null,
        lastClose: null,
        priceBasis: "UNADJUSTED_CLOSE",
        adv10d: null,
        adv20d: null,
        adv90d: null,
        ratio20d: null,
        fxRate: null,
        fxPriceTimestamp: null,
        notionalUsd: null,
        meetsMinimum: null,
        minAdvUsd,
        observationType: "COMPLETED_DAILY_VOLUME_NOTIONAL",
        barStatus: "INCOMPLETE",
        freshnessStatus: prepared.freshnessStatus,
        expectedCompletedDate: prepared.expectedCompletedDate,
        latestAvailableBarDate: dataDate,
        excludedIncompleteBar: prepared.excludedIncompleteBar,
        retryAttempted,
        dataDate,
        note: "Volume gate UNKNOWN — latest completed session lacks price or volume",
        recommendedNextAction: "RETRY",
      }, freshnessNotices(ticker, prepared.freshnessReason)));
    }
    const average = (count: number): number | null => {
      const window = rows.slice(-(count + 1), -1);
      if (window.length < count || window.some((row) => row.volume == null)) return null;
      return Math.round(
        window.reduce((sum, row) => sum + (row.volume as number), 0) / window.length
      );
    };
    const lastVolume = lastRow.volume as number;
    const lastPrice = lastRow.rawClose as number;
    const adv10d = average(10);
    const adv20d = average(20);
    const adv90d = average(90);

    let ratio20d: number | null = null;
    let notionalUsd: number | null = null;
    let adv20dTradedValueUsd: number | null = null;
    let recommendedNextAction = "NONE";
    let note: string;
    if (adv20d != null && adv20d > 0) ratio20d = +(lastVolume / adv20d).toFixed(2);
    const adv20dTradedValue = averageTradedValue(rows.slice(-21, -1), 20);
    const fx = await usdConversion(currency);
    if (adv20dTradedValue == null) {
      recommendedNextAction = "RETRY";
      note = "Volume gate UNKNOWN — fewer than 20 prior sessions with volume and close";
    } else if (!fx.ok) {
      recommendedNextAction = "RETRY";
      note = `Volume gate UNKNOWN — ${fx.note}`;
    } else {
      adv20dTradedValueUsd = +(adv20dTradedValue / fx.localPerUsd).toFixed(2);
      notionalUsd = +((lastVolume * lastPrice) / fx.localPerUsd).toFixed(2);
      const meets = meetsMinimum(adv20dTradedValueUsd, minAdvUsd);
      note = `20d average traded value ${usdCompact(adv20dTradedValueUsd)}`
        + (meets != null && minAdvUsd != null ? ` (${meets ? "≥" : "<"} min_adv_usd ${usdCompact(minAdvUsd)})` : "")
        + (ratio20d != null ? `; latest session ${ratio20d.toFixed(2)}x 20d ADV` : "")
        + fx.note;
    }
    void foreignExchange; // accepted for compatibility: non-USD listings are always converted
    const fxRate = fx.ok ? fx.fxRate : null;
    const fxPriceTimestamp = fx.ok ? fx.fxPriceTimestamp : null;

    return JSON.stringify(withNotices({
      ticker,
      status: "OK",
      currency,
      lastVolume,
      lastClose: +lastPrice.toFixed(4),
      priceBasis: "UNADJUSTED_CLOSE",
      adv10d,
      adv20d,
      adv90d,
      ratio20d,
      fxRate,
      fxCurrency: fx.ok ? fx.fxCurrency : null,
      fxPriceTimestamp,
      notionalUsd,
      adv20dTradedValueUsd,
      gateBasis: "ADV20_TRADED_VALUE_USD",
      meetsMinimum: meetsMinimum(adv20dTradedValueUsd, minAdvUsd),
      minAdvUsd,
      observationType: "COMPLETED_DAILY_VOLUME_NOTIONAL",
      barStatus: "COMPLETE",
      freshnessStatus: prepared.freshnessStatus,
      expectedCompletedDate: prepared.expectedCompletedDate,
      latestAvailableBarDate: dataDate,
      excludedIncompleteBar: prepared.excludedIncompleteBar,
      retryAttempted,
      dataDate,
      note,
      recommendedNextAction,
    }, freshnessNotices(ticker, prepared.freshnessReason)));
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}`, ticker });
  }
}

const SNAPSHOT_TICKER_NOT_FOUND = "TICKER_NOT_FOUND";
const SNAPSHOT_NOT_FOUND_MESSAGE = "No provider data was found for the requested ticker.";

/** A component failure's message without provider URLs, as a caller can read it (2.26.5, F56). */
export function snapshotComponentMessage(message: string): string {
  const cleaned = message.slice(0, 300).replace(/https?:\/\/\S+/gi, " ").replace(/\s+/g, " ").trim().replace(/[\s:]+$/, "");
  return cleaned || "error in response";
}

/** Whether a market-snapshot quote failure says the ticker does not exist (2.26.5, F56). */
export function snapshotTickerNotFound(message: string): boolean {
  return /not found|no data found|no price data found|api error 404|ticker_not_found/i.test(message);
}

export async function getMarketSnapshot(
  ticker: string | string[],
  mode: "compact" | "full",
  foreignExchange: boolean,
  minAdvUsd: number | null = null,
): Promise<string> {
  if (Array.isArray(ticker)) {
    // The per-mode caps (tickerCaps get_market_snapshot / get_market_snapshot.full) are enforced by the tool-call gate.
    const snapshots = await mapWithConcurrency(ticker, BATCH_TICKER_CONCURRENCY, async (t) => {
      try {
        return JSON.parse(await getMarketSnapshot(t, mode, foreignExchange, minAdvUsd)) as unknown;
      } catch (e) {
        const message = e instanceof Error ? e.message : String(e);
        // An unknown ticker is TICKER_NOT_FOUND, not retried, as in get_market_quote (2.26.5, F56).
        if (message.startsWith(SNAPSHOT_TICKER_NOT_FOUND)) {
          return { error: true, code: "TICKER_NOT_FOUND", message: SNAPSHOT_NOT_FOUND_MESSAGE, retryable: false };
        }
        return { error: true, message };
      }
    });
    const results: Record<string, unknown> = Object.fromEntries(ticker.map((t, i) => [t, snapshots[i]]));
    return JSON.stringify({ tickers: results });
  }

  const retrievedAt = new Date().toISOString();

  const [quoteResult, priceStatsResult, maResult, volumeRatioResult, volumeGateResult, techResult] =
    await Promise.allSettled([
      // The quote's own error text is kept (a JSON syntax error would hide it), so an unknown ticker is seen (F56).
      getFastInfo(ticker).then((r) => chainedJson<Record<string, unknown>>(r)),
      getPriceStats(ticker).then((r) => chainedJson<Record<string, unknown>>(r)),
      getMaPosition(ticker).then((r) => chainedJson<Record<string, unknown>>(r)),
      getVolumeRatio(ticker, 10).then((r) => chainedJson<Record<string, unknown>>(r)),
      getVolumeGate(ticker, foreignExchange, minAdvUsd).then((r) => chainedJson<Record<string, unknown>>(r)),
      getTechnicalIndicators(ticker, INDICATOR_MIN_LOOKBACK).then((r) => chainedJson<Record<string, unknown>>(r)),
    ]);

  // An unknown ticker fails as get_market_quote does, TICKER_NOT_FOUND and not retried, instead of a PARTIAL
  // snapshot that says RETRY (2.26.5, F56). The tool layer turns the thrown code into the failure envelope.
  const quoteFailure = quoteResult.status === "rejected"
    ? String(quoteResult.reason instanceof Error ? quoteResult.reason.message : quoteResult.reason)
    : quoteResult.value?.error
      ? String((quoteResult.value as Record<string, unknown>).message ?? "")
      : null;
  if (quoteFailure != null && snapshotTickerNotFound(quoteFailure)) {
    throw new Error(`${SNAPSHOT_TICKER_NOT_FOUND}: no data found for ticker ${ticker}`);
  }

  const componentStatus: Record<string, string> = {};
  const failedComponents: string[] = [];
  const limitedComponents: string[] = [];
  const warnings: Record<string, unknown>[] = [];

  const getVal = (
    result: PromiseSettledResult<Record<string, unknown>>,
    name: string
  ): Record<string, unknown> | null => {
    if (result.status === "fulfilled" && !result.value?.error) {
      const payloadStatus = typeof result.value?.status === "string"
        ? result.value.status
        : "OK";
      componentStatus[name] = payloadStatus;
      if (payloadStatus === "STALE_BAR" || payloadStatus === "PARTIAL") {
        limitedComponents.push(name);
        warnings.push({
          code: payloadStatus,
          component: name,
          recommendedNextAction: result.value.recommendedNextAction ?? "RETRY",
        });
      }
      // A component's notices join the snapshot's data.warnings with the component name (3.1.0).
      for (const notice of takeNotices(result.value)) {
        warnings.push({ code: notice.code, component: name, message: notice.message });
      }
      return result.value;
    }
    componentStatus[name] = "FAILED";
    failedComponents.push(name);
    const msg =
      result.status === "rejected"
        ? String(result.reason instanceof Error ? result.reason.message : result.reason)
        : String((result.value as Record<string, unknown>)?.message ?? "error in response");
    warnings.push({ code: "COMPONENT_FAILED", component: name, message: snapshotComponentMessage(msg) });
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
  const completedBarDataDate =
    (tech?.dataDate as string | null) ??
    (priceStats?.dataDate as string | null) ??
    (volumeGate?.dataDate as string | null) ??
    null;
  const quoteTimestamp = (quote?.priceTimestamp as string | null) ?? null;
  const marketOpen = (quote?.marketOpen as boolean | null) ?? null;
  const quoteFreshnessClass = classifyQuoteFreshness(
    quoteTimestamp,
    retrievedAt,
    marketOpen,
  );

  const lastPrice = (quote?.lastPrice as number | null) ?? null;
  const prevClose = (quote?.previousClose as number | null) ?? null;
  const changePct =
    (priceStats?.pctChangeTodayVsPrevClose as number | null) ??
    (lastPrice != null && prevClose != null && prevClose !== 0
      ? roundHalfAwayFromZero((lastPrice - prevClose) / prevClose * 100, 2)
      : null);

  const snapshot: Record<string, unknown> = {
    ticker,
    price: {
      last: lastPrice,
      previousClose: prevClose,
      changePct,
      lastTradeDate: dataDate,
      marketOpen,
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
      lastVolumeObservationType: "REGULAR_SESSION_CUMULATIVE_VOLUME",
      lastCompletedVolume: (volumeRatio?.lastVolume as number | null) ?? null,
      completedVolumeDataDate: (volumeRatio?.dataDate as string | null) ?? null,
      avgVolume10d:
        (volumeRatio?.avgVolume10d as number | null)
        ?? (quote?.tenDayAverageVolume as number | null)
        ?? null,
      avgVolume20d: (volumeGate?.adv20d as number | null) ?? null,
      avgVolume90d:
        (volumeRatio?.avgVolume90d as number | null)
        ?? (quote?.threeMonthAverageVolume as number | null)
        ?? null,
      ratio10d: (volumeRatio?.ratio10d as number | null) ?? null,
      ratio20d: (volumeGate?.ratio20d as number | null) ?? null,
      ratio90d: (volumeRatio?.ratio90d as number | null) ?? null,
      volumeFlag: (volumeRatio?.volumeFlag as string | null) ?? null,
      meetsMinimum: meetsMinimum(volumeGate?.adv20dTradedValueUsd as number | null | undefined, minAdvUsd),
      minAdvUsd,
    },
    risk: {
      annualizedVolatility30d: (priceStats?.annualizedVolatility30d as number | null) ?? null,
    },
    freshness: {
      dataDate,
      quoteDataDate: (quote?.lastTradeDate as string | null) ?? null,
      quoteTimestamp,
      completedBarDataDate,
      componentDataDates: {
        priceStats: (priceStats?.dataDate as string | null) ?? null,
        maPosition: (ma?.dataDate as string | null) ?? null,
        volumeRatio: (volumeRatio?.dataDate as string | null) ?? null,
        volumeGate: (volumeGate?.dataDate as string | null) ?? null,
        technicalIndicators: (tech?.dataDate as string | null) ?? null,
      },
      retrievedAt,
      marketSessionAware: true,
      freshnessClass: quoteFreshnessClass,
      quoteFreshnessClass,
      completedBarFreshnessStatus:
        (tech?.freshnessStatus as string | null)
        ?? (priceStats?.freshnessStatus as string | null)
        ?? null,
    },
    status: failedComponents.length > 0 || limitedComponents.length > 0 ? "PARTIAL" : "OK",
    componentStatus,
    partialSuccess:
      (failedComponents.length > 0 || limitedComponents.length > 0)
      && failedComponents.length < 6,
    failedComponents,
    limitedComponents,
    warnings,
    recommendedNextAction:
      failedComponents.length > 0 || limitedComponents.length > 0 ? "RETRY" : "NONE",
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

/** Today's date on the US options exchanges (America/New_York). */
export function usMarketDate(now: Date = new Date()): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" }).format(now);
}

/**
 * The first expiry after today's US market date. A chain expiring today is
 * mostly 0DTE contracts that close within hours (and quote zero bids before
 * the open), so it does not describe positioning.
 */
export function nextOptionExpiry(dates: string[], today: string): string | null {
  return dates.find((d) => d > today) ?? dates[dates.length - 1] ?? null;
}

/** A chained tool's JSON; its plain-text error (e.g. an HTTP 429) is raised as-is, not as a JSON syntax error. */
function chainedJson<T>(raw: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new Error(raw.slice(0, 300));
  }
}

export async function getOptionsSummary(ticker: string, expiryHint?: string): Promise<string> {
  try {
    const expData = chainedJson<string[]>(await getOptionExpirationDates(ticker));
    if (!expData || expData.length === 0) {
      // Return the legacy failure shape here so callTool's mcpSuccess wrapper
      // can retain its canonical/alias metadata while converting it to V2
      // `ok: false`.  Do not return `{ ticker, error: string }`: that shape
      // is indistinguishable from successful tool data and becomes `ok: true`.
      return JSON.stringify({
        error: true,
        code: ErrorCode.NO_OPTIONS_DATA,
        message: `No options data available for ${ticker.toUpperCase()}`,
        ticker: ticker.toUpperCase(),
      });
    }
    const today = usMarketDate();
    const expiry = expiryHint || (nextOptionExpiry(expData, today) as string);
    const skippedExpiries = expiryHint ? [] : expData.filter((d) => d <= today && d !== expiry);
    if (!expData.includes(expiry)) {
      return JSON.stringify(invalidExpiryPayload(ticker, expiry, expData));
    }
    // Fetch all contracts without illiquid filtering and with strike sort so we get the full chain
    const callsRaw = chainedJson<Record<string, unknown>>(await getOptionChain(ticker, expiry, "calls", 200, 0, 0, null, null, "all", "strike", 20, true));
    const putsRaw = chainedJson<Record<string, unknown>>(await getOptionChain(ticker, expiry, "puts", 200, 0, 0, null, null, "all", "strike", 20, true));
    const calls = ((callsRaw.contracts ?? []) as Record<string, unknown>[]).map(normalizeContractIv);
    const puts = ((putsRaw.contracts ?? []) as Record<string, unknown>[]).map(normalizeContractIv);

    const fi = chainedJson<Record<string, unknown>>(await getFastInfo(ticker));
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
      maxPainStrike = computeMaxPainStrike(calls, puts);
    }

    return JSON.stringify({
      ticker, nearestExpiry: expiry, expirySelection: expiryHint ? "REQUESTED" : "NEXT_AFTER_TODAY", skippedExpiries, currentPrice,
      atmIV, pcRatioVolume, pcRatioOI,
      callVolume: callVol, putVolume: putVol, callOI, putOI,
      maxPainStrike, dataDate,
      dataQuality,
      warnings: summaryWarnings,
    });
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    return JSON.stringify({
      error: true,
      // Without a code the envelope classifies the message (429 -> retryable RATE_LIMIT).
      ...(classifyErrorMessage(message) ? {} : { code: ErrorCode.PROVIDER_ERROR }),
      message,
      ticker: ticker.toUpperCase(),
    });
  }
}
