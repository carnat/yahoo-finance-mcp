/**
 * Yahoo Finance client (Phase 4 unit 2, moved out of yahoo-finance.ts): the session crumb, the throttling retry on
 * Yahoo's other API host, the shared GET body cache and its edge cache, yGet / yPost, the {raw, fmt} unwrapping,
 * the fundamentals timeseries read and the valuation market inputs.
 */

import { BoundedTtlCache } from "../cache.js";
import { awaitNotingBudget, consumeSubrequest, countCacheEvent, countProviderRetry, emptyCacheUsage, type CacheEvent, type CacheUsage } from "../request-context.js";
import { marketInputsFromQuoteSummary, type MarketInputs } from "../valuation.js";
import { edgeCache, fetchProviderWithTimeout, UA } from "./provider-http.js";
import { yahooQuoteSummaryUrl, yahooTimeseriesUrl } from "./endpoints.js";

// Module-level crumb cache — shared within a Cloudflare isolate session
let _crumb: { value: string; cookie: string; exp: number } | null = null;
// In-flight refresh shared by concurrent callers so a cold isolate performs
// one fc.yahoo.com + getcrumb round trip instead of one per parallel request.
let _crumbRefresh: Promise<{ value: string; cookie: string; exp: number }> | null = null;

const YAHOO_FETCH_TIMEOUT_MS = 15_000;

// Yahoo throttling (HTTP 429) is handled the way yfinance handles it: a
// rate-limited crumb request does not fail the call (many endpoints answer
// without a crumb), and a rate-limited data request is retried once, after a
// short pause, on Yahoo's other API host.
const YAHOO_API_HOSTS = ["query2.finance.yahoo.com", "query1.finance.yahoo.com"] as const;
const YAHOO_RATE_LIMIT_RETRY_DELAY_MS = 750;
// After a rate-limited crumb request, skip crumb requests for a minute so a
// throttled isolate does not add a cookie + crumb round trip to every call.
const CRUMB_RATE_LIMIT_COOLDOWN_MS = 60_000;
let _crumbUnavailableUntil = 0;

/** Yahoo refused the crumb request with HTTP 429. */
class YahooCrumbRateLimitError extends Error {
  readonly status = 429;
  constructor() {
    super("Yahoo Finance crumb request rate limited (HTTP 429)");
  }
}

/** The same URL on Yahoo's other API host, or null for non-API URLs. */
function alternateYahooHost(url: string): string | null {
  const [primary, secondary] = YAHOO_API_HOSTS;
  if (url.includes(`//${primary}/`)) return url.replace(`//${primary}/`, `//${secondary}/`);
  if (url.includes(`//${secondary}/`)) return url.replace(`//${secondary}/`, `//${primary}/`);
  return null;
}

function pause(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function refreshCrumb(): Promise<{ value: string; cookie: string }> {
  // fc.yahoo.com sets the consent cookie used by Yahoo Finance APIs
  const init = await fetchProviderWithTimeout("https://fc.yahoo.com", {
    headers: { "User-Agent": UA },
  }, YAHOO_FETCH_TIMEOUT_MS);

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

  // Like yfinance's two cookie strategies, fall back to the other API host
  // when the first refuses the crumb request.
  let lastStatus = 0;
  for (const [attempt, host] of YAHOO_API_HOSTS.entries()) {
    if (attempt > 0) countProviderRetry();
    const crumbRes = await fetchProviderWithTimeout(`https://${host}/v1/test/getcrumb`, {
      headers: { "User-Agent": UA, Cookie: cookie },
    }, YAHOO_FETCH_TIMEOUT_MS);
    if (crumbRes.ok) return { value: await crumbRes.text(), cookie };
    await crumbRes.body?.cancel();
    lastStatus = crumbRes.status;
  }
  if (lastStatus === 429) throw new YahooCrumbRateLimitError();
  throw new Error(`Crumb fetch failed: ${lastStatus}`);
}

/**
 * A crumb, or undefined while Yahoo is rate-limiting crumb requests; the
 * caller then requests the endpoint without one.
 */
async function getCrumbIfAvailable(): Promise<{ value: string; cookie: string } | undefined> {
  if (Date.now() < _crumbUnavailableUntil) return undefined;
  try {
    return await getCrumb();
  } catch (error) {
    if (error instanceof YahooCrumbRateLimitError) {
      _crumbUnavailableUntil = Date.now() + CRUMB_RATE_LIMIT_COOLDOWN_MS;
      return undefined;
    }
    throw error;
  }
}

export async function getCrumb(): Promise<{ value: string; cookie: string }> {
  if (_crumb && Date.now() < _crumb.exp) return _crumb;
  _crumbRefresh ??= refreshCrumb()
    .then(({ value, cookie }) => {
      _crumb = { value, cookie, exp: Date.now() + 3_600_000 }; // 1-hour cache
      return _crumb;
    })
    .finally(() => {
      _crumbRefresh = null;
    });
  return awaitNotingBudget(_crumbRefresh);
}

/** Drop the cached crumb after a 401, unless a concurrent caller already replaced it. */
export function invalidateCrumb(rejected: { value: string }): void {
  if (_crumb?.value === rejected.value) _crumb = null;
}

/** Error thrown for a non-OK Yahoo response; `status` drives failure classification. */
class YahooHttpError extends Error {
  constructor(readonly status: number, url: string) {
    super(`Yahoo Finance API error ${status} for: ${url}`);
  }
}

// Composite tools (market snapshot, position signals) request the same Yahoo
// URLs from several components in parallel. Identical GETs share one
// in-flight request and a short-lived body cache; each caller parses its own
// copy so callers can never mutate each other's payloads. Retry URLs carry a
// unique cache-buster, so they always reach Yahoo.
const YAHOO_GET_TTL_MS = 30_000;
// Bodies range from a few KB to over 1 MB (long daily histories), so the
// cache is limited by total characters as well as entries.
const YAHOO_GET_CACHE_MAX_CHARS = 24_000_000;
const yahooGetBodies = new BoundedTtlCache<string>(200, YAHOO_GET_CACHE_MAX_CHARS, (body) => body.length);
const yahooGetInflight = new Map<string, Promise<string>>();

// Per-isolate counters for the Yahoo GET caches, reported on /health.
const yahooCacheCounters: CacheUsage = emptyCacheUsage();
// The Workers clock reads 0 while a module is first evaluated, so the
// counters' start time is taken from the first request instead.
export let countersSince: string | null = null;

export function markYahooCacheActivity(): void {
  countersSince ??= new Date().toISOString();
}

function countYahooCache(event: CacheEvent): void {
  yahooCacheCounters[event]++;
  countCacheEvent("yahoo", event);
}

export function yahooCacheStats(): Record<string, unknown> {
  return {
    scope: "isolate",
    since: countersSince,
    ...yahooCacheCounters,
    bodyCacheEntries: yahooGetBodies.size,
    bodyCacheChars: yahooGetBodies.weight,
  };
}

async function yGetText(url: string, auth: boolean): Promise<string> {
  const makeReq = async (target: string, c?: { value: string; cookie: string }): Promise<Response> => {
    const headers: Record<string, string> = { "User-Agent": UA };
    let u = target;
    if (c) {
      headers.Cookie = c.cookie;
      u += `${u.includes("?") ? "&" : "?"}crumb=${encodeURIComponent(c.value)}`;
    }
    return fetchProviderWithTimeout(u, { headers }, YAHOO_FETCH_TIMEOUT_MS);
  };

  let crumb = auth ? await getCrumbIfAvailable() : undefined;
  const crumbThrottled = auth && crumb === undefined;
  let res = await makeReq(url, crumb);

  // Retry once with a fresh crumb on 401
  if (res.status === 401 && crumb) {
    // Cancel the unconsumed response body to free the HTTP slot
    await res.body?.cancel();
    invalidateCrumb(crumb);
    crumb = await getCrumbIfAvailable();
    countProviderRetry();
    res = await makeReq(url, crumb);
  }

  // Retry a throttled request once on the other API host.
  const alternate = res.status === 429 ? alternateYahooHost(url) : null;
  if (alternate) {
    await res.body?.cancel();
    await pause(YAHOO_RATE_LIMIT_RETRY_DELAY_MS);
    countProviderRetry();
    res = await makeReq(alternate, crumb);
  }

  if (!res.ok) {
    await res.body?.cancel();
    // An endpoint that needs a crumb refuses the crumb-less request; the
    // cause is Yahoo's rate limit on crumbs, so report it as a rate limit.
    const status = crumbThrottled && (res.status === 401 || res.status === 403) ? 429 : res.status;
    throw new YahooHttpError(status, url);
  }
  return res.text();
}

// Slow-changing Yahoo data is also kept in the colo's edge cache, so fresh
// isolates reuse it instead of calling Yahoo again. Only endpoints and
// quoteSummary modules listed here qualify; anything that carries a live
// price or quote stays on the 30-second in-process cache above.
const YAHOO_EDGE_CACHE_VERSION = "v1";
const HOUR_MS = 3_600_000;
const YAHOO_TIMESERIES_EDGE_TTL_MS = 6 * HOUR_MS;
const YAHOO_QUOTE_SUMMARY_EDGE_TTL_MS: Record<string, number> = {
  assetProfile: 6 * HOUR_MS,
  summaryProfile: 6 * HOUR_MS,
  fundProfile: 6 * HOUR_MS,
  topHoldings: 6 * HOUR_MS,
  majorHoldersBreakdown: 6 * HOUR_MS,
  institutionOwnership: 6 * HOUR_MS,
  fundOwnership: 6 * HOUR_MS,
  insiderHolders: 6 * HOUR_MS,
  insiderTransactions: 6 * HOUR_MS,
  netSharePurchaseActivity: 6 * HOUR_MS,
  recommendationTrend: HOUR_MS,
  upgradeDowngradeHistory: HOUR_MS,
  earningsTrend: HOUR_MS,
  earningsHistory: HOUR_MS,
  calendarEvents: HOUR_MS,
  secFilings: HOUR_MS,
};

/**
 * The cache identity of a Yahoo GET. Timeseries requests end their window at
 * the current second (`period2`), which would give every call a new key, so
 * the window end is dropped from the key.
 */
function yahooCacheUrl(url: string): string {
  if (!url.includes("/ws/fundamentals-timeseries/")) return url;
  return url.replace(/([?&])period2=\d+(&|$)/, (_m, lead: string, tail: string) => (tail ? lead : ""));
}

/** Edge-cache lifetime for a Yahoo GET, or 0 when it must not be edge cached. */
export function yahooEdgeTtlMs(url: string): number {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return 0;
  }
  if (parsed.pathname.includes("/ws/fundamentals-timeseries/")) return YAHOO_TIMESERIES_EDGE_TTL_MS;
  if (!parsed.pathname.includes("/v10/finance/quoteSummary/")) return 0;
  const modules = (parsed.searchParams.get("modules") ?? "").split(",").filter(Boolean);
  if (modules.length === 0) return 0;
  let ttl = Infinity;
  for (const module of modules) {
    const moduleTtl = YAHOO_QUOTE_SUMMARY_EDGE_TTL_MS[module];
    if (!moduleTtl) return 0;
    ttl = Math.min(ttl, moduleTtl);
  }
  return ttl;
}

/** Only a complete, error-free Yahoo answer is shared through the edge cache. */
function isEdgeCacheableYahooBody(url: string, text: string): boolean {
  let body: Record<string, unknown>;
  try {
    body = JSON.parse(text) as Record<string, unknown>;
  } catch {
    return false;
  }
  if (url.includes("/ws/fundamentals-timeseries/")) {
    const series = body?.timeseries as { result?: unknown; error?: unknown } | undefined;
    const results = Array.isArray(series?.result) ? series.result as Record<string, unknown>[] : [];
    return series?.error == null && results.some((item) =>
      item != null && typeof item === "object" && Object.keys(item).some((key) => key !== "meta" && key !== "timestamp")
    );
  }
  const summary = body?.quoteSummary as { result?: unknown; error?: unknown } | undefined;
  const first = Array.isArray(summary?.result) ? summary.result[0] : undefined;
  return summary?.error == null && first != null && typeof first === "object" && Object.keys(first).length > 0;
}

function yahooEdgeRequest(cacheUrl: string): Request {
  return new Request(`https://yahoo-cache.invalid/${YAHOO_EDGE_CACHE_VERSION}/${encodeURIComponent(cacheUrl)}`);
}

async function readYahooEdge(cacheUrl: string, ttlMs: number): Promise<string | null> {
  const cache = edgeCache();
  if (!cache || !consumeSubrequest("cache")) return null;
  try {
    const response = await cache.match(yahooEdgeRequest(cacheUrl));
    if (!response) return null;
    const storedAt = Number(response.headers.get("X-Stored-At"));
    if (!Number.isFinite(storedAt) || Date.now() - storedAt >= ttlMs) {
      await response.body?.cancel();
      return null;
    }
    return await response.text();
  } catch {
    return null;
  }
}

async function writeYahooEdge(cacheUrl: string, text: string, ttlMs: number): Promise<void> {
  const cache = edgeCache();
  if (!cache || !consumeSubrequest("cache")) return;
  try {
    await cache.put(
      yahooEdgeRequest(cacheUrl),
      new Response(text, {
        headers: {
          "Content-Type": "application/json",
          "Cache-Control": `public, max-age=${Math.max(1, Math.floor(ttlMs / 1000))}`,
          "X-Stored-At": String(Date.now()),
        },
      }),
    );
  } catch {
    // Edge cache is best effort; the Yahoo response remains usable.
  }
}

async function fetchYahooBody(url: string, auth: boolean, cacheUrl: string): Promise<string> {
  const edgeTtl = yahooEdgeTtlMs(url);
  if (edgeTtl > 0) {
    const cached = await readYahooEdge(cacheUrl, edgeTtl);
    if (cached !== null) {
      countYahooCache("edgeHits");
      return cached;
    }
    countYahooCache("edgeMisses");
  }
  countYahooCache("upstreamFetches");
  const text = await yGetText(url, auth);
  if (edgeTtl > 0 && isEdgeCacheableYahooBody(url, text)) {
    await writeYahooEdge(cacheUrl, text, edgeTtl);
    countYahooCache("edgeWrites");
  }
  return text;
}

function yahooGetKey(url: string, auth: boolean): string {
  return `${auth ? "auth" : "anon"}:${yahooCacheUrl(url)}`;
}

/** Drop a cached Yahoo GET body so the next yGet for this URL refetches it. */
export function evictYahooGet(url: string, auth = true): void {
  yahooGetBodies.delete(yahooGetKey(url, auth));
}

export async function yGet(url: string, auth = true): Promise<unknown> {
  const key = yahooGetKey(url, auth);
  let body = yahooGetBodies.get(key);
  if (body !== undefined) {
    countYahooCache("memoryHits");
  } else {
    let pending = yahooGetInflight.get(key);
    if (pending) {
      countYahooCache("sharedInflight");
    } else {
      pending = fetchYahooBody(url, auth, yahooCacheUrl(url))
        .then((text) => {
          yahooGetBodies.set(key, text, YAHOO_GET_TTL_MS);
          return text;
        })
        .finally(() => {
          yahooGetInflight.delete(key);
        });
      yahooGetInflight.set(key, pending);
    }
    body = await awaitNotingBudget(pending);
  }
  try {
    return JSON.parse(body);
  } catch (error) {
    yahooGetBodies.delete(key);
    throw error;
  }
}

export async function yPost(url: string, body: Record<string, unknown>): Promise<unknown> {
  const makeReq = async (c: { value: string; cookie: string }): Promise<Response> => {
    const u = `${url}${url.includes("?") ? "&" : "?"}crumb=${encodeURIComponent(c.value)}`;
    return fetchProviderWithTimeout(u, {
      method: "POST",
      headers: {
        "User-Agent": UA,
        Cookie: c.cookie,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    }, YAHOO_FETCH_TIMEOUT_MS);
  };
  const crumb = await getCrumb();
  let res = await makeReq(crumb);
  if (res.status === 401) {
    await res.body?.cancel();
    invalidateCrumb(crumb);
    const fresh = await getCrumb();
    countProviderRetry();
    res = await makeReq(fresh);
  }
  if (!res.ok) {
    await res.body?.cancel();
    throw new YahooHttpError(res.status, url);
  }
  return res.json();
}

// Extract the `raw` numeric value from Yahoo Finance's {raw, fmt} wrapper objects.
// Empty objects {} are treated as missing values and normalized to null.
export function raw(v: unknown): unknown {
  if (v !== null && v !== undefined && typeof v === "object") {
    if ("raw" in (v as object)) {
      return (v as { raw: unknown }).raw;
    }
    // Yahoo Finance sometimes returns {} for fields that have no value — treat as null.
    if (Object.keys(v as object).length === 0) return null;
  }
  return v ?? null;
}

export const enc = encodeURIComponent;
export const iso = (ts: number) => new Date(ts * 1000).toISOString();
export const noData = (t: string) => `Error: no data found for ticker ${t}`;

/**
 * Yahoo fundamentals timeseries as one row per period, newest first:
 * [{date: "2026-06-30", totalDebt: 1, ...}]. Keys are the type names without
 * the prefix, first letter lower-cased ("quarterlyTotalDebt" -> "totalDebt").
 * Null when Yahoo returns no series at all.
 */
export async function fetchTimeseriesByDate(
  ticker: string,
  prefix: string,
  baseTypes: string[],
): Promise<Record<string, unknown>[] | null> {
  const types = baseTypes.map((t) => `${prefix}${t}`);
  // period1: 1985-08-20 (yfinance default); period2: now
  const p2 = Math.floor(Date.now() / 1000);
  const d = (await yGet(
    yahooTimeseriesUrl(enc(ticker)) +
      `?type=${types.join(",")}&period1=493590046&period2=${p2}` +
      `&lang=en-US&region=US&corsDomain=finance.yahoo.com`
  )) as Record<string, unknown>;

  const results =
    ((d?.timeseries as Record<string, unknown> | undefined)?.result as Record<
      string,
      unknown
    >[]) ?? [];

  if (!results.length) return null;

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

  return Object.keys(byDate)
    .sort((a, b) => b.localeCompare(a))
    .map((date) => byDate[date]);
}

export async function valuationMarketInputs(ticker: string): Promise<MarketInputs> {
  const d = (await yGet(
    yahooQuoteSummaryUrl(enc(ticker), "price,financialData,defaultKeyStatistics,earningsTrend")
  )) as Record<string, unknown>;
  const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as Record<string, unknown> | undefined;
  if (!result) throw new Error(`No Yahoo quote summary for ${ticker.toUpperCase()}`);
  return marketInputsFromQuoteSummary(ticker, result);
}
