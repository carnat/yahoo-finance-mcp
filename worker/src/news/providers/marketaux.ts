/**
 * Marketaux provider: the API fetch (fetchMarketauxJson) and collectMarketauxEvents.
 * Moved out of news.ts in 2.15.1 (pure restructure); providers never import news.ts.
 */

import { getWorkerVar } from "../../response.js";
import {
  UA,
  type ProviderJsonResult,
  providerCacheKey,
  getProviderCache,
  setProviderCache,
  providerHttpStatus,
  fetchProviderWithTimeout,
  _str,
  toIsoUtc,
} from "../../yahoo-finance.js";
import { shortText, eventTypeFromKeywords, makeDupGroupId, withinDateWindow, normalizeExchange, withoutZeroMillis } from "../shared.js";
import type { NewsProviderResultWithDiagnostics } from "./types.js";

const MARKETAUX_NEWS_API = "https://api.marketaux.com/v1/news/all";
export const MARKETAUX_WIRE_DOMAINS = ["businesswire.com", "prnewswire.com", "globenewswire.com"] as const;
export const MARKETAUX_MAX_ITEMS = 3;
const MARKETAUX_NEWS_TTL_MS = 15 * 60 * 1000;

async function fetchMarketauxJson(
  params: Record<string, unknown>,
  ttlMs = MARKETAUX_NEWS_TTL_MS,
): Promise<ProviderJsonResult> {
  const cacheKey = providerCacheKey("marketaux", "/v1/news/all", params);
  const cached = await getProviderCache(cacheKey, ttlMs);
  if (cached) return cached;
  const token = getWorkerVar("MARKETAUX_API_TOKEN") ?? getWorkerVar("MARKETAUX_API_KEY");
  const publicQuery = new URLSearchParams(
    Object.entries({ ...params, api_token: "REDACTED" }).map(([key, value]): [string, string] => [key, String(value)])
  );
  const publicUrl = `${MARKETAUX_NEWS_API}?${publicQuery.toString()}`;
  if (!token) {
    return { payload: null, status: "SOURCE_UNCONFIGURED", publicUrl, providerAttempted: false, cacheStatus: "MISS" };
  }
  const requestQuery = new URLSearchParams(publicQuery);
  requestQuery.set("api_token", token);
  try {
    const response = await fetchProviderWithTimeout(
      `${MARKETAUX_NEWS_API}?${requestQuery.toString()}`,
      { headers: { "User-Agent": UA } },
      12_000,
    );
    const retryAfter = response.headers.get("Retry-After") ?? undefined;
    if (!response.ok) {
      const body = (await response.text()).replaceAll(token, "REDACTED");
      const status = response.status === 402 || response.status === 429
        ? "RATE_LIMIT"
        : response.status === 401 || response.status === 403
          ? "AUTH_ERROR"
          // R-news D (2.22.0, F13a): a Marketaux 404 is a provider failure, not evidence of absence (Python's mapping);
          // the shared providerHttpStatus keeps NOT_FOUND for the other providers.
          : response.status === 404
            ? "PROVIDER_ERROR"
            : providerHttpStatus(response.status, body);
      return {
        payload: null,
        status,
        publicUrl,
        providerAttempted: true,
        cacheStatus: "MISS",
        httpStatus: response.status,
        message: body.slice(0, 500) || undefined,
        retryAfter,
      };
    }
    const rawJson = await response.json();
    if (!rawJson || typeof rawJson !== "object" || Array.isArray(rawJson)) {
      // R-news D (2.22.0, F13b): a non-object body is a changed contract, as in Python and the Finnhub non-array rule.
      return { payload: null, status: "PROVIDER_CHANGED", publicUrl, providerAttempted: true, cacheStatus: "MISS", message: "Marketaux returned a non-object payload." };
    }
    const payload = rawJson as Record<string, unknown>;
    const fetchedAt = new Date().toISOString();
    await setProviderCache(cacheKey, payload, publicUrl, fetchedAt, ttlMs);
    return { payload, status: "OK", publicUrl, providerAttempted: true, cacheStatus: "MISS", fetchedAt };
  } catch (error) {
    const message = error instanceof Error ? error.message.replaceAll(token, "REDACTED") : String(error);
    const status = /timeout|timed out|abort/i.test(message) ? "TIMEOUT" : "PROVIDER_ERROR";
    return { payload: null, status, publicUrl, providerAttempted: true, cacheStatus: "MISS", message };
  }
}
export async function collectMarketauxEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number,
  fallbackReason = "EXPLICIT_SOURCE_SELECTION",
  searchQuery = "",
  expectedExchange: string | null = null,
): Promise<NewsProviderResultWithDiagnostics> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const limit = Math.min(MARKETAUX_MAX_ITEMS, Math.max(1, maxResults));
  const rejectionCounts: Record<string, number> = {};
  const diagnostics: Record<string, unknown> = {
    status: "PROVIDER_ERROR",
    rawCount: 0,
    retrievedCount: 0,
    acceptedCount: 0,
    collectedCount: 0,
    rejectedCount: 0,
    rejectionCounts,
    attempted: true,
    completed: false,
    fallbackReason,
    domains: [...MARKETAUX_WIRE_DOMAINS],
    quotaPolicy: {
      dailyLimit: 100,
      maxArticlesPerRequest: MARKETAUX_MAX_ITEMS,
      pagesRequested: 1,
      automaticPagination: false,
    },
  };
  const reject = (reason: string): void => {
    rejectionCounts[reason] = (rejectionCounts[reason] ?? 0) + 1;
    diagnostics.rejectedCount = Number(diagnostics.rejectedCount ?? 0) + 1;
  };
  const now = new Date();
  const fromDay = startDate || new Date(now.getTime() - (lookbackDays ?? 14) * 86400000).toISOString().slice(0, 10);
  const toDay = endDate || now.toISOString().slice(0, 10);
  const providerParams: Record<string, unknown> = {
    symbols: ticker.toUpperCase(),
    filter_entities: true,
    language: "en",
    domains: MARKETAUX_WIRE_DOMAINS.join(","),
    published_after: `${fromDay}T00:00:00`,
    published_before: `${toDay}T23:59:59`,
    limit,
    page: 1,
  };
  if (searchQuery.trim()) providerParams.search = searchQuery.trim();
  const provider = await fetchMarketauxJson(providerParams);
  diagnostics.providerUrl = provider.publicUrl;
  diagnostics.cacheStatus = provider.cacheStatus;
  diagnostics.attempted = provider.providerAttempted;
  if (provider.status !== "OK" || !provider.payload) {
    const status = provider.status === "SOURCE_UNCONFIGURED"
      ? "UNCONFIGURED"
      : provider.status === "RATE_LIMIT"
        ? "RATE_LIMITED"
        : provider.status;
    diagnostics.status = status;
    // R-news Batch C (decision 12): Python's templates, `returned HTTP <n> (<STATUS>).` when the provider answered with an HTTP
    // status, `unavailable (<STATUS>).` otherwise.
    const message = status === "UNCONFIGURED"
      ? "Marketaux wire fallback is not configured; set MARKETAUX_API_TOKEN to enable it."
      : typeof provider.httpStatus === "number"
        ? `Marketaux wire fallback returned HTTP ${provider.httpStatus} (${status}).`
        : `Marketaux wire fallback unavailable (${status}).`;
    warnings.push({ code: "SOURCE_UNAVAILABLE", message, severity: "warning", source: "marketaux" });
    return { items, warnings, used: false, diagnostics };
  }
  const rawItems = provider.payload.data;
  if (!Array.isArray(rawItems)) {
    diagnostics.status = "PROVIDER_CHANGED";
    warnings.push({ code: "SOURCE_UNAVAILABLE", message: "Marketaux provider changed: expected a data array.", severity: "warning", source: "marketaux" });
    return { items, warnings, used: false, diagnostics };
  }
  diagnostics.rawCount = rawItems.length;
  diagnostics.retrievedCount = rawItems.length;
  const tickerU = ticker.toUpperCase();
  for (const candidate of rawItems) {
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
      reject("INVALID_ROW");
      continue;
    }
    const row = candidate as Record<string, unknown>;
    const title = _str(row.title).trim();
    const url = _str(row.url).trim();
    if (!title) {
      reject("MISSING_HEADLINE");
      continue;
    }
    let host = "";
    try {
      const parsed = new URL(url);
      if (parsed.protocol !== "https:") throw new Error("invalid protocol");
      host = parsed.hostname.toLowerCase();
    } catch {
      reject("INVALID_URL");
      continue;
    }
    if (!MARKETAUX_WIRE_DOMAINS.some(domain => host === domain || host.endsWith(`.${domain}`))) {
      reject("DOMAIN_NOT_ALLOWED");
      continue;
    }
    const entities = Array.isArray(row.entities) ? row.entities : [];
    const matchedEntity = entities.find(entity => (
      !!entity && typeof entity === "object" && !Array.isArray(entity)
      && _str((entity as Record<string, unknown>).symbol).toUpperCase() === tickerU
    )) as Record<string, unknown> | undefined;
    if (!matchedEntity) {
      reject("ENTITY_SYMBOL_MISMATCH");
      continue;
    }
    const expectedExchangeNorm = normalizeExchange(expectedExchange);
    const providerExchangeNorm = normalizeExchange(_str(matchedEntity.exchange));
    if (expectedExchangeNorm && providerExchangeNorm && expectedExchangeNorm !== providerExchangeNorm) {
      reject("ENTITY_EXCHANGE_MISMATCH");
      continue;
    }
    const parsedAt = toIsoUtc(row.published_at);
    const publishedAt = parsedAt ? withoutZeroMillis(parsedAt) : null;
    if (!withinDateWindow(publishedAt, startDate, endDate, lookbackDays)) {
      reject("DATE_OUTSIDE_WINDOW");
      continue;
    }
    const summary = _str(row.snippet) || _str(row.description) || title;
    if (items.length >= limit) {
      diagnostics.acceptedCount = Number(diagnostics.acceptedCount ?? 0) + 1;
      continue;
    }
    const duplicateGroupId = makeDupGroupId(tickerU, title, publishedAt, null);
    items.push({
      title,
      source: "marketaux",
      originalSource: _str(row.source) || host,
      sourceType: "marketaux_wire",
      provider: "marketaux",
      discoveredVia: "marketaux_api",
      publishedAt,
      retrievedAt,
      url,
      issuer: _str(matchedEntity.name) || null,
      tickers: [tickerU],
      matchBasis: "PROVIDER_ENTITY_SYMBOL",
      sourceTickerMatch: true,
      eventType: eventTypeFromKeywords(title, summary),
      summary: shortText(summary, 240),
      evidenceText: shortText(summary, 180),
      confidence: "LOW",
      tickerRelevance: "HIGH",
      duplicateGroupId,
      providerArticleId: _str(row.uuid) || null,
      providerRelevanceScore: matchedEntity.match_score ?? null,
    });
    // R-news Batch C (decision 12): keep reading after the 3-row limit, so acceptedCount and the rejection counts cover every
    // row (Python behaviour); only the first `limit` accepted rows are returned.
    diagnostics.acceptedCount = Number(diagnostics.acceptedCount ?? 0) + 1;
  }
  diagnostics.status = Number(diagnostics.acceptedCount ?? 0) > 0 ? "OK" : "EMPTY_RESULT";
  diagnostics.collectedCount = items.length;
  diagnostics.completed = true;
  return { items, warnings, used: true, diagnostics };
}
