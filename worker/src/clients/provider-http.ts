/**
 * Provider HTTP transport (Phase 4 unit 2, moved out of yahoo-finance.ts): the shared browser User-Agent, the
 * provider JSON cache (process memory plus the colo edge cache), provider status mapping and the timed fetch that
 * every upstream client goes through.
 */

import { setBounded } from "../cache.js";
import { consumeSubrequest, countCacheEvent, countProviderTimeout, providerFetch } from "../request-context.js";

export const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

export type ProviderJsonResult = {
  payload: Record<string, unknown> | null;
  status: string;
  publicUrl: string;
  providerAttempted: boolean;
  cacheStatus: "MISS" | "HIT_PROCESS" | "HIT_EDGE";
  fetchedAt?: string;
  httpStatus?: number;
  message?: string;
  retryAfter?: string;
  // When a remembered denial (daily quota, entitlement) expires and the provider is asked again (2.5.22).
  deniedUntil?: string;
};

type ProviderCacheEntry = {
  payload: Record<string, unknown>;
  publicUrl: string;
  fetchedAt: string;
  storedAt: number;
  ttlMs: number;
};

type EdgeCache = {
  match(request: Request): Promise<Response | undefined>;
  put(request: Request, response: Response): Promise<void>;
  delete(request: Request): Promise<boolean>;
};

const PROVIDER_JSON_CACHE_MAX = 200;
const providerJsonCache = new Map<string, ProviderCacheEntry>();

export function providerCacheKey(provider: string, operation: string, params: Record<string, unknown>): string {
  const query = new URLSearchParams(
    Object.entries(params)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, value]): [string, string] => [key, String(value)])
  );
  return `${provider}:${operation}:${query.toString()}`;
}

/** "finnhub:company-news:..." → "finnhub" */
export function providerCacheName(cacheKey: string): string {
  return cacheKey.slice(0, cacheKey.indexOf(":"));
}

function providerEdgeRequest(cacheKey: string): Request {
  return new Request(`https://provider-cache.invalid/${encodeURIComponent(cacheKey)}`);
}

export function edgeCache(): EdgeCache | null {
  const runtime = globalThis as typeof globalThis & { caches?: { default?: EdgeCache } };
  return runtime.caches?.default ?? null;
}

export async function getProviderCache(cacheKey: string, ttlMs: number): Promise<ProviderJsonResult | null> {
  const memory = providerJsonCache.get(cacheKey);
  if (memory && Date.now() - memory.storedAt < Math.min(ttlMs, memory.ttlMs)) {
    countCacheEvent(providerCacheName(cacheKey), "memoryHits");
    return {
      payload: memory.payload,
      status: "OK",
      publicUrl: memory.publicUrl,
      providerAttempted: false,
      cacheStatus: "HIT_PROCESS",
      fetchedAt: memory.fetchedAt,
    };
  }
  const cache = edgeCache();
  if (!cache) return null;
  // A spent subrequest budget skips the edge read: a miss, never an error.
  if (!consumeSubrequest("cache")) return null;
  try {
    const response = await cache.match(providerEdgeRequest(cacheKey));
    if (!response) return null;
    const value = await response.json() as ProviderCacheEntry;
    if (!value?.payload || Date.now() - Number(value.storedAt) >= Math.min(ttlMs, Number(value.ttlMs))) return null;
    setBounded(providerJsonCache, cacheKey, value, PROVIDER_JSON_CACHE_MAX);
    countCacheEvent(providerCacheName(cacheKey), "edgeHits");
    return {
      payload: value.payload,
      status: "OK",
      publicUrl: value.publicUrl,
      providerAttempted: false,
      cacheStatus: "HIT_EDGE",
      fetchedAt: value.fetchedAt,
    };
  } catch {
    return null;
  }
}

export async function setProviderCache(
  cacheKey: string,
  payload: Record<string, unknown>,
  publicUrl: string,
  fetchedAt: string,
  ttlMs: number,
): Promise<void> {
  const value: ProviderCacheEntry = { payload, publicUrl, fetchedAt, storedAt: Date.now(), ttlMs };
  setBounded(providerJsonCache, cacheKey, value, PROVIDER_JSON_CACHE_MAX);
  const cache = edgeCache();
  if (!cache || !consumeSubrequest("cache")) return;
  try {
    await cache.put(
      providerEdgeRequest(cacheKey),
      new Response(JSON.stringify(value), {
        headers: {
          "Content-Type": "application/json",
          "Cache-Control": `public, max-age=${Math.max(1, Math.floor(ttlMs / 1000))}`,
        },
      }),
    );
  } catch {
    // Edge cache is best effort; the provider response remains usable.
  }
}

export async function deleteProviderCache(cacheKey: string): Promise<void> {
  providerJsonCache.delete(cacheKey);
  const cache = edgeCache();
  if (!cache || !consumeSubrequest("cache")) return;
  try {
    await cache.delete(providerEdgeRequest(cacheKey));
  } catch {
    // Edge cache invalidation is best effort; the provider request still proceeds.
  }
}

export function providerMessageStatus(message: string): string {
  const normalized = message.toLowerCase();
  if (/rate limit|frequency|call volume|standard api rate|too many requests/.test(normalized)) return "RATE_LIMIT";
  if (/premium|subscription|subscribe|entitlement|current plan|access to this resource/.test(normalized)) return "ENTITLEMENT_REQUIRED";
  if (/api key|apikey|authentication|unauthorized|invalid token|forbidden/.test(normalized)) return "AUTH_ERROR";
  return "PROVIDER_ERROR";
}

export function providerHttpStatus(status: number, message: string): string {
  if (status === 429) return "RATE_LIMIT";
  if (status === 401) return "AUTH_ERROR";
  if (status === 403) return providerMessageStatus(message) === "ENTITLEMENT_REQUIRED" ? "ENTITLEMENT_REQUIRED" : "AUTH_ERROR";
  if (status === 404) return "NOT_FOUND";
  return "PROVIDER_ERROR";
}

export async function fetchProviderWithTimeout(
  url: string,
  init: RequestInit,
  timeoutMs: number,
): Promise<Response> {
  const controller = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; controller.abort(); }, timeoutMs);
  try {
    return await providerFetch(url, { ...init, signal: controller.signal });
  } catch (error) {
    if (timedOut) countProviderTimeout();
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
