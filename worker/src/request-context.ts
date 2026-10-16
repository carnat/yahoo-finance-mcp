import { AsyncLocalStorage } from "node:async_hooks";
import { checkFetchUrl, UrlPolicyError } from "./url-policy.js";

/**
 * Per-request and per-tool-call cache attribution.
 *
 * Every cached provider read (Yahoo GETs, SEC archive documents, scarce
 * provider JSON) reports its outcome here, and every outbound provider
 * request goes through providerFetch. A scope collects the events of one
 * `/mcp` request or one tool call; scopes nest, and an event counts in every
 * enclosing scope. `AsyncLocalStorage` (the `nodejs_als` compatibility flag)
 * keeps concurrent requests in one isolate apart.
 */

export type CacheEvent =
  | "memoryHits"
  | "sharedInflight"
  | "edgeHits"
  | "edgeMisses"
  | "edgeWrites"
  | "upstreamFetches";

export type CacheUsage = Record<CacheEvent, number>;

export function emptyCacheUsage(): CacheUsage {
  return { memoryHits: 0, sharedInflight: 0, edgeHits: 0, edgeMisses: 0, edgeWrites: 0, upstreamFetches: 0 };
}

export interface CacheScope {
  /** Cache events by cache name, e.g. "yahoo", "sec", "finnhub". */
  caches: Record<string, CacheUsage>;
  /** Outbound provider HTTP requests, cached reads' misses included. */
  upstreamRequests: number;
  parent: CacheScope | undefined;
}

const scopes = new AsyncLocalStorage<CacheScope>();

export function countCacheEvent(cache: string, event: CacheEvent): void {
  for (let scope = scopes.getStore(); scope; scope = scope.parent) {
    (scope.caches[cache] ??= emptyCacheUsage())[event]++;
  }
}

function countUpstreamRequest(): void {
  for (let scope = scopes.getStore(); scope; scope = scope.parent) scope.upstreamRequests++;
}

const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);
/** Most redirects providerFetch follows for one request. */
export const MAX_REDIRECT_HOPS = 5;

function checkedUrl(url: string): URL {
  const check = checkFetchUrl(url);
  if (!check.ok) throw new UrlPolicyError(check.reason);
  return check.url;
}

/**
 * fetch() for provider requests, and the single choke point for outbound URLs.
 *
 * The first URL and every redirect target must pass checkFetchUrl, or a
 * UrlPolicyError is thrown. Redirects are followed here (`redirect: "manual"`),
 * at most MAX_REDIRECT_HOPS of them; a 303 continues as GET. Each request,
 * redirect hops included, counts against the current scopes.
 */
export async function providerFetch(input: string | URL, init: RequestInit = {}): Promise<Response> {
  // The first request goes out exactly as the caller spelled it; hops use the resolved target.
  let href = String(input);
  let current = checkedUrl(href);
  let requestInit: RequestInit = { ...init, redirect: "manual" };
  for (let hops = 0; ; hops++) {
    countUpstreamRequest();
    const response = await fetch(href, requestInit);
    const location = REDIRECT_STATUSES.has(response.status) ? response.headers.get("Location") : null;
    if (location === null) return response;
    await response.body?.cancel();
    if (hops >= MAX_REDIRECT_HOPS) throw new UrlPolicyError("too many redirects");
    let target: string;
    try {
      target = new URL(location, current).toString();
    } catch {
      throw new UrlPolicyError("invalid URL");
    }
    const next = checkedUrl(target);
    const method = (requestInit.method ?? "GET").toUpperCase();
    if (response.status === 303 && method !== "GET" && method !== "HEAD") {
      const { body: _body, ...rest } = requestInit;
      requestInit = { ...rest, method: "GET" };
    }
    if (next.origin !== current.origin && requestInit.headers) {
      // As fetch's own redirect handling does: a credential header stays with its origin.
      const headers = new Headers(requestInit.headers);
      headers.delete("Authorization");
      requestInit = { ...requestInit, headers };
    }
    current = next;
    href = target;
  }
}

/** Run fn in a new scope nested in the current one, and return that scope's events. */
export async function withCacheScope<T>(fn: () => Promise<T>): Promise<{ result: T; scope: CacheScope }> {
  const scope: CacheScope = { caches: {}, upstreamRequests: 0, parent: scopes.getStore() };
  const result = await scopes.run(scope, fn);
  return { result, scope };
}

/** Header form, e.g. "memory=1, shared=0, edge-hit=2, edge-miss=0, edge-write=0, upstream=1". */
export function formatCacheUsage(usage: CacheUsage | undefined): string {
  const u = usage ?? emptyCacheUsage();
  return [
    `memory=${u.memoryHits}`,
    `shared=${u.sharedInflight}`,
    `edge-hit=${u.edgeHits}`,
    `edge-miss=${u.edgeMisses}`,
    `edge-write=${u.edgeWrites}`,
    `upstream=${u.upstreamFetches}`,
  ].join(", ");
}

export type CacheSource = "memory" | "edge" | "upstream" | "mixed";

/**
 * Where the current scope's provider data came from. `memory` covers the
 * process cache and requests already in flight for another caller. cacheHit
 * is true only when data was read and no provider request was made.
 */
export function currentCacheSummary(): { cacheHit: boolean; cacheSource: CacheSource | null } {
  const scope = scopes.getStore();
  if (!scope) return { cacheHit: false, cacheSource: null };
  const sources = new Set<CacheSource>();
  for (const usage of Object.values(scope.caches)) {
    if (usage.memoryHits + usage.sharedInflight > 0) sources.add("memory");
    if (usage.edgeHits > 0) sources.add("edge");
  }
  if (scope.upstreamRequests > 0) sources.add("upstream");
  const [only] = sources;
  return {
    cacheHit: sources.size > 0 && !sources.has("upstream"),
    cacheSource: sources.size === 0 ? null : sources.size === 1 ? only : "mixed",
  };
}

/**
 * Facts about the inbound HTTP request that tools may report. Only booleans
 * derived from the request are kept here, never header values.
 */
export interface RequestFacts {
  /** A non-empty Authorization header arrived with this /mcp request. */
  authorizationHeaderPresent: boolean;
  /** How this /mcp request presented its credential. Never the credential itself. */
  credentialTransport: "header" | "path" | "none";
}

const requestFacts = new AsyncLocalStorage<RequestFacts>();

/** Run fn with the facts of the current inbound request. */
export function withRequestFacts<T>(facts: RequestFacts, fn: () => Promise<T>): Promise<T> {
  return requestFacts.run(facts, fn);
}

/** The current request's facts, or undefined outside an /mcp request. */
export function currentRequestFacts(): RequestFacts | undefined {
  return requestFacts.getStore();
}
