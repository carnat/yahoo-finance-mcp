import { AsyncLocalStorage } from "node:async_hooks";

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

/** fetch() for provider requests; the request counts against the current scopes. */
export function providerFetch(input: string | URL | Request, init?: RequestInit): Promise<Response> {
  countUpstreamRequest();
  return fetch(input, init);
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
