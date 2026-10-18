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
  /**
   * Provider fetches refused for the subrequest budget (or by Cloudflare's own
   * subrequest limit) while this scope or a scope nested in it was current.
   */
  budgetRefusals: number;
  /** Root scope only: subrequests (fetches and Cache API calls) this invocation has made. */
  subrequests?: number;
  /** Root scope only: the budget guard refused at least one fetch in this invocation. */
  budgetExhausted?: boolean;
  /** Root scope only (telemetry, 2.11.1): provider fetches aborted by their timeout. */
  providerTimeouts?: number;
  /** Root scope only (telemetry, 2.11.1): provider requests re-sent after a failed attempt. */
  providerRetries?: number;
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

/**
 * Per-invocation subrequest budget (2.11.0). Workers Free allows 50 subrequests
 * per invocation, Cache API calls and redirect hops included; new upstream work
 * stops at 45 so the reserve covers the response path and waitUntil cache writes.
 */
export const SUBREQUEST_LIMIT = 50;
export const SUBREQUEST_RESERVE = 5;
export const SUBREQUEST_BUDGET = SUBREQUEST_LIMIT - SUBREQUEST_RESERVE;

/** A provider fetch the budget guard refused; providerFetch throws it instead of calling fetch. */
export class BudgetExhaustedError extends Error {
  constructor() {
    super(`PROVIDER_BUDGET_EXHAUSTED: the per-request subrequest budget (${SUBREQUEST_BUDGET} of ${SUBREQUEST_LIMIT}) is spent; the request was not sent.`);
    this.name = "BudgetExhaustedError";
  }
}

/** A BudgetExhaustedError, or Cloudflare's own "Too many subrequests" error, which maps to the same state. */
export function isBudgetExhaustedError(error: unknown): boolean {
  if (error instanceof BudgetExhaustedError) return true;
  if (!error || typeof error !== "object") return false;
  const { name, message } = error as { name?: unknown; message?: unknown };
  return name === "BudgetExhaustedError" || (typeof message === "string" && /too many subrequests/i.test(message));
}

function rootScope(): CacheScope | undefined {
  let scope = scopes.getStore();
  while (scope?.parent) scope = scope.parent;
  return scope;
}

/** The current invocation's root scope, or undefined outside a request scope. */
export function currentRootScope(): CacheScope | undefined {
  return rootScope();
}

/** Count a provider fetch aborted by its timeout (telemetry only; nothing else changes). */
export function countProviderTimeout(): void {
  const root = rootScope();
  if (root) root.providerTimeouts = (root.providerTimeouts ?? 0) + 1;
}

/** Count a provider request re-sent after a failed attempt (telemetry only; nothing else changes). */
export function countProviderRetry(): void {
  const root = rootScope();
  if (root) root.providerRetries = (root.providerRetries ?? 0) + 1;
}

function countBudgetRefusal(): void {
  for (let scope = scopes.getStore(); scope; scope = scope.parent) scope.budgetRefusals++;
}

/**
 * Count one subrequest against the invocation's budget, at the call site of a
 * provider fetch (each redirect hop included) or a Cache API match/put/delete.
 * False means the budget is spent: the caller must not make the call. A refused
 * fetch marks the invocation as budget-exhausted; a refused cache call is
 * skipped by its caller (a miss, or a no-op) and consumes nothing. Outside a
 * request scope nothing is counted and the answer is always true.
 */
export function consumeSubrequest(kind: "fetch" | "cache"): boolean {
  const root = rootScope();
  if (!root) return true;
  const used = root.subrequests ?? 0;
  if (used >= SUBREQUEST_BUDGET) {
    if (kind === "fetch") {
      root.budgetExhausted = true;
      countBudgetRefusal();
    }
    return false;
  }
  root.subrequests = used + 1;
  return true;
}

/** The budget guard refused at least one fetch in the current invocation (drives the meta.warnings row). */
export function subrequestBudgetExhausted(): boolean {
  return rootScope()?.budgetExhausted === true;
}

/** The meta.warnings row of a response in an invocation where the budget guard refused a fetch. */
export const SUBREQUEST_BUDGET_WARNING = Object.freeze({
  code: "PROVIDER_BUDGET_EXHAUSTED",
  message: `Stopped starting new upstream requests at the per-request subrequest budget (${SUBREQUEST_BUDGET} of ${SUBREQUEST_LIMIT}); some sources were not queried.`,
  severity: "warning",
});

/** Fetches refused for budget while the current scope was current, nested scopes included. */
export function budgetRefusalCount(): number {
  return scopes.getStore()?.budgetRefusals ?? 0;
}

/**
 * Await a provider promise that may be shared with other callers (an in-flight
 * request): when it fails for budget, the refusal also counts in this caller's
 * scopes, so a waiter's source is reported as starved like the one that asked.
 */
export async function awaitNotingBudget<T>(pending: Promise<T>): Promise<T> {
  try {
    return await pending;
  } catch (error) {
    if (isBudgetExhaustedError(error) && scopes.getStore()) countBudgetRefusal();
    throw error;
  }
}

/** Run fn in a nested scope and say whether a provider fetch in it was refused for budget. */
export async function withBudgetWatch<T>(fn: () => Promise<T>): Promise<{ result: T; starved: boolean }> {
  const { result, scope } = await withCacheScope(fn);
  return { result, starved: scope.budgetRefusals > 0 };
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
 * redirect hops included, counts against the current scopes and against the
 * invocation's subrequest budget: once it is spent, BudgetExhaustedError is
 * thrown before fetch is called. Cloudflare's own "Too many subrequests" error
 * counts as a budget refusal too, and is rethrown as is.
 */
export async function providerFetch(input: string | URL, init: RequestInit = {}): Promise<Response> {
  // The first request goes out exactly as the caller spelled it; hops use the resolved target.
  let href = String(input);
  let current = checkedUrl(href);
  let requestInit: RequestInit = { ...init, redirect: "manual" };
  for (let hops = 0; ; hops++) {
    if (!consumeSubrequest("fetch")) throw new BudgetExhaustedError();
    countUpstreamRequest();
    let response: Response;
    try {
      response = await fetch(href, requestInit);
    } catch (error) {
      if (isBudgetExhaustedError(error) && scopes.getStore()) countBudgetRefusal();
      throw error;
    }
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
  const parent = scopes.getStore();
  const scope: CacheScope = parent
    ? { caches: {}, upstreamRequests: 0, budgetRefusals: 0, parent }
    : { caches: {}, upstreamRequests: 0, budgetRefusals: 0, subrequests: 0, budgetExhausted: false, providerTimeouts: 0, providerRetries: 0, parent };
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
  return summarizeCacheScope(scope);
}

/** currentCacheSummary for a given scope (the request's root scope, after it has finished). */
export function summarizeCacheScope(scope: CacheScope): { cacheHit: boolean; cacheSource: CacheSource | null } {
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
  /**
   * The caller may use the key-gated paid providers (2.18.0): true for the
   * owner key and for anonymous grace-mode requests, a personal key's
   * `paidProviders` otherwise. When false the provider credentials read as
   * unconfigured for this request (see paidProvidersAllowed).
   */
  paidProvidersAllowed?: boolean;
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

/**
 * Whether the current request may read the key-gated provider credentials
 * (2.18.0). True outside an /mcp request and whenever the flag is not false.
 * Checked in one place only: getWorkerVar (response.ts), which index.ts
 * points at this function through setPaidProvidersProvider.
 */
export function paidProvidersAllowed(): boolean {
  return requestFacts.getStore()?.paidProvidersAllowed !== false;
}
