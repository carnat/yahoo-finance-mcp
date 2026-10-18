/**
 * Per-person access keys (2.18.0, Phase 1b). Worker only.
 *
 * A personal key is `yfk_` + 43 base64url characters. Only its SHA-256 hash is
 * stored: KV namespace `yfmcp_access_keys` (binding `ACCESS_KEYS`), record
 * `key:<sha256 hex>`. Usage is counted by the `KeyUsage` Durable Object
 * (binding `USAGE`), one instance per key id.
 *
 * Personal keys work only when BOTH bindings are present; otherwise only the
 * owner key (`MCP_TOKEN`) works, exactly as before 2.18.0. The owner key never
 * reaches this module.
 *
 * Never logged, returned or recorded: the key, its hash, or the record's label.
 * A failing store or counter, or a malformed record, throws (fail closed); the
 * error message carries none of them.
 */

/** A stored key record (the JSON value of `key:<hash>`). */
export interface AccessKeyRecord {
  id: string;
  label: string;
  createdAt: string;
  status: string;
  dailyLimit: number;
  perMinuteLimit: number;
  paidProviders: boolean;
}

export interface UsageDecision {
  allowed: boolean;
  retryAfterSeconds: number;
}

/** Looks up a record by the key's SHA-256 hex hash. */
export interface AccessKeyStore {
  get(hash: string): Promise<AccessKeyRecord | null>;
}

/** Counts one request against a key's limits, and says whether it is allowed. */
export interface UsageCounter {
  checkAndIncrement(id: string, perMinuteLimit: number, dailyLimit: number): Promise<UsageDecision>;
}

export const ACCESS_KEY_SHAPE = /^yfk_[A-Za-z0-9_-]{43}$/;

/** The record lookup cache's lifetime in this isolate, for found and not-found results alike. */
export const ACCESS_KEY_CACHE_TTL_MS = 60_000;
const ACCESS_KEY_CACHE_MAX_ENTRIES = 1024;

/** Thrown when the store, the counter or a record cannot be trusted. Carries no key, hash or label. */
export class AccessKeyBackendError extends Error {
  constructor(what: string) {
    super(`ACCESS_KEY_BACKEND_ERROR: ${what}`);
    this.name = "AccessKeyBackendError";
  }
}

// ── Record validation ───────────────────────────────────────────────────────

function isLimit(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

/** A well-formed record, or a thrown AccessKeyBackendError (malformed records fail closed). */
export function parseAccessKeyRecord(value: unknown): AccessKeyRecord {
  const r = value as Partial<AccessKeyRecord> | null;
  if (
    r === null || typeof r !== "object" || Array.isArray(r)
    || typeof r.id !== "string" || !/^[0-9a-f]{8}$/.test(r.id)
    || typeof r.label !== "string"
    || typeof r.createdAt !== "string"
    || typeof r.status !== "string"
    || !isLimit(r.dailyLimit) || !isLimit(r.perMinuteLimit)
    || typeof r.paidProviders !== "boolean"
  ) {
    throw new AccessKeyBackendError("malformed access key record");
  }
  return {
    id: r.id, label: r.label, createdAt: r.createdAt, status: r.status,
    dailyLimit: r.dailyLimit, perMinuteLimit: r.perMinuteLimit, paidProviders: r.paidProviders,
  };
}

// ── Hashing ─────────────────────────────────────────────────────────────────

export async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

// ── KV store ────────────────────────────────────────────────────────────────

interface KvLike {
  get(key: string, type: "json"): Promise<unknown>;
}

function isKvLike(value: unknown): value is KvLike {
  return !!value && typeof value === "object" && typeof (value as KvLike).get === "function";
}

export function kvAccessKeyStore(kv: KvLike): AccessKeyStore {
  return {
    async get(hash) {
      let value: unknown;
      try {
        value = await kv.get(`key:${hash}`, "json");
      } catch {
        throw new AccessKeyBackendError("access key store unavailable");
      }
      return value === null || value === undefined ? null : parseAccessKeyRecord(value);
    },
  };
}

// ── Durable Object counter ──────────────────────────────────────────────────

interface DoStubLike {
  fetch(input: string, init?: RequestInit): Promise<Response>;
}

interface DoNamespaceLike {
  idFromName(name: string): unknown;
  get(id: unknown): DoStubLike;
}

function isDoNamespaceLike(value: unknown): value is DoNamespaceLike {
  return !!value && typeof value === "object"
    && typeof (value as DoNamespaceLike).idFromName === "function"
    && typeof (value as DoNamespaceLike).get === "function";
}

function parseDecision(value: unknown): UsageDecision {
  const d = value as Partial<UsageDecision> | null;
  if (d === null || typeof d !== "object" || typeof d.allowed !== "boolean" || !isLimit(d.retryAfterSeconds)) {
    throw new AccessKeyBackendError("malformed usage counter response");
  }
  return { allowed: d.allowed, retryAfterSeconds: d.retryAfterSeconds };
}

/**
 * The counter backed by the `KeyUsage` Durable Object: one instance per key id,
 * one stub fetch per request (a subrequest from the reserve outside the
 * 45-subrequest provider budget).
 */
export function durableObjectUsageCounter(namespace: DoNamespaceLike): UsageCounter {
  return {
    async checkAndIncrement(id, perMinuteLimit, dailyLimit) {
      let response: Response;
      try {
        const stub = namespace.get(namespace.idFromName(id));
        response = await stub.fetch("https://key-usage.internal/check-and-increment", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ perMinuteLimit, dailyLimit }),
        });
      } catch {
        throw new AccessKeyBackendError("usage counter unavailable");
      }
      if (!response.ok) throw new AccessKeyBackendError("usage counter unavailable");
      let body: unknown;
      try {
        body = await response.json();
      } catch {
        throw new AccessKeyBackendError("malformed usage counter response");
      }
      return parseDecision(body);
    },
  };
}

// ── Counting windows (pure; used by the Durable Object) ─────────────────────

export interface UsageWindowState {
  /** Minutes since the epoch of the current per-minute window. */
  minute: number;
  minuteCount: number;
  /** UTC days since the epoch of the current daily window. */
  day: number;
  dayCount: number;
}

export function emptyUsageWindow(): UsageWindowState {
  return { minute: 0, minuteCount: 0, day: 0, dayCount: 0 };
}

/**
 * Count one request in `state` if both limits allow it. A refused request is
 * not counted. Retry-After is the time to the end of the window that refused:
 * the UTC day when the daily limit is reached, else the current minute.
 */
export function checkAndIncrementWindow(
  state: UsageWindowState,
  nowMs: number,
  perMinuteLimit: number,
  dailyLimit: number,
): UsageDecision {
  const minute = Math.floor(nowMs / 60_000);
  const day = Math.floor(nowMs / 86_400_000);
  if (state.minute !== minute) {
    state.minute = minute;
    state.minuteCount = 0;
  }
  if (state.day !== day) {
    state.day = day;
    state.dayCount = 0;
  }
  if (state.dayCount >= dailyLimit) {
    return { allowed: false, retryAfterSeconds: Math.max(1, Math.ceil(((day + 1) * 86_400_000 - nowMs) / 1000)) };
  }
  if (state.minuteCount >= perMinuteLimit) {
    return { allowed: false, retryAfterSeconds: Math.max(1, Math.ceil(((minute + 1) * 60_000 - nowMs) / 1000)) };
  }
  state.minuteCount += 1;
  state.dayCount += 1;
  return { allowed: true, retryAfterSeconds: 0 };
}

interface DoStorageLike {
  get<T>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
}

interface DoStateLike {
  storage: DoStorageLike;
}

/**
 * Durable Object class `KeyUsage` (SQLite backend), one instance per key id.
 * A fetch-handler class, so the Worker module needs no `cloudflare:workers`
 * import. POST {perMinuteLimit, dailyLimit} → {allowed, retryAfterSeconds}.
 *
 * 2.18.0 exports the class only; the `USAGE` binding and its
 * `new_sqlite_classes` migration arrive with the activation release.
 */
export class KeyUsage {
  private readonly state: DoStateLike;
  private window: UsageWindowState | null = null;

  constructor(state: DoStateLike, _env: unknown) {
    this.state = state;
  }

  async fetch(request: Request): Promise<Response> {
    let body: { perMinuteLimit?: unknown; dailyLimit?: unknown };
    try {
      body = await request.json() as typeof body;
    } catch {
      return new Response("Bad Request", { status: 400 });
    }
    if (!isLimit(body?.perMinuteLimit) || !isLimit(body?.dailyLimit)) {
      return new Response("Bad Request", { status: 400 });
    }
    if (this.window === null) {
      this.window = (await this.state.storage.get<UsageWindowState>("usage")) ?? emptyUsageWindow();
    }
    const decision = checkAndIncrementWindow(this.window, Date.now(), body.perMinuteLimit, body.dailyLimit);
    await this.state.storage.put("usage", this.window);
    return new Response(JSON.stringify(decision), { headers: { "Content-Type": "application/json" } });
  }
}

// ── Isolate cache and dependency resolution ─────────────────────────────────

const recordCache = new Map<string, { record: AccessKeyRecord | null; expiresAt: number }>();

/** Look a hash up through the 60-second isolate cache. Failures (throws) are never cached. */
async function cachedLookup(store: AccessKeyStore, hash: string): Promise<AccessKeyRecord | null> {
  const now = Date.now();
  const hit = recordCache.get(hash);
  if (hit && hit.expiresAt > now) return hit.record;
  if (hit) recordCache.delete(hash);
  const record = await store.get(hash);
  if (recordCache.size >= ACCESS_KEY_CACHE_MAX_ENTRIES) {
    const oldest = recordCache.keys().next();
    if (!oldest.done) recordCache.delete(oldest.value);
  }
  recordCache.set(hash, { record, expiresAt: now + ACCESS_KEY_CACHE_TTL_MS });
  return record;
}

let injectedStore: AccessKeyStore | null | undefined;
let injectedCounter: UsageCounter | null | undefined;

/** Tests only: replace the store (null forces it absent; undefined restores the env binding). */
export function setAccessKeyStoreForTests(store: AccessKeyStore | null | undefined): void {
  injectedStore = store;
  recordCache.clear();
}

/** Tests only: replace the counter (null forces it absent; undefined restores the env binding). */
export function setUsageCounterForTests(counter: UsageCounter | null | undefined): void {
  injectedCounter = counter;
}

/** Tests only: empty the isolate cache. */
export function clearAccessKeyCacheForTests(): void {
  recordCache.clear();
}

function resolveStore(env: Record<string, unknown>): AccessKeyStore | null {
  if (injectedStore !== undefined) return injectedStore;
  return isKvLike(env.ACCESS_KEYS) ? kvAccessKeyStore(env.ACCESS_KEYS) : null;
}

function resolveCounter(env: Record<string, unknown>): UsageCounter | null {
  if (injectedCounter !== undefined) return injectedCounter;
  return isDoNamespaceLike(env.USAGE) ? durableObjectUsageCounter(env.USAGE) : null;
}

export type PersonalKeyResult =
  | { kind: "unknown" }
  | { kind: "allowed"; id: string; paidProviders: boolean }
  | { kind: "limited"; id: string; retryAfterSeconds: number };

/**
 * Check a presented credential that is not the owner key. `unknown` (the
 * existing 401) for a credential without the key shape, without both
 * bindings, with no record, or with a record that is not active. Throws when
 * the store or counter fails or the record is malformed.
 */
export async function checkPersonalKey(presented: string, env: Record<string, unknown>): Promise<PersonalKeyResult> {
  if (!ACCESS_KEY_SHAPE.test(presented)) return { kind: "unknown" };
  const store = resolveStore(env);
  const counter = resolveCounter(env);
  if (store === null || counter === null) return { kind: "unknown" };
  const record = await cachedLookup(store, await sha256Hex(presented));
  if (record === null || record.status !== "active") return { kind: "unknown" };
  const decision = await counter.checkAndIncrement(record.id, record.perMinuteLimit, record.dailyLimit);
  return decision.allowed
    ? { kind: "allowed", id: record.id, paidProviders: record.paidProviders }
    : { kind: "limited", id: record.id, retryAfterSeconds: decision.retryAfterSeconds };
}
