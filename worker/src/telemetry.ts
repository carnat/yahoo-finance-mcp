/**
 * Privacy-minimised operational telemetry (2.11.1, R4 Q10).
 *
 * One Workers Analytics Engine data point per /mcp request, rejections
 * included, written after the response is built, plus one structured
 * dispatch-start log line so an invocation killed for CPU still carries its
 * action in Workers Logs.
 *
 * What is recorded comes only from the server itself: `action` and `domain`
 * are looked up in the server's own tool registry (an unknown tool records
 * `tools/call:unknown`, an unknown or non-string method `rpc:other`), error
 * codes come from the server's own envelopes, and everything else is a
 * counter, a bucket or a fixed enum. The caller is recorded as a key id only
 * (`owner`, a personal key's hash prefix, or `-`). Never recorded: the credential, the
 * Authorization header, the request URL or path, request or response bodies,
 * tool arguments, provider content, the caller's IP or `cf` geo fields.
 *
 * Without the TELEMETRY binding (tests, local dev) the data point is a no-op.
 * Telemetry never changes a response: the binding call is synchronous, not
 * awaited, and any error from it is swallowed.
 */

import { GROUPED_TOOL_DEFS } from "./tool-catalog.js";
import type { CacheScope } from "./request-context.js";
import { summarizeCacheScope } from "./request-context.js";
import { getServerVersion, getWorkerVar } from "./response.js";
import { TOOLS, isGroupedMode } from "./tools.js";

/** The part of the Analytics Engine binding this module uses. */
export interface TelemetryDataset {
  writeDataPoint(point: { indexes?: string[]; blobs?: string[]; doubles?: number[] }): void;
}

export type TelemetryOutcome = "ok" | "tool_error" | "rpc_error" | "rejected" | "exception";

/** What one /mcp request contributes to its data point; filled in as the request is served. */
export interface RequestRecord {
  action: string;
  domain: string;
  outcome: TelemetryOutcome;
  errorCode: string;
  credentialTransport: "header" | "path" | "none";
  httpStatus: number;
  /** The response body as sent, or null for none. Measured only when the binding is present. */
  body: string | null;
  /** The request's root cache scope, once dispatch has started. */
  scope: CacheScope | undefined;
  /**
   * Who called (2.18.0): `owner`, a personal key's 8-character id, or `-` when
   * no key is identified. Never the key, its hash or its label.
   */
  keyId: string;
}

export function newRequestRecord(credentialTransport: "header" | "path" | "none"): RequestRecord {
  return {
    action: "rpc:other",
    domain: "-",
    outcome: "ok",
    errorCode: "",
    credentialTransport,
    httpStatus: 0,
    body: null,
    scope: undefined,
    keyId: "-",
  };
}

// ── Registry lookups ────────────────────────────────────────────────────────

const GROUPED_ACTIONS = new Map(GROUPED_TOOL_DEFS.map((group) => [group.name, new Set(Object.keys(group.actions))]));
/** Expanded-mode tool name → the grouped tool that carries it as an action. */
const GROUP_OF_ACTION = new Map<string, string>();
for (const group of GROUPED_TOOL_DEFS) {
  for (const action of Object.keys(group.actions)) if (!GROUP_OF_ACTION.has(action)) GROUP_OF_ACTION.set(action, group.name);
}
const EXPANDED_TOOLS = new Set(TOOLS.map((tool) => tool.name));
/** JSON-RPC methods the dispatcher implements (mcp.ts). */
const KNOWN_METHODS = new Set(["initialize", "ping", "tools/list", "tools/call"]);

function classifyMessage(message: unknown): { action: string; domain: string } {
  if (message == null || typeof message !== "object" || Array.isArray(message)) return { action: "rpc:other", domain: "-" };
  const { method, params } = message as { method?: unknown; params?: unknown };
  if (typeof method !== "string" || !KNOWN_METHODS.has(method)) return { action: "rpc:other", domain: "-" };
  if (method !== "tools/call") return { action: method, domain: "-" };

  const p = params != null && typeof params === "object" ? params as { name?: unknown; arguments?: unknown } : {};
  const name = typeof p.name === "string" ? p.name : null;
  if (name === null) return { action: "tools/call:unknown", domain: "-" };
  if (isGroupedMode()) {
    const actions = GROUPED_ACTIONS.get(name);
    if (!actions) return { action: "tools/call:unknown", domain: "-" };
    const args = p.arguments != null && typeof p.arguments === "object" ? p.arguments as { action?: unknown } : {};
    // The dispatcher trims the action before looking it up.
    const action = typeof args.action === "string" ? args.action.trim() : "";
    return actions.has(action)
      ? { action: `tools/call:${name}.${action}`, domain: name }
      : { action: "tools/call:unknown", domain: name };
  }
  return EXPANDED_TOOLS.has(name)
    ? { action: `tools/call:${name}`, domain: GROUP_OF_ACTION.get(name) ?? "-" }
    : { action: "tools/call:unknown", domain: "-" };
}

/**
 * The action and domain of a parsed /mcp body. For a batch: its tools/call
 * (a batch carries at most one), else its first message.
 */
export function classifyBody(body: unknown): { action: string; domain: string } {
  if (!Array.isArray(body)) return classifyMessage(body);
  const call = body.find((m) => m != null && typeof m === "object" && (m as { method?: unknown }).method === "tools/call");
  return classifyMessage(call ?? body[0]);
}

// ── Outcome of a dispatched request ─────────────────────────────────────────

const ENVELOPE_CODE = /^[A-Z][A-Z0-9_]{0,63}$/;

function envelopeErrorCode(structured: unknown): string {
  if (structured == null || typeof structured !== "object") return "";
  const record = structured as { error?: unknown; code?: unknown };
  const nested = record.error != null && typeof record.error === "object" ? (record.error as { code?: unknown }).code : undefined;
  const code = typeof nested === "string" ? nested : typeof record.code === "string" ? record.code : "";
  return ENVELOPE_CODE.test(code) ? code : "";
}

function rpcErrorCode(error: unknown): string {
  const code = error != null && typeof error === "object" ? (error as { code?: unknown }).code : undefined;
  return typeof code === "number" && Number.isInteger(code) ? String(code) : "";
}

/** Outcome and error code of a JSON-RPC response (or batch of them) built by handleMcp. */
export function classifyResult(result: unknown): { outcome: TelemetryOutcome; errorCode: string } {
  const responses = Array.isArray(result) ? result : [result];
  let toolError: string | null = null;
  for (const response of responses) {
    if (response == null || typeof response !== "object") continue;
    const r = response as { error?: unknown; result?: unknown };
    if (r.error !== undefined) return { outcome: "rpc_error", errorCode: rpcErrorCode(r.error) };
    const res = r.result as { isError?: unknown; structuredContent?: unknown } | undefined;
    if (toolError === null && res?.isError === true) toolError = envelopeErrorCode(res.structuredContent);
  }
  return toolError === null ? { outcome: "ok", errorCode: "" } : { outcome: "tool_error", errorCode: toolError };
}

// ── Build identity ──────────────────────────────────────────────────────────

/** The first 7 characters of BUILD_SHA, lower-case, or empty when unset or not a hex SHA. */
export function buildShaShort(): string {
  const sha = getWorkerVar("BUILD_SHA")?.trim();
  return sha && /^[0-9a-f]{7,64}$/i.test(sha) ? sha.slice(0, 7).toLowerCase() : "";
}

function serverVersion(): string {
  const version = getServerVersion();
  return /^[0-9A-Za-z.+-]{1,64}$/.test(version) ? version : "";
}

// ── Emitters ────────────────────────────────────────────────────────────────

/**
 * The one dispatch-start line for Workers Logs. It carries the action, the
 * domain and the build only: never arguments, tickers, ids or URLs. Logged as
 * an object, not a JSON string, so Workers Logs extracts its fields (2.14.0).
 */
export function logDispatchStart(action: string, domain: string): void {
  console.log({ t: "yfmcp", v: 1, action, domain, build: buildShaShort() });
}

export function sizeBucket(bytes: number): string {
  if (bytes < 1024) return "<1K";
  if (bytes < 10 * 1024) return "1-10K";
  if (bytes < 100 * 1024) return "10-100K";
  if (bytes < 1024 * 1024) return "100K-1M";
  return ">=1M";
}

/** `owner`, an 8-hex key id, or `-`; anything else is recorded as `-`. */
function keyIdBlob(keyId: string): string {
  return keyId === "owner" || /^[0-9a-f]{8}$/.test(keyId) ? keyId : "-";
}

function timeoutClass(timeouts: number, upstream: number): string {
  if (timeouts <= 0) return "none";
  return timeouts >= upstream ? "all" : "some";
}

/**
 * Write the request's data point. A no-op without the binding; never throws.
 *
 * index1 = action. Blobs: domain, serverVersion, buildShaShort, outcome,
 * errorCode, sizeBucket, credentialTransport, budgetExhausted, cacheSource,
 * toolMode, timeoutClass, keyId (2.18.0). Doubles: latencyMs, responseBytes, httpStatus,
 * upstreamRequests, subrequestsUsed, budgetRefusals, providerTimeouts,
 * edgeCacheHits, edgeCacheMisses, providerRetries.
 */
export function writeRequestDataPoint(dataset: TelemetryDataset | undefined, record: RequestRecord, startedAt: number): void {
  if (!dataset || typeof dataset.writeDataPoint !== "function") return;
  try {
    const latencyMs = Math.max(0, Date.now() - startedAt);
    const responseBytes = record.body === null ? 0 : new TextEncoder().encode(record.body).length;
    const scope = record.scope;
    let edgeHits = 0;
    let edgeMisses = 0;
    for (const usage of Object.values(scope?.caches ?? {})) {
      edgeHits += usage.edgeHits;
      edgeMisses += usage.edgeMisses;
    }
    const upstream = scope?.upstreamRequests ?? 0;
    const timeouts = scope?.providerTimeouts ?? 0;
    const cacheSource = scope ? summarizeCacheScope(scope).cacheSource ?? "" : "";
    dataset.writeDataPoint({
      indexes: [record.action],
      blobs: [
        record.domain,
        serverVersion(),
        buildShaShort(),
        record.outcome,
        record.errorCode,
        sizeBucket(responseBytes),
        record.credentialTransport,
        scope?.budgetExhausted === true ? "1" : "0",
        cacheSource,
        isGroupedMode() ? "grouped" : "expanded",
        timeoutClass(timeouts, upstream),
        keyIdBlob(record.keyId),
      ],
      doubles: [
        latencyMs,
        responseBytes,
        record.httpStatus,
        upstream,
        scope?.subrequests ?? 0,
        scope?.budgetRefusals ?? 0,
        timeouts,
        edgeHits,
        edgeMisses,
        scope?.providerRetries ?? 0,
      ],
    });
  } catch {
    // Telemetry must never affect the response.
  }
}
