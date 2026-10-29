/**
 * Cloudflare Worker entry point for Yahoo Finance MCP server.
 *
 * Exposes a single MCP endpoint using the Streamable HTTP transport
 * (stateless; the only Durable Object is the per-person key usage counter,
 * `KeyUsage`, 2.18.0):
 *
 *   POST /mcp          →  MCP JSON-RPC request / batch
 *   POST /mcp/<token>  →  the same, with the credential in the path
 *   GET  /      →  health check
 *
 * Connect from Claude Desktop (claude_desktop_config.json):
 *   {
 *     "mcpServers": {
 *       "yahoo-finance": {
 *         "url": "https://<your-worker>.workers.dev/mcp"
 *       }
 *     }
 *   }
 *
 * Connect from Claude.ai remote MCP:
 *   Add the URL https://<your-worker>.workers.dev/mcp in Settings → Integrations.
 */

import { identifyCaller, resolveCredential, timingSafeEqual, type CallerIdentity } from "./auth.js";
import { handleMcp } from "./mcp.js";
import { setEvidenceBucket } from "./evidence-store.js";
import { getBuildVersion, getServerVersion, setCacheSummaryProvider, setPaidProvidersProvider, setWorkerEnv, getWorkerVar } from "./response.js";
import { TOOLS, callTool } from "./tools.js";
import { currentCacheSummary, currentRootScope, formatCacheUsage, paidProvidersAllowed, withCacheScope, withRequestFacts } from "./request-context.js";
import { classifyBody, classifyResult, logDispatchStart, newRequestRecord, writeRequestDataPoint, type RequestRecord, type TelemetryDataset } from "./telemetry.js";
import { markYahooCacheActivity, yahooCacheStats } from "./clients/yahoo.js";
import { secDocumentCacheStats } from "./clients/edgar.js";

/** Durable Object class for per-person key usage (2.18.0); bound as `USAGE` from the activation release on. */
export { KeyUsage } from "./access-keys.js";

export interface Env {
  MCP_ENVELOPE_V2?: string;
  SERVER_VERSION?: string;
  BUILD_VERSION?: string;
  WORKER_VERSION?: string;
  TOOL_MODE?: string;
  AUDIT_TOKEN?: string;
  MCP_TOKEN?: string;
  MCP_AUTH_MODE?: string;
  BUILD_SHA?: string;
  DEPLOYED_AT?: string;
  CF_VERSION_METADATA?: WorkerVersionMetadata;
  /** Workers Analytics Engine dataset (2.11.1); absent in tests and local dev, where telemetry is a no-op. */
  TELEMETRY?: TelemetryDataset;
  /** Per-person access keys (2.18.0): KV `yfmcp_access_keys`. Personal keys need it and USAGE. */
  ACCESS_KEYS?: unknown;
  /** Per-person key usage counter (2.18.0): Durable Object namespace of class KeyUsage. */
  USAGE?: unknown;
  [key: string]: unknown;
}

setCacheSummaryProvider(currentCacheSummary);
// Per-person keys without paidProviders read the key-gated provider credentials as unset (2.18.0).
setPaidProvidersProvider(paidProvidersAllowed);

/** The one browser origin allowed to read /mcp responses. */
const MCP_ALLOWED_ORIGIN = "https://claude.ai";

type CorsRoute = "public" | "mcp" | "none";

/**
 * CORS headers for a response (2.10.0, S-13).
 *
 * - `public` (`/`, `/health`): readable from any origin.
 * - `mcp` (`/mcp`, `/mcp/<token>`): only for `Origin: https://claude.ai`, which
 *   is reflected with `Vary: Origin`; any other origin, or none, gets no CORS
 *   headers. Server-side clients send no Origin and are unaffected.
 * - `none` (`/audit/mcp`, unknown paths): no CORS headers.
 */
function corsHeadersFor(request: Request, route: CorsRoute): Record<string, string> {
  if (route === "public") {
    return {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Mcp-Session-Id",
      "Access-Control-Max-Age": "86400",
      "Access-Control-Expose-Headers": "X-Yahoo-Cache, X-Sec-Cache",
    };
  }
  if (route === "mcp" && request.headers.get("Origin") === MCP_ALLOWED_ORIGIN) {
    return {
      "Access-Control-Allow-Origin": MCP_ALLOWED_ORIGIN,
      "Vary": "Origin",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Mcp-Session-Id, Authorization",
      "Access-Control-Max-Age": "86400",
      "Access-Control-Expose-Headers": "X-Yahoo-Cache, X-Sec-Cache",
    };
  }
  return {};
}

function isMcpPath(pathname: string): boolean {
  return pathname === "/mcp" || pathname.startsWith("/mcp/");
}

/** Largest accepted request body: 1 MiB. */
const MAX_BODY_BYTES = 1_048_576;
/** Most tools/call requests accepted in one JSON-RPC batch. */
const MAX_TOOL_CALLS_PER_BATCH = 1;

const AUTH_REQUIRED_BODY = {
  jsonrpc: "2.0",
  id: null,
  error: {
    code: -32001,
    message: "AUTH_REQUIRED: a valid credential is required (Authorization: Bearer <token> or /mcp/<token>).",
    data: { code: "AUTH_REQUIRED" },
  },
};

/** HTTP 429 body for a personal key over its per-minute or daily limit (2.18.0); the AUTH_REQUIRED shape plus retryAfterSeconds. */
function keyRateLimitedBody(retryAfterSeconds: number) {
  return {
    jsonrpc: "2.0",
    id: null,
    error: {
      code: -32029,
      message: "Key rate limit reached.",
      data: { code: "KEY_RATE_LIMITED", retryAfterSeconds },
    },
  };
}

function rpcError(record: RequestRecord, status: number, code: number, message: string, dataCode: string, cors: Record<string, string>): Response {
  return recordedJson(record, { jsonrpc: "2.0", id: null, error: { code, message, data: { code: dataCode } } }, status, {}, cors);
}

/** `grace` (the default) serves requests without a credential; anything else enforces. */
function authEnforced(env: Env): boolean {
  const mode = typeof env.MCP_AUTH_MODE === "string" ? env.MCP_AUTH_MODE.trim().toLowerCase() : "";
  return mode !== "" && mode !== "grace";
}

function isToolCall(message: unknown): boolean {
  return typeof message === "object" && message !== null && (message as { method?: unknown }).method === "tools/call";
}

function json(data: unknown, status = 200, extraHeaders: Record<string, string> = {}, cors: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...cors, ...extraHeaders },
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Populate module-level env store so response wrappers and tools
    // can read Cloudflare runtime bindings (e.g. MCP_ENVELOPE_V2).
    setWorkerEnv({
      ...env,
      WORKER_VERSION_ID: env.CF_VERSION_METADATA?.id,
    });
    setEvidenceBucket(env.EVIDENCE_BUCKET);
    markYahooCacheActivity();
    const { method } = request;
    const { pathname } = new URL(request.url);

    // CORS preflight
    if (method === "OPTIONS") {
      const route: CorsRoute = pathname === "/" || pathname === "/health" ? "public" : isMcpPath(pathname) ? "mcp" : "none";
      return new Response(null, { status: 204, headers: corsHeadersFor(request, route) });
    }

    // Health check
    if (pathname === "/" || pathname === "/health") {
      return json({
        name: "yahoo-finance-mcp",
        status: "ok",
        transport: "streamable-http",
        serverVersion: getServerVersion(),
        buildVersion: getBuildVersion(),
        buildSha: getWorkerVar("BUILD_SHA")?.trim() || null,
        deployedAt: getWorkerVar("DEPLOYED_AT")?.trim() || null,
        workerVersionId: env.CF_VERSION_METADATA?.id ?? null,
        yahooCache: yahooCacheStats(),
        secDocumentCache: secDocumentCacheStats(),
      }, 200, {}, corsHeadersFor(request, "public"));
    }

    // Audit endpoint
    if (pathname === "/audit/mcp") {
      if (method !== "GET") {
        return new Response("Method Not Allowed", { status: 405 });
      }
      const token = getWorkerVar("AUDIT_TOKEN");
      if (!token) {
        return new Response("Not Found", { status: 404 });
      }
      const authHeader = request.headers.get("Authorization") ?? "";
      if (!timingSafeEqual(authHeader, `Bearer ${token}`)) {
        return new Response("Unauthorized", { status: 401 });
      }
      return json(await runAudit());
    }

    // MCP endpoint: /mcp, or /mcp/<token> with the credential in the path
    const credential = resolveCredential(request, pathname, env);
    if (credential.route === "mcp") {
      // One telemetry data point per /mcp request, rejections included (2.11.1).
      const startedAt = Date.now();
      const record = newRequestRecord(credential.transport);
      let response: Response;
      try {
        // A failing key store or usage counter throws here: fail closed (500, outcome exception).
        const caller = await identifyCaller(request, pathname, env, credential.status);
        response = await serveMcp(request, env, caller, record);
      } catch (error) {
        record.outcome = "exception";
        record.httpStatus = 500;
        record.body = null;
        writeRequestDataPoint(env.TELEMETRY, record, startedAt);
        throw error;
      }
      record.httpStatus = response.status;
      writeRequestDataPoint(env.TELEMETRY, record, startedAt);
      return response;
    }

    return new Response("Not Found", { status: 404 });
  },
};

/** Like json(), and keeps the body text on the telemetry record (measured only when the binding is present). */
function recordedJson(
  record: RequestRecord,
  data: unknown,
  status: number,
  extraHeaders: Record<string, string>,
  cors: Record<string, string>,
): Response {
  record.body = JSON.stringify(data);
  return new Response(record.body, {
    status,
    headers: { "Content-Type": "application/json", ...cors, ...extraHeaders },
  });
}

function reject(record: RequestRecord, action: string, errorCode: string): RequestRecord {
  record.action = action;
  record.outcome = "rejected";
  record.errorCode = errorCode;
  return record;
}

async function serveMcp(request: Request, env: Env, caller: CallerIdentity, record: RequestRecord): Promise<Response> {
  const { method } = request;
  const cors = corsHeadersFor(request, "mcp");
  // Telemetry key id: `owner`, a personal key's id, or `-` when no key is identified.
  record.keyId = caller.keyId;
  // Auth first, so an unauthenticated client learns nothing about limits or methods.
  if (caller.kind === "rejected" || (caller.kind === "anonymous" && authEnforced(env))) {
    return recordedJson(reject(record, "rejected:auth", "AUTH_REQUIRED"), AUTH_REQUIRED_BODY, 401, { "WWW-Authenticate": 'Bearer realm="yahoo-finance-mcp"' }, cors);
  }
  if (caller.kind === "limited") {
    const retryAfter = caller.retryAfterSeconds;
    return recordedJson(reject(record, "rejected:key_limit", "KEY_RATE_LIMITED"), keyRateLimitedBody(retryAfter), 429, { "Retry-After": String(retryAfter) }, cors);
  }

  if (method !== "POST") {
    reject(record, "rejected:method", "").body = "Method Not Allowed";
    return new Response("Method Not Allowed", { status: 405, headers: cors });
  }

  const tooLarge = () => {
    const code = "PAYLOAD_TOO_LARGE";
    reject(record, "rejected:body_size", code);
    return rpcError(record, 413, -32600, `PAYLOAD_TOO_LARGE: request body exceeds ${MAX_BODY_BYTES} bytes.`, code, cors);
  };
  const declaredLength = Number(request.headers.get("Content-Length"));
  if (Number.isFinite(declaredLength) && declaredLength > MAX_BODY_BYTES) return tooLarge();
  const text = await request.text();
  if (new TextEncoder().encode(text).length > MAX_BODY_BYTES) return tooLarge();

  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    return recordedJson(
      reject(record, "rejected:invalid", "-32700"),
      { jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } },
      400,
      {},
      cors,
    );
  }

  if (Array.isArray(body)) {
    if (body.length === 0) {
      return recordedJson(reject(record, "rejected:invalid", "-32600"), { jsonrpc: "2.0", id: null, error: { code: -32600, message: "Invalid Request" } }, 400, {}, cors);
    }
    if (body.filter(isToolCall).length > MAX_TOOL_CALLS_PER_BATCH) {
      reject(record, "rejected:batch", "BATCH_LIMIT_EXCEEDED");
      return rpcError(
        record,
        400, -32600,
        `BATCH_LIMIT_EXCEEDED: at most ${MAX_TOOL_CALLS_PER_BATCH} tools/call per request.`,
        "BATCH_LIMIT_EXCEEDED",
        cors,
      );
    }
  }

  const { action, domain } = classifyBody(body);
  record.action = action;
  record.domain = domain;
  logDispatchStart(action, domain);

  try {
    const authorizationHeaderPresent = (request.headers.get("Authorization") ?? "").trim() !== "";
    const { result, scope } = await withRequestFacts(
      { authorizationHeaderPresent, credentialTransport: record.credentialTransport, paidProvidersAllowed: caller.paidProviders },
      () => withCacheScope(() => {
        record.scope = currentRootScope();
        return handleMcp(body);
      }),
    );

    // Null means the request was notification-only — no response body needed
    if (result === null) {
      return new Response(null, { status: 202, headers: cors });
    }

    const { outcome, errorCode } = classifyResult(result);
    record.outcome = outcome;
    record.errorCode = errorCode;
    // Where this request's Yahoo and SEC data came from: process
    // memory, a request already in flight, the edge cache, or the
    // provider itself.
    return recordedJson(record, result, 200, {
      "X-Yahoo-Cache": formatCacheUsage(scope.caches.yahoo),
      "X-Sec-Cache": formatCacheUsage(scope.caches.sec),
    }, cors);
  } catch (e) {
    const message = e instanceof Error ? e.message : "Internal server error";
    record.outcome = "exception";
    record.errorCode = "-32603";
    return recordedJson(
      record,
      { jsonrpc: "2.0", id: null, error: { code: -32603, message } },
      500,
      {},
      cors,
    );
  }
}

async function runAudit(): Promise<Record<string, unknown>> {
  const healthRaw = await callTool("health_check", {});
  let health: Record<string, unknown> = {};
  try { health = JSON.parse(healthRaw) as Record<string, unknown>; } catch { /* ignore */ }
  return {
    auditVersion: "1",
    generatedAt: new Date().toISOString(),
    toolCount: TOOLS.length,
    tools: TOOLS.map(t => ({ name: t.name })),
    health,
  };
}
