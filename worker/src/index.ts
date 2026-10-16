/**
 * Cloudflare Worker entry point for Yahoo Finance MCP server.
 *
 * Exposes a single MCP endpoint using the Streamable HTTP transport
 * (stateless — no Durable Objects required):
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

import { resolveCredential, timingSafeEqual } from "./auth.js";
import { handleMcp } from "./mcp.js";
import { setEvidenceBucket } from "./evidence-store.js";
import { getBuildVersion, getServerVersion, setCacheSummaryProvider, setWorkerEnv, getWorkerVar } from "./response.js";
import { TOOLS, callTool } from "./tools.js";
import { currentCacheSummary, formatCacheUsage, withCacheScope, withRequestFacts } from "./request-context.js";
import { markYahooCacheActivity, secDocumentCacheStats, yahooCacheStats } from "./yahoo-finance.js";

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
  [key: string]: unknown;
}

setCacheSummaryProvider(currentCacheSummary);

const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Mcp-Session-Id",
  "Access-Control-Max-Age": "86400",
  "Access-Control-Expose-Headers": "X-Yahoo-Cache, X-Sec-Cache",
};

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

function rpcError(status: number, code: number, message: string, dataCode: string): Response {
  return json({ jsonrpc: "2.0", id: null, error: { code, message, data: { code: dataCode } } }, status);
}

/** `grace` (the default) serves requests without a credential; anything else enforces. */
function authEnforced(env: Env): boolean {
  const mode = typeof env.MCP_AUTH_MODE === "string" ? env.MCP_AUTH_MODE.trim().toLowerCase() : "";
  return mode !== "" && mode !== "grace";
}

function isToolCall(message: unknown): boolean {
  return typeof message === "object" && message !== null && (message as { method?: unknown }).method === "tools/call";
}

function json(data: unknown, status = 200, extraHeaders: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS, ...extraHeaders },
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
      return new Response(null, { status: 204, headers: CORS_HEADERS });
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
      });
    }

    // Audit endpoint
    if (pathname === "/audit/mcp") {
      if (method !== "GET") {
        return new Response("Method Not Allowed", { status: 405, headers: CORS_HEADERS });
      }
      const token = getWorkerVar("AUDIT_TOKEN");
      if (!token) {
        return new Response("Not Found", { status: 404, headers: CORS_HEADERS });
      }
      const authHeader = request.headers.get("Authorization") ?? "";
      if (!timingSafeEqual(authHeader, `Bearer ${token}`)) {
        return new Response("Unauthorized", { status: 401, headers: CORS_HEADERS });
      }
      return json(await runAudit());
    }

    // MCP endpoint: /mcp, or /mcp/<token> with the credential in the path
    const credential = resolveCredential(request, pathname, env);
    if (credential.route === "mcp") {
      // Auth first, so an unauthenticated client learns nothing about limits or methods.
      if (credential.status === "invalid" || (credential.status === "absent" && authEnforced(env))) {
        return json(AUTH_REQUIRED_BODY, 401, { "WWW-Authenticate": 'Bearer realm="yahoo-finance-mcp"' });
      }

      if (method !== "POST") {
        return new Response("Method Not Allowed", { status: 405, headers: CORS_HEADERS });
      }

      const tooLarge = () =>
        rpcError(413, -32600, `PAYLOAD_TOO_LARGE: request body exceeds ${MAX_BODY_BYTES} bytes.`, "PAYLOAD_TOO_LARGE");
      const declaredLength = Number(request.headers.get("Content-Length"));
      if (Number.isFinite(declaredLength) && declaredLength > MAX_BODY_BYTES) return tooLarge();
      const text = await request.text();
      if (new TextEncoder().encode(text).length > MAX_BODY_BYTES) return tooLarge();

      let body: unknown;
      try {
        body = JSON.parse(text);
      } catch {
        return json(
          { jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } },
          400
        );
      }

      if (Array.isArray(body)) {
        if (body.length === 0) {
          return json({ jsonrpc: "2.0", id: null, error: { code: -32600, message: "Invalid Request" } }, 400);
        }
        if (body.filter(isToolCall).length > MAX_TOOL_CALLS_PER_BATCH) {
          return rpcError(
            400, -32600,
            `BATCH_LIMIT_EXCEEDED: at most ${MAX_TOOL_CALLS_PER_BATCH} tools/call per request.`,
            "BATCH_LIMIT_EXCEEDED",
          );
        }
      }

      try {
        const authorizationHeaderPresent = (request.headers.get("Authorization") ?? "").trim() !== "";
        const { result, scope } = await withRequestFacts(
          { authorizationHeaderPresent, credentialTransport: credential.transport },
          () => withCacheScope(() => handleMcp(body)),
        );

        // Null means the request was notification-only — no response body needed
        if (result === null) {
          return new Response(null, { status: 202, headers: CORS_HEADERS });
        }

        // Where this request's Yahoo and SEC data came from: process
        // memory, a request already in flight, the edge cache, or the
        // provider itself.
        return json(result, 200, {
          "X-Yahoo-Cache": formatCacheUsage(scope.caches.yahoo),
          "X-Sec-Cache": formatCacheUsage(scope.caches.sec),
        });
      } catch (e) {
        const message = e instanceof Error ? e.message : "Internal server error";
        return json(
          { jsonrpc: "2.0", id: null, error: { code: -32603, message } },
          500
        );
      }
    }

    return new Response("Not Found", { status: 404 });
  },
};

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
