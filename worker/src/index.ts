/**
 * Cloudflare Worker entry point for Yahoo Finance MCP server.
 *
 * Exposes a single MCP endpoint using the Streamable HTTP transport
 * (stateless — no Durable Objects required):
 *
 *   POST /mcp   →  MCP JSON-RPC request / batch
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

import { handleMcp } from "./mcp.js";

const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Mcp-Session-Id",
  "Access-Control-Max-Age": "86400",
};

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

export default {
  async fetch(request: Request): Promise<Response> {
    const { method } = request;
    const { pathname } = new URL(request.url);

    // CORS preflight
    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    // Health check
    if (pathname === "/" || pathname === "/health") {
      return json({ name: "yahoo-finance-mcp", status: "ok", transport: "streamable-http" });
    }

    // MCP endpoint
    if (pathname === "/mcp") {
      if (method !== "POST") {
        return new Response("Method Not Allowed", { status: 405, headers: CORS_HEADERS });
      }

      let body: unknown;
      try {
        body = await request.json();
      } catch {
        return json(
          { jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } },
          400
        );
      }

      try {
        const result = await handleMcp(body);

        // Null means the request was notification-only — no response body needed
        if (result === null) {
          return new Response(null, { status: 202, headers: CORS_HEADERS });
        }

        return json(result);
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
