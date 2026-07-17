/**
 * MCP Streamable HTTP protocol handler (stateless, no sessions).
 *
 * Implements the JSON-RPC 2.0 layer of the MCP spec:
 *   https://modelcontextprotocol.io/specification/2025-06-18/
 *
 * Supports: initialize, ping, tools/list, tools/call
 * Notifications (no `id` field) are accepted but produce no response body.
 */

import { getServerVersion } from "./response.js";
import { callVisibleTool, listVisibleTools } from "./tools.js";

interface JsonRpcRequest {
  jsonrpc: "2.0";
  id: string | number;
  method: string;
  params?: unknown;
}

interface JsonRpcNotification {
  jsonrpc: "2.0";
  method: string;
  params?: unknown;
  // no `id`
}

type McpMessage = JsonRpcRequest | JsonRpcNotification;

const isNotification = (msg: McpMessage): msg is JsonRpcNotification => !("id" in msg);

// ── Public entry point ───────────────────────────────────────────────────────

/**
 * Handle a single MCP message or a batch array.
 * Returns `null` for pure-notification inputs (caller should respond 202).
 */
export async function handleMcp(body: unknown): Promise<unknown> {
  if (Array.isArray(body)) {
    const results = await Promise.all((body as McpMessage[]).map(handleMessage));
    const responses = results.filter((r) => r !== null);
    return responses.length > 0 ? responses : null;
  }
  return handleMessage(body as McpMessage);
}

// ── Internal ─────────────────────────────────────────────────────────────────

async function handleMessage(msg: McpMessage): Promise<unknown> {
  if (isNotification(msg)) {
    // Fire-and-forget: notifications/initialized, notifications/cancelled, etc.
    return null;
  }

  const req = msg as JsonRpcRequest;
  try {
    const result = await dispatch(req.method, req.params);
    return { jsonrpc: "2.0", id: req.id, result };
  } catch (e) {
    const code = (e as { code?: number }).code ?? -32603;
    const message = e instanceof Error ? e.message : "Internal error";
    return { jsonrpc: "2.0", id: req.id, error: { code, message } };
  }
}

async function dispatch(method: string, params: unknown): Promise<unknown> {
  switch (method) {
    case "initialize": {
      const requestedVersion = (params as { protocolVersion?: unknown } | null)?.protocolVersion;
      const supportedVersions = new Set(["2025-06-18", "2024-11-05"]);
      const protocolVersion = typeof requestedVersion === "string" && supportedVersions.has(requestedVersion)
        ? requestedVersion
        : "2025-06-18";
      return {
        protocolVersion,
        serverInfo: { name: "yahoo-finance-mcp", version: getServerVersion() },
        capabilities: { tools: {} },
      };
    }

    case "ping":
      return {};

    case "tools/list":
      return { tools: listVisibleTools() };

    case "tools/call": {
      const p = params as { name?: string; arguments?: Record<string, unknown> };
      if (!p?.name) throw Object.assign(new Error("Missing tool name"), { code: -32602 });

      const text = await callVisibleTool(p.name, p.arguments ?? {});
      let parsed: unknown = null;
      try { parsed = JSON.parse(text); } catch { /* text-only legacy payload */ }
      const structuredContent = parsed != null && typeof parsed === "object" && !Array.isArray(parsed)
        ? parsed as Record<string, unknown>
        : parsed !== null
          ? { result: parsed }
          : null;
      const isError = structuredContent != null && (
        structuredContent.ok === false || structuredContent.error === true
      );
      return {
        content: [{ type: "text", text }],
        ...(structuredContent ? { structuredContent } : {}),
        ...(isError ? { isError: true } : {}),
      };
    }

    default:
      throw Object.assign(new Error(`Method not found: ${method}`), { code: -32601 });
  }
}
