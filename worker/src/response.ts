export const SERVER_VERSION = "1.0.0";

export interface ToolMeta {
  tool: string;
  source: string;
  dataDate: string | null;
  serverVersion: string;
  cacheHit: boolean;
  warnings: string[];
}

export interface ErrorDetail {
  code: string;
  message: string;
}

export interface McpResponse<T = unknown> {
  ok: boolean;
  data: T | null;
  meta: ToolMeta;
  error: ErrorDetail | null;
}

export const ErrorCode = {
  TICKER_NOT_FOUND: "TICKER_NOT_FOUND",
  NO_OPTIONS_DATA: "NO_OPTIONS_DATA",
  NO_FILING_DATA: "NO_FILING_DATA",
  PROVIDER_ERROR: "PROVIDER_ERROR",
  PROVIDER_TIMEOUT: "PROVIDER_TIMEOUT",
  RATE_LIMIT: "RATE_LIMIT",
  INPUT_VALIDATION_ERROR: "INPUT_VALIDATION_ERROR",
  DEPRECATED_TOOL: "DEPRECATED_TOOL",
} as const;

export type ErrorCodeValue = (typeof ErrorCode)[keyof typeof ErrorCode];

// Cloudflare Workers env binding — read at module init time.
function _isEnvelopeV2(): boolean {
  try {
    const g = globalThis as unknown as Record<string, unknown>;
    return typeof g["MCP_ENVELOPE_V2"] === "string" && g["MCP_ENVELOPE_V2"] === "true";
  } catch {
    return false;
  }
}

const ENVELOPE_V2 = _isEnvelopeV2();

export function mcpSuccess(
  tool: string,
  rawData: string,
  opts?: {
    source?: string;
    dataDate?: string | null;
    cacheHit?: boolean;
    warnings?: string[];
  }
): string {
  if (!ENVELOPE_V2) return rawData;
  let data: unknown;
  try {
    data = JSON.parse(rawData);
  } catch {
    data = rawData;
  }
  const resp: McpResponse = {
    ok: true,
    data,
    meta: {
      tool,
      source: opts?.source ?? "yahoo_finance",
      dataDate: opts?.dataDate ?? null,
      serverVersion: SERVER_VERSION,
      cacheHit: opts?.cacheHit ?? false,
      warnings: opts?.warnings ?? [],
    },
    error: null,
  };
  return JSON.stringify(resp);
}

export function mcpFailure(
  tool: string,
  code: string,
  message: string,
  opts?: { source?: string }
): string {
  const resp: McpResponse = {
    ok: false,
    data: null,
    meta: {
      tool,
      source: opts?.source ?? "yahoo_finance",
      dataDate: null,
      serverVersion: SERVER_VERSION,
      cacheHit: false,
      warnings: [],
    },
    error: { code, message },
  };
  if (!ENVELOPE_V2) return JSON.stringify({ error: true, code, message });
  return JSON.stringify(resp);
}
