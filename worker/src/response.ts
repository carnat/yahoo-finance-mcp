export const SERVER_VERSION = "1.0.0";

// Module-level Worker env store — populated by setWorkerEnv() in index.ts
// on each incoming request before tool dispatch.
//
// Note: although this module-level store is shared across concurrent requests,
// Cloudflare Worker [vars] bindings are static deploy-time values identical for
// every request to the same Worker instance, so concurrent overwrites are safe.
let _workerEnv: Record<string, string | undefined> = {};

export function setWorkerEnv(env: Record<string, string | undefined>): void {
  _workerEnv = env;
}

export function getWorkerVar(name: string): string | undefined {
  return _workerEnv[name];
}

export interface ToolMeta {
  tool: string;
  canonicalTool?: string;
  deprecatedTool?: boolean;
  useInstead?: string;
  partialSuccess?: boolean;
  successCount?: number;
  errorCount?: number;
  source: string;
  dataDate: string | null;
  serverVersion: string;
  cacheHit: boolean;
  warnings: unknown[];
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

function enrichFacts(val: any, parentSourceType: string | null = null, parentConfidence: string | null = null, isMetric = false): any {
  if (val && typeof val === "object") {
    if (Array.isArray(val)) {
      return val.map(item => enrichFacts(item, parentSourceType, parentConfidence, isMetric));
    }
    
    const isFact = ("value" in val) || ("low" in val) || ("high" in val) || ("valueRatio" in val) || ("valuePct" in val) || isMetric;
    
    if (isFact) {
      let conf = val.confidence || parentConfidence;
      if (!conf) {
        if ((val.value !== undefined && val.value !== null) || (val.low !== undefined && val.low !== null) || (val.valueRatio !== undefined && val.valueRatio !== null)) {
          conf = "HIGH";
        } else {
          conf = "NOT_DECISION_GRADE";
        }
      }
      conf = String(conf).toUpperCase();
      if (!["HIGH", "MEDIUM", "LOW", "NOT_DECISION_GRADE"].includes(conf)) {
        if (conf === "NOT_DISCLOSED") {
          conf = "NOT_DECISION_GRADE";
        } else {
          conf = "LOW";
        }
      }
      val.confidence = conf;
      val.sourceType = val.sourceType || parentSourceType || "yahoo";
      val.evidenceRequired = true;
      val.decisionGrade = ["HIGH", "MEDIUM"].includes(conf);
      
      const origEv = val.evidence;
      let evList: any[] = [];
      if (Array.isArray(origEv)) {
        evList = origEv;
      } else if (origEv) {
        evList = [origEv];
      }
      
      const standardisedEv = evList.map(ev => {
        if (ev && typeof ev === "object") {
          return {
            url: ev.url || ev.documentUrl || null,
            filingType: ev.filingType || null,
            accessionNumber: ev.accessionNumber || null,
            filingDate: ev.filingDate || null,
            tableIndex: ev.tableIndex || null,
            rowLabel: ev.rowLabel || null,
            columnLabel: ev.columnLabel || null,
            rawRow: ev.rawRow || null,
          };
        } else if (ev) {
          return {
            url: null,
            filingType: null,
            accessionNumber: null,
            filingDate: null,
            tableIndex: null,
            rowLabel: null,
            columnLabel: null,
            rawRow: String(ev),
          };
        }
        return null;
      }).filter(Boolean);
      val.evidence = standardisedEv.length > 0 ? standardisedEv : null;
    }
    
    const sourceType = val.source || val.sourceType || parentSourceType;
    const confidence = val.confidence || parentConfidence;
    
    for (const key of Object.keys(val)) {
      if (["confidence", "sourceType", "evidenceRequired", "decisionGrade", "evidence"].includes(key)) {
        continue;
      }
      const childIsMetric = isFact || ["metrics", "actual", "estimate", "revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex", "eps"].includes(key);
      val[key] = enrichFacts(val[key], sourceType, confidence, childIsMetric);
    }
  }
  return val;
}

export function mcpSuccess(
  tool: string,
  rawData: string,
  opts?: {
    canonicalTool?: string;
    deprecatedTool?: boolean;
    useInstead?: string;
    partialSuccess?: boolean;
    successCount?: number;
    errorCount?: number;
    source?: string;
    dataDate?: string | null;
    cacheHit?: boolean;
    warnings?: unknown[];
  }
): string {
  if (_workerEnv["MCP_ENVELOPE_V2"] !== "true") return rawData;
  let data: unknown;
  try {
    data = JSON.parse(rawData);
    data = enrichFacts(data);
  } catch {
    data = rawData;
  }
  const resp: McpResponse = {
    ok: true,
    data,
    meta: {
      tool,
      ...(opts?.canonicalTool ? { canonicalTool: opts.canonicalTool } : {}),
      ...(opts?.deprecatedTool != null ? { deprecatedTool: opts.deprecatedTool } : {}),
      ...(opts?.useInstead ? { useInstead: opts.useInstead } : {}),
      ...(opts?.partialSuccess != null ? { partialSuccess: opts.partialSuccess } : {}),
      ...(opts?.successCount != null ? { successCount: opts.successCount } : {}),
      ...(opts?.errorCount != null ? { errorCount: opts.errorCount } : {}),
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
  opts?: { source?: string; metaExtra?: Record<string, unknown>; diagnostics?: unknown }
): string {
  if (_workerEnv["MCP_ENVELOPE_V2"] !== "true") {
    return JSON.stringify({
      error: true,
      code,
      message,
      ...(opts?.metaExtra || {}),
      ...(opts?.diagnostics !== undefined ? { diagnostics: opts.diagnostics } : {}),
    });
  }
  const resp: any = {
    ok: false,
    data: null,
    meta: {
      tool,
      source: opts?.source ?? "yahoo_finance",
      dataDate: null,
      serverVersion: SERVER_VERSION,
      cacheHit: false,
      warnings: [],
      ...(opts?.metaExtra || {}),
    },
    error: {
      code,
      message,
      ...(opts?.metaExtra || {}),
    },
  };
  if (opts?.diagnostics !== undefined) {
    resp.diagnostics = opts.diagnostics;
  }
  return JSON.stringify(resp);
}
