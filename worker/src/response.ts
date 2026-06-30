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
  capabilityStatus?: string;
  decisionGrade?: boolean;
  doctrineUse?: string;
  failureMode?: string | null;
  evidenceRequired?: boolean;
  sourceType?: string;
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

function buildMeta(
  tool: string,
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
    metaExtra?: Record<string, unknown>;
  }
): ToolMeta {
  return {
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
    ...(opts?.metaExtra || {}),
  } as ToolMeta;
}

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

      const origEv = val.evidence;
      let evList: any[] = [];
      if (Array.isArray(origEv)) {
        evList = origEv;
      } else if (origEv) {
        evList = [origEv];
      }
      
      const urls: string[] = [];
      const standardisedEv = evList.map(ev => {
        if (ev && typeof ev === "object") {
          const url = ev.url || ev.documentUrl || null;
          if (url) urls.push(String(url));
          return {
            url,
            filingType: ev.filingType || null,
            accessionNumber: ev.accessionNumber || null,
            filingDate: ev.filingDate || null,
            tableIndex: ev.tableIndex || null,
            rowLabel: ev.rowLabel || null,
            columnLabel: ev.columnLabel || null,
            rawRow: ev.rawRow || null,
          };
        } else if (ev) {
          const evStr = String(ev);
          if (evStr.startsWith("http")) urls.push(evStr);
          return {
            url: evStr.startsWith("http") ? evStr : null,
            filingType: null,
            accessionNumber: null,
            filingDate: null,
            tableIndex: null,
            rowLabel: null,
            columnLabel: null,
            rawRow: evStr,
          };
        }
        return null;
      }).filter(Boolean);
      val.evidence = standardisedEv.length > 0 ? standardisedEv : null;

      let inferredSourceType: string | null = null;
      for (const url of urls) {
        const urlLower = url.toLowerCase();
        if (urlLower.includes("sec.gov")) {
          if (urlLower.includes("companyfacts") || urlLower.includes("submissions") || urlLower.includes(".xml")) {
            inferredSourceType = "sec_xbrl";
            break;
          } else if (urlLower.includes("ix?doc=") || urlLower.includes("index")) {
            inferredSourceType = "sec_table";
            break;
          } else {
            inferredSourceType = "sec_filing";
            break;
          }
        } else if (urlLower.includes("yahoo.com")) {
          inferredSourceType = "yahoo";
          break;
        } else if (urlLower.includes("ir.") || urlLower.includes("investor")) {
          inferredSourceType = "company_ir";
          break;
        } else if (urlLower.startsWith("http")) {
          inferredSourceType = "unknown";
          break;
        }
      }

      if (!inferredSourceType) {
        inferredSourceType = parentSourceType;
      }

      if (!inferredSourceType) {
        const extMethod = String(val.extractionMethod || "").toUpperCase();
        if (extMethod.includes("XBRL") || extMethod.includes("COMPANYFACTS")) {
          inferredSourceType = "sec_xbrl";
        } else if (extMethod.includes("HTML") || extMethod.includes("TABLE")) {
          inferredSourceType = "sec_table";
        } else if (extMethod.includes("TEXT")) {
          inferredSourceType = "sec_filing";
        }
      }

      if (!inferredSourceType) {
        inferredSourceType = "unknown";
      }

      val.sourceType = val.sourceType || inferredSourceType;
      val.evidenceRequired = true;
      val.decisionGrade = ["HIGH", "MEDIUM"].includes(conf);
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
    metaExtra?: Record<string, unknown>;
  }
): string {
  if (_workerEnv["MCP_ENVELOPE_V2"] !== "true") return rawData;
  let data: unknown;
  try {
    const parsed = JSON.parse(rawData);
    if (
      parsed != null &&
      typeof parsed === "object" &&
      typeof (parsed as Record<string, unknown>).ok === "boolean" &&
      ("data" in parsed || "error" in parsed)
    ) {
      const inner = parsed as Record<string, unknown>;
      const innerMeta = inner.meta && typeof inner.meta === "object"
        ? inner.meta as Record<string, unknown>
        : {};
      const baseMeta = buildMeta(tool, opts);
      const baseWarnings = Array.isArray(baseMeta.warnings) ? baseMeta.warnings : [];
      const innerWarnings = Array.isArray(innerMeta.warnings) ? innerMeta.warnings : [];
      const resp: McpResponse = {
        ok: inner.ok === true,
        data: inner.ok === true ? enrichFacts(inner.data) : inner.data ?? null,
        meta: {
          ...baseMeta,
          ...innerMeta,
          ...(opts?.metaExtra || {}),
          warnings: [...baseWarnings, ...innerWarnings],
        } as ToolMeta,
        error: inner.ok === true ? null : (inner.error as ErrorDetail | null) ?? {
          code: "PROVIDER_ERROR",
          message: "Tool returned an error envelope without error details.",
        },
      };
      const passthrough = inner as Record<string, unknown>;
      for (const key of ["diagnostics"]) {
        if (passthrough[key] !== undefined) {
          (resp as any)[key] = passthrough[key];
        }
      }
      return JSON.stringify(resp);
    }
    if (parsed != null && typeof parsed === "object" && (parsed as Record<string, unknown>).error === true) {
      const inner = parsed as Record<string, unknown>;
      const errorCode = typeof inner.code === "string" ? inner.code : "PROVIDER_ERROR";
      const errorMessage = typeof inner.message === "string"
        ? inner.message
        : "Tool returned a legacy error envelope without error details.";
      const resp: McpResponse = {
        ok: false,
        data: null,
        meta: buildMeta(tool, opts),
        error: {
          code: errorCode,
          message: errorMessage,
        },
      };
      for (const key of ["diagnostics"]) {
        if (inner[key] !== undefined) {
          (resp as any)[key] = inner[key];
        }
      }
      return JSON.stringify(resp);
    }
    data = parsed;
    data = enrichFacts(data);
  } catch {
    data = rawData;
  }
  const resp: McpResponse = {
    ok: true,
    data,
    meta: buildMeta(tool, opts),
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
