export const UNCONFIGURED_SERVER_VERSION = "0.0.0+unconfigured";

// Module-level Worker env store — populated by setWorkerEnv() in index.ts
// on each incoming request before tool dispatch.
//
// Note: although this module-level store is shared across concurrent requests,
// Cloudflare Worker [vars] bindings are static deploy-time values identical for
// every request to the same Worker instance, so concurrent overwrites are safe.
let _workerEnv: Record<string, string | undefined> = {};

export function setWorkerEnv(env: Record<string, unknown>): void {
  _workerEnv = Object.fromEntries(
    Object.entries(env).filter((entry): entry is [string, string] => typeof entry[1] === "string")
  );
}

/**
 * The key-gated provider credentials (Finnhub, Marketaux, Alpha Vantage). For a
 * personal access key without `paidProviders` (2.18.0) they read as unset, so
 * every existing "unconfigured provider" path applies unchanged.
 */
const KEY_GATED_PROVIDER_VARS: ReadonlySet<string> = new Set([
  "FINNHUB_API_KEY",
  "FINNHUB_TOKEN",
  "MARKETAUX_API_TOKEN",
  "MARKETAUX_API_KEY",
  "ALPHA_VANTAGE_API_KEY",
  "ALPHAVANTAGE_API_KEY",
]);

// Whether the current request may read the key-gated provider credentials.
// index.ts registers request-context's paidProvidersAllowed (this module stays
// import-free); until then, and outside a request, the answer is yes.
let paidProvidersAllowed: () => boolean = () => true;

export function setPaidProvidersProvider(provider: () => boolean): void {
  paidProvidersAllowed = provider;
}

export function getWorkerVar(name: string): string | undefined {
  if (KEY_GATED_PROVIDER_VARS.has(name) && !paidProvidersAllowed()) return undefined;
  return _workerEnv[name];
}

/** V2 envelope is the default since 2.0.0; any explicit value other than "true" opts out. */
export function envelopeV2Enabled(): boolean {
  return (getWorkerVar("MCP_ENVELOPE_V2") ?? "true") === "true";
}

export function getServerVersion(): string {
  return getWorkerVar("SERVER_VERSION")?.trim() || UNCONFIGURED_SERVER_VERSION;
}

export function getBuildVersion(): string {
  const explicit = getWorkerVar("BUILD_VERSION")?.trim();
  if (explicit) return explicit;
  const sha = getWorkerVar("BUILD_SHA")?.trim();
  const shortSha = sha && /^[0-9a-f]{7,64}$/i.test(sha) ? sha.slice(0, 7).toLowerCase() : null;
  return shortSha ? `${getServerVersion()}+git.${shortSha}` : getServerVersion();
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
  workerVersionId?: string;
  cacheHit: boolean;
  cacheSource?: string | null;
  warnings: unknown[];
  capabilityStatus?: string;
  reliabilityClass?: string;
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
  // The per-request subrequest budget was spent before this work could run (2.11.0).
  PROVIDER_BUDGET_EXHAUSTED: "PROVIDER_BUDGET_EXHAUSTED",
  // A batch array longer than the action's tool_catalog.json tickerCaps entry, rejected before any fetch (2.12.0).
  BATCH_LIMIT_EXCEEDED: "BATCH_LIMIT_EXCEEDED",
  // An explicit accession_number that is not among the company's recent SEC filings; never falls back (2.13.0).
  ACCESSION_NOT_FOUND: "ACCESSION_NOT_FOUND",
} as const;

export type ErrorCodeValue = (typeof ErrorCode)[keyof typeof ErrorCode];

// Where the current tool call's provider data came from. index.ts installs
// the request-context implementation; this module stays import-free so it
// loads on its own (as the Node-based envelope tests do).
type CacheSummary = { cacheHit: boolean; cacheSource: string | null };
let cacheSummary: () => CacheSummary = () => ({ cacheHit: false, cacheSource: null });

export function setCacheSummaryProvider(provider: () => CacheSummary): void {
  cacheSummary = provider;
}

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
  const workerVersionId = getWorkerVar("WORKER_VERSION_ID");
  const cache = cacheSummary();
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
    serverVersion: getServerVersion(),
    cacheHit: opts?.cacheHit ?? cache.cacheHit,
    cacheSource: cache.cacheSource,
    warnings: opts?.warnings ?? [],
    ...(opts?.metaExtra || {}),
    ...(workerVersionId ? { workerVersionId } : {}),
  } as ToolMeta;
}

/**
 * meta.dataDate from the payload's own dataDate (YYYY-MM-DD), so envelopes
 * say which date the data describes without reading the data.
 */
function payloadDataDate(data: unknown): string | null {
  if (data == null || typeof data !== "object" || Array.isArray(data)) return null;
  const value = (data as Record<string, unknown>).dataDate;
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}/.test(value) ? value.slice(0, 10) : null;
}

function isPriceBar(val: Record<string, unknown>): boolean {
  return "open" in val && "high" in val && "low" in val && "close" in val;
}

/**
 * Table evidence rows as cell strings (2.14.0). A flat list of strings is one row;
 * a list of lists is several rows. Other shapes give no rows.
 */
function sourceRowsAsCells(sourceRows: unknown): string[][] {
  if (!Array.isArray(sourceRows) || sourceRows.length === 0) return [];
  const cell = (c: unknown): string => (c === null || c === undefined ? "" : String(c));
  if (sourceRows.every((r) => typeof r === "string")) return [sourceRows.map(cell)];
  if (sourceRows.every((r) => Array.isArray(r))) return (sourceRows as unknown[][]).map((r) => r.map(cell));
  return [];
}

// ── Confidence levels (3.0.0, F20) ──────────────────────────────────────────
// confidence holds only HIGH, MEDIUM or LOW, or null when there is no value to
// be confident in. A status string that producers used to carry in confidence
// (EXTRACTION_FAILED, NOT_DISCLOSED, ...) moves to the object's status when it
// has none. Python mirrors this in yfmcp/envelope.py.

export const CONFIDENCE_LEVELS: ReadonlySet<string> = new Set(["HIGH", "MEDIUM", "LOW"]);

/** Tools whose payload is a stored evidence cut, returned verbatim so its SHA-256 still verifies. */
export const VERBATIM_CONFIDENCE_TOOLS: ReadonlySet<string> = new Set(["get_evidence_cut"]);

/**
 * Rewrites, in place, every `confidence` string that is not a level: it
 * becomes null, and the string moves to `status` when the object has no
 * `status` key. Returns the value it was given.
 */
export function normalizeStatusConfidence<T>(value: T, depth = 0): T {
  if (depth > 64 || value == null || typeof value !== "object") return value;
  if (Array.isArray(value)) {
    for (const item of value) normalizeStatusConfidence(item, depth + 1);
    return value;
  }
  const rec = value as Record<string, unknown>;
  const conf = rec.confidence;
  if (typeof conf === "string" && !CONFIDENCE_LEVELS.has(conf)) {
    if (!Object.prototype.hasOwnProperty.call(rec, "status") && conf !== "") rec.status = conf;
    rec.confidence = null;
  }
  for (const inner of Object.values(rec)) {
    if (inner != null && typeof inner === "object") normalizeStatusConfidence(inner, depth + 1);
  }
  return value;
}

function enrichFacts(val: any, parentSourceType: string | null = null, parentConfidence: string | null = null, isMetric = false): any {
  if (val && typeof val === "object") {
    if (Array.isArray(val)) {
      return val.map(item => enrichFacts(item, parentSourceType, parentConfidence, isMetric));
    }
    
    // OHLC price bars carry high/low keys but are raw exchange prices, not
    // extracted facts that need evidence tagging.
    if (isPriceBar(val)) return val;
    // Evidence payloads (2.5.0) are returned verbatim: an evidence cut's hash
    // covers exactly these fields, and provider figures carry their own states.
    if (val.decisionUse === "EVIDENCE_ONLY" || String(val.schema ?? "").startsWith("yfmcp.evidence")) return val;
    const isFact = ("value" in val) || ("low" in val) || ("high" in val) || ("valueRatio" in val) || ("valuePct" in val) || isMetric;
    
    if (isFact) {
      const hasExplicitDecisionGrade = Object.prototype.hasOwnProperty.call(val, "decisionGrade");
      const ownConf = val.confidence;
      let conf: string | null = ownConf || parentConfidence;
      if (!conf) {
        // An untagged fact with a value is LOW (2.21.0): a producer tags what is genuinely HIGH.
        // Without a value there is no confidence to state (3.0.0, F20).
        const hasValue = (val.value !== undefined && val.value !== null) || (val.low !== undefined && val.low !== null) || (val.valueRatio !== undefined && val.valueRatio !== null);
        conf = hasValue ? "LOW" : null;
      }
      if (conf != null) {
        conf = String(conf).toUpperCase();
        if (!CONFIDENCE_LEVELS.has(conf)) {
          // A status string in confidence (F20): it moves to status, and confidence is null.
          if (typeof ownConf === "string" && ownConf !== "" && !Object.prototype.hasOwnProperty.call(val, "status")) val.status = ownConf;
          conf = null;
        }
      }
      val.confidence = conf;

      const urls: string[] = [];
      const sourceEvidence = val.sourceEvidence;
      const preserveDecisionGradeXbrlEvidence =
        val.decisionGrade === true
        && sourceEvidence
        && typeof sourceEvidence === "object"
        && !Array.isArray(sourceEvidence)
        && String(val.extractionMethod || "").toUpperCase().includes("XBRL");
      if (preserveDecisionGradeXbrlEvidence) {
        val.evidence = { ...sourceEvidence };
        const url = sourceEvidence.url || sourceEvidence.documentUrl;
        if (url) urls.push(String(url));
      } else {
        const origEv = val.evidence;
        let evList: any[] = [];
        if (Array.isArray(origEv)) {
          evList = origEv;
        } else if (origEv) {
          evList = [origEv];
        }

        // The 14-key row, in Python's order (2.14.0). Table evidence (sourceTableId / sourceRows) fills
        // tableIndex, rowLabel and rawRow when the raw object has none of its own.
        const standardisedEv = evList.map(ev => {
          if (ev && typeof ev === "object") {
            const url = ev.url || ev.documentUrl || null;
            if (url) urls.push(String(url));
            // sourceRows is either one flat row of cells (string[]) or a list of rows (cell[][]).
            // rowLabel = first cell of the first row; rawRow = cells joined " | ", rows joined "; ".
            const tableRows = sourceRowsAsCells(ev.sourceRows);
            const firstSourceRow = tableRows.length > 0 && tableRows[0][0] ? tableRows[0][0] : null;
            const joinedSourceRows = tableRows.length > 0
              ? tableRows.map((row) => row.join(" | ")).join("; ") || null
              : null;
            return {
              url,
              sourceType: ev.sourceType || null,
              filingType: ev.filingType || null,
              accessionNumber: ev.accessionNumber || null,
              filingDate: ev.filingDate || null,
              publishedAt: ev.publishedAt || null,
              retrievedAt: ev.retrievedAt || null,
              excerpt: ev.excerpt || null,
              concept: ev.concept || null,
              periodEnd: ev.periodEnd || null,
              tableIndex: ev.tableIndex || ev.sourceTableId || null,
              rowLabel: ev.rowLabel || firstSourceRow || null,
              columnLabel: ev.columnLabel || null,
              rawRow: ev.rawRow || joinedSourceRows || null,
            };
          } else if (ev) {
            const evStr = String(ev);
            if (evStr.startsWith("http")) urls.push(evStr);
            return {
              url: evStr.startsWith("http") ? evStr : null,
              sourceType: null,
              filingType: null,
              accessionNumber: null,
              filingDate: null,
              publishedAt: null,
              retrievedAt: null,
              excerpt: null,
              concept: null,
              periodEnd: null,
              tableIndex: null,
              rowLabel: null,
              columnLabel: null,
              rawRow: evStr,
            };
          }
          return null;
        // A row with no populated field is not evidence (an empty `{}` on a
        // fail-closed result); dropping it leaves evidence null.
        }).filter((row) => row && Object.values(row).some((v) => v !== null));
        val.evidence = standardisedEv.length > 0 ? standardisedEv : null;
      }

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
      if (!hasExplicitDecisionGrade) val.decisionGrade = false;
    }
    
    const sourceType = val.source || val.sourceType || parentSourceType;
    // A fact passes its own resolved confidence (null included) to its children.
    const confidence = isFact ? val.confidence : (val.confidence || parentConfidence);
    
    for (const key of Object.keys(val)) {
      if (["confidence", "sourceType", "evidenceRequired", "decisionGrade", "evidence", "sourceEvidence", "xbrlContext", "calculation"].includes(key)) {
        continue;
      }
      const childIsMetric = isFact || ["metrics", "actual", "estimate", "revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex", "eps"].includes(key);
      val[key] = enrichFacts(val[key], sourceType, confidence, childIsMetric);
    }
  }
  return val;
}

/**
 * A tool response as sent to the client (`text`) plus, when available, the
 * value that `text` serializes. Callers that need the object form (the MCP
 * layer's structuredContent) use `value` instead of parsing `text` again.
 */
export interface ToolResult {
  text: string;
  value?: unknown;
}

export type McpSuccessOptions = {
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
  /**
   * Notices appended to meta.warnings on a successful response only, from the
   * success payload (2.8.0 DEPRECATED_FIELD). Never applied to failures or the
   * legacy envelope, and never written into `data`.
   */
  successWarnings?: (data: unknown) => unknown[];
};

function toolResult(value: unknown): ToolResult {
  return { text: JSON.stringify(value), value };
}

/** Wrap a tool payload string in the V2 envelope. */
/**
 * The code for an error payload that carries only a message: a provider 429 is
 * a retryable RATE_LIMIT and a timeout a retryable PROVIDER_TIMEOUT, as they
 * are for thrown errors. Null when the message names neither.
 */
export function classifyErrorMessage(message: string): { code: string; retryable: boolean } | null {
  const lower = message.toLowerCase();
  if (lower.includes("rate limit") || /\b(?:error|http|status)[:\s]*429\b/.test(lower)) return { code: "RATE_LIMIT", retryable: true };
  if (lower.includes("timeout") || lower.includes("timed out")) return { code: "PROVIDER_TIMEOUT", retryable: true };
  return null;
}

export function mcpSuccess(tool: string, rawData: string, opts?: McpSuccessOptions): string {
  return mcpSuccessResult(tool, rawData, opts).text;
}

/** The F20 rule over a whole payload, except a stored evidence cut (returned verbatim). */
function confidenceLevels<T>(tool: string, data: T): T {
  return VERBATIM_CONFIDENCE_TOOLS.has(tool) ? data : normalizeStatusConfidence(data);
}

/** The legacy envelope's payload text with the F20 rule applied; unchanged when there is nothing to rewrite. */
function legacyConfidenceText(tool: string, rawData: string): string {
  if (VERBATIM_CONFIDENCE_TOOLS.has(tool) || !rawData.includes('"confidence"')) return rawData;
  try {
    const parsed = JSON.parse(rawData);
    const before = JSON.stringify(parsed);
    const after = JSON.stringify(normalizeStatusConfidence(parsed));
    return after === before ? rawData : after;
  } catch {
    return rawData;
  }
}

export function mcpSuccessResult(tool: string, rawData: string, opts?: McpSuccessOptions): ToolResult {
  if (!envelopeV2Enabled()) return { text: legacyConfidenceText(tool, rawData) };
  let parsed: unknown;
  try {
    parsed = JSON.parse(rawData);
  } catch {
    return toolResult({ ok: true, data: rawData, meta: buildMeta(tool, opts), error: null } satisfies McpResponse);
  }
  return mcpSuccessFromValue(tool, parsed, rawData, opts);
}

/**
 * Wrap an already-parsed tool payload. `rawData` is the payload's source
 * text, used as the data if enrichment fails, as mcpSuccess always did.
 */
export function mcpSuccessFromValue(
  tool: string,
  parsed: unknown,
  rawData: string,
  opts?: McpSuccessOptions,
): ToolResult {
  if (!envelopeV2Enabled()) return { text: legacyConfidenceText(tool, rawData) };
  let data: unknown;
  try {
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
      const baseMeta = buildMeta(tool, { ...opts, dataDate: opts?.dataDate ?? payloadDataDate(inner.data) });
      const baseWarnings = Array.isArray(baseMeta.warnings) ? baseMeta.warnings : [];
      const innerWarnings = Array.isArray(innerMeta.warnings) ? innerMeta.warnings : [];
      const workerVersionId = baseMeta.workerVersionId;
      const resp: McpResponse = {
        ok: inner.ok === true,
        data: inner.ok === true ? confidenceLevels(tool, enrichFacts(inner.data)) : inner.data ?? null,
        meta: {
          ...baseMeta,
          ...innerMeta,
          ...(opts?.metaExtra || {}),
          // The inner envelope was built before the tool finished; its
          // cache summary may be incomplete.
          cacheHit: baseMeta.cacheHit,
          cacheSource: baseMeta.cacheSource,
          dataDate: (innerMeta.dataDate as string | null | undefined) ?? baseMeta.dataDate,
          warnings: [
            ...baseWarnings,
            ...innerWarnings,
            ...(inner.ok === true && opts?.successWarnings ? opts.successWarnings(inner.data) : []),
          ],
          ...(workerVersionId ? { workerVersionId } : {}),
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
      return toolResult(resp);
    }
    if (parsed != null && typeof parsed === "object" && (parsed as Record<string, unknown>).error === true) {
      const inner = parsed as Record<string, unknown>;
      const errorMessage = typeof inner.message === "string"
        ? inner.message
        : "Tool returned a legacy error envelope without error details.";
      const classified = typeof inner.code === "string" ? null : classifyErrorMessage(errorMessage);
      const errorCode = typeof inner.code === "string" ? inner.code : classified?.code ?? "PROVIDER_ERROR";
      const resp: McpResponse = {
        ok: false,
        data: null,
        meta: classified ? { ...buildMeta(tool, opts), retryable: classified.retryable } as ToolMeta : buildMeta(tool, opts),
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
      return toolResult(resp);
    }
    data = parsed;
    data = confidenceLevels(tool, enrichFacts(data));
  } catch {
    data = rawData;
  }
  const meta = buildMeta(tool, { ...opts, dataDate: opts?.dataDate ?? payloadDataDate(parsed) });
  if (opts?.successWarnings && data !== rawData) {
    const notices = opts.successWarnings(data);
    if (notices.length > 0) meta.warnings = [...meta.warnings, ...notices];
  }
  const resp: McpResponse = {
    ok: true,
    data,
    meta,
    error: null,
  };
  return toolResult(resp);
}

export function mcpFailure(
  tool: string,
  code: string,
  message: string,
  opts?: { source?: string; metaExtra?: Record<string, unknown>; diagnostics?: unknown }
): string {
  return mcpFailureResult(tool, code, message, opts).text;
}

export function mcpFailureResult(
  tool: string,
  code: string,
  message: string,
  opts?: { source?: string; metaExtra?: Record<string, unknown>; diagnostics?: unknown }
): ToolResult {
  if (!envelopeV2Enabled()) {
    return toolResult({
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
    meta: buildMeta(tool, {
      source: opts?.source,
      metaExtra: opts?.metaExtra,
    }),
    error: {
      code,
      message,
      ...(opts?.metaExtra || {}),
    },
  };
  if (opts?.diagnostics !== undefined) {
    resp.diagnostics = opts.diagnostics;
  }
  return toolResult(resp);
}
