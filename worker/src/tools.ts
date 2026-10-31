import { mcpSuccessFromValue, mcpSuccessResult, mcpFailure, mcpFailureResult, type McpSuccessOptions, type ToolResult, ErrorCode, envelopeV2Enabled, getBuildVersion, getServerVersion, getWorkerVar } from "./response.js";
import { FILING_PERIOD_HELP, parseFilingPeriod } from "./sec-facts.js";
import { TICKER_CAPS } from "./tool-catalog.js";
import { withCacheScope, currentRequestFacts, budgetRefusalCount, SUBREQUEST_BUDGET, SUBREQUEST_LIMIT } from "./request-context.js";
import {
  getAnalystConsensus,
  getAnalystUpgradeRadar,
  getCalendar,
  getMarketCalendar,
  analyzeShareCountTrend,
  getCreditHealth,
  getOptionsFlowScan,
  getEarningsAnalysis,
  getEarningsMomentum,
  getPriceTargetBracket,
  getEtfInfo,
  getFastInfo,
  getFilingData,
  getFinancialRatios,
  getFinancialStatement,
  getVolumeGate,
  getHistoricalPrices,
  historicalRangeError,
  getHolderInfo,
  getExpandedInstitutionalOwnership,
  getMaPosition,
  getOptionChain,
  getOptionExpirationDates,
  getHistoricalPutCallRatio,
  getPriceSlope,
  getPriceStats,
  getPutHedgeCandidates,
  getRecommendations,
  getShortInterest,
  getShortMomentum,
  getTechnicalIndicators,
  getPositionScoreInputs,
  searchFilingText,
  getVolumeRatio,
  screenStocks,
  searchTicker,
  getStockActions,
  getStockInfo,
  getOptionsSummary,
  getCompanyNews,
  listSecCompanyFilings,
  getFilingOutline,
  getFilingSection,
  listFilingTables,
  getFilingTable,
  extractFilingFact,
  getSecFilingIndex,
  searchCompanyNews,
  getCompanyPressReleases,
  getSecRecentEvents,
  getPublicEventTimeline,
  verifyCompanyEvent,
  extractGeographicRevenue,
  extractSegmentRevenue,
  extractTotalRevenue,
  extractRevenueExposure,
  extractChinaExposure,
  extractExposure,
  extractRiskFactorMentions,
  extractCustomerConcentration,
  extractDilutionBridge,
  extractCapitalStructure,
  extractFundingCapexSchedule,
  extractOperatingDriverLedger,
  getGuidanceHistory,
  getHistoricalValuationContext,
  reconcileMetricSources,
  getShareCountScenarios,
  extractAnalystValuationMethods,
  getUkCompanyFilings,
  getValuationSnapshot,
  comparePeerValuations,
  querySecFilingIndex,
  getLatestEarningsRelease,
  indexEarningsRelease,
  extractEarningsMetrics,
  extractGuidance,
  extractManagementCommentary,
  compareEarningsActualVsEstimate,
  getMarketSnapshot,
  listSecMaterialFilings,
  getSecFilingIntelligence,
  getSecFilingSectionMarkdown,
  listSecFilingExhibits,
  getSecFilingExhibitContent,
  parsePublicTranscript,
  getEarningsCallTranscript,
  parseYahooTranscriptSourceUrl,
  xbrlSourceEvidence,
  isDecisionGradeXbrlFact,
} from "./yahoo-finance.js";
import {
  searchThaiFunds,
  getThaiFundDividendHistory,
  getThaiFundFactsheet,
  getThaiFundNav,
  getThaiFundNavBatch,
} from "./sec-thailand.js";
import { validateAccession, validateTicker } from "./validate.js";
import { buildValuationEvidencePack, getConsensusForecastCurve, getEpsRevisions, getEvidenceCut, getEvidenceQuality, listEvidenceCuts } from "./evidence-pack.js";
import { withAuthorityBoundary } from "./evidence.js";
import { parseMinAdvUsd } from "./deprecations.js";

import { TOOLS, annotationsForTool, ENVELOPE_V2_OUTPUT_SCHEMA, LLM_DETAILED_OUTPUT_TOOLS, envelopedToolOutputSchema, GROUPED_TOOLS, GROUPED_ACTIONS, type Tool } from "./schemas.js";

export { TOOLS, type Tool };

export function isGroupedMode(): boolean {
  return (getWorkerVar("TOOL_MODE") ?? "grouped").toLowerCase() === "grouped";
}

const ENVELOPE_SCHEMA_VERSION = "2026-07-08";

function currentToolMode(): "expanded" | "grouped" {
  return isGroupedMode() ? "grouped" : "expanded";
}

export function listVisibleTools(): Tool[] {
  const visible = isGroupedMode() ? GROUPED_TOOLS : TOOLS.filter((t) => !t.deprecated);
  return visible.map(tool => ({
    ...tool,
    annotations: tool.annotations ?? annotationsForTool(tool.name),
    ...(envelopeV2Enabled()
      ? {
          outputSchema: LLM_DETAILED_OUTPUT_TOOLS.has(tool.name)
            ? envelopedToolOutputSchema(tool.outputSchema)
            : ENVELOPE_V2_OUTPUT_SCHEMA,
        }
      : {}),
  }));
}

const str = (v: unknown, fallback = ""): string => (typeof v === "string" ? v : fallback);
const num = (v: unknown, fallback: number): number => (typeof v === "number" ? v : fallback);
const SLOPE_DAYS_MESSAGE = "days must be a whole number of sessions";
/** A session count: null when absent, the number when one is given, digits parsed, "invalid" otherwise. */
function sessionCount(v: unknown): number | null | "invalid" {
  if (v == null) return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : "invalid";
  if (typeof v === "string" && /^\s*-?\d+\s*$/.test(v)) return parseInt(v, 10);
  return "invalid";
}
const tickerArg = (v: unknown): string | string[] =>
  Array.isArray(v) ? v.map(String) : str(v);

function isEmptyRequiredValue(value: unknown): boolean {
  if (value == null) return true;
  if (typeof value === "string") return value.trim() === "";
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === "object") return Object.keys(value as Record<string, unknown>).length === 0;
  return false;
}

function matchesParamSchema(value: unknown, rawSchema: unknown): boolean {
  if (rawSchema == null || typeof rawSchema !== "object" || Array.isArray(rawSchema)) return true;
  const schema = rawSchema as Record<string, unknown>;
  const oneOf = schema.oneOf;
  if (Array.isArray(oneOf)) {
    return oneOf.some((candidate) => matchesParamSchema(value, candidate));
  }
  const rawType = schema.type;
  const types = Array.isArray(rawType) ? rawType.map(String) : rawType == null ? [] : [String(rawType)];
  if (types.length > 0) {
    const typeMatches = types.some((type) => {
      switch (type) {
        case "null": return value == null;
        case "string": return typeof value === "string";
        case "number": return typeof value === "number" && Number.isFinite(value);
        case "integer": return typeof value === "number" && Number.isInteger(value);
        case "boolean": return typeof value === "boolean";
        case "array": return Array.isArray(value);
        case "object": return value != null && typeof value === "object" && !Array.isArray(value);
        default: return true;
      }
    });
    if (!typeMatches) return false;
  }
  // ponytail: grouped boundary checks shape; action handlers own semantic enums.
  if (Array.isArray(value)) {
    if (typeof schema.maxItems === "number" && value.length > schema.maxItems) return false;
    if (typeof schema.minItems === "number" && value.length < schema.minItems) return false;
    if (schema.items != null && !value.every((item) => matchesParamSchema(item, schema.items))) return false;
  }
  if (typeof value === "number") {
    if (typeof schema.minimum === "number" && value < schema.minimum) return false;
    if (typeof schema.maximum === "number" && value > schema.maximum) return false;
  }
  return true;
}

function validateGroupedActionParams(
  action: string,
  params: Record<string, unknown>,
): string | null {
  const definition = TOOLS.find((tool) => tool.name === action);
  if (!definition) {
    return mcpFailure(action, ErrorCode.INPUT_VALIDATION_ERROR, `No canonical input contract exists for '${action}'.`);
  }
  const properties = definition.inputSchema.properties;
  const expectedParams = Object.keys(properties);
  const required = definition.inputSchema.required ?? [];
  const missingParams = required.filter((name) => !(name in params));
  const emptyParams = required.filter((name) => name in params && isEmptyRequiredValue(params[name]));
  const unexpectedParams = Object.keys(params).filter((name) => !(name in properties)).sort();
  const invalidParams = Object.entries(params)
    .filter(([name, value]) => name in properties && !matchesParamSchema(value, properties[name]))
    .map(([name]) => name)
    .sort();
  const allInvalidParams = [...new Set([...emptyParams, ...invalidParams])].sort();
  if (missingParams.length === 0 && allInvalidParams.length === 0 && unexpectedParams.length === 0) {
    return null;
  }
  const reasons: string[] = [];
  if (missingParams.length > 0) reasons.push(`missing required parameter(s): ${missingParams.join(", ")}`);
  if (emptyParams.length > 0) reasons.push(`empty required parameter(s): ${emptyParams.join(", ")}`);
  const typedInvalid = invalidParams.filter((name) => !emptyParams.includes(name));
  if (typedInvalid.length > 0) reasons.push(`invalid parameter value(s): ${typedInvalid.join(", ")}`);
  if (unexpectedParams.length > 0) reasons.push(`unexpected parameter(s): ${unexpectedParams.join(", ")}`);
  return mcpFailure(
    action,
    ErrorCode.INPUT_VALIDATION_ERROR,
    `Invalid params for '${action}': ${reasons.join("; ")}.`,
    {
      metaExtra: {
        missingParams,
        invalidParams: allInvalidParams,
        unexpectedParams,
        expectedParams,
        recommendedNextAction: "CORRECT_TOOL_PARAMS",
      },
    },
  );
}

/**
 * True when text reports an HTTP 429 status ("api error 429", "HTTP 429",
 * "status 429"). A bare substring test also matched digits inside URLs and
 * cache-buster timestamps, turning ordinary failures into RATE_LIMIT.
 */
function mentionsHttp429(lower: string): boolean {
  return /\b(?:error|http|status)[:\s]*429\b/.test(lower);
}

function legacyToolFailure(raw: string): { code: string; message: string } | null {
  let text = raw.trim();
  if (!text) return null;
  // A JSON object or array is never a legacy text failure, and invalid JSON
  // starting with "{" or "[" fails the prefix checks below; skip parsing it.
  if (text[0] === "{" || text[0] === "[") return null;
  try {
    const parsed = JSON.parse(text);
    if (parsed != null && typeof parsed === "object") return null;
    if (typeof parsed === "string") text = parsed.trim();
  } catch {
    // Plain-text legacy payload.
  }
  const lower = text.toLowerCase();
  if (!(lower.startsWith("error") || (lower.startsWith("company ticker") && lower.includes("not found")))) {
    return null;
  }
  const code = lower.startsWith("error: invalid") || lower.includes(" is required")
    ? ErrorCode.INPUT_VALIDATION_ERROR
    : lower.includes("no option")
      ? ErrorCode.NO_OPTIONS_DATA
      : lower.includes("not found") || lower.includes("api error 404")
        ? ErrorCode.TICKER_NOT_FOUND
        : lower.includes("rate limit") || mentionsHttp429(lower)
          ? ErrorCode.RATE_LIMIT
          : lower.includes("timeout") || lower.includes("timed out")
            ? ErrorCode.PROVIDER_TIMEOUT
            : ErrorCode.PROVIDER_ERROR;
  return { code, message: text };
}

const SEC_XBRL_CONCEPT_ALIASES: Record<string, string> = {
  cash: "cash",
  cashandcashequivalentsatcarryingvalue: "cash",
  revenuefromcontractwithcustomerexcludingassessedtax: "total_revenue",
  revenuefromcontractwithcustomerincludingassessedtax: "total_revenue",
  revenues: "total_revenue",
  revenue: "total_revenue",
  salesrevenuenet: "total_revenue",
  totalrevenue: "total_revenue",
};

function normalizeSecXbrlConceptName(value: string): string {
  return value
    .replace(/^(?:us-gaap|dei|srt|country):/i, "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toLowerCase();
}

function mappedSecFactType(value: string): string | null {
  return SEC_XBRL_CONCEPT_ALIASES[normalizeSecXbrlConceptName(value)] ?? null;
}

function looksLikeSecXbrlConcept(value: string): boolean {
  const bare = value.trim().replace(/^(?:us-gaap|dei|srt|country):/i, "");
  return /^[A-Z][A-Za-z0-9]+$/.test(bare) && /[a-z][A-Z]/.test(bare);
}

function supportedXbrlConceptAliases(): string[] {
  return [
    "CashAndCashEquivalentsAtCarryingValue",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
  ];
}

function hasUsefulEvidence(evidence: unknown): boolean {
  if (!evidence) return false;
  const rows = Array.isArray(evidence) ? evidence : [evidence];
  return rows.some((row) => {
    if (!row || typeof row !== "object") return false;
    return Object.values(row as Record<string, unknown>).some((value) => {
      if (value == null) return false;
      if (Array.isArray(value)) return value.length > 0;
      if (typeof value === "object") return Object.keys(value as Record<string, unknown>).length > 0;
      return String(value).trim() !== "";
    });
  });
}

type ToolReliabilityStatus = {
  capabilityStatus: "ACTIVE" | "DEGRADED" | "PROVIDER_GATED" | "EXPERIMENTAL" | "RETIRED";
  reliabilityClass: "RELIABLE" | "VERIFY_BEFORE_USE" | "DIAGNOSTIC" | "UNRELIABLE";
  failureMode: string | null;
  evidenceRequired: boolean;
  sourceType: "sec_xbrl" | "sec_table" | "sec_filing" | "company_ir" | "company_ir_page" | "yahoo" | "exchange" | "provider_diagnostic" | "unknown";
  successCriteria?: string[];
  limitationStatuses?: string[];
  note?: string;
};

const TOOL_RELIABILITY_STATUS: Record<string, ToolReliabilityStatus> = {
  get_sec_filing_section_markdown: {
    capabilityStatus: "DEGRADED",
    reliabilityClass: "UNRELIABLE",
    failureMode: "LIVE_SECTION_EXTRACTION_UNRELIABLE",
    evidenceRequired: true,
    sourceType: "sec_filing",
  },
  get_company_press_releases: {
    capabilityStatus: "ACTIVE",
    reliabilityClass: "RELIABLE",
    failureMode: null,
    evidenceRequired: true,
    sourceType: "company_ir_page",
    successCriteria: ["coverageStatus=SEC_EX99_RESOLVED or APPROVED_IR_PAGE_RESOLVED", "decisionGrade=true", "secEvidence or irPageEvidence present"],
    limitationStatuses: ["SEC_8K_FOUND_EX99_NOT_FOUND", "NO_OFFICIAL_RELEASE_SOURCE", "NO_YAHOO_PRESS_RELEASE", "COMPANY_IR_PAGE_NOT_APPROVED", "COMPANY_IR_PAGE_CANDIDATE_ONLY", "COMPANY_IR_NOT_FOUND", "NOT_FOUND"],
    note: "Tool is usable, but gate-clear is payload-level only; candidate, unresolved, RSS-only, newswire, and Yahoo payloads remain non-decision-grade.",
  },
  extract_sec_filing_fact: {
    capabilityStatus: "ACTIVE",
    reliabilityClass: "RELIABLE",
    failureMode: null,
    evidenceRequired: true,
    sourceType: "sec_xbrl",
    successCriteria: ["value", "xbrlContext", "sourceEvidence"],
    limitationStatuses: ["SEC_FACT_NOT_AVAILABLE", "NO_COMPANYCONCEPT_FACT_FOR_FORM", "UNSUPPORTED_XBRL_CONCEPT"],
    note: "Decision-grade only for successful XBRL facts with value, xbrlContext, and sourceEvidence; limitation statuses remain non-decision-grade at payload level.",
  },
};

/**
 * Success-envelope options for a tool: its status table entry in meta.
 */
function successOptions(name: string): McpSuccessOptions {
  const status = TOOL_RELIABILITY_STATUS[name];
  return {
    ...(status ? { metaExtra: { ...status } } : {}),
  };
}

async function computeHash(data: string): Promise<string> {
  const encoded = new TextEncoder().encode(data);
  const buf = await crypto.subtle.digest("SHA-256", encoded);
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, "0")).join("");
}

async function resolveSecDocumentUrl(
  ticker: string,
  filingType: string,
  limit: number
): Promise<string | null> {
  const listed = JSON.parse(await listSecCompanyFilings(ticker, filingType, limit)) as Record<string, unknown>;
  const filings = (listed.filings as Record<string, unknown>[]) ?? [];
  const first = filings[0] ?? {};
  return (first.documentUrl as string | null) ?? (first.primaryDocumentUrl as string | null) ?? null;
}

function secIndexTablesPayload(indexPayload: Record<string, unknown>, offset: number, limit: number): string {
  if (indexPayload.ok === false || indexPayload.error) return JSON.stringify(indexPayload);
  const safeOffset = Math.max(0, Math.trunc(offset));
  const safeLimit = Math.min(100, Math.max(1, Math.trunc(limit || 50)));
  const index = indexPayload.index && typeof indexPayload.index === "object"
    ? indexPayload.index as Record<string, unknown>
    : {};
  const allTables = Array.isArray(index.tables) ? index.tables as Record<string, unknown>[] : [];
  const rawTableCount = typeof index.rawTableCount === "number" ? index.rawTableCount : allTables.length;
  const excludedTableCount = typeof index.excludedTableCount === "number"
    ? index.excludedTableCount
    : Math.max(0, rawTableCount - allTables.length);
  const tables = allTables.slice(safeOffset, safeOffset + safeLimit).map((table, i) => ({
    tableIndex: table.tableId ?? safeOffset + i,
    title: table.title ?? null,
    headers: Array.isArray(table.headers) ? table.headers : [],
    rowLabels: Array.isArray(table.rowLabels) ? table.rowLabels : [],
    sectionId: table.sectionId ?? null,
    unitScale: table.unitScale ?? "unknown",
    confidence: table.confidence ?? "LOW",
  }));
  return JSON.stringify({
    ticker: indexPayload.ticker,
    filingType: indexPayload.filingType,
    filingDate: indexPayload.filingDate ?? null,
    accessionNumber: indexPayload.accessionNumber ?? null,
    documentUrl: indexPayload.documentUrl ?? null,
    status: allTables.length > 0 ? "OK" : "NO_USABLE_TABLES",
    tableCount: rawTableCount,
    usableTableCount: allTables.length,
    excludedTableCount,
    returnedCount: tables.length,
    offset: safeOffset,
    limit: safeLimit,
    hasMore: safeOffset + tables.length < allTables.length,
    tables,
    recommendedNextAction: allTables.length > 0 ? "GET_SEC_FILING_TABLE" : "SEARCH_FILING_TEXT",
  });
}

function secIndexOutlinePayload(indexPayload: Record<string, unknown>): string {
  if (indexPayload.ok === false || indexPayload.error) return JSON.stringify(indexPayload);
  const index = indexPayload.index && typeof indexPayload.index === "object"
    ? indexPayload.index as Record<string, unknown>
    : {};
  const sections = Array.isArray(index.sections) ? index.sections as Record<string, unknown>[] : [];
  const tables = Array.isArray(index.tables) ? index.tables as Record<string, unknown>[] : [];
  const outline = sections.map((section) => ({
    level: section.level ?? null,
    title: section.heading ?? section.normalizedHeading ?? "",
    sectionId: section.sectionId ?? null,
  })).filter((section) => String(section.title ?? "").trim());
  const warnings = outline.length === 0 && tables.length > 0
    ? [{ code: "TABLES_FOUND_OUTLINE_EMPTY", message: "Filing tables were detected but no section outline headings were parsed.", severity: "warning" }]
    : [];
  return JSON.stringify({
    ticker: indexPayload.ticker,
    filingType: indexPayload.filingType ?? null,
    filingDate: indexPayload.filingDate ?? null,
    accessionNumber: indexPayload.accessionNumber ?? null,
    documentUrl: indexPayload.documentUrl ?? null,
    outline,
    status: outline.length > 0 ? "OK" : (tables.length > 0 ? "OUTLINE_NOT_PARSED" : "EMPTY"),
    tableCount: tables.length,
    warnings,
  });
}

export async function callTool(name: string, args: Record<string, unknown>): Promise<string> {
  return (await callToolResult(name, args)).text;
}

/**
 * Each tool call runs in its own cache scope, so its meta.cacheHit and
 * meta.cacheSource describe that call alone.
 */
export async function callToolResult(name: string, args: Record<string, unknown>): Promise<ToolResult> {
  return (await withCacheScope(async () => budgetExhaustedFailure(name, await callToolResultInScope(name, args)))).result;
}

/**
 * A tool call that failed after the subrequest budget refused one of its
 * fetches failed for budget, whatever its own error path called it (2.11.0).
 * Calls that succeeded, or that met no refusal, are returned unchanged.
 */
function budgetExhaustedFailure(name: string, result: ToolResult): ToolResult {
  if (budgetRefusalCount() === 0) return result;
  let value = result.value;
  if (value === undefined) {
    try { value = JSON.parse(result.text); } catch { return result; }
  }
  if (value == null || typeof value !== "object" || Array.isArray(value)) return result;
  const record = value as Record<string, unknown>;
  if (record.ok !== false && record.error !== true) return result;
  return mcpFailureResult(
    name,
    ErrorCode.PROVIDER_BUDGET_EXHAUSTED,
    `The per-request subrequest budget (${SUBREQUEST_BUDGET} of ${SUBREQUEST_LIMIT}) was spent before this request could complete. Retry later.`,
    { metaExtra: { retryable: true, recoveryAction: "RETRY_LATER" } },
  );
}

async function callToolResultInScope(name: string, args: Record<string, unknown>): Promise<ToolResult> {
  let raw: string;
  try {
    raw = withAuthorityBoundary(name, await _dispatchTool(name, args));
  } catch (error) {
    const rawMessage = error instanceof Error ? error.message : String(error);
    const lower = rawMessage.toLowerCase();
    const httpStatus = (error as { status?: unknown } | null)?.status;
    if (lower.includes("unknown tool") || lower.includes("unknown grouped tool")) {
      return mcpFailureResult(name, ErrorCode.INPUT_VALIDATION_ERROR, "Unknown tool name.");
    }
    if (lower.includes("ticker_not_found") || lower.includes("api error 404") || lower.includes("no data found for ticker")) {
      return mcpFailureResult(name, ErrorCode.TICKER_NOT_FOUND, "No provider data was found for the requested ticker.", {
        metaExtra: { retryable: false },
      });
    }
    if (httpStatus === 429 || lower.includes("rate limit") || lower.includes("rate_limit") || mentionsHttp429(lower)) {
      return mcpFailureResult(name, ErrorCode.RATE_LIMIT, "The upstream data provider rate limit was reached. Retry later.", {
        metaExtra: { retryable: true },
      });
    }
    if (lower.includes("timeout") || lower.includes("timed out") || lower.includes("abort")) {
      return mcpFailureResult(name, ErrorCode.PROVIDER_TIMEOUT, "The upstream data provider timed out. Retry this request.", {
        metaExtra: { retryable: true },
      });
    }
    return mcpFailureResult(name, ErrorCode.PROVIDER_ERROR, "The upstream data provider request failed.", {
      metaExtra: { retryable: false },
    });
  }
  const legacyFailure = legacyToolFailure(raw);
  if (legacyFailure) {
    // Throttling and timeouts are transient; say so, as the thrown-error path does.
    const retryable = legacyFailure.code === ErrorCode.RATE_LIMIT || legacyFailure.code === ErrorCode.PROVIDER_TIMEOUT;
    return mcpFailureResult(name, legacyFailure.code, legacyFailure.message, retryable ? { metaExtra: { retryable: true } } : undefined);
  }
  let batchMeta: { partialSuccess?: boolean; successCount?: number; errorCount?: number } | undefined;
  // Only batch results carry __batchMeta; everything else (large filings and
  // transcripts included) is parsed once, by mcpSuccessResult.
  if (raw.includes('"__batchMeta"')) {
    let parsed: Record<string, unknown> | null = null;
    try {
      parsed = JSON.parse(raw) as Record<string, unknown>;
    } catch {
      // non-JSON payload
    }
    const metaRaw = parsed?.__batchMeta;
    if (parsed && metaRaw != null && typeof metaRaw === "object") {
      const bm = metaRaw as Record<string, unknown>;
      batchMeta = {
        partialSuccess: bm.partialSuccess === true,
        successCount: typeof bm.successCount === "number" ? bm.successCount : undefined,
        errorCount: typeof bm.errorCount === "number" ? bm.errorCount : undefined,
      };
      delete parsed.__batchMeta;
      return mcpSuccessFromValue(name, parsed, JSON.stringify(parsed), {
        ...batchMeta,
        ...successOptions(name),
      });
    }
  }
  return mcpSuccessResult(name, raw, successOptions(name));
}

export async function callVisibleTool(name: string, args: Record<string, unknown>): Promise<string> {
  return (await callVisibleToolResult(name, args)).text;
}

export async function callVisibleToolResult(name: string, args: Record<string, unknown>): Promise<ToolResult> {
  return (await withCacheScope(() => callVisibleToolResultInScope(name, args))).result;
}

async function callVisibleToolResultInScope(name: string, args: Record<string, unknown>): Promise<ToolResult> {
  if (!isGroupedMode()) return callToolResult(name, args);

  const actions = GROUPED_ACTIONS.get(name);
  if (!actions) {
    return mcpFailureResult(name, ErrorCode.INPUT_VALIDATION_ERROR, "Unknown tool name.");
  }
  const action = str(args.action).trim();
  if (!action) {
    return mcpFailureResult(name, ErrorCode.INPUT_VALIDATION_ERROR, "action is required");
  }
  if (!actions.has(action)) {
    return mcpFailureResult(name, ErrorCode.INPUT_VALIDATION_ERROR, `Unknown action '${action}' for grouped tool '${name}'`);
  }
  const params = args.params;
  if (params != null && (typeof params !== "object" || Array.isArray(params))) {
    return mcpFailureResult(name, ErrorCode.INPUT_VALIDATION_ERROR, "params must be an object when provided");
  }
  const actionParams = (params as Record<string, unknown> | undefined) ?? {};
  const capFailure = tickerCapFailure(action, actionParams);
  if (capFailure) return { text: capFailure };
  const validationFailure = validateGroupedActionParams(action, actionParams);
  if (validationFailure) return { text: validationFailure };
  return callToolResult(action, actionParams);
}

/**
 * The public filing index lists each table's first 8 row labels and its
 * label count; the full labels stay in the cached index the extractors read.
 * A 10-K index otherwise runs to tens of thousands of characters.
 */
const INDEX_ROW_LABELS_SHOWN = 8;
function compactFilingIndexPayload(raw: string): string {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return raw;
  }
  const index = parsed.index as Record<string, unknown> | undefined;
  if (!index || !Array.isArray(index.tables)) return raw;
  index.tables = (index.tables as Record<string, unknown>[]).map((table) => {
    const labels = Array.isArray(table.rowLabels) ? table.rowLabels : [];
    if (labels.length <= INDEX_ROW_LABELS_SHOWN) return table;
    return { ...table, rowLabels: labels.slice(0, INDEX_ROW_LABELS_SHOWN), rowLabelCount: labels.length };
  });
  return JSON.stringify(parsed);
}

// ── Tool-call gate (2.12.0, Q3b/Q3c) ─────────────────────────────────────────
// One check at the tool-call boundary, before any handler code or fetch, for
// every public action in both tool modes (grouped dispatch reaches it through
// callToolResult). Absent or null arguments, empty or whitespace-only ticker
// values (and an empty accession) are skipped; they and non-string values are
// left to the handler, as before.

const TICKER_ARGS = ["ticker", "tickers", "subject", "peers"] as const;
const ACCESSION_ARGS = ["accession_number", "accessionNumber"] as const;
// get_company_news sources that read one issuer's own pages: one ticker per call.
const SINGLE_TICKER_NEWS_SOURCES = new Set(["company_ir", "company_ir_page", "newswire"]);
const SINGLE_TICKER_NEWS_CAP = 1;

/** The tickerCaps entry that applies to this call, or null when the action is uncapped. */
function tickerCapFor(name: string, args: Record<string, unknown>): number | null {
  if (name === "get_company_news" && Array.isArray(args.sources)
    && args.sources.some((source) => SINGLE_TICKER_NEWS_SOURCES.has(String(source).toLowerCase().trim()))) {
    return SINGLE_TICKER_NEWS_CAP;
  }
  // Mirrors the handler: only mode "full" selects full mode.
  const key = name === "get_market_snapshot" && str(args.mode, "compact") === "full" ? "get_market_snapshot.full" : name;
  return TICKER_CAPS[key] ?? null;
}

/** BATCH_LIMIT_EXCEEDED when the ticker array is longer than the action's cap. */
function tickerCapFailure(name: string, args: Record<string, unknown>): string | null {
  const ticker = args.ticker;
  if (!Array.isArray(ticker)) return null;
  const cap = tickerCapFor(name, args);
  if (cap == null || ticker.length <= cap) return null;
  return mcpFailure(
    name,
    ErrorCode.BATCH_LIMIT_EXCEEDED,
    `Too many tickers: ${ticker.length}. Maximum is ${cap} per call for ${name}.`,
    { metaExtra: { retryable: false } },
  );
}

const PUBLIC_ACTIONS = new Set(TOOLS.map((tool) => tool.name));

/**
 * INPUT_VALIDATION_ERROR (echoing the caller's raw value) or BATCH_LIMIT_EXCEEDED, else null.
 * An unknown tool name is left to the dispatcher ("Unknown tool name.").
 */
export function validateToolArgs(name: string, args: Record<string, unknown>): string | null {
  if (!PUBLIC_ACTIONS.has(name)) return null;
  const capFailure = tickerCapFailure(name, args);
  if (capFailure) return capFailure;
  for (const arg of TICKER_ARGS) {
    const value = args[arg];
    const values = Array.isArray(value) ? value : [value];
    for (const item of values) {
      // Empty or whitespace-only values read as absent: the handler keeps its own empty-value behaviour.
      if (typeof item !== "string" || item.trim() === "") continue;
      const error = validateTicker(item);
      if (error) return mcpFailure(name, ErrorCode.INPUT_VALIDATION_ERROR, error);
    }
  }
  for (const arg of ACCESSION_ARGS) {
    const value = args[arg];
    // An empty accession reads as absent, as Python's `if accession_number:` call sites do today.
    if (typeof value !== "string" || value === "") continue;
    const error = validateAccession(value);
    if (error) return mcpFailure(name, ErrorCode.INPUT_VALIDATION_ERROR, error);
  }
  return null;
}

// SEC fact tools whose `period` selects the fact: "latest" or a fiscal year, never an unread string (2.5.16).
const FILING_PERIOD_TOOLS = new Set([
  "extract_sec_filing_fact",
  "extract_geographic_revenue",
  "extract_segment_revenue",
  "extract_total_revenue",
  "extract_revenue_exposure",
  "extract_china_exposure",
  "extract_exposure",
  "query_sec_filing_index",
]);

// Earnings tools whose schema allows only period "latest": another value was silently read as the latest
// release, so "FY2025 Q4" returned FY2026 Q3 with no mismatch (2.25.1, F28).
export const LATEST_ONLY_PERIOD_TOOLS = new Set([
  "get_latest_earnings_release",
  "index_earnings_release",
  "extract_earnings_metrics",
  "extract_guidance",
  "extract_management_commentary",
  "compare_earnings_actual_vs_estimate",
  "get_earnings_call_transcript",
]);

type ToolHandler = (args: Record<string, unknown>) => Promise<string> | string;

const TOOL_HANDLERS: ReadonlyMap<string, ToolHandler> = new Map<string, ToolHandler>([
  ["search_thai_funds", (args) => searchThaiFunds(args.project_info, args.company_info, args.fund_class_name, args.page_size, args.next_cursors)],
  ["get_thai_fund_nav", (args) => getThaiFundNav(args.fund_class_name, args.proj_id, args.as_of_date, args.lookback_days, args.project_info)],
  ["get_thai_fund_nav_batch", (args) => getThaiFundNavBatch(args.funds, args.as_of_date, args.lookback_days)],
  ["get_thai_fund_factsheet", (args) => getThaiFundFactsheet(args.fund_class_name, args.proj_id, args.sections, args.project_info)],
  ["get_thai_fund_dividend_history", (args) => getThaiFundDividendHistory(args.fund_class_name, args.proj_id, args.max_results, args.next_cursor, args.project_info)],
  ["get_historical_prices", (args) => {
    const rawTicker = args.ticker;
    if (rawTicker == null || String(rawTicker).trim() === "") {
      return mcpFailure("get_historical_prices", ErrorCode.INPUT_VALIDATION_ERROR, "ticker is required");
    }
    const tickerStr = String(rawTicker).trim().toUpperCase();
    const tickerErr = validateTicker(tickerStr);
    if (tickerErr) return mcpFailure("get_historical_prices", ErrorCode.INPUT_VALIDATION_ERROR, tickerErr);
    const period = str(args.period, "1mo").trim();
    const interval = str(args.interval, "1d").trim();
    const rangeErr = historicalRangeError(period, interval);
    if (rangeErr) return mcpFailure("get_historical_prices", ErrorCode.INPUT_VALIDATION_ERROR, rangeErr);
    return getHistoricalPrices(tickerStr, period, interval, args.prepost === true);
  }],
  ["get_company_profile", (args) => getStockInfo(tickerArg(args.ticker), args.include_all === true)],
  ["get_fund_profile", (args) => getEtfInfo(tickerArg(args.ticker), Array.isArray(args.sections) ? args.sections.map((item) => str(item)) : undefined)],
  ["get_company_news", (args) => {
    return getCompanyNews(
      tickerArg(args.ticker),
      num(args.max_results, 10),
      num(args.lookback_days, 14),
      Array.isArray(args.sources) ? args.sources.map(String) : null,
    );
  }],
  ["get_corporate_actions", (args) => getStockActions(str(args.ticker), args.start_date != null ? str(args.start_date) : "", num(args.limit, 40))],
  ["get_financial_statement", (args) => getFinancialStatement(str(args.ticker), str(args.financial_type), Array.isArray(args.line_items) ? args.line_items.map((item) => str(item)) : undefined)],
  ["get_ownership_holders", (args) => getHolderInfo(str(args.ticker), str(args.holder_type))],
  ["get_expanded_institutional_ownership", (args) => {
    return getExpandedInstitutionalOwnership(
      str(args.ticker),
      args.allow_scarce_fallback === true,
      num(args.max_holders, 50),
    );
  }],
  ["get_option_expiration_dates", (args) => getOptionExpirationDates(str(args.ticker))],
  ["get_historical_put_call_ratio", (args) => getHistoricalPutCallRatio(str(args.ticker), str(args.date))],
  ["get_option_chain", (args) => {
    return getOptionChain(str(args.ticker), str(args.expiration_date), str(args.option_type),
      num(args.max_contracts, 50), num(args.min_open_interest, 0), num(args.min_volume, 0),
      args.strike_min != null ? num(args.strike_min, 0) : null,
      args.strike_max != null ? num(args.strike_max, 0) : null,
      str(args.moneyness, "near_money"),
      str(args.sort_by, "relevance"),
      num(args.moneyness_window_pct, 20),
      args.include_illiquid === true);
  }],
  ["get_analyst_recommendations", (args) => {
    return getRecommendations(
      str(args.ticker),
      str(args.recommendation_type),
      num(args.months_back, 12)
    );
  }],
  ["get_market_quote", (args) => getFastInfo(tickerArg(args.ticker))],
  ["analyze_price_performance", (args) => getPriceStats(tickerArg(args.ticker))],
  ["get_analyst_consensus", (args) => getAnalystConsensus(tickerArg(args.ticker))],
  ["get_earnings_analysis", (args) => getEarningsAnalysis(str(args.ticker))],
  ["analyze_financial_ratios", (args) => getFinancialRatios(tickerArg(args.ticker), num(args.history_periods, 0), str(args.frequency, "quarterly"))],
  ["analyze_share_count_trend", (args) => analyzeShareCountTrend(str(args.ticker), args.start_date != null ? str(args.start_date) : undefined, args.end_date != null ? str(args.end_date) : undefined)],
  ["get_company_events_calendar", (args) => getCalendar(str(args.ticker), str(args.mode, "upcoming"), num(args.limit, 12), num(args.offset, 0))],
  ["get_market_calendar", (args) => getMarketCalendar(str(args.event_type, "earnings"), args.start_date != null ? str(args.start_date) : undefined, args.end_date != null ? str(args.end_date) : undefined, num(args.limit, 25), num(args.offset, 0))],
  ["search_ticker", (args) => searchTicker(str(args.query), num(args.max_results, 8), args.exchange != null ? str(args.exchange) : null)],
  ["screen_stocks", (args) => screenStocks(str(args.screener_name), num(args.count, 25))],
  ["get_short_interest", (args) => getShortInterest(str(args.ticker))],
  ["get_technical_indicators", (args) => getTechnicalIndicators(tickerArg(args.ticker), str(args.period, "1y"))],
  ["get_price_slope", (args) => {
    // A whole number of sessions, as a number or digits; anything else is refused rather than read as 5, the same
    // in both runtimes (R5-pricing-1, Q3b).
    const days = sessionCount(args.days);
    if (days === "invalid") return mcpFailure("get_price_slope", ErrorCode.INPUT_VALIDATION_ERROR, SLOPE_DAYS_MESSAGE);
    return getPriceSlope(tickerArg(args.ticker), days ?? 5);
  }],
  ["analyze_volume_ratio", (args) => getVolumeRatio(tickerArg(args.ticker), num(args.period, 10))],
  ["analyze_moving_average_position", (args) => getMaPosition(tickerArg(args.ticker))],
  ["analyze_credit_health", (args) => getCreditHealth(tickerArg(args.ticker))],
  ["get_short_momentum", (args) => getShortMomentum(tickerArg(args.ticker))],
  ["analyze_earnings_momentum", (args) => getEarningsMomentum(tickerArg(args.ticker))],
  ["summarize_options_flow", (args) => getOptionsSummary(str(args.ticker), args.expiry_hint != null ? str(args.expiry_hint) : undefined)],
  ["find_put_hedge_candidates", (args) => {
    return getPutHedgeCandidates(
      str(args.ticker),
      num(args.otm_pct_min, 8),
      num(args.otm_pct_max, 12),
      num(args.budget_usd, 500),
      str(args.expiry_after)
    );
  }],
  ["get_analyst_rating_changes", (args) => getAnalystUpgradeRadar(tickerArg(args.ticker), num(args.days_back, 30))],
  ["extract_sec_filing_fact", async (args) => {
    const requestedFactName = str(args.fact_type ?? args.fact ?? args.fact_name);
    const mappedFact = requestedFactName ? mappedSecFactType(requestedFactName) : null;
    if (requestedFactName && mappedFact == null && looksLikeSecXbrlConcept(requestedFactName)) {
      return mcpFailure(
        "extract_sec_filing_fact",
        "UNSUPPORTED_XBRL_CONCEPT",
        `Unsupported XBRL concept '${requestedFactName}'. Use a supported fact_type or one of the supported concept aliases.`,
        { metaExtra: { supportedXbrlConceptAliases: supportedXbrlConceptAliases(), supportedFactTypes: ["cash", "total_revenue", "geographic_revenue", "segment_revenue"] } },
      );
    }
    if (args.fact_type != null || args.region != null || args.fact_name == null || args.fact != null || mappedFact != null) {
      const fact = mappedFact ?? str(args.fact_type ?? args.fact, args.region != null ? "geographic_revenue" : "total_revenue");
      const raw = await getFilingData(
        str(args.ticker),
        fact,
        args.region != null ? str(args.region) : null,
        str(args.filing_type, "10-K"),
        str(args.period, "latest"),
        str(args.period_mode, "auto"),
        args.accession_number != null ? str(args.accession_number) : null,
      );
      let parsed: Record<string, unknown> = {};
      try {
        let parsedAny: unknown = JSON.parse(raw);
        if (
          parsedAny != null &&
          typeof parsedAny === "object" &&
          "ok" in (parsedAny as Record<string, unknown>) &&
          "data" in (parsedAny as Record<string, unknown>)
        ) {
          parsedAny = (parsedAny as Record<string, unknown>).data;
        }
        if (typeof parsedAny === "string") {
          parsedAny = JSON.parse(parsedAny);
        }
        if (parsedAny != null && typeof parsedAny === "object") {
          parsed = parsedAny as Record<string, unknown>;
        }
      } catch {
        parsed = {};
      }
      const sourceEvidence = xbrlSourceEvidence(parsed)
        ?? (hasUsefulEvidence(parsed.evidence) ? parsed.evidence : null);
      const status = parsed.status ?? (parsed.value != null ? "FOUND" : (parsed.code ?? parsed.confidence ?? "NOT_DISCLOSED"));
      const decisionEvidence = sourceEvidence && typeof sourceEvidence === "object" && !Array.isArray(sourceEvidence)
        ? sourceEvidence as Record<string, unknown>
        : null;
      const decisionGrade = isDecisionGradeXbrlFact(parsed, decisionEvidence, status);
      return JSON.stringify({
        fact,
        region: args.region != null ? str(args.region) : null,
        value: parsed.value ?? null,
        denominator: parsed.denominator ?? null,
        valueRatio: parsed.valueRatio ?? null,
        valuePct: parsed.valuePct ?? null,
        rawValue: parsed.rawValue ?? null,
        rawDenominator: parsed.rawDenominator ?? null,
        unit: "USD",
        unitScale: parsed.unitScale ?? null,
        period: parsed.period ?? null,
        filingType: parsed.filingType ?? str(args.filing_type, "10-K"),
        filingDate: parsed.filingDate ?? null,
        accessionNumber: parsed.accessionNumber ?? null,
        ...(parsed.requestedAccession ? { requestedAccession: parsed.requestedAccession } : {}),
        extractionMethod: parsed.extractionMethod ?? "NONE",
        source: parsed.source ?? "NOT_DISCLOSED",
        confidence: parsed.confidence ?? "NOT_DISCLOSED",
        status,
        code: parsed.code ?? null,
        ...(typeof parsed.retryable === "boolean" ? { retryable: parsed.retryable } : {}),
        ...(Array.isArray(parsed.failedReads) ? { failedReads: parsed.failedReads } : {}),
        decisionGrade,
        documentUrl: parsed.documentUrl ?? null,
        indexUrl: parsed.indexUrl ?? null,
        primaryDocumentUrl: parsed.primaryDocumentUrl ?? null,
        xbrlContext: parsed.xbrlContext ?? null,
        evidence: decisionGrade ? sourceEvidence : (parsed.evidence ?? null),
        sourceEvidence,
        calculation: parsed.calculation ?? null,
        warnings: parsed.warnings ?? [],
        ticker: parsed.ticker ?? str(args.ticker),
      });
    }
    return extractFilingFact(str(args.ticker), str(args.fact_name), args.document_url != null ? str(args.document_url) : null, args.accession_number != null ? str(args.accession_number) : null);
  }],
  ["list_sec_company_filings", (args) => listSecCompanyFilings(str(args.ticker), str(args.filing_type ?? args.form_type, "10-K"), num(args.limit ?? args.max_filings, 5))],
  ["get_sec_filing_outline", async (args) => {
    const ticker = str(args.ticker);
    const filingType = str(args.filing_type ?? args.form_type, "10-K");
    if (args.document_url == null) {
      const idx = JSON.parse(await getSecFilingIndex(ticker, filingType, str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null)) as Record<string, unknown>;
      return secIndexOutlinePayload(idx);
    }
    return getFilingOutline(ticker, args.accession_number != null ? str(args.accession_number) : null, str(args.document_url));
  }],
  ["get_sec_filing_section", async (args) => {
    const ticker = str(args.ticker);
    const filingType = str(args.filing_type ?? args.form_type, "10-K");
    const sectionName = args.section_name != null
      ? str(args.section_name)
      : str((args.selector as Record<string, unknown> | undefined)?.item, "Item 1A");
    const docUrl = args.document_url != null
      ? str(args.document_url)
      : await resolveSecDocumentUrl(ticker, filingType, 1);
    return getFilingSection(ticker, sectionName, str(docUrl), num(args.context_chars, 3000));
  }],
  ["list_sec_filing_tables", async (args) => {
    const ticker = str(args.ticker);
    const filingType = str(args.filing_type ?? args.form_type, "10-K");
    const offset = num(args.offset, 0);
    const limit = num(args.limit, 50);
    if (args.document_url != null) {
      return listFilingTables(ticker, str(args.document_url), offset, limit);
    }
    const idx = JSON.parse(await getSecFilingIndex(ticker, filingType, str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null)) as Record<string, unknown>;
    return secIndexTablesPayload(idx, offset, limit);
  }],
  ["get_sec_filing_table", async (args) => {
    const ticker = str(args.ticker);
    const filingType = str(args.filing_type ?? args.form_type, "10-K");
    const docUrl = args.document_url != null
      ? str(args.document_url)
      : (JSON.parse(await getSecFilingIndex(ticker, filingType, str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null)) as Record<string, unknown>).documentUrl;
    if (!docUrl) return JSON.stringify({ ok: false, error: { code: "FILING_NOT_FOUND_TRY_OTHER_TYPE", message: `No ${filingType} filing document found for '${ticker}'` } });
    return getFilingTable(ticker, str(docUrl), num(args.table_index, 0), num(args.max_rows, 30));
  }],
  ["search_sec_filing_text", (args) => {
    return searchFilingText(
      str(args.ticker),
      Array.isArray(args.search_terms) ? args.search_terms.map(String) : [],
      args.section_hint != null ? str(args.section_hint) : (args.selector != null ? str((args.selector as Record<string, unknown>).item, "") || null : null),
      str(args.filing_type, "10-K"),
      args.accession_number != null ? str(args.accession_number) : null,
      num(args.context_chars, 1500),
      args.return_tables === true,
      args.document_url != null ? str(args.document_url) : null,
      {
        query: args.search_query != null ? str(args.search_query) : null,
        excludeTerms: Array.isArray(args.exclude_terms) ? args.exclude_terms : [],
        near: Array.isArray(args.near) ? args.near : [],
        match: args.match != null ? str(args.match) : null,
        order: args.order != null ? str(args.order) : null,
        maxMatches: num(args.max_matches, 10),
        cursor: args.cursor != null ? str(args.cursor) : null,
        filingCount: num(args.filing_count, 1),
        since: args.since != null ? str(args.since) : null,
        includeExhibits: args.include_exhibits === true,
      },
    );
  }],
  ["get_sec_filing_index", async (args) => compactFilingIndexPayload(await getSecFilingIndex(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null))],
  ["list_sec_material_filings", (args) => listSecMaterialFilings(str(args.ticker), Array.isArray(args.forms) ? args.forms.map(String) : null, num(args.limit, 5))],
  ["get_sec_filing_intelligence", (args) => getSecFilingIntelligence(str(args.ticker), str(args.filing_type, "10-K"), num(args.filing_index, 0))],
  ["get_sec_filing_section_markdown", (args) => getSecFilingSectionMarkdown(str(args.ticker), str(args.section, "Item 1A"), str(args.filing_type, "10-K"), num(args.filing_index, 0), num(args.max_chars, 50000))],
  ["list_sec_filing_exhibits", (args) => listSecFilingExhibits(str(args.ticker), str(args.accessionNumber))],
  ["get_sec_filing_exhibit_content", (args) => {
    return getSecFilingExhibitContent(
      str(args.ticker),
      str(args.accessionNumber),
      str(args.fileName),
      Array.isArray(args.topics) ? args.topics.map(String) : null,
    );
  }],
  ["parse_public_transcript", (args) => {
    return parsePublicTranscript(
      str(args.url),
      Array.isArray(args.topics) ? args.topics.map(String) : null,
      args.raw_text != null ? str(args.raw_text) : null,
    );
  }],
  ["get_earnings_call_transcript", (args) => {
    if (args.fiscal_quarter != null && !/^\d{4}Q[1-4]$/i.test(str(args.fiscal_quarter).trim())) {
      return mcpFailure(
        "get_earnings_call_transcript",
        ErrorCode.INPUT_VALIDATION_ERROR,
        "fiscal_quarter must use issuer fiscal-quarter format YYYYQ1 through YYYYQ4.",
      );
    }
    if (args.event_id != null && !/^\d+$/.test(str(args.event_id).trim())) {
      return mcpFailure(
        "get_earnings_call_transcript",
        ErrorCode.INPUT_VALIDATION_ERROR,
        "event_id must contain digits only.",
      );
    }
    if (args.source_url != null) {
      const parsedSource = parseYahooTranscriptSourceUrl(str(args.ticker), str(args.source_url).trim());
      if (!parsedSource.identity) {
        return mcpFailure(
          "get_earnings_call_transcript",
          ErrorCode.INPUT_VALIDATION_ERROR,
          parsedSource.error ?? "source_url is not a valid Yahoo earnings-call URL.",
        );
      }
      if (args.event_id != null && str(args.event_id).trim() !== parsedSource.identity.eventId) {
        return mcpFailure(
          "get_earnings_call_transcript",
          ErrorCode.INPUT_VALIDATION_ERROR,
          "event_id does not match source_url.",
        );
      }
      if (args.fiscal_quarter != null && str(args.fiscal_quarter).trim().toUpperCase() !== parsedSource.identity.fiscalQuarter) {
        return mcpFailure(
          "get_earnings_call_transcript",
          ErrorCode.INPUT_VALIDATION_ERROR,
          "fiscal_quarter does not match source_url.",
        );
      }
    }
    if (args.paragraph_limit != null && (!Number.isInteger(Number(args.paragraph_limit)) || Number(args.paragraph_limit) < 1 || Number(args.paragraph_limit) > 50)) {
      return mcpFailure(
        "get_earnings_call_transcript",
        ErrorCode.INPUT_VALIDATION_ERROR,
        "paragraph_limit must be an integer from 1 through 50.",
      );
    }
    if (args.paragraph_cursor != null && !/^\d+$/.test(str(args.paragraph_cursor).trim())) {
      return mcpFailure(
        "get_earnings_call_transcript",
        ErrorCode.INPUT_VALIDATION_ERROR,
        "paragraph_cursor must be a non-negative integer.",
      );
    }
    return getEarningsCallTranscript(
      str(args.ticker),
      str(args.period, "latest"),
      Array.isArray(args.topics) ? args.topics.map(String) : null,
      args.fiscal_quarter != null ? str(args.fiscal_quarter).trim().toUpperCase() : null,
      args.event_id != null ? str(args.event_id).trim() : null,
      args.source_url != null ? str(args.source_url).trim() : null,
      num(args.paragraph_limit, 20),
      args.paragraph_cursor != null ? Number(str(args.paragraph_cursor).trim()) : 0,
    );
  }],
  ["extract_geographic_revenue", (args) => {
    return extractGeographicRevenue(
      str(args.ticker),
      str(args.region),
      str(args.filing_type, "10-K"),
      str(args.period, "latest"),
      args.accession_number != null ? str(args.accession_number) : null,
      str(args.detailLevel, "compact"),
    );
  }],
  ["extract_segment_revenue", (args) => extractSegmentRevenue(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"))],
  ["extract_total_revenue", (args) => extractTotalRevenue(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"))],
  ["extract_revenue_exposure", (args) => extractRevenueExposure(str(args.ticker), str(args.exposure_query), str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"))],
  ["extract_china_exposure", (args) => {
    return extractChinaExposure(
      str(args.ticker),
      str(args.filing_type, "10-K"),
      str(args.period, "latest"),
      args.accession_number != null ? str(args.accession_number) : null,
      str(args.detailLevel, "compact"),
    );
  }],
  ["extract_exposure", (args) => {
    return extractExposure(
      str(args.ticker),
      str(args.topic),
      str(args.filing_type, "10-K"),
      str(args.period, "latest"),
      args.include_risk_factors !== false,
    );
  }],
  ["extract_risk_factor_mentions", (args) => extractRiskFactorMentions(str(args.ticker), Array.isArray(args.terms) ? args.terms.map(String) : [], str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"))],
  ["extract_customer_concentration", (args) => extractCustomerConcentration(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"))],
  ["get_valuation_snapshot", (args) => {
    return getValuationSnapshot(
      str(args.ticker),
      args.price != null ? Number(args.price) : null,
      str(args.filing_type, "latest"),
      args.include_sec_filings !== false,
    );
  }],
  ["compare_peer_valuations", (args) => {
    return comparePeerValuations(
      Array.isArray(args.tickers) ? args.tickers.map(String) : [],
      args.subject != null ? str(args.subject) : null,
    );
  }],
  ["get_historical_valuation_context", (args) => {
    return getHistoricalValuationContext(
      str(args.ticker),
      Array.isArray(args.dates) ? args.dates.map(String) : null,
      Array.isArray(args.peers) ? args.peers.map(String) : null,
    );
  }],
  ["reconcile_metric_sources", (args) => reconcileMetricSources(str(args.ticker), str(args.metric), str(args.period, "latest"), num(args.tolerance_pct, 0.5))],
  ["get_consensus_forecast_curve", (args) => {
    return getConsensusForecastCurve(str(args.ticker), num(args.horizon_years, 5), num(args.min_analyst_count, 3), num(args.conflict_tolerance_pct, 10),
      args.observation_date == null || args.observation_date === "" ? null : str(args.observation_date));
  }],
  ["get_eps_revisions", (args) => getEpsRevisions(str(args.ticker))],
  ["get_evidence_quality", (args) => getEvidenceQuality(str(args.ticker))],
  ["build_valuation_evidence_pack", (args) => buildValuationEvidencePack(str(args.ticker), num(args.horizon_years, 5), args.persist !== false)],
  ["get_evidence_cut", (args) => getEvidenceCut(str(args.evidence_cut_id))],
  ["list_evidence_cuts", (args) => listEvidenceCuts(str(args.ticker), num(args.limit, 20))],
  ["extract_dilution_bridge", (args) => {
    return extractDilutionBridge(
      str(args.ticker),
      Number(args.price),
      args.as_of_date != null ? str(args.as_of_date) : null,
      str(args.currency, "USD"),
      str(args.filing_type, "latest"),
      args.accession_number != null ? str(args.accession_number) : null,
      args.include_atm !== false,
    );
  }],
  ["get_share_count_scenarios", (args) => {
    return getShareCountScenarios(
      str(args.ticker),
      args.scenarios,
      str(args.filing_type, "latest"),
      args.accession_number != null ? str(args.accession_number) : null,
      str(args.currency, "USD"),
    );
  }],
  ["extract_funding_capex_schedule", (args) => extractFundingCapexSchedule(str(args.ticker), str(args.filing_type, "latest"), args.accession_number != null ? str(args.accession_number) : null)],
  ["extract_operating_driver_ledger", (args) => extractOperatingDriverLedger(str(args.ticker), str(args.filing_type, "latest"), args.accession_number != null ? str(args.accession_number) : null)],
  ["get_guidance_history", (args) => getGuidanceHistory(str(args.ticker), args.max_releases != null ? Number(args.max_releases) : 8)],
  ["extract_capital_structure", (args) => {
    return extractCapitalStructure(
      str(args.ticker),
      str(args.filing_type, "latest"),
      args.accession_number != null ? str(args.accession_number) : null,
      args.include_funding_statements !== false,
    );
  }],
  ["extract_analyst_valuation_methods", (args) => extractAnalystValuationMethods(str(args.ticker), num(args.days_back, 30))],
  ["get_uk_company_filings", (args) => {
    return getUkCompanyFilings(
      args.ticker != null ? str(args.ticker) : null,
      args.company_number != null ? str(args.company_number) : null,
      args.company_name != null ? str(args.company_name) : null,
      args.category != null ? str(args.category) : null,
      num(args.limit, 25),
      args.include_charges === true,
    );
  }],
  ["query_sec_filing_index", (args) => {
    return querySecFilingIndex(
      str(args.ticker),
      str(args.filing_type, "10-K"),
      str(args.period, "latest"),
      args.accession_number != null ? str(args.accession_number) : null,
      str(args.query_type),
      (args.params && typeof args.params === "object" && !Array.isArray(args.params)) ? args.params as Record<string, unknown> : {},
      args.return_evidence !== false,
      str(args.detailLevel, "compact"),
    );
  }],
  ["get_latest_earnings_release", (args) => getLatestEarningsRelease(str(args.ticker), str(args.period, "latest"))],
  ["index_earnings_release", (args) => indexEarningsRelease(str(args.ticker), str(args.period, "latest"), args.source_url != null ? str(args.source_url) : null)],
  ["extract_earnings_metrics", (args) => {
    return extractEarningsMetrics(
      str(args.ticker),
      str(args.period, "latest"),
      Array.isArray(args.source_preference) ? args.source_preference.map(String) : ["sec_8k", "company_ir", "10-q", "yahoo"],
    );
  }],
  ["extract_guidance", (args) => extractGuidance(str(args.ticker), str(args.period, "latest"))],
  ["extract_management_commentary", (args) => {
    return extractManagementCommentary(
      str(args.ticker),
      str(args.period, "latest"),
      Array.isArray(args.topics) ? args.topics.map(String) : [],
    );
  }],
  ["compare_earnings_actual_vs_estimate", (args) => compareEarningsActualVsEstimate(str(args.ticker), str(args.period, "latest"))],
  ["analyze_options_flow_window", (args) => getOptionsFlowScan(str(args.ticker), str(args.window_label))],
  ["calculate_price_target_distance", (args) => {
    return getPriceTargetBracket(
      str(args.ticker),
      num(args.reference_target_price ?? args.io_pt, 0),
    );
  }],
  ["analyze_position_signals", (args) => getPositionScoreInputs(tickerArg(args.ticker))],
  ["check_volume_liquidity_threshold", (args) => {
    const minAdvUsd = parseMinAdvUsd(args.min_adv_usd);
    if (minAdvUsd != null && typeof minAdvUsd === "object") {
      return mcpFailure("check_volume_liquidity_threshold", ErrorCode.INPUT_VALIDATION_ERROR, minAdvUsd.error);
    }
    return getVolumeGate(str(args.ticker), args.foreign_exchange === true, minAdvUsd);
  }],
  ["search_company_news", (args) => {
    if (str(args.query).trim() === "") {
      return mcpFailure("search_company_news", ErrorCode.INPUT_VALIDATION_ERROR, "query is required");
    }
    return searchCompanyNews(
      str(args.ticker),
      str(args.query),
      str(args.start_date, ""),
      str(args.end_date, ""),
      Array.isArray(args.sources) ? args.sources.map(String) : null,
      num(args.max_results, 10),
    );
  }],
  ["get_company_press_releases", (args) => {
    return getCompanyPressReleases(
      str(args.ticker),
      num(args.lookback_days, 90),
      num(args.max_results, 20),
      Array.isArray(args.sources) ? args.sources.map(String) : null,
    );
  }],
  ["get_sec_recent_events", (args) => {
    return getSecRecentEvents(
      str(args.ticker),
      Array.isArray(args.filing_types) ? args.filing_types.map(String) : ["8-K", "8-K/A", "10-Q", "10-K"],
      num(args.lookback_days, 90),
      num(args.max_results, 20),
    );
  }],
  ["get_public_event_timeline", (args) => {
    return getPublicEventTimeline(
      str(args.ticker),
      str(args.start_date, ""),
      str(args.end_date, ""),
      Array.isArray(args.sources) ? args.sources.map(String) : null,
      num(args.max_results, 50),
      args.newest_first === true,
    );
  }],
  ["verify_company_event", (args) => {
    if (str(args.event_query).trim() === "") {
      return mcpFailure("verify_company_event", ErrorCode.INPUT_VALIDATION_ERROR, "event_query is required");
    }
    return verifyCompanyEvent(
      str(args.ticker),
      str(args.event_query),
      str(args.start_date, ""),
      str(args.end_date, ""),
      Array.isArray(args.sources) ? args.sources.map(String) : null,
    );
  }],
  ["health_check", async () => {
    const version = getServerVersion();
    const buildVersion = getBuildVersion();
    const visibleTools = listVisibleTools();
    const manifestVersion = getWorkerVar("MANIFEST_VERSION") ?? "1";
    const manifestHash = await computeHash(JSON.stringify(visibleTools.map(t => t.name)));
    const schemaHash = await computeHash(JSON.stringify(visibleTools.map(t => ({
      name: t.name,
      description: t.description,
      inputSchema: t.inputSchema,
      outputSchema: t.outputSchema,
    }))));
    return JSON.stringify({
      status: "ok",
      serverVersion: version,
      buildVersion,
      buildSha: getWorkerVar("BUILD_SHA")?.trim() || null,
      deployedAt: getWorkerVar("DEPLOYED_AT")?.trim() || null,
      toolCount: visibleTools.length,
      manifestVersion,
      manifestHash,
      schemaHash,
      runtimeHash: await computeHash(`${buildVersion}|${schemaHash}|${currentToolMode()}`),
      toolMode: currentToolMode(),
      envelopeSchemaVersion: ENVELOPE_SCHEMA_VERSION,
      generatedAt: new Date().toISOString(),
      privacyScope: "public_market_data_only",
      // Whether this request carried a non-empty Authorization header (never its value); null outside /mcp.
      authorizationHeaderPresent: currentRequestFacts()?.authorizationHeaderPresent ?? null,
      // How this request presented its credential: "header", "path" or "none" (never the credential); null outside /mcp.
      credentialTransport: currentRequestFacts()?.credentialTransport ?? null,
    });
  }],
  ["get_market_snapshot", (args) => {
    const t = args.ticker;
    const ticker = Array.isArray(t) ? (t as string[]) : str(t);
    const mode = str(args.mode, "compact") === "full" ? "full" : "compact";
    const foreignExchange = args.foreign_exchange === true;
    const minAdvUsd = parseMinAdvUsd(args.min_adv_usd);
    if (minAdvUsd != null && typeof minAdvUsd === "object") {
      return mcpFailure("get_market_snapshot", ErrorCode.INPUT_VALIDATION_ERROR, minAdvUsd.error);
    }
    return getMarketSnapshot(ticker, mode, foreignExchange, minAdvUsd);
  }],
]);

async function _dispatchTool(name: string, args: Record<string, unknown>): Promise<string> {
  const gateFailure = validateToolArgs(name, args);
  if (gateFailure) return gateFailure;
  if (FILING_PERIOD_TOOLS.has(name) && args.period != null && !parseFilingPeriod(String(args.period))) {
    return mcpFailure(name, ErrorCode.INPUT_VALIDATION_ERROR, `${FILING_PERIOD_HELP} Got '${String(args.period)}'.`);
  }
  if (LATEST_ONLY_PERIOD_TOOLS.has(name) && args.period != null && !/^\s*latest\s*$/i.test(String(args.period))) {
    return mcpFailure(name, ErrorCode.INPUT_VALIDATION_ERROR, `period supports only "latest" for ${name}; it reads the latest earnings release. Got '${String(args.period)}'.`);
  }
  const handler = TOOL_HANDLERS.get(name);
  if (!handler) throw new Error(`Unknown tool: ${name}`);
  return handler(args);
}
