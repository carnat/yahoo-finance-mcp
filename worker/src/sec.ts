/**
 * SEC EDGAR filing retrieval: filing lookup, text search, outline, sections, tables, facts, the filing index and
 * material filings, exhibits, XBRL facts and the SEC filing intelligence tool (Phase 5 move out of yahoo-finance.ts).
 * The EDGAR transport stays in clients/edgar.ts; helpers shared with other domains still live in yahoo-finance.ts
 * and are imported back. yahoo-finance.ts re-exports every moved name, so tools.ts and the parity bundle are unchanged.
 */

import { setBounded, BoundedTtlCache } from "./cache.js";
import { countCacheEvent } from "./request-context.js";
import { ErrorCode, mcpFailure, classifyErrorMessage } from "./response.js";
import { validateTicker, validateAccession } from "./validate.js";
import {
  type HeadingAnchor,
  projectFilingText,
  filingTableSpans,
  type NearSpec,
  termsFromList,
  type MatchMode,
  type MatchOrder,
  parseSearchQuery,
  type SearchSpec,
  type SearchMatch,
  sectionHintScope,
  type SearchDocument,
  searchDocument,
  matchContext,
  hitsBySection,
  orderMatches,
  matchPayload,
  termLabel,
  type FilingTextProjection,
} from "./filing-search.js";
import {
  parseFilingPeriod,
  FILING_PERIOD_HELP,
  pickConceptFacts,
  selectFiscalYearRows,
  filingFactInAccession,
  REVENUE_CONCEPTS,
} from "./sec-facts.js";
import { annualFilingsFromSubmissions, latestAnnualFiling, latestAnnualCoverageWarning } from "./companyfacts-coverage.js";
import { documentFiscalYearFocus, filingFiscalYearLabel } from "./fiscal-calendar.js";
import {
  getSubmissionsForTicker,
  edgarBuildFilingUrls,
  edgarGetJson,
  resolveCikForTicker,
  getEdgarTickerCikMap,
  edgarReadJson,
  edgarGetHtml,
  SEC_DOCUMENT_READ_MAX_CHARS,
  type SecDocumentResult,
  fetchSecDocument,
  edgarListExhibitsFromIndex,
  edgarCikFromAccession,
  edgarReadExhibitIndex,
  edgarDocumentUrlFromRef,
  edgarDocumentFileName,
} from "./clients/edgar.js";
import {
  secCompanyFactsUrl,
  filingManualLookup,
  secCompanyConceptUrl,
  SEC_ARCHIVES_PREFIX,
  secArchiveDocumentUrl,
} from "./clients/endpoints.js";
import {
  UNIT_SCALE_ASSUMED_WARNING,
  stripHtmlTags,
  mergeFinancialCells,
  _sanitizeFilingHtml,
  _stripHtmlTagsIdx,
  tableUnitScale,
  decodeHtmlEntities,
  htmlToReadableText,
  filterParagraphsByTopics,
} from "./clients/html.js";
import { parseObjectJson } from "./util.js";
import {
  FILING_INDEX_CACHE_MAX,
  FILING_INDEX_TTL_MS,
  type ResolvedSecFiling,
  extractChinaExposure,
  extractCustomerConcentration,
  extractGeoRevenueFromHtml,
  extractGeographicRevenue,
  extractRevenueExposure,
  extractRiskFactorMentions,
  extractSegmentRevenue,
  extractTotalRevenue,
  filingIndexCache,
  geoRegionAliases,
  normalizeStatus,
} from "./yahoo-finance.js";

// Equivalent concepts are all read; the one filed most recently wins (sec-facts.ts).
const FILING_FACT_CONCEPTS: Record<string, { primary: string; fallback?: string; alternates?: string[] }> = {
  geographic_revenue: { primary: REVENUE_CONCEPTS[0], alternates: REVENUE_CONCEPTS.slice(1) },
  segment_revenue: { primary: REVENUE_CONCEPTS[0], alternates: REVENUE_CONCEPTS.slice(1) },
  capex: { primary: "PaymentsToAcquirePropertyPlantAndEquipment" },
  rd_expense: { primary: "ResearchAndDevelopmentExpense" },
  operating_income: { primary: "OperatingIncomeLoss" },
  net_income: { primary: "NetIncomeLoss" },
  total_revenue: { primary: REVENUE_CONCEPTS[0], alternates: REVENUE_CONCEPTS.slice(1) },
  long_term_debt: { primary: "LongTermDebt" },
  cash: { primary: "CashAndCashEquivalentsAtCarryingValue" },
};

function uniqueRecentForms(forms: unknown[], limit = 12): string[] {
  const out: string[] = [];
  for (const form of forms) {
    const value = String(form ?? "").toUpperCase();
    if (value && !out.includes(value)) out.push(value);
    if (out.length >= limit) break;
  }
  return out;
}

function filingNotFoundPayload(ticker: string, requestedFilingType: string, availableFilingTypes: string[]): Record<string, unknown> {
  const suggestedFilingTypes = requestedFilingType.toUpperCase() === "10-K" && availableFilingTypes.includes("20-F") ? ["20-F"] : [];
  return {
    status: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
    code: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
    ticker,
    requestedFilingType,
    availableFilingTypes,
    suggestedFilingTypes,
    accessionNumber: null,
    filingDate: null,
    documentUrl: null,
    warnings: [{
      code: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
      message: `No ${requestedFilingType} filing found for '${ticker}'.`,
      severity: "error",
    }],
  };
}

/**
 * An explicit accession that is not among the company's recent SEC filings (submissions filings.recent, about the
 * last 1,000): the action fails closed, never falling back to the latest filing or another form (2.13.0, R4 Q3a).
 */
function accessionNotFoundError(ticker: string, accessionNumber: string, recentOnly = true): Record<string, unknown> {
  return {
    code: ErrorCode.ACCESSION_NOT_FOUND,
    // The exhibit readers check SEC's filing index, not the recent-filings list, so their miss is any filing (2.15.0).
    message: `Accession ${accessionNumber} was not found in ${ticker.toUpperCase()}'s ${recentOnly ? "recent " : ""}SEC filings.`,
    requestedAccession: accessionNumber,
    retryable: false,
  };
}

export function isAccessionNotFound(error: Record<string, unknown>): boolean {
  return error.code === ErrorCode.ACCESSION_NOT_FOUND;
}

/** The ACCESSION_NOT_FOUND error envelope a public action returns. */
export function accessionNotFoundEnvelope(error: Record<string, unknown>): string {
  return JSON.stringify({ ok: false, error });
}

/**
 * For the readers 2.13.0 does not fail closed on an accession miss (extract_china_exposure, the structured
 * geographic_revenue HTML fallback): the filing-not-found payload they read before, unchanged.
 */
export async function legacyAccessionMissPayload(ticker: string, filingType: string): Promise<Record<string, unknown>> {
  const { submissions } = await getSubmissionsForTicker(ticker);
  const recent = ((submissions?.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  return filingNotFoundPayload(ticker, (filingType || "10-K").toUpperCase(), uniqueRecentForms((recent.form as string[]) ?? []));
}

function isLikelyXbrlDocumentUrl(url: string): boolean {
  const lower = url.split("?", 1)[0].toLowerCase();
  return lower.endsWith(".xml") || /\/xbrl\//.test(lower) || /(_htm|xbrl|cal|def|lab|pre)\.xml$/.test(lower);
}

export async function resolveSecFiling(
  ticker: string,
  requestedFilingType: string = "10-K",
  accessionNumber: string | null = null,
): Promise<{ ok: true; filing: ResolvedSecFiling } | { ok: false; error: Record<string, unknown> }> {
  const requested = (requestedFilingType || "10-K").toUpperCase();
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return { ok: false, error: { status: "TICKER_NOT_FOUND", code: "TICKER_NOT_FOUND", ticker, message: `Could not resolve EDGAR submissions for ticker '${ticker}'` } };
  }

  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form as string[]) ?? [];
  const accessions = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs = (recent.primaryDocument as string[]) ?? [];
  const filingDates = (recent.filingDate as string[]) ?? [];
  const reportDates = (recent.reportDate as string[]) ?? [];
  const acceptedDts = (recent.acceptanceDateTime as string[]) ?? [];
  const availableFilingTypes = uniqueRecentForms(forms);

  let targetIdx = -1;
  const warnings: Record<string, unknown>[] = [];
  if (accessionNumber) {
    targetIdx = accessions.findIndex((a) => a === accessionNumber);
  } else {
    targetIdx = forms.findIndex((f) => String(f).toUpperCase() === requested);
    if (targetIdx < 0 && requested === "10-K") {
      const fallbackIdx = forms.findIndex((f) => String(f).toUpperCase() === "20-F");
      if (fallbackIdx >= 0) {
        targetIdx = fallbackIdx;
        warnings.push({
          code: "AUTO_20F_FALLBACK",
          message: "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
          severity: "info",
        });
      }
    }
  }

  if (accessionNumber && targetIdx < 0) {
    return { ok: false, error: accessionNotFoundError(ticker, accessionNumber) };
  }
  if (targetIdx < 0 || !accessions[targetIdx]) {
    return { ok: false, error: filingNotFoundPayload(ticker, requested, availableFilingTypes) };
  }

  const primaryDocument = String(primaryDocs[targetIdx] ?? "");
  if (!primaryDocument) {
    return {
      ok: false,
      error: {
        status: "FILING_TEXT_NOT_AVAILABLE",
        code: "FILING_TEXT_NOT_AVAILABLE",
        ticker,
        requestedFilingType: requested,
        filingType: String(forms[targetIdx] ?? requested),
        filingDate: filingDates[targetIdx] ?? null,
        accessionNumber: accessions[targetIdx],
        documentUrl: null,
        availableFilingTypes,
        suggestedFilingTypes: [],
        warnings: [{ code: "PRIMARY_DOCUMENT_MISSING", message: "SEC submissions entry has no primaryDocument.", severity: "error" }],
      },
    };
  }

  const cikInt = parseInt(cikPadded, 10);
  const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(cikInt, accessions[targetIdx], primaryDocument);
  if (!edgarPrimaryDocumentUrl || isLikelyXbrlDocumentUrl(edgarPrimaryDocumentUrl)) {
    return {
      ok: false,
      error: {
        status: "FILING_TEXT_NOT_AVAILABLE",
        code: "FILING_TEXT_NOT_AVAILABLE",
        ticker,
        requestedFilingType: requested,
        filingType: String(forms[targetIdx] ?? requested),
        filingDate: filingDates[targetIdx] ?? null,
        accessionNumber: accessions[targetIdx],
        documentUrl: edgarPrimaryDocumentUrl,
        availableFilingTypes,
        suggestedFilingTypes: [],
        warnings: [{ code: "PRIMARY_HTML_NOT_FOUND", message: "Could not resolve a human-readable primary filing HTML document.", severity: "error" }],
      },
    };
  }

  return {
    ok: true,
    filing: {
      ticker,
      cikPadded,
      cikInt,
      requestedFilingType: requested,
      filingType: String(forms[targetIdx] ?? requested),
      filingDate: filingDates[targetIdx] ?? null,
      reportDate: reportDates[targetIdx] || null,
      acceptedAt: acceptedDts[targetIdx] ?? null,
      accessionNumber: accessions[targetIdx],
      primaryDocument,
      documentUrl: edgarPrimaryDocumentUrl,
      availableFilingTypes,
      suggestedFilingTypes: requested === "10-K" && String(forms[targetIdx]).toUpperCase() === "20-F" ? ["20-F"] : [],
      warnings,
    },
  };
}

function filingHasRelevantGeoText(html: string, region: string): boolean {
  const htmlLower = html.toLowerCase();
  const aliases = geoRegionAliases(region);
  if (aliases.length === 0) return false;
  let idx = 0;
  let checked = 0;
  while (checked < 20) {
    const positions = aliases
      .map((term) => htmlLower.indexOf(term, idx))
      .filter((pos) => pos >= 0);
    if (positions.length === 0) return false;
    const pos = Math.min(...positions);
    if (pos < 0) return false;
    const window = htmlLower.slice(Math.max(0, pos - 4_000), Math.min(htmlLower.length, pos + 4_000));
    if (/revenue|sales|geographic|segment/.test(window)) return true;
    idx = pos + 1;
    checked++;
  }
  return false;
}

function normalizeSegmentLabel(segment: unknown): string {
  if (segment == null) return "";
  if (Array.isArray(segment)) return segment.map((s) => normalizeSegmentLabel(s)).join(" ").trim();
  if (typeof segment === "object") return Object.values(segment as Record<string, unknown>).map(String).join(" ").trim();
  return String(segment).trim();
}

function buildXbrlFactContext(fact: Record<string, unknown>, concept: string, periodMode: string): Record<string, unknown> {
  let durationDays: number | null = null;
  if (fact.start && fact.end) {
    try {
      const d0 = new Date(String(fact.start));
      const d1 = new Date(String(fact.end));
      if (!isNaN(d0.getTime()) && !isNaN(d1.getTime())) {
        durationDays = Math.round((d1.getTime() - d0.getTime()) / (1000 * 60 * 60 * 24));
      }
    } catch { /* ignore */ }
  }
  const segmentLabel = normalizeSegmentLabel(fact.segment);
  const dimensions: Record<string, unknown> = {};
  if (segmentLabel) dimensions.segment = segmentLabel;
  if (fact.segment != null) dimensions.rawSegment = fact.segment;
  return {
    concept,
    taxonomy: "us-gaap",
    unit: "USD",
    periodStart: fact.start ?? null,
    periodEnd: fact.end ?? null,
    instant: fact.start ? null : (fact.end ?? null),
    durationDays,
    fiscalPeriod: String(fact.fp ?? ""),
    fiscalYear: String(fact.fy ?? ""),
    form: String(fact.form ?? ""),
    frame: fact.frame ?? null,
    accessionNumber: fact.accn ?? null,
    filedAt: fact.filed ?? null,
    periodMode,
    dimensions,
  };
}

function regionMatches(segmentLabel: string, region: string, includeAsiaFallback = false): boolean {
  const label = segmentLabel.toLowerCase();
  const regionLower = region.toLowerCase();
  if (label.includes(regionLower)) return true;
  // Also try compact (no-space) matching for XBRL member names like "GreaterChinaMember"
  const regionCompact = regionLower.replace(/\s+/g, "");
  if (regionCompact && label.includes(regionCompact)) return true;
  if (regionLower === "china") {
    if (["country:cn", "greater china", "srt:chinamember", "greaterchina"].some((t) => label.includes(t))) return true;
    return includeAsiaFallback && label.includes("asiapacificmember");
  }
  if (regionLower === "greater china") {
    if (label.includes("greaterchina") || label.includes("greater china")) return true;
  }
  return false;
}

const ANNUAL_FILING_TYPE_RE = /^(?:10-K|20-F|40-F)\b/i;

/**
 * The LATEST_ANNUAL_NOT_IN_COMPANYFACTS warning for an annual-form fact read (2.5.30, F-015), or null. It is
 * looked for only when the read has no value or its value is from another accession than the latest annual
 * report, so a fact read from that report costs no companyfacts read. Unreadable SEC data gives none.
 */
async function annualCoverageWarningFor(ticker: string, payload: Record<string, unknown>): Promise<Record<string, unknown> | null> {
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker).catch(() => ({ cikPadded: null, submissions: null }));
  if (!cikPadded || !submissions) return null;
  const filings = annualFilingsFromSubmissions(submissions);
  const latest = latestAnnualFiling(filings);
  const used = payload.value != null && typeof payload.accessionNumber === "string" ? payload.accessionNumber.replace(/-/g, "") : null;
  if (!latest || used === latest.accessionNumber.replace(/-/g, "")) return null;
  const facts = await edgarGetJson(secCompanyFactsUrl(cikPadded)).catch(() => null);
  return facts ? latestAnnualCoverageWarning(filings, facts) : null;
}

export async function getFilingData(
  ticker: string,
  factType: string,
  region: string | null = null,
  filingType = "10-K",
  period = "latest",
  periodMode = "auto",
  pinAccession: string | null = null,
): Promise<string> {
  const raw = await getFilingDataUnchecked(ticker, factType, region, filingType, period, periodMode, pinAccession);
  if (!ANNUAL_FILING_TYPE_RE.test(filingType)) return raw;
  let payload: unknown;
  try {
    payload = JSON.parse(raw);
  } catch {
    return raw;
  }
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return raw;
  const p = payload as Record<string, unknown>;
  // A failed read, an unresolved ticker or a rejected period says nothing about what companyfacts holds.
  if (p.status === "PROVIDER_ERROR" || ["NO_SEC_REGISTRANT", "SEC_LOOKUP_UNAVAILABLE", "INVALID_PERIOD"].includes(String(p.code))) return raw;
  const warning = await annualCoverageWarningFor(ticker, p);
  if (!warning) return raw;
  p.warnings = [...(Array.isArray(p.warnings) ? p.warnings : []), warning];
  return JSON.stringify(p);
}

async function getFilingDataUnchecked(
  ticker: string,
  factType: string,
  region: string | null = null,
  filingType = "10-K",
  period = "latest",
  periodMode = "auto",
  pinAccession: string | null = null,
): Promise<string> {
  const FLOATING_POINT_EPSILON = 1e-9;
  const RATIO_SCALE = 10000;
  const PCT_SCALE = 100;
  const PCT_MULTIPLIER = 100;
  const ratioToPct = (ratio: number): number => Math.round(ratio * PCT_MULTIPLIER * PCT_SCALE) / PCT_SCALE;
  const formatRawNumber = (n: number | null | undefined): string | null => {
    if (n == null || !Number.isFinite(n)) return null;
    if (Math.abs(n - Math.round(n)) < FLOATING_POINT_EPSILON) return Math.round(n).toLocaleString("en-US");
    return n.toLocaleString("en-US", { maximumFractionDigits: 2, minimumFractionDigits: 2 });
  };
  // Warnings raised while reading SEC facts, carried on every result (2.5.15).
  const readWarnings: Record<string, unknown>[] = [];
  const withGeoShape = (input: Record<string, unknown>, addDenominatorWarning = false): string => {
    const payload = readWarnings.length
      ? { ...input, warnings: [...(Array.isArray(input.warnings) ? input.warnings : []), ...readWarnings] }
      : input;
    if (factType !== "geographic_revenue") return JSON.stringify(payload);
    const warnings = Array.isArray(payload.warnings) ? [...payload.warnings] : [];
    const hasDenominator = payload.denominator != null;
    const shaped: Record<string, unknown> = {
      ticker: payload.ticker ?? ticker,
      factType: payload.factType ?? "geographic_revenue",
      region: payload.region ?? region,
      period: payload.period ?? null,
      rawValue: payload.rawValue ?? null,
      rawDenominator: payload.rawDenominator ?? null,
      unit: payload.unit ?? "USD",
      unitScale: payload.unitScale ?? "actual",
      // How a parsed table's scale was known (2.5.21); null for XBRL facts and when nothing was parsed.
      unitScaleSource: payload.unitScaleSource ?? null,
      value: payload.value ?? null,
      denominator: payload.denominator ?? null,
      valueRatio: hasDenominator ? (payload.valueRatio ?? null) : null,
      valuePct: hasDenominator ? (payload.valuePct ?? null) : null,
      extractionMethod: payload.extractionMethod ?? "NONE",
      source: payload.source ?? "NOT_DISCLOSED",
      confidence: payload.confidence ?? "NOT_DISCLOSED",
      filingType: payload.filingType ?? filingType,
      filingDate: payload.filingDate ?? null,
      accessionNumber: payload.accessionNumber ?? null,
      documentUrl: payload.documentUrl ?? null,
      indexUrl: payload.indexUrl ?? null,
      primaryDocumentUrl: payload.primaryDocumentUrl ?? null,
      evidence: payload.evidence ?? {},
      calculation: payload.calculation ?? null,
      scanCoverage: payload.scanCoverage ?? null,
      searchedTerms: payload.searchedTerms ?? [],
      notDisclosedBasis: payload.notDisclosedBasis ?? null,
      status: payload.status ?? (payload.value != null ? "FOUND" : (payload.confidence ?? "NOT_DISCLOSED")),
      code: payload.code ?? null,
      ...(typeof payload.retryable === "boolean" ? { retryable: payload.retryable } : {}),
      xbrlContext: payload.xbrlContext ?? null,
      warnings,
    };
    if (
      shaped.value != null
      && shaped.extractionMethod === "XBRL"
      && shaped.xbrlContext == null
      && !(shaped.warnings as Record<string, unknown>[]).some((w) => w.code === "XBRL_CONTEXT_METADATA_UNAVAILABLE")
    ) {
      (shaped.warnings as unknown[]).push({
        code: "XBRL_CONTEXT_METADATA_UNAVAILABLE",
        message: "XBRL fact value was found, but SEC context metadata was unavailable.",
        severity: "warning",
      });
    }
    if (addDenominatorWarning && shaped.value != null && shaped.denominator == null) {
      (shaped.warnings as unknown[]).push({
        code: "DENOMINATOR_NOT_FOUND",
        message: "Could not compute geographic revenue percentage due to missing denominator.",
        severity: "warning",
      });
    }
    return JSON.stringify(shaped);
  };

  const config = FILING_FACT_CONCEPTS[factType];
  if (!config) return JSON.stringify({ error: true, message: `Unsupported fact_type '${factType}'`, ticker });
  // A period this reader cannot resolve is refused, never answered with an arbitrary fact (2.5.16).
  const namedPeriod = parseFilingPeriod(period);
  if (!namedPeriod) {
    return JSON.stringify({
      ticker, factType, value: null, unit: null, period: null, filingType,
      extractionMethod: "NONE", source: "INPUT", confidence: "NOT_DECISION_GRADE",
      status: "INVALID_PERIOD", code: "INVALID_PERIOD", decisionGrade: false, requestedPeriod: period,
      warnings: [{ code: "INVALID_PERIOD", message: FILING_PERIOD_HELP, severity: "error" }],
    });
  }
  if (factType === "geographic_revenue" && !region) {
    return JSON.stringify({ error: true, message: "region is required for fact_type='geographic_revenue'", ticker });
  }

  const cikPadded = await resolveCikForTicker(ticker);
  const unavailableStructuredFact = async (
    code: string,
    message: string,
    conceptName: string | null,
    requestedAccession: string | null = null,
    extra: Record<string, unknown> = {},
  ): Promise<string> => {
    // A failed pin names the filing that was asked for, never the latest one.
    const resolved = requestedAccession ? null : await resolveSecFiling(ticker, filingType, null).catch(() => null);
    const filing = resolved?.ok ? resolved.filing : null;
    const filingWarnings = resolved?.ok ? resolved.filing.warnings : [];
    return JSON.stringify({
      ticker,
      factType,
      concept: conceptName,
      value: null,
      denominator: null,
      valueRatio: null,
      valuePct: null,
      rawValue: null,
      rawDenominator: null,
      unit: "USD",
      unitScale: "actual",
      period: null,
      filingType: filing?.filingType ?? filingType,
      filingDate: filing?.filingDate ?? null,
      accessionNumber: requestedAccession ?? filing?.accessionNumber ?? null,
      ...(requestedAccession ? { requestedAccession } : {}),
      documentUrl: filing?.documentUrl ?? null,
      indexUrl: null,
      primaryDocumentUrl: filing?.documentUrl ?? null,
      extractionMethod: "NONE",
      source: "SEC_COMPANYCONCEPT",
      confidence: "NOT_DECISION_GRADE",
      status: "SEC_FACT_NOT_AVAILABLE",
      code,
      decisionGrade: false,
      evidence: filing ? {
        sourceType: "sec_filing",
        filingType: filing.filingType,
        filingDate: filing.filingDate,
        accessionNumber: filing.accessionNumber,
        documentUrl: filing.documentUrl,
      } : {},
      xbrlContext: null,
      calculation: null,
      warnings: [
        ...filingWarnings,
        ...readWarnings,
        { code, message, severity: "warning" },
      ],
      _manualLookup: filingManualLookup(ticker, cikPadded, filingType),
      ...extra,
    });
  };
  if (!cikPadded) {
    // No CIK is a lookup outcome, not a disclosure: say whether SEC's ticker
    // index was unreachable (retry) or has no such registrant, and keep the
    // accession a caller pinned.
    const tickerIndex = await getEdgarTickerCikMap().catch(() => null);
    const code = tickerIndex ? "NO_SEC_REGISTRANT" : "SEC_LOOKUP_UNAVAILABLE";
    const requested = pinAccession && pinAccession.trim() ? pinAccession.trim() : null;
    const message = tickerIndex
      ? `SEC has no registrant CIK for ticker ${ticker.toUpperCase()}.`
      : `SEC's ticker index could not be read, so ${ticker.toUpperCase()} was not resolved to a CIK; retry.`;
    return withGeoShape({
      ticker,
      factType,
      value: null,
      denominator: null,
      valueRatio: null,
      valuePct: null,
      filingType,
      accessionNumber: requested,
      ...(requested ? { requestedAccession: requested } : {}),
      extractionMethod: "NONE",
      source: "NONE",
      confidence: "NOT_DECISION_GRADE",
      // An unreadable ticker index is a failed read, like SEC_READ_FAILED, not a missing fact (2.5.18).
      status: code === "SEC_LOOKUP_UNAVAILABLE" ? "PROVIDER_ERROR" : "SEC_FACT_NOT_AVAILABLE",
      code,
      retryable: code === "SEC_LOOKUP_UNAVAILABLE",
      evidence: null,
      warnings: [{ code, message, severity: "warning" }],
      _manualLookup: filingManualLookup(ticker, null, filingType),
    });
  }

  // A read that failed (not a 404) leaves a fact's absence unproven: the result is SEC_READ_FAILED, never
  // "no facts" (2.5.17).
  const failedReads: { endpoint: string; concept: string | null; httpStatus: number | null }[] = [];
  const readSecJson = async (url: string, endpoint: string, conceptName: string | null): Promise<Record<string, unknown> | null> => {
    const read = await edgarReadJson(url);
    if (read.ok) return read.json;
    failedReads.push({ endpoint, concept: conceptName, httpStatus: read.httpStatus });
    return null;
  };
  const companyfactsUrl = secCompanyFactsUrl(cikPadded);
  const secReadFailed = (conceptName: string | null): Promise<string> => {
    // Reads finish in network order; list them in candidate order, companyfacts last, so the result is stable.
    const rank = (r: { endpoint: string; concept: string | null }): number =>
      r.endpoint === "companyconcept" ? candidateNames.indexOf(r.concept ?? "") : candidateNames.length;
    failedReads.sort((a, b) => rank(a) - rank(b));
    const reads = failedReads
      .map((r) => `${r.endpoint}${r.concept ? ` ${r.concept}` : ""} (${r.httpStatus != null ? `HTTP ${r.httpStatus}` : "no response"})`)
      .join(", ");
    return unavailableStructuredFact(
      "SEC_READ_FAILED",
      `SEC could not be read for ${ticker.toUpperCase()}: ${reads}. Whether the fact exists is unknown; retry.`,
      conceptName,
      pinAccession && pinAccession.trim() ? pinAccession.trim() : null,
      { status: "PROVIDER_ERROR", retryable: true, failedReads: [...failedReads] },
    );
  };
  const fetchConcept = async (concept: string): Promise<Record<string, unknown> | null> =>
    readSecJson(secCompanyConceptUrl(cikPadded, concept), "companyconcept", concept);

  // Every equivalent concept is read; the one with the newest filing of this
  // form wins, so a filer that switched concepts is not read from its old one.
  const candidateNames = [config.primary, ...(config.alternates ?? []), ...(config.fallback ? [config.fallback] : [])];
  // SEC's companyconcept endpoint has served a concept's USD facts as an empty object instead of a list (BE,
  // 2026-09): those facts are read from companyfacts, which carries them (2.5.15).
  const malformedConcepts: string[] = [];
  let companyfacts: Promise<Record<string, unknown> | null> | null = null;
  const conceptRows = async (name: string): Promise<Record<string, unknown>[]> => {
    const usd = ((await fetchConcept(name))?.units as Record<string, unknown> | undefined)?.USD;
    if (usd == null || Array.isArray(usd)) return (usd as Record<string, unknown>[] | undefined) ?? [];
    malformedConcepts.push(name);
    companyfacts ??= readSecJson(companyfactsUrl, "companyfacts", null);
    const rows = (((((await companyfacts)?.facts as Record<string, unknown> | undefined)?.["us-gaap"] as Record<string, unknown> | undefined)?.[name] as Record<string, unknown> | undefined)?.units as Record<string, unknown> | undefined)?.USD;
    return Array.isArray(rows) ? rows as Record<string, unknown>[] : [];
  };
  const fetchedConcepts = await Promise.all(candidateNames.map(async (name) => ({ concept: name, facts: await conceptRows(name) })));
  if (malformedConcepts.length) {
    readWarnings.push({
      code: "SEC_COMPANYCONCEPT_MALFORMED",
      message: `SEC companyconcept returned no fact list for ${malformedConcepts.join(", ")}; those facts were read from companyfacts.`,
      severity: "info",
    });
  }
  if (failedReads.length) return secReadFailed(config.primary);
  const pinnedAccession = pinAccession && pinAccession.trim() ? pinAccession.trim() : null;
  const chosen = pickConceptFacts(fetchedConcepts, filingType, pinnedAccession, factType === "total_revenue" ? "larger" : "first");
  const concept = chosen?.concept ?? config.primary;
  let filtered = chosen?.facts ?? [];
  // A pinned geographic read with no XBRL fact reads that filing's own table below, as the unpinned read does (2.23.0, F4).
  if (!filtered.length && pinnedAccession && factType !== "geographic_revenue") {
    // A pinned IFRS filing was searched for us-gaap concepts only: say IFRS was not read, not that the filing
    // has no such fact (2.5.30, F-018).
    companyfacts ??= readSecJson(companyfactsUrl, "companyfacts", null);
    const taxonomies = (((await companyfacts)?.facts ?? {}) as Record<string, unknown>);
    if (failedReads.length) return secReadFailed(concept);
    if (taxonomies["ifrs-full"] && !taxonomies["us-gaap"]) {
      return unavailableStructuredFact(
        "SEC_FACTS_IFRS_ONLY",
        `${ticker.toUpperCase()} reports its SEC facts under IFRS (ifrs-full), which this action does not read, so accession ${pinnedAccession} was searched for ${candidateNames.join(" / ")} (us-gaap) only and its IFRS facts were not searched; reconcile_metric_sources reads IFRS facts in the reporting currency.`,
        concept,
        pinnedAccession,
      );
    }
    return unavailableStructuredFact(
      "NO_FACT_FOR_ACCESSION",
      `SEC companyconcept has no ${candidateNames.join(" / ")} fact in accession ${pinnedAccession} (${filingType}).`,
      concept,
      pinnedAccession,
    );
  }
  if (!filtered.length) {
    if (factType !== "geographic_revenue") {
      // An IFRS filer (TSM's 20-F) tags ifrs-full, never us-gaap: say so rather than report the fact missing (2.5.15).
      companyfacts ??= readSecJson(companyfactsUrl, "companyfacts", null);
      const taxonomies = (((await companyfacts)?.facts ?? {}) as Record<string, unknown>);
      if (failedReads.length) return secReadFailed(concept);
      if (taxonomies["ifrs-full"] && !taxonomies["us-gaap"]) {
        return unavailableStructuredFact(
          "SEC_FACTS_IFRS_ONLY",
          `${ticker.toUpperCase()} reports its SEC facts under IFRS (ifrs-full), which this action does not read; reconcile_metric_sources reads IFRS facts in the reporting currency.`,
          concept,
        );
      }
      return unavailableStructuredFact(
        "NO_COMPANYCONCEPT_FACT_FOR_FORM",
        `SEC companyconcept has no ${concept} facts for filing type ${filingType}.`,
        concept,
      );
    }
    // For geographic_revenue, fall through to HTML fallback below (picked remains null)
  }
  if (filtered.length && namedPeriod.kind === "latest" && !pinnedAccession) {
    const latestFiled = filtered.map((f) => String(f.filed ?? "")).sort().slice(-1)[0];
    filtered = filtered.filter((f) => String(f.filed ?? "") === latestFiled);
  }

  // ── period_mode filtering ─────────────────────────────────────────────
  // XBRL facts include both quarterly and YTD figures for the same filing.
  // Filter by duration to avoid returning 6-month revenue as quarterly.
  let resolvedMode = (periodMode || "auto").toLowerCase().trim();
  if (resolvedMode === "auto") {
    resolvedMode = filingType.toUpperCase() === "10-Q" ? "quarter" : "annual";
  }
  if ((resolvedMode === "quarter" || resolvedMode === "annual" || resolvedMode === "ytd") && filtered.length) {
    const durationDays = (f: Record<string, unknown>): number | null => {
      const s = f.start as string | undefined;
      const e = f.end as string | undefined;
      if (!s || !e) return null;
      try {
        const d0 = new Date(s);
        const d1 = new Date(e);
        if (isNaN(d0.getTime()) || isNaN(d1.getTime())) return null;
        return Math.round((d1.getTime() - d0.getTime()) / (1000 * 60 * 60 * 24));
      } catch {
        return null;
      }
    };
    const tagged = filtered.map((f) => ({ f, d: durationDays(f) }));
    let modeFiltered: Record<string, unknown>[];
    if (resolvedMode === "quarter") {
      modeFiltered = tagged.filter(({ d }) => d == null || (d >= 60 && d <= 110)).map(({ f }) => f);
    } else if (resolvedMode === "ytd") {
      modeFiltered = tagged.filter(({ f, d }) => {
        if (d == null) return true;
        const fp = String(f.fp ?? "").toUpperCase().trim();
        if (fp === "Q1") return d >= 60 && d <= 110;
        if (fp === "Q2") return d >= 150 && d <= 200;
        if (fp === "Q3") return d >= 240 && d <= 290;
        const form = String(f.form ?? "").toUpperCase().trim();
        if (form === "10-K") return d >= 340 && d <= 400;
        return d >= 60;
      }).map(({ f }) => f);
    } else {
      modeFiltered = tagged.filter(({ d }) => d == null || (d >= 340 && d <= 400)).map(({ f }) => f);
    }
    if (modeFiltered.length) {
      filtered = modeFiltered;
    }
  }
  // A named fiscal year is an annual period: the rows for the period the issuer calls that year (2.5.16).
  let namedYearLabel: string | null = null;
  if (namedPeriod.kind === "fiscalYear") {
    if (resolvedMode !== "annual") {
      return unavailableStructuredFact(
        "INVALID_PERIOD",
        `period ${period} names a fiscal year, an annual period; request filing_type 10-K or 20-F (or period_mode annual).`,
        concept,
      );
    }
    const selected = selectFiscalYearRows(filtered, namedPeriod.year);
    if (!selected.rows.length) {
      return unavailableStructuredFact(
        "PERIOD_NOT_FOUND",
        `No annual ${concept} fact is fiscal year ${namedPeriod.year} (the issuer's stated fiscal year, else the year the period ends); fiscal years found: ${selected.fiscalYears.slice(-6).join(", ") || "none"}.`,
        concept,
      );
    }
    filtered = selected.rows;
    namedYearLabel = `FY${namedPeriod.year}`;
  }
  if (namedPeriod.kind === "latest" && filtered.length) {
    filtered = [...filtered].sort((a, b) => {
      const endCmp = String(b.end ?? "").localeCompare(String(a.end ?? ""));
      if (endCmp !== 0) return endCmp;
      const fyCmp = Number(b.fy ?? 0) - Number(a.fy ?? 0);
      if (fyCmp !== 0) return fyCmp;
      return String(b.filed ?? "").localeCompare(String(a.filed ?? ""));
    });
  }

  if (factType === "segment_revenue") {
    const allSegments = filtered
      .map((f) => {
        const segmentLabel = normalizeSegmentLabel(f.segment);
        if (!segmentLabel) return null;
        const xbrlContext = buildXbrlFactContext(f, concept, resolvedMode);
        return {
          segmentLabel,
          value: f.val ?? null,
          fiscalYear: String(f.fy ?? ""),
          fiscalPeriod: String(f.fp ?? ""),
          filingDate: String(f.filed ?? ""),
          accessionNumber: String(f.accn ?? ""),
          xbrlContext,
        };
      })
      .filter((v) => v != null);
    return JSON.stringify({
      ticker,
      factType,
      concept,
      value: allSegments[0]?.value ?? null,
      fiscalYear: allSegments[0]?.fiscalYear ?? null,
      fiscalPeriod: allSegments[0]?.fiscalPeriod ?? null,
      filingType,
      filingDate: allSegments[0]?.filingDate ?? null,
      accessionNumber: allSegments[0]?.accessionNumber ?? null,
      xbrlContext: allSegments[0]?.xbrlContext ?? null,
      extractionMethod: "XBRL",
      source: "XBRL",
      confidence: "HIGH",
      allSegments,
    });
  }

  let picked: Record<string, unknown> | null = null;
  let valueRatio: number | null = null;
  let valuePct: number | null = null;
  let denominator: number | null = null;
  let segmentLabel: string | null = null;
  if (factType === "geographic_revenue") {
    picked = filtered.find((f) => {
      const label = normalizeSegmentLabel(f.segment);
      if (!label) return false;
      if (regionMatches(label, region ?? "", false)) {
        segmentLabel = label;
        return true;
      }
      return false;
    }) ?? null;
    if (!picked && (region ?? "").toLowerCase() === "china") {
      picked = filtered.find((f) => {
        const label = normalizeSegmentLabel(f.segment);
        if (!label) return false;
        if (regionMatches(label, region ?? "", true)) {
          segmentLabel = label;
          return true;
        }
        return false;
      }) ?? null;
    }
    if (picked) {
      const accn = String(picked.accn ?? "");
      // The total for the same period: a 10-Q also carries the prior year's total (2.5.9, MRVL).
      const total = filtered.find((f) => String(f.accn ?? "") === accn && f.segment == null
        && String(f.start ?? "") === String(picked!.start ?? "") && String(f.end ?? "") === String(picked!.end ?? "")) ?? null;
      const totalVal = total ? Number(total.val ?? 0) : 0;
      const partVal = picked.val != null ? Number(picked.val) : null;
      if (partVal != null && totalVal > 0) {
        denominator = totalVal;
        valueRatio = Math.round((partVal / totalVal) * RATIO_SCALE) / RATIO_SCALE;
        valuePct = ratioToPct(valueRatio);
      }
    }
  } else {
    picked = filtered.find((f) => f.segment == null) ?? filtered[0] ?? null;
  }

  if (!picked) {
    // ── HTML fallback for geographic_revenue ────────────────────────────────
    // Some companies (e.g. GLW) do not XBRL-tag geographic-revenue segments.
    // Fall through to the same HTML-parsing path used by searchFilingText.
    if (factType === "geographic_revenue") {
      // A pinned read falls back to that filing's tables, never to the latest filing's (2.5.18).
      const resolved = await resolveSecFiling(ticker, filingType, pinnedAccession);
      if (!resolved.ok) {
        // An accession miss reads as before here: the structured fact path is not one 2.13.0 fails closed.
        const error = isAccessionNotFound(resolved.error) ? await legacyAccessionMissPayload(ticker, filingType) : resolved.error;
        return withGeoShape({
          ...error,
          factType,
          region,
          value: null,
          denominator: null,
          valueRatio: null,
          valuePct: null,
          extractionMethod: "NONE",
          source: error.code,
          confidence: error.code,
          filingType,
          evidence: {},
        });
      }
      const filing = resolved.filing;
      const htmlText = await edgarGetHtml(filing.documentUrl, 5_000_000);
      if (htmlText) {
        const geo = extractGeoRevenueFromHtml(htmlText, region ?? "");
        if (geo) {
          // A quarterly report's table is a quarter or year-to-date, never a fiscal year: its own column
          // header says which ("Three Months Ended August 1, 2026"), not the filing date (2.5.9, MRVL). An
          // annual report's year is its period of report's fiscal year, not its filing date's (2.5.11).
          const annual = /^(?:10-K|20-F|40-F)/i.test(String(filing.filingType ?? ""));
          // The fiscal year the filing states for itself wins over the period-end rule (DG's year ending January 2026 is its FY2025) (2.5.13).
          const focus = annual ? documentFiscalYearFocus(htmlText) : null;
          // A column header with no year ("Three Months Ended" alone, the date in a row below) names no period, so
          // the label is null rather than the header (2.26.5, F60).
          const quarterColumn = geo.sourceColumns[0] ?? "";
          const fiscalYear = !annual
            ? (/\b\d{4}\b/.test(quarterColumn) ? quarterColumn : "")
            : (filingFiscalYearLabel(focus, filing.reportDate ?? null) ?? "");
          const warnings: Array<Record<string, unknown>> = geo.denominator == null && geo.usd != null
            ? [{
                code: "DENOMINATOR_NOT_FOUND",
                message: "Could not compute geographic revenue percentage due to missing denominator.",
                severity: "warning",
              }]
            : [];
          const scaleAssumed = geo.unitScaleSource === "ASSUMED_MILLIONS";
          if (scaleAssumed) warnings.push(UNIT_SCALE_ASSUMED_WARNING);
          return withGeoShape({
            ticker,
            factType,
            region,
            period: fiscalYear || null,
            rawValue: geo.rawValue ?? formatRawNumber(geo.usd ?? null),
            rawDenominator: geo.rawDenominator ?? formatRawNumber(geo.denominator ?? null),
            unit: "USD",
            unitScale: geo.unitScale,
            unitScaleSource: geo.unitScaleSource,
            value: geo.usd ?? null,
            denominator: geo.denominator ?? null,
            valueRatio: geo.pct,
            valuePct: geo.denominator != null ? ratioToPct(geo.pct) : null,
            extractionMethod: "PARSED_TABLE",
            source: "PARSED_TABLE",
            // An assumed scale leaves the amounts unproven, not the share (2.5.21).
            confidence: geo.denominator == null ? "LOW" : scaleAssumed ? "MEDIUM" : "HIGH",
            filingType: filing.filingType,
            filingDate: filing.filingDate,
            accessionNumber: filing.accessionNumber,
            documentUrl: filing.documentUrl,
            indexUrl: null,
            primaryDocumentUrl: filing.documentUrl,
            evidence: {
              sectionHeading: geo.sectionHeading || null,
              tableTitle: null,
              sourceTableId: 1,
              sourceRows: geo.sourceRows,
              sourceColumns: geo.sourceColumns.length > 0 ? geo.sourceColumns : [fiscalYear],
              // The filing the table came from, so the standardised evidence row names its document (2.14.0).
              documentUrl: filing.documentUrl,
              filingType: filing.filingType,
              accessionNumber: filing.accessionNumber,
              filingDate: filing.filingDate,
            },
            calculation: geo.denominator != null
              ? {
                  formula: "value / denominator * 100",
                  valueSource: "sourceRows[0]",
                  denominatorSource: "sourceRows[1]",
                  resultPct: ratioToPct(geo.pct),
                }
              : null,
            warnings: [...warnings, ...filing.warnings],
          });
        }
        const regionText = String(region ?? "");
        const searchedTerms = geoRegionAliases(regionText);
        const relevantGeoText = filingHasRelevantGeoText(htmlText, regionText);
        const filingReadTruncated = htmlText.length >= SEC_DOCUMENT_READ_MAX_CHARS;
        const scanCoverage = {
          sourceType: "sec_primary_html",
          documentUrl: filing.documentUrl,
          charsScanned: htmlText.length,
          maxCharsRequested: SEC_DOCUMENT_READ_MAX_CHARS,
          filingReadTruncated,
          relevantGeoTextFound: relevantGeoText,
          searchedTerms,
        };
        if (relevantGeoText || filingReadTruncated) {
          const extractionWarnings = [...filing.warnings];
          if (relevantGeoText) {
            extractionWarnings.push({ code: "TABLE_NOT_PARSED", message: "Relevant filing text exists, but no geographic revenue table was parsed.", severity: "warning" });
          }
          if (filingReadTruncated) {
            extractionWarnings.push({ code: "FILING_READ_TRUNCATED", message: "Geographic revenue scan read only the bounded filing prefix; relevant tables may appear later in the filing.", severity: "warning" });
          }
          return withGeoShape({
            ticker,
            factType,
            region,
            value: null,
            denominator: null,
            valueRatio: null,
            valuePct: null,
            extractionMethod: "NONE",
            source: "EXTRACTION_FAILED",
            confidence: "EXTRACTION_FAILED",
            status: "EXTRACTION_FAILED",
            code: "EXTRACTION_FAILED",
            filingType: filing.filingType,
            filingDate: filing.filingDate,
            accessionNumber: filing.accessionNumber,
            documentUrl: filing.documentUrl,
            primaryDocumentUrl: filing.documentUrl,
            evidence: {
              sourceType: "sec_filing",
              filingType: filing.filingType,
              filingDate: filing.filingDate,
              accessionNumber: filing.accessionNumber,
              documentUrl: filing.documentUrl,
            },
            scanCoverage,
            searchedTerms,
            warnings: extractionWarnings,
          });
        }
        return withGeoShape({
          ticker,
          factType,
          region,
          value: null,
          denominator: null,
          valueRatio: null,
          valuePct: null,
          extractionMethod: "NONE",
          source: "NOT_DISCLOSED",
          confidence: "NOT_DISCLOSED",
          status: "NOT_DISCLOSED",
          filingType: filing.filingType,
          filingDate: filing.filingDate,
          accessionNumber: filing.accessionNumber,
          documentUrl: filing.documentUrl,
          primaryDocumentUrl: filing.documentUrl,
          evidence: {
            sourceType: "sec_filing",
            filingType: filing.filingType,
            filingDate: filing.filingDate,
            accessionNumber: filing.accessionNumber,
            documentUrl: filing.documentUrl,
          },
          scanCoverage,
          searchedTerms,
          notDisclosedBasis: "Resolved and scanned the primary SEC filing HTML; no matching geographic revenue disclosure text or parseable table was found for the requested region.",
          warnings: [...filing.warnings],
        });
      }
      return withGeoShape({
        ticker,
        factType,
        region,
        value: null,
        denominator: null,
        valueRatio: null,
        valuePct: null,
        extractionMethod: "NONE",
        source: "EXTRACTION_FAILED",
        confidence: "EXTRACTION_FAILED",
        status: "EXTRACTION_FAILED",
        code: "FILING_TEXT_NOT_AVAILABLE",
        filingType: filing.filingType,
        filingDate: filing.filingDate,
        accessionNumber: filing.accessionNumber,
        documentUrl: filing.documentUrl,
        primaryDocumentUrl: filing.documentUrl,
        evidence: {
          sourceType: "sec_filing",
          filingType: filing.filingType,
          filingDate: filing.filingDate,
          accessionNumber: filing.accessionNumber,
          documentUrl: filing.documentUrl,
        },
        warnings: [
          ...filing.warnings,
          { code: "FILING_TEXT_NOT_AVAILABLE", message: "Resolved SEC filing, but primary HTML text could not be fetched for geographic revenue scan.", severity: "warning" },
        ],
      });
    }
    return withGeoShape({
      ticker,
      factType,
      value: null,
      denominator: null,
      valueRatio: null,
      valuePct: null,
      extractionMethod: "NONE",
      source: "NOT_DISCLOSED",
      confidence: "NOT_DISCLOSED",
      evidence: {},
      warnings: [],
      _manualLookup: filingManualLookup(ticker, cikPadded, filingType),
    });
  }

  const accessionNumber = String(picked.accn ?? "") || null;
  let indexUrl: string | null = null;
  let primaryDocumentUrl: string | null = null;
  if (accessionNumber) {
    const baseUrls = edgarBuildFilingUrls(parseInt(cikPadded, 10), accessionNumber, null);
    indexUrl = baseUrls.edgarIndexUrl;
    const { submissions } = await getSubmissionsForTicker(ticker);
    const recent = (((submissions?.filings as Record<string, unknown>)?.recent) as Record<string, unknown[]> | undefined) ?? {};
    const accessions = (recent.accessionNumber as string[]) ?? [];
    const primaryDocs = (recent.primaryDocument as string[]) ?? [];
    const idx = accessions.findIndex((a) => String(a) === accessionNumber);
    const primaryDoc = idx >= 0 && idx < primaryDocs.length ? primaryDocs[idx] : null;
    if (primaryDoc) {
      primaryDocumentUrl = edgarBuildFilingUrls(parseInt(cikPadded, 10), accessionNumber, primaryDoc).edgarPrimaryDocumentUrl;
    }
  }
  const documentUrl = primaryDocumentUrl ?? indexUrl;
  const periodLabelRaw = String(picked.fy ?? "");
  // A named year is labelled as the period it selected, not by the fy of the filing that carried it (2.5.16).
  const periodLabel = namedYearLabel ?? (periodLabelRaw && !periodLabelRaw.startsWith("FY") ? `FY${periodLabelRaw}` : periodLabelRaw);
  const valueNum = picked.val != null ? Number(picked.val) : null;
  const rawValue = formatRawNumber(valueNum);
  const rawDenominator = formatRawNumber(denominator);

  // ── XBRL context metadata ─────────────────────────────────────────────
  let durationDaysVal: number | null = null;
  if (picked.start && picked.end) {
    try {
      const d0 = new Date(String(picked.start));
      const d1 = new Date(String(picked.end));
      if (!isNaN(d0.getTime()) && !isNaN(d1.getTime())) {
        durationDaysVal = Math.round((d1.getTime() - d0.getTime()) / (1000 * 60 * 60 * 24));
      }
    } catch { /* ignore */ }
  }
  const xbrlContext = {
    concept,
    taxonomy: "us-gaap",
    unit: "USD",
    periodStart: picked.start ?? null,
    periodEnd: picked.end ?? null,
    instant: picked.start ? null : (picked.end ?? null),
    durationDays: durationDaysVal,
    fiscalPeriod: String(picked.fp ?? ""),
    fiscalYear: String(picked.fy ?? ""),
    form: String(picked.form ?? ""),
    frame: picked.frame ?? null,
    accessionNumber: picked.accn ?? null,
    filedAt: picked.filed ?? null,
    periodMode: resolvedMode,
    dimensions: {
      ...(segmentLabel ? { segment: segmentLabel } : {}),
      ...(picked.segment != null ? { rawSegment: picked.segment } : {}),
    },
  };

  const resultWarnings: Record<string, unknown>[] = [];
  if (resolvedMode === "quarter" && durationDaysVal != null && durationDaysVal > 110) {
    resultWarnings.push({
      code: "PERIOD_MODE_MISMATCH",
      message: `Requested quarter but picked fact has ${durationDaysVal}-day duration. No quarterly fact available.`,
      severity: "warning",
    });
  }

  return withGeoShape({
    ticker,
    factType,
    region,
    period: periodLabel || null,
    rawValue,
    rawDenominator,
    unit: "USD",
    unitScale: "actual",
    value: valueNum,
    denominator: factType === "geographic_revenue" ? denominator : null,
    valueRatio: factType === "geographic_revenue" ? valueRatio : null,
    valuePct: factType === "geographic_revenue" ? valuePct : null,
    extractionMethod: "XBRL",
    source: "XBRL",
    confidence: factType !== "geographic_revenue" || denominator != null ? "HIGH" : "LOW",
    filingType,
    filingDate: String(picked.filed ?? ""),
    accessionNumber,
    documentUrl,
    indexUrl,
    primaryDocumentUrl,
    xbrlContext,
    evidence: {
      sectionHeading: segmentLabel,
      tableTitle: null,
      sourceTableId: null,
      sourceRows: [
        [segmentLabel ?? (region ?? "Region"), rawValue ?? ""],
        ["Total revenue", rawDenominator ?? ""],
      ],
      sourceColumns: [periodLabel || String(picked.fp ?? "")],
    },
    calculation: factType === "geographic_revenue" && denominator != null
      ? {
          formula: "value / denominator * 100",
          valueSource: "sourceRows[0]",
          denominatorSource: "sourceRows[1]",
          resultPct: valuePct,
        }
      : null,
    warnings: resultWarnings,
  }, factType === "geographic_revenue" && denominator == null);
}

export interface FilingSearchOptions {
  /** search_query syntax: "exact phrase", A NEAR/n B, -excluded. */
  query?: string | null;
  excludeTerms?: unknown[];
  near?: unknown[];
  match?: string | null;
  order?: string | null;
  maxMatches?: number;
  cursor?: string | null;
  filingCount?: number;
  since?: string | null;
  includeExhibits?: boolean;
}

// fiscalYearFocus: the fiscal year the document tags for itself (dei:DocumentFiscalYearFocus), read before truncation.
type ProjectedFilingDocument = { projection: FilingTextProjection; htmlChars: number; truncated: boolean; fiscalYearFocus: number | null };

type FilingSearchTarget = {
  key: string;
  url: string;
  documentType: string;
  defaultSection: string | null;
  primary: boolean;
  accessionNumber: string | null;
  filingDate: string | null;
  filingType: string;
};

const FILING_TEXT_TTL_MS = 30 * 60 * 1000;
// Display text and its folded copy of recently searched documents.
const filingTextProjections = new BoundedTtlCache<ProjectedFilingDocument>(24, 12_000_000, (doc) => doc.projection.text.length * 2);
const FILING_SEARCH_MAX_FILINGS = 5;
const FILING_SEARCH_MAX_EXHIBITS = 4;
const FILING_SEARCH_TABLE_ROWS = 40;

/** Item and Part headings; filings that mark them with <h1>-<h6> instead of bold text use those. */
export function searchHeadings(html: string): HeadingAnchor[] {
  const bold = filingItemHeadings(html);
  if (bold.length > 0) return bold.map(({ start, level, title }) => ({ start, level, title }));
  const out: HeadingAnchor[] = [];
  for (const m of html.matchAll(/<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi)) {
    const start = m.index ?? 0;
    const title = stripHtmlTags(m[2]).trim();
    const level = /^part\s+[ivx]+\b/i.test(title) ? 1 : /^item\s+\d+[a-z]?\b/i.test(title) ? 2 : 0;
    if (level === 0 || /\s\d{1,3}$/.test(title) || isTocMatch(html, start, start + m[0].length)) continue;
    out.push({ start, level, title: title.slice(0, 200) });
  }
  return out;
}

/** A filing document as searchable display text, projected once per isolate. */
async function projectedFilingDocument(url: string): Promise<ProjectedFilingDocument | null> {
  const cached = filingTextProjections.get(url);
  if (cached) {
    countCacheEvent("sec", "memoryHits");
    return cached;
  }
  let doc: SecDocumentResult;
  try {
    doc = await fetchSecDocument(url);
  } catch {
    return null;
  }
  if (!doc.ok) return null;
  const truncated = doc.text.length > SEC_DOCUMENT_READ_MAX_CHARS;
  const html = truncated ? doc.text.slice(0, SEC_DOCUMENT_READ_MAX_CHARS) : doc.text;
  const projection = projectFilingText(html, searchHeadings(html), filingTableSpans(html));
  const entry = { projection, htmlChars: doc.text.length, truncated, fiscalYearFocus: documentFiscalYearFocus(doc.text) };
  filingTextProjections.set(url, entry, FILING_TEXT_TTL_MS);
  return entry;
}

function nearSpecsFromOptions(values: unknown[] | undefined): NearSpec[] {
  const out: NearSpec[] = [];
  for (const value of values ?? []) {
    if (!value || typeof value !== "object") continue;
    const item = value as Record<string, unknown>;
    const terms = termsFromList(Array.isArray(item.terms) ? item.terms : []);
    if (terms.length !== 2) continue;
    const within = Number(item.within_words ?? item.withinWords ?? 10);
    out.push({ a: terms[0], b: terms[1], withinWords: Math.max(1, Math.min(200, Number.isFinite(within) ? Math.trunc(within) : 10)) });
  }
  return out;
}

/** The latest filings of a form for a multi-filing search, newest first. */
async function resolveSecFilingsForSearch(
  ticker: string,
  first: ResolvedSecFiling,
  filingCount: number,
  since: string | null,
): Promise<{ filings: ResolvedSecFiling[]; capped: boolean }> {
  if (filingCount <= 1 && !since) return { filings: [first], capped: false };
  const { submissions } = await getSubmissionsForTicker(ticker);
  const recent = ((submissions?.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form as string[]) ?? [];
  const accessions = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs = (recent.primaryDocument as string[]) ?? [];
  const filingDates = (recent.filingDate as string[]) ?? [];
  const reportDates = (recent.reportDate as string[]) ?? [];
  const acceptedDts = (recent.acceptanceDateTime as string[]) ?? [];
  const wanted = first.filingType.toUpperCase();
  const limit = since && filingCount <= 1 ? FILING_SEARCH_MAX_FILINGS : filingCount;
  const filings: ResolvedSecFiling[] = [];
  let capped = false;
  for (let i = 0; i < forms.length; i++) {
    if (String(forms[i]).toUpperCase() !== wanted || !accessions[i] || !primaryDocs[i]) continue;
    if (since && String(filingDates[i] ?? "") < since) continue;
    const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(first.cikInt, accessions[i], String(primaryDocs[i]));
    if (!edgarPrimaryDocumentUrl || isLikelyXbrlDocumentUrl(edgarPrimaryDocumentUrl)) continue;
    if (filings.length >= limit) {
      capped = since != null;
      break;
    }
    filings.push({
      ...first,
      filingType: String(forms[i]),
      filingDate: filingDates[i] ?? null,
      reportDate: reportDates[i] || null,
      acceptedAt: acceptedDts[i] ?? null,
      accessionNumber: accessions[i],
      primaryDocument: String(primaryDocs[i]),
      documentUrl: edgarPrimaryDocumentUrl,
      warnings: [],
    });
  }
  return { filings: filings.length > 0 ? filings : [first], capped };
}

async function exhibitTargets(cikInt: number, accessionNumber: string, filingDate: string | null, filingType: string, primaryUrl: string): Promise<FilingSearchTarget[]> {
  const { edgarIndexUrl } = edgarBuildFilingUrls(cikInt, accessionNumber, null);
  const out: FilingSearchTarget[] = [];
  for (const exhibit of await edgarListExhibitsFromIndex(edgarIndexUrl)) {
    const type = String(exhibit.type ?? "").toUpperCase();
    const url = typeof exhibit.documentUrl === "string" ? exhibit.documentUrl : "";
    if (!/^EX-99/.test(type) || !/\.(?:htm|html|txt)$/i.test(url) || url === primaryUrl) continue;
    const description = String(exhibit.description ?? "").trim();
    out.push({
      key: url,
      url,
      documentType: type,
      // The filing index often repeats the type as the description.
      defaultSection: description && description.toUpperCase() !== type ? `${type}: ${description}` : type,
      primary: false,
      accessionNumber,
      filingDate,
      filingType,
    });
    if (out.length >= FILING_SEARCH_MAX_EXHIBITS) break;
  }
  return out;
}

/**
 * A filing's fiscal year from the year its primary document tags for itself once read (2.5.14), else from its
 * period of report, never its filing date (a December year is filed the next spring) (2.5.11).
 */
function fiscalYearOf(filing: { reportDate?: string | null } | null, focus: number | null = null): string | null {
  return filingFiscalYearLabel(focus, filing?.reportDate ?? null);
}

export async function searchFilingText(
  ticker: string,
  searchTerms: string[] = [],
  sectionHint: string | null = null,
  filingType = "10-K",
  accessionNumber: string | null = null,
  contextChars = 1500,
  returnTables = false,
  documentUrl: string | null = null,
  options: FilingSearchOptions = {},
): Promise<string> {
  const warnings: Record<string, unknown>[] = [];
  const mode: MatchMode = options.match === "substring" ? "substring" : "word";
  const order: MatchOrder = options.order === "document" ? "document" : "relevance";
  const budget = Math.max(200, Math.min(Math.floor(Number(contextChars)) || 1500, 4000));
  const pageSize = Math.max(1, Math.min(50, Math.trunc(Number(options.maxMatches ?? 10)) || 10));
  const cursor = Math.max(0, parseInt(String(options.cursor ?? "0"), 10) || 0);
  const parsedQuery = options.query ? parseSearchQuery(options.query) : { terms: [], near: [], exclude: [] };
  const spec: SearchSpec = {
    terms: [...termsFromList(searchTerms ?? []), ...parsedQuery.terms],
    near: [...parsedQuery.near, ...nearSpecsFromOptions(options.near)],
    exclude: [...parsedQuery.exclude, ...termsFromList(options.excludeTerms ?? [])],
    mode,
    budget,
  };
  const filingCount = Math.max(1, Math.min(FILING_SEARCH_MAX_FILINGS, Math.trunc(Number(options.filingCount ?? 1)) || 1));
  const since = options.since && /^\d{4}-\d{2}-\d{2}$/.test(options.since) ? options.since : null;
  const emptyResult = (fields: Record<string, unknown>) => JSON.stringify({
    ticker,
    accessionNumber,
    documentUrl: null,
    fiscalYear: null,
    filingType,
    filingDate: null,
    documentKind: "unavailable",
    matches: [],
    matchCount: 0,
    ...fields,
  });

  // ── Documents to search ──
  let filings: ResolvedSecFiling[] = [];
  let explicitTarget: FilingSearchTarget | null = null;
  if (documentUrl && !isLikelyXbrlDocumentUrl(documentUrl)) {
    if (!documentUrl.startsWith(SEC_ARCHIVES_PREFIX)) {
      return mcpFailure("search_sec_filing_text", ErrorCode.INPUT_VALIDATION_ERROR, "document_url must be an https://www.sec.gov/Archives/ URL.");
    }
    explicitTarget = { key: documentUrl, url: documentUrl, documentType: "primary", defaultSection: null, primary: true, accessionNumber, filingDate: null, filingType };
  } else {
    if (documentUrl) {
      if (!accessionNumber) {
        return emptyResult({
          documentUrl,
          documentKind: "xbrl_xml",
          status: "FILING_TEXT_NOT_AVAILABLE",
          code: "FILING_TEXT_NOT_AVAILABLE",
          confidence: "FILING_TEXT_NOT_AVAILABLE",
          warnings: [{ code: "FILING_TEXT_NOT_AVAILABLE", message: "Provided document_url appears to be XBRL/XML and no accession_number was supplied to resolve primary HTML.", severity: "error" }],
        });
      }
      warnings.push({ code: "DOCUMENT_URL_REPLACED_WITH_PRIMARY_HTML", message: "Provided document_url was XBRL/XML; resolved the accession primary HTML document instead.", severity: "warning" });
    }
    const resolved = await resolveSecFiling(ticker, filingType, accessionNumber);
    if (!resolved.ok && isAccessionNotFound(resolved.error)) return accessionNotFoundEnvelope(resolved.error);
    if (!resolved.ok) {
      return emptyResult({
        ...resolved.error,
        accessionNumber: resolved.error.accessionNumber ?? accessionNumber,
        documentUrl: resolved.error.documentUrl ?? null,
        filingDate: resolved.error.filingDate ?? null,
        confidence: resolved.error.code,
      });
    }
    warnings.push(...resolved.filing.warnings);
    if (accessionNumber && (filingCount > 1 || since)) {
      warnings.push({ code: "FILING_COUNT_IGNORED", message: "accession_number selects one filing; filing_count and since were ignored.", severity: "info" });
      filings = [resolved.filing];
    } else {
      const list = await resolveSecFilingsForSearch(ticker, resolved.filing, filingCount, since);
      filings = list.filings;
      if (list.capped) {
        warnings.push({ code: "FILINGS_CAPPED", message: `Searched the latest ${filings.length} matching filings; older ones were not searched.`, severity: "info" });
      }
    }
  }

  const first = filings[0] ?? null;
  const primaryUrl = explicitTarget?.url ?? first?.documentUrl ?? null;
  const base = {
    ticker,
    accessionNumber: first?.accessionNumber ?? accessionNumber,
    documentUrl: primaryUrl,
    fiscalYear: fiscalYearOf(first),
    filingType: first?.filingType ?? filingType,
    filingDate: first?.filingDate ?? null,
  };
  if (spec.terms.length === 0 && spec.near.length === 0 && !sectionHint) {
    // Nothing to look for: the resolved filing alone, without reading it.
    return JSON.stringify({
      ...base,
      documentKind: "primary_html",
      matches: [],
      matchCount: 0,
      totalMatches: 0,
      confidence: "NOT_DISCLOSED",
      warnings: [...warnings, { code: "NO_SEARCH_TERMS", message: "Pass search_terms, search_query or near to search the filing text.", severity: "warning" }],
    });
  }

  const targetsByFiling: { filing: ResolvedSecFiling | null; targets: FilingSearchTarget[] }[] = explicitTarget
    ? [{ filing: null, targets: [explicitTarget] }]
    : filings.map((filing) => ({
      filing,
      targets: [{
        key: filing.documentUrl,
        url: filing.documentUrl,
        documentType: "primary",
        defaultSection: null,
        primary: true,
        accessionNumber: filing.accessionNumber,
        filingDate: filing.filingDate,
        filingType: filing.filingType,
      }],
    }));
  if (options.includeExhibits) {
    for (const group of targetsByFiling) {
      const acc = group.filing?.accessionNumber ?? accessionNumber;
      const cikInt = group.filing?.cikInt ?? (primaryUrl ? parseInt(primaryUrl.match(/\/data\/(\d+)\//)?.[1] ?? "", 10) : NaN);
      if (!acc || !Number.isFinite(cikInt)) continue;
      group.targets.push(...await exhibitTargets(cikInt, acc, group.filing?.filingDate ?? null, group.filing?.filingType ?? filingType, group.targets[0].url));
    }
  }

  // ── Search each document in turn ──
  const allMatches: SearchMatch[] = [];
  const termStats = new Map<string, { count: number; capped: boolean }>();
  const documentsSearched: Record<string, unknown>[] = [];
  const filingSummaries: Record<string, unknown>[] = [];
  const docOrder = new Map<string, number>();
  let excludedHitCount = 0;
  let sectionScope: string | null = null;
  let hintMissing = false;
  let truncatedDocuments = 0;
  for (const group of targetsByFiling) {
    const filingMatches: SearchMatch[] = [];
    const filingTermHits: Record<string, number> = {};
    let primaryFocus: number | null = null;
    for (const target of group.targets) {
      const doc = await projectedFilingDocument(target.url);
      if (!doc) {
        if (target.primary && group === targetsByFiling[0]) {
          return emptyResult({
            ...base,
            documentKind: "primary_html",
            status: "FILING_TEXT_NOT_AVAILABLE",
            code: "FILING_TEXT_NOT_AVAILABLE",
            confidence: "FILING_TEXT_NOT_AVAILABLE",
            warnings: [...warnings, { code: "FILING_TEXT_NOT_AVAILABLE", message: "Unable to fetch primary filing HTML.", severity: "error" }],
          });
        }
        documentsSearched.push({ documentUrl: target.url, documentType: target.documentType, accessionNumber: target.accessionNumber, filingDate: target.filingDate, status: "FETCH_FAILED" });
        continue;
      }
      if (doc.truncated) truncatedDocuments += 1;
      if (target.primary) {
        primaryFocus = doc.fiscalYearFocus;
        // The resolved (first) filing's label follows the year its document tags for itself.
        if (group === targetsByFiling[0] && group.filing === first) base.fiscalYear = fiscalYearOf(first, primaryFocus);
      }
      const projection = doc.projection;
      let scopeStart = 0;
      let scopeEnd = projection.text.length;
      let scope: { start: number; end: number; title: string } | null = null;
      if (target.primary && sectionHint) {
        scope = sectionHintScope(projection, sectionHint, (hint, title) =>
          sectionHeadingMatches(hint, title) || title.toLowerCase().includes(hint.toLowerCase()));
        if (scope) {
          scopeStart = scope.start;
          scopeEnd = scope.end;
          sectionScope ??= scope.title;
        } else {
          hintMissing = true;
        }
      }
      docOrder.set(target.key, docOrder.size);
      const searchDoc: SearchDocument = { key: target.key, documentType: target.documentType, defaultSection: target.defaultSection, projection, scopeStart, scopeEnd };
      const result = searchDocument(searchDoc, spec);
      if (spec.terms.length === 0 && spec.near.length === 0 && scope) {
        // A section hint alone returns the start of that section.
        result.matches.push({
          doc: searchDoc,
          terms: [sectionHint as string],
          hitCount: 1,
          start: scope.start,
          context: matchContext(projection, scope.start, scope.start + 1, budget),
          section: scope.title,
          score: 0,
        });
      }
      excludedHitCount += result.excludedCount;
      let docHits = 0;
      for (const [label, stat] of result.termHits) {
        const prior = termStats.get(label) ?? { count: 0, capped: false };
        termStats.set(label, { count: prior.count + stat.count, capped: prior.capped || stat.capped });
        filingTermHits[label] = (filingTermHits[label] ?? 0) + stat.count;
        docHits += stat.count;
      }
      filingMatches.push(...result.matches);
      documentsSearched.push({
        documentUrl: target.url,
        documentType: target.documentType,
        accessionNumber: target.accessionNumber,
        filingDate: target.filingDate,
        hitCount: docHits,
        matchCount: result.matches.length,
        textChars: projection.text.length,
        truncated: doc.truncated,
        ...(scope ? { sectionScope: scope.title } : {}),
      });
    }
    allMatches.push(...filingMatches);
    if (group.filing) {
      filingSummaries.push({
        accessionNumber: group.filing.accessionNumber,
        filingDate: group.filing.filingDate,
        filingType: group.filing.filingType,
        fiscalYear: fiscalYearOf(group.filing, primaryFocus),
        documentUrl: group.filing.documentUrl,
        totalMatches: filingMatches.length,
        termHits: filingTermHits,
        hitsBySection: hitsBySection(filingMatches),
      });
    }
  }
  if (hintMissing) {
    warnings.push({ code: "SECTION_HINT_NOT_FOUND", message: `No heading matched section_hint '${sectionHint}'; the whole document was searched.`, severity: "warning" });
  }
  if (truncatedDocuments > 0) {
    warnings.push({ code: "FILING_READ_TRUNCATED", message: `${truncatedDocuments} document(s) exceeded ${SEC_DOCUMENT_READ_MAX_CHARS} characters; text past that point was not searched.`, severity: "warning" });
  }

  // ── Order, page and describe ──
  const ordered = orderMatches(allMatches, order, docOrder);
  const page = ordered.slice(cursor, cursor + pageSize);
  const multiDocument = documentsSearched.length > 1;
  const targetByKey = new Map<string, FilingSearchTarget>();
  for (const group of targetsByFiling) for (const target of group.targets) targetByKey.set(target.key, target);
  const payloads: Record<string, unknown>[] = [];
  for (const match of page) {
    const payload = matchPayload(match, false);
    const target = targetByKey.get(match.doc.key);
    if (multiDocument && target) {
      payload.documentUrl = target.url;
      payload.documentType = target.documentType;
      payload.accessionNumber = target.accessionNumber;
      payload.filingDate = target.filingDate;
    }
    if (returnTables) {
      const tableParsed: Record<string, unknown>[] = [];
      const table = match.context.table;
      if (table && target) {
        const doc = await fetchSecDocument(target.url).catch(() => null);
        if (doc?.ok) {
          const rows = parseFilingTableRows(doc.text.slice(table.htmlStart, table.htmlEnd), FILING_SEARCH_TABLE_ROWS).map(mergeFinancialCells);
          tableParsed.push({ tableIndex: table.tableIndex, rows, rowsTruncated: rows.length >= FILING_SEARCH_TABLE_ROWS });
        }
      }
      payload.tableParsed = tableParsed;
    }
    payloads.push(payload);
  }
  const labels = [...termStats.keys()];
  const stats = labels.map((label) => ({
    term: label,
    hitCount: termStats.get(label)?.count ?? 0,
    hitCountCapped: termStats.get(label)?.capped ?? false,
    matchCount: allMatches.filter((m) => m.terms.includes(label)).length,
    returnedCount: page.filter((m) => m.terms.includes(label)).length,
  }));
  const nextCursor = cursor + page.length < ordered.length ? String(cursor + page.length) : null;
  return JSON.stringify({
    ...base,
    documentKind: "primary_html",
    ...(sectionHint ? { sectionScope } : {}),
    query: {
      terms: spec.terms.map(termLabel),
      near: spec.near.map((n) => `${termLabel(n.a)} NEAR/${n.withinWords} ${termLabel(n.b)}`),
      exclude: spec.exclude.map(termLabel),
      match: mode,
      order,
    },
    matches: payloads,
    matchCount: payloads.length,
    totalMatches: ordered.length,
    excludedHitCount,
    termStats: stats,
    hitsBySection: filingSummaries.length > 1 ? undefined : hitsBySection(allMatches),
    ...(filingSummaries.length > 1 ? { filings: filingSummaries } : {}),
    documentsSearched,
    pagination: { cursor: String(cursor), nextCursor, limit: pageSize, returned: payloads.length, hasMore: nextCursor != null },
    confidence: payloads.length === 0 ? "NOT_DISCLOSED" : "MEDIUM",
    warnings: [...warnings, ...(payloads.length > 0 ? [{
      code: "RAW_FILING_TEXT",
      message: "Returned text is sanitized filing context, not structured fact extraction.",
      severity: "info",
    }] : [])],
  });
}

function cleanFilingDisplayText(text: string): string {
  return text
    .replace(/<[^>]+>/g, " ")
    .replace(/\b(?:contextRef|unitRef|decimals|id|name|class|style|href)=("[^"]*"|'[^']*'|[^\s]+)/gi, " ")
    .replace(/\b(?:xbrli|ix|dei|us-gaap|srt|country):[A-Za-z0-9_.:-]+\b/g, " ")
    .replace(/https?:\/\/\S+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function looksLikeFilingMarkupText(text: string): boolean {
  return /[<>]=?|(?:^|\s)(?:contextRef|unitRef|decimals|style|class|id|name)=|(?:xbrli|ix|dei|us-gaap|srt|country):|https?:\/\//i.test(text);
}

/** Phase 2 canonical listing: returns cik, filingType, acceptedAt, primaryDocument, documentUrl, meta. */
export async function listSecCompanyFilings(ticker: string, filingType: string = "10-K", limit: number = 5): Promise<string> {
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not find EDGAR submissions for ticker '${ticker}'` } });
  }

  const cikInt = parseInt(cikPadded, 10);
  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form as string[]) ?? [];
  const dates = (recent.filingDate as string[]) ?? [];
  const accessions = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs = (recent.primaryDocument as string[]) ?? [];
  const acceptedDts = (recent.acceptanceDateTime as string[]) ?? [];
  const itemLists = (recent.items as string[]) ?? [];

  const cap = Math.min(Math.max(1, limit), 20);
  const results: Record<string, unknown>[] = [];
  for (let i = 0; i < forms.length && results.length < cap; i++) {
    if ((forms[i] ?? "").toUpperCase() !== filingType.toUpperCase()) continue;
    const acc = accessions[i] ?? "";
    const primaryDoc = primaryDocs[i] ?? "";
    const accClean = acc.replace(/-/g, "");
    const documentUrl = primaryDoc
      ? secArchiveDocumentUrl(cikInt, accClean, primaryDoc)
      : null;
    results.push({
      filingType: forms[i],
      filingDate: dates[i] ?? "",
      acceptedAt: acceptedDts[i] ?? null,
      accessionNumber: acc,
      primaryDocument: primaryDoc,
      documentUrl,
      items: itemLists[i] != null ? String(itemLists[i]) : "",
    });
  }

  return JSON.stringify({
    ticker,
    cik: cikPadded,
    filings: results,
    meta: { source: "sec_submissions", retrievedAt: new Date().toISOString() },
  });
}

/**
 * Python's _sanitize_sec_html (yfmcp/validation.py), exactly: script and style blocks and on* event-handler attributes
 * are removed before outline headings are read, so their text never reaches a title (2.15.0, R4-OUTLINE-SHAPE).
 */
export function sanitizeSecHtml(html: string): string {
  return html
    .replace(/<script[^>]*>[\s\S]*?<\/script[^>]*>/gi, "")
    .replace(/<style[^>]*>[\s\S]*?<\/style[^>]*>/gi, "")
    .replace(/\s+on\w+\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]+)/gi, "");
}

/** The standard ok:false error envelope, with the codes Python's get_filing_outline uses (2.15.0). */
function outlineFailure(code: string, message: string): string {
  return JSON.stringify({ ok: false, error: { code, message } });
}

/** A failed outline read: a 429 stays RATE_LIMIT and a timeout PROVIDER_TIMEOUT (same classifier as the envelope), else PROVIDER_ERROR. */
function outlineReadFailure(message: string): string {
  return outlineFailure(classifyErrorMessage(message)?.code ?? ErrorCode.PROVIDER_ERROR, message);
}

/**
 * One outline shape in both runtimes (2.15.0, R4-OUTLINE-SHAPE): ticker, accessionNumber (the resolved filing's, the
 * accession given with a document_url, or null when only a document_url was given), documentUrl (the document read),
 * outline, status (OK / OUTLINE_NOT_PARSED / EMPTY), tableCount and warnings. Failures are the standard ok:false
 * envelope; an accession miss stays ACCESSION_NOT_FOUND.
 */
export async function getFilingOutline(ticker: string, accessionNumber: string | null, documentUrl: string | null): Promise<string> {
  // The ticker and accession checks the tool-call gate runs, here too, as Python's get_filing_outline does
  // (R5-pricing-1, Q3b): a caller of this function never reads a filing for a malformed input.
  const tickerError = validateTicker(ticker);
  if (tickerError) return outlineFailure(ErrorCode.INPUT_VALIDATION_ERROR, tickerError);
  const accessionError = accessionNumber ? validateAccession(accessionNumber) : null;
  if (accessionError) return outlineFailure(ErrorCode.INPUT_VALIDATION_ERROR, accessionError);
  try {
    let resolvedAccession: string | null = accessionNumber || null;
    if (!documentUrl && accessionNumber) {
      // An explicit accession without document_url: the outline of that filing's primary document, found by the
      // same resolver as the filing index; a miss is ACCESSION_NOT_FOUND (2.13.0, R4 Q3a). document_url wins when given.
      const resolved = await resolveSecFiling(ticker, "10-K", accessionNumber);
      if (!resolved.ok && isAccessionNotFound(resolved.error)) return accessionNotFoundEnvelope(resolved.error);
      if (!resolved.ok) return JSON.stringify({ ok: false, error: resolved.error, ...resolved.error });
      documentUrl = resolved.filing.documentUrl;
      resolvedAccession = resolved.filing.accessionNumber;
    }
    if (!documentUrl) return outlineFailure(ErrorCode.INPUT_VALIDATION_ERROR, "Either accession_number or document_url is required");
    if (!documentUrl.startsWith(SEC_ARCHIVES_PREFIX)) {
      return outlineFailure(ErrorCode.INPUT_VALIDATION_ERROR, "Invalid SEC URL: must start with 'https://www.sec.gov/Archives/'.");
    }
    const doc = await fetchSecDocument(documentUrl);
    if (!doc.ok) return outlineReadFailure(`HTTP ${doc.status}`);
    const html = sanitizeSecHtml(doc.text);

    const outline: { level: number; title: string }[] = [];
    const headingRe = /<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi;
    const itemRe = /Part\s+[IVX]+|Item\s+\d+[A-Z]?|Note\s+\d+/i;
    let m: RegExpExecArray | null;
    while ((m = headingRe.exec(html)) !== null && outline.length < 100) {
      const level = parseInt(m[1], 10);
      const text = m[2].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
      if (text && (itemRe.test(text) || text.length < 100)) {
        outline.push({ level, title: text });
      }
    }
    if (outline.length === 0) {
      for (const heading of filingItemHeadings(html).slice(0, 100)) {
        outline.push({ level: heading.level, title: heading.title });
      }
    }
    const tableCount = (html.match(/<table\b/gi) ?? []).length;
    return JSON.stringify({
      ticker,
      accessionNumber: resolvedAccession,
      documentUrl,
      outline,
      status: outline.length > 0 ? "OK" : (tableCount > 0 ? "OUTLINE_NOT_PARSED" : "EMPTY"),
      tableCount,
      warnings: outline.length === 0 && tableCount > 0
        ? [{ code: "TABLES_FOUND_OUTLINE_EMPTY", message: "Filing tables were detected but no section outline headings were parsed.", severity: "warning" }]
        : [],
    });
  } catch (e) {
    return outlineReadFailure(`${e instanceof Error ? e.message : String(e)}`);
  }
}

function isTocMatch(html: string, matchStart: number, matchEnd: number): boolean {
  const slice = html.slice(Math.max(0, matchStart - 30), Math.min(html.length, matchEnd + 30));
  if (/<a\b[^>]*\bhref\s*=\s*['"]#[^'"]*['"]/i.test(slice)) {
    return true;
  }
  const surrBefore = html.slice(Math.max(0, matchStart - 100), matchStart);
  if (/<a\b[^>]*\bhref\s*=\s*['"]#[^'"]*['"][^>]*>\s*$/i.test(surrBefore)) {
    return true;
  }
  const context = html.slice(Math.max(0, matchStart - 150), Math.min(html.length, matchEnd + 150));
  if (context.includes("....") || context.includes(". . .") || context.includes("&#183;") || context.includes("&middot;")) {
    return true;
  }
  const plainContext = context.replace(/<[^>]+>/g, " ");
  if (/\.{3,}\s*\d+|\.\s*\.\s*\.\s*\d+/.test(plainContext)) {
    return true;
  }
  return false;
}

interface SectionBounds {
  startIdx: number | null;
  endIdx: number | null;
  foundHeading: string;
  tocSkipped: boolean;
  errCode: string | null;
}

function sectionItemToken(value: string): string | null {
  const match = value.match(/\bitem\s+(\d+[a-z]?)\b/i);
  return match ? `item ${match[1].toLowerCase()}` : null;
}

export function sectionHeadingMatches(requested: string, heading: string): boolean {
  const requestedText = requested.toLowerCase().replace(/\s+/g, " ").trim();
  const headingText = heading.toLowerCase().replace(/\s+/g, " ").trim();
  const requestedItem = sectionItemToken(requestedText);
  if (requestedItem) return requestedItem === sectionItemToken(headingText);
  return requestedText.includes(headingText) || headingText.includes(requestedText);
}

// Bold markup: <b>, <strong>, or a span whose style sets a bold weight. The
// style value is matched per quote character, because inline-XBRL filings
// quote font names inside it (style="font-family:'Helvetica';font-weight:700").
const BOLD_OPEN = `(?:<b\\b[^>]*>|<strong\\b[^>]*>|<span\\b[^>]*style\\s*=\\s*(?:"[^"]*font-weight\\s*:\\s*(?:bold|[6-9]00)[^"]*"|'[^']*font-weight\\s*:\\s*(?:bold|[6-9]00)[^']*')[^>]*>)`;
const BOLD_ITEM_RE = new RegExp(`${BOLD_OPEN}\\s*((?:<[^>]+>\\s*)*Item(?:\\s|&nbsp;|&#160;|\\u00a0)+\\d+[A-Z]?(?:\\s*\\([^)]+\\))?[^<]{0,160})`, "gi");
const BOLD_PART_RE = new RegExp(`${BOLD_OPEN}\\s*((?:<[^>]+>\\s*)*Part(?:\\s|&nbsp;|&#160;|\\u00a0)+[IVX]+\\b[^<]{0,80})`, "gi");

/** Plain text of the block (paragraph, div or table row) that starts at pos, for a heading's full title. */
function headingBlockText(html: string, pos: number): string {
  const rest = html.slice(pos, pos + 2_000);
  const close = rest.search(/<\/(?:p|div|tr|h[1-6])>/i);
  const block = close >= 0 ? rest.slice(0, close) : rest.slice(0, 600);
  return stripHtmlTags(block).replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim().slice(0, 200);
}

function boldItemHeadings(html: string): { start: number; end: number; text: string; token: string }[] {
  const matches: { start: number; end: number; text: string; token: string }[] = [];
  for (const match of html.matchAll(BOLD_ITEM_RE)) {
    const start = match.index ?? 0;
    const text = headingBlockText(html, start) || stripHtmlTags(match[1]).replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
    const token = sectionItemToken(text);
    if (token) matches.push({ start, end: start + match[0].length, text, token });
  }
  return matches;
}

/**
 * Part and Item headings of a filing in document order, outside the table
 * of contents, for filings that mark headings with bold text instead of
 * <h1>-<h6> (inline-XBRL 10-K and 10-Q documents).
 */
export function filingItemHeadings(html: string): { start: number; end: number; level: number; title: string; token: string }[] {
  const headings: { start: number; end: number; level: number; title: string; token: string }[] = [];
  const seen = new Set<string>();
  const add = (re: RegExp, level: number, tokenOf: (text: string) => string | null): void => {
    for (const match of html.matchAll(re)) {
      const start = match.index ?? 0;
      const end = start + match[0].length;
      if (isTocMatch(html, start, end)) continue;
      const title = headingBlockText(html, start);
      // A table-of-contents row ends with its page number.
      if (!title || /\s\d{1,3}$/.test(title)) continue;
      const token = tokenOf(title);
      if (!token || seen.has(token)) continue;
      seen.add(token);
      headings.push({ start, end, level, title, token });
    }
  };
  add(BOLD_PART_RE, 1, (text) => {
    const m = text.match(/^part\s+([ivx]+)\b/i);
    return m ? `part ${m[1].toLowerCase()}` : null;
  });
  add(BOLD_ITEM_RE, 2, (text) => sectionItemToken(text));
  return headings.sort((a, b) => a.start - b.start);
}

export function findSectionBounds(html: string, section: string, maxChars: number = 50000): SectionBounds {
  const headingRe = /<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi;
  let tocSkipped = false;

  const escapedSection = section.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&");
  const occurrenceRe = new RegExp(escapedSection, "gi");
  let occurrence: RegExpExecArray | null;
  while ((occurrence = occurrenceRe.exec(html)) !== null) {
    if (isTocMatch(html, occurrence.index, occurrenceRe.lastIndex)) tocSkipped = true;
  }

  const allHeadings: { text: string; start: number; end: number; level: number; rawText: string }[] = [];
  let hMatch: RegExpExecArray | null;
  while ((hMatch = headingRe.exec(html)) !== null) {
    const text = hMatch[2].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().toLowerCase();
    allHeadings.push({
      text,
      start: hMatch.index,
      end: headingRe.lastIndex,
      level: parseInt(hMatch[1], 10),
      rawText: hMatch[2].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim(),
    });
  }
  
  const candidates: { start: number; level: number; rawText: string; hIdx: number }[] = [];
  for (let i = 0; i < allHeadings.length; i++) {
    const h = allHeadings[i];
    if (sectionHeadingMatches(section, h.rawText)) {
      if (isTocMatch(html, h.start, h.end)) {
        tocSkipped = true;
        continue;
      }
      candidates.push({ start: h.start, level: h.level, rawText: h.rawText, hIdx: i });
    }
  }
  
  const itemMatches: { start: number; end: number; text: string; token: string }[] = [];
  const allBoldItems = boldItemHeadings(html);
  if (candidates.length === 0) {
    for (const itemMatch of allBoldItems) {
      if (!sectionHeadingMatches(section, itemMatch.text)) continue;
      if (isTocMatch(html, itemMatch.start, itemMatch.end)) {
        tocSkipped = true;
        continue;
      }
      itemMatches.push(itemMatch);
    }
  }
  
  if (candidates.length === 0 && itemMatches.length === 0) {
    return { startIdx: null, endIdx: null, foundHeading: "", tocSkipped, errCode: null };
  }
  
  if (candidates.length > 1 || itemMatches.length > 1) {
    return { startIdx: null, endIdx: null, foundHeading: "", tocSkipped, errCode: "SECTION_AMBIGUOUS" };
  }
  
  if (candidates.length === 1) {
    const c = candidates[0];
    let endPos: number | null = null;
    for (let i = c.hIdx + 1; i < allHeadings.length; i++) {
      if (allHeadings[i].level <= c.level) {
        endPos = allHeadings[i].start;
        break;
      }
    }
    if (endPos === null) {
      endPos = Math.min(c.start + maxChars * 3, html.length);
    }
    return { startIdx: c.start, endIdx: endPos, foundHeading: c.rawText, tocSkipped, errCode: null };
  } else {
    const match = itemMatches[0];
    let endPos: number | null = null;
    for (const nextMatch of allBoldItems) {
      if (nextMatch.start < match.end || nextMatch.token === match.token) continue;
      if (isTocMatch(html, nextMatch.start, nextMatch.end)) continue;
      endPos = nextMatch.start;
      break;
    }
    if (endPos === null) {
      endPos = Math.min(match.start + maxChars * 3, html.length);
    }
    return { startIdx: match.start, endIdx: endPos, foundHeading: match.text, tocSkipped, errCode: null };
  }
}

export async function getFilingSection(ticker: string, sectionName: string, documentUrl: string, contextChars: number = 3000): Promise<string> {
  try {
    if (!documentUrl.startsWith(SEC_ARCHIVES_PREFIX)) {
      return JSON.stringify({ error: true, message: "Invalid SEC URL" });
    }
    const doc = await fetchSecDocument(documentUrl);
    if (!doc.ok) return JSON.stringify({ error: true, message: `HTTP ${doc.status}` });
    const html = doc.text;

    const bounds = findSectionBounds(html, sectionName, contextChars);
    if (bounds.errCode === "SECTION_AMBIGUOUS") {
      return mcpFailure("get_filing_section", "SECTION_AMBIGUOUS", "The section heading could not be resolved unambiguously.");
    }

    if (bounds.startIdx !== null && bounds.endIdx !== null) {
      const sectionHtml = html.slice(bounds.startIdx, bounds.endIdx);
      const plainSection = stripHtmlTags(sectionHtml);
      return JSON.stringify({
        ticker,
        sectionName,
        status: "OK",
        found: true,
        text: plainSection.slice(0, contextChars),
        sectionStartOffset: bounds.startIdx,
        sectionEndOffset: bounds.endIdx,
        matchedHeading: bounds.foundHeading,
        tocSkipped: bounds.tocSkipped,
      });
    }

    if (sectionItemToken(sectionName)) {
      return JSON.stringify({
        ticker,
        sectionName,
        status: "SECTION_STRUCTURE_NOT_RESOLVED",
        found: false,
        text: null,
        sectionStartOffset: null,
        sectionEndOffset: null,
        matchedHeading: "",
        tocSkipped: bounds.tocSkipped,
        decisionGrade: false,
        recommendedNextAction: "GET_FILING_OUTLINE",
      });
    }

    // Non-Item labels retain the legacy plain-text fallback.
    const text = stripHtmlTags(html);
    const idx = text.toLowerCase().indexOf(sectionName.toLowerCase());
    if (idx === -1) {
      return JSON.stringify({
        ticker,
        sectionName,
        status: "SECTION_NOT_FOUND",
        found: false,
        text: null,
        sectionStartOffset: null,
        sectionEndOffset: null,
        matchedHeading: "",
        tocSkipped: bounds.tocSkipped,
      });
    }
    return JSON.stringify({
      ticker,
      sectionName,
      status: "TEXT_FALLBACK",
      found: true,
      text: text.slice(idx, idx + contextChars),
      sectionStartOffset: idx,
      sectionEndOffset: idx + contextChars,
      matchedHeading: sectionName,
      tocSkipped: bounds.tocSkipped,
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

function parseFilingTableRows(tableHtml: string, maxRows: number | null = null): string[][] {
  const rows: string[][] = [];
  const rowRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let rowMatch: RegExpExecArray | null;
  while ((rowMatch = rowRe.exec(tableHtml)) !== null) {
    const cells: string[] = [];
    const cellRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
    let cellMatch: RegExpExecArray | null;
    while ((cellMatch = cellRe.exec(rowMatch[1])) !== null) {
      cells.push(stripHtmlTags(cellMatch[1]));
    }
    if (cells.some(Boolean)) rows.push(cells);
    if (maxRows != null && rows.length >= maxRows) break;
  }
  return rows;
}

function filingTableIsUsable(rows: string[][]): boolean {
  return rows.flat().filter((cell) => cell.trim() !== "").length >= 2;
}

export async function listFilingTables(ticker: string, documentUrl: string, offset: number = 0, limit: number = 50): Promise<string> {
  try {
    if (!documentUrl.startsWith(SEC_ARCHIVES_PREFIX)) {
      return JSON.stringify({ error: true, message: "Invalid SEC URL" });
    }
    const doc = await fetchSecDocument(documentUrl);
    if (!doc.ok) return JSON.stringify({ error: true, message: `HTTP ${doc.status}` });
    const html = doc.text;

    const tables: { tableIndex: number; rowCount: number; title: string | null; headers: string[]; qualityStatus: "USABLE" }[] = [];
    const tableRe = /<table[^>]*>([\s\S]*?)<\/table>/gi;
    let tm: RegExpExecArray | null;
    let idx = 0;
    while ((tm = tableRe.exec(html)) !== null) {
      const tableStart = tm.index;
      const tableHtml = tm[1];
      const rows = parseFilingTableRows(tableHtml);
      const tableIndex = idx++;
      if (!filingTableIsUsable(rows)) continue;
      const headers = (rows[0] ?? []).slice(0, 6);
      const preText = stripHtmlTags(html.slice(Math.max(0, tableStart - 500), tableStart));
      const lines = preText.split(/\n| {2,}/).map(l => cleanFilingDisplayText(l.trim())).filter(Boolean);
      const candidate = lines[lines.length - 1] ?? "";
      const title = candidate.length > 10 && candidate.length < 200 && !looksLikeFilingMarkupText(candidate) ? candidate : null;
      tables.push({ tableIndex, rowCount: rows.length, title, headers, qualityStatus: "USABLE" });
    }
    const safeOffset = Math.max(0, Math.trunc(offset));
    const safeLimit = Math.min(100, Math.max(1, Math.trunc(limit || 50)));
    const page = tables.slice(safeOffset, safeOffset + safeLimit);
    return JSON.stringify({
      ticker,
      documentUrl,
      status: tables.length > 0 ? "OK" : "NO_USABLE_TABLES",
      tableCount: idx,
      usableTableCount: tables.length,
      excludedTableCount: idx - tables.length,
      returnedCount: page.length,
      offset: safeOffset,
      limit: safeLimit,
      hasMore: safeOffset + page.length < tables.length,
      tables: page,
      recommendedNextAction: tables.length > 0 ? "GET_SEC_FILING_TABLE" : "SEARCH_FILING_TEXT",
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

export async function getFilingTable(ticker: string, documentUrl: string, tableIndex: number, maxRows: number = 30): Promise<string> {
  try {
    if (!documentUrl.startsWith(SEC_ARCHIVES_PREFIX)) {
      return JSON.stringify({ error: true, message: "Invalid SEC URL" });
    }
    const doc = await fetchSecDocument(documentUrl);
    if (!doc.ok) return JSON.stringify({ error: true, message: `HTTP ${doc.status}` });
    const html = doc.text;

    const tableRe = /<table[^>]*>([\s\S]*?)<\/table>/gi;
    const tables: string[] = [];
    let tm: RegExpExecArray | null;
    while ((tm = tableRe.exec(html)) !== null) tables.push(tm[1]);

    if (tableIndex < 0 || tableIndex >= tables.length) {
      return JSON.stringify({ error: true, message: `Table index ${tableIndex} not found. Document has ${tables.length} tables.` });
    }

    const tableHtml = tables[tableIndex];
    const allRows = parseFilingTableRows(tableHtml);
    if (!filingTableIsUsable(allRows)) {
      return JSON.stringify({
        ticker,
        tableIndex,
        status: "UNUSABLE_TABLE",
        decisionGrade: false,
        totalRows: allRows.length,
        returnedRows: 0,
        rows: [],
        recommendedNextAction: "LIST_USABLE_TABLES",
      });
    }
    // "$" and "%" merged into their values, so each period sits in one column.
    const parsedRows = allRows.slice(0, Math.max(1, maxRows)).map(mergeFinancialCells);

    return JSON.stringify({
      ticker,
      tableIndex,
      status: "OK",
      decisionGrade: false,
      totalRows: allRows.length,
      returnedRows: parsedRows.length,
      rows: parsedRows,
      recommendedNextAction: "VERIFY_TABLE_PERIOD_AND_UNITS",
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

export async function extractFilingFact(ticker: string, factName: string, _documentUrl: string | null, _accessionNumber: string | null): Promise<string> {
  try {
    const searchResult = JSON.parse(await searchFilingText(
      ticker,
      [factName],
      null,
      "10-K",
      _accessionNumber,
      1000,
      true,
    )) as Record<string, unknown>;
    const searchError = searchResult.ok === false ? searchResult.error as Record<string, unknown> | undefined : undefined;
    if (searchError && isAccessionNotFound(searchError)) return accessionNotFoundEnvelope(searchError);
    return JSON.stringify({
      ticker, factName,
      extractionMethod: "text_search",
      result: searchResult,
    });
  } catch (e) {
    return JSON.stringify({ error: true, message: `${e instanceof Error ? e.message : String(e)}` });
  }
}

const _INDEX_KEYWORDS = [
  "china", "greater china", "prc", "geographic", "segment", "revenue",
  "customers", "long-lived assets", "risk factors", "americas", "europe",
  "japan", "asia", "rest of asia",
];

export function _buildFilingIndexFromHtml(
  html: string,
): {
  sections: Record<string, unknown>[];
  tables: Record<string, unknown>[];
  keywordMap: Record<string, string[]>;
  rawTableCount: number;
  excludedTableCount: number;
} {
  // Remove scripts/styles/event handlers to reduce noise.
  const sanitized = _sanitizeFilingHtml(html);

  // --- Section extraction ---
  const sections: Record<string, unknown>[] = [];
  const headingRe = /<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi;
  let hm: RegExpExecArray | null;
  while ((hm = headingRe.exec(sanitized)) !== null && sections.length < 50) {
    const level = parseInt(hm[1], 10);
    const rawText = _stripHtmlTagsIdx(hm[2]);
    if (!rawText || rawText.length > 200) continue;
    const normalized = rawText.toLowerCase().trim();
    const keywords = _INDEX_KEYWORDS.filter(kw => normalized.includes(kw));
    const sectionId = normalized.replace(/[^a-z0-9]+/g, "_").slice(0, 60);
    sections.push({ sectionId, heading: rawText, normalizedHeading: normalized, level, keywords, startChar: hm.index, endChar: hm.index + hm[0].length });
  }

  if (sections.length === 0) {
    for (const heading of filingItemHeadings(sanitized).slice(0, 50)) {
      const normalized = heading.title.toLowerCase();
      sections.push({
        sectionId: normalized.replace(/[^a-z0-9]+/g, "_").slice(0, 60),
        heading: heading.title,
        normalizedHeading: normalized,
        level: heading.level,
        keywords: _INDEX_KEYWORDS.filter((kw) => normalized.includes(kw)),
        startChar: heading.start,
        endChar: heading.end,
      });
    }
  }

  // --- Table extraction ---
  const tables: Record<string, unknown>[] = [];
  const tableRe = /<table[^>]*>([\s\S]*?)<\/table>/gi;
  let tm: RegExpExecArray | null;
  let tableIdx = 0;
  while ((tm = tableRe.exec(sanitized)) !== null && tableIdx < 100) {
    const tableStart = tm.index;
    const tableHtml = tm[0];

    // Nearby section (last section starting before this table)
    let nearbySectionId: string | null = null;
    let nearbyHeading = "";
    for (let si = sections.length - 1; si >= 0; si--) {
      if ((sections[si]!.startChar as number) <= tableStart) {
        nearbySectionId = sections[si]!.sectionId as string;
        nearbyHeading = sections[si]!.heading as string;
        break;
      }
    }

    // Parse rows
    const parsedRows = parseFilingTableRows(tableHtml);
    if (!filingTableIsUsable(parsedRows)) { tableIdx++; continue; }

    // Headers from first row
    const headers = (parsedRows[0] ?? []).slice(0, 10);

    // Row labels from first column of subsequent rows (up to 19 data rows)
    const rowLabels: string[] = [];
    for (let r = 1; r < Math.min(parsedRows.length, 20); r++) {
      const row = parsedRows[r] ?? [];
      if (row.length > 0) {
        const label = row[0];
        if (label && label.length < 100) rowLabels.push(label);
      }
    }

    // Detect unit scale: default to "unknown"; detect explicitly from context.
    // Lowercase tableHtml separately since preContext is already lowercased,
    // then concatenate to build the search context.
    const unitScale = tableUnitScale(tableHtml, sanitized.slice(Math.max(0, tableStart - 2000), tableStart));

    // Confidence: also lower when unitScale is unknown
    const hasYearHeaders = headers.some(h => /\b20\d\d\b/.test(h));
    const hasRowLabels = rowLabels.length > 0;
    const confidence = (hasYearHeaders && hasRowLabels && unitScale !== "unknown") ? "HIGH"
      : (hasYearHeaders || hasRowLabels) ? "MEDIUM"
      : "LOW";

    // Infer title from pre-context
    const preText = _stripHtmlTagsIdx(sanitized.slice(Math.max(0, tableStart - 500), tableStart));
    const lines = preText.split("\n").map(l => cleanFilingDisplayText(l.trim())).filter(Boolean);
    let title = "";
    const candidate = lines[lines.length - 1] ?? "";
    if (candidate.length > 10 && candidate.length < 200 && !looksLikeFilingMarkupText(candidate)) title = candidate;

    tables.push({ tableId: tableIdx, sectionId: nearbySectionId, title: title || nearbyHeading, headers, rowLabels, unit: "USD", unitScale, confidence });
    tableIdx++;
  }

  // --- Keyword map ---
  const keywordMap: Record<string, string[]> = {};
  for (const kw of _INDEX_KEYWORDS) {
    const refs: string[] = [];
    for (const sec of sections) {
      if ((sec.normalizedHeading as string).includes(kw)) {
        const ref = `sectionId:${sec.sectionId}`;
        if (!refs.includes(ref)) refs.push(ref);
      }
    }
    for (const tbl of tables) {
      const haystack = [
        ...(tbl.rowLabels as string[]),
        ...(tbl.headers as string[]),
        tbl.title as string,
      ].join(" ").toLowerCase();
      if (haystack.includes(kw)) {
        const ref = `tableId:${tbl.tableId}`;
        if (!refs.includes(ref)) refs.push(ref);
      }
    }
    if (refs.length > 0) keywordMap[kw] = refs;
  }

  return {
    sections,
    tables,
    keywordMap,
    rawTableCount: tableIdx,
    excludedTableCount: tableIdx - tables.length,
  };
}

async function _getSecFilingIndexImpl(
  ticker: string,
  filingType: string,
  // `period` is reserved for future multi-period support (e.g. "2024", "prior").
  // Currently only "latest" is supported; the field is kept for API stability.
  _period: string,
  accessionNumber: string | null,
): Promise<string> {
  const resolved = await resolveSecFiling(ticker, filingType, accessionNumber);
  if (!resolved.ok && isAccessionNotFound(resolved.error)) return accessionNotFoundEnvelope(resolved.error);
  if (!resolved.ok) {
    return JSON.stringify({ ok: false, error: resolved.error, ...resolved.error });
  }
  const filing = resolved.filing;
  accessionNumber = filing.accessionNumber;

  // Check cache
  const cacheKey = `secidx:${ticker.toUpperCase()}:${accessionNumber}:${filing.filingType}`;
  const cached = filingIndexCache.get(cacheKey);
  if (cached && Date.now() - cached.storedAt < FILING_INDEX_TTL_MS) {
    countCacheEvent("filing_index", "memoryHits");
    return cached.value;
  }

  // Fetch filing HTML
  // Through the SEC document cache, so the outline, section and table tools
  // reuse the same read.
  const doc = await fetchSecDocument(filing.documentUrl);
  if (!doc.ok) {
    return JSON.stringify({ ok: false, error: { code: "PROVIDER_ERROR", message: `Failed to fetch filing: HTTP ${doc.status}` } });
  }
  const html = doc.text.length > SEC_DOCUMENT_READ_MAX_CHARS ? doc.text.slice(0, SEC_DOCUMENT_READ_MAX_CHARS) : doc.text;

  const index = _buildFilingIndexFromHtml(html);
  const indexedAt = new Date().toISOString();

  const result = JSON.stringify({
    ticker,
    cik: filing.cikPadded,
    requestedFilingType: filing.requestedFilingType,
    filingType: filing.filingType,
    filingDate: filing.filingDate,
    acceptedAt: filing.acceptedAt,
    accessionNumber: filing.accessionNumber,
    documentUrl: filing.documentUrl,
    index,
    meta: {
      indexedAt,
      source: "sec",
      cacheKey: `${ticker.toUpperCase()}:${accessionNumber}`,
      cacheTtlHours: 24,
    },
    warnings: filing.warnings,
  });

  setBounded(filingIndexCache, cacheKey, { value: result, storedAt: Date.now() }, FILING_INDEX_CACHE_MAX);
  return result;
}

export async function getSecFilingIndex(
  ticker: string,
  filingType: string = "10-K",
  period: string = "latest",
  accessionNumber: string | null = null,
): Promise<string> {
  try {
    return await _getSecFilingIndexImpl(ticker, filingType, period, accessionNumber);
  } catch (e) {
    return JSON.stringify({ ok: false, error: { code: "PROVIDER_ERROR", message: `${e instanceof Error ? e.message : String(e)}` } });
  }
}


const SEC_MATERIAL_FORMS_DEFAULT = ["10-K", "10-Q", "8-K", "S-1", "424B", "DEF 14A", "20-F", "6-K"];
const SEC_NOISY_FORMS = new Set(["4", "3", "5", "SC 13G", "SC 13G/A", "SC 13D", "SC 13D/A", "144", "SD", "CORRESP", "UPLOAD", "CT ORDER"]);

const SEC_MATERIAL_FILINGS_MAX = 20;

export async function listSecMaterialFilings(
  ticker: string,
  forms: string[] | null = null,
  limit: number = 5,
): Promise<string> {
  const resolvedLimit = Math.min(Math.max(1, limit), SEC_MATERIAL_FILINGS_MAX);
  const allowedForms = new Set((forms ?? SEC_MATERIAL_FORMS_DEFAULT).map(f => f.toUpperCase()));

  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not find EDGAR submissions for ticker '${ticker}'` } });
  }

  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const formsList: string[] = (recent.form as string[]) ?? [];
  const dates: string[] = (recent.filingDate as string[]) ?? [];
  const accessions: string[] = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs: string[] = (recent.primaryDocument as string[]) ?? [];
  const acceptedDts: string[] = (recent.acceptanceDateTime as string[]) ?? [];
  // SEC submissions flag each filing that carries XBRL (1) or inline XBRL.
  const isXbrl: unknown[] = (recent.isXBRL as unknown[]) ?? [];
  const isInlineXbrl: unknown[] = (recent.isInlineXBRL as unknown[]) ?? [];

  const results: Record<string, unknown>[] = [];
  const cikInt = parseInt(cikPadded, 10);
  const material = (form: unknown): boolean => {
    const formUpper = String(form).toUpperCase();
    return !SEC_NOISY_FORMS.has(formUpper) && Array.from(allowedForms).some(af => formUpper === af || formUpper.startsWith(af));
  };
  let lastIndex = -1;
  for (let i = 0; i < formsList.length && results.length < resolvedLimit; i++) {
    if (!material(formsList[i])) continue;
    lastIndex = i;

    const acc = accessions[i] ?? "";
    const accClean = acc.replace(/-/g, "");
    const primaryDoc = primaryDocs[i] ?? "";
    const docUrl = primaryDoc ? secArchiveDocumentUrl(cikInt, accClean, primaryDoc) : null;

    results.push({
      filingType: formsList[i],
      filingDate: dates[i] ?? "",
      acceptedAt: acceptedDts[i] ?? null,
      accessionNumber: acc,
      primaryDocument: primaryDoc,
      documentUrl: docUrl,
      xbrl_available: Number(isXbrl[i]) === 1 || Number(isInlineXbrl[i]) === 1,
    });
  }

  // The cap and whether it hid filings are stated (2.5.31, F-028: limit 120 returned 20 rows and said nothing).
  const moreAvailable = results.length === resolvedLimit && formsList.slice(lastIndex + 1).some(material);
  const warnings: Record<string, unknown>[] = [];
  if (limit > SEC_MATERIAL_FILINGS_MAX) {
    warnings.push({ code: "LIMIT_CAPPED", message: `limit ${limit} is above the maximum of ${SEC_MATERIAL_FILINGS_MAX}; at most ${SEC_MATERIAL_FILINGS_MAX} filings are returned${moreAvailable ? ", and more matching filings exist in SEC's recent submissions" : ""}.`, severity: "warning" });
  }
  return JSON.stringify({
    ticker,
    cik: cikPadded,
    filings: results,
    limit: { requested: limit, applied: resolvedLimit, maximum: SEC_MATERIAL_FILINGS_MAX },
    moreAvailable,
    meta: { source: "sec_submissions", materialFormsFilter: Array.from(allowedForms).sort(), retrievedAt: new Date().toISOString() },
    ...(warnings.length ? { warnings } : {}),
  });
}

const XBRL_INTELLIGENCE_CONCEPTS: Record<string, string[]> = {
  // The shared revenue concepts (sec-facts.ts); each filing is read from the one it tags.
  "revenue": [...REVENUE_CONCEPTS],
  net_income: ["NetIncomeLoss", "ProfitLoss"],
  cash: ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsAndShortTermInvestments"],
  long_term_debt: ["LongTermDebt", "LongTermDebtNoncurrent"],
  total_assets: ["Assets"],
  operating_income: ["OperatingIncomeLoss"],
};

/** The selected filing's own value for one fact across candidate concepts (sec-facts.ts). */
function extractXbrlFilingFact(
  factsData: Record<string, unknown>,
  conceptNames: string[],
  accessionNumber: string,
  documentUrl: string | null,
): Record<string, unknown> | null {
  const facts = factsData.facts && typeof factsData.facts === "object"
    ? factsData.facts as Record<string, unknown>
    : {};
  const usGaap = facts["us-gaap"] && typeof facts["us-gaap"] === "object"
    ? facts["us-gaap"] as Record<string, unknown>
    : {};
  const candidates = conceptNames.map((concept) => {
    const conceptData = usGaap[concept] && typeof usGaap[concept] === "object" ? usGaap[concept] as Record<string, unknown> : null;
    const units = conceptData?.units && typeof conceptData.units === "object" ? conceptData.units as Record<string, unknown> : {};
    return { concept, facts: Array.isArray(units.USD) ? units.USD as Record<string, unknown>[] : [] };
  });
  const hit = filingFactInAccession(candidates, accessionNumber);
  if (!hit) return null;
  const latest = hit.fact;
  const sourceEvidence = {
    sourceType: "sec_xbrl_companyfacts",
    concept: hit.concept,
    taxonomy: "us-gaap",
    unit: "USD",
    accessionNumber: latest.accn,
    filingType: latest.form,
    filingDate: latest.filed,
    periodStart: latest.start ?? null,
    periodEnd: latest.end,
    documentUrl,
  };
  const decisionGrade = Boolean(accessionNumber && latest.accn === accessionNumber && latest.end && documentUrl);
  return {
    value: latest.val,
    unit: "USD",
    period: latest.end,
    periodStart: latest.start ?? null,
    form: latest.form,
    filed: latest.filed,
    confidence: "HIGH",
    decisionGrade,
    evidence: decisionGrade ? sourceEvidence : null,
    sourceEvidence,
  };
}

export async function getSecFilingIntelligence(
  ticker: string,
  filingType: string = "10-K",
  filingIndex: number = 0,
): Promise<string> {
  filingIndex = Math.max(0, Math.min(filingIndex, 9));

  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not find EDGAR submissions for ticker '${ticker}'` } });
  }

  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const formsList: string[] = (recent.form as string[]) ?? [];
  const dates: string[] = (recent.filingDate as string[]) ?? [];
  const accessions: string[] = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs: string[] = (recent.primaryDocument as string[]) ?? [];
  const acceptedDts: string[] = (recent.acceptanceDateTime as string[]) ?? [];

  let matchCount = 0;
  let targetIdx: number | null = null;
  for (let i = 0; i < formsList.length; i++) {
    if (String(formsList[i]).toUpperCase() === filingType.toUpperCase()) {
      if (matchCount === filingIndex) { targetIdx = i; break; }
      matchCount++;
    }
  }

  if (targetIdx === null) {
    return JSON.stringify({ ok: false, error: { code: "NO_FILING_DATA", message: `No ${filingType} filing at index ${filingIndex} for '${ticker}'` } });
  }

  const accessionNumber = accessions[targetIdx] ?? "";
  const filingDate = dates[targetIdx] ?? "";
  const acceptedAt = acceptedDts[targetIdx] ?? null;
  const primaryDoc = primaryDocs[targetIdx] ?? "";
  const cikInt = parseInt(cikPadded, 10);
  const accClean = accessionNumber.replace(/-/g, "");
  const documentUrl = primaryDoc ? secArchiveDocumentUrl(cikInt, accClean, primaryDoc) : null;

  let xbrlAvailable = false;
  let xbrlStatus = "UNAVAILABLE";
  const xbrlFacts: Record<string, unknown> = {};
  const factsData = await edgarGetJson(secCompanyFactsUrl(cikPadded));
  if (factsData?.facts && typeof factsData.facts === "object") {
    xbrlAvailable = true;
    xbrlStatus = "OK";
    for (const [factName, concepts] of Object.entries(XBRL_INTELLIGENCE_CONCEPTS)) {
      const fact = extractXbrlFilingFact(factsData, concepts, accessionNumber, documentUrl);
      if (fact) xbrlFacts[factName] = fact;
    }
    if (Object.keys(xbrlFacts).length === 0) xbrlStatus = "AVAILABLE_NO_MATCHING_FACTS";
  }

  // Attempt to get filing index
  let indexStatus = "UNAVAILABLE";
  let sectionsCount = 0;
  let tablesCount = 0;
  const sectionsList: string[] = [];
  try {
    const indexRaw = await _getSecFilingIndexImpl(ticker, filingType, "latest", accessionNumber);
    const indexData = JSON.parse(indexRaw) as Record<string, unknown>;
    if (indexData.index) {
      const idx = indexData.index as Record<string, unknown[]>;
      sectionsCount = (idx.sections ?? []).length;
      tablesCount = (idx.tables ?? []).length;
      for (const s of (idx.sections ?? []).slice(0, 20)) {
        const sec = s as Record<string, unknown>;
        if (sec.heading) sectionsList.push(String(sec.heading));
      }
      indexStatus = "OK";
    }
  } catch { indexStatus = "ERROR"; }

  // Exhibits are counted from the filing index under the issuer's CIK; the
  // count had been a constant 0 (2.4.5).
  let exhibitsCount: number | null = null;
  try {
    const { edgarIndexUrl } = edgarBuildFilingUrls(cikInt, accessionNumber, null);
    exhibitsCount = (await edgarListExhibitsFromIndex(edgarIndexUrl)).length;
  } catch { exhibitsCount = null; }

  let recommendedQueries = ["revenue by segment", "risk factors", "liquidity and capital resources", "customer concentration", "long-term debt"];
  if (filingType.toUpperCase() === "10-K" || filingType.toUpperCase() === "20-F") {
    recommendedQueries.push("geographic revenue", "R&D expense", "guidance");
  } else if (filingType.toUpperCase() === "10-Q") {
    recommendedQueries.push("quarter-over-quarter revenue", "material events");
  } else if (filingType.toUpperCase() === "8-K") {
    recommendedQueries = ["material event", "exhibit content", "financial results"];
  }

  return JSON.stringify({
    ticker,
    filing: { type: filingType, accessionNumber, filedAt: filingDate, acceptedAt, documentUrl },
    xbrl_available: xbrlAvailable,
    xbrl_facts: xbrlFacts,
    index: { sections_count: sectionsCount, tables_count: tablesCount, sections: sectionsList, exhibits_count: exhibitsCount },
    recommended_queries: recommendedQueries,
    status: { xbrl: xbrlStatus, index: indexStatus, sections: sectionsCount > 0 ? "AVAILABLE" : "EMPTY" },
  });
}

export async function getSecFilingSectionMarkdown(
  ticker: string,
  section: string = "Item 1A",
  filingType: string = "10-K",
  filingIndex: number = 0,
  maxChars: number = 50000,
): Promise<string> {
  filingIndex = Math.max(0, Math.min(filingIndex, 9));
  maxChars = Math.min(Math.max(1000, maxChars), 100000);

  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not find EDGAR submissions for ticker '${ticker}'` } });
  }

  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const formsList: string[] = (recent.form as string[]) ?? [];
  const accessions: string[] = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs: string[] = (recent.primaryDocument as string[]) ?? [];

  let matchCount = 0;
  let targetIdx: number | null = null;
  for (let i = 0; i < formsList.length; i++) {
    if (String(formsList[i]).toUpperCase() === filingType.toUpperCase()) {
      if (matchCount === filingIndex) { targetIdx = i; break; }
      matchCount++;
    }
  }

  if (targetIdx === null) {
    return JSON.stringify({ ok: false, error: { code: "NO_FILING_DATA", message: `No ${filingType} filing at index ${filingIndex} for '${ticker}'` } });
  }

  const accessionNumber = accessions[targetIdx] ?? "";
  const primaryDoc = primaryDocs[targetIdx] ?? "";
  const cikInt = parseInt(cikPadded, 10);
  const accClean = accessionNumber.replace(/-/g, "");
  const documentUrl = primaryDoc ? secArchiveDocumentUrl(cikInt, accClean, primaryDoc) : null;

  if (!documentUrl) {
    return JSON.stringify({ ok: false, error: { code: "NO_FILING_DATA", message: `No document URL for ${accessionNumber}` } });
  }

  // Fetch filing HTML
  const html = await edgarGetHtml(documentUrl);
  if (!html) {
    return JSON.stringify({ ok: false, error: { code: "PROVIDER_ERROR", message: `Failed to fetch filing document: ${documentUrl}` } });
  }

  // Find section boundaries
  const bounds = findSectionBounds(html, section, maxChars);
  if (bounds.errCode === "SECTION_AMBIGUOUS") {
    return JSON.stringify({ ok: false, error: { code: "SECTION_AMBIGUOUS", message: "The section heading could not be resolved unambiguously." } });
  }

  if (bounds.startIdx === null || bounds.endIdx === null) {
    return JSON.stringify({ ok: false, error: { code: "NO_FILING_DATA", message: `Section '${section}' not found in filing` } });
  }

  const sectionStart = bounds.startIdx;
  const sectionEnd = bounds.endIdx;
  const foundHeading = bounds.foundHeading;

  // Convert to markdown (basic fallback — no sec2md in worker)
  let sectionHtml = html.slice(sectionStart, sectionEnd);
  // Strip scripts/styles iteratively to handle nested/malformed patterns
  const MAX_CELL_CHARS = 60;
  let prevHtml = "";
  while (prevHtml !== sectionHtml) {
    prevHtml = sectionHtml;
    sectionHtml = sectionHtml.replace(/<script\b[^>]*>[\s\S]*?<\/script[^>]*>/gi, "");
    sectionHtml = sectionHtml.replace(/<style\b[^>]*>[\s\S]*?<\/style[^>]*>/gi, "");
  }
  // Convert headers
  for (let lvl = 1; lvl <= 6; lvl++) {
    const prefix = "#".repeat(lvl);
    sectionHtml = sectionHtml.replace(new RegExp(`<h${lvl}[^>]*>([\\s\\S]*?)<\\/h${lvl}>`, "gi"), (_, content) => `\n${prefix} ${(content as string).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim()}\n`);
  }
  // Convert tables to pipe-delimited
  sectionHtml = sectionHtml.replace(/<table[^>]*>[\s\S]*?<\/table>/gi, (tableHtml) => {
    const rows: string[][] = [];
    const trRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
    let trMatch: RegExpExecArray | null;
    while ((trMatch = trRe.exec(tableHtml)) !== null) {
      const cells: string[] = [];
      const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
      let tdMatch: RegExpExecArray | null;
      while ((tdMatch = tdRe.exec(trMatch[1])) !== null) {
        cells.push(tdMatch[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().slice(0, MAX_CELL_CHARS));
      }
      if (cells.length > 0) rows.push(cells);
      if (rows.length >= 50) break;
    }
    if (rows.length === 0) return "";
    const lines: string[] = [];
    for (let i = 0; i < rows.length; i++) {
      lines.push("| " + rows[i].join(" | ") + " |");
      if (i === 0) lines.push("| " + rows[i].map(() => "---").join(" | ") + " |");
    }
    return "\n" + lines.join("\n") + "\n";
  });
  // Convert paragraphs/divs
  sectionHtml = sectionHtml.replace(/<(?:p|div|br)[^>]*\/?>/gi, "\n");
  sectionHtml = sectionHtml.replace(/<\/(?:p|div)>/gi, "\n");
  // Strip remaining tags, then decode entities, as Python's _strip_html_tags does: the markdown kept &#160; and
  // &#8217; (2.25.3 audit, F32). Decoding after the strip keeps a decoded "&lt;" from reading as a tag.
  let markdown = decodeHtmlEntities(sectionHtml.replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();
  markdown = markdown.replace(/\n{3,}/g, "\n\n");

  // Count tables
  const tablesInSection = (html.slice(sectionStart, sectionEnd).match(/<table[^>]*>/gi) ?? []).length;

  let truncated = false;
  if (markdown.length > maxChars) {
    markdown = markdown.slice(0, maxChars).trimEnd();
    truncated = true;
  }

  const wordCount = markdown ? markdown.split(/\s+/).length : 0;
  const warnings: Record<string, unknown>[] = [{
    code: "LIVE_SECTION_EXTRACTION_UNRELIABLE",
    message: "Section markdown is produced by a lossy Worker HTML fallback and is blocked from decision-grade use.",
    severity: "warning",
  }];
  if (wordCount < 50) {
    warnings.push({
      code: "SECTION_MARKDOWN_LOW_CONTENT",
      message: "Extracted section markdown is unusually short; verify with the source filing.",
      severity: "warning",
    });
  }

  return JSON.stringify({
    ticker,
    section: foundHeading || section,
    filingType,
    accessionNumber,
    markdown,
    tables_in_section: tablesInSection,
    word_count: wordCount,
    status: "SECTION_MARKDOWN_UNVERIFIED",
    confidence: "NOT_DECISION_GRADE",
    decisionGrade: false,
    reliabilityClass: "UNRELIABLE",
    warnings,
    source: "html_parser_fallback",
    truncated,
    sectionStartOffset: sectionStart,
    sectionEndOffset: sectionEnd,
    matchedHeading: foundHeading,
    tocSkipped: bounds.tocSkipped,
  });
}

export async function listSecFilingExhibits(ticker: string, accessionNumber: string): Promise<string> {
  if (!accessionNumber.trim()) {
    return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: "accessionNumber is required." } });
  }

  // The issuer's CIK comes from the ticker; an accession prefix names the filer
  // agent (0001193125 is Donnelley), so it is only a fallback.
  const { cikPadded: issuerCik } = await getSubmissionsForTicker(ticker);
  let cik: number | null = issuerCik ? parseInt(issuerCik, 10) : null;
  if (!cik) cik = edgarCikFromAccession(accessionNumber);
  if (!cik) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not resolve CIK for ticker '${ticker}'.` } });
  }

  const { edgarIndexUrl } = edgarBuildFilingUrls(cik, accessionNumber, null);
  const index = await edgarReadExhibitIndex(edgarIndexUrl);
  // SEC has no filing index for (CIK, accession): the accession is not that company's filing, never an empty
  // exhibit list (2.15.0, R4-ACC-EXHIBITS). Older accessions outside the recent-filings list still read.
  if (index.notFound) return accessionNotFoundEnvelope(accessionNotFoundError(ticker, accessionNumber, false));
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    accessionNumber,
    indexUrl: edgarIndexUrl,
    exhibits: index.exhibits,
  });
}

export async function getSecFilingExhibitContent(
  ticker: string,
  accessionNumber: string,
  fileName: string,
  topics: string[] | null = null,
): Promise<string> {
  if (!accessionNumber.trim()) {
    return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: "accessionNumber is required." } });
  }
  if (!fileName.trim()) {
    return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: "fileName is required." } });
  }

  // The issuer's CIK comes from the ticker; an accession prefix names the filer
  // agent (0001193125 is Donnelley), so it is only a fallback.
  const { cikPadded: issuerCik } = await getSubmissionsForTicker(ticker);
  let cik: number | null = issuerCik ? parseInt(issuerCik, 10) : null;
  if (!cik) cik = edgarCikFromAccession(accessionNumber);
  if (!cik) {
    return JSON.stringify({ ok: false, error: { code: "TICKER_NOT_FOUND", message: `Could not resolve CIK for ticker '${ticker}'.` } });
  }

  const { edgarIndexUrl } = edgarBuildFilingUrls(cik, accessionNumber, null);
  let documentUrl = edgarDocumentUrlFromRef(cik, accessionNumber, fileName);
  let html = documentUrl ? await edgarGetHtml(documentUrl, 5_000_000) : null;
  if (!html) {
    const requestedFileName = edgarDocumentFileName(fileName).toLowerCase();
    const requestedUrl = documentUrl?.toLowerCase() ?? "";
    const index = await edgarReadExhibitIndex(edgarIndexUrl);
    // No filing index for (CIK, accession): the accession is not that company's filing (2.15.0, R4-ACC-EXHIBITS).
    if (index.notFound) return accessionNotFoundEnvelope(accessionNotFoundError(ticker, accessionNumber, false));
    const exhibits = index.exhibits;
    const matched = exhibits.find((exhibit) => {
      const document = String(exhibit.document ?? "").toLowerCase();
      const exhibitUrl = String(exhibit.documentUrl ?? "").toLowerCase();
      return document === requestedFileName || exhibitUrl === requestedUrl || exhibitUrl.endsWith(`/${requestedFileName}`);
    });
    if (matched?.documentUrl) {
      documentUrl = String(matched.documentUrl);
      html = await edgarGetHtml(documentUrl, 5_000_000);
    }
  }
  if (!html) {
    return JSON.stringify({ ok: false, error: { code: "FETCH_ERROR", message: `Could not fetch exhibit '${fileName}'.` } });
  }

  const cleanText = htmlToReadableText(html);
  const warnings: Record<string, unknown>[] = [];
  const topicList = (topics ?? []).filter((topic) => typeof topic === "string" && topic.trim()).map((topic) => topic.trim());

  if (topicList.length) {
    const matchedParagraphs = filterParagraphsByTopics(cleanText, topicList);
    if (!matchedParagraphs.length) {
      warnings.push({ code: "NO_TOPIC_MATCHES", message: `No paragraphs matched the provided topics: ${topicList.join(", ")}` });
    }
    return JSON.stringify({
      ticker: ticker.toUpperCase(),
      accessionNumber,
      fileName,
      documentUrl,
      filteredByTopics: topicList,
      matchedParagraphs,
      totalTextLength: cleanText.length,
      warnings,
    });
  }

  const maxChars = 50_000;
  const truncated = cleanText.length > maxChars;
  if (truncated) {
    warnings.push({ code: "TEXT_TRUNCATED", message: `Text truncated from ${cleanText.length} to ${maxChars} characters.` });
  }
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    accessionNumber,
    fileName,
    documentUrl,
    filteredByTopics: null,
    text: cleanText.slice(0, maxChars),
    totalTextLength: cleanText.length,
    truncated,
    warnings,
  });
}

export function xbrlSourceEvidence(parsed: Record<string, unknown>): Record<string, unknown> | null {
  const context = parsed.xbrlContext;
  if (!context || typeof context !== "object") return null;
  const row = context as Record<string, unknown>;
  return {
    sourceType: "sec_xbrl_companyconcept",
    concept: row.concept ?? null,
    taxonomy: row.taxonomy ?? null,
    unit: row.unit ?? parsed.unit ?? null,
    accessionNumber: parsed.accessionNumber ?? row.accessionNumber ?? null,
    filingType: parsed.filingType ?? row.form ?? null,
    filingDate: parsed.filingDate ?? row.filedAt ?? null,
    periodStart: row.periodStart ?? null,
    periodEnd: row.periodEnd ?? row.instant ?? null,
    fiscalYear: row.fiscalYear ?? null,
    fiscalPeriod: row.fiscalPeriod ?? null,
    dimensions: row.dimensions ?? {},
    documentUrl: parsed.documentUrl ?? null,
    indexUrl: parsed.indexUrl ?? null,
  };
}

export function isDecisionGradeXbrlFact(
  parsed: Record<string, unknown>,
  sourceEvidence: Record<string, unknown> | null,
  status: unknown,
): boolean {
  if (parsed.value == null || String(status).toUpperCase() !== "FOUND") return false;
  if (!String(parsed.extractionMethod ?? "").toUpperCase().includes("XBRL")) return false;
  if (!parsed.xbrlContext || typeof parsed.xbrlContext !== "object" || Array.isArray(parsed.xbrlContext) || !sourceEvidence) return false;
  return ["sourceType", "concept", "accessionNumber", "periodEnd"]
    .every((field) => sourceEvidence[field] != null && String(sourceEvidence[field]).trim() !== "");
}

export async function querySecFilingIndex(
  ticker: string,
  filingType = "10-K",
  period = "latest",
  accessionNumber: string | null = null,
  queryType = "",
  params: Record<string, unknown> = {},
  returnEvidence = true,
  detailLevel = "compact",
): Promise<string> {
  const detail = String(detailLevel || "compact").toLowerCase();
  if (!["compact", "evidence", "raw"].includes(detail)) {
    return JSON.stringify({
      status: "INPUT_VALIDATION_ERROR",
      queryType,
      ticker,
      filingType,
      period,
      answer: null,
      confidence: "NOT_DISCLOSED",
      evidence: [],
      warnings: [{ code: "INPUT_VALIDATION_ERROR", message: "detailLevel must be one of: compact, evidence, raw" }],
    });
  }

  const query = String(queryType || "").trim();
  const warnings: Record<string, unknown>[] = [];

  const shapeEvidence = (ev: Record<string, unknown>): Record<string, unknown> => {
    const out: Record<string, unknown> = {
      filingDate: ev.filingDate ?? null,
      acceptedAt: ev.acceptedAt ?? null,
      accessionNumber: ev.accessionNumber ?? null,
      documentUrl: ev.documentUrl ?? null,
      sectionHeading: ev.sectionHeading ?? null,
      tableTitle: ev.tableTitle ?? null,
      sourceTableId: ev.sourceTableId ?? null,
    };
    if (detail === "evidence" || detail === "raw") {
      out.sourceRows = Array.isArray(ev.sourceRows) ? ev.sourceRows : [];
      out.sourceColumns = Array.isArray(ev.sourceColumns) ? ev.sourceColumns : [];
      if (ev.excerpt != null) out.excerpt = ev.excerpt;
    }
    return out;
  };

  const result = (
    status: string,
    answer: Record<string, unknown> | null,
    confidence: string,
    evidenceItems: Record<string, unknown>[] = [],
    warnItems: Record<string, unknown>[] = [],
  ): string => JSON.stringify({
    status,
    queryType: query,
    ticker,
    filingType,
    period,
    answer,
    confidence,
    evidence: returnEvidence ? evidenceItems : [],
    warnings: warnItems,
  });

  const missingParam = (name: string): string => result(
    "INPUT_VALIDATION_ERROR",
    null,
    "NOT_DISCLOSED",
    [],
    [{ code: "INPUT_VALIDATION_ERROR", message: `Missing required params.${name} for query_type=${query}` }],
  );

  const supported = new Set([
    "geographic_revenue_share",
    "revenue_exposure",
    "china_exposure",
    "risk_factor_mentions",
    "customer_concentration",
    "total_revenue",
    "segment_revenue",
  ]);
  if (!supported.has(query)) {
    return mcpFailure(
      "query_sec_filing_index",
      "UNSUPPORTED_QUERY_TYPE",
      `Unsupported query type '${query}'. Supported types are: ${Array.from(supported).sort().join(", ")}`,
      {
        metaExtra: {
          supportedQueryTypes: Array.from(supported),
        }
      }
    );
  }

  if (query === "geographic_revenue_share") {
    const region = String(params.region ?? "").trim();
    if (!region) return missingParam("region");
    const geo = parseObjectJson(await extractGeographicRevenue(ticker, region, filingType, period, accessionNumber, detail));
    // An explicit accession that is not among the company's recent filings fails closed here too (2.15.0, R4-ACC-QUERY).
    if (geo.ok === false && geo.error && typeof geo.error === "object" && isAccessionNotFound(geo.error as Record<string, unknown>)) {
      return accessionNotFoundEnvelope(geo.error as Record<string, unknown>);
    }
    const evidenceObj = geo.evidence && typeof geo.evidence === "object" ? geo.evidence as Record<string, unknown> : {};
    const evidence = Object.keys(evidenceObj).length > 0 ? [shapeEvidence(evidenceObj)] : [];
    let status = geo.value != null ? "ANSWERED" : normalizeStatus(geo);
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    const answer = {
      region,
      value: geo.value ?? null,
      denominator: geo.denominator ?? null,
      valueRatio: geo.valueRatio ?? null,
      valuePct: geo.valuePct ?? null,
      unit: geo.unit ?? "USD",
    };
    const confidence = String(geo.confidence ?? (status === "ANSWERED" ? "LOW" : "NOT_DISCLOSED"));
    return result(status, answer, confidence, evidence, warnings);
  }

  if (query === "revenue_exposure") {
    const exposureQuery = String(params.exposure_query ?? "").trim();
    if (!exposureQuery) return missingParam("exposure_query");
    const rex = parseObjectJson(await extractRevenueExposure(ticker, exposureQuery, filingType, period, detail, accessionNumber));
    const matches = Array.isArray(rex.matches) ? rex.matches.filter((m) => m && typeof m === "object") as Record<string, unknown>[] : [];
    const first = matches.length > 0 ? matches[0] : {};
    const evidenceObj = first.evidence && typeof first.evidence === "object" ? first.evidence as Record<string, unknown> : {};
    const evidence = Object.keys(evidenceObj).length > 0 ? [shapeEvidence(evidenceObj)] : [];
    let status = matches.length > 0 && String(rex.status ?? "") === "FOUND_REVENUE_EXPOSURE" ? "ANSWERED" : String(rex.status ?? "NOT_FOUND");
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    return result(status, {
      exposureQuery,
      value: first.value ?? null,
      denominator: first.denominator ?? null,
      valueRatio: first.valueRatio ?? null,
      valuePct: first.valuePct ?? null,
      period: first.period ?? null,
    }, String(first.confidence ?? (status === "ANSWERED" ? "LOW" : "NOT_DISCLOSED")), evidence, warnings);
  }

  if (query === "china_exposure") {
    const china = parseObjectJson(await extractChinaExposure(ticker, filingType, period, accessionNumber, detail));
    const overall = String(china.overallStatus ?? "NOT_FOUND");
    const answer = {
      revenueExposure: china.revenueExposure ?? null,
      manufacturingExposure: china.manufacturingExposure ?? null,
      entityExposure: china.entityExposure ?? null,
      bankExposure: china.bankExposure ?? null,
      riskFactorExposure: china.riskFactorExposure ?? null,
      overallStatus: overall,
    };
    const evidence: Record<string, unknown>[] = [];
    for (const key of ["revenueExposure", "manufacturingExposure", "entityExposure", "bankExposure", "riskFactorExposure"]) {
      const block = china[key];
      if (!block || typeof block !== "object") continue;
      const b = block as Record<string, unknown>;
      if (b.evidence && typeof b.evidence === "object" && !Array.isArray(b.evidence)) evidence.push(shapeEvidence(b.evidence as Record<string, unknown>));
      if (Array.isArray(b.evidence)) {
        for (const item of b.evidence) {
          if (item && typeof item === "object") evidence.push(shapeEvidence(item as Record<string, unknown>));
        }
      }
    }
    let status = (overall === "FOUND_REVENUE_EXPOSURE" || overall === "FOUND_NON_REVENUE_EXPOSURE") ? "ANSWERED" : overall;
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    const confidence = status === "ANSWERED" ? "HIGH" : (overall === "NOT_DISCLOSED" ? "NOT_DISCLOSED" : "LOW");
    return result(status, answer, confidence, evidence, warnings);
  }

  // An explicit accession that is not among the company's filings fails closed for the pinned reads too, as
  // geographic_revenue_share does; checked after each query's own params (2.24.0, F21).
  const accessionMiss = async (): Promise<string | null> => {
    if (!accessionNumber || !accessionNumber.trim()) return null;
    const resolved = await resolveSecFiling(ticker, filingType, accessionNumber.trim());
    return !resolved.ok && isAccessionNotFound(resolved.error) ? accessionNotFoundEnvelope(resolved.error) : null;
  };

  if (query === "risk_factor_mentions") {
    const terms = Array.isArray(params.terms) ? params.terms.map((t) => String(t)) : [];
    if (terms.length === 0) return missingParam("terms");
    const riskMiss = await accessionMiss();
    if (riskMiss) return riskMiss;
    const risk = parseObjectJson(await extractRiskFactorMentions(ticker, terms, filingType, period, detail, accessionNumber));
    const matches = Array.isArray(risk.matches) ? risk.matches.filter((m) => m && typeof m === "object") as Record<string, unknown>[] : [];
    const evidence = matches
      .map((m) => (m.evidence && typeof m.evidence === "object" && !Array.isArray(m.evidence)) ? shapeEvidence(m.evidence as Record<string, unknown>) : null)
      .filter((m): m is Record<string, unknown> => m != null);
    let status = matches.length > 0 ? "ANSWERED" : String(risk.status ?? "NOT_FOUND");
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    return result(status, { terms, matches }, status === "ANSWERED" ? "MEDIUM" : "LOW", evidence, warnings);
  }

  if (query === "customer_concentration") {
    const customerLabel = String(params.customer_label ?? "").trim();
    const custMiss = await accessionMiss();
    if (custMiss) return custMiss;
    const cust = parseObjectJson(await extractCustomerConcentration(ticker, filingType, period, detail, accessionNumber));
    let customers = Array.isArray(cust.customers) ? cust.customers.filter((c) => c && typeof c === "object") as Record<string, unknown>[] : [];
    if (customerLabel) customers = customers.filter((c) => String(c.label ?? "").toLowerCase() === customerLabel.toLowerCase());
    const evidence = customers
      .map((c) => (c.evidence && typeof c.evidence === "object" && !Array.isArray(c.evidence)) ? shapeEvidence(c.evidence as Record<string, unknown>) : null)
      .filter((m): m is Record<string, unknown> => m != null);
    let status = customers.length > 0 ? "ANSWERED" : String(cust.status ?? "NOT_FOUND");
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    const confidence = status === "ANSWERED" ? "HIGH" : (status === "NOT_DISCLOSED" ? "NOT_DISCLOSED" : "LOW");
    return result(status, { customerLabel: customerLabel || null, customers }, confidence, evidence, warnings);
  }

  if (query === "total_revenue") {
    const totalMiss = await accessionMiss();
    if (totalMiss) return totalMiss;
    const total = parseObjectJson(await extractTotalRevenue(ticker, filingType, period, accessionNumber));
    const evidenceObj = total.evidence && typeof total.evidence === "object" ? total.evidence as Record<string, unknown> : {};
    const evidence = Object.keys(evidenceObj).length > 0 ? [shapeEvidence(evidenceObj)] : [];
    let status = total.value != null ? "ANSWERED" : String(total.status ?? "NOT_FOUND");
    if (status === "ANSWERED" && evidence.length === 0) {
      status = "NOT_FOUND";
      warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
    }
    return result(
      status,
      { value: total.value ?? null, period: total.period ?? null, unit: "USD" },
      String(total.confidence ?? (status === "ANSWERED" ? "LOW" : "NOT_DISCLOSED")),
      evidence,
      warnings,
    );
  }

  const segmentName = String(params.segment ?? "").trim();
  const segMiss = await accessionMiss();
  if (segMiss) return segMiss;
  const seg = parseObjectJson(await extractSegmentRevenue(ticker, filingType, period, detail, accessionNumber));
  let segments = Array.isArray(seg.segments) ? seg.segments.filter((s) => s && typeof s === "object") as Record<string, unknown>[] : [];
  if (segmentName) segments = segments.filter((s) => String(s.label ?? "").toLowerCase() === segmentName.toLowerCase());
  const evidence = segments
    .map((s) => (s.evidence && typeof s.evidence === "object" && !Array.isArray(s.evidence)) ? shapeEvidence(s.evidence as Record<string, unknown>) : null)
    .filter((m): m is Record<string, unknown> => m != null);
  let status = segments.length > 0 ? "ANSWERED" : (segmentName ? "NOT_FOUND" : String(seg.status ?? "NOT_FOUND"));
  if (status === "ANSWERED" && evidence.length === 0) {
    status = "NOT_FOUND";
    warnings.push({ code: "EVIDENCE_REQUIRED", message: "ANSWERED responses require evidence." });
  }
  const segmentConfidence = segments.length > 0 ? String(segments[0].confidence ?? "LOW") : null;
  const confidence = status === "ANSWERED" ? segmentConfidence ?? "LOW" : (status === "NOT_DISCLOSED" ? "NOT_DISCLOSED" : "LOW");
  if (Array.isArray(seg.warnings)) warnings.push(...seg.warnings as Record<string, unknown>[]);
  return result(status, { segment: segmentName || null, segments }, confidence, evidence, warnings);
}
