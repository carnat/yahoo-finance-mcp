/**
 * SEC filing extractors and exposure tools: revenue, segment, geographic, customer-concentration, China and risk-factor
 * exposure, capital structure, dilution, funding, operating drivers and share-count scenarios (Phase 5 move out of
 * yahoo-finance.ts). Helpers shared with other domains still live in yahoo-finance.ts and are imported back;
 * yahoo-finance.ts re-exports every moved name, so tools.ts and the parity bundle are unchanged.
 */

import { BoundedTtlCache } from "./cache.js";
import { countCacheEvent } from "./request-context.js";
import {
  capitalStructure,
  wantsMaturityText,
  MATURITY_SEARCH_TERMS,
  type IxSource,
  type TextMatch,
  type DilutionInput,
  dilutionBridge,
  CONVERTIBLE_SETTLEMENT_SEARCH_TERMS,
  CAPPED_CALL_SEARCH_TERMS,
  atmEvidence,
  parseIxbrl,
  SHARE_CLAIM_SEARCH_TERMS,
  WARRANT_LIFECYCLE_SEARCH_TERMS,
  groupedCount,
  type IxDocument,
} from "./capital-structure.js";
import { parseFilingPeriod } from "./sec-facts.js";
import { fundingCapexSchedule } from "./funding-schedule.js";
import { operatingDriverLedger } from "./driver-ledger.js";
import { parseShareScenarios, shareCountScenarios } from "./share-scenarios.js";
import { customerConcentration, type ConcentrationFinding } from "./extraction-rules.js";
import {
  edgarGetJson,
  getSubmissionsForTicker,
  edgarGetHtml,
  fetchSecDocument,
  SEC_DOCUMENT_READ_MAX_CHARS,
} from "./clients/edgar.js";
import { secCompanyFactsUrl } from "./clients/endpoints.js";
import {
  parseNumericCell,
  DASH_CELL_RE,
  type UnitScaleSource,
  lazyDocumentScale,
  stripHtmlTags,
  parseFinancialTableRows,
  TOTAL_LABELS,
  detectUnitScale,
  UNIT_SCALE_ASSUMED_WARNING,
} from "./clients/html.js";
import { compactExcerpt, parseObjectJson } from "./util.js";
import {
  getSecFilingIndex,
  isAccessionNotFound,
  legacyAccessionMissPayload,
  searchFilingText,
  resolveSecFiling,
  accessionNotFoundEnvelope,
  getFilingData,
  xbrlSourceEvidence,
  isDecisionGradeXbrlFact,
} from "./sec.js";
import exposurePresets from "./exposure-presets.json";
import { earningsReleasePicks, readReleaseText } from "./earnings.js";

/** The China exposure preset (Q5): the terms extractChinaExposure and extractExposure search, shared with Python. */
export const CHINA_PRESET = exposurePresets.presets.china;

export type ResolvedSecFiling = {
  ticker: string;
  cikPadded: string;
  cikInt: number;
  requestedFilingType: string;
  filingType: string;
  filingDate: string | null;
  // The period of report (SEC submissions reportDate): the fiscal period the filing covers.
  reportDate?: string | null;
  acceptedAt: string | null;
  accessionNumber: string;
  primaryDocument: string;
  documentUrl: string;
  availableFilingTypes: string[];
  suggestedFilingTypes: string[];
  warnings: Record<string, unknown>[];
};

/** A percent cell, or a number whose "%" sign sits in the next cell. */
function isPctCell(row: string[], col: number): boolean {
  return row[col].includes("%") || (col + 1 < row.length && row[col + 1].trim() === "%");
}

function amountCells(row: string[]): { col: number; value: number }[] {
  const out: { col: number; value: number }[] = [];
  row.forEach((cell, col) => {
    if (isPctCell(row, col)) return;
    if (DASH_CELL_RE.test(cell)) { out.push({ col, value: 0 }); return; }
    const v = parseNumericCell(cell);
    if (v !== null) out.push({ col, value: v });
  });
  return out;
}

/**
 * A region row's first positive amount and the total row's amount in the same
 * column, paired by position among amount cells. statedPct is the table's own
 * percentage in the cell after the region's value, when it has one.
 */
export function alignedGeoAmounts(regionRow: string[], totalRow: string[]): { regionVal: number; totalVal: number; valueCol: number; totalCol: number; statedPct: number | null } | null {
  const region = amountCells(regionRow);
  const k = region.findIndex((c) => c.value > 0);
  if (k < 0) return null;
  const total = amountCells(totalRow)[k];
  if (!total) return null;
  const nextCol = region[k].col + 1;
  const statedPct = nextCol < regionRow.length && isPctCell(regionRow, nextCol) && regionRow[nextCol].trim() !== "%" ? parseNumericCell(regionRow[nextCol]) : null;
  return { regionVal: region[k].value, totalVal: total.value, valueCol: region[k].col, totalCol: total.col, statedPct };
}

/**
 * The header of the k-th amount column: the header row with one label per amount column ("% of
 * Total" headers dropped), prefixed by a period group ("Three Months Ended") when a row above
 * spans the columns evenly (2.5.9).
 */
export function geoColumnHeader(rows: string[][], regionRowIdx: number, k: number): string {
  const n = amountCells(rows[regionRowIdx]).length;
  const labels = (r: string[]) => r.map((c) => c.trim()).filter((c) => c && !SHARE_HEADER_RE.test(c));
  let label = "";
  let group = "";
  for (let i = 0; i < regionRowIdx; i++) {
    const cells = labels(rows[i]);
    if (cells.length === 0 || cells.some((c) => parseNumericCell(c) !== null && !/\d{4}/.test(c))) continue;
    if (cells.length === n && !label) label = cells[k];
    else if (cells.length > 0 && cells.length < n && n % cells.length === 0 && !group && !label) group = cells[Math.floor(k / (n / cells.length))];
  }
  return [group, label].filter(Boolean).join(" ");
}

// A computed share more than this many points from the table's own percentage is a misread column.
const STATED_PCT_TOLERANCE = 1.0;
const SHARE_HEADER_RE = /%\s*of\b|\bpercent(?:age)?\s+of\b/i;

function escapeRegexLiteral(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function geoRegionAliases(region: string): string[] {
  const lower = region.toLowerCase().trim();
  const compact = lower.replace(/\s+/g, "");
  const aliases = new Set<string>([lower]);
  if (lower === "china" || lower === "prc" || lower === "mainland china") {
    for (const alias of [
      "china",
      "mainland china",
      "people's republic of china",
      "peoples republic of china",
      "prc",
      "country:cn",
      "srt:chinamember",
      "chinamember",
      "mainlandchinamember",
    ]) aliases.add(alias);
  } else if (lower === "greater china" || compact === "greaterchina") {
    aliases.add("greater china");
    aliases.add("greaterchinamember");
  } else if (lower === "hong kong" || compact === "hongkong") {
    aliases.add("hong kong");
    aliases.add("hongkong");
    aliases.add("country:hk");
    aliases.add("srt:hongkongmember");
    aliases.add("hongkongmember");
  } else if (lower === "taiwan") {
    aliases.add("taiwan");
    aliases.add("country:tw");
    aliases.add("srt:taiwanmember");
    aliases.add("taiwanmember");
  }
  return [...aliases].filter(Boolean);
}

function textContainsGeoRegion(text: string, region: string): boolean {
  const lower = text.toLowerCase();
  return geoRegionAliases(region).some((term) => {
    if (!term) return false;
    if (/^[a-z0-9 ]+$/.test(term)) {
      return new RegExp(`\\b${escapeRegexLiteral(term).replace(/\s+/g, "\\s+")}\\b`, "i").test(lower);
    }
    return lower.includes(term);
  });
}

function rowMatchesGeoRegion(row: string[], region: string): boolean {
  return row.some((cell) => textContainsGeoRegion(cell, region));
}

/**
 * Search an SEC filing HTML document for a geographic revenue table.
 * Returns { pct, usd, sectionHeading, parsedTables } or null if not found.
 */
export function extractGeoRevenueFromHtml(
  html: string,
  region: string
): {
  pct: number;
  usd: number | null;
  denominator: number | null;
  sectionHeading: string;
  parsedTables: unknown[];
  unitScale: "thousands" | "millions" | "billions" | "actual";
  unitScaleSource: UnitScaleSource;
  rawValue: string | null;
  rawDenominator: string | null;
  sourceRows: string[][];
  sourceColumns: string[];
  statedPct: number | null;
  /** The chosen table's lead-in text (in memory only, for extract_revenue_exposure's exposureType). */
  tableLead: string;
} | null {
  const documentScale = lazyDocumentScale(html);
  // Every table that names the region and is about revenue is a candidate
  // (scanning windows after the first mentions missed tables late in long
  // filings). Tables introduced as a geographic breakdown come first; a
  // table without revenue or sales in it or its lead-in (a properties list
  // of square footage by location) never counts.
  const candidateTables: { pos: number; tableHtml: string; rows: string[][]; score: number; lead: string }[] = [];
  let scanned = 0;
  for (const m of html.matchAll(/<table[^>]*>[\s\S]*?<\/table>/gi)) {
    if (++scanned > 800) break;
    const tableHtml = m[0];
    if (!textContainsGeoRegion(tableHtml, region)) continue;
    const pos = m.index ?? 0;
    // The lead-in stops at the previous table.
    const before = html.slice(Math.max(0, pos - 1_500), pos);
    const lead = stripHtmlTags(before.slice(before.toLowerCase().lastIndexOf("</table>") + 1)).slice(-800);
    const tableText = stripHtmlTags(tableHtml).slice(0, 600);
    const context = `${lead} ${tableText}`.toLowerCase();
    if (!/revenue|net sales|\bsales\b/.test(context) || /square f(?:oo|ee)t/i.test(tableText)) continue;
    // An asset table after a revenue paragraph is not revenue: COHR's long-lived assets by country (2.5.20).
    if (/long-lived assets|property,? plant,? and equipment|\btotal assets\b|identifiable assets/i.test(tableText) && !/revenue|net sales|\bsales\b/i.test(tableText)) continue;
    const rows = parseFinancialTableRows(tableHtml);
    if (rows.length < 2) continue;
    const score = /geograph|by region|by country|by location|region of|country of|location of (?:the )?customer/.test(context) ? 2 : 1;
    candidateTables.push({ pos, tableHtml, rows, score, lead });
  }
  if (candidateTables.length === 0) return null;
  candidateTables.sort((a, b) => b.score - a.score || a.pos - b.pos);

  for (const tbl of candidateTables) {
    const { rows, tableHtml } = tbl;

    // Find region row
    let regionRowIdx: number | null = null;
    for (let i = 0; i < rows.length; i++) {
      if (rowMatchesGeoRegion(rows[i], region)) { regionRowIdx = i; break; }
    }
    if (regionRowIdx === null) continue;

    // Find total row
    let totalRowIdx: number | null = null;
    for (let i = 0; i < rows.length; i++) {
      // A total row carries amounts: COHR's "Revenues" header row is a caption, not the total (2.5.20).
      if (rows[i].some(c => TOTAL_LABELS.has(c.trim().toLowerCase())) && rows[i].some(c => parseNumericCell(c) !== null)) { totalRowIdx = i; break; }
    }
    if (totalRowIdx === null) {
      for (let i = rows.length - 1; i >= 0; i--) {
        if (rows[i].some(c => parseNumericCell(c) !== null)) { totalRowIdx = i; break; }
      }
    }
    if (totalRowIdx === null || totalRowIdx === regionRowIdx) continue;

    // The value and the total in the same column, paired among amount cells.
    const aligned = alignedGeoAmounts(rows[regionRowIdx], rows[totalRowIdx]);
    if (!aligned) continue;
    const { regionVal, totalVal, valueCol, totalCol, statedPct } = aligned;
    // A region cannot exceed the total it is part of.
    if (totalVal <= 0 || regionVal <= 0 || regionVal > totalVal * 1.001) continue;

    const pct = Math.round((regionVal / totalVal) * 10000) / 10000;
    // The table's own share for the region must agree with the computed one. A
    // percent beside a value is a share only under a "% of total" header; a
    // "Change" column is not.
    const shareHeader = rows.slice(0, regionRowIdx).some((r) => r.some((c) => SHARE_HEADER_RE.test(c)));
    const statedShare = shareHeader ? statedPct : null;
    if (statedShare != null && Math.abs(pct * 100 - statedShare) > STATED_PCT_TOLERANCE) continue;
    const contextHtml = html.slice(Math.max(0, tbl.pos - 3_000), tbl.pos);
    const { multiplier: unitMult, source: unitScaleSource } = detectUnitScale(tableHtml, contextHtml, documentScale);
    const usd = regionVal * unitMult;
    const denominator = totalVal * unitMult;
    const unitScale = unitMult === 1e3 ? "thousands" : unitMult === 1e6 ? "millions" : unitMult === 1e9 ? "billions" : "actual";

    // Find nearest section heading
    const preHtml = html.slice(Math.max(0, tbl.pos - 6_000), tbl.pos);
    const hMatches = [...preHtml.matchAll(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/gi)];
    const sectionHeading = hMatches.length > 0
      ? stripHtmlTags(hMatches[hMatches.length - 1][1])
      : "";

    const headerRow = rows[0] ?? [];
    // A header row without a label cell is one cell shorter than the data rows.
    const headerOffset = Math.max(0, rows[regionRowIdx].length - headerRow.length);
    const headerCol = valueCol - headerOffset;
    const ordinal = amountCells(rows[regionRowIdx]).findIndex((c) => c.col === valueCol);
    const sourceColumn = geoColumnHeader(rows, regionRowIdx, ordinal)
      || (headerCol >= 0 && headerCol < headerRow.length ? String(headerRow[headerCol]).trim() : "");
    const rawValue = valueCol < rows[regionRowIdx].length ? String(rows[regionRowIdx][valueCol]) : null;
    const rawDenominator = String(rows[totalRowIdx][totalCol]);
    const sourceRows = [
      [String(rows[regionRowIdx][0] ?? region), rawValue ?? ""],
      // An unlabeled total row's first cell is a number, not a label.
      [/[A-Za-z]/.test(String(rows[totalRowIdx][0] ?? "")) ? String(rows[totalRowIdx][0]) : "Total (unlabeled row)", rawDenominator ?? ""],
    ];
    return {
      pct,
      usd,
      denominator,
      sectionHeading,
      parsedTables: [{ rows }],
      unitScale,
      unitScaleSource,
      rawValue,
      rawDenominator,
      sourceRows,
      sourceColumns: sourceColumn ? [sourceColumn] : [],
      statedPct: statedShare,
      tableLead: tbl.lead,
    };
  }

  return null;
}

/** A table lead-in that introduces a product or category breakdown ("net sales by category", "disaggregated net
 * sales"), not a geographic one (2.25.4 audit, F40). */
export const PRODUCT_BREAKDOWN_LEAD_RE = /\bby (?:product|products|category|categories|product (?:line|category))\b|\bdisaggregat\w*/i;

const EXPOSURE_XBRL_TOKEN_RE = /\b(?:us-gaap|srt|country|dei):[A-Za-z0-9_.-]+\b/gi;
const EXPOSURE_COMPETITION_RE = /\b(?:competitors?|competition|compete(?:s|d|ing)?\s+with)\b/i;
const EXPOSURE_RELATIONSHIP_RE = /\b(?:supplier|vendor|customer|manufactur\w*|assembl\w*|facilit\w*|factory|subsidiar\w*|operations?|production|procur\w*|sourc\w*|partner\w*|loan|credit|deposit|account|lender|borrow\w*)\b/i;

/** Index of the first whole-word match of term, plural forms included ("tariff" matches "tariffs"); -1 if none. */
function exposureTermIndex(text: string, term: string): number {
  const escaped = String(term).trim().replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  if (!escaped) return -1;
  const match = new RegExp(`(^|[^A-Za-z0-9])${escaped}(?:e?s)?(?![A-Za-z0-9])`, "i").exec(text);
  return match ? match.index + match[1].length : -1;
}

function exposureTermPresent(text: string, term: string): boolean {
  return exposureTermIndex(text, term) >= 0;
}

/** About maxLen characters around the first term, starting at its sentence when that fits. */
export function termCenteredExcerpt(text: string, terms: string[], maxLen: number): string {
  const cleaned = text.replace(/\s+/g, " ").trim();
  if (cleaned.length <= maxLen) return cleaned;
  const starts = terms.map((term) => exposureTermIndex(cleaned, term)).filter((i) => i >= 0);
  if (starts.length === 0) return compactExcerpt(cleaned, maxLen);
  const at = Math.min(...starts);
  let start = Math.max(0, Math.min(at - Math.floor(maxLen / 3), cleaned.length - maxLen));
  const sentence = cleaned.lastIndexOf(". ", at - 1);
  if (sentence >= start) {
    start = sentence + 2;
  } else if (start > 0) {
    const space = cleaned.indexOf(" ", start);
    if (space >= 0 && space < at) start = space + 1;
  }
  let end = start + maxLen;
  if (end < cleaned.length) {
    const space = cleaned.lastIndexOf(" ", end);
    if (space > at) end = space;
  }
  return `${start > 0 ? "..." : ""}${cleaned.slice(start, end).trim()}${end < cleaned.length ? "..." : ""}`;
}

export function readableExposureExcerpt(
  value: unknown,
  terms: string[],
  maxLen = 240,
  minWords = 5,
): string {
  const excerpt = termCenteredExcerpt(String(value ?? "").replace(EXPOSURE_XBRL_TOKEN_RE, " "), terms, maxLen);
  if (!excerpt || !terms.some((term) => exposureTermPresent(excerpt, term))) return "";
  const words = excerpt.match(/[A-Za-z]{2,}/g) ?? [];
  return words.length >= minWords ? excerpt : "";
}

function contextualEntityExcerpt(value: unknown, entity: string, maxLen = 240): string {
  const excerpt = readableExposureExcerpt(value, [entity], maxLen, 4);
  if (!excerpt || EXPOSURE_COMPETITION_RE.test(excerpt)) return "";
  return EXPOSURE_RELATIONSHIP_RE.test(excerpt) ? excerpt : "";
}

export function normalizeStatus(payload: Record<string, unknown>): string {
  const status = String(payload.status ?? payload.code ?? "").toUpperCase();
  if (status === "FILING_NOT_FOUND_TRY_OTHER_TYPE") return "FILING_NOT_FOUND_TRY_OTHER_TYPE";
  if (status === "FILING_TEXT_NOT_AVAILABLE") return "FILING_TEXT_NOT_AVAILABLE";
  if (status === "EXTRACTION_FAILED") return "EXTRACTION_FAILED";
  if (status === "TABLE_NOT_PARSED") return "TABLE_NOT_PARSED";
  if (status === "PROVIDER_LIMITATION") return "PROVIDER_LIMITATION";
  if (status === "NO_DIMENSIONAL_REVENUE_FACT") return "NO_DIMENSIONAL_REVENUE_FACT";
  // A failed SEC read is not a missing fact (2.5.17).
  if (status === "PROVIDER_ERROR") return "PROVIDER_ERROR";
  const source = String(payload.source ?? "").toUpperCase();
  const confidence = String(payload.confidence ?? "").toUpperCase();
  if (source === "FILING_NOT_FOUND_TRY_OTHER_TYPE" || confidence === "FILING_NOT_FOUND_TRY_OTHER_TYPE") return "FILING_NOT_FOUND_TRY_OTHER_TYPE";
  if (source === "EXTRACTION_FAILED" || confidence === "EXTRACTION_FAILED") return "EXTRACTION_FAILED";
  if (source === "TABLE_NOT_PARSED" || confidence === "TABLE_NOT_PARSED") return "TABLE_NOT_PARSED";
  if (source === "PROVIDER_LIMITATION" || confidence === "PROVIDER_LIMITATION") return "PROVIDER_LIMITATION";
  if (source === "NO_DIMENSIONAL_REVENUE_FACT" || confidence === "NO_DIMENSIONAL_REVENUE_FACT") return "NO_DIMENSIONAL_REVENUE_FACT";
  if (source === "NOT_DISCLOSED" || confidence === "NOT_DISCLOSED") return "NOT_DISCLOSED";
  if (source === "CONFLICTING" || confidence === "CONFLICTING") return "CONFLICTING";
  return "NOT_FOUND";
}

function hasWarningCode(warnings: unknown[], code: string): boolean {
  return warnings.some((w) => typeof w === "object" && w != null && (w as Record<string, unknown>).code === code);
}

const NON_DECISION_REVENUE_STATUSES = new Set([
  "EXTRACTION_FAILED",
  "TABLE_NOT_PARSED",
  "PROVIDER_LIMITATION",
  "NO_DIMENSIONAL_REVENUE_FACT",
  "FILING_NOT_FOUND_TRY_OTHER_TYPE",
  "FILING_TEXT_NOT_AVAILABLE",
]);

function explicitRevenueLimitationStatus(status: unknown, warnings: unknown[] = []): string | null {
  const normalized = String(status ?? "").toUpperCase();
  if (NON_DECISION_REVENUE_STATUSES.has(normalized)) return normalized;
  if (hasWarningCode(warnings, "TABLE_NOT_PARSED")) return "TABLE_NOT_PARSED";
  if (hasWarningCode(warnings, "NO_DIMENSIONAL_REVENUE_FACT")) return "NO_DIMENSIONAL_REVENUE_FACT";
  return null;
}

async function mayBe20FFiler(ticker: string): Promise<boolean> {
  const { submissions } = await getSubmissionsForTicker(ticker);
  if (!submissions || typeof submissions !== "object") return false;
  const filings = (submissions.filings as Record<string, unknown>) ?? {};
  const recent = (filings.recent as Record<string, unknown>) ?? {};
  const forms = Array.isArray(recent.form) ? recent.form : [];
  return forms.some((form: unknown) => String(form ?? "").toUpperCase() === "20-F");
}

export async function extractGeographicRevenue(
  ticker: string,
  region: string,
  filingType = "10-K",
  period = "latest",
  accessionNumber: string | null = null,
  detailLevel = "compact",
): Promise<string> {
  if (!region || !region.trim()) {
    return JSON.stringify({
      ticker,
      factType: "geographic_revenue",
      region,
      period: null,
      rawValue: null,
      rawDenominator: null,
      unit: "USD",
      unitScale: "unknown",
      value: null,
      denominator: null,
      valueRatio: null,
      valuePct: null,
      extractionMethod: "NONE",
      confidence: "NOT_DISCLOSED",
      evidence: {},
      calculation: null,
      warnings: [{ code: "INPUT_VALIDATION_ERROR", message: "region is required", severity: "error" }],
    });
  }
  // An explicit accession is the filing read: the figure, the evidence and the raw index all come from it, and a
  // miss fails closed with no fallback to the latest filing (2.15.0, R4-ACC-GEO).
  const pinned = accessionNumber != null && accessionNumber.trim() !== "";
  let pinnedFilingForm: string | null = null;
  if (pinned) {
    const resolved = await resolveSecFiling(ticker, filingType, accessionNumber);
    if (!resolved.ok && isAccessionNotFound(resolved.error)) return accessionNotFoundEnvelope(resolved.error);
    pinnedFilingForm = String(resolved.ok ? resolved.filing.filingType : (resolved.error.filingType ?? "")).toUpperCase();
  }
  const payload = parseObjectJson(await getFilingData(ticker, "geographic_revenue", region, filingType, period, "auto", pinned ? accessionNumber : null));
  const detail = String(detailLevel).toLowerCase();
  const needsIndex = detail === "raw";
  const idx = needsIndex ? parseObjectJson(await getSecFilingIndex(ticker, filingType, period, accessionNumber)) : {};
  const evidence = payload.evidence && typeof payload.evidence === "object" ? payload.evidence as Record<string, unknown> : {};
  const warnings = Array.isArray(payload.warnings) ? [...payload.warnings] : [];
  const payloadStatus = String(payload.status ?? payload.code ?? payload.confidence ?? "").trim();
  if (payload.value != null && payload.denominator == null && !hasWarningCode(warnings, "DENOMINATOR_NOT_FOUND")) {
    warnings.push({ code: "DENOMINATOR_NOT_FOUND", message: "Could not compute geographic revenue percentage due to missing denominator.", severity: "warning" });
  }
  const out: Record<string, unknown> = {
    ticker,
    factType: "geographic_revenue",
    region,
    period: payload.period ?? null,
    rawValue: payload.rawValue ?? null,
    rawDenominator: payload.rawDenominator ?? null,
    unit: payload.unit ?? "USD",
    unitScale: payload.unitScale ?? "unknown",
    unitScaleSource: payload.unitScaleSource ?? null,
    value: payload.value ?? null,
    denominator: payload.denominator ?? null,
    valueRatio: payload.denominator != null ? (payload.valueRatio ?? null) : null,
    valuePct: payload.denominator != null ? (payload.valuePct ?? null) : null,
    extractionMethod: payload.extractionMethod ?? "NONE",
    confidence: payload.confidence ?? "NOT_DISCLOSED",
    evidence: {
      filingType: idx.filingType ?? payload.filingType ?? filingType,
      filingDate: idx.filingDate ?? payload.filingDate ?? null,
      acceptedAt: idx.acceptedAt ?? null,
      accessionNumber: idx.accessionNumber ?? payload.accessionNumber ?? null,
      documentUrl: idx.documentUrl ?? payload.documentUrl ?? null,
      sectionHeading: evidence.sectionHeading ?? null,
      tableTitle: evidence.tableTitle ?? null,
      sourceTableId: evidence.sourceTableId ?? null,
      sourceRows: Array.isArray(evidence.sourceRows) ? evidence.sourceRows : [],
      sourceColumns: Array.isArray(evidence.sourceColumns) ? evidence.sourceColumns : [],
    },
    calculation: payload.calculation ?? null,
    scanCoverage: payload.scanCoverage ?? null,
    searchedTerms: Array.isArray(payload.searchedTerms) ? payload.searchedTerms : [],
    notDisclosedBasis: payload.notDisclosedBasis ?? null,
    status: payload.value != null ? "FOUND" : (payloadStatus || "NOT_DISCLOSED"),
    code: payload.code ?? (payloadStatus && payloadStatus !== "FOUND" && payloadStatus !== "NOT_DISCLOSED" ? payloadStatus : null),
    warnings,
  };
  if (filingType.toUpperCase() === "10-K" && String(out.confidence ?? "").toUpperCase() === "NOT_DISCLOSED") {
    const evidenceObj = out.evidence as Record<string, unknown>;
    const evidenceFilingType = String(evidenceObj?.filingType ?? "").toUpperCase();
    let possible20F = evidenceFilingType === "20-F";
    if (pinned) {
      // A pinned read stays pinned: the 20-F retry reads the same filing, and only when that filing is a 20-F (2.15.0).
      possible20F = pinnedFilingForm === "20-F";
    } else if (!possible20F && (evidenceFilingType === "" || evidenceFilingType === "10-K")) {
      possible20F = await mayBe20FFiler(ticker);
    }
    if (possible20F) {
      // Automatic 20-F fallback: retry extraction with 20-F filing type
      const fallbackPayload = parseObjectJson(await getFilingData(ticker, "geographic_revenue", region, "20-F", period, "auto", pinned ? accessionNumber : null));
      const fallbackIdx = parseObjectJson(await getSecFilingIndex(ticker, "20-F", period, accessionNumber));
      if (fallbackPayload.value != null) {
        // 20-F extraction succeeded — replace output with fallback data
        const fbEvidence = fallbackPayload.evidence && typeof fallbackPayload.evidence === "object"
          ? fallbackPayload.evidence as Record<string, unknown> : {};
        const fbWarnings = Array.isArray(fallbackPayload.warnings) ? [...fallbackPayload.warnings] : [];
        out.period = fallbackPayload.period ?? null;
        out.rawValue = fallbackPayload.rawValue ?? null;
        out.rawDenominator = fallbackPayload.rawDenominator ?? null;
        out.unit = fallbackPayload.unit ?? "USD";
        out.unitScale = fallbackPayload.unitScale ?? "unknown";
        out.unitScaleSource = fallbackPayload.unitScaleSource ?? null;
        out.value = fallbackPayload.value ?? null;
        out.denominator = fallbackPayload.denominator ?? null;
        out.valueRatio = fallbackPayload.denominator != null ? (fallbackPayload.valueRatio ?? null) : null;
        out.valuePct = fallbackPayload.denominator != null ? (fallbackPayload.valuePct ?? null) : null;
        out.extractionMethod = fallbackPayload.extractionMethod ?? "NONE";
        // An untagged fallback is never HIGH: it takes the primary path's default (2.23.0, BR-EXT-035).
        out.confidence = fallbackPayload.confidence ?? "NOT_DISCLOSED";
        out.evidence = {
          filingType: fallbackIdx.filingType ?? fallbackPayload.filingType ?? "20-F",
          filingDate: fallbackIdx.filingDate ?? fallbackPayload.filingDate ?? null,
          acceptedAt: fallbackIdx.acceptedAt ?? null,
          accessionNumber: fallbackIdx.accessionNumber ?? fallbackPayload.accessionNumber ?? null,
          documentUrl: fallbackIdx.documentUrl ?? fallbackPayload.documentUrl ?? null,
          sectionHeading: fbEvidence.sectionHeading ?? null,
          tableTitle: fbEvidence.tableTitle ?? null,
          sourceTableId: fbEvidence.sourceTableId ?? null,
          sourceRows: Array.isArray(fbEvidence.sourceRows) ? fbEvidence.sourceRows : [],
          sourceColumns: Array.isArray(fbEvidence.sourceColumns) ? fbEvidence.sourceColumns : [],
        };
        out.calculation = fallbackPayload.calculation ?? null;
        out.scanCoverage = fallbackPayload.scanCoverage ?? null;
        out.searchedTerms = Array.isArray(fallbackPayload.searchedTerms) ? fallbackPayload.searchedTerms : [];
        out.notDisclosedBasis = fallbackPayload.notDisclosedBasis ?? null;
        out.status = "FOUND";
        out.code = fallbackPayload.code ?? null;
        // Append advisory warning noting automatic 20-F selection
        fbWarnings.push({
          code: "AUTO_20F_FALLBACK",
          message: "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
          severity: "info",
        });
        out.warnings = fbWarnings;
      } else {
        // 20-F extraction also failed — add advisory warning
        if (!hasWarningCode(warnings, "POSSIBLE_20F_FILER")) {
          warnings.push({
            code: "POSSIBLE_20F_FILER",
            message: "POSSIBLE_20F_FILER: Ticker may file 20-F. Retry with filing_type='20-F' or use IR web search.",
            severity: "warning",
          });
        }
      }
    }
  }
  if (detail === "raw") out.rawContext = { filingIndex: idx };
  return JSON.stringify(out);
}

export interface SegmentTableRow {
  label: string;
  rawValue: string;
  value: number;
}

export interface SegmentTableResult {
  segments: SegmentTableRow[];
  total: SegmentTableRow;
  tableIndex: number;
  title: string | null;
  column: string | null;
  unitScale: "thousands" | "millions" | "billions" | "actual";
  unitMultiplier: number;
  unitScaleSource: UnitScaleSource;
}

const YEAR_CELL = /^(?:fy\s*)?(?:19|20)\d{2}$/i;

/**
 * A reportable-segment revenue table in a filing: segment rows that sum to
 * the table's total row (within 1%), for the latest period column. The
 * table must follow text that mentions segments and revenue or sales.
 * Returns the tables it considered (segmentTablesSeen) even when none of
 * them parses, so callers can tell "not parsed" from "not disclosed".
 * With `fiscalYear`, only that year's column is read; a table without it is skipped, and the year columns the
 * segment tables do have are returned as `yearsSeen` (2.25.1, F26: the latest column was returned for any year).
 */
export function extractSegmentTableFromHtml(html: string, fiscalYear: number | null = null): { result: SegmentTableResult | null; segmentTablesSeen: number; tablesScanned: number; yearsSeen: number[] } {
  const documentScale = lazyDocumentScale(html);
  let tableIndex = -1;
  let segmentTablesSeen = 0;
  const yearsSeen = new Set<number>();
  for (const m of html.matchAll(/<table[^>]*>([\s\S]*?)<\/table>/gi)) {
    tableIndex++;
    if (tableIndex > 800) break;
    const pos = m.index ?? 0;
    const before = stripHtmlTags(html.slice(Math.max(0, pos - 2_000), pos)).replace(/\s+/g, " ").trim();
    const lead = before.slice(-400).toLowerCase();
    if (!/segment/.test(lead) || !/revenue|net sales|sales/.test(lead)) continue;
    segmentTablesSeen++;
    const rows = parseFinancialTableRows(m[0]);
    const headerIdx = rows.findIndex((row) => row.some((cell) => YEAR_CELL.test(cell)));
    const header = headerIdx >= 0 ? rows[headerIdx] : null;
    let yearCol: number | null = null;
    if (header) {
      let latest = -1;
      header.forEach((cell, i) => {
        const year = YEAR_CELL.test(cell) ? Number(cell.replace(/\D/g, "")) : NaN;
        if (!Number.isFinite(year)) return;
        yearsSeen.add(year);
        if (fiscalYear != null ? year === fiscalYear && yearCol == null : year > latest) {
          latest = year;
          yearCol = i;
        }
      });
    }
    // A named year needs a column headed with it; a table without year columns cannot show which year it is.
    if (fiscalYear != null && (header == null || yearCol == null)) continue;
    // Without a year column, a "Total" column is the company figure: VRT's disaggregation table is by region
    // ("Americas | Asia Pacific | EMEA | Total"), and its first amount column is the Americas (2.5.20).
    const totalHeaderIdx = headerIdx >= 0 ? -1 : rows.findIndex((row) => row.slice(1).some((cell) => /^total$/i.test(cell.trim())) && !row.some((cell) => parseNumericCell(cell) != null));
    const columnHeader = header ?? (totalHeaderIdx >= 0 ? rows[totalHeaderIdx] : null);
    const valueCol: number | null = header ? yearCol : totalHeaderIdx >= 0 ? rows[totalHeaderIdx].findIndex((cell, i) => i > 0 && /^total$/i.test(cell.trim())) : null;
    const valueOf = (row: string[]): { raw: string; value: number } | null => {
      if (columnHeader && valueCol != null) {
        const offset = Math.max(0, row.length - columnHeader.length);
        const raw = row[valueCol + offset];
        const value = raw != null ? parseNumericCell(raw) : null;
        return raw != null && value != null ? { raw, value } : null;
      }
      for (const raw of row.slice(1)) {
        const value = parseNumericCell(raw);
        if (value != null) return { raw, value };
      }
      return null;
    };
    const segments: SegmentTableRow[] = [];
    let total: SegmentTableRow | null = null;
    for (const row of rows.slice(Math.max(headerIdx, totalHeaderIdx) + 1)) {
      const label = (row[0] ?? "").trim();
      const cell = row.length > 1 ? valueOf(row) : null;
      if (!label) {
        // An unlabeled numeric row after the segment rows is their total.
        if (cell && segments.length >= 2) total = { label: "Total", rawValue: cell.raw, value: cell.value };
        if (total) break;
        continue;
      }
      if (!cell) {
        // A label-only row after segment rows starts another block
        // (for example operating income); before them it is a heading.
        if (segments.length > 0) break;
        continue;
      }
      if (/^total\b/i.test(label) || /^(?:consolidated|net sales|total net sales|total revenues?)$/i.test(label)) {
        total = { label, rawValue: cell.raw, value: cell.value };
        break;
      }
      segments.push({ label, rawValue: cell.raw, value: cell.value });
    }
    if (!total || segments.length < 2 || total.value <= 0) continue;
    const sum = segments.reduce((acc, row) => acc + row.value, 0);
    if (Math.abs(sum - total.value) > total.value * 0.01) continue;
    const { multiplier: unitMultiplier, source: unitScaleSource } = detectUnitScale(m[0], html.slice(Math.max(0, pos - 3_000), pos), documentScale);
    return {
      result: {
        segments,
        total,
        tableIndex,
        title: before.slice(-300).split(/(?<=[.:])\s+/).pop() ?? null,
        column: columnHeader && valueCol != null ? columnHeader[valueCol] : null,
        unitScale: unitMultiplier === 1e9 ? "billions" : unitMultiplier === 1e6 ? "millions" : unitMultiplier === 1e3 ? "thousands" : "actual",
        unitMultiplier,
        unitScaleSource,
      },
      segmentTablesSeen,
      tablesScanned: tableIndex + 1,
      yearsSeen: [...yearsSeen].sort((a, b) => b - a),
    };
  }
  return { result: null, segmentTablesSeen, tablesScanned: tableIndex + 1, yearsSeen: [...yearsSeen].sort((a, b) => b - a) };
}

// accessionNumber is internal (not a tool param): a pinned query reads the pinned filing (2.24.0, F21).
export async function extractSegmentRevenue(ticker: string, filingType = "10-K", period = "latest", detailLevel = "compact", accessionNumber: string | null = null): Promise<string> {
  const pin = accessionNumber && accessionNumber.trim() ? accessionNumber.trim() : null;
  const payload = parseObjectJson(await getFilingData(ticker, "segment_revenue", null, filingType, period, "auto", pin));
  const segsRaw = Array.isArray(payload.allSegments) ? payload.allSegments : [];
  let segments = segsRaw
    .filter((s) => typeof s === "object" && s != null)
    .map((s) => {
      const row = s as Record<string, unknown>;
      return {
        label: row.segmentLabel ?? null,
        value: row.value ?? null,
        period: row.fiscalYear ? `FY${String(row.fiscalYear)}` : null,
        confidence: "HIGH",
        evidence: {
          filingDate: row.filingDate ?? null,
          accessionNumber: row.accessionNumber ?? null,
        },
      };
    });
  const warnings: Array<Record<string, unknown>> = [];
  // Automatic 20-F fallback for foreign private issuers
  if (segments.length === 0 && filingType.toUpperCase() === "10-K") {
    // A pinned read stays pinned: the 20-F retry reads the same filing, and only when that filing is a 20-F (2.24.0, F21).
    let possible20F: boolean;
    if (pin) {
      const resolved = await resolveSecFiling(ticker, filingType, pin);
      possible20F = resolved.ok && String(resolved.filing.filingType ?? "").toUpperCase() === "20-F";
    } else {
      possible20F = await mayBe20FFiler(ticker);
    }
    if (possible20F) {
      const fbPayload = parseObjectJson(await getFilingData(ticker, "segment_revenue", null, "20-F", period, "auto", pin));
      const fbSegsRaw = Array.isArray(fbPayload.allSegments) ? fbPayload.allSegments : [];
      const fbSegments = fbSegsRaw
        .filter((s) => typeof s === "object" && s != null)
        .map((s) => {
          const row = s as Record<string, unknown>;
          return {
            label: row.segmentLabel ?? null,
            value: row.value ?? null,
            period: row.fiscalYear ? `FY${String(row.fiscalYear)}` : null,
            confidence: "HIGH",
            evidence: {
              filingDate: row.filingDate ?? null,
              accessionNumber: row.accessionNumber ?? null,
            },
          };
        });
      if (fbSegments.length > 0) {
        segments = fbSegments;
        warnings.push({
          code: "AUTO_20F_FALLBACK",
          message: "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
          severity: "info",
        });
      } else {
        warnings.push({
          code: "POSSIBLE_20F_FILER",
          message: "POSSIBLE_20F_FILER: Ticker may file 20-F. Retry with filing_type='20-F' or use IR web search.",
          severity: "warning",
        });
      }
    }
  }
  const out: Record<string, unknown> = { ticker, factType: "segment_revenue", segments, status: segments.length > 0 ? "FOUND" : "NOT_DISCLOSED" };
  if (segments.length === 0 && payload.code === "INVALID_PERIOD") {
    // A fiscal year with a quarterly filing type: no table read can answer it either (2.25.1, F26).
    const invalid = (Array.isArray(payload.warnings) ? payload.warnings : []).find((w) => (w as Record<string, unknown> | null)?.code === "INVALID_PERIOD") as Record<string, unknown> | undefined;
    Object.assign(out, { status: "INVALID_PERIOD", code: "INVALID_PERIOD", message: invalid?.message ?? null });
  } else if (segments.length === 0) {
    // SEC companyconcept facts carry no segment dimensions, so most filers
    // need the filing's own segment table; absence is only reported with
    // the scan that supports it. The table read honours the period and a pinned filing (2.25.1, F26).
    Object.assign(out, await segmentRevenueFromFilingTable(ticker, filingType, warnings, period, pin));
  }
  if (warnings.length > 0) out.warnings = warnings;
  if (String(detailLevel).toLowerCase() === "raw") out.rawContext = payload;
  return JSON.stringify(out);
}

async function segmentRevenueFromFilingTable(
  ticker: string,
  filingType: string,
  warnings: Array<Record<string, unknown>>,
  period = "latest",
  pin: string | null = null,
): Promise<Record<string, unknown>> {
  const named = parseFilingPeriod(period);
  const fiscalYear = named?.kind === "fiscalYear" ? named.year : null;
  const resolved = await resolveSecFiling(ticker, filingType, pin);
  if (!resolved.ok) {
    return { status: String(resolved.error.status ?? resolved.error.code ?? "FILING_NOT_FOUND"), code: resolved.error.code ?? null, message: resolved.error.message ?? null };
  }
  const filing = resolved.filing;
  warnings.push(...filing.warnings);
  const filingMeta = {
    filingType: filing.filingType,
    filingDate: filing.filingDate,
    accessionNumber: filing.accessionNumber,
    documentUrl: filing.documentUrl,
  };
  const html = await edgarGetHtml(filing.documentUrl, SEC_DOCUMENT_READ_MAX_CHARS);
  if (!html) {
    return { ...filingMeta, status: "PROVIDER_ERROR", code: "PROVIDER_ERROR", message: "The filing document could not be read." };
  }
  const { result, segmentTablesSeen, tablesScanned, yearsSeen } = extractSegmentTableFromHtml(html, fiscalYear);
  const filingReadTruncated = html.length >= SEC_DOCUMENT_READ_MAX_CHARS;
  const searchedTerms = ["segment", "revenue", "net sales"];
  const scanCoverage = {
    sourceType: "sec_primary_html",
    documentUrl: filing.documentUrl,
    charsScanned: html.length,
    tablesScanned,
    segmentTablesSeen,
    filingReadTruncated,
    searchedTerms,
  };
  if (result) {
    const period = result.column ? `FY${result.column.replace(/\D/g, "")}` : null;
    const scaleAssumed = result.unitScaleSource === "ASSUMED_MILLIONS";
    if (scaleAssumed) warnings.push(UNIT_SCALE_ASSUMED_WARNING);
    return {
      ...filingMeta,
      status: "FOUND",
      extractionMethod: "PARSED_TABLE",
      decisionGrade: false,
      unitScale: result.unitScale,
      unitScaleSource: result.unitScaleSource,
      segments: result.segments.map((row) => ({
        label: row.label,
        value: row.value * result.unitMultiplier,
        rawValue: row.rawValue,
        period,
        confidence: scaleAssumed ? "LOW" : "MEDIUM",
        evidence: {
          documentUrl: filing.documentUrl,
          filingType: filing.filingType,
          accessionNumber: filing.accessionNumber,
          filingDate: filing.filingDate,
          sourceTableId: result.tableIndex,
          tableTitle: result.title,
          sourceRows: [[row.label, row.rawValue], [result.total.label, result.total.rawValue]],
          sourceColumns: result.column ? [result.column] : [],
        },
      })),
      // A parsed table's total is no surer than its rows (2.5.21).
      total: { label: result.total.label, value: result.total.value * result.unitMultiplier, rawValue: result.total.rawValue, confidence: scaleAssumed ? "LOW" : "MEDIUM" },
      evidence: {
        sourceType: "sec_filing_table",
        documentUrl: filing.documentUrl,
        tableIndex: result.tableIndex,
        tableTitle: result.title,
        column: result.column,
      },
      calculation: { check: "segments sum to the table total within 1%" },
      recommendedNextAction: "VERIFY_TABLE_PERIOD_AND_UNITS",
    };
  }
  if (fiscalYear != null && yearsSeen.length > 0 && !yearsSeen.includes(fiscalYear)) {
    // The filing's segment tables report other years: say so rather than return another year's rows (2.25.1, F26).
    return {
      ...filingMeta,
      status: "PERIOD_NOT_FOUND",
      decisionGrade: false,
      message: `The segment tables in this ${String(filing.filingType)} (filed ${String(filing.filingDate)}) have columns for ${yearsSeen.join(", ")}, not fiscal year ${fiscalYear}.`,
      fiscalYearsAvailable: yearsSeen,
      scanCoverage,
      searchedTerms,
    };
  }
  if (segmentTablesSeen > 0 || filingReadTruncated) {
    if (segmentTablesSeen > 0) {
      warnings.push({ code: "TABLE_NOT_PARSED", message: "The filing has tables introduced as segment revenue, but none parsed into segments that sum to a total.", severity: "warning" });
    }
    if (filingReadTruncated) {
      warnings.push({ code: "FILING_READ_TRUNCATED", message: "Only the first part of the filing was scanned; segment tables may appear later.", severity: "warning" });
    }
    return { ...filingMeta, status: "EXTRACTION_FAILED", code: "EXTRACTION_FAILED", decisionGrade: false, scanCoverage, searchedTerms, recommendedNextAction: "LIST_SEC_FILING_TABLES" };
  }
  return {
    ...filingMeta,
    status: "NOT_DISCLOSED",
    decisionGrade: false,
    scanCoverage,
    searchedTerms,
    notDisclosedBasis: "No XBRL segment facts and no table introduced as segment revenue in the full filing.",
  };
}

// accessionNumber is internal (not a tool param): a pinned query reads the pinned filing (2.24.0, F21).
export async function extractTotalRevenue(ticker: string, filingType = "10-K", period = "latest", accessionNumber: string | null = null): Promise<string> {
  const pin = accessionNumber && accessionNumber.trim() ? accessionNumber.trim() : null;
  const payload = parseObjectJson(await getFilingData(ticker, "total_revenue", null, filingType, period, "auto", pin));
  const value = payload.value ?? null;
  const status = value != null ? "FOUND" : normalizeStatus(payload);
  const sourceEvidence = xbrlSourceEvidence(payload);
  const decisionGrade = isDecisionGradeXbrlFact(payload, sourceEvidence, status);
  const filingEvidence = {
    filingType: payload.filingType ?? filingType,
    filingDate: payload.filingDate ?? null,
    accessionNumber: payload.accessionNumber ?? null,
    documentUrl: payload.documentUrl ?? null,
  };
  return JSON.stringify({
    ticker,
    factType: "total_revenue",
    value,
    period: payload.period ?? null,
    // No value, no unit: a missing fact has no currency (2.5.15).
    unit: value != null ? (payload.unit ?? "USD") : null,
    unitScale: payload.unitScale ?? null,
    // An untagged value is LOW, never HIGH (2.23.0, F18).
    confidence: payload.confidence ?? (value != null ? "LOW" : "NOT_DISCLOSED"),
    extractionMethod: payload.extractionMethod ?? "NONE",
    evidence: decisionGrade ? sourceEvidence : filingEvidence,
    sourceEvidence,
    xbrlContext: payload.xbrlContext ?? null,
    decisionGrade,
    status,
    // Why there is no value (NO_SEC_REGISTRANT, SEC_FACTS_IFRS_ONLY, ...) and the read's warnings (2.5.15).
    code: value != null ? null : (typeof payload.code === "string" ? payload.code : null),
    message: value != null ? null : (typeof payload.message === "string" ? payload.message : null),
    // A failed SEC read says so and which reads failed; retry rather than treat the fact as missing (2.5.17).
    ...(value == null && payload.retryable === true ? { retryable: true } : {}),
    ...(value == null && Array.isArray(payload.failedReads) ? { failedReads: payload.failedReads } : {}),
    warnings: Array.isArray(payload.warnings) ? payload.warnings : [],
  });
}

export async function extractRevenueExposure(
  ticker: string,
  exposureQuery: string,
  filingType = "10-K",
  period = "latest",
  detailLevel = "compact",
  // Internal only (not a tool param): a pinned caller reads the pinned filing (2.23.0, F3).
  accessionNumber: string | null = null,
): Promise<string> {
  const geo = parseObjectJson(await extractGeographicRevenue(ticker, exposureQuery, filingType, period, accessionNumber, detailLevel));
  const found = geo.value != null;
  // A value read from a product or category table ("iPhone" in "net sales by category") is segment revenue, not
  // geographic revenue (2.25.4 audit, F40). The filing is cached, so re-reading its chosen table costs no request.
  let exposureType = "geographic_revenue";
  const geoEvidence = geo.evidence && typeof geo.evidence === "object" ? geo.evidence as Record<string, unknown> : {};
  const parsedUrl = typeof geoEvidence.documentUrl === "string" ? geoEvidence.documentUrl : typeof geo.documentUrl === "string" ? geo.documentUrl : null;
  if (found && geo.extractionMethod === "PARSED_TABLE" && parsedUrl) {
    const html = await edgarGetHtml(parsedUrl, 5_000_000);
    const table = html ? extractGeoRevenueFromHtml(html, exposureQuery) : null;
    if (table && PRODUCT_BREAKDOWN_LEAD_RE.test(table.tableLead)) exposureType = "segment_revenue";
  }
  const status = found ? "FOUND_REVENUE_EXPOSURE" : normalizeStatus(geo);
  const warnings = Array.isArray(geo.warnings) ? geo.warnings : [];
  const matches = found
    ? [{
        exposureType,
        label: exposureQuery,
        value: geo.value ?? null,
        denominator: geo.denominator ?? null,
        valueRatio: geo.valueRatio ?? null,
        valuePct: geo.valuePct ?? null,
        period: geo.period ?? null,
        confidence: geo.confidence ?? "LOW",
        evidence: geo.evidence ?? {},
      }]
    : [];
  return JSON.stringify({
    ticker,
    query: exposureQuery,
    matches,
    status,
    code: found ? null : (geo.code ?? (status === "NOT_DISCLOSED" ? null : status)),
    requestedFilingType: geo.requestedFilingType ?? filingType,
    filingType: (geo.evidence && typeof geo.evidence === "object" ? (geo.evidence as Record<string, unknown>).filingType : null) ?? geo.filingType ?? filingType,
    filingDate: (geo.evidence && typeof geo.evidence === "object" ? (geo.evidence as Record<string, unknown>).filingDate : null) ?? geo.filingDate ?? null,
    accessionNumber: (geo.evidence && typeof geo.evidence === "object" ? (geo.evidence as Record<string, unknown>).accessionNumber : null) ?? geo.accessionNumber ?? null,
    documentUrl: (geo.evidence && typeof geo.evidence === "object" ? (geo.evidence as Record<string, unknown>).documentUrl : null) ?? geo.documentUrl ?? null,
    availableFilingTypes: geo.availableFilingTypes ?? [],
    suggestedFilingTypes: geo.suggestedFilingTypes ?? [],
    warnings,
  });
}

export async function extractRiskFactorMentions(
  ticker: string,
  terms: string[],
  filingType = "10-K",
  _period = "latest",
  detailLevel = "compact",
  // Internal only (not a tool param): a pinned caller reads the pinned filing (2.24.0, F21).
  accessionNumber: string | null = null,
): Promise<string> {
  const matches: Record<string, unknown>[] = [];
  let rawMatchCount = 0;
  let rejectedNoiseCount = 0;
  const warnings: Record<string, unknown>[] = [];
  for (const term of terms ?? []) {
    const search = parseObjectJson(await searchFilingText(ticker, [term], "Risk Factors", filingType, accessionNumber, 1200, false));
    rawMatchCount += Number(search.matchCount ?? 0);
    const list = Array.isArray(search.matches) ? search.matches : [];
    for (const row of list.slice(0, 3)) {
      if (!row || typeof row !== "object") continue;
      const item = row as Record<string, unknown>;
      const rowTerm = String(item.term ?? "");
      const sectionHeading = String(item.sectionHeading ?? "Risk Factors");
      if (rowTerm && rowTerm.toLowerCase() !== String(term).toLowerCase()) {
        rejectedNoiseCount += 1;
        continue;
      }
      // Reject a match only when its section is known and is not a risk section.
      if (item.sectionHeading && !sectionHeading.toLowerCase().includes("risk")) {
        rejectedNoiseCount += 1;
        continue;
      }
      const excerpt = readableExposureExcerpt(
        item.contextText ?? item.context ?? item.excerpt ?? "",
        [String(term)],
      );
      if (!excerpt) {
        rejectedNoiseCount += 1;
        continue;
      }
      matches.push({
        term,
        sectionHeading,
        excerpt,
        excerptAvailable: true,
        confidence: item.confidence ?? "MEDIUM",
        evidence: {
          filingDate: search.filingDate ?? null,
          accessionNumber: search.accessionNumber ?? null,
          documentUrl: search.documentUrl ?? null,
        },
      });
    }
  }
  if (rawMatchCount > 0 && matches.length === 0) {
    warnings.push({
      code: "EXCERPT_NOT_AVAILABLE",
      message: "Risk terms were detected, but no readable excerpt window could be produced.",
      severity: "warning",
    });
  }
  const out: Record<string, unknown> = {
    ticker,
    matches,
    status: matches.length > 0 ? "FOUND" : (rawMatchCount > 0 ? "FOUND_NO_EXCERPT" : "NOT_FOUND"),
    rawMatchCount,
    rejectedNoiseCount,
    warnings,
  };
  if (String(detailLevel).toLowerCase() === "raw") out.rawTerms = terms ?? [];
  return JSON.stringify(out);
}

/**
 * Revenue-concentration statements in filing text matches (extraction-rules.ts):
 * named customers, customers the filing leaves unnamed, and aggregates such as
 * "our top ten customers", for the latest year stated. A negated statement is
 * never a customer ("no customer accounted for more than 10%"), and a share
 * of receivables or anything other than revenue never matches.
 */
export function customerConcentrationFromMatches(
  list: unknown[],
  filingEvidence: Record<string, unknown>,
  period: string | null,
): { customers: Record<string, unknown>[]; aggregates: Record<string, unknown>[]; negation: Record<string, unknown> | null } {
  const rows = list.filter((row): row is Record<string, unknown> => row != null && typeof row === "object");
  const { findings, negation } = customerConcentration(rows);
  const fallbackPeriod = period ? (/^FY/i.test(period) ? period : `FY${period}`) : null;
  const evidenceFor = (f: ConcentrationFinding): Record<string, unknown> => ({ sectionHeading: f.sectionHeading, excerpt: compactExcerpt(f.sentence), ...filingEvidence });
  const customers = findings.filter((f) => f.kind === "customer").slice(0, 8).map((f) => ({
    label: f.name ?? "Unnamed customer",
    name: f.name,
    nameDisclosed: f.name != null,
    description: f.description,
    valuePct: f.valuePct,
    period: f.year != null ? `FY${f.year}` : fallbackPeriod,
    confidence: "MEDIUM",
    evidence: evidenceFor(f),
  }));
  const aggregates = findings.filter((f) => f.kind === "aggregate").slice(0, 4).map((f) => ({
    description: f.description,
    valuePct: f.valuePct,
    period: f.year != null ? `FY${f.year}` : fallbackPeriod,
    confidence: "MEDIUM",
    evidence: evidenceFor(f),
  }));
  return {
    customers,
    aggregates,
    negation: negation ? { sectionHeading: negation.sectionHeading, excerpt: compactExcerpt(negation.sentence), ...filingEvidence } : null,
  };
}

const CUSTOMER_TABLE_ROW_TERMS = ["Customer A", "Customer B", "Customer C", "Customer 1", "Distributor A", "Distributor B"];

export async function extractCustomerConcentration(
  ticker: string,
  filingType = "10-K",
  _period = "latest",
  detailLevel = "compact",
  // Internal only (not a tool param): a pinned caller reads the pinned filing (2.24.0, F21).
  accessionNumber: string | null = null,
): Promise<string> {
  const searchedTerms = ["major customer", "customers", "customer accounted", "accounted for", "of our revenue", "of total revenue", "of net sales", "percent of revenue"];
  const search = parseObjectJson(await searchFilingText(ticker, searchedTerms, null, filingType, accessionNumber, 1200, false));
  const list = Array.isArray(search.matches) ? search.matches : [];
  const filingEvidence = {
    filingDate: search.filingDate ?? null,
    accessionNumber: search.accessionNumber ?? null,
    documentUrl: search.documentUrl ?? null,
  };
  // The rows of a significant-customer table in the same filing, which the prose search does not reach (2.5.32,
  // F-024: MRVL's "Distributor A | 37% | 34% | 24%").
  const tableSearch = search.accessionNumber
    ? parseObjectJson(await searchFilingText(ticker, CUSTOMER_TABLE_ROW_TERMS, null, filingType, String(search.accessionNumber), 1200, false, null, { maxMatches: 12 }))
    : {};
  const tableRows = (Array.isArray(tableSearch.matches) ? tableSearch.matches : []).filter((m: unknown) => m != null && typeof m === "object" && (m as Record<string, unknown>).inTable === true);
  const { customers, aggregates, negation } = customerConcentrationFromMatches([...list, ...tableRows], filingEvidence, search.fiscalYear ? String(search.fiscalYear) : null);
  const matchCount = Number(search.matchCount ?? 0);
  const scanCoverage = { sourceType: "sec_primary_html", ...filingEvidence, matchCount, searchedTerms };
  const out: Record<string, unknown> = { ticker, customers, aggregates };
  if (customers.length > 0 || aggregates.length > 0) {
    out.status = "FOUND";
  } else if (negation) {
    Object.assign(out, {
      status: "NOT_DISCLOSED",
      notDisclosedBasis: "The filing states that no customer reached the disclosure threshold.",
      evidence: negation,
      scanCoverage,
    });
  } else if (matchCount > 0) {
    // Customer text exists but no percentage or explicit statement parsed.
    Object.assign(out, {
      status: "EXTRACTION_FAILED",
      code: "EXTRACTION_FAILED",
      scanCoverage,
      warnings: [{ code: "CUSTOMER_PERCENT_NOT_PARSED", message: "Customer disclosures were found, but no customer percentage or no-major-customer statement was parsed.", severity: "warning" }],
    });
  } else {
    Object.assign(out, { status: "NOT_FOUND", scanCoverage });
  }
  if (String(detailLevel).toLowerCase() === "raw") out.rawMatchCount = matchCount;
  return JSON.stringify(out);
}

type IxCacheEntry = { doc: IxDocument; truncated: boolean };
// Parsed inline XBRL of recently read filings; the HTML stays in the SEC document cache.
const ixbrlDocuments = new BoundedTtlCache<IxCacheEntry>(8);
const IXBRL_DOCUMENT_TTL_MS = 30 * 60 * 1000;

async function ixbrlSource(role: string, filing: ResolvedSecFiling): Promise<{ source: IxSource; truncated: boolean } | null> {
  let entry = ixbrlDocuments.get(filing.documentUrl) ?? null;
  if (entry) {
    countCacheEvent("sec", "memoryHits");
  } else {
    const res = await fetchSecDocument(filing.documentUrl).catch(() => null);
    if (!res || !res.ok) return null;
    const truncated = res.text.length > SEC_DOCUMENT_READ_MAX_CHARS;
    entry = { doc: parseIxbrl(truncated ? res.text.slice(0, SEC_DOCUMENT_READ_MAX_CHARS) : res.text), truncated };
    ixbrlDocuments.set(filing.documentUrl, entry, IXBRL_DOCUMENT_TTL_MS);
  }
  return {
    source: {
      role,
      filingType: filing.filingType,
      filingDate: filing.filingDate,
      accessionNumber: filing.accessionNumber,
      documentUrl: filing.documentUrl,
      doc: entry.doc,
    },
    truncated: entry.truncated,
  };
}

/** The newest periodic report, plus the latest annual report when the newest is a 10-Q. */
async function resolvePeriodicFilings(
  ticker: string,
  filingType: string,
  accessionNumber: string | null,
): Promise<{ ok: true; filings: { role: string; filing: ResolvedSecFiling }[] } | { ok: false; error: Record<string, unknown> }> {
  const requested = (filingType || "latest").toUpperCase();
  if (accessionNumber) {
    const one = await resolveSecFiling(ticker, requested === "LATEST" ? "10-K" : requested, accessionNumber);
    return one.ok ? { ok: true, filings: [{ role: "primary", filing: one.filing }] } : one;
  }
  if (requested !== "LATEST" && requested !== "10-Q") {
    const one = await resolveSecFiling(ticker, requested, null);
    return one.ok ? { ok: true, filings: [{ role: "primary", filing: one.filing }] } : one;
  }
  const [annual, quarterly] = await Promise.all([resolveSecFiling(ticker, "10-K", null), resolveSecFiling(ticker, "10-Q", null)]);
  if (requested === "10-Q" && !quarterly.ok) return quarterly;
  if (!annual.ok && !quarterly.ok) return annual;
  if (quarterly.ok && (!annual.ok || String(quarterly.filing.filingDate ?? "") > String(annual.filing.filingDate ?? "") || requested === "10-Q")) {
    const filings = [{ role: "primary", filing: quarterly.filing }];
    if (annual.ok) filings.push({ role: "latest_annual_fallback", filing: annual.filing });
    return { ok: true, filings };
  }
  return { ok: true, filings: [{ role: "primary", filing: (annual as { ok: true; filing: ResolvedSecFiling }).filing }] };
}

function textMatchesFrom(search: Record<string, unknown>): TextMatch[] {
  const matches = Array.isArray(search.matches) ? search.matches as Record<string, unknown>[] : [];
  return matches.map((m) => ({
    contextText: String(m.contextText ?? ""),
    sectionHeading: typeof m.sectionHeading === "string" && m.sectionHeading ? m.sectionHeading : null,
    documentUrl: (m.documentUrl ?? search.documentUrl ?? null) as string | null,
    filingDate: (m.filingDate ?? search.filingDate ?? null) as string | null,
    accessionNumber: (m.accessionNumber ?? search.accessionNumber ?? null) as string | null,
    inTable: m.inTable === true,
    tableTitle: typeof m.tableTitle === "string" ? m.tableTitle : null,
    rowLabel: typeof m.rowLabel === "string" ? m.rowLabel : null,
  }));
}

const AWARD_TABLE_SEARCH_TERMS = ["unvested", "nonvested"];
const ATM_SEARCH_TERMS = ["at-the-market", "at the market offering", "ATM program", "equity distribution agreement", "sales agreement"];
const FUNDING_SEARCH_TERMS = ["sufficient to fund", "sufficient to meet", "fully funded", "going concern", "cash runway", "next twelve months", "next 12 months", "at-the-market", "capital expenditures"];

async function filingTextMatches(ticker: string, filing: ResolvedSecFiling, terms: string[], maxMatches: number, contextChars: number): Promise<TextMatch[]> {
  const search = parseObjectJson(await searchFilingText(ticker, terms, null, filing.filingType, filing.accessionNumber, contextChars, false, null, { maxMatches }));
  return textMatchesFrom(search);
}

/** Share-claim text across the resolved filings; null when a filing's text could not be searched. */
async function shareClaimMatches(ticker: string, filings: { filing: ResolvedSecFiling }[]): Promise<TextMatch[] | null> {
  const out: TextMatch[] = [];
  for (const { filing } of filings) {
    const search = parseObjectJson(await searchFilingText(ticker, SHARE_CLAIM_SEARCH_TERMS, null, filing.filingType, filing.accessionNumber, 900, false, null, { maxMatches: 32 }));
    if (search.error || search.code || !Array.isArray(search.matches)) return null;
    out.push(...textMatchesFrom(search));
  }
  return out;
}

/**
 * Exercise, expiry and redemption text for warrant classes counted before the period end, searched by
 * the lifecycle phrases and each class's count; null when a filing's text could not be searched (2.5.11).
 */
async function warrantLifecycleMatches(ticker: string, filings: { filing: ResolvedSecFiling }[], counts: number[]): Promise<TextMatch[] | null> {
  return searchedMatches(ticker, filings, [...WARRANT_LIFECYCLE_SEARCH_TERMS, ...counts.filter((n) => Number.isInteger(n) && n >= 1000).map(groupedCount)]);
}

/** Text matches for the terms across the resolved filings; null when a filing's text could not be searched. */
async function searchedMatches(ticker: string, filings: { filing: ResolvedSecFiling }[], terms: string[]): Promise<TextMatch[] | null> {
  const out: TextMatch[] = [];
  for (const { filing } of filings) {
    const search = parseObjectJson(await searchFilingText(ticker, terms, null, filing.filingType, filing.accessionNumber, 900, false, null, { maxMatches: 32 }));
    if (search.error || search.code || !Array.isArray(search.matches)) return null;
    out.push(...textMatchesFrom(search));
  }
  return out;
}

function resolveFailure(error: Record<string, unknown>, ticker: string): string {
  if (isAccessionNotFound(error)) return accessionNotFoundEnvelope(error);
  return JSON.stringify({ ticker, ...error });
}

export async function extractDilutionBridge(
  ticker: string,
  price: number,
  asOfDate: string | null = null,
  currency = "USD",
  filingType = "latest",
  accessionNumber: string | null = null,
  includeAtm = true,
): Promise<string> {
  if (!Number.isFinite(price) || price <= 0) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "price is required and must be a positive number: the bridge is evaluated at the price you supply." });
  }
  const resolved = await resolvePeriodicFilings(ticker, filingType, accessionNumber);
  if (!resolved.ok) return resolveFailure(resolved.error, ticker);
  const loaded = await Promise.all(resolved.filings.map(({ role, filing }) => ixbrlSource(role, filing)));
  const sources = loaded.filter((l): l is { source: IxSource; truncated: boolean } => l != null).map((l) => l.source);
  if (sources.length === 0) {
    return JSON.stringify({ ticker, status: "FILING_TEXT_NOT_AVAILABLE", code: "FILING_TEXT_NOT_AVAILABLE", message: "The filing document could not be read from SEC." });
  }
  let atmMatches: TextMatch[] = [];
  if (includeAtm) {
    for (const { filing } of resolved.filings) {
      atmMatches = await filingTextMatches(ticker, filing, ATM_SEARCH_TERMS, 10, 800);
      if (atmMatches.length > 0) break;
    }
  }
  const claimMatches = await shareClaimMatches(ticker, resolved.filings);
  const input: DilutionInput = {
    ticker: ticker.toUpperCase(),
    price,
    priceCurrency: (currency || "USD").toUpperCase(),
    asOfDate: asOfDate && /^\d{4}-\d{2}-\d{2}$/.test(asOfDate) ? asOfDate : null,
    sources,
    atmMatches,
    claimMatches,
  };
  let out = dilutionBridge(input);
  // A warrant class counted before the period end: read the filing text for its exercise, expiry or redemption (2.5.11).
  const warrantComponent = (out.components as Record<string, unknown>[]).find((c) => c.component === "warrants");
  const earlyCounts = ((warrantComponent?.classes ?? []) as Record<string, unknown>[]).filter((c) => c.countBeforePeriodEnd === true).map((c) => Number(c.outstanding));
  if (earlyCounts.length > 0) input.warrantLifecycleMatches = await warrantLifecycleMatches(ticker, resolved.filings, earlyCounts);
  // Convertible notes: read the filing text for a stated cash settlement of principal (2.5.11).
  if ((out.components as Record<string, unknown>[]).some((c) => c.component === "convertible_debt")) {
    input.convertibleSettlementMatches = await searchedMatches(ticker, resolved.filings, CONVERTIBLE_SETTLEMENT_SEARCH_TERMS);
    // And for capped call terms (2.5.12).
    input.cappedCallMatches = await searchedMatches(ticker, resolved.filings, CAPPED_CALL_SEARCH_TERMS);
  }
  if ("warrantLifecycleMatches" in input || "convertibleSettlementMatches" in input) out = dilutionBridge(input);
  // No unvested award count is tagged: read it from the filing's award table.
  if ((out.notDisclosed as string[]).includes("unvested_share_awards")) {
    for (const { filing } of resolved.filings) {
      const awardTableMatches = (await filingTextMatches(ticker, filing, AWARD_TABLE_SEARCH_TERMS, 30, 400)).filter((m) => m.inTable);
      if (awardTableMatches.length === 0) continue;
      const retried = dilutionBridge({ ...input, awardTableMatches });
      if (!(retried.notDisclosed as string[]).includes("unvested_share_awards")) {
        out = retried;
        break;
      }
    }
  }
  if (loaded.some((l) => l?.truncated)) {
    (out.warnings as Record<string, unknown>[]).push({ code: "FILING_READ_TRUNCATED", message: "A filing exceeded the read limit; facts past that point were not parsed.", severity: "warning" });
  }
  return JSON.stringify(out);
}

export async function extractCapitalStructure(
  ticker: string,
  filingType = "latest",
  accessionNumber: string | null = null,
  includeFundingStatements = true,
): Promise<string> {
  const resolved = await resolvePeriodicFilings(ticker, filingType, accessionNumber);
  if (!resolved.ok) return resolveFailure(resolved.error, ticker);
  const primary = resolved.filings[0];
  const loaded = await ixbrlSource("primary", primary.filing);
  if (!loaded) {
    return JSON.stringify({ ticker, status: "FILING_TEXT_NOT_AVAILABLE", code: "FILING_TEXT_NOT_AVAILABLE", message: "The filing document could not be read from SEC." });
  }
  const fundingMatches = includeFundingStatements ? await filingTextMatches(ticker, primary.filing, FUNDING_SEARCH_TERMS, 20, 700) : [];
  let out = capitalStructure({ ticker: ticker.toUpperCase(), source: loaded.source, fundingMatches });
  // An outstanding instrument with no tagged maturity: read the filing's own maturity sentences (2.5.24).
  if (wantsMaturityText(out)) {
    const maturityMatches = await filingTextMatches(ticker, primary.filing, MATURITY_SEARCH_TERMS, 12, 400);
    out = capitalStructure({ ticker: ticker.toUpperCase(), source: loaded.source, fundingMatches, maturityMatches });
  }
  if (loaded.truncated) {
    (out.warnings as Record<string, unknown>[]).push({ code: "FILING_READ_TRUNCATED", message: "The filing exceeded the read limit; facts past that point were not parsed.", severity: "warning" });
  }
  return JSON.stringify(out);
}

/**
 * Share counts under caller scenarios (2.5.2): the dilution bridge's
 * instrument inventory, re-evaluated at each scenario's price and treatments.
 */
export async function getShareCountScenarios(
  ticker: string,
  scenarios: unknown,
  filingType = "latest",
  accessionNumber: string | null = null,
  currency = "USD",
): Promise<string> {
  const parsed = parseShareScenarios(scenarios);
  if ("error" in parsed) return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: parsed.error });
  const bridge = parseObjectJson(await extractDilutionBridge(ticker, parsed.scenarios[0].price, null, currency, filingType, accessionNumber, true));
  if (bridge.error === true || bridge.basis !== "MECHANICAL_COMPANY_DISCLOSED") return JSON.stringify(bridge);
  return JSON.stringify(shareCountScenarios(ticker, bridge, parsed.scenarios));
}

const SCHEDULE_SEARCH_TERMS = [
  "capital expenditures", "purchase commitments", "purchase obligations", "non-cancelable", "committed to",
  "contractual obligations", "grant agreement", "awarded", "milestone payments", "CHIPS", "incentive agreement",
  "expect to invest", "sufficient to fund", "at-the-market",
];

/** Funding and capex schedule (2.5.2): tagged contractual schedules plus classified funding and capex statements. */
export async function extractFundingCapexSchedule(ticker: string, filingType = "latest", accessionNumber: string | null = null): Promise<string> {
  const resolved = await resolvePeriodicFilings(ticker, filingType, accessionNumber);
  if (!resolved.ok) return resolveFailure(resolved.error, ticker);
  const primary = resolved.filings[0];
  const loaded = await ixbrlSource("primary", primary.filing);
  if (!loaded) {
    return JSON.stringify({ ticker, status: "FILING_TEXT_NOT_AVAILABLE", code: "FILING_TEXT_NOT_AVAILABLE", message: "The filing document could not be read from SEC." });
  }
  const matches = await filingTextMatches(ticker, primary.filing, SCHEDULE_SEARCH_TERMS, 30, 700);
  const capital = capitalStructure({ ticker: ticker.toUpperCase(), source: loaded.source, fundingMatches: [] });
  const atm = atmEvidence(matches);
  const remaining = atm.find((e) => e.kind === "remaining_capacity") ?? null;
  const doc = loaded.source.doc;
  const out = fundingCapexSchedule({
    ticker,
    periodEnd: doc.documentPeriodEnd,
    source: (capital.source as Record<string, unknown> | undefined) ?? null,
    facts: doc.facts.map((f) => ({ local: f.local, value: f.value, periodEnd: f.periodEnd, dims: f.dims, unit: f.unit })),
    statements: matches.map((m) => ({ contextText: m.contextText, sectionHeading: m.sectionHeading, documentUrl: m.documentUrl, filingDate: m.filingDate, accessionNumber: m.accessionNumber })),
    balances: (capital.balances as Record<string, unknown> | undefined) ?? null,
    atmRemainingUsd: remaining ? (remaining.amountUsd as number) : null,
    atmEvidence: remaining,
  });
  if (loaded.truncated) {
    (out.warnings as Record<string, unknown>[]).push({ code: "FILING_READ_TRUNCATED", message: "The filing exceeded the read limit; facts past that point were not parsed.", severity: "warning" });
  }
  return JSON.stringify(out);
}

const DRIVER_SEARCH_TERMS = [
  "capacity", "backlog", "bookings", "deliveries", "shipments", "customers", "subscribers", "launched", "deployed",
  "production", "utilization", "average selling price", "employees", "remaining performance obligations",
];

/** Operating-driver ledger (2.5.3): XBRL driver series, the filer's own tagged KPIs, and operating statements with figures. */
export async function extractOperatingDriverLedger(ticker: string, filingType = "latest", accessionNumber: string | null = null): Promise<string> {
  const resolved = await resolvePeriodicFilings(ticker, filingType, accessionNumber);
  if (!resolved.ok) return resolveFailure(resolved.error, ticker);
  const primary = resolved.filings[0].filing;
  const [loaded, matches, companyfacts, release] = await Promise.all([
    ixbrlSource("primary", primary).catch(() => null),
    filingTextMatches(ticker, primary, DRIVER_SEARCH_TERMS, 30, 700).catch(() => [] as TextMatch[]),
    edgarGetJson(secCompanyFactsUrl(primary.cikPadded)).catch(() => null),
    getSubmissionsForTicker(ticker).then(async ({ submissions }): Promise<Record<string, unknown>> => {
      // The newest results-of-operations 8-K, as in get_guidance_history.
      const pick = submissions ? earningsReleasePicks(submissions, primary.cikInt, 1)[0] : undefined;
      if (!pick) return { status: "RELEASE_NOT_RESOLVED", url: null, filingDate: null, accessionNumber: null, text: null };
      const read = await readReleaseText(primary.cikInt, pick.accessionNumber, pick.primaryUrl);
      return { ...read, filingDate: pick.filingDate, accessionNumber: pick.accessionNumber };
    }).catch((): Record<string, unknown> => ({ status: "RELEASE_NOT_RESOLVED", url: null, filingDate: null, accessionNumber: null, text: null })),
  ]);
  const statements = [
    ...(typeof release.text === "string" ? [{
      contextText: release.text, sectionHeading: null, documentUrl: (release.url as string | null) ?? null,
      filingDate: (release.filingDate as string | null) ?? null, accessionNumber: (release.accessionNumber as string | null) ?? null, source: "EARNINGS_RELEASE",
    }] : []),
    ...matches.map((m) => ({ contextText: m.contextText, sectionHeading: m.sectionHeading, documentUrl: m.documentUrl, filingDate: m.filingDate, accessionNumber: m.accessionNumber, source: "PERIODIC_FILING" })),
  ];
  const out = operatingDriverLedger({
    ticker,
    companyfacts,
    inlineFacts: loaded ? loaded.source.doc.facts.map((f) => ({ name: f.name, unit: f.unit, value: f.value, periodStart: f.periodStart, periodEnd: f.periodEnd, dims: f.dims })) : null,
    filing: { filingType: primary.filingType, filingDate: primary.filingDate, accessionNumber: primary.accessionNumber, documentUrl: primary.documentUrl,
      status: loaded ? "READ" : "NOT_READ", textMatches: matches.length },
    release: { filingDate: release.filingDate ?? null, accessionNumber: release.accessionNumber ?? null, url: release.url ?? null, status: release.status },
    statements,
  });
  const warnings: Record<string, unknown>[] = [];
  if (!loaded) warnings.push({ code: "FILING_TEXT_NOT_AVAILABLE", message: "The periodic filing could not be read from SEC, so company-specific series were not read; retry.", severity: "warning" });
  else if (loaded.truncated) warnings.push({ code: "FILING_READ_TRUNCATED", message: "The filing exceeded the read limit; facts past that point were not parsed.", severity: "warning" });
  if (!companyfacts) warnings.push({ code: "COMPANYFACTS_NOT_AVAILABLE", message: "SEC companyfacts could not be read, so standard driver series were not read; retry.", severity: "warning" });
  if (release.status === "NOT_READ") warnings.push({ code: "RELEASE_TEXT_NOT_AVAILABLE", message: `The earnings release ${String(release.url)} could not be read from SEC; retry.`, severity: "warning" });
  else if (release.status === "RELEASE_NOT_RESOLVED") warnings.push({ code: "RELEASE_NOT_RESOLVED", message: "No SEC earnings release was resolved, so release statements were not read.", severity: "warning" });
  out.status = warnings.some((w) => w.code !== "FILING_READ_TRUNCATED" && w.code !== "RELEASE_NOT_RESOLVED") ? "PARTIAL" : "OK";
  out.retryable = !loaded || !companyfacts || release.status === "NOT_READ";
  out.warnings = warnings;
  return JSON.stringify(out);
}

export async function extractChinaExposure(
  ticker: string,
  filingType = "10-K",
  period = "latest",
  accessionNumber: string | null = null,
  detailLevel = "compact",
): Promise<string> {
  let idx = parseObjectJson(await getSecFilingIndex(ticker, filingType, period, accessionNumber));
  // The revenue read is pinned too, so its evidence names the pinned filing (2.23.0, F3).
  const revenue = parseObjectJson(await extractRevenueExposure(ticker, CHINA_PRESET.region, filingType, period, "compact", accessionNumber));
  if (idx.ok === false && idx.error && typeof idx.error === "object" && isAccessionNotFound(idx.error as Record<string, unknown>)) {
    // Not one of the actions 2.13.0 fails closed on an accession miss: it reads as FILING_NOT_FOUND_TRY_OTHER_TYPE,
    // as before (the index's payload for a filing not found), rather than falling through to the latest filing.
    const notFound = await legacyAccessionMissPayload(ticker, filingType);
    idx = { ok: false, error: notFound, ...notFound };
  }
  const idxError = idx.error && typeof idx.error === "object" ? idx.error as Record<string, unknown> : {};
  const idxStatus = String(idx.status ?? idx.code ?? idxError.code ?? "").toUpperCase();
  const revenueStatus = String(revenue.status ?? revenue.code ?? "").toUpperCase();
  if (idxStatus === "FILING_NOT_FOUND_TRY_OTHER_TYPE" || revenueStatus === "FILING_NOT_FOUND_TRY_OTHER_TYPE") {
    const source = idxStatus === "FILING_NOT_FOUND_TRY_OTHER_TYPE" ? idx : revenue;
    return JSON.stringify({
      ticker,
      exposureType: "china_exposure",
      filingType,
      filingDate: null,
      accessionNumber: null,
      documentUrl: null,
      revenueExposure: { status: "FILING_NOT_FOUND_TRY_OTHER_TYPE", value: null, denominator: null, valueRatio: null, valuePct: null, confidence: "FILING_NOT_FOUND_TRY_OTHER_TYPE", evidence: [] },
      manufacturingExposure: { status: "NOT_FOUND", confidence: "LOW", evidence: [] },
      entityExposure: { status: "NOT_FOUND", entities: [], confidence: "LOW", evidence: [] },
      bankExposure: { status: "NOT_FOUND", entities: [], confidence: "LOW", evidence: [] },
      riskFactorExposure: { status: "NOT_FOUND", confidence: "LOW", evidence: [] },
      overallStatus: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
      code: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
      requestedFilingType: source.requestedFilingType ?? filingType,
      availableFilingTypes: source.availableFilingTypes ?? [],
      suggestedFilingTypes: source.suggestedFilingTypes ?? [],
      warnings: source.warnings ?? [],
    });
  }
  const index = idx.index && typeof idx.index === "object" ? idx.index as Record<string, unknown> : {};
  const sections = Array.isArray(index.sections) ? index.sections : [];
  const tables = Array.isArray(index.tables) ? index.tables : [];

  const collect = (
    terms: string[],
    evidenceKind: "entity" | "bank" | "manufacturing",
  ): { evidence: Record<string, unknown>[]; rejectedNoiseCount: number } => {
    const found: Record<string, unknown>[] = [];
    let rejectedNoiseCount = 0;
    const scopedExcerpt = (value: unknown, term: string): string => {
      if (evidenceKind === "entity") return contextualEntityExcerpt(value, term);
      const excerpt = readableExposureExcerpt(value, [term], 240, 3);
      if (evidenceKind === "bank" && excerpt && !EXPOSURE_RELATIONSHIP_RE.test(excerpt)) return "";
      return excerpt;
    };
    for (const sec of sections) {
      if (!sec || typeof sec !== "object") continue;
      const s = sec as Record<string, unknown>;
      const heading = String(s.heading ?? "");
      const low = heading.toLowerCase();
      for (const term of terms) {
        if (low.includes(term.toLowerCase())) {
          const excerpt = scopedExcerpt(heading, term);
          if (!excerpt) {
            rejectedNoiseCount += 1;
            continue;
          }
          found.push({ source: "section", term, sectionHeading: heading, excerpt, excerptAvailable: excerpt.length > 0 });
        }
      }
    }
    for (const tbl of tables) {
      if (!tbl || typeof tbl !== "object") continue;
      const t = tbl as Record<string, unknown>;
      const rowLabels = Array.isArray(t.rowLabels) ? t.rowLabels.map((v) => String(v)) : [];
      const haystackRaw = `${String(t.title ?? "")} ${rowLabels.join(" ")}`;
      const haystack = haystackRaw.toLowerCase();
      for (const term of terms) {
        if (haystack.includes(term.toLowerCase())) {
          const excerpt = scopedExcerpt(haystackRaw, term);
          if (!excerpt) {
            rejectedNoiseCount += 1;
            continue;
          }
          found.push({ source: "table", term, tableTitle: t.title ?? null, sourceTableId: t.tableId ?? null, sectionId: t.sectionId ?? null, excerpt, excerptAvailable: excerpt.length > 0 });
        }
      }
    }
    return { evidence: found, rejectedNoiseCount };
  };

  const entityTerms = [...CHINA_PRESET.entityTerms];
  const bankTerms = [...CHINA_PRESET.bankTerms];
  // "manufactured by outsourcing partners located primarily in China mainland" (AAPL) names manufacturing without the
  // word "manufacturing" (2.25.4 audit, F39). New phrases go after the old ones, so an old term still reports first.
  const manuTerms = [...CHINA_PRESET.manufacturingTerms];
  const riskMentions = parseObjectJson(await extractRiskFactorMentions(ticker, [...CHINA_PRESET.riskTerms], filingType, period, "compact", accessionNumber));

  const entityCollected = collect(entityTerms, "entity");
  const bankCollected = collect(bankTerms, "bank");
  const manuCollected = collect(manuTerms, "manufacturing");
  const entityEvidence = entityCollected.evidence;
  const bankEvidence = bankCollected.evidence;
  const manuEvidence = manuCollected.evidence;
  if (manuEvidence.length === 0) {
    // Headings and table labels rarely name manufacturing locations; the
    // filing text does (as extract_exposure's operational scan reads it).
    const chinaText = parseObjectJson(await searchFilingText(ticker, [CHINA_PRESET.region], null, filingType, accessionNumber, 800, false));
    for (const m of Array.isArray(chinaText.matches) ? chinaText.matches as Record<string, unknown>[] : []) {
      const excerpt = readableExposureExcerpt(m?.contextText ?? "", [CHINA_PRESET.region], 240, 3);
      const term = excerpt ? manuTerms.find((t) => excerpt.toLowerCase().includes(t)) : undefined;
      if (!excerpt || !term) continue;
      manuEvidence.push({ source: "text", term, sectionHeading: m.sectionHeading ?? null, excerpt, excerptAvailable: true });
      if (manuEvidence.length >= 5) break;
    }
  }
  const riskEvidence: Record<string, unknown>[] = [];
  const rawRiskEvidence = Array.isArray(riskMentions.matches) ? riskMentions.matches : [];
  for (const ev of rawRiskEvidence) {
    if (ev && typeof ev === "object") {
      const item = ev as Record<string, unknown>;
      const excerpt = readableExposureExcerpt(
        item.excerpt ?? item.context ?? "",
        [String(item.term ?? CHINA_PRESET.region)],
      );
      if (excerpt) {
        riskEvidence.push({ ...item, excerpt, excerptAvailable: true });
      }
    }
  }
  const nonRevenueFound = entityEvidence.length > 0 || bankEvidence.length > 0 || manuEvidence.length > 0 || riskEvidence.length > 0;
  const revStatus = String(revenue.status ?? "NOT_FOUND");
  const revFound = revStatus === "FOUND_REVENUE_EXPOSURE";
  const explicitRevenueStatus = explicitRevenueLimitationStatus(revStatus, Array.isArray(revenue.warnings) ? revenue.warnings : []);

  const revenueMatches = Array.isArray(revenue.matches) ? revenue.matches : [];
  const firstRevenue = revenueMatches.length > 0 && typeof revenueMatches[0] === "object" ? revenueMatches[0] as Record<string, unknown> : {};

  // A revenue read that failed (SEC_READ_FAILED, SEC_LOOKUP_UNAVAILABLE) is not a missing exposure (2.5.18).
  const revenueReadFailed = revStatus === "PROVIDER_ERROR";
  const overallStatus = revFound
    ? "FOUND_REVENUE_EXPOSURE"
    : nonRevenueFound
      ? "FOUND_NON_REVENUE_EXPOSURE"
      : explicitRevenueStatus
        ? explicitRevenueStatus
      : revenueReadFailed
        ? "PROVIDER_ERROR"
      : revStatus === "NOT_DISCLOSED"
        ? "NOT_DISCLOSED"
        : revStatus === "CONFLICTING"
          ? "CONFLICTING"
          : "NOT_FOUND";

  const out: Record<string, unknown> = {
    ticker,
    exposureType: "china_exposure",
    filingType: idx.filingType ?? filingType,
    filingDate: idx.filingDate ?? null,
    accessionNumber: idx.accessionNumber ?? null,
    documentUrl: idx.documentUrl ?? null,
    revenueExposure: {
      status: revFound ? "FOUND" : (revStatus as string),
      value: firstRevenue.value ?? null,
      denominator: firstRevenue.denominator ?? null,
      valueRatio: firstRevenue.valueRatio ?? null,
      valuePct: firstRevenue.valuePct ?? null,
      confidence: revFound ? "HIGH" : (explicitRevenueStatus ? explicitRevenueStatus : (revStatus === "NOT_DISCLOSED" ? "NOT_DISCLOSED" : "LOW")),
      evidence: firstRevenue.evidence ?? [],
    },
    manufacturingExposure: { status: manuEvidence.length > 0 ? "FOUND" : "NOT_FOUND", confidence: "MEDIUM", evidence: manuEvidence, rejectedNoiseCount: manuCollected.rejectedNoiseCount },
    entityExposure: { status: entityEvidence.length > 0 ? "FOUND" : "NOT_FOUND", entities: entityEvidence.length > 0 ? entityTerms : [], confidence: "MEDIUM", evidence: entityEvidence, rejectedNoiseCount: entityCollected.rejectedNoiseCount },
    bankExposure: { status: bankEvidence.length > 0 ? "FOUND" : "NOT_FOUND", entities: bankEvidence.length > 0 ? bankTerms : [], confidence: "MEDIUM", evidence: bankEvidence, rejectedNoiseCount: bankCollected.rejectedNoiseCount },
    riskFactorExposure: { status: riskEvidence.length > 0 ? "FOUND" : "NOT_FOUND", confidence: "MEDIUM", evidence: riskEvidence },
    overallStatus,
    code: explicitRevenueStatus ?? (revenueReadFailed ? (typeof revenue.code === "string" ? revenue.code : "PROVIDER_ERROR") : null),
    warnings: Array.isArray(revenue.warnings) ? revenue.warnings : [],
  };
  if (String(detailLevel).toLowerCase() === "raw") out.rawContext = { filingIndex: idx };
  return JSON.stringify(out);
}

// XBRL synonym lookup for extractExposure
const EXPOSURE_SYNONYMS: Record<string, string[]> = {
  china:          ["china", "greaterchinese", "greaterchina", "prc", "hongkong", "taiwan", "chinesesimplified"],
  "greater china":["greaterchina", "greaterchinese", "china", "hongkong", "taiwan"],
  europe:         ["europe", "emea", "europeanunion"],
  japan:          ["japan"],
  americas:       ["americas", "northamerica", "unitedstates", "us"],
  russia:         ["russia", "russianfederation"],
  india:          ["india"],
  "rest of asia": ["restofasia", "asiapacific", "asiaxjapan"],
};

// Named entities known to be associated with China manufacturing / supply chain (the China preset, exposure-presets.json)
const CHINA_NAMED_ENTITIES: string[] = [...CHINA_PRESET.namedEntities];

// Operational terms for geographic exposure scanning
// The F39 manufacturing phrases apply to every region, not only the China preset (2.26.5, F62).
const OPERATIONAL_TERMS = ["manufacturing", "assembly", "supply chain", "factory", "production facility", "manufactured", "outsourcing partner", "contract manufacturer"];

/** Whether any table in a get_sec_filing_index payload names the region (title, headers or row labels); null when
 * the index has no tables to read. */
export function indexTablesNameRegion(indexPayload: Record<string, unknown>, region: string): boolean | null {
  const index = indexPayload.index && typeof indexPayload.index === "object" ? indexPayload.index as Record<string, unknown> : null;
  const tables = index && Array.isArray(index.tables) ? index.tables as Record<string, unknown>[] : null;
  if (!tables || tables.length === 0) return null;
  const words = (v: unknown): string => (Array.isArray(v) ? v.map(String).join(" ") : "");
  return tables.some((t) => t && typeof t === "object"
    && textContainsGeoRegion(`${String(t.title ?? "")} ${words(t.headers)} ${words(t.rowLabels)}`, region));
}

export async function extractExposure(
  ticker: string,
  topic: string,
  filingType = "10-K",
  period = "latest",
  includeRiskFactors = true,
): Promise<string> {
  if (!ticker || !ticker.trim()) {
    return JSON.stringify({ ticker, topic, overallStatus: "NOT_FOUND", warnings: [{ code: "INPUT_VALIDATION_ERROR", message: "ticker is required" }] });
  }
  if (!topic || !topic.trim()) {
    return JSON.stringify({ ticker, topic, overallStatus: "NOT_FOUND", warnings: [{ code: "INPUT_VALIDATION_ERROR", message: "topic is required" }] });
  }

  const topicLower = topic.trim().toLowerCase();
  const warnings: Record<string, unknown>[] = [];

  // Resolve filing metadata via index
  const idx = parseObjectJson(await getSecFilingIndex(ticker, filingType, period, null));

  const filingDate: string | null = typeof idx.filingDate === "string" ? idx.filingDate : null;
  const accessionNumber: string | null = typeof idx.accessionNumber === "string" ? idx.accessionNumber : null;
  const documentUrl: string | null = typeof idx.documentUrl === "string" ? idx.documentUrl : null;

  // Determine whether the topic maps to a known exposure family.
  const synonymEntry = Object.entries(EXPOSURE_SYNONYMS).find(([key]) => key === topicLower);

  // --- Fan-out: revenue extraction, operational scan, risk factor scan (all parallel) ---

  // 1. Revenue extraction via the same public revenue exposure path used by extract_china_exposure.
  const revenuePromise = extractRevenueExposure(ticker, topic, filingType, period, "compact");

  // 2. Operational/entity scan via searchFilingText
  const operationalPromise = searchFilingText(ticker, [topicLower], null, filingType, null, 600, false);

  // 3. Entity scan — only for China topic
  const isChina = topicLower === "china" || (synonymEntry?.[0] === "china") || ((synonymEntry?.[1] ?? []).includes("china"));
  const entityPromise = isChina
    ? searchFilingText(ticker, CHINA_NAMED_ENTITIES.slice(0, 3), null, filingType, null, 400, false)
    : Promise.resolve(null as string | null);

  // 4. Risk factor scan
  const riskPromise = includeRiskFactors
    ? extractRiskFactorMentions(ticker, [topicLower], filingType, period, "compact")
    : Promise.resolve(null as string | null);

  const [revenueResult, operationalResult, entityResult, riskResult] = await Promise.allSettled([
    revenuePromise,
    operationalPromise,
    entityPromise,
    riskPromise,
  ]);

  // --- Process revenue ---
  const revenueRaw = revenueResult.status === "fulfilled" ? parseObjectJson(revenueResult.value) : {};
  let revenueWarnings: unknown[] = Array.isArray(revenueRaw.warnings) ? revenueRaw.warnings : [];
  const revenueMatches: Record<string, unknown>[] = Array.isArray(revenueRaw.matches)
    ? (revenueRaw.matches as Record<string, unknown>[]).filter((m) => m && typeof m === "object")
    : [];
  const firstRevenue = revenueMatches.length > 0 ? revenueMatches[0] : {};
  const geoValue: number | null = typeof firstRevenue.value === "number" ? firstRevenue.value : null;
  const geoDenominator: number | null = typeof firstRevenue.denominator === "number" ? firstRevenue.denominator : null;
  const geoConf = String(firstRevenue.confidence ?? "LOW").toUpperCase();
  const geoEvidenceRaw = firstRevenue.evidence;
  const geoEvidence = Array.isArray(geoEvidenceRaw)
    ? (geoEvidenceRaw.find((ev) => ev && typeof ev === "object") as Record<string, unknown> | undefined) ?? {}
    : (geoEvidenceRaw && typeof geoEvidenceRaw === "object" ? geoEvidenceRaw as Record<string, unknown> : {});
  const revenueStatus = String(revenueRaw.status ?? revenueRaw.code ?? "").toUpperCase();
  let explicitRevenueStatus = explicitRevenueLimitationStatus(revenueStatus, revenueWarnings);
  // A failed table parse needs a table to have failed on: when the region is only in the filing's text and no
  // indexed table names it, the revenue exposure is NOT_FOUND, not a parser failure (2.25.4 audit, F35: AAPL India).
  if ((explicitRevenueStatus === "EXTRACTION_FAILED" || explicitRevenueStatus === "TABLE_NOT_PARSED")
      && hasWarningCode(revenueWarnings, "TABLE_NOT_PARSED") && !hasWarningCode(revenueWarnings, "FILING_READ_TRUNCATED")
      && indexTablesNameRegion(idx, topicLower) === false) {
    explicitRevenueStatus = null;
    // The parsed tables simply have no row for the region, so no table failed to parse (2.26.5, F62).
    revenueWarnings = revenueWarnings.filter((w) => !(w && typeof w === "object" && (w as Record<string, unknown>).code === "TABLE_NOT_PARSED"));
  }
  let revStatus: string;
  if (geoValue != null) {
    revStatus = "FOUND";
  } else if (explicitRevenueStatus) {
    revStatus = explicitRevenueStatus;
  } else if (revenueStatus === "NOT_DISCLOSED" && synonymEntry != null) {
    revStatus = "NOT_DISCLOSED";
  } else {
    revStatus = "NOT_FOUND";
  }

  const revenueExposure: Record<string, unknown> = {
    status: revStatus,
    value: geoValue,
    denominator: geoDenominator,
    valuePct: geoDenominator != null ? (firstRevenue.valuePct ?? null) : null,
    valueRatio: geoDenominator != null ? (firstRevenue.valueRatio ?? null) : null,
    unit: firstRevenue.unit ?? "USD",
    region: firstRevenue.label ?? topic,
    period: firstRevenue.period ?? null,
    extractionMethod: geoValue != null ? "REVENUE_EXPOSURE" : "NONE",
    confidence: geoValue != null ? (geoConf || "MEDIUM") : (explicitRevenueStatus ?? (geoConf === "NOT_DISCLOSED" ? "LOW" : "LOW")),
    code: explicitRevenueStatus,
    evidence: {
      filingType: revenueRaw.filingType ?? filingType,
      filingDate: revenueRaw.filingDate ?? filingDate,
      accessionNumber: revenueRaw.accessionNumber ?? accessionNumber,
      documentUrl: revenueRaw.documentUrl ?? documentUrl,
      sectionHeading: geoEvidence.sectionHeading ?? null,
      sourceRows: Array.isArray(geoEvidence.sourceRows) ? geoEvidence.sourceRows : [],
      sourceColumns: Array.isArray(geoEvidence.sourceColumns) ? geoEvidence.sourceColumns : [],
    },
  };

  // --- Process operational exposure ---
  const opsRaw = operationalResult.status === "fulfilled" && operationalResult.value
    ? parseObjectJson(operationalResult.value)
    : {};
  const opsMatches: Record<string, unknown>[] = Array.isArray(opsRaw.matches)
    ? (opsRaw.matches as Record<string, unknown>[]).filter((m) => m && typeof m === "object")
    : [];

  // Scan for operational keyword co-occurrence in text excerpts
  const opsEvidence: Record<string, unknown>[] = [];
  const foundOpTerms = new Set<string>();
  let rejectedOpsNoise = 0;
  for (const m of opsMatches) {
    const excerpt = readableExposureExcerpt(
      m.contextText ?? m.context ?? "",
      [topicLower],
      200,
    );
    if (!excerpt) {
      rejectedOpsNoise += 1;
      continue;
    }
    const contextText = excerpt.toLowerCase();
    for (const opTerm of OPERATIONAL_TERMS) {
      if (contextText.includes(opTerm)) {
        foundOpTerms.add(opTerm);
        if (opsEvidence.length < 5) {
          opsEvidence.push({
            term: opTerm,
            excerpt,
            section: String(m.sectionHeading ?? ""),
          });
        }
        break;
      }
    }
    if (opsEvidence.length >= 5) break;
  }

  const operationalExposure: Record<string, unknown> = {
    status: opsEvidence.length > 0 ? "FOUND" : "NOT_FOUND",
    terms: Array.from(foundOpTerms),
    evidence: opsEvidence,
    rejectedNoiseCount: rejectedOpsNoise,
  };

  // --- Process entity exposure (China only) ---
  let entityExposure: Record<string, unknown>;
  if (isChina && entityResult.status === "fulfilled" && entityResult.value) {
    const entRaw = parseObjectJson(entityResult.value);
    const entMatches: Record<string, unknown>[] = Array.isArray(entRaw.matches)
      ? (entRaw.matches as Record<string, unknown>[]).filter((m) => m && typeof m === "object")
      : [];
    const foundEntities = new Set<string>();
    const entEvidence: Record<string, unknown>[] = [];
    let rejectedEntityNoise = 0;
    for (const m of entMatches) {
      const termLow = String(m.term ?? "").toLowerCase();
      const excerpt = termLow
        ? contextualEntityExcerpt(m.contextText ?? m.context ?? "", termLow, 200)
        : "";
      if (!termLow || !excerpt) {
        rejectedEntityNoise += 1;
        continue;
      }
      if (termLow && !foundEntities.has(termLow)) {
        foundEntities.add(termLow);
        if (entEvidence.length < 5) {
          entEvidence.push({
            entity: String(m.term ?? ""),
            excerpt,
            section: String(m.sectionHeading ?? ""),
          });
        }
      }
      if (entEvidence.length >= 5) break;
    }
    entityExposure = {
      status: entEvidence.length > 0 ? "FOUND" : "NOT_FOUND",
      entities: Array.from(foundEntities),
      evidence: entEvidence,
      rejectedNoiseCount: rejectedEntityNoise,
    };
  } else {
    entityExposure = { status: "NOT_FOUND", entities: [], evidence: [], rejectedNoiseCount: 0 };
  }

  // --- Process risk factor exposure ---
  let riskFactorExposure: Record<string, unknown>;
  if (includeRiskFactors && riskResult.status === "fulfilled" && riskResult.value) {
    const riskRaw = parseObjectJson(riskResult.value);
    const riskMatches: Record<string, unknown>[] = Array.isArray(riskRaw.matches)
      ? (riskRaw.matches as Record<string, unknown>[]).filter((m) => m && typeof m === "object")
      : [];
    const riskEvidence = riskMatches
      .map((m) => {
        const excerpt = readableExposureExcerpt(
          m.excerpt ?? m.context ?? m.contextText ?? "",
          [topicLower],
          200,
        );
        return { excerpt, section: String(m.sectionHeading ?? "Risk Factors") };
      })
      .filter((m) => m.excerpt.length > 0)
      .slice(0, 5);
    riskFactorExposure = {
      status: riskEvidence.length > 0 ? "FOUND" : "NOT_FOUND",
      mentionCount: riskMatches.length,
      evidence: riskEvidence,
    };
  } else {
    riskFactorExposure = { status: "NOT_FOUND", mentionCount: 0, evidence: [] };
  }

  // --- Determine overallStatus ---
  const nonRevenueFound = (opsEvidence.length > 0) ||
    (Array.isArray(entityExposure.evidence) && (entityExposure.evidence as unknown[]).length > 0) ||
    (Array.isArray(riskFactorExposure.evidence) && (riskFactorExposure.evidence as unknown[]).length > 0);

  let overallStatus: string;
  if (revStatus === "FOUND") {
    overallStatus = "FOUND_REVENUE_EXPOSURE";
  } else if (nonRevenueFound) {
    overallStatus = "FOUND_NON_REVENUE_EXPOSURE";
  } else if (explicitRevenueStatus) {
    overallStatus = explicitRevenueStatus;
  } else if (revStatus === "NOT_DISCLOSED") {
    overallStatus = "NOT_DISCLOSED";
  } else {
    overallStatus = "NOT_FOUND";
  }

  // Propagate any errors as warnings
  if (revenueResult.status === "rejected") warnings.push({ code: "REVENUE_EXTRACTION_ERROR", message: String(revenueResult.reason), severity: "warning" });
  if (revenueResult.status === "fulfilled") {
    for (const warning of revenueWarnings) {
      if (warning && typeof warning === "object") warnings.push(warning as Record<string, unknown>);
    }
  }
  if (operationalResult.status === "rejected") warnings.push({ code: "OPERATIONAL_SCAN_ERROR", message: String(operationalResult.reason), severity: "warning" });

  return JSON.stringify({
    ticker,
    topic: topicLower,
    filingType: idx.filingType ?? filingType,
    filingDate,
    accessionNumber,
    documentUrl,
    revenueExposure,
    operationalExposure,
    entityExposure,
    riskFactorExposure,
    overallStatus,
    code: explicitRevenueStatus,
    warnings,
  });
}
