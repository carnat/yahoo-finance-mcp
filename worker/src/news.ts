/**
 * News and public events: company news, press releases, SEC recent events, the event timeline and event
 * verification, with the source collectors and parsers behind them (Phase 3 move out of yahoo-finance.ts).
 * Shared helpers still live in yahoo-finance.ts and are imported back; see analysis/yahoo-finance-mcp/PHASE3_IMPORTS_TS.md.
 *
 * 2.15.1: the source collectors live in news/providers/*.ts and the helpers they share with this pipeline in
 * news/shared.ts. This file keeps the shared pipeline (sources, dedupe, coverage, collection status, the public
 * tool functions) and re-exports the moved names other modules and tests import from here.
 */

import { withBudgetWatch } from "./request-context.js";
import { tickerCap } from "./tool-catalog.js";
import { rankEvidence, stemWord } from "./extraction-rules.js";
import { reliabilityClassFor } from "./deprecations.js";
import {
  yGet,
  enc,
  runPartialBatch,
  edgarCikFromAccession,
  _str,
  clampInt,
  type NewsCompanyIdentity,
  finnhubEligibility,
  resolveNewsCompanyIdentity,
  resolveEx991Url,
  getSubmissionsForTicker,
} from "./yahoo-finance.js";
import { collectCompanyIrPageEvents } from "./news/providers/company-ir-page.js";
import { collectCompanyIrEvents } from "./news/providers/company-ir.js";
import { collectFinnhubEvents } from "./news/providers/finnhub.js";
import { collectGlobeNewswireEvents } from "./news/providers/globenewswire.js";
import { MARKETAUX_WIRE_DOMAINS, MARKETAUX_MAX_ITEMS, collectMarketauxEvents } from "./news/providers/marketaux.js";
import { collectSecEvents } from "./news/providers/sec.js";
import { yahooNewsIdentityFor, yahooDecisionUse, collectYahooEvents } from "./news/providers/yahoo.js";
import { sourceDecisionUse, shortText, withinDateWindow, isoUtc, tickerInText } from "./news/shared.js";
import newsSourceRegistry from "./news-source-registry.json";
import { ErrorCode, getWorkerVar, mcpFailure } from "./response.js";
// Names other modules and tests import from news.ts (2.15.1 provider move).
export { parseHtmlTable } from "./news/shared.js";
export { getNews, yahooItemIssuer, collectYahooEvents } from "./news/providers/yahoo.js";

const COMPANY_IR_BLOCKED_WEBSITE_HOSTS = [
  "bloomberg.com",
  "facebook.com",
  "finance.yahoo.com",
  "google.com",
  "instagram.com",
  "linkedin.com",
  "marketwatch.com",
  "nasdaq.com",
  "reuters.com",
  "sec.gov",
  "twitter.com",
  "wikipedia.org",
  "x.com",
  "yahoo.com",
  "youtube.com",
];
const OFFICIAL_SOURCE_TYPES = new Set(["sec_filing", "company_ir", "company_ir_page", "press_release", "newswire", "yahoo_finance_press_releases"]);
const SUPPORTED_EVENT_SOURCES = new Set([
  "sec", "company_ir", "company_ir_page", "newswire",
  "yahoo_finance",                  // legacy: aggregates news + press releases
  "yahoo_finance_news",             // Yahoo Finance general news tab
  "yahoo_finance_press_releases",   // Yahoo Finance press releases tab
  "finnhub", "marketaux",
]);

const SOURCE_PRIORITY: Record<string, number> = {
  sec_filing: 0,
  sec_ex99_found: 0,
  company_ir: 1,
  company_ir_page: 1,
  press_release: 2,
  yahoo_finance_press_releases: 2,
  newswire: 3,
  company_news: 4,
  yahoo_finance: 5,
  yahoo_finance_news: 5,
  marketaux_wire: 6,
  other: 7,
};
function confidenceForSourceType(sourceType: unknown, fallback: unknown = "LOW"): string {
  const src = _str(sourceType);
  if (src === "sec_filing" || src === "company_ir" || src === "company_ir_page") return "HIGH";
  if (src === "press_release" || src === "yahoo_finance_press_releases" || src === "newswire") return "MEDIUM";
  if (src === "company_news" || src === "yahoo_finance_news" || src === "yahoo_finance" || src === "finnhub") return "LOW";
  return _str(fallback, "LOW");
}
// Default source sets the news tools apply when `sources` is omitted or null (Q3h, 2.7.0).
// An explicit array, including [], is used as given: [] attempts no source.
// 2.16.0: the news defaults come from the shared source registry (news-source-registry.json), which Python reads too.
export const DEFAULT_NEWS_SOURCES: string[] = [...newsSourceRegistry.defaultSources];
export const DEFAULT_PRESS_RELEASE_SOURCES = ["sec", "company_ir_page", "yahoo_finance_press_releases"];
export const DEFAULT_EVENT_SOURCES = ["sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"];
const NOT_ATTEMPTED_SOURCE_STATUSES = new Set(["NOT_ELIGIBLE", "NOT_NEEDED", "UNCONFIGURED"]);
/** What the caller asked for and what was actually tried (Q3h): requestedSources is null when sources was omitted. */
export function sourceRequestFields(requestedSources: string[] | null, sourceStatus: Record<string, unknown>): Record<string, unknown> {
  const attemptedSources: Record<string, unknown>[] = [];
  for (const [source, value] of Object.entries(sourceStatus)) {
    if (!value || typeof value !== "object") continue;
    const info = value as Record<string, unknown>;
    const status = _str(info.status);
    if (info.attempted === false || NOT_ATTEMPTED_SOURCE_STATUSES.has(status)) continue;
    attemptedSources.push({ sourceType: source, status });
  }
  return { requestedSources, defaultSourcesApplied: requestedSources === null, attemptedSources };
}
function normalizeSources(sources: string[] | undefined, defaults: string[]): { selected: string[]; warnings: Record<string, unknown>[] } {
  const warnings: Record<string, unknown>[] = [];
  const input = (sources ?? defaults).map(s => _str(s).toLowerCase().trim()).filter(Boolean);
  const seen = new Set<string>();
  const selected: string[] = [];
  for (const src of input) {
    if (!SUPPORTED_EVENT_SOURCES.has(src)) {
      warnings.push({ code: "SOURCE_UNSUPPORTED", message: `Source '${src}' is not supported.`, severity: "warning" });
      continue;
    }
    if (!seen.has(src)) {
      seen.add(src);
      selected.push(src);
    }
  }
  // An explicit list is used as given (Q3h): [] or only unsupported names selects no source.
  return { selected, warnings };
}
/**
 * Operator off switch (2.16.0): NEWS_SOURCES_DISABLED is a comma-separated list of source names, each trimmed
 * and lower-cased; unknown names are ignored and an empty or unset value disables nothing.
 */
function disabledNewsSources(): Set<string> {
  const raw = getWorkerVar("NEWS_SOURCES_DISABLED") ?? "";
  return new Set(raw.split(",").map(name => name.trim().toLowerCase()).filter(name => SUPPORTED_EVENT_SOURCES.has(name)));
}
/** Selected sources split into the ones to collect and the ones the operator disabled (selection order kept). */
function splitDisabledSources(selected: string[]): { active: string[]; disabled: string[] } {
  const off = disabledNewsSources();
  return { active: selected.filter(src => !off.has(src)), disabled: selected.filter(src => off.has(src)) };
}
/** One info warning per disabled source that was selected (existing SOURCE_UNAVAILABLE shape). */
function disabledSourceWarnings(disabled: string[]): Record<string, unknown>[] {
  return disabled.map(source => ({
    code: "SOURCE_UNAVAILABLE",
    message: `Source ${source} is disabled by the server operator.`,
    severity: "info",
    source,
  }));
}
/** A disabled source's row always replaces whatever message inference produced (RNEWS2 §8.2/§8.3). */
function markDisabledSources(sourceStatus: Record<string, unknown>, disabled: string[]): Record<string, unknown> {
  for (const source of disabled) {
    sourceStatus[source] = { status: "UNCONFIGURED", rawCount: 0, filteredCount: 0, attempted: false };
  }
  return sourceStatus;
}
/**
 * An explicit sources list that selects no supported source attempts nothing (Q3h). The result is
 * neither a provider failure nor confirmed absence, so it reads as source-limited.
 */
function markNoSourceSelected(payload: Record<string, unknown>, requestedSources: string[] | null): Record<string, unknown> {
  if (requestedSources === null || normalizeSources(requestedSources, []).selected.length > 0) return payload;
  payload.status = "SOURCE_LIMITED_NOT_FOUND";
  payload.sourceCoverage = "PARTIAL";
  if (payload.coverage && typeof payload.coverage === "object") {
    payload.coverage = { ...(payload.coverage as Record<string, unknown>), state: "PARTIAL", recommendedNextAction: "CHECK_OFFICIAL_RELEASES" };
  }
  return payload;
}
type CompanyEventCollectionResult = {
  items: Record<string, unknown>[];
  sourcesUsed: string[];
  warnings: Record<string, unknown>[];
  watermark: string;
  sourceDiagnostics: Record<string, unknown>;
  /** Sources whose collection had a fetch refused for the subrequest budget (2.11.0); never serialized. */
  budgetStarvedSources?: string[];
  /** Selected sources the operator disabled (2.16.0); never serialized. */
  disabledSources?: string[];
};
function evidenceClassFor(sourceType: unknown): string {
  const source = _str(sourceType);
  if (source === "sec_filing" || source === "sec_ex99_found") return "SEC_FILING";
  if (source === "company_ir_page") return "APPROVED_IR_PAGE";
  if (source === "company_ir") return "OFFICIAL_RSS_ATOM";
  if (["newswire", "press_release", "yahoo_finance_press_releases"].includes(source)) return "WIRE_RELEASE";
  return "CONTEXTUAL_NEWS";
}
function urlProvenanceFor(item: Record<string, unknown>): string {
  const source = _str(item.sourceType);
  if (["sec_filing", "sec_ex99_found", "company_ir", "company_ir_page"].includes(source)) return "OFFICIAL";
  const url = _str(item.url);
  if (!url) return "UNKNOWN";
  try {
    const host = new URL(url).hostname.toLowerCase();
    if (host.endsWith("finance.yahoo.com") || host.endsWith("finnhub.io")) return "PROVIDER";
  } catch { return "UNKNOWN"; }
  return "PUBLISHER";
}
function tickerMatchFor(item: Record<string, unknown>): string {
  if (["TICKER_TOKEN", "ISSUER_NAME", "ISSUER_ACRONYM", "PROVIDER_ENTITY_SYMBOL"].includes(_str(item.matchBasis))) return "EXPLICIT";
  const configuredTicker = Array.isArray(item.tickers) ? item.tickers.map(value => _str(value).toUpperCase()).find(Boolean) ?? "" : "";
  const text = `${_str(item.title)} ${_str(item.summary)} ${_str(item.evidenceText)}`.toUpperCase();
  if (configuredTicker && new RegExp(`\\b${configuredTicker.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`).test(text)) return "EXPLICIT";
  if (configuredTicker) return "SOURCE_SCOPED";
  return "UNVERIFIED";
}
function enrichNewsItemForLlm(item: Record<string, unknown>): Record<string, unknown> {
  const sourceType = _str(item.sourceType);
  const decisionUse = sourceType === "company_ir_page" && _str(item.approvalStatus) === "approved"
    ? "USE_OFFICIAL_EVIDENCE"
    : (sourceType === "yahoo_finance_news" || sourceType === "company_news"
      ? yahooDecisionUse(_str(item.eventType))
      : sourceDecisionUse(sourceType));
  return {
    ...item,
    evidenceClass: evidenceClassFor(item.sourceType),
    tickerMatch: tickerMatchFor(item),
    urlProvenance: urlProvenanceFor(item),
    decisionUse,
  };
}
export function buildCoverage(sourceStatus: Record<string, unknown>, resolvedAction: string | null = null): Record<string, unknown> {
  const failedStatuses = new Set(["UNCONFIGURED", "PROVIDER_ERROR", "AUTH_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_BUDGET_EXHAUSTED", "PROVIDER_CHANGED", "IDENTITY_UNAVAILABLE", "WEBSITE_NOT_AVAILABLE", "DISCOVERY_NOT_FOUND", "DISCOVERY_BUDGET_EXHAUSTED", "FEED_NOT_FOUND", "PARSE_ERROR", "CANDIDATE_AVAILABLE", "NOT_REGISTERED", "DISABLED", "REVALIDATION_DUE", "SOURCE_VALIDATION_FAILED"]);
  const failedSources: Record<string, unknown>[] = [];
  const skippedSources: Record<string, unknown>[] = [];
  const truncatedSources: Record<string, unknown>[] = [];
  for (const [source, value] of Object.entries(sourceStatus)) {
    if (!value || typeof value !== "object") continue;
    const info = value as Record<string, unknown>;
    const status = _str(info.status);
    const entry = { source, status, attempted: info.attempted !== false, reasonCode: info.reasonCode ?? null };
    if (status === "NOT_ELIGIBLE" || status === "NOT_NEEDED") skippedSources.push(entry);
    else if (failedStatuses.has(status)) failedSources.push(entry);
    if (info.hasMoreAccepted === true) {
      truncatedSources.push({
        source,
        acceptedCount: Number(info.acceptedCount ?? 0),
        returnedCount: Number(info.returnedCount ?? 0),
        truncatedCount: Number(info.truncatedCount ?? 0),
      });
    }
  }
  const coverageLimitingSkips = skippedSources.filter(entry => entry.status !== "NOT_NEEDED");
  const recommendedNextAction = resolvedAction ?? (
    failedSources.some(entry => ["RATE_LIMITED", "TIMEOUT", "PROVIDER_BUDGET_EXHAUSTED"].includes(_str(entry.status)))
      ? "RETRY_RETRYABLE_SOURCES"
      : (failedSources.length || coverageLimitingSkips.length
        ? "CHECK_OFFICIAL_RELEASES"
        : (truncatedSources.length ? "RETRY_TRUNCATED_SOURCE" : "USE_RETURNED_CONTEXT"))
  );
  return { state: computeSourceCoverage(sourceStatus), failedSources, skippedSources, truncatedSources, recommendedNextAction };
}
export function hostWithoutWww(host: string): string {
  return host.toLowerCase().replace(/^www\./, "");
}
export function blockedCompanyWebsiteHost(host: string): boolean {
  const h = hostWithoutWww(host);
  return COMPANY_IR_BLOCKED_WEBSITE_HOSTS.some(blocked => h === blocked || h.endsWith(`.${blocked}`));
}
function dedupeEventItems(items: Record<string, unknown>[], warnings: Record<string, unknown>[]): Record<string, unknown>[] {
  const grouped = new Map<string, Record<string, unknown>>();
  const passthrough: Record<string, unknown>[] = [];
  for (const item of items) {
    const gid = _str(item.duplicateGroupId);
    if (!gid) {
      passthrough.push(item);
      continue;
    }
    const existing = grouped.get(gid);
    if (!existing) {
      grouped.set(gid, item);
      continue;
    }
    const existingRank = SOURCE_PRIORITY[_str(existing.sourceType, "other")] ?? SOURCE_PRIORITY.other;
    const currentRank = SOURCE_PRIORITY[_str(item.sourceType, "other")] ?? SOURCE_PRIORITY.other;
    let keepNew = currentRank < existingRank;
    if (currentRank === existingRank) {
      keepNew = _str(item.publishedAt) > _str(existing.publishedAt);
    }
    if (_str(existing.publishedAt) !== _str(item.publishedAt)) {
      warnings.push({
        code: "TIMESTAMP_VARIANCE",
        message: `Different source timestamps observed for duplicateGroupId=${gid}; this is dedupe metadata, not factual evidence conflict.`,
        severity: "warning",
        duplicateGroupId: gid,
        statusImpact: false,
      });
    }
    const preferred = keepNew ? item : existing;
    const alternate = keepNew ? existing : item;
    const refs = Array.isArray(preferred.sourceRefs) ? preferred.sourceRefs as Record<string, unknown>[] : [];
    refs.push({
      source: alternate.source,
      sourceType: alternate.sourceType,
      publishedAt: alternate.publishedAt,
      url: alternate.url,
    });
    preferred.sourceRefs = refs;
    grouped.set(gid, preferred);
  }
  const deduped = [...grouped.values(), ...passthrough];
  deduped.sort((a, b) => _str(b.publishedAt).localeCompare(_str(a.publishedAt)));
  return deduped;
}
function collectionStatus(items: Record<string, unknown>[], sourcesUsed: string[], warnings: Record<string, unknown>[]): string | null {
  const hasLimitedSource = warnings.some(w => ["SOURCE_UNAVAILABLE", "SOURCE_NOT_ELIGIBLE", "SOURCE_IDENTITY_UNAVAILABLE"].includes(_str(w.code)));
  if (items.length > 0 && hasLimitedSource) return "PARTIAL";
  if (items.length === 0) {
    if (hasLimitedSource) return "SOURCE_LIMITED_NOT_FOUND";
    if (warnings.some(w => [
      "COMPANY_WEBSITE_NOT_AVAILABLE",
      "COMPANY_IR_FEED_NOT_FOUND",
      "COMPANY_IR_RSS_HOST_REJECTED",
      "COMPANY_IR_RSS_PARSE_ERROR",
      "COMPANY_IR_RSS_UNAVAILABLE",
      "COMPANY_IR_DISCOVERY_BUDGET_EXHAUSTED",
      "COMPANY_IR_PAGE_VALIDATION_FAILED",
      "COMPANY_IR_PAGE_REVALIDATION_DUE",
      "COMPANY_IR_PAGE_CANDIDATE_AVAILABLE",
      "COMPANY_IR_PAGE_DISABLED",
    ].includes(_str(w.code)))) return "SOURCE_LIMITED_NOT_FOUND";
    if (sourcesUsed.length > 0) return "NOT_FOUND";
    return "PROVIDER_ERROR";
  }
  return null;
}
function computeSourceStatus(
  sourcesUsed: string[],
  warnings: Record<string, unknown>[],
  items: Record<string, unknown>[],
  selectedSources: string[],
  sourceDiagnostics: Record<string, unknown> = {},
  budgetStarvedSources: string[] = [],
  disabledSources: string[] = [],
): Record<string, unknown> {
  const warningCodes = new Set(warnings.map(w => _str(w.code)));
  const warningMsgs = warnings
    .filter(w => _str(w.code) === "SOURCE_UNAVAILABLE")
    .map(w => _str(w.message).toLowerCase());
  const isYfError = warningMsgs.some(m => m.includes("yahoo finance"));
  const secItems = items.filter(it => _str(it.sourceType).includes("sec"));
  const getItemSource = (it: Record<string, unknown>): string => _str(it.source);
  const getItemSourceType = (it: Record<string, unknown>): string => _str(it.sourceType);
  // Fine-grained Yahoo Finance items
  const yfNewsItems = items.filter(
    it => getItemSource(it) === "yahoo_finance_news" || getItemSourceType(it) === "yahoo_finance_news"
  );
  const yfPrItems = items.filter(
    it =>
      getItemSource(it) === "yahoo_finance_press_releases"
      || getItemSourceType(it) === "yahoo_finance_press_releases"
  );
  // Legacy yahoo_finance aggregates both fine-grained sources plus legacy-tagged items
  const yfLegacyItems = items.filter(
    it =>
      ["yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases"].includes(getItemSource(it))
      || ["yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases"].includes(getItemSourceType(it))
  );
  const newswireItems = items.filter(it => _str(it.sourceType) === "newswire");
  const companyIrItems = items.filter(it => ["company_ir", "press_release"].includes(_str(it.sourceType)));
  const companyIrPageItems = items.filter(it => _str(it.sourceType) === "company_ir_page");
  const finnhubItems = items.filter(it => _str(it.source) === "finnhub");
  const marketauxItems = items.filter(it => _str(it.source) === "marketaux");
  const result: Record<string, unknown> = {};
  const yahooStatus = (source: string, sourceItems: Record<string, unknown>[]): Record<string, unknown> | null => {
    const diagnostic = (sourceDiagnostics[source] && typeof sourceDiagnostics[source] === "object")
      ? sourceDiagnostics[source] as Record<string, unknown>
      : null;
    if (!diagnostic) return null;
    const acceptedCount = Number(diagnostic.acceptedCount ?? diagnostic.filteredCount ?? sourceItems.length);
    return {
      status: diagnostic.completed === false ? "PROVIDER_ERROR" : (acceptedCount > 0 ? "OK" : "EMPTY_RESULT"),
      rawCount: Number(diagnostic.rawCount ?? 0),
      retrievedCount: Number(diagnostic.retrievedCount ?? diagnostic.rawCount ?? 0),
      filteredCount: Number(diagnostic.filteredCount ?? 0),
      acceptedCount,
      returnedCount: sourceItems.length,
      rejectedCount: Number(diagnostic.rejectedCount ?? 0),
      rejectionCounts: diagnostic.rejectionCounts ?? {},
      identityStatus: diagnostic.identityStatus ?? null,
      attempted: diagnostic.attempted !== false,
      ...(diagnostic.method ? { method: diagnostic.method } : {}),
    };
  };
  const identityDiagnostic = (sourceDiagnostics.yahoo_finance_identity && typeof sourceDiagnostics.yahoo_finance_identity === "object")
    ? sourceDiagnostics.yahoo_finance_identity as Record<string, unknown>
    : null;
  if (identityDiagnostic && _str(identityDiagnostic.status) === "IDENTITY_UNAVAILABLE") {
    result.yahoo_finance_identity = identityDiagnostic;
  }
  if (selectedSources.includes("sec")) {
    const secDiagnostic = (sourceDiagnostics.sec && typeof sourceDiagnostics.sec === "object")
      ? sourceDiagnostics.sec as Record<string, unknown>
      : {};
    if (_str(secDiagnostic.status) === "NOT_ELIGIBLE") {
      result.sec = { status: "NOT_ELIGIBLE", rawCount: 0, filteredCount: 0, ...secDiagnostic };
    } else if (sourcesUsed.includes("sec")) {
      result.sec = { status: secItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: secItems.length, filteredCount: secItems.length };
    } else if (warningMsgs.some(m => m.includes("sec submissions"))) {
      result.sec = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.sec = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  // Fine-grained Yahoo Finance sources
  if (selectedSources.includes("yahoo_finance_news")) {
    const diagnosticStatus = yahooStatus("yahoo_finance_news", yfNewsItems);
    if (diagnosticStatus) {
      result.yahoo_finance_news = diagnosticStatus;
    } else if (sourcesUsed.includes("yahoo_finance_news")) {
      result.yahoo_finance_news = { status: yfNewsItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: yfNewsItems.length, filteredCount: yfNewsItems.length };
    } else if (isYfError) {
      result.yahoo_finance_news = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.yahoo_finance_news = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  if (selectedSources.includes("yahoo_finance_press_releases")) {
    const diagnosticStatus = yahooStatus("yahoo_finance_press_releases", yfPrItems);
    if (diagnosticStatus) {
      result.yahoo_finance_press_releases = diagnosticStatus;
    } else if (sourcesUsed.includes("yahoo_finance_press_releases")) {
      result.yahoo_finance_press_releases = { status: yfPrItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: yfPrItems.length, filteredCount: yfPrItems.length };
    } else if (isYfError) {
      result.yahoo_finance_press_releases = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.yahoo_finance_press_releases = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  // Legacy yahoo_finance aggregate
  if (selectedSources.includes("yahoo_finance")) {
    const diagnosticStatus = yahooStatus("yahoo_finance", yfLegacyItems);
    if (diagnosticStatus) {
      result.yahoo_finance = diagnosticStatus;
    } else if (sourcesUsed.includes("yahoo_finance")) {
      result.yahoo_finance = { status: yfLegacyItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: yfLegacyItems.length, filteredCount: yfLegacyItems.length };
    } else if (isYfError) {
      result.yahoo_finance = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.yahoo_finance = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  if (selectedSources.includes("finnhub")) {
    const diagnostic = (sourceDiagnostics.finnhub && typeof sourceDiagnostics.finnhub === "object")
      ? sourceDiagnostics.finnhub as Record<string, unknown>
      : {};
    if (_str(diagnostic.status) === "NOT_ELIGIBLE") {
      result.finnhub = { status: "NOT_ELIGIBLE", rawCount: 0, filteredCount: 0, ...diagnostic };
    } else if (sourcesUsed.includes("finnhub")) {
      const acceptedCount = Number(diagnostic.acceptedCount ?? finnhubItems.length);
      const returnedCount = finnhubItems.length;
      const truncatedCount = Math.max(0, acceptedCount - returnedCount);
      result.finnhub = {
        status: acceptedCount > 0 ? "OK" : "EMPTY_RESULT",
        rawCount: Number(diagnostic.rawCount ?? acceptedCount),
        retrievedCount: Number(diagnostic.retrievedCount ?? diagnostic.rawCount ?? acceptedCount),
        filteredCount: acceptedCount,
        acceptedCount,
        collectedCount: Number(diagnostic.collectedCount ?? acceptedCount),
        returnedCount,
        rejectedCount: Number(diagnostic.rejectedCount ?? 0),
        rejectionCounts: diagnostic.rejectionCounts ?? {},
        truncatedCount,
        hasMoreAccepted: truncatedCount > 0,
        attempted: diagnostic.attempted !== false,
      };
    } else if (warningMsgs.some(m => m.includes("finnhub company-news source is not configured"))) {
      result.finnhub = { status: "UNCONFIGURED" };
    } else if (warningMsgs.some(m => m.includes("finnhub auth error"))) {
      result.finnhub = { status: "AUTH_ERROR", rawCount: 0, filteredCount: 0 };
    } else if (warningMsgs.some(m => m.includes("finnhub rate limited"))) {
      result.finnhub = { status: "RATE_LIMITED", rawCount: 0, filteredCount: 0 };
    } else if (warningMsgs.some(m => m.includes("finnhub provider changed"))) {
      result.finnhub = { status: "PROVIDER_CHANGED", rawCount: 0, filteredCount: 0 };
    } else if (warningMsgs.some(m => m.includes("finnhub"))) {
      result.finnhub = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.finnhub = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  if (selectedSources.includes("marketaux")) {
    const diagnostic = (sourceDiagnostics.marketaux && typeof sourceDiagnostics.marketaux === "object")
      ? sourceDiagnostics.marketaux as Record<string, unknown>
      : {};
    const acceptedCount = Number(diagnostic.acceptedCount ?? marketauxItems.length);
    const status = _str(diagnostic.status) || (acceptedCount > 0 ? "OK" : "EMPTY_RESULT");
    const allowedAction: Record<string, string> = {
      NOT_NEEDED: "none",
      UNCONFIGURED: "configure_marketaux_api_token",
      AUTH_ERROR: "verify_marketaux_api_token",
      RATE_LIMITED: "retry_after_quota_reset",
      TIMEOUT: "retry_later",
      PROVIDER_CHANGED: "inspect_provider_contract",
      PROVIDER_ERROR: "retry_or_use_official_sources",
    };
    result.marketaux = {
      status,
      rawCount: Number(diagnostic.rawCount ?? 0),
      retrievedCount: Number(diagnostic.retrievedCount ?? diagnostic.rawCount ?? 0),
      filteredCount: acceptedCount,
      acceptedCount,
      returnedCount: marketauxItems.length,
      rejectedCount: Number(diagnostic.rejectedCount ?? 0),
      rejectionCounts: diagnostic.rejectionCounts ?? {},
      attempted: diagnostic.attempted !== false,
      reasonCode: diagnostic.reasonCode ?? null,
      fallbackReason: diagnostic.fallbackReason ?? null,
      quotaPolicy: diagnostic.quotaPolicy ?? null,
      domains: diagnostic.domains ?? null,
      allowedAction: allowedAction[status] ?? (acceptedCount > 0 ? "check_official_releases" : "none"),
    };
  }
  if (selectedSources.includes("company_ir")) {
    const diagnostic = (sourceDiagnostics.company_ir && typeof sourceDiagnostics.company_ir === "object")
      ? sourceDiagnostics.company_ir as Record<string, unknown>
      : {};
    let companyIrStatus = "EMPTY_RESULT";
    if (sourcesUsed.includes("company_ir")) {
      companyIrStatus = companyIrItems.length > 0 ? "OK" : "EMPTY_RESULT";
    } else if (warningCodes.has("COMPANY_WEBSITE_NOT_AVAILABLE")) {
      companyIrStatus = "WEBSITE_NOT_AVAILABLE";
    } else if (warningCodes.has("COMPANY_IR_DISCOVERY_BUDGET_EXHAUSTED")) {
      companyIrStatus = "DISCOVERY_BUDGET_EXHAUSTED";
    } else if (warningCodes.has("COMPANY_IR_FEED_NOT_FOUND")) {
      companyIrStatus = "FEED_NOT_FOUND";
    } else if (warningCodes.has("COMPANY_IR_RSS_PARSE_ERROR")) {
      companyIrStatus = "PARSE_ERROR";
    } else if (warningCodes.has("COMPANY_IR_RSS_UNAVAILABLE")) {
      companyIrStatus = "PROVIDER_ERROR";
    } else if (warningCodes.has("COMPANY_IR_RSS_HOST_REJECTED")) {
      companyIrStatus = "DISCOVERY_NOT_FOUND";
    } else if (isYfError) {
      companyIrStatus = "PROVIDER_ERROR";
    }
    result.company_ir = { status: companyIrStatus, rawCount: companyIrItems.length, filteredCount: companyIrItems.length, ...diagnostic };
  }
  if (selectedSources.includes("company_ir_page")) {
    const diagnostic = (sourceDiagnostics.company_ir_page && typeof sourceDiagnostics.company_ir_page === "object")
      ? sourceDiagnostics.company_ir_page as Record<string, unknown>
      : {};
    let companyIrPageStatus = _str(diagnostic.status) || "EMPTY_RESULT";
    if (sourcesUsed.includes("company_ir_page") && !companyIrPageStatus) {
      companyIrPageStatus = companyIrPageItems.length > 0 ? "APPROVED_SOURCE_OK" : "EMPTY_RESULT";
    } else if (warningCodes.has("COMPANY_IR_PAGE_VALIDATION_FAILED")) {
      companyIrPageStatus = "SOURCE_VALIDATION_FAILED";
    } else if (warningCodes.has("COMPANY_IR_PAGE_REVALIDATION_DUE")) {
      companyIrPageStatus = "REVALIDATION_DUE";
    }
    result.company_ir_page = {
      status: companyIrPageStatus,
      rawCount: companyIrPageItems.length,
      filteredCount: companyIrPageItems.length,
      ...diagnostic,
    };
  }
  if (selectedSources.includes("newswire")) {
    if (sourcesUsed.includes("newswire")) {
      result.newswire = { status: newswireItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: newswireItems.length, filteredCount: newswireItems.length };
    } else if (warningMsgs.some(m => m.includes("globenewswire"))) {
      result.newswire = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.newswire = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  return markDisabledSources(markBudgetStarvedSources(result, budgetStarvedSources), disabledSources);
}
/**
 * A source whose collection had a fetch refused for the subrequest budget and
 * did not end OK reports PROVIDER_BUDGET_EXHAUSTED (2.11.0), never
 * PROVIDER_ERROR, EMPTY_RESULT or another absence. Only `status` changes.
 */
const BUDGET_UNAFFECTED_STATUSES = new Set(["OK", "APPROVED_SOURCE_OK"]);
function markBudgetStarvedSources(sourceStatus: Record<string, unknown>, budgetStarvedSources: string[]): Record<string, unknown> {
  for (const source of budgetStarvedSources) {
    const info = sourceStatus[source];
    if (!info || typeof info !== "object") continue;
    const row = info as Record<string, unknown>;
    if (!BUDGET_UNAFFECTED_STATUSES.has(_str(row.status))) row.status = "PROVIDER_BUDGET_EXHAUSTED";
  }
  return sourceStatus;
}
function computeSourceCoverage(sourceStatus: Record<string, unknown>): string {
  const limitedStatuses = new Set([
    "UNCONFIGURED",
    "NOT_ELIGIBLE",
    "PROVIDER_ERROR",
    "AUTH_ERROR",
    "RATE_LIMITED",
    "TIMEOUT",
    "PROVIDER_BUDGET_EXHAUSTED",
    "PROVIDER_CHANGED",
    "IDENTITY_UNAVAILABLE",
    "WEBSITE_NOT_AVAILABLE",
    "DISCOVERY_NOT_FOUND",
    "DISCOVERY_BUDGET_EXHAUSTED",
    "FEED_NOT_FOUND",
    "PARSE_ERROR",
    "CANDIDATE_AVAILABLE",
    "NOT_REGISTERED",
    "DISABLED",
    "REVALIDATION_DUE",
    "SOURCE_VALIDATION_FAILED",
  ]);
  for (const info of Object.values(sourceStatus)) {
    if (info && typeof info === "object" && limitedStatuses.has(_str((info as Record<string, unknown>).status))) {
      return "PARTIAL";
    }
  }
  return "FULL";
}
function verifyCoverageFailureMode(
  sourceStatus: Record<string, unknown>,
  warnings: Record<string, unknown>[],
  companyIrRequested = false,
): string | null {
  const sourceStatuses = Object.values(sourceStatus)
    .filter((info): info is Record<string, unknown> => !!info && typeof info === "object")
    .map(info => _str(info.status));
  const warningText = warnings
    .filter(w => _str(w.code) === "SOURCE_UNAVAILABLE")
    .map(w => _str(w.message).toLowerCase())
    .join(" ");
  if (warningText.includes("too many subrequests") || sourceStatuses.includes("PROVIDER_BUDGET_EXHAUSTED")) return "WORKER_SUBREQUEST_LIMIT";
  if (sourceStatuses.includes("RATE_LIMITED")) return "RATE_LIMITED";
  if (sourceStatuses.includes("TIMEOUT")) return "PROVIDER_TIMEOUT";
  if (sourceStatuses.includes("PROVIDER_CHANGED")) return "PROVIDER_CHANGED";
  if (sourceStatuses.includes("IDENTITY_UNAVAILABLE")) return "YAHOO_IDENTITY_UNAVAILABLE";
  // AUTH_ERROR and an unregistered company_ir map to the existing SOURCE_UNCONFIGURED (2.19.0, calls #10 / #14). The
  // company_ir upgrade applies only when the caller named company_ir: a defaults-applied NOT_REGISTERED row stays in
  // sourceStatus / coverage without changing the status (2.19.0, planner decision 7).
  const companyIrStatus = companyIrRequested && sourceStatus.company_ir && typeof sourceStatus.company_ir === "object"
    ? _str((sourceStatus.company_ir as Record<string, unknown>).status)
    : "";
  if (sourceStatuses.includes("UNCONFIGURED") || sourceStatuses.includes("AUTH_ERROR") || companyIrStatus === "NOT_REGISTERED") return "SOURCE_UNCONFIGURED";
  if (sourceStatuses.includes("NOT_ELIGIBLE")) return "SOURCE_NOT_ELIGIBLE";
  if (sourceStatuses.includes("PROVIDER_ERROR")) return "PROVIDER_ERROR";
  return null;
}
function marketauxFallbackReason(
  selected: string[],
  items: Record<string, unknown>[],
  warnings: Record<string, unknown>[],
  sourceDiagnostics: Record<string, unknown>,
  searchQuery = "",
): string | null {
  const prioritySources = new Set(["yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"]);
  const selectedPriority = selected.filter(source => prioritySources.has(source));
  if (selectedPriority.length === 0) return "EXPLICIT_SOURCE_SELECTION";
  const query = searchQuery.trim().toLowerCase();
  const hasPriorityItems = items.some(item => {
    const isPriority = prioritySources.has(_str(item.source))
      || prioritySources.has(_str(item.sourceType))
      || _str(item.sourceType) === "company_news";
    if (!isPriority || !query) return isPriority;
    const text = `${_str(item.title)} ${_str(item.summary)} ${_str(item.source)} ${_str(item.eventType)} ${_str(item.evidenceText)}`.toLowerCase();
    return text.includes(query);
  });
  let limited = warnings.some(warning => {
    const code = _str(warning.code);
    const sourceOrMessage = `${_str(warning.source)} ${_str(warning.message)}`.toLowerCase();
    return code === "SOURCE_IDENTITY_UNAVAILABLE"
      || (["SOURCE_UNAVAILABLE", "SOURCE_NOT_ELIGIBLE"].includes(code) && sourceOrMessage.includes("finnhub"))
      || (code === "SOURCE_UNAVAILABLE" && sourceOrMessage.includes("yahoo finance"));
  });
  for (const source of selectedPriority) {
    const value = sourceDiagnostics[source];
    const diagnostic = value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
    if (diagnostic && (
      diagnostic.completed === false
      || ["UNCONFIGURED", "NOT_ELIGIBLE", "PROVIDER_ERROR", "AUTH_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_CHANGED"].includes(_str(diagnostic.status))
    )) limited = true;
  }
  if (limited) return "HIGHER_PRIORITY_COVERAGE_LIMITED";
  if (!hasPriorityItems) return "HIGHER_PRIORITY_EMPTY";
  return null;
}
async function collectCompanyEvents(
  ticker: string,
  {
    maxResults = 10,
    lookbackDays = 14,
    startDate = "",
    endDate = "",
    sources,
    secFilingTypes = ["8-K", "10-Q", "10-K", "S-3", "DEF14A"],
    searchQuery = "",
    keepAll = false,
  }: {
    maxResults?: number;
    lookbackDays?: number;
    startDate?: string;
    endDate?: string;
    sources?: string[];
    secFilingTypes?: string[];
    searchQuery?: string;
    /** Keep every deduplicated item instead of the newest maxResults (event verification matches before capping). */
    keepAll?: boolean;
  } = {}
): Promise<CompanyEventCollectionResult> {
  const safeMax = clampInt(maxResults, 10, 1, 100);
  const safeLookback = clampInt(lookbackDays, 14, 1, 3650);
  const watermark = isoUtc(new Date());
  const { selected: requestedSelection, warnings: sourceWarnings } = normalizeSources(sources, [
    "sec", "company_ir_page", "company_ir",
    "yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases",
    "finnhub", "marketaux",
  ]);
  // A source the operator disabled is never collected (2.16.0); it is reported instead.
  const { active: selected, disabled: disabledSources } = splitDisabledSources(requestedSelection);
  const warnings: Record<string, unknown>[] = [...sourceWarnings, ...disabledSourceWarnings(disabledSources)];
  const items: Record<string, unknown>[] = [];
  const sourcesUsed: string[] = [];
  const sourceDiagnostics: Record<string, unknown> = {};
  // Each source is collected in its own scope, so a fetch the subrequest budget
  // refused is charged to the source that asked for it (2.11.0).
  const budgetStarved = new Set<string>();
  const watched = async <T>(charged: string[], fn: () => Promise<T>): Promise<T> => {
    const { result, starved } = await withBudgetWatch(fn);
    if (starved) for (const source of charged) budgetStarved.add(source);
    return result;
  };
  let eventIdentity: NewsCompanyIdentity | null = null;
  const getEventIdentity = async (): Promise<NewsCompanyIdentity | null> => {
    if (eventIdentity) return eventIdentity;
    try {
      eventIdentity = await watched(["yahoo_finance_identity"], () => resolveNewsCompanyIdentity(ticker));
    } catch {
      eventIdentity = null;
    }
    return eventIdentity;
  };

  if (selected.includes("sec")) {
    const sec = await watched(["sec"], () => collectSecEvents(ticker, secFilingTypes, safeMax, watermark, startDate, endDate, safeLookback));
    if (sec.used) sourcesUsed.push("sec");
    items.push(...sec.items);
    if (sec.noCik) {
      // A ticker SEC has no CIK for is not eligible, as an ineligible Finnhub ticker is (RNEWS2 §8.7).
      sourceDiagnostics.sec = { status: "NOT_ELIGIBLE", attempted: false, reasonCode: null };
      warnings.push({
        code: "SOURCE_NOT_ELIGIBLE",
        message: `SEC has no filings for ${ticker.toUpperCase()} (no SEC CIK).`,
        severity: "warning",
        source: "sec",
        attempted: false,
        reasonCode: null,
      });
    } else {
      warnings.push(...sec.warnings);
    }
  }

  if (selected.includes("company_ir")) {
    const ir = await watched(["company_ir"], () => collectCompanyIrEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback));
    sourceDiagnostics.company_ir = ir.diagnostics;
    if (ir.used) sourcesUsed.push("company_ir");
    items.push(...ir.items);
    warnings.push(...ir.warnings);
  }

  if (selected.includes("company_ir_page")) {
    const page = await watched(["company_ir_page"], () => collectCompanyIrPageEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback));
    sourceDiagnostics.company_ir_page = page.diagnostics;
    if (page.used) sourcesUsed.push("company_ir_page");
    items.push(...page.items);
    warnings.push(...page.warnings);
  }

  // Yahoo Finance news tab (explicit or via legacy yahoo_finance).
  // company_ir is served by the approved company IR registry above; Yahoo
  // sources remain selectable separately as yahoo_finance_news / _press_releases.
  const needYfNews = selected.includes("yahoo_finance_news")
    || selected.includes("yahoo_finance");
  const needYfPr = selected.includes("yahoo_finance_press_releases") || selected.includes("yahoo_finance");
  const needYahooIdentity = needYfNews || needYfPr;
  const yahooIdentity = needYahooIdentity ? yahooNewsIdentityFor(await getEventIdentity()) : null;
  const yahooDiagnostics: Record<string, unknown>[] = [];
  if (yahooIdentity?.status === "UNAVAILABLE") {
    sourceDiagnostics.yahoo_finance_identity = {
      status: "IDENTITY_UNAVAILABLE",
      attempted: true,
      reasonCode: "YAHOO_PROFILE_IDENTITY_UNAVAILABLE",
      allowedAction: "retry_or_use_explicit_ticker_matches_only",
    };
    warnings.push({
      code: "SOURCE_IDENTITY_UNAVAILABLE",
      message: "Yahoo company identity lookup was unavailable; only exact ticker-token matches were retained.",
      severity: "warning",
      source: "yahoo_finance_identity",
    });
  }
  if (needYfNews) {
    const yf = await watched(["yahoo_finance_news", "yahoo_finance"], () => collectYahooEvents(ticker, safeMax, watermark, yahooIdentity!, startDate, endDate, safeLookback, "news"));
    const diagnostic = { ...yf.diagnostics, attempted: true, completed: yf.used };
    sourceDiagnostics.yahoo_finance_news = diagnostic;
    yahooDiagnostics.push(diagnostic);
    if (yf.used) {
      if (selected.includes("yahoo_finance_news")) sourcesUsed.push("yahoo_finance_news");
      if (selected.includes("yahoo_finance") && !sourcesUsed.includes("yahoo_finance")) sourcesUsed.push("yahoo_finance");
    }
    for (const item of yf.items) {
      const src = _str(item.source);
      if (selected.includes("yahoo_finance_news") && src === "yahoo_finance_news") items.push(item);
      else if (selected.includes("yahoo_finance") && ["yahoo_finance_news", "yahoo_finance_press_releases"].includes(src)) items.push(item);
    }
    warnings.push(...yf.warnings);
  }

  // Yahoo Finance press releases tab (explicit or via legacy yahoo_finance)
  if (needYfPr) {
    const pr = await watched(["yahoo_finance_press_releases", "yahoo_finance"], () => collectYahooEvents(ticker, safeMax, watermark, yahooIdentity!, startDate, endDate, safeLookback, "press_releases"));
    const diagnostic = { ...pr.diagnostics, attempted: true, completed: pr.used };
    sourceDiagnostics.yahoo_finance_press_releases = diagnostic;
    yahooDiagnostics.push(diagnostic);
    if (pr.used && selected.includes("yahoo_finance_press_releases") && !sourcesUsed.includes("yahoo_finance_press_releases")) {
      sourcesUsed.push("yahoo_finance_press_releases");
    }
    items.push(...pr.items);
    warnings.push(...pr.warnings);
  }

  if (selected.includes("yahoo_finance") && yahooDiagnostics.length) {
    // Both passes read the same Yahoo feed, so the legacy aggregate counts it once (2.17.0): the news pass accepts every
    // row the press-release pass accepts and rejects the rest for the same reasons, so its counts are the feed's. A
    // failed news pass falls back to the press-release pass's counts.
    const feedRead = yahooDiagnostics.find(diagnostic => diagnostic.completed === true) ?? yahooDiagnostics[0];
    sourceDiagnostics.yahoo_finance = {
      rawCount: Number(feedRead.rawCount ?? 0),
      retrievedCount: Number(feedRead.retrievedCount ?? 0),
      filteredCount: Number(feedRead.filteredCount ?? 0),
      acceptedCount: Number(feedRead.acceptedCount ?? 0),
      rejectedCount: Number(feedRead.rejectedCount ?? 0),
      rejectionCounts: { ...((feedRead.rejectionCounts as Record<string, number>) ?? {}) },
      identityStatus: yahooIdentity?.status ?? "UNAVAILABLE",
      attempted: true,
      completed: yahooDiagnostics.every(diagnostic => diagnostic.completed === true),
    };
  }

  if (selected.includes("finnhub") && finnhubEligibility(ticker).eligible) {
    const finnhub = await watched(["finnhub"], () => collectFinnhubEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback));
    sourceDiagnostics.finnhub = finnhub.diagnostics;
    if (finnhub.used) sourcesUsed.push("finnhub");
    items.push(...finnhub.items);
    warnings.push(...finnhub.warnings);
  } else if (selected.includes("finnhub")) {
    const eligibility = finnhubEligibility(ticker);
    sourceDiagnostics.finnhub = {
      status: "NOT_ELIGIBLE",
      attempted: false,
      reasonCode: eligibility.reasonCode,
      allowedAction: "use_yahoo_or_official_sources",
    };
    warnings.push({
      code: "SOURCE_NOT_ELIGIBLE",
      message: `Finnhub company-news is intentionally skipped for ${ticker.toUpperCase()} under the deployed market capability policy.`,
      severity: "warning",
      source: "finnhub",
      attempted: false,
      reasonCode: eligibility.reasonCode,
    });
  }

  if (selected.includes("marketaux")) {
    // The requested selection, disabled sources included: a disabled finnhub counts as limited coverage (RNEWS2 §8.3).
    const fallbackReason = marketauxFallbackReason(requestedSelection, items, warnings, sourceDiagnostics, searchQuery);
    if (fallbackReason === null) {
      sourceDiagnostics.marketaux = {
        status: "NOT_NEEDED",
        attempted: false,
        completed: true,
        reasonCode: "HIGHER_PRIORITY_EVIDENCE_FOUND",
        fallbackReason: null,
        domains: [...MARKETAUX_WIRE_DOMAINS],
        quotaPolicy: {
          dailyLimit: 100,
          maxArticlesPerRequest: MARKETAUX_MAX_ITEMS,
          pagesRequested: 0,
          automaticPagination: false,
        },
      };
    } else {
      const marketaux = await watched(["marketaux"], () => collectMarketauxEvents(
        ticker,
        safeMax,
        watermark,
        startDate,
        endDate,
        safeLookback,
        fallbackReason,
        searchQuery,
        yahooIdentity?.exchange ?? null,
      ));
      sourceDiagnostics.marketaux = marketaux.diagnostics;
      if (marketaux.used) sourcesUsed.push("marketaux");
      items.push(...marketaux.items);
      warnings.push(...marketaux.warnings);
    }
  }

  // GlobeNewswire RSS is an explicit shallow snapshot and intentionally runs
  // after bounded API sources so cold feed latency cannot delay them.
  if (selected.includes("newswire")) {
    const identity = await getEventIdentity();
    const gnw = await watched(["newswire"], () => collectGlobeNewswireEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback, identity?.exchange ?? null));
    if (gnw.used) sourcesUsed.push("newswire");
    items.push(...gnw.items);
    warnings.push(...gnw.warnings);
  }

  const allDeduped = dedupeEventItems(items, warnings);
  const deduped = (keepAll ? allDeduped : allDeduped.slice(0, safeMax)).map(enrichNewsItemForLlm);
  const uniqueWarnings: Record<string, unknown>[] = [];
  const warningKeys = new Set<string>();
  for (const w of warnings) {
    const key = `${_str(w.code)}|${_str(w.message)}`;
    if (warningKeys.has(key)) continue;
    warningKeys.add(key);
    uniqueWarnings.push(w);
  }
  return { items: deduped, sourcesUsed, warnings: uniqueWarnings, watermark, sourceDiagnostics, budgetStarvedSources: [...budgetStarved], disabledSources };
}
// ─── Public event / news tools ─────────────────────────────────────────────────

const NEWS_RELEVANCE_RANK: Record<string, number> = { HIGH: 0, MEDIUM: 1, LOW: 2 };
/** Items ordered by tickerRelevance (HIGH, MEDIUM, LOW), keeping their order (newest first) within each level. */
export function rankNewsItemsByRelevance(items: Record<string, unknown>[]): Record<string, unknown>[] {
  const rank = (item: Record<string, unknown>): number => NEWS_RELEVANCE_RANK[_str(item.tickerRelevance).toUpperCase()] ?? 1;
  return items.map((item, index) => ({ item, index }))
    .sort((a, b) => rank(a.item) - rank(b.item) || a.index - b.index)
    .map(({ item }) => item);
}
export async function getCompanyNews(
  ticker: string | string[],
  maxResults = 10,
  lookbackDays = 14,
  requestedSources: string[] | null = null,
): Promise<string> {
  const sources = requestedSources ?? DEFAULT_NEWS_SOURCES;
  if (
    (typeof ticker === "string" && ticker.trim() === "")
    || (Array.isArray(ticker) && (ticker.length === 0 || ticker.some((item) => typeof item !== "string" || item.trim() === "")))
  ) {
    return JSON.stringify({
      error: true,
      code: "INPUT_VALIDATION_ERROR",
      message: "ticker is required and must not contain empty symbols.",
    });
  }
  ticker = Array.isArray(ticker) ? ticker.map((item) => item.trim()) : ticker.trim();
  // Batch path: fetch each ticker independently and return a per-ticker keyed
  // object (a union of results), matching the other multi-ticker tools. News is
  // fetched per ticker; there is no combined query that could zero out the
  // whole batch under low-news conditions.
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getCompanyNews(t, maxResults, lookbackDays, requestedSources), tickerCap("get_company_news"));
  }
  // Collect a wider pool, then keep the most relevant items: an item that
  // only mentions the company in passing should not displace one about it.
  const safeMax = clampInt(maxResults, 10, 1, 100);
  const out = await collectCompanyEvents(ticker, { maxResults: Math.min(100, safeMax * 3), lookbackDays, sources });
  out.items = rankNewsItemsByRelevance(out.items).slice(0, safeMax);
  const status = collectionStatus(out.items, out.sourcesUsed, out.warnings);
  const sourceStatus = computeSourceStatus(out.sourcesUsed, out.warnings, out.items, sources, out.sourceDiagnostics, out.budgetStarvedSources, out.disabledSources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    items: out.items,
    meta: { sourcesUsed: out.sourcesUsed, deduped: true, watermark: out.watermark },
    warnings: out.warnings,
    sourceCoverage,
    coverage,
    sourceStatus,
    ...sourceRequestFields(requestedSources, sourceStatus),
  };
  if (status) payload.status = status;
  return JSON.stringify(markNoSourceSelected(payload, requestedSources));
}
export async function searchCompanyNews(
  ticker: string,
  query: string,
  startDate = "",
  endDate = "",
  requestedSources: string[] | null = null,
  maxResults = 10
): Promise<string> {
  const sources = requestedSources ?? DEFAULT_NEWS_SOURCES;
  // The query filters the full collected pool; filtering only the first
  // max_results items found almost nothing.
  const out = await collectCompanyEvents(ticker, {
    maxResults: 100,
    lookbackDays: 14,
    startDate,
    endDate,
    sources,
    searchQuery: query,
  });
  const q = query.toLowerCase().trim();
  const matchedAll = q
    ? out.items.filter(item => `${_str(item.title)} ${_str(item.summary)} ${_str(item.source)} ${_str(item.eventType)} ${_str(item.evidenceText)}`.toLowerCase().includes(q))
    : out.items;
  const matched = rankNewsItemsByRelevance(matchedAll).slice(0, clampInt(maxResults, 10, 1, 100));
  const status = collectionStatus(matched, out.sourcesUsed, out.warnings);
  const sourceStatus = computeSourceStatus(out.sourcesUsed, out.warnings, matched, sources, out.sourceDiagnostics, out.budgetStarvedSources, out.disabledSources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    query,
    matchCount: matchedAll.length,
    items: matched,
    meta: { sourcesUsed: out.sourcesUsed, deduped: true, watermark: out.watermark },
    warnings: out.warnings,
    sourceCoverage,
    coverage,
    sourceStatus,
    ...sourceRequestFields(requestedSources, sourceStatus),
  };
  if (status) payload.status = status;
  return JSON.stringify(markNoSourceSelected(payload, requestedSources));
}
export async function getCompanyPressReleases(
  ticker: string,
  lookbackDays = 90,
  maxResults = 20,
  requestedSources: string[] | null = null
): Promise<string> {
  const sources = requestedSources ?? DEFAULT_PRESS_RELEASE_SOURCES;
  const safeMax = clampInt(maxResults, 20, 1, 100);
  const safeLookback = clampInt(lookbackDays, 90, 1, 3650);
  const { selected: requestedSelection, warnings: sourceWarnings } = normalizeSources(sources, ["sec", "company_ir_page", "yahoo_finance_press_releases"]);
  // Disabled sources are dropped here, so the inner collections never see them; the report is attached once below (2.16.0).
  const { active: selected, disabled: disabledSources } = splitDisabledSources(requestedSelection);
  const primarySources = selected.filter(src => src === "sec" || src === "company_ir_page");
  const optionalSources = selected.filter(src => src !== "sec" && src !== "company_ir_page");
  const selectedHasCompanyIr = selected.includes("company_ir");
  const emptyCollection = (): CompanyEventCollectionResult => ({
    items: [],
    sourcesUsed: [],
    warnings: [],
    watermark: isoUtc(new Date()),
    sourceDiagnostics: {},
  });
  const primary = primarySources.length > 0
    ? await collectCompanyEvents(ticker, {
      maxResults: safeMax,
      lookbackDays: safeLookback,
      sources: primarySources,
      secFilingTypes: ["8-K"],
    })
    : emptyCollection();
  const releaseTypes = new Set(["company_ir", "company_ir_page", "press_release", "newswire", "marketaux_wire", "sec_filing", "sec_ex99_found", "yahoo_finance_press_releases"]);
  const primaryItems = primary.items.filter(it => releaseTypes.has(_str(it.sourceType)));

  let hasSecEx99Found = false;
  const sec8kEvidence: Record<string, unknown>[] = [];
  let sec8kWithoutEx99Count = 0;
  const processedPrimaryItems: Record<string, unknown>[] = [];
  for (const it of primaryItems) {
    if (_str(it.sourceType) === "sec_filing" && _str(it.filingType) === "8-K") {
      const accession = _str(it.accessionNumber);
      const url = _str(it.url);
      const evidence: Record<string, unknown> = {
        filingType: "8-K",
        filingDate: it.filingDate ?? null,
        acceptedAt: it.acceptedAt ?? null,
        accessionNumber: accession || null,
        cikInt: it.cikInt ?? null,
        documentUrl: url || null,
      };
      sec8kEvidence.push(evidence);
      const cikFromItem = Number.parseInt(_str(it.cikInt), 10);
      const cikFromAccession = accession ? edgarCikFromAccession(accession) : null;
      const cikMatch = /\/data\/(\d+)\//.exec(url);
      const cikFromUrl = cikMatch ? Number.parseInt(cikMatch[1], 10) : null;
      // The accession prefix names the filer agent, so it comes last.
      const cik = [cikFromItem, cikFromUrl, cikFromAccession]
        .find((value): value is number => typeof value === "number" && Number.isFinite(value) && value > 0) ?? null;
      if (accession && cik !== null) {
        const ex991Url = await resolveEx991Url(cik, accession);
        if (ex991Url) {
          hasSecEx99Found = true;
          evidence.ex991Url = ex991Url;
          evidence.ex991Resolved = true;
          processedPrimaryItems.push({
            ...it,
            sourceType: "sec_ex99_found",
            url: ex991Url,
            title: "EX-99.1 exhibit found in 8-K",
            eventType: "press_release",
            evidenceText: "Resolved EX-99.1 press-release exhibit from SEC 8-K.",
            confidence: "HIGH",
            decisionUse: sourceDecisionUse("sec_ex99_found"),
          });
          continue;
        }
      }
      sec8kWithoutEx99Count += 1;
    }
    processedPrimaryItems.push(it);
  }

  const supplemental = optionalSources.length > 0
    ? await collectCompanyEvents(ticker, {
      maxResults: safeMax,
      lookbackDays: safeLookback,
      sources: optionalSources,
      secFilingTypes: ["8-K"],
    })
    : emptyCollection();
  const warnings = [...sourceWarnings, ...disabledSourceWarnings(disabledSources), ...primary.warnings, ...supplemental.warnings];
  const warningKeys = new Set<string>();
  const uniqueWarnings: Record<string, unknown>[] = [];
  for (const w of warnings) {
    const key = `${_str(w.code)}|${_str(w.message)}|${_str(w.feedUrl)}|${_str(w.canonicalUrl)}`;
    if (warningKeys.has(key)) continue;
    warningKeys.add(key);
    uniqueWarnings.push(w);
  }
  const supplementalItems = supplemental.items.filter(it => releaseTypes.has(_str(it.sourceType)));
  const processedItems = dedupeEventItems([...processedPrimaryItems, ...supplementalItems], uniqueWarnings);
  const sourcesUsed = [...new Set([...primary.sourcesUsed, ...supplemental.sourcesUsed])];
  const sourceDiagnostics = { ...primary.sourceDiagnostics, ...supplemental.sourceDiagnostics };
  const hasApprovedIrPageResolved = processedItems.some(it =>
    _str(it.sourceType) === "company_ir_page" && _str(it.approvalStatus) === "approved" && !!_str(it.url)
  );
  const irPageEvidence = processedItems
    .filter(it => _str(it.sourceType) === "company_ir_page" && _str(it.approvalStatus) === "approved")
    .slice(0, 10)
    .map(it => ({
      title: it.title ?? null,
      publishedAt: it.publishedAt ?? null,
      url: it.url ?? null,
      canonicalUrl: it.canonicalUrl ?? null,
      adapter: it.adapter ?? null,
      registryVersion: it.registryVersion ?? null,
      basis: "approved_company_ir_page_registry",
    }));
  const companyIrPageStatus = sourceDiagnostics.company_ir_page && typeof sourceDiagnostics.company_ir_page === "object"
    ? sourceDiagnostics.company_ir_page as Record<string, unknown>
    : {};
  const candidateSources = Array.isArray(companyIrPageStatus.candidates) ? companyIrPageStatus.candidates : [];

  if (processedItems.length === 0) {
    uniqueWarnings.push({
      code: "NO_OFFICIAL_RELEASE_SOURCE",
      message: "No company-originated or official release source found in requested window.",
      severity: "warning",
    });
  }
  if (!hasSecEx99Found && sec8kWithoutEx99Count > 0) {
    uniqueWarnings.push({
      code: "SEC_8K_FOUND_EX99_NOT_FOUND",
      message: "SEC 8-K filing(s) were found, but no EX-99.1 press-release exhibit was resolved.",
      severity: "warning",
      filingsSearched: sec8kWithoutEx99Count,
    });
  }

  let status: string | null = null;
  if (hasSecEx99Found) {
    status = "SEC_EX99_FOUND";
  } else if (hasApprovedIrPageResolved) {
    status = "APPROVED_IR_PAGE_FOUND";
  } else if (sec8kWithoutEx99Count > 0) {
    status = "SEC_8K_FOUND_EX99_NOT_FOUND";
  } else if (processedItems.length === 0) {
    if (selected.includes("yahoo_finance_press_releases")) {
      status = "NO_YAHOO_PRESS_RELEASE";
    } else if (selected.includes("company_ir_page")) {
      status = candidateSources.length > 0 ? "COMPANY_IR_PAGE_CANDIDATE_ONLY" : "COMPANY_IR_PAGE_NOT_APPROVED";
    } else if (selectedHasCompanyIr) {
      status = "COMPANY_IR_NOT_FOUND";
    } else {
      // A disabled source never yields NOT_FOUND: the source-limited path applies (2.16.0).
      status = disabledSources.length > 0 ? collectionStatus(processedItems, sourcesUsed, uniqueWarnings) : "NOT_FOUND";
    }
  } else {
    status = collectionStatus(processedItems, sourcesUsed, uniqueWarnings);
  }

  const coverageStatus = hasSecEx99Found ? "SEC_EX99_RESOLVED"
    : hasApprovedIrPageResolved ? "APPROVED_IR_PAGE_RESOLVED"
    : sec8kWithoutEx99Count > 0 ? "SEC_8K_FOUND_EX99_NOT_FOUND"
    : processedItems.length > 0 ? "OFFICIAL_RELEASE_SOURCE_FOUND"
    : "NO_OFFICIAL_RELEASE_SOURCE";
  const payloadDecisionGrade = hasSecEx99Found || hasApprovedIrPageResolved;
  const decisionGradeBasis = payloadDecisionGrade
    ? hasSecEx99Found
      ? "Resolved SEC 8-K EX-99 press-release exhibit for this call."
      : "Resolved an approved, registry-reviewed company IR page source for this call."
    : "No resolved SEC EX-99 or approved IR-page evidence in this call; use for verification/context only.";
  const budgetStarvedSources = [...new Set([...(primary.budgetStarvedSources ?? []), ...(supplemental.budgetStarvedSources ?? [])])];
  const sourceStatus = computeSourceStatus(sourcesUsed, uniqueWarnings, processedItems, requestedSelection, sourceDiagnostics, budgetStarvedSources, disabledSources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus, payloadDecisionGrade ? "USE_OFFICIAL_EVIDENCE" : null);
  // An SEC outage keeps the status; the failure mode comes from the failed SEC row, so the empty result is not read as
  // absence (2.19.0, call #9). sourceStatus.sec already makes coverage PARTIAL.
  const secRow = sourceStatus.sec && typeof sourceStatus.sec === "object" ? sourceStatus.sec as Record<string, unknown> : null;
  const secOutageMode = secRow && SEC_OUTAGE_STATUSES.has(_str(secRow.status)) ? verifyCoverageFailureMode({ sec: secRow }, []) : null;

  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    items: processedItems.slice(0, safeMax),
    meta: { sourcesUsed, deduped: true, watermark: primary.watermark || supplemental.watermark },
    warnings: uniqueWarnings,
    sourceCoverage,
    coverage,
    sourceStatus,
    ...sourceRequestFields(requestedSources, sourceStatus),
    coverageStatus,
    decisionGrade: payloadDecisionGrade,
    decisionGradeBasis,
    capabilityStatus: "ACTIVE",
    doctrineUse: "ALLOWED",
    reliabilityClass: reliabilityClassFor("ALLOWED"),
    failureMode: payloadDecisionGrade ? null : (secOutageMode ?? coverageStatus),
  };
  if (sec8kEvidence.length > 0) payload.secEvidence = sec8kEvidence.slice(0, 10);
  if (irPageEvidence.length > 0) payload.irPageEvidence = irPageEvidence;
  if (candidateSources.length > 0) payload.candidateSources = candidateSources;
  if (status) payload.status = status;
  return JSON.stringify(markNoSourceSelected(payload, requestedSources));
}
/** SEC row statuses that are an outage (not absence, not ineligibility, not an operator switch-off). */
const SEC_OUTAGE_STATUSES = new Set(["PROVIDER_ERROR", "PROVIDER_BUDGET_EXHAUSTED", "RATE_LIMITED", "TIMEOUT", "AUTH_ERROR", "PROVIDER_CHANGED"]);
export async function getSecRecentEvents(
  ticker: string,
  filingTypes: string[] = ["8-K", "10-Q", "10-K"],
  lookbackDays = 90,
  maxResults = 20
): Promise<string> {
  // [] means the default forms and a list of only blank types is rejected (2.19.0, B-G8; Python behaviour).
  const selectedTypes = (filingTypes.length > 0 ? filingTypes : ["8-K", "10-Q", "10-K"]).filter(ft => String(ft).trim() !== "");
  if (selectedTypes.length === 0) {
    return mcpFailure("get_sec_recent_events", ErrorCode.INPUT_VALIDATION_ERROR, "filing_types must not be empty");
  }
  const watermark = isoUtc(new Date());
  const { result: sec, starved } = await withBudgetWatch(() => collectSecEvents(ticker, selectedTypes, clampInt(maxResults, 20, 1, 100), watermark, "", "", clampInt(lookbackDays, 90, 1, 3650)));
  const status = collectionStatus(sec.items, sec.used ? ["sec"] : [], sec.warnings);
  const sourceStatus = markBudgetStarvedSources(
    { sec: { status: sec.used ? (sec.items.length ? "OK" : "EMPTY_RESULT") : "PROVIDER_ERROR", rawCount: sec.items.length, filteredCount: sec.items.length } },
    starved ? ["sec"] : [],
  );
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    items: sec.items.map(enrichNewsItemForLlm),
    meta: { sourcesUsed: sec.used ? ["sec"] : [], watermark },
    warnings: sec.warnings,
    sourceCoverage,
    coverage,
    sourceStatus,
  };
  if (status) payload.status = status;
  return JSON.stringify(payload);
}
export async function getPublicEventTimeline(
  ticker: string,
  startDate = "",
  endDate = "",
  requestedSources: string[] | null = null,
  maxResults = 50,
  newestFirst = false
): Promise<string> {
  const sources = requestedSources ?? DEFAULT_EVENT_SOURCES;
  const out = await collectCompanyEvents(ticker, { maxResults, lookbackDays: 365, startDate, endDate, sources });
  const timeline = out.items
    .filter(item => _str(item.publishedAt))
    .map(item => ({
      timestamp: item.publishedAt,
      eventType: item.eventType,
      title: item.title,
      source: item.source,
      sourceType: item.sourceType,
      url: item.url,
      confidence: item.confidence,
      evidenceClass: item.evidenceClass,
      tickerMatch: item.tickerMatch,
      matchBasis: item.matchBasis ?? null,
      urlProvenance: item.urlProvenance,
      decisionUse: item.decisionUse ?? null,
      duplicateGroupId: item.duplicateGroupId,
      sourceRefs: item.sourceRefs ?? [],
    }))
    .sort((a, b) => newestFirst
      ? _str(b.timestamp).localeCompare(_str(a.timestamp))
      : _str(a.timestamp).localeCompare(_str(b.timestamp)))
    .slice(0, clampInt(maxResults, 50, 1, 100));
  const status = collectionStatus(out.items, out.sourcesUsed, out.warnings);
  const sourceStatus = computeSourceStatus(out.sourcesUsed, out.warnings, out.items, sources, out.sourceDiagnostics, out.budgetStarvedSources, out.disabledSources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    timeline,
    meta: { sourcesUsed: out.sourcesUsed, deduped: true, watermark: out.watermark },
    warnings: out.warnings,
    sourceCoverage,
    coverage,
    sourceStatus,
    ...sourceRequestFields(requestedSources, sourceStatus),
  };
  if (status) payload.status = status;
  return JSON.stringify(markNoSourceSelected(payload, requestedSources));
}
export async function verifyCompanyEvent(
  ticker: string,
  eventQuery: string,
  startDate = "",
  endDate = "",
  requestedSources: string[] | null = null
): Promise<string> {
  const sources = requestedSources ?? DEFAULT_EVENT_SOURCES;
  const normalizeEventText = (value: unknown): string =>
    _str(value).toLowerCase().replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim();
  const eventQueryStopwords = new Set([
    "announce", "announced", "announcement", "announcements", "announces",
    "company", "corporate", "latest", "news", "official", "press", "release",
    "report", "reported", "reports", "result", "results", "today", "update",
    "updated", "updates",
  ]);
  const normalizedQuery = normalizeEventText(eventQuery);
  const meaningfulTerms = normalizedQuery
    .split(" ")
    .filter(term => term.length >= 3 && !eventQueryStopwords.has(term));
  const requiredTermMatches = meaningfulTerms.length <= 2
    ? meaningfulTerms.length
    : Math.ceil(meaningfulTerms.length * 0.67);
  const queryMatch = (item: Record<string, unknown>): {
    matched: boolean;
    method: "PHRASE" | "TERM_THRESHOLD" | "NONE";
    matchedTerms: string[];
    requiredTermMatches: number;
  } => {
    const text = normalizeEventText(`${_str(item.title)} ${_str(item.summary)} ${_str(item.evidenceText)} ${_str(item.eventType)} ${_str(item.source)}`);
    // Stemmed, so "launch" matches "launches" and "launched" (extraction-rules.ts).
    const words = new Set(text.split(" ").filter(Boolean).map(stemWord));
    const matchedTerms = meaningfulTerms.filter(term => words.has(stemWord(term)));
    if (normalizedQuery && text.includes(normalizedQuery)) {
      return { matched: true, method: "PHRASE", matchedTerms, requiredTermMatches };
    }
    if (requiredTermMatches > 0 && matchedTerms.length >= requiredTermMatches) {
      return { matched: true, method: "TERM_THRESHOLD", matchedTerms, requiredTermMatches };
    }
    return { matched: false, method: "NONE", matchedTerms, requiredTermMatches };
  };
  // A query with no meaningful terms can verify nothing, and NOT_FOUND would claim absence (2.19.0, call #12).
  if (meaningfulTerms.length === 0) {
    return mcpFailure(
      "verify_company_event",
      ErrorCode.INPUT_VALIDATION_ERROR,
      "The event query contains no specific event terms. Add a term such as acquisition, dividend, guidance, contract, or offering.",
    );
  }
  // Match against everything collected: a cap of the newest 50 let a flood of
  // aggregator items push the official release out before matching (ASTS
  // BlueBird launch, 2.4.5). The 365-day window is collected whatever the range, which applies only when classifying,
  // so a match outside the range can still read STALE (2.19.0, A-G8; Python behaviour).
  const out = await collectCompanyEvents(ticker, { maxResults: 50, lookbackDays: 365, sources, keepAll: true });
  const matched = out.items.filter(item => queryMatch(item).matched);
  const staleCutoff = new Date(Date.now() - 90 * 86400000).toISOString().slice(0, 10);
  // Without a range a match older than 90 days is not in range, so an old release reads STALE, not CONFIRMED (2.19.0, call #11).
  // An undated match stays in range there, as before.
  const inRange = matched.filter(item => {
    if (!startDate && !endDate) {
      const day = _str(item.publishedAt).slice(0, 10);
      return !day || day >= staleCutoff;
    }
    return withinDateWindow(_str(item.publishedAt) || null, startDate, endDate);
  });
  const official = inRange.filter(item =>
    OFFICIAL_SOURCE_TYPES.has(_str(item.sourceType)) &&
    !!_str(item.url) &&
    ["HIGH", "MEDIUM"].includes(confidenceForSourceType(item.sourceType, item.confidence))
  );
  const staleOnly = matched.length > 0 && matched.every(item => _str(item.publishedAt).slice(0, 10) < staleCutoff);
  const matchedGroupIds = new Set(matched.map(item => _str(item.duplicateGroupId)).filter(Boolean));
  const conflicts: Record<string, unknown>[] = out.warnings
    .filter(w => _str(w.code) === "EVIDENCE_CONFLICT" && matchedGroupIds.has(_str(w.duplicateGroupId)))
    .map(w => ({
      type: "evidence",
      duplicateGroupId: _str(w.duplicateGroupId),
      message: _str(w.message, "Conflicting evidence observed for the matched event."),
    }));
  let status = "NOT_FOUND";
  if (conflicts.length > 0) status = "CONFLICTING";
  else if (official.length > 0) status = "CONFIRMED";
  else if (inRange.length > 0) status = "PARTIAL";
  else if (staleOnly) status = "STALE";

  // Stronger evidence first, then newest: a LOW-confidence aggregator item
  // never outranks a press release that matched the same query.
  const pool = official.length > 0 ? official : (inRange.length > 0 ? inRange : matched);
  const tickerUpper = ticker.toUpperCase();
  // Issuer names score relevance only, so no evidence means no lookup. The name set is Yahoo longName and shortName plus the
  // SEC submissions name, without any name equal to the ticker (2.19.0, planner decision 6; the call #16 rule).
  const companyNames: string[] = [];
  if (pool.length > 0) {
    const addName = (value: unknown): void => {
      const name = _str(value).trim();
      if (name && name.toLowerCase() !== ticker.toLowerCase() && !companyNames.includes(name)) companyNames.push(name);
    };
    try {
      const priceD = await yGet(
        `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=price`
      ) as Record<string, unknown>;
      const priceResult = ((priceD?.quoteSummary as Record<string, unknown> | undefined)
        ?.result as Record<string, unknown>[] | undefined)?.[0]?.price as Record<string, unknown> | undefined;
      addName(priceResult?.longName);
      addName(priceResult?.shortName);
    } catch { /* non-fatal */ }
    try {
      const { submissions } = await getSubmissionsForTicker(ticker);
      addName(submissions?.name);
    } catch { /* non-fatal */ }
  }

  const extractBaseCompanyName = (name: string): string => {
    if (!name) return "";
    let nameClean = name.toUpperCase();
    const suffixes = [
      " INC", " CORP", " CORPORATION", " LTD", " LIMITED", " PLC",
      " CO", " COMPANY", " S.A.", " AG", " GMBH", " S.A.B. DE C.V.",
      " GROUP", " HOLDINGS", " TRUST"
    ];
    for (const p of [".", ",", "/"]) {
      nameClean = nameClean.split(p).join("");
    }
    for (const suff of suffixes) {
      if (nameClean.endsWith(suff)) {
        nameClean = nameClean.slice(0, -suff.length);
      }
    }
    return nameClean.trim();
  };

  const nameTests = companyNames.flatMap(name => [extractBaseCompanyName(name), name])
    .map(name => name.toLowerCase())
    .filter(Boolean);
  const tickerPattern = new RegExp(`\\b${tickerUpper}\\b`, "i");

  // Relevance is scored from the evidence text and issuer, never from the item's tickers list: every collector
  // injects the queried ticker there, which made LOW_RELEVANCE_EVIDENCE unreachable (2.17.0).
  const relevanceScore = (ev: Record<string, unknown>): string => {
    // 1. Check source type
    const sourceType = _str(ev.sourceType);
    if (["sec_filing", "sec", "company_ir", "company_ir_page", "sec_ex99_found"].includes(sourceType)) {
      return "HIGH";
    }

    // 2. Check text content
    const hay = `${_str(ev.title)} ${_str(ev.summary)} ${_str(ev.evidenceText)} ${_str(ev.issuer)}`.toLowerCase();
    if (tickerPattern.test(hay)) {
      return "HIGH";
    }

    if (nameTests.some(name => hay.includes(name))) {
      return "HIGH";
    }

    // Check issuer field explicitly
    if (tickerInText(_str(ev.issuer), tickerUpper)) {
      return "HIGH";
    }

    return "LOW";
  };

  const best = rankEvidence(pool, (ev) => confidenceForSourceType(ev.sourceType, ev.confidence)).slice(0, 5).map(ev => {
    const relevance = relevanceScore(ev);
    const match = queryMatch(ev);
    return {
      source: ev.source,
      sourceType: ev.sourceType,
      publishedAt: ev.publishedAt,
      retrievedAt: ev.retrievedAt,
      url: ev.url,
      confidence: confidenceForSourceType(ev.sourceType, ev.confidence),
      relevance,
      entityRelevance: relevance,
      queryMatch: {
        method: match.method,
        matchedTerms: match.matchedTerms,
        requiredTermMatches: match.requiredTermMatches,
      },
      evidenceText: shortText(ev.evidenceText || ev.summary || ev.title, 180),
    };
  });

  // If all best evidence is LOW relevance, downgrade status
  if (best.length > 0 && best.every(e => e.relevance === "LOW")) {
    if (status === "CONFIRMED") {
      status = "PARTIAL";
      out.warnings.push({
        code: "LOW_RELEVANCE_EVIDENCE",
        message: `Evidence found but none contain word-boundary match for ticker '${tickerUpper}'. Confidence downgraded.`,
        severity: "warning",
      });
    }
  }
  const sourceStatus = computeSourceStatus(out.sourcesUsed, out.warnings, out.items, sources, out.sourceDiagnostics, out.budgetStarvedSources, out.disabledSources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus);
  const companyIrRequested = requestedSources !== null && normalizeSources(requestedSources, []).selected.includes("company_ir");
  const coverageFailureMode = verifyCoverageFailureMode(sourceStatus, out.warnings, companyIrRequested);
  if (status === "NOT_FOUND" && coverageFailureMode) {
    status = "SOURCE_LIMITED_NOT_FOUND";
    out.warnings.push({
      code: "SOURCE_LIMITED_NOT_FOUND",
      message: "One or more selected sources were unavailable; this is not confirmed evidence that the event did not occur.",
      severity: "warning",
      failureMode: coverageFailureMode,
    });
  }
  const failureMode = status === "SOURCE_LIMITED_NOT_FOUND" ? coverageFailureMode : null;
  const retryable = ["WORKER_SUBREQUEST_LIMIT", "RATE_LIMITED", "PROVIDER_TIMEOUT"].includes(_str(failureMode));
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    query: eventQuery,
    queryPolicy: {
      meaningfulTerms,
      requiredTermMatches,
      matchedEvidenceCount: matched.length,
    },
    status,
    bestEvidence: best,
    conflicts,
    meta: { sourcesChecked: out.sourcesUsed, watermark: out.watermark },
    warnings: out.warnings,
    sourceCoverage,
    coverage,
    sourceStatus,
    ...sourceRequestFields(requestedSources, sourceStatus),
    failureMode,
    retryable,
    recommendedAction: failureMode
      ? "Inspect sourceStatus and retry with a narrower sources list that excludes failed providers; do not treat this result as confirmed absence."
      : null,
  };
  return JSON.stringify(markNoSourceSelected(payload, requestedSources));
}
