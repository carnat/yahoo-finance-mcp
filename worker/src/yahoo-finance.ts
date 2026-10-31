import { BoundedTtlCache, setBounded } from "./cache.js";
import { budgetRefusalCount, countCacheEvent, countProviderRetry, providerFetch } from "./request-context.js";
import { ErrorCode, getWorkerVar, mcpFailure } from "./response.js";
import { yahooTranscriptContentSha256 } from "./transcript-contract.js";
import { allowedHostKind, approvedRegistryUrlAllowed, checkFetchUrl } from "./url-policy.js";
import { companiesHouseFilings, pickCompaniesHouseMatch } from "./capital-structure.js";
import { extractEarningsPeriodFromText, type EarningsPeriodInfo } from "./earnings-period.js";
import { REVENUE_CONCEPTS } from "./sec-facts.js";
import { guidanceHistory, parseAmount, periodForExcerpt, unitOf as guidanceUnitOf, type ReleaseText } from "./guidance-history.js";
import { guidanceRanges, releaseTextMetric, type ReleaseMetricName } from "./extraction-rules.js";
import { adsRatio, majorPrice, valuationSnapshot, type MarketInputs } from "./valuation.js";
import { coverReadsNeeded, coverShareCounts, foreignFiler, historicalValuation, latestShareCount, peerMedians, reportingCurrencies, taxonomyOf, valuationDates, type Bar, type PeriodicFiling, type Split } from "./valuation-history.js";
import { annualFilingsFromSubmissions, latestAnnualCoverageWarning } from "./companyfacts-coverage.js";
import { DEFAULT_TOLERANCE_PCT, METRICS as RECONCILE_METRICS, RECONCILE_PERIOD_HELP, isReconcilePeriod, metricReconciliation, resolvePeriod } from "./metric-reconciliation.js";
import { AUTHORITY_BOUNDARY } from "./evidence.js";
import {
  edgarBuildFilingUrls,
  edgarCikFromAccession,
  edgarFetchPublicHtml,
  edgarGetHtml,
  edgarGetJson,
  edgarListExhibitsFromIndex,
  getEdgarTickerCikMap,
  getSubmissionsForTicker,
  resolveCikForTicker,
  resolveEx991Url,
} from "./clients/edgar.js";
import {
  SEC_ARCHIVES_PREFIX,
  secArchiveDocumentUrl,
  secCompanyConceptUrl,
  secCompanyFactsUrl,
  yahooChartUrl,
  yahooQuoteTypeUrl,
} from "./clients/endpoints.js";
// Phase 4 unit 1: the EDGAR client moved to clients/edgar.ts; these re-exports keep tools.ts, index.ts,
// evidence-pack.ts and the parity bundle (m.yahooFinance.*) unchanged.
export {
  edgarCikFromAccession,
  edgarGetHtml,
  edgarTickerMapLoaded,
  getSubmissionsForTicker,
  resolveEx991Url,
  secDocumentCacheStats,
  setTickerCikFixturesForTests,
} from "./clients/edgar.js";
import {
  deleteProviderCache,
  fetchProviderWithTimeout,
  getProviderCache,
  providerCacheKey,
  providerCacheName,
  providerHttpStatus,
  providerMessageStatus,
  setProviderCache,
  UA,
  type ProviderJsonResult,
} from "./clients/provider-http.js";
import { enc, fetchTimeseriesByDate, getCrumb, invalidateCrumb, valuationMarketInputs, yGet } from "./clients/yahoo.js";
// Phase 4 unit 2: the Yahoo client and the provider transport moved to clients/yahoo.ts and
// clients/provider-http.ts; these re-exports keep tools.ts, index.ts, evidence-pack.ts and the parity bundle unchanged.
export {
  edgeCache,
  fetchProviderWithTimeout,
  getProviderCache,
  providerCacheKey,
  providerHttpStatus,
  setProviderCache,
  UA,
  type ProviderJsonResult,
} from "./clients/provider-http.js";
export {
  countersSince,
  enc,
  iso,
  markYahooCacheActivity,
  yahooCacheStats,
  yahooEdgeTtlMs,
  yGet,
} from "./clients/yahoo.js";
import { filterParagraphsByTopics, htmlToReadableText, _sanitizeFilingHtml, _stripHtmlTagsIdx } from "./clients/html.js";
// Phase 4 unit 3: the HTML and filing-table parsing helpers moved to clients/html.ts; these re-exports keep
// tools.ts, the parity bundle (m.yahooFinance.*) and the news modules unchanged.
export {
  _stripHtmlTagsIdx,
  isShareScaleStatement,
  mergeFinancialCells,
  stripHtmlTags,
  tableUnitScale,
  type UnitScaleSource,
} from "./clients/html.js";
import { _str, clampInt, compactExcerpt, dailyBarDate, parseObjectJson, roundHalfAwayFromZero, toIsoUtc } from "./util.js";
import { finnhubEligibility, resolveNewsCompanyIdentity } from "./news/identity.js";
// Phase 4 unit 5: the generic helpers moved to util.ts, the batch runner to batch.ts and the news company identity
// to news/identity.ts; these re-exports keep the parity bundle (m.yahooFinance.*) unchanged.
export { _str, clampInt, toIsoUtc } from "./util.js";
export { runPartialBatch } from "./batch.js";
export { COMPANY_IR_DISCOVERY_TTL_MS, finnhubEligibility, resolveNewsCompanyIdentity, type NewsCompanyIdentity } from "./news/identity.js";
export {
  getNews,
  DEFAULT_NEWS_SOURCES,
  DEFAULT_PRESS_RELEASE_SOURCES,
  DEFAULT_EVENT_SOURCES,
  sourceRequestFields,
  yahooItemIssuer,
  collectYahooEvents,
  rankNewsItemsByRelevance,
  getCompanyNews,
  searchCompanyNews,
  getCompanyPressReleases,
  getSecRecentEvents,
  getPublicEventTimeline,
  verifyCompanyEvent,
} from "./news.js";
// Phase 5 unit 1: SEC EDGAR retrieval moved to sec.ts; this re-export keeps tools.ts, index.ts, evidence-pack.ts and the
// parity bundle (m.yahooFinance.*) unchanged.
import { _buildFilingIndexFromHtml } from "./sec.js";
export {
  getFilingData,
  type FilingSearchOptions,
  searchHeadings,
  searchFilingText,
  listSecCompanyFilings,
  sanitizeSecHtml,
  getFilingOutline,
  sectionHeadingMatches,
  filingItemHeadings,
  findSectionBounds,
  getFilingSection,
  listFilingTables,
  getFilingTable,
  extractFilingFact,
  getSecFilingIndex,
  listSecMaterialFilings,
  getSecFilingIntelligence,
  getSecFilingSectionMarkdown,
  listSecFilingExhibits,
  getSecFilingExhibitContent,
  xbrlSourceEvidence,
  isDecisionGradeXbrlFact,
  querySecFilingIndex,
} from "./sec.js";
// Phase 5 unit 2: SEC extractors and exposure tools moved to exposure.ts; this re-export keeps tools.ts, index.ts,
// evidence-pack.ts and the parity bundle (m.yahooFinance.*) unchanged.
import {
  extractDilutionBridge,
  extractCapitalStructure,
} from "./exposure.js";
export {
  CHINA_PRESET,
  type ResolvedSecFiling,
  alignedGeoAmounts,
  geoColumnHeader,
  geoRegionAliases,
  extractGeoRevenueFromHtml,
  PRODUCT_BREAKDOWN_LEAD_RE,
  termCenteredExcerpt,
  readableExposureExcerpt,
  normalizeStatus,
  extractGeographicRevenue,
  type SegmentTableRow,
  type SegmentTableResult,
  extractSegmentTableFromHtml,
  extractSegmentRevenue,
  extractTotalRevenue,
  extractRevenueExposure,
  extractRiskFactorMentions,
  customerConcentrationFromMatches,
  extractCustomerConcentration,
  extractDilutionBridge,
  extractCapitalStructure,
  getShareCountScenarios,
  extractFundingCapexSchedule,
  extractOperatingDriverLedger,
  extractChinaExposure,
  indexTablesNameRegion,
  extractExposure,
} from "./exposure.js";
// Phase 5 unit 3: pricing and options moved to pricing.ts; this re-export keeps tools.ts, index.ts, evidence-pack.ts and the
// parity bundle (m.yahooFinance.*) unchanged.
export {
  historicalRangeError,
  getHistoricalPrices,
  getOptionExpirationDates,
  getOptionChain,
  getFastInfo,
  getPriceStats,
  searchTicker,
  screenStocks,
  getShortInterest,
  indicatorLookback,
  wilderRsi,
  macd,
  annualizedVolatility,
  getTechnicalIndicators,
  getPriceSlope,
  getVolumeRatio,
  getMaPosition,
  getShortMomentum,
  getPutHedgeCandidates,
  computeMaxPainStrike,
  getOptionsFlowScan,
  getPriceTargetBracket,
  getPositionScoreInputs,
  listingCurrencyUnit,
  averageTradedValue,
  getVolumeGate,
  snapshotComponentMessage,
  snapshotTickerNotFound,
  getMarketSnapshot,
  usMarketDate,
  nextOptionExpiry,
  getOptionsSummary,
} from "./pricing.js";

// Phase 5 unit 4: fundamentals and analyst tools moved to fundamentals.ts; this re-export keeps tools.ts, index.ts, evidence-pack.ts and the
// parity bundle (m.yahooFinance.*) unchanged.
import {
  getEarningsAnalysis,
  getCalendar,
  peerValuationError,
} from "./fundamentals.js";
export {
  notAFundMessage,
  getEtfInfo,
  getStockInfo,
  getStockActions,
  limitCorporateActions,
  getFinancialStatement,
  getHolderInfo,
  unwrapYahooValues,
  getAnalystConsensus,
  getEarningsAnalysis,
  getFinancialRatios,
  analyzeShareCountTrend,
  getCalendar,
  getMarketCalendar,
  getRecommendations,
  getCreditHealth,
  getEarningsMomentum,
  analystPriceTargetChange,
  getAnalystUpgradeRadar,
  secFiscalYearNaming,
  extractAnalystValuationMethods,
  peerValuationError,
  comparePeerValuations,
} from "./fundamentals.js";

/**
 * Yahoo Finance API client for Cloudflare Workers.
 * Calls Yahoo Finance HTTP endpoints directly (replacing yfinance + pandas).
 *
 * Endpoints that need a session crumb:   /v10/finance/quoteSummary/, /v7/finance/options/,
 *                                        /ws/fundamentals-timeseries/
 * Endpoints that work without auth:      /v8/finance/chart/, /v1/finance/search
 */

const ALPHA_VANTAGE_API = "https://www.alphavantage.co/query";
const FINNHUB_API = "https://finnhub.io/api/v1";
const ALPHA_HISTORICAL_PUT_CALL_TTL_MS = 365 * 24 * 60 * 60 * 1000;
const PROVIDER_OWNERSHIP_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const ALPHA_TRANSCRIPT_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const YAHOO_TRANSCRIPT_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const YAHOO_TRANSCRIPT_CACHE_VERSION = "v2";
const YAHOO_TRANSCRIPT_PREVIEW_MAX_PARAGRAPHS = 3;
const YAHOO_TRANSCRIPT_PREVIEW_MAX_FULL_TEXT_CHARS = 256;

// Provider refusals that repeat until something changes (a plan entitlement,
// a daily quota) are remembered per endpoint, so each call does not spend a
// request, and quota, only to be refused again.
const PROVIDER_ENTITLEMENT_DENIAL_TTL_MS = 6 * 60 * 60 * 1000;
const providerDenials = new BoundedTtlCache<ProviderJsonResult>(100);

function rememberedProviderDenial(denialKey: string): ProviderJsonResult | null {
  const denial = providerDenials.get(denialKey);
  if (!denial) return null;
  countCacheEvent(providerCacheName(denialKey), "memoryHits");
  const until = denial.deniedUntil ? ` until ${denial.deniedUntil}` : "";
  return { ...denial, providerAttempted: false, cacheStatus: "HIT_PROCESS", message: `${denial.message ?? denial.status} (remembered${until}; not retried)` };
}

function rememberProviderDenial(denialKey: string, result: ProviderJsonResult): ProviderJsonResult {
  const now = Date.now();
  if (result.status === "ENTITLEMENT_REQUIRED" || result.status === "AUTH_ERROR") {
    const deniedUntil = new Date(now + PROVIDER_ENTITLEMENT_DENIAL_TTL_MS).toISOString();
    providerDenials.set(denialKey, { ...result, deniedUntil }, PROVIDER_ENTITLEMENT_DENIAL_TTL_MS);
    return { ...result, deniedUntil };
  } else if (result.status === "RATE_LIMIT" && /per day|daily/i.test(result.message ?? "")) {
    const d = new Date(now);
    const ttl = Math.max(60_000, Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1) - now);
    const deniedUntil = new Date(now + ttl).toISOString();
    providerDenials.set(denialKey, { ...result, deniedUntil }, ttl);
    return { ...result, deniedUntil };
  }
  return result;
}

export async function fetchAlphaVantageJson(
  operation: string,
  params: Record<string, unknown>,
  ttlMs: number,
): Promise<ProviderJsonResult> {
  const safeParams = { function: operation, ...params };
  const cacheKey = providerCacheKey("alpha_vantage", operation, safeParams);
  const cached = await getProviderCache(cacheKey, ttlMs);
  if (cached) return cached;
  const denialKey = "alpha_vantage:quota";
  const denied = rememberedProviderDenial(denialKey);
  if (denied) return denied;
  const apiKey = getWorkerVar("ALPHA_VANTAGE_API_KEY") ?? getWorkerVar("ALPHAVANTAGE_API_KEY");
  const publicQuery = new URLSearchParams({
    ...Object.fromEntries(Object.entries(safeParams).map(([key, value]) => [key, String(value)])),
    apikey: "REDACTED",
  });
  const publicUrl = `${ALPHA_VANTAGE_API}?${publicQuery.toString()}`;
  if (!apiKey) return { payload: null, status: "SOURCE_UNCONFIGURED", publicUrl, providerAttempted: false, cacheStatus: "MISS" };
  const requestQuery = new URLSearchParams(publicQuery);
  requestQuery.set("apikey", apiKey);
  try {
    const response = await fetchProviderWithTimeout(
      `${ALPHA_VANTAGE_API}?${requestQuery.toString()}`,
      { headers: { "User-Agent": UA } },
      30_000,
    );
    const retryAfter = response.headers.get("Retry-After") ?? undefined;
    if (!response.ok) {
      const body = (await response.text()).replaceAll(apiKey, "REDACTED");
      return {
        payload: null,
        status: providerHttpStatus(response.status, body),
        publicUrl,
        providerAttempted: true,
        cacheStatus: "MISS",
        httpStatus: response.status,
        message: body.slice(0, 500) || undefined,
        retryAfter,
      };
    }
    const rawJson = await response.json();
    if (!rawJson || typeof rawJson !== "object" || Array.isArray(rawJson)) {
      return { payload: null, status: "PROVIDER_ERROR", publicUrl, providerAttempted: true, cacheStatus: "MISS", message: "Alpha Vantage returned a non-object payload." };
    }
    const json = rawJson as Record<string, unknown>;
    const providerMessage = String(json.Note ?? json.Information ?? json["Error Message"] ?? "").replaceAll(apiKey, "REDACTED");
    if (providerMessage) {
      return rememberProviderDenial(denialKey, { payload: null, status: providerMessageStatus(providerMessage), publicUrl, providerAttempted: true, cacheStatus: "MISS", message: providerMessage });
    }
    const fetchedAt = new Date().toISOString();
    await setProviderCache(cacheKey, json, publicUrl, fetchedAt, ttlMs);
    return { payload: json, status: "OK", publicUrl, providerAttempted: true, cacheStatus: "MISS", fetchedAt };
  } catch (error) {
    const message = error instanceof Error ? error.message.replaceAll(apiKey, "REDACTED") : String(error);
    const status = /timeout|timed out|abort/i.test(message) ? "TIMEOUT" : "PROVIDER_ERROR";
    return { payload: null, status, publicUrl, providerAttempted: true, cacheStatus: "MISS", message };
  }
}

async function fetchFinnhubJson(
  path: string,
  params: Record<string, unknown>,
  ttlMs: number,
): Promise<ProviderJsonResult> {
  const cleanPath = `/${path.replace(/^\/+/, "")}`;
  const cacheKey = providerCacheKey("finnhub", cleanPath, params);
  const cached = await getProviderCache(cacheKey, ttlMs);
  if (cached) return cached;
  // Entitlements are per endpoint, not per symbol.
  const denialKey = `finnhub:${cleanPath}`;
  const denied = rememberedProviderDenial(denialKey);
  if (denied) return { ...denied, publicUrl: `${FINNHUB_API}${cleanPath}?${new URLSearchParams(Object.entries(params).map(([key, value]): [string, string] => [key, String(value)])).toString()}` };
  const token = getWorkerVar("FINNHUB_API_KEY") ?? getWorkerVar("FINNHUB_TOKEN");
  const query = new URLSearchParams(
    Object.entries(params).map(([key, value]): [string, string] => [key, String(value)])
  );
  const publicUrl = `${FINNHUB_API}${cleanPath}?${query.toString()}`;
  if (!token) return { payload: null, status: "SOURCE_UNCONFIGURED", publicUrl, providerAttempted: false, cacheStatus: "MISS" };
  try {
    const response = await fetchProviderWithTimeout(
      publicUrl,
      { headers: { "User-Agent": UA, "X-Finnhub-Token": token } },
      20_000,
    );
    const retryAfter = response.headers.get("Retry-After") ?? undefined;
    if (!response.ok) {
      const body = (await response.text()).replaceAll(token, "REDACTED");
      return rememberProviderDenial(denialKey, {
        payload: null,
        status: providerHttpStatus(response.status, body),
        publicUrl,
        providerAttempted: true,
        cacheStatus: "MISS",
        httpStatus: response.status,
        message: body.slice(0, 500) || undefined,
        retryAfter,
      });
    }
    const json = await response.json();
    if (!json || typeof json !== "object" || Array.isArray(json)) {
      return { payload: null, status: "PROVIDER_ERROR", publicUrl, providerAttempted: true, cacheStatus: "MISS", message: "Finnhub returned a non-object payload." };
    }
    const payload = json as Record<string, unknown>;
    const fetchedAt = new Date().toISOString();
    await setProviderCache(cacheKey, payload, publicUrl, fetchedAt, ttlMs);
    return { payload, status: "OK", publicUrl, providerAttempted: true, cacheStatus: "MISS", fetchedAt };
  } catch (error) {
    const message = error instanceof Error ? error.message.replaceAll(token, "REDACTED") : String(error);
    const status = /timeout|timed out|abort/i.test(message) ? "TIMEOUT" : "PROVIDER_ERROR";
    return { payload: null, status, publicUrl, providerAttempted: true, cacheStatus: "MISS", message };
  }
}


// ── get_etf_info ─────────────────────────────────────────────────────────────


// Yahoo chart ranges and intervals. Yahoo answers an unknown value with a
// fallback series (a single live bar) instead of an error, so inputs are
// checked against the documented sets before the request.
export const HISTORICAL_PERIODS = ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"] as const;
export const HISTORICAL_INTERVALS = ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"] as const;

// ── Financial statements via fundamentals timeseries API ────────────────────
//
// The quoteSummary v10 API returns only a small subset of income/balance/cash
// statement fields and frequently returns {raw:0} for fields it doesn't fully
// populate (e.g. grossProfit, costOfRevenue, all balance sheet numerics).
// Balance sheet results contain only the endDate — no numeric data.
//
// yfinance >= 0.2.x switched to the fundamentals-timeseries API for this
// reason. We do the same here.

export const HOLDER_MOD: Record<string, string> = {
  major_holders: "majorHoldersBreakdown",
  institutional_holders: "institutionOwnership",
  mutualfund_holders: "fundOwnership",
  insider_transactions: "insiderTransactions",
  insider_purchases: "netSharePurchaseActivity",
  insider_roster_holders: "insiderHolders",
};
export const SUPPORTED_HOLDER_TYPES = Object.keys(HOLDER_MOD);

function providerNumber(value: unknown): number | null {
  if (value == null || typeof value === "boolean") return null;
  const parsed = Number(String(value).replaceAll(",", ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function providerAttempt(provider: string, result: ProviderJsonResult): Record<string, unknown> {
  return Object.fromEntries(Object.entries({
    provider,
    status: result.status,
    attempted: result.providerAttempted,
    cacheStatus: result.cacheStatus,
    url: result.publicUrl,
    httpStatus: result.httpStatus,
    message: result.message,
    retryAfter: result.retryAfter,
  }).filter(([, value]) => value != null));
}

function providerRecovery(status: string, alphaAllowed = false): string {
  if (status === "SOURCE_UNCONFIGURED") return "CONFIGURE_PROVIDER";
  if (status === "AUTH_ERROR") return "CHECK_PROVIDER_CREDENTIALS";
  if (status === "ENTITLEMENT_REQUIRED") return alphaAllowed ? "USE_ALPHA_VANTAGE_EXPLICITLY" : "CHECK_PROVIDER_ENTITLEMENT";
  if (status === "RATE_LIMIT") return "RETRY_AFTER_PROVIDER_RESET";
  if (status === "TIMEOUT") return "RETRY_PROVIDER";
  if (status === "NOT_ELIGIBLE") return "USE_YAHOO_OR_OFFICIAL_FILINGS";
  if (status === "NOT_FOUND") return "VERIFY_TICKER_AND_DATE";
  return "RETRY_OR_USE_PRIMARY_PROVIDER";
}

export async function getHistoricalPutCallRatio(ticker: string, date: string): Promise<string> {
  const tool = "get_historical_put_call_ratio";
  if (!String(ticker ?? "").trim()) {
    return mcpFailure(tool, ErrorCode.INPUT_VALIDATION_ERROR, "ticker is required.");
  }
  const normalizedDate = String(date ?? "").trim();
  const parsedDate = /^\d{4}-\d{2}-\d{2}$/.test(normalizedDate)
    ? new Date(`${normalizedDate}T00:00:00.000Z`)
    : new Date(Number.NaN);
  const today = new Date().toISOString().slice(0, 10);
  if (
    !Number.isFinite(parsedDate.getTime())
    || parsedDate.toISOString().slice(0, 10) !== normalizedDate
    || normalizedDate <= "2008-01-01"
    || normalizedDate > today
  ) {
    return mcpFailure(
      tool,
      ErrorCode.INPUT_VALIDATION_ERROR,
      "date must be a valid YYYY-MM-DD after 2008-01-01 and not in the future. Use summarize_options_flow for a current Yahoo options snapshot.",
    );
  }
  const tickerU = ticker.toUpperCase();
  const result = await fetchAlphaVantageJson(
    "HISTORICAL_PUT_CALL_RATIO",
    { symbol: tickerU, date: normalizedDate },
    ALPHA_HISTORICAL_PUT_CALL_TTL_MS,
  );
  const attempts = [providerAttempt("alpha_vantage", result)];
  if (result.status !== "OK" || !result.payload) {
    return JSON.stringify({
      ticker: tickerU,
      status: result.status,
      source: null,
      providerGapFilled: false,
      capacityClass: "SCARCE_SHARED_QUOTA",
      dataDate: normalizedDate,
      evidenceClass: "CONTEXTUAL_PROVIDER_DATA",
      decisionGrade: false,
      providerAttempts: attempts,
      recommendedNextAction: providerRecovery(result.status),
    });
  }
  const responseSymbol = (_str(result.payload.symbol) || tickerU).toUpperCase();
  const responseDate = _str(result.payload.date) || normalizedDate;
  if (responseSymbol !== tickerU || responseDate !== normalizedDate) {
    attempts[0].validationStatus = "PROVIDER_IDENTITY_MISMATCH";
    return JSON.stringify({
      ticker: tickerU,
      status: "PROVIDER_IDENTITY_MISMATCH",
      source: null,
      providerGapFilled: false,
      capacityClass: "SCARCE_SHARED_QUOTA",
      dataDate: normalizedDate,
      evidenceClass: "CONTEXTUAL_PROVIDER_DATA",
      decisionGrade: false,
      providerAttempts: attempts,
      recommendedNextAction: "RETRY_OR_VERIFY_PROVIDER_RESPONSE",
    });
  }
  const fullChain = providerNumber(result.payload.put_call_ratio_full_chain);
  const byExpiration = (Array.isArray(result.payload.put_call_ratio_by_expiration)
    ? result.payload.put_call_ratio_by_expiration
    : [])
    .filter((row): row is Record<string, unknown> => !!row && typeof row === "object" && !Array.isArray(row))
    .map((row) => ({
      expirationDate: _str(row.date),
      putCallRatio: providerNumber(row.value),
    }))
    .filter((row) => row.expirationDate && row.putCallRatio != null);
  if (fullChain == null && byExpiration.length === 0) {
    return JSON.stringify({
      ticker: tickerU,
      status: "PROVIDER_ERROR",
      source: null,
      providerGapFilled: false,
      capacityClass: "SCARCE_SHARED_QUOTA",
      dataDate: normalizedDate,
      evidenceClass: "CONTEXTUAL_PROVIDER_DATA",
      decisionGrade: false,
      providerAttempts: attempts,
      recommendedNextAction: "RETRY_OR_USE_PRIMARY_PROVIDER",
    });
  }
  return JSON.stringify({
    ticker: tickerU,
    status: "OK",
    source: "alpha_vantage",
    providerGapFilled: true,
    capacityClass: "SCARCE_SHARED_QUOTA",
    dataDate: responseDate,
    freshnessStatus: "HISTORICAL_OBSERVATION",
    putCallRatioFullChain: fullChain,
    byExpiration,
    expirationCount: byExpiration.length,
    evidenceClass: "CONTEXTUAL_OPTIONS_DATA",
    decisionGrade: false,
    cacheStatus: result.cacheStatus,
    fetchedAt: result.fetchedAt ?? null,
    providerAttempts: attempts,
    recommendedNextAction: "NONE",
  });
}

function ownershipRows(payload: Record<string, unknown>, provider: string): Record<string, unknown>[] {
  const candidates = [payload.holdings, payload.ownership, payload.data].find(Array.isArray);
  if (!Array.isArray(candidates)) return [];
  return candidates
    .filter((row): row is Record<string, unknown> => !!row && typeof row === "object" && !Array.isArray(row))
    .map((row) => {
      const normalized = provider === "alpha_vantage"
        ? {
            holderName: row.holder_name,
            sharesHeld: providerNumber(row.shares_held),
            sharesChanged: providerNumber(row.shares_changed),
            sharesChangedPct: providerNumber(row.shares_changed_percentage),
            changeType: row.change_type,
            reportDate: row.last_reported,
          }
        : {
            holderName: row.name ?? row.holder ?? row.holderName,
            sharesHeld: providerNumber(row.share ?? row.shares ?? row.sharesHeld),
            sharesChanged: providerNumber(row.change ?? row.sharesChanged),
            sharesChangedPct: providerNumber(row.changePercent ?? row.sharesChangedPct),
            reportDate: row.filingDate ?? row.reportDate ?? row.date,
          };
      return Object.fromEntries(Object.entries(normalized).filter(([, value]) => value != null && value !== ""));
    })
    .filter((row) => Object.keys(row).length > 0);
}

function ownershipDataDate(payload: Record<string, unknown>, rows: Record<string, unknown>[]): string | null {
  const direct = payload.date ?? payload.last_reported ?? payload.lastReported;
  if (direct) return String(direct).slice(0, 10);
  const dates = rows.map(row => _str(row.reportDate).slice(0, 10)).filter(Boolean).sort().reverse();
  return dates[0] ?? null;
}

export async function getExpandedInstitutionalOwnership(
  ticker: string,
  allowScarceFallback = false,
  maxHolders = 50,
): Promise<string> {
  const tool = "get_expanded_institutional_ownership";
  if (!String(ticker ?? "").trim()) {
    return mcpFailure(tool, ErrorCode.INPUT_VALIDATION_ERROR, "ticker is required.");
  }
  if (!Number.isInteger(maxHolders) || maxHolders < 1 || maxHolders > 100) {
    return mcpFailure(tool, ErrorCode.INPUT_VALIDATION_ERROR, "max_holders must be an integer from 1 through 100.");
  }
  const tickerU = ticker.toUpperCase();
  const attempts: Record<string, unknown>[] = [];
  const eligibility = finnhubEligibility(tickerU);
  let provider = "finnhub";
  let result: ProviderJsonResult;
  if (eligibility.eligible) {
    result = await fetchFinnhubJson("/stock/ownership", { symbol: tickerU, limit: maxHolders }, PROVIDER_OWNERSHIP_TTL_MS);
    attempts.push(providerAttempt("finnhub", result));
  } else {
    result = {
      payload: null,
      status: "NOT_ELIGIBLE",
      publicUrl: "",
      providerAttempted: false,
      cacheStatus: "MISS",
      message: eligibility.reasonCode ?? undefined,
    };
    attempts.push({ provider: "finnhub", status: "NOT_ELIGIBLE", attempted: false, reasonCode: eligibility.reasonCode });
  }
  let rows = result.payload ? ownershipRows(result.payload, "finnhub") : [];
  if (result.payload && (_str(result.payload.symbol) || tickerU).toUpperCase() !== tickerU) {
    attempts[attempts.length - 1].validationStatus = "PROVIDER_IDENTITY_MISMATCH";
    result = {
      ...result,
      payload: null,
      status: "PROVIDER_IDENTITY_MISMATCH",
      message: "Provider response symbol did not match the requested ticker.",
    };
    rows = [];
  }
  if (rows.length === 0 && allowScarceFallback) {
    provider = "alpha_vantage";
    result = await fetchAlphaVantageJson("INSTITUTIONAL_HOLDINGS", { symbol: tickerU }, PROVIDER_OWNERSHIP_TTL_MS);
    attempts.push(providerAttempt("alpha_vantage", result));
    rows = result.payload ? ownershipRows(result.payload, "alpha_vantage") : [];
    if (result.payload && (_str(result.payload.symbol) || tickerU).toUpperCase() !== tickerU) {
      attempts[attempts.length - 1].validationStatus = "PROVIDER_IDENTITY_MISMATCH";
      result = {
        ...result,
        payload: null,
        status: "PROVIDER_IDENTITY_MISMATCH",
        message: "Provider response symbol did not match the requested ticker.",
      };
      rows = [];
    }
  }
  const capacityClass = provider === "alpha_vantage" ? "SCARCE_SHARED_QUOTA" : "ENTITLEMENT_DEPENDENT";
  if (result.status !== "OK" || !result.payload || rows.length === 0) {
    const status = result.status === "OK" ? "NOT_FOUND" : result.status;
    const recommendedNextAction = status === "NOT_FOUND" && !allowScarceFallback
      ? "VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK"
      : providerRecovery(status, !allowScarceFallback);
    return JSON.stringify({
      ticker: tickerU,
      status,
      source: null,
      providerGapFilled: false,
      capacityClass,
      evidenceClass: "CONTEXTUAL_PROVIDER_DATA",
      decisionGrade: false,
      providerAttempts: attempts,
      alphaFallbackAllowed: allowScarceFallback,
      recommendedNextAction,
    });
  }
  const totalCount = rows.length;
  const holders = rows.slice(0, maxHolders);
  const dataDate = ownershipDataDate(result.payload, holders);
  const aggregate: Record<string, number> = {};
  if (provider === "alpha_vantage") {
    const mappings: Array<[string, string]> = [
      ["total_institutional_holders", "totalInstitutionalHolders"],
      ["total_institutional_shares", "totalInstitutionalShares"],
      ["total_institutional_ownership_percentage", "totalInstitutionalOwnershipPct"],
      ["holders_with_increased_holdings", "holdersIncreased"],
      ["holders_with_decreased_holdings", "holdersDecreased"],
      ["holders_with_unchanged_holdings", "holdersUnchanged"],
      ["shares_with_increased_holdings", "sharesIncreased"],
      ["shares_with_decreased_holdings", "sharesDecreased"],
      ["shares_with_unchanged_holdings", "sharesUnchanged"],
    ];
    for (const [input, output] of mappings) {
      const value = providerNumber(result.payload[input]);
      if (value != null) aggregate[output] = value;
    }
  }
  return JSON.stringify({
    ticker: tickerU,
    status: "OK",
    source: provider,
    providerGapFilled: true,
    capacityClass,
    scope: "PROVIDER_AGGREGATED_INSTITUTIONAL_OWNERSHIP",
    dataDate,
    freshnessStatus: dataDate ? "DATED" : "DATE_NOT_DISCLOSED",
    holders,
    returnedCount: holders.length,
    providerRowCount: totalCount,
    truncated: totalCount > holders.length,
    aggregate,
    evidenceClass: "CONTEXTUAL_OWNERSHIP_DATA",
    decisionGrade: false,
    cacheStatus: result.cacheStatus,
    fetchedAt: result.fetchedAt ?? null,
    providerAttempts: attempts,
    recommendedNextAction: "VERIFY_SEC_13F_FOR_MATERIAL_USE",
  });
}

export const REC_MOD: Record<string, string> = {
  recommendations: "recommendationTrend",
  upgrades_downgrades: "upgradeDowngradeHistory",
};
export const SUPPORTED_RECOMMENDATION_TYPES = Object.keys(REC_MOD);

// ── New tools ────────────────────────────────────────────────────────────────

// ── get_short_interest ───────────────────────────────────────────────────────

// ── get_technical_indicators ─────────────────────────────────────────────────

// ── Indicators ───────────────────────────────────────────────────────────────
// RSI and MACD are recursive averages: their value depends on how much history
// feeds them until the seed's weight decays. A year of daily bars (about 250)
// leaves the seed under 0.01% of the result, so shorter periods are widened.
export const INDICATOR_MIN_LOOKBACK = "1y";

// ── get_price_slope ──────────────────────────────────────────────────────────

// ── get_volume_ratio ─────────────────────────────────────────────────────────

// ── get_ma_position ──────────────────────────────────────────────────────────

// ── get_credit_health ────────────────────────────────────────────────────────

// ── get_short_momentum ───────────────────────────────────────────────────────

// ── get_earnings_momentum ────────────────────────────────────────────────────

// ── get_options_flow_summary ─────────────────────────────────────────────────

// ── get_analyst_upgrade_radar ────────────────────────────────────────────────

// ── SEC geographic revenue extraction ─────────────────────────────────────────

export const FILING_INDEX_CACHE_MAX = 50;
export const filingIndexCache = new Map<string, { value: string; storedAt: number }>();
export const FILING_INDEX_TTL_MS = 24 * 60 * 60 * 1000;



// ── search_sec_filing_text ────────────────────────────────────────────────────

// ── get_options_flow_scan ─────────────────────────────────────────────────────

// ── get_price_target_bracket ──────────────────────────────────────────────────

// ── get_position_score_inputs ─────────────────────────────────────────────────

/** analyze_position_signals takes one ticker (Q3i call 4). */
export const POSITION_SINGLE_TICKER_MESSAGE = "analyze_position_signals takes one ticker as a string, not a list.";

// ── freshness classifier ──────────────────────────────────────────────────────

// ── get_volume_gate ───────────────────────────────────────────────────────────

// ── get_market_snapshot ───────────────────────────────────────────────────────

// ── get_options_summary ───────────────────────────────────────────────────────

// ── list_sec_company_filings ──────────────────────────────────────────────────

// ── get_filing_outline ────────────────────────────────────────────────────────

// ── get_filing_section ────────────────────────────────────────────────────────

// ── list_filing_tables ────────────────────────────────────────────────────────

// ── get_filing_table ──────────────────────────────────────────────────────────

// ── extract_filing_fact ───────────────────────────────────────────────────────

// ─── Public news / event helpers ──────────────────────────────────────────────

// ── get_sec_filing_index ───────────────────────────────────────────────────────


/** The ticker of a Yahoo earnings-call transcript URL that the transcript parser accepts, else null. */
function yahooTranscriptUrlTicker(url: string): string | null {
  const m = url.match(/^https:\/\/finance\.yahoo\.com\/quote\/([^/?#]+)\/earnings\//i);
  if (!m) return null;
  let ticker: string;
  try {
    ticker = decodeURIComponent(m[1]).toUpperCase();
  } catch {
    return null;
  }
  return parseYahooTranscriptSourceUrl(ticker, url).identity ? ticker : null;
}

/**
 * Allow-list check for a caller-supplied URL (2.10.0): checkFetchUrl plus the
 * caller path rule. SEC URLs must be under /Archives/, Yahoo URLs must be
 * transcript pages the parser accepts, and registry hosts must sit inside an
 * approved entry's path prefixes. Other allow-listed hosts (provider APIs)
 * are never fetched for a caller URL.
 */
function callerUrlRefusal(url: string): string | null {
  const check = checkFetchUrl(url);
  if (!check.ok) return check.reason;
  const kind = allowedHostKind(check.url);
  if (kind === "sec" && url.startsWith(SEC_ARCHIVES_PREFIX)) return null;
  if (kind === "yahoo" && yahooTranscriptUrlTicker(url) !== null) return null;
  if (kind === "registry" && approvedRegistryUrlAllowed(url)) return null;
  return "URL path not allowed";
}

/**
 * Full text of a finance.yahoo.com earnings-call transcript URL, one
 * "Speaker: text" paragraph per line, read page by page from the structured
 * transcript. Null for other URLs or when the transcript is unavailable.
 */
async function yahooTranscriptTextForUrl(url: string): Promise<string | null> {
  const ticker = yahooTranscriptUrlTicker(url);
  if (ticker === null) return null;
  const paragraphs: string[] = [];
  let cursor = 0;
  for (let page = 0; page < 20; page++) {
    const data = parseObjectJson(await getEarningsCallTranscript(ticker, "latest", null, null, null, url, 50, cursor));
    const rows = Array.isArray(data.paragraphs) ? data.paragraphs as Record<string, unknown>[] : [];
    for (const row of rows) {
      const text = _str(row.text);
      if (text) paragraphs.push(row.speaker ? `${_str(row.speaker)}: ${text}` : text);
    }
    const pagination = data.pagination && typeof data.pagination === "object" ? data.pagination as Record<string, unknown> : {};
    const next = Number(pagination.nextCursor);
    if (pagination.hasMore !== true || !Number.isFinite(next) || next <= cursor) break;
    cursor = next;
  }
  return paragraphs.length > 0 ? paragraphs.join("\n\n") : null;
}

export async function parsePublicTranscript(
  url: string = "",
  topics: string[] | null = null,
  rawText: string | null = null,
): Promise<string> {
  const hasRawText = typeof rawText === "string" && rawText.trim() !== "";
  if (!hasRawText && !url.startsWith("https://")) {
    return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: "A valid https:// URL or raw_text is required." } });
  }

  if (!hasRawText) {
    const refusal = callerUrlRefusal(url);
    if (refusal !== null) {
      return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: `url is not on the fetch allow-list (${refusal}). Allowed: SEC Archives, finance.yahoo.com transcript pages, and approved company IR pages. Paste the text as raw_text instead.` } });
    }
  }

  let cleanText: string;
  let source: "raw_text" | "public_url";
  if (hasRawText) {
    cleanText = htmlToReadableText(rawText as string);
    source = "raw_text";
  } else {
    // Yahoo transcript pages do not render for server requests; their
    // transcripts come from the same structured Yahoo/Quartr source that
    // get_earnings_call_transcript uses.
    const yahooText = await yahooTranscriptTextForUrl(url);
    // An SEC page goes through the EDGAR client, with SEC's required User-Agent and the same read (2.26.0, D9).
    const check = checkFetchUrl(url);
    const secPage = check.ok && allowedHostKind(check.url) === "sec";
    const html = yahooText == null ? await (secPage ? edgarFetchPublicHtml : fetchPublicHtml)(url, 5_000_000) : null;
    if (yahooText == null && !html) {
      return JSON.stringify({ ok: false, error: { code: "FETCH_ERROR", message: `Could not fetch URL: ${url}` } });
    }
    cleanText = yahooText ?? htmlToReadableText(html as string);
    source = "public_url";
  }
  const warnings: Record<string, unknown>[] = [];
  const topicList = (topics ?? []).filter((topic) => typeof topic === "string" && topic.trim()).map((topic) => topic.trim());

  if (topicList.length) {
    const matchedParagraphs = filterParagraphsByTopics(cleanText, topicList);
    if (!matchedParagraphs.length) {
      warnings.push({ code: "NO_TOPIC_MATCHES", message: `No paragraphs matched the provided topics: ${topicList.join(", ")}` });
    }
    return JSON.stringify({
      url: hasRawText ? null : url,
      source,
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
    url: hasRawText ? null : url,
    source,
    filteredByTopics: null,
    text: cleanText.slice(0, maxChars),
    totalTextLength: cleanText.length,
    truncated,
    warnings,
  });
}

function transcriptAttempt(sourceType: string, status: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
  return Object.fromEntries(Object.entries({ sourceType, status, ...extra }).filter(([, v]) => v != null));
}

// Attempts that read content, or never fetched, keep their status when the budget ran out.
const TRANSCRIPT_BUDGET_UNAFFECTED = new Set(["SUCCESS", "SKIPPED", "INCOMPLETE_TRANSCRIPT"]);

/**
 * A transcript source attempt during which the subrequest budget refused a
 * fetch (refusals counted since `mark`, from budgetRefusalCount()) reports
 * PROVIDER_BUDGET_EXHAUSTED unless it read content (2.11.0). The waterfall
 * runs its sources one after another, so the count since the mark is this
 * source's alone. Only `status` changes.
 */
function budgetCheckedAttempt(attempt: Record<string, unknown>, mark: number): Record<string, unknown> {
  if (budgetRefusalCount() <= mark || TRANSCRIPT_BUDGET_UNAFFECTED.has(String(attempt.status))) return attempt;
  return { ...attempt, status: "PROVIDER_BUDGET_EXHAUSTED" };
}

type YahooTranscriptSourceIdentity = {
  eventId: string;
  fiscalQuarter: string;
  sourceUrl: string;
};

type YahooTranscriptFetchResult = ProviderJsonResult & {
  sourceUrl: string | null;
  eventId: string | null;
  contentDiagnostics?: Record<string, unknown>;
};

function yahooTickerPath(ticker: string): string {
  return encodeURIComponent(ticker.toUpperCase())
    .replace(/%5E/gi, "^")
    .replace(/%3D/gi, "=")
    .replace(/%2E/gi, ".");
}

function canonicalYahooTranscriptUrl(ticker: string, quarter: string, year: number, eventId: string): string {
  const encoded = yahooTickerPath(ticker);
  return `https://finance.yahoo.com/quote/${encoded}/earnings/${encoded}-${quarter.toUpperCase()}-${year}-earnings_call-${eventId}.html`;
}

export function parseYahooTranscriptSourceUrl(
  ticker: string,
  sourceUrl: string,
): { identity: YahooTranscriptSourceIdentity | null; error: string | null } {
  let parsed: URL;
  try {
    parsed = new URL(String(sourceUrl ?? "").trim());
  } catch {
    return { identity: null, error: "source_url is malformed" };
  }
  if (parsed.protocol !== "https:" || parsed.hostname.toLowerCase() !== "finance.yahoo.com") {
    return { identity: null, error: "source_url must be an https://finance.yahoo.com earnings-call URL" };
  }
  if (parsed.username || parsed.password || (parsed.port && parsed.port !== "443")) {
    return { identity: null, error: "source_url must not contain credentials or a non-standard port" };
  }
  let path: string;
  try {
    path = decodeURIComponent(parsed.pathname);
  } catch {
    return { identity: null, error: "source_url path is malformed" };
  }
  const match = path.match(/^\/quote\/([^/]+)\/earnings\/([^/]+)-Q([1-4])-(20\d{2})-earnings_call-(\d+)\.html\/?$/i);
  if (!match) {
    return { identity: null, error: "source_url must match Yahoo's /quote/{ticker}/earnings/...-earnings_call-{eventId}.html format" };
  }
  const requested = ticker.toUpperCase();
  if (match[1].toUpperCase() !== requested || match[2].toUpperCase() !== requested) {
    return { identity: null, error: "source_url ticker does not match the requested ticker" };
  }
  const year = Number(match[4]);
  const quarter = `Q${match[3]}`;
  return {
    identity: {
      eventId: match[5],
      fiscalQuarter: `${year}Q${match[3]}`,
      sourceUrl: canonicalYahooTranscriptUrl(requested, quarter, year, match[5]),
    },
    error: null,
  };
}

function discoverYahooTranscriptEvent(
  ticker: string,
  html: string,
  fiscalQuarter: string | null,
): YahooTranscriptSourceIdentity | null {
  const tickerU = ticker.toUpperCase();
  const pattern = /(?:https:\/\/finance\.yahoo\.com)?\/quote\/([^/"'?]+)\/earnings\/([^/"'?]+)-Q([1-4])-(20\d{2})-earnings_call-(\d+)\.html/gi;
  const seen = new Set<string>();
  const calls: Array<YahooTranscriptSourceIdentity & { year: number; quarterNumber: number }> = [];
  for (const match of html.matchAll(pattern)) {
    const pathTicker = String(match[1] ?? "").toUpperCase();
    const slugTicker = String(match[2] ?? "").toUpperCase();
    const eventId = String(match[5] ?? "");
    if (pathTicker !== tickerU || slugTicker !== tickerU || !eventId || seen.has(eventId)) continue;
    seen.add(eventId);
    const quarterNumber = Number(match[3]);
    const year = Number(match[4]);
    calls.push({
      eventId,
      fiscalQuarter: `${year}Q${quarterNumber}`,
      sourceUrl: canonicalYahooTranscriptUrl(tickerU, `Q${quarterNumber}`, year, eventId),
      year,
      quarterNumber,
    });
  }
  calls.sort((a, b) => b.year - a.year || b.quarterNumber - a.quarterNumber);
  const requested = String(fiscalQuarter ?? "").toUpperCase();
  const selected = requested ? calls.find((call) => call.fiscalQuarter === requested) : calls[0];
  return selected ? { eventId: selected.eventId, fiscalQuarter: selected.fiscalQuarter, sourceUrl: selected.sourceUrl } : null;
}

function validateYahooTranscriptPayloadIdentity(
  payload: Record<string, unknown>,
  expectedEventId: string,
  expectedFiscalQuarter: string | null,
): string | null {
  const metadata = payload.transcriptMetadata && typeof payload.transcriptMetadata === "object" && !Array.isArray(payload.transcriptMetadata)
    ? payload.transcriptMetadata as Record<string, unknown>
    : {};
  const content = payload.transcriptContent && typeof payload.transcriptContent === "object" && !Array.isArray(payload.transcriptContent)
    ? payload.transcriptContent as Record<string, unknown>
    : {};
  const returnedEventIds = new Set(
    [metadata.eventId, content.event_id, content.eventId]
      .filter((value) => value != null && String(value) !== "")
      .map((value) => String(value)),
  );
  if (!returnedEventIds.size) {
    return `Yahoo transcript metadata omitted the expected event ID ${expectedEventId}.`;
  }
  if (returnedEventIds.size !== 1 || !returnedEventIds.has(expectedEventId)) {
    return `Yahoo returned event ID ${[...returnedEventIds].sort().join(", ")}; expected ${expectedEventId}.`;
  }

  const expectedQuarter = String(expectedFiscalQuarter ?? "").trim().toUpperCase();
  if (!expectedQuarter) return null;
  const fiscalPeriod = String(metadata.fiscalPeriod ?? "").trim().toUpperCase();
  const fiscalYear = String(metadata.fiscalYear ?? "").trim();
  const returnedQuarter = /^20\d{2}$/.test(fiscalYear) && /^Q[1-4]$/.test(fiscalPeriod)
    ? `${fiscalYear}${fiscalPeriod}`
    : null;
  return returnedQuarter === expectedQuarter
    ? null
    : `Yahoo returned fiscal quarter ${returnedQuarter ?? "UNKNOWN"}; expected ${expectedQuarter}.`;
}

export function assessYahooTranscriptPayloadCompleteness(
  payload: Record<string, unknown>,
): Record<string, unknown> {
  const content = payload.transcriptContent && typeof payload.transcriptContent === "object" && !Array.isArray(payload.transcriptContent)
    ? payload.transcriptContent as Record<string, unknown>
    : {};
  const transcript = content.transcript && typeof content.transcript === "object" && !Array.isArray(content.transcript)
    ? content.transcript as Record<string, unknown>
    : null;
  if (!transcript) {
    return {
      contentCompleteness: "UNKNOWN", reasonCode: "TRANSCRIPT_SHAPE_INVALID",
      usableParagraphCount: 0, advertisedSpeakerCount: 0, paragraphSpeakerCount: 0,
      fullTextLength: 0, paragraphTextLength: 0,
    };
  }

  const paragraphRows = Array.isArray(transcript.paragraphs)
    ? transcript.paragraphs as Record<string, unknown>[]
    : [];
  let usableParagraphCount = 0;
  let paragraphTextLength = 0;
  const paragraphSpeakers = new Set<string>();
  for (const row of paragraphRows) {
    if (!row || typeof row !== "object") continue;
    let text = String(row.text ?? "").trim();
    if (!text && Array.isArray(row.sentences)) {
      text = (row.sentences as Record<string, unknown>[])
        .map((sentence) => String(sentence?.text ?? "").trim())
        .filter(Boolean)
        .join(" ");
    }
    if (!text) continue;
    usableParagraphCount += 1;
    paragraphTextLength += text.length;
    if (row.speaker != null && String(row.speaker) !== "") paragraphSpeakers.add(String(row.speaker));
  }

  const speakerRows = Array.isArray(content.speaker_mapping)
    ? content.speaker_mapping
    : Array.isArray(content.speakerMapping) ? content.speakerMapping : [];
  const declaredSpeakerCount = Number(transcript.number_of_speakers ?? transcript.numberOfSpeakers ?? 0);
  const advertisedSpeakerCount = Math.max(
    Number.isFinite(declaredSpeakerCount) ? declaredSpeakerCount : 0,
    speakerRows.length,
  );
  const fullTextLength = String(transcript.text ?? "").trim().length;
  const sparseSingleParagraph = usableParagraphCount <= 1 && advertisedSpeakerCount >= 2;
  const sparseMultiParagraph = usableParagraphCount <= YAHOO_TRANSCRIPT_PREVIEW_MAX_PARAGRAPHS
    && advertisedSpeakerCount >= 4;
  const previewOnly = fullTextLength <= YAHOO_TRANSCRIPT_PREVIEW_MAX_FULL_TEXT_CHARS
    && paragraphSpeakers.size <= 1
    && (sparseSingleParagraph || sparseMultiParagraph);
  const noUsableContent = usableParagraphCount === 0 && fullTextLength === 0;
  return {
    contentCompleteness: previewOnly ? "PARTIAL" : noUsableContent ? "UNKNOWN" : "FULL",
    reasonCode: previewOnly ? "YAHOO_PREVIEW_ONLY" : noUsableContent ? "TRANSCRIPT_SHAPE_INVALID" : null,
    usableParagraphCount,
    advertisedSpeakerCount,
    paragraphSpeakerCount: paragraphSpeakers.size,
    fullTextLength,
    paragraphTextLength,
  };
}

function yahooTranscriptCompletenessMessage(diagnostics: Record<string, unknown>): string {
  if (diagnostics.reasonCode === "YAHOO_PREVIEW_ONLY") {
    return `Yahoo returned only sparse preview content for ${diagnostics.advertisedSpeakerCount ?? 0} advertised speakers; the transcript is incomplete.`;
  }
  return "Yahoo returned transcript data without usable content.";
}

async function yahooTranscriptResponse(url: string, timeoutMs: number): Promise<Response> {
  const makeRequest = async (session: { value: string; cookie: string }): Promise<Response> => {
    const parsed = new URL(url);
    parsed.searchParams.set("crumb", session.value);
    return fetchProviderWithTimeout(
      parsed.toString(),
      { headers: { "User-Agent": UA, Cookie: session.cookie, Accept: "application/json,text/html;q=0.9,*/*;q=0.8" } },
      timeoutMs,
    );
  };
  const crumb = await getCrumb();
  let response = await makeRequest(crumb);
  if (response.status === 401) {
    await response.body?.cancel();
    invalidateCrumb(crumb);
    const fresh = await getCrumb();
    countProviderRetry();
    response = await makeRequest(fresh);
  }
  return response;
}

async function yahooTranscriptHttpFailure(
  response: Response,
  sourceUrl: string | null,
  eventId: string | null,
): Promise<YahooTranscriptFetchResult> {
  const retryAfter = response.headers.get("Retry-After") ?? undefined;
  const body = (await response.text()).slice(0, 500);
  return {
    payload: null,
    status: providerHttpStatus(response.status, body),
    publicUrl: sourceUrl ?? "https://finance.yahoo.com/",
    sourceUrl,
    eventId,
    providerAttempted: true,
    cacheStatus: "MISS",
    httpStatus: response.status,
    message: body || `Yahoo returned HTTP ${response.status}.`,
    retryAfter,
  };
}

async function fetchYahooQuartrTranscript(
  ticker: string,
  eventId: string | null,
  sourceUrl: string | null,
  fiscalQuarter: string | null,
): Promise<YahooTranscriptFetchResult> {
  const tickerU = ticker.toUpperCase();
  let resolvedEventId = eventId;
  let resolvedSourceUrl = sourceUrl;
  let expectedFiscalQuarter = String(fiscalQuarter ?? "").trim().toUpperCase() || null;
  if (sourceUrl) {
    const parsed = parseYahooTranscriptSourceUrl(tickerU, sourceUrl);
    if (!parsed.identity) {
      return {
        payload: null, status: "INVALID_SOURCE_URL", publicUrl: sourceUrl, sourceUrl, eventId,
        providerAttempted: false, cacheStatus: "MISS", message: parsed.error ?? "Invalid Yahoo transcript source URL.",
      };
    }
    if (eventId && eventId !== parsed.identity.eventId) {
      return {
        payload: null, status: "INVALID_SOURCE_URL", publicUrl: parsed.identity.sourceUrl,
        sourceUrl: parsed.identity.sourceUrl, eventId, providerAttempted: false, cacheStatus: "MISS",
        message: "event_id does not match source_url",
      };
    }
    resolvedEventId = resolvedEventId ?? parsed.identity.eventId;
    resolvedSourceUrl = parsed.identity.sourceUrl;
    expectedFiscalQuarter = expectedFiscalQuarter ?? parsed.identity.fiscalQuarter;
  }

  const cachedForEvent = async (): Promise<YahooTranscriptFetchResult | null> => {
    if (!resolvedEventId) return null;
    const cacheKey = providerCacheKey("yahoo_quartr", "transcript", {
      contract: YAHOO_TRANSCRIPT_CACHE_VERSION,
      ticker: tickerU, eventId: resolvedEventId, lang: "en-US", region: "US",
    });
    const cached = await getProviderCache(cacheKey, YAHOO_TRANSCRIPT_TTL_MS);
    if (!cached) return null;
    const identityError = validateYahooTranscriptPayloadIdentity(
      cached.payload ?? {}, String(resolvedEventId), expectedFiscalQuarter,
    );
    const completeness = assessYahooTranscriptPayloadCompleteness(cached.payload ?? {});
    if (identityError || completeness.contentCompleteness !== "FULL") {
      await deleteProviderCache(cacheKey);
      return null;
    }
    return { ...cached, sourceUrl: resolvedSourceUrl, eventId: resolvedEventId, contentDiagnostics: completeness };
  };
  const initialCached = await cachedForEvent();
  if (initialCached) return initialCached;

  try {
    if (!resolvedEventId) {
      const quotePageUrl = `https://finance.yahoo.com/quote/${yahooTickerPath(tickerU)}`;
      const response = await yahooTranscriptResponse(quotePageUrl, 20_000);
      if (!response.ok) return yahooTranscriptHttpFailure(response, null, null);
      const html = await response.text();
      if (html.length > 10_000_000) {
        return {
          payload: null, status: "PROVIDER_ERROR", publicUrl: quotePageUrl, sourceUrl: null, eventId: null,
          providerAttempted: true, cacheStatus: "MISS", message: "Yahoo quote page exceeded the configured size limit.",
        };
      }
      const discovered = discoverYahooTranscriptEvent(tickerU, html, fiscalQuarter);
      if (!discovered) {
        return {
          payload: null, status: "NOT_FOUND", publicUrl: quotePageUrl, sourceUrl: null, eventId: null,
          providerAttempted: true, cacheStatus: "MISS",
          message: fiscalQuarter
            ? `Yahoo did not expose an earnings-call page for ${fiscalQuarter}.`
            : "Yahoo did not expose an earnings-call page for this ticker.",
        };
      }
      resolvedEventId = discovered.eventId;
      resolvedSourceUrl = discovered.sourceUrl;
      expectedFiscalQuarter = expectedFiscalQuarter ?? discovered.fiscalQuarter;
      const discoveredCached = await cachedForEvent();
      if (discoveredCached) return discoveredCached;
    }

    const quoteTypeUrl = yahooQuoteTypeUrl(yahooTickerPath(tickerU));
    const quoteResponse = await yahooTranscriptResponse(quoteTypeUrl, 20_000);
    if (!quoteResponse.ok) return yahooTranscriptHttpFailure(quoteResponse, resolvedSourceUrl, resolvedEventId);
    const quoteJson = await quoteResponse.json() as Record<string, unknown>;
    const quoteType = quoteJson.quoteType as Record<string, unknown> | undefined;
    const quoteResults = Array.isArray(quoteType?.result) ? quoteType.result as Record<string, unknown>[] : [];
    const quartrId = quoteResults[0]?.quartrId;
    if (quartrId == null || String(quartrId) === "") {
      return {
        payload: null, status: "NOT_FOUND", publicUrl: resolvedSourceUrl ?? quoteTypeUrl,
        sourceUrl: resolvedSourceUrl, eventId: resolvedEventId, providerAttempted: true, cacheStatus: "MISS",
        message: "Yahoo quote metadata did not expose a Quartr company identifier.",
      };
    }

    const xhr = new URL("https://finance.yahoo.com/xhr/transcript");
    xhr.searchParams.set("eventType", "earnings_call");
    xhr.searchParams.set("quartrId", String(quartrId));
    xhr.searchParams.set("eventId", String(resolvedEventId));
    xhr.searchParams.set("lang", "en-US");
    xhr.searchParams.set("region", "US");
    const transcriptResponse = await yahooTranscriptResponse(xhr.toString(), 30_000);
    if (!transcriptResponse.ok) return yahooTranscriptHttpFailure(transcriptResponse, resolvedSourceUrl, resolvedEventId);
    const rawJson = await transcriptResponse.json() as unknown;
    if (!rawJson || typeof rawJson !== "object" || Array.isArray(rawJson)) {
      return {
        payload: null, status: "PROVIDER_ERROR", publicUrl: resolvedSourceUrl ?? xhr.toString(),
        sourceUrl: resolvedSourceUrl, eventId: resolvedEventId, providerAttempted: true, cacheStatus: "MISS",
        message: "Yahoo returned a non-object transcript response.",
      };
    }
    const payload = rawJson as Record<string, unknown>;
    if (!payload.transcriptContent || typeof payload.transcriptContent !== "object") {
      return {
        payload: null, status: "NOT_FOUND", publicUrl: resolvedSourceUrl ?? xhr.toString(),
        sourceUrl: resolvedSourceUrl, eventId: resolvedEventId, providerAttempted: true, cacheStatus: "MISS",
        message: "Yahoo returned no structured transcript content for this event.",
      };
    }
    const identityError = validateYahooTranscriptPayloadIdentity(
      payload, String(resolvedEventId), expectedFiscalQuarter,
    );
    if (identityError) {
      return {
        payload: null, status: "YAHOO_METADATA_MISMATCH", publicUrl: resolvedSourceUrl ?? xhr.toString(),
        sourceUrl: resolvedSourceUrl, eventId: resolvedEventId, providerAttempted: true, cacheStatus: "MISS",
        message: identityError,
      };
    }
    const completeness = assessYahooTranscriptPayloadCompleteness(payload);
    if (completeness.contentCompleteness !== "FULL") {
      return {
        payload,
        status: completeness.contentCompleteness === "PARTIAL" ? "INCOMPLETE_TRANSCRIPT" : "PROVIDER_ERROR",
        publicUrl: resolvedSourceUrl ?? xhr.toString(),
        sourceUrl: resolvedSourceUrl,
        eventId: resolvedEventId,
        providerAttempted: true,
        cacheStatus: "MISS",
        message: yahooTranscriptCompletenessMessage(completeness),
        contentDiagnostics: completeness,
      };
    }
    const fetchedAt = new Date().toISOString();
    const cacheKey = providerCacheKey("yahoo_quartr", "transcript", {
      contract: YAHOO_TRANSCRIPT_CACHE_VERSION,
      ticker: tickerU, eventId: String(resolvedEventId), lang: "en-US", region: "US",
    });
    await setProviderCache(cacheKey, payload, resolvedSourceUrl ?? xhr.toString(), fetchedAt, YAHOO_TRANSCRIPT_TTL_MS);
    return {
      payload, status: "OK", publicUrl: resolvedSourceUrl ?? xhr.toString(), sourceUrl: resolvedSourceUrl,
      eventId: resolvedEventId, providerAttempted: true, cacheStatus: "MISS", fetchedAt,
      contentDiagnostics: completeness,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      payload: null,
      status: /timeout|timed out|abort/i.test(message) ? "TIMEOUT" : "PROVIDER_ERROR",
      publicUrl: resolvedSourceUrl ?? "https://finance.yahoo.com/",
      sourceUrl: resolvedSourceUrl,
      eventId: resolvedEventId,
      providerAttempted: true,
      cacheStatus: "MISS",
      message,
    };
  }
}

function metadataEpochIso(value: unknown): string | null {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  try {
    return new Date(numeric * 1000).toISOString();
  } catch {
    return null;
  }
}

async function normalizeYahooQuartrPayload(
  ticker: string,
  payload: Record<string, unknown>,
  sourceUrl: string | null,
  eventId: string | null,
  topics: string[] | null,
  paragraphLimit: number,
  paragraphCursor: number,
  cacheStatus: string,
  fetchedAt: string | undefined,
): Promise<Record<string, unknown> | null> {
  const content = payload.transcriptContent;
  if (!content || typeof content !== "object" || Array.isArray(content)) return null;
  const contentRecord = content as Record<string, unknown>;
  const transcript = contentRecord.transcript;
  if (!transcript || typeof transcript !== "object" || Array.isArray(transcript)) return null;
  const transcriptRecord = transcript as Record<string, unknown>;
  const metadata = payload.transcriptMetadata && typeof payload.transcriptMetadata === "object" && !Array.isArray(payload.transcriptMetadata)
    ? payload.transcriptMetadata as Record<string, unknown>
    : {};

  const speakerRows = Array.isArray(contentRecord.speaker_mapping)
    ? contentRecord.speaker_mapping as Record<string, unknown>[]
    : Array.isArray(contentRecord.speakerMapping)
      ? contentRecord.speakerMapping as Record<string, unknown>[]
      : [];
  const speakerMap = new Map<string, Record<string, unknown>>();
  const speakers: Record<string, unknown>[] = [];
  for (const row of speakerRows) {
    if (!row || typeof row !== "object") continue;
    const speakerDataRaw = row.speaker_data ?? row.speakerData;
    const speakerData = speakerDataRaw && typeof speakerDataRaw === "object" && !Array.isArray(speakerDataRaw)
      ? speakerDataRaw as Record<string, unknown>
      : {};
    const compact = {
      speakerId: row.speaker ?? null,
      name: speakerData.name ?? null,
      role: speakerData.role ?? null,
      company: speakerData.company ?? null,
    };
    speakerMap.set(String(row.speaker), compact);
    speakers.push(compact);
  }

  const paragraphRows = Array.isArray(transcriptRecord.paragraphs)
    ? transcriptRecord.paragraphs as Record<string, unknown>[]
    : [];
  let paragraphs: Record<string, unknown>[] = [];
  for (let index = 0; index < paragraphRows.length; index++) {
    const row = paragraphRows[index];
    if (!row || typeof row !== "object") continue;
    let text = String(row.text ?? "").trim();
    if (!text && Array.isArray(row.sentences)) {
      text = (row.sentences as Record<string, unknown>[])
        .map((sentence) => String(sentence?.text ?? "").trim())
        .filter(Boolean)
        .join(" ");
    }
    if (!text) continue;
    const speaker = speakerMap.get(String(row.speaker)) ?? {};
    paragraphs.push({
      index,
      speaker: speaker.name ?? (String(row.speaker).toLowerCase() === "operator" ? "Operator" : null),
      role: speaker.role ?? null,
      company: speaker.company ?? null,
      start: row.start ?? null,
      end: row.end ?? null,
      text,
    });
  }
  if (!paragraphs.length) {
    paragraphs = String(transcriptRecord.text ?? "")
      .split(/\n\s*\n/)
      .map((text, index) => ({ index, speaker: null, role: null, company: null, start: null, end: null, text: text.trim() }))
      .filter((paragraph) => paragraph.text);
  }
  if (!paragraphs.length) return null;

  const topicList = (topics ?? []).map((topic) => String(topic).trim()).filter(Boolean);
  const selected = topicList.length
    ? paragraphs.flatMap((paragraph) => {
        const searchable = `${String(paragraph.speaker ?? "")} ${String(paragraph.text ?? "")}`;
        const matchedTopics = topicList.filter((topic) => new RegExp(topic.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(searchable));
        return matchedTopics.length ? [{ ...paragraph, matchedTopics }] : [];
      })
    : paragraphs;
  const offset = Math.min(paragraphCursor, selected.length);
  const page = selected.slice(offset, offset + paragraphLimit);
  const nextCursor = offset + page.length < selected.length ? String(offset + page.length) : null;
  const resolvedEventId = String(metadata.eventId ?? contentRecord.event_id ?? contentRecord.eventId ?? eventId ?? "") || null;
  const fiscalPeriod = String(metadata.fiscalPeriod ?? "") || null;
  const fiscalYearValue = Number(metadata.fiscalYear);
  const fiscalYear = Number.isFinite(fiscalYearValue) ? fiscalYearValue : null;
  let resolvedSourceUrl = sourceUrl;
  if (!resolvedSourceUrl && resolvedEventId && fiscalPeriod && fiscalYear && /^Q[1-4]$/i.test(fiscalPeriod)) {
    resolvedSourceUrl = canonicalYahooTranscriptUrl(ticker, fiscalPeriod, fiscalYear, resolvedEventId);
  }
  const warnings: Record<string, unknown>[] = [];
  if (topicList.length && !selected.length) {
    warnings.push({ code: "NO_TOPIC_MATCHES", message: `No transcript paragraphs matched: ${topicList.join(", ")}` });
  }
  return {
    ticker: ticker.toUpperCase(),
    status: "OK",
    sourceType: "yahoo_quartr",
    provider: "yahoo_finance_quartr",
    evidenceClass: "CONTEXTUAL_TRANSCRIPT",
    decisionGrade: false,
    contentCompleteness: "FULL",
    completenessBasis: "YAHOO_STRUCTURED_TRANSCRIPT_VALIDATED",
    sourceUrl: resolvedSourceUrl,
    eventId: resolvedEventId,
    title: metadata.title ?? null,
    fiscalPeriod,
    fiscalYear,
    eventDate: metadataEpochIso(metadata.date),
    updatedAt: metadataEpochIso(metadata.updated),
    retrievedAt: fetchedAt ?? null,
    cacheStatus,
    contentSha256: await yahooTranscriptContentSha256(
      ticker,
      resolvedEventId,
      fiscalPeriod,
      fiscalYear,
      speakers,
      paragraphs,
    ),
    contentHashBasis: "CANONICAL_TRANSCRIPT_TEXT_V1",
    speakerCount: transcriptRecord.number_of_speakers ?? transcriptRecord.numberOfSpeakers ?? speakers.length,
    speakers,
    filteredByTopics: topicList.length ? topicList : null,
    paragraphs: topicList.length ? null : page,
    matchedParagraphs: topicList.length ? page : [],
    content: null,
    pagination: {
      cursor: String(offset), nextCursor, limit: paragraphLimit, returned: page.length,
      matchingParagraphs: selected.length, totalParagraphs: paragraphs.length, hasMore: nextCursor != null,
      pageExhausted: nextCursor == null,
    },
    warnings,
  };
}

async function attemptYahooQuartrTranscript(
  ticker: string,
  fiscalQuarter: string | null,
  topics: string[] | null,
  eventId: string | null,
  sourceUrl: string | null,
  paragraphLimit: number,
  paragraphCursor: number,
): Promise<{ payload: Record<string, unknown> | null; attempt: Record<string, unknown> }> {
  const result = await fetchYahooQuartrTranscript(ticker, eventId, sourceUrl, fiscalQuarter);
  const attemptExtra: Record<string, unknown> = {
    url: result.sourceUrl,
    eventId: result.eventId,
    fiscalQuarter,
    cacheStatus: result.cacheStatus,
    attempted: result.providerAttempted,
    httpStatus: result.httpStatus,
    message: result.message,
    retryAfter: result.retryAfter,
  };
  if (result.contentDiagnostics) {
    attemptExtra.contentCompleteness = result.contentDiagnostics.contentCompleteness;
    attemptExtra.contentDiagnostics = result.contentDiagnostics;
    if (result.contentDiagnostics.contentCompleteness === "PARTIAL") {
      attemptExtra.reasonCode = result.contentDiagnostics.reasonCode ?? "YAHOO_PREVIEW_ONLY";
      attemptExtra.reason = result.message ?? "Yahoo returned an incomplete transcript preview.";
      attemptExtra.recommendedNextAction = "TRY_ALTERNATE_SOURCE";
    }
  }
  if (result.status !== "OK" || !result.payload) {
    return { payload: null, attempt: transcriptAttempt("yahoo_quartr", result.status, attemptExtra) };
  }
  let expectedQuarter = fiscalQuarter;
  if (!expectedQuarter && result.sourceUrl) {
    expectedQuarter = parseYahooTranscriptSourceUrl(ticker, result.sourceUrl).identity?.fiscalQuarter ?? null;
  }
  const identityError = validateYahooTranscriptPayloadIdentity(
    result.payload, String(result.eventId ?? eventId ?? ""), expectedQuarter,
  );
  if (identityError) {
    return {
      payload: null,
      attempt: transcriptAttempt("yahoo_quartr", "YAHOO_METADATA_MISMATCH", {
        ...attemptExtra,
        reasonCode: "YAHOO_METADATA_MISMATCH",
        reason: identityError,
      }),
    };
  }
  const completeness = assessYahooTranscriptPayloadCompleteness(result.payload);
  if (completeness.contentCompleteness !== "FULL") {
    return {
      payload: null,
      attempt: transcriptAttempt(
        "yahoo_quartr",
        completeness.contentCompleteness === "PARTIAL" ? "INCOMPLETE_TRANSCRIPT" : "PROVIDER_ERROR",
        {
          ...attemptExtra,
          contentCompleteness: completeness.contentCompleteness,
          contentDiagnostics: completeness,
          reasonCode: completeness.reasonCode ?? "TRANSCRIPT_SHAPE_INVALID",
          reason: completeness.contentCompleteness === "PARTIAL"
            ? "Yahoo returned only a preview of the earnings-call transcript."
            : "Yahoo returned transcript data without usable content.",
          recommendedNextAction: "TRY_ALTERNATE_SOURCE",
        },
      ),
    };
  }
  const normalized = await normalizeYahooQuartrPayload(
    ticker, result.payload, result.sourceUrl, result.eventId, topics,
    paragraphLimit, paragraphCursor, result.cacheStatus, result.fetchedAt,
  );
  if (!normalized) {
    return {
      payload: null,
      attempt: transcriptAttempt("yahoo_quartr", "PROVIDER_ERROR", {
        ...attemptExtra,
        reasonCode: "TRANSCRIPT_SHAPE_INVALID",
        reason: "Yahoo returned transcript data without usable paragraphs.",
      }),
    };
  }
  return { payload: normalized, attempt: transcriptAttempt("yahoo_quartr", "SUCCESS", attemptExtra) };
}

const SEC_TRANSCRIPT_MARKER_RE = /\b(?:earnings|conference)\s+call\b|\bprepared\s+remarks\b|\bquestion(?:s)?[- ]and[- ]answer(?:s)?\b|\bopen\s+the\s+call\s+(?:for|to)\s+questions\b/i;
const SEC_TRANSCRIPT_SPEAKER_RE = /\b(Operator|[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,3})(?:\s*[-–—]\s*(?:CEO|CFO|COO|President|Analyst|Investor Relations))?\s*:/g;
const SEC_PRESENTATION_RE = /\b(?:investor presentation|presentation slides?|slide\s+\d+)\b/i;

function validateSecTranscriptText(text: string): { valid: boolean; diagnostics: Record<string, unknown> } {
  const normalized = String(text ?? "").replace(/\s+/g, " ").trim();
  const speakerTurns = [...normalized.matchAll(SEC_TRANSCRIPT_SPEAKER_RE)].map((match) => String(match[1]));
  const distinctSpeakers = new Set(speakerTurns.map((speaker) => speaker.toLowerCase()));
  const callMarkerFound = SEC_TRANSCRIPT_MARKER_RE.test(normalized);
  const presentationMarkersFound = SEC_PRESENTATION_RE.test(normalized);
  const valid = normalized.length >= 300
    && callMarkerFound
    && speakerTurns.length >= 3
    && distinctSpeakers.size >= 2
    && !(presentationMarkersFound && speakerTurns.length < 5);
  return {
    valid,
    diagnostics: {
      textLength: normalized.length,
      callMarkerFound,
      speakerTurnCount: speakerTurns.length,
      distinctSpeakerCount: distinctSpeakers.size,
      presentationMarkersFound,
    },
  };
}

function isSecTranscriptCandidate(exhibit: Record<string, unknown>): boolean {
  const exhibitType = String(exhibit.type ?? "").toUpperCase().replace(/\s+/g, "");
  const description = String(exhibit.description ?? "").toUpperCase();
  return /^EX-99\.0?[23]\b/.test(exhibitType)
    || ["TRANSCRIPT", "CONFERENCE CALL", "PROCEEDINGS", "EARNINGS CALL"].some((keyword) => description.includes(keyword));
}

export function nextTranscriptFallback(attemptedSources: Record<string, unknown>[]): Record<string, unknown> | null {
  const statusBySource = new Map(attemptedSources.map((a) => [String(a.sourceType), String(a.status)]));
  const reasonBySource = new Map(attemptedSources.map((a) => [String(a.sourceType), String(a.reasonCode ?? "")]));
  // A source the tool tried that failed for a reason the caller can fix comes first (3.4.0, BR-ERN-009):
  // Yahoo matched another event, or Alpha Vantage was not queried only because the fiscal quarter was unknown.
  if (statusBySource.get("yahoo_quartr") === "YAHOO_METADATA_MISMATCH") {
    return { sourceType: "yahoo_quartr", action: "Retry with a validated Yahoo earnings-call source_url or event_id." };
  }
  const alphaConfigured = Boolean(getWorkerVar("ALPHA_VANTAGE_API_KEY") ?? getWorkerVar("ALPHAVANTAGE_API_KEY"));
  if (statusBySource.get("alpha_vantage") === "NOT_ELIGIBLE" && reasonBySource.get("alpha_vantage") === "FISCAL_QUARTER_UNRESOLVED" && alphaConfigured) {
    return { sourceType: "alpha_vantage", action: "Retry with fiscal_quarter as YYYYQn so Alpha Vantage can be queried for that quarter." };
  }
  if (!statusBySource.has("company_ir") || statusBySource.get("company_ir") === "SKIPPED") {
    return { sourceType: "company_ir", action: "Provide or discover a company IR earnings-call/transcript URL, then call parse_public_transcript." };
  }
  if (!statusBySource.has("public_transcript_url") || statusBySource.get("public_transcript_url") === "SKIPPED") {
    return { sourceType: "public_transcript_url", action: "Call parse_public_transcript with a verified public transcript URL." };
  }
  if (!statusBySource.has("yahoo_quartr") || statusBySource.get("yahoo_quartr") === "SKIPPED") {
    return { sourceType: "yahoo_quartr", action: "Retry with a validated Yahoo earnings-call source_url or event_id." };
  }
  if (!statusBySource.has("alpha_vantage") || statusBySource.get("alpha_vantage") === "SKIPPED") {
    return { sourceType: "alpha_vantage", action: "Configure ALPHA_VANTAGE_API_KEY and retry when a fiscal quarter is known." };
  }
  return null;
}

type TranscriptFiscalQuarterResolution = {
  fiscalQuarter: string | null;
  fiscalQuarterStatus: "EXPLICIT" | "OFFICIAL_RELEASE_RESOLVED" | "UNRESOLVED";
  periodEvidence: string | null;
};

function alphaVantageQuarter(period: string): string | null {
  const match = String(period ?? "").match(/(\d{4})\s*Q([1-4])/i);
  if (match) return `${match[1]}Q${match[2]}`;
  return null;
}

async function resolveTranscriptFiscalQuarter(
  fiscalQuarter: string | null,
  officialReleaseSource: Record<string, unknown> | null,
): Promise<TranscriptFiscalQuarterResolution> {
  const explicit = alphaVantageQuarter(fiscalQuarter ?? "");
  if (explicit) {
    return { fiscalQuarter: explicit, fiscalQuarterStatus: "EXPLICIT", periodEvidence: null };
  }
  if (officialReleaseSource) {
    const periodInfo = await resolveEarningsPeriodFromSource(officialReleaseSource);
    const resolved = alphaVantageQuarter(periodInfo.period ?? "");
    if (resolved) {
      return {
        fiscalQuarter: resolved,
        fiscalQuarterStatus: "OFFICIAL_RELEASE_RESOLVED",
        periodEvidence: periodInfo.periodEvidence,
      };
    }
  }
  return { fiscalQuarter: null, fiscalQuarterStatus: "UNRESOLVED", periodEvidence: null };
}

async function attemptAlphaVantageTranscript(
  ticker: string,
  fiscalQuarter: string | null,
  officialReleaseSource: Record<string, unknown> | null,
  topics: string[] | null,
): Promise<{
  payload: Record<string, unknown> | null;
  attempt: Record<string, unknown>;
  resolution: TranscriptFiscalQuarterResolution;
}> {
  const resolution = await resolveTranscriptFiscalQuarter(fiscalQuarter, officialReleaseSource);
  if (!resolution.fiscalQuarter) {
    return {
      payload: null,
      attempt: transcriptAttempt("alpha_vantage", "NOT_ELIGIBLE", {
        reasonCode: "FISCAL_QUARTER_UNRESOLVED",
        reason: "Alpha Vantage requires an issuer fiscal quarter. Provide fiscal_quarter as YYYYQn or retry after an official earnings release is available.",
      }),
      resolution,
    };
  }
  const alpha = await fetchAlphaVantageTranscript(ticker, resolution.fiscalQuarter, topics);
  return { ...alpha, resolution };
}

function transcriptOfficialReleaseSource(
  secSource: Record<string, unknown>,
  exhibits: Record<string, unknown>[],
  cik: number,
  accessionNumber: string,
): Record<string, unknown> | null {
  const release = exhibits.find((exhibit) => {
    const type = String(exhibit.type ?? "").toUpperCase().replace(/\s+/g, "");
    const description = String(exhibit.description ?? "").toUpperCase();
    return /^EX-99\.0?1\b/.test(type)
      || (type.startsWith("EX-99") && /PRESS RELEASE|EARNINGS RELEASE|RESULTS RELEASE/.test(description));
  });
  const document = String(release?.document ?? "");
  if (!document) return null;
  return {
    ...secSource,
    sourceType: "sec_8k_ex991",
    url: secArchiveDocumentUrl(cik, accessionNumber.replace(/-/g, ""), document),
  };
}

async function fetchAlphaVantageTranscript(
  ticker: string,
  quarter: string,
  topics: string[] | null = null,
): Promise<{ payload: Record<string, unknown> | null; attempt: Record<string, unknown> }> {
  const result = await fetchAlphaVantageJson(
    "EARNINGS_CALL_TRANSCRIPT",
    { symbol: ticker.toUpperCase(), quarter },
    ALPHA_TRANSCRIPT_TTL_MS,
  );
  const safeUrl = result.publicUrl;
  const rateLimit = {
    provider: "alpha_vantage",
    used: result.providerAttempted,
    note: "Alpha Vantage free-tier rate limits may apply.",
  };
  if (result.status !== "OK" || !result.payload) {
    return {
      payload: null,
      attempt: transcriptAttempt("alpha_vantage", result.status, {
        url: safeUrl,
        quarter,
        httpStatus: result.httpStatus,
        message: result.message,
        retryAfter: result.retryAfter,
        cacheStatus: result.cacheStatus,
        rateLimit,
        reasonCode: result.status === "SOURCE_UNCONFIGURED" ? "ALPHA_VANTAGE_API_KEY_MISSING" : null,
        reason: result.status === "SOURCE_UNCONFIGURED" ? "ALPHA_VANTAGE_API_KEY not configured." : null,
      }),
    };
  }
  const json = result.payload;
  const rows = Array.isArray(json.transcript) ? json.transcript as Record<string, unknown>[] : [];
  const paragraphs = rows
    .map((row) => {
      const speaker = String(row.speaker ?? row.speaker_name ?? "").trim();
      const content = String(row.content ?? row.text ?? "").trim();
      return content ? (speaker ? `${speaker}: ${content}` : content) : "";
    })
    .filter(Boolean);
  if (!paragraphs.length) {
    return { payload: null, attempt: transcriptAttempt("alpha_vantage", "NOT_FOUND", { url: safeUrl, quarter, rateLimit, cacheStatus: result.cacheStatus }) };
  }
  const cleanText = paragraphs.join("\n\n");
  const warnings: Record<string, unknown>[] = [];
  const topicList = (topics ?? []).filter((topic) => typeof topic === "string" && topic.trim()).map((topic) => topic.trim());
  if (topicList.length) {
    const matchedParagraphs = filterParagraphsByTopics(cleanText, topicList);
    if (!matchedParagraphs.length) warnings.push({ code: "NO_TOPIC_MATCHES", message: `No paragraphs matched the provided topics: ${topicList.join(", ")}` });
    return {
      payload: {
        sourceType: "alpha_vantage",
        status: "OK",
        evidenceClass: "CONTEXTUAL_TRANSCRIPT",
        decisionGrade: false,
        contentCompleteness: "FULL",
        completenessBasis: "ALPHA_TRANSCRIPT_ROWS_PRESENT",
        filteredByTopics: topicList,
        matchedParagraphs,
        content: null,
        totalTextLength: cleanText.length,
        truncated: false,
        warnings,
      },
      attempt: transcriptAttempt("alpha_vantage", "SUCCESS", { url: safeUrl, quarter, rateLimit, cacheStatus: result.cacheStatus }),
    };
  }
  const maxChars = 50_000;
  const truncated = cleanText.length > maxChars;
  if (truncated) warnings.push({ code: "TEXT_TRUNCATED", message: `Text truncated from ${cleanText.length} to ${maxChars} characters.` });
  return {
    payload: {
      sourceType: "alpha_vantage",
      status: "OK",
      evidenceClass: "CONTEXTUAL_TRANSCRIPT",
      decisionGrade: false,
      contentCompleteness: "FULL",
      completenessBasis: "ALPHA_TRANSCRIPT_ROWS_PRESENT",
      filteredByTopics: null,
      content: cleanText.slice(0, maxChars),
      totalTextLength: cleanText.length,
      truncated,
      warnings,
    },
    attempt: transcriptAttempt("alpha_vantage", "SUCCESS", { url: safeUrl, quarter, rateLimit, cacheStatus: result.cacheStatus }),
  };
}

export async function getEarningsCallTranscript(
  ticker: string,
  period: string = "latest",
  topics: string[] | null = null,
  fiscalQuarter: string | null = null,
  eventId: string | null = null,
  sourceUrl: string | null = null,
  paragraphLimit: number = 20,
  paragraphCursor: number = 0,
): Promise<string> {
  const attemptedSources: Record<string, unknown>[] = [];
  let normalizedFiscalQuarter = String(fiscalQuarter ?? "").trim().toUpperCase() || null;
  let normalizedEventId = String(eventId ?? "").trim() || null;
  let normalizedSourceUrl = String(sourceUrl ?? "").trim() || null;
  if (normalizedSourceUrl) {
    const parsed = parseYahooTranscriptSourceUrl(ticker, normalizedSourceUrl);
    if (!parsed.identity) {
      return JSON.stringify({ status: "INPUT_VALIDATION_ERROR", message: parsed.error });
    }
    if (normalizedEventId && normalizedEventId !== parsed.identity.eventId) {
      return JSON.stringify({ status: "INPUT_VALIDATION_ERROR", message: "event_id does not match source_url." });
    }
    if (normalizedFiscalQuarter && normalizedFiscalQuarter !== parsed.identity.fiscalQuarter) {
      return JSON.stringify({ status: "INPUT_VALIDATION_ERROR", message: "fiscal_quarter does not match source_url." });
    }
    normalizedEventId = normalizedEventId ?? parsed.identity.eventId;
    normalizedFiscalQuarter = normalizedFiscalQuarter ?? parsed.identity.fiscalQuarter;
    normalizedSourceUrl = parsed.identity.sourceUrl;
  }
  const pageLimit = Math.min(50, Math.max(1, Math.trunc(paragraphLimit)));
  const pageCursor = Math.max(0, Math.trunc(paragraphCursor));
  const explicitYahoo = Boolean(normalizedEventId || normalizedSourceUrl);
  if (explicitYahoo) {
    const yahooMark = budgetRefusalCount();
    const yahoo = await attemptYahooQuartrTranscript(
      ticker, normalizedFiscalQuarter, topics, normalizedEventId, normalizedSourceUrl, pageLimit, pageCursor,
    );
    attemptedSources.push(budgetCheckedAttempt(yahoo.attempt, yahooMark));
    if (yahoo.payload) {
      const { warnings = [], ...payload } = yahoo.payload;
      return JSON.stringify({
        ...payload,
        period,
        fiscalQuarter: normalizedFiscalQuarter,
        fiscalQuarterStatus: "EXPLICIT_SOURCE",
        attemptedSources,
        nextRecommendedFallback: null,
        warnings,
      });
    }
  }

  let secFallback: Record<string, unknown>;
  let officialReleaseSource: Record<string, unknown> | null = null;
  const secMark = budgetRefusalCount();
  const secSource = await resolveLatestEarningsSecSource(ticker);
  if (!secSource) {
    attemptedSources.push(budgetCheckedAttempt(transcriptAttempt("sec_8k_exhibit", "NOT_FOUND"), secMark));
    secFallback = {
      ticker: ticker.toUpperCase(),
      period,
      status: "SEC_8K_NOT_FOUND",
      message: "No recent SEC 8-K filing found for this ticker.",
      content: null,
    };
  } else {
    const accessionNumber = String(secSource.accessionNumber ?? "");
    // The issuer's CIK comes from the ticker; the accession prefix is only a fallback.
    const { cikPadded: issuerCik } = await getSubmissionsForTicker(ticker);
    let cik: number | null = issuerCik ? parseInt(issuerCik, 10) : null;
    if (!cik) cik = edgarCikFromAccession(accessionNumber);
    if (!cik) {
      attemptedSources.push(budgetCheckedAttempt(transcriptAttempt("sec_8k_exhibit", "FAILED", {
        accessionNumber, reason: "CIK resolution failed.",
      }), secMark));
      secFallback = {
        ticker: ticker.toUpperCase(), period, status: "CIK_RESOLUTION_FAILED",
        message: "Could not resolve CIK for ticker.", content: null,
      };
    } else {
      const { edgarIndexUrl } = edgarBuildFilingUrls(cik, accessionNumber, null);
      const exhibits = await edgarListExhibitsFromIndex(edgarIndexUrl);
      officialReleaseSource = transcriptOfficialReleaseSource(secSource, exhibits, cik, accessionNumber);
      const candidates = exhibits.filter(isSecTranscriptCandidate);
      const candidateDiagnostics: Record<string, unknown>[] = [];
      let validated: {
        exhibit: Record<string, unknown>;
        documentUrl: string;
        cleanText: string;
        validation: Record<string, unknown>;
      } | null = null;
      for (const exhibit of candidates.slice(0, 4)) {
        const fileName = String(exhibit.document ?? "");
        if (!fileName) {
          candidateDiagnostics.push({
            type: exhibit.type ?? null, document: null, status: "REJECTED", reasonCode: "MISSING_DOCUMENT_NAME",
          });
          continue;
        }
        const documentUrl = secArchiveDocumentUrl(cik, accessionNumber.replace(/-/g, ""), fileName);
        const html = await edgarGetHtml(documentUrl, 5_000_000);
        if (!html) {
          candidateDiagnostics.push({ type: exhibit.type ?? null, document: fileName, url: documentUrl, status: "FETCH_ERROR" });
          continue;
        }
        const cleanText = htmlToReadableText(html);
        const check = validateSecTranscriptText(cleanText);
        candidateDiagnostics.push({
          type: exhibit.type ?? null, document: fileName, url: documentUrl,
          status: check.valid ? "VALIDATED" : "REJECTED", validation: check.diagnostics,
        });
        if (check.valid) {
          validated = { exhibit, documentUrl, cleanText, validation: check.diagnostics };
          break;
        }
      }

      if (validated) {
        attemptedSources.push(transcriptAttempt("sec_8k_exhibit", "SUCCESS", {
          url: validated.documentUrl,
          accessionNumber,
          filingDate: secSource.filingDate ?? null,
          validation: validated.validation,
        }));
        const warnings: Record<string, unknown>[] = [];
        const common = {
          ticker: ticker.toUpperCase(), period, status: "OK", sourceType: "sec_8k_exhibit",
          evidenceClass: "SEC_FILING", decisionGrade: true, sourceUrl: validated.documentUrl,
          contentCompleteness: "FULL", completenessBasis: "CONTENT_VALIDATED_SEC_EXHIBIT",
          accessionNumber, filingDate: secSource.filingDate ?? null, exhibitType: validated.exhibit.type ?? null,
          documentUrl: validated.documentUrl, totalTextLength: validated.cleanText.length,
          attemptedSources, nextRecommendedFallback: null,
        };
        const topicList = (topics ?? []).map((topic) => String(topic).trim()).filter(Boolean);
        if (topicList.length) {
          const matchedParagraphs = filterParagraphsByTopics(validated.cleanText, topicList);
          if (!matchedParagraphs.length) {
            warnings.push({ code: "NO_TOPIC_MATCHES", message: `No paragraphs matched the provided topics: ${topicList.join(", ")}` });
          }
          return JSON.stringify({
            ...common, filteredByTopics: topicList, matchedParagraphs, content: null, warnings,
          });
        }
        const maxChars = 50_000;
        const truncated = validated.cleanText.length > maxChars;
        if (truncated) {
          warnings.push({ code: "TEXT_TRUNCATED", message: `Text truncated from ${validated.cleanText.length} to ${maxChars} characters.` });
        }
        return JSON.stringify({
          ...common, filteredByTopics: null, content: validated.cleanText.slice(0, maxChars), truncated, warnings,
        });
      }

      attemptedSources.push(budgetCheckedAttempt(transcriptAttempt("sec_8k_exhibit", "NOT_FOUND", {
        url: edgarIndexUrl,
        accessionNumber,
        filingDate: secSource.filingDate ?? null,
        exhibitsSearched: exhibits.length,
        candidatesChecked: candidateDiagnostics.length,
        candidateDiagnostics,
      }), secMark));
      secFallback = {
        ticker: ticker.toUpperCase(), period, status: "SEC_EXHIBIT_NOT_FOUND", accessionNumber,
        filingDate: secSource.filingDate ?? null,
        availableExhibits: exhibits.map((exhibit) => ({
          type: exhibit.type ?? "", description: exhibit.description ?? "", document: exhibit.document ?? "",
        })),
        candidateDiagnostics,
        message: "8-K filing found but no content-validated transcript exhibit was detected.",
        content: null,
      };
    }
  }

  attemptedSources.push(transcriptAttempt("company_ir", "SKIPPED", {
    reason: "No approved company IR transcript source was configured.",
  }));
  if (!explicitYahoo) {
    const yahooMark = budgetRefusalCount();
    const yahoo = await attemptYahooQuartrTranscript(
      ticker, normalizedFiscalQuarter, topics, null, null, pageLimit, pageCursor,
    );
    attemptedSources.push(budgetCheckedAttempt(yahoo.attempt, yahooMark));
    if (yahoo.payload) {
      const metadataQuarter = yahoo.payload.fiscalYear && yahoo.payload.fiscalPeriod
        ? `${yahoo.payload.fiscalYear}${String(yahoo.payload.fiscalPeriod).toUpperCase()}`
        : null;
      const { warnings = [], ...payload } = yahoo.payload;
      return JSON.stringify({
        ...payload,
        period,
        fiscalQuarter: normalizedFiscalQuarter ?? metadataQuarter,
        fiscalQuarterStatus: normalizedFiscalQuarter ? "EXPLICIT" : "YAHOO_METADATA_RESOLVED",
        attemptedSources,
        nextRecommendedFallback: null,
        warnings,
      });
    }
  }

  attemptedSources.push(transcriptAttempt("public_transcript_url", "SKIPPED", {
    reason: "No non-Yahoo public transcript URL was provided to parse_public_transcript.",
  }));
  const alphaMark = budgetRefusalCount();
  const alpha = await attemptAlphaVantageTranscript(ticker, normalizedFiscalQuarter, officialReleaseSource, topics);
  attemptedSources.push(budgetCheckedAttempt(alpha.attempt, alphaMark));
  if (alpha.payload) {
    return JSON.stringify({
      ticker: ticker.toUpperCase(), period, ...alpha.resolution, ...alpha.payload,
      attemptedSources, nextRecommendedFallback: null,
    });
  }
  const incompleteYahoo = [...attemptedSources].reverse().find(
    (attempt) => attempt.sourceType === "yahoo_quartr" && attempt.status === "INCOMPLETE_TRANSCRIPT",
  );
  if (incompleteYahoo) {
    secFallback = {
      ...secFallback,
      status: "INCOMPLETE_TRANSCRIPT",
      sourceType: "yahoo_quartr",
      provider: "yahoo_finance_quartr",
      evidenceClass: "CONTEXTUAL_TRANSCRIPT",
      decisionGrade: false,
      sourceUrl: incompleteYahoo.url ?? null,
      eventId: incompleteYahoo.eventId ?? null,
      contentCompleteness: "PARTIAL",
      recommendedNextAction: "TRY_ALTERNATE_SOURCE",
      message: incompleteYahoo.reason ?? incompleteYahoo.message ?? "Yahoo returned an incomplete transcript preview.",
    };
  }
  return JSON.stringify({
    ...secFallback,
    ...alpha.resolution,
    attemptedSources,
    nextRecommendedFallback: incompleteYahoo
      ? {
          sourceType: "alternate_transcript_source",
          action: "Provide a verified public transcript URL or retry later; do not treat the preview as a full call.",
        }
      : nextTranscriptFallback(attemptedSources),
  });
}

// ── Dilution bridge, capital structure, analyst methods, UK filings (2.3.0) ──

/** The newest earnings-release 8-Ks (Item 2.02, results of operations) in SEC's recent submissions; 8-K/As too when asked. */
export function earningsReleasePicks(submissions: Record<string, unknown>, cikInt: number, limit: number, includeAmendments = false): { filingDate: string; accessionNumber: string; primaryUrl: string; form: string }[] {
  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form as string[]) ?? [];
  const picks: { filingDate: string; accessionNumber: string; primaryUrl: string; form: string }[] = [];
  for (let i = 0; i < forms.length && picks.length < limit; i++) {
    const form = (forms[i] ?? "").toUpperCase();
    if (!(form === "8-K" || (includeAmendments && form === "8-K/A")) || !String(recent.items?.[i] ?? "").includes("2.02")) continue;
    const acc = String(recent.accessionNumber?.[i] ?? "");
    const doc = String(recent.primaryDocument?.[i] ?? "");
    if (!acc || !doc) continue;
    picks.push({ filingDate: String(recent.filingDate?.[i] ?? ""), accessionNumber: acc, primaryUrl: secArchiveDocumentUrl(cikInt, acc.replace(/-/g, ""), doc), form });
  }
  return picks;
}

/** An earnings-release 8-K's text: EX-99.1 when the index names one, else the 8-K itself. */
export async function readReleaseText(cikInt: number, accessionNumber: string, primaryUrl: string): Promise<{ url: string; status: string; text: string | null }> {
  const ex991 = await resolveEx991Url(cikInt, accessionNumber).catch(() => null);
  const url = ex991 ?? primaryUrl;
  const html = await edgarGetHtml(url, 5_000_000).catch(() => null);
  return html ? { url, status: "READ", text: _stripHtmlTagsIdx(_sanitizeFilingHtml(html)) } : { url, status: "NOT_READ", text: null };
}

// ── Historical valuation context and metric reconciliation (2.5.4) ────────

/** Daily closes (Yahoo split-adjusted), split events and the quote currency from fromDate to now. */
async function dailyHistory(ticker: string, fromDate: string): Promise<{ bars: Bar[]; splits: Split[]; currency: string | null } | null> {
  const p1 = Math.floor(Date.parse(`${fromDate}T00:00:00Z`) / 1000);
  const p2 = Math.floor(Date.now() / 1000);
  const d = (await yGet(yahooChartUrl(enc(ticker), `period1=${p1}&period2=${p2}&interval=1d&events=split`), false)) as Record<string, unknown>;
  const result = (d?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as Record<string, unknown> | undefined;
  if (!result) return null;
  const meta = (result.meta as Record<string, unknown> | undefined) ?? {};
  const tz = typeof meta.exchangeTimezoneName === "string" ? meta.exchangeTimezoneName : "UTC";
  const timestamps = (result.timestamp as number[]) ?? [];
  const closes = (((result.indicators as Record<string, unknown[]>)?.quote?.[0] as Record<string, (number | null)[]>) ?? {}).close ?? [];
  const bars: Bar[] = [];
  timestamps.forEach((t, i) => {
    const date = dailyBarDate(t, tz);
    const close = closes[i];
    if (date && typeof close === "number" && Number.isFinite(close)) bars.push({ date, close });
  });
  const rawSplits = (((result.events as Record<string, unknown> | undefined)?.splits ?? {}) as Record<string, Record<string, unknown>>);
  const splits: Split[] = Object.values(rawSplits)
    .map((e) => ({ date: dailyBarDate(Number(e.date), tz) ?? "", ratio: Number(e.numerator) / Number(e.denominator) }))
    .filter((e) => e.date !== "" && Number.isFinite(e.ratio) && e.ratio > 0)
    .sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
  return { bars, splits, currency: typeof meta.currency === "string" ? meta.currency : null };
}

// Cover pages sit at the top of a periodic report with the header contexts; the first 900 KB is read, not
// the whole report (ASTS's 10-Q is 3.5 MB, its class counts end near 194 KB).
const COVER_READ_MAX_BYTES = 900_000;

const PERIODIC_REPORT_FORMS = new Set(["10-K", "10-Q", "20-F", "40-F"]);

/** The filer's periodic reports in SEC's recent submissions, with their primary documents. */
function periodicReports(submissions: Record<string, unknown> | null, cikPadded: string | null): (PeriodicFiling & { documentUrl: string })[] {
  if (!submissions || !cikPadded) return [];
  const recent = ((submissions.filings as Record<string, unknown> | undefined)?.recent ?? {}) as Record<string, unknown[]>;
  const out: (PeriodicFiling & { documentUrl: string })[] = [];
  const forms = (recent.form ?? []) as string[];
  for (let i = 0; i < forms.length; i++) {
    const form = String(forms[i] ?? "").toUpperCase();
    const acc = String(recent.accessionNumber?.[i] ?? "");
    const doc = String(recent.primaryDocument?.[i] ?? "");
    if (!PERIODIC_REPORT_FORMS.has(form) || !acc || !doc) continue;
    out.push({
      accessionNumber: acc,
      form,
      filed: String(recent.filingDate?.[i] ?? ""),
      reportDate: recent.reportDate?.[i] ? String(recent.reportDate[i]) : null,
      documentUrl: secArchiveDocumentUrl(parseInt(cikPadded, 10), acc.replace(/-/g, ""), doc),
    });
  }
  return out;
}

/** Per-class cover-page share counts from the periodic reports companyfacts cannot give a point-in-time count for. */
async function coverCountsFor(companyfacts: unknown, reports: (PeriodicFiling & { documentUrl: string })[], dates: string[]): Promise<Record<string, Record<string, unknown>>> {
  const out: Record<string, Record<string, unknown>> = {};
  for (const n of coverReadsNeeded(companyfacts, reports, dates)) {
    const url = (n as PeriodicFiling & { documentUrl: string }).documentUrl;
    const html = await edgarGetHtml(url, COVER_READ_MAX_BYTES).catch(() => null);
    out[n.accessionNumber] = html ? { ...coverShareCounts(html), documentUrl: url } : { status: "NOT_READ", value: null, documentUrl: url };
  }
  return out;
}

async function valuationHistoryFor(ticker: string, dates: string[] | null, fromDate: string): Promise<Record<string, unknown>> {
  const upper = ticker.toUpperCase();
  const cikPadded = await resolveCikForTicker(ticker);
  if (!cikPadded) {
    const tickerIndex = await getEdgarTickerCikMap().catch(() => null);
    const code = tickerIndex ? "NO_SEC_REGISTRANT" : "SEC_LOOKUP_UNAVAILABLE";
    return { ticker: upper, status: code, code, retryable: !tickerIndex, ...AUTHORITY_BOUNDARY };
  }
  const [facts, history] = await Promise.all([
    edgarGetJson(secCompanyFactsUrl(cikPadded)).catch(() => null),
    dailyHistory(ticker, fromDate).catch(() => null),
  ]);
  if (!facts) return { ticker: upper, status: "COMPANYFACTS_NOT_AVAILABLE", code: "COMPANYFACTS_NOT_AVAILABLE", retryable: true, ...AUTHORITY_BOUNDARY };
  if (!history || history.bars.length === 0) return { ticker: upper, status: "PRICE_HISTORY_NOT_AVAILABLE", code: "PRICE_HISTORY_NOT_AVAILABLE", retryable: true, ...AUTHORITY_BOUNDARY };
  const useDates = dates ?? valuationDates(null, history.bars[history.bars.length - 1].date).dates;
  const { currency } = taxonomyOf(facts);
  const priceCurrency = history.currency ? majorPrice(1, history.currency).currency : null;
  const warnings: Record<string, unknown>[] = [];
  // One FX series per reporting currency the dates use (2.5.32, F-029: NBIS reported in RUB, then USD).
  const fxByCurrency: Record<string, { pair: string; bars: Bar[] }> = {};
  if (priceCurrency) {
    for (const c of reportingCurrencies(facts, useDates)) {
      if (c === priceCurrency) continue;
      const pair = `${c}${priceCurrency}=X`;
      const fxHistory = await dailyHistory(pair, fromDate).catch(() => null);
      fxByCurrency[c] = { pair, bars: fxHistory?.bars ?? [] };
    }
  }
  const fx = currency ? fxByCurrency[currency] ?? null : null;
  let ratio: number | null = null;
  if (foreignFiler(facts)) {
    const market = await valuationMarketInputs(ticker).catch(() => null);
    const sec = latestShareCount(facts);
    const quoted = market?.sharesOutstanding ?? null;
    const r = sec != null && quoted != null ? adsRatio(sec, quoted) : null;
    if (r != null && r !== 1) {
      ratio = r;
      warnings.push({ code: "ADR_RATIO_APPLIED", message: `SEC counts ${sec} ordinary shares; Yahoo counts ${quoted} quoted shares, ${r} ordinary shares each. Share counts are divided by ${r} at every date.`, severity: "info" });
    }
  }
  const { submissions } = await getSubmissionsForTicker(ticker).catch(() => ({ submissions: null }));
  const reports = periodicReports(submissions, cikPadded);
  const coverCounts = await coverCountsFor(facts, reports, useDates);
  const periodicFilings = reports.map(({ accessionNumber, form, filed, reportDate }) => ({ accessionNumber, form, filed, reportDate }));
  const out = historicalValuation({ ticker, dates: useDates, companyfacts: facts, bars: history.bars, priceCurrency: history.currency, splits: history.splits, fx, fxByCurrency, adsRatio: ratio, coverCounts, periodicFilings });
  if (!submissions) warnings.push({ code: "SUBMISSIONS_NOT_AVAILABLE", message: "SEC submissions could not be read, so no cover page was read; filers without an undimensioned share count have no market cap. Retry.", severity: "warning" });
  out.warnings = warnings;
  return out;
}

/** Historical valuation context (2.5.4): point-in-time market value, EV and multiples at dates, for a ticker and caller-named peers. */
export async function getHistoricalValuationContext(ticker: string, dates: string[] | null = null, peers: string[] | null = null): Promise<string> {
  let requested: string[] | null = null;
  if (dates && dates.length > 0) {
    const check = valuationDates(dates.map((d) => String(d).trim()), null);
    if (check.error) return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: check.error });
    requested = check.dates;
  }
  const peerList = [...new Set((peers ?? []).map((p) => String(p).trim().toUpperCase()).filter((p) => p !== "" && p !== ticker.toUpperCase()))];
  if (peerList.length > 5) return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "At most 5 peers." });
  const today = new Date().toISOString().slice(0, 10);
  const earliestRaw = requested ? requested[0] : `${Number(today.slice(0, 4)) - 5}${today.slice(4)}`;
  const earliest = earliestRaw.slice(4) === "-02-29" ? `${earliestRaw.slice(0, 4)}-02-28` : earliestRaw;
  const fromDate = new Date(Date.parse(`${earliest}T00:00:00Z`) - 15 * 86_400_000).toISOString().slice(0, 10);
  const subject = await valuationHistoryFor(ticker, requested, fromDate);
  const finalDates = requested ?? ((subject.points ?? []) as Record<string, unknown>[]).map((p) => String(p.date));
  const peerResults: Record<string, unknown>[] = [];
  // One peer at a time: each parsed companyfacts document can be tens of megabytes, and the
  // Worker's memory is bounded; only one is held at once.
  for (const p of peerList) {
    peerResults.push(await valuationHistoryFor(p, finalDates.length > 0 ? finalDates : null, fromDate));
  }
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    status: String(subject.status ?? "NOT_AVAILABLE"),
    dates: finalDates,
    subject,
    peers: peerResults,
    peerMedians: peerList.length > 0 && finalDates.length > 0 ? peerMedians(finalDates, peerResults.filter((p) => Array.isArray(p.points))) : [],
    notes: [
      "Peers are the caller's; their medians are unweighted and count only peers with that multiple on that date. Each peer's denominators are its own fiscal periods, named in its points.",
      "The latest date's close can be intraday while the market is open.",
    ],
    ...AUTHORITY_BOUNDARY,
  });
}

const RELEASE_READ_MAX = 4;
const RECONCILE_INCOME_TYPES = ["TotalRevenue", "NetIncome", "NetIncomeCommonStockholders", "OperatingIncome", "DilutedEPS"];

/** Metric source reconciliation (2.5.4): one metric for one period across SEC XBRL, the issuer release and Yahoo. */
export async function reconcileMetricSources(ticker: string, metric: string, period = "latest", tolerancePct: number = DEFAULT_TOLERANCE_PCT): Promise<string> {
  const upper = ticker.toUpperCase();
  if (!(metric in RECONCILE_METRICS)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: `metric must be one of: ${Object.keys(RECONCILE_METRICS).join(", ")}` });
  }
  // A period it cannot read is an input error before any fetch, not ok:true with INVALID_PERIOD (2.26.5, F60).
  if (!isReconcilePeriod(period)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: RECONCILE_PERIOD_HELP });
  }
  const tolerance = Number.isFinite(tolerancePct) ? Math.min(10, Math.max(0, tolerancePct)) : DEFAULT_TOLERANCE_PCT;
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded) {
    const tickerIndex = await getEdgarTickerCikMap().catch(() => null);
    const code = tickerIndex ? "NO_SEC_REGISTRANT" : "SEC_LOOKUP_UNAVAILABLE";
    return JSON.stringify({ ticker: upper, metric, status: code, code, retryable: !tickerIndex });
  }
  const facts = await edgarGetJson(secCompanyFactsUrl(cikPadded)).catch(() => null);
  if (!facts) return JSON.stringify({ ticker: upper, metric, status: "COMPANYFACTS_NOT_AVAILABLE", code: "COMPANYFACTS_NOT_AVAILABLE", retryable: true });
  // The latest annual report may be missing from companyfacts (2.5.30, F-015): said on every result, found or not.
  const coverageWarning = latestAnnualCoverageWarning(annualFilingsFromSubmissions(submissions), facts);
  const resolved = resolvePeriod(facts, metric, period);
  if (resolved.status !== "OK") return JSON.stringify({ ticker: upper, metric, ...resolved, ...(coverageWarning ? { warnings: [coverageWarning] } : {}) });
  const cikInt = parseInt(cikPadded, 10);
  const end = String(resolved.periodEnd);
  // Results releases (Item 2.02 8-Ks and 8-K/As) filed within 100 days after the period ended. The newest
  // RELEASE_READ_MAX are read, so a later correction is never crowded out by earlier filings; they are kept
  // oldest first, and the latest with a scoped figure wins (an early Item 2.02 8-K, as ASTS filed on
  // 2026-07-15, is not always the results release).
  const inWindow = submissions
    ? earningsReleasePicks(submissions, cikInt, 40, true)
      .filter((p) => p.filingDate > end && (Date.parse(`${p.filingDate}T00:00:00Z`) - Date.parse(`${end}T00:00:00Z`)) / 86_400_000 <= 100)
      .sort((a, b) => (a.filingDate < b.filingDate ? -1 : a.filingDate > b.filingDate ? 1 : a.accessionNumber < b.accessionNumber ? -1 : a.accessionNumber > b.accessionNumber ? 1 : 0))
    : [];
  const candidates = inWindow.slice(-RELEASE_READ_MAX);
  const notRead = inWindow.slice(0, inWindow.length - candidates.length);
  const annual = resolved.periodType === "ANNUAL";
  const spec = RECONCILE_METRICS[metric];
  const readReleases = async (): Promise<Record<string, unknown>[]> => {
    const read: Record<string, unknown>[] = [];
    for (const c of candidates) {
      const r: Record<string, unknown> = await readReleaseText(cikInt, c.accessionNumber, c.primaryUrl)
        .then((x): Record<string, unknown> => ({ ...x, filingDate: c.filingDate, accessionNumber: c.accessionNumber }))
        .catch((): Record<string, unknown> => ({ status: "NOT_READ", url: c.primaryUrl, filingDate: c.filingDate, accessionNumber: c.accessionNumber, text: null }));
      read.push(r);
    }
    return read;
  };
  const [releases, yahooRows] = await Promise.all([
    readReleases(),
    fetchTimeseriesByDate(ticker, annual ? "annual" : "quarterly", spec.kind === "instant" ? ["CashAndCashEquivalents"] : RECONCILE_INCOME_TYPES).catch(() => null),
  ]);
  const out = metricReconciliation({ ticker, metric, period: resolved, companyfacts: facts, releases, yahooRows, tolerancePct: tolerance });
  const warnings: Record<string, unknown>[] = [];
  const unread = releases.filter((r) => r.status === "NOT_READ");
  if (unread.length > 0) warnings.push({ code: "RELEASE_TEXT_NOT_AVAILABLE", message: `${unread.length} earnings release(s) could not be read from SEC; retry.`, severity: "warning" });
  if (yahooRows == null) warnings.push({ code: "YAHOO_NOT_AVAILABLE", message: "Yahoo's statements could not be read; retry.", severity: "warning" });
  out.releaseCandidates = { inWindow: inWindow.length, read: candidates.length, notRead: notRead.map((p) => ({ filingDate: p.filingDate, accessionNumber: p.accessionNumber, form: p.form })) };
  if (notRead.length > 0) warnings.push({ code: "OLDER_RELEASE_CANDIDATES_NOT_READ", message: `${notRead.length} older Item 2.02 filing(s) in the 100-day window were not read; the ${candidates.length} newest were.`, severity: "info" });
  if (coverageWarning) warnings.push(coverageWarning);
  out.retryable = unread.length > 0 || yahooRows == null;
  out.warnings = warnings;
  return JSON.stringify(out);
}

const COMPANIES_HOUSE_API = "https://api.company-information.service.gov.uk";
const COMPANIES_HOUSE_TIMEOUT_MS = 15_000;
const COMPANIES_HOUSE_CATEGORIES = new Set(["accounts", "capital", "mortgage", "confirmation-statement", "resolution", "incorporation", "officers", "persons-with-significant-control", "address", "annotation", "change-of-name", "miscellaneous"]);

async function companiesHouseGet(path: string, key: string): Promise<{ status: number; body: Record<string, unknown> | null }> {
  const resp = await fetchProviderWithTimeout(`${COMPANIES_HOUSE_API}${path}`, {
    headers: { Authorization: `Basic ${btoa(`${key}:`)}`, Accept: "application/json" },
  }, COMPANIES_HOUSE_TIMEOUT_MS);
  if (!resp.ok) {
    await resp.body?.cancel();
    return { status: resp.status, body: null };
  }
  return { status: resp.status, body: await resp.json() as Record<string, unknown> };
}

function companiesHouseError(status: number, step: string): string {
  if (status === 429) return JSON.stringify({ error: true, message: `Companies House ${step}: HTTP 429 rate limited (600 requests per 5 minutes).` });
  if (status === 401 || status === 403) return JSON.stringify({ error: true, code: "PROVIDER_ERROR", status: "AUTH_ERROR", message: `Companies House ${step}: HTTP ${status}; check COMPANIES_HOUSE_API_KEY.` });
  return JSON.stringify({ error: true, code: "PROVIDER_ERROR", message: `Companies House ${step}: HTTP ${status}.` });
}

function chargeRows(payload: Record<string, unknown>): Record<string, unknown>[] {
  const items = Array.isArray(payload.items) ? payload.items as Record<string, unknown>[] : [];
  return items.map((c) => {
    const classification = (c.classification && typeof c.classification === "object" ? c.classification : {}) as Record<string, unknown>;
    const particulars = (c.particulars && typeof c.particulars === "object" ? c.particulars : {}) as Record<string, unknown>;
    const persons = Array.isArray(c.persons_entitled) ? c.persons_entitled as Record<string, unknown>[] : [];
    return {
      chargeCode: c.charge_code ?? null,
      status: c.status ?? null,
      createdOn: c.created_on ?? null,
      deliveredOn: c.delivered_on ?? null,
      satisfiedOn: c.satisfied_on ?? null,
      classification: classification.description ?? null,
      personsEntitled: persons.map((p) => String(p.name ?? "")).filter(Boolean),
      particulars: typeof particulars.description === "string" ? particulars.description.slice(0, 400) : null,
    };
  });
}

export async function getUkCompanyFilings(
  ticker: string | null,
  companyNumber: string | null = null,
  companyName: string | null = null,
  category: string | null = null,
  limit = 25,
  includeCharges = false,
): Promise<string> {
  const key = getWorkerVar("COMPANIES_HOUSE_API_KEY")?.trim();
  const base = { source: "companies_house", sourceType: "uk_companies_house" };
  if (!key) {
    return JSON.stringify({
      ...base,
      ticker,
      status: "SOURCE_UNCONFIGURED",
      code: "SOURCE_UNCONFIGURED",
      message: "Companies House is not configured; set the COMPANIES_HOUSE_API_KEY secret (a free key from the Companies House developer hub).",
      filings: [],
    });
  }
  const cat = category ? category.trim().toLowerCase() : null;
  if (cat && !COMPANIES_HOUSE_CATEGORIES.has(cat)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: `category must be one of: ${[...COMPANIES_HOUSE_CATEGORIES].join(", ")}.` });
  }
  const size = clampInt(limit, 25, 1, 100);
  const warnings: Record<string, unknown>[] = [];
  let number = companyNumber ? companyNumber.trim().toUpperCase() : null;
  if (number && /^\d{1,8}$/.test(number)) number = number.padStart(8, "0");
  if (number && !/^[A-Z0-9]{8}$/.test(number)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "company_number must be an 8-character Companies House number, e.g. 01234567 or SC123456." });
  }
  let matchedBy = "company_number";
  let company: Record<string, unknown> | null = null;
  try {
    if (!number) {
      let name = companyName?.trim() || null;
      matchedBy = "company_name";
      if (!name && ticker) {
        const identity = await resolveNewsCompanyIdentity(ticker);
        name = identity.longName ?? identity.companyName ?? identity.shortName;
        matchedBy = "ticker_issuer_name";
        if (!/\.(?:L|IL)$/i.test(ticker)) {
          warnings.push({ code: "NON_UK_LISTING", message: `${ticker} is not a London listing; the match is by issuer name only.`, severity: "info" });
        }
      }
      if (!name) {
        return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "Provide company_number, company_name, or a ticker whose issuer name can be resolved." });
      }
      const search = await companiesHouseGet(`/search/companies?q=${encodeURIComponent(name)}&items_per_page=20`, key);
      if (!search.body) return companiesHouseError(search.status, "company search");
      const match = pickCompaniesHouseMatch(name, search.body);
      if (!match) {
        const items = Array.isArray(search.body.items) ? search.body.items as Record<string, unknown>[] : [];
        return JSON.stringify({
          ...base,
          ticker,
          status: "COMPANY_NOT_MATCHED",
          searchedName: name,
          candidates: items.slice(0, 5).map((i) => ({ companyNumber: i.company_number ?? null, title: i.title ?? null, companyStatus: i.company_status ?? null, addressSnippet: i.address_snippet ?? null })),
          message: "No registered company name matched the issuer exactly; pass company_number from the candidates.",
          filings: [],
          warnings,
        });
      }
      number = String(match.company_number ?? "");
      company = { companyNumber: number, name: match.title ?? null, status: match.company_status ?? null };
    }
    if (!company) {
      const profile = await companiesHouseGet(`/company/${number}`, key);
      if (profile.status === 404) return JSON.stringify({ ...base, ticker, status: "COMPANY_NOT_FOUND", companyNumber: number, filings: [] });
      if (!profile.body) return companiesHouseError(profile.status, "company profile");
      company = { companyNumber: number, name: profile.body.company_name ?? null, status: profile.body.company_status ?? null };
    }
    const history = await companiesHouseGet(`/company/${number}/filing-history?items_per_page=${size}${cat ? `&category=${cat}` : ""}`, key);
    if (!history.body) return companiesHouseError(history.status, "filing history");
    const out: Record<string, unknown> = {
      ...base,
      ticker,
      status: "OK",
      decisionUse: "USE_OFFICIAL_EVIDENCE",
      company: { ...company, matchedBy },
      category: cat,
      filings: companiesHouseFilings(String(number), history.body),
      totalFilings: typeof history.body.total_count === "number" ? history.body.total_count : null,
      filingHistoryUrl: `https://find-and-update.company-information.service.gov.uk/company/${number}/filing-history`,
      notes: [
        "Companies House holds statutory filings: accounts, share allotments (SH01), charges (MR01), resolutions and confirmation statements.",
        "Market announcements (RNS) such as results, loan-note terms and trading updates are not filed here; read them on the company's investor site.",
        "Documents are PDFs; documentContentUrl needs the same API key, viewerUrl opens the public viewer.",
      ],
      warnings,
    };
    if (includeCharges) {
      const charges = await companiesHouseGet(`/company/${number}/charges`, key);
      if (charges.body) out.charges = chargeRows(charges.body);
      else if (charges.status === 404) out.charges = [];
      else warnings.push({ code: "CHARGES_UNAVAILABLE", message: `Companies House charges: HTTP ${charges.status}.`, severity: "warning" });
    }
    return JSON.stringify(out);
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    if (/abort|timed? ?out/i.test(message)) return JSON.stringify({ error: true, message: "Companies House request timed out." });
    return JSON.stringify({ error: true, code: "PROVIDER_ERROR", message: `Companies House request failed: ${message}` });
  }
}

// ── Valuation snapshot and peer multiples (2.4.0) ─────────────────────────

const SEC_UNAVAILABLE_STATUSES = new Set(["TICKER_NOT_FOUND", "FILING_NOT_FOUND_TRY_OTHER_TYPE", "FILING_TEXT_NOT_AVAILABLE"]);

export async function getValuationSnapshot(
  ticker: string,
  price: number | null = null,
  filingType = "latest",
  includeSecFilings = true,
): Promise<string> {
  if (price != null && !(Number.isFinite(price) && price > 0)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "price must be a positive number when supplied." });
  }
  let market: MarketInputs;
  try {
    market = await valuationMarketInputs(ticker);
  } catch (e) {
    // As compare_peer_valuations (F57): an unknown ticker is TICKER_NOT_FOUND, and no provider URL is shown (3.2.1).
    const failure = peerValuationError(ticker.toUpperCase(), e instanceof Error ? e.message : String(e));
    return JSON.stringify({
      error: true,
      code: failure.code,
      message: failure.code === "TICKER_NOT_FOUND" ? failure.message : `Yahoo quote summary failed for ${ticker.toUpperCase()}: ${failure.message}`,
    });
  }
  const quote = price ?? market.price;
  const secWarnings: Record<string, unknown>[] = [];
  let bridge: Record<string, unknown> | null = null;
  let capital: Record<string, unknown> | null = null;
  if (quote != null && includeSecFilings) {
    const major = majorPrice(quote, market.currency);
    if (major.currency == null || major.currency === "USD") {
      const [b, c] = await Promise.all([
        extractDilutionBridge(ticker, major.price, null, "USD", filingType, null, true).then(parseObjectJson),
        extractCapitalStructure(ticker, filingType, null, false).then(parseObjectJson),
      ]);
      if (b.basis === "MECHANICAL_COMPANY_DISCLOSED") bridge = b;
      if (c.basis === "COMPANY_DISCLOSED") capital = c;
      const status = String(b.status ?? b.code ?? c.status ?? "");
      if (!bridge || !capital) {
        secWarnings.push({
          code: "SEC_FILINGS_UNAVAILABLE",
          message: SEC_UNAVAILABLE_STATUSES.has(status) ? `SEC filings were not read (${status}); Yahoo figures are used.` : "SEC filings could not be read in full; Yahoo figures fill the gaps.",
          severity: "info",
        });
      }
    } else {
      secWarnings.push({ code: "NON_USD_LISTING", message: `SEC filings are read only for USD listings; ${ticker.toUpperCase()} is quoted in ${market.currency}, so Yahoo figures are used.`, severity: "info" });
    }
  }
  return JSON.stringify(valuationSnapshot({ ticker: ticker.toUpperCase(), market, suppliedPrice: price, bridge, capital, secWarnings }));
}

function nowIsoUtc(): string {
  return new Date().toISOString();
}


async function resolveEarningsPeriodFromSource(
  src: Record<string, unknown>,
  resolved?: { url: string; sourceType: string },
): Promise<EarningsPeriodInfo> {
  const content = resolved ?? await resolveEarningsContentSource(src);
  if (!content.url) return { period: null, periodStatus: "UNRESOLVED", periodEvidence: null };
  const html = content.url.startsWith(SEC_ARCHIVES_PREFIX)
    ? await edgarGetHtml(content.url, 5_000_000)
    : await fetchPublicHtml(content.url);
  return html
    ? extractEarningsPeriodFromText(_stripHtmlTagsIdx(_sanitizeFilingHtml(html)))
    : { period: null, periodStatus: "UNRESOLVED", periodEvidence: null };
}

function isPaywalledUrl(url: string): boolean {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return new Set([
      "seekingalpha.com",
      "www.seekingalpha.com",
      "wsj.com",
      "www.wsj.com",
      "bloomberg.com",
      "www.bloomberg.com",
    ]).has(host);
  } catch {
    return false;
  }
}

function classifyEarningsSourceUrl(url: string): { sourceType: "sec_8k" | "company_ir" | null; error: string | null } {
  if (!url || !url.trim()) return { sourceType: null, error: "source_url must be a non-empty string" };
  let u: URL;
  try {
    u = new URL(url.trim());
  } catch {
    return { sourceType: null, error: "source_url must be a valid URL" };
  }
  if (u.protocol !== "https:") return { sourceType: null, error: "source_url must use https" };
  if (isPaywalledUrl(u.toString())) return { sourceType: null, error: "source_url appears paywalled and is not allowed" };
  const check = checkFetchUrl(url.trim());
  if (!check.ok) return { sourceType: null, error: `source_url is not on the fetch allow-list (${check.reason})` };
  const kind = allowedHostKind(check.url);
  if (kind === "sec" && u.toString().startsWith(SEC_ARCHIVES_PREFIX)) return { sourceType: "sec_8k", error: null };
  if (kind === "registry" && approvedRegistryUrlAllowed(url.trim())) return { sourceType: "company_ir", error: null };
  return { sourceType: null, error: "source_url is not on the fetch allow-list (URL path not allowed)" };
}

async function fetchPublicHtml(url: string, maxBytes = 3_000_000): Promise<string | null> {
  try {
    const resp = await providerFetch(url, { headers: { "User-Agent": UA } });
    if (!resp.ok) return null;
    const reader = resp.body?.getReader();
    if (!reader) return await resp.text();
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      chunks.push(value);
      total += value.byteLength;
      if (total >= maxBytes) { await reader.cancel(); break; }
    }
    const merged = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) { merged.set(c, off); off += c.byteLength; }
    return new TextDecoder().decode(merged);
  } catch {
    return null;
  }
}

/** A reported release figure under the 2.5.20 rules (extraction-rules.ts), in units, with its excerpt. */
function extractReleaseTextMetric(text: string, metric: ReleaseMetricName): { value: number | null; rawValue: string | null; scaleBasis: string | null; excerpt: string | null } {
  const hit = releaseTextMetric(text, metric);
  return hit ? { value: hit.value, rawValue: hit.rawValue, scaleBasis: hit.scaleBasis, excerpt: compactExcerpt(hit.sentence, 220) } : { value: null, rawValue: null, scaleBasis: null, excerpt: null };
}

const MANAGEMENT_COMMENTARY_TOPIC_ALIASES: Record<string, string[]> = {
  guidance: ["guidance", "outlook", "expects", "expect", "forecast", "sees", "project", "projects", "reaffirm", "full year", "next quarter"],
  capacity: ["capacity", "production", "manufacturing", "facility", "facilities", "utilization", "ramp", "expansion", "supply"],
  revenue: ["revenue", "sales", "net sales", "top line", "revenue growth", "sales growth", "net sales growth"],
  margin: ["margin", "gross margin", "operating margin", "profitability"],
  ai: ["ai", "artificial intelligence"],
};

function managementCommentaryTerms(topic: string): string[] {
  const topicLower = topic.toLowerCase().trim();
  const aliases = MANAGEMENT_COMMENTARY_TOPIC_ALIASES[topicLower] ?? [];
  return Array.from(new Set([topicLower, ...aliases].filter(Boolean)));
}

/**
 * A commentary term's singular form (2.26.5, F55): "tariffs" -> "tariff", "supplies" -> "supply", "taxes" -> "tax".
 * Short words and words ending in -ss, -is or -us are kept ("news", "gas", "business", "analysis", "status").
 */
export function commentarySingular(word: string): string {
  const w = word.toLowerCase();
  if (!/^[a-z]+$/.test(w) || w.length < 5) return w;
  if (w.endsWith("ies")) return `${w.slice(0, -3)}y`;
  if (/(?:ss|sh|ch|x|z)es$/.test(w)) return w.slice(0, -2);
  if (w.endsWith("s") && !/[siu]s$/.test(w)) return w.slice(0, -1);
  return w;
}

/** The regex source for a commentary term: its last word in singular or plural form (2.26.5, F55). */
function commentaryTermPattern(term: string): string {
  const esc = (v: string) => v.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const words = term.trim().split(/\s+/).filter(Boolean);
  const last = words.pop() ?? "";
  const singular = commentarySingular(last);
  const lastPattern = !/^[a-z]+$/i.test(last) || last.length < 3
    ? esc(last)
    : /[^aeiou]y$/.test(singular)
      ? `${esc(singular.slice(0, -1))}(?:y|ies)`
      : `${esc(singular)}(?:e?s)?`;
  return [...words.map(esc), lastPattern].join("\\s+");
}

function commentaryTermMatches(text: string, term: string): boolean {
  return new RegExp(`\\b${commentaryTermPattern(term)}\\b`, "i").test(text);
}

function isManagementCommentaryBoilerplate(sentence: string): boolean {
  return /emerging growth company|smaller reporting company|accelerated filer|shell company|check mark|exchange act|securities act/i.test(sentence);
}

export function sentenceForTopic(text: string, topic: string): { excerpt: string; matchedTerms: string[] } | null {
  const terms = managementCommentaryTerms(topic);
  const sentences = text
    .replace(/\s+/g, " ")
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
  let best: { score: number; excerpt: string; matchedTerms: string[] } | null = null;
  for (let i = 0; i < sentences.length; i++) {
    const sentence = sentences[i];
    if (isManagementCommentaryBoilerplate(sentence)) continue;
    const matchedTerms = terms.filter((term) => commentaryTermMatches(sentence, term));
    if (!matchedTerms.length) continue;
    const exactMatch = commentaryTermMatches(sentence, topic);
    const score = (exactMatch ? 100 : 0) + matchedTerms.length * 10 - Math.min(sentence.length / 200, 3);
    const context = sentence.length < 90 && sentences[i + 1]
      ? `${sentence} ${sentences[i + 1]}`
      : sentence;
    const excerpt = compactExcerpt(context, 320);
    // The evidence excerpt (its first 240 characters) must show a matched term: a long unpunctuated run such as an
    // income-statement header matched "Greater China" past its cut (2.25.4 audit, F36).
    const shown = matchedTerms.filter((term) => commentaryTermMatches(excerpt.slice(0, 240), term));
    if (!shown.length) continue;
    const candidate = { score, excerpt, matchedTerms };
    if (!best || candidate.score > best.score) best = candidate;
  }
  return best ? { excerpt: best.excerpt, matchedTerms: best.matchedTerms } : null;
}

async function resolveEarningsContentSource(src: Record<string, unknown>): Promise<{ url: string; sourceType: string }> {
  const srcUrl = String(src.url ?? "");
  const sourceType = String(src.sourceType ?? "company_ir");
  if (!srcUrl.startsWith(SEC_ARCHIVES_PREFIX)) return { url: srcUrl, sourceType };

  const accessionNumber = String(src.accessionNumber ?? "");
  const cikInt = parseInt(String(src.cikPadded ?? "0"), 10);
  if (!accessionNumber || !cikInt) return { url: srcUrl, sourceType };

  const ex991 = await resolveEx991Url(cikInt, accessionNumber);
  return ex991 ? { url: ex991, sourceType: "sec_8k_ex991" } : { url: srcUrl, sourceType };
}

/** Parse inline XBRL (iXBRL) numeric tags from an HTML document.
 *  Returns a Map of lowercased concept name to scaled numeric value. */
function parseIxbrlConceptValues(html: string): Map<string, number> {
  const result = new Map<string, number>();
  // Match ix:nonFraction and ix:nonfraction (case-insensitive tag name)
  const re = /<ix:non[Ff]raction\b([^>]*)>([\s\S]*?)<\/ix:non[Ff]raction>/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(html)) !== null) {
    const attrStr = m[1];
    const rawText = m[2].replace(/,/g, "").replace(/\s/g, "").trim();
    if (!rawText) continue;
    const nameMx = /\bname="([^"]+)"/i.exec(attrStr);
    if (!nameMx) continue;
    const conceptKey = nameMx[1].toLowerCase();
    let num = parseFloat(rawText);
    if (!Number.isFinite(num)) continue;
    const scaleMx = /\bscale="(-?\d+)"/i.exec(attrStr);
    if (scaleMx) num *= Math.pow(10, parseInt(scaleMx[1], 10));
    if (/\bsign="-"/i.test(attrStr)) num = -num;
    // Keep first occurrence per concept (consolidated figure appears first)
    if (!result.has(conceptKey)) result.set(conceptKey, num);
  }
  return result;
}

async function resolveLatestEarningsSecSource(ticker: string): Promise<Record<string, unknown> | null> {
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) return null;

  const cikInt = parseInt(cikPadded, 10);
  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form as string[]) ?? [];
  const dates = (recent.filingDate as string[]) ?? [];
  const accessions = (recent.accessionNumber as string[]) ?? [];
  const primaryDocs = (recent.primaryDocument as string[]) ?? [];
  const acceptedDts = (recent.acceptanceDateTime as string[]) ?? [];
  const items = (recent.items as string[]) ?? [];
  const isInlineXBRL = (recent.isInlineXBRL as (number | boolean)[]) ?? [];

  // Two passes: prefer item-2.02 (earnings) 8-Ks; fall back to any 8-K
  for (const requireItem202 of [true, false]) {
    let checked = 0;
    for (let i = 0; i < forms.length && checked < 20; i++) {
      if ((forms[i] ?? "").toUpperCase() !== "8-K") continue;
      checked++;
      const itemStr = String(items[i] ?? "");
      if (requireItem202 && !itemStr.includes("2.02")) continue;
      const acc = accessions[i] ?? "";
      const primaryDoc = primaryDocs[i] ?? "";
      if (!acc || !primaryDoc) continue;
      const accClean = acc.replace(/-/g, "");
      const documentUrl = secArchiveDocumentUrl(cikInt, accClean, primaryDoc);
      return {
        sourceType: "sec_8k",
        url: documentUrl,
        filingDate: dates[i] ?? null,
        acceptedAt: acceptedDts[i] ?? null,
        accessionNumber: acc,
        cikPadded,
        items: itemStr,
        isInlineXBRL: !!(isInlineXBRL[i]),
        confidence: "HIGH",
      };
    }
  }
  return null;
}

async function resolveLatestEarningsRelease(ticker: string): Promise<Record<string, unknown>> {
  const sec = await resolveLatestEarningsSecSource(ticker);
  if (sec) {
    const reportingTs = toIsoUtc((sec.acceptedAt as string | null) ?? null) ?? toIsoUtc((sec.filingDate as string | null) ?? null);
    return {
      ticker: ticker.toUpperCase(),
      eventType: "earnings_release",
      period: "latest",
      periodStatus: "UNRESOLVED",
      periodEvidence: null,
      reportedAt: reportingTs,
      sources: [sec],
      confidence: "HIGH",
      warnings: [],
    };
  }

  const calendar = parseObjectJson(await getCalendar(ticker));
  const earningsDates = ((((calendar.calendar as Record<string, unknown> | undefined)?.earnings as Record<string, unknown> | undefined)?.earningsDate as unknown[]) ?? [])
    .filter((d) => typeof d === "string") as string[];
  // Yahoo's calendar lists the next (estimated) report date: a date still ahead is not the latest release
  // (2.25.1, F27: 7203.T reported 2026-11-05 as its latest release on 2026-10-20).
  const nowMs = Date.now();
  const first = earningsDates
    .filter((d) => { const t = Date.parse(toIsoUtc(d) ?? ""); return Number.isFinite(t) && t <= nowMs; })
    .sort((a, b) => Date.parse(toIsoUtc(b) ?? "") - Date.parse(toIsoUtc(a) ?? ""))[0] ?? null;
  if (first) {
    return {
      ticker: ticker.toUpperCase(),
      eventType: "earnings_release",
      period: "latest",
      periodStatus: "UNRESOLVED",
      periodEvidence: null,
      reportedAt: toIsoUtc(first),
      sources: [
        {
          sourceType: "yahoo_estimate",
          url: `https://finance.yahoo.com/quote/${ticker.toUpperCase()}/analysis`,
          publishedAt: toIsoUtc(first),
          retrievedAt: nowIsoUtc(),
          confidence: "MEDIUM",
        },
      ],
      confidence: "MEDIUM",
      warnings: [{ code: "SEC_8K_NOT_FOUND", message: "SEC 8-K earnings release source not found" }],
    };
  }

  return {
    ticker: ticker.toUpperCase(),
    eventType: "earnings_release",
    period: "latest",
    periodStatus: "UNRESOLVED",
    periodEvidence: null,
    reportedAt: null,
    sources: [],
    confidence: "NOT_FOUND",
    warnings: [],
  };
}

export async function getLatestEarningsRelease(ticker: string, _period = "latest"): Promise<string> {
  const release = await resolveLatestEarningsRelease(ticker);
  const src = Array.isArray(release.sources) && release.sources[0] && typeof release.sources[0] === "object"
    ? release.sources[0] as Record<string, unknown>
    : {};
  const isSec = String(src.sourceType ?? "") === "sec_8k";
  const content = isSec ? await resolveEarningsContentSource(src) : null;
  if (isSec && content) {
    // F59 (3.0.0): the document whose text is read, the EX-99.1 exhibit when the 8-K has one; placed after url.
    const withContent: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(src)) {
      withContent[key] = value;
      if (key === "url") withContent.contentUrl = content.url || null;
    }
    (release.sources as unknown[])[0] = withContent;
  }
  const periodInfo = String(src.sourceType ?? "") === "yahoo_estimate"
    ? { period: null, periodStatus: "UNRESOLVED" as const, periodEvidence: null }
    : await resolveEarningsPeriodFromSource(src, content ?? undefined);
  return JSON.stringify({
    ...release,
    period: periodInfo.period ?? release.period ?? "latest",
    periodStatus: periodInfo.periodStatus,
    periodEvidence: periodInfo.periodEvidence,
  });
}

export async function indexEarningsRelease(ticker: string, period = "latest", sourceUrl: string | null = null): Promise<string> {
  let sourceType: string | null = null;
  let sourceMeta: Record<string, unknown> = {};
  if (sourceUrl) {
    const classified = classifyEarningsSourceUrl(sourceUrl);
    if (classified.error) return JSON.stringify({ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: classified.error } });
    sourceType = classified.sourceType;
    sourceMeta = { sourceType, url: sourceUrl };
  } else {
    const latest = await resolveLatestEarningsRelease(ticker);
    const sources = Array.isArray(latest.sources) ? latest.sources : [];
    const src = (sources[0] && typeof sources[0] === "object") ? sources[0] as Record<string, unknown> : {};
    const content = await resolveEarningsContentSource(src);
    sourceType = content.sourceType;
    sourceUrl = content.url;
    sourceMeta = { ...src, sourceType };
  }

  if (!sourceUrl) {
    return JSON.stringify({
      ticker: ticker.toUpperCase(),
      period,
      source: { sourceType: sourceType ?? "unknown", url: null },
      index: { sections: [], tables: [], keywordMap: {} },
      meta: { indexedAt: nowIsoUtc(), cacheKey: `earnidx:${ticker.toUpperCase()}:none`, cacheTtlHours: 24 },
      warnings: [{ code: "SOURCE_NOT_FOUND", message: "No public earnings release source found" }],
    });
  }

  const cacheId = String(sourceMeta.accessionNumber ?? sourceUrl);
  const cacheKey = `earnidx:${ticker.toUpperCase()}:${cacheId}`;
  const cached = filingIndexCache.get(cacheKey);
  if (cached && Date.now() - cached.storedAt < FILING_INDEX_TTL_MS) {
    countCacheEvent("filing_index", "memoryHits");
    return cached.value;
  }

  const html = sourceUrl.startsWith(SEC_ARCHIVES_PREFIX)
    ? await edgarGetHtml(sourceUrl, 5_000_000)
    : await fetchPublicHtml(sourceUrl);
  if (!html) return JSON.stringify({ ok: false, error: { code: "PROVIDER_ERROR", message: `Failed to fetch source: ${sourceUrl}` } });

  const index = _buildFilingIndexFromHtml(_sanitizeFilingHtml(html));
  const periodInfo = extractEarningsPeriodFromText(_stripHtmlTagsIdx(_sanitizeFilingHtml(html)));
  const out = {
    ticker: ticker.toUpperCase(),
    period: periodInfo.period ?? period,
    periodStatus: periodInfo.periodStatus,
    periodEvidence: periodInfo.periodEvidence,
    source: {
      sourceType: sourceType ?? sourceMeta.sourceType ?? "company_ir",
      url: sourceUrl,
      publishedAt: sourceMeta.publishedAt ?? null,
      retrievedAt: nowIsoUtc(),
      filingDate: sourceMeta.filingDate ?? null,
      acceptedAt: sourceMeta.acceptedAt ?? null,
      accessionNumber: sourceMeta.accessionNumber ?? null,
    },
    index,
    meta: { indexedAt: nowIsoUtc(), cacheKey, cacheTtlHours: 24 },
  };
  const encoded = JSON.stringify(out);
  setBounded(filingIndexCache, cacheKey, { value: encoded, storedAt: Date.now() }, FILING_INDEX_CACHE_MAX);
  return encoded;
}

export interface QuarterFact {
  value: number;
  start: string;
  end: string;
  filed: string;
  form: string;
  /** DIRECT_QUARTER: a reported three-month fact. YTD_DIFFERENCE: two year-to-date facts subtracted. */
  method: "DIRECT_QUARTER" | "YTD_DIFFERENCE";
}

const DAY_MS = 86_400_000;
const isQuarterLength = (days: number): boolean => days >= 80 && days <= 100;

/**
 * The latest fiscal quarter's value among SEC companyconcept facts.
 *
 * The latest period end comes from 10-Q/10-K facts filed on or after
 * minFiled. A three-month fact for that end is used as is. Otherwise, when
 * derive is set, the longest fact for that end (year to date, or the fiscal
 * year in a 10-K) less the fact with the same start that ends one quarter
 * earlier gives the quarter. Never returns a longer period as a quarter.
 */
export function selectQuarterFact(
  facts: Record<string, unknown>[],
  opts: { minFiled?: string; derive: boolean },
): QuarterFact | null {
  type Fact = { value: number; start: string; end: string; filed: string; form: string; days: number };
  const usable: Fact[] = [];
  for (const f of facts) {
    const form = String(f.form ?? "").toUpperCase();
    if (!/^10-[QK]/.test(form) || f.segment) continue;
    if (typeof f.val !== "number" || !Number.isFinite(f.val)) continue;
    const start = String(f.start ?? "");
    const end = String(f.end ?? "");
    const startMs = Date.parse(start);
    const endMs = Date.parse(end);
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) continue;
    usable.push({ value: f.val, start, end, filed: String(f.filed ?? ""), form, days: Math.round((endMs - startMs) / DAY_MS) });
  }
  const recent = usable.filter((f) => !opts.minFiled || f.filed >= opts.minFiled);
  if (!recent.length) return null;
  const targetEnd = recent.reduce((latest, f) => (f.end > latest ? f.end : latest), "");
  const newestFirst = (a: Fact, b: Fact): number => b.filed.localeCompare(a.filed);

  const direct = recent.filter((f) => f.end === targetEnd && isQuarterLength(f.days)).sort(newestFirst)[0];
  if (direct) {
    return { value: direct.value, start: direct.start, end: direct.end, filed: direct.filed, form: direct.form, method: "DIRECT_QUARTER" };
  }
  if (!opts.derive) return null;

  const cumulative = recent
    .filter((f) => f.end === targetEnd && f.days > 100)
    .sort((a, b) => b.days - a.days || newestFirst(a, b))[0];
  if (!cumulative) return null;
  const prior = usable
    .filter((f) => f.start === cumulative.start && f.end < targetEnd && isQuarterLength(cumulative.days - f.days))
    .sort((a, b) => b.end.localeCompare(a.end) || newestFirst(a, b))[0];
  if (!prior) return null;
  const quarterStartMs = Date.parse(prior.end) + DAY_MS;
  return {
    value: cumulative.value - prior.value,
    start: new Date(quarterStartMs).toISOString().slice(0, 10),
    end: cumulative.end,
    filed: cumulative.filed,
    form: cumulative.form,
    method: "YTD_DIFFERENCE",
  };
}

/** The quarter end tier 1 reads: revenue's, else EPS's. */
function xbrlPeriodEndOf(rev: { end: string } | null, eps: { end: string } | null): string | null {
  return rev?.end ?? eps?.end ?? null;
}

/**
 * Whether an XBRL quarter is the one an earnings release reports (2.26.4, F51):
 * it ended on or before the release filing date and less than maxAgeMs before
 * it. With no release date, the age is measured from now.
 */
export function xbrlQuarterInWindow(
  periodEnd: string | null,
  releaseFilingDate: string | null | undefined,
  maxAgeMs: number,
  nowMs: number = Date.now(),
): boolean {
  if (!periodEnd) return false;
  const endMs = new Date(String(periodEnd).slice(0, 10)).getTime();
  if (!Number.isFinite(endMs)) return false;
  const releaseMs = releaseFilingDate ? new Date(String(releaseFilingDate).slice(0, 10)).getTime() : NaN;
  if (Number.isFinite(releaseMs)) {
    const lag = releaseMs - endMs;
    return lag >= 0 && lag < maxAgeMs;
  }
  return nowMs - endMs < maxAgeMs;
}

export async function extractEarningsMetrics(
  ticker: string,
  period = "latest",
  _sourcePreference: string[] = ["sec_8k", "company_ir", "10-q", "yahoo"],
): Promise<string> {
  const release = await resolveLatestEarningsRelease(ticker);
  const defaultMetric = (unit: string): Record<string, unknown> => ({ value: null, unit, confidence: "NOT_DISCLOSED", evidence: null });
  const metrics: Record<string, unknown> = {
    revenue: defaultMetric("USD"),
    epsDiluted: defaultMetric("USD/share"),
    grossMargin: { valueRatio: null, valuePct: null, rawValue: null, confidence: "NOT_DISCLOSED", evidence: null },
    operatingIncome: defaultMetric("USD"),
    freeCashFlow: defaultMetric("USD"),
    capex: defaultMetric("USD"),
  };
  const evidence: Record<string, unknown>[] = [];
  const warnings: Record<string, unknown>[] = [];
  let sourcePeriodInfo: EarningsPeriodInfo = { period: null, periodStatus: "UNRESOLVED", periodEvidence: null };
  const src = Array.isArray(release.sources) && release.sources[0] && typeof release.sources[0] === "object"
    ? release.sources[0] as Record<string, unknown>
    : {};
  const srcUrl = String(src.url ?? "");
  const srcType = String(src.sourceType ?? "yahoo");
  const srcAccession = String(src.accessionNumber ?? "");
  const srcCikPadded = String(src.cikPadded ?? "");
  const releaseFilingDate = String(src.filingDate ?? "");
  const publishedAt = toIsoUtc((src.filingDate as string | null) ?? (src.publishedAt as string | null) ?? null);
  const retrievedAt = nowIsoUtc();
  // Resolve the issuer fiscal period independently of whether a structured
  // metric source is available. A correct metric does not justify a calendar
  // quarter label inferred from the 8-K filing date.
  sourcePeriodInfo = await resolveEarningsPeriodFromSource(src);

  const hasHighConfidence = (): boolean =>
    Object.values(metrics).some((m) => (m as Record<string, unknown>).confidence === "HIGH");

  // ── Tier 1: XBRL CompanyConcept API (confirmed 10-Q data) ────────────────
  // Fetches the most recent 10-Q fact for each concept. Covers periods where
  // the 10-Q is already filed (~40 days after earnings).
  const cikPadded = srcCikPadded || (await resolveCikForTicker(ticker)) || "";
  if (cikPadded) {
    // A 10-Q reports each income-statement concept for the quarter and for the
    // year to date with the same period end, and cash-flow concepts only for
    // the year to date; a 10-K reports the fiscal year. The quarter is picked
    // by period length, or derived as the difference of two year-to-date
    // facts (for example FY minus nine months for a fourth quarter).
    const fetchQuarterlyFact = async (
      primary: string | string[],
      fallback?: string,
      unitType: "USD" | "USD/shares" = "USD",
    ): Promise<QuarterFact & { concept: string } | null> => {
      // Equivalent concepts (revenue) all compete; the latest quarter wins, the
      // earlier-listed concept on a tie. A single concept with a fallback is
      // read in order, as before.
      const concepts = Array.isArray(primary) ? primary : [primary, ...(fallback ? [fallback] : [])];
      let best: (QuarterFact & { concept: string }) | null = null;
      for (const concept of concepts) {
        const d = await edgarGetJson(
          secCompanyConceptUrl(cikPadded, concept),
        );
        const facts =
          (((d?.units as Record<string, unknown>) ?? {})[unitType] as Record<string, unknown>[] | undefined) ?? [];
        // A filing cannot be used for a release that it predates: that was the
        // path that allowed a stale 10-Q to masquerade as the latest release
        // metric. Per-share amounts are not additive, so EPS is never derived.
        const quarter = selectQuarterFact(facts, {
          minFiled: releaseFilingDate || undefined,
          derive: unitType === "USD",
        });
        if (quarter && !Array.isArray(primary)) return { ...quarter, concept };
        if (quarter && (best == null || quarter.end > best.end)) best = { ...quarter, concept };
      }
      return best;
    };

    const [xRev, xEpsRaw, xGpRaw, xOiRaw, xCapexRaw, xOcfRaw] = await Promise.all([
      fetchQuarterlyFact(REVENUE_CONCEPTS),
      fetchQuarterlyFact("EarningsPerShareDiluted", undefined, "USD/shares"),
      fetchQuarterlyFact("GrossProfit"),
      fetchQuarterlyFact("OperatingIncomeLoss"),
      fetchQuarterlyFact("PaymentsToAcquirePropertyPlantAndEquipment", "CapitalExpenditures"),
      fetchQuarterlyFact("NetCashProvidedByUsedInOperatingActivities"),
    ]);
    // Every metric must describe the same quarter as revenue (or EPS).
    const quarterEnd = xRev?.end ?? xEpsRaw?.end ?? null;
    const sameQuarter = <T extends { end: string }>(fact: T | null): T | null =>
      fact && (quarterEnd == null || fact.end === quarterEnd) ? fact : null;
    const xEps = sameQuarter(xEpsRaw);
    const xGp = sameQuarter(xGpRaw);
    const xOi = sameQuarter(xOiRaw);
    const xCapex = sameQuarter(xCapexRaw);
    const xOcf = sameQuarter(xOcfRaw);

    // Tier 1 applies only to the quarter the release reports: a period that
    // ended within 120 days (the 10-Q lag window) before the release was filed
    // (2.26.4, F51). The window is measured from today only when the release
    // date is unknown, so a quarter does not go stale while its release is
    // still the latest.
    const XBRL_MAX_AGE_MS = 120 * 24 * 60 * 60 * 1000;
    const xbrlPeriodEnd = xbrlPeriodEndOf(xRev, xEps);
    const xbrlIsRecent = xbrlQuarterInWindow(xbrlPeriodEnd, releaseFilingDate, XBRL_MAX_AGE_MS);

    if (xbrlIsRecent) {
      const xbrlEv = (fact: QuarterFact & { concept: string }): Record<string, unknown> => ({
        url: secCompanyConceptUrl(cikPadded, fact.concept),
        sourceType: fact.form.startsWith("10-K") ? "sec_xbrl_10k" : "sec_xbrl_10q",
        publishedAt: null,
        retrievedAt,
        excerpt: fact.method === "YTD_DIFFERENCE"
          ? `XBRL ${fact.concept} quarter ${fact.start} to ${fact.end}, derived as the ${fact.form} year-to-date value less the prior year-to-date value`
          : `XBRL ${fact.concept} quarter ${fact.start} to ${fact.end}`,
      });
      const quarterFields = (fact: QuarterFact): Record<string, unknown> => ({
        periodStart: fact.start,
        periodEnd: fact.end,
        derivation: fact.method,
      });
      const setM = (key: string, val: Record<string, unknown>): void => {
        metrics[key] = val;
        if (val.evidence) evidence.push(val.evidence as Record<string, unknown>);
      };
      if (xRev) setM("revenue", { value: xRev.value, unit: "USD", rawValue: null, confidence: "HIGH", ...quarterFields(xRev), evidence: xbrlEv(xRev) });
      if (xEps) setM("epsDiluted", { value: xEps.value, unit: "USD/share", rawValue: null, confidence: "HIGH", ...quarterFields(xEps), evidence: xbrlEv(xEps) });
      if (xGp && xRev && xRev.value !== 0) {
        const pct = Number(((xGp.value / xRev.value) * 100).toFixed(2));
        setM("grossMargin", { valueRatio: Number((xGp.value / xRev.value).toFixed(6)), valuePct: pct, rawValue: null, confidence: "HIGH", ...quarterFields(xGp), evidence: xbrlEv(xGp) });
      }
      if (xOi) setM("operatingIncome", { value: xOi.value, unit: "USD", rawValue: null, confidence: "HIGH", ...quarterFields(xOi), evidence: xbrlEv(xOi) });
      if (xCapex) setM("capex", { value: Math.abs(xCapex.value), unit: "USD", rawValue: null, confidence: "HIGH", ...quarterFields(xCapex), evidence: xbrlEv(xCapex) });
      if (xOcf && xCapex) setM("freeCashFlow", { value: xOcf.value - Math.abs(xCapex.value), unit: "USD", rawValue: null, confidence: "HIGH", ...quarterFields(xOcf), evidence: xbrlEv(xOcf) });
    }
  }

  // ── Tier 2 + 3: SEC 8-K press release (iXBRL → regex fallback) ───────────
  if (!hasHighConfidence() && srcUrl.startsWith(SEC_ARCHIVES_PREFIX)) {
    const cikInt = parseInt(cikPadded || "0", 10);

    // Resolve EX-99.1 exhibit URL — the actual earnings press release document.
    // Modern 8-Ks put financial details in EX-99.1, not in the primary 8-K wrapper.
    let contentUrl = srcUrl;
    if (srcAccession && cikInt) {
      const ex991 = await resolveEx991Url(cikInt, srcAccession);
      if (ex991) contentUrl = ex991;
    }
    const contentSourceType = contentUrl !== srcUrl ? "sec_8k_ex991" : "sec_8k";

    const html = await edgarGetHtml(contentUrl, 5_000_000);
    if (html) {
      const contentEv = (excerpt: string | null): Record<string, unknown> | null =>
        excerpt ? { url: contentUrl, sourceType: contentSourceType, publishedAt, retrievedAt, excerpt } : null;

      // Tier 2: iXBRL — structured data embedded in modern press releases
      const conceptValues = parseIxbrlConceptValues(html);
      if (conceptValues.size > 0) {
        const getConcept = (...names: string[]): number | undefined => {
          for (const n of names) {
            const v = conceptValues.get(n);
            if (v != null) return v;
          }
          return undefined;
        };
        const ixEv = (label: string): Record<string, unknown> => ({
          url: contentUrl,
          sourceType: "sec_8k_ixbrl",
          publishedAt,
          retrievedAt,
          excerpt: `iXBRL: ${label}`,
        });
        const setIx = (key: string, val: Record<string, unknown>): void => {
          // iXBRL tags can contain comparative, annual, and quarterly values.
          // Until a tag is tied to the release's stated period, retain it only
          // as supporting evidence, never as decision-grade output.
          const unscoped: Record<string, unknown> = { ...val, confidence: "LOW", extractionMethod: "EX99_IXBRL_UNSCOPED", periodMatch: false };
          metrics[key] = unscoped;
          evidence.push(unscoped.evidence as Record<string, unknown>);
        };

        const ixRev = getConcept(
          "us-gaap:revenuesfromcontractwithcustomerexcludingassessedtax",
          "us-gaap:revenues",
        );
        const ixEps = getConcept("us-gaap:earningspersharediluted");
        const ixGp = getConcept("us-gaap:grossprofit");
        const ixOi = getConcept("us-gaap:operatingincomeloss");
        const ixCapex = getConcept(
          "us-gaap:paymentstoacquirepropertyplantandequipment",
          "us-gaap:capitalexpenditures",
        );
        const ixOcf = getConcept("us-gaap:netcashprovidedbyusedinoperatingactivities");

        if (ixRev != null) setIx("revenue", { value: ixRev, unit: "USD", rawValue: null, confidence: "HIGH", evidence: ixEv("Revenues") });
        if (ixEps != null) setIx("epsDiluted", { value: ixEps, unit: "USD/share", rawValue: null, confidence: "HIGH", evidence: ixEv("EarningsPerShareDiluted") });
        const gmRev = ixRev ?? (typeof (metrics.revenue as Record<string, unknown>).value === "number"
          ? (metrics.revenue as Record<string, unknown>).value as number : null);
        if (ixGp != null && gmRev != null && gmRev !== 0) {
          const pct = Number(((ixGp / gmRev) * 100).toFixed(2));
          setIx("grossMargin", { valueRatio: Number((ixGp / gmRev).toFixed(6)), valuePct: pct, rawValue: null, confidence: "HIGH", evidence: ixEv("GrossProfit") });
        }
        if (ixOi != null) setIx("operatingIncome", { value: ixOi, unit: "USD", rawValue: null, confidence: "HIGH", evidence: ixEv("OperatingIncomeLoss") });
        if (ixCapex != null) setIx("capex", { value: Math.abs(ixCapex), unit: "USD", rawValue: null, confidence: "HIGH", evidence: ixEv("CapEx") });
        if (ixOcf != null && ixCapex != null) setIx("freeCashFlow", { value: ixOcf - Math.abs(ixCapex), unit: "USD", rawValue: null, confidence: "HIGH", evidence: ixEv("FreeCashFlow") });
      }

      // Tier 3 (regex fallback): plain-text extraction for non-iXBRL press releases
      if (!hasHighConfidence()) {
        const text = _stripHtmlTagsIdx(_sanitizeFilingHtml(html));
        sourcePeriodInfo = extractEarningsPeriodFromText(text);
        const rev = extractReleaseTextMetric(text, "revenue");
        const eps = extractReleaseTextMetric(text, "epsDiluted");
        const gm = extractReleaseTextMetric(text, "grossMargin");
        const op = extractReleaseTextMetric(text, "operatingIncome");
        const fcf = extractReleaseTextMetric(text, "freeCashFlow");
        const capex = extractReleaseTextMetric(text, "capex");
        const setFallbackMetric = (key: string, val: Record<string, unknown>): void => {
          metrics[key] = val;
          const ev = val.evidence;
          if (ev && typeof ev === "object") evidence.push(ev as Record<string, unknown>);
        };
        const textMetric = (value: Record<string, unknown>): Record<string, unknown> => ({
          ...value,
          confidence: "LOW",
          extractionMethod: "EX99_TEXT_CONTEXT",
          periodMatch: Boolean(sourcePeriodInfo.period),
        });

        if (rev.value != null) setFallbackMetric("revenue", textMetric({ value: rev.value, unit: "USD", rawValue: rev.rawValue, scaleBasis: rev.scaleBasis, evidence: contentEv(rev.excerpt) }));
        if (eps.value != null) setFallbackMetric("epsDiluted", textMetric({ value: eps.value, unit: "USD/share", rawValue: eps.rawValue, evidence: contentEv(eps.excerpt) }));
        if (gm.value != null) {
          const pct = Number(gm.value);
          setFallbackMetric("grossMargin", textMetric({ valueRatio: Number((pct / 100).toFixed(6)), valuePct: pct, rawValue: gm.rawValue, evidence: contentEv(gm.excerpt) }));
        }
        if (op.value != null) setFallbackMetric("operatingIncome", textMetric({ value: op.value, unit: "USD", rawValue: op.rawValue, scaleBasis: op.scaleBasis, evidence: contentEv(op.excerpt) }));
        if (fcf.value != null) setFallbackMetric("freeCashFlow", textMetric({ value: fcf.value, unit: "USD", rawValue: fcf.rawValue, scaleBasis: fcf.scaleBasis, evidence: contentEv(fcf.excerpt) }));
        if (capex.value != null) setFallbackMetric("capex", textMetric({ value: capex.value, unit: "USD", rawValue: capex.rawValue, scaleBasis: capex.scaleBasis, evidence: contentEv(capex.excerpt) }));
      }
    }
  } else if (!hasHighConfidence() && !srcUrl.startsWith(SEC_ARCHIVES_PREFIX)) {
    warnings.push({ code: "PUBLIC_RELEASE_NOT_FOUND", message: "No SEC 8-K earnings release source available" });
  }

  let confidence = "NOT_DISCLOSED";
  for (const key of ["revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex"]) {
    const conf = String((metrics[key] as Record<string, unknown>).confidence ?? "");
    if (conf === "HIGH") { confidence = "HIGH"; break; }
  }
  const hasLowConfidence = Object.values(metrics).some((metric) => (metric as Record<string, unknown>).confidence === "LOW");
  if (confidence !== "HIGH" && hasLowConfidence) {
    confidence = "LOW";
    warnings.push({
      code: "TEXT_METRIC_VERIFY_REQUIRED",
      message: "EX-99 text or unscoped iXBRL metrics are not decision-grade without a structured period-matched fact.",
    });
  }
  if (confidence === "NOT_DISCLOSED" && (release.confidence === "MEDIUM" || release.confidence === "LOW")) confidence = String(release.confidence);

  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    eventType: "earnings_release",
    period: sourcePeriodInfo.period ?? release.period ?? period,
    periodStatus: sourcePeriodInfo.periodStatus,
    periodEvidence: sourcePeriodInfo.periodEvidence,
    reportedAt: release.reportedAt ?? null,
    source: srcType || "yahoo",
    metrics,
    evidence,
    confidence,
    warnings,
  });
}

export async function extractGuidance(ticker: string, period = "latest"): Promise<string> {
  const release = await resolveLatestEarningsRelease(ticker);
  const guidance: Record<string, unknown> = {
    revenue: { status: "NOT_DISCLOSED", low: null, high: null, midpoint: null, unit: "USD", evidence: [] },
    grossMargin: { status: "NOT_DISCLOSED", lowPct: null, highPct: null, midpointPct: null, evidence: [] },
    eps: { status: "NOT_DISCLOSED", low: null, high: null, midpoint: null, unit: "USD/share", evidence: [] },
  };
  const src = Array.isArray(release.sources) && release.sources[0] && typeof release.sources[0] === "object"
    ? release.sources[0] as Record<string, unknown>
    : {};
  const content = await resolveEarningsContentSource(src);
  const srcUrl = content.url;
  // A release that was not found or not read is not a company that gave no guidance (2.5.2).
  const notRead = (code: string, message: string, retryable: boolean): string => {
    for (const key of ["revenue", "grossMargin", "eps"]) (guidance[key] as Record<string, unknown>).status = "NOT_READ";
    return JSON.stringify({
      ticker: ticker.toUpperCase(), period: release.period ?? period, status: code, code, retryable,
      sourceUrl: srcUrl || null, guidance, confidence: "NOT_DECISION_GRADE",
      warnings: [{ code, message, severity: "warning" }],
    });
  };
  if (!srcUrl.startsWith(SEC_ARCHIVES_PREFIX)) {
    return notRead("RELEASE_NOT_RESOLVED", "No SEC earnings release was resolved, so guidance was not read.", false);
  }
  const html = await edgarGetHtml(srcUrl, 5_000_000);
  if (!html) return notRead("RELEASE_TEXT_NOT_AVAILABLE", `The earnings release ${srcUrl} could not be read from SEC; retry.`, true);
  const text = _stripHtmlTagsIdx(_sanitizeFilingHtml(html));
  // "revenue guidance of $X to $Y" and "expects revenue between $X and $Y" (extraction-rules.ts).
  const periodLabel = (at: number, len: number) => (at < 0 ? null : (periodForExcerpt(text, at, len).label as string | null) ?? null);
  const ranges = guidanceRanges(text, periodLabel);
  // The period each range targets, from its own sentence or the heading above it (2.5.10).
  const targetPeriod = (excerpt: string) => periodLabel(text.indexOf(excerpt), excerpt.length);
  const rev = ranges.revenue;
  const gm = ranges.grossMargin;
  const eps = ranges.eps;

  const ev = (excerpt: string): Record<string, unknown> => ({
    url: srcUrl,
    sourceType: content.sourceType,
    publishedAt: toIsoUtc((src.filingDate as string | null) ?? null),
    retrievedAt: nowIsoUtc(),
    excerpt: compactExcerpt(excerpt),
  });

  // The same metric on another basis ("GAAP ...; non-GAAP ..."), each with its own excerpt (2.5.9).
  const alternates = (r: NonNullable<typeof rev>, parse: (t: string) => number | null, lowKey: string, highKey: string) =>
    r.alternates.map((a) => ({ basis: a.basis, statedAs: a.statedAs, targetPeriod: targetPeriod(a.excerpt), [lowKey]: parse(a.low), [highKey]: parse(a.high), excerpt: compactExcerpt(a.excerpt) }));
  const plain = (t: string): number | null => (Number.isFinite(Number(t)) ? Number(t) : null);
  // An end without a unit takes the other end's ("$2.0 to $2.4 billion"), as get_guidance_history reads it (2.5.20).
  const amounts = (r: { low: string; high: string }): { low: number | null; high: number | null } => {
    const fallback = guidanceUnitOf(r.high) ?? guidanceUnitOf(r.low);
    return { low: parseAmount(r.low, fallback).value, high: parseAmount(r.high, fallback).value };
  };
  if (rev) {
    const { low, high } = amounts(rev);
    if (low != null && high != null) {
      const revAlternates = rev.alternates.map((a) => ({ basis: a.basis, statedAs: a.statedAs, targetPeriod: targetPeriod(a.excerpt), ...amounts(a), excerpt: compactExcerpt(a.excerpt) }));
      guidance.revenue = { status: "FOUND", basis: rev.basis, statedAs: rev.statedAs, targetPeriod: targetPeriod(rev.excerpt), low, high, midpoint: (low + high) / 2, unit: "USD", evidence: [ev(rev.excerpt)], alternates: revAlternates };
    }
  }
  if (gm) {
    const lowPct = Number(gm.low);
    const highPct = Number(gm.high);
    guidance.grossMargin = { status: "FOUND", basis: gm.basis, statedAs: gm.statedAs, targetPeriod: targetPeriod(gm.excerpt), lowPct, highPct, midpointPct: (lowPct + highPct) / 2, evidence: [ev(gm.excerpt)], alternates: alternates(gm, plain, "lowPct", "highPct") };
  }
  if (eps) {
    const low = Number(eps.low);
    const high = Number(eps.high);
    guidance.eps = { status: "FOUND", basis: eps.basis, statedAs: eps.statedAs, targetPeriod: targetPeriod(eps.excerpt), low, high, midpoint: (low + high) / 2, unit: "USD/share", evidence: [ev(eps.excerpt)], alternates: alternates(eps, plain, "low", "high") };
  }
  const found = ["revenue", "grossMargin", "eps"].some((k) => ((guidance[k] as Record<string, unknown>).status === "FOUND"));
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    period: release.period ?? period,
    status: found ? "FOUND" : "NOT_DISCLOSED",
    sourceUrl: srcUrl,
    guidance,
    confidence: found ? "HIGH" : "NOT_DISCLOSED",
    warnings: [],
  });
}

/** Guidance history (2.5.3): guidance ranges across recent earnings releases, their revisions, and outcomes against reported XBRL actuals. */
export async function getGuidanceHistory(ticker: string, maxReleases = 8): Promise<string> {
  const limit = clampInt(maxReleases, 8, 1, 12);
  const upper = ticker.toUpperCase();
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded) {
    const tickerIndex = await getEdgarTickerCikMap().catch(() => null);
    const code = tickerIndex ? "NO_SEC_REGISTRANT" : "SEC_LOOKUP_UNAVAILABLE";
    const message = tickerIndex ? `SEC has no registrant CIK for ticker ${upper}.` : `SEC's ticker index could not be read, so ${upper} was not resolved to a CIK; retry.`;
    return JSON.stringify({ ticker: upper, status: code, code, retryable: !tickerIndex, message });
  }
  if (!submissions) {
    return JSON.stringify({ ticker: upper, status: "SEC_LOOKUP_UNAVAILABLE", code: "SEC_LOOKUP_UNAVAILABLE", retryable: true, message: "SEC submissions could not be read; retry." });
  }
  const cikInt = parseInt(cikPadded, 10);
  const picks = earningsReleasePicks(submissions, cikInt, limit);
  const releases: ReleaseText[] = [];
  // Four at a time keeps within SEC's request-rate guidance.
  for (let i = 0; i < picks.length; i += 4) {
    releases.push(...await Promise.all(picks.slice(i, i + 4).map(async (p) => ({
      filingDate: p.filingDate, accessionNumber: p.accessionNumber, ...(await readReleaseText(cikInt, p.accessionNumber, p.primaryUrl)),
    }))));
  }
  const companyfacts = await edgarGetJson(secCompanyFactsUrl(cikPadded)).catch(() => null);
  const out = guidanceHistory(ticker, releases, companyfacts);
  const unread = releases.filter((r) => r.status !== "READ").length;
  const warnings: Record<string, unknown>[] = [];
  if (picks.length === 0) warnings.push({ code: "NO_EARNINGS_RELEASES_FOUND", message: "No 8-K reporting results of operations (Item 2.02) is in SEC's recent submissions.", severity: "warning" });
  if (unread > 0) warnings.push({ code: "RELEASE_TEXT_NOT_AVAILABLE", message: `${unread} of ${releases.length} earnings releases could not be read from SEC; their guidance is missing, not withdrawn. Retry.`, severity: "warning" });
  if (!companyfacts) warnings.push({ code: "ACTUALS_NOT_READ", message: "SEC companyfacts could not be read, so outcomes were not evaluated; retry.", severity: "warning" });
  out.status = picks.length === 0 ? "NO_EARNINGS_RELEASES_FOUND" : unread === releases.length ? "RELEASE_TEXT_NOT_AVAILABLE" : unread > 0 || !companyfacts ? "PARTIAL" : "OK";
  out.retryable = unread > 0 || !companyfacts;
  out.warnings = warnings;
  return JSON.stringify(out);
}

export async function extractManagementCommentary(
  ticker: string,
  period = "latest",
  topics: string[] = [],
): Promise<string> {
  const release = await resolveLatestEarningsRelease(ticker);
  const src = Array.isArray(release.sources) && release.sources[0] && typeof release.sources[0] === "object"
    ? release.sources[0] as Record<string, unknown>
    : {};
  const content = await resolveEarningsContentSource(src);
  const srcUrl = content.url;
  const sourceType = content.sourceType;
  let text = "";
  if (srcUrl.startsWith(SEC_ARCHIVES_PREFIX)) {
    text = _stripHtmlTagsIdx(_sanitizeFilingHtml((await edgarGetHtml(srcUrl, 5_000_000)) ?? ""));
  } else if (srcUrl) {
    text = _stripHtmlTagsIdx(_sanitizeFilingHtml((await fetchPublicHtml(srcUrl)) ?? ""));
  }
  const outTopics = (topics ?? []).map((topic) => {
    const match = text ? sentenceForTopic(text, topic) : null;
    if (!match) {
      return { topic, status: "NOT_FOUND", summary: "", evidence: [], confidence: "LOW" };
    }
    const excerpt = match.excerpt;
    return {
      topic,
      status: "FOUND",
      summary: excerpt,
      matchedTerms: match.matchedTerms,
      evidence: [{
        sourceType,
        url: srcUrl,
        publishedAt: toIsoUtc((src.filingDate as string | null) ?? (src.publishedAt as string | null) ?? null),
        retrievedAt: nowIsoUtc(),
        excerpt: excerpt.slice(0, 240),
        matchedTerms: match.matchedTerms,
      }],
      confidence: sourceType.startsWith("sec_8k") ? "HIGH" : "MEDIUM",
    };
  });
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    period: release.period ?? period,
    topics: outTopics,
    warnings: [],
  });
}

export async function compareEarningsActualVsEstimate(ticker: string, period = "latest"): Promise<string> {
  const metrics = parseObjectJson(await extractEarningsMetrics(ticker, period));
  const ea = parseObjectJson(await getEarningsAnalysis(ticker));
  const m = (metrics.metrics && typeof metrics.metrics === "object") ? metrics.metrics as Record<string, unknown> : {};
  const revenueMetric = (m.revenue && typeof m.revenue === "object") ? m.revenue as Record<string, unknown> : {};
  const epsMetric = (m.epsDiluted && typeof m.epsDiluted === "object") ? m.epsDiluted as Record<string, unknown> : {};
  const actualRevenue = typeof revenueMetric.value === "number" ? revenueMetric.value : null;

  const toNumber = (value: unknown): number | null =>
    typeof value === "number" && Number.isFinite(value) ? value : null;
  const rowPeriod = (row: Record<string, unknown> | null): string | null => {
    if (!row) return null;
    for (const key of ["reportedPeriod", "period", "quarter", "index"]) {
      let value: unknown = row[key];
      if (value == null) continue;
      // Unwrap Yahoo Finance {raw, fmt} nested objects (e.g. quarter: {raw: 1617148800, fmt: "3/31/2021"})
      if (typeof value === "object" && value !== null) {
        const obj = value as Record<string, unknown>;
        value = obj.fmt ?? obj.raw ?? obj.label ?? null;
        if (value == null) continue;
      }
      const str = String(value).trim();
      if (str) return str;
    }
    return null;
  };
  const rowDate = (row: Record<string, unknown> | null): string | null => {
    if (!row) return null;
    for (const key of ["reportedDate", "quarter", "date", "earningsDate", "index"]) {
      let value: unknown = row[key];
      if (value == null) continue;
      // Unwrap Yahoo Finance {raw, fmt} nested objects; prefer raw (often a Unix timestamp number)
      if (typeof value === "object" && value !== null) {
        const obj = value as Record<string, unknown>;
        value = obj.raw ?? obj.fmt ?? null;
        if (value == null) continue;
      }
      if (typeof value === "number" && Number.isFinite(value)) {
        const ts = value > 1_000_000_000_000 ? value : value * 1000;
        return new Date(ts).toISOString().slice(0, 10);
      }
      const parsed = new Date(String(value));
      if (!Number.isNaN(parsed.getTime())) return parsed.toISOString().slice(0, 10);
      const text = String(value).trim();
      if (text) return text;
    }
    return null;
  };

  const earningsHistory = Array.isArray(ea.earningsHistory) ? ea.earningsHistory as Record<string, unknown>[] : [];
  const reportedRows = earningsHistory.filter((row) => toNumber(row.epsActual) != null);
  const selected = reportedRows.sort((a, b) => String(rowDate(b) ?? "").localeCompare(String(rowDate(a) ?? "")))[0] ?? null;
  const sourcePeriod = _str(metrics.period) && _str(metrics.period).toLowerCase() !== "latest" ? _str(metrics.period) : null;
  // A Yahoo row named by a date is named by that date alone, YYYY-MM-DD, in both runtimes (3.3.0, Q3i call 5).
  const rowLabel = rowPeriod(selected);
  const estimatePeriod = rowLabel != null && /^\d{4}-\d{2}-\d{2}/.test(rowLabel) ? rowLabel.slice(0, 10) : rowLabel;
  // Yahoo's earningsHistory row is dated by its fiscal quarter end; the
  // release date comes from the earnings release itself.
  const estimateQuarterEnd = rowDate(selected);
  // reportedDate keeps its documented meaning: the date of the Yahoo row.
  const reportedDate = estimateQuarterEnd;
  const releaseDate = typeof metrics.reportedAt === "string" ? metrics.reportedAt.slice(0, 10) : null;
  // A structured quarter matches Yahoo's quarter when their period ends are
  // within a week of each other (52/53-week fiscal calendars end on a weekday).
  const metricPeriodEnd = typeof revenueMetric.periodEnd === "string" ? revenueMetric.periodEnd : null;
  const periodEndsMatch = metricPeriodEnd != null && estimateQuarterEnd != null
    && Math.abs(Date.parse(metricPeriodEnd) - Date.parse(estimateQuarterEnd)) <= 7 * 86_400_000;
  const periodAlignmentStatus = sourcePeriod && estimatePeriod
    ? (sourcePeriod === estimatePeriod || periodEndsMatch ? "MATCHED" : (/^\d{4}-\d{2}-\d{2}/.test(estimatePeriod) ? "UNVERIFIED" : "MISMATCH"))
    : periodEndsMatch ? "MATCHED" : "INCOMPLETE";
  // Surprise actuals follow source policy R3 (3.3.0): the official release figure first, as revenue already does;
  // Yahoo's epsActual only when the release has none, labelled yahoo and LOW.
  const releaseEps = toNumber(epsMetric.value);
  const epsFromRelease = releaseEps != null;
  const actualEps = releaseEps ?? toNumber(selected?.epsActual);
  const estEps = toNumber(selected?.epsEstimate);

  let estRevenue: number | null = null;
  const revenueEstimate = Array.isArray(ea.revenueEstimate) ? ea.revenueEstimate as Record<string, unknown>[] : [];
  for (const row of revenueEstimate) {
    const candidatePeriod = String(row.period ?? row.reportedPeriod ?? "");
    if (
      ((estimatePeriod != null && candidatePeriod === estimatePeriod)
        || (estimateQuarterEnd != null && rowDate(row) === estimateQuarterEnd))
      && typeof row.avg === "number"
    ) {
      estRevenue = row.avg as number;
      break;
    }
  }
  // While the Yahoo row is identified only by its date, its revenue estimate is withheld, as the revenue surprise is (3.3.0, call 5).
  if (periodAlignmentStatus === "UNVERIFIED") estRevenue = null;

  const out: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    period: sourcePeriod ?? estimatePeriod ?? period,
    reportedPeriod: sourcePeriod ?? estimatePeriod,
    reportedDate,
    releaseDate,
    quarterEnd: metricPeriodEnd ?? estimateQuarterEnd,
    releasePublishedAt: metrics.reportedAt ?? null,
    estimatePeriod,
    periodAlignmentStatus,
    actual: {
      revenue: { ...revenueMetric, value: actualRevenue, unit: "USD", period: sourcePeriod ?? metricPeriodEnd, decisionGrade: revenueMetric.decisionGrade === true },
      // The release figure keeps its own source and confidence; a Yahoo fallback is rated by its source, LOW (2.26.4, F22).
      eps: epsFromRelease
        ? { ...epsMetric, value: releaseEps, unit: "USD/share", period: sourcePeriod ?? metricPeriodEnd, decisionGrade: epsMetric.decisionGrade === true }
        : { value: actualEps, unit: "USD/share", source: "yahoo", period: estimatePeriod ?? estimateQuarterEnd, decisionGrade: false, confidence: "LOW" },
    },
    estimate: {
      // Yahoo consensus estimates are LOW, as in get_earnings_analysis; the comparison keeps its own confidence (2.25.0, F16).
      revenue: { value: estRevenue, unit: "USD", source: "yahoo", period: estimatePeriod ?? estimateQuarterEnd, decisionGrade: false, confidence: "LOW" },
      eps: { value: estEps, unit: "USD/share", source: "yahoo", period: estimatePeriod ?? estimateQuarterEnd, decisionGrade: false, confidence: "LOW" },
    },
    surprise: {
      revenueSurprisePct: null,
      epsSurprisePct: null,
    },
    confidence: "NOT_DISCLOSED",
    warnings: [] as Record<string, unknown>[],
  };

  if (!selected) {
    (out.warnings as Record<string, unknown>[]).push({
      code: "NO_REPORTED_QUARTER",
      message: "Yahoo earningsHistory did not include a quarter with non-null actual EPS.",
    });
    return JSON.stringify(out);
  }

  // A release EPS is compared only with the estimate of the same quarter; Yahoo's own actual and estimate share a row.
  const epsComparable = !epsFromRelease || periodAlignmentStatus === "MATCHED";
  if ((sourcePeriod || epsFromRelease) && periodAlignmentStatus !== "MATCHED") {
    const omitted = epsFromRelease ? "cross-source revenue and EPS surprise are omitted" : "cross-source revenue surprise is omitted";
    (out.warnings as Record<string, unknown>[]).push({
      code: periodAlignmentStatus === "MISMATCH" ? "PERIOD_ALIGNMENT_MISMATCH" : "PERIOD_ALIGNMENT_UNVERIFIED",
      message: periodAlignmentStatus === "MISMATCH"
        ? `The official-release period differs from the selected Yahoo estimate period; ${omitted}.`
        : periodAlignmentStatus === "UNVERIFIED"
          ? `Yahoo identifies the selected estimate row by date rather than the official fiscal-period label; ${omitted} unless the periods can be matched.`
          : `The official-release period could not be matched to the selected Yahoo estimate row; ${omitted}.`,
    });
  }

  if (periodAlignmentStatus === "MATCHED" && actualRevenue != null && estRevenue != null && estRevenue !== 0) {
    (out.surprise as Record<string, unknown>).revenueSurprisePct =
      roundHalfAwayFromZero(((actualRevenue - estRevenue) / Math.abs(estRevenue)) * 100, 2);
  } else if (actualRevenue != null && periodAlignmentStatus === "MATCHED") {
    (out.warnings as Record<string, unknown>[]).push({
      code: "REVENUE_ESTIMATE_UNAVAILABLE",
      message: "No Yahoo revenue estimate was available for the selected reported quarter.",
    });
  }

  if (!epsComparable) return JSON.stringify(out);
  // An estimate of exactly 0 takes the near-zero rule below and still gives epsDelta (3.3.0, BR-ERN-031).
  if (actualEps == null || estEps == null) {
    (out.warnings as Record<string, unknown>[]).push({
      code: "EPS_ESTIMATE_UNAVAILABLE",
      message: "No Yahoo EPS estimate was available for the selected reported quarter.",
    });
    return JSON.stringify(out);
  }

  const epsDelta = actualEps - estEps;
  out.surprise = {
    revenueSurprisePct: (out.surprise as Record<string, unknown>).revenueSurprisePct ?? null,
    epsSurprisePct: null,
    epsDelta: roundHalfAwayFromZero(epsDelta, 4),
  };
  if (Math.abs(estEps) < 0.02) {
    (out.warnings as Record<string, unknown>[]).push({
      code: "EPS_NEAR_ZERO_ESTIMATE_BASE",
      message: "EPS percentage surprise is omitted because the absolute estimate is below $0.02 per share; use epsDelta instead.",
    });
    out.confidence = "MEDIUM";
    return JSON.stringify(out);
  }
  (out.surprise as Record<string, unknown>).epsSurprisePct = roundHalfAwayFromZero((epsDelta / Math.abs(estEps)) * 100, 2);
  out.confidence = (out.surprise as Record<string, unknown>).revenueSurprisePct != null ? "HIGH" : "MEDIUM";
  return JSON.stringify(out);
}
