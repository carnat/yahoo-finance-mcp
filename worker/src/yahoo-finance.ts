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

// Phase 5 unit 5: scarce-provider tools and Companies House moved to providers.ts; this re-export keeps tools.ts, index.ts and the
// parity bundle (m.yahooFinance.*) unchanged.
export {
  fetchAlphaVantageJson,
  getHistoricalPutCallRatio,
  getExpandedInstitutionalOwnership,
  getUkCompanyFilings,
} from "./providers.js";

// Phase 5 unit 6: earnings tools moved to earnings.ts; this re-export keeps tools.ts, index.ts and the
// parity bundle (m.yahooFinance.*) unchanged.
export {
  parsePublicTranscript,
  parseYahooTranscriptSourceUrl,
  assessYahooTranscriptPayloadCompleteness,
  nextTranscriptFallback,
  getEarningsCallTranscript,
  earningsReleasePicks,
  readReleaseText,
  commentarySingular,
  sentenceForTopic,
  getLatestEarningsRelease,
  indexEarningsRelease,
  type QuarterFact,
  selectQuarterFact,
  xbrlQuarterInWindow,
  extractEarningsMetrics,
  extractGuidance,
  getGuidanceHistory,
  extractManagementCommentary,
  compareEarningsActualVsEstimate,
} from "./earnings.js";

// Phase 5 unit 7: valuation tools moved to valuation-tools.ts; this re-export keeps tools.ts, index.ts and the
// parity bundle (m.yahooFinance.*) unchanged.
export {
  getHistoricalValuationContext,
  reconcileMetricSources,
  getValuationSnapshot,
} from "./valuation-tools.js";

/**
 * Yahoo Finance API client for Cloudflare Workers.
 * Calls Yahoo Finance HTTP endpoints directly (replacing yfinance + pandas).
 *
 * Endpoints that need a session crumb:   /v10/finance/quoteSummary/, /v7/finance/options/,
 *                                        /ws/fundamentals-timeseries/
 * Endpoints that work without auth:      /v8/finance/chart/, /v1/finance/search
 */


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


// ── Dilution bridge, capital structure, analyst methods, UK filings (2.3.0) ──
