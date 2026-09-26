import { mcpSuccessFromValue, mcpSuccessResult, mcpFailure, mcpFailureResult, type ToolResult, ErrorCode, envelopeV2Enabled, getBuildVersion, getServerVersion, getWorkerVar } from "./response.js";
import { GROUPED_TOOL_DEFS } from "./tool-catalog.js";
import { withCacheScope } from "./request-context.js";
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
  HISTORICAL_PERIODS,
  HISTORICAL_INTERVALS,
  getHolderInfo,
  getExpandedInstitutionalOwnership,
  SUPPORTED_HOLDER_TYPES,
  getMaPosition,
  getOptionChain,
  getOptionExpirationDates,
  getHistoricalPutCallRatio,
  getPriceSlope,
  getPriceStats,
  getPutHedgeCandidates,
  getRecommendations,
  SUPPORTED_RECOMMENDATION_TYPES,
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
import { validateTicker } from "./validate.js";

export interface Tool {
  name: string;
  description: string;
  inputSchema: {
    type: "object";
    properties: Record<string, unknown>;
    required?: string[];
    oneOf?: Array<Record<string, unknown>>;
    additionalProperties?: boolean;
    [key: string]: unknown;
  };
  outputSchema?: {
    type?: "object";
    properties?: Record<string, unknown>;
    required?: string[];
    additionalProperties?: boolean;
    [key: string]: unknown;
  };
  deprecated?: boolean;
  useInstead?: string;
  deprecationReason?: string;
  annotations?: {
    readOnlyHint: boolean;
    destructiveHint: boolean;
    idempotentHint: boolean;
    openWorldHint: boolean;
  };
}

const READ_ONLY_TOOL_ANNOTATIONS = Object.freeze({
  readOnlyHint: true,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: true,
});
const CLOSED_WORLD_TOOL_ANNOTATIONS = Object.freeze({
  ...READ_ONLY_TOOL_ANNOTATIONS,
  openWorldHint: false,
});

function annotationsForTool(name: string): Tool["annotations"] {
  return ["system", "health_check"].includes(name)
    ? CLOSED_WORLD_TOOL_ANNOTATIONS
    : READ_ONLY_TOOL_ANNOTATIONS;
}

export const TOOLS: Tool[] = [
  {
    name: "search_thai_funds",
    description:
      "Search bounded official Thai SEC active fund-profile candidates. Provide project_info (official project name or abbreviation), company_info, or fund_class_name. The result queries Registered and IPO profiles separately, returns compact candidates and per-status nextCursors, and never selects a candidate automatically. Use a returned projId and fundClassName in a later fund-data call.",
    inputSchema: {
      type: "object",
      properties: {
        project_info: { type: "string", description: "Official project name or project abbreviation; supports the documented partial profile search." },
        company_info: { type: "string", description: "AMC name or unique ID for the documented profile search." },
        fund_class_name: { type: "string", description: "Exact Thai SEC share-class code when known." },
        page_size: { type: "integer", minimum: 1, maximum: 20, default: 10, description: "Candidate rows per active status, capped at 20." },
        next_cursors: {
          type: "object",
          properties: {
            Registered: { type: ["string", "null"] },
            IPO: { type: ["string", "null"] },
          },
          description: "The complete nextCursors object from a prior search response. Null marks a completed status page.",
        },
      },
    },
  },
  {
    name: "get_thai_fund_nav",
    description:
      "Return the latest official Thai SEC daily NAV in a bounded Bangkok-time window. Without proj_id, fund_class_name must exactly match an SEC share class. With explicit proj_id, the tool calls NAV directly and returns the SEC source class, which may be main rather than a public distributor code; multiple returned classes are never inferred. Defaults to 45 days ending today in Asia/Bangkok and never searches more than 90 days. NAV_NOT_FOUND_IN_WINDOW is not evidence of no NAV outside the requested window.",
    inputSchema: {
      type: "object",
      properties: {
        fund_class_name: { type: "string", description: "Exact Thai SEC share-class code, e.g. SCBSEMI(E)." },
        proj_id: { type: "string", description: "Optional authoritative SEC project ID. Enables direct NAV lookup; the source class may be main." },
        project_info: { type: "string", description: "Optional official project name or abbreviation for the documented SEC profile search; does not replace exact fund_class_name." },
        as_of_date: { type: "string", description: "Optional Bangkok-date endpoint in YYYY-MM-DD; defaults to today." },
        lookback_days: { type: "integer", minimum: 1, maximum: 90, default: 45, description: "Calendar-day window length, capped at 90." },
      },
      required: ["fund_class_name"],
    },
  },
  {
    name: "get_thai_fund_nav_batch",
    description:
      "Refresh up to 20 official Thai SEC NAVs with explicit project identities. Each funds entry requires fund_class_name and proj_id; reference is an optional caller label. Requests run sequentially, never profile-search or infer a class, and return a status/freshness/recovery object per item. This tool returns NAV evidence only; it does not calculate portfolio value or tax-wrapper eligibility.",
    inputSchema: {
      type: "object",
      properties: {
        funds: {
          type: "array",
          minItems: 1,
          maxItems: 20,
          items: {
            type: "object",
            properties: {
              reference: { type: "string", description: "Optional caller-owned label for this fund." },
              fund_class_name: { type: "string", description: "Public or SEC share-class code retained as the requested identity." },
              proj_id: { type: "string", description: "Authoritative Thai SEC project ID; required for deterministic direct NAV lookup." },
            },
            required: ["fund_class_name", "proj_id"],
          },
        },
        as_of_date: { type: "string", description: "Optional Bangkok-date endpoint in YYYY-MM-DD; defaults to today." },
        lookback_days: { type: "integer", minimum: 1, maximum: 90, default: 45, description: "Calendar-day window length, capped at 90." },
      },
      required: ["funds"],
    },
  },
  {
    name: "get_thai_fund_factsheet",
    description:
      "Return dated official Thai SEC factsheet evidence for an exact share class. Use proj_id to disambiguate or project_info to narrow the documented SEC profile search by official project name/abbreviation. sections defaults to statistics, top_holdings, and urls. Statistics/URLs are share-class scoped; top holdings are project scoped. Read each section's asOfDate and status. This tool returns URLs only and never fetches/parses PDFs.",
    inputSchema: {
      type: "object",
      properties: {
        fund_class_name: { type: "string", description: "Exact Thai SEC share-class code, e.g. SCBSEMI(E)." },
        proj_id: { type: "string", description: "Optional exact SEC project ID. Required when fund_class_name is ambiguous." },
        project_info: { type: "string", description: "Optional official project name or abbreviation for the documented SEC profile search; does not replace exact fund_class_name." },
        sections: { type: "array", items: { type: "string", enum: ["statistics", "top_holdings", "urls"] }, description: "Optional subset; default is all three sections." },
      },
      required: ["fund_class_name"],
    },
  },
  {
    name: "get_thai_fund_dividend_history",
    description:
      "Return one page of official Thai SEC mutual-fund dividend history. Use proj_id to disambiguate or project_info to narrow the documented SEC profile search by official project name/abbreviation. The requested share class is resolved exactly, but the SEC history is project scoped. Each row retains classAbbrName; nextCursor/hasMore mean the page is not a claimed complete history.",
    inputSchema: {
      type: "object",
      properties: {
        fund_class_name: { type: "string", description: "Exact Thai SEC share-class code, e.g. SCBSEMI(E)." },
        proj_id: { type: "string", description: "Optional exact SEC project ID. Required when fund_class_name is ambiguous." },
        project_info: { type: "string", description: "Optional official project name or abbreviation for the documented SEC profile search; does not replace exact fund_class_name." },
        max_results: { type: "integer", minimum: 1, maximum: 100, default: 100, description: "Rows in this returned SEC page." },
        next_cursor: { type: "string", description: "Cursor from a prior response to fetch its next page." },
      },
      required: ["fund_class_name"],
    },
  },
  {
    name: "get_financial_statement",
    description:
      "Get a financial statement for a ticker. Supports annual, quarterly, trailing income/cash-flow, and optional line_items filtering.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
        financial_type: {
          type: "string",
          description: "The type of financial statement to retrieve.",
          enum: [
            "income_stmt",
            "quarterly_income_stmt",
            "balance_sheet",
            "quarterly_balance_sheet",
            "cashflow",
            "quarterly_cashflow",
            "ttm_income_stmt",
            "ttm_cashflow",
          ],
        },
        line_items: { type: "array", items: { type: "string" }, description: "Optional line-item names to return, e.g. Total Revenue or Free Cash Flow." },
      },
      required: ["ticker", "financial_type"],
    },
  },
  {
    name: "get_option_expiration_dates",
    description: "Get the available options expiration dates (YYYY-MM-DD) for a ticker.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_option_chain",
    description:
      "Get the options chain (calls or puts) for a ticker and expiration date. Use get_option_expiration_dates first to find valid dates. Default mode is Robot/LLM-safe: moneyness=near_money (±20%), sort_by=relevance (valid quotes first, then liquidity, then ATM-proximity), include_illiquid=false. Response is wrapped: { ticker, expiration, optionType, dataDate, totalContracts, returnedContracts, truncated, dataQuality, filtersApplied, contracts }. For a raw full chain pass moneyness='all', sort_by='strike', include_illiquid=true.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
        expiration_date: {
          type: "string",
          description: "Options expiration date in YYYY-MM-DD format.",
        },
        option_type: {
          type: "string",
          description: "The type of options to retrieve.",
          enum: ["calls", "puts"],
        },
        min_open_interest: { type: "number", description: "Minimum open interest filter.", default: 0 },
        min_volume: { type: "number", description: "Minimum volume filter.", default: 0 },
        max_contracts: { type: "number", description: "Maximum number of contracts to return.", default: 50 },
        strike_min: { type: "number", description: "Minimum strike filter." },
        strike_max: { type: "number", description: "Maximum strike filter." },
        moneyness: {
          type: "string",
          enum: ["all", "itm", "otm", "near_money"],
          default: "near_money",
          description: "Moneyness filter. Default near_money uses moneyness_window_pct.",
        },
        moneyness_window_pct: {
          type: "number",
          default: 20,
          description: "Half-width of the near_money window as a percentage of the underlying price (default: 20). Only used when moneyness=near_money.",
        },
        sort_by: {
          type: "string",
          enum: ["strike", "volume", "openInterest", "relevance"],
          default: "relevance",
          description: "Sort field. 'relevance' (default) prioritizes valid quotes → liquidity → valid IV → ATM proximity. Use 'strike' for raw ascending order.",
        },
        include_illiquid: {
          type: "boolean",
          default: false,
          description: "When false (default), contracts with zero bid/ask AND zero open interest are excluded. Set true to include all contracts.",
        },
      },
      required: ["ticker", "expiration_date", "option_type"],
    },
  },
  {
    name: "get_analyst_consensus",
    description:
      "Get analyst consensus for one or more tickers. priceTargets.current is the current market price; low/high/mean/median are consensus targets and pctUpsideFromLastPrice uses the mean target. Max 5 tickers per call.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: {
          description: "Stock ticker symbol (e.g. 'AAPL') or an array of up to 5 symbols (e.g. ['AAPL', 'MSFT']). If more than 5 are provided, only the first 5 are processed — split larger lists into multiple calls.",
          oneOf: [
            { type: "string" },
            { type: "array", items: { type: "string" }, maxItems: 5 },
          ],
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_earnings_analysis",
    description:
      "Get analyst forward-looking data: EPS/revenue estimates, EPS trend, up/down EPS revision counts, earnings history, and growth estimates.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "search_ticker",
    description:
      "Search for ticker symbols by company name, partial name, or ISIN. Returns matching quotes with symbol, short name, exchange, and type. Use this to resolve a company name to a ticker before calling other tools. Use exchange='US' to restrict to NMS (NASDAQ) + NYQ (NYSE) — recommended for small/mid-cap US equity searches to avoid foreign listings. Valid exchange values: 'US' (NMS+NYQ), 'NMS', 'NYQ', or null (all).",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Company name, partial name, or ISIN, e.g. 'Apple' or 'US0378331005'",
        },
        max_results: {
          type: "number",
          description: "Maximum number of results to return (default: 8).",
          default: 8,
        },
        exchange: {
          type: "string",
          description: "Optional exchange filter. 'US' for NMS+NYQ, or a specific code like 'NMS' or 'NYQ'. Omit to return all exchanges.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "screen_stocks",
    description:
      "Screen the market for stocks matching predefined criteria. Screener names: aggressive_small_caps, day_gainers, day_losers, growth_technology_stocks, most_actives, most_shorted_stocks, small_cap_gainers, undervalued_growth_stocks, undervalued_large_caps, conservative_foreign_funds, high_yield_bond, portfolio_anchors, solid_large_growth_funds, solid_midcap_growth_funds, top_mutual_funds.",
    inputSchema: {
      type: "object",
      properties: {
        screener_name: {
          type: "string",
          description: "Name of the predefined screener, e.g. 'day_gainers'",
        },
        count: {
          type: "number",
          description: "Number of results to return (default: 25, max: 250).",
          default: 25,
        },
      },
      required: ["screener_name"],
    },
  },
  {
    name: "get_short_interest",
    description:
      "Get short interest data for a ticker symbol. Returns structured short-selling metrics: sharesShort, sharesShortPriorMonth, shortRatio (days-to-cover), shortPercentOfFloat (0–1 scale), sharesPercentSharesOut, floatShares, sharesOutstanding, dateShortInterest, and sharesShortPreviousMonthDate. Short interest data is reported bi-monthly by exchanges and may be up to 2 weeks old.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_technical_indicators",
    description:
      "Get RSI-14 and MACD from completed daily sessions only. An unfinished active bar is excluded; read priceBasis, dataDate, freshnessStatus, and recommendedNextAction. STALE_BAR publishes no indicator values. Max 5 tickers.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: {
          description: "Stock ticker symbol or array of up to 5 symbols. Split larger lists into multiple calls.",
          oneOf: [
            { type: "string" },
            { type: "array", items: { type: "string" }, maxItems: 5 },
          ],
        },
        period: {
          type: "string",
          description:
            "History fed to the indicators (default '1y'). Periods shorter than a year are widened to '1y' so RSI and MACD settle. Valid: 1y, 2y, 5y.",
          default: "1y",
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_price_slope",
    description:
      "Get N completed trading-session close-to-close price change and direction for one or more tickers. days=N uses N+1 completed daily bars and excludes an unfinished current-session bar. Returns startClose/endClose on one consistent priceBasis, same-bar endRawClose, freshness/bar status, retry/fallback diagnostics, slopePct, direction, and dataDate. STALE_BAR returns no slope and recommends RETRY. Max 5 tickers per call.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: {
          description: "Stock ticker symbol or array of up to 5 symbols. Split larger lists into multiple calls.",
          oneOf: [
            { type: "string" },
            { type: "array", items: { type: "string" }, maxItems: 5 },
          ],
        },
        days: {
          type: "integer",
          description: "Completed close-to-close trading-session intervals (default: 5; uses 6 completed bars).",
          default: 5,
          minimum: 1,
          maximum: 500,
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_short_momentum",
    description:
      "Get short interest with MoM delta, direction, squeeze risk, and the SEC/provider observation date in dataDate (not today's market date). Max 5 tickers per call; split larger lists into multiple calls.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: {
          description: "Stock ticker symbol or array of up to 5 symbols. Split larger lists into multiple calls.",
          oneOf: [
            { type: "string" },
            { type: "array", items: { type: "string" }, maxItems: 5 },
          ],
        },
      },
      required: ["ticker"],
    },
  },
];

const CANONICAL_ADDITIONS: Tool[] = [
  { name: "get_market_quote", description: "Get a lightweight Yahoo regular-market price observation for one or more tickers. lastPrice uses priceBasis=REGULAR_MARKET_PRICE and includes priceTimestamp; use get_price_slope for adjusted daily-bar analytics.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "get_historical_prices", description: "Get raw historical OHLCV. Daily rows without a usable close are INCOMPLETE/isFinal=false.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: [...HISTORICAL_PERIODS], default: "1mo" }, interval: { type: "string", enum: [...HISTORICAL_INTERVALS], default: "1d" }, prepost: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "analyze_price_performance", description: "Analyze price performance metrics.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "analyze_moving_average_position", description: "Analyze moving-average position.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "analyze_volume_ratio", description: "Analyze the latest completed-session volume ratio. PARTIAL/INCOMPLETE means retry; do not use a volume signal.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, period: { type: "number", default: 10 } }, required: ["ticker"] } },
  { name: "check_volume_liquidity_threshold", description: "Check completed-session traded value against public liquidity thresholds: passes when the average daily traded value (volume x close) of the 20 sessions before the latest completed one is at least $10M, converted to USD for non-USD listings. ratio20d compares the latest session with that average. PARTIAL/INCOMPLETE means retry; gatePass is unknown.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, foreign_exchange: { type: "boolean", default: false, description: "Accepted for compatibility; non-USD listings are always converted to USD." } }, required: ["ticker"] } },
  { name: "get_company_profile", description: "Get company profile/fundamentals.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, include_all: { type: "boolean" } }, required: ["ticker"] } },
  { name: "get_fund_profile", description: "Get ETF/fund profile with per-section status and as-of-date limitations. Valuation characteristics are conventional multiples; provider inverse yields are retained separately. Request overview, holdings, allocation, operations, or fixed-income sections.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, sections: { type: "array", items: { type: "string", enum: ["overview", "holdings", "allocation", "operations", "fixed_income"] }, uniqueItems: true } }, required: ["ticker"] } },
  { name: "analyze_financial_ratios", description: "Analyze current financial ratios with explicit unitSemantics and optional historical Yahoo valuation measures.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, history_periods: { type: "integer", minimum: 0, maximum: 20, default: 0 }, frequency: { type: "string", enum: ["quarterly", "monthly", "yearly", "trailing"], default: "quarterly" } }, required: ["ticker"] } },
  { name: "analyze_share_count_trend", description: "Use for dilution, issuance, buyback, or historical shares-outstanding questions. Returns contextual Yahoo data and directs material changes to SEC confirmation.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, start_date: { type: "string" }, end_date: { type: "string" } }, required: ["ticker"] } },
  { name: "analyze_credit_health", description: "Analyze credit health metrics.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "get_corporate_actions", description: "Get stock splits, dividends, and fund capital-gain distributions from Yahoo Finance, oldest first. Every split is returned; dividends and capital gains are limited to the most recent `limit` on or after `start_date`.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, start_date: { type: "string", pattern: "^\\d{4}-\\d{2}-\\d{2}$", description: "Earliest dividend/capital-gain date (YYYY-MM-DD)." }, limit: { type: "integer", minimum: 1, maximum: 1000, default: 40, description: "Most recent dividends/capital gains to return." } }, required: ["ticker"] } },
  { name: "get_ownership_holders", description: "Get ownership/holder data. Supported holder_type values: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, holder_type: { type: "string", enum: SUPPORTED_HOLDER_TYPES, description: "One of: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders." } }, required: ["ticker", "holder_type"] } },
  { name: "get_expanded_institutional_ownership", description: "Use only when Yahoo's ordinary top-holder view is insufficient. Tries eligible Finnhub coverage first and never spends scarce Alpha quota unless allow_scarce_fallback=true. Results are contextual; verify material ownership claims against SEC 13F filings.", inputSchema: { type: "object", properties: { ticker: { type: "string", minLength: 1 }, allow_scarce_fallback: { type: "boolean", default: false, description: "Explicitly permit one Alpha Vantage call if Finnhub is unavailable, ineligible, or returns no usable holders." }, max_holders: { type: "integer", minimum: 1, maximum: 100, default: 50 } }, required: ["ticker"] } },
  { name: "get_analyst_recommendations", description: "Get analyst recommendations or upgrade/downgrade history. Supported recommendation_type values: recommendations, upgrades_downgrades.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, recommendation_type: { type: "string", enum: SUPPORTED_RECOMMENDATION_TYPES, description: "One of: recommendations, upgrades_downgrades." }, months_back: { type: "number", default: 12 } }, required: ["ticker", "recommendation_type"] } },
  { name: "get_analyst_rating_changes", description: "Get analyst rating changes radar.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, days_back: { type: "number", default: 30 } }, required: ["ticker"] } },
  { name: "analyze_earnings_momentum", description: "Analyze earnings momentum.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "get_company_events_calendar", description: "Get a ticker's upcoming Yahoo calendar estimates or paginated earnings-date history. Yahoo dates are always marked UNVERIFIED and material dates should be confirmed with official releases.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, mode: { type: "string", enum: ["upcoming", "history"], default: "upcoming" }, limit: { type: "integer", minimum: 1, maximum: 100, default: 12 }, offset: { type: "integer", minimum: 0, default: 0 } }, required: ["ticker"] } },
  { name: "get_market_calendar", description: "Use for market-wide earnings, economic-event, IPO, or stock-split calendar questions. Results are paginated Yahoo provider data, not official confirmation.", inputSchema: { type: "object", properties: { event_type: { type: "string", enum: ["earnings", "economic", "ipo", "splits"], default: "earnings" }, start_date: { type: "string" }, end_date: { type: "string" }, limit: { type: "integer", minimum: 1, maximum: 100, default: 25 }, offset: { type: "integer", minimum: 0, default: 0 } } } },
  { name: "summarize_options_flow", description: "Summarize options flow.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, expiry_hint: { type: "string" } }, required: ["ticker"] } },
  { name: "get_historical_put_call_ratio", description: "Return one explicit historical Alpha Vantage put/call-ratio observation. date is required so scarce quota is never spent on a current snapshot Yahoo already provides; use summarize_options_flow for current options context.", inputSchema: { type: "object", properties: { ticker: { type: "string", minLength: 1 }, date: { type: "string", pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}$", description: "Historical trading date after 2008-01-01 in YYYY-MM-DD." } }, required: ["ticker", "date"] } },
  { name: "analyze_options_flow_window", description: "Analyze options flow in an event window. LOW chain quality returns RETRY and must not support derivative conclusions.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, window_label: { type: "string" } }, required: ["ticker", "window_label"] } },
  { name: "find_put_hedge_candidates", description: "Find OTM put hedge candidates using executable ask cost. Contracts require a two-sided quote plus liquidity evidence; PARTIAL means retry and do not use budgetFeasible as a decision.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, otm_pct_min: { type: "number", default: 8 }, otm_pct_max: { type: "number", default: 12 }, budget_usd: { type: "number", default: 500 }, expiry_after: { type: "string" } }, required: ["ticker"] } },
  { name: "list_sec_company_filings", description: "List SEC filings for a company from EDGAR submissions. Returns cik, filingType, filingDate, acceptedAt, accessionNumber, primaryDocument, documentUrl, and meta.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, form_type: { type: "string", default: "10-K" }, limit: { type: "number", default: 5 }, max_filings: { type: "number", default: 5 } }, required: ["ticker"] } },
  { name: "get_sec_filing_outline", description: "Get SEC filing outline. Uses the indexed filing path for ticker/filing_type calls and returns OUTLINE_NOT_PARSED with tableCount when tables exist but headings are not parsed.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, accession_number: { type: "string" }, document_url: { type: "string" } }, required: ["ticker"] } },
  { name: "get_sec_filing_section", description: "Get structurally resolved SEC filing section text. For Item headings, check status/found; unresolved structure fails closed instead of returning TOC text.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, selector: { type: "object" }, section_name: { type: "string" }, document_url: { type: "string" }, context_chars: { type: "number", default: 3000 } }, required: ["ticker"] } },
  { name: "list_sec_filing_tables", description: "List only usable SEC filing tables while preserving original tableIndex. Read usableTableCount/excludedTableCount before selecting a table.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, document_url: { type: "string" }, offset: { type: "number", default: 0 }, limit: { type: "number", default: 50 } }, required: ["ticker"] } },
  { name: "get_sec_filing_table", description: "Get a selected SEC filing table. Empty/layout-only candidates return UNUSABLE_TABLE with a deterministic recovery action.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, document_url: { type: "string" }, table_index: { type: "number" }, max_rows: { type: "number", default: 30 } }, required: ["ticker", "table_index"] } },
  { name: "extract_sec_filing_fact", description: "Extract SEC filing fact.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, fact: { type: "string" }, fact_name: { type: "string" }, fact_type: { type: "string" }, region: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, period_mode: { type: "string", default: "auto", description: "quarter, ytd, or annual; auto selects quarter for 10-Q and annual for 10-K." }, document_url: { type: "string" }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "search_sec_filing_text", description: "Search the visible text of SEC filings: whole words (plurals included) with curly/straight quotes and dashes folded. search_terms are phrases; search_query also takes \"exact phrase\", A NEAR/n B and -excluded. Matches merge per passage, rank risk factors and MD&A first with every term represented, and page with max_matches/cursor; termStats and hitsBySection summarize all hits. filing_count/since search several filings; include_exhibits adds EX-99 exhibits.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, search_terms: { type: "array", items: { type: "string" } }, search_query: { type: "string" }, exclude_terms: { type: "array", items: { type: "string" } }, near: { type: "array", items: { type: "object", properties: { terms: { type: "array", items: { type: "string" }, minItems: 2, maxItems: 2 }, within_words: { type: "number", minimum: 1, maximum: 200, default: 10 } }, required: ["terms"] } }, match: { type: "string", enum: ["word", "substring"], default: "word" }, order: { type: "string", enum: ["relevance", "document"], default: "relevance" }, max_matches: { type: "number", minimum: 1, maximum: 50, default: 10 }, cursor: { type: "string" }, section_hint: { type: "string" }, selector: { type: "object" }, filing_type: { type: "string", default: "10-K" }, accession_number: { type: "string" }, document_url: { type: "string" }, filing_count: { type: "number", minimum: 1, maximum: 5, default: 1 }, since: { type: "string", pattern: "^\\d{4}-\\d{2}-\\d{2}$" }, include_exhibits: { type: "boolean", default: false }, context_chars: { type: "number", default: 1500 }, return_tables: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "get_sec_filing_index", description: "Build or retrieve the cached section/table index for an SEC filing. Identifies headings, tables, row labels, and units. period is reserved for future multi-period support; currently only 'latest' is supported unless accession_number is provided.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "Reserved. Only 'latest' supported currently." }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "list_sec_material_filings", description: "List latest material SEC filings for a ticker, filtering out noise (Form 4, 144, SC 13G, etc.). Returns only significant filings (10-K, 10-Q, 8-K, S-1, 424B, DEF 14A, 20-F, 6-K by default).", inputSchema: { type: "object", properties: { ticker: { type: "string" }, forms: { type: "array", items: { type: "string" }, default: ["10-K", "10-Q", "8-K", "S-1", "424B", "DEF 14A", "20-F", "6-K"] }, limit: { type: "number", default: 5 } }, required: ["ticker"] } },
  { name: "get_sec_filing_intelligence", description: "Preferred SEC diagnostic call. Returns an accession-matched official companyfacts snapshot, usable section/table index summary, evidence metadata, and recommended follow-ups.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, filing_index: { type: "number", default: 0 } }, required: ["ticker"] } },
  { name: "get_sec_filing_section_markdown", description: "Return a specific SEC filing section as unverified Markdown from a degraded Worker HTML fallback. Payloads are blocked from decision-grade use and include source offsets/warnings.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, section: { type: "string", default: "Item 1A" }, filing_type: { type: "string", default: "10-K" }, filing_index: { type: "number", default: 0 }, max_chars: { type: "number", default: 50000 } }, required: ["ticker"] } },
  { name: "analyze_position_signals", description: "Aggregate public market, analyst, earnings, and technical inputs that may be useful for a caller-defined scoring model. This tool does not access holdings, cost basis, position size, or private scoring rules.", inputSchema: { type: "object", properties: { ticker: { type: "string" } }, required: ["ticker"] } },
  { name: "calculate_price_target_distance", description: "Compare current price to a user-supplied reference price target. Returns both the legacy current/target ratio and directional percent distance to target.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, reference_target_price: { type: "number", description: "Preferred: user-supplied reference target price." }, io_pt: { type: "number", description: "Backward-compatible alias for reference_target_price." } }, required: ["ticker"] } },
  { name: "get_company_news", description: "Get recent public company news and press releases with compact coverage guidance. The default priority is Yahoo Finance, eligible Finnhub, then one quota-bounded Marketaux wire-domain fallback only when higher-priority coverage is empty or limited. Marketaux never paginates automatically and is contextual evidence only. Yahoo primary items are emitted only for an exact ticker token or canonical issuer identity; inspect tickerMatch=EXPLICIT and matchBasis before use. Read coverage.state, failedSources, skippedSources, recommendedNextAction, and per-source diagnostics before treating an empty result as absence. decisionUse=CHECK_OFFICIAL_RELEASES means escalate the item to get_company_press_releases or verify_company_event. Direct newswire RSS is an explicit shallow snapshot, not a default. Accepts a ticker or array of up to 5 symbols.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }], description: "Ticker symbol (e.g. 'AAPL') or an array of up to 5 symbols (e.g. ['AAPL', 'MSFT']). If more than 5 are provided, only the first 5 are processed; split larger lists into multiple calls." }, max_results: { type: "number", default: 10 }, lookback_days: { type: "number", default: 14 }, sources: { type: "array", items: { type: "string" }, default: ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"], description: "Allowed sources: yahoo_finance_news, yahoo_finance_press_releases, finnhub, marketaux, sec, newswire, company_ir, company_ir_page, or legacy yahoo_finance. Marketaux is quota-aware fallback by default; explicit sources=['marketaux'] forces one bounded request." } }, required: ["ticker"] } },
  { name: "search_company_news", description: "Search public company news/events for a ticker and query across selected source metadata and short snippets. Defaults use Yahoo, eligible Finnhub, then a single Marketaux wire-domain fallback only for empty or limited higher-priority coverage. Include company_ir or company_ir_page for official-source evidence; inspect sourceStatus/sourceCoverage before relying on absence.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Ticker symbol, e.g. 'AAPL'" }, query: { type: "string", description: "Required search query string." }, start_date: { type: "string", default: "" }, end_date: { type: "string", default: "" }, sources: { type: "array", items: { type: "string" }, default: ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"] }, max_results: { type: "number", default: 10 } }, required: ["ticker", "query"] } },
  { name: "get_company_press_releases", description: "Get company press releases and official release-style events. Defaults resolve SEC 8-K/EX-99 evidence first, then registry-backed company_ir_page, then Yahoo press-release context. Explicit sources can also include company_ir RSS/Atom, quota-bounded Marketaux, and direct newswire RSS. Gate use is payload-level: decisionGrade:true is allowed only for coverageStatus=SEC_EX99_RESOLVED or APPROVED_IR_PAGE_RESOLVED with evidence fields; candidates/RSS/Marketaux/newswire/Yahoo remain verification/context evidence.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, lookback_days: { type: "number", default: 90 }, max_results: { type: "number", default: 20 }, sources: { type: "array", items: { type: "string" }, default: ["sec", "company_ir_page", "yahoo_finance_press_releases"] } }, required: ["ticker"] } },
  { name: "get_sec_recent_events", description: "Get recent SEC filing events with filing type, filing date, accepted timestamp, accession number, SEC archive URL, and event metadata.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_types: { type: "array", items: { type: "string" }, default: ["8-K", "10-Q", "10-K"] }, lookback_days: { type: "number", default: 90 }, max_results: { type: "number", default: 20 } }, required: ["ticker"] } },
  { name: "get_public_event_timeline", description: "Get a deduplicated chronological timeline across SEC, reviewed/RSS company IR, Yahoo, Finnhub, and quota-aware Marketaux fallback. Direct newswire RSS is explicit because it is a shallow, potentially slow snapshot. Returns sourceStatus/sourceCoverage so source limitations are distinguishable from no events.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, start_date: { type: "string", default: "" }, end_date: { type: "string", default: "" }, sources: { type: "array", items: { type: "string" }, default: ["sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"] }, max_results: { type: "number", default: 50 }, newest_first: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "verify_company_event", description: "Verify whether a public company event is source-backed across SEC, reviewed/RSS company IR, Yahoo, Finnhub, and quota-aware Marketaux fallback. Direct newswire RSS is explicit. Generic publication words do not establish a match by themselves; inspect queryPolicy and each queryMatch. Returns CONFIRMED, PARTIAL, NOT_FOUND, SOURCE_LIMITED_NOT_FOUND, STALE, or CONFLICTING. Marketaux evidence remains contextual and must be escalated to an official release.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, event_query: { type: "string", description: "Specific event terms to verify, such as acquisition, dividend, guidance, contract, or offering." }, start_date: { type: "string", default: "" }, end_date: { type: "string", default: "" }, sources: { type: "array", items: { type: "string" }, default: ["sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"] } }, required: ["ticker", "event_query"] } },
  { name: "extract_geographic_revenue", description: "Extract geographic revenue exposure from official SEC data and indexed filing tables. Returns explicit parser/provider limitation statuses instead of silent non-disclosure when data cannot be parsed.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, region: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, accession_number: { type: "string" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker", "region"] } },
  { name: "extract_segment_revenue", description: "Extract segment revenue rows from official SEC facts and filing tables; may return explicit limitation statuses when no parseable segment data is found.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker"] } },
  { name: "extract_total_revenue", description: "Extract total revenue from official SEC facts or filing tables with evidence metadata.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" } }, required: ["ticker"] } },
  { name: "extract_revenue_exposure", description: "Extract revenue exposure for a region/customer/segment query, returning explicit parser/provider limitation statuses when no decision-grade value is available.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, exposure_query: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker", "exposure_query"] } },
  { name: "extract_china_exposure", description: "Extract China exposure with separate revenue and non-revenue classifications; revenue values are decision-grade only when evidence and status support them.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, accession_number: { type: "string" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker"] } },
  { name: "extract_risk_factor_mentions", description: "Extract concise risk-factor term mentions from SEC filings.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, terms: { type: "array", items: { type: "string" } }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker", "terms"] } },
  { name: "extract_customer_concentration", description: "Extract customer concentration percentages from SEC filings.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker"] } },
  { name: "get_valuation_snapshot", description: "Valuation context for one ticker at the current price or one you supply: diluted shares from the SEC dilution bridge at that price, the filing's period-end cash and debt, equity and enterprise value (in-the-money convertibles leave the debt), EV/Revenue, EV/EBITDA and P/E on trailing results and Yahoo's current and next fiscal-year consensus with analyst counts, and ATM capacity. Falls back to Yahoo figures for non-USD or non-SEC listings. Mechanical context, not a price target; nothing is back-solved.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, price: { type: "number", exclusiveMinimum: 0 }, filing_type: { type: "string", default: "latest" }, include_sec_filings: { type: "boolean", default: true } }, required: ["ticker"] } },
  { name: "compare_peer_valuations", description: "The same multiples for a peer set on one Yahoo basis: price x shares outstanding + total debt - total cash, over trailing results and current and next fiscal-year consensus (EV/Revenue, EV/EBITDA, P/E, revenue growth, gross margin). Returns peer medians, min and max excluding the subject, and the subject's premium or discount to each median. Context, not a signal.", inputSchema: { type: "object", properties: { tickers: { type: "array", items: { type: "string" }, minItems: 1, maxItems: 10 }, subject: { type: "string" } }, required: ["tickers"] } },
  { name: "extract_dilution_bridge", description: "Basic-to-diluted share bridge at a price you supply, from the filing's inline XBRL: cover-page basic shares, options (treasury-stock method, by exercise-price range when tagged), unvested RSUs/PSUs (gross), warrants per class (treasury stock), convertibles (if-converted when in the money) and ATM remaining capacity from filing text. Mechanical and company-disclosed, not a consensus diluted share count; never back-solve it into one. filing_type latest uses the newest 10-Q with the last 10-K as fallback.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, price: { type: "number", exclusiveMinimum: 0 }, as_of_date: { type: "string", pattern: "^\\d{4}-\\d{2}-\\d{2}$" }, currency: { type: "string", default: "USD" }, filing_type: { type: "string", default: "latest" }, accession_number: { type: "string" }, include_atm: { type: "boolean", default: true } }, required: ["ticker", "price"] } },
  { name: "extract_capital_structure", description: "Company-disclosed capital structure at the filing's period end: cash, short-term investments, total debt and net cash; each debt instrument's face amount, carrying amount, coupon, maturity and conversion terms from dimensional inline XBRL (which companyfacts omits); the tagged maturity ladder; and the company's own funding, runway, going-concern and ATM statements quoted from the filing. Disclosed, not forecast.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "latest" }, accession_number: { type: "string" }, include_funding_statements: { type: "boolean", default: true } }, required: ["ticker"] } },
  { name: "extract_analyst_valuation_methods", description: "Valuation methods analysts name in recent news headlines and summaries: multiples with metric and period (e.g. 41x 2H27 EV/EBITDA), DCF with WACC, discount rate and terminal growth, sum-of-the-parts and rNPV, with firm, price target and source link. Lists firms whose targets carry no disclosed method. Context only: not consensus inputs.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, days_back: { type: "number", minimum: 1, maximum: 365, default: 30 } }, required: ["ticker"] } },
  { name: "get_uk_company_filings", description: "UK Companies House filing history for a UK-registered issuer (e.g. IQE.L): accounts, share allotments (SH01), charges (MR01, secured lending) and resolutions, with document links; include_charges adds the charge register. Resolves by company_number, company_name, or the ticker's issuer name. Official statutory filings; RNS market announcements are not held here. Needs the COMPANIES_HOUSE_API_KEY secret.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, company_number: { type: "string" }, company_name: { type: "string" }, category: { type: "string", enum: ["accounts", "capital", "mortgage", "confirmation-statement", "resolution", "incorporation", "officers", "persons-with-significant-control", "address", "annotation", "change-of-name", "miscellaneous"] }, limit: { type: "number", minimum: 1, maximum: 100, default: 25 }, include_charges: { type: "boolean", default: false } } } },
  { name: "extract_exposure", description: "Extract multi-dimensional SEC exposure for a geographic region or named entity/topic. Returns revenue, operational, named-entity, and risk evidence with explicit non-decision-grade statuses when parser/provider limits prevent a value.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Ticker symbol, e.g. 'AAPL'" }, topic: { type: "string", description: "Geographic region or entity to search for, e.g. 'china', 'russia', 'europe', 'huawei'. Case-insensitive." }, filing_type: { type: "string", default: "10-K", description: "SEC filing type: '10-K' or '20-F'." }, period: { type: "string", default: "latest" }, include_risk_factors: { type: "boolean", default: true } }, required: ["ticker", "topic"] } },
  { name: "query_sec_filing_index", description: "Deterministically route supported SEC filing query types to index-backed extractor tools.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, accession_number: { type: "string" }, query_type: { type: "string", enum: ["geographic_revenue_share", "revenue_exposure", "china_exposure", "risk_factor_mentions", "customer_concentration", "total_revenue", "segment_revenue"] }, params: { type: "object", default: {} }, return_evidence: { type: "boolean", default: true }, detailLevel: { type: "string", default: "compact", enum: ["compact", "evidence", "raw"] } }, required: ["ticker", "query_type"] } },
  { name: "get_latest_earnings_release", description: "Resolve the latest public earnings release source for a ticker. Fiscal period is returned only when explicit release text resolves it; otherwise it remains unresolved.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" }, period: { type: "string", enum: ["latest"], default: "latest", description: "Period selector. Only 'latest' is supported." } }, required: ["ticker"] } },
  { name: "index_earnings_release", description: "Build a compact section/table index of the latest public earnings release for deterministic metric extraction.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, period: { type: "string", enum: ["latest"], default: "latest" }, source_url: { type: "string", description: "Optional override URL (must be https://www.sec.gov/Archives/ or company IR). Paywalled sources are blocked." } }, required: ["ticker"] } },
  { name: "extract_earnings_metrics", description: "Extract reported earnings metrics from SEC 8-K or public IR source. EX-99 prose or unscoped iXBRL values remain non-decision-grade until their period is structurally matched.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" }, source_preference: { type: "array", items: { type: "string", enum: ["sec_8k", "company_ir", "10-q", "yahoo"] }, description: "Accepted for compatibility; sources are tried in a fixed order (SEC XBRL quarter facts, then the 8-K release).", default: ["sec_8k", "company_ir", "10-q", "yahoo"] } }, required: ["ticker"] } },
  { name: "extract_guidance", description: "Extract company-provided forward ranges from the resolved official SEC earnings exhibit, including 'between X and Y' wording. Guidance remains non-decision-grade unless separately verified.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" } }, required: ["ticker"] } },
  { name: "extract_management_commentary", description: "Extract topic-keyed management commentary snippets from the latest earnings release. Returns first relevant sentence per topic.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" }, topics: { type: "array", items: { type: "string" }, description: "Topics to search for, e.g. ['AI', 'margins', 'guidance', 'supply chain']" } }, required: ["ticker"] } },
  { name: "compare_earnings_actual_vs_estimate", description: "Compare official-release actuals with Yahoo's historical estimate row. The official fiscal label remains period/reportedPeriod; estimatePeriod and reportedDate identify the Yahoo row; releaseDate is the earnings release date. Read periodAlignmentStatus before using cross-source revenue comparisons. Returns epsDelta and omits percentage surprise for near-zero estimates.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" } }, required: ["ticker"] } },
  { name: "list_sec_filing_exhibits", description: "List all exhibits/documents attached to a specific SEC filing by accession number.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, accessionNumber: { type: "string", description: "SEC filing accession number, e.g. '0000320193-24-000081'" } }, required: ["ticker", "accessionNumber"] } },
  { name: "get_sec_filing_exhibit_content", description: "Fetch and return the text content of a specific exhibit from an SEC filing. Supports topic-based paragraph filtering to reduce token usage.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, accessionNumber: { type: "string", description: "SEC filing accession number" }, fileName: { type: "string", description: "Exhibit filename from the filing index" }, topics: { type: "array", items: { type: "string" }, description: "Optional list of keywords/topics to filter paragraphs by" } }, required: ["ticker", "accessionNumber", "fileName"] } },
  { name: "parse_public_transcript", description: "Fetch and parse a public transcript page (Motley Fool, company IR, etc.). Supports topic-based paragraph filtering to reduce token usage. Provide raw_text to skip URL fetching.", inputSchema: { type: "object", properties: { url: { type: "string", description: "Public https URL of the transcript page" }, topics: { type: "array", items: { type: "string" }, description: "Optional list of keywords/topics to filter paragraphs by" }, raw_text: { type: "string", description: "Raw HTML or text content to parse directly (bypasses URL fetching)" } } } },
  { name: "get_earnings_call_transcript", description: "Retrieve an earnings-call transcript from a content-validated SEC exhibit, then Yahoo/Quartr, then optional Alpha Vantage. Yahoo results are structured and paragraph-paginated for LLM use and include the human-readable sourceUrl. Preview-only Yahoo payloads are reported as INCOMPLETE_TRANSCRIPT and continue to the alternate-source fallback. Inspect contentCompleteness rather than treating cursor exhaustion as proof of a full call. Yahoo contentSha256 identifies the canonical transcript text and speaker projection declared by contentHashBasis, so it remains stable across pages and runtimes. Provide source_url or event_id to target a known Yahoo call. Yahoo and Alpha transcripts are contextual and never decision-grade; filing dates are never treated as fiscal periods.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, period: { type: "string", enum: ["latest"], default: "latest", description: "Event selector. Only the latest release is supported; this is not a fiscal-quarter value." }, fiscal_quarter: { type: "string", pattern: "^[0-9]{4}Q[1-4]$", description: "Optional issuer fiscal quarter, e.g. 2026Q4. Do not derive this from the filing or publication date." }, event_id: { type: "string", pattern: "^[0-9]+$", description: "Optional Yahoo earnings-event ID. Use only to continue a previously returned Yahoo transcript." }, source_url: { type: "string", format: "uri", description: "Optional canonical finance.yahoo.com transcript URL returned by this tool. Other hosts and ticker mismatches are rejected." }, paragraph_limit: { type: "integer", minimum: 1, maximum: 50, default: 20, description: "Maximum structured Yahoo transcript paragraphs to return." }, paragraph_cursor: { type: "string", pattern: "^[0-9]+$", description: "Opaque nextCursor from a prior Yahoo transcript response." }, topics: { type: "array", items: { type: "string" }, description: "Optional list of keywords/topics to filter transcript paragraphs by" } }, required: ["ticker"] } },
  { name: "get_market_snapshot", description: "Compact market-state packet composing quote, price performance, moving-average trend, volume ratios, liquidity gate, and technical indicators in one call. Supports compact (default) and full modes, and optional batch of tickers.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, mode: { type: "string", enum: ["compact", "full"], default: "compact" }, foreign_exchange: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "health_check", description: "Return public-safe MCP availability, release/build identity, schema identity, tool mode, and connector-freshness metadata.", inputSchema: { type: "object", properties: {} } },
];

TOOLS.push(...CANONICAL_ADDITIONS);

const SIMPLE_OBJECT_SCHEMA: Tool["outputSchema"] = {
  type: "object",
  properties: {},
  additionalProperties: true,
};
const MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA: Tool["outputSchema"] = {
  type: "object",
  properties: {
    status: { type: "string" },
    serverVersion: { type: "string" },
    buildVersion: { type: "string" },
    buildSha: { type: ["string", "null"] },
    deployedAt: { type: ["string", "null"] },
    toolCount: { type: "number" },
    manifestVersion: { type: "string" },
    manifestHash: { type: "string" },
    schemaHash: { type: "string" },
    runtimeHash: { type: "string" },
    toolMode: { type: "string" },
    envelopeSchemaVersion: { type: "string" },
    generatedAt: { type: "string" },
    privacyScope: { type: "string" },
  },
  additionalProperties: false,
};
const NEWS_OUTPUT_SCHEMA: Tool["outputSchema"] = {
  type: "object",
  properties: {
    ticker: { type: "string" },
    items: { type: "array" },
    meta: { type: "object" },
  },
  additionalProperties: true,
};
const ENVELOPE_V2_OUTPUT_SCHEMA: Tool["outputSchema"] = {
  type: "object",
  properties: {
    ok: { type: "boolean" },
    data: {},
    meta: {
      type: "object",
      additionalProperties: true,
    },
    error: { type: ["object", "null"] },
    diagnostics: {},
  },
  required: ["ok", "data", "meta", "error"],
  additionalProperties: true,
};

const NUMBER_OR_NULL = { type: ["number", "null"] };
const STRING_OR_NULL = { type: ["string", "null"] };

function contextualOutputSchema(properties: Record<string, unknown>): Tool["outputSchema"] {
  return {
    type: "object",
    properties: {
      source: { const: "yahoo_finance" },
      evidenceClass: { const: "CONTEXTUAL_MARKET_DATA" },
      decisionGrade: { const: false },
      recommendedNextAction: {
        type: "string",
        enum: [
          "NONE",
          "RETRY_OR_REQUEST_AVAILABLE_SECTION",
          "CHECK_OFFICIAL_FUND_SOURCE",
          "CHECK_SEC_FILINGS",
          "CHECK_OFFICIAL_RELEASES",
        ],
      },
      ...properties,
    },
    additionalProperties: true,
  };
}

const FUND_PROFILE_OUTPUT_SCHEMA = contextualOutputSchema({
  ticker: { type: "string" },
  sectionsRequested: { type: "array", items: { type: "string" } },
  sectionStatus: { type: "object", additionalProperties: { type: "string" } },
  sectionDates: { type: "object" },
  description: STRING_OR_NULL,
  fundOverview: { type: ["object", "null"] },
  topHoldings: { type: ["array", "null"] },
  equityHoldings: { type: ["object", "array", "null"] },
  equityHoldingsProviderRaw: { type: ["object", "array", "null"] },
  valuationBasis: STRING_OR_NULL,
  valuationNormalization: { type: "object" },
  assetClasses: { type: ["object", "null"] },
  sectorWeights: { type: ["array", "null"] },
  fundOperations: { type: ["object", "null"] },
  bondHoldings: { type: ["object", "null"] },
  bondRatings: { type: ["object", "null"] },
  warnings: { type: "array", items: { type: "string" } },
});

const EARNINGS_ANALYSIS_OUTPUT_SCHEMA = contextualOutputSchema({
  ticker: { type: "string" },
  earningsEstimate: { type: ["array", "null"] },
  revenueEstimate: { type: ["array", "null"] },
  epsTrend: { type: ["array", "null"] },
  epsRevisions: { type: ["array", "null"] },
  earningsHistory: { type: ["array", "null"] },
  growthEstimates: { type: ["array", "null"] },
});

const FINANCIAL_RATIOS_OUTPUT_SCHEMA = contextualOutputSchema({
  ticker: { type: "string" },
  currency: STRING_OR_NULL,
  trailingPE: NUMBER_OR_NULL,
  forwardPE: NUMBER_OR_NULL,
  pegRatio: NUMBER_OR_NULL,
  priceToSales: NUMBER_OR_NULL,
  priceToBook: NUMBER_OR_NULL,
  enterpriseToEbitda: NUMBER_OR_NULL,
  enterpriseToRevenue: NUMBER_OR_NULL,
  grossMargins: NUMBER_OR_NULL,
  operatingMargins: NUMBER_OR_NULL,
  profitMargins: NUMBER_OR_NULL,
  returnOnEquity: NUMBER_OR_NULL,
  returnOnAssets: NUMBER_OR_NULL,
  debtToEquity: NUMBER_OR_NULL,
  currentRatio: NUMBER_OR_NULL,
  quickRatio: NUMBER_OR_NULL,
  freeCashflow: NUMBER_OR_NULL,
  freeCashflowYield: NUMBER_OR_NULL,
  dividendYield: NUMBER_OR_NULL,
  payoutRatio: NUMBER_OR_NULL,
  earningsGrowth: NUMBER_OR_NULL,
  revenueGrowth: NUMBER_OR_NULL,
  unitSemantics: { type: "object" },
  valuationHistory: { type: ["array", "null"], items: { type: "object" } },
  valuationFrequency: STRING_OR_NULL,
  historyPeriodsRequested: { type: ["integer", "null"] },
  valuationHistoryStatus: STRING_OR_NULL,
});

const SHARE_COUNT_TREND_OUTPUT_SCHEMA = contextualOutputSchema({
  ticker: { type: "string" },
  status: { type: "string" },
  startDate: STRING_OR_NULL,
  endDate: STRING_OR_NULL,
  dataDate: STRING_OR_NULL,
  firstShares: NUMBER_OR_NULL,
  currentShares: NUMBER_OR_NULL,
  changeShares: NUMBER_OR_NULL,
  changePct: NUMBER_OR_NULL,
  sampleCount: { type: ["integer", "null"] },
  returnedSampleCount: { type: ["integer", "null"] },
  truncated: { type: ["boolean", "null"] },
  observations: {
    type: "array",
    items: {
      type: "object",
      properties: { date: { type: "string" }, shares: { type: "number" } },
      additionalProperties: true,
    },
  },
});

const COMPANY_CALENDAR_OUTPUT_SCHEMA = contextualOutputSchema({
  ticker: { type: "string" },
  mode: { type: "string", enum: ["upcoming", "history"] },
  status: STRING_OR_NULL,
  items: { type: ["array", "null"] },
  calendar: { type: ["object", "null"] },
  limit: { type: ["integer", "null"] },
  offset: { type: ["integer", "null"] },
  hasMore: { type: ["boolean", "null"] },
  confirmationStatus: { const: "UNVERIFIED" },
  earningsDateConfirmed: { type: ["boolean", "null"] },
  earningsDateSource: STRING_OR_NULL,
  providerMethod: STRING_OR_NULL,
});

const MARKET_CALENDAR_OUTPUT_SCHEMA = contextualOutputSchema({
  status: { type: "string" },
  eventType: { type: "string", enum: ["earnings", "economic", "ipo", "splits"] },
  startDate: { type: "string" },
  endDate: { type: "string" },
  limit: { type: "integer" },
  offset: { type: "integer" },
  itemCount: { type: "integer" },
  hasMore: { type: "boolean" },
  coverage: { type: "object" },
  items: { type: "array" },
  confirmationStatus: { const: "UNVERIFIED" },
});

const OUTPUT_SCHEMAS: Record<string, Tool["outputSchema"]> = {
  get_historical_stock_prices: SIMPLE_OBJECT_SCHEMA,
  get_stock_info: SIMPLE_OBJECT_SCHEMA,
  get_etf_info: FUND_PROFILE_OUTPUT_SCHEMA,
  get_fund_profile: FUND_PROFILE_OUTPUT_SCHEMA,
  get_yahoo_finance_news: NEWS_OUTPUT_SCHEMA,
  get_stock_actions: SIMPLE_OBJECT_SCHEMA,
  get_financial_statement: SIMPLE_OBJECT_SCHEMA,
  get_holder_info: SIMPLE_OBJECT_SCHEMA,
  get_expanded_institutional_ownership: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      status: { type: "string" },
      source: { type: ["string", "null"] },
      providerGapFilled: { type: "boolean" },
      capacityClass: { type: ["string", "null"] },
      dataDate: { type: ["string", "null"] },
      holders: { type: "array" },
      aggregate: { type: "object" },
      providerAttempts: { type: "array" },
      evidenceClass: { type: "string" },
      decisionGrade: { const: false },
      recommendedNextAction: { type: "string" },
    },
    additionalProperties: true,
  },
  get_option_expiration_dates: SIMPLE_OBJECT_SCHEMA,
  get_historical_put_call_ratio: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      status: { type: "string" },
      source: { type: ["string", "null"] },
      providerGapFilled: { type: "boolean" },
      capacityClass: { type: ["string", "null"] },
      dataDate: { type: ["string", "null"] },
      putCallRatioFullChain: { type: ["number", "null"] },
      byExpiration: { type: "array" },
      providerAttempts: { type: "array" },
      evidenceClass: { type: "string" },
      decisionGrade: { const: false },
      recommendedNextAction: { type: "string" },
    },
    additionalProperties: true,
  },
  get_option_chain: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      expiration: { type: "string" },
      optionType: { type: "string" },
      dataDate: { type: "string" },
      totalContracts: { type: "number" },
      returnedContracts: { type: "number" },
      truncated: { type: "boolean" },
      dataQuality: { type: "object" },
      filtersApplied: { type: "object" },
      contracts: { type: "array" },
    },
    additionalProperties: true,
  },
  get_recommendations: SIMPLE_OBJECT_SCHEMA,
  get_fast_info: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      lastPrice: { type: "number" },
      priceBasis: { type: "string", enum: ["REGULAR_MARKET_PRICE"] },
      observationType: { type: "string", enum: ["REGULAR_MARKET_QUOTE"] },
      priceTimestamp: { type: ["string", "null"] },
      marketState: { type: ["string", "null"] },
      currency: { type: "string" },
      exchange: { type: "string" },
      quoteType: { type: "string" },
      marketCap: { type: ["number", "null"] },
      shares: { type: ["number", "null"] },
      dayHigh: { type: "number" },
      dayLow: { type: "number" },
      yearHigh: { type: "number" },
      yearLow: { type: "number" },
      yearChange: { type: "number" },
      preMarketPrice: { type: ["number", "null"] },
      postMarketPrice: { type: ["number", "null"] },
      marketOpen: { type: "boolean" },
      lastTradeDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_short_interest: SIMPLE_OBJECT_SCHEMA,
  get_price_stats: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      status: { type: "string", enum: ["OK", "PARTIAL"] },
      lastPrice: { type: "number" },
      priceBasis: { type: "string", enum: ["REGULAR_MARKET_PRICE"] },
      priceObservationType: { type: "string", enum: ["REGULAR_MARKET_QUOTE"] },
      priceTimestamp: { type: ["string", "null"] },
      marketState: { type: ["string", "null"] },
      changePct: { type: "number" },
      distFromHigh52wPct: { type: "number" },
      distFromLow52wPct: { type: "number" },
      distFrom50dmaPct: { type: "number" },
      distFrom200dmaPct: { type: "number" },
      volatility30d: { type: "number" },
      cagr1y: { type: "number" },
      cagr3y: { type: "number" },
      cagr5y: { type: "number" },
      historicalPriceBasis: { type: ["string", "null"] },
      historicalObservationType: { type: "string", enum: ["COMPLETED_DAILY_PRICE_SERIES"] },
      historicalBarStatus: { type: "string", enum: ["COMPLETE", "STALE", "INCOMPLETE", "UNAVAILABLE"] },
      freshnessStatus: { type: "string", enum: ["CURRENT", "STALE", "UNKNOWN"] },
      excludedIncompleteBar: { type: "boolean" },
      retryAttempted: { type: "boolean" },
      fallbackUsed: { type: "boolean" },
      recommendedNextAction: { type: "string", enum: ["NONE", "RETRY"] },
      dataDate: { type: ["string", "null"] },
    },
    additionalProperties: true,
  },
  get_analyst_consensus: SIMPLE_OBJECT_SCHEMA,
  get_earnings_analysis: EARNINGS_ANALYSIS_OUTPUT_SCHEMA,
  get_financial_ratios: FINANCIAL_RATIOS_OUTPUT_SCHEMA,
  analyze_financial_ratios: FINANCIAL_RATIOS_OUTPUT_SCHEMA,
  analyze_share_count_trend: SHARE_COUNT_TREND_OUTPUT_SCHEMA,
  get_market_calendar: MARKET_CALENDAR_OUTPUT_SCHEMA,
  get_calendar: COMPANY_CALENDAR_OUTPUT_SCHEMA,
  get_company_events_calendar: COMPANY_CALENDAR_OUTPUT_SCHEMA,
  search_ticker: SIMPLE_OBJECT_SCHEMA,
  screen_stocks: SIMPLE_OBJECT_SCHEMA,
  get_technical_indicators: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      status: { type: "string", enum: ["OK", "STALE_BAR"] },
      rsi14: { type: ["number", "null"] },
      macd: { type: ["number", "null"] },
      macdSignal: { type: ["number", "null"] },
      macdHistogram: { type: ["number", "null"] },
      lastClose: { type: ["number", "null"] },
      priceBasis: { type: ["string", "null"] },
      observationType: { type: "string", enum: ["COMPLETED_DAILY_PRICE_SERIES"] },
      barStatus: { type: "string", enum: ["COMPLETE", "STALE"] },
      freshnessStatus: { type: "string", enum: ["CURRENT", "STALE", "UNKNOWN"] },
      excludedIncompleteBar: { type: "boolean" },
      retryAttempted: { type: "boolean" },
      fallbackUsed: { type: "boolean" },
      recommendedNextAction: { type: "string", enum: ["NONE", "RETRY"] },
      dataDate: { type: ["string", "null"] },
    },
    additionalProperties: true,
  },
  get_price_slope: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      days: { type: "integer" },
      status: { type: "string", enum: ["OK", "STALE_BAR"] },
      startClose: { type: ["number", "null"] },
      endClose: { type: ["number", "null"] },
      endRawClose: { type: ["number", "null"] },
      priceBasis: { type: ["string", "null"], enum: ["ADJUSTED_CLOSE", "UNADJUSTED_CLOSE", null] },
      observationType: { type: "string", enum: ["DAILY_PRICE_BAR"] },
      calculationBasis: { type: "string", enum: ["COMPLETED_SESSION_CLOSE_TO_CLOSE"] },
      observationCount: { type: "integer" },
      barStatus: { type: "string", enum: ["COMPLETE", "STALE"] },
      freshnessStatus: { type: "string", enum: ["CURRENT", "STALE", "UNKNOWN"] },
      expectedCompletedDate: { type: ["string", "null"] },
      latestAvailableBarDate: { type: ["string", "null"] },
      excludedIncompleteBar: { type: "boolean" },
      retryAttempted: { type: "boolean" },
      fallbackUsed: { type: "boolean" },
      slopePct: { type: ["number", "null"] },
      direction: { type: "string" },
      dataDate: { type: ["string", "null"] },
      recommendedNextAction: { type: "string", enum: ["NONE", "RETRY"] },
    },
    additionalProperties: true,
  },
  get_volume_ratio: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      status: { type: "string", enum: ["OK", "STALE_BAR", "PARTIAL"] },
      lastVolume: { type: ["number", "null"] },
      avgVolume10d: { type: ["number", "null"] },
      avgVolume90d: { type: ["number", "null"] },
      ratio10d: { type: ["number", "null"] },
      ratio90d: { type: ["number", "null"] },
      volumeFlag: { type: ["string", "null"] },
      observationType: { type: "string", enum: ["COMPLETED_DAILY_VOLUME"] },
      barStatus: { type: "string", enum: ["COMPLETE", "STALE", "INCOMPLETE"] },
      freshnessStatus: { type: "string", enum: ["CURRENT", "STALE", "UNKNOWN"] },
      excludedIncompleteBar: { type: "boolean" },
      retryAttempted: { type: "boolean" },
      recommendedNextAction: { type: "string", enum: ["NONE", "RETRY"] },
      dataDate: { type: ["string", "null"] },
    },
    additionalProperties: true,
  },
  get_ma_position: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      priceBasis: { type: "string", enum: ["REGULAR_MARKET_PRICE"] },
      observationType: { type: "string", enum: ["REGULAR_MARKET_QUOTE_VS_TRAILING_AVERAGES"] },
      priceTimestamp: { type: ["string", "null"] },
      marketState: { type: ["string", "null"] },
      lastClose: { type: ["number", "null"] },
      sma50: { type: ["number", "null"] },
      sma200: { type: ["number", "null"] },
      distFrom50dmaPct: { type: ["number", "null"] },
      distFrom200dmaPct: { type: ["number", "null"] },
      trend: { type: "string" },
      recommendedNextAction: { type: "string", enum: ["NONE"] },
      dataDate: { type: ["string", "null"] },
    },
    additionalProperties: true,
  },
  get_credit_health: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      ebitdaUsd: { type: ["number", "null"] },
      ebitdaSource: { type: ["string", "null"] },
      operationalEbitdaUsd: { type: ["number", "null"] },
      operationalEbitdaSource: { type: ["string", "null"] },
      depreciationAmortizationUsd: { type: ["number", "null"] },
      interestExpenseUsd: { type: ["number", "null"] },
      interestExpenseSource: { type: ["string", "null"] },
      netDebtToEbitda: { type: ["number", "null"] },
      interestCoverage: { type: ["number", "null"] },
      interestCoverageEbit: { type: ["number", "null"] },
      interestCoverageEbitda: { type: ["number", "null"] },
      debtTier: { type: ["string", "null"] },
      creditStress: { type: ["boolean", "null"] },
      creditStressFlag: { type: ["boolean", "null"] },
      dataDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_short_momentum: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      sharesShort: { type: ["number", "null"] },
      sharesShortPriorMonth: { type: ["number", "null"] },
      shortPctFloat: { type: ["number", "null"] },
      daysToCover: { type: ["number", "null"] },
      momDeltaPct: { type: ["number", "null"] },
      momDirection: { type: ["string", "null"] },
      squeezeRisk: { type: ["string", "null"] },
      flag: { type: ["string", "null"] },
      dateShortInterest: { type: ["string", "null"] },
      dataDate: { type: ["string", "null"] },
      dataDateBasis: { type: "string", enum: ["SHORT_INTEREST_OBSERVATION", "UNAVAILABLE"] },
    },
    additionalProperties: true,
  },
  get_earnings_momentum: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      revision7d: { type: ["number", "null"] },
      revision30d: { type: ["number", "null"] },
      revision90d: { type: ["number", "null"] },
      momentumFlag: { type: ["string", "null"] },
      beatRate: { type: ["number", "null"] },
      avgSurprisePct: { type: ["number", "null"] },
      currentBeatStreak: { type: ["number", "null"] },
      forwardRevisionSignal: { type: ["string", "null"] },
      compositeMomentumSignal: { type: ["string", "null"] },
      compositeMethodNote: { type: ["string", "null"] },
      dataDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_options_flow_summary: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      expiryDate: { type: "string" },
      totalCallOI: { type: "number" },
      totalPutOI: { type: "number" },
      pcRatio: { type: ["number", "null"] },
      pcRatioOI: { type: ["number", "null"] },
      pcSentiment: { type: ["string", "null"] },
      atmStrike: { type: ["number", "null"] },
      atmIV: { type: ["number", "null"] },
      ivPctile: { type: ["number", "null"] },
      ivFlag: { type: ["string", "null"] },
      maxPainStrike: { type: ["number", "null"] },
      highestOICallStrike: { type: ["number", "null"] },
      highestOIPutStrike: { type: ["number", "null"] },
      dataDate: { type: "string" },
      dataQuality: { type: "object" },
      warnings: { type: "array" },
    },
    additionalProperties: true,
  },
  get_put_hedge_candidates: SIMPLE_OBJECT_SCHEMA,
  get_analyst_upgrade_radar: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      netSentiment: { type: ["number", "null"] },
      mixedSignal: { type: ["boolean", "null"] },
      upgrades: { type: ["number", "null"] },
      upgrades30d: { type: ["number", "null"] },
      downgrades: { type: ["number", "null"] },
      downgrades30d: { type: ["number", "null"] },
      initiations: { type: ["number", "null"] },
      initiations30d: { type: ["number", "null"] },
      dataDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_filing_data: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      factType: { type: "string" },
      region: { type: ["string", "null"] },
      period: { type: ["string", "null"] },
      rawValue: { type: ["string", "null"] },
      rawDenominator: { type: ["string", "null"] },
      unit: { type: ["string", "null"] },
      unitScale: { type: ["string", "null"] },
      value: { type: ["number", "null"] },
      denominator: { type: ["number", "null"] },
      valueRatio: { type: ["number", "null"] },
      valuePct: { type: ["number", "null"] },
      extractionMethod: { type: "string" },
      source: { type: "string" },
      confidence: { type: "string" },
      filingType: { type: ["string", "null"] },
      filingDate: { type: ["string", "null"] },
      accessionNumber: { type: ["string", "null"] },
      documentUrl: { type: ["string", "null"] },
      indexUrl: { type: ["string", "null"] },
      primaryDocumentUrl: { type: ["string", "null"] },
      evidence: { type: ["object", "null"] },
      calculation: { type: ["object", "null"] },
      warnings: { type: "array" },
    },
    additionalProperties: true,
  },
  search_filing_text: SIMPLE_OBJECT_SCHEMA,
  get_options_flow_scan: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      windowLabel: { type: "string" },
      pcRatio: { type: ["number", "null"] },
      ivPctile: { type: ["number", "null"] },
      putVolVs10dAvg: { type: ["number", "null"] },
      ivVsRealizedRangePct: { type: ["number", "null"] },
      putVolPer1PctStockAdv: { type: ["number", "null"] },
      fieldNotes: { type: "object" },
      putVolTrend: { type: ["string", "null"] },
      maxPainStrike: { type: ["number", "null"] },
      bracket: { type: ["string", "null"] },
      formattedBlock: { type: "string" },
      realizedVolPriceBasis: { type: ["string", "null"] },
      historicalObservationType: { type: "string", enum: ["COMPLETED_DAILY_PRICE_SERIES"] },
      historicalBarStatus: { type: "string" },
      freshnessStatus: { type: "string" },
      excludedIncompleteBar: { type: "boolean" },
      retryAttempted: { type: "boolean" },
      fallbackUsed: { type: "boolean" },
      recommendedNextAction: { type: "string", enum: ["NONE", "RETRY"] },
      dataDate: { type: ["string", "null"] },
      dataQuality: { type: "object" },
      warnings: { type: "array" },
    },
    additionalProperties: true,
  },
  get_price_target_bracket: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      currentPrice: { type: ["number", "null"] },
      priceBasis: { type: "string", enum: ["REGULAR_MARKET_PRICE"] },
      observationType: { type: "string", enum: ["REGULAR_MARKET_QUOTE_VS_USER_REFERENCE"] },
      priceTimestamp: { type: ["string", "null"] },
      marketState: { type: ["string", "null"] },
      referenceTargetPrice: { type: ["number", "null"] },
      referenceTargetPct: { type: ["number", "null"] },
      currentToTargetRatioPct: { type: ["number", "null"] },
      distanceToTargetPct: { type: ["number", "null"] },
      distanceConvention: { type: "string" },
      ioPt: { type: ["number", "null"] },
      eqfPct: { type: ["number", "null"] },
      bracket: { type: ["string", "null"] },
      inferredTag: { type: ["string", "null"] },
      tag: { type: ["string", "null"] },
      tagNote: { type: ["string", "null"] },
      invertedFlag: { type: ["boolean", "null"] },
      recommendedNextAction: { type: "string", enum: ["NONE"] },
      dataDate: { type: ["string", "null"] },
    },
    additionalProperties: true,
  },
  get_position_score_inputs: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      status: { type: "string", enum: ["OK", "PARTIAL"] },
      t1_inputs: { type: "object" },
      t2_inputs: { type: "object" },
      t4_inputs: { type: "object" },
      t5_inputs: { type: "object" },
      componentStatus: { type: "object" },
      failedComponents: { type: "array" },
      limitedComponents: { type: "array" },
      recommendedNextAction: { type: "string", enum: ["NONE", "RETRY"] },
      dataDate: { type: ["string", "null"] },
    },
    additionalProperties: true,
  },
  get_volume_gate: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      status: { type: "string", enum: ["OK", "STALE_BAR", "PARTIAL"] },
      currency: { type: ["string", "null"] },
      fxRate: { type: ["number", "null"] },
      fxPriceTimestamp: { type: ["string", "null"] },
      lastVolume: { type: ["number", "null"] },
      lastClose: { type: ["number", "null"] },
      priceBasis: { type: "string", enum: ["UNADJUSTED_CLOSE"] },
      adv10d: { type: ["number", "null"] },
      adv20d: { type: ["number", "null"] },
      adv90d: { type: ["number", "null"] },
      ratio20d: { type: ["number", "null"] },
      notionalUsd: { type: ["number", "null"] },
      adv20dTradedValueUsd: { type: ["number", "null"] },
      gateBasis: { type: "string", enum: ["ADV20_TRADED_VALUE_USD"] },
      gateThresholdUsd: { type: "number" },
      fxCurrency: { type: ["string", "null"] },
      gatePass: { type: ["boolean", "null"] },
      observationType: { type: "string", enum: ["COMPLETED_DAILY_VOLUME_NOTIONAL"] },
      barStatus: { type: "string", enum: ["COMPLETE", "STALE", "INCOMPLETE"] },
      freshnessStatus: { type: "string", enum: ["CURRENT", "STALE", "UNKNOWN"] },
      excludedIncompleteBar: { type: "boolean" },
      retryAttempted: { type: "boolean" },
      recommendedNextAction: { type: "string", enum: ["NONE", "RETRY"] },
      dataDate: { type: ["string", "null"] },
      note: { type: ["string", "null"] },
    },
    additionalProperties: true,
  },
  get_market_snapshot: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      status: { type: "string", enum: ["OK", "PARTIAL"] },
      price: { type: "object" },
      range: { type: "object" },
      trend: { type: "object" },
      volume: { type: "object" },
      risk: { type: "object" },
      freshness: { type: "object" },
      componentStatus: { type: "object" },
      partialSuccess: { type: "boolean" },
      failedComponents: { type: "array" },
      limitedComponents: { type: "array" },
      warnings: { type: "array" },
      recommendedNextAction: { type: "string", enum: ["NONE", "RETRY"] },
    },
    additionalProperties: true,
  },
  get_options_summary: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      nearestExpiry: { type: "string" },
      currentPrice: { type: ["number", "null"] },
      atmIV: { type: ["number", "null"] },
      pcRatioVolume: { type: ["number", "null"] },
      pcRatioOI: { type: ["number", "null"] },
      callVolume: { type: "number" },
      putVolume: { type: "number" },
      callOI: { type: "number" },
      putOI: { type: "number" },
      maxPainStrike: { type: ["number", "null"] },
      dataDate: { type: "string" },
      dataQuality: { type: "object" },
      warnings: { type: "array" },
    },
    additionalProperties: true,
  },
  list_sec_filings: SIMPLE_OBJECT_SCHEMA,
  get_filing_outline: SIMPLE_OBJECT_SCHEMA,
  get_filing_section: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      sectionName: { type: "string" },
      status: { type: "string", enum: ["OK", "TEXT_FALLBACK", "SECTION_NOT_FOUND", "SECTION_STRUCTURE_NOT_RESOLVED"] },
      found: { type: "boolean" },
      text: { type: ["string", "null"] },
      matchedHeading: { type: "string" },
      tocSkipped: { type: "boolean" },
      decisionGrade: { type: "boolean" },
      recommendedNextAction: { type: "string", enum: ["GET_FILING_OUTLINE"] },
    },
    additionalProperties: true,
  },
  list_filing_tables: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      status: { type: "string", enum: ["OK", "NO_USABLE_TABLES"] },
      tableCount: { type: "number" },
      usableTableCount: { type: "number" },
      excludedTableCount: { type: "number" },
      returnedCount: { type: "number" },
      tables: { type: "array" },
      recommendedNextAction: { type: "string", enum: ["GET_SEC_FILING_TABLE", "SEARCH_FILING_TEXT"] },
    },
    additionalProperties: true,
  },
  get_filing_table: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      tableIndex: { type: "number" },
      status: { type: "string", enum: ["OK", "UNUSABLE_TABLE"] },
      decisionGrade: { type: "boolean" },
      totalRows: { type: "number" },
      returnedRows: { type: "number" },
      rows: { type: "array" },
      recommendedNextAction: { type: "string", enum: ["LIST_USABLE_TABLES", "VERIFY_TABLE_PERIOD_AND_UNITS"] },
    },
    additionalProperties: true,
  },
  extract_filing_fact: SIMPLE_OBJECT_SCHEMA,
  get_sec_filing_index: SIMPLE_OBJECT_SCHEMA,
  extract_geographic_revenue: SIMPLE_OBJECT_SCHEMA,
  extract_segment_revenue: SIMPLE_OBJECT_SCHEMA,
  extract_total_revenue: SIMPLE_OBJECT_SCHEMA,
  extract_revenue_exposure: SIMPLE_OBJECT_SCHEMA,
  extract_china_exposure: SIMPLE_OBJECT_SCHEMA,
  extract_risk_factor_mentions: SIMPLE_OBJECT_SCHEMA,
  extract_customer_concentration: SIMPLE_OBJECT_SCHEMA,
  extract_dilution_bridge: SIMPLE_OBJECT_SCHEMA,
  get_valuation_snapshot: SIMPLE_OBJECT_SCHEMA,
  compare_peer_valuations: SIMPLE_OBJECT_SCHEMA,
  extract_capital_structure: SIMPLE_OBJECT_SCHEMA,
  extract_analyst_valuation_methods: SIMPLE_OBJECT_SCHEMA,
  get_uk_company_filings: SIMPLE_OBJECT_SCHEMA,
  extract_exposure: SIMPLE_OBJECT_SCHEMA,
  health_check: MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA,
  get_latest_earnings_release: ENVELOPE_V2_OUTPUT_SCHEMA,
  index_earnings_release: ENVELOPE_V2_OUTPUT_SCHEMA,
  extract_earnings_metrics: ENVELOPE_V2_OUTPUT_SCHEMA,
  extract_guidance: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      period: { type: ["string", "null"] },
      guidance: { type: "object" },
      confidence: { type: "string" },
    },
    additionalProperties: true,
  },
  extract_management_commentary: ENVELOPE_V2_OUTPUT_SCHEMA,
  compare_earnings_actual_vs_estimate: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      period: { type: ["string", "null"] },
      reportedPeriod: { type: ["string", "null"] },
      reportedDate: { type: ["string", "null"] },
      releasePublishedAt: { type: ["string", "null"] },
      estimatePeriod: { type: ["string", "null"] },
      periodAlignmentStatus: { type: "string" },
      actual: { type: "object" },
      estimate: { type: "object" },
      surprise: { type: "object" },
      confidence: { type: "string" },
      warnings: { type: "array" },
    },
    additionalProperties: true,
  },
  list_sec_filing_exhibits: ENVELOPE_V2_OUTPUT_SCHEMA,
  get_sec_filing_exhibit_content: ENVELOPE_V2_OUTPUT_SCHEMA,
  parse_public_transcript: {
    type: "object",
    properties: {
      url: { type: ["string", "null"] },
      source: { type: "string", enum: ["raw_text", "public_url"] },
      filteredByTopics: { type: ["array", "null"] },
      matchedParagraphs: { type: "array" },
      text: { type: "string" },
      totalTextLength: { type: "number" },
      truncated: { type: "boolean" },
    },
    additionalProperties: true,
  },
  get_earnings_call_transcript: ENVELOPE_V2_OUTPUT_SCHEMA,
  list_sec_material_filings: ENVELOPE_V2_OUTPUT_SCHEMA,
  get_sec_filing_intelligence: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      filing: { type: "object" },
      xbrl_available: { type: "boolean" },
      xbrl_facts: { type: "object" },
      index: { type: "object" },
      recommended_queries: { type: "array" },
      status: { type: "object" },
    },
    additionalProperties: true,
  },
  get_sec_filing_section_markdown: ENVELOPE_V2_OUTPUT_SCHEMA,
};

OUTPUT_SCHEMAS.analyze_volume_ratio = OUTPUT_SCHEMAS.get_volume_ratio;
OUTPUT_SCHEMAS.check_volume_liquidity_threshold = OUTPUT_SCHEMAS.get_volume_gate;
OUTPUT_SCHEMAS.get_sec_filing_section = OUTPUT_SCHEMAS.get_filing_section;
OUTPUT_SCHEMAS.list_sec_filing_tables = OUTPUT_SCHEMAS.list_filing_tables;
OUTPUT_SCHEMAS.get_sec_filing_table = OUTPUT_SCHEMAS.get_filing_table;
OUTPUT_SCHEMAS.extract_sec_filing_fact = {
  type: "object",
  properties: {
    ticker: { type: "string" },
    fact: { type: "string" },
    value: { type: ["number", "null"] },
    unit: { type: "string" },
    period: { type: ["string", "null"] },
    status: { type: "string" },
    decisionGrade: { type: "boolean" },
    evidence: { type: ["object", "array", "null"] },
    sourceEvidence: { type: ["object", "null"] },
    recommendedNextAction: { type: "string" },
  },
  additionalProperties: true,
};

for (const tool of TOOLS) {
  tool.outputSchema = OUTPUT_SCHEMAS[tool.name] ?? SIMPLE_OBJECT_SCHEMA;
}

const LLM_DETAILED_OUTPUT_TOOLS = new Set([
  "get_fund_profile",
  "analyze_financial_ratios",
  "get_earnings_analysis",
  "get_expanded_institutional_ownership",
  "get_historical_put_call_ratio",
  "analyze_share_count_trend",
  "get_company_events_calendar",
  "get_market_calendar",
  "analyze_volume_ratio",
  "check_volume_liquidity_threshold",
  "get_sec_filing_section",
  "list_sec_filing_tables",
  "get_sec_filing_table",
  "get_sec_filing_intelligence",
  "extract_sec_filing_fact",
  "extract_guidance",
  "parse_public_transcript",
]);

function envelopedToolOutputSchema(dataSchema: Tool["outputSchema"]): Tool["outputSchema"] {
  const envelope = ENVELOPE_V2_OUTPUT_SCHEMA as NonNullable<Tool["outputSchema"]>;
  const contextualData = dataSchema?.type === "object"
    ? { ...dataSchema, type: ["object", "null"] }
    : {};
  return {
    ...envelope,
    properties: {
      ...(envelope.properties ?? {}),
      data: contextualData,
    },
  };
}

function groupedInputSchema(
  groupName: string,
  actions: Record<string, string>,
): Tool["inputSchema"] {
  const actionNames = Object.keys(actions);
  const oneOf = actionNames.map((action) => {
    const definition = TOOLS.find((tool) => tool.name === action);
    if (!definition) {
      throw new Error(`No canonical input schema for grouped action '${groupName}.${action}'`);
    }
    const requiredParams = definition.inputSchema.required ?? [];
    return {
      title: action,
      type: "object",
      properties: {
        action: {
          type: "string",
          const: action,
          enum: [action],
        },
        params: {
          ...definition.inputSchema,
          additionalProperties: false,
        },
      },
      required: requiredParams.length > 0 ? ["action", "params"] : ["action"],
      additionalProperties: false,
    };
  });
  return {
    type: "object",
    properties: {
      action: { type: "string", enum: actionNames },
      params: {
        type: "object",
        description:
          "Arguments for the selected action. The matching oneOf branch defines allowed and required fields.",
      },
    },
    required: ["action"],
    oneOf,
    additionalProperties: false,
  };
}

const GROUPED_TOOLS: Tool[] = GROUPED_TOOL_DEFS.map((group) => ({
  name: group.name,
  description: group.description,
  inputSchema: groupedInputSchema(group.name, group.actions),
  outputSchema: ENVELOPE_V2_OUTPUT_SCHEMA,
  annotations: annotationsForTool(group.name),
}));

const GROUPED_ACTIONS = new Map(
  GROUPED_TOOL_DEFS.map((group) => [group.name, new Set(Object.keys(group.actions))])
);

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

type DoctrineToolStatus = {
  capabilityStatus: "ACTIVE" | "DEGRADED" | "PROVIDER_GATED" | "EXPERIMENTAL" | "RETIRED";
  decisionGrade: boolean;
  doctrineUse: "ALLOWED" | "VERIFY_ONLY" | "DIAGNOSTICS_ONLY" | "BLOCKED";
  failureMode: string | null;
  evidenceRequired: boolean;
  sourceType: "sec_xbrl" | "sec_table" | "sec_filing" | "company_ir" | "company_ir_page" | "yahoo" | "exchange" | "provider_diagnostic" | "unknown";
  successCriteria?: string[];
  limitationStatuses?: string[];
  note?: string;
};

const TOOL_DOCTRINE_STATUS: Record<string, DoctrineToolStatus> = {
  get_sec_filing_section_markdown: {
    capabilityStatus: "DEGRADED",
    decisionGrade: false,
    doctrineUse: "BLOCKED",
    failureMode: "LIVE_SECTION_EXTRACTION_UNRELIABLE",
    evidenceRequired: true,
    sourceType: "sec_filing",
  },
  get_company_press_releases: {
    capabilityStatus: "ACTIVE",
    decisionGrade: false,
    doctrineUse: "ALLOWED",
    failureMode: null,
    evidenceRequired: true,
    sourceType: "company_ir_page",
    successCriteria: ["coverageStatus=SEC_EX99_RESOLVED or APPROVED_IR_PAGE_RESOLVED", "decisionGrade=true", "secEvidence or irPageEvidence present"],
    limitationStatuses: ["SEC_8K_FOUND_EX99_NOT_FOUND", "NO_OFFICIAL_RELEASE_SOURCE", "NO_YAHOO_PRESS_RELEASE", "COMPANY_IR_PAGE_NOT_APPROVED", "COMPANY_IR_NOT_FOUND", "NOT_FOUND"],
    note: "Tool is usable, but gate-clear is payload-level only; candidate, unresolved, RSS-only, newswire, and Yahoo payloads remain non-decision-grade.",
  },
  extract_sec_filing_fact: {
    capabilityStatus: "ACTIVE",
    decisionGrade: true,
    doctrineUse: "ALLOWED",
    failureMode: null,
    evidenceRequired: true,
    sourceType: "sec_xbrl",
    successCriteria: ["value", "xbrlContext", "sourceEvidence"],
    limitationStatuses: ["SEC_FACT_NOT_AVAILABLE", "NO_COMPANYCONCEPT_FACT_FOR_FORM", "UNSUPPORTED_XBRL_CONCEPT"],
    note: "Decision-grade only for successful XBRL facts with value, xbrlContext, and sourceEvidence; limitation statuses remain non-decision-grade at payload level.",
  },
};

function doctrineStatusFor(tool: string): DoctrineToolStatus | undefined {
  return TOOL_DOCTRINE_STATUS[tool];
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
  return (await withCacheScope(() => callToolResultInScope(name, args))).result;
}

async function callToolResultInScope(name: string, args: Record<string, unknown>): Promise<ToolResult> {
  let raw: string;
  try {
    raw = await _dispatchTool(name, args);
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
        ...(doctrineStatusFor(name) ? { metaExtra: doctrineStatusFor(name) } : {}),
      });
    }
  }
  return mcpSuccessResult(name, raw, {
    ...(doctrineStatusFor(name) ? { metaExtra: doctrineStatusFor(name) } : {}),
  });
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

async function _dispatchTool(name: string, args: Record<string, unknown>): Promise<string> {
  switch (name) {
    case "search_thai_funds":
      return searchThaiFunds(args.project_info, args.company_info, args.fund_class_name, args.page_size, args.next_cursors);
    case "get_thai_fund_nav":
      return getThaiFundNav(args.fund_class_name, args.proj_id, args.as_of_date, args.lookback_days, args.project_info);
    case "get_thai_fund_nav_batch":
      return getThaiFundNavBatch(args.funds, args.as_of_date, args.lookback_days);
    case "get_thai_fund_factsheet":
      return getThaiFundFactsheet(args.fund_class_name, args.proj_id, args.sections, args.project_info);
    case "get_thai_fund_dividend_history":
      return getThaiFundDividendHistory(args.fund_class_name, args.proj_id, args.max_results, args.next_cursor, args.project_info);
    case "get_historical_prices": {
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
    }
    case "get_company_profile":
      return getStockInfo(tickerArg(args.ticker), args.include_all === true);
    case "get_fund_profile":
      return getEtfInfo(tickerArg(args.ticker), Array.isArray(args.sections) ? args.sections.map((item) => str(item)) : undefined);
    case "get_company_news":
      return getCompanyNews(
        tickerArg(args.ticker),
        num(args.max_results, 10),
        num(args.lookback_days, 14),
        Array.isArray(args.sources)
          ? args.sources.map(String)
          : ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"],
      );
    case "get_corporate_actions":
      return getStockActions(str(args.ticker), args.start_date != null ? str(args.start_date) : "", num(args.limit, 40));
    case "get_financial_statement":
      return getFinancialStatement(str(args.ticker), str(args.financial_type), Array.isArray(args.line_items) ? args.line_items.map((item) => str(item)) : undefined);
    case "get_ownership_holders":
      return getHolderInfo(str(args.ticker), str(args.holder_type));
    case "get_expanded_institutional_ownership":
      return getExpandedInstitutionalOwnership(
        str(args.ticker),
        args.allow_scarce_fallback === true,
        num(args.max_holders, 50),
      );
    case "get_option_expiration_dates":
      return getOptionExpirationDates(str(args.ticker));
    case "get_historical_put_call_ratio":
      return getHistoricalPutCallRatio(str(args.ticker), str(args.date));
    case "get_option_chain":
      return getOptionChain(str(args.ticker), str(args.expiration_date), str(args.option_type),
        num(args.max_contracts, 50), num(args.min_open_interest, 0), num(args.min_volume, 0),
        args.strike_min != null ? num(args.strike_min, 0) : null,
        args.strike_max != null ? num(args.strike_max, 0) : null,
        str(args.moneyness, "near_money"),
        str(args.sort_by, "relevance"),
        num(args.moneyness_window_pct, 20),
        args.include_illiquid === true);
    case "get_analyst_recommendations":
      return getRecommendations(
        str(args.ticker),
        str(args.recommendation_type),
        num(args.months_back, 12)
      );
    case "get_market_quote":
      return getFastInfo(tickerArg(args.ticker));
    case "analyze_price_performance":
      return getPriceStats(tickerArg(args.ticker));
    case "get_analyst_consensus":
      return getAnalystConsensus(tickerArg(args.ticker));
    case "get_earnings_analysis":
      return getEarningsAnalysis(str(args.ticker));
    case "analyze_financial_ratios":
      return getFinancialRatios(tickerArg(args.ticker), num(args.history_periods, 0), str(args.frequency, "quarterly"));
    case "analyze_share_count_trend":
      return analyzeShareCountTrend(str(args.ticker), args.start_date != null ? str(args.start_date) : undefined, args.end_date != null ? str(args.end_date) : undefined);
    case "get_company_events_calendar":
      return getCalendar(str(args.ticker), str(args.mode, "upcoming"), num(args.limit, 12), num(args.offset, 0));
    case "get_market_calendar":
      return getMarketCalendar(str(args.event_type, "earnings"), args.start_date != null ? str(args.start_date) : undefined, args.end_date != null ? str(args.end_date) : undefined, num(args.limit, 25), num(args.offset, 0));
    case "search_ticker":
      return searchTicker(str(args.query), num(args.max_results, 8), args.exchange != null ? str(args.exchange) : null);
    case "screen_stocks":
      return screenStocks(str(args.screener_name), num(args.count, 25));
    case "get_short_interest":
      return getShortInterest(str(args.ticker));
    case "get_technical_indicators":
      return getTechnicalIndicators(tickerArg(args.ticker), str(args.period, "1y"));
    case "get_price_slope":
      return getPriceSlope(tickerArg(args.ticker), num(args.days, 5));
    case "analyze_volume_ratio":
      return getVolumeRatio(tickerArg(args.ticker), num(args.period, 10));
    case "analyze_moving_average_position":
      return getMaPosition(tickerArg(args.ticker));
    case "analyze_credit_health":
      return getCreditHealth(tickerArg(args.ticker));
    case "get_short_momentum":
      return getShortMomentum(tickerArg(args.ticker));
    case "analyze_earnings_momentum":
      return getEarningsMomentum(tickerArg(args.ticker));
    case "summarize_options_flow":
      return getOptionsSummary(str(args.ticker), args.expiry_hint != null ? str(args.expiry_hint) : undefined);
    case "find_put_hedge_candidates":
      return getPutHedgeCandidates(
        str(args.ticker),
        num(args.otm_pct_min, 8),
        num(args.otm_pct_max, 12),
        num(args.budget_usd, 500),
        str(args.expiry_after)
      );
    case "get_analyst_rating_changes":
      return getAnalystUpgradeRadar(tickerArg(args.ticker), num(args.days_back, 30));

    case "extract_sec_filing_fact": {
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
    }
    case "list_sec_company_filings":
      return listSecCompanyFilings(str(args.ticker), str(args.filing_type ?? args.form_type, "10-K"), num(args.limit ?? args.max_filings, 5));
    case "get_sec_filing_outline": {
      const ticker = str(args.ticker);
      const filingType = str(args.filing_type ?? args.form_type, "10-K");
      if (args.document_url == null) {
        const idx = JSON.parse(await getSecFilingIndex(ticker, filingType, str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null)) as Record<string, unknown>;
        return secIndexOutlinePayload(idx);
      }
      return getFilingOutline(ticker, args.accession_number != null ? str(args.accession_number) : null, str(args.document_url));
    }
    case "get_sec_filing_section": {
      const ticker = str(args.ticker);
      const filingType = str(args.filing_type ?? args.form_type, "10-K");
      const sectionName = args.section_name != null
        ? str(args.section_name)
        : str((args.selector as Record<string, unknown> | undefined)?.item, "Item 1A");
      const docUrl = args.document_url != null
        ? str(args.document_url)
        : await resolveSecDocumentUrl(ticker, filingType, 1);
      return getFilingSection(ticker, sectionName, str(docUrl), num(args.context_chars, 3000));
    }
    case "list_sec_filing_tables": {
      const ticker = str(args.ticker);
      const filingType = str(args.filing_type ?? args.form_type, "10-K");
      const offset = num(args.offset, 0);
      const limit = num(args.limit, 50);
      if (args.document_url != null) {
        return listFilingTables(ticker, str(args.document_url), offset, limit);
      }
      const idx = JSON.parse(await getSecFilingIndex(ticker, filingType, str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null)) as Record<string, unknown>;
      return secIndexTablesPayload(idx, offset, limit);
    }
    case "get_sec_filing_table": {
      const ticker = str(args.ticker);
      const filingType = str(args.filing_type ?? args.form_type, "10-K");
      const docUrl = args.document_url != null
        ? str(args.document_url)
        : (JSON.parse(await getSecFilingIndex(ticker, filingType, str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null)) as Record<string, unknown>).documentUrl;
      if (!docUrl) return JSON.stringify({ ok: false, error: { code: "FILING_NOT_FOUND_TRY_OTHER_TYPE", message: `No ${filingType} filing document found for '${ticker}'` } });
      return getFilingTable(ticker, str(docUrl), num(args.table_index, 0), num(args.max_rows, 30));
    }
    case "search_sec_filing_text":
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
    case "get_sec_filing_index":
      return compactFilingIndexPayload(await getSecFilingIndex(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null));
    case "list_sec_material_filings":
      return listSecMaterialFilings(str(args.ticker), Array.isArray(args.forms) ? args.forms.map(String) : null, num(args.limit, 5));
    case "get_sec_filing_intelligence":
      return getSecFilingIntelligence(str(args.ticker), str(args.filing_type, "10-K"), num(args.filing_index, 0));
    case "get_sec_filing_section_markdown":
      return getSecFilingSectionMarkdown(str(args.ticker), str(args.section, "Item 1A"), str(args.filing_type, "10-K"), num(args.filing_index, 0), num(args.max_chars, 50000));
    case "list_sec_filing_exhibits":
      return listSecFilingExhibits(str(args.ticker), str(args.accessionNumber));
    case "get_sec_filing_exhibit_content":
      return getSecFilingExhibitContent(
        str(args.ticker),
        str(args.accessionNumber),
        str(args.fileName),
        Array.isArray(args.topics) ? args.topics.map(String) : null,
      );
    case "parse_public_transcript":
      return parsePublicTranscript(
        str(args.url),
        Array.isArray(args.topics) ? args.topics.map(String) : null,
        args.raw_text != null ? str(args.raw_text) : null,
      );
    case "get_earnings_call_transcript":
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
    case "extract_geographic_revenue":
      return extractGeographicRevenue(
        str(args.ticker),
        str(args.region),
        str(args.filing_type, "10-K"),
        str(args.period, "latest"),
        args.accession_number != null ? str(args.accession_number) : null,
        str(args.detailLevel, "compact"),
      );
    case "extract_segment_revenue":
      return extractSegmentRevenue(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"));
    case "extract_total_revenue":
      return extractTotalRevenue(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"));
    case "extract_revenue_exposure":
      return extractRevenueExposure(str(args.ticker), str(args.exposure_query), str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"));
    case "extract_china_exposure":
      return extractChinaExposure(
        str(args.ticker),
        str(args.filing_type, "10-K"),
        str(args.period, "latest"),
        args.accession_number != null ? str(args.accession_number) : null,
        str(args.detailLevel, "compact"),
      );
    case "extract_exposure":
      return extractExposure(
        str(args.ticker),
        str(args.topic),
        str(args.filing_type, "10-K"),
        str(args.period, "latest"),
        args.include_risk_factors !== false,
      );
    case "extract_risk_factor_mentions":
      return extractRiskFactorMentions(str(args.ticker), Array.isArray(args.terms) ? args.terms.map(String) : [], str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"));
    case "extract_customer_concentration":
      return extractCustomerConcentration(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"));
    case "get_valuation_snapshot":
      return getValuationSnapshot(
        str(args.ticker),
        args.price != null ? Number(args.price) : null,
        str(args.filing_type, "latest"),
        args.include_sec_filings !== false,
      );
    case "compare_peer_valuations":
      return comparePeerValuations(
        Array.isArray(args.tickers) ? args.tickers.map(String) : [],
        args.subject != null ? str(args.subject) : null,
      );
    case "extract_dilution_bridge":
      return extractDilutionBridge(
        str(args.ticker),
        Number(args.price),
        args.as_of_date != null ? str(args.as_of_date) : null,
        str(args.currency, "USD"),
        str(args.filing_type, "latest"),
        args.accession_number != null ? str(args.accession_number) : null,
        args.include_atm !== false,
      );
    case "extract_capital_structure":
      return extractCapitalStructure(
        str(args.ticker),
        str(args.filing_type, "latest"),
        args.accession_number != null ? str(args.accession_number) : null,
        args.include_funding_statements !== false,
      );
    case "extract_analyst_valuation_methods":
      return extractAnalystValuationMethods(str(args.ticker), num(args.days_back, 30));
    case "get_uk_company_filings":
      return getUkCompanyFilings(
        args.ticker != null ? str(args.ticker) : null,
        args.company_number != null ? str(args.company_number) : null,
        args.company_name != null ? str(args.company_name) : null,
        args.category != null ? str(args.category) : null,
        num(args.limit, 25),
        args.include_charges === true,
      );
    case "query_sec_filing_index":
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
    case "get_latest_earnings_release":
      return getLatestEarningsRelease(str(args.ticker), str(args.period, "latest"));
    case "index_earnings_release":
      return indexEarningsRelease(str(args.ticker), str(args.period, "latest"), args.source_url != null ? str(args.source_url) : null);
    case "extract_earnings_metrics":
      return extractEarningsMetrics(
        str(args.ticker),
        str(args.period, "latest"),
        Array.isArray(args.source_preference) ? args.source_preference.map(String) : ["sec_8k", "company_ir", "10-q", "yahoo"],
      );
    case "extract_guidance":
      return extractGuidance(str(args.ticker), str(args.period, "latest"));
    case "extract_management_commentary":
      return extractManagementCommentary(
        str(args.ticker),
        str(args.period, "latest"),
        Array.isArray(args.topics) ? args.topics.map(String) : [],
      );
    case "compare_earnings_actual_vs_estimate":
      return compareEarningsActualVsEstimate(str(args.ticker), str(args.period, "latest"));
    case "analyze_options_flow_window":
      return getOptionsFlowScan(str(args.ticker), str(args.window_label));
    case "calculate_price_target_distance":
      return getPriceTargetBracket(
        str(args.ticker),
        num(args.reference_target_price ?? args.io_pt, 0),
      );
    case "analyze_position_signals":
      return getPositionScoreInputs(tickerArg(args.ticker));
    case "check_volume_liquidity_threshold":
      return getVolumeGate(str(args.ticker), args.foreign_exchange === true);
    case "search_company_news":
      if (str(args.query).trim() === "") {
        return mcpFailure("search_company_news", ErrorCode.INPUT_VALIDATION_ERROR, "query is required");
      }
      return searchCompanyNews(
        str(args.ticker),
        str(args.query),
        str(args.start_date, ""),
        str(args.end_date, ""),
        Array.isArray(args.sources) ? args.sources.map(String) : ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"],
        num(args.max_results, 10),
      );
    case "get_company_press_releases":
      return getCompanyPressReleases(
        str(args.ticker),
        num(args.lookback_days, 90),
        num(args.max_results, 20),
        Array.isArray(args.sources) ? args.sources.map(String) : ["sec", "company_ir_page", "yahoo_finance_press_releases"],
      );
    case "get_sec_recent_events":
      return getSecRecentEvents(
        str(args.ticker),
        Array.isArray(args.filing_types) ? args.filing_types.map(String) : ["8-K", "10-Q", "10-K"],
        num(args.lookback_days, 90),
        num(args.max_results, 20),
      );
    case "get_public_event_timeline":
      return getPublicEventTimeline(
        str(args.ticker),
        str(args.start_date, ""),
        str(args.end_date, ""),
        Array.isArray(args.sources) ? args.sources.map(String) : ["sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"],
        num(args.max_results, 50),
        args.newest_first === true,
      );
    case "verify_company_event":
      if (str(args.event_query).trim() === "") {
        return mcpFailure("verify_company_event", ErrorCode.INPUT_VALIDATION_ERROR, "event_query is required");
      }
      return verifyCompanyEvent(
        str(args.ticker),
        str(args.event_query),
        str(args.start_date, ""),
        str(args.end_date, ""),
        Array.isArray(args.sources) ? args.sources.map(String) : ["sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"],
      );
    case "health_check": {
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
      });
    }
    case "get_market_snapshot": {
      const t = args.ticker;
      const ticker = Array.isArray(t) ? (t as string[]) : str(t);
      const mode = str(args.mode, "compact") === "full" ? "full" : "compact";
      const foreignExchange = args.foreign_exchange === true;
      return getMarketSnapshot(ticker, mode, foreignExchange);
    }
    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}
