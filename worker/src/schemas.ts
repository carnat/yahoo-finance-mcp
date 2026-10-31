import { HISTORICAL_PERIODS, HISTORICAL_INTERVALS, SUPPORTED_HOLDER_TYPES, SUPPORTED_RECOMMENDATION_TYPES } from "./yahoo-finance.js";
import { GROUPED_TOOL_DEFS, TICKER_CAPS } from "./tool-catalog.js";

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

export function annotationsForTool(name: string): Tool["annotations"] {
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
          description: "Stock ticker symbol (e.g. 'AAPL') or an array of up to 5 symbols (e.g. ['AAPL', 'MSFT']). A longer array is rejected with BATCH_LIMIT_EXCEEDED; split larger lists into multiple calls.",
          oneOf: [
            { type: "string" },
            { type: "array", items: { type: "string" } },
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
      "Search for ticker symbols by company name, partial name, or ISIN. Returns matching quotes with symbol, short name, exchange, and type. Use this to resolve a company name to a ticker before calling other tools. Use exchange='US' to restrict to US listings (NMS, NGM, NCM: Nasdaq; NYQ: NYSE; ASE: NYSE American; PCX: NYSE Arca) — recommended for small/mid-cap US equity searches to avoid foreign listings. Valid exchange values: 'US', a single exchange code such as 'NMS' or 'NYQ', or null (all).",
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
          description: "Optional exchange filter. 'US' for every US listing venue (NMS, NGM, NCM, NYQ, ASE, PCX), or a specific code like 'NMS' or 'NYQ'. Omit to return all exchanges.",
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
            { type: "array", items: { type: "string" } },
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
            { type: "array", items: { type: "string" } },
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
            { type: "array", items: { type: "string" } },
          ],
        },
      },
      required: ["ticker"],
    },
  },
];

const CANONICAL_ADDITIONS: Tool[] = [
  { name: "get_market_quote", description: "Get a lightweight Yahoo regular-market price observation for one or more tickers. lastPrice uses priceBasis=REGULAR_MARKET_PRICE and includes priceTimestamp; use get_price_slope for adjusted daily-bar analytics.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] } }, required: ["ticker"] } },
  { name: "get_historical_prices", description: "Get raw historical OHLCV. Daily rows without a usable close are INCOMPLETE/isFinal=false.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: [...HISTORICAL_PERIODS], default: "1mo" }, interval: { type: "string", enum: [...HISTORICAL_INTERVALS], default: "1d" }, prepost: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "analyze_price_performance", description: "Analyze price performance metrics.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] } }, required: ["ticker"] } },
  { name: "analyze_moving_average_position", description: "Analyze moving-average position.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] } }, required: ["ticker"] } },
  { name: "analyze_volume_ratio", description: "Analyze the latest completed-session volume ratio. PARTIAL/INCOMPLETE means retry; do not use a volume signal.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] }, period: { type: "number", default: 10 } }, required: ["ticker"] } },
  { name: "check_volume_liquidity_threshold", description: "Completed-session traded value: adv20dTradedValueUsd is the average daily traded value (volume x close) of the 20 sessions before the latest completed one, converted to USD for non-USD listings. ratio20d compares the latest session with that average. Pass min_adv_usd for a caller-chosen threshold; meetsMinimum reports adv20dTradedValueUsd >= min_adv_usd. PARTIAL/INCOMPLETE means retry; the average is unknown.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, foreign_exchange: { type: "boolean", default: false, description: "Accepted for compatibility; non-USD listings are always converted to USD." }, min_adv_usd: { type: "number", exclusiveMinimum: 0, description: "Optional caller-chosen minimum 20-day average daily traded value in USD; when supplied, meetsMinimum compares adv20dTradedValueUsd against it." } }, required: ["ticker"] } },
  { name: "get_company_profile", description: "Get company profile/fundamentals.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] }, include_all: { type: "boolean" } }, required: ["ticker"] } },
  { name: "get_fund_profile", description: "Get ETF/fund profile with per-section status and as-of-date limitations. Valuation characteristics are conventional multiples; provider inverse yields are retained separately. Request overview, holdings, allocation, operations, or fixed-income sections.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] }, sections: { type: "array", items: { type: "string", enum: ["overview", "holdings", "allocation", "operations", "fixed_income"] }, uniqueItems: true } }, required: ["ticker"] } },
  { name: "analyze_financial_ratios", description: "Analyze current financial ratios with explicit unitSemantics and optional historical Yahoo valuation measures.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] }, history_periods: { type: "integer", minimum: 0, maximum: 20, default: 0 }, frequency: { type: "string", enum: ["quarterly", "monthly", "yearly", "trailing"], default: "quarterly" } }, required: ["ticker"] } },
  { name: "analyze_share_count_trend", description: "Use for dilution, issuance, buyback, or historical shares-outstanding questions. Returns contextual Yahoo data and directs material changes to SEC confirmation.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, start_date: { type: "string" }, end_date: { type: "string" } }, required: ["ticker"] } },
  { name: "analyze_credit_health", description: "Analyze credit health metrics.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] } }, required: ["ticker"] } },
  { name: "get_corporate_actions", description: "Get stock splits, dividends, and fund capital-gain distributions from Yahoo Finance, oldest first. Every split is returned; dividends and capital gains are limited to the most recent `limit` on or after `start_date`.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, start_date: { type: "string", pattern: "^\\d{4}-\\d{2}-\\d{2}$", description: "Earliest dividend/capital-gain date (YYYY-MM-DD)." }, limit: { type: "integer", minimum: 1, maximum: 1000, default: 40, description: "Most recent dividends/capital gains to return." } }, required: ["ticker"] } },
  { name: "get_ownership_holders", description: "Get ownership/holder data. Supported holder_type values: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, holder_type: { type: "string", enum: SUPPORTED_HOLDER_TYPES, description: "One of: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders." } }, required: ["ticker", "holder_type"] } },
  { name: "get_expanded_institutional_ownership", description: "Use only when Yahoo's ordinary top-holder view is insufficient. Tries eligible Finnhub coverage first and never spends scarce Alpha quota unless allow_scarce_fallback=true. Results are contextual; verify material ownership claims against SEC 13F filings.", inputSchema: { type: "object", properties: { ticker: { type: "string", minLength: 1 }, allow_scarce_fallback: { type: "boolean", default: false, description: "Explicitly permit one Alpha Vantage call if Finnhub is unavailable, ineligible, or returns no usable holders." }, max_holders: { type: "integer", minimum: 1, maximum: 100, default: 50 } }, required: ["ticker"] } },
  { name: "get_analyst_recommendations", description: "Get analyst recommendations or upgrade/downgrade history. Supported recommendation_type values: recommendations, upgrades_downgrades.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, recommendation_type: { type: "string", enum: SUPPORTED_RECOMMENDATION_TYPES, description: "One of: recommendations, upgrades_downgrades." }, months_back: { type: "number", default: 12 } }, required: ["ticker", "recommendation_type"] } },
  { name: "get_analyst_rating_changes", description: "Get analyst rating changes radar.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] }, days_back: { type: "number", default: 30 } }, required: ["ticker"] } },
  { name: "analyze_earnings_momentum", description: "Analyze earnings momentum.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] } }, required: ["ticker"] } },
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
  { name: "extract_sec_filing_fact", description: "Extract SEC filing fact.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, fact: { type: "string" }, fact_name: { type: "string" }, fact_type: { type: "string" }, region: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "\"latest\" or a fiscal year (\"FY2025\" or \"2025\"); a fiscal year selects the annual period the issuer calls that year. Other values are rejected." }, period_mode: { type: "string", default: "auto", description: "quarter, ytd, or annual; auto selects quarter for 10-Q and annual for 10-K." }, document_url: { type: "string" }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "search_sec_filing_text", description: "Search the visible text of SEC filings: whole words (plurals included) with curly/straight quotes and dashes folded. search_terms are phrases; search_query also takes \"exact phrase\", A NEAR/n B and -excluded. Matches merge per passage, rank risk factors and MD&A first with every term represented, and page with max_matches/cursor; termStats and hitsBySection summarize all hits. filing_count/since search several filings; include_exhibits adds EX-99 exhibits.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, search_terms: { type: "array", items: { type: "string" } }, search_query: { type: "string" }, exclude_terms: { type: "array", items: { type: "string" } }, near: { type: "array", items: { type: "object", properties: { terms: { type: "array", items: { type: "string" }, minItems: 2, maxItems: 2 }, within_words: { type: "number", minimum: 1, maximum: 200, default: 10 } }, required: ["terms"] } }, match: { type: "string", enum: ["word", "substring"], default: "word" }, order: { type: "string", enum: ["relevance", "document"], default: "relevance" }, max_matches: { type: "number", minimum: 1, maximum: 50, default: 10 }, cursor: { type: "string" }, section_hint: { type: "string" }, selector: { type: "object" }, filing_type: { type: "string", default: "10-K" }, accession_number: { type: "string" }, document_url: { type: "string" }, filing_count: { type: "number", minimum: 1, maximum: 5, default: 1 }, since: { type: "string", pattern: "^\\d{4}-\\d{2}-\\d{2}$" }, include_exhibits: { type: "boolean", default: false }, context_chars: { type: "number", default: 1500 }, return_tables: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "get_sec_filing_index", description: "Build or retrieve the cached section/table index for an SEC filing. Identifies headings, tables, row labels, and units. period is reserved for future multi-period support; currently only 'latest' is supported unless accession_number is provided.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "Reserved. Only 'latest' supported currently." }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "list_sec_material_filings", description: "List latest material SEC filings for a ticker, filtering out noise (Form 4, 144, SC 13G, etc.). Returns only significant filings (10-K, 10-Q, 8-K, S-1, 424B, DEF 14A, 20-F, 6-K by default).", inputSchema: { type: "object", properties: { ticker: { type: "string" }, forms: { type: "array", items: { type: "string" }, default: ["10-K", "10-Q", "8-K", "S-1", "424B", "DEF 14A", "20-F", "6-K"] }, limit: { type: "number", default: 5 } }, required: ["ticker"] } },
  { name: "get_sec_filing_intelligence", description: "Preferred SEC diagnostic call. Returns an accession-matched official companyfacts snapshot, usable section/table index summary, evidence metadata, and recommended follow-ups.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, filing_index: { type: "number", default: 0 } }, required: ["ticker"] } },
  { name: "get_sec_filing_section_markdown", description: "Return a specific SEC filing section as unverified Markdown from a degraded Worker HTML fallback. Payloads are blocked from decision-grade use and include source offsets/warnings.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, section: { type: "string", default: "Item 1A" }, filing_type: { type: "string", default: "10-K" }, filing_index: { type: "number", default: 0 }, max_chars: { type: "number", default: 50000 } }, required: ["ticker"] } },
  { name: "analyze_position_signals", description: "Aggregate public market, analyst, earnings, and technical inputs that may be useful for a caller-defined scoring model. This tool does not access holdings, cost basis, position size, or private scoring rules.", inputSchema: { type: "object", properties: { ticker: { type: "string" } }, required: ["ticker"] } },
  { name: "calculate_price_target_distance", description: "Compare current price to a user-supplied reference price target. Returns both the legacy current/target ratio and directional percent distance to target.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, reference_target_price: { type: "number", description: "Preferred: user-supplied reference target price." }, io_pt: { type: "number", description: "Backward-compatible alias for reference_target_price." } }, required: ["ticker"] } },
  { name: "get_company_news", description: "Get recent public company news and press releases with compact coverage guidance. The default sources are Yahoo Finance, SEC filings and eligible Finnhub, then one quota-bounded Marketaux wire-domain fallback only when higher-priority coverage is empty or limited. Marketaux never paginates automatically and is contextual evidence only. Yahoo primary items are emitted only for an exact ticker token or canonical issuer identity; inspect tickerMatch=EXPLICIT and matchBasis before use. Read coverage.state, failedSources, skippedSources, recommendedNextAction, and per-source diagnostics before treating an empty result as absence. decisionUse=CHECK_OFFICIAL_RELEASES means escalate the item to get_company_press_releases or verify_company_event. Direct newswire RSS is an explicit shallow snapshot, not a default. Accepts a ticker or array of up to 5 symbols.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }], description: "Ticker symbol (e.g. 'AAPL') or an array of up to 5 symbols (e.g. ['AAPL', 'MSFT']); only one symbol when sources includes company_ir, company_ir_page or newswire. A longer array is rejected with BATCH_LIMIT_EXCEEDED; split larger lists into multiple calls." }, max_results: { type: "number", default: 10 }, lookback_days: { type: "number", default: 14 }, sources: { type: "array", items: { type: "string" }, default: ["yahoo_finance_news", "yahoo_finance_press_releases", "sec", "finnhub", "marketaux"], description: "Allowed sources: yahoo_finance_news, yahoo_finance_press_releases, finnhub, marketaux, sec, newswire, company_ir, company_ir_page, or legacy yahoo_finance. Marketaux is quota-aware fallback by default; explicit sources=['marketaux'] forces one bounded request. Omit for the default set; an explicit list is used as given, and [] attempts no source. requestedSources, defaultSourcesApplied and attemptedSources report what was asked for and tried." } }, required: ["ticker"] } },
  { name: "search_company_news", description: "Search public company news/events for a ticker and query across selected source metadata and short snippets. Defaults use Yahoo, SEC filings and eligible Finnhub, then a single Marketaux wire-domain fallback only for empty or limited higher-priority coverage. Include company_ir or company_ir_page for official-source evidence; inspect sourceStatus/sourceCoverage before relying on absence.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Ticker symbol, e.g. 'AAPL'" }, query: { type: "string", description: "Required search query string." }, start_date: { type: "string", default: "" }, end_date: { type: "string", default: "" }, sources: { type: "array", items: { type: "string" }, default: ["yahoo_finance_news", "yahoo_finance_press_releases", "sec", "finnhub", "marketaux"], description: "Omit for the default set; an explicit list is used as given, and [] attempts no source. requestedSources, defaultSourcesApplied and attemptedSources report what was asked for and tried." }, max_results: { type: "number", default: 10 } }, required: ["ticker", "query"] } },
  { name: "get_company_press_releases", description: "Get company press releases and official release-style events. Defaults resolve SEC 8-K/EX-99 evidence first, then registry-backed company_ir_page, then Yahoo press-release context. Explicit sources can also include company_ir RSS/Atom, quota-bounded Marketaux, and direct newswire RSS. Gate use is payload-level: decisionGrade:true is allowed only for coverageStatus=SEC_EX99_RESOLVED or APPROVED_IR_PAGE_RESOLVED with evidence fields; candidates/RSS/Marketaux/newswire/Yahoo remain verification/context evidence.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, lookback_days: { type: "number", default: 90 }, max_results: { type: "number", default: 20 }, sources: { type: "array", items: { type: "string" }, default: ["sec", "company_ir_page", "yahoo_finance_press_releases"], description: "Omit for the default set; an explicit list is used as given, and [] attempts no source. requestedSources, defaultSourcesApplied and attemptedSources report what was asked for and tried." } }, required: ["ticker"] } },
  { name: "get_sec_recent_events", description: "Get recent SEC filing events with filing type, filing date, accepted timestamp, accession number, SEC archive URL, and event metadata.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_types: { type: "array", items: { type: "string" }, default: ["8-K", "8-K/A", "10-Q", "10-K"] }, lookback_days: { type: "number", default: 90 }, max_results: { type: "number", default: 20 } }, required: ["ticker"] } },
  { name: "get_public_event_timeline", description: "Get a deduplicated chronological timeline across SEC, reviewed/RSS company IR, Yahoo, Finnhub, and quota-aware Marketaux fallback. Direct newswire RSS is explicit because it is a shallow, potentially slow snapshot. Returns sourceStatus/sourceCoverage so source limitations are distinguishable from no events.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, start_date: { type: "string", default: "" }, end_date: { type: "string", default: "" }, sources: { type: "array", items: { type: "string" }, default: ["sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"], description: "Omit for the default set; an explicit list is used as given, and [] attempts no source. requestedSources, defaultSourcesApplied and attemptedSources report what was asked for and tried." }, max_results: { type: "number", default: 50 }, newest_first: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "verify_company_event", description: "Verify whether a public company event is source-backed across SEC, reviewed/RSS company IR, Yahoo, Finnhub, and quota-aware Marketaux fallback. Direct newswire RSS is explicit. Generic publication words do not establish a match by themselves; inspect queryPolicy and each queryMatch. Returns CONFIRMED, PARTIAL, NOT_FOUND, SOURCE_LIMITED_NOT_FOUND, STALE, or CONFLICTING. Marketaux evidence remains contextual and must be escalated to an official release.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, event_query: { type: "string", description: "Specific event terms to verify, such as acquisition, dividend, guidance, contract, or offering." }, start_date: { type: "string", default: "" }, end_date: { type: "string", default: "" }, sources: { type: "array", items: { type: "string" }, default: ["sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"], description: "Omit for the default set; an explicit list is used as given, and [] attempts no source. requestedSources, defaultSourcesApplied and attemptedSources report what was asked for and tried." } }, required: ["ticker", "event_query"] } },
  { name: "extract_geographic_revenue", description: "Extract geographic revenue exposure from official SEC data and indexed filing tables. Returns explicit parser/provider limitation statuses instead of silent non-disclosure when data cannot be parsed.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, region: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "\"latest\" or a fiscal year (\"FY2025\" or \"2025\"); a fiscal year selects the annual period the issuer calls that year. Other values are rejected." }, accession_number: { type: "string" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker", "region"] } },
  { name: "extract_segment_revenue", description: "Extract segment revenue rows from official SEC facts and filing tables; may return explicit limitation statuses when no parseable segment data is found.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "\"latest\" or a fiscal year (\"FY2025\" or \"2025\"); a fiscal year selects the annual period the issuer calls that year. Other values are rejected." }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker"] } },
  { name: "extract_total_revenue", description: "Extract total revenue from official SEC facts or filing tables with evidence metadata.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "\"latest\" or a fiscal year (\"FY2025\" or \"2025\"); a fiscal year selects the annual period the issuer calls that year. Other values are rejected." } }, required: ["ticker"] } },
  { name: "extract_revenue_exposure", description: "Extract revenue exposure for a region/customer/segment query, returning explicit parser/provider limitation statuses when no decision-grade value is available.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, exposure_query: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "\"latest\" or a fiscal year (\"FY2025\" or \"2025\"); a fiscal year selects the annual period the issuer calls that year. Other values are rejected." }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker", "exposure_query"] } },
  { name: "extract_china_exposure", description: "Extract China exposure with separate revenue and non-revenue classifications; revenue values are decision-grade only when evidence and status support them.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "\"latest\" or a fiscal year (\"FY2025\" or \"2025\"); a fiscal year selects the annual period the issuer calls that year. Other values are rejected." }, accession_number: { type: "string" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker"] } },
  { name: "extract_risk_factor_mentions", description: "Extract concise risk-factor term mentions from SEC filings.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, terms: { type: "array", items: { type: "string" } }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker", "terms"] } },
  { name: "extract_customer_concentration", description: "Extract customer concentration percentages from SEC filings.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker"] } },
  { name: "get_valuation_snapshot", description: "Valuation context for one ticker at the current price or one you supply: diluted shares from the SEC dilution bridge at that price, the filing's period-end cash and debt, equity and enterprise value (in-the-money convertibles leave the debt), EV/Revenue, EV/EBITDA and P/E on trailing results and Yahoo's current and next fiscal-year consensus with analyst counts, and ATM capacity. Falls back to Yahoo figures for non-USD or non-SEC listings. Mechanical context, not a price target; nothing is back-solved.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, price: { type: "number", exclusiveMinimum: 0 }, filing_type: { type: "string", default: "latest" }, include_sec_filings: { type: "boolean", default: true } }, required: ["ticker"] } },
  { name: "compare_peer_valuations", description: "The same multiples for a peer set on one Yahoo basis: price x shares outstanding + total debt - total cash, over trailing results and current and next fiscal-year consensus (EV/Revenue, EV/EBITDA, P/E, revenue growth, gross margin). Returns peer medians, min and max excluding the subject, and the subject's premium or discount to each median. Context, not a signal.", inputSchema: { type: "object", properties: { tickers: { type: "array", items: { type: "string" }, minItems: 1, maxItems: 10 }, subject: { type: "string" } }, required: ["tickers"] } },
  { name: "extract_dilution_bridge", description: "Basic-to-diluted share bridge at a price you supply, from the filing's inline XBRL: cover-page basic shares, options (treasury-stock method, by exercise-price range when tagged), unvested RSUs/PSUs (gross), warrants per class (treasury stock), convertibles and convertible preferred (if-converted when in the money) and ATM remaining capacity from filing text. Mechanical and company-disclosed, not a consensus diluted share count; never back-solve it into one. status covers tagged instruments only, never a full claim inventory: share claims stated only in filing text (price protection, anti-dilution rights, forward sales, contingent or share-settled obligations, exchangeable interests, SAFEs, equity lines, post-period warrants) are quoted in unquantifiedShareClaims and never counted. filing_type latest uses the newest 10-Q with the last 10-K as fallback.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, price: { type: "number", exclusiveMinimum: 0 }, as_of_date: { type: "string", pattern: "^\\d{4}-\\d{2}-\\d{2}$" }, currency: { type: "string", default: "USD" }, filing_type: { type: "string", default: "latest" }, accession_number: { type: "string" }, include_atm: { type: "boolean", default: true } }, required: ["ticker", "price"] } },
  { name: "get_share_count_scenarios", description: "Share counts under scenarios you define: for each scenario's price and treatments (options treasury-stock/gross/exclude, unvested awards gross/exclude, warrants treasury-stock/gross/exclude on vested or all, convertibles if-converted when in the money/all/net-share settlement where the filing states principal is settled in cash/exclude, ATM remaining capacity include/exclude, capped calls ignored or offset where the filing states their strike, cap and covered shares, and known issuance you supply), every instrument from the filing's inline XBRL is listed as included or excluded with its treasury-stock or if-converted mechanics and threshold price; unresolved instruments are listed and left out. Scenarios are reported side by side; no denominator is selected. Evidence only.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, scenarios: { type: "array", minItems: 1, maxItems: 8, items: { type: "object", properties: { name: { type: "string" }, price: { type: "number", exclusiveMinimum: 0 }, options: { type: "string", enum: ["treasury_stock", "gross", "exclude"] }, unvested_awards: { type: "string", enum: ["gross", "exclude"] }, warrants: { type: "string", enum: ["treasury_stock", "gross", "exclude"] }, warrant_vesting: { type: "string", enum: ["vested_only", "all"] }, convertibles: { type: "string", enum: ["if_converted_when_in_the_money", "if_converted_all", "net_share_settlement_when_stated", "exclude"] }, atm: { type: "string", enum: ["exclude", "full_remaining_capacity"] }, capped_calls: { type: "string", enum: ["ignore", "offset_when_stated"] }, known_issuance: { type: "array", maxItems: 10, items: { type: "object", properties: { label: { type: "string" }, shares: { type: "number" }, source: { type: "string" } }, required: ["label", "shares"] } } }, required: ["name", "price"] } }, filing_type: { type: "string", default: "latest" }, accession_number: { type: "string" }, currency: { type: "string", default: "USD" } }, required: ["ticker", "scenarios"] } },
  { name: "extract_funding_capex_schedule", description: "Funding and capex schedule from the latest periodic filing: tagged contractual schedules (debt principal, operating and finance lease payments, purchase and contractual obligations by due period; purchase commitments) and funding or capex statements from the text, each classified CONTRACTUAL, COMPANY_DISCLOSED_COMMITTED, COMPANY_GUIDED, AWARDED_CONTINGENT or UNRESOLVED with amount, timing and the quoted source; plus liquidity sources (cash, undrawn facilities, ATM capacity). Nothing is netted or forecast. Evidence only.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "latest" }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "extract_operating_driver_ledger", description: "Operating drivers as the company discloses them: SEC XBRL series (revenue, gross profit, R&D, capex, remaining performance obligations, contract liabilities; newest filing per period, year-to-date periods kept as filed), the filer's own tagged KPIs from its latest periodic filing, and filing and earnings-release statements with figures by category (capacity, production, deliveries, launches and deployments, customers, backlog and bookings, utilization, pricing, yield, headcount), each marked REPORTED_ACTUAL, TARGET_OR_PLAN or UNCLEAR with timing and the quoted source. Untagged drivers are listed as not reported and unread sources as not read; nothing is derived or annualized. Evidence only.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "latest" }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "extract_capital_structure", description: "Company-disclosed capital structure at the filing's period end: cash, short-term investments, total debt and net cash; each debt instrument's face amount, carrying amount, coupon, maturity and conversion terms from dimensional inline XBRL (which companyfacts omits); the tagged maturity ladder; and the company's own funding, runway, going-concern and ATM statements quoted from the filing. Disclosed, not forecast.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "latest" }, accession_number: { type: "string" }, include_funding_statements: { type: "boolean", default: true } }, required: ["ticker"] } },
  { name: "extract_analyst_valuation_methods", description: "Valuation methods analysts name in recent news headlines and summaries: multiples with metric and period (e.g. 41x 2H27 EV/EBITDA), DCF with WACC, discount rate and terminal growth, sum-of-the-parts and rNPV, with firm, price target and source link. Lists firms whose targets carry no disclosed method. Context only: not consensus inputs.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, days_back: { type: "number", minimum: 1, maximum: 365, default: 30 } }, required: ["ticker"] } },
  { name: "get_consensus_forecast_curve", description: "Street consensus by fiscal year, FY0 to FY+horizon, for EPS and revenue from Yahoo Finance and Alpha Vantage, each provider reported separately with fiscal year end, currency, mean/high/low, analyst count and retrieval time. Each metric and period states its coverage: PROVIDER_COVERED, PROVIDER_NOT_COVERED, INSUFFICIENT_ANALYST_COUNT or PROVIDER_CONFLICT (with the cross-provider difference). Years and metrics no provider covers stay PROVIDER_NOT_COVERED; nothing is interpolated, extended by growth rates, or derived. crossCheck states whether the providers were actually compared: a failed provider (CROSS_CHECK_DEGRADED) or figures identical to the last digit (IDENTICAL, PROVIDERS_NOT_INDEPENDENT) are flagged. A provider row whose mean falls outside its own high-low range is PROVIDER_INCONSISTENT and left out of the comparison; an EPS gap between providers that matches a recent stock split is NOT_SPLIT_ADJUSTED, not a conflict. When Alpha Vantage refuses a request for quota, a timeout or an upstream error, its figures stored up to 7 days earlier stand in, marked with their retrieval time, staleFallback and PROVIDER_DATA_STALE. With observation_date, returns the curve stored that day. Evidence only.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, horizon_years: { type: "integer", minimum: 1, maximum: 5, default: 5 }, min_analyst_count: { type: "integer", minimum: 1, maximum: 50, default: 3 }, conflict_tolerance_pct: { type: "number", minimum: 0, maximum: 100, default: 10 }, observation_date: { type: "string", pattern: "^\\d{4}-\\d{2}-\\d{2}$", description: "Read the consensus observation stored on this date (YYYY-MM-DD) instead of fetching providers now; get_eps_revisions lists the stored dates." } }, required: ["ticker"] } },
  { name: "get_eps_revisions", description: "EPS estimate revision windows for FY0 and FY+1 as each provider reports them: the mean now and 7, 30, 60 and 90 days ago with change and percent change, and up/down revision counts over 7 and 30 days. A window spanning a stock split carries no change (SPLIT_IN_WINDOW), nor does a row whose mean falls outside its own range (PROVIDER_INCONSISTENT). Stored figures standing in for a refused provider request (PROVIDER_DATA_STALE) have windows ending at their retrieval time. Revenue revisions and analyst adds/drops are PROVIDER_NOT_COVERED. Lists the dates of stored daily consensus observations. Evidence only.", inputSchema: { type: "object", properties: { ticker: { type: "string" } }, required: ["ticker"] } },
  { name: "get_historical_valuation_context", description: "Market cap, enterprise value and EV/Revenue, EV/EBITDA, P/E and P/S as they stood at each date, for a ticker and caller-named peers on the same dates. Point in time: SEC companyfacts filed on or before each date; the close on or before it with later split adjustments undone; balances at the latest balance date then filed; LTM (LFY + YTD - prior YTD) and LFY denominators with their periods, concepts and filings; reporting currency converted at that date's FX close; ADR share counts divided by ordinary shares per ADS. Shares are point in time: the cover-page or balance-sheet count, else the filing's per-class cover counts summed; a weighted-average count never sets market cap (POINT_IN_TIME_SHARES_UNRESOLVED). EBITDA is computed (operating income + D&A). Untagged or unfiled figures leave their multiples null with a status; status is OK only when all eight multiples are usable (coreStatus covers market cap and EV, coverage lists the rest); peer medians are unweighted. No multiple is selected. Evidence only.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, dates: { type: "array", items: { type: "string", pattern: "^\\d{4}-\\d{2}-\\d{2}$" }, maxItems: 12, description: "Default: the latest close and its anniversaries over five years." }, peers: { type: "array", items: { type: "string" }, maxItems: 5 } }, required: ["ticker"] } },
  { name: "reconcile_metric_sources", description: "One metric for one period as each source states it: SEC XBRL as first filed and as latest filed (a difference is a restatement), the issuer's earnings release (only sentences naming the metric, an amount and the period's scope) and Yahoo's statement row, each compared with the latest SEC value with difference, percentage and tolerance (the larger of tolerance_pct and half the release's last stated digit). Status AGREED requires latest SEC plus at least one independent matching provider; PARTIAL covers a missing SEC baseline or only one provider; otherwise CONFLICT or NOT_FOUND, with source evidence. Metrics: revenue, net_income, operating_income, eps_diluted, cash_and_equivalents. Evidence only.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, metric: { type: "string", enum: ["revenue", "net_income", "operating_income", "eps_diluted", "cash_and_equivalents"] }, period: { type: "string", default: "latest", description: "latest (the latest quarter), FY<yyyy> (the issuer's fiscal year from SEC fy/fp, else the fiscal year ending in that year) or FY<yyyy> Q<n> (the issuer's fiscal quarter)" }, tolerance_pct: { type: "number", minimum: 0, maximum: 10, default: 0.5 } }, required: ["ticker", "metric"] } },
  { name: "get_evidence_quality", description: "Preflight before a valuation evidence pack: status, freshness and coverage per evidence family (quote, latest SEC periodic filing, capital structure and dilution readiness, earnings-release guidance, material 8-Ks, FY0/FY+1 consensus cells, durable storage) and the blockers, from light requests only. Evidence only.", inputSchema: { type: "object", properties: { ticker: { type: "string" } }, required: ["ticker"] } },
  { name: "build_valuation_evidence_pack", description: "One evidence cut for valuation work: quote, evidence quality, consensus curve, EPS revisions, current capital structure, dilution bridge at the current price, latest guidance and material filings, each with its source tool, status, warnings and failure, plus a provenance receipt hashing every component. decisionUse is EVIDENCE_ONLY. Stored immutably under a content-addressed evidenceCutId when storage is available; the full payload is returned either way.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, horizon_years: { type: "integer", minimum: 1, maximum: 5, default: 5 }, persist: { type: "boolean", default: true } }, required: ["ticker"] } },
  { name: "get_evidence_cut", description: "Retrieve a stored evidence cut by evidenceCutId and verify its integrity: the SHA-256 of the stored canonical JSON must equal the hash in the id (integrity VERIFIED or MISMATCH).", inputSchema: { type: "object", properties: { evidence_cut_id: { type: "string" } }, required: ["evidence_cut_id"] } },
  { name: "list_evidence_cuts", description: "Stored evidence cuts for a ticker, newest first, with their ids and cutoff times.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, limit: { type: "integer", minimum: 1, maximum: 100, default: 20 } }, required: ["ticker"] } },
  { name: "get_uk_company_filings", description: "UK Companies House filing history for a UK-registered issuer (e.g. IQE.L): accounts, share allotments (SH01), charges (MR01, secured lending) and resolutions, with document links; include_charges adds the charge register. Resolves by company_number, company_name, or the ticker's issuer name. Official statutory filings; RNS market announcements are not held here. Needs the COMPANIES_HOUSE_API_KEY secret.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, company_number: { type: "string" }, company_name: { type: "string" }, category: { type: "string", enum: ["accounts", "capital", "mortgage", "confirmation-statement", "resolution", "incorporation", "officers", "persons-with-significant-control", "address", "annotation", "change-of-name", "miscellaneous"] }, limit: { type: "number", minimum: 1, maximum: 100, default: 25 }, include_charges: { type: "boolean", default: false } } } },
  { name: "extract_exposure", description: "Extract multi-dimensional SEC exposure for a geographic region or named entity/topic. Returns revenue, operational, named-entity, and risk evidence with explicit non-decision-grade statuses when parser/provider limits prevent a value.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Ticker symbol, e.g. 'AAPL'" }, topic: { type: "string", description: "Geographic region or entity to search for, e.g. 'china', 'russia', 'europe', 'huawei'. Case-insensitive." }, filing_type: { type: "string", default: "10-K", description: "SEC filing type: '10-K' or '20-F'." }, period: { type: "string", default: "latest", description: "\"latest\" or a fiscal year (\"FY2025\" or \"2025\"); a fiscal year selects the annual period the issuer calls that year. Other values are rejected." }, include_risk_factors: { type: "boolean", default: true } }, required: ["ticker", "topic"] } },
  { name: "query_sec_filing_index", description: "Deterministically route supported SEC filing query types to index-backed extractor tools.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "\"latest\" or a fiscal year (\"FY2025\" or \"2025\"); a fiscal year selects the annual period the issuer calls that year. Other values are rejected." }, accession_number: { type: "string" }, query_type: { type: "string", enum: ["geographic_revenue_share", "revenue_exposure", "china_exposure", "risk_factor_mentions", "customer_concentration", "total_revenue", "segment_revenue"] }, params: { type: "object", default: {} }, return_evidence: { type: "boolean", default: true }, detailLevel: { type: "string", default: "compact", enum: ["compact", "evidence", "raw"] } }, required: ["ticker", "query_type"] } },
  { name: "get_latest_earnings_release", description: "Resolve the latest public earnings release source for a ticker. Fiscal period is returned only when explicit release text resolves it; otherwise it remains unresolved.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" }, period: { type: "string", enum: ["latest"], default: "latest", description: "Period selector. Only 'latest' is supported." } }, required: ["ticker"] } },
  { name: "index_earnings_release", description: "Build a compact section/table index of the latest public earnings release for deterministic metric extraction.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, period: { type: "string", enum: ["latest"], default: "latest" }, source_url: { type: "string", description: "Optional override URL (must be https://www.sec.gov/Archives/ or company IR). Paywalled sources are blocked." } }, required: ["ticker"] } },
  { name: "extract_earnings_metrics", description: "Extract reported earnings metrics from SEC 8-K or public IR source. EX-99 prose or unscoped iXBRL values remain non-decision-grade until their period is structurally matched.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" }, source_preference: { type: "array", items: { type: "string", enum: ["sec_8k", "company_ir", "10-q", "yahoo"] }, description: "Accepted for compatibility; sources are tried in a fixed order (SEC XBRL quarter facts, then the 8-K release).", default: ["sec_8k", "company_ir", "10-q", "yahoo"] } }, required: ["ticker"] } },
  { name: "extract_guidance", description: "Extract company-provided forward ranges from the resolved official SEC earnings exhibit, including 'between X and Y' wording. Guidance remains non-decision-grade unless separately verified.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" } }, required: ["ticker"] } },
  { name: "get_guidance_history", description: "Company guidance across recent earnings releases (SEC 8-K Item 2.02, EX-99.1): each revenue, gross-margin and EPS range with its stated target period and source; revisions between consecutive releases for the same metric and period (INITIATED, RAISED, LOWERED, NARROWED, WIDENED, REAFFIRMED, WITHDRAWN); and outcomes comparing the later reported SEC XBRL actual with the first and last range (BELOW, WITHIN, ABOVE). Ranges without a stated period are listed but not compared; unread releases are reported, never treated as withdrawn. Evidence only.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, max_releases: { type: "integer", default: 8, minimum: 1, maximum: 12 } }, required: ["ticker"] } },
  { name: "extract_management_commentary", description: "Extract topic-keyed management commentary snippets from the latest earnings release. Returns first relevant sentence per topic.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" }, topics: { type: "array", items: { type: "string" }, description: "Topics to search for, e.g. ['AI', 'margins', 'guidance', 'supply chain']" } }, required: ["ticker"] } },
  { name: "compare_earnings_actual_vs_estimate", description: "Compare official-release actuals with Yahoo's historical estimate row. The official fiscal label remains period/reportedPeriod; estimatePeriod and reportedDate identify the Yahoo row; releaseDate is the earnings release date. Read periodAlignmentStatus before using cross-source revenue comparisons. Returns epsDelta and omits percentage surprise for near-zero estimates.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" } }, required: ["ticker"] } },
  { name: "list_sec_filing_exhibits", description: "List all exhibits/documents attached to a specific SEC filing by accession number.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, accessionNumber: { type: "string", description: "SEC filing accession number, e.g. '0000320193-24-000081'" } }, required: ["ticker", "accessionNumber"] } },
  { name: "get_sec_filing_exhibit_content", description: "Fetch and return the text content of a specific exhibit from an SEC filing. Supports topic-based paragraph filtering to reduce token usage.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, accessionNumber: { type: "string", description: "SEC filing accession number" }, fileName: { type: "string", description: "Exhibit filename from the filing index" }, topics: { type: "array", items: { type: "string" }, description: "Optional list of keywords/topics to filter paragraphs by" } }, required: ["ticker", "accessionNumber", "fileName"] } },
  { name: "parse_public_transcript", description: "Fetch and parse a public transcript page (Motley Fool, company IR, etc.). Supports topic-based paragraph filtering to reduce token usage. Provide raw_text to skip URL fetching.", inputSchema: { type: "object", properties: { url: { type: "string", description: "Public https URL of the transcript page" }, topics: { type: "array", items: { type: "string" }, description: "Optional list of keywords/topics to filter paragraphs by" }, raw_text: { type: "string", description: "Raw HTML or text content to parse directly (bypasses URL fetching)" } } } },
  { name: "get_earnings_call_transcript", description: "Retrieve an earnings-call transcript from a content-validated SEC exhibit, then Yahoo/Quartr, then optional Alpha Vantage. Yahoo results are structured and paragraph-paginated for LLM use and include the human-readable sourceUrl. Preview-only Yahoo payloads are reported as INCOMPLETE_TRANSCRIPT and continue to the alternate-source fallback. Inspect contentCompleteness rather than treating cursor exhaustion as proof of a full call. Yahoo contentSha256 identifies the canonical transcript text and speaker projection declared by contentHashBasis, so it remains stable across pages and runtimes. Provide source_url or event_id to target a known Yahoo call. Yahoo and Alpha transcripts are contextual and never decision-grade; filing dates are never treated as fiscal periods.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, period: { type: "string", enum: ["latest"], default: "latest", description: "Event selector. Only the latest release is supported; this is not a fiscal-quarter value." }, fiscal_quarter: { type: "string", pattern: "^[0-9]{4}Q[1-4]$", description: "Optional issuer fiscal quarter, e.g. 2026Q4. Do not derive this from the filing or publication date." }, event_id: { type: "string", pattern: "^[0-9]+$", description: "Optional Yahoo earnings-event ID. Use only to continue a previously returned Yahoo transcript." }, source_url: { type: "string", format: "uri", description: "Optional canonical finance.yahoo.com transcript URL returned by this tool. Other hosts and ticker mismatches are rejected." }, paragraph_limit: { type: "integer", minimum: 1, maximum: 50, default: 20, description: "Maximum structured Yahoo transcript paragraphs to return." }, paragraph_cursor: { type: "string", pattern: "^[0-9]+$", description: "Opaque nextCursor from a prior Yahoo transcript response." }, topics: { type: "array", items: { type: "string" }, description: "Optional list of keywords/topics to filter transcript paragraphs by" } }, required: ["ticker"] } },
  { name: "get_market_snapshot", description: "Compact market-state packet composing quote, price performance, moving-average trend, volume ratios, liquidity gate, and technical indicators in one call. Supports compact (default) and full modes, and optional batch of tickers. Pass min_adv_usd for a caller-chosen threshold; meetsMinimum reports adv20dTradedValueUsd >= min_adv_usd.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" } }] }, mode: { type: "string", enum: ["compact", "full"], default: "compact" }, foreign_exchange: { type: "boolean", default: false }, min_adv_usd: { type: "number", exclusiveMinimum: 0, description: "Optional caller-chosen minimum 20-day average daily traded value in USD; when supplied, meetsMinimum compares adv20dTradedValueUsd against it." } }, required: ["ticker"] } },
  { name: "health_check", description: "Return public-safe MCP availability, release/build identity, schema identity, tool mode, and connector-freshness metadata. authorizationHeaderPresent reports whether the request carried an Authorization header (never its value). credentialTransport reports how the calling request presented its credential (header, path or none), never the credential itself.", inputSchema: { type: "object", properties: {} } },
];

TOOLS.push(...CANONICAL_ADDITIONS);

/**
 * Each capped action's ticker array branch advertises its tool_catalog.json
 * tickerCaps entry as maxItems (2.12.0). get_market_snapshot advertises its
 * compact cap; the smaller full-mode cap is enforced by the tool-call gate.
 */
function applyTickerCaps(tools: Tool[]): void {
  for (const [key, cap] of Object.entries(TICKER_CAPS)) {
    if (key.includes(".")) continue;
    const tool = tools.find((candidate) => candidate.name === key);
    const branches = (tool?.inputSchema.properties.ticker as { oneOf?: Array<Record<string, unknown>> } | undefined)?.oneOf;
    const arrayBranch = branches?.find((branch) => branch.type === "array");
    if (!arrayBranch) throw new Error(`tickerCaps names '${key}', which has no ticker array schema`);
    arrayBranch.maxItems = cap;
  }
}
applyTickerCaps(TOOLS);

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
    authorizationHeaderPresent: { type: ["boolean", "null"] },
    credentialTransport: { type: ["string", "null"], enum: ["header", "path", "none", null] },
  },
  additionalProperties: false,
};
export const ENVELOPE_V2_OUTPUT_SCHEMA: Tool["outputSchema"] = {
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
      downgrades: { type: ["number", "null"] },
      initiations: { type: ["number", "null"] },
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
      invertedFlag: { type: ["boolean", "null"] },
      priceAtOrAboveTarget: { type: ["boolean", "null"], description: "currentPrice >= referenceTargetPrice on the unrounded prices; null when either price is missing or not positive." },
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
      analystInputs: { type: "object", description: "Analyst sentiment, rating-change counts, dominant rating and analyst count." },
      pricePositionInputs: { type: "object", description: "Current price and its position in the 52-week range." },
      earningsInputs: { type: "object", description: "Earnings beat rate, beat streak, average surprise and momentum." },
      technicalInputs: { type: "object", description: "RSI, MACD histogram, moving-average position and last close." },
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
      fxCurrency: { type: ["string", "null"] },
      meetsMinimum: { type: ["boolean", "null"], description: "adv20dTradedValueUsd >= min_adv_usd; null when min_adv_usd is not given or the average is unknown." },
      minAdvUsd: { type: ["number", "null"], description: "The caller's min_adv_usd, or null." },
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
      volume: {
        type: "object",
        properties: {
          meetsMinimum: { type: ["boolean", "null"], description: "20-day average traded value (USD) >= min_adv_usd; null when min_adv_usd is not given or the average is unknown." },
          minAdvUsd: { type: ["number", "null"], description: "The caller's min_adv_usd, or null." },
        },
      },
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
  get_share_count_scenarios: SIMPLE_OBJECT_SCHEMA,
  extract_funding_capex_schedule: SIMPLE_OBJECT_SCHEMA,
  extract_operating_driver_ledger: SIMPLE_OBJECT_SCHEMA,
  get_guidance_history: SIMPLE_OBJECT_SCHEMA,
  get_historical_valuation_context: SIMPLE_OBJECT_SCHEMA,
  reconcile_metric_sources: SIMPLE_OBJECT_SCHEMA,
  get_consensus_forecast_curve: SIMPLE_OBJECT_SCHEMA,
  get_eps_revisions: SIMPLE_OBJECT_SCHEMA,
  get_evidence_quality: SIMPLE_OBJECT_SCHEMA,
  build_valuation_evidence_pack: SIMPLE_OBJECT_SCHEMA,
  get_evidence_cut: SIMPLE_OBJECT_SCHEMA,
  list_evidence_cuts: SIMPLE_OBJECT_SCHEMA,
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
OUTPUT_SCHEMAS.calculate_price_target_distance = OUTPUT_SCHEMAS.get_price_target_bracket;
OUTPUT_SCHEMAS.analyze_position_signals = OUTPUT_SCHEMAS.get_position_score_inputs;
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

export const LLM_DETAILED_OUTPUT_TOOLS = new Set([
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

export function envelopedToolOutputSchema(dataSchema: Tool["outputSchema"]): Tool["outputSchema"] {
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

export const GROUPED_TOOLS: Tool[] = GROUPED_TOOL_DEFS.map((group) => ({
  name: group.name,
  description: group.description,
  inputSchema: groupedInputSchema(group.name, group.actions),
  outputSchema: ENVELOPE_V2_OUTPUT_SCHEMA,
  annotations: annotationsForTool(group.name),
}));

export const GROUPED_ACTIONS = new Map(
  GROUPED_TOOL_DEFS.map((group) => [group.name, new Set(Object.keys(group.actions))])
);
