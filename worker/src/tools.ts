import { mcpSuccess, mcpFailure, ErrorCode, getWorkerVar } from "./response.js";
import { GROUPED_TOOL_DEFS } from "./tool-catalog.js";
import {
  getAnalystConsensus,
  getAnalystUpgradeRadar,
  getCalendar,
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
  getHolderInfo,
  getMaPosition,
  getOptionChain,
  getOptionExpirationDates,
  getOvernightQuote,
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
  listSecFilings,
  listSecCompanyFilings,
  getFilingOutline,
  getFilingSection,
  listFilingTables,
  getFilingTable,
  extractFilingFact,
  indexSecFiling,
  getSecFilingIndex,
  searchCompanyNews,
  getCompanyPressReleases,
  getSecRecentEvents,
  getPublicEventTimeline,
  verifyCompanyEvent,
  extractRiskFactorMentions,
  extractCustomerConcentration,
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
} from "./yahoo-finance.js";
import { validateTicker } from "./validate.js";

export interface Tool {
  name: string;
  description: string;
  inputSchema: {
    type: "object";
    properties: Record<string, unknown>;
    required?: string[];
  };
  outputSchema?: {
    type: "object";
    properties: Record<string, unknown>;
    required?: string[];
    additionalProperties?: boolean;
  };
  deprecated?: boolean;
  useInstead?: string;
  deprecationReason?: string;
}

export const TOOLS: Tool[] = [
  {
    name: "get_historical_stock_prices",
    description:
      "Get historical stock prices for a given ticker symbol. Returns camelCase fields: date, open, high, low, close, volume, and adjClose.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
        period: {
          type: "string",
          description: "Valid periods: 1d | 5d | 1mo | 3mo | 6mo | 1y | 2y | 5y | 10y | ytd | max",
          default: "1mo",
        },
        interval: {
          type: "string",
          description:
            "Valid intervals: 1m | 2m | 5m | 15m | 30m | 60m | 90m | 1h | 1d | 5d | 1wk | 1mo | 3mo. Intraday data cannot extend past 60 days.",
          default: "1d",
        },
        prepost: {
          type: "boolean",
          description:
            "If true, includes pre-market and after-hours rows. Only meaningful with intraday intervals (1m–90m) and period ≤ 60d. Default false.",
          default: false,
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_stock_info",
    description:
      "Get stock fundamentals for one or more tickers. Returns ~30 key fields by default: identity (shortName, sector, industry, country), price (currentPrice, previousClose, marketCap, enterpriseValue), valuation (trailingPE, forwardPE, priceToBook, EV/EBITDA), earnings (EPS, revenueGrowth), margins (gross/operating/profit, ROE, ROA), dividends, analyst ratings, and longBusinessSummary. Pass include_all: true to get the full 120+ field payload. Pass an array of symbols to fetch multiple tickers in one call — returns a dict keyed by symbol. Max 5 tickers per call; if you need more, split into multiple calls. For ETFs or mutual funds, use get_etf_info instead.",
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
        include_all: {
          type: "boolean",
          description: "Set to true to return the full 120+ field payload. Defaults to false (returns ~30 key fields).",
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_etf_info",
    description:
      "Get ETF or mutual fund data for one or more tickers. Returns identity (shortName, category, fundFamily, legalType, fundInceptionDate), pricing (navPrice, previousClose, open, dayHigh, dayLow, volume, averageVolume), AUM/costs (totalAssets, yield, annualReportExpenseRatio, ytdReturn, beta3Year), 52-week stats, moving averages, top-10 holdings (topHoldings), sector weights (sectorWeights), and recent annual returns. Use for ETF/fund tickers: SPY, QQQ, VTI, ARKK, VFIAX, etc. Max 5 tickers per call; split larger lists into multiple calls.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: {
          description: "ETF or fund ticker symbol (e.g. 'SPY') or an array of up to 5 symbols (e.g. ['SPY', 'QQQ']). If more than 5 are provided, only the first 5 are processed — split larger lists into multiple calls.",
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
    name: "get_yahoo_finance_news",
    description: "Deprecated alias for get_company_news.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_stock_actions",
    description: "Get dividend payment history and stock split history for a ticker.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_financial_statement",
    description:
      "Get a financial statement for a ticker. Choose from: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cashflow, quarterly_cashflow.",
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
          ],
        },
      },
      required: ["ticker", "financial_type"],
    },
  },
  {
    name: "get_holder_info",
    description:
      "Get shareholder data for a ticker. Choose from: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
        holder_type: {
          type: "string",
          description: "The type of holder information to retrieve.",
          enum: [
            "major_holders",
            "institutional_holders",
            "mutualfund_holders",
            "insider_transactions",
            "insider_purchases",
            "insider_roster_holders",
          ],
        },
      },
      required: ["ticker", "holder_type"],
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
    name: "get_recommendations",
    description:
      "Get analyst recommendations or upgrade/downgrade history for a ticker. For upgrades_downgrades, specify months_back (default 12).",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
        recommendation_type: {
          type: "string",
          description: "The type of recommendation data to retrieve.",
          enum: ["recommendations", "upgrades_downgrades"],
        },
        months_back: {
          type: "number",
          description: "Number of months of upgrade/downgrade history to return (default: 12).",
          default: 12,
        },
      },
      required: ["ticker", "recommendation_type"],
    },
  },
  {
    name: "get_fast_info",
    description:
      "Alias for get_market_quote. Get lightweight real-time price and market data for one or more tickers. Returns high-signal fields: currency, exchange, quoteType, lastPrice, open, previousClose, dayHigh, dayLow, yearHigh, yearLow, yearChange, marketCap, shares, lastVolume, tenDayAverageVolume, threeMonthAverageVolume, fiftyDayAverage, twoHundredDayAverage, preMarketPrice, postMarketPrice, marketOpen (true only during regular session hours), lastTradeDate (YYYY-MM-DD date of the session the OHLCV data belongs to — use this to detect weekend/holiday staleness), postMarketTimestamp (ISO8601 timestamp of postMarketPrice, null if no AH activity). Prefer this over get_stock_info for price/market data queries — it uses far fewer tokens. Max 5 tickers per call; if you need more, split into multiple calls.",
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
    name: "get_price_stats",
    description:
      "Get pre-computed price statistics for one or more tickers: current price, % change vs previous close, % distance from 52-week high/low and 50/200-day moving averages, 30-day annualized volatility, and CAGR over 1y/3y/5y. Max 5 tickers per call; if you need more, split into multiple calls.",
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
    name: "get_analyst_consensus",
    description:
      "Get analyst consensus summary for one or more tickers: price targets (current, low, high, mean, median) with % upside from last price, recommendation breakdown (strongBuy, buy, hold, sell, strongSell counts), and dominant rating. Max 5 tickers per call; if you need more, split into multiple calls.",
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
      "Get all analyst forward-looking data in one call: EPS estimates, revenue estimates, EPS trend (7/30/60/90-day revisions), earnings history (actual vs estimated EPS and surprise %), and growth estimates.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_financial_ratios",
    description:
      "Get pre-computed key financial ratios for one or more tickers. Includes: P/E (trailing & forward), P/S, P/B, EV/EBITDA, EV/Revenue, PEG; gross/operating/net margins, ROE, ROA; debt/equity, current ratio, quick ratio; FCF and FCF yield; dividend yield and payout ratio. Max 5 tickers per call; if you need more, split into multiple calls.",
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
    name: "get_calendar",
    description:
      "Get upcoming earnings and dividend schedule for a ticker: next earnings date range, EPS/revenue consensus estimates, ex-dividend date, and dividend pay date. Also returns earningsDateConfirmed (true = single fixed date from IR, false = analyst estimate range) and earningsDateSource ('IR_FILING' | 'ESTIMATE' | 'UNKNOWN'). Use earningsDateConfirmed to distinguish a confirmed single date (from IR/filing) vs an estimated date range.",
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
      "Get pre-computed technical / momentum indicators for one or more tickers. Computes indicators server-side from historical daily close prices so the LLM does NOT need to fetch raw OHLCV history and calculate manually. Returns: rsi14 (14-day RSI, Wilder smoothing; below 30 = oversold, above 70 = overbought), macd (MACD line: 12-day EMA minus 26-day EMA), macdSignal (9-day EMA of MACD), macdHistogram (MACD minus signal; positive = bullish momentum), lastClose, and dataDate. Max 5 tickers per call; split larger lists into multiple calls.",
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
            "Lookback period for fetching history (default '3mo'). Longer periods give more accurate indicator warm-up. Valid: 1mo, 3mo, 6mo, 1y, 2y, 5y.",
          default: "3mo",
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_price_slope",
    description:
      "Get N-day price slope (% change) and direction for one or more tickers. Returns startClose, endClose, slopePct, direction (UP/DOWN/FLAT). Max 5 tickers per call.",
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
          type: "number",
          description: "Lookback window in trading days (default: 5).",
          default: 5,
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_volume_ratio",
    description:
      "Get last-session volume vs N-day average volume ratio. Returns ratio10d, ratio90d, volumeFlag (HIGH/NORMAL/LOW). Max 5 tickers per call.",
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
          type: "number",
          description: "Averaging period in days (default: 10).",
          default: 10,
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_ma_position",
    description:
      "Get price position vs 50DMA and 200DMA with trend classification (BULLISH/BEARISH/MIXED). Max 5 tickers per call.",
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
  {
    name: "get_credit_health",
    description:
      "Get pre-computed credit/leverage metrics using operational EBITDA (EBIT plus depreciation/amortization when available), EBIT and EBITDA interest coverage, debt tier, credit stress flag, and source fields. Max 5 tickers per call; split larger lists into multiple calls.",
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
  {
    name: "get_short_momentum",
    description:
      "Get short interest with MoM delta, direction (RISING/FALLING/FLAT), squeeze risk (HIGH/MODERATE/LOW), and flag. Max 5 tickers per call; split larger lists into multiple calls.",
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
  {
    name: "get_earnings_momentum",
    description:
      "Deprecated alias for analyze_earnings_momentum. Get earnings revision momentum, beat rate, and estimate direction signals. Returns revision7d/30d/90d, momentumFlag, beatRate, currentBeatStreak. Max 5 tickers per call; split larger lists into multiple calls.",
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
  {
    name: "get_options_flow_summary",
    description:
      "Get options flow summary: P/C ratio, IV percentile, max pain strike, highest OI strikes for nearest liquid expiry. Single ticker only.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
        expiry_hint: {
          type: "string",
          description: "Optional YYYY-MM-DD expiry date. If omitted, selects nearest liquid expiry.",
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_put_hedge_candidates",
    description:
      "Get pre-filtered OTM put options within a strike range and budget with feasibility pre-computed. Single ticker only.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
        otm_pct_min: { type: "number", description: "Minimum OTM % (default: 8).", default: 8 },
        otm_pct_max: { type: "number", description: "Maximum OTM % (default: 12).", default: 12 },
        budget_usd: { type: "number", description: "Max premium per contract in USD (default: 500).", default: 500 },
        expiry_after: { type: "string", description: "YYYY-MM-DD minimum expiry date.", default: "" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_analyst_upgrade_radar",
    description:
      "Get recent analyst rating changes with canonical signal classification (UPGRADE/DOWNGRADE/INITIATED/MAINTAIN), separate upgrade/downgrade/initiation counts, netSentiment, and summary. Returns ptFrom, ptTo (null — price target data not exposed by yfinance), and ptDirection (RAISE/CUT/UNCHANGED/INITIATED/null). Max 5 tickers per call.",
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
        days_back: {
          type: "number",
          description: "Lookback window in calendar days (default: 30).",
          default: 30,
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_overnight_quote",
    description:
      "Deprecated diagnostics-only Yahoo extended-hours proxy. This does not provide true 20:00-04:00 ET overnight venue data. Returns provider, providerStatus, dataKind, decisionGrade, warnings, requestedFeed, overnightPrice, overnightTime, overnightHigh, overnightLow, overnightOpen, overnightVolume, previousClose, gapPct, gapDirection, dataSource, isBlueOceanWindow, isStale, dataAgeHours, fallback, and note.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Ticker symbol, e.g. 'BTC-USD'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_filing_data",
    description:
      "Retrieve structured XBRL-tagged financial facts from EDGAR. Try this tool before search_filing_text for GAAP line items and geographic revenue.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Ticker symbol, e.g. 'GLW'" },
        fact_type: {
          type: "string",
          enum: [
            "geographic_revenue",
            "segment_revenue",
            "capex",
            "rd_expense",
            "operating_income",
            "net_income",
            "total_revenue",
            "long_term_debt",
            "cash",
          ],
          description: "Fact type to retrieve from EDGAR companyconcept.",
        },
        region: {
          type: "string",
          description: "Required for fact_type='geographic_revenue'.",
        },
        filing_type: {
          type: "string",
          enum: ["10-K", "10-Q"],
          default: "10-K",
        },
        period: {
          type: "string",
          enum: ["latest", "all"],
          default: "latest",
        },
        period_mode: {
          type: "string",
          enum: ["auto", "quarter", "ytd", "annual"],
          default: "auto",
          description:
            "Filter XBRL facts by duration. 'auto' selects quarter for 10-Q, annual for 10-K. Use 'quarter' to avoid YTD figures.",
        },
      },
      required: ["ticker", "fact_type"],
    },
  },
  {
    name: "search_filing_text",
    description:
      "Full-text search or section retrieval from SEC filing HTML. Use only when get_filing_data returns NOT_DISCLOSED or the fact is not XBRL-tagged.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Ticker symbol, e.g. 'GLW'" },
        search_terms: {
          type: "array",
          items: { type: "string" },
          description: "Keywords to search for in filing text.",
        },
        section_hint: {
          type: "string",
          description: "Optional section/heading hint.",
        },
        filing_type: {
          type: "string",
          enum: ["10-K", "10-Q", "8-K"],
          default: "10-K",
        },
        accession_number: {
          type: "string",
          description: "Optional accession number; if omitted latest filing is selected from submissions.",
        },
        context_chars: {
          type: "number",
          default: 1500,
        },
        return_tables: {
          type: "boolean",
          default: true,
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_options_flow_scan",
    description:
      "Structured options flow scan for a binary event window. Returns pcRatio, ivPctile, putVolVs10dAvg, putVolTrend (INCREASING/STABLE/DECREASING), maxPainStrike, bracket (UPPER/MID/LOWER), formattedBlock (paste directly into session output), dataDate. Prior window-label readings cached server-side 72h for trend computation (e.g. T-14 → T-7 → T-2).",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'ASTS'" },
        window_label: {
          type: "string",
          description: "Free-form label for this reading, e.g. 'T-14', 'T-7', 'T-2', 'pre-earnings', 'week1'. Used as cache key for trend computation across readings.",
        },
      },
      required: ["ticker", "window_label"],
    },
  },
  {
    name: "get_price_target_bracket",
    description:
      "Compare current market price to a user-supplied reference price target and return percentage distance and bracket labels. Preferred input is reference_target_price; io_pt is accepted as a backward-compatible alias.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'ASTS'" },
        reference_target_price: { type: "number", description: "Preferred user-supplied reference target price." },
        io_pt: { type: "number", description: "Backward-compatible alias for reference_target_price." },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_position_score_inputs",
    description:
      "Aggregate public market, analyst, earnings, and technical inputs that may be useful for a caller-defined scoring model. This tool does not access holdings, cost basis, position size, or private scoring rules.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'ASTS'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_volume_gate",
    description:
      "Deprecated alias for check_volume_liquidity_threshold. Check current trading volume and dollar-notional liquidity against configurable public liquidity thresholds.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'ASTS'" },
        foreign_exchange: {
          type: "boolean",
          description: "Set true for foreign exchange / ADR tickers to convert daily notional to USD for the $10M threshold check. Default false.",
          default: false,
        },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_options_summary",
    description: "Get options summary for a single ticker: ATM implied volatility, put/call ratio by volume and OI, max pain strike for the nearest or requested expiry. Preferred for data-source use because it returns a compact snapshot without the full contract list.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
        expiry_hint: { type: "string", description: "Optional YYYY-MM-DD expiry. Must be one of get_option_expiration_dates." },
      },
      required: ["ticker"],
    },
  },
  {
    name: "list_sec_filings",
    description: "List recent SEC filings for a ticker from EDGAR. Returns accession number, filing date, form type, primary document URL, and EDGAR index URL.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string" },
        form_type: { type: "string", enum: ["10-K", "10-Q", "8-K", "DEF 14A"], default: "10-K" },
        max_filings: { type: "number", default: 5 },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_filing_outline",
    description: "Parse the document outline of an SEC filing. Returns a hierarchical tree of Parts, Items, and Notes.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string" },
        accession_number: { type: "string" },
        document_url: { type: "string" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_filing_section",
    description: "Retrieve the text content of a specific section from an SEC filing document.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string" },
        section_name: { type: "string" },
        document_url: { type: "string" },
        context_chars: { type: "number", default: 3000 },
      },
      required: ["ticker", "section_name", "document_url"],
    },
  },
  {
    name: "list_filing_tables",
    description: "List all HTML tables in an SEC filing document. Returns table index, headers, and row count.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string" },
        document_url: { type: "string" },
      },
      required: ["ticker", "document_url"],
    },
  },
  {
    name: "get_filing_table",
    description: "Get the parsed rows of a specific table from an SEC filing document.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string" },
        document_url: { type: "string" },
        table_index: { type: "number" },
        max_rows: { type: "number", default: 30 },
      },
      required: ["ticker", "document_url", "table_index"],
    },
  },
  {
    name: "extract_filing_fact",
    description: "Extract a specific financial fact from an SEC filing. Uses XBRL first, parsed tables second, text search last.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string" },
        fact_name: { type: "string" },
        document_url: { type: "string" },
        accession_number: { type: "string" },
      },
      required: ["ticker", "fact_name"],
    },
  },
];

export const TOOL_ALIASES: Record<string, string> = {
  get_fast_info: "get_market_quote",
  get_historical_stock_prices: "get_historical_prices",
  get_stock_info: "get_company_profile",
  get_etf_info: "get_fund_profile",
  get_stock_actions: "get_corporate_actions",
  get_holder_info: "get_ownership_holders",

  get_price_stats: "analyze_price_performance",
  get_ma_position: "analyze_moving_average_position",
  get_volume_ratio: "analyze_volume_ratio",
  get_volume_gate: "check_volume_liquidity_threshold",

  get_financial_ratios: "analyze_financial_ratios",
  get_credit_health: "analyze_credit_health",

  get_recommendations: "get_analyst_recommendations",
  get_analyst_upgrade_radar: "get_analyst_rating_changes",
  get_earnings_momentum: "analyze_earnings_momentum",
  get_calendar: "get_company_events_calendar",
  get_yahoo_finance_news: "get_company_news",

  get_options_flow_summary: "summarize_options_flow",
  get_options_summary: "summarize_options_flow",
  get_options_flow_scan: "analyze_options_flow_window",
  get_put_hedge_candidates: "find_put_hedge_candidates",

  get_price_target_bracket: "calculate_price_target_distance",
  get_position_score_inputs: "analyze_position_signals",

  list_sec_filings: "list_sec_company_filings",
  get_filing_outline: "get_sec_filing_outline",
  get_filing_section: "get_sec_filing_section",
  list_filing_tables: "list_sec_filing_tables",
  get_filing_table: "get_sec_filing_table",

  get_filing_data: "extract_sec_filing_fact",
  extract_filing_fact: "extract_sec_filing_fact",

  search_filing_text: "search_sec_filing_text",
};

const CANONICAL_ADDITIONS: Tool[] = [
  { name: "get_market_quote", description: "Get market quote for one or more tickers.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "get_historical_prices", description: "Get historical prices for a ticker.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", default: "1mo" }, interval: { type: "string", default: "1d" }, prepost: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "analyze_price_performance", description: "Analyze price performance metrics.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "analyze_moving_average_position", description: "Analyze moving-average position.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "analyze_volume_ratio", description: "Analyze volume ratio signals.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, period: { type: "number", default: 10 } }, required: ["ticker"] } },
  { name: "check_volume_liquidity_threshold", description: "Check current trading volume and dollar-notional liquidity against configurable public liquidity thresholds.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, foreign_exchange: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "get_company_profile", description: "Get company profile/fundamentals.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, include_all: { type: "boolean" } }, required: ["ticker"] } },
  { name: "get_fund_profile", description: "Get ETF/fund profile.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "analyze_financial_ratios", description: "Analyze financial ratios.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "analyze_credit_health", description: "Analyze credit health metrics.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "get_corporate_actions", description: "Get corporate actions.", inputSchema: { type: "object", properties: { ticker: { type: "string" } }, required: ["ticker"] } },
  { name: "get_ownership_holders", description: "Get ownership/holder data.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, holder_type: { type: "string" } }, required: ["ticker", "holder_type"] } },
  { name: "get_analyst_recommendations", description: "Get analyst recommendations and changes.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, recommendation_type: { type: "string" }, months_back: { type: "number", default: 12 } }, required: ["ticker", "recommendation_type"] } },
  { name: "get_analyst_rating_changes", description: "Get analyst rating changes radar.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, days_back: { type: "number", default: 30 } }, required: ["ticker"] } },
  { name: "analyze_earnings_momentum", description: "Analyze earnings momentum.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] } }, required: ["ticker"] } },
  { name: "get_company_events_calendar", description: "Get upcoming earnings and dividend schedule for a ticker, including whether the earnings date appears confirmed by company filing/IR source or is an estimate.", inputSchema: { type: "object", properties: { ticker: { type: "string" } }, required: ["ticker"] } },
  { name: "summarize_options_flow", description: "Summarize options flow.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, expiry_hint: { type: "string" } }, required: ["ticker"] } },
  { name: "analyze_options_flow_window", description: "Analyze options flow in an event window.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, window_label: { type: "string" } }, required: ["ticker", "window_label"] } },
  { name: "find_put_hedge_candidates", description: "Find put hedge candidates.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, otm_pct_min: { type: "number", default: 8 }, otm_pct_max: { type: "number", default: 12 }, budget_usd: { type: "number", default: 500 }, expiry_after: { type: "string" } }, required: ["ticker"] } },
  { name: "list_sec_company_filings", description: "List SEC filings for a company from EDGAR submissions. Returns cik, filingType, filingDate, acceptedAt, accessionNumber, primaryDocument, documentUrl, and meta.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, form_type: { type: "string", default: "10-K" }, limit: { type: "number", default: 5 }, max_filings: { type: "number", default: 5 } }, required: ["ticker"] } },
  { name: "get_sec_filing_outline", description: "Get SEC filing outline.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, accession_number: { type: "string" }, document_url: { type: "string" } }, required: ["ticker"] } },
  { name: "get_sec_filing_section", description: "Get SEC filing section text.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, selector: { type: "object" }, section_name: { type: "string" }, document_url: { type: "string" }, context_chars: { type: "number", default: 3000 } }, required: ["ticker"] } },
  { name: "list_sec_filing_tables", description: "List SEC filing tables.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, document_url: { type: "string" }, offset: { type: "number", default: 0 }, limit: { type: "number", default: 50 } }, required: ["ticker"] } },
  { name: "get_sec_filing_table", description: "Get SEC filing table.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, document_url: { type: "string" }, table_index: { type: "number" }, max_rows: { type: "number", default: 30 } }, required: ["ticker", "table_index"] } },
  { name: "extract_sec_filing_fact", description: "Extract SEC filing fact.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, fact: { type: "string" }, fact_name: { type: "string" }, fact_type: { type: "string" }, region: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, document_url: { type: "string" }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "search_sec_filing_text", description: "Search SEC filing text.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, search_terms: { type: "array", items: { type: "string" } }, search_query: { type: "string" }, section_hint: { type: "string" }, selector: { type: "object" }, filing_type: { type: "string", default: "10-K" }, accession_number: { type: "string" }, document_url: { type: "string" }, context_chars: { type: "number", default: 1500 }, return_tables: { type: "boolean", default: true } }, required: ["ticker"] } },
  { name: "index_sec_filing", description: "Build a deterministic section/table index for an SEC filing. Identifies headings, tables, row labels, and units. period is reserved for future multi-period support; currently only 'latest' is supported unless accession_number is provided.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "Reserved. Only 'latest' supported currently." }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "get_sec_filing_index", description: "Get the pre-built section/table index for an SEC filing. Returns cached index when available; builds and caches on first call. period is reserved for future multi-period support; currently only 'latest' is supported unless accession_number is provided.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "Reserved. Only 'latest' supported currently." }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "list_sec_material_filings", description: "List latest material SEC filings for a ticker, filtering out noise (Form 4, 144, SC 13G, etc.). Returns only significant filings (10-K, 10-Q, 8-K, S-1, 424B, DEF 14A, 20-F, 6-K by default).", inputSchema: { type: "object", properties: { ticker: { type: "string" }, forms: { type: "array", items: { type: "string" }, default: ["10-K", "10-Q", "8-K", "S-1", "424B", "DEF 14A", "20-F", "6-K"] }, limit: { type: "number", default: 5 } }, required: ["ticker"] } },
  { name: "get_sec_filing_intelligence", description: "Get a comprehensive intelligence map of a company's SEC filing — XBRL facts snapshot, section/table index summary, and recommended queries — in a single call.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, filing_index: { type: "number", default: 0 } }, required: ["ticker"] } },
  { name: "get_sec_filing_section_markdown", description: "Return a specific SEC filing section as LLM-ready Markdown. Converts filing HTML to clean Markdown with preserved section headers and pipe-delimited tables.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, section: { type: "string", default: "Item 1A" }, filing_type: { type: "string", default: "10-K" }, filing_index: { type: "number", default: 0 }, max_chars: { type: "number", default: 50000 } }, required: ["ticker"] } },
  { name: "analyze_position_signals", description: "Aggregate public market, analyst, earnings, and technical inputs that may be useful for a caller-defined scoring model. This tool does not access holdings, cost basis, position size, or private scoring rules.", inputSchema: { type: "object", properties: { ticker: { type: "string" } }, required: ["ticker"] } },
  { name: "calculate_price_target_distance", description: "Compare current market price to a user-supplied reference price target and return percentage distance and bracket labels.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, reference_target_price: { type: "number", description: "Preferred: user-supplied reference target price." }, io_pt: { type: "number", description: "Backward-compatible alias for reference_target_price." } }, required: ["ticker"] } },
  { name: "get_company_news", description: "Get recent public company news and press releases from selected public sources with precise source labels (yahoo_finance_news, yahoo_finance_press_releases, finnhub), timestamps, URL, dedupe metadata, and short evidence text. Accepts a single ticker or an array of up to 5 symbols; for an array, results are returned as a per-ticker keyed object (each ticker fetched independently).", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }], description: "Ticker symbol (e.g. 'AAPL') or an array of up to 5 symbols (e.g. ['AAPL', 'MSFT']). If more than 5 are provided, only the first 5 are processed; split larger lists into multiple calls." }, max_results: { type: "number", default: 10 }, lookback_days: { type: "number", default: 14 }, sources: { type: "array", items: { type: "string" }, default: ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"] } }, required: ["ticker"] } },
  { name: "search_company_news", description: "Search public company news/events for a ticker and query across allowed source metadata and short snippets only.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Ticker symbol, e.g. 'AAPL'" }, query: { type: "string", description: "Required search query string." }, start_date: { type: "string", default: "" }, end_date: { type: "string", default: "" }, sources: { type: "array", items: { type: "string" }, default: ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"] }, max_results: { type: "number", default: 10 } }, required: ["ticker", "query"] } },
  { name: "get_company_press_releases", description: "Get company press releases and official release-style events. Returns Yahoo Finance press releases (yahoo_finance_press_releases) as a first-class source alongside SEC 8-K filings, company IR, and newswire content.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, lookback_days: { type: "number", default: 90 }, max_results: { type: "number", default: 20 }, sources: { type: "array", items: { type: "string" }, default: ["yahoo_finance_press_releases", "company_ir", "newswire", "sec"] } }, required: ["ticker"] } },
  { name: "get_sec_recent_events", description: "Get recent SEC filing events with filing type, filing date, accepted timestamp, accession number, SEC archive URL, and event metadata.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_types: { type: "array", items: { type: "string" }, default: ["8-K", "10-Q", "10-K"] }, lookback_days: { type: "number", default: 90 }, max_results: { type: "number", default: 20 } }, required: ["ticker"] } },
  { name: "get_public_event_timeline", description: "Get a deduplicated chronological timeline of public company events across selected public sources.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, start_date: { type: "string", default: "" }, end_date: { type: "string", default: "" }, sources: { type: "array", items: { type: "string" }, default: ["sec", "company_ir", "newswire", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"] }, max_results: { type: "number", default: 50 }, newest_first: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "verify_company_event", description: "Verify whether a public company event is source-backed, returning CONFIRMED, PARTIAL, NOT_FOUND, STALE, or CONFLICTING with best evidence.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, event_query: { type: "string", description: "Keywords describing the event to verify." }, start_date: { type: "string", default: "" }, end_date: { type: "string", default: "" }, sources: { type: "array", items: { type: "string" }, default: ["sec", "company_ir", "newswire", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"] } }, required: ["ticker", "event_query"] } },
  { name: "extract_geographic_revenue", description: "Extract geographic revenue exposure with compact evidence-backed output.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, region: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, accession_number: { type: "string" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker", "region"] } },
  { name: "extract_segment_revenue", description: "Extract segment revenue rows from SEC filing facts.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker"] } },
  { name: "extract_total_revenue", description: "Extract total revenue from SEC filing facts.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" } }, required: ["ticker"] } },
  { name: "extract_revenue_exposure", description: "Extract revenue exposure for a region/customer/segment query.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, exposure_query: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker", "exposure_query"] } },
  { name: "extract_china_exposure", description: "Extract China exposure with separate revenue and non-revenue classifications.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, accession_number: { type: "string" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker"] } },
  { name: "extract_risk_factor_mentions", description: "Extract concise risk-factor term mentions from SEC filings.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, terms: { type: "array", items: { type: "string" } }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker", "terms"] } },
  { name: "extract_customer_concentration", description: "Extract customer concentration percentages from SEC filings.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, detailLevel: { type: "string", default: "compact" } }, required: ["ticker"] } },
  { name: "extract_exposure", description: "Extract multi-dimensional exposure for any geographic region or named entity/topic from the latest SEC 10-K (or 20-F) filing. Returns revenue exposure (XBRL + HTML fallback), operational evidence, named-entity mentions, and risk-factor excerpts in one call. Replaces extract_geographic_revenue, extract_china_exposure, and extract_revenue_exposure.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Ticker symbol, e.g. 'AAPL'" }, topic: { type: "string", description: "Geographic region or entity to search for, e.g. 'china', 'russia', 'europe', 'huawei'. Case-insensitive." }, filing_type: { type: "string", default: "10-K", description: "SEC filing type: '10-K' or '20-F'." }, period: { type: "string", default: "latest" }, include_risk_factors: { type: "boolean", default: true } }, required: ["ticker", "topic"] } },
  { name: "query_sec_filing_index", description: "Deterministically route supported SEC filing query types to index-backed extractor tools.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, accession_number: { type: "string" }, query_type: { type: "string", enum: ["geographic_revenue_share", "revenue_exposure", "china_exposure", "risk_factor_mentions", "customer_concentration", "total_revenue", "segment_revenue"] }, params: { type: "object", default: {} }, return_evidence: { type: "boolean", default: true }, detailLevel: { type: "string", default: "compact", enum: ["compact", "evidence", "raw"] } }, required: ["ticker", "query_type"] } },
  { name: "get_latest_earnings_release", description: "Resolve the latest public earnings release source for a ticker. Returns SEC 8-K URL with HIGH confidence, or Yahoo calendar estimate with MEDIUM confidence.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" }, period: { type: "string", enum: ["latest"], default: "latest", description: "Period selector. Only 'latest' is supported." } }, required: ["ticker"] } },
  { name: "index_earnings_release", description: "Build a compact section/table index of the latest public earnings release for deterministic metric extraction.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, period: { type: "string", enum: ["latest"], default: "latest" }, source_url: { type: "string", description: "Optional override URL (must be https://www.sec.gov/Archives/ or company IR). Paywalled sources are blocked." } }, required: ["ticker"] } },
  { name: "extract_earnings_metrics", description: "Extract reported earnings metrics (revenue, EPS diluted, gross margin, operating income, FCF, capex) from SEC 8-K or public IR source.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" }, source_preference: { type: "array", items: { type: "string", enum: ["sec_8k", "company_ir", "10-q", "yahoo"] }, description: "Ordered preference list for source resolution.", default: ["sec_8k", "company_ir", "10-q", "yahoo"] } }, required: ["ticker"] } },
  { name: "extract_guidance", description: "Extract company-provided forward guidance ranges (revenue, gross margin, EPS) from the latest SEC 8-K or IR release.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" } }, required: ["ticker"] } },
  { name: "extract_management_commentary", description: "Extract topic-keyed management commentary snippets from the latest earnings release. Returns first relevant sentence per topic.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" }, topics: { type: "array", items: { type: "string" }, description: "Topics to search for, e.g. ['AI', 'margins', 'guidance', 'supply chain']" } }, required: ["ticker"] } },
  { name: "compare_earnings_actual_vs_estimate", description: "Compare the latest reported quarter with non-null actual EPS against Yahoo's historical estimate for that same reported quarter/date. Returns reportedPeriod, reportedDate, actual, estimate, surprise, confidence, and warnings.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", enum: ["latest"], default: "latest" } }, required: ["ticker"] } },
  { name: "list_sec_filing_exhibits", description: "List all exhibits/documents attached to a specific SEC filing by accession number.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, accessionNumber: { type: "string", description: "SEC filing accession number, e.g. '0000320193-24-000081'" } }, required: ["ticker", "accessionNumber"] } },
  { name: "get_sec_filing_exhibit_content", description: "Fetch and return the text content of a specific exhibit from an SEC filing. Supports topic-based paragraph filtering to reduce token usage.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, accessionNumber: { type: "string", description: "SEC filing accession number" }, fileName: { type: "string", description: "Exhibit filename from the filing index" }, topics: { type: "array", items: { type: "string" }, description: "Optional list of keywords/topics to filter paragraphs by" } }, required: ["ticker", "accessionNumber", "fileName"] } },
  { name: "parse_public_transcript", description: "Fetch and parse a public transcript page (Motley Fool, company IR, etc.). Supports topic-based paragraph filtering to reduce token usage. Provide raw_text to skip URL fetching.", inputSchema: { type: "object", properties: { url: { type: "string", description: "Public https URL of the transcript page" }, topics: { type: "array", items: { type: "string" }, description: "Optional list of keywords/topics to filter paragraphs by" }, raw_text: { type: "string", description: "Raw HTML or text content to parse directly (bypasses URL fetching)" } } } },
  { name: "get_earnings_call_transcript", description: "High-level tool to retrieve earnings call transcript content from SEC 8-K exhibits, then return structured fallback metadata for company IR, public transcript URLs, and optional Alpha Vantage.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Stock ticker symbol" }, period: { type: "string", enum: ["latest"], default: "latest", description: "Period selector. Only 'latest' is supported." }, topics: { type: "array", items: { type: "string" }, description: "Optional list of keywords/topics to filter paragraphs by" } }, required: ["ticker"] } },
  { name: "get_manifest_diagnostics", description: "Return deployment and manifest diagnostics: tool counts, manifest version, hash, build SHA, deploy timestamp, privacy scope, and connector-staleness advisory.", inputSchema: { type: "object", properties: {} } },
  { name: "get_market_snapshot", description: "Compact market-state packet composing quote, price performance, moving-average trend, volume ratios, liquidity gate, and technical indicators in one call. Supports compact (default) and full modes, and optional batch of tickers.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, mode: { type: "string", enum: ["compact", "full"], default: "compact" }, foreign_exchange: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "health_check", description: "Return runtime and deployment health metadata.", inputSchema: { type: "object", properties: {} } },
];

const DEPRECATED_ALIAS_TOOLS: Tool[] = [];
const DEPRECATED_ALIAS_NAMES = new Set(Object.keys(TOOL_ALIASES));

TOOLS.push(...CANONICAL_ADDITIONS, ...DEPRECATED_ALIAS_TOOLS);
for (const tool of TOOLS) {
  const canonical = TOOL_ALIASES[tool.name];
  if (canonical && tool.name !== canonical) {
    tool.deprecated = true;
    tool.useInstead = canonical;
    tool.deprecationReason ??= "Use the canonical public tool name.";
  }
}

const SIMPLE_OBJECT_SCHEMA: Tool["outputSchema"] = {
  type: "object",
  properties: {},
  additionalProperties: true,
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
    data: { type: "object" },
    meta: {
      type: "object",
      properties: {
        tool: { type: "string" },
        generatedAt: { type: "string" },
        warnings: { type: "array", items: { type: "object" } },
      },
    },
  },
};

const OUTPUT_SCHEMAS: Record<string, Tool["outputSchema"]> = {
  get_historical_stock_prices: SIMPLE_OBJECT_SCHEMA,
  get_stock_info: SIMPLE_OBJECT_SCHEMA,
  get_etf_info: SIMPLE_OBJECT_SCHEMA,
  get_yahoo_finance_news: NEWS_OUTPUT_SCHEMA,
  get_stock_actions: SIMPLE_OBJECT_SCHEMA,
  get_financial_statement: SIMPLE_OBJECT_SCHEMA,
  get_holder_info: SIMPLE_OBJECT_SCHEMA,
  get_option_expiration_dates: SIMPLE_OBJECT_SCHEMA,
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
      lastPrice: { type: "number" },
      changePct: { type: "number" },
      distFromHigh52wPct: { type: "number" },
      distFromLow52wPct: { type: "number" },
      distFrom50dmaPct: { type: "number" },
      distFrom200dmaPct: { type: "number" },
      volatility30d: { type: "number" },
      cagr1y: { type: "number" },
      cagr3y: { type: "number" },
      cagr5y: { type: "number" },
      dataDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_analyst_consensus: SIMPLE_OBJECT_SCHEMA,
  get_earnings_analysis: SIMPLE_OBJECT_SCHEMA,
  get_financial_ratios: SIMPLE_OBJECT_SCHEMA,
  get_calendar: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      earningsDateConfirmed: { type: ["boolean", "null"] },
      earningsDateSource: { type: ["string", "null"] },
    },
    additionalProperties: true,
  },
  search_ticker: SIMPLE_OBJECT_SCHEMA,
  screen_stocks: SIMPLE_OBJECT_SCHEMA,
  get_technical_indicators: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      rsi14: { type: ["number", "null"] },
      macd: { type: ["number", "null"] },
      macdSignal: { type: ["number", "null"] },
      macdHistogram: { type: ["number", "null"] },
      lastClose: { type: ["number", "null"] },
      dataDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_price_slope: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      startClose: { type: ["number", "null"] },
      endClose: { type: ["number", "null"] },
      slopePct: { type: ["number", "null"] },
      direction: { type: "string" },
      dataDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_volume_ratio: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      ratio10d: { type: ["number", "null"] },
      ratio90d: { type: ["number", "null"] },
      volumeFlag: { type: ["string", "null"] },
      dataDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_ma_position: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      lastClose: { type: ["number", "null"] },
      sma50: { type: ["number", "null"] },
      sma200: { type: ["number", "null"] },
      distFrom50dmaPct: { type: ["number", "null"] },
      distFrom200dmaPct: { type: ["number", "null"] },
      trend: { type: "string" },
      dataDate: { type: "string" },
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
      shortPctOfFloat: { type: ["number", "null"] },
      momDelta: { type: ["number", "null"] },
      direction: { type: ["string", "null"] },
      squeezeRisk: { type: ["string", "null"] },
      flag: { type: ["string", "null"] },
      dataDate: { type: "string" },
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
  get_overnight_quote: SIMPLE_OBJECT_SCHEMA,
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
      putVolTrend: { type: ["string", "null"] },
      maxPainStrike: { type: ["number", "null"] },
      bracket: { type: ["string", "null"] },
      formattedBlock: { type: "string" },
      dataDate: { type: "string" },
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
      referenceTargetPrice: { type: ["number", "null"] },
      referenceTargetPct: { type: ["number", "null"] },
      ioPt: { type: ["number", "null"] },
      eqfPct: { type: ["number", "null"] },
      bracket: { type: ["string", "null"] },
      inferredTag: { type: ["string", "null"] },
      tag: { type: ["string", "null"] },
      tagNote: { type: ["string", "null"] },
      invertedFlag: { type: ["boolean", "null"] },
      dataDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_position_score_inputs: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      t1_inputs: { type: "object" },
      t2_inputs: { type: "object" },
      t4_inputs: { type: "object" },
      t5_inputs: { type: "object" },
      dataDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_volume_gate: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      currency: { type: ["string", "null"] },
      fxRate: { type: ["number", "null"] },
      lastVolume: { type: ["number", "null"] },
      adv10d: { type: ["number", "null"] },
      adv20d: { type: ["number", "null"] },
      adv90d: { type: ["number", "null"] },
      ratio20d: { type: ["number", "null"] },
      gatePass: { type: ["boolean", "null"] },
      dataDate: { type: "string" },
      note: { type: ["string", "null"] },
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
  get_filing_section: SIMPLE_OBJECT_SCHEMA,
  list_filing_tables: SIMPLE_OBJECT_SCHEMA,
  get_filing_table: SIMPLE_OBJECT_SCHEMA,
  extract_filing_fact: SIMPLE_OBJECT_SCHEMA,
  index_sec_filing: SIMPLE_OBJECT_SCHEMA,
  get_sec_filing_index: SIMPLE_OBJECT_SCHEMA,
  extract_geographic_revenue: SIMPLE_OBJECT_SCHEMA,
  extract_segment_revenue: SIMPLE_OBJECT_SCHEMA,
  extract_total_revenue: SIMPLE_OBJECT_SCHEMA,
  extract_revenue_exposure: SIMPLE_OBJECT_SCHEMA,
  extract_china_exposure: SIMPLE_OBJECT_SCHEMA,
  extract_risk_factor_mentions: SIMPLE_OBJECT_SCHEMA,
  extract_customer_concentration: SIMPLE_OBJECT_SCHEMA,
  extract_exposure: SIMPLE_OBJECT_SCHEMA,
  get_latest_earnings_release: ENVELOPE_V2_OUTPUT_SCHEMA,
  index_earnings_release: ENVELOPE_V2_OUTPUT_SCHEMA,
  extract_earnings_metrics: ENVELOPE_V2_OUTPUT_SCHEMA,
  extract_guidance: ENVELOPE_V2_OUTPUT_SCHEMA,
  extract_management_commentary: ENVELOPE_V2_OUTPUT_SCHEMA,
  compare_earnings_actual_vs_estimate: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      period: { type: ["string", "null"] },
      reportedPeriod: { type: ["string", "null"] },
      reportedDate: { type: ["string", "null"] },
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
  parse_public_transcript: ENVELOPE_V2_OUTPUT_SCHEMA,
  get_earnings_call_transcript: ENVELOPE_V2_OUTPUT_SCHEMA,
  list_sec_material_filings: ENVELOPE_V2_OUTPUT_SCHEMA,
  get_sec_filing_intelligence: ENVELOPE_V2_OUTPUT_SCHEMA,
  get_sec_filing_section_markdown: ENVELOPE_V2_OUTPUT_SCHEMA,
};

for (const [alias, canonical] of Object.entries(TOOL_ALIASES)) {
  OUTPUT_SCHEMAS[alias] = OUTPUT_SCHEMAS[canonical] ?? SIMPLE_OBJECT_SCHEMA;
}

for (const tool of TOOLS) {
  tool.outputSchema = OUTPUT_SCHEMAS[tool.name] ?? SIMPLE_OBJECT_SCHEMA;
}

const GROUPED_TOOLS: Tool[] = GROUPED_TOOL_DEFS.map((group) => ({
  name: group.name,
  description: group.description,
  inputSchema: {
    type: "object",
    properties: {
      action: { type: "string", enum: Object.keys(group.actions) },
      params: { type: "object", default: {} },
    },
    required: ["action"],
  },
  outputSchema: ENVELOPE_V2_OUTPUT_SCHEMA,
}));

const GROUPED_ACTIONS = new Map(
  GROUPED_TOOL_DEFS.map((group) => [group.name, new Set(Object.keys(group.actions))])
);

export function isGroupedMode(): boolean {
  return (getWorkerVar("TOOL_MODE") ?? "expanded").toLowerCase() === "grouped";
}

export function listVisibleTools(): Tool[] {
  return isGroupedMode() ? GROUPED_TOOLS : TOOLS.filter((t) => !t.deprecated);
}

const str = (v: unknown, fallback = ""): string => (typeof v === "string" ? v : fallback);
const num = (v: unknown, fallback: number): number => (typeof v === "number" ? v : fallback);
const tickerArg = (v: unknown): string | string[] =>
  Array.isArray(v) ? v.map(String) : str(v);

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

function xbrlSourceEvidence(parsed: Record<string, unknown>): Record<string, unknown> | null {
  const ctx = parsed.xbrlContext;
  if (!ctx || typeof ctx !== "object") return null;
  const c = ctx as Record<string, unknown>;
  return {
    sourceType: "sec_xbrl_companyconcept",
    concept: c.concept ?? null,
    taxonomy: c.taxonomy ?? null,
    unit: c.unit ?? parsed.unit ?? null,
    accessionNumber: parsed.accessionNumber ?? c.accessionNumber ?? null,
    filingType: parsed.filingType ?? c.form ?? null,
    filingDate: parsed.filingDate ?? c.filedAt ?? null,
    periodStart: c.periodStart ?? null,
    periodEnd: c.periodEnd ?? c.instant ?? null,
    fiscalYear: c.fiscalYear ?? null,
    fiscalPeriod: c.fiscalPeriod ?? null,
    dimensions: c.dimensions ?? {},
    documentUrl: parsed.documentUrl ?? null,
    indexUrl: parsed.indexUrl ?? null,
  };
}

type DoctrineToolStatus = {
  capabilityStatus: "ACTIVE" | "DEGRADED" | "PROVIDER_GATED" | "EXPERIMENTAL" | "RETIRED";
  decisionGrade: boolean;
  doctrineUse: "ALLOWED" | "VERIFY_ONLY" | "DIAGNOSTICS_ONLY" | "BLOCKED";
  failureMode: string | null;
  evidenceRequired: boolean;
  sourceType: "sec_xbrl" | "sec_table" | "sec_filing" | "company_ir" | "yahoo" | "exchange" | "provider_diagnostic" | "unknown";
};

const TOOL_DOCTRINE_STATUS: Record<string, DoctrineToolStatus> = {
  get_overnight_quote: {
    capabilityStatus: "DEGRADED",
    decisionGrade: false,
    doctrineUse: "DIAGNOSTICS_ONLY",
    failureMode: "YAHOO_EXTENDED_HOURS_PROXY_ONLY",
    evidenceRequired: false,
    sourceType: "yahoo",
  },
  get_sec_filing_section_markdown: {
    capabilityStatus: "DEGRADED",
    decisionGrade: false,
    doctrineUse: "BLOCKED",
    failureMode: "LIVE_SECTION_EXTRACTION_UNRELIABLE",
    evidenceRequired: true,
    sourceType: "sec_filing",
  },
  get_company_press_releases: {
    capabilityStatus: "DEGRADED",
    decisionGrade: false,
    doctrineUse: "VERIFY_ONLY",
    failureMode: "SEC_EX99_LINKAGE_INCOMPLETE",
    evidenceRequired: true,
    sourceType: "company_ir",
  },
  extract_sec_filing_fact: {
    capabilityStatus: "DEGRADED",
    decisionGrade: false,
    doctrineUse: "VERIFY_ONLY",
    failureMode: "XBRL_CONTEXT_METADATA_UNDER_VERIFICATION",
    evidenceRequired: true,
    sourceType: "sec_xbrl",
  },
};

function doctrineStatusFor(tool: string): DoctrineToolStatus | undefined {
  return TOOL_DOCTRINE_STATUS[tool];
}

function doctrineStatusDiagnostics(): Record<string, unknown> {
  const counts: Record<string, number> = {};
  for (const status of Object.values(TOOL_DOCTRINE_STATUS)) {
    counts[status.capabilityStatus] = (counts[status.capabilityStatus] ?? 0) + 1;
  }
  return {
    doctrineToolStatusCount: Object.keys(TOOL_DOCTRINE_STATUS).length,
    doctrineToolStatusCounts: counts,
    doctrineToolStatus: TOOL_DOCTRINE_STATUS,
  };
}

type AliasSuccessOptions = {
  canonicalTool: string;
  deprecatedTool?: boolean;
  useInstead?: string;
  partialSuccess?: boolean;
  successCount?: number;
  errorCount?: number;
  warnings?: { code: string; message: string; severity: string }[];
  metaExtra?: Record<string, unknown>;
};

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
    tableCount: allTables.length,
    returnedCount: tables.length,
    offset: safeOffset,
    limit: safeLimit,
    hasMore: safeOffset + tables.length < allTables.length,
    tables,
  });
}

const SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json";
const SEC_DATA_BASE = "https://data.sec.gov";
const SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data";
const REVENUE_CONCEPTS = [
  "RevenueFromContractWithCustomerExcludingAssessedTax",
  "Revenues",
  "SalesRevenueNet",
  "RevenueFromContractWithCustomerIncludingAssessedTax",
];

function structuredFactsDisabled(): boolean {
  return getWorkerVar("STRUCTURED_FACT_PROVIDER") === "disabled";
}

function secUserAgent(): string {
  return getWorkerVar("SEC_USER_AGENT") ?? "yahoo-finance-mcp/1.4.1 public-market-data";
}

function structuredFactsUnavailable(tool: string, code: string, message: string, args: Record<string, unknown>, extra: Record<string, unknown> = {}): string {
  return JSON.stringify({
    status: code,
    code,
    provider: "official_sec_data_api",
    tool,
    ticker: str(args.ticker).toUpperCase(),
    topic: str(args.topic ?? args.region ?? args.exposure_query ?? (tool === "extract_china_exposure" ? "China" : "")),
    value: null,
    valuePct: null,
    warnings: [{ code, message, severity: code === "STRUCTURED_FACT_PROVIDER_UNCONFIGURED" ? "info" : "error" }],
    ...extra,
  });
}

function structuredTopic(tool: string, args: Record<string, unknown>): string {
  if (tool === "extract_geographic_revenue") return str(args.region);
  if (tool === "extract_revenue_exposure") return str(args.exposure_query);
  if (tool === "extract_china_exposure") return "China";
  if (tool === "extract_segment_revenue") return "segment revenue";
  if (tool === "extract_total_revenue") return "total revenue";
  return str(args.topic);
}

function shapeStructuredFactPayload(tool: string, args: Record<string, unknown>, payload: Record<string, unknown>): string {
  const topic = structuredTopic(tool, args);
  const status = str(payload.status ?? payload.code);
  const common = {
    ...payload,
    provider: payload.provider ?? "official_sec_data_api",
    factType: tool === "extract_total_revenue" ? "total_revenue"
      : tool === "extract_segment_revenue" ? "segment_revenue"
      : "geographic_revenue",
  };
  if (tool === "extract_china_exposure") {
    return JSON.stringify({
      ...common,
      overallStatus: status === "FOUND" ? "FOUND_REVENUE_EXPOSURE" : status,
      revenueExposure: payload,
      nonRevenueExposure: null,
      topic: "China",
    });
  }
  if (tool === "extract_revenue_exposure") {
    return JSON.stringify({
      ...common,
      exposureQuery: topic,
      matches: status === "FOUND" ? [payload.evidence ?? payload] : [],
    });
  }
  if (tool === "extract_exposure") {
    return JSON.stringify({
      ...common,
      topic,
      revenueExposure: payload,
      riskMentions: [],
      operationalExposure: null,
    });
  }
  return JSON.stringify({
    ...common,
    region: topic,
  });
}

function compactText(value: unknown): string {
  return String(value ?? "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function padCik(cik: number | string): string {
  return String(cik).replace(/\D/g, "").padStart(10, "0");
}

function accessionNoDashes(accession: string): string {
  return accession.replace(/-/g, "");
}

async function fetchSecJson(url: string, timeoutMs = 20_000): Promise<Record<string, unknown>> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, {
      headers: {
        "Accept": "application/json",
        "User-Agent": secUserAgent(),
      },
      signal: controller.signal,
    });
    if (resp.status === 429) {
      throw new Error("SEC_RATE_LIMITED");
    }
    if (!resp.ok) {
      throw new Error(`SEC API returned HTTP ${resp.status}`);
    }
    return await resp.json() as Record<string, unknown>;
  } finally {
    clearTimeout(timeout);
  }
}

async function resolveCikFromSecTickerMap(ticker: string): Promise<{ cik: string; companyName: string | null }> {
  const tickerU = ticker.toUpperCase();
  const payload = await fetchSecJson(SEC_COMPANY_TICKERS_URL);
  for (const item of Object.values(payload)) {
    if (!item || typeof item !== "object") continue;
    const row = item as Record<string, unknown>;
    if (String(row.ticker ?? "").toUpperCase() !== tickerU) continue;
    return { cik: padCik(String(row.cik_str ?? "")), companyName: str(row.title, "") || null };
  }
  throw new Error("TICKER_NOT_FOUND");
}

function recentFilings(submissions: Record<string, unknown>): Array<Record<string, unknown>> {
  const recent = submissions.recent as Record<string, unknown> | undefined;
  if (!recent) return [];
  const accessionNumber = Array.isArray(recent.accessionNumber) ? recent.accessionNumber : [];
  const forms = Array.isArray(recent.form) ? recent.form : [];
  const filingDate = Array.isArray(recent.filingDate) ? recent.filingDate : [];
  const primaryDocument = Array.isArray(recent.primaryDocument) ? recent.primaryDocument : [];
  return accessionNumber.map((accn, i) => ({
    accessionNumber: String(accn),
    filingType: String(forms[i] ?? ""),
    filingDate: String(filingDate[i] ?? ""),
    primaryDocument: String(primaryDocument[i] ?? ""),
  }));
}

async function resolveOfficialSecFiling(ticker: string, cik: string, requestedType: string, accessionNumber: string | null): Promise<Record<string, unknown>> {
  const submissions = await fetchSecJson(`${SEC_DATA_BASE}/submissions/CIK${cik}.json`);
  const filings = recentFilings(submissions);
  const requested = requestedType.toUpperCase();
  const candidates = requested === "10-K" ? [requested, "20-F"] : [requested];
  let filing = accessionNumber
    ? filings.find((f) => String(f.accessionNumber) === accessionNumber)
    : null;
  let actualType = filing ? String(filing.filingType) : "";
  if (!filing) {
    for (const form of candidates) {
      filing = filings.find((f) => String(f.filingType).toUpperCase() === form) ?? null;
      if (filing) {
        actualType = form;
        break;
      }
    }
  }
  const availableFilingTypes = Array.from(new Set(filings.map((f) => String(f.filingType)).filter(Boolean))).slice(0, 20);
  if (!filing) {
    throw new Error(JSON.stringify({
      code: "FILING_NOT_FOUND_TRY_OTHER_TYPE",
      requestedFilingType: requestedType,
      availableFilingTypes,
      suggestedFilingTypes: requested === "10-K" && availableFilingTypes.includes("20-F") ? ["20-F"] : [],
    }));
  }
  const accn = String(filing.accessionNumber ?? "");
  const primaryDocument = String(filing.primaryDocument ?? "");
  const documentUrl = accn && primaryDocument
    ? `${SEC_ARCHIVES_BASE}/${Number(cik)}/${accessionNoDashes(accn)}/${primaryDocument}`
    : null;
  return {
    ticker: ticker.toUpperCase(),
    cik,
    companyName: str(submissions.name, "") || null,
    filingType: actualType || String(filing.filingType ?? requestedType),
    requestedFilingType: requestedType,
    filingDate: filing.filingDate ?? null,
    accessionNumber: accn || null,
    documentUrl,
    availableFilingTypes,
  };
}

type SecFactRow = {
  concept: string;
  taxonomy: string;
  unit: string;
  label: string;
  value: number | null;
  accn: string | null;
  form: string | null;
  filed: string | null;
  fy: number | null;
  fp: string | null;
  end: string | null;
  raw: Record<string, unknown>;
};

function factRows(companyFacts: Record<string, unknown>, concepts: string[]): SecFactRow[] {
  const out: SecFactRow[] = [];
  const facts = companyFacts.facts as Record<string, unknown> | undefined;
  if (!facts) return out;
  for (const [taxonomy, taxonomyFacts] of Object.entries(facts)) {
    if (!taxonomyFacts || typeof taxonomyFacts !== "object") continue;
    const conceptMap = taxonomyFacts as Record<string, unknown>;
    for (const concept of concepts) {
      const node = conceptMap[concept] as Record<string, unknown> | undefined;
      if (!node || typeof node !== "object") continue;
      const label = str(node.label ?? node.description ?? concept);
      const units = node.units as Record<string, unknown> | undefined;
      if (!units) continue;
      for (const [unit, rows] of Object.entries(units)) {
        if (!Array.isArray(rows)) continue;
        for (const raw of rows) {
          if (!raw || typeof raw !== "object") continue;
          const r = raw as Record<string, unknown>;
          out.push({
            concept,
            taxonomy,
            unit,
            label,
            value: typeof r.val === "number" ? r.val : null,
            accn: r.accn != null ? String(r.accn) : null,
            form: r.form != null ? String(r.form) : null,
            filed: r.filed != null ? String(r.filed) : null,
            fy: typeof r.fy === "number" ? r.fy : null,
            fp: r.fp != null ? String(r.fp) : null,
            end: r.end != null ? String(r.end) : null,
            raw: r,
          });
        }
      }
    }
  }
  return out;
}

function chooseRevenueFact(rows: SecFactRow[], filing: Record<string, unknown>, topic: string | null): SecFactRow | null {
  const accn = String(filing.accessionNumber ?? "");
  const filingType = String(filing.filingType ?? "");
  const topicNorm = topic ? compactText(topic) : "";
  const filtered = rows.filter((row) => {
    if (row.value == null) return false;
    if (!["10-K", "20-F", "40-F", "10-Q", "6-K", "8-K"].includes(String(row.form ?? ""))) return false;
    if (accn && row.accn && row.accn !== accn) return false;
    if (filingType && row.form && row.form !== filingType) return false;
    if (topicNorm) {
      const haystack = compactText(`${row.concept} ${row.label} ${JSON.stringify(row.raw)}`);
      return haystack.includes(topicNorm);
    }
    return true;
  });
  const candidates = filtered.length ? filtered : rows.filter((row) => row.value != null && (!topicNorm));
  return candidates.sort((a, b) => {
    const dateCmp = String(b.end ?? b.filed ?? "").localeCompare(String(a.end ?? a.filed ?? ""));
    if (dateCmp !== 0) return dateCmp;
    return Math.abs(b.value ?? 0) - Math.abs(a.value ?? 0);
  })[0] ?? null;
}

function officialSecPayload(tool: string, args: Record<string, unknown>, filing: Record<string, unknown>, row: SecFactRow | null, totalRow: SecFactRow | null, status: string, message?: string): Record<string, unknown> {
  const topic = structuredTopic(tool, args);
  const value = row?.value ?? null;
  const total = totalRow?.value ?? null;
  const valuePct = value != null && total != null && total !== 0 ? Number(((value / total) * 100).toFixed(4)) : null;
  return {
    status,
    code: status,
    provider: "official_sec_data_api",
    ticker: str(args.ticker).toUpperCase(),
    topic,
    value,
    valuePct,
    currency: row?.unit ?? totalRow?.unit ?? "USD",
    denominator: total,
    ...filing,
    evidence: row ? {
      provider: "sec_companyfacts",
      sourceUrl: `${SEC_DATA_BASE}/api/xbrl/companyfacts/CIK${filing.cik}.json`,
      concept: row.concept,
      taxonomy: row.taxonomy,
      label: row.label,
      unit: row.unit,
      periodEnd: row.end,
      filed: row.filed,
      accessionNumber: row.accn,
      form: row.form,
    } : null,
    warnings: message ? [{ code: status, message, severity: status === "FOUND" ? "info" : "warning" }] : [],
  };
}

async function callStructuredFactsProvider(tool: string, args: Record<string, unknown>): Promise<string> {
  if (structuredFactsDisabled()) {
    return structuredFactsUnavailable(
      tool,
      "STRUCTURED_FACT_PROVIDER_UNCONFIGURED",
      "STRUCTURED_FACT_PROVIDER is disabled; official SEC structured facts are not active.",
      args,
    );
  }
  try {
    const ticker = str(args.ticker).toUpperCase();
    const { cik } = await resolveCikFromSecTickerMap(ticker);
    const filing = await resolveOfficialSecFiling(
      ticker,
      cik,
      str(args.filing_type, "10-K"),
      args.accession_number != null ? str(args.accession_number) : null,
    );
    const facts = await fetchSecJson(`${SEC_DATA_BASE}/api/xbrl/companyfacts/CIK${cik}.json`);
    const rows = factRows(facts, REVENUE_CONCEPTS);
    const totalRow = chooseRevenueFact(rows, filing, null);
    const topic = structuredTopic(tool, args);
    const wantsDimensionalFact = !["extract_total_revenue"].includes(tool);
    const row = wantsDimensionalFact ? chooseRevenueFact(rows, filing, topic) : totalRow;
    let payload: Record<string, unknown>;
    if (row) {
      payload = officialSecPayload(tool, args, filing, row, totalRow, "FOUND");
    } else if (totalRow && wantsDimensionalFact) {
      payload = officialSecPayload(
        tool,
        args,
        filing,
        null,
        totalRow,
        "PROVIDER_LIMITATION",
        "Official SEC companyfacts exposes standardized entity-level facts, but no matching dimensional geography/segment fact was available for this topic.",
      );
    } else {
      payload = officialSecPayload(
        tool,
        args,
        filing,
        null,
        totalRow,
        "NO_STANDARD_REVENUE_FACT",
        "Official SEC companyfacts did not expose a standard revenue fact for this filing.",
      );
    }
    return shapeStructuredFactPayload(tool, args, payload);
  } catch (e) {
    let parsed: Record<string, unknown> | null = null;
    try {
      parsed = JSON.parse(e instanceof Error ? e.message : String(e)) as Record<string, unknown>;
    } catch {
      parsed = null;
    }
    if (parsed?.code === "FILING_NOT_FOUND_TRY_OTHER_TYPE") {
      return structuredFactsUnavailable(tool, "FILING_NOT_FOUND_TRY_OTHER_TYPE", "No requested SEC filing was found; try another filing_type.", args, parsed);
    }
    const message = e instanceof Error ? e.message : String(e);
    const code = message.includes("SEC_RATE_LIMITED") ? "SEC_RATE_LIMITED"
      : message.includes("TICKER_NOT_FOUND") ? "TICKER_NOT_FOUND"
      : "STRUCTURED_FACT_PROVIDER_UNAVAILABLE";
    return structuredFactsUnavailable(
      tool,
      code,
      code === "SEC_RATE_LIMITED" ? "SEC rate limited the official data API request." : `Official SEC data API unavailable: ${message}`,
      args,
    );
  }
}

async function structuredFactProviderDiagnostics(): Promise<Record<string, unknown>> {
  const disabled = structuredFactsDisabled();
  const lastSmokeStatus = getWorkerVar("EDGAR_FACTS_LAST_SMOKE_STATUS") ?? null;
  const smokeOk = typeof lastSmokeStatus === "string"
    ? lastSmokeStatus.toUpperCase() === "OK" || lastSmokeStatus.toUpperCase() === "PASS"
    : false;
  return {
    structuredFactProvider: disabled ? "disabled" : "official_sec_data_api",
    structuredFactProviderConfigured: !disabled,
    structuredFactProviderUrlConfigured: true,
    structuredFactProviderLastSmokeStatus: lastSmokeStatus,
    structuredFactProviderHealth: disabled ? "UNCONFIGURED" : (smokeOk ? "OK" : "UNKNOWN"),
    structuredFactProviderCacheStatus: null,
    structuredFactProviderLastErrorCode: disabled ? "STRUCTURED_FACT_PROVIDER_UNCONFIGURED" : (smokeOk ? null : "STRUCTURED_FACT_PROVIDER_SMOKE_UNKNOWN"),
  };
}

export async function callTool(name: string, args: Record<string, unknown>): Promise<string> {
  const aliasTarget = TOOL_ALIASES[name];
  const canonicalTool = aliasTarget ?? name;
  let raw = await _dispatchTool(canonicalTool, args);
  let batchMeta: { partialSuccess?: boolean; successCount?: number; errorCount?: number } | undefined;
  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const metaRaw = parsed.__batchMeta;
    if (metaRaw != null && typeof metaRaw === "object") {
      const bm = metaRaw as Record<string, unknown>;
      batchMeta = {
        partialSuccess: bm.partialSuccess === true,
        successCount: typeof bm.successCount === "number" ? bm.successCount : undefined,
        errorCount: typeof bm.errorCount === "number" ? bm.errorCount : undefined,
      };
      delete parsed.__batchMeta;
      raw = JSON.stringify(parsed);
    }
  } catch {
    // non-JSON payload
  }
  if (aliasTarget != null) {
    const aliasToolDef = TOOLS.find((t) => t.name === name);
    const opts: AliasSuccessOptions = {
      canonicalTool,
      ...(doctrineStatusFor(canonicalTool) ? { metaExtra: doctrineStatusFor(canonicalTool) } : {}),
      ...(DEPRECATED_ALIAS_NAMES.has(name)
        ? {
            deprecatedTool: true,
            useInstead: aliasToolDef?.useInstead ?? TOOL_ALIASES[name] ?? canonicalTool,
          }
        : {}),
      ...(batchMeta ?? {}),
    };
    if (DEPRECATED_ALIAS_NAMES.has(name)) {
      opts.warnings = [{
        code: "DEPRECATED_ALIAS",
        message: `Use ${canonicalTool} instead.`,
        severity: "info",
      }];
    }
    return mcpSuccess(name, raw, opts);
  }
  return mcpSuccess(name, raw, {
    ...(batchMeta ?? {}),
    ...(doctrineStatusFor(canonicalTool) ? { metaExtra: doctrineStatusFor(canonicalTool) } : {}),
  });
}

export async function callVisibleTool(name: string, args: Record<string, unknown>): Promise<string> {
  if (!isGroupedMode()) return callTool(name, args);

  const actions = GROUPED_ACTIONS.get(name);
  if (!actions) {
    throw new Error(`Unknown grouped tool: ${name}`);
  }
  const action = str(args.action).trim();
  if (!action) {
    return mcpFailure(name, ErrorCode.INPUT_VALIDATION_ERROR, "action is required");
  }
  if (!actions.has(action)) {
    return mcpFailure(name, ErrorCode.INPUT_VALIDATION_ERROR, `Unknown action '${action}' for grouped tool '${name}'`);
  }
  const params = args.params;
  if (params != null && (typeof params !== "object" || Array.isArray(params))) {
    return mcpFailure(name, ErrorCode.INPUT_VALIDATION_ERROR, "params must be an object when provided");
  }
  return callTool(action, (params as Record<string, unknown> | undefined) ?? {});
}

async function _dispatchTool(name: string, args: Record<string, unknown>): Promise<string> {
  switch (name) {
    case "get_historical_prices": {
      const rawTicker = args.ticker;
      if (rawTicker == null || String(rawTicker).trim() === "") {
        return mcpFailure("get_historical_prices", ErrorCode.INPUT_VALIDATION_ERROR, "ticker is required");
      }
      const tickerStr = String(rawTicker).trim().toUpperCase();
      const tickerErr = validateTicker(tickerStr);
      if (tickerErr) return mcpFailure("get_historical_prices", ErrorCode.INPUT_VALIDATION_ERROR, tickerErr);
      return getHistoricalPrices(tickerStr, str(args.period, "1mo"), str(args.interval, "1d"), args.prepost === true);
    }
    case "get_company_profile":
      return getStockInfo(tickerArg(args.ticker), args.include_all === true);
    case "get_fund_profile":
      return getEtfInfo(tickerArg(args.ticker));
    case "get_company_news":
      return getCompanyNews(
        tickerArg(args.ticker),
        num(args.max_results, 10),
        num(args.lookback_days, 14),
        Array.isArray(args.sources)
          ? args.sources.map(String)
          : ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"],
      );
    case "get_corporate_actions":
      return getStockActions(str(args.ticker));
    case "get_financial_statement":
      return getFinancialStatement(str(args.ticker), str(args.financial_type));
    case "get_ownership_holders":
      return getHolderInfo(str(args.ticker), str(args.holder_type));
    case "get_option_expiration_dates":
      return getOptionExpirationDates(str(args.ticker));
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
      return getFinancialRatios(tickerArg(args.ticker));
    case "get_company_events_calendar":
      return getCalendar(str(args.ticker));
    case "search_ticker":
      return searchTicker(str(args.query), num(args.max_results, 8), args.exchange != null ? str(args.exchange) : null);
    case "screen_stocks":
      return screenStocks(str(args.screener_name), num(args.count, 25));
    case "get_short_interest":
      return getShortInterest(str(args.ticker));
    case "get_technical_indicators":
      return getTechnicalIndicators(tickerArg(args.ticker), str(args.period, "3mo"));
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
    case "get_overnight_quote":
      return getOvernightQuote(str(args.ticker));

    case "extract_sec_filing_fact":
      if (args.fact_type != null || args.region != null || args.fact_name == null || args.fact != null) {
        const fact = str(args.fact_type ?? args.fact, args.region != null ? "geographic_revenue" : "total_revenue");
        const raw = await getFilingData(
          str(args.ticker),
          fact,
          args.region != null ? str(args.region) : null,
          str(args.filing_type, "10-K"),
          str(args.period, "latest"),
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
        const sourceEvidence = hasUsefulEvidence(parsed.evidence)
          ? parsed.evidence
          : xbrlSourceEvidence(parsed);
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
          extractionMethod: parsed.extractionMethod ?? "NONE",
          source: parsed.source ?? "NOT_DISCLOSED",
          confidence: parsed.confidence ?? "NOT_DISCLOSED",
          documentUrl: parsed.documentUrl ?? null,
          indexUrl: parsed.indexUrl ?? null,
          primaryDocumentUrl: parsed.primaryDocumentUrl ?? null,
          xbrlContext: parsed.xbrlContext ?? null,
          evidence: parsed.evidence ?? null,
          sourceEvidence,
          calculation: parsed.calculation ?? null,
          warnings: parsed.warnings ?? [],
          ticker: parsed.ticker ?? str(args.ticker),
        });
      }
      return extractFilingFact(str(args.ticker), str(args.fact_name), args.document_url != null ? str(args.document_url) : null, args.accession_number != null ? str(args.accession_number) : null);
    case "list_sec_company_filings":
      return listSecCompanyFilings(str(args.ticker), str(args.filing_type ?? args.form_type, "10-K"), num(args.limit ?? args.max_filings, 5));
    case "get_sec_filing_outline": {
      const ticker = str(args.ticker);
      const filingType = str(args.filing_type ?? args.form_type, "10-K");
      const docUrl = args.document_url != null
        ? str(args.document_url)
        : await resolveSecDocumentUrl(ticker, filingType, 1);
      return getFilingOutline(ticker, args.accession_number != null ? str(args.accession_number) : null, docUrl);
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
        (args.search_terms as string[]) ?? (args.search_query != null ? [str(args.search_query)] : []),
        args.section_hint != null ? str(args.section_hint) : (args.selector != null ? str((args.selector as Record<string, unknown>).item, "") : null),
        str(args.filing_type, "10-K"),
        args.accession_number != null ? str(args.accession_number) : null,
        num(args.context_chars, 1500),
        args.return_tables !== false,
        args.document_url != null ? str(args.document_url) : null,
      );
    case "index_sec_filing":
      return indexSecFiling(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null);
    case "get_sec_filing_index":
      return getSecFilingIndex(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null);
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
      return parsePublicTranscript(str(args.url), Array.isArray(args.topics) ? args.topics.map(String) : null);
    case "get_earnings_call_transcript":
      return getEarningsCallTranscript(
        str(args.ticker),
        str(args.period, "latest"),
        Array.isArray(args.topics) ? args.topics.map(String) : null,
      );
    case "extract_geographic_revenue":
    case "extract_segment_revenue":
    case "extract_total_revenue":
    case "extract_revenue_exposure":
    case "extract_china_exposure":
    case "extract_exposure":
      return callStructuredFactsProvider(name, args);
    case "extract_risk_factor_mentions":
      return extractRiskFactorMentions(str(args.ticker), Array.isArray(args.terms) ? args.terms.map(String) : [], str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"));
    case "extract_customer_concentration":
      return extractCustomerConcentration(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"));
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
    case "search_filing_text":
      return searchFilingText(
        str(args.ticker),
        (args.search_terms as string[]) ?? [],
        args.section_hint != null ? str(args.section_hint) : null,
        str(args.filing_type, "10-K"),
        args.accession_number != null ? str(args.accession_number) : null,
        num(args.context_chars, 1500),
        args.return_tables !== false,
      );
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
        Array.isArray(args.sources) ? args.sources.map(String) : ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"],
        num(args.max_results, 10),
      );
    case "get_company_press_releases":
      return getCompanyPressReleases(
        str(args.ticker),
        num(args.lookback_days, 90),
        num(args.max_results, 20),
        Array.isArray(args.sources) ? args.sources.map(String) : ["yahoo_finance_press_releases", "company_ir", "newswire", "sec"],
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
        Array.isArray(args.sources) ? args.sources.map(String) : ["sec", "company_ir", "newswire", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"],
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
        Array.isArray(args.sources) ? args.sources.map(String) : ["sec", "company_ir", "newswire", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"],
      );
    case "health_check": {
      const buildSha = getWorkerVar("BUILD_SHA") ?? "unknown";
      const buildDate = getWorkerVar("BUILD_DATE") ?? "unknown";
      const version = getWorkerVar("SERVER_VERSION") ?? "1.1.0";
      const visibleTools = listVisibleTools();
      const canonicalToolCount = visibleTools.length;
      const deprecatedAliasCount = isGroupedMode() ? 0 : TOOLS.filter((t) => t.deprecated === true).length;
      const manifestVersion = getWorkerVar("MANIFEST_VERSION") ?? "1";
      const deployedAt = getWorkerVar("DEPLOYED_AT") ?? new Date().toISOString();
      const manifestHash = await computeHash(JSON.stringify(visibleTools.map(t => t.name)));
      const structuredFacts = await structuredFactProviderDiagnostics();
      const doctrineStatus = doctrineStatusDiagnostics();
      return JSON.stringify({
        status: "ok",
        serverVersion: version,
        envelopeV2: getWorkerVar("MCP_ENVELOPE_V2") === "true",
        nodeVersion: "cloudflare-worker",
        toolCount: canonicalToolCount,
        canonicalToolCount,
        deprecatedAliasCount,
        manifestVersion,
        manifestHash,
        schemaHash: manifestHash,
        runtimeHash: await computeHash(version + String(canonicalToolCount)),
        buildSha,
        buildDate,
        deployedAt,
        generatedAt: new Date().toISOString(),
        privacyScope: "public_market_data_only",
        ...structuredFacts,
        ...doctrineStatus,
      });
    }
    case "get_manifest_diagnostics": {
      const buildSha = getWorkerVar("BUILD_SHA") ?? "unknown";
      const version = getWorkerVar("SERVER_VERSION") ?? "1.0.0";
      const visibleTools = listVisibleTools();
      const canonicalToolCount = visibleTools.length;
      const deprecatedAliasCount = isGroupedMode() ? 0 : TOOLS.filter((t) => t.deprecated === true).length;
      const manifestVersion = getWorkerVar("MANIFEST_VERSION") ?? null;
      const deployedAt = getWorkerVar("DEPLOYED_AT") ?? null;
      const manifestHash = await computeHash(JSON.stringify(visibleTools.map(t => t.name)));
      const workerSchemaGeneratedAt = new Date().toISOString();
      const structuredFacts = await structuredFactProviderDiagnostics();
      const doctrineStatus = doctrineStatusDiagnostics();
      return JSON.stringify({
        toolCount: canonicalToolCount,
        manifestVersion,
        manifestHash,
        buildSha,
        deployedAt,
        privacyScope: "public_market_data_only",
        canonicalToolCount,
        deprecatedAliasCount,
        publicSchemaGeneratedAt: null,
        workerSchemaGeneratedAt,
        manifestMismatch: null,
        staleConnectorWarning: "ChatGPT connector schema may lag the deployed Worker schema. Direct Worker tools/list and get_manifest_diagnostics are source of truth.",
        serverVersion: version,
        ...structuredFacts,
        ...doctrineStatus,
      });
    }
    case "get_market_snapshot": {
      const t = args.ticker;
      const ticker = Array.isArray(t) ? (t as string[]) : str(t);
      const mode = str(args.mode, "compact") === "full" ? "full" : "compact";
      const foreignExchange = args.foreign_exchange === true;
      return getMarketSnapshot(ticker, mode, foreignExchange);
    }
    case "get_options_summary":
      return getOptionsSummary(str(args.ticker), args.expiry_hint != null ? str(args.expiry_hint) : undefined);
    case "get_filing_data":
      return getFilingData(str(args.ticker), str(args.fact_type), args.region != null ? str(args.region) : null, str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.period_mode, "auto"));
    case "list_sec_filings":
      return listSecFilings(str(args.ticker), str(args.filing_type ?? args.form_type, "10-K"), num(args.limit ?? args.max_filings, 5));
    case "get_filing_outline":
      return _dispatchTool("get_sec_filing_outline", args);
    case "get_filing_section":
      return _dispatchTool("get_sec_filing_section", args);
    case "list_filing_tables":
      return _dispatchTool("list_sec_filing_tables", args);
    case "get_filing_table":
      return _dispatchTool("get_sec_filing_table", args);
    case "extract_filing_fact":
      return extractFilingFact(str(args.ticker), str(args.fact_name), args.document_url != null ? str(args.document_url) : null, args.accession_number != null ? str(args.accession_number) : null);
    case "get_fast_info":
      return getFastInfo(tickerArg(args.ticker));
    case "get_historical_stock_prices": {
      const rawTicker = args.ticker;
      if (rawTicker == null || String(rawTicker).trim() === "") {
        return mcpFailure("get_historical_stock_prices", ErrorCode.INPUT_VALIDATION_ERROR, "ticker is required");
      }
      const tickerStr = String(rawTicker).trim().toUpperCase();
      const tickerErr = validateTicker(tickerStr);
      if (tickerErr) return mcpFailure("get_historical_stock_prices", ErrorCode.INPUT_VALIDATION_ERROR, tickerErr);
      return getHistoricalPrices(tickerStr, str(args.period, "1mo"), str(args.interval, "1d"), args.prepost === true);
    }
    case "get_stock_info":
      return getStockInfo(tickerArg(args.ticker), args.include_all === true);
    case "get_etf_info":
      return getEtfInfo(tickerArg(args.ticker));
    case "get_stock_actions":
      return getStockActions(str(args.ticker));
    case "get_holder_info":
      return getHolderInfo(str(args.ticker), str(args.holder_type));
    case "get_price_stats":
      return getPriceStats(tickerArg(args.ticker));
    case "get_ma_position":
      return getMaPosition(tickerArg(args.ticker));
    case "get_volume_ratio":
      return getVolumeRatio(tickerArg(args.ticker), num(args.period, 10));
    case "get_volume_gate":
      return getVolumeGate(str(args.ticker), args.foreign_exchange === true);
    case "get_financial_ratios":
      return getFinancialRatios(tickerArg(args.ticker));
    case "get_credit_health":
      return getCreditHealth(tickerArg(args.ticker));
    case "get_recommendations":
      return getRecommendations(str(args.ticker), str(args.recommendation_type), num(args.months_back, 12));
    case "get_analyst_upgrade_radar":
      return getAnalystUpgradeRadar(tickerArg(args.ticker), num(args.days_back, 30));
    case "get_earnings_momentum":
      return getEarningsMomentum(tickerArg(args.ticker));
    case "get_calendar":
      return getCalendar(str(args.ticker));
    case "get_yahoo_finance_news":
      return getCompanyNews(str(args.ticker), 10, 14, ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"]);
    case "get_options_flow_summary":
      return getOptionsSummary(str(args.ticker), args.expiry_hint != null ? str(args.expiry_hint) : undefined);
    case "get_options_flow_scan":
      return getOptionsFlowScan(str(args.ticker), str(args.window_label));
    case "get_put_hedge_candidates":
      return getPutHedgeCandidates(str(args.ticker), num(args.otm_pct_min, 8), num(args.otm_pct_max, 12), num(args.budget_usd, 500), str(args.expiry_after));
    case "get_price_target_bracket":
      return getPriceTargetBracket(str(args.ticker), num(args.reference_target_price ?? args.io_pt, 0));
    case "get_position_score_inputs":
      return getPositionScoreInputs(tickerArg(args.ticker));
    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}
