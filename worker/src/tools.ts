import { mcpSuccess, mcpFailure, ErrorCode, getWorkerVar } from "./response.js";
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
  extractGeographicRevenue,
  extractSegmentRevenue,
  extractTotalRevenue,
  extractRevenueExposure,
  extractChinaExposure,
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
      "Get historical stock prices for a given ticker symbol. Returns Date, Open, High, Low, Close, Volume, and Adj Close.",
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
      "Get pre-computed credit/leverage metrics: Net Debt/EBITDA, interest coverage, debt tier, credit stress flag. Max 5 tickers per call; split larger lists into multiple calls.",
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
      "Get recent analyst rating changes with signal classification (UPGRADE/DOWNGRADE/MAINTAIN), netSentiment, and summary. Returns ptFrom, ptTo (null — price target data not exposed by yfinance), and ptDirection (RAISE/CUT/UNCHANGED/INITIATED/null). Max 5 tickers per call.",
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
      "Get overnight trading data for a ticker. Filters for the true overnight window (20:00–04:00 ET / 00:00–08:00 UTC). If no data exists in that window, falls back to the most recent pre-market bar with fallback=true. Returns overnightPrice, overnightTime, overnightHigh, overnightLow, overnightOpen, overnightVolume, previousClose, gapPct, gapDirection, dataSource ('EXCHANGE' for crypto/futures with real volume, 'OTC_INDICATIVE' for equities with zero volume), isBlueOceanWindow, isStale, dataAgeHours, fallback, and note.",
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
    description: "Get options summary for a single ticker: ATM implied volatility, put/call ratio by volume and OI, max pain strike for the nearest liquid expiry. Preferred for LLM use — returns a compact snapshot without the full contract list.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
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
  get_adv_gate: "check_volume_liquidity_threshold",

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
  get_dc134_options_scan: "analyze_options_flow_window",
  get_put_hedge_candidates: "find_put_hedge_candidates",

  get_price_target_bracket: "calculate_price_target_distance",
  get_eqf_bracket: "calculate_price_target_distance",
  get_position_score_inputs: "analyze_position_signals",
  get_tps_inputs: "analyze_position_signals",

  list_sec_filings: "list_sec_company_filings",
  get_filing_outline: "get_sec_filing_outline",
  get_filing_section: "get_sec_filing_section",
  list_filing_tables: "list_sec_filing_tables",
  get_filing_table: "get_sec_filing_table",

  get_filing_data: "extract_sec_filing_fact",
  extract_filing_fact: "extract_sec_filing_fact",
  get_geographic_revenue: "extract_sec_filing_fact",
  get_china_revenue_pct: "extract_sec_filing_fact",

  search_filing_text: "search_sec_filing_text",
  get_filing_text_search: "search_sec_filing_text",
  get_filing_document: "get_sec_filing_section",
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
  { name: "list_sec_filing_tables", description: "List SEC filing tables.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, document_url: { type: "string" } }, required: ["ticker"] } },
  { name: "get_sec_filing_table", description: "Get SEC filing table.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, document_url: { type: "string" }, table_index: { type: "number" }, max_rows: { type: "number", default: 30 } }, required: ["ticker", "table_index"] } },
  { name: "extract_sec_filing_fact", description: "Extract SEC filing fact.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, fact: { type: "string" }, fact_name: { type: "string" }, fact_type: { type: "string" }, region: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, document_url: { type: "string" }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "search_sec_filing_text", description: "Search SEC filing text.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, search_terms: { type: "array", items: { type: "string" } }, search_query: { type: "string" }, section_hint: { type: "string" }, selector: { type: "object" }, filing_type: { type: "string", default: "10-K" }, accession_number: { type: "string" }, context_chars: { type: "number", default: 1500 }, return_tables: { type: "boolean", default: true } }, required: ["ticker"] } },
  { name: "index_sec_filing", description: "Build a deterministic section/table index for an SEC filing. Identifies headings, tables, row labels, and units. period is reserved for future multi-period support; currently only 'latest' is supported unless accession_number is provided.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "Reserved. Only 'latest' supported currently." }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "get_sec_filing_index", description: "Get the pre-built section/table index for an SEC filing. Returns cached index when available; builds and caches on first call. period is reserved for future multi-period support; currently only 'latest' is supported unless accession_number is provided.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest", description: "Reserved. Only 'latest' supported currently." }, accession_number: { type: "string" } }, required: ["ticker"] } },
  { name: "analyze_position_signals", description: "Aggregate public market, analyst, earnings, and technical inputs that may be useful for a caller-defined scoring model. This tool does not access holdings, cost basis, position size, or private scoring rules.", inputSchema: { type: "object", properties: { ticker: { type: "string" } }, required: ["ticker"] } },
  { name: "calculate_price_target_distance", description: "Compare current market price to a user-supplied reference price target and return percentage distance and bracket labels.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, reference_target_price: { type: "number", description: "Preferred: user-supplied reference target price." }, io_pt: { type: "number", description: "Backward-compatible alias for reference_target_price." } }, required: ["ticker"] } },
  { name: "get_company_news", description: "Get recent public company news and press releases from selected public sources with precise source labels (yahoo_finance_news, yahoo_finance_press_releases, finnhub), timestamps, URL, dedupe metadata, and short evidence text.", inputSchema: { type: "object", properties: { ticker: { type: "string", description: "Ticker symbol, e.g. 'AAPL'" }, max_results: { type: "number", default: 10 }, lookback_days: { type: "number", default: 14 }, sources: { type: "array", items: { type: "string" }, default: ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"] } }, required: ["ticker"] } },
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
  { name: "query_sec_filing_index", description: "Deterministically route supported SEC filing query types to index-backed extractor tools.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, filing_type: { type: "string", default: "10-K" }, period: { type: "string", default: "latest" }, accession_number: { type: "string" }, query_type: { type: "string" }, params: { type: "object", default: {} }, return_evidence: { type: "boolean", default: true }, detailLevel: { type: "string", default: "compact", enum: ["compact", "evidence", "raw"] } }, required: ["ticker", "query_type"] } },
  { name: "get_latest_earnings_release", description: "Find the latest public earnings release evidence from SEC 8-K, company IR, or Yahoo earnings calendars.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", default: "latest" } }, required: ["ticker"] } },
  { name: "index_earnings_release", description: "Build a compact section/table index for an earnings release/report source for deterministic follow-up extraction.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", default: "latest" }, source_url: { type: "string" } }, required: ["ticker"] } },
  { name: "extract_earnings_metrics", description: "Extract reported earnings metrics (revenue, EPS, gross margin, operating income, free cash flow, capex) from public sources.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", default: "latest" }, source_preference: { type: "array", items: { type: "string" }, default: ["sec_8k", "company_ir", "10-q", "yahoo"] } }, required: ["ticker"] } },
  { name: "extract_guidance", description: "Extract company-provided guidance/outlook ranges from public earnings release/report sources.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", default: "latest" } }, required: ["ticker"] } },
  { name: "extract_management_commentary", description: "Extract neutral topic-specific management commentary with short evidence excerpts from public earnings release/report sources.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", default: "latest" }, topics: { type: "array", items: { type: "string" } } }, required: ["ticker"] } },
  { name: "compare_earnings_actual_vs_estimate", description: "Compare reported actual earnings metrics versus public analyst estimates and return surprise percentages.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, period: { type: "string", default: "latest" } }, required: ["ticker"] } },
  { name: "get_manifest_diagnostics", description: "Return deployment and manifest diagnostics: tool counts, manifest version, hash, build SHA, deploy timestamp, privacy scope, and connector-staleness advisory.", inputSchema: { type: "object", properties: {} } },
  { name: "get_market_snapshot", description: "Compact market-state packet composing quote, price performance, moving-average trend, volume ratios, liquidity gate, and technical indicators in one call. Supports compact (default) and full modes, and optional batch of tickers.", inputSchema: { type: "object", properties: { ticker: { oneOf: [{ type: "string" }, { type: "array", items: { type: "string" }, maxItems: 5 }] }, mode: { type: "string", enum: ["compact", "full"], default: "compact" }, foreign_exchange: { type: "boolean", default: false } }, required: ["ticker"] } },
  { name: "health_check", description: "Return runtime and deployment health metadata.", inputSchema: { type: "object", properties: {} } },
];

const DEPRECATED_ALIAS_TOOLS: Tool[] = [
  { name: "get_adv_gate", description: "Deprecated alias for check_volume_liquidity_threshold.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, foreign_exchange: { type: "boolean", default: false } }, required: ["ticker"] }, deprecated: true, useInstead: "check_volume_liquidity_threshold", deprecationReason: "Use the canonical public tool name." },
  { name: "get_dc134_options_scan", description: "Deprecated alias for analyze_options_flow_window.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, window_label: { type: "string" } }, required: ["ticker", "window_label"] }, deprecated: true, useInstead: "analyze_options_flow_window", deprecationReason: "Use the canonical public tool name." },
  { name: "get_eqf_bracket", description: "Deprecated alias for calculate_price_target_distance.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, reference_target_price: { type: "number" }, io_pt: { type: "number" } }, required: ["ticker"] }, deprecated: true, useInstead: "calculate_price_target_distance", deprecationReason: "Use the canonical public tool name." },
  { name: "get_tps_inputs", description: "Deprecated alias for analyze_position_signals.", inputSchema: { type: "object", properties: { ticker: { type: "string" } }, required: ["ticker"] }, deprecated: true, useInstead: "analyze_position_signals", deprecationReason: "Use the canonical public tool name." },
  { name: "get_geographic_revenue", description: "Deprecated alias for extract_sec_filing_fact.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, region: { type: "string", default: "China" } }, required: ["ticker"] }, deprecated: true, useInstead: "extract_sec_filing_fact", deprecationReason: "Use the canonical public tool name." },
  { name: "get_china_revenue_pct", description: "Deprecated alias for extract_sec_filing_fact.", inputSchema: { type: "object", properties: { ticker: { type: "string" } }, required: ["ticker"] }, deprecated: true, useInstead: "extract_sec_filing_fact", deprecationReason: "Use the canonical public tool name." },
  { name: "get_filing_text_search", description: "Deprecated alias for search_sec_filing_text.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, search_terms: { type: "array", items: { type: "string" } } }, required: ["ticker"] }, deprecated: true, useInstead: "search_sec_filing_text", deprecationReason: "Use the canonical public tool name." },
  { name: "get_filing_document", description: "Deprecated alias for get_sec_filing_section.", inputSchema: { type: "object", properties: { ticker: { type: "string" }, section_name: { type: "string" }, document_url: { type: "string" }, context_chars: { type: "number", default: 3000 } }, required: ["ticker", "section_name", "document_url"] }, deprecated: true, useInstead: "get_sec_filing_section", deprecationReason: "Use the canonical public tool name." },
];
const DEPRECATED_ALIAS_NAMES = new Set<string>([
  "get_adv_gate",
  "get_dc134_options_scan",
  "get_eqf_bracket",
  "get_tps_inputs",
  "get_geographic_revenue",
  "get_china_revenue_pct",
  "get_filing_text_search",
  "get_filing_document",
]);

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
      netDebtToEbitda: { type: ["number", "null"] },
      interestCoverage: { type: ["number", "null"] },
      debtTier: { type: ["string", "null"] },
      creditStress: { type: ["boolean", "null"] },
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
      tag: { type: ["string", "null"] },
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
  get_latest_earnings_release: SIMPLE_OBJECT_SCHEMA,
  index_earnings_release: SIMPLE_OBJECT_SCHEMA,
  extract_earnings_metrics: SIMPLE_OBJECT_SCHEMA,
  extract_guidance: SIMPLE_OBJECT_SCHEMA,
  extract_management_commentary: SIMPLE_OBJECT_SCHEMA,
  compare_earnings_actual_vs_estimate: SIMPLE_OBJECT_SCHEMA,
};

for (const [alias, canonical] of Object.entries(TOOL_ALIASES)) {
  OUTPUT_SCHEMAS[alias] = OUTPUT_SCHEMAS[canonical] ?? SIMPLE_OBJECT_SCHEMA;
}

for (const tool of TOOLS) {
  tool.outputSchema = OUTPUT_SCHEMAS[tool.name] ?? SIMPLE_OBJECT_SCHEMA;
}


const str = (v: unknown, fallback = ""): string => (typeof v === "string" ? v : fallback);
const num = (v: unknown, fallback: number): number => (typeof v === "number" ? v : fallback);
const tickerArg = (v: unknown): string | string[] =>
  Array.isArray(v) ? v.map(String) : str(v);
type AliasSuccessOptions = {
  canonicalTool: string;
  deprecatedTool?: boolean;
  useInstead?: string;
  partialSuccess?: boolean;
  successCount?: number;
  errorCount?: number;
  warnings?: { code: string; message: string; severity: string }[];
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
  const listed = JSON.parse(await listSecFilings(ticker, filingType, limit)) as Record<string, unknown>;
  const filings = (listed.filings as Record<string, unknown>[]) ?? [];
  const first = filings[0] ?? {};
  return (first.primaryDocumentUrl as string | null) ?? null;
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
  return mcpSuccess(name, raw, batchMeta);
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
        str(args.ticker),
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
      return getOptionsSummary(str(args.ticker));
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
          evidence: parsed.evidence ?? null,
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
      const docUrl = args.document_url != null
        ? str(args.document_url)
        : await resolveSecDocumentUrl(ticker, filingType, 1);
      return listFilingTables(ticker, str(docUrl));
    }
    case "get_sec_filing_table": {
      const ticker = str(args.ticker);
      const filingType = str(args.filing_type ?? args.form_type, "10-K");
      const docUrl = args.document_url != null
        ? str(args.document_url)
        : await resolveSecDocumentUrl(ticker, filingType, 1);
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
      );
    case "index_sec_filing":
      return indexSecFiling(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null);
    case "get_sec_filing_index":
      return getSecFilingIndex(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null);
    case "extract_geographic_revenue":
      return extractGeographicRevenue(str(args.ticker), str(args.region), str(args.filing_type, "10-K"), str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null, str(args.detailLevel, "compact"));
    case "extract_segment_revenue":
      return extractSegmentRevenue(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"));
    case "extract_total_revenue":
      return extractTotalRevenue(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"));
    case "extract_revenue_exposure":
      return extractRevenueExposure(str(args.ticker), str(args.exposure_query), str(args.filing_type, "10-K"), str(args.period, "latest"), str(args.detailLevel, "compact"));
    case "extract_china_exposure":
      return extractChinaExposure(str(args.ticker), str(args.filing_type, "10-K"), str(args.period, "latest"), args.accession_number != null ? str(args.accession_number) : null, str(args.detailLevel, "compact"));
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
      const version = getWorkerVar("SERVER_VERSION") ?? "1.0.0";
      const toolCount = TOOLS.length;
      const canonicalToolCount = TOOLS.filter((t) => t.deprecated !== true).length;
      const deprecatedAliasCount = TOOLS.filter((t) => t.deprecated === true).length;
      const manifestVersion = getWorkerVar("MANIFEST_VERSION") ?? "1";
      const deployedAt = getWorkerVar("DEPLOYED_AT") ?? new Date().toISOString();
      const manifestHash = await computeHash(JSON.stringify(TOOLS.map(t => t.name)));
      return JSON.stringify({
        status: "ok",
        serverVersion: version,
        envelopeV2: getWorkerVar("MCP_ENVELOPE_V2") === "true",
        nodeVersion: "cloudflare-worker",
        toolCount,
        canonicalToolCount,
        deprecatedAliasCount,
        manifestVersion,
        manifestHash,
        schemaHash: manifestHash,
        runtimeHash: await computeHash(version + String(toolCount)),
        buildSha,
        deployedAt,
        generatedAt: new Date().toISOString(),
        privacyScope: "public_market_data_only",
      });
    }
    case "get_manifest_diagnostics": {
      const buildSha = getWorkerVar("BUILD_SHA") ?? "unknown";
      const version = getWorkerVar("SERVER_VERSION") ?? "1.0.0";
      const toolCount = TOOLS.length;
      const canonicalToolCount = TOOLS.filter((t) => t.deprecated !== true).length;
      const deprecatedAliasCount = TOOLS.filter((t) => t.deprecated === true).length;
      const manifestVersion = getWorkerVar("MANIFEST_VERSION") ?? null;
      const deployedAt = getWorkerVar("DEPLOYED_AT") ?? null;
      const manifestHash = await computeHash(JSON.stringify(TOOLS.map(t => t.name)));
      const workerSchemaGeneratedAt = new Date().toISOString();
      return JSON.stringify({
        toolCount,
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
      return getOptionsSummary(str(args.ticker));
    case "get_filing_data":
      return getFilingData(str(args.ticker), str(args.fact_type), args.region != null ? str(args.region) : null, str(args.filing_type, "10-K"), str(args.period, "latest"));
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
      return getOptionsSummary(str(args.ticker));
    case "get_options_flow_scan":
      return getOptionsFlowScan(str(args.ticker), str(args.window_label));
    case "get_put_hedge_candidates":
      return getPutHedgeCandidates(str(args.ticker), num(args.otm_pct_min, 8), num(args.otm_pct_max, 12), num(args.budget_usd, 500), str(args.expiry_after));
    case "get_price_target_bracket":
      return getPriceTargetBracket(str(args.ticker), num(args.io_pt, 0));
    case "get_position_score_inputs":
      return getPositionScoreInputs(tickerArg(args.ticker));
    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}
