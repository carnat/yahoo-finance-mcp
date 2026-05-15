import { mcpSuccess } from "./response.js";
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
  getNews,
  getOptionChain,
  getOptionExpirationDates,
  getOptionsFlowSummary,
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
} from "./yahoo-finance.js";

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
    description: "Get the latest news articles for a given stock ticker.",
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
      "Get the options chain (calls or puts) for a ticker and expiration date. Use get_option_expiration_dates first to find valid dates. Response is wrapped: { ticker, expiration, optionType, dataDate (YYYY-MM-DD last trading day), contracts: [...] }.",
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
      "Get lightweight real-time price and market data for one or more tickers. Returns high-signal fields: currency, exchange, quoteType, lastPrice, open, previousClose, dayHigh, dayLow, yearHigh, yearLow, yearChange, marketCap, shares, lastVolume, tenDayAverageVolume, threeMonthAverageVolume, fiftyDayAverage, twoHundredDayAverage, preMarketPrice, postMarketPrice, marketOpen (true only during regular session hours), lastTradeDate (YYYY-MM-DD date of the session the OHLCV data belongs to — use this to detect weekend/holiday staleness), postMarketTimestamp (ISO8601 timestamp of postMarketPrice, null if no AH activity). Prefer this over get_stock_info for price/market data queries — it uses far fewer tokens. Max 5 tickers per call; if you need more, split into multiple calls.",
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
      "Get upcoming earnings and dividend schedule for a ticker: next earnings date range, EPS/revenue consensus estimates, ex-dividend date, and dividend pay date. Also returns earningsDateConfirmed (true = single fixed date from IR, false = analyst estimate range) and earningsDateSource ('IR_FILING' | 'ESTIMATE' | 'UNKNOWN'). DC-149 PCCE Rule 9 requires earningsDateConfirmed=true before entry.",
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
      "Get earnings revision momentum, beat rate, and estimate direction signals. Returns revision7d/30d/90d, momentumFlag, beatRate, currentBeatStreak. Max 5 tickers per call; split larger lists into multiple calls.",
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
      "Compute EQF bracket for a position. EQF = currentPrice / io_pt × 100. Brackets: ≤75% STRONG_BUY | 75–90% ACCEPTABLE | 90–100% CAUTION | >100% AVOID. Tags: <40% SPECULATIVE | 40–79% LONG | 80–99% NEAR | ≥100% INVERTED. Returns currentPrice, ioPt, eqfPct, bracket, tag, invertedFlag, dataDate.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'ASTS'" },
        io_pt: { type: "number", description: "IO price target (Commander/IO-set, not analyst consensus)" },
      },
      required: ["ticker", "io_pt"],
    },
  },
  {
    name: "get_position_score_inputs",
    description:
      "Aggregate all position scoring inputs for T1, T2, T4, and T5 in a single call. T3 (PT proximity) and T2 (vs cost basis) require portfolio state — IO scores those manually. Runs 6 parallel data fetches. Returns t1_inputs (analyst sentiment), t2_inputs (price vs 52wk), t4_inputs (earnings momentum), t5_inputs (technical indicators), dataDate.",
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
      "DC Section 6.2 Volume Gate: checks regularMarketVolume ≥ 0.5 × 20-day ADV. Returns currency, fxRate, lastVolume, adv10d, adv20d (computed from last 20 sessions), adv90d, ratio20d, gatePass (true = PASS), dataDate, note. Set foreign_exchange=true for DC-80 FX gate: daily notional is converted to USD via live {CCY}=X rate before comparing to the $10M threshold. ratio20d is always computed when adv20d is available.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'ASTS'" },
        foreign_exchange: {
          type: "boolean",
          description: "Set true for DC-80 FX/ADR tickers to use $10M notional threshold. Default false.",
          default: false,
        },
      },
      required: ["ticker"],
    },
  },
];

const SIMPLE_OBJECT_SCHEMA: Tool["outputSchema"] = {
  type: "object",
  properties: {},
  additionalProperties: true,
};

const OUTPUT_SCHEMAS: Record<string, Tool["outputSchema"]> = {
  get_historical_stock_prices: SIMPLE_OBJECT_SCHEMA,
  get_stock_info: SIMPLE_OBJECT_SCHEMA,
  get_etf_info: SIMPLE_OBJECT_SCHEMA,
  get_yahoo_finance_news: SIMPLE_OBJECT_SCHEMA,
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
  get_options_flow_summary: SIMPLE_OBJECT_SCHEMA,
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
      source: { type: "string" },
      confidence: { type: "string" },
      value: {},
      currency: { type: ["string", "null"] },
      period: { type: ["string", "null"] },
      filingType: { type: ["string", "null"] },
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
      formattedBlock: { type: ["string", "null"] },
      dataDate: { type: "string" },
    },
    additionalProperties: true,
  },
  get_price_target_bracket: {
    type: "object",
    properties: {
      ticker: { type: "string" },
      currentPrice: { type: ["number", "null"] },
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
};

for (const tool of TOOLS) {
  tool.outputSchema = OUTPUT_SCHEMAS[tool.name] ?? SIMPLE_OBJECT_SCHEMA;
}


const str = (v: unknown, fallback = ""): string => (typeof v === "string" ? v : fallback);
const num = (v: unknown, fallback: number): number => (typeof v === "number" ? v : fallback);
const tickerArg = (v: unknown): string | string[] =>
  Array.isArray(v) ? v.map(String) : str(v);

export async function callTool(name: string, args: Record<string, unknown>): Promise<string> {
  const raw = await _dispatchTool(name, args);
  return mcpSuccess(name, raw);
}

async function _dispatchTool(name: string, args: Record<string, unknown>): Promise<string> {
  switch (name) {
    case "get_historical_stock_prices":
      return getHistoricalPrices(str(args.ticker), str(args.period, "1mo"), str(args.interval, "1d"), args.prepost === true);
    case "get_stock_info":
      return getStockInfo(tickerArg(args.ticker), args.include_all === true);
    case "get_etf_info":
      return getEtfInfo(tickerArg(args.ticker));
    case "get_yahoo_finance_news":
      return getNews(str(args.ticker));
    case "get_stock_actions":
      return getStockActions(str(args.ticker));
    case "get_financial_statement":
      return getFinancialStatement(str(args.ticker), str(args.financial_type));
    case "get_holder_info":
      return getHolderInfo(str(args.ticker), str(args.holder_type));
    case "get_option_expiration_dates":
      return getOptionExpirationDates(str(args.ticker));
    case "get_option_chain":
      return getOptionChain(str(args.ticker), str(args.expiration_date), str(args.option_type));
    case "get_recommendations":
      return getRecommendations(
        str(args.ticker),
        str(args.recommendation_type),
        num(args.months_back, 12)
      );
    case "get_fast_info":
      return getFastInfo(tickerArg(args.ticker));
    case "get_price_stats":
      return getPriceStats(tickerArg(args.ticker));
    case "get_analyst_consensus":
      return getAnalystConsensus(tickerArg(args.ticker));
    case "get_earnings_analysis":
      return getEarningsAnalysis(str(args.ticker));
    case "get_financial_ratios":
      return getFinancialRatios(tickerArg(args.ticker));
    case "get_calendar":
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
    case "get_volume_ratio":
      return getVolumeRatio(tickerArg(args.ticker), num(args.period, 10));
    case "get_ma_position":
      return getMaPosition(tickerArg(args.ticker));
    case "get_credit_health":
      return getCreditHealth(tickerArg(args.ticker));
    case "get_short_momentum":
      return getShortMomentum(tickerArg(args.ticker));
    case "get_earnings_momentum":
      return getEarningsMomentum(tickerArg(args.ticker));
    case "get_options_flow_summary":
      return getOptionsFlowSummary(str(args.ticker), args.expiry_hint != null ? str(args.expiry_hint) : undefined);
    case "get_put_hedge_candidates":
      return getPutHedgeCandidates(
        str(args.ticker),
        num(args.otm_pct_min, 8),
        num(args.otm_pct_max, 12),
        num(args.budget_usd, 500),
        str(args.expiry_after)
      );
    case "get_analyst_upgrade_radar":
      return getAnalystUpgradeRadar(tickerArg(args.ticker), num(args.days_back, 30));
    case "get_overnight_quote":
      return getOvernightQuote(str(args.ticker));

    case "get_filing_data":
      return getFilingData(
        str(args.ticker),
        str(args.fact_type),
        args.region != null ? str(args.region) : null,
        str(args.filing_type, "10-K"),
        str(args.period, "latest"),
      );
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
    case "get_options_flow_scan":
      return getOptionsFlowScan(str(args.ticker), str(args.window_label));
    case "get_price_target_bracket":
      return getPriceTargetBracket(str(args.ticker), num(args.io_pt, 0));
    case "get_position_score_inputs":
      return getPositionScoreInputs(str(args.ticker));
    case "get_volume_gate":
      return getVolumeGate(str(args.ticker), args.foreign_exchange === true);
    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}
