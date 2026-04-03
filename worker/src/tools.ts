import {
  getAnalystConsensus,
  getCalendar,
  getEarningsAnalysis,
  getFastInfo,
  getFinancialRatios,
  getFinancialStatement,
  getHistoricalPrices,
  getHolderInfo,
  getNews,
  getOptionChain,
  getOptionExpirationDates,
  getPriceStats,
  getRecommendations,
  screenStocks,
  searchTicker,
  getSecFilings,
  getStockActions,
  getStockInfo,
  getSustainability,
} from "./yahoo-finance.js";

export interface Tool {
  name: string;
  description: string;
  inputSchema: {
    type: "object";
    properties: Record<string, unknown>;
    required?: string[];
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
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_stock_info",
    description:
      "Get comprehensive stock information for a ticker: price & trading info, company details, financial metrics, earnings, margins, dividends, balance sheet, ownership, analyst coverage, and risk metrics.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
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
      "Get the options chain (calls or puts) for a ticker and expiration date. Use get_option_expiration_dates first to find valid dates.",
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
      "Get lightweight real-time price and market data for a ticker. Returns ~20 high-signal fields: currency, exchange, quoteType, timezone, lastPrice, open, previousClose, dayHigh, dayLow, yearHigh, yearLow, yearChange, marketCap, shares, lastVolume, tenDayAverageVolume, threeMonthAverageVolume, fiftyDayAverage, twoHundredDayAverage. Prefer this over get_stock_info for price/market data queries — it uses far fewer tokens.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_price_stats",
    description:
      "Get pre-computed price statistics for a ticker: current price, % change vs previous close, % distance from 52-week high/low and 50/200-day moving averages, 30-day annualized volatility, and CAGR over 1y/3y/5y.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_analyst_consensus",
    description:
      "Get analyst consensus summary for a ticker: price targets (current, low, high, mean, median) with % upside from last price, recommendation breakdown (strongBuy, buy, hold, sell, strongSell counts), and dominant rating.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
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
      "Get pre-computed key financial ratios for a ticker. Includes: P/E (trailing & forward), P/S, P/B, EV/EBITDA, EV/Revenue, PEG; gross/operating/net margins, ROE, ROA; debt/equity, current ratio, quick ratio; FCF and FCF yield; dividend yield and payout ratio.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_calendar",
    description:
      "Get upcoming earnings and dividend schedule for a ticker: next earnings date range, EPS/revenue consensus estimates, ex-dividend date, and dividend pay date.",
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
      "Search for ticker symbols by company name, partial name, or ISIN. Returns matching quotes with symbol, short name, exchange, and type. Use this to resolve a company name to a ticker before calling other tools.",
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
    name: "get_sustainability",
    description:
      "Get ESG (Environmental, Social, Governance) sustainability scores for a ticker: environment, social, and governance scores, total ESG score, ESG performance category, controversy level, and peer group percentile.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
  {
    name: "get_sec_filings",
    description:
      "Get recent SEC filings for a ticker (10-K, 10-Q, 8-K, etc.) with form type, filing date, and URL.",
    inputSchema: {
      type: "object",
      properties: {
        ticker: { type: "string", description: "Stock ticker symbol, e.g. 'AAPL'" },
      },
      required: ["ticker"],
    },
  },
];

const str = (v: unknown, fallback = ""): string => (typeof v === "string" ? v : fallback);
const num = (v: unknown, fallback: number): number => (typeof v === "number" ? v : fallback);

export async function callTool(name: string, args: Record<string, unknown>): Promise<string> {
  switch (name) {
    case "get_historical_stock_prices":
      return getHistoricalPrices(str(args.ticker), str(args.period, "1mo"), str(args.interval, "1d"));
    case "get_stock_info":
      return getStockInfo(str(args.ticker));
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
      return getFastInfo(str(args.ticker));
    case "get_price_stats":
      return getPriceStats(str(args.ticker));
    case "get_analyst_consensus":
      return getAnalystConsensus(str(args.ticker));
    case "get_earnings_analysis":
      return getEarningsAnalysis(str(args.ticker));
    case "get_financial_ratios":
      return getFinancialRatios(str(args.ticker));
    case "get_calendar":
      return getCalendar(str(args.ticker));
    case "search_ticker":
      return searchTicker(str(args.query), num(args.max_results, 8));
    case "screen_stocks":
      return screenStocks(str(args.screener_name), num(args.count, 25));
    case "get_sustainability":
      return getSustainability(str(args.ticker));
    case "get_sec_filings":
      return getSecFilings(str(args.ticker));
    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}
