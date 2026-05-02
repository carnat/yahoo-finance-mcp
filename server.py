import asyncio
import datetime
import json
import time
from enum import Enum

import pandas as pd
import yfinance as yf
from mcp.server.fastmcp import FastMCP


# Define an enum for the type of financial statement
class FinancialType(str, Enum):
    income_stmt = "income_stmt"
    quarterly_income_stmt = "quarterly_income_stmt"
    ttm_income_stmt = "ttm_income_stmt"
    balance_sheet = "balance_sheet"
    quarterly_balance_sheet = "quarterly_balance_sheet"
    cashflow = "cashflow"
    quarterly_cashflow = "quarterly_cashflow"
    ttm_cashflow = "ttm_cashflow"


class HolderType(str, Enum):
    major_holders = "major_holders"
    institutional_holders = "institutional_holders"
    mutualfund_holders = "mutualfund_holders"
    insider_transactions = "insider_transactions"
    insider_purchases = "insider_purchases"
    insider_roster_holders = "insider_roster_holders"


class RecommendationType(str, Enum):
    recommendations = "recommendations"
    upgrades_downgrades = "upgrades_downgrades"


# ---------------------------------------------------------------------------
# In-memory cache with TTL
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[float, str]] = {}  # key -> (stored_at, json_str)
_PRICE_TTL = 15 * 60        # 15 minutes for price / historical data
_STMT_TTL  = 24 * 3600      # 24 hours for financial statements


def _cache_get(key: str, ttl: float) -> str | None:
    entry = _cache.get(key)
    if entry and (time.monotonic() - entry[0]) < ttl:
        return entry[1]
    return None


def _cache_set(key: str, value: str) -> None:
    _cache[key] = (time.monotonic(), value)


async def _fetch_with_retry(fn, *args, retries: int = 1, delay: float = 2.0, **kwargs):
    """Call fn(*args, **kwargs) with one retry on exception, waiting `delay` seconds."""
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception:
            if attempt < retries:
                await asyncio.sleep(delay)
            else:
                raise


def get_last_trading_date(df=None) -> str:
    """Returns the last trading date as a YYYY-MM-DD string.
    Uses the last DataFrame index row if provided.
    Falls back to the last weekday from the UTC system clock.
    Note: does not account for market holidays — weekday fallback only.
    """
    if df is not None and len(df) > 0:
        return df.index[-1].strftime('%Y-%m-%d')
    d = datetime.datetime.utcnow().date()
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= datetime.timedelta(days=1)
    return d.strftime('%Y-%m-%d')


def _safe_parse(result: object, ticker: str) -> object:
    """Parse a JSON result string, returning a structured error dict on failure.

    Handles both Exception objects returned by asyncio.gather(return_exceptions=True)
    and plain error strings returned by single-ticker handlers.
    """
    if isinstance(result, Exception):
        return {"error": True, "message": str(result), "ticker": ticker}
    try:
        return json.loads(result)  # type: ignore[arg-type]
    except Exception:
        return {"error": True, "message": str(result), "ticker": ticker}


# Fields that only apply to ETFs, mutual funds, or crypto — stripped from EQUITY responses
# to reduce payload size and prevent downstream misinterpretation.
_EQUITY_EXCLUDED_FIELDS: frozenset[str] = frozenset({
    "yield", "ytdReturn", "qtdReturn", "totalAssets", "expireDate",
    "strikePrice", "openInterest", "navPrice", "volume24Hr",
    "volumeAllCurrencies", "circulatingSupply", "algorithm", "maxSupply",
    "totalSupply", "startDate", "fullyDilutedValue", "volume24HrMarketCapPercent",
    "morningStarOverallRating", "morningStarRiskRating", "category",
    "beta3Year", "fundFamily", "fundInceptionDate", "legalType",
    "threeYearAverageReturn", "fiveYearAverageReturn", "annualHoldingsTurnover",
    "annualReportExpenseRatio", "latestFundingDate", "latestAmountRaised",
    "latestImpliedValuation", "latestShareClass", "leadInvestor",
    "fundingToDate", "totalFundingRounds", "coinMarketCapLink",
    "fromCurrency", "toCurrency", "lastMarket", "lastCapGain",
})

# ~30-field default summary returned by get_stock_info when include_all=False and no fields filter.
_STOCK_INFO_DEFAULT_FIELDS: tuple[str, ...] = (
    # Identity
    "shortName", "longName", "sector", "industry", "country", "website", "fullTimeEmployees",
    # Price / market
    "currentPrice", "previousClose", "marketCap", "enterpriseValue", "currency",
    # Valuation
    "trailingPE", "forwardPE", "priceToBook", "priceToSalesTrailing12Months", "enterpriseToEbitda",
    # Earnings
    "trailingEps", "forwardEps", "revenueGrowth", "earningsGrowth",
    # Quality
    "grossMargins", "operatingMargins", "profitMargins", "returnOnEquity", "returnOnAssets",
    # Dividends
    "dividendYield", "payoutRatio",
    # Analyst
    "recommendationMean", "numberOfAnalystOpinions", "targetMeanPrice",
    # Description
    "longBusinessSummary",
)

# Named field-group aliases accepted in the `fields` parameter.
_STOCK_INFO_FIELD_GROUPS: dict[str, tuple[str, ...]] = {
    "identity":     ("shortName", "longName", "sector", "industry", "country", "website", "fullTimeEmployees"),
    "pricing":      ("currentPrice", "previousClose", "marketCap", "enterpriseValue", "currency"),
    "valuation":    ("trailingPE", "forwardPE", "priceToBook", "priceToSalesTrailing12Months", "enterpriseToEbitda"),
    "earnings":     ("trailingEps", "forwardEps", "revenueGrowth", "earningsGrowth"),
    "margins":      ("grossMargins", "operatingMargins", "profitMargins", "returnOnEquity", "returnOnAssets"),
    "dividends":    ("dividendYield", "payoutRatio"),
    "analyst":      ("recommendationMean", "numberOfAnalystOpinions", "targetMeanPrice"),
    "description":  ("longBusinessSummary",),
}


# Initialize FastMCP server
yfinance_server = FastMCP(
    "yfinance",
    instructions="""
# Yahoo Finance MCP Server

This server provides financial market data from Yahoo Finance.

## Tool selection guidance
- **Prefer `get_fast_info`** over `get_stock_info` for current price, market cap, 52-week range, or moving averages — it returns ~20 fields instead of 120+ and uses far fewer tokens. Also includes pre-market / after-hours prices when available.
- Use `get_stock_info` only when you need deep fundamentals, business description, or fields not in fast_info. For ETFs or mutual funds, use `get_etf_info` instead.
- **Use `get_etf_info`** for ETFs and mutual funds — returns NAV, expense ratio, AUM, YTD return, top-10 holdings, and sector weights. Covers tickers like SPY, QQQ, VTI, ARKK, etc.
- **Prefer `get_financial_ratios`** over fetching full financial statements when you need valuation or profitability ratios — ratios are pre-computed server-side.
- **Prefer `get_analyst_consensus`** over `get_recommendations` when you need a quick summary of analyst sentiment and price targets.
- **Prefer `get_earnings_analysis`** to get all forward-looking analyst estimates in a single call instead of five separate calls.
- Use `get_short_interest` for short-selling metrics (short % of float, days to cover, etc.).
- Use `get_technical_indicators` for momentum signals (RSI-14, MACD) without fetching raw OHLCV history.
- Use `search_ticker` to resolve a company name or ISIN to a ticker symbol before calling other tools.
- Use `screen_stocks` to discover stocks matching criteria (e.g., day_gainers, most_actives) without iterating tickers manually.
- Index tickers like `^VIX`, `^GSPC`, `^DJI` are supported by `get_fast_info`, `get_price_stats`, and `get_technical_indicators`.

## Available tools

### Price & market data
- get_fast_info: **Lightweight** — current price, market cap, 52-week high/low, moving averages, volume, market state (marketOpen, lastTradeDate, postMarketTimestamp), plus pre-market/after-hours prices when available. Prefer this for price lookups.
- get_historical_stock_prices: OHLCV history. Supports period, interval, and optional columns filter to reduce output size.
- get_stock_info: **Fundamentals** — returns ~30 key fields by default (identity, price, valuation, earnings, margins, dividends, analyst ratings, business description). Pass include_all=true for the full ~120-field payload. Use only when fast_info is insufficient. For ETFs/funds, use get_etf_info instead.
- get_etf_info: **ETF/fund data** — NAV, expense ratio, AUM, YTD return, 52-week stats, moving averages, top-10 holdings, and sector weights. Use for SPY, QQQ, VTI, ARKK, and any mutual fund ticker.
- get_price_stats: Pre-computed price statistics: % change vs 52-week high/low, distance from moving averages, 30-day volatility, and CAGR.
- get_stock_actions: Dividend and split history.
- get_short_interest: Short interest metrics: short % of float, shares short, days-to-cover ratio, float shares.
- get_overnight_quote: Overnight session OHLCV. Filters for the true overnight window (20:00–04:00 ET / 00:00–08:00 UTC). Falls back to last pre-market bar for equities with a fallback flag. Returns dataSource (EXCHANGE or OTC_INDICATIVE), isBlueOceanWindow, isStale, dataAgeHours, gapPct, and gapDirection.

### Financials & ratios
- get_financial_statement: Raw financial statements (income, balance sheet, cashflow). Supports ttm_income_stmt and ttm_cashflow for trailing-twelve-months data. Supports optional line_items filter.
- get_financial_ratios: **Pre-computed** valuation, profitability, and leverage ratios (P/E, P/S, P/B, EV/EBITDA, margins, ROE, D/E, etc.). Prefer over fetching full statements.

### Analyst & forecasts
- get_analyst_consensus: Compact analyst price targets + recommendation breakdown. Prefer over get_recommendations.
- get_earnings_analysis: All analyst forward estimates in one call: EPS/revenue estimates, EPS trend, earnings history, growth estimates.
- get_recommendations: Raw recommendations or upgrades/downgrades table.
- get_calendar: Next earnings date, EPS/revenue guidance, and next dividend dates.

### Holders & ownership
- get_holder_info: Major, institutional, mutual fund holders; insider transactions and purchases.

### Options
- get_option_expiration_dates: Available expiration dates for options.
- get_option_chain: Option chain (calls or puts). Supports min_strike, max_strike, and in_the_money_only filters to reduce output size.

### News & filings
- get_yahoo_finance_news: Latest news articles.
- get_sec_filings: Recent SEC filings (10-K, 10-Q, 8-K) with dates and links.

### Technical analysis
- get_technical_indicators: Pre-computed RSI-14 and MACD (12,26,9) from historical daily prices. Use for momentum/oversold screening without fetching raw history.

### Discovery
- search_ticker: Search by company name, partial name, or ISIN to get matching ticker symbols.
- screen_stocks: Screen the market using predefined or custom criteria. Predefined: aggressive_small_caps, day_gainers, day_losers, growth_technology_stocks, most_actives, most_shorted_stocks, small_cap_gainers, undervalued_growth_stocks, undervalued_large_caps, conservative_foreign_funds, high_yield_bond, portfolio_anchors, solid_large_growth_funds, solid_midcap_growth_funds, top_mutual_funds.
""",
)


@yfinance_server.tool(
    name="get_historical_stock_prices",
    description="""Get historical stock prices for a given ticker symbol from yahoo finance. Include the following information: Date, Open, High, Low, Close (adjusted), Volume.
Args:
    ticker: str
        The ticker symbol of the stock to get historical prices for, e.g. "AAPL"
    period : str
        Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        Either Use period parameter or use start and end
        Default is "1mo"
    interval : str
        Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        Intraday data cannot extend last 60 days
        Default is "1d"
    columns : list[str] | None
        Optional list of OHLCV column names to return, e.g. ["Close"] or ["Close","Volume"].
        Valid column names: Open, High, Low, Close, Volume, Dividends, Stock Splits.
        If omitted, all columns are returned.
        Tip: request only "Close" when you only need price trend data — this reduces response size significantly.
    prepost : bool
        If True, includes pre-market and after-hours data rows.
        Only meaningful with intraday intervals (1m–90m) and period ≤ 60d.
        Default is False.
""",
)
async def get_historical_stock_prices(
    ticker: str, period: str = "1mo", interval: str = "1d",
    columns: list[str] | None = None, prepost: bool = False,
) -> str:
    """Get historical stock prices for a given ticker symbol

    Args:
        ticker: str
            The ticker symbol of the stock to get historical prices for, e.g. "AAPL"
        period : str
            Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            Either Use period parameter or use start and end
            Default is "1mo"
        interval : str
            Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            Intraday data cannot extend last 60 days
            Default is "1d"
        columns : list[str] | None
            Optional subset of columns to return. If None, all columns are returned.
    """
    cache_key = f"hist:{ticker}:{period}:{interval}:{prepost}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
        if columns:
            try:
                rows = json.loads(cached)
                filtered = [{k: r[k] for k in ["Date"] + columns if k in r} for r in rows]
                return json.dumps(filtered)
            except Exception:
                pass
        return cached

    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting historical stock prices for {ticker}: {e}")
        return f"Error: getting historical stock prices for {ticker}: {e}"

    try:
        hist_data = await _fetch_with_retry(company.history, period, interval, prepost=prepost)
    except Exception as e:
        print(f"Error: getting historical stock prices for {ticker}: {e}")
        return f"Error: getting historical stock prices for {ticker}: {e}"

    hist_data = hist_data.reset_index(names="Date")
    full_result = hist_data.to_json(orient="records", date_format="iso")
    _cache_set(cache_key, full_result)

    if columns:
        try:
            rows = json.loads(full_result)
            filtered = [{k: r[k] for k in ["Date"] + columns if k in r} for r in rows]
            return json.dumps(filtered)
        except Exception:
            pass
    return full_result


@yfinance_server.tool(
    name="get_stock_info",
    description="""Get stock fundamentals for one or more ticker symbols from Yahoo Finance.

By default returns ~30 key fields covering identity, price, valuation, earnings, margins,
dividends, analyst ratings, and the business description — enough for most queries at a
fraction of the token cost of the full payload.

Pass include_all=true only when you specifically need fields outside the default set (e.g.
raw balance-sheet items, governance scores, or insider-ownership details).

For ETFs or mutual funds (SPY, QQQ, VTI, ARKK, etc.), use get_etf_info instead — it returns
fund-specific fields including NAV, expense ratio, top-10 holdings, and sector weights.

Default fields (~30): shortName, longName, sector, industry, country, website,
fullTimeEmployees, currentPrice, previousClose, marketCap, enterpriseValue, currency,
trailingPE, forwardPE, priceToBook, priceToSalesTrailing12Months, enterpriseToEbitda,
trailingEps, forwardEps, revenueGrowth, earningsGrowth, grossMargins, operatingMargins,
profitMargins, returnOnEquity, returnOnAssets, dividendYield, payoutRatio,
recommendationMean, numberOfAnalystOpinions, targetMeanPrice, longBusinessSummary.

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of symbols (e.g. ["AAPL", "MSFT"]).
        When a list is provided, returns a dict keyed by symbol.
    fields: list[str] | None
        Optional list of exact field names or group aliases to return.
        Group aliases: "identity", "pricing", "valuation", "earnings", "margins",
        "dividends", "analyst", "description".
        Mixing aliases and exact names is supported, e.g. ["pricing", "trailingPE"].
        Ignored when include_all=true.
    include_all: bool
        Set to true to return the full ~120-field payload. Default is false.
""",
)
async def get_stock_info(
    ticker: str | list[str],
    fields: list[str] | None = None,
    include_all: bool = False,
) -> str:
    """Get stock information for a given ticker symbol"""
    if isinstance(ticker, list):
        results = await asyncio.gather(
            *[get_stock_info(t, fields, include_all) for t in ticker],
            return_exceptions=True,
        )
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting stock information for {ticker}: {e}")
        return f"Error: getting stock information for {ticker}: {e}"
    info = company.info
    # Strip ETF/crypto/fund fields from equity responses to reduce payload size
    if info.get("quoteType") == "EQUITY":
        info = {k: v for k, v in info.items() if k not in _EQUITY_EXCLUDED_FIELDS}
    if not include_all:
        if fields:
            # Expand any group aliases, then de-duplicate while preserving order
            expanded: list[str] = []
            seen: set[str] = set()
            for f in fields:
                group = _STOCK_INFO_FIELD_GROUPS.get(f)
                items = group if group is not None else (f,)
                for item in items:
                    if item not in seen:
                        seen.add(item)
                        expanded.append(item)
            info = {k: info[k] for k in expanded if k in info}
        else:
            info = {k: info[k] for k in _STOCK_INFO_DEFAULT_FIELDS if k in info}
    return json.dumps(info)


@yfinance_server.tool(
    name="get_yahoo_finance_news",
    description="""Get news for a given ticker symbol from yahoo finance.

Args:
    ticker: str
        The ticker symbol of the stock to get news for, e.g. "AAPL"
""",
)
async def get_yahoo_finance_news(ticker: str) -> str:
    """Get news for a given ticker symbol

    Args:
        ticker: str
            The ticker symbol of the stock to get news for, e.g. "AAPL"
    """
    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting news for {ticker}: {e}")
        return f"Error: getting news for {ticker}: {e}"

    # If the company is found, get the news
    try:
        news = company.news
    except Exception as e:
        print(f"Error: getting news for {ticker}: {e}")
        return f"Error: getting news for {ticker}: {e}"

    news_list = []
    for news in company.news:
        if news.get("content", {}).get("contentType", "") == "STORY":
            title = news.get("content", {}).get("title", "")
            summary = news.get("content", {}).get("summary", "")
            description = news.get("content", {}).get("description", "")
            url = news.get("content", {}).get("canonicalUrl", {}).get("url", "")
            news_list.append(
                f"Title: {title}\nSummary: {summary}\nDescription: {description}\nURL: {url}"
            )
    if not news_list:
        print(f"No news found for company that searched with {ticker} ticker.")
        return f"No news found for company that searched with {ticker} ticker."
    return "\n\n".join(news_list)


@yfinance_server.tool(
    name="get_stock_actions",
    description="""Get stock dividends and stock splits for a given ticker symbol from yahoo finance.

Args:
    ticker: str
        The ticker symbol of the stock to get stock actions for, e.g. "AAPL"
""",
)
async def get_stock_actions(ticker: str) -> str:
    """Get stock dividends and stock splits for a given ticker symbol"""
    try:
        company = yf.Ticker(ticker)
    except Exception as e:
        print(f"Error: getting stock actions for {ticker}: {e}")
        return f"Error: getting stock actions for {ticker}: {e}"
    actions_df = company.actions
    actions_df = actions_df.reset_index(names="Date")
    return actions_df.to_json(orient="records", date_format="iso")


@yfinance_server.tool(
    name="get_financial_statement",
    description="""Get financial statement for a given ticker symbol from yahoo finance.

Financial statement types:
- income_stmt: Annual income statement (4 years)
- quarterly_income_stmt: Quarterly income statement (4 quarters)
- ttm_income_stmt: Trailing-twelve-months income statement (1 column — use this for recency without 4x the data)
- balance_sheet: Annual balance sheet (4 years)
- quarterly_balance_sheet: Quarterly balance sheet (4 quarters)
- cashflow: Annual cash flow statement (4 years)
- quarterly_cashflow: Quarterly cash flow statement (4 quarters)
- ttm_cashflow: Trailing-twelve-months cash flow (1 column)

Tip: Use ttm_income_stmt or ttm_cashflow to reduce token usage by ~75% when you only care about the most recent period.
Use the optional line_items parameter to filter to only the rows you need.

Args:
    ticker: str
        The ticker symbol of the stock to get financial statement for, e.g. "AAPL"
    financial_type: str
        The type of financial statement to get (see types above).
    line_items: list[str] | None
        Optional list of line item names to return, e.g. ["Total Revenue", "Net Income", "EBITDA"].
        If omitted, all line items are returned. Specify only what you need to reduce token usage.
""",
)
async def get_financial_statement(
    ticker: str, financial_type: str, line_items: list[str] | None = None
) -> str:
    """Get financial statement for a given ticker symbol"""

    # Check cache first
    cache_key = f"stmt:{ticker}:{financial_type}"
    cached = _cache_get(cache_key, _STMT_TTL)
    if cached is not None:
        if line_items:
            try:
                rows = json.loads(cached)
                filtered = [r for r in rows if r.get("lineItem") in line_items]
                return json.dumps(filtered)
            except Exception:
                pass
        return cached

    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting financial statement for {ticker}: {e}")
        return f"Error: getting financial statement for {ticker}: {e}"

    _freq_map = {
        FinancialType.income_stmt:             ("yearly",    "income"),
        FinancialType.quarterly_income_stmt:   ("quarterly", "income"),
        FinancialType.ttm_income_stmt:         ("trailing",  "income"),
        FinancialType.balance_sheet:           ("yearly",    "balance"),
        FinancialType.quarterly_balance_sheet: ("quarterly", "balance"),
        FinancialType.cashflow:                ("yearly",    "cashflow"),
        FinancialType.quarterly_cashflow:      ("quarterly", "cashflow"),
        FinancialType.ttm_cashflow:            ("trailing",  "cashflow"),
    }
    if financial_type not in _freq_map:
        return (
            f"Error: invalid financial type {financial_type}. Please use one of: "
            + ", ".join(e.value for e in FinancialType)
        )

    freq, stmt_kind = _freq_map[financial_type]

    def _fetch_stmt(c):
        if stmt_kind == "income":
            return c.get_income_stmt(freq=freq, pretty=True)
        elif stmt_kind == "balance":
            return c.get_balance_sheet(freq=freq, pretty=True)
        else:
            return c.get_cashflow(freq=freq, pretty=True)

    try:
        df = await _fetch_with_retry(_fetch_stmt, company)
    except Exception as e:
        print(f"Error: getting financial statement for {ticker}: {e}")
        return f"Error: getting financial statement for {ticker}: {e}"

    if df is None or df.empty:
        return json.dumps([])

    # CRITICAL: yfinance financial DataFrames have line items (e.g. "Gross
    # Profit") as the INDEX, not as a column.  reset_index() promotes them
    # into an ordinary column so they appear in the serialised output.
    df = df.reset_index()
    df = df.rename(columns={df.columns[0]: "lineItem"})

    # Date columns are pandas Timestamps — convert to plain YYYY-MM-DD strings.
    df.columns = [
        c.strftime("%Y-%m-%d") if hasattr(c, "strftime") else str(c)
        for c in df.columns
    ]

    # pandas uses NaN for missing values; replace with None for valid JSON.
    df = df.where(pd.notnull(df), None)
    result = json.dumps(df.to_dict(orient="records"))

    _cache_set(cache_key, result)

    if line_items:
        try:
            rows = json.loads(result)
            filtered = [r for r in rows if r.get("lineItem") in line_items]
            return json.dumps(filtered)
        except Exception:
            pass
    return result


@yfinance_server.tool(
    name="get_holder_info",
    description="""Get holder information for a given ticker symbol from yahoo finance. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.

Args:
    ticker: str
        The ticker symbol of the stock to get holder information for, e.g. "AAPL"
    holder_type: str
        The type of holder information to get. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.
""",
)
async def get_holder_info(ticker: str, holder_type: str) -> str:
    """Get holder information for a given ticker symbol"""

    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting holder info for {ticker}: {e}")
        return f"Error: getting holder info for {ticker}: {e}"

    if holder_type == HolderType.major_holders:
        return company.major_holders.reset_index(names="metric").to_json(orient="records")
    elif holder_type == HolderType.institutional_holders:
        return company.institutional_holders.to_json(orient="records")
    elif holder_type == HolderType.mutualfund_holders:
        return company.mutualfund_holders.to_json(orient="records", date_format="iso")
    elif holder_type == HolderType.insider_transactions:
        return company.insider_transactions.to_json(orient="records", date_format="iso")
    elif holder_type == HolderType.insider_purchases:
        return company.insider_purchases.to_json(orient="records", date_format="iso")
    elif holder_type == HolderType.insider_roster_holders:
        return company.insider_roster_holders.to_json(orient="records", date_format="iso")
    else:
        return f"Error: invalid holder type {holder_type}. Please use one of the following: {HolderType.major_holders}, {HolderType.institutional_holders}, {HolderType.mutualfund_holders}, {HolderType.insider_transactions}, {HolderType.insider_purchases}, {HolderType.insider_roster_holders}."


@yfinance_server.tool(
    name="get_option_expiration_dates",
    description="""Fetch the available options expiration dates for a given ticker symbol.

Args:
    ticker: str
        The ticker symbol of the stock to get option expiration dates for, e.g. "AAPL"
""",
)
async def get_option_expiration_dates(ticker: str) -> str:
    """Fetch the available options expiration dates for a given ticker symbol."""

    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting option expiration dates for {ticker}: {e}")
        return f"Error: getting option expiration dates for {ticker}: {e}"
    return json.dumps(company.options)


@yfinance_server.tool(
    name="get_option_chain",
    description="""Fetch the option chain for a given ticker symbol, expiration date, and option type.

Use the optional strike filters to narrow the results — a full options chain (e.g. AAPL) can have 200+ rows;
filtering near-the-money reduces this to ~10-20 rows and dramatically cuts token usage.

Returns a JSON object with top-level fields: ticker, expiration, optionType, dataDate (YYYY-MM-DD of
the last trading session — use to detect weekend/holiday staleness), and contracts (array of option rows).

Args:
    ticker: str
        The ticker symbol of the stock to get option chain for, e.g. "AAPL"
    expiration_date: str
        The expiration date for the options chain (format: 'YYYY-MM-DD')
    option_type: str
        The type of option to fetch ('calls' or 'puts')
    min_strike: float | None
        Optional minimum strike price filter. Only options with strike >= min_strike are returned.
    max_strike: float | None
        Optional maximum strike price filter. Only options with strike <= max_strike are returned.
    in_the_money_only: bool
        If True, only return in-the-money options. Default is False.
""",
)
async def get_option_chain(
    ticker: str,
    expiration_date: str,
    option_type: str,
    min_strike: float | None = None,
    max_strike: float | None = None,
    in_the_money_only: bool = False,
) -> str:
    """Fetch the option chain for a given ticker symbol, expiration date, and option type.

    Args:
        ticker: The ticker symbol of the stock
        expiration_date: The expiration date for the options chain (format: 'YYYY-MM-DD')
        option_type: The type of option to fetch ('calls' or 'puts')
        min_strike: Optional minimum strike price filter.
        max_strike: Optional maximum strike price filter.
        in_the_money_only: If True, only return in-the-money options.

    Returns:
        str: JSON string containing the option chain data
    """

    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting option chain for {ticker}: {e}")
        return f"Error: getting option chain for {ticker}: {e}"

    # Check if the expiration date is valid
    if expiration_date not in company.options:
        return f"Error: No options available for the date {expiration_date}. You can use `get_option_expiration_dates` to get the available expiration dates."

    # Check if the option type is valid
    if option_type not in ["calls", "puts"]:
        return "Error: Invalid option type. Please use 'calls' or 'puts'."

    # Get the option chain
    option_chain = company.option_chain(expiration_date)
    if option_type == "calls":
        df = option_chain.calls
    elif option_type == "puts":
        df = option_chain.puts
    else:
        return f"Error: invalid option type {option_type}. Please use one of the following: calls, puts."

    if min_strike is not None:
        df = df[df["strike"] >= min_strike]
    if max_strike is not None:
        df = df[df["strike"] <= max_strike]
    if in_the_money_only:
        df = df[df["inTheMoney"]]

    # Derive dataDate from the last trading session
    try:
        _hist = company.history(period="5d", interval="1d")
        data_date = (
            str(_hist.index[-1].date())
            if _hist is not None and not _hist.empty
            else str(datetime.date.today())
        )
    except Exception:
        data_date = str(datetime.date.today())

    contracts = json.loads(df.to_json(orient="records", date_format="iso"))
    return json.dumps({
        "ticker": ticker,
        "expiration": expiration_date,
        "optionType": option_type,
        "dataDate": data_date,
        "contracts": contracts,
    })


@yfinance_server.tool(
    name="get_recommendations",
    description="""Get recommendations or upgrades/downgrades for a given ticker symbol from yahoo finance. You can also specify the number of months back to get upgrades/downgrades for, default is 12.

DEPRECATED: Use `get_analyst_upgrade_radar` instead, which supports batch tickers and includes pre-computed signal classification (UPGRADE/DOWNGRADE/MAINTAIN) and net sentiment score.

Args:
    ticker: str
        The ticker symbol of the stock to get recommendations for, e.g. "AAPL"
    recommendation_type: str
        The type of recommendation to get. You can choose from the following recommendation types: recommendations, upgrades_downgrades.
    months_back: int
        The number of months back to get upgrades/downgrades for, default is 12.
""",
)
async def get_recommendations(ticker: str, recommendation_type: str, months_back: int = 12) -> str:
    """Get recommendations or upgrades/downgrades for a given ticker symbol"""
    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting recommendations for {ticker}: {e}")
        return f"Error: getting recommendations for {ticker}: {e}"
    try:
        if recommendation_type == RecommendationType.recommendations:
            return company.recommendations.to_json(orient="records")
        elif recommendation_type == RecommendationType.upgrades_downgrades:
            # Get the upgrades/downgrades based on the cutoff date
            upgrades_downgrades = company.upgrades_downgrades.reset_index()
            cutoff_date = pd.Timestamp.now() - pd.DateOffset(months=months_back)
            upgrades_downgrades = upgrades_downgrades[
                upgrades_downgrades["GradeDate"] >= cutoff_date
            ]
            upgrades_downgrades = upgrades_downgrades.sort_values("GradeDate", ascending=False)
            # Get the first occurrence (most recent) for each firm
            latest_by_firm = upgrades_downgrades.drop_duplicates(subset=["Firm"])
            return latest_by_firm.to_json(orient="records", date_format="iso")
    except Exception as e:
        print(f"Error: getting recommendations for {ticker}: {e}")
        return f"Error: getting recommendations for {ticker}: {e}"


# ---------------------------------------------------------------------------
# Group 1.1 — get_fast_info
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_fast_info",
    description="""Get lightweight real-time price and market data for one or more ticker symbols. Returns ~20 high-signal fields
plus pre-market/after-hours prices when available.

PREFER THIS over get_stock_info for any query involving current price, market cap, 52-week range,
moving averages, or trading volume — it uses ~85-90% fewer tokens than get_stock_info.

Fields returned: currency, exchange, quoteType, lastPrice, open, previousClose,
dayHigh, dayLow, yearHigh, yearLow, yearChange, marketCap, shares, lastVolume,
tenDayAverageVolume, threeMonthAverageVolume, fiftyDayAverage, twoHundredDayAverage,
marketOpen, lastTradeDate, postMarketTimestamp.

marketOpen: true only during regular session hours (09:30–16:00 ET Mon–Fri). false
pre-market, after-hours, weekends, and holidays. Always true for crypto (24/7 markets).
lastTradeDate: YYYY-MM-DD date of the session that open/dayHigh/dayLow/lastVolume belong to.
On weekends this is the prior Friday, not today.
postMarketTimestamp: ISO8601 timestamp of postMarketPrice. null when no AH activity.

Extended-hours fields (included when available): preMarketPrice, preMarketChange,
preMarketChangePercent, postMarketPrice, postMarketChange, postMarketChangePercent.

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of symbols (e.g. ["AAPL", "MSFT"]).
        When a list is provided, returns a dict keyed by symbol.
""",
)
async def get_fast_info(ticker: str | list[str]) -> str:
    """Get lightweight real-time price and market data for a ticker symbol."""
    if isinstance(ticker, list):
        results = await asyncio.gather(*[get_fast_info(t) for t in ticker], return_exceptions=True)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    cache_key = f"fast_info:{ticker}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        data = {}
        for k in fi.keys():
            try:
                data[k] = fi[k]
            except Exception:
                data[k] = None
    except Exception as e:
        print(f"Error: getting fast info for {ticker}: {e}")
        return f"Error: getting fast info for {ticker}: {e}"

    # Fetch .info once to cover both the shares fallback and extended-hours enrichment.
    try:
        info = company.info
        if data.get("shares") is None:
            data["shares"] = info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")
        for key in (
            "preMarketPrice", "preMarketChange", "preMarketChangePercent",
            "postMarketPrice", "postMarketChange", "postMarketChangePercent",
            "postMarketTime",
        ):
            val = info.get(key)
            if val is not None:
                data[key] = val
        # Market state fields (CR-03)
        data["marketOpen"] = info.get("marketState") == "REGULAR"
        reg_mkt_time = info.get("regularMarketTime")
        data["lastTradeDate"] = (
            datetime.datetime.utcfromtimestamp(reg_mkt_time).strftime('%Y-%m-%d')
            if isinstance(reg_mkt_time, (int, float)) and reg_mkt_time
            else None
        )
        post_mkt_time = info.get("postMarketTime")
        data["postMarketTimestamp"] = (
            datetime.datetime.fromtimestamp(post_mkt_time, tz=datetime.timezone.utc).isoformat()
            if isinstance(post_mkt_time, (int, float)) and post_mkt_time
            else None
        )
    except Exception:
        pass  # Extended-hours data is optional; fast_info fields are still returned

    # For index tickers, volume and open are not meaningful — replace zeros with null.
    # lastTradeDate / marketOpen / postMarketTimestamp are still valid for indices.
    if data.get("quoteType") == "INDEX":
        for field in ("open", "lastVolume", "tenDayAverageVolume", "threeMonthAverageVolume"):
            if data.get(field) == 0:
                data[field] = None
        data["_note"] = "Index ticker — volume and open fields not applicable"

    # timezone is always null via yfinance and not useful — omit to reduce noise
    data.pop("timezone", None)

    result = json.dumps(data)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 1.2 — get_short_interest
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_short_interest",
    description="""Get short interest data for a ticker symbol.

Returns structured short-selling metrics sourced from yfinance .info, including the
pre-computed short-interest-as-percentage-of-float ratio.

Fields returned (when available):
- sharesShort: Number of shares currently sold short
- sharesShortPriorMonth: Shares short in the prior reporting period
- shortRatio: Days-to-cover ratio (sharesShort / avg daily volume)
- shortPercentOfFloat: Short interest as a fraction of float (0–1 scale)
- sharesPercentSharesOut: Short shares as a fraction of shares outstanding
- floatShares: Total shares in the public float
- sharesOutstanding: Total shares outstanding
- dateShortInterest: Date of the short interest data
- sharesShortPreviousMonthDate: Date of the prior month data

Note: Short interest data is reported bi-monthly by exchanges and may be up to 2 weeks old.

Args:
    ticker: str
        The ticker symbol of the stock, e.g. "AAPL"
""",
)
async def get_short_interest(ticker: str) -> str:
    """Get short interest data for a ticker symbol."""
    cache_key = f"short_interest:{ticker}"
    cached = _cache_get(cache_key, _STMT_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        info = company.info
    except Exception as e:
        print(f"Error: getting short interest for {ticker}: {e}")
        return f"Error: getting short interest for {ticker}: {e}"

    _SHORT_FIELDS = (
        "sharesShort",
        "sharesShortPriorMonth",
        "shortRatio",
        "shortPercentOfFloat",
        "sharesPercentSharesOut",
        "floatShares",
        "sharesOutstanding",
        "dateShortInterest",
        "sharesShortPreviousMonthDate",
    )

    def _serialize(v):
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return v

    data: dict = {"ticker": ticker}
    for key in _SHORT_FIELDS:
        val = info.get(key)
        if val is not None:
            data[key] = _serialize(val)

    result = json.dumps(data)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.1 — get_price_stats
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_price_stats",
    description="""Get pre-computed price statistics for one or more tickers. Returns a compact summary so you do NOT
need to fetch raw history and compute these yourself.

Includes:
- Current price, previous close, % change today
- % distance from 52-week high and 52-week low
- % distance from 50-day and 200-day moving averages
- 30-day realized annualized volatility (from daily close returns)
- CAGR over 1y, 3y, 5y (where data is available)

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of symbols (e.g. ["AAPL", "MSFT"]).
        When a list is provided, returns a dict keyed by symbol.
""",
)
async def get_price_stats(ticker: str | list[str]) -> str:
    """Get pre-computed price statistics for a ticker."""
    if isinstance(ticker, list):
        results = await asyncio.gather(*[get_price_stats(t) for t in ticker])
        return json.dumps({t: json.loads(r) for t, r in zip(ticker, results)})
    cache_key = f"price_stats:{ticker}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        last_price = fi.last_price
        prev_close = fi.previous_close
    except Exception as e:
        print(f"Error: getting price stats for {ticker}: {e}")
        return f"Error: getting price stats for {ticker}: {e}"

    def _pct(value, reference):
        if reference and reference != 0:
            return round((value - reference) / reference * 100, 4)
        return None

    stats: dict = {
        "ticker": ticker,
        "currency": fi.currency,
        "lastPrice": last_price,
        "previousClose": prev_close,
        "pctChangeTodayVsPrevClose": _pct(last_price, prev_close),
        "yearHigh": fi.year_high,
        "yearLow": fi.year_low,
        "pctFromYearHigh": _pct(last_price, fi.year_high),
        "pctFromYearLow": _pct(last_price, fi.year_low),
        "fiftyDayAverage": fi.fifty_day_average,
        "twoHundredDayAverage": fi.two_hundred_day_average,
        "pctFromFiftyDayAvg": _pct(last_price, fi.fifty_day_average),
        "pctFromTwoHundredDayAvg": _pct(last_price, fi.two_hundred_day_average),
    }

    # Compute 30-day realised volatility and CAGR from history
    hist_5y = None
    try:
        hist_5y = await _fetch_with_retry(company.history, "5y", "1d")
        if hist_5y is not None and not hist_5y.empty and "Close" in hist_5y.columns:
            closes = hist_5y["Close"].dropna()
            daily_returns = closes.pct_change().dropna()

            # 30-day volatility (annualised)
            if len(daily_returns) >= 20:
                vol_30d = daily_returns.tail(30).std() * (252 ** 0.5)
                stats["annualizedVolatility30d"] = round(float(vol_30d) * 100, 4)

            # CAGR
            def _cagr(closes_series, years):
                if len(closes_series) < 2:
                    return None
                try:
                    start = float(closes_series.iloc[0])
                    end = float(closes_series.iloc[-1])
                    if start <= 0:
                        return None
                    return round(((end / start) ** (1 / years) - 1) * 100, 4)
                except Exception:
                    return None

            now = closes.index[-1]
            for years, label in [(1, "cagr1y"), (3, "cagr3y"), (5, "cagr5y")]:
                cutoff = now - pd.DateOffset(years=years)
                subset = closes[closes.index >= cutoff]
                stats[label] = _cagr(subset, years)
    except Exception:
        pass  # Stats from fast_info are still returned

    stats["dataDate"] = get_last_trading_date(hist_5y)
    result = json.dumps(stats)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.2 — get_analyst_consensus
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_analyst_consensus",
    description="""Get a compact analyst consensus summary for one or more tickers.

Returns pre-aggregated data so you do NOT need to call get_recommendations separately.
Includes:
- Consensus price target (current, low, high, mean, median) and % upside from current price
- Recommendation breakdown (strongBuy, buy, hold, sell, strongSell counts) for recent periods
- Dominant consensus rating

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of symbols (e.g. ["AAPL", "MSFT"]).
        When a list is provided, returns a dict keyed by symbol.
""",
)
async def get_analyst_consensus(ticker: str | list[str]) -> str:
    """Get compact analyst consensus summary."""
    if isinstance(ticker, list):
        results = await asyncio.gather(*[get_analyst_consensus(t) for t in ticker], return_exceptions=True)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    cache_key = f"analyst_consensus:{ticker}"
    cached = _cache_get(cache_key, _STMT_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        last_price = fi.last_price
    except Exception as e:
        print(f"Error: getting analyst consensus for {ticker}: {e}")
        return f"Error: getting analyst consensus for {ticker}: {e}"

    output: dict = {"ticker": ticker}

    # Price targets
    try:
        targets = company.analyst_price_targets
        if targets:
            current_target = targets.get("current")
            output["priceTargets"] = {
                "current": current_target,
                "low": targets.get("low"),
                "high": targets.get("high"),
                "mean": targets.get("mean"),
                "median": targets.get("median"),
                "pctUpsideFromLastPrice": (
                    round((current_target - last_price) / last_price * 100, 2)
                    if current_target and last_price
                    else None
                ),
            }
    except Exception:
        output["priceTargets"] = None

    # Recommendation summary (period breakdown)
    try:
        rec_df = company.recommendations_summary
        if rec_df is not None and not rec_df.empty:
            rec_df = rec_df.reset_index()
            # Identify dominant rating for most recent period
            cols = ["strongBuy", "buy", "hold", "sell", "strongSell"]
            latest = rec_df.iloc[0]
            counts = {c: int(latest.get(c, 0)) for c in cols if c in latest}
            dominant = max(counts, key=counts.get) if counts else None
            output["recommendationSummary"] = rec_df.to_dict(orient="records")
            output["dominantRating"] = dominant
            output["ratingCounts"] = counts
            output["totalAnalysts"] = sum(counts.values()) if counts else None
    except Exception:
        output["recommendationSummary"] = None

    result = json.dumps(output)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.3 — get_earnings_analysis
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_earnings_analysis",
    description="""Get all analyst forward-looking data in a single call — replaces 5 separate tool calls.

Returns:
- earningsEstimate: EPS estimates for current quarter, next quarter, current year, next year
- revenueEstimate: Revenue estimates for the same periods
- epsTrend: How EPS estimates have moved over the last 7/30/60/90 days
- earningsHistory: Last 4 quarters — actual vs estimated EPS and surprise %
- growthEstimates: Analyst growth estimates for stock vs industry/sector/index

Args:
    ticker: str
        The ticker symbol, e.g. "AAPL"
""",
)
async def get_earnings_analysis(ticker: str) -> str:
    """Get all forward-looking analyst estimates in one call."""
    cache_key = f"earnings_analysis:{ticker}"
    cached = _cache_get(cache_key, _STMT_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        if fi.currency is None:
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting earnings analysis for {ticker}: {e}")
        return f"Error: getting earnings analysis for {ticker}: {e}"

    def _df_to_records(df):
        if df is None or (hasattr(df, "empty") and df.empty):
            return None
        df = df.reset_index()
        df.columns = [str(c) for c in df.columns]
        df = df.where(pd.notnull(df), None)
        return df.to_dict(orient="records")

    output: dict = {"ticker": ticker}
    for key, attr in [
        ("earningsEstimate", "earnings_estimate"),
        ("revenueEstimate", "revenue_estimate"),
        ("epsTrend", "eps_trend"),
        ("earningsHistory", "earnings_history"),
        ("growthEstimates", "growth_estimates"),
    ]:
        try:
            output[key] = _df_to_records(getattr(company, attr))
        except Exception:
            output[key] = None

    result = json.dumps(output)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.4 — get_financial_ratios
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_financial_ratios",
    description="""Get pre-computed key financial ratios for one or more tickers.

PREFER THIS over fetching full financial statements when you need valuation or profitability ratios.
Ratios are computed server-side from company.info so the LLM does not have to process raw statements.

Includes:
- Valuation: P/E (trailing & forward), P/S, P/B, EV/EBITDA, EV/Revenue, PEG ratio
- Profitability: Gross/Operating/Net margins, ROE, ROA
- Leverage: Debt/Equity, Current ratio, Quick ratio
- Cash flow: Free Cash Flow, FCF yield (FCF / market cap)
- Dividend: Yield, Payout ratio

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of symbols (e.g. ["AAPL", "MSFT"]).
        When a list is provided, returns a dict keyed by symbol.
""",
)
async def get_financial_ratios(ticker: str | list[str]) -> str:
    """Get pre-computed key financial ratios."""
    if isinstance(ticker, list):
        results = await asyncio.gather(*[get_financial_ratios(t) for t in ticker], return_exceptions=True)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    cache_key = f"financial_ratios:{ticker}"
    cached = _cache_get(cache_key, _STMT_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        info = company.info
    except Exception as e:
        print(f"Error: getting financial ratios for {ticker}: {e}")
        return f"Error: getting financial ratios for {ticker}: {e}"

    def _get(key):
        return info.get(key)

    market_cap = _get("marketCap")
    free_cashflow = _get("freeCashflow")

    ratios: dict = {
        "ticker": ticker,
        "currency": _get("financialCurrency"),
        # Valuation
        "trailingPE": _get("trailingPE"),
        "forwardPE": _get("forwardPE"),
        "pegRatio": _get("pegRatio"),
        "priceToSales": _get("priceToSalesTrailing12Months"),
        "priceToBook": _get("priceToBook"),
        "enterpriseToEbitda": _get("enterpriseToEbitda"),
        "enterpriseToRevenue": _get("enterpriseToRevenue"),
        # Profitability
        "grossMargins": _get("grossMargins"),
        "operatingMargins": _get("operatingMargins"),
        "profitMargins": _get("profitMargins"),
        "returnOnEquity": _get("returnOnEquity"),
        "returnOnAssets": _get("returnOnAssets"),
        # Leverage / Liquidity
        "debtToEquity": _get("debtToEquity"),
        "currentRatio": _get("currentRatio"),
        "quickRatio": _get("quickRatio"),
        # Cash flow
        "freeCashflow": free_cashflow,
        "freeCashflowYield": (
            round(free_cashflow / market_cap * 100, 4)
            if free_cashflow and market_cap
            else None
        ),
        # Dividends
        "dividendYield": _get("dividendYield"),
        "payoutRatio": _get("payoutRatio"),
        # Growth (trailing)
        "earningsGrowth": _get("earningsGrowth"),
        "revenueGrowth": _get("revenueGrowth"),
    }

    # Replace any dict values (empty {} or non-numeric wrappers) with None
    ratios = {k: (None if isinstance(v, dict) else v) for k, v in ratios.items()}

    result = json.dumps(ratios)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.5 — get_calendar
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_calendar",
    description="""Get upcoming earnings date and dividend schedule for a ticker.

DEPRECATED: All fields returned by this tool (earnings date, EPS/revenue estimates,
ex-dividend date, dividend pay date) are also available in `get_stock_info` under the
`earnings` and dividend keys. Prefer `get_stock_info` to save a round-trip.

Returns:
- Next earnings date range and EPS/revenue estimates
- Ex-dividend date and dividend pay date
- earningsDateConfirmed: true when Yahoo Finance shows a single fixed date (likely IR-confirmed
  press release/8-K). false when a date range is returned (analyst estimate).
- earningsDateSource: "IR_FILING" | "ESTIMATE" | "UNKNOWN"

Args:
    ticker: str
        The ticker symbol, e.g. "AAPL"
""",
)
async def get_calendar(ticker: str) -> str:
    """Get upcoming earnings and dividend calendar for a ticker."""
    cache_key = f"calendar:{ticker}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        if fi.currency is None:
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting calendar for {ticker}: {e}")
        return f"Error: getting calendar for {ticker}: {e}"

    try:
        cal = company.calendar
    except Exception as e:
        print(f"Error: getting calendar for {ticker}: {e}")
        return f"Error: getting calendar for {ticker}: {e}"

    if not cal:
        return json.dumps({"ticker": ticker, "calendar": None})

    # calendar values may be datetime.date objects — convert to strings
    def _serialize(v):
        if hasattr(v, "isoformat"):
            return v.isoformat()
        if isinstance(v, list):
            return [_serialize(i) for i in v]
        return v

    # Determine whether the earnings date is IR-confirmed or an analyst estimate.
    # Heuristic: if Yahoo Finance provides a single fixed date, it's likely sourced from
    # an IR press release / 8-K filing. A date range (start ≠ end) signals an analyst
    # estimate. This heuristic is imperfect but is the best available without SEC parsing.
    ed_raw = cal.get("Earnings Date")
    if isinstance(ed_raw, list):
        ed_dates = ed_raw
    elif ed_raw is not None:
        ed_dates = [ed_raw]
    else:
        ed_dates = []

    unique_dates = {getattr(d, "date", lambda: d)() if hasattr(d, "date") else d for d in ed_dates}
    if len(unique_dates) == 0:
        earnings_date_confirmed = False
        earnings_date_source = "UNKNOWN"
    elif len(unique_dates) == 1:
        earnings_date_confirmed = True
        earnings_date_source = "IR_FILING"
    else:
        earnings_date_confirmed = False
        earnings_date_source = "ESTIMATE"

    output = {
        "ticker": ticker,
        "earningsDateConfirmed": earnings_date_confirmed,
        "earningsDateSource": earnings_date_source,
        "calendar": {k: _serialize(v) for k, v in cal.items()},
    }
    result = json.dumps(output)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 3.1 — search_ticker
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="search_ticker",
    description="""Search for ticker symbols by company name, partial name, or ISIN.

Use this tool to resolve a company name to a ticker symbol before calling other tools.
Returns matching quotes with symbol, shortname, exchange, and type.

Args:
    query: str
        Company name, partial name, or ISIN to search for, e.g. "Apple", "AAPL", "US0378331005"
    max_results: int
        Maximum number of quote results to return. Default is 8.
    exchange: str | None
        Optional exchange filter. Pass "US" to restrict results to NMS (NASDAQ) and NYQ (NYSE) only
        — use this for small/mid-cap US equity searches that may otherwise return foreign listings or
        crypto tokens. Pass a specific code (e.g. "NMS", "NYQ") for an exact exchange match.
        Default is None (all exchanges returned).
""",
)
async def search_ticker(query: str, max_results: int = 8, exchange: str | None = None) -> str:
    """Search for ticker symbols by company name or ISIN."""
    try:
        search = yf.Search(query, max_results=max_results, news_count=0)
        quotes = search.quotes
        # Return only the most useful fields to minimise token use
        trimmed = [
            {
                "symbol": q.get("symbol"),
                "shortname": q.get("shortname") or q.get("longname"),
                "exchange": q.get("exchange"),
                "quoteType": q.get("quoteType"),
                "score": q.get("score"),
            }
            for q in quotes
            if q.get("symbol")
        ]
        # Apply exchange filter when requested
        if exchange:
            exch_upper = exchange.upper()
            if exch_upper == "US":
                _us = {"NMS", "NYQ", "PCX"}  # NASDAQ, NYSE, NYSE Arca
                trimmed = [r for r in trimmed if r.get("exchange") in _us]
            else:
                trimmed = [r for r in trimmed if r.get("exchange") == exch_upper]
        return json.dumps(trimmed)
    except Exception as e:
        print(f"Error: searching for '{query}': {e}")
        return f"Error: searching for '{query}': {e}"


# ---------------------------------------------------------------------------
# Group 3.2 — screen_stocks
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="screen_stocks",
    description="""Screen the market for stocks matching predefined criteria.

Use this tool to discover stocks without iterating over individual tickers.

Predefined screener names:
  aggressive_small_caps, day_gainers, day_losers, growth_technology_stocks, most_actives,
  most_shorted_stocks, small_cap_gainers, undervalued_growth_stocks, undervalued_large_caps,
  conservative_foreign_funds, high_yield_bond, portfolio_anchors, solid_large_growth_funds,
  solid_midcap_growth_funds, top_mutual_funds

Returns the top results with symbol, name, price, change%, market cap, and volume.

Args:
    screener_name: str
        Name of the predefined screener to use (see list above), e.g. "day_gainers"
    count: int
        Number of results to return (default 25, max 250).
""",
)
async def screen_stocks(screener_name: str, count: int = 25) -> str:
    """Screen the market using a predefined yfinance screener."""
    if count > 250:
        count = 250

    valid_screeners = list(yf.PREDEFINED_SCREENER_QUERIES.keys())
    if screener_name not in valid_screeners:
        return (
            f"Error: unknown screener '{screener_name}'. "
            f"Valid options: {', '.join(valid_screeners)}"
        )

    try:
        raw = yf.screen(screener_name, count=count)
        quotes = raw.get("quotes", [])
        trimmed = [
            {
                "symbol": q.get("symbol"),
                "shortName": q.get("shortName"),
                "regularMarketPrice": q.get("regularMarketPrice"),
                "regularMarketChangePercent": q.get("regularMarketChangePercent"),
                "marketCap": q.get("marketCap"),
                "regularMarketVolume": q.get("regularMarketVolume"),
                "exchange": q.get("exchange"),
            }
            for q in quotes
        ]
        return json.dumps({"screener": screener_name, "count": len(trimmed), "results": trimmed})
    except Exception as e:
        print(f"Error: running screener '{screener_name}': {e}")
        return f"Error: running screener '{screener_name}': {e}"


# ---------------------------------------------------------------------------
# Group 3.4 — get_sec_filings
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_sec_filings",
    description="""Get recent SEC filings for a ticker (10-K, 10-Q, 8-K, etc.).

Returns a list of recent filings with form type, filing date, and URL.

Args:
    ticker: str
        The ticker symbol, e.g. "AAPL"
""",
)
async def get_sec_filings(ticker: str) -> str:
    """Get recent SEC filings for a ticker."""
    cache_key = f"sec_filings:{ticker}"
    cached = _cache_get(cache_key, _STMT_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        if fi.currency is None:
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting SEC filings for {ticker}: {e}")
        return f"Error: getting SEC filings for {ticker}: {e}"

    try:
        filings = company.sec_filings
    except Exception as e:
        print(f"Error: getting SEC filings for {ticker}: {e}")
        return f"Error: getting SEC filings for {ticker}: {e}"

    if not filings:
        return json.dumps({"ticker": ticker, "filings": []})

    # sec_filings is a list of dicts; ensure dates are serialisable
    def _serialize_filing(f):
        return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in f.items()}

    result = json.dumps({"ticker": ticker, "filings": [_serialize_filing(f) for f in filings]})
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 3.5 — get_technical_indicators
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_technical_indicators",
    description="""Get pre-computed technical / momentum indicators for one or more tickers.

Computes indicators server-side from historical daily close prices so the LLM
does NOT need to fetch raw OHLCV history and calculate manually.

Returns:
- rsi14: 14-day Relative Strength Index (Wilder smoothing). Values below 30 are
  typically considered oversold; above 70 overbought.
- macd: MACD line (12-day EMA minus 26-day EMA)
- macdSignal: 9-day EMA of the MACD line
- macdHistogram: MACD minus signal (positive = bullish momentum)
- lastClose: Most recent closing price used for the calculations
- dataDate: Date of the most recent data point

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of up to 5 symbols.
        When a list is provided, returns a dict keyed by symbol.
        Max 5 tickers per call; split larger lists into multiple calls.
    period: str
        Lookback period for fetching history (default "3mo"). Longer periods give
        more accurate indicator warm-up. Valid: 1mo, 3mo, 6mo, 1y, 2y, 5y.
""",
)
async def get_technical_indicators(ticker: str | list[str], period: str = "3mo") -> str:
    """Get pre-computed technical indicators (RSI, MACD) for one or more tickers."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_technical_indicators(t, period))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    cache_key = f"tech_indicators:{ticker}:{period}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        hist = await _fetch_with_retry(company.history, period, "1d")
    except Exception as e:
        print(f"Error: getting technical indicators for {ticker}: {e}")
        return f"Error: getting technical indicators for {ticker}: {e}"

    if hist is None or hist.empty or "Close" not in hist.columns:
        return f"Error: no price history available for {ticker}"

    closes = hist["Close"].dropna()
    if len(closes) < 26:
        return (
            f"Error: insufficient price history for {ticker} "
            f"(need ≥26 data points, got {len(closes)})"
        )

    output: dict = {"ticker": ticker}

    # --- RSI-14 (Wilder smoothing) ---
    try:
        delta = closes.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        output["rsi14"] = round(float(rsi.iloc[-1]), 2)
    except Exception:
        output["rsi14"] = None

    # --- MACD (12, 26, 9) ---
    try:
        ema12 = closes.ewm(span=12, adjust=False).mean()
        ema26 = closes.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        histogram = macd_line - signal_line
        output["macd"] = round(float(macd_line.iloc[-1]), 4)
        output["macdSignal"] = round(float(signal_line.iloc[-1]), 4)
        output["macdHistogram"] = round(float(histogram.iloc[-1]), 4)
    except Exception:
        output["macd"] = None
        output["macdSignal"] = None
        output["macdHistogram"] = None

    output["lastClose"] = round(float(closes.iloc[-1]), 2)
    last_idx = closes.index[-1]
    output["dataDate"] = (
        str(last_idx.date()) if hasattr(last_idx, "date") else str(last_idx)
    )

    result = json.dumps(output)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Tool: get_price_slope
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_price_slope",
    description="""Get N-day price slope (% change) and direction for one or more tickers. Pre-computed server-side.

Returns: startClose, endClose, slopePct, direction (UP/DOWN/FLAT).

Args:
    ticker: str | list[str] — single or batch
    days: int — lookback in trading days (default: 5)
""",
)
async def get_price_slope(ticker: str | list[str], days: int = 5) -> str:
    """Return N-day price slope for one or more tickers."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_price_slope(t, days))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})

    company = yf.Ticker(ticker)
    try:
        # Fetch extra buffer for weekends/holidays
        hist = company.history(period=f"{days + 10}d", interval="1d")
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    if hist is None or hist.empty or len(hist) < 2:
        return json.dumps({"error": True, "message": f"Insufficient price data for {ticker}", "ticker": ticker})

    closes = hist["Close"].dropna()
    if len(closes) < 2:
        return json.dumps({"error": True, "message": f"Insufficient close data for {ticker}", "ticker": ticker})

    # Take last N trading days
    closes = closes.tail(days)
    start_close = float(closes.iloc[0])
    end_close = float(closes.iloc[-1])
    slope_pct = round((end_close - start_close) / start_close * 100, 2) if start_close != 0 else None

    if slope_pct is None:
        direction = "FLAT"
    elif abs(slope_pct) < 0.5:
        direction = "FLAT"
    elif slope_pct > 0:
        direction = "UP"
    else:
        direction = "DOWN"

    last_idx = closes.index[-1]
    data_date = str(last_idx.date()) if hasattr(last_idx, "date") else str(last_idx)

    return json.dumps({
        "ticker": ticker,
        "days": days,
        "startClose": round(start_close, 2),
        "endClose": round(end_close, 2),
        "slopePct": slope_pct,
        "direction": direction,
        "dataDate": data_date,
    })


# ---------------------------------------------------------------------------
# Tool: get_volume_ratio
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_volume_ratio",
    description="""Get last-session volume vs N-day average volume ratio. Pre-computed server-side.

Returns: lastVolume, avgVolume10d, avgVolume90d, ratio10d, ratio90d, volumeFlag (HIGH/NORMAL/LOW).

Args:
    ticker: str | list[str] — single or batch
    period: int — averaging period in days (default: 10, used for flag threshold)
""",
)
async def get_volume_ratio(ticker: str | list[str], period: int = 10) -> str:
    """Return volume ratio for one or more tickers."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_volume_ratio(t, period))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        last_vol = fi.last_volume
        avg_10d = fi.ten_day_average_volume
        avg_90d = fi.three_month_average_volume
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    ratio_10d = round(last_vol / avg_10d, 3) if last_vol and avg_10d else None
    ratio_90d = round(last_vol / avg_90d, 3) if last_vol and avg_90d else None

    ref_ratio = ratio_10d
    if ref_ratio is not None:
        if ref_ratio > 1.5:
            volume_flag = "HIGH"
        elif ref_ratio < 0.7:
            volume_flag = "LOW"
        else:
            volume_flag = "NORMAL"
    else:
        volume_flag = None

    try:
        _hist = company.history(period="5d", interval="1d")
        data_date = (
            str(_hist.index[-1].date())
            if _hist is not None and not _hist.empty
            else str(datetime.date.today())
        )
    except Exception:
        data_date = str(datetime.date.today())

    return json.dumps({
        "ticker": ticker,
        "lastVolume": last_vol,
        "avgVolume10d": avg_10d,
        "avgVolume90d": avg_90d,
        "ratio10d": ratio_10d,
        "ratio90d": ratio_90d,
        "volumeFlag": volume_flag,
        "dataDate": data_date,
    })


# ---------------------------------------------------------------------------
# Tool: get_ma_position
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_ma_position",
    description="""Get price position vs 50DMA and 200DMA with trend classification. Pre-computed server-side.

Returns: lastPrice, fiftyDayAverage, twoHundredDayAverage, pctVs50dma, pctVs200dma, regime50, regime200, trend (BULLISH/BEARISH/MIXED).

Args:
    ticker: str | list[str] — single or batch
""",
)
async def get_ma_position(ticker: str | list[str]) -> str:
    """Return MA position for one or more tickers."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_ma_position(t))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        last_price = fi.last_price
        fifty_dma = fi.fifty_day_average
        two_hundred_dma = fi.two_hundred_day_average
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    pct_vs_50 = round((last_price - fifty_dma) / fifty_dma * 100, 2) if last_price and fifty_dma else None
    pct_vs_200 = round((last_price - two_hundred_dma) / two_hundred_dma * 100, 2) if last_price and two_hundred_dma else None

    regime_50 = "ABOVE" if pct_vs_50 is not None and pct_vs_50 >= 0 else ("BELOW" if pct_vs_50 is not None else None)
    regime_200 = "ABOVE" if pct_vs_200 is not None and pct_vs_200 >= 0 else ("BELOW" if pct_vs_200 is not None else None)

    if regime_50 == "ABOVE" and regime_200 == "ABOVE":
        trend = "BULLISH"
    elif regime_50 == "BELOW" and regime_200 == "BELOW":
        trend = "BEARISH"
    elif regime_50 is not None and regime_200 is not None:
        trend = "MIXED"
    else:
        trend = None

    try:
        _hist = company.history(period="5d", interval="1d")
        data_date = (
            str(_hist.index[-1].date())
            if _hist is not None and not _hist.empty
            else str(datetime.date.today())
        )
    except Exception:
        data_date = str(datetime.date.today())

    return json.dumps({
        "ticker": ticker,
        "lastPrice": round(last_price, 2) if last_price else None,
        "fiftyDayAverage": round(fifty_dma, 2) if fifty_dma else None,
        "twoHundredDayAverage": round(two_hundred_dma, 2) if two_hundred_dma else None,
        "pctVs50dma": pct_vs_50,
        "pctVs200dma": pct_vs_200,
        "regime50": regime_50,
        "regime200": regime_200,
        "trend": trend,
        "dataDate": data_date,
    })


# ---------------------------------------------------------------------------
# Tool: get_credit_health
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_credit_health",
    description="""Get pre-computed credit/leverage metrics: Net Debt/EBITDA, interest coverage, debt tier, credit stress flag.

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of up to 5 symbols.
        When a list is provided, returns a dict keyed by symbol.
        Max 5 tickers per call; split larger lists into multiple calls.
""",
)
async def get_credit_health(ticker: str | list[str]) -> str:
    """Return credit health metrics for one or more tickers."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_credit_health(t))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    company = yf.Ticker(ticker)

    data_quality = "OK"

    # Fetch quarterly balance sheet
    try:
        bs = company.quarterly_balance_sheet
    except Exception as e:
        return json.dumps({"error": True, "message": f"Balance sheet fetch failed: {e}", "ticker": ticker})

    # Fetch quarterly income statement
    try:
        inc = company.quarterly_income_stmt
    except Exception as e:
        return json.dumps({"error": True, "message": f"Income statement fetch failed: {e}", "ticker": ticker})

    if bs is None or bs.empty:
        return json.dumps({"error": True, "message": "No balance sheet data available", "ticker": ticker})
    if inc is None or inc.empty:
        return json.dumps({"error": True, "message": "No income statement data available", "ticker": ticker})

    # Most recent quarter column
    bs_col = bs.columns[0]
    inc_col = inc.columns[0]

    def _safe_get(df, col, *row_names):
        for name in row_names:
            try:
                val = df.loc[name, col]
                if pd.notna(val):
                    return float(val)
            except (KeyError, TypeError):
                continue
        return None

    total_debt = _safe_get(bs, bs_col, "Total Debt", "TotalDebt", "Long Term Debt")
    cash = _safe_get(bs, bs_col, "Cash And Cash Equivalents", "CashAndCashEquivalents", "Cash")
    ebitda = _safe_get(inc, inc_col, "EBITDA", "Normalized EBITDA", "NormalizedEBITDA")
    ebit = _safe_get(inc, inc_col, "EBIT", "Operating Income", "OperatingIncome")
    interest_expense = _safe_get(inc, inc_col, "Interest Expense", "InterestExpense", "Interest Expense Non Operating")

    net_debt = (total_debt - cash) if total_debt is not None and cash is not None else None

    # Annualize quarterly EBITDA/EBIT (multiply by 4)
    ebitda_annual = ebitda * 4 if ebitda is not None else None
    ebit_annual = ebit * 4 if ebit is not None else None
    interest_annual = interest_expense * 4 if interest_expense is not None else None

    net_debt_to_ebitda = round(net_debt / ebitda_annual, 2) if net_debt is not None and ebitda_annual else None
    interest_coverage = round(ebit_annual / abs(interest_annual), 2) if ebit_annual is not None and interest_annual and interest_annual != 0 else None

    credit_stress = None
    if net_debt_to_ebitda is not None and interest_coverage is not None:
        credit_stress = net_debt_to_ebitda > 2.5 and interest_coverage < 3

    if net_debt_to_ebitda is not None:
        if net_debt_to_ebitda < 1:
            debt_tier = "CLEAN"
        elif net_debt_to_ebitda <= 2.5:
            debt_tier = "MODERATE"
        elif net_debt_to_ebitda <= 4:
            debt_tier = "ELEVATED"
        else:
            debt_tier = "STRESSED"
    else:
        debt_tier = None

    # Check for partial data
    if any(v is None for v in [total_debt, cash, ebitda, ebit, interest_expense]):
        data_quality = "PARTIAL"

    quarter_date = str(bs_col.date()) if hasattr(bs_col, "date") else str(bs_col)

    return json.dumps({
        "ticker": ticker,
        "quarterDate": quarter_date,
        "totalDebtUsd": total_debt,
        "cashUsd": cash,
        "netDebtUsd": net_debt,
        "ebitdaUsd": ebitda_annual,
        "ebitUsd": ebit_annual,
        "interestExpenseUsd": interest_annual,
        "netDebtToEbitda": net_debt_to_ebitda,
        "interestCoverage": interest_coverage,
        "creditStressFlag": credit_stress,
        "debtTier": debt_tier,
        "dataQuality": data_quality,
        "dataDate": get_last_trading_date(),
    })


# ---------------------------------------------------------------------------
# Tool: get_short_momentum
# ---------------------------------------------------------------------------
@yfinance_server.tool(
    name="get_short_momentum",
    description="""Get short interest with pre-computed momentum: MoM delta, direction, squeeze risk, and flag.

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of up to 5 symbols.
        When a list is provided, returns a dict keyed by symbol.
        Max 5 tickers per call; split larger lists into multiple calls.
""",
)
async def get_short_momentum(ticker: str | list[str]) -> str:
    """Return short interest momentum for one or more tickers."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_short_momentum(t))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    company = yf.Ticker(ticker)
    try:
        info = company.info
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    shares_short = info.get("sharesShort")
    shares_short_prior = info.get("sharesShortPriorMonth")
    short_pct_float_raw = info.get("shortPercentOfFloat")
    short_ratio = info.get("shortRatio")
    date_short = info.get("dateShortInterest")

    # Convert 0-1 to 0-100 scale
    short_pct_float = round(short_pct_float_raw * 100, 2) if short_pct_float_raw is not None else None

    # MoM delta
    if shares_short is not None and shares_short_prior is not None and shares_short_prior != 0:
        mom_delta_pct = round((shares_short - shares_short_prior) / shares_short_prior * 100, 2)
    else:
        mom_delta_pct = None

    # MoM direction
    if mom_delta_pct is not None:
        if abs(mom_delta_pct) < 2:
            mom_direction = "FLAT"
        elif mom_delta_pct > 0:
            mom_direction = "RISING"
        else:
            mom_direction = "FALLING"
    else:
        mom_direction = None

    # Squeeze risk
    if short_pct_float is not None:
        if short_pct_float > 30 and short_ratio is not None and short_ratio < 3:
            squeeze_risk = "HIGH"
        elif short_pct_float > 20:
            squeeze_risk = "MODERATE"
        else:
            squeeze_risk = "LOW"
    else:
        squeeze_risk = None

    # Flag
    if short_pct_float is not None and short_pct_float > 30:
        flag = "🔴 CRITICAL SHORT"
    elif short_pct_float is not None and short_pct_float > 20:
        flag = "⚠️ HIGH SHORT"
    else:
        flag = None

    def _ser(v):
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return v

    return json.dumps({
        "ticker": ticker,
        "shortPctFloat": short_pct_float,
        "daysToCover": short_ratio,
        "sharesShort": shares_short,
        "sharesShortPriorMonth": shares_short_prior,
        "momDeltaPct": mom_delta_pct,
        "momDirection": mom_direction,
        "squeezeRisk": squeeze_risk,
        "flag": flag,
        "dateShortInterest": _ser(date_short),
        "dataDate": get_last_trading_date(),
    })


# ---------------------------------------------------------------------------
# Tool: get_earnings_momentum
# ---------------------------------------------------------------------------
@yfinance_server.tool(
    name="get_earnings_momentum",
    description="""Get earnings revision momentum, beat rate, and estimate direction signals.

Returns: revision7d/30d/90d, revisionDirection, momentumFlag, beatRate, beatCount, avgSurprisePct, currentBeatStreak.

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of up to 5 symbols.
        When a list is provided, returns a dict keyed by symbol.
        Max 5 tickers per call; split larger lists into multiple calls.
""",
)
async def get_earnings_momentum(ticker: str | list[str]) -> str:
    """Return earnings momentum for one or more tickers."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_earnings_momentum(t))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        if fi.currency is None:
            return json.dumps({"error": True, "message": f"Ticker {ticker} not found", "ticker": ticker})
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    def _df_to_records(df):
        if df is None or (hasattr(df, "empty") and df.empty):
            return None
        df = df.reset_index()
        df.columns = [str(c) for c in df.columns]
        df = df.where(pd.notnull(df), None)
        return df.to_dict(orient="records")

    # Fetch EPS trend and earnings history
    eps_trend_records = None
    earnings_history_records = None
    try:
        eps_trend_records = _df_to_records(company.eps_trend)
    except Exception:
        pass
    try:
        earnings_history_records = _df_to_records(company.earnings_history)
    except Exception:
        pass

    output: dict = {"ticker": ticker}
    data_quality = "OK"

    # From epsTrend for current quarter (0q)
    revision_7d = None
    revision_30d = None
    revision_90d = None
    current_qtr_eps = None
    if eps_trend_records:
        # Find 0q row
        q0 = None
        for row in eps_trend_records:
            period = row.get("index") or row.get("period") or row.get("0")
            if period == "0q":
                q0 = row
                break
        if q0 is None and len(eps_trend_records) > 0:
            q0 = eps_trend_records[0]

        if q0:
            current = q0.get("current")
            ago_7d = q0.get("7daysAgo")
            ago_30d = q0.get("30daysAgo")
            ago_90d = q0.get("90daysAgo")
            current_qtr_eps = current

            # abs() in denominator is intentional: when EPS goes from negative
            # to less-negative (e.g. -0.50→-0.30), the revision is positive.
            # Without abs(), (-0.30-(-0.50))/-0.50 = -40%, which incorrectly
            # signals a downgrade.
            if current is not None and ago_7d is not None and ago_7d != 0:
                revision_7d = round((current - ago_7d) / abs(ago_7d) * 100, 2)
            if current is not None and ago_30d is not None and ago_30d != 0:
                revision_30d = round((current - ago_30d) / abs(ago_30d) * 100, 2)
            if current is not None and ago_90d is not None and ago_90d != 0:
                revision_90d = round((current - ago_90d) / abs(ago_90d) * 100, 2)

    # Revision direction
    if revision_30d is not None:
        if abs(revision_30d) < 3:
            revision_direction = "STABLE"
        elif revision_30d > 0:
            revision_direction = "UPGRADING"
        else:
            revision_direction = "DOWNGRADING"
    else:
        revision_direction = None

    # Momentum flag
    if revision_30d is not None:
        if revision_30d > 10:
            momentum_flag = "STRONG"
        elif revision_30d >= 0:
            momentum_flag = "POSITIVE"
        elif revision_30d > -10:
            momentum_flag = "NEGATIVE"
        else:
            momentum_flag = "COLLAPSING"
    else:
        momentum_flag = None

    # From earningsHistory (last 4 quarters)
    beat_count = 0
    total_quarters = 0
    surprises = []
    beat_streak = 0

    if earnings_history_records:
        for row in earnings_history_records:
            actual = row.get("epsActual")
            estimate = row.get("epsEstimate")
            surprise_pct = row.get("surprisePercent")
            if actual is not None and estimate is not None:
                total_quarters += 1
                if actual > estimate:
                    beat_count += 1
                if surprise_pct is not None:
                    surprises.append(float(surprise_pct) * 100 if abs(float(surprise_pct)) < 1 else float(surprise_pct))

        # Beat streak (consecutive from most recent)
        for row in earnings_history_records:
            actual = row.get("epsActual")
            estimate = row.get("epsEstimate")
            if actual is not None and estimate is not None:
                if actual > estimate:
                    beat_streak += 1
                else:
                    break

    beat_rate = round(beat_count / total_quarters, 2) if total_quarters > 0 else None
    avg_surprise = round(sum(surprises) / len(surprises), 2) if surprises else None

    if any(v is None for v in [revision_30d, beat_rate]):
        data_quality = "PARTIAL"

    output.update({
        "currentQtrEpsEstimate": current_qtr_eps,
        "revision7d": revision_7d,
        "revision30d": revision_30d,
        "revision90d": revision_90d,
        "revisionDirection": revision_direction,
        "momentumFlag": momentum_flag,
        "beatRate": beat_rate,
        "beatCount": beat_count,
        "totalQuarters": total_quarters,
        "avgSurprisePct": avg_surprise,
        "currentBeatStreak": beat_streak,
        "dataQuality": data_quality,
        "dataDate": get_last_trading_date(),
    })
    return json.dumps(output)


# ---------------------------------------------------------------------------
# Tool: get_options_flow_summary
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_options_flow_summary",
    description="""Get options flow summary: P/C ratio, IV percentile, max pain strike, highest OI strikes. Single ticker only.

Args:
    ticker: str — single ticker
    expiry_hint: str | None — optional YYYY-MM-DD; if omitted, selects nearest liquid expiry
""",
)
async def get_options_flow_summary(ticker: str, expiry_hint: str | None = None) -> str:
    """Return options flow summary for a single ticker."""
    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            return json.dumps({"error": True, "message": f"Ticker {ticker} not found", "ticker": ticker})
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    try:
        expirations = company.options
    except Exception as e:
        return json.dumps({"error": True, "message": f"No options data: {e}", "ticker": ticker})

    if not expirations:
        return json.dumps({"error": True, "message": "No options expirations available", "ticker": ticker})

    last_price = company.fast_info.last_price

    # Select expiry
    selected_expiry = None
    if expiry_hint and expiry_hint in expirations:
        selected_expiry = expiry_hint
    else:
        # Select nearest expiry with sufficient OI
        for exp in expirations:
            try:
                chain = company.option_chain(exp)
                total_oi = chain.calls["openInterest"].sum() + chain.puts["openInterest"].sum()
                if total_oi > 500:
                    selected_expiry = exp
                    break
            except Exception:
                continue

    if selected_expiry is None:
        selected_expiry = expirations[0]

    try:
        chain = company.option_chain(selected_expiry)
    except Exception as e:
        return json.dumps({"error": True, "message": f"Failed to fetch chain: {e}", "ticker": ticker})

    calls = chain.calls
    puts = chain.puts

    total_call_oi = int(calls["openInterest"].sum()) if "openInterest" in calls.columns else 0
    total_put_oi = int(puts["openInterest"].sum()) if "openInterest" in puts.columns else 0

    pc_ratio = round(total_put_oi / total_call_oi, 3) if total_call_oi > 0 else None

    if pc_ratio is not None:
        if pc_ratio > 1.5:
            pc_sentiment = "PUT_HEAVY"
        elif pc_ratio < 0.7:
            pc_sentiment = "CALL_HEAVY"
        else:
            pc_sentiment = "NEUTRAL"
    else:
        pc_sentiment = None

    # ATM strike and IV
    atm_strike = None
    atm_iv = None
    if last_price is not None and "strike" in calls.columns:
        calls_valid = calls.dropna(subset=["impliedVolatility"])
        if not calls_valid.empty:
            atm_idx = (calls_valid["strike"] - last_price).abs().idxmin()
            atm_strike = float(calls_valid.loc[atm_idx, "strike"])
            atm_iv = round(float(calls_valid.loc[atm_idx, "impliedVolatility"]), 3)

    # IV percentile
    all_ivs = []
    if "impliedVolatility" in calls.columns:
        all_ivs.extend(calls["impliedVolatility"].dropna().tolist())
    if "impliedVolatility" in puts.columns:
        all_ivs.extend(puts["impliedVolatility"].dropna().tolist())

    iv_pctile = None
    if atm_iv is not None and all_ivs:
        below = sum(1 for iv in all_ivs if iv <= atm_iv)
        iv_pctile = int(round(below / len(all_ivs) * 100))

    iv_flag = "⚠️ HIGH IV" if iv_pctile is not None and iv_pctile > 70 else None

    # Max pain calculation
    max_pain_strike = None
    if "strike" in calls.columns and "openInterest" in calls.columns:
        all_strikes = sorted(set(calls["strike"].tolist() + puts["strike"].tolist()))
        if all_strikes:
            min_pain = float("inf")
            for strike in all_strikes:
                call_pain = sum(
                    max(0, strike - row_strike) * oi
                    for row_strike, oi in zip(calls["strike"], calls["openInterest"].fillna(0))
                )
                put_pain = sum(
                    max(0, row_strike - strike) * oi
                    for row_strike, oi in zip(puts["strike"], puts["openInterest"].fillna(0))
                )
                total_pain = call_pain + put_pain
                if total_pain < min_pain:
                    min_pain = total_pain
                    max_pain_strike = float(strike)

    # Highest OI strikes
    highest_oi_call = None
    highest_oi_put = None
    if "openInterest" in calls.columns and not calls.empty:
        idx = calls["openInterest"].idxmax()
        if pd.notna(idx):
            highest_oi_call = float(calls.loc[idx, "strike"])
    if "openInterest" in puts.columns and not puts.empty:
        idx = puts["openInterest"].idxmax()
        if pd.notna(idx):
            highest_oi_put = float(puts.loc[idx, "strike"])

    return json.dumps({
        "ticker": ticker,
        "expiryDate": selected_expiry,
        "totalCallOI": total_call_oi,
        "totalPutOI": total_put_oi,
        "pcRatio": pc_ratio,
        "pcSentiment": pc_sentiment,
        "atmStrike": atm_strike,
        "atmIV": atm_iv,
        "ivPctile": iv_pctile,
        "ivFlag": iv_flag,
        "maxPainStrike": max_pain_strike,
        "highestOICallStrike": highest_oi_call,
        "highestOIPutStrike": highest_oi_put,
        "dataDate": str(datetime.date.today()),
    })


# ---------------------------------------------------------------------------
# Tool: get_put_hedge_candidates
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_put_hedge_candidates",
    description="""Get pre-filtered OTM put options within a strike range and budget. Single ticker only.

Args:
    ticker: str — single ticker
    otm_pct_min: float — minimum OTM % (default: 8)
    otm_pct_max: float — maximum OTM % (default: 12)
    budget_usd: float — max premium per contract (100 shares)
    expiry_after: str — YYYY-MM-DD minimum expiry date
""",
)
async def get_put_hedge_candidates(
    ticker: str,
    otm_pct_min: float = 8.0,
    otm_pct_max: float = 12.0,
    budget_usd: float = 500.0,
    expiry_after: str = "",
) -> str:
    """Return filtered put hedge candidates."""
    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        current_price = fi.last_price
        if current_price is None:
            return json.dumps({"error": True, "message": f"No price for {ticker}", "ticker": ticker})
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    try:
        expirations = company.options
    except Exception as e:
        return json.dumps({"error": True, "message": f"No options: {e}", "ticker": ticker})

    # Filter expiries >= expiry_after
    if expiry_after:
        qualifying_expiries = [e for e in expirations if e >= expiry_after]
    else:
        qualifying_expiries = list(expirations)

    # Select nearest 2
    qualifying_expiries = qualifying_expiries[:2]

    if not qualifying_expiries:
        return json.dumps({"error": True, "message": "No qualifying expiry dates", "ticker": ticker})

    strike_min = current_price * (1 - otm_pct_max / 100)
    strike_max = current_price * (1 - otm_pct_min / 100)

    candidates = []
    for exp in qualifying_expiries:
        try:
            chain = company.option_chain(exp)
            puts_df = chain.puts
        except Exception:
            continue

        # Filter strikes
        filtered = puts_df[(puts_df["strike"] >= strike_min) & (puts_df["strike"] <= strike_max)]

        # Collect IVs for percentile calculation
        all_ivs = puts_df["impliedVolatility"].dropna().tolist() if "impliedVolatility" in puts_df.columns else []

        for _, row in filtered.iterrows():
            bid = float(row.get("bid", 0) or 0)
            ask = float(row.get("ask", 0) or 0)
            mid = round((bid + ask) / 2, 2)
            contract_cost = round(mid * 100, 2)
            within_budget = contract_cost <= budget_usd
            strike = float(row["strike"])
            oi = int(row.get("openInterest", 0) or 0)
            iv = float(row.get("impliedVolatility", 0) or 0)

            # IV percentile within chain
            iv_pctile = None
            if all_ivs and iv > 0:
                below = sum(1 for v in all_ivs if v <= iv)
                iv_pctile = int(round(below / len(all_ivs) * 100))

            iv_flag = "⚠️ HIGH IV" if iv_pctile is not None and iv_pctile > 70 else None
            otm_pct = round((current_price - strike) / current_price * 100, 2)

            candidates.append({
                "expiry": exp,
                "strike": strike,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "contractCost": contract_cost,
                "withinBudget": within_budget,
                "openInterest": oi,
                "ivPctile": iv_pctile,
                "ivFlag": iv_flag,
                "otmPct": otm_pct,
            })

    # Sort by expiry then strike
    candidates.sort(key=lambda c: (c["expiry"], c["strike"]))

    budget_feasible = any(c["withinBudget"] for c in candidates)

    # Generate note
    if not candidates:
        note = "No put options found in the specified OTM range."
        budget_gap = None
    elif not budget_feasible:
        nearest = min(candidates, key=lambda c: c["contractCost"])
        budget_gap = round(nearest["contractCost"] - budget_usd, 2)
        note = f"No candidates within budget. Nearest: ${nearest['strike']} put at ${nearest['contractCost']}/contract vs ${budget_usd} budget."
    else:
        budget_gap = None
        count = sum(1 for c in candidates if c["withinBudget"])
        note = f"{count} candidate(s) within ${budget_usd} budget."

    return json.dumps({
        "ticker": ticker,
        "currentPrice": round(current_price, 2),
        "strikeRangeMin": round(strike_min, 2),
        "strikeRangeMax": round(strike_max, 2),
        "budgetUsd": budget_usd,
        "candidates": candidates,
        "budgetFeasible": budget_feasible,
        "budgetGapUsd": budget_gap,
        "note": note,
        "dataDate": get_last_trading_date(),
    })


# ---------------------------------------------------------------------------
# Tool: get_analyst_upgrade_radar
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_analyst_upgrade_radar",
    description="""Get recent analyst rating changes with pre-computed signal classification. Batch supported.

Returns: changes with signal, ptFrom, ptTo, ptDirection, mixedSignal, strengthFlag; netSentiment, summary.

ptFrom / ptTo: prior and new price target (null — yfinance does not expose numeric targets; stubs for
future compatibility). ptDirection: RAISE/CUT/UNCHANGED/INITIATED — derived from ptFrom→ptTo when
both are available; INITIATED for new coverage; UNCHANGED for reiterations with no target change.

Args:
    ticker: str | list[str] — single or batch
    days_back: int — lookback window in calendar days (default: 30)
""",
)
async def get_analyst_upgrade_radar(ticker: str | list[str], days_back: int = 30) -> str:
    """Return recent analyst upgrades/downgrades with signals."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_analyst_upgrade_radar(t, days_back))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})

    company = yf.Ticker(ticker)
    try:
        ud = company.upgrades_downgrades
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    if ud is None or (hasattr(ud, "empty") and ud.empty):
        return json.dumps({
            "ticker": ticker,
            "windowDays": days_back,
            "netSentiment": 0,
            "changes": [],
            "summary": "NO CHANGES",
            "dataDate": get_last_trading_date(),
        })

    ud = ud.reset_index()
    cutoff = pd.Timestamp.now() - pd.DateOffset(days=days_back)

    # Filter to window
    if "GradeDate" in ud.columns:
        ud = ud[ud["GradeDate"] >= cutoff]
    elif "Date" in ud.columns:
        ud = ud[ud["Date"] >= cutoff]

    ud = ud.sort_values(ud.columns[0], ascending=False)

    changes = []
    upgrade_count = 0
    downgrade_count = 0

    _upgrade_grades = {"Buy", "Outperform", "Overweight", "Strong Buy", "Positive", "Market Outperform", "Top Pick"}
    _downgrade_grades = {"Sell", "Underperform", "Underweight", "Strong Sell", "Negative", "Market Underperform", "Reduce"}

    for _, row in ud.iterrows():
        from_grade = row.get("FromGrade", "")
        to_grade = row.get("ToGrade", "")
        firm = row.get("Firm", "")
        action = row.get("Action", "")

        date_val = row.get("GradeDate") or row.get("Date")
        date_str = str(date_val.date()) if hasattr(date_val, "date") else str(date_val)

        # Signal classification
        if action in ("up", "upgrade", "Up", "Upgrade") or to_grade in _upgrade_grades:
            signal = "UPGRADE"
            upgrade_count += 1
        elif action in ("down", "downgrade", "Down", "Downgrade") or to_grade in _downgrade_grades:
            signal = "DOWNGRADE"
            downgrade_count += 1
        else:
            signal = "MAINTAIN"

        # Price target fields — yfinance upgrades_downgrades doesn't expose
        # numeric price targets; stubs are included for forward-compatibility.
        pt_from: float | None = None
        pt_to: float | None = None

        # Derive ptDirection: use ptFrom/ptTo comparison when available;
        # fall back to grade-change signal when PT numerics are absent (Option A);
        # UNCHANGED for reiterations.
        if pt_from is not None and pt_to is not None:
            if pt_to > pt_from:
                pt_direction = "RAISE"
            elif pt_to < pt_from:
                pt_direction = "CUT"
            else:
                pt_direction = "UNCHANGED"
        elif action in ("initiated", "Initiated", "init"):
            pt_direction = "INITIATED"
        elif signal == "MAINTAIN":
            pt_direction = "UNCHANGED"
        elif signal == "UPGRADE":
            pt_direction = "RAISE"
        elif signal == "DOWNGRADE":
            pt_direction = "CUT"
        else:
            pt_direction = None

        mixed_signal = signal == "UPGRADE" and pt_direction == "CUT"

        # Strength flag
        if signal == "UPGRADE" and not mixed_signal:
            strength_flag = "BULLISH"
        elif signal == "DOWNGRADE":
            strength_flag = "BEARISH"
        elif mixed_signal:
            strength_flag = "MIXED"
        else:
            strength_flag = "NEUTRAL"

        changes.append({
            "date": date_str,
            "firm": firm,
            "fromGrade": from_grade,
            "toGrade": to_grade,
            "signal": signal,
            "ptFrom": pt_from,
            "ptTo": pt_to,
            "ptDirection": pt_direction,
            "mixedSignal": mixed_signal,
            "strengthFlag": strength_flag,
        })

    net_sentiment = upgrade_count - downgrade_count

    # Summary
    parts = []
    if upgrade_count:
        parts.append(f"{upgrade_count} UPGRADE(s)")
    if downgrade_count:
        parts.append(f"{downgrade_count} DOWNGRADE(s)")
    summary = ", ".join(parts) if parts else "NO CHANGES"

    return json.dumps({
        "ticker": ticker,
        "windowDays": days_back,
        "netSentiment": net_sentiment,
        "changes": changes,
        "summary": summary,
        "dataDate": get_last_trading_date(),
    })


# ---------------------------------------------------------------------------
# Tool: get_etf_info
# ---------------------------------------------------------------------------

_ETF_INFO_FIELDS = [
    "shortName", "quoteType", "category", "fundFamily", "legalType", "fundInceptionDate",
    "navPrice", "previousClose", "open", "dayHigh", "dayLow", "volume", "averageVolume",
    "totalAssets", "yield", "annualReportExpenseRatio", "ytdReturn", "beta3Year",
    "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "fiftyTwoWeekChange",
    "fiftyDayAverage", "twoHundredDayAverage",
]


def _df_to_records(df) -> list | None:
    """Convert a DataFrame to a JSON-serialisable list of records, or None if empty."""
    if df is None or df.empty:
        return None
    return json.loads(df.reset_index().to_json(orient="records"))


@yfinance_server.tool(
    name="get_etf_info",
    description="""Get ETF or mutual fund data for one or more ticker symbols.

Returns identity (shortName, category, fundFamily, legalType, fundInceptionDate),
pricing (navPrice, previousClose, open, dayHigh, dayLow, volume, averageVolume),
AUM/costs (totalAssets, yield, annualReportExpenseRatio, ytdReturn, beta3Year),
52-week stats (fiftyTwoWeekHigh, fiftyTwoWeekLow, fiftyTwoWeekChange),
moving averages (fiftyDayAverage, twoHundredDayAverage),
top-10 holdings (topHoldings), and sector weights (sectorWeights).

Use this tool for ETF and fund tickers: SPY, QQQ, VTI, ARKK, VFIAX, etc.
For individual stocks, use get_fast_info or get_stock_info instead.

Args:
    ticker: str | list[str]
        A single ETF/fund ticker (e.g. "SPY") or a list of up to 5 symbols.
        When a list is provided, returns a dict keyed by symbol.
        Max 5 tickers per call; split larger lists into multiple calls.
""",
)
async def get_etf_info(ticker: str | list[str]) -> str:
    """Get ETF/fund information for one or more ticker symbols."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_etf_info(t))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})

    cache_key = f"etf_info:{ticker}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        info = company.info
    except Exception as e:
        return f"Error: getting ETF info for {ticker}: {e}"

    data: dict = {k: info.get(k) for k in _ETF_INFO_FIELDS}

    # Top-10 holdings from funds_top_holdings DataFrame
    try:
        holdings_df = company.funds_top_holdings
        records = _df_to_records(None if holdings_df is None else holdings_df.head(10))
        data["topHoldings"] = records
    except Exception:
        data["topHoldings"] = None

    # Sector weights from funds_sector_weightings DataFrame
    try:
        data["sectorWeights"] = _df_to_records(company.funds_sector_weightings)
    except Exception:
        data["sectorWeights"] = None

    result = json.dumps(data)
    _cache_set(cache_key, result)
    return result



# ---------------------------------------------------------------------------
# Tool: get_overnight_quote
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_overnight_quote",
    description="""Get overnight trading data for a ticker.

The true overnight window is 20:00–04:00 ET (00:00–08:00 UTC / Blue Ocean ATS).
If no bars fall in that window, falls back to the most recent pre-market bar
(04:00–09:30 ET / 08:00–13:30 UTC) with fallback=true and a note.

For crypto/futures (24/7 markets), returns true exchange data with real volume
(dataSource=EXCHANGE). For equities, OTC indicative quotes typically have Volume=0
(dataSource=OTC_INDICATIVE).

Returns: overnightPrice, overnightTime, overnightHigh, overnightLow, overnightOpen,
overnightVolume, sessionDate, timezone, previousClose, gapPct, gapDirection,
dataSource, isBlueOceanWindow, isStale, dataAgeHours, fallback, note.

Args:
    ticker: str
        The ticker symbol, e.g. "BTC-USD", "ASTS", or "ES=F"
""",
)
async def get_overnight_quote(ticker: str) -> str:
    """Get overnight (00:00–08:00 UTC) OHLCV data for a ticker with data quality flags."""
    import zoneinfo

    cache_key = f"overnight_quote:{ticker}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        hist = await _fetch_with_retry(
            company.history, period="5d", interval="1h", prepost=True, auto_adjust=False
        )
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    if hist is None or hist.empty:
        return json.dumps({
            "ticker": ticker,
            "overnightPrice": None,
            "overnightTime": None,
            "overnightHigh": None,
            "overnightLow": None,
            "overnightOpen": None,
            "overnightVolume": None,
            "_note": "No price history available for this ticker",
        })

    # Exchange timezone (used for sessionDate and tz label)
    try:
        tz_name = company.fast_info.timezone
    except Exception:
        tz_name = "UTC"

    try:
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = zoneinfo.ZoneInfo("UTC")
        tz_name = "UTC"

    # Previous close for gap calculation.
    # Use bracket access fi["previousClose"] which reliably translates camelCase
    # to the snake_case FastInfo property via __getitem__.
    prev_close = None
    try:
        prev_close = company.fast_info["previousClose"]
    except Exception:
        pass
    if not prev_close:
        try:
            _info_pc = company.info
            prev_close = (
                _info_pc.get("regularMarketPreviousClose") or _info_pc.get("previousClose")
            )
        except Exception:
            pass

    # UTC index — true overnight window is 00:00–08:00 UTC (= 20:00–04:00 ET)
    utc_index = hist.index.tz_convert("UTC")
    overnight_mask = (utc_index.hour >= 0) & (utc_index.hour < 8)
    overnight = hist[overnight_mask]

    is_fallback = False
    if overnight.empty:
        # Fallback: most recent prepost bar before 13:30 UTC (09:30 ET market open)
        premarket_mask = utc_index.hour < 13
        premarket = hist[premarket_mask]
        if premarket.empty:
            result = json.dumps({
                "ticker": ticker,
                "overnightPrice": None,
                "overnightTime": None,
                "overnightHigh": None,
                "overnightLow": None,
                "overnightOpen": None,
                "overnightVolume": None,
                "_note": "No overnight or pre-market data found for this ticker",
            })
            _cache_set(cache_key, result)
            return result
        # Single-bar fallback — last available pre-market candle
        day_bars = premarket.iloc[[-1]]
        is_fallback = True
    else:
        # Use only the most recent UTC date that has overnight bars
        overnight_utc_index = utc_index[overnight_mask]
        latest_utc_date = max(overnight_utc_index.date)
        day_mask = pd.to_datetime(overnight_utc_index.date) == pd.Timestamp(latest_utc_date)
        day_bars = overnight[day_mask]
        if day_bars.empty:
            result = json.dumps({
                "ticker": ticker,
                "overnightPrice": None,
                "overnightTime": None,
                "overnightHigh": None,
                "overnightLow": None,
                "overnightOpen": None,
                "overnightVolume": None,
                "_note": "No overnight trading data found for this ticker",
            })
            _cache_set(cache_key, result)
            return result

    overnight_open   = float(day_bars["Open"].iloc[0])   if "Open"   in day_bars.columns else None
    overnight_high   = float(day_bars["High"].max())      if "High"   in day_bars.columns else None
    overnight_low    = float(day_bars["Low"].min())       if "Low"    in day_bars.columns else None
    overnight_price  = float(day_bars["Close"].iloc[-1])  if "Close"  in day_bars.columns else None
    overnight_volume = int(day_bars["Volume"].sum())      if "Volume" in day_bars.columns else None

    last_ts = day_bars.index[-1]
    last_ts_utc = pd.Timestamp(last_ts).tz_convert("UTC")
    overnight_time = last_ts_utc.isoformat()

    # sessionDate in exchange local timezone
    session_date = str(last_ts_utc.tz_convert(tz).date())

    # Data quality flags
    is_blue_ocean = last_ts_utc.hour < 8  # 00:00–08:00 UTC = true overnight (20:00–04:00 ET)
    data_source = "EXCHANGE" if (overnight_volume or 0) > 0 else "OTC_INDICATIVE"

    # Staleness: >6 hours old is considered stale
    now_utc = pd.Timestamp.now(tz="UTC")
    data_age_hours = round((now_utc - last_ts_utc).total_seconds() / 3600, 1)
    is_stale = data_age_hours > 6

    # Gap vs previous close
    gap_pct = None
    gap_direction = None
    if prev_close and overnight_price:
        gap_pct = round((overnight_price - prev_close) / prev_close * 100, 2)
        gap_direction = "UP" if gap_pct > 0.1 else ("DOWN" if gap_pct < -0.1 else "FLAT")

    result = json.dumps({
        "ticker": ticker,
        "overnightPrice": overnight_price,
        "overnightTime": overnight_time,
        "overnightHigh": overnight_high,
        "overnightLow": overnight_low,
        "overnightOpen": overnight_open,
        "overnightVolume": overnight_volume,
        "sessionDate": session_date,
        "timezone": tz_name,
        "previousClose": prev_close,
        "gapPct": gap_pct,
        "gapDirection": gap_direction,
        "dataSource": data_source,
        "isBlueOceanWindow": is_blue_ocean,
        "isStale": is_stale,
        "dataAgeHours": data_age_hours,
        "fallback": is_fallback,
        "note": (
            "True overnight window (20:00–04:00 ET) unavailable via Yahoo Finance API. "
            "Returning last pre-market OTC indicative quote as proxy."
        ) if is_fallback else None,
    })
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# EDGAR helpers (used by get_geographic_revenue)
# ---------------------------------------------------------------------------
import urllib.request as _urlreq

_EDGAR_TICKERS: dict[str, int] | None = None
_EDGAR_TICKERS_LOADED_AT: float = 0.0
_EDGAR_TTL = 24 * 3600  # 24h


async def _load_edgar_tickers() -> dict[str, int]:
    """Return ticker→CIK mapping from SEC EDGAR, refreshed every 24 h."""
    global _EDGAR_TICKERS, _EDGAR_TICKERS_LOADED_AT
    now = time.monotonic()
    if _EDGAR_TICKERS is not None and (now - _EDGAR_TICKERS_LOADED_AT) < _EDGAR_TTL:
        return _EDGAR_TICKERS
    loop = asyncio.get_event_loop()

    def _fetch() -> dict[str, int]:
        req = _urlreq.Request(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "yahoo-finance-mcp/1.0 (contact@example.com)"},
        )
        with _urlreq.urlopen(req, timeout=15) as resp:  # noqa: S310
            data = json.loads(resp.read())
        return {v["ticker"].upper(): int(v["cik_str"]) for v in data.values()}

    try:
        _EDGAR_TICKERS = await loop.run_in_executor(None, _fetch)
        _EDGAR_TICKERS_LOADED_AT = now
    except Exception:
        if _EDGAR_TICKERS is None:
            _EDGAR_TICKERS = {}
    return _EDGAR_TICKERS  # type: ignore[return-value]


async def _edgar_get(url: str) -> dict | None:
    """Fetch a JSON document from the SEC EDGAR API (runs in a thread executor)."""
    loop = asyncio.get_event_loop()

    def _fetch() -> dict | None:
        req = _urlreq.Request(
            url,
            headers={"User-Agent": "yahoo-finance-mcp/1.0 (contact@example.com)"},
        )
        try:
            with _urlreq.urlopen(req, timeout=10) as resp:  # noqa: S310
                return json.loads(resp.read())
        except Exception:
            return None

    return await loop.run_in_executor(None, _fetch)


# ---------------------------------------------------------------------------
# CR-10 — get_geographic_revenue
# ---------------------------------------------------------------------------

# Option C interim lookup table — confirmed values from 10-K annual filings.
# Commander verifies and updates each entry when a new 10-K is filed.
# pct is the decimal fraction (e.g. 0.117 = 11.7%).
_CHINA_REVENUE_CONFIRMED: dict[str, dict] = {
    "MU":   {"pct": 0.117, "fiscalYear": "FY2025", "filingDate": "2025-10-03"},
    "AAPL": {"pct": 0.170, "fiscalYear": "FY2025", "filingDate": "2025-10-31"},
    "ANET": {"pct": 0.140, "fiscalYear": "FY2024", "filingDate": "2025-02-14"},
    "QCOM": {"pct": 0.620, "fiscalYear": "FY2024", "filingDate": "2024-11-06"},
    "NVDA": {"pct": 0.170, "fiscalYear": "FY2025", "filingDate": "2025-02-26"},
    "AMD":  {"pct": 0.220, "fiscalYear": "FY2024", "filingDate": "2025-02-04"},
    "AVGO": {"pct": 0.350, "fiscalYear": "FY2024", "filingDate": "2024-12-19"},
    "SWKS": {"pct": 0.580, "fiscalYear": "FY2024", "filingDate": "2024-11-20"},
    "MRVL": {"pct": 0.550, "fiscalYear": "FY2025", "filingDate": "2025-03-13"},
    "ON":   {"pct": 0.330, "fiscalYear": "FY2024", "filingDate": "2025-02-10"},
}

@yfinance_server.tool(
    name="get_geographic_revenue",
    description="""Get geographic revenue % for a specified region from SEC EDGAR filing metadata and yfinance data.

Default region is 'China'. DC-151 CLASS A/B/C/D China Revenue Risk Classification (when region='China'):
  ≥20% → BANNED (CLASS B) | 15–20% → CHINA WATCH | ≤14.9% CLASS C eligible (cap 30%)

Returns: region, regionRevenuePct, regionRevenueUSD, fiscalYear, filingType, filingDate, segmentLabel,
source, confidence.

confidence:
  CONFIRMED — sourced from income statement segment (satisfies DC-151 Rule 1 annual confirmation).
  NOT_DISCLOSED — company does not break out the region separately in machine-readable form.
  When NOT_DISCLOSED, _manualLookup is populated with direct EDGAR URLs and step-by-step
  instructions to locate the geographic segment table in the 10-K.

Args:
    ticker: str
        The ticker symbol, e.g. "MU", "AAPL", "QCOM"
    region: str
        Geographic region to query, e.g. "China", "Europe", "Americas", "Asia Pacific", "Japan".
        Defaults to "China".
""",
)
async def get_geographic_revenue(ticker: str, region: str = "China") -> str:
    """Return geographic revenue % from SEC EDGAR / yfinance data."""
    cache_key = f"geographic_revenue:{ticker}:{region}"
    cached = _cache_get(cache_key, _EDGAR_TTL)
    if cached is not None:
        return cached

    # Option C: check confirmed lookup table for China (China only — other regions not yet confirmed)
    if region.lower() == "china":
        confirmed_entry = _CHINA_REVENUE_CONFIRMED.get(ticker.upper())
        if confirmed_entry:
            result = json.dumps({
                "ticker": ticker,
                "region": region,
                "regionRevenuePct": confirmed_entry["pct"],
                "regionRevenueUSD": None,
                "fiscalYear": confirmed_entry["fiscalYear"],
                "filingType": "10-K",
                "filingDate": confirmed_entry["filingDate"],
                "segmentLabel": "China",
                "source": "confirmed_lookup_table",
                "confidence": "CONFIRMED",
                "_note": None,
            })
            _cache_set(cache_key, result)
            return result

    # Step 1: Get CIK and latest 10-K filing metadata from EDGAR
    cik: int | None = None
    filing_date: str | None = None
    fiscal_year: str | None = None
    filing_type = "10-K"

    try:
        tickers_map = await _load_edgar_tickers()
        cik = tickers_map.get(ticker.upper())
    except Exception:
        pass

    if cik:
        cik_padded = str(cik).zfill(10)
        try:
            subs = await _edgar_get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json")
            if subs:
                filings = subs.get("filings", {}).get("recent", {})
                forms: list = filings.get("form", [])
                dates: list = filings.get("filingDate", [])
                periods: list = filings.get("reportDate", [])
                for i, form in enumerate(forms):
                    if form in ("10-K", "10-K405", "10-KSB"):
                        filing_date = dates[i] if i < len(dates) else None
                        period = periods[i] if i < len(periods) else None
                        if period:
                            fiscal_year = f"FY{period[:4]}"
                        break
        except Exception:
            pass

    # Step 2: Try yfinance income statement for geographic segment rows
    region_revenue_usd: float | None = None
    total_revenue_usd: float | None = None
    region_revenue_pct: float | None = None
    segment_label = region
    source = "not_available"
    confidence = "NOT_DISCLOSED"

    try:
        company = yf.Ticker(ticker)
        inc = company.income_stmt
        if inc is not None and not inc.empty:
            latest_col = inc.columns[0]
            if fiscal_year is None and hasattr(latest_col, "year"):
                fiscal_year = f"FY{latest_col.year}"

            # Total revenue
            for row_name in ["Total Revenue", "Revenue", "Net Sales", "Total Net Revenue"]:
                try:
                    val = inc.loc[row_name, latest_col]
                    if pd.notna(val):
                        total_revenue_usd = float(val)
                        break
                except (KeyError, TypeError):
                    pass

            # Build region-specific candidate row names
            r = region
            region_candidates = [r, f"{r} Revenue", f"Revenue from {r}", f"{r} and Other"]
            if r.lower() == "china":
                region_candidates += ["Greater China", "Mainland China", "China Segment"]
            else:
                region_candidates.append(f"{r} Segment")
            for row_name in region_candidates:
                try:
                    val = inc.loc[row_name, latest_col]
                    if pd.notna(val):
                        region_revenue_usd = float(val)
                        segment_label = row_name
                        source = "income_statement_segment"
                        confidence = "CONFIRMED"
                        break
                except (KeyError, TypeError):
                    pass
    except Exception:
        pass

    if region_revenue_usd is not None and total_revenue_usd and total_revenue_usd > 0:
        region_revenue_pct = round(region_revenue_usd / total_revenue_usd, 4)

    # Build a precise LLM-actionable pointer when data could not be resolved automatically.
    manual_lookup: dict | None = None
    if confidence == "NOT_DISCLOSED":
        t_upper = ticker.upper()
        cik_padded = str(cik).zfill(10) if cik else None
        edgar_search_url = (
            f"https://efts.sec.gov/LATEST/search-index?q=%22{region}%22"
            f"&forms=10-K&entity={t_upper}"
        )
        edgar_filings_url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
            f"&CIK={cik_padded}&type=10-K&dateb=&owner=include&count=5"
        ) if cik_padded else (
            f"https://www.sec.gov/cgi-bin/browse-edgar?company={t_upper}"
            f"&CIK=&type=10-K&dateb=&owner=include&count=5&search_text=&action=getcompany"
        )
        manual_lookup = {
            "reason": (
                f"Geographic revenue breakdown for '{region}' is not available in machine-readable "
                "form via this data pipeline. NOT_DISCLOSED does NOT satisfy DC-151 Rule 1 annual "
                "confirmation requirement."
            ),
            "action": (
                f"Open the most recent 10-K for {t_upper} and search for the section titled "
                "'Geographic Information', 'Geographic Areas', or 'Segment Information'. "
                f"Look for a table that lists revenue by region including '{region}'. "
                "Divide that figure by Total Revenue to compute regionRevenuePct."
            ),
            "edgarFullTextSearchUrl": edgar_search_url,
            "edgarFilingsPageUrl": edgar_filings_url,
            "cik": cik_padded,
            "filingDate": filing_date,
            "fiscalYear": fiscal_year,
        }

    result = json.dumps({
        "ticker": ticker,
        "region": region,
        "regionRevenuePct": region_revenue_pct,
        "regionRevenueUSD": region_revenue_usd,
        "fiscalYear": fiscal_year,
        "filingType": filing_type,
        "filingDate": filing_date,
        "segmentLabel": segment_label,
        "source": source,
        "confidence": confidence,
        "_manualLookup": manual_lookup,
    })
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# CR-12 — get_options_flow_scan
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_options_flow_scan",
    description="""Structured options flow scan for a binary event window.

Returns the formatted options flow output block. IO pastes formattedBlock directly into
session output. Prior window-label readings are cached server-side (72 h TTL) to enable trend
computation across readings (e.g. T-14 → T-7 → T-2).

Returns: pcRatio, ivPctile, putVolVs10dAvg, putVolTrend (INCREASING/STABLE/DECREASING),
maxPainStrike, bracket (UPPER/MID/LOWER), formattedBlock, dataDate.

Args:
    ticker: str
        The ticker symbol, e.g. "ASTS"
    window_label: str
        Free-form label for this reading, e.g. "T-14", "T-7", "T-2", "pre-earnings", "week1".
        Used as cache key for trend computation across readings.
""",
)
async def get_options_flow_scan(ticker: str, window_label: str) -> str:
    """Return structured options flow scan for the specified window label."""
    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        current_price: float | None = fi["lastPrice"]
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    if not current_price:
        return json.dumps({"error": True, "message": f"No price data for {ticker}", "ticker": ticker})

    # Get nearest-expiry options chain
    try:
        exps = company.options
        if not exps:
            return json.dumps({"error": True, "message": f"No options data for {ticker}", "ticker": ticker})
        exp = exps[0]
        chain = company.option_chain(exp)
        calls_df = chain.calls
        puts_df = chain.puts
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    # P/C ratio by volume
    call_vol = float(calls_df["volume"].sum(skipna=True)) if not calls_df.empty else 0.0
    put_vol = float(puts_df["volume"].sum(skipna=True)) if not puts_df.empty else 0.0
    pc_ratio: float | None = round(put_vol / call_vol, 2) if call_vol > 0 else None

    # Max pain strike — strike with maximum combined open interest
    max_pain_strike: float | None = None
    try:
        combined = pd.concat([
            calls_df[["strike", "openInterest"]],
            puts_df[["strike", "openInterest"]],
        ])
        oi_by_strike = combined.groupby("strike")["openInterest"].sum()
        if not oi_by_strike.empty:
            max_pain_strike = float(oi_by_strike.idxmax())
    except Exception:
        pass

    # ATM implied volatility (nearest call strike to current price)
    atm_iv: float | None = None
    try:
        if not calls_df.empty:
            _calls = calls_df.copy()
            _calls = _calls.assign(_dist=(_calls["strike"] - current_price).abs())
            atm_row = _calls.nsmallest(1, "_dist")
            if not atm_row.empty:
                iv_val = atm_row["impliedVolatility"].iloc[0]
                if pd.notna(iv_val):
                    atm_iv = float(iv_val)
    except Exception:
        pass

    # IV percentile — approximate using annualised 30-day rolling realised vol over 1 year
    iv_pctile: int | None = None
    try:
        hist_1y = company.history(period="1y", interval="1d")
        if hist_1y is not None and len(hist_1y) >= 30 and atm_iv is not None:
            rets = hist_1y["Close"].pct_change().dropna()
            roll_rv = rets.rolling(30).std() * (252 ** 0.5)
            roll_rv = roll_rv.dropna()
            if len(roll_rv) >= 5:
                rv_min, rv_max = float(roll_rv.min()), float(roll_rv.max())
                if rv_max > rv_min:
                    pctile = (atm_iv - rv_min) / (rv_max - rv_min) * 100
                    iv_pctile = max(0, min(100, round(pctile)))
    except Exception:
        pass

    # Put vol vs 10-day average proxy (since historical options volume is unavailable via yfinance)
    # Proxy: put vol as a multiple of 1% of the stock's 10d average daily volume.
    put_vol_vs_10d: float | None = None
    try:
        adv10 = fi["tenDayAverageVolume"]
        if adv10 and adv10 > 0 and put_vol > 0:
            put_vol_vs_10d = round(put_vol / (adv10 * 0.01), 2)
    except Exception:
        pass

    # Data date from history
    try:
        _h = company.history(period="5d", interval="1d")
        data_date = get_last_trading_date(_h)
    except Exception:
        data_date = get_last_trading_date()

    # Look up prior window reading for trend analysis
    prev_window_map = {"T-7": "T-14", "T-2": "T-7"}
    prev_window = prev_window_map.get(window_label)
    prev_data: dict | None = None
    if prev_window:
        prev_cached = _cache_get(f"options_flow:{ticker}:{prev_window}", 72 * 3600)
        if prev_cached:
            try:
                prev_data = json.loads(prev_cached)
            except Exception:
                pass

    # Put vol trend (compare primary metric with prior window reading)
    put_vol_trend = "STABLE"
    _cmp_curr: float | None = put_vol_vs_10d if put_vol_vs_10d is not None else pc_ratio
    _cmp_prev: float | None = None
    if prev_data:
        _cmp_prev = (
            prev_data.get("putVolVs10dAvg")
            if prev_data.get("putVolVs10dAvg") is not None
            else prev_data.get("pcRatio")
        )
    if _cmp_curr is not None and _cmp_prev is not None and _cmp_prev > 0:
        ratio_change = _cmp_curr / _cmp_prev
        if ratio_change > 1.1:
            put_vol_trend = "INCREASING"
        elif ratio_change < 0.9:
            put_vol_trend = "DECREASING"

    # Bracket classification
    bracket: str | None = None
    if pc_ratio is not None:
        if pc_ratio >= 1.3 or (pc_ratio >= 1.0 and put_vol_trend == "INCREASING"):
            bracket = "UPPER"
        elif pc_ratio <= 0.8 and put_vol_trend != "INCREASING":
            bracket = "LOWER"
        else:
            bracket = "MID"

    # Formatted block
    iv_str = f"{iv_pctile}th%ile" if iv_pctile is not None else "N/A"
    pv_str = f"{put_vol_vs_10d:.2f}x" if put_vol_vs_10d is not None else "N/A"
    pc_str = f"{pc_ratio:.2f}" if pc_ratio is not None else "N/A"
    formatted_block = (
        f"OPTIONS FLOW SCAN [{window_label}] {ticker} | "
        f"P/C: {pc_str} | "
        f"IV: {iv_str} | "
        f"Put vol vs 10d avg: {pv_str} | "
        f"Trend: {put_vol_trend} | "
        f"Advisory: {bracket or 'N/A'} bracket"
    )

    result_dict: dict = {
        "ticker": ticker,
        "windowLabel": window_label,
        "dataDate": data_date,
        "pcRatio": pc_ratio,
        "ivPctile": iv_pctile,
        "putVolVs10dAvg": put_vol_vs_10d,
        "putVolTrend": put_vol_trend,
        "maxPainStrike": max_pain_strike,
        "bracket": bracket,
        "formattedBlock": formatted_block,
    }

    # Cache current reading for future trend comparison (72h TTL via 3-day window check)
    _cache_set(f"options_flow:{ticker}:{window_label}", json.dumps(result_dict))
    return json.dumps(result_dict)


# ---------------------------------------------------------------------------
# CR-13 — get_price_target_bracket
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_price_target_bracket",
    description="""Compute EQF bracket for a position.

EQF = currentPrice / io_pt × 100. Used at every /snipe, /dca, and /thesis.

Bracket thresholds: ≤75% → STRONG_BUY | 75–90% → ACCEPTABLE | 90–100% → CAUTION | >100% → AVOID
Tag (DC-123): <40% → SPECULATIVE | 40–79% → LONG | 80–99% → NEAR | ≥100% → INVERTED

Note: pre-revenue positions should be overridden to SPECULATIVE by IO per DC-123. This tool
handles revenue-generating positions only.

Args:
    ticker: str
        The ticker symbol, e.g. "ASTS"
    io_pt: float
        IO's price target (Commander/IO-set, not analyst consensus)
""",
)
async def get_price_target_bracket(ticker: str, io_pt: float) -> str:
    """Return EQF bracket and tag for the current position."""
    if io_pt <= 0:
        return json.dumps({"error": True, "message": "io_pt must be a positive number", "ticker": ticker})

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        current_price: float | None = fi["lastPrice"]
        if current_price is None:
            return json.dumps({"error": True, "message": f"No price data for {ticker}", "ticker": ticker})
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    eqf_pct = round(current_price / io_pt * 100, 1)

    if eqf_pct <= 75:
        bracket = "STRONG_BUY"
    elif eqf_pct <= 90:
        bracket = "ACCEPTABLE"
    elif eqf_pct <= 100:
        bracket = "CAUTION"
    else:
        bracket = "AVOID"

    if eqf_pct < 40:
        tag = "SPECULATIVE"
    elif eqf_pct < 80:
        tag = "LONG"
    elif eqf_pct < 100:
        tag = "NEAR"
    else:
        tag = "INVERTED"

    inverted_flag = eqf_pct >= 100

    data_date: str = str(datetime.date.today())
    try:
        _h = company.history(period="5d", interval="1d")
        if _h is not None and not _h.empty:
            data_date = str(_h.index[-1].date())
    except Exception:
        pass

    return json.dumps({
        "ticker": ticker,
        "currentPrice": round(current_price, 4),
        "ioPt": io_pt,
        "eqfPct": eqf_pct,
        "bracket": bracket,
        "tag": tag,
        "invertedFlag": inverted_flag,
        "dataDate": data_date,
    })


# ---------------------------------------------------------------------------
# CR-14 — get_position_score_inputs
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_position_score_inputs",
    description="""Aggregate all position scoring inputs for T1, T2, T4, and T5 components.

T3 (PT proximity) and T2 (vs cost basis) require portfolio state (IO PT, cost basis) not available
here — IO scores those manually from currentPrice in t2_inputs and stored values.

Runs up to 6 parallel data fetches per call.

Returns: t1_inputs (analyst sentiment), t2_inputs (price vs 52wk), t4_inputs (earnings momentum),
t5_inputs (technical indicators), dataDate.

Args:
    ticker: str
        Single ticker symbol, e.g. "ASTS"
""",
)
async def get_position_score_inputs(ticker: str) -> str:
    """Return aggregated position scoring inputs for T1, T2, T4, and T5 components."""
    results = await asyncio.gather(
        get_analyst_upgrade_radar(ticker, days_back=30),
        get_analyst_consensus(ticker),
        get_price_stats(ticker),
        get_earnings_momentum(ticker),
        get_technical_indicators(ticker, "3mo"),
        get_ma_position(ticker),
        return_exceptions=True,
    )

    def _parse(r: object) -> dict:
        if isinstance(r, Exception):
            return {}
        try:
            return json.loads(str(r)) if isinstance(r, str) else {}
        except Exception:
            return {}

    upgrade = _parse(results[0])
    consensus = _parse(results[1])
    price = _parse(results[2])
    earnings = _parse(results[3])
    tech = _parse(results[4])
    ma = _parse(results[5])

    # T1: analyst sentiment
    t1: dict = {
        "analystNetSentiment": upgrade.get("netSentiment"),
        "upgrades30d": sum(
            1 for c in (upgrade.get("changes") or []) if c.get("signal") == "UPGRADE"
        ),
        "downgrades30d": sum(
            1 for c in (upgrade.get("changes") or []) if c.get("signal") == "DOWNGRADE"
        ),
        "dominantRating": consensus.get("dominantRating"),
        "analystCount": consensus.get("totalAnalysts"),
    }

    # T2: price vs 52-week range
    t2: dict = {
        "currentPrice": price.get("lastPrice"),
        "fiftyTwoWeekHigh": price.get("yearHigh"),
        "fiftyTwoWeekLow": price.get("yearLow"),
        "pctFromYearHigh": price.get("pctFromYearHigh"),
        "pctFromYearLow": price.get("pctFromYearLow"),
    }

    # T4: earnings momentum
    t4: dict = {
        "beatRate": earnings.get("beatRate"),
        "currentBeatStreak": earnings.get("currentBeatStreak"),
        "avgSurprisePct": earnings.get("avgSurprisePct"),
        "momentumFlag": earnings.get("momentumFlag"),
    }

    # T5: technical indicators
    t5: dict = {
        "rsi14": tech.get("rsi14"),
        "macd": tech.get("macd"),
        "macdHistogram": tech.get("macdHistogram"),
        "maPosition": ma.get("trend"),
        "pctFrom50dma": ma.get("pctVs50dma"),
        "pctFrom200dma": ma.get("pctVs200dma"),
        "lastClose": tech.get("lastClose"),
    }

    # Data date: prefer last OHLCV row from technical indicators (DC-05 clock discipline)
    data_date = tech.get("dataDate") or ma.get("dataDate") or get_last_trading_date()

    return json.dumps({
        "ticker": ticker,
        "dataDate": data_date,
        "t1_inputs": t1,
        "t2_inputs": t2,
        "t4_inputs": t4,
        "t5_inputs": t5,
    })


# ---------------------------------------------------------------------------
# CR-15 — get_volume_gate
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_volume_gate",
    description="""DC Section 6.2 Volume Gate check: regularMarketVolume ≥ 0.5 × 20-day ADV.

Returns lastVolume, adv10d, adv20d (computed from last 20 daily sessions), adv90d, ratio20d,
gatePass (true = volume gate PASS), dataDate, and a pre-formatted note.

foreignExchange: bool (default False). When True, applies the DC-80 FX gate: passes when
lastVolume × lastPrice ≥ $10M USD/day instead of the 0.5 × adv20d threshold.

Args:
    ticker: str
        The ticker symbol, e.g. "ASTS"
    foreign_exchange: bool
        Set True for DC-80 foreign exchange / ADR tickers. Default False.
""",
)
async def get_volume_gate(ticker: str, foreign_exchange: bool = False) -> str:
    """Return DC Section 6.2 volume gate assessment."""
    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        last_volume: int | None = fi["lastVolume"]
        adv10d: float | None = fi["tenDayAverageVolume"]
        adv90d: float | None = fi["threeMonthAverageVolume"]
        last_price: float | None = fi["lastPrice"]
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    # 20-day ADV from history
    adv20d: float | None = None
    data_date: str = str(datetime.date.today())
    try:
        hist = company.history(period="1mo", interval="1d")
        if hist is not None and not hist.empty:
            vols = hist["Volume"].dropna()
            if len(vols) >= 5:
                adv20d = round(float(vols.tail(20).mean()))
            data_date = str(hist.index[-1].date())
    except Exception:
        pass

    # Gate evaluation
    gate_pass: bool | None = None
    ratio20d: float | None = None
    note: str

    if foreign_exchange:
        # DC-80: daily notional ≥ $10M USD
        if last_volume is not None and last_price is not None and last_price > 0:
            daily_notional = last_volume * last_price
            gate_pass = daily_notional >= 10_000_000
            note = (
                f"Volume gate {'PASS' if gate_pass else 'FAIL'} (DC-80 FX) — "
                f"${daily_notional / 1_000_000:.1f}M daily notional "
                f"({'≥' if gate_pass else '<'} $10M threshold)"
            )
        else:
            note = "Volume gate UNKNOWN — insufficient price/volume data for DC-80 FX check"
    else:
        if last_volume is not None and adv20d and adv20d > 0:
            ratio20d = round(last_volume / adv20d, 2)
            gate_pass = ratio20d >= 0.5
            note = (
                f"Volume gate {'PASS' if gate_pass else 'FAIL'} — "
                f"{ratio20d:.2f}x 20d ADV"
            )
        else:
            note = "Volume gate UNKNOWN — insufficient volume data for 20d ADV calculation"

    return json.dumps({
        "ticker": ticker,
        "lastVolume": last_volume,
        "adv10d": adv10d,
        "adv20d": adv20d,
        "adv90d": adv90d,
        "ratio20d": ratio20d,
        "gatePass": gate_pass,
        "dataDate": data_date,
        "note": note,
    })


# ---------------------------------------------------------------------------
# Deprecated aliases — backward compat (remove in next major version)
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_china_revenue_pct",
    description="[DEPRECATED] Use get_geographic_revenue instead. Alias preserved for backward compatibility.",
)
async def _deprecated_get_china_revenue_pct(ticker: str) -> str:
    return await get_geographic_revenue(ticker, region="China")


@yfinance_server.tool(
    name="get_dc134_options_scan",
    description="[DEPRECATED] Use get_options_flow_scan instead. Alias preserved for backward compatibility.",
)
async def _deprecated_get_dc134_options_scan(ticker: str, window_day: str) -> str:
    return await get_options_flow_scan(ticker, window_label=window_day)


@yfinance_server.tool(
    name="get_eqf_bracket",
    description="[DEPRECATED] Use get_price_target_bracket instead. Alias preserved for backward compatibility.",
)
async def _deprecated_get_eqf_bracket(ticker: str, io_pt: float) -> str:
    return await get_price_target_bracket(ticker, io_pt)


@yfinance_server.tool(
    name="get_tps_inputs",
    description="[DEPRECATED] Use get_position_score_inputs instead. Alias preserved for backward compatibility.",
)
async def _deprecated_get_tps_inputs(ticker: str) -> str:
    return await get_position_score_inputs(ticker)


@yfinance_server.tool(
    name="get_adv_gate",
    description="[DEPRECATED] Use get_volume_gate instead. Alias preserved for backward compatibility.",
)
async def _deprecated_get_adv_gate(ticker: str, foreign_exchange: bool = False) -> str:
    return await get_volume_gate(ticker, foreign_exchange)


if __name__ == "__main__":
    # Initialize and run the server
    print("Starting Yahoo Finance MCP server...")
    yfinance_server.run(transport="stdio")
