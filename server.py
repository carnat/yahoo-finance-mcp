import asyncio
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


async def _fetch_with_retry(fn, *args, retries: int = 1, delay: float = 2.0):
    """Call fn(*args) with one retry on exception, waiting `delay` seconds."""
    for attempt in range(retries + 1):
        try:
            return fn(*args)
        except Exception:
            if attempt < retries:
                await asyncio.sleep(delay)
            else:
                raise


# Initialize FastMCP server
yfinance_server = FastMCP(
    "yfinance",
    instructions="""
# Yahoo Finance MCP Server

This server provides financial market data from Yahoo Finance.

## Tool selection guidance
- **Prefer `get_fast_info`** over `get_stock_info` for current price, market cap, 52-week range, or moving averages — it returns ~15 fields instead of 120+ and uses far fewer tokens.
- Use `get_stock_info` only when you need deep fundamentals, business description, or fields not in fast_info.
- **Prefer `get_financial_ratios`** over fetching full financial statements when you need valuation or profitability ratios — ratios are pre-computed server-side.
- **Prefer `get_analyst_consensus`** over `get_recommendations` when you need a quick summary of analyst sentiment and price targets.
- **Prefer `get_earnings_analysis`** to get all forward-looking analyst estimates in a single call instead of five separate calls.
- Use `search_ticker` to resolve a company name or ISIN to a ticker symbol before calling other tools.
- Use `screen_stocks` to discover stocks matching criteria (e.g., day_gainers, most_actives) without iterating tickers manually.

## Available tools

### Price & market data
- get_fast_info: **Lightweight** — current price, market cap, 52-week high/low, moving averages, volume (~15 fields). Prefer this for price lookups.
- get_historical_stock_prices: OHLCV history. Supports period, interval, and optional columns filter to reduce output size.
- get_stock_info: **Heavyweight** — full ~120-field company info dict. Use only when fast_info is insufficient. Supports optional fields filter.
- get_price_stats: Pre-computed price statistics: % change vs 52-week high/low, distance from moving averages, 30-day volatility, and CAGR.
- get_stock_actions: Dividend and split history.

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
""",
)
async def get_historical_stock_prices(
    ticker: str, period: str = "1mo", interval: str = "1d",
    columns: list[str] | None = None,
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
    cache_key = f"hist:{ticker}:{period}:{interval}"
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
        hist_data = await _fetch_with_retry(company.history, period, interval)
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
    description="""Get stock information for a given ticker symbol from yahoo finance. Include the following information:
Stock Price & Trading Info, Company Information, Financial Metrics, Earnings & Revenue, Margins & Returns, Dividends, Balance Sheet, Ownership, Analyst Coverage, Risk Metrics, Other.

IMPORTANT: This tool returns 120+ fields and is token-heavy. Prefer get_fast_info for price/market-cap lookups.
Use this tool only when you need deep fundamentals, the business description, or fields not available in get_fast_info.
Use the optional `fields` parameter to request only the fields you need.

Args:
    ticker: str
        The ticker symbol of the stock to get information for, e.g. "AAPL"
    fields: list[str] | None
        Optional list of field names to return, e.g. ["shortName", "sector", "industry", "fullTimeEmployees"].
        If omitted, all ~120+ fields are returned. Specify only what you need to reduce token usage.
""",
)
async def get_stock_info(ticker: str, fields: list[str] | None = None) -> str:
    """Get stock information for a given ticker symbol"""
    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting stock information for {ticker}: {e}")
        return f"Error: getting stock information for {ticker}: {e}"
    info = company.info
    if fields:
        info = {k: info[k] for k in fields if k in info}
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

    return df.to_json(orient="records", date_format="iso")


@yfinance_server.tool(
    name="get_recommendations",
    description="""Get recommendations or upgrades/downgrades for a given ticker symbol from yahoo finance. You can also specify the number of months back to get upgrades/downgrades for, default is 12.

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
    description="""Get lightweight real-time price and market data for a ticker symbol. Returns ~20 high-signal fields.

PREFER THIS over get_stock_info for any query involving current price, market cap, 52-week range,
moving averages, or trading volume — it uses ~85-90% fewer tokens than get_stock_info.

Fields returned: currency, exchange, quoteType, timezone, lastPrice, open, previousClose,
dayHigh, dayLow, yearHigh, yearLow, yearChange, marketCap, shares, lastVolume,
tenDayAverageVolume, threeMonthAverageVolume, fiftyDayAverage, twoHundredDayAverage.

Args:
    ticker: str
        The ticker symbol of the stock, e.g. "AAPL"
""",
)
async def get_fast_info(ticker: str) -> str:
    """Get lightweight real-time price and market data for a ticker symbol."""
    cache_key = f"fast_info:{ticker}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        result = json.dumps({k: getattr(fi, k, None) for k in fi.keys()})
    except Exception as e:
        print(f"Error: getting fast info for {ticker}: {e}")
        return f"Error: getting fast info for {ticker}: {e}"

    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.1 — get_price_stats
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_price_stats",
    description="""Get pre-computed price statistics for a ticker. Returns a compact summary so you do NOT
need to fetch raw history and compute these yourself.

Includes:
- Current price, previous close, % change today
- % distance from 52-week high and 52-week low
- % distance from 50-day and 200-day moving averages
- 30-day realized annualized volatility (from daily close returns)
- CAGR over 1y, 3y, 5y (where data is available)

Args:
    ticker: str
        The ticker symbol, e.g. "AAPL"
""",
)
async def get_price_stats(ticker: str) -> str:
    """Get pre-computed price statistics for a ticker."""
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

    result = json.dumps(stats)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.2 — get_analyst_consensus
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_analyst_consensus",
    description="""Get a compact analyst consensus summary for a ticker.

Returns pre-aggregated data so you do NOT need to call get_recommendations separately.
Includes:
- Consensus price target (current, low, high, mean, median) and % upside from current price
- Recommendation breakdown (strongBuy, buy, hold, sell, strongSell counts) for recent periods
- Dominant consensus rating

Args:
    ticker: str
        The ticker symbol, e.g. "AAPL"
""",
)
async def get_analyst_consensus(ticker: str) -> str:
    """Get compact analyst consensus summary."""
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
    description="""Get pre-computed key financial ratios for a ticker.

PREFER THIS over fetching full financial statements when you need valuation or profitability ratios.
Ratios are computed server-side from company.info so the LLM does not have to process raw statements.

Includes:
- Valuation: P/E (trailing & forward), P/S, P/B, EV/EBITDA, EV/Revenue, PEG ratio
- Profitability: Gross/Operating/Net margins, ROE, ROA
- Leverage: Debt/Equity, Current ratio, Quick ratio
- Cash flow: Free Cash Flow, FCF yield (FCF / market cap)
- Dividend: Yield, Payout ratio

Args:
    ticker: str
        The ticker symbol, e.g. "AAPL"
""",
)
async def get_financial_ratios(ticker: str) -> str:
    """Get pre-computed key financial ratios."""
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

    result = json.dumps(ratios)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.5 — get_calendar
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_calendar",
    description="""Get upcoming earnings date and dividend schedule for a ticker.

Returns:
- Next earnings date range and EPS/revenue estimates
- Ex-dividend date and dividend pay date

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

    output = {"ticker": ticker, "calendar": {k: _serialize(v) for k, v in cal.items()}}
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
""",
)
async def search_ticker(query: str, max_results: int = 8) -> str:
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


if __name__ == "__main__":
    # Initialize and run the server
    print("Starting Yahoo Finance MCP server...")
    yfinance_server.run(transport="stdio")
