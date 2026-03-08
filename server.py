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
    balance_sheet = "balance_sheet"
    quarterly_balance_sheet = "quarterly_balance_sheet"
    cashflow = "cashflow"
    quarterly_cashflow = "quarterly_cashflow"


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

This server is used to get information about a given ticker symbol from yahoo finance.

Available tools:
- get_historical_stock_prices: Get historical stock prices for a given ticker symbol from yahoo finance. Include the following information: Date, Open, High, Low, Close (adjusted), Volume.
- get_stock_info: Get stock information for a given ticker symbol from yahoo finance. Include the following information: Stock Price & Trading Info, Company Information, Financial Metrics, Earnings & Revenue, Margins & Returns, Dividends, Balance Sheet, Ownership, Analyst Coverage, Risk Metrics, Other.
- get_yahoo_finance_news: Get news for a given ticker symbol from yahoo finance.
- get_stock_actions: Get stock dividends and stock splits for a given ticker symbol from yahoo finance.
- get_financial_statement: Get financial statement for a given ticker symbol from yahoo finance. You can choose from the following financial statement types: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cashflow, quarterly_cashflow.
- get_holder_info: Get holder information for a given ticker symbol from yahoo finance. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.
- get_option_expiration_dates: Fetch the available options expiration dates for a given ticker symbol.
- get_option_chain: Fetch the option chain for a given ticker symbol, expiration date, and option type.
- get_recommendations: Get recommendations or upgrades/downgrades for a given ticker symbol from yahoo finance. You can also specify the number of months back to get upgrades/downgrades for, default is 12.
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
""",
)
async def get_historical_stock_prices(
    ticker: str, period: str = "1mo", interval: str = "1d"
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
    """
    cache_key = f"hist:{ticker}:{period}:{interval}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
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
    result = hist_data.to_json(orient="records", date_format="iso")
    _cache_set(cache_key, result)
    return result


@yfinance_server.tool(
    name="get_stock_info",
    description="""Get stock information for a given ticker symbol from yahoo finance. Include the following information:
Stock Price & Trading Info, Company Information, Financial Metrics, Earnings & Revenue, Margins & Returns, Dividends, Balance Sheet, Ownership, Analyst Coverage, Risk Metrics, Other.

Args:
    ticker: str
        The ticker symbol of the stock to get information for, e.g. "AAPL"
""",
)
async def get_stock_info(ticker: str) -> str:
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
    description="""Get financial statement for a given ticker symbol from yahoo finance. You can choose from the following financial statement types: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cashflow, quarterly_cashflow.

Args:
    ticker: str
        The ticker symbol of the stock to get financial statement for, e.g. "AAPL"
    financial_type: str
        The type of financial statement to get. You can choose from the following financial statement types: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cashflow, quarterly_cashflow.
""",
)
async def get_financial_statement(ticker: str, financial_type: str) -> str:
    """Get financial statement for a given ticker symbol"""

    # Check cache first
    cache_key = f"stmt:{ticker}:{financial_type}"
    cached = _cache_get(cache_key, _STMT_TTL)
    if cached is not None:
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
        FinancialType.balance_sheet:           ("yearly",    "balance"),
        FinancialType.quarterly_balance_sheet: ("quarterly", "balance"),
        FinancialType.cashflow:                ("yearly",    "cashflow"),
        FinancialType.quarterly_cashflow:      ("quarterly", "cashflow"),
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

Args:
    ticker: str
        The ticker symbol of the stock to get option chain for, e.g. "AAPL"
    expiration_date: str
        The expiration date for the options chain (format: 'YYYY-MM-DD')
    option_type: str
        The type of option to fetch ('calls' or 'puts')
""",
)
async def get_option_chain(ticker: str, expiration_date: str, option_type: str) -> str:
    """Fetch the option chain for a given ticker symbol, expiration date, and option type.

    Args:
        ticker: The ticker symbol of the stock
        expiration_date: The expiration date for the options chain (format: 'YYYY-MM-DD')
        option_type: The type of option to fetch ('calls' or 'puts')

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
        return option_chain.calls.to_json(orient="records", date_format="iso")
    elif option_type == "puts":
        return option_chain.puts.to_json(orient="records", date_format="iso")
    else:
        return f"Error: invalid option type {option_type}. Please use one of the following: calls, puts."


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


if __name__ == "__main__":
    # Initialize and run the server
    print("Starting Yahoo Finance MCP server...")
    yfinance_server.run(transport="stdio")
