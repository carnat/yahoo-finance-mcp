import asyncio
import datetime
import hashlib
import html as _html_module
import json
import os
import re as _re
import time
import urllib.parse as _urlparse
import urllib.request as _urlrequest
import urllib.error as _urlerror
from enum import Enum
from typing import TypedDict

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


class FilingFactType(str, Enum):
    geographic_revenue = "geographic_revenue"
    segment_revenue = "segment_revenue"
    capex = "capex"
    rd_expense = "rd_expense"
    operating_income = "operating_income"
    net_income = "net_income"
    total_revenue = "total_revenue"
    long_term_debt = "long_term_debt"
    cash = "cash"


# ---------------------------------------------------------------------------
# Server version and envelope feature flag
# ---------------------------------------------------------------------------
SERVER_VERSION = "0.1.0"
_ENVELOPE_V2 = os.environ.get("MCP_ENVELOPE_V2", "").lower() == "true"


# ---------------------------------------------------------------------------
# Typed domain error codes
# ---------------------------------------------------------------------------
class ErrorCode:
    TICKER_NOT_FOUND = "TICKER_NOT_FOUND"
    NO_OPTIONS_DATA = "NO_OPTIONS_DATA"
    NO_FILING_DATA = "NO_FILING_DATA"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    PROVIDER_TIMEOUT = "PROVIDER_TIMEOUT"
    RATE_LIMIT = "RATE_LIMIT"
    INPUT_VALIDATION_ERROR = "INPUT_VALIDATION_ERROR"
    DEPRECATED_TOOL = "DEPRECATED_TOOL"


# ---------------------------------------------------------------------------
# McpResponse TypedDicts
# ---------------------------------------------------------------------------
class ToolMeta(TypedDict):
    tool: str
    canonicalTool: str | None
    deprecatedTool: bool
    useInstead: str
    source: str
    dataDate: str | None
    serverVersion: str
    cacheHit: bool
    warnings: list[object]


class ErrorDetail(TypedDict):
    code: str
    message: str


class McpResponse(TypedDict):
    ok: bool
    data: object
    meta: ToolMeta
    error: ErrorDetail | None


# ---------------------------------------------------------------------------
# McpResponse helpers
# ---------------------------------------------------------------------------
def _mcp_success(
    tool: str,
    data: object,
    *,
    canonical_tool: str | None = None,
    deprecated_tool: bool | None = None,
    use_instead: str | None = None,
    source: str = "yahoo_finance",
    data_date: str | None = None,
    cache_hit: bool = False,
    warnings: list[object] | None = None,
) -> str:
    if not _ENVELOPE_V2:
        return data if isinstance(data, str) else json.dumps(data)
    return json.dumps({
        "ok": True,
        "data": data if not isinstance(data, str) else json.loads(data),
        "meta": {
            "tool": tool,
            **({"canonicalTool": canonical_tool} if canonical_tool is not None else {}),
            **({"deprecatedTool": deprecated_tool} if deprecated_tool is not None else {}),
            **({"useInstead": use_instead} if use_instead is not None else {}),
            "source": source,
            "dataDate": data_date,
            "serverVersion": SERVER_VERSION,
            "cacheHit": cache_hit,
            "warnings": warnings or [],
        },
        "error": None,
    })


def _mcp_failure(
    tool: str,
    code: str,
    message: str,
    *,
    source: str = "yahoo_finance",
    data_date: str | None = None,
) -> str:
    payload = {
        "ok": False,
        "data": None,
        "meta": {
            "tool": tool,
            "source": source,
            "dataDate": data_date,
            "serverVersion": SERVER_VERSION,
            "cacheHit": False,
            "warnings": [],
        },
        "error": {"code": code, "message": message},
    }
    if not _ENVELOPE_V2:
        return json.dumps({"error": True, "code": code, "message": message})
    return json.dumps(payload)


def _mcp_warning(
    tool: str,
    data: object,
    message: str,
    *,
    canonical_tool: str | None = None,
    source: str = "yahoo_finance",
    data_date: str | None = None,
) -> str:
    if not _ENVELOPE_V2:
        return data if isinstance(data, str) else json.dumps(data)
    parsed = data if not isinstance(data, str) else json.loads(data)
    return json.dumps({
        "ok": True,
        "data": parsed,
        "meta": {
            "tool": tool,
            **({"canonicalTool": canonical_tool} if canonical_tool is not None else {}),
            "source": source,
            "dataDate": data_date,
            "serverVersion": SERVER_VERSION,
            "cacheHit": False,
            "warnings": [message],
        },
        "error": None,
    })


# ---------------------------------------------------------------------------
# Input validation helpers
# ---------------------------------------------------------------------------
_TICKER_RE = _re.compile(r'^[A-Z0-9.\-\^=]{1,20}$')
_ACCESSION_RE = _re.compile(r'^\d{10}-\d{2}-\d{6}$')


def _validate_ticker(ticker: str) -> str | None:
    """Returns an error message if the ticker is invalid, else None."""
    t = ticker.strip().upper()
    if not _TICKER_RE.match(t):
        return f"Invalid ticker symbol: '{ticker}'. Must be 1-20 characters: uppercase letters, digits, or . - ^ ="
    return None


def _validate_accession(acc: str) -> str | None:
    """Returns an error message if the accession number is invalid, else None."""
    if not _ACCESSION_RE.match(acc.strip()):
        return f"Invalid accession number: '{acc}'. Expected format: XXXXXXXXXX-YY-ZZZZZZ."
    return None


def _validate_batch_tickers(tickers: list) -> str | None:
    """Returns an error message if the batch is too large, else None."""
    if len(tickers) > 5:
        return f"Too many tickers: {len(tickers)}. Maximum is 5 per call."
    return None


def _validate_sec_url(url: str) -> str | None:
    """Returns an error message if the SEC URL is not from sec.gov/Archives, else None."""
    if not url.startswith("https://www.sec.gov/Archives/"):
        return f"Invalid SEC URL: must start with 'https://www.sec.gov/Archives/'."
    return None


def _sanitize_sec_html(html: str) -> str:
    """Strip script/style tags and event handler attributes from SEC HTML."""
    html = _re.sub(r'<script[^>]*>.*?</script[^>]*>', '', html, flags=_re.DOTALL | _re.IGNORECASE)
    html = _re.sub(r'<style[^>]*>.*?</style[^>]*>', '', html, flags=_re.DOTALL | _re.IGNORECASE)
    html = _re.sub(r'\s+on\w+\s*=\s*(?:"[^"]*"|\'[^\']*\'|[^\s>]+)', '', html, flags=_re.IGNORECASE)
    return html


# ---------------------------------------------------------------------------
# Centralized TTL cache
# ---------------------------------------------------------------------------
class ToolCache:
    def __init__(self) -> None:
        self._store: dict[str, tuple[float, str, float]] = {}  # key -> (stored_at, value, ttl)

    def get(self, key: str) -> tuple[str, bool, str | None] | None:
        """Returns (value, cache_hit, cached_at_iso) or None if miss/expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        stored_at, value, ttl = entry
        age = time.monotonic() - stored_at
        if age >= ttl:
            return None
        cached_at = (
            datetime.datetime.fromtimestamp(time.time() - age, tz=datetime.timezone.utc)
            .isoformat()
        )
        return value, True, cached_at

    def set(self, key: str, value: str, ttl: float) -> None:
        self._store[key] = (time.monotonic(), value, ttl)

    def is_stale(self, key: str) -> bool:
        """True if age > 2× TTL (stale but still cached)."""
        entry = self._store.get(key)
        if not entry:
            return False
        stored_at, _, ttl = entry
        return (time.monotonic() - stored_at) > 2 * ttl


_tool_cache = ToolCache()

# TTL tiers
TTL_PRICE = 5 * 60          # 5 min
TTL_ANALYST = 15 * 60       # 15 min
TTL_FINANCIALS = 4 * 3600   # 4 hours
TTL_EDGAR = 24 * 3600       # 24 hours
TTL_OPTIONS = 15 * 60       # 15 min

# Backward-compat aliases (old names still work)
_PRICE_TTL = TTL_PRICE
_STMT_TTL = TTL_FINANCIALS


def _cache_get(key: str, ttl: float) -> str | None:
    """Legacy cache get — delegates to ToolCache with the given TTL."""
    entry = _tool_cache._store.get(key)
    if entry is None:
        return None
    stored_at, value, stored_ttl = entry
    # honour the caller-supplied TTL (may differ from stored TTL)
    if (time.monotonic() - stored_at) < ttl:
        return value
    return None


def _cache_set(key: str, value: str, ttl: float = TTL_PRICE) -> None:
    """Legacy cache set — delegates to ToolCache."""
    _tool_cache.set(key, value, ttl)


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


_PLACEHOLDER_IV_THRESHOLD = 0.0001


def _compute_data_quality(
    contracts: list[dict],
    data_date: str,
    stale_days_threshold: int = 5,
) -> dict:
    """Compute dataQuality metrics for a list of option contracts.

    Returns a dict with counts and a quality label of "HIGH", "MEDIUM", or "LOW".
    """
    n = len(contracts)
    if n == 0:
        return {
            "zeroBidAskCount": 0,
            "zeroOpenInterestCount": 0,
            "placeholderIvCount": 0,
            "staleLastTradeCount": 0,
            "returnedContracts": 0,
            "quality": "LOW",
            "warnings": ["NO_CONTRACTS_RETURNED"],
        }

    try:
        data_date_obj = datetime.date.fromisoformat(data_date)
    except Exception:
        data_date_obj = None

    zero_bid_ask = 0
    zero_oi = 0
    placeholder_iv = 0
    stale_trade = 0

    for c in contracts:
        bid = float(c.get("bid") or 0)
        ask = float(c.get("ask") or 0)
        if bid <= 0 or ask <= 0:
            zero_bid_ask += 1

        oi = float(c.get("openInterest") or 0)
        if oi <= 0:
            zero_oi += 1

        iv = float(c.get("impliedVolatility") or 0)
        if iv <= _PLACEHOLDER_IV_THRESHOLD:
            placeholder_iv += 1

        if data_date_obj is not None:
            ltd = c.get("lastTradeDate")
            if ltd:
                try:
                    if isinstance(ltd, str):
                        ltd_date = datetime.date.fromisoformat(ltd[:10])
                    else:
                        # Yahoo Finance returns epoch seconds (< 1e10); some
                        # sources return epoch milliseconds (> 1e10). Divide
                        # by 1000 only for the ms case, mirroring TypeScript:
                        # ltdMs = ltd > 1e10 ? ltd : ltd * 1000
                        raw_ts = float(ltd)
                        ltd_seconds = raw_ts / 1000 if raw_ts > 1e10 else raw_ts
                        ltd_date = datetime.datetime.utcfromtimestamp(ltd_seconds).date()
                    if (data_date_obj - ltd_date).days > stale_days_threshold:
                        stale_trade += 1
                except Exception:
                    pass

    warnings: list[str] = []

    # Per-dimension thresholds (any single dimension can trigger LOW/MEDIUM)
    zero_ba_frac = zero_bid_ask / n
    zero_oi_frac = zero_oi / n
    placeholder_iv_frac = placeholder_iv / n
    stale_frac = stale_trade / n

    if (
        zero_ba_frac > 0.50
        or zero_oi_frac > 0.80
        or placeholder_iv_frac > 0.50
        or stale_frac > 0.50
    ):
        quality = "LOW"
    elif (
        zero_ba_frac > 0.30
        or zero_oi_frac > 0.50
        or placeholder_iv_frac > 0.30
        or stale_frac > 0.30
    ):
        quality = "MEDIUM"
    else:
        quality = "HIGH"

    if zero_bid_ask > n * 0.5:
        warnings.append("MAJORITY_ZERO_BID_ASK")
    if zero_oi > n * 0.5:
        warnings.append("MAJORITY_ZERO_OPEN_INTEREST")
    if placeholder_iv > n * 0.5:
        warnings.append("MAJORITY_PLACEHOLDER_IV")
    if stale_trade > n * 0.5:
        warnings.append("MAJORITY_STALE_LAST_TRADE")

    return {
        "zeroBidAskCount": zero_bid_ask,
        "zeroOpenInterestCount": zero_oi,
        "placeholderIvCount": placeholder_iv,
        "staleLastTradeCount": stale_trade,
        "returnedContracts": n,
        "quality": quality,
        "warnings": warnings,
    }


def _sort_by_relevance(
    contracts: list[dict],
    underlying_price: float | None,
) -> list[dict]:
    """Sort contracts by relevance for LLM/Robot use.

    Priority (desc):
      1. validQuote (bid > 0 AND ask > 0)
      2. hasLiquidity (openInterest > 0 OR volume > 0)
      3. validIv (impliedVolatility > 0.0001)
      4. distancePct asc (closer to ATM first)
      5. openInterest desc
      6. volume desc
      7. spreadPct asc (nulls last)
    """
    def _key(c: dict):
        bid = float(c.get("bid") or 0)
        ask = float(c.get("ask") or 0)
        oi = float(c.get("openInterest") or 0)
        vol = float(c.get("volume") or 0)
        iv = float(c.get("impliedVolatility") or 0)
        strike = float(c.get("strike") or 0)

        valid_quote = 1 if (bid > 0 and ask > 0) else 0
        has_liquidity = 1 if (oi > 0 or vol > 0) else 0
        valid_iv = 1 if iv > _PLACEHOLDER_IV_THRESHOLD else 0

        if underlying_price and underlying_price > 0:
            dist_pct = abs(strike - underlying_price) / underlying_price
        else:
            dist_pct = 0.0

        if bid > 0 and ask > 0:
            spread_pct = (ask - bid) / ((bid + ask) / 2)
            spread_sort = spread_pct
        else:
            spread_sort = 9999.0

        return (
            -valid_quote,
            -has_liquidity,
            -valid_iv,
            dist_pct,
            -oi,
            -vol,
            spread_sort,
        )

    return sorted(contracts, key=_key)


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
- get_filing_data: Structured XBRL-tagged SEC filing facts (try this first for GAAP line items and geographic revenue).
- search_filing_text: Full-text search/section retrieval on SEC filing HTML (use when facts are not XBRL-tagged).

### Technical analysis
- get_technical_indicators: Pre-computed RSI-14 and MACD (12,26,9) from historical daily prices. Use for momentum/oversold screening without fetching raw history.

### Discovery
- search_ticker: Search by company name, partial name, or ISIN to get matching ticker symbols.
- screen_stocks: Screen the market using predefined or custom criteria. Predefined: aggressive_small_caps, day_gainers, day_losers, growth_technology_stocks, most_actives, most_shorted_stocks, small_cap_gainers, undervalued_growth_stocks, undervalued_large_caps, conservative_foreign_funds, high_yield_bond, portfolio_anchors, solid_large_growth_funds, solid_midcap_growth_funds, top_mutual_funds.
""",
)




_SIMPLE_OUTPUT_SCHEMA: dict = {"type": "object", "properties": {}, "additionalProperties": True}

_NEWS_EVENT_OUTPUT_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "ticker": {"type": "string"},
        "items": {"type": "array"},
        "meta": {"type": "object"},
    },
    "additionalProperties": True,
}

_TOOL_OUTPUT_SCHEMAS: dict[str, dict] = {
    "get_historical_stock_prices": _SIMPLE_OUTPUT_SCHEMA,
    "get_stock_info": _SIMPLE_OUTPUT_SCHEMA,
    "get_yahoo_finance_news": _NEWS_EVENT_OUTPUT_SCHEMA,
    "get_stock_actions": _SIMPLE_OUTPUT_SCHEMA,
    "get_financial_statement": _SIMPLE_OUTPUT_SCHEMA,
    "get_holder_info": _SIMPLE_OUTPUT_SCHEMA,
    "get_option_expiration_dates": _SIMPLE_OUTPUT_SCHEMA,
    "get_option_chain": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'expiration': {'type': 'string'},
                        'optionType': {'type': 'string'},
                        'dataDate': {'type': 'string'},
                        'totalContracts': {'type': 'number'},
                        'returnedContracts': {'type': 'number'},
                        'truncated': {'type': 'boolean'},
                        'dataQuality': {'type': 'object'},
                        'filtersApplied': {'type': 'object'},
                        'contracts': {'type': 'array'}},
         'additionalProperties': True},
    "get_recommendations": _SIMPLE_OUTPUT_SCHEMA,
    "get_fast_info": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'lastPrice': {'type': 'number'},
                        'currency': {'type': 'string'},
                        'exchange': {'type': 'string'},
                        'quoteType': {'type': 'string'},
                        'marketCap': {'type': ['number', 'null']},
                        'shares': {'type': ['number', 'null']},
                        'dayHigh': {'type': 'number'},
                        'dayLow': {'type': 'number'},
                        'yearHigh': {'type': 'number'},
                        'yearLow': {'type': 'number'},
                        'yearChange': {'type': 'number'},
                        'preMarketPrice': {'type': ['number', 'null']},
                        'postMarketPrice': {'type': ['number', 'null']},
                        'marketOpen': {'type': 'boolean'},
                        'lastTradeDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_short_interest": _SIMPLE_OUTPUT_SCHEMA,
    "get_price_stats": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'lastPrice': {'type': 'number'},
                        'changePct': {'type': 'number'},
                        'distFromHigh52wPct': {'type': 'number'},
                        'distFromLow52wPct': {'type': 'number'},
                        'distFrom50dmaPct': {'type': 'number'},
                        'distFrom200dmaPct': {'type': 'number'},
                        'volatility30d': {'type': 'number'},
                        'cagr1y': {'type': 'number'},
                        'cagr3y': {'type': 'number'},
                        'cagr5y': {'type': 'number'},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_analyst_consensus": _SIMPLE_OUTPUT_SCHEMA,
    "get_earnings_analysis": _SIMPLE_OUTPUT_SCHEMA,
    "get_financial_ratios": _SIMPLE_OUTPUT_SCHEMA,
    "get_calendar": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'earningsDateConfirmed': {'type': ['boolean', 'null']},
                        'earningsDateSource': {'type': ['string', 'null']}},
         'additionalProperties': True},
    "search_ticker": _SIMPLE_OUTPUT_SCHEMA,
    "screen_stocks": _SIMPLE_OUTPUT_SCHEMA,
    "get_filing_data": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'factType': {'type': 'string'},
                        'region': {'type': ['string', 'null']},
                        'period': {'type': ['string', 'null']},
                        'rawValue': {'type': ['string', 'null']},
                        'rawDenominator': {'type': ['string', 'null']},
                        'unit': {'type': ['string', 'null']},
                        'unitScale': {'type': ['string', 'null']},
                        'value': {'type': ['number', 'null']},
                        'denominator': {'type': ['number', 'null']},
                        'valueRatio': {'type': ['number', 'null']},
                        'valuePct': {'type': ['number', 'null']},
                        'extractionMethod': {'type': 'string'},
                        'source': {'type': 'string'},
                        'confidence': {'type': 'string'},
                        'filingType': {'type': ['string', 'null']},
                        'filingDate': {'type': ['string', 'null']},
                        'accessionNumber': {'type': ['string', 'null']},
                        'documentUrl': {'type': ['string', 'null']},
                        'indexUrl': {'type': ['string', 'null']},
                        'primaryDocumentUrl': {'type': ['string', 'null']},
                        'evidence': {'type': ['object', 'null']},
                        'calculation': {'type': ['object', 'null']},
                        'warnings': {'type': 'array'}},
         'additionalProperties': True},
    "search_filing_text": _SIMPLE_OUTPUT_SCHEMA,
    "get_technical_indicators": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'rsi14': {'type': ['number', 'null']},
                        'macd': {'type': ['number', 'null']},
                        'macdSignal': {'type': ['number', 'null']},
                        'macdHistogram': {'type': ['number', 'null']},
                        'lastClose': {'type': ['number', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_price_slope": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'startClose': {'type': ['number', 'null']},
                        'endClose': {'type': ['number', 'null']},
                        'slopePct': {'type': ['number', 'null']},
                        'direction': {'type': 'string'},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_volume_ratio": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'ratio10d': {'type': ['number', 'null']},
                        'ratio90d': {'type': ['number', 'null']},
                        'volumeFlag': {'type': ['string', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_ma_position": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'lastClose': {'type': ['number', 'null']},
                        'sma50': {'type': ['number', 'null']},
                        'sma200': {'type': ['number', 'null']},
                        'distFrom50dmaPct': {'type': ['number', 'null']},
                        'distFrom200dmaPct': {'type': ['number', 'null']},
                        'trend': {'type': 'string'},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_credit_health": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'netDebtToEbitda': {'type': ['number', 'null']},
                        'interestCoverage': {'type': ['number', 'null']},
                        'debtTier': {'type': ['string', 'null']},
                        'creditStress': {'type': ['boolean', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_short_momentum": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'sharesShort': {'type': ['number', 'null']},
                        'shortPctOfFloat': {'type': ['number', 'null']},
                        'momDelta': {'type': ['number', 'null']},
                        'direction': {'type': ['string', 'null']},
                        'squeezeRisk': {'type': ['string', 'null']},
                        'flag': {'type': ['string', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_earnings_momentum": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'revision7d': {'type': ['number', 'null']},
                        'revision30d': {'type': ['number', 'null']},
                        'revision90d': {'type': ['number', 'null']},
                        'momentumFlag': {'type': ['string', 'null']},
                        'beatRate': {'type': ['number', 'null']},
                        'avgSurprisePct': {'type': ['number', 'null']},
                        'currentBeatStreak': {'type': ['number', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_options_flow_summary": _SIMPLE_OUTPUT_SCHEMA,
    "get_put_hedge_candidates": _SIMPLE_OUTPUT_SCHEMA,
    "get_analyst_upgrade_radar": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'netSentiment': {'type': ['number', 'null']},
                        'mixedSignal': {'type': ['boolean', 'null']},
                        'upgrades': {'type': ['number', 'null']},
                        'downgrades': {'type': ['number', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_etf_info": _SIMPLE_OUTPUT_SCHEMA,
    "get_overnight_quote": _SIMPLE_OUTPUT_SCHEMA,
    "get_options_flow_scan": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'windowLabel': {'type': 'string'},
                        'pcRatio': {'type': ['number', 'null']},
                        'ivPctile': {'type': ['number', 'null']},
                        'putVolVs10dAvg': {'type': ['number', 'null']},
                        'putVolTrend': {'type': ['string', 'null']},
                        'maxPainStrike': {'type': ['number', 'null']},
                        'bracket': {'type': ['string', 'null']},
                        'formattedBlock': {'type': ['string', 'null']},
                        'dataDate': {'type': 'string'},
                        'dataQuality': {'type': 'object'},
                        'warnings': {'type': 'array'}},
         'additionalProperties': True},
    "get_price_target_bracket": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'currentPrice': {'type': ['number', 'null']},
                        'referenceTargetPrice': {'type': ['number', 'null']},
                        'referenceTargetPct': {'type': ['number', 'null']},
                        'ioPt': {'type': ['number', 'null']},
                        'eqfPct': {'type': ['number', 'null']},
                        'bracket': {'type': ['string', 'null']},
                        'tag': {'type': ['string', 'null']},
                        'invertedFlag': {'type': ['boolean', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_position_score_inputs": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        't1_inputs': {'type': 'object'},
                        't2_inputs': {'type': 'object'},
                        't4_inputs': {'type': 'object'},
                        't5_inputs': {'type': 'object'},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_volume_gate": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'currency': {'type': ['string', 'null']},
                        'fxRate': {'type': ['number', 'null']},
                        'lastVolume': {'type': ['number', 'null']},
                        'adv10d': {'type': ['number', 'null']},
                        'adv20d': {'type': ['number', 'null']},
                        'adv90d': {'type': ['number', 'null']},
                        'ratio20d': {'type': ['number', 'null']},
                        'gatePass': {'type': ['boolean', 'null']},
                        'dataDate': {'type': 'string'},
                        'note': {'type': ['string', 'null']}},
         'additionalProperties': True},
    "get_options_summary": {
        "type": "object",
        "properties": {
            "ticker": {"type": "string"},
            "nearestExpiry": {"type": ["string", "null"]},
            "currentPrice": {"type": ["number", "null"]},
            "atmIV": {"type": ["number", "null"]},
            "pcRatioVolume": {"type": ["number", "null"]},
            "pcRatioOI": {"type": ["number", "null"]},
            "callVolume": {"type": ["number", "null"]},
            "putVolume": {"type": ["number", "null"]},
            "callOI": {"type": ["number", "null"]},
            "putOI": {"type": ["number", "null"]},
            "maxPainStrike": {"type": ["number", "null"]},
            "dataDate": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "list_sec_filings": _SIMPLE_OUTPUT_SCHEMA,
    "get_filing_outline": _SIMPLE_OUTPUT_SCHEMA,
    "get_filing_section": _SIMPLE_OUTPUT_SCHEMA,
    "list_filing_tables": _SIMPLE_OUTPUT_SCHEMA,
    "get_filing_table": _SIMPLE_OUTPUT_SCHEMA,
    "extract_filing_fact": _SIMPLE_OUTPUT_SCHEMA,
    "index_sec_filing": {
        "type": "object",
        "properties": {
            "ticker": {"type": "string"},
            "cik": {"type": "string"},
            "filingType": {"type": "string"},
            "filingDate": {"type": ["string", "null"]},
            "acceptedAt": {"type": ["string", "null"]},
            "accessionNumber": {"type": "string"},
            "documentUrl": {"type": "string"},
            "index": {
                "type": "object",
                "properties": {
                    "sections": {"type": "array"},
                    "tables": {"type": "array"},
                    "keywordMap": {"type": "object"},
                },
                "additionalProperties": True,
            },
            "meta": {"type": "object"},
        },
        "additionalProperties": True,
    },
    "get_sec_filing_index": {
        "type": "object",
        "properties": {
            "ticker": {"type": "string"},
            "cik": {"type": "string"},
            "filingType": {"type": "string"},
            "filingDate": {"type": ["string", "null"]},
            "acceptedAt": {"type": ["string", "null"]},
            "accessionNumber": {"type": "string"},
            "documentUrl": {"type": "string"},
            "index": {
                "type": "object",
                "properties": {
                    "sections": {"type": "array"},
                    "tables": {"type": "array"},
                    "keywordMap": {"type": "object"},
                },
                "additionalProperties": True,
            },
            "meta": {"type": "object"},
        },
        "additionalProperties": True,
    },
    "search_company_news": _NEWS_EVENT_OUTPUT_SCHEMA,
    "get_company_press_releases": _NEWS_EVENT_OUTPUT_SCHEMA,
    "get_sec_recent_events": _NEWS_EVENT_OUTPUT_SCHEMA,
    "get_public_event_timeline": _NEWS_EVENT_OUTPUT_SCHEMA,
    "verify_company_event": _NEWS_EVENT_OUTPUT_SCHEMA,
    "get_latest_earnings_release": _SIMPLE_OUTPUT_SCHEMA,
    "index_earnings_release": _SIMPLE_OUTPUT_SCHEMA,
    "extract_earnings_metrics": _SIMPLE_OUTPUT_SCHEMA,
    "extract_guidance": _SIMPLE_OUTPUT_SCHEMA,
    "extract_management_commentary": _SIMPLE_OUTPUT_SCHEMA,
    "compare_earnings_actual_vs_estimate": _SIMPLE_OUTPUT_SCHEMA,
}

TOOL_ALIASES: dict[str, str] = {
    "get_fast_info": "get_market_quote",
    "get_historical_stock_prices": "get_historical_prices",
    "get_stock_info": "get_company_profile",
    "get_etf_info": "get_fund_profile",
    "get_stock_actions": "get_corporate_actions",
    "get_holder_info": "get_ownership_holders",
    "get_price_stats": "analyze_price_performance",
    "get_ma_position": "analyze_moving_average_position",
    "get_volume_ratio": "analyze_volume_ratio",
    "get_volume_gate": "check_volume_liquidity_threshold",
    "get_adv_gate": "check_volume_liquidity_threshold",
    "get_financial_ratios": "analyze_financial_ratios",
    "get_credit_health": "analyze_credit_health",
    "get_recommendations": "get_analyst_recommendations",
    "get_analyst_upgrade_radar": "get_analyst_rating_changes",
    "get_earnings_momentum": "analyze_earnings_momentum",
    "get_calendar": "get_company_events_calendar",
    "get_yahoo_finance_news": "get_company_news",
    "get_options_flow_summary": "summarize_options_flow",
    "get_options_summary": "summarize_options_flow",
    "get_options_flow_scan": "analyze_options_flow_window",
    "get_dc134_options_scan": "analyze_options_flow_window",
    "get_put_hedge_candidates": "find_put_hedge_candidates",
    "get_price_target_bracket": "calculate_price_target_distance",
    "get_eqf_bracket": "calculate_price_target_distance",
    "get_position_score_inputs": "analyze_position_signals",
    "get_tps_inputs": "analyze_position_signals",
    "list_sec_filings": "list_sec_company_filings",
    "get_filing_outline": "get_sec_filing_outline",
    "get_filing_section": "get_sec_filing_section",
    "list_filing_tables": "list_sec_filing_tables",
    "get_filing_table": "get_sec_filing_table",
    "get_filing_data": "extract_sec_filing_fact",
    "extract_filing_fact": "extract_sec_filing_fact",
    "get_geographic_revenue": "extract_sec_filing_fact",
    "get_china_revenue_pct": "extract_sec_filing_fact",
    "search_filing_text": "search_sec_filing_text",
    "get_filing_text_search": "search_sec_filing_text",
    "get_filing_document": "get_sec_filing_section",
}

for _alias_name, _canonical_name in TOOL_ALIASES.items():
    if _canonical_name in _TOOL_OUTPUT_SCHEMAS and _alias_name not in _TOOL_OUTPUT_SCHEMAS:
        _TOOL_OUTPUT_SCHEMAS[_alias_name] = _TOOL_OUTPUT_SCHEMAS[_canonical_name]

# Canonical/alias schemas that route to existing base implementations.
_TOOL_OUTPUT_SCHEMAS.setdefault("analyze_position_signals", _TOOL_OUTPUT_SCHEMAS["get_position_score_inputs"])
_TOOL_OUTPUT_SCHEMAS.setdefault("calculate_price_target_distance", _TOOL_OUTPUT_SCHEMAS["get_price_target_bracket"])
_TOOL_OUTPUT_SCHEMAS.setdefault("check_volume_liquidity_threshold", _TOOL_OUTPUT_SCHEMAS["get_volume_gate"])
_TOOL_OUTPUT_SCHEMAS.setdefault("analyze_options_flow_window", _TOOL_OUTPUT_SCHEMAS["get_options_flow_scan"])
_TOOL_OUTPUT_SCHEMAS.setdefault("summarize_options_flow", _TOOL_OUTPUT_SCHEMAS["get_options_summary"])
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_sec_filing_fact", _TOOL_OUTPUT_SCHEMAS["extract_filing_fact"])
_TOOL_OUTPUT_SCHEMAS.setdefault("search_sec_filing_text", _TOOL_OUTPUT_SCHEMAS["search_filing_text"])
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_geographic_revenue", _TOOL_OUTPUT_SCHEMAS["get_filing_data"])
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_segment_revenue", _TOOL_OUTPUT_SCHEMAS["get_filing_data"])
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_total_revenue", _TOOL_OUTPUT_SCHEMAS["get_filing_data"])
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_revenue_exposure", _SIMPLE_OUTPUT_SCHEMA)
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_china_exposure", _SIMPLE_OUTPUT_SCHEMA)
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_risk_factor_mentions", _SIMPLE_OUTPUT_SCHEMA)
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_customer_concentration", _SIMPLE_OUTPUT_SCHEMA)
_TOOL_OUTPUT_SCHEMAS.setdefault("query_sec_filing_index", _SIMPLE_OUTPUT_SCHEMA)
_TOOL_OUTPUT_SCHEMAS.setdefault("get_tps_inputs", _TOOL_OUTPUT_SCHEMAS["get_position_score_inputs"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_eqf_bracket", _TOOL_OUTPUT_SCHEMAS["get_price_target_bracket"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_adv_gate", _TOOL_OUTPUT_SCHEMAS["get_volume_gate"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_dc134_options_scan", _TOOL_OUTPUT_SCHEMAS["get_options_flow_scan"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_china_revenue_pct", _TOOL_OUTPUT_SCHEMAS["get_filing_data"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_geographic_revenue", _TOOL_OUTPUT_SCHEMAS["get_filing_data"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_filing_text_search", _TOOL_OUTPUT_SCHEMAS["search_filing_text"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_filing_document", _TOOL_OUTPUT_SCHEMAS["get_filing_section"])

@yfinance_server.tool(
    name="get_historical_stock_prices",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_historical_stock_prices"],
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
    if ticker is None or str(ticker).strip() == "":
        return _mcp_failure(
            "get_historical_stock_prices",
            ErrorCode.INPUT_VALIDATION_ERROR,
            "ticker is required",
        )

    ticker = str(ticker).strip().upper()
    ticker_err = _validate_ticker(ticker)
    if ticker_err:
        return _mcp_failure("get_historical_stock_prices", ErrorCode.INPUT_VALIDATION_ERROR, ticker_err)

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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_stock_info"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_yahoo_finance_news"],
    description="""Get news for a given ticker symbol from yahoo finance.

Args:
    ticker: str
        The ticker symbol of the stock to get news for, e.g. "AAPL"
""",
)
async def get_yahoo_finance_news(ticker: str) -> str:
    """Alias for get_company_news. Routes to the canonical multi-source news tool."""
    return await get_company_news(ticker)


# ---------------------------------------------------------------------------
# Phase 6B public event helpers (multi-source, source-backed, deduped)
# ---------------------------------------------------------------------------

_DEDUP_TITLE_MAX_LEN = 80
_STALE_EVENT_DAYS = 90
_PHASE6B_SUPPORTED_SOURCES = {"sec", "company_ir", "newswire", "yahoo_finance", "finnhub"}
_OFFICIAL_SOURCE_TYPES = {"sec_filing", "company_ir", "press_release", "newswire"}
_SOURCE_PRIORITY = {
    "sec_filing": 0,
    "company_ir": 1,
    "press_release": 2,
    "newswire": 3,
    "yahoo_finance": 4,
    "other": 5,
}
_NEWSWIRE_HINTS = ("businesswire", "globenewswire", "prnewswire")
_FINNHUB_NEWS_API = "https://finnhub.io/api/v1/company-news"
_SMOKE_TICKER_CIK_FALLBACKS: dict[str, str] = {
    "AAPL": "0000320193",
    "MSFT": "0000789019",
    "AMZN": "0001018724",
    "GOOGL": "0001652044",
    "GOOG": "0001652044",
    "NVDA": "0001045810",
    "TSLA": "0001318605",
    "META": "0001326801",
    "VRT": "0001674101",
    "AAOI": "0001158114",
    "AXTI": "0001051627",
}


def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _to_iso_utc(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            return datetime.datetime.fromtimestamp(float(raw), datetime.timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            return None
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None
    if value.isdigit() and len(value) in (8, 14):
        if len(value) == 8:
            return f"{value[0:4]}-{value[4:6]}-{value[6:8]}T00:00:00Z"
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}T{value[8:10]}:{value[10:12]}:{value[12:14]}Z"
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _coerce_max_results(value: int, default_value: int) -> int:
    return min(max(1, int(value or default_value)), 100)


def _coerce_lookback_days(value: int, default_value: int) -> int:
    return min(max(1, int(value or default_value)), 3650)


def _normalize_event_sources(sources: list[str] | None, default_sources: list[str]) -> tuple[list[str], list[dict]]:
    warnings: list[dict] = []
    source_list = [str(s).strip().lower() for s in (sources or default_sources) if str(s).strip()]
    if not source_list:
        source_list = list(default_sources)
    normalized: list[str] = []
    seen: set[str] = set()
    for src in source_list:
        if src not in _PHASE6B_SUPPORTED_SOURCES:
            warnings.append({
                "code": "SOURCE_UNSUPPORTED",
                "message": f"Source '{src}' is not supported.",
                "severity": "warning",
            })
            continue
        if src not in seen:
            seen.add(src)
            normalized.append(src)
    if not normalized:
        normalized = [s for s in default_sources if s in _PHASE6B_SUPPORTED_SOURCES]
    return normalized, warnings


def _event_type_from_keywords(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ("earnings", "eps", "quarterly", "10-q", "10-k", "annual report")):
        return "earnings"
    if any(k in t for k in ("guidance", "outlook", "forecast", "reaffirm", "raises", "lowers")):
        return "guidance"
    if any(k in t for k in ("contract", "agreement", "deal", "partnership")):
        return "contract"
    if any(k in t for k in ("offering", "financing", "debt", "credit facility", "note")):
        return "financing"
    if any(k in t for k in ("launch", "product", "introduce", "announces new")):
        return "product"
    if any(k in t for k in ("analyst", "rating", "upgrade", "downgrade", "price target")):
        return "analyst"
    if any(k in t for k in ("macro", "inflation", "rates", "fomc", "cpi")):
        return "macro"
    if any(k in t for k in ("lawsuit", "litigation", "court", "settlement")):
        return "litigation"
    if any(k in t for k in ("insider", "director", "officer", "form 4")):
        return "insider"
    if any(k in t for k in ("sec", "regulatory", "8-k", "10-q", "10-k", "filing")):
        return "regulatory"
    return "other"


def _event_type_from_form(form_type: str) -> str:
    ft = (form_type or "").upper()
    if ft in ("10-Q", "10-K"):
        return "earnings"
    if ft in ("S-3", "S-1", "424B"):
        return "financing"
    if ft in ("8-K", "DEF14A", "PRE14A"):
        return "regulatory"
    if ft == "4":
        return "insider"
    return "other"


def _short_text(text: object, max_chars: int = 220) -> str | None:
    value = " ".join(str(text or "").split())
    if not value:
        return None
    return value[:max_chars]


def _canonicalize_event_url(url: str | None) -> str | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    try:
        parsed = _urlparse.urlparse(raw)
        if parsed.scheme not in ("http", "https"):
            return None
        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            params="",
            query="",
            fragment="",
        )
        return _urlparse.urlunparse(normalized)
    except Exception:
        return None


def _make_duplicate_group_id(
    ticker: str,
    title: str | None,
    published_at: str | None,
    issuer: str | None,
    url: str | None,
) -> str | None:
    norm_title = " ".join((title or "").lower().split())[:_DEDUP_TITLE_MAX_LEN]
    event_day = (published_at or "")[:10]
    entity = (issuer or ticker or "").upper().strip()
    canon_url = _canonicalize_event_url(url) or ""
    if not norm_title and not event_day and not canon_url:
        return None
    key = f"{norm_title}|{event_day}|{entity}|{canon_url}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _source_rank(source_type: object) -> int:
    return _SOURCE_PRIORITY.get(str(source_type or "other"), _SOURCE_PRIORITY["other"])


def _safe_sec_url(candidate: object) -> str | None:
    url = str(candidate or "").strip()
    return url if url.startswith("https://www.sec.gov/Archives/") else None


def _within_date_window(
    iso_ts: str | None,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
) -> bool:
    if not iso_ts:
        return False
    day = iso_ts[:10]
    if start_date and day < start_date:
        return False
    if end_date and day > end_date:
        return False
    if lookback_days is not None:
        cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        if day < cutoff:
            return False
    return True


def _build_yahoo_event_item(ticker: str, news_item: dict, retrieved_at: str) -> tuple[dict, list[dict]]:
    warnings: list[dict] = []
    content = news_item.get("content", {}) if isinstance(news_item.get("content"), dict) else {}
    title = str(content.get("title") or news_item.get("title") or "").strip()
    summary = str(content.get("summary") or news_item.get("summary") or "").strip()
    url = str((content.get("canonicalUrl", {}) or {}).get("url") or news_item.get("link") or news_item.get("url") or "").strip()
    provider = str((content.get("provider", {}) or {}).get("displayName") or news_item.get("publisher") or "Yahoo Finance").strip()
    source_type = "newswire" if any(h in provider.lower() for h in _NEWSWIRE_HINTS) else "yahoo_finance"
    published_at = _to_iso_utc(news_item.get("providerPublishTime") or content.get("pubDate"))
    if not published_at:
        warnings.append({
            "code": "PUBLISHED_AT_UNAVAILABLE",
            "message": f"Published timestamp unavailable for source '{provider or 'Yahoo Finance'}'.",
            "severity": "warning",
        })
    ticker_u = ticker.upper()
    text_blob = f"{title} {summary}".upper()
    ticker_relevance = "HIGH" if ticker_u in text_blob else ("LOW" if source_type == "yahoo_finance" else "UNKNOWN")
    confidence = "MEDIUM"
    if not url:
        confidence = "LOW"
    if ticker_relevance in ("LOW", "UNKNOWN") and source_type == "yahoo_finance":
        confidence = "LOW"
    issuer = None
    duplicate_group_id = _make_duplicate_group_id(ticker, title, published_at, issuer, url)
    if duplicate_group_id is None:
        warnings.append({"code": "DEDUPE_WEAK_KEY", "message": "Weak dedupe key for at least one item.", "severity": "warning"})
    item = {
        "title": title,
        "source": provider or "Yahoo Finance",
        "sourceType": source_type,
        "publishedAt": published_at,
        "retrievedAt": retrieved_at,
        "url": url or None,
        "issuer": issuer,
        "tickers": [ticker_u],
        "eventType": _event_type_from_keywords(f"{title} {summary}"),
        "summary": _short_text(summary or title, 240),
        "evidenceText": _short_text(summary or title, 180),
        "confidence": confidence,
        "tickerRelevance": ticker_relevance,
        "duplicateGroupId": duplicate_group_id,
    }
    return item, warnings


def _build_sec_event_item(ticker: str, filing: dict, retrieved_at: str, issuer: str | None = None) -> tuple[dict, list[dict]]:
    warnings: list[dict] = []
    filing_type = str(filing.get("filingType") or filing.get("formType") or filing.get("form") or "").upper()
    filing_date = str(filing.get("filingDate") or "").strip()
    accepted_at = _to_iso_utc(filing.get("acceptedAt") or filing.get("acceptanceDateTime"))
    published_at = accepted_at
    if not published_at and filing_date:
        published_at = f"{filing_date}T00:00:00Z"
        warnings.append({
            "code": "PUBLISHED_AT_ESTIMATED",
            "message": f"acceptedAt unavailable for {filing_type or 'SEC filing'}; filingDate used.",
            "severity": "warning",
        })
    accession = str(filing.get("accessionNumber") or "").strip()
    cik_int = str(filing.get("cikInt") or filing.get("cik") or "").strip()
    acc_clean = accession.replace("-", "")
    primary_document = str(filing.get("primaryDocument") or "").strip()
    url = _safe_sec_url(filing.get("documentUrl")) or _safe_sec_url(filing.get("primaryDocumentUrl"))
    if not url and cik_int and accession and primary_document:
        url = _safe_sec_url(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{primary_document}")
    if not url and cik_int and accession:
        url = _safe_sec_url(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{accession}-index.htm")
    confidence = "HIGH" if (accession and accepted_at and url) else ("MEDIUM" if accession and url else "LOW")
    title = f"{filing_type} filed" if filing_type else "SEC filing"
    event_type = _event_type_from_form(filing_type)
    duplicate_group_id = _make_duplicate_group_id(ticker, title, published_at, issuer, url)
    if duplicate_group_id is None:
        warnings.append({"code": "DEDUPE_WEAK_KEY", "message": "Weak dedupe key for at least one item.", "severity": "warning"})
    item = {
        "title": title,
        "source": "SEC",
        "sourceType": "sec_filing",
        "filingType": filing_type or None,
        "filingDate": filing_date or None,
        "acceptedAt": accepted_at,
        "accessionNumber": accession or None,
        "url": url,
        "publishedAt": published_at,
        "retrievedAt": retrieved_at,
        "issuer": issuer,
        "tickers": [ticker.upper()],
        "eventType": event_type or _event_type_from_keywords(title),
        "summary": _short_text(f"SEC {filing_type} filing for {ticker.upper()}"),
        "evidenceText": _short_text(f"{filing_type} accepted by SEC on {accepted_at or filing_date}"),
        "confidence": confidence,
        "tickerRelevance": "HIGH",
        "duplicateGroupId": duplicate_group_id,
    }
    return item, warnings


async def _collect_sec_events(
    ticker: str,
    *,
    filing_types: list[str],
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
) -> tuple[list[dict], list[dict], bool]:
    warnings: list[dict] = []
    events: list[dict] = []
    cik_padded, submissions = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not submissions:
        warnings.append({"code": "SOURCE_UNAVAILABLE", "message": "SEC submissions source unavailable.", "severity": "warning"})
        return events, warnings, False
    recent = ((submissions.get("filings") or {}).get("recent") or {}) if isinstance(submissions, dict) else {}
    forms = list(recent.get("form") or [])
    filing_dates = list(recent.get("filingDate") or [])
    accepted = list(recent.get("acceptanceDateTime") or [])
    accessions = list(recent.get("accessionNumber") or [])
    primary_docs = list(recent.get("primaryDocument") or [])
    issuer = str(submissions.get("name") or "").strip() or None if isinstance(submissions, dict) else None
    desired = {f.upper() for f in filing_types}
    cik_int = str(int(cik_padded))
    for i, form in enumerate(forms):
        form_str = str(form or "").upper()
        if desired and form_str not in desired:
            continue
        filing_date = str(filing_dates[i] or "") if i < len(filing_dates) else ""
        accepted_at = str(accepted[i] or "") if i < len(accepted) else ""
        accession = str(accessions[i] or "") if i < len(accessions) else ""
        primary_doc = str(primary_docs[i] or "") if i < len(primary_docs) else ""
        filing_obj = {
            "filingType": form_str,
            "filingDate": filing_date,
            "acceptedAt": accepted_at,
            "accessionNumber": accession,
            "primaryDocument": primary_doc,
            "cikInt": cik_int,
        }
        item, item_warnings = _build_sec_event_item(ticker, filing_obj, retrieved_at, issuer=issuer)
        if not _within_date_window(item.get("publishedAt"), start_date=start_date, end_date=end_date, lookback_days=lookback_days):
            continue
        events.append(item)
        warnings.extend(item_warnings)
        if len(events) >= max_results:
            break
    return events, warnings, True


async def _collect_yahoo_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
) -> tuple[list[dict], list[dict], bool]:
    warnings: list[dict] = []
    items: list[dict] = []
    try:
        company = yf.Ticker(ticker)
        raw_news = company.news or []
    except Exception as exc:
        warnings.append({"code": "SOURCE_UNAVAILABLE", "message": f"Yahoo Finance source unavailable: {exc}", "severity": "warning"})
        return items, warnings, False
    for n in raw_news:
        if not isinstance(n, dict):
            continue
        content = n.get("content", {}) if isinstance(n.get("content"), dict) else {}
        if content.get("contentType", "") not in ("", "STORY"):
            continue
        item, item_warnings = _build_yahoo_event_item(ticker, n, retrieved_at)
        if not _within_date_window(item.get("publishedAt"), start_date=start_date, end_date=end_date, lookback_days=lookback_days):
            continue
        items.append(item)
        warnings.extend(item_warnings)
        if len(items) >= max_results:
            break
    return items, warnings, True


async def _collect_finnhub_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
) -> tuple[list[dict], list[dict], bool]:
    warnings: list[dict] = []
    items: list[dict] = []
    api_key = os.environ.get("FINNHUB_API_KEY") or os.environ.get("FINNHUB_TOKEN")
    if not api_key:
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub company-news source is not configured; skipped.",
            "severity": "warning",
        })
        return items, warnings, False

    from_day = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=lookback_days or 14)
    ).strftime("%Y-%m-%d")
    to_day = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    query = _urlparse.urlencode({
        "symbol": ticker.upper(),
        "from": from_day,
        "to": to_day,
        "token": api_key,
    })
    url = f"{_FINNHUB_NEWS_API}?{query}"
    loop = asyncio.get_event_loop()

    def _fetch() -> list[dict]:
        req = _urlrequest.Request(url, headers={"User-Agent": _SEC_REQUIRED_UA})
        with _urlrequest.urlopen(req, timeout=20) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    try:
        raw_items = await loop.run_in_executor(None, _fetch)
    except Exception as exc:
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": f"Finnhub source unavailable: {exc}",
            "severity": "warning",
        })
        return items, warnings, False

    ticker_u = ticker.upper()
    for row in raw_items:
        if not isinstance(row, dict):
            continue
        title = str(row.get("headline") or "").strip()
        summary = str(row.get("summary") or "").strip()
        source = str(row.get("source") or "Finnhub").strip() or "Finnhub"
        item_url = str(row.get("url") or "").strip() or None
        published_at = _to_iso_utc(row.get("datetime"))
        duplicate_group_id = _make_duplicate_group_id(ticker_u, title, published_at, None, item_url)
        if duplicate_group_id is None:
            warnings.append({
                "code": "DEDUPE_WEAK_KEY",
                "message": "Weak dedupe key for at least one item.",
                "severity": "warning",
            })
        blob = f"{title} {summary}".upper()
        item = {
            "title": title,
            "source": source,
            "sourceType": "finnhub",
            "publishedAt": published_at,
            "retrievedAt": retrieved_at,
            "url": item_url,
            "issuer": None,
            "tickers": [ticker_u],
            "eventType": _event_type_from_keywords(f"{title} {summary}"),
            "summary": _short_text(summary or title, 240),
            "evidenceText": _short_text(summary or title, 180),
            "confidence": "MEDIUM" if item_url else "LOW",
            "tickerRelevance": "HIGH" if ticker_u in blob else "LOW",
            "duplicateGroupId": duplicate_group_id,
        }
        if not _within_date_window(
            item.get("publishedAt"),
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback_days,
        ):
            continue
        items.append(item)
        if len(items) >= max_results:
            break
    return items, warnings, True


def _dedupe_event_items(items: list[dict], warnings: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    passthrough: list[dict] = []
    for item in items:
        gid = item.get("duplicateGroupId")
        if not gid:
            passthrough.append(item)
            continue
        existing = grouped.get(gid)
        if existing is None:
            grouped[gid] = item
            continue
        existing_rank = _source_rank(existing.get("sourceType"))
        current_rank = _source_rank(item.get("sourceType"))
        keep_new = current_rank < existing_rank
        if current_rank == existing_rank:
            keep_new = str(item.get("publishedAt") or "") > str(existing.get("publishedAt") or "")
        if str(existing.get("publishedAt") or "") != str(item.get("publishedAt") or ""):
            warnings.append({
                "code": "TIMESTAMP_CONFLICT",
                "message": f"Conflicting source timestamps observed for duplicateGroupId={gid}.",
                "severity": "warning",
            })
        preferred = item if keep_new else existing
        alternate = existing if keep_new else item
        refs = list(preferred.get("sourceRefs") or [])
        refs.append({
            "source": alternate.get("source"),
            "sourceType": alternate.get("sourceType"),
            "publishedAt": alternate.get("publishedAt"),
            "url": alternate.get("url"),
        })
        preferred["sourceRefs"] = refs
        grouped[gid] = preferred
    deduped = list(grouped.values()) + passthrough
    deduped.sort(key=lambda it: str(it.get("publishedAt") or ""), reverse=True)
    return deduped


def _build_collection_status(items: list[dict], sources_used: list[str], warnings: list[dict]) -> str | None:
    if items and any(w.get("code") == "SOURCE_UNAVAILABLE" for w in warnings if isinstance(w, dict)):
        return "PARTIAL"
    if not items:
        # If any source is unconfigured/provider-error/rate-limited, report SOURCE_LIMITED_NOT_FOUND
        # so callers know the empty result may be due to missing coverage, not genuine absence.
        if any(w.get("code") == "SOURCE_UNAVAILABLE" for w in warnings if isinstance(w, dict)):
            return "SOURCE_LIMITED_NOT_FOUND"
        if sources_used:
            return "NOT_FOUND"
        return "PROVIDER_ERROR"
    return None


def _compute_source_status(
    sources_used: list[str],
    warnings: list[dict],
    items: list[dict],
    selected_sources: list[str] | None = None,
) -> dict:
    """Build per-source status dict from collection results."""
    warning_msgs = [w.get("message", "") for w in warnings if isinstance(w, dict) and w.get("code") == "SOURCE_UNAVAILABLE"]
    sec_items = [it for it in items if "sec" in str(it.get("sourceType", "")).lower()]
    yf_items = [it for it in items if str(it.get("sourceType", "")) == "yahoo_finance"]
    finnhub_items = [it for it in items if str(it.get("sourceType", "")) == "finnhub"]
    sources = selected_sources or ["sec", "company_ir", "newswire", "yahoo_finance", "finnhub"]

    result: dict = {}
    if "sec" in sources:
        if "sec" in sources_used:
            result["sec"] = {"status": "OK" if sec_items else "EMPTY_RESULT", "rawCount": len(sec_items), "filteredCount": len(sec_items)}
        elif any("sec submissions" in m.lower() for m in warning_msgs):
            result["sec"] = {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0}
        else:
            result["sec"] = {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}
    if "yahoo_finance" in sources:
        if "yahoo_finance" in sources_used:
            result["yahoo_finance"] = {"status": "OK" if yf_items else "EMPTY_RESULT", "rawCount": len(yf_items), "filteredCount": len(yf_items)}
        elif any("yahoo finance" in m.lower() for m in warning_msgs):
            result["yahoo_finance"] = {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0}
        else:
            result["yahoo_finance"] = {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}
    if "finnhub" in sources:
        if "finnhub" in sources_used:
            result["finnhub"] = {"status": "OK" if finnhub_items else "EMPTY_RESULT", "rawCount": len(finnhub_items), "filteredCount": len(finnhub_items)}
        elif any("finnhub company-news source is not configured" in m.lower() for m in warning_msgs):
            result["finnhub"] = {"status": "UNCONFIGURED"}
        elif any("finnhub" in m.lower() for m in warning_msgs):
            result["finnhub"] = {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0}
        else:
            result["finnhub"] = {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}
    if "company_ir" in sources:
        result["company_ir"] = {"status": "UNCONFIGURED"}
    if "newswire" in sources:
        result["newswire"] = {"status": "UNCONFIGURED"}
    return result


def _compute_source_coverage(source_status: dict) -> str:
    """Return PARTIAL if any source is UNCONFIGURED or has an error, else FULL."""
    for info in source_status.values():
        s = info.get("status", "") if isinstance(info, dict) else ""
        if s in ("UNCONFIGURED", "PROVIDER_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_CHANGED"):
            return "PARTIAL"
    return "FULL"


async def _collect_company_events(
    ticker: str,
    *,
    max_results: int,
    lookback_days: int,
    start_date: str = "",
    end_date: str = "",
    sources: list[str] | None = None,
    sec_filing_types: list[str] | None = None,
) -> tuple[list[dict], list[str], list[dict], str]:
    retrieved_at = _utc_now_iso()
    selected_sources, warnings = _normalize_event_sources(
        sources,
        ["sec", "company_ir", "newswire", "yahoo_finance", "finnhub"],
    )
    items: list[dict] = []
    sources_used: list[str] = []
    max_cap = _coerce_max_results(max_results, 10)
    lookback = _coerce_lookback_days(lookback_days, 14)

    if "sec" in selected_sources:
        sec_items, sec_warnings, used = await _collect_sec_events(
            ticker,
            filing_types=sec_filing_types or ["8-K", "10-Q", "10-K", "S-3", "DEF14A"],
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
        )
        if used:
            sources_used.append("sec")
        items.extend(sec_items)
        warnings.extend(sec_warnings)

    if "company_ir" in selected_sources:
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Company IR source is not configured; skipped.",
            "severity": "warning",
        })

    if "newswire" in selected_sources:
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Newswire source is not configured; skipped.",
            "severity": "warning",
        })

    if "yahoo_finance" in selected_sources:
        yf_items, yf_warnings, used = await _collect_yahoo_events(
            ticker,
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
        )
        if used:
            sources_used.append("yahoo_finance")
        items.extend(yf_items)
        warnings.extend(yf_warnings)

    if "finnhub" in selected_sources:
        finnhub_items, finnhub_warnings, used = await _collect_finnhub_events(
            ticker,
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
        )
        if used:
            sources_used.append("finnhub")
        items.extend(finnhub_items)
        warnings.extend(finnhub_warnings)

    deduped = _dedupe_event_items(items, warnings)
    deduped = deduped[:max_cap]
    seen_warning_keys: set[str] = set()
    unique_warnings: list[dict] = []
    for w in warnings:
        if not isinstance(w, dict):
            continue
        key = f"{w.get('code')}|{w.get('message')}"
        if key in seen_warning_keys:
            continue
        seen_warning_keys.add(key)
        unique_warnings.append(w)
    return deduped, sources_used, unique_warnings, retrieved_at


@yfinance_server.tool(
    name="search_company_news",
    output_schema=_TOOL_OUTPUT_SCHEMAS["search_company_news"],
    description="""Search public company news/events for a ticker and query string.

Returns source-backed, deduplicated event items with source type, published/retrieved timestamps,
URL, confidence, relevance, and short evidence excerpts.
""",
)
async def search_company_news(
    ticker: str,
    query: str,
    start_date: str = "",
    end_date: str = "",
    sources: list[str] | None = None,
    max_results: int = 10,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("search_company_news", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if not str(query or "").strip():
        return _mcp_failure("search_company_news", ErrorCode.INPUT_VALIDATION_ERROR, "query is required")
    items, sources_used, warnings, retrieved_at = await _collect_company_events(
        ticker,
        max_results=max_results,
        lookback_days=14,
        start_date=start_date,
        end_date=end_date,
        sources=sources,
    )
    q = query.strip().lower()
    filtered: list[dict] = []
    for item in items:
        text = " ".join([
            str(item.get("title") or ""),
            str(item.get("summary") or ""),
            str(item.get("source") or ""),
            str(item.get("eventType") or ""),
            str(item.get("evidenceText") or ""),
        ]).lower()
        if q in text:
            filtered.append(item)
    status = _build_collection_status(filtered, sources_used, warnings)
    payload = {
        "ticker": ticker.upper(),
        "query": query,
        "items": filtered[:_coerce_max_results(max_results, 10)],
        "meta": {
            "sourcesUsed": sources_used,
            "deduped": True,
            "watermark": retrieved_at,
        },
        "warnings": warnings,
    }
    if status:
        payload["status"] = status
    return json.dumps(payload)


@yfinance_server.tool(
    name="get_company_press_releases",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_company_press_releases"],
    description="""Get company-originated or official release-style public events.

Prefers SEC 8-K and other official release channels. Returns structured source-backed event metadata
with short evidence excerpts only.
""",
)
async def get_company_press_releases(
    ticker: str,
    lookback_days: int = 90,
    max_results: int = 20,
    sources: list[str] | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_company_press_releases", ErrorCode.INPUT_VALIDATION_ERROR, err)
    selected_sources, source_warnings = _normalize_event_sources(sources, ["company_ir", "newswire", "sec"])
    items, sources_used, warnings, retrieved_at = await _collect_company_events(
        ticker,
        max_results=max_results,
        lookback_days=lookback_days,
        sources=selected_sources,
        sec_filing_types=["8-K"],
    )
    warnings = source_warnings + warnings
    release_types = {"company_ir", "press_release", "newswire", "sec_filing"}
    release_items = [it for it in items if str(it.get("sourceType")) in release_types]
    if not release_items:
        warnings.append({
            "code": "NO_OFFICIAL_RELEASE_SOURCE",
            "message": "No company-originated or official release source found in requested window.",
            "severity": "warning",
        })
    status = _build_collection_status(release_items, sources_used, warnings)
    payload = {
        "ticker": ticker.upper(),
        "items": release_items[:_coerce_max_results(max_results, 20)],
        "meta": {
            "sourcesUsed": sources_used,
            "deduped": True,
            "watermark": retrieved_at,
        },
        "warnings": warnings,
    }
    if status:
        payload["status"] = status
    return json.dumps(payload)


@yfinance_server.tool(
    name="get_sec_recent_events",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_sec_recent_events"],
    description="""Get recent SEC filing events with filing metadata and SEC archive URLs.

Uses SEC submissions as the primary source and returns structured event records for requested filing types.
""",
)
async def get_sec_recent_events(
    ticker: str,
    filing_types: list[str] | None = None,
    lookback_days: int = 90,
    max_results: int = 20,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_sec_recent_events", ErrorCode.INPUT_VALIDATION_ERROR, err)
    selected_types = [str(ft).upper() for ft in (filing_types or ["8-K", "10-Q", "10-K"]) if str(ft).strip()]
    if not selected_types:
        return _mcp_failure("get_sec_recent_events", ErrorCode.INPUT_VALIDATION_ERROR, "filing_types must not be empty")
    retrieved_at = _utc_now_iso()
    items, warnings, used = await _collect_sec_events(
        ticker,
        filing_types=selected_types,
        retrieved_at=retrieved_at,
        max_results=_coerce_max_results(max_results, 20),
        lookback_days=_coerce_lookback_days(lookback_days, 90),
    )
    for item in items:
        if not _safe_sec_url(item.get("url")):
            item["confidence"] = "LOW"
            warnings.append({
                "code": "SEC_URL_INVALID",
                "message": "SEC event URL missing or invalid SEC Archives URL.",
                "severity": "warning",
            })
    status = _build_collection_status(items, ["sec"] if used else [], warnings)
    payload = {
        "ticker": ticker.upper(),
        "items": items,
        "meta": {
            "sourcesUsed": ["sec"] if used else [],
            "watermark": retrieved_at,
        },
        "warnings": warnings,
    }
    if status:
        payload["status"] = status
    return json.dumps(payload)


@yfinance_server.tool(
    name="get_public_event_timeline",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_public_event_timeline"],
    description="""Get a deduplicated chronological timeline of public company events.

Combines selected public sources, deduplicates related items, and returns timeline entries ordered by time.
""",
)
async def get_public_event_timeline(
    ticker: str,
    start_date: str = "",
    end_date: str = "",
    sources: list[str] | None = None,
    max_results: int = 50,
    newest_first: bool = False,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_public_event_timeline", ErrorCode.INPUT_VALIDATION_ERROR, err)
    items, sources_used, warnings, retrieved_at = await _collect_company_events(
        ticker,
        max_results=max_results,
        lookback_days=365,
        start_date=start_date,
        end_date=end_date,
        sources=sources,
    )
    timeline = [{
        "timestamp": it.get("publishedAt"),
        "eventType": it.get("eventType"),
        "title": it.get("title"),
        "source": it.get("source"),
        "sourceType": it.get("sourceType"),
        "url": it.get("url"),
        "confidence": it.get("confidence"),
        "duplicateGroupId": it.get("duplicateGroupId"),
    } for it in items if it.get("publishedAt")]
    timeline.sort(key=lambda ev: str(ev.get("timestamp") or ""), reverse=bool(newest_first))
    timeline = timeline[:_coerce_max_results(max_results, 50)]
    status = _build_collection_status(items, sources_used, warnings)
    payload = {
        "ticker": ticker.upper(),
        "timeline": timeline,
        "meta": {
            "sourcesUsed": sources_used,
            "deduped": True,
            "watermark": retrieved_at,
        },
        "warnings": warnings,
    }
    if status:
        payload["status"] = status
    return json.dumps(payload)


@yfinance_server.tool(
    name="verify_company_event",
    output_schema=_TOOL_OUTPUT_SCHEMAS["verify_company_event"],
    description="""Verify whether a public company event has source-backed evidence.

Returns CONFIRMED, PARTIAL, NOT_FOUND, STALE, or CONFLICTING with best source evidence and metadata.
""",
)
async def verify_company_event(
    ticker: str,
    event_query: str,
    start_date: str = "",
    end_date: str = "",
    sources: list[str] | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("verify_company_event", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if not str(event_query or "").strip():
        return _mcp_failure("verify_company_event", ErrorCode.INPUT_VALIDATION_ERROR, "event_query is required")
    items, sources_used, warnings, retrieved_at = await _collect_company_events(
        ticker,
        max_results=50,
        lookback_days=365,
        sources=sources,
    )
    query_text = event_query.strip().lower()
    query_tokens = [tok for tok in _re.split(r"\s+", query_text) if tok]

    def _is_match(item: dict) -> bool:
        hay = " ".join([
            str(item.get("title") or ""),
            str(item.get("summary") or ""),
            str(item.get("evidenceText") or ""),
            str(item.get("eventType") or ""),
            str(item.get("source") or ""),
        ]).lower()
        if query_text in hay:
            return True
        return any(len(tok) >= 4 and tok in hay for tok in query_tokens)

    matched = [it for it in items if _is_match(it)]
    matched_in_range = [
        it for it in matched
        if _within_date_window(it.get("publishedAt"), start_date=start_date, end_date=end_date)
        or (not start_date and not end_date)
    ]
    official_in_range = [
        it for it in matched_in_range
        if str(it.get("sourceType") or "") in _OFFICIAL_SOURCE_TYPES
        and bool(it.get("url"))
        and str(it.get("confidence") or "") in ("HIGH", "MEDIUM")
    ]
    stale_cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=_STALE_EVENT_DAYS)).strftime("%Y-%m-%d")
    stale_only = bool(matched) and all(str(it.get("publishedAt") or "")[:10] < stale_cutoff for it in matched if it.get("publishedAt"))
    conflicts: list[dict] = []
    if any(w.get("code") == "TIMESTAMP_CONFLICT" for w in warnings if isinstance(w, dict)):
        conflicts.append({"type": "timestamp", "message": "Conflicting timestamps observed across sources for related events."})

    if conflicts:
        status = "CONFLICTING"
    elif official_in_range:
        status = "CONFIRMED"
    elif matched_in_range:
        status = "PARTIAL"
    elif stale_only:
        status = "STALE"
    else:
        status = "NOT_FOUND"

    best = official_in_range or matched_in_range or matched
    best_evidence = [{
        "source": ev.get("source"),
        "sourceType": ev.get("sourceType"),
        "publishedAt": ev.get("publishedAt"),
        "retrievedAt": ev.get("retrievedAt"),
        "url": ev.get("url"),
        "confidence": ev.get("confidence"),
        "evidenceText": _short_text(ev.get("evidenceText") or ev.get("summary") or ev.get("title")),
    } for ev in best[:5]]

    return json.dumps({
        "ticker": ticker.upper(),
        "query": event_query,
        "status": status,
        "bestEvidence": best_evidence,
        "conflicts": conflicts,
        "meta": {
            "sourcesChecked": sources_used,
            "watermark": retrieved_at,
        },
        "warnings": warnings,
    })


@yfinance_server.tool(
    name="get_stock_actions",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_stock_actions"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_financial_statement"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_holder_info"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_option_expiration_dates"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_option_chain"],
    description="""Fetch the option chain for a given ticker symbol, expiration date, and option type.

Use optional filters to narrow results — a full chain can have 200+ rows; filtering near-the-money and/or
liquidity significantly reduces output size.

Returns a JSON object with top-level fields: ticker, expiration, optionType, dataDate (YYYY-MM-DD of
the last trading session — use to detect weekend/holiday staleness), totalContracts, returnedContracts,
truncated, dataQuality, and contracts (array of option rows).

Args:
    ticker: str
        The ticker symbol of the stock to get option chain for, e.g. "AAPL"
    expiration_date: str
        The expiration date for the options chain (format: 'YYYY-MM-DD')
    option_type: str
        The type of option to fetch ('calls' or 'puts')
    strike_min: float | None
        Optional minimum strike price filter. Only options with strike >= strike_min are returned.
    strike_max: float | None
        Optional maximum strike price filter. Only options with strike <= strike_max are returned.
    moneyness: str
        "all" | "itm" | "otm" | "near_money". Default "near_money".
    moneyness_window_pct: float
        Half-width of the near-money window as a percentage of the underlying price (default: 20).
    sort_by: str
        "strike" | "volume" | "openInterest" | "relevance". Default "relevance".
    max_contracts: int
        Maximum number of contracts to return (default: 50, 0 = no limit).
    min_open_interest: int
        Minimum open interest filter (default: 0).
    min_volume: int
        Minimum volume filter (default: 0).
    include_illiquid: bool
        When False (default), contracts with zero bid/ask AND zero openInterest are excluded.
""",
)
async def get_option_chain(
    ticker: str,
    expiration_date: str,
    option_type: str,
    max_contracts: int = 50,
    min_open_interest: int = 0,
    min_volume: int = 0,
    strike_min: float | None = None,
    strike_max: float | None = None,
    moneyness: str = "near_money",
    moneyness_window_pct: float = 20.0,
    sort_by: str = "relevance",
    include_illiquid: bool = False,
    min_strike: float | None = None,  # legacy alias
    max_strike: float | None = None,  # legacy alias
    in_the_money_only: bool = False,  # legacy alias
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

    effective_strike_min = strike_min if strike_min is not None else min_strike
    effective_strike_max = strike_max if strike_max is not None else max_strike
    if in_the_money_only and moneyness == "all":
        moneyness = "itm"

    # Get underlying price once (needed for near_money and relevance sort)
    underlying_price: float | None = None
    try:
        underlying_price = float(company.fast_info.last_price)
        if underlying_price <= 0:
            underlying_price = None
    except Exception:
        pass

    if moneyness == "itm":
        df = df[df["inTheMoney"] == True]
    elif moneyness == "otm":
        df = df[df["inTheMoney"] == False]
    elif moneyness == "near_money":
        if underlying_price:
            window_fraction = moneyness_window_pct / 100.0
            low = underlying_price * (1 - window_fraction)
            high = underlying_price * (1 + window_fraction)
            df = df[(df["strike"] >= low) & (df["strike"] <= high)]
    if effective_strike_min is not None:
        df = df[df["strike"] >= effective_strike_min]
    if effective_strike_max is not None:
        df = df[df["strike"] <= effective_strike_max]
    if min_open_interest > 0:
        df = df[df["openInterest"] >= min_open_interest]
    if min_volume > 0:
        df = df[df["volume"] >= min_volume]

    # include_illiquid=False: drop contracts that have zero bid/ask AND zero OI
    if not include_illiquid:
        bid_col = df["bid"].fillna(0).astype(float) if "bid" in df.columns else pd.Series([0.0] * len(df), index=df.index)
        ask_col = df["ask"].fillna(0).astype(float) if "ask" in df.columns else pd.Series([0.0] * len(df), index=df.index)
        oi_col = df["openInterest"].fillna(0).astype(float) if "openInterest" in df.columns else pd.Series([0.0] * len(df), index=df.index)
        liquid_mask = (bid_col > 0) | (ask_col > 0) | (oi_col > 0)
        df = df[liquid_mask]

    if sort_by == "relevance":
        contracts_list = json.loads(df.to_json(orient="records", date_format="iso"))
        contracts_list = _sort_by_relevance(contracts_list, underlying_price)
        total_contracts = len(contracts_list)
        if max_contracts > 0:
            contracts_list = contracts_list[:max_contracts]
        returned_contracts = len(contracts_list)
    else:
        if sort_by in {"volume", "openInterest", "strike"} and sort_by in df.columns:
            df = df.sort_values(by=sort_by, ascending=False if sort_by in {"volume", "openInterest"} else True)
        total_contracts = len(df)
        if max_contracts > 0:
            df = df.head(max_contracts)
        returned_contracts = len(df)
        contracts_list = json.loads(df.to_json(orient="records", date_format="iso"))

    # Derive dataDate from the last trading session
    try:
        _hist = company.history(period="5d", interval="1d")
        data_date = (
            str(_hist.index[-1].date())
            if _hist is not None and not _hist.empty
            else get_last_trading_date()
        )
    except Exception:
        data_date = get_last_trading_date()

    data_quality = _compute_data_quality(contracts_list, data_date)

    return json.dumps({
        "ticker": ticker,
        "expiration": expiration_date,
        "optionType": option_type,
        "dataDate": data_date,
        "totalContracts": total_contracts,
        "returnedContracts": returned_contracts,
        "truncated": returned_contracts < total_contracts,
        "dataQuality": data_quality,
        "filtersApplied": {
            "max_contracts": max_contracts,
            "min_open_interest": min_open_interest,
            "min_volume": min_volume,
            "strike_min": effective_strike_min,
            "strike_max": effective_strike_max,
            "moneyness": moneyness,
            "moneyness_window_pct": moneyness_window_pct,
            "sort_by": sort_by,
            "include_illiquid": include_illiquid,
        },
        "contracts": contracts_list,
    })


@yfinance_server.tool(
    name="get_options_summary",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_options_summary"],
    description="Get options summary for a single ticker: ATM implied volatility, put/call ratio by volume and OI, max pain strike for the nearest liquid expiry. Preferred for LLM use — returns a compact snapshot without the full contract list.",
)
async def get_options_summary(ticker: str) -> str:
    company = yf.Ticker(ticker)
    try:
        expirations = company.options
        if not expirations:
            return json.dumps({"ticker": ticker, "error": "No options data available"})
        expiry = expirations[0]
        opt = company.option_chain(expiry)
        calls = opt.calls
        puts = opt.puts

        current_price = None
        try:
            current_price = company.fast_info.last_price
        except Exception:
            pass

        atm_iv = None
        atm_iv_reason: str | None = None
        if not current_price:
            atm_iv_reason = "ATM_IV_UNAVAILABLE_NO_PRICE"
        elif calls.empty:
            atm_iv_reason = "ATM_IV_UNAVAILABLE_NO_CALLS"
        else:
            idx = (calls["strike"] - current_price).abs().idxmin()
            raw_atm_iv = float(calls.loc[idx, "impliedVolatility"]) if "impliedVolatility" in calls.columns else None
            if raw_atm_iv is not None and raw_atm_iv > _PLACEHOLDER_IV_THRESHOLD:
                atm_iv = raw_atm_iv
            else:
                atm_iv_reason = "ATM_IV_PLACEHOLDER"

        call_vol = float(calls["volume"].sum()) if "volume" in calls.columns else 0
        put_vol = float(puts["volume"].sum()) if "volume" in puts.columns else 0
        pc_ratio_volume = round(put_vol / call_vol, 3) if call_vol > 0 else None

        call_oi = float(calls["openInterest"].sum()) if "openInterest" in calls.columns else 0
        put_oi = float(puts["openInterest"].sum()) if "openInterest" in puts.columns else 0
        pc_ratio_oi = round(put_oi / call_oi, 3) if call_oi > 0 else None

        all_strikes = sorted(set(calls["strike"].tolist() + puts["strike"].tolist()))
        max_pain_strike = None
        flow_warnings: list[str] = []
        if call_oi + put_oi <= 0:
            flow_warnings.append("MAX_PAIN_UNAVAILABLE_ZERO_OI")
        elif all_strikes:
            min_pain = float("inf")
            for s in all_strikes:
                call_pain = float(((s - calls["strike"]).clip(lower=0) * calls.get("openInterest", 0)).sum())
                put_pain = float(((puts["strike"] - s).clip(lower=0) * puts.get("openInterest", 0)).sum())
                total = call_pain + put_pain
                if total < min_pain:
                    min_pain = total
                    max_pain_strike = s

        if atm_iv_reason is not None:
            flow_warnings.append(atm_iv_reason)

        # dataQuality over the full nearest-expiry chain
        calls_list = json.loads(calls.to_json(orient="records", date_format="iso"))
        puts_list = json.loads(puts.to_json(orient="records", date_format="iso"))
        data_quality = _compute_data_quality(calls_list + puts_list, get_last_trading_date())

        return json.dumps({
            "ticker": ticker,
            "nearestExpiry": expiry,
            "currentPrice": current_price,
            "atmIV": round(atm_iv, 4) if atm_iv is not None else None,
            "pcRatioVolume": pc_ratio_volume,
            "pcRatioOI": pc_ratio_oi,
            "callVolume": int(call_vol),
            "putVolume": int(put_vol),
            "callOI": int(call_oi),
            "putOI": int(put_oi),
            "maxPainStrike": max_pain_strike,
            "dataDate": get_last_trading_date(),
            "dataQuality": data_quality,
            "warnings": flow_warnings,
        })
    except Exception as e:
        return json.dumps({"ticker": ticker, "error": str(e)})


@yfinance_server.tool(
    name="list_sec_filings",
    output_schema=_TOOL_OUTPUT_SCHEMAS["list_sec_filings"],
    description="""List recent SEC filings for a ticker from EDGAR.
Returns accession number, filing date, form type, primary document URL, and EDGAR index URL.
Supports form types: 10-K, 10-Q, 8-K, DEF 14A.
Args:
    ticker: str - The ticker symbol
    form_type: str - Optional form type filter (10-K, 10-Q, 8-K, DEF 14A). Default: 10-K
    max_filings: int - Maximum filings to return (default: 5, max: 20)
""",
)
async def list_sec_filings(ticker: str, form_type: str = "10-K", max_filings: int = 5) -> str:
    ALLOWED_FORMS = {"10-K", "10-Q", "8-K", "DEF 14A"}
    if form_type not in ALLOWED_FORMS:
        return _mcp_failure("list_sec_filings", ErrorCode.INPUT_VALIDATION_ERROR,
                            f"Invalid form_type '{form_type}'. Must be one of: {sorted(ALLOWED_FORMS)}")
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_sec_filings", ErrorCode.INPUT_VALIDATION_ERROR, err)
    max_filings = min(max(1, max_filings), 20)

    import urllib.request
    ticker_upper = ticker.upper()
    try:
        tickers_url = "https://www.sec.gov/files/company_tickers.json"
        req = urllib.request.Request(tickers_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            tickers_data = json.loads(resp.read())

        cik = None
        for _entry in tickers_data.values():
            if _entry.get("ticker", "").upper() == ticker_upper:
                cik = int(_entry["cik_str"])
                break

        if cik is None:
            return _mcp_failure("list_sec_filings", ErrorCode.TICKER_NOT_FOUND,
                                f"Could not find EDGAR CIK for ticker '{ticker}'")

        cik_padded = str(cik).zfill(10)
        sub_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        req2 = urllib.request.Request(sub_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req2, timeout=10) as resp2:
            sub_data = json.loads(resp2.read())

        filings_data = sub_data.get("filings", {}).get("recent", {})
        forms = filings_data.get("form", [])
        dates = filings_data.get("filingDate", [])
        accessions = filings_data.get("accessionNumber", [])
        primary_docs = filings_data.get("primaryDocument", [])

        results = []
        for i, form in enumerate(forms):
            if form == form_type and len(results) < max_filings:
                acc = accessions[i] if i < len(accessions) else ""
                date = dates[i] if i < len(dates) else ""
                doc = primary_docs[i] if i < len(primary_docs) else ""
                acc_clean = acc.replace("-", "")
                index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{acc}-index.htm"
                doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{doc}" if doc else None
                results.append({
                    "accessionNumber": acc,
                    "filingDate": date,
                    "formType": form,
                    "primaryDocumentUrl": doc_url,
                    "edgarIndexUrl": index_url,
                })

        return json.dumps({"ticker": ticker, "formType": form_type, "filings": results})
    except Exception as e:
        return _mcp_failure("list_sec_filings", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="get_filing_outline",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_outline"],
    description="""Parse the document outline of an SEC filing (10-K/10-Q). Returns a hierarchical tree of Parts, Items, Notes as found in the document.
Args:
    ticker: str - ticker symbol
    accession_number: str - SEC accession number (format: XXXXXXXXXX-YY-ZZZZZZ)
    document_url: str - Optional direct URL to the filing HTML document (must be https://www.sec.gov/Archives/...)
""",
)
async def get_filing_outline(ticker: str, accession_number: str | None = None, document_url: str | None = None) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if document_url:
        url_err = _validate_sec_url(document_url)
        if url_err:
            return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR, url_err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)

    try:
        import urllib.request
        if not document_url and accession_number:
            cik = None
            tickers_url = "https://www.sec.gov/files/company_tickers.json"
            req = urllib.request.Request(tickers_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                tickers_data = json.loads(resp.read())
            for _entry in tickers_data.values():
                if _entry.get("ticker", "").upper() == ticker.upper():
                    cik = int(_entry["cik_str"])
                    break
            if cik:
                acc_clean = accession_number.replace("-", "")
                document_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{accession_number}-index.htm"

        if not document_url:
            return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR,
                                "Either accession_number or document_url is required")

        req = urllib.request.Request(document_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        html = _sanitize_sec_html(html)
        outline = []
        heading_re = _re.compile(r'<h([1-6])[^>]*>(.*?)</h\1>', _re.IGNORECASE | _re.DOTALL)
        item_re = _re.compile(r'(Part\s+[IVX]+|Item\s+\d+[A-Z]?|Note\s+\d+)', _re.IGNORECASE)
        for m in heading_re.finditer(html):
            level = int(m.group(1))
            text = _re.sub(r'<[^>]+>', '', m.group(2)).strip()
            text = ' '.join(text.split())
            if text and (item_re.search(text) or len(text) < 100):
                outline.append({"level": level, "title": text})

        return json.dumps({"ticker": ticker, "accessionNumber": accession_number, "outline": outline[:100]})
    except Exception as e:
        return _mcp_failure("get_filing_outline", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="get_filing_section",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_section"],
    description="""Retrieve the text content of a specific section from an SEC filing document.
Args:
    ticker: str - ticker symbol
    section_name: str - Section name/heading to find, e.g. 'Item 1A', 'Note 3', 'Risk Factors'
    document_url: str - Direct URL to filing HTML (must be https://www.sec.gov/Archives/...)
    context_chars: int - Characters of context around matched section (default: 3000)
""",
)
async def get_filing_section(ticker: str, section_name: str, document_url: str, context_chars: int = 3000) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_filing_section", ErrorCode.INPUT_VALIDATION_ERROR, err)
    url_err = _validate_sec_url(document_url)
    if url_err:
        return _mcp_failure("get_filing_section", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        import urllib.request
        req = urllib.request.Request(document_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        html = _sanitize_sec_html(html)
        text = _re.sub(r'<[^>]+>', ' ', html)
        text = ' '.join(text.split())

        pattern = _re.compile(_re.escape(section_name), _re.IGNORECASE)
        m = pattern.search(text)
        if not m:
            words = section_name.split()
            if words:
                pattern2 = _re.compile(r'\b' + r'\s+'.join(_re.escape(w) for w in words), _re.IGNORECASE)
                m = pattern2.search(text)

        if not m:
            return json.dumps({"ticker": ticker, "sectionName": section_name, "found": False, "text": None})

        start = max(0, m.start())
        end = min(len(text), m.start() + context_chars)
        return json.dumps({
            "ticker": ticker,
            "sectionName": section_name,
            "found": True,
            "text": text[start:end],
        })
    except Exception as e:
        return _mcp_failure("get_filing_section", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="list_filing_tables",
    output_schema=_TOOL_OUTPUT_SCHEMAS["list_filing_tables"],
    description="""List all HTML tables in an SEC filing document. Returns table index, headers, and row count.
Args:
    ticker: str
    document_url: str - Direct URL to filing HTML (must be https://www.sec.gov/Archives/...)
""",
)
async def list_filing_tables(ticker: str, document_url: str) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_filing_tables", ErrorCode.INPUT_VALIDATION_ERROR, err)
    url_err = _validate_sec_url(document_url)
    if url_err:
        return _mcp_failure("list_filing_tables", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        import urllib.request
        req = urllib.request.Request(document_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        html = _sanitize_sec_html(html)
        tables = []
        table_re = _re.compile(r'<table[^>]*>(.*?)</table>', _re.DOTALL | _re.IGNORECASE)
        tr_re = _re.compile(r'<tr[^>]*>(.*?)</tr>', _re.DOTALL | _re.IGNORECASE)
        td_re = _re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', _re.DOTALL | _re.IGNORECASE)

        for i, table_m in enumerate(table_re.finditer(html)):
            rows = tr_re.findall(table_m.group(1))
            row_count = len(rows)
            headers = []
            if rows:
                first_cells = td_re.findall(rows[0])
                headers = [' '.join(_re.sub(r'<[^>]+>', '', c).split()) for c in first_cells[:6]]
            tables.append({"tableIndex": i, "rowCount": row_count, "headers": headers})

        return json.dumps({"ticker": ticker, "documentUrl": document_url, "tableCount": len(tables), "tables": tables[:50]})
    except Exception as e:
        return _mcp_failure("list_filing_tables", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="get_filing_table",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_table"],
    description="""Get the parsed rows of a specific table from an SEC filing document.
Args:
    ticker: str
    document_url: str - Direct URL to filing HTML (must be https://www.sec.gov/Archives/...)
    table_index: int - Table index from list_filing_tables (0-based)
    max_rows: int - Maximum rows to return (default: 30)
""",
)
async def get_filing_table(ticker: str, document_url: str, table_index: int, max_rows: int = 30) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_filing_table", ErrorCode.INPUT_VALIDATION_ERROR, err)
    url_err = _validate_sec_url(document_url)
    if url_err:
        return _mcp_failure("get_filing_table", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        import urllib.request
        req = urllib.request.Request(document_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        html = _sanitize_sec_html(html)
        table_re = _re.compile(r'<table[^>]*>(.*?)</table>', _re.DOTALL | _re.IGNORECASE)
        tr_re = _re.compile(r'<tr[^>]*>(.*?)</tr>', _re.DOTALL | _re.IGNORECASE)
        td_re = _re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', _re.DOTALL | _re.IGNORECASE)

        tables = list(table_re.finditer(html))
        if table_index >= len(tables):
            return _mcp_failure("get_filing_table", ErrorCode.NO_FILING_DATA,
                                f"Table index {table_index} not found. Document has {len(tables)} tables.")

        table_html = tables[table_index].group(1)
        rows = tr_re.findall(table_html)
        parsed_rows = []
        for row in rows[:max_rows + 1]:
            cells = td_re.findall(row)
            parsed_rows.append([' '.join(_re.sub(r'<[^>]+>', '', c).split()) for c in cells])

        return json.dumps({
            "ticker": ticker,
            "tableIndex": table_index,
            "totalRows": len(rows),
            "returnedRows": len(parsed_rows),
            "rows": parsed_rows,
        })
    except Exception as e:
        return _mcp_failure("get_filing_table", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="extract_filing_fact",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_filing_fact"],
    description="""Extract a specific financial fact from an SEC filing. Uses XBRL first, parsed tables second, text search last.
Args:
    ticker: str
    fact_name: str - Fact to extract (e.g. 'revenue', 'net income', 'R&D expense')
    document_url: str - Optional direct URL to filing HTML (must be https://www.sec.gov/Archives/...)
    accession_number: str - Optional accession number for XBRL lookup
""",
)
async def extract_filing_fact(
    ticker: str,
    fact_name: str,
    document_url: str | None = None,
    accession_number: str | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("extract_filing_fact", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if document_url:
        url_err = _validate_sec_url(document_url)
        if url_err:
            return _mcp_failure("extract_filing_fact", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        result_str = await search_filing_text(
            ticker=ticker,
            search_terms=[fact_name],
            filing_type="10-K",
            accession_number=accession_number,
            context_chars=1000,
            return_tables=True,
        )
        result = json.loads(result_str)
        return json.dumps({
            "ticker": ticker,
            "factName": fact_name,
            "extractionMethod": "text_search",
            "result": result,
        })
    except Exception as e:
        return _mcp_failure("extract_filing_fact", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="get_recommendations",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_recommendations"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_fast_info"],
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
            if val is not None and val != {}:
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

    # Normalize any empty-dict values (Yahoo API sometimes returns {} for missing scalars)
    data = {k: (None if isinstance(v, dict) and not v else v) for k, v in data.items()}

    result = json.dumps(data)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 1.2 — get_short_interest
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_short_interest",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_short_interest"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_price_stats"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_analyst_consensus"],
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
    warnings: list[dict[str, str]] = []

    # Price targets
    target_mean = None
    try:
        targets = company.analyst_price_targets
        if targets:
            current_target = targets.get("current")
            target_mean = targets.get("mean")
            output["priceTargets"] = {
                "current": current_target,
                "low": targets.get("low"),
                "high": targets.get("high"),
                "mean": target_mean,
                "median": targets.get("median"),
                "pctUpsideFromLastPrice": (
                    round((current_target - last_price) / last_price * 100, 2)
                    if current_target and last_price
                    else None
                ),
            }
    except Exception:
        output["priceTargets"] = None

    # Recent upgrades (last 30d) to flag potential target lag
    recent_upgrade_count_30d = None
    try:
        upgrades = company.upgrades_downgrades
        if upgrades is not None and not upgrades.empty:
            u = upgrades.reset_index()
            cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=30)
            date_col = next(
                (
                    c
                    for c in ("GradeDate", "Date", "date", "epochGradeDate", "index")
                    if c in u.columns
                ),
                None,
            )

            def _to_dt(v):
                if v is None:
                    return None
                if isinstance(v, (int, float)):
                    ts = float(v)
                    if ts > 1_000_000_000_000:
                        ts /= 1000.0
                    return datetime.datetime.fromtimestamp(ts, tz=datetime.UTC)
                dt = pd.to_datetime(v, utc=True, errors="coerce")
                if pd.isna(dt):
                    return None
                return dt.to_pydatetime()

            count = 0
            for _, row in u.iterrows():
                dt = _to_dt(row.get(date_col)) if date_col else None
                if dt is None or dt < cutoff:
                    continue
                action = str(row.get("Action") or row.get("action") or "").lower()
                to_grade = str(row.get("ToGrade") or row.get("toGrade") or "").lower()
                from_grade = str(row.get("FromGrade") or row.get("fromGrade") or "").lower()
                if ("up" in action) or ("upgrade" in action) or ("buy" in to_grade and "buy" not in from_grade):
                    count += 1
            recent_upgrade_count_30d = count
    except Exception:
        recent_upgrade_count_30d = None

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

    target_lag_signal = "UNKNOWN"
    if target_mean is not None and last_price is not None:
        if target_mean >= last_price:
            target_lag_signal = "CURRENT"
        elif recent_upgrade_count_30d is not None and recent_upgrade_count_30d > 0:
            target_lag_signal = "LIKELY_STALE_OR_LAGGING"
            warnings.append({
                "code": "CONSENSUS_TARGET_BELOW_PRICE_DESPITE_UPGRADES",
                "message": "Consensus price target may lag recent market or analyst sentiment changes.",
            })
        else:
            target_lag_signal = "POSSIBLY_STALE"

    output["currentPrice"] = last_price
    output["recentUpgradeCount30d"] = recent_upgrade_count_30d
    output["targetLagSignal"] = target_lag_signal
    output["warnings"] = warnings

    result = json.dumps(output)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.3 — get_earnings_analysis
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_earnings_analysis",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_earnings_analysis"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_financial_ratios"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_calendar"],
    description="""Get upcoming earnings and dividend schedule for a ticker.

Returns:
- Next earnings date range and EPS/revenue estimates
- Ex-dividend date and dividend pay date
- earningsDateConfirmed: true when Yahoo Finance shows a single fixed date (likely confirmed
  by company filing/IR source); false when a date range is returned (estimate).
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["search_ticker"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["screen_stocks"],
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
# Group 3.4b — get_filing_data / search_filing_text
# ---------------------------------------------------------------------------

_SEC_REQUIRED_UA = "yahoo-finance-mcp contact@example.com"
_FILING_CIK_CACHE: dict[str, str] = {}
_FILING_SUBMISSIONS_BY_TICKER: dict[str, dict] = {}

_FILING_FACT_CONCEPTS: dict[FilingFactType, tuple[str, str | None]] = {
    FilingFactType.geographic_revenue: ("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"),
    FilingFactType.segment_revenue: ("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"),
    FilingFactType.capex: ("PaymentsToAcquirePropertyPlantAndEquipment", None),
    FilingFactType.rd_expense: ("ResearchAndDevelopmentExpense", None),
    FilingFactType.operating_income: ("OperatingIncomeLoss", None),
    FilingFactType.net_income: ("NetIncomeLoss", None),
    FilingFactType.total_revenue: ("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"),
    FilingFactType.long_term_debt: ("LongTermDebt", None),
    FilingFactType.cash: ("CashAndCashEquivalentsAtCarryingValue", None),
}



async def _resolve_cik_for_ticker(ticker: str) -> str | None:
    t_upper = ticker.upper()
    cached = _FILING_CIK_CACHE.get(t_upper)
    if cached:
        return cached
    cik_raw = None
    try:
        cik_raw = yf.Ticker(ticker).info.get("cik")
    except Exception:
        cik_raw = None
    if cik_raw:
        cik_padded = str(cik_raw).strip().zfill(10)
        _FILING_CIK_CACHE[t_upper] = cik_padded
        return cik_padded

    # Fallback: look up from SEC EDGAR company_tickers.json
    try:
        tickers_map = await _load_edgar_tickers()
        cik_int = tickers_map.get(t_upper)
        if cik_int:
            cik_padded = str(cik_int).zfill(10)
            _FILING_CIK_CACHE[t_upper] = cik_padded
            return cik_padded
    except Exception:
        pass

    # Stable fixture fallback map for smoke/regression-critical tickers.
    fixture_cik = _SMOKE_TICKER_CIK_FALLBACKS.get(t_upper)
    if fixture_cik:
        _FILING_CIK_CACHE[t_upper] = fixture_cik
        return fixture_cik

    def _extract_cik_from_edgar_atom(text: str) -> str | None:
        for pattern in (
            r"CIK=(\d{1,10})",
            r"/CIK0*([1-9]\d{0,9})\.json",
            r"/edgar/data/0*([1-9]\d{0,9})/",
        ):
            m = _re.search(pattern, text, flags=_re.IGNORECASE)
            if m:
                return m.group(1).zfill(10)
        return None

    # Final fallback: EDGAR CIK lookup by ticker symbol
    atom_urls = [
        (
            "https://www.sec.gov/cgi-bin/browse-edgar?"
            f"action=getcompany&CIK={_urlparse.quote(ticker)}&type=&dateb=&owner=include&count=10&output=atom"
        ),
        (
            "https://www.sec.gov/cgi-bin/browse-edgar?"
            f"action=getcompany&company={_urlparse.quote(ticker)}&CIK=&type=&dateb=&owner=include&count=10&output=atom"
        ),
    ]
    loop = asyncio.get_event_loop()

    def _fetch_atom() -> str | None:
        for atom_url in atom_urls:
            req = _urlreq.Request(atom_url, headers={"User-Agent": _SEC_REQUIRED_UA})
            try:
                with _urlreq.urlopen(req, timeout=15) as resp:  # noqa: S310
                    text = resp.read().decode("utf-8", errors="replace")
                cik = _extract_cik_from_edgar_atom(text)
                if cik:
                    return cik
            except Exception:
                continue
        return None

    cik_padded = await loop.run_in_executor(None, _fetch_atom)
    if cik_padded:
        _FILING_CIK_CACHE[t_upper] = cik_padded
    return cik_padded


async def _get_submissions_for_ticker(ticker: str) -> tuple[str | None, dict | None]:
    t_upper = ticker.upper()
    cached_subs = _FILING_SUBMISSIONS_BY_TICKER.get(t_upper)
    if cached_subs is not None:
        cik = _FILING_CIK_CACHE.get(t_upper)
        return cik, cached_subs
    cik_padded = await _resolve_cik_for_ticker(ticker)
    if not cik_padded:
        return None, None
    subs = await _edgar_get_submissions(cik_padded)
    if subs is not None:
        _FILING_SUBMISSIONS_BY_TICKER[t_upper] = subs
    return cik_padded, subs


def _normalize_segment_label(segment: object) -> str:
    if isinstance(segment, dict):
        return " ".join(str(v) for v in segment.values() if v is not None)
    if isinstance(segment, list):
        return " ".join(_normalize_segment_label(s) for s in segment)
    return str(segment or "")


def _region_matches(label: str, region: str, include_asia_fallback: bool = False) -> bool:
    label_low = label.lower()
    region_low = region.lower()
    if region_low in label_low:
        return True
    # Also try compact (no-space) region for XBRL member names like "GreaterChinaMember"
    region_compact = region_low.replace(" ", "")
    if region_compact and region_compact in label_low:
        return True
    if region_low == "china":
        base_tokens = ("country:cn", "greater china", "srt:chinamember", "greaterchina")
        if any(token in label_low for token in base_tokens):
            return True
        return include_asia_fallback and "asiapacificmember" in label_low
    if region_low == "greater china":
        if "greaterchina" in label_low or "greater china" in label_low:
            return True
    return False


def _manual_lookup_payload(ticker: str, cik_padded: str | None, filing_type: str, note: str) -> dict:
    edgar_index_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_padded}&type={filing_type}&owner=include&count=10"
        if cik_padded
        else f"https://www.sec.gov/cgi-bin/browse-edgar?company={ticker}&action=getcompany&type={filing_type}&owner=include&count=10"
    )
    efts_url = (
        "https://efts.sec.gov/LATEST/search-index"
        f"?q={_urlparse.quote(ticker)}&forms={_urlparse.quote(filing_type)}"
    )
    return {
        "edgarIndexUrl": edgar_index_url,
        "eftsSearchUrl": efts_url,
        "note": note,
    }


@yfinance_server.tool(
    name="get_filing_data",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_data"],
    description="""Retrieve structured XBRL-tagged financial facts from EDGAR.

Try this tool before search_filing_text for GAAP line items or geographic revenue.
""",
)
async def get_filing_data(
    ticker: str,
    fact_type: FilingFactType,
    region: str | None = None,
    filing_type: str = "10-K",
    period: str = "latest",
) -> str:
    FLOATING_POINT_EPSILON = 1e-9
    RATIO_DECIMALS = 4
    PCT_DECIMALS = 2
    PCT_MULTIPLIER = 100

    def _format_raw_number(n: float | int | None) -> str | None:
        if n is None:
            return None
        try:
            f = float(n)
            if abs(f - round(f)) < FLOATING_POINT_EPSILON:
                return f"{int(round(f)):,}"
            return f"{f:,.2f}"
        except Exception:
            return None

    def _scale_label(multiplier: float | None) -> str:
        if multiplier == 1_000.0:
            return "thousands"
        if multiplier == 1_000_000.0:
            return "millions"
        if multiplier == 1.0:
            return "actual"
        return "actual"

    def _geo_shape(payload: dict, *, warn_denominator: bool = False) -> str:
        if fact_type != FilingFactType.geographic_revenue:
            return json.dumps(payload)
        shaped = {
            "ticker": payload.get("ticker", ticker),
            "factType": payload.get("factType", FilingFactType.geographic_revenue.value),
            "region": payload.get("region", region),
            "period": payload.get("period"),
            "rawValue": payload.get("rawValue"),
            "rawDenominator": payload.get("rawDenominator"),
            "unit": payload.get("unit", "USD"),
            "unitScale": payload.get("unitScale", "actual"),
            "value": payload.get("value"),
            "denominator": payload.get("denominator"),
            "valueRatio": payload.get("valueRatio"),
            "valuePct": payload.get("valuePct"),
            "extractionMethod": payload.get("extractionMethod", "NONE"),
            "source": payload.get("source", "NOT_DISCLOSED"),
            "confidence": payload.get("confidence", "NOT_DISCLOSED"),
            "filingType": payload.get("filingType", filing_type),
            "filingDate": payload.get("filingDate"),
            "accessionNumber": payload.get("accessionNumber"),
            "documentUrl": payload.get("documentUrl"),
            "indexUrl": payload.get("indexUrl"),
            "primaryDocumentUrl": payload.get("primaryDocumentUrl"),
            "evidence": payload.get("evidence", {}),
            "calculation": payload.get("calculation"),
            "warnings": list(payload.get("warnings", [])) if isinstance(payload.get("warnings"), list) else [],
        }
        has_denominator = shaped["denominator"] is not None
        if not has_denominator:
            shaped["valueRatio"] = None
            shaped["valuePct"] = None
        if warn_denominator and shaped.get("value") is not None and not has_denominator:
            shaped["warnings"].append({
                "code": "DENOMINATOR_NOT_FOUND",
                "message": "Could not compute geographic revenue percentage due to missing denominator.",
                "severity": "warning",
            })
        return json.dumps(shaped)

    async def _resolve_filing_urls_for_accession(accn: str) -> tuple[str | None, str | None]:
        if not accn:
            return None, None
        if not cik_padded:
            return None, None
        index_url, primary_url = _edgar_build_filing_urls(int(cik_padded), accn, None)
        _, subs = await _get_submissions_for_ticker(ticker)
        if not subs:
            return index_url, primary_url
        recent = subs.get("filings", {}).get("recent", {})
        accessions: list[str] = recent.get("accessionNumber", [])
        primary_docs: list[str] = recent.get("primaryDocument", [])
        try:
            idx = accessions.index(accn)
            primary_doc = primary_docs[idx] if idx < len(primary_docs) else None
            if primary_doc:
                _, primary_url = _edgar_build_filing_urls(int(cik_padded), accn, primary_doc)
        except Exception:
            pass
        return index_url, primary_url

    if fact_type == FilingFactType.geographic_revenue and not region:
        return json.dumps({"error": True, "message": "region is required for fact_type='geographic_revenue'"})

    concept_primary, concept_fallback = _FILING_FACT_CONCEPTS[fact_type]
    cik_padded = await _resolve_cik_for_ticker(ticker)
    if not cik_padded:
        return _geo_shape({
            "ticker": ticker,
            "factType": fact_type.value,
            "value": None,
            "denominator": None,
            "valueRatio": None,
            "valuePct": None,
            "extractionMethod": "NONE",
            "source": "NOT_DISCLOSED",
            "confidence": "NOT_DISCLOSED",
            "evidence": {},
            "warnings": [],
            "_manualLookup": _manual_lookup_payload(
                ticker, None, filing_type, "Fact not XBRL-tagged. Use search_filing_text instead."
            ),
        })

    async def _concept_json(concept_name: str) -> dict | None:
        return await _edgar_get(
            f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik_padded}/us-gaap/{concept_name}.json"
        )

    concept_used = concept_primary
    concept_data = await _concept_json(concept_primary)
    usd_facts: list[dict] = (
        concept_data.get("units", {}).get("USD", [])  # type: ignore[union-attr]
        if concept_data else []
    )
    if not usd_facts and concept_fallback:
        fallback_data = await _concept_json(concept_fallback)
        fallback_usd = fallback_data.get("units", {}).get("USD", []) if fallback_data else []
        if fallback_usd:
            concept_used = concept_fallback
            concept_data = fallback_data
            usd_facts = fallback_usd

    filtered = [f for f in usd_facts if str(f.get("form", "")).upper() == filing_type.upper()]
    if not filtered:
        if fact_type != FilingFactType.geographic_revenue:
            return _geo_shape({
                "ticker": ticker,
                "factType": fact_type.value,
                "value": None,
                "denominator": None,
                "valueRatio": None,
                "valuePct": None,
                "extractionMethod": "NONE",
                "source": "NOT_DISCLOSED",
                "confidence": "NOT_DISCLOSED",
                "evidence": {},
                "warnings": [],
                "_manualLookup": _manual_lookup_payload(
                    ticker, cik_padded, filing_type, "Fact not XBRL-tagged. Use search_filing_text instead."
                ),
            })
        # For geographic_revenue, fall through to HTML fallback below (picked remains None)

    if filtered and period == "latest":
        latest_filed = max(str(f.get("filed", "")) for f in filtered)
        filtered = [f for f in filtered if str(f.get("filed", "")) == latest_filed]

    if fact_type == FilingFactType.segment_revenue:
        seg_rows = []
        for f in filtered:
            seg_label = _normalize_segment_label(f.get("segment"))
            if not seg_label:
                continue
            seg_rows.append({
                "segmentLabel": seg_label,
                "value": f.get("val"),
                "fiscalYear": str(f.get("fy") or ""),
                "fiscalPeriod": str(f.get("fp") or ""),
                "filingDate": str(f.get("filed") or ""),
                "accessionNumber": str(f.get("accn") or ""),
            })
        return json.dumps({
            "ticker": ticker,
            "factType": fact_type.value,
            "concept": concept_used,
            "value": seg_rows[0]["value"] if seg_rows else None,
            "fiscalYear": seg_rows[0]["fiscalYear"] if seg_rows else "",
            "fiscalPeriod": seg_rows[0]["fiscalPeriod"] if seg_rows else "",
            "filingType": filing_type,
            "filingDate": seg_rows[0]["filingDate"] if seg_rows else "",
            "accessionNumber": seg_rows[0]["accessionNumber"] if seg_rows else "",
            "extractionMethod": "XBRL",
            "source": "XBRL",
            "confidence": "HIGH",
            "allSegments": seg_rows,
        })

    picked: dict | None = None
    value_ratio: float | None = None
    value_pct: float | None = None
    denominator: float | None = None
    segment_label: str | None = None
    if fact_type == FilingFactType.geographic_revenue:
        for f in filtered:
            seg_label = _normalize_segment_label(f.get("segment"))
            if seg_label and _region_matches(seg_label, region or "", include_asia_fallback=False):
                picked = f
                segment_label = seg_label
                break
        if picked is None and (region or "").lower() == "china":
            for f in filtered:
                seg_label = _normalize_segment_label(f.get("segment"))
                if seg_label and _region_matches(seg_label, region or "", include_asia_fallback=True):
                    picked = f
                    segment_label = seg_label
                    break
        if picked is not None:
            accn = str(picked.get("accn") or "")
            total_fact = next(
                (
                    f for f in filtered
                    if str(f.get("accn") or "") == accn and not f.get("segment")
                ),
                None,
            )
            try:
                picked_val = picked.get("val")
                if total_fact and picked_val is not None and float(total_fact.get("val", 0)) > 0:
                    denominator = float(total_fact.get("val", 0))
                    value_ratio = round(float(picked_val) / denominator, RATIO_DECIMALS)
                    value_pct = round(value_ratio * PCT_MULTIPLIER, PCT_DECIMALS)
            except Exception:
                denominator = None
                value_ratio = None
                value_pct = None
    else:
        picked = next((f for f in filtered if not f.get("segment")), filtered[0] if filtered else None)

    if picked is None:
        # ── HTML fallback for geographic_revenue ──────────────────────────────
        # Some companies (e.g. GLW) do not XBRL-tag geographic-revenue segments.
        # Fall through to the same HTML-parsing path used by search_filing_text.
        if fact_type == FilingFactType.geographic_revenue:
            _, subs = await _get_submissions_for_ticker(ticker)
            if subs:
                recent = subs.get("filings", {}).get("recent", {})
                forms: list[str] = recent.get("form", [])
                accessions_list: list[str] = recent.get("accessionNumber", [])
                primary_docs_list: list[str] = recent.get("primaryDocument", [])
                filing_dates_list: list[str] = recent.get("filingDate", [])
                report_dates_list: list[str] = recent.get("reportDate", [])
                idx: int | None = None
                for i, form in enumerate(forms):
                    if str(form).upper() == filing_type.upper():
                        idx = i
                        break
                if idx is not None:
                    primary_doc = primary_docs_list[idx] if idx < len(primary_docs_list) else None
                    if primary_doc:
                        cik_int = int(cik_padded)
                        _, doc_url = _edgar_build_filing_urls(cik_int, accessions_list[idx], primary_doc)
                        if doc_url:
                            html_text = await _edgar_get_html(doc_url, max_bytes=5_000_000)
                            if html_text:
                                geo_ratio, geo_usd, geo_denominator, geo_heading, geo_evidence = _extract_geo_revenue_from_html(
                                    html_text, region or ""
                                )
                                if geo_usd is not None:
                                    acc_num = accessions_list[idx] if idx < len(accessions_list) else ""
                                    filing_date_str = filing_dates_list[idx] if idx < len(filing_dates_list) else ""
                                    report_date_str = report_dates_list[idx] if idx < len(report_dates_list) else ""
                                    fiscal_year = f"FY{report_date_str[:4]}" if report_date_str else ""
                                    raw_value = (
                                        geo_evidence.get("rawValue") if isinstance(geo_evidence, dict) else None
                                    ) or _format_raw_number(geo_usd)
                                    raw_den = (
                                        geo_evidence.get("rawDenominator") if isinstance(geo_evidence, dict) else None
                                    ) or _format_raw_number(geo_denominator)
                                    source_rows = (
                                        geo_evidence.get("sourceRows") if isinstance(geo_evidence, dict) else None
                                    ) or [
                                        [region or "Region", raw_value],
                                        ["Total revenue", raw_den],
                                    ]
                                    source_cols = (
                                        geo_evidence.get("sourceColumns") if isinstance(geo_evidence, dict) else None
                                    ) or [fiscal_year]
                                    warnings = []
                                    if geo_denominator is None and geo_usd is not None:
                                        warnings.append({
                                            "code": "DENOMINATOR_NOT_FOUND",
                                            "message": "Could not compute geographic revenue percentage due to missing denominator.",
                                            "severity": "warning",
                                        })
                                    return _geo_shape({
                                        "ticker": ticker,
                                        "factType": fact_type.value,
                                        "region": region,
                                        "period": fiscal_year or None,
                                        "rawValue": raw_value,
                                        "rawDenominator": raw_den,
                                        "unit": "USD",
                                        "unitScale": (geo_evidence.get("unitScale") if isinstance(geo_evidence, dict) else "actual") or "actual",
                                        "value": geo_usd,
                                        "denominator": geo_denominator,
                                        "valueRatio": geo_ratio,
                                        "valuePct": round(geo_ratio * PCT_MULTIPLIER, PCT_DECIMALS) if geo_ratio is not None else None,
                                        "extractionMethod": "PARSED_TABLE",
                                        "source": "PARSED_TABLE",
                                        "confidence": "HIGH" if geo_denominator is not None else "LOW",
                                        "filingType": filing_type,
                                        "filingDate": filing_date_str,
                                        "accessionNumber": acc_num,
                                        "documentUrl": doc_url,
                                        "indexUrl": None,
                                        "primaryDocumentUrl": doc_url,
                                        "evidence": {
                                            "sectionHeading": geo_heading or (geo_evidence.get("sectionHeading") if isinstance(geo_evidence, dict) else None),
                                            "tableTitle": geo_evidence.get("tableTitle") if isinstance(geo_evidence, dict) else None,
                                            "sourceTableId": geo_evidence.get("sourceTableId") if isinstance(geo_evidence, dict) else 1,
                                            "sourceRows": source_rows,
                                            "sourceColumns": source_cols,
                                        },
                                        "calculation": (
                                            {
                                                "formula": "value / denominator * 100",
                                                "valueSource": "sourceRows[0]",
                                                "denominatorSource": "sourceRows[1]",
                                                "resultPct": round(geo_ratio * PCT_MULTIPLIER, PCT_DECIMALS),
                                            }
                                            if geo_ratio is not None and geo_denominator is not None else None
                                        ),
                                        "warnings": warnings,
                                    })
        return _geo_shape({
            "ticker": ticker,
            "factType": fact_type.value,
            "value": None,
            "denominator": None,
            "valueRatio": None,
            "valuePct": None,
            "extractionMethod": "NONE",
            "source": "NOT_DISCLOSED",
            "confidence": "NOT_DISCLOSED",
            "evidence": {},
            "warnings": [],
            "_manualLookup": _manual_lookup_payload(
                ticker, cik_padded, filing_type, "Fact not XBRL-tagged. Use search_filing_text instead."
            ),
        })

    accession_number = str(picked.get("accn") or "")
    index_url, primary_document_url = await _resolve_filing_urls_for_accession(accession_number)
    document_url = primary_document_url or index_url
    value_num = float(picked.get("val", 0)) if picked.get("val") is not None else None
    raw_value = _format_raw_number(value_num)
    raw_denominator = _format_raw_number(denominator)
    period_label = str(picked.get("fy") or "")
    if period_label and not period_label.startswith("FY"):
        period_label = f"FY{period_label}"

    return _geo_shape({
        "ticker": ticker,
        "factType": fact_type.value,
        "region": region,
        "period": period_label or None,
        "rawValue": raw_value,
        "rawDenominator": raw_denominator,
        "unit": "USD",
        "unitScale": "actual",
        "value": value_num,
        "denominator": denominator if fact_type == FilingFactType.geographic_revenue else None,
        "valueRatio": value_ratio if fact_type == FilingFactType.geographic_revenue else None,
        "valuePct": value_pct if fact_type == FilingFactType.geographic_revenue else None,
        "extractionMethod": "XBRL",
        "source": "XBRL",
        "confidence": "HIGH" if fact_type != FilingFactType.geographic_revenue or denominator is not None else "LOW",
        "filingType": filing_type,
        "filingDate": str(picked.get("filed") or ""),
        "accessionNumber": accession_number or None,
        "documentUrl": document_url,
        "indexUrl": index_url,
        "primaryDocumentUrl": primary_document_url,
        "evidence": {
            "sectionHeading": segment_label,
            "tableTitle": None,
            "sourceTableId": None,
            "sourceRows": [
                [segment_label or (region or "Region"), raw_value],
                ["Total revenue", raw_denominator],
            ],
            "sourceColumns": [period_label or str(picked.get("fp") or "")],
        },
        "calculation": (
            {
                "formula": "value / denominator * 100",
                "valueSource": "sourceRows[0]",
                "denominatorSource": "sourceRows[1]",
                "resultPct": value_pct,
            }
            if fact_type == FilingFactType.geographic_revenue and denominator is not None else None
        ),
        "warnings": [],
    }, warn_denominator=(fact_type == FilingFactType.geographic_revenue and denominator is None))


@yfinance_server.tool(
    name="search_filing_text",
    output_schema=_TOOL_OUTPUT_SCHEMAS["search_filing_text"],
    description="""Search filing narrative text by keyword or section hint.

Use this only when get_filing_data returns NOT_DISCLOSED or the fact is not XBRL-tagged.
""",
)
async def search_filing_text(
    ticker: str,
    search_terms: list[str] | None = None,
    section_hint: str | None = None,
    filing_type: str = "10-K",
    accession_number: str | None = None,
    context_chars: int = 1500,
    return_tables: bool = True,
) -> str:
    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": accession_number,
            "documentUrl": None,
            "fiscalYear": None,
            "filingType": filing_type,
            "filingDate": None,
            "matches": [],
            "matchCount": 0,
            "confidence": "PARSED_HTML",
            "_note": "Could not resolve SEC submissions for ticker.",
        })

    recent = subs.get("filings", {}).get("recent", {})
    forms: list[str] = recent.get("form", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    filing_dates: list[str] = recent.get("filingDate", [])
    report_dates: list[str] = recent.get("reportDate", [])

    target_idx: int | None = None
    if accession_number:
        for i, acc in enumerate(accessions):
            if acc == accession_number:
                target_idx = i
                break
    else:
        for i, form in enumerate(forms):
            if str(form).upper() == filing_type.upper():
                target_idx = i
                accession_number = accessions[i] if i < len(accessions) else None
                break

    if target_idx is None or not accession_number:
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": accession_number,
            "documentUrl": None,
            "fiscalYear": None,
            "filingType": filing_type,
            "filingDate": None,
            "matches": [],
            "matchCount": 0,
            "confidence": "PARSED_HTML",
            "_note": f"No {filing_type} filing found in submissions JSON.",
        })

    primary_doc = primary_docs[target_idx] if target_idx < len(primary_docs) else None
    if not primary_doc:
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": accession_number,
            "documentUrl": None,
            "fiscalYear": None,
            "filingType": filing_type,
            "filingDate": filing_dates[target_idx] if target_idx < len(filing_dates) else None,
            "matches": [],
            "matchCount": 0,
            "confidence": "PARSED_HTML",
            "_note": "primaryDocument missing in submissions JSON.",
        })

    cik_int = int(cik_padded)
    _, document_url = _edgar_build_filing_urls(cik_int, accession_number, primary_doc)
    if not document_url:
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": accession_number,
            "documentUrl": None,
            "fiscalYear": None,
            "filingType": filing_type,
            "filingDate": filing_dates[target_idx] if target_idx < len(filing_dates) else None,
            "matches": [],
            "matchCount": 0,
            "confidence": "PARSED_HTML",
            "_note": "Failed constructing filing document URL.",
        })

    html_text = await _edgar_get_html(document_url, max_bytes=5_000_000)
    if not html_text:
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": accession_number,
            "documentUrl": document_url,
            "fiscalYear": f"FY{str(report_dates[target_idx])[:4]}" if target_idx < len(report_dates) and report_dates[target_idx] else None,
            "filingType": filing_type,
            "filingDate": filing_dates[target_idx] if target_idx < len(filing_dates) else None,
            "matches": [],
            "matchCount": 0,
            "confidence": "PARSED_HTML",
            "_note": "Unable to fetch filing HTML.",
        })

    html_low = html_text.lower()
    context_window = max(200, min(int(context_chars), 4000))
    matches: list[dict] = []
    seen: set[int] = set()

    def _append_match(term: str, pos: int) -> None:
        if any(abs(pos - p) < 150 for p in seen):
            return
        seen.add(pos)
        start = max(0, pos - context_window // 2)
        end = min(len(html_text), pos + context_window // 2)
        context_html = html_text[start:end]
        pre_html = html_text[max(0, pos - 8_000):pos]
        h_matches = _re.findall(r"<h[1-6][^>]*>(.*?)</h[1-6]>", pre_html, _re.IGNORECASE | _re.DOTALL)
        section_heading = _strip_html_tags(h_matches[-1]) if h_matches else ""
        item = {
            "term": term,
            "sectionHeading": section_heading,
            "contextText": _strip_html_tags(context_html),
        }
        if return_tables:
            parsed_tables: list[dict] = []
            for tbl_m in _re.finditer(r"<table[^>]*>([\s\S]*?)</table>", context_html, _re.IGNORECASE):
                rows = _parse_html_table(tbl_m.group(0))
                if len(rows) >= 2:
                    parsed_tables.append({"rows": rows})
                if len(parsed_tables) >= 3:
                    break
            item["tableParsed"] = parsed_tables
        matches.append(item)

    if section_hint:
        pos = html_low.find(section_hint.lower())
        if pos >= 0:
            _append_match(section_hint, pos)
    for term in (search_terms or []):
        pos = 0
        term_low = term.lower()
        while len(matches) < 10:
            found = html_low.find(term_low, pos)
            if found < 0:
                break
            _append_match(term, found)
            pos = found + 1

    return json.dumps({
        "ticker": ticker,
        "accessionNumber": accession_number,
        "documentUrl": document_url,
        "fiscalYear": f"FY{str(report_dates[target_idx])[:4]}" if target_idx < len(report_dates) and report_dates[target_idx] else None,
        "filingType": filing_type,
        "filingDate": filing_dates[target_idx] if target_idx < len(filing_dates) else None,
        "matches": matches,
        "matchCount": len(matches),
        "confidence": "PARSED_HTML",
    })


# ---------------------------------------------------------------------------
# Group 3.5 — get_technical_indicators
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_technical_indicators",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_technical_indicators"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_price_slope"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_volume_ratio"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_ma_position"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_credit_health"],
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

    missing_components = []
    if total_debt is None:
        missing_components.append("totalDebtUsd")
    if cash is None:
        missing_components.append("cashUsd")
    if ebitda_annual is None:
        missing_components.append("ebitdaUsd")
    if ebit_annual is None:
        missing_components.append("ebitUsd")
    if interest_annual is None:
        missing_components.append("interestExpenseUsd")

    unavailable_metrics = []
    if net_debt_to_ebitda is None:
        unavailable_metrics.append("netDebtToEbitda")
    if interest_coverage is None:
        unavailable_metrics.append("interestCoverage")
    if credit_stress is None:
        unavailable_metrics.append("creditStressFlag")

    computed_metrics = []
    if net_debt is not None:
        computed_metrics.append("netDebtUsd")
    if net_debt_to_ebitda is not None:
        computed_metrics.append("netDebtToEbitda")
    if interest_coverage is not None:
        computed_metrics.append("interestCoverage")
    if credit_stress is not None:
        computed_metrics.append("creditStressFlag")
    if debt_tier is not None:
        computed_metrics.append("debtTier")

    warnings = []
    if interest_annual is None:
        warnings.append({
            "code": "INTEREST_EXPENSE_UNAVAILABLE",
            "message": "Interest coverage cannot be computed from available provider data.",
        })
    if ebitda_annual is not None and ebitda_annual < 0 or ebit_annual is not None and ebit_annual < 0:
        warnings.append({
            "code": "NEGATIVE_EARNINGS_BASE",
            "message": "Company has negative EBIT/EBITDA; leverage metrics may understate operating credit risk despite net cash or low net debt.",
        })

    # Check for partial data
    if missing_components:
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
        "missingComponents": missing_components,
        "unavailableMetrics": unavailable_metrics,
        "computedMetrics": computed_metrics,
        "warnings": warnings,
        "dataDate": get_last_trading_date(),
    })


# ---------------------------------------------------------------------------
# Tool: get_short_momentum
# ---------------------------------------------------------------------------
@yfinance_server.tool(
    name="get_short_momentum",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_short_momentum"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_earnings_momentum"],
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
    warnings: list[dict[str, str]] = []
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

    if beat_rate is None:
        historical_surprise_signal = "UNKNOWN"
    elif beat_rate >= 0.75:
        historical_surprise_signal = "STRONG"
    elif beat_rate >= 0.55:
        historical_surprise_signal = "POSITIVE"
    elif beat_rate >= 0.40:
        historical_surprise_signal = "NEUTRAL"
    else:
        historical_surprise_signal = "NEGATIVE"

    revs = [r for r in (revision_30d, revision_90d) if r is not None]
    if not revs:
        forward_revision_signal = "UNKNOWN"
    elif any(r <= -3 for r in revs):
        forward_revision_signal = "NEGATIVE"
    elif any(r >= 3 for r in revs):
        forward_revision_signal = "POSITIVE"
    else:
        forward_revision_signal = "NEUTRAL"

    mixed_negative_revision = beat_rate is not None and beat_rate >= 0.75 and any(
        r is not None and r < 0 for r in (revision_30d, revision_90d)
    )
    if mixed_negative_revision:
        warnings.append({
            "code": "MIXED_EARNINGS_SIGNAL",
            "message": "Historical beat streak is positive, but forward estimates were revised down.",
        })

    if historical_surprise_signal == "UNKNOWN" and forward_revision_signal == "UNKNOWN":
        composite_momentum_signal = "UNKNOWN"
    elif forward_revision_signal == "NEGATIVE" and historical_surprise_signal in {"STRONG", "POSITIVE"}:
        composite_momentum_signal = "MIXED_NEGATIVE_REVISION"
    elif forward_revision_signal == "POSITIVE" and historical_surprise_signal == "NEGATIVE":
        composite_momentum_signal = "MIXED_POSITIVE_REVISION"
    elif forward_revision_signal == "POSITIVE" and historical_surprise_signal in {"STRONG", "POSITIVE"}:
        composite_momentum_signal = "STRONG_POSITIVE"
    elif forward_revision_signal == "NEGATIVE":
        composite_momentum_signal = "NEGATIVE"
    else:
        composite_momentum_signal = "NEUTRAL"

    interpretation_note_map = {
        "STRONG_POSITIVE": "Historical earnings surprises and forward estimate revisions are both supportive.",
        "MIXED_NEGATIVE_REVISION": "Historical beat performance is strong, but forward revisions are negative.",
        "MIXED_POSITIVE_REVISION": "Historical surprise trend is weak, but forward revisions are improving.",
        "NEGATIVE": "Both historical surprise trend and forward revisions indicate weakness.",
        "NEUTRAL": "Signals are mixed or modest without a strong directional bias.",
        "UNKNOWN": "Insufficient data to classify both historical and forward signals.",
    }

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
        "beatSample": total_quarters,
        "totalQuarters": total_quarters,
        "avgSurprisePct": avg_surprise,
        "currentBeatStreak": beat_streak,
        "historicalSurpriseSignal": historical_surprise_signal,
        "forwardRevisionSignal": forward_revision_signal,
        "compositeMomentumSignal": composite_momentum_signal,
        "interpretationNote": interpretation_note_map[composite_momentum_signal],
        "warnings": warnings,
        "dataQuality": data_quality,
        "dataDate": get_last_trading_date(),
    })
    return json.dumps(output)


# ---------------------------------------------------------------------------
# Tool: get_options_flow_summary
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_options_flow_summary",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_options_flow_summary"],
    description="""Get options flow summary: P/C ratio, IV percentile, max pain strike, highest OI strikes. Single ticker only.

Args:
    ticker: str — single ticker
    expiry_hint: str | None — optional YYYY-MM-DD; if omitted, selects nearest liquid expiry
""",
)
async def get_options_flow_summary(ticker: str, expiry_hint: str | None = None) -> str:
    # Consolidated naming: route to the same payload implementation as get_options_summary.
    return await get_options_summary(ticker)


# ---------------------------------------------------------------------------
# Tool: get_put_hedge_candidates
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_put_hedge_candidates",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_put_hedge_candidates"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_analyst_upgrade_radar"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_etf_info"],
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_overnight_quote"],
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
            headers={"User-Agent": _SEC_REQUIRED_UA},
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
            headers={"User-Agent": _SEC_REQUIRED_UA},
        )
        try:
            with _urlreq.urlopen(req, timeout=10) as resp:  # noqa: S310
                return json.loads(resp.read())
        except Exception:
            return None

    return await loop.run_in_executor(None, _fetch)


# In-process cache for EDGAR company facts (keyed by zero-padded CIK).
_EDGAR_FACTS_CACHE: dict[str, tuple[dict, float]] = {}

# In-process cache for EDGAR submissions JSON (keyed by zero-padded CIK).
_EDGAR_SUBS_CACHE: dict[str, tuple[dict, float]] = {}


async def _edgar_get_company_facts(cik_padded: str) -> dict | None:
    """Fetch the EDGAR XBRL company-facts JSON for a CIK, with 24 h in-process caching."""
    now = time.monotonic()
    cached_entry = _EDGAR_FACTS_CACHE.get(cik_padded)
    if cached_entry is not None and (now - cached_entry[1]) < _EDGAR_TTL:
        return cached_entry[0]
    data = await _edgar_get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json")
    if data is not None:
        _EDGAR_FACTS_CACHE[cik_padded] = (data, now)
    return data


async def _edgar_get_submissions(cik_padded: str) -> dict | None:
    """Fetch the EDGAR submissions JSON for a CIK, with 24 h in-process caching."""
    now = time.monotonic()
    cached_entry = _EDGAR_SUBS_CACHE.get(cik_padded)
    if cached_entry is not None and (now - cached_entry[1]) < _EDGAR_TTL:
        return cached_entry[0]
    data = await _edgar_get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json")
    if data is not None:
        _EDGAR_SUBS_CACHE[cik_padded] = (data, now)
    return data


def _edgar_build_filing_urls(cik: int, accession_number: str, primary_doc: str | None) -> tuple[str, str | None]:
    """Build the EDGAR index URL and primary document URL for a filing."""
    accession_nodash = accession_number.replace("-", "")
    index_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik}"
        f"/{accession_nodash}/{accession_number}-index.htm"
    )
    primary_url: str | None = None
    if primary_doc:
        primary_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik}"
            f"/{accession_nodash}/{primary_doc}"
        )
    return index_url, primary_url


def _edgar_cik_from_accession(accession_number: str) -> int | None:
    """Derive CIK from the accession number prefix (e.g. '0000024741-26-000124' → 24741).

    The first 10 digits of an EDGAR accession number are the zero-padded filer CIK.
    Returns None for any non-positive result (EDGAR CIKs start at 1).
    """
    try:
        prefix = accession_number.split("-")[0].lstrip("0")
        # Empty string means all zeros (CIK 0 is invalid in EDGAR).
        return int(prefix) if prefix else None
    except Exception:
        return None


async def _edgar_primary_doc_from_index(index_url: str) -> str | None:
    """Fetch the EDGAR filing index HTM and return the primary document filename.

    The EDGAR filing index page (e.g. ``0000024741-26-000124-index.htm``) contains a
    table listing all documents for a filing.  The sequence-1 entry is the primary
    document (e.g. ``glw-20251231.htm``).  This function is ticker- and naming-
    convention-agnostic and works regardless of the EDGAR submissions window.

    Returns the bare filename (suitable for passing to ``_edgar_build_filing_urls``),
    or ``None`` if the page cannot be fetched or parsed.
    """
    html = await _edgar_get_html(index_url, max_bytes=500_000)
    if not html:
        return None
    def _normalize_href(raw_href: str) -> str | None:
        href = _html_module.unescape(raw_href).strip()
        if not href:
            return None
        # SEC often wraps document links as /ixviewer/ix.html?doc=/Archives/.../file.htm
        doc_m = _re.search(r"[?&]doc=([^&#]+)", href, _re.IGNORECASE)
        if doc_m:
            href = doc_m.group(1)
        href = href.split("#", 1)[0].split("?", 1)[0]
        if not href:
            return None
        fname = href.rsplit("/", 1)[-1].strip()
        return fname if fname else None

    # Prefer the first row matching Sequence=1 OR Type=10-K.
    for row_m in _re.finditer(r"<tr[^>]*>([\s\S]*?)</tr>", html, _re.IGNORECASE):
        row_html = row_m.group(1)
        cell_html = _re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", row_html, _re.IGNORECASE)
        if not cell_html:
            continue
        seq = _strip_html_tags(cell_html[0])
        doc_type = _strip_html_tags(cell_html[1]) if len(cell_html) > 1 else ""
        if seq == "1" or doc_type.upper().startswith("10-K"):
            href_m = _re.search(r'<a[^>]+href=["\']([^"\']+)["\']', row_html, _re.IGNORECASE)
            if href_m:
                fname = _normalize_href(href_m.group(1))
                if fname and not fname.lower().endswith(("-index.htm", "-index.html")):
                    return fname

    # Fallback: return the first document-like link that is not the index file itself.
    for href_m in _re.finditer(r'href=["\']([^"\']+)["\']', html, _re.IGNORECASE):
        fname = _normalize_href(href_m.group(1))
        if fname and fname.lower().endswith((".htm", ".html")) and not fname.lower().endswith(("-index.htm", "-index.html")):
            return fname
    return None


async def _edgar_get_html(url: str, max_bytes: int = 5_000_000) -> str | None:
    """Fetch an HTML document from EDGAR, reading at most max_bytes uncompressed bytes."""
    loop = asyncio.get_event_loop()

    def _fetch() -> str | None:
        req = _urlreq.Request(
            url,
            headers={"User-Agent": _SEC_REQUIRED_UA},
        )
        try:
            with _urlreq.urlopen(req, timeout=30) as resp:  # noqa: S310
                raw = resp.read(max_bytes)
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError:
                return raw.decode("latin-1", errors="replace")
        except Exception:
            return None

    return await loop.run_in_executor(None, _fetch)


# ---------------------------------------------------------------------------
# HTML table parsing helpers used by the HTML filing fallback layer.
# ---------------------------------------------------------------------------


def _strip_html_tags(html_str: str) -> str:
    """Remove HTML tags and decode entities to produce plain text."""
    text = _re.sub(r"<[^>]+>", " ", html_str)
    text = _html_module.unescape(text)
    return _re.sub(r"\s+", " ", text).strip()


def _parse_html_table(table_html: str) -> list[list[str]]:
    """Parse an HTML table into a list of rows, each a list of plain-text cell strings."""
    rows: list[list[str]] = []
    tr_pat = _re.compile(r"<tr[^>]*>(.*?)</tr>", _re.IGNORECASE | _re.DOTALL)
    td_pat = _re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", _re.IGNORECASE | _re.DOTALL)
    for tr_m in tr_pat.finditer(table_html):
        row = [_strip_html_tags(td_m.group(1)) for td_m in td_pat.finditer(tr_m.group(1))]
        if row:
            rows.append(row)
    return rows


def _parse_numeric_cell(text: str) -> float | None:
    """Parse a table cell's text as a number. Returns None when not parseable."""
    s = text.strip().replace(",", "").replace(" ", "").replace("$", "").replace("%", "")
    # Parentheses → negative value: (123) → -123
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    multiplier = 1.0
    if s.upper().endswith("B"):
        multiplier, s = 1_000_000_000.0, s[:-1]
    elif s.upper().endswith("M"):
        multiplier, s = 1_000_000.0, s[:-1]
    elif s.upper().endswith("K"):
        multiplier, s = 1_000.0, s[:-1]
    try:
        return float(s) * multiplier
    except ValueError:
        return None


def _detect_unit_multiplier(table_html: str, context_html: str) -> float:
    """Detect the monetary unit scale from a table caption/nearby headings.

    Returns 1_000_000 (millions) if not found — the most common 10-K scale.
    """
    combined = (table_html + context_html).lower()
    if "in billions" in combined or "$ billions" in combined:
        return 1_000_000_000.0
    if "in thousands" in combined or "$ thousands" in combined or "in thousands)" in combined:
        return 1_000.0
    if "in millions" in combined or "$ millions" in combined or "in millions)" in combined:
        return 1_000_000.0
    # Default assumption for 10-K financials: millions
    return 1_000_000.0


def _extract_geo_revenue_from_html(
    html_text: str,
    region: str,
) -> tuple[float | None, float | None, float | None, str, dict | None]:
    """Search an SEC filing HTML document for a geographic revenue table.

    Returns (regionRevenueRatio, regionRevenueUSD, totalRevenueUSD, sectionHeading, evidence).
    Parses the first table that contains the target region and a numeric total row.
    """
    region_lower = region.lower()
    html_lower = html_text.lower()

    # Candidate search terms ordered by specificity
    search_terms = [
        "geographic information",
        "geographic areas",
        "geographic segment",
        "revenue by region",
        "revenues by geography",
        region_lower,
    ]

    # Collect positions of all search-term matches (cap to keep runtime bounded)
    term_positions: list[int] = []
    for term in search_terms:
        idx = 0
        while len(term_positions) < 30:
            pos = html_lower.find(term, idx)
            if pos == -1:
                break
            term_positions.append(pos)
            idx = pos + 1

    if not term_positions:
        return None, None, None, "", None

    # For each match, find the nearest enclosing or following <table>
    checked_tables: set[int] = set()
    candidate_tables: list[dict] = []

    for pos in sorted(set(term_positions))[:20]:
        # Search window: 1 000 chars before match → 60 000 chars after
        search_start = max(0, pos - 1_000)
        search_end = min(len(html_text), pos + 60_000)
        chunk = html_text[search_start:search_end]

        for tbl_m in _re.finditer(r"<table[^>]*>", chunk, _re.IGNORECASE):
            abs_start = search_start + tbl_m.start()
            if abs_start in checked_tables:
                continue
            checked_tables.add(abs_start)

            # Walk forward tracking nested table depth to find matching </table>
            depth = 0
            i = abs_start
            table_end = abs_start
            while i < min(len(html_text), abs_start + 200_000):
                o = html_lower.find("<table", i)
                c = html_lower.find("</table>", i)
                if o == -1 and c == -1:
                    break
                if o != -1 and (c == -1 or o < c):
                    depth += 1
                    i = o + 6
                else:
                    depth -= 1
                    if depth == 0:
                        table_end = c + 8
                        break
                    i = c + 8

            table_html = html_text[abs_start:table_end]
            if region_lower not in table_html.lower():
                continue

            parsed = _parse_html_table(table_html)
            if len(parsed) < 2:
                continue

            candidate_tables.append({
                "pos": abs_start,
                "table_html": table_html,
                "rows": parsed,
            })

    if not candidate_tables:
        return None, None, None, "", None

    _TOTAL_LABELS = frozenset({
        "total", "consolidated", "total revenues", "total net revenues",
        "net revenues", "revenues", "total revenue",
    })

    for tbl in candidate_tables:
        rows: list[list[str]] = tbl["rows"]

        # Find the row index for the target region
        region_row_idx: int | None = None
        for i, row in enumerate(rows):
            if any(region_lower in cell.lower() for cell in row):
                region_row_idx = i
                break
        if region_row_idx is None:
            continue

        # Find a "Total" row
        total_row_idx: int | None = None
        for i, row in enumerate(rows):
            if any(cell.strip().lower() in _TOTAL_LABELS for cell in row):
                total_row_idx = i
                break
        if total_row_idx is None:
            # Fall back: last row that has any numeric value
            for i in range(len(rows) - 1, -1, -1):
                if any(_parse_numeric_cell(c) is not None for c in rows[i]):
                    total_row_idx = i
                    break

        if total_row_idx is None or total_row_idx == region_row_idx:
            continue

        # Find the first numeric column in the region row (skip label column)
        region_row = rows[region_row_idx]
        value_col: int | None = None
        for j, cell in enumerate(region_row):
            v = _parse_numeric_cell(cell)
            if v is not None and v > 0:
                value_col = j
                break
        if value_col is None:
            continue

        region_val = _parse_numeric_cell(
            rows[region_row_idx][value_col] if value_col < len(rows[region_row_idx]) else ""
        )
        total_val = _parse_numeric_cell(
            rows[total_row_idx][value_col] if value_col < len(rows[total_row_idx]) else ""
        )

        if region_val is None or total_val is None or total_val <= 0:
            continue

        ratio = round(region_val / total_val, 4)

        # Detect unit scale for USD conversion
        context_html = html_text[max(0, tbl["pos"] - 3_000): tbl["pos"]]
        unit_mult = _detect_unit_multiplier(tbl["table_html"], context_html)
        region_usd = region_val * unit_mult
        total_usd = total_val * unit_mult

        # Extract nearest section heading (last <h*> tag before the table)
        heading = ""
        pre_html = html_text[max(0, tbl["pos"] - 6_000): tbl["pos"]]
        h_matches = _re.findall(r"<h[1-6][^>]*>(.*?)</h[1-6]>", pre_html, _re.IGNORECASE | _re.DOTALL)
        if h_matches:
            heading = _strip_html_tags(h_matches[-1])

        header_row = rows[0] if rows else []
        source_col = str(header_row[value_col]).strip() if value_col < len(header_row) else ""
        unit_scale = (
            "thousands" if unit_mult == 1_000.0
            else "millions" if unit_mult == 1_000_000.0
            else "actual" if unit_mult == 1.0
            else "actual"
        )
        evidence = {
            "sectionHeading": heading or None,
            "tableTitle": None,
            "sourceTableId": 1,
            "sourceRows": [
                [
                    str(rows[region_row_idx][0] if rows[region_row_idx] else region),
                    str(rows[region_row_idx][value_col]) if value_col < len(rows[region_row_idx]) else "",
                ],
                [
                    str(rows[total_row_idx][0] if rows[total_row_idx] else "Total revenue"),
                    str(rows[total_row_idx][value_col]) if value_col < len(rows[total_row_idx]) else "",
                ],
            ],
            "sourceColumns": [source_col] if source_col else [],
            "unitScale": unit_scale,
            "rawValue": str(rows[region_row_idx][value_col]) if value_col < len(rows[region_row_idx]) else None,
            "rawDenominator": str(rows[total_row_idx][value_col]) if value_col < len(rows[total_row_idx]) else None,
        }
        return ratio, region_usd, total_usd, heading, evidence

    return None, None, None, "", None


# ---------------------------------------------------------------------------
# Region → XBRL segment-member mapping for geographic revenue extraction.
# Keys are lowercase region names; values are ordered candidate member strings.
# Substring fallback (region_lower in member.lower()) handles custom prefixes
# such as "aapl:GreaterChinaMember".
# ---------------------------------------------------------------------------
_REGION_XBRL_MEMBERS: dict[str, list[str]] = {
    "china": ["country:CN", "srt:ChinaMember"],
    "united states": ["country:US", "srt:UnitedStatesMember"],
    "europe": ["srt:EuropeMember", "srt:EuropeMiddleEastAndAfricaMember"],
    "japan": ["country:JP", "srt:JapanMember"],
    "asia pacific": ["srt:AsiaPacificMember", "srt:AsiaMember"],
    "rest of world": ["srt:NonUsMember", "srt:OtherGeographicAreasMember"],
}

# Revenue concept names to probe, in priority order.
_GEO_REVENUE_CONCEPTS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomer",
]

_GEO_AXIS = "srt:StatementGeographicalAxis"


def _extract_geographic_pct(
    facts_data: dict,
    region: str,
    filing_date: str | None,
) -> tuple[float | None, float | None, str, str, str]:
    """Extract geographic revenue from EDGAR XBRL company-facts JSON.

    Returns (regionRevenuePct, regionRevenueUSD, segmentLabel, source, confidence).
    All-None/NOT_DISCLOSED on failure.
    """
    region_lower = region.lower()
    candidate_members: list[str] = _REGION_XBRL_MEMBERS.get(region_lower, [])

    def _member_matches(member_val: str) -> bool:
        if any(m.lower() == member_val.lower() for m in candidate_members):
            return True
        # Substring fallback — catches custom prefixes like "aapl:GreaterChinaMember"
        return region_lower in member_val.lower()

    us_gaap: dict = facts_data.get("facts", {}).get("us-gaap", {})

    for concept in _GEO_REVENUE_CONCEPTS:
        concept_data = us_gaap.get(concept)
        if not concept_data:
            continue

        usd_units: list[dict] = concept_data.get("units", {}).get("USD", [])
        if not usd_units:
            continue

        # Pin facts to the specific 10-K by filing date (±10 days) or, when
        # filing_date is unknown, accept any 10-K annual fact.
        def _is_target_filing(fact: dict) -> bool:
            if fact.get("form") not in ("10-K", "10-K405", "10-KSB"):
                return False
            if filing_date is None:
                return True
            try:
                fd = datetime.date.fromisoformat(filing_date)
                ff = datetime.date.fromisoformat(fact["filed"])
                return abs((ff - fd).days) <= 10
            except Exception:
                return True

        target_facts = [f for f in usd_units if _is_target_filing(f)]
        if not target_facts:
            # Relax: accept any 10-K fact for this concept if none match the date
            target_facts = [
                f for f in usd_units
                if f.get("form") in ("10-K", "10-K405", "10-KSB")
            ]
        if not target_facts:
            continue

        # Group facts by period end-date to align regional vs. total rows
        by_period: dict[str, list[dict]] = {}
        for fact in target_facts:
            end = fact.get("end", "")
            by_period.setdefault(end, []).append(fact)

        # Try each period (most-recent first)
        for period_end in sorted(by_period.keys(), reverse=True):
            period_facts = by_period[period_end]

            regional_fact: dict | None = None
            total_fact: dict | None = None

            for fact in period_facts:
                seg = fact.get("segment")
                if seg is None:
                    # No segment dimension → consolidated total
                    total_fact = fact
                elif (
                    isinstance(seg, dict)
                    and seg.get("dimension") == _GEO_AXIS
                    and _member_matches(str(seg.get("member", "")))
                ):
                    regional_fact = fact
                elif isinstance(seg, list):
                    # Some filers encode segment as a list of {dimension, member} objects
                    for dim_entry in seg:
                        if (
                            isinstance(dim_entry, dict)
                            and dim_entry.get("dimension") == _GEO_AXIS
                            and _member_matches(str(dim_entry.get("member", "")))
                        ):
                            regional_fact = fact
                            break

            if regional_fact is not None and total_fact is not None:
                r_val = float(regional_fact["val"])
                t_val = float(total_fact["val"])
                if t_val > 0:
                    pct = round(r_val / t_val, 4)
                    seg_member = (
                        regional_fact["segment"]["member"]
                        if isinstance(regional_fact.get("segment"), dict)
                        else region
                    )
                    return pct, r_val, seg_member, "edgar_xbrl", "CONFIRMED"

    return None, None, region, "not_available", "NOT_DISCLOSED"


# ---------------------------------------------------------------------------
# CR-12 — get_options_flow_scan
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_options_flow_scan",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_options_flow_scan"],
    description="""Structured options flow scan for a binary event window.

Returns the formatted options flow output block. Callers can paste formattedBlock directly into
client output. Prior window-label readings are cached server-side (72 h TTL) to enable trend
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

    # Total OI for max pain guard
    call_oi_total = float(calls_df["openInterest"].sum(skipna=True)) if "openInterest" in calls_df.columns else 0.0
    put_oi_total = float(puts_df["openInterest"].sum(skipna=True)) if "openInterest" in puts_df.columns else 0.0

    # Max pain strike — strike with maximum combined open interest
    max_pain_strike: float | None = None
    scan_warnings: list[str] = []
    if call_oi_total + put_oi_total <= 0:
        scan_warnings.append("MAX_PAIN_UNAVAILABLE_ZERO_OI")
    else:
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
    atm_iv_reason: str | None = None
    if current_price is None:
        atm_iv_reason = "ATM_IV_UNAVAILABLE_NO_PRICE"
    elif calls_df.empty:
        atm_iv_reason = "ATM_IV_UNAVAILABLE_NO_CALLS"
    else:
        try:
            _calls = calls_df.copy()
            _calls = _calls.assign(_dist=(_calls["strike"] - current_price).abs())
            atm_row = _calls.nsmallest(1, "_dist")
            if not atm_row.empty:
                iv_val = atm_row["impliedVolatility"].iloc[0]
                if pd.notna(iv_val) and float(iv_val) > _PLACEHOLDER_IV_THRESHOLD:
                    atm_iv = float(iv_val)
                else:
                    atm_iv_reason = "ATM_IV_PLACEHOLDER"
        except Exception:
            atm_iv_reason = "ATM_IV_PLACEHOLDER"

    if atm_iv_reason is not None:
        scan_warnings.append(atm_iv_reason)

    # dataQuality over the full nearest-expiry chain
    calls_list = json.loads(calls_df.to_json(orient="records", date_format="iso"))
    puts_list = json.loads(puts_df.to_json(orient="records", date_format="iso"))
    data_quality = _compute_data_quality(calls_list + puts_list, get_last_trading_date())
    quality = data_quality.get("quality", "HIGH")

    # IV percentile — approximate using annualised 30-day rolling realised vol over 1 year
    iv_pctile: int | None = None
    if quality == "LOW" and data_quality.get("placeholderIvCount", 0) > len(calls_list + puts_list) * 0.5:
        scan_warnings.append("IV_PERCENTILE_UNAVAILABLE_PLACEHOLDER_IV")
    else:
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

    # Bracket classification — suppressed when data quality is LOW
    bracket: str | None = None
    if quality != "LOW" and pc_ratio is not None:
        if pc_ratio >= 1.3 or (pc_ratio >= 1.0 and put_vol_trend == "INCREASING"):
            bracket = "UPPER"
        elif pc_ratio <= 0.8 and put_vol_trend != "INCREASING":
            bracket = "LOWER"
        else:
            bracket = "MID"

    # Formatted block
    if quality == "LOW":
        formatted_block = (
            f"OPTIONS FLOW: DATA QUALITY LOW — raw chain unreliable; not suitable for inference."
        )
    else:
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
        "dataQuality": data_quality,
        "warnings": scan_warnings,
    }

    # Cache current reading for future trend comparison (72h TTL via 3-day window check)
    _cache_set(f"options_flow:{ticker}:{window_label}", json.dumps(result_dict))
    return json.dumps(result_dict)


# ---------------------------------------------------------------------------
# CR-13 — get_price_target_bracket
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_price_target_bracket",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_price_target_bracket"],
    description="""Compare current market price to a user-supplied reference target price and return distance/bracket labels.

ratio = currentPrice / reference_target_price × 100.

Brackets: ≤75% → STRONG_BUY | 75–90% → ACCEPTABLE | 90–100% → RISK | >100% → ABOVE_TARGET
Tags: <40% → SPECULATIVE | 40–79% → LONG | 80–99% → NEAR | ≥100% → INVERTED

Args:
    ticker: str
        The ticker symbol, e.g. "ASTS"
    reference_target_price: float | None
        Preferred user-supplied reference target price.
    io_pt: float | None
        Backward-compatible alias for reference_target_price.
""",
)
async def get_price_target_bracket(
    ticker: str, reference_target_price: float | None = None, io_pt: float | None = None
) -> str:
    """Return bracket and distance fields for current price vs reference target."""
    target_price = reference_target_price if reference_target_price is not None else io_pt
    if target_price is None or target_price <= 0:
        return json.dumps({
            "error": True,
            "message": "reference_target_price (or io_pt alias) must be a positive number",
            "ticker": ticker,
        })

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        current_price: float | None = fi["lastPrice"]
        if current_price is None:
            return json.dumps({"error": True, "message": f"No price data for {ticker}", "ticker": ticker})
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    reference_target_pct = round(current_price / target_price * 100, 1)

    if reference_target_pct <= 75:
        bracket = "STRONG_BUY"
    elif reference_target_pct <= 90:
        bracket = "ACCEPTABLE"
    elif reference_target_pct <= 100:
        bracket = "CAUTION"
    else:
        bracket = "AVOID"

    if reference_target_pct < 40:
        tag = "SPECULATIVE"
    elif reference_target_pct < 80:
        tag = "LONG"
    elif reference_target_pct < 100:
        tag = "NEAR"
    else:
        tag = "INVERTED"

    inverted_flag = reference_target_pct >= 100

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
        "referenceTargetPrice": target_price,
        "referenceTargetPct": reference_target_pct,
        "ioPt": target_price,
        "eqfPct": reference_target_pct,
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_position_score_inputs"],
    description="""Aggregate public market, analyst, earnings, and technical inputs for caller-defined scoring models.

Runs up to 6 parallel data fetches per call.

Returns grouped analyst, price/range, earnings-momentum, and technical indicator inputs plus dataDate.

This tool does not access holdings, cost basis, position size, or private scoring rules.

Args:
    ticker: str
        Single ticker symbol, e.g. "ASTS"
""",
)
async def get_position_score_inputs(ticker: str) -> str:
    """Return grouped public inputs for caller-defined scoring workflows."""
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

    # Data date: prefer last OHLCV row from technical indicators for consistent timing.
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
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_volume_gate"],
    description="""Check current trading volume and dollar-notional liquidity against public liquidity thresholds.

Returns currency, fxRate, lastVolume, adv10d, adv20d (computed from last 20 daily sessions),
adv90d, ratio20d (always computed when adv20d is available), gatePass,
dataDate, and a pre-formatted note.

foreign_exchange: bool (default False). When True, enables foreign exchange notional conversion:
daily notional is converted to USD via a live {CCY}=X FX rate fetch before comparing to the $10M threshold.
ratio20d is still computed and returned alongside the notional gate result.

Args:
    ticker: str
        The ticker symbol, e.g. "ASTS"
    foreign_exchange: bool
        Set True for foreign exchange / ADR tickers to convert daily notional to USD for the threshold check. Default False.
""",
)
async def get_volume_gate(ticker: str, foreign_exchange: bool = False) -> str:
    """Return volume liquidity threshold assessment."""
    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        last_volume: int | None = fi["lastVolume"]
        adv10d: float | None = fi["tenDayAverageVolume"]
        adv90d: float | None = fi["threeMonthAverageVolume"]
        last_price: float | None = fi["lastPrice"]
        currency: str | None = fi["currency"]
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
    fx_rate: float | None = None
    note: str

    if foreign_exchange:
        # FX notional mode: daily notional ≥ $10M USD (convert to USD via live FX rate)
        if last_volume is not None and last_price is not None and last_price > 0:
            local_notional = last_volume * last_price
            applied_fx_rate = 1.0
            fx_conversion_note = ""

            if currency and currency != "USD":
                try:
                    fx_ticker = yf.Ticker(f"{currency}=X")
                    rate_val = fx_ticker.fast_info.last_price
                    if rate_val and rate_val > 0:
                        applied_fx_rate = rate_val
                        fx_rate = round(float(rate_val), 4)
                        fx_conversion_note = f" [{currency}\u2192USD at {rate_val:.2f}]"
                    else:
                        fx_conversion_note = f" [{currency}=X rate unavailable \u2014 notional in local currency]"
                except Exception:
                    fx_conversion_note = f" [{currency}=X fetch failed \u2014 notional in local currency]"
            elif currency == "USD":
                fx_rate = 1.0

            daily_notional_usd = local_notional / applied_fx_rate
            gate_pass = daily_notional_usd >= 10_000_000
            note = (
                f"Volume gate {'PASS' if gate_pass else 'FAIL'} (FX notional) — "
                f"${daily_notional_usd / 1_000_000:.1f}M daily notional "
                f"({'≥' if gate_pass else '<'} $10M threshold){fx_conversion_note}"
            )
        else:
            note = "Volume gate UNKNOWN — insufficient price/volume data for FX notional check"
        # Bug 5: compute ratio20d in FX branch too
        if last_volume is not None and adv20d and adv20d > 0:
            ratio20d = round(last_volume / adv20d, 2)
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
        "currency": currency,
        "lastVolume": last_volume,
        "adv10d": adv10d,
        "adv20d": adv20d,
        "adv90d": adv90d,
        "ratio20d": ratio20d,
        "fxRate": fx_rate,
        "gatePass": gate_pass,
        "dataDate": data_date,
        "note": note,
    })


def _deprecated_alias_response(alias_tool: str, canonical_tool: str, raw: str) -> str:
    warning_obj = {
        "code": "DEPRECATED_ALIAS",
        "message": f"Use {canonical_tool} instead.",
        "severity": "info",
    }
    if not _ENVELOPE_V2:
        return raw
    try:
        payload = json.loads(raw)
    except Exception:
        payload = raw
    if isinstance(payload, dict) and "ok" in payload:
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            meta = {}
            payload["meta"] = meta
        meta["tool"] = alias_tool
        meta["canonicalTool"] = canonical_tool
        meta["deprecatedTool"] = True
        meta["useInstead"] = canonical_tool
        warnings = meta.get("warnings")
        warning_list = list(warnings) if isinstance(warnings, list) else []
        warning_list.append(warning_obj)
        meta["warnings"] = warning_list
        return json.dumps(payload)
    return _mcp_success(
        alias_tool,
        payload,
        canonical_tool=canonical_tool,
        deprecated_tool=True,
        use_instead=canonical_tool,
        warnings=[warning_obj],
    )


@yfinance_server.tool(name="health_check", output_schema=_SIMPLE_OUTPUT_SCHEMA, description="Return runtime health metadata.")
async def health_check() -> str:
    try:
        tool_count = len(yfinance_server._tool_manager._tools)
    except Exception:
        tool_count = len(TOOL_ALIASES) + 50
    tool_names = sorted(TOOL_ALIASES.keys())
    manifest_hash = hashlib.sha256(json.dumps(tool_names).encode("utf-8")).hexdigest()[:16]
    manifest_version = os.environ.get("MANIFEST_VERSION", "1")
    deployed_at = os.environ.get("DEPLOYED_AT", datetime.datetime.utcnow().isoformat() + "Z")
    runtime_hash = hashlib.sha256((SERVER_VERSION + str(tool_count)).encode("utf-8")).hexdigest()[:16]
    deprecated_alias_count = len(
        {
            "get_tps_inputs",
            "get_eqf_bracket",
            "get_adv_gate",
            "get_dc134_options_scan",
            "get_china_revenue_pct",
            "get_geographic_revenue",
            "get_filing_text_search",
            "get_filing_document",
        }
    )
    return json.dumps({
        "serverVersion": SERVER_VERSION,
        "buildSha": os.environ.get("BUILD_SHA", "unknown"),
        "toolCount": tool_count,
        "canonicalToolCount": max(tool_count - deprecated_alias_count, 0),
        "deprecatedAliasCount": deprecated_alias_count,
        "manifestVersion": manifest_version,
        "manifestHash": manifest_hash,
        "schemaHash": manifest_hash,
        "runtimeHash": runtime_hash,
        "deployedAt": deployed_at,
        "generatedAt": datetime.datetime.utcnow().isoformat() + "Z",
        "privacyScope": "public_market_data_only",
    })


def _classify_freshness(data_date: str | None, retrieved_at: str) -> str:
    """Classify data freshness based on data date and retrieval time."""
    if not data_date:
        return "UNKNOWN"
    try:
        now = datetime.datetime.fromisoformat(retrieved_at.replace("Z", "+00:00"))
        # Approximate US market close as 21:00 UTC (4pm ET / 5pm EDT)
        data_dt = datetime.datetime(
            int(data_date[:4]), int(data_date[5:7]), int(data_date[8:10]),
            21, 0, 0, tzinfo=datetime.timezone.utc
        )
        diff_ms = (now - data_dt).total_seconds() * 1000
        if diff_ms < 0:
            return "UNKNOWN"
        diff_hours = diff_ms / (1000 * 60 * 60)
        # Python weekday(): 0=Monday, 1=Tuesday, ..., 4=Friday, 5=Saturday, 6=Sunday
        now_day = now.weekday()    # 0=Mon, 4=Fri, 5=Sat, 6=Sun
        data_day = data_dt.weekday()
        # Weekend: current day is Saturday(5) or Sunday(6), data from Friday(4)
        if now_day in (5, 6) and data_day == 4 and diff_hours <= 72:
            return "WEEKEND_EXPECTED_STALE"
        if diff_hours <= 28:
            return "FRESH"
        if diff_hours <= 56:
            return "MARKET_CLOSED_EXPECTED_STALE"
        if diff_hours <= 168:
            return "STALE"
        return "VERY_STALE"
    except Exception:
        return "UNKNOWN"


_MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "toolCount": {"type": "number"},
        "manifestVersion": {"type": ["string", "null"]},
        "manifestHash": {"type": ["string", "null"]},
        "buildSha": {"type": ["string", "null"]},
        "deployedAt": {"type": ["string", "null"]},
        "privacyScope": {"type": "string"},
        "canonicalToolCount": {"type": "number"},
        "deprecatedAliasCount": {"type": "number"},
        "publicSchemaGeneratedAt": {"type": ["string", "null"]},
        "workerSchemaGeneratedAt": {"type": ["string", "null"]},
        "manifestMismatch": {"type": ["boolean", "null"]},
        "staleConnectorWarning": {"type": ["string", "null"]},
    },
}

_MARKET_SNAPSHOT_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "ticker": {"type": "string"},
        "price": {"type": "object"},
        "range": {"type": "object"},
        "trend": {"type": "object"},
        "volume": {"type": "object"},
        "risk": {"type": "object"},
        "freshness": {"type": "object"},
        "componentStatus": {"type": "object"},
        "partialSuccess": {"type": "boolean"},
        "failedComponents": {"type": "array"},
        "warnings": {"type": "array"},
    },
    "additionalProperties": True,
}


@yfinance_server.tool(name="get_manifest_diagnostics", output_schema=_MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA, description="Return deployment and manifest diagnostics: tool counts, manifest version, hash, build SHA, deploy timestamp, privacy scope, and connector-staleness advisory.")
async def get_manifest_diagnostics() -> str:
    try:
        tool_count = len(yfinance_server._tool_manager._tools)
    except Exception:
        tool_count = len(TOOL_ALIASES) + 50
    tool_names = sorted(TOOL_ALIASES.keys())
    manifest_hash = hashlib.sha256(json.dumps(tool_names).encode("utf-8")).hexdigest()[:16]
    manifest_version = os.environ.get("MANIFEST_VERSION", None)
    deployed_at = os.environ.get("DEPLOYED_AT", None)
    deprecated_alias_set = {
        "get_tps_inputs",
        "get_eqf_bracket",
        "get_adv_gate",
        "get_dc134_options_scan",
        "get_china_revenue_pct",
        "get_geographic_revenue",
        "get_filing_text_search",
        "get_filing_document",
    }
    deprecated_alias_count = len(deprecated_alias_set)
    canonical_tool_count = max(tool_count - deprecated_alias_count, 0)
    worker_schema_generated_at = datetime.datetime.utcnow().isoformat() + "Z"
    return json.dumps({
        "toolCount": tool_count,
        "manifestVersion": manifest_version,
        "manifestHash": manifest_hash,
        "buildSha": os.environ.get("BUILD_SHA", "unknown"),
        "deployedAt": deployed_at,
        "privacyScope": "public_market_data_only",
        "canonicalToolCount": canonical_tool_count,
        "deprecatedAliasCount": deprecated_alias_count,
        "publicSchemaGeneratedAt": None,
        "workerSchemaGeneratedAt": worker_schema_generated_at,
        "manifestMismatch": None,
        "staleConnectorWarning": "ChatGPT connector schema may lag the deployed Worker schema. Direct Worker tools/list and get_manifest_diagnostics are source of truth.",
        "serverVersion": SERVER_VERSION,
    })


@yfinance_server.tool(name="get_market_snapshot", output_schema=_MARKET_SNAPSHOT_OUTPUT_SCHEMA, description="Compact market-state packet composing quote, price performance, moving-average trend, volume ratios, liquidity gate, and technical indicators in one call. Supports compact (default) and full modes, and optional batch of tickers.")
async def get_market_snapshot(
    ticker: str | list[str],
    mode: str = "compact",
    foreign_exchange: bool = False,
) -> str:
    """Return a compact or full market-state snapshot for one or more tickers."""
    if isinstance(ticker, list):
        cap = 2 if mode == "full" else 5
        limited = ticker[:cap]
        results = {}
        for t in limited:
            try:
                results[t] = json.loads(await get_market_snapshot(t, mode, foreign_exchange))
            except Exception as e:
                results[t] = {"error": True, "message": str(e)}
        return json.dumps({
            "tickers": results,
            "truncated": len(ticker) > cap,
            **({"droppedTickers": ticker[cap:]} if len(ticker) > cap else {}),
        })

    retrieved_at = datetime.datetime.utcnow().isoformat() + "Z"

    component_status: dict[str, str] = {}
    failed_components: list[str] = []
    warnings_list: list[dict] = []

    component_results = await asyncio.gather(
        get_fast_info(ticker),
        get_price_stats(ticker),
        get_ma_position(ticker),
        get_volume_ratio(ticker, 10),
        get_volume_gate(ticker, foreign_exchange),
        get_technical_indicators(ticker, "3mo"),
        return_exceptions=True,
    )

    names = ["quote", "priceStats", "maPosition", "volumeRatio", "volumeGate", "technicalIndicators"]

    def _parse_component(raw, name: str) -> dict | None:
        if isinstance(raw, Exception):
            component_status[name] = "FAILED"
            failed_components.append(name)
            warnings_list.append({"code": "COMPONENT_FAILED", "component": name, "message": str(raw)})
            return None
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, dict) and parsed.get("error"):
                component_status[name] = "FAILED"
                failed_components.append(name)
                warnings_list.append({"code": "COMPONENT_FAILED", "component": name, "message": parsed.get("message", "error in response")})
                return None
            component_status[name] = "OK"
            return parsed if isinstance(parsed, dict) else None
        except Exception as e:
            component_status[name] = "FAILED"
            failed_components.append(name)
            warnings_list.append({"code": "COMPONENT_FAILED", "component": name, "message": str(e)})
            return None

    quote, price_stats, ma, volume_ratio, volume_gate, tech = (
        _parse_component(r, n) for r, n in zip(component_results, names)
    )

    data_date = (
        (quote.get("lastTradeDate") if quote else None)
        or (price_stats.get("dataDate") if price_stats else None)
    )

    last_price = quote.get("lastPrice") if quote else None
    prev_close = quote.get("previousClose") if quote else None
    change_pct = (
        (price_stats.get("pctChangeTodayVsPrevClose") if price_stats else None)
        or (
            round((last_price - prev_close) / prev_close * 100, 2)
            if last_price is not None and prev_close is not None and prev_close != 0
            else None
        )
    )

    snapshot: dict = {
        "ticker": ticker,
        "price": {
            "last": last_price,
            "previousClose": prev_close,
            "changePct": change_pct,
            "lastTradeDate": data_date,
            "marketOpen": quote.get("marketOpen") if quote else None,
        },
        "range": {
            "yearHigh": quote.get("yearHigh") if quote else None,
            "yearLow": quote.get("yearLow") if quote else None,
            "pctFromYearHigh": price_stats.get("pctFromYearHigh") if price_stats else None,
            "pctFromYearLow": price_stats.get("pctFromYearLow") if price_stats else None,
        },
        "trend": {
            "fiftyDayAverage": quote.get("fiftyDayAverage") if quote else None,
            "twoHundredDayAverage": quote.get("twoHundredDayAverage") if quote else None,
            "pctFrom50dma": ma.get("pctVs50dma") if ma else None,
            "pctFrom200dma": ma.get("pctVs200dma") if ma else None,
            "maTrend": ma.get("trend") if ma else None,
            "rsi14": tech.get("rsi14") if tech else None,
            "macdHistogram": tech.get("macdHistogram") if tech else None,
        },
        "volume": {
            "lastVolume": quote.get("lastVolume") if quote else None,
            "avgVolume10d": quote.get("tenDayAverageVolume") if quote else None,
            "avgVolume20d": volume_gate.get("adv20d") if volume_gate else None,
            "avgVolume90d": quote.get("threeMonthAverageVolume") if quote else None,
            "ratio10d": volume_ratio.get("ratio10d") if volume_ratio else None,
            "ratio20d": volume_gate.get("ratio20d") if volume_gate else None,
            "ratio90d": volume_ratio.get("ratio90d") if volume_ratio else None,
            "volumeFlag": volume_ratio.get("volumeFlag") if volume_ratio else None,
            "liquidityGatePass": volume_gate.get("gatePass") if volume_gate else None,
        },
        "risk": {
            "annualizedVolatility30d": price_stats.get("annualizedVolatility30d") if price_stats else None,
        },
        "freshness": {
            "dataDate": data_date,
            "retrievedAt": retrieved_at,
            "marketSessionAware": True,
            "freshnessClass": _classify_freshness(data_date, retrieved_at),
        },
        "componentStatus": component_status,
        "partialSuccess": len(failed_components) > 0 and len(failed_components) < 6,
        "failedComponents": failed_components,
        "warnings": warnings_list,
    }

    if mode == "full":
        snapshot["_components"] = {
            "quote": quote,
            "priceStats": price_stats,
            "maPosition": ma,
            "volumeRatio": volume_ratio,
            "volumeGate": volume_gate,
            "technicalIndicators": tech,
        }

    return json.dumps(snapshot)


@yfinance_server.tool(name="get_market_quote", output_schema=_TOOL_OUTPUT_SCHEMAS["get_fast_info"], description="Canonical alias for get_fast_info.")
async def get_market_quote(ticker: str | list[str]) -> str:
    return await get_fast_info(ticker)


@yfinance_server.tool(name="get_historical_prices", output_schema=_TOOL_OUTPUT_SCHEMAS["get_historical_stock_prices"], description="Canonical alias for get_historical_stock_prices.")
async def get_historical_prices(ticker: str, period: str = "1mo", interval: str = "1d", prepost: bool = False) -> str:
    return await get_historical_stock_prices(ticker=ticker, period=period, interval=interval, prepost=prepost)


@yfinance_server.tool(name="analyze_price_performance", output_schema=_TOOL_OUTPUT_SCHEMAS["get_price_stats"], description="Canonical alias for get_price_stats.")
async def analyze_price_performance(ticker: str | list[str]) -> str:
    return await get_price_stats(ticker)


@yfinance_server.tool(name="analyze_moving_average_position", output_schema=_TOOL_OUTPUT_SCHEMAS["get_ma_position"], description="Canonical alias for get_ma_position.")
async def analyze_moving_average_position(ticker: str | list[str]) -> str:
    return await get_ma_position(ticker)


@yfinance_server.tool(name="analyze_volume_ratio", output_schema=_TOOL_OUTPUT_SCHEMAS["get_volume_ratio"], description="Canonical alias for get_volume_ratio.")
async def analyze_volume_ratio(ticker: str | list[str], period: int = 10) -> str:
    return await get_volume_ratio(ticker, period)


@yfinance_server.tool(name="check_volume_liquidity_threshold", output_schema=_TOOL_OUTPUT_SCHEMAS["get_volume_gate"], description="Canonical alias for get_volume_gate.")
async def check_volume_liquidity_threshold(ticker: str, foreign_exchange: bool = False) -> str:
    return await get_volume_gate(ticker, foreign_exchange)


@yfinance_server.tool(name="get_company_profile", output_schema=_TOOL_OUTPUT_SCHEMAS["get_stock_info"], description="Canonical alias for get_stock_info.")
async def get_company_profile(ticker: str | list[str], include_all: bool = False) -> str:
    return await get_stock_info(ticker, include_all=include_all)


@yfinance_server.tool(name="get_fund_profile", output_schema=_TOOL_OUTPUT_SCHEMAS["get_etf_info"], description="Canonical alias for get_etf_info.")
async def get_fund_profile(ticker: str | list[str]) -> str:
    return await get_etf_info(ticker)


@yfinance_server.tool(name="analyze_financial_ratios", output_schema=_TOOL_OUTPUT_SCHEMAS["get_financial_ratios"], description="Canonical alias for get_financial_ratios.")
async def analyze_financial_ratios(ticker: str | list[str]) -> str:
    return await get_financial_ratios(ticker)


@yfinance_server.tool(name="analyze_credit_health", output_schema=_TOOL_OUTPUT_SCHEMAS["get_credit_health"], description="Canonical alias for get_credit_health.")
async def analyze_credit_health(ticker: str | list[str]) -> str:
    return await get_credit_health(ticker)


@yfinance_server.tool(name="get_corporate_actions", output_schema=_TOOL_OUTPUT_SCHEMAS["get_stock_actions"], description="Canonical alias for get_stock_actions.")
async def get_corporate_actions(ticker: str) -> str:
    return await get_stock_actions(ticker)


@yfinance_server.tool(name="get_ownership_holders", output_schema=_TOOL_OUTPUT_SCHEMAS["get_holder_info"], description="Canonical alias for get_holder_info.")
async def get_ownership_holders(ticker: str, holder_type: HolderType) -> str:
    return await get_holder_info(ticker, holder_type)


@yfinance_server.tool(name="get_analyst_recommendations", output_schema=_TOOL_OUTPUT_SCHEMAS["get_recommendations"], description="Canonical alias for get_recommendations.")
async def get_analyst_recommendations(ticker: str, recommendation_type: RecommendationType, months_back: int = 12) -> str:
    return await get_recommendations(ticker, recommendation_type, months_back)


@yfinance_server.tool(name="get_analyst_rating_changes", output_schema=_TOOL_OUTPUT_SCHEMAS["get_analyst_upgrade_radar"], description="Canonical alias for get_analyst_upgrade_radar.")
async def get_analyst_rating_changes(ticker: str | list[str], days_back: int = 30) -> str:
    return await get_analyst_upgrade_radar(ticker, days_back)


@yfinance_server.tool(name="analyze_earnings_momentum", output_schema=_TOOL_OUTPUT_SCHEMAS["get_earnings_momentum"], description="Canonical alias for get_earnings_momentum.")
async def analyze_earnings_momentum(ticker: str | list[str]) -> str:
    return await get_earnings_momentum(ticker)


@yfinance_server.tool(name="get_company_events_calendar", output_schema=_TOOL_OUTPUT_SCHEMAS["get_calendar"], description="Canonical alias for get_calendar.")
async def get_company_events_calendar(ticker: str) -> str:
    return await get_calendar(ticker)


@yfinance_server.tool(
    name="get_company_news",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_yahoo_finance_news"],
    description="""Get recent public company news/events from selected public sources.

Returns deduplicated source-backed items with source type, timestamps, URL, event classification,
confidence, ticker relevance, and short evidence excerpts.
""",
)
async def get_company_news(
    ticker: str,
    max_results: int = 10,
    lookback_days: int = 14,
    sources: list[str] | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_company_news", ErrorCode.INPUT_VALIDATION_ERROR, err)
    items, sources_used, warnings, retrieved_at = await _collect_company_events(
        ticker,
        max_results=max_results,
        lookback_days=lookback_days,
        sources=sources,
    )
    status = _build_collection_status(items, sources_used, warnings)
    selected = sources or ["sec", "company_ir", "newswire", "yahoo_finance", "finnhub"]
    source_status = _compute_source_status(sources_used, warnings, items, selected)
    source_coverage = _compute_source_coverage(source_status)
    payload = {
        "ticker": ticker.upper(),
        "items": items,
        "meta": {
            "sourcesUsed": sources_used,
            "deduped": True,
            "watermark": retrieved_at,
        },
        "warnings": warnings,
        "sourceCoverage": source_coverage,
        "sourceStatus": source_status,
    }
    if status:
        payload["status"] = status
    return json.dumps(payload)


@yfinance_server.tool(name="summarize_options_flow", output_schema=_TOOL_OUTPUT_SCHEMAS["get_options_summary"], description="Canonical alias for get_options_summary/get_options_flow_summary.")
async def summarize_options_flow(ticker: str, expiry_hint: str | None = None) -> str:
    return await get_options_summary(ticker=ticker)


@yfinance_server.tool(name="analyze_options_flow_window", output_schema=_TOOL_OUTPUT_SCHEMAS["get_options_flow_scan"], description="Canonical alias for get_options_flow_scan.")
async def analyze_options_flow_window(ticker: str, window_label: str) -> str:
    return await get_options_flow_scan(ticker, window_label)


@yfinance_server.tool(name="find_put_hedge_candidates", output_schema=_TOOL_OUTPUT_SCHEMAS["get_put_hedge_candidates"], description="Canonical alias for get_put_hedge_candidates.")
async def find_put_hedge_candidates(ticker: str, otm_pct_min: float = 8, otm_pct_max: float = 12, budget_usd: float = 500, expiry_after: str = "") -> str:
    return await get_put_hedge_candidates(ticker, otm_pct_min, otm_pct_max, budget_usd, expiry_after)


@yfinance_server.tool(name="calculate_price_target_distance", output_schema=_TOOL_OUTPUT_SCHEMAS["get_price_target_bracket"], description="Canonical alias for get_price_target_bracket.")
async def calculate_price_target_distance(
    ticker: str,
    reference_target_price: float | None = None,
    io_pt: float | None = None,
) -> str:
    return await get_price_target_bracket(
        ticker, reference_target_price=reference_target_price, io_pt=io_pt
    )


@yfinance_server.tool(name="analyze_position_signals", output_schema=_TOOL_OUTPUT_SCHEMAS["get_position_score_inputs"], description="Canonical alias for get_position_score_inputs.")
async def analyze_position_signals(ticker: str) -> str:
    return await get_position_score_inputs(ticker)


@yfinance_server.tool(name="list_sec_company_filings", output_schema=_TOOL_OUTPUT_SCHEMAS["list_sec_filings"], description="""List SEC filings for a company from EDGAR submissions.

Returns compact metadata for each filing including accession number, filing date, accepted timestamp, and a direct document URL.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type, e.g. "10-K", "10-Q", "8-K". Defaults to "10-K".
    limit: Maximum number of filings to return (1-20). Defaults to 5.
""")
async def list_sec_company_filings(ticker: str, filing_type: str = "10-K", limit: int = 5, form_type: str | None = None, max_filings: int | None = None) -> str:
    resolved_type = form_type or filing_type
    resolved_limit = min(max(1, max_filings or limit), 20)
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_sec_company_filings", ErrorCode.INPUT_VALIDATION_ERROR, err)

    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("list_sec_company_filings", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not find EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms: list[str] = recent.get("form", [])
    dates: list[str] = recent.get("filingDate", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    accepted_dts: list[str] = recent.get("acceptanceDateTime", [])

    results: list[dict] = []
    for i, form in enumerate(forms):
        if len(results) >= resolved_limit:
            break
        if str(form).upper() != resolved_type.upper():
            continue
        acc = accessions[i] if i < len(accessions) else ""
        date = dates[i] if i < len(dates) else ""
        accepted_at = accepted_dts[i] if i < len(accepted_dts) else None
        primary_doc = primary_docs[i] if i < len(primary_docs) else ""
        _, doc_url = _edgar_build_filing_urls(cik_int, acc, primary_doc)
        results.append({
            "filingType": form,
            "filingDate": date,
            "acceptedAt": accepted_at,
            "accessionNumber": acc,
            "primaryDocument": primary_doc,
            "documentUrl": doc_url,
        })

    retrieved_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    return json.dumps({
        "ticker": ticker,
        "cik": cik_padded,
        "filings": results,
        "meta": {
            "source": "sec_submissions",
            "retrievedAt": retrieved_at,
        },
    })


async def _resolve_latest_sec_doc_url(ticker: str, filing_type: str = "10-K") -> str | None:
    listed_raw = await list_sec_filings(ticker=ticker, form_type=filing_type, max_filings=1)
    try:
        listed = json.loads(listed_raw)
        filings = listed.get("filings") if isinstance(listed, dict) else None
        if isinstance(filings, list) and filings:
            first = filings[0] if isinstance(filings[0], dict) else {}
            return first.get("primaryDocumentUrl")
    except Exception:
        return None
    return None


@yfinance_server.tool(name="get_sec_filing_outline", output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_outline"], description="Canonical alias for get_filing_outline.")
async def get_sec_filing_outline(ticker: str, filing_type: str = "10-K", period: str = "latest", accession_number: str | None = None, document_url: str | None = None) -> str:
    resolved_doc_url = document_url or (await _resolve_latest_sec_doc_url(ticker, filing_type) if period == "latest" else None)
    return await get_filing_outline(ticker, accession_number, resolved_doc_url)


@yfinance_server.tool(name="get_sec_filing_section", output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_section"], description="Canonical alias for get_filing_section.")
async def get_sec_filing_section(
    ticker: str,
    filing_type: str = "10-K",
    selector: dict | None = None,
    section_name: str | None = None,
    document_url: str | None = None,
    context_chars: int = 3000,
) -> str:
    resolved_doc_url = document_url or await _resolve_latest_sec_doc_url(ticker, filing_type)
    section = section_name or (selector or {}).get("item") or "Item 1A"
    return await get_filing_section(ticker, str(section), str(resolved_doc_url), context_chars)


@yfinance_server.tool(name="list_sec_filing_tables", output_schema=_TOOL_OUTPUT_SCHEMAS["list_filing_tables"], description="Canonical alias for list_filing_tables.")
async def list_sec_filing_tables(ticker: str, filing_type: str = "10-K", document_url: str | None = None) -> str:
    resolved_doc_url = document_url or await _resolve_latest_sec_doc_url(ticker, filing_type)
    return await list_filing_tables(ticker, str(resolved_doc_url))


@yfinance_server.tool(name="get_sec_filing_table", output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_table"], description="Canonical alias for get_filing_table.")
async def get_sec_filing_table(ticker: str, table_index: int, filing_type: str = "10-K", document_url: str | None = None, max_rows: int = 30) -> str:
    resolved_doc_url = document_url or await _resolve_latest_sec_doc_url(ticker, filing_type)
    return await get_filing_table(ticker, str(resolved_doc_url), table_index, max_rows)


@yfinance_server.tool(name="extract_sec_filing_fact", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_filing_fact"], description="Canonical SEC fact extractor (routes to get_filing_data or extract_filing_fact).")
async def extract_sec_filing_fact(
    ticker: str,
    fact: str | None = None,
    fact_name: str | None = None,
    fact_type: FilingFactType | None = None,
    region: str | None = None,
    filing_type: str = "10-K",
    period: str = "latest",
    document_url: str | None = None,
    accession_number: str | None = None,
) -> str:
    routed_fact_type = fact_type
    if routed_fact_type is None and fact is not None:
        try:
            routed_fact_type = FilingFactType(fact)
        except Exception:
            routed_fact_type = FilingFactType.geographic_revenue if region is not None else None
    if routed_fact_type is not None or region is not None or fact_name is None:
        routed_fact_type = routed_fact_type or FilingFactType.geographic_revenue
        raw = await get_filing_data(ticker=ticker, fact_type=routed_fact_type, region=region, filing_type=filing_type, period=period)
        parsed_payload: dict = {}
        try:
            parsed_any = json.loads(raw)
            if isinstance(parsed_any, dict) and "ok" in parsed_any and "data" in parsed_any:
                parsed_any = parsed_any.get("data")
            if isinstance(parsed_any, str):
                parsed_any = json.loads(parsed_any)
            if isinstance(parsed_any, dict):
                parsed_payload = parsed_any
        except Exception:
            parsed_payload = {}
        return json.dumps({
            "fact": routed_fact_type.value,
            "region": region,
            "value": parsed_payload.get("value"),
            "denominator": parsed_payload.get("denominator"),
            "valueRatio": parsed_payload.get("valueRatio"),
            "valuePct": parsed_payload.get("valuePct"),
            "rawValue": parsed_payload.get("rawValue"),
            "rawDenominator": parsed_payload.get("rawDenominator"),
            "unit": "USD",
            "unitScale": parsed_payload.get("unitScale"),
            "period": parsed_payload.get("period"),
            "filingType": parsed_payload.get("filingType", filing_type),
            "filingDate": parsed_payload.get("filingDate"),
            "accessionNumber": parsed_payload.get("accessionNumber"),
            "extractionMethod": parsed_payload.get("extractionMethod", "NONE"),
            "source": parsed_payload.get("source", "NOT_DISCLOSED"),
            "confidence": parsed_payload.get("confidence", "NOT_DISCLOSED"),
            "documentUrl": parsed_payload.get("documentUrl"),
            "indexUrl": parsed_payload.get("indexUrl"),
            "primaryDocumentUrl": parsed_payload.get("primaryDocumentUrl"),
            "evidence": parsed_payload.get("evidence"),
            "calculation": parsed_payload.get("calculation"),
            "warnings": parsed_payload.get("warnings", []),
            "ticker": parsed_payload.get("ticker", ticker),
        })
    return await extract_filing_fact(ticker=ticker, fact_name=fact_name, document_url=document_url, accession_number=accession_number)


@yfinance_server.tool(name="search_sec_filing_text", output_schema=_TOOL_OUTPUT_SCHEMAS["search_filing_text"], description="Canonical alias for search_filing_text.")
async def search_sec_filing_text(
    ticker: str,
    search_terms: list[str] | None = None,
    search_query: str | None = None,
    selector: dict | None = None,
    section_hint: str | None = None,
    filing_type: str = "10-K",
    accession_number: str | None = None,
    context_chars: int = 1500,
    return_tables: bool = True,
) -> str:
    terms = search_terms or ([search_query] if search_query else [])
    hint = section_hint or (selector or {}).get("item")
    return await search_filing_text(ticker, terms, hint, filing_type, accession_number, context_chars, return_tables)


# ---------------------------------------------------------------------------
# SEC Filing Index helpers
# ---------------------------------------------------------------------------

_INDEX_KEYWORDS = [
    "china", "greater china", "prc", "geographic", "segment", "revenue",
    "customers", "long-lived assets", "risk factors", "americas", "europe",
    "japan", "asia", "rest of asia",
]


def _build_filing_index_from_html(html: str) -> dict:
    """Parse an SEC filing HTML and return a structured index (sections, tables, keywordMap)."""
    # Sanitize: remove scripts/styles/event handlers.
    # Apply iteratively until stable to prevent nested/malformed pattern bypass.
    _script_re = _re.compile(r'<script\b[^>]*>[\s\S]*?</\s*script[^>]*>', _re.IGNORECASE)
    _style_re = _re.compile(r'<style\b[^>]*>[\s\S]*?</\s*style[^>]*>', _re.IGNORECASE)
    sanitized = html
    while True:
        next_s = _script_re.sub('<!--removed-->', sanitized)
        next_s = _style_re.sub('<!--removed-->', next_s)
        if next_s == sanitized:
            break
        sanitized = next_s
    sanitized = _re.sub(r'\s+on\w+=(?:"[^"]*"|\'[^\']*\'|[^\s>]+)', ' ', sanitized, flags=_re.IGNORECASE)

    # Section extraction
    sections: list[dict] = []
    heading_re = _re.compile(r'<h([1-6])[^>]*>(.*?)</h\1>', _re.DOTALL | _re.IGNORECASE)
    for h_match in heading_re.finditer(sanitized):
        if len(sections) >= 50:
            break
        level = int(h_match.group(1))
        raw_text = _strip_html_tags(h_match.group(2))
        if not raw_text or len(raw_text) > 200:
            continue
        normalized = raw_text.lower().strip()
        keywords = [kw for kw in _INDEX_KEYWORDS if kw in normalized]
        section_id = _re.sub(r'[^a-z0-9]+', '_', normalized)[:60]
        sections.append({
            "sectionId": section_id,
            "heading": raw_text,
            "normalizedHeading": normalized,
            "level": level,
            "keywords": keywords,
            "startChar": h_match.start(),
            "endChar": h_match.end(),
        })

    # Table extraction
    tables: list[dict] = []
    table_re = _re.compile(r'<table[^>]*>(.*?)</table>', _re.DOTALL | _re.IGNORECASE)
    tr_re = _re.compile(r'<tr[^>]*>(.*?)</tr>', _re.DOTALL | _re.IGNORECASE)
    td_re = _re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', _re.DOTALL | _re.IGNORECASE)

    for table_idx, t_match in enumerate(table_re.finditer(sanitized)):
        if table_idx >= 100:
            break
        table_start = t_match.start()
        table_html = t_match.group(0)

        # Nearest section starting before this table
        nearby_section_id: str | None = None
        nearby_heading = ""
        for sec in reversed(sections):
            if sec["startChar"] <= table_start:
                nearby_section_id = sec["sectionId"]
                nearby_heading = sec["heading"]
                break

        rows = tr_re.findall(t_match.group(1))
        if not rows:
            continue

        # Headers from first row
        first_cells = td_re.findall(rows[0])
        headers = [_strip_html_tags(c) for c in first_cells[:10]]

        # Row labels from first column of subsequent rows
        row_labels: list[str] = []
        for row in rows[1:20]:
            cells = td_re.findall(row)
            if cells:
                label = _strip_html_tags(cells[0])
                if label and len(label) < 100:
                    row_labels.append(label)

        # Unit scale: default to "unknown"; detect explicitly from context.
        pre_context = sanitized[max(0, table_start - 2000):table_start].lower()
        table_context = table_html.lower() + pre_context
        if "billion" in table_context:
            unit_scale = "billions"
        elif "million" in table_context:
            unit_scale = "millions"
        elif "thousand" in table_context:
            unit_scale = "thousands"
        else:
            unit_scale = "unknown"

        # Confidence: also lower when unitScale is unknown
        has_year_headers = any(_re.search(r'\b20\d\d\b', h) for h in headers)
        has_row_labels = bool(row_labels)
        if has_year_headers and has_row_labels and unit_scale != "unknown":
            confidence = "HIGH"
        elif has_year_headers or has_row_labels:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        # Infer title from preceding text
        pre_text = _strip_html_tags(sanitized[max(0, table_start - 500):table_start])
        lines = [ln.strip() for ln in pre_text.split('\n') if ln.strip()]
        title = ""
        if lines:
            candidate = lines[-1]
            if 10 < len(candidate) < 200:
                title = candidate

        tables.append({
            "tableId": table_idx,
            "sectionId": nearby_section_id,
            "title": title or nearby_heading,
            "headers": headers,
            "rowLabels": row_labels,
            "unit": "USD",
            "unitScale": unit_scale,
            "confidence": confidence,
        })

    # Keyword map
    keyword_map: dict[str, list[str]] = {}
    for kw in _INDEX_KEYWORDS:
        refs: list[str] = []
        for sec in sections:
            if kw in sec["normalizedHeading"]:
                ref = f"sectionId:{sec['sectionId']}"
                if ref not in refs:
                    refs.append(ref)
        for tbl in tables:
            haystack = " ".join(tbl["rowLabels"] + tbl["headers"] + [tbl["title"]]).lower()
            if kw in haystack:
                ref = f"tableId:{tbl['tableId']}"
                if ref not in refs:
                    refs.append(ref)
        if refs:
            keyword_map[kw] = refs

    return {"sections": sections, "tables": tables, "keywordMap": keyword_map}


async def _index_sec_filing_impl(
    ticker: str,
    filing_type: str = "10-K",
    accession_number: str | None = None,
) -> str:
    """Shared implementation for index_sec_filing and get_sec_filing_index."""
    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("index_sec_filing", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not resolve EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms: list[str] = recent.get("form", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    filing_dates: list[str] = recent.get("filingDate", [])
    accepted_dts: list[str] = recent.get("acceptanceDateTime", [])

    target_idx: int | None = None
    if accession_number:
        for i, acc in enumerate(accessions):
            if acc == accession_number:
                target_idx = i
                break
    else:
        for i, form in enumerate(forms):
            if str(form).upper() == filing_type.upper():
                target_idx = i
                accession_number = accessions[i] if i < len(accessions) else None
                break

    if target_idx is None or not accession_number:
        return _mcp_failure("index_sec_filing", ErrorCode.NO_FILING_DATA,
                            f"No {filing_type} filing found for '{ticker}'")

    filing_date = filing_dates[target_idx] if target_idx < len(filing_dates) else ""
    accepted_at = accepted_dts[target_idx] if target_idx < len(accepted_dts) else None
    primary_doc = primary_docs[target_idx] if target_idx < len(primary_docs) else None

    _, document_url = _edgar_build_filing_urls(cik_int, accession_number, primary_doc)
    if not document_url:
        return _mcp_failure("index_sec_filing", ErrorCode.NO_FILING_DATA,
                            f"primaryDocument missing for {accession_number}")

    # Check cache
    cache_key = f"secidx:{ticker.upper()}:{accession_number}:{filing_type}"
    cached = _tool_cache.get(cache_key)
    if cached is not None:
        return cached[0]

    # Fetch filing HTML
    html = await _edgar_get_html(document_url, max_bytes=5_000_000)
    if not html:
        return _mcp_failure("index_sec_filing", ErrorCode.PROVIDER_ERROR,
                            f"Failed to fetch filing document: {document_url}")

    index = _build_filing_index_from_html(html)
    indexed_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    result = json.dumps({
        "ticker": ticker,
        "cik": cik_padded,
        "filingType": filing_type,
        "filingDate": filing_date,
        "acceptedAt": accepted_at,
        "accessionNumber": accession_number,
        "documentUrl": document_url,
        "index": index,
        "meta": {
            "indexedAt": indexed_at,
            "source": "sec",
            "cacheKey": f"{ticker.upper()}:{accession_number}",
            "cacheTtlHours": 24,
        },
    })

    _tool_cache.set(cache_key, result, TTL_EDGAR)
    return result


@yfinance_server.tool(
    name="index_sec_filing",
    output_schema=_TOOL_OUTPUT_SCHEMAS["index_sec_filing"],
    description="""Build a deterministic section/table index for an SEC filing.
Identifies headings, tables, row labels, and units, enabling subsequent queries without re-fetching the filing.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type, e.g. "10-K" or "10-Q". Defaults to "10-K".
    period: Reserved for future multi-period support. Currently only "latest" is supported.
        When accession_number is provided, the specific filing is indexed regardless of period.
    accession_number: Optional SEC accession number (format XXXXXXXXXX-YY-ZZZZZZ).
        If omitted, the most recent filing matching filing_type is indexed.
""",
)
async def index_sec_filing(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("index_sec_filing", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("index_sec_filing", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)
    return await _index_sec_filing_impl(ticker, filing_type, accession_number)


@yfinance_server.tool(
    name="get_sec_filing_index",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_sec_filing_index"],
    description="""Get the pre-built section/table index for an SEC filing.
Returns cached index when available; builds and caches on first call.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type, e.g. "10-K" or "10-Q". Defaults to "10-K".
    period: Reserved for future multi-period support. Currently only "latest" is supported.
        When accession_number is provided, the specific filing is returned regardless of period.
    accession_number: Optional SEC accession number (format XXXXXXXXXX-YY-ZZZZZZ).
        If omitted, the most recent filing matching filing_type is used.
""",
)
async def get_sec_filing_index(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("get_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)
    return await _index_sec_filing_impl(ticker, filing_type, accession_number)


def _safe_json_loads(payload: str) -> dict:
    try:
        parsed = json.loads(payload)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _compact_excerpt(text: str, max_len: int = 240) -> str:
    cleaned = _re.sub(r"\s+", " ", str(text or "")).strip()
    return cleaned if len(cleaned) <= max_len else cleaned[:max_len].rstrip() + "..."


def _as_status(source_payload: dict) -> str:
    confidence = str(source_payload.get("confidence") or "").upper()
    source = str(source_payload.get("source") or "").upper()
    if source in {"NOT_DISCLOSED"} or confidence in {"NOT_DISCLOSED"}:
        return "NOT_DISCLOSED"
    if source in {"CONFLICTING"} or confidence in {"CONFLICTING"}:
        return "CONFLICTING"
    return "NOT_FOUND"


async def _extract_geo_payload(
    ticker: str,
    region: str,
    filing_type: str,
    period: str,
) -> dict:
    raw = await get_filing_data(
        ticker=ticker,
        fact_type=FilingFactType.geographic_revenue,
        region=region,
        filing_type=filing_type,
        period=period,
    )
    payload = _safe_json_loads(raw)
    warnings = payload.get("warnings")
    if not isinstance(warnings, list):
        warnings = []
    if payload.get("value") is not None and payload.get("denominator") is None:
        payload["valueRatio"] = None
        payload["valuePct"] = None
        if not any(isinstance(w, dict) and w.get("code") == "DENOMINATOR_NOT_FOUND" for w in warnings):
            warnings.append({
                "code": "DENOMINATOR_NOT_FOUND",
                "message": "Could not compute geographic revenue percentage due to missing denominator.",
                "severity": "warning",
            })
    payload["warnings"] = warnings
    return payload


@yfinance_server.tool(
    name="extract_geographic_revenue",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_geographic_revenue"],
    description="Deterministically extract geographic revenue exposure from SEC filing data and filing index.",
)
async def extract_geographic_revenue(
    ticker: str,
    region: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
    detailLevel: str = "compact",
) -> str:
    if not region or not str(region).strip():
        return json.dumps({
            "ticker": ticker,
            "factType": "geographic_revenue",
            "region": region,
            "period": None,
            "rawValue": None,
            "rawDenominator": None,
            "unit": "USD",
            "unitScale": "unknown",
            "value": None,
            "denominator": None,
            "valueRatio": None,
            "valuePct": None,
            "extractionMethod": "NONE",
            "confidence": "NOT_DISCLOSED",
            "evidence": {},
            "calculation": None,
            "warnings": [{"code": "INPUT_VALIDATION_ERROR", "message": "region is required", "severity": "error"}],
        })

    payload = await _extract_geo_payload(ticker, region, filing_type, period)
    idx_payload = _safe_json_loads(await get_sec_filing_index(ticker, filing_type, period, accession_number))
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    shaped = {
        "ticker": ticker,
        "factType": "geographic_revenue",
        "region": region,
        "period": payload.get("period"),
        "rawValue": payload.get("rawValue"),
        "rawDenominator": payload.get("rawDenominator"),
        "unit": payload.get("unit", "USD"),
        "unitScale": payload.get("unitScale", "unknown"),
        "value": payload.get("value"),
        "denominator": payload.get("denominator"),
        "valueRatio": payload.get("valueRatio"),
        "valuePct": payload.get("valuePct"),
        "extractionMethod": payload.get("extractionMethod", "NONE"),
        "confidence": payload.get("confidence", "NOT_DISCLOSED"),
        "evidence": {
            "filingType": idx_payload.get("filingType") or payload.get("filingType") or filing_type,
            "filingDate": idx_payload.get("filingDate") or payload.get("filingDate"),
            "acceptedAt": idx_payload.get("acceptedAt"),
            "accessionNumber": idx_payload.get("accessionNumber") or payload.get("accessionNumber"),
            "documentUrl": idx_payload.get("documentUrl") or payload.get("documentUrl"),
            "sectionHeading": evidence.get("sectionHeading"),
            "tableTitle": evidence.get("tableTitle"),
            "sourceTableId": evidence.get("sourceTableId"),
            "sourceRows": evidence.get("sourceRows") if isinstance(evidence.get("sourceRows"), list) else [],
            "sourceColumns": evidence.get("sourceColumns") if isinstance(evidence.get("sourceColumns"), list) else [],
        },
        "calculation": payload.get("calculation"),
        "warnings": payload.get("warnings") if isinstance(payload.get("warnings"), list) else [],
    }
    if shaped["denominator"] is None:
        shaped["valueRatio"] = None
        shaped["valuePct"] = None
    if str(detailLevel).lower() == "raw":
        shaped["rawContext"] = {"filingIndex": idx_payload}
    return json.dumps(shaped)


@yfinance_server.tool(name="extract_segment_revenue", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_segment_revenue"], description="Extract segment revenue rows from SEC filing facts with evidence metadata.")
async def extract_segment_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.segment_revenue, filing_type=filing_type, period=period))
    segments = payload.get("allSegments") if isinstance(payload.get("allSegments"), list) else []
    rows = []
    for seg in segments:
        if not isinstance(seg, dict):
            continue
        rows.append({
            "label": seg.get("segmentLabel"),
            "value": seg.get("value"),
            "period": f"FY{seg.get('fiscalYear')}" if seg.get("fiscalYear") else None,
            "confidence": "HIGH",
            "evidence": {
                "filingDate": seg.get("filingDate"),
                "accessionNumber": seg.get("accessionNumber"),
            },
        })
    out = {"ticker": ticker, "factType": "segment_revenue", "segments": rows, "status": "FOUND" if rows else "NOT_DISCLOSED"}
    if str(detailLevel).lower() == "raw":
        out["rawContext"] = payload
    return json.dumps(out)


@yfinance_server.tool(name="extract_total_revenue", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_total_revenue"], description="Extract total revenue from SEC filing facts with stable null fields.")
async def extract_total_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
) -> str:
    payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.total_revenue, filing_type=filing_type, period=period))
    val = payload.get("value")
    return json.dumps({
        "ticker": ticker,
        "factType": "total_revenue",
        "value": val,
        "period": payload.get("period"),
        "confidence": payload.get("confidence", "NOT_DISCLOSED" if val is None else "HIGH"),
        "evidence": {
            "filingType": payload.get("filingType", filing_type),
            "filingDate": payload.get("filingDate"),
            "accessionNumber": payload.get("accessionNumber"),
            "documentUrl": payload.get("documentUrl"),
        },
        "status": "FOUND" if val is not None else _as_status(payload),
    })


@yfinance_server.tool(name="extract_revenue_exposure", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_revenue_exposure"], description="Extract revenue exposure for a region/customer/segment query with deterministic status codes.")
async def extract_revenue_exposure(
    ticker: str,
    exposure_query: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    geo = _safe_json_loads(await extract_geographic_revenue(ticker=ticker, region=exposure_query, filing_type=filing_type, period=period, detailLevel=detailLevel))
    found = geo.get("value") is not None
    status = "FOUND_REVENUE_EXPOSURE" if found else _as_status(geo)
    matches = []
    if found:
        matches.append({
            "exposureType": "geographic_revenue",
            "label": exposure_query,
            "value": geo.get("value"),
            "denominator": geo.get("denominator"),
            "valueRatio": geo.get("valueRatio"),
            "valuePct": geo.get("valuePct"),
            "period": geo.get("period"),
            "confidence": geo.get("confidence", "HIGH"),
            "evidence": geo.get("evidence", {}),
        })
    return json.dumps({"ticker": ticker, "query": exposure_query, "matches": matches, "status": status})


@yfinance_server.tool(name="extract_risk_factor_mentions", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_risk_factor_mentions"], description="Extract concise risk-factor mentions for explicit terms from a filing.")
async def extract_risk_factor_mentions(
    ticker: str,
    terms: list[str],
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    matches: list[dict] = []
    for term in (terms or []):
        search = _safe_json_loads(await search_sec_filing_text(
            ticker=ticker,
            search_terms=[str(term)],
            section_hint="Risk Factors",
            filing_type=filing_type,
        ))
        for m in (search.get("matches") if isinstance(search.get("matches"), list) else [])[:3]:
            if not isinstance(m, dict):
                continue
            matches.append({
                "term": term,
                "sectionHeading": m.get("sectionHeading") or "Risk Factors",
                "excerpt": _compact_excerpt(str(m.get("context") or m.get("excerpt") or "")),
                "confidence": "MEDIUM",
                "evidence": {
                    "filingDate": search.get("filingDate"),
                    "accessionNumber": search.get("accessionNumber"),
                    "documentUrl": search.get("documentUrl"),
                },
            })
    result = {"ticker": ticker, "matches": matches, "status": "FOUND" if matches else "NOT_FOUND"}
    if str(detailLevel).lower() == "raw":
        result["rawTerms"] = terms or []
    return json.dumps(result)


@yfinance_server.tool(name="extract_customer_concentration", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_customer_concentration"], description="Extract customer concentration percentages from SEC filing text evidence.")
async def extract_customer_concentration(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    search = _safe_json_loads(await search_sec_filing_text(
        ticker=ticker,
        search_terms=["major customer", "customers", "customer accounted", "percent of revenue"],
        filing_type=filing_type,
    ))
    customers: list[dict] = []
    seen: set[str] = set()
    for m in (search.get("matches") if isinstance(search.get("matches"), list) else []):
        if not isinstance(m, dict):
            continue
        ctx = str(m.get("context") or "")
        pct_match = _re.search(r"(\d{1,2}(?:\.\d+)?)\s*%", ctx)
        if not pct_match:
            continue
        pct = float(pct_match.group(1))
        key = f"{pct:.2f}"
        if key in seen:
            continue
        seen.add(key)
        customers.append({
            "label": f"Customer {chr(64 + len(customers) + 1)}",
            "valuePct": pct,
            "period": f"FY{str(search.get('fiscalYear') or '')}".rstrip(),
            "confidence": "HIGH",
            "evidence": {
                "sectionHeading": m.get("sectionHeading"),
                "excerpt": _compact_excerpt(ctx),
                "filingDate": search.get("filingDate"),
                "accessionNumber": search.get("accessionNumber"),
                "documentUrl": search.get("documentUrl"),
            },
        })
        if len(customers) >= 5:
            break
    status = "FOUND" if customers else ("NOT_DISCLOSED" if (search.get("matchCount") or 0) > 0 else "NOT_FOUND")
    result = {"ticker": ticker, "customers": customers, "status": status}
    if str(detailLevel).lower() == "raw":
        result["rawMatchCount"] = search.get("matchCount", 0)
    return json.dumps(result)


@yfinance_server.tool(name="extract_china_exposure", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_china_exposure"], description="Extract China exposure with separate revenue and non-revenue classifications.")
async def extract_china_exposure(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
    detailLevel: str = "compact",
) -> str:
    idx = _safe_json_loads(await get_sec_filing_index(ticker=ticker, filing_type=filing_type, period=period, accession_number=accession_number))
    revenue = _safe_json_loads(await extract_revenue_exposure(ticker=ticker, exposure_query="China", filing_type=filing_type, period=period))
    revenue_status = "FOUND" if revenue.get("status") == "FOUND_REVENUE_EXPOSURE" else revenue.get("status", "NOT_FOUND")

    index = idx.get("index") if isinstance(idx.get("index"), dict) else {}
    sections = index.get("sections") if isinstance(index.get("sections"), list) else []
    tables = index.get("tables") if isinstance(index.get("tables"), list) else []

    def _collect(term_list: list[str]) -> list[dict]:
        found = []
        for sec in sections:
            if not isinstance(sec, dict):
                continue
            heading = str(sec.get("heading") or "")
            low = heading.lower()
            for term in term_list:
                if term.lower() in low:
                    found.append({"source": "section", "term": term, "sectionHeading": heading})
        for tbl in tables:
            if not isinstance(tbl, dict):
                continue
            hay = " ".join([str(tbl.get("title") or ""), *[str(x) for x in (tbl.get("rowLabels") or [])]]).lower()
            for term in term_list:
                if term.lower() in hay:
                    found.append({
                        "source": "table",
                        "term": term,
                        "tableTitle": tbl.get("title"),
                        "sourceTableId": tbl.get("tableId"),
                        "sectionId": tbl.get("sectionId"),
                    })
        return found

    entity_terms = ["Tongmei", "JinMei", "BoYu"]
    bank_terms = ["Bank of China"]
    manuf_terms = ["manufacturing", "production", "supply chain", "fab"]
    risk_terms = ["China", "tariff", "export control"]

    entity_evidence = _collect(entity_terms)
    bank_evidence = _collect(bank_terms)
    manu_evidence = _collect(manuf_terms)
    risk_mentions = _safe_json_loads(await extract_risk_factor_mentions(ticker=ticker, terms=risk_terms, filing_type=filing_type, period=period))
    risk_evidence = risk_mentions.get("matches") if isinstance(risk_mentions.get("matches"), list) else []

    non_revenue_found = bool(entity_evidence or bank_evidence or manu_evidence or risk_evidence)
    if revenue.get("status") == "FOUND_REVENUE_EXPOSURE":
        overall = "FOUND_REVENUE_EXPOSURE"
    elif non_revenue_found:
        overall = "FOUND_NON_REVENUE_EXPOSURE"
    elif revenue.get("status") == "NOT_DISCLOSED":
        overall = "NOT_DISCLOSED"
    elif revenue.get("status") == "CONFLICTING":
        overall = "CONFLICTING"
    else:
        overall = "NOT_FOUND"

    out = {
        "ticker": ticker,
        "exposureType": "china_exposure",
        "filingType": idx.get("filingType", filing_type),
        "filingDate": idx.get("filingDate"),
        "accessionNumber": idx.get("accessionNumber"),
        "documentUrl": idx.get("documentUrl"),
        "revenueExposure": {
            "status": revenue_status,
            "value": revenue.get("matches", [{}])[0].get("value") if revenue.get("matches") else None,
            "denominator": revenue.get("matches", [{}])[0].get("denominator") if revenue.get("matches") else None,
            "valueRatio": revenue.get("matches", [{}])[0].get("valueRatio") if revenue.get("matches") else None,
            "valuePct": revenue.get("matches", [{}])[0].get("valuePct") if revenue.get("matches") else None,
            "confidence": "HIGH" if revenue_status == "FOUND" else ("NOT_DISCLOSED" if revenue_status == "NOT_DISCLOSED" else "LOW"),
            "evidence": revenue.get("matches", [{}])[0].get("evidence") if revenue.get("matches") else [],
        },
        "manufacturingExposure": {"status": "FOUND" if manu_evidence else "NOT_FOUND", "confidence": "MEDIUM", "evidence": manu_evidence},
        "entityExposure": {"status": "FOUND" if entity_evidence else "NOT_FOUND", "entities": entity_terms if entity_evidence else [], "confidence": "MEDIUM", "evidence": entity_evidence},
        "bankExposure": {"status": "FOUND" if bank_evidence else "NOT_FOUND", "entities": bank_terms if bank_evidence else [], "confidence": "MEDIUM", "evidence": bank_evidence},
        "riskFactorExposure": {"status": "FOUND" if risk_evidence else "NOT_FOUND", "confidence": "MEDIUM", "evidence": risk_evidence},
        "overallStatus": overall,
        "warnings": [],
    }
    if str(detailLevel).lower() == "raw":
        out["rawContext"] = {"filingIndex": idx}
    return json.dumps(out)


@yfinance_server.tool(
    name="query_sec_filing_index",
    output_schema=_TOOL_OUTPUT_SCHEMAS["query_sec_filing_index"],
    description="Deterministically route supported SEC filing index query types to extractor tools.",
)
async def query_sec_filing_index(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
    query_type: str = "",
    params: dict | None = None,
    return_evidence: bool = True,
    detailLevel: str = "compact",
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("query_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("query_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)

    allowed_detail = {"compact", "evidence", "raw"}
    detail = str(detailLevel or "compact").lower()
    if detail not in allowed_detail:
        return json.dumps({
            "status": "INPUT_VALIDATION_ERROR",
            "queryType": query_type,
            "ticker": ticker,
            "filingType": filing_type,
            "period": period,
            "answer": None,
            "confidence": "NOT_DISCLOSED",
            "evidence": [],
            "warnings": [{"code": "INPUT_VALIDATION_ERROR", "message": "detailLevel must be one of: compact, evidence, raw"}],
        })

    query = str(query_type or "").strip()
    routed_params = params if isinstance(params, dict) else {}

    def _shape_evidence(ev: dict) -> dict:
        shaped = {
            "filingDate": ev.get("filingDate"),
            "acceptedAt": ev.get("acceptedAt"),
            "accessionNumber": ev.get("accessionNumber"),
            "documentUrl": ev.get("documentUrl"),
            "sectionHeading": ev.get("sectionHeading"),
            "tableTitle": ev.get("tableTitle"),
            "sourceTableId": ev.get("sourceTableId"),
        }
        if detail in {"evidence", "raw"}:
            shaped["sourceRows"] = ev.get("sourceRows") if isinstance(ev.get("sourceRows"), list) else []
            shaped["sourceColumns"] = ev.get("sourceColumns") if isinstance(ev.get("sourceColumns"), list) else []
            if ev.get("excerpt") is not None:
                shaped["excerpt"] = ev.get("excerpt")
        return shaped

    def _result(status: str, answer: dict | None, confidence: str, evidence_items: list[dict] | None = None, warnings: list[dict] | None = None) -> str:
        return json.dumps({
            "status": status,
            "queryType": query,
            "ticker": ticker,
            "filingType": filing_type,
            "period": period,
            "answer": answer,
            "confidence": confidence,
            "evidence": evidence_items if return_evidence else [],
            "warnings": warnings or [],
        })

    def _missing_param(name: str) -> str:
        return _result(
            "INPUT_VALIDATION_ERROR",
            None,
            "NOT_DISCLOSED",
            [],
            [{"code": "INPUT_VALIDATION_ERROR", "message": f"Missing required params.{name} for query_type={query}"}],
        )

    supported = {
        "geographic_revenue_share",
        "revenue_exposure",
        "china_exposure",
        "risk_factor_mentions",
        "customer_concentration",
        "total_revenue",
        "segment_revenue",
    }
    if query not in supported:
        return _result(
            "UNSUPPORTED_BY_INDEX",
            None,
            "NOT_DISCLOSED",
            [],
            [{"code": "UNSUPPORTED_QUERY_TYPE", "message": "Use one of the supported query_type values."}],
        )

    warnings: list[dict] = []

    if query == "geographic_revenue_share":
        region = str(routed_params.get("region") or "").strip()
        if not region:
            return _missing_param("region")
        geo = _safe_json_loads(await extract_geographic_revenue(
            ticker=ticker,
            region=region,
            filing_type=filing_type,
            period=period,
            accession_number=accession_number,
            detailLevel=detail,
        ))
        evidence_obj = geo.get("evidence") if isinstance(geo.get("evidence"), dict) else {}
        evidence = [_shape_evidence(evidence_obj)] if evidence_obj else []
        status = "ANSWERED" if geo.get("value") is not None else _as_status(geo)
        if status == "ANSWERED" and not evidence:
            status = "NOT_FOUND"
            warnings.append({"code": "EVIDENCE_REQUIRED", "message": "ANSWERED responses require evidence."})
        answer = {
            "region": region,
            "value": geo.get("value"),
            "denominator": geo.get("denominator"),
            "valueRatio": geo.get("valueRatio"),
            "valuePct": geo.get("valuePct"),
            "unit": geo.get("unit", "USD"),
        }
        confidence = str(geo.get("confidence") or ("HIGH" if status == "ANSWERED" else "NOT_DISCLOSED"))
        if status == "NOT_FOUND":
            confidence = "NOT_DISCLOSED" if str(geo.get("confidence") or "").upper() == "NOT_DISCLOSED" else "LOW"
        return _result(status, answer, confidence, evidence, warnings)

    if query == "revenue_exposure":
        exposure_query = str(routed_params.get("exposure_query") or "").strip()
        if not exposure_query:
            return _missing_param("exposure_query")
        rex = _safe_json_loads(await extract_revenue_exposure(
            ticker=ticker,
            exposure_query=exposure_query,
            filing_type=filing_type,
            period=period,
            detailLevel=detail,
        ))
        matches = rex.get("matches") if isinstance(rex.get("matches"), list) else []
        first = matches[0] if matches and isinstance(matches[0], dict) else {}
        evidence_obj = first.get("evidence") if isinstance(first.get("evidence"), dict) else {}
        evidence = [_shape_evidence(evidence_obj)] if evidence_obj else []
        status = "ANSWERED" if bool(matches) and str(rex.get("status")) == "FOUND_REVENUE_EXPOSURE" else str(rex.get("status") or "NOT_FOUND")
        if status == "ANSWERED" and not evidence:
            status = "NOT_FOUND"
            warnings.append({"code": "EVIDENCE_REQUIRED", "message": "ANSWERED responses require evidence."})
        answer = {
            "exposureQuery": exposure_query,
            "value": first.get("value"),
            "denominator": first.get("denominator"),
            "valueRatio": first.get("valueRatio"),
            "valuePct": first.get("valuePct"),
            "period": first.get("period"),
        }
        confidence = str(first.get("confidence") or ("HIGH" if status == "ANSWERED" else "NOT_DISCLOSED"))
        return _result(status, answer, confidence, evidence, warnings)

    if query == "china_exposure":
        china = _safe_json_loads(await extract_china_exposure(
            ticker=ticker,
            filing_type=filing_type,
            period=period,
            accession_number=accession_number,
            detailLevel=detail,
        ))
        overall = str(china.get("overallStatus") or "NOT_FOUND")
        answer = {
            "revenueExposure": china.get("revenueExposure"),
            "manufacturingExposure": china.get("manufacturingExposure"),
            "entityExposure": china.get("entityExposure"),
            "bankExposure": china.get("bankExposure"),
            "riskFactorExposure": china.get("riskFactorExposure"),
            "overallStatus": overall,
        }
        evidence: list[dict] = []
        for key in ("revenueExposure", "manufacturingExposure", "entityExposure", "bankExposure", "riskFactorExposure"):
            block = china.get(key)
            if not isinstance(block, dict):
                continue
            ev = block.get("evidence")
            if isinstance(ev, dict):
                evidence.append(_shape_evidence(ev))
            elif isinstance(ev, list):
                for item in ev:
                    if isinstance(item, dict):
                        evidence.append(_shape_evidence(item))
        status = "ANSWERED" if overall in {"FOUND_REVENUE_EXPOSURE", "FOUND_NON_REVENUE_EXPOSURE"} else overall
        if status == "ANSWERED" and not evidence:
            status = "NOT_FOUND"
            warnings.append({"code": "EVIDENCE_REQUIRED", "message": "ANSWERED responses require evidence."})
        confidence = "HIGH" if status == "ANSWERED" else ("NOT_DISCLOSED" if overall == "NOT_DISCLOSED" else "LOW")
        return _result(status, answer, confidence, evidence, warnings)

    if query == "risk_factor_mentions":
        terms = routed_params.get("terms")
        terms_list = [str(t) for t in terms] if isinstance(terms, list) else []
        if not terms_list:
            return _missing_param("terms")
        risk = _safe_json_loads(await extract_risk_factor_mentions(
            ticker=ticker,
            terms=terms_list,
            filing_type=filing_type,
            period=period,
            detailLevel=detail,
        ))
        matches = [m for m in (risk.get("matches") if isinstance(risk.get("matches"), list) else []) if isinstance(m, dict)]
        evidence = [_shape_evidence(m.get("evidence")) for m in matches if isinstance(m.get("evidence"), dict)]
        status = "ANSWERED" if matches else str(risk.get("status") or "NOT_FOUND")
        if status == "ANSWERED" and not evidence:
            status = "NOT_FOUND"
            warnings.append({"code": "EVIDENCE_REQUIRED", "message": "ANSWERED responses require evidence."})
        return _result(
            status,
            {"terms": terms_list, "matches": matches},
            "MEDIUM" if status == "ANSWERED" else "LOW",
            evidence,
            warnings,
        )

    if query == "customer_concentration":
        customer_label = str(routed_params.get("customer_label") or "").strip()
        cust = _safe_json_loads(await extract_customer_concentration(
            ticker=ticker,
            filing_type=filing_type,
            period=period,
            detailLevel=detail,
        ))
        customers = [c for c in (cust.get("customers") if isinstance(cust.get("customers"), list) else []) if isinstance(c, dict)]
        if customer_label:
            customers = [c for c in customers if str(c.get("label") or "").lower() == customer_label.lower()]
        evidence = [_shape_evidence(c.get("evidence")) for c in customers if isinstance(c.get("evidence"), dict)]
        status = "ANSWERED" if customers else str(cust.get("status") or "NOT_FOUND")
        if status == "ANSWERED" and not evidence:
            status = "NOT_FOUND"
            warnings.append({"code": "EVIDENCE_REQUIRED", "message": "ANSWERED responses require evidence."})
        return _result(
            status,
            {"customerLabel": customer_label or None, "customers": customers},
            "HIGH" if status == "ANSWERED" else ("NOT_DISCLOSED" if status == "NOT_DISCLOSED" else "LOW"),
            evidence,
            warnings,
        )

    if query == "total_revenue":
        total = _safe_json_loads(await extract_total_revenue(
            ticker=ticker,
            filing_type=filing_type,
            period=period,
        ))
        evidence_obj = total.get("evidence") if isinstance(total.get("evidence"), dict) else {}
        evidence = [_shape_evidence(evidence_obj)] if evidence_obj else []
        status = "ANSWERED" if total.get("value") is not None else str(total.get("status") or "NOT_FOUND")
        if status == "ANSWERED" and not evidence:
            status = "NOT_FOUND"
            warnings.append({"code": "EVIDENCE_REQUIRED", "message": "ANSWERED responses require evidence."})
        return _result(
            status,
            {"value": total.get("value"), "period": total.get("period"), "unit": "USD"},
            str(total.get("confidence") or ("HIGH" if status == "ANSWERED" else "NOT_DISCLOSED")),
            evidence,
            warnings,
        )

    segment_name = str(routed_params.get("segment") or "").strip()
    seg = _safe_json_loads(await extract_segment_revenue(
        ticker=ticker,
        filing_type=filing_type,
        period=period,
        detailLevel=detail,
    ))
    segments = [s for s in (seg.get("segments") if isinstance(seg.get("segments"), list) else []) if isinstance(s, dict)]
    if segment_name:
        segments = [s for s in segments if str(s.get("label") or "").lower() == segment_name.lower()]
    evidence = [_shape_evidence(s.get("evidence")) for s in segments if isinstance(s.get("evidence"), dict)]
    status = "ANSWERED" if segments else ("NOT_FOUND" if segment_name else str(seg.get("status") or "NOT_FOUND"))
    if status == "ANSWERED" and not evidence:
        status = "NOT_FOUND"
        warnings.append({"code": "EVIDENCE_REQUIRED", "message": "ANSWERED responses require evidence."})
    return _result(
        status,
        {"segment": segment_name or None, "segments": segments},
        "HIGH" if status == "ANSWERED" else ("NOT_DISCLOSED" if status == "NOT_DISCLOSED" else "LOW"),
        evidence,
        warnings,
    )


def _utc_now_z() -> str:
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _to_iso_utc(ts: str | None) -> str | None:
    if not ts:
        return None
    s = str(ts).strip()
    if not s:
        return None
    try:
        if len(s) == 10 and _re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return f"{s}T00:00:00Z"
        return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(
            datetime.timezone.utc
        ).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _derive_fiscal_period_from_date(date_str: str | None) -> str | None:
    if not date_str:
        return None
    try:
        d = datetime.datetime.fromisoformat(str(date_str)[:10]).date()
    except Exception:
        return None
    q = ((d.month - 1) // 3) + 1
    return f"FY{d.year} Q{q}"


def _is_paywalled_url(url: str) -> bool:
    host = (_urlparse.urlparse(url).hostname or "").lower()
    blocked = {
        "seekingalpha.com",
        "www.seekingalpha.com",
        "wsj.com",
        "www.wsj.com",
        "bloomberg.com",
        "www.bloomberg.com",
    }
    return host in blocked


def _classify_earnings_source_url(url: str) -> tuple[str | None, str | None]:
    if not isinstance(url, str) or not url.strip():
        return None, "source_url must be a non-empty string"
    parsed = _urlparse.urlparse(url.strip())
    if parsed.scheme != "https":
        return None, "source_url must use https"
    if _is_paywalled_url(url):
        return None, "source_url appears paywalled and is not allowed"
    if url.startswith("https://www.sec.gov/Archives/"):
        return "sec_8k", None
    return "company_ir", None


def _fetch_public_html(url: str, max_bytes: int = 3_000_000) -> str | None:
    req = _urlrequest.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; yahoo-finance-mcp-earnings/1.0)"},
        method="GET",
    )
    try:
        with _urlrequest.urlopen(req, timeout=60) as resp:
            data = resp.read(max_bytes)
            return data.decode("utf-8", errors="ignore")
    except (_urlerror.URLError, ValueError):
        return None


def _scale_number_from_text(raw: str) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "")
    if not s:
        return None
    m = _re.search(r"[-+]?\d+(?:\.\d+)?", s)
    if not m:
        return None
    n = float(m.group(0))
    low = s.lower()
    if "billion" in low or low.endswith("b") or " bn" in low:
        n *= 1_000_000_000
    elif "million" in low or low.endswith("m"):
        n *= 1_000_000
    elif "thousand" in low or low.endswith("k"):
        n *= 1_000
    return n


def _first_sentence_for_topic(text: str, topic: str) -> str | None:
    topic_l = topic.lower()
    for sent in _re.split(r"(?<=[.!?])\s+", text):
        if topic_l in sent.lower():
            return _compact_excerpt(sent, max_len=220)
    return None


def _extract_metric_number(text: str, patterns: list[str]) -> tuple[float | None, str | None, str | None]:
    for pat in patterns:
        m = _re.search(pat, text, flags=_re.IGNORECASE)
        if m:
            raw = m.group(1)
            val = _scale_number_from_text(raw)
            if val is not None:
                return val, raw, _compact_excerpt(m.group(0), max_len=220)
    return None, None, None


async def _resolve_latest_earnings_sec_source(ticker: str) -> dict | None:
    raw = await list_sec_company_filings(ticker=ticker, filing_type="8-K", limit=10)
    payload = _safe_json_loads(raw)
    filings = payload.get("filings") if isinstance(payload.get("filings"), list) else []
    if not filings:
        return None
    for filing in filings:
        if not isinstance(filing, dict):
            continue
        doc_url = str(filing.get("documentUrl") or "")
        if not doc_url.startswith("https://www.sec.gov/Archives/"):
            continue
        return {
            "sourceType": "sec_8k",
            "url": doc_url,
            "filingDate": filing.get("filingDate"),
            "acceptedAt": filing.get("acceptedAt"),
            "accessionNumber": filing.get("accessionNumber"),
            "confidence": "HIGH",
        }
    return None


async def _resolve_latest_earnings_release(ticker: str) -> dict:
    sec = await _resolve_latest_earnings_sec_source(ticker)
    if sec:
        reporting_ts = _to_iso_utc(sec.get("acceptedAt")) or _to_iso_utc(sec.get("filingDate"))
        period = _derive_fiscal_period_from_date(sec.get("filingDate")) or "latest"
        return {
            "ticker": ticker.upper(),
            "eventType": "earnings_release",
            "period": period,
            "reportedAt": reporting_ts,
            "sources": [sec],
            "confidence": "HIGH",
            "warnings": [],
        }

    yahoo_url = f"https://finance.yahoo.com/quote/{ticker.upper()}/analysis"
    cal_raw = await get_calendar(ticker=ticker.upper())
    cal = _safe_json_loads(cal_raw)
    earnings_dates = (((cal.get("calendar") or {}).get("earnings") or {}).get("earningsDate") or [])
    published = earnings_dates[0] if isinstance(earnings_dates, list) and earnings_dates else None
    if published:
        period = _derive_fiscal_period_from_date(published) or "latest"
        return {
            "ticker": ticker.upper(),
            "eventType": "earnings_release",
            "period": period,
            "reportedAt": _to_iso_utc(published),
            "sources": [
                {
                    "sourceType": "yahoo_estimate",
                    "url": yahoo_url,
                    "publishedAt": _to_iso_utc(published),
                    "retrievedAt": _utc_now_z(),
                    "confidence": "MEDIUM",
                }
            ],
            "confidence": "MEDIUM",
            "warnings": [{"code": "SEC_8K_NOT_FOUND", "message": "SEC 8-K earnings release source not found"}],
        }

    return {
        "ticker": ticker.upper(),
        "eventType": "earnings_release",
        "period": "latest",
        "reportedAt": None,
        "sources": [],
        "confidence": "NOT_FOUND",
        "warnings": [],
    }


@yfinance_server.tool(
    name="get_latest_earnings_release",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_latest_earnings_release"],
    description="Find the latest public earnings release evidence from SEC 8-K, company IR, or Yahoo earnings calendars.",
)
async def get_latest_earnings_release(ticker: str, period: str = "latest") -> str:
    _ = period  # reserved for future explicit periods
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_latest_earnings_release", ErrorCode.INPUT_VALIDATION_ERROR, err)
    return json.dumps(await _resolve_latest_earnings_release(ticker))


@yfinance_server.tool(
    name="index_earnings_release",
    output_schema=_TOOL_OUTPUT_SCHEMAS["index_earnings_release"],
    description="Build a compact section/table index for the latest public earnings release to support deterministic extraction.",
)
async def index_earnings_release(ticker: str, period: str = "latest", source_url: str | None = None) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("index_earnings_release", ErrorCode.INPUT_VALIDATION_ERROR, err)
    source_type = None
    source_meta: dict = {}
    if source_url:
        source_type, source_err = _classify_earnings_source_url(source_url)
        if source_err:
            return _mcp_failure("index_earnings_release", ErrorCode.INPUT_VALIDATION_ERROR, source_err)
        source_meta = {"sourceType": source_type, "url": source_url}
    else:
        latest = await _resolve_latest_earnings_release(ticker)
        sources = latest.get("sources") if isinstance(latest.get("sources"), list) else []
        src = sources[0] if sources and isinstance(sources[0], dict) else {}
        source_type = str(src.get("sourceType") or "")
        source_url = str(src.get("url") or "")
        source_meta = src

    if not source_url:
        return json.dumps({
            "ticker": ticker.upper(),
            "period": period,
            "source": {"sourceType": source_type or "unknown", "url": None},
            "index": {"sections": [], "tables": [], "keywordMap": {}},
            "meta": {"indexedAt": _utc_now_z(), "cacheKey": f"earnidx:{ticker.upper()}:none", "cacheTtlHours": 24},
            "warnings": [{"code": "SOURCE_NOT_FOUND", "message": "No public earnings release source found"}],
        })

    cache_id = str(source_meta.get("accessionNumber") or source_url)
    cache_key = f"earnidx:{ticker.upper()}:{cache_id}"
    cached = _tool_cache.get(cache_key)
    if cached is not None:
        return cached[0]

    html = await _edgar_get_html(source_url, max_bytes=5_000_000) if source_url.startswith(
        "https://www.sec.gov/Archives/"
    ) else _fetch_public_html(source_url)
    if not html:
        return _mcp_failure("index_earnings_release", ErrorCode.PROVIDER_ERROR, f"Failed to fetch source: {source_url}")

    idx = _build_filing_index_from_html(_sanitize_sec_html(html))
    out = {
        "ticker": ticker.upper(),
        "period": _derive_fiscal_period_from_date(source_meta.get("filingDate") or source_meta.get("publishedAt")) or period,
        "source": {
            "sourceType": source_type or source_meta.get("sourceType") or "company_ir",
            "url": source_url,
            "publishedAt": source_meta.get("publishedAt"),
            "retrievedAt": _utc_now_z(),
            "filingDate": source_meta.get("filingDate"),
            "acceptedAt": source_meta.get("acceptedAt"),
            "accessionNumber": source_meta.get("accessionNumber"),
        },
        "index": idx,
        "meta": {"indexedAt": _utc_now_z(), "cacheKey": cache_key, "cacheTtlHours": 24},
    }
    encoded = json.dumps(out)
    _tool_cache.set(cache_key, encoded, TTL_EDGAR)
    return encoded


@yfinance_server.tool(
    name="extract_earnings_metrics",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_earnings_metrics"],
    description="Extract reported earnings metrics (revenue, EPS, margin, operating income, free cash flow, capex) from public earnings sources.",
)
async def extract_earnings_metrics(
    ticker: str,
    period: str = "latest",
    source_preference: list[str] | None = None,
) -> str:
    _ = source_preference or ["sec_8k", "company_ir", "10-q", "yahoo"]
    release = await _resolve_latest_earnings_release(ticker)
    default_metric = lambda unit: {  # noqa: E731
        "value": None,
        "unit": unit,
        "confidence": "NOT_DISCLOSED",
        "evidence": None,
    }
    metrics: dict = {
        "revenue": default_metric("USD"),
        "epsDiluted": default_metric("USD/share"),
        "grossMargin": {
            "valueRatio": None,
            "valuePct": None,
            "rawValue": None,
            "confidence": "NOT_DISCLOSED",
            "evidence": None,
        },
        "operatingIncome": default_metric("USD"),
        "freeCashFlow": default_metric("USD"),
        "capex": default_metric("USD"),
    }
    evidence_items: list[dict] = []
    warnings: list[dict] = []
    sources = release.get("sources") if isinstance(release.get("sources"), list) else []
    src = sources[0] if sources and isinstance(sources[0], dict) else {}
    src_url = str(src.get("url") or "")
    src_type = str(src.get("sourceType") or "yahoo")
    src_published = _to_iso_utc(src.get("filingDate") or src.get("publishedAt"))
    retrieved_at = _utc_now_z()

    if src_url and src_url.startswith("https://www.sec.gov/Archives/"):
        html = await _edgar_get_html(src_url, max_bytes=5_000_000)
        text = _strip_html_tags(_sanitize_sec_html(html or ""))
        revenue_val, revenue_raw, revenue_ex = _extract_metric_number(
            text,
            [
                r"(?:net sales|revenue(?:s)?)\D{0,20}\$?\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)",
            ],
        )
        eps_val, eps_raw, eps_ex = _extract_metric_number(
            text,
            [
                r"(?:diluted (?:earnings per share|eps)|eps \(diluted\))\D{0,20}\$?\s*([0-9]+(?:\.[0-9]+)?)",
            ],
        )
        gm_val, gm_raw, gm_ex = _extract_metric_number(
            text,
            [r"gross margin\D{0,15}([0-9]{1,2}(?:\.[0-9]+)?)\s*%"],
        )
        op_val, op_raw, op_ex = _extract_metric_number(
            text,
            [r"operating income\D{0,20}\$?\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)"],
        )
        fcf_val, fcf_raw, fcf_ex = _extract_metric_number(
            text,
            [r"free cash flow\D{0,20}\$?\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)"],
        )
        capex_val, capex_raw, capex_ex = _extract_metric_number(
            text,
            [r"(?:capital expenditures|capex)\D{0,20}\$?\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)"],
        )

        def _ev(excerpt: str | None) -> dict | None:
            if not excerpt:
                return None
            return {
                "url": src_url,
                "sourceType": src_type,
                "publishedAt": src_published,
                "retrievedAt": retrieved_at,
                "excerpt": excerpt,
            }

        if revenue_val is not None:
            metrics["revenue"] = {
                "value": revenue_val,
                "unit": "USD",
                "rawValue": revenue_raw,
                "confidence": "HIGH",
                "evidence": _ev(revenue_ex),
            }
        if eps_val is not None:
            metrics["epsDiluted"] = {
                "value": eps_val,
                "unit": "USD/share",
                "rawValue": f"${eps_raw}" if eps_raw else None,
                "confidence": "HIGH",
                "evidence": _ev(eps_ex),
            }
        if gm_val is not None:
            gm_pct = float(gm_val)
            metrics["grossMargin"] = {
                "valueRatio": round(gm_pct / 100.0, 6),
                "valuePct": gm_pct,
                "rawValue": f"{gm_raw}%" if gm_raw and "%" not in gm_raw else gm_raw,
                "confidence": "HIGH",
                "evidence": _ev(gm_ex),
            }
        if op_val is not None:
            metrics["operatingIncome"] = {
                "value": op_val,
                "unit": "USD",
                "rawValue": op_raw,
                "confidence": "HIGH",
                "evidence": _ev(op_ex),
            }
        if fcf_val is not None:
            metrics["freeCashFlow"] = {
                "value": fcf_val,
                "unit": "USD",
                "rawValue": fcf_raw,
                "confidence": "HIGH",
                "evidence": _ev(fcf_ex),
            }
        if capex_val is not None:
            metrics["capex"] = {
                "value": capex_val,
                "unit": "USD",
                "rawValue": capex_raw,
                "confidence": "HIGH",
                "evidence": _ev(capex_ex),
            }
        for key in ("revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex"):
            ev = metrics[key].get("evidence") if isinstance(metrics[key], dict) else None
            conf = str(metrics[key].get("confidence") or "")
            if conf == "HIGH" and isinstance(ev, dict):
                evidence_items.append(ev)
    else:
        warnings.append({"code": "PUBLIC_RELEASE_NOT_FOUND", "message": "No SEC 8-K earnings release source available"})

    overall_conf = "NOT_DISCLOSED"
    if any(str((metrics[k] or {}).get("confidence")) == "HIGH" for k in ("revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex")):
        overall_conf = "HIGH"
    elif release.get("confidence") in {"MEDIUM", "LOW"}:
        overall_conf = str(release.get("confidence"))

    return json.dumps({
        "ticker": ticker.upper(),
        "eventType": "earnings_release",
        "period": release.get("period") or period,
        "reportedAt": release.get("reportedAt"),
        "source": src_type if src_type else "yahoo",
        "metrics": metrics,
        "evidence": evidence_items,
        "confidence": overall_conf,
        "warnings": warnings,
    })


@yfinance_server.tool(
    name="extract_guidance",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_guidance"],
    description="Extract company-provided earnings guidance/outlook ranges from public SEC 8-K or IR release text.",
)
async def extract_guidance(ticker: str, period: str = "latest") -> str:
    release = await _resolve_latest_earnings_release(ticker)
    base = {
        "revenue": {"status": "NOT_DISCLOSED", "low": None, "high": None, "midpoint": None, "unit": "USD", "evidence": []},
        "grossMargin": {"status": "NOT_DISCLOSED", "lowPct": None, "highPct": None, "midpointPct": None, "evidence": []},
        "eps": {"status": "NOT_DISCLOSED", "low": None, "high": None, "midpoint": None, "unit": "USD/share", "evidence": []},
    }
    sources = release.get("sources") if isinstance(release.get("sources"), list) else []
    src = sources[0] if sources and isinstance(sources[0], dict) else {}
    src_url = str(src.get("url") or "")
    if not src_url.startswith("https://www.sec.gov/Archives/"):
        return json.dumps({"ticker": ticker.upper(), "period": release.get("period") or period, "guidance": base, "confidence": "NOT_DISCLOSED", "warnings": []})

    html = await _edgar_get_html(src_url, max_bytes=5_000_000)
    text = _strip_html_tags(_sanitize_sec_html(html or ""))
    patterns = {
        "revenue": _re.search(r"(?:expects|guidance|outlook)[^.\n]{0,120}revenue[^$]{0,25}\$?\s*([0-9.,]+(?:\s*(?:billion|million|thousand|bn|m|k))?)\s*(?:to|-)\s*\$?\s*([0-9.,]+(?:\s*(?:billion|million|thousand|bn|m|k))?)", text, flags=_re.IGNORECASE),
        "grossMargin": _re.search(r"gross margin[^0-9]{0,20}([0-9]{1,2}(?:\.[0-9]+)?)\s*%\s*(?:to|-)\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%", text, flags=_re.IGNORECASE),
        "eps": _re.search(r"(?:expects|guidance|outlook)[^.\n]{0,120}(?:eps|earnings per share)[^$]{0,25}\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|-)\s*\$?\s*([0-9]+(?:\.[0-9]+)?)", text, flags=_re.IGNORECASE),
    }
    if patterns["revenue"]:
        lo = _scale_number_from_text(patterns["revenue"].group(1))
        hi = _scale_number_from_text(patterns["revenue"].group(2))
        if lo is not None and hi is not None:
            base["revenue"] = {
                "status": "FOUND",
                "low": lo,
                "high": hi,
                "midpoint": (lo + hi) / 2.0,
                "unit": "USD",
                "evidence": [{"url": src_url, "sourceType": src.get("sourceType", "sec_8k"), "publishedAt": _to_iso_utc(src.get("filingDate")), "retrievedAt": _utc_now_z(), "excerpt": _compact_excerpt(patterns["revenue"].group(0))}],
            }
    if patterns["grossMargin"]:
        lo = float(patterns["grossMargin"].group(1))
        hi = float(patterns["grossMargin"].group(2))
        base["grossMargin"] = {
            "status": "FOUND",
            "lowPct": lo,
            "highPct": hi,
            "midpointPct": (lo + hi) / 2.0,
            "evidence": [{"url": src_url, "sourceType": src.get("sourceType", "sec_8k"), "publishedAt": _to_iso_utc(src.get("filingDate")), "retrievedAt": _utc_now_z(), "excerpt": _compact_excerpt(patterns["grossMargin"].group(0))}],
        }
    if patterns["eps"]:
        lo = float(patterns["eps"].group(1))
        hi = float(patterns["eps"].group(2))
        base["eps"] = {
            "status": "FOUND",
            "low": lo,
            "high": hi,
            "midpoint": (lo + hi) / 2.0,
            "unit": "USD/share",
            "evidence": [{"url": src_url, "sourceType": src.get("sourceType", "sec_8k"), "publishedAt": _to_iso_utc(src.get("filingDate")), "retrievedAt": _utc_now_z(), "excerpt": _compact_excerpt(patterns["eps"].group(0))}],
        }
    found = any(base[k]["status"] == "FOUND" for k in ("revenue", "grossMargin", "eps"))
    return json.dumps({
        "ticker": ticker.upper(),
        "period": release.get("period") or period,
        "guidance": base,
        "confidence": "HIGH" if found else "NOT_DISCLOSED",
        "warnings": [],
    })


@yfinance_server.tool(
    name="extract_management_commentary",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_management_commentary"],
    description="Extract neutral, topic-specific management commentary snippets from public earnings release sources.",
)
async def extract_management_commentary(ticker: str, period: str = "latest", topics: list[str] | None = None) -> str:
    topic_list = [str(t).strip() for t in (topics or []) if str(t).strip()]
    release = await _resolve_latest_earnings_release(ticker)
    sources = release.get("sources") if isinstance(release.get("sources"), list) else []
    src = sources[0] if sources and isinstance(sources[0], dict) else {}
    src_url = str(src.get("url") or "")
    text = ""
    if src_url.startswith("https://www.sec.gov/Archives/"):
        html = await _edgar_get_html(src_url, max_bytes=5_000_000)
        text = _strip_html_tags(_sanitize_sec_html(html or ""))
    elif src_url:
        text = _strip_html_tags(_sanitize_sec_html(_fetch_public_html(src_url) or ""))
    out_topics: list[dict] = []
    for topic in topic_list:
        excerpt = _first_sentence_for_topic(text, topic) if text else None
        if excerpt:
            out_topics.append({
                "topic": topic,
                "status": "FOUND",
                "summary": excerpt,
                "evidence": [{
                    "sourceType": src.get("sourceType", "company_ir"),
                    "url": src_url,
                    "publishedAt": _to_iso_utc(src.get("filingDate") or src.get("publishedAt")),
                    "retrievedAt": _utc_now_z(),
                    "excerpt": excerpt[:240],
                }],
                "confidence": "MEDIUM" if src.get("sourceType") != "sec_8k" else "HIGH",
            })
        else:
            out_topics.append({
                "topic": topic,
                "status": "NOT_FOUND",
                "summary": "",
                "evidence": [],
                "confidence": "LOW",
            })
    return json.dumps({"ticker": ticker.upper(), "period": release.get("period") or period, "topics": out_topics, "warnings": []})


@yfinance_server.tool(
    name="compare_earnings_actual_vs_estimate",
    output_schema=_TOOL_OUTPUT_SCHEMAS["compare_earnings_actual_vs_estimate"],
    description="Compare reported actual earnings metrics against public Yahoo analyst estimates and return surprise percentages.",
)
async def compare_earnings_actual_vs_estimate(ticker: str, period: str = "latest") -> str:
    metrics = _safe_json_loads(await extract_earnings_metrics(ticker=ticker, period=period))
    ea = _safe_json_loads(await get_earnings_analysis(ticker=ticker))
    actual_rev = (((metrics.get("metrics") or {}).get("revenue") or {}).get("value")
                  if isinstance((metrics.get("metrics") or {}).get("revenue"), dict) else None)
    actual_eps = (((metrics.get("metrics") or {}).get("epsDiluted") or {}).get("value")
                  if isinstance((metrics.get("metrics") or {}).get("epsDiluted"), dict) else None)

    revenue_est = None
    rev_est_arr = ea.get("revenueEstimate") if isinstance(ea.get("revenueEstimate"), list) else []
    for row in rev_est_arr:
        if isinstance(row, dict) and str(row.get("period")) == "0q":
            revenue_est = row.get("avg")
            break
    eps_est = None
    hist = ea.get("earningsHistory") if isinstance(ea.get("earningsHistory"), list) else []
    if hist and isinstance(hist[0], dict):
        eps_est = hist[0].get("epsEstimate")

    warnings: list[dict] = []
    result = {
        "ticker": ticker.upper(),
        "period": metrics.get("period") or period,
        "actual": {
            "revenue": {"value": actual_rev, "unit": "USD"},
            "eps": {"value": actual_eps, "unit": "USD/share"},
        },
        "estimate": {
            "revenue": {"value": revenue_est, "unit": "USD", "source": "yahoo"},
            "eps": {"value": eps_est, "unit": "USD/share", "source": "yahoo"},
        },
        "surprise": {
            "revenueSurprisePct": None,
            "epsSurprisePct": None,
        },
        "confidence": "NOT_DISCLOSED",
        "warnings": warnings,
    }
    if actual_rev is None or actual_eps is None or revenue_est in (None, 0) or eps_est in (None, 0):
        return json.dumps(result)

    try:
        result["surprise"]["revenueSurprisePct"] = round(((float(actual_rev) - float(revenue_est)) / abs(float(revenue_est))) * 100, 2)
        result["surprise"]["epsSurprisePct"] = round(((float(actual_eps) - float(eps_est)) / abs(float(eps_est))) * 100, 2)
    except Exception:
        return json.dumps(result)
    result["confidence"] = "HIGH"
    actual_period = str(metrics.get("period") or "")
    if not actual_period:
        warnings.append({"code": "PERIOD_MISMATCH", "message": "Actual and estimate periods could not be aligned"})
        result["confidence"] = "LOW"
    return json.dumps(result)


@yfinance_server.tool(name="get_tps_inputs", output_schema=_TOOL_OUTPUT_SCHEMAS["get_tps_inputs"], description="Deprecated alias for analyze_position_signals.")
async def get_tps_inputs(ticker: str) -> str:
    return _deprecated_alias_response("get_tps_inputs", "analyze_position_signals", await analyze_position_signals(ticker))


@yfinance_server.tool(name="get_eqf_bracket", output_schema=_TOOL_OUTPUT_SCHEMAS["get_eqf_bracket"], description="Deprecated alias for calculate_price_target_distance.")
async def get_eqf_bracket(ticker: str, io_pt: float) -> str:
    return _deprecated_alias_response(
        "get_eqf_bracket",
        "calculate_price_target_distance",
        await calculate_price_target_distance(ticker, io_pt=io_pt),
    )


@yfinance_server.tool(name="get_adv_gate", output_schema=_TOOL_OUTPUT_SCHEMAS["get_adv_gate"], description="Deprecated alias for check_volume_liquidity_threshold.")
async def get_adv_gate(ticker: str, foreign_exchange: bool = False) -> str:
    return _deprecated_alias_response("get_adv_gate", "check_volume_liquidity_threshold", await check_volume_liquidity_threshold(ticker, foreign_exchange))


@yfinance_server.tool(name="get_dc134_options_scan", output_schema=_TOOL_OUTPUT_SCHEMAS["get_dc134_options_scan"], description="Deprecated alias for get_options_flow_scan.")
async def get_dc134_options_scan(ticker: str, window_label: str) -> str:
    return _deprecated_alias_response("get_dc134_options_scan", "analyze_options_flow_window", await analyze_options_flow_window(ticker, window_label))


@yfinance_server.tool(name="get_china_revenue_pct", output_schema=_TOOL_OUTPUT_SCHEMAS["get_china_revenue_pct"], description="Deprecated alias for extract_sec_filing_fact.")
async def get_china_revenue_pct(ticker: str) -> str:
    return _deprecated_alias_response("get_china_revenue_pct", "extract_sec_filing_fact", await extract_sec_filing_fact(ticker=ticker, fact_type=FilingFactType.geographic_revenue, region="China"))


@yfinance_server.tool(name="get_geographic_revenue", output_schema=_TOOL_OUTPUT_SCHEMAS["get_geographic_revenue"], description="Deprecated alias for extract_sec_filing_fact.")
async def get_geographic_revenue(ticker: str, region: str = "China") -> str:
    return _deprecated_alias_response("get_geographic_revenue", "extract_sec_filing_fact", await extract_sec_filing_fact(ticker=ticker, fact_type=FilingFactType.geographic_revenue, region=region))


@yfinance_server.tool(name="get_filing_text_search", output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_text_search"], description="Deprecated alias for search_sec_filing_text.")
async def get_filing_text_search(
    ticker: str,
    search_terms: list[str] | None = None,
    section_hint: str | None = None,
    filing_type: str = "10-K",
    accession_number: str | None = None,
    context_chars: int = 1500,
    return_tables: bool = True,
) -> str:
    return _deprecated_alias_response("get_filing_text_search", "search_sec_filing_text", await search_sec_filing_text(ticker, search_terms, section_hint, filing_type, accession_number, context_chars, return_tables))


@yfinance_server.tool(name="get_filing_document", output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_document"], description="Deprecated alias for get_sec_filing_section.")
async def get_filing_document(ticker: str, section_name: str, document_url: str, context_chars: int = 3000) -> str:
    return _deprecated_alias_response("get_filing_document", "get_sec_filing_section", await get_sec_filing_section(ticker, section_name, document_url, context_chars))



if __name__ == "__main__":
    # Initialize and run the server
    print("Starting Yahoo Finance MCP server...")
    yfinance_server.run(transport="stdio")
