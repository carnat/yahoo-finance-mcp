import asyncio
import datetime
import email.utils as _email_utils
import hashlib
import html as _html_module
import functools
import inspect
import json
import math
import os
from pathlib import Path
import re as _re
import threading
import time
import sys
import urllib.parse as _urlparse
import urllib.request as _urlrequest
import urllib.error as _urlerror
import xml.etree.ElementTree as _ET
from typing import Annotated, Any, Literal, TypedDict
import zoneinfo

import pandas as pd
import yfinance as yf
from pydantic import Field

# Phase 2b: yfmcp.app owns the YfmcpServer class, the yfinance_server instance, and
# build_handler_registry.
from yfmcp.app import create_server, yfinance_server, build_handler_registry
from yfmcp.exposure_presets import CHINA as _CHINA_PRESET  # Q5: the China exposure preset, shared with the Worker
from yfmcp.schemas import (
    FinancialType, HolderType, RecommendationType, FilingFactType,
    _TOOL_OUTPUT_SCHEMAS, _MARKET_SNAPSHOT_OUTPUT_SCHEMA,
)


# ---------------------------------------------------------------------------
# Phase 1: yfmcp infrastructure imports
# ---------------------------------------------------------------------------
from yfmcp.envelope import (
    SERVER_VERSION, _ENVELOPE_V2, ErrorCode, ToolMeta, ErrorDetail, McpResponse,
    _mcp_success, _mcp_failure, _wrap_envelope_v2, _classify_error_message, normalize_status_confidence,
)
from yfmcp.validation import (
    _TICKER_RE, _ACCESSION_RE,
    _validate_ticker, _validate_accession,
    _validate_sec_url, _sanitize_sec_html,
)
from yfmcp.cache import (
    ToolCache, _tool_cache,
    TTL_PRICE, TTL_ANALYST, TTL_FINANCIALS, TTL_EDGAR, TTL_OPTIONS, TTL_NEWS,
    _PRICE_TTL, _STMT_TTL,
)
from yfmcp.pricing_notices import freshness_notices as _freshness_notices, take_notices, unique_notices, with_notices  # noqa: E402
from yfmcp.util import (
    _fetch_with_retry, get_last_trading_date,
    _PLACEHOLDER_IV_THRESHOLD, _compute_data_quality, _sort_by_relevance,
    _utc_now_iso, _to_iso_utc, _parse_rss_date,
    _filter_paragraphs_by_topics, _safe_json_loads, _compact_excerpt,
    fetch_quote_summary_module, unwrap_yahoo_values, yahoo_iso, yahoo_raw,
    _add_years, _clamp_int, _df_period_rows, _fy_number, _js_json_numbers, _nn, _now_iso, _plain, _unwrap_payload,
    _round_half_away_from_zero, _utf16_length,
)
from yfmcp.clients.yahoo import _safe_parse, _valuation_market_inputs
from yfmcp.clients.endpoints import _manual_lookup_payload, sec_archive_document_url, sec_company_concept_url, sec_company_facts_url, sec_filing_index_url
from yfmcp.clients.market_providers import _PROVIDER_USER_AGENT
from yfmcp import url_policy as _url_policy
from yfmcp import filing_search as _fs
from yfmcp import capital_structure as _cs
from yfmcp import valuation as _vl
from yfmcp import sec_facts as _sf
from yfmcp import extraction_rules as _er
from yfmcp import driver_ledger as _dl
from yfmcp import guidance_history as _gh
from yfmcp import metric_reconciliation as _mr
from yfmcp.evidence import AUTHORITY_BOUNDARY as _AUTHORITY_BOUNDARY, EPS_BASIS as _EPS_BASIS
from yfmcp.deprecations import MIN_ADV_USD_DESCRIPTION, TOOL_RELIABILITY_CLASS as _TOOL_RELIABILITY_CLASS, parse_min_adv_usd
from yfmcp.fiscal_calendar import document_fiscal_year_focus, filing_fiscal_year_label, fiscal_year_naming
from yfmcp import valuation_history as _vh
from yfmcp.clients.edgar import (
    _resolve_cik_for_ticker, _get_submissions_for_ticker,
    _EDGAR_TICKERS, _EDGAR_TICKERS_LOADED_AT, _EDGAR_TTL,
    _load_edgar_tickers, _edgar_get, EdgarError,
    _EDGAR_FACTS_CACHE, _EDGAR_SUBS_CACHE,
    _edgar_get_company_facts, _edgar_get_submissions,
    _edgar_build_filing_urls, _edgar_cik_from_accession,
    _edgar_list_exhibits_from_index, _edgar_get_html,
    _edgar_read_json, _warm_sec_submissions,
    _edgar_read_submissions, _edgar_get_document,
)
from yfmcp.parsing.html import (
    _strip_html_tags, _strip_html_blocks, _clean_filing_display_text, _looks_like_filing_markup_text,
    _parse_html_table, _parse_numeric_cell, _detect_unit_multiplier, _UNIT_SCALE_ASSUMED_WARNING,
    _MD_MAX_CELL_CHARS, _html_table_to_markdown, _html_to_markdown_fallback, _table_unit_scale,
)
from yfmcp.parsing.extractors import (
    _normalize_segment_label, _region_matches,
    _extract_geo_revenue_from_html, extract_segment_table_from_html, _geo_region_aliases, _text_contains_geo_region, _js_round,
    _extract_xbrl_latest_annual,
    extract_geo_revenue_from_html as _extract_geo_table, PRODUCT_BREAKDOWN_LEAD_RE as _PRODUCT_BREAKDOWN_LEAD_RE,
)

# ---------------------------------------------------------------------------
# Phase 2b: domain tool modules — import triggers @yfinance_server.tool registration
# ---------------------------------------------------------------------------
import yfmcp.tools.system  # noqa: F401 (side-effect import)
import yfmcp.tools.pricing  # noqa: F401 (side-effect import)
import yfmcp.tools.thai_funds  # noqa: F401 (side-effect import)
import yfmcp.tools.provider_gaps  # noqa: F401 (side-effect import)
from yfmcp.tools.system import health_check  # re-export for grouped routing
from yfmcp.tools.pricing import (  # re-export for compatibility and grouped routing
    get_historical_stock_prices,
    HistoricalPeriod,
    HistoricalInterval,
    get_fast_info,
    get_price_stats,
    get_ma_position,
    get_volume_ratio,
    get_volume_gate,
    get_technical_indicators,
    get_price_slope,
    get_short_interest,
    get_short_momentum,
    get_market_snapshot,
    _run_ticker_batch,
    _load_completed_daily_history,
    _select_completed_close_series,
    _daily_bar_date,
)
from yfmcp.tools.thai_funds import (  # re-export for grouped routing
    search_thai_funds,
    get_thai_fund_nav,
    get_thai_fund_nav_batch,
    get_thai_fund_factsheet,
    get_thai_fund_dividend_history,
)
from yfmcp.tools.provider_gaps import (  # re-export for grouped routing
    get_historical_put_call_ratio,
    get_expanded_institutional_ownership,
)


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


def _dividend_yield_ratio(info: dict) -> float | None:
    """info["dividendYield"] as a decimal ratio, the Worker's summaryDetail.dividendYield (2.26.4, F53).

    yfinance overlays Yahoo's v7 quote on summaryDetail, and the quote states the yield as a percent (AAPL 0.31 for
    0.0031). When the quote call fails the ratio stays. The scale is the one closer to dividendRate / price, or to
    trailingAnnualDividendYield; with neither, the percent of the quote is assumed.
    """
    dy = info.get("dividendYield")
    if isinstance(dy, bool) or not isinstance(dy, (int, float)):
        return dy
    as_ratio = float(dy) / 100
    rate = info.get("dividendRate")
    price = next((info.get(k) for k in ("currentPrice", "regularMarketPrice", "previousClose")
                  if isinstance(info.get(k), (int, float)) and info.get(k) > 0), None)
    reference = None
    if isinstance(rate, (int, float)) and not isinstance(rate, bool) and price:
        reference = float(rate) / float(price)
    elif isinstance(info.get("trailingAnnualDividendYield"), (int, float)) and not isinstance(info.get("trailingAnnualDividendYield"), bool):
        reference = float(info["trailingAnnualDividendYield"])
    if reference is not None and abs(float(dy) - reference) < abs(as_ratio - reference):
        return dy
    return round(as_ratio, 8)


def _info_ratio_units(info: dict) -> dict:
    """yfinance info with dividendYield as a decimal ratio (2.26.4, F53)."""
    if "dividendYield" not in info:
        return info
    return {**info, "dividendYield": _dividend_yield_ratio(info)}


async def get_stock_info(
    ticker: str | list[str],
    fields: list[str] | None = None,
    include_all: bool = False,
) -> str:
    """Get stock information for a given ticker symbol"""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, lambda t: get_stock_info(t, fields, include_all))
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.", file=sys.stderr)
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting stock information for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting stock information for {ticker}: {e}"
    info = _info_ratio_units(company.info)
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


# ---------------------------------------------------------------------------
# News & public-event domain tools - imported for registration and re-exported
# ---------------------------------------------------------------------------
import yfmcp.tools.news  # noqa: F401 (side-effect import: registers the news tools)
from yfmcp.tools.news import (  # re-export for compatibility, grouped routing and server-side callers
    _finnhub_policy,
    _finnhub_eligibility,
    _enrich_news_item_for_llm,
    _build_coverage,
    _yahoo_item_issuer,
    _coerce_max_results,
    _coerce_lookback_days,
    _normalize_event_sources,
    _source_request_fields,
    _event_type_from_keywords,
    _event_type_from_form,
    _short_text,
    _make_duplicate_group_id,
    _parse_form4_transaction,
    _source_rank,
    _safe_sec_url,
    _within_date_window,
    _load_company_ir_page_registry,
    _company_ir_page_item,
    _parse_company_ir_page_json,
    _parse_company_ir_page_html,
    _collect_company_ir_events,
    _collect_company_ir_page_events,
    _build_yahoo_event_item,
    _collect_sec_events,
    _collect_yahoo_events,
    _yahoo_search_news,
    _collect_globenewswire_events,
    _collect_finnhub_events,
    _collect_marketaux_events,
    _marketaux_fallback_reason,
    _dedupe_event_items,
    _build_collection_status,
    _compute_source_status,
    _compute_source_coverage,
    _collect_company_events,
    search_company_news,
    get_company_press_releases,
    get_sec_recent_events,
    get_public_event_timeline,
    verify_company_event,
    get_company_news,
    _GLOBENEWSWIRE_RSS_FEEDS,
    _GLOBENEWSWIRE_MAX_BYTES,
)


_PRE_REVENUE_EPS_EPSILON = 1e-9


async def get_stock_actions(ticker: str, start_date: str = "", limit: int = 40) -> str:
    """Get dividends, splits, and fund capital-gain distributions for a ticker.

    Every split is kept; dividends and capital gains are limited to the most
    recent ``limit`` on or after ``start_date`` (as the Worker does).
    """
    try:
        company = yf.Ticker(ticker)
    except Exception as e:
        print(f"Error: getting stock actions for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting stock actions for {ticker}: {e}"
    actions_df = company.actions
    actions_df = actions_df.reset_index(names="Date")
    records = json.loads(actions_df.to_json(orient="records", date_format="iso"))
    return json.dumps(_limit_corporate_actions(records, start_date, limit))


def _limit_corporate_actions(rows: list[dict], start_date: str, limit: int) -> list[dict]:
    """Every split plus the most recent ``limit`` dividend/capital-gain rows on or after start_date."""
    cap = max(1, min(1000, int(limit or 40)))
    def is_split(row: dict) -> bool:
        try:
            return float(row.get("Stock Splits") or 0) > 0
        except (TypeError, ValueError):
            return False
    payouts = [row for row in rows if not is_split(row) and (not start_date or str(row.get("Date") or "")[:10] >= start_date)]
    kept = {id(row) for row in rows if is_split(row)} | {id(row) for row in payouts[-cap:]}
    return [row for row in rows if id(row) in kept]


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
    cached = _tool_cache.get_value(cache_key)
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
            print(f"Company ticker {ticker} not found.", file=sys.stderr)
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting financial statement for {ticker}: {e}", file=sys.stderr)
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
        print(f"Error: getting financial statement for {ticker}: {e}", file=sys.stderr)
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

    _tool_cache.set(cache_key, result, _STMT_TTL)

    if line_items:
        try:
            rows = json.loads(result)
            filtered = [r for r in rows if r.get("lineItem") in line_items]
            return json.dumps(filtered)
        except Exception:
            pass
    return result


# holder_type -> quoteSummary module (the Worker's HOLDER_MOD).
_HOLDER_MOD = {
    "major_holders": "majorHoldersBreakdown",
    "institutional_holders": "institutionOwnership",
    "mutualfund_holders": "fundOwnership",
    "insider_transactions": "insiderTransactions",
    "insider_purchases": "netSharePurchaseActivity",
    "insider_roster_holders": "insiderHolders",
}


async def get_holder_info(ticker: str, holder_type: str) -> str:
    """Get holder information for a given ticker symbol"""

    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.", file=sys.stderr)
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting holder info for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting holder info for {ticker}: {e}"

    module = _HOLDER_MOD.get(holder_type)
    if module is None:
        return f"Error: invalid holder type {holder_type}. Please use one of the following: {HolderType.major_holders}, {HolderType.institutional_holders}, {HolderType.mutualfund_holders}, {HolderType.insider_transactions}, {HolderType.insider_purchases}, {HolderType.insider_roster_holders}."

    # The Worker's getHolderInfo: the quoteSummary module, with Yahoo's {raw, fmt} wrappers unwrapped (F17, 3.0.0).
    try:
        result = await asyncio.to_thread(fetch_quote_summary_module, ticker, module)
    except Exception as e:
        print(f"Error: getting holder info for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting holder info for {ticker}: {e}"
    if result is None:
        return f"Error: no data found for ticker {ticker}"
    return json.dumps(unwrap_yahoo_values(result.get(module)))


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
            print(f"Company ticker {ticker} not found.", file=sys.stderr)
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting option expiration dates for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting option expiration dates for {ticker}: {e}"
    return json.dumps(company.options)


@yfinance_server.tool(
    name="get_option_chain",
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
) -> str:
    """Fetch the option chain for a given ticker symbol, expiration date, and option type.

    Args:
        ticker: The ticker symbol of the stock
        expiration_date: The expiration date for the options chain (format: 'YYYY-MM-DD')
        option_type: The type of option to fetch ('calls' or 'puts')
        strike_min: Optional minimum strike price filter.
        strike_max: Optional maximum strike price filter.
        moneyness: "near_money" (default), "itm", "otm", or "all".

    Returns:
        str: JSON string containing the option chain data
    """

    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.", file=sys.stderr)
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting option chain for {ticker}: {e}", file=sys.stderr)
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

    effective_strike_min = strike_min
    effective_strike_max = strike_max

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


def _invalid_expiry_payload(ticker: str, requested: str, expirations: list[str]) -> dict:
    nearest = None
    if expirations:
        nearest = expirations[0]
        if _re.match(r"^\d{4}-\d{2}-\d{2}$", requested or ""):
            try:
                req_date = datetime.date.fromisoformat(requested)
                nearest = min(expirations, key=lambda d: abs((datetime.date.fromisoformat(d) - req_date).days))
            except Exception:
                nearest = expirations[0]
    return {
        "error": True,
        "code": "INVALID_EXPIRY_DATE",
        "message": f"{requested} is not in the options calendar for {ticker.upper()}",
        "ticker": ticker.upper(),
        "requestedExpiration": requested,
        "nearestExpiration": nearest,
        "validExpirations": expirations,
        "hint": "Call get_option_expiration_dates first and pass one of the returned dates.",
    }


def _us_market_date(now: datetime.datetime | None = None) -> str:
    """Today's date on the US options exchanges (America/New_York)."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    return now.astimezone(zoneinfo.ZoneInfo("America/New_York")).date().isoformat()


def _next_option_expiry(dates: list[str], today: str) -> str | None:
    """The first expiry after today's US market date (the Worker's nextOptionExpiry).
    A chain expiring today is mostly 0DTE contracts that close within hours, and
    quotes zero bids before the open, so it does not describe positioning."""
    return next((d for d in dates if d > today), dates[-1] if dates else None)


async def get_options_summary(ticker: str, expiry_hint: str | None = None) -> str:
    try:
        company = yf.Ticker(ticker)
        expirations = list(company.options or [])
        if not expirations:
            return _mcp_failure(
                "get_options_summary",
                ErrorCode.NO_OPTIONS_DATA,
                f"No options data available for {ticker.upper()}",
                meta_extra={"error_extra": {"ticker": ticker.upper()}},
            )
        today = _us_market_date()
        expiry = expiry_hint or _next_option_expiry(expirations, today)
        skipped_expiries = [] if expiry_hint else [d for d in expirations if d <= today and d != expiry]
        if expiry not in expirations:
            invalid_expiry = _invalid_expiry_payload(ticker, expiry, expirations)
            return _mcp_failure(
                "get_options_summary",
                str(invalid_expiry["code"]),
                str(invalid_expiry["message"]),
                meta_extra={
                    "error_extra": {
                        key: value
                        for key, value in invalid_expiry.items()
                        if key not in {"error", "code", "message"}
                    }
                },
            )
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
        zero_oi_contracts = 0
        total_contracts = 0
        for df in (calls, puts):
            if "openInterest" in df.columns:
                total_contracts += len(df)
                zero_oi_contracts += int((df["openInterest"].fillna(0) <= 0).sum())
        majority_zero_oi = total_contracts > 0 and zero_oi_contracts / total_contracts > 0.5
        if call_oi + put_oi <= 0 or majority_zero_oi:
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
            "expirySelection": "REQUESTED" if expiry_hint else "NEXT_AFTER_TODAY",
            "skippedExpiries": skipped_expiries,
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
        classified = _classify_error_message(str(e))
        return _mcp_failure(
            "get_options_summary",
            classified[0] if classified else ErrorCode.PROVIDER_ERROR,
            str(e),
            meta_extra={"error_extra": {"ticker": ticker.upper(), **({"retryable": True} if classified else {})}},
        )


async def get_recommendations(ticker: str, recommendation_type: str, months_back: int = 12) -> str:
    """Get recommendations or upgrades/downgrades for a given ticker symbol"""
    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.", file=sys.stderr)
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting recommendations for {ticker}: {e}", file=sys.stderr)
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
        print(f"Error: getting recommendations for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting recommendations for {ticker}: {e}"


# ---------------------------------------------------------------------------
# Group 1.1 — get_fast_info
# ---------------------------------------------------------------------------

_ANALYST_UPGRADE_GRADES = {
    "buy", "outperform", "overweight", "strong buy", "positive",
    "market outperform", "top pick",
}
_ANALYST_DOWNGRADE_GRADES = {
    "sell", "underperform", "underweight", "strong sell", "negative",
    "market underperform", "reduce",
}
_ANALYST_INITIATION_ACTIONS = {"initiated", "init", "initiation", "new coverage"}


def _classify_analyst_change(action: object, from_grade: object, to_grade: object) -> str:
    action_l = str(action or "").strip().lower()
    from_l = str(from_grade or "").strip().lower()
    to_l = str(to_grade or "").strip().lower()
    if action_l in _ANALYST_INITIATION_ACTIONS or action_l.startswith("init"):
        return "INITIATED"
    if "downgrade" in action_l or action_l == "down":
        return "DOWNGRADE"
    if "upgrade" in action_l or action_l == "up":
        return "UPGRADE"
    if to_l in _ANALYST_DOWNGRADE_GRADES and from_l not in _ANALYST_DOWNGRADE_GRADES:
        return "DOWNGRADE"
    if to_l in _ANALYST_UPGRADE_GRADES and from_l and from_l not in _ANALYST_UPGRADE_GRADES:
        return "UPGRADE"
    if to_l and not from_l:
        return "INITIATED"
    return "MAINTAIN"


def _target_analyst_count(company) -> int | None:
    """Analysts contributing price targets (Yahoo financialData.numberOfAnalystOpinions)."""
    try:
        value = (company.info or {}).get("numberOfAnalystOpinions")
    except Exception:
        return None
    return int(value) if isinstance(value, (int, float)) else None


@yfinance_server.tool(
    name="get_analyst_consensus",
    description="""Get a compact analyst consensus summary for one or more tickers.

Returns pre-aggregated data so you do NOT need to call get_recommendations separately.
Includes:
- Current market price plus consensus price targets (low, high, mean, median) and % upside to the mean target
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
        results = await _run_ticker_batch(ticker, get_analyst_consensus)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    cache_key = f"analyst_consensus:{ticker}"
    cached = _tool_cache.get_value(cache_key)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        last_price = fi.last_price
    except Exception as e:
        print(f"Error: getting analyst consensus for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting analyst consensus for {ticker}: {e}"

    output: dict = {
        "ticker": ticker,
        "source": "yahoo_finance",
        "evidenceClass": "CONTEXTUAL_MARKET_DATA",
        "decisionGrade": False,
        "recommendedNextAction": "NONE",
    }
    warnings: list[dict[str, str]] = []

    # Price targets
    target_mean = None
    try:
        targets = company.analyst_price_targets
        if targets:
            target_mean = targets.get("mean")
            output["priceTargets"] = {
                "current": last_price,
                "low": targets.get("low"),
                "high": targets.get("high"),
                "mean": target_mean,
                "median": targets.get("median"),
                # Analysts behind the price targets (not the rating counts).
                "numberOfAnalysts": _target_analyst_count(company),
                "pctUpsideFromLastPrice": (
                    round((target_mean - last_price) / last_price * 100, 2)
                    if target_mean and last_price
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
                action = row.get("Action") or row.get("action") or ""
                to_grade = row.get("ToGrade") or row.get("toGrade") or ""
                from_grade = row.get("FromGrade") or row.get("fromGrade") or ""
                if _classify_analyst_change(action, from_grade, to_grade) == "UPGRADE":
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
            output["ratingCount"] = sum(counts.values()) if counts else None
            # Deprecated names for the rating count, kept for existing callers;
            # priceTargets.numberOfAnalysts counts target contributors.
            output["numberOfAnalysts"] = output["ratingCount"]
            output["totalAnalysts"] = output["ratingCount"]
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

    pct_below_current_price = None
    if target_mean is not None and last_price is not None and last_price > 0:
        pct_below_current_price = round((last_price - target_mean) / last_price * 100, 2)

    output["currentPrice"] = last_price
    output["pctBelowCurrentPrice"] = pct_below_current_price
    output["recentUpgradeCount30d"] = recent_upgrade_count_30d
    output["targetLagSignal"] = target_lag_signal
    output["warnings"] = warnings

    result = json.dumps(output)
    _tool_cache.set(cache_key, result, _STMT_TTL)
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
- epsRevisions: Analyst count changes (up/down) over the last 7/30 days
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
    cached = _tool_cache.get_value(cache_key)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        if fi.currency is None:
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting earnings analysis for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting earnings analysis for {ticker}: {e}"

    def _df_to_records(df):
        if df is None or (hasattr(df, "empty") and df.empty):
            return None
        df = df.reset_index()
        df.columns = [str(c) for c in df.columns]
        df = df.where(pd.notnull(df), None)
        records = df.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                if isinstance(value, (pd.Timestamp, datetime.datetime, datetime.date)):
                    record[key] = value.isoformat()
        return records

    output: dict = {
        "ticker": ticker,
        "source": "yahoo_finance",
        "evidenceClass": "CONTEXTUAL_MARKET_DATA",
        "decisionGrade": False,
        "recommendedNextAction": "NONE",
    }
    for key, attr in [
        ("earningsEstimate", "earnings_estimate"),
        ("revenueEstimate", "revenue_estimate"),
        ("epsTrend", "eps_trend"),
        ("epsRevisions", "eps_revisions"),
        ("earningsHistory", "earnings_history"),
        ("growthEstimates", "growth_estimates"),
    ]:
        try:
            output[key] = _df_to_records(getattr(company, attr))
        except Exception:
            output[key] = None
    # Yahoo's surprisePercent is a decimal ratio (0.0452 = 4.52%); surprisePct is in percent.
    for row in output.get("earningsHistory") or []:
        if isinstance(row, dict):
            ratio = row.get("surprisePercent")
            row["surprisePct"] = round(float(ratio) * 100, 2) if isinstance(ratio, (int, float)) else None
    output["unitSemantics"] = {
        "decimalRatios": ["earningsEstimate[].growth", "revenueEstimate[].growth", "earningsHistory[].surprisePercent"],
        "percentValues": ["earningsHistory[].surprisePct"],
    }
    output["epsBasis"] = _EPS_BASIS

    result = json.dumps(output)
    _tool_cache.set(cache_key, result, _STMT_TTL)
    return result


# ---------------------------------------------------------------------------
# Group 2.4 — get_financial_ratios
# ---------------------------------------------------------------------------

async def get_financial_ratios(
    ticker: str | list[str],
    history_periods: int = 0,
    frequency: Literal["quarterly", "monthly", "yearly", "trailing"] = "quarterly",
) -> str:
    """Get pre-computed key financial ratios."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, lambda t: get_financial_ratios(t, history_periods, frequency))
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    history_periods = max(0, min(int(history_periods), 20))
    if frequency not in {"quarterly", "monthly", "yearly", "trailing"}:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "frequency must be quarterly, monthly, yearly, or trailing"})
    cache_key = f"financial_ratios:{ticker}:{history_periods}:{frequency}"
    cached = _tool_cache.get_value(cache_key)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        info = _info_ratio_units(company.info)
    except Exception as e:
        print(f"Error: getting financial ratios for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting financial ratios for {ticker}: {e}"

    def _get(key):
        return info.get(key)

    market_cap = _get("marketCap")
    free_cashflow = _get("freeCashflow")

    ratios: dict = {
        "ticker": ticker,
        "source": "yahoo_finance",
        "evidenceClass": "CONTEXTUAL_MARKET_DATA",
        "decisionGrade": False,
        "recommendedNextAction": "NONE",
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
    # Quote and financial currencies must match for value-over-results multiples.
    quote_currency = _vl._major_price(1, _get("currency"))[1] if isinstance(_get("currency"), str) else None
    financial_currency = ratios.get("currency") if isinstance(ratios.get("currency"), str) else None
    if quote_currency and financial_currency and quote_currency != financial_currency:
        withheld = {}
        for key in ("priceToSales", "priceToBook", "enterpriseToEbitda", "enterpriseToRevenue", "freeCashflowYield"):
            withheld[key] = ratios.get(key)
            ratios[key] = None
        ratios["quoteCurrency"] = quote_currency
        ratios["withheldMultiples"] = withheld
        ratios["warnings"] = [{"code": "CURRENCY_MISMATCH_MULTIPLES_WITHHELD", "message": "Yahoo's price/sales, price/book and EV multiples, and the free-cash-flow yield, divide a quote-currency market value by financial-currency results; they are withheld when the two currencies differ (TSM: USD ADR, TWD financials). P/E stays, since Yahoo reports EPS per quoted share.", "severity": "warning"}]
    ratios["unitSemantics"] = {
        "currency": ratios.get("currency"),
        "multiples": [
            "trailingPE", "forwardPE", "pegRatio", "priceToSales", "priceToBook",
            "enterpriseToEbitda", "enterpriseToRevenue", "currentRatio", "quickRatio",
        ],
        "decimalRatios": [
            "grossMargins", "operatingMargins", "profitMargins", "returnOnEquity",
            "returnOnAssets", "dividendYield", "payoutRatio", "earningsGrowth", "revenueGrowth",
        ],
        "percentValues": ["debtToEquity", "freeCashflowYield"],
        "currencyValues": ["freeCashflow"],
    }

    if history_periods:
        label_map = {
            "Market Cap": "marketCap",
            "Enterprise Value": "enterpriseValue",
            "Trailing P/E": "trailingPE",
            "Forward P/E": "forwardPE",
            "PEG Ratio (5yr expected)": "pegRatio",
            "Price/Sales": "priceToSales",
            "Price/Book": "priceToBook",
            "Enterprise Value/Revenue": "enterpriseToRevenue",
            "Enterprise Value/EBITDA": "enterpriseToEbitda",
        }
        def _valuation_number(value: Any) -> float | int | None:
            if value is None or pd.isna(value):
                return None
            if isinstance(value, (int, float)):
                return value
            text = str(value).strip().replace(",", "")
            if not text or text in {"-", "N/A"}:
                return None
            multiplier = 1.0
            if text[-1:].upper() in {"K", "M", "B", "T"}:
                multiplier = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[text[-1].upper()]
                text = text[:-1]
            try:
                return float(text.rstrip("%")) * multiplier
            except ValueError:
                return None
        try:
            try:
                valuation = company.get_valuation_measures(freq=frequency, periods=history_periods)
            except TypeError:
                valuation = company.get_valuation_measures()
            history: list[dict[str, Any]] = []
            if valuation is not None and not valuation.empty:
                for period in list(valuation.columns)[:history_periods]:
                    row: dict[str, Any] = {"period": str(period)}
                    for source_label, output_key in label_map.items():
                        value = valuation.at[source_label, period] if source_label in valuation.index else None
                        row[output_key] = _valuation_number(value)
                    history.append(row)
            ratios["valuationHistory"] = history
            ratios["valuationFrequency"] = frequency
            ratios["historyPeriodsRequested"] = history_periods
        except Exception as e:
            ratios["valuationHistory"] = None
            ratios["valuationHistoryStatus"] = "PROVIDER_ERROR"
            ratios["valuationHistoryWarning"] = str(e)

    result = json.dumps(ratios)
    _tool_cache.set(cache_key, result, _STMT_TTL)
    return result


# ---------------------------------------------------------------------------
# Group 2.5 — get_calendar
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="analyze_share_count_trend",
    description="""Analyze historical shares outstanding for dilution, issuance, or buyback questions.

Use this tool whenever the user asks whether a company diluted shareholders, issued shares,
repurchased stock, or changed its share count. Yahoo share-count history is contextual evidence;
material changes should be confirmed in SEC filings before a decision.
""",
)
async def analyze_share_count_trend(
    ticker: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    try:
        end = datetime.date.fromisoformat(end_date) if end_date else datetime.date.today()
        start = datetime.date.fromisoformat(start_date) if start_date else end - datetime.timedelta(days=5 * 365)
    except ValueError:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "dates must use YYYY-MM-DD"})
    if start >= end:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "start_date must be before end_date"})
    try:
        shares = yf.Ticker(ticker).get_shares_full(start=start.isoformat(), end=end.isoformat())
    except Exception as e:
        return json.dumps({"error": True, "code": "PROVIDER_ERROR", "message": str(e), "ticker": ticker})
    if shares is None or shares.empty:
        return json.dumps({
            "ticker": ticker, "status": "EMPTY_RESULT", "observations": [],
            "source": "yahoo_finance", "evidenceClass": "CONTEXTUAL_MARKET_DATA",
            "decisionGrade": False, "recommendedNextAction": "CHECK_SEC_FILINGS",
        })
    shares = shares.dropna().sort_index()
    if shares.index.has_duplicates:
        shares = shares.groupby(level=0).last()
    if shares.empty:
        return json.dumps({
            "ticker": ticker, "status": "EMPTY_RESULT", "observations": [],
            "source": "yahoo_finance", "evidenceClass": "CONTEXTUAL_MARKET_DATA",
            "decisionGrade": False, "recommendedNextAction": "CHECK_SEC_FILINGS",
        })
    compact: list[dict[str, Any]] = []
    previous = object()
    for index, value in shares.items():
        numeric = float(value)
        if numeric == previous:
            continue
        compact.append({"date": index.isoformat(), "shares": numeric})
        previous = numeric
    first = compact[0]["shares"]
    current = compact[-1]["shares"]
    change = current - first
    change_pct = round(change / first * 100, 4) if first else None
    observations = compact[-60:]
    return json.dumps({
        "ticker": ticker,
        "status": "OK",
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "dataDate": compact[-1]["date"],
        "firstShares": first,
        "currentShares": current,
        "changeShares": change,
        "changePct": change_pct,
        "sampleCount": len(compact),
        "returnedSampleCount": len(observations),
        "truncated": len(compact) > len(observations),
        "observations": observations,
        "source": "yahoo_finance",
        "evidenceClass": "CONTEXTUAL_MARKET_DATA",
        "decisionGrade": False,
        "interpretation": "Share-count changes may reflect issuance, buybacks, splits, or provider revisions; confirm material changes in company filings.",
        "recommendedNextAction": "CHECK_SEC_FILINGS" if change else "NONE",
    })


async def get_calendar(ticker: str, mode: Literal["upcoming", "history"] = "upcoming", limit: int = 12, offset: int = 0) -> str:
    """Get upcoming earnings and dividend calendar for a ticker."""
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    if mode not in {"upcoming", "history"}:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "mode must be upcoming or history"})
    cache_key = f"calendar:{ticker}:{mode}:{limit}:{offset}"
    cached = _tool_cache.get_value(cache_key)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        if fi.currency is None:
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting calendar for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting calendar for {ticker}: {e}"

    if mode == "history":
        try:
            dates = company.get_earnings_dates(limit=limit, offset=offset)
        except Exception as e:
            return json.dumps({"error": True, "code": "PROVIDER_ERROR", "message": str(e), "ticker": ticker})
        items = _df_to_records(dates) or []
        return json.dumps({
            "ticker": ticker, "mode": mode, "status": "OK" if items else "EMPTY_RESULT",
            "items": items, "limit": limit, "offset": offset,
            "hasMore": len(items) == limit, "source": "yahoo_finance",
            "sourceType": "provider_calendar", "evidenceClass": "CONTEXTUAL_MARKET_DATA",
            "providerMethod": "YAHOO_CALENDAR_HTML",
            "confirmationStatus": "UNVERIFIED", "decisionGrade": False,
            "recommendedNextAction": "NONE",
        })

    # The Worker's getCalendar: quoteSummary calendarEvents, with the Worker's calendar keys (F47, 3.0.0).
    try:
        quote_result = await asyncio.to_thread(fetch_quote_summary_module, ticker, "calendarEvents")
    except Exception as e:
        print(f"Error: getting calendar for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting calendar for {ticker}: {e}"
    if quote_result is None:
        return f"Error: no data found for ticker {ticker}"

    cal = quote_result.get("calendarEvents") or {}
    earnings = cal.get("earnings") or {}
    earnings_dates = [
        yahoo_iso(d["raw"]) if isinstance(d, dict) and d.get("raw") is not None else None
        for d in (earnings.get("earningsDate") or [])
    ]
    ed_dates = [d for d in earnings_dates if d]
    ex_div = cal.get("exDividendDate")
    div_date = cal.get("dividendDate")

    output = {
        "ticker": ticker,
        "mode": mode,
        "earningsDateConfirmed": False,
        "earningsDateSource": "YAHOO_CALENDAR" if ed_dates else "UNKNOWN",
        "confirmationStatus": "UNVERIFIED",
        "source": "yahoo_finance",
        "sourceType": "provider_calendar",
        "evidenceClass": "CONTEXTUAL_MARKET_DATA",
        "decisionGrade": False,
        "recommendedNextAction": "CHECK_OFFICIAL_RELEASES" if ed_dates else "NONE",
        "calendar": {
            "earnings": {
                "earningsDate": earnings_dates,
                "earningsAverage": yahoo_raw(earnings.get("earningsAverage")),
                "earningsLow": yahoo_raw(earnings.get("earningsLow")),
                "earningsHigh": yahoo_raw(earnings.get("earningsHigh")),
                "revenueAverage": yahoo_raw(earnings.get("revenueAverage")),
                "revenueLow": yahoo_raw(earnings.get("revenueLow")),
                "revenueHigh": yahoo_raw(earnings.get("revenueHigh")),
            },
            "exDividendDate": yahoo_iso(ex_div["raw"]) if isinstance(ex_div, dict) and ex_div.get("raw") is not None else None,
            "dividendDate": yahoo_iso(div_date["raw"]) if isinstance(div_date, dict) and div_date.get("raw") is not None else None,
        },
    }
    result = json.dumps(output)
    _tool_cache.set(cache_key, result, _PRICE_TTL)
    return result


@yfinance_server.tool(
    name="get_market_calendar",
    description="""Get a market-wide Yahoo calendar for earnings, economic events, IPOs, or stock splits.

Use this tool when the user asks which companies report this week, what macro releases are due,
which IPOs are scheduled, or which stocks are splitting. Results are paginated provider data,
not official-company confirmation.
""",
)
async def get_market_calendar(
    event_type: Literal["earnings", "economic", "ipo", "splits"] = "earnings",
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 25,
    offset: int = 0,
) -> str:
    try:
        start = datetime.date.fromisoformat(start_date) if start_date else datetime.date.today()
        end = datetime.date.fromisoformat(end_date) if end_date else start + datetime.timedelta(days=7)
    except ValueError:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "dates must use YYYY-MM-DD"})
    if start > end or (end - start).days > 31:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "date range must be 0 to 31 days"})
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    methods = {
        "earnings": "get_earnings_calendar",
        "economic": "get_economic_events_calendar",
        "ipo": "get_ipo_info_calendar",
        "splits": "get_splits_calendar",
    }
    if event_type not in methods:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "event_type must be earnings, economic, ipo, or splits"})
    try:
        calendars = yf.Calendars(start=start.isoformat(), end=end.isoformat())
        method = getattr(calendars, methods[event_type])
        kwargs = {"limit": limit, "offset": offset}
        if event_type == "earnings":
            kwargs["filter_most_active"] = False
        frame = method(**kwargs)
        items = _df_to_records(frame) or []
        for item in items:
            normalized = {
                _re.sub(r"[^a-z]", "", str(key).lower()): key
                for key in item
            }
            actual_key = normalized.get("epsactual") or normalized.get("reportedeps")
            if actual_key is not None and item.get(actual_key) is None:
                for normalized_key, original_key in normalized.items():
                    if "surprise" in normalized_key:
                        item[original_key] = None
    except Exception as e:
        return json.dumps({"error": True, "code": "PROVIDER_ERROR", "message": str(e), "eventType": event_type})
    return json.dumps({
        "status": "OK" if items else "EMPTY_RESULT",
        "eventType": event_type, "startDate": start.isoformat(), "endDate": end.isoformat(),
        "limit": limit, "offset": offset, "itemCount": len(items), "hasMore": len(items) == limit,
        "coverage": {"state": "PARTIAL" if len(items) == limit else "FULL", "truncated": len(items) == limit},
        "items": items, "source": "yahoo_finance", "sourceType": "provider_calendar",
        "evidenceClass": "CONTEXTUAL_MARKET_DATA", "confirmationStatus": "UNVERIFIED",
        "decisionGrade": False, "recommendedNextAction": "CHECK_OFFICIAL_RELEASES" if event_type in {"earnings", "ipo", "splits"} else "NONE",
    })


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
        Optional exchange filter. Pass "US" to restrict results to US listing venues (NMS, NGM, NCM:
        Nasdaq; NYQ: NYSE; ASE: NYSE American; PCX: NYSE Arca) — use this for small/mid-cap US equity searches that may otherwise return foreign listings or
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
                # Every US listing venue, the same set in both runtimes (R5-pricing-1, Q3i call 24): Nasdaq Global Select/Global/Capital Market,
                # NYSE, NYSE American, NYSE Arca.
                _us = {"NMS", "NGM", "NCM", "NYQ", "ASE", "PCX"}
                trimmed = [r for r in trimmed if r.get("exchange") in _us]
            else:
                trimmed = [r for r in trimmed if r.get("exchange") == exch_upper]
        return json.dumps(trimmed)
    except Exception as e:
        print(f"Error: searching for '{query}': {e}", file=sys.stderr)
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
        print(f"Error: running screener '{screener_name}': {e}", file=sys.stderr)
        return f"Error: running screener '{screener_name}': {e}"


# ---------------------------------------------------------------------------
# Group 3.5 — get_technical_indicators
# ---------------------------------------------------------------------------

async def get_credit_health(ticker: str | list[str]) -> str:
    """Return credit health metrics for one or more tickers."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, get_credit_health)
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

    # Balance sheet: single most-recent quarter (point-in-time, no TTM needed)
    bs_col = bs.columns[0]

    # Income statement: up to 4 most-recent quarters for TTM
    inc_cols = list(inc.columns[:4])
    n_inc_quarters = len(inc_cols)

    def _safe_get(df, col, *row_names):
        for name in row_names:
            try:
                val = df.loc[name, col]
                if pd.notna(val):
                    return float(val)
            except (KeyError, TypeError):
                continue
        return None

    def _ttm_sum(df, cols, *row_names):
        """TTM sum of the first matching row. Returns (total, source_row, n_quarters_used).

        A trailing-twelve-month figure needs a value in each of the 4 newest quarters (R5-fundamentals): a row
        that has fewer is not a year, so the total is None and the component is reported missing.
        """
        for name in row_names:
            try:
                vals = []
                for col in cols:
                    try:
                        v = df.loc[name, col]
                        if pd.notna(v):
                            vals.append(float(v))
                    except (KeyError, TypeError):
                        pass
                if vals:
                    if len(cols) < 4 or len(vals) < len(cols):
                        return None, None, len(vals)
                    return sum(vals), str(name), len(vals)
            except Exception:
                continue
        return None, None, 0

    def _ttm_quarterly_vals(df, cols, *row_names):
        """Return per-quarter list (newest first) for the first matching row."""
        for name in row_names:
            try:
                vals = []
                for col in cols:
                    try:
                        v = df.loc[name, col]
                        if pd.notna(v):
                            vals.append(float(v))
                    except (KeyError, TypeError):
                        pass
                if vals:
                    return vals
            except Exception:
                continue
        return []

    # Balance sheet values (point-in-time)
    # Total debt only: long-term debt alone leaves out current debt and would understate it (R5-fundamentals).
    total_debt = _safe_get(bs, bs_col, "Total Debt", "TotalDebt")
    cash = _safe_get(bs, bs_col, "Cash And Cash Equivalents", "CashAndCashEquivalents", "Cash")

    # Income statement: TTM sums (sum of up to 4 most-recent quarters)
    ebitda_ttm, ebitda_source_row, _ = _ttm_sum(inc, inc_cols, "EBITDA", "Normalized EBITDA", "NormalizedEBITDA")
    # BUG-03: prefer operatingIncome over Yahoo's EBIT field, which can include non-cash
    # non-operating items (warrant fair value changes, convertible note adjustments)
    ebit_ttm, ebit_source_row, _ = _ttm_sum(
        inc, inc_cols, "Operating Income", "OperatingIncome", "EBIT"
    )
    da_ttm, da_source_row, _ = _ttm_sum(
        inc,
        inc_cols,
        "Reconciled Depreciation",
        "ReconciledDepreciation",
        "Depreciation And Amortization",
        "DepreciationAndAmortization",
        "Depreciation Amortization Depletion Income Statement",
        "DepreciationAmortizationDepletionIncomeStatement",
    )
    interest_ttm, interest_source_row, _ = _ttm_sum(
        inc,
        inc_cols,
        "Interest Expense Non Operating",
        "InterestExpenseNonOperating",
        "Interest Expense",
        "InterestExpense",
    )
    interest_quarterly_vals = _ttm_quarterly_vals(
        inc,
        inc_cols,
        "Interest Expense Non Operating",
        "InterestExpenseNonOperating",
        "Interest Expense",
        "InterestExpense",
    )

    net_debt = (total_debt - cash) if total_debt is not None and cash is not None else None

    # Prefer operating income + D&A (TTM) over provider EBITDA
    operational_ebitda_annual = None
    operational_ebitda_source = None
    if ebit_ttm is not None and da_ttm is not None:
        operational_ebitda_annual = ebit_ttm + da_ttm
        operational_ebitda_source = "ttm_operating_income_plus_da"
    elif ebitda_ttm is not None:
        operational_ebitda_annual = ebitda_ttm
        operational_ebitda_source = "provider_ebitda_fallback"

    # TTM sums are the annualized figures — no × 4 multiplication needed
    ebitda_annual = ebitda_ttm
    ebit_annual = ebit_ttm
    depreciation_amortization_annual = da_ttm
    interest_annual = interest_ttm

    # Flag partial data quality when fewer than 4 income quarters are available
    if n_inc_quarters < 4:
        data_quality = "PARTIAL"

    # Leverage needs a positive earnings base: over zero or negative EBITDA the ratio has no meaning (a loss-maker
    # with net debt would read as a negative ratio and the CLEAN tier), so it is withheld with the tier and the
    # stress flag; NEGATIVE_EARNINGS_BASE says why (R5-fundamentals).
    net_debt_to_ebitda = (
        round(net_debt / operational_ebitda_annual, 2)
        if net_debt is not None and operational_ebitda_annual is not None and operational_ebitda_annual > 0 else None
    )
    interest_coverage_ebit = round(ebit_annual / abs(interest_annual), 2) if ebit_annual is not None and interest_annual and interest_annual != 0 else None
    interest_coverage_ebitda = round(operational_ebitda_annual / abs(interest_annual), 2) if operational_ebitda_annual is not None and interest_annual and interest_annual != 0 else None
    interest_coverage = interest_coverage_ebit

    credit_stress = None
    if net_debt_to_ebitda is not None and interest_coverage_ebit is not None:
        credit_stress = net_debt_to_ebitda > 2.5 and interest_coverage_ebit < 3

    if net_debt_to_ebitda is not None:
        if net_debt_to_ebitda < 1:
            debt_tier = "CLEAN"
        elif net_debt_to_ebitda <= 2.5:
            debt_tier = "MODERATE"
        elif net_debt_to_ebitda <= 4:
            debt_tier = "ELEVATED"
        else:
            debt_tier = "STRESSED"
    elif net_debt is not None and operational_ebitda_annual is not None and operational_ebitda_annual < 0:
        # A loss-maker has no leverage tier: the state is named instead of reading as CLEAN (R5-fundamentals).
        debt_tier = "NEGATIVE_EARNINGS_BASE"
    else:
        debt_tier = None

    missing_components = []
    if total_debt is None:
        missing_components.append("totalDebtUsd")
    if cash is None:
        missing_components.append("cashUsd")
    if ebitda_annual is None:
        missing_components.append("ebitdaUsd")
    if operational_ebitda_annual is None:
        missing_components.append("operationalEbitdaUsd")
    if ebit_annual is None:
        missing_components.append("ebitUsd")
    if interest_annual is None:
        missing_components.append("interestExpenseUsd")

    unavailable_metrics = []
    if net_debt_to_ebitda is None:
        unavailable_metrics.append("netDebtToEbitda")
    if interest_coverage is None:
        unavailable_metrics.append("interestCoverage")
    if interest_coverage_ebit is None:
        unavailable_metrics.append("interestCoverageEbit")
    if interest_coverage_ebitda is None:
        unavailable_metrics.append("interestCoverageEbitda")
    if credit_stress is None:
        unavailable_metrics.append("creditStressFlag")

    computed_metrics = []
    if net_debt is not None:
        computed_metrics.append("netDebtUsd")
    if operational_ebitda_annual is not None:
        computed_metrics.append("operationalEbitdaUsd")
    if net_debt_to_ebitda is not None:
        computed_metrics.append("netDebtToEbitda")
    if interest_coverage is not None:
        computed_metrics.append("interestCoverage")
    if interest_coverage_ebit is not None:
        computed_metrics.append("interestCoverageEbit")
    if interest_coverage_ebitda is not None:
        computed_metrics.append("interestCoverageEbitda")
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
    # BUG-02: flag when most-recent quarter interest is anomalously large vs prior quarters
    if len(interest_quarterly_vals) >= 2:
        most_recent_q = abs(interest_quarterly_vals[0])
        prior_abs = [abs(v) for v in interest_quarterly_vals[1:]]
        prior_avg = sum(prior_abs) / len(prior_abs)
        if prior_avg > 0 and most_recent_q > prior_avg * 2.0:
            ratio = most_recent_q / prior_avg
            warnings.append({
                "code": "INTEREST_EXPENSE_ANOMALY",
                "message": (
                    f"Most recent quarter interest expense ({most_recent_q:,.0f}) is "
                    f"{ratio:.1f}× prior {len(prior_abs)}-quarter average ({prior_avg:,.0f}). "
                    "May include one-time items. Coverage ratios may not reflect ongoing debt service capacity."
                ),
                "mostRecentQuarter": most_recent_q,
                "prior3QAverage": prior_avg,
            })
    if operational_ebitda_source == "provider_ebitda_fallback":
        warnings.append({
            "code": "OPERATIONAL_EBITDA_UNAVAILABLE",
            "message": "Operational EBITDA could not be computed from EBIT plus depreciation and amortization; provider EBITDA is used as a fallback.",
        })
    if ebitda_annual is not None and operational_ebitda_annual is not None:
        basis = max(abs(operational_ebitda_annual), 1.0)
        if abs(ebitda_annual - operational_ebitda_annual) / basis >= 0.25 and abs(ebitda_annual - operational_ebitda_annual) >= 100_000_000:
            warnings.append({
                "code": "NON_OPERATING_EBITDA_DIVERGENCE",
                "message": "Provider EBITDA materially differs from EBIT plus depreciation and amortization; leverage metrics use operational EBITDA.",
            })
    if (operational_ebitda_annual is not None and operational_ebitda_annual < 0) or (ebit_annual is not None and ebit_annual < 0):
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
        "ebitdaSource": ebitda_source_row,
        "operationalEbitdaUsd": operational_ebitda_annual,
        "operationalEbitdaSource": operational_ebitda_source,
        "depreciationAmortizationUsd": depreciation_amortization_annual,
        "depreciationAmortizationSource": da_source_row,
        "ebitUsd": ebit_annual,
        "interestExpenseUsd": interest_annual,
        "interestExpenseSource": interest_source_row,
        "netDebtToEbitda": net_debt_to_ebitda,
        "interestCoverage": interest_coverage,
        "interestCoverageEbit": interest_coverage_ebit,
        "interestCoverageEbitda": interest_coverage_ebitda,
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
def _beat_streak_quarter(value) -> str | None:
    """A Yahoo earnings-history quarter as YYYY-MM-DD, or None when it is missing or unreadable."""
    if value is None:
        return None
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(stamp) else stamp.strftime("%Y-%m-%d")


async def get_earnings_momentum(ticker: str | list[str]) -> str:
    """Return earnings momentum for one or more tickers."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, get_earnings_momentum)
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
    actual_eps_values: list[float] = []

    if earnings_history_records:
        for row in earnings_history_records:
            actual = row.get("epsActual")
            estimate = row.get("epsEstimate")
            surprise_pct = row.get("surprisePercent")
            if actual is not None and estimate is not None:
                try:
                    actual_eps_values.append(float(actual))
                except Exception:
                    pass
                total_quarters += 1
                if actual > estimate:
                    beat_count += 1
                if surprise_pct is not None:
                    # Yahoo's surprisePercent is a decimal ratio at every size (-2.337 is
                    # -233.7%); scaling only values under 1 turned ASTS's -117.5% into -27.0%.
                    surprises.append(float(surprise_pct) * 100)

        # Beat streak: consecutive beats counted from the most recent quarter. Yahoo lists quarters oldest first,
        # so rows are ordered by their quarter date, newest first; when a counted row has no quarter date the
        # order is unknown and the streak is withheld (R5-fundamentals, BR-SHR-010).
        counted = [row for row in earnings_history_records
                   if row.get("epsActual") is not None and row.get("epsEstimate") is not None]
        dated = [(_beat_streak_quarter(row.get("quarter")), row) for row in counted]
        if any(d is None for d, _ in dated):
            beat_streak = None
        else:
            for _, row in sorted(dated, key=lambda item: item[0], reverse=True):
                if row["epsActual"] > row["epsEstimate"]:
                    beat_streak += 1
                else:
                    break

    beat_rate = round(beat_count / total_quarters, 2) if total_quarters > 0 else None
    avg_surprise = round(sum(surprises) / len(surprises), 2) if surprises else None
    pre_revenue = (total_quarters == 0 and not earnings_history_records) or (
        bool(actual_eps_values) and all(abs(v) < _PRE_REVENUE_EPS_EPSILON for v in actual_eps_values)
    )
    if pre_revenue:
        momentum_flag = "NO_HISTORY"
        avg_surprise = None
        warnings.append({
            "code": "PRE_REVENUE_NO_HISTORY",
            "message": "Earnings history appears pre-revenue or unavailable; momentum fields are not reliable.",
        })

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

    revision_driver = "none"
    if revision_30d is not None:
        revision_driver = "30d"
        if revision_30d <= -3:
            forward_revision_signal = "NEGATIVE"
        elif revision_30d >= 3:
            forward_revision_signal = "POSITIVE"
        elif revision_7d is not None and revision_7d <= -3:
            revision_driver = "30d_neutral_7d"
            forward_revision_signal = "NEGATIVE"
        elif revision_7d is not None and revision_7d >= 3:
            revision_driver = "30d_neutral_7d"
            forward_revision_signal = "POSITIVE"
        else:
            forward_revision_signal = "NEUTRAL"
    elif revision_7d is not None:
        revision_driver = "7d"
        if revision_7d <= -3:
            forward_revision_signal = "NEGATIVE"
        elif revision_7d >= 3:
            forward_revision_signal = "POSITIVE"
        else:
            forward_revision_signal = "NEUTRAL"
    elif revision_90d is not None:
        revision_driver = "90d_fallback"
        if revision_90d <= -3:
            forward_revision_signal = "NEGATIVE"
        elif revision_90d >= 3:
            forward_revision_signal = "POSITIVE"
        else:
            forward_revision_signal = "NEUTRAL"
    else:
        forward_revision_signal = "UNKNOWN"

    composite_method_note = (
        "Forward revision signal uses 30d revision as primary, 7d as confirmation when 30d is neutral or missing, "
        "and 90d only as fallback/context; a negative 90d revision does not override positive recent revisions."
    )
    if revision_30d is not None and revision_30d >= 3 and revision_90d is not None and revision_90d <= -3:
        warnings.append({
            "code": "LONGER_LOOKBACK_REVISION_DIVERGENCE",
            "message": "Recent EPS revisions are positive while the 90d revision remains negative.",
        })

    mixed_negative_revision = beat_rate is not None and beat_rate >= 0.75 and any(
        r is not None and r <= -3 for r in (revision_30d, revision_7d)
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
        "preRevenue": pre_revenue,
        "currentBeatStreak": beat_streak,
        "historicalSurpriseSignal": historical_surprise_signal,
        "forwardRevisionSignal": forward_revision_signal,
        "compositeMomentumSignal": composite_momentum_signal,
        "interpretationNote": interpretation_note_map[composite_momentum_signal],
        "compositeMethodNote": composite_method_note,
        "revisionSignalDriver": revision_driver,
        "warnings": warnings,
        "dataQuality": data_quality,
        "dataDate": get_last_trading_date(),
    })
    return json.dumps(output)


# ---------------------------------------------------------------------------
# Tool: get_options_flow_summary
# ---------------------------------------------------------------------------

async def get_options_flow_summary(ticker: str, expiry_hint: str | None = None) -> str:
    # Consolidated naming: route to the same payload implementation as get_options_summary.
    return await get_options_summary(ticker, expiry_hint=expiry_hint)


# ---------------------------------------------------------------------------
# Tool: get_put_hedge_candidates
# ---------------------------------------------------------------------------

async def get_put_hedge_candidates(
    ticker: str,
    otm_pct_min: float = 8.0,
    otm_pct_max: float = 12.0,
    budget_usd: float = 500.0,
    expiry_after: str = "",
) -> str:
    """Return filtered put hedge candidates."""
    if budget_usd <= 0:
        return json.dumps({
            "error": True,
            "code": "INPUT_VALIDATION_ERROR",
            "message": "budget_usd must be greater than zero",
            "ticker": ticker,
        })
    if otm_pct_min < 0 or otm_pct_max < otm_pct_min:
        return json.dumps({
            "error": True,
            "code": "INPUT_VALIDATION_ERROR",
            "message": "otm_pct_min and otm_pct_max must define a non-negative ascending range",
            "ticker": ticker,
        })
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

    # Filter expiries >= expiry_after. Without it, skip expiries less than a
    # week out (as the Worker does): they rarely suit a hedge and often have
    # no quotes. When every expiry is under a week out the screen fails closed
    # instead of returning the contracts it says it skips (R5-pricing-1, Q3i call 23).
    if expiry_after:
        qualifying_expiries = [e for e in expirations if e >= expiry_after]
    else:
        default_after = (datetime.datetime.utcnow() + datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        qualifying_expiries = [e for e in expirations if e >= default_after]

    # Select nearest 2
    qualifying_expiries = qualifying_expiries[:2]

    if not qualifying_expiries:
        return json.dumps({"error": True, "message": "No qualifying expiry dates", "ticker": ticker})

    strike_min = current_price * (1 - otm_pct_max / 100)
    strike_max = current_price * (1 - otm_pct_min / 100)

    candidates = []
    considered_contracts: list[dict[str, Any]] = []
    excluded_contracts = {
        "noTwoSidedQuote": 0,
        "noLiquidityEvidence": 0,
    }
    for exp in qualifying_expiries:
        try:
            chain = company.option_chain(exp)
            puts_df = chain.puts
        except Exception:
            continue

        # Filter strikes
        filtered = puts_df[(puts_df["strike"] >= strike_min) & (puts_df["strike"] <= strike_max)]
        considered_contracts.extend(
            json.loads(filtered.to_json(orient="records", date_format="iso"))
        )

        # Collect IVs for percentile calculation
        all_ivs = puts_df["impliedVolatility"].dropna().tolist() if "impliedVolatility" in puts_df.columns else []

        for _, row in filtered.iterrows():
            bid = float(row.get("bid", 0) or 0)
            ask = float(row.get("ask", 0) or 0)
            oi = int(row.get("openInterest", 0) or 0)
            volume = int(row.get("volume", 0) or 0)
            if bid <= 0 or ask <= 0:
                excluded_contracts["noTwoSidedQuote"] += 1
                continue
            if oi <= 0 and volume <= 0:
                excluded_contracts["noLiquidityEvidence"] += 1
                continue
            mid = round((bid + ask) / 2, 2)
            contract_cost = round(ask * 100, 2)
            within_budget = contract_cost <= budget_usd
            strike = float(row["strike"])
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
                "contractCostBasis": "ASK",
                "withinBudget": within_budget,
                "openInterest": oi,
                "volume": volume,
                "quoteStatus": "TWO_SIDED",
                "ivPctile": iv_pctile,
                "ivFlag": iv_flag,
                "otmPct": otm_pct,
            })

    # Sort by expiry then strike
    candidates.sort(key=lambda c: (c["expiry"], c["strike"]))

    data_date = get_last_trading_date()
    data_quality = _compute_data_quality(considered_contracts, data_date)
    quality_warnings = list(data_quality.get("warnings", []))
    if excluded_contracts["noTwoSidedQuote"]:
        quality_warnings.append("UNQUOTED_CONTRACTS_EXCLUDED")
    if excluded_contracts["noLiquidityEvidence"]:
        quality_warnings.append("NO_LIQUIDITY_EVIDENCE_EXCLUDED")
    warnings = list(dict.fromkeys(quality_warnings))

    budget_feasible: bool | None = (
        None
        if data_quality.get("quality") == "LOW"
        else any(c["withinBudget"] for c in candidates)
        if candidates else None if considered_contracts else False
    )
    status = (
        "PARTIAL"
        if data_quality.get("quality") == "LOW" or (considered_contracts and not candidates)
        else "OK" if candidates
        else "EMPTY_RESULT"
    )

    # Generate note
    if status == "PARTIAL":
        note = "Option-chain quality is insufficient for a hedge or budget conclusion; retry before use."
        budget_gap = None
    elif not candidates:
        note = "No put options found in the specified OTM range."
        budget_gap = None
    elif budget_feasible is False:
        nearest = min(candidates, key=lambda c: c["contractCost"])
        budget_gap = round(nearest["contractCost"] - budget_usd, 2)
        note = f"No candidates within budget. Nearest: ${nearest['strike']} put at ${nearest['contractCost']}/contract ask cost vs ${budget_usd} budget."
    else:
        budget_gap = None
        count = sum(1 for c in candidates if c["withinBudget"])
        note = f"{count} candidate(s) within ${budget_usd} budget using executable ask cost."

    if considered_contracts and not candidates:
        note = "No decision-usable hedge candidates: contracts lacked a two-sided quote or liquidity evidence."

    return json.dumps({
        "ticker": ticker,
        "currentPrice": round(current_price, 2),
        "strikeRangeMin": round(strike_min, 2),
        "strikeRangeMax": round(strike_max, 2),
        "budgetUsd": budget_usd,
        "candidates": candidates,
        "budgetFeasible": budget_feasible,
        "budgetGapUsd": budget_gap,
        "status": status,
        "decisionGrade": False,
        "decisionUse": "HEDGE_SCREEN_ONLY",
        "consideredContracts": len(considered_contracts),
        "excludedContracts": excluded_contracts,
        "dataQuality": data_quality,
        "warnings": warnings,
        "recommendedNextAction": "RETRY" if status == "PARTIAL" else "NONE",
        "note": note,
        "dataDate": data_date,
    })


# ---------------------------------------------------------------------------
# Tool: get_analyst_upgrade_radar
# ---------------------------------------------------------------------------

def _analyst_price_target_change(row, signal: str) -> tuple[float | None, float | None, str | None]:
    """Price-target change of one upgrades_downgrades row, as the Worker reports it.

    yfinance returns currentPriceTarget/priorPriceTarget (0 means no target)
    and priceTargetAction. Direction comes from the targets when both exist,
    else from the action wording.
    """
    def _target(value) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if number > 0 and number == number else None

    pt_to = _target(row.get("currentPriceTarget"))
    pt_from = _target(row.get("priorPriceTarget"))
    action = str(row.get("priceTargetAction") or "").lower()
    if pt_from is not None and pt_to is not None:
        direction = "RAISED" if pt_to > pt_from else "LOWERED" if pt_to < pt_from else "UNCHANGED"
        return pt_from, pt_to, direction
    if "raise" in action:
        return pt_from, pt_to, "RAISED"
    if "lower" in action:
        return pt_from, pt_to, "LOWERED"
    if ("announce" in action or "initiat" in action or signal == "INITIATED") and pt_to is not None:
        return pt_from, pt_to, "INITIATED"
    if "maintain" in action:
        return pt_from, pt_to, "UNCHANGED"
    return pt_from, pt_to, None


async def get_analyst_upgrade_radar(ticker: str | list[str], days_back: int = 30) -> str:
    """Return recent analyst upgrades/downgrades with signals."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, lambda t: get_analyst_upgrade_radar(t, days_back))
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
            "upgrades": 0,
            "downgrades": 0,
            "initiations": 0,
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
    initiation_count = 0

    for _, row in ud.iterrows():
        from_grade = row.get("FromGrade", "")
        to_grade = row.get("ToGrade", "")
        firm = row.get("Firm", "")
        action = row.get("Action", "")

        date_val = row.get("GradeDate") or row.get("Date")
        date_str = str(date_val.date()) if hasattr(date_val, "date") else str(date_val)

        signal = _classify_analyst_change(action, from_grade, to_grade)
        if signal == "UPGRADE":
            upgrade_count += 1
        elif signal == "DOWNGRADE":
            downgrade_count += 1
        elif signal == "INITIATED":
            initiation_count += 1

        pt_from, pt_to, pt_direction = _analyst_price_target_change(row, signal)

        mixed_signal = (signal == "UPGRADE" and pt_direction == "LOWERED") or (signal == "DOWNGRADE" and pt_direction == "RAISED")

        # Strength flag
        # A mixed signal wins over the rating direction in both directions (2.14.0).
        if mixed_signal:
            strength_flag = "MIXED"
        elif signal == "UPGRADE":
            strength_flag = "BULLISH"
        elif signal == "DOWNGRADE":
            strength_flag = "BEARISH"
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
    if initiation_count:
        parts.append(f"{initiation_count} INITIATION(s)")
    summary = ", ".join(parts) if parts else "NO CHANGES"

    return json.dumps({
        "ticker": ticker,
        "windowDays": days_back,
        "netSentiment": net_sentiment,
        "upgrades": upgrade_count,
        "downgrades": downgrade_count,
        "initiations": initiation_count,
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
    return json.loads(df.reset_index().to_json(orient="records", date_format="iso"))


_FUND_INVERSE_VALUATION_LABELS = {
    "Price/Earnings",
    "Price/Book",
    "Price/Sales",
    "Price/Cashflow",
}


def _normalize_fund_equity_holdings(df) -> tuple[list | None, list | None, dict[str, str]]:
    """Convert Yahoo inverse valuation yields to the conventional labeled multiples."""
    records = _df_to_records(df)
    if not records:
        return records, None, {}
    raw_records = json.loads(json.dumps(records))
    methods: dict[str, str] = {}
    changed = False
    for record in records:
        label_key = next(
            (key for key in ("Average", "index") if key in record),
            next(iter(record), None),
        )
        label = record.get(label_key) if label_key else None
        if label not in _FUND_INVERSE_VALUATION_LABELS:
            continue
        for key, value in list(record.items()):
            if key == label_key or not isinstance(value, (int, float)) or value <= 0:
                continue
            if value < 1:
                record[key] = round(1 / value, 4)
                methods[f"{label}:{key}"] = "RECIPROCAL_FROM_PROVIDER_YIELD"
                changed = True
            else:
                methods[f"{label}:{key}"] = "PASSTHROUGH_CONVENTIONAL_MULTIPLE"
    return records, raw_records if changed else None, methods


async def get_etf_info(
    ticker: str | list[str],
    sections: list[str] | None = None,
) -> str:
    """Get ETF/fund information for one or more ticker symbols."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, lambda t: get_etf_info(t, sections))
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})

    requested = sections or ["overview", "holdings", "allocation"]
    allowed = {"overview", "holdings", "allocation", "operations", "fixed_income"}
    invalid = sorted(set(requested) - allowed)
    if invalid:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": f"Unsupported sections: {', '.join(invalid)}", "supportedSections": sorted(allowed)})
    cache_key = f"etf_info:{ticker}:{','.join(sorted(requested))}"
    cached = _tool_cache.get_value(cache_key)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        info = company.info
    except Exception as e:
        return f"Error: getting ETF info for {ticker}: {e}"

    # A stock is not a fund: say so instead of sections that recommend a retry (2.25.2, F33; the Worker's FUND_QUOTE_TYPES).
    fund_quote_type = info.get("quoteType") if isinstance(info, dict) else None
    if isinstance(fund_quote_type, str) and fund_quote_type and fund_quote_type.upper() not in {"ETF", "MUTUALFUND", "MONEYMARKET"}:
        return _mcp_failure(
            "get_fund_profile",
            ErrorCode.INPUT_VALIDATION_ERROR,
            f"{ticker} is quote type {fund_quote_type}, not an ETF or fund; get_fund_profile reads fund data only. "
            "Use get_company_profile for a company.",
        )

    data: dict = {k: info.get(k) for k in _ETF_INFO_FIELDS}
    data.update({
        "ticker": ticker,
        "source": "yahoo_finance",
        "evidenceClass": "CONTEXTUAL_MARKET_DATA",
        "decisionGrade": False,
        "sectionsRequested": requested,
        "sectionStatus": {},
        "sectionDates": {},
        "warnings": [],
    })

    try:
        funds = company.funds_data
    except Exception as e:
        funds = None
        data["fundsDataStatus"] = "PROVIDER_ERROR"
        data["fundsDataWarning"] = str(e)

    def _section(name: str, loader) -> None:
        if name not in requested:
            return
        try:
            present = loader()
            data["sectionStatus"][name] = "OK" if present is not False else "EMPTY_RESULT"
        except Exception:
            data["sectionStatus"][name] = "PROVIDER_ERROR"

    if funds is not None:
        def _load_overview() -> bool:
            data["description"] = funds.description
            data["fundOverview"] = funds.fund_overview
            return bool(data["description"] or data["fundOverview"])

        def _load_holdings() -> bool:
            holdings = funds.top_holdings
            data["topHoldings"] = _df_to_records(None if holdings is None else holdings.head(10))
            normalized, provider_raw, methods = _normalize_fund_equity_holdings(funds.equity_holdings)
            data["equityHoldings"] = normalized
            data["equityHoldingsProviderRaw"] = provider_raw
            data["valuationBasis"] = "CONVENTIONAL_MULTIPLE"
            data["valuationNormalization"] = methods
            data["sectionDates"]["holdings"] = {
                "asOfDate": None,
                "status": "NOT_EXPOSED_BY_PROVIDER",
            }
            return bool(data["topHoldings"] or data["equityHoldings"])

        def _load_allocation() -> bool:
            data["assetClasses"] = funds.asset_classes
            sector_weights = funds.sector_weightings
            data["sectorWeights"] = (
                [{"sector": key, "weight": value} for key, value in sector_weights.items()]
                if isinstance(sector_weights, dict) else None
            )
            data["sectionDates"]["allocation"] = {
                "asOfDate": None,
                "status": "NOT_EXPOSED_BY_PROVIDER",
            }
            return bool(data["assetClasses"] or data["sectorWeights"])

        def _load_operations() -> bool:
            data["fundOperations"] = _df_to_records(funds.fund_operations)
            return bool(data["fundOperations"])

        def _load_fixed_income() -> bool:
            data["bondHoldings"] = _df_to_records(funds.bond_holdings)
            data["bondRatings"] = funds.bond_ratings
            return bool(data["bondHoldings"] or data["bondRatings"])

        _section("overview", _load_overview)
        _section("holdings", _load_holdings)
        _section("allocation", _load_allocation)
        _section("operations", _load_operations)
        _section("fixed_income", _load_fixed_income)
    else:
        for section in requested:
            data["sectionStatus"][section] = "PROVIDER_ERROR"

    dated_sections = {"holdings", "allocation"}.intersection(requested)
    if dated_sections and any(data["sectionStatus"].get(section) == "OK" for section in dated_sections):
        data["warnings"].append("FUND_CHARACTERISTICS_AS_OF_DATE_NOT_EXPOSED")
    if not all(v == "OK" for v in data["sectionStatus"].values()):
        data["recommendedNextAction"] = "RETRY_OR_REQUEST_AVAILABLE_SECTION"
    elif data["warnings"]:
        data["recommendedNextAction"] = "CHECK_OFFICIAL_FUND_SOURCE"
    else:
        data["recommendedNextAction"] = "NONE"

    result = json.dumps(data)
    _tool_cache.set(cache_key, result, _PRICE_TTL)
    return result


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
        exp = _next_option_expiry(list(exps), _us_market_date())
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

    # Max pain strike: where option holders' total payout at expiry is
    # smallest, as summarize_options_flow computes it (not the strike with
    # the most open interest).
    max_pain_strike: float | None = None
    scan_warnings: list[str] = []
    if call_oi_total + put_oi_total <= 0:
        scan_warnings.append("MAX_PAIN_UNAVAILABLE_ZERO_OI")
    else:
        try:
            call_oi = calls_df["openInterest"].fillna(0)
            put_oi = puts_df["openInterest"].fillna(0)
            min_pain = float("inf")
            for strike in sorted(set(calls_df["strike"].tolist() + puts_df["strike"].tolist())):
                pain = float(((strike - calls_df["strike"]).clip(lower=0) * call_oi).sum()
                             + ((puts_df["strike"] - strike).clip(lower=0) * put_oi).sum())
                if pain < min_pain:
                    min_pain = pain
                    max_pain_strike = float(strike)
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
    if quality == "LOW":
        scan_warnings.append("OPTIONS_CHAIN_LOW_QUALITY")

    # IV percentile — approximate using annualised 30-day rolling realised vol over 1 year
    iv_pctile: int | None = None
    data_date: str | None = None
    realized_vol_price_basis: str | None = None
    historical_bar_status = "NOT_USED"
    freshness_status = "UNKNOWN"
    freshness_reason = None
    excluded_incomplete_bar = False
    retry_attempted = False
    fallback_used = False
    if quality == "LOW" and data_quality.get("placeholderIvCount", 0) > len(calls_list + puts_list) * 0.5:
        scan_warnings.append("IV_PERCENTILE_UNAVAILABLE_PLACEHOLDER_IV")
    else:
        try:
            prepared = await _load_completed_daily_history(
                company,
                "1y",
                required_bars=31,
                retry_adjusted_gap=True,
            )
            completed = prepared["completed"]
            data_date = (
                _daily_bar_date(completed.index[-1], prepared["timezoneName"])
                if not completed.empty
                else None
            )
            freshness_status = prepared["freshnessStatus"]
            freshness_reason = prepared.get("freshnessReason")
            excluded_incomplete_bar = prepared["excludedIncompleteBar"]
            retry_attempted = prepared["retryAttempted"]
            if freshness_status == "STALE":
                historical_bar_status = "STALE"
                scan_warnings.append("IV_PERCENTILE_UNAVAILABLE_STALE_DAILY_BARS")
            else:
                selected = _select_completed_close_series(completed)
                if selected is None:
                    historical_bar_status = "INCOMPLETE"
                    scan_warnings.append("IV_PERCENTILE_UNAVAILABLE_INCOMPLETE_DAILY_BARS")
                else:
                    closes, realized_vol_price_basis, fallback_used = selected
                    historical_bar_status = "COMPLETE"
                    rets = closes.pct_change().dropna()
                    if len(closes) >= 31 and atm_iv is not None:
                        roll_rv = rets.rolling(30).std() * (252 ** 0.5)
                        roll_rv = roll_rv.dropna()
                        if len(roll_rv) >= 5:
                            rv_min, rv_max = float(roll_rv.min()), float(roll_rv.max())
                            if rv_max > rv_min:
                                pctile = (atm_iv - rv_min) / (rv_max - rv_min) * 100
                                iv_pctile = max(0, min(100, round(pctile)))
        except Exception:
            historical_bar_status = "UNAVAILABLE"
            scan_warnings.append("IV_PERCENTILE_UNAVAILABLE_DAILY_BARS")

    # Put vol vs 10-day average proxy (since historical options volume is unavailable via yfinance)
    # Proxy: put vol as a multiple of 1% of the stock's 10d average daily volume.
    put_vol_vs_10d: float | None = None
    try:
        adv10 = fi["tenDayAverageVolume"]
        if adv10 and adv10 > 0 and put_vol > 0:
            put_vol_vs_10d = round(put_vol / (adv10 * 0.01), 2)
    except Exception:
        pass

    # Look up prior window reading for trend analysis
    prev_window_map = {"T-7": "T-14", "T-2": "T-7"}
    prev_window = prev_window_map.get(window_label)
    prev_data: dict | None = None
    if prev_window:
        prev_cached = _tool_cache.get_value(f"options_flow:{ticker}:{prev_window}")
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
            prev_data.get("putVolPer1PctStockAdv")
            if prev_data.get("putVolPer1PctStockAdv") is not None
            else prev_data.get("putVolVs10dAvg")
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
        iv_str = f"{iv_pctile}%" if iv_pctile is not None else "N/A"
        pv_str = f"{put_vol_vs_10d:.2f}x" if put_vol_vs_10d is not None else "N/A"
        pc_str = f"{pc_ratio:.2f}" if pc_ratio is not None else "N/A"
        formatted_block = (
            f"OPTIONS FLOW SCAN [{window_label}] {ticker} | "
            f"P/C: {pc_str} | "
            f"IV vs 1y RV range: {iv_str} | "
            f"Put vol / 1% stock ADV: {pv_str} | "
            f"Trend: {put_vol_trend} | "
            f"Advisory: {bracket or 'N/A'} bracket"
        )

    result_dict: dict = {
        "ticker": ticker,
        "windowLabel": window_label,
        "dataDate": data_date,
        "pcRatio": pc_ratio,
        # Named for what they measure (2.4.4); the old aliases were removed in 3.0.0.
        "ivVsRealizedRangePct": iv_pctile,
        "putVolPer1PctStockAdv": put_vol_vs_10d,
        "putVolTrend": put_vol_trend,
        "expiry": exp,
        "expirySelection": "NEXT_AFTER_TODAY",
        "maxPainStrike": max_pain_strike,
        "bracket": bracket,
        "formattedBlock": formatted_block,
        "realizedVolPriceBasis": realized_vol_price_basis,
        "historicalObservationType": "COMPLETED_DAILY_PRICE_SERIES",
        "historicalBarStatus": historical_bar_status,
        "freshnessStatus": freshness_status,
        "excludedIncompleteBar": excluded_incomplete_bar,
        "retryAttempted": retry_attempted,
        "fallbackUsed": fallback_used,
        "recommendedNextAction": (
            "RETRY"
            if quality == "LOW" or historical_bar_status in {"STALE", "UNAVAILABLE", "INCOMPLETE"}
            else "NONE"
        ),
        "dataQuality": data_quality,
        "warnings": scan_warnings,
    }

    # Cache current reading for future trend comparison (72h TTL via 3-day window check)
    _tool_cache.set(f"options_flow:{ticker}:{window_label}", json.dumps(result_dict), 72 * 3600)
    return json.dumps(with_notices(result_dict, _freshness_notices(ticker, freshness_reason)))


# ---------------------------------------------------------------------------
# CR-13 — get_price_target_bracket
# ---------------------------------------------------------------------------

async def get_price_target_bracket(
    ticker: str, reference_target_price: float | None = None, io_pt: float | None = None
) -> str:
    """Return bracket and distance fields for current price vs reference target."""
    target_price = reference_target_price if reference_target_price is not None else io_pt
    if target_price is None or not target_price > 0:
        # A caller's bad input, not a provider failure (2.25.2, F42).
        return _mcp_failure("calculate_price_target_distance", ErrorCode.INPUT_VALIDATION_ERROR,
                            "reference_target_price (or io_pt alias) must be a positive number")

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        current_price: float | None = fi["lastPrice"]
        if current_price is None:
            return json.dumps({"error": True, "message": f"No price data for {ticker}", "ticker": ticker})
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    # Display rounding is half away from zero on the decimal value, as in the Worker (R5-pricing-1, Q3d); a zero price
    # has no distance to the target (G1).
    reference_target_pct = _round_half_away_from_zero(current_price / target_price * 100, 1)
    distance_to_target_pct = (_round_half_away_from_zero((target_price - current_price) / current_price * 100, 1)
                              if current_price != 0 else None)

    inverted_flag = reference_target_pct >= 100

    data_date: str | None = None
    price_timestamp: str | None = None
    market_state: str | None = None
    market_open: bool | None = None
    try:
        info = company.info
        market_time = info.get("regularMarketTime")
        market_state = info.get("marketState")
        market_open = market_state == "REGULAR"
        if isinstance(market_time, (int, float)) and market_time:
            price_time = datetime.datetime.fromtimestamp(
                market_time,
                tz=datetime.timezone.utc,
            )
            price_timestamp = price_time.isoformat().replace("+00:00", "Z")
            data_date = price_time.date().isoformat()
    except Exception:
        pass

    return json.dumps({
        "ticker": ticker,
        "currentPrice": _round_half_away_from_zero(current_price, 4),
        "priceBasis": "REGULAR_MARKET_PRICE",
        "observationType": "REGULAR_MARKET_QUOTE_VS_USER_REFERENCE",
        "priceTimestamp": price_timestamp,
        "marketState": market_state,
        "marketOpen": market_open,
        "referenceTargetPrice": target_price,
        "referenceTargetPct": reference_target_pct,
        "currentToTargetRatioPct": reference_target_pct,
        "distanceToTargetPct": distance_to_target_pct,
        "distanceConvention": "(referenceTargetPrice-currentPrice)/currentPrice*100",
        "ioPt": target_price,
        "eqfPct": reference_target_pct,
        "invertedFlag": inverted_flag,
        # From the raw prices, not the rounded referenceTargetPct (2.8.0).
        "priceAtOrAboveTarget": current_price >= target_price if current_price > 0 and target_price > 0 else None,
        "dataDate": data_date,
        "recommendedNextAction": "NONE",
    })


# ---------------------------------------------------------------------------
# CR-14 — get_position_score_inputs
# ---------------------------------------------------------------------------

POSITION_SINGLE_TICKER_MESSAGE = "analyze_position_signals takes one ticker as a string, not a list."


async def get_position_score_inputs(ticker: str) -> str:
    """Return grouped public inputs for caller-defined scoring workflows."""
    # Q3i call 4 (3.2.0): one ticker, as the declared schema says; a list is refused, not passed to the components.
    if not isinstance(ticker, str):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": POSITION_SINGLE_TICKER_MESSAGE})
    results = await asyncio.gather(
        get_analyst_upgrade_radar(ticker, days_back=30),
        get_analyst_consensus(ticker),
        get_price_stats(ticker),
        get_earnings_momentum(ticker),
        get_technical_indicators(ticker, "1y"),
        get_ma_position(ticker),
        return_exceptions=True,
    )

    def _parse(r: object) -> dict:
        if isinstance(r, Exception):
            return {}
        try:
            value = json.loads(str(r)) if isinstance(r, str) else {}
        except Exception:
            return {}
        return value if isinstance(value, dict) else {}

    upgrade = _parse(results[0])
    consensus = _parse(results[1])
    price = _parse(results[2])
    earnings = _parse(results[3])
    tech = _parse(results[4])
    ma = _parse(results[5])
    component_payloads = {
        "analystUpgrades": upgrade,
        "analystConsensus": consensus,
        "priceStats": price,
        "earningsMomentum": earnings,
        "technicalIndicators": tech,
        "maPosition": ma,
    }
    component_status: dict[str, str] = {}
    failed_components: list[str] = []
    limited_components: list[str] = []
    for name, payload in component_payloads.items():
        if not payload or payload.get("error"):
            component_status[name] = "FAILED"
            failed_components.append(name)
        elif payload.get("status") in {"STALE_BAR", "PARTIAL"}:
            component_status[name] = str(payload["status"])
            limited_components.append(name)
        else:
            component_status[name] = "OK"

    # T1: analyst sentiment
    t1: dict = {
        "analystNetSentiment": upgrade.get("netSentiment"),
        "upgrades30d": upgrade.get("upgrades"),
        "downgrades30d": upgrade.get("downgrades"),
        "initiations30d": upgrade.get("initiations"),
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
        "avgSurprisePct": None if earnings.get("preRevenue") else earnings.get("avgSurprisePct"),
        "momentumFlag": earnings.get("momentumFlag"),
        "preRevenue": bool(earnings.get("preRevenue")),
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

    # Data date: the last completed bar of the technicals, else of the moving averages; never today (Q3d, 3.2.0).
    data_date = tech.get("dataDate") or ma.get("dataDate") or None
    # The components' notices (3.1.0) reach this tool's meta.warnings, once each.
    notices = unique_notices([n for payload in component_payloads.values() for n in take_notices(payload)])
    quote_timestamp = price.get("priceTimestamp") if isinstance(price.get("priceTimestamp"), str) and price.get("priceTimestamp") else None

    return json.dumps(with_notices({
        "ticker": ticker,
        "status": "PARTIAL" if failed_components or limited_components else "OK",
        "dataDate": data_date,
        "quoteDataDate": quote_timestamp[:10] if quote_timestamp else None,
        "quoteTimestamp": quote_timestamp,
        "completedBarDataDate": tech.get("dataDate"),
        "analystInputs": t1,
        "pricePositionInputs": t2,
        "earningsInputs": t4,
        "technicalInputs": t5,
        "componentStatus": component_status,
        "failedComponents": failed_components,
        "limitedComponents": limited_components,
        "recommendedNextAction": (
            "RETRY" if failed_components or limited_components else "NONE"
        ),
    }, notices))


# ---------------------------------------------------------------------------
# CR-15 — get_volume_gate

@yfinance_server.tool(
    name="get_market_quote",
    description=(
        "Get a lightweight Yahoo regular-market price observation for one or more tickers. "
        "lastPrice uses priceBasis=REGULAR_MARKET_PRICE and includes priceTimestamp. "
        "It may differ from get_price_slope.endClose, which is adjusted daily-bar analytics."
    ),
)
async def get_market_quote(ticker: str | list[str]) -> str:
    return await get_fast_info(ticker)


@yfinance_server.tool(name="get_historical_prices", description="Canonical alias for get_historical_stock_prices.")
async def get_historical_prices(ticker: str, period: HistoricalPeriod = "1mo", interval: HistoricalInterval = "1d", prepost: bool = False) -> str:
    return await get_historical_stock_prices(ticker=ticker, period=period, interval=interval, prepost=prepost)


@yfinance_server.tool(name="analyze_price_performance", description="Canonical alias for get_price_stats.")
async def analyze_price_performance(ticker: str | list[str]) -> str:
    return await get_price_stats(ticker)


@yfinance_server.tool(name="analyze_moving_average_position", description="Canonical alias for get_ma_position.")
async def analyze_moving_average_position(ticker: str | list[str]) -> str:
    return await get_ma_position(ticker)


@yfinance_server.tool(name="analyze_volume_ratio", description="Canonical alias for get_volume_ratio.")
async def analyze_volume_ratio(ticker: str | list[str], period: int = 10) -> str:
    return await get_volume_ratio(ticker, period)


@yfinance_server.tool(name="check_volume_liquidity_threshold", description="Canonical alias for get_volume_gate. Pass min_adv_usd for a caller-chosen threshold; meetsMinimum reports adv20dTradedValueUsd >= min_adv_usd.")
async def check_volume_liquidity_threshold(
    ticker: str,
    foreign_exchange: bool = False,
    min_adv_usd: Annotated[float | None, Field(description=MIN_ADV_USD_DESCRIPTION)] = None,
) -> str:
    min_adv_usd, min_adv_error = parse_min_adv_usd(min_adv_usd)
    if min_adv_error:
        return _mcp_failure("check_volume_liquidity_threshold", ErrorCode.INPUT_VALIDATION_ERROR, min_adv_error)
    return await get_volume_gate(ticker, foreign_exchange, min_adv_usd)


@yfinance_server.tool(name="get_company_profile", description="Canonical alias for get_stock_info.")
async def get_company_profile(ticker: str | list[str], include_all: bool = False) -> str:
    return await get_stock_info(ticker, include_all=include_all)


@yfinance_server.tool(name="get_fund_profile", description="Get a fund/ETF profile with section status and as-of-date limitations. Valuation characteristics are conventional multiples; provider inverse yields are retained separately. Use sections to request overview, holdings, allocation, operations, or fixed-income detail.")
async def get_fund_profile(ticker: str | list[str], sections: list[str] | None = None) -> str:
    return await get_etf_info(ticker, sections)


@yfinance_server.tool(name="analyze_financial_ratios", description="Analyze current financial ratios with explicit unitSemantics and optionally historical Yahoo valuation measures.")
async def analyze_financial_ratios(ticker: str | list[str], history_periods: int = 0, frequency: Literal["quarterly", "monthly", "yearly", "trailing"] = "quarterly") -> str:
    return await get_financial_ratios(ticker, history_periods, frequency)


@yfinance_server.tool(name="analyze_credit_health", description="Canonical alias for get_credit_health.")
async def analyze_credit_health(ticker: str | list[str]) -> str:
    return await get_credit_health(ticker)


@yfinance_server.tool(name="get_corporate_actions", description="Get stock splits, dividends, and fund capital-gain distributions from Yahoo Finance, oldest first. Every split is returned; dividends and capital gains are limited to the most recent `limit` on or after `start_date`.")
async def get_corporate_actions(
    ticker: str,
    start_date: str | None = None,
    limit: int = 40,
) -> str:
    return await get_stock_actions(ticker, start_date or "", limit)


@yfinance_server.tool(name="get_ownership_holders", description="Canonical alias for get_holder_info.")
async def get_ownership_holders(ticker: str, holder_type: HolderType) -> str:
    return await get_holder_info(ticker, holder_type)


@yfinance_server.tool(name="get_analyst_recommendations", description="Canonical alias for get_recommendations.")
async def get_analyst_recommendations(ticker: str, recommendation_type: RecommendationType, months_back: int = 12) -> str:
    return await get_recommendations(ticker, recommendation_type, months_back)


@yfinance_server.tool(name="get_analyst_rating_changes", description="Canonical alias for get_analyst_upgrade_radar.")
async def get_analyst_rating_changes(ticker: str | list[str], days_back: int = 30) -> str:
    return await get_analyst_upgrade_radar(ticker, days_back)


@yfinance_server.tool(name="analyze_earnings_momentum", description="Canonical alias for get_earnings_momentum.")
async def analyze_earnings_momentum(ticker: str | list[str]) -> str:
    return await get_earnings_momentum(ticker)


@yfinance_server.tool(name="get_company_events_calendar", description="Get a ticker's upcoming Yahoo calendar estimates or paginated earnings history. Yahoo dates are UNVERIFIED provider data; confirm material dates with official releases.")
async def get_company_events_calendar(ticker: str, mode: Literal["upcoming", "history"] = "upcoming", limit: int = 12, offset: int = 0) -> str:
    return await get_calendar(ticker, mode, limit, offset)


# get_company_news is registered by yfmcp.tools.news (imported above, next to the other news tools) with
# after="get_company_events_calendar", which keeps its original position in the tool listing.


@yfinance_server.tool(name="summarize_options_flow", description="Canonical alias for get_options_summary/get_options_flow_summary.")
async def summarize_options_flow(ticker: str, expiry_hint: str | None = None) -> str:
    return await get_options_summary(ticker=ticker, expiry_hint=expiry_hint)


@yfinance_server.tool(name="analyze_options_flow_window", description="Analyze an event-window options flow reading. LOW chain quality returns RETRY and must not support derivative conclusions.")
async def analyze_options_flow_window(ticker: str, window_label: str) -> str:
    return await get_options_flow_scan(ticker, window_label)


@yfinance_server.tool(name="find_put_hedge_candidates", description="Find OTM put hedge candidates using executable ask cost. Contracts require a two-sided quote plus liquidity evidence; PARTIAL means retry and do not use budgetFeasible as a decision.")
async def find_put_hedge_candidates(ticker: str, otm_pct_min: float = 8, otm_pct_max: float = 12, budget_usd: float = 500, expiry_after: str = "") -> str:
    return await get_put_hedge_candidates(ticker, otm_pct_min, otm_pct_max, budget_usd, expiry_after)


@yfinance_server.tool(name="calculate_price_target_distance", description="Compare current price to a user-supplied reference price target with both the legacy ratio and directional percent distance.")
async def calculate_price_target_distance(
    ticker: str,
    reference_target_price: float | None = None,
    io_pt: float | None = None,
) -> str:
    return await get_price_target_bracket(
        ticker, reference_target_price=reference_target_price, io_pt=io_pt
    )


@yfinance_server.tool(name="analyze_position_signals", description="Canonical alias for get_position_score_inputs.")
async def analyze_position_signals(ticker: str) -> str:
    return await get_position_score_inputs(ticker)


# ---------------------------------------------------------------------------
# SEC EDGAR retrieval tools - imported for registration and re-exported
# ---------------------------------------------------------------------------
import yfmcp.tools.sec  # noqa: F401 (side-effect import: registers the SEC EDGAR retrieval tools)
from yfmcp.tools.sec import (  # re-export for compatibility, grouped routing and server-side callers
    _FilingPeriodParam,
    _SEC_DOCUMENT_READ_MAX_CHARS,
    _accession_not_found_envelope,
    _build_filing_index_from_html,
    _filing_period_failure,
    _filing_table_is_usable,
    _find_section_bounds,
    _is_accession_not_found,
    _is_decision_grade_sec_xbrl_fact,
    _is_likely_xbrl_document_url,
    _legacy_accession_miss_payload,
    _map_extraction_to_retrieval_path,
    _parse_filing_table_rows,
    _resolve_periodic_filings,
    _resolve_sec_filing,
    _sec_filing_index_full,
    _sec_xbrl_source_evidence,
    _suggest_alternative_queries,
    extract_filing_fact,
    extract_sec_filing_fact,
    get_filing_data,
    get_filing_outline,
    get_filing_section,
    get_filing_table,
    get_sec_filing_index,
    get_sec_filing_intelligence,
    get_sec_filing_outline,
    get_sec_filing_section,
    get_sec_filing_section_markdown,
    get_sec_filing_table,
    list_filing_tables,
    list_sec_company_filings,
    list_sec_filing_tables,
    list_sec_filings,
    list_sec_material_filings,
    search_filing_text,
    search_sec_filing_text,
)


def _as_status(source_payload: dict) -> str:
    """The Worker's normalizeStatus: status/code first, then source/confidence, then NOT_DISCLOSED, CONFLICTING, NOT_FOUND."""
    def _text(value: object) -> str:
        return "" if value is None else str(value).upper()

    status = _text(source_payload.get("status") if source_payload.get("status") is not None else source_payload.get("code"))
    for named in ("FILING_NOT_FOUND_TRY_OTHER_TYPE", "FILING_TEXT_NOT_AVAILABLE", "EXTRACTION_FAILED", "TABLE_NOT_PARSED",
                  "PROVIDER_LIMITATION", "NO_DIMENSIONAL_REVENUE_FACT"):
        if status == named:
            return named
    # A failed SEC read is not a missing fact (2.5.17).
    if status == "PROVIDER_ERROR":
        return "PROVIDER_ERROR"
    source = _text(source_payload.get("source"))
    confidence = _text(source_payload.get("confidence"))
    for named in ("FILING_NOT_FOUND_TRY_OTHER_TYPE", "EXTRACTION_FAILED", "TABLE_NOT_PARSED", "PROVIDER_LIMITATION",
                  "NO_DIMENSIONAL_REVENUE_FACT", "NOT_DISCLOSED", "CONFLICTING"):
        if source == named or confidence == named:
            return named
    return "NOT_FOUND"


# The revenue statuses that say why there is no value (the Worker's NON_DECISION_REVENUE_STATUSES).
_NON_DECISION_REVENUE_STATUSES = frozenset({
    "EXTRACTION_FAILED", "TABLE_NOT_PARSED", "PROVIDER_LIMITATION", "NO_DIMENSIONAL_REVENUE_FACT",
    "FILING_NOT_FOUND_TRY_OTHER_TYPE", "FILING_TEXT_NOT_AVAILABLE",
})


def _explicit_revenue_limitation_status(status: object, warnings: list | None = None) -> str | None:
    """The Worker's explicitRevenueLimitationStatus: a named limitation, from the status or from a warning code."""
    normalized = "" if status is None else str(status).upper()
    if normalized in _NON_DECISION_REVENUE_STATUSES:
        return normalized
    codes = {w.get("code") for w in (warnings or []) if isinstance(w, dict)}
    if "TABLE_NOT_PARSED" in codes:
        return "TABLE_NOT_PARSED"
    if "NO_DIMENSIONAL_REVENUE_FACT" in codes:
        return "NO_DIMENSIONAL_REVENUE_FACT"
    return None


async def _may_be_20f_filer(ticker: str) -> bool:
    _, submissions = await _get_submissions_for_ticker(ticker)
    if not isinstance(submissions, dict):
        return False
    forms = (((submissions.get("filings") or {}).get("recent") or {}).get("form") or [])
    return any(str(form or "").upper() == "20-F" for form in forms)


@yfinance_server.tool(
    name="extract_geographic_revenue",
    description="Extract geographic revenue exposure from official SEC data and indexed filing tables, returning explicit parser/provider limitation statuses when no decision-grade value is available.",
)
async def extract_geographic_revenue(
    ticker: str,
    region: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    accession_number: str | None = None,
    detailLevel: str = "compact",
) -> str:
    failure = _filing_period_failure("extract_geographic_revenue", period)
    if failure:
        return failure
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

    # An explicit accession is the filing read: the figure, the evidence and the raw index all come from it, and a
    # miss fails closed with no fallback to the latest filing (2.15.0, R4-ACC-GEO).
    pinned = accession_number is not None and str(accession_number).strip() != ""
    pinned_filing_form = None
    if pinned:
        resolved_filing, resolve_error = await _resolve_sec_filing(ticker, filing_type, accession_number)
        if resolved_filing is None and _is_accession_not_found(resolve_error):
            return _accession_not_found_envelope(resolve_error)
        pinned_filing_form = str(
            resolved_filing.get("filingType") if resolved_filing is not None else (resolve_error or {}).get("filingType") or ""
        ).upper()
    payload = _safe_json_loads(await get_filing_data(
        ticker=ticker, fact_type=FilingFactType.geographic_revenue, region=region, filing_type=filing_type, period=period,
        accession_number=accession_number if pinned else None,
    ))
    detail = str(detailLevel).lower()
    # The filing index is read for detailLevel raw only, as the Worker does (compact evidence comes from the payload).
    idx_payload = _safe_json_loads(await _sec_filing_index_full(ticker, filing_type, period, accession_number)) if detail == "raw" else {}
    warnings = list(payload["warnings"]) if isinstance(payload.get("warnings"), list) else []
    payload_status = str(_nn(payload.get("status"), _nn(payload.get("code"), _nn(payload.get("confidence"), "")))).strip()
    if payload.get("value") is not None and payload.get("denominator") is None and not any(isinstance(w, dict) and w.get("code") == "DENOMINATOR_NOT_FOUND" for w in warnings):
        warnings.append({
            "code": "DENOMINATOR_NOT_FOUND",
            "message": "Could not compute geographic revenue percentage due to missing denominator.",
            "severity": "warning",
        })

    def _evidence(source: dict, index: dict, default_filing_type: str) -> dict:
        found = source.get("evidence") if isinstance(source.get("evidence"), dict) else {}
        return {
            "filingType": _nn(index.get("filingType"), _nn(source.get("filingType"), default_filing_type)),
            "filingDate": _nn(index.get("filingDate"), source.get("filingDate")),
            "acceptedAt": index.get("acceptedAt"),
            "accessionNumber": _nn(index.get("accessionNumber"), source.get("accessionNumber")),
            "documentUrl": _nn(index.get("documentUrl"), source.get("documentUrl")),
            "sectionHeading": found.get("sectionHeading"),
            "tableTitle": found.get("tableTitle"),
            "sourceTableId": found.get("sourceTableId"),
            "sourceRows": found.get("sourceRows") if isinstance(found.get("sourceRows"), list) else [],
            "sourceColumns": found.get("sourceColumns") if isinstance(found.get("sourceColumns"), list) else [],
        }

    has_denominator = payload.get("denominator") is not None
    shaped = {
        "ticker": ticker,
        "factType": "geographic_revenue",
        "region": region,
        "period": payload.get("period"),
        "rawValue": payload.get("rawValue"),
        "rawDenominator": payload.get("rawDenominator"),
        "unit": _nn(payload.get("unit"), "USD"),
        "unitScale": _nn(payload.get("unitScale"), "unknown"),
        "unitScaleSource": payload.get("unitScaleSource"),
        "value": payload.get("value"),
        "denominator": payload.get("denominator"),
        "valueRatio": payload.get("valueRatio") if has_denominator else None,
        "valuePct": payload.get("valuePct") if has_denominator else None,
        "extractionMethod": _nn(payload.get("extractionMethod"), "NONE"),
        "confidence": _nn(payload.get("confidence"), "NOT_DISCLOSED"),
        "evidence": _evidence(payload, idx_payload, filing_type),
        "calculation": payload.get("calculation"),
        "scanCoverage": payload.get("scanCoverage"),
        "searchedTerms": payload["searchedTerms"] if isinstance(payload.get("searchedTerms"), list) else [],
        "notDisclosedBasis": payload.get("notDisclosedBasis"),
        "status": "FOUND" if payload.get("value") is not None else (payload_status or "NOT_DISCLOSED"),
        "code": _nn(payload.get("code"), payload_status if payload_status and payload_status not in ("FOUND", "NOT_DISCLOSED") else None),
        "warnings": warnings,
    }
    if str(filing_type or "").upper() == "10-K" and str(shaped["confidence"] or "").upper() == "NOT_DISCLOSED":
        evidence_filing_type = str(shaped["evidence"].get("filingType") or "").upper()
        maybe_20f = evidence_filing_type == "20-F"
        if pinned:
            # A pinned read stays pinned: the 20-F retry reads the same filing, and only when that filing is a 20-F (2.15.0).
            maybe_20f = pinned_filing_form == "20-F"
        elif not maybe_20f and evidence_filing_type in ("", "10-K"):
            maybe_20f = await _may_be_20f_filer(ticker)
        if maybe_20f:
            # Automatic 20-F fallback: retry extraction with 20-F filing type
            fallback_payload = _safe_json_loads(await get_filing_data(
                ticker=ticker, fact_type=FilingFactType.geographic_revenue, region=region, filing_type="20-F", period=period,
                accession_number=accession_number if pinned else None,
            ))
            fallback_idx = _safe_json_loads(await _sec_filing_index_full(ticker, "20-F", period, accession_number))
            if fallback_payload.get("value") is not None:
                # 20-F extraction succeeded — replace the output with the fallback data
                fallback_denominator = fallback_payload.get("denominator") is not None
                shaped["period"] = fallback_payload.get("period")
                shaped["rawValue"] = fallback_payload.get("rawValue")
                shaped["rawDenominator"] = fallback_payload.get("rawDenominator")
                shaped["unit"] = _nn(fallback_payload.get("unit"), "USD")
                shaped["unitScale"] = _nn(fallback_payload.get("unitScale"), "unknown")
                shaped["unitScaleSource"] = fallback_payload.get("unitScaleSource")
                shaped["value"] = fallback_payload.get("value")
                shaped["denominator"] = fallback_payload.get("denominator")
                shaped["valueRatio"] = fallback_payload.get("valueRatio") if fallback_denominator else None
                shaped["valuePct"] = fallback_payload.get("valuePct") if fallback_denominator else None
                shaped["extractionMethod"] = _nn(fallback_payload.get("extractionMethod"), "NONE")
                shaped["confidence"] = _nn(fallback_payload.get("confidence"), "NOT_DISCLOSED")
                shaped["evidence"] = _evidence(fallback_payload, fallback_idx, "20-F")
                shaped["calculation"] = fallback_payload.get("calculation")
                shaped["scanCoverage"] = fallback_payload.get("scanCoverage")
                shaped["searchedTerms"] = fallback_payload["searchedTerms"] if isinstance(fallback_payload.get("searchedTerms"), list) else []
                shaped["notDisclosedBasis"] = fallback_payload.get("notDisclosedBasis")
                shaped["status"] = "FOUND"
                shaped["code"] = fallback_payload.get("code")
                # Append advisory warning noting automatic 20-F selection
                shaped["warnings"] = [
                    *(fallback_payload["warnings"] if isinstance(fallback_payload.get("warnings"), list) else []),
                    {
                        "code": "AUTO_20F_FALLBACK",
                        "message": "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
                        "severity": "info",
                    },
                ]
            elif not any(isinstance(w, dict) and w.get("code") == "POSSIBLE_20F_FILER" for w in warnings):
                # 20-F extraction also failed — keep the original and add an advisory warning
                warnings.append({
                    "code": "POSSIBLE_20F_FILER",
                    "message": "POSSIBLE_20F_FILER: Ticker may file 20-F. Retry with filing_type='20-F' or use IR web search.",
                    "severity": "warning",
                })
    if detail == "raw":
        shaped["rawContext"] = {"filingIndex": idx_payload}
    return json.dumps(shaped)


@yfinance_server.tool(name="extract_segment_revenue", description="Extract segment revenue rows from official SEC facts and filing tables, returning explicit limitation statuses when no parseable segment data is found.")
async def extract_segment_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    detailLevel: str = "compact",
) -> str:
    return await _extract_segment_revenue(ticker, filing_type, period, detailLevel)


async def _extract_segment_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
    # Internal only (not a tool param): a pinned caller reads the pinned filing (2.24.0, F21).
    accession_number: str | None = None,
) -> str:
    failure = _filing_period_failure("extract_segment_revenue", period)
    if failure:
        return failure
    pin = str(accession_number).strip() if accession_number is not None and str(accession_number).strip() else None
    payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.segment_revenue, filing_type=filing_type, period=period, accession_number=pin))
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
    warnings: list[dict] = []
    # Automatic 20-F fallback for foreign private issuers
    if not rows and str(filing_type or "").upper() == "10-K":
        if pin:
            # A pinned read stays pinned: the 20-F retry reads the same filing, and only when that filing is a 20-F (2.24.0, F21).
            pinned_filing, _pin_error = await _resolve_sec_filing(ticker, filing_type, pin)
            maybe_20f = pinned_filing is not None and str(pinned_filing.get("filingType") or "").upper() == "20-F"
        else:
            maybe_20f = await _may_be_20f_filer(ticker)
        if maybe_20f:
            fb_payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.segment_revenue, filing_type="20-F", period=period, accession_number=pin))
            fb_segments = fb_payload.get("allSegments") if isinstance(fb_payload.get("allSegments"), list) else []
            fb_rows = []
            for seg in fb_segments:
                if not isinstance(seg, dict):
                    continue
                fb_rows.append({
                    "label": seg.get("segmentLabel"),
                    "value": seg.get("value"),
                    "period": f"FY{seg.get('fiscalYear')}" if seg.get("fiscalYear") else None,
                    "confidence": "HIGH",
                    "evidence": {
                        "filingDate": seg.get("filingDate"),
                        "accessionNumber": seg.get("accessionNumber"),
                    },
                })
            if fb_rows:
                rows = fb_rows
                out = {"ticker": ticker, "factType": "segment_revenue", "segments": rows, "status": "FOUND"}
                warnings.append({
                    "code": "AUTO_20F_FALLBACK",
                    "message": "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
                    "severity": "info",
                })
            else:
                warnings.append({
                    "code": "POSSIBLE_20F_FILER",
                    "message": "POSSIBLE_20F_FILER: Ticker may file 20-F. Retry with filing_type='20-F' or use IR web search.",
                    "severity": "warning",
                })
    if not rows and payload.get("code") == "INVALID_PERIOD":
        # A fiscal year with a quarterly filing type: no table read can answer it either (2.25.1, F26).
        invalid = next((w for w in (payload.get("warnings") if isinstance(payload.get("warnings"), list) else [])
                        if isinstance(w, dict) and w.get("code") == "INVALID_PERIOD"), None)
        out.update({"status": "INVALID_PERIOD", "code": "INVALID_PERIOD", "message": invalid.get("message") if invalid else None})
    elif not rows:
        # SEC companyconcept facts carry no segment dimensions, so most filers need the filing's own segment table;
        # the table read honours the period and a pinned filing (2.25.1, F26).
        out.update(await _segment_revenue_from_filing_table(ticker, filing_type, warnings, period, pin))
    if warnings:
        out["warnings"] = warnings
    if str(detailLevel).lower() == "raw":
        out["rawContext"] = payload
    return json.dumps(out)


async def _segment_revenue_from_filing_table(
    ticker: str,
    filing_type: str,
    warnings: list[dict],
    period: str = "latest",
    pin: str | None = None,
) -> dict:
    """The Worker's segmentRevenueFromFilingTable: segment rows parsed from the filing's own table (2.25.1, F26)."""
    named = _sf.parse_filing_period(period)
    fiscal_year = named["year"] if named and named["kind"] == "fiscalYear" else None
    filing, error = await _resolve_sec_filing(ticker, filing_type, pin)
    if filing is None:
        error = error or {}
        return {"status": str(error.get("status") or error.get("code") or "FILING_NOT_FOUND"), "code": error.get("code"), "message": error.get("message")}
    warnings.extend(filing["warnings"])
    filing_meta = {
        "filingType": filing["filingType"],
        "filingDate": filing["filingDate"],
        "accessionNumber": filing["accessionNumber"],
        "documentUrl": filing["documentUrl"],
    }
    html = await _edgar_get_html(filing["documentUrl"], max_bytes=_SEC_DOCUMENT_READ_MAX_CHARS + 1)
    # The read is bounded in UTF-16 code units, as the Worker's string length.
    html_length = _utf16_length(html) if html else 0
    if html and html_length > _SEC_DOCUMENT_READ_MAX_CHARS:
        html = html.encode("utf-16-le", "surrogatepass")[: 2 * _SEC_DOCUMENT_READ_MAX_CHARS].decode("utf-16-le", "ignore")
        html_length = _utf16_length(html)
    if not html:
        return {**filing_meta, "status": "PROVIDER_ERROR", "code": "PROVIDER_ERROR", "message": "The filing document could not be read."}
    parsed = extract_segment_table_from_html(html, fiscal_year)
    result, tables_seen, tables_scanned, years_seen = parsed["result"], parsed["segmentTablesSeen"], parsed["tablesScanned"], parsed["yearsSeen"]
    truncated = html_length >= _SEC_DOCUMENT_READ_MAX_CHARS
    searched_terms = ["segment", "revenue", "net sales"]
    scan_coverage = {
        "sourceType": "sec_primary_html",
        "documentUrl": filing["documentUrl"],
        "charsScanned": html_length,
        "tablesScanned": tables_scanned,
        "segmentTablesSeen": tables_seen,
        "filingReadTruncated": truncated,
        "searchedTerms": searched_terms,
    }
    if result:
        result_period = f"FY{_re.sub(r'[^0-9]', '', result['column'])}" if result["column"] else None
        scale_assumed = result["unitScaleSource"] == "ASSUMED_MILLIONS"
        if scale_assumed:
            warnings.append(_UNIT_SCALE_ASSUMED_WARNING)
        mult = result["unitMultiplier"]
        total = result["total"]
        return {
            **filing_meta,
            "status": "FOUND",
            "extractionMethod": "PARSED_TABLE",
            "decisionGrade": False,
            "unitScale": result["unitScale"],
            "unitScaleSource": result["unitScaleSource"],
            "segments": [{
                "label": row["label"],
                "value": row["value"] * mult,
                "rawValue": row["rawValue"],
                "period": result_period,
                "confidence": "LOW" if scale_assumed else "MEDIUM",
                "evidence": {
                    "documentUrl": filing["documentUrl"],
                    "filingType": filing["filingType"],
                    "accessionNumber": filing["accessionNumber"],
                    "filingDate": filing["filingDate"],
                    "sourceTableId": result["tableIndex"],
                    "tableTitle": result["title"],
                    "sourceRows": [[row["label"], row["rawValue"]], [total["label"], total["rawValue"]]],
                    "sourceColumns": [result["column"]] if result["column"] else [],
                },
            } for row in result["segments"]],
            # A parsed table's total is no surer than its rows (2.5.21).
            "total": {"label": total["label"], "value": total["value"] * mult, "rawValue": total["rawValue"], "confidence": "LOW" if scale_assumed else "MEDIUM"},
            "evidence": {
                "sourceType": "sec_filing_table",
                "documentUrl": filing["documentUrl"],
                "tableIndex": result["tableIndex"],
                "tableTitle": result["title"],
                "column": result["column"],
            },
            "calculation": {"check": "segments sum to the table total within 1%"},
            "recommendedNextAction": "VERIFY_TABLE_PERIOD_AND_UNITS",
        }
    if fiscal_year is not None and years_seen and fiscal_year not in years_seen:
        # The filing's segment tables report other years: say so rather than return another year's rows (2.25.1, F26).
        return {
            **filing_meta,
            "status": "PERIOD_NOT_FOUND",
            "decisionGrade": False,
            "message": f"The segment tables in this {filing['filingType']} (filed {filing['filingDate']}) have columns for {', '.join(str(y) for y in years_seen)}, not fiscal year {fiscal_year}.",
            "fiscalYearsAvailable": years_seen,
            "scanCoverage": scan_coverage,
            "searchedTerms": searched_terms,
        }
    if tables_seen > 0 or truncated:
        if tables_seen > 0:
            warnings.append({"code": "TABLE_NOT_PARSED", "message": "The filing has tables introduced as segment revenue, but none parsed into segments that sum to a total.", "severity": "warning"})
        if truncated:
            warnings.append({"code": "FILING_READ_TRUNCATED", "message": "Only the first part of the filing was scanned; segment tables may appear later.", "severity": "warning"})
        return {**filing_meta, "status": "EXTRACTION_FAILED", "code": "EXTRACTION_FAILED", "decisionGrade": False, "scanCoverage": scan_coverage, "searchedTerms": searched_terms, "recommendedNextAction": "LIST_SEC_FILING_TABLES"}
    return {
        **filing_meta,
        "status": "NOT_DISCLOSED",
        "decisionGrade": False,
        "scanCoverage": scan_coverage,
        "searchedTerms": searched_terms,
        "notDisclosedBasis": "No XBRL segment facts and no table introduced as segment revenue in the full filing.",
    }


@yfinance_server.tool(name="extract_total_revenue", description="Extract total revenue from official SEC facts or filing tables with evidence metadata.")
async def extract_total_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
) -> str:
    return await _extract_total_revenue(ticker, filing_type, period)


async def _extract_total_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    # Internal only (not a tool param): a pinned caller reads the pinned filing (2.24.0, F21).
    accession_number: str | None = None,
) -> str:
    failure = _filing_period_failure("extract_total_revenue", period)
    if failure:
        return failure
    pin = str(accession_number).strip() if accession_number is not None and str(accession_number).strip() else None
    payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.total_revenue, filing_type=filing_type, period=period, accession_number=pin))
    val = payload.get("value")
    status = "FOUND" if val is not None else _as_status(payload)
    source_evidence = _sec_xbrl_source_evidence(payload)
    decision_grade = _is_decision_grade_sec_xbrl_fact(
        payload,
        source_evidence,
        status,
    )
    filing_evidence = {
        "filingType": payload.get("filingType", filing_type),
        "filingDate": payload.get("filingDate"),
        "accessionNumber": payload.get("accessionNumber"),
        "documentUrl": payload.get("documentUrl"),
    }
    return json.dumps({
        "ticker": ticker,
        "factType": "total_revenue",
        "value": val,
        "period": payload.get("period"),
        # No value, no unit: a missing fact has no currency (2.5.15).
        "unit": (payload.get("unit") or "USD") if val is not None else None,
        "unitScale": payload.get("unitScale"),
        "confidence": _nn(payload.get("confidence"), "NOT_DISCLOSED" if val is None else "LOW"),
        "extractionMethod": payload.get("extractionMethod", "NONE"),
        "evidence": source_evidence if decision_grade else filing_evidence,
        "sourceEvidence": source_evidence,
        "xbrlContext": payload.get("xbrlContext"),
        "decisionGrade": decision_grade,
        "status": status,
        # Why there is no value (NO_SEC_REGISTRANT, SEC_FACTS_IFRS_ONLY, ...) and the read's warnings (2.5.15).
        "code": None if val is not None else (payload.get("code") if isinstance(payload.get("code"), str) else None),
        "message": None if val is not None else (payload.get("message") if isinstance(payload.get("message"), str) else None),
        # A failed SEC read says so and which reads failed; retry rather than treat the fact as missing (2.5.17).
        **({"retryable": True} if val is None and payload.get("retryable") is True else {}),
        **({"failedReads": payload["failedReads"]} if val is None and isinstance(payload.get("failedReads"), list) else {}),
        "warnings": payload.get("warnings") if isinstance(payload.get("warnings"), list) else [],
    })


@yfinance_server.tool(name="extract_revenue_exposure", description="Extract revenue exposure for a region/customer/segment query, returning explicit parser/provider limitation statuses when no decision-grade value is available.")
async def extract_revenue_exposure(
    ticker: str,
    exposure_query: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    detailLevel: str = "compact",
) -> str:
    return await _extract_revenue_exposure(ticker, exposure_query, filing_type, period, detailLevel)


async def _extract_revenue_exposure(
    ticker: str,
    exposure_query: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
    # Internal only (not a tool param): a pinned caller reads the pinned filing (2.23.0, F3).
    accession_number: str | None = None,
) -> str:
    failure = _filing_period_failure("extract_revenue_exposure", period)
    if failure:
        return failure
    geo = _safe_json_loads(await extract_geographic_revenue(ticker=ticker, region=exposure_query, filing_type=filing_type, period=period, accession_number=accession_number, detailLevel=detailLevel))
    found = geo.get("value") is not None
    status = "FOUND_REVENUE_EXPOSURE" if found else _as_status(geo)
    warnings = geo["warnings"] if isinstance(geo.get("warnings"), list) else []
    # A value read from a product or category table ("iPhone" in "net sales by category") is segment revenue, not
    # geographic revenue (2.25.4 audit, F40). The filing is cached, so re-reading its chosen table costs no request.
    exposure_type = "geographic_revenue"
    parsed_evidence = geo.get("evidence") if isinstance(geo.get("evidence"), dict) else {}
    parsed_url = parsed_evidence.get("documentUrl") or geo.get("documentUrl")
    if found and geo.get("extractionMethod") == "PARSED_TABLE" and isinstance(parsed_url, str) and parsed_url:
        html_text = await _edgar_get_html(parsed_url, max_bytes=5_000_000)
        table = _extract_geo_table(html_text, exposure_query) if html_text else None
        if table and _PRODUCT_BREAKDOWN_LEAD_RE.search(table.get("tableLead") or ""):
            exposure_type = "segment_revenue"
    matches = []
    if found:
        matches.append({
            "exposureType": exposure_type,
            "label": exposure_query,
            "value": geo.get("value"),
            "denominator": geo.get("denominator"),
            "valueRatio": geo.get("valueRatio"),
            "valuePct": geo.get("valuePct"),
            "period": geo.get("period"),
            "confidence": _nn(geo.get("confidence"), "LOW"),
            "evidence": _nn(geo.get("evidence"), {}),
        })
    geo_evidence = geo["evidence"] if isinstance(geo.get("evidence"), dict) else {}
    return json.dumps({
        "ticker": ticker,
        "query": exposure_query,
        "matches": matches,
        "status": status,
        # Why there is no value (SEC_READ_FAILED, FILING_NOT_FOUND_TRY_OTHER_TYPE, ...); a plain NOT_DISCLOSED has no code.
        "code": None if found else _nn(geo.get("code"), None if status == "NOT_DISCLOSED" else status),
        "requestedFilingType": _nn(geo.get("requestedFilingType"), filing_type),
        "filingType": _nn(geo_evidence.get("filingType"), _nn(geo.get("filingType"), filing_type)),
        "filingDate": _nn(geo_evidence.get("filingDate"), geo.get("filingDate")),
        "accessionNumber": _nn(geo_evidence.get("accessionNumber"), geo.get("accessionNumber")),
        "documentUrl": _nn(geo_evidence.get("documentUrl"), geo.get("documentUrl")),
        "availableFilingTypes": _nn(geo.get("availableFilingTypes"), []),
        "suggestedFilingTypes": _nn(geo.get("suggestedFilingTypes"), []),
        "warnings": warnings,
    })


_EXPOSURE_XBRL_TOKEN_RE = _re.compile(
    r"\b(?:us-gaap|srt|country|dei):[A-Za-z0-9_.-]+\b",
    _re.IGNORECASE,
)
_EXPOSURE_COMPETITION_RE = _re.compile(
    r"\b(?:competitors?|competition|compete(?:s|d|ing)?\s+with)\b",
    _re.IGNORECASE,
)
_EXPOSURE_RELATIONSHIP_RE = _re.compile(
    r"\b(?:supplier|vendor|customer|manufactur\w*|assembl\w*|facilit\w*|factory|"
    r"subsidiar\w*|operations?|production|procur\w*|sourc\w*|partner\w*|"
    r"loan|credit|deposit|account|lender|borrow\w*)\b",
    _re.IGNORECASE,
)


def _exposure_term_match(text: str, term: str):
    """First whole-word match of term, plural forms included ("tariff" matches "tariffs")."""
    escaped = _re.escape(str(term).strip())
    if not escaped:
        return None
    return _re.search(rf"(?<![A-Za-z0-9]){escaped}(?:e?s)?(?![A-Za-z0-9])", text, _re.IGNORECASE)


def _exposure_term_present(text: str, term: str) -> bool:
    return _exposure_term_match(text, term) is not None


def _term_centered_excerpt(text: str, terms: list[str], max_len: int) -> str:
    """About max_len characters around the first term, starting at its sentence when that fits."""
    cleaned = " ".join(text.split())
    if len(cleaned) <= max_len:
        return cleaned
    starts = [m.start() for m in (_exposure_term_match(cleaned, term) for term in terms) if m]
    if not starts:
        return _compact_excerpt(cleaned, max_len)
    at = min(starts)
    start = max(0, min(at - max_len // 3, len(cleaned) - max_len))
    sentence = cleaned.rfind(". ", start, at)
    if sentence >= 0:
        start = sentence + 2
    elif start > 0:
        space = cleaned.find(" ", start, at)
        if space >= 0:
            start = space + 1
    end = start + max_len
    if end < len(cleaned):
        space = cleaned.rfind(" ", at, end)
        if space > at:
            end = space
    return f"{'...' if start > 0 else ''}{cleaned[start:end].strip()}{'...' if end < len(cleaned) else ''}"


def _readable_exposure_excerpt(
    value: object,
    terms: list[str],
    *,
    max_len: int = 240,
    min_words: int = 5,
) -> str:
    cleaned = _EXPOSURE_XBRL_TOKEN_RE.sub(" ", str(value or ""))
    excerpt = _term_centered_excerpt(cleaned, terms, max_len)
    if not excerpt or not any(_exposure_term_present(excerpt, term) for term in terms):
        return ""
    if len(_re.findall(r"[A-Za-z]{2,}", excerpt)) < min_words:
        return ""
    return excerpt


def _contextual_entity_excerpt(value: object, entity: str, *, max_len: int = 240) -> str:
    excerpt = _readable_exposure_excerpt(value, [entity], max_len=max_len, min_words=4)
    if not excerpt or _EXPOSURE_COMPETITION_RE.search(excerpt):
        return ""
    return excerpt if _EXPOSURE_RELATIONSHIP_RE.search(excerpt) else ""


@yfinance_server.tool(name="extract_risk_factor_mentions", description="Extract concise risk-factor mentions for explicit terms from a filing.")
async def extract_risk_factor_mentions(
    ticker: str,
    terms: list[str],
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    return await _extract_risk_factor_mentions(ticker, terms, filing_type, period, detailLevel)


async def _extract_risk_factor_mentions(
    ticker: str,
    terms: list[str],
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
    # Internal only (not a tool param): a pinned caller reads the pinned filing (2.24.0, F21).
    accession_number: str | None = None,
) -> str:
    matches: list[dict] = []
    rejected_noise_count = 0
    raw_match_count = 0
    for term in (terms or []):
        search = _safe_json_loads(await search_sec_filing_text(
            ticker=ticker,
            search_terms=[str(term)],
            section_hint="Risk Factors",
            filing_type=filing_type,
            accession_number=accession_number,
        ))
        raw_match_count += int(search.get("matchCount") or 0)
        for m in (search.get("matches") if isinstance(search.get("matches"), list) else [])[:3]:
            if not isinstance(m, dict):
                continue
            row_term = str(m.get("term") or "")
            heading = str(m.get("sectionHeading") or "Risk Factors")
            if row_term and row_term.lower() != str(term).lower():
                rejected_noise_count += 1
                continue
            if m.get("sectionHeading") and "risk" not in heading.lower():
                rejected_noise_count += 1
                continue
            excerpt = _readable_exposure_excerpt(
                m.get("contextText") or m.get("context") or m.get("excerpt") or "",
                [str(term)],
            )
            if not excerpt:
                rejected_noise_count += 1
                continue
            matches.append({
                "term": term,
                "sectionHeading": heading,
                "excerpt": excerpt,
                "excerptAvailable": True,
                "confidence": "MEDIUM",
                "evidence": {
                    "filingDate": search.get("filingDate"),
                    "accessionNumber": search.get("accessionNumber"),
                    "documentUrl": search.get("documentUrl"),
                },
            })
    result = {
        "ticker": ticker,
        "matches": matches,
        "status": "FOUND" if matches else ("FOUND_NO_EXCERPT" if raw_match_count else "NOT_FOUND"),
        "rawMatchCount": raw_match_count,
        "rejectedNoiseCount": rejected_noise_count,
    }
    if str(detailLevel).lower() == "raw":
        result["rawTerms"] = terms or []
    return json.dumps(result)


_CUSTOMER_TABLE_ROW_TERMS = ["Customer A", "Customer B", "Customer C", "Customer 1", "Distributor A", "Distributor B"]


@yfinance_server.tool(name="extract_customer_concentration", description="Extract customer concentration percentages from SEC filing text evidence.")
async def extract_customer_concentration(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    return await _extract_customer_concentration(ticker, filing_type, period, detailLevel)


async def _extract_customer_concentration(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
    # Internal only (not a tool param): a pinned caller reads the pinned filing (2.24.0, F21).
    accession_number: str | None = None,
) -> str:
    # Mirrors the Worker: revenue-concentration rules live in yfmcp/extraction_rules.py.
    searched_terms = ["major customer", "customers", "customer accounted", "accounted for", "of our revenue", "of total revenue", "of net sales", "percent of revenue"]
    search = _safe_json_loads(await search_sec_filing_text(ticker=ticker, search_terms=searched_terms, filing_type=filing_type, accession_number=accession_number, context_chars=1200))
    matches = [m for m in (search.get("matches") if isinstance(search.get("matches"), list) else []) if isinstance(m, dict)]
    # The rows of a significant-customer table in the same filing, which the prose search does not reach (2.5.32,
    # F-024: MRVL's "Distributor A | 37% | 34% | 24%").
    if search.get("accessionNumber"):
        table_search = _safe_json_loads(await search_sec_filing_text(
            ticker=ticker, search_terms=_CUSTOMER_TABLE_ROW_TERMS, filing_type=filing_type, accession_number=str(search["accessionNumber"]),
            max_matches=12, context_chars=1200))
        matches += [m for m in (table_search.get("matches") if isinstance(table_search.get("matches"), list) else []) if isinstance(m, dict) and m.get("inTable") is True]
    filing_evidence = {
        "filingDate": search.get("filingDate"),
        "accessionNumber": search.get("accessionNumber"),
        "documentUrl": search.get("documentUrl"),
    }
    fiscal_year = str(search.get("fiscalYear") or "")
    fallback_period = (fiscal_year if fiscal_year.upper().startswith("FY") else f"FY{fiscal_year}") if fiscal_year else None
    rules = _er.customer_concentration(matches)

    def _evidence(f: dict) -> dict:
        return {"sectionHeading": f["sectionHeading"], "excerpt": _compact_excerpt(f["sentence"]), **filing_evidence}

    def _period(f: dict) -> str | None:
        return f"FY{f['year']}" if f["year"] is not None else fallback_period

    customers = [{
        "label": f["name"] or "Unnamed customer",
        "name": f["name"],
        "nameDisclosed": f["name"] is not None,
        "description": f["description"],
        "valuePct": f["valuePct"],
        "period": _period(f),
        "confidence": "MEDIUM",
        "evidence": _evidence(f),
    } for f in rules["findings"] if f["kind"] == "customer"][:8]
    aggregates = [{
        "description": f["description"],
        "valuePct": f["valuePct"],
        "period": _period(f),
        "confidence": "MEDIUM",
        "evidence": _evidence(f),
    } for f in rules["findings"] if f["kind"] == "aggregate"][:4]
    negation = rules["negation"]
    match_count = int(search.get("matchCount") or 0)
    scan_coverage = {"sourceType": "sec_primary_html", **filing_evidence, "matchCount": match_count, "searchedTerms": searched_terms}
    result: dict = {"ticker": ticker, "customers": customers, "aggregates": aggregates}
    if customers or aggregates:
        result["status"] = "FOUND"
    elif negation:
        result.update({
            "status": "NOT_DISCLOSED",
            "notDisclosedBasis": "The filing states that no customer reached the disclosure threshold.",
            "evidence": {"sectionHeading": negation["sectionHeading"], "excerpt": _compact_excerpt(negation["sentence"]), **filing_evidence},
            "scanCoverage": scan_coverage,
        })
    elif match_count > 0:
        result.update({
            "status": "EXTRACTION_FAILED",
            "code": "EXTRACTION_FAILED",
            "scanCoverage": scan_coverage,
            "warnings": [{"code": "CUSTOMER_PERCENT_NOT_PARSED", "message": "Customer disclosures were found, but no customer percentage or no-major-customer statement was parsed.", "severity": "warning"}],
        })
    else:
        result.update({"status": "NOT_FOUND", "scanCoverage": scan_coverage})
    if str(detailLevel).lower() == "raw":
        result["rawMatchCount"] = match_count
    return json.dumps(result)


# ── Dilution bridge, capital structure, analyst methods, UK filings (2.3.0) ──
# Mirrors the Worker; the parsing and arithmetic live in yfmcp.capital_structure.

_IXBRL_DOCUMENTS: dict[str, tuple[_cs.IxDocument, bool, float]] = {}
_IXBRL_DOCUMENT_TTL = 30 * 60
_IXBRL_DOCUMENT_MAX = 8
_AWARD_TABLE_SEARCH_TERMS = ["unvested", "nonvested"]
_ATM_SEARCH_TERMS = ["at-the-market", "at the market offering", "ATM program", "equity distribution agreement", "sales agreement"]
_FUNDING_SEARCH_TERMS = ["sufficient to fund", "sufficient to meet", "fully funded", "going concern", "cash runway", "next twelve months", "next 12 months", "at-the-market", "capital expenditures"]


def _resolve_failure(error: dict, ticker: str) -> str:
    """The Worker's resolveFailure: ACCESSION_NOT_FOUND as its error envelope, any other resolver error with the ticker."""
    if _is_accession_not_found(error):
        return _accession_not_found_envelope(error)
    return json.dumps({"ticker": ticker, **error})


async def _ixbrl_source(role: str, filing: dict) -> tuple[_cs.IxSource, bool] | None:
    url = filing["documentUrl"]
    hit = _IXBRL_DOCUMENTS.get(url)
    if hit is not None and time.monotonic() - hit[2] < _IXBRL_DOCUMENT_TTL:
        doc, truncated = hit[0], hit[1]
    else:
        html = await _edgar_get_html(url, max_bytes=_SEC_DOCUMENT_READ_MAX_CHARS + 1)
        if not html:
            return None
        truncated = len(html) > _SEC_DOCUMENT_READ_MAX_CHARS
        doc = _cs.parse_ixbrl(html[:_SEC_DOCUMENT_READ_MAX_CHARS])
        _IXBRL_DOCUMENTS.pop(url, None)
        _IXBRL_DOCUMENTS[url] = (doc, truncated, time.monotonic())
        while len(_IXBRL_DOCUMENTS) > _IXBRL_DOCUMENT_MAX:
            _IXBRL_DOCUMENTS.pop(next(iter(_IXBRL_DOCUMENTS)))
    source = _cs.IxSource(role, filing["filingType"], filing["filingDate"], filing["accessionNumber"], url, doc)
    return source, truncated


async def _filing_text_matches(ticker: str, filing: dict, terms: list[str], max_matches: int, context_chars: int) -> list[_cs.TextMatch]:
    search = _safe_json_loads(await search_filing_text(
        ticker, terms, None, filing["filingType"], filing["accessionNumber"], context_chars, False, None, max_matches=max_matches,
    ))
    return _text_matches_from(search)


async def _share_claim_matches(ticker: str, filings: list[tuple[str, dict]]) -> list[_cs.TextMatch] | None:
    """Share-claim text across the resolved filings; None when a filing's text could not be searched."""
    out: list[_cs.TextMatch] = []
    for _, filing in filings:
        search = _safe_json_loads(await search_filing_text(
            ticker, _cs.SHARE_CLAIM_SEARCH_TERMS, None, filing["filingType"], filing["accessionNumber"], 900, False, None, max_matches=32,
        ))
        if search.get("error") or search.get("code") or not isinstance(search.get("matches"), list):
            return None
        out.extend(_text_matches_from(search))
    return out


async def _warrant_lifecycle_matches(ticker: str, filings: list[tuple[str, dict]], counts: list[float]) -> list[_cs.TextMatch] | None:
    """Exercise, expiry and redemption text for warrant classes counted before the period end (2.5.11).

    Searched by the lifecycle phrases and each class's count; None when a filing's text could not be searched.
    """
    terms = [*_cs.WARRANT_LIFECYCLE_SEARCH_TERMS,
             *(f"{int(n):,}" for n in counts if isinstance(n, (int, float)) and not isinstance(n, bool) and float(n).is_integer() and n >= 1000)]
    return await _searched_matches(ticker, filings, terms)


async def _searched_matches(ticker: str, filings: list[tuple[str, dict]], terms: list[str]) -> list[_cs.TextMatch] | None:
    """Text matches for the terms across the resolved filings; None when a filing's text could not be searched."""
    out: list[_cs.TextMatch] = []
    for _, filing in filings:
        search = _safe_json_loads(await search_filing_text(
            ticker, terms, None, filing["filingType"], filing["accessionNumber"], 900, False, None, max_matches=32,
        ))
        if search.get("error") or search.get("code") or not isinstance(search.get("matches"), list):
            return None
        out.extend(_text_matches_from(search))
    return out


def _text_matches_from(search: dict) -> list[_cs.TextMatch]:
    out = []
    for m in search.get("matches") if isinstance(search.get("matches"), list) else []:
        if not isinstance(m, dict):
            continue
        heading = m.get("sectionHeading")
        out.append(_cs.TextMatch(
            context_text=str(m.get("contextText") or ""),
            section_heading=heading if isinstance(heading, str) and heading else None,
            document_url=_first_present(m.get("documentUrl"), search.get("documentUrl")),
            filing_date=_first_present(m.get("filingDate"), search.get("filingDate")),
            accession_number=_first_present(m.get("accessionNumber"), search.get("accessionNumber")),
            in_table=m.get("inTable") is True,
            table_title=m.get("tableTitle") if isinstance(m.get("tableTitle"), str) else None,
            row_label=m.get("rowLabel") if isinstance(m.get("rowLabel"), str) else None,
        ))
    return out


def _first_present(*values: Any) -> Any:
    return next((v for v in values if v is not None), None)


@yfinance_server.tool(
    name="extract_dilution_bridge",
    description="Basic-to-diluted share bridge at a price you supply, from the filing's inline XBRL: cover-page basic shares, options (treasury-stock method, by exercise-price range when tagged), unvested RSUs/PSUs (gross), warrants per class (treasury stock), convertibles and convertible preferred (if-converted when in the money) and ATM remaining capacity from filing text. Mechanical and company-disclosed, not a consensus diluted share count; never back-solve it into one. status covers tagged instruments only, never a full claim inventory: share claims stated only in filing text (price protection, anti-dilution rights, forward sales, contingent or share-settled obligations, exchangeable interests, SAFEs, equity lines, post-period warrants) are quoted in unquantifiedShareClaims and never counted. filing_type latest uses the newest 10-Q with the last 10-K as fallback.",
)
async def extract_dilution_bridge(
    ticker: str,
    price: float,
    as_of_date: str | None = None,
    currency: str = "USD",
    filing_type: str = "latest",
    accession_number: str | None = None,
    include_atm: bool = True,
) -> str:
    try:
        price_value = float(price)
    except (TypeError, ValueError):
        price_value = math.nan
    if not math.isfinite(price_value) or price_value <= 0:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "price is required and must be a positive number: the bridge is evaluated at the price you supply."})
    filings, error = await _resolve_periodic_filings(ticker, filing_type, accession_number)
    if error:
        return _resolve_failure(error, ticker)
    loaded = [await _ixbrl_source(role, filing) for role, filing in filings]
    sources = [item[0] for item in loaded if item is not None]
    if not sources:
        return json.dumps({"ticker": ticker, "status": "FILING_TEXT_NOT_AVAILABLE", "code": "FILING_TEXT_NOT_AVAILABLE", "message": "The filing document could not be read from SEC."})
    atm_matches: list[_cs.TextMatch] = []
    if include_atm is not False:
        for _, filing in filings:
            atm_matches = await _filing_text_matches(ticker, filing, _ATM_SEARCH_TERMS, 10, 800)
            if atm_matches:
                break
    claim_matches = await _share_claim_matches(ticker, filings)
    bridge_args = (
        ticker.upper(), price_value, (currency or "USD").upper(),
        as_of_date if as_of_date and _re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", as_of_date) else None,
        sources, atm_matches,
    )
    out = _cs.dilution_bridge(*bridge_args, None, claim_matches)
    # A warrant class counted before the period end: read the filing text for its exercise, expiry or redemption (2.5.11).
    lifecycle_matches: list[_cs.TextMatch] | None = None
    warrant_component = next((c for c in out["components"] if c.get("component") == "warrants"), None)
    early_counts = [c.get("outstanding") for c in ((warrant_component or {}).get("classes") or []) if c.get("countBeforePeriodEnd") is True]
    if early_counts:
        lifecycle_matches = await _warrant_lifecycle_matches(ticker, filings, early_counts)
    # Convertible notes: read the filing text for a stated cash settlement of principal (2.5.11).
    settlement_matches: list[_cs.TextMatch] | None = None
    capped_call_matches: list[_cs.TextMatch] | None = None
    has_convertibles = any(c.get("component") == "convertible_debt" for c in out["components"])
    if has_convertibles:
        settlement_matches = await _searched_matches(ticker, filings, list(_cs.CONVERTIBLE_SETTLEMENT_SEARCH_TERMS))
        # And for capped call terms (2.5.12).
        capped_call_matches = await _searched_matches(ticker, filings, list(_cs.CAPPED_CALL_SEARCH_TERMS))
    if early_counts or has_convertibles:
        out = _cs.dilution_bridge(*bridge_args, None, claim_matches, lifecycle_matches, settlement_matches, capped_call_matches)
    # No unvested award count is tagged: read it from the filing's award table.
    if "unvested_share_awards" in out["notDisclosed"]:
        for _, filing in filings:
            table_matches = [m for m in await _filing_text_matches(ticker, filing, _AWARD_TABLE_SEARCH_TERMS, 30, 400) if m.in_table]
            if not table_matches:
                continue
            retried = _cs.dilution_bridge(*bridge_args, table_matches, claim_matches, lifecycle_matches, settlement_matches, capped_call_matches)
            if "unvested_share_awards" not in retried["notDisclosed"]:
                out = retried
                break
    if any(item is not None and item[1] for item in loaded):
        out["warnings"].append({"code": "FILING_READ_TRUNCATED", "message": "A filing exceeded the read limit; facts past that point were not parsed.", "severity": "warning"})
    return json.dumps(out)


@yfinance_server.tool(
    name="extract_capital_structure",
    description="Company-disclosed capital structure at the filing's period end: cash, short-term investments, total debt and net cash; each debt instrument's face amount, carrying amount, coupon, maturity and conversion terms from dimensional inline XBRL (which companyfacts omits); the tagged maturity ladder; and the company's own funding, runway, going-concern and ATM statements quoted from the filing. Disclosed, not forecast.",
)
async def extract_capital_structure(
    ticker: str,
    filing_type: str = "latest",
    accession_number: str | None = None,
    include_funding_statements: bool = True,
) -> str:
    filings, error = await _resolve_periodic_filings(ticker, filing_type, accession_number)
    if error:
        return _resolve_failure(error, ticker)
    _, primary = filings[0]
    loaded = await _ixbrl_source("primary", primary)
    if loaded is None:
        return json.dumps({"ticker": ticker, "status": "FILING_TEXT_NOT_AVAILABLE", "code": "FILING_TEXT_NOT_AVAILABLE", "message": "The filing document could not be read from SEC."})
    funding = await _filing_text_matches(ticker, primary, _FUNDING_SEARCH_TERMS, 20, 700) if include_funding_statements is not False else []
    out = _cs.capital_structure(ticker.upper(), loaded[0], funding)
    # An outstanding instrument with no tagged maturity: read the filing's own maturity sentences (2.5.24).
    if _cs.wants_maturity_text(out):
        maturity_matches = await _filing_text_matches(ticker, primary, _cs.MATURITY_SEARCH_TERMS, 12, 400)
        out = _cs.capital_structure(ticker.upper(), loaded[0], funding, maturity_matches)
    if loaded[1]:
        out["warnings"].append({"code": "FILING_READ_TRUNCATED", "message": "The filing exceeded the read limit; facts past that point were not parsed.", "severity": "warning"})
    return json.dumps(out)


from yfmcp import share_scenarios as _ss  # noqa: E402
from yfmcp import funding_schedule as _fsched  # noqa: E402


@yfinance_server.tool(
    name="get_share_count_scenarios",
    description="Share counts under scenarios you define: for each scenario's price and treatments (options treasury-stock/gross/exclude, unvested awards gross/exclude, warrants treasury-stock/gross/exclude on vested or all, convertibles if-converted when in the money/all/net-share settlement where the filing states principal is settled in cash/exclude, ATM remaining capacity include/exclude, capped calls ignored or offset where the filing states their strike, cap and covered shares, and known issuance you supply), every instrument from the filing's inline XBRL is listed as included or excluded with its treasury-stock or if-converted mechanics and threshold price; unresolved instruments are listed and left out. Scenarios are reported side by side; no denominator is selected. Evidence only.",
)
async def get_share_count_scenarios(
    ticker: str,
    scenarios: list[dict],
    filing_type: str = "latest",
    accession_number: str | None = None,
    currency: str = "USD",
) -> str:
    parsed = _ss.parse_share_scenarios(scenarios)
    if "error" in parsed:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": parsed["error"]})
    raw = await extract_dilution_bridge(ticker=ticker, price=parsed["scenarios"][0]["price"], currency=currency, filing_type=filing_type,
                                        accession_number=accession_number, include_atm=True)
    bridge = _unwrap_payload(raw)
    if bridge.get("error") is True or bridge.get("basis") != "MECHANICAL_COMPANY_DISCLOSED":
        return raw
    return json.dumps(_ss.share_count_scenarios(ticker, bridge, parsed["scenarios"]))


_SCHEDULE_SEARCH_TERMS = [
    "capital expenditures", "purchase commitments", "purchase obligations", "non-cancelable", "committed to",
    "contractual obligations", "grant agreement", "awarded", "milestone payments", "CHIPS", "incentive agreement",
    "expect to invest", "sufficient to fund", "at-the-market",
]


@yfinance_server.tool(
    name="extract_funding_capex_schedule",
    description="Funding and capex schedule from the latest periodic filing: tagged contractual schedules (debt principal, operating and finance lease payments, purchase and contractual obligations by due period; purchase commitments) and funding or capex statements from the text, each classified CONTRACTUAL, COMPANY_DISCLOSED_COMMITTED, COMPANY_GUIDED, AWARDED_CONTINGENT or UNRESOLVED with amount, timing and the quoted source; plus liquidity sources (cash, undrawn facilities, ATM capacity). Nothing is netted or forecast. Evidence only.",
)
async def extract_funding_capex_schedule(ticker: str, filing_type: str = "latest", accession_number: str | None = None) -> str:
    filings, error = await _resolve_periodic_filings(ticker, filing_type, accession_number)
    if error:
        return _resolve_failure(error, ticker)
    _, primary = filings[0]
    loaded = await _ixbrl_source("primary", primary)
    if loaded is None:
        return json.dumps({"ticker": ticker, "status": "FILING_TEXT_NOT_AVAILABLE", "code": "FILING_TEXT_NOT_AVAILABLE", "message": "The filing document could not be read from SEC."})
    matches = await _filing_text_matches(ticker, primary, _SCHEDULE_SEARCH_TERMS, 30, 700)
    source, truncated = loaded
    capital = _cs.capital_structure(ticker.upper(), source, [])
    remaining = next((e for e in _cs.atm_evidence(matches) if e.get("kind") == "remaining_capacity"), None)
    doc = source.doc
    out = _fsched.funding_capex_schedule(
        ticker=ticker,
        period_end=doc.document_period_end,
        source=capital.get("source"),
        facts=[{"local": f.local, "value": f.value, "periodEnd": f.period_end, "dims": f.dims, "unit": f.unit} for f in doc.facts],
        statements=[{"contextText": m.context_text, "sectionHeading": m.section_heading, "documentUrl": m.document_url,
                     "filingDate": m.filing_date, "accessionNumber": m.accession_number} for m in matches],
        balances=capital.get("balances"),
        atm_remaining_usd=remaining["amountUsd"] if remaining else None,
        atm_evidence=remaining,
    )
    if truncated:
        out["warnings"].append({"code": "FILING_READ_TRUNCATED", "message": "The filing exceeded the read limit; facts past that point were not parsed.", "severity": "warning"})
    return json.dumps(out)


_DRIVER_SEARCH_TERMS = [
    "capacity", "backlog", "bookings", "deliveries", "shipments", "customers", "subscribers", "launched", "deployed",
    "production", "utilization", "average selling price", "employees", "remaining performance obligations",
]


def _earnings_release_picks(subs: dict, cik_int: int, limit: int, include_amendments: bool = False) -> list[dict]:
    """The newest earnings-release 8-Ks (Item 2.02, results of operations) in SEC's recent submissions; 8-K/As too when asked."""
    recent = (subs.get("filings") or {}).get("recent") or {}

    def at(key: str, i: int) -> str:
        values = recent.get(key) or []
        return str(values[i] or "") if i < len(values) else ""

    picks: list[dict] = []
    for i, form in enumerate(recent.get("form") or []):
        if len(picks) >= limit:
            break
        form_upper = str(form or "").upper()
        if not (form_upper == "8-K" or (include_amendments and form_upper == "8-K/A")) or "2.02" not in at("items", i):
            continue
        acc, doc = at("accessionNumber", i), at("primaryDocument", i)
        if not acc or not doc:
            continue
        picks.append({"filingDate": at("filingDate", i), "accessionNumber": acc,
                      "primaryUrl": sec_archive_document_url(cik_int, acc.replace('-', ''), doc), "form": form_upper})
    return picks


async def _read_release_text(cik_int: int, accession_number: str, primary_url: str) -> dict:
    """An earnings-release 8-K's text: EX-99.1 when the index names one, else the 8-K itself."""
    from yfmcp.tools.earnings import _resolve_ex991_url
    try:
        ex991 = await _resolve_ex991_url(accession_number, cik_int)
    except Exception:  # noqa: BLE001 - fall back to the 8-K document
        ex991 = None
    url = ex991 or primary_url
    try:
        html = await _edgar_get_html(url, max_bytes=5_000_000)
    except Exception:  # noqa: BLE001 - reported as unread
        html = None
    if not html:
        return {"url": url, "status": "NOT_READ", "text": None}
    return {"url": url, "status": "READ", "text": _strip_html_blocks(_sanitize_sec_html(html))}


@yfinance_server.tool(
    name="extract_operating_driver_ledger",
    description="Operating drivers as the company discloses them: SEC XBRL series (revenue, gross profit, R&D, capex, remaining performance obligations, contract liabilities; newest filing per period, year-to-date periods kept as filed), the filer's own tagged KPIs from its latest periodic filing, and filing and earnings-release statements with figures by category (capacity, production, deliveries, launches and deployments, customers, backlog and bookings, utilization, pricing, yield, headcount), each marked REPORTED_ACTUAL, TARGET_OR_PLAN or UNCLEAR with timing and the quoted source. Untagged drivers are listed as not reported and unread sources as not read; nothing is derived or annualized. Evidence only.",
)
async def extract_operating_driver_ledger(ticker: str, filing_type: str = "latest", accession_number: str | None = None) -> str:
    filings, error = await _resolve_periodic_filings(ticker, filing_type, accession_number)
    if error:
        return _resolve_failure(error, ticker)
    _, primary = filings[0]
    cik_padded, subs = await _get_submissions_for_ticker(ticker)

    async def release_text() -> dict:
        # The newest results-of-operations 8-K, as in get_guidance_history.
        picks = _earnings_release_picks(subs, int(cik_padded), 1) if cik_padded and subs else []
        if not picks:
            return {"status": "RELEASE_NOT_RESOLVED", "url": None, "filingDate": None, "accessionNumber": None, "text": None}
        read = await _read_release_text(int(cik_padded), picks[0]["accessionNumber"], picks[0]["primaryUrl"])
        return {**read, "filingDate": picks[0]["filingDate"], "accessionNumber": picks[0]["accessionNumber"]}

    async def companyfacts() -> dict | None:
        return await _edgar_get_company_facts(cik_padded) if cik_padded else None

    loaded, matches, facts, release = await asyncio.gather(
        _ixbrl_source("primary", primary),
        _filing_text_matches(ticker, primary, _DRIVER_SEARCH_TERMS, 30, 700),
        companyfacts(),
        release_text(),
        return_exceptions=True,
    )
    loaded = None if isinstance(loaded, BaseException) else loaded
    matches = [] if isinstance(matches, BaseException) else matches
    facts = None if isinstance(facts, BaseException) else facts
    if isinstance(release, BaseException):
        release = {"status": "RELEASE_NOT_RESOLVED", "url": None, "filingDate": None, "accessionNumber": None, "text": None}
    statements = []
    if isinstance(release.get("text"), str):
        statements.append({"contextText": release["text"], "sectionHeading": None, "documentUrl": release.get("url"),
                           "filingDate": release.get("filingDate"), "accessionNumber": release.get("accessionNumber"), "source": "EARNINGS_RELEASE"})
    statements += [{"contextText": m.context_text, "sectionHeading": m.section_heading, "documentUrl": m.document_url,
                    "filingDate": m.filing_date, "accessionNumber": m.accession_number, "source": "PERIODIC_FILING"} for m in matches]
    source, truncated = loaded if loaded else (None, False)
    out = _dl.operating_driver_ledger(
        ticker=ticker,
        companyfacts=facts,
        inline_facts=[{"name": f.name, "unit": f.unit, "value": f.value, "periodStart": f.period_start, "periodEnd": f.period_end, "dims": f.dims}
                      for f in source.doc.facts] if source else None,
        filing={"filingType": primary["filingType"], "filingDate": primary["filingDate"], "accessionNumber": primary["accessionNumber"],
                "documentUrl": primary["documentUrl"], "status": "READ" if source else "NOT_READ", "textMatches": len(matches)},
        release={"filingDate": release.get("filingDate"), "accessionNumber": release.get("accessionNumber"), "url": release.get("url"), "status": release["status"]},
        statements=statements,
    )
    warnings = []
    if not source:
        warnings.append({"code": "FILING_TEXT_NOT_AVAILABLE", "message": "The periodic filing could not be read from SEC, so company-specific series were not read; retry.", "severity": "warning"})
    elif truncated:
        warnings.append({"code": "FILING_READ_TRUNCATED", "message": "The filing exceeded the read limit; facts past that point were not parsed.", "severity": "warning"})
    if not facts:
        warnings.append({"code": "COMPANYFACTS_NOT_AVAILABLE", "message": "SEC companyfacts could not be read, so standard driver series were not read; retry.", "severity": "warning"})
    if release["status"] == "NOT_READ":
        warnings.append({"code": "RELEASE_TEXT_NOT_AVAILABLE", "message": f"The earnings release {release.get('url')} could not be read from SEC; retry.", "severity": "warning"})
    elif release["status"] == "RELEASE_NOT_RESOLVED":
        warnings.append({"code": "RELEASE_NOT_RESOLVED", "message": "No SEC earnings release was resolved, so release statements were not read.", "severity": "warning"})
    out["status"] = "PARTIAL" if any(w["code"] not in ("FILING_READ_TRUNCATED", "RELEASE_NOT_RESOLVED") for w in warnings) else "OK"
    out["retryable"] = not source or not facts or release["status"] == "NOT_READ"
    out["warnings"] = warnings
    return json.dumps(out)


@yfinance_server.tool(
    name="get_guidance_history",
    description="Company guidance across recent earnings releases (SEC 8-K Item 2.02, EX-99.1): each revenue, gross-margin and EPS range with its stated target period and source; revisions between consecutive releases for the same metric and period (INITIATED, RAISED, LOWERED, NARROWED, WIDENED, REAFFIRMED, WITHDRAWN); and outcomes comparing the later reported SEC XBRL actual with the first and last range (BELOW, WITHIN, ABOVE). Ranges without a stated period are listed but not compared; unread releases are reported, never treated as withdrawn. Evidence only.",
)
async def get_guidance_history(ticker: str, max_releases: int = 8) -> str:
    limit = _clamp_int(max_releases, 8, 1, 12)
    upper = ticker.upper()
    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded:
        ticker_index = await _load_edgar_tickers()
        code = "NO_SEC_REGISTRANT" if ticker_index else "SEC_LOOKUP_UNAVAILABLE"
        message = (f"SEC has no registrant CIK for ticker {upper}." if ticker_index
                   else f"SEC's ticker index could not be read, so {upper} was not resolved to a CIK; retry.")
        return json.dumps({"ticker": upper, "status": code, "code": code, "retryable": not ticker_index, "message": message})
    if not subs:
        return json.dumps({"ticker": upper, "status": "SEC_LOOKUP_UNAVAILABLE", "code": "SEC_LOOKUP_UNAVAILABLE", "retryable": True,
                           "message": "SEC submissions could not be read; retry."})
    cik_int = int(cik_padded)
    picks = _earnings_release_picks(subs, cik_int, limit)
    releases = []
    # Four at a time keeps within SEC's request-rate guidance.
    for i in range(0, len(picks), 4):
        batch = picks[i:i + 4]
        reads = await asyncio.gather(*(_read_release_text(cik_int, p["accessionNumber"], p["primaryUrl"]) for p in batch))
        releases += [{"filingDate": p["filingDate"], "accessionNumber": p["accessionNumber"], **r} for p, r in zip(batch, reads)]
    facts = await _edgar_get_company_facts(cik_padded)
    out = _gh.guidance_history(ticker, releases, facts)
    unread = len([r for r in releases if r["status"] != "READ"])
    warnings = []
    if not picks:
        warnings.append({"code": "NO_EARNINGS_RELEASES_FOUND", "message": "No 8-K reporting results of operations (Item 2.02) is in SEC's recent submissions.", "severity": "warning"})
    if unread:
        warnings.append({"code": "RELEASE_TEXT_NOT_AVAILABLE", "message": f"{unread} of {len(releases)} earnings releases could not be read from SEC; their guidance is missing, not withdrawn. Retry.", "severity": "warning"})
    if not facts:
        warnings.append({"code": "ACTUALS_NOT_READ", "message": "SEC companyfacts could not be read, so outcomes were not evaluated; retry.", "severity": "warning"})
    out["status"] = ("NO_EARNINGS_RELEASES_FOUND" if not picks else "RELEASE_TEXT_NOT_AVAILABLE" if unread == len(releases)
                     else "PARTIAL" if unread or not facts else "OK")
    out["retryable"] = bool(unread) or not facts
    out["warnings"] = warnings
    return json.dumps(out)


# ── Historical valuation context and metric reconciliation (2.5.4) ────────

def _daily_history_sync(ticker: str, from_date: str) -> dict | None:
    """Daily closes (Yahoo split-adjusted), split events and the quote currency from from_date to now."""
    company = yf.Ticker(ticker)
    df = company.history(start=from_date, interval="1d", auto_adjust=False, actions=True)
    if df is None or getattr(df, "empty", True):
        return None
    bars: list[dict] = []
    splits: list[dict] = []
    for idx, row in df.iterrows():
        date = idx.strftime("%Y-%m-%d")
        close = row.get("Close")
        if close is not None and math.isfinite(float(close)):
            bars.append({"date": date, "close": float(close)})
        ratio = row.get("Stock Splits")
        if ratio is not None and math.isfinite(float(ratio)) and float(ratio) > 0:
            splits.append({"date": date, "ratio": float(ratio)})
    meta = getattr(company, "history_metadata", None) or {}
    return {"bars": bars, "splits": splits, "currency": meta.get("currency")}


async def _daily_history(ticker: str, from_date: str) -> dict | None:
    try:
        return await asyncio.to_thread(_daily_history_sync, ticker, from_date)
    except Exception:  # noqa: BLE001 - reported as unread
        return None


# Cover pages sit at the top of a periodic report with the header contexts; the first 900 KB is read, not
# the whole report (ASTS's 10-Q is 3.5 MB, its class counts end near 194 KB).
_COVER_READ_MAX_BYTES = 900_000


_PERIODIC_REPORT_FORMS = {"10-K", "10-Q", "20-F", "40-F"}


def _periodic_reports(subs: dict | None, cik_padded: str | None) -> list[dict]:
    """The filer's periodic reports in SEC's recent submissions, with their primary documents."""
    if not subs or not cik_padded:
        return []
    recent = (subs.get("filings") or {}).get("recent") or {}

    def at(key: str, i: int) -> str:
        values = recent.get(key) or []
        return str(values[i] or "") if i < len(values) and values[i] is not None else ""

    out = []
    for i, form in enumerate(recent.get("form") or []):
        form_upper = str(form or "").upper()
        acc, doc = at("accessionNumber", i), at("primaryDocument", i)
        if form_upper not in _PERIODIC_REPORT_FORMS or not acc or not doc:
            continue
        out.append({"accessionNumber": acc, "form": form_upper, "filed": at("filingDate", i), "reportDate": at("reportDate", i) or None,
                    "documentUrl": sec_archive_document_url(int(cik_padded), acc.replace('-', ''), doc)})
    return out


async def _cover_counts_for(companyfacts: dict, reports: list[dict], dates: list[str]) -> dict:
    """Per-class cover-page share counts from the periodic reports companyfacts cannot give a point-in-time count for."""
    out: dict = {}
    for n in _vh.cover_reads_needed(companyfacts, reports, dates):
        url = n["documentUrl"]
        try:
            html = await _edgar_get_html(url, max_bytes=_COVER_READ_MAX_BYTES)
        except Exception:  # noqa: BLE001 - reported as unread
            html = None
        out[n["accessionNumber"]] = {**_vh.cover_share_counts(html), "documentUrl": url} if html else {"status": "NOT_READ", "value": None, "documentUrl": url}
    return out


async def _valuation_history_for(ticker: str, dates: list[str] | None, from_date: str) -> dict:
    upper = ticker.upper()
    cik_padded = await _resolve_cik_for_ticker(ticker)
    if not cik_padded:
        ticker_index = await _load_edgar_tickers()
        code = "NO_SEC_REGISTRANT" if ticker_index else "SEC_LOOKUP_UNAVAILABLE"
        return {"ticker": upper, "status": code, "code": code, "retryable": not ticker_index, **_AUTHORITY_BOUNDARY}
    facts, history = await asyncio.gather(_edgar_get_company_facts(cik_padded), _daily_history(ticker, from_date), return_exceptions=True)
    facts = None if isinstance(facts, BaseException) else facts
    history = None if isinstance(history, BaseException) else history
    if not facts:
        return {"ticker": upper, "status": "COMPANYFACTS_NOT_AVAILABLE", "code": "COMPANYFACTS_NOT_AVAILABLE", "retryable": True, **_AUTHORITY_BOUNDARY}
    if not history or not history["bars"]:
        return {"ticker": upper, "status": "PRICE_HISTORY_NOT_AVAILABLE", "code": "PRICE_HISTORY_NOT_AVAILABLE", "retryable": True, **_AUTHORITY_BOUNDARY}
    use_dates = dates if dates is not None else _vh.valuation_dates(None, history["bars"][-1]["date"])[0]
    _, currency = _vh.taxonomy_of(facts)
    price_currency = _vl._major_price(1, history["currency"])[1] if history["currency"] else None
    warnings: list[dict] = []
    # One FX series per reporting currency the dates use (2.5.32, F-029: NBIS reported in RUB, then USD).
    fx_by_currency: dict[str, dict] = {}
    if price_currency:
        for c in _vh.reporting_currencies(facts, use_dates):
            if c == price_currency:
                continue
            pair = f"{c}{price_currency}=X"
            fx_history = await _daily_history(pair, from_date)
            fx_by_currency[c] = {"pair": pair, "bars": (fx_history or {}).get("bars") or []}
    fx = fx_by_currency.get(currency) if currency else None
    ratio = None
    if _vh.foreign_filer(facts):
        try:
            market = await _valuation_market_inputs(ticker)
        except Exception:  # noqa: BLE001 - no ratio without Yahoo's count
            market = None
        sec = _vh.latest_share_count(facts)
        quoted = (market or {}).get("sharesOutstanding")
        r = _vl.ads_ratio(sec, quoted) if sec is not None and quoted is not None else None
        if r is not None and r != 1:
            ratio = r
            warnings.append({"code": "ADR_RATIO_APPLIED", "message": (
                f"SEC counts {sec} ordinary shares; Yahoo counts {quoted} quoted shares, {r} ordinary shares each. Share counts are divided by "
                f"{r} at every date."), "severity": "info"})
    try:
        _, subs = await _get_submissions_for_ticker(ticker)
    except Exception:  # noqa: BLE001 - reported below
        subs = None
    reports = _periodic_reports(subs, cik_padded)
    cover_counts = await _cover_counts_for(facts, reports, use_dates)
    periodic_filings = [{k: r[k] for k in ("accessionNumber", "form", "filed", "reportDate")} for r in reports]
    out = _vh.historical_valuation({"ticker": ticker, "dates": use_dates, "companyfacts": facts, "bars": history["bars"],
                                    "priceCurrency": history["currency"], "splits": history["splits"], "fx": fx, "fxByCurrency": fx_by_currency, "adsRatio": ratio,
                                    "coverCounts": cover_counts, "periodicFilings": periodic_filings})
    if not subs:
        warnings.append({"code": "SUBMISSIONS_NOT_AVAILABLE", "message": (
            "SEC submissions could not be read, so no cover page was read; filers without an undimensioned share count have no market cap. Retry."),
            "severity": "warning"})
    out["warnings"] = warnings
    return out


@yfinance_server.tool(
    name="get_historical_valuation_context",
    description="Market cap, enterprise value and EV/Revenue, EV/EBITDA, P/E and P/S as they stood at each date, for a ticker and caller-named peers on the same dates. Point in time: SEC companyfacts filed on or before each date; the close on or before it with later split adjustments undone; balances at the latest balance date then filed; LTM (LFY + YTD - prior YTD) and LFY denominators with their periods, concepts and filings; reporting currency converted at that date's FX close; ADR share counts divided by ordinary shares per ADS. Shares are point in time: the cover-page or balance-sheet count, else the filing's per-class cover counts summed; a weighted-average count never sets market cap (POINT_IN_TIME_SHARES_UNRESOLVED). EBITDA is computed (operating income + D&A). Untagged or unfiled figures leave their multiples null with a status; status is OK only when all eight multiples are usable (coreStatus covers market cap and EV, coverage lists the rest); peer medians are unweighted. No multiple is selected. Evidence only.",
)
async def get_historical_valuation_context(ticker: str, dates: list[str] | None = None, peers: list[str] | None = None) -> str:
    requested = None
    if dates:
        requested, error = _vh.valuation_dates([str(d).strip() for d in dates], None)
        if error:
            return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": error})
    peer_list = list(dict.fromkeys(p for p in (str(x).strip().upper() for x in (peers or [])) if p and p != ticker.upper()))
    if len(peer_list) > 5:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "At most 5 peers."})
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    earliest = requested[0] if requested else f"{int(today[:4]) - 5}{today[4:]}"
    from_date = (datetime.date.fromisoformat(earliest[:4] + ("-02-28" if earliest[4:] == "-02-29" else earliest[4:])) - datetime.timedelta(days=15)).isoformat()
    subject = await _valuation_history_for(ticker, requested, from_date)
    final_dates = requested if requested else [str(p["date"]) for p in subject.get("points") or []]
    peer_results: list[dict] = []
    # Three at a time keeps SEC and Yahoo request rates modest.
    for i in range(0, len(peer_list), 3):
        peer_results += await asyncio.gather(*(_valuation_history_for(p, final_dates or None, from_date) for p in peer_list[i:i + 3]))
    return json.dumps({
        "ticker": ticker.upper(),
        "status": str(subject.get("status") or "NOT_AVAILABLE"),
        "dates": final_dates,
        "subject": subject,
        "peers": peer_results,
        "peerMedians": _vh.peer_medians(final_dates, [p for p in peer_results if isinstance(p.get("points"), list)]) if peer_list and final_dates else [],
        "notes": [
            "Peers are the caller's; their medians are unweighted and count only peers with that multiple on that date. Each peer's denominators are its own fiscal periods, named in its points.",
            "The latest date's close can be intraday while the market is open.",
        ],
        **_AUTHORITY_BOUNDARY,
    })


_RELEASE_READ_MAX = 4


def _yahoo_statement_rows_sync(ticker: str, annual: bool, balance: bool) -> list[dict]:
    """Yahoo statement columns as rows keyed by camelCase line item: {"date": "2026-06-30", "totalRevenue": ...}."""
    company = yf.Ticker(ticker)
    if balance:
        df = company.balance_sheet if annual else company.quarterly_balance_sheet
    else:
        df = company.income_stmt if annual else company.quarterly_income_stmt
    if df is None or getattr(df, "empty", True):
        return []
    rows = []
    for col in df.columns:
        row: dict = {"date": pd.Timestamp(col).strftime("%Y-%m-%d")}
        for name, value in df[col].items():
            if value is None or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                continue
            key = str(name).replace(" ", "")
            row[key[:1].lower() + key[1:]] = float(value)
        rows.append(row)
    return rows


@yfinance_server.tool(
    name="reconcile_metric_sources",
    description="One metric for one period as each source states it: SEC XBRL as first filed and as latest filed (a difference is a restatement), the issuer's earnings release (only sentences naming the metric, an amount and the period's scope) and Yahoo's statement row, each compared with the latest SEC value with difference, percentage and tolerance (the larger of tolerance_pct and half the release's last stated digit). Status AGREED requires latest SEC plus at least one independent matching provider; PARTIAL covers a missing SEC baseline or only one provider; otherwise CONFLICT or NOT_FOUND, with source evidence. Metrics: revenue, net_income, operating_income, eps_diluted, cash_and_equivalents. Evidence only.",
)
async def reconcile_metric_sources(ticker: str, metric: str, period: Annotated[str, Field(description=_mr.RECONCILE_PERIOD_DESCRIPTION)] = "latest", tolerance_pct: float = _mr.DEFAULT_TOLERANCE_PCT) -> str:
    upper = ticker.upper()
    if metric not in _mr.METRICS:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": f"metric must be one of: {', '.join(_mr.METRICS)}"})
    # A period it cannot read is an input error before any fetch, not ok:true with INVALID_PERIOD (2.26.5, F60).
    if not _mr.is_reconcile_period(period):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": _mr.RECONCILE_PERIOD_HELP})
    # Q3e (3.2.1): only a finite number is clamped to 0-10; NaN, an infinity, a numeric string or anything else is the
    # 0.5 default, as in the Worker (min/max before the finiteness check turned NaN into 0 and +inf into 10).
    if isinstance(tolerance_pct, (int, float)) and not isinstance(tolerance_pct, bool) and math.isfinite(tolerance_pct):
        tolerance = min(10.0, max(0.0, float(tolerance_pct)))
    else:
        tolerance = _mr.DEFAULT_TOLERANCE_PCT
    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded:
        ticker_index = await _load_edgar_tickers()
        code = "NO_SEC_REGISTRANT" if ticker_index else "SEC_LOOKUP_UNAVAILABLE"
        return json.dumps({"ticker": upper, "metric": metric, "status": code, "code": code, "retryable": not ticker_index})
    facts = await _edgar_get_company_facts(cik_padded)
    if not facts:
        return json.dumps({"ticker": upper, "metric": metric, "status": "COMPANYFACTS_NOT_AVAILABLE", "code": "COMPANYFACTS_NOT_AVAILABLE", "retryable": True})
    # The latest annual report may be missing from companyfacts (2.5.30, F-015): said on every result, found or not.
    coverage_warning = _cfc.latest_annual_coverage_warning(_cfc.annual_filings_from_submissions(subs), facts)
    resolved = _mr.resolve_period(facts, metric, period)
    if resolved["status"] != "OK":
        return json.dumps({"ticker": upper, "metric": metric, **resolved, **({"warnings": [coverage_warning]} if coverage_warning else {})})
    cik_int = int(cik_padded)
    end = str(resolved["periodEnd"])
    # Results releases (Item 2.02 8-Ks and 8-K/As) filed within 100 days after the period ended. The newest
    # _RELEASE_READ_MAX are read, so a later correction is never crowded out by earlier filings; they are kept
    # oldest first, and the latest with a scoped figure wins (an early Item 2.02 8-K, as ASTS filed on
    # 2026-07-15, is not always the results release).
    in_window = sorted((p for p in (_earnings_release_picks(subs, cik_int, 40, True) if subs else [])
                        if p["filingDate"] > end and (datetime.date.fromisoformat(p["filingDate"]) - datetime.date.fromisoformat(end)).days <= 100),
                       key=lambda p: (p["filingDate"], p["accessionNumber"]))
    candidates = in_window[-_RELEASE_READ_MAX:]
    not_read = in_window[:len(in_window) - len(candidates)]
    annual = resolved["periodType"] == "ANNUAL"
    instant = _mr.METRICS[metric]["kind"] == "instant"

    async def read_releases() -> list[dict]:
        read = []
        for c in candidates:
            try:
                r = {**await _read_release_text(cik_int, c["accessionNumber"], c["primaryUrl"]), "filingDate": c["filingDate"], "accessionNumber": c["accessionNumber"]}
            except Exception:  # noqa: BLE001 - reported as unread
                r = {"status": "NOT_READ", "url": c["primaryUrl"], "filingDate": c["filingDate"], "accessionNumber": c["accessionNumber"], "text": None}
            read.append(r)
        return read

    async def yahoo_rows() -> list[dict] | None:
        try:
            return await asyncio.to_thread(_yahoo_statement_rows_sync, ticker, annual, instant)
        except Exception:  # noqa: BLE001 - reported as unread
            return None

    releases, rows = await asyncio.gather(read_releases(), yahoo_rows())
    out = _mr.metric_reconciliation(ticker=ticker, metric=metric, period=resolved, companyfacts=facts, releases=releases, yahoo_rows=rows,
                                    tolerance_pct=tolerance)
    warnings = []
    unread = [r for r in releases if r["status"] == "NOT_READ"]
    if unread:
        warnings.append({"code": "RELEASE_TEXT_NOT_AVAILABLE", "message": f"{len(unread)} earnings release(s) could not be read from SEC; retry.", "severity": "warning"})
    if rows is None:
        warnings.append({"code": "YAHOO_NOT_AVAILABLE", "message": "Yahoo's statements could not be read; retry.", "severity": "warning"})
    out["releaseCandidates"] = {"inWindow": len(in_window), "read": len(candidates),
                                "notRead": [{"filingDate": p["filingDate"], "accessionNumber": p["accessionNumber"], "form": p["form"]} for p in not_read]}
    if not_read:
        warnings.append({"code": "OLDER_RELEASE_CANDIDATES_NOT_READ", "message": (
            f"{len(not_read)} older Item 2.02 filing(s) in the 100-day window were not read; the {len(candidates)} newest were."), "severity": "info"})
    if coverage_warning:
        warnings.append(coverage_warning)
    out["retryable"] = bool(unread) or rows is None
    out["warnings"] = warnings
    return json.dumps(out)


@yfinance_server.tool(
    name="extract_analyst_valuation_methods",
    description="Valuation methods analysts name in recent news headlines and summaries: multiples with metric and period (e.g. 41x 2H27 EV/EBITDA), DCF with WACC, discount rate and terminal growth, sum-of-the-parts and rNPV, with firm, price target and source link. Lists firms whose targets carry no disclosed method. Context only: not consensus inputs.",
)
async def extract_analyst_valuation_methods(ticker: str, days_back: int = 30) -> str:
    days = _clamp_int(days_back, 30, 1, 365)
    news_raw, radar_raw, issuer = await asyncio.gather(
        get_company_news(ticker, max_results=100, lookback_days=days),
        get_analyst_upgrade_radar(ticker, days),
        _issuer_names(ticker),
        return_exceptions=True,
    )
    # Targets are attributed to the subject by name; without a name nothing can be checked.
    names = issuer if isinstance(issuer, list) else []
    news = _unwrap_payload(news_raw) if isinstance(news_raw, str) else {}
    radar = _unwrap_payload(radar_raw) if isinstance(radar_raw, str) else {}
    items = news.get("items") if isinstance(news.get("items"), list) else []
    changes = radar.get("changes") if isinstance(radar.get("changes"), list) else []
    out = _cs.analyst_valuation_methods(ticker.upper(), [i for i in items if isinstance(i, dict)], [c for c in changes if isinstance(c, dict)],
                                        names or None)
    warnings = []
    if not names:
        warnings.append({"code": "ATTRIBUTION_UNCHECKED", "message": "The company name could not be resolved, so targets were not checked against the subject.", "severity": "warning"})
    if news.get("error") or not isinstance(news_raw, str):
        warnings.append({"code": "NEWS_UNAVAILABLE", "message": str(news.get("message") or "News could not be read."), "severity": "warning"})
    if radar.get("error") or not isinstance(radar_raw, str):
        warnings.append({"code": "RATING_CHANGES_UNAVAILABLE", "message": str(radar.get("message") or "Rating changes could not be read."), "severity": "warning"})
    out["windowDays"] = days
    out["newsSourcesUsed"] = (news.get("meta") or {}).get("sourcesUsed", []) if isinstance(news.get("meta"), dict) else []
    out["warnings"] = warnings
    return json.dumps(out)


_COMPANIES_HOUSE_API = "https://api.company-information.service.gov.uk"
_COMPANIES_HOUSE_CATEGORIES = [
    "accounts", "capital", "mortgage", "confirmation-statement", "resolution", "incorporation", "officers",
    "persons-with-significant-control", "address", "annotation", "change-of-name", "miscellaneous",
]


async def _companies_house_get(path: str, key: str) -> tuple[int, dict | None]:
    import base64

    token = base64.b64encode(f"{key}:".encode()).decode()

    def fetch() -> tuple[int, dict | None]:
        req = _urlrequest.Request(
            f"{_COMPANIES_HOUSE_API}{path}",
            headers={"Authorization": f"Basic {token}", "Accept": "application/json", "User-Agent": _PROVIDER_USER_AGENT},
        )
        try:
            with _url_policy.urlopen(req, timeout=15) as resp:  # noqa: S310
                return resp.status, json.loads(resp.read().decode("utf-8", errors="replace"))
        except _urlerror.HTTPError as exc:
            return exc.code, None

    return await asyncio.get_event_loop().run_in_executor(None, _url_policy.in_context(fetch))


def _companies_house_error(status: int, step: str) -> str:
    if status == 429:
        return json.dumps({"error": True, "message": f"Companies House {step}: HTTP 429 rate limited (600 requests per 5 minutes)."})
    if status in (401, 403):
        return json.dumps({"error": True, "code": "PROVIDER_ERROR", "status": "AUTH_ERROR", "message": f"Companies House {step}: HTTP {status}; check COMPANIES_HOUSE_API_KEY."})
    return json.dumps({"error": True, "code": "PROVIDER_ERROR", "message": f"Companies House {step}: HTTP {status}."})


def _charge_rows(payload: dict) -> list[dict]:
    out = []
    for c in payload.get("items") if isinstance(payload.get("items"), list) else []:
        classification = c.get("classification") if isinstance(c.get("classification"), dict) else {}
        particulars = c.get("particulars") if isinstance(c.get("particulars"), dict) else {}
        persons = c.get("persons_entitled") if isinstance(c.get("persons_entitled"), list) else []
        out.append({
            "chargeCode": c.get("charge_code"),
            "status": c.get("status"),
            "createdOn": c.get("created_on"),
            "deliveredOn": c.get("delivered_on"),
            "satisfiedOn": c.get("satisfied_on"),
            "classification": classification.get("description"),
            "personsEntitled": [str(p.get("name") or "") for p in persons if isinstance(p, dict) and p.get("name")],
            "particulars": particulars["description"][:400] if isinstance(particulars.get("description"), str) else None,
        })
    return out


async def _issuer_names(ticker: str) -> list[str]:
    """Attribution names for a ticker (2.19.0, B-G11 / call #16; the Worker's alias set): Yahoo longName, shortName and the
    company name (longName, else shortName), without any name equal to the ticker."""
    try:
        info = await asyncio.to_thread(lambda: yf.Ticker(ticker).info)
    except Exception:
        return []
    info = info if isinstance(info, dict) else {}
    long_name = info.get("longName")
    short_name = info.get("shortName")
    company_name = long_name or short_name
    return [
        name for name in (long_name, short_name, company_name)
        if isinstance(name, str) and name.strip() and name.strip().lower() != ticker.strip().lower()
    ]


async def _issuer_name(ticker: str) -> str | None:
    try:
        info = await asyncio.to_thread(lambda: yf.Ticker(ticker).info)
    except Exception:
        return None
    info = info if isinstance(info, dict) else {}
    return str(info.get("longName") or info.get("shortName") or "").strip() or None


@yfinance_server.tool(
    name="get_uk_company_filings",
    description="UK Companies House filing history for a UK-registered issuer (e.g. IQE.L): accounts, share allotments (SH01), charges (MR01, secured lending) and resolutions, with document links; include_charges adds the charge register. Resolves by company_number, company_name, or the ticker's issuer name. Official statutory filings; RNS market announcements are not held here. Needs the COMPANIES_HOUSE_API_KEY secret.",
)
async def get_uk_company_filings(
    ticker: str | None = None,
    company_number: str | None = None,
    company_name: str | None = None,
    category: str | None = None,
    limit: int = 25,
    include_charges: bool = False,
) -> str:
    key = (os.environ.get("COMPANIES_HOUSE_API_KEY") or "").strip()
    base = {"source": "companies_house", "sourceType": "uk_companies_house"}
    if not key:
        return json.dumps({
            **base, "ticker": ticker, "status": "SOURCE_UNCONFIGURED", "code": "SOURCE_UNCONFIGURED",
            "message": "Companies House is not configured; set the COMPANIES_HOUSE_API_KEY secret (a free key from the Companies House developer hub).",
            "filings": [],
        })
    cat = category.strip().lower() if category else None
    if cat and cat not in _COMPANIES_HOUSE_CATEGORIES:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": f"category must be one of: {', '.join(_COMPANIES_HOUSE_CATEGORIES)}."})
    size = _clamp_int(limit, 25, 1, 100)
    warnings: list[dict] = []
    number = company_number.strip().upper() if company_number else None
    if number and _re.fullmatch(r"[0-9]{1,8}", number):
        number = number.zfill(8)
    if number and not _re.fullmatch(r"[A-Z0-9]{8}", number):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "company_number must be an 8-character Companies House number, e.g. 01234567 or SC123456."})
    matched_by = "company_number"
    company: dict | None = None
    try:
        if not number:
            name = (company_name or "").strip() or None
            matched_by = "company_name"
            if not name and ticker:
                name = await _issuer_name(ticker)
                matched_by = "ticker_issuer_name"
                if not _re.search(r"\.(?:L|IL)$", ticker, _re.IGNORECASE):
                    warnings.append({"code": "NON_UK_LISTING", "message": f"{ticker} is not a London listing; the match is by issuer name only.", "severity": "info"})
            if not name:
                return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "Provide company_number, company_name, or a ticker whose issuer name can be resolved."})
            status, body = await _companies_house_get(f"/search/companies?q={_urlparse.quote(name, safe='')}&items_per_page=20", key)
            if body is None:
                return _companies_house_error(status, "company search")
            match = _cs.pick_companies_house_match(name, body)
            if match is None:
                items = body.get("items") if isinstance(body.get("items"), list) else []
                return json.dumps({
                    **base, "ticker": ticker, "status": "COMPANY_NOT_MATCHED", "searchedName": name,
                    "candidates": [{"companyNumber": i.get("company_number"), "title": i.get("title"), "companyStatus": i.get("company_status"), "addressSnippet": i.get("address_snippet")} for i in items[:5]],
                    "message": "No registered company name matched the issuer exactly; pass company_number from the candidates.",
                    "filings": [], "warnings": warnings,
                })
            number = str(match.get("company_number") or "")
            company = {"companyNumber": number, "name": match.get("title"), "status": match.get("company_status")}
        if company is None:
            status, body = await _companies_house_get(f"/company/{number}", key)
            if status == 404:
                return json.dumps({**base, "ticker": ticker, "status": "COMPANY_NOT_FOUND", "companyNumber": number, "filings": []})
            if body is None:
                return _companies_house_error(status, "company profile")
            company = {"companyNumber": number, "name": body.get("company_name"), "status": body.get("company_status")}
        status, history = await _companies_house_get(f"/company/{number}/filing-history?items_per_page={size}{f'&category={cat}' if cat else ''}", key)
        if history is None:
            return _companies_house_error(status, "filing history")
        total = history.get("total_count")
        out: dict = {
            **base, "ticker": ticker, "status": "OK", "decisionUse": "USE_OFFICIAL_EVIDENCE",
            "company": {**company, "matchedBy": matched_by},
            "category": cat,
            "filings": _cs.companies_house_filings(str(number), history),
            "totalFilings": total if isinstance(total, (int, float)) and not isinstance(total, bool) else None,
            "filingHistoryUrl": f"https://find-and-update.company-information.service.gov.uk/company/{number}/filing-history",
            "notes": [
                "Companies House holds statutory filings: accounts, share allotments (SH01), charges (MR01), resolutions and confirmation statements.",
                "Market announcements (RNS) such as results, loan-note terms and trading updates are not filed here; read them on the company's investor site.",
                "Documents are PDFs; documentContentUrl needs the same API key, viewerUrl opens the public viewer.",
            ],
            "warnings": warnings,
        }
        if include_charges:
            status, charges = await _companies_house_get(f"/company/{number}/charges", key)
            if charges is not None:
                out["charges"] = _charge_rows(charges)
            elif status == 404:
                out["charges"] = []
            else:
                warnings.append({"code": "CHARGES_UNAVAILABLE", "message": f"Companies House charges: HTTP {status}.", "severity": "warning"})
        return json.dumps(out)
    except Exception as exc:
        message = str(exc)
        if "timed out" in message.lower():
            return json.dumps({"error": True, "message": "Companies House request timed out."})
        return json.dumps({"error": True, "code": "PROVIDER_ERROR", "message": f"Companies House request failed: {message}"})


# ── Valuation snapshot and peer multiples (2.4.0) ─────────────────────────
# Mirrors the Worker; the arithmetic lives in yfmcp.valuation.


_SEC_UNAVAILABLE_STATUSES = {"TICKER_NOT_FOUND", "FILING_NOT_FOUND_TRY_OTHER_TYPE", "FILING_TEXT_NOT_AVAILABLE"}


@yfinance_server.tool(
    name="get_valuation_snapshot",
    description="Valuation context for one ticker at the current price or one you supply: diluted shares from the SEC dilution bridge at that price, the filing's period-end cash and debt, equity and enterprise value (in-the-money convertibles leave the debt), EV/Revenue, EV/EBITDA and P/E on trailing results and Yahoo's current and next fiscal-year consensus with analyst counts, and ATM capacity. Falls back to Yahoo figures for non-USD or non-SEC listings. Mechanical context, not a price target; nothing is back-solved.",
)
async def get_valuation_snapshot(
    ticker: str,
    price: float | None = None,
    filing_type: str = "latest",
    include_sec_filings: bool = True,
) -> str:
    if price is not None:
        try:
            price = float(price)
        except (TypeError, ValueError):
            price = math.nan
        if not math.isfinite(price) or price <= 0:
            return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "price must be a positive number when supplied."})
    try:
        market = await _valuation_market_inputs(ticker)
    except Exception as exc:
        # As compare_peer_valuations (F57): an unknown ticker is TICKER_NOT_FOUND, and no provider URL is shown (3.2.1).
        failure = _peer_valuation_error(ticker.upper(), str(exc))
        message = failure["message"] if failure["code"] == "TICKER_NOT_FOUND" else f"Yahoo quote summary failed for {ticker.upper()}: {failure['message']}"
        return json.dumps({"error": True, "code": failure["code"], "message": message})
    quote = price if price is not None else market["price"]
    sec_warnings: list[dict] = []
    bridge = capital = None
    if quote is not None and include_sec_filings is not False:
        major, major_currency = _vl._major_price(quote, market["currency"])
        if major_currency is None or major_currency == "USD":
            b_raw, c_raw = await asyncio.gather(
                extract_dilution_bridge(ticker=ticker, price=major, currency="USD", filing_type=filing_type, include_atm=True),
                extract_capital_structure(ticker=ticker, filing_type=filing_type, include_funding_statements=False),
            )
            b, c = _unwrap_payload(b_raw), _unwrap_payload(c_raw)
            bridge = b if b.get("basis") == "MECHANICAL_COMPANY_DISCLOSED" else None
            capital = c if c.get("basis") == "COMPANY_DISCLOSED" else None
            status = str(b.get("status") or b.get("code") or c.get("status") or "")
            if bridge is None or capital is None:
                sec_warnings.append({
                    "code": "SEC_FILINGS_UNAVAILABLE",
                    "message": f"SEC filings were not read ({status}); Yahoo figures are used." if status in _SEC_UNAVAILABLE_STATUSES else "SEC filings could not be read in full; Yahoo figures fill the gaps.",
                    "severity": "info",
                })
        else:
            sec_warnings.append({"code": "NON_USD_LISTING", "message": f"SEC filings are read only for USD listings; {ticker.upper()} is quoted in {market['currency']}, so Yahoo figures are used.", "severity": "info"})
    return json.dumps(_vl.valuation_snapshot(ticker.upper(), market, price, bridge, capital, sec_warnings))


_PEER_VALUATION_MAX_TICKERS = 10


def _peer_valuation_error(ticker: str, message: str) -> dict:
    """The Worker's peerValuationError (2.26.5, F57): an unknown ticker is TICKER_NOT_FOUND, anything else
    PROVIDER_ERROR, with no provider URL in the message."""
    if _re.search(r"api error 404|no yahoo quote summary|not found", message, _re.I):
        return {"ticker": ticker, "code": "TICKER_NOT_FOUND", "message": "No provider data was found for the requested ticker."}
    from yfmcp.tools.pricing import _snapshot_component_message
    return {"ticker": ticker, "code": "PROVIDER_ERROR", "message": _snapshot_component_message(message)}


@yfinance_server.tool(
    name="compare_peer_valuations",
    description="The same multiples for a peer set on one Yahoo basis: price x shares outstanding + total debt - total cash, over trailing results and current and next fiscal-year consensus (EV/Revenue, EV/EBITDA, P/E, revenue growth, gross margin). Returns peer medians, min and max excluding the subject, and the subject's premium or discount to each median. Context, not a signal.",
)
async def compare_peer_valuations(tickers: list[str], subject: str | None = None) -> str:
    names: list[str] = []
    for t in [subject, *(tickers or [])]:
        u = t.strip().upper() if isinstance(t, str) else ""
        if u and u not in names:
            names.append(u)
    if len(names) < 2 or len(names) > _PEER_VALUATION_MAX_TICKERS:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": f"tickers must name 2 to {_PEER_VALUATION_MAX_TICKERS} distinct symbols, including the subject."})
    results = await asyncio.gather(*(_valuation_market_inputs(t) for t in names), return_exceptions=True)
    markets = [r for r in results if isinstance(r, dict)]
    errors = [_peer_valuation_error(t, str(r)) for t, r in zip(names, results) if not isinstance(r, dict)]
    if not markets:
        return json.dumps({"error": True, "message": "Yahoo quote summary failed for every ticker: " + "; ".join(e["message"] for e in errors)})
    return json.dumps(_vl.peer_valuations(subject, markets, errors))


def _china_filing_not_found(ticker: str, filing_type: str, source: dict) -> str:
    """The Worker's extractChinaExposure payload for a filing that is not there: no read past the lookup."""
    return json.dumps({
        "ticker": ticker,
        "exposureType": "china_exposure",
        "filingType": filing_type,
        "filingDate": None,
        "accessionNumber": None,
        "documentUrl": None,
        "revenueExposure": {"status": "FILING_NOT_FOUND_TRY_OTHER_TYPE", "value": None, "denominator": None, "valueRatio": None, "valuePct": None, "confidence": "FILING_NOT_FOUND_TRY_OTHER_TYPE", "evidence": []},
        "manufacturingExposure": {"status": "NOT_FOUND", "confidence": "LOW", "evidence": []},
        "entityExposure": {"status": "NOT_FOUND", "entities": [], "confidence": "LOW", "evidence": []},
        "bankExposure": {"status": "NOT_FOUND", "entities": [], "confidence": "LOW", "evidence": []},
        "riskFactorExposure": {"status": "NOT_FOUND", "confidence": "LOW", "evidence": []},
        "overallStatus": "FILING_NOT_FOUND_TRY_OTHER_TYPE",
        "code": "FILING_NOT_FOUND_TRY_OTHER_TYPE",
        "requestedFilingType": _nn(source.get("requestedFilingType"), filing_type),
        "availableFilingTypes": _nn(source.get("availableFilingTypes"), []),
        "suggestedFilingTypes": _nn(source.get("suggestedFilingTypes"), []),
        "warnings": _nn(source.get("warnings"), []),
    })


@yfinance_server.tool(name="extract_china_exposure", description="Extract China exposure with separate revenue and non-revenue classifications; revenue values are decision-grade only when evidence and status support them.")
async def extract_china_exposure(
    ticker: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    accession_number: str | None = None,
    detailLevel: str = "compact",
) -> str:
    failure = _filing_period_failure("extract_china_exposure", period)
    if failure:
        return failure
    risk_terms = list(_CHINA_PRESET["riskTerms"])
    # Warm the shared submissions cache once, then fetch the three
    # independent filing reads together.
    await _warm_sec_submissions(ticker)
    # The filing the Worker reads: a 10-K not filed falls to the latest 20-F, a pinned accession is exact. One with no
    # such filing is FILING_NOT_FOUND_TRY_OTHER_TYPE before any other read (the Worker's resolveSecFiling).
    resolved_filing: dict | None = None
    resolve_error: dict | None = None
    try:
        resolved_filing, resolve_error = await _resolve_sec_filing(ticker, filing_type, accession_number)
    except Exception:
        pass
    if _is_accession_not_found(resolve_error):
        # Not an action 2.13.0 fails closed on a miss: it reads FILING_NOT_FOUND_TRY_OTHER_TYPE, as before.
        resolve_error = await _legacy_accession_miss_payload(ticker, filing_type)
    if resolve_error and resolve_error.get("status") == "FILING_NOT_FOUND_TRY_OTHER_TYPE":
        return _china_filing_not_found(ticker, filing_type, resolve_error)

    async def _no_index() -> str:
        # The Worker reads the index from the resolved filing; one that could not be resolved (no primary document, an
        # XBRL-only one, no submissions) is its error payload, read from as the index (the Worker's {ok: false, ...error}).
        return json.dumps(resolve_error)

    idx_raw, revenue_raw, risk_raw = await asyncio.gather(
        _sec_filing_index_full(
            ticker=ticker,
            filing_type=resolved_filing["filingType"],
            period=period,
            accession_number=resolved_filing["accessionNumber"],
        ) if resolved_filing else (_no_index() if resolve_error else _sec_filing_index_full(
            ticker=ticker, filing_type=filing_type, period=period, accession_number=accession_number)),
        _extract_revenue_exposure(ticker=ticker, exposure_query=_CHINA_PRESET["region"], filing_type=filing_type, period=period, accession_number=accession_number),
        _extract_risk_factor_mentions(ticker=ticker, terms=risk_terms, filing_type=filing_type, period=period, accession_number=accession_number),
    )
    idx = _safe_json_loads(idx_raw)
    revenue = _safe_json_loads(revenue_raw)
    if revenue.get("status") == "FILING_NOT_FOUND_TRY_OTHER_TYPE":
        return _china_filing_not_found(ticker, filing_type, revenue)
    revenue_status_text = str(_nn(revenue.get("status"), "NOT_FOUND"))
    revenue_warnings = revenue["warnings"] if isinstance(revenue.get("warnings"), list) else []
    explicit_revenue_status = _explicit_revenue_limitation_status(revenue_status_text, revenue_warnings)
    revenue_status = "FOUND" if revenue_status_text == "FOUND_REVENUE_EXPOSURE" else revenue_status_text
    # A revenue read that failed (SEC_READ_FAILED, SEC_LOOKUP_UNAVAILABLE) is not a missing exposure (2.5.18).
    revenue_read_failed = revenue_status_text == "PROVIDER_ERROR"

    index = idx.get("index") if isinstance(idx.get("index"), dict) else {}
    sections = index.get("sections") if isinstance(index.get("sections"), list) else []
    tables = index.get("tables") if isinstance(index.get("tables"), list) else []

    def _collect(term_list: list[str], evidence_kind: str) -> tuple[list[dict], int]:
        found = []
        rejected = 0

        def _scoped_excerpt(value: object, term: str) -> str:
            if evidence_kind == "entity":
                return _contextual_entity_excerpt(value, term)
            excerpt = _readable_exposure_excerpt(value, [term], min_words=3)
            if evidence_kind == "bank" and excerpt and not _EXPOSURE_RELATIONSHIP_RE.search(excerpt):
                return ""
            return excerpt

        for sec in sections:
            if not isinstance(sec, dict):
                continue
            heading = str(sec.get("heading") or "")
            low = heading.lower()
            for term in term_list:
                if term.lower() in low:
                    excerpt = _scoped_excerpt(heading, term)
                    if not excerpt:
                        rejected += 1
                        continue
                    found.append({
                        "source": "section",
                        "term": term,
                        "sectionHeading": heading,
                        "excerpt": excerpt,
                        "excerptAvailable": bool(excerpt),
                    })
        for tbl in tables:
            if not isinstance(tbl, dict):
                continue
            hay_source = " ".join([str(tbl.get("title") or ""), *[str(x) for x in (tbl.get("rowLabels") or [])]])
            hay = hay_source.lower()
            for term in term_list:
                if term.lower() in hay:
                    excerpt = _scoped_excerpt(hay_source, term)
                    if not excerpt:
                        rejected += 1
                        continue
                    found.append({
                        "source": "table",
                        "term": term,
                        "tableTitle": tbl.get("title"),
                        "sourceTableId": tbl.get("tableId"),
                        "sectionId": tbl.get("sectionId"),
                        "excerpt": excerpt,
                        "excerptAvailable": bool(excerpt),
                    })
        return found, rejected

    entity_terms = list(_CHINA_PRESET["entityTerms"])
    bank_terms = list(_CHINA_PRESET["bankTerms"])
    # "manufactured by outsourcing partners located primarily in China mainland" (AAPL) names manufacturing without the
    # word "manufacturing" (2.25.4 audit, F39). New phrases go after the old ones, so an old term still reports first.
    manuf_terms = list(_CHINA_PRESET["manufacturingTerms"])

    entity_evidence, entity_rejected = _collect(entity_terms, "entity")
    bank_evidence, bank_rejected = _collect(bank_terms, "bank")
    manu_evidence, manu_rejected = _collect(manuf_terms, "manufacturing")
    risk_mentions = _safe_json_loads(risk_raw)
    raw_risk_evidence = risk_mentions.get("matches") if isinstance(risk_mentions.get("matches"), list) else []
    risk_evidence = []
    for ev in raw_risk_evidence:
        if isinstance(ev, dict):
            term = str(ev.get("term") or _CHINA_PRESET["region"])
            excerpt = _readable_exposure_excerpt(
                ev.get("excerpt") or ev.get("context") or "",
                [term],
            )
            if excerpt:
                shaped = dict(ev)
                shaped["excerpt"] = excerpt
                shaped["excerptAvailable"] = True
                risk_evidence.append(shaped)

    if not manu_evidence:
        # Headings and table labels rarely name manufacturing locations; the filing text does (as extract_exposure's
        # operational scan reads it).
        china_text = _safe_json_loads(await search_sec_filing_text(
            ticker=ticker, search_terms=[_CHINA_PRESET["region"]], filing_type=filing_type, accession_number=accession_number,
            context_chars=800, return_tables=False,
        ))
        for m in china_text["matches"] if isinstance(china_text.get("matches"), list) else []:
            if not isinstance(m, dict):
                continue
            excerpt = _readable_exposure_excerpt(_nn(m.get("contextText"), ""), [_CHINA_PRESET["region"]], max_len=240, min_words=3)
            term = next((t for t in manuf_terms if t in excerpt.lower()), None) if excerpt else None
            if not excerpt or not term:
                continue
            manu_evidence.append({"source": "text", "term": term, "sectionHeading": m.get("sectionHeading"), "excerpt": excerpt, "excerptAvailable": True})
            if len(manu_evidence) >= 5:
                break

    non_revenue_found = bool(entity_evidence or bank_evidence or manu_evidence or risk_evidence)
    revenue_matches = revenue["matches"] if isinstance(revenue.get("matches"), list) else []
    first_revenue = revenue_matches[0] if revenue_matches and isinstance(revenue_matches[0], dict) else {}
    if revenue_status == "FOUND":
        overall = "FOUND_REVENUE_EXPOSURE"
    elif non_revenue_found:
        overall = "FOUND_NON_REVENUE_EXPOSURE"
    elif explicit_revenue_status:
        overall = explicit_revenue_status
    elif revenue_read_failed:
        overall = "PROVIDER_ERROR"
    elif revenue_status_text == "NOT_DISCLOSED":
        overall = "NOT_DISCLOSED"
    elif revenue_status_text == "CONFLICTING":
        overall = "CONFLICTING"
    else:
        overall = "NOT_FOUND"

    out = {
        "ticker": ticker,
        "exposureType": "china_exposure",
        "filingType": _nn(idx.get("filingType"), filing_type),
        "filingDate": idx.get("filingDate"),
        "accessionNumber": idx.get("accessionNumber"),
        "documentUrl": idx.get("documentUrl"),
        "revenueExposure": {
            "status": revenue_status,
            "value": first_revenue.get("value"),
            "denominator": first_revenue.get("denominator"),
            "valueRatio": first_revenue.get("valueRatio"),
            "valuePct": first_revenue.get("valuePct"),
            "confidence": "HIGH" if revenue_status == "FOUND" else (explicit_revenue_status or ("NOT_DISCLOSED" if revenue_status_text == "NOT_DISCLOSED" else "LOW")),
            "evidence": _nn(first_revenue.get("evidence"), []),
        },
        "manufacturingExposure": {"status": "FOUND" if manu_evidence else "NOT_FOUND", "confidence": "MEDIUM", "evidence": manu_evidence, "rejectedNoiseCount": manu_rejected},
        "entityExposure": {"status": "FOUND" if entity_evidence else "NOT_FOUND", "entities": entity_terms if entity_evidence else [], "confidence": "MEDIUM", "evidence": entity_evidence, "rejectedNoiseCount": entity_rejected},
        "bankExposure": {"status": "FOUND" if bank_evidence else "NOT_FOUND", "entities": bank_terms if bank_evidence else [], "confidence": "MEDIUM", "evidence": bank_evidence, "rejectedNoiseCount": bank_rejected},
        "riskFactorExposure": {"status": "FOUND" if risk_evidence else "NOT_FOUND", "confidence": "MEDIUM", "evidence": risk_evidence},
        "overallStatus": overall,
        "code": explicit_revenue_status or ((revenue["code"] if isinstance(revenue.get("code"), str) else "PROVIDER_ERROR") if revenue_read_failed else None),
        "warnings": revenue_warnings,
    }
    if str(detailLevel).lower() == "raw":
        out["rawContext"] = {"filingIndex": idx}
    return json.dumps(out)


@yfinance_server.tool(
    name="extract_exposure",
    description="Extract multi-dimensional SEC exposure for a geographic region or named entity/topic. Returns revenue, operational, named-entity, and risk evidence with explicit non-decision-grade statuses when parser/provider limits prevent a value.",
)
async def extract_exposure(
    ticker: str,
    topic: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    include_risk_factors: bool = True,
) -> str:
    failure = _filing_period_failure("extract_exposure", period)
    if failure:
        return failure
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("extract_exposure", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if not topic or not topic.strip():
        return json.dumps({"ticker": ticker, "topic": topic, "overallStatus": "NOT_FOUND", "warnings": [{"code": "INPUT_VALIDATION_ERROR", "message": "topic is required"}]})

    topic_lower = topic.strip().lower()

    EXPOSURE_SYNONYMS = {
        "china": ["china", "greaterchinese", "greaterchina", "prc", "hongkong", "taiwan"],
        "greater china": ["greaterchina", "greaterchinese", "china", "hongkong", "taiwan"],
        "europe": ["europe", "emea", "europeanunion"],
        "japan": ["japan"],
        "americas": ["americas", "northamerica", "unitedstates", "us"],
        "russia": ["russia", "russianfederation"],
        "india": ["india"],
        "rest of asia": ["restofasia", "asiapacific", "asiaxjapan"],
    }
    CHINA_NAMED_ENTITIES = list(_CHINA_PRESET["namedEntities"])
    # The F39 manufacturing phrases apply to every region, not only the China preset (2.26.5, F62).
    OPERATIONAL_TERMS = ["manufacturing", "assembly", "supply chain", "factory", "production facility", "manufactured", "outsourcing partner", "contract manufacturer"]

    is_china = topic_lower in ("china", "greater china")
    synonym_entry = EXPOSURE_SYNONYMS.get(topic_lower)
    region_label = "Greater China" if topic_lower == "china" else topic

    warnings: list[dict] = []

    async def _skipped() -> None:
        return None

    # The filing reads below are independent. Warm the shared submissions
    # cache once, then run them together; each keeps its own error handling.
    await _warm_sec_submissions(ticker)
    idx_raw, geo_raw, ops_raw, ent_raw, risk_raw = await asyncio.gather(
        _sec_filing_index_full(ticker=ticker, filing_type=filing_type, period=period),
        extract_geographic_revenue(ticker=ticker, region=region_label, filing_type=filing_type, period=period, detailLevel="compact"),
        search_sec_filing_text(ticker=ticker, search_terms=[topic_lower], filing_type=filing_type, context_chars=600, return_tables=False),
        search_sec_filing_text(ticker=ticker, search_terms=CHINA_NAMED_ENTITIES[:3], filing_type=filing_type, context_chars=400, return_tables=False)
        if is_china else _skipped(),
        extract_risk_factor_mentions(ticker=ticker, terms=[topic_lower], filing_type=filing_type, period=period, detailLevel="compact")
        if include_risk_factors else _skipped(),
        return_exceptions=True,
    )
    for result in (idx_raw, geo_raw, ops_raw, ent_raw, risk_raw):
        if isinstance(result, BaseException) and not isinstance(result, Exception):
            raise result
    if isinstance(idx_raw, Exception):
        raise idx_raw

    # Filing index for metadata
    idx = _safe_json_loads(idx_raw)

    filing_date = idx.get("filingDate")
    accession_number = idx.get("accessionNumber")
    document_url = idx.get("documentUrl")

    # Revenue extraction
    if isinstance(geo_raw, Exception):
        warnings.append({"code": "REVENUE_EXTRACTION_ERROR", "message": str(geo_raw), "severity": "warning"})
        geo = {}
    else:
        geo = _safe_json_loads(geo_raw)

    geo_value = geo.get("value")
    geo_denominator = geo.get("denominator")
    geo_conf = str(geo.get("confidence") or "LOW").upper()
    geo_method = str(geo.get("extractionMethod") or "NONE").upper()
    geo_evidence = geo.get("evidence") or {}
    if isinstance(geo_evidence, dict):
        pass
    else:
        geo_evidence = {}

    # A named limitation (EXTRACTION_FAILED, TABLE_NOT_PARSED, ...) is the revenue status, as in the Worker. A failed
    # table parse needs a table to have failed on: when the region is only in the filing's text and no indexed table
    # names it, the status is NOT_FOUND (2.25.4 audit, F35: AAPL India).
    geo_warnings = [w for w in geo.get("warnings") or [] if isinstance(w, dict)]
    geo_warning_codes = {w.get("code") for w in geo_warnings}
    explicit_revenue_status = _explicit_revenue_limitation_status(geo.get("status") or geo.get("code"), geo_warnings)
    if (explicit_revenue_status in ("EXTRACTION_FAILED", "TABLE_NOT_PARSED") and "TABLE_NOT_PARSED" in geo_warning_codes
            and "FILING_READ_TRUNCATED" not in geo_warning_codes and _index_tables_name_region(idx, topic_lower) is False):
        explicit_revenue_status = None
        # The parsed tables simply have no row for the region, so no table failed to parse (2.26.5, F62).
        geo_warnings = [w for w in geo_warnings if w.get("code") != "TABLE_NOT_PARSED"]

    if geo_value is not None:
        rev_status = "FOUND"
    elif explicit_revenue_status:
        rev_status = explicit_revenue_status
    elif geo_conf == "NOT_DISCLOSED":
        rev_status = "NOT_DISCLOSED"
    else:
        rev_status = "NOT_FOUND"

    revenue_exposure = {
        "status": rev_status,
        "value": geo_value,
        "denominator": geo_denominator,
        "valuePct": geo.get("valuePct") if geo_denominator is not None else None,
        "valueRatio": geo.get("valueRatio") if geo_denominator is not None else None,
        "unit": geo.get("unit", "USD"),
        "region": geo_evidence.get("sectionHeading") or region_label,
        "period": geo.get("period"),
        "extractionMethod": geo_method,
        "confidence": geo_conf if geo_value is not None else (explicit_revenue_status or "LOW"),
        "code": explicit_revenue_status,
        "evidence": {
            "sectionHeading": geo_evidence.get("sectionHeading"),
            "sourceRows": geo_evidence.get("sourceRows") if isinstance(geo_evidence.get("sourceRows"), list) else [],
            "sourceColumns": geo_evidence.get("sourceColumns") if isinstance(geo_evidence.get("sourceColumns"), list) else [],
        },
    }

    # Operational scan via existing search
    ops_matches = _safe_json_loads(ops_raw).get("matches") or []

    ops_evidence: list[dict] = []
    found_op_terms: set[str] = set()
    rejected_ops_noise = 0
    for m in ops_matches:
        if not isinstance(m, dict):
            continue
        raw_context = m.get("contextText") or m.get("context") or ""
        context_excerpt = _readable_exposure_excerpt(raw_context, [topic_lower], max_len=200)
        if not context_excerpt:
            rejected_ops_noise += 1
            continue
        context_text = context_excerpt.lower()
        for op_term in OPERATIONAL_TERMS:
            if op_term in context_text:
                found_op_terms.add(op_term)
                if len(ops_evidence) < 5:
                    ops_evidence.append({
                        "term": op_term,
                        "excerpt": context_excerpt,
                        "section": str(m.get("sectionHeading") or ""),
                    })
                break
        if len(ops_evidence) >= 5:
            break

    operational_exposure = {
        "status": "FOUND" if ops_evidence else "NOT_FOUND",
        "terms": list(found_op_terms),
        "evidence": ops_evidence,
        "rejectedNoiseCount": rejected_ops_noise,
    }

    # Entity scan (China only)
    if is_china:
        ent_matches = _safe_json_loads(ent_raw).get("matches") or []
        found_entities: set[str] = set()
        ent_evidence: list[dict] = []
        rejected_entity_noise = 0
        for m in ent_matches:
            if not isinstance(m, dict):
                continue
            term_low = str(m.get("term") or "").lower()
            excerpt = _contextual_entity_excerpt(
                m.get("contextText") or m.get("context") or "",
                term_low,
                max_len=200,
            )
            if not term_low or not excerpt:
                rejected_entity_noise += 1
                continue
            if term_low and term_low not in found_entities:
                found_entities.add(term_low)
                if len(ent_evidence) < 5:
                    ent_evidence.append({
                        "entity": str(m.get("term") or ""),
                        "excerpt": excerpt,
                        "section": str(m.get("sectionHeading") or ""),
                    })
            if len(ent_evidence) >= 5:
                break
        entity_exposure = {
            "status": "FOUND" if ent_evidence else "NOT_FOUND",
            "entities": list(found_entities),
            "evidence": ent_evidence,
            "rejectedNoiseCount": rejected_entity_noise,
        }
    else:
        entity_exposure = {"status": "NOT_FOUND", "entities": [], "evidence": [], "rejectedNoiseCount": 0}

    # Risk factor scan
    if include_risk_factors:
        risk_matches = _safe_json_loads(risk_raw).get("matches") or []
        risk_evidence = []
        for m in risk_matches[:5]:
            if not isinstance(m, dict):
                continue
            excerpt = _readable_exposure_excerpt(
                m.get("excerpt") or m.get("context") or m.get("contextText") or "",
                [topic_lower],
                max_len=200,
            )
            if excerpt:
                risk_evidence.append({
                    "excerpt": excerpt,
                    "section": str(m.get("sectionHeading") or "Risk Factors"),
                })
        risk_factor_exposure = {
            "status": "FOUND" if risk_evidence else "NOT_FOUND",
            "mentionCount": len(risk_matches),
            "evidence": risk_evidence,
        }
    else:
        risk_factor_exposure = {"status": "NOT_FOUND", "mentionCount": 0, "evidence": []}

    # Overall status
    non_revenue_found = bool(ops_evidence or entity_exposure.get("evidence") or risk_factor_exposure.get("evidence"))
    if rev_status == "FOUND":
        overall_status = "FOUND_REVENUE_EXPOSURE"
    elif non_revenue_found:
        overall_status = "FOUND_NON_REVENUE_EXPOSURE"
    elif explicit_revenue_status:
        overall_status = explicit_revenue_status
    elif rev_status == "NOT_DISCLOSED":
        overall_status = "NOT_DISCLOSED"
    else:
        overall_status = "NOT_FOUND"

    return json.dumps({
        "ticker": ticker,
        "topic": topic_lower,
        "filingType": idx.get("filingType", filing_type),
        "filingDate": filing_date,
        "accessionNumber": accession_number,
        "documentUrl": document_url,
        "revenueExposure": revenue_exposure,
        "operationalExposure": operational_exposure,
        "entityExposure": entity_exposure,
        "riskFactorExposure": risk_factor_exposure,
        "overallStatus": overall_status,
        "code": explicit_revenue_status,
        # The revenue extraction's warnings, after this tool's own, as in the Worker.
        "warnings": [*warnings, *geo_warnings],
    })


def _index_tables_name_region(index_payload: dict, region: str) -> bool | None:
    """The Worker's indexTablesNameRegion: whether any indexed table names the region; None with no tables to read."""
    index = index_payload.get("index") if isinstance(index_payload.get("index"), dict) else None
    tables = index.get("tables") if index and isinstance(index.get("tables"), list) else None
    if not tables:
        return None
    def words(v):
        return " ".join(str(x) for x in v) if isinstance(v, list) else ""
    return any(isinstance(t, dict) and _text_contains_geo_region(
        f"{t.get('title') or ''} {words(t.get('headers'))} {words(t.get('rowLabels'))}", region) for t in tables)


@yfinance_server.tool(
    name="query_sec_filing_index",
    description="Deterministically route supported SEC filing index query types to extractor tools.",
)
async def query_sec_filing_index(
    ticker: str,
    # Required, as on the Worker: an omitted query type must not silently
    # become geographic_revenue_share.
    query_type: Literal[
        "geographic_revenue_share",
        "revenue_exposure",
        "china_exposure",
        "risk_factor_mentions",
        "customer_concentration",
        "total_revenue",
        "segment_revenue",
    ],
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    accession_number: str | None = None,
    params: dict | None = None,
    return_evidence: bool = True,
    detailLevel: str = "compact",
) -> str:
    failure = _filing_period_failure("query_sec_filing_index", period)
    if failure:
        return failure
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
        return _mcp_failure(
            "query_sec_filing_index",
            "UNSUPPORTED_QUERY_TYPE",
            f"Unsupported query type '{query}'. Supported types are: {', '.join(sorted(supported))}",
            meta_extra={
                "supportedQueryTypes": list(supported),
                "error_extra": {
                    "supportedQueryTypes": list(supported)
                }
            }
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
        # An explicit accession that is not among the company's recent filings fails closed here too (2.15.0, R4-ACC-QUERY).
        if geo.get("ok") is False and _is_accession_not_found(geo.get("error")):
            return _accession_not_found_envelope(geo["error"])
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
        confidence = str(_nn(geo.get("confidence"), "LOW" if status == "ANSWERED" else "NOT_DISCLOSED"))
        if status == "NOT_FOUND":
            confidence = "NOT_DISCLOSED" if str(geo.get("confidence") or "").upper() == "NOT_DISCLOSED" else "LOW"
        return _result(status, answer, confidence, evidence, warnings)

    if query == "revenue_exposure":
        exposure_query = str(routed_params.get("exposure_query") or "").strip()
        if not exposure_query:
            return _missing_param("exposure_query")
        rex = _safe_json_loads(await _extract_revenue_exposure(
            ticker=ticker,
            exposure_query=exposure_query,
            filing_type=filing_type,
            period=period,
            detailLevel=detail,
            accession_number=accession_number,
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
        confidence = str(_nn(first.get("confidence"), "LOW" if status == "ANSWERED" else "NOT_DISCLOSED"))
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

    # An explicit accession that is not among the company's filings fails closed for the pinned reads too, as
    # geographic_revenue_share does; checked after each query's own params (2.24.0, F21).
    async def _accession_miss() -> str | None:
        if accession_number is None or not str(accession_number).strip():
            return None
        _resolved, resolve_error = await _resolve_sec_filing(ticker, filing_type, str(accession_number).strip())
        return _accession_not_found_envelope(resolve_error) if _resolved is None and _is_accession_not_found(resolve_error) else None

    if query == "risk_factor_mentions":
        terms = routed_params.get("terms")
        terms_list = [str(t) for t in terms] if isinstance(terms, list) else []
        if not terms_list:
            return _missing_param("terms")
        risk_miss = await _accession_miss()
        if risk_miss:
            return risk_miss
        risk = _safe_json_loads(await _extract_risk_factor_mentions(
            ticker=ticker,
            terms=terms_list,
            filing_type=filing_type,
            period=period,
            detailLevel=detail,
            accession_number=accession_number,
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
        cust_miss = await _accession_miss()
        if cust_miss:
            return cust_miss
        cust = _safe_json_loads(await _extract_customer_concentration(
            ticker=ticker,
            filing_type=filing_type,
            period=period,
            detailLevel=detail,
            accession_number=accession_number,
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
        total_miss = await _accession_miss()
        if total_miss:
            return total_miss
        total = _safe_json_loads(await _extract_total_revenue(
            ticker=ticker,
            filing_type=filing_type,
            period=period,
            accession_number=accession_number,
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
            str(_nn(total.get("confidence"), "LOW" if status == "ANSWERED" else "NOT_DISCLOSED")),
            evidence,
            warnings,
        )

    segment_name = str(routed_params.get("segment") or "").strip()
    seg_miss = await _accession_miss()
    if seg_miss:
        return seg_miss
    seg = _safe_json_loads(await _extract_segment_revenue(
        ticker=ticker,
        filing_type=filing_type,
        period=period,
        detailLevel=detail,
        accession_number=accession_number,
    ))
    segments = [s for s in (seg.get("segments") if isinstance(seg.get("segments"), list) else []) if isinstance(s, dict)]
    if segment_name:
        segments = [s for s in segments if str(s.get("label") or "").lower() == segment_name.lower()]
    evidence = [_shape_evidence(s.get("evidence")) for s in segments if isinstance(s.get("evidence"), dict)]
    status = "ANSWERED" if segments else ("NOT_FOUND" if segment_name else str(seg.get("status") or "NOT_FOUND"))
    if status == "ANSWERED" and not evidence:
        status = "NOT_FOUND"
        warnings.append({"code": "EVIDENCE_REQUIRED", "message": "ANSWERED responses require evidence."})
    # The table fallback's rows carry their own confidence (MEDIUM, or LOW when the scale was assumed), as the Worker reads it.
    segment_confidence = str(segments[0].get("confidence") or "LOW") if segments else None
    confidence = (segment_confidence or "LOW") if status == "ANSWERED" else ("NOT_DISCLOSED" if status == "NOT_DISCLOSED" else "LOW")
    # The extractor's warnings (POSSIBLE_20F_FILER, ...) go with the answer, as in the Worker (2.26.4, F23).
    if isinstance(seg.get("warnings"), list):
        warnings.extend(w for w in seg["warnings"] if isinstance(w, dict))
    return _result(
        status,
        {"segment": segment_name or None, "segments": segments},
        confidence,
        evidence,
        warnings,
    )


# ---------------------------------------------------------------------------
# Earnings/transcript domain tools - imported for registration and re-exported
# ---------------------------------------------------------------------------
import yfmcp.tools.earnings  # noqa: F401 (side-effect import)
from yfmcp.tools.earnings import (  # re-export for compatibility and grouped routing
    _is_paywalled_url,
    _classify_earnings_source_url,
    _fetch_public_html,
    _scale_number_from_text,
    _first_sentence_for_topic,
    _resolve_latest_earnings_sec_source,
    _resolve_latest_earnings_release,
    get_latest_earnings_release,
    index_earnings_release,
    extract_earnings_metrics,
    extract_guidance,
    extract_management_commentary,
    compare_earnings_actual_vs_estimate,
    list_sec_filing_exhibits,
    get_sec_filing_exhibit_content,
    parse_public_transcript,
    _transcript_attempt,
    _next_transcript_fallback,
    _alpha_vantage_quarter,
    _fetch_alpha_vantage_transcript,
    get_earnings_call_transcript,
)


# ---------------------------------------------------------------------------
# Evidence tools (2.5.0): consensus curve, EPS revisions, evidence quality,
# valuation evidence pack and content-addressed evidence cuts. Shared logic
# lives in yfmcp/evidence.py (parity with worker/src/evidence.ts); storage in
# yfmcp/evidence_store.py (YFMCP_EVIDENCE_DIR, else UNAVAILABLE).
# ---------------------------------------------------------------------------
from yfmcp import evidence as _ev  # noqa: E402
from yfmcp import companyfacts_coverage as _cfc  # noqa: E402
from yfmcp import evidence_store as _es  # noqa: E402
from yfmcp.build_info import BUILD_SHA as _BUILD_SHA  # noqa: E402
from yfmcp.clients.market_providers import fetch_alpha_vantage_json as _fetch_alpha_vantage_json  # noqa: E402

# Estimates move slowly and the server's key allows 25 requests a day: one request per ticker per day (2.5.29).
_ALPHA_VANTAGE_ESTIMATES_TTL_SECONDS = 24 * 60 * 60


def _yahoo_consensus_snapshot(ticker: str) -> tuple[dict, dict]:
    """yfinance estimate tables as the shared Yahoo input, plus the quote. Blocking."""
    retrieved_at = _now_iso()
    try:
        company = yf.Ticker(ticker)
        info = company.info or {}
        ee = _df_period_rows(company.earnings_estimate)
        re_ = _df_period_rows(company.revenue_estimate)
        et = _df_period_rows(company.eps_trend)
        er = _df_period_rows(company.eps_revisions)
    except Exception as exc:  # noqa: BLE001 - each provider fails on its own
        message = str(exc)
        return (_ev.yahoo_consensus_input([], retrieved_at=retrieved_at, status="PROVIDER_ERROR", message=message),
                {"price": None, "currency": None, "priceTime": None, "status": "PROVIDER_ERROR", "message": message})
    # yfinance tables carry no fiscal period end; FY0 ends at Yahoo's nextFiscalYearEnd.
    fy_end_epoch = info.get("nextFiscalYearEnd")
    fiscal_year_ends: dict[str, str] = {}
    if isinstance(fy_end_epoch, (int, float)) and fy_end_epoch > 0:
        fy0 = datetime.datetime.fromtimestamp(fy_end_epoch, datetime.timezone.utc).date().isoformat()
        fiscal_year_ends = {"0y": fy0, "+1y": _add_years(fy0, 1)}
    trend = [
        {"period": p, "earningsEstimate": ee.get(p) or {}, "revenueEstimate": re_.get(p) or {}, "epsTrend": et.get(p), "epsRevisions": er.get(p)}
        for p in ("0y", "+1y") if p in ee or p in re_ or p in et
    ]
    yahoo = _ev.yahoo_consensus_input(trend, retrieved_at=retrieved_at, financial_currency=info.get("financialCurrency"),
                                      fiscal_year_ends=fiscal_year_ends, fiscal_year_end_basis="DERIVED_FROM_NEXT_FISCAL_YEAR_END")
    price = info.get("regularMarketPrice") if info.get("regularMarketPrice") is not None else info.get("currentPrice")
    market_time = info.get("regularMarketTime")
    quote = {
        "price": _plain(price) if isinstance(_plain(price), (int, float)) else None,
        "currency": info.get("currency"),
        "priceTime": datetime.datetime.fromtimestamp(market_time, datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        if isinstance(market_time, (int, float)) else None,
        "status": "OK" if info else "NO_DATA",
        "message": None if info else "Yahoo returned no quote summary.",
    }
    return yahoo, quote


def _consensus_split_history(symbol: str) -> dict:
    """The ticker's splits over the last two years (2.5.23): enough to tell a provider EPS gap or a revision window
    that spans a split. A failure is reported, not read as "no splits". Blocking."""
    try:
        series = yf.Ticker(symbol).splits
        if series is None:
            return {"status": "NO_DATA", "splits": [], "message": "Yahoo returned no split history."}
        cutoff = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=2 * 365 + 1)
        splits = []
        for index, value in series.items():
            ratio = float(value)
            date = index.date() if hasattr(index, "date") else datetime.date.fromisoformat(str(index)[:10])
            if math.isfinite(ratio) and ratio > 0 and date >= cutoff:
                splits.append({"date": date.isoformat(), "ratio": ratio})
        splits.sort(key=lambda sp: sp["date"])
        return {"status": "OK", "splits": splits}
    except Exception as exc:  # noqa: BLE001 - reported as PROVIDER_ERROR, not read as no splits
        return {"status": "PROVIDER_ERROR", "splits": [], "message": str(exc)}


def _latest_provider_snapshot(provider: str, operation: str, symbol: str, as_of: str) -> dict | None:
    """The newest stored snapshot in the stale-fallback window, or None; a storage failure reads as none. At most eight reads, and only after a refusal."""
    store = _es.get_store()
    if store is None:
        return None
    try:
        for key in _ev.provider_snapshot_candidate_keys(provider, operation, symbol, as_of):
            text = store.get(key)
            if text is not None:
                return {"key": key, "text": text}
    except Exception:  # noqa: BLE001 - a storage failure means no fallback, not a failed curve
        return None
    return None


async def _alpha_vantage_estimates(symbol: str) -> dict:
    """Alpha Vantage EARNINGS_ESTIMATES as a consensus input (2.5.29). A good payload is kept once a day; when a live
    request is refused for a reason that passes, the newest kept payload within the window stands in, marked stale."""
    r = await _fetch_alpha_vantage_json("EARNINGS_ESTIMATES", {"symbol": symbol}, ttl_seconds=_ALPHA_VANTAGE_ESTIMATES_TTL_SECONDS)
    now = _now_iso()
    expect = {"provider": "alpha_vantage", "operation": "EARNINGS_ESTIMATES", "ticker": symbol}
    if r.status == "OK":
        fetched_at = r.fetched_at or now
        estimates = r.payload.get("estimates") if isinstance(r.payload, dict) else None
        if isinstance(estimates, list) and estimates:
            await asyncio.to_thread(
                _es.put_once,
                _ev.provider_snapshot_key(expect["provider"], expect["operation"], symbol, fetched_at),
                _ev.provider_snapshot_body(expect["provider"], expect["operation"], symbol, fetched_at, r.payload),
                {"ticker": symbol, "kind": "provider-snapshot"},
            )
        return _ev.alpha_vantage_consensus_input(r.payload, retrieved_at=fetched_at)
    stored = (await asyncio.to_thread(_latest_provider_snapshot, expect["provider"], expect["operation"], symbol, now)
              if r.status in _ev.STALE_FALLBACK_STATUSES else None)
    stale = _ev.stale_snapshot(stored, expect, {"status": r.status, "message": r.message}, now)
    if stale:
        return _ev.alpha_vantage_consensus_input(stale["payload"], retrieved_at=stale["fetchedAt"], stale_fallback=stale["staleFallback"])
    return _ev.alpha_vantage_consensus_input(r.payload, retrieved_at=r.fetched_at or now, status=r.status, message=r.message)


async def _consensus_providers(ticker: str) -> tuple[list[dict], dict, dict]:
    symbol = ticker.upper()
    async def alpha_watched() -> tuple[dict, bool]:
        # Its own budget watch, so a fetch the subrequest budget refused is charged to this provider (2.11.0).
        with _url_policy.budget_watch() as watch:
            read = await _alpha_vantage_estimates(symbol)
        return read, watch.starved

    (yahoo, quote), (alpha_read, alpha_starved), splits = await asyncio.gather(
        asyncio.to_thread(_yahoo_consensus_snapshot, symbol),
        alpha_watched(),
        asyncio.to_thread(_consensus_split_history, symbol),
    )
    # A provider starved of budget reports PROVIDER_BUDGET_EXHAUSTED, never PROVIDER_ERROR or NO_DATA.
    alpha_input = {**alpha_read, "status": "PROVIDER_BUDGET_EXHAUSTED"} if alpha_starved and alpha_read.get("status") != "OK" else alpha_read
    return [yahoo, alpha_input], quote, splits


async def _consensus_naming(ticker: str) -> dict:
    """How the company names its fiscal years, from its annual reports' stated fy (2.5.13); the period-end rule when SEC is not read."""
    try:
        cik_padded = await _resolve_cik_for_ticker(ticker)
        facts = await _edgar_get_company_facts(cik_padded) if cik_padded else None
    except Exception:  # noqa: BLE001 - SEC is optional here; the curve falls back to the period-end rule
        facts = None
    return fiscal_year_naming(facts)


def _consensus_policy(horizon_years: Any, min_analyst_count: Any, conflict_tolerance_pct: Any) -> dict | str:
    try:
        horizon = float(horizon_years)
        minimum = float(min_analyst_count)
        tolerance = float(conflict_tolerance_pct)
    except (TypeError, ValueError):
        return "horizon_years, min_analyst_count and conflict_tolerance_pct must be numbers."
    if not horizon.is_integer() or not 1 <= horizon <= 5:
        return "horizon_years must be an integer from 1 to 5."
    if not minimum.is_integer() or not 1 <= minimum <= 50:
        return "min_analyst_count must be an integer from 1 to 50."
    if not math.isfinite(tolerance) or not 0 <= tolerance <= 100:
        return "conflict_tolerance_pct must be from 0 to 100."
    return {**_ev.DEFAULT_CONSENSUS_POLICY, "horizonYears": int(horizon), "minAnalystCount": int(minimum), "conflictTolerancePct": tolerance}


def _write_consensus_observation(ticker: str, curve: dict, observed_at: str) -> dict:
    key = _ev.consensus_observation_key(ticker, observed_at)
    body = _ev.canonical_json({
        "schema": _ev.CONSENSUS_OBSERVATION_SCHEMA, "ticker": ticker.upper(), "observedAt": observed_at,
        "serverVersion": SERVER_VERSION, "buildSha": _BUILD_SHA, "curve": curve,
    })
    written = _es.put_once(key, body, {"ticker": ticker.upper(), "kind": "consensus-observation"})
    return {"status": written["status"], "key": None if written["status"] == "UNAVAILABLE" else key, **({"message": written["message"]} if written.get("message") else {})}


def _read_consensus_observation(ticker: str, observation_date: str) -> str:
    """A stored daily consensus observation, read back as it was written (2.5.22, F-006). Not found lists the stored dates."""
    ticker_err = _validate_ticker(ticker)
    if ticker_err:
        return _mcp_failure("get_consensus_forecast_curve", ErrorCode.INPUT_VALIDATION_ERROR, ticker_err)
    symbol = ticker.upper()
    if not _re.fullmatch(r"\d{4}-\d{2}-\d{2}", observation_date, _re.ASCII):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "observation_date must be YYYY-MM-DD."})
    store = _es.get_store()
    if store is None:
        return json.dumps({"ticker": symbol, "status": "STORAGE_UNAVAILABLE", "observationDate": observation_date,
                           "storage": {"source": "STORED_OBSERVATION", "storageStatus": "UNAVAILABLE"}})
    key = _ev.consensus_observation_key(symbol, observation_date)
    dates: list[str] = []
    try:
        text = store.get(key)
        if text is None:
            dates = [k.rsplit("/", 1)[-1].removesuffix(".json") for k in store.list(f"consensus-history/{symbol}/", 400)]
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"ticker": symbol, "status": "STORAGE_FAILED", "observationDate": observation_date, "retryable": True,
                           "storage": {"source": "STORED_OBSERVATION", "storageStatus": "FAILED", "key": key, "message": str(exc)}})
    if text is None:
        return json.dumps({"ticker": symbol, "status": "OBSERVATION_NOT_FOUND", "observationDate": observation_date,
                           "storage": {"source": "STORED_OBSERVATION", "storageStatus": "AVAILABLE", "key": key, "observationDates": dates}})
    stored = json.loads(text)
    curve = stored.get("curve") or {}
    return json.dumps({
        **curve,
        "storage": {
            "source": "STORED_OBSERVATION",
            "storageStatus": "AVAILABLE",
            "key": key,
            "schema": stored.get("schema"),
            "observedAt": stored.get("observedAt"),
            "serverVersion": stored.get("serverVersion"),
            "buildSha": stored.get("buildSha"),
        },
    })


@yfinance_server.tool(
    name="get_consensus_forecast_curve",
    description="Street consensus by fiscal year, FY0 to FY+horizon, for EPS and revenue from Yahoo Finance and Alpha Vantage, each provider reported separately with fiscal year end, currency, mean/high/low, analyst count and retrieval time. Each metric and period states its coverage: PROVIDER_COVERED, PROVIDER_NOT_COVERED, INSUFFICIENT_ANALYST_COUNT or PROVIDER_CONFLICT (with the cross-provider difference). Years and metrics no provider covers stay PROVIDER_NOT_COVERED; nothing is interpolated, extended by growth rates, or derived. crossCheck states whether the providers were actually compared: a failed provider (CROSS_CHECK_DEGRADED) or figures identical to the last digit (IDENTICAL, PROVIDERS_NOT_INDEPENDENT) are flagged. A provider row whose mean falls outside its own high-low range is PROVIDER_INCONSISTENT and left out of the comparison; an EPS gap between providers that matches a recent stock split is NOT_SPLIT_ADJUSTED, not a conflict. When Alpha Vantage refuses a request for quota, a timeout or an upstream error, its figures stored up to 7 days earlier stand in, marked with their retrieval time, staleFallback and PROVIDER_DATA_STALE. With observation_date, returns the curve stored that day. Evidence only.",
)
async def get_consensus_forecast_curve(
    ticker: str,
    horizon_years: int = 5,
    min_analyst_count: int = 3,
    conflict_tolerance_pct: float = 10,
    observation_date: Annotated[str | None, Field(description="Read the consensus observation stored on this date (YYYY-MM-DD) instead of fetching providers now; get_eps_revisions lists the stored dates.")] = None,
) -> str:
    if observation_date:
        return _read_consensus_observation(ticker, observation_date)
    policy = _consensus_policy(horizon_years, min_analyst_count, conflict_tolerance_pct)
    if isinstance(policy, str):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": policy})
    as_of = _now_iso()
    (inputs, _, splits), naming = await asyncio.gather(_consensus_providers(ticker), _consensus_naming(ticker))
    curve = _ev.build_consensus_curve(ticker, inputs, as_of, policy, naming, splits)
    # The first observation of the day is kept; later calls report ALREADY_STORED.
    observation = _write_consensus_observation(ticker, curve, as_of)
    return json.dumps({**curve, "storage": {"consensusObservation": observation}})


@yfinance_server.tool(
    name="get_eps_revisions",
    description="EPS estimate revision windows for FY0 and FY+1 as each provider reports them: the mean now and 7, 30, 60 and 90 days ago with change and percent change, and up/down revision counts over 7 and 30 days. A window spanning a stock split carries no change (SPLIT_IN_WINDOW), nor does a row whose mean falls outside its own range (PROVIDER_INCONSISTENT). Stored figures standing in for a refused provider request (PROVIDER_DATA_STALE) have windows ending at their retrieval time. Revenue revisions and analyst adds/drops are PROVIDER_NOT_COVERED. Lists the dates of stored daily consensus observations. Evidence only.",
)
async def get_eps_revisions(ticker: str) -> str:
    ticker_err = _validate_ticker(ticker)
    if ticker_err:
        return _mcp_failure("get_eps_revisions", ErrorCode.INPUT_VALIDATION_ERROR, ticker_err)
    as_of = _now_iso()
    inputs, _, splits = await _consensus_providers(ticker)
    revisions = _ev.build_eps_revisions(ticker, inputs, as_of, splits)
    store = _es.get_store()
    stored: dict = {"storageStatus": "UNAVAILABLE", "observationDates": []}
    if store is not None:
        try:
            keys = store.list(f"consensus-history/{ticker.upper()}/", 400)
            stored = {"storageStatus": "AVAILABLE", "observationDates": [k.rsplit("/", 1)[-1].removesuffix(".json") for k in keys]}
        except Exception as exc:  # noqa: BLE001
            stored = {"storageStatus": "FAILED", "observationDates": [], "message": str(exc)}
    return json.dumps({**revisions, "storedConsensusObservations": stored})


async def _sec_filing_rows(ticker: str) -> tuple[list[dict] | None, str]:
    try:
        cik_padded, subs = await _get_submissions_for_ticker(ticker)
    except Exception as exc:  # noqa: BLE001
        return None, f"PROVIDER_ERROR: {exc}"
    if not cik_padded or not subs:
        return None, "TICKER_NOT_FOUND"
    recent = (subs.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []

    def at(field: str, i: int) -> Any:
        values = recent.get(field) or []
        return values[i] if i < len(values) else None

    rows = []
    for i, form in enumerate(forms):
        filing_date = str(at("filingDate", i) or "")
        if not filing_date:
            continue
        inline = at("isInlineXBRL", i)
        rows.append({
            "form": str(form),
            "filingDate": filing_date,
            "reportDate": str(at("reportDate", i)) if at("reportDate", i) else None,
            "items": str(at("items", i)) if at("items", i) is not None else None,
            "isInlineXBRL": (int(inline) == 1) if inline is not None and str(inline).strip() != "" else None,
        })
    return rows, "OK"


@yfinance_server.tool(
    name="get_evidence_quality",
    description="Preflight before a valuation evidence pack: status, freshness and coverage per evidence family (quote, latest SEC periodic filing, capital structure and dilution readiness, earnings-release guidance, material 8-Ks, FY0/FY+1 consensus cells, durable storage) and the blockers, from light requests only. Evidence only.",
)
async def get_evidence_quality(ticker: str) -> str:
    as_of = _now_iso()
    (inputs, quote, splits), (rows, status), naming = await asyncio.gather(_consensus_providers(ticker), _sec_filing_rows(ticker), _consensus_naming(ticker))
    curve = _ev.build_consensus_curve(ticker, inputs, as_of, None, naming, splits)
    return json.dumps(_ev.evidence_quality(ticker=ticker, as_of=as_of, quote=quote, filings=rows, filings_status=status,
                                           consensus=curve, storage_available=_es.get_store() is not None))


async def _run_component(source_tool: str, coro: Any) -> dict:
    try:
        text = await coro
        return _ev.component_from_tool_text(source_tool, text, _now_iso())
    except Exception as exc:  # noqa: BLE001 - a failed component is evidence of that failure
        return _ev.component_from_tool_text(source_tool, None, _now_iso(), exc)


def _not_applicable(source_tool: str, code: str, message: str, retrieved_at: str) -> dict:
    return {"status": "NOT_APPLICABLE", "sourceTool": source_tool, "retrievedAt": retrieved_at, "data": None, "warnings": [], "error": {"code": code, "message": message}}


@yfinance_server.tool(
    name="build_valuation_evidence_pack",
    description="One evidence cut for valuation work: quote, evidence quality, consensus curve, EPS revisions, current capital structure, dilution bridge at the current price, latest guidance and material filings, each with its source tool, status, warnings and failure, plus a provenance receipt hashing every component. decisionUse is EVIDENCE_ONLY. Stored immutably under a content-addressed evidenceCutId when storage is available; the full payload is returned either way.",
)
async def build_valuation_evidence_pack(ticker: str, horizon_years: int = 5, persist: bool = True) -> str:
    policy = _consensus_policy(horizon_years, _ev.DEFAULT_CONSENSUS_POLICY["minAnalystCount"], _ev.DEFAULT_CONSENSUS_POLICY["conflictTolerancePct"])
    if isinstance(policy, str):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": policy})
    symbol = ticker.upper()
    cutoff = _now_iso()
    (inputs, quote, splits), (rows, filings_status), naming = await asyncio.gather(_consensus_providers(symbol), _sec_filing_rows(symbol), _consensus_naming(symbol))
    curve = _ev.build_consensus_curve(symbol, inputs, cutoff, policy, naming, splits)
    major_price, major_currency = _vl._major_price(quote["price"], quote["currency"]) if quote["price"] is not None else (None, None)

    async def dilution() -> dict:
        if major_price is None:
            return _not_applicable("extract_dilution_bridge", "PRICE_UNAVAILABLE", "No current price to run the dilution bridge at.", cutoff)
        if major_currency != "USD":
            return _not_applicable("extract_dilution_bridge", "NON_USD_LISTING", f"The dilution bridge reads SEC filings for USD listings; {symbol} is quoted in {quote['currency']}.", cutoff)
        return await _run_component("extract_dilution_bridge", extract_dilution_bridge(ticker=symbol, price=major_price, currency="USD", filing_type="latest", include_atm=True))

    await _warm_sec_submissions(symbol)
    capital, diluted, guidance, events = await asyncio.gather(
        _run_component("extract_capital_structure", extract_capital_structure(ticker=symbol, filing_type="latest", include_funding_statements=True)),
        dilution(),
        _run_component("extract_guidance", extract_guidance(symbol, "latest")),
        _run_component("list_sec_material_filings", list_sec_material_filings(symbol, None, 10)),
    )
    # The release can fail to load while the other SEC reads run; one sequential retry, and the result says which.
    if (guidance.get("error") or {}).get("code") == "RELEASE_TEXT_NOT_AVAILABLE":
        guidance = await _run_component("extract_guidance", extract_guidance(symbol, "latest"))
    components = {
        "quote": {
            "status": "OK" if quote["price"] is not None else "FAILED",
            "sourceTool": "yahoo_quote_summary",
            "retrievedAt": inputs[0]["retrievedAt"],
            "data": {**quote, "majorUnitPrice": major_price, "majorUnitCurrency": major_currency},
            "warnings": [],
            "error": None if quote["price"] is not None else {"code": quote["status"], "message": quote.get("message") or "No price."},
        },
        "evidenceQuality": _ev.component_from_value("get_evidence_quality", _ev.evidence_quality(
            ticker=symbol, as_of=cutoff, quote=quote, filings=rows, filings_status=filings_status, consensus=curve, storage_available=_es.get_store() is not None,
        ), cutoff),
        "consensus": _ev.component_from_value("get_consensus_forecast_curve", curve, cutoff),
        "epsRevisions": _ev.component_from_value("get_eps_revisions", _ev.build_eps_revisions(symbol, inputs, cutoff, splits), cutoff),
        "currentCapitalStructure": capital,
        "currentDilution": diluted,
        "latestGuidance": guidance,
        "materialEvents": events,
    }
    # A pack whose providers all failed is still evidence of that; its components say so.
    if all(not i["periods"] for i in inputs):
        components["consensus"]["status"] = "LIMITED"
    # Components carry confidence levels only (F20), fixed before they are hashed.
    for component in components.values():
        normalize_status_confidence(component)
    hashes = {name: _es.sha256_hex(_ev.canonical_json(c)) for name, c in components.items()}
    receipt = _ev.build_receipt(ticker=symbol, evidence_cutoff=cutoff, server_version=SERVER_VERSION, build_sha=_BUILD_SHA, runtime="python_local",
                                components=components, component_hashes=hashes, components_sha256=_es.sha256_hex(_ev.canonical_json(components)))
    document = _ev.evidence_cut_document(ticker=symbol, evidence_cutoff=cutoff, components=components, receipt=receipt)
    body = _ev.canonical_json(document)
    sha = _es.sha256_hex(body)
    key = _ev.evidence_cut_key(symbol, _ev.compact_timestamp(cutoff), sha)
    stored = _es.put_once(key, body, {"ticker": symbol, "sha256": sha, "serverVersion": SERVER_VERSION}) if persist is not False else {"status": "SKIPPED"}
    observation = _write_consensus_observation(symbol, curve, cutoff) if persist is not False else {"status": "SKIPPED", "key": None}
    return json.dumps({
        **document,
        "evidenceCut": {
            "evidenceCutId": _ev.evidence_cut_id(symbol, cutoff, sha),
            "contentSha256": sha,
            "storageStatus": stored["status"],
            "storageKey": key if stored["status"] in ("STORED", "ALREADY_STORED") else None,
            **({"storageMessage": stored["message"]} if stored.get("message") else {}),
            "consensusObservation": observation,
            "verification": "contentSha256 is SHA-256 over canonical JSON (sorted keys, no whitespace, ECMAScript numbers) of this payload without the evidenceCut field.",
        },
    })


@yfinance_server.tool(
    name="get_evidence_cut",
    description="Retrieve a stored evidence cut by evidenceCutId and verify its integrity: the SHA-256 of the stored canonical JSON must equal the hash in the id (integrity VERIFIED or MISMATCH).",
)
async def get_evidence_cut(evidence_cut_id: str) -> str:
    parsed = _ev.parse_evidence_cut_id(evidence_cut_id or "")
    if parsed is None:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "evidence_cut_id must look like ec1_<TICKER>_<YYYYMMDDTHHMMSSZ>_<sha256>."})
    store = _es.get_store()
    if store is None:
        return json.dumps({"error": True, "code": "STORAGE_UNAVAILABLE", "message": "Durable evidence storage is not configured; evidence cuts cannot be retrieved."})
    try:
        text = store.get(parsed["key"])
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"error": True, "code": "STORAGE_ERROR", "message": str(exc)})
    if text is None:
        return json.dumps({"error": True, "code": "NOT_FOUND", "message": f"No evidence cut is stored under {evidence_cut_id}."})
    actual = _es.sha256_hex(text)
    try:
        document = json.loads(text)
    except ValueError:
        document = None
    return json.dumps({
        "evidenceCutId": evidence_cut_id,
        "storageKey": parsed["key"],
        "integrity": "VERIFIED" if actual == parsed["sha256"] and document is not None else "MISMATCH",
        "expectedSha256": parsed["sha256"],
        "actualSha256": actual,
        "document": document,
    })


@yfinance_server.tool(
    name="list_evidence_cuts",
    description="Stored evidence cuts for a ticker, newest first, with their ids and cutoff times.",
)
async def list_evidence_cuts(ticker: str, limit: int = 20) -> str:
    ticker_err = _validate_ticker(ticker)
    if ticker_err:
        return _mcp_failure("list_evidence_cuts", ErrorCode.INPUT_VALIDATION_ERROR, ticker_err)
    symbol = ticker.upper()
    store = _es.get_store()
    if store is None:
        return json.dumps({"ticker": symbol, "storageStatus": "UNAVAILABLE", "cuts": []})
    cap = max(1, min(100, int(limit) if isinstance(limit, (int, float)) else 20))
    try:
        keys = store.list(f"evidence-cuts/{symbol}/", 1000)
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"ticker": symbol, "storageStatus": "FAILED", "cuts": [], "message": str(exc)})
    cuts = []
    for key in keys:
        m = _re.match(r"^evidence-cuts/([^/]+)/(\d{8}T\d{6}Z)/([0-9a-f]{64})\.json$", key)
        if m:
            cuts.append({"evidenceCutId": f"ec1_{m.group(1)}_{m.group(2)}_{m.group(3)}", "cutoff": m.group(2), "storageKey": key})
    cuts.sort(key=lambda c: c["cutoff"], reverse=True)
    return json.dumps({"ticker": symbol, "storageStatus": "AVAILABLE", "cuts": cuts[:cap]})


# ---------------------------------------------------------------------------
# Grouped (token-efficient) server mode
# ---------------------------------------------------------------------------
# TOOL_MODE env var controls which interface is exposed:
#   - "grouped" (default): 12 domain meta-tools with action routing
#   - "expanded": all 79 individual tools (compatibility/debug mode)
# ---------------------------------------------------------------------------
_TOOL_MODE = os.environ.get("TOOL_MODE", "grouped").lower().strip()

# tool_catalog.json tickerCaps are advertised as maxItems by YfmcpServer.list_tools (2.12.0, Q3c).


def _build_grouped_server():
    """Create a YfmcpServer with grouped meta-tools for token efficiency."""
    from tool_groups import TOOL_GROUPS, register_grouped_tools
    from yfmcp.app import build_tool_contract_registry

    grouped = create_server(
        "yfinance",
        instructions="""
# Yahoo Finance MCP Server (Grouped Mode)

Grouped mode is the default. Set TOOL_MODE=expanded to expose 79 individual tools.

This server provides financial market data via domain-grouped tools for token efficiency.
Each tool covers a domain (pricing, fundamentals, options, etc.) and accepts an `action`
parameter to select the specific operation, plus a `params` dict for action arguments.

## How to call
1. Pick the domain tool (e.g. `stock_pricing`, `sec_filings`)
2. Set `action` to the specific operation (listed in each tool's description)
3. Pass action-specific arguments in `params` dict (e.g. `{"ticker": "AAPL", "period": "1y"}`)

## Example
Tool: stock_pricing
Input: {"action": "get_market_quote", "params": {"ticker": "AAPL"}}

## Domain tools available
- stock_pricing: Price, volume, technicals, short interest
- stock_fundamentals: Company profile, financials, ratios, credit health
- analyst_data: Consensus, recommendations, earnings momentum
- options_analysis: Chain data, flow, hedging candidates
- sec_filings: EDGAR access, indexing, text search
- sec_extractors: Geographic/segment revenue, risk factors
- news_events: Multi-source news, press releases, event timeline
- earnings_intelligence: Earnings metrics, guidance, commentary
- screening: Ticker search, stock screens, position signals
- system: Health check, diagnostics
""",
    )

    # Resolve handlers from the shared server's registry (single source of truth)
    # rather than this module's globals, so the mapping holds as handlers move
    # into yfmcp.tools.* during the Phase 2 split.
    handler_registry = build_handler_registry(yfinance_server)
    contract_registry = build_tool_contract_registry(yfinance_server)
    register_grouped_tools(grouped, handler_registry, contract_registry)
    return grouped


# Lazily built grouped server (only constructed if TOOL_MODE=grouped)
_grouped_server = None


def get_server():
    """Return the appropriate server based on TOOL_MODE env var.

    - TOOL_MODE=grouped (default): 12 domain meta-tools
    - TOOL_MODE=expanded: 79 individual tools
    """
    global _grouped_server
    if _TOOL_MODE == "grouped":
        if _grouped_server is None:
            _grouped_server = _build_grouped_server()
        return _grouped_server
    return yfinance_server


if __name__ == "__main__":
    # Initialize and run the server
    server = get_server()
    mode_label = "grouped" if _TOOL_MODE == "grouped" else "expanded"
    print(f"Starting Yahoo Finance MCP server (mode={mode_label})...", file=sys.stderr)
    server.run(transport="stdio")
