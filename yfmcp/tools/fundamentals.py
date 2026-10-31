"""Fundamentals and analyst MCP tools.

Phase 5 unit 4: extracted from server.py as a pure move (no behaviour change).
Covers company profile and stock info, financial statements and ratios, calendars and corporate
actions, holders and ownership, ETF and fund profiles, analyst consensus, recommendations, rating
changes and the upgrade radar, EPS revisions, the consensus forecast curve, earnings analysis and
momentum, credit health, share-count trend, peer valuations, analyst valuation methods and the
two position-score tools that call these handlers.

Shared dependencies are imported from ``yfmcp.*``; this module never imports ``server`` (no import
cycle). ``server.py`` re-exports the names that remaining server code, grouped routing, scripts and
tests still look up as ``server.<name>``.
"""
import asyncio
import datetime
import json
import math
import re as _re
import sys
from typing import Annotated, Any, Literal

import pandas as pd
import yfinance as yf
from pydantic import Field

from yfmcp import capital_structure as _cs
from yfmcp import evidence as _ev
from yfmcp import evidence_store as _es
from yfmcp import url_policy as _url_policy
from yfmcp import valuation as _vl
from yfmcp.app import yfinance_server
from yfmcp.build_info import BUILD_SHA as _BUILD_SHA
from yfmcp.cache import _PRICE_TTL, _STMT_TTL, _tool_cache
from yfmcp.clients.edgar import _edgar_get_company_facts, _resolve_cik_for_ticker
from yfmcp.clients.market_providers import fetch_alpha_vantage_json as _fetch_alpha_vantage_json
from yfmcp.clients.yahoo import _safe_parse, _valuation_market_inputs
from yfmcp.envelope import SERVER_VERSION, ErrorCode, _mcp_failure
from yfmcp.evidence import EPS_BASIS as _EPS_BASIS
from yfmcp.fiscal_calendar import fiscal_year_naming
from yfmcp.pricing_notices import take_notices, unique_notices, with_notices
from yfmcp.schemas import FinancialType, HolderType, RecommendationType
from yfmcp.tools.news import get_company_news
from yfmcp.tools.pricing import (
    _run_ticker_batch,
    get_ma_position,
    get_options_summary,
    get_price_stats,
    get_technical_indicators,
)
from yfmcp.util import (
    _add_years,
    _clamp_int,
    _df_period_rows,
    _fetch_with_retry,
    _now_iso,
    _plain,
    _unwrap_payload,
    fetch_quote_summary_module,
    get_last_trading_date,
    unwrap_yahoo_values,
    yahoo_iso,
    yahoo_raw,
)
from yfmcp.validation import _validate_ticker


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


@yfinance_server.tool(name="analyze_position_signals", description="Canonical alias for get_position_score_inputs.")
async def analyze_position_signals(ticker: str) -> str:
    return await get_position_score_inputs(ticker)


@yfinance_server.tool(
    name="extract_analyst_valuation_methods",
    description="Valuation methods analysts name in recent news headlines and summaries: multiples with metric and period (e.g. 41x 2H27 EV/EBITDA), DCF with WACC, discount rate and terminal growth, sum-of-the-parts and rNPV, with firm, price target and source link. Lists firms whose targets carry no disclosed method. Context only: not consensus inputs.",
    after="reconcile_metric_sources",
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
    after="get_valuation_snapshot",
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
    after="get_earnings_call_transcript",
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
    after="get_consensus_forecast_curve",
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
