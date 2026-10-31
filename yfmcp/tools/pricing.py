import asyncio
import datetime
import json
import math
import re
import re as _re
import sys
from typing import Annotated, Any, Literal, get_args
import zoneinfo

import pandas as pd
import yfinance as yf
from pydantic import Field

from yfmcp import url_policy
from yfmcp.app import yfinance_server
from yfmcp.schemas import _MARKET_SNAPSHOT_OUTPUT_SCHEMA, _TOOL_OUTPUT_SCHEMAS
from yfmcp.deprecations import MIN_ADV_USD_DESCRIPTION, meets_minimum, parse_min_adv_usd
from yfmcp.envelope import ErrorCode, _classify_error_message, _mcp_failure
from yfmcp.validation import _validate_ticker
from yfmcp.cache import _PRICE_TTL, _STMT_TTL, _tool_cache
from yfmcp.util import (
    _PLACEHOLDER_IV_THRESHOLD,
    _add_years,
    _compute_data_quality,
    _fetch_with_retry,
    _round_half_away_from_zero,
    _sort_by_relevance,
    get_last_trading_date,
)
from yfmcp.clients.yahoo import _safe_parse
from yfmcp.exchange_calendars import previous_exchange_session
from yfmcp.pricing_notices import freshness_notices as _freshness_notices
from yfmcp.pricing_notices import (
    SESSION_CALENDAR_UNKNOWN,
    freshness_notices,
    insufficient_history_notice,
    take_notices,
    with_notices,
    zero_volume_notice,
)


# Batch requests run this many tickers at once, as the Worker does
# (BATCH_TICKER_CONCURRENCY), so a batch is faster without bursting Yahoo.
_BATCH_TICKER_CONCURRENCY = 3


def _budget_exhausted_ticker_result(ticker: str, result: object) -> object:
    """A failed ticker whose work met a budget refusal keeps its message and reports PROVIDER_BUDGET_EXHAUSTED,
    retryable, RETRY_LATER. A ticker that completed is returned as is."""
    message: str | None = None
    if isinstance(result, Exception):
        message = str(result)
    else:
        try:
            parsed = json.loads(result)  # type: ignore[arg-type]
        except Exception:
            message = str(result)
        else:
            if isinstance(parsed, dict) and (parsed.get("error") is True or parsed.get("ok") is False):
                error = parsed.get("error")
                detail = error.get("message") if isinstance(error, dict) else parsed.get("message")
                message = detail if isinstance(detail, str) else f"Error for {ticker}"
    if message is None:
        return result
    return json.dumps({
        "error": True,
        "code": ErrorCode.PROVIDER_BUDGET_EXHAUSTED,
        "message": message,
        "ticker": ticker,
        "retryable": True,
        "recoveryAction": "RETRY_LATER",
    })


async def _run_ticker_batch(tickers: list[str], call) -> list[object]:
    """Run ``call(ticker)`` for each ticker, at most three at a time.

    Results keep the input order; a failure is returned as its exception.
    The per-ticker tools make blocking yfinance calls, so each call runs in
    its own event loop on a worker thread (as MCP invocations do, see
    ``yfmcp.app._offload_to_thread``).
    """
    semaphore = asyncio.Semaphore(_BATCH_TICKER_CONCURRENCY)

    async def one(ticker: str) -> object:
        async with semaphore:
            # Each ticker runs in its own budget watch, so a fetch the subrequest budget refused is charged
            # to the ticker that asked for it (2.11.0).
            with url_policy.budget_watch() as watch:
                try:
                    result: object = await asyncio.to_thread(lambda: asyncio.run(call(ticker)))
                except Exception as e:
                    result = e
            if watch.starved or (isinstance(result, Exception) and url_policy.is_budget_exhausted_error(result)):
                return _budget_exhausted_ticker_result(ticker, result)
            return result

    return await asyncio.gather(*(one(t) for t in tickers))


# Yahoo answers an unknown range or interval with a fallback series (a single
# live bar) instead of an error, so inputs are checked against these sets.
HistoricalPeriod = Literal["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]
HistoricalInterval = Literal["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"]
HISTORICAL_PERIODS: tuple[str, ...] = get_args(HistoricalPeriod)
HISTORICAL_INTERVALS: tuple[str, ...] = get_args(HistoricalInterval)


def _historical_range_error(period: str, interval: str) -> str | None:
    if period not in HISTORICAL_PERIODS:
        return f"period must be one of: {', '.join(HISTORICAL_PERIODS)}"
    if interval not in HISTORICAL_INTERVALS:
        return f"interval must be one of: {', '.join(HISTORICAL_INTERVALS)}"
    return None


async def get_historical_stock_prices(
    ticker: str, period: HistoricalPeriod = "1mo", interval: HistoricalInterval = "1d",
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
    period = str(period).strip()
    interval = str(interval).strip()
    range_err = _historical_range_error(period, interval)
    if range_err:
        return _mcp_failure("get_historical_stock_prices", ErrorCode.INPUT_VALIDATION_ERROR, range_err)

    cache_key = f"hist:{ticker}:{period}:{interval}:{prepost}:camel-v5"
    column_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "Adj Close": "adjClose",
        "Dividends": "dividends",
        "Stock Splits": "stockSplits",
    }

    def _filter_rows(rows: list[dict]) -> list[dict]:
        if not columns:
            return rows
        wanted = {"date", "tradingDate"}
        if interval == "1d":
            wanted.update({"barStatus", "isFinal"})
        for col in columns:
            key = column_map.get(str(col), str(col))
            wanted.add(key)
        return [{k: r[k] for k in wanted if k in r} for r in rows]

    cached = _tool_cache.get_value(cache_key)
    if cached is not None:
        if columns:
            try:
                rows = json.loads(cached)
                return json.dumps(_filter_rows(rows))
            except Exception:
                pass
        return cached

    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.", file=sys.stderr)
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting historical stock prices for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting historical stock prices for {ticker}: {e}"

    try:
        hist_data = await _fetch_with_retry(company.history, period, interval, prepost=prepost)
    except Exception as e:
        print(f"Error: getting historical stock prices for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting historical stock prices for {ticker}: {e}"

    if interval == "1d":
        try:
            metadata = await asyncio.to_thread(company.get_history_metadata)
            prepared = _prepare_completed_daily_history(
                metadata if isinstance(metadata, dict) else {},
                hist_data,
                int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
            )
            completed_index = prepared["completed"].index
            raw_close = (
                pd.to_numeric(hist_data["Close"], errors="coerce")
                if "Close" in hist_data
                else pd.Series(float("nan"), index=hist_data.index)
            )
            adjusted_close = (
                pd.to_numeric(hist_data["Adj Close"], errors="coerce")
                if "Adj Close" in hist_data
                else pd.Series(float("nan"), index=hist_data.index)
            )
            usable_close = raw_close.notna() | adjusted_close.notna()
            statuses = [
                "COMPLETE" if index in completed_index and bool(has_close) else "INCOMPLETE"
                for index, has_close in zip(hist_data.index, usable_close.tolist())
            ]
            hist_data = hist_data.copy()
            hist_data["barStatus"] = statuses
            hist_data["isFinal"] = [status == "COMPLETE" for status in statuses]
        except Exception:
            hist_data = hist_data.copy()
            hist_data["barStatus"] = "UNKNOWN"
            hist_data["isFinal"] = False

    # The index is in the exchange's timezone; `date` serializes as UTC, which
    # puts east-of-UTC sessions on the previous day, so keep the local date.
    hist_data = hist_data.copy()
    hist_data.insert(0, "tradingDate", [
        ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else None for ts in hist_data.index
    ])
    hist_data = hist_data.reset_index(names="Date").rename(columns=column_map)
    full_result = hist_data.to_json(orient="records", date_format="iso")
    _tool_cache.set(cache_key, full_result, _PRICE_TTL)

    if columns:
        try:
            rows = json.loads(full_result)
            return json.dumps(_filter_rows(rows))
        except Exception:
            pass
    return full_result

# ---------------------------------------------------------------------------

async def get_fast_info(ticker: str | list[str]) -> str:
    """Get lightweight real-time price and market data for a ticker symbol."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, get_fast_info)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    cache_key = f"fast_info:{ticker}"
    cached = _tool_cache.get_value(cache_key)
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
        print(f"Error: getting fast info for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting fast info for {ticker}: {e}"

    data["priceBasis"] = "REGULAR_MARKET_PRICE"
    data["observationType"] = "REGULAR_MARKET_QUOTE"
    data["priceTimestamp"] = None
    data["marketState"] = None

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
        data["priceTimestamp"] = (
            datetime.datetime.fromtimestamp(reg_mkt_time, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
            if isinstance(reg_mkt_time, (int, float)) and reg_mkt_time
            else None
        )
        data["marketState"] = info.get("marketState")
        data["lastTradeDate"] = (
            datetime.datetime.fromtimestamp(reg_mkt_time, datetime.timezone.utc).strftime('%Y-%m-%d')
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

    # Normalize any empty-dict values (Yahoo API sometimes returns {} for missing scalars), and NaN, which is not
    # JSON (2.25.2, F30: an unknown ticker returned yearHigh/yearLow NaN).
    data = {
        k: (None if (isinstance(v, dict) and not v) or (isinstance(v, float) and v != v) else v)
        for k, v in data.items()
    }

    # An unknown ticker gets every field null from yfinance; the Worker's quoteSummary 404 is TICKER_NOT_FOUND (F30).
    if data.get("lastPrice") is None and data.get("currency") is None and data.get("quoteType") is None:
        return _mcp_failure(
            "get_market_quote",
            ErrorCode.TICKER_NOT_FOUND,
            "No provider data was found for the requested ticker.",
            meta_extra={"retryable": False},
        )

    result = json.dumps(data)
    _tool_cache.set(cache_key, result, _PRICE_TTL)
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
    cached = _tool_cache.get_value(cache_key)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        info = company.info
    except Exception as e:
        print(f"Error: getting short interest for {ticker}: {e}", file=sys.stderr)
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
    # Observation dates as YYYY-MM-DD, as get_short_momentum reports them.
    for key in ("dateShortInterest", "sharesShortPreviousMonthDate"):
        value = data.get(key)
        if isinstance(value, (int, float)) and value > 0:
            data[key] = datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc).date().isoformat()
        elif isinstance(value, str) and len(value) >= 10:
            data[key] = value[:10]
    data["dataDate"] = data.get("dateShortInterest") if isinstance(data.get("dateShortInterest"), str) else None
    data["dataDateBasis"] = "SHORT_INTEREST_OBSERVATION"
    data["unitSemantics"] = {
        "decimalRatios": ["shortPercentOfFloat", "sharesPercentSharesOut"],
        "days": ["shortRatio"],
        "shares": ["sharesShort", "sharesShortPriorMonth", "floatShares", "sharesOutstanding"],
    }

    result = json.dumps(data)
    _tool_cache.set(cache_key, result, _STMT_TTL)
    return result


# ---------------------------------------------------------------------------
# Group 2.1 — get_price_stats

# ---------------------------------------------------------------------------

async def get_price_stats(ticker: str | list[str]) -> str:
    """Get pre-computed price statistics for a ticker."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, get_price_stats)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    cache_key = f"price_stats:{ticker}"
    cached = _tool_cache.get_value(cache_key)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        last_price = fi.last_price
        prev_close = fi.previous_close
    except Exception as e:
        print(f"Error: getting price stats for {ticker}: {e}", file=sys.stderr)
        return f"Error: getting price stats for {ticker}: {e}"

    def _pct(value, reference):
        if reference and reference != 0:
            return round((value - reference) / reference * 100, 4)
        return None

    stats: dict = {
        "ticker": ticker,
        "status": "OK",
        "currency": fi.currency,
        "lastPrice": last_price,
        "priceBasis": "REGULAR_MARKET_PRICE",
        "priceObservationType": "REGULAR_MARKET_QUOTE",
        "priceTimestamp": None,
        "marketState": None,
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

    try:
        info = company.info
        market_time = info.get("regularMarketTime")
        stats["priceTimestamp"] = (
            datetime.datetime.fromtimestamp(
                market_time,
                tz=datetime.timezone.utc,
            ).isoformat().replace("+00:00", "Z")
            if isinstance(market_time, (int, float)) and market_time
            else None
        )
        stats["marketState"] = info.get("marketState")
    except Exception:
        pass

    prepared = None
    selected = None
    notices: list[dict] = []
    try:
        prepared = await _load_completed_daily_history(
            company,
            "5y",
            required_bars=31,
            retry_adjusted_gap=True,
        )
        completed = prepared["completed"]
        selected = _select_completed_close_series(completed)
        if prepared["freshnessStatus"] == "STALE":
            stats["status"] = "PARTIAL"
        elif selected is not None:
            closes, historical_price_basis, fallback_used = selected
            volatility = _annualized_volatility([float(v) for v in closes], 30)
            if volatility is not None:
                stats["annualizedVolatility30d"] = round(volatility, 4)

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

            # The horizon starts at the first bar on or after the same exchange-local date N years before the last
            # completed bar (29 February -> 28 February), in both runtimes (R5-pricing-1, BR-PRC-021).
            bar_dates = [_daily_bar_date(ts, prepared["timezoneName"]) for ts in closes.index]
            end_date = bar_dates[-1]
            for years, label in [(1, "cagr1y"), (3, "cagr3y"), (5, "cagr5y")]:
                cutoff_date = _add_years(end_date, -years) if end_date else None
                # A history that starts after the window does not cover it: null with INSUFFICIENT_HISTORY instead of
                # dividing a shorter run by the nominal years (R5-pricing-2, 3.1.0).
                if cutoff_date is not None and not _history_covers(bar_dates[0], cutoff_date):
                    stats[label] = None
                    notices.append(insufficient_history_notice(ticker, years))
                    continue
                subset = closes[[d is not None and cutoff_date is not None and d >= cutoff_date for d in bar_dates]]
                stats[label] = _cagr(subset, years)
            stats["historicalPriceBasis"] = historical_price_basis
            stats["fallbackUsed"] = fallback_used
        else:
            stats["status"] = "PARTIAL"
    except Exception:
        stats["status"] = "PARTIAL"

    completed = prepared["completed"] if prepared is not None else pd.DataFrame()
    timezone_name = prepared["timezoneName"] if prepared is not None else "UTC"
    data_date = (
        _daily_bar_date(completed.index[-1], timezone_name)
        if not completed.empty
        else None
    )
    stats.setdefault("annualizedVolatility30d", None)
    for label in ("cagr1y", "cagr3y", "cagr5y"):
        stats.setdefault(label, None)
    stats.setdefault("historicalPriceBasis", None)
    stats.setdefault("fallbackUsed", False)
    stats.update({
        "historicalObservationType": "COMPLETED_DAILY_PRICE_SERIES",
        "historicalBarStatus": (
            "STALE"
            if prepared is not None and prepared["freshnessStatus"] == "STALE"
            else (
                "COMPLETE"
                if selected is not None
                else ("INCOMPLETE" if not completed.empty else "UNAVAILABLE")
            )
        ),
        "freshnessStatus": (
            prepared["freshnessStatus"] if prepared is not None else "UNKNOWN"
        ),
        "expectedCompletedDate": (
            prepared["expectedCompletedDate"] if prepared is not None else None
        ),
        "latestAvailableBarDate": data_date,
        "excludedIncompleteBar": (
            prepared["excludedIncompleteBar"] if prepared is not None else False
        ),
        "retryAttempted": (
            prepared["retryAttempted"] if prepared is not None else False
        ),
        "recommendedNextAction": "RETRY" if stats["status"] == "PARTIAL" else "NONE",
        "dataDate": data_date,
    })
    with_notices(stats, [*freshness_notices(ticker, prepared.get("freshnessReason") if prepared is not None else None), *notices])
    result = json.dumps(stats)
    _tool_cache.set(cache_key, result, _PRICE_TTL)
    return result


# ---------------------------------------------------------------------------
# Group 2.2 — get_analyst_consensus

# ---------------------------------------------------------------------------

# RSI and MACD are recursive averages: their value depends on how much history
# feeds them until the seed's weight decays. A year of daily bars (about 250)
# leaves the seed under 0.01% of the result, so shorter periods are widened.
# These mirror the Worker's wilderRsi, macd and annualizedVolatility exactly.
INDICATOR_MIN_LOOKBACK = "1y"
_INDICATOR_LONG_PERIODS = {"1y", "2y", "5y", "10y", "max"}


def _indicator_lookback(period: str | None) -> str:
    return period if period in _INDICATOR_LONG_PERIODS else INDICATOR_MIN_LOOKBACK


def _wilder_rsi(closes: list[float], period: int = 14) -> float | None:
    """Wilder RSI: gains and losses averaged over the first `period` changes, then smoothed by 1/period."""
    if len(closes) <= period:
        return None
    avg_gain = 0.0
    avg_loss = 0.0
    for i in range(1, period + 1):
        change = closes[i] - closes[i - 1]
        if change > 0:
            avg_gain += change
        else:
            avg_loss -= change
    avg_gain /= period
    avg_loss /= period
    for i in range(period + 1, len(closes)):
        change = closes[i] - closes[i - 1]
        avg_gain = (avg_gain * (period - 1) + (change if change > 0 else 0)) / period
        avg_loss = (avg_loss * (period - 1) + (-change if change < 0 else 0)) / period
    if avg_loss == 0:
        return 50.0 if avg_gain == 0 else 100.0
    return 100 - 100 / (1 + avg_gain / avg_loss)


def _ema_series(values: list[float], span: int) -> list[float]:
    k = 2 / (span + 1)
    out = [values[0]]
    for value in values[1:]:
        out.append(value * k + out[-1] * (1 - k))
    return out


def _macd(closes: list[float], fast: int = 12, slow: int = 26, signal_span: int = 9) -> tuple[float, float, float] | None:
    """MACD(12, 26, 9) with each EMA seeded by its first value: (macd, signal, histogram)."""
    if len(closes) < slow:
        return None
    fast_ema = _ema_series(closes, fast)
    slow_ema = _ema_series(closes, slow)
    line = [f - s for f, s in zip(fast_ema, slow_ema)]
    signal = _ema_series(line, signal_span)
    return line[-1], signal[-1], line[-1] - signal[-1]


def _annualized_volatility(closes: list[float], window: int = 30) -> float | None:
    """Annualized volatility in percent: sample standard deviation of the last `window`
    daily log returns, times sqrt(252)."""
    import math

    if len(closes) < window + 1:
        return None
    tail = closes[-(window + 1):]
    returns = [math.log(tail[i + 1] / tail[i]) for i in range(window)]
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return math.sqrt(variance * 252) * 100


@yfinance_server.tool(
    name="get_technical_indicators",
    description="""Get pre-computed technical / momentum indicators for one or more tickers.

Computes indicators server-side from completed daily closes only. An unfinished
active-session bar is excluded. Read priceBasis, dataDate, freshnessStatus, and
recommendedNextAction; STALE_BAR publishes no indicator values.

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
        A longer list is rejected with BATCH_LIMIT_EXCEEDED; split larger lists into multiple calls.
    period: str
        History fed to the indicators (default "1y"). Periods shorter than a year
        are widened to "1y" so RSI and MACD settle. Valid: 1y, 2y, 5y.
""",
)
async def get_technical_indicators(ticker: str | list[str], period: str = "1y") -> str:
    """Get pre-computed technical indicators (RSI, MACD) for one or more tickers."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, lambda t: get_technical_indicators(t, period))
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    lookback = _indicator_lookback(period)
    cache_key = f"tech_indicators:{ticker}:{lookback}"
    cached = _tool_cache.get_value(cache_key)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        prepared = await _load_completed_daily_history(
            company,
            lookback,
            required_bars=26,
            retry_adjusted_gap=True,
        )
    except Exception as e:
        print(f"Error: getting technical indicators for {ticker}: {e}", file=sys.stderr)
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    completed = prepared["completed"]
    timezone_name = prepared["timezoneName"]
    data_date = (
        _daily_bar_date(completed.index[-1], timezone_name)
        if not completed.empty
        else None
    )
    if prepared["freshnessStatus"] == "STALE":
        return json.dumps({
            "ticker": ticker,
            "status": "STALE_BAR",
            "rsi14": None,
            "macd": None,
            "macdSignal": None,
            "macdHistogram": None,
            "lastClose": None,
            "priceBasis": None,
            "observationType": "COMPLETED_DAILY_PRICE_SERIES",
            "barStatus": "STALE",
            "freshnessStatus": "STALE",
            "expectedCompletedDate": prepared["expectedCompletedDate"],
            "latestAvailableBarDate": data_date,
            "excludedIncompleteBar": prepared["excludedIncompleteBar"],
            "retryAttempted": prepared["retryAttempted"],
            "fallbackUsed": False,
            "dataDate": data_date,
            "recommendedNextAction": "RETRY",
        })

    selected = _select_completed_close_series(completed)
    if selected is None:
        return json.dumps({
            "error": True,
            "message": f"Incomplete completed-close series for {ticker}",
            "ticker": ticker,
        })
    closes, price_basis, fallback_used = selected
    if len(closes) < 26:
        return (
            f"Error: insufficient price history for {ticker} "
            f"(need ≥26 data points, got {len(closes)})"
        )

    output: dict = {
        "ticker": ticker,
        "status": "OK",
        "priceBasis": price_basis,
        "observationType": "COMPLETED_DAILY_PRICE_SERIES",
        "barStatus": "COMPLETE",
        "freshnessStatus": prepared["freshnessStatus"],
        "expectedCompletedDate": prepared["expectedCompletedDate"],
        "latestAvailableBarDate": data_date,
        "excludedIncompleteBar": prepared["excludedIncompleteBar"],
        "retryAttempted": prepared["retryAttempted"],
        "fallbackUsed": fallback_used,
        "recommendedNextAction": "NONE",
    }

    values = [float(v) for v in closes]
    rsi = _wilder_rsi(values)
    output["rsi14"] = None if rsi is None else round(rsi, 2)
    macd_values = _macd(values)
    output["macd"] = None if macd_values is None else round(macd_values[0], 4)
    output["macdSignal"] = None if macd_values is None else round(macd_values[1], 4)
    output["macdHistogram"] = None if macd_values is None else round(macd_values[2], 4)
    output["lookbackPeriod"] = lookback
    output["lookbackBars"] = len(values)

    output["lastClose"] = round(float(closes.iloc[-1]), 2)
    output["dataDate"] = data_date
    with_notices(output, freshness_notices(ticker, prepared.get("freshnessReason")))

    result = json.dumps(output)
    _tool_cache.set(cache_key, result, _PRICE_TTL)
    return result


# ---------------------------------------------------------------------------
# Tool: get_price_slope

# ---------------------------------------------------------------------------

def _price_slope_period(days: int) -> str:
    if days <= 20:
        return "1mo"
    if days <= 60:
        return "3mo"
    if days <= 120:
        return "6mo"
    if days <= 250:
        return "1y"
    return "2y"


def _daily_bar_epoch(value) -> int | None:
    if isinstance(value, (int, float)) and not pd.isna(value):
        return int(value)
    if isinstance(value, (pd.Timestamp, datetime.datetime)):
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        return int(timestamp.timestamp())
    return None


def _daily_bar_date(value, timezone_name: str) -> str | None:
    if value is None:
        return None
    try:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize(timezone_name)
        else:
            timestamp = timestamp.tz_convert(timezone_name)
        return timestamp.date().isoformat()
    except (TypeError, ValueError, zoneinfo.ZoneInfoNotFoundError):
        return None


def _prepare_completed_daily_history(
    metadata: dict,
    hist: pd.DataFrame,
    now_epoch: int,
) -> dict:
    timezone_name = str(metadata.get("exchangeTimezoneName") or "UTC")
    current_period = metadata.get("currentTradingPeriod")
    regular = current_period.get("regular", {}) if isinstance(current_period, dict) else {}
    regular_start = _daily_bar_epoch(regular.get("start")) if isinstance(regular, dict) else None
    regular_end = _daily_bar_epoch(regular.get("end")) if isinstance(regular, dict) else None
    active_session = (
        regular_start is not None
        and regular_end is not None
        and regular_start <= now_epoch < regular_end
    )

    completed = hist.copy()
    active_row = None
    excluded_incomplete = False
    if active_session and not completed.empty and regular_start is not None:
        session_date = _daily_bar_date(
            pd.Timestamp(regular_start, unit="s", tz="UTC"),
            timezone_name,
        )
        last_date = _daily_bar_date(completed.index[-1], timezone_name)
        if session_date is not None and last_date == session_date:
            active_row = completed.iloc[-1].copy()
            completed = completed.iloc[:-1]
            excluded_incomplete = True

    # A finished session row is not usable price evidence until at least one
    # close series is populated. Yahoo can briefly return a daily row with
    # volume/OHLC values but null Close and Adj Close. Trim only trailing
    # close-incomplete rows; internal gaps stay in the series so selectors fail
    # closed instead of silently bridging missing observations.
    while not completed.empty:
        raw_close = (
            pd.to_numeric(completed["Close"].iloc[-1], errors="coerce")
            if "Close" in completed
            else float("nan")
        )
        adjusted_close = (
            pd.to_numeric(completed["Adj Close"].iloc[-1], errors="coerce")
            if "Adj Close" in completed
            else float("nan")
        )
        if not pd.isna(raw_close) or not pd.isna(adjusted_close):
            break
        completed = completed.iloc[:-1]
        excluded_incomplete = True

    last_completed_date = _daily_bar_date(completed.index[-1], timezone_name) if not completed.empty else None
    last_completed_raw_close = (
        pd.to_numeric(completed["Close"].iloc[-1], errors="coerce")
        if not completed.empty and "Close" in completed
        else float("nan")
    )

    # expectedCompletedDate is the most recent session that should already be
    # complete. Outside a regular session that is the session of
    # regularMarketTime. During one it is the previous session, which is only
    # named when proven: Yahoo's previousClose equals the last completed close,
    # or (without previousClose) the exchange's holiday calendar names it.
    # Without a calendar it is never inferred from weekday arithmetic: the
    # status is UNKNOWN with SESSION_CALENDAR_UNKNOWN (BR-PRC-002, 3.1.0).
    expected_date = None
    freshness_reason = None
    if not active_session:
        regular_market_time = _daily_bar_epoch(metadata.get("regularMarketTime"))
        if regular_market_time is not None:
            expected_date = _daily_bar_date(
                pd.Timestamp(regular_market_time, unit="s", tz="UTC"),
                timezone_name,
            )
        if expected_date is None or last_completed_date is None:
            freshness_status = "UNKNOWN"
        else:
            freshness_status = "STALE" if last_completed_date < expected_date else "CURRENT"
    else:
        previous_close = pd.to_numeric(metadata.get("previousClose"), errors="coerce")
        session_date = _daily_bar_date(pd.Timestamp(regular_start, unit="s", tz="UTC"), timezone_name)
        if not pd.isna(previous_close) and not pd.isna(last_completed_raw_close):
            tolerance = max(0.01, abs(float(previous_close)) * 1e-6)
            matches = abs(float(last_completed_raw_close) - float(previous_close)) <= tolerance
            freshness_status = "CURRENT" if matches else "STALE"
            expected_date = last_completed_date if matches else None
        else:
            previous_session = (
                previous_exchange_session(metadata.get("exchangeName"), session_date)
                if session_date is not None
                else None
            )
            if previous_session is None:
                freshness_status = "UNKNOWN"
                freshness_reason = SESSION_CALENDAR_UNKNOWN if session_date is not None else None
            elif last_completed_date is None or last_completed_date > previous_session:
                freshness_status = "UNKNOWN"
            else:
                freshness_status = "CURRENT" if last_completed_date == previous_session else "STALE"
                expected_date = previous_session

    return {
        "history": hist,
        "completed": completed,
        "activeRow": active_row,
        "activeSession": active_session,
        "excludedIncompleteBar": excluded_incomplete,
        "expectedCompletedDate": expected_date,
        "freshnessStatus": freshness_status,
        "freshnessReason": freshness_reason,
        "timezoneName": timezone_name,
    }


def _completed_adjusted_window_incomplete(
    hist: pd.DataFrame,
    required_bars: int | None = None,
) -> bool:
    if hist.empty or "Adj Close" not in hist or "Close" not in hist:
        return False
    window = hist if required_bars is None else hist.tail(required_bars)
    adjusted = pd.to_numeric(window["Adj Close"], errors="coerce")
    raw_close = pd.to_numeric(window["Close"], errors="coerce")
    return adjusted.notna().any() and adjusted.isna().any() and raw_close.notna().all()


def _select_completed_close_series(
    hist: pd.DataFrame,
) -> tuple[pd.Series, str, bool] | None:
    if hist.empty:
        return None
    adjusted = (
        pd.to_numeric(hist["Adj Close"], errors="coerce")
        if "Adj Close" in hist
        else pd.Series(dtype=float)
    )
    raw_closes = (
        pd.to_numeric(hist["Close"], errors="coerce")
        if "Close" in hist
        else pd.Series(dtype=float)
    )
    if len(adjusted) == len(hist) and adjusted.notna().all():
        return adjusted, "ADJUSTED_CLOSE", False
    if len(raw_closes) == len(hist) and raw_closes.notna().all():
        return raw_closes, "UNADJUSTED_CLOSE", True
    return None


async def _load_completed_daily_history(
    company,
    period: str,
    *,
    required_bars: int | None = None,
    retry_adjusted_gap: bool = False,
    retry_missing_latest_volume: bool = False,
    retry_missing_latest_raw_close: bool = False,
) -> dict:
    async def _load_history() -> pd.DataFrame:
        return await asyncio.to_thread(
            company.history,
            period=period,
            interval="1d",
            auto_adjust=False,
            keepna=True,
        )

    async def _load_metadata() -> dict:
        try:
            metadata = await asyncio.to_thread(company.get_history_metadata)
            return metadata if isinstance(metadata, dict) else {}
        except Exception:
            return {}

    hist = await _load_history()
    metadata = await _load_metadata()
    if hist is None:
        hist = pd.DataFrame()
    now_epoch = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    prepared = _prepare_completed_daily_history(metadata, hist, now_epoch)
    retry_attempted = False
    adjusted_gap = (
        retry_adjusted_gap
        and _completed_adjusted_window_incomplete(
            prepared["completed"],
            required_bars,
        )
    )
    latest_completed = (
        prepared["completed"].iloc[-1]
        if not prepared["completed"].empty
        else None
    )
    missing_latest_volume = (
        retry_missing_latest_volume
        and (
            latest_completed is None
            or "Volume" not in prepared["completed"]
            or pd.isna(pd.to_numeric(latest_completed.get("Volume"), errors="coerce"))
        )
    )
    missing_latest_raw_close = (
        retry_missing_latest_raw_close
        and (
            latest_completed is None
            or "Close" not in prepared["completed"]
            or pd.isna(pd.to_numeric(latest_completed.get("Close"), errors="coerce"))
        )
    )
    if (
        prepared["freshnessStatus"] == "STALE"
        or adjusted_gap
        or missing_latest_volume
        or missing_latest_raw_close
    ):
        retry_attempted = True
        try:
            hist = await _load_history()
            metadata = await _load_metadata()
            retried = _prepare_completed_daily_history(metadata, hist, now_epoch)
            retried["excludedIncompleteBar"] = (
                prepared["excludedIncompleteBar"]
                or retried["excludedIncompleteBar"]
            )
            prepared = retried
        except Exception:
            pass
    prepared["metadata"] = metadata
    prepared["retryAttempted"] = retry_attempted
    return prepared


# A CAGR window counts as covered when the history's first bar is no more than this many calendar days after the
# window's start date: the 5y fetch starts about five years before today, not before the last completed bar, and the
# first bar after that start can follow a weekend and a holiday (R5-pricing-2, 3.1.0).
_HISTORY_START_TOLERANCE_DAYS = 10


def _history_covers(first_bar_date: str | None, cutoff_date: str) -> bool:
    """Whether a history whose first bar is on first_bar_date reaches back to a window starting on cutoff_date."""
    if first_bar_date is None:
        return False
    limit = datetime.date.fromisoformat(cutoff_date) + datetime.timedelta(days=_HISTORY_START_TOLERANCE_DAYS)
    return first_bar_date <= limit.isoformat()


_SLOPE_DAYS_MESSAGE = "days must be a whole number of sessions"


def _session_count(value) -> int | float | None | str:
    """A session count: None when absent, the number when one is given, digits parsed, "invalid" otherwise (the
    Worker's sessionCount)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "invalid"
    if isinstance(value, (int, float)):
        return value if math.isfinite(value) else "invalid"
    if isinstance(value, str) and re.fullmatch(r"\s*-?\d+\s*", value):
        return int(value)
    return "invalid"


@yfinance_server.tool(
    name="get_price_slope",
    description="""Get completed-session N-day close-to-close price change and direction for one or more tickers. Pre-computed server-side.

days=N uses N+1 completed daily bars. An unfinished current-session bar is
excluded. Returns startClose, endClose, endRawClose, priceBasis,
calculationBasis, observationCount, barStatus, freshnessStatus, slopePct,
direction (UP/DOWN/FLAT), and dataDate.

startClose/endClose use adjusted daily closes when Yahoo provides them.
endRawClose is the unadjusted close from the same completed dated bar. A stale
or incomplete adjusted response is retried once; persistent adjusted gaps use
one consistent unadjusted series rather than silently dropping the latest bar.

Args:
    ticker: str | list[str] — single or batch
    days: int — completed close-to-close trading-session intervals (default: 5)
""",
)
async def get_price_slope(ticker: str | list[str], days: int = 5) -> str:
    """Return completed-session N-day price change for one or more tickers."""
    # A whole number of sessions, as a number or digits; anything else is refused rather than raising, the same in
    # both runtimes (R5-pricing-1, Q3b).
    parsed_days = _session_count(days)
    if parsed_days == "invalid":
        return _mcp_failure("get_price_slope", ErrorCode.INPUT_VALIDATION_ERROR, _SLOPE_DAYS_MESSAGE)
    days = 5 if parsed_days is None else parsed_days
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, lambda t: get_price_slope(t, days))
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})

    normalized_days = max(1, min(int(days), 500))
    required_bars = normalized_days + 1
    period = _price_slope_period(normalized_days)
    company = yf.Ticker(ticker)
    try:
        prepared = await _load_completed_daily_history(
            company,
            period,
            required_bars=required_bars,
            retry_adjusted_gap=True,
        )
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    completed = prepared["completed"]
    if completed.empty:
        return json.dumps({"error": True, "message": f"Insufficient price data for {ticker}", "ticker": ticker})

    excluded_incomplete = prepared["excludedIncompleteBar"]
    expected_date = prepared["expectedCompletedDate"]
    freshness_status = prepared["freshnessStatus"]
    retry_attempted = prepared["retryAttempted"]
    timezone_name = prepared["timezoneName"]
    latest_available_date = (
        _daily_bar_date(completed.index[-1], timezone_name)
        if not completed.empty
        else None
    )
    if freshness_status == "STALE":
        return json.dumps({
            "ticker": ticker,
            "days": normalized_days,
            "status": "STALE_BAR",
            "startClose": None,
            "endClose": None,
            "endRawClose": None,
            "priceBasis": None,
            "observationType": "DAILY_PRICE_BAR",
            "calculationBasis": "COMPLETED_SESSION_CLOSE_TO_CLOSE",
            "observationCount": 0,
            "barStatus": "STALE",
            "freshnessStatus": "STALE",
            "expectedCompletedDate": expected_date,
            "latestAvailableBarDate": latest_available_date,
            "excludedIncompleteBar": excluded_incomplete,
            "retryAttempted": retry_attempted,
            "fallbackUsed": False,
            "slopePct": None,
            "direction": "UNKNOWN",
            "dataDate": latest_available_date,
            "recommendedNextAction": "RETRY",
        })

    if len(completed) < required_bars:
        return json.dumps({
            "error": True,
            "message": f"Insufficient completed price data for {ticker}: need {required_bars} bars",
            "ticker": ticker,
        })

    window = completed.tail(required_bars)
    selected = _select_completed_close_series(window)
    if selected is None:
        return json.dumps({
            "error": True,
            "message": f"Incomplete completed-close series for {ticker}",
            "ticker": ticker,
        })
    closes, price_basis, fallback_used = selected

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

    last_idx = window.index[-1]
    data_date = str(last_idx.date()) if hasattr(last_idx, "date") else str(last_idx)
    raw_close = window.loc[last_idx, "Close"] if "Close" in window and last_idx in window.index else None

    return json.dumps(with_notices({
        "ticker": ticker,
        "days": normalized_days,
        "status": "OK",
        "startClose": round(start_close, 2),
        "endClose": round(end_close, 2),
        "endRawClose": round(float(raw_close), 2) if raw_close is not None and not pd.isna(raw_close) else None,
        "priceBasis": price_basis,
        "observationType": "DAILY_PRICE_BAR",
        "calculationBasis": "COMPLETED_SESSION_CLOSE_TO_CLOSE",
        "observationCount": required_bars,
        "barStatus": "COMPLETE",
        "freshnessStatus": freshness_status,
        "expectedCompletedDate": expected_date,
        "latestAvailableBarDate": latest_available_date,
        "excludedIncompleteBar": excluded_incomplete,
        "retryAttempted": retry_attempted,
        "fallbackUsed": fallback_used,
        "slopePct": slope_pct,
        "direction": direction,
        "dataDate": data_date,
        "recommendedNextAction": "NONE",
    }, freshness_notices(ticker, prepared.get("freshnessReason"))))


# ---------------------------------------------------------------------------
# Tool: get_volume_ratio

# ---------------------------------------------------------------------------

async def get_volume_ratio(ticker: str | list[str], period: int = 10) -> str:
    """Return volume ratio for one or more tickers."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, lambda t: get_volume_ratio(t, period))
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})

    company = yf.Ticker(ticker)
    try:
        prepared = await _load_completed_daily_history(
            company,
            "6mo",
            retry_missing_latest_volume=True,
        )
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    completed = prepared["completed"]
    timezone_name = prepared["timezoneName"]
    data_date = (
        _daily_bar_date(completed.index[-1], timezone_name)
        if not completed.empty
        else None
    )
    if prepared["freshnessStatus"] == "STALE":
        return json.dumps({
            "ticker": ticker,
            "status": "STALE_BAR",
            "lastVolume": None,
            "avgVolume10d": None,
            "avgVolume90d": None,
            "ratio10d": None,
            "ratio90d": None,
            "volumeFlag": None,
            "observationType": "COMPLETED_DAILY_VOLUME",
            "barStatus": "STALE",
            "freshnessStatus": "STALE",
            "expectedCompletedDate": prepared["expectedCompletedDate"],
            "latestAvailableBarDate": data_date,
            "excludedIncompleteBar": prepared["excludedIncompleteBar"],
            "retryAttempted": prepared["retryAttempted"],
            "dataDate": data_date,
            "recommendedNextAction": "RETRY",
        })

    volumes = (
        pd.to_numeric(completed["Volume"], errors="coerce")
        if "Volume" in completed
        else pd.Series(dtype=float)
    )
    if volumes.empty or pd.isna(volumes.iloc[-1]):
        return json.dumps(with_notices({
            "ticker": ticker,
            "status": "PARTIAL",
            "lastVolume": None,
            "avgVolume10d": None,
            "avgVolume90d": None,
            "ratio10d": None,
            "ratio90d": None,
            "volumeFlag": None,
            "observationType": "COMPLETED_DAILY_VOLUME",
            "barStatus": "INCOMPLETE",
            "freshnessStatus": prepared["freshnessStatus"],
            "expectedCompletedDate": prepared["expectedCompletedDate"],
            "latestAvailableBarDate": data_date,
            "excludedIncompleteBar": prepared["excludedIncompleteBar"],
            "retryAttempted": prepared["retryAttempted"],
            "dataDate": data_date,
            "recommendedNextAction": "RETRY",
        }, freshness_notices(ticker, prepared.get("freshnessReason"))))
    last_vol = int(volumes.iloc[-1])
    prior_volumes = volumes.iloc[:-1]
    avg_10d = (
        float(prior_volumes.tail(10).mean())
        if len(prior_volumes) >= 10 and prior_volumes.tail(10).notna().all()
        else None
    )
    avg_90d = (
        float(prior_volumes.tail(90).mean())
        if len(prior_volumes) >= 90 and prior_volumes.tail(90).notna().all()
        else None
    )

    # The ratio uses the unrounded average; only the published averages and ratios are rounded, half away from zero,
    # as in the Worker (R5-pricing-1, Q3c/Q3d).
    ratio_10d = _round_half_away_from_zero(last_vol / avg_10d, 3) if last_vol and avg_10d else None
    ratio_90d = _round_half_away_from_zero(last_vol / avg_90d, 3) if last_vol and avg_90d else None

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
    # A zero-volume session has no ratio and no flag, with an explicit notice (Q3c, R5-pricing-2, 3.1.0).
    volume_notices = [zero_volume_notice(ticker)] if last_vol == 0 else []

    return json.dumps(with_notices({
        "ticker": ticker,
        "status": "OK",
        "lastVolume": last_vol,
        "avgVolume10d": int(_round_half_away_from_zero(avg_10d, 0)) if avg_10d is not None else None,
        "avgVolume90d": int(_round_half_away_from_zero(avg_90d, 0)) if avg_90d is not None else None,
        "ratio10d": ratio_10d,
        "ratio90d": ratio_90d,
        "volumeFlag": volume_flag,
        "observationType": "COMPLETED_DAILY_VOLUME",
        "barStatus": "COMPLETE",
        "freshnessStatus": prepared["freshnessStatus"],
        "expectedCompletedDate": prepared["expectedCompletedDate"],
        "latestAvailableBarDate": data_date,
        "excludedIncompleteBar": prepared["excludedIncompleteBar"],
        "retryAttempted": prepared["retryAttempted"],
        "dataDate": data_date,
        "recommendedNextAction": "NONE",
    }, [*volume_notices, *freshness_notices(ticker, prepared.get("freshnessReason"))]))


# ---------------------------------------------------------------------------
# Tool: get_ma_position

# ---------------------------------------------------------------------------

async def get_ma_position(ticker: str | list[str]) -> str:
    """Return MA position for one or more tickers."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, get_ma_position)
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

    price_timestamp = None
    market_state = None
    market_open = None
    data_date = None
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
        "lastPrice": round(last_price, 2) if last_price else None,
        "priceBasis": "REGULAR_MARKET_PRICE",
        "observationType": "REGULAR_MARKET_QUOTE_VS_TRAILING_AVERAGES",
        "priceTimestamp": price_timestamp,
        "marketState": market_state,
        "marketOpen": market_open,
        "fiftyDayAverage": round(fifty_dma, 2) if fifty_dma else None,
        "twoHundredDayAverage": round(two_hundred_dma, 2) if two_hundred_dma else None,
        "pctVs50dma": pct_vs_50,
        "pctVs200dma": pct_vs_200,
        "regime50": regime_50,
        "regime200": regime_200,
        "trend": trend,
        "dataDate": data_date,
        "recommendedNextAction": "NONE",
    })


# ---------------------------------------------------------------------------
# Tool: get_credit_health

# ---------------------------------------------------------------------------
@yfinance_server.tool(
    name="get_short_momentum",
    description="""Get short interest with pre-computed momentum: MoM delta, direction, squeeze risk, and flag. dataDate is the short-interest observation date, not today's market date.

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of up to 5 symbols.
        When a list is provided, returns a dict keyed by symbol.
        A longer list is rejected with BATCH_LIMIT_EXCEEDED; split larger lists into multiple calls.
""",
)
async def get_short_momentum(ticker: str | list[str]) -> str:
    """Return short interest momentum for one or more tickers."""
    if isinstance(ticker, list):
        results = await _run_ticker_batch(ticker, get_short_momentum)
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

    def _observation_date(v):
        if v is None:
            return None
        if isinstance(v, datetime.datetime):
            return v.date().isoformat()
        if isinstance(v, datetime.date):
            return v.isoformat()
        if isinstance(v, (int, float)):
            timestamp = float(v)
            if timestamp > 1_000_000_000_000:
                timestamp /= 1000
            return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc).date().isoformat()
        parsed = pd.to_datetime(v, utc=True, errors="coerce")
        return None if pd.isna(parsed) else parsed.date().isoformat()

    observation_date = _observation_date(date_short)

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
        "dateShortInterest": observation_date,
        "dataDate": observation_date,
        "dataDateBasis": "SHORT_INTEREST_OBSERVATION" if observation_date else "UNAVAILABLE",
    })


# ---------------------------------------------------------------------------
# CR-12 — get_options_flow_scan

# ---------------------------------------------------------------------------

def _usd_compact(value: float) -> str:
    """$14.22B above a billion, else $12.3M (the Worker's usdCompact)."""
    return f"${value / 1e9:.2f}B" if value >= 1e9 else f"${value / 1e6:.1f}M"


# Listings quoted in a currency's minor unit: (ISO currency, minor units per unit).
_MINOR_UNIT_CURRENCIES = {"GBp": ("GBP", 100), "GBX": ("GBP", 100), "ZAc": ("ZAR", 100), "ILA": ("ILS", 100)}


def _listing_currency_unit(currency: str) -> tuple[str, int]:
    """The ISO currency behind a listing currency and how many listing units make one of it."""
    return _MINOR_UNIT_CURRENCIES.get(currency, (currency.upper(), 1))


def _average_traded_value(rows: list[tuple[float | None, float | None]], count: int) -> float | None:
    """Mean daily volume x close over exactly `count` sessions, or None when any is missing."""
    if len(rows) != count or any(v is None or c is None for v, c in rows):
        return None
    return sum(v * c for v, c in rows) / count


def _usd_conversion(currency: str | None) -> dict:
    """Listing-currency units per US dollar, from Yahoo's <ISO>=X quote (ISO units per USD)."""
    if not currency:
        return {"ok": False, "note": "listing currency unknown"}
    iso, per_unit = _listing_currency_unit(currency)
    if iso == "USD":
        return {"ok": True, "localPerUsd": per_unit, "fxRate": 1.0, "fxCurrency": "USD", "fxPriceTimestamp": None,
                "note": "" if per_unit == 1 else f" [{currency} = USD/{per_unit}]"}
    try:
        fx_ticker = yf.Ticker(f"{iso}=X")
        rate = fx_ticker.fast_info.last_price
        if not rate or rate <= 0:
            return {"ok": False, "note": f"{iso}=X rate unavailable"}
        fx_price_timestamp = None
        try:
            fx_time = fx_ticker.info.get("regularMarketTime")
            if isinstance(fx_time, (int, float)) and fx_time:
                fx_price_timestamp = datetime.datetime.fromtimestamp(fx_time, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            pass
        unit_note = "" if per_unit == 1 else f", {per_unit} {currency} per {iso}"
        return {"ok": True, "localPerUsd": float(rate) * per_unit, "fxRate": round(float(rate), 4), "fxCurrency": iso,
                "fxPriceTimestamp": fx_price_timestamp, "note": f" [{currency}\u2192USD at {float(rate):.4f} {iso}/USD{unit_note}]"}
    except Exception:
        return {"ok": False, "note": f"{iso}=X fetch failed"}


async def get_volume_gate(ticker: str, foreign_exchange: bool = False, min_adv_usd: float | None = None) -> str:
    """Return volume liquidity threshold assessment. ``min_adv_usd`` is the caller's already-validated floor."""
    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        currency: str | None = fi["currency"]
        prepared = await _load_completed_daily_history(
            company,
            "6mo",
            retry_missing_latest_volume=True,
            retry_missing_latest_raw_close=True,
        )
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    completed = prepared["completed"]
    timezone_name = prepared["timezoneName"]
    data_date = (
        _daily_bar_date(completed.index[-1], timezone_name)
        if not completed.empty
        else None
    )
    if prepared["freshnessStatus"] == "STALE":
        return json.dumps({
            "ticker": ticker,
            "status": "STALE_BAR",
            "currency": currency,
            "lastVolume": None,
            "lastClose": None,
            "priceBasis": "UNADJUSTED_CLOSE",
            "adv10d": None,
            "adv20d": None,
            "adv90d": None,
            "ratio20d": None,
            "fxRate": None,
            "fxPriceTimestamp": None,
            "notionalUsd": None,
            "meetsMinimum": None,
            "minAdvUsd": min_adv_usd,
            "observationType": "COMPLETED_DAILY_VOLUME_NOTIONAL",
            "barStatus": "STALE",
            "freshnessStatus": "STALE",
            "expectedCompletedDate": prepared["expectedCompletedDate"],
            "latestAvailableBarDate": data_date,
            "excludedIncompleteBar": prepared["excludedIncompleteBar"],
            "retryAttempted": prepared["retryAttempted"],
            "dataDate": data_date,
            "note": "Volume gate UNKNOWN — completed daily bars are stale",
            "recommendedNextAction": "RETRY",
        })

    volumes = (
        pd.to_numeric(completed["Volume"], errors="coerce")
        if "Volume" in completed
        else pd.Series(dtype=float)
    )
    closes = (
        pd.to_numeric(completed["Close"], errors="coerce")
        if "Close" in completed
        else pd.Series(dtype=float)
    )
    last_volume = (
        int(volumes.iloc[-1])
        if not volumes.empty and not pd.isna(volumes.iloc[-1])
        else None
    )
    last_price = (
        float(closes.iloc[-1])
        if not closes.empty and not pd.isna(closes.iloc[-1])
        else None
    )
    if last_volume is None or last_price is None:
        return json.dumps(with_notices({
            "ticker": ticker,
            "status": "PARTIAL",
            "currency": currency,
            "lastVolume": None,
            "lastClose": None,
            "priceBasis": "UNADJUSTED_CLOSE",
            "adv10d": None,
            "adv20d": None,
            "adv90d": None,
            "ratio20d": None,
            "fxRate": None,
            "fxPriceTimestamp": None,
            "notionalUsd": None,
            "meetsMinimum": None,
            "minAdvUsd": min_adv_usd,
            "observationType": "COMPLETED_DAILY_VOLUME_NOTIONAL",
            "barStatus": "INCOMPLETE",
            "freshnessStatus": prepared["freshnessStatus"],
            "expectedCompletedDate": prepared["expectedCompletedDate"],
            "latestAvailableBarDate": data_date,
            "excludedIncompleteBar": prepared["excludedIncompleteBar"],
            "retryAttempted": prepared["retryAttempted"],
            "dataDate": data_date,
            "note": "Volume gate UNKNOWN — latest completed session lacks price or volume",
            "recommendedNextAction": "RETRY",
        }, freshness_notices(ticker, prepared.get("freshnessReason"))))
    prior_volumes = volumes.iloc[:-1]

    def _prior_average(count: int) -> float | None:
        window = prior_volumes.tail(count)
        return (
            round(float(window.mean()))
            if len(window) == count and window.notna().all()
            else None
        )

    adv10d = _prior_average(10)
    adv20d = _prior_average(20)
    adv90d = _prior_average(90)

    # Gate: average daily traded value of the 20 prior sessions, in USD.
    ratio20d: float | None = None
    daily_notional_usd: float | None = None
    adv20d_traded_value_usd: float | None = None
    recommended_next_action = "NONE"
    if adv20d and adv20d > 0:
        ratio20d = round(last_volume / adv20d, 2)
    prior_closes = closes.iloc[:-1]
    window_values = [
        (None if pd.isna(v) else float(v), None if pd.isna(c) else float(c))
        for v, c in zip(prior_volumes.tail(20), prior_closes.tail(20))
    ]
    adv20d_traded_value = _average_traded_value(window_values, 20)
    fx = _usd_conversion(currency)
    if adv20d_traded_value is None:
        recommended_next_action = "RETRY"
        note = "Volume gate UNKNOWN — fewer than 20 prior sessions with volume and close"
    elif not fx["ok"]:
        recommended_next_action = "RETRY"
        note = f"Volume gate UNKNOWN — {fx['note']}"
    else:
        adv20d_traded_value_usd = round(adv20d_traded_value / fx["localPerUsd"], 2)
        daily_notional_usd = last_volume * last_price / fx["localPerUsd"]
        meets = meets_minimum(adv20d_traded_value_usd, min_adv_usd)
        note = (
            f"20d average traded value {_usd_compact(adv20d_traded_value_usd)}"
            + (f" ({'≥' if meets else '<'} min_adv_usd {_usd_compact(min_adv_usd)})" if meets is not None and min_adv_usd is not None else "")
            + (f"; latest session {ratio20d:.2f}x 20d ADV" if ratio20d is not None else "")
            + fx["note"]
        )
    del foreign_exchange  # accepted for compatibility: non-USD listings are always converted
    fx_rate = fx.get("fxRate") if fx["ok"] else None
    fx_price_timestamp = fx.get("fxPriceTimestamp") if fx["ok"] else None

    return json.dumps(with_notices({
        "ticker": ticker,
        "status": "OK",
        "currency": currency,
        "lastVolume": last_volume,
        "lastClose": round(last_price, 4) if last_price is not None else None,
        "priceBasis": "UNADJUSTED_CLOSE",
        "adv10d": adv10d,
        "adv20d": adv20d,
        "adv90d": adv90d,
        "ratio20d": ratio20d,
        "fxRate": fx_rate,
        "fxCurrency": fx.get("fxCurrency") if fx["ok"] else None,
        "fxPriceTimestamp": fx_price_timestamp,
        "notionalUsd": round(daily_notional_usd, 2) if daily_notional_usd is not None else None,
        "adv20dTradedValueUsd": adv20d_traded_value_usd,
        "gateBasis": "ADV20_TRADED_VALUE_USD",
        "meetsMinimum": meets_minimum(adv20d_traded_value_usd, min_adv_usd),
        "minAdvUsd": min_adv_usd,
        "observationType": "COMPLETED_DAILY_VOLUME_NOTIONAL",
        "barStatus": "COMPLETE",
        "freshnessStatus": prepared["freshnessStatus"],
        "expectedCompletedDate": prepared["expectedCompletedDate"],
        "latestAvailableBarDate": data_date,
        "excludedIncompleteBar": prepared["excludedIncompleteBar"],
        "retryAttempted": prepared["retryAttempted"],
        "dataDate": data_date,
        "note": note,
        "recommendedNextAction": recommended_next_action,
    }, freshness_notices(ticker, prepared.get("freshnessReason"))))

def _classify_quote_freshness(
    quote_timestamp: str | None,
    retrieved_at: str,
    market_open: bool | None,
) -> str:
    """Classify quote freshness from its timestamp and current session state."""
    if not quote_timestamp:
        return "UNKNOWN"
    try:
        now = datetime.datetime.fromisoformat(retrieved_at.replace("Z", "+00:00"))
        observed = datetime.datetime.fromisoformat(
            quote_timestamp.replace("Z", "+00:00")
        )
        if now.tzinfo is None:
            now = now.replace(tzinfo=datetime.timezone.utc)
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=datetime.timezone.utc)
        now = now.astimezone(datetime.timezone.utc)
        observed = observed.astimezone(datetime.timezone.utc)
        diff_seconds = (now - observed).total_seconds()
        # Allow small provider/host clock skew, but reject genuinely future quotes.
        if diff_seconds < -5 * 60:
            return "UNKNOWN"
        diff_hours = diff_seconds / (60 * 60)
        if diff_hours <= 0.5:
            return "FRESH"
        if market_open is True:
            return "STALE"
        if (
            now.weekday() in (5, 6)
            and observed.weekday() == 4
            and diff_hours <= 96
        ):
            return "WEEKEND_EXPECTED_STALE"
        if diff_hours <= 96:
            return "MARKET_CLOSED_EXPECTED_STALE"
        if diff_hours <= 168:
            return "STALE"
        return "VERY_STALE"
    except Exception:
        return "UNKNOWN"


_SNAPSHOT_NOT_FOUND_MESSAGE = "No provider data was found for the requested ticker."


def _snapshot_component_message(message: str) -> str:
    """The Worker's snapshotComponentMessage (2.26.5, F56): the message without provider URLs."""
    cleaned = re.sub(r"\s+", " ", re.sub(r"https?://\S+", " ", message[:300], flags=re.I)).strip()
    cleaned = re.sub(r"[\s:]+$", "", cleaned)
    return cleaned or "error in response"


def _snapshot_ticker_not_found(message: str) -> bool:
    """The Worker's snapshotTickerNotFound (2.26.5, F56): a quote failure that says the ticker does not exist."""
    return re.search(r"not found|no data found|no price data found|api error 404|ticker_not_found", message, re.I) is not None


@yfinance_server.tool(name="get_market_snapshot", description="Compact market-state packet with separate live-quote and completed-bar dates, component status, price/range, moving-average trend, completed-session volume/liquidity, and technical indicators. Supports compact/full modes and ticker batches. Pass min_adv_usd for a caller-chosen threshold; meetsMinimum reports adv20dTradedValueUsd >= min_adv_usd.")
async def get_market_snapshot(
    ticker: str | list[str],
    mode: str = "compact",
    foreign_exchange: bool = False,
    min_adv_usd: Annotated[float | None, Field(description=MIN_ADV_USD_DESCRIPTION)] = None,
) -> str:
    """Return a compact or full market-state snapshot for one or more tickers."""
    min_adv_usd, min_adv_error = parse_min_adv_usd(min_adv_usd)
    if min_adv_error:
        return _mcp_failure("get_market_snapshot", ErrorCode.INPUT_VALIDATION_ERROR, min_adv_error)
    if isinstance(ticker, list):
        # The per-mode caps (tickerCaps get_market_snapshot / get_market_snapshot.full) are enforced by the tool-call gate.
        outcomes = await _run_ticker_batch(ticker, lambda t: get_market_snapshot(t, mode, foreign_exchange, min_adv_usd))
        results = {}
        for t, outcome in zip(ticker, outcomes):
            if isinstance(outcome, Exception):
                results[t] = {"error": True, "message": str(outcome)}
                continue
            try:
                parsed = json.loads(outcome)
            except Exception as e:
                results[t] = {"error": True, "message": str(e)}
                continue
            # An unknown ticker is TICKER_NOT_FOUND, not retried, as in the Worker (2.26.5, F56).
            error = parsed.get("error") if isinstance(parsed, dict) and parsed.get("ok") is False else None
            if isinstance(error, dict) and error.get("code") == ErrorCode.TICKER_NOT_FOUND:
                results[t] = {"error": True, "code": ErrorCode.TICKER_NOT_FOUND, "message": _SNAPSHOT_NOT_FOUND_MESSAGE, "retryable": False}
                continue
            results[t] = parsed
        return json.dumps({"tickers": results})

    retrieved_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat() + "Z"

    component_status: dict[str, str] = {}
    failed_components: list[str] = []
    limited_components: list[str] = []
    warnings_list: list[dict] = []

    component_results = await asyncio.gather(
        get_fast_info(ticker),
        get_price_stats(ticker),
        get_ma_position(ticker),
        get_volume_ratio(ticker, 10),
        get_volume_gate(ticker, foreign_exchange, min_adv_usd),
        get_technical_indicators(ticker, INDICATOR_MIN_LOOKBACK),
        return_exceptions=True,
    )

    names = ["quote", "priceStats", "maPosition", "volumeRatio", "volumeGate", "technicalIndicators"]

    # An unknown ticker fails as get_market_quote does, TICKER_NOT_FOUND and not retried, instead of a PARTIAL
    # snapshot that says RETRY (2.26.5, F56).
    quote_raw = component_results[0]
    quote_failure = None
    if isinstance(quote_raw, Exception):
        quote_failure = str(quote_raw)
    elif isinstance(quote_raw, str):
        try:
            quote_parsed = json.loads(quote_raw)
            error = quote_parsed.get("error") if isinstance(quote_parsed, dict) else None
            if isinstance(error, dict):
                # get_market_quote's failure envelope: its code says whether the ticker exists.
                quote_failure = "TICKER_NOT_FOUND" if error.get("code") == ErrorCode.TICKER_NOT_FOUND else str(error.get("message") or "")
            elif error:
                quote_failure = str(quote_parsed.get("message") or "")
        except Exception:
            quote_failure = quote_raw
    if quote_failure is not None and _snapshot_ticker_not_found(quote_failure):
        return _mcp_failure("get_market_snapshot", ErrorCode.TICKER_NOT_FOUND, _SNAPSHOT_NOT_FOUND_MESSAGE,
                            meta_extra={"retryable": False})

    def _parse_component(raw, name: str) -> dict | None:
        if isinstance(raw, Exception):
            component_status[name] = "FAILED"
            failed_components.append(name)
            warnings_list.append({"code": "COMPONENT_FAILED", "component": name, "message": _snapshot_component_message(str(raw))})
            return None
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, dict) and parsed.get("error"):
                component_status[name] = "FAILED"
                failed_components.append(name)
                warnings_list.append({"code": "COMPONENT_FAILED", "component": name,
                                      "message": _snapshot_component_message(str(parsed.get("message", "error in response")))})
                return None
            payload_status = parsed.get("status") if isinstance(parsed, dict) else None
            if payload_status in {"STALE_BAR", "PARTIAL"}:
                component_status[name] = str(payload_status)
                limited_components.append(name)
                warnings_list.append({
                    "code": "COMPONENT_LIMITED",
                    "component": name,
                    "message": f"{name} returned {payload_status}",
                })
            else:
                component_status[name] = "OK"
            # A component's notices join the snapshot's data.warnings with the component name (3.1.0).
            for notice in take_notices(parsed):
                warnings_list.append({"code": notice.get("code"), "component": name, "message": notice.get("message")})
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            # A component's plain-text error is its message, not the JSON parser's (2.26.5, F56).
            component_status[name] = "FAILED"
            failed_components.append(name)
            warnings_list.append({"code": "COMPONENT_FAILED", "component": name, "message": _snapshot_component_message(str(raw))})
            return None

    quote, price_stats, ma, volume_ratio, volume_gate, tech = (
        _parse_component(r, n) for r, n in zip(component_results, names)
    )

    data_date = (
        (quote.get("lastTradeDate") if quote else None)
        or (price_stats.get("dataDate") if price_stats else None)
    )
    completed_bar_data_date = (
        (tech.get("dataDate") if tech else None)
        or (price_stats.get("dataDate") if price_stats else None)
        or (volume_gate.get("dataDate") if volume_gate else None)
    )
    quote_timestamp = quote.get("priceTimestamp") if quote else None
    market_open = quote.get("marketOpen") if quote else None
    quote_freshness_class = _classify_quote_freshness(
        quote_timestamp,
        retrieved_at,
        market_open,
    )

    last_price = quote.get("lastPrice") if quote else None
    prev_close = quote.get("previousClose") if quote else None
    # A true 0.0 from priceStats is kept, as the Worker's `??` does; only a missing one falls back to the quote
    # (R5-pricing-1, BR-PRC-040).
    stats_change_pct = price_stats.get("pctChangeTodayVsPrevClose") if price_stats else None
    change_pct = (
        stats_change_pct
        if stats_change_pct is not None
        else (
            _round_half_away_from_zero((last_price - prev_close) / prev_close * 100, 2)
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
            "marketOpen": market_open,
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
            "lastVolumeObservationType": "REGULAR_SESSION_CUMULATIVE_VOLUME",
            "lastCompletedVolume": volume_ratio.get("lastVolume") if volume_ratio else None,
            "completedVolumeDataDate": volume_ratio.get("dataDate") if volume_ratio else None,
            "avgVolume10d": (
                volume_ratio.get("avgVolume10d")
                if volume_ratio and volume_ratio.get("avgVolume10d") is not None
                else (quote.get("tenDayAverageVolume") if quote else None)
            ),
            "avgVolume20d": volume_gate.get("adv20d") if volume_gate else None,
            "avgVolume90d": (
                volume_ratio.get("avgVolume90d")
                if volume_ratio and volume_ratio.get("avgVolume90d") is not None
                else (quote.get("threeMonthAverageVolume") if quote else None)
            ),
            "ratio10d": volume_ratio.get("ratio10d") if volume_ratio else None,
            "ratio20d": volume_gate.get("ratio20d") if volume_gate else None,
            "ratio90d": volume_ratio.get("ratio90d") if volume_ratio else None,
            "volumeFlag": volume_ratio.get("volumeFlag") if volume_ratio else None,
            "meetsMinimum": meets_minimum(volume_gate.get("adv20dTradedValueUsd") if volume_gate else None, min_adv_usd),
            "minAdvUsd": min_adv_usd,
        },
        "risk": {
            "annualizedVolatility30d": price_stats.get("annualizedVolatility30d") if price_stats else None,
        },
        "freshness": {
            "dataDate": data_date,
            "quoteDataDate": quote.get("lastTradeDate") if quote else None,
            "quoteTimestamp": quote_timestamp,
            "completedBarDataDate": completed_bar_data_date,
            "componentDataDates": {
                "priceStats": price_stats.get("dataDate") if price_stats else None,
                "maPosition": ma.get("dataDate") if ma else None,
                "volumeRatio": volume_ratio.get("dataDate") if volume_ratio else None,
                "volumeGate": volume_gate.get("dataDate") if volume_gate else None,
                "technicalIndicators": tech.get("dataDate") if tech else None,
            },
            "retrievedAt": retrieved_at,
            "marketSessionAware": True,
            "freshnessClass": quote_freshness_class,
            "quoteFreshnessClass": quote_freshness_class,
            "completedBarFreshnessStatus": (
                (tech.get("freshnessStatus") if tech else None)
                or (price_stats.get("freshnessStatus") if price_stats else None)
            ),
        },
        "status": "PARTIAL" if failed_components or limited_components else "OK",
        "componentStatus": component_status,
        "partialSuccess": (
            bool(failed_components or limited_components)
            and len(failed_components) < 6
        ),
        "failedComponents": failed_components,
        "limitedComponents": limited_components,
        "warnings": warnings_list,
        "recommendedNextAction": "RETRY" if failed_components or limited_components else "NONE",
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


# ---------------------------------------------------------------------------
# Phase 5 unit 3: pricing and options tools moved from server.py (pure move, no behaviour change)
# ---------------------------------------------------------------------------


@yfinance_server.tool(
    name="get_option_expiration_dates",
    description="""Fetch the available options expiration dates for a given ticker symbol.

Args:
    ticker: str
        The ticker symbol of the stock to get option expiration dates for, e.g. "AAPL"
""",
    after="get_financial_statement",
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
    after="get_option_expiration_dates",
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
    after="get_market_calendar",
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
    after="search_ticker",
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
        default_after = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=7)).strftime("%Y-%m-%d")
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


@yfinance_server.tool(
    name="get_market_quote",
    description=(
        "Get a lightweight Yahoo regular-market price observation for one or more tickers. "
        "lastPrice uses priceBasis=REGULAR_MARKET_PRICE and includes priceTimestamp. "
        "It may differ from get_price_slope.endClose, which is adjusted daily-bar analytics."
    ),
    after="screen_stocks",
)
async def get_market_quote(ticker: str | list[str]) -> str:
    return await get_fast_info(ticker)


@yfinance_server.tool(name="get_historical_prices", description="Canonical alias for get_historical_stock_prices.", after="get_market_quote")
async def get_historical_prices(ticker: str, period: HistoricalPeriod = "1mo", interval: HistoricalInterval = "1d", prepost: bool = False) -> str:
    return await get_historical_stock_prices(ticker=ticker, period=period, interval=interval, prepost=prepost)


@yfinance_server.tool(name="analyze_price_performance", description="Canonical alias for get_price_stats.", after="get_historical_prices")
async def analyze_price_performance(ticker: str | list[str]) -> str:
    return await get_price_stats(ticker)


@yfinance_server.tool(name="analyze_moving_average_position", description="Canonical alias for get_ma_position.", after="analyze_price_performance")
async def analyze_moving_average_position(ticker: str | list[str]) -> str:
    return await get_ma_position(ticker)


@yfinance_server.tool(name="analyze_volume_ratio", description="Canonical alias for get_volume_ratio.", after="analyze_moving_average_position")
async def analyze_volume_ratio(ticker: str | list[str], period: int = 10) -> str:
    return await get_volume_ratio(ticker, period)


@yfinance_server.tool(name="check_volume_liquidity_threshold", description="Canonical alias for get_volume_gate. Pass min_adv_usd for a caller-chosen threshold; meetsMinimum reports adv20dTradedValueUsd >= min_adv_usd.", after="analyze_volume_ratio")
async def check_volume_liquidity_threshold(
    ticker: str,
    foreign_exchange: bool = False,
    min_adv_usd: Annotated[float | None, Field(description=MIN_ADV_USD_DESCRIPTION)] = None,
) -> str:
    min_adv_usd, min_adv_error = parse_min_adv_usd(min_adv_usd)
    if min_adv_error:
        return _mcp_failure("check_volume_liquidity_threshold", ErrorCode.INPUT_VALIDATION_ERROR, min_adv_error)
    return await get_volume_gate(ticker, foreign_exchange, min_adv_usd)


@yfinance_server.tool(name="summarize_options_flow", description="Canonical alias for get_options_summary/get_options_flow_summary.", after="get_company_events_calendar")
async def summarize_options_flow(ticker: str, expiry_hint: str | None = None) -> str:
    return await get_options_summary(ticker=ticker, expiry_hint=expiry_hint)


@yfinance_server.tool(name="analyze_options_flow_window", description="Analyze an event-window options flow reading. LOW chain quality returns RETRY and must not support derivative conclusions.", after="summarize_options_flow")
async def analyze_options_flow_window(ticker: str, window_label: str) -> str:
    return await get_options_flow_scan(ticker, window_label)


@yfinance_server.tool(name="find_put_hedge_candidates", description="Find OTM put hedge candidates using executable ask cost. Contracts require a two-sided quote plus liquidity evidence; PARTIAL means retry and do not use budgetFeasible as a decision.", after="analyze_options_flow_window")
async def find_put_hedge_candidates(ticker: str, otm_pct_min: float = 8, otm_pct_max: float = 12, budget_usd: float = 500, expiry_after: str = "") -> str:
    return await get_put_hedge_candidates(ticker, otm_pct_min, otm_pct_max, budget_usd, expiry_after)


@yfinance_server.tool(name="calculate_price_target_distance", description="Compare current price to a user-supplied reference price target with both the legacy ratio and directional percent distance.", after="find_put_hedge_candidates")
async def calculate_price_target_distance(
    ticker: str,
    reference_target_price: float | None = None,
    io_pt: float | None = None,
) -> str:
    return await get_price_target_bracket(
        ticker, reference_target_price=reference_target_price, io_pt=io_pt
    )
