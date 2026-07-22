import asyncio
import datetime
import json
from typing import Literal
import zoneinfo

import pandas as pd
import yfinance as yf

from yfmcp.app import yfinance_server
from yfmcp.schemas import _MARKET_SNAPSHOT_OUTPUT_SCHEMA, _TOOL_OUTPUT_SCHEMAS
from yfmcp.envelope import ErrorCode, _mcp_failure
from yfmcp.validation import _validate_ticker
from yfmcp.cache import _PRICE_TTL, _STMT_TTL, _cache_get, _cache_set
from yfmcp.util import _fetch_with_retry, get_last_trading_date
from yfmcp.clients.yahoo import _safe_parse


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

    cache_key = f"hist:{ticker}:{period}:{interval}:{prepost}:camel-v2"
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
        wanted = {"date"}
        for col in columns:
            key = column_map.get(str(col), str(col))
            wanted.add(key)
        return [{k: r[k] for k in wanted if k in r} for r in rows]

    cached = _cache_get(cache_key, _PRICE_TTL)
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

    hist_data = hist_data.reset_index(names="Date").rename(columns=column_map)
    full_result = hist_data.to_json(orient="records", date_format="iso")
    _cache_set(cache_key, full_result)

    if columns:
        try:
            rows = json.loads(full_result)
            return json.dumps(_filter_rows(rows))
        except Exception:
            pass
    return full_result

# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_fast_info",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_fast_info"],
    description="""Alias for get_market_quote. Get lightweight real-time price and market data for one or more ticker symbols. Returns ~20 high-signal fields
plus pre-market/after-hours prices when available.

PREFER THIS over get_stock_info for any query involving current price, market cap, 52-week range,
moving averages, or trading volume — it uses ~85-90% fewer tokens than get_stock_info.

Fields returned: currency, exchange, quoteType, lastPrice, priceBasis, observationType, open, previousClose,
dayHigh, dayLow, yearHigh, yearLow, yearChange, marketCap, shares, lastVolume,
tenDayAverageVolume, threeMonthAverageVolume, fiftyDayAverage, twoHundredDayAverage,
priceTimestamp, marketState, marketOpen, lastTradeDate, postMarketTimestamp.

lastPrice is Yahoo's regular-market price observation. It is not a historical
adjusted close and may differ from get_price_slope.endClose during an active
session or when the two Yahoo endpoints were observed at different times.

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

Returns: startClose, endClose, endRawClose, priceBasis, observationType,
slopePct, direction (UP/DOWN/FLAT), and dataDate.

startClose/endClose use adjusted daily closes when Yahoo provides them.
endRawClose is the unadjusted close from the same dated bar. Compare it with
get_market_quote.lastPrice only when their dates/timestamps describe the same
market observation; an active daily bar may change during the session.

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
        hist = company.history(period=f"{days + 10}d", interval="1d", auto_adjust=False)
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    if hist is None or hist.empty or len(hist) < 2:
        return json.dumps({"error": True, "message": f"Insufficient price data for {ticker}", "ticker": ticker})

    adjusted_closes = hist["Adj Close"].dropna() if "Adj Close" in hist else hist["Close"].dropna()
    price_basis = "ADJUSTED_CLOSE" if "Adj Close" in hist and len(adjusted_closes) >= 2 else "UNADJUSTED_CLOSE"
    closes = adjusted_closes if len(adjusted_closes) >= 2 else hist["Close"].dropna()
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
    raw_close = hist.loc[last_idx, "Close"] if "Close" in hist and last_idx in hist.index else None

    return json.dumps({
        "ticker": ticker,
        "days": days,
        "startClose": round(start_close, 2),
        "endClose": round(end_close, 2),
        "endRawClose": round(float(raw_close), 2) if raw_close is not None and not pd.isna(raw_close) else None,
        "priceBasis": price_basis,
        "observationType": "DAILY_PRICE_BAR",
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

_NY_TZ = zoneinfo.ZoneInfo("America/New_York")
_ENDED_SESSION_STALE_HOURS = 12


def _overnight_window_utc_for_session_end_date(
    session_end_date_et: datetime.date,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return UTC overnight window for a given ET session-end date (04:00 ET boundary)."""
    start_et = pd.Timestamp(
        datetime.datetime.combine(
            session_end_date_et - datetime.timedelta(days=1),
            datetime.time(hour=20),
        ),
        tz=_NY_TZ,
    )
    end_et = pd.Timestamp(
        datetime.datetime.combine(session_end_date_et, datetime.time(hour=4)),
        tz=_NY_TZ,
    )
    return start_et.tz_convert("UTC"), end_et.tz_convert("UTC")


def _overnight_window_utc(now_utc: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return the relevant Blue Ocean overnight UTC window for the current ET date context."""
    now_utc = pd.Timestamp(now_utc).tz_convert("UTC")
    now_et = now_utc.tz_convert(_NY_TZ)

    if now_et.hour < 12:
        # Overnight session that ended (or is ending) this ET morning.
        session_end_date_et = now_et.date()
    else:
        # Upcoming overnight session for tonight (or currently active after 20:00 ET).
        session_end_date_et = now_et.date() + datetime.timedelta(days=1)
    return _overnight_window_utc_for_session_end_date(session_end_date_et)


def _classify_overnight_session(now_utc: pd.Timestamp) -> Literal["ACTIVE", "ENDED", "NOT_STARTED"]:
    """Classify the overnight session state for current ET context."""
    session_start_utc, session_end_utc = _overnight_window_utc(now_utc)
    if session_start_utc <= now_utc < session_end_utc:
        return "ACTIVE"
    if now_utc < session_start_utc:
        return "NOT_STARTED"
    return "ENDED"

@yfinance_server.tool(
    name="get_overnight_quote",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_overnight_quote"],
    description="""Deprecated diagnostics-only Yahoo extended-hours proxy for a ticker.

This does not provide true 20:00-04:00 ET overnight venue data. Equity results
are Yahoo indicative extended-hours data and are not decision-grade overnight
quotes. Prefer get_market_quote for regular/pre/post-market fields.

Returns: overnightPrice, overnightTime, overnightHigh, overnightLow, overnightOpen,
overnightVolume, sessionDate, timezone, previousClose, gapPct, gapDirection,
dataSource, isBlueOceanWindow, sessionStatus, requestedAt, isStale, dataAgeHours,
fallback, provider, providerStatus, dataKind, decisionGrade, warnings, note.

Args:
    ticker: str
        The ticker symbol, e.g. "BTC-USD", "ASTS", or "ES=F"
""",
)
async def get_overnight_quote(ticker: str) -> str:
    """Get overnight quote data with session-status and timezone guardrails."""
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
            "sessionStatus": "NOT_STARTED",
            "requestedAt": pd.Timestamp.now(tz="UTC").isoformat(),
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
    fi = None
    try:
        fi = company.fast_info
        prev_close = fi["previousClose"]
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

    now_utc = pd.Timestamp.now(tz="UTC")
    requested_at = now_utc.isoformat()
    session_status = _classify_overnight_session(now_utc)
    session_start_utc, session_end_utc = _overnight_window_utc(now_utc)
    target_start_utc = session_start_utc
    target_end_utc = session_end_utc
    note: str | None = None
    if session_status == "NOT_STARTED":
        target_start_utc, target_end_utc = _overnight_window_utc_for_session_end_date(
            now_utc.tz_convert(_NY_TZ).date()
        )
        note = (
            "Overnight session has not started yet for current ET day. "
            "Returning prior overnight session data."
        )

    # Opportunistic upgrade path for yfinance PR #2640 FastInfo overnight fields.
    fast_info_payload: dict[str, object] | None = None
    if fi is not None:
        try:
            fi_price = getattr(fi, "overnight_price")
            fi_time = getattr(fi, "overnight_time")
            fi_high = getattr(fi, "overnight_high")
            fi_low = getattr(fi, "overnight_low")
            fi_open = getattr(fi, "overnight_open")
            fi_volume = getattr(fi, "overnight_volume")
            if fi_time is not None:
                fi_ts_utc = pd.Timestamp(fi_time)
                if fi_ts_utc.tzinfo is None:
                    fi_ts_utc = fi_ts_utc.tz_localize("UTC")
                else:
                    fi_ts_utc = fi_ts_utc.tz_convert("UTC")
                fast_info_payload = {
                    "price": float(fi_price) if fi_price is not None else None,
                    "time_utc": fi_ts_utc,
                    "high": float(fi_high) if fi_high is not None else None,
                    "low": float(fi_low) if fi_low is not None else None,
                    "open": float(fi_open) if fi_open is not None else None,
                    "volume": int(fi_volume) if fi_volume is not None else None,
                }
        except AttributeError:
            fast_info_payload = None
        except Exception:
            fast_info_payload = None

    is_fallback = False
    day_bars = None
    last_ts_utc = None
    overnight_open = overnight_high = overnight_low = overnight_price = overnight_volume = None
    if fast_info_payload is not None and target_start_utc <= fast_info_payload["time_utc"] < target_end_utc:
        overnight_open = fast_info_payload["open"]
        overnight_high = fast_info_payload["high"]
        overnight_low = fast_info_payload["low"]
        overnight_price = fast_info_payload["price"]
        overnight_volume = fast_info_payload["volume"]
        last_ts_utc = fast_info_payload["time_utc"]
    else:
        utc_index = hist.index.tz_convert("UTC")
        overnight_mask = (utc_index >= target_start_utc) & (utc_index < target_end_utc)
        overnight = hist[overnight_mask]

        if overnight.empty:
            # Fallback: most recent pre-market bar in 08:00–14:00 UTC.
            premarket_mask = (utc_index.hour >= 8) & (utc_index.hour < 14)
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
                    "sessionStatus": session_status,
                    "requestedAt": requested_at,
                    "_note": "No overnight or pre-market data found for this ticker",
                })
                _cache_set(cache_key, result)
                return result
            day_bars = premarket.iloc[[-1]]
            is_fallback = True
        else:
            day_bars = overnight

        if day_bars is None or day_bars.empty:
            result = json.dumps({
                "ticker": ticker,
                "overnightPrice": None,
                "overnightTime": None,
                "overnightHigh": None,
                "overnightLow": None,
                "overnightOpen": None,
                "overnightVolume": None,
                "sessionStatus": session_status,
                "requestedAt": requested_at,
                "_note": "No overnight data found for selected session window",
            })
            _cache_set(cache_key, result)
            return result

        overnight_open = float(day_bars["Open"].iloc[0]) if "Open" in day_bars.columns else None
        overnight_high = float(day_bars["High"].max()) if "High" in day_bars.columns else None
        overnight_low = float(day_bars["Low"].min()) if "Low" in day_bars.columns else None
        overnight_price = float(day_bars["Close"].iloc[-1]) if "Close" in day_bars.columns else None
        overnight_volume = int(day_bars["Volume"].sum()) if "Volume" in day_bars.columns else None
        last_ts = day_bars.index[-1]
        last_ts_utc = pd.Timestamp(last_ts).tz_convert("UTC")

    if last_ts_utc is None:
        result = json.dumps({
            "ticker": ticker,
            "overnightPrice": None,
            "overnightTime": None,
            "overnightHigh": None,
            "overnightLow": None,
            "overnightOpen": None,
            "overnightVolume": None,
            "sessionStatus": session_status,
            "requestedAt": requested_at,
            "_note": "Overnight timestamp unavailable",
        })
        _cache_set(cache_key, result)
        return result

    overnight_time = last_ts_utc.isoformat()

    # sessionDate in exchange local timezone
    session_date = str(last_ts_utc.tz_convert(tz).date())

    # Data quality flags
    last_ts_et = last_ts_utc.tz_convert(_NY_TZ)
    is_blue_ocean = (last_ts_et.hour >= 20) or (last_ts_et.hour < 4)
    data_source = "EXCHANGE" if (overnight_volume or 0) > 0 else "OTC_INDICATIVE"

    # Staleness with session-status guardrails
    data_age_hours = round((now_utc - last_ts_utc).total_seconds() / 3600, 1)
    if session_status == "ACTIVE":
        is_stale = data_age_hours > 2
    elif session_status == "ENDED":
        is_stale = now_utc > (target_end_utc + pd.Timedelta(hours=_ENDED_SESSION_STALE_HOURS))
    else:
        is_stale = True

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
        "sessionStatus": session_status,
        "requestedAt": requested_at,
        "isStale": is_stale,
        "dataAgeHours": data_age_hours,
        "fallback": is_fallback,
        "note": (
            "True overnight window (20:00–04:00 ET) unavailable via Yahoo Finance API. "
            "Returning last pre-market OTC indicative quote as proxy."
        ) if is_fallback else note,
    })
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# CR-12 — get_options_flow_scan

# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_volume_gate",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_volume_gate"],
    description="""Deprecated alias for check_volume_liquidity_threshold. Check current trading volume and dollar-notional liquidity against public liquidity thresholds.

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
