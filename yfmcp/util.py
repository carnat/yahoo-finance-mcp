"""General utility helpers: retry, date/ISO, data quality, option sorting, text filtering.

Extracted from server.py in Phase 1 of the refactoring plan.
"""

import asyncio
import datetime
import email.utils as _email_utils
import json
import decimal
import math
import re as _re
from typing import Any
from yfmcp.clients.endpoints import YAHOO_QUOTE_SUMMARY_PREFIX_QUERY2


def _safe_json_loads(payload: str) -> dict:
    try:
        data = json.loads(payload)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _compact_excerpt(text: object, max_len: int = 240) -> str:
    value = _re.sub(r"\s+", " ", str(text or "")).strip()
    return value if len(value) <= max_len else value[:max_len].rstrip() + "..."


# ---------------------------------------------------------------------------
# Async retry helper
# ---------------------------------------------------------------------------
async def _fetch_with_retry(fn, *args, retries: int = 1, delay: float = 2.0, **kwargs):
    """Call blocking fn(*args, **kwargs) off the event loop, retrying once on exception."""
    for attempt in range(retries + 1):
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        except Exception:
            if attempt < retries:
                await asyncio.sleep(delay)
            else:
                raise


# ---------------------------------------------------------------------------
# Trading-date helper
# ---------------------------------------------------------------------------
def get_last_trading_date(df=None) -> str:
    """Returns the last trading date as a YYYY-MM-DD string.
    Uses the last DataFrame index row if provided.
    Falls back to the last weekday from the UTC system clock.
    Note: does not account for market holidays — weekday fallback only.
    """
    if df is not None and len(df) > 0:
        return df.index[-1].strftime('%Y-%m-%d')
    d = datetime.datetime.now(datetime.timezone.utc).date()
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= datetime.timedelta(days=1)
    return d.strftime('%Y-%m-%d')


# ---------------------------------------------------------------------------
# Options data-quality helpers
# ---------------------------------------------------------------------------
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
                        ltd_date = datetime.datetime.fromtimestamp(ltd_seconds, datetime.timezone.utc).date()
                    if (data_date_obj - ltd_date).days > stale_days_threshold:
                        stale_trade += 1
                except (ValueError, TypeError):
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


# ---------------------------------------------------------------------------
# ISO UTC date/time helpers
# ---------------------------------------------------------------------------
def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _to_iso_utc(raw: object) -> str | None:
    """Normalize a timestamp to ISO 8601 UTC with a Z suffix.

    Accepts epoch seconds (int/float), 8/14-digit compact timestamps
    (YYYYMMDD[HHMMSS]), date-only strings (YYYY-MM-DD), and ISO 8601
    strings. Returns None for anything unparseable.

    Date-only and compact inputs are normalized without going through
    datetime.fromisoformat, which would treat them as naive local time
    and shift the result on non-UTC hosts.
    """
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
    if len(value) == 10 and _re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return f"{value}T00:00:00Z"
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        pass
    # RFC 2822 provider dates ("Tue, 13 Oct 2026 10:00:00 GMT"), which the Worker's Date parser reads (2.17.0).
    try:
        parsed = _email_utils.parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _parse_rss_date(raw: str) -> str | None:
    """Parse an RSS pubDate (RFC 2822) string to ISO 8601 UTC.

    Falls back to :func:`_to_iso_utc` for non-RFC-2822 strings so that
    alternative date formats found in some feeds are handled gracefully.
    """
    if not raw:
        return None
    try:
        dt = _email_utils.parsedate_to_datetime(raw.strip())
        return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return _to_iso_utc(raw)


# ---------------------------------------------------------------------------
# Text paragraph filter (used by earnings/filing content tools)
# ---------------------------------------------------------------------------
def _filter_paragraphs_by_topics(text: str, topics: list[str], context_chars: int = 200) -> list[dict]:
    """Split text into paragraphs and return those matching any of the given topics/keywords.

    Returns a list of dicts with keys: paragraph, matchedTopics.
    If topics is empty, returns nothing (caller should use full text).
    """
    if not topics:
        return []
    # Split on double-newlines or single newlines with enough content
    paragraphs = [p.strip() for p in _re.split(r"\n\s*\n|\n", text) if p.strip() and len(p.strip()) > 20]
    results: list[dict] = []
    topic_patterns = [(t, _re.compile(_re.escape(t), _re.IGNORECASE)) for t in topics]
    for para in paragraphs:
        matched: list[str] = []
        for topic_name, pattern in topic_patterns:
            if pattern.search(para):
                matched.append(topic_name)
        if matched:
            results.append({"paragraph": para[:context_chars * 5] if len(para) > context_chars * 5 else para, "matchedTopics": matched})
    return results


# ---------------------------------------------------------------------------
# Yahoo quoteSummary helpers (mirror worker/src/yahoo-finance.ts)
# ---------------------------------------------------------------------------
_YAHOO_DATE_FMT = _re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def unwrap_yahoo_values(value: object) -> object:
    """Worker ``unwrapYahooValues``: Yahoo quoteSummary values arrive as {raw, fmt, longFmt} objects.
    Keep the number (or the formatted date, for date values) and drop Yahoo's maxAge bookkeeping:
    {pctHeld: {raw: 0.08, fmt: "8%"}} -> {pctHeld: 0.08}."""
    if isinstance(value, list):
        return [unwrap_yahoo_values(item) for item in value]
    if not isinstance(value, dict):
        return value
    if "raw" in value and all(key in ("raw", "fmt", "longFmt") for key in value):
        fmt = value.get("fmt")
        if isinstance(fmt, str) and _YAHOO_DATE_FMT.fullmatch(fmt):
            return fmt
        return value.get("raw")
    if not value:
        return None
    return {key: unwrap_yahoo_values(inner) for key, inner in value.items() if key != "maxAge"}


def yahoo_raw(value: object) -> object:
    """Worker ``raw``: the ``raw`` of Yahoo's {raw, fmt} wrapper; {} is null."""
    if isinstance(value, dict):
        if "raw" in value:
            return value["raw"]
        if not value:
            return None
    return value


def yahoo_iso(ts: float) -> str:
    """Worker ``iso``: ``new Date(ts * 1000).toISOString()``, e.g. 2026-11-05T21:00:00.000Z."""
    moment = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(milliseconds=int(ts * 1000))
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def fetch_quote_summary_module(ticker: str, module: str) -> dict | None:
    """Blocking: the first quoteSummary result for ``module`` through yfinance's crumb-aware fetcher, or None
    when Yahoo returns no result. Run it in a thread."""
    import yfinance as yf

    data = yf.Ticker(ticker)._data.get_raw_json(
        YAHOO_QUOTE_SUMMARY_PREFIX_QUERY2 + ticker,
        params={"modules": module, "corsDomain": "finance.yahoo.com", "formatted": "true"},
    )
    results = ((data.get("quoteSummary") or {}).get("result")) if isinstance(data, dict) else None
    return results[0] if isinstance(results, list) and results and isinstance(results[0], dict) else None


def _fy_number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _js_json_numbers(value: Any) -> Any:
    """The payload as JavaScript's JSON.stringify writes its numbers: an integral float is an int (2023994000.0 -> 2023994000)."""
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    if isinstance(value, dict):
        return {k: _js_json_numbers(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_js_json_numbers(v) for v in value]
    return value


def _nn(value: object, default: object = None) -> object:
    """JavaScript's `value ?? default`: only None falls back, never "", 0 or False."""
    return default if value is None else value


def _unwrap_payload(raw: str) -> dict:
    data = _safe_json_loads(raw)
    if "ok" in data and isinstance(data.get("data"), dict):
        return data["data"]
    return data


def _clamp_int(value: Any, fallback: int, low: int, high: int) -> int:
    try:
        number = float(value)
        n = int(number) if math.isfinite(number) else fallback
    except (TypeError, ValueError):
        n = fallback
    return min(high, max(low, n))


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _plain(value: Any) -> Any:
    return value.item() if hasattr(value, "item") else value


def _df_period_rows(df: Any) -> dict[str, dict]:
    if df is None or getattr(df, "empty", True):
        return {}
    rows: dict[str, dict] = {}
    for period, row in df.iterrows():
        rows[str(period)] = {str(k): _plain(v) for k, v in row.items()}
    return rows


def _add_years(iso_date: str, years: int) -> str:
    d = datetime.date.fromisoformat(iso_date)
    try:
        return d.replace(year=d.year + years).isoformat()
    except ValueError:  # 29 February
        return d.replace(year=d.year + years, day=28).isoformat()


def _utf16_length(text: str) -> int:
    """A string's length as JavaScript counts it (UTF-16 code units)."""
    return len(text.encode("utf-16-le", "surrogatepass")) // 2


def _round_half_away_from_zero(value: float, digits: int) -> float:
    """Round half away from zero to `digits` decimals, decimal-safe (2.17.0; moved here from yfmcp/news/shared.py in R5-pricing-1): the number's shortest decimal form is
    rounded, so 1.005 gives 1.01 and -0.125 gives -0.13 (the Worker's roundHalfAwayFromZero). A result of zero is 0.0."""
    value = float(value)
    # Past 2^52 at this scale a double has no fraction left to round.
    if not math.isfinite(value) or abs(value) * 10 ** digits >= 2 ** 52:
        return value
    quantum = decimal.Decimal(1).scaleb(-digits)
    rounded = float(decimal.Decimal(repr(abs(value))).quantize(quantum, rounding=decimal.ROUND_HALF_UP))
    return -rounded if value < 0 and rounded != 0 else rounded
