"""General utility helpers: retry, date/ISO, data quality, option sorting, text filtering.

Extracted from server.py in Phase 1 of the refactoring plan.
"""

import asyncio
import datetime
import email.utils as _email_utils
import json
import re as _re


# ---------------------------------------------------------------------------
# Async retry helper
# ---------------------------------------------------------------------------
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
    d = datetime.datetime.utcnow().date()
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
                        ltd_date = datetime.datetime.utcfromtimestamp(ltd_seconds).date()
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
