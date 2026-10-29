"""Helpers shared by the news providers and the news pipeline (yfmcp.tools.news): text cutting,
keyword event typing, duplicate-group identity, date windows and exchange normalisation.

Moved out of yfmcp/tools/news.py in 2.15.1 (pure restructure). Providers import this module; it
imports no provider and never imports yfmcp.tools.news.
"""

import datetime
import hashlib
import html as _html_module
import re as _re

# Moved to yfmcp.util in R5-pricing-1; re-exported for the news modules that import it from here.
from yfmcp.util import _round_half_away_from_zero  # noqa: F401


_DEDUP_TITLE_MAX_LEN = 80


# Event-type keyword table, shared with the Worker (worker/src/news/shared.ts EVENT_TYPE_KEYWORDS). Groups are tested in
# order and the first group with a whole-word match (optional s/es/d/ed/ing suffix on the last word) wins.
_EVENT_TYPE_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("earnings", ("earnings", "eps", "quarterly", "10-q", "10-k", "annual report")),
    ("guidance", ("guidance", "outlook", "forecast", "reaffirm", "raises", "lowers")),
    ("contract", ("contract", "agreement", "deal", "partnership")),
    ("financing", ("offering", "financing", "debt", "credit facility", "note")),
    ("product", ("launch", "product", "introduce", "announces new")),
    ("analyst", ("analyst", "rating", "upgrade", "downgrade", "price target")),
    ("macro", ("macro", "inflation", "rates", "fomc", "cpi")),
    ("litigation", ("lawsuit", "litigation", "court", "settlement")),
    ("insider", ("insider", "director", "officer", "form 4")),
    ("regulatory", ("sec", "regulatory", "8-k", "10-q", "10-k", "filing")),
)
# re.ASCII keeps \b the same as JavaScript's, so 'sector', 'dealer' and 'steps' do not hit 'sec', 'deal' and 'eps'.
_EVENT_TYPE_PATTERNS = tuple(
    (event_type, _re.compile(r"\b(?:" + "|".join(_re.escape(k) for k in keywords) + r")(?:s|es|d|ed|ing)?\b", _re.ASCII))
    for event_type, keywords in _EVENT_TYPE_KEYWORDS
)


def _event_type_from_keywords(text: str) -> str:
    t = (text or "").lower()
    for event_type, pattern in _EVENT_TYPE_PATTERNS:
        if pattern.search(t):
            return event_type
    return "other"


def _ticker_in_text(text: object, ticker: str) -> bool:
    """Whether the ticker appears in the text (case-insensitive). A single-letter ticker must be a whole token, so 'A' is
    not found inside 'MARKETS' (2.17.0); longer tickers keep the substring test. Mirrors the Worker's tickerInText."""
    ticker_u = str(ticker or "").upper()
    text_u = str(text or "").upper()
    if len(ticker_u) == 1:
        return _re.search(r"\b" + _re.escape(ticker_u) + r"\b", text_u, _re.ASCII) is not None
    return ticker_u in text_u


def _normalized_event_title_stem(ticker: str, title: str | None) -> str:
    text = _html_module.unescape(str(title or "")).lower()
    text = _re.sub(r"https?://\S+", " ", text)
    text = _re.sub(r"[^a-z0-9]+", " ", text)
    ticker_l = str(ticker or "").lower()
    if ticker_l:
        text = _re.sub(rf"\b{_re.escape(ticker_l)}\b", " ", text)
    stop_terms = {
        "yahoo", "finance", "finnhub", "globenewswire", "press", "release",
        "inc", "corp", "corporation", "company", "plc", "ltd", "llc",
    }
    words = [w for w in text.split() if w not in stop_terms]
    return " ".join(words)[:_DEDUP_TITLE_MAX_LEN]


def _short_text(text: object, max_chars: int = 220) -> str | None:
    value = " ".join(str(text or "").split())
    if not value:
        return None
    return value[:max_chars]


def _make_duplicate_group_id(
    ticker: str,
    title: str | None,
    published_at: str | None,
    issuer: str | None,
    url: str | None,
) -> str | None:
    norm_title = _normalized_event_title_stem(ticker, title)
    event_day = (published_at or "")[:10]
    entity = (issuer or ticker or "").upper().strip()
    if not norm_title and not event_day:
        return None
    key = f"{norm_title}|{event_day}|{entity}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


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


_NASDAQ_EXCHANGE_CODES = frozenset({"GS", "NMS", "NGM", "NCM"})


def _normalize_exchange(exch: str | None) -> str | None:
    """The exchange normaliser shared by every news exchange check and by the Worker (2.17.0): NYSE American / AMEX / exact
    ASE are tested before NYSE, and GS / NMS / NGM / NCM count only as whole tokens or exact codes."""
    if not exch:
        return None
    exch = str(exch).upper()
    tokens = [token for token in _re.split(r"[^A-Z0-9]+", exch) if token]
    if "NASDAQ" in exch or any(token in _NASDAQ_EXCHANGE_CODES for token in tokens):
        return "NASDAQ"
    if "NYSE AMERICAN" in exch or "AMEX" in exch or exch == "ASE":
        return "NYSEAMERICAN"
    if "NYSE" in exch or exch == "NYQ":
        return "NYSE"
    if "TSXV" in exch or "CVE" in exch:
        return "TSXV"
    if "TSX" in exch or "TOR" in exch:
        return "TSX"
    return None
