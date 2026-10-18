"""Helpers shared by the news providers and the news pipeline (yfmcp.tools.news): text cutting,
keyword event typing, duplicate-group identity, date windows and exchange normalisation.

Moved out of yfmcp/tools/news.py in 2.15.1 (pure restructure). Providers import this module; it
imports no provider and never imports yfmcp.tools.news.
"""

import datetime
import hashlib
import html as _html_module
import re as _re


_DEDUP_TITLE_MAX_LEN = 80


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


def _normalize_exchange(exch: str | None) -> str | None:
    if not exch:
        return None
    exch = str(exch).upper()
    if any(x in exch for x in ("NASDAQ", "NMS", "NGM", "NCM", "GS")):
        return "NASDAQ"
    if "NYSE" in exch or exch == "NYQ":
        return "NYSE"
    if any(x in exch for x in ("AMEX", "NYSE AMERICAN", "ASE")):
        return "NYSEAMERICAN"
    if "TSXV" in exch or "CVE" in exch:
        return "TSXV"
    if "TSX" in exch or "TOR" in exch:
        return "TSX"
    return None
