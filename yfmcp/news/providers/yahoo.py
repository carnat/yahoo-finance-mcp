"""Yahoo Finance news provider: the search feed (_yahoo_search_news), issuer/identity matching and
_collect_yahoo_events.

Adapter contract (unchanged by the move): each ``_collect_*_events`` keeps its current signature and
return value. The return value is the items plus the per-source status/diagnostics; warnings are
appended to the caller's ``warnings`` list. There is no provider registry yet (2.15.1 is a pure
restructure; the registry and the off switch are part 2).
"""

import re as _re

import yfinance as yf

from yfmcp.util import _to_iso_utc
from yfmcp.news.shared import (
    _event_type_from_keywords,
    _make_duplicate_group_id,
    _normalize_exchange,
    _short_text,
    _ticker_in_text,
    _within_date_window,
)


_YAHOO_NEWS_LEGAL_SUFFIXES = frozenset({
    "inc", "incorporated", "corp", "corporation", "ltd", "limited", "llc", "plc",
    "co", "company", "sa", "ag", "nv", "se", "gmbh",
})


_YAHOO_NEWS_ACRONYM_IGNORED_WORDS = frozenset({"the", "and", "of", "for"})


def _normalize_event_text(value: object) -> str:
    return _re.sub(r"\s+", " ", _re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()


def _strip_yahoo_news_legal_suffix(value: str) -> str:
    words = [word for word in value.split() if word]
    while len(words) > 1 and words[-1] in _YAHOO_NEWS_LEGAL_SUFFIXES:
        words.pop()
    return " ".join(words)


def _yahoo_news_identity_from_info(ticker: str, info: dict | None) -> dict:
    info = info if isinstance(info, dict) else {}
    aliases: set[str] = set()
    acronyms: set[str] = set()
    company_name = str(info.get("longName") or info.get("shortName") or "").strip() or None
    for raw_name in (info.get("shortName"), info.get("longName")):
        normalized = _normalize_event_text(raw_name)
        if not normalized:
            continue
        stripped = _strip_yahoo_news_legal_suffix(normalized)
        for alias in (normalized, stripped):
            # A shortName that falls back to the ticker is not issuer identity.
            if len(alias) >= 3 and alias != ticker.lower():
                aliases.add(alias)
        initials = "".join(
            word[0] for word in stripped.split()
            if word and word not in _YAHOO_NEWS_ACRONYM_IGNORED_WORDS
        )
        if 3 <= len(initials) <= 6:
            acronyms.add(initials)
    return {
        "status": "RESOLVED" if aliases else "UNAVAILABLE",
        "companyName": company_name,
        "aliases": tuple(sorted(aliases)),
        "acronyms": tuple(sorted(acronyms)),
        "exchange": info.get("exchange") or info.get("exchangeName"),
        "ticker": ticker.upper(),
    }


def _yahoo_item_issuer(item: dict, identity: dict) -> str | None:
    """Issuer of a Yahoo press-release item: the company when the headline starts with its name or ticker."""
    if item.get("source") != "yahoo_finance_press_releases":
        return None
    title = _normalize_event_text(item.get("title"))
    leads = [*(identity.get("aliases") or ()), *(str(t).lower() for t in item.get("tickers") or [])]
    if any(lead and (title == lead or title.startswith(f"{lead} ")) for lead in leads):
        return identity.get("companyName")
    return None


def _yahoo_news_match_for(text: str, ticker: str, identity: dict) -> tuple[str, int] | None:
    if _is_ticker_compatible_with_context(text, ticker, identity.get("exchange")):
        return "TICKER_TOKEN", 0
    if identity.get("status") != "RESOLVED":
        return None
    normalized_text = f" {_normalize_event_text(text)} "
    if any(f" {alias} " in normalized_text for alias in identity.get("aliases") or ()):
        return "ISSUER_NAME", 1
    if any(_re.search(r"\b" + _re.escape(acronym) + r"\b", text, _re.IGNORECASE) for acronym in identity.get("acronyms") or ()):
        return "ISSUER_ACRONYM", 2
    return None


def _yahoo_decision_use(event_type: str) -> str:
    return "CHECK_OFFICIAL_RELEASES" if event_type in {
        "earnings", "guidance", "contract", "financing", "product", "regulatory", "litigation",
    } else "CONTEXT_ONLY"


_YAHOO_ALLOWED_CONTENT_TYPES = {"STORY", "ARTICLE", "PRESS_RELEASE"}


def _build_yahoo_event_item(
    ticker: str,
    news_item: dict,
    retrieved_at: str,
    feed_source: str | None = None,
) -> tuple[dict, list[dict]]:
    """Build a standardised event item from a Yahoo Finance news entry.

    feed_source, when provided, sets the ``source`` / ``sourceType`` explicitly
    (``"yahoo_finance_news"`` or ``"yahoo_finance_press_releases"``).  When
    omitted the label is inferred from the item's ``contentType`` field:
    ``PRESS_RELEASE`` → ``yahoo_finance_press_releases``, anything else →
    ``yahoo_finance_news``.  The original publisher name is preserved in
    ``originalSource``.
    """
    warnings: list[dict] = []
    content = news_item.get("content", {}) if isinstance(news_item.get("content"), dict) else {}
    title = str(content.get("title") or news_item.get("title") or "").strip()
    summary = str(content.get("summary") or news_item.get("summary") or "").strip()
    url = str((content.get("canonicalUrl", {}) or {}).get("url") or news_item.get("link") or news_item.get("url") or "").strip()
    provider = str((content.get("provider", {}) or {}).get("displayName") or news_item.get("publisher") or "Yahoo Finance").strip()

    # Determine the precise source label.
    if feed_source in ("yahoo_finance_news", "yahoo_finance_press_releases"):
        source_key = feed_source
    else:
        content_type = str(content.get("contentType") or news_item.get("contentType") or "").upper()
        source_key = "yahoo_finance_press_releases" if content_type == "PRESS_RELEASE" else "yahoo_finance_news"

    published_at = _to_iso_utc(news_item.get("providerPublishTime") or content.get("pubDate") or news_item.get("publishedAt"))
    if not published_at:
        warnings.append({
            "code": "PUBLISHED_AT_UNAVAILABLE",
            "message": f"Published timestamp unavailable for source '{provider or 'Yahoo Finance'}'.",
            "severity": "warning",
        })
    ticker_u = ticker.upper()
    ticker_relevance = "HIGH" if _ticker_in_text(f"{title} {summary}", ticker_u) else "LOW"
    confidence = "MEDIUM"
    if not url:
        confidence = "LOW"
    if ticker_relevance == "LOW":
        confidence = "LOW"
    issuer = None
    duplicate_group_id = _make_duplicate_group_id(ticker, title, published_at, issuer, url)
    if duplicate_group_id is None:
        warnings.append({"code": "DEDUPE_WEAK_KEY", "message": "Weak dedupe key for at least one item.", "severity": "warning"})
    item = {
        "title": title,
        "source": source_key,
        "originalSource": provider or "Yahoo Finance",
        "sourceType": source_key,
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


def _is_ticker_compatible_with_context(text: str, ticker: str, company_exchange: str | None) -> bool:
    ticker_u = ticker.upper()
    ticker_pat = _re.compile(r"\b" + _re.escape(ticker_u) + r"\b", _re.IGNORECASE)
    matches = list(ticker_pat.finditer(text))
    if not matches:
        return False
    
    norm_comp = _normalize_exchange(company_exchange)
    if not norm_comp:
        return True
        
    prefix_pat = _re.compile(r"\b([A-Za-z0-9]+)\s*:\s*$", _re.IGNORECASE)
    
    for m in matches:
        start = m.start()
        preceding = text[max(0, start - 20):start]
        pm = prefix_pat.search(preceding)
        if pm:
            prefix = pm.group(1).upper()
            norm_prefix = _normalize_exchange(prefix)
            if norm_prefix:
                if norm_prefix == norm_comp:
                    return True
            else:
                if prefix in ("TSXV", "TSX", "NYSE", "NASDAQ", "AMEX", "CVE", "ASX", "LSE", "TSX-V"):
                    # Incompatible exchange prefix
                    pass
                else:
                    return True
        else:
            return True
            
    return False


async def _collect_yahoo_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
    feed: str = "news",
    identity: dict | None = None,
    include_diagnostics: bool = False,
) -> tuple:
    """Fetch Yahoo Finance items for the given *feed*.

    ``feed="news"`` fetches the general news tab and labels items as
    ``yahoo_finance_news`` (press-release items from that feed are labelled
    ``yahoo_finance_press_releases`` automatically via their ``contentType``).

    ``feed="press_releases"`` fetches the press-releases tab directly
    (requires yfinance ≥ 0.2.x with ``get_news(tab=…)`` support) and labels
    all returned items as ``yahoo_finance_press_releases``.
    """
    warnings: list[dict] = []
    items: list[dict] = []
    diagnostics: dict = {
        "rawCount": 0,
        "retrievedCount": 0,
        "filteredCount": 0,
        "acceptedCount": 0,
        "rejectedCount": 0,
        "rejectionCounts": {},
        "identityStatus": "UNAVAILABLE",
    }

    def _reject(reason: str) -> None:
        diagnostics["rejectedCount"] += 1
        counts = diagnostics["rejectionCounts"]
        counts[reason] = counts.get(reason, 0) + 1

    def _result(collected: list[dict], used: bool) -> tuple:
        if include_diagnostics:
            return collected, warnings, used, diagnostics
        return collected, warnings, used

    feed_source_override: str | None = "yahoo_finance_press_releases" if feed == "press_releases" else None

    try:
        company = yf.Ticker(ticker)
        # Yahoo retired the endpoint behind get_news(tab=...) (/xhr/ncp: HTTP 404 for every ticker, and yfinance
        # returns an empty list), so both feeds read the news search feed the Worker reads; press releases are the
        # issuer's own wire releases in it (2.5.33).
        if feed == "press_releases":
            diagnostics["method"] = _YAHOO_PRESS_RELEASE_METHOD
        raw_news = _yahoo_search_news(ticker)
    except Exception as exc:
        warnings.append({"code": "SOURCE_UNAVAILABLE", "message": f"Yahoo Finance source unavailable: {exc}", "severity": "warning"})
        return _result(items, False)

    if identity is None:
        try:
            identity = _yahoo_news_identity_from_info(ticker, company.info)
        except Exception:
            identity = _yahoo_news_identity_from_info(ticker, None)
    diagnostics["identityStatus"] = identity.get("status") or "UNAVAILABLE"
    accepted: list[tuple[dict, int]] = []

    for n in raw_news:
        if not isinstance(n, dict):
            continue
        diagnostics["rawCount"] += 1
        diagnostics["retrievedCount"] += 1
        content = n.get("content", {}) if isinstance(n.get("content"), dict) else {}
        content_type = str(content.get("contentType") or n.get("contentType") or "").upper()
        if content_type and content_type not in _YAHOO_ALLOWED_CONTENT_TYPES:
            _reject("CONTENT_TYPE")
            continue
        # For the press-releases feed, Yahoo tab membership is authoritative:
        # valid press-release tab items may still arrive as STORY/ARTICLE.
        item, item_warnings = _build_yahoo_event_item(ticker, n, retrieved_at, feed_source=feed_source_override)

        if not _within_date_window(item.get("publishedAt"), start_date=start_date, end_date=end_date, lookback_days=lookback_days):
            _reject("DATE_WINDOW")
            continue
        # Identity and ticker matching read the title only; the summary is for display (planner call #13, 2.17.0).
        match = _yahoo_news_match_for(str(item.get("title") or ""), ticker, identity)
        if match is None:
            _reject("IDENTITY_UNAVAILABLE_TICKER_NOT_FOUND" if identity.get("status") == "UNAVAILABLE" else "IDENTITY_MISMATCH")
            continue
        match_basis, rank = match
        # The match names the company, but a release about it may be issued
        # by another company ("Qualcomm renews license with Apple").
        item["mentionedCompany"] = identity.get("companyName")
        item["issuer"] = _yahoo_item_issuer(item, identity)
        # A press release is the company's own: a wire release whose headline leads with its name or ticker
        # (AEHR's ACCESS Newswire Sonoma order), not another issuer's release that mentions it (QuickLogic's).
        if feed == "press_releases" and not (_PRESS_RELEASE_WIRE_RE.match(str(item.get("originalSource") or "")) and item["issuer"]):
            _reject("NOT_ISSUER_WIRE_RELEASE")
            continue
        item["matchBasis"] = match_basis
        item["sourceTickerMatch"] = True
        item["tickerRelevance"] = "HIGH"
        item["confidence"] = "MEDIUM" if item.get("url") else "LOW"
        item["decisionUse"] = _yahoo_decision_use(str(item.get("eventType") or "other"))
        accepted.append((item, rank))
        warnings.extend(item_warnings)
    # Stable sorts make the primary match rank deterministic, then newest first.
    accepted.sort(key=lambda pair: str(pair[0].get("publishedAt") or ""), reverse=True)
    accepted.sort(key=lambda pair: pair[1])
    diagnostics["filteredCount"] = len(accepted)
    diagnostics["acceptedCount"] = len(accepted)
    items = [item for item, _rank in accepted[:max_results]]
    return _result(items, True)


def _yahoo_search_news(ticker: str) -> list[dict]:
    """Yahoo's news search feed for a ticker (the Worker's getNews reads the same endpoint)."""
    return list(yf.Search(ticker, max_results=0, news_count=20, enable_fuzzy_query=False, raise_errors=True).news or [])


# Wire services that distribute issuers' own press releases (not news services such as MT Newswires).
_PRESS_RELEASE_WIRE_RE = _re.compile(r"(?:access\s?newswire|accesswire|globe\s?newswire|business\s?wire|pr\s?newswire|newsfile|prweb|eqs(?:\s+newswire)?|cision)\b", _re.I | _re.A)


_YAHOO_PRESS_RELEASE_METHOD = "ISSUER_WIRE_RELEASES_IN_NEWS_FEED"
