"""News and public-event MCP tools.

Phase 3 pilot: extracted from server.py as a pure move (no behaviour change).
Covers the multi-source news/event collectors (Yahoo, GlobeNewswire, Finnhub,
Marketaux, SEC, company IR pages), their coverage/status helpers, and the tools
``get_company_news``, ``search_company_news``, ``get_company_press_releases``,
``get_sec_recent_events``, ``get_public_event_timeline`` and ``verify_company_event``.

Shared dependencies are imported from ``yfmcp.*``; this module never imports
``server`` (no import cycle). ``server.py`` re-exports the names that remaining
server code, grouped routing, scripts and tests still look up as ``server.<name>``.
"""

import asyncio
import datetime
import hashlib
import html as _html_module
import functools
import json
import math
import os
from pathlib import Path
import re as _re
import urllib.parse as _urlparse
import urllib.request as _urlrequest
import urllib.error as _urlerror
import xml.etree.ElementTree as _ET
from typing import Annotated

import yfinance as yf
from pydantic import Field

from yfmcp import url_policy as _url_policy
from yfmcp.app import yfinance_server
from yfmcp.schemas import _TOOL_OUTPUT_SCHEMAS
from yfmcp.envelope import ErrorCode, _mcp_failure
from yfmcp.validation import _validate_ticker
from yfmcp.cache import _tool_cache, TTL_NEWS
from yfmcp.util import _utc_now_iso, _to_iso_utc, _parse_rss_date
from yfmcp.clients.yahoo import _safe_parse
from yfmcp.clients.edgar import _edgar_get_html, _get_submissions_for_ticker
from yfmcp.clients.market_providers import _PROVIDER_USER_AGENT
from yfmcp.parsing.html import _parse_html_table, _strip_html_tags
from yfmcp import extraction_rules as _er
from yfmcp.deprecations import TOOL_RELIABILITY_CLASS as _TOOL_RELIABILITY_CLASS
from yfmcp.tools.pricing import _run_ticker_batch

# ---------------------------------------------------------------------------
# Phase 6B public event helpers (multi-source, source-backed, deduped)
# ---------------------------------------------------------------------------

_DEDUP_TITLE_MAX_LEN = 80
_STALE_EVENT_DAYS = 90
_PHASE6B_SUPPORTED_SOURCES = {
    "sec", "company_ir", "company_ir_page", "newswire",
    "yahoo_finance",                  # legacy: aggregates news + press releases
    "yahoo_finance_news",             # Yahoo Finance general news tab
    "yahoo_finance_press_releases",   # Yahoo Finance press releases tab
    "finnhub", "marketaux",
}
_OFFICIAL_SOURCE_TYPES = {"sec_filing", "company_ir", "company_ir_page", "press_release", "newswire", "yahoo_finance_press_releases"}
_SOURCE_PRIORITY = {
    "sec_filing": 0,
    "sec_ex99_found": 0,
    "company_ir": 1,
    "company_ir_page": 1,
    "press_release": 2,
    "yahoo_finance_press_releases": 2,
    "newswire": 3,
    "company_news": 4,
    "yahoo_finance": 5,
    "yahoo_finance_news": 5,
    "marketaux_wire": 6,
    "other": 7,
}


@functools.lru_cache(maxsize=1)
def _finnhub_policy() -> dict:
    """The deployed Finnhub market policy, read once per process."""
    return json.loads(_NEWS_SOURCE_CAPABILITIES_PATH.read_text(encoding="utf-8"))["providers"]["finnhub"]


def _finnhub_eligibility(ticker: str) -> tuple[bool, str | None]:
    policy = _finnhub_policy()
    ticker_u = ticker.upper()
    ineligible = ticker_u in set(policy.get("ineligibleTickers") or []) or any(
        ticker_u.endswith(str(suffix).upper()) for suffix in policy.get("ineligibleTickerSuffixes") or []
    )
    return (not ineligible, str(policy.get("reasonCode") or "") if ineligible else None)


def _evidence_class_for(source_type: object) -> str:
    source = str(source_type or "")
    if source in {"sec_filing", "sec_ex99_found"}:
        return "SEC_FILING"
    if source == "company_ir_page":
        return "APPROVED_IR_PAGE"
    if source == "company_ir":
        return "OFFICIAL_RSS_ATOM"
    if source in {"newswire", "press_release", "yahoo_finance_press_releases"}:
        return "WIRE_RELEASE"
    return "CONTEXTUAL_NEWS"


def _url_provenance_for(item: dict) -> str:
    source = str(item.get("sourceType") or "")
    if source in {"sec_filing", "sec_ex99_found", "company_ir", "company_ir_page"}:
        return "OFFICIAL"
    url = str(item.get("url") or "")
    if not url:
        return "UNKNOWN"
    host = _urlparse.urlparse(url).hostname or ""
    if host.lower().endswith(("finance.yahoo.com", "finnhub.io")):
        return "PROVIDER"
    return "PUBLISHER"


def _enrich_news_item_for_llm(item: dict) -> dict:
    enriched = dict(item)
    ticker = next((str(value).upper() for value in item.get("tickers") or [] if str(value).strip()), "")
    text = " ".join(str(item.get(key) or "") for key in ("title", "summary", "evidenceText")).upper()
    enriched["evidenceClass"] = _evidence_class_for(item.get("sourceType"))
    enriched["tickerMatch"] = "EXPLICIT" if item.get("matchBasis") in {"TICKER_TOKEN", "ISSUER_NAME", "ISSUER_ACRONYM", "PROVIDER_ENTITY_SYMBOL"} or (ticker and _re.search(rf"\b{_re.escape(ticker)}\b", text)) else ("SOURCE_SCOPED" if ticker else "UNVERIFIED")
    enriched["urlProvenance"] = _url_provenance_for(item)
    source_type = str(item.get("sourceType") or "")
    if source_type == "sec_ex99_found" or (source_type == "company_ir_page" and item.get("approvalStatus") == "approved"):
        enriched["decisionUse"] = "USE_OFFICIAL_EVIDENCE"
    elif source_type in {"sec_filing", "company_ir", "company_ir_page", "press_release", "yahoo_finance_press_releases", "newswire", "marketaux_wire"}:
        enriched["decisionUse"] = "CHECK_OFFICIAL_RELEASES"
    elif source_type in {"yahoo_finance_news", "company_news"}:
        enriched["decisionUse"] = _yahoo_decision_use(str(item.get("eventType") or "other"))
    else:
        enriched["decisionUse"] = "CONTEXT_ONLY"
    return enriched


def _build_coverage(source_status: dict, resolved_action: str | None = None) -> dict:
    failed_states = {
        "UNCONFIGURED", "PROVIDER_ERROR", "AUTH_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_BUDGET_EXHAUSTED", "PROVIDER_CHANGED",
        "IDENTITY_UNAVAILABLE",
        "WEBSITE_NOT_AVAILABLE", "DISCOVERY_NOT_FOUND", "DISCOVERY_BUDGET_EXHAUSTED", "FEED_NOT_FOUND",
        "PARSE_ERROR", "CANDIDATE_AVAILABLE", "NOT_REGISTERED", "DISABLED", "REVALIDATION_DUE", "SOURCE_VALIDATION_FAILED",
    }
    failed_sources: list[dict] = []
    skipped_sources: list[dict] = []
    truncated_sources: list[dict] = []
    for source, info in source_status.items():
        if not isinstance(info, dict):
            continue
        entry = {"source": source, "status": info.get("status"), "attempted": info.get("attempted") is not False, "reasonCode": info.get("reasonCode")}
        if info.get("status") in {"NOT_ELIGIBLE", "NOT_NEEDED"}:
            skipped_sources.append(entry)
        elif info.get("status") in failed_states:
            failed_sources.append(entry)
        if info.get("hasMoreAccepted") is True:
            truncated_sources.append({
                "source": source,
                "acceptedCount": int(info.get("acceptedCount") or 0),
                "returnedCount": int(info.get("returnedCount") or 0),
                "truncatedCount": int(info.get("truncatedCount") or 0),
            })
    coverage_limiting_skips = [row for row in skipped_sources if row["status"] != "NOT_NEEDED"]
    action = resolved_action or (
        "RETRY_RETRYABLE_SOURCES" if any(row["status"] in {"RATE_LIMITED", "TIMEOUT", "PROVIDER_BUDGET_EXHAUSTED"} for row in failed_sources) else (
            "CHECK_OFFICIAL_RELEASES" if failed_sources or coverage_limiting_skips else (
                "RETRY_TRUNCATED_SOURCE" if truncated_sources else "USE_RETURNED_CONTEXT"
            )
        )
    )
    return {
        "state": _compute_source_coverage(source_status),
        "failedSources": failed_sources,
        "skippedSources": skipped_sources,
        "truncatedSources": truncated_sources,
        "recommendedNextAction": action,
    }


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
            if len(alias) >= 3:
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
_NEWSWIRE_HINTS = ("businesswire", "globenewswire", "prnewswire")
_COMPANY_IR_URL_MARKERS = ("investor.", "investors.", "/investor", "/news-releases", "/press-release")
_YAHOO_ALLOWED_CONTENT_TYPES = {"STORY", "ARTICLE", "PRESS_RELEASE"}

_FINNHUB_NEWS_API = "https://finnhub.io/api/v1/company-news"
_MARKETAUX_NEWS_API = "https://api.marketaux.com/v1/news/all"
_MARKETAUX_WIRE_DOMAINS = ("businesswire.com", "prnewswire.com", "globenewswire.com")
_MARKETAUX_MAX_ITEMS = 3
_GLOBENEWSWIRE_RSS_FEEDS = (
    (
        "public_companies",
        "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/"
        "GlobeNewswire%20-%20News%20about%20Public%20Companies",
    ),
    (
        "press_releases",
        "https://www.globenewswire.com/RssFeed/subjectcode/72-Press%20Releases/"
        "feedTitle/GlobeNewswire%20-%20Press%20Releases",
    ),
    (
        "earnings",
        "https://www.globenewswire.com/RssFeed/subjectcode/"
        "13-Earnings%20Releases%20And%20Operating%20Results/feedTitle/"
        "GlobeNewswire%20-%20Earnings%20Releases%20And%20Operating%20Results",
    ),
    (
        "stock_market_news",
        "https://www.globenewswire.com/RssFeed/subjectcode/39-Stock%20Market%20News/"
        "feedTitle/GlobeNewswire%20-%20Stock%20Market%20News",
    ),
    (
        "technology",
        "https://www.globenewswire.com/RssFeed/industry/9000-Technology/feedTitle/"
        "GlobeNewswire%20-%20Industry%20News%20on%20Technology",
    ),
    (
        "semiconductors",
        "https://www.globenewswire.com/RssFeed/industry/9576-Semiconductors/feedTitle/"
        "GlobeNewswire%20-%20Industry%20News%20on%20Semiconductors",
    ),
    (
        "telecommunications",
        "https://www.globenewswire.com/RssFeed/industry/6000-Telecommunications/"
        "feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Telecommunications",
    ),
    (
        "mobile_telecommunications",
        "https://www.globenewswire.com/RssFeed/industry/6575-Mobile%20Telecommunications/"
        "feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Mobile%20Telecommunications",
    ),
    (
        "telecommunications_equipment",
        "https://www.globenewswire.com/RssFeed/industry/9578-Telecommunications%20Equipment/"
        "feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Telecommunications%20Equipment",
    ),
    (
        "electronic_equipment",
        "https://www.globenewswire.com/RssFeed/industry/2737-Electronic%20Equipment/"
        "feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Electronic%20Equipment",
    ),
)
_GLOBENEWSWIRE_RSS_URL = _GLOBENEWSWIRE_RSS_FEEDS[0][1]
_GLOBENEWSWIRE_MAX_BYTES = 2 * 1024 * 1024
_GLOBENEWSWIRE_BLOCKED_XML_MARKERS = ("<!doctype", "<!entity")
_GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN = "https://www.globenewswire.com/rss/stock"
_GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN = "https://www.globenewswire.com/rss/ISIN"
_COMPANY_IR_PAGE_REGISTRY_PATH = Path(__file__).resolve().parents[2] / "worker" / "src" / "company-ir-page-registry.json"

_NEWS_SOURCE_CAPABILITIES_PATH = Path(__file__).resolve().parents[2] / "worker" / "src" / "news-source-capabilities.json"
_COMPANY_IR_PAGE_MAX_BYTES = 750 * 1024


def _globenewswire_xml_is_safe(xml_content: str) -> bool:
    lowered = xml_content.lower()
    return not any(marker in lowered for marker in _GLOBENEWSWIRE_BLOCKED_XML_MARKERS)


def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].split(":", 1)[-1]


def _xml_child_texts(node: _ET.Element, local_name: str) -> list[str]:
    values: list[str] = []
    for child in list(node):
        if _xml_local_name(str(child.tag)) == local_name:
            text = "".join(child.itertext()).strip()
            if text:
                values.append(text)
    return values


def _xml_first_child_text(node: _ET.Element, local_name: str) -> str:
    values = _xml_child_texts(node, local_name)
    return values[0] if values else ""


def _globenewswire_category_values(rss_item: _ET.Element, domain: str) -> list[str]:
    values: list[str] = []
    domain_l = domain.lower()
    for child in list(rss_item):
        if _xml_local_name(str(child.tag)) != "category":
            continue
        if str(child.attrib.get("domain") or "").lower() != domain_l:
            continue
        text = "".join(child.itertext()).strip()
        for part in text.split(","):
            value = part.strip()
            if value:
                values.append(value)
    return values


def _globenewswire_stock_category_matches(ticker: str, stock_categories: list[str]) -> bool:
    ticker_u = ticker.upper()
    for category in stock_categories:
        symbol = category.rsplit(":", 1)[-1].strip().upper()
        if symbol == ticker_u:
            return True
    return False


def _globenewswire_plain_text(value: str) -> str:
    text = _re.sub(r"<[^>]+>", " ", value or "")
    text = _html_module.unescape(text)
    return _re.sub(r"\s+", " ", text).strip()


def _coerce_max_results(value: int, default_value: int) -> int:
    return min(max(1, int(value or default_value)), 100)


def _coerce_lookback_days(value: int, default_value: int) -> int:
    return min(max(1, int(value or default_value)), 3650)


def _normalize_event_sources(sources: list[str] | None, default_sources: list[str]) -> tuple[list[str], list[dict]]:
    warnings: list[dict] = []
    source_list = [str(s).strip().lower() for s in (default_sources if sources is None else sources) if str(s).strip()]
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
    # An explicit list is used as given (Q3h): [] or only unsupported names selects no source.
    return normalized, warnings


# Default source sets the news tools apply when ``sources`` is omitted or None (Q3h).
# An explicit list, including [], is used as given: [] attempts no source.
_DEFAULT_NEWS_SOURCES = ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"]
_DEFAULT_PRESS_RELEASE_SOURCES = ["sec", "company_ir_page", "yahoo_finance_press_releases"]
_DEFAULT_EVENT_SOURCES = [
    "sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux",
]
_GET_COMPANY_NEWS_SOURCES_DESCRIPTION = (
    "Allowed sources: yahoo_finance_news, yahoo_finance_press_releases, finnhub, marketaux, sec, newswire, company_ir, company_ir_page, or legacy yahoo_finance. Marketaux is quota-aware fallback by default; explicit sources=['marketaux'] forces one bounded request. Omit for the default set; an explicit list is used as given, and [] attempts no source. requestedSources, defaultSourcesApplied and attemptedSources report what was asked for and tried."
)
_SOURCES_DEFAULT_DESCRIPTION = (
    'Omit for the default set; an explicit list is used as given, and [] attempts no source. requestedSources, defaultSourcesApplied and attemptedSources report what was asked for and tried.'
)
_NOT_ATTEMPTED_SOURCE_STATUSES = frozenset({"NOT_ELIGIBLE", "NOT_NEEDED", "UNCONFIGURED"})


def _source_request_fields(requested_sources: list[str] | None, source_status: dict) -> dict:
    """What the caller asked for and what was actually tried (Q3h); mirrors Worker sourceRequestFields."""
    attempted: list[dict] = []
    for source, info in source_status.items():
        if not isinstance(info, dict):
            continue
        status = str(info.get("status") or "")
        if info.get("attempted") is False or status in _NOT_ATTEMPTED_SOURCE_STATUSES:
            continue
        attempted.append({"sourceType": source, "status": status})
    requested = None if requested_sources is None else [str(s) for s in requested_sources]
    return {
        "requestedSources": requested,
        "defaultSourcesApplied": requested is None,
        "attemptedSources": attempted,
    }


def _mark_no_source_selected(payload: dict, requested_sources: list[str] | None) -> dict:
    """An explicit sources list selecting no supported source attempts nothing; reads as source-limited (Q3h)."""
    if requested_sources is None or _normalize_event_sources(requested_sources, [])[0]:
        return payload
    payload["status"] = "SOURCE_LIMITED_NOT_FOUND"
    payload["sourceCoverage"] = "PARTIAL"
    if isinstance(payload.get("coverage"), dict):
        payload["coverage"] = {**payload["coverage"], "state": "PARTIAL", "recommendedNextAction": "CHECK_OFFICIAL_RELEASES"}
    return payload


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


def _xml_text(root: _ET.Element, path: str) -> str | None:
    found = root.find(path)
    if found is not None and found.text:
        text = found.text.strip()
        return text or None
    return None


def _strip_xml_namespaces(xml_text: str) -> str:
    return _re.sub(r'\sxmlns(:\w+)?="[^"]*"', "", xml_text)


_FORM4_TRANSACTION_LABELS = {
    "P": "Purchase",
    "S": "Sale",
    "A": "Grant/Award",
    "D": "Disposition",
    "M": "Option exercise/conversion",
    "F": "Tax withholding/payment",
    "G": "Gift",
}


def _form4_num(value: str | None) -> float | None:
    if value is None:
        return None
    text = _strip_html_tags(value)
    match = _re.search(r"-?\$?\s*\(?\d[\d,]*(?:\.\d+)?\)?", text)
    if not match:
        return None
    s = match.group(0).replace("$", "").replace(",", "").replace(" ", "")
    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    try:
        n = float(s)
    except Exception:
        return None
    return -n if negative else n


def _form4_date(value: str | None) -> str | None:
    text = _strip_html_tags(value or "")
    if not text:
        return None
    if _re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    m = _re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text)
    if not m:
        return text
    month, day, year = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return f"{year:04d}-{month:02d}-{day:02d}"


def _form4_owner_from_html(html_text: str) -> str | None:
    m = _re.search(
        r"Name and Address of Reporting Person[\s\S]*?<a\b[^>]*>([\s\S]*?)</a>",
        html_text,
        _re.IGNORECASE,
    )
    return _strip_html_tags(m.group(1)) if m else None


def _form4_role_from_html(html_text: str) -> str | None:
    m = _re.search(
        r"Relationship of Reporting Person\(s\) to Issuer([\s\S]*?)Individual or Joint/Group Filing",
        html_text,
        _re.IGNORECASE,
    )
    if not m:
        return None
    roles: list[str] = []
    for row in _parse_html_table(m.group(1)):
        for i, cell in enumerate(row[:-1]):
            if cell.strip().upper() == "X":
                label = row[i + 1].strip().lower()
                if "director" in label:
                    roles.append("director")
                elif "officer" in label:
                    roles.append("officer")
                elif "10%" in label or "owner" in label:
                    roles.append("ten_percent_owner")
                elif "other" in label:
                    roles.append("other")
    return ", ".join(dict.fromkeys(roles)) or None


def _parse_form4_html_transaction(html_text: str) -> dict | None:
    owner_name = _form4_owner_from_html(html_text)
    role = _form4_role_from_html(html_text)
    for table_m in _re.finditer(r"<table[^>]*>([\s\S]*?)</table>", html_text, _re.IGNORECASE):
        table_html = table_m.group(0)
        table_text = _strip_html_tags(table_html).lower()
        if "non-derivative securities" in table_text:
            for row in _parse_html_table(table_html):
                if len(row) < 10:
                    continue
                code = row[3].strip().upper()
                shares = _form4_num(row[5])
                if not code or shares is None:
                    continue
                price = _form4_num(row[7])
                return _form4_transaction_payload(
                    owner_name=owner_name,
                    role=role,
                    code=code,
                    shares=shares,
                    price=price,
                    ownership_form=row[9].strip() or None,
                    transaction_date=_form4_date(row[1]),
                )
        if "derivative securities" in table_text:
            for row in _parse_html_table(table_html):
                if len(row) < 15:
                    continue
                code = row[4].strip().upper()
                shares = _form4_num(row[6])
                if not code or shares is None:
                    continue
                return _form4_transaction_payload(
                    owner_name=owner_name,
                    role=role,
                    code=code,
                    shares=shares,
                    price=_form4_num(row[1]),
                    ownership_form=row[14].strip() or None,
                    transaction_date=_form4_date(row[2]),
                )
    return None


def _form4_transaction_payload(
    *,
    owner_name: str | None,
    role: str | None,
    code: str | None,
    shares: float | None,
    price: float | None,
    ownership_form: str | None,
    transaction_date: str | None,
) -> dict:
    return {
        "owner": owner_name,
        "role": role,
        "transactionCode": code,
        "transactionLabel": _FORM4_TRANSACTION_LABELS.get(str(code or "").upper(), "Other/Unclassified"),
        "shares": shares,
        "price": price,
        "value": round(shares * price, 2) if shares is not None and price is not None else None,
        "ownershipForm": ownership_form,
        "transactionDate": transaction_date,
    }


def _parse_form4_transaction(xml_text: str) -> dict | None:
    try:
        root = _ET.fromstring(_strip_xml_namespaces(xml_text))
    except Exception:
        return _parse_form4_html_transaction(xml_text)
    owner_name = _xml_text(root, ".//reportingOwnerId/rptOwnerName")
    officer_title = _xml_text(root, ".//reportingOwnerRelationship/officerTitle")
    roles: list[str] = []
    for flag, label in (
        ("isDirector", "director"),
        ("isOfficer", "officer"),
        ("isTenPercentOwner", "ten_percent_owner"),
        ("isOther", "other"),
    ):
        if (_xml_text(root, f".//reportingOwnerRelationship/{flag}") or "").lower() in {"1", "true"}:
            roles.append(label)
    tx = root.find(".//nonDerivativeTransaction")
    if tx is None:
        tx = root.find(".//derivativeTransaction")
    if tx is None:
        return None
    code = _xml_text(tx, ".//transactionCoding/transactionCode")
    shares_text = _xml_text(tx, ".//transactionAmounts/transactionShares/value")
    price_text = _xml_text(tx, ".//transactionAmounts/transactionPricePerShare/value")
    shares = _form4_num(shares_text)
    price = _form4_num(price_text)
    return _form4_transaction_payload(
        owner_name=owner_name,
        role=officer_title or ", ".join(roles) or None,
        code=code,
        shares=shares,
        price=price,
        ownership_form=_xml_text(tx, ".//ownershipNature/directOrIndirectOwnership/value"),
        transaction_date=_form4_date(_xml_text(tx, ".//transactionDate/value")),
    )


async def _try_attach_form4_transaction(item: dict, filing: dict, warnings: list[dict]) -> None:
    if str(filing.get("filingType") or "").upper() != "4":
        return
    url = _safe_sec_url(item.get("url"))
    if not url:
        warnings.append({
            "code": "FORM4_PARSE_UNAVAILABLE",
            "message": "Form 4 primary document URL is unavailable.",
            "severity": "warning",
        })
        return
    xml_text = await _edgar_get_html(url, max_bytes=2_000_000)
    parsed = _parse_form4_transaction(xml_text or "")
    if not parsed:
        warnings.append({
            "code": "FORM4_PARSE_UNAVAILABLE",
            "message": "Form 4 transaction details could not be parsed from the primary document.",
            "severity": "warning",
        })
        return
    item["insiderTransaction"] = parsed
    label = parsed.get("transactionLabel") or "Insider transaction"
    owner = parsed.get("owner") or "reporting owner"
    shares = parsed.get("shares")
    value = parsed.get("value")
    value_part = f", value ${value:,.0f}" if isinstance(value, (int, float)) else ""
    shares_part = f"{shares:,.0f} shares" if isinstance(shares, (int, float)) else "shares unavailable"
    item["title"] = f"Form 4: {label} by {owner}"
    item["summary"] = _short_text(f"{label} by {owner}: {shares_part}{value_part}.")
    item["evidenceText"] = _short_text(f"SEC Form 4 transaction code {parsed.get('transactionCode') or 'unknown'} on {parsed.get('transactionDate') or item.get('filingDate')}.")


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


def _load_company_ir_page_registry() -> dict:
    try:
        return json.loads(_COMPANY_IR_PAGE_REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"schemaVersion": "unknown", "registryVersion": "unknown", "sources": []}


def _company_ir_page_entry(ticker: str) -> dict | None:
    ticker_u = str(ticker or "").upper()
    registry = _load_company_ir_page_registry()
    for entry in registry.get("sources") or []:
        if isinstance(entry, dict) and str(entry.get("ticker") or "").upper() == ticker_u:
            return dict(entry, registryVersion=registry.get("registryVersion"), registrySchemaVersion=registry.get("schemaVersion"))
    return None


def _host_without_www(host: str) -> str:
    return str(host or "").lower().removeprefix("www.")


def _company_ir_page_url_allowed(entry: dict, raw_url: str) -> bool:
    try:
        parsed = _urlparse.urlparse(str(raw_url or ""))
    except Exception:
        return False
    if parsed.scheme.lower() != "https":
        return False
    host = _host_without_www(parsed.hostname or "")
    allowed_hosts = {_host_without_www(h) for h in entry.get("allowedHosts") or []}
    prefixes = [str(p) for p in entry.get("allowedPathPrefixes") or [] if str(p)]
    return host in allowed_hosts and any(parsed.path.startswith(prefix) for prefix in prefixes)


def _company_ir_page_registry_fresh(entry: dict) -> bool:
    raw = entry.get("revalidateAfter")
    if not raw:
        return True
    try:
        deadline = datetime.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=datetime.timezone.utc)
        return deadline >= datetime.datetime.now(datetime.timezone.utc)
    except Exception:
        return False


def _company_ir_page_item(
    ticker: str,
    entry: dict,
    title: str,
    summary: str,
    raw_url: str | None,
    published_at: str | None,
    retrieved_at: str,
    final_url: str,
) -> dict | None:
    url = _urlparse.urljoin(final_url, raw_url or final_url)
    if not _company_ir_page_url_allowed(entry, url):
        return None
    ticker_u = ticker.upper()
    issuer = str(entry.get("issuerName") or ticker_u)
    duplicate_group_id = _make_duplicate_group_id(ticker_u, title, published_at, issuer, url)
    return {
        "title": title,
        "source": "company_ir_page",
        "originalSource": issuer,
        "sourceType": "company_ir_page",
        "provider": "company_ir_page_registry",
        "discoveredVia": "approved_company_ir_page_registry",
        "adapter": entry.get("adapter"),
        "canonicalUrl": entry.get("canonicalUrl"),
        "registryVersion": entry.get("registryVersion"),
        "approvalStatus": "approved",
        "publishedAt": published_at,
        "retrievedAt": retrieved_at,
        "url": url,
        "issuer": issuer,
        "tickers": [ticker_u],
        "eventType": _event_type_from_keywords(f"{title} {summary}"),
        "summary": _short_text(summary or title, 240),
        "evidenceText": _short_text(summary or title, 180),
        "confidence": "HIGH",
        "tickerRelevance": "HIGH",
        "duplicateGroupId": duplicate_group_id,
    }


def _company_ir_page_json_records(payload: object) -> list[dict]:
    if not isinstance(payload, dict):
        return []
    candidates = [
        payload.get("items"),
        payload.get("releases"),
        payload.get("pressReleases"),
        payload.get("news"),
        payload.get("data"),
    ]
    feed = payload.get("feed")
    if isinstance(feed, dict):
        candidates.append(feed.get("items"))
    data = payload.get("data")
    if isinstance(data, dict):
        candidates.extend(data.values())
    for candidate in candidates:
        if isinstance(candidate, list):
            records = [r for r in candidate if isinstance(r, dict)]
            if records:
                return records
    return []


def _parse_company_ir_page_json(ticker: str, entry: dict, text: str, retrieved_at: str, final_url: str) -> list[dict]:
    try:
        payload = json.loads(text)
    except Exception:
        return []
    items: list[dict] = []
    for raw in _company_ir_page_json_records(payload):
        title = str(raw.get("title") or raw.get("headline") or raw.get("name") or "").strip()
        if not title:
            continue
        summary = str(raw.get("summary") or raw.get("description") or raw.get("excerpt") or raw.get("content") or "").strip()
        raw_url = str(raw.get("url") or raw.get("link") or raw.get("external_url") or raw.get("permalink") or "").strip() or None
        published_at = _to_iso_utc(raw.get("publishedAt") or raw.get("published_at") or raw.get("datePublished") or raw.get("date_published") or raw.get("date") or raw.get("pubDate"))
        item = _company_ir_page_item(ticker, entry, title, summary, raw_url, published_at, retrieved_at, final_url)
        if item:
            items.append(item)
    return items


def _html_attr(attrs: str, name: str) -> str:
    match = _re.search(rf'\b{_re.escape(name)}\s*=\s*["\']([^"\']+)["\']', attrs or "", flags=_re.I)
    return _html_module.unescape(match.group(1)) if match else ""


def _parse_company_ir_page_html(ticker: str, entry: dict, html_text: str, retrieved_at: str, final_url: str) -> list[dict]:
    blocks = _re.findall(r"<article\b[^>]*>[\s\S]*?</article>", html_text or "", flags=_re.I)
    if not blocks:
        blocks = [
            m.group(0)
            for m in _re.finditer(r"<li\b[^>]*>[\s\S]*?</li>", html_text or "", flags=_re.I)
            if _re.search(r"press release|news release|earnings|results|announces|reports", _strip_html_tags(m.group(0)), flags=_re.I)
        ]
    items: list[dict] = []
    for block in blocks[:50]:
        heading = _re.search(r"<h[1-4]\b[^>]*>([\s\S]*?)</h[1-4]>", block, flags=_re.I)
        anchor = _re.search(r"<a\b([^>]*)>([\s\S]*?)</a>", block, flags=_re.I)
        title = _strip_html_tags((heading.group(1) if heading else anchor.group(2) if anchor else "") or "").strip()
        if len(title) < 6:
            continue
        href = _html_attr(anchor.group(1), "href") if anchor else ""
        time_match = _re.search(r"<time\b([^>]*)>([\s\S]*?)</time>", block, flags=_re.I)
        datetime_raw = ""
        if time_match:
            datetime_raw = _html_attr(time_match.group(1), "datetime") or _strip_html_tags(time_match.group(2))
        item = _company_ir_page_item(
            ticker,
            entry,
            title,
            _strip_html_tags(block),
            href or None,
            _to_iso_utc(datetime_raw),
            retrieved_at,
            final_url,
        )
        if item:
            items.append(item)
    return items


async def _fetch_company_ir_page(entry: dict) -> tuple[str, str, str]:
    canonical_url = str(entry.get("canonicalUrl") or "")
    if not _company_ir_page_url_allowed(entry, canonical_url):
        raise ValueError("registry canonical URL is outside its allow-list")
    if str(entry.get("status") or "") != "approved":
        # Only approved entries are fetched. The Worker reaches the same refusal because the fetch allow-list
        # holds the hosts of approved entries only.
        raise _url_policy.UrlPolicyError("URL host not allowed")

    def _fetch() -> tuple[str, str, str]:
        req = _urlrequest.Request(
            canonical_url,
            headers={
                "User-Agent": "yahoo-finance-mcp/ir-page-registry",
                "Accept": "application/json, application/xml, text/xml, text/html;q=0.9",
            },
        )
        with _url_policy.urlopen(req, timeout=15) as resp:
            final_url = resp.geturl()
            if not _company_ir_page_url_allowed(entry, final_url):
                raise ValueError("redirect target is outside registry allow-list")
            content_type = str(resp.headers.get("content-type") or "").lower()
            data = resp.read(_COMPANY_IR_PAGE_MAX_BYTES + 1)
            if len(data) > _COMPANY_IR_PAGE_MAX_BYTES:
                raise ValueError("response exceeded size limit")
            return data.decode("utf-8", errors="replace"), final_url, content_type

    return await asyncio.to_thread(_fetch)


async def _collect_company_ir_page_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
) -> tuple[list[dict], list[dict], bool]:
    entry = _company_ir_page_entry(ticker)
    if not entry:
        return [], [], False
    status = str(entry.get("status") or "")
    warning_base = {
        "canonicalUrl": entry.get("canonicalUrl"),
        "registryVersion": entry.get("registryVersion"),
        "approvalStatus": status,
    }
    if status == "disabled":
        return [], [{**warning_base, "code": "COMPANY_IR_PAGE_DISABLED", "message": "Company IR page registry entry is disabled.", "severity": "info"}], False
    if status == "candidate":
        candidate = {
            "ticker": entry.get("ticker"),
            "issuerName": entry.get("issuerName"),
            "canonicalUrl": entry.get("canonicalUrl"),
            "adapter": entry.get("adapter"),
            "candidateReason": entry.get("candidateReason"),
            "discoveredAt": entry.get("discoveredAt"),
            "decisionGrade": False,
        }
        return [], [{**warning_base, "code": "COMPANY_IR_PAGE_CANDIDATE_AVAILABLE", "message": "Company IR page candidate is available for review but was not fetched.", "severity": "info", "candidate": candidate}], False
    if not _company_ir_page_registry_fresh(entry):
        return [], [{**warning_base, "code": "COMPANY_IR_PAGE_REVALIDATION_DUE", "message": "Approved company IR page is past its revalidation date; skipped until reviewed.", "severity": "warning"}], False
    try:
        text, final_url, content_type = await _fetch_company_ir_page(entry)
        adapter = str(entry.get("adapter") or "")
        if adapter == "structured" or "json" in content_type:
            items = _parse_company_ir_page_json(ticker, entry, text, retrieved_at, final_url)
        else:
            items = _parse_company_ir_page_html(ticker, entry, text, retrieved_at, final_url)
        items = [
            item for item in items
            if _within_date_window(item.get("publishedAt"), start_date=start_date, end_date=end_date, lookback_days=lookback_days)
        ][:max_results]
        return items, [], True
    except Exception as exc:
        return [], [{**warning_base, "code": "COMPANY_IR_PAGE_VALIDATION_FAILED", "message": f"Approved company IR page validation failed: {exc}", "severity": "warning"}], False


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
    text_blob = f"{title} {summary}".upper()
    ticker_relevance = "HIGH" if ticker_u in text_blob else "LOW"
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
        await _try_attach_form4_transaction(item, filing_obj, item_warnings)
        if not _within_date_window(item.get("publishedAt"), start_date=start_date, end_date=end_date, lookback_days=lookback_days):
            continue
        events.append(item)
        warnings.extend(item_warnings)
        if len(events) >= max_results:
            break
    return events, warnings, True


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
        match = _yahoo_news_match_for(
            f"{item.get('title') or ''} {item.get('summary') or ''} {item.get('evidenceText') or ''}",
            ticker,
            identity,
        )
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


async def _collect_globenewswire_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
    filter_low_relevance: bool = True,
) -> tuple[list[dict], list[dict], bool]:
    """Fetch GlobeNewswire RSS feeds and return exact stock-category matches.

    Items are labelled with ``source="newswire"``, ``sourceType="newswire"``,
    ``originalSource="GlobeNewswire"``, ``provider="globenewswire"``, and
    ``discoveredVia="globenewswire_rss"``. Feeds are cached independently for
    :data:`TTL_NEWS` seconds. Ticker relevance is established only from
    GlobeNewswire RSS stock-category metadata such as ``Nasdaq:NVDA``.
    """
    warnings: list[dict] = []
    items: list[dict] = []
    ticker_u = ticker.upper()
    seen_keys: set[str] = set()
    parsed_any_feed = False

    for feed_name, feed_url in _GLOBENEWSWIRE_RSS_FEEDS:
        cache_key = f"gnw_rss:{feed_name}"
        xml_content: str | None = None
        cached_entry = _tool_cache.get(cache_key)
        if cached_entry is not None:
            xml_content = cached_entry[0]

        if xml_content is None:
            loop = asyncio.get_event_loop()

            def _fetch_rss() -> str:
                req = _urlrequest.Request(
                    feed_url,
                    headers={"User-Agent": _PROVIDER_USER_AGENT},
                )
                with _url_policy.urlopen(req, timeout=20) as resp:  # noqa: S310
                    raw = resp.read(_GLOBENEWSWIRE_MAX_BYTES + 1)
                    if len(raw) > _GLOBENEWSWIRE_MAX_BYTES:
                        raise ValueError("GlobeNewswire RSS response exceeded size limit")
                    return raw.decode("utf-8", errors="replace")

            try:
                xml_content = await loop.run_in_executor(None, _url_policy.in_context(_fetch_rss))
                _tool_cache.set(cache_key, xml_content, TTL_NEWS)
            except Exception as exc:
                warnings.append({
                    "code": "SOURCE_UNAVAILABLE",
                    "message": f"GlobeNewswire RSS feed '{feed_name}' unavailable: {exc}",
                    "severity": "warning",
                })
                continue

        try:
            if not _globenewswire_xml_is_safe(xml_content):
                raise ValueError("unsupported XML declaration in GlobeNewswire RSS")
            root = _ET.fromstring(xml_content)  # noqa: S314 (trusted source)
            channel = root.find("channel") if _xml_local_name(str(root.tag)) != "channel" else root
            rss_items = channel.findall("item") if channel is not None else []
            parsed_any_feed = True
        except Exception as exc:
            warnings.append({
                "code": "SOURCE_UNAVAILABLE",
                "message": f"GlobeNewswire RSS feed '{feed_name}' parse error: {exc}",
                "severity": "warning",
            })
            continue

        for rss_item in rss_items:
            stock_categories = _globenewswire_category_values(
                rss_item,
                _GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN,
            )
            if not _globenewswire_stock_category_matches(ticker_u, stock_categories):
                continue
            relevance = "HIGH"

            title = _xml_first_child_text(rss_item, "title").strip()
            description_raw = _xml_first_child_text(rss_item, "description").strip()
            description = _globenewswire_plain_text(description_raw)
            link = _xml_first_child_text(rss_item, "link").strip() or None
            guid = _xml_first_child_text(rss_item, "guid").strip() or None
            published_at = _parse_rss_date(_xml_first_child_text(rss_item, "pubDate").strip())
            isin_values = _globenewswire_category_values(rss_item, _GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN)
            keywords = _xml_child_texts(rss_item, "keyword")
            subject = _xml_first_child_text(rss_item, "subject").strip() or None
            language = _xml_first_child_text(rss_item, "language").strip() or None
            issuer = _xml_first_child_text(rss_item, "contributor").strip() or None
            globenewswire_id = _xml_first_child_text(rss_item, "identifier").strip() or None

            if not _within_date_window(
                published_at,
                start_date=start_date,
                end_date=end_date,
                lookback_days=lookback_days,
            ):
                continue

            if not published_at:
                warnings.append({
                    "code": "PUBLISHED_AT_UNAVAILABLE",
                    "message": "Published timestamp unavailable for GlobeNewswire item.",
                    "severity": "warning",
                })

            duplicate_group_id = _make_duplicate_group_id(
                ticker_u,
                title,
                published_at,
                issuer,
                link or guid,
            )
            if duplicate_group_id is None:
                warnings.append({
                    "code": "DEDUPE_WEAK_KEY",
                    "message": "Weak dedupe key for at least one GlobeNewswire item.",
                    "severity": "warning",
                })

            dedupe_key = duplicate_group_id or f"{feed_name}:{link or guid}:{title}:{published_at}"
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)

            item = {
                "title": title,
                "source": "newswire",
                "originalSource": "GlobeNewswire",
                "sourceType": "newswire",
                "provider": "globenewswire",
                "discoveredVia": "globenewswire_rss",
                "publishedAt": published_at,
                "retrievedAt": retrieved_at,
                "url": link or guid,
                "issuer": issuer,
                "tickers": [ticker_u],
                "eventType": _event_type_from_keywords(f"{title} {description} {subject or ''}"),
                "summary": _short_text(description or title, 240),
                "evidenceText": _short_text(description or title, 180),
                "confidence": "HIGH" if (relevance == "HIGH" and (link or guid)) else "MEDIUM",
                "tickerRelevance": relevance,
                "duplicateGroupId": duplicate_group_id,
                "stockCategories": stock_categories,
                "feedSource": feed_name,
            }
            if isin_values:
                item["isin"] = isin_values[0]
            if subject:
                item["subject"] = subject
            if keywords:
                item["keywords"] = keywords
            if language:
                item["language"] = language
            if globenewswire_id:
                item["globenewswireId"] = globenewswire_id
            items.append(item)
            if len(items) >= max_results:
                return items, warnings, True

    if warnings and not items:
        return items, warnings, False
    return items, warnings, parsed_any_feed


async def _collect_finnhub_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
) -> tuple[list[dict], list[dict], bool, dict]:
    warnings: list[dict] = []
    items: list[dict] = []
    diagnostics = {
        "rawCount": 0,
        "retrievedCount": 0,
        "acceptedCount": 0,
        "collectedCount": 0,
        "rejectedCount": 0,
        "rejectionCounts": {},
        "attempted": True,
        "completed": False,
    }

    def _reject(reason: str) -> None:
        diagnostics["rejectedCount"] += 1
        diagnostics["rejectionCounts"][reason] = diagnostics["rejectionCounts"].get(reason, 0) + 1

    api_key = os.environ.get("FINNHUB_API_KEY") or os.environ.get("FINNHUB_TOKEN")
    if not api_key:
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub company-news source is not configured; skipped.",
            "severity": "warning",
        })
        diagnostics["attempted"] = False
        return items, warnings, False, diagnostics

    from_day = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=lookback_days or 14)
    ).strftime("%Y-%m-%d")
    to_day = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    query = _urlparse.urlencode({
        "symbol": ticker.upper(),
        "from": from_day,
        "to": to_day,
    })
    url = f"{_FINNHUB_NEWS_API}?{query}"
    loop = asyncio.get_event_loop()

    def _fetch() -> list[dict]:
        req = _urlrequest.Request(
            url,
            headers={"User-Agent": _PROVIDER_USER_AGENT, "X-Finnhub-Token": api_key},
        )
        with _url_policy.urlopen(req, timeout=20) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    try:
        raw_items = await loop.run_in_executor(None, _url_policy.in_context(_fetch))
    except _urlerror.HTTPError as exc:
        if exc.code in (401, 403):
            warnings.append({
                "code": "SOURCE_UNAVAILABLE",
                "message": f"Finnhub auth error: HTTP {exc.code}",
                "severity": "warning",
            })
        elif exc.code == 429:
            warnings.append({
                "code": "SOURCE_UNAVAILABLE",
                "message": "Finnhub rate limited: HTTP 429",
                "severity": "warning",
            })
        else:
            warnings.append({
                "code": "SOURCE_UNAVAILABLE",
                "message": f"Finnhub source unavailable: {exc}",
                "severity": "warning",
            })
        return items, warnings, False, diagnostics
    except Exception as exc:
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": f"Finnhub source unavailable: {exc}",
            "severity": "warning",
        })
        return items, warnings, False, diagnostics

    if not isinstance(raw_items, list):
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub provider changed: unexpected response format",
            "severity": "warning",
        })
        return items, warnings, False, diagnostics

    diagnostics["rawCount"] = len(raw_items)
    diagnostics["retrievedCount"] = len(raw_items)
    ticker_u = ticker.upper()
    for row in raw_items:
        if not isinstance(row, dict):
            _reject("INVALID_ROW")
            continue
        title = str(row.get("headline") or "").strip()
        if not title:
            _reject("MISSING_HEADLINE")
            continue
        summary = str(row.get("summary") or "").strip()
        original_source = str(row.get("source") or "").strip() or None
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
            "source": "finnhub",
            "originalSource": original_source,
            "sourceType": "company_news",
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
            _reject("DATE_OUTSIDE_WINDOW")
            continue
        diagnostics["acceptedCount"] += 1
        if len(items) < max_results:
            items.append(item)
    diagnostics["collectedCount"] = len(items)
    diagnostics["completed"] = True
    return items, warnings, True, diagnostics


async def _collect_marketaux_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
    fallback_reason: str = "EXPLICIT_SOURCE_SELECTION",
    search_query: str = "",
    expected_exchange: str | None = None,
) -> tuple[list[dict], list[dict], bool, dict]:
    """Fetch one bounded Marketaux page from major wire domains.

    Marketaux is a contextual aggregator, not official evidence. The free-tier
    request is deliberately capped at three rows and never paginated.
    """
    warnings: list[dict] = []
    items: list[dict] = []
    limit = min(_MARKETAUX_MAX_ITEMS, max(1, int(max_results)))
    diagnostics = {
        "status": "PROVIDER_ERROR",
        "rawCount": 0,
        "retrievedCount": 0,
        "acceptedCount": 0,
        "collectedCount": 0,
        "rejectedCount": 0,
        "rejectionCounts": {},
        "attempted": True,
        "completed": False,
        "fallbackReason": fallback_reason,
        "domains": list(_MARKETAUX_WIRE_DOMAINS),
        "quotaPolicy": {
            "dailyLimit": 100,
            "maxArticlesPerRequest": _MARKETAUX_MAX_ITEMS,
            "pagesRequested": 1,
            "automaticPagination": False,
        },
    }

    def _reject(reason: str) -> None:
        diagnostics["rejectedCount"] += 1
        diagnostics["rejectionCounts"][reason] = diagnostics["rejectionCounts"].get(reason, 0) + 1

    api_token = os.environ.get("MARKETAUX_API_TOKEN") or os.environ.get("MARKETAUX_API_KEY")
    if not api_token:
        diagnostics.update({"status": "UNCONFIGURED", "attempted": False})
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Marketaux wire fallback is not configured; set MARKETAUX_API_TOKEN to enable it.",
            "severity": "warning",
            "source": "marketaux",
        })
        return items, warnings, False, diagnostics

    now = datetime.datetime.now(datetime.timezone.utc)
    from_day = start_date or (now - datetime.timedelta(days=lookback_days or 14)).strftime("%Y-%m-%d")
    to_day = end_date or now.strftime("%Y-%m-%d")
    safe_params = {
        "symbols": ticker.upper(),
        "filter_entities": "true",
        "language": "en",
        "domains": ",".join(_MARKETAUX_WIRE_DOMAINS),
        "published_after": f"{from_day}T00:00:00",
        "published_before": f"{to_day}T23:59:59",
        "limit": str(limit),
        "page": "1",
    }
    if str(search_query or "").strip():
        safe_params["search"] = str(search_query).strip()
    public_url = f"{_MARKETAUX_NEWS_API}?{_urlparse.urlencode({**safe_params, 'api_token': 'REDACTED'})}"
    request_url = f"{_MARKETAUX_NEWS_API}?{_urlparse.urlencode({**safe_params, 'api_token': api_token})}"
    diagnostics["providerUrl"] = public_url
    loop = asyncio.get_event_loop()

    def _fetch() -> dict:
        req = _urlrequest.Request(request_url, headers={"User-Agent": _PROVIDER_USER_AGENT})
        with _url_policy.urlopen(req, timeout=12) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    try:
        payload = await loop.run_in_executor(None, _url_policy.in_context(_fetch))
    except _urlerror.HTTPError as exc:
        status = "AUTH_ERROR" if exc.code in (401, 403) else "RATE_LIMITED" if exc.code in (402, 429) else "PROVIDER_ERROR"
        diagnostics["status"] = status
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": f"Marketaux wire fallback returned HTTP {exc.code} ({status}).",
            "severity": "warning",
            "source": "marketaux",
        })
        return items, warnings, False, diagnostics
    except Exception as exc:
        message = str(exc).replace(api_token, "REDACTED")
        status = "TIMEOUT" if any(token in message.lower() for token in ("timeout", "timed out")) else "PROVIDER_ERROR"
        diagnostics["status"] = status
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": f"Marketaux wire fallback unavailable ({status}).",
            "severity": "warning",
            "source": "marketaux",
        })
        return items, warnings, False, diagnostics

    raw_items = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(raw_items, list):
        diagnostics["status"] = "PROVIDER_CHANGED"
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Marketaux provider changed: expected a data array.",
            "severity": "warning",
            "source": "marketaux",
        })
        return items, warnings, False, diagnostics

    diagnostics["rawCount"] = len(raw_items)
    diagnostics["retrievedCount"] = len(raw_items)
    ticker_u = ticker.upper()
    for row in raw_items:
        if not isinstance(row, dict):
            _reject("INVALID_ROW")
            continue
        title = str(row.get("title") or "").strip()
        item_url = str(row.get("url") or "").strip()
        if not title:
            _reject("MISSING_HEADLINE")
            continue
        if not item_url.startswith("https://"):
            _reject("INVALID_URL")
            continue
        host = (_urlparse.urlparse(item_url).hostname or "").lower()
        if not any(host == domain or host.endswith(f".{domain}") for domain in _MARKETAUX_WIRE_DOMAINS):
            _reject("DOMAIN_NOT_ALLOWED")
            continue
        entities = row.get("entities") if isinstance(row.get("entities"), list) else []
        matched_entity = next((
            entity for entity in entities
            if isinstance(entity, dict) and str(entity.get("symbol") or "").upper() == ticker_u
        ), None)
        if matched_entity is None:
            _reject("ENTITY_SYMBOL_MISMATCH")
            continue
        expected_exchange_norm = _normalize_exchange(expected_exchange)
        provider_exchange_norm = _normalize_exchange(str(matched_entity.get("exchange") or ""))
        if expected_exchange_norm and provider_exchange_norm and expected_exchange_norm != provider_exchange_norm:
            _reject("ENTITY_EXCHANGE_MISMATCH")
            continue
        published_at = _to_iso_utc(row.get("published_at"))
        if not _within_date_window(published_at, start_date=start_date, end_date=end_date, lookback_days=lookback_days):
            _reject("DATE_OUTSIDE_WINDOW")
            continue
        summary = str(row.get("snippet") or row.get("description") or title).strip()
        duplicate_group_id = _make_duplicate_group_id(ticker_u, title, published_at, None, item_url)
        item = {
            "title": title,
            "source": "marketaux",
            "originalSource": str(row.get("source") or host).strip() or host,
            "sourceType": "marketaux_wire",
            "provider": "marketaux",
            "discoveredVia": "marketaux_api",
            "publishedAt": published_at,
            "retrievedAt": retrieved_at,
            "url": item_url,
            "issuer": str(matched_entity.get("name") or "").strip() or None,
            "tickers": [ticker_u],
            "matchBasis": "PROVIDER_ENTITY_SYMBOL",
            "sourceTickerMatch": True,
            "eventType": _event_type_from_keywords(f"{title} {summary}"),
            "summary": _short_text(summary, 240),
            "evidenceText": _short_text(summary, 180),
            "confidence": "LOW",
            "tickerRelevance": "HIGH",
            "duplicateGroupId": duplicate_group_id,
            "providerArticleId": str(row.get("uuid") or "").strip() or None,
            "providerRelevanceScore": matched_entity.get("match_score"),
        }
        diagnostics["acceptedCount"] += 1
        if len(items) < limit:
            items.append(item)

    diagnostics.update({
        "status": "OK" if diagnostics["acceptedCount"] else "EMPTY_RESULT",
        "collectedCount": len(items),
        "completed": True,
    })
    return items, warnings, True, diagnostics


def _marketaux_fallback_reason(
    selected_sources: list[str],
    items: list[dict],
    warnings: list[dict],
    source_diagnostics: dict[str, dict],
    search_query: str = "",
) -> str | None:
    priority_sources = {
        "yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub",
    }
    selected_priority = priority_sources.intersection(selected_sources)
    if not selected_priority:
        return "EXPLICIT_SOURCE_SELECTION"

    priority_item_sources = {
        "yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub",
    }
    query = str(search_query or "").strip().lower()

    def _matches_query(item: dict) -> bool:
        if not query:
            return True
        text = " ".join(str(item.get(key) or "") for key in (
            "title", "summary", "source", "eventType", "evidenceText",
        )).lower()
        return query in text

    has_priority_items = any(
        (
            str(item.get("source") or "") in priority_item_sources
            or str(item.get("sourceType") or "") in priority_item_sources.union({"company_news"})
        )
        and _matches_query(item)
        for item in items
    )
    limited = any(
        isinstance(warning, dict) and (
            warning.get("code") == "SOURCE_IDENTITY_UNAVAILABLE"
            or (
                warning.get("code") in {"SOURCE_UNAVAILABLE", "SOURCE_NOT_ELIGIBLE"}
                and str(warning.get("source") or warning.get("message") or "").lower().find("finnhub") >= 0
            )
            or (
                warning.get("code") == "SOURCE_UNAVAILABLE"
                and "yahoo finance" in str(warning.get("message") or "").lower()
            )
        )
        for warning in warnings
    )
    for source in selected_priority:
        diagnostic = source_diagnostics.get(source)
        if isinstance(diagnostic, dict) and (
            diagnostic.get("completed") is False
            or str(diagnostic.get("status") or "") in {
                "UNCONFIGURED", "NOT_ELIGIBLE", "PROVIDER_ERROR", "AUTH_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_CHANGED",
            }
        ):
            limited = True
    if limited:
        return "HIGHER_PRIORITY_COVERAGE_LIMITED"
    if not has_priority_items:
        return "HIGHER_PRIORITY_EMPTY"
    return None


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
                "code": "TIMESTAMP_VARIANCE",
                "message": f"Different source timestamps observed for duplicateGroupId={gid}; this is dedupe metadata, not factual evidence conflict.",
                "severity": "warning",
                "duplicateGroupId": gid,
                "statusImpact": False,
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
    has_limited_source = any(
        w.get("code") in {"SOURCE_UNAVAILABLE", "SOURCE_NOT_ELIGIBLE", "SOURCE_IDENTITY_UNAVAILABLE"}
        for w in warnings if isinstance(w, dict)
    )
    if items and has_limited_source:
        return "PARTIAL"
    if not items:
        # If any source is unconfigured/provider-error/rate-limited, report SOURCE_LIMITED_NOT_FOUND
        # so callers know the empty result may be due to missing coverage, not genuine absence.
        if has_limited_source:
            return "SOURCE_LIMITED_NOT_FOUND"
        if any(str(w.get("code", "")).startswith("COMPANY_IR_PAGE_") for w in warnings if isinstance(w, dict)):
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
    source_diagnostics: dict | None = None,
    budget_starved_sources: list[str] | None = None,
) -> dict:
    """Build per-source status dict from collection results.

    Supports both the new fine-grained source names (``yahoo_finance_news``,
    ``yahoo_finance_press_releases``) and the legacy ``yahoo_finance`` aggregate
    name for backward compatibility.
    """
    warning_msgs = [w.get("message", "") for w in warnings if isinstance(w, dict) and w.get("code") == "SOURCE_UNAVAILABLE"]
    warning_codes = {w.get("code") for w in warnings if isinstance(w, dict)}
    sec_items = [it for it in items if "sec" in str(it.get("sourceType", "")).lower()]
    sources = (
        ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"]
        if selected_sources is None
        else selected_sources
    )

    def _item_source(it: dict) -> str:
        return str(it.get("source") or "")

    def _item_source_type(it: dict) -> str:
        return str(it.get("sourceType") or "")

    # Per-source item counts
    yf_news_items = [
        it
        for it in items
        if _item_source(it) == "yahoo_finance_news"
        or _item_source_type(it) == "yahoo_finance_news"
    ]
    yf_pr_items = [
        it
        for it in items
        if _item_source(it) == "yahoo_finance_press_releases"
        or _item_source_type(it) == "yahoo_finance_press_releases"
    ]
    # Legacy yahoo_finance aggregates both fine-grained sources plus legacy-tagged items
    yf_legacy_items = [
        it
        for it in items
        if _item_source(it)
        in ("yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases")
        or _item_source_type(it)
        in ("yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases")
    ]
    newswire_items = [it for it in items if str(it.get("sourceType", "")) == "newswire"]
    company_ir_items = [it for it in items if str(it.get("sourceType", "")) in ("company_ir", "press_release")]
    company_ir_page_items = [it for it in items if str(it.get("sourceType", "")) == "company_ir_page"]
    finnhub_items = [it for it in items if str(it.get("source", "")) == "finnhub"]
    marketaux_items = [it for it in items if str(it.get("source", "")) == "marketaux"]
    diagnostics = source_diagnostics or {}

    def _yf_error_status(warn_msgs: list[str]) -> str | None:
        if any("yahoo finance" in m.lower() for m in warn_msgs):
            return "PROVIDER_ERROR"
        return None

    result: dict = {}
    identity_diagnostic = diagnostics.get("yahoo_finance_identity") if isinstance(diagnostics.get("yahoo_finance_identity"), dict) else None
    if identity_diagnostic and identity_diagnostic.get("status") == "IDENTITY_UNAVAILABLE":
        result["yahoo_finance_identity"] = dict(identity_diagnostic)

    def _yahoo_status(source: str, source_items: list[dict]) -> dict | None:
        diagnostic = diagnostics.get(source) if isinstance(diagnostics.get(source), dict) else None
        if diagnostic is None:
            return None
        accepted_count = int(
            diagnostic["acceptedCount"]
            if diagnostic.get("acceptedCount") is not None
            else diagnostic["filteredCount"]
            if diagnostic.get("filteredCount") is not None
            else len(source_items)
        )
        return {
            "status": "PROVIDER_ERROR" if diagnostic.get("completed") is False else ("OK" if accepted_count > 0 else "EMPTY_RESULT"),
            "rawCount": int(diagnostic.get("rawCount") or 0),
            "retrievedCount": int(diagnostic.get("retrievedCount") or diagnostic.get("rawCount") or 0),
            "filteredCount": int(diagnostic.get("filteredCount") or 0),
            "acceptedCount": accepted_count,
            "returnedCount": len(source_items),
            "rejectedCount": int(diagnostic.get("rejectedCount") or 0),
            "rejectionCounts": dict(diagnostic.get("rejectionCounts") or {}),
            "identityStatus": diagnostic.get("identityStatus"),
            "attempted": diagnostic.get("attempted") is not False,
            **({"method": diagnostic["method"]} if diagnostic.get("method") else {}),
        }

    if "sec" in sources:
        if "sec" in sources_used:
            result["sec"] = {"status": "OK" if sec_items else "EMPTY_RESULT", "rawCount": len(sec_items), "filteredCount": len(sec_items)}
        elif any("sec submissions" in m.lower() for m in warning_msgs):
            result["sec"] = {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0}
        else:
            result["sec"] = {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}

    # Fine-grained Yahoo Finance sources
    if "yahoo_finance_news" in sources:
        err = _yf_error_status(warning_msgs)
        diagnostic_status = _yahoo_status("yahoo_finance_news", yf_news_items)
        if diagnostic_status is not None:
            result["yahoo_finance_news"] = diagnostic_status
        elif "yahoo_finance_news" in sources_used:
            result["yahoo_finance_news"] = {"status": "OK" if yf_news_items else "EMPTY_RESULT", "rawCount": len(yf_news_items), "filteredCount": len(yf_news_items)}
        elif err:
            result["yahoo_finance_news"] = {"status": err, "rawCount": 0, "filteredCount": 0}
        else:
            result["yahoo_finance_news"] = {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}
    if "yahoo_finance_press_releases" in sources:
        err = _yf_error_status(warning_msgs)
        diagnostic_status = _yahoo_status("yahoo_finance_press_releases", yf_pr_items)
        if diagnostic_status is not None:
            result["yahoo_finance_press_releases"] = diagnostic_status
        elif "yahoo_finance_press_releases" in sources_used:
            result["yahoo_finance_press_releases"] = {"status": "OK" if yf_pr_items else "EMPTY_RESULT", "rawCount": len(yf_pr_items), "filteredCount": len(yf_pr_items)}
        elif err:
            result["yahoo_finance_press_releases"] = {"status": err, "rawCount": 0, "filteredCount": 0}
        else:
            result["yahoo_finance_press_releases"] = {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}

    # Legacy yahoo_finance aggregate source
    if "yahoo_finance" in sources:
        err = _yf_error_status(warning_msgs)
        diagnostic_status = _yahoo_status("yahoo_finance", yf_legacy_items)
        if diagnostic_status is not None:
            result["yahoo_finance"] = diagnostic_status
        elif "yahoo_finance" in sources_used:
            result["yahoo_finance"] = {"status": "OK" if yf_legacy_items else "EMPTY_RESULT", "rawCount": len(yf_legacy_items), "filteredCount": len(yf_legacy_items)}
        elif err:
            result["yahoo_finance"] = {"status": err, "rawCount": 0, "filteredCount": 0}
        else:
            result["yahoo_finance"] = {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}

    if "finnhub" in sources:
        ineligible_warning = next((w for w in warnings if isinstance(w, dict) and w.get("code") == "SOURCE_NOT_ELIGIBLE" and w.get("source") == "finnhub"), None)
        if ineligible_warning:
            result["finnhub"] = {
                "status": "NOT_ELIGIBLE", "rawCount": 0, "filteredCount": 0,
                "attempted": False, "reasonCode": ineligible_warning.get("reasonCode"),
                "allowedAction": "use_yahoo_or_official_sources",
            }
        elif "finnhub" in sources_used:
            diagnostic = diagnostics.get("finnhub") if isinstance(diagnostics.get("finnhub"), dict) else {}
            accepted_count = int(diagnostic.get("acceptedCount") if diagnostic.get("acceptedCount") is not None else len(finnhub_items))
            returned_count = len(finnhub_items)
            truncated_count = max(0, accepted_count - returned_count)
            result["finnhub"] = {
                "status": "OK" if accepted_count else "EMPTY_RESULT",
                "rawCount": int(diagnostic.get("rawCount") if diagnostic.get("rawCount") is not None else accepted_count),
                "retrievedCount": int(diagnostic.get("retrievedCount") if diagnostic.get("retrievedCount") is not None else diagnostic.get("rawCount") or accepted_count),
                "filteredCount": accepted_count,
                "acceptedCount": accepted_count,
                "collectedCount": int(diagnostic.get("collectedCount") if diagnostic.get("collectedCount") is not None else accepted_count),
                "returnedCount": returned_count,
                "rejectedCount": int(diagnostic.get("rejectedCount") or 0),
                "rejectionCounts": dict(diagnostic.get("rejectionCounts") or {}),
                "truncatedCount": truncated_count,
                "hasMoreAccepted": truncated_count > 0,
                "attempted": diagnostic.get("attempted") is not False,
            }
        elif any("finnhub company-news source is not configured" in m.lower() for m in warning_msgs):
            result["finnhub"] = {"status": "UNCONFIGURED"}
        elif any("finnhub auth error" in m.lower() for m in warning_msgs):
            result["finnhub"] = {"status": "AUTH_ERROR", "rawCount": 0, "filteredCount": 0}
        elif any("finnhub rate limited" in m.lower() for m in warning_msgs):
            result["finnhub"] = {"status": "RATE_LIMITED", "rawCount": 0, "filteredCount": 0}
        elif any("finnhub provider changed" in m.lower() for m in warning_msgs):
            result["finnhub"] = {"status": "PROVIDER_CHANGED", "rawCount": 0, "filteredCount": 0}
        elif any("finnhub" in m.lower() for m in warning_msgs):
            result["finnhub"] = {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0}
        else:
            result["finnhub"] = {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}
    if "marketaux" in sources:
        diagnostic = diagnostics.get("marketaux") if isinstance(diagnostics.get("marketaux"), dict) else {}
        diagnostic_status = str(diagnostic.get("status") or "")
        accepted_count = int(diagnostic.get("acceptedCount") if diagnostic.get("acceptedCount") is not None else len(marketaux_items))
        returned_count = len(marketaux_items)
        status = diagnostic_status or ("OK" if accepted_count else "EMPTY_RESULT")
        allowed_action = {
            "NOT_NEEDED": "none",
            "UNCONFIGURED": "configure_marketaux_api_token",
            "AUTH_ERROR": "verify_marketaux_api_token",
            "RATE_LIMITED": "retry_after_quota_reset",
            "TIMEOUT": "retry_later",
            "PROVIDER_CHANGED": "inspect_provider_contract",
            "PROVIDER_ERROR": "retry_or_use_official_sources",
        }.get(status, "check_official_releases" if accepted_count else "none")
        result["marketaux"] = {
            "status": status,
            "rawCount": int(diagnostic.get("rawCount") or 0),
            "retrievedCount": int(diagnostic.get("retrievedCount") or diagnostic.get("rawCount") or 0),
            "filteredCount": accepted_count,
            "acceptedCount": accepted_count,
            "returnedCount": returned_count,
            "rejectedCount": int(diagnostic.get("rejectedCount") or 0),
            "rejectionCounts": dict(diagnostic.get("rejectionCounts") or {}),
            "attempted": diagnostic.get("attempted") is not False,
            "reasonCode": diagnostic.get("reasonCode"),
            "fallbackReason": diagnostic.get("fallbackReason"),
            "quotaPolicy": diagnostic.get("quotaPolicy"),
            "domains": diagnostic.get("domains"),
            "allowedAction": allowed_action,
        }
    if "company_ir" in sources:
        if "company_ir" in sources_used:
            result["company_ir"] = {"status": "OK" if company_ir_items else "EMPTY_RESULT", "rawCount": len(company_ir_items), "filteredCount": len(company_ir_items)}
        elif _yf_error_status(warning_msgs):
            result["company_ir"] = {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0}
        else:
            result["company_ir"] = {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}
    if "company_ir_page" in sources:
        page_warning = next((w for w in warnings if isinstance(w, dict) and str(w.get("code", "")).startswith("COMPANY_IR_PAGE_")), {})
        status = "EMPTY_RESULT"
        if "company_ir_page" in sources_used:
            status = "APPROVED_SOURCE_OK" if company_ir_page_items else "EMPTY_RESULT"
        elif "COMPANY_IR_PAGE_CANDIDATE_AVAILABLE" in warning_codes:
            status = "CANDIDATE_AVAILABLE"
        elif "COMPANY_IR_PAGE_DISABLED" in warning_codes:
            status = "DISABLED"
        elif "COMPANY_IR_PAGE_REVALIDATION_DUE" in warning_codes:
            status = "REVALIDATION_DUE"
        elif "COMPANY_IR_PAGE_VALIDATION_FAILED" in warning_codes:
            status = "SOURCE_VALIDATION_FAILED"
        else:
            status = "NOT_REGISTERED"
        result["company_ir_page"] = {
            "status": status,
            "rawCount": len(company_ir_page_items),
            "filteredCount": len(company_ir_page_items),
            "registryVersion": page_warning.get("registryVersion"),
            "canonicalUrl": page_warning.get("canonicalUrl"),
            "approvalStatus": page_warning.get("approvalStatus"),
            "allowedAction": "fetch_configured_source" if status == "APPROVED_SOURCE_OK" else (
                "review_and_promote" if status == "CANDIDATE_AVAILABLE" else "registry_review_required"
            ),
        }
        if isinstance(page_warning.get("candidate"), dict):
            result["company_ir_page"]["candidates"] = [page_warning["candidate"]]
    if "newswire" in sources:
        if "newswire" in sources_used:
            result["newswire"] = {"status": "OK" if newswire_items else "EMPTY_RESULT", "rawCount": len(newswire_items), "filteredCount": len(newswire_items)}
        elif any("globenewswire" in m.lower() for m in warning_msgs):
            result["newswire"] = {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0}
        else:
            result["newswire"] = {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}
    if budget_starved_sources is None:
        budget_starved_sources = list(getattr(source_diagnostics, "budget_starved", None) or [])
    return _mark_budget_starved_sources(result, budget_starved_sources)


# A source whose collection had a fetch refused for the subrequest budget and did not end OK reports
# PROVIDER_BUDGET_EXHAUSTED (2.11.0), never PROVIDER_ERROR, EMPTY_RESULT or another absence. Only `status` changes.
_BUDGET_UNAFFECTED_STATUSES = {"OK", "APPROVED_SOURCE_OK"}


def _mark_budget_starved_sources(source_status: dict, budget_starved_sources: list[str]) -> dict:
    for source in budget_starved_sources:
        row = source_status.get(source)
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "") not in _BUDGET_UNAFFECTED_STATUSES:
            row["status"] = "PROVIDER_BUDGET_EXHAUSTED"
    return source_status


class _SourceDiagnostics(dict):
    """Per-source diagnostics plus the sources whose collection had a fetch refused for budget (never serialized)."""

    budget_starved: list[str]

    def __init__(self) -> None:
        super().__init__()
        self.budget_starved = []


def _compute_source_coverage(source_status: dict) -> str:
    """Return PARTIAL if any source is UNCONFIGURED or has an error, else FULL."""
    for info in source_status.values():
        s = info.get("status", "") if isinstance(info, dict) else ""
        if s in (
            "UNCONFIGURED", "NOT_ELIGIBLE", "PROVIDER_ERROR", "AUTH_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_BUDGET_EXHAUSTED",
            "PROVIDER_CHANGED", "IDENTITY_UNAVAILABLE",
            "WEBSITE_NOT_AVAILABLE", "DISCOVERY_NOT_FOUND", "DISCOVERY_BUDGET_EXHAUSTED", "FEED_NOT_FOUND", "PARSE_ERROR",
            "CANDIDATE_AVAILABLE", "NOT_REGISTERED", "DISABLED", "REVALIDATION_DUE", "SOURCE_VALIDATION_FAILED",
        ):
            return "PARTIAL"
    return "FULL"


def _verify_coverage_failure_mode(source_status: dict, warnings: list[dict]) -> str | None:
    """Classify unavailable verification coverage without mistaking it for absence."""
    warning_text = " ".join(
        str(w.get("message") or "").lower()
        for w in warnings
        if isinstance(w, dict) and w.get("code") == "SOURCE_UNAVAILABLE"
    )
    source_states = {
        str(info.get("status") or "")
        for info in source_status.values()
        if isinstance(info, dict)
    }
    if "too many subrequests" in warning_text or "PROVIDER_BUDGET_EXHAUSTED" in source_states:
        return "WORKER_SUBREQUEST_LIMIT"
    if "RATE_LIMITED" in source_states:
        return "RATE_LIMITED"
    if "TIMEOUT" in source_states:
        return "PROVIDER_TIMEOUT"
    if "PROVIDER_CHANGED" in source_states:
        return "PROVIDER_CHANGED"
    if "IDENTITY_UNAVAILABLE" in source_states:
        return "YAHOO_IDENTITY_UNAVAILABLE"
    if "UNCONFIGURED" in source_states:
        return "SOURCE_UNCONFIGURED"
    if "NOT_ELIGIBLE" in source_states:
        return "SOURCE_NOT_ELIGIBLE"
    if "PROVIDER_ERROR" in source_states:
        return "PROVIDER_ERROR"
    return None


async def _collect_company_events(
    ticker: str,
    *,
    max_results: int,
    lookback_days: int,
    start_date: str = "",
    end_date: str = "",
    sources: list[str] | None = None,
    sec_filing_types: list[str] | None = None,
    search_query: str = "",
    include_diagnostics: bool = False,
    keep_all: bool = False,
) -> tuple:
    retrieved_at = _utc_now_iso()
    selected_sources, warnings = _normalize_event_sources(
        sources,
        ["sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"],
    )
    items: list[dict] = []
    sources_used: list[str] = []
    source_diagnostics: dict[str, dict] = _SourceDiagnostics()
    max_cap = _coerce_max_results(max_results, 10)
    lookback = _coerce_lookback_days(lookback_days, 14)

    # Each source is collected inside its own budget watch, so a fetch the subrequest budget refused is
    # charged to the source that asked for it (2.11.0).
    budget_starved: set[str] = set()

    async def _watched(charged: list[str], work):
        with _url_policy.budget_watch() as watch:
            result = await work
        if watch.starved:
            budget_starved.update(charged)
        return result

    # --- Yahoo Finance news ---
    # ``yahoo_finance_news`` fetches the news tab explicitly.
    # ``yahoo_finance`` (legacy) fetches news tab and also the press-releases tab;
    # items are labelled with their specific source (yahoo_finance_news / _press_releases).
    _need_yf_news = "yahoo_finance_news" in selected_sources or "yahoo_finance" in selected_sources
    _need_yf_pr = "yahoo_finance_press_releases" in selected_sources or "yahoo_finance" in selected_sources
    # Also satisfy old company_ir source via the Yahoo feed when it is requested
    # but yahoo_finance* is not.  ``newswire`` is now served by the direct
    # GlobeNewswire RSS fetcher below and is intentionally excluded here.
    _need_yf_news = _need_yf_news or "company_ir" in selected_sources
    finnhub_eligible, finnhub_reason_code = (
        _finnhub_eligibility(ticker) if "finnhub" in selected_sources else (False, None)
    )
    window = {
        "retrieved_at": retrieved_at,
        "max_results": max_cap,
        "start_date": start_date,
        "end_date": end_date,
        "lookback_days": lookback,
    }

    async def _skipped() -> None:
        return None

    async def _yahoo_feeds() -> tuple:
        if not (_need_yf_news or _need_yf_pr):
            return None, None, None
        try:
            with _url_policy.budget_watch() as identity_watch:
                info = await asyncio.to_thread(lambda: yf.Ticker(ticker).info)
            identity = _yahoo_news_identity_from_info(ticker, info)
        except Exception:
            identity = _yahoo_news_identity_from_info(ticker, None)
        if identity_watch.starved:
            budget_starved.add("yahoo_finance_identity")

        def _feed(feed: str):
            # The Yahoo collector calls yfinance synchronously, so each feed
            # runs on its own thread to overlap with the other sources.
            return asyncio.to_thread(lambda: asyncio.run(_collect_yahoo_events(
                ticker, **window, feed=feed, identity=identity, include_diagnostics=True,
            )))

        news, press = await asyncio.gather(
            _watched(["yahoo_finance_news", "yahoo_finance"], _feed("news")) if _need_yf_news else _skipped(),
            _watched(["yahoo_finance_press_releases", "yahoo_finance"], _feed("press_releases")) if _need_yf_pr else _skipped(),
        )
        return identity, news, press

    # The first-tier sources are independent, so they are fetched together;
    # results are merged below in the fixed source order, as before.
    sec_result, page_result, yahoo_result, finnhub_result = await asyncio.gather(
        _watched(["sec"], _collect_sec_events(
            ticker,
            filing_types=sec_filing_types or ["8-K", "10-Q", "10-K", "S-3", "DEF14A"],
            **window,
        )) if "sec" in selected_sources else _skipped(),
        _watched(["company_ir_page"], _collect_company_ir_page_events(ticker, **window)) if "company_ir_page" in selected_sources else _skipped(),
        _yahoo_feeds(),
        _watched(["finnhub"], _collect_finnhub_events(ticker, **window)) if "finnhub" in selected_sources and finnhub_eligible else _skipped(),
    )

    if sec_result is not None:
        sec_items, sec_warnings, used = sec_result
        if used:
            sources_used.append("sec")
        items.extend(sec_items)
        warnings.extend(sec_warnings)

    if page_result is not None:
        page_items, page_warnings, page_used = page_result
        if page_used:
            sources_used.append("company_ir_page")
        items.extend(page_items)
        warnings.extend(page_warnings)

    yahoo_identity, yf_result, pr_result = yahoo_result
    if yahoo_identity is not None and yahoo_identity.get("status") == "UNAVAILABLE":
        source_diagnostics["yahoo_finance_identity"] = {
            "status": "IDENTITY_UNAVAILABLE",
            "attempted": True,
            "reasonCode": "YAHOO_PROFILE_IDENTITY_UNAVAILABLE",
            "allowedAction": "retry_or_use_explicit_ticker_matches_only",
        }
        warnings.append({
            "code": "SOURCE_IDENTITY_UNAVAILABLE",
            "message": "Yahoo company identity lookup was unavailable; only exact ticker-token matches were retained.",
            "severity": "warning",
            "source": "yahoo_finance_identity",
        })

    def _unpack_provider_result(value: tuple) -> tuple[list[dict], list[dict], bool, dict]:
        if len(value) == 4:
            result_items, result_warnings, result_used, result_diagnostics = value
            return result_items, result_warnings, result_used, result_diagnostics
        result_items, result_warnings, result_used = value
        return result_items, result_warnings, result_used, {}

    if yf_result is not None:
        yf_items, yf_warnings, used, yf_diagnostics = _unpack_provider_result(yf_result)
        source_diagnostics["yahoo_finance_news"] = {**yf_diagnostics, "attempted": True, "completed": used}
        if used:
            if "yahoo_finance_news" in selected_sources:
                sources_used.append("yahoo_finance_news")
            if "yahoo_finance" in selected_sources and "yahoo_finance" not in sources_used:
                sources_used.append("yahoo_finance")
            # Legacy sub-source resolved via Yahoo feed
            if "company_ir" in selected_sources and "company_ir" not in sources_used:
                sources_used.append("company_ir")
        for item in yf_items:
            src = str(item.get("source") or "")
            if "yahoo_finance_news" in selected_sources and src == "yahoo_finance_news":
                items.append(item)
            elif "yahoo_finance" in selected_sources and src in ("yahoo_finance_news", "yahoo_finance_press_releases"):
                items.append(item)
            elif "company_ir" in selected_sources and src in ("yahoo_finance_news", "yahoo_finance_press_releases"):
                items.append(item)
        warnings.extend(yf_warnings)

    if pr_result is not None:
        pr_items, pr_warnings, used, pr_diagnostics = _unpack_provider_result(pr_result)
        source_diagnostics["yahoo_finance_press_releases"] = {**pr_diagnostics, "attempted": True, "completed": used}
        if used and "yahoo_finance_press_releases" in selected_sources and "yahoo_finance_press_releases" not in sources_used:
            sources_used.append("yahoo_finance_press_releases")
        items.extend(pr_items)
        warnings.extend(pr_warnings)

    if finnhub_result is not None:
        finnhub_items, finnhub_warnings, used, finnhub_diagnostics = _unpack_provider_result(finnhub_result)
        source_diagnostics["finnhub"] = finnhub_diagnostics
        if used:
            sources_used.append("finnhub")
        items.extend(finnhub_items)
        warnings.extend(finnhub_warnings)
    elif "finnhub" in selected_sources:
        warnings.append({
            "code": "SOURCE_NOT_ELIGIBLE",
            "message": f"Finnhub company-news is intentionally skipped for {ticker.upper()} under the deployed market capability policy.",
            "severity": "warning",
            "source": "finnhub",
            "attempted": False,
            "reasonCode": finnhub_reason_code,
        })

    if "marketaux" in selected_sources:
        fallback_reason = _marketaux_fallback_reason(
            selected_sources,
            items,
            warnings,
            source_diagnostics,
            search_query,
        )
        if fallback_reason is None:
            source_diagnostics["marketaux"] = {
                "status": "NOT_NEEDED",
                "attempted": False,
                "completed": True,
                "reasonCode": "HIGHER_PRIORITY_EVIDENCE_FOUND",
                "fallbackReason": None,
                "domains": list(_MARKETAUX_WIRE_DOMAINS),
                "quotaPolicy": {
                    "dailyLimit": 100,
                    "maxArticlesPerRequest": _MARKETAUX_MAX_ITEMS,
                    "pagesRequested": 0,
                    "automaticPagination": False,
                },
            }
        else:
            marketaux_items, marketaux_warnings, used, marketaux_diagnostics = _unpack_provider_result(
                await _watched(["marketaux"], _collect_marketaux_events(
                    ticker,
                    retrieved_at=retrieved_at,
                    max_results=max_cap,
                    start_date=start_date,
                    end_date=end_date,
                    lookback_days=lookback,
                    fallback_reason=fallback_reason,
                    search_query=search_query,
                    expected_exchange=(yahoo_identity or {}).get("exchange"),
                ))
            )
            source_diagnostics["marketaux"] = marketaux_diagnostics
            if used:
                sources_used.append("marketaux")
            items.extend(marketaux_items)
            warnings.extend(marketaux_warnings)

    # Direct GlobeNewswire RSS is an explicitly selected shallow snapshot and
    # runs after the bounded API sources so its cold latency cannot delay them.
    if "newswire" in selected_sources:
        gnw_items, gnw_warnings, gnw_used = await _watched(["newswire"], _collect_globenewswire_events(
            ticker,
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
        ))
        if gnw_used:
            sources_used.append("newswire")
        items.extend(gnw_items)
        warnings.extend(gnw_warnings)

    deduped = [_enrich_news_item_for_llm(item) for item in _dedupe_event_items(items, warnings)]
    # Event verification keeps every item and matches before capping (keep_all).
    if not keep_all:
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
    if "yahoo_finance" in selected_sources:
        yahoo_diagnostics = [
            source_diagnostics[name]
            for name in ("yahoo_finance_news", "yahoo_finance_press_releases")
            if name in source_diagnostics
        ]
        if yahoo_diagnostics:
            rejection_counts: dict[str, int] = {}
            for diagnostic in yahoo_diagnostics:
                for reason, count in (diagnostic.get("rejectionCounts") or {}).items():
                    rejection_counts[reason] = rejection_counts.get(reason, 0) + int(count or 0)
            source_diagnostics["yahoo_finance"] = {
                "rawCount": sum(int(diagnostic.get("rawCount") or 0) for diagnostic in yahoo_diagnostics),
                "retrievedCount": sum(int(diagnostic.get("retrievedCount") or 0) for diagnostic in yahoo_diagnostics),
                "filteredCount": sum(int(diagnostic.get("filteredCount") or 0) for diagnostic in yahoo_diagnostics),
                "acceptedCount": sum(int(diagnostic.get("acceptedCount") or 0) for diagnostic in yahoo_diagnostics),
                "rejectedCount": sum(int(diagnostic.get("rejectedCount") or 0) for diagnostic in yahoo_diagnostics),
                "rejectionCounts": rejection_counts,
                "identityStatus": (yahoo_identity or {}).get("status") or "UNAVAILABLE",
                "attempted": True,
                "completed": all(diagnostic.get("completed") is True for diagnostic in yahoo_diagnostics),
            }
    source_diagnostics.budget_starved = sorted(budget_starved)
    if include_diagnostics:
        return deduped, sources_used, unique_warnings, retrieved_at, source_diagnostics
    return deduped, sources_used, unique_warnings, retrieved_at


def _unpack_company_event_result(value: tuple) -> tuple[list[dict], list[str], list[dict], str, dict]:
    """Accept legacy test fixtures while production callers receive diagnostics."""
    if len(value) == 5:
        items, sources_used, warnings, retrieved_at, source_diagnostics = value
        return items, sources_used, warnings, retrieved_at, source_diagnostics
    items, sources_used, warnings, retrieved_at = value
    return items, sources_used, warnings, retrieved_at, {}


_NEWS_RELEVANCE_RANK = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


def _rank_news_items_by_relevance(items: list[dict]) -> list[dict]:
    """Items ordered by tickerRelevance (HIGH, MEDIUM, LOW), keeping their order within each level."""
    return sorted(items, key=lambda item: _NEWS_RELEVANCE_RANK.get(str(item.get("tickerRelevance") or "").upper(), 1))


@yfinance_server.tool(
    name="search_company_news",
    output_schema=_TOOL_OUTPUT_SCHEMAS["search_company_news"],
    description="""Search public company news/events for a ticker and query string.

Defaults use Yahoo Finance, eligible Finnhub, then one quota-bounded Marketaux
wire-domain fallback only when higher-priority coverage is empty or limited.
Marketaux never paginates automatically and remains contextual evidence; inspect
coverage and sourceStatus before treating an empty result as absence.
""",
)
async def search_company_news(
    ticker: str,
    query: str,
    start_date: str = "",
    end_date: str = "",
    sources: Annotated[list[str] | None, Field(description=_SOURCES_DEFAULT_DESCRIPTION)] = None,
    max_results: int = 10,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("search_company_news", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if not str(query or "").strip():
        return _mcp_failure("search_company_news", ErrorCode.INPUT_VALIDATION_ERROR, "query is required")
    effective_sources = _DEFAULT_NEWS_SOURCES if sources is None else sources
    items, sources_used, warnings, retrieved_at, source_diagnostics = _unpack_company_event_result(
        # The query filters the full collected pool, as the Worker does.
        await _collect_company_events(
            ticker,
            max_results=100,
            lookback_days=14,
            start_date=start_date,
            end_date=end_date,
            sources=effective_sources,
            search_query=query,
            include_diagnostics=True,
        )
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
    returned = _rank_news_items_by_relevance(filtered)[:_coerce_max_results(max_results, 10)]
    status = _build_collection_status(returned, sources_used, warnings)
    source_status = _compute_source_status(sources_used, warnings, returned, effective_sources, source_diagnostics)
    source_coverage = _compute_source_coverage(source_status)
    payload = {
        "ticker": ticker.upper(),
        "query": query,
        "matchCount": len(filtered),
        "items": returned,
        "meta": {
            "sourcesUsed": sources_used,
            "deduped": True,
            "watermark": retrieved_at,
        },
        "warnings": warnings,
        "sourceCoverage": source_coverage,
        "coverage": _build_coverage(source_status),
        "sourceStatus": source_status,
        **_source_request_fields(sources, source_status),
    }
    if status:
        payload["status"] = status
    return json.dumps(_mark_no_source_selected(payload, sources))


@yfinance_server.tool(
    name="get_company_press_releases",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_company_press_releases"],
    description="""Get company press releases and official release-style public events.

Defaults resolve SEC 8-K/EX-99 evidence first, then registry-backed
``company_ir_page``, then Yahoo press-release context. Explicit sources can
also include ``company_ir`` RSS/Atom, quota-bounded ``marketaux``, and direct
``newswire`` RSS. Items are labelled with precise source identifiers so callers
can distinguish their origin.

Decision-grade use is payload-level only: ``decisionGrade:true`` requires
``coverageStatus`` of ``SEC_EX99_RESOLVED`` or
``APPROVED_IR_PAGE_RESOLVED`` with evidence fields.
""",
)
async def get_company_press_releases(
    ticker: str,
    lookback_days: int = 90,
    max_results: int = 20,
    sources: Annotated[list[str] | None, Field(description=_SOURCES_DEFAULT_DESCRIPTION)] = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_company_press_releases", ErrorCode.INPUT_VALIDATION_ERROR, err)
    selected_sources, source_warnings = _normalize_event_sources(sources, _DEFAULT_PRESS_RELEASE_SOURCES)
    primary_sources = [src for src in selected_sources if src in ("sec", "company_ir_page")]
    optional_sources = [src for src in selected_sources if src not in ("sec", "company_ir_page")]
    safe_max = _coerce_max_results(max_results, 20)
    primary_items: list[dict] = []
    primary_used: list[str] = []
    primary_warnings: list[dict] = []
    primary_diagnostics: dict = {}
    retrieved_at = _utc_now_iso()

    async def _collect(source_list: list[str]) -> tuple:
        return _unpack_company_event_result(
            await _collect_company_events(
                ticker,
                max_results=safe_max,
                lookback_days=lookback_days,
                sources=source_list,
                sec_filing_types=["8-K"],
                include_diagnostics=True,
            )
        )

    async def _no_sources() -> tuple:
        return [], [], [], retrieved_at, {}

    # Primary and optional sources are merged only after both return, so
    # they are collected together.
    primary_result, optional_result = await asyncio.gather(
        _collect(primary_sources) if primary_sources else _no_sources(),
        _collect(optional_sources) if optional_sources else _no_sources(),
    )
    if primary_sources:
        primary_items, primary_used, primary_warnings, retrieved_at, primary_diagnostics = primary_result
    release_types = {"company_ir", "company_ir_page", "press_release", "newswire", "marketaux_wire", "sec_filing", "sec_ex99_found", "yahoo_finance_press_releases"}
    release_items = [it for it in primary_items if str(it.get("sourceType")) in release_types]

    modified_primary_items: list[dict] = []
    has_sec_ex99_found = False
    sec_8k_evidence: list[dict] = []
    sec_8k_without_ex99_count = 0
    from yfmcp.tools.earnings import _resolve_ex991_url

    # Resolve every 8-K's EX-99.1 exhibit up front, a few SEC requests at a time.
    ex991_limit = asyncio.Semaphore(4)

    async def _ex991_for(item: dict) -> str | None:
        if item.get("sourceType") != "sec_filing" or item.get("filingType") != "8-K":
            return None
        acc = item.get("accessionNumber")
        cik_match = _re.search(r'/data/(\d+)/', item.get("url") or "")
        if not acc or not cik_match:
            return None
        async with ex991_limit:
            return await _resolve_ex991_url(acc, int(cik_match.group(1)))

    ex991_urls = await asyncio.gather(*(_ex991_for(it) for it in release_items))
    for it, ex991_url in zip(release_items, ex991_urls):
        if it.get("sourceType") == "sec_filing" and it.get("filingType") == "8-K":
            acc = it.get("accessionNumber")
            url = it.get("url") or ""
            evidence = {
                "filingType": "8-K",
                "filingDate": it.get("filingDate"),
                "acceptedAt": it.get("acceptedAt"),
                "accessionNumber": acc,
                "documentUrl": url or None,
            }
            sec_8k_evidence.append(evidence)
            if ex991_url:
                evidence["ex991Url"] = ex991_url
                evidence["ex991Resolved"] = True
                it = dict(it)
                it["sourceType"] = "sec_ex99_found"
                it["url"] = ex991_url
                it["title"] = "EX-99.1 exhibit found in 8-K"
                it["eventType"] = "press_release"
                it["evidenceText"] = "Resolved EX-99.1 press-release exhibit from SEC 8-K."
                it["confidence"] = "HIGH"
                has_sec_ex99_found = True
            else:
                sec_8k_without_ex99_count += 1
        modified_primary_items.append(it)

    optional_items: list[dict] = []
    optional_used: list[str] = []
    optional_warnings: list[dict] = []
    optional_diagnostics: dict = {}
    if optional_sources:
        optional_items, optional_used, optional_warnings, _optional_retrieved_at, optional_diagnostics = optional_result
    warnings = source_warnings + primary_warnings + optional_warnings
    modified_release_items = _dedupe_event_items(
        modified_primary_items + [it for it in optional_items if str(it.get("sourceType")) in release_types],
        warnings,
    )
    modified_release_items = [_enrich_news_item_for_llm(item) for item in modified_release_items]
    sources_used = list(dict.fromkeys(primary_used + optional_used))
    has_approved_ir_page = any(
        it.get("sourceType") == "company_ir_page" and it.get("approvalStatus") == "approved" and it.get("url")
        for it in modified_release_items
    )

    if not modified_release_items:
        warnings.append({
            "code": "NO_OFFICIAL_RELEASE_SOURCE",
            "message": "No company-originated or official release source found in requested window.",
            "severity": "warning",
        })
    if not has_sec_ex99_found and sec_8k_without_ex99_count > 0:
        warnings.append({
            "code": "SEC_8K_FOUND_EX99_NOT_FOUND",
            "message": "SEC 8-K filing(s) were found, but no EX-99.1 press-release exhibit was resolved.",
            "severity": "warning",
            "filingsSearched": sec_8k_without_ex99_count,
        })

    status = None
    if has_sec_ex99_found:
        status = "SEC_EX99_FOUND"
    elif has_approved_ir_page:
        status = "APPROVED_IR_PAGE_FOUND"
    elif sec_8k_without_ex99_count > 0:
        status = "SEC_8K_FOUND_EX99_NOT_FOUND"
    elif not modified_release_items:
        if "yahoo_finance_press_releases" in selected_sources:
            status = "NO_YAHOO_PRESS_RELEASE"
        elif "company_ir_page" in selected_sources:
            status = "COMPANY_IR_PAGE_NOT_APPROVED"
        elif "company_ir" in selected_sources:
            status = "COMPANY_IR_NOT_FOUND"
        else:
            status = "NOT_FOUND"
    else:
        status = _build_collection_status(modified_release_items, sources_used, warnings)

    source_status = _compute_source_status(
        sources_used,
        warnings,
        modified_release_items,
        selected_sources,
        {**primary_diagnostics, **optional_diagnostics},
        sorted({
            *getattr(primary_diagnostics, "budget_starved", []),
            *getattr(optional_diagnostics, "budget_starved", []),
        }),
    )
    source_coverage = _compute_source_coverage(source_status)
    coverage_status = (
        "SEC_EX99_RESOLVED" if has_sec_ex99_found else
        "APPROVED_IR_PAGE_RESOLVED" if has_approved_ir_page else
        "SEC_8K_FOUND_EX99_NOT_FOUND" if sec_8k_without_ex99_count > 0 else
        "OFFICIAL_RELEASE_SOURCE_FOUND" if modified_release_items else
        "NO_OFFICIAL_RELEASE_SOURCE"
    )
    decision_grade = has_sec_ex99_found or has_approved_ir_page
    coverage = _build_coverage(source_status, "USE_OFFICIAL_EVIDENCE" if decision_grade else None)
    decision_grade_basis = (
        "Resolved SEC 8-K EX-99 press-release exhibit for this call."
        if has_sec_ex99_found else
        "Resolved an approved, registry-reviewed company IR page source for this call."
        if has_approved_ir_page else
        "No resolved SEC EX-99 or approved IR-page evidence in this call; use for verification/context only."
    )
    ir_page_evidence = [
        {
            "title": it.get("title"),
            "publishedAt": it.get("publishedAt"),
            "url": it.get("url"),
            "canonicalUrl": it.get("canonicalUrl"),
            "adapter": it.get("adapter"),
            "registryVersion": it.get("registryVersion"),
            "basis": "approved_company_ir_page_registry",
        }
        for it in modified_release_items
        if it.get("sourceType") == "company_ir_page" and it.get("approvalStatus") == "approved"
    ][:10]
    candidate_sources = [
        w.get("candidate")
        for w in warnings
        if isinstance(w, dict) and w.get("code") == "COMPANY_IR_PAGE_CANDIDATE_AVAILABLE" and isinstance(w.get("candidate"), dict)
    ]
    payload = {
        "ticker": ticker.upper(),
        "items": modified_release_items[:safe_max],
        "meta": {
            "sourcesUsed": sources_used,
            "deduped": True,
            "watermark": retrieved_at,
        },
        "warnings": warnings,
        "sourceCoverage": source_coverage,
        "coverage": coverage,
        "sourceStatus": source_status,
        **_source_request_fields(sources, source_status),
        "coverageStatus": coverage_status,
        "decisionGrade": decision_grade,
        "decisionGradeBasis": decision_grade_basis,
        "capabilityStatus": "ACTIVE",
        "reliabilityClass": _TOOL_RELIABILITY_CLASS["get_company_press_releases"],
        "failureMode": None if decision_grade else coverage_status,
    }
    if sec_8k_evidence:
        payload["secEvidence"] = sec_8k_evidence[:10]
    if ir_page_evidence:
        payload["irPageEvidence"] = ir_page_evidence
    if candidate_sources:
        payload["candidateSources"] = candidate_sources
    if status:
        payload["status"] = status
    return json.dumps(_mark_no_source_selected(payload, sources))


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
    with _url_policy.budget_watch() as sec_watch:
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
    items = [_enrich_news_item_for_llm(item) for item in items]
    status = _build_collection_status(items, ["sec"] if used else [], warnings)
    source_status = _mark_budget_starved_sources(
        {"sec": {"status": "OK" if items else "EMPTY_RESULT"} if used else {"status": "PROVIDER_ERROR"}},
        ["sec"] if sec_watch.starved else [],
    )
    source_coverage = _compute_source_coverage(source_status)
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

Defaults combine SEC and reviewed/RSS company IR with Yahoo, eligible Finnhub,
and one quota-aware Marketaux fallback. Direct newswire RSS is explicit because
it is a shallow snapshot. Returns coverage metadata so source limitations are
distinguishable from no events.
""",
)
async def get_public_event_timeline(
    ticker: str,
    start_date: str = "",
    end_date: str = "",
    sources: Annotated[list[str] | None, Field(description=_SOURCES_DEFAULT_DESCRIPTION)] = None,
    max_results: int = 50,
    newest_first: bool = False,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_public_event_timeline", ErrorCode.INPUT_VALIDATION_ERROR, err)
    effective_sources = _DEFAULT_EVENT_SOURCES if sources is None else sources
    items, sources_used, warnings, retrieved_at, source_diagnostics = _unpack_company_event_result(
        await _collect_company_events(
            ticker,
            max_results=max_results,
            lookback_days=365,
            start_date=start_date,
            end_date=end_date,
            sources=effective_sources,
            include_diagnostics=True,
        )
    )
    timeline = [{
        "timestamp": it.get("publishedAt"),
        "eventType": it.get("eventType"),
        "title": it.get("title"),
        "source": it.get("source"),
        "sourceType": it.get("sourceType"),
        "url": it.get("url"),
        "confidence": it.get("confidence"),
        "evidenceClass": it.get("evidenceClass"),
        "tickerMatch": it.get("tickerMatch"),
        "matchBasis": it.get("matchBasis"),
        "urlProvenance": it.get("urlProvenance"),
        "decisionUse": it.get("decisionUse"),
        "duplicateGroupId": it.get("duplicateGroupId"),
        "sourceRefs": it.get("sourceRefs") or [],
    } for it in items if it.get("publishedAt")]
    timeline.sort(key=lambda ev: str(ev.get("timestamp") or ""), reverse=bool(newest_first))
    timeline = timeline[:_coerce_max_results(max_results, 50)]
    status = _build_collection_status(items, sources_used, warnings)
    source_status = _compute_source_status(sources_used, warnings, items, effective_sources, source_diagnostics)
    source_coverage = _compute_source_coverage(source_status)
    payload = {
        "ticker": ticker.upper(),
        "timeline": timeline,
        "meta": {
            "sourcesUsed": sources_used,
            "deduped": True,
            "watermark": retrieved_at,
        },
        "warnings": warnings,
        "sourceCoverage": source_coverage,
        "coverage": _build_coverage(source_status),
        "sourceStatus": source_status,
        **_source_request_fields(sources, source_status),
    }
    if status:
        payload["status"] = status
    return json.dumps(_mark_no_source_selected(payload, sources))


@yfinance_server.tool(
    name="verify_company_event",
    output_schema=_TOOL_OUTPUT_SCHEMAS["verify_company_event"],
    description="""Verify whether a public company event has source-backed evidence.

Returns CONFIRMED, PARTIAL, NOT_FOUND, SOURCE_LIMITED_NOT_FOUND, STALE, or CONFLICTING.
SOURCE_LIMITED_NOT_FOUND means selected provider coverage was incomplete, not that
the event is confirmed absent; inspect sourceStatus and failureMode for recovery.
Generic publication words such as announced, report, results, and update do not
establish a match by themselves; inspect queryPolicy and each queryMatch.
Defaults use SEC/company IR, Yahoo, eligible Finnhub, then one quota-aware
Marketaux fallback. Marketaux evidence remains contextual and should be escalated
to an official release before decision use.
""",
)
async def verify_company_event(
    ticker: str,
    event_query: str,
    start_date: str = "",
    end_date: str = "",
    sources: Annotated[list[str] | None, Field(description=_SOURCES_DEFAULT_DESCRIPTION)] = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("verify_company_event", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if not str(event_query or "").strip():
        return _mcp_failure("verify_company_event", ErrorCode.INPUT_VALIDATION_ERROR, "event_query is required")
    effective_sources = _DEFAULT_EVENT_SOURCES if sources is None else sources
    items, sources_used, warnings, retrieved_at, source_diagnostics = _unpack_company_event_result(
        # Match against everything collected: a cap of the newest 50 let a flood
        # of aggregator items push the official release out before matching
        # (ASTS BlueBird launch, 2.4.5).
        await _collect_company_events(
            ticker,
            max_results=50,
            lookback_days=365,
            sources=effective_sources,
            include_diagnostics=True,
            keep_all=True,
        )
    )
    event_query_stopwords = {
        "announce", "announced", "announcement", "announcements", "announces",
        "company", "corporate", "latest", "news", "official", "press", "release",
        "report", "reported", "reports", "result", "results", "today", "update",
        "updated", "updates",
    }
    query_text = _normalize_event_text(event_query)
    meaningful_terms = [
        term for term in query_text.split()
        if len(term) >= 3 and term not in event_query_stopwords
    ]
    required_term_matches = (
        len(meaningful_terms)
        if len(meaningful_terms) <= 2
        else int(math.ceil(len(meaningful_terms) * 0.67))
    )

    def _query_match(item: dict) -> dict:
        hay = _normalize_event_text(" ".join([
            str(item.get("title") or ""),
            str(item.get("summary") or ""),
            str(item.get("evidenceText") or ""),
            str(item.get("eventType") or ""),
            str(item.get("source") or ""),
        ]))
        # Stemmed, so "launch" matches "launches" and "launched" (yfmcp/extraction_rules.py).
        words = {_er.stem_word(w) for w in hay.split()}
        matched_terms = [term for term in meaningful_terms if _er.stem_word(term) in words]
        if query_text and query_text in hay:
            return {"matched": True, "method": "PHRASE", "matchedTerms": matched_terms, "requiredTermMatches": required_term_matches}
        if required_term_matches > 0 and len(matched_terms) >= required_term_matches:
            return {"matched": True, "method": "TERM_THRESHOLD", "matchedTerms": matched_terms, "requiredTermMatches": required_term_matches}
        return {"matched": False, "method": "NONE", "matchedTerms": matched_terms, "requiredTermMatches": required_term_matches}

    if not meaningful_terms:
        warnings.append({
            "code": "EVENT_QUERY_TOO_GENERIC",
            "message": "The event query contains no specific event terms. Add a term such as acquisition, dividend, guidance, contract, or offering.",
            "severity": "warning",
        })

    matched = [it for it in items if _query_match(it)["matched"]]
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
    matched_group_ids = {
        str(item.get("duplicateGroupId") or "")
        for item in matched
        if str(item.get("duplicateGroupId") or "")
    }
    conflicts = [
        {
            "type": "evidence",
            "duplicateGroupId": str(warning.get("duplicateGroupId") or ""),
            "message": str(warning.get("message") or "Conflicting evidence observed for the matched event."),
        }
        for warning in warnings
        if isinstance(warning, dict)
        and warning.get("code") == "EVIDENCE_CONFLICT"
        and str(warning.get("duplicateGroupId") or "") in matched_group_ids
    ]

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

    # Stronger evidence first, then newest: a LOW-confidence aggregator item
    # never outranks a press release that matched the same query.
    best = _er.rank_evidence(official_in_range or matched_in_range or matched, lambda ev: str(ev.get("confidence") or ""))

    # ── Ticker/entity relevance filtering ────────────────────────────────
    # Ensure bestEvidence items actually reference the queried ticker/entity
    # to prevent returning evidence about unrelated companies.
    ticker_upper = ticker.upper()
    ticker_pattern = _re.compile(r'\b' + _re.escape(ticker_upper) + r'\b', _re.IGNORECASE)

    # The company name only scores bestEvidence relevance, so no evidence means no SEC lookup
    # (sources=[] must make no request).
    cik_padded, submissions = await _get_submissions_for_ticker(ticker) if best else (None, None)
    company_name = ""
    if submissions and isinstance(submissions, dict):
        company_name = str(submissions.get("name") or "").strip()

    def _extract_base_company_name(name: str) -> str:
        if not name:
            return ""
        name_clean = name.upper()
        suffixes = [
            " INC", " CORP", " CORPORATION", " LTD", " LIMITED", " PLC",
            " CO", " COMPANY", " S.A.", " AG", " GMBH", " S.A.B. DE C.V.",
            " GROUP", " HOLDINGS", " TRUST"
        ]
        for p in (".", ",", "/"):
            name_clean = name_clean.replace(p, "")
        for suff in suffixes:
            if name_clean.endswith(suff):
                name_clean = name_clean[:-len(suff)]
        return name_clean.strip()

    base_company_name = _extract_base_company_name(company_name)

    def _relevance_score(ev: dict) -> str:
        """Return 'HIGH', 'MEDIUM', or 'LOW' based on ticker/entity presence."""
        # 1. Check explicit tickers list
        ev_tickers = ev.get("tickers") or []
        if any(str(t).upper() == ticker_upper for t in ev_tickers):
            return "HIGH"
            
        # 2. Check source type
        source_type = str(ev.get("sourceType") or "")
        if source_type in ("sec_filing", "sec", "company_ir", "sec_ex99_found"):
            return "HIGH"
            
        # 3. Check text content
        hay = " ".join([
            str(ev.get("title") or ""),
            str(ev.get("summary") or ""),
            str(ev.get("evidenceText") or ""),
            str(ev.get("issuer") or ""),
        ])
        
        # Word boundary match for ticker
        if ticker_pattern.search(hay):
            return "HIGH"
            
        # Check company name/base name
        if base_company_name and base_company_name.lower() in hay.lower():
            return "HIGH"
        if company_name and company_name.lower() in hay.lower():
            return "HIGH"
            
        # Check issuer field explicitly
        ev_issuer = str(ev.get("issuer") or "").upper()
        if ticker_upper in ev_issuer:
            return "HIGH"
            
        return "LOW"

    best_evidence = []
    for ev in best[:5]:
        relevance = _relevance_score(ev)
        match = _query_match(ev)
        evidence_item = {
            "source": ev.get("source"),
            "sourceType": ev.get("sourceType"),
            "publishedAt": ev.get("publishedAt"),
            "retrievedAt": ev.get("retrievedAt"),
            "url": ev.get("url"),
            "confidence": ev.get("confidence"),
            "relevance": relevance,
            "entityRelevance": relevance,
            "queryMatch": {
                "method": match["method"],
                "matchedTerms": match["matchedTerms"],
                "requiredTermMatches": match["requiredTermMatches"],
            },
            "evidenceText": _short_text(ev.get("evidenceText") or ev.get("summary") or ev.get("title")),
        }
        best_evidence.append(evidence_item)

    # If all best evidence is LOW relevance, downgrade status
    if best_evidence and all(e.get("relevance") == "LOW" for e in best_evidence):
        if status == "CONFIRMED":
            status = "PARTIAL"
            warnings.append({
                "code": "LOW_RELEVANCE_EVIDENCE",
                "message": f"Evidence found but none contain word-boundary match for ticker '{ticker_upper}'. Confidence downgraded.",
                "severity": "warning",
            })

    source_status = _compute_source_status(sources_used, warnings, items, effective_sources, source_diagnostics)
    source_coverage = _compute_source_coverage(source_status)
    coverage = _build_coverage(source_status)
    coverage_failure_mode = _verify_coverage_failure_mode(source_status, warnings)
    if status == "NOT_FOUND" and coverage_failure_mode:
        status = "SOURCE_LIMITED_NOT_FOUND"
        warnings.append({
            "code": "SOURCE_LIMITED_NOT_FOUND",
            "message": "One or more selected sources were unavailable; this is not confirmed evidence that the event did not occur.",
            "severity": "warning",
            "failureMode": coverage_failure_mode,
        })
    failure_mode = coverage_failure_mode if status == "SOURCE_LIMITED_NOT_FOUND" else None
    retryable = failure_mode in ("WORKER_SUBREQUEST_LIMIT", "RATE_LIMITED", "PROVIDER_TIMEOUT")

    payload = {
        "ticker": ticker_upper,
        "query": event_query,
        "queryPolicy": {
            "meaningfulTerms": meaningful_terms,
            "requiredTermMatches": required_term_matches,
            "matchedEvidenceCount": len(matched),
        },
        "status": status,
        "bestEvidence": best_evidence,
        "conflicts": conflicts,
        "meta": {
            "sourcesChecked": sources_used,
            "watermark": retrieved_at,
        },
        "warnings": warnings,
        "sourceCoverage": source_coverage,
        "coverage": coverage,
        "sourceStatus": source_status,
        **_source_request_fields(sources, source_status),
        "failureMode": failure_mode,
        "retryable": retryable,
        "recommendedAction": (
            "Inspect sourceStatus and retry with a narrower sources list that excludes failed providers; do not treat this result as confirmed absence."
            if failure_mode else None
        ),
    }
    return json.dumps(_mark_no_source_selected(payload, sources))


@yfinance_server.tool(
    name="get_company_news",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_yahoo_finance_news"],
    description="""Get recent public company news and press releases from selected public sources.

Accepts a single ticker or an array of up to 5 symbols. For an array, each
ticker is fetched independently and results are returned as a per-ticker keyed
object (a union of per-ticker results), so low-news conditions for one ticker
never zero out the others.

Returns deduplicated source-backed items with precise source labels
(``yahoo_finance_news``, ``yahoo_finance_press_releases``, ``finnhub``,
``marketaux``),
timestamps, URL, event classification, confidence, ticker relevance,
and short evidence excerpts.

Read ``coverage`` before treating an empty result as absence. It provides
``state``, failed/skipped sources, and a deterministic next action; use item
``tickerMatch`` and ``matchBasis`` before treating Yahoo context as issuer
evidence. Yahoo primary items require an explicit ticker, canonical issuer
name, or bounded issuer acronym; source status exposes raw/accepted/rejected
counts. ``decisionUse=CHECK_OFFICIAL_RELEASES`` means escalate a material item
to official-release or event verification. Legacy confidence is not a
cross-provider quality ranking.

Supported sources: ``yahoo_finance_news``, ``yahoo_finance_press_releases``,
``finnhub``, quota-aware ``marketaux``, ``sec``, ``company_ir`` (RSS/Atom
only), ``company_ir_page`` (Git-reviewed IR-page registry), explicit snapshot
``newswire``, and the legacy ``yahoo_finance`` aggregate alias.
""",
)
async def get_company_news(
    ticker: str | list[str],
    max_results: int = 10,
    lookback_days: int = 14,
    sources: Annotated[list[str] | None, Field(description=_GET_COMPANY_NEWS_SOURCES_DESCRIPTION)] = None,
) -> str:
    if isinstance(ticker, list) and (
        not ticker
        or any(not isinstance(item, str) or not item.strip() for item in ticker)
    ):
        return _mcp_failure(
            "get_company_news",
            ErrorCode.INPUT_VALIDATION_ERROR,
            "ticker is required and must not contain empty symbols.",
        )
    # Batch path: fetch each ticker independently and return a per-ticker keyed
    # object (a union of results), matching the other multi-ticker tools. News is
    # fetched per ticker; there is no combined query that could zero out the
    # whole batch under low-news conditions.
    if isinstance(ticker, list):
        results = await _run_ticker_batch(
            ticker, lambda t: get_company_news(t, max_results=max_results, lookback_days=lookback_days, sources=sources),
        )
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_company_news", ErrorCode.INPUT_VALIDATION_ERROR, err)
    effective_sources = _DEFAULT_NEWS_SOURCES if sources is None else sources
    # Collect a wider pool and keep the most relevant items, as the Worker does.
    safe_max = _coerce_max_results(max_results, 10)
    items, sources_used, warnings, retrieved_at, source_diagnostics = _unpack_company_event_result(
        await _collect_company_events(
            ticker,
            max_results=min(100, safe_max * 3),
            lookback_days=lookback_days,
            sources=effective_sources,
            include_diagnostics=True,
        )
    )
    items = _rank_news_items_by_relevance(items)[:safe_max]
    status = _build_collection_status(items, sources_used, warnings)
    source_status = _compute_source_status(sources_used, warnings, items, effective_sources, source_diagnostics)
    source_coverage = _compute_source_coverage(source_status)
    coverage = _build_coverage(source_status)
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
        "coverage": coverage,
        "sourceStatus": source_status,
        **_source_request_fields(sources, source_status),
    }
    if status:
        payload["status"] = status
    return json.dumps(_mark_no_source_selected(payload, sources))
