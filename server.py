import asyncio
import datetime
import email.utils as _email_utils
import hashlib
import html as _html_module
import inspect
import json
import os
from pathlib import Path
import re as _re
import time
import urllib.parse as _urlparse
import urllib.request as _urlrequest
import urllib.error as _urlerror
import xml.etree.ElementTree as _ET
from typing import Any, Literal, TypedDict
import zoneinfo

import pandas as pd
import yfinance as yf
from mcp.server.fastmcp import FastMCP

# Phase 2b: yfmcp.app owns the FastMCP compat shim, yfinance_server instance, TOOL_ALIASES, and
# build_handler_registry.  Import first so the compat shim fires before any decorator runs.
from yfmcp.app import yfinance_server, TOOL_ALIASES, build_handler_registry
from yfmcp.schemas import (
    FinancialType, HolderType, RecommendationType, FilingFactType,
    _TOOL_OUTPUT_SCHEMAS, _MARKET_SNAPSHOT_OUTPUT_SCHEMA,
)


# ---------------------------------------------------------------------------
# Phase 1: yfmcp infrastructure imports
# ---------------------------------------------------------------------------
import yfmcp.envelope as _envelope
from yfmcp.envelope import (
    SERVER_VERSION, _ENVELOPE_V2, ErrorCode, ToolMeta, ErrorDetail, McpResponse,
    _mcp_success, _mcp_failure, _mcp_warning, _wrap_envelope_v2,
)
from yfmcp.validation import (
    _TICKER_RE, _ACCESSION_RE,
    _validate_ticker, _validate_accession, _validate_batch_tickers,
    _validate_sec_url, _sanitize_sec_html,
)
from yfmcp.cache import (
    ToolCache, _tool_cache,
    TTL_PRICE, TTL_ANALYST, TTL_FINANCIALS, TTL_EDGAR, TTL_OPTIONS, TTL_NEWS,
    _PRICE_TTL, _STMT_TTL,
    _cache_get, _cache_set,
)
from yfmcp.util import (
    _fetch_with_retry, get_last_trading_date,
    _PLACEHOLDER_IV_THRESHOLD, _compute_data_quality, _sort_by_relevance,
    _utc_now_iso, _to_iso_utc, _parse_rss_date,
    _filter_paragraphs_by_topics, _safe_json_loads, _compact_excerpt,
)
from yfmcp.clients.yahoo import _safe_parse
from yfmcp.clients.edgar import (
    _SEC_REQUIRED_UA, _SMOKE_TICKER_CIK_FALLBACKS,
    _resolve_cik_for_ticker, _get_submissions_for_ticker,
    _EDGAR_TICKERS, _EDGAR_TICKERS_LOADED_AT, _EDGAR_TTL,
    _load_edgar_tickers, _edgar_get, EdgarError,
    _EDGAR_FACTS_CACHE, _EDGAR_SUBS_CACHE,
    _edgar_get_company_facts, _edgar_get_submissions,
    _edgar_build_filing_urls, _edgar_cik_from_accession,
    _edgar_list_exhibits_from_index,
    _edgar_primary_doc_from_index, _edgar_get_html,
)
from yfmcp.parsing.html import (
    _strip_html_tags, _parse_html_table, _parse_numeric_cell, _detect_unit_multiplier,
    _MD_MAX_CELL_CHARS, _html_table_to_markdown, _html_to_markdown_fallback,
)
from yfmcp.parsing.extractors import (
    _normalize_segment_label, _region_matches,
    _extract_geo_revenue_from_html,
    _REGION_XBRL_MEMBERS, _GEO_REVENUE_CONCEPTS, _GEO_AXIS,
    _extract_geographic_pct,
    _extract_xbrl_latest_annual,
)

# ---------------------------------------------------------------------------
# Phase 2b: domain tool modules — import triggers @yfinance_server.tool registration
# ---------------------------------------------------------------------------
import yfmcp.tools.system  # noqa: F401 (side-effect import)
import yfmcp.tools.pricing  # noqa: F401 (side-effect import)
import yfmcp.tools.thai_funds  # noqa: F401 (side-effect import)
from yfmcp.tools.system import health_check, get_manifest_diagnostics  # re-export for grouped routing
from yfmcp.tools.pricing import (  # re-export for compatibility and grouped routing
    get_historical_stock_prices,
    get_fast_info,
    get_price_stats,
    get_ma_position,
    get_volume_ratio,
    get_volume_gate,
    get_technical_indicators,
    get_price_slope,
    get_short_interest,
    get_short_momentum,
    get_overnight_quote,
    get_market_snapshot,
    _overnight_window_utc_for_session_end_date,
    _overnight_window_utc,
    _classify_overnight_session,
    _classify_freshness,
)
from yfmcp.tools.thai_funds import (  # re-export for grouped routing
    search_thai_funds,
    get_thai_fund_nav,
    get_thai_fund_nav_batch,
    get_thai_fund_factsheet,
    get_thai_fund_dividend_history,
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


@yfinance_server.tool(
    name="get_stock_info",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_stock_info"],
    description="""Get stock fundamentals for one or more ticker symbols from Yahoo Finance.

By default returns ~30 key fields covering identity, price, valuation, earnings, margins,
dividends, analyst ratings, and the business description — enough for most queries at a
fraction of the token cost of the full payload.

Pass include_all=true only when you specifically need fields outside the default set (e.g.
raw balance-sheet items, governance scores, or insider-ownership details).

For ETFs or mutual funds (SPY, QQQ, VTI, ARKK, etc.), use get_etf_info instead — it returns
fund-specific fields including NAV, expense ratio, top-10 holdings, and sector weights.

Default fields (~30): shortName, longName, sector, industry, country, website,
fullTimeEmployees, currentPrice, previousClose, marketCap, enterpriseValue, currency,
trailingPE, forwardPE, priceToBook, priceToSalesTrailing12Months, enterpriseToEbitda,
trailingEps, forwardEps, revenueGrowth, earningsGrowth, grossMargins, operatingMargins,
profitMargins, returnOnEquity, returnOnAssets, dividendYield, payoutRatio,
recommendationMean, numberOfAnalystOpinions, targetMeanPrice, longBusinessSummary.

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of symbols (e.g. ["AAPL", "MSFT"]).
        When a list is provided, returns a dict keyed by symbol.
    fields: list[str] | None
        Optional list of exact field names or group aliases to return.
        Group aliases: "identity", "pricing", "valuation", "earnings", "margins",
        "dividends", "analyst", "description".
        Mixing aliases and exact names is supported, e.g. ["pricing", "trailingPE"].
        Ignored when include_all=true.
    include_all: bool
        Set to true to return the full ~120-field payload. Default is false.
""",
)
async def get_stock_info(
    ticker: str | list[str],
    fields: list[str] | None = None,
    include_all: bool = False,
) -> str:
    """Get stock information for a given ticker symbol"""
    if isinstance(ticker, list):
        results = await asyncio.gather(
            *[get_stock_info(t, fields, include_all) for t in ticker],
            return_exceptions=True,
        )
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting stock information for {ticker}: {e}")
        return f"Error: getting stock information for {ticker}: {e}"
    info = company.info
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


@yfinance_server.tool(
    name="get_yahoo_finance_news",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_yahoo_finance_news"],
    description="""Deprecated alias for get_company_news.

Args:
    ticker: str
        The ticker symbol of the stock to get news for, e.g. "AAPL"
""",
)
async def get_yahoo_finance_news(ticker: str) -> str:
    """Alias for get_company_news. Routes to the canonical news tool with Yahoo Finance + Finnhub sources."""
    return await get_company_news(ticker, sources=["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"])


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
    "finnhub",
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
    "other": 6,
}


def _finnhub_eligibility(ticker: str) -> tuple[bool, str | None]:
    policy = json.loads(_NEWS_SOURCE_CAPABILITIES_PATH.read_text(encoding="utf-8"))["providers"]["finnhub"]
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
    enriched["tickerMatch"] = "EXPLICIT" if item.get("matchBasis") in {"TICKER_TOKEN", "ISSUER_NAME", "ISSUER_ACRONYM"} or (ticker and _re.search(rf"\b{_re.escape(ticker)}\b", text)) else ("SOURCE_SCOPED" if ticker else "UNVERIFIED")
    enriched["urlProvenance"] = _url_provenance_for(item)
    source_type = str(item.get("sourceType") or "")
    if source_type == "sec_ex99_found" or (source_type == "company_ir_page" and item.get("approvalStatus") == "approved"):
        enriched["decisionUse"] = "USE_OFFICIAL_EVIDENCE"
    elif source_type in {"sec_filing", "company_ir", "company_ir_page", "press_release", "yahoo_finance_press_releases", "newswire"}:
        enriched["decisionUse"] = "CHECK_OFFICIAL_RELEASES"
    elif source_type in {"yahoo_finance_news", "company_news"}:
        enriched["decisionUse"] = _yahoo_decision_use(str(item.get("eventType") or "other"))
    else:
        enriched["decisionUse"] = "CONTEXT_ONLY"
    return enriched


def _build_coverage(source_status: dict, resolved_action: str | None = None) -> dict:
    failed_states = {
        "UNCONFIGURED", "PROVIDER_ERROR", "AUTH_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_CHANGED",
        "IDENTITY_UNAVAILABLE",
        "WEBSITE_NOT_AVAILABLE", "DISCOVERY_NOT_FOUND", "DISCOVERY_BUDGET_EXHAUSTED", "FEED_NOT_FOUND",
        "PARSE_ERROR", "CANDIDATE_AVAILABLE", "NOT_REGISTERED", "DISABLED", "REVALIDATION_DUE", "SOURCE_VALIDATION_FAILED",
    }
    failed_sources: list[dict] = []
    skipped_sources: list[dict] = []
    for source, info in source_status.items():
        if not isinstance(info, dict):
            continue
        entry = {"source": source, "status": info.get("status"), "attempted": info.get("attempted") is not False, "reasonCode": info.get("reasonCode")}
        if info.get("status") == "NOT_ELIGIBLE":
            skipped_sources.append(entry)
        elif info.get("status") in failed_states:
            failed_sources.append(entry)
    action = resolved_action or (
        "RETRY_RETRYABLE_SOURCES" if any(row["status"] in {"RATE_LIMITED", "TIMEOUT"} for row in failed_sources) else (
            "CHECK_OFFICIAL_RELEASES" if failed_sources or skipped_sources else "USE_RETURNED_CONTEXT"
        )
    )
    return {"state": _compute_source_coverage(source_status), "failedSources": failed_sources, "skippedSources": skipped_sources, "recommendedNextAction": action}


_YAHOO_NEWS_LEGAL_SUFFIXES = frozenset({
    "inc", "incorporated", "corp", "corporation", "ltd", "limited", "llc", "plc",
    "co", "company", "sa", "ag", "nv", "se", "gmbh",
})
_YAHOO_NEWS_ACRONYM_IGNORED_WORDS = frozenset({"the", "and", "of", "for"})


def _normalized_yahoo_news_phrase(value: object) -> str:
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
        normalized = _normalized_yahoo_news_phrase(raw_name)
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


def _yahoo_news_match_for(text: str, ticker: str, identity: dict) -> tuple[str, int] | None:
    if _is_ticker_compatible_with_context(text, ticker, identity.get("exchange")):
        return "TICKER_TOKEN", 0
    if identity.get("status") != "RESOLVED":
        return None
    normalized_text = f" {_normalized_yahoo_news_phrase(text)} "
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
_PRE_REVENUE_EPS_EPSILON = 1e-9
_FINNHUB_NEWS_API = "https://finnhub.io/api/v1/company-news"
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
_COMPANY_IR_PAGE_REGISTRY_PATH = Path(__file__).resolve().parent / "worker" / "src" / "company-ir-page-registry.json"
_NEWS_SOURCE_CAPABILITIES_PATH = Path(__file__).resolve().parent / "worker" / "src" / "news-source-capabilities.json"
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
    source_list = [str(s).strip().lower() for s in (sources or default_sources) if str(s).strip()]
    if not source_list:
        source_list = list(default_sources)
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
    if not normalized:
        normalized = [s for s in default_sources if s in _PHASE6B_SUPPORTED_SOURCES]
    return normalized, warnings


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


def _canonicalize_event_url(url: str | None) -> str | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    try:
        parsed = _urlparse.urlparse(raw)
        if parsed.scheme not in ("http", "https"):
            return None
        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            params="",
            query="",
            fragment="",
        )
        return _urlparse.urlunparse(normalized)
    except Exception:
        return None


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

    def _fetch() -> tuple[str, str, str]:
        req = _urlrequest.Request(
            canonical_url,
            headers={
                "User-Agent": "yahoo-finance-mcp/ir-page-registry",
                "Accept": "application/json, application/xml, text/xml, text/html;q=0.9",
            },
        )
        with _urlrequest.urlopen(req, timeout=15) as resp:
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
        if feed == "press_releases":
            try:
                # ``get_news(tab=...)`` was introduced in yfinance ≥ 0.2.x.
                raw_news = company.get_news(tab="press releases") or []
            except Exception:
                # Do NOT fall back to the general feed — mislabeling generic
                # news items as press releases would corrupt source fidelity.
                # The yahoo_finance_news path fetches the general feed separately.
                warnings.append({
                    "code": "PRESS_RELEASE_TAB_UNAVAILABLE",
                    "message": (
                        "Yahoo Finance press-releases tab unavailable "
                        "(requires yfinance ≥ 0.2.x with get_news(tab=...) support)."
                    ),
                    "severity": "warning",
                })
                return items, warnings, False
        else:
            try:
                # ``get_news(tab=...)`` was introduced in yfinance ≥ 0.2.x.
                # Falls back to company.news on older versions.
                raw_news = company.get_news(tab="news") or []
            except Exception:
                raw_news = company.news or []
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
        item["issuer"] = identity.get("companyName")
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
                    headers={"User-Agent": _SEC_REQUIRED_UA},
                )
                with _urlrequest.urlopen(req, timeout=20) as resp:  # noqa: S310
                    raw = resp.read(_GLOBENEWSWIRE_MAX_BYTES + 1)
                    if len(raw) > _GLOBENEWSWIRE_MAX_BYTES:
                        raise ValueError("GlobeNewswire RSS response exceeded size limit")
                    return raw.decode("utf-8", errors="replace")

            try:
                xml_content = await loop.run_in_executor(None, _fetch_rss)
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
) -> tuple[list[dict], list[dict], bool]:
    warnings: list[dict] = []
    items: list[dict] = []
    api_key = os.environ.get("FINNHUB_API_KEY") or os.environ.get("FINNHUB_TOKEN")
    if not api_key:
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub company-news source is not configured; skipped.",
            "severity": "warning",
        })
        return items, warnings, False

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
            headers={"User-Agent": _SEC_REQUIRED_UA, "X-Finnhub-Token": api_key},
        )
        with _urlrequest.urlopen(req, timeout=20) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    try:
        raw_items = await loop.run_in_executor(None, _fetch)
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
        return items, warnings, False
    except Exception as exc:
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": f"Finnhub source unavailable: {exc}",
            "severity": "warning",
        })
        return items, warnings, False

    if not isinstance(raw_items, list):
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub provider changed: unexpected response format",
            "severity": "warning",
        })
        return items, warnings, False

    ticker_u = ticker.upper()
    for row in raw_items:
        if not isinstance(row, dict):
            continue
        title = str(row.get("headline") or "").strip()
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
            continue
        items.append(item)
        if len(items) >= max_results:
            break
    return items, warnings, True


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
) -> dict:
    """Build per-source status dict from collection results.

    Supports both the new fine-grained source names (``yahoo_finance_news``,
    ``yahoo_finance_press_releases``) and the legacy ``yahoo_finance`` aggregate
    name for backward compatibility.
    """
    warning_msgs = [w.get("message", "") for w in warnings if isinstance(w, dict) and w.get("code") == "SOURCE_UNAVAILABLE"]
    warning_codes = {w.get("code") for w in warnings if isinstance(w, dict)}
    sec_items = [it for it in items if "sec" in str(it.get("sourceType", "")).lower()]
    sources = selected_sources or ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"]

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
            result["finnhub"] = {"status": "OK" if finnhub_items else "EMPTY_RESULT", "rawCount": len(finnhub_items), "filteredCount": len(finnhub_items)}
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
    return result


def _compute_source_coverage(source_status: dict) -> str:
    """Return PARTIAL if any source is UNCONFIGURED or has an error, else FULL."""
    for info in source_status.values():
        s = info.get("status", "") if isinstance(info, dict) else ""
        if s in (
            "UNCONFIGURED", "NOT_ELIGIBLE", "PROVIDER_ERROR", "AUTH_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_CHANGED",
            "IDENTITY_UNAVAILABLE",
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
    if "too many subrequests" in warning_text:
        return "WORKER_SUBREQUEST_LIMIT"
    source_states = {
        str(info.get("status") or "")
        for info in source_status.values()
        if isinstance(info, dict)
    }
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
    include_diagnostics: bool = False,
) -> tuple:
    retrieved_at = _utc_now_iso()
    selected_sources, warnings = _normalize_event_sources(
        sources,
        ["sec", "company_ir_page", "company_ir", "newswire", "yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"],
    )
    items: list[dict] = []
    sources_used: list[str] = []
    source_diagnostics: dict[str, dict] = {}
    max_cap = _coerce_max_results(max_results, 10)
    lookback = _coerce_lookback_days(lookback_days, 14)

    if "sec" in selected_sources:
        sec_items, sec_warnings, used = await _collect_sec_events(
            ticker,
            filing_types=sec_filing_types or ["8-K", "10-Q", "10-K", "S-3", "DEF14A"],
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
        )
        if used:
            sources_used.append("sec")
        items.extend(sec_items)
        warnings.extend(sec_warnings)

    if "company_ir_page" in selected_sources:
        page_items, page_warnings, page_used = await _collect_company_ir_page_events(
            ticker,
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
        )
        if page_used:
            sources_used.append("company_ir_page")
        items.extend(page_items)
        warnings.extend(page_warnings)

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

    yahoo_identity: dict | None = None
    if _need_yf_news or _need_yf_pr:
        try:
            yahoo_identity = _yahoo_news_identity_from_info(ticker, yf.Ticker(ticker).info)
        except Exception:
            yahoo_identity = _yahoo_news_identity_from_info(ticker, None)
        if yahoo_identity.get("status") == "UNAVAILABLE":
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

    def _unpack_yahoo_result(value: tuple) -> tuple[list[dict], list[dict], bool, dict]:
        if len(value) == 4:
            result_items, result_warnings, result_used, result_diagnostics = value
            return result_items, result_warnings, result_used, result_diagnostics
        result_items, result_warnings, result_used = value
        return result_items, result_warnings, result_used, {}

    if _need_yf_news:
        yf_result = await _collect_yahoo_events(
            ticker,
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
            feed="news",
            identity=yahoo_identity,
            include_diagnostics=True,
        )
        yf_items, yf_warnings, used, yf_diagnostics = _unpack_yahoo_result(yf_result)
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

    if _need_yf_pr:
        pr_result = await _collect_yahoo_events(
            ticker,
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
            feed="press_releases",
            identity=yahoo_identity,
            include_diagnostics=True,
        )
        pr_items, pr_warnings, used, pr_diagnostics = _unpack_yahoo_result(pr_result)
        source_diagnostics["yahoo_finance_press_releases"] = {**pr_diagnostics, "attempted": True, "completed": used}
        if used and "yahoo_finance_press_releases" in selected_sources and "yahoo_finance_press_releases" not in sources_used:
            sources_used.append("yahoo_finance_press_releases")
        items.extend(pr_items)
        warnings.extend(pr_warnings)

    if "newswire" in selected_sources:
        gnw_items, gnw_warnings, gnw_used = await _collect_globenewswire_events(
            ticker,
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
        )
        if gnw_used:
            sources_used.append("newswire")
        items.extend(gnw_items)
        warnings.extend(gnw_warnings)

    if "finnhub" in selected_sources and _finnhub_eligibility(ticker)[0]:
        finnhub_items, finnhub_warnings, used = await _collect_finnhub_events(
            ticker,
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
        )
        if used:
            sources_used.append("finnhub")
        items.extend(finnhub_items)
        warnings.extend(finnhub_warnings)
    elif "finnhub" in selected_sources:
        _, reason_code = _finnhub_eligibility(ticker)
        warnings.append({
            "code": "SOURCE_NOT_ELIGIBLE",
            "message": f"Finnhub company-news is intentionally skipped for {ticker.upper()} under the deployed market capability policy.",
            "severity": "warning",
            "source": "finnhub",
            "attempted": False,
            "reasonCode": reason_code,
        })

    deduped = [_enrich_news_item_for_llm(item) for item in _dedupe_event_items(items, warnings)]
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


@yfinance_server.tool(
    name="search_company_news",
    output_schema=_TOOL_OUTPUT_SCHEMAS["search_company_news"],
    description="""Search public company news/events for a ticker and query string.

Returns source-backed, deduplicated event items with source type, published/retrieved timestamps,
URL, confidence, relevance, and short evidence excerpts.
""",
)
async def search_company_news(
    ticker: str,
    query: str,
    start_date: str = "",
    end_date: str = "",
    sources: list[str] | None = None,
    max_results: int = 10,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("search_company_news", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if not str(query or "").strip():
        return _mcp_failure("search_company_news", ErrorCode.INPUT_VALIDATION_ERROR, "query is required")
    effective_sources = sources or ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"]
    items, sources_used, warnings, retrieved_at, source_diagnostics = _unpack_company_event_result(
        await _collect_company_events(
            ticker,
            max_results=max_results,
            lookback_days=14,
            start_date=start_date,
            end_date=end_date,
            sources=effective_sources,
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
    status = _build_collection_status(filtered, sources_used, warnings)
    source_status = _compute_source_status(sources_used, warnings, items, effective_sources, source_diagnostics)
    source_coverage = _compute_source_coverage(source_status)
    payload = {
        "ticker": ticker.upper(),
        "query": query,
        "items": filtered[:_coerce_max_results(max_results, 10)],
        "meta": {
            "sourcesUsed": sources_used,
            "deduped": True,
            "watermark": retrieved_at,
        },
        "warnings": warnings,
        "sourceCoverage": source_coverage,
        "coverage": _build_coverage(source_status),
        "sourceStatus": source_status,
    }
    if status:
        payload["status"] = status
    return json.dumps(payload)


@yfinance_server.tool(
    name="get_company_press_releases",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_company_press_releases"],
    description="""Get company press releases and official release-style public events.

Defaults resolve SEC 8-K/EX-99 evidence first, then registry-backed
``company_ir_page``, then Yahoo press-release context. Explicit sources can
also include ``company_ir`` RSS/Atom and ``newswire``. Items are labelled with
precise source identifiers so callers can distinguish their origin.

Decision-grade use is payload-level only: ``decisionGrade:true`` requires
``coverageStatus`` of ``SEC_EX99_RESOLVED`` or
``APPROVED_IR_PAGE_RESOLVED`` with evidence fields.
""",
)
async def get_company_press_releases(
    ticker: str,
    lookback_days: int = 90,
    max_results: int = 20,
    sources: list[str] | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_company_press_releases", ErrorCode.INPUT_VALIDATION_ERROR, err)
    selected_sources, source_warnings = _normalize_event_sources(
        sources, ["sec", "company_ir_page", "yahoo_finance_press_releases"]
    )
    primary_sources = [src for src in selected_sources if src in ("sec", "company_ir_page")]
    optional_sources = [src for src in selected_sources if src not in ("sec", "company_ir_page")]
    safe_max = _coerce_max_results(max_results, 20)
    primary_items: list[dict] = []
    primary_used: list[str] = []
    primary_warnings: list[dict] = []
    primary_diagnostics: dict = {}
    retrieved_at = _utc_now_iso()
    if primary_sources:
        primary_items, primary_used, primary_warnings, retrieved_at, primary_diagnostics = _unpack_company_event_result(
            await _collect_company_events(
                ticker,
                max_results=safe_max,
                lookback_days=lookback_days,
                sources=primary_sources,
                sec_filing_types=["8-K"],
                include_diagnostics=True,
            )
        )
    release_types = {"company_ir", "company_ir_page", "press_release", "newswire", "sec_filing", "sec_ex99_found", "yahoo_finance_press_releases"}
    release_items = [it for it in primary_items if str(it.get("sourceType")) in release_types]

    modified_primary_items: list[dict] = []
    has_sec_ex99_found = False
    sec_8k_evidence: list[dict] = []
    sec_8k_without_ex99_count = 0
    from yfmcp.tools.earnings import _resolve_ex991_url
    for it in release_items:
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
            cik_match = _re.search(r'/data/(\d+)/', url)
            cik = int(cik_match.group(1)) if cik_match else None
            if acc and cik is not None:
                ex991_url = await _resolve_ex991_url(acc, cik)
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
            else:
                sec_8k_without_ex99_count += 1
        modified_primary_items.append(it)

    optional_items: list[dict] = []
    optional_used: list[str] = []
    optional_warnings: list[dict] = []
    optional_diagnostics: dict = {}
    if optional_sources:
        optional_items, optional_used, optional_warnings, _optional_retrieved_at, optional_diagnostics = _unpack_company_event_result(
            await _collect_company_events(
                ticker,
                max_results=safe_max,
                lookback_days=lookback_days,
                sources=optional_sources,
                sec_filing_types=["8-K"],
                include_diagnostics=True,
            )
        )
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
        "coverageStatus": coverage_status,
        "decisionGrade": decision_grade,
        "decisionGradeBasis": decision_grade_basis,
        "capabilityStatus": "ACTIVE",
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
    return json.dumps(payload)


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
    source_status = {"sec": {"status": "OK" if items else "EMPTY_RESULT"} if used else {"status": "PROVIDER_ERROR"}}
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

Combines selected public sources, deduplicates related items, and returns timeline entries ordered by time.
""",
)
async def get_public_event_timeline(
    ticker: str,
    start_date: str = "",
    end_date: str = "",
    sources: list[str] | None = None,
    max_results: int = 50,
    newest_first: bool = False,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_public_event_timeline", ErrorCode.INPUT_VALIDATION_ERROR, err)
    items, sources_used, warnings, retrieved_at, source_diagnostics = _unpack_company_event_result(
        await _collect_company_events(
            ticker,
            max_results=max_results,
            lookback_days=365,
            start_date=start_date,
            end_date=end_date,
            sources=sources,
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
    source_status = _compute_source_status(sources_used, warnings, items, sources, source_diagnostics)
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
    }
    if status:
        payload["status"] = status
    return json.dumps(payload)


@yfinance_server.tool(
    name="verify_company_event",
    output_schema=_TOOL_OUTPUT_SCHEMAS["verify_company_event"],
    description="""Verify whether a public company event has source-backed evidence.

Returns CONFIRMED, PARTIAL, NOT_FOUND, SOURCE_LIMITED_NOT_FOUND, STALE, or CONFLICTING.
SOURCE_LIMITED_NOT_FOUND means selected provider coverage was incomplete, not that
the event is confirmed absent; inspect sourceStatus and failureMode for recovery.
Generic publication words such as announced, report, results, and update do not
establish a match by themselves; inspect queryPolicy and each queryMatch.
""",
)
async def verify_company_event(
    ticker: str,
    event_query: str,
    start_date: str = "",
    end_date: str = "",
    sources: list[str] | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("verify_company_event", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if not str(event_query or "").strip():
        return _mcp_failure("verify_company_event", ErrorCode.INPUT_VALIDATION_ERROR, "event_query is required")
    items, sources_used, warnings, retrieved_at, source_diagnostics = _unpack_company_event_result(
        await _collect_company_events(
            ticker,
            max_results=50,
            lookback_days=365,
            sources=sources,
            include_diagnostics=True,
        )
    )
    def _normalize_event_text(value: object) -> str:
        return _re.sub(r"\s+", " ", _re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()

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
        words = set(hay.split())
        matched_terms = [term for term in meaningful_terms if term in words]
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

    best = official_in_range or matched_in_range or matched

    # ── Ticker/entity relevance filtering ────────────────────────────────
    # Ensure bestEvidence items actually reference the queried ticker/entity
    # to prevent returning evidence about unrelated companies.
    ticker_upper = ticker.upper()
    ticker_pattern = _re.compile(r'\b' + _re.escape(ticker_upper) + r'\b', _re.IGNORECASE)

    cik_padded, submissions = await _get_submissions_for_ticker(ticker)
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

    source_status = _compute_source_status(sources_used, warnings, items, sources, source_diagnostics)
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

    return json.dumps({
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
        "failureMode": failure_mode,
        "retryable": retryable,
        "recommendedAction": (
            "Inspect sourceStatus and retry with a narrower sources list that excludes failed providers; do not treat this result as confirmed absence."
            if failure_mode else None
        ),
    })


@yfinance_server.tool(
    name="get_stock_actions",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_stock_actions"],
    description="""Get stock dividends and stock splits for a given ticker symbol from yahoo finance.

Args:
    ticker: str
        The ticker symbol of the stock to get stock actions for, e.g. "AAPL"
""",
)
async def get_stock_actions(ticker: str) -> str:
    """Get stock dividends and stock splits for a given ticker symbol"""
    try:
        company = yf.Ticker(ticker)
    except Exception as e:
        print(f"Error: getting stock actions for {ticker}: {e}")
        return f"Error: getting stock actions for {ticker}: {e}"
    actions_df = company.actions
    actions_df = actions_df.reset_index(names="Date")
    return actions_df.to_json(orient="records", date_format="iso")


@yfinance_server.tool(
    name="get_financial_statement",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_financial_statement"],
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
    cached = _cache_get(cache_key, _STMT_TTL)
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
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting financial statement for {ticker}: {e}")
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
        print(f"Error: getting financial statement for {ticker}: {e}")
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

    _cache_set(cache_key, result)

    if line_items:
        try:
            rows = json.loads(result)
            filtered = [r for r in rows if r.get("lineItem") in line_items]
            return json.dumps(filtered)
        except Exception:
            pass
    return result


@yfinance_server.tool(
    name="get_holder_info",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_holder_info"],
    description="""Get holder information for a given ticker symbol from yahoo finance. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.

Args:
    ticker: str
        The ticker symbol of the stock to get holder information for, e.g. "AAPL"
    holder_type: str
        The type of holder information to get. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.
""",
)
async def get_holder_info(ticker: str, holder_type: str) -> str:
    """Get holder information for a given ticker symbol"""

    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting holder info for {ticker}: {e}")
        return f"Error: getting holder info for {ticker}: {e}"

    if holder_type == HolderType.major_holders:
        return company.major_holders.reset_index(names="metric").to_json(orient="records")
    elif holder_type == HolderType.institutional_holders:
        return company.institutional_holders.to_json(orient="records")
    elif holder_type == HolderType.mutualfund_holders:
        return company.mutualfund_holders.to_json(orient="records", date_format="iso")
    elif holder_type == HolderType.insider_transactions:
        return company.insider_transactions.to_json(orient="records", date_format="iso")
    elif holder_type == HolderType.insider_purchases:
        return company.insider_purchases.to_json(orient="records", date_format="iso")
    elif holder_type == HolderType.insider_roster_holders:
        return company.insider_roster_holders.to_json(orient="records", date_format="iso")
    else:
        return f"Error: invalid holder type {holder_type}. Please use one of the following: {HolderType.major_holders}, {HolderType.institutional_holders}, {HolderType.mutualfund_holders}, {HolderType.insider_transactions}, {HolderType.insider_purchases}, {HolderType.insider_roster_holders}."


@yfinance_server.tool(
    name="get_option_expiration_dates",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_option_expiration_dates"],
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
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting option expiration dates for {ticker}: {e}")
        return f"Error: getting option expiration dates for {ticker}: {e}"
    return json.dumps(company.options)


@yfinance_server.tool(
    name="get_option_chain",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_option_chain"],
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
    min_strike: float | None = None,  # legacy alias
    max_strike: float | None = None,  # legacy alias
    in_the_money_only: bool = False,  # legacy alias
) -> str:
    """Fetch the option chain for a given ticker symbol, expiration date, and option type.

    Args:
        ticker: The ticker symbol of the stock
        expiration_date: The expiration date for the options chain (format: 'YYYY-MM-DD')
        option_type: The type of option to fetch ('calls' or 'puts')
        min_strike: Optional minimum strike price filter.
        max_strike: Optional maximum strike price filter.
        in_the_money_only: If True, only return in-the-money options.

    Returns:
        str: JSON string containing the option chain data
    """

    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting option chain for {ticker}: {e}")
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

    effective_strike_min = strike_min if strike_min is not None else min_strike
    effective_strike_max = strike_max if strike_max is not None else max_strike
    if in_the_money_only and moneyness == "all":
        moneyness = "itm"

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


@yfinance_server.tool(
    name="get_options_summary",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_options_summary"],
    description="Get options summary for a single ticker: ATM implied volatility, put/call ratio by volume and OI, max pain strike for the nearest or requested expiry. Preferred for data-source use because it returns a compact snapshot without the full contract list.",
)
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
        expiry = expiry_hint or expirations[0]
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
        return _mcp_failure(
            "get_options_summary",
            ErrorCode.PROVIDER_ERROR,
            str(e),
            meta_extra={"error_extra": {"ticker": ticker.upper()}},
        )


@yfinance_server.tool(
    name="list_sec_filings",
    output_schema=_TOOL_OUTPUT_SCHEMAS["list_sec_filings"],
    description="""List recent SEC filings for a ticker from EDGAR.
Returns accession number, filing date, form type, primary document URL, and EDGAR index URL.
Supports form types: 10-K, 10-Q, 8-K, DEF 14A.
Args:
    ticker: str - The ticker symbol
    form_type: str - Optional form type filter (10-K, 10-Q, 8-K, DEF 14A). Default: 10-K
    max_filings: int - Maximum filings to return (default: 5, max: 20)
""",
)
async def list_sec_filings(ticker: str, form_type: str = "10-K", max_filings: int = 5) -> str:
    ALLOWED_FORMS = {"10-K", "10-Q", "8-K", "DEF 14A"}
    if form_type not in ALLOWED_FORMS:
        return _mcp_failure("list_sec_filings", ErrorCode.INPUT_VALIDATION_ERROR,
                            f"Invalid form_type '{form_type}'. Must be one of: {sorted(ALLOWED_FORMS)}")
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_sec_filings", ErrorCode.INPUT_VALIDATION_ERROR, err)
    max_filings = min(max(1, max_filings), 20)

    import urllib.request
    ticker_upper = ticker.upper()
    try:
        tickers_url = "https://www.sec.gov/files/company_tickers.json"
        req = urllib.request.Request(tickers_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            tickers_data = json.loads(resp.read())

        cik = None
        for _entry in tickers_data.values():
            if _entry.get("ticker", "").upper() == ticker_upper:
                cik = int(_entry["cik_str"])
                break

        if cik is None:
            return _mcp_failure("list_sec_filings", ErrorCode.TICKER_NOT_FOUND,
                                f"Could not find EDGAR CIK for ticker '{ticker}'")

        cik_padded = str(cik).zfill(10)
        sub_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        req2 = urllib.request.Request(sub_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req2, timeout=10) as resp2:
            sub_data = json.loads(resp2.read())

        filings_data = sub_data.get("filings", {}).get("recent", {})
        forms = filings_data.get("form", [])
        dates = filings_data.get("filingDate", [])
        accessions = filings_data.get("accessionNumber", [])
        primary_docs = filings_data.get("primaryDocument", [])

        results = []
        for i, form in enumerate(forms):
            if form == form_type and len(results) < max_filings:
                acc = accessions[i] if i < len(accessions) else ""
                date = dates[i] if i < len(dates) else ""
                doc = primary_docs[i] if i < len(primary_docs) else ""
                acc_clean = acc.replace("-", "")
                index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{acc}-index.htm"
                doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{doc}" if doc else None
                results.append({
                    "accessionNumber": acc,
                    "filingDate": date,
                    "formType": form,
                    "primaryDocumentUrl": doc_url,
                    "edgarIndexUrl": index_url,
                })

        return json.dumps({"ticker": ticker, "formType": form_type, "filings": results})
    except Exception as e:
        return _mcp_failure("list_sec_filings", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="get_filing_outline",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_outline"],
    description="""Parse the document outline of an SEC filing (10-K/10-Q). Returns a hierarchical tree of Parts, Items, Notes as found in the document.
Args:
    ticker: str - ticker symbol
    accession_number: str - SEC accession number (format: XXXXXXXXXX-YY-ZZZZZZ)
    document_url: str - Optional direct URL to the filing HTML document (must be https://www.sec.gov/Archives/...)
""",
)
async def get_filing_outline(ticker: str, accession_number: str | None = None, document_url: str | None = None) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if document_url:
        url_err = _validate_sec_url(document_url)
        if url_err:
            return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR, url_err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)

    try:
        import urllib.request
        if not document_url and accession_number:
            cik = None
            tickers_url = "https://www.sec.gov/files/company_tickers.json"
            req = urllib.request.Request(tickers_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                tickers_data = json.loads(resp.read())
            for _entry in tickers_data.values():
                if _entry.get("ticker", "").upper() == ticker.upper():
                    cik = int(_entry["cik_str"])
                    break
            if cik:
                acc_clean = accession_number.replace("-", "")
                document_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{accession_number}-index.htm"

        if not document_url:
            return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR,
                                "Either accession_number or document_url is required")

        req = urllib.request.Request(document_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        html = _sanitize_sec_html(html)
        outline = []
        heading_re = _re.compile(r'<h([1-6])[^>]*>(.*?)</h\1>', _re.IGNORECASE | _re.DOTALL)
        item_re = _re.compile(r'(Part\s+[IVX]+|Item\s+\d+[A-Z]?|Note\s+\d+)', _re.IGNORECASE)
        for m in heading_re.finditer(html):
            level = int(m.group(1))
            text = _re.sub(r'<[^>]+>', '', m.group(2)).strip()
            text = ' '.join(text.split())
            if text and (item_re.search(text) or len(text) < 100):
                outline.append({"level": level, "title": text})

        return json.dumps({"ticker": ticker, "accessionNumber": accession_number, "outline": outline[:100]})
    except Exception as e:
        return _mcp_failure("get_filing_outline", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="get_filing_section",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_section"],
    description="""Retrieve the text content of a specific section from an SEC filing document.
Args:
    ticker: str - ticker symbol
    section_name: str - Section name/heading to find, e.g. 'Item 1A', 'Note 3', 'Risk Factors'
    document_url: str - Direct URL to filing HTML (must be https://www.sec.gov/Archives/...)
    context_chars: int - Characters of context around matched section (default: 3000)
""",
)
async def get_filing_section(ticker: str, section_name: str, document_url: str, context_chars: int = 3000) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_filing_section", ErrorCode.INPUT_VALIDATION_ERROR, err)
    url_err = _validate_sec_url(document_url)
    if url_err:
        return _mcp_failure("get_filing_section", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        import urllib.request
        req = urllib.request.Request(document_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        html = _sanitize_sec_html(html)
        start_idx, end_idx, found_heading, toc_skipped, err_code = _find_section_bounds(html, section_name, context_chars)
        if err_code == "SECTION_AMBIGUOUS":
            return _mcp_failure("get_filing_section", "SECTION_AMBIGUOUS", "The section heading could not be resolved unambiguously.")

        if start_idx is not None and end_idx is not None:
            section_html = html[start_idx:end_idx]
            plain_section = _re.sub(r'<[^>]+>', ' ', section_html)
            plain_section = ' '.join(plain_section.split())
            return json.dumps({
                "ticker": ticker,
                "sectionName": section_name,
                "found": True,
                "text": plain_section[:context_chars],
                "sectionStartOffset": start_idx,
                "sectionEndOffset": end_idx,
                "matchedHeading": found_heading,
                "tocSkipped": toc_skipped,
            })

        # Fallback to plain-text regex search if structure bounds not found
        text = _re.sub(r'<[^>]+>', ' ', html)
        text = ' '.join(text.split())

        pattern = _re.compile(_re.escape(section_name), _re.IGNORECASE)
        m = pattern.search(text)
        if not m:
            words = section_name.split()
            if words:
                pattern2 = _re.compile(r'\b' + r'\s+'.join(_re.escape(w) for w in words), _re.IGNORECASE)
                m = pattern2.search(text)

        if not m:
            return json.dumps({
                "ticker": ticker,
                "sectionName": section_name,
                "found": False,
                "text": None,
                "sectionStartOffset": None,
                "sectionEndOffset": None,
                "matchedHeading": "",
                "tocSkipped": toc_skipped,
            })

        start = max(0, m.start())
        end = min(len(text), m.start() + context_chars)
        return json.dumps({
            "ticker": ticker,
            "sectionName": section_name,
            "found": True,
            "text": text[start:end],
            "sectionStartOffset": start,
            "sectionEndOffset": end,
            "matchedHeading": section_name,
            "tocSkipped": toc_skipped,
        })
    except Exception as e:
        return _mcp_failure("get_filing_section", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="list_filing_tables",
    output_schema=_TOOL_OUTPUT_SCHEMAS["list_filing_tables"],
    description="""List all HTML tables in an SEC filing document. Returns table index, headers, and row count.
Args:
    ticker: str
    document_url: str - Direct URL to filing HTML (must be https://www.sec.gov/Archives/...)
""",
)
async def list_filing_tables(ticker: str, document_url: str) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_filing_tables", ErrorCode.INPUT_VALIDATION_ERROR, err)
    url_err = _validate_sec_url(document_url)
    if url_err:
        return _mcp_failure("list_filing_tables", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        import urllib.request
        req = urllib.request.Request(document_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        html = _sanitize_sec_html(html)
        tables = []
        table_re = _re.compile(r'<table[^>]*>(.*?)</table>', _re.DOTALL | _re.IGNORECASE)
        tr_re = _re.compile(r'<tr[^>]*>(.*?)</tr>', _re.DOTALL | _re.IGNORECASE)
        td_re = _re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', _re.DOTALL | _re.IGNORECASE)

        for i, table_m in enumerate(table_re.finditer(html)):
            rows = tr_re.findall(table_m.group(1))
            row_count = len(rows)
            headers = []
            if rows:
                first_cells = td_re.findall(rows[0])
                headers = [' '.join(_re.sub(r'<[^>]+>', '', c).split()) for c in first_cells[:6]]
            tables.append({"tableIndex": i, "rowCount": row_count, "headers": headers})

        return json.dumps({"ticker": ticker, "documentUrl": document_url, "tableCount": len(tables), "tables": tables[:50]})
    except Exception as e:
        return _mcp_failure("list_filing_tables", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="get_filing_table",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_table"],
    description="""Get the parsed rows of a specific table from an SEC filing document.
Args:
    ticker: str
    document_url: str - Direct URL to filing HTML (must be https://www.sec.gov/Archives/...)
    table_index: int - Table index from list_filing_tables (0-based)
    max_rows: int - Maximum rows to return (default: 30)
""",
)
async def get_filing_table(ticker: str, document_url: str, table_index: int, max_rows: int = 30) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_filing_table", ErrorCode.INPUT_VALIDATION_ERROR, err)
    url_err = _validate_sec_url(document_url)
    if url_err:
        return _mcp_failure("get_filing_table", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        import urllib.request
        req = urllib.request.Request(document_url, headers={"User-Agent": "yahoo-finance-mcp/1.0 admin@example.com"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        html = _sanitize_sec_html(html)
        table_re = _re.compile(r'<table[^>]*>(.*?)</table>', _re.DOTALL | _re.IGNORECASE)
        tr_re = _re.compile(r'<tr[^>]*>(.*?)</tr>', _re.DOTALL | _re.IGNORECASE)
        td_re = _re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', _re.DOTALL | _re.IGNORECASE)

        tables = list(table_re.finditer(html))
        if table_index >= len(tables):
            return _mcp_failure("get_filing_table", ErrorCode.NO_FILING_DATA,
                                f"Table index {table_index} not found. Document has {len(tables)} tables.")

        table_html = tables[table_index].group(1)
        rows = tr_re.findall(table_html)
        parsed_rows = []
        for row in rows[:max_rows + 1]:
            cells = td_re.findall(row)
            parsed_rows.append([' '.join(_re.sub(r'<[^>]+>', '', c).split()) for c in cells])

        return json.dumps({
            "ticker": ticker,
            "tableIndex": table_index,
            "totalRows": len(rows),
            "returnedRows": len(parsed_rows),
            "rows": parsed_rows,
        })
    except Exception as e:
        return _mcp_failure("get_filing_table", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="extract_filing_fact",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_filing_fact"],
    description="""Extract a specific financial fact from an SEC filing. Uses XBRL first, parsed tables second, text search last.
Args:
    ticker: str
    fact_name: str - Fact to extract (e.g. 'revenue', 'net income', 'R&D expense')
    document_url: str - Optional direct URL to filing HTML (must be https://www.sec.gov/Archives/...)
    accession_number: str - Optional accession number for XBRL lookup
""",
)
async def extract_filing_fact(
    ticker: str,
    fact_name: str,
    document_url: str | None = None,
    accession_number: str | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("extract_filing_fact", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if document_url:
        url_err = _validate_sec_url(document_url)
        if url_err:
            return _mcp_failure("extract_filing_fact", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        result_str = await search_filing_text(
            ticker=ticker,
            search_terms=[fact_name],
            filing_type="10-K",
            accession_number=accession_number,
            context_chars=1000,
            return_tables=True,
        )
        result = json.loads(result_str)
        return json.dumps({
            "ticker": ticker,
            "factName": fact_name,
            "extractionMethod": "text_search",
            "result": result,
        })
    except Exception as e:
        return _mcp_failure("extract_filing_fact", ErrorCode.PROVIDER_ERROR, str(e))


@yfinance_server.tool(
    name="get_recommendations",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_recommendations"],
    description="""Get recommendations or upgrades/downgrades for a given ticker symbol from yahoo finance. You can also specify the number of months back to get upgrades/downgrades for, default is 12.

Args:
    ticker: str
        The ticker symbol of the stock to get recommendations for, e.g. "AAPL"
    recommendation_type: str
        The type of recommendation to get. You can choose from the following recommendation types: recommendations, upgrades_downgrades.
    months_back: int
        The number of months back to get upgrades/downgrades for, default is 12.
""",
)
async def get_recommendations(ticker: str, recommendation_type: str, months_back: int = 12) -> str:
    """Get recommendations or upgrades/downgrades for a given ticker symbol"""
    company = yf.Ticker(ticker)
    try:
        if company.fast_info.currency is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting recommendations for {ticker}: {e}")
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
        print(f"Error: getting recommendations for {ticker}: {e}")
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


@yfinance_server.tool(
    name="get_analyst_consensus",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_analyst_consensus"],
    description="""Get a compact analyst consensus summary for one or more tickers.

Returns pre-aggregated data so you do NOT need to call get_recommendations separately.
Includes:
- Consensus price target (current, low, high, mean, median) and % upside from current price
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
        results = await asyncio.gather(*[get_analyst_consensus(t) for t in ticker], return_exceptions=True)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    cache_key = f"analyst_consensus:{ticker}"
    cached = _cache_get(cache_key, _STMT_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        last_price = fi.last_price
    except Exception as e:
        print(f"Error: getting analyst consensus for {ticker}: {e}")
        return f"Error: getting analyst consensus for {ticker}: {e}"

    output: dict = {"ticker": ticker}
    warnings: list[dict[str, str]] = []

    # Price targets
    target_mean = None
    try:
        targets = company.analyst_price_targets
        if targets:
            current_target = targets.get("current")
            target_mean = targets.get("mean")
            output["priceTargets"] = {
                "current": current_target,
                "low": targets.get("low"),
                "high": targets.get("high"),
                "mean": target_mean,
                "median": targets.get("median"),
                "pctUpsideFromLastPrice": (
                    round((current_target - last_price) / last_price * 100, 2)
                    if current_target and last_price
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
            output["totalAnalysts"] = sum(counts.values()) if counts else None
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
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.3 — get_earnings_analysis
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_earnings_analysis",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_earnings_analysis"],
    description="""Get all analyst forward-looking data in a single call — replaces 5 separate tool calls.

Returns:
- earningsEstimate: EPS estimates for current quarter, next quarter, current year, next year
- revenueEstimate: Revenue estimates for the same periods
- epsTrend: How EPS estimates have moved over the last 7/30/60/90 days
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
    cached = _cache_get(cache_key, _STMT_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        if fi.currency is None:
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting earnings analysis for {ticker}: {e}")
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

    output: dict = {"ticker": ticker}
    for key, attr in [
        ("earningsEstimate", "earnings_estimate"),
        ("revenueEstimate", "revenue_estimate"),
        ("epsTrend", "eps_trend"),
        ("earningsHistory", "earnings_history"),
        ("growthEstimates", "growth_estimates"),
    ]:
        try:
            output[key] = _df_to_records(getattr(company, attr))
        except Exception:
            output[key] = None

    result = json.dumps(output)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.4 — get_financial_ratios
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_financial_ratios",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_financial_ratios"],
    description="""Get pre-computed key financial ratios for one or more tickers.

PREFER THIS over fetching full financial statements when you need valuation or profitability ratios.
Ratios are computed server-side from company.info so the LLM does not have to process raw statements.

Includes:
- Valuation: P/E (trailing & forward), P/S, P/B, EV/EBITDA, EV/Revenue, PEG ratio
- Profitability: Gross/Operating/Net margins, ROE, ROA
- Leverage: Debt/Equity, Current ratio, Quick ratio
- Cash flow: Free Cash Flow, FCF yield (FCF / market cap)
- Dividend: Yield, Payout ratio

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of symbols (e.g. ["AAPL", "MSFT"]).
        When a list is provided, returns a dict keyed by symbol.
""",
)
async def get_financial_ratios(ticker: str | list[str]) -> str:
    """Get pre-computed key financial ratios."""
    if isinstance(ticker, list):
        results = await asyncio.gather(*[get_financial_ratios(t) for t in ticker], return_exceptions=True)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    cache_key = f"financial_ratios:{ticker}"
    cached = _cache_get(cache_key, _STMT_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        info = company.info
    except Exception as e:
        print(f"Error: getting financial ratios for {ticker}: {e}")
        return f"Error: getting financial ratios for {ticker}: {e}"

    def _get(key):
        return info.get(key)

    market_cap = _get("marketCap")
    free_cashflow = _get("freeCashflow")

    ratios: dict = {
        "ticker": ticker,
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

    result = json.dumps(ratios)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 2.5 — get_calendar
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_calendar",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_calendar"],
    description="""Get upcoming earnings and dividend schedule for a ticker.

Returns:
- Next earnings date range and EPS/revenue estimates
- Ex-dividend date and dividend pay date
- earningsDateConfirmed: true when Yahoo Finance shows a single fixed date (likely confirmed
  by company filing/IR source); false when a date range is returned (estimate).
- earningsDateSource: "IR_FILING" | "ESTIMATE" | "UNKNOWN"

Args:
    ticker: str
        The ticker symbol, e.g. "AAPL"
""",
)
async def get_calendar(ticker: str) -> str:
    """Get upcoming earnings and dividend calendar for a ticker."""
    cache_key = f"calendar:{ticker}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        if fi.currency is None:
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting calendar for {ticker}: {e}")
        return f"Error: getting calendar for {ticker}: {e}"

    try:
        cal = company.calendar
    except Exception as e:
        print(f"Error: getting calendar for {ticker}: {e}")
        return f"Error: getting calendar for {ticker}: {e}"

    if not cal:
        return json.dumps({"ticker": ticker, "calendar": None})

    # calendar values may be datetime.date objects — convert to strings
    def _serialize(v):
        if hasattr(v, "isoformat"):
            return v.isoformat()
        if isinstance(v, list):
            return [_serialize(i) for i in v]
        return v

    # Determine whether the earnings date is IR-confirmed or an analyst estimate.
    # Heuristic: if Yahoo Finance provides a single fixed date, it's likely sourced from
    # an IR press release / 8-K filing. A date range (start ≠ end) signals an analyst
    # estimate. This heuristic is imperfect but is the best available without SEC parsing.
    ed_raw = cal.get("Earnings Date")
    if isinstance(ed_raw, list):
        ed_dates = ed_raw
    elif ed_raw is not None:
        ed_dates = [ed_raw]
    else:
        ed_dates = []

    unique_dates = {getattr(d, "date", lambda: d)() if hasattr(d, "date") else d for d in ed_dates}
    if len(unique_dates) == 0:
        earnings_date_confirmed = False
        earnings_date_source = "UNKNOWN"
    elif len(unique_dates) == 1:
        earnings_date_confirmed = True
        earnings_date_source = "IR_FILING"
    else:
        earnings_date_confirmed = False
        earnings_date_source = "ESTIMATE"

    output = {
        "ticker": ticker,
        "earningsDateConfirmed": earnings_date_confirmed,
        "earningsDateSource": earnings_date_source,
        "calendar": {k: _serialize(v) for k, v in cal.items()},
    }
    result = json.dumps(output)
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Group 3.1 — search_ticker
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="search_ticker",
    output_schema=_TOOL_OUTPUT_SCHEMAS["search_ticker"],
    description="""Search for ticker symbols by company name, partial name, or ISIN.

Use this tool to resolve a company name to a ticker symbol before calling other tools.
Returns matching quotes with symbol, shortname, exchange, and type.

Args:
    query: str
        Company name, partial name, or ISIN to search for, e.g. "Apple", "AAPL", "US0378331005"
    max_results: int
        Maximum number of quote results to return. Default is 8.
    exchange: str | None
        Optional exchange filter. Pass "US" to restrict results to NMS (NASDAQ) and NYQ (NYSE) only
        — use this for small/mid-cap US equity searches that may otherwise return foreign listings or
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
                _us = {"NMS", "NYQ", "PCX"}  # NASDAQ, NYSE, NYSE Arca
                trimmed = [r for r in trimmed if r.get("exchange") in _us]
            else:
                trimmed = [r for r in trimmed if r.get("exchange") == exch_upper]
        return json.dumps(trimmed)
    except Exception as e:
        print(f"Error: searching for '{query}': {e}")
        return f"Error: searching for '{query}': {e}"


# ---------------------------------------------------------------------------
# Group 3.2 — screen_stocks
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="screen_stocks",
    output_schema=_TOOL_OUTPUT_SCHEMAS["screen_stocks"],
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
        print(f"Error: running screener '{screener_name}': {e}")
        return f"Error: running screener '{screener_name}': {e}"



# ---------------------------------------------------------------------------
# Group 3.4b — get_filing_data / search_filing_text
# ---------------------------------------------------------------------------

_FILING_FACT_CONCEPTS: dict[FilingFactType, tuple[str, str | None]] = {
    FilingFactType.geographic_revenue: ("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"),
    FilingFactType.segment_revenue: ("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"),
    FilingFactType.capex: ("PaymentsToAcquirePropertyPlantAndEquipment", None),
    FilingFactType.rd_expense: ("ResearchAndDevelopmentExpense", None),
    FilingFactType.operating_income: ("OperatingIncomeLoss", None),
    FilingFactType.net_income: ("NetIncomeLoss", None),
    FilingFactType.total_revenue: ("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"),
    FilingFactType.long_term_debt: ("LongTermDebt", None),
    FilingFactType.cash: ("CashAndCashEquivalentsAtCarryingValue", None),
}


def _manual_lookup_payload(ticker: str, cik_padded: str | None, filing_type: str, note: str) -> dict:
    edgar_index_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_padded}&type={filing_type}&owner=include&count=10"
        if cik_padded
        else f"https://www.sec.gov/cgi-bin/browse-edgar?company={ticker}&action=getcompany&type={filing_type}&owner=include&count=10"
    )
    efts_url = (
        "https://efts.sec.gov/LATEST/search-index"
        f"?q={_urlparse.quote(ticker)}&forms={_urlparse.quote(filing_type)}"
    )
    return {
        "edgarIndexUrl": edgar_index_url,
        "eftsSearchUrl": efts_url,
        "note": note,
    }


@yfinance_server.tool(
    name="get_filing_data",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_data"],
    description="""Retrieve structured XBRL-tagged financial facts from EDGAR.

Try this tool before search_filing_text for GAAP line items or geographic revenue.
Use period_mode to select quarter, ytd, or annual facts (default: auto selects quarter for 10-Q, annual for 10-K).
""",
)
async def get_filing_data(
    ticker: str,
    fact_type: FilingFactType,
    region: str | None = None,
    filing_type: str = "10-K",
    period: str = "latest",
    period_mode: str = "auto",
) -> str:
    FLOATING_POINT_EPSILON = 1e-9
    RATIO_DECIMALS = 4
    PCT_DECIMALS = 2
    PCT_MULTIPLIER = 100

    def _format_raw_number(n: float | int | None) -> str | None:
        if n is None:
            return None
        try:
            f = float(n)
            if abs(f - round(f)) < FLOATING_POINT_EPSILON:
                return f"{int(round(f)):,}"
            return f"{f:,.2f}"
        except Exception:
            return None

    def _scale_label(multiplier: float | None) -> str:
        if multiplier == 1_000.0:
            return "thousands"
        if multiplier == 1_000_000.0:
            return "millions"
        if multiplier == 1.0:
            return "actual"
        return "actual"

    def _geo_shape(payload: dict, *, warn_denominator: bool = False) -> str:
        if fact_type != FilingFactType.geographic_revenue:
            return json.dumps(payload)
        shaped = {
            "ticker": payload.get("ticker", ticker),
            "factType": payload.get("factType", FilingFactType.geographic_revenue.value),
            "region": payload.get("region", region),
            "period": payload.get("period"),
            "rawValue": payload.get("rawValue"),
            "rawDenominator": payload.get("rawDenominator"),
            "unit": payload.get("unit", "USD"),
            "unitScale": payload.get("unitScale", "actual"),
            "value": payload.get("value"),
            "denominator": payload.get("denominator"),
            "valueRatio": payload.get("valueRatio"),
            "valuePct": payload.get("valuePct"),
            "extractionMethod": payload.get("extractionMethod", "NONE"),
            "source": payload.get("source", "NOT_DISCLOSED"),
            "confidence": payload.get("confidence", "NOT_DISCLOSED"),
            "filingType": payload.get("filingType", filing_type),
            "filingDate": payload.get("filingDate"),
            "accessionNumber": payload.get("accessionNumber"),
            "documentUrl": payload.get("documentUrl"),
            "indexUrl": payload.get("indexUrl"),
            "primaryDocumentUrl": payload.get("primaryDocumentUrl"),
            "evidence": payload.get("evidence", {}),
            "calculation": payload.get("calculation"),
            "warnings": list(payload.get("warnings", [])) if isinstance(payload.get("warnings"), list) else [],
        }
        has_denominator = shaped["denominator"] is not None
        if not has_denominator:
            shaped["valueRatio"] = None
            shaped["valuePct"] = None
        if warn_denominator and shaped.get("value") is not None and not has_denominator:
            shaped["warnings"].append({
                "code": "DENOMINATOR_NOT_FOUND",
                "message": "Could not compute geographic revenue percentage due to missing denominator.",
                "severity": "warning",
            })
        return json.dumps(shaped)

    async def _resolve_filing_urls_for_accession(accn: str) -> tuple[str | None, str | None]:
        if not accn:
            return None, None
        if not cik_padded:
            return None, None
        index_url, primary_url = _edgar_build_filing_urls(int(cik_padded), accn, None)
        _, subs = await _get_submissions_for_ticker(ticker)
        if not subs:
            return index_url, primary_url
        recent = subs.get("filings", {}).get("recent", {})
        accessions: list[str] = recent.get("accessionNumber", [])
        primary_docs: list[str] = recent.get("primaryDocument", [])
        try:
            idx = accessions.index(accn)
            primary_doc = primary_docs[idx] if idx < len(primary_docs) else None
            if primary_doc:
                _, primary_url = _edgar_build_filing_urls(int(cik_padded), accn, primary_doc)
        except Exception:
            pass
        return index_url, primary_url

    if fact_type == FilingFactType.geographic_revenue and not region:
        return json.dumps({"error": True, "message": "region is required for fact_type='geographic_revenue'"})

    concept_primary, concept_fallback = _FILING_FACT_CONCEPTS[fact_type]
    cik_padded = await _resolve_cik_for_ticker(ticker)
    if not cik_padded:
        return _geo_shape({
            "ticker": ticker,
            "factType": fact_type.value,
            "value": None,
            "denominator": None,
            "valueRatio": None,
            "valuePct": None,
            "extractionMethod": "NONE",
            "source": "NOT_DISCLOSED",
            "confidence": "NOT_DISCLOSED",
            "evidence": {},
            "warnings": [],
            "_manualLookup": _manual_lookup_payload(
                ticker, None, filing_type, "Fact not XBRL-tagged. Use search_filing_text instead."
            ),
        })

    async def _concept_json(concept_name: str) -> dict | None:
        try:
            return await _edgar_get(
                f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik_padded}/us-gaap/{concept_name}.json"
            )
        except EdgarError:
            return None

    concept_used = concept_primary
    concept_data = await _concept_json(concept_primary)
    usd_facts: list[dict] = (
        concept_data.get("units", {}).get("USD", [])  # type: ignore[union-attr]
        if concept_data else []
    )
    if not usd_facts and concept_fallback:
        fallback_data = await _concept_json(concept_fallback)
        fallback_usd = fallback_data.get("units", {}).get("USD", []) if fallback_data else []
        if fallback_usd:
            concept_used = concept_fallback
            concept_data = fallback_data
            usd_facts = fallback_usd

    filtered = [f for f in usd_facts if str(f.get("form", "")).upper() == filing_type.upper()]
    if not filtered:
        if fact_type != FilingFactType.geographic_revenue:
            return _geo_shape({
                "ticker": ticker,
                "factType": fact_type.value,
                "value": None,
                "denominator": None,
                "valueRatio": None,
                "valuePct": None,
                "extractionMethod": "NONE",
                "source": "NOT_DISCLOSED",
                "confidence": "NOT_DISCLOSED",
                "evidence": {},
                "warnings": [],
                "_manualLookup": _manual_lookup_payload(
                    ticker, cik_padded, filing_type, "Fact not XBRL-tagged. Use search_filing_text instead."
                ),
            })
        # For geographic_revenue, fall through to HTML fallback below (picked remains None)

    if filtered and period == "latest":
        latest_filed = max(str(f.get("filed", "")) for f in filtered)
        filtered = [f for f in filtered if str(f.get("filed", "")) == latest_filed]

    # ── period_mode filtering ─────────────────────────────────────────────
    # XBRL facts include both quarterly and YTD figures for the same filing.
    # Filter by duration to avoid returning 6-month revenue as quarterly.
    resolved_mode = period_mode.lower().strip() if period_mode else "auto"
    if resolved_mode == "auto":
        resolved_mode = "quarter" if filing_type.upper() == "10-Q" else "annual"
    if resolved_mode in ("quarter", "annual", "ytd") and filtered:
        def _duration_days(f: dict) -> int | None:
            """Compute duration in days from XBRL start/end dates."""
            s, e = f.get("start"), f.get("end")
            if not s or not e:
                return None
            try:
                d0 = datetime.date.fromisoformat(str(s))
                d1 = datetime.date.fromisoformat(str(e))
                return (d1 - d0).days
            except (ValueError, TypeError):
                return None
        dur_tagged = [(f, _duration_days(f)) for f in filtered]
        if resolved_mode == "quarter":
            # Keep facts with duration 80-100 days (one quarter ~90 days)
            # Also keep instant facts (no start/end) and untagged duration
            mode_filtered = [f for f, d in dur_tagged if d is None or (60 <= d <= 110)]
        elif resolved_mode == "ytd":
            # Keep facts with year-to-date duration based on fiscal period (fp) or form type
            def _is_ytd_match(f: dict, d: int | None) -> bool:
                if d is None:
                    return True
                fp = str(f.get("fp") or "").upper().strip()
                if fp == "Q1":
                    return 60 <= d <= 110
                elif fp == "Q2":
                    return 150 <= d <= 200
                elif fp == "Q3":
                    return 240 <= d <= 290
                else:  # FY, Q4, or fallback
                    form = str(f.get("form") or "").upper().strip()
                    if form == "10-K":
                        return 340 <= d <= 400
                    return d >= 60
            mode_filtered = [f for f, d in dur_tagged if _is_ytd_match(f, d)]
        else:  # annual
            # Keep facts with duration 350-380 days (one year ~365 days)
            mode_filtered = [f for f, d in dur_tagged if d is None or (340 <= d <= 400)]
        if mode_filtered:
            filtered = mode_filtered

    if fact_type == FilingFactType.segment_revenue:
        seg_rows = []
        for f in filtered:
            seg_label = _normalize_segment_label(f.get("segment"))
            if not seg_label:
                continue
            seg_rows.append({
                "segmentLabel": seg_label,
                "value": f.get("val"),
                "fiscalYear": str(f.get("fy") or ""),
                "fiscalPeriod": str(f.get("fp") or ""),
                "filingDate": str(f.get("filed") or ""),
                "accessionNumber": str(f.get("accn") or ""),
            })
        return json.dumps({
            "ticker": ticker,
            "factType": fact_type.value,
            "concept": concept_used,
            "value": seg_rows[0]["value"] if seg_rows else None,
            "fiscalYear": seg_rows[0]["fiscalYear"] if seg_rows else "",
            "fiscalPeriod": seg_rows[0]["fiscalPeriod"] if seg_rows else "",
            "filingType": filing_type,
            "filingDate": seg_rows[0]["filingDate"] if seg_rows else "",
            "accessionNumber": seg_rows[0]["accessionNumber"] if seg_rows else "",
            "extractionMethod": "XBRL",
            "source": "XBRL",
            "confidence": "HIGH",
            "allSegments": seg_rows,
        })

    picked: dict | None = None
    value_ratio: float | None = None
    value_pct: float | None = None
    denominator: float | None = None
    segment_label: str | None = None
    if fact_type == FilingFactType.geographic_revenue:
        for f in filtered:
            seg_label = _normalize_segment_label(f.get("segment"))
            if seg_label and _region_matches(seg_label, region or "", include_asia_fallback=False):
                picked = f
                segment_label = seg_label
                break
        if picked is None and (region or "").lower() == "china":
            for f in filtered:
                seg_label = _normalize_segment_label(f.get("segment"))
                if seg_label and _region_matches(seg_label, region or "", include_asia_fallback=True):
                    picked = f
                    segment_label = seg_label
                    break
        if picked is not None:
            accn = str(picked.get("accn") or "")
            total_fact = next(
                (
                    f for f in filtered
                    if str(f.get("accn") or "") == accn and not f.get("segment")
                ),
                None,
            )
            try:
                picked_val = picked.get("val")
                if total_fact and picked_val is not None and float(total_fact.get("val", 0)) > 0:
                    denominator = float(total_fact.get("val", 0))
                    value_ratio = round(float(picked_val) / denominator, RATIO_DECIMALS)
                    value_pct = round(value_ratio * PCT_MULTIPLIER, PCT_DECIMALS)
            except Exception:
                denominator = None
                value_ratio = None
                value_pct = None
    else:
        picked = next((f for f in filtered if not f.get("segment")), filtered[0] if filtered else None)

    if picked is None:
        # ── HTML fallback for geographic_revenue ──────────────────────────────
        # Some companies (e.g. GLW) do not XBRL-tag geographic-revenue segments.
        # Fall through to the same HTML-parsing path used by search_filing_text.
        if fact_type == FilingFactType.geographic_revenue:
            _, subs = await _get_submissions_for_ticker(ticker)
            if subs:
                recent = subs.get("filings", {}).get("recent", {})
                forms: list[str] = recent.get("form", [])
                accessions_list: list[str] = recent.get("accessionNumber", [])
                primary_docs_list: list[str] = recent.get("primaryDocument", [])
                filing_dates_list: list[str] = recent.get("filingDate", [])
                report_dates_list: list[str] = recent.get("reportDate", [])
                idx: int | None = None
                for i, form in enumerate(forms):
                    if str(form).upper() == filing_type.upper():
                        idx = i
                        break
                if idx is not None:
                    primary_doc = primary_docs_list[idx] if idx < len(primary_docs_list) else None
                    if primary_doc:
                        cik_int = int(cik_padded)
                        _, doc_url = _edgar_build_filing_urls(cik_int, accessions_list[idx], primary_doc)
                        if doc_url:
                            html_text = await _edgar_get_html(doc_url, max_bytes=5_000_000)
                            if html_text:
                                geo_ratio, geo_usd, geo_denominator, geo_heading, geo_evidence = _extract_geo_revenue_from_html(
                                    html_text, region or ""
                                )
                                if geo_usd is not None:
                                    acc_num = accessions_list[idx] if idx < len(accessions_list) else ""
                                    filing_date_str = filing_dates_list[idx] if idx < len(filing_dates_list) else ""
                                    report_date_str = report_dates_list[idx] if idx < len(report_dates_list) else ""
                                    fiscal_year = f"FY{report_date_str[:4]}" if report_date_str else ""
                                    raw_value = (
                                        geo_evidence.get("rawValue") if isinstance(geo_evidence, dict) else None
                                    ) or _format_raw_number(geo_usd)
                                    raw_den = (
                                        geo_evidence.get("rawDenominator") if isinstance(geo_evidence, dict) else None
                                    ) or _format_raw_number(geo_denominator)
                                    source_rows = (
                                        geo_evidence.get("sourceRows") if isinstance(geo_evidence, dict) else None
                                    ) or [
                                        [region or "Region", raw_value],
                                        ["Total revenue", raw_den],
                                    ]
                                    source_cols = (
                                        geo_evidence.get("sourceColumns") if isinstance(geo_evidence, dict) else None
                                    ) or [fiscal_year]
                                    warnings = []
                                    if geo_denominator is None and geo_usd is not None:
                                        warnings.append({
                                            "code": "DENOMINATOR_NOT_FOUND",
                                            "message": "Could not compute geographic revenue percentage due to missing denominator.",
                                            "severity": "warning",
                                        })
                                    return _geo_shape({
                                        "ticker": ticker,
                                        "factType": fact_type.value,
                                        "region": region,
                                        "period": fiscal_year or None,
                                        "rawValue": raw_value,
                                        "rawDenominator": raw_den,
                                        "unit": "USD",
                                        "unitScale": (geo_evidence.get("unitScale") if isinstance(geo_evidence, dict) else "actual") or "actual",
                                        "value": geo_usd,
                                        "denominator": geo_denominator,
                                        "valueRatio": geo_ratio,
                                        "valuePct": round(geo_ratio * PCT_MULTIPLIER, PCT_DECIMALS) if geo_ratio is not None else None,
                                        "extractionMethod": "PARSED_TABLE",
                                        "source": "PARSED_TABLE",
                                        "confidence": "HIGH" if geo_denominator is not None else "LOW",
                                        "filingType": filing_type,
                                        "filingDate": filing_date_str,
                                        "accessionNumber": acc_num,
                                        "documentUrl": doc_url,
                                        "indexUrl": None,
                                        "primaryDocumentUrl": doc_url,
                                        "evidence": {
                                            "sectionHeading": geo_heading or (geo_evidence.get("sectionHeading") if isinstance(geo_evidence, dict) else None),
                                            "tableTitle": geo_evidence.get("tableTitle") if isinstance(geo_evidence, dict) else None,
                                            "sourceTableId": geo_evidence.get("sourceTableId") if isinstance(geo_evidence, dict) else 1,
                                            "sourceRows": source_rows,
                                            "sourceColumns": source_cols,
                                        },
                                        "calculation": (
                                            {
                                                "formula": "value / denominator * 100",
                                                "valueSource": "sourceRows[0]",
                                                "denominatorSource": "sourceRows[1]",
                                                "resultPct": round(geo_ratio * PCT_MULTIPLIER, PCT_DECIMALS),
                                            }
                                            if geo_ratio is not None and geo_denominator is not None else None
                                        ),
                                        "warnings": warnings,
                                    })
        return _geo_shape({
            "ticker": ticker,
            "factType": fact_type.value,
            "value": None,
            "denominator": None,
            "valueRatio": None,
            "valuePct": None,
            "extractionMethod": "NONE",
            "source": "NOT_DISCLOSED",
            "confidence": "NOT_DISCLOSED",
            "evidence": {},
            "warnings": [],
            "_manualLookup": _manual_lookup_payload(
                ticker, cik_padded, filing_type, "Fact not XBRL-tagged. Use search_filing_text instead."
            ),
        })

    accession_number = str(picked.get("accn") or "")
    index_url, primary_document_url = await _resolve_filing_urls_for_accession(accession_number)
    document_url = primary_document_url or index_url
    value_num = float(picked.get("val", 0)) if picked.get("val") is not None else None
    raw_value = _format_raw_number(value_num)
    raw_denominator = _format_raw_number(denominator)
    period_label = str(picked.get("fy") or "")
    if period_label and not period_label.startswith("FY"):
        period_label = f"FY{period_label}"

    # ── XBRL context metadata ─────────────────────────────────────────────
    xbrl_context: dict = {
        "periodStart": picked.get("start"),
        "periodEnd": picked.get("end"),
        "durationDays": None,
        "fiscalPeriod": str(picked.get("fp") or ""),
        "fiscalYear": str(picked.get("fy") or ""),
        "form": str(picked.get("form") or ""),
        "frame": picked.get("frame"),
        "periodMode": resolved_mode,
    }
    if picked.get("start") and picked.get("end"):
        try:
            d0 = datetime.date.fromisoformat(str(picked["start"]))
            d1 = datetime.date.fromisoformat(str(picked["end"]))
            xbrl_context["durationDays"] = (d1 - d0).days
        except (ValueError, TypeError):
            pass

    result_warnings: list[dict] = []
    # Warn if period_mode filtering was requested but couldn't narrow results
    if resolved_mode == "quarter" and xbrl_context.get("durationDays") and xbrl_context["durationDays"] > 110:
        result_warnings.append({
            "code": "PERIOD_MODE_MISMATCH",
            "message": f"Requested quarter but picked fact has {xbrl_context['durationDays']}-day duration. No quarterly fact available.",
            "severity": "warning",
        })

    return _geo_shape({
        "ticker": ticker,
        "factType": fact_type.value,
        "region": region,
        "period": period_label or None,
        "rawValue": raw_value,
        "rawDenominator": raw_denominator,
        "unit": "USD",
        "unitScale": "actual",
        "value": value_num,
        "denominator": denominator if fact_type == FilingFactType.geographic_revenue else None,
        "valueRatio": value_ratio if fact_type == FilingFactType.geographic_revenue else None,
        "valuePct": value_pct if fact_type == FilingFactType.geographic_revenue else None,
        "extractionMethod": "XBRL",
        "source": "XBRL",
        "confidence": "HIGH" if fact_type != FilingFactType.geographic_revenue or denominator is not None else "LOW",
        "filingType": filing_type,
        "filingDate": str(picked.get("filed") or ""),
        "accessionNumber": accession_number or None,
        "documentUrl": document_url,
        "indexUrl": index_url,
        "primaryDocumentUrl": primary_document_url,
        "xbrlContext": xbrl_context,
        "evidence": {
            "sectionHeading": segment_label,
            "tableTitle": None,
            "sourceTableId": None,
            "sourceRows": [
                [segment_label or (region or "Region"), raw_value],
                ["Total revenue", raw_denominator],
            ],
            "sourceColumns": [period_label or str(picked.get("fp") or "")],
        },
        "calculation": (
            {
                "formula": "value / denominator * 100",
                "valueSource": "sourceRows[0]",
                "denominatorSource": "sourceRows[1]",
                "resultPct": value_pct,
            }
            if fact_type == FilingFactType.geographic_revenue and denominator is not None else None
        ),
        "warnings": result_warnings,
    }, warn_denominator=(fact_type == FilingFactType.geographic_revenue and denominator is None))


@yfinance_server.tool(
    name="search_filing_text",
    output_schema=_TOOL_OUTPUT_SCHEMAS["search_filing_text"],
    description="""Search filing narrative text by keyword or section hint.

Use this only when get_filing_data returns NOT_DISCLOSED or the fact is not XBRL-tagged.
""",
)
async def search_filing_text(
    ticker: str,
    search_terms: list[str] | None = None,
    section_hint: str | None = None,
    filing_type: str = "10-K",
    accession_number: str | None = None,
    context_chars: int = 1500,
    return_tables: bool = True,
) -> str:
    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": accession_number,
            "documentUrl": None,
            "fiscalYear": None,
            "filingType": filing_type,
            "filingDate": None,
            "matches": [],
            "matchCount": 0,
            "confidence": "PARSED_HTML",
            "_note": "Could not resolve SEC submissions for ticker.",
        })

    recent = subs.get("filings", {}).get("recent", {})
    forms: list[str] = recent.get("form", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    filing_dates: list[str] = recent.get("filingDate", [])
    report_dates: list[str] = recent.get("reportDate", [])

    target_idx: int | None = None
    if accession_number:
        for i, acc in enumerate(accessions):
            if acc == accession_number:
                target_idx = i
                break
    else:
        for i, form in enumerate(forms):
            if str(form).upper() == filing_type.upper():
                target_idx = i
                accession_number = accessions[i] if i < len(accessions) else None
                break

    if target_idx is None or not accession_number:
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": accession_number,
            "documentUrl": None,
            "fiscalYear": None,
            "filingType": filing_type,
            "filingDate": None,
            "matches": [],
            "matchCount": 0,
            "confidence": "PARSED_HTML",
            "_note": f"No {filing_type} filing found in submissions JSON.",
        })

    primary_doc = primary_docs[target_idx] if target_idx < len(primary_docs) else None
    if not primary_doc:
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": accession_number,
            "documentUrl": None,
            "fiscalYear": None,
            "filingType": filing_type,
            "filingDate": filing_dates[target_idx] if target_idx < len(filing_dates) else None,
            "matches": [],
            "matchCount": 0,
            "confidence": "PARSED_HTML",
            "_note": "primaryDocument missing in submissions JSON.",
        })

    cik_int = int(cik_padded)
    _, document_url = _edgar_build_filing_urls(cik_int, accession_number, primary_doc)
    if not document_url:
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": accession_number,
            "documentUrl": None,
            "fiscalYear": None,
            "filingType": filing_type,
            "filingDate": filing_dates[target_idx] if target_idx < len(filing_dates) else None,
            "matches": [],
            "matchCount": 0,
            "confidence": "PARSED_HTML",
            "_note": "Failed constructing filing document URL.",
        })

    html_text = await _edgar_get_html(document_url, max_bytes=5_000_000)
    if not html_text:
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": accession_number,
            "documentUrl": document_url,
            "fiscalYear": f"FY{str(report_dates[target_idx])[:4]}" if target_idx < len(report_dates) and report_dates[target_idx] else None,
            "filingType": filing_type,
            "filingDate": filing_dates[target_idx] if target_idx < len(filing_dates) else None,
            "matches": [],
            "matchCount": 0,
            "confidence": "PARSED_HTML",
            "_note": "Unable to fetch filing HTML.",
        })

    html_low = html_text.lower()
    context_window = max(200, min(int(context_chars), 4000))
    matches: list[dict] = []
    seen: set[int] = set()

    def _append_match(term: str, pos: int) -> None:
        if any(abs(pos - p) < 150 for p in seen):
            return
        seen.add(pos)
        start = max(0, pos - context_window // 2)
        end = min(len(html_text), pos + context_window // 2)
        context_html = html_text[start:end]
        pre_html = html_text[max(0, pos - 8_000):pos]
        h_matches = _re.findall(r"<h[1-6][^>]*>(.*?)</h[1-6]>", pre_html, _re.IGNORECASE | _re.DOTALL)
        section_heading = _strip_html_tags(h_matches[-1]) if h_matches else ""
        item = {
            "term": term,
            "sectionHeading": section_heading,
            "contextText": _strip_html_tags(context_html),
        }
        if return_tables:
            parsed_tables: list[dict] = []
            for tbl_m in _re.finditer(r"<table[^>]*>([\s\S]*?)</table>", context_html, _re.IGNORECASE):
                rows = _parse_html_table(tbl_m.group(0))
                if len(rows) >= 2:
                    parsed_tables.append({"rows": rows})
                if len(parsed_tables) >= 3:
                    break
            item["tableParsed"] = parsed_tables
        matches.append(item)

    if section_hint:
        pos = html_low.find(section_hint.lower())
        if pos >= 0:
            _append_match(section_hint, pos)
    for term in (search_terms or []):
        pos = 0
        term_low = term.lower()
        while len(matches) < 10:
            found = html_low.find(term_low, pos)
            if found < 0:
                break
            _append_match(term, found)
            pos = found + 1

    return json.dumps({
        "ticker": ticker,
        "accessionNumber": accession_number,
        "documentUrl": document_url,
        "fiscalYear": f"FY{str(report_dates[target_idx])[:4]}" if target_idx < len(report_dates) and report_dates[target_idx] else None,
        "filingType": filing_type,
        "filingDate": filing_dates[target_idx] if target_idx < len(filing_dates) else None,
        "matches": matches,
        "matchCount": len(matches),
        "confidence": "PARSED_HTML",
    })


# ---------------------------------------------------------------------------
# Group 3.5 — get_technical_indicators
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_credit_health",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_credit_health"],
    description="""Get pre-computed credit/leverage metrics using operational EBITDA when available: Net Debt/EBITDA, interest coverage, debt tier, credit stress flag, and source fields.

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of up to 5 symbols.
        When a list is provided, returns a dict keyed by symbol.
        Max 5 tickers per call; split larger lists into multiple calls.
""",
)
async def get_credit_health(ticker: str | list[str]) -> str:
    """Return credit health metrics for one or more tickers."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_credit_health(t))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
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

    def _safe_get_with_source(df, col, *row_names):
        for name in row_names:
            try:
                val = df.loc[name, col]
                if pd.notna(val):
                    return float(val), str(name)
            except (KeyError, TypeError):
                continue
        return None, None

    def _ttm_sum(df, cols, *row_names):
        """Sum first matching row across quarterly cols for TTM. Returns (total, source_row, n_quarters_used)."""
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
    total_debt = _safe_get(bs, bs_col, "Total Debt", "TotalDebt", "Long Term Debt")
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

    net_debt_to_ebitda = round(net_debt / operational_ebitda_annual, 2) if net_debt is not None and operational_ebitda_annual else None
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
@yfinance_server.tool(
    name="get_earnings_momentum",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_earnings_momentum"],
    description="""Deprecated alias for analyze_earnings_momentum. Get earnings revision momentum, beat rate, and estimate direction signals.

Returns: revision7d/30d/90d, revisionDirection, momentumFlag, beatRate, beatCount, avgSurprisePct, currentBeatStreak.

Args:
    ticker: str | list[str]
        A single ticker symbol (e.g. "AAPL") or a list of up to 5 symbols.
        When a list is provided, returns a dict keyed by symbol.
        Max 5 tickers per call; split larger lists into multiple calls.
""",
)
async def get_earnings_momentum(ticker: str | list[str]) -> str:
    """Return earnings momentum for one or more tickers."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_earnings_momentum(t))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
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
                    surprises.append(float(surprise_pct) * 100 if abs(float(surprise_pct)) < 1 else float(surprise_pct))

        # Beat streak (consecutive from most recent)
        for row in earnings_history_records:
            actual = row.get("epsActual")
            estimate = row.get("epsEstimate")
            if actual is not None and estimate is not None:
                if actual > estimate:
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

@yfinance_server.tool(
    name="get_options_flow_summary",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_options_flow_summary"],
    description="""Get options flow summary: P/C ratio, IV percentile, max pain strike, highest OI strikes. Single ticker only.

Args:
    ticker: str — single ticker
    expiry_hint: str | None — optional YYYY-MM-DD; if omitted, selects nearest liquid expiry
""",
)
async def get_options_flow_summary(ticker: str, expiry_hint: str | None = None) -> str:
    # Consolidated naming: route to the same payload implementation as get_options_summary.
    return await get_options_summary(ticker, expiry_hint=expiry_hint)


# ---------------------------------------------------------------------------
# Tool: get_put_hedge_candidates
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_put_hedge_candidates",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_put_hedge_candidates"],
    description="""Get pre-filtered OTM put options within a strike range and budget. Single ticker only.

Args:
    ticker: str — single ticker
    otm_pct_min: float — minimum OTM % (default: 8)
    otm_pct_max: float — maximum OTM % (default: 12)
    budget_usd: float — max premium per contract (100 shares)
    expiry_after: str — YYYY-MM-DD minimum expiry date
""",
)
async def get_put_hedge_candidates(
    ticker: str,
    otm_pct_min: float = 8.0,
    otm_pct_max: float = 12.0,
    budget_usd: float = 500.0,
    expiry_after: str = "",
) -> str:
    """Return filtered put hedge candidates."""
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

    # Filter expiries >= expiry_after
    if expiry_after:
        qualifying_expiries = [e for e in expirations if e >= expiry_after]
    else:
        qualifying_expiries = list(expirations)

    # Select nearest 2
    qualifying_expiries = qualifying_expiries[:2]

    if not qualifying_expiries:
        return json.dumps({"error": True, "message": "No qualifying expiry dates", "ticker": ticker})

    strike_min = current_price * (1 - otm_pct_max / 100)
    strike_max = current_price * (1 - otm_pct_min / 100)

    candidates = []
    for exp in qualifying_expiries:
        try:
            chain = company.option_chain(exp)
            puts_df = chain.puts
        except Exception:
            continue

        # Filter strikes
        filtered = puts_df[(puts_df["strike"] >= strike_min) & (puts_df["strike"] <= strike_max)]

        # Collect IVs for percentile calculation
        all_ivs = puts_df["impliedVolatility"].dropna().tolist() if "impliedVolatility" in puts_df.columns else []

        for _, row in filtered.iterrows():
            bid = float(row.get("bid", 0) or 0)
            ask = float(row.get("ask", 0) or 0)
            mid = round((bid + ask) / 2, 2)
            contract_cost = round(mid * 100, 2)
            within_budget = contract_cost <= budget_usd
            strike = float(row["strike"])
            oi = int(row.get("openInterest", 0) or 0)
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
                "withinBudget": within_budget,
                "openInterest": oi,
                "ivPctile": iv_pctile,
                "ivFlag": iv_flag,
                "otmPct": otm_pct,
            })

    # Sort by expiry then strike
    candidates.sort(key=lambda c: (c["expiry"], c["strike"]))

    budget_feasible = any(c["withinBudget"] for c in candidates)

    # Generate note
    if not candidates:
        note = "No put options found in the specified OTM range."
        budget_gap = None
    elif not budget_feasible:
        nearest = min(candidates, key=lambda c: c["contractCost"])
        budget_gap = round(nearest["contractCost"] - budget_usd, 2)
        note = f"No candidates within budget. Nearest: ${nearest['strike']} put at ${nearest['contractCost']}/contract vs ${budget_usd} budget."
    else:
        budget_gap = None
        count = sum(1 for c in candidates if c["withinBudget"])
        note = f"{count} candidate(s) within ${budget_usd} budget."

    return json.dumps({
        "ticker": ticker,
        "currentPrice": round(current_price, 2),
        "strikeRangeMin": round(strike_min, 2),
        "strikeRangeMax": round(strike_max, 2),
        "budgetUsd": budget_usd,
        "candidates": candidates,
        "budgetFeasible": budget_feasible,
        "budgetGapUsd": budget_gap,
        "note": note,
        "dataDate": get_last_trading_date(),
    })


# ---------------------------------------------------------------------------
# Tool: get_analyst_upgrade_radar
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_analyst_upgrade_radar",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_analyst_upgrade_radar"],
    description="""Get recent analyst rating changes with canonical signal classification. Batch supported.

Returns: changes with signal (UPGRADE/DOWNGRADE/INITIATED/MAINTAIN), separate upgrade/downgrade/initiation counts, ptFrom, ptTo, ptDirection, mixedSignal, strengthFlag; netSentiment, summary.

ptFrom / ptTo: prior and new price target (null — yfinance does not expose numeric targets; stubs for
future compatibility). ptDirection: RAISE/CUT/UNCHANGED/INITIATED — derived from ptFrom→ptTo when
both are available; INITIATED for new coverage; UNCHANGED for reiterations with no target change.

Args:
    ticker: str | list[str] — single or batch
    days_back: int — lookback window in calendar days (default: 30)
""",
)
async def get_analyst_upgrade_radar(ticker: str | list[str], days_back: int = 30) -> str:
    """Return recent analyst upgrades/downgrades with signals."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_analyst_upgrade_radar(t, days_back))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
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
            "upgrades30d": 0 if days_back == 30 else None,
            "downgrades": 0,
            "downgrades30d": 0 if days_back == 30 else None,
            "initiations": 0,
            "initiations30d": 0 if days_back == 30 else None,
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

        # Price target fields — yfinance upgrades_downgrades doesn't expose
        # numeric price targets; stubs are included for forward-compatibility.
        pt_from: float | None = None
        pt_to: float | None = None

        # Derive ptDirection: use ptFrom/ptTo comparison when available;
        # fall back to grade-change signal when PT numerics are absent (Option A);
        # UNCHANGED for reiterations.
        if pt_from is not None and pt_to is not None:
            if pt_to > pt_from:
                pt_direction = "RAISE"
            elif pt_to < pt_from:
                pt_direction = "CUT"
            else:
                pt_direction = "UNCHANGED"
        elif signal == "INITIATED":
            pt_direction = "INITIATED"
        elif signal == "MAINTAIN":
            pt_direction = "UNCHANGED"
        elif signal == "UPGRADE":
            pt_direction = "RAISE"
        elif signal == "DOWNGRADE":
            pt_direction = "CUT"
        else:
            pt_direction = None

        mixed_signal = signal == "UPGRADE" and pt_direction == "CUT"

        # Strength flag
        if signal == "UPGRADE" and not mixed_signal:
            strength_flag = "BULLISH"
        elif signal == "DOWNGRADE":
            strength_flag = "BEARISH"
        elif mixed_signal:
            strength_flag = "MIXED"
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
        "upgrades30d": upgrade_count if days_back == 30 else None,
        "downgrades": downgrade_count,
        "downgrades30d": downgrade_count if days_back == 30 else None,
        "initiations": initiation_count,
        "initiations30d": initiation_count if days_back == 30 else None,
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
    return json.loads(df.reset_index().to_json(orient="records"))


@yfinance_server.tool(
    name="get_etf_info",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_etf_info"],
    description="""Get ETF or mutual fund data for one or more ticker symbols.

Returns identity (shortName, category, fundFamily, legalType, fundInceptionDate),
pricing (navPrice, previousClose, open, dayHigh, dayLow, volume, averageVolume),
AUM/costs (totalAssets, yield, annualReportExpenseRatio, ytdReturn, beta3Year),
52-week stats (fiftyTwoWeekHigh, fiftyTwoWeekLow, fiftyTwoWeekChange),
moving averages (fiftyDayAverage, twoHundredDayAverage),
top-10 holdings (topHoldings), and sector weights (sectorWeights).

Use this tool for ETF and fund tickers: SPY, QQQ, VTI, ARKK, VFIAX, etc.
For individual stocks, use get_fast_info or get_stock_info instead.

Args:
    ticker: str | list[str]
        A single ETF/fund ticker (e.g. "SPY") or a list of up to 5 symbols.
        When a list is provided, returns a dict keyed by symbol.
        Max 5 tickers per call; split larger lists into multiple calls.
""",
)
async def get_etf_info(ticker: str | list[str]) -> str:
    """Get ETF/fund information for one or more ticker symbols."""
    if isinstance(ticker, list):
        results = []
        for t in ticker:
            try:
                results.append(await get_etf_info(t))
            except Exception as e:
                results.append(json.dumps({"error": True, "message": str(e), "ticker": t}))
            await asyncio.sleep(0.1)
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})

    cache_key = f"etf_info:{ticker}"
    cached = _cache_get(cache_key, _PRICE_TTL)
    if cached is not None:
        return cached

    company = yf.Ticker(ticker)
    try:
        info = company.info
    except Exception as e:
        return f"Error: getting ETF info for {ticker}: {e}"

    data: dict = {k: info.get(k) for k in _ETF_INFO_FIELDS}

    # Top-10 holdings from funds_top_holdings DataFrame
    try:
        holdings_df = company.funds_top_holdings
        records = _df_to_records(None if holdings_df is None else holdings_df.head(10))
        data["topHoldings"] = records
    except Exception:
        data["topHoldings"] = None

    # Sector weights from funds_sector_weightings DataFrame
    try:
        data["sectorWeights"] = _df_to_records(company.funds_sector_weightings)
    except Exception:
        data["sectorWeights"] = None

    result = json.dumps(data)
    _cache_set(cache_key, result)
    return result



# ---------------------------------------------------------------------------
# Tool: get_overnight_quote
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_options_flow_scan",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_options_flow_scan"],
    description="""Structured options flow scan for a binary event window.

Returns the formatted options flow output block. Callers can paste formattedBlock directly into
client output. Prior window-label readings are cached server-side (72 h TTL) to enable trend
computation across readings (e.g. T-14 → T-7 → T-2).

Returns: pcRatio, ivPctile, putVolVs10dAvg, putVolTrend (INCREASING/STABLE/DECREASING),
maxPainStrike, bracket (UPPER/MID/LOWER), formattedBlock, dataDate.

Args:
    ticker: str
        The ticker symbol, e.g. "ASTS"
    window_label: str
        Free-form label for this reading, e.g. "T-14", "T-7", "T-2", "pre-earnings", "week1".
        Used as cache key for trend computation across readings.
""",
)
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
        exp = exps[0]
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

    # Max pain strike — strike with maximum combined open interest
    max_pain_strike: float | None = None
    scan_warnings: list[str] = []
    if call_oi_total + put_oi_total <= 0:
        scan_warnings.append("MAX_PAIN_UNAVAILABLE_ZERO_OI")
    else:
        try:
            combined = pd.concat([
                calls_df[["strike", "openInterest"]],
                puts_df[["strike", "openInterest"]],
            ])
            oi_by_strike = combined.groupby("strike")["openInterest"].sum()
            if not oi_by_strike.empty:
                max_pain_strike = float(oi_by_strike.idxmax())
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

    # IV percentile — approximate using annualised 30-day rolling realised vol over 1 year
    iv_pctile: int | None = None
    if quality == "LOW" and data_quality.get("placeholderIvCount", 0) > len(calls_list + puts_list) * 0.5:
        scan_warnings.append("IV_PERCENTILE_UNAVAILABLE_PLACEHOLDER_IV")
    else:
        try:
            hist_1y = company.history(period="1y", interval="1d")
            if hist_1y is not None and len(hist_1y) >= 30 and atm_iv is not None:
                rets = hist_1y["Close"].pct_change().dropna()
                roll_rv = rets.rolling(30).std() * (252 ** 0.5)
                roll_rv = roll_rv.dropna()
                if len(roll_rv) >= 5:
                    rv_min, rv_max = float(roll_rv.min()), float(roll_rv.max())
                    if rv_max > rv_min:
                        pctile = (atm_iv - rv_min) / (rv_max - rv_min) * 100
                        iv_pctile = max(0, min(100, round(pctile)))
        except Exception:
            pass

    # Put vol vs 10-day average proxy (since historical options volume is unavailable via yfinance)
    # Proxy: put vol as a multiple of 1% of the stock's 10d average daily volume.
    put_vol_vs_10d: float | None = None
    try:
        adv10 = fi["tenDayAverageVolume"]
        if adv10 and adv10 > 0 and put_vol > 0:
            put_vol_vs_10d = round(put_vol / (adv10 * 0.01), 2)
    except Exception:
        pass

    # Data date from history
    try:
        _h = company.history(period="5d", interval="1d")
        data_date = get_last_trading_date(_h)
    except Exception:
        data_date = get_last_trading_date()

    # Look up prior window reading for trend analysis
    prev_window_map = {"T-7": "T-14", "T-2": "T-7"}
    prev_window = prev_window_map.get(window_label)
    prev_data: dict | None = None
    if prev_window:
        prev_cached = _cache_get(f"options_flow:{ticker}:{prev_window}", 72 * 3600)
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
            prev_data.get("putVolVs10dAvg")
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
        iv_str = f"{iv_pctile}th%ile" if iv_pctile is not None else "N/A"
        pv_str = f"{put_vol_vs_10d:.2f}x" if put_vol_vs_10d is not None else "N/A"
        pc_str = f"{pc_ratio:.2f}" if pc_ratio is not None else "N/A"
        formatted_block = (
            f"OPTIONS FLOW SCAN [{window_label}] {ticker} | "
            f"P/C: {pc_str} | "
            f"IV: {iv_str} | "
            f"Put vol vs 10d avg: {pv_str} | "
            f"Trend: {put_vol_trend} | "
            f"Advisory: {bracket or 'N/A'} bracket"
        )

    result_dict: dict = {
        "ticker": ticker,
        "windowLabel": window_label,
        "dataDate": data_date,
        "pcRatio": pc_ratio,
        "ivPctile": iv_pctile,
        "putVolVs10dAvg": put_vol_vs_10d,
        "putVolTrend": put_vol_trend,
        "maxPainStrike": max_pain_strike,
        "bracket": bracket,
        "formattedBlock": formatted_block,
        "dataQuality": data_quality,
        "warnings": scan_warnings,
    }

    # Cache current reading for future trend comparison (72h TTL via 3-day window check)
    _cache_set(f"options_flow:{ticker}:{window_label}", json.dumps(result_dict))
    return json.dumps(result_dict)


# ---------------------------------------------------------------------------
# CR-13 — get_price_target_bracket
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_price_target_bracket",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_price_target_bracket"],
    description="""Compare current market price to a user-supplied reference target price and return distance/bracket labels.

ratio = currentPrice / reference_target_price × 100.

Brackets: ≤75% → STRONG_BUY | 75–90% → ACCEPTABLE | 90–100% → RISK | >100% → ABOVE_TARGET
Tags: <40% → SPECULATIVE | 40–79% → LONG | 80–99% → NEAR | ≥100% → INVERTED

Args:
    ticker: str
        The ticker symbol, e.g. "ASTS"
    reference_target_price: float | None
        Preferred user-supplied reference target price.
    io_pt: float | None
        Backward-compatible alias for reference_target_price.
""",
)
async def get_price_target_bracket(
    ticker: str, reference_target_price: float | None = None, io_pt: float | None = None
) -> str:
    """Return bracket and distance fields for current price vs reference target."""
    target_price = reference_target_price if reference_target_price is not None else io_pt
    if target_price is None or target_price <= 0:
        return json.dumps({
            "error": True,
            "message": "reference_target_price (or io_pt alias) must be a positive number",
            "ticker": ticker,
        })

    company = yf.Ticker(ticker)
    try:
        fi = company.fast_info
        current_price: float | None = fi["lastPrice"]
        if current_price is None:
            return json.dumps({"error": True, "message": f"No price data for {ticker}", "ticker": ticker})
    except Exception as e:
        return json.dumps({"error": True, "message": str(e), "ticker": ticker})

    reference_target_pct = round(current_price / target_price * 100, 1)

    if reference_target_pct <= 75:
        bracket = "STRONG_BUY"
    elif reference_target_pct <= 90:
        bracket = "ACCEPTABLE"
    elif reference_target_pct <= 100:
        bracket = "CAUTION"
    else:
        bracket = "AVOID"

    if reference_target_pct < 40:
        inferred_tag = "SPECULATIVE"
    elif reference_target_pct < 80:
        inferred_tag = "LONG"
    elif reference_target_pct < 100:
        inferred_tag = "NEAR"
    else:
        inferred_tag = "INVERTED"

    inverted_flag = reference_target_pct >= 100

    data_date: str = str(datetime.date.today())
    try:
        _h = company.history(period="5d", interval="1d")
        if _h is not None and not _h.empty:
            data_date = str(_h.index[-1].date())
    except Exception:
        pass

    return json.dumps({
        "ticker": ticker,
        "currentPrice": round(current_price, 4),
        "referenceTargetPrice": target_price,
        "referenceTargetPct": reference_target_pct,
        "ioPt": target_price,
        "eqfPct": reference_target_pct,
        "bracket": bracket,
        "inferredTag": inferred_tag,
        "tag": inferred_tag,
        "tagNote": "Deprecated: tag is inferred from currentPrice/referenceTargetPrice distance. Use inferredTag.",
        "invertedFlag": inverted_flag,
        "dataDate": data_date,
    })


# ---------------------------------------------------------------------------
# CR-14 — get_position_score_inputs
# ---------------------------------------------------------------------------

@yfinance_server.tool(
    name="get_position_score_inputs",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_position_score_inputs"],
    description="""Aggregate public market, analyst, earnings, and technical inputs for caller-defined scoring models.

Runs up to 6 parallel data fetches per call.

Returns grouped analyst, price/range, earnings-momentum, and technical indicator inputs plus dataDate.

This tool does not access holdings, cost basis, position size, or private scoring rules.

Args:
    ticker: str
        Single ticker symbol, e.g. "ASTS"
""",
)
async def get_position_score_inputs(ticker: str) -> str:
    """Return grouped public inputs for caller-defined scoring workflows."""
    results = await asyncio.gather(
        get_analyst_upgrade_radar(ticker, days_back=30),
        get_analyst_consensus(ticker),
        get_price_stats(ticker),
        get_earnings_momentum(ticker),
        get_technical_indicators(ticker, "3mo"),
        get_ma_position(ticker),
        return_exceptions=True,
    )

    def _parse(r: object) -> dict:
        if isinstance(r, Exception):
            return {}
        try:
            return json.loads(str(r)) if isinstance(r, str) else {}
        except Exception:
            return {}

    upgrade = _parse(results[0])
    consensus = _parse(results[1])
    price = _parse(results[2])
    earnings = _parse(results[3])
    tech = _parse(results[4])
    ma = _parse(results[5])

    # T1: analyst sentiment
    t1: dict = {
        "analystNetSentiment": upgrade.get("netSentiment"),
        "upgrades30d": upgrade.get("upgrades30d") if upgrade.get("upgrades30d") is not None else upgrade.get("upgrades"),
        "downgrades30d": upgrade.get("downgrades30d") if upgrade.get("downgrades30d") is not None else upgrade.get("downgrades"),
        "initiations30d": upgrade.get("initiations30d") if upgrade.get("initiations30d") is not None else upgrade.get("initiations"),
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

    # Data date: prefer last OHLCV row from technical indicators for consistent timing.
    data_date = tech.get("dataDate") or ma.get("dataDate") or get_last_trading_date()

    return json.dumps({
        "ticker": ticker,
        "dataDate": data_date,
        "t1_inputs": t1,
        "t2_inputs": t2,
        "t4_inputs": t4,
        "t5_inputs": t5,
    })


# ---------------------------------------------------------------------------
# CR-15 — get_volume_gate

def _deprecated_alias_response(alias_tool: str, canonical_tool: str, raw: str) -> str:
    warning_obj = {
        "code": "DEPRECATED_ALIAS",
        "message": f"Use {canonical_tool} instead.",
        "severity": "info",
    }
    if not _envelope._ENVELOPE_V2:
        return raw
    try:
        payload = json.loads(raw)
    except Exception:
        payload = raw
    if isinstance(payload, dict) and "ok" in payload:
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            meta = {}
            payload["meta"] = meta
        meta["tool"] = alias_tool
        meta["canonicalTool"] = canonical_tool
        meta["deprecatedTool"] = True
        meta["useInstead"] = canonical_tool
        warnings = meta.get("warnings")
        warning_list = list(warnings) if isinstance(warnings, list) else []
        warning_list.append(warning_obj)
        meta["warnings"] = warning_list
        return json.dumps(payload)
    return _mcp_success(
        alias_tool,
        payload,
        canonical_tool=canonical_tool,
        deprecated_tool=True,
        use_instead=canonical_tool,
        warnings=[warning_obj],
    )






@yfinance_server.tool(name="get_market_quote", output_schema=_TOOL_OUTPUT_SCHEMAS["get_fast_info"], description="Canonical alias for get_fast_info.")
async def get_market_quote(ticker: str | list[str]) -> str:
    return await get_fast_info(ticker)


@yfinance_server.tool(name="get_historical_prices", output_schema=_TOOL_OUTPUT_SCHEMAS["get_historical_stock_prices"], description="Canonical alias for get_historical_stock_prices.")
async def get_historical_prices(ticker: str, period: str = "1mo", interval: str = "1d", prepost: bool = False) -> str:
    return await get_historical_stock_prices(ticker=ticker, period=period, interval=interval, prepost=prepost)


@yfinance_server.tool(name="analyze_price_performance", output_schema=_TOOL_OUTPUT_SCHEMAS["get_price_stats"], description="Canonical alias for get_price_stats.")
async def analyze_price_performance(ticker: str | list[str]) -> str:
    return await get_price_stats(ticker)


@yfinance_server.tool(name="analyze_moving_average_position", output_schema=_TOOL_OUTPUT_SCHEMAS["get_ma_position"], description="Canonical alias for get_ma_position.")
async def analyze_moving_average_position(ticker: str | list[str]) -> str:
    return await get_ma_position(ticker)


@yfinance_server.tool(name="analyze_volume_ratio", output_schema=_TOOL_OUTPUT_SCHEMAS["get_volume_ratio"], description="Canonical alias for get_volume_ratio.")
async def analyze_volume_ratio(ticker: str | list[str], period: int = 10) -> str:
    return await get_volume_ratio(ticker, period)


@yfinance_server.tool(name="check_volume_liquidity_threshold", output_schema=_TOOL_OUTPUT_SCHEMAS["get_volume_gate"], description="Canonical alias for get_volume_gate.")
async def check_volume_liquidity_threshold(ticker: str, foreign_exchange: bool = False) -> str:
    return await get_volume_gate(ticker, foreign_exchange)


@yfinance_server.tool(name="get_company_profile", output_schema=_TOOL_OUTPUT_SCHEMAS["get_stock_info"], description="Canonical alias for get_stock_info.")
async def get_company_profile(ticker: str | list[str], include_all: bool = False) -> str:
    return await get_stock_info(ticker, include_all=include_all)


@yfinance_server.tool(name="get_fund_profile", output_schema=_TOOL_OUTPUT_SCHEMAS["get_etf_info"], description="Canonical alias for get_etf_info.")
async def get_fund_profile(ticker: str | list[str]) -> str:
    return await get_etf_info(ticker)


@yfinance_server.tool(name="analyze_financial_ratios", output_schema=_TOOL_OUTPUT_SCHEMAS["get_financial_ratios"], description="Canonical alias for get_financial_ratios.")
async def analyze_financial_ratios(ticker: str | list[str]) -> str:
    return await get_financial_ratios(ticker)


@yfinance_server.tool(name="analyze_credit_health", output_schema=_TOOL_OUTPUT_SCHEMAS["get_credit_health"], description="Canonical alias for get_credit_health.")
async def analyze_credit_health(ticker: str | list[str]) -> str:
    return await get_credit_health(ticker)


@yfinance_server.tool(name="get_corporate_actions", output_schema=_TOOL_OUTPUT_SCHEMAS["get_stock_actions"], description="Canonical alias for get_stock_actions.")
async def get_corporate_actions(ticker: str) -> str:
    return await get_stock_actions(ticker)


@yfinance_server.tool(name="get_ownership_holders", output_schema=_TOOL_OUTPUT_SCHEMAS["get_holder_info"], description="Canonical alias for get_holder_info.")
async def get_ownership_holders(ticker: str, holder_type: HolderType) -> str:
    return await get_holder_info(ticker, holder_type)


@yfinance_server.tool(name="get_analyst_recommendations", output_schema=_TOOL_OUTPUT_SCHEMAS["get_recommendations"], description="Canonical alias for get_recommendations.")
async def get_analyst_recommendations(ticker: str, recommendation_type: RecommendationType, months_back: int = 12) -> str:
    return await get_recommendations(ticker, recommendation_type, months_back)


@yfinance_server.tool(name="get_analyst_rating_changes", output_schema=_TOOL_OUTPUT_SCHEMAS["get_analyst_upgrade_radar"], description="Canonical alias for get_analyst_upgrade_radar.")
async def get_analyst_rating_changes(ticker: str | list[str], days_back: int = 30) -> str:
    return await get_analyst_upgrade_radar(ticker, days_back)


@yfinance_server.tool(name="analyze_earnings_momentum", output_schema=_TOOL_OUTPUT_SCHEMAS["get_earnings_momentum"], description="Canonical alias for get_earnings_momentum.")
async def analyze_earnings_momentum(ticker: str | list[str]) -> str:
    return await get_earnings_momentum(ticker)


@yfinance_server.tool(name="get_company_events_calendar", output_schema=_TOOL_OUTPUT_SCHEMAS["get_calendar"], description="Canonical alias for get_calendar.")
async def get_company_events_calendar(ticker: str) -> str:
    return await get_calendar(ticker)


@yfinance_server.tool(
    name="get_company_news",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_yahoo_finance_news"],
    description="""Get recent public company news and press releases from selected public sources.

Accepts a single ticker or an array of up to 5 symbols. For an array, each
ticker is fetched independently and results are returned as a per-ticker keyed
object (a union of per-ticker results), so low-news conditions for one ticker
never zero out the others.

Returns deduplicated source-backed items with precise source labels
(``yahoo_finance_news``, ``yahoo_finance_press_releases``, ``finnhub``),
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
``finnhub``, ``sec``, ``company_ir`` (RSS/Atom only), ``company_ir_page``
(Git-reviewed IR-page registry), ``newswire``, and the legacy
``yahoo_finance`` aggregate alias.
""",
)
async def get_company_news(
    ticker: str | list[str],
    max_results: int = 10,
    lookback_days: int = 14,
    sources: list[str] | None = None,
) -> str:
    # Batch path: fetch each ticker independently and return a per-ticker keyed
    # object (a union of results), matching the other multi-ticker tools. News is
    # fetched per ticker; there is no combined query that could zero out the
    # whole batch under low-news conditions.
    if isinstance(ticker, list):
        results = await asyncio.gather(
            *[get_company_news(t, max_results=max_results, lookback_days=lookback_days, sources=sources) for t in ticker],
            return_exceptions=True,
        )
        return json.dumps({t: _safe_parse(r, t) for t, r in zip(ticker, results)})
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_company_news", ErrorCode.INPUT_VALIDATION_ERROR, err)
    effective_sources = sources or ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"]
    items, sources_used, warnings, retrieved_at, source_diagnostics = _unpack_company_event_result(
        await _collect_company_events(
            ticker,
            max_results=max_results,
            lookback_days=lookback_days,
            sources=effective_sources,
            include_diagnostics=True,
        )
    )
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
    }
    if status:
        payload["status"] = status
    return json.dumps(payload)


@yfinance_server.tool(name="summarize_options_flow", output_schema=_TOOL_OUTPUT_SCHEMAS["get_options_summary"], description="Canonical alias for get_options_summary/get_options_flow_summary.")
async def summarize_options_flow(ticker: str, expiry_hint: str | None = None) -> str:
    return await get_options_summary(ticker=ticker, expiry_hint=expiry_hint)


@yfinance_server.tool(name="analyze_options_flow_window", output_schema=_TOOL_OUTPUT_SCHEMAS["get_options_flow_scan"], description="Canonical alias for get_options_flow_scan.")
async def analyze_options_flow_window(ticker: str, window_label: str) -> str:
    return await get_options_flow_scan(ticker, window_label)


@yfinance_server.tool(name="find_put_hedge_candidates", output_schema=_TOOL_OUTPUT_SCHEMAS["get_put_hedge_candidates"], description="Canonical alias for get_put_hedge_candidates.")
async def find_put_hedge_candidates(ticker: str, otm_pct_min: float = 8, otm_pct_max: float = 12, budget_usd: float = 500, expiry_after: str = "") -> str:
    return await get_put_hedge_candidates(ticker, otm_pct_min, otm_pct_max, budget_usd, expiry_after)


@yfinance_server.tool(name="calculate_price_target_distance", output_schema=_TOOL_OUTPUT_SCHEMAS["get_price_target_bracket"], description="Canonical alias for get_price_target_bracket.")
async def calculate_price_target_distance(
    ticker: str,
    reference_target_price: float | None = None,
    io_pt: float | None = None,
) -> str:
    return await get_price_target_bracket(
        ticker, reference_target_price=reference_target_price, io_pt=io_pt
    )


@yfinance_server.tool(name="analyze_position_signals", output_schema=_TOOL_OUTPUT_SCHEMAS["get_position_score_inputs"], description="Canonical alias for get_position_score_inputs.")
async def analyze_position_signals(ticker: str) -> str:
    return await get_position_score_inputs(ticker)


@yfinance_server.tool(name="list_sec_company_filings", output_schema=_TOOL_OUTPUT_SCHEMAS["list_sec_filings"], description="""List SEC filings for a company from EDGAR submissions.

Returns compact metadata for each filing including accession number, filing date, accepted timestamp, and a direct document URL.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type, e.g. "10-K", "10-Q", "8-K". Defaults to "10-K".
    limit: Maximum number of filings to return (1-20). Defaults to 5.
""")
async def list_sec_company_filings(ticker: str, filing_type: str = "10-K", limit: int = 5, form_type: str | None = None, max_filings: int | None = None) -> str:
    resolved_type = form_type or filing_type
    resolved_limit = min(max(1, max_filings or limit), 20)
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_sec_company_filings", ErrorCode.INPUT_VALIDATION_ERROR, err)

    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("list_sec_company_filings", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not find EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms: list[str] = recent.get("form", [])
    dates: list[str] = recent.get("filingDate", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    accepted_dts: list[str] = recent.get("acceptanceDateTime", [])

    results: list[dict] = []
    for i, form in enumerate(forms):
        if len(results) >= resolved_limit:
            break
        if str(form).upper() != resolved_type.upper():
            continue
        acc = accessions[i] if i < len(accessions) else ""
        date = dates[i] if i < len(dates) else ""
        accepted_at = accepted_dts[i] if i < len(accepted_dts) else None
        primary_doc = primary_docs[i] if i < len(primary_docs) else ""
        _, doc_url = _edgar_build_filing_urls(cik_int, acc, primary_doc)
        results.append({
            "filingType": form,
            "filingDate": date,
            "acceptedAt": accepted_at,
            "accessionNumber": acc,
            "primaryDocument": primary_doc,
            "documentUrl": doc_url,
        })

    retrieved_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    return json.dumps({
        "ticker": ticker,
        "cik": cik_padded,
        "filings": results,
        "meta": {
            "source": "sec_submissions",
            "retrievedAt": retrieved_at,
        },
    })


async def _resolve_latest_sec_doc_url(ticker: str, filing_type: str = "10-K") -> str | None:
    listed_raw = await list_sec_filings(ticker=ticker, form_type=filing_type, max_filings=1)
    try:
        listed = json.loads(listed_raw)
        filings = listed.get("filings") if isinstance(listed, dict) else None
        if isinstance(filings, list) and filings:
            first = filings[0] if isinstance(filings[0], dict) else {}
            return first.get("primaryDocumentUrl")
    except Exception:
        return None
    return None


@yfinance_server.tool(name="get_sec_filing_outline", output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_outline"], description="Canonical alias for get_filing_outline.")
async def get_sec_filing_outline(ticker: str, filing_type: str = "10-K", period: str = "latest", accession_number: str | None = None, document_url: str | None = None) -> str:
    resolved_doc_url = document_url or (await _resolve_latest_sec_doc_url(ticker, filing_type) if period == "latest" else None)
    return await get_filing_outline(ticker, accession_number, resolved_doc_url)


@yfinance_server.tool(name="get_sec_filing_section", output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_section"], description="Canonical alias for get_filing_section.")
async def get_sec_filing_section(
    ticker: str,
    filing_type: str = "10-K",
    selector: dict | None = None,
    section_name: str | None = None,
    document_url: str | None = None,
    context_chars: int = 3000,
) -> str:
    resolved_doc_url = document_url or await _resolve_latest_sec_doc_url(ticker, filing_type)
    section = section_name or (selector or {}).get("item") or "Item 1A"
    return await get_filing_section(ticker, str(section), str(resolved_doc_url), context_chars)


@yfinance_server.tool(name="list_sec_filing_tables", output_schema=_TOOL_OUTPUT_SCHEMAS["list_filing_tables"], description="Canonical alias for list_filing_tables.")
async def list_sec_filing_tables(ticker: str, filing_type: str = "10-K", document_url: str | None = None) -> str:
    resolved_doc_url = document_url or await _resolve_latest_sec_doc_url(ticker, filing_type)
    return await list_filing_tables(ticker, str(resolved_doc_url))


@yfinance_server.tool(name="get_sec_filing_table", output_schema=_TOOL_OUTPUT_SCHEMAS["get_filing_table"], description="Canonical alias for get_filing_table.")
async def get_sec_filing_table(ticker: str, table_index: int, filing_type: str = "10-K", document_url: str | None = None, max_rows: int = 30) -> str:
    resolved_doc_url = document_url or await _resolve_latest_sec_doc_url(ticker, filing_type)
    return await get_filing_table(ticker, str(resolved_doc_url), table_index, max_rows)


# ---------------------------------------------------------------------------
# Retrieval path and alternative query helpers for extract_sec_filing_fact
# ---------------------------------------------------------------------------

def _map_extraction_to_retrieval_path(extraction_method: str) -> str:
    """Map extractionMethod to a semantic retrieval path label."""
    method_upper = str(extraction_method or "").upper()
    if method_upper in ("XBRL", "COMPANYFACTS", "XBRL_COMPANYFACTS"):
        return "XBRL"
    elif method_upper in ("HTML_TABLE", "INDEXED_TABLE", "TABLE_PARSE"):
        return "INDEXED_TABLE"
    elif method_upper in ("TEXT_SEARCH", "SECTION_TEXT", "TARGETED_TEXT"):
        return "SECTION_TEXT"
    elif method_upper in ("FULL_DOC_SEARCH", "FULL_TEXT", "FALLBACK"):
        return "FULL_DOC_SEARCH"
    elif method_upper == "NONE":
        return "NONE"
    return "UNKNOWN"


def _suggest_alternative_queries(fact_type: str, region: str | None, payload: dict) -> list[str]:
    """Suggest alternative queries when confidence is low or result is NOT_DISCLOSED."""
    source = str(payload.get("source") or "").upper()
    confidence = str(payload.get("confidence") or "").upper()

    if source not in ("NOT_DISCLOSED", "CONFLICTING") and confidence not in ("NOT_DISCLOSED", "LOW"):
        return []

    suggestions: list[str] = []
    if fact_type == "geographic_revenue" and region:
        suggestions.append(f"search_sec_filing_text with search_terms=['{region}', 'revenue'] and section_hint='geographic'")
        suggestions.append("Try extract_revenue_exposure for broader region matching")
    elif fact_type == "segment_revenue":
        suggestions.append("search_sec_filing_text with search_terms=['segment', 'revenue'] and section_hint='segment'")
    elif fact_type in ("total_revenue", "net_income", "operating_income"):
        suggestions.append(f"search_sec_filing_text with search_terms=['{fact_type.replace('_', ' ')}']")
        suggestions.append("Try get_sec_filing_table with the financial statements table")
    elif fact_type in ("long_term_debt", "cash"):
        suggestions.append("search_sec_filing_text with search_terms=['debt', 'borrowings'] and section_hint='balance sheet'")
    elif fact_type in ("capex", "rd_expense"):
        suggestions.append(f"search_sec_filing_text with search_terms=['{fact_type.replace('_', ' ')}']")

    if not suggestions:
        suggestions.append("Use get_sec_filing_intelligence to check available data sources")
        suggestions.append("Try get_sec_filing_section_markdown for the relevant section")

    return suggestions


@yfinance_server.tool(name="extract_sec_filing_fact", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_filing_fact"], description="Canonical SEC fact extractor (routes to get_filing_data or extract_filing_fact).")
async def extract_sec_filing_fact(
    ticker: str,
    fact: str | None = None,
    fact_name: str | None = None,
    fact_type: FilingFactType | None = None,
    region: str | None = None,
    filing_type: str = "10-K",
    period: str = "latest",
    period_mode: str = "auto",
    document_url: str | None = None,
    accession_number: str | None = None,
) -> str:
    routed_fact_type = fact_type
    if routed_fact_type is None and fact is not None:
        try:
            routed_fact_type = FilingFactType(fact)
        except Exception:
            routed_fact_type = FilingFactType.geographic_revenue if region is not None else None
    if routed_fact_type is not None or region is not None or fact_name is None:
        routed_fact_type = routed_fact_type or FilingFactType.geographic_revenue
        raw = await get_filing_data(ticker=ticker, fact_type=routed_fact_type, region=region, filing_type=filing_type, period=period, period_mode=period_mode)
        parsed_payload: dict = {}
        try:
            parsed_any = json.loads(raw)
            if isinstance(parsed_any, dict) and "ok" in parsed_any and "data" in parsed_any:
                parsed_any = parsed_any.get("data")
            if isinstance(parsed_any, str):
                parsed_any = json.loads(parsed_any)
            if isinstance(parsed_any, dict):
                parsed_payload = parsed_any
        except Exception:
            parsed_payload = {}
        return json.dumps({
            "fact": routed_fact_type.value,
            "region": region,
            "value": parsed_payload.get("value"),
            "denominator": parsed_payload.get("denominator"),
            "valueRatio": parsed_payload.get("valueRatio"),
            "valuePct": parsed_payload.get("valuePct"),
            "rawValue": parsed_payload.get("rawValue"),
            "rawDenominator": parsed_payload.get("rawDenominator"),
            "unit": "USD",
            "unitScale": parsed_payload.get("unitScale"),
            "period": parsed_payload.get("period"),
            "filingType": parsed_payload.get("filingType", filing_type),
            "filingDate": parsed_payload.get("filingDate"),
            "accessionNumber": parsed_payload.get("accessionNumber"),
            "extractionMethod": parsed_payload.get("extractionMethod", "NONE"),
            "source": parsed_payload.get("source", "NOT_DISCLOSED"),
            "confidence": parsed_payload.get("confidence", "NOT_DISCLOSED"),
            "xbrlContext": parsed_payload.get("xbrlContext"),
            "retrieval_path": _map_extraction_to_retrieval_path(parsed_payload.get("extractionMethod", "NONE")),
            "documentUrl": parsed_payload.get("documentUrl"),
            "indexUrl": parsed_payload.get("indexUrl"),
            "primaryDocumentUrl": parsed_payload.get("primaryDocumentUrl"),
            "evidence": parsed_payload.get("evidence"),
            "calculation": parsed_payload.get("calculation"),
            "warnings": parsed_payload.get("warnings", []),
            "alternative_queries": _suggest_alternative_queries(routed_fact_type.value, region, parsed_payload),
            "ticker": parsed_payload.get("ticker", ticker),
        })
    return await extract_filing_fact(ticker=ticker, fact_name=fact_name, document_url=document_url, accession_number=accession_number)


@yfinance_server.tool(name="search_sec_filing_text", output_schema=_TOOL_OUTPUT_SCHEMAS["search_filing_text"], description="Canonical alias for search_filing_text.")
async def search_sec_filing_text(
    ticker: str,
    search_terms: list[str] | None = None,
    search_query: str | None = None,
    selector: dict | None = None,
    section_hint: str | None = None,
    filing_type: str = "10-K",
    accession_number: str | None = None,
    context_chars: int = 1500,
    return_tables: bool = True,
) -> str:
    terms = search_terms or ([search_query] if search_query else [])
    hint = section_hint or (selector or {}).get("item")
    return await search_filing_text(ticker, terms, hint, filing_type, accession_number, context_chars, return_tables)


# ---------------------------------------------------------------------------
# SEC Filing Index helpers
# ---------------------------------------------------------------------------

_INDEX_KEYWORDS = [
    "china", "greater china", "prc", "geographic", "segment", "revenue",
    "customers", "long-lived assets", "risk factors", "americas", "europe",
    "japan", "asia", "rest of asia",
]


def _build_filing_index_from_html(html: str) -> dict:
    """Parse an SEC filing HTML and return a structured index (sections, tables, keywordMap)."""
    # Sanitize: remove scripts/styles/event handlers.
    # Apply iteratively until stable to prevent nested/malformed pattern bypass.
    _script_re = _re.compile(r'<script\b[^>]*>[\s\S]*?</\s*script[^>]*>', _re.IGNORECASE)
    _style_re = _re.compile(r'<style\b[^>]*>[\s\S]*?</\s*style[^>]*>', _re.IGNORECASE)
    sanitized = html
    while True:
        next_s = _script_re.sub('<!--removed-->', sanitized)
        next_s = _style_re.sub('<!--removed-->', next_s)
        if next_s == sanitized:
            break
        sanitized = next_s
    sanitized = _re.sub(r'\s+on\w+=(?:"[^"]*"|\'[^\']*\'|[^\s>]+)', ' ', sanitized, flags=_re.IGNORECASE)

    # Section extraction
    sections: list[dict] = []
    heading_re = _re.compile(r'<h([1-6])[^>]*>(.*?)</h\1>', _re.DOTALL | _re.IGNORECASE)
    for h_match in heading_re.finditer(sanitized):
        if len(sections) >= 50:
            break
        level = int(h_match.group(1))
        raw_text = _strip_html_tags(h_match.group(2))
        if not raw_text or len(raw_text) > 200:
            continue
        normalized = raw_text.lower().strip()
        keywords = [kw for kw in _INDEX_KEYWORDS if kw in normalized]
        section_id = _re.sub(r'[^a-z0-9]+', '_', normalized)[:60]
        sections.append({
            "sectionId": section_id,
            "heading": raw_text,
            "normalizedHeading": normalized,
            "level": level,
            "keywords": keywords,
            "startChar": h_match.start(),
            "endChar": h_match.end(),
        })

    # Table extraction
    tables: list[dict] = []
    table_re = _re.compile(r'<table[^>]*>(.*?)</table>', _re.DOTALL | _re.IGNORECASE)
    tr_re = _re.compile(r'<tr[^>]*>(.*?)</tr>', _re.DOTALL | _re.IGNORECASE)
    td_re = _re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', _re.DOTALL | _re.IGNORECASE)

    for table_idx, t_match in enumerate(table_re.finditer(sanitized)):
        if table_idx >= 100:
            break
        table_start = t_match.start()
        table_html = t_match.group(0)

        # Nearest section starting before this table
        nearby_section_id: str | None = None
        nearby_heading = ""
        for sec in reversed(sections):
            if sec["startChar"] <= table_start:
                nearby_section_id = sec["sectionId"]
                nearby_heading = sec["heading"]
                break

        rows = tr_re.findall(t_match.group(1))
        if not rows:
            continue

        # Headers from first row
        first_cells = td_re.findall(rows[0])
        headers = [_strip_html_tags(c) for c in first_cells[:10]]

        # Row labels from first column of subsequent rows
        row_labels: list[str] = []
        for row in rows[1:20]:
            cells = td_re.findall(row)
            if cells:
                label = _strip_html_tags(cells[0])
                if label and len(label) < 100:
                    row_labels.append(label)

        # Unit scale: default to "unknown"; detect explicitly from context.
        pre_context = sanitized[max(0, table_start - 2000):table_start].lower()
        table_context = table_html.lower() + pre_context
        if "billion" in table_context:
            unit_scale = "billions"
        elif "million" in table_context:
            unit_scale = "millions"
        elif "thousand" in table_context:
            unit_scale = "thousands"
        else:
            unit_scale = "unknown"

        # Confidence: also lower when unitScale is unknown
        has_year_headers = any(_re.search(r'\b20\d\d\b', h) for h in headers)
        has_row_labels = bool(row_labels)
        if has_year_headers and has_row_labels and unit_scale != "unknown":
            confidence = "HIGH"
        elif has_year_headers or has_row_labels:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        # Infer title from preceding text
        pre_text = _strip_html_tags(sanitized[max(0, table_start - 500):table_start])
        lines = [ln.strip() for ln in pre_text.split('\n') if ln.strip()]
        title = ""
        if lines:
            candidate = lines[-1]
            if 10 < len(candidate) < 200:
                title = candidate

        tables.append({
            "tableId": table_idx,
            "sectionId": nearby_section_id,
            "title": title or nearby_heading,
            "headers": headers,
            "rowLabels": row_labels,
            "unit": "USD",
            "unitScale": unit_scale,
            "confidence": confidence,
        })

    # Keyword map
    keyword_map: dict[str, list[str]] = {}
    for kw in _INDEX_KEYWORDS:
        refs: list[str] = []
        for sec in sections:
            if kw in sec["normalizedHeading"]:
                ref = f"sectionId:{sec['sectionId']}"
                if ref not in refs:
                    refs.append(ref)
        for tbl in tables:
            haystack = " ".join(tbl["rowLabels"] + tbl["headers"] + [tbl["title"]]).lower()
            if kw in haystack:
                ref = f"tableId:{tbl['tableId']}"
                if ref not in refs:
                    refs.append(ref)
        if refs:
            keyword_map[kw] = refs

    return {"sections": sections, "tables": tables, "keywordMap": keyword_map}


async def _index_sec_filing_impl(
    ticker: str,
    filing_type: str = "10-K",
    accession_number: str | None = None,
) -> str:
    """Shared implementation for index_sec_filing and get_sec_filing_index."""
    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("index_sec_filing", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not resolve EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms: list[str] = recent.get("form", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    filing_dates: list[str] = recent.get("filingDate", [])
    accepted_dts: list[str] = recent.get("acceptanceDateTime", [])

    target_idx: int | None = None
    if accession_number:
        for i, acc in enumerate(accessions):
            if acc == accession_number:
                target_idx = i
                break
    else:
        for i, form in enumerate(forms):
            if str(form).upper() == filing_type.upper():
                target_idx = i
                accession_number = accessions[i] if i < len(accessions) else None
                break

    if target_idx is None or not accession_number:
        return _mcp_failure("index_sec_filing", ErrorCode.NO_FILING_DATA,
                            f"No {filing_type} filing found for '{ticker}'")

    filing_date = filing_dates[target_idx] if target_idx < len(filing_dates) else ""
    accepted_at = accepted_dts[target_idx] if target_idx < len(accepted_dts) else None
    primary_doc = primary_docs[target_idx] if target_idx < len(primary_docs) else None

    _, document_url = _edgar_build_filing_urls(cik_int, accession_number, primary_doc)
    if not document_url:
        return _mcp_failure("index_sec_filing", ErrorCode.NO_FILING_DATA,
                            f"primaryDocument missing for {accession_number}")

    # Check cache
    cache_key = f"secidx:{ticker.upper()}:{accession_number}:{filing_type}"
    cached = _tool_cache.get(cache_key)
    if cached is not None:
        return cached[0]

    # Fetch filing HTML
    html = await _edgar_get_html(document_url, max_bytes=5_000_000)
    if not html:
        return _mcp_failure("index_sec_filing", ErrorCode.PROVIDER_ERROR,
                            f"Failed to fetch filing document: {document_url}")

    index = _build_filing_index_from_html(html)
    indexed_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    result = json.dumps({
        "ticker": ticker,
        "cik": cik_padded,
        "filingType": filing_type,
        "filingDate": filing_date,
        "acceptedAt": accepted_at,
        "accessionNumber": accession_number,
        "documentUrl": document_url,
        "index": index,
        "meta": {
            "indexedAt": indexed_at,
            "source": "sec",
            "cacheKey": f"{ticker.upper()}:{accession_number}",
            "cacheTtlHours": 24,
        },
    })

    _tool_cache.set(cache_key, result, TTL_EDGAR)
    return result


@yfinance_server.tool(
    name="index_sec_filing",
    output_schema=_TOOL_OUTPUT_SCHEMAS["index_sec_filing"],
    description="""Build a deterministic section/table index for an SEC filing.
Identifies headings, tables, row labels, and units, enabling subsequent queries without re-fetching the filing.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type, e.g. "10-K" or "10-Q". Defaults to "10-K".
    period: Reserved for future multi-period support. Currently only "latest" is supported.
        When accession_number is provided, the specific filing is indexed regardless of period.
    accession_number: Optional SEC accession number (format XXXXXXXXXX-YY-ZZZZZZ).
        If omitted, the most recent filing matching filing_type is indexed.
""",
)
async def index_sec_filing(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("index_sec_filing", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("index_sec_filing", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)
    return await _index_sec_filing_impl(ticker, filing_type, accession_number)


@yfinance_server.tool(
    name="get_sec_filing_index",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_sec_filing_index"],
    description="""Get the pre-built section/table index for an SEC filing.
Returns cached index when available; builds and caches on first call.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type, e.g. "10-K" or "10-Q". Defaults to "10-K".
    period: Reserved for future multi-period support. Currently only "latest" is supported.
        When accession_number is provided, the specific filing is returned regardless of period.
    accession_number: Optional SEC accession number (format XXXXXXXXXX-YY-ZZZZZZ).
        If omitted, the most recent filing matching filing_type is used.
""",
)
async def get_sec_filing_index(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("get_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)
    return await _index_sec_filing_impl(ticker, filing_type, accession_number)


# ---------------------------------------------------------------------------
# SEC Material Filing Forms (non-noisy filings for intelligence layer)
# ---------------------------------------------------------------------------
_SEC_MATERIAL_FORMS_DEFAULT: list[str] = [
    "10-K", "10-Q", "8-K", "S-1", "424B", "DEF 14A", "20-F", "6-K",
]

_SEC_NOISY_FORMS: set[str] = {
    "4", "3", "5", "SC 13G", "SC 13G/A", "SC 13D", "SC 13D/A",
    "144", "SD", "CORRESP", "UPLOAD", "CT ORDER",
}


@yfinance_server.tool(
    name="list_sec_material_filings",
    output_schema=_TOOL_OUTPUT_SCHEMAS["list_sec_material_filings"],
    description="""List latest material SEC filings for a ticker, filtering out noise (Form 4, 144, SC 13G, etc.).
Returns only significant filings (10-K, 10-Q, 8-K, S-1, 424B, DEF 14A, 20-F, 6-K by default).

Args:
    ticker: Ticker symbol.
    forms: List of form types to include (default: ["10-K", "10-Q", "8-K", "S-1", "424B", "DEF 14A", "20-F", "6-K"]).
    limit: Maximum number of filings to return (default: 5, max: 20).
""",
)
async def list_sec_material_filings(
    ticker: str,
    forms: list[str] | None = None,
    limit: int = 5,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_sec_material_filings", ErrorCode.INPUT_VALIDATION_ERROR, err)

    resolved_limit = min(max(1, limit), 20)
    allowed_forms: set[str] = set(f.upper() for f in (forms or _SEC_MATERIAL_FORMS_DEFAULT))

    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("list_sec_material_filings", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not find EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms_list: list[str] = recent.get("form", [])
    dates: list[str] = recent.get("filingDate", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    accepted_dts: list[str] = recent.get("acceptanceDateTime", [])

    results: list[dict] = []
    for i, form in enumerate(forms_list):
        if len(results) >= resolved_limit:
            break
        form_upper = str(form).upper()
        # Filter: must match allowed forms and not be in noisy set
        if form_upper in _SEC_NOISY_FORMS:
            continue
        # Check if form matches any of the allowed prefixes (e.g. "424B" matches "424B4")
        matched = any(form_upper == af or form_upper.startswith(af) for af in allowed_forms)
        if not matched:
            continue
        acc = accessions[i] if i < len(accessions) else ""
        date = dates[i] if i < len(dates) else ""
        accepted_at = accepted_dts[i] if i < len(accepted_dts) else None
        primary_doc = primary_docs[i] if i < len(primary_docs) else ""
        _, doc_url = _edgar_build_filing_urls(cik_int, acc, primary_doc)

        # XBRL availability: true if companyfacts have been fetched for this CIK.
        # This is a fast cache check; call get_sec_filing_intelligence for precise status.
        xbrl_available = _EDGAR_FACTS_CACHE.get(cik_padded) is not None

        results.append({
            "filingType": form,
            "filingDate": date,
            "acceptedAt": accepted_at,
            "accessionNumber": acc,
            "primaryDocument": primary_doc,
            "documentUrl": doc_url,
            "xbrl_available": xbrl_available,
        })

    retrieved_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    return json.dumps({
        "ticker": ticker,
        "cik": cik_padded,
        "filings": results,
        "meta": {
            "source": "sec_submissions",
            "materialFormsFilter": sorted(allowed_forms),
            "retrievedAt": retrieved_at,
        },
    })


# ---------------------------------------------------------------------------
# SEC Filing Intelligence - composite tool
# ---------------------------------------------------------------------------

# XBRL concept names for the intelligence snapshot
_XBRL_INTELLIGENCE_CONCEPTS: dict[str, list[str]] = {
    "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "cash": ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsAndShortTermInvestments"],
    "long_term_debt": ["LongTermDebt", "LongTermDebtNoncurrent"],
    "total_assets": ["Assets"],
    "operating_income": ["OperatingIncomeLoss"],
}


@yfinance_server.tool(
    name="get_sec_filing_intelligence",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_sec_filing_intelligence"],
    description="""Get a comprehensive intelligence map of a company's SEC filing — XBRL facts snapshot, section/table index summary, and recommended queries — in a single call.
Gives the LLM a filing "map" so it knows what data is available and can make targeted follow-up calls.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type (default: "10-K").
    filing_index: 0 = latest, 1 = previous (default: 0).
""",
)
async def get_sec_filing_intelligence(
    ticker: str,
    filing_type: str = "10-K",
    filing_index: int = 0,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_sec_filing_intelligence", ErrorCode.INPUT_VALIDATION_ERROR, err)

    filing_index = max(0, min(filing_index, 9))

    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("get_sec_filing_intelligence", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not find EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms_list: list[str] = recent.get("form", [])
    dates: list[str] = recent.get("filingDate", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    accepted_dts: list[str] = recent.get("acceptanceDateTime", [])

    # Find the Nth matching filing
    match_count = 0
    target_idx: int | None = None
    for i, form in enumerate(forms_list):
        if str(form).upper() == filing_type.upper():
            if match_count == filing_index:
                target_idx = i
                break
            match_count += 1

    if target_idx is None:
        return _mcp_failure("get_sec_filing_intelligence", ErrorCode.NO_FILING_DATA,
                            f"No {filing_type} filing at index {filing_index} for '{ticker}'")

    accession_number = accessions[target_idx] if target_idx < len(accessions) else ""
    filing_date = dates[target_idx] if target_idx < len(dates) else ""
    accepted_at = accepted_dts[target_idx] if target_idx < len(accepted_dts) else None
    primary_doc = primary_docs[target_idx] if target_idx < len(primary_docs) else ""
    _, document_url = _edgar_build_filing_urls(cik_int, accession_number, primary_doc)

    # --- XBRL facts snapshot ---
    xbrl_available = False
    xbrl_facts: dict[str, dict | None] = {}
    xbrl_status = "UNAVAILABLE"
    try:
        facts_data = await _edgar_get_company_facts(cik_padded)
        if facts_data and facts_data.get("facts"):
            xbrl_available = True
            xbrl_status = "OK"
            for fact_key, concepts in _XBRL_INTELLIGENCE_CONCEPTS.items():
                xbrl_facts[fact_key] = _extract_xbrl_latest_annual(facts_data, concepts)
    except Exception:
        xbrl_status = "ERROR"

    # --- Filing index (sections/tables) ---
    index_status = "UNAVAILABLE"
    sections_list: list[str] = []
    sections_count = 0
    tables_count = 0
    exhibits_count = 0
    try:
        index_raw = await _index_sec_filing_impl(ticker, filing_type, accession_number)
        index_data = json.loads(index_raw)
        if isinstance(index_data, dict) and "index" in index_data:
            idx = index_data["index"]
            sections_count = len(idx.get("sections", []))
            tables_count = len(idx.get("tables", []))
            sections_list = [s.get("heading", "") for s in idx.get("sections", [])[:20]]
            index_status = "OK"
    except Exception:
        index_status = "ERROR"

    # Recommended queries based on filing type
    recommended_queries = [
        "revenue by segment",
        "risk factors",
        "liquidity and capital resources",
        "customer concentration",
        "long-term debt",
    ]
    if filing_type.upper() in ("10-K", "20-F"):
        recommended_queries.extend(["geographic revenue", "R&D expense", "guidance"])
    elif filing_type.upper() == "10-Q":
        recommended_queries.extend(["quarter-over-quarter revenue", "material events"])
    elif filing_type.upper() == "8-K":
        recommended_queries = ["material event", "exhibit content", "financial results"]

    return json.dumps({
        "ticker": ticker,
        "filing": {
            "type": filing_type,
            "accessionNumber": accession_number,
            "filedAt": filing_date,
            "acceptedAt": accepted_at,
            "documentUrl": document_url,
        },
        "xbrl_available": xbrl_available,
        "xbrl_facts": {k: v for k, v in xbrl_facts.items() if v is not None},
        "index": {
            "sections_count": sections_count,
            "tables_count": tables_count,
            "sections": sections_list,
            "exhibits_count": exhibits_count,
        },
        "recommended_queries": recommended_queries,
        "status": {
            "xbrl": xbrl_status,
            "index": index_status,
            "sections": "AVAILABLE" if sections_count > 0 else "EMPTY",
        },
    })


def _is_toc_match(html: str, match_start: int, match_end: int) -> bool:
    if _re.search(r'<a\b[^>]*\bhref\s*=\s*[\'"]#[^\'"]*[\'"]', html[max(0, match_start - 30):min(len(html), match_end + 30)], _re.IGNORECASE):
        return True
    surr_before = html[max(0, match_start - 100):match_start]
    if _re.search(r'<a\b[^>]*\bhref\s*=\s*[\'"]#[^\'"]*[\'"][^>]*>\s*$', surr_before, _re.IGNORECASE):
        return True
    context = html[max(0, match_start - 150):min(len(html), match_end + 150)]
    if "...." in context or ". . ." in context or "&#183;" in context or "&middot;" in context:
        return True
    plain_context = _re.sub(r'<[^>]+>', ' ', context)
    if _re.search(r'\.{3,}\s*\d+|\.\s*\.\s*\.\s*\d+', plain_context):
        return True
    return False


def _find_section_bounds(html: str, section: str, max_chars: int = 50000) -> tuple[int | None, int | None, str, bool, str | None]:
    section_lower = section.lower().strip()
    heading_re = _re.compile(r'<h([1-6])[^>]*>(.*?)</h\1>', _re.DOTALL | _re.IGNORECASE)
    candidates = []
    toc_skipped = False
    
    all_headings = list(heading_re.finditer(html))
    for idx, h_match in enumerate(all_headings):
        heading_text = _strip_html_tags(h_match.group(2)).lower().strip()
        level = int(h_match.group(1))
        
        if section_lower in heading_text or heading_text in section_lower:
            if _is_toc_match(html, h_match.start(), h_match.end()):
                toc_skipped = True
                continue
            candidates.append((h_match.start(), level, _strip_html_tags(h_match.group(2)).strip(), idx))
            
    item_matches = []
    if not candidates:
        item_re = _re.compile(
            rf'(?:<b[^>]*>|<span[^>]*font-weight:\s*bold[^>]*>)\s*{_re.escape(section)}\b',
            _re.IGNORECASE,
        )
        for item_match in item_re.finditer(html):
            if _is_toc_match(html, item_match.start(), item_match.end()):
                toc_skipped = True
                continue
            item_matches.append((item_match.start(), item_match.end()))
            
    if not candidates and not item_matches:
        return None, None, "", toc_skipped, None
        
    if len(candidates) > 1 or len(item_matches) > 1:
        return None, None, "", toc_skipped, "SECTION_AMBIGUOUS"
        
    if candidates:
        start_pos, level, found_heading, h_idx = candidates[0]
        end_pos = None
        for next_h in all_headings[h_idx + 1:]:
            next_level = int(next_h.group(1))
            if next_level <= level:
                end_pos = next_h.start()
                break
        if end_pos is None:
            end_pos = min(start_pos + max_chars * 3, len(html))
        return start_pos, end_pos, found_heading, toc_skipped, None
    else:
        start_pos, end_pos_match = item_matches[0]
        found_heading = section
        _MIN_SECTION_SPACING = 100
        item_re = _re.compile(
            rf'(?:<b[^>]*>|<span[^>]*font-weight:\s*bold[^>]*>)\s*{_re.escape(section)}\b',
            _re.IGNORECASE,
        )
        next_start = end_pos_match + _MIN_SECTION_SPACING
        end_pos = None
        while True:
            next_item = item_re.search(html, next_start)
            if not next_item:
                break
            if _is_toc_match(html, next_item.start(), next_item.end()):
                next_start = next_item.end()
                continue
            end_pos = next_item.start()
            break
        if end_pos is None:
            end_pos = min(start_pos + max_chars * 3, len(html))
        return start_pos, end_pos, found_heading, toc_skipped, None


@yfinance_server.tool(
    name="get_sec_filing_section_markdown",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_sec_filing_section_markdown"],
    description="""Return a specific SEC filing section as LLM-ready Markdown.
Converts filing HTML to clean Markdown with preserved section headers and pipe-delimited tables.
Use after get_sec_filing_intelligence to drill into a specific section.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type (default: "10-K").
    section: Section name to extract (e.g. "Risk Factors", "Item 7", "MD&A", "Item 1A").
    filing_index: 0 = latest, 1 = previous (default: 0).
    max_chars: Maximum characters to return (default: 50000).
""",
)
async def get_sec_filing_section_markdown(
    ticker: str,
    section: str = "Item 1A",
    filing_type: str = "10-K",
    filing_index: int = 0,
    max_chars: int = 50000,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.INPUT_VALIDATION_ERROR, err)

    filing_index = max(0, min(filing_index, 9))
    max_chars = min(max(1000, max_chars), 100000)

    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not find EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms_list: list[str] = recent.get("form", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])

    # Find the Nth matching filing
    match_count = 0
    target_idx: int | None = None
    for i, form in enumerate(forms_list):
        if str(form).upper() == filing_type.upper():
            if match_count == filing_index:
                target_idx = i
                break
            match_count += 1

    if target_idx is None:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.NO_FILING_DATA,
                            f"No {filing_type} filing at index {filing_index} for '{ticker}'")

    accession_number = accessions[target_idx] if target_idx < len(accessions) else ""
    primary_doc = primary_docs[target_idx] if target_idx < len(primary_docs) else ""
    _, document_url = _edgar_build_filing_urls(cik_int, accession_number, primary_doc)

    if not document_url:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.NO_FILING_DATA,
                            f"No document URL for {accession_number}")

    # Fetch the filing HTML
    html = await _edgar_get_html(document_url, max_bytes=5_000_000)
    if not html:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.PROVIDER_ERROR,
                            f"Failed to fetch filing document: {document_url}")

# Find the section boundaries using heading patterns
    start_idx, end_idx, found_heading, toc_skipped, err_code = _find_section_bounds(html, section, max_chars)
    if err_code == "SECTION_AMBIGUOUS":
        return _mcp_failure("get_sec_filing_section_markdown", "SECTION_AMBIGUOUS", "The section heading could not be resolved unambiguously.")

    if start_idx is None:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.NO_FILING_DATA,
                            f"Section '{section}' not found in filing")

    parser_source = "html_parser_fallback"
    markdown = _html_to_markdown_fallback(html, start_idx, end_idx)

    # Count tables in the section
    section_slice = html[start_idx:end_idx]
    tables_in_section = len(_re.findall(r'<table[^>]*>', section_slice, _re.IGNORECASE))

    # Truncate if needed
    truncated = False
    if len(markdown) > max_chars:
        markdown = markdown[:max_chars].rstrip()
        truncated = True

    word_count = len(markdown.split())

    return json.dumps({
        "ticker": ticker,
        "section": found_heading or section,
        "filingType": filing_type,
        "accessionNumber": accession_number,
        "markdown": markdown,
        "tables_in_section": tables_in_section,
        "word_count": word_count,
        "confidence": "MEDIUM",
        "source": parser_source,
        "truncated": truncated,
        "sectionStartOffset": start_idx,
        "sectionEndOffset": end_idx,
        "matchedHeading": found_heading,
        "tocSkipped": toc_skipped,
    })


def _as_status(source_payload: dict) -> str:
    confidence = str(source_payload.get("confidence") or "").upper()
    source = str(source_payload.get("source") or "").upper()
    if source in {"NOT_DISCLOSED"} or confidence in {"NOT_DISCLOSED"}:
        return "NOT_DISCLOSED"
    if source in {"CONFLICTING"} or confidence in {"CONFLICTING"}:
        return "CONFLICTING"
    return "NOT_FOUND"


async def _extract_geo_payload(
    ticker: str,
    region: str,
    filing_type: str,
    period: str,
) -> dict:
    raw = await get_filing_data(
        ticker=ticker,
        fact_type=FilingFactType.geographic_revenue,
        region=region,
        filing_type=filing_type,
        period=period,
    )
    payload = _safe_json_loads(raw)
    warnings = payload.get("warnings")
    if not isinstance(warnings, list):
        warnings = []
    if payload.get("value") is not None and payload.get("denominator") is None:
        payload["valueRatio"] = None
        payload["valuePct"] = None
        if not any(isinstance(w, dict) and w.get("code") == "DENOMINATOR_NOT_FOUND" for w in warnings):
            warnings.append({
                "code": "DENOMINATOR_NOT_FOUND",
                "message": "Could not compute geographic revenue percentage due to missing denominator.",
                "severity": "warning",
            })
    payload["warnings"] = warnings
    return payload


async def _may_be_20f_filer(ticker: str) -> bool:
    _, submissions = await _get_submissions_for_ticker(ticker)
    if not isinstance(submissions, dict):
        return False
    forms = (((submissions.get("filings") or {}).get("recent") or {}).get("form") or [])
    return any(str(form or "").upper() == "20-F" for form in forms)


@yfinance_server.tool(
    name="extract_geographic_revenue",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_geographic_revenue"],
    description="Extract geographic revenue exposure from official SEC data and indexed filing tables, returning explicit parser/provider limitation statuses when no decision-grade value is available.",
)
async def extract_geographic_revenue(
    ticker: str,
    region: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
    detailLevel: str = "compact",
) -> str:
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

    payload = await _extract_geo_payload(ticker, region, filing_type, period)
    idx_payload = _safe_json_loads(await get_sec_filing_index(ticker, filing_type, period, accession_number))
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    shaped = {
        "ticker": ticker,
        "factType": "geographic_revenue",
        "region": region,
        "period": payload.get("period"),
        "rawValue": payload.get("rawValue"),
        "rawDenominator": payload.get("rawDenominator"),
        "unit": payload.get("unit", "USD"),
        "unitScale": payload.get("unitScale", "unknown"),
        "value": payload.get("value"),
        "denominator": payload.get("denominator"),
        "valueRatio": payload.get("valueRatio"),
        "valuePct": payload.get("valuePct"),
        "extractionMethod": payload.get("extractionMethod", "NONE"),
        "confidence": payload.get("confidence", "NOT_DISCLOSED"),
        "evidence": {
            "filingType": idx_payload.get("filingType") or payload.get("filingType") or filing_type,
            "filingDate": idx_payload.get("filingDate") or payload.get("filingDate"),
            "acceptedAt": idx_payload.get("acceptedAt"),
            "accessionNumber": idx_payload.get("accessionNumber") or payload.get("accessionNumber"),
            "documentUrl": idx_payload.get("documentUrl") or payload.get("documentUrl"),
            "sectionHeading": evidence.get("sectionHeading"),
            "tableTitle": evidence.get("tableTitle"),
            "sourceTableId": evidence.get("sourceTableId"),
            "sourceRows": evidence.get("sourceRows") if isinstance(evidence.get("sourceRows"), list) else [],
            "sourceColumns": evidence.get("sourceColumns") if isinstance(evidence.get("sourceColumns"), list) else [],
        },
        "calculation": payload.get("calculation"),
        "warnings": payload.get("warnings") if isinstance(payload.get("warnings"), list) else [],
    }
    if shaped["denominator"] is None:
        shaped["valueRatio"] = None
        shaped["valuePct"] = None
    if (
        str(filing_type or "").upper() == "10-K"
        and str(shaped.get("confidence") or "").upper() == "NOT_DISCLOSED"
    ):
        evidence_filing_type = str((shaped.get("evidence") or {}).get("filingType") or "").upper()
        maybe_20f = evidence_filing_type == "20-F"
        if not maybe_20f and evidence_filing_type in ("", "10-K"):
            maybe_20f = await _may_be_20f_filer(ticker)
        if maybe_20f:
            # Automatic 20-F fallback: retry extraction with 20-F filing type
            fallback_payload = await _extract_geo_payload(ticker, region, "20-F", period)
            fallback_idx = _safe_json_loads(await get_sec_filing_index(ticker, "20-F", period, accession_number))
            if fallback_payload.get("value") is not None:
                # 20-F extraction succeeded — replace shaped with fallback data
                fallback_evidence = fallback_payload.get("evidence") if isinstance(fallback_payload.get("evidence"), dict) else {}
                shaped = {
                    "ticker": ticker,
                    "factType": "geographic_revenue",
                    "region": region,
                    "period": fallback_payload.get("period"),
                    "rawValue": fallback_payload.get("rawValue"),
                    "rawDenominator": fallback_payload.get("rawDenominator"),
                    "unit": fallback_payload.get("unit", "USD"),
                    "unitScale": fallback_payload.get("unitScale", "unknown"),
                    "value": fallback_payload.get("value"),
                    "denominator": fallback_payload.get("denominator"),
                    "valueRatio": fallback_payload.get("valueRatio"),
                    "valuePct": fallback_payload.get("valuePct"),
                    "extractionMethod": fallback_payload.get("extractionMethod", "NONE"),
                    "confidence": fallback_payload.get("confidence", "HIGH"),
                    "evidence": {
                        "filingType": fallback_idx.get("filingType") or fallback_payload.get("filingType") or "20-F",
                        "filingDate": fallback_idx.get("filingDate") or fallback_payload.get("filingDate"),
                        "acceptedAt": fallback_idx.get("acceptedAt"),
                        "accessionNumber": fallback_idx.get("accessionNumber") or fallback_payload.get("accessionNumber"),
                        "documentUrl": fallback_idx.get("documentUrl") or fallback_payload.get("documentUrl"),
                        "sectionHeading": fallback_evidence.get("sectionHeading"),
                        "tableTitle": fallback_evidence.get("tableTitle"),
                        "sourceTableId": fallback_evidence.get("sourceTableId"),
                        "sourceRows": fallback_evidence.get("sourceRows") if isinstance(fallback_evidence.get("sourceRows"), list) else [],
                        "sourceColumns": fallback_evidence.get("sourceColumns") if isinstance(fallback_evidence.get("sourceColumns"), list) else [],
                    },
                    "calculation": fallback_payload.get("calculation"),
                    "warnings": fallback_payload.get("warnings") if isinstance(fallback_payload.get("warnings"), list) else [],
                }
                if shaped["denominator"] is None:
                    shaped["valueRatio"] = None
                    shaped["valuePct"] = None
                # Append advisory warning noting automatic 20-F selection
                shaped_warnings = shaped.get("warnings")
                if not isinstance(shaped_warnings, list):
                    shaped_warnings = []
                shaped_warnings.append({
                    "code": "AUTO_20F_FALLBACK",
                    "message": "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
                    "severity": "info",
                })
                shaped["warnings"] = shaped_warnings
            else:
                # 20-F extraction also failed — keep original shaped and add advisory warning
                warnings = shaped.get("warnings")
                if not isinstance(warnings, list):
                    warnings = []
                if not any(isinstance(w, dict) and w.get("code") == "POSSIBLE_20F_FILER" for w in warnings):
                    warnings.append({
                        "code": "POSSIBLE_20F_FILER",
                        "message": "POSSIBLE_20F_FILER: Ticker may file 20-F. Retry with filing_type='20-F' or use IR web search.",
                        "severity": "warning",
                    })
                shaped["warnings"] = warnings
    if str(detailLevel).lower() == "raw":
        shaped["rawContext"] = {"filingIndex": idx_payload}
    return json.dumps(shaped)


@yfinance_server.tool(name="extract_segment_revenue", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_segment_revenue"], description="Extract segment revenue rows from official SEC facts and filing tables, returning explicit limitation statuses when no parseable segment data is found.")
async def extract_segment_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.segment_revenue, filing_type=filing_type, period=period))
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
        maybe_20f = await _may_be_20f_filer(ticker)
        if maybe_20f:
            fb_payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.segment_revenue, filing_type="20-F", period=period))
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
    if warnings:
        out["warnings"] = warnings
    if str(detailLevel).lower() == "raw":
        out["rawContext"] = payload
    return json.dumps(out)


@yfinance_server.tool(name="extract_total_revenue", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_total_revenue"], description="Extract total revenue from official SEC facts or filing tables with evidence metadata.")
async def extract_total_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
) -> str:
    payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.total_revenue, filing_type=filing_type, period=period))
    val = payload.get("value")
    return json.dumps({
        "ticker": ticker,
        "factType": "total_revenue",
        "value": val,
        "period": payload.get("period"),
        "confidence": payload.get("confidence", "NOT_DISCLOSED" if val is None else "HIGH"),
        "evidence": {
            "filingType": payload.get("filingType", filing_type),
            "filingDate": payload.get("filingDate"),
            "accessionNumber": payload.get("accessionNumber"),
            "documentUrl": payload.get("documentUrl"),
        },
        "status": "FOUND" if val is not None else _as_status(payload),
    })


@yfinance_server.tool(name="extract_revenue_exposure", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_revenue_exposure"], description="Extract revenue exposure for a region/customer/segment query, returning explicit parser/provider limitation statuses when no decision-grade value is available.")
async def extract_revenue_exposure(
    ticker: str,
    exposure_query: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    geo = _safe_json_loads(await extract_geographic_revenue(ticker=ticker, region=exposure_query, filing_type=filing_type, period=period, detailLevel=detailLevel))
    found = geo.get("value") is not None
    status = "FOUND_REVENUE_EXPOSURE" if found else _as_status(geo)
    matches = []
    if found:
        matches.append({
            "exposureType": "geographic_revenue",
            "label": exposure_query,
            "value": geo.get("value"),
            "denominator": geo.get("denominator"),
            "valueRatio": geo.get("valueRatio"),
            "valuePct": geo.get("valuePct"),
            "period": geo.get("period"),
            "confidence": geo.get("confidence", "HIGH"),
            "evidence": geo.get("evidence", {}),
        })
    return json.dumps({"ticker": ticker, "query": exposure_query, "matches": matches, "status": status})


@yfinance_server.tool(name="extract_risk_factor_mentions", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_risk_factor_mentions"], description="Extract concise risk-factor mentions for explicit terms from a filing.")
async def extract_risk_factor_mentions(
    ticker: str,
    terms: list[str],
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    matches: list[dict] = []
    for term in (terms or []):
        search = _safe_json_loads(await search_sec_filing_text(
            ticker=ticker,
            search_terms=[str(term)],
            section_hint="Risk Factors",
            filing_type=filing_type,
        ))
        for m in (search.get("matches") if isinstance(search.get("matches"), list) else [])[:3]:
            if not isinstance(m, dict):
                continue
            excerpt = _compact_excerpt(str(m.get("context") or m.get("excerpt") or ""))
            matches.append({
                "term": term,
                "sectionHeading": m.get("sectionHeading") or "Risk Factors",
                "excerpt": excerpt,
                "excerptAvailable": bool(excerpt),
                "confidence": "MEDIUM",
                "evidence": {
                    "filingDate": search.get("filingDate"),
                    "accessionNumber": search.get("accessionNumber"),
                    "documentUrl": search.get("documentUrl"),
                },
            })
    result = {"ticker": ticker, "matches": matches, "status": "FOUND" if matches else "NOT_FOUND"}
    if str(detailLevel).lower() == "raw":
        result["rawTerms"] = terms or []
    return json.dumps(result)


@yfinance_server.tool(name="extract_customer_concentration", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_customer_concentration"], description="Extract customer concentration percentages from SEC filing text evidence.")
async def extract_customer_concentration(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    search = _safe_json_loads(await search_sec_filing_text(
        ticker=ticker,
        search_terms=["major customer", "customers", "customer accounted", "percent of revenue"],
        filing_type=filing_type,
    ))
    customers: list[dict] = []
    seen: set[str] = set()
    for m in (search.get("matches") if isinstance(search.get("matches"), list) else []):
        if not isinstance(m, dict):
            continue
        ctx = str(m.get("context") or "")
        pct_match = _re.search(r"(\d{1,2}(?:\.\d+)?)\s*%", ctx)
        if not pct_match:
            continue
        pct = float(pct_match.group(1))
        key = f"{pct:.2f}"
        if key in seen:
            continue
        seen.add(key)
        customers.append({
            "label": f"Customer {chr(64 + len(customers) + 1)}",
            "valuePct": pct,
            "period": f"FY{str(search.get('fiscalYear') or '')}".rstrip(),
            "confidence": "HIGH",
            "evidence": {
                "sectionHeading": m.get("sectionHeading"),
                "excerpt": _compact_excerpt(ctx),
                "filingDate": search.get("filingDate"),
                "accessionNumber": search.get("accessionNumber"),
                "documentUrl": search.get("documentUrl"),
            },
        })
        if len(customers) >= 5:
            break
    status = "FOUND" if customers else ("NOT_DISCLOSED" if (search.get("matchCount") or 0) > 0 else "NOT_FOUND")
    result = {"ticker": ticker, "customers": customers, "status": status}
    if str(detailLevel).lower() == "raw":
        result["rawMatchCount"] = search.get("matchCount", 0)
    return json.dumps(result)


@yfinance_server.tool(name="extract_china_exposure", output_schema=_TOOL_OUTPUT_SCHEMAS["extract_china_exposure"], description="Extract China exposure with separate revenue and non-revenue classifications; revenue values are decision-grade only when evidence and status support them.")
async def extract_china_exposure(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
    detailLevel: str = "compact",
) -> str:
    idx = _safe_json_loads(await get_sec_filing_index(ticker=ticker, filing_type=filing_type, period=period, accession_number=accession_number))
    revenue = _safe_json_loads(await extract_revenue_exposure(ticker=ticker, exposure_query="China", filing_type=filing_type, period=period))
    revenue_status = "FOUND" if revenue.get("status") == "FOUND_REVENUE_EXPOSURE" else revenue.get("status", "NOT_FOUND")

    index = idx.get("index") if isinstance(idx.get("index"), dict) else {}
    sections = index.get("sections") if isinstance(index.get("sections"), list) else []
    tables = index.get("tables") if isinstance(index.get("tables"), list) else []

    def _collect(term_list: list[str]) -> list[dict]:
        found = []
        for sec in sections:
            if not isinstance(sec, dict):
                continue
            heading = str(sec.get("heading") or "")
            low = heading.lower()
            for term in term_list:
                if term.lower() in low:
                    excerpt = _compact_excerpt(heading)
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
                    excerpt = _compact_excerpt(hay_source)
                    found.append({
                        "source": "table",
                        "term": term,
                        "tableTitle": tbl.get("title"),
                        "sourceTableId": tbl.get("tableId"),
                        "sectionId": tbl.get("sectionId"),
                        "excerpt": excerpt,
                        "excerptAvailable": bool(excerpt),
                    })
        return found

    entity_terms = ["Tongmei", "JinMei", "BoYu"]
    bank_terms = ["Bank of China"]
    manuf_terms = ["manufacturing", "production", "supply chain", "fab"]
    risk_terms = ["China", "tariff", "export control"]

    entity_evidence = _collect(entity_terms)
    bank_evidence = _collect(bank_terms)
    manu_evidence = _collect(manuf_terms)
    risk_mentions = _safe_json_loads(await extract_risk_factor_mentions(ticker=ticker, terms=risk_terms, filing_type=filing_type, period=period))
    risk_evidence = risk_mentions.get("matches") if isinstance(risk_mentions.get("matches"), list) else []
    for ev in risk_evidence:
        if isinstance(ev, dict):
            excerpt = _compact_excerpt(str(ev.get("excerpt") or ev.get("context") or ""))
            if excerpt:
                ev["excerpt"] = excerpt
                ev["excerptAvailable"] = True
            else:
                ev.pop("excerpt", None)
                ev["excerptAvailable"] = False

    non_revenue_found = bool(entity_evidence or bank_evidence or manu_evidence or risk_evidence)
    if revenue.get("status") == "FOUND_REVENUE_EXPOSURE":
        overall = "FOUND_REVENUE_EXPOSURE"
    elif non_revenue_found:
        overall = "FOUND_NON_REVENUE_EXPOSURE"
    elif revenue.get("status") == "NOT_DISCLOSED":
        overall = "NOT_DISCLOSED"
    elif revenue.get("status") == "CONFLICTING":
        overall = "CONFLICTING"
    else:
        overall = "NOT_FOUND"

    out = {
        "ticker": ticker,
        "exposureType": "china_exposure",
        "filingType": idx.get("filingType", filing_type),
        "filingDate": idx.get("filingDate"),
        "accessionNumber": idx.get("accessionNumber"),
        "documentUrl": idx.get("documentUrl"),
        "revenueExposure": {
            "status": revenue_status,
            "value": revenue.get("matches", [{}])[0].get("value") if revenue.get("matches") else None,
            "denominator": revenue.get("matches", [{}])[0].get("denominator") if revenue.get("matches") else None,
            "valueRatio": revenue.get("matches", [{}])[0].get("valueRatio") if revenue.get("matches") else None,
            "valuePct": revenue.get("matches", [{}])[0].get("valuePct") if revenue.get("matches") else None,
            "confidence": "HIGH" if revenue_status == "FOUND" else ("NOT_DISCLOSED" if revenue_status == "NOT_DISCLOSED" else "LOW"),
            "evidence": revenue.get("matches", [{}])[0].get("evidence") if revenue.get("matches") else [],
        },
        "manufacturingExposure": {"status": "FOUND" if manu_evidence else "NOT_FOUND", "confidence": "MEDIUM", "evidence": manu_evidence},
        "entityExposure": {"status": "FOUND" if entity_evidence else "NOT_FOUND", "entities": entity_terms if entity_evidence else [], "confidence": "MEDIUM", "evidence": entity_evidence},
        "bankExposure": {"status": "FOUND" if bank_evidence else "NOT_FOUND", "entities": bank_terms if bank_evidence else [], "confidence": "MEDIUM", "evidence": bank_evidence},
        "riskFactorExposure": {"status": "FOUND" if risk_evidence else "NOT_FOUND", "confidence": "MEDIUM", "evidence": risk_evidence},
        "overallStatus": overall,
        "warnings": [],
    }
    if str(detailLevel).lower() == "raw":
        out["rawContext"] = {"filingIndex": idx}
    return json.dumps(out)


@yfinance_server.tool(
    name="extract_exposure",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_exposure"],
    description="Extract multi-dimensional SEC exposure for a geographic region or named entity/topic. Returns revenue, operational, named-entity, and risk evidence with explicit non-decision-grade statuses when parser/provider limits prevent a value.",
)
async def extract_exposure(
    ticker: str,
    topic: str,
    filing_type: str = "10-K",
    period: str = "latest",
    include_risk_factors: bool = True,
) -> str:
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
    CHINA_NAMED_ENTITIES = ["foxconn", "tsmc", "luxshare", "catl", "byd", "tongmei", "catcher", "pegatron"]
    OPERATIONAL_TERMS = ["manufacturing", "assembly", "supply chain", "factory", "production facility"]

    is_china = topic_lower in ("china", "greater china")
    synonym_entry = EXPOSURE_SYNONYMS.get(topic_lower)
    region_label = "Greater China" if topic_lower == "china" else topic

    warnings: list[dict] = []

    # Get filing index for metadata
    idx = _safe_json_loads(await get_sec_filing_index(ticker=ticker, filing_type=filing_type, period=period))

    filing_date = idx.get("filingDate")
    accession_number = idx.get("accessionNumber")
    document_url = idx.get("documentUrl")

    # Revenue extraction
    try:
        geo = _safe_json_loads(await extract_geographic_revenue(ticker=ticker, region=region_label, filing_type=filing_type, period=period, detailLevel="compact"))
    except Exception as e:
        warnings.append({"code": "REVENUE_EXTRACTION_ERROR", "message": str(e), "severity": "warning"})
        geo = {}

    geo_value = geo.get("value")
    geo_denominator = geo.get("denominator")
    geo_conf = str(geo.get("confidence") or "LOW").upper()
    geo_method = str(geo.get("extractionMethod") or "NONE").upper()
    geo_evidence = geo.get("evidence") or {}
    if isinstance(geo_evidence, dict):
        pass
    else:
        geo_evidence = {}

    if geo_value is not None:
        rev_status = "FOUND"
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
        "confidence": "LOW" if geo_conf == "NOT_DISCLOSED" else (geo_conf if geo_value is not None else "LOW"),
        "evidence": {
            "sectionHeading": geo_evidence.get("sectionHeading"),
            "sourceRows": geo_evidence.get("sourceRows") if isinstance(geo_evidence.get("sourceRows"), list) else [],
            "sourceColumns": geo_evidence.get("sourceColumns") if isinstance(geo_evidence.get("sourceColumns"), list) else [],
        },
    }

    # Operational scan via existing search
    try:
        ops_raw = _safe_json_loads(await search_sec_filing_text(ticker=ticker, search_terms=[topic_lower], filing_type=filing_type, context_chars=600, return_tables=False))
        ops_matches = ops_raw.get("matches") or []
    except Exception:
        ops_matches = []

    ops_evidence: list[dict] = []
    found_op_terms: set[str] = set()
    for m in ops_matches:
        if not isinstance(m, dict):
            continue
        context_text = str(m.get("contextText") or m.get("context") or "").lower()
        for op_term in OPERATIONAL_TERMS:
            if op_term in context_text:
                found_op_terms.add(op_term)
                if len(ops_evidence) < 5:
                    ops_evidence.append({
                        "term": op_term,
                        "excerpt": _compact_excerpt(str(m.get("contextText") or m.get("context") or ""), 200),
                        "section": str(m.get("sectionHeading") or ""),
                    })
                break
        if len(ops_evidence) >= 5:
            break

    operational_exposure = {
        "status": "FOUND" if ops_evidence else "NOT_FOUND",
        "terms": list(found_op_terms),
        "evidence": ops_evidence,
    }

    # Entity scan (China only)
    if is_china:
        try:
            ent_raw = _safe_json_loads(await search_sec_filing_text(ticker=ticker, search_terms=CHINA_NAMED_ENTITIES[:3], filing_type=filing_type, context_chars=400, return_tables=False))
            ent_matches = ent_raw.get("matches") or []
        except Exception:
            ent_matches = []
        found_entities: set[str] = set()
        ent_evidence: list[dict] = []
        for m in ent_matches:
            if not isinstance(m, dict):
                continue
            term_low = str(m.get("term") or "").lower()
            if term_low and term_low not in found_entities:
                found_entities.add(term_low)
                if len(ent_evidence) < 5:
                    ent_evidence.append({
                        "entity": str(m.get("term") or ""),
                        "excerpt": _compact_excerpt(str(m.get("contextText") or m.get("context") or ""), 200),
                        "section": str(m.get("sectionHeading") or ""),
                    })
            if len(ent_evidence) >= 5:
                break
        entity_exposure = {
            "status": "FOUND" if ent_evidence else "NOT_FOUND",
            "entities": list(found_entities),
            "evidence": ent_evidence,
        }
    else:
        entity_exposure = {"status": "NOT_FOUND", "entities": [], "evidence": []}

    # Risk factor scan
    if include_risk_factors:
        try:
            risk_raw = _safe_json_loads(await extract_risk_factor_mentions(ticker=ticker, terms=[topic_lower], filing_type=filing_type, period=period, detailLevel="compact"))
            risk_matches = risk_raw.get("matches") or []
        except Exception:
            risk_matches = []
        risk_evidence = [
            {
                "excerpt": _compact_excerpt(str(m.get("excerpt") or m.get("context") or m.get("contextText") or ""), 200),
                "section": str(m.get("sectionHeading") or "Risk Factors"),
            }
            for m in risk_matches[:5] if isinstance(m, dict)
        ]
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
        "warnings": warnings,
    })


@yfinance_server.tool(
    name="query_sec_filing_index",
    output_schema=_TOOL_OUTPUT_SCHEMAS["query_sec_filing_index"],
    description="Deterministically route supported SEC filing index query types to extractor tools.",
)
async def query_sec_filing_index(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
    query_type: Literal[
        "geographic_revenue_share",
        "revenue_exposure",
        "china_exposure",
        "risk_factor_mentions",
        "customer_concentration",
        "total_revenue",
        "segment_revenue",
    ] = "geographic_revenue_share",
    params: dict | None = None,
    return_evidence: bool = True,
    detailLevel: str = "compact",
) -> str:
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
        confidence = str(geo.get("confidence") or ("HIGH" if status == "ANSWERED" else "NOT_DISCLOSED"))
        if status == "NOT_FOUND":
            confidence = "NOT_DISCLOSED" if str(geo.get("confidence") or "").upper() == "NOT_DISCLOSED" else "LOW"
        return _result(status, answer, confidence, evidence, warnings)

    if query == "revenue_exposure":
        exposure_query = str(routed_params.get("exposure_query") or "").strip()
        if not exposure_query:
            return _missing_param("exposure_query")
        rex = _safe_json_loads(await extract_revenue_exposure(
            ticker=ticker,
            exposure_query=exposure_query,
            filing_type=filing_type,
            period=period,
            detailLevel=detail,
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
        confidence = str(first.get("confidence") or ("HIGH" if status == "ANSWERED" else "NOT_DISCLOSED"))
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

    if query == "risk_factor_mentions":
        terms = routed_params.get("terms")
        terms_list = [str(t) for t in terms] if isinstance(terms, list) else []
        if not terms_list:
            return _missing_param("terms")
        risk = _safe_json_loads(await extract_risk_factor_mentions(
            ticker=ticker,
            terms=terms_list,
            filing_type=filing_type,
            period=period,
            detailLevel=detail,
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
        cust = _safe_json_loads(await extract_customer_concentration(
            ticker=ticker,
            filing_type=filing_type,
            period=period,
            detailLevel=detail,
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
        total = _safe_json_loads(await extract_total_revenue(
            ticker=ticker,
            filing_type=filing_type,
            period=period,
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
            str(total.get("confidence") or ("HIGH" if status == "ANSWERED" else "NOT_DISCLOSED")),
            evidence,
            warnings,
        )

    segment_name = str(routed_params.get("segment") or "").strip()
    seg = _safe_json_loads(await extract_segment_revenue(
        ticker=ticker,
        filing_type=filing_type,
        period=period,
        detailLevel=detail,
    ))
    segments = [s for s in (seg.get("segments") if isinstance(seg.get("segments"), list) else []) if isinstance(s, dict)]
    if segment_name:
        segments = [s for s in segments if str(s.get("label") or "").lower() == segment_name.lower()]
    evidence = [_shape_evidence(s.get("evidence")) for s in segments if isinstance(s.get("evidence"), dict)]
    status = "ANSWERED" if segments else ("NOT_FOUND" if segment_name else str(seg.get("status") or "NOT_FOUND"))
    if status == "ANSWERED" and not evidence:
        status = "NOT_FOUND"
        warnings.append({"code": "EVIDENCE_REQUIRED", "message": "ANSWERED responses require evidence."})
    return _result(
        status,
        {"segment": segment_name or None, "segments": segments},
        "HIGH" if status == "ANSWERED" else ("NOT_DISCLOSED" if status == "NOT_DISCLOSED" else "LOW"),
        evidence,
        warnings,
    )


# ---------------------------------------------------------------------------
# Earnings/transcript domain tools - imported for registration and re-exported
# ---------------------------------------------------------------------------
import yfmcp.tools.earnings  # noqa: F401 (side-effect import)
from yfmcp.tools.earnings import (  # re-export for compatibility and grouped routing
    _derive_fiscal_period_from_date,
    _is_paywalled_url,
    _classify_earnings_source_url,
    _fetch_public_html,
    _scale_number_from_text,
    _first_sentence_for_topic,
    _extract_metric_number,
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
# Grouped (token-efficient) server mode
# ---------------------------------------------------------------------------
# TOOL_MODE env var controls which interface is exposed:
#   - "expanded" (default): all 111 individual tools (backward-compatible)
#   - "grouped": 11 domain meta-tools with action routing (~80-85% token savings)
# ---------------------------------------------------------------------------
_TOOL_MODE = os.environ.get("TOOL_MODE", "expanded").lower().strip()


def _build_grouped_server():
    """Create a FastMCP server with grouped meta-tools for token efficiency."""
    from tool_groups import TOOL_GROUPS, register_grouped_tools

    grouped = FastMCP(
        "yfinance",
        instructions="""
# Yahoo Finance MCP Server (Grouped Mode)

Activated via TOOL_MODE=grouped env var (default is "expanded" with 111 individual tools).

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

    # Resolve handlers from the shared FastMCP instance (single source of truth)
    # rather than this module's globals, so the mapping holds as handlers move
    # into yfmcp.tools.* during the Phase 2 split.
    handler_registry = build_handler_registry(yfinance_server)
    register_grouped_tools(grouped, handler_registry)
    return grouped


# Lazily built grouped server (only constructed if TOOL_MODE=grouped)
_grouped_server = None


def get_server():
    """Return the appropriate server based on TOOL_MODE env var.

    - TOOL_MODE=expanded (default): 111 individual tools
    - TOOL_MODE=grouped: 11 domain meta-tools (~80-85% token savings)
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
    print(f"Starting Yahoo Finance MCP server (mode={mode_label})...")
    server.run(transport="stdio")
