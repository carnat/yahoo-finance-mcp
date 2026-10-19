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

# Moved to yfmcp/news/ in 2.15.1 (pure restructure). Every moved name is re-imported here so
# `yfmcp.tools.news.<name>` and `server.<name>` keep resolving; providers never import this module.

from yfmcp.news.shared import (
    _DEDUP_TITLE_MAX_LEN,
    _event_type_from_keywords,
    _make_duplicate_group_id,
    _normalize_exchange,
    _ticker_in_text,
    _normalized_event_title_stem,
    _short_text,
    _within_date_window,
)
from yfmcp.news.providers.yahoo import (
    _PRESS_RELEASE_WIRE_RE,
    _YAHOO_ALLOWED_CONTENT_TYPES,
    _YAHOO_NEWS_ACRONYM_IGNORED_WORDS,
    _YAHOO_NEWS_LEGAL_SUFFIXES,
    _YAHOO_PRESS_RELEASE_METHOD,
    _build_yahoo_event_item,
    _collect_yahoo_events,
    _is_ticker_compatible_with_context,
    _normalize_event_text,
    _strip_yahoo_news_legal_suffix,
    _yahoo_decision_use,
    _yahoo_item_issuer,
    _yahoo_news_identity_from_info,
    _yahoo_news_match_for,
    _yahoo_search_news,
)
from yfmcp.news.providers.sec import (
    _FORM4_TRANSACTION_LABELS,
    _build_sec_event_item,
    _collect_sec_events,
    _event_type_from_form,
    _form4_date,
    _form4_num,
    _form4_owner_from_html,
    _form4_role_from_html,
    _form4_transaction_payload,
    _parse_form4_html_transaction,
    _parse_form4_transaction,
    _safe_sec_url,
    _strip_xml_namespaces,
    _try_attach_form4_transaction,
    _xml_text,
)
from yfmcp.news.providers.company_ir_page import (
    _COMPANY_IR_PAGE_MAX_BYTES,
    _COMPANY_IR_PAGE_REGISTRY_PATH,
    _collect_company_ir_page_events,
    _company_ir_page_entry,
    _company_ir_page_item,
    _company_ir_page_json_records,
    _company_ir_page_registry_fresh,
    _company_ir_page_url_allowed,
    _fetch_company_ir_page,
    _host_without_www,
    _html_attr,
    _load_company_ir_page_registry,
    _parse_company_ir_page_html,
    _parse_company_ir_page_json,
)
from yfmcp.news.providers.company_ir import _collect_company_ir_events
from yfmcp.news.providers.globenewswire import (
    _GLOBENEWSWIRE_BLOCKED_XML_MARKERS,
    _GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN,
    _GLOBENEWSWIRE_MAX_BYTES,
    _GLOBENEWSWIRE_RSS_FEEDS,
    _GLOBENEWSWIRE_RSS_URL,
    _GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN,
    _collect_globenewswire_events,
    _globenewswire_category_values,
    _globenewswire_plain_text,
    _globenewswire_stock_category_matches,
    _globenewswire_xml_is_safe,
    _xml_child_texts,
    _xml_first_child_text,
    _xml_local_name,
)
from yfmcp.news.providers.finnhub import (
    _FINNHUB_NEWS_API,
    _collect_finnhub_events,
)
from yfmcp.news.providers.marketaux import (
    _MARKETAUX_MAX_ITEMS,
    _MARKETAUX_NEWS_API,
    _MARKETAUX_WIRE_DOMAINS,
    _collect_marketaux_events,
)

# ---------------------------------------------------------------------------
# Phase 6B public event helpers (multi-source, source-backed, deduped)
# ---------------------------------------------------------------------------

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


# The bundled policy's reasonCode, used when the policy file cannot be read: the Worker compiles the policy in, so an unreadable
# file there cannot happen and Python fails closed with the same code (R-news Batch C, decision 14).
_FINNHUB_UNREADABLE_POLICY_REASON_CODE = "MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT"


def _finnhub_eligibility(ticker: str) -> tuple[bool, str | None]:
    try:
        policy = _finnhub_policy()
    except (OSError, KeyError, TypeError, ValueError):
        return False, _FINNHUB_UNREADABLE_POLICY_REASON_CODE
    ticker_u = ticker.upper()
    ineligible = ticker_u in set(policy.get("ineligibleTickers") or []) or any(
        ticker_u.endswith(str(suffix).upper()) for suffix in policy.get("ineligibleTickerSuffixes") or []
    )
    return (not ineligible, str(policy.get("reasonCode") or "") if ineligible else None)


def _confidence_for_source_type(source_type: object, fallback: object = "LOW") -> str:
    """Confidence by source type (the Worker's confidenceForSourceType): official sources HIGH, wires MEDIUM, contextual news LOW;
    any other type keeps the item's own confidence."""
    source = source_type if isinstance(source_type, str) else ""
    if source in ("sec_filing", "company_ir", "company_ir_page"):
        return "HIGH"
    if source in ("press_release", "yahoo_finance_press_releases", "newswire"):
        return "MEDIUM"
    if source in ("company_news", "yahoo_finance_news", "yahoo_finance", "finnhub"):
        return "LOW"
    return fallback if isinstance(fallback, str) else "LOW"


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
    # A URL that does not parse as an absolute URL is UNKNOWN (the Worker's `new URL()` throws), 2.17.0.
    try:
        parsed = _urlparse.urlparse(url)
        host = parsed.hostname or ""
    except ValueError:
        return "UNKNOWN"
    if not parsed.scheme or (parsed.scheme.lower() in {"http", "https", "ftp", "ws", "wss"} and not host):
        return "UNKNOWN"
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


def _build_coverage(
    source_status: dict,
    resolved_action: str | None = None,
    caller_limit: int | None = None,
    returned_count: int | None = None,
) -> dict:
    """``caller_limit`` is the caller's effective max_results (after clamping) and ``returned_count`` the number of items
    the tool returns. When truncatedSources is non-empty and the output is below that limit, the accepted evidence was cut
    by the pool cap or dedupe, not by the caller, so coverage.state becomes PARTIAL (R-news Batch C, decisions 10 and 18).
    A full output keeps the state _compute_source_coverage gives. Both are parameters, never emitted fields."""
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
    cut_below_caller_limit = (
        caller_limit is not None and returned_count is not None and bool(truncated_sources) and returned_count < caller_limit
    )
    return {
        "state": "PARTIAL" if cut_below_caller_limit else _compute_source_coverage(source_status),
        "failedSources": failed_sources,
        "skippedSources": skipped_sources,
        "truncatedSources": truncated_sources,
        "recommendedNextAction": action,
    }














_NEWSWIRE_HINTS = ("businesswire", "globenewswire", "prnewswire")
_COMPANY_IR_URL_MARKERS = ("investor.", "investors.", "/investor", "/news-releases", "/press-release")


_NEWS_SOURCE_CAPABILITIES_PATH = Path(__file__).resolve().parents[2] / "worker" / "src" / "news-source-capabilities.json"
_NEWS_SOURCE_REGISTRY_PATH = Path(__file__).resolve().parents[2] / "worker" / "src" / "news-source-registry.json"
















def _coerce_max_results(value: int, default_value: int) -> int:
    return min(max(1, int(default_value if value is None else value)), 100)


def _coerce_lookback_days(value: int, default_value: int) -> int:
    return min(max(1, int(default_value if value is None else value)), 3650)


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
# 2.16.0: the news defaults come from the shared source registry (news-source-registry.json), which the Worker reads too.
_DEFAULT_NEWS_SOURCES = list(json.loads(_NEWS_SOURCE_REGISTRY_PATH.read_text(encoding="utf-8"))["defaultSources"])
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


def _disabled_news_sources() -> set[str]:
    """Operator off switch (2.16.0): NEWS_SOURCES_DISABLED is a comma-separated list of source names, each trimmed
    and lower-cased; unknown names are ignored and an empty or unset value disables nothing."""
    raw = os.environ.get("NEWS_SOURCES_DISABLED") or ""
    return {name for name in (part.strip().lower() for part in raw.split(",")) if name in _PHASE6B_SUPPORTED_SOURCES}


def _split_disabled_sources(selected: list[str]) -> tuple[list[str], list[str]]:
    """Selected sources split into (active, disabled) by the operator switch; selection order kept."""
    off = _disabled_news_sources()
    return [src for src in selected if src not in off], [src for src in selected if src in off]


def _disabled_source_warnings(disabled: list[str]) -> list[dict]:
    """One info warning per disabled source that was selected (existing SOURCE_UNAVAILABLE shape)."""
    return [
        {
            "code": "SOURCE_UNAVAILABLE",
            "message": f"Source {source} is disabled by the server operator.",
            "severity": "info",
            "source": source,
        }
        for source in disabled
    ]


def _mark_disabled_sources(source_status: dict, disabled: list[str]) -> dict:
    """A disabled source's row always replaces whatever message inference produced (RNEWS2 8.2/8.3)."""
    for source in disabled:
        source_status[source] = {"status": "UNCONFIGURED", "rawCount": 0, "filteredCount": 0, "attempted": False}
    return source_status


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


































def _source_rank(source_type: object) -> int:
    return _SOURCE_PRIORITY.get(str(source_type or "other"), _SOURCE_PRIORITY["other"])




















































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
    disabled_sources: list[str] | None = None,
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
        list(_DEFAULT_NEWS_SOURCES)
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
        sec_diagnostic = diagnostics.get("sec") if isinstance(diagnostics.get("sec"), dict) else {}
        if str(sec_diagnostic.get("status") or "") == "NOT_ELIGIBLE":
            result["sec"] = {"status": "NOT_ELIGIBLE", "rawCount": 0, "filteredCount": 0, **sec_diagnostic}
        elif "sec" in sources_used:
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
        # Registry-only since 2.19.0 (Worker 2.10.0): the collector's diagnostics carry the registry state (NOT_REGISTERED
        # without an approved entry) and override the item-count status, as the Worker's spread does.
        company_ir_diagnostic = diagnostics.get("company_ir") if isinstance(diagnostics.get("company_ir"), dict) else {}
        company_ir_status = "EMPTY_RESULT"
        if "company_ir" in sources_used:
            company_ir_status = "OK" if company_ir_items else "EMPTY_RESULT"
        elif _yf_error_status(warning_msgs):
            company_ir_status = "PROVIDER_ERROR"
        result["company_ir"] = {"status": company_ir_status, "rawCount": len(company_ir_items), "filteredCount": len(company_ir_items), **company_ir_diagnostic}
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
        # The registry metadata comes from the entry itself, so a clean approved fetch carries it too (2.19.0, row g).
        page_diagnostic = diagnostics.get("company_ir_page") if isinstance(diagnostics.get("company_ir_page"), dict) else {}
        registry_fields = page_diagnostic if page_diagnostic.get("approvalStatus") not in (None, "not_registered") else page_warning
        # A fetched page answers fetch_configured_source whether or not it had items in the window (2.19.0, row f).
        fetched = status == "APPROVED_SOURCE_OK" or (status == "EMPTY_RESULT" and "company_ir_page" in sources_used)
        result["company_ir_page"] = {
            "status": status,
            "rawCount": len(company_ir_page_items),
            "filteredCount": len(company_ir_page_items),
            "registryVersion": registry_fields.get("registryVersion"),
            "canonicalUrl": registry_fields.get("canonicalUrl"),
            "approvalStatus": registry_fields.get("approvalStatus"),
            "allowedAction": "fetch_configured_source" if fetched else (
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
    if disabled_sources is None:
        disabled_sources = list(getattr(source_diagnostics, "disabled_sources", None) or [])
    return _mark_disabled_sources(_mark_budget_starved_sources(result, budget_starved_sources), disabled_sources)


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
    disabled_sources: list[str]

    def __init__(self) -> None:
        super().__init__()
        self.budget_starved = []
        self.disabled_sources = []


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


def _verify_coverage_failure_mode(source_status: dict, warnings: list[dict], company_ir_requested: bool = False) -> str | None:
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
    # AUTH_ERROR and an unregistered company_ir map to the existing SOURCE_UNCONFIGURED (2.19.0, calls #10 / #14). The company_ir
    # upgrade applies only when the caller named company_ir: a defaults-applied NOT_REGISTERED row stays in sourceStatus /
    # coverage without changing the status (planner decision 7).
    company_ir_row = source_status.get("company_ir")
    company_ir_status = str(company_ir_row.get("status") or "") if company_ir_requested and isinstance(company_ir_row, dict) else ""
    if "UNCONFIGURED" in source_states or "AUTH_ERROR" in source_states or company_ir_status == "NOT_REGISTERED":
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
    requested_selection, warnings = _normalize_event_sources(
        sources,
        ["sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"],
    )
    # A source the operator disabled is never collected (2.16.0); it is reported instead.
    selected_sources, disabled_sources = _split_disabled_sources(requested_selection)
    warnings.extend(_disabled_source_warnings(disabled_sources))
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
    # ``company_ir`` is registry-only (2.19.0): it never reads the Yahoo feed. ``newswire`` is served by the direct
    # GlobeNewswire RSS fetcher below and is likewise excluded here.
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
    sec_result, company_ir_result, page_result, yahoo_result, finnhub_result = await asyncio.gather(
        _watched(["sec"], _collect_sec_events(
            ticker,
            filing_types=sec_filing_types or ["8-K", "10-Q", "10-K", "S-3", "DEF14A"],
            **window,
        )) if "sec" in selected_sources else _skipped(),
        _watched(["company_ir"], _collect_company_ir_events(ticker, **window)) if "company_ir" in selected_sources else _skipped(),
        _watched(["company_ir_page"], _collect_company_ir_page_events(ticker, **window)) if "company_ir_page" in selected_sources else _skipped(),
        _yahoo_feeds(),
        _watched(["finnhub"], _collect_finnhub_events(ticker, **window)) if "finnhub" in selected_sources and finnhub_eligible else _skipped(),
    )

    if sec_result is not None:
        sec_items, sec_warnings, used = sec_result
        if used:
            sources_used.append("sec")
        items.extend(sec_items)
        if getattr(sec_result, "no_cik", False):
            # A ticker SEC has no CIK for is not eligible, as an ineligible Finnhub ticker is (RNEWS2 8.7).
            source_diagnostics["sec"] = {"status": "NOT_ELIGIBLE", "attempted": False, "reasonCode": None}
            warnings.append({
                "code": "SOURCE_NOT_ELIGIBLE",
                "message": f"SEC has no filings for {ticker.upper()} (no SEC CIK).",
                "severity": "warning",
                "source": "sec",
                "attempted": False,
                "reasonCode": None,
            })
        else:
            warnings.extend(sec_warnings)

    if company_ir_result is not None:
        company_ir_items, company_ir_warnings, company_ir_used = company_ir_result
        source_diagnostics["company_ir"] = dict(getattr(company_ir_result, "diagnostics", None) or {})
        if company_ir_used:
            sources_used.append("company_ir")
        items.extend(company_ir_items)
        warnings.extend(company_ir_warnings)

    if page_result is not None:
        page_items, page_warnings, page_used = page_result
        page_diagnostics = getattr(page_result, "diagnostics", None)
        if page_diagnostics:
            source_diagnostics["company_ir_page"] = dict(page_diagnostics)
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
        for item in yf_items:
            src = str(item.get("source") or "")
            if "yahoo_finance_news" in selected_sources and src == "yahoo_finance_news":
                items.append(item)
            elif "yahoo_finance" in selected_sources and src in ("yahoo_finance_news", "yahoo_finance_press_releases"):
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
        # The requested selection, disabled sources included: a disabled finnhub counts as limited coverage (RNEWS2 8.3).
        fallback_reason = _marketaux_fallback_reason(
            requested_selection,
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
        # The stock-category exchange check reads the company exchange; resolved on demand when no Yahoo pass ran (2.17.0).
        gnw_identity = yahoo_identity
        if gnw_identity is None:
            try:
                with _url_policy.budget_watch() as gnw_identity_watch:
                    gnw_info = await asyncio.to_thread(lambda: yf.Ticker(ticker).info)
                gnw_identity = _yahoo_news_identity_from_info(ticker, gnw_info)
                if gnw_identity_watch.starved:
                    budget_starved.add("yahoo_finance_identity")
            except Exception:
                gnw_identity = None
        gnw_items, gnw_warnings, gnw_used = await _watched(["newswire"], _collect_globenewswire_events(
            ticker,
            retrieved_at=retrieved_at,
            max_results=max_cap,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback,
            exchange=(gnw_identity or {}).get("exchange"),
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
            # Both passes read the same Yahoo feed, so the legacy aggregate counts it once (2.17.0): the news pass accepts
            # every row the press-release pass accepts and rejects the rest for the same reasons, so its counts are the
            # feed's. A failed news pass falls back to the press-release pass's counts.
            feed_read = next((diagnostic for diagnostic in yahoo_diagnostics if diagnostic.get("completed") is True), yahoo_diagnostics[0])
            source_diagnostics["yahoo_finance"] = {
                "rawCount": int(feed_read.get("rawCount") or 0),
                "retrievedCount": int(feed_read.get("retrievedCount") or 0),
                "filteredCount": int(feed_read.get("filteredCount") or 0),
                "acceptedCount": int(feed_read.get("acceptedCount") or 0),
                "rejectedCount": int(feed_read.get("rejectedCount") or 0),
                "rejectionCounts": dict(feed_read.get("rejectionCounts") or {}),
                "identityStatus": (yahoo_identity or {}).get("status") or "UNAVAILABLE",
                "attempted": True,
                "completed": all(diagnostic.get("completed") is True for diagnostic in yahoo_diagnostics),
            }
    source_diagnostics.budget_starved = sorted(budget_starved)
    source_diagnostics.disabled_sources = list(disabled_sources)
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
    description="""Search public company news/events for a ticker and query string.

Defaults use Yahoo Finance, SEC filings and eligible Finnhub, then one quota-bounded Marketaux
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
    safe_max = _coerce_max_results(max_results, 10)
    returned = _rank_news_items_by_relevance(filtered)[:safe_max]
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
        "coverage": _build_coverage(source_status, None, safe_max, len(returned)),
        "sourceStatus": source_status,
        **_source_request_fields(sources, source_status),
    }
    if status:
        payload["status"] = status
    return json.dumps(_mark_no_source_selected(payload, sources))


@yfinance_server.tool(
    name="get_company_press_releases",
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
    requested_selection, source_warnings = _normalize_event_sources(sources, _DEFAULT_PRESS_RELEASE_SOURCES)
    # Disabled sources are dropped here, so the inner collections never see them; the report is attached once below (2.16.0).
    selected_sources, disabled_sources = _split_disabled_sources(requested_selection)
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
            # The filer's CIK as the decimal string the SEC archive URLs use (the Worker's secEvidence[].cikInt, 2.19.0, call #18).
            cik_in_url = _re.search(r"/data/(\d+)/", url)
            evidence = {
                "filingType": "8-K",
                "filingDate": it.get("filingDate"),
                "acceptedAt": it.get("acceptedAt"),
                "accessionNumber": acc,
                "cikInt": str(it.get("cikInt") or "") or (cik_in_url.group(1) if cik_in_url else None),
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
    warnings = source_warnings + _disabled_source_warnings(disabled_sources) + primary_warnings + optional_warnings
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

    candidate_sources = [
        w.get("candidate")
        for w in warnings
        if isinstance(w, dict) and w.get("code") == "COMPANY_IR_PAGE_CANDIDATE_AVAILABLE" and isinstance(w.get("candidate"), dict)
    ]
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
            status = "COMPANY_IR_PAGE_CANDIDATE_ONLY" if candidate_sources else "COMPANY_IR_PAGE_NOT_APPROVED"
        elif "company_ir" in selected_sources:
            status = "COMPANY_IR_NOT_FOUND"
        else:
            # A disabled source never yields NOT_FOUND: the source-limited path applies (2.16.0).
            status = _build_collection_status(modified_release_items, sources_used, warnings) if disabled_sources else "NOT_FOUND"
    else:
        status = _build_collection_status(modified_release_items, sources_used, warnings)

    source_status = _compute_source_status(
        sources_used,
        warnings,
        modified_release_items,
        requested_selection,
        {**primary_diagnostics, **optional_diagnostics},
        sorted({
            *getattr(primary_diagnostics, "budget_starved", []),
            *getattr(optional_diagnostics, "budget_starved", []),
        }),
        disabled_sources,
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
    # An SEC outage keeps the status; the failure mode comes from the failed SEC row, so the empty result is not read as absence
    # (2.19.0, call #9). A failed row is PROVIDER_ERROR or PROVIDER_BUDGET_EXHAUSTED; NOT_ELIGIBLE and an operator-disabled SEC
    # keep the coverage status (planner decision 8). sourceStatus.sec already makes coverage PARTIAL.
    sec_row = source_status.get("sec")
    sec_outage_mode = (
        _verify_coverage_failure_mode({"sec": sec_row}, [])
        if isinstance(sec_row, dict) and sec_row.get("status") in ("PROVIDER_ERROR", "PROVIDER_BUDGET_EXHAUSTED")
        else None
    )
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
        "failureMode": None if decision_grade else (sec_outage_mode or coverage_status),
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
    # SEC_URL_INVALID is raised by the SEC provider's item builder (the news path and this tool share it, 2.17.0).
    items = [_enrich_news_item_for_llm(item) for item in items]
    status = _build_collection_status(items, ["sec"] if used else [], warnings)
    source_status = _mark_budget_starved_sources(
        {"sec": {
            "status": ("OK" if items else "EMPTY_RESULT") if used else "PROVIDER_ERROR",
            "rawCount": len(items),
            "filteredCount": len(items),
        }},
        ["sec"] if sec_watch.starved else [],
    )
    source_coverage = _compute_source_coverage(source_status)
    # coverage, sourceCoverage and sourceStatus, as in the Worker (2.19.0, B-G8 / call #15).
    payload = {
        "ticker": ticker.upper(),
        "items": items,
        "meta": {
            "sourcesUsed": ["sec"] if used else [],
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
    name="get_public_event_timeline",
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
    safe_max = _coerce_max_results(max_results, 50)
    timeline = timeline[:safe_max]
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
        "coverage": _build_coverage(source_status, None, safe_max, len(timeline)),
        "sourceStatus": source_status,
        **_source_request_fields(sources, source_status),
    }
    if status:
        payload["status"] = status
    return json.dumps(_mark_no_source_selected(payload, sources))


@yfinance_server.tool(
    name="verify_company_event",
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

    # A query with no meaningful terms can verify nothing, and NOT_FOUND would claim absence (2.19.0, call #12).
    if not meaningful_terms:
        return _mcp_failure(
            "verify_company_event",
            ErrorCode.INPUT_VALIDATION_ERROR,
            "The event query contains no specific event terms. Add a term such as acquisition, dividend, guidance, contract, or offering.",
        )

    items, sources_used, warnings, retrieved_at, source_diagnostics = _unpack_company_event_result(
        # Match against everything collected: a cap of the newest 50 let a flood
        # of aggregator items push the official release out before matching
        # (ASTS BlueBird launch, 2.4.5). The 365-day window is collected whatever the range, which applies only when
        # classifying, so a match outside the range can still read STALE.
        await _collect_company_events(
            ticker,
            max_results=50,
            lookback_days=365,
            sources=effective_sources,
            include_diagnostics=True,
            keep_all=True,
        )
    )

    matched = [it for it in items if _query_match(it)["matched"]]
    stale_cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=_STALE_EVENT_DAYS)).strftime("%Y-%m-%d")

    def _in_range(item: dict) -> bool:
        if not start_date and not end_date:
            # Without a range a match older than 90 days is not in range, so an old release reads STALE, not CONFIRMED
            # (2.19.0, call #11). An undated match stays in range there.
            day = str(item.get("publishedAt") or "")[:10]
            return not day or day >= stale_cutoff
        return _within_date_window(item.get("publishedAt"), start_date=start_date, end_date=end_date)

    matched_in_range = [it for it in matched if _in_range(it)]
    official_in_range = [
        it for it in matched_in_range
        if str(it.get("sourceType") or "") in _OFFICIAL_SOURCE_TYPES
        and bool(it.get("url"))
        and _confidence_for_source_type(it.get("sourceType"), it.get("confidence")) in ("HIGH", "MEDIUM")
    ]
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
    best = _er.rank_evidence(
        official_in_range or matched_in_range or matched,
        lambda ev: _confidence_for_source_type(ev.get("sourceType"), ev.get("confidence")),
    )

    # ── Ticker/entity relevance filtering ────────────────────────────────
    # Ensure bestEvidence items actually reference the queried ticker/entity
    # to prevent returning evidence about unrelated companies.
    ticker_upper = ticker.upper()
    ticker_pattern = _re.compile(r'\b' + _re.escape(ticker_upper) + r'\b', _re.IGNORECASE)

    # Issuer names score relevance only, so no evidence means no lookup (sources=[] must make no request). The name set is
    # Yahoo longName and shortName plus the SEC submissions name, without any name equal to the ticker (2.19.0, planner
    # decision 6; the call #16 rule).
    company_names: list[str] = []

    def _add_company_name(value: object) -> None:
        name = value.strip() if isinstance(value, str) else ""
        if name and name.lower() != ticker.lower() and name not in company_names:
            company_names.append(name)

    if best:
        try:
            yahoo_info = await asyncio.to_thread(lambda: yf.Ticker(ticker).info)
        except Exception:
            yahoo_info = None
        if isinstance(yahoo_info, dict):
            _add_company_name(yahoo_info.get("longName"))
            _add_company_name(yahoo_info.get("shortName"))
        try:
            _cik_padded, submissions = await _get_submissions_for_ticker(ticker)
        except Exception:
            submissions = None
        if isinstance(submissions, dict):
            _add_company_name(submissions.get("name"))

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

    name_tests = [
        test.lower()
        for name in company_names
        for test in (_extract_base_company_name(name), name)
        if test
    ]

    def _relevance_score(ev: dict) -> str:
        """Return 'HIGH', 'MEDIUM', or 'LOW' based on ticker/entity presence."""
        # Relevance is scored from the evidence text and issuer, never from the item's tickers list: every collector
        # injects the queried ticker there, which made LOW_RELEVANCE_EVIDENCE unreachable (2.17.0).
        # 1. Check source type
        source_type = str(ev.get("sourceType") or "")
        if source_type in ("sec_filing", "sec", "company_ir", "company_ir_page", "sec_ex99_found"):
            return "HIGH"

        # 2. Check text content
        hay = " ".join([
            str(ev.get("title") or ""),
            str(ev.get("summary") or ""),
            str(ev.get("evidenceText") or ""),
            str(ev.get("issuer") or ""),
        ])
        
        # Word boundary match for ticker
        if ticker_pattern.search(hay):
            return "HIGH"
            
        # Check the issuer names and their base forms
        if any(test in hay.lower() for test in name_tests):
            return "HIGH"
            
        # Check issuer field explicitly
        if _ticker_in_text(str(ev.get("issuer") or ""), ticker_upper):
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
            "confidence": _confidence_for_source_type(ev.get("sourceType"), ev.get("confidence")),
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
    company_ir_requested = sources is not None and "company_ir" in _normalize_event_sources(sources, [])[0]
    coverage_failure_mode = _verify_coverage_failure_mode(source_status, warnings, company_ir_requested)
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
    after="get_company_events_calendar",  # its position in the published listing (server.py registers the calendar)
    description="""Get recent public company news and press releases from selected public sources.

Accepts a single ticker or an array of up to 5 symbols (one symbol when sources
include company_ir, company_ir_page or newswire; a longer array is rejected with
BATCH_LIMIT_EXCEEDED). For an array, each
ticker is fetched independently and results are returned as a per-ticker keyed
object (a union of per-ticker results), so low-news conditions for one ticker
never zero out the others.

The default sources are Yahoo Finance, SEC filings and eligible Finnhub, then one
quota-bounded Marketaux wire-domain fallback only when higher-priority coverage
is empty or limited.

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
    coverage = _build_coverage(source_status, None, safe_max, len(items))
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
