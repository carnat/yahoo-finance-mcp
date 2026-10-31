import asyncio
import datetime
import email.utils as _email_utils
import hashlib
import html as _html_module
import functools
import inspect
import json
import math
import os
from pathlib import Path
import re as _re
import threading
import time
import sys
import urllib.parse as _urlparse
import urllib.request as _urlrequest
import urllib.error as _urlerror
import xml.etree.ElementTree as _ET
from typing import Annotated, Any, Literal, TypedDict
import zoneinfo

import pandas as pd
import yfinance as yf
from pydantic import Field

# Phase 2b: yfmcp.app owns the YfmcpServer class, the yfinance_server instance, and
# build_handler_registry.
from yfmcp.app import create_server, yfinance_server, build_handler_registry
from yfmcp.exposure_presets import CHINA as _CHINA_PRESET  # Q5: the China exposure preset, shared with the Worker
from yfmcp.schemas import (
    FinancialType, HolderType, RecommendationType, FilingFactType,
    _TOOL_OUTPUT_SCHEMAS, _MARKET_SNAPSHOT_OUTPUT_SCHEMA,
)


# ---------------------------------------------------------------------------
# Phase 1: yfmcp infrastructure imports
# ---------------------------------------------------------------------------
from yfmcp.envelope import (
    SERVER_VERSION, _ENVELOPE_V2, ErrorCode, ToolMeta, ErrorDetail, McpResponse,
    _mcp_success, _mcp_failure, _wrap_envelope_v2, _classify_error_message, normalize_status_confidence,
)
from yfmcp.validation import (
    _TICKER_RE, _ACCESSION_RE,
    _validate_ticker, _validate_accession,
    _validate_sec_url, _sanitize_sec_html,
)
from yfmcp.cache import (
    ToolCache, _tool_cache,
    TTL_PRICE, TTL_ANALYST, TTL_FINANCIALS, TTL_EDGAR, TTL_OPTIONS, TTL_NEWS,
    _PRICE_TTL, _STMT_TTL,
)
from yfmcp.pricing_notices import freshness_notices as _freshness_notices, take_notices, unique_notices, with_notices  # noqa: E402
from yfmcp.util import (
    _fetch_with_retry, get_last_trading_date,
    _PLACEHOLDER_IV_THRESHOLD, _compute_data_quality, _sort_by_relevance,
    _utc_now_iso, _to_iso_utc, _parse_rss_date,
    _filter_paragraphs_by_topics, _safe_json_loads, _compact_excerpt,
    fetch_quote_summary_module, unwrap_yahoo_values, yahoo_iso, yahoo_raw,
    _add_years, _clamp_int, _df_period_rows, _fy_number, _js_json_numbers, _nn, _now_iso, _plain, _unwrap_payload,
    _round_half_away_from_zero, _utf16_length,
)
from yfmcp.clients.yahoo import _safe_parse, _valuation_market_inputs
from yfmcp.clients.endpoints import _manual_lookup_payload, sec_archive_document_url, sec_company_concept_url, sec_company_facts_url, sec_filing_index_url
from yfmcp.clients.market_providers import _PROVIDER_USER_AGENT
from yfmcp import url_policy as _url_policy
from yfmcp import filing_search as _fs
from yfmcp import capital_structure as _cs
from yfmcp import valuation as _vl
from yfmcp import sec_facts as _sf
from yfmcp import extraction_rules as _er
from yfmcp import driver_ledger as _dl
from yfmcp import guidance_history as _gh
from yfmcp import metric_reconciliation as _mr
from yfmcp.evidence import AUTHORITY_BOUNDARY as _AUTHORITY_BOUNDARY, EPS_BASIS as _EPS_BASIS
from yfmcp.deprecations import MIN_ADV_USD_DESCRIPTION, TOOL_RELIABILITY_CLASS as _TOOL_RELIABILITY_CLASS, parse_min_adv_usd
from yfmcp.fiscal_calendar import document_fiscal_year_focus, filing_fiscal_year_label, fiscal_year_naming
from yfmcp import valuation_history as _vh
from yfmcp.clients.edgar import (
    _resolve_cik_for_ticker, _get_submissions_for_ticker,
    _EDGAR_TICKERS, _EDGAR_TICKERS_LOADED_AT, _EDGAR_TTL,
    _load_edgar_tickers, _edgar_get, EdgarError,
    _EDGAR_FACTS_CACHE, _EDGAR_SUBS_CACHE,
    _edgar_get_company_facts, _edgar_get_submissions,
    _edgar_build_filing_urls, _edgar_cik_from_accession,
    _edgar_list_exhibits_from_index, _edgar_get_html,
    _edgar_read_json, _warm_sec_submissions,
    _edgar_read_submissions, _edgar_get_document,
)
from yfmcp.parsing.html import (
    _strip_html_tags, _strip_html_blocks, _clean_filing_display_text, _looks_like_filing_markup_text,
    _parse_html_table, _parse_numeric_cell, _detect_unit_multiplier, _UNIT_SCALE_ASSUMED_WARNING,
    _MD_MAX_CELL_CHARS, _html_table_to_markdown, _html_to_markdown_fallback, _table_unit_scale,
)
from yfmcp.parsing.extractors import (
    _normalize_segment_label, _region_matches,
    _extract_geo_revenue_from_html, extract_segment_table_from_html, _geo_region_aliases, _text_contains_geo_region, _js_round,
    _extract_xbrl_latest_annual,
    extract_geo_revenue_from_html as _extract_geo_table, PRODUCT_BREAKDOWN_LEAD_RE as _PRODUCT_BREAKDOWN_LEAD_RE,
)

# ---------------------------------------------------------------------------
# Phase 2b: domain tool modules — import triggers @yfinance_server.tool registration
# ---------------------------------------------------------------------------
import yfmcp.tools.system  # noqa: F401 (side-effect import)
import yfmcp.tools.pricing  # noqa: F401 (side-effect import)
import yfmcp.tools.thai_funds  # noqa: F401 (side-effect import)
import yfmcp.tools.provider_gaps  # noqa: F401 (side-effect import)
from yfmcp.tools.system import health_check  # re-export for grouped routing
from yfmcp.tools.pricing import (  # re-export for compatibility and grouped routing
    get_historical_stock_prices,
    HistoricalPeriod,
    HistoricalInterval,
    get_fast_info,
    get_price_stats,
    get_ma_position,
    get_volume_ratio,
    get_volume_gate,
    get_technical_indicators,
    get_price_slope,
    get_short_interest,
    get_short_momentum,
    get_market_snapshot,
    _run_ticker_batch,
    _load_completed_daily_history,
    _select_completed_close_series,
    _daily_bar_date,
    _next_option_expiry,
    _us_market_date,
    analyze_options_flow_window,
    analyze_volume_ratio,
    calculate_price_target_distance,
    check_volume_liquidity_threshold,
    find_put_hedge_candidates,
    get_historical_prices,
    get_market_quote,
    get_option_chain,
    get_options_flow_scan,
    get_options_summary,
    get_price_target_bracket,
)
from yfmcp.tools.thai_funds import (  # re-export for grouped routing
    search_thai_funds,
    get_thai_fund_nav,
    get_thai_fund_nav_batch,
    get_thai_fund_factsheet,
    get_thai_fund_dividend_history,
)
from yfmcp.tools.provider_gaps import (  # re-export for grouped routing
    get_historical_put_call_ratio,
    get_expanded_institutional_ownership,
    get_uk_company_filings,
)


# ---------------------------------------------------------------------------
# News & public-event domain tools - imported for registration and re-exported
# ---------------------------------------------------------------------------
import yfmcp.tools.news  # noqa: F401 (side-effect import: registers the news tools)
from yfmcp.tools.news import (  # re-export for compatibility, grouped routing and server-side callers
    _finnhub_policy,
    _finnhub_eligibility,
    _enrich_news_item_for_llm,
    _build_coverage,
    _yahoo_item_issuer,
    _coerce_max_results,
    _coerce_lookback_days,
    _normalize_event_sources,
    _source_request_fields,
    _event_type_from_keywords,
    _event_type_from_form,
    _short_text,
    _make_duplicate_group_id,
    _parse_form4_transaction,
    _source_rank,
    _safe_sec_url,
    _within_date_window,
    _load_company_ir_page_registry,
    _company_ir_page_item,
    _parse_company_ir_page_json,
    _parse_company_ir_page_html,
    _collect_company_ir_events,
    _collect_company_ir_page_events,
    _build_yahoo_event_item,
    _collect_sec_events,
    _collect_yahoo_events,
    _yahoo_search_news,
    _collect_globenewswire_events,
    _collect_finnhub_events,
    _collect_marketaux_events,
    _marketaux_fallback_reason,
    _dedupe_event_items,
    _build_collection_status,
    _compute_source_status,
    _compute_source_coverage,
    _collect_company_events,
    search_company_news,
    get_company_press_releases,
    get_sec_recent_events,
    get_public_event_timeline,
    verify_company_event,
    get_company_news,
    _GLOBENEWSWIRE_RSS_FEEDS,
    _GLOBENEWSWIRE_MAX_BYTES,
)


# ---------------------------------------------------------------------------
# Fundamentals and analyst tools - imported for registration and re-exported
# ---------------------------------------------------------------------------
import yfmcp.tools.fundamentals  # noqa: F401 (side-effect import: registers the fundamentals and analyst tools)
from yfmcp.tools.fundamentals import (  # re-export for compatibility, grouped routing and server-side callers
    _alpha_vantage_estimates,
    _analyst_price_target_change,
    _consensus_naming,
    _consensus_policy,
    _consensus_providers,
    _dividend_yield_ratio,
    _limit_corporate_actions,
    _peer_valuation_error,
    _write_consensus_observation,
    analyze_financial_ratios,
    analyze_position_signals,
    analyze_share_count_trend,
    compare_peer_valuations,
    extract_analyst_valuation_methods,
    get_analyst_consensus,
    get_analyst_upgrade_radar,
    get_calendar,
    get_company_events_calendar,
    get_consensus_forecast_curve,
    get_credit_health,
    get_earnings_analysis,
    get_earnings_momentum,
    get_eps_revisions,
    get_etf_info,
    get_financial_ratios,
    get_fund_profile,
    get_market_calendar,
    get_ownership_holders,
    get_stock_info,
)


# get_company_news is registered by yfmcp.tools.news (imported above, next to the other news tools) with
# after="get_company_events_calendar", which keeps its original position in the tool listing.


# ---------------------------------------------------------------------------
# SEC EDGAR retrieval tools - imported for registration and re-exported
# ---------------------------------------------------------------------------
import yfmcp.tools.sec  # noqa: F401 (side-effect import: registers the SEC EDGAR retrieval tools)
from yfmcp.tools.sec import (  # re-export for compatibility, grouped routing and server-side callers
    _FilingPeriodParam,
    _SEC_DOCUMENT_READ_MAX_CHARS,
    _accession_not_found_envelope,
    _build_filing_index_from_html,
    _filing_period_failure,
    _filing_table_is_usable,
    _find_section_bounds,
    _is_accession_not_found,
    _is_decision_grade_sec_xbrl_fact,
    _is_likely_xbrl_document_url,
    _legacy_accession_miss_payload,
    _map_extraction_to_retrieval_path,
    _parse_filing_table_rows,
    _resolve_periodic_filings,
    _resolve_sec_filing,
    _sec_filing_index_full,
    _sec_xbrl_source_evidence,
    _suggest_alternative_queries,
    extract_filing_fact,
    extract_sec_filing_fact,
    get_filing_data,
    get_filing_outline,
    get_filing_section,
    get_filing_table,
    get_sec_filing_index,
    get_sec_filing_intelligence,
    get_sec_filing_outline,
    get_sec_filing_section,
    get_sec_filing_section_markdown,
    get_sec_filing_table,
    list_filing_tables,
    list_sec_company_filings,
    list_sec_filing_tables,
    list_sec_filings,
    list_sec_material_filings,
    search_filing_text,
    search_sec_filing_text,
)


# ---------------------------------------------------------------------------
# SEC extractors and exposure tools - imported for registration and re-exported
# ---------------------------------------------------------------------------
import yfmcp.tools.exposure  # noqa: F401 (side-effect import: registers the extractor and exposure tools)
from yfmcp.tools.exposure import (  # re-export for compatibility, grouped routing and server-side callers
    _as_status,
    _earnings_release_picks,
    _extract_customer_concentration,
    _extract_revenue_exposure,
    _extract_risk_factor_mentions,
    _extract_segment_revenue,
    _extract_total_revenue,
    _read_release_text,
    _readable_exposure_excerpt,
    extract_capital_structure,
    extract_china_exposure,
    extract_customer_concentration,
    extract_dilution_bridge,
    extract_exposure,
    extract_funding_capex_schedule,
    extract_geographic_revenue,
    extract_operating_driver_ledger,
    extract_revenue_exposure,
    extract_risk_factor_mentions,
    extract_segment_revenue,
    extract_total_revenue,
    get_share_count_scenarios,
    query_sec_filing_index,
)


# ── Historical valuation context and metric reconciliation (2.5.4) ────────

def _daily_history_sync(ticker: str, from_date: str) -> dict | None:
    """Daily closes (Yahoo split-adjusted), split events and the quote currency from from_date to now."""
    company = yf.Ticker(ticker)
    df = company.history(start=from_date, interval="1d", auto_adjust=False, actions=True)
    if df is None or getattr(df, "empty", True):
        return None
    bars: list[dict] = []
    splits: list[dict] = []
    for idx, row in df.iterrows():
        date = idx.strftime("%Y-%m-%d")
        close = row.get("Close")
        if close is not None and math.isfinite(float(close)):
            bars.append({"date": date, "close": float(close)})
        ratio = row.get("Stock Splits")
        if ratio is not None and math.isfinite(float(ratio)) and float(ratio) > 0:
            splits.append({"date": date, "ratio": float(ratio)})
    meta = getattr(company, "history_metadata", None) or {}
    return {"bars": bars, "splits": splits, "currency": meta.get("currency")}


async def _daily_history(ticker: str, from_date: str) -> dict | None:
    try:
        return await asyncio.to_thread(_daily_history_sync, ticker, from_date)
    except Exception:  # noqa: BLE001 - reported as unread
        return None


# Cover pages sit at the top of a periodic report with the header contexts; the first 900 KB is read, not
# the whole report (ASTS's 10-Q is 3.5 MB, its class counts end near 194 KB).
_COVER_READ_MAX_BYTES = 900_000


_PERIODIC_REPORT_FORMS = {"10-K", "10-Q", "20-F", "40-F"}


def _periodic_reports(subs: dict | None, cik_padded: str | None) -> list[dict]:
    """The filer's periodic reports in SEC's recent submissions, with their primary documents."""
    if not subs or not cik_padded:
        return []
    recent = (subs.get("filings") or {}).get("recent") or {}

    def at(key: str, i: int) -> str:
        values = recent.get(key) or []
        return str(values[i] or "") if i < len(values) and values[i] is not None else ""

    out = []
    for i, form in enumerate(recent.get("form") or []):
        form_upper = str(form or "").upper()
        acc, doc = at("accessionNumber", i), at("primaryDocument", i)
        if form_upper not in _PERIODIC_REPORT_FORMS or not acc or not doc:
            continue
        out.append({"accessionNumber": acc, "form": form_upper, "filed": at("filingDate", i), "reportDate": at("reportDate", i) or None,
                    "documentUrl": sec_archive_document_url(int(cik_padded), acc.replace('-', ''), doc)})
    return out


async def _cover_counts_for(companyfacts: dict, reports: list[dict], dates: list[str]) -> dict:
    """Per-class cover-page share counts from the periodic reports companyfacts cannot give a point-in-time count for."""
    out: dict = {}
    for n in _vh.cover_reads_needed(companyfacts, reports, dates):
        url = n["documentUrl"]
        try:
            html = await _edgar_get_html(url, max_bytes=_COVER_READ_MAX_BYTES)
        except Exception:  # noqa: BLE001 - reported as unread
            html = None
        out[n["accessionNumber"]] = {**_vh.cover_share_counts(html), "documentUrl": url} if html else {"status": "NOT_READ", "value": None, "documentUrl": url}
    return out


async def _valuation_history_for(ticker: str, dates: list[str] | None, from_date: str) -> dict:
    upper = ticker.upper()
    cik_padded = await _resolve_cik_for_ticker(ticker)
    if not cik_padded:
        ticker_index = await _load_edgar_tickers()
        code = "NO_SEC_REGISTRANT" if ticker_index else "SEC_LOOKUP_UNAVAILABLE"
        return {"ticker": upper, "status": code, "code": code, "retryable": not ticker_index, **_AUTHORITY_BOUNDARY}
    facts, history = await asyncio.gather(_edgar_get_company_facts(cik_padded), _daily_history(ticker, from_date), return_exceptions=True)
    facts = None if isinstance(facts, BaseException) else facts
    history = None if isinstance(history, BaseException) else history
    if not facts:
        return {"ticker": upper, "status": "COMPANYFACTS_NOT_AVAILABLE", "code": "COMPANYFACTS_NOT_AVAILABLE", "retryable": True, **_AUTHORITY_BOUNDARY}
    if not history or not history["bars"]:
        return {"ticker": upper, "status": "PRICE_HISTORY_NOT_AVAILABLE", "code": "PRICE_HISTORY_NOT_AVAILABLE", "retryable": True, **_AUTHORITY_BOUNDARY}
    use_dates = dates if dates is not None else _vh.valuation_dates(None, history["bars"][-1]["date"])[0]
    _, currency = _vh.taxonomy_of(facts)
    price_currency = _vl._major_price(1, history["currency"])[1] if history["currency"] else None
    warnings: list[dict] = []
    # One FX series per reporting currency the dates use (2.5.32, F-029: NBIS reported in RUB, then USD).
    fx_by_currency: dict[str, dict] = {}
    if price_currency:
        for c in _vh.reporting_currencies(facts, use_dates):
            if c == price_currency:
                continue
            pair = f"{c}{price_currency}=X"
            fx_history = await _daily_history(pair, from_date)
            fx_by_currency[c] = {"pair": pair, "bars": (fx_history or {}).get("bars") or []}
    fx = fx_by_currency.get(currency) if currency else None
    ratio = None
    if _vh.foreign_filer(facts):
        try:
            market = await _valuation_market_inputs(ticker)
        except Exception:  # noqa: BLE001 - no ratio without Yahoo's count
            market = None
        sec = _vh.latest_share_count(facts)
        quoted = (market or {}).get("sharesOutstanding")
        r = _vl.ads_ratio(sec, quoted) if sec is not None and quoted is not None else None
        if r is not None and r != 1:
            ratio = r
            warnings.append({"code": "ADR_RATIO_APPLIED", "message": (
                f"SEC counts {sec} ordinary shares; Yahoo counts {quoted} quoted shares, {r} ordinary shares each. Share counts are divided by "
                f"{r} at every date."), "severity": "info"})
    try:
        _, subs = await _get_submissions_for_ticker(ticker)
    except Exception:  # noqa: BLE001 - reported below
        subs = None
    reports = _periodic_reports(subs, cik_padded)
    cover_counts = await _cover_counts_for(facts, reports, use_dates)
    periodic_filings = [{k: r[k] for k in ("accessionNumber", "form", "filed", "reportDate")} for r in reports]
    out = _vh.historical_valuation({"ticker": ticker, "dates": use_dates, "companyfacts": facts, "bars": history["bars"],
                                    "priceCurrency": history["currency"], "splits": history["splits"], "fx": fx, "fxByCurrency": fx_by_currency, "adsRatio": ratio,
                                    "coverCounts": cover_counts, "periodicFilings": periodic_filings})
    if not subs:
        warnings.append({"code": "SUBMISSIONS_NOT_AVAILABLE", "message": (
            "SEC submissions could not be read, so no cover page was read; filers without an undimensioned share count have no market cap. Retry."),
            "severity": "warning"})
    out["warnings"] = warnings
    return out


@yfinance_server.tool(
    name="get_historical_valuation_context",
    description="Market cap, enterprise value and EV/Revenue, EV/EBITDA, P/E and P/S as they stood at each date, for a ticker and caller-named peers on the same dates. Point in time: SEC companyfacts filed on or before each date; the close on or before it with later split adjustments undone; balances at the latest balance date then filed; LTM (LFY + YTD - prior YTD) and LFY denominators with their periods, concepts and filings; reporting currency converted at that date's FX close; ADR share counts divided by ordinary shares per ADS. Shares are point in time: the cover-page or balance-sheet count, else the filing's per-class cover counts summed; a weighted-average count never sets market cap (POINT_IN_TIME_SHARES_UNRESOLVED). EBITDA is computed (operating income + D&A). Untagged or unfiled figures leave their multiples null with a status; status is OK only when all eight multiples are usable (coreStatus covers market cap and EV, coverage lists the rest); peer medians are unweighted. No multiple is selected. Evidence only.",
)
async def get_historical_valuation_context(ticker: str, dates: list[str] | None = None, peers: list[str] | None = None) -> str:
    requested = None
    if dates:
        requested, error = _vh.valuation_dates([str(d).strip() for d in dates], None)
        if error:
            return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": error})
    peer_list = list(dict.fromkeys(p for p in (str(x).strip().upper() for x in (peers or [])) if p and p != ticker.upper()))
    if len(peer_list) > 5:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "At most 5 peers."})
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    earliest = requested[0] if requested else f"{int(today[:4]) - 5}{today[4:]}"
    from_date = (datetime.date.fromisoformat(earliest[:4] + ("-02-28" if earliest[4:] == "-02-29" else earliest[4:])) - datetime.timedelta(days=15)).isoformat()
    subject = await _valuation_history_for(ticker, requested, from_date)
    final_dates = requested if requested else [str(p["date"]) for p in subject.get("points") or []]
    peer_results: list[dict] = []
    # Three at a time keeps SEC and Yahoo request rates modest.
    for i in range(0, len(peer_list), 3):
        peer_results += await asyncio.gather(*(_valuation_history_for(p, final_dates or None, from_date) for p in peer_list[i:i + 3]))
    return json.dumps({
        "ticker": ticker.upper(),
        "status": str(subject.get("status") or "NOT_AVAILABLE"),
        "dates": final_dates,
        "subject": subject,
        "peers": peer_results,
        "peerMedians": _vh.peer_medians(final_dates, [p for p in peer_results if isinstance(p.get("points"), list)]) if peer_list and final_dates else [],
        "notes": [
            "Peers are the caller's; their medians are unweighted and count only peers with that multiple on that date. Each peer's denominators are its own fiscal periods, named in its points.",
            "The latest date's close can be intraday while the market is open.",
        ],
        **_AUTHORITY_BOUNDARY,
    })


_RELEASE_READ_MAX = 4


def _yahoo_statement_rows_sync(ticker: str, annual: bool, balance: bool) -> list[dict]:
    """Yahoo statement columns as rows keyed by camelCase line item: {"date": "2026-06-30", "totalRevenue": ...}."""
    company = yf.Ticker(ticker)
    if balance:
        df = company.balance_sheet if annual else company.quarterly_balance_sheet
    else:
        df = company.income_stmt if annual else company.quarterly_income_stmt
    if df is None or getattr(df, "empty", True):
        return []
    rows = []
    for col in df.columns:
        row: dict = {"date": pd.Timestamp(col).strftime("%Y-%m-%d")}
        for name, value in df[col].items():
            if value is None or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                continue
            key = str(name).replace(" ", "")
            row[key[:1].lower() + key[1:]] = float(value)
        rows.append(row)
    return rows


@yfinance_server.tool(
    name="reconcile_metric_sources",
    description="One metric for one period as each source states it: SEC XBRL as first filed and as latest filed (a difference is a restatement), the issuer's earnings release (only sentences naming the metric, an amount and the period's scope) and Yahoo's statement row, each compared with the latest SEC value with difference, percentage and tolerance (the larger of tolerance_pct and half the release's last stated digit). Status AGREED requires latest SEC plus at least one independent matching provider; PARTIAL covers a missing SEC baseline or only one provider; otherwise CONFLICT or NOT_FOUND, with source evidence. Metrics: revenue, net_income, operating_income, eps_diluted, cash_and_equivalents. Evidence only.",
)
async def reconcile_metric_sources(ticker: str, metric: str, period: Annotated[str, Field(description=_mr.RECONCILE_PERIOD_DESCRIPTION)] = "latest", tolerance_pct: float = _mr.DEFAULT_TOLERANCE_PCT) -> str:
    upper = ticker.upper()
    if metric not in _mr.METRICS:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": f"metric must be one of: {', '.join(_mr.METRICS)}"})
    # A period it cannot read is an input error before any fetch, not ok:true with INVALID_PERIOD (2.26.5, F60).
    if not _mr.is_reconcile_period(period):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": _mr.RECONCILE_PERIOD_HELP})
    # Q3e (3.2.1): only a finite number is clamped to 0-10; NaN, an infinity, a numeric string or anything else is the
    # 0.5 default, as in the Worker (min/max before the finiteness check turned NaN into 0 and +inf into 10).
    if isinstance(tolerance_pct, (int, float)) and not isinstance(tolerance_pct, bool) and math.isfinite(tolerance_pct):
        tolerance = min(10.0, max(0.0, float(tolerance_pct)))
    else:
        tolerance = _mr.DEFAULT_TOLERANCE_PCT
    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded:
        ticker_index = await _load_edgar_tickers()
        code = "NO_SEC_REGISTRANT" if ticker_index else "SEC_LOOKUP_UNAVAILABLE"
        return json.dumps({"ticker": upper, "metric": metric, "status": code, "code": code, "retryable": not ticker_index})
    facts = await _edgar_get_company_facts(cik_padded)
    if not facts:
        return json.dumps({"ticker": upper, "metric": metric, "status": "COMPANYFACTS_NOT_AVAILABLE", "code": "COMPANYFACTS_NOT_AVAILABLE", "retryable": True})
    # The latest annual report may be missing from companyfacts (2.5.30, F-015): said on every result, found or not.
    coverage_warning = _cfc.latest_annual_coverage_warning(_cfc.annual_filings_from_submissions(subs), facts)
    resolved = _mr.resolve_period(facts, metric, period)
    if resolved["status"] != "OK":
        return json.dumps({"ticker": upper, "metric": metric, **resolved, **({"warnings": [coverage_warning]} if coverage_warning else {})})
    cik_int = int(cik_padded)
    end = str(resolved["periodEnd"])
    # Results releases (Item 2.02 8-Ks and 8-K/As) filed within 100 days after the period ended. The newest
    # _RELEASE_READ_MAX are read, so a later correction is never crowded out by earlier filings; they are kept
    # oldest first, and the latest with a scoped figure wins (an early Item 2.02 8-K, as ASTS filed on
    # 2026-07-15, is not always the results release).
    in_window = sorted((p for p in (_earnings_release_picks(subs, cik_int, 40, True) if subs else [])
                        if p["filingDate"] > end and (datetime.date.fromisoformat(p["filingDate"]) - datetime.date.fromisoformat(end)).days <= 100),
                       key=lambda p: (p["filingDate"], p["accessionNumber"]))
    candidates = in_window[-_RELEASE_READ_MAX:]
    not_read = in_window[:len(in_window) - len(candidates)]
    annual = resolved["periodType"] == "ANNUAL"
    instant = _mr.METRICS[metric]["kind"] == "instant"

    async def read_releases() -> list[dict]:
        read = []
        for c in candidates:
            try:
                r = {**await _read_release_text(cik_int, c["accessionNumber"], c["primaryUrl"]), "filingDate": c["filingDate"], "accessionNumber": c["accessionNumber"]}
            except Exception:  # noqa: BLE001 - reported as unread
                r = {"status": "NOT_READ", "url": c["primaryUrl"], "filingDate": c["filingDate"], "accessionNumber": c["accessionNumber"], "text": None}
            read.append(r)
        return read

    async def yahoo_rows() -> list[dict] | None:
        try:
            return await asyncio.to_thread(_yahoo_statement_rows_sync, ticker, annual, instant)
        except Exception:  # noqa: BLE001 - reported as unread
            return None

    releases, rows = await asyncio.gather(read_releases(), yahoo_rows())
    out = _mr.metric_reconciliation(ticker=ticker, metric=metric, period=resolved, companyfacts=facts, releases=releases, yahoo_rows=rows,
                                    tolerance_pct=tolerance)
    warnings = []
    unread = [r for r in releases if r["status"] == "NOT_READ"]
    if unread:
        warnings.append({"code": "RELEASE_TEXT_NOT_AVAILABLE", "message": f"{len(unread)} earnings release(s) could not be read from SEC; retry.", "severity": "warning"})
    if rows is None:
        warnings.append({"code": "YAHOO_NOT_AVAILABLE", "message": "Yahoo's statements could not be read; retry.", "severity": "warning"})
    out["releaseCandidates"] = {"inWindow": len(in_window), "read": len(candidates),
                                "notRead": [{"filingDate": p["filingDate"], "accessionNumber": p["accessionNumber"], "form": p["form"]} for p in not_read]}
    if not_read:
        warnings.append({"code": "OLDER_RELEASE_CANDIDATES_NOT_READ", "message": (
            f"{len(not_read)} older Item 2.02 filing(s) in the 100-day window were not read; the {len(candidates)} newest were."), "severity": "info"})
    if coverage_warning:
        warnings.append(coverage_warning)
    out["retryable"] = bool(unread) or rows is None
    out["warnings"] = warnings
    return json.dumps(out)


# ── Valuation snapshot and peer multiples (2.4.0) ─────────────────────────
# Mirrors the Worker; the arithmetic lives in yfmcp.valuation.


_SEC_UNAVAILABLE_STATUSES = {"TICKER_NOT_FOUND", "FILING_NOT_FOUND_TRY_OTHER_TYPE", "FILING_TEXT_NOT_AVAILABLE"}


@yfinance_server.tool(
    name="get_valuation_snapshot",
    description="Valuation context for one ticker at the current price or one you supply: diluted shares from the SEC dilution bridge at that price, the filing's period-end cash and debt, equity and enterprise value (in-the-money convertibles leave the debt), EV/Revenue, EV/EBITDA and P/E on trailing results and Yahoo's current and next fiscal-year consensus with analyst counts, and ATM capacity. Falls back to Yahoo figures for non-USD or non-SEC listings. Mechanical context, not a price target; nothing is back-solved.",
)
async def get_valuation_snapshot(
    ticker: str,
    price: float | None = None,
    filing_type: str = "latest",
    include_sec_filings: bool = True,
) -> str:
    if price is not None:
        try:
            price = float(price)
        except (TypeError, ValueError):
            price = math.nan
        if not math.isfinite(price) or price <= 0:
            return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "price must be a positive number when supplied."})
    try:
        market = await _valuation_market_inputs(ticker)
    except Exception as exc:
        # As compare_peer_valuations (F57): an unknown ticker is TICKER_NOT_FOUND, and no provider URL is shown (3.2.1).
        failure = _peer_valuation_error(ticker.upper(), str(exc))
        message = failure["message"] if failure["code"] == "TICKER_NOT_FOUND" else f"Yahoo quote summary failed for {ticker.upper()}: {failure['message']}"
        return json.dumps({"error": True, "code": failure["code"], "message": message})
    quote = price if price is not None else market["price"]
    sec_warnings: list[dict] = []
    bridge = capital = None
    if quote is not None and include_sec_filings is not False:
        major, major_currency = _vl._major_price(quote, market["currency"])
        if major_currency is None or major_currency == "USD":
            b_raw, c_raw = await asyncio.gather(
                extract_dilution_bridge(ticker=ticker, price=major, currency="USD", filing_type=filing_type, include_atm=True),
                extract_capital_structure(ticker=ticker, filing_type=filing_type, include_funding_statements=False),
            )
            b, c = _unwrap_payload(b_raw), _unwrap_payload(c_raw)
            bridge = b if b.get("basis") == "MECHANICAL_COMPANY_DISCLOSED" else None
            capital = c if c.get("basis") == "COMPANY_DISCLOSED" else None
            status = str(b.get("status") or b.get("code") or c.get("status") or "")
            if bridge is None or capital is None:
                sec_warnings.append({
                    "code": "SEC_FILINGS_UNAVAILABLE",
                    "message": f"SEC filings were not read ({status}); Yahoo figures are used." if status in _SEC_UNAVAILABLE_STATUSES else "SEC filings could not be read in full; Yahoo figures fill the gaps.",
                    "severity": "info",
                })
        else:
            sec_warnings.append({"code": "NON_USD_LISTING", "message": f"SEC filings are read only for USD listings; {ticker.upper()} is quoted in {market['currency']}, so Yahoo figures are used.", "severity": "info"})
    return json.dumps(_vl.valuation_snapshot(ticker.upper(), market, price, bridge, capital, sec_warnings))


# ---------------------------------------------------------------------------
# Earnings/transcript domain tools - imported for registration and re-exported
# ---------------------------------------------------------------------------
import yfmcp.tools.earnings  # noqa: F401 (side-effect import)
from yfmcp.tools.earnings import (  # re-export for compatibility and grouped routing
    _is_paywalled_url,
    _classify_earnings_source_url,
    _fetch_public_html,
    _scale_number_from_text,
    _first_sentence_for_topic,
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
    get_guidance_history,
)


# ---------------------------------------------------------------------------
# Evidence tools (2.5.0): consensus curve, EPS revisions, evidence quality,
# valuation evidence pack and content-addressed evidence cuts. Shared logic
# lives in yfmcp/evidence.py (parity with worker/src/evidence.ts); storage in
# yfmcp/evidence_store.py (YFMCP_EVIDENCE_DIR, else UNAVAILABLE).
# ---------------------------------------------------------------------------
from yfmcp import evidence as _ev  # noqa: E402
from yfmcp import companyfacts_coverage as _cfc  # noqa: E402
from yfmcp import evidence_store as _es  # noqa: E402
from yfmcp.build_info import BUILD_SHA as _BUILD_SHA  # noqa: E402
from yfmcp.clients.market_providers import fetch_alpha_vantage_json as _fetch_alpha_vantage_json  # noqa: E402


async def _sec_filing_rows(ticker: str) -> tuple[list[dict] | None, str]:
    try:
        cik_padded, subs = await _get_submissions_for_ticker(ticker)
    except Exception as exc:  # noqa: BLE001
        return None, f"PROVIDER_ERROR: {exc}"
    if not cik_padded or not subs:
        return None, "TICKER_NOT_FOUND"
    recent = (subs.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []

    def at(field: str, i: int) -> Any:
        values = recent.get(field) or []
        return values[i] if i < len(values) else None

    rows = []
    for i, form in enumerate(forms):
        filing_date = str(at("filingDate", i) or "")
        if not filing_date:
            continue
        inline = at("isInlineXBRL", i)
        rows.append({
            "form": str(form),
            "filingDate": filing_date,
            "reportDate": str(at("reportDate", i)) if at("reportDate", i) else None,
            "items": str(at("items", i)) if at("items", i) is not None else None,
            "isInlineXBRL": (int(inline) == 1) if inline is not None and str(inline).strip() != "" else None,
        })
    return rows, "OK"


@yfinance_server.tool(
    name="get_evidence_quality",
    description="Preflight before a valuation evidence pack: status, freshness and coverage per evidence family (quote, latest SEC periodic filing, capital structure and dilution readiness, earnings-release guidance, material 8-Ks, FY0/FY+1 consensus cells, durable storage) and the blockers, from light requests only. Evidence only.",
)
async def get_evidence_quality(ticker: str) -> str:
    as_of = _now_iso()
    (inputs, quote, splits), (rows, status), naming = await asyncio.gather(_consensus_providers(ticker), _sec_filing_rows(ticker), _consensus_naming(ticker))
    curve = _ev.build_consensus_curve(ticker, inputs, as_of, None, naming, splits)
    return json.dumps(_ev.evidence_quality(ticker=ticker, as_of=as_of, quote=quote, filings=rows, filings_status=status,
                                           consensus=curve, storage_available=_es.get_store() is not None))


async def _run_component(source_tool: str, coro: Any) -> dict:
    try:
        text = await coro
        return _ev.component_from_tool_text(source_tool, text, _now_iso())
    except Exception as exc:  # noqa: BLE001 - a failed component is evidence of that failure
        return _ev.component_from_tool_text(source_tool, None, _now_iso(), exc)


def _not_applicable(source_tool: str, code: str, message: str, retrieved_at: str) -> dict:
    return {"status": "NOT_APPLICABLE", "sourceTool": source_tool, "retrievedAt": retrieved_at, "data": None, "warnings": [], "error": {"code": code, "message": message}}


@yfinance_server.tool(
    name="build_valuation_evidence_pack",
    description="One evidence cut for valuation work: quote, evidence quality, consensus curve, EPS revisions, current capital structure, dilution bridge at the current price, latest guidance and material filings, each with its source tool, status, warnings and failure, plus a provenance receipt hashing every component. decisionUse is EVIDENCE_ONLY. Stored immutably under a content-addressed evidenceCutId when storage is available; the full payload is returned either way.",
)
async def build_valuation_evidence_pack(ticker: str, horizon_years: int = 5, persist: bool = True) -> str:
    policy = _consensus_policy(horizon_years, _ev.DEFAULT_CONSENSUS_POLICY["minAnalystCount"], _ev.DEFAULT_CONSENSUS_POLICY["conflictTolerancePct"])
    if isinstance(policy, str):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": policy})
    symbol = ticker.upper()
    cutoff = _now_iso()
    (inputs, quote, splits), (rows, filings_status), naming = await asyncio.gather(_consensus_providers(symbol), _sec_filing_rows(symbol), _consensus_naming(symbol))
    curve = _ev.build_consensus_curve(symbol, inputs, cutoff, policy, naming, splits)
    major_price, major_currency = _vl._major_price(quote["price"], quote["currency"]) if quote["price"] is not None else (None, None)

    async def dilution() -> dict:
        if major_price is None:
            return _not_applicable("extract_dilution_bridge", "PRICE_UNAVAILABLE", "No current price to run the dilution bridge at.", cutoff)
        if major_currency != "USD":
            return _not_applicable("extract_dilution_bridge", "NON_USD_LISTING", f"The dilution bridge reads SEC filings for USD listings; {symbol} is quoted in {quote['currency']}.", cutoff)
        return await _run_component("extract_dilution_bridge", extract_dilution_bridge(ticker=symbol, price=major_price, currency="USD", filing_type="latest", include_atm=True))

    await _warm_sec_submissions(symbol)
    capital, diluted, guidance, events = await asyncio.gather(
        _run_component("extract_capital_structure", extract_capital_structure(ticker=symbol, filing_type="latest", include_funding_statements=True)),
        dilution(),
        _run_component("extract_guidance", extract_guidance(symbol, "latest")),
        _run_component("list_sec_material_filings", list_sec_material_filings(symbol, None, 10)),
    )
    # The release can fail to load while the other SEC reads run; one sequential retry, and the result says which.
    if (guidance.get("error") or {}).get("code") == "RELEASE_TEXT_NOT_AVAILABLE":
        guidance = await _run_component("extract_guidance", extract_guidance(symbol, "latest"))
    components = {
        "quote": {
            "status": "OK" if quote["price"] is not None else "FAILED",
            "sourceTool": "yahoo_quote_summary",
            "retrievedAt": inputs[0]["retrievedAt"],
            "data": {**quote, "majorUnitPrice": major_price, "majorUnitCurrency": major_currency},
            "warnings": [],
            "error": None if quote["price"] is not None else {"code": quote["status"], "message": quote.get("message") or "No price."},
        },
        "evidenceQuality": _ev.component_from_value("get_evidence_quality", _ev.evidence_quality(
            ticker=symbol, as_of=cutoff, quote=quote, filings=rows, filings_status=filings_status, consensus=curve, storage_available=_es.get_store() is not None,
        ), cutoff),
        "consensus": _ev.component_from_value("get_consensus_forecast_curve", curve, cutoff),
        "epsRevisions": _ev.component_from_value("get_eps_revisions", _ev.build_eps_revisions(symbol, inputs, cutoff, splits), cutoff),
        "currentCapitalStructure": capital,
        "currentDilution": diluted,
        "latestGuidance": guidance,
        "materialEvents": events,
    }
    # A pack whose providers all failed is still evidence of that; its components say so.
    if all(not i["periods"] for i in inputs):
        components["consensus"]["status"] = "LIMITED"
    # Components carry confidence levels only (F20), fixed before they are hashed.
    for component in components.values():
        normalize_status_confidence(component)
    hashes = {name: _es.sha256_hex(_ev.canonical_json(c)) for name, c in components.items()}
    receipt = _ev.build_receipt(ticker=symbol, evidence_cutoff=cutoff, server_version=SERVER_VERSION, build_sha=_BUILD_SHA, runtime="python_local",
                                components=components, component_hashes=hashes, components_sha256=_es.sha256_hex(_ev.canonical_json(components)))
    document = _ev.evidence_cut_document(ticker=symbol, evidence_cutoff=cutoff, components=components, receipt=receipt)
    body = _ev.canonical_json(document)
    sha = _es.sha256_hex(body)
    key = _ev.evidence_cut_key(symbol, _ev.compact_timestamp(cutoff), sha)
    stored = _es.put_once(key, body, {"ticker": symbol, "sha256": sha, "serverVersion": SERVER_VERSION}) if persist is not False else {"status": "SKIPPED"}
    observation = _write_consensus_observation(symbol, curve, cutoff) if persist is not False else {"status": "SKIPPED", "key": None}
    return json.dumps({
        **document,
        "evidenceCut": {
            "evidenceCutId": _ev.evidence_cut_id(symbol, cutoff, sha),
            "contentSha256": sha,
            "storageStatus": stored["status"],
            "storageKey": key if stored["status"] in ("STORED", "ALREADY_STORED") else None,
            **({"storageMessage": stored["message"]} if stored.get("message") else {}),
            "consensusObservation": observation,
            "verification": "contentSha256 is SHA-256 over canonical JSON (sorted keys, no whitespace, ECMAScript numbers) of this payload without the evidenceCut field.",
        },
    })


@yfinance_server.tool(
    name="get_evidence_cut",
    description="Retrieve a stored evidence cut by evidenceCutId and verify its integrity: the SHA-256 of the stored canonical JSON must equal the hash in the id (integrity VERIFIED or MISMATCH).",
)
async def get_evidence_cut(evidence_cut_id: str) -> str:
    parsed = _ev.parse_evidence_cut_id(evidence_cut_id or "")
    if parsed is None:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "evidence_cut_id must look like ec1_<TICKER>_<YYYYMMDDTHHMMSSZ>_<sha256>."})
    store = _es.get_store()
    if store is None:
        return json.dumps({"error": True, "code": "STORAGE_UNAVAILABLE", "message": "Durable evidence storage is not configured; evidence cuts cannot be retrieved."})
    try:
        text = store.get(parsed["key"])
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"error": True, "code": "STORAGE_ERROR", "message": str(exc)})
    if text is None:
        return json.dumps({"error": True, "code": "NOT_FOUND", "message": f"No evidence cut is stored under {evidence_cut_id}."})
    actual = _es.sha256_hex(text)
    try:
        document = json.loads(text)
    except ValueError:
        document = None
    return json.dumps({
        "evidenceCutId": evidence_cut_id,
        "storageKey": parsed["key"],
        "integrity": "VERIFIED" if actual == parsed["sha256"] and document is not None else "MISMATCH",
        "expectedSha256": parsed["sha256"],
        "actualSha256": actual,
        "document": document,
    })


@yfinance_server.tool(
    name="list_evidence_cuts",
    description="Stored evidence cuts for a ticker, newest first, with their ids and cutoff times.",
)
async def list_evidence_cuts(ticker: str, limit: int = 20) -> str:
    ticker_err = _validate_ticker(ticker)
    if ticker_err:
        return _mcp_failure("list_evidence_cuts", ErrorCode.INPUT_VALIDATION_ERROR, ticker_err)
    symbol = ticker.upper()
    store = _es.get_store()
    if store is None:
        return json.dumps({"ticker": symbol, "storageStatus": "UNAVAILABLE", "cuts": []})
    cap = max(1, min(100, int(limit) if isinstance(limit, (int, float)) else 20))
    try:
        keys = store.list(f"evidence-cuts/{symbol}/", 1000)
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"ticker": symbol, "storageStatus": "FAILED", "cuts": [], "message": str(exc)})
    cuts = []
    for key in keys:
        m = _re.match(r"^evidence-cuts/([^/]+)/(\d{8}T\d{6}Z)/([0-9a-f]{64})\.json$", key)
        if m:
            cuts.append({"evidenceCutId": f"ec1_{m.group(1)}_{m.group(2)}_{m.group(3)}", "cutoff": m.group(2), "storageKey": key})
    cuts.sort(key=lambda c: c["cutoff"], reverse=True)
    return json.dumps({"ticker": symbol, "storageStatus": "AVAILABLE", "cuts": cuts[:cap]})


# ---------------------------------------------------------------------------
# Grouped (token-efficient) server mode
# ---------------------------------------------------------------------------
# TOOL_MODE env var controls which interface is exposed:
#   - "grouped" (default): 12 domain meta-tools with action routing
#   - "expanded": all 79 individual tools (compatibility/debug mode)
# ---------------------------------------------------------------------------
_TOOL_MODE = os.environ.get("TOOL_MODE", "grouped").lower().strip()

# tool_catalog.json tickerCaps are advertised as maxItems by YfmcpServer.list_tools (2.12.0, Q3c).


def _build_grouped_server():
    """Create a YfmcpServer with grouped meta-tools for token efficiency."""
    from tool_groups import TOOL_GROUPS, register_grouped_tools
    from yfmcp.app import build_tool_contract_registry

    grouped = create_server(
        "yfinance",
        instructions="""
# Yahoo Finance MCP Server (Grouped Mode)

Grouped mode is the default. Set TOOL_MODE=expanded to expose 79 individual tools.

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

    # Resolve handlers from the shared server's registry (single source of truth)
    # rather than this module's globals, so the mapping holds as handlers move
    # into yfmcp.tools.* during the Phase 2 split.
    handler_registry = build_handler_registry(yfinance_server)
    contract_registry = build_tool_contract_registry(yfinance_server)
    register_grouped_tools(grouped, handler_registry, contract_registry)
    return grouped


# Lazily built grouped server (only constructed if TOOL_MODE=grouped)
_grouped_server = None


def get_server():
    """Return the appropriate server based on TOOL_MODE env var.

    - TOOL_MODE=grouped (default): 12 domain meta-tools
    - TOOL_MODE=expanded: 79 individual tools
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
    print(f"Starting Yahoo Finance MCP server (mode={mode_label})...", file=sys.stderr)
    server.run(transport="stdio")
