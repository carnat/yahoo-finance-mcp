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


# ---------------------------------------------------------------------------
# Valuation tools - imported for registration and re-exported
# ---------------------------------------------------------------------------
import yfmcp.tools.valuation  # noqa: F401 (side-effect import: registers the valuation tools)
from yfmcp.tools.valuation import (  # re-export for compatibility, grouped routing and server-side callers
    get_historical_valuation_context,
    get_valuation_snapshot,
    reconcile_metric_sources,
)


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


# Evidence tools: handlers live in yfmcp/tools/evidence_pack.py.
import yfmcp.tools.evidence_pack  # noqa: F401 (side-effect import: registers the evidence-pack tools)
from yfmcp.tools.evidence_pack import (  # re-export for compatibility, grouped routing and server-side callers
    build_valuation_evidence_pack,
    get_evidence_cut,
    get_evidence_quality,
    list_evidence_cuts,
)


# ---------------------------------------------------------------------------
# Grouped (token-efficient) server mode
# ---------------------------------------------------------------------------
# TOOL_MODE env var controls which interface is exposed:
#   - "grouped" (default): 12 domain meta-tools with action routing
#   - "expanded": every individual tool (compatibility/debug mode)
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

Grouped mode is the default. Set TOOL_MODE=expanded to expose every individual tool.

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
    - TOOL_MODE=expanded: every individual tool
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
