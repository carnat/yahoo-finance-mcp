"""Central FastMCP application instance and tool alias registry.

This module is the single source of truth for:
- The FastMCP compat shim (strips unsupported output_schema kwarg on older SDKs)
- The shared ``yfinance_server`` FastMCP instance that all domain modules register on
- ``TOOL_ALIASES``: the canonical mapping of deprecated/alternate tool names to canonical ones

All ``yfmcp/tools/*.py`` domain modules import ``yfinance_server`` from here.
``server.py`` also imports it from here (never the reverse) to avoid circular imports.
"""

from __future__ import annotations

import inspect
from typing import Any

from mcp.server.fastmcp import FastMCP


# ---------------------------------------------------------------------------
# Backward-compatible FastMCP decorator shim
# ---------------------------------------------------------------------------
# Older FastMCP SDK versions do not accept an ``output_schema`` kwarg on
# ``@server.tool()``.  This shim silently drops the kwarg so the codebase can
# annotate decorators with output schemas without breaking older runtimes.
# ---------------------------------------------------------------------------
_FASTMCP_TOOL_PARAMS = inspect.signature(FastMCP.tool).parameters
_FASTMCP_TOOL_SUPPORTS_OUTPUT_SCHEMA = "output_schema" in _FASTMCP_TOOL_PARAMS or any(
    _param.kind == inspect.Parameter.VAR_KEYWORD
    for _param in _FASTMCP_TOOL_PARAMS.values()
)
if not _FASTMCP_TOOL_SUPPORTS_OUTPUT_SCHEMA:
    _ORIGINAL_FASTMCP_TOOL = FastMCP.tool

    def _fastmcp_tool_compat(self: FastMCP, *args: Any, **kwargs: Any) -> Any:
        """Strip unsupported output_schema kwarg for FastMCP SDK compatibility."""
        kwargs.pop("output_schema", None)
        return _ORIGINAL_FASTMCP_TOOL(self, *args, **kwargs)

    FastMCP.tool = _fastmcp_tool_compat


# ---------------------------------------------------------------------------
# Deprecated / alternate → canonical tool name mapping
# ---------------------------------------------------------------------------
TOOL_ALIASES: dict[str, str] = {
    "get_fast_info": "get_market_quote",
    "get_historical_stock_prices": "get_historical_prices",
    "get_stock_info": "get_company_profile",
    "get_etf_info": "get_fund_profile",
    "get_stock_actions": "get_corporate_actions",
    "get_holder_info": "get_ownership_holders",
    "get_price_stats": "analyze_price_performance",
    "get_ma_position": "analyze_moving_average_position",
    "get_volume_ratio": "analyze_volume_ratio",
    "get_volume_gate": "check_volume_liquidity_threshold",
    "get_adv_gate": "check_volume_liquidity_threshold",
    "get_financial_ratios": "analyze_financial_ratios",
    "get_credit_health": "analyze_credit_health",
    "get_recommendations": "get_analyst_recommendations",
    "get_analyst_upgrade_radar": "get_analyst_rating_changes",
    "get_earnings_momentum": "analyze_earnings_momentum",
    "get_calendar": "get_company_events_calendar",
    "get_yahoo_finance_news": "get_company_news",
    "get_options_flow_summary": "summarize_options_flow",
    "get_options_summary": "summarize_options_flow",
    "get_options_flow_scan": "analyze_options_flow_window",
    "get_dc134_options_scan": "analyze_options_flow_window",
    "get_put_hedge_candidates": "find_put_hedge_candidates",
    "get_price_target_bracket": "calculate_price_target_distance",
    "get_eqf_bracket": "calculate_price_target_distance",
    "get_position_score_inputs": "analyze_position_signals",
    "get_tps_inputs": "analyze_position_signals",
    "list_sec_filings": "list_sec_company_filings",
    "get_filing_outline": "get_sec_filing_outline",
    "get_filing_section": "get_sec_filing_section",
    "list_filing_tables": "list_sec_filing_tables",
    "get_filing_table": "get_sec_filing_table",
    "get_filing_data": "extract_sec_filing_fact",
    "extract_filing_fact": "extract_sec_filing_fact",
    "get_geographic_revenue": "extract_sec_filing_fact",
    "get_china_revenue_pct": "extract_sec_filing_fact",
    "search_filing_text": "search_sec_filing_text",
    "get_filing_text_search": "search_sec_filing_text",
    "get_filing_document": "get_sec_filing_section",
}


# ---------------------------------------------------------------------------
# Shared FastMCP server instance
# ---------------------------------------------------------------------------
yfinance_server = FastMCP(
    "yfinance",
    instructions="""
# Yahoo Finance MCP Server

This server provides financial market data from Yahoo Finance and SEC EDGAR via canonical tool names.

## Tool selection guidance
- **Prefer `get_market_snapshot`** for a one-call market overview (price, range, MA trend, volume, RSI, MACD, liquidity gate, freshness). Supports compact (max 5 tickers) and full (max 2 tickers) modes.
- **Prefer `get_market_quote`** over `get_company_profile` for current price, market cap, 52-week range, or moving averages — it returns ~20 fields instead of 120+ and uses far fewer tokens.
- Use `get_company_profile` only when you need deep fundamentals, business description, or fields not in get_market_quote. For ETFs or mutual funds, use `get_fund_profile` instead.
- **Prefer `analyze_financial_ratios`** over fetching full financial statements when you need valuation or profitability ratios — ratios are pre-computed server-side.
- **Prefer `get_analyst_consensus`** over `get_analyst_recommendations` when you need a quick summary of analyst sentiment and price targets.
- **Prefer `get_earnings_analysis`** to get all forward-looking analyst estimates in a single call instead of five separate calls.
- Use `get_short_interest` or `get_short_momentum` for short-selling metrics.
- Use `get_technical_indicators` for momentum signals (RSI-14, MACD) without fetching raw OHLCV history.
- Use `search_ticker` to resolve a company name or ISIN to a ticker symbol before calling other tools.
- Use `screen_stocks` to discover stocks matching criteria (e.g., day_gainers, most_actives) without iterating tickers manually.
- Index tickers like `^VIX`, `^GSPC`, `^DJI` are supported by `get_market_quote`, `analyze_price_performance`, and `get_technical_indicators`.
- For SEC data: use `extract_sec_filing_fact` first for XBRL-tagged facts. If it returns NOT_DISCLOSED, use `search_sec_filing_text` with `return_tables=true` as fallback.
- For news: use `get_company_news` (multi-source) instead of `get_yahoo_finance_news` (legacy single-source).

## Available tools

### Snapshot & diagnostics
- get_market_snapshot: One-call market-state packet: price, range, MA trend, volume, RSI, MACD, liquidity gate, freshness. Compact (default, max 5 tickers) or full (max 2) modes.
- get_manifest_diagnostics: Deployment diagnostics: tool count, manifest hash, build SHA, deploy time, canonical/deprecated counts, connector-staleness advisory.
- health_check: Runtime health metadata: server version, tool count, manifest hash, envelope V2 status.

### Price & market data
- get_market_quote: Current price, market cap, 52-week range, moving averages, volume (~20 fields). Pre/after-market prices when available.
- get_historical_prices: Historical OHLCV data with configurable period, interval, and optional columns filter.
- analyze_price_performance: % distance from 52w high/low and MAs, 30d annualised volatility, CAGR.
- analyze_moving_average_position: % vs 50DMA/200DMA, trend (BULLISH/BEARISH/MIXED).
- analyze_volume_ratio: Volume vs 10d/90d averages, volumeFlag (HIGH/NORMAL/LOW).
- check_volume_liquidity_threshold: 20d ADV liquidity gate pass/fail. FX notional mode via foreign_exchange=true.
- get_technical_indicators: Pre-computed RSI-14 (Wilder) and MACD (12,26,9) from daily closes.
- get_price_slope: N-day price slope (% change) and direction (UP/DOWN/FLAT).
- get_short_interest: Short % of float, shares short, days-to-cover, prior-month comparison.
- get_short_momentum: Short interest with MoM delta, direction (RISING/FALLING/FLAT), squeeze risk.
- get_overnight_quote: Overnight trading data (20:00–04:00 ET). Returns price, gap %, data source, and staleness flag.

### Company fundamentals
- get_company_profile: ~30 key fundamental fields by default. Pass include_all=true for full ~120-field payload. For ETFs/funds, use get_fund_profile instead.
- get_fund_profile: ETF/mutual fund data — NAV, expense ratio, AUM, YTD return, top-10 holdings, sector weights.
- get_financial_statement: Income statement, balance sheet, or cash flow (annual/quarterly/TTM). Optional line_items filter.
- analyze_financial_ratios: Pre-computed P/E, PEG, P/S, P/B, EV/EBITDA, margins, ROE, ROA, debt ratios.
- analyze_credit_health: Net Debt/EBITDA, interest coverage, debt tier, credit stress flag.
- get_corporate_actions: Dividends and splits history.
- get_ownership_holders: Major holders, institutional holders, mutual funds, or insider transactions.

### Analyst & forecasts
- get_analyst_consensus: Compact analyst price targets (current/low/high/mean/median + % upside) and rating breakdown.
- get_earnings_analysis: All analyst forward-looking data: EPS/revenue estimates, trend, history, growth.
- get_analyst_recommendations: Raw analyst recommendations or upgrades/downgrades history.
- get_analyst_rating_changes: Recent rating changes with signal (UPGRADE/DOWNGRADE/MAINTAIN), price target direction, net sentiment.
- analyze_earnings_momentum: EPS revision momentum (7/30/90d), revision direction, beat rate, beat streak.
- get_company_events_calendar: Next earnings date (confirmed vs estimated), ex-dividend/pay dates.

### Options
- get_option_expiration_dates: Available options expiration dates.
- get_option_chain: Options chain for a specific expiry and type. Supports strike filters and in_the_money_only.
- summarize_options_flow: Put/call ratio, P/C sentiment, ATM IV, IV percentile, max pain, highest OI strikes.
- find_put_hedge_candidates: Pre-filtered OTM puts within configurable OTM % range and budget.
- analyze_options_flow_window: Structured event-window options scan with 72h cached trend.

### SEC filings
- list_sec_company_filings: List SEC filings from EDGAR submissions. Returns filing type, date, accession number, document URL.
- get_sec_filing_outline: Section/heading outline of an SEC filing.
- get_sec_filing_section: Text of a specific filing section.
- list_sec_filing_tables: Tables present in an SEC filing.
- get_sec_filing_table: Specific table from an SEC filing by index.
- extract_sec_filing_fact: Extract a specific XBRL fact from a filing (try this first for GAAP line items).
- search_sec_filing_text: Search narrative filing HTML text. Use as fallback when extract_sec_filing_fact returns NOT_DISCLOSED.
- index_sec_filing: Build a deterministic section/table index for a filing (cached 24h).
- get_sec_filing_index: Get the pre-built filing index.
- query_sec_filing_index: Route SEC filing query types to index-backed extractors.

### SEC structured extractors
- extract_geographic_revenue: Geographic revenue exposure with evidence.
- extract_segment_revenue: Segment revenue rows from SEC facts.
- extract_total_revenue: Total revenue from SEC facts.
- extract_revenue_exposure: Revenue exposure for a region/customer/segment query.
- extract_china_exposure: China exposure with revenue and non-revenue classifications.
- extract_risk_factor_mentions: Risk-factor term mentions from SEC filings.
- extract_customer_concentration: Customer concentration percentages.

### Public news & events
- get_company_news: Recent public company news from Yahoo Finance, Finnhub, and SEC sources. Multi-source with sourceStatus.
- search_company_news: Search news with keyword filter.
- get_company_press_releases: 8-K press releases as structured public events.
- get_sec_recent_events: Recent SEC filings as structured public events.
- get_public_event_timeline: Deduplicated chronological timeline across all sources.
- verify_company_event: Cross-validate an event across sources: CONFIRMED/PARTIAL/NOT_FOUND/STALE/CONFLICTING.

### Earnings intelligence
- get_latest_earnings_release: Find the latest earnings release evidence from SEC 8-K, IR, or Yahoo.
- index_earnings_release: Build a section/table index for an earnings release.
- extract_earnings_metrics: Extract reported revenue, EPS, gross margin, operating income, FCF, capex.
- extract_guidance: Extract company-provided guidance ranges.
- extract_management_commentary: Extract topic-specific management commentary with evidence excerpts.
- compare_earnings_actual_vs_estimate: Compare actual earnings vs analyst estimates with surprise %.

### Discovery & position
- search_ticker: Resolve company name or ISIN to ticker symbol.
- screen_stocks: Screen with predefined criteria (day_gainers, most_actives, undervalued_large_caps, etc.).
- analyze_position_signals: Aggregate public market, analyst, earnings, and technical inputs for caller-defined scoring.
- calculate_price_target_distance: Compare current price to a user-supplied reference target.
""",
)
