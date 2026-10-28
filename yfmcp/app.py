"""Central MCP application instance.

This module is the single source of truth for:
- ``YfmcpServer``: the ``MCPServer`` subclass every tool is registered on. It overrides the public ``tool()`` and
  ``list_tools()`` methods only (read-only annotations, thread offload, tool-call gate and envelope; own ordered
  registry; ticker-cap and grouped schemas applied to the listing). No SDK class is patched and no SDK
  private attribute is read (Q8, 2.13.1).
- The shared ``yfinance_server`` instance that all domain modules register on
- ``build_handler_registry`` / ``build_tool_contract_registry``: derive the handler and input-schema maps from the
  registry for grouped mode

All ``yfmcp/tools/*.py`` domain modules import ``yfinance_server`` from here.
``server.py`` also imports it from here (never the reverse) to avoid circular imports.
"""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import functools
import inspect
from typing import Any, Callable

try:
    from mcp.server.mcpserver import MCPServer
    from mcp.server.mcpserver.tools import Tool as _SdkTool
    from mcp.types import Tool as MCPTool, ToolAnnotations
except ImportError as exc:  # Q8: no 1.x fallback; fail loudly with the supported range.
    raise ImportError(
        "yahoo-finance-mcp requires the MCP Python SDK in the range mcp[cli]>=2.2,<3 "
        "(mcp.server.mcpserver.MCPServer); install it with: pip install 'mcp[cli]>=2.2,<3'"
    ) from exc

from yfmcp.build_info import BUILD_VERSION
from yfmcp import url_policy
from yfmcp.envelope import _budget_guarded_result, _envelope_tool_result
from yfmcp.validation import TICKER_CAPS, validate_tool_args

_READ_ONLY_ANNOTATION_VALUES = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": True,
}
READ_ONLY_TOOL_ANNOTATIONS = ToolAnnotations(**_READ_ONLY_ANNOTATION_VALUES)
_CLOSED_WORLD_ANNOTATION_VALUES = {
    **_READ_ONLY_ANNOTATION_VALUES,
    "openWorldHint": False,
}
CLOSED_WORLD_TOOL_ANNOTATIONS = ToolAnnotations(**_CLOSED_WORLD_ANNOTATION_VALUES)
_CLOSED_WORLD_TOOL_NAMES = {
    "system",
    "health_check",
}


def _call_arguments(fn: Callable[..., Any], args: tuple, kwargs: dict[str, Any]) -> dict[str, Any]:
    """The call's arguments by parameter name, for the tool-call gate."""
    if not args:
        return kwargs
    try:
        return dict(inspect.signature(fn).bind_partial(*args, **kwargs).arguments)
    except TypeError:
        return kwargs


def _offload_to_thread(fn: Callable[..., Any], tool_name: str) -> Callable[..., Any]:
    """Run each MCP invocation of an async tool on a worker thread.

    Tool bodies call blocking yfinance and urllib APIs directly. Running the
    invocation in its own event loop on a worker thread keeps the stdio
    server's loop responsive and lets concurrent requests proceed in parallel.
    Module-level names keep the original coroutine function, so direct calls
    and grouped routing (see ``build_handler_registry``) are unaffected. The
    MCP response gets the same V2 envelope the Worker applies; grouped results
    arrive already enveloped by the router and pass through unchanged.

    One MCP invocation is one subrequest-budget scope (2.11.0, ``yfmcp.url_policy``): the counter starts fresh
    for every call, and a grouped dispatch is a single call because the router reaches the handlers directly.
    """

    def run(*args: Any, **kwargs: Any) -> Any:
        with url_policy.invocation_scope():
            # The tool-call gate (2.12.0, Q3b/Q3c): central ticker/accession validation and per-tool ticker caps,
            # before any handler code or fetch. Grouped dispatch runs it in tool_groups._route_grouped_call.
            gate_failure = validate_tool_args(tool_name, _call_arguments(fn, args, kwargs))
            if gate_failure is not None:
                result = _envelope_tool_result(tool_name, gate_failure)
            else:
                result = _envelope_tool_result(tool_name, asyncio.run(fn(*args, **kwargs)))
            return _budget_guarded_result(
                tool_name,
                result,
                refusals=url_policy.budget_refusal_count(),
                exhausted=url_policy.subrequest_budget_exhausted(),
            )

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(run, *args, **kwargs)

    return wrapper


# ---------------------------------------------------------------------------
# Server subclass: public tool()/list_tools() overrides and our own tool registry
# ---------------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class ToolRegistration:
    """One tool registered through ``YfmcpServer.tool``."""

    name: str
    fn: Callable[..., Any]  # the handler as written in the domain module (grouped routing calls this)
    registered: Callable[..., Any]  # what the SDK invokes: ``fn`` wrapped for thread offload, gate and envelope
    sdk_kwargs: dict[str, Any]  # the keyword arguments passed on to ``MCPServer.tool``
    after: str | None  # list this tool directly after the named tool (see ``YfmcpServer.tool``)


class YfmcpServer(MCPServer):
    """``MCPServer`` with the project's registration and listing rules, built only on public SDK methods."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._yfmcp_tools: dict[str, ToolRegistration] = {}
        self._yfmcp_schema_overrides: dict[str, dict[str, Any]] = {}
        super().__init__(*args, **kwargs)

    # -- registration -------------------------------------------------------
    def tool(  # type: ignore[override]
        self,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        annotations: ToolAnnotations | None = None,
        icons: list[Any] | None = None,
        meta: dict[str, Any] | None = None,
        structured_output: bool | None = None,
        *,
        after: str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a tool: read-only metadata, thread offload for async handlers, entry in our registry.

        ``after`` places the tool directly after another registered tool in ``tools/list`` (resolved when
        listing, so registration order across modules does not matter). The decorator returns the
        original function, so module-level names and grouped routing keep the un-wrapped handler.
        """
        if annotations is None:
            annotations = (
                CLOSED_WORLD_TOOL_ANNOTATIONS if name in _CLOSED_WORLD_TOOL_NAMES else READ_ONLY_TOOL_ANNOTATIONS
            )
        sdk_kwargs: dict[str, Any] = {
            "name": name,
            "title": title,
            "description": description,
            "annotations": annotations,
            "icons": icons,
            "meta": meta,
            "structured_output": structured_output,
        }
        register = super().tool(**sdk_kwargs)  # also rejects a bare ``@server.tool`` (no call)

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            tool_name = name or fn.__name__
            registered = _offload_to_thread(fn, tool_name) if inspect.iscoroutinefunction(fn) else fn
            register(registered)
            # The SDK keeps the first registration of a name; so does the registry.
            if tool_name not in self._yfmcp_tools:
                self._yfmcp_tools[tool_name] = ToolRegistration(tool_name, fn, registered, sdk_kwargs, after)
            return fn

        return decorator

    def set_input_schema(self, name: str, schema: dict[str, Any]) -> None:
        """Advertise ``schema`` as the listed ``inputSchema`` of a registered tool (grouped meta-tools)."""
        if name not in self._yfmcp_tools:
            raise RuntimeError(f"Cannot set the input schema of '{name}': tool is not registered")
        self._yfmcp_schema_overrides[name] = copy.deepcopy(schema)

    # -- registry reads (sync; fail loudly) ---------------------------------
    def registrations(self) -> list[ToolRegistration]:
        """Registered tools in listing order. Raises when nothing is registered or an ``after`` target is missing."""
        if not self._yfmcp_tools:
            raise RuntimeError("No tools are registered on this server")
        return self._ordered_registrations()

    def _ordered_registrations(self) -> list[ToolRegistration]:
        names = list(self._yfmcp_tools)
        for name, registration in self._yfmcp_tools.items():
            if registration.after is None:
                continue
            if registration.after not in self._yfmcp_tools:
                raise RuntimeError(f"Tool '{name}' is to be listed after '{registration.after}', which is not registered")
            names.remove(name)
            names.insert(names.index(registration.after) + 1, name)
        return [self._yfmcp_tools[name] for name in names]

    def registration(self, name: str) -> ToolRegistration:
        try:
            return self._yfmcp_tools[name]
        except KeyError:
            raise RuntimeError(f"Tool '{name}' is not registered") from None

    def tool_specs(self) -> list[dict[str, Any]]:
        """Listing-order specs (name, description, parameters, output_schema) as ``tools/list`` advertises them."""
        specs: list[dict[str, Any]] = []
        for registration in self.registrations():
            sdk_tool = _SdkTool.from_function(registration.registered, **registration.sdk_kwargs)
            spec: dict[str, Any] = {
                "name": registration.name,
                "description": str(sdk_tool.description or ""),
                "parameters": self._listing_schema(registration.name, sdk_tool.parameters),
            }
            if sdk_tool.output_schema is not None:
                spec["output_schema"] = copy.deepcopy(sdk_tool.output_schema)
            specs.append(spec)
        return specs

    # -- listing ------------------------------------------------------------
    def _listing_schema(self, name: str, schema: dict[str, Any]) -> dict[str, Any]:
        """The ``inputSchema`` advertised for ``name``: a grouped override, else the schema with tickerCaps applied.

        Listing only: the argument model is untouched, so an oversize array still reaches the tool-call gate and
        gets BATCH_LIMIT_EXCEEDED (2.12.0, Q3c). get_market_snapshot advertises its compact cap; the full-mode cap
        is enforced by the gate (as in the Worker).
        """
        override = self._yfmcp_schema_overrides.get(name)
        if override is not None:
            return copy.deepcopy(override)
        cap = TICKER_CAPS.get(name) if "." not in name else None
        patched = copy.deepcopy(schema)
        if cap is None:
            return patched
        ticker = patched.get("properties", {}).get("ticker")
        array_branch = next(
            (b for b in (ticker or {}).get("anyOf", []) + (ticker or {}).get("oneOf", []) if b.get("type") == "array"),
            None,
        )
        if array_branch is None:
            raise RuntimeError(f"tickerCaps names '{name}', which has no ticker array schema")
        array_branch["maxItems"] = cap
        return patched

    async def list_tools(self) -> list[MCPTool]:
        """``tools/list`` in registry order, with ticker caps and grouped schemas applied to ``inputSchema``."""
        listed = await super().list_tools()
        by_name = {tool.name: tool for tool in listed}
        ordered = [by_name[r.name] for r in self._ordered_registrations() if r.name in by_name]
        ordered += [tool for tool in listed if tool.name not in self._yfmcp_tools]
        return [
            tool.model_copy(update={"input_schema": self._listing_schema(tool.name, tool.input_schema)})
            for tool in ordered
        ]


def registered_handler(name: str, server: YfmcpServer | None = None) -> Callable[..., Any]:
    """The handler function as written (no thread offload, gate or envelope); ``server`` defaults to ``yfinance_server``."""
    return (server or yfinance_server).registration(name).fn


def registered_tool_fn(name: str, server: YfmcpServer | None = None) -> Callable[..., Any]:
    """The callable the SDK invokes for ``name`` (thread offload, gate, envelope); ``server`` defaults to ``yfinance_server``."""
    return (server or yfinance_server).registration(name).registered



# ---------------------------------------------------------------------------
# Shared server instance
# ---------------------------------------------------------------------------
def create_server(name: str, instructions: str) -> YfmcpServer:
    """Build a server whose MCP ``initialize`` reports this build's version."""
    return YfmcpServer(name, instructions=instructions, version=BUILD_VERSION)


yfinance_server = create_server(
    "yfinance",
    instructions="""
# Yahoo Finance MCP Server

This server provides financial market data from Yahoo Finance and SEC EDGAR via canonical tool names.

## Tool selection guidance
- **Prefer `get_market_snapshot`** for a one-call market overview (price, range, MA trend, volume, RSI, MACD, liquidity gate, freshness). Supports compact and full modes (max 5 tickers per call in either).
- **Prefer `get_market_quote`** over `get_company_profile` for current price, market cap, 52-week range, or moving averages — it returns ~20 fields instead of 120+ and uses far fewer tokens.
- Use `get_company_profile` only when you need deep fundamentals, business description, or fields not in get_market_quote. For ETFs or mutual funds, use `get_fund_profile` instead.
- **Prefer `analyze_financial_ratios`** over fetching full financial statements when you need current ratios; request history_periods only for valuation history.
- Use `analyze_share_count_trend` automatically for dilution, issuance, buyback, or shares-outstanding trend questions, then confirm material changes in SEC filings.
- Use `get_market_calendar` automatically for market-wide earnings, economic-event, IPO, or split schedules. Use `get_company_events_calendar` for one ticker.
- **Prefer `get_analyst_consensus`** over `get_analyst_recommendations` when you need a quick summary of analyst sentiment and price targets.
- **Prefer `get_earnings_analysis`** to get all forward-looking analyst estimates in a single call instead of five separate calls.
- Use `get_short_interest` or `get_short_momentum` for short-selling metrics.
- Use `get_technical_indicators` for completed-session momentum signals (RSI-14, MACD) without fetching raw OHLCV history. Read bar/freshness status before use.
- Raw daily history may include an unfinished active row; check `barStatus`/`isFinal`. Volume ratios, liquidity gates, volatility, CAGR, and realized-volatility context exclude that row.
- Moving-average position and target-distance tools intentionally use a live regular-market quote and expose `priceTimestamp`; do not compare them to a completed close without aligning observation dates.
- Use `search_ticker` to resolve a company name or ISIN to a ticker symbol before calling other tools.
- Use `screen_stocks` to discover stocks matching criteria (e.g., day_gainers, most_actives) without iterating tickers manually.
- Index tickers like `^VIX`, `^GSPC`, `^DJI` are supported by `get_market_quote`, `analyze_price_performance`, and `get_technical_indicators`.
- For SEC data: use `extract_sec_filing_fact` first for XBRL-tagged facts. If it returns NOT_DISCLOSED, use `search_sec_filing_text` with `return_tables=true` as fallback.
- For news: use `get_company_news` (multi-source routing with coverage diagnostics).
- For news completeness, read `coverage.truncatedSources` as well as failed/skipped sources. `RETRY_TRUNCATED_SOURCE` means accepted items were omitted by caps or dedupe, not that the provider failed.
- Use `get_ownership_holders` for ordinary Yahoo holder questions. Use `get_expanded_institutional_ownership` only for a deeper list; it tries eligible Finnhub coverage first and spends scarce Alpha quota only with `allow_scarce_fallback=true`.
- Use `summarize_options_flow` for current options context. Use `get_historical_put_call_ratio` only for one explicit historical date; the Alpha-backed result is contextual and never decision-grade.

## Available tools

### Snapshot & diagnostics
- get_market_snapshot: One-call market-state packet: price, range, MA trend, volume, RSI, MACD, liquidity gate, freshness. Compact (default) or full modes, max 5 tickers per call.
- health_check: Public-safe MCP availability, release/build identity, schema identity, tool mode, and connector-freshness metadata.

### Price & market data
- get_market_quote: Current regular-market price observation with priceBasis and priceTimestamp, plus market cap, 52-week range, moving averages, and volume.
- get_historical_prices: Historical OHLCV data; daily rows without a usable close are INCOMPLETE/isFinal=false.
- analyze_price_performance: % distance from 52w high/low and MAs, 30d annualised volatility, CAGR.
- analyze_moving_average_position: % vs 50DMA/200DMA, trend (BULLISH/BEARISH/MIXED).
- analyze_volume_ratio: Completed-session volume vs 10d/90d averages. PARTIAL/INCOMPLETE means retry and do not use volumeFlag.
- check_volume_liquidity_threshold: Completed-session 20d ADV liquidity gate. PARTIAL/INCOMPLETE means the average is unknown. Reports adv20dTradedValueUsd; pass min_adv_usd for a caller-chosen threshold and read meetsMinimum.
- get_technical_indicators: Pre-computed RSI-14 (Wilder) and MACD (12,26,9) from daily closes.
- get_price_slope: N completed-session close-to-close change using N+1 bars. Excludes unfinished bars and returns same-bar endRawClose, priceBasis, freshness/retry diagnostics, dataDate, and direction (UP/DOWN/FLAT). Not a real-time quote.
- get_short_interest: Short % of float, shares short, days-to-cover, prior-month comparison.
- get_short_momentum: Short interest with MoM delta, direction (RISING/FALLING/FLAT), squeeze risk.

### Company fundamentals
- get_company_profile: ~30 key fundamental fields by default. Pass include_all=true for full ~120-field payload. For ETFs/funds, use get_fund_profile instead.
- get_fund_profile: ETF/mutual fund data with section dates/status. Valuation characteristics are conventional multiples; raw provider yields are retained. Request sections: overview, holdings, allocation, operations, fixed_income.
- get_financial_statement: Income statement, balance sheet, or cash flow (annual/quarterly/TTM). Optional line_items filter.
- analyze_financial_ratios: Current P/E, PEG, P/S, P/B, EV/EBITDA, margins, ROE, ROA, debt ratios with explicit unitSemantics; optional valuation history.
- get_valuation_snapshot: Diluted shares, period-end cash/debt, enterprise value and multiples on trailing and consensus figures; context, not a target.
- compare_peer_valuations: The same multiples for a peer set on one Yahoo basis, with peer medians.
- get_historical_valuation_context: Point-in-time market cap, EV and multiples at dates for a ticker and caller-named peers, with named LTM/LFY denominators; no multiple selected.
- get_share_count_scenarios: Share counts under caller-defined scenarios (price, per-instrument treatment, known issuance); every instrument included/excluded/unresolved with mechanics; no denominator selected.
- extract_funding_capex_schedule: Contractual schedules by due period and funding/capex statements classified CONTRACTUAL, COMPANY_DISCLOSED_COMMITTED, COMPANY_GUIDED, AWARDED_CONTINGENT or UNRESOLVED, with timing and source.
- extract_operating_driver_ledger: Operating drivers as disclosed: XBRL driver series, the filer's own tagged KPIs, and filing/release statements with figures by category, each REPORTED_ACTUAL, TARGET_OR_PLAN or UNCLEAR with timing and source; nothing derived.
- analyze_share_count_trend: Historical shares outstanding for dilution/buyback questions; Yahoo context, not filing confirmation.
- analyze_credit_health: Net Debt/EBITDA, interest coverage, debt tier, credit stress flag.
- get_corporate_actions: Dividends, splits, and fund capital-gain distribution history.
- get_ownership_holders: Major holders, institutional holders, mutual funds, or insider transactions.
- get_expanded_institutional_ownership: Deeper institutional ownership via eligible Finnhub coverage; optional Alpha fallback requires allow_scarce_fallback=true. Verify material claims against SEC 13F filings.

### Analyst & forecasts
- get_analyst_consensus: Compact analyst price targets (current/low/high/mean/median + % upside) and rating breakdown.
- get_earnings_analysis: EPS/revenue estimates, trend, analyst up/down revision counts, history, and growth.
- get_analyst_recommendations: Raw analyst recommendations or upgrades/downgrades history.
- get_analyst_rating_changes: Recent rating changes with signal (UPGRADE/DOWNGRADE/MAINTAIN), price target direction, net sentiment.
- extract_analyst_valuation_methods: Valuation methods named in news (multiples, DCF inputs) with firm and link; context only.
- analyze_earnings_momentum: EPS revision momentum (7/30/90d), revision direction, beat rate, beat streak.
- get_consensus_forecast_curve: FY0-FY+5 EPS/revenue consensus per provider with coverage per cell (PROVIDER_COVERED, PROVIDER_NOT_COVERED, INSUFFICIENT_ANALYST_COUNT, PROVIDER_CONFLICT); gaps stay visible; evidence only.
- get_eps_revisions: FY0/FY+1 EPS mean now and 7/30/60/90 days ago, up/down revision counts per provider; evidence only.

### Evidence cuts
- build_valuation_evidence_pack: Quote, evidence quality, consensus curve, EPS revisions, capital structure, dilution at the current price, guidance and material filings in one content-addressed evidence cut with a provenance receipt. decisionUse EVIDENCE_ONLY.
- get_evidence_quality: Preflight status, freshness and blockers per evidence family.
- get_evidence_cut: Retrieve a stored evidence cut and verify its SHA-256 integrity.
- list_evidence_cuts: Stored evidence cuts for a ticker, newest first.
- reconcile_metric_sources: One metric for one period across SEC XBRL, the issuer release and Yahoo; AGREED, PARTIAL, CONFLICT or NOT_FOUND with variances and evidence.
- get_company_events_calendar: Upcoming Yahoo provider dates or earnings history; dates are UNVERIFIED.
- get_market_calendar: Market-wide earnings, economic, IPO, or stock-split calendar with pagination.

### Options
- get_option_expiration_dates: Available options expiration dates.
- get_option_chain: Options chain for a specific expiry and type. Supports strike_min/strike_max filters and moneyness (near_money, itm, otm, all).
- summarize_options_flow: Put/call ratio, P/C sentiment, ATM IV, IV percentile, max pain, highest OI strikes.
- get_historical_put_call_ratio: Explicit dated historical put/call ratio. Uses scarce Alpha quota, never substitutes for the current Yahoo snapshot, and is decisionGrade=false.
- find_put_hedge_candidates: Pre-filtered OTM puts with two-sided/liquidity evidence; cost and budget use executable ask.
- analyze_options_flow_window: Structured event-window options scan with 72h cached trend; LOW quality requires retry.

### SEC filings
- list_sec_company_filings: List SEC filings from EDGAR submissions. Returns filing type, date, accession number, document URL.
- get_sec_filing_outline: Section/heading outline of an SEC filing.
- get_sec_filing_section: Text of a specific filing section.
- list_sec_filing_tables: Tables present in an SEC filing.
- get_sec_filing_table: Specific table from an SEC filing by index.
- extract_sec_filing_fact: Extract a specific XBRL fact from a filing (try this first for GAAP line items).
- search_sec_filing_text: Search narrative filing HTML text. Use as fallback when extract_sec_filing_fact returns NOT_DISCLOSED.
- get_sec_filing_index: Build or retrieve the cached deterministic section/table index for a filing.
- query_sec_filing_index: Route SEC filing query types to index-backed extractors.

### SEC structured extractors
- extract_geographic_revenue: Geographic revenue exposure with evidence.
- extract_segment_revenue: Segment revenue rows from SEC facts.
- extract_total_revenue: Total revenue from SEC facts.
- extract_revenue_exposure: Revenue exposure for a region/customer/segment query.
- extract_china_exposure: China exposure with revenue and non-revenue classifications.
- extract_risk_factor_mentions: Risk-factor term mentions from SEC filings.
- extract_customer_concentration: Customer concentration percentages.
- extract_dilution_bridge: Basic-to-diluted shares at a price you supply, from inline XBRL (mechanical, not consensus).
- extract_capital_structure: Cash, debt, instruments, maturity ladder and quoted funding statements at period end.

### Public news & events
- get_company_news: Recent public company news with strict Yahoo issuer matching. Read coverage/sourceStatus first; Yahoo items require tickerMatch=EXPLICIT and expose matchBasis plus raw/accepted/rejected diagnostics. Escalate decisionUse=CHECK_OFFICIAL_RELEASES to official releases or event verification.
- search_company_news: Search news with keyword filter.
- get_company_press_releases: 8-K press releases as structured public events.
- get_sec_recent_events: Recent SEC filings as structured public events.
- get_public_event_timeline: Deduplicated chronological timeline across all sources.
- verify_company_event: Cross-validate an event across sources: CONFIRMED/PARTIAL/NOT_FOUND/STALE/CONFLICTING.
- get_uk_company_filings: UK Companies House statutory filings (accounts, SH01, MR01 charges) with document links.

### Earnings intelligence
- get_latest_earnings_release: Find the latest earnings release evidence from SEC 8-K, IR, or Yahoo.
- index_earnings_release: Build a section/table index for an earnings release.
- extract_earnings_metrics: Extract reported revenue, EPS, gross margin, operating income, FCF, capex.
- extract_guidance: Extract company-provided guidance ranges.
- get_guidance_history: Guidance ranges across recent earnings releases with target period, revisions between releases and outcomes against the later reported SEC actual; unread releases stay visible.
- extract_management_commentary: Extract topic-specific management commentary with evidence excerpts.
- compare_earnings_actual_vs_estimate: Compare actual earnings vs analyst estimates with surprise %.

### Discovery & position
- search_ticker: Resolve company name or ISIN to ticker symbol.
- screen_stocks: Screen with predefined criteria (day_gainers, most_actives, undervalued_large_caps, etc.).
- analyze_position_signals: Aggregate public market, analyst, earnings, and technical inputs for caller-defined scoring.
- calculate_price_target_distance: Compare current price to a user target; separates the legacy ratio from directional percent distance.
""",
)


def build_handler_registry(*servers: YfmcpServer) -> dict[str, Callable[..., Any]]:
    """Map handler function name -> function for every tool registered on ``servers``.

    Reads the server's own registry so the mapping always reflects the live set
    of registered tools, independent of where the handlers are defined, and fails
    loudly (RuntimeError) when a server has none. Used by grouped mode to route
    each action name (e.g. "get_market_quote") to the canonical tool function of
    the same name. The original handler is returned, not the thread-offloaded
    wrapper, so an action call does not hop threads twice.
    """
    registry: dict[str, Callable[..., Any]] = {}
    for server in servers:
        for registration in server.registrations():
            registry[registration.fn.__name__] = registration.fn
    return registry


def build_tool_contract_registry(*servers: YfmcpServer) -> dict[str, dict[str, Any]]:
    """Map handler function name to its listed input schema (tickerCaps ``maxItems`` included)."""
    registry: dict[str, dict[str, Any]] = {}
    for server in servers:
        for registration, spec in zip(server.registrations(), server.tool_specs()):
            registry[registration.fn.__name__] = spec["parameters"]
    return registry
