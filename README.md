# Yahoo Finance MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server that gives any MCP-compatible AI client (Claude, Cursor, VS Code Copilot, etc.) direct access to live public financial data from Yahoo Finance and SEC EDGAR.

> **Source of truth:** The live MCP tool manifest exposed by the running server is the canonical reference for tool names, input schemas, and deprecation status. Use `get_manifest_diagnostics` to verify the deployed tool set at runtime.

---

## Live Deployment Status

Last checked against `https://yahoo-finance-mcp.artinatw.workers.dev/mcp` on 2026-06-30.

| Field | Live value |
|-------|------------|
| Server version | `1.4.1` |
| Expanded canonical tools | `73` |
| Deprecated aliases | `31` |
| Grouped meta-tools | `10` when `TOOL_MODE=grouped` |
| Privacy scope | `public_market_data_only` |
| Structured SEC facts provider | `official_sec_data_api` |
| Structured SEC facts health | `OK` |
| Quarantined/degraded tools | `5` |
| Opaque response count in tool-by-tool envelope probe | `0` |

The tool-by-tool probe verifies that every expanded tool is discoverable through live `tools/list` and returns a JSON MCP envelope instead of opaque text. It is an interface and safety check, not a guarantee that every possible ticker, filing, provider, or argument combination has fresh data. Provider rate limits, market-data entitlements, and SEC availability can still affect individual calls.

Live smoke policy and PR checklist guidance lives in [`docs/live-smoke-policy.md`](docs/live-smoke-policy.md). Tool/provider changes should update smoke expectations in the same PR and avoid hardcoded volatile market dates or live provider values.

Representative live smoke calls also passed for:

| Tool | Live result |
|------|-------------|
| `health_check` | `ok:true`, `serverVersion: "1.4.1"` |
| `get_manifest_diagnostics` | `ok:true`, reports canonical/deprecated counts and doctrine status metadata |
| `get_market_quote(AAPL)` | `ok:true` |
| `get_fast_info(AAPL)` | `ok:true`, `deprecatedTool:true`, `DEPRECATED_ALIAS` warning |
| `get_historical_stock_prices({})` | `ok:false`, `INPUT_VALIDATION_ERROR`, `DEPRECATED_ALIAS` warning |
| `get_market_snapshot(AAPL)` | `ok:true` |
| `get_option_expiration_dates(AAPL)` | `ok:true` |
| `summarize_options_flow(AAPL)` | `ok:true` |
| `get_company_news(AAPL)` | `ok:true` |
| `list_sec_company_filings(AAPL, 10-K)` | `ok:true` |
| `query_sec_filing_index` unsupported query | `ok:false`, `UNSUPPORTED_QUERY_TYPE` |
| `get_overnight_quote(AAPL)` | `ok:true`, Yahoo indicative/pre-post-market data, `DIAGNOSTICS_ONLY` |
| `extract_geographic_revenue(AAPL, Greater China)` | JSON envelope returned through `official_sec_data_api`; provider availability is reported explicitly |

### Quarantined Or Degraded Tools

These tools remain visible for compatibility and diagnostics, but their `meta` status tells clients not to treat them as decision-grade without external verification.

| Tool | Capability status | Doctrine use | Decision grade | Failure mode |
|------|-------------------|--------------|----------------|--------------|
| `get_overnight_quote` | `DEGRADED` | `DIAGNOSTICS_ONLY` | `false` | `TRUE_OVERNIGHT_PROVIDER_REMOVED` |
| `get_sec_filing_section_markdown` | `DEGRADED` | `BLOCKED` | `false` | `LIVE_SECTION_EXTRACTION_UNRELIABLE`; successful payloads are `SECTION_MARKDOWN_UNVERIFIED` |
| `get_company_press_releases` | `DEGRADED` | `VERIFY_ONLY` | `false` | `SEC_EX99_LINKAGE_INCOMPLETE` |
| `query_sec_filing_index` | `DEGRADED` | `VERIFY_ONLY` | `false` | `ENVELOPE_SEMANTICS_UNDER_VERIFICATION` |
| `extract_sec_filing_fact` | `DEGRADED` | `VERIFY_ONLY` | `false` | XBRL context metadata is included when available; otherwise `XBRL_CONTEXT_METADATA_UNAVAILABLE` |

### Tool-By-Tool Live Status

`Supported` means the tool is present in live discovery and returned a standard JSON envelope in the tool-by-tool probe. Quarantined entries expose stricter runtime metadata in `meta.capabilityStatus`, `meta.doctrineUse`, and `meta.decisionGrade`.

| Domain | Tool | Live status |
|--------|------|-------------|
| Price & Market Data | `get_market_quote` | Supported |
| Price & Market Data | `get_historical_prices` | Supported |
| Price & Market Data | `analyze_price_performance` | Supported |
| Price & Market Data | `analyze_moving_average_position` | Supported |
| Price & Market Data | `analyze_volume_ratio` | Supported |
| Price & Market Data | `check_volume_liquidity_threshold` | Supported |
| Price & Market Data | `get_technical_indicators` | Supported |
| Price & Market Data | `get_price_slope` | Supported |
| Price & Market Data | `get_short_interest` | Supported |
| Price & Market Data | `get_short_momentum` | Supported |
| Price & Market Data | `get_overnight_quote` | `DEGRADED` / `DIAGNOSTICS_ONLY` |
| Price & Market Data | `get_market_snapshot` | Supported |
| Company Fundamentals | `get_company_profile` | Supported |
| Company Fundamentals | `get_fund_profile` | Supported |
| Company Fundamentals | `get_financial_statement` | Supported |
| Company Fundamentals | `analyze_financial_ratios` | Supported |
| Company Fundamentals | `analyze_credit_health` | Supported |
| Company Fundamentals | `get_corporate_actions` | Supported |
| Company Fundamentals | `get_ownership_holders` | Supported |
| Analyst & Forecasts | `get_analyst_consensus` | Supported |
| Analyst & Forecasts | `get_earnings_analysis` | Supported |
| Analyst & Forecasts | `get_analyst_recommendations` | Supported |
| Analyst & Forecasts | `get_analyst_rating_changes` | Supported |
| Analyst & Forecasts | `analyze_earnings_momentum` | Supported |
| Analyst & Forecasts | `get_company_events_calendar` | Supported |
| Options | `get_option_expiration_dates` | Supported |
| Options | `get_option_chain` | Supported |
| Options | `summarize_options_flow` | Supported |
| Options | `find_put_hedge_candidates` | Supported |
| Options | `analyze_options_flow_window` | Supported |
| SEC Filings | `list_sec_company_filings` | Supported |
| SEC Filings | `list_sec_material_filings` | Supported |
| SEC Filings | `get_sec_filing_outline` | Supported |
| SEC Filings | `get_sec_filing_section` | Supported |
| SEC Filings | `get_sec_filing_section_markdown` | `DEGRADED` / `BLOCKED` |
| SEC Filings | `list_sec_filing_tables` | Supported |
| SEC Filings | `get_sec_filing_table` | Supported |
| SEC Filings | `extract_sec_filing_fact` | `DEGRADED` / `VERIFY_ONLY` |
| SEC Filings | `search_sec_filing_text` | Supported |
| SEC Filings | `index_sec_filing` | Supported |
| SEC Filings | `get_sec_filing_index` | Supported |
| SEC Filings | `get_sec_filing_intelligence` | Supported |
| SEC Filings | `query_sec_filing_index` | `DEGRADED` / `VERIFY_ONLY` |
| SEC Filings | `list_sec_filing_exhibits` | Supported |
| SEC Filings | `get_sec_filing_exhibit_content` | Supported |
| SEC Extractors | `extract_geographic_revenue` | Supported; provider failures are explicit |
| SEC Extractors | `extract_segment_revenue` | Supported; provider failures are explicit |
| SEC Extractors | `extract_total_revenue` | Supported; provider failures are explicit |
| SEC Extractors | `extract_revenue_exposure` | Supported; provider failures are explicit |
| SEC Extractors | `extract_china_exposure` | Supported; provider failures are explicit |
| SEC Extractors | `extract_risk_factor_mentions` | Supported |
| SEC Extractors | `extract_customer_concentration` | Supported |
| SEC Extractors | `extract_exposure` | Supported; provider failures are explicit |
| News & Events | `get_company_news` | Supported |
| News & Events | `search_company_news` | Supported |
| News & Events | `get_company_press_releases` | `DEGRADED` / `VERIFY_ONLY` |
| News & Events | `get_sec_recent_events` | Supported |
| News & Events | `get_public_event_timeline` | Supported |
| News & Events | `verify_company_event` | Supported |
| Earnings Intelligence | `get_latest_earnings_release` | Supported |
| Earnings Intelligence | `index_earnings_release` | Supported |
| Earnings Intelligence | `extract_earnings_metrics` | Supported |
| Earnings Intelligence | `extract_guidance` | Supported |
| Earnings Intelligence | `extract_management_commentary` | Supported |
| Earnings Intelligence | `compare_earnings_actual_vs_estimate` | Supported |
| Earnings Intelligence | `get_earnings_call_transcript` | Supported |
| Earnings Intelligence | `parse_public_transcript` | Supported |
| Discovery & Position | `search_ticker` | Supported |
| Discovery & Position | `screen_stocks` | Supported |
| Discovery & Position | `analyze_position_signals` | Supported |
| Discovery & Position | `calculate_price_target_distance` | Supported |
| Diagnostics | `health_check` | Supported |
| Diagnostics | `get_manifest_diagnostics` | Supported |

---

## Canonical Tool Names vs Legacy Aliases

Canonical tool names use neutral public language. Legacy aliases are preserved for backward compatibility but should not be used in new integrations. Deprecated aliases return `meta.deprecatedTool=true` and `meta.useInstead`.

| Canonical name | Legacy alias |
|----------------|-------------|
| `get_market_quote` | `get_fast_info` |
| `get_historical_prices` | `get_historical_stock_prices` |
| `get_company_profile` | `get_stock_info` |
| `get_fund_profile` | `get_etf_info` |
| `get_corporate_actions` | `get_stock_actions` |
| `get_ownership_holders` | `get_holder_info` |
| `analyze_price_performance` | `get_price_stats` |
| `analyze_moving_average_position` | `get_ma_position` |
| `analyze_volume_ratio` | `get_volume_ratio` |
| `check_volume_liquidity_threshold` | `get_volume_gate` |
| `analyze_financial_ratios` | `get_financial_ratios` |
| `analyze_credit_health` | `get_credit_health` |
| `get_analyst_recommendations` | `get_recommendations` |
| `get_analyst_rating_changes` | `get_analyst_upgrade_radar` |
| `analyze_earnings_momentum` | `get_earnings_momentum` |
| `get_company_events_calendar` | `get_calendar` |
| `analyze_position_signals` | `get_position_score_inputs` |
| `calculate_price_target_distance` | `get_price_target_bracket` |
| `summarize_options_flow` | `get_options_flow_summary`, `get_options_summary` |
| `analyze_options_flow_window` | `get_options_flow_scan` |
| `find_put_hedge_candidates` | `get_put_hedge_candidates` |
| `list_sec_company_filings` | `list_sec_filings` |
| `get_sec_filing_outline` | `get_filing_outline` |
| `get_sec_filing_section` | `get_filing_section` |
| `list_sec_filing_tables` | `list_filing_tables` |
| `get_sec_filing_table` | `get_filing_table` |
| `extract_sec_filing_fact` | `get_filing_data`, `extract_filing_fact` |
| `search_sec_filing_text` | `search_filing_text` |
| `get_company_news` | `get_yahoo_finance_news` |

---

## Grouped Meta-Tools Mode (LLM Token Optimization)

Set `TOOL_MODE=grouped` in the Python or Worker runtime to expose **10 domain-level meta-tools** instead of the expanded tool list, reducing LLM tool-schema token overhead by ~80–85%.

| Env var | Value | Behavior |
|---------|-------|----------|
| `TOOL_MODE` | `expanded` (default) | individual tools registered as normal |
| `TOOL_MODE` | `grouped` | 10 domain meta-tools with `action` + `params` routing |

**Grouped meta-tools:**

| Meta-tool | Domain | Actions |
|-----------|--------|---------|
| `stock_pricing` | Price, volume, technicals | `get_market_quote`, `get_historical_prices`, `analyze_price_performance`, `analyze_moving_average_position`, `analyze_volume_ratio`, `check_volume_liquidity_threshold`, `get_technical_indicators`, `get_price_slope`, `get_short_interest`, `get_short_momentum`, `get_overnight_quote`, `get_market_snapshot` |
| `stock_fundamentals` | Company fundamentals | `get_company_profile`, `get_fund_profile`, `get_financial_statement`, `analyze_financial_ratios`, `analyze_credit_health`, `get_corporate_actions`, `get_ownership_holders` |
| `analyst_data` | Analyst ratings, forecasts | `get_analyst_consensus`, `get_earnings_analysis`, `get_analyst_recommendations`, `get_analyst_rating_changes`, `analyze_earnings_momentum`, `get_company_events_calendar` |
| `options_analysis` | Options chain, flow, hedging | `get_option_expiration_dates`, `get_option_chain`, `summarize_options_flow`, `find_put_hedge_candidates`, `analyze_options_flow_window` |
| `sec_filings` | SEC EDGAR access, indexing | `list_sec_company_filings`, `list_sec_material_filings`, `get_sec_filing_outline`, `get_sec_filing_section`, `get_sec_filing_section_markdown`, `list_sec_filing_tables`, `get_sec_filing_table`, `extract_sec_filing_fact`, `search_sec_filing_text`, `index_sec_filing`, `get_sec_filing_index`, `get_sec_filing_intelligence`, `query_sec_filing_index`, `list_sec_filing_exhibits`, `get_sec_filing_exhibit_content` |
| `sec_extractors` | Structured SEC extraction | `extract_geographic_revenue`, `extract_segment_revenue`, `extract_total_revenue`, `extract_revenue_exposure`, `extract_china_exposure`, `extract_risk_factor_mentions`, `extract_customer_concentration`, `extract_exposure` |
| `news_events` | News, events, timeline | `get_company_news`, `search_company_news`, `get_company_press_releases`, `get_sec_recent_events`, `get_public_event_timeline`, `verify_company_event` |
| `earnings_intelligence` | Earnings analysis | `get_latest_earnings_release`, `index_earnings_release`, `extract_earnings_metrics`, `extract_guidance`, `extract_management_commentary`, `compare_earnings_actual_vs_estimate`, `get_earnings_call_transcript`, `parse_public_transcript` |
| `screening` | Discovery, screening | `search_ticker`, `screen_stocks`, `analyze_position_signals`, `calculate_price_target_distance` |
| `system` | Diagnostics | `health_check`, `get_manifest_diagnostics` |

**Usage in grouped mode:**

```json
{
  "tool": "stock_pricing",
  "arguments": {
    "action": "get_market_quote",
    "params": { "ticker": "AAPL" }
  }
}
```

All canonical action names and response schemas remain identical to expanded mode — only the routing interface changes.

---

## MCP Tools Reference

### Phase 8 Diagnostics & Snapshot

| Tool | Description |
|------|-------------|
| `get_manifest_diagnostics` | Return deployment diagnostics: tool count, manifest hash, build SHA, deploy time, canonical/deprecated counts, and a connector-staleness advisory. |
| `get_market_snapshot` | One-call market-state packet composing price, range, MA trend, volume, RSI, MACD, liquidity gate, and freshness. Supports `compact` (default, max 5 tickers) and `full` (max 2 tickers) modes. |

### Price & Market Data

| Tool | Description |
|------|-------------|
| `get_market_quote` | Current price, market cap, 52-week range, moving averages, volume (~20 fields). Pre/after-market prices when available. Alias: `get_fast_info`. |
| `get_historical_prices` | Historical OHLCV data with configurable period, interval, and optional `columns` filter. Alias: `get_historical_stock_prices`. |
| `analyze_price_performance` | Pre-computed: % distance from 52w high/low and MAs, 30d annualised volatility, CAGR. Alias: `get_price_stats`. |
| `analyze_moving_average_position` | Pre-computed: % vs 50DMA/200DMA, regime50, regime200, trend (BULLISH/BEARISH/MIXED). Alias: `get_ma_position`. |
| `analyze_volume_ratio` | Pre-computed: volume vs 10d/90d averages, volumeFlag (HIGH/NORMAL/LOW). Alias: `get_volume_ratio`. |
| `check_volume_liquidity_threshold` | 20d ADV liquidity gate pass/fail. FX notional mode available via `foreign_exchange=true`. Alias: `get_volume_gate`. |
| `get_technical_indicators` | Pre-computed RSI-14 (Wilder) and MACD (12,26,9) from daily closes. |
| `get_price_slope` | N-day price slope (% change) and direction (UP/DOWN/FLAT). Args: `days` (default 5). |
| `get_short_interest` | Short % of float, shares short, days-to-cover, prior-month comparison. |
| `get_short_momentum` | Short interest with MoM delta, direction (RISING/FALLING/FLAT), squeeze risk (HIGH/MODERATE/LOW). |
| `get_overnight_quote` | Overnight trading data (20:00–04:00 ET). Returns price, gap %, data source, and staleness flag. |

### Company Fundamentals

| Tool | Description |
|------|-------------|
| `get_company_profile` | ~30 key fundamental fields by default. Alias: `get_stock_info`. |
| `get_fund_profile` | ETF/mutual fund data. Alias: `get_etf_info`. |
| `get_financial_statement` | Income statement, balance sheet, or cash flow (annual/quarterly/TTM). Optional `line_items` filter. |
| `analyze_financial_ratios` | Pre-computed: P/E, PEG, P/S, P/B, EV/EBITDA, margins, ROE, ROA, debt ratios. Alias: `get_financial_ratios`. |
| `analyze_credit_health` | Pre-computed: Net Debt/EBITDA, interest coverage, debt tier, credit stress flag. Alias: `get_credit_health`. |
| `get_corporate_actions` | Dividends and splits history. Alias: `get_stock_actions`. |
| `get_ownership_holders` | Major holders, institutional holders, mutual funds, or insider transactions. Alias: `get_holder_info`. |

### Analyst & Forecasts

| Tool | Description |
|------|-------------|
| `get_analyst_consensus` | Compact analyst consensus: price targets (current/low/high/mean/median + % upside) and rating breakdown. |
| `get_earnings_analysis` | All analyst forward-looking data: EPS/revenue estimates, trend, history, growth. Replaces 5 calls. |
| `get_analyst_recommendations` | Raw analyst recommendations or upgrades/downgrades history. Alias: `get_recommendations`. |
| `get_analyst_rating_changes` | Recent rating changes with signal (UPGRADE/DOWNGRADE/MAINTAIN), price target direction, net sentiment. Alias: `get_analyst_upgrade_radar`. |
| `analyze_earnings_momentum` | Pre-computed: EPS revision momentum (7/30/90d), revision direction, beat rate, beat streak. Alias: `get_earnings_momentum`. |
| `get_company_events_calendar` | Next earnings date (confirmed vs estimated), ex-dividend/pay dates. Alias: `get_calendar`. |

### Options

| Tool | Description |
|------|-------------|
| `get_option_expiration_dates` | Available options expiration dates. |
| `get_option_chain` | Options chain for a specific expiry and type. Supports strike filters and `in_the_money_only`. |
| `summarize_options_flow` | Pre-computed: put/call ratio, P/C sentiment, ATM IV, IV percentile, max pain, highest OI strikes. Alias: `get_options_flow_summary`. |
| `find_put_hedge_candidates` | Pre-filtered OTM puts within configurable OTM % range and budget. Alias: `get_put_hedge_candidates`. |
| `analyze_options_flow_window` | Structured event-window options scan with 72h cached trend. Args: `ticker`, `window_label`. Alias: `get_options_flow_scan`. |

### SEC Filings

| Tool | Description |
|------|-------------|
| `list_sec_company_filings` | List SEC filings from EDGAR submissions. Returns filing type, date, accession number, document URL. |
| `get_sec_filing_outline` | Section/heading outline of an SEC filing. |
| `get_sec_filing_section` | Text of a specific filing section. Alias: `get_filing_section`. |
| `list_sec_filing_tables` | Tables present in an SEC filing. |
| `get_sec_filing_table` | Specific table from an SEC filing by index. |
| `extract_sec_filing_fact` | Extract a specific fact from a filing, including XBRL context metadata when available. |
| `search_sec_filing_text` | Search narrative filing HTML text or retrieve table-context snippets. Use as fallback when `get_filing_data` returns `NOT_DISCLOSED`. Alias: `search_filing_text`. |
| `index_sec_filing` | Build a deterministic section/table index for a filing (cached 24h). |
| `get_sec_filing_index` | Get the pre-built filing index. |
| `query_sec_filing_index` | Route SEC filing query types to index-backed extractors. |

### SEC Structured Extractors

| Tool | Description |
|------|-------------|
| `extract_geographic_revenue` | Geographic revenue exposure with evidence. |
| `extract_segment_revenue` | Segment revenue rows from SEC facts. |
| `extract_total_revenue` | Total revenue from SEC facts. |
| `extract_revenue_exposure` | Revenue exposure for a region/customer/segment query. |
| `extract_china_exposure` | China exposure with revenue and non-revenue classifications. |
| `extract_risk_factor_mentions` | Risk-factor term mentions from SEC filings. |
| `extract_customer_concentration` | Customer concentration percentages. |

### Public News & Events

| Tool | Description |
|------|-------------|
| `get_company_news` | Recent public company news from Yahoo Finance and SEC sources. |
| `search_company_news` | Search news with keyword filter. |
| `get_company_press_releases` | 8-K press releases as structured public events. Returns explicit `SEC_8K_FOUND_EX99_NOT_FOUND` when 8-K evidence exists but no EX-99.1 exhibit is resolved. |
| `get_sec_recent_events` | Recent SEC filings as structured public events. |
| `get_public_event_timeline` | Deduplicated chronological timeline across all sources. |
| `verify_company_event` | Cross-validate an event across sources: CONFIRMED/PARTIAL/NOT_FOUND/STALE/CONFLICTING. |

### Phase 5 Earnings Intelligence

| Tool | Description |
|------|-------------|
| `get_latest_earnings_release` | Find the latest earnings release evidence from SEC 8-K, IR, or Yahoo. |
| `index_earnings_release` | Build a section/table index for an earnings release. |
| `extract_earnings_metrics` | Extract reported revenue, EPS, gross margin, operating income, FCF, capex. |
| `extract_guidance` | Extract company-provided guidance ranges. |
| `extract_management_commentary` | Extract topic-specific management commentary with evidence excerpts. |
| `compare_earnings_actual_vs_estimate` | Compare actual earnings vs analyst estimates with surprise %. |
| `get_earnings_call_transcript` | Retrieve earnings call transcript content from SEC 8-K exhibits with optional topic filter. |

### Discovery & Position

| Tool | Description |
|------|-------------|
| `search_ticker` | Resolve company name or ISIN to ticker symbol. |
| `screen_stocks` | Screen with predefined criteria (day_gainers, most_actives, undervalued_large_caps, etc.). |
| `analyze_position_signals` | Aggregate public market, analyst, earnings, and technical inputs for caller-defined scoring. Alias: `get_position_score_inputs`. |
| `calculate_price_target_distance` | Compare current price to a user-supplied reference target. `reference_target_price` is preferred; `io_pt` is accepted for backward compatibility. Alias: `get_price_target_bracket`. |

### Diagnostics

| Tool | Description |
|------|-------------|
| `health_check` | Runtime health metadata: server version, tool count, manifest hash, envelope V2 status. |
| `get_manifest_diagnostics` | Full deployment diagnostics including canonical/deprecated tool counts and connector-staleness advisory. |

---

## Privacy & Scope

All tools operate exclusively on **publicly available** data:

- **Yahoo Finance** — public market data, news, analyst estimates, options data.
- **SEC EDGAR** — public filings via the official EDGAR API.

No private portfolio data, user accounts, or proprietary datasets are accessed. `privacyScope` is always `public_market_data_only` (verified via `get_manifest_diagnostics`).

---

## Requirements

- Python 3.11 or higher
- Dependencies: `mcp`, `yfinance`, `pandas`, `pydantic` (see `pyproject.toml`)

## Setup

```bash
git clone https://github.com/carnat/yahoo-finance-mcp.git
cd yahoo-finance-mcp
uv venv && source .venv/bin/activate
uv pip install -e .
uv run server.py
```

To use grouped meta-tools mode (recommended for LLM token efficiency):

```bash
TOOL_MODE=grouped uv run server.py
```

### Claude Desktop integration

```json
{
  "mcpServers": {
    "yfinance": {
      "command": "uv",
      "args": ["--directory", "/ABSOLUTE/PATH/TO/yahoo-finance-mcp", "run", "server.py"]
    }
  }
}
```

### Remote MCP (Replit / Claude.ai)

```
https://<your-replit>.repl.co/mcp
```

### SEC structured-facts provider

Structured SEC revenue/geography facts use the official SEC `data.sec.gov`
JSON APIs directly from the Cloudflare Worker. No separate Python service or API key is
required. See
[`docs/sec-facts-provider.md`](docs/sec-facts-provider.md).

## License

MIT
