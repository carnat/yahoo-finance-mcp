# Yahoo Finance MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server that gives any MCP-compatible AI client (Claude, Cursor, VS Code Copilot, etc.) direct access to live public financial data from Yahoo Finance and SEC EDGAR.

> **Source of truth:** The live MCP tool manifest exposed by the running server is the canonical reference for tool names, input schemas, and deprecation status. README tables are updated on each phase release. Use `get_manifest_diagnostics` to verify the deployed tool set at runtime.

## Demo

![MCP Demo](assets/demo.gif)

---

## V2 Response Envelope Contract

All tool responses use the V2 envelope when `MCP_ENVELOPE_V2=true`:

```json
{
  "ok": true,
  "data": { ... },
  "meta": {
    "tool": "get_market_quote",
    "source": "yahoo_finance",
    "dataDate": "2026-05-15",
    "serverVersion": "0.1.0",
    "cacheHit": false,
    "warnings": []
  },
  "error": null
}
```

**Key semantics:**

| Pattern | Meaning |
|---------|---------|
| `ok=true`, `error=null` | Tool succeeded. |
| `ok=true`, `data.status="NOT_DISCLOSED"` | Tool succeeded; the specific fact was not found in XBRL-tagged data. Use `search_sec_filing_text` as fallback. |
| `ok=true`, `data.status="NOT_FOUND"` | Tool succeeded; no matching items found in searched sources. |
| `ok=false`, `error` present | Transport or validation failure. Check `error.code`. |
| `ok=true` with `meta.warnings[]` | Partial or degraded data — e.g., stale targets, source-limited results. **Do not hide warnings.** |

`ok=true` with a non-null `data.status` field is **not** a transport failure. `NOT_DISCLOSED` and `NOT_FOUND` are data-quality outcomes, not errors.

---

## Canonical Tool Names vs Legacy Aliases

Canonical tool names use neutral public language. Legacy aliases are preserved for backward compatibility but should not be used in new integrations. Deprecated aliases return `meta.deprecatedTool=true` and `meta.useInstead`.

| Canonical name | Legacy alias |
|----------------|-------------|
| `get_market_quote` | `get_fast_info` |
| `analyze_price_performance` | `get_price_stats` |
| `analyze_moving_average_position` | `get_ma_position` |
| `analyze_volume_ratio` | `get_volume_ratio` |
| `check_volume_liquidity_threshold` | `get_volume_gate`, `get_adv_gate` |
| `analyze_position_signals` | `get_tps_inputs` |
| `calculate_price_target_distance` | `get_eqf_bracket` |
| `analyze_options_flow_window` | `get_dc134_options_scan` |
| `extract_sec_filing_fact` | `get_filing_data`, `get_geographic_revenue`, `get_china_revenue_pct` |
| `search_sec_filing_text` | `get_filing_text_search` |
| `get_sec_filing_section` | `get_filing_document` |
| `get_company_news` | `get_yahoo_finance_news` |

---

## Atomic Tools vs Snapshot Tools

**Atomic tools** return one focused data category per call. Use them for targeted queries or when you need to inspect a specific metric.

**Snapshot/package tools** compose multiple atomic tools in one call for LLM token efficiency. Use them when you need a broad overview.

| Type | Tool | Purpose |
|------|------|---------|
| Snapshot | `get_market_snapshot` | Compact or full market-state packet: price, range, MA trend, volume, RSI, MACD, freshness, liquidity gate |
| Atomic | `get_market_quote` | Current price, volume, 52-week range, moving averages |
| Atomic | `analyze_price_performance` | % distance from 52w high/low, 30d volatility, CAGR |
| Atomic | `analyze_moving_average_position` | % vs 50DMA/200DMA, trend (BULLISH/BEARISH/MIXED) |
| Atomic | `analyze_volume_ratio` | Volume vs 10d/90d averages, volumeFlag |
| Atomic | `check_volume_liquidity_threshold` | 20d ADV liquidity gate pass/fail |
| Atomic | `get_technical_indicators` | RSI-14, MACD (12,26,9) |

**Snapshot output fields (compact mode):**

```json
{
  "ticker": "ASTS",
  "price": { "last": 83.67, "previousClose": 83.01, "changePct": 0.80, "lastTradeDate": "2026-05-15", "marketOpen": false },
  "range": { "yearHigh": 129.89, "yearLow": 22.47, "pctFromYearHigh": -35.58, "pctFromYearLow": 272.36 },
  "trend": { "fiftyDayAverage": 83.71, "twoHundredDayAverage": 74.68, "pctFrom50dma": -0.05, "pctFrom200dma": 12.04, "maTrend": "MIXED", "rsi14": 54.15, "macdHistogram": 1.71 },
  "volume": { "lastVolume": 21579066, "avgVolume10d": 22959070, "avgVolume20d": 20516600, "avgVolume90d": 15802664, "ratio10d": 0.94, "ratio20d": 1.05, "ratio90d": 1.37, "volumeFlag": "NORMAL", "liquidityGatePass": true },
  "risk": { "annualizedVolatility30d": 103.46 },
  "freshness": { "dataDate": "2026-05-15", "retrievedAt": "...", "marketSessionAware": true, "freshnessClass": "WEEKEND_EXPECTED_STALE" },
  "componentStatus": { "quote": "OK", "priceStats": "OK", "maPosition": "OK", "volumeRatio": "OK", "volumeGate": "OK", "technicalIndicators": "OK" },
  "partialSuccess": false,
  "failedComponents": [],
  "warnings": []
}
```

**Partial component failure:** If one component fails, the snapshot still returns with the available data, setting `partialSuccess=true` and listing `failedComponents`. Warnings are always included.

**Batch caps:** compact mode: max 5 tickers; full mode: max 2 tickers.

**Freshness classes:**

| Class | Meaning |
|-------|---------|
| `FRESH` | Data ≤28h old |
| `MARKET_CLOSED_EXPECTED_STALE` | Data 28–56h old (overnight close) |
| `WEEKEND_EXPECTED_STALE` | It's Saturday/Sunday and data is from Friday |
| `STALE` | Data 56–168h old |
| `VERY_STALE` | Data >168h old |
| `UNKNOWN` | Cannot determine |

---

## Data-Quality Semantics

Tools that return enriched metadata include some or all of these fields:

| Field | Type | Meaning |
|-------|------|---------|
| `dataQuality` | string | Overall data quality: `HIGH`, `MEDIUM`, `LOW`, `PARTIAL` |
| `warnings` | array | List of `{code, message}` objects. Never suppressed in compact mode. |
| `missingComponents` | array | Sub-components that returned no data |
| `unavailableMetrics` | array | Specific metrics that could not be computed |
| `computedMetrics` | array | Metrics derived from raw data server-side |
| `targetLagSignal` | string/null | Analyst price target may lag recent price moves |
| `historicalSurpriseSignal` | string/null | Historical EPS beat/miss pattern |
| `forwardRevisionSignal` | string/null | Recent EPS estimate revision direction |
| `compositeMomentumSignal` | string/null | Combined earnings + revision momentum |
| `sourceCoverage` | string | `FULL`, `PARTIAL`, or `NONE` |
| `freshnessClass` | string | Freshness classification (see table above) |

**Warning codes include:** `COMPONENT_FAILED`, `DEPRECATED_ALIAS`, `TARGET_LAG`, `STALE_CONSENSUS`, `PARTIAL_SOURCE_COVERAGE`, `NEGATIVE_EARNINGS_BASE`.

---

## SEC Extraction Semantics

| Term | Meaning |
|------|---------|
| `NOT_DISCLOSED` | The fact was not found in XBRL-tagged EDGAR data. Try `search_sec_filing_text` with `return_tables=true` for table-level fallback. |
| `NOT_FOUND` | No matching filing or section was located. |
| `valueRatio` | Decimal fraction (e.g., `0.1547` = 15.47%) |
| `valuePct` | Percentage (e.g., `15.47`). **Never emitted without a `denominator`.** |
| `evidence` | Source metadata: filing URL, section, matched text or table row |

**Workflow:** Use `extract_sec_filing_fact` (XBRL companyconcept) first. If `status=NOT_DISCLOSED`, use `search_sec_filing_text` with `return_tables=true` to locate narrative tables. `valuePct` is only emitted when a denominator is available.

---

## Connector Schema Cache Warning

ChatGPT (and other connector-based clients) cache the connector schema independently of the deployed Worker. The cached schema can lag the live deployment, causing stale or missing tool names to appear.

**The live Worker tools/list and `get_manifest_diagnostics` are always the source of truth.** When integrating, call `get_manifest_diagnostics` to confirm the deployed tool set.

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
| `check_volume_liquidity_threshold` | 20d ADV liquidity gate pass/fail. FX notional mode available via `foreign_exchange=true`. Aliases: `get_volume_gate`, `get_adv_gate`. |
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
| `get_sec_filing_section` | Text of a specific filing section. Alias: `get_filing_document`. |
| `list_sec_filing_tables` | Tables present in an SEC filing. |
| `get_sec_filing_table` | Specific table from an SEC filing by index. |
| `extract_sec_filing_fact` | Extract a specific fact from a filing. |
| `search_sec_filing_text` | Search narrative filing HTML text or retrieve table-context snippets. Use as fallback when `get_filing_data` returns `NOT_DISCLOSED`. Alias: `get_filing_text_search`. |
| `index_sec_filing` | Build a deterministic section/table index for a filing (cached 24h). |
| `get_sec_filing_index` | Get the pre-built filing index. |
| `query_sec_filing_index` | Route SEC filing query types to index-backed extractors. |

### SEC Phase 3 Extractors

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
| `get_company_press_releases` | 8-K press releases as structured public events. |
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
| `analyze_position_signals` | Aggregate public market, analyst, earnings, and technical inputs for caller-defined scoring. Alias: `get_tps_inputs`. |
| `calculate_price_target_distance` | Compare current price to a user-supplied reference target. `reference_target_price` is preferred; `io_pt` is accepted for backward compatibility. Alias: `get_eqf_bracket`. |

### Diagnostics

| Tool | Description |
|------|-------------|
| `health_check` | Runtime health metadata: server version, tool count, manifest hash, envelope V2 status. |
| `get_manifest_diagnostics` | Full deployment diagnostics including canonical/deprecated tool counts and connector-staleness advisory. |

---

## Phase Roadmap

| Phase | Status | Summary |
|-------|--------|---------|
| 1 | ✅ Complete | V2 envelope, core market data tools |
| 2 | ✅ Complete | SEC filing infrastructure, EDGAR submissions |
| 3 | ✅ Complete | SEC structured extractors (geographic, segment, risk, customer) |
| 4 | ✅ Complete | Options flow, put hedge, event-window scan |
| 5 | ✅ Complete | Earnings intelligence (release, metrics, guidance, commentary) |
| 6 | ✅ Complete | Public event/news multi-source schema |
| 7 | ✅ Complete | Semantic quality metadata, SEC query router, smoke infrastructure |
| 8 | ✅ Complete | README + manifest diagnostics + `get_market_snapshot` snapshot tool |

**Phase 8 follow-on TODOs (PR63–PR66):**
- PR63: `get_company_quality_snapshot` — profile + ratios + credit + earnings + analyst + calendar
- PR64: `get_sec_exposure_snapshot` — `get_filing_data` + text-search fallback, AAPL Greater China table fix
- PR65: `get_event_news_snapshot` — SOURCE_LIMITED_NOT_FOUND / PARTIAL_SOURCE_COVERAGE semantics
- PR66: `get_equity_research_snapshot` — top-level LLM packet with `include[]` selector

---

## Acceptance Constraints (all phases)

- Do not delete atomic tools.
- Do not hide warnings in compact mode.
- Do not emit investment recommendations such as BUY/SELL.
- Do not collapse source quality into an opaque score.
- Do not return plain NOT_FOUND when major sources are unavailable — use SOURCE_LIMITED_NOT_FOUND.
- Do not return `valuePct` without a `denominator`.
- Do not expose private doctrine wording in public tool descriptions.
- Keep aliases backward-compatible.

---

## Smoke & Regression Commands

```bash
# Tool sync check (server.py ↔ worker manifest)
python scripts/check_tool_sync.py

# Phase unit tests (offline, no network)
python scripts/test_phase1.py
python scripts/test_phase3.py
python scripts/test_phase3_extractors.py
python scripts/test_phase4.py
python scripts/test_phase5.py
python scripts/test_phase6b.py
python scripts/test_phase8.py

# Universal alias routing smoke
ALLOW_NETWORK_SKIP=1 python scripts/test_universal_aliases.py

# Geographic revenue schema smoke
python scripts/test_geographic_revenue_schema.py

# Source discovery schema
python scripts/test_source_discovery_schema.py

# Options quality
python scripts/test_options_quality.py

# Deployed smoke (requires MCP_URL env var)
MCP_URL=https://<your-worker>.workers.dev python scripts/test_deployed_discovery.py
```

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

## License

MIT
