# Yahoo Finance MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io/) server for public
market data from Yahoo Finance and SEC EDGAR.

It can be used in two ways:

- **Remote MCP:** the Worker implementation in `worker/` is the remote MCP
  server. The public hosted endpoint currently runs on Cloudflare Workers.
- **Local MCP:** `server.py` runs the Python MCP server over stdio for local
  clients such as Claude Desktop.

All tools use public data only. No private brokerage, account, portfolio, or
user data is accessed.

## Public Endpoint

```text
https://yahoo-finance-mcp.artinatw.workers.dev/mcp
```

The live server exposes its current tool manifest through MCP `tools/list`.
For runtime metadata, call:

- `health_check`
- `get_manifest_diagnostics`

## Install Locally

Requirements:

- Python 3.11+
- `uv`

```bash
git clone https://github.com/carnat/yahoo-finance-mcp.git
cd yahoo-finance-mcp
uv venv
uv pip install -e .
uv run server.py
```

Claude Desktop example:

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

## Tool Modes

Grouped mode is the default. It exposes 11 domain tools with
`{ "action": "...", "params": {...} }` routing so LLM clients receive a compact,
deterministic tool list. MCP discovery publishes an action-discriminated
`oneOf` schema for each domain, so required action parameters are typed before
execution. All tools are annotated read-only, non-destructive, and idempotent;
they may query external market and regulatory sources.

```bash
uv run server.py
```

Expanded mode remains available for compatibility and debugging:

```bash
TOOL_MODE=expanded uv run server.py
```

Grouped domains:

- `stock_pricing`
- `stock_fundamentals`
- `analyst_data`
- `options_analysis`
- `sec_filings`
- `sec_extractors`
- `news_events`
- `earnings_intelligence`
- `screening`
- `system`
- `thai_funds`

Example grouped call:

```json
{
  "tool": "stock_pricing",
  "arguments": {
    "action": "get_market_quote",
    "params": { "ticker": "AAPL" }
  }
}
```

Clients that cache MCP discovery may need to reconnect or refresh the connector
after switching tool modes.

## Tool Coverage

Main public tool areas:

- price, volume, technicals, short interest, and market snapshots;
- company profile, section-selectable fund data, annual/quarterly/trailing statements, current and historical valuation ratios, share-count trends, credit health, Yahoo holder views, explicit deeper institutional ownership, and corporate actions including fund capital-gain distributions;
- analyst consensus, EPS revision counts, rating changes, ticker earnings history, and market-wide earnings/economic/IPO/split calendars;
- options expirations, chains, current flow summaries, explicit historical put/call ratios, and hedge candidates;
- SEC filing lists, sections, tables, exhibits, text search, and filing indexes;
- SEC structured extractors for revenue, segment, geography, risk, and exposure queries;
- company news, press releases, SEC events, event timelines, and event verification;
- earnings release indexing, metrics, guidance, commentary, actual-vs-estimate, and transcripts;
- Thailand SEC Open Data fund discovery, direct-project NAV refresh, dated factsheet evidence, and project-scoped dividend history;
- ticker search, stock screens, diagnostics, and manifest health.

Price observations are explicitly labeled. `get_market_quote.lastPrice` is a
regular-market quote with `priceTimestamp`; `get_price_slope.endClose` is an
adjusted completed daily-bar value when available and includes `endRawClose`
for the same `dataDate`. `days=N` uses N+1 completed bars; an unfinished
current-session bar is excluded. Check `freshnessStatus` and
`recommendedNextAction` before using the result. Do not treat values from
different dates or observation times as conflicts.

Other daily derived tools follow the same boundary. Technical indicators,
volume ratios, liquidity gates, realized-volatility context, and historical
performance use completed sessions only and expose `dataDate`, `barStatus`,
and freshness/retry fields. Raw daily history can still include the active row,
but labels it with `barStatus:"INCOMPLETE"` and `isFinal:false`. Moving-average
and target-distance tools intentionally compare a live quote and therefore
expose `priceTimestamp` separately. In `get_market_snapshot`,
`quoteFreshnessClass` (and the legacy `freshnessClass`) uses that quote
timestamp, while `completedBarFreshnessStatus` remains independent.

Fund valuation characteristics are returned as conventional labeled
multiples. When Yahoo supplies inverse valuation yields, `get_fund_profile`
retains the provider values separately and records the reciprocal
normalization. Yahoo does not expose a reliable holdings/allocation as-of date
in this response; read `sectionDates`, `warnings`, and
`recommendedNextAction` before material use.

Put hedge candidates require a two-sided quote plus open-interest or volume
evidence. Contract cost and budget feasibility use the executable ask price,
not midpoint. `PARTIAL` or `recommendedNextAction:"RETRY"` means the option
chain is not suitable for a hedge conclusion.

Use `search_thai_funds` to map an official project name, abbreviation, AMC, or
known SEC share-class code to compact active-profile candidates. It never
selects one automatically. Thai SEC fund data calls resolve an exact SEC
`fund_class_name`; use `project_info` to narrow the documented profile query.
For NAV, an explicit `proj_id` calls the official NAV endpoint directly: the
returned SEC class is source truth and can be `main` rather than a public
distributor code. `get_thai_fund_nav_batch` refreshes up to 20 explicit
project identities sequentially; it does not calculate portfolio value or
wrapper/tax eligibility. Returned NAV, factsheet, and dividend data retain
their own dates and scopes.

Use `tools/list` or `get_manifest_diagnostics` for the exact current tool names,
schemas, aliases, and deprecation metadata.

## Important Limitations

- `get_overnight_quote` is a deprecated diagnostics-only Yahoo extended-hours
  proxy. It does not provide true 20:00-04:00 ET overnight venue data.
- `get_sec_filing_section_markdown` is degraded and should be verified against
  the source filing before use; it uses a lossy Worker HTML fallback.
- `get_company_press_releases` is payload-gated: only responses with
  `decisionGrade:true` and `coverageStatus` of `SEC_EX99_RESOLVED` or
  `APPROVED_IR_PAGE_RESOLVED` are decision-grade. SEC responses include
  `secEvidence`; approved IR-page responses include `irPageEvidence`.
- News/event tools can use `company_ir` to attempt safe official company
  website RSS/Atom autodiscovery. `company_ir` remains RSS/Atom-only.
  `company_ir_page` is separate and registry-backed: candidate entries return
  compact review links only, while approved entries fetch a configured HTTPS
  host/path prefix. `get_company_news` keeps lightweight Yahoo Finance/Finnhub
  defaults for batch efficiency; pass `sources:["company_ir"]` or
  `sources:["company_ir_page"]` when official company-site coverage is needed.
  RSS-only releases, candidate links, newswire, and Yahoo items are
  verification/context evidence unless the payload also resolves SEC EX-99 or
  approved IR-page evidence.
- News/event responses include a compact `coverage` object. Check its `state`,
  `failedSources`, `skippedSources`, `truncatedSources`, and
  `recommendedNextAction` before
  treating an empty result as absence. Yahoo primary items are retained only
  when `tickerMatch:"EXPLICIT"` is supported by `matchBasis` of
  `TICKER_TOKEN`, `ISSUER_NAME`, or `ISSUER_ACRONYM`; source diagnostics expose
  `rawCount`, `acceptedCount`, and rejection reasons. `decisionUse` is
  `CHECK_OFFICIAL_RELEASES` for material Yahoo events that should be escalated
  to `get_company_press_releases` or `verify_company_event`, otherwise
  `CONTEXT_ONLY`. `evidenceClass` and `urlProvenance` remain comparable for LLM
  callers; legacy `confidence` is backward-compatible but not a provider-quality
  rank. `RETRY_TRUNCATED_SOURCE` means accepted evidence was omitted by the
  response cap or cross-source dedupe; it does not mark a completed provider as
  failed or make `sourceCoverage` partial.
- `extract_sec_filing_fact` and SEC exposure tools can return explicit
  limitation statuses such as `EXTRACTION_FAILED`, `TABLE_NOT_PARSED`,
  `PROVIDER_LIMITATION`, or `NO_DIMENSIONAL_REVENUE_FACT`.
- SEC Item-section extraction uses structural headings and fails closed rather
  than returning table-of-contents text. Filing-table lists exclude empty or
  layout-only tables while retaining each original `tableIndex`; check
  `status`, `usableTableCount`, `excludedTableCount`, and
  `recommendedNextAction`. Filing intelligence binds companyfacts evidence to
  the selected accession. `parse_public_transcript` accepts either an HTTPS URL
  or caller-supplied `raw_text`.
- `get_earnings_call_transcript` is SEC-first. Optional Alpha Vantage fallback
  requires `ALPHA_VANTAGE_API_KEY` and an issuer fiscal quarter supplied as
  `fiscal_quarter:"YYYYQn"` or resolved from explicit official-release text.
  The tool never converts a filing/publication date into a fiscal quarter.
  Alpha transcript output is `evidenceClass:"CONTEXTUAL_TRANSCRIPT"` with
  `decisionGrade:false`; inspect `fiscalQuarterStatus`, `periodEvidence`, and
  `attemptedSources` before use.
- Use `get_ownership_holders` for ordinary Yahoo holder questions. The deeper
  `get_expanded_institutional_ownership` action tries eligible Finnhub coverage
  first and never calls Alpha unless `allow_scarce_fallback:true` is explicit.
  Verify material ownership claims against SEC 13F filings.
- Use `summarize_options_flow` for a current Yahoo options snapshot.
  `get_historical_put_call_ratio` requires one explicit historical date and
  returns contextual Alpha data with `decisionGrade:false`.
- Alpha-backed gap actions expose
  `capacityClass:"SCARCE_SHARED_QUOTA"`. Process and Worker edge caches reduce
  repeat calls, but the edge cache is best effort and data-center local; it is
  not global daily-quota enforcement.
- Provider rate limits, market data availability, filing formats, and SEC EDGAR
  availability can affect individual calls.
- Thai fund tools require `SEC_OPEN_DATA_API_KEY`. They never infer a share
  class: profile search returns candidates only, while an explicit NAV
  `proj_id` may return a one-class source alias such as `main`; multiple
  returned classes remain an explicit ambiguity. Check `status`, `scope`,
  `dataDate`/section `asOfDate`, and `recovery` before using results: NAV is
  share-class scoped, factsheet holdings and dividends are project scoped, and
  factsheet URLs are references only (no PDF fetching/parsing).

## Data Sources

- Yahoo Finance public market data
- SEC EDGAR official public filing data and `data.sec.gov` JSON APIs
- Official company website RSS/Atom feeds when discoverable from public profile
  website metadata
- Git-reviewed official company IR-page registry at
  `worker/src/company-ir-page-registry.json`; daily discovery writes candidates
  for manual review and never promotes sources automatically.
- Thailand SEC Open Data Fund API (`https://api.sec.or.th/v2/fund/...`) with a
  configured subscription key. It returns official regulatory data but does
  not change the existing decision-grade gate.
- Optional Alpha Vantage transcript, explicit historical put/call-ratio, and
  explicit institutional-ownership fallback workflows with a configured API
  key. They supplement rather than replace Yahoo, Finnhub, SEC, or official
  evidence and remain contextual.
- Optional Finnhub company-news and expanded institutional-ownership coverage
  with a configured API key and market/endpoint eligibility checks.

Structured SEC revenue/geography facts use official SEC data plus the Worker
filing/index fallback. No separate Python sidecar or paid hosted parser is
required.

Provider/runtime design notes live in:

- `docs/sec-facts-provider.md`
- `docs/provider-runtime-guidance.md`
- `docs/thai-sec-fund-phase2.md`

## Development

Common checks:

```bash
python -X utf8 scripts/check_tool_sync.py
python -m unittest scripts.test_worker_grouped_mode -v
```

Worker:

```bash
cd worker
npm install
npm run type-check
```

The remote MCP implementation lives in `worker/`. Replit, Smithery, and Python
HTTP deployment wrappers are not maintained in this repo. If a local dashboard
is added later, keep it as a separate app surface rather than another MCP
deployment path.

## License

MIT
