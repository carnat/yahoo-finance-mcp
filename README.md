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

Expanded mode is the default and exposes individual tools.

Grouped mode exposes 11 domain tools with `{ "action": "...", "params": {...} }`
routing. It is useful when an MCP client benefits from a smaller tool list.

```bash
TOOL_MODE=grouped uv run server.py
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

## Tool Coverage

Main public tool areas:

- price, volume, technicals, short interest, and market snapshots;
- company profile, funds, statements, ratios, credit health, ownership, and corporate actions;
- analyst consensus, rating changes, earnings analysis, and calendars;
- options expirations, chains, flow summaries, and hedge candidates;
- SEC filing lists, sections, tables, exhibits, text search, and filing indexes;
- SEC structured extractors for revenue, segment, geography, risk, and exposure queries;
- company news, press releases, SEC events, event timelines, and event verification;
- earnings release indexing, metrics, guidance, commentary, actual-vs-estimate, and transcripts;
- Thailand SEC Open Data fund NAV, dated factsheet evidence, and project-scoped dividend history;
- ticker search, stock screens, diagnostics, and manifest health.

Thai SEC fund tools resolve an exact `fund_class_name`; use `proj_id` to
disambiguate or `project_info` to narrow the documented SEC profile query by
the official project name or abbreviation. Profile lookup first covers
`Registered` funds, then retries `IPO` only when no registered exact match is
returned. This is discovery only: the returned NAV, factsheet, and dividend
data retain their own dates and scopes.

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
  `failedSources`, `skippedSources`, and `recommendedNextAction` before
  treating an empty result as absence. Yahoo primary items are retained only
  when `tickerMatch:"EXPLICIT"` is supported by `matchBasis` of
  `TICKER_TOKEN`, `ISSUER_NAME`, or `ISSUER_ACRONYM`; source diagnostics expose
  `rawCount`, `acceptedCount`, and rejection reasons. `decisionUse` is
  `CHECK_OFFICIAL_RELEASES` for material Yahoo events that should be escalated
  to `get_company_press_releases` or `verify_company_event`, otherwise
  `CONTEXT_ONLY`. `evidenceClass` and `urlProvenance` remain comparable for LLM
  callers; legacy `confidence` is backward-compatible but not a provider-quality
  rank.
- `extract_sec_filing_fact` and SEC exposure tools can return explicit
  limitation statuses such as `EXTRACTION_FAILED`, `TABLE_NOT_PARSED`,
  `PROVIDER_LIMITATION`, or `NO_DIMENSIONAL_REVENUE_FACT`.
- Provider rate limits, market data availability, filing formats, and SEC EDGAR
  availability can affect individual calls.
- Thai fund tools require `SEC_OPEN_DATA_API_KEY`. They resolve
  `fund_class_name` exactly and never infer a share class. Check `status`,
  `scope`, `dataDate`/section `asOfDate`, and `recovery` before using results:
  NAV is share-class scoped, factsheet holdings and dividends are project
  scoped, and factsheet URLs are references only (no PDF fetching/parsing).

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
