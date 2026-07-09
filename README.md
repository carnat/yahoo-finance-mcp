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

Grouped mode exposes 10 domain tools with `{ "action": "...", "params": {...} }`
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
- ticker search, stock screens, diagnostics, and manifest health.

Use `tools/list` or `get_manifest_diagnostics` for the exact current tool names,
schemas, aliases, and deprecation metadata.

## Important Limitations

- `get_overnight_quote` is a deprecated diagnostics-only Yahoo extended-hours
  proxy. It does not provide true 20:00-04:00 ET overnight venue data.
- `get_sec_filing_section_markdown` is degraded and should be verified against
  the source filing before use; it uses a lossy Worker HTML fallback.
- `get_company_press_releases` is payload-gated: only responses with
  `decisionGrade:true` and resolved SEC EX-99 press-release evidence are
  decision-grade; unresolved exhibit states remain explicit.
- `extract_sec_filing_fact` and SEC exposure tools can return explicit
  limitation statuses such as `EXTRACTION_FAILED`, `TABLE_NOT_PARSED`,
  `PROVIDER_LIMITATION`, or `NO_DIMENSIONAL_REVENUE_FACT`.
- Provider rate limits, market data availability, filing formats, and SEC EDGAR
  availability can affect individual calls.

## Data Sources

- Yahoo Finance public market data
- SEC EDGAR official public filing data and `data.sec.gov` JSON APIs

Structured SEC revenue/geography facts use official SEC data plus the Worker
filing/index fallback. No separate Python sidecar or paid hosted parser is
required.

Provider/runtime design notes live in:

- `docs/sec-facts-provider.md`
- `docs/provider-runtime-guidance.md`

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
