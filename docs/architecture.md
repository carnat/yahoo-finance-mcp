# Architecture

The server runs in two runtimes that must give the same output for the same input:

- **Python** (`server.py`, `yfmcp/`): a local MCP server on the MCP Python SDK.
- **Cloudflare Worker** (`worker/src/*.ts`): the deployed MCP endpoint.

Every behaviour change lands in both, with parity cases (`scripts/parity/`), a section in
`docs/provider-runtime-guidance.md` and a version bump (`CLAUDE.md`). Pinned Python/Worker differences are listed in the
harness cases with a `FINDING` decision label.

## Request path

| Step | Python | Worker |
|---|---|---|
| Entry and transport | `server.py` (imports the domain modules for registration, re-exports handlers) | `index.ts` (HTTP, auth, CORS), `mcp.ts` (JSON-RPC) |
| Tool listing and grouped mode | `yfmcp/app.py` (`YfmcpServer`, ordered registry, grouped schemas), `tool_groups.py`, `tool_catalog.json` | `schemas.ts` (tool definitions, input/output schemas, grouped tools), `tool-catalog.ts` |
| Argument checks | `yfmcp/validation.py` | `tools.ts` (`validateToolArgs`, period checks), `validate.ts` |
| Dispatch | Decorator registration in each `yfmcp/tools/*.py` | `tools.ts` `TOOL_HANDLERS` (a `Map` from tool name to handler) |
| Envelope and errors | `yfmcp/envelope.py` | `response.ts` |
| Request budget, cache, telemetry | `yfmcp/cache.py` | `request-context.ts`, `cache.ts`, `batch.ts`, `telemetry.ts` |
| Deprecation notices | `yfmcp/deprecations.py` | `deprecations.ts` |

## Domain modules

| Domain | Python | Worker |
|---|---|---|
| Pricing and options | `yfmcp/tools/pricing.py`, `yfmcp/pricing_notices.py`, `yfmcp/exchange_calendars.py` | `pricing.ts`, `pricing-notices.ts`, `exchange-calendars.ts` |
| Fundamentals and analyst | `yfmcp/tools/fundamentals.py` | `fundamentals.ts` |
| SEC filings (retrieval, index, search, facts) | `yfmcp/tools/sec.py`, `yfmcp/filing_search.py`, `yfmcp/sec_facts.py`, `yfmcp/companyfacts_coverage.py` | `sec.ts`, `filing-search.ts`, `sec-facts.ts`, `companyfacts-coverage.ts` |
| Extractors and exposure | `yfmcp/tools/exposure.py`, `yfmcp/extraction_rules.py`, `yfmcp/exposure_presets.py` | `exposure.ts`, `extraction-rules.ts` |
| Capital structure, dilution, schedules | `yfmcp/capital_structure.py`, `yfmcp/share_scenarios.py`, `yfmcp/funding_schedule.py`, `yfmcp/driver_ledger.py` | `capital-structure.ts`, `share-scenarios.ts`, `funding-schedule.ts`, `driver-ledger.ts` |
| Earnings, guidance, transcripts | `yfmcp/tools/earnings.py`, `yfmcp/guidance_history.py`, `yfmcp/earnings_period.py`, `yfmcp/fiscal_calendar.py` | `earnings.ts`, `guidance-history.ts`, `earnings-period.ts`, `fiscal-calendar.ts`, `transcript-contract.ts` |
| Valuation | `yfmcp/tools/valuation.py`, `yfmcp/valuation.py`, `yfmcp/valuation_history.py`, `yfmcp/metric_reconciliation.py` | `valuation-tools.ts`, `valuation.ts`, `valuation-history.ts`, `metric-reconciliation.ts` |
| Evidence pack and cuts | `yfmcp/tools/evidence_pack.py`, `yfmcp/evidence.py`, `yfmcp/evidence_store.py` | `evidence-pack.ts`, `evidence.ts`, `evidence-store.ts` |
| News and public events | `yfmcp/tools/news.py`, `yfmcp/news/` (one provider per file) | `news.ts`, `news/` (one provider per file) |
| Scarce providers, Companies House | `yfmcp/tools/provider_gaps.py` | `providers.ts` |
| Thai SEC funds | `yfmcp/tools/thai_funds.py` | `sec-thailand.ts` |
| System (health check) | `yfmcp/tools/system.py` | `tools.ts` (`health_check` handler) |

`worker/src/yahoo-finance.ts` is now a thin module: shared Yahoo helpers and re-exports kept so the parity bundle's
`m.yahooFinance.*` names stay stable.

## Clients (one per upstream)

| Upstream | Python | Worker |
|---|---|---|
| Yahoo Finance | `yfmcp/clients/yahoo.py`, `yfmcp/clients/yahoo_transcripts.py` (and `yfinance`) | `clients/yahoo.ts` |
| SEC EDGAR | `yfmcp/clients/edgar.py` | `clients/edgar.ts` |
| Finnhub, Alpha Vantage, Marketaux | `yfmcp/clients/market_providers.py` | `clients/provider-http.ts`, `providers.ts` |
| URL building | `yfmcp/clients/endpoints.py` | `clients/endpoints.ts` |
| HTML and tables | `yfmcp/clients/edgar.py` (fetch, exhibit index), `yfmcp/tools/sec.py` (table rows) | `clients/html.ts` |
| Outbound fetch allow-list | `yfmcp/url_policy.py` | `url-policy.ts` |

## Shared data

`yfmcp/data/` holds the JSON registries both runtimes use: the company IR page registry (also written by the daily
`ir-source-discovery` workflow), news source registry and capabilities, exposure presets and the exchange calendar.
Python reads them through `yfmcp.data.DATA_DIR`; the Worker compiles them in. Python never reads `worker/src`
(`scripts/test_no_worker_src_reads.py`).

## Tests and checks

- `scripts/parity/`: the parity harness runs each case against both runtimes and compares outputs.
- `scripts/test_*.py`: unit and contract tests; `.github/workflows/ci.yml` lists every step CI runs.
- `analysis/yahoo-finance-mcp/extract_topology.py`: regenerates `topology.json`, the module and data-flow map.
- Deploy (`deploy-worker.yml`) runs the deployed discovery, contract and canary checks after each Worker release.
