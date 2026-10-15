# Modernization Assessment — yahoo-finance-mcp

Assessed 2026-10-14 at `f122d0e` (release 2.5.33), full git history (794 commits, 2025-04-06 → 2026-10-08). The scope, constraints and off-limits list are in `PREFLIGHT.md`. The owner confirmed them.

## Executive Summary

yahoo-finance-mcp is a public MCP server with 97 tools for market and SEC data. It is implemented **twice with strict output parity**: a Python server (`server.py` + `yfmcp/`) and a TypeScript Cloudflare Worker (`worker/src/`). That is about **59 KSLOC of production code** plus 32 KSLOC of tests. It is healthy where it counts: CI is 3–4 min and fully reproducible locally (65/65 green), the tool contract matches exactly across both runtimes, there are no orphan imports, and there are no hardcoded secrets. The risk is concentrated in two places. First, **two god files**, `server.py` (12.5k lines) and `worker/src/yahoo-finance.ts` (17.8k lines), hold all 46 of the functions with CCN ≥ 50, and a module extraction has stalled halfway. Second, the **public Worker is an unauthenticated open fetcher**: there is SSRF through user-supplied URLs, and `/mcp` spends paid API-key quota with no auth or rate limit. **Recommendation: Refactor in place, same stack.** Fix the two High security findings first. Then finish the domain-by-domain extraction into matching 1:1 Python/TS module pairs, each guarded by an execute-both parity harness.

## System Inventory

Tool: `scc` 3.7.0 (repo root; excludes `node_modules`, `__pycache__`, `analysis`). Complexity hotspots come from `lizard` 1.17.

**Whole repository**

| Language | Files | Lines | Blanks | Comments | Code | Complexity |
|---|---:|---:|---:|---:|---:|---:|
| Python | 117 | 64,667 | 4,529 | 2,760 | 57,378 | 8,775 |
| TypeScript | 28 | 32,628 | 1,770 | 1,909 | 28,949 | 9,354 |
| JSON | 14 | 764 | 0 | 0 | 764 | 0 |
| Markdown | 13 | 3,545 | 303 | 0 | 3,242 | 0 |
| YAML | 5 | 645 | 122 | 11 | 512 | 0 |
| TOML / JS / other | 5 | 141 | 18 | 13 | 110 | 13 |
| **Total** | **182** | **102,390** | **6,742** | **4,693** | **90,955** | **18,142** |

**Production code only** (excludes `scripts/` tests, `docs/` and `fixtures/`): Python 41 files / 30,183 SLOC; TypeScript 28 files / 28,949 SLOC. **59.1 KSLOC.**

**Highest-complexity files (scc)**

| File | Code | Complexity |
|---|---:|---:|
| `worker/src/yahoo-finance.ts` | 16,124 | 5,211 |
| `server.py` | 11,875 | 1,629 |
| `worker/src/capital-structure.ts` | 2,704 | 1,067 |
| `yfmcp/capital_structure.py` | 3,114 | 1,010 |
| `yfmcp/tools/earnings.py` | 1,734 | 581 |
| `worker/src/tools.ts` | 2,525 | 479 |
| `worker/src/evidence.ts` | 989 | 392 |

**Highest-complexity functions (lizard; production code, 2,648 functions, mean CCN 7.2)**

| CCN | NLOC | Function | File |
|---:|---:|---|---|
| 179 | 522 | `_get_filing_data_unchecked` | `server.py` |
| 159 | 139 | `extractGeographicRevenue` | `worker/src/yahoo-finance.ts` |
| 152 | 252 | `dilution_bridge` | `yfmcp/capital_structure.py` |
| 150 | 207 | `_dispatchTool` (104-case switch) | `worker/src/tools.ts` |
| 143 | 190 | `querySecFilingIndex` | `worker/src/yahoo-finance.ts` |
| 133 | 174 | `valuation_snapshot` | `yfmcp/valuation.py` |
| 127 | 234 | `capital_structure` | `yfmcp/capital_structure.py` |
| 124 | 218 | `extractExposure` | `worker/src/yahoo-finance.ts` |
| 117 | 288 | `search_filing_text` | `server.py` |
| 112 | 198 | `_compute_source_status` | `server.py` |

46 functions have CCN ≥ 50 and 210 have CCN ≥ 20.

### Technology fingerprint

| Area | Evidence |
|---|---|
| Languages / runtimes | Python ≥3.11 (`pyproject.toml`, `.python-version` = 3.11; CI uses 3.11). TypeScript 7.0 on Cloudflare Workers (`worker/package.json`, `wrangler.toml` `compatibility_date = 2026-09-21`, `nodejs_als`); CI uses Node 24 |
| Frameworks | Python: `mcp[cli]` FastMCP (`>=1.6.0,<3`), `yfinance` ≥1.4.1. TS: hand-rolled JSON-RPC MCP handler (`worker/src/mcp.ts`), hand-built Yahoo client (crumb/`yGet`) |
| Build / deps | `uv.lock` (PyPI), `worker/package-lock.json` (npm); wrangler 4.138 deploy with immutable version upload → candidate canaries → promote (`.github/workflows/deploy-worker.yml`) |
| Data stores | No database. Evidence store: R2 bucket `yfmcp-evidence` (Worker) / local dir `YFMCP_EVIDENCE_DIR` (Python), content-addressed write-once keys. Process-local TTL caches plus Cloudflare edge cache. JSON registries: `tool_catalog.json`, `worker/src/company-ir-page-registry.json`, `worker/src/news-source-capabilities.json` |
| Integration points | Upstream HTTP: SEC (`www.sec.gov`, `data.sec.gov`, `efts.sec.gov`), Yahoo (`query1`/`query2.finance.yahoo.com`), GlobeNewswire/Cision RSS, company IR pages, Finnhub, Alpha Vantage, Marketaux, UK Companies House, Thai SEC (`api.sec.or.th`). Downstream: MCP clients over stdio (Python) and HTTP POST `/mcp` (Worker, public) |
| Config / secrets | Env vars: `EDGAR_CONTACT_EMAIL`, `FINNHUB_API_KEY`, `ALPHA_VANTAGE_API_KEY`, `MARKETAUX_API_TOKEN`, `COMPANIES_HOUSE_API_KEY`, `SEC_OPEN_DATA_API_KEY`, `TOOL_MODE`, `MCP_ENVELOPE_V2`. Worker secrets come from GitHub secrets at deploy |
| Tests | 80 files / 1,268 `test_` functions in `scripts/`, run as 65 CI steps; about 26 files run the TS through node for execute-both parity. No line-coverage tooling is configured. The coverage signal is strong for the extracted leaf modules and weak for the god-file domains, which are guarded mostly by source-text assertions (Debt #2) |

## Architecture-at-a-Glance

Diagram: `ARCHITECTURE.mmd` (12 domains, 4 clusters). The contract is identical in `tool_catalog.json`, the Python registry and the Worker `TOOLS`: **97 actions in 12 groups**, with empty set differences in every direction.

| # | Domain | Python | TypeScript | LOC Py / TS |
|---|---|---|---|---|
| 1 | Transport and tool registry | `server.py` (tail), `yfmcp/app.py`, `tool_groups.py`, `yfmcp/tools/system.py` | `index.ts`, `mcp.ts`, `tools.ts`, `tool-catalog.ts` | ~900 / ~1,950 |
| 2 | Envelope, schemas and cross-cutting | `yfmcp/envelope.py`, `schemas.py`, `cache.py`, `util.py`, `validation.py`, `build_info.py`, `version.py` | `response.ts`, `request-context.ts`, `cache.ts`, `validate.ts`; schemas inline in `tools.ts` | ~1,900 / ~1,500 |
| 3 | Yahoo pricing, technicals, options, screening | `yfmcp/tools/pricing.py`, `yfinance`, `server.py` regions | `yahoo-finance.ts` (Yahoo client + regions) | ~3,500 / ~3,700 |
| 4 | Yahoo fundamentals, analyst, calendar | `server.py` regions | `yahoo-finance.ts` regions | ~2,400 / ~2,300 |
| 5 | Third-party fallbacks (key-gated) | `yfmcp/clients/market_providers.py`, `tools/provider_gaps.py`, `server.py` (Companies House) | `yahoo-finance.ts` regions | ~860 / ~885 |
| 6 | SEC EDGAR retrieval | `yfmcp/clients/edgar.py`, `parsing/html.py`, `filing_search.py`, `sec_facts.py`, `fiscal_calendar.py`, `companyfacts_coverage.py`, `server.py` regions | `filing-search.ts`, `sec-facts.ts`, `fiscal-calendar.ts`, `companyfacts-coverage.ts`, `yahoo-finance.ts` regions | ~5,450 / ~5,350 |
| 7 | SEC extractors and capital structure | `parsing/extractors.py`, `extraction_rules.py`, `capital_structure.py`, `share_scenarios.py`, `funding_schedule.py`, `driver_ledger.py`, `server.py` regions | matching `.ts` modules + `yahoo-finance.ts` regions | ~7,150 / ~6,900 |
| 8 | Valuation and reconciliation | `valuation.py`, `valuation_history.py`, `metric_reconciliation.py` | matching `.ts` modules | ~2,470 / ~2,240 |
| 9 | Earnings intelligence and transcripts | `yfmcp/tools/earnings.py`, `clients/yahoo_transcripts.py`, `earnings_period.py`, `guidance_history.py` | `yahoo-finance.ts` regions, `guidance-history.ts`, `earnings-period.ts`, `transcript-contract.ts` | ~3,200 / ~2,830 |
| 10 | News and public events | `server.py` only (no module) | `yahoo-finance.ts` only (no module) | ~3,240 / ~3,400 |
| 11 | Evidence layer | `evidence.py`, `evidence_store.py`, `server.py` (pack orchestration) | `evidence.ts`, `evidence-store.ts`, `evidence-pack.ts` | ~1,775 / ~1,767 |
| 12 | Thai SEC funds (optional) | `yfmcp/tools/thai_funds.py` | `sec-thailand.ts` | 1,093 / 820 |

**Structural observations**
- The **leaf algorithm modules map 1:1** across runtimes, with matching internal import graphs. Examples: capital_structure, share_scenarios, funding_schedule, driver_ledger, extraction_rules, guidance_history, valuation(_history), metric_reconciliation, filing_search, sec_facts, fiscal_calendar, evidence(_store). These are also the only parts with execute-both parity harnesses.
- The **god files don't hold the same domains**. Python has dedicated EDGAR, HTML-parsing, provider, pricing and earnings modules that TS buries in `yahoo-finance.ts`. TS has a dedicated `evidence-pack.ts` that Python buries in `server.py`. **News/events (~3.3k lines a side) has no module in either runtime.**
- **Hidden couplings:**
  - `yfmcp/tools/earnings.py` calls back into `server.py` through a lazy `import server`, creating a cycle.
  - The Python runtime reads `worker/src/*.json` at run time.
  - Exhibit tools are filed under earnings in Python but under SEC in TS.
- **Dangling items:**
  - `yfmcp/valuation.py:79` `market_inputs_from_quote_summary` is unused. Python feeds valuation from yfinance `.info`, while TS feeds it from raw quoteSummary. That divergence is a parity seam, and choosing one is a planner decision.
  - `fiscal_year_label` is used only by tests, in both runtimes.
  - Stale text in `server.py:12384,12399,12445` says "79 individual tools"; the real count is 97.
  - `tool_groups.py` starts with a UTF-8 BOM.

## Production Runtime Profile

**No telemetry available.** No APM/observability MCP is connected, and Cloudflare Workers logs (enabled in `wrangler.toml`) are not reachable from this environment. There are no p50/p95/p99 figures, and no domain can be ranked by operational variance. The closest signals are the deploy workflow's candidate and production smoke summaries. To add the overlay later, export Workers invocation logs (wall time per `tools/call` name) and re-run Step 4.

## Technical Debt

Ranked by remediation value. **[PLANNER]** marks fixes that change output or a fallback, which `CLAUDE.md` reserves for the planner.

1. **Python SEC filing tools bypass the shared EDGAR client.**
   - `list_sec_filings`, `get_filing_outline`, `get_filing_section`, `list_filing_tables` and `get_filing_table` (`server.py:3962–4256`) call `urllib.request.urlopen` directly. They hardcode a placeholder User-Agent, `yahoo-finance-mcp/1.0 admin@example.com`, at 7 sites, so the configured `EDGAR_CONTACT_EMAIL` is ignored and SEC fair-access throttling becomes a risk.
   - They re-download SEC's ~1 MB `company_tickers.json` on every call (the Worker caches it at `yahoo-finance.ts:5420`), and `ALLOWED_FORMS` excludes 20-F.
   - Live drift: Python accepts `accession_number` (`server.py:4038`), but the Worker ignores it (`yahoo-finance.ts:8625`).
   - Fix: route these calls through `yfmcp/clients/edgar.py`. The accession behaviour is a **[PLANNER]** choice.
2. **Most parity checks test source text, not behaviour.**
   - `check_tool_sync.py` regex-matches tool names, and about 209 assertions only check that a string appears in the TS source (e.g. `test_worker_sec_provider.py:26-93`). Execute-both harnesses exist only for the extracted modules.
   - Python re-implements JS number semantics in several places: `_js_json_numbers` at `server.py:5210`, `_js_round` at `parsing/extractors.py:236`, `_js_num` at `extraction_rules.py:188` (missing the 1e21 guard), and two *different* functions both named `_js_number`, at `sec_facts.py:155` and `capital_structure.py:245`.
   - Probable unconfirmed drift: with `sources=[]`, Python substitutes defaults (`server.py:8282`), while TS passes `[]` (`yahoo-finance.ts:11918`).
   - Fix: extend the execute-both harness to the god-file domains, and consolidate the JS-number helpers into one module.
3. **`extractChinaExposure` duplicates `extractExposure`** (`yahoo-finance.ts:16052` / `:16245`; `server.py:11152` / `:11353`).
   - The China tool hardcodes one issuer's subsidiaries, `["Tongmei","JinMei","BoYu"]` (`yahoo-finance.ts:16138`), and they appear to belong to AXTI.
   - The generic tool searches only the first 3 entries of `CHINA_NAMED_ENTITIES`.
   - The two tools have different schemas and status ladders.
   - There is a dead ternary at `yahoo-finance.ts:16335`, and Python swallows errors with `except Exception: pass` (`server.py:11171`).
   - Fix: make the China tool a preset over the generic one, with entity lists held as data **[PLANNER]**.
4. **God files, and the extraction has stalled halfway.** `server.py` has 12,460 lines, 262 top-level functions and 75 inline tool registrations. `yahoo-finance.ts` has 17,790 lines and 423 functions. `_dispatchTool` is a 104-case switch (`tools.ts:2009`). Every CCN ≥ 100 hotspot except `dilution_bridge`, `capital_structure` and `valuation_snapshot` sits inside them. Fix: extract one domain at a time into matching `yfmcp/<x>.py` and `worker/src/<x>.ts` modules, each with a harness.
5. **Swallowed errors turn a failed read into "no data".**
   - Python has 54 `except Exception:` handlers followed directly by `pass`, `return` or `continue` (e.g. `server.py:3497, 3568, 3727, 6917, 7820, 11125`).
   - TS has `catch { /* ignore */ }` at `yahoo-finance.ts:5466, 6211, 7005, 7601, 7779` and 21 `.catch(() => null)` calls.
   - This contradicts the project's own 2.5.17 rule that a failed read must be distinguished from a missing record.
   - Fix: audit each silent handler and emit warnings **[PLANNER]** if that needs new warning codes.
6. **Smoke-test CIK fixtures sit in the production lookup chain.** `SMOKE_TICKER_CIK_FALLBACKS` (11 tickers, `yahoo-finance.ts:535`, consulted at `:5475`; `yfmcp/clients/edgar.py:56`, consulted at `:334`) means those tickers still resolve during an SEC/Yahoo outage while every other ticker fails. Fix: move the map behind a test-only seam **[PLANNER]**, since it changes the fallback order.
7. **Private MCP SDK internals across a two-major-version range.** `mcp[cli]>=1.6.0,<3` allows both 1.x and 2.x. The code monkey-patches `FastMCP.tool` (`yfmcp/app.py:65,128`) and reads `_tool_manager._tools` (`app.py:299,315`). `yfmcp/tools/system.py:25` swallows any failure into an empty manifest. Each call also gets its own event loop (`asyncio.run` in a thread, `app.py:120`). Fix: pin one major version, use the public API and fail loudly.
8. **Upstream URLs are hardcoded and scattered.** There are about 15 inline `quoteSummary` and about 11 `v8/finance/chart` variants. The timeseries endpoint uses `query1` in some places and `query2` in others. The companyfacts URL is built 8 times. The Marketaux URL appears in both runtimes. Release 2.5.33 already had to chase a retired Yahoo endpoint. Fix: one endpoint-builder module per runtime, with a shared JSON URL table.
9. **Deprecated Python standard-library calls.** `asyncio.get_event_loop()` inside coroutines at 8 sites (e.g. `server.py:1551, 1732, 10875`; `edgar.py:90`). `datetime.utcnow()`/`utcfromtimestamp()` at 5 sites (e.g. `server.py:7204`, `util.py:52,122`). Function-local `import urllib.request` repeats the module-level import. Fix: use `get_running_loop()` / `asyncio.to_thread` and `datetime.now(UTC)`.
10. **Stale comments and leftover compatibility code** after the retirement of the Yahoo tab endpoint (2.5.33). The docstring at `server.py:1405` still describes `get_news(tab=…)`. The legacy `yahoo_finance` aggregate source (`server.py:2187-2277`) and fixture-only `_unpack_company_event_result` (`server.py:2707`) are still carried. Deprecated output fields have no removal plan. Fix: correct the comments now; the deprecation schedule is **[PLANNER]**.

## Security Findings

**Hardcoded credentials: none found** in tracked files or git history (only test placeholders). No `SECRETS.local.md` is needed. The two High findings (S-01, S-02) and S-05 were re-verified against the source by the planner.

| ID | CWE | Sev. | file:line | Finding | Remediation |
|---|---|---|---|---|---|
| S-01 | CWE-918 SSRF | **High** | `worker/src/yahoo-finance.ts:13268` (`parsePublicTranscript`), `:16787` (`classifyEarningsSourceUrl`), `:16801` (`fetchPublicHtml`); exposed at `tools.ts:574,566` | **The public Worker is an open fetch proxy.** `parse_public_transcript` checks only for `https://`, and `index_earnings_release` treats any non-SEC https URL as company IR. Both fetch with no host allow-list, follow redirects, and return up to 5 MB as text to anonymous callers | Host allow-list (SEC Archives, Yahoo transcript, approved IR-registry hosts via `companyIrPageUrlAllowed`), `redirect:"manual"` with re-validation of each hop **[PLANNER: behaviour change in both runtimes]** |
| S-02 | CWE-306 / CWE-770 | **High** | `worker/src/index.ts:111-148`; paid sinks `yahoo-finance.ts:279` (AV), `:388` (Marketaux), `:11402` (Finnhub), `:15831` (Companies House); denial memo `:238` | **`/mcp` has no auth and no rate limit, but spends paid or quota-limited keys.** Varying the arguments skips the caches, and `sources:["marketaux"]` forces a call. Draining a quota triggers a 6-hour denial memo that blocks every legitimate user | Bearer token or Cloudflare Access on `/mcp`, a Workers Rate Limiting binding, and a per-caller paid-provider budget **[PLANNER]** |
| S-03 | CWE-770 | Medium | `worker/src/mcp.ts:39-41`, `index.ts:118` | JSON-RPC batches of any size run under `Promise.all` with no body cap, which amplifies S-01 and S-02 | Cap at about 10 entries per batch and about 256 KB per body; limit concurrency |
| S-04 | CWE-918 SSRF | Medium | `yfmcp/tools/earnings.py:1039-1050`, `:129-138`, `:346` | Python mirror of S-01. `urlopen` follows https→http redirects, so on a user's machine a prompt injection could reach `localhost` or `169.254.169.254`. The existing `file://` test only covers `_validate_sec_url` | Same allow-list plus a redirect handler that re-validates each hop and rejects private IPs; add tests |
| S-05 | CWE-22 Path traversal | Medium | `yfmcp/evidence_store.py:51-56` (`LocalDirStore.list`); callers `server.py:12122,12183,12367` | `list()` builds `root / prefix` from an unvalidated ticker and runs `rglob`, skipping the `_path()` guard. `get_eps_revisions(ticker="../../..")` lists `.json` file stems outside the store and can walk the whole filesystem. Python only, and only when `YFMCP_EVIDENCE_DIR` is set | Validate the prefix and require `root in base.resolve().parents`; call `_validate_ticker` in the callers |
| S-06 | CWE-1333 ReDoS | Low | `worker/src/yahoo-finance.ts:12379` | An unescaped ticker is interpolated into `new RegExp` in `verifyCompanyEvent` | Escape it, or validate tickers centrally in `_dispatchTool` |
| S-07 | CWE-250/732 | Medium | `.github/workflows/deploy-worker.yml` | No top-level `permissions:`. Jobs that hold `CLOUDFLARE_API_TOKEN` also run `npm ci`, which runs package lifecycle scripts | Set `permissions: contents: read`, use `npm ci --ignore-scripts`, and put the Cloudflare secrets in a protected GitHub Environment |
| S-08 | CWE-732 | Low | `.github/workflows/ir-source-discovery.yml:8-10,42` | A write token persists in `.git/config` while the job parses untrusted websites; it then force-pushes | Split into a read-only discovery job and a separate write job; set `persist-credentials: false` and use `--force-with-lease` |
| S-09 | CWE-829 | Low | all workflows | Actions are pinned to mutable major tags, including in jobs that hold secrets | Pin to commit SHAs and let Dependabot update them |
| S-10 | CWE-1395 | Medium | `uv.lock`; `ci.yml:77` | pip-audit: **38 known vulnerabilities in 9 packages** (urllib3 2.3.0, requests 2.32.3, soupsieve, protobuf, anyio, httpx2/httpcore2, pygments, python-dotenv). CI's `pip install -e .` ignores the lockfile | Run `uv lock --upgrade-package …`, install with `uv sync --frozen` in CI, and gate on pip-audit |
| S-11 | CWE-1395 | Low | `worker/package-lock.json` | Production dependencies are clean. The dev toolchain (wrangler → miniflare → undici/sharp) has 4 high advisories and runs with the Cloudflare token in deploy | Bump wrangler to ≥4.149 and add `npm audit` to CI |
| S-12 | CWE-209 | Low | `worker/src/index.ts:141-145`, `mcp.ts:59-62`, `evidence-pack.ts:418,459` | Raw exception messages are returned to anonymous callers. Provider tokens are already redacted | Return generic messages and log the details server-side |
| S-13 | CWE-942 | Low | `worker/src/index.ts:45-51` | `Access-Control-Allow-Origin: *` on an unauthenticated endpoint that spends quota lets browsers amplify S-02 | Restrict origins once auth exists |
| S-14 | CWE-400 | Low | `worker/src/yahoo-finance.ts:9957-9971` | Chunked IR responses are buffered in full before the size check | Stream with a byte cap, as `fetchPublicHtml` does |
| S-15 | CWE-1188 | Low | `yahoo-finance.ts:5078`, `edgar.py:24`, `wrangler.toml:6` | A placeholder EDGAR contact is used when the secret is unset. `preview_urls = true` exposes candidate versions, which carry production secrets | Warn in `/health` or fail closed; put preview URLs behind Cloudflare Access |

**Defences that hold:** every `document_url` path pins `https://www.sec.gov/Archives/`; evidence `get` and `put` are guarded; provider tokens are URL-encoded and redacted; `/audit/mcp` uses a constant-time token comparison; the CI secrets file is mode 0600, never echoed, and removed with `if: always()`.

## Documentation Gaps

1. **No current-state architecture or runtime-mapping doc.** Nothing explains which domain lives where in each runtime, or why the god files split differently (e.g. EDGAR is a module in Python but buried in TS; evidence-pack is the reverse). `docs/provider-runtime-guidance.md` (2,663 lines) is an append-only per-release log, so a newcomer has to replay 40+ release sections to learn current behaviour. `.git-blame-ignore-revs` cites a `REFACTORING_PLAN.md` that does not exist.
2. **How parity is enforced is undocumented.** `CLAUDE.md` demands strict parity but doesn't say which tests execute both runtimes and which only check source text. It also doesn't cover the JS-number emulation rules (`_js_round`, `_js_number` ×2, the 1e21 guard), which are the most fragile part of the mirror.
3. **The public Worker's security model.** The README says "public data only" but says nothing about `/mcp` being unauthenticated, that paid provider keys are spent on anonymous calls, which tools fetch caller-supplied URLs, or the 6-hour provider-denial memo and how one caller can trigger it for everyone.
4. **Cross-runtime couplings and test seams in production.** Undocumented: Python reads `worker/src/*.json` at run time; `yfmcp/tools/earnings.py` back-imports `server`; `SMOKE_TICKER_CIK_FALLBACKS` steers production CIK resolution; the valuation inputs differ by runtime (yfinance `.info` vs raw quoteSummary).
5. **Stale in-code docs.** `server.py:12384,12399,12445` says "79 individual tools" (actual 97). The `server.py:1405` docstring describes the retired Yahoo `tab` endpoint. The deprecated output fields (`server.py:4527, 7890, 8005`) have no documented removal plan.

## Relative Scale

| Input | Value |
|---|---|
| Production KSLOC (Python + TS, scc "Code") | 59.1 |
| COCOMO-II index, production code: `2.94 × 59.1^1.10` | **≈ 261** |
| All-code KSLOC (incl. tests, docs, config) | 91.0 |
| COCOMO-II index, all code: `2.94 × 91.0^1.10` | ≈ 420 |

This is a **relative size and complexity index** for ranking this system against others. It is not an estimate of how long modernization will take or what it will cost. The formula assumes traditional human-team productivity, which agentic transformation does not follow, so no person-months, schedule, cost or date are given. One caveat specific to this system: about half the KSLOC is the deliberate second implementation, so the "unique logic" scale is roughly half the index suggests.

## Recommended Modernization Pattern

**Refactor (in place, same stack) → `/modernize-transform`, applied one domain at a time with the same language as source and target.** Both runtimes are on current toolchains (Python 3.11, TS 7, wrangler 4, compatibility date 2026-09), so there is no version uplift to do. A rebuild would throw away about 40 releases of hard-won SEC-extraction rules recorded in `provider-runtime-guidance.md`. The debt is structural:
- **two god files**
- **a parity net that is strong only where code was already extracted**
- **an unauthenticated public fetch surface**

The plan is therefore:
1. Close the two High security findings (S-01 SSRF, S-02 auth and rate limit) as planner-approved behaviour changes in both runtimes. The quick wins (S-05, S-07, S-10) can run alongside, via `/modernize-harden`.
2. Then use `/modernize-transform` to extract one domain at a time out of `server.py` and `yahoo-finance.ts` into matching 1:1 `yfmcp/<x>.py` and `worker/src/<x>.ts` modules. Each extraction lands with an execute-both parity harness that replaces the source-text assertions. Order: news/events (no module on either side), then SEC retrieval, the exposure extractors and pricing/options.

Fast local CI (3–4 min) makes this low-risk, provided each step respects the off-limits list in `PREFLIGHT.md` (frozen MCP contract, fixtures as oracle, two runtimes changed together). One question this assessment flags but does not answer: should the system keep two runtimes at all? Retiring the Python runtime in favour of the Worker, or the reverse, would roughly halve the code. That is a Rearchitect decision with consequences for local stdio users, and it belongs to `/modernize-brief` and the owner, not this pass.
