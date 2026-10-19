# Phase 4: extract the shared clients (hub), unit specs

This spec covers brief §3 "Phase 4 — Extract the shared clients (the hub)". Owner approval is recorded in brief §8 ("approve Phase 1, R4 and Phase 4"). The work follows `PLAYBOOK.md`; the precedent is `PHASE3_SPEC.md`.

It meets the open entry criterion "Each hub unit's spec lists its scope from the topology (`#shared` plus the client functions), as in `phase3_news_region.json`" with four region files:

| Unit | Order | Region file |
|---|---|---|
| EDGAR client (this phase's pilot slice) | 1 | `phase4_edgar_region.json` |
| Yahoo client | 2 | `phase4_yahoo_region.json` |
| *Q3g `MarketInputs` release (behaviour, not a move)* | *between 2 and 3* | — |
| HTML/table parsing | 3 | `phase4_html_region.json` |
| Endpoint builder | 4 | `phase4_endpoints_region.json` |

Each region file has the same shape as `phase3_news_region.json` (`{file: [{name, line, nloc}]}`, `server.py` first). Each entry adds two fields:
- `region`: the topology region the function sits in today;
- `dest`: the proposed destination module.

**Planner decisions are collected in §11.** Nothing in this spec settles one of them. Where a default is written, it is a proposal.

## 0. Gates and provenance

- **Merge gate: telemetry baseline (Q10).** No Phase 4 PR merges before the baseline window is captured.
  - The window counts from 2026-10-17, so the first eligible day is **2026-10-24**.
  - Exclude the preview-canary requests (Q3i finding F6): about 15 per deploy, counted by Analytics Engine but never seen in Workers Logs. Filter out deploy-hour canary actions, or tag the preview version.
  - Specs, branches and local verification may start before then; merging may not.
- **Source-text cleanup** (entry criterion, met). `scripts/source_text.py` reads `worker/src/**/*.ts` and `yfmcp/**/*.py` recursively, so new client modules are covered automatically.
- **Topology provenance.**
  - Regenerated on 2026-10-19 at HEAD `486e23d` (`server.py` and `yahoo-finance.ts` are unchanged since `a9df015`).
  - `extract_topology.py` only writes in place and derives `ROOT` from its own path. It was run as a copy with `ROOT` pinned to the repo and `OUT` sent to a scratch file, plus a `func_region` dump.
  - The committed `topology.json` (2026-10-16) is **not** updated. Commit the regenerated topology once, after both halves of a unit land (playbook §5).
  - Drift since 2026-10-16:

    | Region | NLOC 2026-10-16 | NLOC now |
    |---|---|---|
    | `server.py#shared` | 1929 | 2019 |
    | `yahoo-finance.ts#shared` | 1629 | 1623 |
    | `server.py#sec` | 1431 | 1475 |
    | `yahoo-finance.ts#sec` | 2922 | 3065 |

- **Current `#shared` regions:**

  | Region | Functions | NLOC |
  |---|---|---|
  | `server.py#shared` | 44 | 2019 |
  | `yahoo-finance.ts#shared` | 73 | 1623 |

## 1. Rule zero: output is byte-identical (with one stated exception)

Every unit is a **move**:
- No behaviour, text, key order, number formatting, warning, status, schema, cache key or edge-cache URL may change.
- The parity harness passes with no expected value edited.
- Pinned `FINDING` divergences stay as they are.
- Do not fix anything you notice. List it in the report instead.

**The one exception is the brief's Python EDGAR routing** ("Route the Python SEC filing tools through `yfmcp/clients/edgar.py`. This changes the User-Agent and adds caching"):
- It is a behaviour change. Under brief §3 ("two kinds of change, never mixed in one PR") and `CLAUDE.md`, it needs its own release, with its own parity tests, docs section in `docs/provider-runtime-guidance.md` and version bump.
- It must not ride inside the unit-1 move PR (decision D2).
- §3.6 spells out exactly what changes.

## 2. Scope summary

The counts and NLOC are the topology's per-function counts. A function's text runs to the next top-level definition, so interleaved constants count toward the function above them.

| Unit | Runtime | Functions | NLOC | From `#shared` | From other regions | Destination |
|---|---|---|---|---|---|---|
| 1 EDGAR | Worker | 37 | 642 | 21 (445) | 16: 14 `#sec`, 1 `#providers`, 1 `#earnings` | `worker/src/clients/edgar.ts` (new) |
| 1 EDGAR | Python | 9 | 126 | 6 (102) | 3 `#extractors` (pulled-in dependencies) | `yfmcp/clients/edgar.py` (exists, 408 lines) |
| 2 Yahoo | Worker | 36 | 546 | 25 (469) | 11: `#pricing`, `#fundamentals`, `#providers`, `#earnings` | `worker/src/clients/yahoo.ts` (26 functions) + `worker/src/clients/provider-http.ts` (10 functions) |
| 2 Yahoo | Python | 1 | 8 | 1 | — | `yfmcp/clients/yahoo.py` (exists, 20 lines) |
| 3 HTML | Worker | 13 | 207 | 10 (157) | 3: 2 `#earnings`, 1 `#sec` | `worker/src/clients/html.ts` |
| 3 HTML | Python | 0 | 0 | 0 | — | `yfmcp/parsing/html.py` (already holds the Python helpers) |
| 4 Endpoints | Worker | 1 | 17 | 0 | 1 `#sec` | `worker/src/clients/endpoints.ts` |
| 4 Endpoints | Python | 1 | 15 | 1 | — | `yfmcp/clients/endpoints.py` |
| **Residual `#shared`, no client home** | Worker | **17** | **552** | 17 | — | **decision D1** |
| **Residual `#shared`, no client home** | Python | **36** | **1894** | 36 | — | **decision D1** |

The `worker/src/clients/` directory is proposed to mirror `yfmcp/clients/`. The brief's names are `edgar-client` / `yahoo-client` / `html` / `endpoints`. The directory choice has tooling consequences (§9, decision D3).

## 3. Unit 1: EDGAR client (pilot slice)

### 3.1 Worker scope (37 functions, `yahoo-finance.ts` → `worker/src/clients/edgar.ts`)

- **From `#shared` (21):**
  - **Document cache:** `countSecDocumentCache` 4946, `secDocumentEdgeRequest` 4961, `readSecDocumentEdge` 4965, `writeSecDocumentEdge` 4982, `loadSecDocument` 5003, `fetchSecDocument` 5022.
  - **JSON and HTML reads, URLs:** `edgarGetJson` 5073, `edgarGetHtml` 5102, `edgarBuildFilingUrls` 5154, `edgarListExhibitsFromIndex` 5207.
  - **CIK and submissions:** `edgarCikFromAccession` 5251, `getEdgarTickerCikMap` 5272, `edgarResolveCik` 5303, `resolveCikForTicker` 5308, `getSubmissionsForTicker` 5389.
  - **Filing resolution:** `uniqueRecentForms` 5421, `filingNotFoundPayload` 5431, `isAccessionNotFound` 5465, `accessionNotFoundEnvelope` 5470, `legacyAccessionMissPayload` 5478, `resolveSecFiling` 5489.
- **Client functions outside `#shared` (16):**
  - **`#sec` (14):**
    - transport and User-Agent: `edgarUserAgent` 4920, `edgarFetch` 4924 (the single SEC network primitive);
    - cache stats: `secDocumentCacheStats` 4951 (exported; `index.ts` imports it);
    - reads: `edgarReadJson` 5089, `edgarStreamHtml` 5119;
    - document references: `normalizeEdgarDocumentRef` 5167, `edgarDocumentFileName` 5182, `edgarDocumentUrlFromRef` 5187, `edgarDocumentUrlFromIndexUrl` 5197;
    - exhibit index: `edgarReadExhibitIndex` 5215, `parseExhibitIndexHtml` 5220;
    - ticker map: `edgarTickerMapLoaded` 5298 (imported by `news/providers/sec.ts`);
    - filing resolution: `accessionNotFoundError` 5455, `isLikelyXbrlDocumentUrl` 5484.
  - **`#providers` (1):** `setTickerCikFixturesForTests` 421 (the R4 Q4 CIK fixture hook; the parity cases call it as `m.yahooFinance.setTickerCikFixturesForTests`).
  - **`#earnings` (1):** `resolveEx991Url` 13657. It is one of the 23 helpers, imported by `news.ts`, and reads the EDGAR exhibit index.
- **Considered and left out** (consumers of the client, not client code):
  - `secFiscalYearNaming` 5379 (`#sec`; exported, used by `evidence-pack.ts`);
  - `filingManualLookup` 6134 (unit 4);
  - every `#sec` tool that calls `edgarGetJson` / `fetchSecDocument`.
- **Constants and state that move with the scope** (used only by moved functions; verify each):
  - `EDGAR_FALLBACK_CONTACT`, `EDGAR_FETCH_TIMEOUT_MS`;
  - `SEC_DOCUMENT_*` (memory TTL, edge TTL, edge cache version, cache max chars, read max chars), `secDocumentBodies`, `secDocumentInflight`, `secDocumentCounters`, `SecDocumentResult`;
  - `FILING_CIK_TTL_MS`, `FILING_SUBMISSIONS_TTL_MS`, `filingCikCache`, `filingSubmissionsCache`;
  - `EDGAR_TICKER_MAP_TTL_MS`, `edgarTickerMap`, `edgarTickerMapLoad`, `tickerCikFixtures` (one test asserts the exact text `let tickerCikFixtures: ReadonlyMap<string, string> = new Map();`), `EdgarJsonRead`, `ResolvedSecFiling`.
  - Interleaved constants that stay unless only moved code reads them: `FILING_FACT_CONCEPTS`, `FILING_INDEX_CACHE_MAX`, `FILING_INDEX_TTL_MS`, `filingIndexCache` (these read as `getFilingDataUnchecked` / filing-index state).
- **Cross-unit dependencies of the moved code** (import back from `yahoo-finance.ts` during unit 1, or move first; decision D4):

  | Dependency | Owning unit |
  |---|---|
  | `yGet`, `enc` (used by `resolveCikForTicker`'s Yahoo `secFilings` lookup) | Yahoo, unit 2 |
  | `fetchProviderWithTimeout` (the transport under `edgarFetch`), `edgeCache` | provider transport, unit 2 |
  | `stripHtmlTags` | HTML, unit 3 |
  | `countersSince` (a mutable `let` shared by `secDocumentCacheStats` and the Yahoo `markYahooCacheActivity`/`yahooCacheStats`) | Yahoo, unit 2 |

  - Leaf imports already available: `./cache.js` (`BoundedTtlCache`, `setBounded`), `./request-context.js`, `./response.js` (`getWorkerVar`), `./sec-facts.js`.

### 3.2 Python scope (9 functions, `server.py` → `yfmcp/clients/edgar.py`)

- **From `#shared` (6):** `_edgar_read_json` 2069, `_accession_not_found_error` 2096, `_is_accession_not_found` 2106, `_accession_not_found_envelope` 2110, `_resolve_sec_filing` 2124, `_warm_sec_submissions` 7997.
- **Pulled in from `#extractors` (3):** `_legacy_accession_miss_payload` 2115, `_filing_not_found` 6945, `_is_likely_xbrl_document_url` 277.
  - `_resolve_sec_filing` and `_legacy_accession_miss_payload` call `_filing_not_found` and `_is_likely_xbrl_document_url`. The client must not import `server` (playbook §2), so these dependencies move too.
  - Their Worker twins (`filingNotFoundPayload`, `legacyAccessionMissPayload`, `isLikelyXbrlDocumentUrl`) are in the Worker scope, so both runtimes end up with the same split.
  - `_filing_not_found` has 10 call sites in `server.py`, so `server.py` re-exports it.
- **Already in `yfmcp/clients/edgar.py`** (not moved again):
  - `_sec_user_agent` / `_SEC_REQUIRED_UA`, `_load_edgar_tickers`, `_edgar_ticker_map_loaded`;
  - `_edgar_get`, `EdgarError`, `_edgar_get_company_facts`, `_edgar_get_submissions`;
  - `_edgar_build_filing_urls`, `_edgar_cik_from_accession`;
  - `_edgar_list_exhibits_from_index`, `_edgar_read_exhibit_index`, `_parse_exhibit_index_html`;
  - `_fetch_edgar_html_status`, `_fetch_edgar_html`, `_edgar_get_html` (30-minute archive cache with shared in-flight requests);
  - `_resolve_cik_for_ticker`, `_get_submissions_for_ticker`, `_TEST_TICKER_CIK_FIXTURES`.
- **Python twin outside the god file:** `resolveEx991Url`'s twin `_resolve_ex991_url` lives in `yfmcp/tools/earnings.py`, and `server._read_release_text` imports it lazily. Whether it moves to the client for symmetry is decision D6.

### 3.3 Importers to re-point (Worker)

`news.ts` and `news/**` must import the EDGAR names from the client module, not from `yahoo-finance.ts`:

| Importer | EDGAR names it imports from `yahoo-finance.ts` today |
|---|---|
| `worker/src/news.ts` | `edgarCikFromAccession`, `resolveEx991Url`, `getSubmissionsForTicker` |
| `worker/src/news/providers/sec.ts` | `edgarGetHtml`, `edgarTickerMapLoaded`, `getSubmissionsForTicker` (its `stripHtmlTags` and `_str` imports wait for units 3 and D1) |

Re-pointing these is recommended but not required by the brief:
- `worker/src/evidence-pack.ts` imports `getSubmissionsForTicker`;
- `worker/src/index.ts` imports `secDocumentCacheStats`.

Following playbook §3, `yahoo-finance.ts` re-exports every moved symbol that was exported, so that `tools.ts`, the parity bundle (`m.yahooFinance.getSubmissionsForTicker`, `m.yahooFinance.setTickerCikFixturesForTests`) and `index.ts` keep working.

### 3.4 Identity proofs (playbook §2 and §3)

- **Worker:**
  - The moved text keeps its line multiset, with only `export` keywords added.
  - `listVisibleTools()` manifest diff in expanded and grouped modes.
  - `wrangler deploy --dry-run` bundle size before and after (news was +16 bytes).
  - Cycle scan: no top-level declaration in `clients/edgar.ts` or `yahoo-finance.ts` reads the other's bindings. Watch the `countersSince` live binding, which is read at call time only.
  - Evaluate each of `clients/edgar.ts`, `yahoo-finance.ts`, `tools.ts` and `index.ts` as the sole bundle entry in Node.
  - **Edge-cache identity:** `secDocumentEdgeRequest` URLs and `SEC_DOCUMENT_EDGE_CACHE_VERSION` must be unchanged. Otherwise the deploy silently invalidates the edge cache, and the telemetry comparison shows a latency regression that is not a code regression.
- **Python:**
  - `ast.dump` of each of the 9 moved functions equals the `git archive` base.
  - Expanded and grouped `list_tools` diff (names, order, descriptions, schemas).
  - `health_check` `manifestHash` and `schemaHash` unchanged.
  - No `import server` in `yfmcp/clients/edgar.py`.
- **Both:** the parity harness passes with 0 expected values changed. Run `check_tool_sync.py`, `test_grouped_contract_parity` and `tsc --noEmit`.

### 3.5 Test plumbing risks (patch targets that would go stale)

The harness refuses network access, so a stale patch fails loudly rather than calling SEC. It still costs a debugging loop. Retarget patch targets only; never change an assertion.

**Python, the main risk: split bindings.**
- `server.py` imports client names by value (`from yfmcp.clients.edgar import _get_submissions_for_ticker, …`).
- After the move, code left in `server.py` looks names up in `server`, and moved code looks them up in `yfmcp.clients.edgar`.
- A test whose call path crosses both needs **both** patch targets.

| Patch target today | Sites | Files | Stale after unit 1 when… |
|---|---|---|---|
| `server._get_submissions_for_ticker` / `patch.object(srv, "_get_submissions_for_ticker")` | 7 + 17 = 24 | `test_sec_intelligence.py` (12), `test_extractor_schemas.py` (3), `test_capital_structure.py`, `test_filing_search.py`, `test_input_validation.py`, `test_local_data_accuracy.py`, `test_valuation.py`, `test_companyfacts_coverage.py`, `test_engine_zero_2531.py`, `test_evidence_tools.py`, `test_sec_fact_payloads.py` | …the path goes through the moved `_resolve_sec_filing`, `_legacy_accession_miss_payload` or `_warm_sec_submissions`. Add `yfmcp.clients.edgar._get_submissions_for_ticker`. |
| `server._edgar_get` | 2 | `test_release_regressions.py:220`, `test_sec_fact_payloads.py:520` | …the path goes through the moved `_edgar_read_json`. Add `yfmcp.clients.edgar._edgar_get`. |
| `server._warm_sec_submissions` | 1 | `parity/cases_pricing_evidence_tools.py:1990` | Safe while its callers stay in `server.py` and `server` re-exports it. It goes stale if a caller moves. |
| `server._load_edgar_tickers` (3), `server._resolve_cik_for_ticker` (3) | 6 | `test_input_validation.py`, `test_release_regressions.py`, `test_sec_fact_payloads.py` | Already affect only `server`-resident callers; the client's `_resolve_cik_for_ticker` uses its own binding. They stay safe after the move. They become relevant in the routing release (§3.6), where `list_sec_filings` starts calling client CIK resolution. |
| `server._edgar_get_html` (1), `srv._edgar_get_html` (8), `srv._edgar_get_company_facts` (2), `server._edgar_get_company_facts` (1), `srv._edgar_list_exhibits_from_index` (1) | 13 | `test_sec_intelligence.py`, `test_capital_structure.py`, `test_filing_search.py`, `test_input_validation.py`, `test_valuation.py`, `test_sec_fact_payloads.py`, `test_companyfacts_coverage.py` | Safe for the move (their callers stay in `server.py`). The routing release changes which tools read through `_edgar_get_html`. |
| `srv._valuation_market_inputs` | 1 | `test_valuation.py:449` | Unit 2: safe while callers stay in `server.py`. |

- **Re-exports:** `server.py` re-exports every moved name that `server.py`, a script or a test reads as `server.<name>`.
- **Constants:** do not re-export a constant that tests patch (`_EDGAR_TICKERS` and friends are already re-exported as stale copies; see §10).

**Worker:**
- **Re-exports needed by the parity bundle:** `setTickerCikFixturesForTests` and `getSubmissionsForTicker` (`test_worker_accession_cik.py`, `parity/cases_pricing_evidence_tools.py:1098`). The bundle (`scripts/parity/harness.py:252`) globs **top-level** `worker/src/*.ts` only, so a module under `worker/src/clients/` is not exposed as `m.<key>`. Re-exports from `yahoo-finance.ts` keep every `m.yahooFinance.*` call working.
- **Source-text checks**, all through `source_text.py`, so they relocate:
  - `find_definition("worker", "resolveEx991Url")` (`test_worker_doctrine_safety.py:254`);
  - `isLikelyXbrlDocumentUrl` in `worker_runtime_source()` (`test_sec_reliability_worker.py:47`);
  - the `setTickerCikFixturesForTests` / `tickerCikFixtures` regexes (`test_worker_accession_cik.py:251-253`).
- **Weakened silently:** `test_worker_accession_cik.py:246` loops over `(WORKER / "src").glob("*.ts")` (top level) to assert `SMOKE_TICKER_CIK` is gone, so it would not see `worker/src/clients/edgar.ts`. The `worker_runtime_source()` check that follows still covers it.

### 3.6 The Python EDGAR routing release (behaviour change; separate PR and release)

**Target functions.** Five `server.py` functions (`#sec`) issue raw `urllib.request.Request` calls with the hard-coded User-Agent `yahoo-finance-mcp/1.0 admin@example.com`, and bypass the client:

| Function | Public action(s) | Raw fetches |
|---|---|---|
| `list_sec_filings` 818 | none directly. Called by `_resolve_latest_sec_doc_url` 5251, which `get_sec_filing_section_markdown`, `list_sec_filing_tables` and `get_sec_filing_table` use when `document_url` is omitted | `company_tickers.json` (timeout 10 s, fetched on every call), `data.sec.gov/submissions/CIK….json` (timeout 10 s, no cache) |
| `get_filing_outline` 886 | `get_sec_filing_outline` | the document (timeout 15 s, whole body, `decode("utf-8", errors="replace")`; `HTTPError` becomes `PROVIDER_ERROR "HTTP <code>"` through `_outline_read_failure`) |
| `get_filing_section` 957 | `get_sec_filing_section` | the document (same; any exception becomes `PROVIDER_ERROR str(e)`) |
| `list_filing_tables` 1069 | `list_sec_filing_tables` | the document (same) |
| `get_filing_table` 1126 | `get_sec_filing_table` | the document (same) |

**What changes when they go through `yfmcp/clients/edgar.py`:**

1. **User-Agent.** `yahoo-finance-mcp/1.0 admin@example.com` becomes `_SEC_REQUIRED_UA`, which is `yahoo-finance-mcp ${EDGAR_CONTACT_EMAIL or contact@example.com}`. That is the same contact the Worker sends.
   - No test asserts the old string.
   - `scripts/parity/record_fixtures.py:26` uses its own recorder User-Agent (also `admin@example.com`); it is a script, not runtime.
2. **Caching.** Which cache applies depends on the client function chosen (decision D2b):
   - **Ticker index:** `_load_edgar_tickers`, 24 h. Inherits Q3i **F7**: a failed load caches `{}` and is not retried for up to a day while uptime is under 24 h. Today `list_sec_filings` refetches every call and reports a failed load as `PROVIDER_ERROR`; after the change the same failure becomes `TICKER_NOT_FOUND`.
   - **Submissions:** `_edgar_get_submissions`, a **24 h** cache. The Worker's `FILING_SUBMISSIONS_TTL_MS` is **15 min**. A newly filed 10-K could stay invisible to Python's "latest document" resolution for up to a day. Today it is fresh on every call.
   - **Archive documents:** `_edgar_get_html`, a 30-minute cache with in-flight dedupe, process-global.
3. **CIK resolution semantics.**
   - Today: exact match in the ticker index only, else `TICKER_NOT_FOUND "Could not find EDGAR CIK for ticker '<t>'"`.
   - `_resolve_cik_for_ticker` adds yfinance `.info["cik"]`, the test fixtures, the EDGAR atom fallback and `_FILING_CIK_CACHE`. That matches the Worker's `resolveCikForTicker`, but tickers that miss today could start to resolve.
   - Decision D2b: index-only lookup through the client, or full client resolution.
4. **Error text.** Today `PROVIDER_ERROR` carries urllib's `str(e)`, for example `HTTP Error 404: Not Found`.
   - `_edgar_get` raises `EdgarError("SEC API returned HTTP 404: Not Found")`.
   - `_edgar_get_submissions` returns `None` on any `EdgarError`, so the current code would then fail with `'NoneType' object has no attribute 'get'`.
   - The message for each failure needs a decision.
5. **Document reads.** The existing client read does not fit:
   - `_edgar_get_html` caps reads at `max_bytes` (default 5 MB; a 10-K over 5 MB would be truncated), uses a 30 s timeout, decodes UTF-8 strictly with a latin-1 fallback (different from `errors="replace"` on invalid UTF-8), and returns `None` on failure, which **loses the HTTP status**.
   - The Worker's `fetchSecDocument` reads the whole body and keeps the status (`{ok:false,status}`), with a 20 s timeout.
   - Matching the Worker needs a status-returning, uncapped client read. That is a new client function, `_edgar_get_document`, or `_fetch_edgar_html_status` with no cap behind the archive cache. Its timeout, cap and decoding are decision D2c.

**Parity and acceptance cases that would notice:**
- `parity/cases_accession.py`, the 11 `r4b2.outline.*` cases. In particular `r4b2.outline.document_read_failure_is_provider_error` expects `{"code":"PROVIDER_ERROR","message":"HTTP 404"}`, which needs the status (item 5).
  - `_python_sec_state()` already clears `edgar._EDGAR_HTML_CACHE`, `_EDGAR_SUBS_CACHE` and `_FILING_CIK_CACHE` and resets the ticker index, so these cases are cache-safe.
- `scripts/test_local_data_accuracy.py:138` patches `yfmcp.url_policy.urlopen` for `get_filing_section`. The client also uses `_url_policy.urlopen`, so the patch still works. But the process-global 30-minute `_EDGAR_HTML_CACHE` can now serve a document cached by an earlier test with the same URL. Add a cache clear (plumbing only).
- `scripts/test_earnings_tools.py` patches `yfmcp.url_policy.urlopen`. Re-run it.
- `parity/cases_validation.py` and `parity/cases_pricing_evidence_tools.py` reference the filing tools. Re-run them.
- **Live:** gate 3 below, plus a local Python structure snapshot of `get_sec_filing_outline`, `get_sec_filing_section`, `list_sec_filing_tables`, `get_sec_filing_table`, and `list_sec_filing_tables` without `document_url`, before and after, against live SEC.
- **Docs:** a `docs/provider-runtime-guidance.md` section stating the User-Agent, cache TTLs and any message changes; a version bump.

### 3.7 Playbook gate 3: live baseline to capture before unit 1

Capture structure only on the deployed Worker, as `phase4_edgar_live_before.json` in the shape of `phase3_live_before.json`: data key sets, envelope and `data.meta` key sets, `status`, error codes, warning codes, coverage and counts. Record no item contents.

| Action | Params | Exercises |
|---|---|---|
| `health_check` | — | build identity, `manifestHash`, `schemaHash`, and the `secDocumentCacheStats` / `yahooCacheStats` key sets |
| `list_sec_company_filings` | AAPL; a 20-F filer (e.g. TSM); an unknown ticker | `getSubmissionsForTicker`, CIK map, `TICKER_NOT_FOUND` |
| `list_sec_material_filings` | AAPL | submissions |
| `get_sec_filing_outline` | AAPL + real 10-K accession; fake accession `0000320193-99-999999` | `resolveSecFiling`, `fetchSecDocument`, `ACCESSION_NOT_FOUND` |
| `get_sec_filing_section` | AAPL, `Item 1A`, document URL | `fetchSecDocument` |
| `list_sec_filing_tables`, `get_sec_filing_table` | AAPL, with and without `document_url` | document cache, resolution |
| `get_sec_filing_index`, `query_sec_filing_index` | AAPL | `_buildFilingIndexFromHtml` over client reads |
| `search_sec_filing_text` | AAPL, one term | `fetchSecDocument`, `edgarStreamHtml` |
| `extract_sec_filing_fact` | AAPL revenue; a 20-F filer | `edgarGetJson` companyfacts, `AUTO_20F_FALLBACK` |
| `get_sec_filing_intelligence` | AAPL | resolution and fallbacks |
| `list_sec_filing_exhibits`, `get_sec_filing_exhibit_content` | AAPL 8-K | `edgarListExhibitsFromIndex`, `parseExhibitIndexHtml` |
| `get_sec_recent_events`, `get_company_press_releases` | AAPL | news paths through `getSubmissionsForTicker`, `edgarCikFromAccession`, `resolveEx991Url`, `edgarTickerMapLoaded` |
| `get_latest_earnings_release` | AAPL | `resolveEx991Url`, `edgarGetHtml` |

Also capture the telemetry baseline slice for `sec_filings` actions (latency p50/p95 and error class), excluding preview canaries (F6). The unit-1 exit compares against it.

## 4. Unit 2: Yahoo client

- **Worker scope (36):**
  - **To `worker/src/clients/yahoo.ts` (26):**
    - `#shared`: `alternateYahooHost`, `refreshCrumb`, `getCrumbIfAvailable`, `getCrumb`, `invalidateCrumb`, `countYahooCache`, `yGetText`, `yahooEdgeTtlMs`, `isEdgeCacheableYahooBody`, `yahooEdgeRequest`, `readYahooEdge`, `writeYahooEdge`, `fetchYahooBody`, `yGet`, `raw`, `iso`, `noData`, `fetchTimeseriesByDate`, `valuationMarketInputs`;
    - pulled in: `pause` (`#pricing`), `markYahooCacheActivity`, `yahooCacheStats`, `yahooCacheUrl`, `yahooGetKey`, `evictYahooGet` (`#pricing`), `yPost` (`#fundamentals`).
  - **To `worker/src/clients/provider-http.ts` (10):**
    - `#shared`: `providerCacheKey`, `getProviderCache`, `setProviderCache`, `providerMessageStatus`, `providerHttpStatus`, `fetchProviderWithTimeout`;
    - pulled in: `providerCacheName` (`#providers`), `providerEdgeRequest`, `edgeCache`, `deleteProviderCache` (`#earnings`).
  - Before moving a pulled-in function, verify that only client code calls it. Otherwise import it back. `edgeCache` has 7 references.
  - **Constants and state:** `UA`, `ProviderJsonResult`, `ProviderCacheEntry`, `EdgeCache`, `PROVIDER_JSON_CACHE_MAX`, `providerJsonCache`, `YAHOO_*` (API hosts, fetch timeout, GET TTL, cache max, edge TTLs, rate-limit retry, edge cache version), `CRUMB_RATE_LIMIT_COOLDOWN_MS`, `_crumb`, `_crumbRefresh`, `_crumbUnavailableUntil`, `YahooHttpError`, `YahooCrumbRateLimitError`, `yahooGetBodies`, `yahooGetInflight`, `yahooCacheCounters`, `countersSince`, `enc`.
  - **Not moved:** `PROVIDER_ENTITLEMENT_DENIAL_TTL_MS` and `providerDenials` belong to `rememberProviderDenial` (`#providers`). `ANALYST_*_GRADES` sit after `noData` but belong to fundamentals.
- **Yahoo fetches outside `yGet`** (noticed, not in the move): `fetchTickerEarningsHistory` 2988 (`#fundamentals`) calls `providerFetch` on `finance.yahoo.com` directly with `UA`. The transcript fetch at 10273 uses `fetchProviderWithTimeout`.
- **Python scope (1):** `_valuation_market_inputs` 7914 (`#shared`) to `yfmcp/clients/yahoo.py`. It is the Python twin of `valuationMarketInputs`, which Q3g makes the `MarketInputs` owner.
  - The remaining yfinance use in `server.py` (`yf.Ticker(...)` in roughly every pricing and fundamentals tool; `server.yf` is patched in 12 files) is consumer code, not client code.
  - Wrapping it is not a move. It is the V6/Q3g question (raw quoteSummary in Python). Decision D7.
- **Importers to re-point:**

  | Importer | Names from this unit |
  |---|---|
  | `news.ts` | `yGet`, `enc` |
  | `news/providers/yahoo.ts` | `yGet`, `enc` |
  | `news/providers/marketaux.ts` | `ProviderJsonResult`, `providerCacheKey`, `getProviderCache`, `setProviderCache`, `providerHttpStatus`, `fetchProviderWithTimeout`, `UA` |
  | `news/providers/finnhub.ts`, `globenewswire.ts`, `company-ir-page.ts` | `UA` |
  | `evidence-pack.ts` (recommended) | `yGet` |
  | `index.ts` (recommended) | `markYahooCacheActivity`, `yahooCacheStats` |

  `clients/edgar.ts` re-points `yGet`, `enc`, `fetchProviderWithTimeout` and `edgeCache` here, if unit 1 imported them back.
- **Patch risk:**
  - Python: `srv._valuation_market_inputs` (`test_valuation.py:449`) is safe while its callers stay in `server.py`.
  - Worker parity: `raw` and `iso` are reached by text in 9 test files, mostly inside JS snippets that `m.yahooFinance` does not need. Check that `raw` stays reachable wherever a slice-and-run appends it.
- **Live baseline:** `get_stock_info`, `get_fast_info`, `get_historical_prices`, `get_option_chain`, `get_analyst_consensus`, `get_valuation_snapshot`, `search_ticker`, `screen_stocks`, `get_market_snapshot`, `health_check` (`yahooCacheStats` key set). Watch crumb behaviour and throttling (brief risk 2) in the telemetry comparison.

## 5. Q3g `MarketInputs` release (between units 2 and 3)

This is not a move. It is a behaviour release:
- both runtimes consume `MarketInputs` from the Yahoo client;
- Python reads quoteSummary directly (brief V6);
- the unused helper is removed or repurposed only after fixtures prove the replacement.

Scope anchors: `valuationMarketInputs` (Worker) and `_valuation_market_inputs` (Python), both moved by unit 2. Any new field or warning code goes through the planner gate.

## 6. Unit 3: HTML/table parsing

- **Worker scope (13), to `worker/src/clients/html.ts`:**
  - `#shared`: `stripHtmlTags`, `htmlToReadableText`, `filterParagraphsByTopics`, `parseFinancialTableRows`, `parseNumericCell`, `statedUnitScales`, `documentUnitScale`, `detectUnitScale`, `lazyDocumentScale`, `tableUnitScale`;
  - pulled in: `decodeHtmlEntities`, `sanitizeFilingHtml` (`#earnings`), `mergeFinancialCells` (`#sec`).
  - Constants that move: `HTML_ENTITY_MAP`, `DASH_CELL_RE`, `TOTAL_LABELS`, `UNIT_SCALE_RE`, `UNIT_SCALE_WORD`, `UNIT_SCALE_ASSUMED_WARNING`, `UnitScaleSource`.
  - **Cross-unit dependencies:** `raw` (Yahoo, unit 2), and `parseHtmlTable`, which the Phase 3 move put in `news.ts` and `yahoo-finance.ts` imports back. Relocating `parseHtmlTable` from `news.ts` into `clients/html.ts` is in the spirit of "the hub phase relocates them later", but it edits `news.ts` (decision D5).
- **Python scope: 0 functions.** `server.py#shared` holds no HTML-parsing function, and `yfmcp/parsing/html.py` already holds `_strip_html_tags`, `_strip_html_blocks`, `_parse_html_table`, `_parse_numeric_cell`, the unit-scale helpers and the Markdown helpers.
  - The Python twins of `parseFilingTableRows` / `filingTableIsUsable` / `_buildFilingIndexFromHtml` (`_parse_filing_table_rows` 1049, `_filing_table_is_usable` 1065, `_build_filing_index_from_html` 5542) are in `server.py#sec`. They move with SEC retrieval in Phase 5.
- **Importers to re-point:** `stripHtmlTags` in `news/shared.ts`, `news/providers/sec.ts`, `news/providers/globenewswire.ts`, `news/providers/company-ir-page.ts`, and in `clients/edgar.ts` if imported back in unit 1.
- **Patch and test risk:**
  - `test_worker_sec_provider.py:141` bundles `stripHtmlTags` through `module_defining(...)`, so it relocates.
  - `test_worker_sec_provider.py:118` asserts `stripHtmlTags(htmlText).toLowerCase()` is **absent** from Worker source. It stays absent after a move.
- **Noticed duplicates (not to fix):** `_stripHtmlTagsIdx` and `_sanitizeFilingHtml` (`#earnings`, 9178/9196) and `sanitizeSecHtml` (`#sec`, 8529) look like near-copies of `stripHtmlTags` / `sanitizeFilingHtml`.

## 7. Unit 4: endpoint builder

- **Topology scope:**
  - Worker: `filingManualLookup` 6134 (`#sec`) to `worker/src/clients/endpoints.ts`.
  - Python: `_manual_lookup_payload` 2045 (`#shared`; builds the `browse-edgar` and `efts` manual-lookup URLs) to `yfmcp/clients/endpoints.py`.
- **The rest of the unit is call-site rewriting, not a move.** Hard-coded upstream URL literals today:

  | Location | Lines with upstream URL literals |
  |---|---|
  | `yahoo-finance.ts` | 95 (largest: 17 `www.sec.gov/Archives/`, 15 `query1…/v10/finance/quoteSummary/`, 12 `www.sec.gov/Archives/edgar/data/…`, 10 `query1…/v8/finance/chart/…`, 8 `data.sec.gov/api/xbrl/companyfacts/CIK…`) |
  | `server.py` | 13 |
  | `yfmcp/tools/earnings.py` | 14 |
  | `yfmcp/clients/edgar.py` | 7 |
  | `yfmcp/clients/yahoo_transcripts.py` | 6 |
  | `news/providers/sec.ts` | 4 |
  | `yfmcp/news/providers/sec.py` | 3 |
  | `evidence-pack.ts` | 2 |

  - About 26 distinct host and path templates; the brief says "~26".
  - Replacing literals with builder calls changes the consumer functions' text, so the line-multiset and `ast.dump` identity proofs cannot apply.
  - The identity proof becomes: every builder returns the exact string of the literal it replaces, pinned by a table-driven test per template in both runtimes, plus a harness diff of 0. Decision D8.

## 8. The 23 helpers (`PHASE3_IMPORTS_TS.md`) and where they land

- **Current `news.ts` + `news/**` imports from `yahoo-finance.ts`:** 22 distinct names. That is 21 of the 23, plus `edgarTickerMapLoaded`, which is new since R-news.
- `iso` and `COMPANY_IR_DISCOVERY_TTL_MS` are no longer imported.

| Helper | Proposed home | Unit |
|---|---|---|
| `edgarGetHtml`, `edgarCikFromAccession`, `getSubmissionsForTicker`, `resolveEx991Url`, (+ `edgarTickerMapLoaded`) | `clients/edgar.ts` | 1 |
| `yGet`, `enc`, `iso` | `clients/yahoo.ts` | 2 |
| `UA`, `ProviderJsonResult`, `providerCacheKey`, `getProviderCache`, `setProviderCache`, `providerHttpStatus`, `fetchProviderWithTimeout` | `clients/provider-http.ts` | 2 |
| `stripHtmlTags` | `clients/html.ts` | 3 |
| `runPartialBatch` | **no client home** (batch utility) | D1 |
| `_str`, `clampInt`, `toIsoUtc` | **no client home** (generic utilities; `toIsoUtc` is `#earnings`) | D1 |
| `NewsCompanyIdentity`, `finnhubEligibility` (`#providers`), `resolveNewsCompanyIdentity` (`#shared`) | **no client home** (news-specific, noted in playbook §5) | D1 |
| `COMPANY_IR_DISCOVERY_TTL_MS` | **no client home** (news-specific; no longer imported) | D1 |

**Exit check for the brief's import rule:**

```
grep -rnE 'from "\.\.?/(\.\./)?yahoo-finance(\.js)?"' worker/src/news.ts worker/src/news
```

- Today: **8 import lines in 8 files**.
- After units 1–3: 0 only if D1 also gives the 7 non-client names a home outside `yahoo-finance.ts`.

## 9. Tooling risks (fix before or with unit 1; analysis and test scripts, not runtime)

1. **The topology misses subdirectories.** `extract_topology.py:208` reads `(ROOT / "worker/src").glob("*.ts")`, top level only.
   - A module under `worker/src/clients/` is invisible. The exit check "region shows 0 NLOC" would pass trivially, and SEC host reads from `clients/edgar.ts` would vanish from the datastore edges.
   - It already misses `worker/src/news/**`.
   - Use `rglob` and add `FILE_DOMAIN` entries for the new modules (`clients/edgar.ts` → `sec`, `clients/yahoo.ts` → `pricing`, and so on), as playbook §1 requires.
   - The current run also reports 15 "no domain" files (`yfmcp/news/**`, `yfmcp/url_policy.py`, `worker/src/{access-keys,auth,telemetry,url-policy}.ts`).
2. **The parity bundle** (`scripts/parity/harness.py:252`) exports top-level `worker/src/*.ts` only. That is fine while `yahoo-finance.ts` re-exports, but cases cannot address `m.edgar` directly.
3. **`test_worker_accession_cik.py:246`** iterates top-level `*.ts` (see §3.5).
4. **`source_text.find_definition`** expects one definition per name. During a move, never leave two copies of a definition (for example, a forgotten original).

## 10. Exit checks

1. **No `#shared` region remains** in either god file (topology re-run with the §9.1 fix).
   - Units 1–4 as scoped remove 56 of 73 Worker `#shared` functions (1071 of 1623 NLOC) and 8 of 44 Python (125 of 2019 NLOC).
   - The residual is listed in §11 D1 and in no unit.
2. **Zero SEC fetches outside the client in either runtime.** Exact grep:

   ```
   grep -rnE 'admin@example\.com|_SEC_REQUIRED_UA|_sec_user_agent\(|\bedgarFetch\(|\bedgarUserAgent\(|EDGAR_CONTACT_EMAIL' \
     server.py yfmcp worker/src --include='*.py' --include='*.ts' \
     | grep -vE '^(yfmcp/clients/edgar\.py|worker/src/clients/edgar\.ts):'
   ```

   **Current count: 17**, which must reach 0:

   | Location | Hits | What they are |
   |---|---|---|
   | `server.py` | 6 | the legacy User-Agent in the 5 raw-`urlopen` tools |
   | `server.py:75` | 1 | an unused re-import of `_SEC_REQUIRED_UA`; nothing in `server.py` reads it |
   | `worker/src/yahoo-finance.ts` | 10 | `edgarUserAgent` / `edgarFetch` and their 5 call sites, all in the EDGAR block |

   Companion checks that prove no SEC fetch hides behind another User-Agent:
   - **Python raw requests outside the allow-listed non-SEC clients:**

     ```
     grep -rnE '(urllib\.request|_urlrequest|_urlreq)\.Request\(' server.py yfmcp --include='*.py' \
       | grep -vE '^yfmcp/(clients/(edgar|market_providers|yahoo_transcripts)|news/providers/(finnhub|globenewswire|marketaux|company_ir_page)|tools/thai_funds|url_policy)\.py:'
     ```

     Current **9**: `server.py` ×6 (SEC), `server.py:7734` (Companies House), `yfmcp/tools/earnings.py:139` (`_fetch_public_html`), `yfmcp/tools/earnings.py:1122` (`parse_public_transcript`).
   - **Worker raw transport outside `clients/` and the allow-listed modules:**

     ```
     grep -rnE '\b(providerFetch|fetchProviderWithTimeout)\(' worker/src --include='*.ts' \
       | grep -vE '^worker/src/(clients/|request-context\.ts|sec-thailand\.ts|news/providers/(finnhub|globenewswire|company-ir-page|marketaux)\.ts)' \
       | grep -v 'function \(providerFetch\|fetchProviderWithTimeout\)'
     ```

     Current **12**, all in `yahoo-finance.ts`: Alpha Vantage 319, Finnhub 377, Yahoo 680/703/801/1015/2998/10273, EDGAR 4925, Companies House 12595, `fetchPublicHtml` 13580.
   - **Not greppable: open fetchers that can reach SEC.** `parse_public_transcript` (Python `earnings.py:1122`, Worker `parsePublicTranscript` → `fetchPublicHtml`) accepts a caller URL. The allow-list admits `https://www.sec.gov/Archives/…` (`callerUrlRefusal` / `_caller_url_refusal`), so an SEC document is fetched outside the client with a browser or generic User-Agent in **both** runtimes. Decision D9.
3. Q3g is released (brief).
4. The harness passes with no expected values changed; acceptance suite and canaries green; live structure before/after shows no diff; `manifestHash` and `schemaHash` unchanged on every move release.
5. Telemetry latency and error rates show no regression beyond agreed bounds, against the 2026-10-17… baseline with F6 canaries excluded. The bounds are not yet agreed (decision D10).

## 11. Planner decisions needed (not chosen here)

- **D1. `#shared` residue with no client home. This blocks the exit check "No `#shared` region remains".**
  - **Worker, 17 functions, 552 NLOC:** `fetchAlphaVantageJson`, `getLastTradingDate`, `toBatchError`, `normalizeBatchSymbolResult`, `runPartialBatch`, `dailyBarDate`, `_str`, `clampInt`, `companyHostRoot`, `normalizeCompanyWebsite`, `resolveNewsCompanyIdentity`, `_buildFilingIndexFromHtml`, `parseObjectJson`, `compactExcerpt`, `normalizeStatus`, `earningsReleasePicks`, `readReleaseText`.
  - **Python, 36 functions, 1894 NLOC.** Dominated by SEC tool code reached from two or more domains:
    - SEC tool code: `_get_filing_data_unchecked` 805, `search_filing_text` 313, `get_filing_data`, `_projected_filing_document`, `_exhibit_targets`, `_annual_coverage_warning_for`, `_sec_xbrl_source_evidence`, `_is_decision_grade_sec_xbrl_fact`, `_filing_period_failure`, `_filing_has_relevant_geo_text`, `_near_specs`, `_fiscal_year_of`, `_utf16_length`;
    - fundamentals and earnings tools: `get_earnings_momentum` 237, `get_analyst_upgrade_radar` 99, `_analyst_price_target_change`, `_earnings_release_picks`, `_read_release_text`, `_estimate_rows`;
    - consensus helpers: `_yahoo_consensus_snapshot`, `_consensus_split_history`, `_latest_provider_snapshot`, `_alpha_vantage_estimates`, `_consensus_providers`, `_consensus_naming`, `_consensus_policy`, `_write_consensus_observation`;
    - generic utilities: `_as_status`, `_unwrap_payload`, `_clamp_int`, `_now_iso`, `_plain`, `_df_period_rows`, `_add_years`, `_fy_number`, `_js_json_numbers`.
  - **Options:**
    - (a) add a fifth Phase 4 unit, "shared utilities", with neutral modules (for example `worker/src/util.ts`, `worker/src/batch.ts`, `worker/src/news/identity.ts`, `yfmcp/util.py`), and leave the domain-tool residue to Phase 5;
    - (b) restate the exit criterion as "no client function remains in `#shared`", and let Phase 5 domain moves drain the rest;
    - (c) change the topology rule, for example assign cross-called tool handlers to their catalog domain.
  - The choice also decides whether `news.ts` can stop importing `yahoo-finance.ts` (§8: 7 names).
- **D2. The Python EDGAR routing release.**
  - **D2a. Sequencing:** before or after the unit-1 move, and as its own release with docs and a version bump (§1).
  - **D2b. CIK and cache semantics for `list_sec_filings`:** index-only through `_load_edgar_tickers` (inherits F7; `PROVIDER_ERROR` becomes `TICKER_NOT_FOUND` on a failed load) or full `_resolve_cik_for_ticker` (aligns with the Worker; new tickers may resolve). Also: the submissions TTL of 24 h (Python) vs 15 min (Worker), and the `PROVIDER_ERROR` message text.
  - **D2c. The document-read primitive:** a new client function that keeps the HTTP status. Decide its timeout (15 s today in Python, 20 s Worker, 30 s client), size cap (none today; 5 MB default in the client) and decoding (`errors="replace"` vs strict UTF-8 with latin-1 fallback).
- **D3. Destination layout.** `worker/src/clients/*.ts` mirrors `yfmcp/clients/`, but needs the §9 glob fixes. Flat `worker/src/edgar-client.ts` etc. (the brief's names) needs none.
- **D4. Unit-1 dependencies on later units.** `clients/edgar.ts` needs `yGet`/`enc` and `countersSince` (Yahoo), `fetchProviderWithTimeout`/`edgeCache` (provider transport) and `stripHtmlTags` (HTML).
  - Option: import them back from `yahoo-finance.ts` for one unit. This creates an `edgar.ts` ↔ `yahoo-finance.ts` cycle, which is safe if the TDZ scan passes.
  - Option: move `clients/provider-http.ts` (and possibly `stripHtmlTags`) into unit 1.
  - **Helpers used by both clients:** `fetchProviderWithTimeout`, `edgeCache`, and the shared mutable `countersSince` / cache-stats `since`. Python has the same coupling: `edgar.py` calls yfinance `.info["cik"]` directly.
- **D5. `parseHtmlTable`.** Relocate it from `news.ts` to `clients/html.ts` in unit 3? This edits `news.ts`. It is in scope by the brief's intent, but not in the topology's `#shared` list.
- **D6. `_resolve_ex991_url`** (Python, `yfmcp/tools/earnings.py`). Move it to `yfmcp/clients/edgar.py` alongside the Worker's `resolveEx991Url`, or leave it?
- **D7. Python "Yahoo client" scope.** The topology yields only `_valuation_market_inputs`. Is a façade over `yf.Ticker` part of unit 2 (not a pure move, and `server.yf` patches in 12 files would need retargeting) or left to Q3g?
- **D8. Unit 4 identity proof.** Call-site rewriting breaks text identity. Accept "every builder returns the exact literal string" tests plus a harness diff of 0 as the proof?
- **D9. Open fetchers that can reach SEC** (`parse_public_transcript` in both runtimes, with a browser or generic User-Agent). Route SEC-Archive URLs through the client (a User-Agent change, so a behaviour release in both runtimes), or exempt them from the "zero SEC fetches outside the client" exit check?
- **D10. Telemetry regression bounds** for the exit comparison, for example a p95 latency delta and an error-rate delta per action group.
- **D11. Ownership of `resolveSecFiling` / `_resolve_sec_filing`.** Also the filing-not-found and `ACCESSION_NOT_FOUND` envelope helpers that sit in the EDGAR block. They produce MCP error payloads; they are filing-selection policy, not transport. The topology puts them in `#shared`, so this spec places them in the EDGAR client. Alternative: they stay for Phase 5 SEC retrieval, which leaves them in `#shared` until then (see D1).

## 12. Noticed, not fixed (report-only)

- `server.py:75` imports `_SEC_REQUIRED_UA` and never uses it.
- `server.py` re-imports client module state by value (`_EDGAR_TICKERS`, `_EDGAR_TICKERS_LOADED_AT`, `_EDGAR_FACTS_CACHE`, `_EDGAR_SUBS_CACHE`):
  - `server._EDGAR_TICKERS` is a stale copy (`None` at import). A `patch("server._EDGAR_TICKERS")` would silently do nothing.
  - The cache dicts are shared objects, so they are fine.
- Q3i **F7** (failed ticker-index load cached for up to 24 h) is still open. It matters for D2b.
- **Submissions TTL divergence:** Python 24 h (`_EDGAR_TTL`), Worker 15 min (`FILING_SUBMISSIONS_TTL_MS`).
- **SEC request timeouts differ:**
  - Python client: 10 s for JSON, 15 s for the ticker index, 30 s for HTML;
  - Python raw tools: 10 s and 15 s;
  - Worker: 20 s for everything.
- **Topology attribution quirks:** `edgarFetch` and `edgarUserAgent` land in `#sec`, not `#shared`, although every caller is an EDGAR client function. Per-function NLOC includes trailing module constants.
- `fetchTickerEarningsHistory` fetches Yahoo HTML outside `yGet`.
- `yfmcp/tools/news.py` imports `_run_ticker_batch` from `yfmcp.tools.pricing` (a cross-tool import; `PHASE3_IMPORTS_PY.md`).
- Pyflakes-unused imports left in `server.py` by Phase 3 are still there.
