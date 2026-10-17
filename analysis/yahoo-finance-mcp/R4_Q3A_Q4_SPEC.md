# R4, third release (2.13.0): Q3a exact accession, fail-closed; Q4 smoke CIK table out of production

**Owner decisions:**
- **Q3a:** the Worker must honour an exact `accession_number` with Python-equivalent fail-closed semantics before Phase 4.
- **Q4:** remove `SMOKE_TICKER_CIK_FALLBACKS` from the production lookup and keep it only as injected test fixtures.
- **Order:** A3.
- **Q11:** the name `ACCESSION_NOT_FOUND` was accepted on 2026-10-15.

**Version: 2.13.0** (minor). `ACCESSION_NOT_FOUND` becomes a public error code. Explicit accessions are now honoured where they were ignored before.

## 1. Q3a rule (both runtimes, identical)

**When a caller supplies a non-empty `accession_number`/`accessionNumber` and an action resolves a filing from it:**
- If that exact accession is **not** among the company's filings that the resolver searches (today `submissions.filings.recent`, in both runtimes), the action fails closed:
  `{ ok: false, error: { code: "ACCESSION_NOT_FOUND", message: "Accession <acc> was not found in <TICKER>'s recent SEC filings.", requestedAccession: "<acc>", retryable: false } }`
- **No fallback** to the latest filing or to another filing type (for example the 20-F fallback) when an accession was supplied.
- If the accession **is** found, behaviour is unchanged.
- Add `ACCESSION_NOT_FOUND` to the Worker and Python `ErrorCode`.

**Where it applies** (the actions that already honour an accession; only the miss path changes):

| Action | Python | Worker |
|---|---|---|
| `get_sec_filing_index` | `_get_sec_filing_index_impl` (server.py ~5632–5666), today `NO_FILING_DATA` | `_getSecFilingIndexImpl` (yahoo-finance.ts ~9248) via `resolveSecFiling`, today `FILING_NOT_FOUND_TRY_OTHER_TYPE` payload |
| `search_sec_filing_text` | `search_filing_text` (server.py ~3251–3280) | `searchFilingText` (yahoo-finance.ts ~7131–7150) |
| `extract_dilution_bridge`, `get_share_count_scenarios`, `extract_capital_structure`, `extract_funding_capex_schedule`, `extract_operating_driver_ledger` | `_resolve_periodic_filings` (server.py ~6872–6905) | `resolvePeriodicFilings` (yahoo-finance.ts ~11917–11925) |
| `extract_sec_filing_fact` (fact_name path) | via `search_filing_text` | via `searchFilingText` |

- **Shared resolvers:** implement the miss once in `_resolve_sec_filing` and `resolveSecFiling`, as an accession-specific branch. Callers that already return the resolver's not-found result then propagate it as the error envelope above.
- **Fact path:** the structured `extract_sec_filing_fact` path keeps `NO_FACT_FOR_ACCESSION`. It is already identical in both runtimes, and it means the filing exists but the fact does not.

**`get_sec_filing_outline`:** an explicit accession is honoured in both runtimes.
- Python today ignores it when `document_url` is absent and the period is "latest". The Worker's `getFilingOutline` ignores it whenever it is given.
- **New rule:** when `document_url` is absent and `accession_number` is given, resolve that accession through the same resolver. A miss gives `ACCESSION_NOT_FOUND`; a hit returns the outline of that filing's primary document.
- When `document_url` is given, it wins, as today.
- The existing output shape stays as each runtime has it, except where the outline itself now comes from the requested filing.
- If the two runtimes' outline shapes still diverge (`OUTLINE_NOT_PARSED`/`tableCount` in the Worker only), report it; do not fix it here. It goes to triage.
- Flip `tool.q3a.outline_accession_only_python_resolves_worker_ignores` to equal.

**Out of scope** (both runtimes already behave the same):
- `extract_geographic_revenue` and `query_sec_filing_index` use the accession only on some routes;
- the exhibit tools build URLs without an existence check.

Each of these becomes a row in the Q3i triage table.

**Known limit (documented):** the resolver searches only `filings.recent` (about the last 1,000 filings), so a real but older accession reports `ACCESSION_NOT_FOUND`. The message says "recent".

## 2. Q4 rule (both runtimes)

- **Remove the smoke step** from `_resolve_cik_for_ticker` (yfmcp/clients/edgar.py ~335–338) and `resolveCikForTicker` (yahoo-finance.ts ~5316–5320). The lookup order becomes: cache → Yahoo/yfinance cik → SEC company_tickers.json → EDGAR atom.
- **Keep the table only as a test fixture:**
  - Python: `edgar._TEST_TICKER_CIK_FIXTURES: dict[str, str] = {}`, read by the resolver at the same position. It is empty in production; tests use `patch.dict`.
  - Worker: a module-level `Map` that is empty by default, plus an exported `setTickerCikFixturesForTests(map)` following the `setEvidenceStoreForTests` pattern.
  - The 11 entries move into a shared test fixture used by both runtimes' tests.
- **Remove** the unused `server.py` re-import (~line 75).
- **Tests and parity:**
  - `scripts/test_input_validation.py` ~193–206 injects the fixture.
  - The parity cases `tool.q4.smoke_table_resolves_listed_ticker…` and `tool.q4.smoke_table_covers_aaoi` become "injected fixture resolves" (mode equal) plus "no fixture and no index → atom → None" (mode equal).
  - `tool.q4.sec_ticker_index_wins_over_the_smoke_table` and `…unlisted_ticker…` stay.
- **Production effect:** only during a combined Yahoo-plus-SEC-index outage do these 11 tickers now fall through to the EDGAR atom lookup, like every other ticker.

## 3. Tests

- **Parity, both runtimes, mode equal:**
  - `ACCESSION_NOT_FOUND` for each listed action;
  - no fallback when an accession is given (a fake submissions list has a 10-K but not the requested accession → error, not the latest 10-K);
  - an accession hit is unchanged;
  - outline with accession only: hit and miss.
- **Q4:** the fixture seam in both runtimes, and production lookup without fixtures.
- **Harness:** green; list every expectation edit.

## 4. Release and live checks

- **Docs section "Exact Accession and CIK Lookup (2.13.0)".**
- **Live checks:**
  - `get_sec_filing_index` for AAPL with a real recent accession returns that filing;
  - the same call with a well-formed but non-existent accession (`0000320193-99-999999`) returns `ACCESSION_NOT_FOUND`;
  - `get_sec_filing_outline` for AAPL with only that real accession returns its outline;
  - `list_sec_company_filings` AAPL still works (CIK lookup);
  - the canaries pass.
