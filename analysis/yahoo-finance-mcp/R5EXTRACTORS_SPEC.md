# R5-extractors (2.23.0): pinned revenue reads, pinned table fallback, no producer-level HIGH defaults

**Source:** approved F triage (`F_TRIAGE.md`, 2026-10-19) rows F3, F4 and F18, and the approved Q3i triage rows BR-EXT-035 and BR-EXT-084 (`q3i/defects_and_inputs.md`, tagged R5-extractors).
**Owner scope decision (2026-10-20):** "do as recommended": 2.23.0 is R5-extractors and ships now (it does not wait for the Phase 4 telemetry gate). F1 and F2 move from R5-sec into the 2.24.0 Python EDGAR routing release, which rewrites the same Python outline function.
**Version:** 2.23.0 (minor). No new field, enum value, warning code or `unresolvedReason`. Values change: which filing a pinned read uses, and the confidence that a producer falls back to.

## 1. Items

### F3: the pinned china and revenue-exposure reads use the pinned filing

- `extractRevenueExposure` / `extract_revenue_exposure` take an internal `accessionNumber` / `accession_number` (default null) and pass it to `extractGeographicRevenue` / `extract_geographic_revenue`.
  - The public tool `extract_revenue_exposure` does **not** gain the param. It is internal only, so `tool_catalog.json` and the schemas are unchanged.
- `extractChinaExposure` / `extract_china_exposure` pass their accession to that revenue read.
- `query_sec_filing_index` with `revenue_exposure` passes its accession too. This is the same defect class as R4-ACC-QUERY: naming the latest filing for a pinned query overstates provenance (P1).
- An accession miss keeps its current result in each path. china_exposure still returns `FILING_NOT_FOUND_TRY_OTHER_TYPE` from the index check.
- **Planner note:** `total_revenue`, `segment_revenue` and `customer_concentration` in `query_sec_filing_index` are also not pinned. They are out of scope here (no approved row), and are recorded as finding F21.

### F4: a pinned geographic read with no XBRL fact reads the pinned filing's table

- In `getFilingDataUnchecked` (`yahoo-finance.ts` ~6441-6464) and the Python equivalent (`server.py` ~2590-2615): when `pinnedAccession` is set, no concept fact matches, and `factType === "geographic_revenue"`, do **not** return `NO_FACT_FOR_ACCESSION`. Fall through to the existing HTML table fallback, the same way the unpinned path does.
  - That fallback already resolves the pinned accession (`resolveSecFiling(ticker, filingType, pinnedAccession)` honours the accession whatever its form).
  - Non-geographic fact types keep `NO_FACT_FOR_ACCESSION` / `SEC_FACTS_IFRS_ONLY` unchanged.
- The existing 20-F retry in `extractGeographicRevenue` then behaves as today: for a pinned read it runs only when the pinned filing is a 20-F, and it reads the same accession.
- **Observed on the Worker (GEOX fixture):**
  - A pinned 20-F asked for as 10-K for "China", where the 20-F table has no China row, gives `NOT_DISCLOSED`, evidence naming the 20-F (`filingType` "20-F", which is the filing actually read), and the existing `POSSIBLE_20F_FILER` warning from the retry.
  - The same filing for "United States" gives FOUND 300,000,000 from the 20-F.
  - Before this release, both returned `SEC_FACT_NOT_AVAILABLE` / `NO_FACT_FOR_ACCESSION`.
  - The `POSSIBLE_20F_FILER` wording ("retry with filing_type='20-F'") is not precise for a pinned 20-F that the retry has already read. It is an existing code on an existing path, so it is left as is and recorded for the Phase 6 warning-text pass.

### F18 and BR-EXT-035: no producer-level HIGH fallback

Each `?? "HIGH"` / `_nn(..., "HIGH")` / `.get("confidence", ... "HIGH")` / `or ("HIGH" ...)` producer fallback falls to `"LOW"`, except the 20-F fallback (BR-EXT-035), which falls to `"NOT_DISCLOSED"`, the primary path's default.

The audit found that every upstream payload these sites read sets its own confidence today:
- the geographic shape (`withGeoShape`) always sets `confidence`;
- the total-revenue fact sets it explicitly;
- segment rows are tagged `"HIGH"` explicitly.

So these fallbacks are dead in normal output. The change stops a future untagged payload being labelled HIGH (P1). No live output is expected to change.

| Site | Worker | Python | Fallback becomes |
|---|---|---|---|
| geographic 20-F fallback (BR-EXT-035) | `yahoo-finance.ts` ~11425 | `server.py` ~6500 | `NOT_DISCLOSED` |
| `extractTotalRevenue` | ~11797 | ~6630 | value non-null: `LOW` (null: `NOT_DISCLOSED`, unchanged) |
| `extractRevenueExposure` match | ~11834 | ~6672 | `LOW` |
| query `geographic_revenue_share` | ~13397 | ~8615 | ANSWERED: `LOW` (else `NOT_DISCLOSED`, unchanged) |
| query `revenue_exposure` | ~13421 | ~8647 | ANSWERED: `LOW` |
| query `total_revenue` | ~13501 | ~8751 | ANSWERED: `LOW` |
| query `segment_revenue` | ~13519-13520 | n/a: Python derives HIGH from status, and rows are always HIGH | `LOW` (Worker only; output unchanged because rows are tagged) |

**Fallback semantics:** Python follows TS `??`. Only `None` falls back: use `_nn`, not `or` and not `.get(k, default)`. `.get` keeps an explicit `None`, and `or` drops `""`. The F18 note records this.

The status-derived confidences (`china_exposure`, `customer_concentration`, Python's `segment_revenue`) are explicit derivations, not fallbacks. They are out of scope.

### BR-EXT-084: a warrant class with no exercisable key is unresolved under vested_only

- `share-scenarios.ts` ~187 and `share_scenarios.py` ~183: under `warrant_vesting = vested_only`, a class with no `exercisable` key is treated like `exercisable: null`.
  - That gives `count` null and the existing "count not tagged" unresolved line.
  - Under `all`, outstanding is used, as today.
- This is latent: the capital-structure producer always emits the key.

## 2. Parity cases (equal mode)

- **F3:** flip `r4b2.query.china_hit_unchanged` (rename it `r4b2.query.china_hit_reads_the_pinned_filing`). Its expected `evidenceAccessions` now name `GEOX_OLD_10K` (the 2024 10-K) wherever the revenue block contributes evidence.
  - Add `r5x.query.revenue_exposure_pinned`: `query_sec_filing_index` GEOX `revenue_exposure` "China" with `GEOX_OLD_10K` answers 100,000,000 from `GEOX_OLD_10K`.
  - Keep `china_miss_unchanged` as it is.
- **F4:** flip `r4b2.geo.pinned_20f_with_10k_filing_type_reads_that_accession` to the table read of the 20-F (China absent, so it is not found). Record the actual status and warnings both runtimes produce, and they must be equal.
  - Add `r5x.geo.pinned_20f_table_read`: `extract_geographic_revenue` GEOX "United States", 10-K, pinned `GEOX_20F` returns 300,000,000 with evidence naming `GEOX_20F`.
- **F18 / BR-EXT-035 (not added, 2026-10-20):** the harness stubs SEC HTTP routes only, and both runtimes' `getFilingData` always set `confidence`, so no route-based case reaches these branches. A Python-only patch would not be a parity case. The change is covered by review and the existing suites. Originally planned: unit cases that stub the upstream to return a value with no `confidence`. The stub's level (an extractor function patch) is up to the implementer, provided both runtimes are stubbed the same way:
  - the 20-F fallback gives `NOT_DISCLOSED`;
  - `extract_total_revenue` gives `LOW`;
  - the revenue-exposure match gives `LOW`;
  - the `query_sec_filing_index` geographic, revenue_exposure and total_revenue answers give `LOW`.
  - A case with an upstream confidence present shows it passes through unchanged.
- **BR-EXT-084:** a share-scenario case with a warrant class missing `exercisable`. Under vested_only, count is null and `unresolvedReason` is "count not tagged". Under all, the outstanding count is used.
- The rest of the harness diff is 0.

## 3. Work order

1. Worker (`worker/src/yahoo-finance.ts`, `worker/src/share-scenarios.ts`).
2. Python mirror (`server.py`, `yfmcp/share_scenarios.py`), parity cases in `scripts/parity/cases_accession.py` and `cases_fact_confidence.py` / `cases_capital_structure.py`.
3. Release: version 2.23.0 in all four places; a docs section "Pinned Revenue Reads And Producer Confidence Fallbacks (2.23.0)"; local CI.

## 4. Live checks

- `health_check` reports 2.23.0. `manifestHash` and `schemaHash` are unchanged (no catalog or schema change).
- `query_sec_filing_index` AAPL `china_exposure` with an older 10-K accession: the revenue evidence names that accession.
- `extract_geographic_revenue` AAPL China, unpinned: the value and confidence are unchanged from 2.22.0.
- `query_sec_filing_index` AAPL `total_revenue`: the confidence is unchanged (upstream tagged).
