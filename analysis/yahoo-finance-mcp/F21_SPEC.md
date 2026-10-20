# 2.24.0: every pinned `query_sec_filing_index` read uses the pinned filing (F21)

**Source:** finding F21 (`F_TRIAGE.md`, raised in the R5-extractors spec and widened by the 2.23.0 live checks).
**Owner decision (2026-10-20):** "ok yes" to F21 as 2.24.0 before the 2026-10-24 Phase 4 gate, then F19 and F16 as 2.25.0. The routing release becomes 2.26.0.
**Planner classification:** REPAIR, the same class as F3 and R4-ACC-QUERY. Naming a filing other than the pinned one overstates provenance (P1). Every read below already accepts an accession, so the fix only passes it through (P5: no new capability).
**Version:** 2.24.0 (minor). No new field, enum value, warning code or `unresolvedReason`. The public extractor tools gain no parameter.

## 1. Changes (both runtimes)

**Internal accession on four extractors.** Each of these gains an internal trailing `accessionNumber` / `accession_number` (default null). It is not a tool param: Python keeps the public function as a thin wrapper over a private helper, as 2.23.0 did for `extract_revenue_exposure`.

| Extractor | How the pin is used |
|---|---|
| `extractTotalRevenue` | passed as `getFilingData`'s `pinAccession`. With a pin, the latest-period sort picks the filing's own newest period. |
| `extractSegmentRevenue` | passed as `pinAccession`. Its 20-F fallback follows the 2.15.0 geographic rule: with a pin, it retries only when the pinned filing's form is 20-F (from `resolveSecFiling`), and the retry reads the same accession. No `mayBe20FFiler` call is made. |
| `extractCustomerConcentration` | passed to its first `searchFilingText` call. The table search already reuses the accession that call returns. |
| `extractRiskFactorMentions` | passed to each `searchFilingText` call. |

**Callers:**
- `querySecFilingIndex` passes its accession for `risk_factor_mentions`, `customer_concentration`, `total_revenue` and `segment_revenue`.
- Before any read for those four query types, an accession that is set but not among the company's filings fails closed with the existing `accessionNotFoundEnvelope` (`ACCESSION_NOT_FOUND`), exactly as `geographic_revenue_share` has since 2.15.0. The check uses `resolveSecFiling(ticker, filingType, accessionNumber)`.
- `extractChinaExposure` passes its accession to the risk-factor read. Its accession-miss handling is unchanged: the legacy `FILING_NOT_FOUND_TRY_OTHER_TYPE` from the index check, before this read matters.

**Unchanged:**
- The public tools `extract_total_revenue`, `extract_segment_revenue`, `extract_customer_concentration` and `extract_risk_factor_mentions` take no accession.
- `extract_exposure` does not take one either.
- A query without an accession behaves exactly as today.

## 2. Parity cases (equal mode, GEOX fixture in `cases_accession.py`)

Extend the fixture where needed:
- GEOX_FACTS already has FY2025 and FY2024 total revenue rows per accession.
- Add the prior-year comparative row inside the FY2025 10-K (FY2024, same `accn` as `GEOX_NEW_10K`), so the pinned pick is shown to choose the filing's own newest period.
- Add a customer sentence and a risk-factor section to each 10-K's HTML that differ by year (e.g. "Customer A accounted for 12%" in 2025 and 15% in 2024), so the filing read is visible in the value.

Cases:
- `f21.query.total_revenue_pinned`: `GEOX_OLD_10K` gives 400,000,000, with evidence naming `GEOX_OLD_10K`.
- `f21.query.total_revenue_pinned_newest_period_of_that_filing`: `GEOX_NEW_10K` gives 500,000,000 (not the FY2024 comparative).
- `f21.query.customer_concentration_pinned`: `GEOX_OLD_10K` gives the 2024 percentage, with evidence naming it.
- `f21.query.risk_factor_mentions_pinned`: `GEOX_OLD_10K`, with all evidence accessions `GEOX_OLD_10K`.
- `f21.query.segment_revenue_pinned`: GEOX has no segment facts, so this gives `NOT_DISCLOSED`. Record that no `mayBe20FFiler` warning appears for a pinned 10-K.
- `f21.query.<each of the four>_accession_miss_fails_closed`: `GEOX_MISSING` gives `GEOX_NOT_FOUND`.
- `f21.query.no_accession_unchanged`: one per query type, reading the latest filing as today.
- Update `r4b2.query.china_hit_reads_the_pinned_filing`: every evidence accession that names a filing is `GEOX_OLD_10K` (the risk block is pinned now).
- The rest of the harness diff is 0.

## 3. Live checks

- `health_check` reports 2.24.0. `manifestHash` and `schemaHash` are unchanged.
- `query_sec_filing_index` AAPL with 0000320193-24-000123:
  - `total_revenue` returns 391.035B FY2024, with evidence naming that accession;
  - `customer_concentration` evidence names it;
  - `risk_factor_mentions` ["China"] evidence names it;
  - `china_exposure` riskFactorExposure evidence names it.
- The same queries with a fake accession 0000320193-99-999999 return `ACCESSION_NOT_FOUND` (china_exposure keeps `FILING_NOT_FOUND_TRY_OTHER_TYPE`).
- `total_revenue` without an accession: 416.161B FY2025, unchanged.

## 4. Implementation notes (2026-10-20)

- `r4b2.query.china_hit_reads_the_pinned_filing` now expects `evidenceAccessions: [GEOX_OLD_10K, None]`. The fixture's risk text now mentions China, which adds a risk-mention evidence row. Its shaped top-level row has no accession, the same in both runtimes and the same pre-existing shape AAPL shows live. The nested mention evidence names `GEOX_OLD_10K` (`f21.query.china_risk_block_reads_the_pinned_filing`). Accepted.
- **New finding F23** (pre-existing, not caused by F21): `query_sec_filing_index` `segment_revenue` without an accession:
  - the Worker appends the extractor's warnings (`POSSIBLE_20F_FILER` for GEOX);
  - Python's query branch drops them.
  - The case is pinned as `F21-SEGMENT-QUERY-WARNINGS`. Aligning means choosing which output is right, so it is not done here.

## 5. Status (2026-10-20)

Released as 2.24.0 (#302). Live checks at 08:39Z on build 2.24.0+git.7d7874a:
- `health_check` reports 2.24.0. `manifestHash` and `schemaHash` are unchanged.
- Pinned to 0000320193-24-000123:
  - `total_revenue` returns 391.035B FY2024, with evidence naming that accession;
  - `risk_factor_mentions` ["China"] has all evidence naming it;
  - `china_exposure` riskFactorExposure evidence names it (on 2.23.0 it named the FY2025 filing).
- A fake accession 0000320193-99-999999 on `total_revenue` returns `ACCESSION_NOT_FOUND`.
- `total_revenue` without an accession returns 416.161B FY2025, unchanged.
- `customer_concentration` returns `EXTRACTION_FAILED` with no evidence, pinned and unpinned alike. AAPL discloses no major customer, so this is unchanged from 2.23.0.

F21 is complete.
