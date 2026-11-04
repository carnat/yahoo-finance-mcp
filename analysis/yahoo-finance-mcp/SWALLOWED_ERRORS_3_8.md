# Swallowed errors: findings and recommendations (2026-11-04)

Phase 6 item "Swallowed-error audit" (Debt #5). Baseline: 3.7.0 (build cf5e87a).
The rule since 2.5.17 is that a failed read must not look like a missing record. This audit checks every error handler against that rule.

## Count
- **Python:** 296 handlers that don't re-raise.
  - 257 are fine: parse probes, best-effort cache or telemetry, or a failure reported elsewhere.
  - 34 are silent failures (20 findings).
  - 5 are unclear.
- **Worker:** 178 catch sites.
  - 156 are fine.
  - 19 are silent failures.
  - 3 are unclear.
- Most findings appear in both runtimes. The SEC client helpers in both runtimes hide most of them: they return "nothing" for a 429, 5xx or timeout as well as for a 404.
- Full inventories:
  - Python: `swallowed-errors-inventory-python-2026-11-04.md`
  - Worker: `swallowed-errors-inventory-worker-2026-11-04.md`

## What a caller sees today (worst cases)
1. If SEC throttles or fails the submissions or ticker-index read, about 14 SEC tools answer `TICKER_NOT_FOUND`.
2. A failed filing or press-release read reads as "no document". Earnings period, guidance and topics come back empty, and the exhibit list is `[]`.
3. If the 8-K index read fails, the Worker reads the 8-K cover page instead and still labels the release text READ.
4. If the Yahoo quote fails for an ADR, valuation history applies no ADR ratio and says nothing. Share counts and multiples can then be off by that ratio.
5. Put-hedge candidates skip an expiry whose option chain failed. If every chain fails, the answer is "No put options found".
6. A failed Yahoo section in analyst consensus, earnings analysis, earnings momentum or position inputs turns into `null` or `{}`, the same as an empty section.
7. In the Worker, a component tool that answered with error text is parsed as `{}`. This happens at 35 call sites. In one case, `extract_sec_filing_fact`, a parse failure reads as `NOT_DISCLOSED`.
8. `get_uk_company_filings` answers `INPUT_VALIDATION_ERROR` when it is really a failed Yahoo read.

## Recommendation: four releases, both runtimes, existing codes where they fit
| Release | Scope | Change the caller sees | Codes |
|---|---|---|---|
| 3.8.0 | SEC reads: Python F1-F6, Worker S01-S03, S05, S06, S11-S14 | A failed SEC read becomes `PROVIDER_ERROR` (retryable), not `TICKER_NOT_FOUND`. A failed document read is reported as not read, not as "no document". The cover-page fallback is no longer labelled READ. `exhibitsCount` is null when the index was not read. The annual-coverage check says when it could not run. Python's public-page fetch treats a timeout like the Worker does. | Existing: `SEC_LOOKUP_UNAVAILABLE`, `SEC_READ_FAILED` (with `failedReads`), `RELEASE_TEXT_NOT_AVAILABLE`, `FILING_TEXT_NOT_AVAILABLE`, status `NOT_READ` |
| 3.8.1 | Yahoo enrichment: F7-F13, F15-F20, Worker S10, S15, S16 | Each failed component adds a warning naming it, and the status is `PARTIAL` where the tool has one. The ADR case warns instead of silently skipping the ratio. The UK filings case becomes `PROVIDER_ERROR`. No new fields. | Existing: `COMPONENT_FAILED`, `SOURCE_UNAVAILABLE`, `SOURCE_IDENTITY_UNAVAILABLE`, `QUOTE_UNAVAILABLE`, `PRICE_UNAVAILABLE`, `RATING_CHANGES_UNAVAILABLE`, `COMPANYFACTS_NOT_AVAILABLE` |
| 3.8.2 | Options: F14 and Worker S09 | A skipped expiry adds a warning naming the expiry. If every expiry failed, the tool returns `PROVIDER_ERROR` instead of "No put options found". | **One new warning code: `OPTION_CHAIN_READ_FAILED`** |
| 3.8.3 | Parsed tool output: Worker S17-S19 and the Python counterparts | When a component tool returns error text, the error is carried as `COMPONENT_FAILED`, not turned into `{}`. A fact read that cannot be parsed is `PROVIDER_ERROR`, not `NOT_DISCLOSED`. A structured IR source that returns non-JSON warns. | Existing: `COMPONENT_FAILED`, `COMPANY_IR_PAGE_VALIDATION_FAILED`, `PROVIDER_ERROR` |

Not changed:
- The handlers classed as fine.
- The internal audit endpoint (Worker S20).
- The 8 unclear sites. Two of them read the bundled registries, which ship with the package. Their failure is a packaging fault, which tests already catch.

## What needs your decision
- Existing warning codes and statuses appear on tools that never emitted them. The biggest change is that an SEC outage reads `PROVIDER_ERROR` instead of `TICKER_NOT_FOUND`.
- There is one new warning code, `OPTION_CHAIN_READ_FAILED`.
- These are minor releases (3.8.x): nothing is removed and the tool schemas don't change.

## Outcome

- Owner approved 2026-11-04 07:36Z ("Yes, as recommended").
- 3.8.0 (SEC reads), 3.8.1 (Yahoo enrichment) and 3.8.2 (option expiries) shipped; details in docs/provider-runtime-guidance.md. 3.8.1 also holds back the options-flow string codes and the Python-only partial rows.
- Held back for new names: the management-commentary "not read" signal and the skipped annual-coverage check.
