# R5-earnings part 2 (3.3.1): transcript next-source suggestion

**Source:** BR-ERN-009 (scheduled repair, `BUSINESS_RULES.md` §3; brief R5 earnings "transcript fallback ordering by sources actually attempted"). Owner decision 2026-10-30 on the card: "Suggest the fix for what failed".

**Version:** 3.3.1 (patch). No new field, status, source type or enum value; hashes unchanged.

## 1. Rule (`_next_transcript_fallback` / `nextTranscriptFallback`)

1. `yahoo_quartr` status `YAHOO_METADATA_MISMATCH` → `yahoo_quartr` (retry with a validated source_url or event_id).
2. `alpha_vantage` `NOT_ELIGIBLE` with `reasonCode` `FISCAL_QUARTER_UNRESOLVED`, and `ALPHA_VANTAGE_API_KEY` (or `ALPHAVANTAGE_API_KEY`) configured → `alpha_vantage` (retry with `fiscal_quarter` as YYYYQn).
3. Otherwise unchanged: company_ir, public_transcript_url, yahoo_quartr, alpha_vantage when absent or SKIPPED; else null.

An incomplete Yahoo preview still overrides with `alternate_transcript_source` (BR-ERN-009 card example).

## 2. Tests

- `cases_sec_earnings.py`: the two pinned waterfall cases become equal (still company_ir: no key in the harness); a third sets a key and expects alpha_vantage.
- `cases_r5ern2.py`: five attempt-list cases (mismatch first, quarter-only block, no key, key missing at fetch, company IR tried).

## 3. Exit

- [ ] Full parity harness and every `ci.yml` test command.
- [ ] PR, CI green, owner merge.
- [ ] Live check: `health_check` 3.3.1, hashes unchanged; a transcript call that finds nothing still returns a `nextRecommendedFallback` (no Alpha Vantage call made by the check).
