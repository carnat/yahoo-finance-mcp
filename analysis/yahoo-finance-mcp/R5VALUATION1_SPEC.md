# R5-valuation part 1 (3.2.1): tolerance input, AGREED wording, convertible basis, 52/53-week quarters, unknown ticker

**Source:** the brief's R5 valuation list (`MODERNIZATION_BRIEF.md` R5-‹unit›: Q3e tolerance NaN/inf and the AGREED wording, EV convertible-debt basis), the Q3i triage (BR-VAL-068 7-day drift rule, numeric-string `tolerance_pct` to the default), and the F57 error rule of `compare_peer_valuations`.

**Version:** 3.2.1 (patch). No new field, status, warning code or enum value. `schemaHash` changes (the Worker's `reconcile_metric_sources` description); `manifestHash` does not.

**Not in this release:** BR-VAL-006 (partly tagged debt group) and BR-VAL-025 (ProfitLoss without a matched NCI) need new names; they wait for the owner's card (2026-10-29) and ship as R5-valuation part 2.

## 1. Items

| Item | Before | Now (both runtimes) |
|---|---|---|
| Q3e tolerance | Python `min(10, max(0, float(x)))` before the finiteness check: NaN 0, +inf 10, "5" 5 | a finite int/float (not bool) is clamped to 0-10; anything else is 0.5 |
| Q3e AGREED wording | Worker: "AGREED (two or more providers agree), PARTIAL (one provider)" | Python's text: latest SEC plus at least one independent matching provider |
| Q3i convertible basis | principal left debt whenever the bridge counted incremental shares | only when `diluted` (the bridge's diluted count) is the share count used |
| BR-VAL-068 | calendar quarter of the raw end (`ceil(month/3)`) | of `nominal_period_end` / `nominalPeriodEnd` (end minus 7 days) |
| F57 snapshot error | `PROVIDER_ERROR` "Yahoo quote summary failed for X: <message with URL>" | `peerValuationError`: `TICKER_NOT_FOUND` with the standard message, else `PROVIDER_ERROR` "Yahoo quote summary failed for X: <message without URL>" |

## 2. Tests

- `cases_valuation.py`: `pinned_tolerance_nan_and_infinity`, `pinned_tolerance_numeric_string` (plus a boolean), `pinned_tool_description_agreed_wording` and the two `pinned_convertible_deducted_*` cases flip to equal (EV 5,240M and 2,200M).
- `cases_r5val1.py`: quarter ended 2027-01-02 matches "fourth quarter of 2026" and "Q4 2026", not "first quarter of 2027"; a regular quarter is unchanged; the snapshot's 404 and 500 failures.

## 3. Exit

- [ ] Full parity harness and every `ci.yml` test command.
- [ ] PR, CI green, owner merge.
- [ ] Live check: `health_check` 3.2.1, schemaHash changed, manifestHash unchanged; `get_valuation_snapshot` ZZZZQQ is TICKER_NOT_FOUND; AAPL snapshot unchanged; `reconcile_metric_sources` AAPL revenue still AGREED.
