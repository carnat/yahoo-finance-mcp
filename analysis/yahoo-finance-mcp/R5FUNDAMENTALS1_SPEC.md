# R5-fundamentals (3.5.0): credit health TTM and earnings base, beat streak order

**Source:** the Q3i repairs scheduled for the fundamentals unit in `BUSINESS_RULES.md` §3: BR-FND-004 and BR-FND-006 (incomplete TTM, long-term-debt stand-in), BR-FND-007 (negative earnings base tiered CLEAN), and BR-SHR-010/012 (beat streak counted from the oldest row). The brief's R5 fundamentals list: "incomplete-TTM state, the negative-earnings-base credit state, newest-first beat streak".

**Q7 targeted extraction:** done on the current code (`server.py` `get_credit_health` / `get_earnings_momentum`, `worker/src/yahoo-finance.ts` `getCreditHealth` / `getEarningsMomentum`) and the parity cases in `cases_pricing_evidence_tools.py`. There were no other fundamentals defects in §3. BR-SHR-003/004 (analyst MIXED) were scheduled for R4b. Form 4 multi-line parsing is the shared item.

**Planner calls (Q3i rules P1 never overstate, P2 alignment, P4 no new codes):**
1. A TTM component needs values in all 4 newest quarters; otherwise it is null and missing. Row choice and fallback order are unchanged. No count field and no new warning: `missingComponents` and `dataQuality` PARTIAL carry it (P4).
2. Leverage only over operational EBITDA > 0. Otherwise leverage and stress flag are null. No STRESSED inference (P1: withhold, do not assert). Below 0 with net debt known, `debtTier` is the new value `NEGATIVE_EARNINGS_BASE`, the "unresolved/negative-earnings-base state" of the owner's Q3i decision (2026-10-15). The new enum value went to the owner on a card on 2026-10-30, recommended over a null tier; exactly 0 stays a null tier.
3. Python drops the `Long Term Debt` stand-in (the Worker's behaviour; P1, P2).
4. Beat streak ordered by quarter date, newest first. A counted row without a date → streak null (the field already allows null).

**Version:** 3.5.0 (minor: published figures change; one new `debtTier` value). Hashes unchanged.

## Tests

- `cases_pricing_evidence_tools.py`:
  - Flipped: `q3i_loss_maker_with_net_debt_is_tiered_clean` → `loss_maker_with_net_debt_has_the_negative_earnings_base_tier`; `q3i_three_quarter_ttm_python_partial_worker_ok` → `three_quarters_are_not_a_ttm`; `q3i_long_term_debt_fallback_exists_only_in_python` → `long_term_debt_alone_is_not_total_debt`; the two `q3i_beat_streak_*` cases → `beat_streak_counts_from_the_most_recent_quarter` and `beat_streak_of_the_two_newest_quarters`.
  - `rows_missing_actual_or_estimate_are_skipped` now expects streak 0.
  - New: `loss_maker_with_net_cash_has_the_negative_earnings_base_tier`, `a_quarter_gap_makes_that_component_incomplete`, `beat_streak_follows_dates_when_rows_are_newest_first`, `beat_streak_is_withheld_when_a_quarter_date_is_missing`.
  - The momentum helper now sends quarter dates to the Worker, as Yahoo does.
- `test_data_quality.py` and `test_earnings_tools.py`: the credit fixtures now have four quarters.

## Exit

- [ ] Full parity harness and every `ci.yml` test command.
- [ ] PR, CI green, merge (owner allowed Claude to merge on green, 2026-10-30).
- [ ] Live check: `health_check` 3.5.0, hashes unchanged; RIVN/LCID/PLUG credit health have null leverage and debtTier `NEGATIVE_EARNINGS_BASE`; AAPL leverage still about 0.27 and CLEAN; AAPL earnings momentum `currentBeatStreak` counted from the newest quarter.
