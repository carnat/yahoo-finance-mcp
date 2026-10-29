# R5-pricing-1 (3.0.8): zero price, rounding, CAGR start, hedge expiries, US venues

**Source:** the brief's R5 pricing list (`MODERNIZATION_BRIEF.md`, R5-‹unit› Pricing), decisions Q3b and Q3d (§7), and the binding Q3i triage (`Q3I_TRIAGE.md`: G1 zero price, G5 CAGR boundary, BR-PRC-040 changePct, calls 23 and 24).

**Version:** 3.0.8 (patch). No new field, enum value, warning code or `unresolvedReason`. `schemaHash` changes because the `search_ticker` descriptions now list the US venues.

**Not in this release:**
- Q3c zero volume, short-history CAGR and BR-PRC-002 freshness need new names (`ZERO_VOLUME_NO_RATIO`, `INSUFFICIENT_HISTORY`, `SESSION_CALENDAR_UNKNOWN`). They wait on the owner's calendar and warning-placement cards and ship as 3.1.0.
- `analyze_position_signals` calls 1 to 4 and quoteDataDate (G6), plus the position-input `dataDate` fallbacks, ship as R5-pricing part 3.

## 1. Items

| Item | Tool | Before | Now |
|---|---|---|---|
| G1 zero price | `calculate_price_target_distance` | Python ZeroDivisionError; Worker null | `distanceToTargetPct` null in both |
| Q3d rounding | price target %, snapshot `changePct` (BR-PRC-040), volume averages and ratios | Python half-even on binary, Worker `toFixed` / `Math.round` | half away from zero on the decimal value (`roundHalfAwayFromZero`, moved to `util.ts` / `yfmcp/util.py`) |
| Q3c volume average | `analyze_volume_ratio` | Worker divided by the rounded average | both divide by the unrounded average |
| Q3d quote date | `analyze_moving_average_position`, `calculate_price_target_distance` | Worker today's date | null when the quote has no trade date |
| G5 CAGR start | `get_price_stats` | Worker N x 365.25 days, Python `DateOffset` on timestamps | first bar on or after the exchange-local date N years back (`addIsoYears` / `_add_years`, 29 Feb to 28 Feb) |
| Call 23 hedge expiries | `find_put_hedge_candidates` | fell back to every expiry when all were under a week | fails closed: no qualifying expiry, no candidates |
| Call 24 US venues | `search_ticker` `exchange: "US"` | Worker NMS, NYQ; Python NMS, NYQ, PCX | NMS, NGM, NCM, NYQ, ASE, PCX in both |
| Q3b slope days | `get_price_slope` | Worker read junk as 5; Python raised | `INPUT_VALIDATION_ERROR` "days must be a whole number of sessions" for anything but a number or digits |
| Q3b outline checks | `getFilingOutline` (Worker) | no ticker or accession check inside the function | same checks as Python (the tool-call gate already refused both) |

Known edges left as they are:
- Python's expanded (non-grouped) mode may refuse a non-numeric `days` in pydantic before the function runs, with pydantic's message. Production runs grouped mode.
- With both a bad accession and a bad URL, the Worker outline reports the accession first and Python the URL.

## 2. Tests

- Pinned to equal: `neutral.n3a.zero_price_has_no_distance_and_a_null_flag`; the bracket tie (75.3), missing quote time (null), volume ratio (1001, 1.0, NORMAL), CAGR horizon (`cagr3y` 10.0642), slope invalid ticker, outline invalid ticker and accession (rejected).
- New: decimal tie 100.05 to 100.1; `scripts/parity/cases_r5price1.py` (calendar oracle for `addIsoYears` / `_add_years` at -1, -3, -5 and +1 years; `days` "abc", "2.5" and `[5]` refused through each runtime's tool call).
- Removed: the non-numeric `days` pinned case (replaced by the refusal cases).

## 3. Exit

- [ ] Full parity harness and every `ci.yml` test command.
- [ ] PR, CI green, owner merge.
- [ ] Live check: `health_check` 3.0.8, manifestHash unchanged, schemaHash changed (search_ticker description only); `search_ticker` "US" keeps NGM/ASE listings; AAPL `get_price_stats` CAGR values present; `get_price_slope` days "abc" refused.
