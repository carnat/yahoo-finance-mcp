# R5-earnings part 1 (3.3.0): release EPS, dated Yahoo rows, zero estimates, rounding, source_url message

**Source:** the brief's R5 earnings list (`MODERNIZATION_BRIEF.md` R5-‹unit›: surprise actuals per source policy R3, official release first), the Q3i triage (call 5, the `source_url` message, BR-ERN-039), and the scheduled repairs BR-ERN-018 and BR-ERN-031 (`BUSINESS_RULES.md` §3).

**Version:** 3.3.0 (minor: `actual.eps` from the release carries that metric's fields). No new field, status, warning code or enum value. `schemaHash` and `manifestHash` unchanged.

**Not in this release:** BR-ERN-009, the transcript fallback suggestion, waits for the owner's card (2026-10-29) and ships as R5-earnings part 2. BR-ERN-039 needs no change: 2.26.5 (F55, owner-approved) made any `sec_8k*` source HIGH in both runtimes, which supersedes the triage line "ex-99.1 commentary becomes MEDIUM".

## 1. Items (`compare_earnings_actual_vs_estimate` unless named)

| Item | Before | Now (both runtimes) |
|---|---|---|
| R3 actual EPS | Yahoo `epsActual` first, release `epsDiluted` as fallback, always labelled yahoo/LOW | release `epsDiluted` first with its own fields and period; Yahoo only when the release has none, yahoo/LOW |
| R3 comparison scope (planner, P1) | EPS surprise always against the selected Yahoo row | a release EPS is compared only when MATCHED; otherwise surprise withheld, alignment warning names EPS too; a release with no period gets `PERIOD_ALIGNMENT_UNVERIFIED` (existing code) |
| Call 5 estimatePeriod | Python "2026-06-30T00:00:00", Worker "2026-06-30" | `YYYY-MM-DD` |
| Call 5 estimate revenue | Worker returned the dated row's revenue estimate, Python null | withheld (null) while UNVERIFIED |
| Call 5 alignment (P2) | Python had no within-a-week period-end match, `quarterEnd` from Yahoo, revenue period only the fiscal label | Python takes the Worker's rule, `quarterEnd` and period fallback |
| BR-ERN-031 | EPS estimate 0 → `EPS_ESTIMATE_UNAVAILABLE`, no delta | near-zero rule: `epsDelta`, no percent, MEDIUM |
| BR-ERN-018 rounding | Python `round()` half-even, Worker `toFixed` | half away from zero (`_round_half_away_from_zero` / `roundHalfAwayFromZero`) |
| `source_url` message | Python "must use https" for text with no scheme | "source_url must be a valid URL" |

## 2. Tests

- `cases_sec_earnings.py`: `unparseable_url_message_differs`, `zero_eps_estimate_gives_no_delta`, `date_identified_row_is_unverified_runtimes_differ` and `actual_eps_comes_from_yahoo_not_the_release` flip to equal under new names.
- `cases_r5ern1.py`: release EPS against another quarter (MISMATCH) and an unverified row; 0.125% rounds to 0.13; the release and Yahoo `actual.eps` rows; XBRL period end 2026-06-27 matching the Yahoo row dated 2026-06-30.

## 3. Exit

- [ ] Full parity harness and every `ci.yml` test command.
- [ ] PR, CI green, owner merge.
- [ ] Live check: `health_check` 3.3.0, hashes unchanged; AAPL and MSFT comparisons MATCHED with `actual.eps` from SEC XBRL (HIGH) and an EPS surprise; `index_earnings_release` with `source_url` "not a url" gives "source_url must be a valid URL".
