# R5-extractors-2 (3.0.6): in the money at the strike, leap days, foreign dollars

**Source:** the brief's R5 extractor list (`MODERNIZATION_BRIEF.md`, R5-‹unit› Extractors: "Q3f inclusive `>=` (§7 V4), Feb 29 arithmetic, `NT$`/`A$` currency recognition"), decision Q3f (§7, CHANGE: inclusive `>=` across instrument classes), and the Q3i scheduled repairs on rule cards BR-EXT-110, BR-EXT-059 and BR-EXT-064.
**Owner decision (2026-10-29, card "Feb 28 + own currency")** on the two SME questions on those cards:
- BR-EXT-110: 29 February plus N years becomes **28 February** in a common year (1 March was the alternative).
- BR-EXT-059 / 064: foreign-dollar amounts **carry their own currency** (not reading them as amounts was the alternative).

**Version:** 3.0.6 (patch). No new field, enum value, warning code or `unresolvedReason`. Values change: the `inTheMoney` flag at the strike, `periodThrough` dates after a 29 February period end, and the `currency` value and amount of foreign-dollar funding text.

The rest of the brief's extractor list (BR-EXT-015 monetary loss sign; Q5 China preset with aliases moved to data, which also closes F39) ships next as R5-extractors-3.

## 1. Items

### Q3f: one inclusive in-the-money boundary

| Site | Worker | Python |
|---|---|---|
| option tranches | `capital-structure.ts` tranche loop | `capital_structure.py` tranche loop |
| weighted-strike options (`inTheMoney`) | `capital-structure.ts` | `capital_structure.py` `out["inTheMoney"]` |
| warrants | `capital-structure.ts` | `capital_structure.py` |
| scenario lines: exclude, gross, treasury stock | `share-scenarios.ts` `exercisable` | `share_scenarios.py` `_exercisable` |
| capped call | `share-scenarios.ts` `scenarioLines` | `share_scenarios.py` `_scenario_lines` |

- Every site moves from `price > strike` to `price >= strike`. Convertibles and convertible preferred already used `>=`.
- Treasury stock is unchanged: `count x (1 - strike / price)` only when price > strike, so 0 at the strike. No share count, dilution percentage or total changes; only the flag does (§7 V4).

### BR-EXT-110: 29 February plus N years

- `addYears` (`capital-structure.ts`, `funding-schedule.ts`) and `_add_years` (`capital_structure.py`, `funding_schedule.py`) keep string arithmetic on the year, and turn `-02-29` into `-02-28` when the target year is a common year (Gregorian rule: divisible by 4, not by 100 unless by 400).
- Both TS helpers are now exported, for the property cases only. No call site changes.
- Affects the funding schedule's contractual `periodThrough`, the capital-structure maturity ladder `periodThrough`, and debt term ends computed from a period end.

### BR-EXT-059 / 064: foreign-dollar amounts

- Funding-schedule money pattern (both runtimes): `(?<![A-Za-z])(US|NT|AU|A|CA|C|HK|S|NZ)?\$...`, case-insensitive as before.
  - Prefix to currency: none and `US` USD, `NT` TWD, `AU`/`A` AUD, `CA`/`C` CAD, `HK` HKD, `S` SGD, `NZ` NZD.
  - The lookbehind means a `$` straight after other letters (`R$`, `MX$`) is not an amount. The sentence is then judged as having no amount (`NOT_STATED`, currency null).
  - `amountAsWritten` includes the prefix (`NT$5 billion`).
  - Two amounts form a range only when they share a currency. Otherwise the first amount stands alone (`stated`).
  - Each text item's `currency` is the matched amount's currency (null when no amount). It was hard-coded `USD`.
- Dilution-bridge ATM capacity (`capital-structure` money pattern, both runtimes): the same lookbehind, so `NT$` and `A$` figures are no longer read as US dollars into `amountUsd`. Its prefix set stays US-only.

## 2. Tests

- Pinned cases flipped to `equal` with the repaired values (`scripts/parity/cases_capital_structure.py`):
  - `cap.ifconv.price_equal_conversion_price_counts_all_shares`, `cap.scen.in_the_money_boundary_options_vs_convertibles`, `cap.pref.price_equal_conversion_price_counts_all_shares`;
  - `cap.funding.leap_day_period_through_invalid_date`, `cap.funding.leap_day_maturity_ladder_period_through_invalid_date`;
  - `cap.funding.text_{ntd,aud,cad}_dollar_labelled_usd` replaced by `cap.funding.text_{ntd,aud,cad,hkd,usd_prefix}_dollar_carries_its_currency`.
- New equal cases: `cap.funding.text_unlisted_dollar_prefix_is_not_an_amount`, `cap.funding.text_two_currencies_are_not_a_range`.
- Property cases (`scripts/parity/cases_r5ext2_properties.py`, the brief's R-release requirement):
  - Both `addYears` copies are checked against Python `datetime` for 1 to 5, 7, 10 and 30 years (ladder buckets reach 5; debt term ends run longer) over about 14,000 dates (1896 to 2404, every end of February plus the 1st and 15th of each month): 110,936 results each.
  - `shareCountScenarios` over 20 strikes (9 fixed, 11 seeded random) and about 240 prices (at, one cent and 0.01% either side, half, double, 5 seeded random): 714 lines.
    - Each option, warrant and convertible line is in the money exactly when price >= threshold.
    - Treasury-stock shares are 0 at or below the strike, between 0 and the count, and never fall as the price rises.
    - A convertible counts all its shares at or above the conversion price, none below.
  - A mutation check (old `>` and old year arithmetic restored in Python) fails both properties.
- Rest of the harness diff: 0 (1,450 tests before the property file, all OK).

## 3. Exit

- [ ] Local CI: every `ci.yml` test command.
- [ ] PR, CI green, owner merge.
- [ ] Live check after deploy: hashes unchanged (no schema change), `health_check` 3.0.6, and one capital-structure call (for example AAPL) unchanged apart from the flags covered here.
