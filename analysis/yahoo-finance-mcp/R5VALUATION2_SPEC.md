# R5-valuation part 2 (3.4.0): partly tagged debt group, untagged NCI

**Source:** the scheduled repairs BR-VAL-006 and BR-VAL-025 (`MODERNIZATION_BRIEF.md` P0 rules with a scheduled repair; owner decision 2026-10-15). Owner card 2026-10-30: "Withhold the figure".

**Version:** 3.4.0 (minor: new status values debt `INCOMPLETE` and EV `DEBT_INCOMPLETE`, approved on the card). No new field or warning code; hashes unchanged.

## 1. Rules

| Item | Before | Now (both runtimes) |
|---|---|---|
| BR-VAL-006 (`valuation_history._balances_at` / `balancesAt`) | first group with any component at the cash date, summed | same group choice; if a group component is untagged at the cash date but the cash fact's accession tags it at another date, debt `INCOMPLETE` (value null, `concepts` = group, `components` = tagged parts) and EV `DEBT_INCOMPLETE` |
| BR-VAL-025 (`metric_reconciliation.sec_observations`) | ProfitLoss chosen and an NCI row missing → raw ProfitLoss FOUND | both SEC rows `NOT_RESOLVED`, value null, ProfitLoss row as `evidence` (+ `filedValues` on latest) |

Completeness test uses the existing `_accession_tags_any` / `accessionTagsAny` (any unit, any date, same accession). No cash accession → no incompleteness test (as the zero-debt rule).

## 2. Tests

- `cases_valuation.py`: `pinned_partial_debt_group_uses_tagged_component_only` → `partial_debt_group_withholds_debt_and_ev`; the four `pinned_*nci*` cases → `*_unresolved`; new `group_component_never_tagged_by_the_filing_is_complete`, `group_component_tagged_only_by_another_filing_is_complete`.

## 3. Exit

- [ ] Full parity harness and every `ci.yml` test command.
- [ ] PR, CI green, owner merge.
- [ ] Live check: `health_check` 3.4.0, hashes unchanged; AAPL `get_historical_valuation_context` EV still OK; BE `reconcile_metric_sources` net_income FY2025 still PARENT_DERIVED (NCI tagged); AAPL net income reconcile still AGREED.
