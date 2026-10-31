# Form 4 multi-line (3.6.0): every transaction of a filing

**Source:** Q3i repair BR-SHR-016 ("multi-line Form 4 parsing instead of first-line-only", owner Q3i decision 2026-10-15; brief R5 "Shared (Form 4)"). Owner card 2026-10-31: "Add a list of every transaction", recommended over "Keep one row, the largest by value".

**Version:** 3.6.0 (minor: new item field `insiderTransactions`). Hashes unchanged (news items have no declared schema).

## Rule (both runtimes, `yfmcp/news/providers/sec.py`, `worker/src/news/providers/sec.ts`)

- XML: every `nonDerivativeTransaction` in document order, then every `derivativeTransaction`. Owner and role are shared.
- HTML: the tables in page order with the existing row rules (Table I rows; a table with no Table I rows is read as Table II).
- No transaction node in a well-formed XHTML document → HTML reader (unchanged).
- `insiderTransaction` = first row (unchanged). `insiderTransactions` = all rows.
- Summary appends " N more transaction(s) in this filing." when N ≥ 1. Title and evidenceText are unchanged.

## Tests

- `cases_pricing_evidence_tools.py`: `q3i_multi_line_filing_reports_only_the_first_transaction` → `multi_line_filing_keeps_the_first_transaction_as_insider_transaction` (equal); new `multi_line_filing_lists_every_transaction`, `derivative_rows_follow_the_non_derivative_rows`, `single_row_filing_has_a_one_row_list`.

## Exit

- [ ] Full parity harness and every `ci.yml` test command.
- [ ] PR, CI green, merge.
- [ ] Live check: `health_check` 3.6.0, hashes unchanged; a recent multi-row Form 4 (e.g. an exercise-and-sell) via `get_sec_recent_events` filing_types ["4"] lists every row.
