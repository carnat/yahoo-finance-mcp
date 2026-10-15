# Parity harness fixtures

Inputs for the Phase 2 behaviour-contract harness (`scripts/parity/`, run by `scripts/test_parity_harness.py`).
They are the **equivalence oracle**: both runtimes are fed the same bytes, and expected values are derived
from them. Do not edit by hand.

- `sec/companyfacts_<TICKER>.json`: SEC `companyfacts`, trimmed to the XBRL concepts named in `yfmcp/` or
  `worker/src/`. Recorded 2026-10-15 for AAPL, COHR, MSTR, BE, AEHR, BABA, TSM, NVDA.
- `yahoo/chart_<TICKER>_5y.json`: Yahoo `v8/finance/chart` daily bars with split/dividend events. Recorded for
  NVDA (2024 10-for-1 split) and AAPL.

Re-record deliberately with `python scripts/parity/record_fixtures.py`, then review the diff: changed
inputs can change expected values in the case files.
