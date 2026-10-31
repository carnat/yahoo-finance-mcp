# Python/Worker differences still accepted by the parity harness (2026-10-31)

Phase 6 exit criterion: "No pinned expected difference remains in the harness except documented intentional ones."
After 3.6.11 the harness accepts 16 differences (22 cases). Three more candidate defects came up during Phase 5.
Each row gives a recommendation. "Align" means the other runtime takes this behaviour, with parity tests and a docs section.

Proposed release: **3.7.0** (minor: some status values, messages and meta keys change in one runtime).

## Keep, and document as intentional (no code change)

| Id | Difference | Why keep |
|---|---|---|
| D1 | Batch near the Worker's subrequest budget: the Worker returns PROVIDER_BUDGET_EXHAUSTED for later tickers; Python finishes them | A Cloudflare runtime limit; Python has none |
| D2 | News `duplicateGroupId`: Python 16-hex SHA-256 prefix, Worker 8-hex hash | Opaque id; callers only compare within one response |
| D3 | The Worker adds `__batchMeta` to batch news results | Worker-only batch accounting |
| D4, D5 | The Worker remembers a Finnhub "no access" for 6 h and Alpha Vantage's daily limit until 00:00Z; Python asks again | The Worker shares one key across all callers; a local Python server is one user |
| D15, D16 | Python-only URL check and news audit script (Worker side is a placeholder) | Nothing to compare; move out of the pinned list into a "Python-only tooling" note |

## Align (code change in one runtime)

| Id | Tool | Today | Recommendation |
|---|---|---|---|
| D6 | get_analyst_consensus | Python still emits deprecated `totalAnalysts`; Worker does not | Python drops it (deprecated since 2.27.0; same count is in `numberOfAnalysts`) |
| D7 | get_company_press_releases, extract_sec_filing_fact, get_sec_filing_section_markdown | Python meta has only `reliabilityClass`; Worker also has `capabilityStatus`, `failureMode`, `evidenceRequired`, `sourceType` | Python adds the four keys with the Worker's values |
| D8 | get_expanded_institutional_ownership | Python's `or` chain drops a 0 value (sharesHeld 0 becomes the next field); Worker keeps 0 | Python keeps 0 (Worker) |
| D9 | Finnhub 403 with a JSON body | Python message = parsed error text; Worker message = raw JSON object | Worker uses the parsed error text (Python) |
| D10 | Finnhub 403 with a plain-text body | Python AUTH_ERROR "HTTP Error 403: stubbed"; Worker ENTITLEMENT_REQUIRED + body text | Python returns ENTITLEMENT_REQUIRED (Worker) |
| D11 | Finnhub 200 with a JSON array | Python NOT_FOUND; Worker PROVIDER_ERROR | Python returns PROVIDER_ERROR (Worker): a wrong shape is a provider fault, not an absence |
| D12 | get_historical_put_call_ratio, date before 2008 | Same code, different message | Python uses the Worker's one message for every date fault |
| D13 | get_historical_put_call_ratio, date "20240315" | Python accepts; Worker refuses | Python refuses (documented format is YYYY-MM-DD) |
| D14 | get_historical_put_call_ratio, blank ratio string | Python PROVIDER_ERROR; Worker OK with ratio 0 | Worker returns PROVIDER_ERROR (Python): a blank is not a zero |

## Candidate defects found during Phase 5 (both runtimes)

| Id | Where | Today | Recommendation |
|---|---|---|---|
| C1 | Valuation snapshot, IFRS filers | IFRS `ProfitLoss` is used raw, without the non-controlling-interest check that us-gaap got in 3.4.0 | Same rule as 3.4.0 (your "withhold" choice): ProfitLoss without an attributable figure is NOT_RESOLVED |
| C2 | Historical valuation context P/E | Uses `ProfitLoss` raw | Same rule as C1 |
| C3 | Form 4 insider transactions (HTML path) | Transaction codes keep footnote marks, e.g. "S(1)" | Strip the mark so the code reads "S"; no new field |

## Outcome (3.7.0)

Implemented as recommended (owner 2026-10-31 13:36Z). D7 carries the Worker's whole status-table entry, including successCriteria, limitationStatuses and note where the Worker has them. C1 applies to reconcile_metric_sources (IFRS net income). The harness now rejects any pinned case whose decision does not start with INTENTIONAL. Specs: R370_A/R370_B in the planner scratchpad; details in docs/provider-runtime-guidance.md (3.7.0).
