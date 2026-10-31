# Phase 5 unit 5: move the scarce-provider tools and Companies House out of the god files (3.6.5)

Follow `analysis/yahoo-finance-mcp/PLAYBOOK.md` §1–§3 exactly. Precedents: Phase 5 units 1–4 (`PHASE5_SEC_SPEC.md` … `PHASE5_FUNDAMENTALS_SPEC.md`).

## Harness coverage first (PLAYBOOK §1, brief Phase 5 entry criterion "Q7 extraction folded in")
`analysis/yahoo-finance-mcp/BUSINESS_RULES.md` holds BR-PRV-001…011. BR-PRV-002/003 already have execute-both cases (`cases_pricing_evidence_tools.py`, `prv.eligibility.*`). The other P1 rules, BR-PRV-001 (expanded ownership fallback lifecycle), BR-PRV-004 (historical put/call request and response validation), BR-PRV-005 (provider failure status mapping), BR-PRV-006 (provider response identity check) and BR-PRV-007 (recommended next action per provider status), get execute-both cases in a new `scripts/parity/cases_providers.py`, registered like the other case modules. P2 rules (BR-PRV-008…011, including the Companies House company-number format) get cases where cheap. Cases are characterization: they pin today's output. A Python/Worker divergence is pinned with `expect_py` / `expect_ts` and a `FINDING` decision note, never fixed. No production code changes in this step.

## Rule zero (the move)
Pure move. Output is byte-identical: no behaviour, text, key order, number format, warning, status, schema, cache key or edge-cache URL may change. The parity harness passes with no expected value edited; pinned FINDING cases stay. Do not fix anything you notice; list it in your report.

## Scope (topology `func_region`, regenerated 2026-10-31 at 3.6.4, 096e1ec)
`analysis/yahoo-finance-mcp/phase5_providers_region.json`:
- `server.py#providers` (5): Companies House (`get_uk_company_filings`, `_companies_house_get`, `_companies_house_error`, `_charge_rows`, `_issuer_name`). The Python scarce-provider tools (expanded ownership, historical put/call) already live in `yfmcp/tools/provider_gaps.py`.
- `worker/src/yahoo-finance.ts#providers` (15): Companies House, Alpha Vantage and Finnhub fetchers, expanded institutional ownership, historical put/call ratio, provider attempt/recovery/denial memory.
Move every listed function, plus the module constants and types used only by them.

## Destinations
- Python: append to `yfmcp/tools/provider_gaps.py` (the existing providers module). It must not import `server`. Keep `tools/list` order with `tool(..., after=...)` where needed. Extend the existing `from yfmcp.tools.provider_gaps import (...)` re-export block in `server.py`. Retarget test patch targets only; check no `server` patch silently stops reaching moved code.
- Worker: new `worker/src/providers.ts`. `yahoo-finance.ts` re-exports every previously exported moved symbol. Modules that import a moved name from `./yahoo-finance.js` (for example `fetchAlphaVantageJson` in `fundamentals.ts` or the earnings code) switch to `./providers.js`. Check no load-time reads across any import cycle.
- Topology: register `worker/src/providers.ts` as `"providers"` in `FILE_DOMAIN` (check `yfmcp/tools/provider_gaps.py` is registered) and remove `DOMAIN_OVERRIDES` entries whose functions moved (`fetchAlphaVantageJson`).

## Verification
As in units 1–4: Python `ast.dump` identity and identical expanded/grouped `list_tools`; Worker line multiset, identical `listVisibleTools()` manifest, `tsc --noEmit`, sole-entry esbuild evaluation; full parity harness (including the new provider cases) and every `run:` step of `ci.yml`.

## Outcome (3.6.5)
- Coverage: `scripts/parity/cases_providers.py` adds 88 execute-both cases (BR-PRV-001 11, 004 12, 005 28, 006 7, 007 8, 008 26, 009 4, 010 9, 011 18). Eight Python/Worker differences are pinned as FINDINGs for a later behaviour decision, not fixed:
  1. BR-PRV-005: a plain-text Finnhub 403 is AUTH_ERROR in Python (the reason phrase replaces a non-JSON body) and ENTITLEMENT_REQUIRED in the Worker; a JSON body gives ENTITLEMENT_REQUIRED in both.
  2. BR-PRV-005: with a JSON error body the attempt message is the parsed error in Python and the raw JSON text in the Worker.
  3. BR-PRV-005: a Finnhub 200 with a JSON array is NOT_FOUND (attempt OK) in Python and PROVIDER_ERROR in the Worker.
  4. BR-PRV-005: the Worker remembers a Finnhub 403 per endpoint for 6 hours and skips the next request; Python asks again.
  5. BR-PRV-001: Python's `or` chain skips zero Finnhub share counts and changes; the Worker's `??` keeps them.
  6. BR-PRV-004/010: a blank-string put/call ratio is no number in Python (PROVIDER_ERROR) and 0 in the Worker (OK).
  7. BR-PRV-004/010: Python accepts the compact date "20240315"; the Worker rejects it (INPUT_VALIDATION_ERROR).
  8. BR-PRV-010: the date-before-2008 message differs (Python appends a summarize_options_flow hint).
  Rule cards that disagree with the code are noted in the case module (BR-PRV-005 message patterns, BR-PRV-007/011 next actions, BR-PRV-010 message).
- Python: the five Companies House functions and two constants moved into `yfmcp/tools/provider_gaps.py`. `get_uk_company_filings` is registered `after="reconcile_metric_sources"` (anchoring on `extract_analyst_valuation_methods`, itself placed with `after=`, would move it).
- Worker: all 15 functions moved to `worker/src/providers.ts`, with 9 constants and the denial memory. `providers.ts` imports nothing from `yahoo-finance.ts`. `evidence-pack.ts` imports `fetchAlphaVantageJson` from it; the transcript code imports it back into `yahoo-finance.ts`.
- `server.py` 1,173 → 1,014 lines; `yahoo-finance.ts` 3,639 → 3,062 lines. Left: earnings (Worker 57, Python 1), valuation (9 / 7), Python evidence (7) and transport (2).
