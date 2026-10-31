# Phase 5 unit 7: Python evidence-pack orchestration and the valuation handlers (3.6.7)

Follow `analysis/yahoo-finance-mcp/PLAYBOOK.md` §1–§3. Precedents: Phase 5 units 1–6 (`PHASE5_*_SPEC.md`). The brief's unit 7 is the Python evidence-pack orchestration (target `evidence_pack.py`); the valuation handlers, the last domain region in both god files, move in the same release.

## Rule zero
Pure move. Output is byte-identical: no behaviour, text, key order, number format, warning, status, schema, cache key or edge-cache URL may change. The parity harness passes with no expected value edited. Do not fix anything you notice; list it in your report.

## Scope (topology `func_region`, regenerated 2026-10-31 at 3.6.6, 83a5e48)
`analysis/yahoo-finance-mcp/phase5_evidence_valuation_region.json`:
- `server.py#evidence` (7): `build_valuation_evidence_pack`, `get_evidence_quality`, `get_evidence_cut`, `list_evidence_cuts`, `_sec_filing_rows`, `_run_component`, `_not_applicable`.
- `server.py#valuation` (9): `get_valuation_snapshot`, `get_historical_valuation_context`, `reconcile_metric_sources`, `_valuation_history_for`, `_cover_counts_for`, `_periodic_reports`, `_daily_history`, `_daily_history_sync`, `_yahoo_statement_rows_sync`.
- `worker/src/yahoo-finance.ts#valuation` (7): `getValuationSnapshot`, `getHistoricalValuationContext`, `reconcileMetricSources`, `valuationHistoryFor`, `coverCountsFor`, `periodicReports`, `dailyHistory`.
Move every listed function plus the module constants and types used only by them. `server.py#transport` (`get_server`, `_build_grouped_server`) stays.

## Destinations
- Python evidence: new `yfmcp/tools/evidence_pack.py`. Python valuation: new `yfmcp/tools/valuation.py` (handlers; the existing `yfmcp/valuation.py` keeps the calculation logic). Neither may import `server`. If the evidence pack calls valuation handlers, `evidence_pack.py` imports `yfmcp.tools.valuation`, never the reverse. Side-effect imports and re-export blocks in `server.py`; keep `tools/list` order identical (expanded and grouped) with `after=`. Retarget test patch targets only (watch for `server` patches that silently stop reaching moved code; `test_valuation.py` patches `_valuation_market_inputs` on both `server` and `fundamentals`), never assertions.
- Worker valuation: new `worker/src/valuation-tools.ts` (the existing `worker/src/valuation.ts` keeps the calculation logic). `yahoo-finance.ts` re-exports every previously exported moved symbol. Check no load-time reads across any import cycle.
- Topology: register the new files in `FILE_DOMAIN` (`evidence_pack.py` as `"evidence"`, the two valuation files as `"valuation"`); remove `DOMAIN_OVERRIDES` entries whose functions moved.

## Verification
As in units 1–6: Python `ast.dump` identity (decorators with `after=` stripped) and identical expanded/grouped `list_tools`; Worker line multiset, identical `listVisibleTools()` manifest, `tsc --noEmit`, sole-entry esbuild evaluation; full parity harness and every `run:` step of `ci.yml`.

## Outcome (3.6.7)
- Python: 9 valuation functions and 4 valuation-only constants moved to `yfmcp/tools/valuation.py`; 7 evidence functions to `yfmcp/tools/evidence_pack.py`. Side-effect imports sit where the old code was, so registration order is unchanged and no `after=` was needed. Only the 7 handlers are re-exported from `server.py`; `_sec_filing_rows` and `_yahoo_statement_rows_sync` are not, because tests patch them on the defining module. Five imports left unused in `server.py` were removed.
- Worker: 7 functions and 5 constants moved to `worker/src/valuation-tools.ts`; no `export` added; `yahoo-finance.ts` re-exports the three handlers. An unused `_buildFilingIndexFromHtml` import was removed.
- Tests: patch targets retargeted in `cases_major_300.py`, `cases_valuation.py`, `cases_r5val1.py`, `cases_pricing_evidence_tools.py`, `test_companyfacts_coverage.py`, `test_evidence_tools.py`, `test_valuation.py`; no assertion or expected value changed. A patch-reach audit showed every patch that reached code at base still does.
- `extract_topology.py` follows `export { … } from` re-exports (before this, `valuation-tools.ts` showed as a dead end because only a re-export pointed at it).
- `server.py` 972 → 460 lines; `yahoo-finance.ts` 630 → 346 (under the Phase 5 exit target of 500). Left in the god files: `server.py#transport` (2). Next: unit 8 table router.
