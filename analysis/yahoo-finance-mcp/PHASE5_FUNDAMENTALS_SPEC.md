# Phase 5 unit 4: move fundamentals and analyst tools out of the god files (3.6.4)

Follow `analysis/yahoo-finance-mcp/PLAYBOOK.md` §1–§3 exactly. Precedents: Phase 5 units 1–3 (`PHASE5_SEC_SPEC.md`, `PHASE5_EXPOSURE_SPEC.md`, `PHASE5_PRICING_SPEC.md`). The Q7 targeted extraction for fundamentals was done before 3.5.0 (`R5FUNDAMENTALS1_SPEC.md`), so the entry criterion is met.

## Rule zero
Pure move. Output is byte-identical: no behaviour, text, key order, number format, warning, status, schema, cache key or edge-cache URL may change. The parity harness passes with no expected value edited; pinned FINDING cases stay. Do not fix anything you notice; list it in your report.

## Scope (topology `func_region`, regenerated 2026-10-31 at 3.6.3, 524b323)
`analysis/yahoo-finance-mcp/phase5_fundamentals_region.json` lists:
- `server.py` (52): the `#fundamentals` region (49: company profile and stock info, statements and ratios, calendars and corporate actions, holders and ownership, ETF and fund profiles, analyst consensus, recommendations, rating changes and upgrade radar, EPS revisions, earnings analysis and momentum, credit health, share-count trend, peer valuations, analyst valuation methods, options-flow summary alias), the `#shared` helper `_peer_valuation_error`, and the two position tools unit 3 left behind (`get_position_score_inputs`, `analyze_position_signals`, with `POSITION_SINGLE_TICKER_MESSAGE`).
- `worker/src/yahoo-finance.ts` (31): the `#fundamentals` region (30) and the `#shared` helper `peerValuationError`.
Move every listed function, plus the module constants and types used only by them. The valuation handlers (`#valuation`) stay for a later unit and import `_peer_valuation_error` / `peerValuationError` from the new module.

## Destinations
- Python: new `yfmcp/tools/fundamentals.py`. The two position tools go here too (they call the fundamentals handlers; putting them in `pricing.py` would make `pricing.py` and `fundamentals.py` import each other). `fundamentals.py` must not import `server`. Wiring in `server.py`: side-effect import plus a re-export block for names read as `server.<name>` by server.py, routing, scripts or tests. Do not re-export a constant tests patch. Keep `tools/list` registration order identical (expanded and grouped) with `tool(..., after=...)` where needed. Patch targets in tests change to `yfmcp.tools.fundamentals.<name>`; never change assertions. Watch for patches on `server` that silently stop applying once the code they target moves (unit 3 found one): a patch must still reach the code it was written for.
- Worker: new `worker/src/fundamentals.ts`. `yahoo-finance.ts` re-exports every previously exported moved symbol (`export { … } from "./fundamentals.js"`). `pricing.ts` imports `getAnalystConsensus`, `getEarningsMomentum` and `getAnalystUpgradeRadar` from `./fundamentals.js` instead of `./yahoo-finance.js`. Check that no top-level declaration in any module of a resulting cycle reads another's bindings at load time.
- Topology: register both new files as `"fundamentals"` in `FILE_DOMAIN`, and remove `DOMAIN_OVERRIDES` entries whose functions moved (overrides apply only to the two god files; the script raises if one names a function its file no longer defines).

## Verification (each half on its own)
- Python: `ast.dump` identity of every moved function and constant vs base `server.py` (decorators that gain `after=` compared with that keyword removed); expanded and grouped `list_tools` identical.
- Worker: moved text keeps its line multiset (only `export` added); `listVisibleTools()` manifest identical in both modes; `npx tsc --noEmit -p worker`; each module evaluates as sole esbuild entry.
- Full parity harness and every `run:` step of `.github/workflows/ci.yml`.

## Outcome (3.6.4)
- Python: all 52 functions moved to `yfmcp/tools/fundamentals.py`, with 13 constants. Four decorators gained `after=`. Tests that patched moved code on `server` now patch `yfmcp.tools.fundamentals`; the parity harness caught one (`_position_case` in `cases_neutrality.py`) and a `setattr` hook over every CI step confirmed no other stale patch. `test_valuation.py` gains a second `_valuation_market_inputs` patch because it calls both `get_valuation_snapshot` (still in `server.py`) and `compare_peer_valuations` (moved).
- Worker: all 31 functions moved to `worker/src/fundamentals.ts`, with 13 constants. `HOLDER_MOD` and `REC_MOD` stay in `yahoo-finance.ts` and gained `export`; `SUPPORTED_HOLDER_TYPES` and `SUPPORTED_RECOMMENDATION_TYPES` stay there too (a test reads them from that file). `pricing.ts` imports the three analyst handlers from `fundamentals.ts`.
- `server.py` 3,569 → 1,173 lines; `yahoo-finance.ts` 5,426 → 3,639 lines. Neither god file has a `fundamentals`, `pricing` or `shared` region left. Left: earnings, evidence (Python), providers, transport (Python), valuation.
