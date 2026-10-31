# Phase 5 unit 3: move pricing and options out of the god files (3.6.3)

Follow `analysis/yahoo-finance-mcp/PLAYBOOK.md` §1–§3 exactly. Precedents: Phase 5 unit 1 (`PHASE5_SEC_SPEC.md`, 3.6.1) and unit 2 (`PHASE5_EXPOSURE_SPEC.md`, 3.6.2).

## Rule zero
Pure move. Output is byte-identical: no behaviour, text, key order, number format, warning, status, schema, cache key or edge-cache URL may change. The parity harness passes with no expected value edited; pinned FINDING cases stay. Do not fix anything you notice; list it in your report.

## Scope (topology `func_region`, regenerated 2026-10-31 after 3.6.2)
`analysis/yahoo-finance-mcp/phase5_pricing_region.json` lists `server.py#pricing` (23 functions: quotes, historical prices, price and volume analysis, position signals, price targets, option expiries and chains, options flow, put hedges, screener, ticker search) and `worker/src/yahoo-finance.ts#pricing` (51 functions). Move every listed function, plus the module constants and types used only by them. Helpers the topology puts in `#shared` or other domains stay and are imported.

## Destinations
- Python: append to the existing `yfmcp/tools/pricing.py` (it already holds the Phase 2 pricing tools and is imported near the top of `server.py`). Because that import runs before the moved tools' old position, keep `tools/list` registration order identical with `tool(..., after=...)` on each moved tool whose position would change; verify the full expanded and grouped listings byte for byte. `pricing.py` must not import `server`. Wiring in `server.py`: extend the existing `from yfmcp.tools.pricing import (...)` re-export block with names read as `server.<name>` by server.py, routing, scripts or tests. Do not re-export a constant tests patch. Patch targets in tests change to `yfmcp.tools.pricing.<name>`; never change assertions.
- Worker: new `worker/src/pricing.ts`. `yahoo-finance.ts` re-exports every previously exported moved symbol (`export { … } from "./pricing.js"`) so `tools.ts`, the parity bundle and other importers stay unchanged. Modules that imported moved names from `./yahoo-finance.js` may keep doing so through the re-export. A cycle with `yahoo-finance.ts` is fine only if no top-level declaration reads the other module's bindings at load time; check it.
- Topology: register `worker/src/pricing.ts` as `"pricing"` in `FILE_DOMAIN` (`yfmcp/tools/pricing.py` is already registered; check), and remove `DOMAIN_OVERRIDES` entries whose functions moved (the script raises if one names a function its file no longer defines). Overrides apply only to the two god files.

## Verification (each half on its own)
- Python: `ast.dump` identity of every moved function and constant vs base `server.py` (decorators that gain `after=` compared with that keyword removed); expanded and grouped `list_tools` identical (names, order, descriptions, schemas).
- Worker: moved text keeps its line multiset (only `export` added); `listVisibleTools()` manifest identical in expanded and grouped modes; `npx tsc --noEmit -p worker`; each module evaluates as sole esbuild entry.
- Full parity harness and every `run:` step of `.github/workflows/ci.yml` (`test_universal_aliases.py` needs `MCP_TOKEN`; it is conditional in CI).

## Outcome (3.6.3)
- Python: 21 of the 23 functions moved into `yfmcp/tools/pricing.py`. `get_position_score_inputs` (with `POSITION_SINGLE_TICKER_MESSAGE`) and `analyze_position_signals` stay in `server.py`: they call `get_analyst_consensus`, `get_analyst_upgrade_radar` and `get_earnings_momentum`, which are fundamentals code still in `server.py`, and `pricing.py` may not import `server`. They move after the fundamentals handlers in Phase 5 unit 4. Fourteen moved tools gained `after=` to keep the `tools/list` order. One parity case (`cases_major_300.py`) had two patches on `server` that silently stopped applying once the code moved; they now patch `yfmcp.tools.pricing`.
- Worker: all 51 functions moved to `worker/src/pricing.ts`, with 17 constants, caches and types. No `export` had to be added. Seven exported names that only moved code uses (`HISTORICAL_PERIODS`, `HISTORICAL_INTERVALS`, `INDICATOR_MIN_LOOKBACK`, `POSITION_SINGLE_TICKER_MESSAGE`, `getAnalystConsensus`, `getEarningsMomentum`, `getAnalystUpgradeRadar`) stay in `yahoo-finance.ts` and are imported back.
- `server.py` 4,607 → 3,569 lines; `yahoo-finance.ts` 8,271 → 5,426 lines. The Worker god file has no `pricing` region left; `server.py#pricing` holds only the two position tools.
