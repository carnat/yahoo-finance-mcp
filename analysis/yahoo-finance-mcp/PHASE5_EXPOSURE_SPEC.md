# Phase 5 unit 2: move the SEC extractors and exposure tools out of the god files (3.6.2)

Follow `analysis/yahoo-finance-mcp/PLAYBOOK.md` §1–§3 exactly. Precedent: Phase 5 unit 1 (`PHASE5_SEC_SPEC.md`, 3.6.1), which moved SEC retrieval into `yfmcp/tools/sec.py` and `worker/src/sec.ts`.

## Rule zero
Pure move. Output is byte-identical: no behaviour, text, key order, number format, warning, status, schema, cache key or edge-cache URL may change. The parity harness passes with no expected value edited; pinned FINDING cases stay. Do not fix anything you notice; list it in your report.

## Scope (topology `func_region`, regenerated 2026-10-31 at 43767a2)
`analysis/yahoo-finance-mcp/phase5_exposure_region.json` lists the functions: `server.py#extractors` (37) plus `query_sec_filing_index` (left in `server.py` by unit 1 because it calls these extractors), 38 in all; and `worker/src/yahoo-finance.ts#extractors` (43). Move every listed function, plus the module constants and types used only by those functions. Helpers the topology puts in `#shared` or other domains stay and are imported.

## Destinations
- Python: `yfmcp/tools/exposure.py`. `query_sec_filing_index` goes here too (not `sec.py`), so `sec.py` never imports `exposure.py` and no import cycle forms. Wiring in `server.py`: side-effect import plus a re-export block for names read as `server.<name>` by server.py, routing, scripts or tests. Do not re-export a constant tests patch. Keep `tools/list` registration order (use `tool(..., after=...)` where needed). Patch targets in tests change to `yfmcp.tools.exposure.<name>` (or stay on `yfmcp.tools.sec` for SEC helpers); never change assertions. Paired patches added in unit 1 because extractors still read `server`'s bindings can collapse to the module that now reads them, provided no assertion changes.
- Worker: `worker/src/exposure.ts`. `yahoo-finance.ts` re-exports every previously exported moved symbol (`export { … } from "./exposure.js"`) so `tools.ts`, the parity bundle and other importers stay unchanged. `sec.ts` imports the extractors it uses from `./exposure.js` instead of `./yahoo-finance.js`. A cycle among `exposure.ts`, `sec.ts` and `yahoo-finance.ts` is fine only if no top-level declaration reads another module's bindings at load time; check it.
- Register the new file in `FILE_DOMAIN` in `analysis/yahoo-finance-mcp/extract_topology.py` as `"extractors"`, and remove `DOMAIN_OVERRIDES` entries whose functions moved (the script raises if one names a function its file no longer defines).

## Verification (each half on its own)
- Python: `ast.dump` identity of every moved function and constant vs `git show 43767a2:server.py`; expanded and grouped `list_tools` identical (names, order, descriptions, schemas).
- Worker: moved text keeps its line multiset (only `export` added); `listVisibleTools()` manifest identical in expanded and grouped modes; `npx tsc --noEmit -p worker`; each module evaluates as sole esbuild entry.
- Full parity harness and every `run:` step of `.github/workflows/ci.yml` (`test_universal_aliases.py` needs `MCP_TOKEN` and fails with 401 in the sandbox, as on main).

## Outcome (3.6.2)
- Python: all 38 functions moved to `yfmcp/tools/exposure.py`, with 13 constants. `_earnings_release_picks` and `_read_release_text` (topology domain `earnings`) moved too, because `extract_operating_driver_ledger` calls them and `exposure.py` may not import `server`; importing `yfmcp/tools/earnings.py` early would change the `tools/list` order. `server.py` re-exports them for `get_guidance_history` and `reconcile_metric_sources`. They join the earnings module in unit 6. Three decorators gained `after=` to keep the registration order.
- Worker: all 43 functions moved to `worker/src/exposure.ts`, with 24 constants, caches and types (including `CHINA_PRESET` and the iXBRL document cache). `earningsReleasePicks` and `readReleaseText` stay in `yahoo-finance.ts` and gained `export`. `sec.ts` imports the extractors from `./exposure.js`.
- `server.py` 6,579 → 4,607 lines; `yahoo-finance.ts` 10,183 → 8,271 lines. Neither god file has an `extractors` or `sec` region left.
