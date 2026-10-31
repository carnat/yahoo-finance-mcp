# Phase 5 unit 1: move SEC EDGAR retrieval out of the god files (3.6.1)

Follow `analysis/yahoo-finance-mcp/PLAYBOOK.md` §1–§3 exactly. Precedent: the Phase 3 news pilot (`PHASE3_SPEC.md`) and Phase 4 units (`PHASE4_SPEC.md`).

## Rule zero
Pure move. Output is byte-identical: no behaviour, text, key order, number format, warning, status, schema, cache key or edge-cache URL may change. The parity harness passes with no expected value edited; pinned FINDING cases stay. Do not fix anything you notice; list it in your report.

## Scope (topology `func_region`, regenerated 2026-10-31 at 617da40)
`analysis/yahoo-finance-mcp/phase5_sec_region.json` lists the functions of `server.py#sec` (56) and `worker/src/yahoo-finance.ts#sec` (54). Move every listed function, plus the module constants/types used only by those functions. Helpers the topology puts in `#shared` or other domains stay and are imported.

## Destinations
- Python: `yfmcp/tools/sec.py`. Wiring in `server.py`: side-effect import plus a re-export block for names read as `server.<name>` by server.py, routing, scripts or tests. Do not re-export a constant tests patch. Keep `tools/list` registration order (use `reseat_tool_registration` where needed, as the playbook says). Patch targets in tests change to `yfmcp.tools.sec.<name>`; never change assertions.
- Worker: `worker/src/sec.ts`. `yahoo-finance.ts` re-exports every previously exported moved symbol (`export { … } from "./sec.js"`) so `tools.ts`, the parity bundle and other importers stay unchanged. Import back from leaf modules where possible; a `yahoo-finance.ts` ↔ `sec.ts` cycle is fine only if no top-level declaration reads the other module's bindings.
- Register the new file in `FILE_DOMAIN` in `analysis/yahoo-finance-mcp/extract_topology.py` as `"sec"`.

## Verification (each half on its own)
- Python: `ast.dump` identity of every moved function vs `git show 617da40:server.py`; expanded and grouped `list_tools` identical (names, order, descriptions, schemas).
- Worker: moved text keeps its line multiset (only `export` added); `listVisibleTools()` manifest identical in expanded and grouped modes; `npx tsc --noEmit -p worker`.
- Full parity harness and the acceptance tests listed in `.github/workflows/ci.yml`.

## Outcome (3.6.1)
- Python: 55 of the 56 functions moved, with 21 constants. `query_sec_filing_index` stays in `server.py` for now: it calls eight extractors that still live there (`_as_status`, `_extract_segment_revenue`, `_extract_total_revenue`, `_extract_revenue_exposure`, `_extract_risk_factor_mentions`, `_extract_customer_concentration`, `extract_china_exposure`, `extract_geographic_revenue`), and moving it now would need a `sec` → `server` back-call. It moves with the extractors in Phase 5 unit 2, so `server.py#sec` keeps about 320 NLOC until then.
- Worker: all 54 functions moved, with 19 constants and types used only by them (including the exported `FilingSearchOptions` type, re-exported from `yahoo-finance.ts`). Six names that remaining code also uses stay in `yahoo-finance.ts` and gained `export` for `sec.ts`.
- The `sec` entries of `DOMAIN_OVERRIDES` in `extract_topology.py` were removed with their functions.
- `server.py` 9,653 → 6,579 lines; `yahoo-finance.ts` 13,264 → 10,183 lines.
