# Phase 5 unit 8: table router and `schemas.ts` (3.6.8)

Brief scope (MODERNIZATION_BRIEF.md, Phase 5 item 8): "Table router and `schemas.ts`". Phase exit criteria this unit closes: "The switch is replaced by a table" and "No function has CCN >= 100 (lizard)".

Worker-only restructuring. Python registers tools with decorators (no switch), so it has no counterpart change. No behaviour change: no tool name, input, output, schema, description, status, warning, error message, cache key or ordering changes.

## Part A: `worker/src/schemas.ts` (new)
Move the tool definition and schema block out of `worker/src/tools.ts` into `worker/src/schemas.ts`:
- `interface Tool`, `READ_ONLY_TOOL_ANNOTATIONS`, `CLOSED_WORLD_TOOL_ANNOTATIONS`, `annotationsForTool`, `TOOLS`, `CANONICAL_ADDITIONS`, the `TOOLS.push(...)`, `applyTickerCaps` and its call, all output-schema constants and helpers (`SIMPLE_OBJECT_SCHEMA` … `OUTPUT_SCHEMAS`), the `for (const tool of TOOLS)` loop near line 1435, `LLM_DETAILED_OUTPUT_TOOLS`, `envelopedToolOutputSchema`, `groupedInputSchema`, `GROUPED_TOOLS`, `GROUPED_ACTIONS` — i.e. everything from the `Tool` interface down to just before `isGroupedMode`, if it has no dependency that would create a load-time cycle.
- Keep every top-level statement in the same relative order (module-load mutations such as `TOOLS.push`, `applyTickerCaps(TOOLS)` and the output-schema loop must still run before anything reads `TOOLS`).
- If a declaration in that block reads something that lives in tools.ts below it (or that would create an import cycle evaluated at load time), leave that declaration in tools.ts and say so in the report. Do not reorder statements to make a move fit.
- `tools.ts` imports what it needs from `schemas.ts` and re-exports `Tool` (type) and `TOOLS`, so `index.ts`, `mcp.ts`, `telemetry.ts` and the parity bundle keep their imports. Do not edit those importers.
- Whether `listVisibleTools`, `isGroupedMode`, `currentToolMode` move is your call; prefer leaving them in tools.ts unless they move cleanly.

## Part B: table router in `worker/src/tools.ts`
Replace the `switch (name)` in `_dispatchTool` with a lookup table:
- `type ToolHandler = (args: Record<string, unknown>) => Promise<string> | string;`
- `const TOOL_HANDLERS: ReadonlyMap<string, ToolHandler> = new Map([...])` with one entry per `case`, in the same order as the cases. Use a `Map` (not an object literal) so names like `constructor` or `__proto__` are not found on the prototype.
- Each entry's body is the case body verbatim: `case "x": return f(...)` → `["x", (args) => f(...)]`; `case "x": { ... return ... }` → `["x", async (args) => { ... }]` (async only where the body uses `await`, otherwise a plain arrow). Cases that share a body (`case "a": case "b":`) get separate entries pointing at one shared function. If any case falls through or uses `break` without returning, stop and report it instead of guessing.
- `_dispatchTool` keeps its pre-checks exactly as they are (gate, filing period, latest-only period), then: `const handler = TOOL_HANDLERS.get(name); if (!handler) throw new Error(\`Unknown tool: ${name}\`); return handler(args);`
- The table may live in tools.ts or a new `worker/src/router.ts`; choose tools.ts if a separate file would need to import back from tools.ts (cycle).

## Do not touch
`analysis/yahoo-finance-mcp/extract_topology.py`, `topology.json`, version files, docs, any Python file. The planner does topology, version and docs.

## Verification (report each)
1. `cd worker && npx tsc --noEmit -p .` passes.
2. `listVisibleTools()` output is byte-identical before/after in both expanded and grouped modes (JSON.stringify), and the exported names of `tools.ts` are identical (plus nothing removed).
3. The set of case names equals the set of Map keys, and the order matches.
4. Line multiset: moved text is identical apart from import/export lines, the Map wrapper syntax and the indentation change of case bodies (compare with whitespace stripped).
5. `wrangler deploy --dry-run --outdir <tmp>` bundles; report bundle size before/after.
6. Each of `schemas.ts`, `tools.ts`, `index.ts` loads with esbuild as the only entry without load-time errors (as in unit 7).
7. Lizard: `pip install lizard` into a throwaway venv under your worktree (not the scratchpad root) and run `lizard -l typescript -C 100 -w worker/src` and `lizard -l python -C 100 -w server.py yfmcp` on the result. Report every function with CCN >= 100 (before and after for `_dispatchTool`). Do not refactor other functions to meet the threshold; just report.
8. The Worker parity steps from `.github/workflows/ci.yml` that exercise the Worker (`npm run test:*` steps and the parity harness `scripts/parity/*` run with `NODE_OPTIONS=--max-old-space-size=8192`) pass with no expected value changed. Write logs under your worktree, never in the scratchpad root.

## Commit
One commit on your worktree branch, not pushed, message starting "Phase 5 unit 8 (Worker): table router and schemas.ts". End the message with:
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01LPCh3i5wx667vv735nuFmE
Never `git add -A`; add files by path. Do not commit node_modules or a node_modules symlink.

## Outcome (3.6.8)
- `worker/src/schemas.ts` (1,419 lines) holds everything from `interface Tool` through `GROUPED_ACTIONS`, in the same order, including the module-load mutations (`TOOLS.push`, `applyTickerCaps(TOOLS)`, the output-schema loop). No import cycle. `isGroupedMode`, `currentToolMode`, `listVisibleTools` and `ENVELOPE_SCHEMA_VERSION` stay in `tools.ts` (they read Worker vars). `tools.ts` re-exports `TOOLS` and `type Tool`.
- `_dispatchTool` keeps its pre-checks and then looks the name up in `TOOL_HANDLERS` (a `Map` of 97 entries in the old case order; no fallthrough, `break` or shared bodies existed). Unknown names still throw `Unknown tool: <name>`. `tools.ts` 2,748 → 1,305 lines.
- `_dispatchTool` CCN 146 → 9. Functions still at CCN >= 100 (lizard): Worker `extractGeographicRevenue` (111), `querySecFilingIndex` (125); Python `_get_filing_data_unchecked` (181), `search_filing_text` (117), `_compute_source_status` (124), `query_sec_filing_index` (119), `_warrants_component` (109), `dilution_bridge` (152), `capital_structure` (139), `valuation_snapshot` (134). The Phase 5 CCN exit criterion is therefore not met by moves alone; splitting these is a later refactor.
- Tests that read `tools.ts` source for `case "<tool>":` now look for the table entry `["<tool>", `; no assertion changed in meaning.
- `extract_topology.py` reads the dispatch arms from `TOOL_HANDLERS` and registers `schemas.ts` as transport; dispatch edges are unchanged.
- Verification: `listVisibleTools()` byte-identical in expanded/grouped × envelope v2 on/off; `JSON.stringify(TOOLS)` identical; 97 case names = 97 keys in order; bundle 1,457,666 → 1,456,872 bytes.
