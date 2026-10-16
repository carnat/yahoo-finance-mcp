# Domain extraction playbook (from the Phase 3 news pilot, 2026-10-16)

This is how one domain moves out of the two god files (`server.py`, `worker/src/yahoo-finance.ts`) with byte-identical output. The news pilot (release 2.8.1) proved it. Phases 4 and 5 reuse it, one domain at a time.

## 0. Gates before starting

1. **Harness coverage.** Run a Q7 rule extraction for the domain (`/modernize-extract-rules` with a `modulePattern`). Every P0 and P1 rule must have an execute-both case, with Python/Worker divergences pinned as `FINDING`. A pure move cannot fix those divergences; the harness only guarantees they stay put.
2. **Repairs first.** Any repair release scheduled for the domain must be released and live-verified before the move.
3. **Live baseline.** Capture structure and coverage of the domain's tools on the deployed Worker, as in `phase3_live_before.json`. Record key sets, statuses, coverage and warning codes, not item contents.

## 1. Scope comes from the topology, not from judgment

- Dump `func_region` from `extract_topology.py` for `<god file>#<domain>` and treat that list as the scope (`phase3_news_region.json`).
- Move every listed function, plus the constants and types that only those functions use.
- Generic helpers that the topology assigns to the domain move with it. The hub phase relocates them later.
- Shared helpers the topology puts in `#shared` stay where they are and are imported.
- **Exit check:** regenerate the topology and confirm the region shows 0 NLOC.
- Register the new file in `FILE_DOMAIN`, or the topology reports it as "no domain".

## 2. Python mechanics

| What | How |
|---|---|
| Destination | `yfmcp/tools/<domain>.py` (precedent: pricing `d905cd1`, earnings `d897da5`, news) |
| Imports | Import dependencies from `yfmcp.*` under the same aliases `server.py` uses. Do not import `server`; check that no cycle results. |
| Wiring in `server.py` | A side-effect import, plus a re-export block holding only names read as `server.<name>` by `server.py`, routing, scripts or tests |
| Patched constants | Do not re-export a constant that tests patch. A stale copy in `server` makes `patch("server.X")` a silent no-op. |
| Shared module constants | Move them to a shared `yfmcp` module, as `_PROVIDER_USER_AGENT` went to `yfmcp/clients/market_providers.py`. |
| Registration order | The `tools/list` order is the registration order. If the domain's tools were contiguous, place the import before the next remaining registration. A tool that sat between other tools gets `reseat_tool_registration(server, name, after=...)` at its old position. It uses FastMCP internals and fails loudly if they change; R4 (Q8) replaces it with a public API. |
| Test plumbing | Patches must target `yfmcp.tools.<domain>.<name>` for names the moved code looks up. Change targets only, never assertions. Source-text tests read the god file plus the new module. |
| Identity proof | Compare `ast.dump` of each moved function with `git archive` of the base commit. Diff the expanded and grouped `list_tools` (names, order, descriptions, schemas). Compare the `health_check` `manifestHash` and `schemaHash`. |

## 3. Worker mechanics

| What | How |
|---|---|
| Destination | `worker/src/<domain>.ts` |
| Re-exports | `yahoo-finance.ts` re-exports every previously exported moved symbol (`export { … } from "./<domain>.js"`), so `tools.ts`, the parity bundle and other importers stay unchanged. |
| Imports back | Import back the moved helpers that remaining code still calls. Prefer leaf modules (`cache.ts`, `request-context.ts`, `response.ts`, `extraction-rules.ts`, …) over `yahoo-finance.ts`. |
| Import cycle | A `yahoo-finance.ts` ↔ `<domain>.ts` cycle is safe if no top-level declaration reads the other module's bindings. Scan for this, then evaluate each module as the sole bundle entry in Node. |
| Identity proof | The moved text keeps its line multiset, with only `export` keywords added. Diff the `listVisibleTools()` manifest in expanded and grouped modes. Compare the `wrangler deploy --dry-run` bundle size (news: +16 bytes). |
| Test plumbing | Source-text tests read `yahoo-finance.ts` plus the new module. A slice-and-run test appends the moved helper's text. |

## 4. Execution shape that worked

- **Planner:** one spec (`PHASE3_SPEC.md`) with the scope list, rule zero ("byte-identical, fix nothing, report what you notice") and a verification list.
- **Two agents in separate worktrees,** one per runtime, run in parallel. Each must keep the full harness green on its own, so the halves are independent. The Worker half took about 20 minutes and the Python half about 38 minutes on a mid-size model.
- **Merge:** merge both branches, then run the harness, tool sync, grouped parity and tsc. Regenerate the topology once after both halves land, not per half.
- **Imports record:** `PHASE3_IMPORTS_PY.md` and `PHASE3_IMPORTS_TS.md`. Imports from the god file into the new module are the hub candidates for Phase 4.

## 5. Costs and lessons

- **Source-text tests are the main friction.** Fourteen test scripts read god-file text. Move them to behavioural checks, or to a helper that reads all domain modules, before the next extraction.
- **Patch targets are the second.** Cases that patch `server.X` silently stop working after a move. The harness refuses network access, so a missed patch shows up as a failure, not as a live call.
- **Topology drift.** Commit the regenerated `topology.json` only after both halves land, and expect unrelated LOC drift from earlier releases.
- **Bookkeeping overhead.** Per-release overhead (version, docs, CI, PR, live verification) is about the same as for a behaviour release. Q9 batching after the pilot applies: group small domains into one move release.
- **Python noticed-but-not-fixed items:**
  - unused imports left in `server.py`;
  - three dead news constants (moved);
  - an unused local in `get_company_news`.
- **Worker noticed-but-not-fixed items:**
  - `resolveNewsCompanyIdentity` and `finnhubEligibility` stay in `#shared`, though they are news-specific;
  - non-news code still calls `getCompanyNews` (`extractAnalystValuationMethods`).
