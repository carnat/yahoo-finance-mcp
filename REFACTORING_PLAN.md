# Code Refactoring Plan

This document proposes a staged refactoring of the yahoo-finance-mcp codebase. Every phase is
behavior-preserving, independently shippable, and gated by the existing acceptance suite, so the
work can stop or pause after any phase with the repo in a healthy state.

## 1. Current state

| Area | Size | Problem |
|------|------|---------|
| `server.py` | 11,231 lines, 221 functions, 111 MCP tools | Single-file monolith: envelope helpers, validation, caching, EDGAR client, HTML parsing, news collectors, earnings extraction, and all tool handlers in one module |
| `worker/src/yahoo-finance.ts` | 8,781 lines | TypeScript monolith mirroring the Python logic for the Cloudflare Worker deployment |
| `worker/src/tools.ts` | 1,992 lines | Hand-maintained tool manifest + dispatch table, duplicated against `server.py` registrations |
| `scripts/test_*.py` | 21 ad-hoc scripts | Not a pytest suite; each script is a separate CI step with manually maintained ordering in `ci.yml` |
| `scripts/check_tool_sync.py` | regex-based | Parses `server.py` and `tools.ts` with regexes to keep the two tool lists in sync |

### Concrete pain points found in the code

1. **Duplicate definitions inside `server.py`.** `_to_iso_utc` is defined twice with different
   semantics (`server.py:1465` and `server.py:10239`). Because Python resolves globals at call
   time, the second definition silently shadows the first for *all* callers, including the
   news/events code written against the first version. `_utc_now_iso` (`server.py:1461`) and
   `_utc_now_z` (`server.py:10235`) are near-identical duplicates. These are latent bugs waiting
   to happen, and a direct symptom of the single-namespace monolith.
2. **Cross-language duplication.** EDGAR fetching, HTML sanitization/table parsing, geographic
   revenue extraction, news collection, and the response envelope are each implemented twice
   (Python and TypeScript), with only a regex script (`check_tool_sync.py`) guarding name-level
   sync — nothing guards behavioral sync of the shared logic.
3. **Fragile registration coupling.** Grouped mode (`tool_groups.py:263`) resolves tool handlers
   by string name from `server.py`'s `globals()`. Any module split must preserve that contract or
   replace it.
4. **Unused tooling config.** `pyproject.toml` configures black, isort, flake8, mypy (strict),
   and bandit, but CI runs none of them. The bandit `test` list enumerates `B101`–`B999`, most of
   which are not real bandit check IDs — the config is effectively noise.
5. **CI test ordering is hand-maintained.** `ci.yml` lists ~15 individual test-script steps with
   comments explaining the ordering; adding a test means editing the workflow.

## 2. Goals and non-goals

**Goals**

- Split both monoliths into domain modules that mirror the 10 tool groups already defined in
  `tool_groups.py` (pricing, fundamentals, analyst, options, SEC filings, SEC extractors,
  news/events, earnings, screening, system).
- Preserve every public contract: tool names, aliases, response envelopes, `TOOL_MODE=grouped`,
  `import server`, and the `server.py` / Docker / smithery / replit entry points.
- Replace the regex tool-sync check with a single declarative tool manifest.
- Consolidate the test scripts into a pytest suite with one CI step.

**Non-goals**

- No changes to tool behavior, schemas, or the v2 envelope.
- No unification of the Python and TypeScript runtimes (the Worker must stay dependency-light and
  cannot run yfinance/pandas).
- No renaming of tools or removal of deprecated aliases.

## 3. Target layout

### Python (`server.py` → `yfmcp/` package)

```
yfmcp/
  app.py            # FastMCP instance, TOOL_MODE switch, get_server()
  envelope.py       # ErrorCode, ToolMeta, _mcp_success/_mcp_failure/_mcp_warning, _wrap_envelope_v2
  validation.py     # ticker/accession/URL validators, HTML sanitization
  cache.py          # ToolCache, _cache_get/_cache_set, TTLs
  util.py           # retry, date/ISO helpers (single _to_iso_utc), data quality, relevance sort
  clients/
    edgar.py        # _edgar_get*, CIK resolution, filing URL builders, exhibit listing
    yahoo.py        # yfinance access patterns, _safe_parse, batch helpers
  parsing/
    html.py         # strip/sanitize, table parsing, unit detection, markdown fallback
    extractors.py   # geo revenue, segment revenue, XBRL concept extraction
  tools/
    pricing.py      # quotes, history, technicals, short interest, overnight, snapshot
    fundamentals.py # profile, statements, ratios, credit health, holders, actions
    analyst.py      # consensus, recommendations, upgrades, earnings momentum, calendar
    options.py      # chain, expirations, flow, hedging
    sec_filings.py  # list/outline/section/tables/index/intelligence/search
    sec_extractors.py
    news_events.py  # multi-source collectors (Yahoo, SEC, GlobeNewswire, Finnhub), timeline
    earnings.py     # release resolution, metrics, guidance, commentary, transcripts
    screening.py    # search_ticker, screen_stocks, position signals
    system.py       # health_check, manifest diagnostics
    aliases.py      # deprecated alias wrappers
  manifest.py       # canonical tool registry (see Phase 4)
server.py           # thin facade: re-exports everything, keeps `import server` working
```

`server.py` stays as the entry point (`Dockerfile`, `smithery.yaml`, `main.py`, and 14 test
scripts all reference it) but shrinks to imports + re-exports.

### Worker (`worker/src/yahoo-finance.ts` → modules)

```
worker/src/
  clients/yahoo.ts      # crumb auth, yGet, batch helpers
  clients/edgar.ts      # edgarGet*, CIK resolution, filing URLs
  lib/html.ts           # strip/sanitize/table parsing/unit detection
  lib/quality.ts        # computeDataQuality, sortByRelevance, limitTickers
  domains/pricing.ts
  domains/fundamentals.ts
  domains/analyst.ts
  domains/options.ts
  domains/sec-filings.ts
  domains/sec-extractors.ts
  domains/news-events.ts
  domains/earnings.ts
  yahoo-finance.ts      # barrel re-export so tools.ts imports are unchanged
```

## 4. Phased execution

Each phase is one PR. The verification gate after every phase is the existing acceptance suite:

```
python -m py_compile server.py
python -c "import server"
python scripts/check_tool_sync.py
python scripts/test_phase1.py ... (full script list, or `pytest` once Phase 5 lands)
cd worker && npx tsc --noEmit
```

### Phase 0 — Quick wins and safety net (small)

- Unify the two `_to_iso_utc` definitions into one function covering both input domains
  (epoch numbers, 8/14-digit compact timestamps, date-only strings, ISO strings); delete
  `_utc_now_z` in favor of `_utc_now_iso`. Add a focused regression test for the merged function.
- Fix the bandit configuration (replace the fictional `B101–B999` list with defaults or a real
  skip list) or delete it.
- Add `.git-blame-ignore-revs` and document it, so the mechanical-move commits in later phases
  don't destroy `git blame`.

### Phase 1 — Extract Python infrastructure (medium)

Move envelope, validation, cache, util, `clients/`, and `parsing/` out of `server.py` into the
`yfmcp` package. Pure mechanical moves; `server.py` imports them back into its namespace
(`from yfmcp.envelope import *`-style with explicit names) so `globals()`-based grouped
registration and any test that reaches into `server._mcp_success` keep working.

### Phase 2 — Split Python tool domains (large, mechanical)

- Move the FastMCP instance to `yfmcp/app.py`; each `yfmcp/tools/*.py` module imports it and
  registers its handlers with the same decorators.
- `server.py` becomes a facade: import every domain module (registration happens on import),
  re-export all handler functions by name.
- Replace the `globals()` lookup in `tool_groups.py` with an explicit handler registry that each
  domain module contributes to (`yfmcp/manifest.py` collects `{tool_name: handler}`), keeping the
  `module_globals` parameter as a deprecated fallback for one release.
- Update `scripts/check_tool_sync.py` to scan `yfmcp/tools/*.py` in addition to `server.py`.

Suggested sub-PR ordering (each independently green): pricing + fundamentals + analyst →
options + screening + system → SEC filings + extractors → news/events + earnings → aliases.

### Phase 3 — Split the Worker monolith (large, mechanical)

Same approach as Phase 2 on the TypeScript side. `yahoo-finance.ts` becomes a barrel re-export,
so `tools.ts`, `mcp.ts`, and `index.ts` need no changes. `tsc --noEmit` plus the deployed smoke
scripts (`scripts/test_deployed_*.py`) are the gate.

### Phase 4 — Single source of truth for the tool manifest (medium)

- Add `tools-manifest.json`: for each tool, its canonical name, aliases, group, summary, and
  which runtimes implement it.
- Python reads it at import time to validate registrations; the Worker imports it (JSON import)
  to build the `TOOLS` array entries' names/aliases.
- `check_tool_sync.py` shrinks to "every manifest entry has a registration in each runtime, and
  nothing is registered that isn't in the manifest" — no more regex scraping of source files.
- `tool_groups.py` group definitions derive from the manifest's `group` field, removing the
  third hand-maintained copy of the tool list.

### Phase 5 — Test suite consolidation (medium)

- Move `scripts/test_*.py` to `tests/`, convert to pytest (most already use plain asserts in
  `test_*` functions, so conversion is mostly renames plus fixtures for shared setup).
- Replace the ~15 individual CI steps with one `pytest` step; keep `check_tool_sync.py` and the
  workflow SDK version guard as separate fast-fail steps. Mark network-dependent tests
  (`test_live_*`, `test_deployed_*`) with markers excluded by default.
- Wire the already-configured formatters into CI: run `black --check` and `isort --check` (both
  already in pre-commit). Defer mypy: the strict config in `pyproject.toml` is aspirational for a
  codebase this size — enable it per-module starting with the new `yfmcp/` infrastructure
  modules (`envelope`, `validation`, `cache`, `util`), which are small and typed already.

## 5. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| `check_tool_sync.py` regexes break when decorators move out of `server.py` | Update the script in the same PR as Phase 2; CI fails loudly if missed |
| Grouped mode (`TOOL_MODE=grouped`) silently loses handlers after the split | `test_universal_aliases.py` + an added unit test asserting the registry covers all 111 tools in both modes |
| Tests reach into `server.<private helper>` | Facade re-exports keep every name importable from `server`; grep for `server\.` usage in `scripts/` before each move |
| Worker bundle/deploy regression from module split | esbuild/wrangler handles multi-module fine; deployed smoke tests (`reconciliation.yml`) verify post-deploy |
| Review burden of huge mechanical PRs | Pure-move commits separated from any edit commits; `.git-blame-ignore-revs`; sub-PR ordering in Phase 2/3 |
| Behavioral drift between Python and TS during the split | No logic edits during moves; Phase 0/4 are the only phases that change code paths, and both are narrowly scoped |

## 6. Explicitly out of scope (possible follow-ups)

- Generating tool input schemas from the manifest (would eliminate description drift too).
- Sharing extraction logic between runtimes via generated fixtures/golden tests.
- Replacing black/isort/flake8 with ruff.
- Pruning the `phaseN` test naming in favor of domain names (happens naturally in Phase 5).
