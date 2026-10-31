# Phase 6: shared data dir (3.6.12)

Brief, Phase 6 scope: "Shared data dir (Python stops reading `worker/src`)". Exit criterion: "Python reads nothing under `worker/src`".

Today the Python runtime reads five JSON registries from `worker/src/` at run time. `pyproject.toml` packages only `yfmcp*` and excludes `worker*`, so an installed wheel cannot find them. Move them to `yfmcp/data/` (shipped as package data) and have the Worker import them from there.

Files (git mv, content unchanged byte for byte):
- worker/src/company-ir-page-registry.json → yfmcp/data/company-ir-page-registry.json
- worker/src/exchange-calendars.json → yfmcp/data/exchange-calendars.json
- worker/src/exposure-presets.json → yfmcp/data/exposure-presets.json
- worker/src/news-source-capabilities.json → yfmcp/data/news-source-capabilities.json
- worker/src/news-source-registry.json → yfmcp/data/news-source-registry.json

## Changes
1. Worker: every `import ... from "./x.json"` (or `../x.json` etc.) of these files points at the new path (relative, e.g. `../../yfmcp/data/x.json` from worker/src). Nothing else in the Worker changes. Confirm `npx tsc --noEmit -p .` passes and `wrangler deploy --dry-run --outdir <tmp>` bundles, and that the bundle size is unchanged or nearly so (report before/after).
2. Python: every reader (yfmcp/exposure_presets.py, yfmcp/url_policy.py, yfmcp/news/providers/company_ir_page.py, yfmcp/tools/news.py, yfmcp/tools/provider_gaps.py, yfmcp/exchange_calendars.py, and any other found by grep) reads from `yfmcp/data/` via a path relative to its own package (`Path(__file__).resolve().parent...` to `yfmcp/data`), never via `worker`. Add a tiny helper `yfmcp/data/__init__.py` exposing `DATA_DIR` (a `Path`) and use it everywhere, so there is one definition.
3. Packaging: `pyproject.toml` gets `[tool.setuptools.package-data] yfmcp = ["data/*.json"]` so the files ship in the wheel. Verify by building a wheel into a temp dir inside your worktree (`.venv/bin/python -m pip wheel . --no-deps -w <tmp>` or `python -m build` if available) and listing that the five JSON files are inside it. Do not commit build output.
4. Writers: `.github/workflows/ir-source-discovery.yml` and `scripts/discover_ir_page_candidates.py` write/commit the IR registry — point them at the new path. Check every other workflow under .github/workflows for these paths too.
5. Tests and analysis scripts: update every path in scripts/ (test_exchange_calendars.py, test_ir_page_registry.py, test_news_events.py, parity cases, etc.) and `analysis/yahoo-finance-mcp/extract_topology.py` (its registry table and the observation that Python reads worker/src — that observation should now not fire; if it is unconditional, make it conditional on the reads existing, as was done for the `_server_attr` observation). README.md: update any path it names.
6. Add a guard test `scripts/test_no_worker_src_reads.py` (and add it to `.github/workflows/ci.yml` next to similar Python test steps) that fails if any file under `yfmcp/` or `server.py` contains the string `"worker"` joined with `"src"` as a path (e.g. `/ "worker" / "src"`, `"worker/src"`, `'worker', 'src'`) outside comments/docstrings. Keep it simple: scan source lines that are not comments and not inside docstrings for the regex `worker["'\s/,)]*\s*/?\s*["']?src`; docstring mentions such as "mirrors worker/src/x.ts" are allowed (they are documentation). Make sure it passes now and would have failed before.
7. Do not change any output. Do not touch version files, docs/provider-runtime-guidance.md or topology.json (the planner does those).

## Verification (report each)
1. `grep -rn "worker" yfmcp server.py` shows no runtime path into worker/src (only docstrings/comments).
2. The five files are byte-identical to before (`git diff -M --stat` shows pure renames, 100% similarity).
3. tsc passes; wrangler dry-run bundles; size before/after.
4. Wheel contains `yfmcp/data/*.json` (list them).
5. All `run:` steps of `.github/workflows/ci.yml` (jobs check-tool-sync, type-check-worker, acceptance-suite, including the new guard test) pass with `PATH=/home/user/yahoo-finance-mcp/.venv/bin:$PATH` and `NODE_OPTIONS=--max-old-space-size=8192`, no expected value changed. test_universal_aliases.py may fail only for missing MCP_TOKEN.
6. `PATH=/home/user/yahoo-finance-mcp/.venv/bin:$PATH python analysis/yahoo-finance-mcp/extract_topology.py` runs without raising (then `git checkout analysis/yahoo-finance-mcp/topology.json` so it is not committed) and reports Unresolved (0).

## Commit
One commit on your worktree branch, not pushed: "Phase 6: move the shared JSON registries to yfmcp/data". End the message with:
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01LPCh3i5wx667vv735nuFmE
Never `git add -A`; add files by path (git mv is fine). Never commit node_modules or a symlink to it.

## Outcome (3.6.12)
- The five registries moved to `yfmcp/data/` as pure renames; `yfmcp/data/__init__.py` defines `DATA_DIR` and every Python reader uses it. The Worker imports them by relative path; bundle +50 bytes (longer source-path comments).
- `pyproject.toml` ships `yfmcp/data/*.json` as package data; a built wheel contains all five files (before this, an installed wheel could not find them).
- The IR discovery workflow and script write the new path. `scripts/test_no_worker_src_reads.py` (in CI) fails if Python code builds a path into `worker/src`; it flags 7 lines on the 3.6.11 source and none now.
- `extract_topology.py`: `yfmcp/data/__init__.py` is mapped to the envelope domain; the Python-reads-worker/src observation only prints while such a read exists.
- Phase 6 exit criterion "Python reads nothing under `worker/src`" is met.
