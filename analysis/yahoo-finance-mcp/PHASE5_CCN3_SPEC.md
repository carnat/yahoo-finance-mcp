# Phase 5 exit, CCN part 3 (3.6.11): Python SEC filing data, filing text search, news source status

Phase 5 exit criterion: "No function has CCN >= 100 (lizard)". This release splits the last three Python functions over the threshold (their Worker counterparts are already under 100):
- `_get_filing_data_unchecked` (yfmcp/tools/sec.py, CCN 181)
- `search_filing_text` (yfmcp/tools/sec.py, CCN 117)
- `_compute_source_status` (yfmcp/tools/news.py, CCN 124)
Where the Worker counterpart (worker/src/sec.ts `getFilingData`/`searchFilingText`, worker/src/news.ts source status) already has helpers for the same phases, mirror their seams and names (snake_case) so the runtimes read alike; do not edit the Worker.

No behaviour change: same outputs, statuses, warnings, messages, key order in returned dicts, rounding, evaluation and fetch order, cache keys and error paths.

## How to split
- Extract-function only. Move contiguous blocks (a phase of the function: argument normalisation, candidate selection, a per-row classifier, the payload builder) into private helpers in the same file, placed directly above the function. Name helpers for what they compute.
- Keep object key insertion order in returned payloads exactly as before (JSON output order matters for parity and hashes). If a helper builds part of an object, the caller must still insert keys in the original order.
- Early returns inside a moved block: return a sentinel (e.g. `null` / `None` meaning "continue") and keep the original return in the caller, or move the whole tail. Do not change which branch wins.
- Do not touch exceptions/`try` scopes in a way that changes what is caught: if a block sits inside a `try`, the helper is called inside the same `try`.
- Async: a helper that awaits is `async` and is awaited at the same point; do not reorder awaits or turn sequential awaits into parallel ones.
- No changes outside these two files except tests if a test greps source text. Helpers are module-private (leading underscore).
- Target: each of the three functions and every new helper CCN < 60 (lizard), and no new function >= 100.

## Verification (report each)
1. `cd worker && npx tsc --noEmit -p .` passes.
2. lizard (throwaway venv inside your worktree: `pip install lizard`): `lizard -l typescript -w -C 100 worker/src` and `lizard -l python -w -C 100 server.py yfmcp` — report the before/after CCN of the three functions and the max CCN of the new helpers; the three must drop off the CCN >= 100 list and nothing new may appear on it.
3. All `run:` steps of `.github/workflows/ci.yml` (jobs check-tool-sync, type-check-worker, acceptance-suite) pass with `PATH=/home/user/yahoo-finance-mcp/.venv/bin:$PATH` and `NODE_OPTIONS=--max-old-space-size=8192`, with no expected value changed. `scripts/test_universal_aliases.py` needs MCP_TOKEN and may fail for that reason only. Pay particular attention to the SEC filing-data, filing-text-search and news parity cases, scripts/test_sec_fact_payloads.py, test_news_events.py and any test that patches names inside yfmcp.tools.sec or yfmcp.tools.news (patches must still reach the code: if a block moves into a helper, a patched name it calls must still be looked up through the module global, not captured).
4. Python `list_tools` (expanded and grouped) unchanged.
5. Report the diff size per file.

## Commit
One commit on your worktree branch, not pushed: "CCN part 3 (Python): split filing data, filing text search and news source status". End each message with:
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01LPCh3i5wx667vv735nuFmE
Never `git add -A`; add files by path. Never commit node_modules or a symlink to it. Do not touch analysis/, docs/, version files.

## Outcome (3.6.11)
- `_get_filing_data_unchecked` 181 → 29: the nested closures and each phase moved to `_fd_*` helpers sharing a `_FilingDataState`. The original closures that read `accession_number` are all called before the function reassigns it, so reading the caller's value from the state object is equivalent. Patched names (`_edgar_read_json`, `_resolve_sec_filing`, `_edgar_get_html`) are still looked up as module globals.
- `search_filing_text` 117 → 20: `_sft_*` helpers with a `_FilingSearchRun` accumulator; early returns come back as a string the caller returns.
- `_compute_source_status` 124 → 45: one helper per source; the caller still sets each key in the original order.
- Max helper CCN 32 (`_fd_pick_fact`). Python `list_tools` hashes unchanged; all CI steps pass with no expected value changed.
- No function in either runtime is at CCN >= 100 now (lizard), which closes the last Phase 5 exit criterion.
