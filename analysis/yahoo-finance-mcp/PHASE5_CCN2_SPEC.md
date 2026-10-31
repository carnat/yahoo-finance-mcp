# Phase 5 exit, CCN part 2 (3.6.10): Python capital structure and valuation snapshot

Phase 5 exit criterion: "No function has CCN >= 100 (lizard)". This release splits four Python functions (their Worker counterparts are already under 100):
- `_warrants_component` (yfmcp/capital_structure.py, CCN 109)
- `dilution_bridge` (yfmcp/capital_structure.py, CCN 152)
- `capital_structure` (yfmcp/capital_structure.py, CCN 139)
- `valuation_snapshot` (yfmcp/valuation.py, CCN 134)
Where the Worker counterpart (worker/src/capital-structure.ts, worker/src/valuation.ts) already has helpers for the same phases, mirror their seams and names (snake_case) so the runtimes read alike; do not edit the Worker.

No behaviour change: same outputs, statuses, warnings, messages, key order in returned dicts, rounding, evaluation order and error paths.

## How to split
- Extract-function only. Move contiguous blocks (a phase of the function: argument normalisation, candidate selection, a per-row classifier, the payload builder) into private helpers in the same file, placed directly above the function. Name helpers for what they compute.
- Keep object key insertion order in returned payloads exactly as before (JSON output order matters for parity and hashes). If a helper builds part of an object, the caller must still insert keys in the original order.
- Early returns inside a moved block: return a sentinel (e.g. `null` / `None` meaning "continue") and keep the original return in the caller, or move the whole tail. Do not change which branch wins.
- Do not touch exceptions/`try` scopes in a way that changes what is caught: if a block sits inside a `try`, the helper is called inside the same `try`.
- Async: a helper that awaits is `async` and is awaited at the same point; do not reorder awaits or turn sequential awaits into parallel ones.
- No changes outside these two files except tests if a test greps source text. Helpers are module-private (leading underscore).
- Target: each of the four functions and every new helper CCN < 60 (lizard), and no new function >= 100.

## Verification (report each)
1. `cd worker && npx tsc --noEmit -p .` passes.
2. lizard (throwaway venv inside your worktree: `pip install lizard`): `lizard -l typescript -w -C 100 worker/src` and `lizard -l python -w -C 100 server.py yfmcp` — report the before/after CCN of the four functions and the max CCN of the new helpers; the four must drop off the CCN >= 100 list and nothing new may appear on it.
3. All `run:` steps of `.github/workflows/ci.yml` (jobs check-tool-sync, type-check-worker, acceptance-suite) pass with `PATH=/home/user/yahoo-finance-mcp/.venv/bin:$PATH` and `NODE_OPTIONS=--max-old-space-size=8192`, with no expected value changed. `scripts/test_universal_aliases.py` needs MCP_TOKEN and may fail for that reason only. Pay particular attention to scripts/test_capital_structure.py, test_valuation.py, the dilution/capital-structure/valuation parity cases and test_input_validation.py.
4. Python `list_tools` (expanded and grouped) unchanged.
5. Report the diff size per file.

## Commit
One commit on your worktree branch, not pushed: "CCN part 2 (Python): split capital structure, dilution bridge, warrants and valuation snapshot". End each message with:
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01LPCh3i5wx667vv735nuFmE
Never `git add -A`; add files by path. Never commit node_modules or a symlink to it. Do not touch analysis/, docs/, version files.

## Outcome (3.6.10)
- `_warrants_component` 109 → 40, `dilution_bridge` 152 → 32, `capital_structure` 139 → 34 (yfmcp/capital_structure.py; 13 helpers, largest `_warrant_class_row` 45, `_bridge_instrument_warnings` 43).
- `valuation_snapshot` 134 → 53 (yfmcp/valuation.py; 7 helpers, largest `_resolve_balances` 21). Warning append order and returned key order unchanged.
- Python `list_tools` (expanded 97, grouped 12) hashes unchanged; all CI steps pass with no expected value changed.
- Remaining CCN >= 100: Python `_get_filing_data_unchecked` 181, `search_filing_text` 117, `_compute_source_status` 124 (part 3).
