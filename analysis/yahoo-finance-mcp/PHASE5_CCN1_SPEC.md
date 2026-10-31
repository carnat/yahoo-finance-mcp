# Phase 5 exit, CCN part 1 (3.6.9): SEC filing index and geographic revenue

Phase 5 exit criterion: "No function has CCN >= 100 (lizard)". After 3.6.8 ten functions remain. This release splits three of them:
- Worker `querySecFilingIndex` (worker/src/sec.ts, CCN 125)
- Python `query_sec_filing_index` (yfmcp/tools/exposure.py, CCN 119) — the Python counterpart; split it along the same seams as the Worker so the two stay readable side by side
- Worker `extractGeographicRevenue` (worker/src/exposure.ts, CCN 111)

No behaviour change: same outputs, statuses, warnings, messages, key order in returned objects, evaluation order of upstream fetches, cache keys and error paths.

## How to split
- Extract-function only. Move contiguous blocks (a phase of the function: argument normalisation, candidate selection, a per-row classifier, the payload builder) into private helpers in the same file, placed directly above the function. Name helpers for what they compute.
- Keep object key insertion order in returned payloads exactly as before (JSON output order matters for parity and hashes). If a helper builds part of an object, the caller must still insert keys in the original order.
- Early returns inside a moved block: return a sentinel (e.g. `null` / `None` meaning "continue") and keep the original return in the caller, or move the whole tail. Do not change which branch wins.
- Do not touch exceptions/`try` scopes in a way that changes what is caught: if a block sits inside a `try`, the helper is called inside the same `try`.
- Async: a helper that awaits is `async` and is awaited at the same point; do not reorder awaits or turn sequential awaits into parallel ones.
- No new exports, no changes outside these three functions' files except tests if a test greps source text.
- Target: each of the three functions and every new helper CCN < 60 (lizard), and no new function >= 100.

## Verification (report each)
1. `cd worker && npx tsc --noEmit -p .` passes.
2. lizard (throwaway venv inside your worktree: `pip install lizard`): `lizard -l typescript -w -C 100 worker/src` and `lizard -l python -w -C 100 server.py yfmcp` — report the before/after CCN of the three functions and the max CCN of the new helpers; confirm the remaining CCN >= 100 list is exactly the other 7 (Worker none; Python `_get_filing_data_unchecked`, `search_filing_text`, `_compute_source_status`, `_warrants_component`, `dilution_bridge`, `capital_structure`, `valuation_snapshot`).
3. All `run:` steps of `.github/workflows/ci.yml` (jobs check-tool-sync, type-check-worker, acceptance-suite) pass with `PATH=/home/user/yahoo-finance-mcp/.venv/bin:$PATH` and `NODE_OPTIONS=--max-old-space-size=8192`, with no expected value changed. `scripts/test_universal_aliases.py` needs MCP_TOKEN and may fail for that reason only. Pay particular attention to the parity harness cases for `get_sec_filing_index`, `extract_geographic_revenue`, `extract_china_exposure`, `extract_revenue_exposure` and `extract_exposure`.
4. Python `list_tools` (expanded and grouped) and Worker `listVisibleTools()` unchanged (they should be: no schema touched).
5. Report the diff size per file.

## Commit
Two commits on your worktree branch, not pushed: "CCN part 1 (Worker): split querySecFilingIndex and extractGeographicRevenue" and "CCN part 1 (Python): split query_sec_filing_index". End each message with:
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01LPCh3i5wx667vv735nuFmE
Never `git add -A`; add files by path. Never commit node_modules or a symlink to it. Do not touch analysis/, docs/, version files.

## Outcome (3.6.9)
- Worker `querySecFilingIndex` CCN 125 → 18: one helper per query type (7) plus `secIndexEvidenceRequired`, sharing a `SecIndexContext` that carries the original closures; the function keeps validation and dispatch. Max helper CCN 19.
- Worker `extractGeographicRevenue` CCN 111 → 23: `geographicEvidenceBlock`, `buildGeographicRevenueOutput`, `applyGeographic20FFallback`, `maybeRetryGeographicRevenueAs20F`. The `warnings` array is still shared by reference with `out.warnings`, so the late POSSIBLE_20F_FILER push lands where it did. Max helper CCN 30.
- Python `query_sec_filing_index` CCN 119 → 16 along the same seams (`_SecIndexContext`, `_sec_index_*`). Max helper CCN 24.
- Remaining CCN >= 100: Worker none; Python `_get_filing_data_unchecked` 181, `search_filing_text` 117, `_compute_source_status` 124, `_warrants_component` 109, `dilution_bridge` 152, `capital_structure` 139, `valuation_snapshot` 134.
- All CI steps pass with no expected value changed.
