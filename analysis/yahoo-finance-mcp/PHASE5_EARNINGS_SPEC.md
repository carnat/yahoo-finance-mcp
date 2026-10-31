# Phase 5 unit 6: move earnings out of the god files and remove the `_server_attr` cycle (3.6.6)

Follow `analysis/yahoo-finance-mcp/PLAYBOOK.md` §1–§3. Precedents: Phase 5 units 1–5 (`PHASE5_*_SPEC.md`).

## Rule zero
No behaviour change. Output is byte-identical: no text, key order, number format, warning, status, schema, cache key or edge-cache URL may change. The parity harness passes with no expected value edited.

## Scope (topology `func_region`, regenerated 2026-10-31 at 3.6.5, a82f2d2)
`analysis/yahoo-finance-mcp/phase5_earnings_region.json`:
- `server.py#earnings` (1): `get_guidance_history`.
- `worker/src/yahoo-finance.ts#earnings` (57): latest earnings release and its SEC source, release index, metrics, guidance and management commentary, actual vs estimate, guidance history, the transcript chain (Yahoo/Quartr, Alpha Vantage, public pages, SEC candidates), release-text and iXBRL helpers, `earningsReleasePicks` and `readReleaseText`.

## Python
1. Move `get_guidance_history` (plus constants only it uses) byte-identical into `yfmcp/tools/earnings.py`; keep `tools/list` order with `after=` if needed; re-export from `server.py`.
2. Remove the `_server_attr` back-calls. `yfmcp/tools/earnings.py` has five: `_build_filing_index_from_html`, `_edgar_read_json`, `_resolve_cik_for_ticker`, `get_calendar`, `get_earnings_analysis`. Each now lives in a module (`yfmcp.tools.sec`, `yfmcp.clients.edgar` or wherever `server.py` imports it from, `yfmcp.tools.fundamentals`). Replace each `_server_attr("x")` with a reference to the defining module that still binds late, e.g. `import yfmcp.tools.fundamentals as _fundamentals` at the top and `_fundamentals.get_calendar`, so the object reached is the same one `server` re-exports and a test patch on the defining module reaches it. Then delete `_server_attr`. `server.py` imports `yfmcp.tools.earnings` after `fundamentals`, `sec` and `exposure`, so importing those at the top of `earnings.py` must not change registration order; verify. These call-site edits are the only non-identical code; `ast.dump` identity holds for everything else, and for the edited functions after normalising the five call expressions.
3. Tests that patch `server.<name>` to reach earnings code through `_server_attr` (for example `get_calendar`, `get_earnings_analysis`, `_resolve_cik_for_ticker`) now patch the defining module. Check every such patch still reaches the code it was written for; never change an assertion.
4. `_earnings_release_picks` and `_read_release_text` (in `yfmcp/tools/exposure.py` since 3.6.2, domain earnings) move to `earnings.py` only if that creates no import cycle (`exposure.py` would then import `earnings.py`, which imports `fundamentals`/`sec`; check `earnings.py` never needs `exposure.py`). Otherwise leave them and report.

## Worker
Move the 57 functions plus constants and types used only by them into a new `worker/src/earnings.ts`, text-identical except added `export`. `yahoo-finance.ts` re-exports every previously exported moved symbol; `exposure.ts` (and any other importer) switches `earningsReleasePicks` / `readReleaseText` to `./earnings.js`. Check no load-time reads across any import cycle.

## Topology
Register `worker/src/earnings.ts` as `"earnings"` in `FILE_DOMAIN` (check `yfmcp/tools/earnings.py` is registered), remove `DOMAIN_OVERRIDES` entries whose functions moved (`earningsReleasePicks`, `readReleaseText`). The `_server_attr` observation in the topology output should disappear.

## Verification
As in units 1–5, plus: `grep -rn _server_attr yfmcp server.py` returns nothing.

## Outcome (3.6.6)
- Python: `get_guidance_history` moved into `yfmcp/tools/earnings.py` (`after="extract_operating_driver_ledger"`). The five `_server_attr` back-calls now read `_fundamentals.get_calendar`, `_fundamentals.get_earnings_analysis`, `_sec._build_filing_index_from_html`, `_edgar_client._edgar_read_json` and `_edgar_client._resolve_cik_for_ticker`; `_server_attr` is gone. Seven test patches that reached earnings code only through `server` (in `test_earnings_tools.py`, `test_data_quality.py`, `cases_sec_earnings.py`) now patch `yfmcp.tools.fundamentals`; before the retarget they failed, which confirms they had been reaching the code through the back-call.
- `_earnings_release_picks` and `_read_release_text` stay in `yfmcp/tools/exposure.py`: it is imported before `earnings.py`, so a top-level import of `earnings.py` there would register the earnings tools earlier and reorder `tools/list`. `earnings.py` imports them from `exposure.py`.
- Worker: all 57 functions moved to `worker/src/earnings.ts`, with 15 constants and types; no `export` added. `exposure.ts` imports the release readers from it.
- `extract_topology.py`: the `_server_attr` observation prints only while back-calls exist; the scan stays as a guard.
- `server.py` 1,014 → 972 lines (under the Phase 5 exit target of 1,000); `yahoo-finance.ts` 3,062 → 630. Left: valuation (Python 9, Worker 7), Python evidence (7) and transport (2).
