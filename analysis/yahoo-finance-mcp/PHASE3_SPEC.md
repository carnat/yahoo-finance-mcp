# Phase 3 pilot: extract News & public events (pure move)

This spec implements brief §Phase 3. Owner approval for it is recorded in brief §8 ("approve Phase 2, R3, RN and Phase 3").

Every entry gate is met except the last:
- the Phase 2 harness is in place;
- R3 is live-verified;
- the Q7 news rules are in the harness (180/180);
- RN 2.8.0 is merged, with live verification pending.

## Rule zero: output is byte-identical

This is a **move**, not a rewrite.
- No behaviour, text, key order, number formatting, warning, status or schema may change.
- The 1078-test parity harness must pass with no expected value changed.
- The many Python/Worker divergences pinned as `FINDING` cases stay exactly as they are. Do not "fix" anything you notice: list it in the report instead.

## Scope: exactly the functions the topology assigns to `#news`

`analysis/yahoo-finance-mcp/phase3_news_region.json` lists them, with name, start line and NLOC. It is generated from `extract_topology.py` (`func_region`).

| Runtime | Functions | NLOC | Source | Destination |
|---|---|---|---|---|
| Python | 79 | 2,839 | `server.py` (lines 256–8325, scattered) | `yfmcp/tools/news.py` |
| Worker | 97 | 3,361 | `worker/src/yahoo-finance.ts` (lines 382–12333) | `worker/src/news.ts` |

- **Constants and types:** module-level constants, regexes and types used **only** by the moved functions move with them. Ones also used elsewhere stay where they are and are imported.
- **Destination path:** the brief said `yfmcp/news.py`. The repo's precedent is `yfmcp/tools/{pricing,earnings,…}.py` (commits `d905cd1`, `d897da5`), so `yfmcp/tools/news.py` is used.
- **Exit check:** the topology re-run (`python analysis/yahoo-finance-mcp/extract_topology.py`, regenerating `topology.json`) must show **0 NLOC** in `server.py#news` and in `worker/src/yahoo-finance.ts#news`.
- **Generic helpers:** some assigned functions are generic, for example `parseHtmlTable` and `getNews`. They still move, because the topology puts them in `#news` (only news code calls them). Phase 4 can relocate shared helpers.

## Python mechanics

Follow the pricing and earnings precedent.

- **New module:** `yfmcp/tools/news.py` imports its dependencies from where they already live. All shared dependencies are already in `yfmcp.*`:

  | Name | Module |
  |---|---|
  | `_mcp_failure`, `ErrorCode` | `yfmcp.envelope` |
  | `_tool_cache`, `TTL_NEWS` | `yfmcp.cache` |
  | `_validate_ticker` | `yfmcp.validation` |
  | `_edgar_get_html`, `_get_submissions_for_ticker` | `yfmcp.clients.edgar` |
  | `_safe_parse` | `yfmcp.clients.yahoo` |
  | `_run_ticker_batch` | `yfmcp.tools.pricing` |
  | `_parse_html_table`, `_strip_html_tags` | `yfmcp.parsing.html` |
  | `_parse_rss_date`, `_to_iso_utc`, `_utc_now_iso` | `yfmcp.util` |
  | `yfinance_server` | `yfmcp.app` |
  | `extraction_rules` (as `_er`) | `yfmcp` |
  | `_TOOL_OUTPUT_SCHEMAS` | `yfmcp.schemas` |
  | `TOOL_RELIABILITY_CLASS` | `yfmcp.deprecations` |

  Use the stdlib modules (`re`, `html`, `urllib.*`, `xml.etree`, `datetime`, `json`, `os`, `hashlib`, `math`, `functools`, `asyncio`) and `yfinance as yf` under the same aliases `server.py` uses (`_re`, `_html_module`, `_urlrequest`, `_ET`, …).
- **No import cycle:** `yfmcp/tools/news.py` must not import `server`.
- **`_PROVIDER_USER_AGENT`** (`server.py:501`): move it if only news uses it. Otherwise move it to a shared `yfmcp` module and import it from both.
- **`server.py` changes:**
  - Add the side-effect import `import yfmcp.tools.news  # noqa: F401` plus a re-export block (`from yfmcp.tools.news import (...)`). Re-export every moved name that is referenced elsewhere in `server.py`, in `tool_groups.py`, by grouped routing, or by any script or test as `server.<name>`; when in doubt, re-export.
  - Non-news server code calls `get_company_news` and `_safe_sec_url`, so both must be re-exported.
- **Tool registration order:** the tool listing must stay byte-identical. Capture these before the move and compare after:
  - expanded `list_tools` names, order and schemas;
  - grouped mode;
  - the manifest output of `scripts/check_tool_sync.py`.

  The news tools are registered at scattered points in `server.py`. If one import point changes the order, keep the order with explicit registration at the original positions, or whatever the earnings precedent did, and explain the choice.
- **Patch targets in tests and cases:** parity cases and test scripts patch `server.<name>`. After the move, patching `server` no longer affects calls inside `yfmcp.tools.news`. Patches that target names the moved code looks up must point at `yfmcp.tools.news.<name>` instead. Known names:

  | Kind | Names |
  |---|---|
  | Moved functions | `_collect_company_events`, `_collect_company_ir_page_events`, `_collect_finnhub_events`, `_collect_marketaux_events`, `_collect_sec_events`, `_collect_yahoo_events`, `_finnhub_eligibility`, `_load_company_ir_page_registry`, `_yahoo_search_news` |
  | Moved constants | `_COMPANY_IR_PAGE_REGISTRY_PATH` |
  | Dependencies the moved code reads by name | `datetime`, `yf`, `_tool_cache`, `_edgar_get_html`, `_get_submissions_for_ticker`, `_urlrequest`, `_utc_now_iso` |

  - **Allowed:** change patch *targets* and import plumbing in `scripts/parity/cases_*.py` and `scripts/test_*.py`.
  - **Not allowed:** change an assertion or expected value. Every such edit is listed in the report.
  - **The network guard must still hold:** an unpatched path would now reach yfinance or the network, so confirm every news case still runs offline (the harness refuses socket connects; yfinance uses curl_cffi, so check that no case starts calling real Yahoo).

## Worker mechanics

- **New module:** `worker/src/news.ts` holds the moved functions, constants and types.
  - It imports what it needs from `yahoo-finance.ts` or the other existing modules, for example `yGet`, the fetch helpers, `_str`, `clampInt`, the identity and FX helpers and the JSON registries.
  - Prefer importing from leaf modules over `yahoo-finance.ts` where a helper already lives elsewhere.
- **Re-exports:** `yahoo-finance.ts` re-exports every moved symbol that was exported before (`export { … } from "./news"`). `tools.ts`, the parity bundle (`m.yahooFinance.getCompanyNews`, …) and any other importer must keep working unchanged.
  - Non-exported moved helpers that remaining `yahoo-finance.ts` code still calls are imported back.
  - The parity bundle also exposes the new module as `m.news`. That is fine and additive.
- **Import cycle:** an ESM cycle `yahoo-finance.ts` ↔ `news.ts` is allowed only if no top-level code in either module reads an imported binding during evaluation (no TDZ errors).
  - Cloudflare loads one esbuild/wrangler bundle, so verify with `npx wrangler deploy --dry-run --outdir <scratch>` or the repo's build script, as well as `npx tsc --noEmit` and the parity harness.
  - If a cycle is unavoidable for a top-level constant, move that constant into `news.ts` or a small leaf module.
- **Size:** the Worker bundle size must not grow by more than a few KB. Report the before/after size from the dry-run build.

## Verification (report every result)

1. `python scripts/test_parity_harness.py`: 1078 tests OK, with no expected values edited.
2. `python scripts/check_tool_sync.py`, `python -m unittest scripts.test_grouped_contract_parity`, `cd worker && npx tsc --noEmit`, and the Worker dry-run build.
3. Every `run:` step in `.github/workflows/ci.yml`, except installs and live/network steps.
4. Tool listing byte-identity: expanded and grouped `list_tools` before/after diff (Python and Worker manifests/hashes, `health_check` `manifestHash`/`schemaHash` computed locally if possible).
5. The topology re-run shows 0 NLOC in both `#news` regions. Report the new `topology.json` region table, and record every function or constant that moved, stayed or was re-exported.
6. `git diff --stat`. The diff of `server.py` and `yahoo-finance.ts` must be (almost) deletions plus import/re-export blocks.

## Imports back into the god files

The brief allows imports back into the god files and requires them to be recorded. Write `analysis/yahoo-finance-mcp/PHASE3_IMPORTS.md` listing:
- every name `yfmcp/tools/news.py` imports, and from where;
- every name `worker/src/news.ts` imports from `yahoo-finance.ts`. These are the shared helpers Phase 4 must extract into the hub;
- every re-export kept in `server.py` and `yahoo-finance.ts`.
