# R4, second release (2.12.0): Q3b central validation and Q3c per-tool ticker caps

**Owner decisions:**
- **Q3b (2026-10-15):** centralise ticker and accession validation, and apply it consistently in both runtimes and all public actions.
- **Q3c with Q12:** per-tool ticker caps, advertised as schema `maxItems` and enforced identically in both runtimes. Oversize requests are rejected with `BATCH_LIMIT_EXCEEDED`. Silent truncation is removed.

**Order (A3):** this release comes after Q10, which was 2.11.1.

**Version: 2.12.0** (minor). Requests that were accepted before can now be rejected, and `BATCH_LIMIT_EXCEEDED` becomes a tool-level error code. The name was accepted under Q11.

**Input:** the planner's inventory from 2026-10-16. Its tables are summarised below.

## 1. Facts this spec relies on

- **Batch parameter:** every capped batch tool takes `ticker: string | string[]`. A comma string is never split; only an array takes the batch path.
- **Worker validation today:**
  - `validateTicker` exists (`worker/src/validate.ts`) with the same grammar and message as Python `_validate_ticker`.
  - It is used only by `get_historical_prices` and the evidence tools.
  - The Worker has **no accession validator**.
- **Python validation today:**
  - `_validate_ticker` and `_validate_accession` (`yfmcp/validation.py`) cover most SEC, news, earnings and provider-gap actions.
  - Some paths resolve a document URL from the ticker **before** validating (server.py ~5213, ~5226).
- **Batch caps today:**
  - Worker: `MAX_TICKERS = 5`. `runPartialBatch` rejects with `INPUT_VALIDATION_ERROR`. `limitTickers` truncates, and is used only by `get_technical_indicators` and `get_price_slope`.
  - `get_market_snapshot` truncates to 5 compact or 2 full, in both runtimes.
  - Python has no other cap.
- **Schemas:**
  - Worker grouped mode enforces schema `maxItems` before the handler. Expanded mode does not.
  - Python schemas carry no `maxItems`, and its grouped checker ignores length.

## 2. Q3b: one validation gate per runtime

**Where the gate runs:** at the tool-call boundary, before any handler code or fetch.
- Worker: in `tools.ts`, in one function called from both the grouped and the expanded dispatch paths.
- Python: in the shared tool-call wrapper (`yfmcp/app.py`), so it covers both modes.

**What it validates, by argument name, for every public action:**

| Argument | Rule |
|---|---|
| `ticker` (string, or each element of an array) | `validateTicker` / `_validate_ticker` |
| `tickers` (each element), `subject`, `peers` (each element) | same |
| `accession_number`, `accessionNumber` | `validateAccession` / `_validate_accession`, `^\d{10}-\d{2}-\d{6}$` after trim. A new Worker function mirrors Python exactly, message included. |

- **Absent or null optional arguments** are skipped, so the existing "missing parameter" behaviour is unchanged.
- **Grammar:** unchanged, 1–20 characters `[A-Z0-9.\-^=]` after trim and upper-case. Nothing that passes today's validators starts failing.

**On failure:** `INPUT_VALIDATION_ERROR` with the existing messages.
- **The echo shows the raw caller value** (Python's behaviour). The Worker's `get_historical_prices` changes from echoing the normalised value to the raw one.
- **The tool name in the failure is the public action name.**

**Excluded:** arguments with other grammars: Thai `fund_class_name`/`proj_id`, UK `company_number`, `document_url`, cut ids (those have their own validators).

**Remove now-redundant per-handler checks** only where the gate makes them dead. Behaviour must not change for a value that passes.

**Ordering fix:** because the gate runs first, Python's resolve-then-validate paths (server.py ~5213, ~5226) now reject before any fetch.

## 3. Q3c: per-tool caps

**One cap table, read by both runtimes.** Put it in `tool_catalog.json` as a new key `tickerCaps` (data, not code): `{ "<public action>": <int> }`. Worker schema `maxItems` and Python schema `max_length` are generated from it, and the gate (§2) enforces it.

| Action | Cap |
|---|---:|
| get_market_quote, get_company_profile, get_fund_profile, analyze_moving_average_position, analyze_volume_ratio, analyze_price_performance, get_analyst_rating_changes, analyze_earnings_momentum | 10 |
| analyze_financial_ratios, analyze_credit_health, get_company_news | 5 |
| get_analyst_consensus, get_technical_indicators, get_price_slope, get_short_momentum (not in the owner table; keep today's Worker value) | 5 |
| get_market_snapshot | measured, §4 |

- **Conditional cap:** `get_company_news` is capped at **1** when `sources` includes `company_ir`, `company_ir_page` or `newswire`. This is checked in the gate, and the action's schema description states it, because JSON Schema cannot express it.
- **Oversize:** an array longer than the cap is rejected before any fetch.
  - The error is `{ ok: false, error: { code: "BATCH_LIMIT_EXCEEDED", message: "Too many tickers: N. Maximum is M per call for <action>." } }`, with `retryable: false`.
  - Add `BATCH_LIMIT_EXCEEDED` to the Worker and Python `ErrorCode`. The planner extends the Q11 kind from HTTP-level to also tool-level, with the same meaning.
- **No truncation anywhere:**
  - Remove `limitTickers`/`wrapBatchResult` truncation (`get_technical_indicators`, `get_price_slope`) and the `get_market_snapshot` truncation (`truncated`, `droppedTickers`) in both runtimes.
  - Oversize gets the error above.
  - The `_truncated`, `truncated` and `droppedTickers` output keys disappear for in-cap requests too. This is a deliberate contract change; list it in the docs.
- **Unchanged, out of this release's scope:** peer tools keep their own rules and messages, in both runtimes already: `compare_peer_valuations` 2–10, `get_historical_valuation_context` peers ≤5, `get_thai_fund_nav_batch` ≤20.
- **Stale text to fix:**
  - the comment at yahoo-finance.ts ~70–87 (Free plan 50 per invocation, 45 budget since 2.11.0, per-tool caps from `tickerCaps`);
  - the `get_analyst_consensus` description ("only the first 5 are processed");
  - the Python docstrings saying "up to 5" (pricing.py ~652, ~1377; news.py ~3355).
- **Python schema:** add `max_length` from `tickerCaps` to the array branch of the generated input schema, and make the grouped checker enforce length. Tool listing parity must then match the Worker's `maxItems`.

## 4. Measuring `get_market_snapshot` (planner rule)

- **How:** run the Worker bundle locally against live upstream, as the 2026-10-16 AAOI diagnostic did, with an empty cache stub and a fetch counter. Measure subrequests for compact and full mode at n = 2, 5, 10 and 20, with real large-cap tickers.
- **Cap:** the largest n among {2, 5, 10, 20} whose **cold** count stays at or below **40** in compact mode. Full mode gets its own cap, by the same rule, under the same action, via the gate. `tickerCaps` gains `get_market_snapshot` and `get_market_snapshot.full`.
- **Report:** put the measured table in the PR body and the docs.

## 5. Tests

- **Gate tests, both runtimes, table-driven, the same cases:**
  - a valid ticker passes;
  - lowercase and whitespace are normalised;
  - `BRK/B`, `A,B`, an empty string and a 21-character string are rejected;
  - a bad element inside an array is rejected;
  - `subject` and `peers` are checked;
  - for accessions, the dashed form passes, while 18 bare digits and a short form are rejected;
  - the error envelope and message are exact;
  - no fetch happens on rejection (fetch spy).
- **Cap tests:**
  - for each capped action, cap passes and cap+1 is `BATCH_LIMIT_EXCEEDED` with no fetch;
  - the `get_company_news` conditional cap;
  - the snapshot caps;
  - Worker schema `maxItems` matches `tickerCaps`;
  - Python schema `max_length` matches as well;
  - the grouped-mode checkers enforce length in both runtimes.
- **Parity:**
  - new `cases_validation.py` and `cases_caps.py`, mode equal;
  - existing cases that pinned old behaviour are updated, and each edit is listed:
    - `cases_news_b.py` ~1442: six tickers is now `BATCH_LIMIT_EXCEEDED` in both runtimes;
    - `cases_pricing_evidence_tools.py` ~1197/1205: `_truncated` goes away, and the oversize case becomes the error;
    - `test_python_batch_concurrency.py` ~92: `droppedTickers` goes away;
    - validation-message cases (`cases_pricing_evidence_tools.py` ~905–919, `cases_news_b.py` ~1382) change only where the echo or tool name changes.
- **Harness:** the tool-listing parity check compares `maxItems` in both runtimes.

## 6. Planner checks during execution

STOP and report rather than choose if any of these come up:
- a currently-valid real-world ticker form fails the grammar;
- an action's batch parameter is not `ticker`;
- removing truncation changes an in-cap response other than dropping the truncation keys;
- the snapshot measurement exceeds 40 at n = 2.

## 7. Release and live verification

- **Docs section "Central Validation and Ticker Caps (2.12.0)":**
  - the gate;
  - the cap table, including the measured snapshot caps;
  - `BATCH_LIMIT_EXCEEDED` at tool level;
  - truncation removed, along with its output keys;
  - the echo and tool-name changes.
- **Live checks:**
  - `get_market_quote` with 10 tickers works and 11 is rejected;
  - `analyze_credit_health` with 6 is rejected (expanded mode, or grouped mode with an array);
  - `get_company_news` with 2 tickers and `company_ir` is rejected;
  - `list_sec_company_filings` with ticker `BRK/B` gets `INPUT_VALIDATION_ERROR` before any fetch;
  - the canaries pass;
  - telemetry shows the rejections under `errorCode`.

## 8. Planner decisions taken during execution (2026-10-16)

1. **Ticker grammar** stays `[A-Z0-9.\-^=]{1,20}`.
   - Real Yahoo symbols containing `&` (e.g. `M&M.NS`) remain rejected, as they already were in Python and in the Worker's `get_historical_prices`.
   - Adding `&` needs an audit that every URL build in both runtimes encodes it. That audit becomes an R4 triage item, not part of this release.
2. **Empty strings:** an empty or whitespace-only `ticker`, `subject` or accession is treated as absent. Empty elements inside arrays are left to each handler's existing behaviour, so every empty-string behaviour stays as it is today.
3. **Accession grammar** is ASCII digits only, `^[0-9]{10}-[0-9]{2}-[0-9]{6}$` after trim, in both runtimes. Python's `\d` currently also accepts other Unicode digits; it changes to match.
4. **The cap is checked before the ticker format.** An oversize array therefore gets `BATCH_LIMIT_EXCEEDED`, whatever its contents.
5. **Conditional news cap:** source names are matched after trim and lower-case.
6. **Measured `get_market_snapshot` cost** is about 2 + 5n subrequests, cold, in both modes:

   | n | Subrequests |
   |---:|---:|
   | 2 | 12–13 |
   | 5 | 27–28 |
   | 10 | 52–53 |
   | 20 | 103 |

   So the caps are `get_market_snapshot` = 5 and `get_market_snapshot.full` = 5. Full mode goes from 2 to 5.
