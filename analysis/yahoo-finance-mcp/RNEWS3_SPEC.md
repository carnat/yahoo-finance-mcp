# R-news part 3: the approved news repairs, in three releases

**Source:** the approved Q3i triage (`Q3I_TRIAGE.md`, 2026-10-18).
- The row detail is in `q3i/news_a.md` and `q3i/news_b.md`. The group IDs below refer to those files, as `A-Gn` and `B-Gn`.
- Planner calls (`#n`) are in `Q3I_TRIAGE.md` §2. Where a row says REPAIR, the target behaviour in its row is binding unless a call below overrides it.
- Code layout since 2.15.1: providers are in `worker/src/news/providers/*.ts` and `yfmcp/news/providers/*.py`; the pipeline is in `worker/src/news.ts` and `yfmcp/tools/news.py`.

**General rules:**
- Each fix flips its pinned parity case(s) to `equal`, with the agreed value.
- Shared defects (Q7 comments on equal cases) keep `equal` with the new value.
- No new status, warning code, reasonCode or field: the rows and calls below only reuse existing ones (rule P4). If a fix seems to need one, STOP and report.
- Preserve rows: align-free ones are aligned; document-only ones get a line in the release docs section.

## Batch A (2.17.0): Yahoo feed, classification, dedupe, parsing

| Ref | Fix |
|---|---|
| B-G1 | **Worker `getNews()`** passes through `contentType` (the row's own, or `content.contentType`), `summary`, and the `content.pubDate` fallback. The existing VIDEO filter and PRESS_RELEASE relabel then apply unchanged. **Call #13:** both runtimes show the summary (`summary[:240]`, `evidenceText[:180]`, the Python cut points), but identity and ticker matching read the title only. Python stops matching on the summary. |
| A-G1, B-G13, #6 | **Event typing:** one keyword table in both runtimes, the Python superset, so the Worker adds `announces new`, `annual report` and `credit facility`. Matching is whole-word (word boundaries), and the first group wins as today. |
| A-G3 (entity), B-G16 | **Worker dedupe** decodes HTML entities in titles before the stem, using its existing decoder. |
| A-G3 (ms), B-G14 (ms) | **Timestamps:** the Worker's ISO formatter emits `Z` without `.000` when the milliseconds are zero. |
| B-G14 (RFC 2822) | **Python** parses RFC 2822 provider dates (`email.utils.parsedate_to_datetime`) like the Worker. |
| B-G14 (cut) | **Text cut:** the Worker cuts at 240 code points (`Array.from`), as Python does, so no lone surrogates. |
| B-G14 (label) | **Provenance label:** an unparseable URL gets `UNKNOWN` in both runtimes (Worker behaviour). |
| A-G2 | **Python identity:** an alias equal to the ticker (`ticker.lower()`) is not issuer identity (Worker behaviour). |
| B-G4, A-G11 | **Exchange normaliser:** one normaliser in both runtimes. It tests `NYSE AMERICAN` / `AMEX` / exact `ASE` before `NYSE`, and treats `GS` / `NMS` / `NGM` / `NCM` as whole tokens or exact codes. Python gains the Worker's GlobeNewswire stock-category exchange check, using the fixed normaliser. |
| B-G12 | **Shared defects:** (a) single-letter tickers match only as whole tokens; (b) the legacy `yahoo_finance` source counts the shared feed once; (c) verify relevance is scored from the evidence text and issuer, not the injected `tickers` list, so `LOW_RELEVANCE_EVIDENCE` can fire. |
| A-G5, B-G9 | **Form 4:** (a) the value is rounded half away from zero to cents in both runtimes, using a decimal-safe method; (b) the summary string uses the same rounding helper; (c) Python falls back to its HTML Form 4 parser when the XML has no transaction node (Worker behaviour). |
| A-G14, B-G15 | **GlobeNewswire XML guard:** the Worker refuses `<!DOCTYPE` as Python does, and the company IR feed guard follows. The entity-refusal message is aligned to one wording (Python's "unsupported XML declaration"). |
| A-G4 | **Python news path** emits `SEC_URL_INVALID` for an SEC filing without a URL (Worker behaviour). |

## Batch B (2.19.0): company IR, IR pages, press releases, verify

| Ref | Fix |
|---|---|
| A-G10, B-G2 | **`company_ir` in Python** becomes registry-only: port the Worker's 2.10.0 `collectCompanyIrEvents` into `yfmcp/news/providers/company_ir.py`. No Yahoo pass-through and no "used" marking without an approved entry. Retire or merge the obsolete discovery cases, as noted in the inventories. |
| A-G9, B-G3 | **IR-page registry states:** the Worker emits Python's info warnings `COMPANY_IR_PAGE_CANDIDATE_AVAILABLE` and `COMPANY_IR_PAGE_DISABLED`, so the collection status is `SOURCE_LIMITED_NOT_FOUND`, not `PROVIDER_ERROR`. `allowedAction` values: rows d/e use Python's existing values; row f uses the Worker's; row g fills the registry metadata in Python. |
| B-G17 | Python's IR-page issuer is null without a registry name. Python reads the structured JSON summary from `content_text`. An html_adapter given a JSON content type is `SOURCE_VALIDATION_FAILED` in both runtimes. **Call #17:** an RSS body on an `html_article` entry is `SOURCE_VALIDATION_FAILED` in both. |
| A-G7, #8, #9 | **Press releases:** Python adopts `COMPANY_IR_PAGE_CANDIDATE_ONLY`, and the Worker adds it to `limitationStatuses`. An SEC outage keeps the status, while `failureMode` and coverage PARTIAL come from the failed SEC row. |
| A-G8, #10, #11, #12, #14 | **verify_company_event:** <ul><li>STALE when the match is older than 90 days, including when no range is given.</li><li>The Worker collects the 365-day window and filters by range only when classifying.</li><li>bestEvidence confidence is source-type based (LOW for contextual news) in Python.</li><li>`AUTH_ERROR` and an unregistered `company_ir` give SOURCE_LIMITED_NOT_FOUND with `failureMode` `SOURCE_UNCONFIGURED`.</li><li>A query with no meaningful terms gets `INPUT_VALIDATION_ERROR`.</li></ul> |
| B-G11, #16 | **Analyst valuation from news:** the alias set is longName, shortName and companyName, excluding an alias equal to the ticker, in both runtimes. Python's `NEWS_UNAVAILABLE` warning goes to the Worker. |
| B-G8, #15 | **`get_sec_recent_events`:** Python adds `coverage`, `sourceCoverage` and `sourceStatus`. A blank `filing_types` gets `INPUT_VALIDATION_ERROR`, and `[]` means the defaults (8-K/10-Q/10-K), in both runtimes (Python behaviour). |
| B-G18, #18 | **Press releases:** Python adds `secEvidence[].cikInt`. `doctrineUse` stays Worker-only and is documented. `decisionGradeBasis` uses Python's wording. |

## Batch C (2.20.0): coverage, providers, inputs, catalog

| Ref | Fix |
|---|---|
| #19 (live input) | **Coverage when truncated:** provider- or cap-side truncation makes coverage PARTIAL in both runtimes: the pool cap of 100, the Marketaux 3-row limit, or dedupe within a source. Truncation to the caller's own `max_results` stays FULL. |
| B-G5, B-G6 | **Finnhub and Marketaux warning text:** one message template per status, Python's `HTTP <n>` wording, no internal tags. Marketaux keeps reading after the 3-row limit, so `acceptedCount` counts every acceptable row (Python behaviour). |
| B-G7 | **Zero inputs:** `max_results=0` / `lookback_days=0` clamp to 1 in both runtimes (Worker behaviour). |
| B-G10 | **Provider responses:** a "current plan does not include" body counts as an entitlement error in Python (Worker pattern). The denial memory stays Worker-only and is documented. |
| A-G13 | **Unreadable news-source policy:** it fails closed in Python, `provider_gaps.py` and `news.py` `_finnhub_policy` alike. |
| A-G12 | **Batches:** the Worker validates ticker format in batch requests as Python does. `__batchMeta` is documented. |
| B-G19 | Delete the dead Python `get_yahoo_finance_news` helper and its schema key, and re-point the parity case. |
| Live input | **`tool_catalog.json` params:** add `sources` to `get_company_press_releases` and `verify_company_event`, and `filing_types` and `max_results` to `get_sec_recent_events`. Tighten `test_grouped_contract_parity` so omitted optional params are caught. |
| A-G3, B-G16 (id) | `duplicateGroupId` format: documented only. |

## Process per batch

1. Worker first, then the Python port, then release. Each batch gets its own PR and version.
2. Live checks per batch, using calls whose output the batch changes. Batch A, for example: the AAPL "tech sector" item is no longer `regulatory`, and `.000Z` disappears from timestamps.

## Planner decisions taken during execution (2026-10-18)

### Batch A
1. **Whole-word event typing keeps simple inflections.**
   - Each keyword matches as `\b<keyword>(?:s|es|d|ed|ing)?\b`. For multi-word phrases the suffix applies to the last word.
   - So "launches", "introduces", "notes" and "announced" still match, while "sector", "dealer" and "steps" no longer hit `sec`, `deal` or `eps`.
   - Python uses `re.ASCII` so that `\b` matches JavaScript's.
2. **Counting the legacy `yahoo_finance` feed once:** its counts are the single read of the feed, which is the news pass (raw, retrieved, accepted, rejected). Example: 20 stories give raw 20, not 40.
3. **Ticker in text:** only single-letter tickers change to whole-token matching, as the batch table says. Longer tickers keep the substring test.
4. **"LSE GS" still reads as NASDAQ:** GS is a whole token there. Accepted.
5. **Notes for the Python port:** a Form 4 value that rounds to zero prints "$0", never "-0". Python's `html.unescape` decodes more entities than the Worker's decoder. Python keeps `html.unescape`, because the case set compares only the common entities.

### Moved to Batch B (the verify fixes)
6. **Verify relevance issuer names.**
   - Both runtimes use one name set: Yahoo `longName`, `shortName` and the SEC submissions `name`, excluding any alias equal to the ticker. This is the same rule as call #16.
   - A `company_ir_page` source counts as an official, HIGH source type in both runtimes (Worker behaviour). It is the official tier in the registry.

### Batch B decisions (2026-10-19)
7. **Call #14 applies only when `company_ir` is requested explicitly.**
   - `company_ir` is a default verify source, and almost no ticker has an approved entry. Mapping a defaults-applied `company_ir` NOT_REGISTERED row to SOURCE_LIMITED_NOT_FOUND would make a default verify call nearly never return NOT_FOUND, which drowns the signal.
   - **Rule:** the upgrade applies when the caller's `sources` names `company_ir`. With default sources, the NOT_REGISTERED row stays in `coverage.failedSources` / `sourceStatus` as today, and it does not change `status` or `failureMode`.
   - AUTH_ERROR (call #10) upgrades in both cases, because it is a real provider failure.
8. **Accepted judgement calls:**
   - AUTH_ERROR and NOT_REGISTERED take the UNCONFIGURED precedence slot.
   - "Failed SEC row" means PROVIDER_ERROR or PROVIDER_BUDGET_EXHAUSTED. NOT_ELIGIBLE and an operator-disabled SEC keep the old `failureMode`.
   - An undated match counts as in range.
   - B-G11's invalid-ticker case completes in Batch C, with A-G12 ticker validation.
9. **New finding, for the next triage:** F8. Python raises `RATING_CHANGES_UNAVAILABLE` when the analyst radar call throws, and the Worker does not.

### Batch C decisions (2026-10-19)
10. **#19 truncation coverage.**
    - The rule only reads sources already listed in `coverage.truncatedSources`. No new entries are added, and no new emitted field.
    - `coverage.state` becomes PARTIAL when a listed source was cut below the caller's effective `max_results`: its `returnedCount` is lower than that limit, as with the pool cap of 100 or the Marketaux 3-row limit. A source cut only by the caller's own `max_results` leaves the state as it is.
    - Any internal marker the pipeline needs to tell the two apart is stripped before output.
    - `status`, `failureMode`, `failedSources` and the `recommendedNextAction` logic are unchanged.
11. **Finnhub message templates (both runtimes).**
    - 401/403: `Finnhub auth error: HTTP <n>`
    - 429: `Finnhub rate limited: HTTP 429`
    - any other HTTP status: `Finnhub source unavailable: HTTP <n>`
    - non-array payload: `Finnhub provider changed: unexpected response format`
    - non-HTTP exceptions keep `Finnhub source unavailable: <error message>` (network error text cannot be aligned).
    - Internal `FINNHUB_*` tags never reach a message. Message-based status inference must keep giving the same statuses; check every reader of these messages.
12. **Marketaux message templates (both runtimes).**
    - HTTP failure: `Marketaux wire fallback returned HTTP <n> (<STATUS>).`
    - non-HTTP failure: `Marketaux wire fallback unavailable (<STATUS>).`
    - The Worker reads every row after the 3-row limit, so `acceptedCount` and the rejection counts cover all rows (Python behaviour).
13. **Zero inputs.** In every news tool, `max_results` / `lookback_days` of 0 clamp to 1 in Python. A missing value still means the default.
14. **Fail closed (A-G13).** If the policy file cannot be read or parsed, `provider_gaps._finnhub_eligible` and `news._finnhub_eligibility` return not eligible with the bundled policy's existing `reasonCode` (`FINNHUB_MARKET_NOT_ELIGIBLE` when absent). There is no new reason code.
15. **Batch ticker validation (A-G12).** The Worker applies Python's `_validate_ticker` rule (same regex, same message) to each batch symbol. A malformed symbol gets an `INPUT_VALIDATION_ERROR` entry and is counted as an error in `__batchMeta`. Single-symbol Worker tools that already validate keep their behaviour; if a single-symbol news tool does not validate, align it in the same way.
16. **F8 to F11 are included** (align-to-Worker fixes with existing values, except F8):
    - **F8:** the Worker adds Python's `RATING_CHANGES_UNAVAILABLE` warning when the analyst radar call throws. It uses the same shape and message. The code already exists in Python output (P2).
    - **F9:** Python rejects a structured IR-page entry whose content type is not json/xml/rss/atom/text/plain, as the Worker does, with the same status and warning.
    - **F10:** Python parses RSS/Atom bodies for structured IR-page entries, as the Worker does. If the port needs more than one parser function, STOP and report.
    - **F11:** Python fills `registryVersion` and `approvalStatus: "not_registered"` on the unregistered IR-page row.
17. **F12 (stop words) is not in this batch.** It is logged in `Q3I_TRIAGE.md` §5. Changing the meaningful-term rule changes the verify matching threshold, so it goes to the next triage.
