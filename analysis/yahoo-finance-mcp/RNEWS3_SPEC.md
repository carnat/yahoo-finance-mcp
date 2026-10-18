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
