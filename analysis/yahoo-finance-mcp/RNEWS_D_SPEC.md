# R-news D (2.22.0): F10, F11 rest, F12, F13, F14

**Source:** `F_TRIAGE.md`, approved by the owner 2026-10-19 (planner calls F10 and F12 answered "Port to Python" and "Add filler words").
**Version:** 2.22.0 (minor). Output values change for verify queries, Marketaux failure statuses and the unregistered IR-page row; the catalog text changes. There is no new field, enum value or warning code (P4). If a fix seems to need one, STOP and report.
**Order:** Worker first, then the Python port. Every case below is equal mode once both runtimes are done.

## 1. Items

| # | Change | Worker | Python |
|---|---|---|---|
| F12 | **Verify stop words.** Add the function words `a, an, and, are, as, at, by, for, from, in, is, it, its, of, on, or, the, to, with` to the event-query stop list. Terms under 3 letters are already dropped, so the words that change matching are `and, are, for, from, its, the, with`. A query left with zero meaningful terms gets the existing `INPUT_VALIDATION_ERROR`. | `worker/src/news.ts` `eventQueryStopwords` (~1339) | `yfmcp/tools/news.py` `event_query_stopwords` (~1829) |
| F13a | **Marketaux HTTP 404** reports `PROVIDER_ERROR`, the Python behaviour. A provider 404 is not evidence absence. The message follows the 2.20.0 template: `Marketaux wire fallback returned HTTP 404 (PROVIDER_ERROR).` | `worker/src/news/providers/marketaux.ts`: the 404 mapping through `providerHttpStatus`. Change only the Marketaux mapping; leave the shared `providerHttpStatus` used by other providers unchanged. | none |
| F13b | **Marketaux non-object JSON body** reports `PROVIDER_CHANGED`, the Python behaviour; it matches the Finnhub non-array rule. The message is `Marketaux wire fallback unavailable (PROVIDER_CHANGED).` | `marketaux.ts` | none |
| F13c | **Legacy-envelope batch entries** keep the entry's `code`. `normalizeBatchSymbolResult` reads `code` on legacy errors too, so a malformed symbol reads `INPUT_VALIDATION_ERROR` under `MCP_ENVELOPE_V2=false`. | `worker/src/yahoo-finance.ts` `normalizeBatchSymbolResult` | none (Python has no `__batchMeta`) |
| F11 rest | **The unregistered IR-page row carries both keys:** `canonicalUrl: null` and `registrySchemaVersion` (the registry file's schema version). | add `canonicalUrl: null` to the NOT_REGISTERED row | add `registrySchemaVersion` from the registry file |
| F10 | **Python parses RSS/Atom bodies for IR-page entries,** as the Worker does. | none | Port `looksLikeFeed`-first dispatch, `parseCompanyIrFeedItems`, `atomEntryLink` and the feed-item mapping (`companyIrPageFeedItems`) into `yfmcp/news/providers/company_ir_page.py`. The dispatch order becomes the Worker's: feed first, then structured JSON, then HTML. The `Accept` header gains `application/rss+xml, application/atom+xml`, matching the Worker exactly. The `<!DOCTYPE` / `<!ENTITY` refusal (2.17.0) applies to these feeds too. Text decoding and entity handling follow the Worker's helpers; where Python's `html.unescape` decodes more entities, keep it, as in RNEWS3 decision 5. |
| F14 | **Catalog text.** Add the missing params to the 25 actions listed in `_KNOWN_UNADVERTISED_PARAMS`, and add a Params line to `get_analyst_recommendations` and `get_ownership_holders`. Empty `_KNOWN_UNADVERTISED_PARAMS`, keeping the stale-entry check. The `tool_catalog.json` text must stay byte-identical between runtimes, so check `check_tool_sync` and the grouped-contract test. | `tool_catalog.json` (shared) | — |

## 2. Parity cases

- **F12:** "the and of" gets `INPUT_VALIDATION_ERROR`. "the earnings results" keeps the existing behaviour, with `meaningfulTerms` excluding "the". A query mixing filler and content words matches on the content words only.
- **F13:** a 404 case and a non-object-body case, flipped from pinned to equal where they exist (`news.marketaux.*`) or added. A legacy-envelope batch case (Worker-only, pinned, documenting `__batchMeta`) asserts the entry code.
- **F11:** extend `news.ir_page.unregistered_ticker_row_carries_registry_version_and_not_registered` to compare the full key set.
- **F10:** an RSS case and an Atom case for a structured IR-page entry, using shared fixtures, plus a DOCTYPE-refusal case. All equal.
- **F14:** `test_grouped_contract_parity` passes with an empty frozen list.

## 3. Docs and live checks

**Docs:** a section "News Repairs, Batch D (2.22.0)".

**Live checks:**
- `health_check` reports 2.22.0.
- `verify_company_event` AAPL "the and of" returns `INPUT_VALIDATION_ERROR`.
- `verify_company_event` AAPL "earnings results" returns CONFIRMED as before.
- `tools/list` descriptions show the new Params lines.

## 4. Planner decisions (2026-10-20)

1. **F13b message.** A non-object body uses Python's existing text for the same shape failure, `Marketaux provider changed: expected a data array.`, in both runtimes. This replaces the template text in §1. Python is unchanged. The Worker maps a `PROVIDER_CHANGED` fetch result to that message.
2. **F13c.** The Worker step also applies the V2 branch's `retryable` rule to legacy entries with a code (`retryable === true` or `PROVIDER_TIMEOUT`). This is accepted: it keeps legacy and V2 entries consistent and adds no new value.
