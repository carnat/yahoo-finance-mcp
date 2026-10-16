# Phase 3 imports record: Worker (TypeScript)

Companion to `PHASE3_SPEC.md` (section "Imports back into the god files"). This file covers the Worker half; the Python half (`yfmcp/tools/news.py`) is recorded separately.

Pure move: `worker/src/news.ts` holds the 97 functions that `phase3_news_region.json` assigns to `worker/src/yahoo-finance.ts#news` (3,361 NLOC by the topology's per-function count) plus 43 module-level constants and types used only by them. The moved text is verbatim; the only edits to moved code are `export` keywords on the three moved helpers (`parseHtmlTable`, `hostWithoutWww`, `blockedCompanyWebsiteHost`) listed under "Imports back" below.

## 1. What `news.ts` imports from `yahoo-finance.ts` (Phase 4 hub candidates)

23 names. These are shared helpers, caches and types that the news code reaches back into the god file for. Phase 4 should extract them into leaf/hub modules so `news.ts` stops importing `yahoo-finance.ts`.

`New export` means the declaration was not exported before and gained an `export` keyword in `yahoo-finance.ts` (no other change to it).

| Name | Kind | New export | Used by (moved functions) |
|---|---|---|---|
| `UA` | const | yes | `collectFinnhubEvents`, `fetchApprovedCompanyIrPage`, `fetchCompanyIrText`, `fetchGlobeNewswireFeed`, `fetchMarketauxJson` |
| `ProviderJsonResult` | type | yes | `fetchMarketauxJson` |
| `providerCacheKey` | function | yes | `fetchMarketauxJson` |
| `getProviderCache` | function | yes | `fetchMarketauxJson` |
| `setProviderCache` | function | yes | `fetchMarketauxJson` |
| `providerHttpStatus` | function | yes | `fetchMarketauxJson` |
| `fetchProviderWithTimeout` | function | yes | `fetchMarketauxJson` |
| `COMPANY_IR_DISCOVERY_TTL_MS` | const | yes | `discoverCompanyIrFeeds` |
| `yGet` | function | no | `getNews`, `verifyCompanyEvent` |
| `enc` | const | yes | `getNews`, `verifyCompanyEvent` |
| `iso` | const | yes | `getNews`, `normalizeIso` |
| `runPartialBatch` | function | yes | `getCompanyNews` |
| `edgarGetHtml` | function | yes | `tryAttachForm4Transaction` |
| `edgarCikFromAccession` | function | yes | `getCompanyPressReleases` |
| `getSubmissionsForTicker` | function | no | `collectSecEvents` |
| `stripHtmlTags` | function | yes | `companyIrFeedItem`, `companyIrPageFeedItems`, `companyIrPageHtmlItems`, `extractCompanyIrFeedLinks`, `form4Date`, `form4Num` (+4 more) |
| `_str` | const | yes | `buildCoverage`, `buildSecEventItem`, `buildYfEventItem`, `collectCompanyEvents`, `collectCompanyIrPageEvents`, `collectCompanyIrRssEvents` (+34 more) |
| `clampInt` | function | yes | `collectCompanyEvents`, `getCompanyNews`, `getCompanyPressReleases`, `getPublicEventTimeline`, `getSecRecentEvents`, `searchCompanyNews` |
| `NewsCompanyIdentity` | type | yes | `candidateCompanyIrPageUrlGroups`, `collectCompanyEvents`, `companyIrCandidateHosts`, `companyIrFeedItem`, `discoverCompanyIrFeeds`, `identityMatchesText` (+2 more) |
| `finnhubEligibility` | function | yes | `collectCompanyEvents` |
| `resolveNewsCompanyIdentity` | function | yes | `collectCompanyEvents`, `collectCompanyIrRssEvents` |
| `toIsoUtc` | function | yes | `collectMarketauxEvents` |
| `resolveEx991Url` | function | yes | `getCompanyPressReleases` |

Type-only names (`ProviderJsonResult`, `NewsCompanyIdentity`) are imported with the inline `type` modifier and erased at build time.

## 2. Every other import of `news.ts`

Taken straight from the leaf modules where the helpers already live (nothing is routed through `yahoo-finance.ts`).

| Module | Names |
|---|---|
| `./cache.js` | `setBounded` |
| `./request-context.js` | `providerFetch` |
| `./response.js` | `getWorkerVar` |
| `./extraction-rules.js` | `rankEvidence`, `stemWord` |
| `./deprecations.js` | `reliabilityClassFor` |
| `./company-ir-page-registry.json` | `registryManifest` |

`registryManifest` (the bundled `company-ir-page-registry.json`) moved with `companyIrPageRegistry`, its only consumer, so the JSON import now lives in `news.ts`. `newsSourceCapabilities` stays in `yahoo-finance.ts` (used by `finnhubEligibility`, which stays).

## 3. Imports back: `yahoo-finance.ts` from `news.ts`

Moved helpers that code remaining in `yahoo-finance.ts` still calls (all moved, non-exported before; `news.ts` now exports them):

| Name | Kind | Called from remaining code |
|---|---|---|
| `parseHtmlTable` | function | `parseFinancialTableRows` |
| `hostWithoutWww` | function | `companyHostRoot`, `normalizeCompanyWebsite` |
| `blockedCompanyWebsiteHost` | function | `normalizeCompanyWebsite` |
| `getCompanyNews` | function (also re-exported) | `extractAnalystValuationMethods` |

`parseHtmlTable`, `hostWithoutWww`, `blockedCompanyWebsiteHost` were non-exported helpers; `news.ts` gained `export` on them. `getCompanyNews` is called by non-news code (it is also in the re-export list below).

## 4. Re-exports kept in `yahoo-finance.ts`

`export { ... } from "./news.js"` for every moved symbol that `yahoo-finance.ts` exported before, so `tools.ts`, the parity bundle (`m.yahooFinance.getCompanyNews`, ...) and every other importer are unchanged:

- `getNews` (function)
- `DEFAULT_NEWS_SOURCES` (const)
- `DEFAULT_PRESS_RELEASE_SOURCES` (const)
- `DEFAULT_EVENT_SOURCES` (const)
- `sourceRequestFields` (function)
- `yahooItemIssuer` (function)
- `collectYahooEvents` (function)
- `rankNewsItemsByRelevance` (function)
- `getCompanyNews` (function)
- `searchCompanyNews` (function)
- `getCompanyPressReleases` (function)
- `getSecRecentEvents` (function)
- `getPublicEventTimeline` (function)
- `verifyCompanyEvent` (function)

The parity bundle additionally exposes the new module as `m.news`.

Nothing exported from `yahoo-finance.ts` was removed. Besides the re-exports, `yahoo-finance.ts` gained the `export` keyword on the declarations in section 1 marked `New export` (additive; `tools.ts` does not use them).

## 5. Cycle handling

`yahoo-finance.ts` and `news.ts` import each other. No top-level code in either module reads a binding of the other during module evaluation:
- the moved constants (`globenewswireCache`, `companyIrDiscoveryCache`, `companyIrTextCache`, `companyIrPageRegistry`, the regexes, sets and TTLs) have initialisers that use only literals, `new Map()`/`new Set()` and the JSON import (checked by scanning every moved and remaining non-function declaration for identifiers owned by the other module: none);
- everything that crosses the boundary is a function body or a type, read at call time.
- Verified by bundling each of `news.ts`, `yahoo-finance.ts`, `tools.ts` and `index.ts` as the sole entry and evaluating it in Node (no TDZ error), as well as the harness's all-modules entry and `wrangler deploy --dry-run`.

## 6. Moved constants and types (used only by moved functions)

- `FINNHUB_COMPANY_NEWS_API` (const)
- `MARKETAUX_NEWS_API` (const)
- `MARKETAUX_WIRE_DOMAINS` (const)
- `MARKETAUX_MAX_ITEMS` (const)
- `MARKETAUX_NEWS_TTL_MS` (const)
- `GLOBENEWSWIRE_RSS_FEEDS` (const)
- `GLOBENEWSWIRE_MAX_BYTES` (const)
- `GLOBENEWSWIRE_TTL_MS` (const)
- `GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN` (const)
- `GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN` (const)
- `GLOBENEWSWIRE_CACHE_MAX` (const)
- `globenewswireCache` (const)
- `COMPANY_IR_HTML_MAX_BYTES` (const)
- `COMPANY_IR_FEED_MAX_BYTES` (const)
- `COMPANY_IR_FEED_TTL_MS` (const)
- `COMPANY_IR_DISCOVERY_MAX_PAGE_PROBES` (const)
- `COMPANY_IR_DISCOVERY_PATHS` (const)
- `COMPANY_IR_BLOCKED_WEBSITE_HOSTS` (const)
- `COMPANY_IR_DISCOVERY_CACHE_MAX` (const)
- `companyIrDiscoveryCache` (const)
- `COMPANY_IR_TEXT_CACHE_MAX` (const)
- `companyIrTextCache` (const)
- `YAHOO_ALLOWED_CONTENT_TYPES` (const)
- `OFFICIAL_SOURCE_TYPES` (const)
- `SUPPORTED_EVENT_SOURCES` (const)
- `SOURCE_PRIORITY` (const)
- `DEFAULT_NEWS_SOURCES` (const, exported)
- `DEFAULT_PRESS_RELEASE_SOURCES` (const, exported)
- `DEFAULT_EVENT_SOURCES` (const, exported)
- `NOT_ATTEMPTED_SOURCE_STATUSES` (const)
- `DiscoveredCompanyIrFeed` (type)
- `CompanyEventCollectionResult` (type)
- `CompanyIrPageSource` (type)
- `CompanyIrPageRegistry` (type)
- `companyIrPageRegistry` (const)
- `YAHOO_NEWS_LEGAL_SUFFIXES` (const)
- `YAHOO_NEWS_ACRONYM_IGNORED_WORDS` (const)
- `YahooNewsIdentity` (type)
- `YahooNewsMatch` (type)
- `FORM4_TRANSACTION_LABELS` (const)
- `PRESS_RELEASE_WIRE_RE` (const)
- `YAHOO_PRESS_RELEASE_METHOD` (const)
- `NEWS_RELEVANCE_RANK` (const)

## 7. Stayed in `yahoo-finance.ts` although the news block references them

Functions inside the news block's line range that the topology does not assign to `#news` (reached from more than one domain), so they were not moved: `finnhubEligibility`, `resolveNewsCompanyIdentity`, `normalizeCompanyWebsite`, `companyHostRoot`, `clampInt`. Their state (`companyIrIdentityCache`, `COMPANY_IR_IDENTITY_CACHE_MAX`, `COMPANY_IR_DISCOVERY_TTL_MS`, `newsSourceCapabilities`) stays with them. The `// ─── Public news / event helpers ───` banner (before `_str`) also stays, because `_str`, `clampInt`, `NewsCompanyIdentity` and the identity helpers it introduces stay.

