# Provider And Runtime Guidance

This project is Worker-first for the public MCP endpoint. Before adding a data
provider, parser dependency, cache path, or deployment path, check the current
official provider/runtime docs and record the consequence in the PR.

Sources checked through 2026-08-04:

- SEC EDGAR APIs: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
- SEC fair access guidance: https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data
- Cloudflare remote MCP server guide: https://developers.cloudflare.com/agents/model-context-protocol/guides/remote-mcp-server/
- Cloudflare Workers limits: https://developers.cloudflare.com/workers/platform/limits/
- Cloudflare Cache API: https://developers.cloudflare.com/workers/runtime-apis/cache/
- yfinance Ticker API: https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.html
- Finnhub API documentation: https://finnhub.io/docs/api
- Alpha Vantage API documentation: https://www.alphavantage.co/documentation/

## Scarce Optional Market Providers

- Yahoo remains the default for current options snapshots and ordinary holder
  views. Do not spend Alpha Vantage quota to duplicate those workflows.
- Finnhub may fill deeper ownership or company-news gaps only when the deployed
  capability policy marks the market eligible. Entitlement and authentication
  failures must remain explicit provider statuses.
- Alpha Vantage is an explicit scarce fallback. Historical put/call calls
  require a date; ownership fallback requires caller opt-in. Do not retry Alpha
  automatically or call it from routine deployed canaries.
- Successful Alpha results are contextual (`decisionGrade:false`) and expose
  `capacityClass:"SCARCE_SHARED_QUOTA"`. Transcript, historical options, and
  ownership caches use different TTLs based on data mutability.
- A Finnhub "no access" answer (entitlement or authentication) is remembered
  per endpoint for 6 hours, and Alpha Vantage's daily-limit answer until the
  next UTC day, so repeated calls are not spent on a known refusal.
- The Alpha Vantage free key allows 25 requests a day and is shared by every
  client that uses it; the deploy canaries and smokes never call Alpha.
- Worker Cache API storage is best effort and data-center local. It reduces
  duplicate requests but is not global quota accounting or a persistent rate
  limiter.

## Yahoo Finance Caching

- Yahoo throttling (HTTP 429) is handled as yfinance 1.7 handles it
  (`yfinance/data.py`, `_make_request` and the basic/csrf crumb strategies):
  a throttled crumb request is retried on the other API host
  (`query2`/`query1`); if both refuse, the call proceeds without a crumb and
  crumb requests pause for 60 s. A throttled data request is retried once,
  after 750 ms, on the other API host. When both hosts throttle, or an endpoint
  needs the missing crumb, the tool returns `RATE_LIMIT` with
  `retryable: true`. yfinance also impersonates a browser TLS fingerprint
  (`curl_cffi`), which a Worker cannot do.

- Every Worker Yahoo GET shares in-flight requests and a 30-second
  process-local body cache, so composite tools that fan out to the same URL
  make one upstream call.
- Slow-changing Yahoo data is also stored in the Worker Cache API, so new
  isolates in the same data center reuse it: fundamentals timeseries
  (statements, valuation history) for 6 hours; profile, fund-profile,
  holdings, and ownership quoteSummary modules for 6 hours; analyst
  recommendations and rating changes, earnings trend/history, calendar events,
  and SEC filing lists for 1 hour. The allow-list is `yahooEdgeTtlMs` in
  `worker/src/yahoo-finance.ts`.
- A quoteSummary request is edge cached only when every requested module is on
  that list, so anything carrying a live price or quote keeps the 30-second
  cache. Error responses and empty Yahoo answers are never edge cached.
- Timeseries requests end their window at the current second (`period2`); the
  cache key ignores that parameter so repeated calls can hit.
- The Worker's 30-second body cache holds at most 200 bodies and 24 million
  characters in total. `/health` reports per-isolate `yahooCache` counters
  (memory hits, shared in-flight requests, edge hits, misses and writes, and
  upstream fetches), and the deployed discovery smoke prints them. Counting
  starts at the isolate's first request (`since`).
- Every `/mcp` response carries an `X-Yahoo-Cache` header with that request's
  own counts, for example `memory=0, shared=0, edge-hit=1, edge-miss=0,
  edge-write=0, upstream=0`. Requests are attributed with `AsyncLocalStorage`
  (the `nodejs_als` compatibility flag), so concurrent requests in one isolate
  do not mix. The deployed discovery smoke reads a statement twice, 31 s apart,
  and reports whether the second read came from the edge cache.
- `meta.cacheHit` and `meta.cacheSource` summarize each tool call's own cache
  use. Every provider request goes through `providerFetch` in
  `worker/src/request-context.ts`, and the Yahoo GET, SEC archive document,
  and scarce-provider caches report their hits there; a call is a cache hit
  only if it read cached data and made no provider request.

## SEC Archive Document Caching

- The Worker's filing tools that read a document by URL (outline, section,
  table list, table) share one cache for `https://www.sec.gov/Archives/`
  documents: 10 minutes in the isolate (at most 12 documents and 16 million
  characters), 24 hours in the Worker Cache API, and one SEC request for
  concurrent reads of the same document. Archive documents do not change once
  filed. Failed fetches are never cached.
- Filing documents are read in full up to 12 million characters through that
  cache (large inline-XBRL 10-Ks exceed 5 MB); scan coverage reports
  `filingReadTruncated` when a filing is longer.
- Inline-XBRL filings mark Part and Item headings with bold spans rather than
  `<h1>`-`<h6>`; the outline, section, index and text-search tools read those
  headings, skipping table-of-contents entries.
- Segment revenue comes from XBRL segment facts when present; the
  `companyconcept` API carries none, so the filing's own table introduced as
  segment revenue is parsed, and its rows must sum to the table total.
  `NOT_DISCLOSED` is returned only with the scan that supports it.
- `/health` reports per-isolate `secDocumentCache` counters, and every `/mcp`
  response carries an `X-Sec-Cache` header in the `X-Yahoo-Cache` format. Its
  memory count also includes the isolate's CIK and submissions caches.
- The local server caches EDGAR archive documents (`https://www.sec.gov/Archives/`)
  for 30 minutes in a 64-million-character budget and shares in-flight fetches
  across concurrent tool calls. Submissions are cached for 24 hours, so new
  filings appear within a day; company facts (16) and submissions (256) are
  size-limited.

## SEC EDGAR Rules

- `data.sec.gov` is keyless and public. Do not add API-key or paid-provider
  requirements for structured SEC facts unless a concrete fixture proves the
  official path cannot support the needed contract.
- SEC `companyfacts` and `companyconcept` APIs aggregate standardized
  non-custom taxonomy facts that apply to the whole filing entity. Use them for
  total revenue and other comparable entity-level facts.
- Do not expect official XBRL JSON alone to solve company-specific geography,
  product, customer, or segment tables. Use the Worker filing index / HTML table
  fallback for those cases, and return explicit limitation statuses when parsing
  fails.
- Do not collapse provider or parser limitations into clean `NOT_DISCLOSED`.
  Clean `NOT_DISCLOSED` requires filing metadata, scan coverage, searched terms,
  a non-disclosure basis, and no `TABLE_NOT_PARSED` warning.
- Use a declared SEC User-Agent for scripted access. Keep request patterns
  efficient and cache repeated submissions/companyfacts/index fetches.
- `data.sec.gov` does not support CORS. Browser/dashboard features should call
  the MCP/backend surface, not fetch SEC APIs directly from client JavaScript.
- Treat SEC fair-access guidance as a hard design constraint. The public
  guidance currently lists a maximum request rate of 10 requests/second; code
  should stay comfortably below that and avoid retry storms on `429`.
- For broad or repeated SEC data sweeps, prefer cached data or SEC bulk archives
  over repeated live per-company API calls.

## Cloudflare Worker Rules

- Keep the public MCP endpoint stateless unless a feature truly needs per-user
  session state. Cloudflare's Remote MCP guidance lists `createMcpHandler()` as
  the simplest fit for stateless tools; Durable Object-backed `McpAgent` is for
  stateful/session use.
- Do not add sidecars or alternate public deployment paths by default. Add them
  only when Worker limits or provider rules block a verified, decision-grade
  contract.
- Design SEC parsing for Worker limits: avoid large in-memory DOMs, avoid broad
  fanout in one tool call, and do not assume many simultaneous upstream fetches.
- Treat Cloudflare limits as runtime design inputs, especially CPU time, 128 MB
  memory, subrequest limits, and 6 simultaneous outgoing connections/request.
- Prefer compact contract checks in deploy canaries. Keep broad parser-quality
  sweeps as audit jobs unless the response contract itself is at risk.

## PR Preflight

Provider/runtime PRs should answer these before implementation:

- Which official provider/runtime docs were checked?
- Does the change alter public MCP tool names, schemas, response envelopes, or
  diagnostic fields?
- Does it increase live SEC or Yahoo request volume?
- What is cached, for how long, and what happens on rate limit or provider
  outage?
- Which blocking canary or audit smoke would catch a broken deploy?
