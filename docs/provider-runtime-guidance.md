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

## Price And Quote Calculations

- RSI-14 (Wilder: averages seeded by the first 14 changes, then smoothed by
  1/14) and MACD(12, 26, 9) (EMAs seeded by their first value) run on at least
  a year of completed daily bars; shorter `period` values are widened to `1y`.
  Both are recursive averages, so on the 64 bars of `3mo` a single extra bar
  moved RSI by 0.7 points. `lookbackPeriod` and `lookbackBars` report the input.
- 30-day volatility is the sample standard deviation of the last 30 daily log
  returns, times sqrt(252), in percent.
- The liquidity gate passes when the 20 sessions before the latest completed
  one averaged at least $10M traded a day (volume x close). Non-USD listings
  are converted with Yahoo's `<ISO>=X` rate (ISO units per USD); GBp, ZAc and
  ILA prices are in 1/100 of their currency. `ratio20d` still compares the
  latest session with the average but no longer decides the gate, and
  `foreign_exchange` is accepted for compatibility only.
- The options summary and flow scan read the first expiry after today's US
  market date: the chain expiring today is mostly 0DTE contracts and quotes
  zero bids before the open. `skippedExpiries` lists what was passed over.
- The Worker and local server share these formulas; scripts/test_quote_calculations.py
  pins them to recorded AAPL bars (RSI 61.36, MACD 6.0381 / 5.2354 / 0.8027,
  volatility 20.2718) in both runtimes.
- A tool error that carries only a message is classified from it: a provider
  429 is a retryable `RATE_LIMIT` and a timeout a retryable `PROVIDER_TIMEOUT`,
  as for thrown errors.

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

## SEC Filing Text Search

- `search_sec_filing_text` searches the text a reader sees. Each document is
  projected once to display text: hidden inline-XBRL data (`ix:header`),
  scripts, styles and `display:none` blocks are dropped, entities are decoded,
  paragraphs and table rows end in a line break, and table cells are joined by
  ` | ` (a "$" or "%" cell joins its value). Projections are cached per
  isolate (Worker: 30 minutes, 24 documents, 12 million characters of text;
  local server: the same limits).
- Matching runs on a folded copy of the same length: lowercase, one apostrophe,
  one double quote and one dash, so "Company's" finds "Company’s" and
  "supply chain" finds "supply‑chain". Terms match whole words and their
  plural or possessive; `"quoted"` terms are exact; `match="substring"` keeps
  the old substring behaviour.
- `search_query` takes `"exact phrase"`, `A NEAR/n B` (single words or quoted
  phrases on each side) and `-excluded`; other consecutive words form one
  phrase, so a query without operators is a single phrase as before.
  `exclude_terms` and `near` are the structured forms. An excluded term drops
  each hit whose sentence (or table row) holds it.
- Hits in one passage merge into one match that lists every term found there.
  A match's context is its sentence grown by whole neighbouring sentences of
  the paragraph up to `context_chars`; a short heading line takes the
  paragraph after it; a table match is its row, with `rowLabel`, `tableIndex`
  (as `list_sec_filing_tables` numbers tables) and `tableTitle`.
- `order="relevance"` (default) ranks risk factors and MD&A first and takes one
  match per term in turn, so a common term cannot fill the page;
  `order="document"` keeps filing order. `max_matches` (up to 50) and `cursor`
  page through every match; `termStats`, `hitsBySection` and `totalMatches`
  cover all of them.
- `filing_count` (up to 5) or `since` searches the latest filings of the form
  and reports `filings[]` with hits per filing; `include_exhibits` adds up to
  four EX-99 exhibits per filing (for 8-Ks, the press release). With
  `return_tables`, a table match carries only its own table's rows.
- Matches outside every Part and Item (the cover and forward-looking
  statements) are labelled with the nearest heading line above them, else
  `Front matter`. An exhibit is labelled `EX-99.1: <description>`, or just
  its type when the filing index repeats the type as the description.
- The Worker and local server share one algorithm (`worker/src/filing-search.ts`
  and `yfmcp/filing_search.py`); `scripts/test_filing_search.py` requires
  identical output from both on shared fixtures.

## Capital Structure, Dilution And Analyst Methods

These tools answer questions that consensus feeds leave open (forward diluted
shares, net debt, how a target was built) with disclosed evidence only. None
of them forecasts, and none may be back-solved into a consensus figure.

- The inline-XBRL parser (`worker/src/capital-structure.ts`,
  `yfmcp/capital_structure.py`) reads the filing's own contexts, including
  their dimensions, which the companyfacts API drops. That is what makes
  per-instrument debt terms and per-class warrants reachable. It reads
  `ix:nonFraction` with scale, sign, and the zero-dash and number-word formats,
  and short `ix:nonNumeric` facts such as maturity dates. It skips text blocks.
  It keeps each fact's `decimals`. When a concept is tagged more than once on
  the same date, the most precise fact wins, so a balance-sheet line beats a
  rounded figure in a note.
- `extract_dilution_bridge` takes a `price` the caller supplies. It adds up:
  - basic shares from the cover page (summed across share classes);
  - options by the treasury-stock method, per exercise-price range when the
    filing tags ranges;
  - unvested RSUs and PSUs, counted gross. Without a total, the breakdown on
    the fewest award or plan axes is summed, so a type-by-plan split is not
    counted twice. Without the us-gaap concept, a company-prefixed nonvested,
    outstanding or vested-and-expected-to-vest count is used (`countBasis`
    says which). With no tagged count at all, the latest "Unvested at ..." row
    of the filing's RSU table is read and quoted (`countBasis:
    filing_table_text`);
  - warrants by the treasury-stock method, per class. The count is the
    outstanding warrants, else the shares the warrants call for. When the
    filing tags an unvested portion (a customer warrant that vests with
    purchases), only the vested shares are exercisable; the rest count in the
    gross total;
  - convertibles if-converted, when the price is at or above the conversion
    price (the ratio is per $1,000 of principal). The principal is the tagged
    face amount, else the issue's tagged carrying amount; `principalBasis`
    says which one was used;
  - ATM capacity, as the stated unsold remainder divided by the price, kept
    outside the main total.

  `filing_type=latest` reads the newest 10-Q. Anything that 10-Q does not tag
  is taken from the latest 10-K. Every component names the filing it came
  from. `notDisclosed` means "not tagged", not "does not exist".
  `reportedEpsDilution` gives the company's own weighted basic and diluted
  EPS share counts, plus the securities it excluded as antidilutive (on
  `AntidilutiveSecuritiesAxis`, else on the single axis the filer used), as a
  cross-check. When a component is missing, `taggedDilutionConcepts` lists the
  share-count concepts the filing does tag, each with its axes.
- `extract_capital_structure` reports, at the filing's period end:
  - cash, short-term investments and total debt, with the concepts used and
    the balances' currency. Short-term investments are the tagged total
    (`ShortTermInvestments`, `MarketableSecuritiesCurrent`); without one, the
    current available-for-sale, held-to-maturity and other short-term
    investment lines are added (VRT tags only its held-to-maturity Treasury
    bills).
    Convertible notes reported on their own balance-sheet line are added to
    the long-term debt lines when they exceed them, so they cannot already be
    inside (AAOI);
  - net cash, defined as cash plus short-term investments minus debt (leases
    excluded);
  - investments and debt figures rounded 100 times more coarsely than the cash
    line (`decimals` two or more lower) are left out, with a
    `ROUNDED_FACT_IGNORED` warning. They are note sentences, not balance-sheet
    lines: ASTS tags "approximately $2.3 billion ... classified as cash
    equivalents" as short-term investments, which counted part of cash twice;
  - an investments figure tagged in a sentence (not a table) that calls it cash
    equivalents or money-market funds, and is no larger than cash, is part of
    cash and is not added (`OVERLAPS_CASH_EQUIVALENTS`, with the sentence). The
    parser keeps that sentence only for investment concepts;
  - when the filing tags its own cash-and-short-term-investments total and it
    equals cash alone, tagged investments are already inside cash and are
    dropped; any other disagreement is flagged (`CASH_AGGREGATE_MISMATCH`);
  - a filing that tags cash but no borrowing concept at any date or dimension
    reports total debt as zero, with a `NO_BORROWINGS_TAGGED` warning, so a
    debt-free company (AEHR) is not given Yahoo's lease-inclusive debt;
  - each `DebtInstrumentAxis` member's face amount, carrying amount, coupon,
    maturity and conversion terms. The carrying amount is the period-end
    balance only; an amount tagged on another date, often the issue date, is
    reported as `taggedAmount` with its date. A member that has only a coupon
    is dropped, since it usually duplicates another member;
  - the tagged maturity ladder, and instrument maturities by year;
  - the company's own funding, runway, going-concern and ATM sentences, quoted
    from the filing. A sufficiency sentence must mention cash, liquidity or
    funding. A capital-expenditure sentence must state an amount or a plan.
    Items from a list (ending in ";") are left out.
- `extract_analyst_valuation_methods` reads recent news headlines and
  summaries. It extracts multiples (value, metric, EV basis, periods such as
  `2H27`), DCF inputs (WACC, discount rate, terminal growth, exit multiple),
  sum-of-the-parts and rNPV, together with the firm and price target. Price
  targets from news and from rating changes that carry no stated method are
  listed under `methodNotDisclosed`. "$92 price target" is read before
  "price target of $92", and a number followed by "%" is never a target. The output is `decisionUse=CONTEXT_ONLY`.
  Only the headline and summary are read, not the research note.
- `scripts/test_capital_structure.py` requires identical output from both
  runtimes. It also drives the tools end to end against a mocked SEC and a
  mocked Companies House.

## Valuation Snapshot And Peer Multiples

- `get_valuation_snapshot` values one ticker at the current Yahoo price or a
  `price` you supply:
  - diluted shares come from `extract_dilution_bridge` at that price, and
    cash, short-term investments and debt from `extract_capital_structure` at
    the filing's period end;
  - enterprise value = price x diluted shares + debt - cash - short-term
    investments. In-the-money convertibles counted as shares are removed from
    the debt (capped at total debt), so they are not counted twice;
  - EV/Revenue, EV/EBITDA and P/E over Yahoo's trailing results and its
    current and next fiscal-year consensus, each with the analyst count. A
    zero or negative denominator leaves the multiple empty, with a note;
  - ATM capacity is reported beside the share count, not added to it;
  - the filing's share counts are ordinary shares. For a 20-F or 40-F filer
    whose count is within 2% of a whole multiple (2x or more) of Yahoo's, the
    quote is for depositary shares: counts are divided by that ratio
    (`ordinarySharesPerQuotedShare`, `ADR_RATIO_APPLIED`; TSM is 5). Otherwise
    counts more than 1.5x apart fall back to Yahoo's (`SHARE_BASIS_MISMATCH`);
  - balances in another currency than the quote are never added to equity.
    Filing balances in another currency are not used (`SEC_BALANCES_CURRENCY`),
    and when Yahoo's financial currency differs from the quote, enterprise
    value is empty with `ENTERPRISE_VALUE_CURRENCY_MISMATCH` and status
    `PARTIAL`. The peer basis and `compare_peer_valuations` rows do the same;
  - filing balances more than 45 days older than Yahoo's latest quarter
    (`mostRecentQuarter`) give way to Yahoo's newer cash and debt, with
    `SEC_BALANCES_STALE` naming both dates. 20-F filers such as TSEM and NBIS
    tag only their annual report, so this is common for them;
  - `SEC_YAHOO_CASH_SHORTFALL` warns when, for the same quarter, Yahoo's cash
    and short-term investments are more than 5% above the filing's: an
    investment line may be tagged under a concept not read;
  - `SEC_YAHOO_CASH_MISMATCH` warns when the filing's cash alone is within 2%
    of Yahoo's total cash (which includes short-term investments) but cash
    plus the filing's investments is more than 10% above it: the investments
    are probably inside cash already. It is a warning only; the filing's
    balances still set enterprise value.

  Listings quoted in a minor unit (GBp, ZAc, ILA) are valued in the major
  currency. Non-USD listings and tickers without SEC filings use Yahoo's
  shares, cash and debt, and say so in `warnings`. `peerComparableBasis`
  gives the same ticker on the peer basis below.
- `compare_peer_valuations` puts up to 10 tickers on one Yahoo basis (price x
  shares + total debt - total cash) with the same multiples. Shares are
  Yahoo's implied all-class count when it is more than 2% above the listed
  class, so Up-C and multi-class issuers such as ASTS count their exchangeable
  classes; `shareBasis` says which count was used. The rows also carry the same
  revenue growth and gross margin. Peer medians, minimum and maximum exclude
  the subject, and `subjectVsPeerMedian` gives its premium or discount to
  each median. A ticker whose financials are in another currency than its
  quote has no multiples rather than wrong ones.
- Both are `decisionUse=CONTEXT_ONLY_NOT_A_PRICE_TARGET`: no implied price, no
  forecast, nothing back-solved from price targets.
- `worker/src/valuation.ts` and `yfmcp/valuation.py` share one algorithm;
  `scripts/test_valuation.py` requires identical output and drives both tools
  end to end against mocked Yahoo and SEC.

## Extraction Correctness (2.4.4)

A QA sweep of all 85 actions found numbers returned with confident labels
that belonged to another period, company or metric. Each rule below now
proves what a number belongs to before returning it.

- SEC facts: equivalent us-gaap concepts are all read and the one filed most
  recently wins (`worker/src/sec-facts.ts`, `yfmcp/sec_facts.py`). ASTS moved
  revenue from `RevenueFromContractWithCustomerExcludingAssessedTax` to
  `...IncludingAssessedTax` after 2023, and the first-concept rule returned a
  2022 quarter as "latest". `extract_sec_filing_fact` honours
  `accession_number`: a pin with no matching fact returns
  `NO_FACT_FOR_ACCESSION`, never a fact from another filing. Within a filing
  the newest period end comes first in both runtimes, so a 10-Q's prior-year
  comparative is not returned as the quarter.
- Issuer CIK: the ticker's CIK is used for exhibits and transcripts; an
  accession prefix names the filer agent (0001193125 is Donnelley) and is only
  a fallback.
- Text rules shared by both runtimes (`worker/src/extraction-rules.ts`,
  `yfmcp/extraction_rules.py`):
  - customer concentration reads "<customer> accounted for / represented X%
    of revenue (or net sales)" only. Named customers keep their name, unnamed
    ones are `Unnamed customer`, and "our top ten customers" is an aggregate.
    Receivable shares, thresholds ("more than 10%", "10% or more") and prior
    years are excluded; the first percentage of a "respectively" list is the
    first year listed (AAOI FY2025: Digicomm 53.1%, Microsoft 28.8%, top ten
    96.6%);
  - guidance reads "revenue guidance of $X to $Y" as well as "expects revenue
    of $X to $Y" (ASTS FY2026 $150M to $200M);
  - a reported release metric must follow its own label within the same
    sentence or bullet, with a result verb and no guidance, award, backlog or
    order wording (ASTS Q2 2026 revenue $31.5M, not the $125M award value);
  - event queries match stemmed words ("launch" matches "launched"), and
    evidence is ranked by source confidence, then recency.
- Analyst targets: `extract_analyst_valuation_methods` resolves the company
  name and keeps a target only when its clause names the subject, or the item
  names the subject and no sentence gives another company's target
  (`subjectMatch`, `attribution.rejectedForOtherCompany`). Cantor's $122
  Rocket Lab target is no longer attributed to ASTS.
- Earnings surprise: Yahoo's `surprisePercent` is a decimal ratio at every
  size and is always multiplied by 100 (ASTS's -233.7% had been read as -2.3%).
- Ratios: when the quote and financial currencies differ, P/S, P/B,
  EV/revenue, EV/EBITDA and the free-cash-flow yield are withheld
  (`withheldMultiples`, `CURRENCY_MISMATCH_MULTIPLES_WITHHELD`); P/E stays.
- Options flow window: `ivVsRealizedRangePct` places current ATM IV in the
  one-year min-max range of rolling 30-day realized volatility, and
  `putVolPer1PctStockAdv` divides put contracts by 1% of the stock's 10-day
  average volume. `ivPctile` and `putVolVs10dAvg` remain one release as
  aliases, explained in `fieldNotes`.
- Valuation snapshot: `secVsYahooSharesDiffPct` is taken after any ADR ratio
  and against the Yahoo count the engine uses, and capital-structure warnings
  travel with the filing balances (`source: extract_capital_structure`).

## Provenance Follow-ups (2.4.5)

- `verify_company_event` matches against every item collected in the date
  window, before the 50-item display cap. ASTS's official "Successful Orbital
  Launch of BlueBirds 11, 12, and 13" release had been dropped behind newer
  headlines before matching ran.
- A failed accession pin (`NO_FACT_FOR_ACCESSION`) reports the requested
  accession as `accessionNumber` and `requestedAccession`, never the latest
  filing's accession.
- `get_sec_filing_intelligence` reads XBRL facts from the filing itself for
  any form (`filingFactInAccession` / `filing_fact_in_accession`: latest period
  end, then the shortest duration), so a 10-Q gives its quarter with
  `periodStart`. Revenue uses the shared `REVENUE_CONCEPTS`, and
  `exhibits_count` counts the filing index's exhibits (ASTS Q2 2026 10-Q:
  revenue $31.52M for 2026-04-01 to 2026-06-30, 7 exhibits).
- HTML stripping joins inline tags (`span`, `font`, `b`, `i`, `ix:*` and
  similar) without a space, as a browser renders them, so a heading split
  across spans reads "FINANCIAL", not "FINANC IAL". Block tags still separate.

## Non-US Primary Filings

- `get_uk_company_filings` reads Companies House, the UK statutory registry:
  - accounts, SH01 share allotments, MR01 charges (secured lending) and
    resolutions, with document links;
  - the charge register, when `include_charges=true`.

  It resolves the company by `company_number`, then `company_name`, then the
  ticker's issuer name, which must match exactly (legal suffixes ignored). An
  ambiguous name returns `COMPANY_NOT_MATCHED` together with the candidates.
  It needs the `COMPANIES_HOUSE_API_KEY` secret, a free key from the Companies
  House developer hub. Without the key it returns `SOURCE_UNCONFIGURED`. The
  API allows 600 requests per 5 minutes; a 429 returns a retryable
  `RATE_LIMIT`.
- Companies House does not hold RNS market announcements (results, loan-note
  terms, trading updates). The FCA National Storage Mechanism has only an
  undocumented search endpoint, so it is not used; this repository reads only
  documented official APIs.
- IQE.L and SIVE.ST are in the IR-page registry as `candidate` entries. At
  runtime a candidate is reported and never fetched. Promote an entry only
  after confirming that the page lists the issuer's regulatory announcements.

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
