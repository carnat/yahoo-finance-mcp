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
- One busy session can spend the whole free quota: the 2.25.0 audit
  (2026-10-20) used it up by about 10:10 UTC (F44). Until the next UTC day these
  degrade, with explicit statuses rather than errors: the curve cross-check
  (`CROSS_CHECK_DEGRADED`), the expanded-ownership fallback, the historical
  put/call ratio, and the last transcript fallback. Yahoo-backed results are
  unaffected.
- On the current Finnhub plan the ownership endpoint answers 403
  `ENTITLEMENT_REQUIRED`, so `get_expanded_institutional_ownership` has no
  source left once the Alpha quota is spent. The owner chose to keep the free
  keys and document the limit (2026-10-21).
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

## Regression Locks (2.4.6)

- The response envelope drops evidence rows with no populated field. A
  fail-closed result such as `NO_FACT_FOR_ACCESSION` returns `evidence: null`
  rather than one row of nulls; its provenance is `accessionNumber`,
  `requestedAccession`, `code` and the warning.
- `scripts/test_release_regressions.py` locks the provider-layer fixes of
  2.4.2 to 2.4.5 in both runtimes, offline:
  - a failed pin echoes the requested accession and skips the latest-filing
    lookup;
  - currency-mixed multiples are withheld and P/E is kept;
  - `surprisePercent` is scaled at every size;
  - exhibits use the issuer's CIK;
  - event verification matches before the display cap;
  - the options flow fields keep their descriptive names.

  The shared modules keep their own parity tests (`test_capital_structure`,
  `test_valuation`, `test_extraction_rules`).

## Evidence Cuts And Consensus (2.5.0)

MCP supplies reproducible facts and mechanical transformations. Valuation
method, selected multiple, scenario weights, price target, G2, opportunity and
capital action belong to the caller's doctrine. The `evidence` group and the
consensus actions compose evidence and never fill a gap with an assumption.

- Authority boundary: every evidence payload carries `decisionUse:
  EVIDENCE_ONLY` and `selectedMethod`, `selectedMultiple`, `scenarioWeights`,
  `priceTarget`, `g2`, `opportunity` and `action`, always null. No other
  field stands in for them, and no provider value is selected, blended or
  averaged.
- `get_consensus_forecast_curve`:
  - Reports FY0 to FY+horizon (1 to 5 years) for EPS and revenue. Each
    provider (Yahoo Finance, Alpha Vantage) is reported separately, with:
    - fiscal year end and its basis;
    - currency and its basis;
    - mean, high and low;
    - analyst count;
    - retrieval time.
  - FY0 is Yahoo's current fiscal year (`0y`). The Worker reads the end date
    Yahoo states. The local server derives it from `nextFiscalYearEnd`,
    labelled `DERIVED_FROM_NEXT_FISCAL_YEAR_END`.
  - Every metric and period has a coverage state:
    - `PROVIDER_COVERED`;
    - `PROVIDER_NOT_COVERED`;
    - `INSUFFICIENT_ANALYST_COUNT`, when fewer analysts than
      `min_analyst_count` (default 3);
    - `PROVIDER_CONFLICT`, when means differ by more than
      `conflict_tolerance_pct` (default 10%, or 0.02 per share for EPS),
      currencies differ, or fiscal year ends differ by more than 10 days.
  - Today both providers stop at FY+1. FY+2 to FY+5 stay
    `PROVIDER_NOT_COVERED`: nothing is interpolated or extended from
    long-term growth rates. EBITDA, EBIT, FCF, capex and margins are listed
    as not covered rather than derived.
  - The only dispersion measure is each provider's high-low range; median
    and standard deviation are unavailable.
  - Agreement does not establish independence: live ASTS figures from both
    providers match to the digit.
- `get_eps_revisions`:
  - Reports each provider's FY0/FY+1 EPS mean now and 7, 30, 60 and 90 days
    ago, with change and percent change.
  - Also reports up/down revision counts over 7 and 30 days.
  - Unreported fields are listed in `notReported`.
  - Revenue revisions and analyst adds/drops are `PROVIDER_NOT_COVERED`.
- Provider interface: providers enter as `ProviderConsensusInput` rows
  (`evidence.ts` / `evidence.py`). A paid estimates source can be added as
  one more adapter without changing the curve or its states. Qualify it for:
  - FY+2 to FY+5 availability and metric coverage;
  - analyst counts;
  - currency and fiscal-period identity;
  - dispersion;
  - timestamps and provenance.
- `get_evidence_quality`: a preflight from light requests only (quote, SEC
  submissions, consensus coverage, storage). It gives a state, freshness and
  blockers for each of:
  - quote;
  - latest periodic filing;
  - capital structure and dilution readiness;
  - earnings-release guidance;
  - material 8-Ks;
  - FY0/FY+1 consensus cells;
  - storage.
- `build_valuation_evidence_pack`:
  - Composes the canonical tools rather than reimplementing them. The
    components are:
    - quote;
    - `evidenceQuality`;
    - `consensus`;
    - `epsRevisions`;
    - `currentCapitalStructure`;
    - `currentDilution` (the bridge at the current price; `NOT_APPLICABLE`
      for non-USD listings);
    - `latestGuidance`;
    - `materialEvents`.
  - Each component keeps its source tool, status (`OK`, `LIMITED`,
    `FAILED`, `NOT_APPLICABLE`), warnings, error and payload.
  - `provenance` is the receipt, with:
    - ticker, cutoff, server version, build SHA and runtime;
    - each component's SHA-256, retrieval time, provider timestamps and
      warning codes;
    - coverage (`COMPLETE`, `PARTIAL` or `FAILED`).
- Evidence cuts:
  - Hashing: the pack without `evidenceCut` is serialized as canonical JSON
    (sorted keys, no whitespace, ECMAScript number formatting; identical in
    both runtimes). Its SHA-256 is `contentSha256`.
  - Identity: `evidenceCutId` is `ec1_<TICKER>_<YYYYMMDDTHHMMSSZ>_<sha256>`,
    stored at `evidence-cuts/<TICKER>/<timestamp>/<sha256>.json`.
  - Retrieval: `get_evidence_cut` recomputes the hash of the stored bytes and
    reports `integrity: VERIFIED` or `MISMATCH`. `list_evidence_cuts` lists a
    ticker's cuts, newest first.
  - Envelope: evidence payloads pass through the response envelope
    unchanged, so the returned fields are the hashed fields.
- Storage:
  - The Worker uses the private R2 bucket `yfmcp-evidence`, bound as
    `EVIDENCE_BUCKET`. The local server uses `YFMCP_EVIDENCE_DIR`.
  - Objects are written once.
  - The first consensus curve of each day is kept at
    `consensus-history/<TICKER>/<yyyy-mm-dd>.json`, to build revision history
    the providers do not publish.
  - Storage is never a hard dependency. Without it, the pack returns in full
    with its receipt and `storageStatus: UNAVAILABLE`. Only retrieval and
    history are unavailable.
  - No bucket URL or credential appears in a response.

## Share Scenarios, Funding Schedule And Unread Sources (2.5.2)

- `get_share_count_scenarios` (group `sec_extractors`) takes up to 8 caller
  scenarios. Each scenario has a `name`, a `price`, and one treatment per
  instrument:
  - options: `treasury_stock`, `gross` or `exclude`;
  - unvested awards: `gross` or `exclude`;
  - warrants: `treasury_stock`, `gross` or `exclude`, on vested or all;
  - convertibles: `if_converted_when_in_the_money`, `if_converted_all`,
    `net_share_settlement_when_stated` (2.5.11) or `exclude`;
  - capped calls: `ignore` or `offset_when_stated` (2.5.12);
  - ATM: `exclude` or `full_remaining_capacity`;
  - optional `known_issuance` rows.

  The instrument inventory is the dilution bridge's company-disclosed inline
  XBRL.
  - Every instrument line reports its treatment, whether it is included, its
    mechanics (count, exercise or conversion price, threshold price, method)
    and why it is unresolved when it is.
  - Unresolved instruments are left out of `resultingShares` and listed.
  - Treatments the caller omits take the documented defaults and are listed
    in `defaulted`.
  - Scenarios are reported side by side. `selectedScenario` and
    `selectedDenominator` are always null, and MCP never picks the
    denominator.
- `extract_funding_capex_schedule` (group `sec_extractors`) reads the latest
  periodic filing and classifies every amount:
  - `CONTRACTUAL`: debt principal, operating and finance lease payments,
    purchase and contractual obligations, tagged by due bucket with
    `periodThrough`; purchase commitments by category. A range tagged as
    Minimum/Maximum becomes one row with `amountLow`/`amountHigh`.
  - `COMPANY_DISCLOSED_COMMITTED`: text stating committed, non-cancelable or
    obligated amounts. A stated commitment stays a commitment even when the
    sentence also uses a forward-looking word.
  - `COMPANY_GUIDED`: amounts the company expects, plans or budgets.
  - `AWARDED_CONTINGENT`: grants, awards, incentives and milestone payments.
    Equity-compensation wording is excluded.
  - `UNRESOLVED`: a funding or capex statement with no amount, or whose type
    the wording does not state. Text without an amount is kept only when it
    names an obligation outright; list items ending in `;` are skipped.

  Timing is read from the sentence (quarter, next 12 months, remainder of the
  year, a year, a year range, through a year), else `UNSTATED`.
  - An amount below $100,000 with no scale word, such as VRT's "$550.0 to
    $570.0" in a filing reporting in millions, keeps its wording in
    `amountAsWritten` and is `SCALE_NOT_STATED`, not read at face value.
  - Liquidity sources are listed separately and not netted: cash, undrawn
    facilities (`CONTRACTUAL_AVAILABILITY`), and ATM remaining capacity
    (`AVAILABLE_AT_COMPANY_DISCRETION`).
- Unread is not undisclosed:
  - `extract_sec_filing_fact` without a CIK returns `NO_SEC_REGISTRANT`, or
    `SEC_LOOKUP_UNAVAILABLE` (`retryable: true`) when SEC's ticker index could
    not be read. A pinned accession is still echoed.
  - `extract_guidance` returns `RELEASE_NOT_RESOLVED` or
    `RELEASE_TEXT_NOT_AVAILABLE` (`retryable: true`), with each metric
    `NOT_READ`, instead of `NOT_DISCLOSED`. A read release reports `status`
    `FOUND` or `NOT_DISCLOSED` and its `sourceUrl`.
  - The evidence pack retries an unreadable release once. Live on 2.5.1,
    ASTS's pack reported guidance `NOT_DISCLOSED` while a standalone call
    found $150–200M.

## Guidance History And Operating Drivers (2.5.3)

- `get_guidance_history` (group `earnings_intelligence`) reads up to
  `max_releases` (default 8, at most 12) of the newest 8-Ks with Item 2.02,
  results of operations. It reads EX-99.1 when the filing index names one,
  else the 8-K itself.
  - Each revenue, gross-margin and EPS range carries its target period and
    source. The period comes from the range's own sentence or bullet first
    ("expects EPS of $0.10 to $0.20 for fiscal 2027"), else from the 200
    characters before it; `scope` says which (`SENTENCE`,
    `PRECEDING_TEXT`). SEC-rendered " o " bullets are sentence breaks.
  - Quarters and halves win over the fiscal year inside them: "first quarter
    of fiscal 2026" is `Q1 2026`, "second-half 2025" and "2H25" are
    `H2 2025`. A range with no stated period is listed but not compared.
  - `revisions` compare consecutive releases for the same metric and period:
    `INITIATED`, `RAISED` or `LOWERED` (midpoint), `NARROWED` or `WIDENED`
    (same midpoint), `REAFFIRMED` (same range), and `WITHDRAWN`.
  - `outcomes` compare the later reported actual (SEC companyfacts, newest
    10-K/10-Q filing per period) with the first and last range: `BELOW`,
    `WITHIN` or `ABOVE`. Statuses:
    - `EVALUATED`;
    - `ACTUAL_NOT_YET_REPORTED`;
    - `NOT_EVALUATED_METRIC` (gross margin);
    - `NOT_EVALUATED_FISCAL_QUARTER_MAPPING` (quarters of a non-calendar
      fiscal year);
    - `NOT_EVALUATED_HALF_YEAR` (a second half is never filed as a period,
      and FY minus H1 is not derived);
    - `ACTUALS_NOT_READ` (companyfacts unavailable; retryable).
  - An unread release is listed with `status: NOT_READ` and contributes
    nothing; it is never read as a withdrawal. `guidanceFound` counts ranges
    stated in text, so guidance given only in a table is not read.
  - ASTS, live on 2.5.3 locally: `H2 2025` $50–75M `INITIATED` (Aug 2025)
    then `REAFFIRMED` (Nov 2025); `FY2026` $150–200M `INITIATED` (May 2026)
    then `REAFFIRMED` (Aug 2026).
- `extract_operating_driver_ledger` (group `sec_extractors`) reads the
  latest periodic filing, SEC companyfacts and the newest Item 2.02 release.
  - `xbrlSeries`: revenue, gross profit, R&D, capex, remaining performance
    obligations and contract liabilities (total, current, noncurrent).
    - Each point is `QUARTER`, `ANNUAL`, `YEAR_TO_DATE` or `INSTANT`, from
      the newest 10-K/10-Q filing of that period.
    - Year-to-date cash-flow periods stay year-to-date; no quarter is
      derived.
    - `latestPeriodEnd` shows when a company stopped tagging a series (ASTS
      gross profit ends in 2023).
    - A series the company does not tag is `NOT_REPORTED` (`notReported`);
      when companyfacts could not be read it is `NOT_READ` (`notRead`).
  - `companySpecificSeries`: the filer's own non-monetary inline XBRL
    concepts, custom units first (ASTS: patents granted, pending claims).
    - Monetary extension line items are counted in `excludedMonetaryConcepts`.
    - Financing, equity and acquisition concepts are counted in
      `excludedCapitalStructureConcepts`; those belong to the capital and
      dilution tools.
  - `textDrivers`: sentences and bullets with a figure, in categories
    capacity, production, deliveries, launches and deployments, customers,
    backlog and bookings, utilization, pricing, yield and headcount.
    - `basis` is `REPORTED_ACTUAL`, `TARGET_OR_PLAN`, or `UNCLEAR` when the
      sentence does both or neither.
    - Each carries timing (as in the funding schedule) and its source.
    - A number inside a name ("Block 2", "BlueBird 8-13") and a bare year
      are not figures.
    - A search window's cut-off first and last pieces are dropped.
    - Pieces over 600 characters, such as flattened highlight tables, are
      skipped.
- Both tools carry the evidence-only authority fields (`decisionUse:
  EVIDENCE_ONLY`, method, multiple, weights, target, G2, opportunity and
  action null). They return `status` `OK` or `PARTIAL` with warnings for
  each unread source and `retryable`.
- The range rules now also read "expectations for revenue of $X to $Y"
  (ASTS, Aug 2025), in `extract_guidance` too.

## Historical Valuation Context And Metric Reconciliation (2.5.4)

- `get_historical_valuation_context` (group `stock_fundamentals`) takes up to
  12 `dates` (default: the latest close and its anniversaries over five
  years) and up to 5 `peers`, valued on the same dates. Each date is point in
  time:
  - The price is the close on or before the date, within 7 days. Yahoo
    adjusts historical closes for later splits; that adjustment is undone
    (`laterSplitFactor`) so the price matches the share count reported then.
    A split between the share count's date and the price date leaves market
    cap null (`SPLIT_AFTER_SHARE_COUNT`).
  - Shares are a point-in-time count filed by the date: the cover-page count,
    else the balance-sheet count, else (since 2.5.6, below) the filing's
    per-class cover-page counts summed. A weighted-average count never sets
    market cap.
  - Balances are read at the latest cash balance date filed by the date.
    Debt is `LongTermDebt`, else current plus noncurrent, else convertible or
    notes payable, plus short-term borrowings and commercial paper. A concept
    last tagged at an earlier date is not carried forward (ASTS last tagged
    `LongTermDebt` in 2023). Untagged debt leaves EV null (`DEBT_NOT_TAGGED`).
    Untagged short-term investments are left out, with a warning.
  - Denominators: `LFY` is the last reported fiscal year. `LTM` is LFY +
    current year-to-date - prior year-to-date, with every component's
    concept, period and filing. EBITDA is computed as operating income plus
    depreciation and amortization, and is labelled as computed. D&A is taken
    from the concept group that matches operating income's period: VRT tags
    the total only in 10-Ks and depreciation and amortization separately in
    10-Qs.
  - Reporting-currency figures are converted at that date's FX close
    (`<reporting><quote>=X`). For 20-F and 40-F filers whose SEC count is a
    whole multiple of Yahoo's, shares are divided by the ordinary shares per
    ADS (TSM: 5).
  - Multiples: EV/Revenue, EV/EBITDA, P/E (market cap / net income) and P/S
    on both bases. A missing, unfiled or non-positive figure leaves the
    multiple null with a status. Stale inputs fail closed (2.5.5, below):
    TSM's 2026 20-F financials are not in companyfacts, so its 2026
    multiples are null with `RESULTS_STALE`.
  - `peerMedians` are unweighted and count only peers with that multiple on
    that date. The Worker reads one peer at a time to bound memory.
- `reconcile_metric_sources` (group `evidence`) takes `metric` (`revenue`,
  `net_income`, `operating_income`, `eps_diluted`, `cash_and_equivalents`),
  `period` (`latest_quarter`, `latest_annual`, `FY<yyyy>` for the issuer's
  fiscal year since 2.5.7, or `Q<n> <yyyy>` for the calendar quarter of the
  period end) and `tolerance_pct` (default 0.5).
  - Periods come from the company's revenue periods. ASTS stopped tagging
    undimensioned EPS after 2022, so its Q2 2026 EPS is SEC `NOT_FOUND`
    rather than a 2022 quarter.
  - SEC: the value as first filed and as most recently filed for the period,
    from the concept filed most recently for it (the earlier-listed on a
    tie). `otherConcepts` lists the rest: ASTS's `ProfitLoss`, which includes
    noncontrolling interests. A difference is a restatement (`restated:
    true`), not a conflict.
  - Issuer release: Item 2.02 8-Ks filed within 100 days after the period
    (since 2.5.6 the four newest, 8-K/As included); the latest with a figure
    wins. ASTS filed an Item 2.02 8-K on 2026-07-15 before its results
    release. A figure is read only from a sentence or bullet that:
    - names the metric and a dollar amount;
    - is scoped to the period (a quarter alone, a full year alone, or "as of"
      for cash);
    - is not guidance, backlog or awards, and not a different aggregate
      ("cash, cash equivalents, and restricted cash").
  - Yahoo: the quarterly or annual statement row within 7 days of the period
    end.
  - Each found value is compared with the latest SEC value, with difference,
    percentage and tolerance. The tolerance is the larger of `tolerance_pct`
    and half the release's last stated digit ("$31.5 million" is within
    $50,000).
  - `status`: `AGREED` (since 2.5.5: the latest SEC value plus another
    provider agree), `PARTIAL`, `CONFLICT` or `NOT_FOUND`. Alpha Vantage, filing tables and
    Companies House are listed in `sourcesNotCompared` with the reason.
- Both tools carry the evidence-only authority fields. No multiple, peer or
  source is selected.
- The Worker's HTML text now decodes hex entities (`&#x2022;`) and common
  named ones (`&bull;`, `&rsquo;`), as Python's `html.unescape` does, so
  release bullets split in both runtimes. Before this, the 2.5.3 driver
  ledger merged two ASTS bullets on the Worker.
- A year before a word ("2026 revenue") is no longer a driver-ledger figure.

## Evidence Integrity Guards (2.5.5)

- Historical valuation fails closed on stale inputs instead of only warning:
  - a share count older than the limit leaves market cap null
    (`SHARE_COUNT_STALE`);
  - balances older than the limit leave EV null (`BALANCES_STALE`);
  - results older than 500 days leave the multiples they feed null
    (`RESULTS_STALE`).

  So stale peer multiples cannot enter peer medians: NVT's stale-balance EV
  multiples no longer count in VRT's.
- Limits follow the cadence of the facts SEC companyfacts holds (named
  `secCompanyfactsCadence` and set per date since 2.5.6, below;
  `stalenessLimitsDays`):
  - quarterly filers: 400 days for shares, 200 for balances, 500 for
    results;
  - annual-only 20-F/40-F filers: 500 for each. They file balances and
    cover counts once a year, about four months after year end, so a
    200-day limit would leave TSM's EV null on every default date.
- Concept order is precedence per period: a later filing updates the same
  concept, but a lower-priority alternate concept cannot replace it.
- `reconcile_metric_sources`:
  - `AGREED` requires the latest SEC value plus at least one independent
    provider that agrees.
  - Without SEC, the other sources are still compared with each other:
    agreement stays `PARTIAL`, and disagreement is `CONFLICT`.
  - All release candidates (up to three) are read, and the latest one with
    a figure wins.
  - A quarter a sentence names must be the period's calendar or fiscal
    quarter, in its calendar or fiscal year. The period carries
    `fiscalQuarter` and `fiscalYears` (from the issuer's `fy`/`fp` since
    2.5.6). NVDA's quarter ended July 26, 2026 is its fiscal Q2 of fiscal
    2027 but calendar Q3.
  - A sentence naming the period's exact end date ("the second quarter ended
    July 26, 2026") is scoped to it.
  - "Second-quarter" is read like "second quarter".
  - "Quarter of fiscal 2027" and "fiscal 2026 third quarter" are quarter
    scope, not full year.
  - A quarter named only in a comparison clause ("compared to Q2 2025")
    does not decide the scope.
- Driver ledger: a number after a sentence-initial product name ("Block 2
  satellites", "New Glenn 3") is not a figure. A sentence-initial reporting
  or quantity word ("Reported 5", "Approximately 5") still precedes one.
- Not yet covered:
  - historical changes in the ADS ratio (the current ratio is applied to
    every date);
  - defaulting the latest date to a completed session;
  - release tables such as ASTS's Q2 net-income table, which are not
    sentences;
  - a segment bullet under a heading ("Data Center • Second-quarter revenue
    was …"), which reads like a company total when it is the only matching
    sentence.

## Share Denominators, Coverage And Fiscal Identity (2.5.6)

- Historical valuation shares are point in time only:
  - Filers with several share classes tag cover counts per class, and
    companyfacts omits those. When there is no undimensioned cover-page or
    balance-sheet count as new as the latest periodic report filed by the
    date (10-K, 10-Q, 20-F or 40-F, from SEC submissions), the tool reads
    that report's cover page. It reads the first 900 KB, which holds the
    cover and the header contexts. The per-class counts are summed
    (`COVER_PAGE_CLASS_SUM`, with `classes`, and a `SHARE_CLASSES_SUMMED`
    note). Every class is valued at the quoted class's close. ASTS stopped
    tagging an undimensioned weighted-average count after 2022, so the report
    is chosen by filing date, not from companyfacts.
  - ASTS's Q2 2026 10-Q counts Class A 299,789,305, Class B 11,215,111 and
    Class C 78,163,078, a total of 389,167,494. Before this, market cap used
    299,061,662 weighted-average Class A shares and was about 23% low.
  - A read that leaves any class unresolved (context or value not read, or a
    dimension other than class of stock) is not summed.
  - A weighted-average count never sets market cap. Without a point-in-time
    count, market cap and EV are null (`POINT_IN_TIME_SHARES_UNRESOLVED`).
    The weighted-average count is shown for context, with `coverPageRead`.
    Without SEC submissions no cover page is read
    (`SUBMISSIONS_NOT_AVAILABLE`).
- `status` no longer overstates completeness:
  - Each point has `coreStatus` (market cap and EV) and `coverage`: the
    eight multiples requested and available, and each unavailable one with
    its status.
  - `status` is `OK` only when the core values and all eight multiples are
    usable. COHR and MRVL, whose LTM EV/EBITDA is
    `DENOMINATOR_NOT_AVAILABLE`, are now `PARTIAL` with `coreStatus` `OK`.
  - The top level carries both.
- Cadence is named for what it measures and set per date:
  - `reportingCadence` is now `secCompanyfactsCadence`. It is `ANNUAL` for
    20-F/40-F filers, which tag annual periods only; it is not the issuer's
    disclosure cadence. Interim 6-K, IR and exchange disclosures are outside
    this engine.
  - Each point sets its cadence and limits from the annual report filed by
    its own date, so a filer that moved between 20-F and 10-K keeps its
    earlier cadence at earlier dates. The top level is `MIXED` when dates
    differ, and `stalenessLimitsDays` is keyed by cadence.
- `reconcile_metric_sources` fiscal identity:
  - `fiscalQuarter` and `fiscalYears` come from companyfacts' `fy`/`fp` for
    the filing that first reported the period (`fiscalYearSource:
    SEC_FY_FP`). NVDA's quarter ended July 26, 2026 is fy 2027 Q2 only.
    "Second quarter fiscal 2026 revenue" is therefore the prior year and is
    not read as the current quarter. Dollar General's quarter ended July 31,
    2026 is fy 2026 Q2.
  - Without usable metadata, the quarter is counted from the prior year end.
    For a January–March year end both adjacent years are accepted, and
    `fiscalYearAmbiguous` is set.
  - Annual periods also take the issuer's fiscal year: Dollar General's year
    ended January 30, 2026 is fiscal 2025. Before 2.5.7 an explicit
    `FY<yyyy>` still selected by the year the period ends (below).
- More fiscal-quarter wordings are read:
  - Shorthand is spelled out before scoping: `FY27`/`FY2027` becomes fiscal
    2027, `Q2FY27` becomes Q2 fiscal 2027, `Q2'27` becomes Q2 2027 and `2Q26`
    becomes Q2 2026. The evidence keeps the sentence as written.
  - "Q2 fiscal 2027", "fiscal 2027 Q2", "second fiscal quarter" and "fiscal
    second quarter" are quarter scope.
  - A year written just before the quarter ("fiscal 2026 second quarter")
    must match, like one written after it.
- Release candidates: the four newest Item 2.02 8-Ks and 8-K/As in the
  100-day window are read, so a later correction is not crowded out.
  `releaseCandidates` counts those in the window and those read, and lists
  any older ones not read (`OLDER_RELEASE_CANDIDATES_NOT_READ`).

## Issuer Fiscal-Year Selection (2.5.7)

- An explicit `FY<yyyy>` in `reconcile_metric_sources` selects the annual
  period the issuer calls fiscal `yyyy`. That is companyfacts `fy` with `fp`
  `FY`, from the filing that first reported the period, within a year of its
  end. The year the period ends is used only when a period has no such
  metadata.
  - Dollar General: `FY2025` is the year ended January 30, 2026, and
    `FY2024` is the year ended January 31, 2025.
  - Dollar General `FY2026` is `PERIOD_NOT_FOUND` until it is filed, with
    `fiscalYearsAvailable`, instead of returning fiscal 2025 because that
    year ends in 2026.
  - A later 10-K's comparative carries that filing's `fy`. The
    first-reported row names the period, so the comparative does not
    rename it.
  - NVDA and calendar-year filers name a year by its end, so their
    selection is unchanged.
- `latest_annual`, `FY<yyyy>` and the annual `fiscalYears` field share one
  rule, so the period selected and the fiscal year reported for it agree.
- `labelBasis` now states this rule. `Q<n> <yyyy>` stays a calendar-quarter
  selector; `fiscalQuarter` gives the issuer's quarter.

## Authority Fields On Every Evidence Result (2.5.8)

- The 12 evidence-only actions now carry `decisionUse: EVIDENCE_ONLY` and
  null `selectedMethod`, `selectedMultiple`, `scenarioWeights`,
  `priceTarget`, `g2`, `opportunity` and `action` on every result, including
  terminal statuses. The actions are:
  - `get_historical_valuation_context`, `reconcile_metric_sources`;
  - `get_share_count_scenarios`, `extract_funding_capex_schedule`;
  - `extract_operating_driver_ledger`, `get_guidance_history`;
  - `get_consensus_forecast_curve`, `get_eps_revisions`,
    `get_evidence_quality`;
  - `build_valuation_evidence_pack`, `get_evidence_cut`,
    `list_evidence_cuts`.
- Terminal statuses such as `PERIOD_NOT_FOUND`, `INVALID_PERIOD`,
  `NO_SEC_REGISTRANT` and `COMPANYFACTS_NOT_AVAILABLE` returned early,
  without these fields.
- The fields are added where each runtime dispatches an action: after
  `_dispatchTool` in the Worker, and in `_envelope_tool_result` in Python.
  A payload that already carries them is passed through unchanged. A stray
  non-null value is overwritten to null.
- Historical valuation's failed subject and peer entries (`NO_SEC_REGISTRANT`,
  `COMPANYFACTS_NOT_AVAILABLE`, `PRICE_HISTORY_NOT_AVAILABLE`) carry them
  too.
- Errors are unchanged: `ok: false` with `data: null`, so there is no
  payload to carry fields. A missing authority field on an error means no
  authority, never an implied selection.

## Exercised Warrants And Convertibles At Period End (2.5.9)

- The dilution bridge, and `get_share_count_scenarios`, which reads it, no
  longer count an exercise as warrants outstanding, and read convertible notes
  as of the period end.
  - VRT's 2025 10-K tags `ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights`
    = 4,812,521 on 2024-12-06. That figure is the shares issued when its
    private placement warrants were exercised cashlessly. The same date and
    class also carry `ClassOfWarrantOrRightNumberOfWarrantsExercised` =
    5,266,667, and the count sits on the equity-statement axis.
  - Before this fix the bridge read the 4,812,521 as private placement
    warrants outstanding. The 10-K states that none were outstanding at
    2025-12-31.
- A warrant count is not read as outstanding when it is:
  - on `StatementEquityComponentsAxis` (an equity-statement movement); or
  - tagged together with a warrants-exercised count for the same class and
    date.

  These counts are listed in `WARRANT_EXERCISE_NOT_OUTSTANDING` with their
  reason. VRT's warrants are now `notDisclosed`, and its scenarios carry
  options and share awards only.
- A count dated before the period end is still read, because some outstanding
  warrants are tagged only at issuance (AAOI's Amazon warrant, 2025-03-13).
  Such a class carries `countBeforePeriodEnd: true` and a
  `WARRANT_COUNT_BEFORE_PERIOD_END` warning, so the filing text can confirm it
  is still outstanding.
- A class with no exercisable warrants adds 0 shares whatever its strike.
- A count followed by an exercise of the same class, dated after the count
  and by the period end, is not read as outstanding (`EXERCISED_AFTER_COUNT`).
  The exercise concepts include
  `StockIssuedDuringPeriodSharesExerciseOfWarrants`. BE's Oracle warrant
  (3,531,073 shares at $113.28, tagged at issuance 2025-10-28) was exercised
  on a cashless basis on 2026-05-01 for 1,905,433 shares. Its entry in the
  warning names that exercise.
- A class that a newer filing shows exercised or retired is not restored from
  an older fallback filing. BE's 2025 10-K still counts the Oracle warrant.
- Convertible notes are read as of the period end:
  - Shares come from the filing's own count issuable on conversion at the
    period end (`DebtInstrumentConvertibleNumberOfSharesAvailableForConversion`),
    when tagged (`ifConvertedBasis: shares_issuable_tagged_at_period_end`).
    BE tags the maximum, make-whole included: 2030 notes 19,554,000; 2029
    notes 1,714,619; 2028 notes 59,486.
  - Otherwise shares come from principal outstanding at the period end
    (`principalBasis: outstanding_at_period_end`). That principal is a face
    amount tagged then, or the instrument's carrying amount when it is below
    95% of the issue's face. BE's 2028 notes had $0.787M left of $632.5M, and
    its 2029 notes $26.971M of $402.5M. A carrying amount close to face is the
    same notes net of discount, so the face is kept. Without either figure the
    issue's face is used, with `CONVERTIBLE_PRINCIPAL_NOT_AT_PERIOD_END`.
  - A tagged conversion ratio is used only when ratio × conversion price is
    within 2% of $1,000. BE tags only each note's make-whole increase (2030:
    2.6926 against $194.97, whose rate is 5.1290), so it is flagged
    `RATIO_INCONSISTENT_WITH_PRICE` (`CONVERSION_RATIO_INCONSISTENT`) and the
    price is used.
  - A redemption or repurchase tagged after the period end is flagged
    (`afterPeriodEnd`, `CONVERTIBLE_REDEMPTION_AFTER_PERIOD_END`), and the
    notes are still counted as of the period end. BE's 2028 notes were
    redeemed in July 2026, so the bridge total 21,328,105 includes their
    59,486 shares.
- `get_valuation_snapshot` reads the bridge. Its diluted shares and the
  convertible principal it removes from debt follow these fixes.
- Not changed: warrants that expired or were redeemed without an exercise tag
  are still read from their last tagged count. Such a count is flagged when
  it predates the period end.

### Claim coverage and untagged share claims (2.5.9, COHR)

- The bridge's `status` covers tagged instruments only. `COMPUTED` means every
  tagged instrument was resolved. It never means every claim on the equity was
  found.
  - Every bridge carries `claimCoverage`:
    - `scope: TAGGED_INSTRUMENTS`;
    - `completeClaimInventory: false`;
    - `modeledComponents`;
    - `textScan` (`READ` or `NOT_READ`), with the claim kinds it looks for;
    - the count of open claims.
  - COHR's FY2026 10-K tags options and awards only; nothing else beyond the
    cover count. Before this fix it read `COMPUTED` at about 201M shares.
- The filing text is searched for share claims that no tagged component
  models (`SHARE_CLAIM_SEARCH_TERMS`):
  - `PRICE_PROTECTION`: price protection granted with a share sale;
  - `ANTI_DILUTION_RIGHT`: an investor's anti-dilution right;
  - `FORWARD_SALE`: a forward sale agreement;
  - `CONTINGENT_SHARES`: earnout or contingent-consideration shares;
  - `CONVERTIBLE_PREFERRED`: convertible preferred stock.

  Each claim is quoted in `unquantifiedShareClaims`: up to four sentences,
  their lead-in, and the document and filing date.
- A claim is never quantified or added to any count.
  - An open claim (`UNQUANTIFIED`) makes the bridge `PARTIAL` and raises
    `UNQUANTIFIED_SHARE_CLAIMS`.
  - COHR's March 2, 2026 NVIDIA purchase agreement (7,788,161 shares at
    $256.80) carries a six-month price-protection provision. The provision
    can be settled in additional shares, so COHR now reads `PARTIAL`.
  - The count itself is unchanged.
- The scan skips terms that are not share claims:
  - A price-protection sentence about distributors, customers, revenue,
    returns or inventory is revenue recognition, not a share claim. An
    example is COHR's variable-consideration policy.
  - An anti-dilution sentence about a warrant's or note's own adjustment
    terms is skipped, because the bridge already reads that instrument.
  - A context window's cut first and last sentences are never quoted.
  - An initialism such as "U.S." does not end a sentence.
- Convertible preferred status comes from tags, not text:
  - If the preferred and temporary-equity share counts tagged at the period
    end are all zero, the claim is closed (`TAGGED_NONE_OUTSTANDING`).
  - If any is positive, the claim stays open and the count is quoted.
  - Text saying a series was converted only sets `extinguishmentStated`,
    because the sentence can describe another series.
  - COHR's Series B and BE's SK ecoplant preferred are tagged zero and
    closed.
- If the text search fails, `textScan` is `NOT_READ` and the bridge warns
  `SHARE_CLAIM_TEXT_NOT_READ`. An unread search is never reported as "no
  claims".
- `get_share_count_scenarios` completeness is scoped the same way:
  - `COMPLETE` is replaced by `TAGGED_INSTRUMENTS_RESOLVED`.
  - `EXCLUDES_UNQUANTIFIED_CLAIMS` and `CLAIM_TEXT_NOT_READ` make the
    scenarios `PARTIAL`.
  - The claims and `claimCoverage` are passed through.
- `get_valuation_snapshot` passes the open claim kinds through as
  `shares.unquantifiedShareClaims`, with `shares.claimScope`, and raises
  `UNQUANTIFIED_SHARE_CLAIMS`.

### Metric-first guidance and its basis (2.5.9, COHR)

- `extract_guidance` returned `NOT_DISCLOSED` for COHR's Q4 FY2026 release.
  Its outlook puts the metric first: "Revenue for the first quarter of fiscal
  2027 is expected to be between $2.2 billion and $2.4 billion".
  - The keyword-first patterns needed "expects" or "guidance" before the
    metric.
  - The gross-margin pattern allowed no digits between the label and the
    range, and "fiscal 2027" has digits.
- Keyword-first wording still wins. Metric-first wording is read when there is
  none:
  - the metric (revenue, gross margin, EPS or earnings per share);
  - up to 120 characters with no period, dollar sign or percent sign;
  - "expected, projected, forecast, anticipated or estimated to be, range or
    total";
  - the range.

  Nothing reported can sit between the metric and the verb, so a reported
  value is never read as guidance.
- Every range carries `basis`: `NON_GAAP`, `GAAP` or `NOT_STATED`. It is read
  from the range's own clause: the sentence up to the range, and the words
  after it up to the next value or clause break.
  - COHR's gross margin (39.5–41.5%) and EPS ($1.85–2.05) are `NON_GAAP`.
  - Its revenue ($2.2–2.4B) is `NOT_STATED`.
  - In "GAAP EPS … $1.00 and $1.20 and non-GAAP EPS between …", the first
    range stays `GAAP`.
- `get_guidance_history` entries and outcomes carry the basis.
  - A non-GAAP range is never scored against a reported GAAP actual:
    `NOT_EVALUATED_NON_GAAP_BASIS`, with both positions null.
  - The actual stays visible.
- A scaled amount is a whole number: 2.05 billion reads 2050000000, not
  2049999999.9999998. This covers guidance history amounts and
  `extract_guidance`/`extract_earnings_metrics` text values.
- The local server now reads the same release as the Worker: the newest 8-K
  reporting results of operations (Item 2.02). Before this fix it took the
  newest 8-K of any kind. For COHR that was the Aug 31, 2026 8-K (Items 5.02,
  8.01), whose text has no guidance.
  - Any 8-K is still the fallback when none reports Item 2.02.
  - `list_sec_company_filings` rows carry each 8-K's `items`, in both
    runtimes.

### Geographic denominators, ± guidance and customer warrants (2.5.9, MRVL)

- **Geographic share.** MRVL's 10-Q geographic table puts a "% of Total"
  cell beside each value. Its total row has no label and no percent cells,
  so the cell index of China's value landed on the prior year's total.
  - China's $1,161.5M was divided by $2,006.1M, which read 57.9%. The table
    states 42%.
  - The value and the total are now paired by position among amount cells:
    - percent cells, including a number followed by a bare "%" cell, are
      skipped;
    - dash cells count as zero placeholders.

    China now reads $1,161.5M of $2,739.3M, 42.4%.
  - Under a "% of Total" header, the table's own percentage must agree with
    the computed share within 1 point. Otherwise the table is not read.
  - A "Change" column is not a share and is not checked.
  - An unlabeled total row is quoted as "Total (unlabeled row)".
  - The column is named from the header rows: "Three Months Ended August 1,
    2026".
  - A quarterly report's period is that column, not "FY" plus the filing
    year.
  - The XBRL path now pairs a region fact only with the total for the same
    period. A 10-Q also carries the prior year's total.
  - The local server also checks the table that encloses a match. MRVL's
    table tag starts about 5,000 characters before "China", because of
    inline styles.
- **Midpoint and tolerance guidance.** Ranges stated as "Net revenue is
  expected to be $3.150 billion +/- 5%" and "GAAP diluted net income per
  share is expected to be $0.53 +/- $0.05" are read (`statedAs:
  MIDPOINT_PLUS_MINUS`).
  - The bounds are computed with exact decimal arithmetic: $2.9925–3.3075B
    and $0.48–0.58.
  - A tolerance can be a percent, or an amount in its own unit (the
    midpoint's when it names none). A bare tolerance with no $, % or unit is
    not read.
  - "Net income per share" counts as EPS.
  - A range for the same metric on another basis is kept in `alternates`:
    MRVL's non-GAAP gross margin 57.5–58.5% and EPS $1.05–1.15 beside the
    GAAP ranges.
- **Warrant vesting.**
  - The bridge now reads `ClassOfWarrantOrRightSharesVested`. MRVL's fiscal
    2025 customer warrant has 1.2M of its 4.2M shares vested, and its fiscal
    2026 warrant 0 of 1.0M. Before this fix, all of them were treated as
    exercisable.
  - A class with a tagged vesting term but no vested or unvested count has an
    unresolved exercisable count (`WARRANT_VESTING_NOT_TAGGED`). It is never
    taken as all outstanding, in the bridge or in scenarios.
  - `exercisableBasis` says which rule applied.
- **Warrants after the period end.**
  - A warrant count dated after the report's period end, or tagged as a
    subsequent event, is not a period-end class.
  - Before this fix, MRVL's 59.0M customer warrant at $206.58, issued after
    the quarter, was counted as a class named "Subsequent Event".
  - It is now a `WARRANT_AFTER_PERIOD_END` claim, `UNQUANTIFIED` and out of
    every count. The claim quotes the filing's sentence and the next one
    (`leadOut`): "eligible for vesting from our third quarter of fiscal 2027
    through the end of fiscal 2033, upon meeting certain revenue milestone
    conditions or time-based conditions".
  - Which fiscal year's milestones vest which shares is not tagged, so no
    split by year is given.
- **Convertible preferred component.**
  - A new `convertible_preferred` component covers preferred stock
    outstanding at the period end. It uses the tagged
    `PreferredStockConvertibleSharesIssuable` and
    `PreferredStockConvertibleConversionPrice`, if-converted when in the
    money.
  - MRVL's Series A (2.0M preferred shares, issued to NVIDIA) converts into up
    to 21.8M common shares at $91.84. It adds 0 at $80, 21.8M at $100, and
    21.8M to the gross count.
  - The issuable count is tagged at issuance, so it is flagged
    `countBeforePeriodEnd`.
  - A text claim for a preferred series the component resolves reads
    `MODELED_IN_BRIDGE`.
  - Scenarios give it the `convertibles` treatment.
  - Liquidation preference, dividends and redemption are not modeled.

## Option Axes, Conversion Units, Outlook Tables And Claim Lifecycle (2.5.10)

- **Options on award axes (AEHR).**
  - AEHR tags its options only under a company member on `AwardTypeAxis`:
    316,000 outstanding at $5.11, 310,000 exercisable. There is no
    undimensioned total, so options were `notDisclosed` and share scenarios
    understated.
  - With no undimensioned count, the bridge now reads option counts tagged
    only on award or plan axes, at the latest date and on the fewest axes
    (`countBasis: award_axis_members`, one entry per member).
  - One member uses its own weighted-average strike. Several members, each
    with a strike, are priced like exercise-price ranges.
- **Conversion-rate units (LITE).**
  - LITE tags `DebtInstrumentConvertibleConversionRatio1` per $1 of principal:
    0.0076319 against a $131.03 price is 7.6319 per $1,000.
  - A ratio whose product with the conversion price is about 1 is scaled by
    1,000 (`RATIO_PER_1_PRINCIPAL_SCALED`) and used.
  - One that agrees with neither scale is still `RATIO_INCONSISTENT_WITH_PRICE`
    (BE's make-whole increases).
  - Without a price, a ratio below 1 has no provable unit
    (`RATIO_UNIT_UNCERTAIN`) and is not used.
  - `conversionRatioPer1000` is the normalized rate, and `conversionRatioTagged`
    keeps the tag.
- **Guidance in outlook tables and bullets.** Rows are read after every
  sentence pattern (`statedAs: OUTLOOK_ROW`):
  - a release-table row, e.g. VRT's "Third Quarter 2026 Guidance Net sales
    $3,650M - $3,850M … Adjusted diluted EPS (1) $1.77 - $1.83";
  - an outlook bullet, e.g. LITE's "Non-GAAP diluted net income per share of
    $4.05 to $4.35".

  A row counts only under a guidance or outlook heading, within 400 characters
  and with no sentence break between. Its basis comes from its own label.
- **More guidance wording.**
  - A comma before a tolerance ("$108.0 billion, plus or minus 2%").
  - A margin tolerance in basis or percentage points ("74.0%, plus or minus
    50 basis points", computed exactly).
  - Plural "gross margins".
  - A second range in the same sentence ("… and adjusted diluted EPS of $6.65
    to $6.75").
  - One range stated for both bases reads `GAAP_AND_NON_GAAP` (NVDA).
- **Target periods.**
  - `extract_guidance` gives each range and alternate a `targetPeriod`.
  - Alternates must share the primary's period: VRT's Q3 table row is not the
    full year's non-GAAP alternate.
  - When a range's own sentence names no period, the nearest guidance or
    outlook mention above it, within 1,500 characters, is read whole
    (`scope: GUIDANCE_MENTION`). A mention that names no period gives way to
    the next.
  - Before this fix, a fixed 200-character look-back cut NVDA's "third quarter
    of fiscal 2027" to "fiscal 2027", and missed MRVL's heading entirely.
- **Warrant lifecycle.**
  - A class whose tagged expiration date (`WarrantsAndRightsOutstandingMaturityDate`
    or an expiration-date concept) is before the period end is closed. It is
    listed in `expiredClasses` (`WARRANT_EXPIRED_BEFORE_PERIOD_END`).
  - A class counted from a date before the period end, whose tagged term
    (`WarrantsAndRightsOutstandingTerm`: "P5Y", "5 years", "five years") has
    since elapsed, is flagged `termElapsedBy` (`WARRANT_TERM_ELAPSED`) and
    still counted. The term can run from a later exercisability date.
- **Claim census.** Four kinds are added:
  - `EXCHANGEABLE_INTERESTS`: Up-C units or shares exchangeable for common
    stock;
  - `SAFE`: simple agreements for future equity;
  - `SHARE_SETTLED_OBLIGATION`: amounts payable or settled in common stock;
  - `EQUITY_LINE`: standby equity purchase agreements and committed equity
    facilities.

  Award, note, warrant and dividend settlement in shares is excluded. So is a
  settlement stated in the past tense. The scan remains a fixed list
  (`completeClaimInventory: false`).
- **Convertible preferred stated in text (LITE).**
  - LITE's Series A (2.9M shares, issued to NVIDIA) has no conversion tags.
    Its note says "The Preferred Stock will convert on a one-for-one basis into
    shares of our common stock".
  - A stated conversion is now read in exact forms only: one-for-one; "each
    share … convertible into N shares"; "convertible in the aggregate into N
    shares".
  - The sentence is quoted in `statedConversion`.
  - A per-share ratio with no conversion price is common-equivalent at any
    price (`method: as_converted_no_conversion_price`). An aggregate with no
    price joins the gross count only.
  - Shares tagged under both `PreferredStockSharesOutstanding` and
    `TemporaryEquitySharesOutstanding` are the larger count, never the sum.
    LITE was 5.8M before this fix.
- **Preferred economics.**
  - The tagged liquidation preference is reported as `liquidationPreference`:
    aggregate, or per share × shares outstanding.
  - The dividend rate is reported as `dividendRatePct`.
  - `get_valuation_snapshot` adds the liquidation preference to enterprise
    value for preferred that is not counted as shares at the price
    (`balances.preferredLiquidationPreferenceInEv`).
  - Without a tag it warns `PREFERRED_NOT_IN_ENTERPRISE_VALUE`.
- Not changed in 2.5.10 (both addressed in 2.5.11, below):
  - Net-share settlement of convertible notes was not modeled. LITE's notes
    settle principal in cash, so if-converted shares overstate them.
  - 52/53-week fiscal calendars resolved to conventional period labels.

## Warrant Lifecycle Text, Net-Share Settlement And 52/53-Week Years (2.5.11)

- **Warrant exercise, expiry and redemption stated in text.**
  - A warrant count tagged before the period end (an issuance, a prior year
    end) says nothing about what happened since. Examples:
    - ASTS's 10-Q counts 122,000 Private Placement Warrants as of 2025-12-31.
      Its text says "the remaining 122,000 Private Placement Warrants were
      exercised" in the quarter ended March 31, 2026.
    - RKLB's 10-K counts 728,835 warrants issued on 2023-12-29. Its text says
      "On November 14, 2024, all 728,835 common stock warrants were exercised".
  - When a class is counted before the period end, the bridge searches the
    filing text for past-tense exercise, expiry and redemption sentences,
    using fixed phrases plus each class's count.
  - A sentence retires the class only when all of these hold:
    - it names the class, by its tagged count (`matchedBy: STATED_COUNT`) or by
      a class name of two or more words (`CLASS_NAME`);
    - it states the event for the whole class ("all", "the remaining", "in
      full", or the class's own count; an expiry or redemption covers every
      unexercised warrant);
    - it dates the event after the tagged count and by the period end. The
      latest date the sentence states by the period end is used, and the
      earliest qualifying event wins.
  - A retired class is listed in `retiredInText` with the quoted sentence
    (`WARRANT_RETIRED_IN_TEXT`) and is not counted.
  - These sentences never retire a class: negated ("No … were exercised"),
    future or conditional sentences, partial exercises, and events after the
    period end.
  - Each class counted before the period end carries `lifecycleText`:
    `NO_EVENT_STATED` when the text was read, `NOT_READ` otherwise.
    `WARRANT_COUNT_BEFORE_PERIOD_END` says which applies. A class the text
    says nothing about stays counted, since silence does not prove it is
    outstanding or retired.
- **Convertible principal settled in cash (LITE).**
  - LITE's 10-K says "The principal amounts of all of our outstanding
    convertible notes must be settled in cash."
  - For a stated cash settlement, a note's `principalSettlement` quotes the
    sentence, with a scope:
    - `ALL_NOTES`: the sentence covers every note;
    - `NAMED_NOTES`: it names the note's year ("2029 Notes", "notes due 2029");
    - `UNNAMED_NOTES`: it names no note, and the company has only one.
  - Such a note's `netShareSettlementShares` is
    `max(0, if-converted shares − principal / price)` in the money, else 0.
  - Settlement "at our election", "may" or "can" is not a stated cash
    settlement. Those notes stay if-converted only.
  - `dilutedSharesAtPrice` keeps the if-converted count (the EPS basis).
    `bridge.dilutedSharesAtPriceNetShareSettlement` and
    `convertibleDebtNetShareSettlement` report the net-share view beside it
    (`CONVERTIBLE_PRINCIPAL_SETTLED_IN_CASH`).
  - At $700, LITE's four notes are 9,449,102 shares if-converted and
    7,228,673 net.
  - `get_share_count_scenarios` takes
    `convertibles: net_share_settlement_when_stated`. Notes without a stated
    cash settlement stay if-converted, and their method says so.
  - Capped calls were not modeled in 2.5.11. See "Capped Calls (2.5.12)" below.
- **52/53-week fiscal identity.**
  - `fiscal-calendar.ts` / `fiscal_calendar.py` read a period's fiscal year
    and quarter from the date a week before its end. A year ending January 2,
    2027 is fiscal 2026. AEHR's year ending May 29, 2026 is fiscal 2026.
  - A retailer that names its year by the start (a "fiscal 2025" ending
    February 2026) is not detected. Where a filing states the fiscal year,
    that stated year wins.
  - **Guidance target periods.**
    - "For the fiscal year ending June 25, 2027" (AEHR) now reads as FY2027,
      with `periodEnd: 2027-06-25` and basis `TEXT_PERIOD_END`. Before, its
      `targetPeriod` was null.
    - An end date after a named period ("third quarter of fiscal 2027 ending
      October 30, 2026") is kept in `periodEnd`. An impossible date is not
      read.
  - **Guidance-history actuals.**
    - FY labels use the fiscal year the annual report states (companyfacts
      `fy` of a 10-K's own latest year), else the period-end rule.
    - Calendar quarters and first halves use the week-shifted end: a quarter
      ending April 5 is Q1.
    - For non-calendar years whose fiscal year an annual report states,
      quarters are counted back from the year end
      (`fiscalQuarterMapping: FILING_STATED_FISCAL_YEAR`). The year in
      progress is counted from a projected end 52 weeks after the latest.
    - Without a stated year, fiscal quarters stay
      `NOT_EVALUATED_FISCAL_QUARTER_MAPPING`.
  - The consensus curve's `fiscalYear` uses the same rule.
  - Filing search results and annual geographic revenue now take the fiscal
    year from the filing's period of report, not its filing date, in both
    runtimes. The Worker had labelled a December 2025 10-K filed in February
    2026 as FY2026.
- **Claim census.** Three kinds are added:
  - `CONTINGENT_SHARES` now also covers holdback, escrow and milestone shares.
  - `CONTINGENT_VALUE_RIGHT`: counted only when the sentence mentions shares
    or common stock.
  - `SHARE_ISSUANCE_COMMITMENT`: "obligated/committed/required to issue …
    shares". Awards are excluded, and the sentence must be present or future
    tense.
- **Preferred conversion wordings.** These are added:
  - one-to-one and 1:1;
  - "each share … convertible, at the option of the holder, into N shares";
  - "a conversion rate of N shares of common stock for each share of …
    preferred stock";
  - "convertible into an aggregate of N shares".

  The scan remains a fixed list (`completeClaimInventory: false`).

## Capped Calls (2.5.12)

- **What is read.** When the bridge has convertible notes, the filing text is
  searched for capped call passages ("cap price", "capped call"). From
  sentences about the capped calls (and the sentence after one) it reads:
  - the strike price. It is `STATED` when the text gives it, and
    `STATED_AS_CONVERSION_PRICE` when the text says only that the strike
    corresponds to the conversion price; then the note's tagged conversion
    price is used;
  - the cap price;
  - the covered shares: a stated count ("33,549,508 shares", "69.3 million
    shares"), or a statement that the calls cover the shares underlying the
    notes;
  - whether the capped calls outlasted conversions of their notes ("were not
    impacted by the induced conversion").

  A passage belongs to the note whose year it names, e.g. "2032 Capped Call
  Options" or "Notes due June 2028". When the company has only one note, a
  passage that names no year is assigned to it.
- **Offset.**
  - Formula: `cappedCall.offsetSharesAtPrice = covered × (min(price, cap) − strike) / price`.
    This is the value the capped call delivers back to the company, in shares
    at the price.
  - It is computed only when the strike, cap and covered shares are all
    stated. Otherwise the capped call carries the stable enum
    `unresolvedReason: CAPPED_CALL_TERMS_INCOMPLETE` and `missingTerms`, a
    list of `STRIKE_PRICE`, `CAP_PRICE` and `COVERED_SHARES` (2.5.13; 2.5.12
    put free text such as "strike price not stated" in `unresolvedReason`).
    The bridge also warns `CAPPED_CALL_TERMS_INCOMPLETE`.
  - A stated count larger than the notes' shares today (coverage at issue,
    after conversions) is used only when the filing says the capped calls
    outlasted those conversions (`STATED_COUNT_SURVIVES_CONVERSIONS`).
    Otherwise the notes' shares at the period end bound it
    (`SHARES_UNDERLYING_NOTES_AT_PERIOD_END_BELOW_STATED_COUNT`), and the
    stated count is kept in `statedCoveredShares`.
- **Bridge fields.**
  - `dilutedSharesAtPrice` is unchanged: diluted EPS excludes capped calls as
    antidilutive.
  - `bridge.cappedCallOffsetShares`, `dilutedSharesAtPriceNetOfCappedCalls`
    and `dilutedSharesAtPriceNetShareSettlementNetOfCappedCalls` report the
    economic view beside it (`CAPPED_CALL_OFFSET`).
- **Live examples.**
  - **BE:** its 2028 capped calls state strike $18.85, cap $26.46 and
    33,549,508 shares, and say they were not impacted by the induced
    conversion. At $30 they offset 8,510,392 shares.
  - **RKLB:** strike $5.1255, cap $8.04. The stated 69.3 million shares are
    bounded by the notes' 27,736,452 shares, which offset 4,041,894 at $20.
  - **LITE:** the 2032 capped calls state the cap ($268.24) and their
    coverage but no strike, so they are reported and not netted
    (`missingTerms: [STRIKE_PRICE]`).
- **Share scenarios.** `get_share_count_scenarios` takes
  `capped_calls: ignore` (the default) or `offset_when_stated`. The latter
  adds a negative `capped_call` line per note with fully stated terms. A
  capped call with incomplete terms is an unresolved line.

## Nested Warrants, The Antidilutive Table And Fiscal-Year Naming (2.5.13)

- **Warrant classes tagged as a total and in parts are counted once.**
  - A count whose dimensions extend another counted class (the class plus a
    tranche or holder axis) is a part of that class.
  - Parts that add up to the class (within 0.5%) replace it and keep their
    own terms. JOBY's Delta Warrants (12,833,333) are counted through their
    tranches (7,000,000 and 5,833,333), not as 25.7M.
  - Parts that do not add up are left out. LUNR's 104,157 related-party
    warrants are part of its 541,667, and the bridge previously counted
    645,824.
  - `nestedCounts` lists what was set aside (`WARRANT_NESTED_COUNT`).
- **The company's antidilutive-securities table.**
  - The EPS note tags every class of potentially dilutive security left out
    of diluted EPS, with the company's own count for the period
    (`AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount`).
  - Each non-zero row is matched to a bridge component by its label in
    `antidilutiveReconciliation.rows`, with its category and `modeledBy`.
  - A row no component models becomes a claim with the company's count
    (`status: REPORTED_NOT_MODELED`, `reportedShares`,
    `reportedSecurities`, `evidence: ANTIDILUTIVE_TABLE`). A text claim of
    the same kind takes the count. Examples:
    - ASTS's Class B and Class C common stock exchangeable for Class A;
    - LUNR's escrow shares;
    - SOUN's contingently issuable shares;
    - RKLB's collared forward transactions (7,451,200), which joins its
      forward-sale text claim.
  - Open claims (`UNQUANTIFIED` or `REPORTED_NOT_MODELED`) keep the bridge
    `PARTIAL` and share scenarios at `EXCLUDES_UNQUANTIFIED_CLAIMS`.
  - The table's warrant total is set against the bridge's. A gap over 5%
    (and 100,000 shares) is flagged `WARRANT_COUNT_DIFFERS_FROM_REPORTED`,
    e.g. LUNR reports 4,857,302 against the bridge's 541,667. The counts can
    be weighted averages, so they are never added into the bridge.
  - `claimCoverage.antidilutiveTable` says whether the table was tagged. The
    claim inventory is still not complete: the table lists only securities
    that were antidilutive for the period.
- **Fiscal-year naming.**
  - Companies name a year ending early in a calendar year differently. DG's
    year ending January 30, 2026 is its fiscal 2025; WMT's year ending
    January 31, 2026 is its fiscal 2026.
  - `fiscalYearNaming` compares the fiscal year each recent 10-K states for
    itself (companyfacts `fy`) with the period-end rule:
    - when the latest three agree, their offset (DG: −1) names the company's
      other years (`basis: SEC_STATED_FISCAL_YEAR`);
    - otherwise the rule stands (`PERIOD_END_RULE_STATED_YEARS_INCONSISTENT`,
      `..._NO_STATED_YEAR`, `..._SEC_NOT_READ`).
  - `get_consensus_forecast_curve`, `get_evidence_quality` and the valuation
    evidence pack apply it and report `fiscalYearNaming`. DG's FY0 ending
    January 2027 is now fiscal 2026, not 2027.
  - Guidance history applies it to a year named only by its end date
    (`namingBasis`).
  - An annual report's geographic revenue period uses the filing's own
    `dei:DocumentFiscalYearFocus` when tagged.
  - Filing-search `fiscalYear` labels still used the period of report and the
    period-end rule in 2.5.13; 2.5.14 moves them to the filing's tagged year.
- **Capped call contract.** `unresolvedReason` is the stable enum
  `CAPPED_CALL_TERMS_INCOMPLETE`, with `missingTerms` listing
  `STRIKE_PRICE`, `CAP_PRICE` and `COVERED_SHARES`.
- **Parity fix.** A guidance outcome for a metric whose actuals table is
  empty is `ACTUAL_NOT_YET_REPORTED` in both runtimes. Python had reported
  `NOT_EVALUATED_METRIC`.

## Filing Fiscal-Year Labels (2.5.14)

- **The problem.** Filing search (`search_sec_filing_text`) labelled each
  filing's `fiscalYear` from its period of report (the period-end rule). For
  companies that name a year by the calendar year it starts in, that label was
  one year high: DG's 10-K for the year ended January 30, 2026 read FY2026,
  although DG calls that year fiscal 2025.
- **The rule.**
  - Once a filing's primary document is read, its label is the fiscal year the
    document tags for itself (`dei:DocumentFiscalYearFocus`).
  - It applies to the top-level `fiscalYear` (the first filing) and to each
    `filings[].fiscalYear` summary.
  - DG's 10-K now reads FY2025. A 10-Q whose quarter ends before the fiscal
    year's calendar year (AAPL's December quarter) reads the fiscal year the
    company assigns it.
  - The geographic-revenue path already used the tagged year for annual
    reports. It now shares the same rule.
- **Guard.** A tagged year more than one year away from the period-end rule is
  treated as a mistag, and the period-end rule is used. Untagged filings
  (8-Ks, older non-inline filings) and the "no search terms" response, which
  does not read the document, keep the period-end rule.
- **Contract.** No field was added or renamed; only label values change.
- **Unchanged.**
  - The consensus curve's `fiscalYearNaming` (2.5.13) is unchanged.
  - The curve's `fiscalYearEnds` remains the provider-stated date (for example
    Yahoo's 2027-01-31 for DG), not the company's 52/53-week end date.
- **Runtimes and tests.** Both runtimes.
  `scripts/test_guidance_and_drivers.py` checks parity through
  `FILING_LABEL_CASES`.

## SEC Fact Reads, Evidence Preflight And Reconciliation Fixes (2.5.15)

A live cross-check of 16 tickers against SEC, Yahoo, issuer releases, Alpha
Vantage and IBKR produced these fixes.

- **SEC fact reads**
  - SEC's companyconcept endpoint served BE's revenue as `"USD": {}` (an
    object, not a list). That crashed the Worker and made Python return
    NOT_DISCLOSED.
  - Such a concept is now read from companyfacts, with the info warning
    `SEC_COMPANYCONCEPT_MALFORMED`.
  - When one filing tags revenue under two concepts, the larger is taken as
    the total. BE FY2025: Revenues 2,023,994,000 against
    RevenueFromContractWithCustomerExcludingAssessedTax 2,001,614,000.
- **`extract_total_revenue`**
  - It passes through `code`, `message` and `warnings`.
  - `unit` is null when there is no value. It used to say USD.
  - IFRS-only filers (TSM) get `SEC_FACTS_IFRS_ONLY`, pointing to
    `reconcile_metric_sources`, which reads IFRS facts.
  - A non-SEC ticker (SIVE.ST) shows `NO_SEC_REGISTRANT`.
- **`get_evidence_quality`**
  - 20-F/40-F filers have an annual cadence. Their periodic filing is STALE
    only after 492 days (365, plus the 120-day filing window, plus 7). The new
    family fields are `cadence`, `staleAfterDays` and `latestInterim6k` (the
    latter for annual filers). TSM, TSEM and NBIS were falsely STALE at 273
    days.
  - A failed quote request is UNAVAILABLE with a retryable
    `QUOTE_UNAVAILABLE` blocker, not `QUOTE_MISSING`.
  - When every consensus provider request failed (none OK, at least one
    PROVIDER_ERROR, RATE_LIMIT or timeout), the consensus family is
    UNAVAILABLE, with one retryable `CONSENSUS_PROVIDER_ERROR` blocker
    instead of per-cell PROVIDER_NOT_COVERED blockers.
  - `providerStatuses` is always listed.
- **`reconcile_metric_sources`**
  - The reporting unit is the one filed most recently. NBIS reported in RUB
    as Yandex to FY2023 and in USD from FY2024, and was compared in RUB.
  - Revenue ties go to the larger concept.
  - Net income from ProfitLoss alone is the parent's. It is derived by
    subtracting NetIncomeLossAttributableToNoncontrollingInterest from the
    same filing, with `basis: PARENT_DERIVED_FROM_PROFITLOSS` and the
    derivation shown. BE FY2025: -87,140,000 - 1,294,000 = -88,434,000.
- **Release parser**
  - It reads B/M/K and bn/mn/mm abbreviations. COHR's "$1.81B" was read as
    1.81.
  - It applies a statement-table scale only when the release declares exactly
    one "(in thousands|millions|billions)", flagged `scaleBasis`. FN's
    "$ 1,214,293" was read unscaled.
  - It rejects per-share figures for money metrics, and non-GAAP or adjusted
    figures for all metrics. COHR, LITE and SNDK EPS were read as net income;
    SNDK's non-GAAP EPS was compared with GAAP.
  - It checks every label/amount pair in a sentence, and reports
    `rejectedCandidates`.
- **Both runtimes.** Python also returns `SEC_FACTS_IFRS_ONLY` on its no-facts
  path. Its other no-facts payload (NOT_DISCLOSED) still differs from the
  Worker's SEC_FACT_NOT_AVAILABLE, as before 2.5.15; that is a known
  follow-up.
- **Tests.** `scripts/test_extraction_rules.py`, `scripts/test_evidence.py`,
  `scripts/test_valuation_history_and_reconcile.py`.

## Company Fiscal-Year End In The Consensus Curve (2.5.16)

- **The problem.** Providers date a fiscal year by a nominal month end. Yahoo
  gives DG's current year as ending 2027-01-31; DG's year ends on the Friday
  nearest January 31, which is 2027-01-29.
- **`fiscalYearNaming.calendar`.**
  - `fiscalYearNaming` (in `get_consensus_forecast_curve` and the evidence
    pack) gains `calendar`: `{patterns, month, weekday, basis:
    "SEC_ANNUAL_PERIOD_ENDS", periodEnds}`.
  - It is read from the company's 10-K annual period ends (companyfacts) and
    kept only if it reproduces every one of them.
  - The patterns are:
    - `MONTH_END`;
    - `WEEKDAY_NEAREST_MONTH_END` (within 3 days of the month's last day);
    - `LAST_WEEKDAY_OF_MONTH`.
  - `calendar` is null when SEC was not read, when fewer than two annual ends
    exist, or when no pattern fits.
- **How the pattern is chosen.**
  - The newest two ends are read first, and older ones are added one at a time
    until a single pattern is left.
  - An end that no pattern fits (a changed calendar) stops the look-back.
  - MU's Thursday nearest August 31 and its last Thursday of August fit 2023
    to 2025; an older year settles it.
- **`companyFiscalYearEnd`.**
  - Each consensus-curve period gains `companyFiscalYearEnd`: the company's own
    end for the year the providers date. `fiscalYearEnds` stays as the
    providers state it.
  - It is null when there is no calendar, when the provider's month differs
    from the calendar's, or when two fitting patterns give different dates for
    that year (they then say nothing).
  - Live examples:
    - DG 2027-01-29;
    - MRVL 2027-01-30 (Saturday nearest Jan 31);
    - FN 2027-06-25 (last Friday of June);
    - LITE 2027-07-03;
    - SNDK 2027-07-02;
    - MU 2026-09-03 (Yahoo: 2026-08-31);
    - ANET 2026-12-31.
- **Runtimes and tests.** Both runtimes. `scripts/test_guidance_and_drivers.py`
  (`CALENDAR_CASES`) and `scripts/test_evidence.py`.

### SEC Fact Payload Parity And Dead Worker Code (2.5.16)

- **Missing facts (Python).** When no us-gaap fact exists for the requested
  form, Python returned `source`/`confidence` NOT_DISCLOSED with no code, so
  `extract_total_revenue` read NOT_DISCLOSED where the Worker reads NOT_FOUND
  with `code` NO_COMPANYCONCEPT_FACT_FOR_FORM. Python now returns the
  Worker's payload: `status` SEC_FACT_NOT_AVAILABLE, the code
  (NO_COMPANYCONCEPT_FACT_FOR_FORM, or SEC_FACTS_IFRS_ONLY for an IFRS-only
  filer), the latest filing of that form as evidence (with AUTO_20F_FALLBACK
  when a 20-F stands in for a 10-K), warnings and `_manualLookup`. A form a
  company tags no facts in is a lookup outcome, not a non-disclosure.
- **Found facts (Python).** A found fact's `xbrlContext` now carries the
  Worker's `concept`, `taxonomy`, `unit`, `instant`, `accessionNumber`,
  `filedAt` and `dimensions`. Without the concept, Python's decision-grade
  check failed: `extract_total_revenue` read `decisionGrade: false` for
  ANET's and BE's FY2025 revenue, where the Worker reads true.
- **Dead Worker code.** `getFilingTextSearch` and `getFilingDocument` had no
  callers (the public tools use `searchFilingText`); they and the helpers only
  they used (`filingTextOnlyMatches`, `edgarPrimaryDocFromIndex`) are removed.
  No public output changes.
- **Tests.** `scripts/test_sec_fact_payloads.py` runs both runtimes against
  mocked SEC responses (found, no facts for the form, IFRS-only, malformed
  concept) and compares their payloads.

### Named Fiscal-Year Periods In SEC Fact Extractors (2.5.16)

- **Defect.** `period` was honoured only as `"latest"`. Any other string
  (`"FY2025"`, `"2025"`, a typo) skipped the latest-filed filter and the
  sort, so the first companyconcept row came back: BE 10-K `"FY2025"` gave
  208,540,000, the 2016 revenue, labelled FY2018 (the fy of the later filing
  that carried it as a comparative).
- **Accepted values.** `period` is `"latest"` (the default) or a fiscal year,
  `"FY2025"` or `"2025"`. Any other value is an `INPUT_VALIDATION_ERROR` for
  `extract_sec_filing_fact`, `extract_geographic_revenue`,
  `extract_segment_revenue`, `extract_total_revenue`,
  `extract_revenue_exposure`, `extract_china_exposure`, `extract_exposure`
  and `query_sec_filing_index`, grouped or expanded, checked before any
  internal call so both runtimes fail the same way.
- **Selection.** A fiscal year selects the annual period the issuer calls
  that year. That is the `fy` (with `fp` FY) of the filing that first
  reported the period, when it is the period's end year or the year before.
  Otherwise it is the year the period ends in, counted from seven days before
  the end so a 52/53-week year ending in early January belongs to the year
  before. This is the rule `reconcile_metric_sources` uses. The latest filed
  value for that period wins (a restatement), and the result is labelled
  with the requested year.
  - BE `"FY2025"`: 2,023,994,000.
  - DG `"FY2025"`: 42,724,369,000 for the year ended 2026-01-30.
- **Refusals.**
  - A fiscal year with a quarterly filing type or `period_mode` is
    `INVALID_PERIOD`.
  - A year with no annual fact is `PERIOD_NOT_FOUND`, listing the fiscal
    years found.
  - Both come back as the `SEC_FACT_NOT_AVAILABLE` payload with the latest
    filing of the form as evidence.
  - Geographic revenue with a named year and no tagged rows refuses the same
    way instead of reading the latest filing's table, which may be another
    year.
- **Quarters** are not named by `period`. Use `"latest"` with `10-Q`, an
  `accession_number`, or `reconcile_metric_sources` (`"Q3 2025"`, `"latest_quarter"`).

## Failed SEC Reads And Python Payload Parity (2.5.17)

- **Defect.** The SEC fact reader (`extract_total_revenue`,
  `extract_sec_filing_fact` and the extractors built on it) read
  companyconcept and companyfacts through a helper that returned nothing for
  a 404 and for a failure alike. A throttled read (429), a server error or a
  dropped connection was reported as `NO_COMPANYCONCEPT_FACT_FOR_FORM`, "the
  company tags no such fact". Seen live on BE under concurrent calls, and
  correct on retry.
- **Absence and failure are now separate.**
  - A 404 from SEC is absence: the concept is not tagged. The "no facts"
    results (`NO_COMPANYCONCEPT_FACT_FOR_FORM`, `NO_FACT_FOR_ACCESSION`,
    `SEC_FACTS_IFRS_ONLY`, `PERIOD_NOT_FOUND`) need every read they rest on
    to have succeeded.
  - Any other status, a network error or an unreadable body is a failed
    read. If any companyconcept read, or a companyfacts read the answer
    depends on, fails, the result is status `PROVIDER_ERROR`, code
    `SEC_READ_FAILED`, `retryable: true`, and `failedReads`:
    `[{endpoint, concept, httpStatus}]` (`httpStatus` null when there was no
    response).
  - It fails closed even when other concepts were read: the concept choice
    (newest filing, larger revenue) needs every candidate, so a partial read
    could return the wrong concept's figure.
  - A pinned accession is echoed as `requestedAccession`.
  - Geographic revenue does not fall back to the filing's HTML table after a
    failed read.
- **Failed reads are listed in a fixed order.** `failedReads` and the
  message list the candidate concepts in order, then companyfacts, whatever
  order the reads finished in.
- **Python payload parity.**
  - Python's fact reader writes integral numbers as integers, as the Worker's
    JSON does: `value` was `2023994000.0`.
  - An empty `sourceRows` value cell is `""`, not null.
  - Python's "no fact" payload (`SEC_FACT_NOT_AVAILABLE`, `PROVIDER_ERROR`)
    is now the Worker's whole payload for every fact type. Geographic revenue
    used to be reshaped without `status` and `code`, and a pinned accession
    with no fact returned a shorter payload.
  - Python's status mapping (`_as_status`) now has every branch of the
    Worker's `normalizeStatus`.
  - `scripts/test_sec_fact_payloads.py` compares whole payloads as JSON,
    which covers key order and integer vs float. It also runs
    `normalizeStatus` itself against `_as_status`.
- **Known Python gap.** Python's geographic and exposure tools lacked the
  Worker's status fields. Closed in 2.5.18.

## Failed Lookups And Geographic Parity (2.5.18)

- **Unreadable ticker index.** When SEC's ticker index cannot be read, the
  SEC fact reader's result is status `PROVIDER_ERROR` with code
  `SEC_LOOKUP_UNAVAILABLE`, `retryable: true`. It was
  `SEC_FACT_NOT_AVAILABLE`, which `extract_total_revenue` showed as
  NOT_FOUND. A ticker SEC does not list keeps `NO_SEC_REGISTRANT` and
  `SEC_FACT_NOT_AVAILABLE`.
- **`extract_china_exposure`.** A failed revenue read (`SEC_READ_FAILED`,
  `SEC_LOOKUP_UNAVAILABLE`) gives `overallStatus` `PROVIDER_ERROR` and that
  code. It was `NOT_FOUND` with no code. A found non-revenue exposure still
  reads `FOUND_NON_REVENUE_EXPOSURE`, with the revenue failure in `code`.
- **Pinned geographic reads.** When a pinned accession's XBRL facts do not
  name the region, the HTML fallback reads that filing's tables. It used to
  read the latest filing of the form, answering from a different filing.
- **Geographic payloads** keep `retryable` (`true` for a failed lookup,
  `false` for `NO_SEC_REGISTRANT`).
- **Python geographic and exposure tools match the Worker.**
  - `extract_geographic_revenue`, `extract_revenue_exposure`,
    `extract_china_exposure` and the geographic `get_filing_data` payload
    now have the Worker's keys, order, statuses and codes on every path:
    found, NOT_DISCLOSED, TABLE_NOT_PARSED, FILING_TEXT_NOT_AVAILABLE,
    FILING_NOT_FOUND_TRY_OTHER_TYPE, the 20-F switch, SEC_READ_FAILED and
    SEC_LOOKUP_UNAVAILABLE.
  - Python's filing-table parser is a port of the Worker's
    `extractGeoRevenueFromHtml`. It now reads the tables the Worker reads:
    live 10-Ks for AAOI (China 0.5752), AXTI (0.6236) and QCOM (0.4593) were
    NOT_DISCLOSED in Python. China is no longer the sum of Mainland China
    and Hong Kong rows.
  - Python's `extract_china_exposure` adds the Worker's "Bank of China"
    risk term and its filing-text search for China manufacturing when the
    tables show none.
  - Ratios round half up and `charsScanned` counts UTF-16 units, as in the
    Worker.
  - `scripts/test_sec_fact_payloads.py` compares whole payloads of both
    runtimes: 18 geographic scenarios through `get_filing_data` and the
    three tools, and 35 parser cases, including trimmed AAOI, AXTI and QCOM
    tables (`fixtures/sec_geo_revenue_tables.json`).
- **Remaining difference.** With `detailLevel` `raw`, `rawContext.filingIndex`
  still differs: Python's `get_sec_filing_index` has no 20-F fallback and
  its own failure codes. Compact output, the default, is identical.

## Earnings Release Periods (2.5.19)

- **Defect.** The Worker read an earnings release's fiscal period as any
  quarter word and any "fiscal YYYY" up to 180 characters apart, the
  earliest such pair winning. MU's FQ4 2026 release opens with a subheadline
  ending "...position Micron for a record fiscal 2027", 140 characters before
  "results for its fourth quarter and full year of fiscal 2026". The release
  was labelled FY2027 Q4 in `get_latest_earnings_release`,
  `extract_earnings_metrics` and `get_earnings_call_transcript`, and Alpha
  Vantage was asked for quarter 2027Q4. Python used different patterns: it
  happened to read FY2026 Q4 but missed other phrasings.
- **Rule (both runtimes, `worker/src/earnings-period.ts`,
  `yfmcp/earnings_period.py`).** A period is read only from one phrase that
  joins a quarter to a year:
  - "fourth quarter [and full (fiscal) year] [of] [fiscal (year) | FY] 2026",
    hyphens allowed ("FOURTH-QUARTER AND FULL-YEAR 2026");
  - "fiscal (year) 2026 ... fourth quarter", at most 40 characters apart
    with no sentence, colon, bullet, dash or pipe between them;
  - "Q4 [of] fiscal (year) 2026", "Q4 FY2026", "fiscal Q4 2026".
  A quarter with only an end date ("fourth quarter ended January 30, 2026")
  states no fiscal year and stays UNRESOLVED. The first phrase in the release
  still wins, so a prior-year comparison later in the text is not read
  (AEHR).
- **Effect.** On 23 current releases the deployed resolver labelled 4 (one
  wrong, MU); the new one labels 21, each matching the issuer's calendar
  (for example MRVL FY2027 Q2, ORCL FY2027 Q1, AVGO FY2026 Q3, calendar
  filers such as ANET Q2 2026), with no change for AEHR, DG and NKE. TSEM
  and NBIS (6-K filers) stay unresolved.
- **Tests.** `scripts/test_earnings_period.py` (in CI) runs both runtimes on
  17 phrasings, including the MU text, forward-looking years across a
  sentence or dash, and end-date-only quarters, and requires identical
  output and the stated period.

## Release And Table Amounts (2.5.20)

- **Defect.** `extract_earnings_metrics` read MU's FQ4 2026 revenue as 54229
  USD and missed its EPS. The highlights bullet "Revenue of $54.23 billion
  versus..." has no result verb, so the text fallback read the statement
  table's "$ 54,229" (in millions) as written, and "$32.87 per diluted share"
  was not an EPS form it knew. An audit of the other readers that turn
  filing or release text into amounts found the same kinds of errors:
  - text metrics: AVGO capex read as the FCF figure that follows it, ORCL
    "Services revenues" read as total revenue and "negative $5 billion" FCF
    read as positive, MRVL and BE full-year revenue read as the quarter's,
    NVDA "Gaming revenue" read as total;
  - filing tables: COHR segment and geographic revenue 1000x too large
    ("($000)" was not a scale statement), COHR's China share read from its
    long-lived assets table (the "Revenues" caption row was taken as the
    total of the real table), VRT segment revenue read from the Americas
    column;
  - guidance: SNDK's outlook table "(in millions)" read unscaled in
    `extract_guidance` and `get_guidance_history` (every outcome compared
    10300 USD with dollar actuals), its GAAP column labelled non-GAAP, and
    its EPS row ("N/A $44.00 - $46.00") missed; "$2.0 to $2.4 billion" read
    as $2 in `extract_guidance` only; "$3.9B" not read; "$1,234 more" read
    as millions.
- **Release figures (both runtimes, `releaseTextMetric` /
  `release_text_metric`).** `extract_earnings_metrics`' text tier reads a
  figure for a metric only when its sentence proves it is that metric's
  result for the quarter:
  - no guidance, award, backlog or ± wording, and no sentence that names only
    an annual period ("in 2025", "fiscal 2026 revenue", "full year");
  - the label leads the sentence or bullet ("Revenue of $X") or follows a
    period or GAAP qualifier; a segment word before it ("Gaming", "Services")
    refuses it;
  - after a change verb the figure follows "to" ("increased 5.2% to
    $11.3 billion"), never the change itself;
  - a non-GAAP or adjusted figure, a per-share figure for a total, and a
    figure attributed to another metric ("$13.7 billion of free cash flow")
    are not read; "$X per diluted share" after GAAP net income or net loss is
    diluted EPS, negative for a loss; "negative $5 billion" is negative;
  - a scale word is used as written; an unscaled figure takes the release's
    table scale only when the release declares exactly one, else it is not
    read.
  Text metrics carry `scaleBasis` (`AS_WRITTEN`,
  `RELEASE_TABLE_IN_MILLIONS`, ..., or null), the vocabulary
  `reconcile_metric_sources` uses. They stay LOW confidence with
  `TEXT_METRIC_VERIFY_REQUIRED`.
- **Filing tables (both runtimes; segment tables are Worker-only).** A
  table's scale is the one it states itself, else the statement in its
  lead-in nearest the table; "($000)", "(000s)" and "thousands of dollars"
  are thousands; millions remains the default when nothing is stated. A
  total row must carry amounts. A geographic candidate whose own text names
  long-lived assets, property and equipment or total assets and not revenue
  is not read. A segment table without a year column reads its "Total"
  column. Geographic `unitScale` gains `billions` (it was `actual`).
- **Guidance (both runtimes).** An unscaled revenue range takes the
  "(in millions ...)" statement of its outlook block (no sentence break
  between). Under an adjacent "GAAP Non-GAAP" column header a range takes its
  column's basis, the second column's range is an alternate, and an "N/A"
  first cell puts a range in the second column. Amounts accept "b" and "mn"
  and need a word boundary after the unit. `extract_guidance` reads a range
  whose low end has no unit with the high end's unit, as
  `get_guidance_history` already did.
- **Other readers.** `extract_funding_capex_schedule` reads two amounts
  joined by "and" as a range only when they run low to high. The driver
  ledger keeps a "B", "M" or "K" after a figure as its unit as written.
- **Release text parity.** Python's earnings release readers (period,
  metrics, guidance, guidance history, commentary) now read the same
  block-per-line text as the Worker (`_strip_html_blocks` mirrors
  `_stripHtmlTagsIdx`); both treat a no-break space as a space.
- **Effect.** Over 48 recent releases of 17 issuers, every changed text
  reading is a correction (MU, LITE, BE, MRVL, ORCL, DG, FN, COHR now read;
  AVGO capex, NVDA segment and ORCL services and annual readings refused).
  Live: MU revenue $54.23B and EPS $32.87; COHR segments $5.27B / $1.84B of
  $7.12B and China 11.43% of revenue; VRT segments $8.207B / $2.023B of
  $10.230B; SNDK guidance $10.3B-$10.8B GAAP with gross margin 83.0-84.9%
  GAAP and 83.0-85.0% non-GAAP.
- **Not changed in 2.5.20.** A "±" outlook table without a forward verb (MU's
  "Revenue $61.5 billion ± $1.5 billion") was not read and reported
  `NOT_DISCLOSED`, and a table with no stated scale defaulted to millions
  silently; both are resolved in 2.5.21 below.
- **Tests.** `scripts/test_extraction_rules.py` runs both runtimes on the
  release-figure and outlook-table cases;
  `scripts/test_worker_data_accuracy.py` and
  `scripts/test_edgar_html_parse.py` cover the COHR, VRT and stripper cases;
  `scripts/test_scenarios_and_schedule.py` and
  `scripts/test_guidance_and_drivers.py` cover the funding and ledger cases.

## Outlook Rows, Table Scales And Funding Classes (2.5.21)

- **Defect (all left open in 2.5.20).**
  - MU's outlook table ("Revenue $61.5 billion ± $1.5 billion", "Gross margin
    Approximately 85.95%", "Diluted earnings per share $37.84 ± $1.00" under
    the columns "GAAP(1) Outlook" and "Non-GAAP(2) Outlook") was not read, so
    `extract_guidance` reported `NOT_DISCLOSED`.
  - A filing table with no stated scale was silently read in millions.
  - A guidance range's basis could come from an earlier sentence. In older BE
    and LITE releases, "A reconciliation of GAAP to Non-GAAP measures..."
    before "• Revenue: $3.4B - $3.8B" labelled the revenue range non-GAAP.
  - `extract_funding_capex_schedule` labelled MU's capex estimate "net of
    proceeds from government incentives" and its nine-month investing cash
    flows `AWARDED_CONTINGENT`, and its "purchase obligations of approximately
    $2.93 billion ... expected to be paid within one year" `COMPANY_GUIDED`.
- **Guidance (both runtimes, `extraction-rules.ts` /
  `extraction_rules.py`).**
  - Outlook-table rows under a guidance or outlook heading may be stated as a
    midpoint ± tolerance (revenue, EPS; the bounds are computed exactly,
    `statedAs` `MIDPOINT_PLUS_MINUS`) or as one approximate value
    ("Approximately N%", "~N%" gross margin; low = high, new `statedAs` value
    `POINT_ESTIMATE`).
  - A two-column header may read "GAAP(1) Outlook Non-GAAP(2) Outlook", with
    an "Adjustments" column between them. Each range takes its column's basis
    and the second column's value is an alternate.
  - A range's basis is read from the clause holding its first amount (from the
    last sentence, semicolon or bullet break before it), not from the clause
    before its guidance keyword.
- **Table scales (geographic in both runtimes, segment in the Worker).**
  Table results carry `unitScaleSource`: `STATED_IN_TABLE`,
  `STATED_BEFORE_TABLE` or `ASSUMED_MILLIONS`. When assumed, amounts are still
  read in millions but carry the warning `UNIT_SCALE_ASSUMED` and a lower
  confidence (geographic HIGH to MEDIUM; segment rows and total MEDIUM to
  LOW); a percentage share is unaffected. The segment total now states its own
  confidence (MEDIUM, LOW when assumed) instead of a decorated default.
  `unitScaleSource` is null for XBRL facts.
- **Funding schedule (both runtimes).** New classification `REPORTED_ACTUAL`
  for amounts a reported period spent or received ("net cash used", "for the
  first nine months", "we spent"); it is checked before the others unless the
  sentence also uses forward wording. "Net of (proceeds from) government
  incentives" no longer makes an amount an award. "Purchase obligations" are
  commitments. "Expenditures for" and "purchases of property, plant and
  equipment" are capex wording.
- **Effect.**
  - MU guidance: revenue $60.0B-$63.0B GAAP (the non-GAAP alternate is the
    same), gross margin 85.95% GAAP and 86.25% non-GAAP (`POINT_ESTIMATE`),
    EPS $36.84-$38.84 GAAP and $37.15-$39.15 non-GAAP. Prior MU releases read
    the same way.
  - BE's latest revenue guidance basis stays `NOT_STATED`, and its "Non-GAAP
    Gross Margin: ~34%" is read as a non-GAAP point.
  - MU funding: the $27B 2026 capex estimate is `COMPANY_GUIDED`, the $2.93B
    purchase obligations `COMPANY_DISCLOSED_COMMITTED`, and the $19.60B and
    $10.20B nine-month expenditures `REPORTED_ACTUAL`; the CHIPS grants stay
    `AWARDED_CONTINGENT`.
- **Tests.** `scripts/test_extraction_rules.py` (MU outlook, BE clause),
  `scripts/test_scenarios_and_schedule.py` (MU funding sentences,
  `REPORTED_ACTUAL`), `scripts/test_edgar_html_parse.py` (scale sources),
  `scripts/test_worker_data_accuracy.py` (segment scale source) and
  `scripts/test_sec_fact_payloads.py` (geographic key lists).

## Cross-Checks, Stored Observations And Document Scales (2.5.22)

- **Defects (a user audit, findings F-003 to F-006, plus one 2.5.21
  follow-up).**
  - F-003: `get_consensus_forecast_curve`'s cross-check quietly fell back to
    Yahoo alone when Alpha Vantage answered "rate limited". The server's Alpha
    Vantage key is its own free-tier key (25 requests a day, shared by every
    caller and every tool that reads Alpha Vantage), separate from any key a
    user calls directly, so a direct call can work while the server's is
    exhausted. Only the per-cell `SINGLE_PROVIDER` showed it.
  - F-004: `reconcile_metric_sources`' `AGREED` rested on SEC plus one or two
    of the issuer release and Yahoo, and never Alpha Vantage, without saying
    which.
  - F-005: Yahoo and Alpha Vantage consensus matched to the last published
    digit (one upstream feed), yet their agreement read as `AGREED`, as if
    they were two checks.
  - F-006: daily consensus observations were stored
    (`consensus-history/{TICKER}/{date}.json`; AAOI has seven, including
    2026-10-04) but no tool could read one back, so AAOI's stored FY2027
    revenue consensus could not be retrieved.
  - 2.5.21 follow-up: VRT's segment table and MU's geographic table were
    marked `ASSUMED_MILLIONS` although the filings state "(Dollars in millions
    ...)" elsewhere (VRT's 10-K states it 14 times and no other scale).
- **Consensus cross-check (both runtimes, `evidence.ts` / `evidence.py`).**
  The curve carries `crossCheck` {`status`, `providersAnswered`,
  `providersFailed`, `cellsCompared`, `cellsIdentical`} and a `warnings` list.
  - `status` is `NO_PROVIDER`, `SINGLE_PROVIDER`, `NOT_COMPARED`,
    `NOT_INDEPENDENT` or `CROSS_CHECKED`.
  - A provider that failed (any status other than `OK` or `NO_DATA`) gives
    the warning `CROSS_CHECK_DEGRADED`, naming its status and message. For
    Alpha Vantage the message adds that the server's key and quota are
    separate from a key used directly.
  - A cell whose providers publish the same mean, high, low and analyst count
    has agreement status `IDENTICAL` (independence `NOT_INDEPENDENT`) instead
    of `AGREED`; other compared cells carry independence `UNVERIFIED`.
  - When every compared cell is `IDENTICAL`, `crossCheck` is
    `NOT_INDEPENDENT` with the warning `PROVIDERS_NOT_INDEPENDENT`.
- **Remembered denials (Worker).** A remembered Alpha Vantage daily-quota or
  entitlement denial now states when it expires ("remembered until <time>;
  not retried"). The daily quota denial lasts to the next UTC day.
- **Stored observations (both runtimes).** `get_consensus_forecast_curve`
  with `observation_date` (`YYYY-MM-DD`) returns the curve stored that day as
  written, with `storage` {`source` `STORED_OBSERVATION`, `key`, `schema`,
  `observedAt`, `serverVersion`, `buildSha`}.
  - `OBSERVATION_NOT_FOUND` lists the stored dates (`get_eps_revisions` lists
    them too).
  - `STORAGE_UNAVAILABLE` and `STORAGE_FAILED` (retryable) are returned when
    the store cannot be read.
  - A malformed date is `INPUT_VALIDATION_ERROR`.
- **Reconciliation (both runtimes).** `reconcile_metric_sources` adds
  `agreementBasis` {`providersRead`, `providersFound`,
  `providersAgreeingWithSec`, `independentChecks`, `notRead`:
  `["ALPHA_VANTAGE"]`}. `AGREED` means SEC plus `independentChecks` (one or
  two) of the issuer release and Yahoo.
- **Document scales (geographic in both runtimes, segment in the Worker).**
  `unitScaleSource` gains `STATED_IN_DOCUMENT`: the table and its lead-in
  state no scale, but every scale statement in the filing names the same one.
  A filing that states two scales (COHR mixes "$000" and "millions") lends
  none, and an unstated table stays `ASSUMED_MILLIONS`.
- **Effect.** Live AAOI on 2.5.21 returned Alpha Vantage `RATE_LIMIT` ("25
  requests per day") with Yahoo alone; 2.5.22 reports `crossCheck`
  `SINGLE_PROVIDER` with `CROSS_CHECK_DEGRADED`. VRT segment revenue is
  `STATED_IN_DOCUMENT` (millions, no warning, MEDIUM confidence); COHR is
  unchanged (`STATED_BEFORE_TABLE`).
- **Not changed.** The server's Alpha Vantage quota itself (a configuration
  matter: a paid `ALPHA_VANTAGE_API_KEY` on the Worker); reconciliation still
  does not read Alpha Vantage.
- **Tests.** `scripts/test_evidence.py` (identical feeds, failed provider),
  `scripts/test_evidence_tools.py` (stored observation read),
  `scripts/test_valuation_history_and_reconcile.py` (`agreementBasis`),
  `scripts/test_edgar_html_parse.py` and
  `scripts/test_worker_data_accuracy.py` (document scale).

## Impossible Provider Rows, Split Adjustment And Published Precision (2.5.23)

- **Defects (a user audit, findings F-001 and F-002, on Alpha Vantage
  EARNINGS_ESTIMATES for ANET).**
  - F-001: consensus rows whose average lies outside their own range. ANET Q1
    2021 revenue averaged 667.6M against a stated high of 652.7M, and Q2 2018
    did the same (519.8M vs 518.4M). A mean above its own high cannot be a
    consensus, yet any such row would have been compared like any other.
  - F-002: EPS history not split-adjusted. ANET's quarterly EPS consensus
    drops 2.73 → 0.73 at the 2021 4-for-1 split and 2.08 → 0.57 at the 2024
    one. One 2021 row mixes the two bases (current 0.73, 90 days ago 2.93).
  - The quarterly rows themselves never reach our outputs (both runtimes read
    only fiscal-year rows), and ANET's current fiscal-year rows are clean. The
    same defects in a fiscal-year row would have reached the curve and the
    revision windows, so both are now guarded for both providers.
- **Impossible rows (both runtimes, `evidence.ts` / `evidence.py`).** A
  provider row whose mean is above its high, below its low, or whose low is
  above its high has state `PROVIDER_INCONSISTENT` and `inconsistency`
  `MEAN_ABOVE_HIGH`, `MEAN_BELOW_LOW` or `LOW_ABOVE_HIGH`.
  - The row is shown as given. It is left out of the agreement check
    (`providersExcluded` [{`provider`, `reason`}]) and named in a
    `PROVIDER_ROW_INCONSISTENT` warning.
  - When every row in a cell is inconsistent, the cell's coverage and
    agreement are `PROVIDER_INCONSISTENT`.
  - In `get_eps_revisions` the provider gets the same `state`; its windows
    carry no change (window state `PROVIDER_INCONSISTENT`).
- **Split adjustment (both runtimes).** Yahoo's split history for the last two
  years is read with the consensus providers. The Worker reads chart events;
  the local server reads yfinance `Ticker.splits`.
  - The curve: an EPS gap between two providers that would be a `CONFLICT` is
    `NOT_SPLIT_ADJUSTED` when the ratio of their means is within 15% of the
    ratio of a split from the last 400 days (or of those splits combined).
    The agreement names the `split` {`date`, `ratio`} and
    `providersNotAdjusted`: the larger magnitude after a forward split, the
    smaller after a reverse one. Warning: `PROVIDER_NOT_SPLIT_ADJUSTED`.
    Coverage stays `PROVIDER_CONFLICT`. The curve carries `splitHistory`
    {`status`, `lookbackDays`, `recentSplits`}.
  - `get_eps_revisions`: every window has a `state`: `COMPARED`,
    `NOT_REPORTED`, `PROVIDER_INCONSISTENT` or `SPLIT_IN_WINDOW`. A window
    with a split inside it (one day of margin) carries no change, plus the
    `split`, with a `SPLIT_IN_WINDOW` warning. The result adds
    `splitHistory` (91-day lookback) and `warnings`.
  - An unreadable split history is not read as "no splits". `splitHistory.
    status` says so and a `SPLIT_HISTORY_UNAVAILABLE` warning states that
    EPS gaps or windows were not checked against splits.
- **Identical at published precision (both runtimes; F-013, the residual of
  2.5.22's F-005).** `IDENTICAL` compared raw numbers. So Yahoo's 5.20199 and
  Alpha Vantage's "5.2020" (the live ANET FY2027 EPS, with high, low and
  analyst count equal) read `AGREED` with independence `UNVERIFIED`, and the
  curve read `CROSS_CHECKED` instead of `NOT_INDEPENDENT`.
  - Figures a provider publishes as text now keep their decimal places
    ("5.2020" is four). Mean, high and low are compared at the coarsest
    precision any provider published.
  - A number (Yahoo's raw values) states no rounding and is taken as exact,
    so two numeric feeds still compare exactly.
  - Analyst counts must still be equal.
  - `IDENTICAL` now reports the actual `absoluteDiff` and `relativeDiffPct`
    (0.00001 for ANET) rather than zero.
- **Effect.** An ANET-shaped fixture (Alpha Vantage revenue mean above its
  high, and an Alpha Vantage FY0 EPS still on the pre-split count 16 days
  after a 4-for-1) gives the following:
  - Revenue `SINGLE_PROVIDER` with Alpha Vantage excluded, instead of a
    comparison against an impossible figure.
  - EPS `NOT_SPLIT_ADJUSTED` naming Alpha Vantage, instead of a 75%
    `CONFLICT`.
  - Yahoo's 30/60/90-day windows `SPLIT_IN_WINDOW`, instead of a −74% "revision".

## Debt Instrument Maturities, Ladder Basis And Reconciliation (2.5.24)

- **Defects (the Engine Zero adapter's report on AAOI's 10-Q, accession
  0001437749-26-026278, period 2026-06-30; checked against the filing and its
  XBRL).** The balances were right; the per-instrument rows and the year
  ladder were not.
  - F-008: the "Convertible Notes Maturing 2030" row had no maturity date
    and sat in no year bucket.
    - The filing tags no `DebtInstrumentMaturityDate` for it. Note 12 states
      "The 2030 Notes will mature on January 15, 2030".
    - The ladder held 58.9M of 188.1M of debt (31%) and said nothing about
      the rest.
  - F-007: the notes' row showed `carryingAmount` 124,900,000 and
    `faceAmount` 125,000,000.
    - The filer tags principal outstanding after a $75,000 conversion as
      `DebtInstrumentCarryingAmount`. The balance sheet carries the notes at
      129,142,000.
    - 125,000,000 is the face amount tagged on the 2024-12-18 issue date.
  - F-010: the China bank row (one `LoanAgreementMember`, 21,069,000) tags a
    revolver maturing 2027-06-25 at 2.6% and an equipment term loan
    maturing 2032-06-12 at 3%. The row kept whichever came first in the
    document.
  - F-009: the year buckets' `faceAmount` summed face amounts, else
    carrying amounts, else other tagged amounts. For AAOI every figure was
    a carrying amount.
  - F-011: the member `DebtMaturingDecember282028Member` became "Debt
    Maturing December 282028", with a 2026-12-28 maturity. The filer's own
    label linkbase also says 2028, so the tagged date is right and the name
    carries the typo.
- **Instrument rows (both runtimes, `capital-structure.ts` /
  `capital_structure.py`).**
  - `faceAmountDate` is the date the face amount is tagged at. An issue date
    before the period end means original principal.
  - `maturityDateSource` is `XBRL` or `FILING_TEXT`. A row with no tagged
    maturity takes the one date the filing text states in the year its name
    carries, and adds `maturityStatement`.
    - Example: "The 2030 Notes will mature on January 15, 2030" for
      "Convertible Notes Maturing 2030".
    - The text is searched only when an outstanding row has no maturity. If
      the text gives more than one date in that year, none is taken.
  - A row tagged with several maturities or coupons reports them all
    (`maturityDates`, `couponPcts`, present only then). Its `maturityDate`
    is the earliest, and the warning `MULTIPLE_MATURITIES` is raised.
  - Member labels split a day and year that run together after a month
    name ("December282028" becomes "December 28, 2028"). A name whose date
    differs from the tagged maturity gives `LABEL_DATE_DIFFERS_FROM_MATURITY`;
    the tagged date is used.
- **Year ladder (both runtimes).** Each `instrumentMaturitiesByYear` bucket
  has:
  - `amount`: each row's period-end carrying amount, else its face amount,
    else a tagged amount;
  - `amountBasis`: `CARRYING`, `FACE`, `TAGGED` or `MIXED`;
  - `faceAmount`: only rows with a tagged face amount, else null.
- **Coverage and reconciliation (both runtimes).**
  - `ladderCoverage` {`totalDebt`, `ladderedCarryingAmount`, `coveragePct`,
    `notLaddered`} measures, at carrying amounts like total debt, how much
    the ladder places. It lists outstanding rows with no maturity
    (`NO_MATURITY_DATE`).
  - `MATURITY_LADDER_INCOMPLETE` is raised when such a row exists and the
    filing tags no maturity ladder of its own.
  - `instrumentReconciliation` {`status` `RECONCILED` / `NOT_RECONCILED` /
    `NOT_COMPARABLE`, `instrumentsCarryingTotal`, `totalDebt`, `difference`,
    `rowsWithoutCarryingAmount`} compares the rows' period-end carrying
    amounts with total debt, within 0.5%. A mismatch is
    `INSTRUMENTS_DO_NOT_RECONCILE`: an instrument may be untagged, or a
    tagged amount may be principal rather than carrying value.
- **Effect.** AAOI's 10-Q now gives:
  - the 2030 Notes at 2030-01-15 (`FILING_TEXT`);
  - ladder coverage of 97.74%, up from 31%;
  - `NOT_RECONCILED` by 4,242,000, the gap between the notes' tagged
    principal and their 129,142,000 carrying value;
  - both China bank maturities;
  - a flag on the 2028/2026 label mismatch.
- **Not changed.** The dilution bridge's convertible principal is unchanged.
  The filing's own 124,925,000 principal and 129,142,000 net carrying amount
  are tagged on a different member (`LongtermDebtTypeAxis`, the 5.250%
  notes), and linking the two members would be an inference.

## Name-Dated Maturities And Comparable Reconciliation (2.5.25)

- **Defects (live 2.5.24 on VRT and MU).**
  - VRT raised `INSTRUMENTS_DO_NOT_RECONCILE` for 0 against 2,939,800,000
    of debt. Its notes carry face amounts only, and the one row with a
    period-end carrying amount was a zero-balance term loan. That compared
    nothing, not a gap.
  - MU's notes ("Due January 2031", "Green Bond Due 2032" and others,
    about 3.05B carried) and VRT's dated tranches ("Senior Notes Due 2036"
    and others) had no maturity. Neither filing tags
    `DebtInstrumentMaturityDate` for them, and neither states "will mature
    on" in its text, so they sat outside the year ladder.
- **Reconciliation (both runtimes).** `instrumentReconciliation` compares
  only when every outstanding row with an amount has a period-end carrying
  amount. Otherwise it is `NOT_COMPARABLE`, with `rowsWithoutCarryingAmount`
  saying how many rows lack one.
- **Maturity from the instrument's name (both runtimes).** A row with no
  tagged maturity and no stated one takes the date its name gives after
  "Due" or "Maturing":
  - "Due January 2031" gives `2031-01`, and "Due 2036" gives `2036`.
  - A full "Month D, YYYY" gives the day as well.
  - `maturityDateSource` is `INSTRUMENT_NAME`.
  - A date before the period end is not taken, and a name with no date
    ("Senior Unsecured Notes") or a range ("Due Between 2027 To 2036") gives
    none.
  - The info warning `MATURITY_FROM_INSTRUMENT_NAME` lists the rows dated
    this way. The year ladder needs only the year.
  - The filing text is still searched for such rows, so a stated exact date
    wins: AAOI's "Convertible Notes Maturing 2030" keeps 2030-01-15.
- **Ladder coverage (both runtimes).**
  - Zero-balance rows are no longer listed in `notLaddered` and no longer
    raise `MATURITY_LADDER_INCOMPLETE`.
  - `ladderCoverage` adds `ladderedAmount`: every bucket amount, on
    whatever basis it has. `ladderedAmountBasis` gives that basis, or
    `MIXED`.
  - This covers ladders built from face or tagged amounts (VRT, ASTS), for
    which `ladderedCarryingAmount` and `coveragePct`, at carrying amounts
    like total debt, read 0.
- **Effect.**
  - In an MU-shaped fixture, four rows are dated from their names and
    coverage is 70.47%. The two left out are a row with no date in its name
    and one naming a year before the period end; they are listed and
    warned.
  - AAOI is unchanged.
- **Not changed.** MU's `totalDebt` is still null (status `PARTIAL`): its
  debt is not tagged on the lines `totalDebt` reads. That is separate from
  the ladder and is not addressed here.

## Convertible Rows Checked Against The Balance Sheet (2.5.26)

- **Defect (F-007, still open after 2.5.24; reported by the Engine Zero
  adapter).**
  - AAOI's "Convertible Notes Maturing 2030" row showed `carryingAmount`
    124,900,000 under `us-gaap:DebtInstrumentCarryingAmount`. That figure
    is the "approximately $124.9 million" principal from Note 12.
  - The balance sheet, the Note 12 table and Note 10 all carry the notes at
    129,142,000.
  - 2.5.24's `INSTRUMENTS_DO_NOT_RECONCILE` showed the 4,242,000 gap, but
    the row itself still read as carrying value. `ladderCoverage` reported
    97.74% with nothing unplaced and did not say where the 2.26% went.
- **Convertible balance-sheet check (both runtimes, `capital-structure.ts`
  / `capital_structure.py`).**
  - Convertible rows with a period-end amount are summed and compared with
    the balance sheet's own convertible line (`ConvertibleNotesPayable` or
    `ConvertibleDebt`, else current plus noncurrent).
  - The comparison is reported as
    `instrumentReconciliation.convertibleBalanceSheet` {`status`,
    `concept`, `balanceSheetAmount`, `instrumentRowsTotal`, `difference`}.
  - When the two differ by more than 0.5%:
    - each convertible row is marked `carryingAmountBasis`
      `MAY_BE_PRINCIPAL`;
    - the warning `CARRYING_AMOUNT_MAY_BE_PRINCIPAL` names both figures and
      their concepts.
  - The row's amount is not replaced. The filing tags the balance-sheet
    line, and its 124,925,000 principal / 129,142,000 net carrying table, on
    a different member from the instrument, so moving them onto the row
    would be an inference.
- **Ladder gap (both runtimes).** `ladderCoverage.gap` {`amount`,
  `inNotLaddered`, `inReconciliationDifference`} splits the shortfall
  between the laddered carrying amount and total debt into two parts:
  - rows the ladder cannot place;
  - the reconciliation difference.
- **Ladder warning wording (both runtimes; from the live verification of
  2.5.25 on VRT).** `MATURITY_LADDER_INCOMPLETE` said the ladder "places 0 of
  total debt (0%)" while VRT's ladder held 2.95B of face amounts. When the
  ladder is on a face, tagged or mixed basis, the message now gives the
  laddered amount, its basis and its share at carrying amounts.
- **Effect.** AAOI's 10-Q now gives:
  - the 2030 Notes marked `MAY_BE_PRINCIPAL`;
  - `convertibleBalanceSheet` `NOT_RECONCILED` (129,142,000 against
    124,900,000);
  - `CARRYING_AMOUNT_MAY_BE_PRINCIPAL`;
  - `ladderCoverage.gap` {4,242,000; 0; 4,242,000}.
- **Not affected.** A filing with no separately tagged convertible line gets
  no check and no mark.

## Lease-Inclusive Debt Totals And Aggregate Debt Rows (2.5.27)

- **Defects (live 2.5.25 and 2.5.26).**
  - MU's `totalDebt` was null (status `PARTIAL`). MU tags its notes only
    dimensioned, and its balance-sheet debt only as `DebtCurrent` 582 and
    `LongTermDebtAndCapitalLeaseObligations` 5,140, which include finance
    leases. It also tags `DebtAndCapitalLeaseObligations` 5,722 and
    `FinanceLeaseLiability` 2,670. Its calculation linkbase gives debt as the
    difference, 3,052, the sum of its nine notes.
  - XOM tags its debt the same way (`DebtCurrent` 10,139 and
    `LongTermDebtAndCapitalLeaseObligations` 32,229) and nothing
    `totalDebt` read. The no-borrowings fallback therefore reported total debt
    0 with `NO_BORROWINGS_TAGGED`, and a net cash about 42B too high.
  - CMCSA and MPC were `PARTIAL` with null debt.
  - VRT's "Senior Unsecured Notes" row (face 2,100.0, tagged on the
    2026-03-03 issue date) is the sum of its 2036, 2046, 2056 and 2066
    notes. The filing says "issued $2,100.0 in aggregate principal amount of
    senior unsecured notes consisting of" them. The four are tagged beside
    it on the same axis, with no linkbase relationship, so the year ladder
    listed it as a further 2.1B unplaced.
- **Lease-inclusive totals (both runtimes).** When no `LongTermDebt`,
  `LongTermDebtCurrent` or `LongTermDebtNoncurrent` line is tagged, total
  debt comes from:
  - `DebtAndCapitalLeaseObligations`; or
  - `DebtCurrent` + `LongTermDebtAndCapitalLeaseObligations`, where
    `LongTermDebtAndCapitalLeaseObligationsCurrent` stands in for a missing
    `DebtCurrent`.

  How leases are treated:
  - When `FinanceLeaseLiability`, or its current and noncurrent parts, is
    tagged and does not exceed that figure, the leases are taken out. Their
    components carry negative amounts, and the info warning
    `FINANCE_LEASES_SUBTRACTED` states the formula.
  - Otherwise total debt includes the leases. The basis says so, and the
    warning `TOTAL_DEBT_INCLUDES_FINANCE_LEASES` is raised.
  - No short-term borrowings are added, since current debt in these totals
    holds them.
  - The lease-inclusive concepts count as borrowings, so a filer tagging only
    them is never reported as debt-free.
- **Aggregate rows (both runtimes).** An undated row is marked `aggregateOf`
  (the component instrument names) when all of these hold:
  - its amount is, within 0.5%, the sum of exactly one set of two or more
    other rows;
  - those rows are on the same amount basis;
  - each of them is named for the same kind of instrument (the row's last
    word, e.g. "Notes").

  A marked row stays listed but is left out of the year ladder, ladder
  coverage, `notLaddered`, the reconciliation and the filing-text maturity
  search. The info warning `AGGREGATE_ROW_EXCLUDED` names its components.
  When two sets of rows add up to it, no aggregate is named. The search
  covers at most 16 candidate rows.
- **Effect** (on the real filings, run locally through the Worker module):
  - MU: total debt 3,052,000,000, status `COMPUTED`, ladder coverage 100%,
    reconciliation `RECONCILED`.
  - XOM: 42,368,000,000 including finance leases, with the warning, instead
    of 0.
  - VRT: the 2.1B row is `aggregateOf` its four notes, and only the 17.2M
    range note is unplaced.
  - AAOI is unchanged.
- **Not changed.** A filer that tags `LongTermDebt` keeps the existing basis.
  Linkbases are still not read.

## Maturity Years Written In Words (2.5.28)

- **Defect (live 2.5.27 on FDX).** Seven FDX notes, about 5.5B, sat outside
  the year ladder. Their members name the year in words, for example
  "Five Point Two Five Percent Senior Unsecured Debt Due Twenty Thirty Six"
  and "One Point Eight Seven Five Zero Percent Due In February Two Thousand
  Thirty Four". Name dating (2.5.25) read only digits. No warning appeared,
  because FDX tags a maturity ladder of its own.
- **Years in words (both runtimes, `capital-structure.ts` /
  `capital_structure.py`).** Before a row is dated from its name, years
  written in words are read as digits. The displayed name is unchanged. Two
  forms count:
  - "Twenty" followed by a teen or a tens word, optionally with a ones word
    ("Twenty Thirty Six" is 2036, "Twenty Twenty Nine" 2029, "Twenty Ten"
    2010);
  - "Two Thousand", optionally followed by "And", then a number up to 99
    ("Two Thousand Thirty Four" is 2034, "Two Thousand And Nine" 2009).

  Spaces or hyphens may separate the words. "Twenty" followed by a ones word
  alone ("Twenty Five Percent", "Due Twenty Nine") is not a year. The
  existing rules still apply: the year must follow "Due" or "Maturing", and
  a date before the period end is not taken.
- **Effect.** In an FDX-shaped fixture, the notes date to 2036, 2029 and
  2034-02 (source `INSTRUMENT_NAME`). "Twenty Five Percent Notes" stays
  undated.

## Alpha Vantage Estimates: Daily Cache And Stale Fallback (2.5.29)

- **Defect (live 2.5.28 on ANET).** The server's Alpha Vantage key is on the
  free tier, which allows 25 requests a day. Once the quota was spent,
  `get_consensus_forecast_curve` reported `SINGLE_PROVIDER` with
  `CROSS_CHECK_DEGRADED` until the next UTC day, even though Alpha Vantage
  had answered for the same ticker hours earlier. `EARNINGS_ESTIMATES` was
  cached for 6 hours, so one ticker could use up to four requests a day.
- **Daily cache (both runtimes).** `EARNINGS_ESTIMATES` is cached for 24
  hours, so each ticker uses at most one request a day per cache.
- **Stored snapshots (both runtimes, `evidence-pack.ts` / `server.py`).**
  - Each good payload (one with estimate rows) is written once a day to the
    evidence store under
    `provider-snapshots/alpha_vantage/EARNINGS_ESTIMATES/<TICKER>/<YYYY-MM-DD>.json`
    (schema `yfmcp.provider-snapshot/1`). The store is R2 in the Worker and
    `YFMCP_EVIDENCE_DIR` locally.
  - The payload is kept as the provider published it, so the 2.5.23
    published-precision rule (F-013) still sees text such as "5.2020".
- **Stale fallback (both runtimes, `evidence.ts` / `evidence.py`,
  `staleSnapshot`).**
  - When the live request returns `RATE_LIMIT`, `TIMEOUT` or
    `PROVIDER_ERROR`, the store is read for the newest snapshot from that day
    back to 7 days earlier. This is at most eight reads, made only after a
    refusal.
  - A snapshot stands in only when it is for the same provider, operation and
    ticker, was fetched no later than now, and is at most 7 days old.
    `AUTH_ERROR`, `ENTITLEMENT_REQUIRED` and `SOURCE_UNCONFIGURED` never fall
    back: they are configuration faults and stay visible.
  - With a snapshot standing in, the Alpha Vantage input is `OK` and its
    `retrievedAt` is the snapshot's fetch time. It carries
    `staleFallback` {`liveStatus`, `liveMessage`, `ageHours`, `snapshotKey`}.
- **Output (additive; present only when a snapshot stood in).**
  - In `get_consensus_forecast_curve`:
    - the provider's entry in `providers` carries `staleFallback`;
    - `crossCheck.providersStale` lists {`provider`, `retrievedAt`, and the
      `staleFallback` fields};
    - the warning `PROVIDER_DATA_STALE` replaces `CROSS_CHECK_DEGRADED` for
      that provider. It names the refusal and the snapshot's age, and says
      that a difference from another provider may be a revision since then,
      not a disagreement.
  - The stale figures are compared like live ones: `IDENTICAL`, `AGREED` and
    `CONFLICT` are unchanged.
  - In `get_eps_revisions`, `providers` carries `staleFallback`, and
    `PROVIDER_DATA_STALE` says the provider's windows end at the snapshot's
    retrieval time, not at `asOf`.
- **Not changed.**
  - Other Alpha Vantage calls neither keep snapshots nor fall back.
  - Yahoo has no fallback.
  - The remembered daily denial (2.5.22) still saves the request.
  - A curve whose providers all answered live carries none of the new fields.
  - The day's stored consensus observation (2.5.22) is still the day's first
    curve. If that curve used a stale stand-in, the stored copy carries
    `staleFallback` and `PROVIDER_DATA_STALE` too.

## Companyfacts Coverage And Stale Figures (2.5.30)

- **Defects (F-015 revised and F-018, reported by the Engine Zero adapter on TSM against 2.5.28).**
  - SEC's companyfacts file for TSM (CIK 0001046179) has no ifrs-full fact from the FY2025 20-F (accession
    0001628280-26-025362, filed 2026-04-16). That filing contributed only one dei and one srt fact, so the
    newest IFRS annual period in the file is FY2024. Yahoo's figures matched the file; the gap is in the file
    itself.
  - Each path met this differently:
    - `get_historical_valuation_context` nulled every multiple as `RESULTS_STALE`, but left
      `denominators.LTM.revenue` at `status` `OK` with period end 2024-12-31 and method
      `LAST_FISCAL_YEAR_IS_LATEST`. On 2026-06-30 that figure was 546 days old, past the point's 500-day
      limit, and `balances` was `OK` for a 2024-12-31 balance date.
    - `extract_total_revenue` returned `SEC_FACTS_IFRS_ONLY`.
    - `extract_sec_filing_fact` with the FY2025 accession pinned returned `NO_FACT_FOR_ACCESSION`, naming
      only us-gaap concepts. It did not say that IFRS concepts were not searched.
    - `reconcile_metric_sources` returned `PERIOD_NOT_FOUND` for FY2025 and did not say why.
- **A pinned IFRS filing (both runtimes, `getFilingData` / `get_filing_data`).** When a pinned accession has
  no us-gaap fact and the filer's companyfacts holds ifrs-full facts but no us-gaap facts, the result is
  `SEC_FACTS_IFRS_ONLY` (status `SEC_FACT_NOT_AVAILABLE`, with `requestedAccession`), not
  `NO_FACT_FOR_ACCESSION`. Its message names the us-gaap concepts searched and says the accession's IFRS
  facts were not searched. Geographic revenue is unchanged.
- **Age on each figure (both runtimes, `valuationAtDate` / `valuation_at_date`).**
  - Every `denominators.LTM` and `denominators.LFY` entry with `status` `OK` adds `periodAgeDays` (period
    end to the point's date) and `stale` (true past the point's results limit, `stalenessLimitsDays.results`).
  - `balances` with `status` `OK` adds `balanceAgeDays` and `stale` (past `stalenessLimitsDays.balances`).
  - Status and value are unchanged; the multiples' `RESULTS_STALE` and the point warnings are unchanged.
- **`LATEST_ANNUAL_NOT_IN_COMPANYFACTS` (both runtimes, new `companyfacts-coverage.ts` /
  `companyfacts_coverage.py`).**
  - The latest annual report is the newest 10-K, 20-F or 40-F (amendments excluded) in SEC submissions filed
    on or before the date in question.
  - The warning is raised when companyfacts holds no us-gaap or ifrs-full fact from that accession (dei and
    srt facts do not count), unless an annual period companyfacts holds already reaches the report's period
    end (an amendment may have carried it).
  - The warning (severity `warning`) carries `form`, `accessionNumber`, `filed`, `reportDate` and
    `latestCompanyfactsAnnualPeriodEnd`. Its message ends "Figures here come from earlier reports, not from
    that filing."
  - Where it is raised:
    - each `get_historical_valuation_context` point, judged at the point's date;
    - `reconcile_metric_sources`, including on `PERIOD_NOT_FOUND`;
    - `extract_sec_filing_fact` and `extract_total_revenue` (and other `get_filing_data` reads) for annual
      filing types, when the read has no value or its value is from another accession than the latest
      annual report.
  - It is not looked for on a failed read, an unresolved ticker or an invalid period. A read whose value
    comes from the latest annual report costs no extra companyfacts read.
- **Effect** (TSM-shaped fixtures):
  - The pinned FY2025 20-F gives `SEC_FACTS_IFRS_ONLY` and `LATEST_ANNUAL_NOT_IN_COMPANYFACTS`.
  - Reconciliation of FY2025 is `PERIOD_NOT_FOUND` with the warning.
  - The 2026-06-30 valuation point shows `periodAgeDays` 546 and `stale` true on its FY2024 denominators and
    balances, plus the warning. The 2025-06-02 point (before the FY2025 20-F) shows 153 days, not stale, and
    no warning.
- **Not changed.** No value is filled in from elsewhere, and the accession's own XBRL is not read (F-015
  option 2, not taken up).

## Ticker Lookup, Zero Share Counts, Limits And EPS Basis (2.5.31)

From the Engine Zero adapter's red-team report against 2.5.30 (F-022, F-025, F-027, F-028, F-030). F-023, F-024,
F-026 and F-029 are scheduled for 2.5.32.

- **F-027: wrong company returned as success (both runtimes, `resolveCikForTicker` /
  `_resolve_cik_for_ticker`).**
  - Under parallel load, `list_sec_company_filings` {BE, 10-K} once returned CIK 0001986183 with no
    filings and `ok: true`.
  - Cause: when Yahoo's CIK and SEC's ticker index were both unreadable, the last fallback asked EDGAR's
    company-name search. For "BE" that search lists "BE 2023 Irrevocable Trust" (0001986183) first, ahead of
    Bloom Energy (0001664703). The result was then cached for the day.
  - A foreign ticker that SEC's index does not list (for example `IQE.L`) could match an unrelated company
    the same way, with no load needed.
  - Now: only EDGAR's ticker lookup (`CIK=<ticker>`) is asked; the company-name search is not. That lookup
    returns Bloom Energy for BE and no company for a symbol it does not know.
  - Not changed: a ticker that cannot be resolved while Yahoo and SEC are both unreadable is still reported
    as `TICKER_NOT_FOUND` on the filing tools. That covers the 13 transient failures in the report; telling
    a failed lookup from an unknown ticker there is left for a later release.
- **F-025: zero share count (both runtimes, `valuationAtDate` / `valuation_at_date`).**
  - BE's 2019-06-28 point counted 0 shares, so market cap was `OK` 0, enterprise value was debt less cash
    (420,664,000) and EV/Revenue 0.54 was `OK`.
  - Now a count of zero or less gives `marketCap` {status `SHARE_COUNT_NOT_POSITIVE`, value null} and the
    warning `SHARE_COUNT_NOT_POSITIVE`. Enterprise value is then `MARKET_CAP_NOT_AVAILABLE` and the multiples
    that use them are null.
  - The count is still shown in `shares`.
- **F-028: material filings limit (both runtimes, `list_sec_material_filings`).**
  - At most 20 filings are returned, as before.
  - The result adds `limit` {`requested`, `applied`, `maximum`} and `moreAvailable` (true when further
    matching filings exist in SEC's recent submissions after the last one returned).
  - A limit above 20 raises the warning `LIMIT_CAPPED`.
- **F-030: denominator concepts (both runtimes, `get_historical_valuation_context`).**
  - Each multiple adds `denominatorConcepts`: the concepts its denominator was read from, in component
    order. For EBITDA this includes the operating income and D&A concepts.
  - BE's EV/Revenue now names `RevenueFromContractWithCustomerExcludingAssessedTax` (1,135,346,000 for
    fiscal 2022, against 1,199,125,000 total revenue from `extract_total_revenue`).
  - Which concept is chosen is unchanged.
- **F-022: EPS basis (both runtimes).**
  - Yahoo Finance and Alpha Vantage do not say whether their EPS is GAAP or adjusted. Year-ago EPS differs
    from GAAP diluted EPS by +23% to -8% (VRT 4.20 against 3.41).
  - `get_consensus_forecast_curve`, `get_eps_revisions` and `get_earnings_analysis` add `epsBasis`
    {`basis` `UNKNOWN`, `determination` `NOT_DISCLOSED_BY_PROVIDER`, `note`}.
  - The basis is never inferred.

## Debt Rows, Customer Tables, Reporting Currency And Debt-Free Filers (2.5.32)

From the Engine Zero adapter's red-team report against 2.5.30: the four findings held back from 2.5.31 (F-023,
F-024, F-026, F-029). Both runtimes change alike.

- **F-023: ASTS capital structure (`instruments`, `capitalStructure` / `_instruments`, `capital_structure`).**
  - In 10-Q 0001193125-26-342550, the 2036 2.00% notes ($1,150.0M) and the UBS bridge loan ($420.0M) were
    missing as rows. The other rows carried issue-date amounts, and the year buckets summed to 3,185.0M against
    total debt of 2,971.9M.
  - Causes:
    - ASTS tags each note's period-end principal as `ConvertibleDebtNoncurrent`, which was not read.
    - Its loans sit on `CreditFacilityAxis`, which was not read at all.
    - The 2034 notes issued on 2026-07-20 (a subsequent event) were counted as period-end debt.
  - Now:
    - `ConvertibleDebtNoncurrent`, `ConvertibleDebtCurrent` and `LongTermLineOfCredit` are read as row amounts.
    - A `CreditFacilityAxis` or `LineOfCreditFacilityAxis` member is a row only when it has a non-zero amount
      at the period end, so a repaid facility or the bridge loan's restricted-cash collateral is not a row.
    - A row whose every fact is dated after the period end, or tagged as a subsequent event, has status
      `issued_after_period_end` and the info warning `ISSUED_AFTER_PERIOD_END`. It is listed, but kept out of
      the ladder, coverage and reconciliation.
    - When rows sum to total debt only after the filing's own `UnamortizedDebtIssuanceExpense` (or
      `DebtInstrumentUnamortizedDiscount`) is taken off:
      - `instrumentReconciliation.status` is `RECONCILED_NET_OF_UNAMORTIZED_COSTS`, with `unamortizedCosts`
        {`concept`, `amount`};
      - each row gets `carryingAmountBasis` `BEFORE_UNAMORTIZED_COSTS`;
      - the info warning `INSTRUMENT_AMOUNTS_BEFORE_UNAMORTIZED_COSTS` is raised.
    - Text maturities:
      - names with spelled-out years count ("Two Thousand Thirty Six" is 2036);
      - when two notes mature in the same year, the "will mature on" sentence that states the row's own coupon
        decides which date is which (ASTS's 2.00% notes mature 2036-01-15, its 2.25% notes 2036-04-15).
  - Result for ASTS:
    - seven rows;
    - buckets of 328.5M (2032) and 2,225.0M (2036);
    - Trinity and UBS listed as having no maturity date;
    - rows of 3,022.2M reconciling to 2,971.9M after 50.2M of issuance costs.
  - The added concepts also count as borrowings for the `NO_BORROWINGS_TAGGED` test.
- **F-024: customer concentration (`customerConcentration` / `customer_concentration`, and the tool
  wrappers).**
  - MRVL: a significant-customer table is now read.
    - The tool runs a second search of the same filing for `Customer A`, `Distributor A` and similar row labels.
    - A table row is read when its label is a placeholder such as "Distributor A" and its title is about
      revenue or sales, not receivables. The first percentage column is taken as the latest period.
    - MRVL gives Distributor A 37% and Customer A 14%.
    - The "ten (10) largest customers, inclusive of …" sentence had come out with an empty subject. It now gives
      the 82% aggregate.
  - AEHR:
    - "EV and power semiconductor revenues accounted for 17%" is a revenue category, not a customer. A
      lower-case subject that names no customer, client, distributor or reseller is skipped.
    - "three customers accounted for approximately 26%, 14% and 11%" is three customers, not a 26% total. A
      subject counting N customers, with N shares in a sentence naming fewer than N years, gives one share per
      customer. A ranked subject ("five largest") stays an aggregate.
    - A prose share within half a point of a table row is that row, so AEHR's table (26.3%, 14.2%, 10.9%)
      replaces the prose.
    - "our five largest customers" and "the Company's five largest customers" are one aggregate.
  - ANET: "Sales to one end customer" is no longer read as a customer named "Sales to one end customer".
    "Sales to" and "revenue from" are stripped from the subject. Result: two unnamed customers, 26% and 16%.
  - The Python tool's search now uses the Worker's 1,200-character context (it had used 1,500).
- **F-026: valuation history debt and EBITDA (`balancesAt`, `daFor`, `valuationAtDate` / `_balances_at`,
  `_da_for`, `valuation_at_date`).**
  - AEHR's enterprise value was `DEBT_NOT_TAGGED` at three dates, while `get_valuation_snapshot` took its debt
    as zero.
  - Now, when no debt concept is tagged at the balance date and the filing that reported that balance sheet
    tags no borrowing concept at any date, debt is 0:
    - `basis` is `NO_BORROWINGS_TAGGED` and `accessionNumber` names the filing;
    - the info warning `NO_BORROWINGS_TAGGED` is raised.
    - Companyfacts holds only undimensioned facts, so a filing that tags debt only by member is not seen.
  - BE's EBITDA was `NOT_AVAILABLE`, because BE tags `Depreciation` but never `AmortizationOfIntangibleAssets`.
    - When no amortization is tagged by the date, depreciation alone is added to operating income.
    - The info warning `EBITDA_DEPRECIATION_ONLY` is raised.
- **F-029: reporting currency by date and share-count age (`reportingCurrencyAt`, `valuationAtDate`,
  `historicalValuation`, `valuationHistoryFor`, and their Python mirrors).**
  - NBIS filed revenue in RUB through its FY2023 20-F and in USD from FY2024. Every date used RUB, so at
    2025-12-31 the results were `RESULTS_STALE` (FY2023), while the market cap used the 2024-12-31 count.
  - Now each date uses the currency of the newest annual revenue filed by then. When that filing tags two
    currencies, the filer's main currency wins.
    - Each point carries `reportingCurrency`.
    - The result lists `reportingCurrencies` when the dates use more than one.
    - FX is read per currency (`fxByCurrency`). A missing series names the date's own pair.
    - The cadence test (`foreignFiler`) uses the date's currency.
  - `shares.ageDays` is the count's age at each date.
  - For an annual-cadence (20-F/40-F) filer whose count is more than 183 days old, but within the 500-day
    limit, the info warning `SHARE_COUNT_AGED` is raised. The count is still used.
  - Not changed: the 2024-12-31 count of 361.5M was the latest filed at that date, which is correct point in
    time. A share move announced only in a 6-K never reaches companyfacts.

## Yahoo Press Releases After The Tab Endpoint Was Retired (2.5.33)

- **Reported:** `get_company_press_releases` (AEHR) gave Yahoo `HTTP 404` / `PROVIDER_ERROR` and
  `NO_OFFICIAL_RELEASE_SOURCE`, while `get_company_news` (AEHR) returned AEHR's own ACCESS Newswire release of a
  Sonoma production order. It reproduced on 2.5.31.
- **Cause:** Yahoo retired the press-releases tab endpoint (`POST /xhr/ncp?queryRef=pressRelease`). It answers HTTP
  404 for every ticker, AAPL included, so `yahoo_finance_press_releases` was failing for every ticker in the Worker.
  In Python, yfinance's `get_news(tab=...)` reads the same endpoint and returns an empty list. Python's Yahoo news
  feed was therefore empty too, while the Worker read Yahoo's news search feed, which still works.
- **Now (both runtimes; `collectYahooEvents` / `_collect_yahoo_events`):**
  - Both Yahoo feeds read the news search feed (`/v1/finance/search`; in Python, `yfinance.Search` through
    `_yahoo_search_news`). Python's Yahoo news now matches the Worker's.
  - `yahoo_finance_press_releases` keeps the company's own wire releases in that feed. Both conditions must hold:
    - the publisher is a press-release wire: ACCESS Newswire / Accesswire, GlobeNewswire, Business Wire,
      PR Newswire, Newsfile, PRWeb, EQS or Cision;
    - the headline leads with the company's name or ticker, which is the existing issuer rule.
  - Any other item is rejected as `NOT_ISSUER_WIRE_RELEASE`, counted in `rejectionCounts`:
    - another issuer's release naming the company (QuickLogic's PR Newswire release mentioning Aehr; REX Shares'
      ETF launch naming ASTS);
    - a news-service story (MT Newswires);
    - a publisher's article (Zacks).
  - `sourceStatus.yahoo_finance_press_releases.method` is `ISSUER_WIRE_RELEASES_IN_NEWS_FEED`.
  - The endpoint is no longer called.
- **Result for AEHR:**
  - `yahoo_finance_press_releases` is `OK`, with the Sonoma order release and AEHR's other ACCESS Newswire
    releases.
  - These items stay `MEDIUM` confidence wire releases, not decision-grade. A resolved SEC 8-K EX-99.1 still sets
    the tool's status (`SEC_EX99_FOUND`).
- **Not changed:**
  - The runtimes still format Yahoo timestamps differently (Python `...Z`, the Worker `...000Z`).
  - A release that Yahoo's search feed does not carry, such as one older than its 20 most recent items, is not
    found this way.

## Authorization Header Presence In health_check (2.6.0)

- **Added:** `health_check` returns one new field, `authorizationHeaderPresent` (boolean or `null`). It lets an
  operator confirm, through an MCP client such as a claude.ai custom connector, whether that client sends an
  `Authorization` header. This is the input to modernization brief section 7 V1 (static bearer auth for `/mcp`,
  Phase 1).
- **Worker:**
  - `true` when the inbound `/mcp` HTTP request carried a non-empty `Authorization` header.
  - `false` when the header is absent or blank.
  - `null` outside an `/mcp` request, such as the internal audit path.
  - Implemented with a per-request `AsyncLocalStorage` (`withRequestFacts` / `currentRequestFacts` in
    `worker/src/request-context.ts`). `worker/src/index.ts` sets it around `handleMcp`; `health_check` in
    `worker/src/tools.ts` reads it. The output schema lists the field.
- **Python (local stdio server):** always `null`, because stdio has no HTTP request. The output schema and tool
  description match the Worker.
- **Privacy:** only the boolean is recorded. The header value is never stored, logged or returned.
- **Not changed:** `/mcp` behaviour is otherwise the same. This release does not reject or authenticate any request.

## News Source Selection And Honest Coverage (2.7.0)

- **Scope:** `get_company_news`, `search_company_news`, `get_company_press_releases`, `get_public_event_timeline` and
  `verify_company_event`, in both runtimes. Release type: minor (additive fields and a documented behaviour repair).
  No field is removed.
- **News coverage (BR-NWS-007):** Python's coverage state now reads `PARTIAL` when a source reports
  `WEBSITE_NOT_AVAILABLE`, `DISCOVERY_NOT_FOUND`, `DISCOVERY_BUDGET_EXHAUSTED`, `FEED_NOT_FOUND` or `PARSE_ERROR`, as
  the Worker already did. It no longer reports `FULL` beside a failed source.
- **`sources` semantics (Q3h):**
  - Omitted or `null`: the tool's advertised default set applies.
  - An explicit list is used as given.
  - `[]`, or a list of only unsupported names, attempts no source and makes no request.
  - Before: Python replaced `[]` with the defaults, and the Worker queried every default source while reporting an
    empty `sourceStatus`.
- **New response fields on those five tools:**
  - `requestedSources`: the list as passed, `null` when omitted.
  - `defaultSourcesApplied`: `true` when `sources` was omitted or `null`.
  - `attemptedSources`: rows of `{sourceType, status}` for each source actually tried. It excludes `NOT_ELIGIBLE`,
    `NOT_NEEDED`, `UNCONFIGURED` and `attempted: false` rows.
- **A request that selects no source:** returns status `SOURCE_LIMITED_NOT_FOUND`, with `sourceCoverage` and
  `coverage.state` `PARTIAL`, and `coverage.recommendedNextAction` `CHECK_OFFICIAL_RELEASES`. This is neither a
  provider failure nor confirmed absence.
- **Timeline and verification defaults:** Python `get_public_event_timeline` and `verify_company_event` with
  `sources` omitted now report `sourceStatus` over the same 7-source default the Worker uses.

## Neutral Field Names And Deprecations (2.8.0)

- **Why:** the server is a universal information outlet. Strategy-specific labels are replaced by neutral measures and
  caller-chosen thresholds (owner decision Q14). Release type: minor, additive. No field is removed.
- **Mapping:**

  | Old field | Replacement | Note |
  | --- | --- | --- |
  | `doctrineUse` | `reliabilityClass` | `ALLOWED` is `RELIABLE`, `VERIFY_ONLY` is `VERIFY_BEFORE_USE`, `DIAGNOSTICS_ONLY` is `DIAGNOSTIC`, `BLOCKED` is `UNRELIABLE`. Python now emits `reliabilityClass` and never `doctrineUse`. |
  | `g2`, `opportunity`, `action`, `priceTarget`, `scenarioWeights`, `selectedMethod`, `selectedMultiple` | `decisionUse=EVIDENCE_ONLY` | These fields are always null. |
  | `bracket`, `inferredTag`, `tag`, `tagNote` | `priceAtOrAboveTarget`, `referenceTargetPct` | `priceAtOrAboveTarget` is the raw `currentPrice >= referenceTargetPrice`. |
  | `gatePass`, `gateThresholdUsd` | `meetsMinimum` | New optional input `min_adv_usd`; the response echoes it as `minAdvUsd`. |
  | `get_market_snapshot` `volume.liquidityGatePass` | `volume.meetsMinimum` | Also takes `min_adv_usd`; echoed as `volume.minAdvUsd`. |
  | `t1_inputs`, `t2_inputs`, `t4_inputs`, `t5_inputs` | `analystInputs`, `pricePositionInputs`, `earningsInputs`, `technicalInputs` | Identical contents. |

- **`DEPRECATED_FIELD` notice:** appears in the envelope's `meta.warnings` only, never in `data`, so evidence payloads
  and `evidenceCutId` are unchanged. Key order is `code`, `message`, `severity`, `fields`, `replacement`. It is not
  emitted in the legacy envelope (`MCP_ENVELOPE_V2=false`).
- **`min_adv_usd` validation:** it must be a positive number. Otherwise the tool returns `INPUT_VALIDATION_ERROR` with
  "min_adv_usd must be a positive number when supplied." Grouped mode rejects non-numeric values in its param-type
  check first.
- **Worker fix:** the output schemas for `calculate_price_target_distance` and `analyze_position_signals` are now
  attached to their public names (visible in the legacy envelope).
- **Window and removal:** old fields keep identical values for at least one minor version. Removal is planned for a
  later major version (modernization Phase 6). Consumers should move to the replacements now.

## News Domain Module Move (2.8.1)

- **Why:** internal restructuring only. Release type: patch. No behaviour change.
- **What moved:** the News & public events tools (`get_company_news`, `search_company_news`,
  `get_company_press_releases`, `get_sec_recent_events`, `get_public_event_timeline`, `verify_company_event`) and their
  helpers now live in `yfmcp/tools/news.py` and `worker/src/news.ts`.
- **Unchanged:** no tool name, input, output, schema, status, warning or ordering changed.
- **Compatibility:** `server.py` and `yahoo-finance.ts` re-export the moved names.
- **Verification:** the 1078-test parity harness passes, and tool listings (expanded and grouped) are byte-identical.

## Credentials (Grace Mode) And Request Limits (2.9.0)

- **Why:** Phase 1 security hardening (owner decisions Q2b, Q13, Q12). This release accepts a credential but does not
  yet require one. Release type: minor.
- **Credential:** a static token held as the Worker secret `MCP_TOKEN`. It is presented as
  `Authorization: Bearer <token>` or in the URL path `/mcp/<token>` (for clients such as the claude.ai connector that
  cannot send headers). The header wins if both are present. The comparison is constant-time, and the token never
  appears in responses, logs or errors. Use a URL-safe token (letters, digits, `-`, `_`).
- **Modes (`MCP_AUTH_MODE`):**
  - `grace` (default; set in `wrangler.toml`): requests without a credential are served; a wrong credential gets
    HTTP 401 `AUTH_REQUIRED`.
  - `enforce` (next release): requests without a credential also get 401.
  - Unknown values behave as `enforce` (fail closed). An `Authorization` header that is not `Bearer <token>` counts as
    no credential.
- **401 response:** HTTP 401 with `WWW-Authenticate: Bearer realm="yahoo-finance-mcp"` and a JSON-RPC error `-32001`
  with `data.code` `AUTH_REQUIRED`. With `MCP_TOKEN` unset, `/mcp/<anything>` is 404.
- **health_check:** gains `credentialTransport`: `"header"` | `"path"` | `"none"` for the calling request (`null` over
  the local stdio server). It never contains the credential.
- **Request limits:**
  - A body over 1 MiB gets HTTP 413, JSON-RPC `-32600`, `data.code` `PAYLOAD_TOO_LARGE`.
  - A JSON-RPC batch with more than one `tools/call` gets HTTP 400, `-32600`, `data.code` `BATCH_LIMIT_EXCEEDED`;
    nothing is executed.
  - An empty batch gets HTTP 400 Invalid Request (it was a silent 202).
- **Unchanged:** `/health` and `/` stay unauthenticated and make no provider calls; `/audit/mcp` keeps `AUDIT_TOKEN`;
  the local Python server (stdio) is unaffected.
- **Scripts:** canary, smoke and audit scripts send `Authorization: Bearer $MCP_TOKEN` when the `MCP_TOKEN` environment
  variable is set (GitHub Actions secret `MCP_TOKEN`).
- **Next:** the enforcement release flips `MCP_AUTH_MODE` to `enforce` after the claude.ai connector is re-added with
  the `/mcp/<token>` URL.

## Credential Enforcement (2.9.1)

- **Why:** Phase 1 security hardening, second release. Release type: patch (config only; no code change).
- **Enforcement:** `/mcp` now requires the credential (`MCP_AUTH_MODE = "enforce"` in `wrangler.toml`). Requests without
  one get HTTP 401 `AUTH_REQUIRED`, the same response 2.9.0 documents.
- **Clients:** the claude.ai connector must use `https://yahoo-finance-mcp.artinatw.workers.dev/mcp/<token>`. Scripts and
  CI send `Authorization: Bearer $MCP_TOKEN`.
- **Unchanged:** `/health` stays public and makes no provider calls; the local stdio server is unaffected.
- **Rollback:** set `MCP_AUTH_MODE = "grace"` (config change plus deploy).

## CI Supply Chain (Phase 1 release 3a; no runtime change, version stays 2.9.1)

- **Why:** Phase 1 security hardening, items S-07, S-09, S-10 and S-11. Nothing a tool returns changes.
- **Pinned actions:** every workflow `uses:` names a full commit SHA, with the release tag in a trailing comment.
  `scripts/test_workflow_sdk_versions.py` enforces this. To move an action, resolve the new tag's SHA with
  `git ls-remote --tags https://github.com/<owner>/<action>` and update the SHA and the comment together.
- **Token scope:** `deploy-worker.yml` now sets `permissions: contents: read` at the top level, so no job gets the
  default write token. `ir-source-discovery.yml` keeps `contents: write` and `pull-requests: write`, because it opens PRs.
- **Install scripts:** every `npm ci` runs with `--ignore-scripts`. esbuild and workerd load their platform binaries
  from optional dependencies, so the type-check, the transcript contract and `wrangler deploy` work without them.
- **Frozen Python deps:** CI installs the exact `uv.lock` versions with hash checking (`uv export --frozen`, then
  `pip install --require-hashes`) instead of resolving the latest releases on each run.
- **Upgrades:** `uv.lock` was refreshed to the latest versions within `pyproject.toml`'s ranges, which clears 38
  advisories across 9 packages. pandas moves to 3.x, which CI was already installing unpinned. Wrangler moves to
  4.149.0 (dev-only; it clears 4 high advisories in miniflare, undici and sharp). The Worker bundle is unchanged.
- **Audit gate:** a new `Dependency audit` CI job fails when `uv.lock` is out of date, when `pip-audit` reports a
  known vulnerability in the locked Python set, or when `npm audit` reports a high or critical advisory. If an
  advisory has no fix, record the exception here with a review date, and use `pip-audit --ignore-vuln <id>` in the job.

## Fetch Allow-list, CORS and Evidence Guard (2.10.0)

- **Why:** Phase 1 release 3b (items Q2a, S-13, S-05). Release type: minor. The behaviour changes below are deliberate.
- **Allow-list:** both runtimes (`worker/src/url-policy.ts`, `yfmcp/url_policy.py`) fetch only exact hosts: SEC
  `www.sec.gov`, `data.sec.gov`, `efts.sec.gov`; Yahoo `finance.yahoo.com`, `query1.finance.yahoo.com`,
  `query2.finance.yahoo.com`, `fc.yahoo.com`; `www.globenewswire.com`, `finnhub.io`, `www.alphavantage.co`,
  `api.marketaux.com`, `api.sec.or.th`, `api.company-information.service.gov.uk`; and the hosts of `approved` entries in
  `company-ir-page-registry.json` (0 today). https only, port 443, no credentials, no IP literals, `localhost`,
  `.local`, `.internal` or `.invalid`. Every redirect hop is re-checked, with a maximum of 5 hops.
- **Caller URLs:** `parse_public_transcript.url` and `index_earnings_release.source_url` accept only SEC Archives,
  `finance.yahoo.com` transcript pages and approved company IR pages. Anything else returns `INPUT_VALIDATION_ERROR`
  with "... is not on the fetch allow-list (<reason>)". `raw_text` is the escape hatch and never fetches.
- **`company_ir` source (Worker):** no more open-web discovery. It reads the approved registry only. Without an
  approved entry the row is `NOT_REGISTERED` (listed in `failedSources`), and the identity and discovery diagnostics
  are replaced by the registry keys `company_ir_page` already uses. Only `approved` registry entries are ever fetched,
  in both runtimes.
- **Adding a company IR site:** add or promote a registry entry to status `approved` with exact `allowedHosts` and
  `allowedPathPrefixes`, then release.
- **CORS:** `/mcp*` answers only `Origin: https://claude.ai` (reflected, `Vary: Origin`, `Authorization` allowed).
  `/` and `/health` keep `*`. Server-side clients (the connector, scripts) are unaffected.
- **Evidence:** tickers are validated before evidence listing, and list prefixes are validated in both stores.
- **Known risk:** an off-list redirect from `fc.yahoo.com` fails the Yahoo crumb refresh instead of being followed.
- **Rollback:** revert the release.

## Subrequest Budget Guard (2.11.0)

- **Why:** Phase 1 release 3c (V3 budget guard, news budget follow-through). Release type: minor. Last Phase 1 release.
- **Rule:** Workers Free allows 50 subrequests per invocation. The Worker stops starting new upstream work at 45
  (5 reserved). Every provider fetch counts, each redirect hop included, and so does every edge Cache API call
  (match, put, delete). Once the budget is spent, cache calls are skipped (a miss or a no-op) and fetches are refused
  without being sent.
- **Source reporting:** starved sources report status `PROVIDER_BUDGET_EXHAUSTED`: news and events `sourceStatus`,
  `attemptedSources` and coverage `failedSources` (with `reasonCode` null), transcript `attemptedSources`, and the
  consensus provider rows. Sources that ended OK keep their status.
- **Coverage:** coverage becomes `PARTIAL` and `recommendedNextAction` is `RETRY_RETRYABLE_SOURCES`. `failureMode`
  stays `WORKER_SUBREQUEST_LIMIT`.
- **Batch tools:** a starved ticker gets error code `PROVIDER_BUDGET_EXHAUSTED`, retryable, `recoveryAction`
  `RETRY_LATER`, with the original message kept.
- **Single tool calls:** a failed call that hit the budget returns `ok:false` with that code ("The per-request
  subrequest budget (45 of 50) was spent before this request could complete. Retry later.").
- **Warning:** one `meta.warnings` row with code `PROVIDER_BUDGET_EXHAUSTED` when the guard refused a fetch.
  Cloudflare's own "Too many subrequests" error maps to the same status.
- **Python:** mirrors the guard per tool call at its single fetch seam (`yfmcp/url_policy.py`). Documented
  divergences: Python has no edge cache to count, and yfinance requests (Yahoo quotes, Yahoo news, Yahoo consensus,
  batch per-ticker work) are not counted, so those paths cannot run out in Python.
- **Providers:** Finnhub, Alpha Vantage and Marketaux are quota-limited free tiers and stay in the defaults (owner
  decision 2026-10-16). `PROVIDER_OPT_IN_REQUIRED` is reserved for a provider the owner pays for later and is not
  emitted.
- **Rollback:** revert the release.

## Telemetry (2.11.1)

- **Why:** R4 Q10. A latency and error baseline is needed before Phase 4. Release type: patch. No tool output
  changes. Worker only: Python is a local stdio server with nothing to export.
- **Where:** Workers Analytics Engine dataset `yfmcp_telemetry`, binding `TELEMETRY`. One data point is written per
  `/mcp` request, early rejections included. Retention is 3 months. The Worker also writes one dispatch-start
  Workers Logs line, `{"t":"yfmcp","v":1,"action","domain","build"}`, so a CPU or resource kill can be attributed
  to an action.
- **Recorded:** index = `action`. Blobs: `domain`, `serverVersion`, `buildShaShort`, `outcome`, `errorCode`,
  `sizeBucket`, `credentialTransport`, `budgetExhausted`, `cacheSource`, `toolMode`, `timeoutClass`. Doubles:
  `latencyMs`, `responseBytes`, `httpStatus`, `upstreamRequests`, `subrequestsUsed`, `budgetRefusals`,
  `providerTimeouts`, `edgeCacheHits`, `edgeCacheMisses`, `providerRetries`.
- **Never recorded:** the token, the `Authorization` header, the request URL or path, request or response bodies,
  tool arguments, tickers, accession numbers, `raw_text`, provider content, IP or geo fields. `action` and `domain`
  come only from the server's own registry, never from caller strings.
- **Export:** `scripts/export_telemetry.py`, run daily at 02:17 UTC by `.github/workflows/telemetry-export.yml`
  and on manual dispatch. It uploads an aggregates-only JSON artifact, kept 90 days. It needs the GitHub secret
  `CLOUDFLARE_TELEMETRY_TOKEN` (Account Analytics: Read and Workers Observability: Write) and the existing
  `CLOUDFLARE_ACCOUNT_ID`. When a secret is absent, the workflow skips with a notice.
- **Manual query:** `python scripts/export_telemetry.py --end <ISO time>`.
- **Baseline rule:** at least 7 consecutive daily exports before the first Phase 4 merge.
- **Note:** `latencyMs` can read 0 for a request with no I/O.
- **Rollback:** revert the release.

### Excluding synthetic traffic (Phase 4 comparison)

- **Rule (F6):** rows with `credentialTransport` (blob7) = `header` are synthetic. Every CI canary, preview canary
  and deploy audit authenticates with an `Authorization: Bearer` header (`scripts/live_smoke_utils.py`). The
  owner's connector uses the path transport (`/mcp/<token>`). Blob7 is recorded on every row since 2.11.1, so
  the rule applies to the 2026-10-17 baseline too. Tooling only: no runtime change.
- **Daily export:** `excludingHeaderTransport` carries the same `totals` and per-action aggregates as
  `analyticsEngine`, from the same three queries with `blob7 != 'header'` added. Every other key is unchanged.
- **D10 bounds:** the exit comparison groups by action group (blob1 `domain`), weighted by `_sample_interval`.
  It judges only groups with 30 or more requests in both windows. A judged group passes when:
  - its p95 `latencyMs` is at most 1.2 × the baseline p95;
  - its p50 `latencyMs` is at most 1.2 × the baseline p50;
  - its error-plus-exception rate (`tool_error` + `rpc_error` + `exception` over all requests) is at most the
    baseline rate + 1 percentage point. `rejected` is reported but does not count as an error.
- **Run:** `python scripts/compare_telemetry_windows.py --compare-start <ISO> --compare-end <ISO> [--out report.json]`.
  It needs `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_TELEMETRY_TOKEN`. The baseline defaults to
  2026-10-17T00:00Z to 2026-10-24T00:00Z. The comparison window defaults to the 7 days before the current UTC hour.
- **Exit codes:**
  - 0: pass;
  - 1: a judged group failed;
  - 2: missing secrets or bad arguments;
  - 3: no group has enough volume to judge;
  - 4: a query failed.
- **Report:** the JSON lists the judged, skipped (low-volume) and failed groups, each group's metrics and checks,
  and the overall result. A failed group blocks the phase exit until it is explained or fixed.
- **Caveat:** once Phase 1b personal keys exist, a real user could use header transport. Revisit the rule then.
- **Logged invocations < Analytics Engine requests (OPS-TEL-4):** expected. Explained 2026-10-19 by two sources:
  - preview canaries (F6), which add Analytics Engine requests (about 15 per deploy) without matching logged
    invocations;
  - rejected-auth requests, which are written to Analytics Engine (one data point per `/mcp` request,
    rejections included) but are refused before dispatch, so they never log the dispatch-start line.

## Central Validation and Ticker Caps (2.12.0)

- **Why:** R4 Q3b (central ticker and accession validation) and Q3c/Q12 (per-tool ticker caps). Release type:
  minor. Deliberate behaviour changes: requests that were accepted before can now be rejected. Both runtimes.
- **Gate:** one per runtime, at the tool-call boundary, before any handler code or fetch. It runs in grouped and
  expanded mode alike (Worker: `tools.ts`; Python: the shared tool-call wrapper in `yfmcp/app.py`).
- **Checked:** `ticker` (a string, or each element of an array), `tickers` (each element), `subject` and `peers`
  (each element), with the unchanged grammar: 1 to 20 characters of `A-Z 0-9 . - ^ =` after trim and upper-case.
  `accession_number` and `accessionNumber` must be ASCII-only `NNNNNNNNNN-NN-NNNNNN` after trim (Python no longer
  accepts other Unicode digits).
- **Skipped:** empty or whitespace-only values and array elements are skipped and keep each tool's existing
  behaviour. Absent or null optional arguments are skipped, so "missing parameter" behaviour is unchanged.
- **Failure:** `INPUT_VALIDATION_ERROR` with the existing messages. The message echoes the raw caller value and
  names the public action. The Worker `get_historical_prices` used to echo the normalised value and now echoes the
  raw one. Python paths that resolved a document URL from the ticker before validating now reject before any fetch.
- **Newly validated in the Worker:** about 25 SEC, news, earnings and provider actions skipped ticker validation,
  and the Worker never validated accessions.
- **Not supported:** symbols containing `&`, such as `M&M.NS`, stay rejected (grammar unchanged; adding `&` needs
  an audit that every URL build encodes it, tracked as an R4 triage item).
- **Excluded:** arguments with their own grammars (Thai `fund_class_name` and `proj_id`, UK `company_number`,
  `document_url`, cut ids).
- **Caps:** one table, `tickerCaps` in `tool_catalog.json`, read by both runtimes and advertised as schema
  `maxItems`. Grouped-mode checkers enforce the length in both runtimes.
  - 10: `get_market_quote`, `get_company_profile`, `get_fund_profile`, `analyze_moving_average_position`,
    `analyze_volume_ratio`, `analyze_price_performance`, `get_analyst_rating_changes`,
    `analyze_earnings_momentum`.
  - 5: `analyze_financial_ratios`, `analyze_credit_health`, `get_company_news`, `get_analyst_consensus`,
    `get_technical_indicators`, `get_price_slope`, `get_short_momentum`, `get_market_snapshot` (compact and full).
  - `get_company_news` is 1 when `sources` includes `company_ir`, `company_ir_page` or `newswire` (names matched
    after trim and lower-case). The gate enforces it and the action description states it.
  - Peer tools keep their own limits: `compare_peer_valuations` 2 to 10, `get_historical_valuation_context` peers
    at most 5, `get_thai_fund_nav_batch` at most 20.
- **Oversize:** an array longer than the cap is rejected before any fetch with `BATCH_LIMIT_EXCEEDED`, "Too many
  tickers: N. Maximum is M per call for <action>.", `retryable` false. The cap is checked before the ticker
  format, so an oversize array gets this error whatever its contents. `BATCH_LIMIT_EXCEEDED` is now also a
  tool-level error code, with the same meaning as at HTTP level.
- **Snapshot cost (measured):** `get_market_snapshot` costs about 2 + 5n subrequests cold, in both modes: n=2 12 to
  13, n=5 27 to 28, n=10 52 to 53, n=20 103. The cap is therefore 5. Full mode goes from 2 to 5.
- **Truncation removed:** `_truncated` (`get_technical_indicators`, `get_price_slope`) and `truncated` and
  `droppedTickers` (`get_market_snapshot`) no longer appear, for in-cap requests too. Oversize is an error instead.
- **Batches:** a comma-separated string is still one symbol. Batches need an array.
- **Rollback:** revert the release.

## Exact Accession and CIK Lookup (2.13.0)

- **Why:** R4 Q3a (exact accession lookup) and Q4 (smoke CIK table out of production). Release type: minor. Both
  runtimes.
- **Q3a rule:** an explicit accession that is not in the company's recent SEC filings (`submissions.filings.recent`)
  returns `{ok:false, error:{code:"ACCESSION_NOT_FOUND", message:"Accession <acc> was not found in <TICKER>'s recent
  SEC filings.", requestedAccession, retryable:false}}`. There is no fallback to the latest filing or to the 20-F.
- **Applies to:** `get_sec_filing_index`, `search_sec_filing_text`, `extract_sec_filing_fact` (`fact_name` path),
  `extract_dilution_bridge`, `get_share_count_scenarios`, `extract_capital_structure`,
  `extract_funding_capex_schedule`, `extract_operating_driver_ledger` and `get_sec_filing_outline`.
- **Outline:** `get_sec_filing_outline` now honours an accession when `document_url` is absent. `document_url` still
  wins.
- **Unchanged:** structured `extract_sec_filing_fact` keeps `NO_FACT_FOR_ACCESSION`. `extract_china_exposure`
  (including the `query_sec_filing_index` china route), the `geographic_revenue` HTML fallback and
  `get_sec_filing_table` keep their previous not-found output (R4 triage items).
- **Known limit:** only recent filings are searched, so an older real accession reports `ACCESSION_NOT_FOUND`.
- **Q4:** the 11-ticker smoke CIK table is out of the production lookup, which is now cache, Yahoo `cik`, SEC
  `company_tickers.json`, then the EDGAR atom feed. The table lives as a test fixture,
  `scripts/parity/fixtures/ticker_cik_fixtures.json`, injected through `edgar._TEST_TICKER_CIK_FIXTURES` (Python) and
  `setTickerCikFixturesForTests` (Worker). Production effect only during a combined Yahoo and SEC-index outage.
- **Rollback:** revert the release.

## MCP SDK Pin (2.13.1)

- **Why:** R4 Q8 (MCP SDK pin and public APIs only). Release type: patch. Python only. The Worker has no MCP SDK
  (hand-rolled JSON-RPC) and redeploys unchanged apart from the version.
- **Pin:** `pyproject.toml` pins `mcp[cli]>=2.2,<3`. `scripts/test_workflow_sdk_versions.py` asserts both bounds. A
  major SDK uplift is out of scope for modernisation.
- **Public-API rule:** `yfmcp/app.py` defines `YfmcpServer(MCPServer)`, which overrides the public `tool()` and
  `list_tools()` methods and keeps its own ordered tool registry (with an `after=` position, used once, for
  `get_company_news`). Ticker `maxItems` (from `tickerCaps`) and the grouped input schemas are applied in the
  `list_tools()` override.
- **Removed:** the 1.x `fastmcp` import fallback, the `FastMCP.tool` monkey-patch, `reseat_tool_registration`, and
  all reads of `_tool_manager._tools`. Tests use the public seams `yfmcp.app.registered_handler(name)` and
  `registered_tool_fn(name)`.
- **Guard test:** `scripts/test_mcp_sdk_public_api.py` (in CI) fails if `yfmcp/`, `server.py`, `tool_groups.py` or
  `scripts/` access `_tool_manager`, `_mcp_server` or `_lowlevel_server`, or patch `MCPServer` or `FastMCP`
  attributes.
- **outputSchema:** the 97 `output_schema=` arguments were removed. The 2.x decorator silently dropped them, so the
  Python listing's outputSchema is still the SDK's auto-derived one. Python `tools/list` (expanded and grouped) was
  verified byte-identical before and after. Declaring explicit Python output schemas is a Q3i triage item, not done
  here.
- **Telemetry export fix (same release, script only):** `scripts/export_telemetry.py` now reads Workers Logs through
  the observability query `view:"events"` filtered on `$workers.scriptName eq "yahoo-finance-mcp"`, paging by offset
  (de-duplicated, 200-page cap).
  - The dispatch line is parsed from a structured or JSON-string `source`, from `$metadata.message`, or from
    top-level fields.
  - The invocation outcome is joined through the same `$metadata.requestId` from the `cf-worker-event`, falling back
    to `$workers.outcome`, then `unknown`.
  - A response without `result.events` gives `available:false` and exit code 1.
  - Reason: the 2.11.1 query returned 0 invocations because the JSON-string log line is not field-indexed and the
    outcome lives on the invocation event.
- **Rollback:** revert the release.

## Evidence and Decision-Grade Repairs (2.14.0)

- **Why:** R4b-1 of the approved Q3i triage (`analysis/yahoo-finance-mcp/Q3I_TRIAGE.md`). Release type: minor. Some
  results lose `decisionGrade`, and Worker evidence rows gain 6 keys. Both runtimes unless stated.
- **EX-99.1 exhibit rule:** over the filing-index exhibit rows, in index order, accept a type of `EX-99.1` or
  `EX-99.01` (also `99.1` and `99.01`), or a type starting `EX-99` whose description says PRESS RELEASE, EARNINGS
  RELEASE or RESULTS RELEASE. A bare `EX-99` without that wording is not accepted, and a match on the file name alone
  is not accepted. The index-HTML fallback is `EX-99\.0?1\b` in both runtimes.
- **Decision-grade predicate:** `xbrlContext` must be a plain object (an array does not qualify), and every required
  evidence field must be non-blank after trimming.
- **Section markdown (Python):** `get_sec_filing_section_markdown` is now marked as the Worker already marks it:
  `status: "SECTION_MARKDOWN_UNVERIFIED"`, `confidence: "NOT_DECISION_GRADE"`, `decisionGrade: false`, and warning
  `LIVE_SECTION_EXTRACTION_UNRELIABLE`, plus `SECTION_MARKDOWN_LOW_CONTENT` when the text is under 50 words.
  `doctrineUse` stays Worker-only until Phase 6.
- **Evidence row:** both runtimes emit 14 keys in this order: `url`, `sourceType`, `filingType`, `accessionNumber`,
  `filingDate`, `publishedAt`, `retrievedAt`, `excerpt`, `concept`, `periodEnd`, `tableIndex`, `rowLabel`,
  `columnLabel`, `rawRow`. The Worker gains `sourceType`, `publishedAt`, `retrievedAt`, `excerpt`, `concept` and
  `periodEnd`. String evidence gets the same 14 keys, with nulls. Falsy semantics are unchanged.
- **Table evidence:** `tableIndex` comes from `sourceTableId`, `rowLabel` from the first cell of the first row, and
  `rawRow` from the cells joined with `" | "` and the rows joined with `"; "`. `sourceRows` may be one flat row or a
  list of rows.
- **Geographic evidence:** `extract_sec_filing_fact` PARSED_TABLE evidence now names its filing (`documentUrl`,
  `filingType`, `accessionNumber`, `filingDate`), so a HIGH table figure carries evidence (the AAOI `evidence:null`
  case).
- **Analyst radar:** a mixed signal (a downgrade with a raised target, or an upgrade with a cut target) is `MIXED`.
  It is checked before `BULLISH` and `BEARISH`.
- **Wording:** the price-target deprecation message and the deprecated-field descriptions now read "use
  priceAtOrAboveTarget (direction); referenceTargetPct gives the price/target ratio". `tagNote` ends "Use
  priceAtOrAboveTarget."
- **Python news schema:** the stale status enum `[OK, PARTIAL]` was removed from the internal output schema. It is not
  advertised, so output does not change.
- **Telemetry:** the dispatch log line is logged as an object (field-indexed) instead of a JSON string. The export
  adds `coverage.warnings` code `LOGS_ZERO_WITH_REQUESTS` when Analytics Engine counts requests but Workers Logs
  counts 0 (exit code 0). `wrangler.toml` carries a comment on the observability flags.
- **Telemetry finding:** Analytics Engine counts about 15 pre-promotion preview-canary requests per deploy that never
  appear in the Workers Logs query, plus 3 `rejected:auth` requests per window that never log. A few long SEC and
  transcript invocations occasionally lack one of their log events, which shows as a missing invocation or
  `unknown`. The Phase 4 baseline should discount preview-canary requests.
- **Rollback:** revert the release.

## Requested Accessions, Outline Shape and Evidence Listing (2.15.0)

- **Why:** R4b-2, the last R4 release, from the approved Q3i triage. Release type: minor.
- **`extract_geographic_revenue`:** a non-empty `accession_number` is honoured. The figure, the evidence and the
  optional index (`detailLevel=raw`) come from that filing.
  - A miss returns `ACCESSION_NOT_FOUND` with message "Accession <acc> was not found in <TICKER>'s recent SEC
    filings.", `requestedAccession` and `retryable: false`. There is no fallback to the latest filing.
  - When pinned, the 20-F retry runs only if the pinned filing is a 20-F.
  - Unpinned calls are unchanged.
  - `extract_sec_filing_fact`'s structured pinned path keeps its 2.13.0 behaviour.
  - `query_sec_filing_index` geographic queries inherit this.
- **Exhibits:** `list_sec_filing_exhibits` and `get_sec_filing_exhibit_content` return `ACCESSION_NOT_FOUND`
  ("... was not found in <TICKER>'s SEC filings.") when the filing index returns HTTP 404, instead of an empty
  success. Older real accessions still work (there is no recent-filings check). 5xx responses and timeouts are
  unchanged.
- **`get_sec_filing_outline` (document path):** one shape in both runtimes: `ticker`, `accessionNumber`,
  `documentUrl`, `outline`, `status` (`OK`, `OUTLINE_NOT_PARSED` or `EMPTY`), `tableCount` and `warnings`
  (`TABLES_FOUND_OUTLINE_EMPTY`).
  - The item-heading fallback exists in both runtimes, and Python's HTML sanitiser now also runs in the Worker.
  - `accessionNumber` is the resolved filing's, else the one given with `document_url`, else null.
  - Errors use the standard `ok:false` envelope. Missing or invalid inputs give `INPUT_VALIDATION_ERROR`. A failed
    read gives `RATE_LIMIT` for a 429, `PROVIDER_TIMEOUT` for a timeout, else `PROVIDER_ERROR` with message
    "HTTP <status>".
  - Known remaining differences: the public tool called without `document_url` still uses the Worker's index-backed
    outline (finding F1, scheduled for the Phase 5 SEC unit), and outline titles strip inner tags differently (F2).
- **Evidence store listing:** the listing now reads fully (up to 10 pages of 1,000 keys) and keeps the newest
  `limit` entries (1,000 cuts, 400 history days), oldest first. Previously the newest entries could disappear
  beyond the cap. No new field.
- **Rollback:** revert the release.

## News Repairs, Batch B (2.19.0)

- **Why:** R-news part 3, Batch B of the approved Q3i triage (`RNEWS3_SPEC.md`, with planner decisions 6 to 9). Release type: minor. Outputs change identically in both runtimes.
- **`company_ir` is registry-only in both runtimes.** Python now uses the Worker's registry-only collector: no Yahoo pass-through, and a company IR source counts as used only with an approved registry entry. Without one, the row is `NOT_REGISTERED`.
- **IR-page registry states:**
  - **Candidate and disabled entries** raise the info warnings `COMPANY_IR_PAGE_CANDIDATE_AVAILABLE` and `COMPANY_IR_PAGE_DISABLED`. The collection status reads `SOURCE_LIMITED_NOT_FOUND`, not `PROVIDER_ERROR`.
  - **`allowedAction`:** a stale or validation-failed entry gets `registry_review_required`; an approved page with nothing in the window gets `fetch_configured_source`.
  - **Registry metadata:** approved rows carry the registry metadata in both runtimes.
- **IR-page parsing:**
  - The issuer is null when the registry has no `issuerName`.
  - A structured JSON item's summary reads `content_text`.
  - An `html_article` entry served a JSON content type, or an RSS/Atom body, fails with `SOURCE_VALIDATION_FAILED`.
- **`get_company_press_releases`:**
  - **Candidate-only status:** `COMPANY_IR_PAGE_CANDIDATE_ONLY` is used in both runtimes and listed in `limitationStatuses`.
  - **SEC outage:** the status is kept, `failureMode` comes from the failed SEC row, and coverage reads PARTIAL.
  - **Python now carries `secEvidence[].cikInt`.** `doctrineUse` stays Worker-only until Phase 6.
- **`verify_company_event`:**
  - **Date range:** it collects 365 days and applies the date range only when classifying.
  - **STALE:** a match older than 90 days is `STALE`, also when no range is given; an undated match counts as in range.
  - **Best-evidence confidence** depends on the source type: contextual news is LOW.
  - **Provider auth failure** (`AUTH_ERROR`): the result is `SOURCE_LIMITED_NOT_FOUND` with `failureMode: SOURCE_UNCONFIGURED`.
  - **Unregistered `company_ir`:** the same upgrade happens only when the caller names `company_ir` in `sources`. With default sources, the `NOT_REGISTERED` row stays in coverage but does not change the status, so a default verify can still return `NOT_FOUND`.
  - **Generic query:** a query made only of stop words returns `INPUT_VALIDATION_ERROR`.
  - **Relevance names:** relevance reads Yahoo `longName` and `shortName` plus the SEC submissions name, excluding any name equal to the ticker. An approved IR page counts as a HIGH source.
- **`extract_analyst_valuation_methods`:** attribution aliases are `longName`, `shortName` and `companyName`, excluding any equal to the ticker. `NEWS_UNAVAILABLE` is raised when the news call fails. An invalid ticker is handled in Batch C.
- **`get_sec_recent_events`:**
  - Python adds `coverage`, `sourceCoverage` and `sourceStatus`.
  - Blank `filing_types` gives `INPUT_VALIDATION_ERROR`.
  - `[]` means the defaults: 8-K, 10-Q and 10-K.
- **Rollback:** revert the release.

## News Repairs, Batch C (2.20.0)

- **Why:** R-news part 3, Batch C of the approved Q3i triage (`RNEWS3_SPEC.md`, planner decisions 10 to 23). Release type: minor. Outputs change identically in both runtimes unless a divergence is listed below.
- **Coverage when truncated:** `coverage.state` is `PARTIAL` when `truncatedSources` is non-empty and the tool returns fewer items than the caller's effective `max_results` (`get_company_news`, `search_company_news`, `get_public_event_timeline`).
  - A full output keeps its state, because the caller's limit cut it.
  - The Marketaux 3-row limit and the `get_company_news` pool cap have no effect: Marketaux never reports `hasMoreAccepted`, and `max_results` clamps to the pool size.
  - `status`, `failureMode`, `failedSources` and `recommendedNextAction` are unchanged.
- **Finnhub messages (both runtimes):**
  - `Finnhub auth error: HTTP <n>` for 401 and 403.
  - `Finnhub rate limited: HTTP 429`.
  - `Finnhub source unavailable: HTTP <n>` for any other HTTP status.
  - `Finnhub provider changed: unexpected response format`.
  - `Finnhub source unavailable: <error>` for non-HTTP failures.
  - Statuses are unchanged.
- **Marketaux:**
  - Wire fallback messages are `Marketaux wire fallback returned HTTP <n> (<STATUS>).` and `Marketaux wire fallback unavailable (<STATUS>).`
  - The Worker reads every row past the 3-row limit, so `acceptedCount` and the rejection counts cover all rows.
- **Zero inputs:** `max_results` and `lookback_days` of 0 clamp to 1 in both runtimes. Omitted means the default.
- **Entitlement:** a provider body containing "current plan" is `ENTITLEMENT_REQUIRED` with `USE_ALPHA_VANTAGE_EXPLICITLY` in Python, as in the Worker.
  - Documented divergence: the Worker's denial memory (6 hours for entitlement, the Alpha Vantage daily limit until 00:00Z) stays Worker-only.
- **Fail closed:** an unreadable Finnhub eligibility policy makes Finnhub `NOT_ELIGIBLE` with reasonCode `MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT` in Python, on both the `provider_gaps` and news paths.
- **Batches:**
  - The Worker validates each batch symbol with Python's ticker rule. A malformed symbol is an `INPUT_VALIDATION_ERROR` entry.
  - `__batchMeta` (`partialSuccess`, `successCount`, `errorCount`) is a Worker-internal key that the envelope lifts into `meta`. Python returns per-ticker results only. Documented divergence.
- **`duplicateGroupId`:** an opaque grouping id for dedupe. Callers must not parse it or compare it across runtimes.
  - It hashes the normalised title stem, the first 10 characters of the publication time and the issuer (else the ticker), upper-cased.
  - The Worker returns 8 hex characters (a 32-bit string hash). Python returns 16 hex characters (a truncated SHA-256). The formats differ by runtime; this release does not change them.
- **`extract_analyst_valuation_methods`:** the Worker now raises `RATING_CHANGES_UNAVAILABLE` when the rating-changes call fails, as Python does (finding F8).
- **IR pages (Python, aligned to the Worker):**
  - A structured entry whose content type is not json, xml, rss, atom, text or plain is `SOURCE_VALIDATION_FAILED` (F9).
  - The unregistered row carries `registryVersion` and `approvalStatus: "not_registered"` (F11).
- **Removed:** the dead Python `get_yahoo_finance_news` helper and the Worker's leftover schema key. Neither was a registered tool; both runtimes answer "Unknown tool name.".
- **Tool catalog:** `tool_catalog.json` Params lines now list `sources` (`get_company_press_releases`, `verify_company_event`) and `filing_types`, `max_results` (`get_sec_recent_events`).
  - `test_grouped_contract_parity` now fails on an accepted optional param missing from a Params line. 25 existing omissions in other groups are frozen in `_KNOWN_UNADVERTISED_PARAMS` (finding F14).
- **Not in this release:** F10 (structured IR-page RSS/Atom parsing in Python), F12 (verify stop words), F13 (Marketaux 404 and non-object status divergences) and F14.
- **Rollback:** revert the release.

## Coverage Field Consistency (2.20.1)

- **Why:** in 2.20.0 the truncation rule (`coverage.state` is `PARTIAL` when `truncatedSources` is non-empty and the tool returns fewer items than the caller's effective `max_results`) changed only `coverage.state`. The top-level `sourceCoverage` still came from the per-source computation, so a search could show `coverage.state` `PARTIAL` with `sourceCoverage` `FULL` (finding F15, seen live on `search_company_news` AAPL "iPhone" with `max_results` 50). Release type: patch.
- **Change:** in `get_company_news`, `search_company_news` and `get_public_event_timeline`, `sourceCoverage` takes the value of `coverage.state`, in both runtimes.
  - No new field or value.
  - The other news tools are unchanged: they have no caller limit, so the two fields already agree.
- **Rollback:** redeploy 2.20.0.

## Untagged Fact Confidence Defaults to LOW (2.21.0)

- **Why:** R5-shared (`R5SHARED_SPEC.md`, BR-ENV-001, approved Q3i call 21). The envelope gave a fact with no own or inherited `confidence` a default of `HIGH`, so a value nobody had rated read as high confidence. Release type: minor.
- **Change:** the envelope gives a fact (a dict with `value`, `low`, `valueRatio` and similar) that has no own or inherited `confidence` the default `LOW`, in both runtimes.
  - Until 2.21.0 the default was `HIGH` when `value`, `low` or `valueRatio` was non-null.
  - A fact with only `high` or `valuePct`, or with null values, still gets `NOT_DECISION_GRADE`.
  - Explicit and inherited confidences are unchanged. Normalisation is unchanged.
- **Producers reaching the default:** an audit of every producer found four.
  - `get_analyst_consensus` `priceTargets` and `get_earnings_analysis` `earningsEstimate[]` and `revenueEstimate[]` rows (Yahoo consensus data, already labelled contextual market data) now read `LOW` in both runtimes.
  - `get_ownership_holders` institutional, fund and insider rows read `LOW` in the Worker only. Python rows use yfinance's `Value` column and are not enriched as facts (finding F17).
  - `extract_dilution_bridge`: the `WARRANT_EXERCISE_NOT_OUTSTANDING` warning counts (inline-XBRL tagged warrant counts) are now tagged `HIGH` explicitly, so their output is unchanged.
- **No other effect:** no `status`, `recommendedNextAction`, `decisionGrade` or warning depends on the post-enrichment confidence, so only the `confidence` string changes.
- **Known differences, recorded and not changed:**
  - F16: `compare_earnings_actual_vs_estimate` still rates Yahoo estimate rows by the comparison's confidence.
  - F18: producer-level `HIGH` fallbacks.
  - F19: `get_analyst_consensus` provenance fields are present in Python only, so the Worker's `priceTargets.sourceType` reads "unknown".
  - F20: status-like strings such as `EXTRACTION_FAILED` stored in `confidence` normalise to LOW.
- **Rollback:** redeploy 2.20.1.

## News Repairs, Batch D (2.22.0)

- **Why:** the R-news batch D repairs (`RNEWS_D_SPEC.md`, from the approved `F_TRIAGE.md`; planner calls on F10 and F12). Release type: minor. There is no new field, enum value or warning code. Output values change for verify queries, Marketaux failure statuses and the company IR-page row, and the catalog text changes.
- **F12, verify stop words:**
  - The event-query stop list gains 19 function words: a, an, and, are, as, at, by, for, from, in, is, it, its, of, on, or, the, to, with.
  - Terms shorter than 3 letters were already dropped, so only and, are, for, from, its, the and with change matching.
  - Matching uses content words only.
  - A query made up only of filler words now returns `INPUT_VALIDATION_ERROR`.
- **F13, Marketaux failures:**
  - HTTP 404 is now `PROVIDER_ERROR` in both runtimes (the Worker returned `NOT_FOUND`; a provider 404 is not evidence of absence). The message is `Marketaux wire fallback returned HTTP 404 (PROVIDER_ERROR).`
  - A body that is not an object is `PROVIDER_CHANGED` in both runtimes, with the message `Marketaux provider changed: expected a data array.`
  - Under the legacy envelope (`MCP_ENVELOPE_V2=false`) a Worker batch entry keeps its own error code (for example `INPUT_VALIDATION_ERROR`), and `retryable` follows the V2 rule.
- **F11, IR-page row:** the `company_ir_page` row carries both `canonicalUrl` and `registrySchemaVersion` in both runtimes.
- **F10, local RSS/Atom parsing:** the local Python server now parses RSS and Atom bodies for registry-approved IR-page entries, like the hosted Worker.
  - Order: feed first, then structured JSON, then HTML.
  - A DOCTYPE or ENTITY declaration is refused.
  - The `Accept` header is the same as the Worker's.
- **XML entities decode once:** in both runtimes a double-escaped `&amp;lt;` stays the text `&lt;`. The change is in the shared decoder used by GlobeNewswire and the company IR feeds, and it affects only double-escaped input.
- **F14, catalog Params:** the `Params` line in `tool_catalog.json` now lists every accepted parameter for all actions. 25 actions gained parameters, and `get_analyst_recommendations` and `get_ownership_holders` gained a `Params` line. `test_grouped_contract_parity` now requires this with no exceptions.
- **Not changed:** the other findings, which are scheduled elsewhere in `F_TRIAGE.md`.
- **Rollback:** redeploy 2.21.0.

## Pinned Revenue Reads And Producer Confidence Fallbacks (2.23.0)

- **Why:** the R5-extractors repairs (`R5EXTRACTORS_SPEC.md`, from the approved `F_TRIAGE.md` rows F3, F4 and F18 and the Q3i rows BR-EXT-035 and BR-EXT-084). Release type: minor. There is no new field, enum value, warning code or `unresolvedReason`. Values change for pinned reads and for producer confidence fallbacks. The tool catalog and schemas are unchanged, so `manifestHash` and `schemaHash` should not change.
- **F3, pinned revenue reads:**
  - `extract_china_exposure` and `query_sec_filing_index` (`china_exposure`, `revenue_exposure`) with an `accession_number` now read revenue from that filing.
  - Their evidence names the pinned filing instead of the latest 10-K.
  - The public `extract_revenue_exposure` tool gains no parameter.
  - `total_revenue`, `segment_revenue` and `customer_concentration` queries still ignore the pin (finding F21).
- **F4, pinned geographic read with no XBRL fact:**
  - A pinned `geographic_revenue` read with no XBRL fact now reads that filing's own table, as the unpinned read does, instead of returning `NO_FACT_FOR_ACCESSION`.
  - Other fact types keep `NO_FACT_FOR_ACCESSION` and `SEC_FACTS_IFRS_ONLY`.
  - Evidence names the filing that was read, including its real form. A pinned 20-F asked for as 10-K reports filingType 20-F.
  - The existing 20-F retry still reads only the same accession.
- **F18 and BR-EXT-035, producer confidence fallbacks:**
  - A producer-level confidence fallback never claims HIGH.
  - An untagged value falls to LOW.
  - The geographic 20-F fallback falls to NOT_DISCLOSED, which is the primary path's default.
  - Python follows JS `??`: only null falls back.
  - Every upstream sets its own confidence today, so no live output is expected to change.
- **BR-EXT-084, warrant vesting:**
  - In share scenarios under `warrant_vesting=vested_only`, a warrant class with no `exercisable` key is unresolved ("count not tagged"). It was counted at outstanding.
  - `all` is unchanged.
  - This is latent: the capital-structure producer always emits the key.
- **Rollback:** redeploy 2.22.0.

## Pinned Filing Reads In query_sec_filing_index (2.24.0)

- **Why:** finding F21 (`F21_SPEC.md`), which completes F3 (2.23.0) for every query type. Release type: minor. There is no new field, enum value, warning code or `unresolvedReason`. Values and evidence change for pinned reads. The tool catalog and schemas are unchanged. Both runtimes change alike.
- **Pinned reads:**
  - With `accession_number`, `query_sec_filing_index` `total_revenue`, `segment_revenue`, `customer_concentration` and `risk_factor_mentions` now read that filing, so values and evidence come from it.
  - `total_revenue` takes the filing's own newest period, not a prior-year comparative.
  - The risk-factor block of `china_exposure` is pinned too.
- **Fail closed on a missing accession:**
  - An accession not among the company's filings returns `ACCESSION_NOT_FOUND` for those four query types, after their own parameter checks. `geographic_revenue_share` has done this since 2.15.0.
  - `china_exposure` keeps `FILING_NOT_FOUND_TRY_OTHER_TYPE`.
- **20-F retry:** a pinned `segment_revenue` read retries as 20-F only when the pinned filing is a 20-F, and reads the same accession.
- **Public tools:** the extractor tools gain no parameter. The catalog and schemas are unchanged.
- **Not changed:** finding F23 (segment query warnings pass-through differs between runtimes) is recorded, not fixed.
- **Rollback:** redeploy 2.23.0.

## Consensus Provenance And Estimate Confidence (2.25.0)

- **Why:** findings F19 (R5-fundamentals) and F16 (R5-earnings), see `R5_F19_F16_SPEC.md`. Release type: minor. There is no new field, enum value, warning code or `unresolvedReason`: every value already existed in the other runtime. The tool catalog and schemas are unchanged.
- **Consensus provenance (F19):**
  - The Worker's `get_analyst_consensus` now carries the top-level `source: "yahoo_finance"`, `evidenceClass: "CONTEXTUAL_MARKET_DATA"`, `decisionGrade: false` and `recommendedNextAction: "NONE"` that Python already set.
  - `priceTargets.sourceType` is therefore `yahoo_finance` in both runtimes (it was `unknown` in the Worker). `priceTargets.confidence` stays LOW.
- **Estimate confidence (F16):**
  - In `compare_earnings_actual_vs_estimate`, `estimate.revenue` and `estimate.eps` carry `confidence: "LOW"` in both runtimes. This matches how `get_earnings_analysis` has rated the same Yahoo consensus rows since 2.21.0.
  - The comparison's own top-level confidence is unchanged.
  - Not changed: `actual.eps` (finding F22).
- **Deprecation notice (R5-fundamentals):** in the Phase 6 major release, Python `get_ownership_holders` rows will be renamed to the Worker's camelCase shape (`Value` becomes `value`, F17). There is no runtime change now.
- **Rollback:** redeploy 2.24.0.

## Audit Repairs (2.25.1)

- **Why:** the 2.25.0 functional audit, findings F25 to F28 in `analysis/yahoo-finance-mcp/F_TRIAGE.md` section 5. Both runtimes change alike. There is no new field, status, warning code or enum value: every status used already existed.
- **F25, table unit scale:**
  - A scale statement about share counts is no longer read as a table's monetary scale. A scale after "except", or about shares, inside the same parenthetical is skipped.
  - Apple's "(In millions, except number of shares, which are reflected in thousands, and per-share amounts)" now reads as millions, not thousands. It was 1000x low on the table paths: `list_sec_filing_tables` and `get_sec_filing_index` `unitScale`, `index_earnings_release`, and every table extractor that uses the shared scale detector.
  - Python's filing-index builder took any "billion" in nearby prose. It now uses the Worker's rule: the table's own statement, else the last statement in the 800 characters before it.
- **F26, `extract_segment_revenue` on the filing-table path honours `period`:**
  - A fiscal year reads that year's column. For AAPL FY2024, Americas is 167,045M; it was the FY2025 column for any year.
  - If the filing's segment tables have no column for that year, the status is `PERIOD_NOT_FOUND`, with `fiscalYearsAvailable` and a message naming the years found.
  - A fiscal year with a quarterly filing type returns `INVALID_PERIOD`.
  - The table read also uses a pinned accession, which closes a gap left after F21 in 2.24.0.
  - Python gains the Worker's segment-table path. It previously returned `NOT_DISCLOSED` for filers such as AAPL whose segments are only in the filing table.
  - `query_sec_filing_index` `segment_revenue` confidence in Python now follows the rows, as in the Worker. A table answer is MEDIUM (LOW when the scale is assumed) rather than HIGH.
- **F27, latest earnings release without an SEC 8-K source:** `get_latest_earnings_release`, and the tools built on it, no longer treat a future Yahoo calendar date as the latest release. Only a date at or before now counts; otherwise the status is `NOT_FOUND`. Before, 7203.T reported 2026-11-05 at MEDIUM on 2026-10-20.
- **F28, `period` validation on the latest-only earnings tools:** `get_latest_earnings_release`, `index_earnings_release`, `extract_earnings_metrics`, `extract_guidance`, `extract_management_commentary`, `compare_earnings_actual_vs_estimate` and `get_earnings_call_transcript` allow only period "latest" in their schema. They now reject any other value with `INPUT_VALIDATION_ERROR` before any fetch. Before, "FY2025 Q4" silently returned the latest quarter (FY2026 Q3).
- **Tests:** 21 new parity cases in `scripts/parity/cases_audit_2251.py`.
- **Rollback:** redeploy 2.25.0.

## Tool Handler Table and schemas.ts (3.6.8)

- **Why:** internal restructuring only (Phase 5 unit 8, `analysis/yahoo-finance-mcp/PHASE5_ROUTER_SCHEMAS_SPEC.md`).
  Release type: patch. No behaviour change. Worker only; Python registers tools with decorators and has no switch.
- **What changed:** the tool definitions and input/output schemas moved from `worker/src/tools.ts` to
  `worker/src/schemas.ts`, and the `switch` in `_dispatchTool` became a `Map` from tool name to handler
  (`TOOL_HANDLERS`). The checks before dispatch are unchanged, and an unknown name still throws `Unknown tool`.
- **Adding a tool (Worker):** define it in `schemas.ts` and add one `TOOL_HANDLERS` entry in `tools.ts`.
- **Unchanged:** no tool name, input, output, schema, description, status, warning, cache key or ordering changed;
  `listVisibleTools()` is byte-identical in every mode.
- **Rollback:** redeploy 3.6.7.

## Evidence Pack and Valuation Module Move (3.6.7)

- **Why:** internal restructuring only (Phase 5 unit 7, `analysis/yahoo-finance-mcp/PHASE5_EVIDENCE_VALUATION_SPEC.md`).
  Release type: patch. No behaviour change.
- **What moved (Python):** the valuation handlers (historical valuation context, metric reconciliation, valuation
  snapshot and their helpers, 9 functions) from `server.py` to `yfmcp/tools/valuation.py`, and the evidence handlers
  (consensus curve, EPS revisions, evidence quality, valuation evidence pack and evidence cuts, 7 functions) to
  `yfmcp/tools/evidence_pack.py`. Neither module imports `server`.
- **What moved (Worker):** the 7 valuation functions and the constants only they use, from
  `worker/src/yahoo-finance.ts` to `worker/src/valuation-tools.ts`. `yahoo-finance.ts` re-exports the three handlers,
  so `tools.ts`, `index.ts` and the parity bundle are unchanged.
- **Tests:** patches that reached the moved code through `server` now patch the defining module; no expected value
  changed.
- **Topology:** `extract_topology.py` now follows TypeScript `export { … } from` re-exports, so callers resolve to the
  module that defines a function instead of the god file.
- **Unchanged:** no tool name, input, output, schema, status, warning, cache key or ordering changed.
- **Verification:** the moved Python functions keep their `ast.dump`; the moved Worker text keeps its line multiset
  (only import and re-export lines differ); tool listings (expanded and grouped) are byte-identical; the parity
  harness passes. `server.py` 972 → 460 lines, `yahoo-finance.ts` 630 → 346.
- **Rollback:** redeploy 3.6.6.

## Earnings Module Move and No More Back-Calls Into server.py (3.6.6)

- **Why:** internal restructuring only (Phase 5 unit 6, `analysis/yahoo-finance-mcp/PHASE5_EARNINGS_SPEC.md`).
  Release type: patch. No behaviour change.
- **What moved (Worker):** the latest earnings release and its SEC source, the release index, metrics, guidance,
  management commentary, actual vs estimate, guidance history and the transcript chain, from
  `worker/src/yahoo-finance.ts` to `worker/src/earnings.ts`: 57 functions and the constants and types only they use.
- **What moved (Python):** `get_guidance_history` from `server.py` into `yfmcp/tools/earnings.py`.
- **Back-calls removed:** `yfmcp/tools/earnings.py` no longer reaches into `server.py` by name at run time
  (`_server_attr`). It reads the same five functions from the modules that define them, so the objects called are
  the ones `server.py` re-exports. Tests that patched those functions on `server` now patch the defining module.
- **Unchanged:** no tool name, input, output, schema, status, warning, cache key or ordering changed.
- **Verification:** the moved Worker text keeps its line multiset (only import lines differ); the moved Python
  function keeps its `ast.dump`, and the five edited call sites match once mapped back; tool listings (expanded and
  grouped) are byte-identical; the parity harness passes with no expected value changed.
- **Rollback:** redeploy 3.6.5.

## Provider Module Move and Provider Rule Cases (3.6.5)

- **Why:** internal restructuring only (Phase 5 unit 5, `analysis/yahoo-finance-mcp/PHASE5_PROVIDERS_SPEC.md`).
  Release type: patch. No behaviour change.
- **What moved (Worker):** the Alpha Vantage and Finnhub fetchers, expanded institutional ownership, the historical
  put/call ratio, provider attempt, recovery and denial memory, and Companies House, from `worker/src/yahoo-finance.ts`
  to `worker/src/providers.ts`: 15 functions and the constants only they use.
- **What moved (Python):** Companies House (`get_uk_company_filings` and its four helpers) from `server.py` into
  `yfmcp/tools/provider_gaps.py`, which already held the Python scarce-provider tools.
- **Tests:** `scripts/parity/cases_providers.py` adds 88 cases for the provider rules BR-PRV-001 and 004 to 011. They
  pin today's output; eight Python/Worker differences are pinned as findings, listed in the spec, and left for a later
  decision.
- **Unchanged:** no tool name, input, output, schema, status, warning, cache key or ordering changed.
- **Verification:** the moved Worker text keeps its line multiset (only import lines differ); the moved Python
  functions keep their `ast.dump`; tool listings (expanded and grouped) are byte-identical; the parity harness passes
  with no existing expected value changed.
- **Rollback:** redeploy 3.6.4.

## Fundamentals and Analyst Module Move (3.6.4)

- **Why:** internal restructuring only (Phase 5 unit 4, `analysis/yahoo-finance-mcp/PHASE5_FUNDAMENTALS_SPEC.md`).
  Release type: patch. No behaviour change.
- **What moved (Worker):** company profile and stock info, statements and ratios, calendars and corporate actions,
  holders, ETF and fund profiles, analyst consensus, recommendations, rating changes and upgrade radar, earnings
  analysis and momentum, credit health, share-count trend, peer valuations and analyst valuation methods, from
  `worker/src/yahoo-finance.ts` to `worker/src/fundamentals.ts`: 31 functions and the constants only they use.
- **What moved (Python):** the same tools from `server.py` to `yfmcp/tools/fundamentals.py`: 52 functions and 13
  constants, including the two position tools left in `server.py` by 3.6.3.
- **Unchanged:** no tool name, input, output, schema, status, warning, cache key or ordering changed.
- **Compatibility:** `yahoo-finance.ts` and `server.py` re-export the moved names other modules and tests read. Tests
  that patch a moved helper now patch it in `yfmcp.tools.fundamentals`; no assertion changed.
- **Verification:** the moved Worker text keeps its line multiset (only `export` keywords and import lines differ);
  the moved Python functions and constants keep their `ast.dump`; tool listings (expanded and grouped) are
  byte-identical; the Worker bundle grows 24 bytes; the parity harness passes with no expected value changed.
- **Rollback:** redeploy 3.6.3.

## Pricing and Options Module Move (3.6.3)

- **Why:** internal restructuring only (Phase 5 unit 3, `analysis/yahoo-finance-mcp/PHASE5_PRICING_SPEC.md`).
  Release type: patch. No behaviour change.
- **What moved (Worker):** quotes and the market snapshot, historical prices, price and volume statistics, technical
  indicators, short interest, price targets, option expiries and chains, options summary and flow, put hedges, the
  screener and ticker search, from `worker/src/yahoo-finance.ts` to `worker/src/pricing.ts`: 51 functions and the
  constants, caches and types only they use.
- **What moved (Python):** the same tools from `server.py` into the existing `yfmcp/tools/pricing.py`: 21 functions.
  `get_position_score_inputs` and `analyze_position_signals` stay in `server.py` until the fundamentals handlers they
  call move (Phase 5 unit 4).
- **Unchanged:** no tool name, input, output, schema, status, warning, cache key or ordering changed.
- **Compatibility:** `yahoo-finance.ts` and `server.py` re-export the moved names other modules and tests read. Tests
  that patch a moved helper now patch it in `yfmcp.tools.pricing`; no assertion changed.
- **Verification:** the moved Worker text keeps its line multiset (only import lines differ); the moved Python
  functions keep their `ast.dump`; tool listings (expanded and grouped) are byte-identical; the Worker bundle grows 19
  bytes; the parity harness passes with no expected value changed.
- **Rollback:** redeploy 3.6.2.

## SEC Extractors and Exposure Module Move (3.6.2)

- **Why:** internal restructuring only (Phase 5 unit 2, `analysis/yahoo-finance-mcp/PHASE5_EXPOSURE_SPEC.md`).
  Release type: patch. No behaviour change.
- **What moved (Worker):** the SEC extractors and exposure tools (total, segment and geographic revenue, customer
  concentration, risk factors, generic and China exposure, capital structure, dilution bridge, funding schedule,
  operating driver ledger, share-count scenarios) from `worker/src/yahoo-finance.ts` to `worker/src/exposure.ts`: 43
  functions and the constants, caches and types only they use. `sec.ts` now imports them from `exposure.ts`.
- **What moved (Python):** the same code from `server.py` to `yfmcp/tools/exposure.py`: 38 functions (including
  `query_sec_filing_index`, left behind by 3.6.1) and 13 constants, plus the two earnings-release readers the driver
  ledger calls.
- **Unchanged:** no tool name, input, output, schema, status, warning, cache key or ordering changed.
- **Compatibility:** `yahoo-finance.ts` and `server.py` re-export the moved names other modules and tests read. Tests
  that patch a moved helper now patch it in `yfmcp.tools.exposure`; no assertion changed.
- **Verification:** the moved Worker text keeps its line multiset (only `export` keywords and import lines differ);
  the moved Python functions and constants keep their `ast.dump`; tool listings (expanded and grouped) are
  byte-identical; the Worker bundle grows 19 bytes; the parity harness passes with no expected value changed.
- **Rollback:** redeploy 3.6.1.

## SEC Retrieval Module Move (3.6.1)

- **Why:** internal restructuring only (Phase 5 unit 1, `analysis/yahoo-finance-mcp/PHASE5_SEC_SPEC.md`). Release
  type: patch. No behaviour change.
- **What moved (Worker):** the SEC EDGAR retrieval code (filing selection and accession resolution, filing data,
  filing index, outline, sections, tables, documents, exhibits, filing text search, XBRL facts and filing intelligence)
  from `worker/src/yahoo-finance.ts` to `worker/src/sec.ts`: 54 functions and the constants and types only they use.
- **What moved (Python):** the same code from `server.py` to `yfmcp/tools/sec.py`: 55 functions and 21 constants.
  `query_sec_filing_index` stays in `server.py` until the extractors it calls move (Phase 5 unit 2).
- **Unchanged:** no tool name, input, output, schema, status, warning, cache key or ordering changed.
- **Compatibility:** `yahoo-finance.ts` and `server.py` re-export the moved names other modules and tests read. Tests
  that patch a moved helper now patch it in `yfmcp.tools.sec`; no assertion changed.
- **Verification:** the moved Worker text keeps its line multiset (only `export` keywords and import lines differ);
  the moved Python functions and constants keep their `ast.dump`; tool listings (expanded and grouped) are
  byte-identical; the Worker bundle grows 13 bytes; the parity harness passes with no expected value changed.
- **Rollback:** redeploy 3.6.0.

## Form 4: Every Transaction of a Filing (3.6.0)

- **Why:** the scheduled Q3i repair BR-SHR-016, multi-line Form 4 parsing (owner decision 2026-10-31, "Add a list
  of every transaction"; `analysis/yahoo-finance-mcp/FORM4_MULTILINE_SPEC.md`). Release type: minor, because one
  field is new. Both runtimes.
- **New field `insiderTransactions`** on each Form 4 item from the SEC source (`get_sec_recent_events`, company news
  with the `sec` source). It lists every transaction row of the filing, each with the same keys as
  `insiderTransaction`: owner, role, transactionCode, transactionLabel, shares, price, value, ownershipForm and
  transactionDate. Order: the non-derivative rows (Table I) in document order, then the derivative rows (Table II).
  For an HTML primary document, the tables are read in page order with the existing row rules.
- **`insiderTransaction` is unchanged:** it is still the first row, the same one as before. The title is unchanged.
  When the filing has more rows, the summary ends with "N more transaction(s) in this filing." Example: an option
  exercise (M, 10,000 at $5.00) followed by the sale of those shares (S, 10,000 at $25.50). Before, only the exercise
  was visible. Now `insiderTransactions` has both, and the summary says "1 more transaction in this filing."
- **Unchanged:** `schemaHash` and `manifestHash`, because news items have no declared item schema. No warning code
  was added.
- **Tests:** in `scripts/parity/cases_pricing_evidence_tools.py` the pinned multi-line case now asserts the first
  row in both runtimes. Three new cases cover the full list with its summary, derivative rows after the
  non-derivative rows, and a one-row filing.
- **Rollback:** redeploy 3.5.0.

## Fundamentals: Credit Health Needs a Full Year and a Positive Earnings Base; Beat Streak From the Newest Quarter (3.5.0)

- **Why:** R5-fundamentals, the scheduled Q3i repairs BR-FND-004, BR-FND-006, BR-FND-007 and BR-SHR-010/012
  (`analysis/yahoo-finance-mcp/R5FUNDAMENTALS1_SPEC.md`). Release type: minor, because published figures change
  and `debtTier` has one new value, `NEGATIVE_EARNINGS_BASE` (owner card 2026-10-30). No field or warning code is
  new. Both runtimes.
- **TTM needs four quarters (BR-FND-004/006), `analyze_credit_health`:** a trailing-twelve-month sum (operating
  income, D&A, provider EBITDA, interest) now needs a value in each of the 4 newest income quarters. A row with
  fewer (a company with only 3 quarters, or a gap inside the 4) is not a year: that component is null and listed in
  `missingComponents`, so the ratios built on it are null and `dataQuality` is PARTIAL. The row picked is the same
  as before (the first in the existing order with any value). When D&A is incomplete and provider EBITDA is
  complete, operational EBITDA uses the existing provider fallback and its `OPERATIONAL_EBITDA_UNAVAILABLE`
  warning. Before, 3 quarters were summed and treated as a year, which understated EBITDA and coverage; the
  Worker also reported `dataQuality` OK.
- **Positive earnings base (BR-FND-007):** `netDebtToEbitda` is computed only when operational EBITDA is above 0.
  Below 0 (with net debt known), `debtTier` is `NEGATIVE_EARNINGS_BASE`, leverage and `creditStressFlag` are null
  (listed in `unavailableMetrics`), and the existing `NEGATIVE_EARNINGS_BASE` warning is kept. Exactly 0 gives a
  null tier, as before. Before, a loss-maker with net debt read as a negative ratio and the CLEAN tier (RIVN, LCID
  and PLUG on 3.4.0). Interest coverage is still reported, negative when EBIT is. Net cash with positive EBITDA
  still gives a negative ratio and CLEAN.
- **Total debt only (BR-FND-006):** Python no longer reads `Long Term Debt` as total debt when no total-debt row
  exists, as the Worker already did. Long-term debt alone leaves out current debt. Total debt is then missing.
- **Beat streak (BR-SHR-010/012), `analyze_earnings_momentum` and the snapshot that embeds it:** `currentBeatStreak`
  counts consecutive beats from the most recent quarter, ordering the rows by their quarter date (Yahoo lists them
  oldest first, and both runtimes counted from the oldest row). If a counted row has no quarter date, the order
  is unknown and the streak is null. `beatRate`, `beatCount` and the average surprise are unchanged.
- **Unchanged:** `schemaHash` and `manifestHash` (`debtTier` is a free string in the schema); every field already
  allowed null.
- **Tests:** in `scripts/parity/cases_pricing_evidence_tools.py` the three pinned credit cases and the two pinned
  beat-streak cases now assert the repaired behaviour in both runtimes, and the case with a skipped row now
  expects a streak of 0 (its newest quarter is a miss). New cases cover a loss-maker with net cash, a quarter
  gap, newest-first rows and a missing quarter date. Two Python unit tests now use four-quarter fixtures.
- **Rollback:** redeploy 3.4.0.

## Valuation: Withhold Partly Tagged Debt and Unresolved Parent Net Income (3.4.0)

- **Why:** R5-valuation part 2, the scheduled repairs BR-VAL-006 and BR-VAL-025 (owner decision 2026-10-30,
  "Withhold the figure"; `analysis/yahoo-finance-mcp/R5VALUATION2_SPEC.md`). Release type: minor, because two
  status values are new: debt `INCOMPLETE` and enterprise value `DEBT_INCOMPLETE`. Both runtimes.
- **Partly tagged debt (BR-VAL-006), `get_historical_valuation_context`:** debt still comes from the first debt
  concept group with a component tagged at the cash balance date. The group is now complete only when every
  component that the cash balance's filing tags at any date is also tagged at that date. If one is tagged only at
  another date (a comparative column), debt is `{status: "INCOMPLETE", value: null}` with the group's `concepts` and
  the tagged `components`, and enterprise value is `DEBT_INCOMPLETE` (null), so the EV multiples are null. Example: a
  10-Q tags current debt 30M at 30 June and non-current debt 280M only at the prior year end. Before, debt read
  30M and EV was understated by 280M. A component the filing never tags, or one tagged only by another filing,
  leaves the group complete as before.
- **Untagged NCI (BR-VAL-025), `reconcile_metric_sources` net income:** when ProfitLoss is the chosen US-GAAP
  concept and either the first-filed or the latest row has no
  `NetIncomeLossAttributableToNoncontrollingInterest` in the same filing, period and unit, both SEC rows are
  `NOT_RESOLVED` with a null value. The ProfitLoss row stays as `evidence`, with `filedValues`. Before, raw ProfitLoss
  was reported as net income, though it includes the minority holders' share. The reconciliation then reads as it
  does without an SEC baseline (PARTIAL when another provider found the value). Matched NCI is unchanged
  (`PARENT_DERIVED_FROM_PROFITLOSS`).
- **Not changed:** IFRS ProfitLoss without the owners-of-parent concept is still reported as is (a recorded
  finding, out of this decision's scope). `get_historical_valuation_context` P/E also still reads ProfitLoss as
  is when `NetIncomeLoss` is untagged; that is a separate candidate finding.
- **Unchanged:** no field or warning code was added. `schemaHash` and `manifestHash` stay the same.
- **Tests:** in `scripts/parity/cases_valuation.py` the partial-group case and the four missing-NCI cases now
  assert the new behaviour in both runtimes. Two new cases cover a group component the filing never tags and one
  tagged only by another filing. `scripts/test_valuation_history_and_reconcile.py` now expects `NOT_RESOLVED` for the
  BE fixtures without a matched NCI row.
- **Rollback:** redeploy 3.3.1.

## Transcript Next Source: Suggest the Fix for What Failed (3.3.1)

- **Why:** R5-earnings part 2, BR-ERN-009 (owner decision 2026-10-30, "Suggest the fix for what failed";
  `analysis/yahoo-finance-mcp/R5EARNINGS2_SPEC.md`). Release type: patch. Both runtimes.
- **Rule:** when `get_earnings_call_transcript` finds no transcript, `nextRecommendedFallback` names first a source
  the tool tried that failed for a reason the caller can fix:
  - Yahoo returned another event (`YAHOO_METADATA_MISMATCH`): `yahoo_quartr`, retry with a validated `source_url`
    or `event_id`.
  - Alpha Vantage was not queried only because the fiscal quarter was unknown (`NOT_ELIGIBLE`,
    `FISCAL_QUARTER_UNRESOLVED`) and an Alpha Vantage key is configured: `alpha_vantage`, retry with
    `fiscal_quarter` as `YYYYQn`.
  - Otherwise the order is unchanged: company IR, then a public transcript URL, then Yahoo, then Alpha Vantage,
    each when not yet tried. An incomplete Yahoo preview still gives `alternate_transcript_source`.
- **Before:** the suggestion was company IR in every run, because the tool records company IR as skipped and treated
  skipped as untried.
- **Unchanged:** no field, status, source type or enum value was added. `schemaHash` and `manifestHash` stay the
  same. The deployment without an Alpha Vantage key still suggests company IR when nothing else can be fixed.
- **Tests:** the two waterfall cases in `scripts/parity/cases_sec_earnings.py` now assert the same value in both
  runtimes, and a third covers the configured-key case end to end. `scripts/parity/cases_r5ern2.py` covers the
  suggestion rule on attempt lists.
- **Rollback:** redeploy 3.3.0.

## Earnings Surprise: Release EPS, Dated Yahoo Rows, Zero Estimates, Rounding (3.3.0)

- **Why:** R5-earnings part 1: source policy R3 (official release figure first), Q3i call 5, BR-ERN-031,
  BR-ERN-018's rounding and the Q3i `source_url` message (`analysis/yahoo-finance-mcp/R5EARNINGS1_SPEC.md`).
  Release type: minor, because `actual.eps` now carries the release metric's own fields. Both runtimes. BR-ERN-039
  was already settled by 2.26.5 (F55: any `sec_8k*` source, the EX-99.1 exhibit included, is HIGH). The transcript
  fallback suggestion (BR-ERN-009) waits for the owner's decision.
- **Actual EPS (R3, BR-ERN-018/031):** `compare_earnings_actual_vs_estimate` takes actual EPS from the official
  release (`extract_earnings_metrics` `epsDiluted`: SEC XBRL first, then the 8-K text), as revenue already did. The
  row keeps that metric's confidence, evidence and period. Yahoo's `epsActual` is used only when the release states
  no EPS, labelled `source: "yahoo"` and LOW (2.26.4, F22). Before, Yahoo's figure came first and the release was
  the fallback, and a release fallback was still labelled Yahoo.
- **Same quarter only:** a release EPS is compared only with the estimate of the same quarter. When
  `periodAlignmentStatus` is not MATCHED, `epsSurprisePct` and `epsDelta` are withheld with the revenue surprise, and
  the alignment warning says "cross-source revenue and EPS surprise are omitted". A release whose period is unknown
  gets `PERIOD_ALIGNMENT_UNVERIFIED` with its own message. With a Yahoo fallback, Yahoo's actual and estimate share
  one row, so the EPS surprise is still given.
- **Dated Yahoo rows (call 5):** a Yahoo row named by a date gives `estimatePeriod` as that date alone,
  `YYYY-MM-DD`, in both runtimes (Python gave "2026-06-30T00:00:00"). While such a row is UNVERIFIED,
  `estimate.revenue.value` is withheld in both runtimes, as the revenue surprise already was. Python now also matches
  a quarter whose period end is within a week of the Yahoo row's date (the Worker's 52/53-week rule), reports
  `quarterEnd` from the release's period end, and labels `actual.revenue.period` with the period end when the release
  has no fiscal label. Before, the live AAPL and MSFT comparisons were MATCHED in the Worker and would have been
  UNVERIFIED in Python.
- **Zero estimate (BR-ERN-031):** an EPS estimate of exactly 0 follows the near-zero rule: `epsDelta` is given, the
  percentage is omitted with `EPS_NEAR_ZERO_ESTIMATE_BASE`, and confidence is MEDIUM. Before, it was
  `EPS_ESTIMATE_UNAVAILABLE` with no delta.
- **Rounding (BR-ERN-018):** `revenueSurprisePct`, `epsSurprisePct` and `epsDelta` round half away from zero in both
  runtimes. Python's `round()` was half-even, so a 0.125% surprise was 0.12 in Python and 0.13 in the Worker; both
  now give 0.13.
- **`source_url` message (Q3i):** text with no URL scheme, such as "not a url", is refused with "source_url must be a
  valid URL" in both runtimes. Python said "source_url must use https".
- **Correction to 3.2.1:** the AGREED wording change altered the expanded-mode tool description. The grouped
  `schemaHash` stayed the same, because the grouped `evidence` description already used that wording.
- **Unchanged:** no field, status, warning code or enum value was added. `schemaHash` and `manifestHash` stay the
  same.
- **Tests:** in `scripts/parity/cases_sec_earnings.py` the zero-estimate, dated-row, release-EPS and `source_url`
  cases now assert the same value in both runtimes. `scripts/parity/cases_r5ern1.py` covers the withheld EPS
  surprise (MISMATCH and UNVERIFIED), half-away rounding, the actual EPS row from the release and from Yahoo, and the
  within-a-week match against XBRL period ends.
- **Rollback:** redeploy 3.2.1.

## Valuation Repairs: Tolerance Input, AGREED Wording, Convertible Basis, 52/53-Week Quarters (3.2.1)

- **Why:** R5-valuation part 1, the valuation repairs that need no new name (decision Q3e, the binding Q3i triage
  for the EV convertible basis, BR-VAL-068 and the numeric-string tolerance, and the F57 rule;
  `analysis/yahoo-finance-mcp/R5VALUATION1_SPEC.md`). Release type: patch. Both runtimes. The partly tagged debt
  group (BR-VAL-006) and untagged NCI (BR-VAL-025) need new names and wait for the owner's decision.
- **Tolerance input (Q3e, BR-VAL-019/063):** `reconcile_metric_sources` clamps `tolerance_pct` to 0 to 10 only when
  it is a finite number. NaN, an infinity, a numeric string ("5") or a boolean is the 0.5 default. Before, Python
  clamped first, so NaN became 0 and +inf became 10, and it read "5" as 5.
- **AGREED wording (Q3e, BR-VAL-020):** the Worker's `reconcile_metric_sources` description now says what both code
  paths do: AGREED needs the latest SEC value plus at least one independent matching provider. It said "two or more
  providers agree". This changes `schemaHash`.
- **Convertible basis (Q3i, BR-VAL-046/074/082):** `get_valuation_snapshot` takes in-the-money convertible principal
  out of debt only when the share count in the equity value is the dilution bridge's diluted count. With Yahoo's
  count (SHARE_BASIS_MISMATCH) or the basic count (no diluted count in the bridge), the principal stays in debt.
  Before, it left debt in those cases too, so the convertible was in neither debt nor shares and EV was understated.
- **52/53-week quarters (BR-VAL-068):** when an issuer release names a calendar quarter ("fourth quarter of 2026"),
  the quarter is read from the date a week before the period end (`fiscal_calendar`'s drift rule). A quarter ending
  January 1 to 7 is Q4 of the year before. Before, the quarter ended 2027-01-02 matched "first quarter of 2027" and
  not "fourth quarter of 2026". Period selection was already fiscal (3.0.0, F60).
- **Unknown ticker:** `get_valuation_snapshot` fails as `compare_peer_valuations` does since 2.26.5 (F57). An
  unknown ticker is `TICKER_NOT_FOUND` with "No provider data was found for the requested ticker.", and any other
  quote-summary failure is `PROVIDER_ERROR` without the provider URL. Before, it was `PROVIDER_ERROR` with the URL.
- **Unchanged:** no field, status, warning code or enum value was added. `manifestHash` stays the same.
- **Tests:** in `scripts/parity/cases_valuation.py` the NaN/infinity tolerance, numeric-string tolerance, AGREED
  wording and both convertible-basis cases now assert the same value in both runtimes. `scripts/parity/cases_r5val1.py`
  covers the 52/53-week quarter sentences and the two snapshot failures.
- **Rollback:** redeploy 3.2.0.

## Position Signals: Component Status, Quote Date, MACD, One Ticker (3.2.0)

- **Why:** R5-pricing part 3, the `analyze_position_signals` rows of the binding Q3i triage (calls 1 to 4 and G6)
  and decision Q3d (no synthesised `dataDate`); `analysis/yahoo-finance-mcp/R5PRICING3_SPEC.md`. Release type:
  minor, because the Worker's `componentStatus` keys change. The Worker takes Python's shape, so Python output is
  unchanged apart from `dataDate`, the refused ticker list and the warning rows.
- **Component status (call 1):** the Worker reports Python's six components, `analystUpgrades`,
  `analystConsensus`, `priceStats`, `earningsMomentum`, `technicalIndicators` and `maPosition`, and adds
  `failedComponents`. **Renamed Worker keys:** `pricePerformance` is now `priceStats` and `movingAveragePosition` is
  now `maPosition`; `technicalIndicators` keeps its name. A component that fails is `FAILED`, is listed in
  `failedComponents`, and makes the result `PARTIAL` with `recommendedNextAction: RETRY`. Before, the Worker did not
  check the analyst and earnings components, and one component that threw failed the whole tool. It now waits for
  every component, as Python does.
- **Quote time and date (call 2, G6):** the Worker adds `quoteTimestamp`, the `priceTimestamp` of the quote behind
  `currentPrice`. In both runtimes `quoteDataDate` is that timestamp's date, or null. Before, the Worker used the
  moving-average date, which is a bar date.
- **MACD (call 3):** the Worker adds `technicalInputs.macd`, which it already computed.
- **One ticker (call 4):** a ticker list is refused with `INPUT_VALIDATION_ERROR` ("analyze_position_signals takes
  one ticker as a string, not a list."). The grouped route already refused it. Before, the Worker's single tool
  batched a list and Python passed it to its components.
- **Data date (Q3d):** `dataDate` is the technicals' completed-bar date, else the moving averages' date, else null.
  Before, Python fell back to the last weekday and the Worker to today.
- **Warning rows:** the components' 3.1.0 rows (`INSUFFICIENT_HISTORY`, `ZERO_VOLUME_NO_RATIO`,
  `SESSION_CALENDAR_UNKNOWN`) reach this tool's `meta.warnings`, each row once. This closes the gap left in 3.1.0.
- **Unchanged:** the output schema already listed `failedComponents` and allows extra keys, and no description
  changed, so `schemaHash` and `manifestHash` stay the same. The `analystCount` source (Python reads
  `totalAnalysts`, the Worker `numberOfAnalysts`) gives the same count and stays pinned.
- **Tests:** in `scripts/parity/cases_neutrality.py` the macd, component-status and quote-date cases now assert the
  same value in both runtimes, the batch case is gone, and the Python stub for price stats carries the history rows
  the Worker raises on the same market. `scripts/parity/cases_r5price3.py` covers the refused list in the grouped
  route and the single tool, a chart outage (two components failed, the rest still delivered), a total outage (six
  failed, `dataDate` null) and the repeated-row rule.
- **Rollback:** redeploy 3.1.0.

## Pricing Notices: Zero Volume, Short-History CAGR, US Session Calendar (3.1.0)

- **Why:** R5-pricing-2, the pricing repairs that need new names (owner decisions Q3c, Q3i and Q1 BR-PRC-002;
  owner cards 2026-10-29: the notices go in `meta.warnings`, and in a snapshot's `data.warnings`; the holiday
  calendar covers US exchanges only; `analysis/yahoo-finance-mcp/R5PRICING2_SPEC.md`). Release type: minor. Both
  runtimes.
- **New warning codes:** `ZERO_VOLUME_NO_RATIO`, `INSUFFICIENT_HISTORY` and `SESSION_CALENDAR_UNKNOWN`. Each row is
  `{code, message, ticker}` in `meta.warnings`, after the server's own warnings and in the order the tool raised
  them. A batch call lists every ticker's rows. Tools no longer return these inside `data`.
- **Zero volume (Q3c, BR-PRC-033):** when the latest completed session traded zero shares, `analyze_volume_ratio`
  gives `ratio10d`, `ratio90d` and `volumeFlag` null with `ZERO_VOLUME_NO_RATIO`. Before, Python gave null without a
  reason and the Worker gave ratio 0 with flag `LOW`. The averages are still reported.
- **Short history (Q3i, BR-PRC-019/021):** `analyze_price_performance` gives `cagr1y`, `cagr3y` or `cagr5y` null with
  one `INSUFFICIENT_HISTORY` row per horizon when the first completed bar is more than 10 calendar days after that
  horizon's start date. Before, a 3-year history reported its whole gain as a 5-year CAGR (33.1% as 5.9%). The 10 days
  absorb a listing whose first bar falls just after the start date over a holiday run.
- **Session calendar (BR-PRC-002):** during a session, when Yahoo gives no `previousClose` to match, the expected
  completed session is the previous session on the exchange's calendar: weekdays minus the NYSE full closures for
  2024 to 2028 (`worker/src/exchange-calendars.json`, read by both runtimes; early closes are sessions). It covers
  NMS, NGM, NCM, NYQ, ASE and PCX. A missing bar is now `STALE` against that session (it was `UNKNOWN`), and after a
  Monday holiday the Friday bar is `CURRENT`. An exchange without a calendar, or a date whose previous session falls
  outside the covered years, gives `freshnessStatus: UNKNOWN`, `expectedCompletedDate: null` and
  `SESSION_CALENDAR_UNKNOWN`. Before, every exchange used the previous weekday. Outside a session nothing changes:
  `regularMarketTime` still names the session.
- **Tools that can raise `SESSION_CALENDAR_UNKNOWN`:** `analyze_price_performance`, `get_price_slope`,
  `analyze_volume_ratio`, `check_volume_liquidity_threshold`, `get_technical_indicators` and
  `analyze_options_flow_window`. `get_historical_prices` returns a bare array, so it keeps only its `isFinal` flags.
- **Snapshot:** `get_market_snapshot` moves a component's rows into its own `data.warnings` as
  `{code, component, message}`, next to the existing `STALE_BAR` and `PARTIAL` rows.
- **Legacy envelope:** with `MCP_ENVELOPE_V2` turned off the payload is returned as the tool built it, so the rows stay
  under `_warnings` in both runtimes.
- **Known gap:** `get_position_score_inputs` does not pass its components' rows on yet (R5-pricing part 3).
- **Unchanged:** no output field or enum value was added, and no tool description changed, so `schemaHash` and
  `manifestHash` stay the same.
- **Tests:** `scripts/parity/cases_pricing_evidence_tools.py` now asserts the same value in both runtimes for the
  zero-volume, short-CAGR and Monday-holiday cases, with new cases for no calendar, a date past the covered years, a
  6-month history and the 10/11-day coverage edge. `scripts/parity/cases_r5price2.py` checks both calendar helpers
  for every day from December 2023 to January 2029 against the NYSE holiday rules, plus malformed and uncovered
  inputs, and checks that the rows reach `meta.warnings` through the envelope and the grouped route and reach a
  snapshot's `data.warnings`. `scripts/test_exchange_calendars.py` (in CI) checks the file and fails 90 days before
  the last covered year ends. `scripts/test_completed_session_freshness.py` gives each scenario an exchange code; the
  Tuesday after Labor Day without `previousClose` is now `CURRENT`, with a new no-calendar scenario that stays
  `UNKNOWN`.
- **Rollback:** redeploy 3.0.8.

## Pricing Repairs: Zero Price, Rounding, CAGR Start, Hedge Expiries, US Venues (3.0.8)

- **Why:** R5-pricing-1, the pricing repairs that need no new name (owner decisions Q3b, Q3d and the Q3i triage,
  2026-10-15; `analysis/yahoo-finance-mcp/R5PRICING1_SPEC.md`). Release type: patch. Both runtimes.
- **Zero price (G1):** `calculate_price_target_distance` returns `distanceToTargetPct: null` when the current price
  is 0, as the Worker already did. Before, Python failed on the division.
- **Rounding (Q3d, BR-PRC-040):** the displayed percentages of `calculate_price_target_distance`, the price-change
  percentage of `get_market_snapshot` and the averages and ratios of `analyze_volume_ratio` round half away from zero
  on the decimal value (75.25 to 75.3, 100.05 to 100.1). Before, Python rounded half to even on the binary value and
  the Worker used `toFixed`, so ties could differ by one in the last place.
- **Volume ratio:** the ratio divides by the unrounded average; only the published averages are rounded. The Worker
  rounded the average first.
- **Quote date (Q3d):** `analyze_moving_average_position` and `calculate_price_target_distance` give
  `dataDate: null` when the quote has no trade date. The Worker used today's date.
- **CAGR start (G5, BR-PRC-021):** `get_price_stats` starts each 1, 3 and 5 year horizon at the first bar on or after
  the same exchange-local date N years before the last completed bar (29 February becomes 28 February). The Worker
  used N x 365.25 days and Python a calendar offset on timestamps, so the start bar could differ by a session.
- **Hedge expiries (Q3i call 23):** `find_put_hedge_candidates` no longer falls back to expiries under a week out
  when every listed expiry is that close. It returns no candidates instead of the contracts it says it skips.
- **US venues (Q3i call 24):** `search_ticker` with `exchange: "US"` keeps NMS, NGM, NCM, NYQ, ASE and PCX in both
  runtimes. Before, the Worker kept NMS and NYQ and Python NMS, NYQ and PCX. The tool and parameter descriptions say
  so, which changes `schemaHash`.
- **Input checks (Q3b):** `get_price_slope` refuses a `days` that is not a number or digits ("abc", "2.5", a list)
  with `INPUT_VALIDATION_ERROR`. Before, the Worker read it as 5 and Python raised. The Worker's filing outline
  checks the ticker and accession itself, as Python's does, so a direct caller never reads a filing for a malformed
  input.
- **Unchanged:** no field, status, warning code or enum was added. The zero-volume rule, short-history CAGR and the
  session calendar need new names and wait for 3.1.0.
- **Tests:** the pinned zero-price, tie, quote-date, volume-ratio, CAGR-horizon, slope and outline cases now assert
  the same value in both runtimes (`scripts/parity/cases_neutrality.py`,
  `scripts/parity/cases_pricing_evidence_tools.py`), with a new decimal-tie case. `scripts/parity/cases_r5price1.py`
  checks `addIsoYears` and `_add_years` against a calendar oracle and the three `days` refusals through each
  runtime's tool call.
  The hedge fixtures in `scripts/test_options_quality.py` used a fixed expiry that is now in the past and passed
  only through the old fallback; they now use an expiry a month out.

## Extractor Repairs: Loss Sign, China Preset As Data (3.0.7)

- **Why:** R5-extractors-3, the last extractor repairs before the Phase 5 extractors move (owner decisions Q1
  BR-EXT-015 / Q3i, 2026-10-15, and Q5; `analysis/yahoo-finance-mcp/R5EXTRACTORS3_SPEC.md`). Release type: patch.
  Both runtimes.
- **Loss sign (BR-EXT-015):** a figure that `extract_earnings_metrics` reads from release text (revenue, diluted
  EPS, gross margin, operating income, free cash flow, capex; `compare_earnings_actual_vs_estimate` reads them too) is
  negative when the words right before
  it state a loss ("operating income was a loss of $5 million", "diluted EPS was a loss of $0.12") or when a minus
  sits directly before the dollar sign ("-$5 billion", also U+2212). Before, both read as positive. "Negative $5
  billion", parentheses and "$-5" were already negative. Wording that names both outcomes ("income (loss) of",
  "income/loss of") and a spaced dash used as a separator ("revenue - $94.9 billion") still sign nothing.
- **China preset as data (Q5):** the term lists `extract_china_exposure` and `extract_exposure` (topic `china`)
  search, including the issuer aliases Tongmei, JinMei and BoYu, moved from code into
  `worker/src/exposure-presets.json`. The Worker compiles it in; Python reads it from `worker/src`
  (`yfmcp/exposure_presets.py`), as it already does for the news registries. Terms and order are unchanged, so output
  is unchanged.
- **Unchanged:** no field, status, warning code or enum was added. The public `extract_china_exposure` action stays.
- **Tests:** the pinned loss-wording case and the "-$5 billion" finding now assert negative values in both
  runtimes, with new cases for operating income, EPS, the per-share form, U+2212 and the three wordings that stay
  unsigned (`scripts/parity/cases_extraction_rules.py`). `scripts/parity/cases_r5ext3.py` pins the preset lists each
  runtime loads to the lists the code held before the move.

## Extractor Repairs: In-The-Money At The Strike, Leap Days, Foreign Dollars (3.0.6)

- **Why:** R5-extractors-2, the extractor repairs that must ship before the Phase 5 extractors move (owner decisions
  Q3f and Q3i, 2026-10-15, and the leap-day and currency choices, 2026-10-29;
  `analysis/yahoo-finance-mcp/R5EXTRACTORS2_SPEC.md`). Release type: patch. Both runtimes.
- **In the money at the strike (Q3f; BR-EXT-003, BR-EXT-082, BR-EXT-083):** options, option tranches, warrants and
  capped calls are `inTheMoney: true` when the price equals the exercise price, as convertibles and convertible
  preferred already were. Treasury stock still adds 0 shares at the strike, so no share count or dilution figure
  changes; only the flag does.
- **Leap days (BR-EXT-044, BR-EXT-045, BR-EXT-110):** a period end of 29 February plus N years is 28 February in a
  common year (it stays 29 February in a leap year). This affects `periodThrough` in the funding schedule's
  contractual rows and in the capital-structure maturity ladder, and debt term ends. Before, the result was an invalid
  date such as `2025-02-29`.
- **Foreign dollar amounts (BR-EXT-059, BR-EXT-064):** in `extract_funding_capex_schedule` text items, an amount
  written `NT$`, `A$`/`AU$`, `C$`/`CA$`, `HK$`, `S$` or `NZ$` keeps its currency (`TWD`, `AUD`, `CAD`, `HKD`, `SGD`,
  `NZD`) and `amountAsWritten` includes the prefix; it was read as USD before. A `$` right after any other letters
  (for example `R$`) is not read as an amount. Two amounts in different currencies are never one range. The ATM
  capacity read in the dilution bridge counts US dollars only (`amountUsd`), so it no longer reads `NT$` or `A$`
  figures as dollars.
- **Unchanged:** no field, status, warning code or enum was added. `currency` already held ISO codes.
- **Tests:** the pinned Q3f, leap-day and foreign-dollar parity cases now assert the repaired values in both runtimes.
  `scripts/parity/cases_r5ext2_properties.py` adds property cases: every 1-to-5, 7, 10 and 30-year anniversary of about 14,000 dates
  from 1896 to 2404 matches the calendar (Python `datetime` is the oracle), and across 20 strikes and about 240 prices
  every option, warrant and convertible is in the money exactly when price >= threshold, with treasury-stock shares 0
  at or below the strike, never above the count, and never falling as the price rises.

## Shared Utilities Module Move (3.0.5)

- **Why:** internal restructuring only (Phase 4 unit 5, planner decision D1 (a) and (c)). Release type: patch. No
  behaviour change.
- **What moved:**
  - Worker: the generic helpers (`_str`, `clampInt`, `parseObjectJson`, `compactExcerpt`, `toIsoUtc`,
    `getLastTradingDate`, `dailyBarDate`) to `worker/src/util.ts`; the batch runner (`runPartialBatch` and its
    result shaping and concurrency helpers) to `worker/src/batch.ts`; the news company identity
    (`resolveNewsCompanyIdentity`, its website normalisation and cache, `finnhubEligibility`) to
    `worker/src/news/identity.ts`; and the two filing-index HTML helpers (`_stripHtmlTagsIdx`,
    `_sanitizeFilingHtml`) to `worker/src/clients/html.ts`.
  - Python: `_clamp_int`, `_now_iso`, `_plain`, `_df_period_rows`, `_add_years`, `_fy_number`, `_js_json_numbers`,
    `_nn`, `_unwrap_payload` and `_utf16_length` to `yfmcp/util.py`.
  - `news.ts` and the news providers no longer import `yahoo-finance.ts`.
  - `server.py` drops its unused import of `_SEC_REQUIRED_UA`, so no SEC User-Agent reference remains outside the two
    EDGAR clients (Phase 4 exit check 2).
- **Topology rule (D1 (c)):** `extract_topology.py` gains a `DOMAIN_OVERRIDES` table. A tool handler or tool-only
  helper that the call-graph rule placed in `shared` only because two or more domains call it is assigned to its
  catalog domain (SEC filing selection and the filing-data tools to `sec`, the shared extractors to `extractors`,
  the earnings-release reads to `earnings`, the analyst and consensus handlers to `fundamentals`, and so on). They
  move in Phase 5. With the moves above, neither god file has a `shared` region left.
- **Unchanged:** no tool name, input, output, schema, status, warning or ordering changed.
- **Compatibility:** `yahoo-finance.ts` and `server.py` re-export the moved names other modules and tests read.
- **Verification:** the moved Worker text keeps its line multiset (only `export` keywords and import lines differ);
  the moved Python functions keep their `ast.dump`; tool listings (expanded and grouped) are byte-identical; every
  new module evaluates as the sole entry; the parity harness passes with no expected value changed.

## Endpoint Builder (3.0.4)

- **Why:** internal restructuring only (Phase 4 unit 4, decision D8). Release type: patch. No behaviour change.
- **What moved:** every Yahoo Finance API and SEC EDGAR URL template is now built in one place:
  `worker/src/clients/endpoints.ts` (quoteSummary, chart, options, timeseries, search, quoteType, screener and
  visualization; SEC submissions, companyfacts, companyconcept, archive documents, filing index pages, the EDGAR Atom
  browse feed and the manual-lookup links) and its twin `yfmcp/clients/endpoints.py`. Call sites keep their own
  encoding and pass the encoded symbol, CIK or accession in.
- **Unchanged:** every builder returns exactly the string its call site wrote before, so no request URL, cache key,
  tool name, input, output, schema, status or warning changed. The Python manual-lookup links keep their own encoding
  (the Worker encodes `company=` and `/`; Python does not), as before.
- **Verification:** `scripts/test_endpoints.py` pins every builder in both runtimes to the old hand-written literal;
  the parity harness passes with no expected value changed; tool listings (expanded and grouped) are byte-identical.

## HTML Parsing Module Move (3.0.3)

- **Why:** internal restructuring only (Phase 4 unit 3). Release type: patch. No behaviour change.
- **What moved (Worker):** tag stripping and entity decoding, readable text and topic paragraphs, HTML table parsing
  (`parseHtmlTable`, from `news/shared.ts`, decision D5) and financial-cell merging, numeric cells and the stated
  unit-scale helpers now live in `worker/src/clients/html.ts`. Python already keeps these in `yfmcp/parsing/html.py`.
- **Unchanged:** no tool name, input, output, schema, status, warning or ordering changed.
- **Compatibility:** `yahoo-finance.ts` and `news/shared.ts` re-export the moved names other modules and tests read.
- **Verification:** the parity harness passes with no expected value changed; tool listings (expanded and grouped) are
  byte-identical; the moved text keeps its line multiset.

## One MarketInputs Source For Valuation (3.0.2)

- **Why:** Phase 4 Q3g (brief §7 Q3g, V6): one `MarketInputs` contract, read by the Yahoo client, in both runtimes.
  Release type: patch. Spec: `analysis/yahoo-finance-mcp/Q3G_SPEC.md`.
- **Change (Python only; the Worker already read quoteSummary):** `get_valuation_snapshot`, `compare_peer_valuations`
  and the ADR ratio in `get_historical_valuation_context` read Yahoo quoteSummary
  (`price,financialData,defaultKeyStatistics,earningsTrend`) at the Worker's URL through yfinance's crumb-aware
  session, and map it with the Worker's mapping. yfinance `.info` and the estimate DataFrames are no longer used here.
- **Values that can change in Python:** the price is `regularMarketPrice` (was `currentPrice` first); estimate rows are
  every `earningsTrend` period; an unknown symbol fails with the Worker's text,
  `Yahoo quote summary failed for X: Yahoo Finance API error 404 for: <URL>` (was `No Yahoo quote summary for X`).
  The message never carries the crumb. Peer errors keep `TICKER_NOT_FOUND` without a URL (F57).
- **Unchanged:** no field, status, warning code or enum was added or removed.
- **Removed (unused after the change):** `yfmcp.valuation.market_inputs_from_yfinance`, `empty_market_inputs` and the
  `_estimate_rows` helper.

## Yahoo Client Module Move (3.0.1)

- **Why:** internal restructuring only (Phase 4 unit 2). Release type: patch. No behaviour change.
- **What moved:**
  - Worker: the Yahoo client (session crumb, the throttling retry on the other API host, the GET body cache and its
    edge cache, `yGet` / `yPost`, `raw`, `fetchTimeseriesByDate`, `valuationMarketInputs`) to
    `worker/src/clients/yahoo.ts`, and the provider transport (`UA`, the provider JSON cache, provider status mapping,
    `fetchProviderWithTimeout`) to `worker/src/clients/provider-http.ts`.
  - Python: `_valuation_market_inputs` and its helper `_estimate_rows` to `yfmcp/clients/yahoo.py`.
- **Unchanged:** no tool name, input, output, schema, status, warning, ordering, cache key, edge-cache URL or
  User-Agent changed. `YAHOO_EDGE_CACHE_VERSION` stays `v1`, so the edge cache is not invalidated by the deploy.
- **Compatibility:** `yahoo-finance.ts` and `server.py` re-export the moved names that other modules and tests read.
- **Verification:** the parity harness passes with no expected value changed; tool listings (expanded and grouped)
  are byte-identical; the moved Worker text keeps its line multiset and the moved Python functions keep their
  `ast.dump`.

## Breaking Changes (3.0.0)

- **Why:** the Phase 6 major release. Every change below was announced by a `DEPRECATED_FIELD` notice or a docs notice for at least one minor version ("Neutral Field Names And Deprecations (2.8.0)", "Phase 6 Deprecation Notices (2.27.0)"). The owner chose on 2026-10-28 to ship it before the Phase 4 and Phase 5 module moves, as an exception to the brief's "Phase 5 exit" entry criterion; those moves and the non-breaking Phase 6 cleanup continue after it. Plan: `analysis/yahoo-finance-mcp/MAJOR3_SPEC.md`.
- **Removed fields** (both runtimes unless named):

  | Removed | Use instead |
  | --- | --- |
  | `doctrineUse` (Worker meta; data of `get_company_press_releases`, `get_sec_filing_section_markdown`) | `reliabilityClass` |
  | `g2`, `opportunity`, `action`, `priceTarget`, `scenarioWeights`, `selectedMethod`, `selectedMultiple` on evidence-only actions | `decisionUse: EVIDENCE_ONLY` |
  | `calculate_price_target_distance` `bracket`, `inferredTag`, `tag`, `tagNote` | `priceAtOrAboveTarget`, `referenceTargetPct` |
  | `check_volume_liquidity_threshold` `gatePass`, `gateThresholdUsd` (the fixed $10M gate) | `meetsMinimum` with `min_adv_usd` |
  | `get_market_snapshot` `volume.liquidityGatePass` | `volume.meetsMinimum` with `min_adv_usd` |
  | `analyze_position_signals` `t1_inputs`, `t2_inputs`, `t4_inputs`, `t5_inputs` | `analystInputs`, `pricePositionInputs`, `earningsInputs`, `technicalInputs` |
  | Worker `meta.decisionGrade` on `extract_sec_filing_fact`, `get_company_press_releases`, `get_sec_filing_section_markdown` (F5, F45) | the payload's `decisionGrade` |
  | `analyze_options_flow_window` `ivPctile`, `putVolVs10dAvg`, `fieldNotes` (F61) | `ivVsRealizedRangePct`, `putVolPer1PctStockAdv` |
  | `get_analyst_rating_changes` `upgrades30d`, `downgrades30d`, `initiations30d` (F61) | `upgrades`, `downgrades`, `initiations` with `windowDays` |

  The Worker meta of the three status-table tools now reads `capabilityStatus`, `reliabilityClass`, `failureMode`, `evidenceRequired`, `sourceType`, then the table's other keys. `analyze_position_signals` `analystInputs` keeps its `upgrades30d`, `downgrades30d`, `initiations30d` (a 30-day window, unchanged values). The options-chain summary's `ivPctile` is a different field and stays.
- **`check_volume_liquidity_threshold` note:** with the fixed gate gone, the note reads `20d average traded value $X`, then ` (≥ min_adv_usd $Y)` or ` (< min_adv_usd $Y)` when `min_adv_usd` is given, then the latest-session ratio and the FX note as before. The UNKNOWN notes are unchanged.
- **F17, Python `get_ownership_holders`:** returns the Worker's payload, the holder type's Yahoo quoteSummary module with Yahoo's `{raw, fmt}` values unwrapped (camelCase keys, dates as `YYYY-MM-DD`), instead of yfinance's renamed columns.
- **F47, Python `get_company_events_calendar` (upcoming):** `calendar` is the Worker's `{earnings: {earningsDate, earningsAverage, earningsLow, earningsHigh, revenueAverage, revenueLow, revenueHigh}, exDividendDate, dividendDate}` from Yahoo's calendarEvents, instead of yfinance's `Earnings Date`-style keys. A Yahoo result without calendar events returns that shape empty, as the Worker does.
- **F20, `confidence` holds only `HIGH`, `MEDIUM`, `LOW` or null:** wherever a payload carried a status string in `confidence` (`EXTRACTION_FAILED`, `NOT_DISCLOSED`, `NOT_DECISION_GRADE`, `FILING_NOT_FOUND_TRY_OTHER_TYPE`, `FILING_TEXT_NOT_AVAILABLE`, `TICKER_NOT_FOUND`, `NOT_FOUND`), `confidence` is null and the string moves to the object's `status` when it has none (an existing `status` is kept). The rule runs over the whole response in both envelopes. The V2 envelope's fact enrichment no longer writes `NOT_DECISION_GRADE`: an untagged fact without a value has null confidence, and one with a value is still LOW. A fact's null confidence is not replaced by its parent's. Evidence-pack components are normalised before they are hashed; a stored evidence cut read back by `get_evidence_cut` is returned verbatim, so a cut stored before 3.0.0 can still carry the old strings and its SHA-256 still verifies.
- **F59, `get_latest_earnings_release` sources:** both runtimes return the Worker's entry for the 8-K, `{sourceType: "sec_8k", url, contentUrl, filingDate, acceptedAt, accessionNumber, cikPadded, items, isInlineXBRL, confidence}`. `contentUrl` (new) is the document whose text is read: the EX-99.1 exhibit when the 8-K has one, else the 8-K. Python drops `sec_8k_ex991` in this entry, `primaryDocumentUrl`, the top-level `errorCode` and `meta.generatedAt`. Tools that read the release (`index_earnings_release` and the extractors) still report the content they read as `sec_8k_ex991`. Python's `period` falls back to `"latest"` when the release states no period, as the Worker's does (it was null). Left as they were, and pinned in the harness: Python writes timestamps such as `reportedAt` without milliseconds (`...21:05:00Z` against the Worker's `...21:05:00.000Z`), and Python's `extract_earnings_metrics` top-level `source` names `sec_8k_ex991` where the Worker's names `sec_8k`.
- **F60, one period vocabulary:** `reconcile_metric_sources` takes `latest` (the latest quarter, now the default), `FY<yyyy>` or `FY<yyyy> Q<n>`. `FY<yyyy> Q<n>` is the issuer's fiscal quarter (SEC fy/fp of the filing that first reported it, else counted from the prior fiscal year end), the quarter `fiscalQuarter`/`fiscalYears` already reported. Before, `Q<n> <yyyy>` was the calendar quarter of the period end, which is a different quarter for off-calendar issuers such as AAPL and MSFT. `latest_quarter`, `latest_annual` and `Q<n> <yyyy>` return `INPUT_VALIDATION_ERROR` ("period must be latest, FY<yyyy> or FY<yyyy> Q<n>."); ask for the latest annual period as `FY<yyyy>`. The extractors' `latest`, `FY<yyyy>` and bare `<yyyy>` are unchanged.
- **Notices:** no `DEPRECATED_FIELD` notice remains; the 2.8.0 and 2.27.0 notices are gone with their fields.
- **Evidence hashes:** payloads of evidence-only actions lost the seven null boundary fields, so a new evidence cut's hash differs from a 2.x cut of the same data.
- **Neutrality check:** `scripts/test_neutrality_contract.py` (in CI) fails when either runtime's tool listing, expanded or grouped, carries doctrine wording or advertises a removed field.
- **Tests:** parity cases in `scripts/parity/cases_major_300.py`; the 2.8.0 and 2.27.0 notice cases flipped to the 3.0.0 contract.
- **Rollback:** redeploy 2.27.0.

## Phase 6 Deprecation Notices (2.27.0)

- **Why:** finding F61 of the 2026-10-27 audit (`analysis/yahoo-finance-mcp/F_TRIAGE.md` §7). The Phase 6 major release removes or reshapes the fields below, and each needs a notice for at least one minor version first. Release type: minor, notice only. **No value, field or status changes**; the notices are `DEPRECATED_FIELD` rows in `meta.warnings` (never in `data`), with the 2.8.0 key order and texts identical in both runtimes.
- **Mapping:**

  | Deprecated | Replacement | Runtimes | Notice |
  | --- | --- | --- | --- |
  | `meta.decisionGrade` on `extract_sec_filing_fact`, `get_company_press_releases`, `get_sec_filing_section_markdown` (F5, F45) | the payload's `decisionGrade`, set per call | Worker (Python's meta never had it) | every success of those tools |
  | Status strings in `confidence` (`EXTRACTION_FAILED`, `FILING_NOT_FOUND_TRY_OTHER_TYPE`, `FILING_TEXT_NOT_AVAILABLE`, `TICKER_NOT_FOUND`, `NOT_DISCLOSED`, `NOT_DECISION_GRADE`, `NOT_FOUND`) (F20) | `confidence` holds only `HIGH`, `MEDIUM` or `LOW`; a status field names the limitation | both | any `confidence` at any depth of `data` holding another string |
  | Python `get_ownership_holders` rows with yfinance column names (`Holder`, `Value`, `Date Reported`, ...) (F17) | the Worker's quoteSummary rows (camelCase keys) | Python | the row keys that are not camelCase are listed in `fields` |
  | Python `get_company_events_calendar` `calendar` with yfinance keys (`Earnings Date`, `Dividend Date`, ...) (F47) | `calendar.earnings.earningsDate`, `calendar.earnings.*`, `calendar.dividendDate`, `calendar.exDividendDate` | Python | the keys present, as `calendar.<key>` |
  | `analyze_options_flow_window` `ivPctile`, `putVolVs10dAvg` (2.4.4 aliases, until now only in `fieldNotes`) | `ivVsRealizedRangePct`, `putVolPer1PctStockAdv` | both | when the aliases are in `data` |
  | `get_analyst_rating_changes` `upgrades30d`, `downgrades30d`, `initiations30d` | `upgrades`, `downgrades`, `initiations` with `windowDays` | both | when the keys are in `data` |

- **`get_analyst_rating_changes`:** the `*30d` keys are the ones that go. They copy the plain counts when `days_back` is 30 and are null for any other window, so the plain keys plus `windowDays` carry every call.
- **Announced for Phase 6, docs only (no field to flag):**
  - **F59, earnings envelope shape:** Python and Worker earnings responses (`get_latest_earnings_release` and the tools built on it) will share one shape: the Worker's keys (`sources[0].sourceType` `sec_8k` for the 8-K, with `items`, `isInlineXBRL`, `cikPadded`), plus `contentUrl` for the EX-99.1 exhibit that was read. Python's top-level `errorCode` and `meta.generatedAt` go.
  - **F60 part 3, one period vocabulary:** period inputs across tools will take one form, `latest`, `FY2026` or `FY2026 Q3`. Forms accepted today only by some tools (`latest_quarter`, `latest_annual`, `Q3 2026`) will be refused with `INPUT_VALIDATION_ERROR`.
- **Already announced:** the 2.8.0 neutrality fields (N1–N3, "Neutral Field Names And Deprecations (2.8.0)") and the F17 docs notice (2.25.0).
- **Window and removal:** at least one minor version, as for 2.8.0. The removal is the Phase 6 major release, which also needs the Phase 5 exit criteria. Consumers should move to the replacements now.
- **Tests:** 15 parity cases in `scripts/parity/cases_notices_2270.py` (the Worker-only and Python-only notices are pinned).
- **Rollback:** redeploy 2.26.5.

## Audit Repairs F54–F57, F60, F62 (2.26.5)

- **Why:** the medium findings of the 2026-10-27 functional audit (`analysis/yahoo-finance-mcp/F_TRIAGE.md` §7). The owner approved the recommendations (`/mnt/project-files/audits/f54-f62-recommendations-2026-10-27.md`) as written on 2026-10-27. No new status, warning code or enum value; one new key inside existing error entries (F57).
- **F54, both runtimes: the tagged principal ladder is what is placed.** In `extract_capital_structure`, when no instrument carries its own maturity date but the filing tags the principal ladder (`LongTermDebtMaturitiesRepaymentsOfPrincipal*`), `ladderCoverage.ladderedAmount` is the ladder's total with `ladderedAmountBasis: "TAGGED"`, and `coveragePct` is null: coverage is measured at carrying amounts, which the tagged ladder does not give. Before, AAPL's 10-K read `ladderedAmount: 0`, `coveragePct: 0` next to a complete 91,281M ladder. `status` stays COMPUTED, as F37 says for debt with a ladder. A filing whose instruments carry maturities is unchanged.
- **F55, both runtimes: singular and plural topic words.** `get_management_commentary` matches a topic's last word in its simple singular or plural form (`-s`, `-es`, `-ies`), so "tariffs" finds "tariff costs" and "supplies" finds "supply". Words under 5 letters and words ending in `-ss`, `-is` or `-us` are kept as written ("news", "business", "analysis", "status"); there is no other stemming. Python now also returns `matchedTerms` on each topic and its evidence, and rates confidence HIGH for any `sec_8k*` source (the EX-99.1 exhibit included), as the Worker does; it picks the best-scoring sentence with the Worker's rule instead of the first.
- **F56, both runtimes: an unknown ticker in `get_market_snapshot` is `TICKER_NOT_FOUND`.** When the quote says the ticker does not exist, the single-ticker call fails with `TICKER_NOT_FOUND` and `retryable: false` ("No provider data was found for the requested ticker."), as `get_market_quote` does, instead of a PARTIAL snapshot that says RETRY. In a batch the ticker's entry is `{error: true, code: "TICKER_NOT_FOUND", message, retryable: false}`. Component failure messages no longer carry provider URLs or JSON parser errors; a component's plain-text error is its message.
- **F57, both runtimes: failed peers in `compare_peer_valuations`.** Each `errors` entry carries `code`: `TICKER_NOT_FOUND` for an unknown symbol, `PROVIDER_ERROR` otherwise, and the message has no provider URL. Python lists an unknown peer under `errors` and drops its all-null row, as the Worker does: a yfinance info dict without `quoteType`, `shortName` or `longName` is "No Yahoo quote summary". The same rule means Python `get_valuation_snapshot` for an unknown symbol now fails with "Yahoo quote summary failed for X: No Yahoo quote summary for X" instead of an all-null snapshot.
- **F60, both runtimes: period inputs and labels.**
  - `reconcile_metric_sources` with a period it cannot read (anything but `latest_quarter`, `latest_annual`, `FY2026` or `Q3 2026`) returns `INPUT_VALIDATION_ERROR` before any fetch, instead of `ok: true` with `status: INVALID_PERIOD`.
  - `extract_geographic_revenue` on a 10-Q labels `period` with the table's column header only when the header names a year. A header such as "Three Months Ended", with the date in a row below, gives a null period.
  - One period vocabulary across tools is left to Phase 6 (breaking); 2.27.0 announces it.
- **F62, both runtimes: `extract_exposure`.**
  - When a region appears only in the filing's text and the parsed tables have no row for it (the F35 case, AAPL India), the revenue warnings no longer include `TABLE_NOT_PARSED`: a table was parsed. The revenue status stays NOT_FOUND.
  - The operational scan also matches "manufactured", "outsourcing partner" and "contract manufacturer" for every region, the phrases F39 added to the China preset. AAPL's 10-K now finds India's operational exposure ("manufactured by outsourcing partners located primarily in China mainland, India, ...").
- **F58 (ops), no change:** the audit's premise that deploy verification and canaries spent the Alpha Vantage quota is not borne out. No deploy or canary script calls an Alpha Vantage-backed tool (`get_consensus_forecast_curve`, `get_eps_revisions`, `get_historical_put_call_ratio`, `get_expanded_institutional_ownership`, the transcript fallback); the transcript canaries fail input validation first. The quota is shared with every caller of the deployed key, so live verification sessions and other use spend it. A separate key for verification is the owner's call.
- **Tests:** 45 parity cases in `scripts/parity/cases_audit_2265.py`; the F35 case in `scripts/parity/cases_audit_2254.py` now expects no warning.
- **Rollback:** redeploy 2.26.4.

## Audit Repairs F51, F52, F53, F22, F23 (2.26.4)

- **Why:** the 2026-10-27 functional audit (`analysis/yahoo-finance-mcp/F_TRIAGE.md` §7). The owner approved these as recommended on 2026-10-27. No new field, status, warning code or enum value.
- **F51, both runtimes: the earnings XBRL tier is measured from the release.** `extract_earnings_metrics` reads the SEC XBRL quarter (tier 1) when that quarter ended less than 120 days before the release's filing date, and not after it (`xbrlQuarterInWindow`, Python `_xbrl_quarter_in_window`). Before, the 120 days were counted to today, so an issuer's latest quarter dropped to the release text about 120 days after it ended, usually just before the next release (AAPL on 2026-10-27). The age from today still applies when the release has no filing date. The fact must still be filed on or after the release.
- **F52, both runtimes: Thai NAV keeps the resolved share class.** `get_thai_fund_nav` without `proj_id` asks the SEC for the resolved class, but the provider can return every class of the project. When the page holds more than one class, only the resolved class's rows are read, as the explicit `proj_id` path already did. A page without that class is `NAV_NOT_FOUND_IN_WINDOW`; a page of one class is read as before. No further page is fetched.
- **F53, Python: `dividendYield` is a decimal ratio.** yfinance overlays Yahoo's v7 quote on `summaryDetail`, and the quote states the yield as a percent (AAPL 0.31 for 0.0031). `get_company_profile` / `get_stock_info` and `analyze_financial_ratios` now return the ratio, as the Worker does and as `unitSemantics.decimalRatios` says. The scale is the one closer to `dividendRate / price` (or `trailingAnnualDividendYield`), so a ratio left by a failed quote call is kept. Python callers that multiplied by 0.01 should stop.
- **F22, both runtimes: the Yahoo actual EPS is LOW.** In `compare_earnings_actual_vs_estimate`, `actual.eps` (`source: yahoo`) carries `confidence: "LOW"`, as the Yahoo estimate rows do since 2.25.0 (F16). It inherited the comparison's confidence before. The comparison's own `confidence` is unchanged.
- **F23, Python: segment warnings in the query answer.** `query_sec_filing_index` `segment_revenue` adds the extractor's warnings (for example `POSSIBLE_20F_FILER`) to its `warnings`, as the Worker does. The pinned case `F21-SEGMENT-QUERY-WARNINGS` is now equal.
- **Tests:** 20 parity cases in `scripts/parity/cases_audit_2264.py`, and the flipped case in `scripts/parity/cases_accession.py`.
- **Rollback:** redeploy 2.26.3.

## Python Filing Index Payload (2.26.3)

- **Why:** finding F50 (`analysis/yahoo-finance-mcp/F_TRIAGE.md` §6), found while fixing F48 and classified by the owner on 2026-10-27 as a repair toward the Worker. Python only. The fields, codes and warnings are the Worker's existing ones; Python callers see the changes below.
- **Python `get_sec_filing_index` goes through `_resolve_sec_filing`,** its port of the Worker's `resolveSecFiling`, as the Worker's `getSecFilingIndex` does:
  - **Payload:** adds `requestedFilingType` (the form asked for, upper-cased) and `warnings`, the Worker's fields.
  - **10-K → 20-F:** a 10-K request for an issuer that files 20-Fs indexes the latest 20-F, with `filingType` `20-F` and the `AUTO_20F_FALLBACK` info warning. Before, Python failed with `NO_FILING_DATA`.
  - **A form that is not filed:** `FILING_NOT_FOUND_TRY_OTHER_TYPE` with `availableFilingTypes`, instead of `NO_FILING_DATA` "No <form> filing found". A ticker without submissions stays `TICKER_NOT_FOUND`, and a pinned accession that is not found stays `ACCESSION_NOT_FOUND`. A primary document that is missing or is XBRL is `FILING_TEXT_NOT_AVAILABLE`.
  - **Failure payloads:** the Worker's shape, `{"ok": false, "error": {...}}` with the error's fields also at the top level, instead of `{"ok": false, "data": null, "error": {"code", "message"}}`.
  - **Document read:** through the EDGAR client's archive cache, bounded at 12,000,000 UTF-16 code units as in the Worker. It was a separate 5 MB read. A failed read is `PROVIDER_ERROR` "Failed to fetch filing: HTTP <status>"; an unexpected error is `PROVIDER_ERROR` with its message.
- **The public tool lists 8 row labels:** a table with more lists its first 8 and `rowLabelCount`, the Worker's `compactFilingIndexPayload`. The tools built on the index (`get_sec_filing_outline`, `list_sec_filing_tables`, `query_sec_filing_index`, `get_sec_filing_intelligence`, the exposure extractors) read every label through `_sec_filing_index_full`, as the Worker's internal callers do.
- **Through the index:** `get_sec_filing_outline` without `document_url` and `list_sec_filing_tables` without `document_url` pass these failures and the 20-F fallback on.
- **Tests:** 16 parity cases in `scripts/parity/cases_index_payload_2263.py`.
- **Rollback:** redeploy 2.26.2.

## Python Filing Index Labels and Table Titles (2.26.2)

- **Why:** findings F48 and F49 (`analysis/yahoo-finance-mcp/F_TRIAGE.md` §6), which the owner classified on 2026-10-27 as repairs toward the Worker. Python only; no new field, status, warning code or enum value.
- **F48, `get_sec_filing_index` `filingType`:** the filing's own form, as in the Worker. With a pinned `accession_number` of another form than `filing_type` (a 10-K accession asked for as a 10-Q), Python said `10-Q`; it now says `10-K`. `get_sec_filing_outline` without `document_url` inherits the label. The index cache is keyed by that form too.
- **F49, index table `title`:** the last line of the 500 characters before the table, read one line per block element (`_strip_html_blocks`, the Worker's `_stripHtmlTagsIdx`), cleaned of leftover tags, XBRL attributes and names, and URLs (`_clean_filing_display_text`, the Worker's `cleanFilingDisplayText`). A line of 10 characters or fewer, of 200 or more, or still reading as markup (`_looks_like_filing_markup_text`) gives way to the section heading, as before. Python read those characters as one line, so a table's title was all of them, or the heading when they ran over 200. The `keywordMap`, which matches keywords in titles, follows.
- **Noticed, not fixed (F50, unclassified):** the rest of Python's index payload (`requestedFilingType`, `warnings`, the 10-K → 20-F fallback, the missing-form error code and envelope, the `rowLabels` compaction) still differs from the Worker's.
- **Tests:** parity cases in `scripts/parity/cases_routing_2260.py` (F48, index and outline) and `scripts/parity/cases_index_bold_2261.py` (F49: titles, tables and keyword map, markup or short titles).
- **Rollback:** redeploy 2.26.1.

## Python Filing Index Bold Headings (2.26.1)

- **Why:** found while checking 2.26.0 live. Since F1, Python's `get_sec_filing_outline` without `document_url` outlines the filing index. Python's index took sections from `<h1>`–`<h6>` headings only, so a filing that marks Part and Item headings in bold text, as most 10-Ks and 10-Qs do (AAPL's latest 10-K and 10-Q), returned an empty outline (`OUTLINE_NOT_PARSED`) in Python and 27 entries in the Worker. No new field, status, warning code or enum value.
- **Python `get_sec_filing_index`:** when a document has no `<h1>`–`<h6>` heading, its sections are the bold Part and Item headings outside the table of contents (`filing_item_headings`, the Worker's `filingItemHeadings`), up to 50. Each has the same fields as before: `sectionId`, `heading`, `normalizedHeading`, `level` (1 for a Part, 2 for an Item), `keywords`, `startChar` and `endChar`. Tables take the nearest such section before them, and the keyword map includes them. This matches the Worker's `_buildFilingIndexFromHtml`.
- **Python `get_sec_filing_outline` without `document_url`:** lists those sections, as the Worker does.
- **Noticed, not fixed (F49, unclassified):** an index table's `title` differs between runtimes (Python strips the preceding text without line breaks), and so does the `keywordMap` built from it. Pinned as a parity case; fixed in 2.26.2.
- **Tests:** 5 parity cases in `scripts/parity/cases_index_bold_2261.py`, including the F49 pin.
- **Rollback:** redeploy 2.26.0.

## Python EDGAR Routing (2.26.0)

- **Why:** the Python EDGAR routing release planned in `analysis/yahoo-finance-mcp/PHASE4_SPEC.md` section 3.6 (decisions D2b, D2c and D9), with findings F7, F1 and F2 from `analysis/yahoo-finance-mcp/F_TRIAGE.md`. It follows Phase 4 unit 1 (the EDGAR client move). No new field, status, warning code or enum value.
- **Python SEC filing tools go through `yfmcp/clients/edgar.py`:** `list_sec_filings` (used when `get_sec_filing_section` and `get_sec_filing_table` have no `document_url`) and the document reads of `get_sec_filing_outline`, `get_sec_filing_section`, `list_sec_filing_tables` and `get_sec_filing_table` with a `document_url`.
  - **User-Agent:** `yahoo-finance-mcp/1.0 admin@example.com` becomes the client's `yahoo-finance-mcp <EDGAR_CONTACT_EMAIL>` (`contact@example.com` when unset), the contact the Worker sends.
  - **CIK resolution:** the Worker's full resolution (Yahoo, SEC's ticker index, test fixtures, the EDGAR ticker lookup) instead of an exact match in the ticker index only. A ticker the index lacks can now resolve. A miss is still `TICKER_NOT_FOUND` "Could not find EDGAR CIK for ticker '<t>'". A failed ticker-index read is no longer a `PROVIDER_ERROR` on its own; the other sources are tried first.
  - **Caching:** the ticker index for 24 hours (a failed load is not cached), submissions for 15 minutes, and filing documents for 30 minutes in the process archive cache, with one shared request for concurrent reads. Before, every call fetched both JSON files and the document.
  - **Submissions TTL:** 15 minutes for every Python caller (it was 24 hours), as the Worker's `FILING_SUBMISSIONS_TTL_MS`, so a new filing shows up within 15 minutes.
  - **Document reads:** the whole body (no size cap), a 15 s timeout and invalid UTF-8 replaced, as before. A failed read keeps its HTTP status: the outline still says `PROVIDER_ERROR` "HTTP 404", and the section and table tools keep urllib's message (for example "HTTP Error 404: Not Found"). A failed submissions read keeps urllib's message too.
- **F7, ticker index:** a failed load of SEC's `company_tickers.json` is not cached. Before, an empty map was served for up to 24 hours. Like the Worker, a failed refresh after expiry returns no map rather than the expired one.
- **F1, `get_sec_filing_outline` without `document_url`:** Python outlines the filing index, as the Worker does (`secIndexOutlinePayload`). The payload adds `filingType`, `filingDate` and each entry's `sectionId`, the Worker's existing fields. A pinned `accession_number` is honoured; before, Python read the latest primary document and ignored it.
- **F2, outline titles:** inner tags in a heading become a space in Python, as in the Worker. "<b>Item</b><b>1A.</b>" reads "Item 1A.", not "Item1A.".
- **D9, `parse_public_transcript` with an SEC page:** both runtimes fetch it through the EDGAR client with SEC's User-Agent instead of a browser User-Agent. The read is otherwise unchanged: the cap (5 MB), the timeout (30 s in Python, none in the Worker), the decoding and the failure codes. It is not cached.
- **Noticed, not fixed (F48, unclassified):** for a pinned accession whose form differs from `filing_type`, `get_sec_filing_index` (and so the outline) labels `filingType` with the requested form in Python and with the filing's own form in the Worker. Both answer the same filing. Pinned as a parity case.
- **Tests:** `scripts/test_python_edgar_routing.py` (16 tests) and 7 parity cases in `scripts/parity/cases_routing_2260.py`.
- **Rollback:** redeploy 2.25.7.

## Thai Fund Audit Repairs, Part 2 (2.25.7)

- **Why:** the two remaining code findings from the Thai fund audit (TF3, TF7), which the owner asked to fix on 2026-10-21. Both runtimes change alike. No new fields, statuses or warning codes; `MIXED` is an existing scope value.
- **TF3, `get_thai_fund_dividend_history`:** the SEC does not document row order, so the newest dividend on one page is not necessarily the fund's newest. `dataDate` (payload and meta) is now reported only when one page holds the whole history: no `next_cursor` was sent and none came back. A page of a longer history returns `dataDate: null`, and its recovery detail says why. Rows are still sorted newest first within the page, and the one-page contract is unchanged.
- **TF7, factsheet `urls`:** the section's scope is `MIXED` instead of `SHARE_CLASS`, because `amcUrlFactsheet` is the share class's document but `pdfFactsheet` is the SEC's one document for the whole project. The recovery detail says which is which.
- **Still open:** TF9, empty dividend history for dividend-paying funds, waits on a raw provider call with `SEC_OPEN_DATA_API_KEY` in the cloud environment.
- **Tests:** `scripts/test_thai_funds.py` (complete versus partial dividend history, URL scope, Worker source parity).
- **Rollback:** redeploy 2.25.6.

## Thai Fund Audit Repairs (2.25.6)

- **Why:** the Thai fund audit against the Thai SEC Open Data API spec (`/mnt/project-files/audits/thai-funds-audit-2026-10-21.md`, items TF2, TF4, TF5, TF6, TF8), approved by the owner on 2026-10-21. Both runtimes change alike. No new fields, statuses, warning codes or enum values.
- **TF2, explicit `proj_id` NAV (`get_thai_fund_nav`, `get_thai_fund_nav_batch`):** the direct NAV read now passes `fund_class_name` first. Rows for that class are `NAV_PROJECT_ID_AND_SEC_CLASS_CONFIRMED`. Only when it returns no row for the class does the tool read the project unfiltered, where one SEC class (`main` or `-`) is still the alias and several classes are still `AMBIGUOUS_SHARE_CLASS`. Before, the unfiltered first page of a multi-class project (SCBSEMI already exceeds 100 rows in 45 days) could hide the class's newest NAV. An alias fund now costs two provider calls, so a 20-fund batch stays within the Worker's 45-subrequest budget.
- **TF4, factsheet `urls`:** rows are listed newest `as_of_date` first and the section's `asOfDate` is the newest one, since the endpoint has no `latest` filter.
- **TF5, factsheet `top_holdings`:** rows are listed by `asset_seq`, unranked rows last.
- **TF6, errors:** Thai `PROVIDER_TIMEOUT` and `RATE_LIMIT` failures carry `retryable: true` on `error` and `meta`, so clients and the deployed canary retry them like other providers' transient failures.
- **TF8, docs:** `docs/thai-sec-fund-phase2.md` names its spec source, records the portal migration, lists `/general-info/amcs`, and records that the live gateway exposes exactly the 21 documented fund endpoints.
- **Not changed:** dividend history (TF3, TF9) and the URL section's `SHARE_CLASS` scope (TF7) wait on a provider check and the owner's call.
- **Tests:** `scripts/test_thai_funds.py` (class-filtered NAV, alias fallback call order, retryable codes, factsheet ordering, Worker source parity).
- **Rollback:** redeploy 2.25.5.

## Audit Repairs, Part 5 (2.25.5)

- **Why:** the approved F29 from the 2.25.0 audit (`analysis/yahoo-finance-mcp/F_TRIAGE.md` §5). Python only; no new fields, statuses, warning codes or enum values.
- **F29, `extract_earnings_metrics` (Python):** Python had no SEC XBRL tier, so revenue, gross margin, operating income, free cash flow and capex came only from the release text at `LOW`, or were empty. It now reads the Worker's tier 1 first:
  - For each metric it reads the SEC companyconcept facts (revenue across the equivalent revenue concepts, the latest quarter winning) and picks the latest fiscal quarter with the Worker's `selectQuarterFact`. A three-month fact is `DIRECT_QUARTER`. A cash-flow or fourth-quarter value is the year-to-date fact less the prior year-to-date fact (`YTD_DIFFERENCE`). EPS is never derived.
  - A 10-Q filed before the release is not used, every metric must share the revenue (or EPS) quarter, and the quarter must have ended within 120 days.
  - When it applies, the metrics are `HIGH` with `periodStart`, `periodEnd`, `derivation` and `sec_xbrl_10q`/`sec_xbrl_10k` evidence, and the release text is not mined for metrics (it is still read for the stated period).
  - Live AAPL output now matches the Worker for all six metrics.
- **Not changed:** Python still has no 8-K iXBRL tier (tier 2), and an empty metric's confidence on the text path is still `NOT_DECISION_GRADE` in Python and `NOT_DISCLOSED` in the Worker.
- **Tests:** `scripts/parity/cases_audit_2255.py`; `scripts/test_earnings_tools.py` stubs the XBRL tier in its release-text test.
- **Rollback:** redeploy 2.25.4.

## Audit Repairs, Part 4 (2.25.4)

- **Why:** the approved F35–F40 and F43 from the 2.25.0 audit (`analysis/yahoo-finance-mcp/F_TRIAGE.md` §5). No new fields, statuses, warning codes or enum values; Python gains the Worker's existing `extract_exposure` `code` fields.
- **F43, `get_sec_recent_events` and the news timeline:** the default forms now include `8-K/A`. An amendment keeps its `filingType` ("8-K/A") and title, and its `eventType` is the form it amends (an 8-K/A is `regulatory`, a 10-K/A is `earnings`).
- **F35, `extract_exposure` revenue status:** when the region is named only in the filing's text and no table title, header or row label names it, the revenue status is `NOT_FOUND` rather than `EXTRACTION_FAILED`. A region in a table that could not be parsed, or a truncated read, keeps `EXTRACTION_FAILED`.
- **F36, `extract_management_commentary`:** an excerpt is returned only when its visible text shows the topic. A long unpunctuated statement header that names the topic past the cut is skipped for the next sentence that shows it.
- **F37, `get_capital_structure`:** total debt with no maturity ladder and no instrument maturity dates is `PARTIAL` with `MATURITY_LADDER_INCOMPLETE`, which, on a filing other than a 10-K, says the 10-K usually tags the ladder. Debt with a ladder stays `COMPUTED`.
- **F38, `get_funding_schedule`:** "purchase obligations" and "purchase commitments" are category `commitment`, even beside "manufacturing", not `capital_expenditure`. A sentence whose only commitment word is the bare verb "committed", with no amount or date, is `UNRESOLVED`; "non-cancelable commitments" without an amount stays committed.
- **F39, `extract_china_exposure`:** manufacturing evidence also matches "manufactured", "outsourcing partner" and "contract manufacturer", so Apple's "manufactured by outsourcing partners located primarily in China mainland" is found.
- **F40, `extract_revenue_exposure`:** a value read from a product or category table (lead-in "by product", "by category", "disaggregated") is `exposureType` `segment_revenue`; a region from the geographic table stays `geographic_revenue`. The cached filing is re-read, so it costs no extra request.
- **Tests:** `scripts/parity/cases_audit_2254.py`; the default-forms case in `scripts/parity/cases_news_b.py`.
- **Rollback:** redeploy 2.25.3.

## Audit Repairs, Part 3 (2.25.3)

- **Why:** the approved F32 and F46 from the 2.25.0 audit (`analysis/yahoo-finance-mcp/F_TRIAGE.md` §5). No new fields, statuses, warning codes or enum values.
- **F32, `get_sec_filing_section_markdown`:**
  - **Python** matched a bold Item heading only when the `style` value held no inner quote. Inline-XBRL filings quote font names inside it (`style="font-family:'Helvetica';font-weight:700"`), so every Apple 10-K Item heading was missed and "Item 7" returned `NO_FILING_DATA`. Python now uses the Worker's bold-heading pattern and the heading's full block text, so both runtimes find the same section and report the same heading.
  - **Worker** markdown kept raw entities such as `&#160;` and `&#8217;`. It now decodes them after the tags are stripped, as Python does.
  - `word_count` still counts the returned (possibly truncated) markdown, and `truncated` says when it was cut.
- **F46, `list_sec_filing_tables` without `document_url` (Python):** lists the filing index's tables, as the Worker does, so each row carries `tableIndex`, `title`, `headers`, `rowLabels`, `sectionId`, `unitScale` and `confidence`, and the payload carries the filing's `filingType`, `filingDate` and `accessionNumber`. With `document_url` both runtimes still list the raw document (`rowCount`, `qualityStatus`).
- **Tests:** `scripts/parity/cases_audit_2253.py`.
- **Rollback:** redeploy 2.25.2.

## Audit Repairs, Part 2 (2.25.2)

- **Why:** the approved F30, F31, F33, F34 and F42 from the 2.25.0 audit (`analysis/yahoo-finance-mcp/F_TRIAGE.md` §5). No new fields, statuses, warning codes or enum values.
- **F30, `get_market_quote` for an unknown ticker (Python):** returns `TICKER_NOT_FOUND` ("No provider data was found for the requested ticker.", `retryable: false`), as the Worker does. It used to return `ok: true` with every field null and `yearHigh`/`yearLow` as NaN, which is not valid JSON. NaN fields are now null in every Python quote.
- **F31, enum parameters in Python grouped calls:** the grouped boundary refused every value of an enum-typed parameter, so `get_ownership_holders` (every `holder_type`) and `get_analyst_recommendations` (every `recommendation_type`) failed with `INPUT_VALIDATION_ERROR`. It now accepts the value's type, as it already did for `Literal` parameters; the tool still rejects a value it does not support.
- **F33, `get_fund_profile` on a company:** returns `INPUT_VALIDATION_ERROR` naming the quote type ("TICKER is quote type EQUITY, not an ETF or fund; ...") instead of empty or failed sections that recommended a retry. ETF, MUTUALFUND and MONEYMARKET are funds. Both runtimes.
- **F34, Worker company profile:** a later quoteSummary module's empty value no longer overwrites an earlier one's, so `priceToSalesTrailing12Months` (and any field in the same position) is kept, as in Python.
- **F42, `calculate_price_target_distance`:** a missing, zero or negative target is `INPUT_VALIDATION_ERROR`, not `PROVIDER_ERROR`. Both runtimes.
- **Tests:** `scripts/parity/cases_audit_2252.py`, the updated price-target cases, and a Python regression for F30.
- **Rollback:** redeploy 2.25.1.

## Per-Person Access Keys (2.18.0)

- **Why:** Phase 1b, approved by the owner on 2026-10-18, for sharing the hosted MCP with a few trusted people, non-commercially. Release type: minor. Worker only: the local Python server is single-user stdio.
- **Status: shipped inactive.** Personal keys need two bindings, the KV namespace `ACCESS_KEYS` (`yfmcp_access_keys`) and the Durable Object `USAGE` (class `KeyUsage`). This release adds neither, so only the owner key (`MCP_TOKEN`) works. Activation is a follow-up; see below.
- **Owner key:** unchanged. It is compared in constant time first, with no limits, no key store and no counter call, and it keeps full provider access.
- **Personal keys:**
  - **Format:** `yfk_` followed by 43 base64url characters. Accepted as `Authorization: Bearer <key>` or `/mcp/<key>`.
  - **Storage:** only as a SHA-256 hash, under KV key `key:<sha256>`, with the record `{id, label, createdAt, status, dailyLimit, perMinuteLimit, paidProviders}`. `id` is the first 8 hex characters of the hash; `label` is a non-secret name.
  - **Unknown or revoked keys** get the existing `AUTH_REQUIRED` 401, byte-identical to a wrong token.
  - **Lookup cache:** results are cached in the isolate for 60 seconds, so a revoke takes effect within a minute.
- **Limits:**
  - **Defaults:** `perMinuteLimit` 30 and `dailyLimit` 2000 (UTC day), counted in the `KeyUsage` Durable Object, one per key.
  - **Over a limit:** HTTP 429 with a `Retry-After` header and the body `{"jsonrpc":"2.0","id":null,"error":{"code":-32029,"message":"Key rate limit reached.","data":{"code":"KEY_RATE_LIMITED","retryAfterSeconds":N}}}`. Refused requests are not counted.
  - **Shared capacity:** all users share the Free plan's roughly 100,000 requests a day, so keep the sum of daily limits well below that.
  - **Subrequests:** the counter call is one subrequest, taken from the 5-request reserve outside the 45 budget.
- **Provider access:** a key with `paidProviders: false` (the default) sees the Finnhub, Marketaux and Alpha Vantage credentials as unset. Those providers then report as unconfigured through their existing paths, which protects the shared free quotas. Data already in the cache may still be served, which costs no quota.
- **Failures:** if the key store or the counter fails, or a record is malformed, the request fails closed (500, telemetry outcome `exception`). It never fails open, and the owner key is unaffected.
- **Telemetry:**
  - Blob 12 is `keyId`: `owner`, a personal key's 8-hex id, or `-` when no key is identified. The key and the label are never recorded.
  - An over-limit request records `rejected:key_limit` with `KEY_RATE_LIMITED`.
  - The daily export groups requests by `keyId`.
- **Admin script `scripts/manage_access_keys.py`** (needs the `ACCESS_KEYS` binding):
  - `issue --label NAME [--daily N] [--per-minute N] [--paid-providers]` prints the new key once; send it to the person privately.
  - `list` shows ids, labels, status and limits. It never shows keys or hashes.
  - `revoke --id ID`.
  - `set-limits --id ID [...]`.
  - To rotate a key, issue a new one and revoke the old.
- **Rules for each person given a key:**
  - personal, non-commercial use only;
  - no redistribution of data, because of the upstream providers' terms;
  - do not share the key;
  - the daily capacity is shared with others.
- **Activation (follow-up):**
  1. The owner creates the KV namespace `yfmcp_access_keys` and gives its ID (not a secret).
  2. A PR adds the `ACCESS_KEYS` and `USAGE` bindings, the `new_sqlite_classes` migration, and a dispatch-only workflow, `apply-do-migration.yml`.
  3. The planner runs that workflow once on the PR's head. It runs `wrangler deploy`, because Cloudflare applies Durable Object migrations only that way, then the production canaries.
  4. After that the PR merges, and the normal versioned pipeline continues as before.
- **Rollback:** revert the release. Before activation there is nothing to undo.

## News Repairs, Batch A (2.17.0)

- **Why:** R-news part 3, Batch A of the approved Q3i triage (`analysis/yahoo-finance-mcp/RNEWS3_SPEC.md`). Release type: minor. News outputs change in both runtimes, identically.
- **Yahoo feed (Worker):** the Worker now passes through `contentType`, `summary` and the `content.pubDate` fallback.
  - Yahoo videos are filtered out.
  - Yahoo's own press-release rows are relabelled and leave a news-only selection.
  - A row with a zero publish time keeps its date.
  - **Both runtimes:** the summary is shown, but issuer and ticker matching read the title only.
- **Event typing:**
  - One keyword table in both runtimes (adding "announces new", "annual report" and "credit facility" to the Worker).
  - Matching is whole-word, with simple endings (-s, -es, -d, -ed, -ing), and the first group still wins.
  - For example, "sector" no longer counts as `sec` (regulatory) and "dealer" no longer counts as `deal`, while "launches", "introduces" and "notes" still type as before.
- **Dedupe:** both runtimes decode HTML entities in titles before grouping, so `&amp;` and `&` titles merge.
- **Identity and tickers:**
  - A short name equal to the ticker is not issuer identity.
  - A single-letter ticker matches only as a whole word.
  - Verify relevance is scored from the evidence text and issuer, not the injected tickers list, so `LOW_RELEVANCE_EVIDENCE` can fire.
- **Exchanges:**
  - One normaliser: NYSE American, AMEX and exact ASE are tested before NYSE, and GS/NMS/NGM/NCM count as NASDAQ only as whole tokens.
  - Python gains the GlobeNewswire exchange check.
- **Counts:** the legacy `yahoo_finance` source counts its feed once (raw 20 for 20 stories, not 40).
- **Form 4:**
  - Values and the summary round half away from zero to cents, decimal-safe, so the same figure appears in both runtimes.
  - Python parses a well-formed XHTML Form 4 through its HTML parser when the XML has no transaction node.
- **Parsing and format:**
  - Timestamps drop `.000` milliseconds.
  - Python parses RFC 2822 provider dates.
  - Text is cut at 240 code points.
  - An unparseable URL has provenance `UNKNOWN`.
  - GlobeNewswire and company IR feeds containing `<!DOCTYPE` are refused, with one message: "unsupported XML declaration".
  - The Python news path flags an SEC filing without a URL (`SEC_URL_INVALID`).
- **Documented as intentional:** the `duplicateGroupId` hash format differs between runtimes. It is an opaque id.
- **Rollback:** revert the release.

## News Source Registry and Off Switch (2.16.0)

- **Why:** R-news part 2 from the approved Q3i triage (§3a), plus the owner's 2026-10-18 decision to add SEC to the default news sources. Release type: minor.
- **Source registry:** `worker/src/news-source-registry.json` lists every news source name that both runtimes accept. Each entry has:
  - `tier`: `official` (`sec`, `company_ir`, `company_ir_page`, `newswire`) or `aggregator` (Yahoo variants, `finnhub`, `marketaux`);
  - `requiresKey`: the env names each runtime reads;
  - `approxSubrequests`: a cold count for one ticker.

  Both runtimes read their default source list from its `defaultSources` field, and a test keeps the registry and both runtimes in step.
- **New defaults:** `yahoo_finance_news`, `yahoo_finance_press_releases`, `sec`, `finnhub`, `marketaux` (Marketaux stays a fallback). They apply to `get_company_news` and `search_company_news` when `sources` is omitted, and the advertised defaults and descriptions say so.
- **Budget:** default `get_company_news` for one ticker, measured cold against live upstream:
  - 11 subrequests before and 12 after (provider keys set);
  - 9 before and 10 after (no keys).

  SEC adds only the submissions request, because the ticker index is already read for identity. The limit is 45 per invocation.
- **SEC for a ticker with no SEC CIK** (most foreign listings): when SEC's ticker index was read and has no entry, `sourceStatus.sec` is `NOT_ELIGIBLE` (`reasonCode: null`).
  - It carries the `SOURCE_NOT_ELIGIBLE` warning "SEC has no filings for <TICKER> (no SEC CIK).".
  - Coverage reads PARTIAL with `CHECK_OFFICIAL_RELEASES`, as an ineligible Finnhub ticker already does.
  - If the index could not be read, or the submissions fetch failed, the result stays `PROVIDER_ERROR` "SEC submissions source unavailable.".
  - `get_sec_recent_events` is unchanged.
- **Operator off switch, `NEWS_SOURCES_DISABLED`:** a comma-separated list of source names. Names are trimmed and case-insensitive, and unknown names are ignored. Set it under `[vars]` in `worker/wrangler.toml`, for example `NEWS_SOURCES_DISABLED = "marketaux,finnhub"`, then redeploy. Locally, set it as an environment variable. A disabled source:
  - is never fetched and costs no subrequests;
  - reports `sourceStatus.<name> = {status: "UNCONFIGURED", rawCount: 0, filteredCount: 0, attempted: false}`;
  - adds an info warning, `SOURCE_UNAVAILABLE` "Source <name> is disabled by the server operator.";
  - adds a `coverage.failedSources` entry, so coverage reads PARTIAL;
  - never yields NOT_FOUND by itself.

  Further rules:
  - Disabling `finnhub` counts as limited coverage, so the Marketaux fallback runs unless `marketaux` is disabled too.
  - `get_company_press_releases` reports a disabled source once, and its status uses only the active sources.
  - `get_sec_recent_events` is not affected.
  - Production does not set the variable.
- **Rollback:** revert the release, or unset the variable to undo a disable.

## News Provider Adapters (2.15.1)

- **Why:** R-news part 1 from the approved Q3i triage (§3a). Release type: patch. This is a pure restructure with no behaviour change.
- **Worker layout:** each news source has its own module under `worker/src/news/providers/`:
  - the sources are `yahoo`, `sec`, `company-ir`, `company-ir-page`, `globenewswire`, `finnhub` and `marketaux`;
  - helpers shared by providers and the pipeline are in `worker/src/news/shared.ts`;
  - the adapter result types are in `types.ts`;
  - `worker/src/news.ts` keeps the pipeline (source lists, dedupe, coverage, collection status, `collectCompanyEvents`, ranking, tool functions) and re-exports every name it exported before. It went from about 3,100 lines to about 1,450.
- **Python layout:**
  - the same split is in `yfmcp/news/providers/*.py` and `yfmcp/news/shared.py`;
  - `yfmcp/tools/news.py` keeps the pipeline and re-imports every moved name, so `yfmcp.tools.news.<name>` and `server.<name>` still resolve. It went from about 3,400 lines to about 2,000.
  - Python has no separate `company_ir` collector: the pipeline still serves it inline, and `company_ir.py` is a placeholder until the R-news repairs port the Worker's registry-only collector.
- **Adapter contract:** every collector keeps its signature and its result (`items`, `warnings`, `used`, and per-source diagnostics where it has them). Providers never import the pipeline.
- **Proof of no change:**
  - All 1,155 parity cases and `tools/list` (expanded and grouped) were serialised in both runtimes before and after the move, and the dumps are identical.
  - The parity harness passes with no case edited.
  - Tests that stub a provider helper now patch it in the provider module.
- **CI:** the Python syntax-check step now covers `yfmcp/news/`.
- **Rollback:** revert the release.

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


## Evidence Component Completeness (2.5.1)

- Evidence-pack component wrappers propagate top-level `PARTIAL`, `INCOMPLETE`, and `STALE` source statuses as `LIMITED`. A mechanically successful sub-tool call is not enough to call the component `OK` when its own payload says the evidence is incomplete.
- `provenance.coverage.state` summarizes component-wrapper status only. The receipt now states `scope: "COMPONENT_WRAPPER_STATUS"` and `evidenceCompleteness: "NOT_ASSERTED"`; `COMPLETE` must never be interpreted as decision-grade evidence completeness, valuation completeness, denominator clearance, or authority to select a method, multiple, scenario, target, G2, opportunity, or action.
- Consumers must continue to inspect the underlying component payload, `evidenceQuality`, warning codes, coverage cells, and source-specific statuses. In particular, a dilution bridge that reports `PARTIAL` keeps the pack receipt `PARTIAL`.
