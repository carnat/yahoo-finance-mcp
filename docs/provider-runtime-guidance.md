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

## Impossible Provider Rows And Split Adjustment (2.5.23)

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
- **Effect.** An ANET-shaped fixture (Alpha Vantage revenue mean above its
  high, and an Alpha Vantage FY0 EPS still on the pre-split count 16 days
  after a 4-for-1) gives the following:
  - Revenue `SINGLE_PROVIDER` with Alpha Vantage excluded, instead of a
    comparison against an impossible figure.
  - EPS `NOT_SPLIT_ADJUSTED` naming Alpha Vantage, instead of a 75%
    `CONFLICT`.
  - Yahoo's 30/60/90-day windows `SPLIT_IN_WINDOW`, instead of a −74% "revision".

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
