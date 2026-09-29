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
  - convertibles: `if_converted_when_in_the_money`, `if_converted_all` or
    `exclude`;
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
