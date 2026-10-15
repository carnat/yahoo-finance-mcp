# Data Objects — yahoo-finance-mcp

52 core records/DTOs catalogued by the extraction workflow's data-object pass (run `wf_36b55a43-b0d`). Rule references link to `BUSINESS_RULES.md` IDs where the name matched exactly. Unmatched names are shown as text.

| Object | Source | Fields |
|---|---|---:|
| [IxFact](#ixfact) | `worker/src/capital-structure.ts:24` | 12 |
| [IxFact (Python mirror)](#ixfact-python-mirror) | `yfmcp/capital_structure.py:154` | 12 |
| [IxContext](#ixcontext) | `worker/src/capital-structure.ts:16` | 5 |
| [IxDocument](#ixdocument) | `worker/src/capital-structure.ts:41` | 4 |
| [IxSource](#ixsource) | `worker/src/capital-structure.ts:48` | 6 |
| [TextMatch](#textmatch) | `worker/src/capital-structure.ts:1516` | 8 |
| [DilutionInput](#dilutioninput) | `worker/src/capital-structure.ts:1864` | 11 |
| [WarrantLifecycleSentence](#warrantlifecyclesentence) | `worker/src/capital-structure.ts:818` | 7 |
| [SettlementSentence](#settlementsentence) | `worker/src/capital-structure.ts:1132` | 7 |
| [CappedCallTerms](#cappedcallterms) | `worker/src/capital-structure.ts:1188` | 11 |
| [PreferredConversionText](#preferredconversiontext) | `worker/src/capital-structure.ts:1419` | 5 |
| [CapitalStructureInput](#capitalstructureinput) | `worker/src/capital-structure.ts:2415` | 4 |
| [NewsItem / RatingChange](#newsitem-ratingchange) | `worker/src/capital-structure.ts:2948` | 6 |
| [MarketInputs](#marketinputs) | `worker/src/valuation.ts:21` | 17 |
| [Estimate](#estimate) | `worker/src/valuation.ts:13` | 5 |
| [SnapshotInput](#snapshotinput) | `worker/src/valuation.ts:181` | 6 |
| [ValuationHistoryInput](#valuationhistoryinput) | `worker/src/valuation-history.ts:31` | 11 |
| [PeriodicFiling](#periodicfiling) | `worker/src/valuation-history.ts:56` | 4 |
| [Fact (valuation-history) / Row (metric-reconciliation)](#fact-valuation-history-row-metric-reconciliation) | `worker/src/metric-reconciliation.ts:55` | 9 |
| [MetricSpec](#metricspec) | `worker/src/metric-reconciliation.ts:24` | 6 |
| [ReconcileInput](#reconcileinput) | `worker/src/metric-reconciliation.ts:512` | 7 |
| [ShareScenario / KnownIssuance](#sharescenario-knownissuance) | `worker/src/share-scenarios.ts:47` | 5 |
| [ProviderConsensusInput](#providerconsensusinput) | `worker/src/evidence.ts:255` | 7 |
| [ProviderPeriod](#providerperiod) | `worker/src/evidence.ts:245` | 7 |
| [MetricEstimate](#metricestimate) | `worker/src/evidence.ts:219` | 7 |
| [EpsTrend / EpsRevisionCounts](#epstrend-epsrevisioncounts) | `worker/src/evidence.ts:230` | 3 |
| [StaleFallback](#stalefallback) | `worker/src/evidence.ts:130` | 4 |
| [SplitEvent / SplitHistory](#splitevent-splithistory) | `worker/src/evidence.ts:267` | 5 |
| [ConsensusPolicy](#consensuspolicy) | `worker/src/evidence.ts:464` | 4 |
| [FilingRow](#filingrow) | `worker/src/evidence.ts:918` | 5 |
| [ComponentRecord](#componentrecord) | `worker/src/evidence.ts:1067` | 6 |
| [EvidenceStore (interface) / StorageStatus](#evidencestore-interface-storagestatus) | `worker/src/evidence-store.ts:12` | 6 |
| [MarketQuote (evidence pack)](#marketquote-evidence-pack) | `worker/src/evidence-pack.ts:58` | 5 |
| [AnnualFilingRef](#annualfilingref) | `worker/src/companyfacts-coverage.ts:12` | 4 |
| [FiscalCalendar / FiscalYearNaming](#fiscalcalendar-fiscalyearnaming) | `worker/src/fiscal-calendar.ts:135` | 9 |
| [EarningsPeriodInfo](#earningsperiodinfo) | `worker/src/earnings-period.ts:10` | 3 |
| [ConceptFacts / FilingPeriod](#conceptfacts-filingperiod) | `worker/src/sec-facts.ts:19` | 3 |
| [ConcentrationFinding](#concentrationfinding) | `worker/src/extraction-rules.ts:49` | 7 |
| [RangeMatch](#rangematch) | `worker/src/extraction-rules.ts:322` | 6 |
| [ReleaseMetricHit](#releasemetrichit) | `worker/src/extraction-rules.ts:466` | 4 |
| [ReleaseText](#releasetext) | `worker/src/guidance-history.ts:21` | 5 |
| [Statement / PlainFact (funding schedule)](#statement-plainfact-funding-schedule) | `worker/src/funding-schedule.ts:26` | 6 |
| [Amount (funding schedule, internal)](#amount-funding-schedule-internal) | `worker/src/funding-schedule.ts:156` | 5 |
| [DriverLedgerInput / InlineFact / DriverStatement](#driverledgerinput-inlinefact-driverstatement) | `worker/src/driver-ledger.ts:301` | 6 |
| [Filing search records (FilingTextProjection, SearchDocument, SearchMatch, Hit, TermStat, SearchSpec)](#filing-search-records-filingtextprojection-searchdocument-searchmatch-hit-termstat-searchspec) | `worker/src/filing-search.ts:698` | 11 |
| [FundIdentity / Resolution / NavBatchFund (Thai SEC)](#fundidentity-resolution-navbatchfund-thai-sec) | `worker/src/sec-thailand.ts:34` | 9 |
| [McpResponse / ToolMeta / ErrorDetail (response envelope)](#mcpresponse-toolmeta-errordetail-response-envelope) | `worker/src/response.ts:66` | 4 |
| [LimitResult](#limitresult) | `worker/src/yahoo-finance.ts:549` | 3 |
| [DataQuality (options chain)](#dataquality-options-chain) | `worker/src/yahoo-finance.ts:600` | 12 |
| [SegmentTableResult / SegmentTableRow](#segmenttableresult-segmenttablerow) | `worker/src/yahoo-finance.ts:14712` | 8 |
| [QuarterFact](#quarterfact) | `worker/src/yahoo-finance.ts:17124` | 6 |
| [YahooTranscriptResult / ProviderJsonResult (Python provider results)](#yahootranscriptresult-providerjsonresult-python-provider-results) | `yfmcp/clients/yahoo_transcripts.py:49` | 11 |

## IxFact

Source: `worker/src/capital-structure.ts:24`

| Field | Type | Note |
|---|---|---|
| `name` | `string` | qualified XBRL concept (prefix:Local) |
| `local` | `string` | concept local name |
| `contextRef` | `string` |  |
| `unit` | `string \| null` |  |
| `value` | `number \| null` |  |
| `text` | `string \| null` |  |
| `periodEnd` | `string \| null` |  |
| `periodStart` | `string \| null` |  |
| `dims` | `Record<string,string>` | axis -> member |
| `decimals` | `number \| null` | null when INF or absent (exact) |
| `sentence` | `string \| null` | surrounding sentence for investment concepts outside a table |
| `order` | `number` | document order |

**Used by rules:** BR-EXT-097 (Treasury-stock incremental shares for options and warrants); BR-EXT-011 (If-converted shares for convertible notes); BR-EXT-002 (Conversion ratio unit normalization); BR-EXT-049 (Convertible preferred if-converted and liquidation preference); BR-EXT-099 (Warrant count vs company-reported antidilutive table); BR-EXT-023 (Total debt concept hierarchy and finance-lease removal); BR-EXT-036 (Balance-sheet fact exclusion (rounded or overlapping cash)); BR-EXT-006 (Debt instrument period-end status); BR-EXT-012 (Instrument-to-total-debt reconciliation); BR-EXT-025 (Unvested RSU/PSU awards counted gross); BR-EXT-095 (Stock option dilution by treasury-stock method (tranches before weighted-average strike))

## IxFact (Python mirror)

Source: `yfmcp/capital_structure.py:154`

| Field | Type | Note |
|---|---|---|
| `name` | `str` |  |
| `local` | `str` |  |
| `context_ref` | `str` |  |
| `unit` | `str \| None` |  |
| `value` | `float \| None` |  |
| `text` | `str \| None` |  |
| `period_end` | `str \| None` |  |
| `period_start` | `str \| None` |  |
| `dims` | `dict[str,str]` |  |
| `order` | `int` | declared before decimals/sentence in Python (dataclass default ordering); TS declares order last |
| `decimals` | `int \| None` | default None |
| `sentence` | `str \| None` | default None |

**Used by rules:** BR-EXT-024 (Treasury-stock and if-converted dilution mechanics); BR-EXT-023 (Total debt concept hierarchy and finance-lease removal)

## IxContext

Source: `worker/src/capital-structure.ts:16`

| Field | Type | Note |
|---|---|---|
| `id` | `string` |  |
| `instant` | `string \| null` |  |
| `start` | `string \| null` |  |
| `end` | `string \| null` |  |
| `dims` | `Record<string,string>` |  |

**Used by rules:** BR-SEC-007 (Fiscal-year assignment for SEC facts (52/53-week years)); BR-EXT-006 (Debt instrument period-end status)

## IxDocument

Source: `worker/src/capital-structure.ts:41`

| Field | Type | Note |
|---|---|---|
| `facts` | `IxFact[]` |  |
| `contextCount` | `number` |  |
| `documentPeriodEnd` | `string \| null` |  |
| `documentType` | `string \| null` |  |

**Used by rules:** BR-EXT-058 (Dilution bridge status); BR-EXT-006 (Debt instrument period-end status); BR-EXT-012 (Instrument-to-total-debt reconciliation); BR-EXT-109 (Company-specific XBRL driver exclusion)

## IxSource

Source: `worker/src/capital-structure.ts:48`

| Field | Type | Note |
|---|---|---|
| `role` | `string` | primary / supplementary filing role (inferred) |
| `filingType` | `string` |  |
| `filingDate` | `string \| null` |  |
| `accessionNumber` | `string \| null` |  |
| `documentUrl` | `string \| null` |  |
| `doc` | `IxDocument` |  |

**Used by rules:** BR-EXT-056 (Diluted share count and dilution percentage at price); BR-EXT-058 (Dilution bridge status); BR-EXT-027 (Warrant exercisable count and treasury-stock dilution); BR-EXT-012 (Instrument-to-total-debt reconciliation); BR-VAL-032 (SEC balance usage guards (currency, staleness, cash cross-checks))

## TextMatch

Source: `worker/src/capital-structure.ts:1516`

| Field | Type | Note |
|---|---|---|
| `contextText` | `string` |  |
| `sectionHeading` | `string \| null` |  |
| `documentUrl` | `string \| null` |  |
| `filingDate` | `string \| null` |  |
| `accessionNumber` | `string \| null` |  |
| `inTable` | `boolean?` |  |
| `tableTitle` | `string \| null?` |  |
| `rowLabel` | `string \| null?` |  |

**Used by rules:** BR-EXT-033 (ATM program potential shares); BR-EXT-013 (Net-share settlement when principal is stated as cash-settled); BR-EXT-039 (Capped call economic offset); BR-EXT-026 (Warrant class retirement by filing text); BR-EXT-062 (Funding text amount parsing and range rule); BR-EXT-025 (Unvested RSU/PSU awards counted gross)

## DilutionInput

Source: `worker/src/capital-structure.ts:1864`

| Field | Type | Note |
|---|---|---|
| `ticker` | `string` |  |
| `price` | `number` | caller-supplied price |
| `priceCurrency` | `string` |  |
| `asOfDate` | `string \| null` |  |
| `sources` | `IxSource[]` | sources[0] treated as primary (capital-structure.ts:1883) |
| `atmMatches` | `TextMatch[]` |  |
| `awardTableMatches` | `TextMatch[]?` |  |
| `claimMatches` | `TextMatch[] \| null?` | null = not read |
| `warrantLifecycleMatches` | `TextMatch[] \| null?` |  |
| `convertibleSettlementMatches` | `TextMatch[] \| null?` |  |
| `cappedCallMatches` | `TextMatch[] \| null?` |  |

**Used by rules:** BR-EXT-097 (Treasury-stock incremental shares for options and warrants); BR-EXT-011 (If-converted shares for convertible notes); BR-EXT-013 (Net-share settlement when principal is stated as cash-settled); BR-EXT-039 (Capped call economic offset); BR-EXT-056 (Diluted share count and dilution percentage at price); BR-EXT-058 (Dilution bridge status); BR-EXT-033 (ATM program potential shares); BR-EXT-049 (Convertible preferred if-converted and liquidation preference); BR-EXT-057 (Dilution bridge requires a caller price); BR-EXT-079 (Price input validation for bridge and snapshot); BR-EXT-024 (Treasury-stock and if-converted dilution mechanics); BR-EXT-027 (Warrant exercisable count and treasury-stock dilution); BR-EXT-085 (Share claim status lifecycle); BR-EXT-095 (Stock option dilution by treasury-stock method (tranches before weighted-average strike)); BR-EXT-025 (Unvested RSU/PSU awards counted gross)

## WarrantLifecycleSentence

Source: `worker/src/capital-structure.ts:818`

| Field | Type | Note |
|---|---|---|
| `event` | `string` | exercise / expiry / redemption |
| `sentence` | `string` |  |
| `dates` | `string[]` |  |
| `sectionHeading` | `string \| null` |  |
| `documentUrl` | `string \| null` |  |
| `filingDate` | `string \| null` |  |
| `accessionNumber` | `string \| null` |  |

**Used by rules:** BR-EXT-026 (Warrant class retirement by filing text); BR-EXT-027 (Warrant exercisable count and treasury-stock dilution)

## SettlementSentence

Source: `worker/src/capital-structure.ts:1132`

| Field | Type | Note |
|---|---|---|
| `sentence` | `string` |  |
| `scope` | `string` |  |
| `years` | `string[]` |  |
| `sectionHeading` | `string \| null` |  |
| `documentUrl` | `string \| null` |  |
| `filingDate` | `string \| null` |  |
| `accessionNumber` | `string \| null` |  |

**Used by rules:** BR-EXT-013 (Net-share settlement when principal is stated as cash-settled)

## CappedCallTerms

Source: `worker/src/capital-structure.ts:1188`

| Field | Type | Note |
|---|---|---|
| `years` | `string[]` |  |
| `strikePrice` | `number \| null` |  |
| `strikeIsConversionPrice` | `boolean` |  |
| `capPrice` | `number \| null` |  |
| `coveredShares` | `number \| null` |  |
| `coversNoteShares` | `boolean` |  |
| `survivesConversions` | `boolean` |  |
| `sentences` | `string[]` |  |
| `documentUrl` | `string \| null` |  |
| `filingDate` | `string \| null` |  |
| `accessionNumber` | `string \| null` |  |

**Used by rules:** BR-EXT-039 (Capped call economic offset)

## PreferredConversionText

Source: `worker/src/capital-structure.ts:1419`

| Field | Type | Note |
|---|---|---|
| `ratio` | `number \| null` |  |
| `aggregateShares` | `number \| null` |  |
| `approximate` | `boolean` |  |
| `sentence` | `string` |  |
| `documentUrl` | `string \| null` |  |

**Used by rules:** BR-EXT-049 (Convertible preferred if-converted and liquidation preference)

## CapitalStructureInput

Source: `worker/src/capital-structure.ts:2415`

| Field | Type | Note |
|---|---|---|
| `ticker` | `string` |  |
| `source` | `IxSource` |  |
| `fundingMatches` | `TextMatch[]` |  |
| `maturityMatches` | `TextMatch[]?` | read only when an outstanding row lacks a tagged maturity |

**Used by rules:** BR-EXT-023 (Total debt concept hierarchy and finance-lease removal); BR-EXT-006 (Debt instrument period-end status); BR-EXT-012 (Instrument-to-total-debt reconciliation); BR-EXT-110 (Contractual obligation bucket dates); BR-EXT-066 (Funding/capex sentence classification precedence)

## NewsItem / RatingChange

Source: `worker/src/capital-structure.ts:2948`

| Field | Type | Note |
|---|---|---|
| `NewsItem.title\|summary\|url\|publishedAt\|source\|originalSource\|publisher` | `unknown?` |  |
| `RatingChange.date` | `unknown?` |  |
| `RatingChange.firm` | `unknown?` |  |
| `RatingChange.toGrade` | `unknown?` |  |
| `RatingChange.ptTo` | `unknown?` |  |
| `RatingChange.ptFrom` | `unknown?` |  |

**Used by rules:** BR-SHR-005 (Analyst rating change classification); BR-SHR-003 (Analyst price-target direction, mixed signal and strength flag); BR-EXT-111 (Event evidence ranking and word stemming)

## MarketInputs

Source: `worker/src/valuation.ts:21`

| Field | Type | Note |
|---|---|---|
| `ticker` | `string` |  |
| `name` | `string \| null` |  |
| `currency` | `string \| null` | quote currency |
| `financialCurrency` | `string \| null` |  |
| `price` | `number \| null` |  |
| `priceTime` | `string \| null` |  |
| `sharesOutstanding` | `number \| null` |  |
| `impliedSharesOutstanding` | `number \| null` |  |
| `marketCap` | `number \| null` |  |
| `totalCash` | `number \| null` |  |
| `totalDebt` | `number \| null` |  |
| `ttmRevenue` | `number \| null` |  |
| `ttmEbitda` | `number \| null` |  |
| `grossMarginPct` | `number \| null` |  |
| `trailingEps` | `number \| null` |  |
| `mostRecentQuarter` | `string \| null` | YYYY-MM-DD, staleness check |
| `estimates` | `Estimate[]` |  |

**Used by rules:** BR-VAL-046 (Enterprise value in valuation snapshot); BR-VAL-045 (Enterprise value formula); BR-VAL-001 (ADR ordinary-shares-per-quoted-share ratio); BR-VAL-037 (ADR ratio detection and share-basis mismatch); BR-VAL-088 (Yahoo implied all-class share count substitution); BR-VAL-034 (SEC vs Yahoo balance-sheet source selection and staleness); BR-VAL-073 (SEC/Yahoo cash mismatch and shortfall checks); BR-VAL-023 (Minor-unit quote conversion and currency comparability); BR-VAL-079 (Valuation multiples and denominators); BR-VAL-080 (Valuation multiples only on positive denominators and matching currency); BR-VAL-091 (Consensus revenue growth percentages); BR-VAL-058 (Peer medians and subject premium); BR-SHR-022 (Peer valuation set size and median premium); BR-VAL-081 (Valuation snapshot balance source and status); BR-VAL-078 (Valuation multiple status)

## Estimate

Source: `worker/src/valuation.ts:13`

| Field | Type | Note |
|---|---|---|
| `period` | `string` | Yahoo earningsTrend period (0q,+1y...) |
| `revenueAvg` | `number \| null` |  |
| `revenueAnalysts` | `number \| null` |  |
| `epsAvg` | `number \| null` |  |
| `epsAnalysts` | `number \| null` |  |

**Used by rules:** BR-VAL-091 (Consensus revenue growth percentages); BR-VAL-079 (Valuation multiples and denominators)

## SnapshotInput

Source: `worker/src/valuation.ts:181`

| Field | Type | Note |
|---|---|---|
| `ticker` | `string` |  |
| `market` | `MarketInputs` |  |
| `suppliedPrice` | `number \| null` |  |
| `bridge` | `Record<string,unknown> \| null` | dilution bridge output |
| `capital` | `Record<string,unknown> \| null` | capital structure output |
| `secWarnings` | `Record<string,unknown>[]` |  |

**Used by rules:** BR-VAL-046 (Enterprise value in valuation snapshot); BR-VAL-085 (Valuation: supplied price must be positive); BR-VAL-034 (SEC vs Yahoo balance-sheet source selection and staleness); BR-VAL-073 (SEC/Yahoo cash mismatch and shortfall checks); BR-VAL-032 (SEC balance usage guards (currency, staleness, cash cross-checks)); BR-VAL-037 (ADR ratio detection and share-basis mismatch); BR-VAL-081 (Valuation snapshot balance source and status)

## ValuationHistoryInput

Source: `worker/src/valuation-history.ts:31`

| Field | Type | Note |
|---|---|---|
| `ticker` | `string` |  |
| `dates` | `string[]` |  |
| `companyfacts` | `unknown` | raw SEC companyfacts JSON |
| `bars` | `Bar[] ({date,close})` |  |
| `priceCurrency` | `string \| null` |  |
| `splits` | `Split[] ({date,ratio})` |  |
| `fx` | `{pair,bars} \| null` |  |
| `fxByCurrency` | `Record<string,{pair,bars}> \| null?` |  |
| `adsRatio` | `number \| null` |  |
| `coverCounts` | `Record<string,Rec> \| null?` | keyed by accession |
| `periodicFilings` | `PeriodicFiling[] \| null?` |  |

**Used by rules:** BR-VAL-015 (LTM and LFY flow figures); BR-VAL-016 (LTM flow derivation); BR-VAL-028 (Point-in-time price with later splits undone); BR-VAL-010 (Historical market cap, EV and multiples); BR-VAL-050 (Historical staleness limits by filer cadence); BR-VAL-095 (Historical valuation dates and peers); BR-VAL-052 (Historical valuation point-in-time limits); BR-VAL-056 (Parent net income derived from ProfitLoss)

## PeriodicFiling

Source: `worker/src/valuation-history.ts:56`

| Field | Type | Note |
|---|---|---|
| `accessionNumber` | `string` |  |
| `form` | `string` |  |
| `filed` | `string` |  |
| `reportDate` | `string \| null` |  |

**Used by rules:** BR-VAL-050 (Historical staleness limits by filer cadence); BR-VAL-052 (Historical valuation point-in-time limits)

## Fact (valuation-history) / Row (metric-reconciliation)

Source: `worker/src/metric-reconciliation.ts:55`

| Field | Type | Note |
|---|---|---|
| `concept` | `string` |  |
| `start` | `string \| null` |  |
| `end` | `string` |  |
| `val` | `number` |  |
| `filed` | `string` |  |
| `form` | `string` |  |
| `accn` | `string \| null` |  |
| `fy` | `number \| null` | Row only; Fact at valuation-history.ts:67 omits fy/fp |
| `fp` | `string \| null` | Row only |

**Used by rules:** BR-VAL-021 (Metric reconciliation tolerance and match); BR-VAL-061 (Reconciliation period resolution); BR-VAL-069 (Reporting unit choice for a metric); BR-VAL-056 (Parent net income derived from ProfitLoss); BR-VAL-015 (LTM and LFY flow figures); BR-SEC-007 (Fiscal-year assignment for SEC facts (52/53-week years))

## MetricSpec

Source: `worker/src/metric-reconciliation.ts:24`

| Field | Type | Note |
|---|---|---|
| `kind` | `"duration" \| "instant"` |  |
| `unit` | `"money" \| "per_share"` |  |
| `gaap` | `string[]` |  |
| `ifrs` | `string[]` |  |
| `yahoo` | `string[]` |  |
| `releaseLabel` | `string` | regex source for release-sentence scoping |

**Used by rules:** BR-VAL-069 (Reporting unit choice for a metric); BR-VAL-062 (Reconciliation release-sentence scoping and scale); BR-VAL-056 (Parent net income derived from ProfitLoss); BR-VAL-021 (Metric reconciliation tolerance and match)

## ReconcileInput

Source: `worker/src/metric-reconciliation.ts:512`

| Field | Type | Note |
|---|---|---|
| `ticker` | `string` |  |
| `metric` | `string` | key of METRICS |
| `period` | `Rec` | periodType ANNUAL\|QUARTERLY |
| `companyfacts` | `unknown` |  |
| `releases` | `Rec[]` | oldest first |
| `yahooRows` | `Rec[] \| null` |  |
| `tolerancePct` | `number` | DEFAULT_TOLERANCE_PCT 0.5 (metric-reconciliation.ts:36) |

**Used by rules:** BR-VAL-021 (Metric reconciliation tolerance and match); BR-VAL-020 (Metric reconciliation status); BR-VAL-019 (Metric reconciliation agreement status); BR-VAL-022 (Metric source reconciliation status); BR-VAL-064 (Release figure selection when multiple releases are read); BR-VAL-062 (Reconciliation release-sentence scoping and scale); BR-VAL-061 (Reconciliation period resolution); BR-VAL-063 (Reconciliation tolerance input clamp)

## ShareScenario / KnownIssuance

Source: `worker/src/share-scenarios.ts:47`

| Field | Type | Note |
|---|---|---|
| `name` | `string` |  |
| `price` | `number` |  |
| `treatments` | `Record<TreatmentKey,string>` | keys options\|unvested_awards\|warrants\|warrant_vesting\|convertibles\|atm\|capped_calls; allowed values share-scenarios.ts:19-27, defaults :31-39 |
| `defaulted` | `TreatmentKey[]` |  |
| `knownIssuance` | `KnownIssuance[] {label:string, shares:number, source:string\|null} (share-scenarios.ts:41)` |  |

**Used by rules:** BR-EXT-092 (Share-count scenario input validation and defaults); BR-EXT-091 (Share-count scenario input validation); BR-EXT-019 (Scenario resulting shares, dilution and completeness); BR-EXT-086 (Share count scenario completeness); BR-EXT-072 (Half-up rounding convention)

## ProviderConsensusInput

Source: `worker/src/evidence.ts:255`

| Field | Type | Note |
|---|---|---|
| `provider` | `string` |  |
| `status` | `string` | OK / NO_DATA / RATE_LIMIT / TIMEOUT / PROVIDER_ERROR ... |
| `retrievedAt` | `string \| null` |  |
| `providerTimestamp` | `string \| null` |  |
| `message` | `string \| null` |  |
| `periods` | `ProviderPeriod[]` |  |
| `staleFallback` | `StaleFallback?` |  |

**Used by rules:** BR-EVD-001 (Consensus provider agreement); BR-EVD-002 (Consensus provider agreement classification); BR-EVD-035 (Provider row consistency and coverage state); BR-EVD-010 (Consensus provider row coverage state); BR-EVD-003 (Cross-provider consensus agreement status); BR-EVD-032 (Metric coverage derived from agreement); BR-EVD-007 (Consensus cross-check summary status); BR-EVD-037 (Stale provider snapshot fallback); BR-EVD-039 (Stale provider snapshot fallback window); BR-EVD-038 (Stale provider snapshot fallback eligibility); BR-EVD-030 (FY0 resolution and fiscal-year offset); BR-EVD-014 (EPS gap explained by an unadjusted stock split)

## ProviderPeriod

Source: `worker/src/evidence.ts:245`

| Field | Type | Note |
|---|---|---|
| `providerPeriodLabel` | `string` |  |
| `fiscalYearEnd` | `string \| null` |  |
| `fiscalYearEndBasis` | `string` |  |
| `eps` | `MetricEstimate` |  |
| `revenue` | `MetricEstimate` |  |
| `epsTrend` | `EpsTrend \| null` |  |
| `epsRevisions` | `EpsRevisionCounts \| null` |  |

**Used by rules:** BR-EVD-030 (FY0 resolution and fiscal-year offset); BR-EVD-035 (Provider row consistency and coverage state); BR-EVD-018 (EPS revision windows); BR-EVD-016 (EPS revision window state)

## MetricEstimate

Source: `worker/src/evidence.ts:219`

| Field | Type | Note |
|---|---|---|
| `mean` | `number \| null` |  |
| `high` | `number \| null` |  |
| `low` | `number \| null` |  |
| `analystCount` | `number \| null` |  |
| `currency` | `string \| null` |  |
| `currencyBasis` | `string` |  |
| `decimals` | `{mean,high,low: number\|null}?` | compare at coarsest published precision |

**Used by rules:** BR-EVD-001 (Consensus provider agreement); BR-EVD-003 (Cross-provider consensus agreement status); BR-SHR-007 (Consensus curve policy inputs); BR-EVD-014 (EPS gap explained by an unadjusted stock split)

## EpsTrend / EpsRevisionCounts

Source: `worker/src/evidence.ts:230`

| Field | Type | Note |
|---|---|---|
| `current` | `number \| null` |  |
| `d7\|d30\|d60\|d90` | `number \| null` |  |
| `up7d\|down7d\|up30d\|down30d` | `number \| null` | EpsRevisionCounts at evidence.ts:238 |

**Used by rules:** BR-EVD-018 (EPS revision windows); BR-EVD-016 (EPS revision window state); BR-SHR-014 (EPS estimate revision percentages and momentum flags); BR-SHR-013 (Earnings momentum: EPS revision percentages and flags)

## StaleFallback

Source: `worker/src/evidence.ts:130`

| Field | Type | Note |
|---|---|---|
| `liveStatus` | `string` | one of STALE_FALLBACK_STATUSES (evidence.ts:128) |
| `liveMessage` | `string \| null` |  |
| `ageHours` | `number` | window STALE_FALLBACK_MAX_AGE_DAYS=7 (evidence.ts:127) |
| `snapshotKey` | `string` | provider-snapshots/{provider}/{op}/{TICKER}/{date}.json |

**Used by rules:** BR-EVD-037 (Stale provider snapshot fallback); BR-EVD-039 (Stale provider snapshot fallback window); BR-EVD-038 (Stale provider snapshot fallback eligibility)

## SplitEvent / SplitHistory

Source: `worker/src/evidence.ts:267`

| Field | Type | Note |
|---|---|---|
| `date` | `string` |  |
| `ratio` | `number` | new per old share |
| `SplitHistory.status` | `string` |  |
| `SplitHistory.splits` | `SplitEvent[]` |  |
| `SplitHistory.message` | `string \| null?` |  |

**Used by rules:** BR-EVD-014 (EPS gap explained by an unadjusted stock split); BR-VAL-028 (Point-in-time price with later splits undone)

## ConsensusPolicy

Source: `worker/src/evidence.ts:464`

| Field | Type | Note |
|---|---|---|
| `horizonYears` | `number` | default 5 |
| `minAnalystCount` | `number` | default 3 |
| `conflictTolerancePct` | `number` | default 10 |
| `epsAbsoluteTolerance` | `number` | default 0.02 |

**Used by rules:** BR-SHR-007 (Consensus curve policy inputs); BR-SHR-008 (Consensus forecast policy bounds); BR-SHR-009 (Consensus policy input validation); BR-EVD-002 (Consensus provider agreement classification)

## FilingRow

Source: `worker/src/evidence.ts:918`

| Field | Type | Note |
|---|---|---|
| `form` | `string` |  |
| `filingDate` | `string` |  |
| `reportDate` | `string \| null` |  |
| `items` | `string \| null` |  |
| `isInlineXBRL` | `boolean \| null` |  |

**Used by rules:** BR-EVD-022 (Evidence quality freshness thresholds); BR-EVD-026 (Evidence quality: SEC periodic filing staleness by cadence); BR-EVD-028 (Evidence-family readiness and staleness thresholds); BR-EVD-024 (Evidence quality: guidance and consensus readiness)

## ComponentRecord

Source: `worker/src/evidence.ts:1067`

| Field | Type | Note |
|---|---|---|
| `status` | `string` |  |
| `sourceTool` | `string` |  |
| `retrievedAt` | `string \| null` |  |
| `data` | `unknown` |  |
| `warnings` | `unknown[]` |  |
| `error` | `{code,message} \| null` |  |

**Used by rules:** BR-EVD-019 (Evidence cut component status and coverage state); BR-EVD-004 (Evidence cut integrity verification); BR-EVD-005 (Evidence storage write-once lifecycle); BR-EVD-041 (Evidence cut listing order)

## EvidenceStore (interface) / StorageStatus

Source: `worker/src/evidence-store.ts:12`

| Field | Type | Note |
|---|---|---|
| `kind` | `string` |  |
| `get` | `(key) => Promise<string\|null>` |  |
| `exists` | `(key) => Promise<boolean>` |  |
| `put` | `(key, body, metadata) => Promise<void>` |  |
| `list` | `(prefix, limit) => Promise<string[]>` |  |
| `StorageStatus` | `STORED \| ALREADY_STORED \| UNAVAILABLE \| FAILED \| SKIPPED (evidence-store.ts:20)` |  |

**Used by rules:** BR-EVD-005 (Evidence storage write-once lifecycle); BR-EVD-041 (Evidence cut listing order); BR-EVD-037 (Stale provider snapshot fallback)

## MarketQuote (evidence pack)

Source: `worker/src/evidence-pack.ts:58`

| Field | Type | Note |
|---|---|---|
| `price` | `number \| null` |  |
| `currency` | `string \| null` |  |
| `priceTime` | `string \| null` |  |
| `status` | `string` |  |
| `message` | `string \| null` |  |

**Used by rules:** BR-EVD-025 (Evidence quality: quote freshness state); BR-PRC-045 (Quote freshness classification)

## AnnualFilingRef

Source: `worker/src/companyfacts-coverage.ts:12`

| Field | Type | Note |
|---|---|---|
| `accessionNumber` | `string` |  |
| `form` | `string` |  |
| `filed` | `string` |  |
| `reportDate` | `string \| null?` |  |

**Used by rules:** BR-SEC-023 (Latest annual report missing from SEC companyfacts)

## FiscalCalendar / FiscalYearNaming

Source: `worker/src/fiscal-calendar.ts:135`

| Field | Type | Note |
|---|---|---|
| `patterns` | `FiscalCalendarPattern[]` | MONTH_END \| WEEKDAY_NEAREST_MONTH_END \| LAST_WEEKDAY_OF_MONTH (fiscal-calendar.ts:134) |
| `month` | `number` |  |
| `weekday` | `string \| null` |  |
| `basis` | `string` |  |
| `periodEnds` | `string[]` |  |
| `FiscalYearNaming.offset` | `number` | fiscal-calendar.ts:89 |
| `FiscalYearNaming.periodEnd` | `string \| null` |  |
| `FiscalYearNaming.statedFiscalYear` | `number \| null` |  |
| `FiscalYearNaming.calendar` | `FiscalCalendar \| null` |  |

**Used by rules:** BR-SEC-022 (Fiscal year of a 52/53-week period end); BR-SEC-033 (Fiscal quarter from distance to year end); BR-SEC-032 (Fiscal calendar rule detection and projection); BR-SEC-010 (Company fiscal-year naming offset); BR-ERN-007 (Company fiscal-year naming applied to end-date-named guidance)

## EarningsPeriodInfo

Source: `worker/src/earnings-period.ts:10`

| Field | Type | Note |
|---|---|---|
| `period` | `string \| null` |  |
| `periodStatus` | `"EX99_TEXT_RESOLVED" \| "UNRESOLVED"` |  |
| `periodEvidence` | `string \| null` |  |

**Used by rules:** BR-ERN-012 (Earnings release fiscal period extraction); BR-ERN-011 (Earnings period alignment between release and Yahoo estimate row); BR-ERN-008 (Earnings actual-vs-estimate period alignment)

## ConceptFacts / FilingPeriod

Source: `worker/src/sec-facts.ts:19`

| Field | Type | Note |
|---|---|---|
| `concept` | `string` |  |
| `facts` | `Record<string,unknown>[]` |  |
| `FilingPeriod` | `{kind:'latest'} \| {kind:'fiscalYear', year:number} (sec-facts.ts:104)` |  |

**Used by rules:** BR-SEC-004 (Filing period must be 'latest' or a fiscal year); BR-SEC-007 (Fiscal-year assignment for SEC facts (52/53-week years)); BR-VAL-071 (SEC filings read only for USD listings)

## ConcentrationFinding

Source: `worker/src/extraction-rules.ts:49`

| Field | Type | Note |
|---|---|---|
| `kind` | `"customer" \| "aggregate"` |  |
| `name` | `string \| null` |  |
| `description` | `string` |  |
| `valuePct` | `number` |  |
| `year` | `number \| null` |  |
| `sectionHeading` | `string \| null` |  |
| `sentence` | `string` |  |

**Used by rules:** BR-EXT-054 (Customer revenue concentration extraction); BR-EXT-052 (Customer concentration latest-year, table-precedence and dedupe)

## RangeMatch

Source: `worker/src/extraction-rules.ts:322`

| Field | Type | Note |
|---|---|---|
| `excerpt` | `string` |  |
| `low` | `string` |  |
| `high` | `string` |  |
| `basis` | `NON_GAAP \| GAAP \| GAAP_AND_NON_GAAP \| NOT_STATED` |  |
| `statedAs` | `RANGE \| MIDPOINT_PLUS_MINUS \| OUTLOOK_ROW \| POINT_ESTIMATE` |  |
| `alternates` | `RangeMatch (without alternates)[]` |  |

**Used by rules:** BR-EXT-074 (Midpoint ± tolerance guidance bounds); BR-EXT-073 (Midpoint plus/minus tolerance bounds); BR-EXT-069 (Guidance pattern precedence and GAAP basis); BR-EXT-071 (Guidance range reading precedence and midpoint-plus-minus bounds); BR-EXT-070 (Guidance range pattern precedence and alternates); BR-EXT-068 (Guidance basis (GAAP / non-GAAP) and alternates); BR-EXT-010 (Guidance basis (GAAP / non-GAAP) classification); BR-EXT-014 (Outlook table scale for unscaled amounts)

## ReleaseMetricHit

Source: `worker/src/extraction-rules.ts:466`

| Field | Type | Note |
|---|---|---|
| `value` | `number` |  |
| `rawValue` | `string` |  |
| `scaleBasis` | `string \| null` |  |
| `sentence` | `string` |  |

**Used by rules:** BR-EXT-016 (Reported release metric acceptance rules); BR-EXT-017 (Reported release metric eligibility); BR-EXT-015 (Release figure scale and loss sign); BR-ERN-034 (Text-extracted release metrics are low confidence)

## ReleaseText

Source: `worker/src/guidance-history.ts:21`

| Field | Type | Note |
|---|---|---|
| `filingDate` | `string` |  |
| `accessionNumber` | `string` |  |
| `url` | `string \| null` |  |
| `status` | `string` |  |
| `text` | `string \| null` |  |

**Used by rules:** BR-ERN-023 (Guidance revision classification); BR-ERN-021 (Guidance outcome evaluation status); BR-ERN-003 (Guidance amount scaling); BR-ERN-019 (Guidance amount parsing with shared unit); BR-ERN-025 (Guidance target period resolution); BR-ERN-024 (Guidance target period from text); BR-ERN-027 (Guidance withdrawal and reaffirmation detection); BR-ERN-029 (Reported actuals labelled by period for guidance outcomes)

## Statement / PlainFact (funding schedule)

Source: `worker/src/funding-schedule.ts:26`

| Field | Type | Note |
|---|---|---|
| `PlainFact.local` | `string` |  |
| `PlainFact.value` | `number \| null` |  |
| `PlainFact.periodEnd` | `string \| null` |  |
| `PlainFact.dims` | `Record<string,string>` |  |
| `PlainFact.unit` | `string \| null` |  |
| `Statement.contextText\|sectionHeading\|documentUrl\|filingDate\|accessionNumber` | `string / string\|null` | funding-schedule.ts:34 |

**Used by rules:** BR-EXT-110 (Contractual obligation bucket dates); BR-EXT-066 (Funding/capex sentence classification precedence); BR-EXT-062 (Funding text amount parsing and range rule)

## Amount (funding schedule, internal)

Source: `worker/src/funding-schedule.ts:156`

| Field | Type | Note |
|---|---|---|
| `low` | `number \| null` |  |
| `high` | `number \| null` |  |
| `qualifier` | `string \| null` |  |
| `amountStatus` | `STATED \| SCALE_NOT_STATED \| NOT_STATED` |  |
| `asWritten` | `string \| null` |  |

**Used by rules:** BR-EXT-062 (Funding text amount parsing and range rule)

## DriverLedgerInput / InlineFact / DriverStatement

Source: `worker/src/driver-ledger.ts:301`

| Field | Type | Note |
|---|---|---|
| `ticker` | `string` |  |
| `companyfacts` | `unknown` |  |
| `inlineFacts` | `InlineFact[] \| null` | InlineFact {name,unit,value,periodStart,periodEnd,dims} at driver-ledger.ts:105; null = filing not read |
| `filing` | `Rec \| null` |  |
| `release` | `Rec \| null` |  |
| `statements` | `DriverStatement[]` | {contextText,sectionHeading,documentUrl,filingDate,accessionNumber,source} at driver-ledger.ts:249 |

**Used by rules:** BR-EXT-119 (Operating driver period classification and text-driver basis); BR-EXT-120 (Operating driver period type); BR-EXT-080 (Reporting period type from period length); BR-EXT-093 (Standard driver series status (read vs not reported)); BR-EXT-094 (Standard driver series status (REPORTED / NOT_REPORTED / NOT_READ)); BR-EXT-109 (Company-specific XBRL driver exclusion); BR-EXT-043 (Company-specific driver concept exclusion); BR-EXT-108 (Company-specific operating series exclusions); BR-EXT-096 (Text operating driver selection and actual-vs-plan basis); BR-EXT-078 (Operating statement basis: actual vs target); BR-EXT-121 (Operating text driver eligibility and figure detection)

## Filing search records (FilingTextProjection, SearchDocument, SearchMatch, Hit, TermStat, SearchSpec)

Source: `worker/src/filing-search.ts:698`

| Field | Type | Note |
|---|---|---|
| `SearchDocument.key\|documentType\|defaultSection\|projection\|scopeStart\|scopeEnd` | `string / string\|null / FilingTextProjection / number` |  |
| `SearchMatch.doc` | `SearchDocument` | filing-search.ts:708 |
| `SearchMatch.terms` | `string[]` |  |
| `SearchMatch.hitCount` | `number` |  |
| `SearchMatch.start` | `number` |  |
| `SearchMatch.context` | `MatchContext` |  |
| `SearchMatch.section` | `string \| null` |  |
| `SearchMatch.score` | `number` |  |
| `Hit` | `{label,terms[],start,end} (filing-search.ts:385)` |  |
| `TermStat` | `{term,hitCount,hitCountCapped,matchCount,returnedCount} (filing-search.ts:718)` |  |
| `SearchSpec` | `{terms,near,exclude,mode,budget} (filing-search.ts:733)` |  |

**Used by rules:** BR-SEC-030 (Filing search relevance score); BR-SEC-031 (Filing search relevance score and round-robin ordering); BR-SEC-029 (Filing search match relevance score and ordering); BR-SEC-034 (Table-of-contents hits excluded); BR-EXT-111 (Event evidence ranking and word stemming); BR-EXT-112 (Evidence ranking order)

## FundIdentity / Resolution / NavBatchFund (Thai SEC)

Source: `worker/src/sec-thailand.ts:34`

| Field | Type | Note |
|---|---|---|
| `fundClassName` | `string` |  |
| `projId` | `string` |  |
| `uniqueId` | `string \| null` |  |
| `companyNameTh\|companyNameEn` | `string \| null` |  |
| `projectNameTh\|projectNameEn` | `string \| null` |  |
| `requestedFundClassName` | `string \| null?` |  |
| `Resolution` | `{status, identity?, candidates?, message?} (sec-thailand.ts:45; Python yfmcp/tools/thai_funds.py:121)` |  |
| `NavBatchFund` | `{reference, fundClassName, projId} (sec-thailand.ts:587)` |  |
| `ProviderError` | `{code,message,recoveryAction,diagnostics?} (sec-thailand.ts:22)` |  |

**Used by rules:** BR-THA-003 (Thai fund share-class resolution and NAV selection); BR-THA-005 (Thai SEC fund share-class resolution); BR-THA-001 (Thai explicit-project NAV identity status); BR-THA-004 (Thai NAV batch completeness); BR-THA-002 (Thai fund NAV request bounds); BR-THA-006 (Thai factsheet section status); BR-THA-007 (Thai fund catalogue search); BR-PRV-011 (Provider failure recovery action mapping)

## McpResponse / ToolMeta / ErrorDetail (response envelope)

Source: `worker/src/response.ts:66`

| Field | Type | Note |
|---|---|---|
| `ok` | `boolean` |  |
| `data` | `T \| null` |  |
| `meta` | `ToolMeta` | tool, source, dataDate, serverVersion, cacheHit, warnings[], partialSuccess?, successCount?, errorCount?, decisionGrade?, capabilityStatus?, failureMode?, sourceType? ... (response.ts:38) |
| `error` | `ErrorDetail {code,message} \| null` | Python mirror yfmcp/envelope.py:52-70 |

**Used by rules:** BR-ENV-001 (Default fact confidence and decision-grade tagging); BR-ENV-009 (Error message classification for retry); BR-PRV-005 (Provider failure status mapping); BR-SHR-006 (Batch ticker cap of 5 (Worker only))

## LimitResult

Source: `worker/src/yahoo-finance.ts:549`

| Field | Type | Note |
|---|---|---|
| `tickers` | `string[]` |  |
| `truncatedFrom` | `number?` |  |
| `dropped` | `string[]?` |  |

**Used by rules:** BR-SHR-006 (Batch ticker cap of 5 (Worker only))

## DataQuality (options chain)

Source: `worker/src/yahoo-finance.ts:600`

| Field | Type | Note |
|---|---|---|
| `zeroBidAskCount` | `number` |  |
| `zeroOpenInterestCount` | `number` |  |
| `placeholderIvCount` | `number` | PLACEHOLDER_IV_THRESHOLD 0.10 (yahoo-finance.ts:598) |
| `staleLastTradeCount` | `number` |  |
| `returnedContracts` | `number` |  |
| `overall` | `HIGH \| MEDIUM \| LOW \| PARTIAL` |  |
| `quality` | `HIGH \| MEDIUM \| LOW` |  |
| `volumeQuality` | `OK \| UNAVAILABLE` |  |
| `oiQuality` | `OK \| STALE \| UNAVAILABLE` |  |
| `ivQuality` | `OK \| PARTIAL \| UNAVAILABLE` |  |
| `priceQuality` | `"OK"` |  |
| `warnings` | `string[]` |  |

**Used by rules:** BR-ENV-004 (Options chain data-quality grade); BR-ENV-003 (Option chain data quality grade); BR-PRC-028 (Put/call ratios and next-expiry selection); BR-PRC-016 (Options flow put/call ratio and max-pain strike)

## SegmentTableResult / SegmentTableRow

Source: `worker/src/yahoo-finance.ts:14712`

| Field | Type | Note |
|---|---|---|
| `segments` | `SegmentTableRow[] {label,rawValue,value}` |  |
| `total` | `SegmentTableRow` |  |
| `tableIndex` | `number` |  |
| `title` | `string \| null` |  |
| `column` | `string \| null` |  |
| `unitScale` | `thousands \| millions \| billions \| actual` |  |
| `unitMultiplier` | `number` |  |
| `unitScaleSource` | `UnitScaleSource` |  |

**Used by rules:** BR-EXT-014 (Outlook table scale for unscaled amounts)

## QuarterFact

Source: `worker/src/yahoo-finance.ts:17124`

| Field | Type | Note |
|---|---|---|
| `value` | `number` |  |
| `start` | `string` |  |
| `end` | `string` |  |
| `filed` | `string` |  |
| `form` | `string` |  |
| `method` | `DIRECT_QUARTER \| YTD_DIFFERENCE` | quarter length 80-100 days (yahoo-finance.ts:17135) |

**Used by rules:** BR-VAL-016 (LTM flow derivation); BR-EXT-080 (Reporting period type from period length); BR-SEC-033 (Fiscal quarter from distance to year end)

## YahooTranscriptResult / ProviderJsonResult (Python provider results)

Source: `yfmcp/clients/yahoo_transcripts.py:49`

| Field | Type | Note |
|---|---|---|
| `payload` | `dict \| None (object \| None for ProviderJsonResult)` |  |
| `status` | `str` |  |
| `source_url / public_url` | `str \| None / str` | ProviderJsonResult at yfmcp/clients/market_providers.py:31 |
| `event_id` | `str \| None` | transcript only |
| `provider_attempted` | `bool` |  |
| `cache_status` | `str` |  |
| `fetched_at` | `str \| None` |  |
| `http_status` | `int \| None` |  |
| `message` | `str \| None` |  |
| `retry_after` | `str \| None` |  |
| `content_diagnostics` | `dict \| None` | transcript only |

**Used by rules:** BR-ERN-037 (Yahoo transcript completeness classification); BR-ERN-038 (Yahoo transcript identity check); BR-ERN-043 (Yahoo transcript HTTP status mapping differs from market providers); BR-ERN-009 (Earnings call transcript source waterfall); BR-ERN-042 (Transcript source fallback order); BR-PRV-006 (Provider response identity check); BR-PRV-005 (Provider failure status mapping); BR-PRV-007 (Recommended next action per provider status); BR-PRV-004 (Historical put/call ratio request and response validation); BR-PRV-009 (Expanded ownership truncation and freshness)

