# Q3i triage: untagged §3 defects and R4 triage inputs

Scope:
- (A) the 20 rows in `BUSINESS_RULES.md` §3 whose Decision is exactly "Q3i triage";
- (B) the R4 triage inputs in `MODERNIZATION_BRIEF.md` (AAOI evidence, ticker `&`, accession readers, outline shape, Python outputSchema, telemetry).

Checked against the code at HEAD `a176314` (2.13.1). I made no repo edits.

Rows already decided as "Q3i: repair before …" are not triaged here. There are 22 of them: 18 "before its Phase 5 unit", 3 "before Phase 4" and 1 "before Phase 3".

None of the 20 rows has its own parity case. No case was flipped to `equal` in 2.12.0 or 2.13.0, so I skipped none. Three NWS rows overlap pinned news FINDING cases, noted on each row.

**"Parity" labels in §3 that are now inaccurate.** §3 marks BR-ENV-001 and BR-PRC-024 as **parity** (meaning the runtimes disagree), but both runtimes now behave identically on both.

**One-sided defects I found that §3 does not state:**
- BR-PRC-046 (PCX is in Python only);
- BR-ERN-010 (the Worker has XBRL/iXBRL paths that assign HIGH; Python has none);
- BR-ERN-039 (the Worker gives ex-99.1 HIGH, Python gives MEDIUM);
- BR-NWS-008 (Python matches `ASE` as a substring, the Worker exactly).

---

## A. Untagged §3 defects (20 rows)

### Group A1: a populated fact with no confidence defaults to HIGH
Root cause: when no confidence is given, a value-bearing fact falls back to HIGH.

Fix: pick one non-HIGH default and use it in both places (the envelope enricher and the 20-F fallback). Under ENV-001 the change affects every untagged fact in every tool, so the default value is a planner choice. EXT-035 can be fixed on its own without waiting for that choice.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| BR-ENV-001 | yfmcp/envelope.py:231-236; worker/src/response.ts:170-176 | Default fact confidence | value/low/valueRatio non-null and no confidence (own or parent) gives HIGH | identical (shared defect; the §3 "parity" label is stale) | confidence | **PLANNER-DECISION**. Options: (a) default LOW; (b) default MEDIUM; (c) default NOT_DECISION_GRADE (safest); (d) keep HIGH and instead tag every producer explicitly. Every option changes `confidence` on many untagged Yahoo/pricing facts, so an audit of the affected tools comes first. | It overstates confidence, but the safer default changes output across all tools and is a threshold-like choice | L | R4 |
| BR-EXT-035 | server.py:6441; worker/src/yahoo-finance.ts:11340 | 20-F fallback confidence | `_nn(fallback.confidence, "HIGH")` | `fallback.confidence ?? "HIGH"` (shared) | confidence | **REPAIR**: default to the primary path's `NOT_DISCLOSED`, with the same fallback in both runtimes | The fallback must not claim more than the primary path. This only happens when a 20-F payload has a value but no confidence (rare, since the producers set one). | S | R5-extractors |

### Group A2: options/pricing

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| BR-PRC-017 | server.py:4774-4778; worker/src/yahoo-finance.ts:7672-7677 | Options flow deprecated aliases | emits `ivPctile`/`putVolVs10dAvg` with the same values as the new fields; `fieldNotes` say the names misdescribe | identical | text | **PLANNER-DECISION**. Options: (a) remove now, since `docs/provider-runtime-guidance.md:375` promised "remain one release" and that window has passed; (b) add a `DEPRECATED_FIELD` notice and remove in Phase 6 with the other deprecated fields; (c) document and keep. | Removing a field is an output-field change. The values are already correct and disclosed. | S | R5-pricing |
| BR-PRC-024 | server.py:4089-4093; worker/src/yahoo-finance.ts:4622-4624 | Put hedge: skip expiries under 7 days | if every expiry is under 7 days away, uses all expiries, with no flag | identical (shared; the §3 "parity" label is stale) | coverage | **PLANNER-DECISION**. Options: (a) fail closed with the existing "No qualifying expiry dates" error (no new code); (b) keep the fallback and add a warning, which needs a new warning code. | The screen silently returns the sub-week contracts it says it skips. Either fix changes behaviour, and (b) adds a code. | S | R5-pricing |
| BR-PRC-040 | yfmcp/tools/pricing.py:1849-1855; worker/src/yahoo-finance.ts:8226-8230 | Market snapshot changePct | `price_stats.pct or recompute`, so a true 0.0 falls through to recomputation from the *quote* lastPrice (2dp) | `??`, so 0 is kept | figure (edge) | **REPAIR → Worker** (`is not None` in Python) | Python can mix two sources: if the history-based pct is 0 but the quote price moved, Python publishes a non-zero recomputed figure | S | R5-pricing |
| BR-PRC-046 | server.py:1931 (search_ticker); worker/src/yahoo-finance.ts:3235 (searchTicker) | `exchange="US"` filter | {NMS, NYQ, PCX} | {NMS, NYQ}: **one-sided divergence**, not in §3. NGM/NCM/ASE are missing in both. | coverage / input-handling | **PLANNER-DECISION**. Options: (a) both use {NMS, NYQ, PCX, NGM, NCM, ASE} (full US listings); (b) both {NMS, NYQ, PCX} (Python); (c) both {NMS, NYQ} (Worker, narrowest). | The allow-list is a coverage policy, and the runtimes also differ from each other | S | R5-pricing |

### Group A3: extractors

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| BR-EXT-063 | yfmcp/funding_schedule.py:230,240-242; worker/src/funding-schedule.ts:243,253-255 | Funding text item, SCALE_NOT_STATED | keeps the wording classification (e.g. COMMITTED) with null low/high, `amountStatus: SCALE_NOT_STATED`, currency "USD" | identical | status | **PLANNER-DECISION**. Options: (a) force UNRESOLVED, which needs a new `unresolvedReason` such as "scale not stated"; (b) document as intentional, since the payload note says classification is by wording and `amountStatus` plus null amounts already disclose it. | Option (a) adds an unresolvedReason value | S | R5-extractors |
| BR-EXT-084 | yfmcp/share_scenarios.py:183-185; worker/src/share-scenarios.ts:187-189 | Warrant vesting basis, vested_only | missing `exercisable` key falls back to outstanding | identical | figure | **REPAIR**: under vested_only, treat a missing key like null, giving `count None` and the existing "count not tagged" unresolved line | It contradicts the code comment (2.5.9) and overstates dilution basis. The capital-structure producer always emits the key (capital_structure.py:1066), so today this is latent. | S | R5-extractors |

### Group A4: valuation / metric reconciliation

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| BR-VAL-064 | yfmcp/metric_reconciliation.py:490-499; worker/src/metric-reconciliation.ts:441-450 | Release figure selection | takes `found[-1]` (newest re-release); docstring says "first" | identical (both docstrings are wrong) | figure | **PLANNER-DECISION** (SME). Options: (a) keep newest and fix both docstrings (PRESERVE, document); (b) switch to first/as-originally-reported (REPAIR, a figure change). | Neither choice overstates evidence; it is a source policy | S | R5-valuation |
| BR-VAL-068 | yfmcp/metric_reconciliation.py:160,355; worker/src/metric-reconciliation.ts:148,317 | `Q<n> <yyyy>` by calendar quarter of the period end | `ceil(month/3)`, so a 52/53-week quarter ending Jan 1-3 counts as Q1 of the next year | identical | status / figure | **REPAIR → 7-day drift rule** from fiscal_calendar.py, in both the period selection and the sentence-scope pairs | Today a Q4 request gets PERIOD_NOT_FOUND, and worse, a "first quarter <next year>" release sentence can be scoped to the Oct–Dec quarter, which is a wrong-period figure | M | R5-valuation |

### Group A5: earnings (Python behind the Worker)
Root cause: Python's `extract_earnings_metrics` and its commentary path did not get the Worker's later SEC changes.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| BR-ERN-010 | yfmcp/tools/earnings.py:385-480 (HIGH branch 473-474); worker/src/yahoo-finance.ts:~13999-14090 | Earnings metrics confidence | text-only (EX-99 prose gives LOW); no XBRL path, so overall HIGH is unreachable | has companyfacts XBRL and EX-99 iXBRL paths that set HIGH metrics, so overall HIGH is reachable. **One-sided divergence, not pinned** (no parity case covers extract_earnings_metrics). Warning messages also differ. | confidence / figure | **PLANNER-DECISION**. Options: (a) port the Worker's XBRL/iXBRL paths to Python (Python gains figures); (b) cut the Worker down to text-only (safest, but loses decision-grade figures); (c) PRESERVE as a documented divergence. | Not dead code: the behaviour differs between runtimes, and resolving it is a behaviour choice | L | R5-earnings |
| BR-ERN-039 | yfmcp/tools/earnings.py:644; worker/src/yahoo-finance.ts:14303 | Management commentary confidence | `!= "sec_8k"` makes `sec_8k_ex991` MEDIUM | `startsWith("sec_8k")` makes ex991 HIGH. **One-sided divergence.** | confidence | **REPAIR → MEDIUM for ex991** (Python behaviour; the Worker aligns) | The safer choice under A2: the excerpt is a first matching sentence. If the planner prefers the documented intent (SEC release = HIGH), that is a planner call. | S | R5-earnings |

### Group A6: news (R-news)
All three rules overlap news cases that are already pinned. They should be triaged together with the news FINDING inventory so nothing is counted twice.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| BR-NWS-002 | yfmcp/tools/news.py:3127; worker/src/news.ts:2906 | Verification term threshold | `ceil(n*0.67)` | identical | status | **PRESERVE (document)** | 0.67 is stricter than 2/3, which is the safer side. Changing it would be a threshold change. Fix the stated intent in the docs. Pinned "Card 138" exercises it. | S | R-news |
| BR-NWS-008 | yfmcp/tools/news.py:1202-1212; worker/src/news.ts:1692-1700 | Exchange normalisation | NYSE tested before AMEX/NYSE American; `"ASE" in exch` substring; `"GS"` substring maps to NASDAQ | same order; `e === "ASE"` exact (divergence) | status (identity accept/reject) | **REPAIR**: test NYSE AMERICAN/AMEX/ASE (exact) before NYSE; `GS` exact, not substring | It wrongly accepts a "NYSE:" mention for an NYSE American issuer, which overstates identity match. NYSEAMERICAN already exists, so no new value is needed. Already pinned as a Q7 FINDING in cases_news_a.py:906 and cases_news_b.py:464,1844. | S | R-news |
| BR-NWS-010 | yfmcp/tools/news.py:455-477; worker/src/news.ts:557-569 | Event type by substring keywords | substring; extra keywords 'annual report', 'credit facility', 'announces new' | regex substring; fewer keywords | status (eventType, decisionUse) | **PLANNER-DECISION**. Options: (a) word-boundary matching plus one keyword list (choose union or Worker's); (b) document. | A misclassification moves decisionUse between CHECK_OFFICIAL_RELEASES and CONTEXT_ONLY. Neither direction overstates evidence, so there is no clearly safer side. The list divergence is already pinned (cases_news_a.py:519, cases_news_b.py:2443). | M | R-news |

### Group A7: evidence store / evidence cut (shared)

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| BR-EVD-020 | yfmcp/evidence.py:1191,1208; worker/src/evidence.ts:1198 | LIMITED status regex | substring regex | identical | status | **PRESERVE (document)** | Over-matching only pushes toward LIMITED, which is the safer side. Note for owners: a status like `PROVIDER_NOT_COVERED` does *not* match. | S | R4 |
| BR-EVD-041 | yfmcp/evidence_store.py:62-70,88-90; worker/src/evidence-store.ts:66-75; callers server.py:8941,9003,9186 / worker/src/evidence-pack.ts:220,274,460 | Listing order newest-first | sorts ascending, then truncates to limit (1000 cuts / 400 days) | R2 lists lexicographically ascending, stops at limit (same result) | coverage | **REPAIR**: list fully (page through), then keep the *newest* `limit` keys, in both stores | Beyond the cap, the newest cuts and history days disappear while the listing claims newest-first. Latent until 1000 cuts or 400 days. | M | R4 |

### Group A8: analyst strength flag asymmetry
Root cause: `DOWNGRADE` is tested before `mixedSignal`. Fix: `DOWNGRADE && !mixed` gives BEARISH, and mixed gives MIXED for both upgrades and downgrades. MIXED is an existing enum value.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| BR-SHR-003 | server.py:4333-4343; worker/src/yahoo-finance.ts:4859-4864 | Price-target mixed signal | a downgrade with a raised target is BEARISH (`mixedSignal: true`) | identical | status | **REPAIR → MIXED** | The flag contradicts its own `mixedSignal: true` and overstates the bearish read | S | R4 |
| BR-SHR-004 | same | Analyst radar strength precedence | same root cause | identical | status | **REPAIR** (same fix) | duplicate card of SHR-003 | — | R4 |

---

## B. R4 triage inputs

### B1. Evidence standardisation drops table evidence
Root cause: both enrichers (yfmcp/envelope.py:267-309, worker/src/response.ts:207-245) keep a raw evidence row only if one of their known keys is set. Table evidence objects carry `sectionHeading/sourceTableId/sourceRows/sourceColumns` and no `url`/`documentUrl`, so the whole row is dropped and `evidence` becomes null. **This is shared, not Worker-only**: Python drops the row the same way. The brief cites `response.ts` only.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| R4-AAOI-EVIDENCE | producers: server.py:2858-2864 / worker/src/yahoo-finance.ts:6684-6690 (`extract_sec_filing_fact` geo PARSED_TABLE); enrichers above | Positive figure must carry evidence | `confidence: HIGH` figure with `evidence: null` | identical | coverage | **REPAIR**. Map table evidence into the existing standardised row: `url←documentUrl` (add documentUrl/filingType/accessionNumber/filingDate to the producer's evidence object), `tableIndex←sourceTableId`, `rowLabel/rawRow←sourceRows[0]`. Do not relax the smoke check. | A HIGH, decision-feeding figure with no evidence row overstates what can be verified. Uses existing row fields only, so no new field. `extract_geographic_revenue` already passes because its evidence carries `documentUrl`. | S–M | R4 |
| NEW-EVIDENCE-ROW-SHAPE (found during this triage) | yfmcp/envelope.py:273-288 vs worker/src/response.ts:211-220 | Standardised evidence row keys | 14 keys: adds `sourceType, publishedAt, retrievedAt, excerpt, concept, periodEnd` | 8 keys; drops excerpt/publishedAt/sourceType from every evidence row (e.g. earnings text evidence) | coverage / text | **PLANNER-DECISION**. Options: (a) the Worker adds the 6 keys (new output fields in the Worker); (b) Python drops them; (c) document. | Not pinned (the parity views select around it). Option (a) adds fields and (b) removes them. | S | R4 |

### B2. Ticker grammar `&`

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| R4-TICKER-AMP | yfmcp/validation.py:14; worker/src/validate.ts:1 | `^[A-Z0-9.\-\^=]{1,20}$` | `M&M.NS` gets INPUT_VALIDATION_ERROR | identical | input-handling | **PRESERVE (document)** today; admitting `&` is an enhancement | Rejecting fails closed, the safe side. Before admitting it, the URL audit must cover e.g. server.py:2026 (`browse-edgar?company={ticker}&…`, unencoded) and yfinance/RSS query builders. The Worker mostly uses `enc()`. | M (if admitted) | R4 / later |

### B3. Accession readers without the Q3a check
Both runtimes are identical here. One fix: honour the pin, and when an accession does not exist, fail closed with the existing `ACCESSION_NOT_FOUND`.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| R4-ACC-GEO | server.py:6360-6365; worker/src/yahoo-finance.ts:11267-11269 (`extract_geographic_revenue`); also the 20-F retry server.py:6423-6426 / ts:11322-11323 | Pinned accession | `get_filing_data(...)` is called **without** `accession_number`, so the figure comes from the latest filing. With `detailLevel=raw`, the evidence takes the accession/filingDate from the index of the *requested* accession. | identical (`getFilingData` has a `pinAccession` param that is not passed) | figure / coverage | **REPAIR**: pass the accession into get_filing_data, and return ACCESSION_NOT_FOUND on a miss | The figure and the evidence can name different filings, and the caller's pin is silently ignored. Note: `getFilingData`'s pinned geo path deliberately returns the legacy FILING_NOT_FOUND payload on a miss (ts:6620-6622, "not one 2.13.0 fails closed"). Which code a miss returns is a minor planner confirmation; both codes exist. | S–M | R4 |
| R4-ACC-QUERY | worker/src/yahoo-finance.ts:13286 (geo branch) / 13330 (china branch); Python mirror in server.py `query_sec_filing_index` | query_sec_filing_index | the geographic query inherits R4-ACC-GEO; the china query goes through `extract_china_exposure`, which already has the Q3a check (server.py:7981 / ts:12738) | identical | figure / coverage | **REPAIR**: fixed by R4-ACC-GEO, plus a parity case per query type | Same root cause | S | R4 |
| R4-ACC-EXHIBITS | yfmcp/tools/earnings.py:854-880 + yfmcp/clients/edgar.py:186-194; worker/src/yahoo-finance.ts:9750-9771, 9773+, 5203 | list_sec_filing_exhibits / get_sec_filing_exhibit_content | builds the index URL from the ticker's CIK and the accession; on a 404, `exhibits: []` comes back as a *success* | identical | coverage | **REPAIR**: ACCESSION_NOT_FOUND when the accession's filing index cannot be fetched (404) | An empty success claims the filing has no exhibits. Implementation note: do not reuse Q3a's recent-filings resolver as is, because it would reject legitimate older accessions outside the recent window. | S | R4 |

### B4. `get_sec_filing_outline` shape

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| R4-OUTLINE-SHAPE | server.py:905-929; worker/src/yahoo-finance.ts:8500-8540 | Outline output | `{ticker, accessionNumber, outline}`. No status. No `filingItemHeadings` fallback, so filings without `<hN>` return an empty outline. Sanitises HTML. Errors are `_mcp_failure` envelopes. | `{ticker, documentUrl, outline, status: OK/OUTLINE_NOT_PARSED/EMPTY, tableCount, warnings[TABLES_FOUND_OUTLINE_EMPTY]}`. Falls back to item headings. Errors are `{error:true,message}`. | status / coverage | **PLANNER-DECISION**. Recommend Python adopt the Worker's status/tableCount/warning and the heading fallback (the safer side: it discloses unparsed tables instead of a bare empty list). The planner chooses the key set (`accessionNumber` and/or `documentUrl`) and the error envelope. | It is a cross-runtime field/enum alignment. The parity `outline_view` (cases_accession.py:134) hides it. | M | R4 |

### B5. Python outputSchema

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| R4-PY-OUTPUTSCHEMA | yfmcp/app.py:137-163 (`structured_output` passed through; no schema); docs/provider-runtime-guidance.md:2876-2879 | Declared output schema | SDK auto-derived outputSchema, since the 97 declarations were removed in 2.13.1 | declared per tool (worker/src/tools.ts:124, 600-640) | text (contract) | **PLANNER-DECISION**. Options: (a) generate Python outputSchema from the Worker declarations, which changes Python's `tools/list` and schemaHash; (b) document as an intentional listing divergence. | No figure changes, but it is a contract/field change in the listing | M | R4 |

### B6. Telemetry follow-ups: **ops follow-up** (outside the repair/preserve classes, not counted below)

| id | file:line | item | Worker change? | note |
|---|---|---|---|---|
| OPS-TEL-1 | worker/src/telemetry.ts:159 | Dispatch line logged as `JSON.stringify(...)` string; logging an object would make it field-indexed | **Yes** (Worker code; `scripts/export_telemetry.py` already parses either form) | Low risk. Keep the same field set (action/domain/build only). |
| OPS-TEL-2 | scripts/export_telemetry.py | Coverage guard: flag when Analytics Engine counts requests but Workers Logs counts 0 | No (export script) | This would have caught the 2.13.0 zero-invocation read |
| OPS-TEL-3 | worker/wrangler.toml:44-51 | `[observability] enabled=false` alongside `[observability.logs] enabled=true` | **Yes** (config, needs a deploy) | Logs work today (45 read). Set the top-level flag consistently, or comment why it is off. |
| OPS-TEL-4 | scripts/export_telemetry.py:447-480 | 45 logged invocations vs 68 AE requests; 3 outcomes `unknown` | Probably no; maybe a Worker change if non-dispatch requests should log | Hypothesis (unverified): AE counts every request (initialize, tools/list, non-tool), while logs count dispatch lines only. `unknown` means no joined `cf-worker-event` and no own `$workers.outcome`. Investigate before the 2026-10-24 baseline. |

Brief R4 items **not** in this file's scope (left for whoever owns them):
- `get_company_news` reports FULL while a source is truncated;
- `tool_catalog.json` omits `sources` for `get_company_press_releases`;
- the RN deprecation wording notes.

---

## Totals
(28 rows: 20 from §3 and 8 R4 inputs. The 4 telemetry ops follow-ups are excluded.)

| | A (§3) | B (R4 inputs) | Total |
|---|---|---|---|
| Rows | 20 | 8 | **28** |
| REPAIR | 9 (EXT-035, PRC-040, EXT-084, VAL-068, ERN-039, NWS-008, EVD-041, SHR-003, SHR-004) | 4 (AAOI-EVIDENCE, ACC-GEO, ACC-QUERY, ACC-EXHIBITS) | **13** |
| PRESERVE align-free | 1 (ENV-008: dead `is_stale`, Python-only, no callers; delete) | 0 | **1** |
| PRESERVE document | 2 (NWS-002, EVD-020) | 1 (TICKER-AMP) | **3** |
| PLANNER-DECISION | 8 (ENV-001, PRC-017, PRC-024, PRC-046, EXT-063, VAL-064, ERN-010, NWS-010) | 3 (EVIDENCE-ROW-SHAPE, OUTLINE-SHAPE, PY-OUTPUTSCHEMA) | **11** |
| Ops follow-ups (separate) | — | 4 (2 need a Worker change: OPS-TEL-1, OPS-TEL-3) | 4 |

Not triaged (already decided): 22 "Q3i: repair …" rows (18 Phase 5 unit, 3 Phase 4, 1 Phase 3).
