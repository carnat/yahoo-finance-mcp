# Q3i triage table (R4, last item): for one owner approval

**Status:** APPROVED 2026-10-18 by the owner, with the R-news additions in §3a. The decisions are recorded in the brief's §7; R4b and R-news implement them.

**Method:** A2, accepted 2026-10-16.
- **Repair** a divergence or defect that changes a published figure, status, `decisionGrade`, confidence or coverage claim. Align both runtimes to the safer behaviour, meaning the one that never overstates evidence.
- **Preserve** text-only differences. Align them when that is free; otherwise document them as intentional.

**Inputs:** checked against 2.13.1 (`a176314`). Every pinned case still holds; none was flipped by 2.12.0–2.13.1.

| Inventory | Rows | Repair | Preserve, align-free | Preserve, document | Planner call |
|---|---:|---:|---:|---:|---:|
| `q3i/news_a.md` (33 pinned + 6 shared defects) | 39 | 23 | 7 | 2 | 7 |
| `q3i/news_b.md` (55 pinned + shared defects + 2 live inputs) | 66 (+1 moot) | 37 | 17 | 5 | 7 |
| `q3i/other_cases.md` (16 pinned + 2 RN wording notes) | 19 | 7 | 3 | 4 | 5 |
| `q3i/defects_and_inputs.md` (20 untagged §3 defects + 8 R4 inputs) | 28 | 13 | 1 | 3 | 11 |
| **Total** | **152** | **80** | **28** | **14** | **30** |

Several news rows share a root cause across the two news files, for example `company_ir`, the exchange normaliser, keyword matching, HTML-entity dedupe and the DOCTYPE guard. Each root cause is fixed once.

Not triaged here:
- **Already decided:** the 22 rows in BUSINESS_RULES §3 marked "Q3i: repair before …".
- **Ops follow-ups:** the 4 telemetry items.

Every row, with code sites, the Python and Worker values and the reasoning, is in the four appendix files under `q3i/`.

---

## 1. Planner rules used for the 30 planner calls

- **P1. Never overstate (A2).** Where one side overstates, the other side is the target. Where neither does, accuracy decides.
- **P2. A value that exists in either runtime's contract is not "new".** Making one runtime emit a field, warning code or enum value the other runtime already emits counts as alignment. Values that exist in neither runtime are avoided.
- **P3. Deprecated fields are not spread.** `doctrineUse`, `totalAnalysts`, `ivPctile` and `putVolVs10dAvg` are not added to the runtime that lacks them. They are documented until Phase 6 removes them.
- **P4. Existing codes come before new ones.** No new `failureMode`, status, warning code or `unresolvedReason` is introduced by this table.
- **P5. A repair doesn't add capability.** It doesn't widen inputs or acceptance.

## 2. Planner calls (30)

| # | Item | Decision | Rule |
|---|---|---|---|
| 1 | `analyze_position_signals` componentStatus / failedComponents | The Worker adopts Python's 6 component keys and `failedComponents`, and reports a failed analyst or earnings component as PARTIAL. Its 3 existing key names change, and the docs list the rename. | P1, P2 |
| 2 | `quoteTimestamp`, Python only | The Worker adds it from the quote's `priceTimestamp`. | P2 |
| 3 | `macd` in `technicalInputs`, Python only | The Worker adds it; the value is already computed. | P2 |
| 4 | List `ticker` in `analyze_position_signals` | The Worker rejects a list with `INPUT_VALIDATION_ERROR`, as its declared schema says. The pinned case moves to the MCP boundary. | P5 |
| 5 | Dated `estimatePeriod` | Normalise it to YYYY-MM-DD, and withhold `estimateRevenue` in both runtimes while alignment is UNVERIFIED, as the revenue surprise already is. | P1 |
| 6 | Event-type keywords (shared substring defect, list split; also BR-NWS-010) | Word-boundary matching and one keyword list, the Python superset, in both runtimes. | accuracy |
| 7 | `EX-99.01` | Accept `^EX-99\.0?1$` in both runtimes: it is the same exhibit number in different notation. | accuracy |
| 8 | Candidate-only press status | Python adopts `COMPANY_IR_PAGE_CANDIDATE_ONLY`, and the Worker adds it to `limitationStatuses`. | P2 |
| 9 | Press releases: an SEC outage reads as absence | Keep the status. Derive `failureMode` and coverage PARTIAL from the failed SEC row. | P4 |
| 10 | verify: `AUTH_ERROR` does not upgrade NOT_FOUND | Upgrade to SOURCE_LIMITED_NOT_FOUND with the existing `failureMode` `SOURCE_UNCONFIGURED`. | P1, P4 |
| 11 | verify: an old official release with no date range is CONFIRMED | Apply the 90-day STALE cutoff when no range is given. | P1 |
| 12 | verify: an all-stop-word query still matches | Reject it with `INPUT_VALIDATION_ERROR`. A query with no meaningful terms can verify nothing, and NOT_FOUND would claim absence. | P1, P4 |
| 13 | Yahoo summary in the Worker | Both runtimes carry the summary for display. Identity is matched on the title only. | P1, P5 |
| 14 | verify with an unregistered `company_ir` | Map NOT_REGISTERED to the existing `failureMode` `SOURCE_UNCONFIGURED`, giving SOURCE_LIMITED_NOT_FOUND. | P1, P4 |
| 15 | `get_sec_recent_events` coverage fields, Worker only | Python adds `coverage`, `sourceCoverage` and `sourceStatus`. | P2 |
| 16 | Valuation attribution aliases | Use the Worker's alias set in both runtimes (longName, shortName, companyName), excluding any alias equal to the ticker. | accuracy, P1 |
| 17 | IR page serving an RSS body to an `html_article` entry | SOURCE_VALIDATION_FAILED in both runtimes. | P1, P4 |
| 18 | Press releases: `doctrineUse` and `secEvidence[].cikInt` | `doctrineUse` stays Worker-only and is documented. Python adds `cikInt`. | P3, P2 |
| 19 | Coverage FULL while a source is truncated (live input) | Provider- or cap-side truncation (the pool cap, the Marketaux 3-row limit) makes coverage PARTIAL. Truncation to the caller's own `max_results` stays FULL. | P1 (BR-NWS-007) |
| 20 | IR-page `allowedAction` values | Use the values both runtimes already have. | P2 |
| 21 | BR-ENV-001: a fact with no confidence defaults to HIGH | First audit the producers and tag explicitly every fact that is genuinely HIGH. Then the default for anything still untagged becomes LOW, in both runtimes. This is large, so it ships as its own release, R5-shared, before the first Phase 5 unit. | P1 |
| 22 | BR-PRC-017: `ivPctile` / `putVolVs10dAvg` | Add a `DEPRECATED_FIELD` notice now and remove them in Phase 6. | P3 |
| 23 | BR-PRC-024: the hedge falls back to sub-week expiries | Fail closed with the existing "No qualifying expiry dates" error. | P1, P4 |
| 24 | BR-PRC-046: the `exchange="US"` filter | Both runtimes use {NMS, NGM, NCM, NYQ, ASE, PCX}. | accuracy |
| 25 | BR-EXT-063: SCALE_NOT_STATED keeps its classification | Document it. `amountStatus` and the null amounts already disclose it. | P4 |
| 26 | BR-VAL-064: newest or first release figure | Keep the newest (the restated, corrected figure) and fix both docstrings. *The owner may override this as a source-policy question.* | P1 |
| 27 | BR-ERN-010: the Worker's XBRL HIGH paths, Python text-only | Document it now; Python under-claims, which is safe. The Phase 5 earnings unit makes the Worker's paths the shared target, with parity tests. | P1 |
| 28 | Evidence row keys (Python 14, Worker 8) | The Worker adds the 6 keys Python has. | P2 |
| 29 | `get_sec_filing_outline` shape | Python adopts the Worker's `status`, `tableCount`, warning and heading fallback. Both runtimes emit both `accessionNumber` and `documentUrl`. Errors use the standard `ok:false` envelope in both. | P1, P2 |
| 30 | Python `outputSchema` | Document it now. It is regenerated from one catalog in Phase 5. Declaring it now would make clients validate against schemas that could drift. | P4 |

**Confirmations of agent-flagged borderline rows (applying P2):**
- Section markdown: Python adopts the Worker's status, confidence and warning. Repair.
- The Worker emits `COMPANY_IR_PAGE_CANDIDATE_AVAILABLE`. Repair.
- `max_results=0` / `lookback_days=0` clamp to 1.
- BR-ERN-039 ex-99.1 commentary becomes MEDIUM.
- An explicit accession that misses in `extract_geographic_revenue` or the exhibit tools returns `ACCESSION_NOT_FOUND`. The exhibit tools use a filing-index 404, not the recent-filings list.
- A numeric-string `tolerance_pct` falls back to the 0.5 default.

## 3. Release plan

| Release | Contents | Gates |
|---|---|---|
| **R4b** (before Phase 4: the hub and SEC/EDGAR surfaces Phase 4 moves) | the EX-99.1 resolver (shared with earnings: no bare EX-99 without release wording, no file-name-only match, accept `.01`); the XBRL decision-grade predicate (×2); section-markdown marking; AAOI table evidence mapped into the standard row, plus the Worker gaining the 6 evidence keys; accession readers (geographic, `query_sec_filing_index`, exhibits); outline shape; analyst MIXED (BR-SHR-003/004); evidence-store newest listing (BR-EVD-041); the stale Python status enum; the RN wording notes; telemetry ops 1–4 | Phase 4 |
| **R-news** (gates nothing) | every news row: the `company_ir` registry-only port to Python; Worker `getNews()` field pass-through; the exchange normaliser and the GlobeNewswire check; keyword matching; dedupe; the DOCTYPE guard; verify fixes (calls 9–14); coverage-while-truncated (call 19); the provider fail-closed policy; Form 4 fixes; warning-text alignment; the `tool_catalog.json` params, with the contract test tightened to catch omitted optional params | none |
| **R5-shared** (before the first Phase 5 unit) | BR-ENV-001 (call 21) | Phase 5 |
| **R5-‹unit›** | pricing: calls 1–4, 22–24, zero price, CAGR boundary, quoteDataDate, changePct. extractors: BR-EXT-035, BR-EXT-084. valuation: BR-VAL-068, `tolerance_pct`. earnings: call 5, BR-ERN-039, the `source_url` message | each unit's move |

### 3a. R-news additions (owner, 2026-10-18)

R-news ships in three parts, in this order:

1. **Provider adapters (pure restructure).**
   - Each news source moves to its own adapter file in both runtimes: `worker/src/news/providers/*.ts` and `yfmcp/news/providers/*.py`.
   - Every adapter returns standard items plus a source status.
   - The shared pipeline stays in one place: identity, dedupe, event typing, ranking and coverage.
   - Output must be byte-identical; the parity harness and the before/after tool output prove it.
2. **Source registry and operator off switch.**
   - **Registry:** a data file lists each source with its name, whether it is on by default, the key it needs, a rough subrequest cost and its tier (official or aggregator).
   - **Off switch:** the setting `NEWS_SOURCES_DISABLED` holds a comma list of source names. It is set as a Worker var in `wrangler.toml` and as an environment variable locally.
   - **What a disabled source does:**
     - it is never fetched and costs no subrequests;
     - its `sourceStatus` reads `SOURCE_UNCONFIGURED` with `providerAttempted: false`, plus a `SOURCE_UNAVAILABLE` warning saying the operator disabled it;
     - coverage reads PARTIAL, never NOT_FOUND;
     - an explicit request for it gets the same reporting.
   - **Planner decision on the status label:** reuse the existing `SOURCE_UNCONFIGURED` status and `SOURCE_UNAVAILABLE` warning, so no new status value is added (rule P4).
   - **Default sources (owner, 2026-10-18):** add `sec` to the `get_company_news` defaults. This ships in R-news, after the registry exists, and the default list lives in the registry.
3. **The Q3i news repairs** from §2, each landing in its own adapter or in the shared pipeline.

The pinned harness cases flip to `equal` as each fix lands. Preserve-document rows get a line in `docs/provider-runtime-guidance.md`.

## 4. Approval

```
Q3i triage — Approved by: repository owner (carnat), in session  Date: 2026-10-18
Owner's words, verbatim: "agree, add it to R-news and approve Q3i triage"
Overrides: none. Addition: R-news provider adapters, source registry and NEWS_SOURCES_DISABLED off switch (§3a).
```

## 5. Findings after approval (to be triaged at the next review, not yet scheduled)

These came up while implementing R4b. None is a regression.
- **F1. Public `get_sec_filing_outline` without `document_url`.**
  - The Worker routes the call to the index-backed outline (`secIndexOutlinePayload`: `filingType`, `filingDate`, items with `sectionId`).
  - Python resolves the latest primary document instead, and ignores `accession_number` when `period=latest`.
  - The 2.15.0 shape unification covers the document path only. Proposed home: the Phase 5 SEC-retrieval unit, which owns the index.
- **F2. Outline titles strip inner tags differently.** The Worker inserts a space ("Item 1") and Python inserts nothing ("Item1"). Text only.
- **F3. `query_sec_filing_index` china query with an older accession.** The evidence names the latest 10-K, because the revenue read is not pinned. Same class as R4-ACC-GEO.
- **F4. A pinned geographic read with no XBRL revenue fact of the requested form.** It returns `NO_FACT_FOR_ACCESSION` without reading that filing's table, so a 20-F accession asked for as `filing_type` 10-K never reaches the table read.
- **F5. `extract_sec_filing_fact` meta vs payload `decisionGrade`.**
  - `meta.decisionGrade` is a static capability flag (true), while the payload says false for a PARSED_TABLE figure. The tool note explains this, but the two read as contradictory.
  - Options: rename or retire the meta flag in Phase 6, or derive it per call.
- **F6. Telemetry.**
  - Analytics Engine counts about 15 preview-canary requests per deploy that Workers Logs never shows.
  - The Phase 4 baseline comparison should exclude them, by filtering out deploy-hour canary actions or by tagging the preview version.
