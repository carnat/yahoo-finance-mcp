# Triage of the post-approval findings (F1–F20) and the open ops items

**Status:** APPROVED 2026-10-19 (owner; §4). The planner calls in §3 carry recommendations; the owner approves the table as a whole, as with Q3i.
**Source:** `Q3I_TRIAGE.md` §5 (F1–F20) and `q3i/defects_and_inputs.md` (OPS-TEL-2…4).
**Rules:** the Q3i planner rules P1–P5 apply unchanged.
- P1: never overstate.
- P2: a value that exists in either runtime is not "new".
- P3: deprecated fields are not spread to the other runtime.
- P4: existing codes before new ones.
- P5: no new capability inside a repair.

**Already closed:**

| Finding | Closed by |
|---|---|
| F8, F9, the F11 main part | 2.20.0 |
| F15 | 2.20.1 |
| R4-TICKER-AMP | Q3i classified it document-only; it is not reopened here |
| OPS-TEL-1 | 2.14.0 |
| OPS-TEL-2 | 2.14.0 |

## 1. Classification

| # | Finding | Class | Target behaviour | Release |
|---|---|---|---|---|
| F1 | Python `get_sec_filing_outline` without `document_url` resolves the latest primary document, and ignores `accession_number` when `period=latest`. The Worker uses the index outline. | REPAIR → Worker | Python follows the Worker: the index-backed outline, honouring `accession_number`. Ignoring a pinned accession fails open (P1). | R5-sec |
| F2 | Outline titles: the Worker inserts a space when stripping inner tags ("Item 1"); Python inserts nothing ("Item1") | REPAIR → Worker (align-free text) | Python inserts the space | R5-sec |
| F3 | `query_sec_filing_index` with an older pinned accession: the evidence names the latest 10-K, because the revenue read is not pinned | REPAIR | The revenue read uses the pinned accession. Naming the wrong filing overstates provenance (P1). Both runtimes. | R5-extractors |
| F4 | A pinned geographic read with no XBRL revenue fact of the requested form returns `NO_FACT_FOR_ACCESSION` without reading that filing's table | REPAIR | Fall through to the existing table read for the pinned accession. This uses an existing path, so it is not a new capability (P5). Both runtimes. | R5-extractors |
| F5 | `extract_sec_filing_fact`: `meta.decisionGrade` (a static capability flag) contradicts the payload's per-call `decisionGrade` | PLANNER CALL | Recommended: retire the meta flag in the Phase 6 major release, with a deprecation notice from R5-extractors on | Phase 6 |
| F6 | Telemetry: preview-canary requests (about 15 per deploy) plus production canaries and deploy audits appear in Analytics Engine | REPAIR (scripts only) | Every CI canary and audit authenticates with an `Authorization: Bearer` header (`scripts/live_smoke_utils.py`). The owner's connector uses the path transport. Blob 7 (`credentialTransport`) is recorded on every row, including the baseline window, so `credentialTransport = 'header'` identifies synthetic traffic. Add a window-comparison script that queries Analytics Engine for the baseline and comparison windows with header-transport rows excluded, and applies the Phase 4 D10 bounds. The daily export gains the same filtered view. No runtime change. Caveat: once Phase 1b personal keys exist, a real user could use header transport; revisit then. | Phase 4 prep (before 2026-10-24) |
| F7 | Python EDGAR ticker index: a failed load caches `{}` and is not retried for up to 24 h | REPAIR | An empty map counts as not cached, so the next call retries. Check the Worker's `getEdgarTickerCikMap` and align it. | Phase 4 Python routing release (it matters for D2b) |
| F10 | The Worker parses RSS/Atom bodies for structured IR-page entries; Python does not | PLANNER CALL | Recommended: port it to Python (4 functions plus fixtures shared with the Worker), so a registry entry behaves the same locally. Alternative: document it as Worker-only while no registry entry uses a structured feed. | R-news D |
| F11 rest | Unregistered IR-page row keys: the Worker has `registrySchemaVersion` and no `canonicalUrl`; Python has the opposite | REPAIR (align-free) | Both rows carry both keys. Python adds `registrySchemaVersion`; the Worker adds `canonicalUrl: null`. Both keys exist already (P2). | R-news D |
| F12 | Verify stop words: English function words count as meaningful terms, so "the and of" escapes the `INPUT_VALIDATION_ERROR` guard | PLANNER CALL (matching threshold) | Recommended: add a fixed function-word list (a, an, and, are, as, at, by, for, from, in, is, it, its, of, on, or, the, to, with) to the shared stop list in both runtimes. A query of only those words then gets `INPUT_VALIDATION_ERROR`, and matching ignores them. | R-news D |
| F13 | Marketaux statuses: HTTP 404 (Worker `NOT_FOUND`, Python `PROVIDER_ERROR`); non-object body (Worker `PROVIDER_ERROR`, Python `PROVIDER_CHANGED`); legacy-envelope batch error code | REPAIR | **404:** `PROVIDER_ERROR` in both. A provider 404 is not absence of evidence; `NOT_FOUND` would overstate absence (P1). **Non-object body:** `PROVIDER_CHANGED` in both, matching the Finnhub non-array rule. **Legacy envelope:** `normalizeBatchSymbolResult` keeps the entry's `code`. | R-news D |
| F14 | 25 actions in 8 groups accept params their `tool_catalog.json` Params line omits; 2 actions have no Params line | REPAIR (catalog text) | Add the missing params and the 2 Params lines, and empty `_KNOWN_UNADVERTISED_PARAMS`. This is a versioned contract change to a frozen file; text only, with no runtime behaviour change. | R-news D (rides along) |
| F16 | `compare_earnings_actual_vs_estimate` rates Yahoo `estimate.*` rows by the comparison's confidence, while `get_earnings_analysis` rates them LOW | REPAIR | Yahoo estimate rows inside compare get LOW (source-based). The comparison result keeps its own confidence. Both runtimes. | R5-earnings |
| F17 | `get_ownership_holders` row shape: Worker camelCase `value`, Python yfinance `Value` | PLANNER CALL | Recommended: align Python to the Worker shape in the Phase 6 major release. It renames Python output keys, so it is breaking; a deprecation notice goes out in R5-fundamentals. | Phase 6 |
| F18 | Producer-level `?? "HIGH"` / `.get("confidence", "HIGH")` fallbacks (Python 6, Worker 7) | REPAIR (with audit) | Each site either gets an explicit HIGH (a direct tagged read) or falls to LOW, following the R5-shared rules. Also align Python's explicit-`None` handling with TS `??`. | R5-extractors (the sites sit in extractor and exposure code) |
| F19 | `get_analyst_consensus` provenance: Python sets `source`, `evidenceClass`, `decisionGrade`, `recommendedNextAction` and `priceTargets.sourceType: yahoo_finance`; the Worker sets none of them (`sourceType: unknown`) | REPAIR → Python | The Worker sets the same four fields and the source type. The values exist (P2). The pinned case flips to equal. | R5-fundamentals |
| F20 | Status strings (`EXTRACTION_FAILED`, `FILING_NOT_FOUND_TRY_OTHER_TYPE`) stored in `confidence` | DEFER | A data-model change: move them to a status field. This needs new or renamed fields, so it is Phase 6. | Phase 6 |
| OPS-TEL-2 | The export has no coverage guard for Analytics Engine counting requests while Workers Logs counts 0 | CLOSED | Already done in 2.14.0 (`coverage.warnings` `LOGS_ZERO_WITH_REQUESTS`) | — |
| OPS-TEL-3 | `[observability] enabled = false` sits beside `[observability.logs] enabled = true` | REPAIR (config) | Set the flags consistently, or add a comment saying why. It changes runtime logging, so it waits until after the Phase 4 exit comparison, to keep the telemetry windows comparable. | After the Phase 4 exit |
| OPS-TEL-4 | Logged invocations are fewer than Analytics Engine requests | DOCUMENT | Explained (2026-10-19): preview canaries (F6) plus rejected-auth requests, which never log. Recorded in the telemetry docs. | Phase 4 prep docs |

## 2. Release plan

| Release | Contents | Timing |
|---|---|---|
| **Phase 4 prep (scripts only)** | F6, OPS-TEL-4 docs | Before 2026-10-24. No runtime change, no version bump; a tooling PR. |
| **R-news D (2.22.0)** | F11 rest, F13, F12*, F10*, F14 | Now; gates nothing |
| **Phase 4 Python routing release (2.24.0)** | F7, plus F1 and F2 (owner, 2026-10-20: it rewrites the same Python outline function) | Phase 4, after unit 1 (D2a) |
| **R5-sec** | ~~F1, F2~~ moved to the Python EDGAR routing release (owner, 2026-10-20) | Before Phase 5 unit 1 |
| **R5-extractors (2.23.0)** | F3, F4, F18, plus Q3i rows BR-EXT-035 and BR-EXT-084 (`R5EXTRACTORS_SPEC.md`) | Now (owner, 2026-10-20); gates nothing in Phase 4 |
| **R5-fundamentals** | F19, plus the F17 deprecation notice | Before Phase 5 unit 4 |
| **R5-earnings** | F16 | Before Phase 5 unit 6 |
| **Phase 6 (major)** | F5, F17, F20; OPS-TEL-3 any time after the Phase 4 exit | — |

\* subject to the planner calls in §3.

## 3. Planner calls for the owner

1. **F12:** add the function-word list to the verify stop list (recommended), or keep the current list.
2. **F10:** port RSS/Atom structured parsing to Python (recommended), or document it as Worker-only.
3. **F17:** align the Python holder shape in the Phase 6 major release, with a deprecation notice first (recommended), or keep the two shapes and document them.
4. **F5:** retire `meta.decisionGrade` in Phase 6 (recommended), or derive it per call now.

Everything else follows the existing rules and needs no separate decision.

## 4. Approval

Approved by the owner on 2026-10-19, after "continue with the next triage while waiting". The four planner calls were answered as follows:
- F12: "Add filler words (Recommended)"
- F10: "Port to Python (Recommended)"
- F17: "Align in Phase 6 (Recommended)"
- F5: "Retire in Phase 6 (Recommended)"

**Progress:**
- **F6:** done as a Phase 4 prep tooling commit (`scripts/compare_telemetry_windows.py` and the export's `excludingHeaderTransport` view). No runtime change.
- **R-news D: released as 2.22.0 (#300, from branch `claude/rnews-d`) and live-verified 2026-10-20 03:11Z on build 2.22.0+git.bcc5274.**
  - `verify_company_event` AAPL "the and of" returns `INPUT_VALIDATION_ERROR`.
  - "earnings results" returns CONFIRMED with meaningfulTerms ["earnings"].
  - The company_ir_page row carries `canonicalUrl: null` and `registrySchemaVersion`.
  - `schemaHash` changed, as expected from the F14 catalog text.
  - F10, F11, F12, F13 and F14 are closed. A double-decoding defect in the shared XML decoder was also fixed.
- **Remaining:** F7 (Phase 4 routing release); F1, F2, F3, F4, F16, F18, F19 (R5 releases); F5, F17, F20 (Phase 6); OPS-TEL-3 (after the Phase 4 exit).
- **2026-10-20 (owner: "do as recommended"):** 2.23.0 is R5-extractors and ships now; F1 and F2 move into the 2.24.0 Python EDGAR routing release.
- **F21 (new, from the R5-extractors spec):** `query_sec_filing_index` `total_revenue`, `segment_revenue` and `customer_concentration` ignore a pinned accession and read the latest filing, so their evidence can name a different filing than the one pinned. Same class as F3. Not yet classified; proposed home R5-sec or the Phase 5 SEC unit.
- **R5-extractors: released as 2.23.0 (#301) and live-verified 2026-10-20 07:45Z on build 2.23.0+git.4cdc53b** (`R5EXTRACTORS_SPEC.md` §5). F3, F4 and F18 are closed, along with Q3i rows BR-EXT-035 and BR-EXT-084.
  - F21 is widened: the risk-factor block of a pinned `china_exposure` read also names the latest 10-K.
- **Remaining:** F7, F1 and F2 (2.24.0 routing release, after the 2026-10-24 gate); F16 and F19 (R5 releases); F21 (unclassified); F5, F17 and F20 (Phase 6); OPS-TEL-3 (after the Phase 4 exit).
- **2026-10-20 (owner: "ok yes"):** F21 is classified REPAIR and ships as 2.24.0 (`F21_SPEC.md`). F19 and F16 ship as 2.25.0 (`R5_F19_F16_SPEC.md`). The Python EDGAR routing release becomes 2.26.0.
- **F22 (new):** in `compare_earnings_actual_vs_estimate`, `actual.eps` is a Yahoo row and inherits the comparison's confidence. It is an actual, not a consensus estimate, so rating it needs its own decision. Unclassified.
- **F23 (new):** `query_sec_filing_index` `segment_revenue` without an accession: the Worker passes the extractor's warnings through and Python drops them. Pinned as `F21-SEGMENT-QUERY-WARNINGS`. Unclassified.
- **F24 (new, from the 2.25.0 port):** after the envelope runs, the `compare_earnings_actual_vs_estimate` `estimate.*` rows have 9 keys in Python and 6 in the Worker. It looks like envelope enrichment that only Python applies. It is pre-existing, and parity cases compare values only. Unclassified.
- **F21: released as 2.24.0 (#302) and live-verified 2026-10-20 08:39Z** (`F21_SPEC.md` §5).

## 5. Findings from the 2.25.0 functional audit (F25–F47)

**Source:** the live audit of 2026-10-20 (`/mnt/project-files/audits/mcp-audit-2026-10-20.md`): every grouped action on the deployed Worker (build 2.25.0+git.9424789), and 41 of the same calls through the Python grouped server. Rules P1–P5 apply.
**Owner, 2026-10-20:** "Reconcile with modernization plan and implement the fix". F25–F28 (the high-severity items that need no new name or threshold) ship now as **2.25.1**, a repair patch: 2.26.0 stays reserved for the Python EDGAR routing release. The rest are proposed classifications awaiting the owner's approval, as with §1.

| # | Finding | Class | Target behaviour | Release |
|---|---|---|---|---|
| F25 | A caption naming two scales ("In millions, except number of shares, which are reflected in thousands") labels the table thousands: both runtimes take the last scale word before the table. AAPL statement tables read 1000× low on the table paths; Python's filing-index builder instead picks any "billion" in nearby prose | REPAIR (both) | A scale after "except", or about shares, in the same parenthetical is not the monetary scale. Python's index builder ports the Worker's `tableUnitScale` | **2.25.1** |
| F26 | `extract_segment_revenue` ignores `period` on the table path: the Worker returns the latest column for any year as FOUND; Python has no table path, so AAPL is always NOT_DISCLOSED (overstates absence, P1). The Worker table read also ignored a pinned accession (an F21 gap) | REPAIR → Worker (table path), both (period) | A fiscal year reads that year's column; a filing whose segment tables lack it returns the existing `PERIOD_NOT_FOUND` with the years found. A fiscal year with a quarterly filing type returns the existing `INVALID_PERIOD`. The table read uses the pin. Python gains the Worker's table path (P2) | **2.25.1** |
| F27 | `get_latest_earnings_release` without an SEC 8-K takes Yahoo calendar `earningsDate[0]`, a future estimate (7203.T: 2026-11-05 at MEDIUM). Python has the same code, unreachable because its calendar keys differ (F47) | REPAIR (both) | Only a calendar date at or before now counts; otherwise NOT_FOUND | **2.25.1** |
| F28 | Seven earnings tools advertise `period` enum `["latest"]` but accept any value: `extract_earnings_metrics` "FY2025 Q4" returned FY2026 Q3 with no mismatch | REPAIR (both) | Any other value gets the existing `INPUT_VALIDATION_ERROR` before any fetch (P4) | **2.25.1** |
| F29 | Python `extract_earnings_metrics` has no SEC XBRL quarter tier, so revenue, margins, FCF and capex are null and EPS is LOW; the Worker reads XBRL at HIGH. The catalog says XBRL first | REPAIR → Worker | Port the Worker's tier 1 (`selectQuarterFact`, YTD derivation, recency window) with fixtures | R5-earnings |
| F30 | Python `get_market_quote` for an unknown ticker: `ok:true`, all nulls, `yearHigh`/`yearLow` NaN (invalid JSON). The Worker returns `TICKER_NOT_FOUND` | REPAIR → Worker | | R5-pricing |
| F31 | Python rejects `get_ownership_holders` `holder_type:"insider_transactions"`, which the catalog lists; the Worker serves it | REPAIR → Worker | | R5-fundamentals |
| F32 | `get_sec_filing_section_markdown` "Item 7" (10-K): Python NO_FILING_DATA; the Worker finds it but keeps raw entities (`&#160;`, `&#8217;`), and `word_count` counts the truncated text | REPAIR (both) | Python finds the section; both decode entities | R5-sec |
| F33 | `get_fund_profile` on an equity: sections are PROVIDER_ERROR (Python) / EMPTY_RESULT (Worker), both recommending a retry | PLANNER CALL | Recommended: one existing status in both, with a message that the ticker is not a fund; no new code (P4) | R5-fundamentals |
| F34 | Worker profile `priceToSalesTrailing12Months` is null: flattening the quoteSummary modules lets a later null overwrite it. Python has 10.72 | REPAIR → Python | A null never overwrites a value when merging modules | R5-fundamentals |
| F35 | `extract_exposure` India (not disclosed): Worker EXTRACTION_FAILED + TABLE_NOT_PARSED, Python NOT_FOUND | PLANNER CALL | Recommended: NOT_FOUND in both when the geographic table parsed and has no such row | R5-extractors (next) |
| F36 | `extract_management_commentary` topic "china": FOUND at HIGH with an excerpt (the income-statement header) that lacks the term. Both runtimes | REPAIR (both) | An excerpt must contain a topic term, else NOT_FOUND (P1) | R5-earnings |
| F37 | `extract_capital_structure` AAPL: COMPUTED with no instruments and an empty maturity ladder (0% coverage), while `extract_funding_capex_schedule` reads the debt-principal ladder from the same 10-K. Both runtimes | REPAIR (both) | Reuse the schedule's ladder; a 0% coverage result is not COMPUTED | R5-extractors (next) |
| F38 | `extract_funding_capex_schedule`: boilerplate ("committed to inclusion") classed COMPANY_DISCLOSED_COMMITTED; $56.2B manufacturing purchase obligations classed capital_expenditure | PLANNER CALL (classification rule) | Recommended: a commitment needs an amount or a dated obligation; purchase obligations get their own existing category if one fits, else UNRESOLVED | R5-extractors (next) |
| F39 | `extract_china_exposure` manufacturingExposure NOT_FOUND although the 10-K says "outsourcing partners located primarily in China mainland" | PLANNER CALL (matching rule; Q5 China preset) | Recommended: add the phrase family to the China preset data | R5-extractors (next) |
| F40 | `extract_revenue_exposure` "iPhone" is labelled `exposureType: geographic_revenue` | PLANNER CALL (enum) | Recommended: an existing product/segment type if one exists, else a Q11 name | R5-extractors (next) |
| F41 | `verify_company_event` "quarterly dividend 0.27" NOT_FOUND: release bodies are not read | DEFER (P5: a new capability) | | Phase 6 or later |
| F42 | `calculate_price_target_distance` with a negative target returns PROVIDER_ERROR, not INPUT_VALIDATION_ERROR. Both runtimes | REPAIR (both) | | R5-fundamentals |
| F43 | Default SEC form lists exclude amendments: `get_sec_recent_events` (`["8-K","10-Q","10-K"]`) and the event timeline miss Apple's 8-K/A of 2026-09-01 | PLANNER CALL | Recommended: include `8-K/A` in the defaults, flagged as an amendment | R-news follow-up |
| F44 | Ops: the free Alpha Vantage key (25/day) was spent by one audit session, degrading the curve cross-check, expanded ownership, historical put/call and the last transcript fallback; Finnhub ownership returns 403 ENTITLEMENT_REQUIRED | OWNER (ops) | A paid key, or document the quota in the runtime guidance | — |
| F45 | `get_company_press_releases`: `meta.decisionGrade` false while `data.decisionGrade` true | Extends F5 | Retire with F5 | Phase 6 |
| F46 | Python `list_sec_filing_tables` has a different row shape (no `unitScale`, `sectionId`, `rowLabels`, `confidence`; `rowCount`, `qualityStatus` instead) and no titles | REPAIR → Worker (P2) | | R5-sec |
| F47 | Python `get_company_events_calendar` returns yfinance keys ("Earnings Date"); the Worker returns `calendar.earnings.earningsDate`. This also makes F27's Python path unreachable | PLANNER CALL | Recommended: align in Phase 6 with a deprecation notice, as F17 | Phase 6 |

**Low-severity items (not yet classified):** short-interest units (`shortPctFloat` percent vs `shortPercentOfFloat` ratio, no `unitSemantics`); `momentumFlag` POSITIVE beside `compositeMomentumSignal` NEUTRAL; insider rows with missing keys and no limit; corporate-action pandas keys and Python missing "Capital Gains"; `compare_peer_valuations` error rows exposing a provider URL without a code; table numbering differing between tools; repeated evidence in `query_sec_filing_index`; the ROUNDED_FACT_IGNORED wording; the dilution price labelled `caller_supplied` when none was supplied; an empty `latestGuidance` failure message; Thai `lastUpdatedAt` without a timezone and 5-day-old NAV not flagged; `extract_customer_concentration` AAPL EXTRACTION_FAILED where only receivables concentration is disclosed; XBRL evidence in `extract_earnings_metrics` with null accession, date and concept; no `truncated` flag on `get_sec_filing_section`.

**Approval of §5:** the owner approved F29–F47 as recommended on 2026-10-21 ("All recommended. Free key limit shall be documented."). The planner calls F33, F35, F38, F39, F40, F43 and F47 take the recommended option. F44 is closed by documentation: the free-key limit and the Finnhub entitlement are in `docs/provider-runtime-guidance.md` under Scarce Optional Market Providers.

**Progress:**
- **F25–F28: released as 2.25.1 (#304) and live-verified 2026-10-21 01:15Z on build 2.25.1+git.93451d9.** AAPL 10-K tables 22, 24 and 28 report `unitScale` millions; `extract_segment_revenue` FY2024 returns the FY2024 column (Americas 167,045M, total 391,035M) and FY2022 returns `PERIOD_NOT_FOUND` with fiscal years 2025, 2024, 2023; 7203.T `get_latest_earnings_release` is NOT_FOUND; `extract_earnings_metrics` period "FY2025 Q4" gets `INPUT_VALIDATION_ERROR`.
- **F44:** closed (documented).
- **Remaining from §5:** F29, F36 (R5-earnings); F30 (R5-pricing); F31, F33, F34, F42 (R5-fundamentals); F32, F46 (R5-sec); F35, F37, F38, F39, F40 (next extractor release); F43 (R-news follow-up); F41, F45, F47 (Phase 6 or later). The low-severity list is unclassified.
