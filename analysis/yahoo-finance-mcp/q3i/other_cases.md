# Q3i triage: non-news pinned FINDING cases + RN wording notes

Scope: every `mode="pinned"` case whose `decision` starts with `FINDING` in `scripts/parity/cases_*.py`, except cases_news_a/b. That is 16 cases: 10 in cases_neutrality, 3 in cases_pricing_evidence_tools, 2 in cases_sec_earnings, 1 in cases_valuation. cases_accession, budget, capital_structure, caps, credential_transport, extraction_rules and validation have no FINDING case. Also in scope: the two RN deprecation wording notes from MODERNIZATION_BRIEF.md (RN status, line 318).

**Checked against the current code (HEAD a176314, after 2.13.1):** I ran all 16 cases through `scripts/parity/harness.py` (`run_worker` / `run_python` / `evaluate`). The script is at `scratchpad/triage/tools/run_findings.py`. **All 16 still PASS as pinned.** None has been flipped to `equal` in 2.12.0–2.13.1, so none is skipped.

Not in scope, but noticed:
- `cases_budget.py` `BATCH_DECISION` is pinned without a FINDING prefix.
- Next to the xbrl FINDINGs are two `equal` "Finding (Phase 2)" cases: an empty `{}` xbrlContext and a numeric `0` evidence field both pass as decision-grade in both runtimes. They are not divergences, so they get no row here. They do share the decision-grade root cause (group G4).

Note on "new field": several fixes add to one runtime a field, key or code that already exists in the other runtime's contract. CLAUDE.md lists "a new or changed output field" as a planner decision. Where the fix only changes a value or adds behaviour, I mark REPAIR. Where it adds a field or renames keys, I mark PLANNER-DECISION. G2 is the one borderline case, flagged in its row.

---

## G1. Price target with a non-positive lastPrice (BR-PRC-022, Q14-N3)
Fix: both runtimes treat a lastPrice that is ≤ 0 or not finite as "no price". Each returns its existing `No price data for {ticker}` error before any ratio is computed. No new code or field.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| neutral.n3a.finding_zero_price_python_raises_worker_reports_null_flag | cases_neutrality.py:255-262; server.py:4826-4833; worker/src/yahoo-finance.ts:7718-7758 | BR-PRC-022 / Q14-N3 | `distance_to_target_pct` divides by 0, so the tool raises ZeroDivisionError | Returns a success payload: `referenceTargetPct` 0, `distanceToTargetPct` null, flag null. The deprecated `bracket` STRONG_BUY and `inferredTag` SPECULATIVE are still emitted for a price of 0 | status | REPAIR → both return the existing "No price data" error for lastPrice ≤ 0 / non-finite | The Worker presents a zero quote as a valid, labelled observation, and Python crashes. Rejecting the price overstates nothing. (Alternative: a success payload with every derived field null. Either is safe; the error reuses an existing path.) | S | R5-pricing |

## G2. Section markdown reliability marking (Q14-N1, spec section 1)
Fix: Python's `get_sec_filing_section_markdown` marks its fallback the way the Worker does: `status` SECTION_MARKDOWN_UNVERIFIED, `confidence` NOT_DECISION_GRADE, `decisionGrade` false, warning `LIVE_SECTION_EXTRACTION_UNRELIABLE`. It also adds `SECTION_MARKDOWN_LOW_CONTENT` when the text is under 50 words. Both runtimes already report `reliabilityClass` UNRELIABLE, so Python's "MEDIUM" contradicts its own meta.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| neutral.n1.finding_section_markdown_status_decision_grade_and_warning_differ | cases_neutrality.py:396-403; server.py:6250-6258; worker/src/yahoo-finance.ts:9714-9740 | Q14-N1 (section markdown) | No status, decisionGrade or warnings; confidence "MEDIUM" | status SECTION_MARKDOWN_UNVERIFIED, decisionGrade false, confidence NOT_DECISION_GRADE, warning LIVE_SECTION_EXTRACTION_UNRELIABLE | confidence / decisionGrade | REPAIR → Python adopts the Worker marking | Python overstates confidence on a lossy HTML fallback. The Worker never does. *Borderline: this adds fields and codes to Python that already exist in the Worker contract. Planner to confirm that is not "new".* | S | R4 |

## G3. doctrineUse is emitted by the Worker only (Q14-N1)
Fix: none now. The RN spec (section 1) chose not to add the deprecated field to Python. `reliabilityClass`, which replaces it, is equal in both runtimes (the n1 equal cases). The divergence ends when Phase 6 removes `doctrineUse`. Document it in provider-runtime-guidance as intentional for the deprecation window.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| neutral.n1.finding_doctrine_use_and_its_notice_are_worker_only_press_releases | cases_neutrality.py:371-375; worker/src/tools.ts:1769-1816, news.ts:2794, deprecations.ts:42-46,135 | Q14-N1 | No doctrineUse, no N1 notice | meta + data doctrineUse ALLOWED, DEPRECATED_FIELD notice | text (deprecated field presence) | PRESERVE document | No figure or status differs; reliabilityClass carries the same value in both. Adding a field to Python only to remove it later is waste | S (docs) | R4 docs → Phase 6 removal |
| neutral.n1.finding_doctrine_use_and_its_notice_are_worker_only_extract_sec_filing_fact | cases_neutrality.py:376-379; worker/src/tools.ts:1811-1816 | Q14-N1 | No doctrineUse, no notice | meta doctrineUse ALLOWED, notice | text | PRESERVE document | Same as above | S | R4 docs → Phase 6 |
| neutral.n1.finding_doctrine_use_and_its_notice_are_worker_only_section_markdown | cases_neutrality.py:380-383; worker/src/yahoo-finance.ts:9738 | Q14-N1 | No doctrineUse, no notice | meta + data doctrineUse BLOCKED, notice | text | PRESERVE document | Same as above. The status side of this tool is G2 | S | R4 docs → Phase 6 |

## G4. Decision-grade XBRL predicate (BR-SHR-001)
Fix: one stricter predicate in both runtimes. `xbrlContext` must be a plain object, not an array: Worker adds `!Array.isArray`. Each evidence field must be non-blank after trimming: Python adds `str(v).strip() != ""`. The related equal-mode gaps (`{}` context and numeric `0` passing) could ride the same fix if the planner wants. They are not pinned divergences.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| shr.decision_grade.array_xbrl_context_diverges | cases_pricing_evidence_tools.py:849-852; server.py:5353; worker/src/yahoo-finance.ts:11687 | BR-SHR-001 | Array xbrlContext → false (requires dict) | Array passes `typeof === "object"` → true | decisionGrade | REPAIR → Python's rule (reject arrays) | The Worker grades a fact decision-grade on a contextless array | S | R4 |
| shr.decision_grade.whitespace_only_evidence_field_diverges | cases_pricing_evidence_tools.py:853-856; server.py:5355-5357; worker/src/yahoo-finance.ts:11688-11689 | BR-SHR-001 | `"   "` concept accepted → true | Trims → false | decisionGrade | REPAIR → Worker's rule (trim, reject blank) | Python grades a fact decision-grade with blank evidence | S | R4 |

## G5. CAGR horizon start bar (BR-PRC-021)
Fix: fold this into the existing Q3i short-history CAGR repair already tagged for R5-pricing (BUSINESS_RULES §3 line 151 names this boundary difference). The cutoff uses calendar years in both runtimes, as Python's `DateOffset` does. When the first bar on or after the cutoff is not near the cutoff (history is short or has a gap), the horizon's CAGR is null and is not divided by the nominal N.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| prc.cagr.start_bar_selection_differs_on_the_horizon_boundary | cases_pricing_evidence_tools.py:1329-1334; yfmcp/tools/pricing.py:503-506; worker/src/yahoo-finance.ts:2605-2615 | BR-PRC-021 | Calendar 3y cutoff includes the 2023-10-09 bar → cagr3y 10.0642 (correct) | 3×365.25-day cutoff drops that bar, starts at 2025-10-09 and still uses exponent 1/3 → 3.5744 | figure | REPAIR → calendar-year cutoff in both, plus the Q3i null-on-short-span rule | The Worker publishes 2 years of growth as a 3-year CAGR | M (with the Q3i repair) | R5-pricing |

## G6. analyze_position_signals asymmetries (Q14-N3c)
These are separate root causes in one tool. One row each.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| neutral.n3c.finding_component_status_keys_and_failed_components_differ | cases_neutrality.py:512-519; server.py:4931-4948; worker/src/yahoo-finance.ts:7826-7833 | Q14-N3c | Tracks all 6 components (`analystUpgrades`, `analystConsensus`, `priceStats`, `earningsMomentum`, `technicalIndicators`, `maPosition`). An empty or error payload → FAILED, listed in `failedComponents`, status PARTIAL | Tracks only 3 (`pricePerformance`, `technicalIndicators`, `movingAveragePosition`) from their `status`. No `failedComponents`. A failed analyst or earnings component leaves nulls while status stays OK | coverage | **PLANNER-DECISION**. Direction is clear: the Worker must report failures in all 6 components (REPAIR toward Python). Key names are open. Options: (a) the Worker adopts Python's 6 keys + `failedComponents`, renaming its 3 existing keys; (b) Python adopts the Worker's 3 names and both add 3 more under agreed names, plus `failedComponents`; (c) each keeps its names and the Worker adds the missing 3 + `failedComponents` (parity stays broken on names) | The Worker overstates coverage (status OK with failed inputs). The fix adds or renames output keys | M | R5-pricing |
| neutral.n3c.finding_quote_timestamp_is_python_only_and_quote_data_date_comes_from_different_components (a: quoteDataDate) | cases_neutrality.py:520-525; server.py:4997-5001; worker/src/yahoo-finance.ts:7843 | Q14-N3c | `quoteDataDate` = date of priceStats `priceTimestamp`, the quote behind `currentPrice`; null if absent | `quoteDataDate` = `ma.dataDate` (completed-bar date, not the quote) | figure (date) | REPAIR → Python's source (date of the price quote; null if missing) | The Worker labels the quote with a bar date that can differ from the quote | S | R5-pricing |
| same case (b: quoteTimestamp presence) | server.py:5002 | Q14-N3c | Emits `quoteTimestamp` | No `quoteTimestamp` | text (field presence) | **PLANNER-DECISION**. Options: add `quoteTimestamp` to the Worker (priceStats `priceTimestamp`); drop it from Python; document as intentional | Adding or removing an output field | S | R5-pricing |
| neutral.n3c.finding_t5_macd_is_python_only | cases_neutrality.py:504-511; server.py:4983; worker/src/yahoo-finance.ts:7818-7825 | Q14-N3c | `t5_inputs` / `technicalInputs` include `macd` | Omits `macd`, although its `getTechnicalIndicators` computes it | text (field presence) | **PLANNER-DECISION**. Options: add `macd` to the Worker's t5/technicalInputs (trivial, value already available); drop it from Python; document. `technicalInputs` survives Phase 6, so this choice is permanent | Adding or removing an output field in a universal group | S | R5-pricing |
| neutral.n3c.finding_batch_tickers_are_accepted_by_the_worker_only | cases_neutrality.py:527-543; server.py:5155-5156; worker/src/yahoo-finance.ts:7770-7772, tools.ts:2660-2661 | Q14-N3c | Over MCP, a list `ticker` is rejected by pydantic (`string_type` validation error; checked via `call_tool`). The harness calls the function directly, which passes the list into the components and returns a bogus OK object with `ticker` set to the list | Expanded mode batches the list (`runPartialBatch`) into a result per ticker, though its inputSchema declares `ticker: string` | input-handling | **PLANNER-DECISION**. Options: (a) Python accepts `str \| list` and batches like its other pricing tools (aligns to the Worker; Q12 cap applies); (b) the Worker rejects lists per its declared schema; (c) document. Separately, the case should go through the MCP or router boundary, since the direct-call path is unreachable | Batch support is a capability and contract choice. Neither runtime overstates over MCP | S–M | R5-pricing |
| neutral.n3c.finding_analyst_count_source_totalanalysts_in_python_numberofanalysts_in_the_worker | cases_neutrality.py:566-577; server.py:1385-1388, 4959; worker/src/yahoo-finance.ts:7800 | Q14-N3c | `analystCount` ← `totalAnalysts`. Consensus emits `ratingCount`, `numberOfAnalysts` and `totalAnalysts`, all 7 | `analystCount` ← `numberOfAnalysts`. Consensus has no `totalAnalysts` | text (deprecated alias presence) | PRESERVE document | Every count value agrees. Python's extra field is a deprecated alias of `ratingCount` (comment at server.py:1385). Removing or adding it is a field change, so leave it to the Phase 6 removal | S (docs) | R5-pricing docs → Phase 6 |

## G7. Earnings input and period text
| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| ern.source_url.unparseable_url_message_differs | cases_sec_earnings.py:756-758; yfmcp/tools/earnings.py:117-122; worker/src/yahoo-finance.ts:13471-13478 | BR-ERN-017 | `"not a url"` → INPUT_VALIDATION_ERROR "source_url must use https" | Same code, "source_url must be a valid URL" | text | PRESERVE align-free → Python returns "must be a valid URL" when urlparse gives no scheme and no netloc | Same refusal and code; only the wording differs, and the Worker's is more accurate | S | R5-earnings |
| ern.compare.date_identified_row_is_unverified_runtimes_differ | cases_sec_earnings.py:962-964; yfmcp/tools/earnings.py:726-756; worker/src/yahoo-finance.ts:14380-14395 | BR-ERN-001 / BR-ERN-018 | `estimatePeriod` "2026-06-30T00:00:00" (str(Timestamp)), so the dated revenue-estimate row does not match → `estimateRevenue` null | `estimatePeriod` "2026-06-30" → matches → `estimateRevenue` 1e9. Both runtimes: UNVERIFIED, revenue surprise omitted, MEDIUM | figure (+ text) | **PLANNER-DECISION**. The date text should be normalised to YYYY-MM-DD in Python either way (align-free). The figure choice is open: (a) align to the Worker (estimate shown under UNVERIFIED, decisionGrade false); (b) safer-by-rule: both withhold `estimateRevenue` while alignment is UNVERIFIED (changes the Worker figure) | Python's null is accidental, not a rule. Whether showing an estimate on an unverified period "overstates" is a judgement call | S | R5-earnings |

## G8. Reconciliation tolerance input coercion (BR-VAL-063, Q3e)
| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| val.recon.pinned_tolerance_numeric_string | cases_valuation.py:1497-1499; server.py:7555-7560; worker/src/tools.ts:1582,2566, yahoo-finance.ts:12427 | BR-VAL-063 | `float("5")` → 5.0. Pydantic lax mode also coerces "5" at the MCP boundary | `num()` takes numbers only → 0.5 default | status (input-handling; tolerance decides AGREED/CONFLICT) | REPAIR → treat a non-number as unusable → 0.5 default in both. Python needs a strict float at the MCP boundary and in the function | Matches the Q3e rule (unusable → documented 0.5 default, as "abc" already does). A tighter tolerance never overstates agreement. (Rejecting with INPUT_VALIDATION_ERROR is the other option, if the planner prefers it.) | S | R5-valuation (with Q3e) |

## G9. RN deprecation wording notes (MODERNIZATION_BRIEF.md:318)
Both runtimes have the same text, so these are wording defects rather than divergences. A fix changes the text in both runtimes, the parity notice constants (cases_neutrality.py:49-51) and provider-runtime-guidance.

| id | file:line | rule | Python behaviour | Worker behaviour | kind | proposed class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| RN-note-1 price-target notice replacement | yfmcp/deprecations.py:66; worker/src/deprecations.ts:62 | Q14-N3a | "…use priceAtOrAboveTarget and referenceTargetPct." `referenceTargetPct` is a ratio, not a direction replacement | Same | text | PRESERVE align-free → e.g. "use priceAtOrAboveTarget (direction); referenceTargetPct gives the price/target ratio". `replacement` stays `priceAtOrAboveTarget` | Message only. Code, fields and `replacement` are unchanged | S | R4 (or Phase 6 per the Brief) |
| RN-note-2 tagNote points to a deprecated field | server.py:4892; worker/src/yahoo-finance.ts:7755 | Q14-N3a | `tagNote` says "Use inferredTag." (deprecated itself) | Same | text | PRESERVE align-free → "…Use priceAtOrAboveTarget." | `tagNote` is removed in Phase 6, so fixing it only there is moot. If fixed at all, do it in R4 | S | R4 |

---

## Totals
- Pinned FINDING cases in scope: 16 (all still pinned at HEAD; none skipped), plus 2 RN wording notes.
- **Rows: 19.** 16 cases + 2 RN notes, with the quoteDataDate/quoteTimestamp case split into two rows.
- **REPAIR: 7.** G1 zero price, G2 section markdown (borderline "new field", planner to confirm), G4 ×2 xbrl predicate, G5 CAGR boundary, G6 quoteDataDate, G8 tolerance string.
- **PRESERVE align-free: 3.** source_url message, RN-note-1, RN-note-2.
- **PRESERVE document: 4.** doctrineUse ×3, totalAnalysts alias.
- **PLANNER-DECISION: 5.** componentStatus/failedComponents coverage, quoteTimestamp presence, t5 macd, batch tickers, dated estimatePeriod / estimateRevenue.
