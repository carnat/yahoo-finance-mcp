# Q3i triage: `scripts/parity/cases_news_a.py` (33 pinned FINDING cases + 6 Q7 shared-defect comments)

Checked against HEAD `a176314` (2.13.1). `python3 scripts/test_parity_harness.py -k news` ran 218 tests, all OK, so every pinned value below still matches current code in both runtimes. None of the in-scope cases has been flipped to `equal`, so nothing was skipped.

Abbreviations: `py` = `yfmcp/tools/news.py` unless another file is named; `ts` = `worker/src/news.ts` unless another file is named; `cases` = `scripts/parity/cases_news_a.py`.

Rows marked **Q7-shared** are equal cases where both runtimes share the same defect. For each one the table says whether a repair is needed.

---

## G1. Event-type keyword lists (BR-NWS-120/130)

eventType drives the Yahoo item's `decisionUse`: material types give CHECK_OFFICIAL_RELEASES and `other` gives CONTEXT_ONLY (ts:498-502).

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.event_type.python_only_phrases_differ_from_worker | cases:517; py:455-475; ts:557-569 | BR-NWS-120/130 | 'announces new' gives product, 'annual report' gives earnings, 'credit facility' gives financing | all three give `other` (CONTEXT_ONLY) | status (eventType/decisionUse) | **REPAIR → Python list** (add the 3 phrases to the Worker) | The Worker downgrades material events to context-only; Python's list sends them to official verification and never understates materiality | S | R-news |
| news.event_type.keyword_priority_and_substring_matching_agree (**Q7-shared**, repair needed: yes) | cases:511-515; py:455-477; ts:557-569 | BR-NWS-130/253 | substring matching ('steps' gives eps/earnings, 'sector' gives sec/regulatory, 'dealer' gives contract, 'operates' gives rates/macro, 'notebook' gives financing); the regulatory '10-k' entry is unreachable | same | status (eventType/decisionUse) | **PLANNER-DECISION** | Whole-word matching fixes misclassification in both directions. Some items would move from CHECK_OFFICIAL_RELEASES to CONTEXT_ONLY (e.g. 'dealer', 'sector'), so the safer side is not uniform. Options: (a) word-boundary matching for every keyword; (b) word-boundary only where the substring match lands in a CONTEXT_ONLY type, keeping over-flagging; (c) keep substring matching and document it | S | R-news |

## G2. Yahoo issuer identity (BR-NWS-139)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.identity.ticker_only_short_name_resolves_in_python_not_in_worker | cases:578; py:198-224; ts:450-473 (459) | BR-NWS-139 | a shortName equal to the ticker becomes an alias: RESOLVED, coverage FULL, no warning | the alias is dropped: UNAVAILABLE, SOURCE_IDENTITY_UNAVAILABLE, status PARTIAL, coverage PARTIAL | coverage | **REPAIR → Worker** (Python skips an alias equal to `ticker.lower()`) | A ticker echoed back as the company name is not issuer identity. Python claims FULL coverage on it | S | R-news |

## G3. Dedupe and identifiers (BR-NWS-114/118/119/136)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.dedupe.html_entity_in_title_groups_in_python_not_in_worker | cases:702; py:480-481; ts:571-580 | BR-NWS-118/251 | `html.unescape` runs before the stem, so `&amp;` and `&` titles merge (1 item) | no unescape, so the same story stays as 2 items | figure (item count / corroboration) | **REPAIR → Python** (the Worker decodes entities, e.g. its existing `decodeXmlText`, before the stem) | One story counted twice inflates apparent corroboration and the item count | S | R-news |
| news.dedupe.group_id_format_differs_between_runtimes | cases:716; py:515-528; ts:591-600 | BR-NWS-114/119 | sha256, first 16 hex | 31x rolling 32-bit hash, 8 hex | text (id format) | **PRESERVE document** | Opaque id that the harness never compares. Aligning needs a synchronous sha256 in the Worker (crypto.subtle is async), so the change is not trivial. Note the theoretical 32-bit collision risk, which would merge distinct stories (negligible at ≤100 items) | M if aligned | R-news (docs) |
| news.timestamp.epoch_publishedat_has_milliseconds_only_in_the_worker | cases:743; yfmcp/util.py:231-246; ts:542-552 | BR-NWS-136 | `2025-10-09T08:53:20Z` | `2025-10-09T08:53:20.000Z` | text (timestamp precision) | **PRESERVE align-free** (strip `.000Z` to `Z` in the Worker's `normalizeIso`/`iso`) | Same instant, only formatting differs. No figure or status changes | S | R-news |

## G4. SEC source in the news path (BR-NWS-133)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.sec.filing_without_url_warns_only_in_the_worker_news_path | cases:996; py:1116-1121 (news path) vs py:2971-2978 (get_sec_recent_events); ts:1247-1252 | BR-NWS-133 | url null, confidence LOW, no SEC_URL_INVALID in the news path | same plus a SEC_URL_INVALID warning | text (warning) | **PRESERVE align-free → Worker** (Python's SEC event builder appends SEC_URL_INVALID, which Python already emits in get_sec_recent_events) | Warning code already exists in Python; confidence and status are identical | S | R-news |

## G5. Form 4 parsing (BR-NWS-123/128)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.form4.half_cent_value_rounds_differently | cases:1111; py:678; ts:660 | BR-NWS-128 | `round(0.125, 2)` gives 0.12 (half-even on float) | `Math.round(x*100)/100` gives 0.13 (half-up; also rounds negatives toward +∞ and has float error on e.g. 1.005) | figure (insiderTransaction.value) | **REPAIR → half away from zero to cents in both** (Python `Decimal(str(shares*price)).quantize(Decimal("0.01"), ROUND_HALF_UP)`; Worker uses a matching decimal-string/epsilon helper with sign handling) | A published value differs by one cent between runtimes. Neither runtime is "safer", so the target is one deterministic convention | S | R-news |
| news.form4.well_formed_xhtml_document_is_parsed_only_by_the_worker | cases:1147; py:684-717 (685-688); ts:730-733 | BR-NWS-123 | falls back to the HTML parser only when XML parsing raises. Well-formed XHTML has no transaction node, so no parse and FORM4_PARSE_UNAVAILABLE | falls back whenever no XML transaction node exists, so the transaction is parsed | figure (insiderTransaction) | **REPAIR → Worker** (Python also falls back to `_parse_form4_html_transaction` when `tx is None`) | Both runtimes already trust the HTML parser for HTML Form 4s. Python's gap is accidental and drops a real transaction. Nothing is overstated, because a failed HTML parse still returns None and the warning. Related to the owner's "multi-line Form 4" Q3i item | S | R-news |

## G6. EX-99.1 press-release exhibit identification (BR-NWS-109, P0)

Python uses `_resolve_ex991_url` (yfmcp/tools/earnings.py:180-195: type in {EX-99.1, EX-99, 99.1}). The Worker uses `resolveEx991Url` (worker/src/yahoo-finance.ts:13567-13589: `^EX-99\.0?1`, or EX-99* with release wording, or a file-name regex, plus an index-HTML regex fallback). The same resolver also feeds the earnings tools, so the release is R4 or must be coordinated with it. Combined target: accept `^EX-99\.0?1\b` (subject to G6c), accept EX-99* only with PRESS/EARNINGS/RESULTS RELEASE wording, and do not accept a file-name-only match.

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| G6a press.ex99.bare_ex99_with_other_description_resolves_only_in_python | cases:1218 | BR-NWS-109 | bare 'EX-99' with description "Exhibit" resolves: SEC_EX99_FOUND, decisionGrade true | not resolved: SEC_8K_FOUND_EX99_NOT_FOUND, decisionGrade false | decisionGrade | **REPAIR → Worker** (require release wording for bare EX-99) | A non-release EX-99 (an investor deck, for example) must not make the payload decision grade | S | R4 (shared resolver; earnings too) |
| G6b press.ex99.file_name_match_without_a_type_resolves_only_in_worker | cases:1228 | BR-NWS-109 | not resolved: decisionGrade false | resolved from file name `ex99_1.htm`: decisionGrade true | decisionGrade | **REPAIR → Python** (drop the file-name-only match in the Worker) | File name alone is weaker evidence than the type column, and it grants decisionGrade | S | R4 |
| G6c press.ex99.zero_padded_type_resolves_only_in_worker | cases:1223 | BR-NWS-109 | 'EX-99.01' not accepted: decisionGrade false | accepted: decisionGrade true | decisionGrade | **PLANNER-DECISION** (evidence threshold) | EX-99.01 is the same exhibit number written differently, so accepting it is accurate. Under the A2 rule, though, Python's rejection is the side that never overstates. Options: (a) accept `EX-99.0?1` in both (Python adds it); (b) reject it in both | S | R4 |

## G7. Press-release empty-result status (BR-NWS-147/148)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| press.empty.candidate_only_ir_page_status_differs | cases:1265; py:2853-2862; ts:2751-2759; ts tools.ts:1795 | BR-NWS-148 | COMPANY_IR_PAGE_NOT_APPROVED | COMPANY_IR_PAGE_CANDIDATE_ONLY | status | **PLANNER-DECISION** (enum value) | decisionGrade is false in both and candidateSources match, so neither overstates. Aligning to the Worker adds a status value to Python. The value is also missing from the Worker's own `limitationStatuses` list (tools.ts:1795). Options: (a) Python adopts CANDIDATE_ONLY and tools.ts lists it; (b) the Worker drops it and returns NOT_APPROVED | S | R-news |
| press.empty.sec_outage_with_yahoo_selected_reads_as_no_yahoo_press_release (**Q7-shared**, repair needed: yes) | cases:1252-1258; py:2853-2862; ts:2751-2759 | BR-NWS-147 | SEC PROVIDER_ERROR but status NO_YAHOO_PRESS_RELEASE; the outage shows only in sourceStatus and the SOURCE_UNAVAILABLE warning | same | status | **PLANNER-DECISION** (new status value for this tool) | The status reads as genuine absence during an outage (owner Q3i: do not present failed sources as clean). Options: (a) a source-limited press status, e.g. SOURCE_LIMITED_NOT_FOUND (new for this tool's status enum, and limitationStatuses must change); (b) keep the status and set `failureMode`/coverage to PARTIAL from the failed SEC row; (c) document it | S | R-news |

## G8. verify_company_event (BR-NWS-106/138/163/164)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| verify.status.match_outside_the_date_range_and_older_than_90_days_is_stale_only_in_python | cases:1366; py:3104-3113 (collects 365d, no range), 3154-3194; ts:2840 (passes start/end into collection), 2932-2956 | BR-NWS-164 | the old match is collected: STALE, 1 match | the range filters it out during collection: NOT_FOUND, 0 matches | status | **REPAIR → Python** (the Worker verify collects the 365-day window without start/end and filters by range only when classifying) | NOT_FOUND hides evidence that was found and reads as "did not happen". STALE reports it without claiming confirmation. Changes the provider query window (Finnhub from/to) | M | R-news |
| verify.best_evidence_confidence_for_contextual_news_differs | cases:1380; py:3266-3288 (item's own confidence), 3159-3164; ts:241-247, 2937-2941, 3029-3038 | BR-NWS-164 | contextual Yahoo news bestEvidence confidence MEDIUM (item's own) | LOW (source-type based) | confidence | **REPAIR → Worker** (Python uses a `confidence_for_source_type` helper for bestEvidence ranking/confidence and for the `official_in_range` HIGH/MEDIUM test) | Contextual aggregator news must not read as MEDIUM. The same root cause also governs CONFIRMED eligibility, so Python could overstate it | S | R-news |
| verify.auth_error_does_not_upgrade_not_found (**Q7-shared**, repair needed: yes) | cases:1326-1330; py:2306-2335; ts:1627-1646 | BR-NWS-106/163 | Finnhub 401 gives AUTH_ERROR, no failureMode, so status stays NOT_FOUND | same | status | **PLANNER-DECISION** (failureMode value) | A real outage reported as confirmed absence breaks the P0 rule BR-NWS-106. The upgrade to SOURCE_LIMITED_NOT_FOUND is clear; the failureMode is the choice. Options: (a) map AUTH_ERROR to an existing value (SOURCE_UNCONFIGURED or PROVIDER_ERROR, not retryable); (b) add a new `PROVIDER_AUTH_ERROR` failureMode | S | R-news |
| verify.status.old_official_release_without_dates_is_still_confirmed (**Q7-shared**, repair needed: probably) | cases:1373-1377; py:3155-3158, 3166-3194; ts:2932-2956 | BR-NWS-164 | no start/end means every match is "in range", so a 200-day-old official release is CONFIRMED and STALE is unreachable | same | status | **PLANNER-DECISION** (evidence threshold) | The event did occur, so CONFIRMED is defensible, but there is no recency signal. Options: (a) without dates, apply the 90-day stale cutoff and return STALE; (b) keep CONFIRMED and add a recency note/warning (possibly a new warning code); (c) document it | S | R-news |
| verify.query_policy.generic_phrase_still_matches_despite_the_warning (**Q7-shared**, repair needed: probably) | cases:1389-1398; py:3141-3153; ts:2917-2930 | BR-NWS-138 | an all-stop-word query still returns a PHRASE match: PARTIAL with EVENT_QUERY_TOO_GENERIC | same | status | **PLANNER-DECISION** (evidence threshold) | Status PARTIAL rests on a query the tool itself calls too generic. Options: (a) zero meaningful terms means no match and NOT_FOUND; (b) keep the match but cap the status (no CONFIRMED/PARTIAL); (c) document it | S | R-news |

## G9. Company IR page registry source (BR-NWS-101/142/144/149)

Python derives `company_ir_page` state from IR-page warnings (py:995-1012, 2227-2255). The Worker derives it from collector diagnostics with no info warnings for candidate or disabled entries (ts:1030-1107). Collection status: py:1992-2009 marks any COMPANY_IR_PAGE_* warning as source-limited. With no warning and nothing used, the Worker falls through to PROVIDER_ERROR.

**Group fix (status rows a–c):** the Worker emits Python's info warnings COMPANY_IR_PAGE_CANDIDATE_AVAILABLE and COMPANY_IR_PAGE_DISABLED, both of which already exist in Python. Its collection status then becomes SOURCE_LIMITED_NOT_FOUND.

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| a. ir_page.candidate_empty_result_status_differs | cases:1580 | BR-NWS-149/144 | SOURCE_LIMITED_NOT_FOUND; warnings SOURCE_UNAVAILABLE + COMPANY_IR_PAGE_CANDIDATE_AVAILABLE (info) | same status; SOURCE_UNAVAILABLE only | text (warning) | **PRESERVE align-free → Python** (comes with the group fix) | Status already agrees. Only the info warning is missing | S | R-news |
| b. ir_page.candidate_alone_empty_result_status_differs | cases:1586 | BR-NWS-149 | SOURCE_LIMITED_NOT_FOUND + info warning | PROVIDER_ERROR, no warning | status | **REPAIR → Python** | No provider failed. SOURCE_LIMITED_NOT_FOUND says what actually happened (unreviewed source). Neither side claims absence | S | R-news |
| c. ir_page.disabled_entry_status_and_allowed_action_differ | cases:1593; py:1001; ts:1057-1059 | BR-NWS-142/149 | DISABLED / registry_review_required / COMPANY_IR_PAGE_DISABLED (info) / SOURCE_LIMITED_NOT_FOUND | DISABLED / `none` / no warning / PROVIDER_ERROR | status | **REPAIR → Python** (status and warning; allowedAction as in d) | Same reasoning as b | S | R-news |
| d. ir_page.stale_entry_allowed_action_differs | cases:1599; ts:1081 | BR-NWS-142/171 | allowedAction registry_review_required | `revalidate` | text (advisory enum) | **PRESERVE align-free → Python** | Advice only. Python's value already exists in both runtimes. Worker-only values (`revalidate`, `review_registry_entry`, `none`) would be new to Python | S | R-news |
| e. ir_page.validation_failure_allowed_action_differs | cases:1606; ts:1106 | BR-NWS-142 | registry_review_required | `review_registry_entry` | text (advisory enum) | **PRESERVE align-free → Python** | same as d | S | R-news |
| f. ir_page.approved_page_with_nothing_in_window_allowed_action_differs | cases:1613; py:2249-2251; ts:1092-1093 | BR-NWS-142/146 | EMPTY_RESULT with registry_review_required | EMPTY_RESULT with fetch_configured_source | text (advisory enum) | **PRESERVE align-free → Worker** (Python maps an approved, used EMPTY_RESULT to fetch_configured_source) | A fresh approved page with an empty window needs no registry review. The value exists in both | S | R-news |
| g. ir_page.approved_fetch_status_row_registry_metadata_differs | cases:1620; py:2244-2248; ts:1035-1055 | BR-NWS-101 | registryVersion/canonicalUrl/approvalStatus null unless an IR-page warning exists | always populated from the registry entry | text (metadata fields, present in both) | **PRESERVE align-free → Worker** (Python fills them from the registry entry) | Fields already exist in Python (as null). Purely descriptive | S | R-news |

Note on d/e: the planner may prefer the Worker's more specific values. That would add three allowedAction values to Python, which makes it a PLANNER-DECISION; it is not counted in the totals.

## G10. `company_ir` semantics (BR-NWS-103/107, P0) — 8 cases, one root cause (`RSS_DECISION`)

Python treats `company_ir` as a Yahoo-feed passthrough: it fetches Yahoo news (py:2381), marks company_ir used and sets status EMPTY_RESULT (py:2220-2226). The collection status is then NOT_FOUND, and Yahoo items come back under a company_ir-only request. The Worker (since 2.10.0/Q2a) reads only the approved IR registry: with no approved entry it returns NOT_REGISTERED, not used, no items, and collection status PROVIDER_ERROR (ts:1139-1162).

**Group fix:** Python adopts the Worker's semantics. `company_ir` is the approved registry entry (or NOT_REGISTERED with `registry_review_required`); company_ir alone does not pull the Yahoo feed, and the source is not marked used. Optional follow-up for the planner: today both runtimes report "unregistered source only" as PROVIDER_ERROR, which is a shared quirk; SOURCE_LIMITED_NOT_FOUND would be more accurate.

| id | file:line | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|
| company_ir.passthrough_in_python_vs_rss_discovery_in_worker | cases:1673 | used ["company_ir"], EMPTY_RESULT, a Yahoo item returned | used [], NOT_REGISTERED, no items | coverage | **REPAIR → Worker** | Python claims a company-IR search happened and found nothing (NOT_FOUND/FULL), but it never consulted a company source. That overstates coverage on a P0 rule | M (group) | R-news |
| company_ir.discovery_finds_alternate_link_feed_on_the_company_domain | cases:1684 | EMPTY_RESULT | NOT_REGISTERED | coverage | REPAIR → Worker (group) | same | – | R-news |
| company_ir.no_feed_anywhere_exhausts_the_probe_budget | cases:1701 | EMPTY_RESULT / NOT_FOUND | NOT_REGISTERED / PROVIDER_ERROR | coverage | REPAIR → Worker (group) | same | – | R-news |
| company_ir.no_company_website_gives_website_not_available | cases:1704 | same | same | coverage | REPAIR → Worker (group) | same | – | R-news |
| company_ir.external_feeds_not_matching_the_company_are_rejected | cases:1708 | same | same | coverage | REPAIR → Worker (group) | same | – | R-news |
| company_ir.feed_that_fails_to_download_is_provider_error | cases:1715 | same | same | coverage | REPAIR → Worker (group) | same | – | R-news |
| company_ir.feed_that_is_not_xml_is_parse_error | cases:1718 | same | same | coverage | REPAIR → Worker (group) | same | – | R-news |
| company_ir.discovery_accepts_a_probed_path_that_is_itself_a_feed | cases:1992 | EMPTY_RESULT, no items | NOT_REGISTERED, no items | coverage | REPAIR → Worker (group) | same | – | R-news |

Several of these case names and comments still describe the pre-2.10.0 RSS discovery (cards 170/183/165/167). When the group flips to `equal`, rename them or merge them into a single case.

## G11. Marketaux exchange check (BR-NWS-121)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.marketaux.nyse_american_normalises_to_nyse_before_the_exchange_check (**Q7-shared**, repair needed: yes) | cases:908-922; py:1200-1211; ts:524-531 and ts:1697 | BR-NWS-121 | 'NYSE American' contains 'NYSE' and is tested first, so it becomes NYSE. An AMEX entity is rejected and a NYSE entity accepted | same | figure (accepted items) / input-handling | **REPAIR → test NYSE American/AMEX/ASE before NYSE in both** (the Worker's NYSEAMERICAN branch at ts:539 is currently unreachable) | For NYSE American issuers the correctly tagged rows are rejected and mis-tagged ones accepted. After the fix, a NYSE-tagged row for an NYSE American issuer is rejected as a mismatch (strict, does not overstate) | S | R-news |

## G12. Batch `get_company_news` (BR-NWS-143)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| batch.per_ticker_results_and_batch_meta_differ | cases:1799; py:3397-3401; worker/src/yahoo-finance.ts:1224-1228 (runPartialBatch), worker/src/tools.ts:1989-2010 (moved into envelope meta) | BR-NWS-143 | per-ticker results only | `__batchMeta` {partialSuccess, successCount, errorCount}, which the envelope lifts into `meta` | text (envelope meta) | **PRESERVE document** | Internal key the Worker envelope strips. Aligning would add output fields to Python's envelope (a planner/R4 envelope matter), and no figure changes | – | R-news (docs) |
| batch.symbol_failing_python_ticker_validation_is_an_error_entry_only_in_python | cases:1806; py:3402-3404 (`_validate_ticker` per ticker); ts:2524-2541 (blank check only) | BR-NWS-143 | 'BAD TICKER!' gives an INPUT_VALIDATION_ERROR entry | runs the collection for it (counts as a success) | input-handling | **REPAIR → Python** (the Worker validates ticker format with Python's rule, single and batch) | A malformed symbol reported as a successful empty news result reads as absence. Probably shared with other Worker tools, so check the Worker validator before scoping | S | R-news |

## G13. Provider-eligibility policy (BR-NWS-177/180)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| ownership.unreadable_policy_fails_open_in_python | cases:1966; yfmcp/tools/provider_gaps.py:216-220 (`except ... return True, None`); policy is bundled in worker/src/news-source-capabilities.json | BR-NWS-180 | unreadable policy file means every ticker is eligible: Finnhub is called (SOURCE_UNCONFIGURED here) | bundled policy: NOT_ELIGIBLE, USE_YAHOO_OR_OFFICIAL_FILINGS | status | **REPAIR → fail closed** (Python returns not-eligible with a policy-unavailable reason on read/parse error) | Already decided by the owner in the Q3i confirmed set ("provider-eligibility policy read/parse failure must fail closed"). Also check `yfmcp/tools/news.py:80-90` (`_finnhub_policy` raises rather than failing closed) | S | R-news (owner-confirmed Q3i item; R4 if the planner treats the policy as shared) |

## G14. GlobeNewswire XML guard (BR-NWS-182/154)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| newswire.doctype_in_a_feed_is_refused_only_by_python | cases:2001; py:318, 327-329; ts:1874 | BR-NWS-182 | a feed containing `<!DOCTYPE` is refused: PROVIDER_ERROR, SOURCE_UNAVAILABLE, coverage PARTIAL | refuses only `<!ENTITY`, so the items are returned OK | status / coverage | **REPAIR → Python** (the Worker also refuses `<!doctype`) | The stricter guard never overstates: the failed source is reported as PARTIAL. It also closes the DTD-based XML attack surface. Real GlobeNewswire feeds carry no DOCTYPE | S | R-news |

---

## PLANNER-DECISION items (8)

1. **G1 Q7 substring matching**: (a) word-boundary matching for all keywords, (b) word-boundary only where the match lands in CONTEXT_ONLY types, or (c) document it.
2. **G6c EX-99.01**: accept `EX-99.0?1` in both, or reject it in both. This is an evidence threshold that sets decisionGrade, on the shared resolver.
3. **G7 candidate-only press status**: Python adopts COMPANY_IR_PAGE_CANDIDATE_ONLY and `limitationStatuses` adds it, or the Worker drops it.
4. **G7 Q7 press SEC outage reads as absence**: a new source-limited press status, or failureMode/coverage only, or document it.
5. **G8 Q7 AUTH_ERROR failureMode**: map to an existing value (PROVIDER_ERROR or SOURCE_UNCONFIGURED), or add a new value. The upgrade to SOURCE_LIMITED_NOT_FOUND itself is clear.
6. **G8 Q7 old official release without dates is CONFIRMED**: apply the 90-day STALE cutoff, add a recency note, or document it.
7. **G8 Q7 generic all-stop-word query still matches**: no match, or a capped status, or document it.
8. **G9 d/e** (not counted in the totals): Python's existing allowedAction values, which this table proposes, versus the Worker's more specific values, which would be new to Python.

## Totals

| | count |
|---|---|
| Rows | 39 (33 pinned FINDING + 6 Q7-shared) |
| REPAIR | 23 (G1 phrases, G2, G3 html-entity, G5 ×2, G6a, G6b, G8 stale, G8 confidence, G9 b, G9 c, G10 ×8, G11, G12 validation, G13, G14) |
| PRESERVE align-free | 7 (G3 timestamp, G4, G9 a, d, e, f, g) |
| PRESERVE document | 2 (G3 group id, G12 batchMeta) |
| PLANNER-DECISION | 7 (G1 Q7, G6c, G7 ×2, G8 auth, G8 old-confirmed, G8 generic phrase) |
| Skipped (already equal) | 0 |

Repair needed for the 6 Q7-shared defects: G11 NYSE American (REPAIR), G1 substring, G7 press outage and G8 AUTH_ERROR (all three PLANNER-DECISION, repair needed: yes), and G8 old-confirmed and G8 generic phrase (both PLANNER-DECISION, repair needed: probably).

Effort: 1 group at M (G10 company_ir) plus G8 stale (M, changes the Worker collection window); everything else is S. The release is R-news for all rows except G6a/G6b/G6c, which go in R4 because the EX-99.1 resolver is shared with the earnings tools.
