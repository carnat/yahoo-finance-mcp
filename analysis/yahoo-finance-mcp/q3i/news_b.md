# Q3i triage — news_b (`scripts/parity/cases_news_b.py`, BR-NWS-191..280)

Scope: every pinned `FINDING:` case in `cases_news_b.py` (55 cases), the Q7 "for triage" comments on equal cases (shared defects), and two live-verification inputs from the brief (§R4 Q3i).

Verified against the current tree (HEAD a176314, release 2.13.1). `python scripts/test_parity_harness.py -k news` ran 218 tests, all OK, so every pin below still matches the code. None of the 55 pinned cases has been flipped to `equal` by 2.12.0/2.13.0. The only Q3c case in the file (`news.validation.batch_of_six_tickers…`) is already `equal`, so it is skipped.

Code anchors: Python is `yfmcp/tools/news.py` (abbreviated `py`), plus `server.py` / `yfmcp/schemas.py`. The Worker is `worker/src/news.ts` (`ts`) and `worker/src/yahoo-finance.ts` (`yf.ts`). The case line is the line in `cases_news_b.py`.

All rows are release **R-news** unless marked otherwise.

---

## G1. The Worker `getNews()` drops Yahoo row fields (contentType, summary, content.pubDate)

**Root cause.** `ts:171 getNews()` keeps only title, publisher, link and providerPublishTime. `YAHOO_ALLOWED_CONTENT_TYPES` (`ts:169`) therefore never fires. Press-release rows are never relabelled. The summary is lost. A `providerPublishTime` of 0 has no `content.pubDate` fallback.

Python `_collect_yahoo_events` (`py:1254`) and `_build_yahoo_event_item` (`py:1031`) read all of these fields.

**Group fix.** Make `getNews()` pass through `contentType` (row or `content.contentType`), `summary`, and the `content.pubDate` fallback, the same way Python does. The Worker collector then applies the existing CONTENT_TYPE filter and the PRESS_RELEASE relabel unchanged. Effort M for the group. The summary sub-question is a planner decision (row G1-4).

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.yahoo.source_selector_item_sets (yahoo_finance_news part) | case 391; ts:171, py:1057 | BR-NWS-198/246 | PRESS_RELEASE rows relabelled to `yahoo_finance_press_releases` and dropped from a news-only selection (accepted 7, returned 5) | all 7 returned as `yahoo_finance_news` | figure (items/returnedCount) | REPAIR → Python | labelling an issuer's own release as independent news overstates corroboration | S (with group) | R-news |
| news.yahoo.content_type_video_filter_fires_in_python_only | case 595 | BR-NWS-280/244 | VIDEO row rejected (CONTENT_TYPE) | VIDEO row accepted | figure (accepted items) | REPAIR → Python | the filter already exists in the Worker but is starved of input; never accept a video as text evidence | S | R-news |
| news.yahoo.press_release_row_in_news_selection_is_dropped_in_python_only | case 604 | BR-NWS-246/198 | returned 4, accepted 5 | returned 5 as news | figure | REPAIR → Python | same root cause as row 1 | S | R-news |
| news.yahoo.summary_text_is_available_in_python_only | case 620; py:1331 | BR-NWS-256/280 | summary[:240] / evidenceText[:180] from the Yahoo summary; **identity matching also reads the summary** | summary = evidenceText = title (17 characters); matching uses the title only | figure (evidence text + acceptance) | **PLANNER-DECISION** | carrying the summary also widens acceptance: a ticker mention deep in a summary admits the row. Options: (a) Worker carries the summary and matches on it (Python as is); (b) both carry the summary for display but match on title only (strictest); (c) Python drops the summary (Worker as is) | M | R-news |
| news.timestamps.yahoo_epoch_zero_and_millisecond_suffix_differ (epoch-0 part) | case 2583 | BR-NWS-278 | providerPublishTime 0 falls through to `content.pubDate`, row kept | row's publishedAt is null, dropped by the date window | figure (item count) | REPAIR → Python | real dated evidence is lost to a lossy adapter; keeping it overstates nothing | S (with group) | R-news |

## G2. `company_ir`: Python Yahoo passthrough vs Worker approved-registry-only (2.10.0, never mirrored)

**Root cause.** Python still serves `company_ir` from the Yahoo feed (`py:2378–2487`; status at `py:2220`). In the selector case it returns 7 Yahoo items under `sourcesUsed: ["company_ir"]` with coverage FULL while its own sourceStatus says EMPTY_RESULT. With no Yahoo items it reports EMPTY_RESULT / NOT_FOUND, which is an absence claim. The Worker (2.10.0) reads only approved registry entries and reports NOT_REGISTERED, which gives PARTIAL coverage and collection PROVIDER_ERROR.

**Group fix.** Port the 2.10.0 registry-only `company_ir` to Python. After the port, all 10 discovery cases flip to `equal` on NOT_REGISTERED. The cards that described official-site RSS discovery (BR-NWS-207/209/210/211/228/258) should be retired or re-scoped. Effort M. The verify status for NOT_REGISTERED is a planner decision (G2-12).

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.yahoo.source_selector_item_sets (company_ir part) | case 391 | BR-NWS-198 | 7 Yahoo items labelled company_ir, coverage FULL | no items, NOT_REGISTERED, coverage PARTIAL | coverage/figure | REPAIR → Worker | Python presents Yahoo news as company-IR evidence with a FULL claim | M (group) | R-news |
| news.company_ir.aggregator_website_is_not_an_official_domain | case 2205 | BR-NWS-209/210/258 | EMPTY_RESULT → NOT_FOUND | NOT_REGISTERED → PROVIDER_ERROR, PARTIAL | status/coverage | REPAIR → Worker | Python's NOT_FOUND claims an absence it never looked for | (group) | R-news |
| news.company_ir.empty_website_is_not_available | case 2209 | BR-NWS-209/210 | same as above | same as above | status/coverage | REPAIR → Worker | same as above | (group) | R-news |
| news.company_ir.ftp_scheme_is_prefixed_with_https_not_rejected | case 2214 | BR-NWS-209/210 | same as above | same as above | status/coverage | REPAIR → Worker | same as above (the Q7 note on this case is handled in G12) | (group) | R-news |
| news.company_ir.bare_domain_gets_https_and_is_reduced_to_its_root | case 2220 | BR-NWS-209/210 | same as above, no probe | same as above, no probe | status/coverage | REPAIR → Worker | same as above | (group) | R-news |
| news.company_ir.path_and_query_are_dropped_and_investors_prefix_is_stripped_for_the_root | case 2227 | BR-NWS-209 | same as above | same as above | status/coverage | REPAIR → Worker | same as above | (group) | R-news |
| news.company_ir.same_domain_feed_accepted_unrelated_external_feed_rejected | case 2265 | BR-NWS-207/228 | same as above | same as above | status/coverage | REPAIR → Worker | same as above | (group) | R-news |
| news.company_ir.external_feed_matches_on_name_token_or_ticker_but_not_stopwords | case 2272 | BR-NWS-211 | same as above | same as above | status/coverage | REPAIR → Worker | same as above | (group) | R-news |
| news.company_ir.identity_confidence_low_without_name_cik_or_website | case 2278 | BR-NWS-258 | EMPTY_RESULT + SOURCE_IDENTITY_UNAVAILABLE → SOURCE_LIMITED_NOT_FOUND | NOT_REGISTERED → PROVIDER_ERROR | status | REPAIR → Worker | the registry is the only approved company_ir path | (group) | R-news |
| news.company_ir.identity_confidence_medium_with_cik_only | case 2285 | BR-NWS-258 | same as above | same as above | status | REPAIR → Worker | same as above | (group) | R-news |
| news.company_ir.feed_with_an_entity_declaration_is_a_parse_error | case 2683 | BR-NWS-207/228 | EMPTY_RESULT → NOT_FOUND | NOT_REGISTERED | status/coverage | REPAIR → Worker | same as above | (group) | R-news |
| news.verify.company_ir_without_a_website_stays_not_found_in_the_worker | case 1333; py:2306 / ts verify failure mode | BR-NWS-197 | SOURCE_LIMITED_NOT_FOUND / YAHOO_IDENTITY_UNAVAILABLE | **NOT_FOUND**, failureMode null (NOT_REGISTERED is not a failure-mode input) | status/decisionGrade | **PLANNER-DECISION** | after the G2 port both runtimes would say NOT_FOUND for a source that was never available, which overstates absence. Options: (a) map NOT_REGISTERED to the existing `SOURCE_UNCONFIGURED`; (b) add a new failureMode such as `SOURCE_NOT_REGISTERED` (new enum value); (c) emit a warning for registry states so the status reads SOURCE_LIMITED_NOT_FOUND (see G3) | S–M | R-news |

## G3. Registry states: collection status and warnings

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.ir_page.shipped_registry_entries_are_candidates_only | case 2621; py:1992 `_build_collection_status`; ts:2755 | BR-NWS-263 | warning `COMPANY_IR_PAGE_CANDIDATE_AVAILABLE`, collection SOURCE_LIMITED_NOT_FOUND | no warning, collection PROVIDER_ERROR | status | REPAIR → Python | names the real limitation (candidate, not approved) instead of a provider error. Note: the warning code already exists in Python but is new in Worker output; the planner should confirm this counts as alignment, not a new code | S | R-news |

## G4. Exchange normalisation and exchange-prefix compatibility (shared defect + Worker-only GlobeNewswire check)

**Root cause.**
- `_normalize_exchange` (`py:1202`) and `normalizeExchange` (`ts:1692`), plus `normalizedExchangeCode` (`ts:523`) for GlobeNewswire, test `NYSE` before `NYSE AMERICAN`, so an NYSE American issuer reads as NYSE.
- The collector variants test `GS` as a substring, so `LSE GS` reads as NASDAQ.
- Python also tests `ASE` as a substring where the Worker tests `=== "ASE"`. This is a latent divergence that no case covers.
- The GlobeNewswire stock-category exchange check exists in the Worker only (`ts:532 exchangeCompatibleWithStockCategory`); Python matches the symbol only (`py:367`).

**Group fix.** Use one exchange normaliser in both runtimes:
- test `NYSE AMERICAN` / `AMEX` / exact `ASE` before `NYSE`;
- match `GS` / `NMS` / `NGM` / `NCM` as whole tokens or exact codes, not substrings.

Then port the Worker's GlobeNewswire exchange check to Python, using the fixed normaliser. Effort M.

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.yahoo.exchange_nyse_american_company_reads_as_nyse (Q7, equal) | case 464 | BR-NWS-217 | NYSE American issuer → NYSE: 'AMEX:' rejected, 'NYSE:' accepted | same | figure (acceptance) | REPAIR → fixed normaliser in both | accepts another exchange's listing and rejects the issuer's own | S | R-news |
| news.yahoo.exchange_gs_substring_reads_as_nasdaq (Q7, equal) | case 472 | BR-NWS-217 | 'LSE GS' → NASDAQ | same | figure | REPAIR → token match in both | a substring hit picks the wrong exchange | S | R-news |
| news.gnw.other_exchange_prefix_is_rejected_in_the_worker_only | case 1834 | BR-NWS-224/225/222 | TSX:/NYSE: tags kept for a NASDAQ issuer | dropped | figure | REPAIR → Worker | never attribute another exchange's ticker to the issuer | S | R-news |
| news.gnw.nyse_american_company_loses_its_own_releases_in_the_worker | case 1846 | BR-NWS-223/224 | all 3 kept (no check) | all rejected, EMPTY_RESULT → NOT_FOUND | figure/status | REPAIR → Worker check with the fixed normaliser (keep 'NYSE American:' and 'AMEX:', drop 'NASDAQ:') | the Worker's false NOT_FOUND overstates absence; Python over-accepts | S | R-news |
| news.gnw.ase_exchange_name_maps_to_nyse_american_in_the_worker | case 1859 | BR-NWS-223 | all 3 kept | 'NASDAQ:' dropped | figure | REPAIR → Worker | the stricter check, consistent with the group fix | S | R-news |

## G5. Finnhub warning text

**Group fix.** Use one message template per status in both runtimes. Either strip the Worker's internal `FINNHUB_*:` error tags from the user message, or use Python's `HTTP <n>` wording. Status, coverage and failureMode are already equal. Effort S.

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.finnhub.http_401_is_auth_error_with_runtime_specific_message | case 748; ts:2005 | BR-NWS-219 | 'Finnhub auth error: HTTP 401' | '…: FINNHUB_AUTH_ERROR:401' | text | PRESERVE align-free | wording only | S | R-news |
| news.finnhub.http_500_is_provider_error_with_runtime_specific_message | case 760 | BR-NWS-219 | 'HTTP Error 500: …' (urllib) | 'HTTP 500' | text | PRESERVE align-free | wording only | S | R-news |
| news.finnhub.non_array_payload_is_provider_changed_with_runtime_specific_message | case 772; ts:2014 | BR-NWS-219 | 'unexpected response format' | 'FINNHUB_PROVIDER_CHANGED: expected array, got object' | text | PRESERVE align-free | wording only | S | R-news |
| news.verify.auth_error_alone_is_not_a_failure_mode | case 1292 | BR-NWS-197 | same text as the 401 row | same text as the 401 row | text | PRESERVE align-free | status, failureMode and next action are equal | S | R-news |
| news.verify.changed_payload_is_provider_changed | case 1318 | BR-NWS-197 | same text as the payload row | same text as the payload row | text | PRESERVE align-free | same as above | S | R-news |

## G6. Marketaux

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.marketaux.http_401_is_auth_error_with_runtime_specific_message | case 1160; ts:2147 | BR-NWS-230 | 'returned HTTP 401 (AUTH_ERROR).' | 'unavailable (AUTH_ERROR).' | text | PRESERVE align-free | wording only; status and action equal | S | R-news |
| news.marketaux.http_402_is_rate_limited_with_runtime_specific_message | case 1164 | BR-NWS-230 | 'returned HTTP 402 (RATE_LIMITED).' | 'unavailable (RATE_LIMITED).' | text | PRESERVE align-free | same as above | S | R-news |
| news.marketaux.http_500_is_provider_error_with_runtime_specific_message | case 1168 | BR-NWS-230 | 'returned HTTP 500 (PROVIDER_ERROR).' | 'unavailable (PROVIDER_ERROR).' | text | PRESERVE align-free | same as above | S | R-news |
| news.marketaux.row_rejection_reasons_and_accepted_count_at_the_3_row_limit | case 1136; ts:2082 | BR-NWS-229/230 | acceptedCount 4 (counts every acceptable row), returned 3 | stops reading at 3: acceptedCount 3; later rejections also go uncounted | figure (diagnostic count / truncation) | REPAIR → Python | matches the accepted/returned semantics of the other providers, and keeps truncatedSources truthful (with acceptedCount 3 the Worker hides that evidence was cut) | S | R-news |

## G7. Result-count and lookback inputs

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.limits.zero_means_default_in_python_and_one_in_worker (also the Q7 note at case 838) | case 839; py:382–387 `int(value or default)`; yf.ts:8961 `clampInt` | BR-NWS-194/277 | 0 → default (10 results / 14 days) | 0 → 1 (1 result / 1 day) | input-handling (figure) | REPAIR → Worker (clamp 0 to 1) | an explicit argument should not be silently widened; negative values already clamp to 1 in both, so 0 is the odd one out. The planner may instead prefer INPUT_VALIDATION_ERROR (existing code) | S | R-news |

## G8. `get_sec_recent_events`

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.sec.recent_events_filing_types_validation_differs | case 987; py:2950 | BR-NWS-237/216 | `['  ']` → INPUT_VALIDATION_ERROR; `[]` → 8-K/10-Q/10-K | `['  ']` → empty NOT_FOUND; `[]` → every form (DEF 14A, 424B5 …) | input-handling/status | REPAIR → Python | the Worker's NOT_FOUND for a blank filter is a false absence claim, and `[]` silently widens to non-event forms | S | R-news |
| news.sec.recent_events_payload_has_coverage_fields_in_worker_only | case 1002 | BR-NWS-195/261 | keys: items, meta, ticker, warnings | adds coverage, sourceCoverage, sourceStatus | coverage (field set) | **PLANNER-DECISION** | a new output field in one runtime. Options: (a) add the three fields to Python (new Python fields); (b) remove them from the Worker (a public field removal) | S | R-news |

## G9. Form 4 summary rounding

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.sec.form4_summary_rounding_differs_on_halves | case 1060 | BR-NWS-255 | `:,.0f` (half-to-even): $18,516, $0, 2 shares | Math.round (half-up): $18,517, $1, 3 shares | text (summary string; the numeric `value` is equal) | PRESERVE align-free | only the display string differs; align on one rounding helper (half-up is the simplest to mirror) | S | R-news |

## G10. Shared provider client (Finnhub/Alpha): entitlement classification and denial memory

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.provider.current_plan_body_is_entitlement_in_the_worker_only | case 1545; yf.ts:236 | BR-NWS-233/191 | 'Your current plan does not include this.' → AUTH_ERROR / CHECK_PROVIDER_CREDENTIALS | ENTITLEMENT_REQUIRED / USE_ALPHA_VANTAGE_EXPLICITLY | status | REPAIR → Worker pattern | a plan denial is not a credential fault; Python sends the caller after the wrong fix. Neither overstates evidence | S | R-news (shared client; also used by ownership) |
| news.provider.entitlement_denial_is_remembered_for_six_hours_in_the_worker_only | case 1556; yf.ts:277–292 | BR-NWS-193 | retries every call | second call: attempted false, HIT_PROCESS, deniedUntil +6h | status (attempt metadata only; status and action equal) | PRESERVE document | the Worker isolate cache protects the subrequest budget and reports itself truthfully; Python has no such constraint | — | R-news |
| news.provider.alpha_daily_limit_is_remembered_until_utc_midnight_in_the_worker_only | case 1567 | BR-NWS-193 | retries every call | remembered until 00:00Z | status (attempt metadata only) | PRESERVE document | same as above | — | R-news |

## G11. Analyst valuation from news

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.valuation_tool.short_name_alias_exists_in_the_worker_only | case 1747 | BR-NWS-226/200/201 | one issuer name (longName, else shortName): both headlines rejected, NOT_FOUND | longName + shortName + companyName aliases: Cantor target CLAUSE-accepted, METHOD_NOT_DISCLOSED | figure/status | **PLANNER-DECISION** | the attribution alias set is an evidence threshold. Python never overstates but rejects a correctly attributed target; the Worker is more accurate but looser. Options: (a) Worker alias set in both; (b) Python single-name in both | S | R-news |
| news.valuation_tool.news_error_payload_raises_news_unavailable_in_python_only | case 1759 | BR-NWS-201/226/240 | invalid ticker → warning NEWS_UNAVAILABLE | no news warning | status (warning presence) | REPAIR → Python | discloses that news evidence was unavailable (existing code in both runtimes) | S | R-news |

## G12. Shared defects from Q7 comments on equal cases (not covered above)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.verify.low_relevance_downgrade_never_fires (Q7) | case 1347 | BR-NWS-242 | every item carries tickers=[ticker], so relevance is HIGH at the first test; CONFIRMED with no LOW_RELEVANCE_EVIDENCE although the evidence text lacks the ticker | same | decisionGrade/confidence | REPAIR → score relevance from evidence text and issuer, not the injected tickers list (both runtimes) | a verification result is CONFIRMED on evidence that never names the subject. LOW_RELEVANCE_EVIDENCE already exists | M | R-news |
| news.yahoo.legacy_aggregate_counts_the_same_feed_twice (Q7) | case 2515 | BR-NWS-257 | rawCount/retrievedCount 40 for 20 stories; 20 NOT_ISSUER_WIRE_RELEASE rejections | same | figure (diagnostic counts) | REPAIR → count the shared feed once in both | inflated raw and rejected counts overstate what was retrieved | S | R-news |
| news.ranking.single_letter_ticker_is_found_inside_any_word (Q7) | case 2544; ts:189-style substring tests | BR-NWS-262 | ticker 'A' found in 'MARKETS RALLY' → tickerRelevance HIGH, ranked first | same | confidence (relevance) | REPAIR → word-boundary ticker token in both | a substring hit labels unrelated news HIGH | S | R-news |
| news.company_ir.ftp_scheme… (Q7 note only) | case 2212; yf.ts:9008 `normalizeCompanyWebsite` | BR-NWS-209/210 | no website handling | 'ftp://acme.com' → 'https://ftp://acme.com' still happens inside `resolveNewsCompanyIdentity`, but since 2.10.0 the website and identityConfidence reach no output (company_ir is registry-only; the identity feeds only exchange and names) | — | **MOOT — skipped** (dead code; optional cleanup when G2 lands) | unobservable in either runtime today | — | R-news |

## G13. Event-type keyword classification (shared substring defect + divergent keyword lists)

**Group fix.** One keyword table in both runtimes, with word-boundary matching. Python's list is the superset: it adds 'announces new', 'annual report' and 'credit facility'. Keep the first-group-wins order. Effort M. This changes eventType, and with it decisionUse (regulatory → CHECK_OFFICIAL_RELEASES).

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.classification.keyword_groups_and_substring_false_hits_agree (Q7, equal) | case 2429; py:455, ts:553 | BR-NWS-253 | 'steps'→earnings, 'dealer'→contract, 'denote'→financing, 'corporates'→macro, 'sector'→regulatory | same | status (eventType → decisionUse) | REPAIR → word-boundary matching in both | false typing changes decisionUse | M (group) | R-news |
| news.classification.python_only_keywords | case 2437 | BR-NWS-253 | 'announces new'→product, 'annual report'→earnings, 'credit facility'→financing | other, other, regulatory ('secures' ⊃ 'sec') | status | REPAIR → Python list (with word boundaries) | same root cause as the row above | (group) | R-news |

## G14. Labels, text cut, timestamps

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.labels.unparseable_url_provenance_differs | case 2478; py:106, ts:322 | BR-NWS-254 | 'not a url' → PUBLISHER | UNKNOWN | confidence (provenance label) | REPAIR → Worker | do not claim a publisher for a URL that does not parse | S | R-news |
| news.text.cut_position_counts_code_points_in_python_and_utf16_units_in_the_worker | case 2506; py:508, ts shortText | BR-NWS-256 | cut at 240 code points | cut at 240 UTF-16 units, can leave a lone surrogate | text | PRESERVE align-free → Python (code-point cut) | text only; a lone surrogate is malformed output | S | R-news |
| news.timestamps.* (millisecond suffix part of both cases) | cases 2583, 2595 | BR-NWS-278 | '2026-10-14T09:00:00Z' | '…09:00:00.000Z' (also the watermark) | text (timestamp precision) | PRESERVE align-free | precision only; one ISO formatter | S | R-news |
| news.timestamps.provider_date_strings_parse_differently (RFC 2822 part) | case 2595; ts:543 normalizeIso | BR-NWS-278 | 'Tue, 13 Oct 2026 10:00:00 GMT' → null, row dropped | parsed, row kept | figure (item count) | REPAIR → Worker (Python parses RFC 2822, e.g. `email.utils`) | the date is unambiguous; dropping real dated evidence is a lossy parse, not caution | S | R-news |

## G15. GlobeNewswire XML guard

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.gnw.doctype_feed_is_rejected_in_python_only | case 1873; py:327, ts:1875 | BR-NWS-272 | any `<!DOCTYPE` rejected | only `<!ENTITY` rejected; the DOCTYPE feed is parsed | input-handling/status | REPAIR → Python | the stricter XML guard, never parsing DTDs. The company IR feed guard (ts:1110) should follow | S | R-news |
| news.gnw.entity_feed_is_rejected_with_runtime_specific_text | case 1882 | BR-NWS-272 | 'unsupported XML declaration' | 'unsupported XML entity declaration' | text | PRESERVE align-free | wording only | S | R-news |

## G16. Dedupe

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.dedupe.html_entities_are_decoded_in_python_only | case 1980; py:480, ts:1284 | BR-NWS-251 | '&amp;' and '&' merge into one item | two items | figure (item count) | REPAIR → Python | the duplicate is counted as two corroborating items | S | R-news |
| news.dedupe.duplicate_group_id_algorithms_differ | case 1990; py:515 | BR-NWS-251/252 | 16-hex SHA-256 prefix | 8-hex 32-bit hash | text (id format) | PRESERVE document | an opaque id; aligning needs async SHA-256 (crypto.subtle) in a sync Worker path, which is not trivial | — | R-news |

## G17. Approved IR-page parsing

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.ir_page.issuer_without_a_registry_name | case 2108; py:828 | BR-NWS-249 | issuer / originalSource = 'IQE.L' (ticker) | null | figure (issuer attribution) | REPAIR → Worker | do not invent an issuer name from a ticker | S | R-news |
| news.ir_page.structured_json_summary_reads_content_text_in_the_worker_only | case 2114; ts:981 | BR-NWS-202 | summary falls back to the title | summary = content_text | text | PRESERVE align-free → Worker | display text from an approved source; no matching or status impact | S | R-news |
| news.ir_page.feed_shaped_body_is_parsed_as_rss_in_the_worker_only | case 2121 | BR-NWS-202 | html_article adapter, RSS body → no items, EMPTY_RESULT → NOT_FOUND | parsed as feed → items, APPROVED_SOURCE_OK | status/figure | **PLANNER-DECISION** | neither is safe: Python claims an approved source is empty, and the Worker accepts content in a format the registry did not approve. Options: (a) Worker; (b) Python; (c) SOURCE_VALIDATION_FAILED in both (existing status, consistent with the content-type rule in the next row) | S | R-news |
| news.ir_page.html_adapter_with_json_content_type_fails_validation_in_the_worker_only | case 2128 | BR-NWS-203 | EMPTY_RESULT → NOT_FOUND | SOURCE_VALIDATION_FAILED + COMPANY_IR_PAGE_VALIDATION_FAILED warning | status/coverage | REPAIR → Worker | Python's NOT_FOUND overstates absence | S | R-news |

## G18. `get_company_press_releases` payload

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.press_releases.payload_has_doctrine_use… (doctrineUse + cikInt part) | case 2341; ts:2794, ts:2652 | BR-NWS-267 | no doctrineUse; secEvidence has no cikInt | doctrineUse 'ALLOWED'; secEvidence[].cikInt | field set | **PLANNER-DECISION** | output fields in one runtime only. Options: (a) add both to Python; (b) drop them from the Worker | S | R-news |
| news.press_releases.payload_has_doctrine_use… (decisionGradeBasis wording) | case 2341 | BR-NWS-267 | 'No resolved SEC EX-99 or approved IR-page evidence…' | 'No resolved SEC EX-99 press-release exhibit…' | text | PRESERVE align-free → Python wording | Python's sentence names both decision-grade paths | S | R-news |

## G19. Schema, tool surface, Python-only helpers

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| news.schema.python_declares_a_status_enum_that_runtime_statuses_leave | case 2377; yfmcp/schemas.py:214 | BR-NWS-214 | internal `_TOOL_OUTPUT_SCHEMAS` declares status enum [OK, PARTIAL], yet NOT_FOUND etc. are emitted. Since 2.13.1 the declarations are no longer advertised | no status property | text (internal schema) | PRESERVE align-free | no client-visible output; delete or correct the stale enum. Tie to the R4 "Python outputSchema" input | S | **R4** |
| news.validation.get_yahoo_finance_news_alias_exists_in_python_only | case 1453; server.py:222 | BR-NWS-271 | unregistered helper function remains (alias removed from the public surface in 2.0.0) | 'Unknown tool name.' | text (dead surface) | PRESERVE align-free | delete the dead Python helper and its schemas.py:271 key (the parity case reads that key; re-point it). No public change | S | R-news |
| news.python_only.safe_sec_url_and_xbrl_document_detection | case 2656 | BR-NWS-235 | Python helpers | placeholder (inline, unreachable) | — | PRESERVE document | no reachable Worker counterpart | — | R-news |
| news.python_only.audit_title_overlap_ratio_and_call_states | case 2667 | BR-NWS-250 | Python CI audit script | placeholder | — | PRESERVE document | a CI tool, not runtime output | — | R-news |

## G20. Live-verification triage inputs (brief §R4 Q3i)

| id | file:line | rule | Python | Worker | kind | class | why | effort | release |
|---|---|---|---|---|---|---|---|---|---|
| LIVE: get_company_news coverage FULL while a source is truncated | py:138 `_build_coverage` / py:2292; ts:360–385 / ts:1598 | BR-NWS-007 / 260 / 275 | `coverage.state` = FULL while `truncatedSources` is non-empty and the action is RETRY_TRUNCATED_SOURCE (e.g. max_results 500: accepted 120, collected 100, truncated 20) | same | coverage | **PLANNER-DECISION** | a coverage guard. Options: (a) any truncatedSources makes the state PARTIAL; (b) only provider- or cap-side truncation (pool cap 100, Marketaux 3-row limit, dedupe) is PARTIAL, while truncation to the caller's own max_results stays FULL; (c) keep FULL, define the state as "sources reached", and document that truncatedSources carries truncation (the catalog already says so); (d) a new state value (new enum) | S | R-news |
| LIVE: tool_catalog.json omits `sources` for get_company_press_releases | tool_catalog.json `groups.news_events.description` | BR-NWS-231 | the function accepts `sources` (py:2727) | the Worker accepts it | text (frozen catalog contract) | PRESERVE align-free | text-only fix to the "Params:" line. The same omission exists for `verify_company_event` (sources) and `get_sec_recent_events` (filing_types, max_results). `scripts/test_grouped_contract_parity.py` checks only advertised ⊆ accepted and required ⊆ advertised, so optional omissions pass; consider tightening it. It is a versioned contract change to a frozen file | S | R-news |

---

## Totals

| | count |
|---|---|
| Rows | 67 (66 classified, plus 1 moot/skipped Q7 note) |
| Pinned FINDING cases covered | 55 (3 cases split into two rows: selector, epoch-zero/ms, RFC/ms, press-release payload) |
| Q7 equal-case comments covered | 7 (NYSE American, GS, low relevance, legacy double count, single-letter ticker, keyword substring, ftp scheme as moot), plus the case 838 note, already pinned |
| Live inputs | 2 |
| **REPAIR** | **37** |
| **PRESERVE align-free** | **17** |
| **PRESERVE document** | **5** |
| **PLANNER-DECISION** | **7** |
| Moot / skipped | 1 (`company_ir` ftp scheme: unobservable since 2.10.0) |
| Flipped to equal in 2.12.0/2.13.0 | none of the pinned cases; the only Q3c case here was already equal |

### PLANNER-DECISION items
1. **Yahoo summary in the Worker** (`news.yahoo.summary_text_is_available_in_python_only`): pass it through and match on it, display it only, or drop it.
2. **Verify status for NOT_REGISTERED** (`news.verify.company_ir_without_a_website…`): map to the existing SOURCE_UNCONFIGURED, add a new failureMode, or emit a registry warning.
3. **`get_sec_recent_events` coverage / sourceCoverage / sourceStatus**: add them to Python or remove them from the Worker.
4. **Valuation attribution alias set** (shortName/companyName in the Worker only): an evidence threshold.
5. **IR page with an RSS body on an html_article adapter**: Worker, Python, or SOURCE_VALIDATION_FAILED in both.
6. **Press releases `doctrineUse` + `secEvidence[].cikInt`**: add them to Python or drop them from the Worker.
7. **Coverage FULL while truncated** (live input): a coverage guard; four options listed in G20.

### Notes for the planner
- G3: Worker adoption of `COMPANY_IR_PAGE_CANDIDATE_AVAILABLE` is a code that already exists in Python but is new in Worker output. Confirm it counts as alignment.
- G7: zero limits are classified REPAIR → Worker clamp. INPUT_VALIDATION_ERROR is a defensible alternative.
- G4: Python's `"ASE"` substring test vs the Worker's exact `=== "ASE"` is an extra latent divergence that no case pins. Fold it into the normaliser fix.
- G2: once the registry-only `company_ir` is ported, the six discovery cards (BR-NWS-207/209/210/211/228/258) describe behaviour neither runtime has. Retire or re-scope them.
