# News rules (Q7 targeted extraction)

Extracted on 2026-10-15 with `/modernize-extract-rules` (module pattern `news`). The run did 4 rounds and confirmed 180 rules: 9 P0, 139 P1 and 32 P2. Referees rejected 9 rules. 63 cards flag a suspected defect, and 59 carry an SME question.

The run stopped at the round limit before it ran dry, so a tail of rules may remain. The IDs `BR-NWS-101` onward are the Q7 series. They sit alongside `BR-NWS-001`…`010` in `BUSINESS_RULES.md`, and some cards overlap with those. Machine-readable cards are in `news_rules.json`.

## Index

| ID | P | Category | Rule | Defect? |
|---|---|---|---|---|
| BR-NWS-101 | P0 | Lifecycle | Approved company IR page registry lifecycle | yes |
| BR-NWS-102 | P0 | Lifecycle | Company IR page registry approval lifecycle |  |
| BR-NWS-103 | P0 | Lifecycle | Legacy company_ir source: Python serves it from Yahoo news, Worker from official RSS discovery | yes |
| BR-NWS-104 | P0 | Policy | Evidence class, ticker match and decision use labeling |  |
| BR-NWS-105 | P0 | Policy | Press release decision-grade and status precedence |  |
| BR-NWS-106 | P0 | Policy | Verify NOT_FOUND upgraded to SOURCE_LIMITED_NOT_FOUND |  |
| BR-NWS-107 | P0 | Policy | company_ir source means different things in Python and the Worker | yes |
| BR-NWS-108 | P0 | Validation | Company IR page URL allow-list |  |
| BR-NWS-109 | P0 | Validation | EX-99.1 press-release exhibit identification and filer CIK selection | yes |
| BR-NWS-110 | P1 | Calculation | Analyst price target extraction from news sentences (four patterns, fixed precedence) |  |
| BR-NWS-111 | P1 | Calculation | Analyst price target parsing precedence and currency |  |
| BR-NWS-112 | P1 | Calculation | Brokerage firm attribution for a news item |  |
| BR-NWS-113 | P1 | Calculation | Company IR page record extraction (JSON vs HTML) |  |
| BR-NWS-114 | P1 | Calculation | Duplicate group id hashing |  |
| BR-NWS-115 | P1 | Calculation | Duplicate group key |  |
| BR-NWS-116 | P1 | Calculation | Duplicate news collapse by source priority |  |
| BR-NWS-117 | P1 | Calculation | Duplicate resolution by source priority |  |
| BR-NWS-118 | P1 | Calculation | Duplicate title stem: HTML entities decoded only in Python, and hash differs | yes |
| BR-NWS-119 | P1 | Calculation | Duplicate-story grouping key | yes |
| BR-NWS-120 | P1 | Calculation | Event-type keyword lists differ between Python and Worker; substring matching misfires | yes |
| BR-NWS-121 | P1 | Calculation | Exchange normalization | yes |
| BR-NWS-122 | P1 | Calculation | Firms with targets but no disclosed method | yes |
| BR-NWS-123 | P1 | Calculation | Form 4 first-transaction selection and HTML column layout | yes |
| BR-NWS-124 | P1 | Calculation | Form 4 insider transaction code to label mapping |  |
| BR-NWS-125 | P1 | Calculation | Form 4 insider transaction value |  |
| BR-NWS-126 | P1 | Calculation | Form 4 numeric parsing (accounting negatives, currency, thousands separators) |  |
| BR-NWS-127 | P1 | Calculation | Form 4 reporting owner role derivation |  |
| BR-NWS-128 | P1 | Calculation | Form 4 transaction value = shares x price rounded to cents | yes |
| BR-NWS-129 | P1 | Calculation | Merged multi-source news is capped to the newest N after de-duplication |  |
| BR-NWS-130 | P1 | Calculation | News event-type classification by keyword | yes |
| BR-NWS-131 | P1 | Calculation | Price target currency code mapping | yes |
| BR-NWS-132 | P1 | Calculation | Requested vs attempted source accounting |  |
| BR-NWS-133 | P1 | Calculation | SEC event URL fallback chain and URL validation (differs between runtimes) | yes |
| BR-NWS-134 | P1 | Calculation | SEC form to event type and SEC item confidence |  |
| BR-NWS-135 | P1 | Calculation | Subject alias derivation for target attribution |  |
| BR-NWS-136 | P1 | Calculation | Timestamp normalization to ISO UTC differs in precision between runtimes | yes |
| BR-NWS-137 | P1 | Calculation | Valuation multiple extraction (multiple, metric, EV flag, periods) |  |
| BR-NWS-138 | P1 | Calculation | Verify event query term threshold | yes |
| BR-NWS-139 | P1 | Calculation | Yahoo company identity aliases and acronyms | yes |
| BR-NWS-140 | P1 | Calculation | Yahoo company identity derivation (aliases and acronyms) |  |
| BR-NWS-141 | P1 | Lifecycle | Analyst valuation result status and method-not-disclosed list |  |
| BR-NWS-142 | P1 | Lifecycle | Approved IR page status and allowed action: Python and Worker disagree | yes |
| BR-NWS-143 | P1 | Lifecycle | Batch news per-ticker outcome state and partial-success metadata (Worker only) | yes |
| BR-NWS-144 | P1 | Lifecycle | Collection status for news results | yes |
| BR-NWS-145 | P1 | Lifecycle | Company IR RSS discovery lifecycle (rssDiscovery state) |  |
| BR-NWS-146 | P1 | Lifecycle | Company IR page status and allowed action mapping |  |
| BR-NWS-147 | P1 | Lifecycle | Empty press-release result reports a 'no release' status even when a source failed | yes |
| BR-NWS-148 | P1 | Lifecycle | Empty press-release status when only the IR-page registry is selected: Worker separates 'candidate only' from 'not approved' | yes |
| BR-NWS-149 | P1 | Lifecycle | Empty-result collection status treats IR page registry states differently per runtime | yes |
| BR-NWS-150 | P1 | Lifecycle | Expanded institutional ownership provider state machine |  |
| BR-NWS-151 | P1 | Lifecycle | Explicit source list selecting nothing forces SOURCE_LIMITED_NOT_FOUND |  |
| BR-NWS-152 | P1 | Lifecycle | Explicit source list with no supported source reads as source-limited |  |
| BR-NWS-153 | P1 | Lifecycle | Finnhub status state machine (eligibility, configuration, HTTP outcome, truncation) | yes |
| BR-NWS-154 | P1 | Lifecycle | GlobeNewswire (newswire) source used/failed determination |  |
| BR-NWS-155 | P1 | Lifecycle | Marketaux status mapping from HTTP and parse outcome |  |
| BR-NWS-156 | P1 | Lifecycle | One Yahoo outage warning marks every selected Yahoo sub-source as PROVIDER_ERROR |  |
| BR-NWS-157 | P1 | Lifecycle | Overall collection status |  |
| BR-NWS-158 | P1 | Lifecycle | Per-source news status: Yahoo feed state from collector completion and accepted count |  |
| BR-NWS-159 | P1 | Lifecycle | Per-source status for SEC, company_ir and newswire |  |
| BR-NWS-160 | P1 | Lifecycle | SEC source status: no CIK or submissions means provider error, not absence | yes |
| BR-NWS-161 | P1 | Lifecycle | Source coverage and recommended next action | yes |
| BR-NWS-162 | P1 | Lifecycle | Valuation method extraction status and output caps |  |
| BR-NWS-163 | P1 | Lifecycle | Verification failure-mode precedence and retryability | yes |
| BR-NWS-164 | P1 | Lifecycle | Verify event status precedence | yes |
| BR-NWS-165 | P1 | Lifecycle | Worker company_ir status precedence |  |
| BR-NWS-166 | P1 | Lifecycle | Yahoo identity unavailable degrades matching |  |
| BR-NWS-167 | P1 | Lifecycle | company_ir source status precedence (Worker) |  |
| BR-NWS-168 | P1 | Policy | Any Finnhub ineligibility or failure forces a Marketaux call | yes |
| BR-NWS-169 | P1 | Policy | Collection status classification |  |
| BR-NWS-170 | P1 | Policy | Company IR RSS/Atom autodiscovery probe paths and budget |  |
| BR-NWS-171 | P1 | Policy | Company IR registry revalidation deadline check |  |
| BR-NWS-172 | P1 | Policy | Coverage recommended next action precedence | yes |
| BR-NWS-173 | P1 | Policy | Decision-use guidance for contextual news |  |
| BR-NWS-174 | P1 | Policy | Dedupe survivor selection by source priority |  |
| BR-NWS-175 | P1 | Policy | Evidence class, URL provenance and decision-use labelling per item |  |
| BR-NWS-176 | P1 | Policy | Finnhub HTTP error classification | yes |
| BR-NWS-177 | P1 | Policy | Finnhub market eligibility gates the expanded institutional ownership tool (news policy file reused) | yes |
| BR-NWS-178 | P1 | Policy | Finnhub market eligibility policy |  |
| BR-NWS-179 | P1 | Policy | Finnhub market eligibility policy |  |
| BR-NWS-180 | P1 | Policy | Finnhub market eligibility reused for expanded ownership, failing open in Python | yes |
| BR-NWS-181 | P1 | Policy | Finnhub requires API key; otherwise UNCONFIGURED |  |
| BR-NWS-182 | P1 | Policy | GlobeNewswire feed set, scan order, confidence and early stop |  |
| BR-NWS-183 | P1 | Policy | IR feed discovery budget and caps |  |
| BR-NWS-184 | P1 | Policy | Marketaux fallback gating (only when higher-priority coverage is empty or limited) |  |
| BR-NWS-185 | P1 | Policy | Marketaux fallback is query-aware for search_company_news |  |
| BR-NWS-186 | P1 | Policy | Marketaux fallback trigger (only when higher-priority sources are empty or limited) |  |
| BR-NWS-187 | P1 | Policy | Marketaux free-tier quota cap |  |
| BR-NWS-188 | P1 | Policy | Marketaux quota and acceptance rules |  |
| BR-NWS-189 | P1 | Policy | Marketaux quota and request bounds |  |
| BR-NWS-190 | P1 | Policy | Marketaux wire fallback trigger |  |
| BR-NWS-191 | P1 | Policy | Provider failure to recommended-action mapping (inverted Alpha flag) |  |
| BR-NWS-192 | P1 | Policy | Recommended next action |  |
| BR-NWS-193 | P1 | Policy | Remembered provider denial window (Worker only) | yes |
| BR-NWS-194 | P1 | Policy | Result count and lookback clamping |  |
| BR-NWS-195 | P1 | Policy | SEC event collection stops at the result cap in filing order |  |
| BR-NWS-196 | P1 | Policy | Source coverage FULL vs PARTIAL |  |
| BR-NWS-197 | P1 | Policy | Verification failure-mode classification and retryability |  |
| BR-NWS-198 | P1 | Policy | Which Yahoo items are kept depends on the selected Yahoo source names |  |
| BR-NWS-199 | P1 | Policy | Yahoo news feed supplies at most 20 raw items per fetch |  |
| BR-NWS-200 | P1 | Policy | extract_analyst_valuation_methods inputs, window and warnings | yes |
| BR-NWS-201 | P1 | Validation | Analyst valuation window clamp and data-availability warnings | yes |
| BR-NWS-202 | P1 | Validation | Approved IR page HTML item extraction rules |  |
| BR-NWS-203 | P1 | Validation | Approved IR page content-type must match adapter (Worker only) | yes |
| BR-NWS-204 | P1 | Validation | Batch news ticker count limit | yes |
| BR-NWS-205 | P1 | Validation | Batch news ticker list must be non-empty with no blank symbols |  |
| BR-NWS-206 | P1 | Validation | Broker attribution of a news item |  |
| BR-NWS-207 | P1 | Validation | Company IR feed and item acceptance by domain or identity |  |
| BR-NWS-208 | P1 | Validation | Company IR page fetch size and redirect limits |  |
| BR-NWS-209 | P1 | Validation | Company website must be an official domain (blocked aggregator hosts) |  |
| BR-NWS-210 | P1 | Validation | Company website must be an official, non-aggregator domain |  |
| BR-NWS-211 | P1 | Validation | Company-name token matching for IR identity |  |
| BR-NWS-212 | P1 | Validation | Date window filter |  |
| BR-NWS-213 | P1 | Validation | Date window filter with lookback | yes |
| BR-NWS-214 | P1 | Validation | Declared news output schema allows status only OK/PARTIAL, but tools emit many other statuses and never OK | yes |
| BR-NWS-215 | P1 | Validation | Default SEC event filing types use 'DEF14A', which never matches EDGAR's 'DEF 14A' | yes |
| BR-NWS-216 | P1 | Validation | Default SEC form set for news collection, with exact form-string matching | yes |
| BR-NWS-217 | P1 | Validation | Exchange-prefix compatibility for ticker mentions | yes |
| BR-NWS-218 | P1 | Validation | Exchange-prefixed ticker mention compatibility |  |
| BR-NWS-219 | P1 | Validation | Finnhub item acceptance |  |
| BR-NWS-220 | P1 | Validation | Finnhub row acceptance and date window | yes |
| BR-NWS-221 | P1 | Validation | Form 4 date normalization |  |
| BR-NWS-222 | P1 | Validation | GlobeNewswire exact stock-category ticker match |  |
| BR-NWS-223 | P1 | Validation | GlobeNewswire stock-category exchange check (Worker only), with NYSE American misclassified | yes |
| BR-NWS-224 | P1 | Validation | GlobeNewswire stock-category match requires a compatible exchange prefix (Worker only) | yes |
| BR-NWS-225 | P1 | Validation | GlobeNewswire ticker relevance by stock category | yes |
| BR-NWS-226 | P1 | Validation | Issuer names used for valuation attribution differ by runtime | yes |
| BR-NWS-227 | P1 | Validation | Issuer-own press release qualification |  |
| BR-NWS-228 | P1 | Validation | Linked external company IR feed must match company identity |  |
| BR-NWS-229 | P1 | Validation | Marketaux item acceptance filters |  |
| BR-NWS-230 | P1 | Validation | Marketaux row acceptance rules |  |
| BR-NWS-231 | P1 | Validation | Press release candidate source-type allow-list |  |
| BR-NWS-232 | P1 | Validation | Price target and method must belong to the subject company |  |
| BR-NWS-233 | P1 | Validation | Provider HTTP/message status classification | yes |
| BR-NWS-234 | P1 | Validation | Query / event_query is required |  |
| BR-NWS-235 | P1 | Validation | SEC archive URL safety and XBRL document detection |  |
| BR-NWS-236 | P1 | Validation | SEC filing-type filter is an exact form match, so the default 'DEF14A' never matches EDGAR proxy filings | yes |
| BR-NWS-237 | P1 | Validation | SEC recent events requires at least one filing type |  |
| BR-NWS-238 | P1 | Validation | Source list normalization and unsupported-source warning |  |
| BR-NWS-239 | P1 | Validation | Target and method attribution to the subject company (CLAUSE / ITEM / rejected) |  |
| BR-NWS-240 | P1 | Validation | Ticker format validation for news and event tools exists only in Python | yes |
| BR-NWS-241 | P1 | Validation | Ticker symbol format validation |  |
| BR-NWS-242 | P1 | Validation | Verify low-relevance downgrade | yes |
| BR-NWS-243 | P1 | Validation | Yahoo news issuer matching and rejection reasons |  |
| BR-NWS-244 | P1 | Validation | Yahoo news item acceptance and match ranking |  |
| BR-NWS-245 | P1 | Validation | Yahoo news relevance gate and match ranking |  |
| BR-NWS-246 | P1 | Validation | Yahoo news-feed items are routed to the output by the requested source label | yes |
| BR-NWS-247 | P1 | Validation | Yahoo press release must be the issuer's own wire release |  |
| BR-NWS-248 | P1 | Validation | search_company_news always caps results at the last 14 days, even with an explicit start_date | yes |
| BR-NWS-249 | P2 | Calculation | Company IR page item confidence and summary length |  |
| BR-NWS-250 | P2 | Calculation | Daily news-quality audit: Arsenal-20 scope and title overlap ratio |  |
| BR-NWS-251 | P2 | Calculation | Dedupe title stem: HTML entities decoded in Python only |  |
| BR-NWS-252 | P2 | Calculation | Duplicate merge records alternates and flags timestamp variance without affecting status |  |
| BR-NWS-253 | P2 | Calculation | Event type keyword classification | yes |
| BR-NWS-254 | P2 | Calculation | Evidence class, ticker match and URL provenance labels |  |
| BR-NWS-255 | P2 | Calculation | Form 4 item title and summary rewrite |  |
| BR-NWS-256 | P2 | Calculation | Item summary and evidence text length limits |  |
| BR-NWS-257 | P2 | Calculation | Legacy yahoo_finance aggregate diagnostics double-count the same feed | yes |
| BR-NWS-258 | P2 | Calculation | News company identity confidence grade |  |
| BR-NWS-259 | P2 | Calculation | Qualitative valuation method keywords |  |
| BR-NWS-260 | P2 | Calculation | Returned news ordered by ticker relevance |  |
| BR-NWS-261 | P2 | Calculation | SEC filing event confidence and published-at estimation |  |
| BR-NWS-262 | P2 | Calculation | Yahoo item initial confidence and ticker relevance | yes |
| BR-NWS-263 | P2 | Lifecycle | Company IR page registry: all shipped entries are unapproved candidates |  |
| BR-NWS-264 | P2 | Policy | News feed cache freshness |  |
| BR-NWS-265 | P2 | Policy | News feed cache lifetime |  |
| BR-NWS-266 | P2 | Policy | News relevance ranking and wider pool |  |
| BR-NWS-267 | P2 | Policy | Press-release payload: warning de-duplication, doctrineUse and basis text differ between runtimes | yes |
| BR-NWS-268 | P2 | Policy | Public event timeline ordering, cap and fixed one-year lookback | yes |
| BR-NWS-269 | P2 | Policy | Requested vs attempted sources reporting |  |
| BR-NWS-270 | P2 | Policy | Warnings de-duplicated by code and message |  |
| BR-NWS-271 | P2 | Policy | get_yahoo_finance_news alias uses a fixed source set without Marketaux |  |
| BR-NWS-272 | P2 | Validation | GlobeNewswire XML safety guard |  |
| BR-NWS-273 | P2 | Validation | GlobeNewswire undated-item and weak-dedupe warnings can never fire | yes |
| BR-NWS-274 | P2 | Validation | News item eligibility, de-duplication and text assembly for valuation evidence |  |
| BR-NWS-275 | P2 | Validation | News request bounds and default sources |  |
| BR-NWS-276 | P2 | Validation | Public event timeline requires timestamps |  |
| BR-NWS-277 | P2 | Validation | Result size and lookback clamping | yes |
| BR-NWS-278 | P2 | Validation | Timestamp normalization output format differs between runtimes | yes |
| BR-NWS-279 | P2 | Validation | Worker requires non-blank query/event_query |  |
| BR-NWS-280 | P2 | Validation | Yahoo content-type filter | yes |

## Cards

### BR-NWS-101 · Approved company IR page registry lifecycle (P0, Lifecycle, confidence Medium)

- **Source:** `server.py:1120-1166; server.py:957-967; server.py:2360-2387`
- **Rule:** A ticker's IR page entry moves through these states. Missing gives NOT_REGISTERED; candidate gives CANDIDATE_AVAILABLE (not fetched, decisionGrade false); disabled gives DISABLED; approved but past revalidateAfter gives REVALIDATION_DUE (skipped); fetch or allow-list failure gives SOURCE_VALIDATION_FAILED; a successful fetch gives APPROVED_SOURCE_OK, or EMPTY_RESULT if nothing falls in the window.
- **Given** registry entry IQE.L with status 'candidate' (registryVersion 2026-09-25.1200)
- **When** company_ir_page is requested for IQE.L
- **Then** no fetch happens; warning COMPANY_IR_PAGE_CANDIDATE_AVAILABLE; status CANDIDATE_AVAILABLE; allowedAction review_and_promote; coverage PARTIAL
- **Parameters:** allowedAction: APPROVED_SOURCE_OK->fetch_configured_source, CANDIDATE_AVAILABLE->review_and_promote, else registry_review_required; fetch only https and allowedHosts+allowedPathPrefixes; max body 750 KB; timeout 15s; HTML parsing capped at 50 blocks; titles under 6 chars dropped
- **Edge cases:** A redirect outside the allow-list raises an error and becomes SOURCE_VALIDATION_FAILED; An unparseable revalidateAfter counts as not fresh, which gives REVALIDATION_DUE
- **Suspected defect:** Fail-open approval: server.py:1138-1153 checks only for disabled/candidate, and _company_ir_page_item hardcodes approvalStatus 'approved' (server.py:996). That item then earns decisionUse USE_OFFICIAL_EVIDENCE (server.py:303). Parity: TS gives DISABLED allowedAction 'none', REVALIDATION_DUE 'revalidate' and SOURCE_VALIDATION_FAILED 'review_registry_entry' (worker/src/yahoo-finance.ts:10224,10247,10272), while Python gives all three 'registry_review_required'.
- **SME question:** Any status other than 'disabled' or 'candidate' (e.g. 'rejected', blank, or a typo) is treated as approved, fetched, and its items stamped approvalStatus='approved'. An entry with no revalidateAfter is fresh forever. Is that intended, or should only status=='approved' with reviewer metadata be fetched?

### BR-NWS-102 · Company IR page registry approval lifecycle (P0, Lifecycle, confidence Medium)

- **Source:** `server.py:1129-1166,2360-2387`
- **Rule:** An IR page is fetched only if its registry entry is approved and not past its revalidation date. Candidate entries are surfaced for review but not fetched. Disabled or overdue entries are skipped. Fetch or parse failures mark it SOURCE_VALIDATION_FAILED.
- **Given** Registry entry for XYZ with status 'candidate'
- **When** company_ir_page is collected
- **Then** Nothing is fetched. Warning COMPANY_IR_PAGE_CANDIDATE_AVAILABLE with candidate decisionGrade=false, sourceStatus CANDIDATE_AVAILABLE, allowedAction review_and_promote
- **Parameters:** States: (no entry)->NOT_REGISTERED, disabled->DISABLED, candidate->CANDIDATE_AVAILABLE, approved+past revalidateAfter->REVALIDATION_DUE, approved+fetch error->SOURCE_VALIDATION_FAILED, approved+ok->APPROVED_SOURCE_OK
- **Edge cases:** A missing revalidateAfter counts as fresh, while an unparseable revalidateAfter counts as stale
- **SME question:** P0 panel doubts spec fidelity: P0 holds, but only on data-integrity grounds and only for the gate itself. The rule decides whether content from an unreviewed or out-of-date issuer web page enters the event feed. An auditor would care if candidate, disabled or overdue entries quietly started being fetched and shown as evidence. The candidate payload's decisionGrade=false flag and the https allow-list fetch guard (server.py:986-997, 1136-1159) are the integrity controls here. No money moves and no regulation is enforced. The status labels and allowedAction strings are output-contract parity, not integrity, so they should not inflate the P0 surface.

Not faithful as written, for four reasons:
(1) The citation is mostly wrong. server.py:1129-1166 is the tail of the HTML parser plus the _fetch_company_ir_page helper. server.py:2360-2387 is Finnhub/Marketaux diagnostics. The real lifecycle logic is at server.py:1162-1208 (_collect_company_ir_page_events) and server.py:2406-2433 (status and allowedAction mapping), with helpers at server.py:973-979 and 999-1009.
(2) "Fetched only if approved" is false. The code never checks status == 'approved'. Only 'disabled' (1180) and 'candidate' (1182) are blocked, so any other status, including an empty or unknown one, is fetched if it passes the freshness check.
(3) The freshness check passes when revalidateAfter is missing (1001-1002) and fails when the date can't be parsed (1008-1009). The rule mentions neither.
(4) An approved entry whose f

### BR-NWS-103 · Legacy company_ir source: Python serves it from Yahoo news, Worker from official RSS discovery (P0, Lifecycle, confidence Medium)

- **Source:** `server.py:2517-2522; server.py:2608-2627; server.py:2243; server.py:2399-2405; worker/src/yahoo-finance.ts:11769-11775; worker/src/yahoo-finance.ts:11785-11789; worker/src/yahoo-finance.ts:10972-10994`
- **Rule:** When a caller selects 'company_ir', the Python server fetches the Yahoo news tab and returns Yahoo items under it, while the Worker runs company-website RSS/Atom discovery. The resulting items, evidence class and status are different.
- **Given** sources=['company_ir'] for AAPL
- **When** Python handles the request
- **Then** Yahoo news/press items (sourceType yahoo_finance_news) are returned and 'company_ir' is marked used. The status counts only sourceType company_ir/press_release items, so sourceStatus.company_ir = EMPTY_RESULT even though items were returned, or PROVIDER_ERROR if any Yahoo warning exists. The Worker returns company_ir RSS items (evidenceClass OFFICIAL_RSS_ATOM) with statuses such as WEBSITE_NOT_AVAILABLE or FEED_NOT_FOUND.
- **Parameters:** Python company_ir item types: company_ir, press_release
- **Edge cases:** Python never emits WEBSITE_NOT_AVAILABLE / DISCOVERY_* / FEED_NOT_FOUND / PARSE_ERROR, although its coverage code recognises them; Contextual Yahoo news is presented under a source name that implies official IR evidence
- **Suspected defect:** Python/TS parity break that violates the project's strict output-parity rule. Python labels Yahoo contextual news as the company_ir source and reports EMPTY_RESULT while returning items.
- **SME question:** Which behaviour is canonical for company_ir: Worker RSS/Atom discovery, or the Python Yahoo-feed fallback? Python needs a port or the alias must be redefined (planner decision).

### BR-NWS-104 · Evidence class, ticker match and decision use labeling (P0, Policy, confidence High)

- **Source:** `server.py:269-311,427-430`
- **Rule:** Every returned item gets an evidence class and a decisionUse. Only resolved SEC EX-99 exhibits and approved IR-page items are USE_OFFICIAL_EVIDENCE. Other official or wire sources are CHECK_OFFICIAL_RELEASES. Yahoo and Finnhub news is CHECK_OFFICIAL_RELEASES only for material event types, otherwise CONTEXT_ONLY.
- **Given** A Finnhub item (sourceType company_news) with eventType 'analyst'
- **When** it is enriched
- **Then** evidenceClass CONTEXTUAL_NEWS, decisionUse CONTEXT_ONLY, urlProvenance PUBLISHER (or PROVIDER if the host is finance.yahoo.com/finnhub.io)
- **Parameters:** Material Yahoo event types: earnings, guidance, contract, financing, product, regulatory, litigation
- **Edge cases:** tickerMatch is EXPLICIT if matchBasis is one of the explicit types or the ticker word appears; SOURCE_SCOPED if a tickers list exists; else UNVERIFIED. Since every item carries tickers=[ticker], UNVERIFIED is effectively unreachable

### BR-NWS-105 · Press release decision-grade and status precedence (P0, Policy, confidence High)

- **Source:** `server.py:2858-3027`
- **Rule:** A press release result is decision-grade only when an 8-K's EX-99.1 exhibit is resolved or an approved IR-page item with a URL is found. Status precedence is SEC_EX99_FOUND, then APPROVED_IR_PAGE_FOUND, then SEC_8K_FOUND_EX99_NOT_FOUND, then a source-specific not-found status.
- **Given** Defaults (sec, company_ir_page, yahoo_finance_press_releases). One 8-K is found whose EX-99.1 does not resolve, and there is no approved IR page
- **When** get_company_press_releases runs
- **Then** status=SEC_8K_FOUND_EX99_NOT_FOUND, coverageStatus same, decisionGrade=false, failureMode=SEC_8K_FOUND_EX99_NOT_FOUND, and warning SEC_8K_FOUND_EX99_NOT_FOUND with filingsSearched=1
- **Parameters:** SEC types limited to 8-K; EX-99.1 resolution concurrency 4; secEvidence and irPageEvidence capped at 10; default lookback 90, max 20
- **Edge cases:** With no release items: NO_YAHOO_PRESS_RELEASE if Yahoo PR was selected, else COMPANY_IR_PAGE_NOT_APPROVED, else COMPANY_IR_NOT_FOUND, else NOT_FOUND; A resolved EX-99.1 rewrites the item to sourceType sec_ex99_found with confidence HIGH

### BR-NWS-106 · Verify NOT_FOUND upgraded to SOURCE_LIMITED_NOT_FOUND (P0, Policy, confidence Medium)

- **Source:** `server.py:2411-2439,3384-3394`
- **Rule:** If nothing was found but coverage was incomplete, the result becomes SOURCE_LIMITED_NOT_FOUND with a failure mode chosen in priority order. It is retryable only for subrequest limits, rate limits and timeouts.
- **Given** No matches. Finnhub status RATE_LIMITED
- **When** verification finishes
- **Then** status=SOURCE_LIMITED_NOT_FOUND, failureMode=RATE_LIMITED, retryable=true, plus a recommendedAction to retry without the failed providers
- **Parameters:** Failure-mode order: WORKER_SUBREQUEST_LIMIT (warning text 'too many subrequests'), RATE_LIMITED, PROVIDER_TIMEOUT, PROVIDER_CHANGED, YAHOO_IDENTITY_UNAVAILABLE, SOURCE_UNCONFIGURED, SOURCE_NOT_ELIGIBLE, PROVIDER_ERROR
- **Edge cases:** IR-page registry states (NOT_REGISTERED, etc.) do not trigger the upgrade here even though they make coverage PARTIAL
- **SME question:** P0 panel doubts spec fidelity: P0 is justified on data integrity and compliance grounds. The rule stops a search with incomplete provider coverage (rate limit, timeout, subrequest cap) from being reported as confirmed absence (NOT_FOUND) of an SEC filing, earnings or corporate event. A silent regression would turn 'the search failed' into 'the event didn't happen', which auditors and controllers would care about. It also sets the failureMode and retryable output contract fields. However, the citation is wrong. server.py:2411-2439 is the company_ir_page/newswire source-status mapping and server.py:3384-3394 is ticker/company-name relevance scoring. Neither contains the NOT_FOUND upgrade, the failure-mode order or the retryable check. The rule text matches the real code, which is at server.py:2458-2486 (_verify_coverage_failure_mode). The order there matches the card exactly: 'too many subrequests' in SOURCE_UNAVAILABLE warnings, then RATE_LIMITED, TIMEOUT->PROVIDER_TIMEOUT, PROVIDER_CHANGED, IDENTITY_UNAVAILABLE->YAHOO_IDENTITY_UNAVAILABLE, UNCONFIGURED, NOT_ELIGIBLE, PROVIDER_ERROR. The rest is at server.py:3436-3473: the upgrade at 3437, retryable limited to WORKER_SUBREQUEST_LIMIT/RATE_LIMITED/PROVIDER_TIMEOUT at 3446, and the recommendedAction to retry without the failed providers at 3470-3473. Finnhub RATE_LIMITED is set at server.py:1961 (HTTP 402/429) and 2361, so the example scenario holds. The citation must be corrected to 2458-2486 and 3436-3473 before this becomes a

### BR-NWS-107 · company_ir source means different things in Python and the Worker (P0, Policy, confidence Medium)

- **Source:** `server.py:2617-2626; server.py:2399-2405; worker/src/yahoo-finance.ts:11769-11775, 11786-11789`
- **Rule:** In the Worker, company_ir is official RSS/Atom autodiscovery on the company website. In Python, selecting company_ir marks the source as used and passes through Yahoo news and press-release items instead.
- **Given** sources=['company_ir'] for ticker AAPL
- **When** get_company_news or verify_company_event runs
- **Then** Worker: items come only from company-domain or identity-matched feeds, and status follows the RSS discovery outcome. Python: Yahoo feed items are returned, sourcesUsed includes company_ir, and company_ir status is OK/EMPTY_RESULT based on items with sourceType company_ir/press_release.
- **Parameters:** none
- **Edge cases:** The Python get_company_news docstring says company_ir is 'RSS/Atom only', which contradicts the Python code
- **Suspected defect:** Python returns Yahoo context items under a source that downstream status logic treats as official (company_ir is in _OFFICIAL_SOURCE_TYPES), which breaks the runtime output-parity requirement.
- **SME question:** Should Python implement the Worker's RSS/Atom autodiscovery for company_ir, or should company_ir in Python stop reporting itself as used when it only relays Yahoo items?

### BR-NWS-108 · Company IR page URL allow-list (P0, Validation, confidence High)

- **Source:** `server.py:944-954,1094-1117,980-982`
- **Rule:** IR page fetches and item links must be https, on a host in the entry's allowedHosts (ignoring a leading www.), and under one of its allowedPathPrefixes. Off-list redirects or items are refused, and responses over 750 KiB are refused.
- **Given** Entry allowedHosts ['investor.acme.com'], prefixes ['/news']. An item links to http://investor.acme.com/news/x
- **When** the item is built
- **Then** The item is dropped (scheme is not https). A redirect to https://evil.com fails the whole source with COMPANY_IR_PAGE_VALIDATION_FAILED
- **Parameters:** Max size 750*1024 bytes; timeout 15s; HTML parsing limited to the first 50 blocks; titles under 6 characters skipped

### BR-NWS-109 · EX-99.1 press-release exhibit identification and filer CIK selection (P0, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:12093-12119, 16926-16949, 5397-5407; server.py:2915-2950; yfmcp/tools/earnings.py:172-188`
- **Rule:** An 8-K is upgraded to a decision-grade press release (sec_ex99_found) only when an EX-99.1 exhibit is found. The two runtimes find the exhibit and the CIK in different ways.
- **Given** An 8-K whose index lists an exhibit typed 'EX-99' described as 'Press Release', and an item URL without /data/<cik>/ but with cikInt=320193
- **When** get_company_press_releases runs
- **Then** Worker: CIK comes from item cikInt, then the URL, then the accession prefix. The exhibit matches if its type starts with EX-99.1/EX-99.01, or its type starts with EX-99 and the description says PRESS/EARNINGS/RESULTS RELEASE, or the filename matches ex99_1, with an HTML index scan as fallback. Result: SEC_EX99_FOUND and decisionGrade=true. Python: CIK comes only from the URL /data/<cik>/ and the exhibit type must be exactly EX-99.1, EX-99 or 99.1. Result: SEC_8K_FOUND_EX99_NOT_FOUND and decisionGrade=false.
- **Parameters:** Python EX-99.1 concurrency 4; Worker HTML index read cap 100,000 bytes
- **Edge cases:** Python accepts a bare 'EX-99' type of any description; the Worker requires release wording for plain EX-99; The Worker uses the accession prefix CIK last because it identifies the filer agent
- **Suspected defect:** Python/TS divergence on the decision-grade gate: the same filing can produce decisionGrade true in one runtime and false in the other.

### BR-NWS-110 · Analyst price target extraction from news sentences (four patterns, fixed precedence) (P1, Calculation, confidence High)

- **Source:** `yfmcp/capital_structure.py:3085-3092,3124-3125,3209-3231; worker/src/capital-structure.ts:2825-2832,2856-2858,2919-2934`
- **Rule:** A price target is read from a news sentence by trying four phrasings in order: 'target from X to Y', 'target to Y from X', '$Y price target', then 'price target ... Y'. A number followed by '%' is never treated as a target.
- **Given** A sentence 'Needham raises its price target from $80 to $95' (or 'PT to 120p from 100p', or 'Cantor reiterates $122 target', or 'price target implies 25% upside')
- **When** The extractor looks for a price target
- **Then** Results: target 95, prior 80, currency USD; target 120, prior 100, currency GBp; target 122, prior null, USD; the '25% upside' sentence gives no target. Thousands separators are removed ('1,250' becomes 1250).
- **Parameters:** Trigger-to-number gap: 40 chars; precedence FROM_TO > TO_FROM > BEFORE > TO; percent lookahead (?![\d.,]*\s?%)
- **Edge cases:** The percent guard rejects '25%' even when backtracking to the '2'; TARGET_BEFORE accepts only $, US$, GBP sign, EUR sign and NT$ symbols; 'SEK 92 target' falls through to the generic pattern; The regexes ignore case, but currency mapping does not: 'usd 95' captures a currency token that maps to null

### BR-NWS-111 · Analyst price target parsing precedence and currency (P1, Calculation, confidence High)

- **Source:** `worker/src/capital-structure.ts:2825-2832,2839-2854,2919-2934; yfmcp/capital_structure.py:3209-3231`
- **Rule:** A price target is read from a sentence in fixed order: 'target from X to Y', then 'target to Y from X', then '$N price target', then 'target ... N'; a number followed by % is never a target, and the currency is inferred from the symbol or a pence/kr suffix.
- **Given** Sentence 'Cantor raises price target from $100 to $122'
- **When** the target parser runs
- **Then** priceTarget = {target:122, prior:100, currency:'USD'}
- **Parameters:** Trigger phrases: price target, target price, PT, price objective; max 40-char gap between phrase and number; currency map $/US$/USD->USD, GBP/£->GBP, GBp/GBX/p/pence->GBp, €/EUR->EUR, SEK/kr->SEK, NT$/TWD->TWD; anything else -> null
- **Edge cases:** Thousands separators are stripped before parsing; TARGET_BEFORE only recognises $, US$, £, €, NT$ symbols (not codes like USD); An unrecognised currency symbol yields currency null rather than an error

### BR-NWS-112 · Brokerage firm attribution for a news item (P1, Calculation, confidence High)

- **Source:** `yfmcp/capital_structure.py:3074-3083,3234-3243,3304-3305,3318; worker/src/capital-structure.ts:2814-2823,2936-2946,2995-2996,3008`
- **Rule:** Each news item is credited to the analyst firm named earliest in its text. Firms from recent rating changes are searched first, then a fixed list of about 70 brokers. Matching is case-sensitive.
- **Given** Rating changes name 'Cantor Fitzgerald', and an item reads 'Morgan Stanley and Cantor Fitzgerald lift targets'
- **When** the item's firm is chosen
- **Then** firm = 'Morgan Stanley' (earliest match position). On a tie at the same position, the longer name wins ('Bernstein SocGen' over 'Bernstein').
- **Parameters:** Built-in broker list of about 70 names (Morgan Stanley ... Arctic)
- **Edge cases:** 'benchmark' in lower case does not match the firm Benchmark; One firm per item: an item quoting two brokers credits every sentence to the first-named one

### BR-NWS-113 · Company IR page record extraction (JSON vs HTML) (P1, Calculation, confidence High)

- **Source:** `server.py:1053-1133,1197-1205`
- **Rule:** Approved IR pages are parsed as JSON when the adapter is 'structured' or the content type contains json, and as HTML otherwise. In HTML, article blocks are used; if there are none, list items mentioning release keywords are used. Only the first 50 blocks are read, and titles shorter than 6 characters are dropped.
- **Given** An HTML IR page with no <article> tags and 80 <li> items, 60 of which mention 'announces'
- **When** IR page events are parsed
- **Then** Only the first 50 qualifying <li> blocks are examined. Each title comes from the first h1-h4 heading, or failing that the first anchor's text. Its date comes from <time datetime> or the time element's text. Items are then filtered by the date window and truncated to max_results
- **Parameters:** Block cap 50; minimum title length 6; li keywords: press release, news release, earnings, results, announces, reports. JSON list keys tried in order: items, releases, pressReleases, news, data, feed.items, data.* values. JSON title keys: title/headline/name. JSON date keys: publishedAt/published_at/datePublished/date_published/date/pubDate
- **Edge cases:** JSON records without a title are skipped; Items whose resolved URL fails the allow-list are dropped; Items without a publish date always fail the date window and are excluded

### BR-NWS-114 · Duplicate group id hashing (P1, Calculation, confidence High)

- **Source:** `server.py:693-706`
- **Rule:** A duplicate group id is the first 16 hex characters of SHA-256 of 'normalized title stem|publish day|upper-cased issuer or ticker'. No id is produced when both the title stem and the day are empty.
- **Given** ticker 'ACME', title 'ACME Corp Announces Q3 Results', publishedAt '2026-10-01T13:00:00Z', issuer 'Acme Corp'
- **When** The duplicate group id is computed
- **Then** key = 'announces q3 results|2026-10-01|ACME CORP' and id = sha256(key)[:16]
- **Parameters:** Hash length 16 hex characters; the day is the first 10 characters of publishedAt
- **Edge cases:** The url argument is accepted but not used in the key; The same story from different issuers' feeds hashes differently if the issuer strings differ

### BR-NWS-115 · Duplicate group key (P1, Calculation, confidence High)

- **Source:** `server.py:228,616-628,651-664`
- **Rule:** Items are treated as the same event when they share the normalized headline (first 80 characters, with the ticker, URLs and boilerplate words removed), the publish date, and the issuer (or the ticker if there is no issuer).
- **Given** 'NVIDIA Corp Announces Q3 Results - GlobeNewswire' and 'Nvidia announces Q3 results', both on 2026-10-10, no issuer
- **When** group ids are computed
- **Then** Both produce stem 'nvidia announces q3 results' (corp/globenewswire removed), day 2026-10-10, entity NVDA, so the sha256 prefixes are identical (16 hex characters)
- **Parameters:** _DEDUP_TITLE_MAX_LEN=80; stop terms yahoo, finance, finnhub, globenewswire, press, release, inc, corp, corporation, company, plc, ltd, llc
- **Edge cases:** If both the title stem and the date are empty, there is no key, a DEDUPE_WEAK_KEY warning is raised, and the item passes through undeduped; An issuer set on one copy but not the other gives different keys

### BR-NWS-116 · Duplicate news collapse by source priority (P1, Calculation, confidence High)

- **Source:** `server.py:651-664; server.py:2089-2127; server.py:238-251`
- **Rule:** Items sharing a normalised-title + day + entity key are merged. The higher-priority source is kept (SEC 0 > IR 1 > press release 2 > newswire 3 > Finnhub 4 > Yahoo news 5 > Marketaux 6 > other 7), with the newer timestamp breaking ties. The losers are kept as sourceRefs, and a non-blocking TIMESTAMP_VARIANCE warning is raised when timestamps differ.
- **Given** the same headline on 2026-10-01 from Finnhub (rank 4) and Yahoo news (rank 5)
- **When** the items are deduped
- **Then** the Finnhub item is kept with the Yahoo item in sourceRefs. Results are sorted newest first
- **Parameters:** title stem max 80 chars; key hash sha256 truncated to 16 hex characters; stop terms yahoo, finance, finnhub, globenewswire, press, release, inc, corp ...
- **Edge cases:** Items without a duplicateGroupId pass through un-deduped; Because the key includes the issuer, Yahoo items (issuer None, then the ticker) and SEC items (issuer = SEC company name) rarely collide

### BR-NWS-117 · Duplicate resolution by source priority (P1, Calculation, confidence High)

- **Source:** `server.py:238-251,889-890,2089-2127; worker/src/yahoo-finance.ts:9088-9101,10691-10730`
- **Rule:** When two items describe the same story, keep the one from the more authoritative source (SEC > company IR > press release > newswire > Finnhub > Yahoo news > Marketaux > other); on a tie keep the later timestamp, and record the loser as a sourceRef.
- **Given** A Finnhub item (company_news, rank 4) and a Yahoo press-release item (rank 2) sharing duplicateGroupId abc, published 10:00Z and 10:05Z
- **When** Items are deduplicated
- **Then** The Yahoo press-release item is kept, Finnhub details appended to its sourceRefs, a TIMESTAMP_VARIANCE warning (statusImpact false) is emitted, and the final list is sorted newest publishedAt first
- **Parameters:** Priority: sec_filing 0, sec_ex99_found 0, company_ir 1, company_ir_page 1, press_release 2, yahoo_finance_press_releases 2, newswire 3, company_news 4, yahoo_finance 5, yahoo_finance_news 5, marketaux_wire 6, other 7
- **Edge cases:** Items with no duplicateGroupId bypass dedupe; Unknown sourceType gets rank 7; Timestamp comparison is lexical on ISO strings

### BR-NWS-118 · Duplicate title stem: HTML entities decoded only in Python, and hash differs (P1, Calculation, confidence High)

- **Source:** `server.py:658-670,693-706; worker/src/yahoo-finance.ts:9637-9648,9659-9668`
- **Rule:** The dedupe title stem is built differently: Python decodes HTML entities before normalizing, and the Worker does not. Python hashes the key with SHA-256 truncated to 16 hex characters, while the Worker uses a 32-bit rolling hash shown as 8 hex characters.
- **Given** Two copies of a headline: 'Smith &amp; Co. wins contract' (from RSS) and 'Smith & Co. wins contract' (from Yahoo), on the same day for ticker SMCO
- **When** duplicate group ids are computed
- **Then** Python gives both the stem 'smith co wins contract' and collapses them into one group. The Worker produces 'smith amp co wins contract' and 'smith co wins contract', so both copies survive dedupe. Even identical keys produce different duplicateGroupId strings across runtimes.
- **Parameters:** Stem max length 80; stop words yahoo, finance, finnhub, globenewswire, press, release, inc, corp, corporation, company, plc, ltd, llc; key = stem|YYYY-MM-DD|issuer-or-ticker
- **Edge cases:** The key is weak (null) only when both the stem and the day are empty; 32-bit hashes can collide, which merges unrelated stories in the Worker
- **Suspected defect:** Dedupe outcomes and duplicateGroupId values differ between runtimes.

### BR-NWS-119 · Duplicate-story grouping key (P1, Calculation, confidence High)

- **Source:** `server.py:616-628,651-664; worker/src/yahoo-finance.ts:9602-9633`
- **Rule:** Two news items are treated as the same story when their cleaned headline (first 80 chars), publish day and issuer (or ticker) match.
- **Given** Ticker NVDA, title 'NVIDIA Corp Reports Q2 Results - Yahoo Finance', publishedAt 2026-08-27T20:05:00Z, no issuer
- **When** The duplicate group id is computed
- **Then** Title is lowercased, URLs removed, non-alphanumerics collapsed to spaces, the ticker word and stop words (yahoo, finance, finnhub, globenewswire, press, release, inc, corp, corporation, company, plc, ltd, llc) removed, truncated to 80 chars; key = 'nvidia reports q2 results|2026-08-27|NVDA'; Python returns first 16 hex of SHA-256, TS returns a 32-bit string hash as 8 hex chars. If both stem and day are empty, no id is assigned and DEDUPE_WEAK_KEY warning is raised.
- **Parameters:** _DEDUP_TITLE_MAX_LEN = 80; stop terms list (13 words); Python hash = sha256[:16]; TS hash = Java-style 31x rolling 32-bit hash padded to 8 hex
- **Edge cases:** URL argument is accepted but ignored in Python key; Issuer differs by source (GlobeNewswire contributor vs null) so the same release from GlobeNewswire and Yahoo will not group together; Python HTML-unescapes the title before stemming (&amp; -> &), TS does not
- **Suspected defect:** Python/TS parity break: duplicateGroupId values differ in format and length (16-hex SHA-256 vs 8-hex 32-bit hash) and Python HTML-unescapes titles while TS does not, so output field values differ between runtimes; the 32-bit hash also has a materially higher collision risk.

### BR-NWS-120 · Event-type keyword lists differ between Python and Worker; substring matching misfires (P1, Calculation, confidence High)

- **Source:** `server.py:633-655; worker/src/yahoo-finance.ts:9622-9635`
- **Rule:** Event type is the first keyword category whose keywords appear anywhere in the lowercased title and summary, but the two runtimes use different keyword lists.
- **Given** The headline 'Acme announces new CEO' / 'Acme files annual report' / 'Acme closes $50M credit facility'
- **When** The event type is classified
- **Then** Python returns product / earnings / financing. The Worker returns other / regulatory ('files' contains 'file' but not 'filing'... in fact 'annual report' alone -> other) / other. Python has the extra keywords 'annual report', 'credit facility' and 'announces new'.
- **Parameters:** Keyword priority order: earnings, guidance, contract, financing, product, analyst, macro, litigation, insider, regulatory, other
- **Edge cases:** Matching is by substring, not whole word: 'eps' matches 'steps', 'sec' matches 'second', 'sector' and 'security', 'rates' matches 'operates', 'note' matches 'notebook', 'deal' matches 'dealer'; 10-Q and 10-K in the regulatory list can never be reached, because earnings is checked first
- **Suspected defect:** The runtimes disagree on keywords, which breaks the parity rule in CLAUDE.md. Substring matching assigns many headlines to the wrong type (for example 'Second-quarter steps' -> earnings, 'Sector update' -> regulatory).

### BR-NWS-121 · Exchange normalization (P1, Calculation, confidence High)

- **Source:** `server.py:1340-1354`
- **Rule:** Exchange codes are mapped to NASDAQ, NYSE, NYSEAMERICAN, TSXV or TSX using substring tests, and the order of the tests matters.
- **Given** exchange string 'NYSE American'
- **When** _normalize_exchange runs
- **Then** It returns 'NYSE' because the NYSE test runs before the AMEX/NYSE AMERICAN test
- **Parameters:** NASDAQ markers NASDAQ,NMS,NGM,NCM,GS; NYSE marker NYSE or NYQ; AMEX markers AMEX,NYSE AMERICAN,ASE; TSXV/CVE; TSX/TOR
- **Edge cases:** The 'GS' substring makes any code containing GS map to NASDAQ; 'ASE' is also a substring of other codes
- **Suspected defect:** 'NYSE AMERICAN' (and any code containing 'NYSE') normalizes to NYSE, so the NYSEAMERICAN branch is unreachable for that literal. This can cause false exchange matches or mismatches.
- **SME question:** Should NYSE American-listed tickers be distinguished from NYSE when validating exchange-prefixed ticker mentions and Marketaux entities?

### BR-NWS-122 · Firms with targets but no disclosed method (P1, Calculation, confidence Medium)

- **Source:** `yfmcp/capital_structure.py:3355-3378; worker/src/capital-structure.ts:3044-3059`
- **Rule:** Firms that issued a target without naming a method anywhere are listed once from news and once from rating changes. A rating change is listed only when its new target is above zero. Its prior target is included only when above zero.
- **Given** Rating change {firm 'Needham', ptTo 95, ptFrom 0} and a news target from Needham with no method
- **When** methodNotDisclosed is built
- **Then** Two entries for Needham: one with source 'news' and one with source 'rating_changes' (priceTarget {target 95, prior null, currency null}).
- **Parameters:** ptTo > 0 required; ptFrom > 0 to report prior
- **Edge cases:** The de-duplication keys 'news|FIRM' and 'rating|FIRM' are separate, so the same firm can appear twice
- **Suspected defect:** The same firm can be listed twice because news and rating-change entries use separate keys, which may over-count non-disclosing firms.
- **SME question:** Should a firm whose target appears in both news and rating changes be listed once (merged) or twice (one per source) in methodNotDisclosed?

### BR-NWS-123 · Form 4 first-transaction selection and HTML column layout (P1, Calculation, confidence Medium)

- **Source:** `server.py:795-836,862-896; worker/src/yahoo-finance.ts:9762-9829`
- **Rule:** Only one transaction is reported per Form 4: the first non-derivative transaction, or the first derivative transaction if there are none. In the HTML fallback, columns are read by fixed position.
- **Given** A Form 4 with 3 non-derivative rows and 1 derivative row
- **When** The filing is parsed
- **Then** Only the first non-derivative row with a code and a numeric share count is used. HTML non-derivative positions: date col1, code col3, shares col5, price col7, ownership col9 (row must have 10+ cells). HTML derivative positions: price col1, date col2, code col4, shares col6, ownership col14 (row must have 15+ cells)
- **Parameters:** Minimum row widths 10 and 15; document fetch cap 2,000,000 bytes
- **Edge cases:** Filings with several transactions (for example a sale plus a tax withholding) report only the first one; Derivative 'price' is read from column 1, which on Form 4 is the conversion/exercise price, not the transaction price
- **Suspected defect:** Python and TS fall back to HTML under different conditions. Python uses the HTML parser only when XML parsing raises an exception: well-formed XML with no transaction node returns None. TS uses the HTML parser whenever no transaction block is found. Well-formed XHTML primary documents can therefore produce different results in the two runtimes.
- **SME question:** Should a multi-transaction Form 4 report only its first row, and is the derivative conversion/exercise price (column 1) the right 'price' for computing the derivative transaction value?

### BR-NWS-124 · Form 4 insider transaction code to label mapping (P1, Calculation, confidence High)

- **Source:** `server.py:721-729,839-859; worker/src/yahoo-finance.ts:9686-9694,9718-9738`
- **Rule:** Each SEC Form 4 transaction code becomes a readable label: P=Purchase, S=Sale, A=Grant/Award, D=Disposition, M=Option exercise/conversion, F=Tax withholding/payment, G=Gift. Any other code becomes 'Other/Unclassified'.
- **Given** A parsed Form 4 transaction with transactionCode 'p' (lowercase) or 'J'
- **When** The insider transaction payload is built
- **Then** 'p' is upper-cased and labelled 'Purchase'; 'J' is labelled 'Other/Unclassified'
- **Parameters:** Label map: P,S,A,D,M,F,G; fallback 'Other/Unclassified'
- **Edge cases:** Matching ignores case; A missing code gives the label 'Other/Unclassified' and transactionCode null

### BR-NWS-125 · Form 4 insider transaction value (P1, Calculation, confidence High)

- **Source:** `server.py:679-687,797-817`
- **Rule:** For Form 4 filings, the transaction value is shares times price, rounded to cents, and the transaction code is mapped to a label.
- **Given** Code S, 10,000 shares at $152.375
- **When** the Form 4 is parsed
- **Then** value=$1,523,750.00, label 'Sale', title 'Form 4: Sale by <owner>'
- **Parameters:** Codes P Purchase, S Sale, A Grant/Award, D Disposition, M Option exercise/conversion, F Tax withholding/payment, G Gift; other codes 'Other/Unclassified'
- **Edge cases:** Only the first transaction row is used; Parenthesized numbers are read as negative

### BR-NWS-126 · Form 4 numeric parsing (accounting negatives, currency, thousands separators) (P1, Calculation, confidence Medium)

- **Source:** `server.py:732-746; worker/src/yahoo-finance.ts:9696-9707`
- **Rule:** Share counts and prices are taken from the first number in the cell. Dollar signs, commas and spaces are removed, and a value in parentheses is treated as negative.
- **Given** Form 4 cells '$1,234.50', '(500)', '<b>12</b> (1)' and 'N/A'
- **When** form4 number parsing runs
- **Then** Results are 1234.5, -500, 12 and null (the first numeric token wins; no number means null)
- **Parameters:** Regex -?\$?\s*\(?\d[\d,]*(?:\.\d+)?\)?
- **Edge cases:** Footnote markers like '(1)' after the main number are ignored because only the first match is used; A leading minus is kept; HTML tags are removed before parsing
- **SME question:** P0 panel split on whether this moves money / is regulatory (The rule card matches the code. server.py:732-746 and worker/src/yahoo-finance.ts:9696-9707 both strip HTML tags, take the first match of -?\$?\s*\(?\d[\d,]*(?:\.\d+)?\)?, remove '$', commas and whitespace, treat a value wrapped in parentheses as negative, and return null when there is no match or the number won't parse. I traced the four examples by hand and they give the stated results. '$1,234.50' gives 1234.5. '(500)' gives -500. '<b>12</b> (1)' becomes '12 (1)' after tag stripping and gives 12, because the regex stops after '12'. 'N/A' gives null.

Through the compliance lens, P0 is not justified. This helper normalises numbers scraped from public SEC Form 4 filings for an informational market-data tool. It moves no money, enforces no regulatory requirement, and is not an integrity guard: it rejects nothing and accepts the first number in the cell, whatever it is. Its output does feed the reported shares, price and value (round(shares*price, 2), server.py:856), so it is a real data-accuracy rule. But no regulator, auditor or finance controller depends on this tool's output, so a silent change here is a data-quality regression, not a compliance event. P1 (data correctness, parity-tested) fits better.

Notes for whoever keeps it as a contract:
(a) There is an untested Python/TS divergence. For '-(500)', Python strips only the parentheses at the ends, gets '-(500' and float() fails, so it returns null. TS removes e

### BR-NWS-127 · Form 4 reporting owner role derivation (P1, Calculation, confidence High)

- **Source:** `server.py:771-792,868-890; worker/src/yahoo-finance.ts:9745-9760,9811-9822`
- **Rule:** An insider's role is their officer title if one is filed. Otherwise it is a comma-separated list of the relationship flags that are set (director, officer, ten_percent_owner, other), with duplicates removed.
- **Given** An XML Form 4 with isDirector=1, isTenPercentOwner=true and an empty officerTitle
- **When** The transaction is parsed
- **Then** role = 'director, ten_percent_owner'. If officerTitle='Chief Executive Officer', role = 'Chief Executive Officer' and the flags are ignored
- **Parameters:** Truthy flag values: '1', 'true' (case-insensitive). HTML fallback: a cell 'X' followed by a label containing director / officer / '10%' or 'owner' / other
- **Edge cases:** When no flags are set the role is null; In HTML, 'director' is checked before 'officer', so a label containing both counts as director

### BR-NWS-128 · Form 4 transaction value = shares x price rounded to cents (P1, Calculation, confidence Medium)

- **Source:** `server.py:856; worker/src/yahoo-finance.ts:9734`
- **Rule:** The dollar value of an insider transaction is shares times price per share, rounded to 2 decimals. It is null if either input is missing.
- **Given** shares 1,500 and price $12.345
- **When** The insider transaction payload is built
- **Then** value = 18517.5. A price of null (for example a grant with no price) gives value null
- **Parameters:** Rounding to 2 decimals. Python uses round() (banker's rounding on floats); TS uses Math.round(x*100)/100 (half-up)
- **Edge cases:** A price of 0 gives value 0, not null; A negative shares value from accounting parentheses gives a negative value
- **Suspected defect:** The two runtimes round differently on exact half-cent ties: Python round() rounds half to even with float artifacts, while TS Math.round rounds half up. This could break output parity (for example 0.125 becomes 0.12 in Python and 0.13 in TS).
- **SME question:** P0 panel split on whether this moves money / is regulatory (I read both cited locations. The rule card describes them accurately. In /home/user/yahoo-finance-mcp/server.py:856 (inside _form4_transaction_payload), value is `round(shares * price, 2)`, or None if shares or price is None. In /home/user/yahoo-finance-mcp/worker/src/yahoo-finance.ts:9734 (inside form4Payload), value is `Math.round(shares * price * 100) / 100`, or null if either is null. The example holds: 1,500 x 12.345 rounds to 18517.5 in both runtimes, and a null price gives a null value. The note on rounding modes is correct in substance. Python's round() rounds exact ties to even, but float representation error often decides first. JS Math.round rounds ties up. So the two runtimes can disagree by one cent on exact half-cent products. For example, a product of exactly 0.125 gives 0.12 in Python and 0.13 in TS. That breaks the project's parity requirement and should be logged as a P1/P2 divergence, not a P0.

Through the compliance lens, P0 is not justified. This tool is read-only and only reports data. It takes shares and price from public SEC Form 4 filings and computes a display value for convenience. It never settles, charges, books or reports money to anyone. It does not enforce any regulatory requirement, since the filer's own Form 4 is the authoritative record. And it does not protect data integrity either: the shares and price fields still pass through unchanged, and any consumer can recompute value from

### BR-NWS-129 · Merged multi-source news is capped to the newest N after de-duplication (P1, Calculation, confidence High)

- **Source:** `server.py:2167-2168; server.py:2713-2716; worker/src/yahoo-finance.ts:10768; worker/src/yahoo-finance.ts:11928-11929`
- **Rule:** Items from all sources are de-duplicated, sorted newest first by publishedAt, and only the first max_results are kept, unless the event-verification path asks to keep everything.
- **Given** max_results 10, with 6 SEC, 8 Yahoo and 5 Finnhub items left after dedupe
- **When** Company events are assembled
- **Then** The 10 items with the latest publishedAt values are returned, whatever their source. Items with no publishedAt sort last and are the first to be cut.
- **Parameters:** Cap = clamped max_results (1-100, default 10). keep_all/keepAll=true for verify_company_event.
- **Edge cases:** The sort compares publishedAt as strings: Python uses code-point order, the Worker uses localeCompare; Worker timestamps from toISOString carry '.000Z' milliseconds, but SEC estimated dates are built as 'YYYY-MM-DDT00:00:00Z', so mixed formats can order inconsistently and also trigger TIMESTAMP_VARIANCE on duplicates
- **SME question:** Should the cross-source cap favour official sources (SEC, approved IR pages) over newer contextual news, or is pure recency intended?

### BR-NWS-130 · News event-type classification by keyword (P1, Calculation, confidence Medium)

- **Source:** `server.py:591-613; worker/src/yahoo-finance.ts:9608-9621`
- **Rule:** Each news item is tagged with an event type by the first matching keyword group in a fixed order: earnings, guidance, contract, financing, product, analyst, macro, litigation, insider, regulatory, else other.
- **Given** Headline 'Acme raises $50M in private placement'
- **When** Event type is derived from title + summary
- **Then** eventType = 'guidance' (because 'raises' is checked in the guidance group before 'offering/financing')
- **Parameters:** earnings: earnings, eps, quarterly, 10-q, 10-k, annual report (Py only); guidance: guidance, outlook, forecast, reaffirm, raises, lowers; contract: contract, agreement, deal, partnership; financing: offering, financing, debt, note(s), credit facility (Py only); product: launch, product, introduce, announces new (Py only); analyst: analyst, rating, upgrade, downgrade, price target; macro: macro, inflation, rates, fomc, cpi; litigation: lawsuit, litigation, court, settlement; insider: insider, director, officer, form 4; regulatory: sec, regulatory, 8-k, filing
- **Edge cases:** Plain substring matching: 'eps' matches 'steps', 'sec' matches 'second'/'sector'/'security', 'note' matches 'notebook', 'rates' matches 'corporates', 'deal' matches 'dealer'; 10-q/10-k listed in regulatory group are unreachable (caught by earnings)
- **Suspected defect:** Python/TS keyword lists diverge: Python adds 'annual report', 'credit facility', 'announces new'; TS lacks them, so the same headline can classify differently (and therefore get a different decisionUse). Substring matching also misclassifies e.g. 'raises capital' as guidance and 'second quarter' items without other hits as regulatory.
- **SME question:** Which keyword list is canonical, and should matching be whole-word rather than substring?

### BR-NWS-131 · Price target currency code mapping (P1, Calculation, confidence High)

- **Source:** `yfmcp/capital_structure.py:3100-3121; worker/src/capital-structure.ts:2839-2854`
- **Rule:** The currency symbol or suffix next to a target is mapped to an ISO-style code. Pence becomes GBp, distinct from pounds (GBP).
- **Given** Target tokens '$95', 'US$95', 'GBP 4', 'GBX 120', '120p', '45 kr', 'NT$900', 'EUR 30'
- **When** currency is resolved for an extracted price target
- **Then** USD, USD, GBP, GBp, GBp, SEK, TWD, EUR. When there is no symbol and no p/pence/kr/SEK suffix, currency is null.
- **Parameters:** $|US$|USD->USD; GBP sign|GBP->GBP; GBp|GBX->GBp; EUR sign|EUR->EUR; SEK|kr->SEK; NT$|TWD->TWD; suffix p|pence->GBp, kr|sek->SEK
- **Edge cases:** The match is exact-case: 'KR', 'usd' and 'gbx' captured by the case-insensitive regex all map to null
- **Suspected defect:** Capture is case-insensitive but mapping is case-sensitive, so lower-case or upper-case variants ('usd', 'KR') silently give a null currency.

### BR-NWS-132 · Requested vs attempted source accounting (P1, Calculation, confidence High)

- **Source:** `server.py:601-619; worker/src/yahoo-finance.ts:9124`
- **Rule:** Each news or event response says which sources the caller asked for, whether defaults were used, and which sources were actually attempted. Sources marked NOT_ELIGIBLE, NOT_NEEDED or UNCONFIGURED, or flagged attempted=false, are not listed as attempted.
- **Given** No sources argument and sourceStatus {finnhub: NOT_ELIGIBLE, marketaux: NOT_NEEDED, yahoo_finance_news: OK}
- **When** The response is built
- **Then** requestedSources=null, defaultSourcesApplied=true, attemptedSources=[{sourceType: yahoo_finance_news, status: OK}]
- **Parameters:** Not-attempted statuses: NOT_ELIGIBLE, NOT_NEEDED, UNCONFIGURED
- **Edge cases:** An explicit empty list [] gives defaultSourcesApplied=false; Non-dict status entries are skipped

### BR-NWS-133 · SEC event URL fallback chain and URL validation (differs between runtimes) (P1, Calculation, confidence High)

- **Source:** `server.py:1292-1301; worker/src/yahoo-finance.ts:10679-10698`
- **Rule:** An SEC event's link is chosen in this order: the supplied document URL, then the primary-document URL, then an archive URL built from CIK, accession number and primary document, then the filing index page. Only links under https://www.sec.gov/Archives/ are kept.
- **Given** A filing with CIK 320193, accession 0000320193-26-000010, primary document 'a8k.htm' and no documentUrl
- **When** The SEC event item is built
- **Then** url = https://www.sec.gov/Archives/edgar/data/320193/000032019326000010/a8k.htm. If the primary document were missing, url = .../000032019326000010/0000320193-26-000010-index.htm. Confidence is HIGH only when accession, acceptedAt and url are all present.
- **Parameters:** Allowed URL prefix = 'https://www.sec.gov/Archives/'
- **Edge cases:** Python: a supplied documentUrl that fails the prefix check falls through to the next candidate (each candidate goes through _safe_sec_url); Worker: a supplied documentUrl/primaryDocumentUrl that fails the prefix check blanks the URL and is never replaced by a built archive URL. The Worker also emits warning SEC_URL_INVALID, which Python never emits; Python sets url to null when missing; the Worker sets it to an empty string; Duplicate key title: when the filing type is blank, Python uses 'SEC filing' and the Worker uses ' filed'
- **Suspected defect:** Parity gap: the Worker emits warning code SEC_URL_INVALID and returns an empty-string url, while Python silently returns null. When a supplied URL is invalid, the Worker does not fall back to the constructed archive URL.

### BR-NWS-134 · SEC form to event type and SEC item confidence (P1, Calculation, confidence High)

- **Source:** `server.py:631-641,1237-1285`
- **Rule:** 10-Q and 10-K map to earnings; S-1, S-3 and 424B to financing; 8-K and proxy forms to regulatory; Form 4 to insider. An SEC item is HIGH confidence with accession number, accepted timestamp and URL; MEDIUM with accession and URL; otherwise LOW. Without an accepted timestamp, the filing date at midnight UTC is used and flagged PUBLISHED_AT_ESTIMATED.
- **Given** An 8-K with accession number and URL but no acceptanceDateTime, filed 2026-10-01
- **When** the SEC event is built
- **Then** publishedAt=2026-10-01T00:00:00Z, warning PUBLISHED_AT_ESTIMATED, confidence MEDIUM, eventType regulatory
- **Parameters:** URLs must start with https://www.sec.gov/Archives/; default SEC types in events 8-K, 10-Q, 10-K, S-3, DEF14A
- **Edge cases:** '424B' matches only that exact string; 424B5 maps to 'other'

### BR-NWS-135 · Subject alias derivation for target attribution (P1, Calculation, confidence High)

- **Source:** `yfmcp/capital_structure.py:3258-3286; worker/src/capital-structure.ts:2955-2979`
- **Rule:** The phrases that count as naming the company are: the ticker root, if it has at least 3 characters, and the normalised company name, both as written and with trailing legal suffixes removed.
- **Given** Ticker 'ASTS' and name 'AST SpaceMobile, Inc.'; separately, ticker 'GE.L'
- **When** aliases are built
- **Then** aliases = ['ast spacemobile', 'ast spacemobile inc', 'asts']. For 'GE.L' the ticker root 'ge' is under 3 characters and is dropped.
- **Parameters:** Minimum alias length: 3; 17 legal suffixes
- **Edge cases:** 'XYZ Holdings Group' is stripped to 'xyz'; A one-word name made only of a suffix (e.g. 'Group') is kept

### BR-NWS-136 · Timestamp normalization to ISO UTC differs in precision between runtimes (P1, Calculation, confidence High)

- **Source:** `yfmcp/util.py:231-263; worker/src/yahoo-finance.ts:9605-9615; worker/src/yahoo-finance.ts:1206`
- **Rule:** Provider timestamps (epoch seconds, YYYYMMDD, YYYYMMDDHHMMSS, YYYY-MM-DD, ISO) are converted to UTC with a Z suffix, but the Worker adds milliseconds and Python does not.
- **Given** providerPublishTime 1760000000
- **When** publishedAt is computed
- **Then** Python gives '2025-10-09T08:53:20Z'. The Worker gives '2025-10-09T08:53:20.000Z'. Compact '20260115' gives '2026-01-15T00:00:00Z' in both.
- **Parameters:** Compact formats: 8 or 14 digits
- **Edge cases:** Unparseable input returns null and triggers PUBLISHED_AT_UNAVAILABLE; A naive ISO string without a timezone is read as host-local time in both runtimes; Python accepts numeric epoch only as int/float, so a numeric string such as '1760000000' (10 digits) returns null; the Worker's new Date('1760000000') also returns null
- **Suspected defect:** Parity gap: publishedAt strings differ between the runtimes, which affects string-based sorting, the TIMESTAMP_VARIANCE check and output parity.

### BR-NWS-137 · Valuation multiple extraction (multiple, metric, EV flag, periods) (P1, Calculation, confidence High)

- **Source:** `yfmcp/capital_structure.py:3093-3097,3128-3158,3170-3189; worker/src/capital-structure.ts:2833-2837,2860-2880,2888-2902`
- **Rule:** A valuation multiple named in a sentence ('41x 2H27 EV/EBITDA' or 'EV/EBITDA multiple of about 20x') is recorded with its number, normalised metric, whether it is enterprise-value based, and any period tokens. Growth comparisons such as '3x faster' are ignored.
- **Given** The sentences 'Target based on 41x 2H27 EV/EBITDA' and 'revenue grew 3x faster than peers'
- **When** multiples are extracted
- **Then** The first gives {multiple 41, metric EBITDA, enterpriseValue true, periods ['2H27']}. The second gives nothing, because its window contains 'grew', 'faster' or 'than'.
- **Parameters:** Multiple digits 1-3 (max 999.x); multiple-first gap 40 chars; metric-first trailing window 30 chars; exclusion words than|grew|growth|increase(s|d)|faster|more; period tokens FY/CY/F+yy(yy)[E], 1H/2H+yy[E], 19xx/20xx[E], NTM, 'next twelve months' (becomes NTM), 'forward'
- **Edge cases:** The metric-first form allows hedges 'about', 'roughly', 'approximately' and '~'; Periods are de-duplicated and upper-cased with whitespace removed ('FY 26E' becomes 'FY26E'); A bare four-digit year in the window counts as a period

### BR-NWS-138 · Verify event query term threshold (P1, Calculation, confidence Medium)

- **Source:** `server.py:3196-3235`
- **Rule:** Generic words (announce, results, report, update, etc.) and words under 3 letters are dropped. An item matches if the whole normalized query appears as a phrase, or if it contains (stemmed) all of the meaningful terms when there are 2 or fewer, or ceil(67%) of them when there are more.
- **Given** event_query 'BlueBird satellite launch contract announced'
- **When** verify_company_event matches items
- **Then** meaningfulTerms=[bluebird, satellite, launch, contract], and requiredTermMatches=ceil(4*0.67)=3
- **Parameters:** Ratio 0.67; min term length 3; stopword list at server.py:3196-3201
- **Edge cases:** A query with no meaningful terms adds EVENT_QUERY_TOO_GENERIC
- **Suspected defect:** An exact PHRASE match is accepted even when the query is entirely generic (e.g. 'announced results'), so a too-generic query can still produce matches and a CONFIRMED status despite the EVENT_QUERY_TOO_GENERIC warning.
- **SME question:** Should a PHRASE match be allowed when meaningfulTerms is empty?

### BR-NWS-139 · Yahoo company identity aliases and acronyms (P1, Calculation, confidence High)

- **Source:** `server.py:356-400; worker/src/yahoo-finance.ts:9440-9494`
- **Rule:** The company's short and long names are normalised and stripped of legal suffixes to form match aliases (min 3 chars), and their initials (ignoring the/and/of/for) form acronyms of 3-6 letters; if no alias can be built the identity is UNAVAILABLE.
- **Given** longName 'Advanced Micro Devices, Inc.'
- **When** Yahoo news identity is built
- **Then** aliases = {'advanced micro devices inc', 'advanced micro devices'}, acronyms = {'amd'}, status RESOLVED
- **Parameters:** Legal suffixes: inc, incorporated, corp, corporation, ltd, limited, llc, plc, co, company, sa, ag, nv, se, gmbh; ignored acronym words: the, and, of, for; alias min length 3; acronym length 3-6
- **Edge cases:** Suffix stripping stops when one word remains; Two-letter acronyms dropped to avoid false positives
- **Suspected defect:** Parity break: TS excludes an alias equal to the ticker (Yahoo shortName fallback) and uses companyName->longName->shortName for companyName, while Python accepts ticker-equal aliases and uses longName->shortName. A ticker-only shortName makes Python RESOLVED but TS UNAVAILABLE.

### BR-NWS-140 · Yahoo company identity derivation (aliases and acronyms) (P1, Calculation, confidence High)

- **Source:** `server.py:356-400`
- **Rule:** Name aliases are built from Yahoo short and long names, normalized, with trailing legal suffixes stripped. Aliases need at least 3 characters. Acronyms are 3-6 initials, ignoring 'the/and/of/for'. Identity is RESOLVED only if at least one alias exists.
- **Given** longName 'Advanced Micro Devices, Inc.'
- **When** identity is built
- **Then** aliases include 'advanced micro devices inc' and 'advanced micro devices', acronym 'amd', status RESOLVED
- **Parameters:** Legal suffixes inc, incorporated, corp, corporation, ltd, limited, llc, plc, co, company, sa, ag, nv, se, gmbh; alias min length 3; acronym length 3-6
- **Edge cases:** If identity lookup fails, status is UNAVAILABLE, only ticker-token matches are kept, and the SOURCE_IDENTITY_UNAVAILABLE warning makes collection PARTIAL

### BR-NWS-141 · Analyst valuation result status and method-not-disclosed list (P1, Lifecycle, confidence High)

- **Source:** `worker/src/capital-structure.ts:2999-3006,3044-3084; yfmcp/capital_structure.py:3308-3316,3355-3395`
- **Rule:** Status is FOUND if any accepted evidence names a method, METHOD_NOT_DISCLOSED if there are targets without methods (from news or rating changes with a positive target), else NOT_FOUND; firms that published a target but never a method are listed once per source, and output is always CONTEXT_ONLY.
- **Given** News yields only 'Jefferies PT $50' and rating changes include Baird ptTo 45, ptFrom 0
- **When** the result is assembled
- **Then** status = METHOD_NOT_DISCLOSED; methodNotDisclosed lists Jefferies (source news, target 50) and Baird (source rating_changes, target 45, prior null)
- **Parameters:** Evidence cap 40; title 240 chars; sentence 400 chars; rating prior shown only if ptFrom > 0
- **Edge cases:** The same firm can appear twice in methodNotDisclosed (once 'news', once 'rating_changes') because the keys are source-scoped; A firm with a method anywhere is excluded from methodNotDisclosed; Items with neither URL nor title are skipped

### BR-NWS-142 · Approved IR page status and allowed action: Python and Worker disagree (P1, Lifecycle, confidence High)

- **Source:** `server.py:1171-1208; server.py:2406-2431; worker/src/yahoo-finance.ts:10220-10288; worker/src/yahoo-finance.ts:10996-11013`
- **Rule:** The registry-driven IR page source gives the same status names in both runtimes, but the allowedAction values differ, and Worker's computeSourceStatus has a branch that can never run.
- **Given** A company_ir_page registry entry that is disabled, past revalidation, or fails validation
- **When** Source status is built
- **Then** Python: DISABLED / REVALIDATION_DUE / SOURCE_VALIDATION_FAILED all give allowedAction 'registry_review_required'. Worker: DISABLED gives 'none', REVALIDATION_DUE gives 'revalidate', SOURCE_VALIDATION_FAILED gives 'review_registry_entry'. Worker's check `sourcesUsed.includes('company_ir_page') && !companyIrPageStatus` is always false because the status defaults to 'EMPTY_RESULT', so APPROVED_SOURCE_OK comes only from collector diagnostics.
- **Parameters:** Python precedence: used > CANDIDATE_AVAILABLE > DISABLED > REVALIDATION_DUE > VALIDATION_FAILED > NOT_REGISTERED. Worker: diagnostic status, overridden by VALIDATION_FAILED then REVALIDATION_DUE warnings
- **Edge cases:** Python picks the JSON parser when the content type contains 'json'; Worker first checks whether the body looks like an RSS/Atom feed and parses feeds, which Python does not
- **Suspected defect:** allowedAction enum values diverge between runtimes, and the Worker has a dead branch at worker/src/yahoo-finance.ts:11001.
- **SME question:** Which allowedAction values are canonical for DISABLED, REVALIDATION_DUE and SOURCE_VALIDATION_FAILED (planner decision on enum values)?

### BR-NWS-143 · Batch news per-ticker outcome state and partial-success metadata (Worker only) (P1, Lifecycle, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:1246-1273; worker/src/yahoo-finance.ts:1297-1356; worker/src/yahoo-finance.ts:11959-11977; server.py:8328-8332; yfmcp/clients/yahoo.py:9-20; yfmcp/tools/pricing.py:21-44`
- **Rule:** In a multi-ticker news call, the Worker gives each ticker a success or error outcome. It classifies errors from the message text (contains 'not found' / 'no data found for ticker' / 'api error 404' -> TICKER_NOT_FOUND; contains 'timeout' -> PROVIDER_TIMEOUT, retryable; anything else -> PROVIDER_ERROR) and adds __batchMeta {partialSuccess, successCount, errorCount}. Python returns raw per-ticker payloads or {error, message, ticker}, with no codes and no batch metadata.
- **Given** get_company_news(['AAPL','ZZZZ']) where ZZZZ fails with the message 'No data found for ticker ZZZZ'
- **When** The batch completes
- **Then** Worker: AAPL -> its news payload; ZZZZ -> {error:true, code:'TICKER_NOT_FOUND', retryable:false, ...}; __batchMeta = {partialSuccess:true, successCount:1, errorCount:1}. Python: ZZZZ -> {error:true, message:..., ticker:'ZZZZ'} or the raw failure JSON, with no __batchMeta.
- **Parameters:** BATCH_TICKER_CONCURRENCY = 3 (both runtimes); MAX_TICKERS = 5 (Worker rejects more; Python has no cap on this path); error codes TICKER_NOT_FOUND, PROVIDER_TIMEOUT (retryable), PROVIDER_ERROR
- **Edge cases:** Worker trims whitespace from each ticker before fetching; Python does not trim, so ' AAPL ' is validated as-is; Worker treats any payload with a non-null 'error' key as a failure, even error:false; Worker error entries have different shapes by path: the shaped-error branch returns {error,code,message,retryable}, but the thrown-exception branch returns {ok:false,data:null,error:{...}}; partialSuccess is true only when there is at least one success and at least one error; all-fail gives partialSuccess=false; A duplicate ticker in the list overwrites its earlier entry in the keyed result in both runtimes; Tool description says 'only the first 5 are processed', but the Worker rejects lists longer than 5 with INPUT_VALIDATION_ERROR (worker/src/tools.ts:533 vs worker/src/yahoo-finance.ts:1301-1307)
- **Suspected defect:** Python lacks __batchMeta and the error-code classification, a parity break. The Worker's error-entry shape depends on which failure path fired. Matching on the substring 'not found' can label unrelated failures as TICKER_NOT_FOUND.
- **SME question:** Should Python emit the same per-ticker error codes and __batchMeta? Should the error-entry shape be standardised across both Worker failure paths?

### BR-NWS-144 · Collection status for news results (P1, Lifecycle, confidence High)

- **Source:** `server.py:2130-2147`
- **Rule:** A news result is PARTIAL when it has items but some source was limited. An empty result is SOURCE_LIMITED_NOT_FOUND if a source was limited, NOT_FOUND if sources ran cleanly, and PROVIDER_ERROR if no source ran at all. A full result with clean sources has no status field.
- **Given** 0 items returned and a SOURCE_NOT_ELIGIBLE warning for Finnhub
- **When** get_company_news builds its payload
- **Then** status = SOURCE_LIMITED_NOT_FOUND (not NOT_FOUND)
- **Parameters:** limiting warning codes: SOURCE_UNAVAILABLE, SOURCE_NOT_ELIGIBLE, SOURCE_IDENTITY_UNAVAILABLE, plus any COMPANY_IR_PAGE_* warning when the result is empty
- **Edge cases:** Status is computed on the post-cap list. In search_company_news it is computed after query filtering
- **Suspected defect:** Parity divergence. Python treats any COMPANY_IR_PAGE_* warning (including CANDIDATE_AVAILABLE and DISABLED) as limiting. The TS collectionStatus (worker/src/yahoo-finance.ts:10757-10776) lists only VALIDATION_FAILED, REVALIDATION_DUE and the company_ir RSS codes, so the two runtimes can return NOT_FOUND vs SOURCE_LIMITED_NOT_FOUND for the same case.

### BR-NWS-145 · Company IR RSS discovery lifecycle (rssDiscovery state) (P1, Lifecycle, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:10528-10601`
- **Rule:** Each company IR RSS lookup moves through discovery states: not_attempted, then website_not_available, feeds_discovered or feed_not_found, then feeds_accepted or feed_not_accepted. The source counts as used only if at least one feed was accepted.
- **Given** A ticker whose profile website yields 2 discovered feeds, both on external hosts that do not mention the company
- **When** collectCompanyIrRssEvents runs
- **Then** rssDiscovery = feed_not_accepted, two COMPANY_IR_RSS_HOST_REJECTED warnings are emitted, and used=false. A missing website gives rssDiscovery=website_not_available and used=false. Collection stops once maxResults items are gathered.
- **Parameters:** acceptedFeeds reported: first 5
- **Edge cases:** A feed that is accepted but whose items all fall outside the date window still gives used=true and EMPTY_RESULT

### BR-NWS-146 · Company IR page status and allowed action mapping (P1, Lifecycle, confidence High)

- **Source:** `server.py:2406-2433`
- **Rule:** The company IR page source reports APPROVED_SOURCE_OK only when an approved page was fetched and returned items. Otherwise its status follows the registry warning, in this order: CANDIDATE_AVAILABLE, DISABLED, REVALIDATION_DUE, SOURCE_VALIDATION_FAILED, NOT_REGISTERED. Each status is paired with a next action.
- **Given** The ticker has a registry entry with status 'candidate'
- **When** Source status is computed
- **Then** company_ir_page.status=CANDIDATE_AVAILABLE, allowedAction='review_and_promote', candidates=[the candidate record]
- **Parameters:** allowedAction: APPROVED_SOURCE_OK->fetch_configured_source; CANDIDATE_AVAILABLE->review_and_promote; all others->registry_review_required
- **Edge cases:** A fetch that runs but returns zero items gives EMPTY_RESULT with allowedAction registry_review_required

### BR-NWS-147 · Empty press-release result reports a 'no release' status even when a source failed (P1, Lifecycle, confidence Medium)

- **Source:** `server.py:2986-3003; worker/src/yahoo-finance.ts:12181-12200`
- **Rule:** When no release items are found, the press-release status is chosen only from which sources were selected (Yahoo press releases, then company_ir_page, then company_ir, otherwise NOT_FOUND). Whether a source actually failed is not considered. The collection-status logic, which knows about source failures, runs only when there are items.
- **Given** get_company_press_releases with the default sources [sec, company_ir_page, yahoo_finance_press_releases], where SEC submissions are unavailable (SOURCE_UNAVAILABLE warning) and Yahoo returns no press releases
- **When** The payload status is computed
- **Then** status = NO_YAHOO_PRESS_RELEASE and coverageStatus = NO_OFFICIAL_RELEASE_SOURCE, not SOURCE_LIMITED_NOT_FOUND. The SEC failure appears only in sourceStatus and coverage.
- **Parameters:** Status precedence: SEC_EX99_FOUND > APPROVED_IR_PAGE_FOUND > SEC_8K_FOUND_EX99_NOT_FOUND > (when empty) NO_YAHOO_PRESS_RELEASE | COMPANY_IR_PAGE_NOT_APPROVED / COMPANY_IR_PAGE_CANDIDATE_ONLY | COMPANY_IR_NOT_FOUND | NOT_FOUND > (when there are items) the collection status
- **Edge cases:** When items exist, the status falls through to the collection status, which can return PARTIAL or null; If yahoo_finance_press_releases is selected, NO_YAHOO_PRESS_RELEASE wins even when the real gap is SEC or the IR page
- **Suspected defect:** An empty result caused by a provider outage is labelled with a genuine-absence style status, which goes against the stated policy that source limits must be distinguishable from absence.
- **SME question:** When a press-release call returns no items and at least one selected source failed (SOURCE_UNAVAILABLE), should status be SOURCE_LIMITED_NOT_FOUND instead of NO_YAHOO_PRESS_RELEASE / COMPANY_IR_NOT_FOUND / NOT_FOUND?

### BR-NWS-148 · Empty press-release status when only the IR-page registry is selected: Worker separates 'candidate only' from 'not approved' (P1, Lifecycle, confidence High)

- **Source:** `server.py:2996-2997; server.py:3042-3046; worker/src/yahoo-finance.ts:12160-12163; worker/src/yahoo-finance.ts:12191-12192; worker/src/yahoo-finance.ts:12235`
- **Rule:** If a press-release call returns nothing and company_ir_page is the deciding selected source, the Worker reports COMPANY_IR_PAGE_CANDIDATE_ONLY when the registry has unapproved candidates and COMPANY_IR_PAGE_NOT_APPROVED otherwise. Python always reports COMPANY_IR_PAGE_NOT_APPROVED.
- **Given** get_company_press_releases(ticker, sources=['company_ir_page']), with no approved IR page and one unapproved candidate registry entry for the ticker
- **When** The empty-result status is chosen
- **Then** Worker: status = COMPANY_IR_PAGE_CANDIDATE_ONLY. Python: status = COMPANY_IR_PAGE_NOT_APPROVED. Both runtimes emit candidateSources, but from different inputs: Python reads COMPANY_IR_PAGE_CANDIDATE_AVAILABLE warnings, the Worker reads sourceDiagnostics.company_ir_page.candidates.
- **Parameters:** Status values: COMPANY_IR_PAGE_CANDIDATE_ONLY (Worker only), COMPANY_IR_PAGE_NOT_APPROVED
- **Edge cases:** If yahoo_finance_press_releases is also selected, NO_YAHOO_PRESS_RELEASE takes precedence in both runtimes; The two runtimes build candidateSources from different inputs, so the lists can differ even when both are present
- **Suspected defect:** Status enums differ between runtimes, which breaks the project's strict output-parity requirement.
- **SME question:** Which status should both runtimes emit when only unapproved IR-page candidates exist: COMPANY_IR_PAGE_CANDIDATE_ONLY or COMPANY_IR_PAGE_NOT_APPROVED?

### BR-NWS-149 · Empty-result collection status treats IR page registry states differently per runtime (P1, Lifecycle, confidence High)

- **Source:** `server.py:2172-2189; worker/src/yahoo-finance.ts:10772-10791`
- **Rule:** With zero items, Python reports SOURCE_LIMITED_NOT_FOUND for any COMPANY_IR_PAGE_* warning, including informational candidate or disabled entries. The Worker only does so for specific IR failure codes, and also for website/RSS discovery failures that Python never emits.
- **Given** sources=['sec','company_ir_page'], SEC returns nothing, and the ticker's IR page entry is 'candidate'
- **When** Collection status is computed
- **Then** Python: SOURCE_LIMITED_NOT_FOUND (the info warning COMPANY_IR_PAGE_CANDIDATE_AVAILABLE matches the prefix). Worker: no warning is emitted for candidates, so the status is NOT_FOUND.
- **Parameters:** Worker list: COMPANY_WEBSITE_NOT_AVAILABLE, COMPANY_IR_FEED_NOT_FOUND, COMPANY_IR_RSS_HOST_REJECTED, COMPANY_IR_RSS_PARSE_ERROR, COMPANY_IR_RSS_UNAVAILABLE, COMPANY_IR_DISCOVERY_BUDGET_EXHAUSTED, COMPANY_IR_PAGE_VALIDATION_FAILED, COMPANY_IR_PAGE_REVALIDATION_DUE
- **Edge cases:** Disabled entries: Python SOURCE_LIMITED_NOT_FOUND, Worker NOT_FOUND
- **Suspected defect:** Parity break in the top-level status.
- **SME question:** Should an unreviewed candidate or disabled IR page make an empty result 'source-limited' (Python) or plain NOT_FOUND (Worker)?

### BR-NWS-150 · Expanded institutional ownership provider state machine (P1, Lifecycle, confidence High)

- **Source:** `yfmcp/tools/provider_gaps.py:306-381; worker/src/yahoo-finance.ts:2204-2268`
- **Rule:** The tool tries Finnhub first, if the ticker is eligible. It moves to the scarce Alpha Vantage quota only when Finnhub gave no usable rows and the caller passed allow_scarce_fallback=true. A provider whose response names a different symbol is treated as a failure.
- **Given** Ticker AAPL. Finnhub returns OK, but payload.symbol is MSFT. allow_scarce_fallback=true
- **When** get_expanded_institutional_ownership runs
- **Then** The Finnhub attempt gets validationStatus PROVIDER_IDENTITY_MISMATCH, rows are discarded, and Alpha Vantage INSTITUTIONAL_HOLDINGS is called. If Alpha also yields no rows, status = Alpha's status, or NOT_FOUND when Alpha returned OK but empty.
- **Parameters:** max_holders default 50, range 1-100; ownership cache TTL 7 days; capacityClass ENTITLEMENT_DEPENDENT (Finnhub) / SCARCE_SHARED_QUOTA (Alpha)
- **Edge cases:** OK with zero rows becomes NOT_FOUND. Without fallback the action is VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK.; A missing symbol in the response defaults to the requested ticker, so it passes.; Success: status OK, truncated = providerRowCount > returnedCount, freshnessStatus DATED or DATE_NOT_DISCLOSED, decisionGrade false, recommendedNextAction VERIFY_SEC_13F_FOR_MATERIAL_USE.

### BR-NWS-151 · Explicit source list selecting nothing forces SOURCE_LIMITED_NOT_FOUND (P1, Lifecycle, confidence High)

- **Source:** `server.py:622-630; server.py:8367-8369; worker/src/yahoo-finance.ts:9159-9167; worker/src/yahoo-finance.ts:11995-11998`
- **Rule:** If the caller passes an explicit source list that contains no supported source (empty, or only unknown names), nothing is attempted. The result is marked source-limited, not a provider error or confirmed absence.
- **Given** get_company_news('AAPL', sources=['bloomberg'])
- **When** The payload is finalised
- **Then** status = SOURCE_LIMITED_NOT_FOUND, sourceCoverage = PARTIAL, coverage.state = PARTIAL, recommendedNextAction = CHECK_OFFICIAL_RELEASES, plus a SOURCE_UNSUPPORTED warning
- **Parameters:** none
- **Edge cases:** This overrides the PROVIDER_ERROR that the collection-status logic would otherwise give for no sources used; Omitting sources (null) never triggers it

### BR-NWS-152 · Explicit source list with no supported source reads as source-limited (P1, Lifecycle, confidence High)

- **Source:** `server.py:622-630; worker/src/yahoo-finance.ts:9159`
- **Rule:** If the caller passes an explicit source list in which no source is supported, nothing is attempted. The result is SOURCE_LIMITED_NOT_FOUND with PARTIAL coverage and a recommendation to check official releases, never a plain NOT_FOUND.
- **Given** sources=['bloomberg']
- **When** search_company_news, get_company_press_releases, get_public_event_timeline or verify_company_event finishes
- **Then** status='SOURCE_LIMITED_NOT_FOUND', sourceCoverage='PARTIAL', coverage.state='PARTIAL', coverage.recommendedNextAction='CHECK_OFFICIAL_RELEASES'
- **Edge cases:** sources=None (defaults) is never overridden; The final status is overwritten even if earlier logic set a different status

### BR-NWS-153 · Finnhub status state machine (eligibility, configuration, HTTP outcome, truncation) (P1, Lifecycle, confidence High)

- **Source:** `server.py:2283-2321`
- **Rule:** Finnhub's status is chosen in a fixed order: NOT_ELIGIBLE, then OK/EMPTY_RESULT if it was used, then UNCONFIGURED, AUTH_ERROR, RATE_LIMITED, PROVIDER_CHANGED, PROVIDER_ERROR, and finally EMPTY_RESULT. The state is read by matching text in warning messages.
- **Given** Finnhub returned 30 in-window items and the cap returned 10
- **When** sourceStatus is built
- **Then** finnhub.status = OK, acceptedCount 30, returnedCount 10, truncatedCount 20, hasMoreAccepted = true
- **Parameters:** Message substrings checked: 'finnhub company-news source is not configured', 'finnhub auth error', 'finnhub rate limited', 'finnhub provider changed', 'finnhub'
- **Edge cases:** A Finnhub timeout is never classified as TIMEOUT. It becomes PROVIDER_ERROR because the collector only special-cases HTTP 401/403/429 (server.py:1744-1770); An UNCONFIGURED status has no rawCount or filteredCount fields
- **Suspected defect:** Status is derived from free-text warning messages, so rewording a message silently changes the state. Finnhub cannot report TIMEOUT, which means RETRY_RETRYABLE_SOURCES never fires for a Finnhub timeout.

### BR-NWS-154 · GlobeNewswire (newswire) source used/failed determination (P1, Lifecycle, confidence Medium)

- **Source:** `server.py:1606-1630; server.py:1720-1725; server.py:2434-2440; worker/src/yahoo-finance.ts:11296-11323; worker/src/yahoo-finance.ts:11388-11399; worker/src/yahoo-finance.ts:11015-11023`
- **Rule:** The newswire source counts as used only if at least one of the 10 GlobeNewswire feeds parsed. If any feed failed and no matching item was found, the whole source counts as not used and is reported as PROVIDER_ERROR.
- **Given** sources=['newswire']; 9 of 10 GlobeNewswire feeds parse successfully with no matching stock category for the ticker, and 1 feed times out
- **When** The collector finishes
- **Then** used=false, so sourceStatus.newswire = PROVIDER_ERROR (the warning message contains 'globenewswire'), coverage is PARTIAL, and the collection status is SOURCE_LIMITED_NOT_FOUND. With all 10 parsed and no failures: used=true and status EMPTY_RESULT. Collection stops early (used=true) once max_results matching items are found.
- **Parameters:** 10 feeds (public_companies, press_releases, earnings, stock_market_news, technology, semiconductors, telecommunications, mobile_telecommunications, telecommunications_equipment, electronic_equipment); 2 MB max response; 20 s fetch timeout (Python)
- **Edge cases:** Worker also rejects feeds with no <rss>/<channel>/</rss> or unbalanced <item> tags as parse errors; Python relies on the XML parser; Any non-item warning (e.g. PUBLISHED_AT_UNAVAILABLE) with zero items would also mark the source unused
- **SME question:** Should one failing feed out of ten turn a clean 'nothing found in 9 feeds' into PROVIDER_ERROR for the whole newswire source, or should partial feed failure be reported separately?

### BR-NWS-155 · Marketaux status mapping from HTTP and parse outcome (P1, Lifecycle, confidence High)

- **Source:** `server.py:1880-1949; server.py:2016-2020; server.py:2322-2352`
- **Rule:** Marketaux moves from an initial PROVIDER_ERROR to a final state: UNCONFIGURED (no token), AUTH_ERROR (401/403), RATE_LIMITED (402/429), TIMEOUT, PROVIDER_ERROR, PROVIDER_CHANGED (no data array), or OK/EMPTY_RESULT. Each state has an allowed next action.
- **Given** Marketaux returns HTTP 402
- **When** the fallback runs
- **Then** status = RATE_LIMITED, allowedAction = retry_after_quota_reset, and coverage.recommendedNextAction = RETRY_RETRYABLE_SOURCES
- **Parameters:** allowedAction map: NOT_NEEDED->none, UNCONFIGURED->configure_marketaux_api_token, AUTH_ERROR->verify_marketaux_api_token, RATE_LIMITED->retry_after_quota_reset, TIMEOUT->retry_later, PROVIDER_CHANGED->inspect_provider_contract, PROVIDER_ERROR->retry_or_use_official_sources, OK with items->check_official_releases
- **Edge cases:** TIMEOUT is detected by 'timeout' or 'timed out' in the exception text; An unknown status with 0 accepted items maps to allowedAction 'none'

### BR-NWS-156 · One Yahoo outage warning marks every selected Yahoo sub-source as PROVIDER_ERROR (P1, Lifecycle, confidence High)

- **Source:** `server.py:2205-2212; server.py:2249-2252; server.py:2293-2327; worker/src/yahoo-finance.ts:10800-10804; worker/src/yahoo-finance.ts:10865-10901`
- **Rule:** If a selected Yahoo source has no per-source diagnostic and was not used, its status is PROVIDER_ERROR whenever any SOURCE_UNAVAILABLE warning mentions 'yahoo finance', and EMPTY_RESULT otherwise. A single outage message therefore fails news, press releases and the legacy aggregate together. When a diagnostic exists, it wins: completed=false gives PROVIDER_ERROR, otherwise acceptedCount > 0 gives OK, else EMPTY_RESULT.
- **Given** sources = [yahoo_finance_news, yahoo_finance_press_releases], no collector diagnostics, and one warning {code: SOURCE_UNAVAILABLE, message: 'Yahoo Finance news unavailable'}
- **When** Per-source status is computed
- **Then** Both yahoo_finance_news and yahoo_finance_press_releases = {status: PROVIDER_ERROR, rawCount: 0, filteredCount: 0}
- **Parameters:** Match = case-insensitive substring 'yahoo finance' in SOURCE_UNAVAILABLE messages; precedence = diagnostic > used-in-sourcesUsed > outage text > EMPTY_RESULT. In Python, when selected_sources is None the selection defaults to [yahoo_finance_news, yahoo_finance_press_releases, finnhub, marketaux]; the Worker requires an explicit selection
- **Edge cases:** The legacy yahoo_finance aggregate counts items tagged with any of the three Yahoo source names; Python coerces counts with int(x or 0) while the Worker uses Number(x ?? 0); a non-numeric count gives 0 in Python and NaN in the Worker; An IDENTITY_UNAVAILABLE identity diagnostic is shown as a separate yahoo_finance_identity entry
- **SME question:** Should a Yahoo outage be attributed to each sub-source separately, or is failing all Yahoo sub-sources together on one outage message intended?

### BR-NWS-157 · Overall collection status (P1, Lifecycle, confidence High)

- **Source:** `server.py:2130-2147`
- **Rule:** A news response is PARTIAL when items exist but a source was unavailable/ineligible/identity-limited; when empty it is SOURCE_LIMITED_NOT_FOUND if any source was limited (or an IR page problem), NOT_FOUND if sources ran cleanly, else PROVIDER_ERROR.
- **Given** No items returned and Finnhub unconfigured
- **When** Status is computed
- **Then** status = SOURCE_LIMITED_NOT_FOUND (empty may reflect missing coverage, not absence)
- **Parameters:** Limiting warning codes: SOURCE_UNAVAILABLE, SOURCE_NOT_ELIGIBLE, SOURCE_IDENTITY_UNAVAILABLE, COMPANY_IR_PAGE_*
- **Edge cases:** Items present with no limits -> no status field

### BR-NWS-158 · Per-source news status: Yahoo feed state from collector completion and accepted count (P1, Lifecycle, confidence High)

- **Source:** `server.py:2213-2236`
- **Rule:** Each Yahoo news feed gets one of three states: PROVIDER_ERROR if the fetch did not finish, OK if at least one item was accepted, otherwise EMPTY_RESULT.
- **Given** get_company_news runs with source yahoo_finance_news, and the Yahoo collector finished (completed=true) with rawCount 20, rejected 20, acceptedCount 0
- **When** sourceStatus is built
- **Then** yahoo_finance_news.status = EMPTY_RESULT, rawCount 20, rejectedCount 20, rejectionCounts broken down by reason (e.g. IDENTITY_MISMATCH, DATE_WINDOW). If completed=false the status is PROVIDER_ERROR whatever the counts are.
- **Parameters:** States: OK | EMPTY_RESULT | PROVIDER_ERROR. acceptedCount falls back to filteredCount, then to the number of returned items.
- **Edge cases:** The legacy 'yahoo_finance' aggregate is completed only if every sub-feed completed (server.py:2700); When there are no diagnostics, the code falls back to 'Yahoo Finance' text in SOURCE_UNAVAILABLE warning messages (server.py:2203-2206)

### BR-NWS-159 · Per-source status for SEC, company_ir and newswire (P1, Lifecycle, confidence Medium)

- **Source:** `server.py:2284-2290,2399-2405,2434-2440`
- **Rule:** Each source's status is OK if it ran and returned items, and EMPTY_RESULT if it ran with none. If it did not run, the status is PROVIDER_ERROR when a SOURCE_UNAVAILABLE warning names that provider, otherwise EMPTY_RESULT.
- **Given** The SEC was selected but not used, and a warning reads 'SEC submissions fetch failed'
- **When** Source status is computed
- **Then** sec.status=PROVIDER_ERROR with rawCount 0. With no such warning, sec.status=EMPTY_RESULT
- **Parameters:** Warning substrings: 'sec submissions' (SEC), 'yahoo finance' (company_ir), 'globenewswire' (newswire)
- **Edge cases:** company_ir errors are keyed off 'yahoo finance' warnings, not IR-specific ones; rawCount equals filteredCount for these sources
- **SME question:** Is it intended that a company_ir source failure is detected only from a 'yahoo finance' warning, and that an unattempted source with no warning reports EMPTY_RESULT instead of an error?

### BR-NWS-160 · SEC source status: no CIK or submissions means provider error, not absence (P1, Lifecycle, confidence High)

- **Source:** `server.py:1340-1345; server.py:2284-2290; worker/src/yahoo-finance.ts:10855-10863`
- **Rule:** When SEC is a selected news source, its status is OK if SEC items came back, EMPTY_RESULT if SEC was read but nothing matched, and PROVIDER_ERROR if the ticker could not be resolved to an SEC CIK or its submissions could not be loaded.
- **Given** A news or event request with sources including 'sec' for a ticker with no SEC CIK (e.g. a non-US listing such as 7203.T)
- **When** The SEC collector cannot load submissions and emits SOURCE_UNAVAILABLE 'SEC submissions source unavailable.'
- **Then** sourceStatus.sec = {status: PROVIDER_ERROR, rawCount 0}; sourceCoverage becomes PARTIAL; with zero items overall the collection status is SOURCE_LIMITED_NOT_FOUND rather than NOT_FOUND
- **Parameters:** Status derived by substring match 'sec submissions' on SOURCE_UNAVAILABLE warning messages
- **Edge cases:** A missing CIK (foreign issuer, ETF) and an SEC outage are reported the same way, as PROVIDER_ERROR; sec used but no item has 'sec' in sourceType gives EMPTY_RESULT
- **Suspected defect:** An issuer that does not file with the SEC is labelled PROVIDER_ERROR (a retryable failure) instead of a not-applicable state, so coverage for every non-SEC filer is permanently PARTIAL.

### BR-NWS-161 · Source coverage and recommended next action (P1, Lifecycle, confidence Medium)

- **Source:** `server.py:314-353,2398-2408; worker/src/yahoo-finance.ts:9248-9278,11013-11040`
- **Rule:** Coverage is PARTIAL if any source failed or was ineligible; the recommended next action is retry when rate-limited/timeout, check official releases when any source failed or was skipped (other than NOT_NEEDED), retry truncated source when results were truncated, otherwise use returned context.
- **Given** sourceStatus finnhub RATE_LIMITED, yahoo OK
- **When** Coverage is built
- **Then** state PARTIAL, recommendedNextAction RETRY_RETRYABLE_SOURCES
- **Parameters:** Failed states: UNCONFIGURED, PROVIDER_ERROR, AUTH_ERROR, RATE_LIMITED, TIMEOUT, PROVIDER_CHANGED, IDENTITY_UNAVAILABLE, WEBSITE_NOT_AVAILABLE, DISCOVERY_NOT_FOUND, DISCOVERY_BUDGET_EXHAUSTED, FEED_NOT_FOUND, PARSE_ERROR, CANDIDATE_AVAILABLE, NOT_REGISTERED, DISABLED, REVALIDATION_DUE, SOURCE_VALIDATION_FAILED; skipped: NOT_ELIGIBLE, NOT_NEEDED
- **Edge cases:** NOT_NEEDED skips do not limit coverage
- **Suspected defect:** sourceCoverage state list omits several failed states used for coverage.failedSources (e.g. WEBSITE_NOT_AVAILABLE, FEED_NOT_FOUND, PARSE_ERROR), so state can be FULL while failedSources is non-empty.
- **SME question:** Should _compute_source_coverage use the same failed-state set as _build_coverage?

### BR-NWS-162 · Valuation method extraction status and output caps (P1, Lifecycle, confidence High)

- **Source:** `yfmcp/capital_structure.py:3379-3406; worker/src/capital-structure.ts:3060-3080`
- **Rule:** The result is FOUND if any evidence names a method. It is METHOD_NOT_DISCLOSED if there is evidence or a non-disclosing firm but no method. Otherwise it is NOT_FOUND. The output is always context-only, never a model input.
- **Given** No method-bearing sentence, but one rating change with ptTo 95
- **When** status is set
- **Then** status = METHOD_NOT_DISCLOSED, basis NEWS_TEXT_EXTRACTION, decisionUse CONTEXT_ONLY
- **Parameters:** Evidence cap 40
- **Edge cases:** methodCounts can exceed what the 40 returned rows show

### BR-NWS-163 · Verification failure-mode precedence and retryability (P1, Lifecycle, confidence High)

- **Source:** `server.py:2458-2486,3436-3446; worker/src/yahoo-finance.ts:11067,12499-12510`
- **Rule:** When verification would return NOT_FOUND while some sources were unavailable, the failure is labelled by a fixed priority. Only subrequest-limit, rate-limit and timeout failures are marked retryable.
- **Given** verify_company_event finds nothing; sourceStatus contains RATE_LIMITED and UNCONFIGURED
- **When** The verification payload is built
- **Then** status=SOURCE_LIMITED_NOT_FOUND, failureMode=RATE_LIMITED, retryable=true. With only UNCONFIGURED: failureMode=SOURCE_UNCONFIGURED, retryable=false
- **Parameters:** Order: 'too many subrequests' in a SOURCE_UNAVAILABLE warning -> WORKER_SUBREQUEST_LIMIT; RATE_LIMITED; TIMEOUT->PROVIDER_TIMEOUT; PROVIDER_CHANGED; IDENTITY_UNAVAILABLE->YAHOO_IDENTITY_UNAVAILABLE; UNCONFIGURED->SOURCE_UNCONFIGURED; NOT_ELIGIBLE->SOURCE_NOT_ELIGIBLE; PROVIDER_ERROR. Retryable set: WORKER_SUBREQUEST_LIMIT, RATE_LIMITED, PROVIDER_TIMEOUT
- **Edge cases:** AUTH_ERROR and company_ir_page states (REVALIDATION_DUE etc.) produce no failure mode, so NOT_FOUND stays NOT_FOUND even though coverage is PARTIAL; failureMode is null unless the final status is SOURCE_LIMITED_NOT_FOUND
- **Suspected defect:** NOT_ELIGIBLE (for example Finnhub skipped for a non-US ticker) turns NOT_FOUND into SOURCE_LIMITED_NOT_FOUND, but AUTH_ERROR does not, although AUTH_ERROR is a real outage. The asymmetry may be unintended.

### BR-NWS-164 · Verify event status precedence (P1, Lifecycle, confidence Medium)

- **Source:** `server.py:3237-3277`
- **Rule:** The verification result is CONFLICTING when an evidence conflict touches a matched group. Otherwise it is CONFIRMED when an official-source match with a URL and HIGH or MEDIUM confidence falls in range, PARTIAL when any match is in range, STALE when all matches are older than 90 days, and NOT_FOUND otherwise.
- **Given** start_date 2026-10-01. The only match is a yahoo_finance_press_releases item dated 2026-10-05 with a URL (MEDIUM)
- **When** verify_company_event runs
- **Then** status=CONFIRMED (yahoo_finance_press_releases is an official source type)
- **Parameters:** _STALE_EVENT_DAYS=90; official types sec_filing, company_ir, company_ir_page, press_release, newswire, yahoo_finance_press_releases; collection lookback 365 days
- **Edge cases:** With no start/end date, every match counts as in range, so STALE can never occur and 300-day-old matches still return CONFIRMED or PARTIAL; sec_ex99_found is not in the official set
- **Suspected defect:** STALE is reachable only when explicit dates exclude the match. Without dates, old evidence (up to 365 days) is reported as CONFIRMED.
- **SME question:** Should matches older than 90 days downgrade to STALE even when the caller gives no date range?

### BR-NWS-165 · Worker company_ir status precedence (P1, Lifecycle, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:10976-10994`
- **Rule:** For the company IR RSS source, the Worker chooses one status by checking warnings in a fixed order: website unavailable, discovery budget exhausted, feed not found, parse error, feed unavailable, external host rejected, then a Yahoo error.
- **Given** company_ir was not used, and the warnings include both COMPANY_IR_DISCOVERY_BUDGET_EXHAUSTED and COMPANY_IR_FEED_NOT_FOUND
- **When** computeSourceStatus runs
- **Then** status = DISCOVERY_BUDGET_EXHAUSTED (the earlier check wins). If company_ir was used: OK when items exist, otherwise EMPTY_RESULT. COMPANY_IR_RSS_HOST_REJECTED maps to DISCOVERY_NOT_FOUND, and COMPANY_IR_RSS_UNAVAILABLE maps to PROVIDER_ERROR.
- **Parameters:** Order: WEBSITE_NOT_AVAILABLE > DISCOVERY_BUDGET_EXHAUSTED > FEED_NOT_FOUND > PARSE_ERROR > PROVIDER_ERROR(RSS unavailable) > DISCOVERY_NOT_FOUND(host rejected) > PROVIDER_ERROR(Yahoo warning) > EMPTY_RESULT
- **Edge cases:** A Yahoo outage turns company_ir into PROVIDER_ERROR even though company_ir no longer uses Yahoo in the Worker; All of these statuses count as failed and make coverage PARTIAL

### BR-NWS-166 · Yahoo identity unavailable degrades matching (P1, Lifecycle, confidence High)

- **Source:** `server.py:2539-2552`
- **Rule:** When the company profile lookup returns no usable name, Yahoo matching falls back to exact ticker tokens only. A yahoo_finance_identity status of IDENTITY_UNAVAILABLE is reported, which makes coverage PARTIAL and lets Marketaux run as a fallback.
- **Given** yfinance info returns no shortName or longName for the ticker
- **When** news is collected
- **Then** warning SOURCE_IDENTITY_UNAVAILABLE; sourceStatus.yahoo_finance_identity = {status IDENTITY_UNAVAILABLE, reasonCode YAHOO_PROFILE_IDENTITY_UNAVAILABLE, allowedAction retry_or_use_explicit_ticker_matches_only}
- **Parameters:** reasonCode YAHOO_PROFILE_IDENTITY_UNAVAILABLE

### BR-NWS-167 · company_ir source status precedence (Worker) (P1, Lifecycle, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:10972-10995, 11027-11054`
- **Rule:** The company_ir status takes the first matching condition in a fixed order. Any non-OK, non-empty status makes overall source coverage PARTIAL.
- **Given** company_ir selected, discovery found feeds but the only feed failed to download
- **When** computeSourceStatus runs
- **Then** Statuses are checked in order: OK/EMPTY_RESULT if used, WEBSITE_NOT_AVAILABLE, DISCOVERY_BUDGET_EXHAUSTED, FEED_NOT_FOUND, PARSE_ERROR, PROVIDER_ERROR (RSS unavailable), DISCOVERY_NOT_FOUND (host rejected), PROVIDER_ERROR (Yahoo error), else EMPTY_RESULT. This case yields PROVIDER_ERROR and sourceCoverage=PARTIAL.
- **Parameters:** limited statuses list at 11028-11047
- **Edge cases:** Warnings from any source with the same code can trigger these statuses because warningCodes is shared across sources

### BR-NWS-168 · Any Finnhub ineligibility or failure forces a Marketaux call (P1, Policy, confidence Medium)

- **Source:** `server.py:2101-2125,2644-2652; worker/src/yahoo-finance.ts:11696-11711,11864-11880`
- **Rule:** If Finnhub is selected but skipped as ineligible, unconfigured or unavailable, coverage counts as 'limited' and Marketaux is always called, even when Yahoo already returned relevant items.
- **Given** A non-US ticker that the Finnhub market policy marks ineligible, the default sources, and Yahoo returning 6 accepted items
- **When** get_company_news runs
- **Then** A SOURCE_NOT_ELIGIBLE warning for finnhub is emitted, the fallback reason becomes HIGHER_PRIORITY_COVERAGE_LIMITED, and one Marketaux request is spent, even though Yahoo evidence exists
- **Parameters:** Limiting warning codes: SOURCE_IDENTITY_UNAVAILABLE; SOURCE_UNAVAILABLE or SOURCE_NOT_ELIGIBLE mentioning finnhub; SOURCE_UNAVAILABLE mentioning 'yahoo finance'. Limiting diagnostic statuses: UNCONFIGURED, NOT_ELIGIBLE, PROVIDER_ERROR, AUTH_ERROR, RATE_LIMITED, TIMEOUT, PROVIDER_CHANGED, or completed=false. Marketaux daily limit 100.
- **Edge cases:** Python looks for 'finnhub' in warning.source, and only in warning.message when source is empty. The Worker searches source and message joined together, so a warning with source 'x' and message 'finnhub ...' limits coverage only in the Worker.; A deployment without a Finnhub key spends Marketaux quota on every default news call
- **Suspected defect:** For markets outside the Finnhub policy, a deliberate policy skip uses up the 100/day Marketaux quota on every call.
- **SME question:** Is it intended that a deliberate policy skip of Finnhub (NOT_ELIGIBLE) counts as 'limited coverage' and spends Marketaux quota even when Yahoo found relevant items?

### BR-NWS-169 · Collection status classification (P1, Policy, confidence High)

- **Source:** `server.py:2130-2147`
- **Rule:** A result is PARTIAL when it has items but some source was unavailable, ineligible or lacked identity. An empty result is SOURCE_LIMITED_NOT_FOUND when coverage was limited (including any IR-page registry issue), NOT_FOUND when sources ran cleanly, and PROVIDER_ERROR when no source ran.
- **Given** No items. Finnhub was unconfigured (SOURCE_UNAVAILABLE warning)
- **When** status is built
- **Then** status=SOURCE_LIMITED_NOT_FOUND (not NOT_FOUND)
- **Parameters:** Limiting codes SOURCE_UNAVAILABLE, SOURCE_NOT_ELIGIBLE, SOURCE_IDENTITY_UNAVAILABLE, COMPANY_IR_PAGE_*

### BR-NWS-170 · Company IR RSS/Atom autodiscovery probe paths and budget (P1, Policy, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:485-507, 9367-9391, 10291-10308, 10344-10437`
- **Rule:** To find an official company news feed, the Worker probes up to 5 candidate hosts (the company website plus investor./investors./ir. subdomains) across 14 standard IR/news paths. It stops after 24 page fetches, after 8 feeds, or after the first host that yields any feed, and caches the result for 24 hours.
- **Given** Company AAPL with website https://www.apple.com and root apple.com
- **When** company_ir is selected and discovery runs
- **Then** Pages such as https://www.apple.com/, /rss.cfm, /rss-news-feeds, ..., /press-releases are tried in order. A page that is itself a feed counts only if its host is on the company domain. Feed links come from <link rel=alternate type=rss|atom|xml> or anchors whose href or label mentions rss/atom/feed, at most 12 per page. Discovery stops after 24 probes and emits COMPANY_IR_DISCOVERY_BUDGET_EXHAUSTED, or emits COMPANY_IR_FEED_NOT_FOUND when no feed is found. The outcome is cached for 24 h.
- **Parameters:** COMPANY_IR_DISCOVERY_MAX_PAGE_PROBES=24; candidate hosts max 5; paths: /, /rss.cfm, /rss-news-feeds, /rss-feeds, /feed/press-releases.xml, /feed/news.xml, /feed, /rss, /investor-relations, /investors, /investor, /newsroom, /news, /press-releases; feeds cap 8; links per page 12; COMPANY_IR_DISCOVERY_TTL_MS=24h; HTML max 750 KiB; feed max 2 MiB; feed TTL 15 min
- **Edge cases:** When an earlier host yields any feed, the remaining hosts are skipped (line 10415); A cached negative result re-emits the warnings without probing again; Candidate links that are on a blocked host, contain /wp-json/ or oembed, or end in .json/format=json are rejected (9398-9411)

### BR-NWS-171 · Company IR registry revalidation deadline check (P1, Policy, confidence High)

- **Source:** `server.py:999-1009; worker/src/yahoo-finance.ts:9335-9339`
- **Rule:** An approved IR page is fetched only while its revalidateAfter date has not passed. No date means always fresh. An unparseable date counts as stale.
- **Given** An approved entry with revalidateAfter '2026-10-14T00:00:00Z' and today 2026-10-15
- **When** Company IR page events are collected
- **Then** The page is skipped and COMPANY_IR_PAGE_REVALIDATION_DUE is raised
- **Parameters:** Comparison is deadline >= now (UTC); a date without a timezone is treated as UTC
- **Edge cases:** A deadline exactly equal to now is still fresh; A garbage date string fails closed (skipped)

### BR-NWS-172 · Coverage recommended next action precedence (P1, Policy, confidence Medium)

- **Source:** `server.py:314-353`
- **Rule:** The recommended next action is RETRY_RETRYABLE_SOURCES if any source is rate-limited or timed out; otherwise CHECK_OFFICIAL_RELEASES if any source failed or was skipped for coverage reasons; otherwise RETRY_TRUNCATED_SOURCE if accepted items were cut by caps; otherwise USE_RETURNED_CONTEXT.
- **Given** Finnhub OK with hasMoreAccepted=true (20 truncated) and every other source OK or EMPTY_RESULT
- **When** coverage is built
- **Then** recommendedNextAction = RETRY_TRUNCATED_SOURCE and truncatedSources lists finnhub with acceptedCount, returnedCount and truncatedCount
- **Parameters:** failed states include UNCONFIGURED, PROVIDER_ERROR, AUTH_ERROR, RATE_LIMITED, TIMEOUT, PROVIDER_CHANGED, IDENTITY_UNAVAILABLE, WEBSITE_NOT_AVAILABLE, DISCOVERY_NOT_FOUND, DISCOVERY_BUDGET_EXHAUSTED, FEED_NOT_FOUND, PARSE_ERROR, CANDIDATE_AVAILABLE, NOT_REGISTERED, DISABLED, REVALIDATION_DUE, SOURCE_VALIDATION_FAILED; skipped states NOT_ELIGIBLE, NOT_NEEDED (NOT_NEEDED does not limit)
- **Suspected defect:** Yahoo diagnostics (server.py:2224-2236) and Marketaux status omit hasMoreAccepted and truncatedCount even when acceptedCount > returnedCount, so RETRY_TRUNCATED_SOURCE under-reports truncation.
- **SME question:** Only Finnhub ever sets hasMoreAccepted. Yahoo and Marketaux also cap accepted items (Yahoo at max_results, Marketaux at 3) without reporting truncation. Should those sources also report truncation, which would trigger RETRY_TRUNCATED_SOURCE?

### BR-NWS-173 · Decision-use guidance for contextual news (P1, Policy, confidence Medium)

- **Source:** `server.py:302-311,427-430; worker/src/yahoo-finance.ts:9185-9192,9232-9246,9541-9545`
- **Rule:** Each returned item is told how it may be used: SEC Ex-99 and approved IR pages are usable as official evidence; SEC/IR/wire/Marketaux items require checking official releases; Yahoo/Finnhub news about material events (earnings, guidance, contract, financing, product, regulatory, litigation) require checking official releases; everything else is context only.
- **Given** A Finnhub item (sourceType company_news) classified eventType 'analyst'
- **When** Items are enriched for the LLM
- **Then** decisionUse = CONTEXT_ONLY; if eventType were 'earnings' it would be CHECK_OFFICIAL_RELEASES
- **Parameters:** Material event types = {earnings, guidance, contract, financing, product, regulatory, litigation}
- **Edge cases:** company_ir_page counts as USE_OFFICIAL_EVIDENCE only when approvalStatus == 'approved'
- **SME question:** Citation was corrected by referee (The rule is real and runs the same way in both runtimes. The Python citations are correct. Most of the TypeScript line numbers are off by about 20 lines.

Python, cited lines are accurate:
- server.py:302-311 (`_enrich_news_item_for_llm`):
  - `sec_ex99_found`, or `company_ir_page` with `approvalStatus == 'approved'`, gives USE_OFFICIAL_EVIDENCE.
  - `sec_filing`, `company_ir`, `company_ir_page`, `press_release`, `yahoo_finance_press_releases`, `newswire` and `marketaux_wire` give CHECK_OFFICIAL_RELEASES.
  - `yahoo_finance_news` and `company_news` (Finnhub) are passed to `_yahoo_decision_use(eventType)`.
  - Everything else gives CONTEXT_ONLY.
- server.py:427-430 (`_yahoo_decision_use`): returns CHECK_OFFICIAL_RELEASES only for {earnings, guidance, contract, financing, product, regulatory, litigation}, and CONTEXT_ONLY otherwise.
- So a Finnhub `company_news` item with eventType 'analyst' gets CONTEXT_ONLY, and with 'earnings' it gets CHECK_OFFICIAL_RELEASES. This matches the specification.

TypeScript, citations mostly wrong:
- yahoo-finance.ts:9164-9171 is a type declaration (`NewsCompanyIdentity` / `DiscoveredCompanyIrFeed`), not logic.
- 9520-9524 is part of `yahooItemIssuer`, which is unrelated.
- 9211-9225 covers `evidenceClassFor` and `urlProvenanceFor`, not decisionUse.

Where the TypeScript logic actually is:
- `sourceDecisionUse`: 9185-9192.
- `enrichNewsItemForLlm` (the approved IR page override, then `yahoo_finance_news`/`compan

### BR-NWS-174 · Dedupe survivor selection by source priority (P1, Policy, confidence High)

- **Source:** `server.py:238-251,2089-2127`
- **Rule:** Within a duplicate group, the most authoritative source wins (SEC, then IR, then press release, then newswire, then Finnhub, then Yahoo news, then Marketaux, then other). Ties go to the newer timestamp. The losers are kept as sourceRefs, and different timestamps raise a non-blocking TIMESTAMP_VARIANCE warning.
- **Given** The same event from yahoo_finance_news at 10:05 and newswire at 10:00
- **When** dedupe runs
- **Then** The newswire item is kept (rank 3 beats 5), with sourceRefs=[yahoo item], and TIMESTAMP_VARIANCE has statusImpact=false
- **Parameters:** Ranks: sec_filing/sec_ex99_found 0; company_ir/company_ir_page 1; press_release/yahoo_finance_press_releases 2; newswire 3; company_news 4; yahoo_finance/yahoo_finance_news 5; marketaux_wire 6; other 7
- **Edge cases:** Output is sorted newest first afterwards

### BR-NWS-175 · Evidence class, URL provenance and decision-use labelling per item (P1, Policy, confidence High)

- **Source:** `server.py:269-311; server.py:427-430`
- **Rule:** Every returned news item gets three labels from its source type. evidenceClass is SEC_FILING, APPROVED_IR_PAGE, OFFICIAL_RSS_ATOM, WIRE_RELEASE or CONTEXTUAL_NEWS. urlProvenance is OFFICIAL, PROVIDER, PUBLISHER or UNKNOWN. decisionUse is USE_OFFICIAL_EVIDENCE only for SEC EX-99 or an approved IR page, CHECK_OFFICIAL_RELEASES for other official, wire or Marketaux items and for material Yahoo/Finnhub event types, and CONTEXT_ONLY otherwise.
- **Given** a Finnhub item (sourceType company_news) with eventType 'earnings'
- **When** it is enriched
- **Then** evidenceClass CONTEXTUAL_NEWS, decisionUse CHECK_OFFICIAL_RELEASES. With eventType 'analyst' it would be CONTEXT_ONLY
- **Parameters:** material event types: earnings, guidance, contract, financing, product, regulatory, litigation; provider hosts finance.yahoo.com, finnhub.io
- **Edge cases:** tickerMatch is EXPLICIT for matchBasis TICKER_TOKEN, ISSUER_NAME, ISSUER_ACRONYM or PROVIDER_ENTITY_SYMBOL, or when the ticker appears as a word; SOURCE_SCOPED when a ticker is present without a match; UNVERIFIED otherwise

### BR-NWS-176 · Finnhub HTTP error classification (P1, Policy, confidence High)

- **Source:** `server.py:1744-1778,2312-2319`
- **Rule:** Finnhub HTTP 401/403 becomes AUTH_ERROR, 429 becomes RATE_LIMITED, a response that is not a list becomes PROVIDER_CHANGED, and anything else becomes PROVIDER_ERROR. Status is derived by matching warning message text.
- **Given** Finnhub returns HTTP 429
- **When** collection runs
- **Then** sourceStatus.finnhub.status=RATE_LIMITED and coverage.recommendedNextAction=RETRY_RETRYABLE_SOURCES
- **Parameters:** 401/403 -> AUTH_ERROR; 429 -> RATE_LIMITED
- **Edge cases:** The status depends on English substrings in warning messages, so rewording a message silently changes the status
- **Suspected defect:** Status classification is coupled to free-text warning messages rather than structured codes; fragile across runtimes.

### BR-NWS-177 · Finnhub market eligibility gates the expanded institutional ownership tool (news policy file reused) (P1, Policy, confidence High)

- **Source:** `yfmcp/tools/provider_gaps.py:217-226,306-324; worker/src/yahoo-finance.ts:9208-9214,2204-2222; worker/src/news-source-capabilities.json:4-9`
- **Rule:** The news-source capability file also decides whether the ownership tool may call Finnhub. Tickers ending .ST, .DE, .L or .TWO, and the ticker FOCI, are recorded as NOT_ELIGIBLE and Finnhub is never called for them.
- **Given** Ticker IQE.L, allow_scarce_fallback=false
- **When** get_expanded_institutional_ownership runs
- **Then** providerAttempts=[{provider: finnhub, status: NOT_ELIGIBLE, attempted: false, reasonCode: MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT}], response status NOT_ELIGIBLE, recommendedNextAction USE_YAHOO_OR_OFFICIAL_FILINGS
- **Parameters:** ineligibleTickerSuffixes=[.ST,.DE,.L,.TWO]; ineligibleTickers=[FOCI]; reasonCode=MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT; Python default reasonCode if missing=FINNHUB_MARKET_NOT_ELIGIBLE
- **Edge cases:** Python fails open: if the policy file cannot be read or parsed (OSError/KeyError/ValueError), every ticker counts as eligible. The Worker bundles the JSON at build time, so it cannot fail this way.; Python matches suffixes and tickers case-insensitively (upper-cases the policy values). The Worker compares the raw policy strings with the upper-cased ticker.
- **Suspected defect:** Python fails open on a missing or bad policy file, so .L/.ST tickers would then spend Finnhub calls. This is a fallback-order difference between the runtimes.

### BR-NWS-178 · Finnhub market eligibility policy (P1, Policy, confidence Medium)

- **Source:** `worker/src/news-source-capabilities.json:2-8; server.py:254-266,2476-2478,2522,2597-2605,2284-2290; worker/src/yahoo-finance.ts:9194-9200,11844-11865`
- **Rule:** Finnhub company news is never called for tickers listed on markets the deployed Finnhub plan does not cover (Stockholm, Germany, London, Taipei OTC) or for specifically excluded tickers; the source is reported as NOT_ELIGIBLE instead.
- **Given** Ticker 'ERIC-B.ST' (or 'FOCI') with 'finnhub' among the selected sources
- **When** Company news is collected
- **Then** No Finnhub request is made; a SOURCE_NOT_ELIGIBLE warning with reasonCode MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT is added and sourceStatus.finnhub = {status: NOT_ELIGIBLE, attempted: false, allowedAction: 'use_yahoo_or_official_sources'}
- **Parameters:** ineligibleTickerSuffixes = ['.ST','.DE','.L','.TWO']; ineligibleTickers = ['FOCI']; reasonCode = MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT; schemaVersion 2026-07-13
- **Edge cases:** Matching is a case-insensitive suffix test on the ticker string, so '.L' also matches any ticker ending in 'L' preceded by a dot only; a ticker like 'BRK.L' is excluded but 'ALL' is not; Python uppercases suffixes before comparing; TS compares against the raw suffix string (works only because the JSON suffixes are already uppercase)
- **SME question:** Citation was corrected by referee (The behaviour is real and runs in both runtimes, but one cited range points at the wrong code and the actual skip logic is not in the citation.

Correct parts of the citation:
- The JSON file (lines 2-8) holds schemaVersion 2026-07-13, the ineligible suffixes ['.ST','.DE','.L','.TWO'], the ineligible ticker ['FOCI'] and the reasonCode MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT.
- server.py:254-266 (_finnhub_policy and _finnhub_eligibility) compares the uppercased ticker against that list and those suffixes.
- server.py:2476-2478 computes eligibility only when finnhub is selected.
- server.py:2597-2605 adds a SOURCE_NOT_ELIGIBLE warning with attempted=False and the reasonCode.

Problems with the citation:
- worker/src/yahoo-finance.ts:9173-9179 is only TypeScript type declarations (DiscoveredCompanyIrFeed and CompanyEventCollectionResult), not policy logic. The TS eligibility function is at 9194-9200.
- The "no Finnhub request" gate is not cited. It is at server.py:2522, which calls _collect_finnhub_events only if finnhub_eligible and otherwise uses _skipped(). In TS it is at yahoo-finance.ts:11844-11865, which calls collectFinnhubEvents only when eligible, and otherwise sets the NOT_ELIGIBLE diagnostic with allowedAction 'use_yahoo_or_official_sources' and pushes the warning.
- The sourceStatus.finnhub entry {status: NOT_ELIGIBLE, attempted: False, allowedAction: 'use_yahoo_or_official_sources'} is built at server.py:2284-2290, also not cite

### BR-NWS-179 · Finnhub market eligibility policy (P1, Policy, confidence Medium)

- **Source:** `worker/src/news-source-capabilities.json:4-9; server.py:254-266, 2476-2478, 2597-2606, 2283-2290; worker/src/yahoo-finance.ts:9194-9200, 11844-11866`
- **Rule:** Finnhub is never called for tickers on markets the deployed subscription does not cover: Stockholm, Germany, London and Taiwan OTC suffixes, plus the ticker FOCI. The source is reported as NOT_ELIGIBLE, not as failed.
- **Given** ticker 'VOLV-B.ST' with finnhub selected
- **When** company events are collected
- **Then** Finnhub is not attempted. A SOURCE_NOT_ELIGIBLE warning is added with reasonCode MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT, and sourceStatus.finnhub={status:NOT_ELIGIBLE, attempted:false, allowedAction:use_yahoo_or_official_sources}
- **Parameters:** ineligibleTickerSuffixes [.ST, .DE, .L, .TWO]; ineligibleTickers [FOCI]; reasonCode MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT
- **Edge cases:** Suffix match is a plain endsWith on the upper-cased ticker, so a ticker ending in '.L' matches but '.LN' does not; Python caches the policy per process (lru_cache)
- **SME question:** Citation was corrected by referee (The behaviour is real and implemented the same way in both runtimes, but part of the citation points at the wrong lines. Two of the three locations are correct. In worker/src/news-source-capabilities.json:4-9, the finnhub policy has ineligibleTickerSuffixes [".ST", ".DE", ".L", ".TWO"], ineligibleTickers ["FOCI"] and reasonCode MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT. In server.py:254-266, _finnhub_policy loads that JSON and _finnhub_eligibility upper-cases the ticker and checks it against the ticker list and the suffixes. The third location is wrong. worker/src/yahoo-finance.ts:9173-9179 contains the CompanyEventCollectionResult type and the start of sourceDecisionUse, not the eligibility check. The TS finnhubEligibility function is actually at 9194-9200. The cited lines also only cover the eligibility check, not the skip, warning and status behaviour the spec describes. That behaviour is executable code at these places. TS yahoo-finance.ts:11844-11866: Finnhub is only collected when eligible. Otherwise sourceDiagnostics.finnhub is set to {status:NOT_ELIGIBLE, attempted:false, reasonCode, allowedAction:use_yahoo_or_official_sources} and a SOURCE_NOT_ELIGIBLE warning with attempted:false and reasonCode is added. Python server.py:2476-2478 computes eligibility. server.py:2597-2606 adds the SOURCE_NOT_ELIGIBLE warning. server.py:2283-2290 builds the sourceStatus finnhub entry: status NOT_ELIGIBLE, attempted False, allowedAction use_yahoo_or

### BR-NWS-180 · Finnhub market eligibility reused for expanded ownership, failing open in Python (P1, Policy, confidence Medium)

- **Source:** `yfmcp/tools/provider_gaps.py:27,217-226,309-324; worker/src/yahoo-finance.ts:45,2206-2210,9208-9214; worker/src/news-source-capabilities.json:1-12`
- **Rule:** The news source policy file also decides whether expanded institutional ownership may call Finnhub. Ineligible markets are recorded as NOT_ELIGIBLE without a call. If Python cannot read the policy file, it treats every ticker as eligible.
- **Given** Ticker 'IQE.L' (suffix .L), or a Python deployment where worker/src/news-source-capabilities.json is missing
- **When** get_expanded_institutional_ownership runs
- **Then** IQE.L gives a Finnhub attempt with status NOT_ELIGIBLE, attempted=false, reasonCode MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT. With the file missing, Python calls Finnhub anyway.
- **Parameters:** Ineligible suffixes .ST, .DE, .L, .TWO; ineligible ticker FOCI
- **Edge cases:** The policy path resolves to the worker/ directory two levels up from yfmcp/tools, which a packaged Python install may not contain
- **Suspected defect:** Python fails open: if the policy file is missing, Finnhub is called for markets the deployed entitlement does not cover.
- **SME question:** Should Python fail closed (treat Finnhub as ineligible or raise) when the source-policy file cannot be read, matching the Worker's bundled policy?

### BR-NWS-181 · Finnhub requires API key; otherwise UNCONFIGURED (P1, Policy, confidence High)

- **Source:** `server.py:1712-1720,2310-2311`
- **Rule:** If no Finnhub key is configured, Finnhub is skipped with a 'not configured' warning and its status is UNCONFIGURED, which makes coverage PARTIAL.
- **Given** FINNHUB_API_KEY and FINNHUB_TOKEN are both unset
- **When** an eligible ticker is collected with finnhub selected
- **Then** No call is made. Warning SOURCE_UNAVAILABLE 'Finnhub company-news source is not configured; skipped.', and sourceStatus.finnhub.status=UNCONFIGURED
- **Parameters:** Env vars FINNHUB_API_KEY or FINNHUB_TOKEN (<credential — masked, read from env>)

### BR-NWS-182 · GlobeNewswire feed set, scan order, confidence and early stop (P1, Policy, confidence High)

- **Source:** `server.py:439-492; server.py:1585-1725; worker/src/yahoo-finance.ts:437-483; worker/src/yahoo-finance.ts:11296-11399`
- **Rule:** The newswire source reads 10 fixed GlobeNewswire RSS feeds in a set order. It de-duplicates across feeds and stops as soon as max_results matching items are found. Items are HIGH confidence when they have a link or guid, otherwise MEDIUM.
- **Given** max_results 3, with matching items in the public_companies feed (2) and the earnings feed (4)
- **When** newswire is collected
- **Then** It returns the 2 public_companies items plus the first earnings item in feed order, and the remaining feeds are not read.
- **Parameters:** Feeds in order: public_companies, press_releases, earnings, stock_market_news, technology, semiconductors, telecommunications, mobile_telecommunications, telecommunications_equipment, electronic_equipment. Max feed size = 2 MiB. Dedupe key = duplicateGroupId, else feed:link-or-guid:title:publishedAt. Summary truncated to 240 characters, evidence to 180.
- **Edge cases:** The early stop picks items by feed order, not by recency; XML guard differs: Python rejects '<!doctype' and '<!entity'; the Worker rejects only '<!entity' but also requires <rss>, <channel> and </rss> and equal counts of <item> and </item>; Python has an unused filter_low_relevance parameter; used=false when any warning occurred and no items were found

### BR-NWS-183 · IR feed discovery budget and caps (P1, Policy, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:485-507; worker/src/yahoo-finance.ts:9367-9391; worker/src/yahoo-finance.ts:10310-10342; worker/src/yahoo-finance.ts:10344-10437`
- **Rule:** RSS discovery probes at most 24 pages across up to 5 candidate hosts (the site host plus investor., investors. and ir. subdomains) and 14 fixed paths. It stops at the first host that yields feeds or at 8 feeds, and caches the result for 24 hours.
- **Given** A company with website acme.com and no feeds on its first host
- **When** Discovery runs
- **Then** Paths such as /, /rss.cfm, /feed and /investors are probed on acme.com, then investor.acme.com, and so on. After 24 probes COMPANY_IR_DISCOVERY_BUDGET_EXHAUSTED is emitted, and COMPANY_IR_FEED_NOT_FOUND too if no feeds were found. A cached negative result re-emits the same warnings for 24 hours.
- **Parameters:** MAX_PAGE_PROBES=24; 14 discovery paths; max 5 hosts; max 8 feeds; max 12 links per page; discovery TTL 24h; feed TTL 15 min; HTML max 750 KB; feed max 2 MB; discovery cache 200 entries
- **Edge cases:** Links to blocked hosts, /wp-json/, oembed or .json are rejected as feed candidates; A direct feed path is only accepted if it is on the same company domain

### BR-NWS-184 · Marketaux fallback gating (only when higher-priority coverage is empty or limited) (P1, Policy, confidence Medium)

- **Source:** `server.py:2024-2086,2607-2629`
- **Rule:** Marketaux is called only when higher-priority sources (Yahoo or Finnhub) were not selected at all, failed, were ineligible or unconfigured, had no Yahoo identity, or returned no items matching the query. Otherwise it is marked NOT_NEEDED to save quota.
- **Given** Default sources and Yahoo news returned 3 matching items, with no failures
- **When** _collect_company_events evaluates marketaux
- **Then** Marketaux is not called. status=NOT_NEEDED, reasonCode HIGHER_PRIORITY_EVIDENCE_FOUND, pagesRequested=0
- **Parameters:** Priority sources: yahoo_finance, yahoo_finance_news, yahoo_finance_press_releases, finnhub. Fallback reasons: EXPLICIT_SOURCE_SELECTION, HIGHER_PRIORITY_COVERAGE_LIMITED, HIGHER_PRIORITY_EMPTY
- **Edge cases:** A Finnhub NOT_ELIGIBLE warning counts as 'limited', so Marketaux always runs for .ST/.DE/.L/.TWO tickers; With a search query, priority items count only if the query substring appears in their text
- **SME question:** P0 panel split on whether this moves money / is regulatory (The card describes the code correctly, but P0 is the wrong rating. Most of the cited lines are not the gate itself.

Faithfulness: the gate is _marketaux_fallback_reason at server.py:2066-2128, and its caller is server.py:2654-2676. Marketaux is not called when four things all hold:
- at least one priority source (yahoo_finance, yahoo_finance_news, yahoo_finance_press_releases or finnhub) was selected;
- there is no limiting warning: SOURCE_IDENTITY_UNAVAILABLE, a Finnhub SOURCE_UNAVAILABLE/SOURCE_NOT_ELIGIBLE, or a Yahoo SOURCE_UNAVAILABLE;
- no selected priority source has completed=False or a status of UNCONFIGURED, NOT_ELIGIBLE, PROVIDER_ERROR, AUTH_ERROR, RATE_LIMITED, TIMEOUT or PROVIDER_CHANGED;
- at least one priority-source item contains the search query as a substring of its title, summary, source, eventType or evidenceText.

In that case the code sets status=NOT_NEEDED, attempted=False, reasonCode=HIGHER_PRIORITY_EVIDENCE_FOUND and quotaPolicy.pagesRequested=0 (lines 2663-2676). The three fallback reasons match lines 2078, 2125 and 2127. The Given/When/Then is therefore accurate.

Citation problems:
- Lines 2024-2063 are Marketaux item normalization: entity, exchange and date-window checks, plus confidence=LOW. That is a different rule.
- Lines 2066-2086 cover only the start of the gate function.
- Lines 2607-2629 merge Yahoo results.
- The citation should be server.py:2066-2128 plus 2654-2676.

P0: not ju

### BR-NWS-185 · Marketaux fallback is query-aware for search_company_news (P1, Policy, confidence High)

- **Source:** `server.py:2083-2100,2126-2127; worker/src/yahoo-finance.ts:11687-11695,11712`
- **Rule:** For keyword searches, a higher-priority Yahoo or Finnhub item only counts as 'evidence found' (which suppresses the paid Marketaux call) if it actually contains the search query. Otherwise Marketaux is called with reason HIGHER_PRIORITY_EMPTY.
- **Given** search_company_news(ticker='AAPL', query='buyback'), and Yahoo returns 8 accepted AAPL items, none of which contain 'buyback' in title, summary, source, eventType or evidenceText
- **When** the Marketaux fallback decision runs
- **Then** Marketaux is called with fallbackReason=HIGHER_PRIORITY_EMPTY and uses one quota unit. If any one priority item contained 'buyback', Marketaux would be NOT_NEEDED.
- **Parameters:** Priority sources: yahoo_finance, yahoo_finance_news, yahoo_finance_press_releases, finnhub (plus sourceType company_news). Match is a lowercase substring.
- **Edge cases:** An empty query means any priority item suppresses the fallback; The match is a plain substring, so 'buy' matches 'buyback' and 'buyer'

### BR-NWS-186 · Marketaux fallback trigger (only when higher-priority sources are empty or limited) (P1, Policy, confidence Medium)

- **Source:** `server.py:2024-2086; server.py:2607-2648`
- **Rule:** Marketaux (quota-limited) is called only when no Yahoo or Finnhub source was selected, when a higher-priority source was limited, or when those sources returned nothing matching the query. Otherwise it is marked NOT_NEEDED and not called.
- **Given** sources = default (yahoo_finance_news, yahoo_finance_press_releases, finnhub, marketaux) and Yahoo returned 3 accepted items, with Finnhub OK
- **When** collection reaches the Marketaux step
- **Then** marketaux.status = NOT_NEEDED, attempted=false, reasonCode HIGHER_PRIORITY_EVIDENCE_FOUND, pagesRequested 0. If Finnhub were NOT_ELIGIBLE, Marketaux would run with fallbackReason HIGHER_PRIORITY_COVERAGE_LIMITED. If Yahoo and Finnhub had 0 items, it would run with HIGHER_PRIORITY_EMPTY.
- **Parameters:** fallbackReason enum: EXPLICIT_SOURCE_SELECTION, HIGHER_PRIORITY_COVERAGE_LIMITED, HIGHER_PRIORITY_EMPTY; priority sources yahoo_finance, yahoo_finance_news, yahoo_finance_press_releases, finnhub; limiting statuses UNCONFIGURED, NOT_ELIGIBLE, PROVIDER_ERROR, AUTH_ERROR, RATE_LIMITED, TIMEOUT, PROVIDER_CHANGED, or completed=false
- **Edge cases:** With search_query set, priority items count only if the query substring appears in title, summary, source, eventType or evidenceText; A SOURCE_IDENTITY_UNAVAILABLE warning on its own forces the fallback; NOT_NEEDED is listed under coverage.skippedSources but does not lower coverage (server.py:339)
- **SME question:** P0 panel split on whether this moves money / is regulatory (I read the code myself. The logic is in _marketaux_fallback_reason (server.py:2066-2128) and the caller step (server.py:2654-2695). The cited range 2024-2086 is mostly the end of the Marketaux item filter, not the decision function, so the citation should be corrected to 2066-2128 plus 2654-2695.

Faithfulness: the card is broadly accurate. Marketaux runs with EXPLICIT_SOURCE_SELECTION when none of yahoo_finance, yahoo_finance_news, yahoo_finance_press_releases or finnhub is selected (2076-2078). It runs with HIGHER_PRIORITY_COVERAGE_LIMITED when any selected priority source has completed=false or one of the seven listed statuses (2115-2125). It runs with HIGHER_PRIORITY_EMPTY when there are no priority items matching the query (2126-2127). Otherwise it gets NOT_NEEDED, attempted=false, reasonCode HIGHER_PRIORITY_EVIDENCE_FOUND and pagesRequested 0 (2662-2676). The Finnhub NOT_ELIGIBLE example holds: the SOURCE_NOT_ELIGIBLE warning with source=finnhub (2644-2652) sets limited (2105-2106).

The card leaves out some details:
- The limited check runs before the empty check, so it wins when both apply.
- Warnings also set limited: SOURCE_IDENTITY_UNAVAILABLE, or SOURCE_UNAVAILABLE that mentions Yahoo Finance.
- An item counts as priority evidence if its sourceType is company_news.
- The NOT_NEEDED example only holds if the 3 Yahoo items contain the search query (if one is given) and there was no identity-unavailable warn

### BR-NWS-187 · Marketaux free-tier quota cap (P1, Policy, confidence High)

- **Source:** `server.py:438,1855,1868-1873,1891-1903`
- **Rule:** A Marketaux call fetches a single page of at most 3 articles, English only, restricted to the BusinessWire, PR Newswire and GlobeNewswire domains, with entity filtering on the exact symbol. It never paginates.
- **Given** max_results=10
- **When** Marketaux fallback runs
- **Then** limit=3, page=1, domains=businesswire.com,prnewswire.com,globenewswire.com, published_after=start_date or (today-14d) at T00:00:00, published_before=end_date or today at T23:59:59
- **Parameters:** MARKETAUX_MAX_ITEMS=3; dailyLimit=100 (reported only, not enforced); timeout 12s; token from MARKETAUX_API_TOKEN/MARKETAUX_API_KEY (<credential — masked>)
- **Edge cases:** The 100/day quota is only metadata; nothing counts calls
- **SME question:** Should the 100/day Marketaux quota actually be enforced (counter) rather than only advertised?

### BR-NWS-188 · Marketaux quota and acceptance rules (P1, Policy, confidence High)

- **Source:** `server.py:437-438,1836-2021; worker/src/yahoo-finance.ts:69-72,11498-11524`
- **Rule:** Marketaux is called for a single page of at most 3 English articles from Business Wire, PR Newswire or GlobeNewswire; an article is accepted only with a title, an https URL on those domains, an entity whose symbol equals the ticker, a compatible exchange, and a date in window; accepted items are always LOW confidence contextual evidence.
- **Given** Ticker NVDA, max_results 10, Marketaux returns 3 rows, one hosted on reuters.com
- **When** Marketaux is queried
- **Then** limit=3, page=1; the reuters row is rejected DOMAIN_NOT_ALLOWED; accepted rows get matchBasis PROVIDER_ENTITY_SYMBOL, confidence LOW, tickerRelevance HIGH, sourceType marketaux_wire
- **Parameters:** MARKETAUX_MAX_ITEMS = 3; dailyLimit 100 (declared); pages 1; no pagination; domains businesswire.com, prnewswire.com, globenewswire.com; window published_after from_day T00:00:00, published_before to_day T23:59:59; TS cache TTL 15 min; HTTP 401/403 -> AUTH_ERROR, 402/429 -> RATE_LIMITED; API token <credential — masked, env MARKETAUX_API_TOKEN, redacted in providerUrl>
- **Edge cases:** Exchange mismatch rejected only when both expected and provider exchanges normalise; Default from-day = now - lookback (14) when no start_date

### BR-NWS-189 · Marketaux quota and request bounds (P1, Policy, confidence Medium)

- **Source:** `server.py:438; server.py:1855-1874; server.py:1894-1903`
- **Rule:** Each Marketaux call fetches one page of at most 3 articles from the three major wire domains, with no automatic pagination, under a declared daily limit of 100.
- **Given** get_company_news max_results=10 triggers the Marketaux fallback
- **When** Marketaux is called
- **Then** The request uses limit=3, page=1, domains businesswire.com, prnewswire.com and globenewswire.com, filter_entities=true, language=en. quotaPolicy reports dailyLimit 100, maxArticlesPerRequest 3, pagesRequested 1, automaticPagination false
- **Parameters:** _MARKETAUX_MAX_ITEMS=3; dailyLimit=100 (declared only, not enforced); timeout 12s; API token from env MARKETAUX_API_TOKEN / MARKETAUX_API_KEY <credential — masked, env only; URL logged with api_token=REDACTED>
- **Edge cases:** Default lookback is 14 days when start_date is empty
- **SME question:** dailyLimit=100 is only reported, never counted or enforced. Is a per-day call counter required, or is the provider's 402/429 response enough?

### BR-NWS-190 · Marketaux wire fallback trigger (P1, Policy, confidence High)

- **Source:** `server.py:2024-2086,2607-2648; worker/src/yahoo-finance.ts:11642`
- **Rule:** Marketaux is queried only if no Yahoo/Finnhub source was selected, or those sources were limited (unavailable, ineligible, unconfigured, errored, identity unavailable), or they returned no items matching the search query; otherwise it is marked NOT_NEEDED.
- **Given** Default sources and Yahoo returned 3 matching items with no warnings
- **When** Collection reaches the Marketaux step
- **Then** No Marketaux call; status NOT_NEEDED, reasonCode HIGHER_PRIORITY_EVIDENCE_FOUND, pagesRequested 0
- **Parameters:** Fallback reasons: EXPLICIT_SOURCE_SELECTION, HIGHER_PRIORITY_COVERAGE_LIMITED, HIGHER_PRIORITY_EMPTY; limiting statuses UNCONFIGURED, NOT_ELIGIBLE, PROVIDER_ERROR, AUTH_ERROR, RATE_LIMITED, TIMEOUT, PROVIDER_CHANGED
- **Edge cases:** Coverage-limited check is evaluated before emptiness; Query matching is lowercase substring on title/summary/source/eventType/evidenceText

### BR-NWS-191 · Provider failure to recommended-action mapping (inverted Alpha flag) (P1, Policy, confidence Medium)

- **Source:** `yfmcp/tools/provider_gaps.py:57-72,368-372; worker/src/yahoo-finance.ts:2043-2052,2253-2255`
- **Rule:** Each provider failure status maps to one next action. For ENTITLEMENT_REQUIRED, the ownership tool suggests USE_ALPHA_VANTAGE_EXPLICITLY only when the caller has NOT already allowed the Alpha fallback.
- **Given** Finnhub returns HTTP 403 with 'premium' in the body (ENTITLEMENT_REQUIRED), allow_scarce_fallback=false
- **When** The ownership tool builds its failure payload
- **Then** recommendedNextAction=USE_ALPHA_VANTAGE_EXPLICITLY. With allow_scarce_fallback=true, the Alpha result's status drives the action, and ENTITLEMENT_REQUIRED there maps to CHECK_PROVIDER_ENTITLEMENT.
- **Parameters:** SOURCE_UNCONFIGURED->CONFIGURE_PROVIDER; AUTH_ERROR->CHECK_PROVIDER_CREDENTIALS; RATE_LIMIT->RETRY_AFTER_PROVIDER_RESET; TIMEOUT->RETRY_PROVIDER; NOT_ELIGIBLE->USE_YAHOO_OR_OFFICIAL_FILINGS; NOT_FOUND->VERIFY_TICKER_AND_DATE; other->RETRY_OR_USE_PRIMARY_PROVIDER
- **Edge cases:** PROVIDER_IDENTITY_MISMATCH in the ownership tool falls through to RETRY_OR_USE_PRIMARY_PROVIDER. The put/call tool uses RETRY_OR_VERIFY_PROVIDER_RESPONSE for the same status.
- **SME question:** Is alpha_allowed = not allow_scarce_fallback intentional, meaning 'suggest enabling Alpha'? And should PROVIDER_IDENTITY_MISMATCH give the same action in both provider-gap tools?

### BR-NWS-192 · Recommended next action (P1, Policy, confidence High)

- **Source:** `server.py:314-353`
- **Rule:** The recommended action is retry when a source was rate-limited or timed out. Otherwise it is check official releases when any source failed or was skipped for a coverage reason. Otherwise retry the truncated source if results were cut off. Otherwise use the returned context. A caller can override this (press releases use USE_OFFICIAL_EVIDENCE when decision-grade).
- **Given** Finnhub returned 25 accepted items but 10 were returned, and nothing failed
- **When** coverage is built
- **Then** recommendedNextAction=RETRY_TRUNCATED_SOURCE
- **Parameters:** Failed states list at server.py:315-320; skip states NOT_ELIGIBLE, NOT_NEEDED (NOT_NEEDED is not coverage-limiting)
- **Edge cases:** The failed list includes WEBSITE_NOT_AVAILABLE, DISCOVERY_* and FEED_NOT_FOUND, which this file's news paths never emit

### BR-NWS-193 · Remembered provider denial window (Worker only) (P1, Policy, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:237-265,276-278,336-339,355-364,314-316`
- **Rule:** After Finnhub (per endpoint) or Alpha Vantage (shared quota key) refuses with an entitlement or auth error, the Worker stops calling that endpoint for 6 hours. A daily rate limit blocks calls until the next UTC midnight. Repeat calls return the remembered refusal without spending quota.
- **Given** Finnhub /stock/ownership returns 403 'premium' at 2026-10-15T10:00Z
- **When** A second ownership call arrives at 12:00Z
- **Then** The cached denial is returned: providerAttempted=false, cacheStatus HIT_PROCESS, deniedUntil 2026-10-15T16:00Z, message suffixed '(remembered until ...; not retried)'
- **Parameters:** PROVIDER_ENTITLEMENT_DENIAL_TTL_MS=6h; daily rate-limit TTL = max(60s, time to next UTC midnight); daily detected by /per day|daily/i in the message; denial cache capacity 100; Finnhub denial key per endpoint path; Alpha denial key 'alpha_vantage:quota' (shared across all functions)
- **Edge cases:** Non-daily RATE_LIMIT, TIMEOUT and PROVIDER_ERROR are not remembered.; Alpha remembers only refusals sent as messages in an HTTP 200 body, not HTTP errors.; Python market_providers has no equivalent, so every Python call retries the provider.
- **Suspected defect:** Parity gap: the Python runtime keeps no denial memory, so outputs (cacheStatus, providerAttempted, deniedUntil) differ between runtimes after a refusal.

### BR-NWS-194 · Result count and lookback clamping (P1, Policy, confidence High)

- **Source:** `server.py:560-565`
- **Rule:** Requested result counts are clamped to 1-100 and lookback windows to 1-3650 days. A zero or missing value falls back to the tool's default.
- **Given** max_results=500, lookback_days=0 (tool default 14)
- **When** events are collected
- **Then** max_results becomes 100 and lookback becomes 14 days
- **Parameters:** max_results range 1..100; lookback range 1..3650 days; defaults per tool (news 10/14, press releases 20/90, timeline 50/365, verify 50/365)
- **Edge cases:** Negative values become 1; 0 counts as 'missing' and takes the default rather than being clamped to 1

### BR-NWS-195 · SEC event collection stops at the result cap in filing order (P1, Policy, confidence High)

- **Source:** `server.py:1346-1379; worker/src/yahoo-finance.ts:11094-11120`
- **Rule:** SEC events are read only from EDGAR's 'recent' filings list, in the order SEC returns them (newest first). Collection stops once the capped number of in-window matching filings is reached.
- **Given** max_results 10 (cap 1-100), 25 matching 8-Ks inside the lookback window
- **When** SEC events are collected
- **Then** The 10 newest matching filings are returned. Filings outside the date window are skipped and do not count toward the cap.
- **Parameters:** Cap = the request's max_results after clamping to 1-100 (default 10). Source list = submissions.filings.recent only.
- **Edge cases:** Older filings that exist only in EDGAR's paged history files are never read; When no CIK or no submissions are found, the source reports SOURCE_UNAVAILABLE and used=false; A missing acceptanceDateTime falls back to filingDate T00:00:00Z with warning PUBLISHED_AT_ESTIMATED

### BR-NWS-196 · Source coverage FULL vs PARTIAL (P1, Policy, confidence High)

- **Source:** `server.py:2398-2408`
- **Rule:** Coverage is PARTIAL if any source status is unconfigured, ineligible, errored, rate-limited, timed out, changed, missing identity, or has a non-approved IR-page registry state. Otherwise it is FULL.
- **Given** sourceStatus {yahoo_finance_news: OK, finnhub: NOT_ELIGIBLE}
- **When** coverage is computed
- **Then** sourceCoverage=PARTIAL
- **Parameters:** PARTIAL states: UNCONFIGURED, NOT_ELIGIBLE, PROVIDER_ERROR, AUTH_ERROR, RATE_LIMITED, TIMEOUT, PROVIDER_CHANGED, IDENTITY_UNAVAILABLE, CANDIDATE_AVAILABLE, NOT_REGISTERED, DISABLED, REVALIDATION_DUE, SOURCE_VALIDATION_FAILED
- **Edge cases:** NOT_NEEDED (Marketaux skipped) and EMPTY_RESULT keep coverage FULL

### BR-NWS-197 · Verification failure-mode classification and retryability (P1, Policy, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:11056-11076, 12499-12510, 12530-12532; server.py:2458-2480, 3446`
- **Rule:** When a verification finds nothing and a source was impaired, the failure reason is the first match in a fixed order. Only subrequest-limit, rate-limit and timeout failures are marked retryable.
- **Given** verify_company_event returns NOT_FOUND, Finnhub status RATE_LIMITED and Marketaux UNCONFIGURED
- **When** The failure mode is computed
- **Then** failureMode=RATE_LIMITED, retryable=true, and status becomes SOURCE_LIMITED_NOT_FOUND with a recommendedAction to retry while excluding failed providers.
- **Parameters:** precedence: WORKER_SUBREQUEST_LIMIT ('too many subrequests' in SOURCE_UNAVAILABLE text) > RATE_LIMITED > PROVIDER_TIMEOUT > PROVIDER_CHANGED > YAHOO_IDENTITY_UNAVAILABLE > SOURCE_UNCONFIGURED > SOURCE_NOT_ELIGIBLE > PROVIDER_ERROR; retryable set = {WORKER_SUBREQUEST_LIMIT, RATE_LIMITED, PROVIDER_TIMEOUT}
- **Edge cases:** failureMode is null unless the final status is SOURCE_LIMITED_NOT_FOUND; Company IR statuses such as FEED_NOT_FOUND or DISCOVERY_BUDGET_EXHAUSTED do not count as failure modes, so a verification with only those stays NOT_FOUND
- **SME question:** Should company_ir discovery limits (DISCOVERY_BUDGET_EXHAUSTED, PARSE_ERROR) also upgrade NOT_FOUND to SOURCE_LIMITED_NOT_FOUND, given they make coverage PARTIAL?

### BR-NWS-198 · Which Yahoo items are kept depends on the selected Yahoo source names (P1, Policy, confidence High)

- **Source:** `server.py:2517-2522; server.py:2608-2635; worker/src/yahoo-finance.ts:11788-11836`
- **Rule:** If only yahoo_finance_news is selected, items in the news feed that Yahoo labels as press releases are dropped. The legacy 'yahoo_finance' name keeps both labels. In Python only, the legacy 'company_ir' name also pulls both labels from the Yahoo feed.
- **Given** The Yahoo news feed returns 5 STORY items and 2 PRESS_RELEASE items, all matching the issuer
- **When** sources=['yahoo_finance_news'] vs sources=['yahoo_finance'] vs sources=['company_ir']
- **Then** yahoo_finance_news -> 5 items. yahoo_finance -> 7 news-feed items plus the issuer-wire press-release feed items. company_ir -> 7 items in Python; in the Worker, company_ir comes from official RSS discovery instead.
- **Parameters:** Selector source names: yahoo_finance_news, yahoo_finance_press_releases, yahoo_finance (legacy), company_ir (legacy; Python only for Yahoo)
- **Edge cases:** Items from the press-release feed are always added when that feed runs, without a per-item source check; The legacy yahoo_finance diagnostics add up raw, retrieved, filtered, accepted and rejected counts and the rejection reasons from both feeds. They report completed=true only if both feeds completed (server.py:2727-2748; worker 11838-11856)

### BR-NWS-199 · Yahoo news feed supplies at most 20 raw items per fetch (P1, Policy, confidence High)

- **Source:** `server.py:1551-1553; server.py:1485-1487; worker/src/yahoo-finance.ts:1726-1733; worker/src/yahoo-finance.ts:11202`
- **Rule:** Both the Yahoo news source and the Yahoo press-release source read the same Yahoo news search feed, which is asked for only 20 articles. So at most 20 Yahoo items can be considered, whatever max_results says.
- **Given** A request with max_results 100 and sources ['yahoo_finance_news']
- **When** Yahoo items are collected
- **Then** At most 20 raw articles are evaluated, and fewer survive the content-type, date-window and identity filters, so the result can never exceed 20 Yahoo items.
- **Parameters:** news_count / newsCount = 20; quotesCount = 0; fuzzy query disabled
- **Edge cases:** Press releases are derived from the same 20 articles (issuer wire releases only), not from a separate tab, because Yahoo retired the press-release tab endpoint; With a long lookback, older stories are unreachable because the feed is limited to 20

### BR-NWS-200 · extract_analyst_valuation_methods inputs, window and warnings (P1, Policy, confidence Medium)

- **Source:** `server.py:10878-10904,10960-10966; worker/src/yahoo-finance.ts:15851-15871`
- **Rule:** The tool reads up to 100 company-news items and the analyst rating changes over a 1-365 day window (default 30). It attributes targets using the issuer name and warns when the name, news or rating changes are unavailable.
- **Given** days_back = 500 and an issuer whose name lookup fails
- **When** the tool runs
- **Then** windowDays = 365, attribution.checked = false, and an ATTRIBUTION_UNCHECKED warning is returned
- **Parameters:** days_back default 30, clamp 1-365; news max_results 100
- **Edge cases:** Python takes one name (longName, else shortName). The Worker uses longName, shortName and companyName together, so the alias sets differ; The Worker also flags NEWS_UNAVAILABLE when the news code is INPUT_VALIDATION_ERROR; Python flags it when the news call raised
- **Suspected defect:** Parity gap: Python builds aliases from a single issuer name, while the Worker uses up to three names. The same headline can be attributed in one runtime and rejected in the other.
- **SME question:** Should Python pass all of longName, shortName and companyName as issuer names, as the Worker does, so the subject aliases and CLAUSE/ITEM attribution match across runtimes?

### BR-NWS-201 · Analyst valuation window clamp and data-availability warnings (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:15851-15872; server.py:10878-10903`
- **Rule:** The look-back window is clamped to 1-365 days (default 30), up to 100 news items are scanned, and warnings are raised when the company name cannot be resolved (ATTRIBUTION_UNCHECKED), news cannot be read (NEWS_UNAVAILABLE) or rating changes cannot be read (RATING_CHANGES_UNAVAILABLE).
- **Given** days_back = 900
- **When** extract_analyst_valuation_methods runs
- **Then** windowDays = 365 and get_company_news is called with max 100 results over 365 days
- **Parameters:** days_back default 30, min 1, max 365; news max_results 100
- **Edge cases:** Worker builds aliases from up to three names (longName, shortName, companyName); Python uses one (longName else shortName)
- **Suspected defect:** Runtime divergence: in the Worker a thrown getCompanyNews/getAnalystUpgradeRadar call is caught as {} so NO NEWS_UNAVAILABLE / RATING_CHANGES_UNAVAILABLE warning is raised and the result can read as NOT_FOUND; Python raises the warning on exception. Worker also raises NEWS_UNAVAILABLE on code INPUT_VALIDATION_ERROR, Python does not check that code. Subject alias sets also differ (3 names vs 1), so attribution can differ between runtimes.

### BR-NWS-202 · Approved IR page HTML item extraction rules (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:10050-10061, 10169-10203, 10265-10278; server.py:1098-1133`
- **Rule:** News items on an approved IR HTML page come from <article> blocks. If the page has none, the parser falls back to <li> blocks containing release words. At most 50 blocks are read, titles under 6 characters are skipped, and an item whose link falls outside the registry allow-list is dropped.
- **Given** An approved IR page with 60 <article> blocks, one titled 'Q3' and one linking to https://othersite.com/x
- **When** The page is parsed
- **Then** Only the first 50 blocks are read. The 'Q3' item is skipped (title shorter than 6 characters). The off-allow-list item is dropped. The rest are emitted with confidence HIGH and tickerRelevance HIGH, then filtered by date window and capped at maxResults.
- **Parameters:** block cap 50; min title length 6; li keywords: press release, news release, earnings, results, announces, reports
- **Edge cases:** Feed-shaped responses use the RSS/Atom parser regardless of adapter; Worker JSON parsing also reads content_text for the summary; Python does not (10160 vs server.py:1086); Items without a date pass the window only if withinDateWindow accepts null

### BR-NWS-203 · Approved IR page content-type must match adapter (Worker only) (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:10012-10048; server.py:1136-1159`
- **Rule:** The Worker rejects an approved IR page whose response content type does not suit its registry adapter. Python performs no content-type check.
- **Given** Registry entry adapter=html_article and a response with Content-Type application/json
- **When** The approved page is fetched
- **Then** Worker: fetch fails with 'unexpected HTML content type', emits COMPANY_IR_PAGE_VALIDATION_FAILED and sets status SOURCE_VALIDATION_FAILED. Python: the page is parsed anyway.
- **Parameters:** structured adapter allows json|xml|rss|atom|text/plain; html_article allows html|text/plain; an empty content-type is allowed; max 750 KiB
- **Edge cases:** The Worker also checks the declared Content-Length before reading; The Accept headers differ: Worker includes rss+xml/atom+xml
- **Suspected defect:** Python/TS divergence: the same registry entry can be SOURCE_VALIDATION_FAILED in one runtime and APPROVED_SOURCE_OK in the other.

### BR-NWS-204 · Batch news ticker count limit (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:65, 1297-1307, 11975-11977; worker/src/tools.ts:533; server.py:8328-8332`
- **Rule:** The Worker rejects a get_company_news batch of more than 5 tickers. The tool description says extra tickers are silently truncated, and Python applies no limit.
- **Given** ticker=['AAPL','MSFT','NVDA','AMZN','META','GOOG']
- **When** get_company_news is called
- **Then** Worker returns INPUT_VALIDATION_ERROR 'Too many tickers: 6. Maximum is 5 per call.' Python fetches all 6, three at a time. The description says only the first 5 are processed.
- **Parameters:** MAX_TICKERS=5; batch concurrency 3 (Python)
- **Edge cases:** The JSON schema maxItems:5 may reject the call at the client before either runtime sees it
- **Suspected defect:** Three-way inconsistency between the Worker code, the tool description and Python.
- **SME question:** What is the intended behavior for more than 5 tickers: reject, truncate to 5, or allow all?

### BR-NWS-205 · Batch news ticker list must be non-empty with no blank symbols (P1, Validation, confidence High)

- **Source:** `server.py:8261-8278`
- **Rule:** When get_company_news gets a list of tickers, the list must not be empty and no entry may be blank or a non-string. Each ticker is then fetched on its own and the results are keyed by ticker.
- **Given** ticker=['AAPL','']
- **When** get_company_news is called
- **Then** INPUT_VALIDATION_ERROR 'ticker is required and must not contain empty symbols.' For ['AAPL','MSFT'], each ticker runs separately and the result is {AAPL:{...},MSFT:{...}}
- **Parameters:** none
- **Edge cases:** One ticker failing does not zero out the others (per-ticker union)

### BR-NWS-206 · Broker attribution of a news item (P1, Validation, confidence High)

- **Source:** `worker/src/capital-structure.ts:2814-2823,2936-2946,2995-2996,3008; yfmcp/capital_structure.py:3234-3243,3304-3305,3318`
- **Rule:** The analyst firm credited for a news item is the earliest-mentioned firm from the rating-change firms plus a fixed broker list, matched case-sensitively as a whole word, with the longer name winning a tie at the same position.
- **Given** Rating changes list firm 'Cantor Fitzgerald' and the item text 'Cantor Fitzgerald and Benchmark lift targets'
- **When** the firm is attributed
- **Then** firm = 'Cantor Fitzgerald' (earliest match); 'benchmark' in lower case would never match
- **Parameters:** Fixed BROKERS list of ~70 firm names (Morgan Stanley ... Arctic); rating-change firms are prepended and de-duplicated case-insensitively
- **Edge cases:** Only one firm is credited per item even if several are named; A firm outside the list and outside rating changes is never recognised

### BR-NWS-207 · Company IR feed and item acceptance by domain or identity (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:10566-10577; worker/src/yahoo-finance.ts:10485-10510; worker/src/yahoo-finance.ts:9357-9365; worker/src/yahoo-finance.ts:9457-9473`
- **Rule:** A discovered feed or item is accepted if it is on the company's own domain or mentions the ticker or a significant company-name word. Confidence is HIGH for same-domain items with a URL, MEDIUM for other items with a URL, and LOW without a URL.
- **Given** A feed on news.acmeinc.com (same root as acmeinc.com) and an external feed whose title is 'Industry Digest'
- **When** Feeds are evaluated
- **Then** The same-domain feed is accepted, and its items get confidence HIGH (with URL) and tickerRelevance HIGH. The external feed is rejected with COMPANY_IR_RSS_HOST_REJECTED unless the ticker or a name token of 4+ characters (excluding corp/inc/group/holdings/technology etc.) appears in its title or first 2,000 characters.
- **Parameters:** Name token min length 4; host root strips investors./investor./ir./newsroom./news.; external feed check scans first 2,000 chars
- **Edge cases:** Feeds containing <!ENTITY are rejected as parse errors

### BR-NWS-208 · Company IR page fetch size and redirect limits (P1, Validation, confidence High)

- **Source:** `server.py:1136-1159`
- **Rule:** An approved IR page is fetched only if its canonical URL and its final redirect target both pass the registry allow-list. Responses over 750 KB are rejected.
- **Given** An approved page that redirects to an off-list host, or returns 800 KB
- **When** The IR page is fetched
- **Then** The fetch fails and COMPANY_IR_PAGE_VALIDATION_FAILED is raised, so the source status becomes SOURCE_VALIDATION_FAILED
- **Parameters:** _COMPANY_IR_PAGE_MAX_BYTES = 750*1024 (768,000 bytes); timeout 15s
- **Edge cases:** Exactly 768,000 bytes is allowed; 768,001 is rejected

### BR-NWS-209 · Company website must be an official domain (blocked aggregator hosts) (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:508-524, 9393-9396, 9413-9455, 9349-9355`
- **Rule:** The company website from Yahoo profile data is used for IR discovery only when it is an http/https URL on a host that is not a known aggregator, social or regulator site. It is then reduced to its root, with investor/ir/news prefixes stripped.
- **Given** A Yahoo profile website of 'linkedin.com/company/acme', or an empty website, or 'ftp://acme.com'
- **When** resolveNewsCompanyIdentity normalizes the website
- **Then** The website is set to null and warning COMPANY_WEBSITE_NOT_AVAILABLE is raised. For 'investors.acme.com/x?y' the result is url=https://investors.acme.com/, host=investors.acme.com, root=acme.com.
- **Parameters:** Blocked hosts (including subdomains): bloomberg.com, facebook.com, finance.yahoo.com, google.com, instagram.com, linkedin.com, marketwatch.com, nasdaq.com, reuters.com, sec.gov, twitter.com, wikipedia.org, x.com, yahoo.com, youtube.com; root-strip prefixes: investors., investor., ir., newsroom., news.
- **Edge cases:** A bare domain with no scheme gets https:// added; Prefix stripping runs in sequence, so 'news.ir.acme.com' would not fully strip because 'ir.' is checked before 'news.'

### BR-NWS-210 · Company website must be an official, non-aggregator domain (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:9413-9455; worker/src/yahoo-finance.ts:508-524; worker/src/yahoo-finance.ts:9393-9396`
- **Rule:** The company website taken from profile data is only used for IR discovery if it is a well-formed http(s) URL whose host is not a known aggregator, social network or regulator.
- **Given** A profile website of 'www.linkedin.com/company/acme'
- **When** normalizeCompanyWebsite runs
- **Then** COMPANY_WEBSITE_NOT_AVAILABLE ('host is not an official company domain'), so company_ir status is WEBSITE_NOT_AVAILABLE. A bare 'acme.com' gets https:// added and is reduced to its root 'https://acme.com/'.
- **Parameters:** Blocked hosts (including subdomains): bloomberg.com, facebook.com, finance.yahoo.com, google.com, instagram.com, linkedin.com, marketwatch.com, nasdaq.com, reuters.com, sec.gov, twitter.com, wikipedia.org, x.com, yahoo.com, youtube.com
- **Edge cases:** Empty website, non-http scheme and malformed URL all produce the same warning code; Path, query and fragment are dropped

### BR-NWS-211 · Company-name token matching for IR identity (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:9457-9473`
- **Rule:** Text matches the company if it contains the ticker as a whole word, or any whole word of 4+ letters from the company's short/long name that is not a generic corporate word.
- **Given** Ticker NVDA, name 'NVIDIA Corporation'
- **When** Feed text 'Nvidia unveils new GPU' is checked
- **Then** It matches on token 'nvidia'. Text containing only 'Corporation' or 'Technologies' does not match.
- **Parameters:** min token length 4; stopwords: corp, corporation, inc, ltd, limited, llc, plc, co, company, group, holdings, technology, technologies
- **Edge cases:** Common words of 4+ letters in a name (e.g. 'Apple', 'General') can cause false positives; This matcher is looser than the Yahoo strict identity matcher catalogued separately

### BR-NWS-212 · Date window filter (P1, Validation, confidence High)

- **Source:** `server.py:904-921`
- **Rule:** An item is in range only if it has a timestamp, its date (YYYY-MM-DD) is on or after start_date, on or before end_date, and on or after today (UTC) minus lookback days.
- **Given** Today 2026-10-15, lookback 14, an item dated 2026-09-30
- **When** the window is checked
- **Then** It is rejected (cutoff 2026-10-01). An item dated 2026-10-01 is kept
- **Parameters:** Comparison is a lexical comparison of date strings; cutoff in UTC
- **Edge cases:** Items with no timestamp are always excluded; When both explicit dates and a lookback are given, all three bounds apply

### BR-NWS-213 · Date window filter with lookback (P1, Validation, confidence Medium)

- **Source:** `server.py:904-921; worker/src/yahoo-finance.ts:9840-9850`
- **Rule:** An item is kept only if it has a publish date, falls on/after start date and on/before end date, and is no older than the lookback period (all three apply together, by UTC calendar day).
- **Given** search_company_news called on 2026-10-15 with start_date 2026-09-01 (lookback fixed at 14 days)
- **When** Items are filtered
- **Then** Only items dated 2026-10-01 or later are kept; items between 2026-09-01 and 2026-09-30 are dropped
- **Parameters:** Comparison on first 10 chars (YYYY-MM-DD); cutoff = now - lookbackDays days
- **Edge cases:** Items with no publishedAt are always dropped; Date strings compared lexically
- **Suspected defect:** search_company_news hard-codes lookback_days=14 (server.py:2754, yahoo-finance.ts:11975) and the lookback is AND-ed with explicit start/end dates, so a caller-supplied start_date older than 14 days is silently truncated.
- **SME question:** Should an explicit start_date override the 14-day default lookback in search_company_news?

### BR-NWS-214 · Declared news output schema allows status only OK/PARTIAL, but tools emit many other statuses and never OK (P1, Validation, confidence Medium)

- **Source:** `yfmcp/schemas.py:48-72; yfmcp/schemas.py:271; yfmcp/schemas.py:722-726; server.py:2172-2189; server.py:2986-3001; server.py:8349; worker/src/tools.ts:608-616; worker/src/tools.ts:777`
- **Rule:** Python publishes a news/event output schema in which 'status' is limited to OK or PARTIAL, and applies it to company news, search, press releases, SEC events, the timeline and verify. At runtime those tools emit statuses such as NOT_FOUND, SOURCE_LIMITED_NOT_FOUND, PROVIDER_ERROR, SEC_EX99_FOUND and CONFIRMED, and leave status out entirely on a clean success. The Worker's news schema declares no status field.
- **Given** get_company_news for a ticker where every selected source succeeds but returns nothing
- **When** The payload is produced
- **Then** status = 'NOT_FOUND', which is outside the declared enum [OK, PARTIAL]. On a clean non-empty result, status is omitted rather than 'OK'.
- **Parameters:** Declared enum = [OK, PARTIAL]; runtime collection statuses = PARTIAL, SOURCE_LIMITED_NOT_FOUND, NOT_FOUND, PROVIDER_ERROR, or omitted; press release adds SEC_EX99_FOUND, APPROVED_IR_PAGE_FOUND, SEC_8K_FOUND_EX99_NOT_FOUND, NO_YAHOO_PRESS_RELEASE, COMPANY_IR_PAGE_NOT_APPROVED, COMPANY_IR_NOT_FOUND; verify adds CONFIRMED, STALE, CONFLICTING, etc.
- **Edge cases:** The schema is dropped on older FastMCP SDKs (yfmcp/app.py:61-95), so whether it is enforced depends on the SDK version; 'OK' is never emitted by the news collection status, although the enum suggests it is the success value
- **Suspected defect:** Schema contract and runtime behaviour disagree. Strict MCP clients that validate structured output could reject valid responses, and Python and Worker publish different schemas.
- **SME question:** Is the status enum meant to list every lifecycle value, or should the field be unconstrained? Should a clean success emit 'OK' rather than omitting status?

### BR-NWS-215 · Default SEC event filing types use 'DEF14A', which never matches EDGAR's 'DEF 14A' (P1, Validation, confidence Medium)

- **Source:** `server.py:2564,673-683,1353-1358; worker/src/yahoo-finance.ts:11724,9650-9657,11101-11105`
- **Rule:** Event collection keeps a filing only if its form type exactly matches a requested type. The default list includes 'DEF14A', but EDGAR spells proxy statements 'DEF 14A', so proxy filings are never picked up by default. Likewise, '424B' never matches real forms such as 424B2 or 424B5 when classifying event type.
- **Given** A company filed a DEF 14A proxy statement 10 days ago and a 424B5 prospectus 5 days ago
- **When** verify_company_event or get_public_event_timeline collects SEC events with the default filing types
- **Then** The DEF 14A is skipped because 'DEF 14A' is not 'DEF14A', and the 424B5 is not collected because it is not in the defaults. If requested explicitly, a 424B5 gets eventType 'other' rather than 'financing'.
- **Parameters:** Default types: 8-K, 10-Q, 10-K, S-3, DEF14A. Form-to-event map: 10-Q/10-K→earnings; S-3/S-1/424B→financing; 8-K/DEF14A/PRE14A→regulatory; 4→insider
- **Edge cases:** Other modules in the same code use the EDGAR spelling 'DEF 14A' (server.py:4017,9016; worker/src/yahoo-finance.ts:12791); Amendments such as 8-K/A are excluded unless requested explicitly; Python's fallback to keyword classification (server.py:1320) never runs, because 'other' is truthy
- **Suspected defect:** Because of the 'DEF14A' spelling, proxy-statement events are silently missing from both runtimes.
- **SME question:** Should proxy statements (EDGAR 'DEF 14A') and 424B-family prospectuses be included in event and timeline collection? If so, should matching normalize spaces and prefixes?

### BR-NWS-216 · Default SEC form set for news collection, with exact form-string matching (P1, Validation, confidence Medium)

- **Source:** `server.py:2562-2566; server.py:1353-1358; server.py:673-683; worker/src/yahoo-finance.ts:11724; worker/src/yahoo-finance.ts:11101-11105; worker/src/yahoo-finance.ts:9650-9657`
- **Rule:** When the sec source runs inside get_company_news, search_company_news or verify_company_event and no filing types are given, only filings whose form string is exactly 8-K, 10-Q, 10-K, S-3 or DEF14A count. Each one is then given an event type from a fixed form-to-type table.
- **Given** Ticker XYZ whose recent EDGAR filings are '8-K', '8-K/A', '10-Q', 'DEF 14A' and '424B5', with sources including 'sec' and no sec_filing_types
- **When** Company events are collected
- **Then** Only the '8-K' (event type regulatory) and the '10-Q' (event type earnings) become items. '8-K/A', 'DEF 14A' and '424B5' are dropped because matching is exact, case-insensitive equality on the whole form string.
- **Parameters:** Default news filing types = [8-K, 10-Q, 10-K, S-3, DEF14A]. The get_sec_recent_events default is a different list, [8-K, 10-Q, 10-K]. Form-to-event-type table: 10-Q and 10-K -> earnings; S-3, S-1 and 424B -> financing; 8-K, DEF14A and PRE14A -> regulatory; 4 -> insider; anything else -> other.
- **Edge cases:** An empty desired set means every form is accepted (server.py:1357, worker 11105); Amended forms (e.g. 10-K/A) never match the default list and map to 'other'; Form '4' is not in the default news list, so Form 4 insider items only appear when filing types are given explicitly
- **Suspected defect:** EDGAR spells the proxy form 'DEF 14A' (with a space), and prospectus forms carry suffixes such as 424B2 or 424B5. If that holds, the default DEF14A filter and the 424B/PRE14A event-type mappings can never match real filings, so proxy filings are silently missing from news.
- **SME question:** EDGAR submissions data writes proxy and prospectus forms as 'DEF 14A', 'PRE 14A' and '424B2'/'424B5'. Should 'DEF14A', 'PRE14A' and '424B' be compared after removing spaces or by prefix, and should amended (/A) forms count?

### BR-NWS-217 · Exchange-prefix compatibility for ticker mentions (P1, Validation, confidence High)

- **Source:** `server.py:1340-1389; worker/src/yahoo-finance.ts:11088-11135`
- **Rule:** A ticker mention preceded by an exchange prefix (e.g. 'TSX: AMD') counts only if that exchange matches the company's own listing; unprefixed mentions always count.
- **Given** Company on NASDAQ (NMS), headline 'Acme (TSXV: ACME) announces...' only
- **When** Ticker-token match is evaluated
- **Then** No match (prefix normalises to TSXV, not NASDAQ); 'NASDAQ: ACME' or plain 'ACME' would match
- **Parameters:** Exchange normalisation: NASDAQ/NMS/NGM/NCM/GS -> NASDAQ; NYSE or NYQ -> NYSE; AMEX/NYSE AMERICAN/ASE -> NYSEAMERICAN; TSXV/CVE -> TSXV; TSX/TOR -> TSX; prefix lookback 20 chars; unknown known-exchange prefixes treated incompatible: TSXV, TSX, NYSE, NASDAQ, AMEX, CVE, ASX, LSE, TSX-V
- **Edge cases:** If company exchange cannot be normalised, any token match counts
- **Suspected defect:** 'NYSE AMERICAN' contains 'NYSE' and is tested first, so it normalises to NYSE and the NYSEAMERICAN branch is reachable only via AMEX/ASE; 'GS' substring makes any exchange string containing GS (e.g. 'NGS', 'LSE GS') read as NASDAQ.
- **SME question:** Should NYSE American listings be distinguished from NYSE, and is the 'GS' substring test intended?

### BR-NWS-218 · Exchange-prefixed ticker mention compatibility (P1, Validation, confidence High)

- **Source:** `server.py:1357-1389`
- **Rule:** A ticker mention in news text counts unless every mention is prefixed by a different known exchange. For example 'TSX:ABC' does not count for a NASDAQ-listed ABC.
- **Given** Company exchange NMS (NASDAQ), text 'Shares of ABC (TSX: ABC) rose'
- **When** Yahoo items are matched
- **Then** There is no ticker-token match. Text 'Nasdaq: ABC' or a bare 'ABC' matches
- **Parameters:** Lookback window 20 characters before the mention; incompatible-prefix list TSXV,TSX,NYSE,NASDAQ,AMEX,CVE,ASX,LSE,TSX-V
- **Edge cases:** If the company exchange is unknown, any word-boundary match counts; An unknown prefix such as 'OTC:' counts as a match

### BR-NWS-219 · Finnhub item acceptance (P1, Validation, confidence High)

- **Source:** `server.py:1686-1833; worker/src/yahoo-finance.ts:11398-11470`
- **Rule:** Finnhub news is requested for the last N lookback days; rows without a headline or outside the date window are rejected; accepted items count toward acceptedCount even beyond the max-results cap, which is reported as truncation.
- **Given** Finnhub returns 40 in-window items, collection cap 30
- **When** Finnhub items are collected
- **Then** 30 items returned, acceptedCount 40, sourceStatus.finnhub.truncatedCount 10, hasMoreAccepted true, coverage recommends RETRY_TRUNCATED_SOURCE if nothing else failed
- **Parameters:** Default lookback 14 days; HTTP timeout 20s (Py); 401/403 -> AUTH_ERROR, 429 -> RATE_LIMITED, non-array -> PROVIDER_CHANGED; API key <credential — masked, env FINNHUB_API_KEY/FINNHUB_TOKEN>
- **Edge cases:** Missing key -> UNCONFIGURED, attempted false

### BR-NWS-220 · Finnhub row acceptance and date window (P1, Validation, confidence Medium)

- **Source:** `server.py:1722-1731,1783-1833 (row acceptance and date window) + server.py:2291-2307 (truncatedCount/hasMoreAccepted) + server.py:324-346 (RETRY_TRUNCATED_SOURCE)`
- **Rule:** Finnhub is queried from (today minus lookback, default 14 days) to today. Rows are rejected if they are malformed, have no headline, or fall outside the date window. Accepted rows are kept up to max_results, and any further accepted rows are counted as truncated.
- **Given** lookback 14 days, max_results 10, Finnhub returns 25 valid in-window rows
- **When** collection runs
- **Then** 10 items are returned, acceptedCount=25, truncatedCount=15, hasMoreAccepted=true, and coverage.recommendedNextAction=RETRY_TRUNCATED_SOURCE if nothing else failed
- **Parameters:** Default lookback 14 days; rejection codes INVALID_ROW, MISSING_HEADLINE, DATE_OUTSIDE_WINDOW; confidence MEDIUM with URL, LOW without
- **Edge cases:** tickerRelevance is HIGH only if the ticker appears as a substring of headline+summary, otherwise LOW. LOW items are not filtered out, only ranked lower; The Finnhub request ignores start_date/end_date (it uses lookback only), so an explicit older start_date gets no Finnhub rows
- **Suspected defect:** An explicit start_date earlier than the lookback window cannot be satisfied by Finnhub because from= is computed from lookback only.
- **SME question:** Should Finnhub's from/to honor caller start_date/end_date when supplied?

### BR-NWS-221 · Form 4 date normalization (P1, Validation, confidence High)

- **Source:** `server.py:749-759; worker/src/yahoo-finance.ts:9709-9716`
- **Rule:** Transaction dates are returned as YYYY-MM-DD. ISO dates pass through, US M/D/YYYY dates are converted, and anything else is returned as raw text.
- **Given** Date cells '2024-03-05', '3/5/2024' and 'March 5'
- **When** form4 date normalization runs
- **Then** Results are '2024-03-05', '2024-03-05' and 'March 5' (passed through unchanged); an empty cell gives null
- **Edge cases:** The month and day are not range-checked, so '13/40/2024' becomes '2024-13-40'; Dates are read as US month-first, never day-first

### BR-NWS-222 · GlobeNewswire exact stock-category ticker match (P1, Validation, confidence High)

- **Source:** `server.py:545-551,1590-1683`
- **Rule:** GlobeNewswire RSS items count for a ticker only if one of their stock-category tags has the exact ticker after the colon (e.g. 'Nasdaq:NVDA'). Matched items are HIGH relevance, and HIGH confidence when they have a link or guid.
- **Given** Ticker NVDA. One item is tagged 'Nasdaq:NVDA', another only mentions NVIDIA in its text
- **When** the newswire source is collected
- **Then** The first is accepted with confidence HIGH. The second is ignored
- **Parameters:** 10 RSS feeds; cache TTL_NEWS per feed; max response size 2 MiB; 20s timeout; stock category domain https://www.globenewswire.com/rss/stock
- **Edge cases:** The exchange part of the tag is ignored ('TSX:NVDA' also matches); Duplicates across feeds are suppressed by duplicateGroupId or feed+link+title+date; Collection stops as soon as max_results is reached

### BR-NWS-223 · GlobeNewswire stock-category exchange check (Worker only), with NYSE American misclassified (P1, Validation, confidence Medium)

- **Source:** `worker/src/yahoo-finance.ts:9584-9603,9912-9918,11920-11926; server.py:545-551,2699-2707`
- **Rule:** The Worker accepts a GlobeNewswire release only if a category symbol matches the ticker and the category's exchange prefix matches the company's listing exchange. Python checks only the symbol. Because the Worker tests 'NYSE' before 'NYSE AMERICAN', a company whose exchange name is 'NYSE American' is treated as NYSE.
- **Given** An NYSE American company UUUU (Yahoo exchangeName 'NYSE American') and a GlobeNewswire item with stock category 'NYSE American:UUUU'
- **When** newswire relevance is checked
- **Then** Worker: expected exchange becomes NYSE and the prefix is 'NYSE AMERICAN', which is not NYSE, so the item is rejected. Python: the symbol UUUU matches, so the item is accepted. A category 'NASDAQ:UUUU' for an NYSE-listed company is rejected by the Worker and accepted by Python.
- **Parameters:** Exchange codes: NASDAQ (NASDAQ/NMS/NGM/NCM), NYSE (contains NYSE or equals NYQ), NYSEAMERICAN (AMEX/NYSE AMERICAN/ASE). An unknown exchange or a category with no prefix passes.
- **Edge cases:** The Worker passes identity.exchange from exchangeName, falling back to fullExchangeName and then exchange (worker/src/yahoo-finance.ts:9949); An exchange name such as 'ASE' maps correctly to NYSEAMERICAN; Python's _normalize_exchange (server.py:1386-1391) has the same NYSE-before-NYSE-AMERICAN ordering
- **Suspected defect:** The NYSE American branch cannot be reached for names containing 'NYSE', so NYSE American issuers lose their own GlobeNewswire releases in the Worker. Python does not check the exchange at all.
- **SME question:** Should newswire acceptance require the category's exchange prefix to match the listing exchange in both runtimes? And should 'NYSE American' be recognised before the generic 'NYSE' test?

### BR-NWS-224 · GlobeNewswire stock-category match requires a compatible exchange prefix (Worker only) (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:9584-9603; worker/src/yahoo-finance.ts:9912-9918; worker/src/yahoo-finance.ts:11920-11922; server.py:545-551; server.py:1632-1638`
- **Rule:** The Worker accepts a GlobeNewswire item only when the symbol after the colon equals the ticker and the exchange prefix fits the company's listing exchange. Python checks only the symbol.
- **Given** A company listed on NASDAQ (ticker ABC) and a GlobeNewswire item tagged 'NYSE:ABC'
- **When** newswire is collected
- **Then** Worker: rejected (prefix NYSE is not NASDAQ). Python: accepted with tickerRelevance HIGH.
- **Parameters:** Exchange normalization: anything containing NASDAQ, NMS, NGM or NCM -> NASDAQ; containing NYSE or equal to NYQ -> NYSE; containing AMEX or equal to ASE -> NYSEAMERICAN. Accepted prefixes for NYSEAMERICAN: NYSEAMERICAN, 'NYSE AMERICAN', AMEX.
- **Edge cases:** An unknown company exchange, or a category with no prefix, is accepted; 'NYSE AMERICAN' normalizes to NYSE because the NYSE check runs first, so the NYSEAMERICAN branch is reached only for AMEX/ASE; For TSX and other non-US exchanges the company normalizes to null, so any prefix is accepted
- **Suspected defect:** Python/TS output divergence: Python can return items that the Worker filters out. Choosing the behaviour is a planner decision under CLAUDE.md.

### BR-NWS-225 · GlobeNewswire ticker relevance by stock category (P1, Validation, confidence Medium)

- **Source:** `server.py:493-496,545-551,1590-1596,1663-1664; worker/src/yahoo-finance.ts:479-483,9570-9589,9898-9904,11313,11361-11362`
- **Rule:** A GlobeNewswire RSS item is attributed to a ticker only if its stock-category metadata (e.g. 'Nasdaq:NVDA') has the exact symbol; matched items are HIGH relevance and HIGH confidence when they carry a link.
- **Given** Ticker NVDA on NASDAQ, RSS item with stock category 'NYSE:NVDA'
- **When** GlobeNewswire feeds are parsed
- **Then** Python accepts it (symbol-only match); TS rejects it because the NYSE prefix is incompatible with the company's NASDAQ listing
- **Parameters:** 10 RSS feeds; max 2 MiB per feed; DOCTYPE/ENTITY declarations rejected; cache 15 min (TS max 40 entries); stock category domain https://www.globenewswire.com/rss/stock
- **Edge cases:** Feed collection stops as soon as max results reached; Items deduped within feeds by duplicateGroupId or feed:link:title:date
- **Suspected defect:** Parity break: TS applies exchange-compatibility to the stock-category prefix; Python _globenewswire_stock_category_matches ignores the exchange, so cross-listed or same-symbol foreign issuers match in Python only.
- **SME question:** Citation was corrected by referee (The rule is real, and the Python citations are right. The TS line ranges for the matching logic point at the wrong code.

Python:
- server.py:545-551 (_globenewswire_stock_category_matches) takes the part after the last ':' in each category and compares it to the upper-cased ticker. It never looks at the exchange, so 'NYSE:NVDA' matches NVDA.
- server.py:1590-1596 reads the categories from the stock domain (constant at line 495) and skips items that don't match. Matched items get relevance "HIGH".
- server.py:1663-1664 sets confidence to HIGH when there is a link or guid, otherwise MEDIUM.
- Lines 493-496 hold the 2 MiB size limit, the blocked DOCTYPE/ENTITY markers and the domain constants.

TS:
- 479-483 is correct: 2 MiB limit, 15-minute TTL, stock domain, cache max 40.
- 9877-9883 is the generic XML tag-text extractor, not the matching logic.
- 9559-9568 is normalizeLinkedUrl and looksLikeFeed, not the exchange check.
- The exchange-aware rejection is in exchangeCompatibleWithStockCategory (9580-9589) and normalizedExchangeCode (9570-9578). The exchange check is combined with the symbol match in globenewswireStockCategoryMatches (9898-9904), which is called at 11313.
- With expected exchange NASDAQ, a 'NYSE' prefix fails, so TS rejects the item. If the exchange is unknown, or the category has no prefix, the item is accepted.
- TS sets tickerRelevance to "HIGH" and confidence to HIGH when there is a link or guid (around 11361-11362).

The

### BR-NWS-226 · Issuer names used for valuation attribution differ by runtime (P1, Validation, confidence High)

- **Source:** `server.py:10878-10904,10960-10966; worker/src/yahoo-finance.ts:15851-15871`
- **Rule:** Python checks attribution against one name (Yahoo longName, else shortName). The Worker checks against longName, shortName and companyName together, so the Worker accepts more targets as belonging to the subject.
- **Given** longName 'Rocket Lab USA, Inc.', shortName 'Rocket Lab'
- **When** A headline says 'Rocket Lab target raised to $30 by Cantor'
- **Then** Worker aliases include 'rocket lab', so the target is accepted (CLAUSE). Python aliases are only 'rocket lab usa inc', 'rocket lab usa' and 'rkl b' base 'rklb', so the target is rejected for another company.
- **Parameters:** days_back default 30, clamp 1-365; news fetched with max_results=100; warnings ATTRIBUTION_UNCHECKED, NEWS_UNAVAILABLE, RATING_CHANGES_UNAVAILABLE
- **Edge cases:** The Worker also raises NEWS_UNAVAILABLE when news.code == INPUT_VALIDATION_ERROR. Python raises it when the news call threw or returned an error.
- **Suspected defect:** Python/TS output divergence in attribution results (subjectMatch, rejectedForOtherCompany) for the same input. This needs a planner decision.

### BR-NWS-227 · Issuer-own press release qualification (P1, Validation, confidence Medium)

- **Source:** `server.py:403-411,1488-1492,1514-1516; worker/src/yahoo-finance.ts:9522-9528,11211-11217,11240-11241`
- **Rule:** In the press-releases feed an item counts as the company's own press release only if its publisher is an issuer wire service and its headline starts with the company's name or ticker.
- **Given** Ticker AEHR; item from publisher 'ACCESS Newswire' titled 'Aehr Test Systems Receives Sonoma Order' vs item from 'GlobeNewswire' titled 'QuickLogic partners with Aehr'
- **When** Press-release feed is filtered
- **Then** First is kept with issuer = company name; second rejected as NOT_ISSUER_WIRE_RELEASE
- **Parameters:** Wire regex (anchored at start, case-insensitive): access newswire, accesswire, globe newswire, business wire, pr newswire, newsfile, prweb, eqs (newswire), cision; method label ISSUER_WIRE_RELEASES_IN_NEWS_FEED
- **Edge cases:** MT Newswires deliberately not a wire service; Both feeds now read the same Yahoo search feed since the press-release tab endpoint was retired
- **SME question:** Citation was corrected by referee (The rule is real and runs in both runtimes, but half of the TS citation points at the wrong code. Python is cited correctly. server.py:403-411 (_yahoo_item_issuer) sets the issuer to companyName only when the item source is yahoo_finance_press_releases and the normalized title equals, or starts with "<lead> ", an identity alias or one of the item's own tickers. server.py:1488-1492 rejects press_releases-feed items with NOT_ISSUER_WIRE_RELEASE unless _PRESS_RELEASE_WIRE_RE.match(originalSource) matches and an issuer was set. server.py:1514-1516 defines the wire regex (re.match anchors it at the start, case-insensitive, ASCII) and the method label ISSUER_WIRE_RELEASES_IN_NEWS_FEED. In TS, the reject gate is at 11214-11217, near the cited 11218-11220. But the cited 9501-9507 is the alias/acronym builder, not the issuer function. yahooItemIssuer is at worker/src/yahoo-finance.ts:9522-9528. The TS regex (/^.../i, explicitly anchored) and method constant are at 11240-11241, which the citation leaves out. Minor nuance: the ticker leads come from the item's own tickers list, not directly from the queried ticker. Nothing in the cited code reads as instructions aimed at an AI.) — confirm server.py:403-411,1488-1492,1514-1516; worker/src/yahoo-finance.ts:9522-9528,11211-11217,11240-11241 is the authoritative implementation.

### BR-NWS-228 · Linked external company IR feed must match company identity (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:10464-10514, 10566-10601, 9357-9365`
- **Rule:** A feed on the company's own domain is trusted outright. A feed hosted elsewhere is accepted only if its title or first 2,000 characters mention the ticker or a company name token. Each item from an external feed must also match the identity on its own.
- **Given** A feed at feeds.example-wire.com linked from the IR page of ACME Corp, with feed title 'Business Wire Top Stories'
- **When** collectCompanyIrRssEvents parses the feed
- **Then** If neither the ticker nor a name token appears, the feed is skipped with COMPANY_IR_RSS_HOST_REJECTED. Accepted items get confidence HIGH (same-domain item with a URL), MEDIUM (URL but external host) or LOW (no URL). tickerRelevance is HIGH when the item is same-domain or matches the identity.
- **Parameters:** identity probe window 2000 chars; items capped at maxResults
- **Edge cases:** Items outside the date window are dropped after acceptance; Feeds containing <!ENTITY are rejected as a parse error (10440); rssDiscovery diagnostic is feed_not_accepted when feeds were found but none accepted

### BR-NWS-229 · Marketaux item acceptance filters (P1, Validation, confidence High)

- **Source:** `server.py:1954-2014`
- **Rule:** A Marketaux article is accepted only if it has a headline, an https URL on an allowed wire domain, an entity whose symbol equals the ticker on a compatible exchange, and a publish date inside the window. Accepted articles are always LOW confidence.
- **Given** an article on prnewswire.com tagged with entity symbol AAPL, exchange NASDAQ, for ticker AAPL (Yahoo exchange NMS)
- **When** it is processed
- **Then** accepted with matchBasis PROVIDER_ENTITY_SYMBOL, confidence LOW, tickerRelevance HIGH. The same article with entity exchange NYSE is rejected as ENTITY_EXCHANGE_MISMATCH
- **Parameters:** rejection reasons: INVALID_ROW, MISSING_HEADLINE, INVALID_URL, DOMAIN_NOT_ALLOWED, ENTITY_SYMBOL_MISMATCH, ENTITY_EXCHANGE_MISMATCH, DATE_OUTSIDE_WINDOW
- **Edge cases:** The exchange check is skipped when either side does not normalise to NASDAQ, NYSE, NYSEAMERICAN, TSX or TSXV

### BR-NWS-230 · Marketaux row acceptance rules (P1, Validation, confidence High)

- **Source:** `server.py:1954-2021`
- **Rule:** A Marketaux article is accepted only if it has a title, an https URL on an allowed wire domain (or subdomain), an entity whose symbol equals the ticker, a matching exchange when both sides are known, and a date inside the window. Accepted items are always LOW confidence.
- **Given** Ticker AAPL (Yahoo exchange NMS). The article's entity has symbol AAPL and exchange NYSE
- **When** the Marketaux row is evaluated
- **Then** Rejected with ENTITY_EXCHANGE_MISMATCH. With exchange NASDAQ or blank it is accepted: confidence LOW, matchBasis PROVIDER_ENTITY_SYMBOL, tickerRelevance HIGH
- **Parameters:** Rejection codes INVALID_ROW, MISSING_HEADLINE, INVALID_URL, DOMAIN_NOT_ALLOWED, ENTITY_SYMBOL_MISMATCH, ENTITY_EXCHANGE_MISMATCH, DATE_OUTSIDE_WINDOW
- **Edge cases:** Exchange check is skipped when either exchange does not normalize; Status OK if accepted>0, else EMPTY_RESULT. HTTP 401/403 -> AUTH_ERROR, 402/429 -> RATE_LIMITED, timeout text -> TIMEOUT

### BR-NWS-231 · Press release candidate source-type allow-list (P1, Validation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:12051-12056,12073-12074,12141; server.py:2871-2873,2904-2905,2962`
- **Rule:** Only items whose source type is a release-style type are considered by get_company_press_releases; SEC and approved IR page sources are collected as primary (8-K only) and all other selected sources as supplemental.
- **Given** Selected sources ['sec','yahoo_finance_news','company_ir_page']
- **When** get_company_press_releases collects items
- **Then** sec and company_ir_page run as primary with SEC filing types ['8-K']; yahoo_finance_news items are collected but dropped because 'yahoo_finance_news' is not a release type
- **Parameters:** Release type set (8 values); SEC filing types fixed to ['8-K']; Worker max 1-100 (default 20), lookback 1-3650 (default 90)
- **Edge cases:** Explicit sources=['yahoo_finance_news'] alone returns no items and NO_OFFICIAL_RELEASE_SOURCE; Worker de-duplicates warnings on code|message|feedUrl|canonicalUrl (wider key than the generic code|message rule)

### BR-NWS-232 · Price target and method must belong to the subject company (P1, Validation, confidence Medium)

- **Source:** `worker/src/capital-structure.ts:2955-2984,2992-3028; yfmcp/capital_structure.py:3258-3291,3294-3340`
- **Rule:** When the company name is known, a target or method is kept only if the clause carrying it names the subject (CLAUSE), or if the sentence names no company alias but the item names the subject and no sentence in the item assigns a target to another company (ITEM); otherwise it is rejected and counted as rejectedForOtherCompany.
- **Given** Subject RKLB with name 'Rocket Lab'; headline 'Rocket Lab climbs; AST SpaceMobile rises as Cantor reiterates $60 target'
- **When** attribution is checked
- **Then** The $60 target clause names no Rocket Lab alias while the sentence does, so the item is conflicted and the target is rejected (rejectedForOtherCompany +1)
- **Parameters:** Aliases: ticker base (before '.') only if >= 3 chars; full normalized name and name with trailing legal suffixes removed (inc, incorporated, corp, corporation, ltd, limited, llc, plc, co, company, sa, ag, nv, se, gmbh, holdings, group), each >= 3 chars; clauses split at '; ' and ', '
- **Edge cases:** Tickers of 1-2 letters (F, GM) never act as aliases; A non-legal trailing word such as 'USA' in 'Rocket Lab USA, Inc.' is kept, so the shorter 'rocket lab' is not an alias and headlines using the short name are rejected; Matching is whole-word on lowercased alphanumeric text
- **SME question:** Should common trailing words such as 'USA', 'Technologies' or 'International' also be stripped when building subject aliases, so headlines using the short brand name are attributed to the subject?

### BR-NWS-233 · Provider HTTP/message status classification (P1, Validation, confidence High)

- **Source:** `yfmcp/clients/market_providers.py:88-109; worker/src/yahoo-finance.ts:207-221`
- **Rule:** Provider responses are classified as follows. HTTP 429 means RATE_LIMIT, 401 means AUTH_ERROR, 404 means NOT_FOUND, and 403 means ENTITLEMENT_REQUIRED only if the body reads like a plan or subscription refusal, otherwise AUTH_ERROR. Any other error is PROVIDER_ERROR.
- **Given** HTTP 403 with body 'You don't have access to this resource'
- **When** Status is classified
- **Then** ENTITLEMENT_REQUIRED
- **Parameters:** Rate regex: rate limit|frequency|call volume|standard api rate|too many requests. Entitlement regex: premium|subscription|subscribe|entitlement|current plan (Worker) vs 'not available under your current plan' (Python)|access to this resource
- **Edge cases:** A body containing only 'current plan' is ENTITLEMENT_REQUIRED in the Worker but falls to AUTH_ERROR/PROVIDER_ERROR in Python.
- **Suspected defect:** The entitlement regex differs between runtimes ('current plan' vs 'not available under your current plan').

### BR-NWS-234 · Query / event_query is required (P1, Validation, confidence High)

- **Source:** `server.py:2746-2747`
- **Rule:** search_company_news returns an input error if the search query is empty or only whitespace. verify_company_event does the same for event_query (server.py:3181-3182).
- **Given** query='   '
- **When** search_company_news is called
- **Then** INPUT_VALIDATION_ERROR 'query is required'
- **Parameters:** none

### BR-NWS-235 · SEC archive URL safety and XBRL document detection (P1, Validation, confidence High)

- **Source:** `server.py:935-943`
- **Rule:** Only SEC EDGAR Archive URLs count as safe filing links. A link counts as an XBRL document if it ends in .xml (ignoring the query string) or contains /xbrl/.
- **Given** URLs 'http://www.sec.gov/Archives/x.htm', 'https://www.sec.gov/Archives/edgar/data/1/a.XML?x=1' and 'https://www.sec.gov/Archives/xbrl/a.htm'
- **When** The URL checks run
- **Then** The first is rejected (not https); the second is an XBRL document (.xml, case-insensitive); the third is an XBRL document (/xbrl/ path)
- **Parameters:** Prefix https://www.sec.gov/Archives/
- **Edge cases:** Leading and trailing whitespace is trimmed before the check

### BR-NWS-236 · SEC filing-type filter is an exact form match, so the default 'DEF14A' never matches EDGAR proxy filings (P1, Validation, confidence Medium)

- **Source:** `server.py:1353-1358; server.py:2564; server.py:673-683; worker/src/yahoo-finance.ts:11101-11105; worker/src/yahoo-finance.ts:11724; worker/src/yahoo-finance.ts:9654`
- **Rule:** News and event collection keeps an SEC filing only if its form code, uppercased, exactly equals one of the requested filing types. If the requested list is empty, every form is kept. The default list for news is 8-K, 10-Q, 10-K, S-3 and DEF14A.
- **Given** get_company_news is called with sources including 'sec' and no explicit SEC filing types, so the defaults [8-K, 10-Q, 10-K, S-3, DEF14A] apply, and EDGAR submissions list the forms 'DEF 14A', '8-K/A', '8-K' and '4'
- **When** SEC events are collected
- **Then** Only the '8-K' filing is kept. 'DEF 14A' is dropped because the default spells it 'DEF14A' with no space. '8-K/A' is dropped because amendments never match exactly. '4' is dropped because it is not in the default list. The form-to-event-type map also uses 'DEF14A' and 'PRE14A', so a proxy filing would map to 'other' rather than 'regulatory' even if it got through.
- **Parameters:** News default SEC types = [8-K, 10-Q, 10-K, S-3, DEF14A]; press-release SEC types = [8-K]; get_sec_recent_events default = [8-K, 10-Q, 10-K]; an empty set means all forms
- **Edge cases:** Amended filings (10-K/A, 8-K/A) are never collected unless the caller names the /A form exactly; An empty filing-type set returns every form type (both runtimes); Other parts of the system spell the proxy form 'DEF 14A' with a space (server.py:4017, server.py:9016, worker/src/yahoo-finance.ts:12791), so the spelling is inconsistent within the codebase
- **Suspected defect:** 'DEF14A' does not match the form string EDGAR returns ('DEF 14A'), so proxy statements are silently left out of default news and event timelines in both runtimes.
- **SME question:** Should proxy statements (DEF 14A / PRE 14A) and amended filings (/A) appear in default news and event collection? If so, should form matching ignore spaces and the /A suffix?

### BR-NWS-237 · SEC recent events requires at least one filing type (P1, Validation, confidence High)

- **Source:** `server.py:3047-3049`
- **Rule:** get_sec_recent_events defaults to 8-K, 10-Q and 10-K. If the caller's list has only blank entries, the request is rejected.
- **Given** filing_types=['  ']
- **When** get_sec_recent_events is called
- **Then** INPUT_VALIDATION_ERROR 'filing_types must not be empty'. filing_types=None uses ['8-K','10-Q','10-K']
- **Parameters:** Default filing types 8-K, 10-Q, 10-K; default lookback 90 days; default max 20
- **Edge cases:** An empty list [] is falsy, so it falls back to the defaults instead of raising the error

### BR-NWS-238 · Source list normalization and unsupported-source warning (P1, Validation, confidence High)

- **Source:** `server.py:230-236,568-588`
- **Rule:** Requested sources are lower-cased and de-duplicated. Unknown sources are dropped with a SOURCE_UNSUPPORTED warning, and if nothing valid remains the tool's default sources are used.
- **Given** sources=['Finnhub','bloomberg','finnhub']
- **When** _normalize_event_sources runs
- **Then** selected=['finnhub'] plus one warning SOURCE_UNSUPPORTED for 'bloomberg'. If sources=['bloomberg'], the defaults are used and the warning is still emitted
- **Parameters:** Supported: sec, company_ir, company_ir_page, newswire, yahoo_finance, yahoo_finance_news, yahoo_finance_press_releases, finnhub, marketaux

### BR-NWS-239 · Target and method attribution to the subject company (CLAUSE / ITEM / rejected) (P1, Validation, confidence High)

- **Source:** `yfmcp/capital_structure.py:3289-3291,3320-3340; worker/src/capital-structure.ts:2982-2984,3010-3028`
- **Rule:** When the company name is known, a target or method is kept only if the clause carrying it names the company (CLAUSE). It is also kept if the sentence names no company alias, the item names the company elsewhere, and no sentence in the item gives another company's target (ITEM). Anything else is rejected and counted.
- **Given** An item 'Rocket Lab climbs as Cantor reiterates $122 target; AST SpaceMobile rises' with subject ASTS
- **When** evidence is attributed
- **Then** The sentence names the subject but its target clause ('Cantor reiterates $122 target') does not, so the item is marked conflicted. The target is rejected and rejectedForOtherCompany increases by 1.
- **Parameters:** Clause delimiters: ';' plus optional whitespace, ',' plus whitespace
- **Edge cases:** A methods-only sentence (no target) is tested against the whole sentence, not a clause; A sentence that names the subject outside the target clause is rejected even when the item is not conflicted

### BR-NWS-240 · Ticker format validation for news and event tools exists only in Python (P1, Validation, confidence High)

- **Source:** `server.py:2790-2792,3091-3093,3150,3227,8333-8335; yfmcp/validation.py:12-21; worker/src/validate.ts:1-9; worker/src/tools.ts:2029-2032,2043-2049,2519-2564`
- **Rule:** Python rejects tickers that are not 1–20 characters of A–Z, 0–9, '.', '-', '^' or '='. It does this in get_company_news, search_company_news, get_company_press_releases, get_sec_recent_events, get_public_event_timeline and verify_company_event. The Worker only applies the same check to get_historical_prices.
- **Given** A caller sends ticker='AAPL US!' to search_company_news with query='dividend'
- **When** the tool is invoked
- **Then** Python returns INPUT_VALIDATION_ERROR 'Invalid ticker symbol ...'. The Worker accepts the ticker and runs collection, returning an empty or limited result.
- **Parameters:** Ticker regex ^[A-Z0-9.\-\^=]{1,20}$ (applied after trim and uppercase)
- **Edge cases:** The Worker's get_company_news only rejects blank symbols (worker/src/yahoo-finance.ts:11960-11969); In Python batch mode each symbol is validated separately inside the per-ticker call
- **Suspected defect:** This breaks Python/Worker output parity: the same malformed input gives an error in one runtime and a result in the other.

### BR-NWS-241 · Ticker symbol format validation (P1, Validation, confidence High)

- **Source:** `yfmcp/validation.py:12-21`
- **Rule:** A news/event request is rejected unless the ticker, once trimmed and upper-cased, is 1-20 characters drawn only from letters, digits and . - ^ =.
- **Given** A caller supplies ticker 'brk.b'
- **When** get_company_news / search_company_news / verify_company_event / get_company_press_releases / get_public_event_timeline / get_sec_recent_events is called
- **Then** The ticker passes (BRK.B matches ^[A-Z0-9.\-^=]{1,20}$). 'AAPL$' or a 21-character string returns INPUT_VALIDATION_ERROR and no fetch happens.
- **Parameters:** Regex ^[A-Z0-9.\-\^=]{1,20}$; trim and upper-case happen before the check
- **Edge cases:** Lower-case input passes validation, but the raw value (not upper-cased) is what gets passed on to the collectors; Empty string fails

### BR-NWS-242 · Verify low-relevance downgrade (P1, Validation, confidence Medium)

- **Source:** `server.py:3312-3379`
- **Rule:** If all top-5 evidence items lack a reference to the ticker or company, a CONFIRMED result is downgraded to PARTIAL with a LOW_RELEVANCE_EVIDENCE warning.
- **Given** status CONFIRMED and top-5 evidence all scored LOW
- **When** the relevance check runs
- **Then** status=PARTIAL with warning LOW_RELEVANCE_EVIDENCE
- **Parameters:** Top 5 evidence; SEC name suffix stripping list at server.py:3298-3302
- **Suspected defect:** Every collected item has tickers=[ticker] set by its collector, so _relevance_score always returns HIGH at step 1 and the downgrade can never fire. The guard is effectively dead code.
- **SME question:** Should relevance be computed from the item's text and issuer instead of the collector-assigned tickers list?

### BR-NWS-243 · Yahoo news issuer matching and rejection reasons (P1, Validation, confidence High)

- **Source:** `server.py:1458-1506; server.py:414-424; server.py:374-400`
- **Rule:** A Yahoo item is kept only if its content type is allowed, it falls in the date window, and it names the company. Matches rank as ticker token (0), issuer name or alias (1), then acronym (2). Kept items are sorted by match rank, then newest first.
- **Given** ticker AEHR, identity RESOLVED with alias 'aehr test systems', and an item headlined 'Aehr Test Systems receives order' with no 'AEHR' token
- **When** the news feed is filtered
- **Then** accepted with matchBasis ISSUER_NAME, tickerRelevance HIGH, confidence MEDIUM if a URL exists (else LOW)
- **Parameters:** allowed contentTypes STORY, ARTICLE, PRESS_RELEASE; legal suffixes stripped (inc, corp, ltd, plc, ag, nv, se, gmbh ...); acronym length 3-6 ignoring the/and/of/for; alias min length 3; Yahoo search news_count 20
- **Edge cases:** When identity is UNAVAILABLE only ticker-token matches pass, and rejections are counted as IDENTITY_UNAVAILABLE_TICKER_NOT_FOUND; A ticker preceded by an incompatible exchange prefix (e.g. 'TSX:' for a NASDAQ company) does not match

### BR-NWS-244 · Yahoo news item acceptance and match ranking (P1, Validation, confidence High)

- **Source:** `server.py:414-424,1458-1506`
- **Rule:** A Yahoo item must be a STORY, ARTICLE or PRESS_RELEASE (or have no type), fall inside the date window, and mention the company by ticker token, issuer name or acronym. Ticker matches rank first, then name, then acronym, and newest first within each rank.
- **Given** A VIDEO item, an ARTICLE from 200 days ago with lookback 14, and an ARTICLE mentioning only 'AMD' for Advanced Micro Devices
- **When** _collect_yahoo_events runs
- **Then** The first is rejected with CONTENT_TYPE and the second with DATE_WINDOW. The third is accepted with matchBasis ISSUER_ACRONYM (rank 2), tickerRelevance HIGH, confidence MEDIUM if it has a URL else LOW
- **Parameters:** Allowed content types STORY, ARTICLE, PRESS_RELEASE; Yahoo search news_count=20; rejection codes CONTENT_TYPE, DATE_WINDOW, IDENTITY_MISMATCH, IDENTITY_UNAVAILABLE_TICKER_NOT_FOUND, NOT_ISSUER_WIRE_RELEASE
- **Edge cases:** Items with no publish timestamp fail the date window and are rejected, even though a PUBLISHED_AT_UNAVAILABLE warning is built

### BR-NWS-245 · Yahoo news relevance gate and match ranking (P1, Validation, confidence Medium)

- **Source:** `server.py:414-424,1475-1505,1511; worker/src/yahoo-finance.ts:9530-9539,11206-11236,1729`
- **Rule:** A Yahoo news item is kept only if it names the company: exact ticker token (rank 0), company name alias (rank 1) or acronym (rank 2); kept items are sorted by match rank then newest first and capped at max results.
- **Given** Ticker AMD, identity aliases 'advanced micro devices', headlines 'AMD beats estimates' (Aug 2) and 'Advanced Micro Devices ships MI400' (Aug 3)
- **When** Yahoo items are filtered
- **Then** Both are kept; 'AMD beats estimates' (TICKER_TOKEN, rank 0) is listed before the newer alias match; each gets tickerRelevance HIGH, confidence MEDIUM if a URL exists else LOW; non-matches are rejected as IDENTITY_MISMATCH or IDENTITY_UNAVAILABLE_TICKER_NOT_FOUND
- **Parameters:** Yahoo search feed newsCount = 20; ranks TICKER_TOKEN 0, ISSUER_NAME 1, ISSUER_ACRONYM 2
- **Edge cases:** Items outside the date window are rejected as DATE_WINDOW before identity check; When identity unavailable only ticker-token matches survive and a SOURCE_IDENTITY_UNAVAILABLE warning is added
- **SME question:** Citation was corrected by referee (I checked the behaviour in both runtimes and it matches the rule. The Python line ranges are correct, but the Worker (TypeScript) ranges point at the wrong lines, so the citation needs fixing.

Python: server.py:414-424 (_yahoo_news_match_for) returns TICKER_TOKEN/0 when the text contains a ticker token that fits the exchange context. If identity status is not RESOLVED it returns None. Otherwise it returns ISSUER_NAME/1 for a whole-word alias match on normalised text, then ISSUER_ACRONYM/2 for a word-boundary acronym match. server.py:1475-1482 rejects a non-match as IDENTITY_UNAVAILABLE_TICKER_NOT_FOUND when identity status is UNAVAILABLE, and as IDENTITY_MISMATCH otherwise. Lines 1495-1496 set tickerRelevance=HIGH, and confidence to MEDIUM if a URL exists or LOW if not. Lines 1501-1502 run two stable sorts (newest first, then by rank), and line 1505 keeps the first max_results. newsCount=20 is set at server.py:1511.

Worker: worker/src/yahoo-finance.ts:9509-9518 is only the end of the identity builder (where status becomes RESOLVED or UNAVAILABLE) and the start of the yahooItemIssuer doc comment. The match function itself is yahooNewsMatchFor at 9530-9539, and it mirrors the Python ranks. The cited 11171-11215 covers diagnostics, the rejection branch (11206-11210) and the mentionedCompany/issuer assignment. It stops before tickerRelevance/confidence (11223-11224) and before the sort and cap: line 11233 sorts by rank, then by publishedAt des

### BR-NWS-246 · Yahoo news-feed items are routed to the output by the requested source label (P1, Validation, confidence Medium)

- **Source:** `server.py:1233-1238,2608-2635; worker/src/yahoo-finance.ts:10622-10629,11197,11808-11836`
- **Rule:** An item from the Yahoo news feed is only returned if its source label matches a source the caller asked for. In Python a news-feed item whose contentType is PRESS_RELEASE is relabelled as a press release and then dropped unless legacy yahoo_finance (or company_ir) was requested. The Worker always labels news-feed items yahoo_finance_news, so they are never dropped this way.
- **Given** A caller requests sources=['yahoo_finance_news'] and the Yahoo news feed returns 5 accepted items, 1 of which has contentType PRESS_RELEASE
- **When** collect company events merges the Yahoo news feed results
- **Then** Python returns 4 items (the PRESS_RELEASE item is labelled yahoo_finance_press_releases and discarded), while yahoo_finance_news acceptedCount still reports 5. The Worker returns all 5, labelled yahoo_finance_news, because it passes feedSource='yahoo_finance_news' for the news feed.
- **Parameters:** Routing labels: yahoo_finance_news, yahoo_finance_press_releases; legacy aggregate yahoo_finance; Python-only legacy company_ir passthrough
- **Edge cases:** The Worker's getNews() drops contentType (worker/src/yahoo-finance.ts:1735-1745), so contentType-based relabelling cannot happen there even if the item were not forced to a label; With legacy yahoo_finance selected, both runtimes keep news and press-release labelled items; Python acceptedCount can be greater than returnedCount without any truncation warning
- **Suspected defect:** Python and the Worker can return different item sets for the same request. Python silently drops relabelled items, so its accepted count does not match what it returns.
- **SME question:** Should a news-feed item that Yahoo marks as PRESS_RELEASE be returned when the caller asked only for yahoo_finance_news, or should it be dropped? Python and the Worker currently do different things.

### BR-NWS-247 · Yahoo press release must be the issuer's own wire release (P1, Validation, confidence High)

- **Source:** `server.py:403-411,1488-1492,1514-1516`
- **Rule:** An item counts as a company press release only if its publisher is a recognized press-release wire and its headline begins with the company's name or ticker. Another company's release that merely mentions it is rejected.
- **Given** Feed press_releases, ticker AAPL. Item 1: publisher 'Business Wire', headline 'Qualcomm renews license with Apple'. Item 2: publisher 'GlobeNewswire', headline 'Apple announces ...'
- **When** the item is evaluated
- **Then** Item 1 is rejected with NOT_ISSUER_WIRE_RELEASE. Item 2 is accepted with issuer=companyName
- **Parameters:** Wire regex: access newswire, accesswire, globe newswire, business wire, pr newswire, newsfile, prweb, eqs (newswire), cision. Method ISSUER_WIRE_RELEASES_IN_NEWS_FEED
- **Edge cases:** MT Newswires and similar news services are deliberately excluded; Headline lead accepts an exact match or the alias followed by a space

### BR-NWS-248 · search_company_news always caps results at the last 14 days, even with an explicit start_date (P1, Validation, confidence High)

- **Source:** `server.py:2798-2807,946-963; worker/src/yahoo-finance.ts:12012-12019,9854-9864`
- **Rule:** search_company_news always applies a 14-day lookback on top of any start_date and end_date the caller gives, so older dates are silently cut off.
- **Given** Today is 2026-10-15 and a caller runs search_company_news(ticker='MSFT', query='acquisition', start_date='2026-07-01', end_date='2026-09-30')
- **When** items are filtered by the date window
- **Then** Only items dated on or after 2026-10-01 pass the lookback, and none pass end_date 2026-09-30. The result is empty even if matching July–September items exist.
- **Parameters:** lookback_days = 14 (hard-coded); collection pool max_results = 100
- **Edge cases:** The date window rejects any item with no publishedAt; verify_company_event and get_public_event_timeline use a fixed 365-day lookback in the same way
- **Suspected defect:** The explicit start_date and end_date inputs are overridden by the hard-coded 14-day lookback, so historical searches return nothing with no warning.

### BR-NWS-249 · Company IR page item confidence and summary length (P2, Calculation, confidence High)

- **Source:** `server.py:1012-1050`
- **Rule:** Every accepted approved IR page item gets HIGH confidence and HIGH ticker relevance. Its event type comes from keywords in the title plus summary. Summaries are cut to 240 characters and evidence text to 180.
- **Given** An approved IR item titled 'Acme announces Q3 earnings'
- **When** The item is built
- **Then** confidence=HIGH, tickerRelevance=HIGH, eventType=earnings, approvalStatus='approved', issuer=the registry issuerName (or the upper-cased ticker if none is set)
- **Parameters:** Summary max 240; evidence max 180; short_text default 220 with whitespace collapsed

### BR-NWS-250 · Daily news-quality audit: Arsenal-20 scope and title overlap ratio (P2, Calculation, confidence High)

- **Source:** `scripts/audit_arsenal_news.py:28-39; scripts/audit_arsenal_news.py:96-129; scripts/audit_arsenal_news.py:165-175; .github/workflows/news-quality-audit.yml:3-23`
- **Rule:** Every day at 07:30 UTC a non-blocking audit queries Yahoo and Finnhub news separately for exactly 20 unique tickers and records, per ticker, how many headlines the two providers share.
- **Given** Yahoo returns titles {A,B,C} and Finnhub returns {b,D} (case-insensitive match 'B'='b')
- **When** The audit compares the providers
- **Then** sharedTitleCount=1, combinedUniqueTitleCount=4, titleOverlapRatio = 1/4 = 0.25 (rounded to 3 decimals). The ratio is null when neither provider has titles.
- **Parameters:** Scope must contain exactly 20 non-empty unique tickers. Defaults: lookback 7 days, max 10 results. Per-call timeout 45 s with 2 retries. Cron '30 7 * * *'. continue-on-error=true.
- **Edge cases:** Titles are compared after trimming and casefolding; A failed call is recorded as CALL_FAILED, and a response whose data is not an object as MALFORMED_RESPONSE, each with 0 items; The per-provider summary counts states by sourceStatus, falling back to callState; lookback and max-results below 1 are rejected

### BR-NWS-251 · Dedupe title stem: HTML entities decoded in Python only (P2, Calculation, confidence High)

- **Source:** `server.py:658-670; worker/src/yahoo-finance.ts:9637-9648`
- **Rule:** Before grouping duplicates, Python decodes HTML entities in the title, so 'AT&amp;T' and 'AT&T' produce the same stem. The Worker does not decode them.
- **Given** Two copies of the same story titled 'Acme &amp; Beta sign deal' and 'Acme & Beta sign deal' on the same day
- **When** Duplicate group keys are computed
- **Then** Python treats them as one group. In the Worker, 'amp' stays in one stem, so they stay separate items.
- **Parameters:** Stem length cap = 80 characters (server.py:228)
- **Edge cases:** Stop words are removed in both: yahoo, finance, finnhub, globenewswire, press, release, inc, corp, corporation, company, plc, ltd, llc

### BR-NWS-252 · Duplicate merge records alternates and flags timestamp variance without affecting status (P2, Calculation, confidence High)

- **Source:** `server.py:2143-2168; worker/src/yahoo-finance.ts:10740-10768`
- **Rule:** When two items share a duplicate group, the higher-priority (or, on a tie, later-published) item is kept. The other is recorded in sourceRefs, and a TIMESTAMP_VARIANCE warning marked statusImpact=false is emitted if their timestamps differ. Final output is newest first.
- **Given** A Yahoo news item at 2026-10-01T13:00Z and a GlobeNewswire item at 2026-10-01T12:58Z in the same duplicate group
- **When** Dedupe runs
- **Then** The newswire item (priority 3) is kept, the Yahoo item (priority 5) is appended to its sourceRefs, and TIMESTAMP_VARIANCE is emitted with statusImpact=false (it is not treated as an evidence conflict)
- **Parameters:** statusImpact=false
- **Edge cases:** Items without a duplicateGroupId pass through unmerged; Equal priority: lexically later publishedAt wins

### BR-NWS-253 · Event type keyword classification (P2, Calculation, confidence High)

- **Source:** `server.py:591-613`
- **Rule:** Each item's event type is the first matching category in a fixed order: earnings, guidance, contract, financing, product, analyst, macro, litigation, insider, regulatory, otherwise 'other'. Matching uses lower-case substrings.
- **Given** Headline 'Acme raises outlook after quarterly beat'
- **When** it is classified
- **Then** eventType=earnings ('quarterly' wins before 'raises'/'outlook')
- **Parameters:** Keyword lists in code (e.g. earnings: earnings, eps, quarterly, 10-q, 10-k, annual report)
- **Edge cases:** Substring matching causes false hits: 'eps' in 'steps', 'deal' in 'dealer', 'note' in 'denote', 'rates' in 'corporates', 'sec' in 'sector'
- **Suspected defect:** Substring (not word-boundary) matching misclassifies items, and eventType drives decisionUse (CHECK_OFFICIAL_RELEASES vs CONTEXT_ONLY).

### BR-NWS-254 · Evidence class, ticker match and URL provenance labels (P2, Calculation, confidence High)

- **Source:** `server.py:269-300; worker/src/yahoo-finance.ts:9181-9209`
- **Rule:** Every item is labeled with an evidence class (SEC_FILING, APPROVED_IR_PAGE, OFFICIAL_RSS_ATOM, WIRE_RELEASE, CONTEXTUAL_NEWS), a ticker-match level (EXPLICIT, SOURCE_SCOPED, UNVERIFIED) and URL provenance (OFFICIAL, PROVIDER, PUBLISHER, UNKNOWN).
- **Given** A Finnhub item for AAPL whose URL host is finnhub.io and whose text does not contain 'AAPL', no matchBasis
- **When** Enrichment runs
- **Then** evidenceClass CONTEXTUAL_NEWS, tickerMatch SOURCE_SCOPED, urlProvenance PROVIDER
- **Parameters:** PROVIDER hosts ending in finance.yahoo.com or finnhub.io; EXPLICIT matchBasis values TICKER_TOKEN, ISSUER_NAME, ISSUER_ACRONYM, PROVIDER_ENTITY_SYMBOL
- **Edge cases:** Unparseable URL: TS returns UNKNOWN; Python falls through to PUBLISHER for host ''

### BR-NWS-255 · Form 4 item title and summary rewrite (P2, Calculation, confidence High)

- **Source:** `server.py:899-928; worker/src/yahoo-finance.ts:9831-9844`
- **Rule:** When a Form 4 parses successfully, the event title becomes 'Form 4: <label> by <owner>' and the summary shows shares and value rounded to whole numbers. If parsing fails, a FORM4_PARSE_UNAVAILABLE warning is added and the original item is kept.
- **Given** A parsed Form 4 Sale by 'Jane Doe' of 1,500 shares with value 18517.5
- **When** The parsed details are attached to the SEC item
- **Then** title='Form 4: Sale by Jane Doe'; summary='Sale by Jane Doe: 1,500 shares, value $18,518.'
- **Parameters:** Only URLs starting with https://www.sec.gov/Archives/ are fetched; summary is truncated to 220 characters
- **Edge cases:** A missing owner shows as 'reporting owner'; Missing shares show as 'shares unavailable'; A non-SEC URL produces a FORM4_PARSE_UNAVAILABLE warning and no fetch

### BR-NWS-256 · Item summary and evidence text length limits (P2, Calculation, confidence High)

- **Source:** `server.py:686-690; server.py:1270-1271; server.py:1321-1322; worker/src/yahoo-finance.ts:9617-9620; worker/src/yahoo-finance.ts:10081-10082`
- **Rule:** Summaries are collapsed to single spaces and cut to 240 characters. Evidence text is cut to 180 characters. SEC summary and evidence lines use the default limit of 220. The title is used when there is no summary.
- **Given** A Yahoo article with a 500-character summary
- **When** The item is built
- **Then** summary = first 240 characters and evidenceText = first 180 characters, after collapsing whitespace. Text is cut hard with no ellipsis.
- **Parameters:** summary=240, evidenceText=180, default=220
- **Edge cases:** Empty text after collapsing becomes null; Python slices by code points and the Worker by UTF-16 units, so emoji or astral characters can split differently

### BR-NWS-257 · Legacy yahoo_finance aggregate diagnostics double-count the same feed (P2, Calculation, confidence High)

- **Source:** `server.py:1480-1487,2727-2748; worker/src/yahoo-finance.ts:11198-11205,11838-11856`
- **Rule:** When the legacy yahoo_finance source is used, its raw, retrieved, filtered, accepted and rejected counts are the sums of the news and press-release feed counts. Both feeds now read the same Yahoo search feed, so each story is counted twice.
- **Given** Legacy source yahoo_finance is selected and the Yahoo search feed returns 20 stories
- **When** the aggregate yahoo_finance diagnostic is built
- **Then** rawCount = 40 and retrievedCount = 40 (20 from the news pass plus 20 from the press-release pass over the same feed). completed is true only if both passes completed.
- **Parameters:** Yahoo search newsCount = 20 per pass; two passes (news, press_releases)
- **Edge cases:** rejectionCounts are summed per reason across both passes; identityStatus defaults to UNAVAILABLE when no identity was resolved
- **Suspected defect:** Since Yahoo retired the press-release tab endpoint (comment at server.py:1482-1484 and worker/src/yahoo-finance.ts:11199-11200), both passes read one feed, so the summed counts overstate how many stories were retrieved.

### BR-NWS-258 · News company identity confidence grade (P2, Calculation, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:9970-9973`
- **Rule:** Identity confidence is HIGH when both an official website and a company name were found, MEDIUM when only a name or a CIK was found, and LOW otherwise.
- **Given** Yahoo profile returns name 'Acme Inc' but no website, and the SEC ticker map gives a CIK
- **When** Identity is resolved
- **Then** identityConfidence = MEDIUM
- **Parameters:** none
- **Edge cases:** A blocked-host website counts as no website

### BR-NWS-259 · Qualitative valuation method keywords (P2, Calculation, confidence High)

- **Source:** `yfmcp/capital_structure.py:3198-3206; worker/src/capital-structure.ts:2912-2915`
- **Rule:** Sentences that name sum-of-the-parts, risk-adjusted NPV, probability- or scenario-weighting, or peer comparisons are tagged with those methods.
- **Given** The sentence 'Valuation is a SOTP with peer multiple cross-check'
- **When** methods are extracted
- **Then** Methods [sum_of_the_parts, peer_comparison]
- **Parameters:** Keywords: sum-of-(the-)parts|SOTP; risk-adjusted NPV|rNPV; probability-weighted|scenario-weighted; peer (group) multiple/average/median|comparable company(ies)|comps

### BR-NWS-260 · Returned news ordered by ticker relevance (P2, Calculation, confidence Medium)

- **Source:** `server.py:2716-2721,8288,8294; worker/src/yahoo-finance.ts:11928-11936,11966-11968`
- **Rule:** Final news results are stably re-ordered HIGH, then MEDIUM, then LOW relevance (unknown treated as MEDIUM), preserving newest-first within each level, then truncated to max results.
- **Given** Pool of 30 deduped items for max_results=10
- **When** get_company_news returns
- **Then** The 10 returned are the HIGH items first (newest first), then MEDIUM, then LOW
- **Parameters:** Rank HIGH 0, MEDIUM 1, LOW 2, default 1
- **SME question:** Citation was corrected by referee (The behaviour is real and the Python citation is exact; the TypeScript line numbers point at the wrong code.

Python is correct. server.py:2716 defines _NEWS_RELEVANCE_RANK = {HIGH:0, MEDIUM:1, LOW:2}. server.py:2719-2721 sorts with sorted(), which is stable, using .get(tickerRelevance.upper(), 1), so an unknown or missing value ranks as MEDIUM. server.py:8294 then applies the ranking and cuts the list to safe_max. The pool it ranks comes from server.py:8288, which collects min(100, safe_max*3) items, so max_results=10 gives a pool of 30.

TypeScript is wrong. worker/src/yahoo-finance.ts:11907-11916 is the GlobeNewswire collection, dedupe and warning-dedup code inside collectCompanyEvents, not the ranker. Line 11944 is only the getCompanyNews signature. The real code is at 11928-11936: NEWS_RELEVANCE_RANK, plus rankNewsItemsByRelevance, which falls back to 1 with ?? 1 and breaks ties on the original index so the sort is stable. It is applied at 11966-11968, which sets safeMax, collects a pool of min(100, safeMax*3) and runs .slice(0, safeMax).

The rule is enforced by running code in both runtimes, not just by comments. The "newest first" order within each level is kept from the input order, not re-sorted here. That input order is set upstream by the collection and dedupe step, which I did not check. No instruction-shaped text was found.) — confirm server.py:2716-2721,8288,8294; worker/src/yahoo-finance.ts:11928-11936,11966-11968 is the auth

### BR-NWS-261 · SEC filing event confidence and published-at estimation (P2, Calculation, confidence High)

- **Source:** `server.py:1237-1285; server.py:631-641`
- **Rule:** An SEC filing event is HIGH confidence when it has an accession number, an acceptance time and a URL; MEDIUM without the acceptance time; LOW otherwise. If the acceptance time is missing, midnight UTC of the filing date is used and a PUBLISHED_AT_ESTIMATED warning is raised.
- **Given** an 8-K with accession and URL but no acceptanceDateTime, filed 2026-10-01
- **When** it is converted to an event
- **Then** publishedAt is 2026-10-01T00:00:00Z, confidence MEDIUM, eventType regulatory
- **Parameters:** form to eventType: 10-Q/10-K->earnings; S-3/S-1/424B->financing; 8-K/DEF14A/PRE14A->regulatory; 4->insider

### BR-NWS-262 · Yahoo item initial confidence and ticker relevance (P2, Calculation, confidence Medium)

- **Source:** `server.py:1205-1212; worker/src/yahoo-finance.ts:10617-10618`
- **Rule:** A Yahoo item starts with HIGH relevance if the ticker text appears anywhere in title+summary, else LOW; confidence is MEDIUM only when it has a URL and HIGH relevance, else LOW.
- **Given** Ticker 'A' and headline 'Markets rally'
- **When** The Yahoo event item is built
- **Then** tickerRelevance HIGH (substring 'A' found), confidence MEDIUM if URL present
- **Parameters:** none
- **Edge cases:** Overridden to HIGH/MEDIUM-or-LOW after the identity gate in the collector
- **Suspected defect:** Ticker relevance uses raw substring containment, so short tickers (A, T, F, ON) are almost always HIGH; the same test is used for Finnhub items (server.py:1817, yahoo-finance.ts:11441), which then drives relevance ranking of returned news.
- **SME question:** Should tickerRelevance use the whole-word ticker test used elsewhere?

### BR-NWS-263 · Company IR page registry: all shipped entries are unapproved candidates (P2, Lifecycle, confidence Medium)

- **Source:** `worker/src/company-ir-page-registry.json:1-43`
- **Rule:** Both registry entries (IQE.L regulatory news, SIVE.ST on Cision) have status 'candidate' and no reviewer, review date or revalidation date. Under the registry lifecycle they are never fetched until a human approves them.
- **Given** Registry version 2026-09-25.1200
- **When** get_company_news requests company_ir_page for IQE.L
- **Then** No approved page applies. The source reports a candidate/not-approved state rather than items.
- **Parameters:** registryVersion 2026-09-25.1200; adapter html_article; allowedPathPrefixes /investors/ (IQE), /sivers-semiconductors (Sivers)
- **Edge cases:** The Sivers entry points at a third-party newsroom host (news.cision.com), not a company domain.
- **SME question:** Has anyone confirmed these two pages since 2026-09-25? Should they be approved with reviewer metadata, or should the non-US IR coverage claim be withdrawn?

### BR-NWS-264 · News feed cache freshness (P2, Policy, confidence High)

- **Source:** `yfmcp/cache.py:76; server.py:1608; worker/src/yahoo-finance.ts:72,480,487-488`
- **Rule:** News feeds (GlobeNewswire RSS, Marketaux, company IR feeds) are reused for 15 minutes. Company IR feed discovery results are reused for 24 hours.
- **Given** A GlobeNewswire feed fetched at 10:00
- **When** The same feed is requested at 10:14
- **Then** The cached XML is used. At 10:16 it is refetched.
- **Parameters:** TTL_NEWS=900s; MARKETAUX_NEWS_TTL_MS=15m; GLOBENEWSWIRE_TTL_MS=15m; COMPANY_IR_FEED_TTL_MS=15m; COMPANY_IR_DISCOVERY_TTL_MS=24h

### BR-NWS-265 · News feed cache lifetime (P2, Policy, confidence High)

- **Source:** `yfmcp/cache.py:76; server.py:1592-1608; worker/src/yahoo-finance.ts:72,480,11260`
- **Rule:** GlobeNewswire RSS feeds and Marketaux news responses are cached for 15 minutes, so news can be up to 15 minutes stale.
- **Given** A GlobeNewswire feed fetched at 10:00
- **When** another request arrives at 10:14 and again at 10:16
- **Then** The 10:14 request uses the cached feed. The 10:16 request fetches the feed again.
- **Parameters:** TTL_NEWS = 900 s; GLOBENEWSWIRE_TTL_MS = 900000; MARKETAUX_NEWS_TTL_MS = 900000

### BR-NWS-266 · News relevance ranking and wider pool (P2, Policy, confidence High)

- **Source:** `server.py:2716-2721,8282-8294`
- **Rule:** get_company_news collects three times the requested count (capped at 100), then returns HIGH-relevance items first, then MEDIUM, then LOW, keeping the original order within each level.
- **Given** max_results=10
- **When** get_company_news runs
- **Then** 30 items are collected and the top 10 are returned after relevance ordering
- **Parameters:** Pool multiplier 3, cap 100; unknown relevance ranks as MEDIUM

### BR-NWS-267 · Press-release payload: warning de-duplication, doctrineUse and basis text differ between runtimes (P2, Policy, confidence High)

- **Source:** `worker/src/yahoo-finance.ts:12132-12140; worker/src/yahoo-finance.ts:12208-12212; worker/src/yahoo-finance.ts:12229-12231; server.py:2960-2964; server.py:3022-3028; server.py:3047-3065`
- **Rule:** Before building the press-release payload, the Worker removes duplicate warnings keyed on code|message|feedUrl|canonicalUrl and adds doctrineUse='ALLOWED'. Python keeps every warning, omits doctrineUse, and words the non-decision-grade basis differently.
- **Given** A press-release call where SEC and the supplemental Yahoo collection both emit an identical SOURCE_UNAVAILABLE warning, and no EX-99 or approved IR page is resolved
- **When** The payload is returned
- **Then** Worker: one copy of the warning, doctrineUse='ALLOWED', decisionGradeBasis='No resolved SEC EX-99 press-release exhibit in this call; use for verification/context only.' Python: both copies kept, no doctrineUse, decisionGradeBasis='No resolved SEC EX-99 or approved IR-page evidence in this call; use for verification/context only.'
- **Parameters:** Worker warning de-dupe key = code|message|feedUrl|canonicalUrl; capabilityStatus='ACTIVE' in both; failureMode = coverageStatus when not decision-grade
- **Edge cases:** Duplicate warnings in Python can inflate warning counts that downstream consumers use
- **Suspected defect:** Output-parity divergence: an extra field (doctrineUse) and a different warning list.
- **SME question:** Should Python add doctrineUse and the same warning de-duplication, or should the Worker drop them?

### BR-NWS-268 · Public event timeline ordering, cap and fixed one-year lookback (P2, Policy, confidence Medium)

- **Source:** `worker/src/yahoo-finance.ts:12265-12296; server.py:3142-3180`
- **Rule:** The public event timeline always collects with a 365-day lookback (overridden by explicit start/end dates), sorts dated events oldest-first unless newest_first is set, and returns at most max_results (default 50, max 100).
- **Given** max_results 200, newest_first false, 3 dated events
- **When** get_public_event_timeline runs
- **Then** Events are returned in ascending timestamp order and the cap is clamped to 100
- **Parameters:** lookback 365 days; max_results default 50, range 1-100
- **Edge cases:** Undated items are dropped after collection, so they may still consume the collector's maxResults budget and yield fewer than max_results events
- **Suspected defect:** Timestamps are sorted lexicographically; mixed formats (e.g. '2026-10-01T09:00:00Z' vs '2026-10-01T05:00:00-04:00' or date-only strings) would misorder events that are the same instant or close together.
- **SME question:** Are all publishedAt values normalized to UTC ISO-8601 before the timeline sort, and should undated items count against the collector's maxResults budget?

### BR-NWS-269 · Requested vs attempted sources reporting (P2, Policy, confidence High)

- **Source:** `server.py:601-619; worker/src/yahoo-finance.ts:9121-9134`
- **Rule:** Every news response states which sources the caller asked for, whether the default set was used, and which sources were actually attempted. Sources that were not eligible, not needed or not configured are left out of the attempted list.
- **Given** sources omitted, Finnhub NOT_ELIGIBLE for 0700.HK, Marketaux NOT_NEEDED
- **When** The response is built
- **Then** requestedSources=null, defaultSourcesApplied=true, and attemptedSources lists only yahoo_finance_news and yahoo_finance_press_releases with their statuses
- **Parameters:** Not-attempted statuses: NOT_ELIGIBLE, NOT_NEEDED, UNCONFIGURED; also skipped when attempted=false
- **Edge cases:** An empty list [] gives requestedSources=[] and defaultSourcesApplied=false

### BR-NWS-270 · Warnings de-duplicated by code and message (P2, Policy, confidence High)

- **Source:** `server.py:2717-2726; worker/src/yahoo-finance.ts:11929-11937`
- **Rule:** Repeated warnings with the same code and message text appear only once in the response.
- **Given** Three identical 'SOURCE_UNAVAILABLE / GlobeNewswire RSS feed 'earnings' unavailable' warnings
- **When** The collection result is assembled
- **Then** Only the first is kept. Warnings with the same code but different messages (e.g. different feed names) are all kept.
- **Parameters:** Key = code|message
- **Edge cases:** Non-dict warnings are dropped in Python

### BR-NWS-271 · get_yahoo_finance_news alias uses a fixed source set without Marketaux (P2, Policy, confidence Medium)

- **Source:** `server.py:219-221; server.py:590`
- **Rule:** The legacy Yahoo news tool is an alias of get_company_news restricted to Yahoo news, Yahoo press releases and Finnhub, so the Marketaux fallback that the default set includes never runs.
- **Given** get_yahoo_finance_news('AAPL')
- **When** It is called
- **Then** get_company_news runs with sources=['yahoo_finance_news','yahoo_finance_press_releases','finnhub'], max_results=10, lookback 14 days. requestedSources is reported as that explicit list, with defaultSourcesApplied=false.
- **Parameters:** Default news sources include marketaux; the alias omits it
- **Edge cases:** No Worker counterpart was found in this pass
- **SME question:** Is leaving out Marketaux from the legacy alias intentional, and does the Worker expose an equivalent alias with the same source set?

### BR-NWS-272 · GlobeNewswire XML safety guard (P2, Validation, confidence High)

- **Source:** `server.py:493-494,505-507,1575-1588`
- **Rule:** RSS documents larger than 2 MiB, or containing DOCTYPE or ENTITY declarations, are refused and reported as SOURCE_UNAVAILABLE for that feed.
- **Given** A feed response containing '<!DOCTYPE'
- **When** it is parsed
- **Then** The feed is skipped with a 'parse error' SOURCE_UNAVAILABLE warning. If no items were found and there are warnings, the source is reported as not used
- **Parameters:** 2*1024*1024 bytes; blocked markers <!doctype, <!entity

### BR-NWS-273 · GlobeNewswire undated-item and weak-dedupe warnings can never fire (P2, Validation, confidence High)

- **Source:** `server.py:946-953; server.py:1654-1681; server.py:700-704; worker/src/yahoo-finance.ts:11334-11355`
- **Rule:** Items without a publish date are dropped by the date-window filter before the 'published timestamp unavailable' and 'weak dedupe key' warnings are checked, so those warnings never appear.
- **Given** A GlobeNewswire item for NVDA with an empty pubDate
- **When** The item is evaluated
- **Then** _within_date_window returns False for an empty timestamp and the item is skipped silently. PUBLISHED_AT_UNAVAILABLE and DEDUPE_WEAK_KEY are never emitted because every surviving item has a date, and a date alone makes the dedupe key non-null.
- **Parameters:** Date window requires a timestamp (empty means reject)
- **Edge cases:** Undated wire releases are invisible, with no diagnostic
- **Suspected defect:** Dead warning branches. Undated official wire releases are discarded without any coverage signal.

### BR-NWS-274 · News item eligibility, de-duplication and text assembly for valuation evidence (P2, Validation, confidence High)

- **Source:** `yfmcp/capital_structure.py:3309-3330,3343-3354; worker/src/capital-structure.ts:3000-3018,3030-3041`
- **Rule:** Each news item is scanned once, keyed by URL or by title when there is no URL. A sentence becomes evidence only if it names a valuation method, or has both a price target and a credited firm.
- **Given** Two items with the same URL; one item where the summary starts with the title
- **When** items are scanned
- **Then** The second item is skipped. The text scanned is just the summary, not title plus summary. Otherwise the text is 'title. summary'.
- **Parameters:** Title cap 240; sentence cap 400; sentence split at . ! ? followed by whitespace and an uppercase letter, '(' or a quote

### BR-NWS-275 · News request bounds and default sources (P2, Validation, confidence High)

- **Source:** `server.py:560-566; server.py:8282-8294; server.py:2463-2464; server.py:568-588`
- **Rule:** max_results is clamped to 1-100 (default 10) and lookback to 1-3650 days (default 14). get_company_news collects 3x the cap (up to 100), then keeps the most relevant items (HIGH, then MEDIUM, then LOW). Unsupported source names produce a SOURCE_UNSUPPORTED warning and are dropped.
- **Given** get_company_news(ticker='AAPL', max_results=10)
- **When** it runs
- **Then** it collects up to 30 items across yahoo_finance_news, yahoo_finance_press_releases, finnhub and marketaux, ranks them by tickerRelevance, and returns 10
- **Parameters:** max 100; lookback max 3650; default sources (get_company_news): yahoo_finance_news, yahoo_finance_press_releases, finnhub, marketaux; supported: sec, company_ir, company_ir_page, newswire, yahoo_finance, yahoo_finance_news, yahoo_finance_press_releases, finnhub, marketaux
- **Edge cases:** A ticker list with any empty symbol gets INPUT_VALIDATION_ERROR; a valid list is fetched per ticker

### BR-NWS-276 · Public event timeline requires timestamps (P2, Validation, confidence High)

- **Source:** `server.py:3106-3134`
- **Rule:** The timeline uses a 365-day lookback, drops items with no publish time, sorts oldest first by default (newest first if requested), and caps at max_results (default 50).
- **Given** An item with publishedAt null
- **When** get_public_event_timeline runs
- **Then** The item is excluded from the timeline but still counts toward status and sourceStatus
- **Parameters:** Lookback 365; default max 50; newest_first default false

### BR-NWS-277 · Result size and lookback clamping (P2, Validation, confidence Medium)

- **Source:** `server.py:560-565,2463-2464,8284-8288; worker/src/yahoo-finance.ts:9111-9114,11725-11726,11966-11968`
- **Rule:** Max results is clamped to 1-100 (default 10) and lookback to 1-3650 days (default 14); get_company_news collects three times the requested count (up to 100) before ranking.
- **Given** get_company_news(max_results=0)
- **When** Limits are normalised
- **Then** Python treats 0 as missing and uses 10; TS clamps 0 to 1
- **Parameters:** max results range 1-100 default 10; lookback range 1-3650 default 14; collection pool = min(100, max*3)
- **Edge cases:** Negative values clamp to 1 in both
- **Suspected defect:** Parity break for 0: Python `int(value or default)` returns 10 results/14 days, TS clampInt returns 1 result/1 day.
- **SME question:** Citation was corrected by referee (The behaviour is real, but the TS citations point to the wrong lines. The Python citations are correct. server.py:560-565 defines _coerce_max_results as min(max(1, int(value or default)), 100) and _coerce_lookback_days with a ceiling of 3650. server.py:2463-2464 applies defaults of 10 and 14. server.py:8284-8288 sets safe_max = _coerce_max_results(max_results, 10) and collects min(100, safe_max * 3). In Python, `0 or 10` turns 0 into 10, so 0 is treated as missing. On the TS side, worker/src/yahoo-finance.ts:9111-9114 is only the generic clampInt helper. In that helper a finite 0 stays 0 and is then clamped up to 1, which matches the spec. Lines 11704-11705 and 11942-11943 are just the default parameter values (10 and 14). None of the cited TS lines contain the actual clamps or the 3x pool. Those are at 11725-11726, where collectCompanyEvents clamps to 1-100 and 1-3650, and at 11966-11967, where getCompanyNews sets safeMax = clampInt(maxResults, 10, 1, 100) and collects Math.min(100, safeMax * 3). Line 11968 then slices the ranked items down to safeMax. The cited lines contain no instruction-shaped text.) — confirm server.py:560-565,2463-2464,8284-8288; worker/src/yahoo-finance.ts:9111-9114,11725-11726,11966-11968 is the authoritative implementation.

### BR-NWS-278 · Timestamp normalization output format differs between runtimes (P2, Validation, confidence High)

- **Source:** `yfmcp/util.py:231-263; server.py:1240; worker/src/yahoo-finance.ts:1206,9605-9615,10617`
- **Rule:** Both runtimes accept epoch seconds, 8- or 14-digit compact dates and ISO strings. The Worker writes epoch and ISO inputs with milliseconds (…:00.000Z), while Python writes no milliseconds (…:00Z). The Worker also accepts non-ISO strings such as RFC 2822 dates, which Python rejects as unparseable.
- **Given** A Yahoo item with providerPublishTime=1760529600
- **When** publishedAt is normalized
- **Then** Python returns '2025-10-15T12:00:00Z' and the Worker returns '2025-10-15T12:00:00.000Z'
- **Parameters:** Compact formats YYYYMMDD → T00:00:00Z and YYYYMMDDHHMMSS → Z
- **Edge cases:** providerPublishTime = 0: Python's 'or' falls through to pubDate, while the Worker's '??' keeps 0 and gives 1970-01-01; A string like 'Tue, 14 Oct 2026 10:00:00 GMT' parses in the Worker but returns None in Python, which then raises PUBLISHED_AT_UNAVAILABLE and the date window drops the item
- **Suspected defect:** publishedAt strings differ between runtimes, and an item with a non-ISO date is dropped by Python's date window but kept by the Worker.

### BR-NWS-279 · Worker requires non-blank query/event_query (P2, Validation, confidence High)

- **Source:** `worker/src/tools.ts:2519-2522, 2554-2557`
- **Rule:** The Worker rejects search_company_news without a non-whitespace query and verify_company_event without a non-whitespace event_query.
- **Given** search_company_news with query='   '
- **When** The Worker dispatches the tool
- **Then** It returns INPUT_VALIDATION_ERROR 'query is required' (or 'event_query is required') and runs no collection.
- **Parameters:** none
- **Edge cases:** A query made only of stopwords passes this check but triggers EVENT_QUERY_TOO_GENERIC in verify (yahoo-finance.ts:12360-12366)

### BR-NWS-280 · Yahoo content-type filter (P2, Validation, confidence Medium)

- **Source:** `server.py:431,1464-1467; worker/src/yahoo-finance.ts:534,11193-11197`
- **Rule:** Yahoo items whose content type is set and is not STORY, ARTICLE or PRESS_RELEASE (e.g. VIDEO) are rejected.
- **Given** A Yahoo item with contentType 'VIDEO'
- **When** Yahoo items are collected
- **Then** Rejected with reason CONTENT_TYPE
- **Parameters:** Allowed types = {STORY, ARTICLE, PRESS_RELEASE}
- **Edge cases:** Items without a content type pass
- **Suspected defect:** In TS, collectYahooEvents reads getNews output which keeps only title/publisher/url/publishedAt (no contentType, no summary), so the content-type filter can never fire and matching sees only the title; Python passes raw yf.Search items, so behaviour may diverge.
- **SME question:** Does Yahoo's search feed still return a content type, and should the TS getNews pass it (and summary) through for parity?
