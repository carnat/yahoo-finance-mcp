# Yahoo Finance MCP Modernization — Open Questions Decision Artifact

**Date:** 2026-10-15  
**Status:** Decision candidate for modernization-brief refinement; no Doctrine amendment or capital authority created by this artifact.  
**Scope:** Resolves §7 Q1–Q10 of `MODERNIZATION_BRIEF.md` using the current `BUSINESS_RULES.md` extraction and the current Sniper Doctrine MCP evidence contract. Doctrine refinement follows after these modernization-owner decisions are accepted.

---

## Executive Decision

Proceed with the modernization architecture, but do **not** use “preserve current behavior” as an absolute rule.

**Controlling modernization principle:**

> Pure restructuring preserves behavior. A confirmed Doctrine-critical correctness defect is repaired before, or in the same governed release as, the phase that would otherwise preserve/fossilize it.

Repository P0 remains an engineering/data-integrity priority. A separate Doctrine-critical contract class will be defined during the subsequent Doctrine refinement.

---

# Q1 — Six Uncertain Top-Priority Rules

## Q1 Decision

All six remain P0-class concerns, but their rule cards are not equally ready to serve as equivalence oracles. Correct the cards before Phase 2 as follows.

### BR-PRC-002 — Daily price freshness classification

**Decision: CONFIRM CORE RULE; CORRECT TARGET SEMANTICS.**

Keep `CURRENT / STALE / UNKNOWN`, the completed-bar requirement, and the existing close-match tolerance.

Target semantics must be **exchange-session-aware**, not merely weekday-aware. A market holiday must not make a valid prior completed session look stale. If the prior exchange session cannot be established reliably, return `UNKNOWN`; do not infer CURRENT or STALE from weekday arithmetic alone.

For Phase-2 characterization, record the current weekday fallback exactly as current behavior, but mark the holiday limitation as a planned correctness repair rather than as desired final semantics.

### BR-EXT-015 — Release figure scale and loss sign

**Decision: CORRECT THE RULE CARD; CHARACTERIZE CURRENT BEHAVIOR; REPAIR UNSAFE SIGN CASES LATER.**

The current card overstates loss-sign behavior.

Correct contract description:

- table scale applies only to metrics that accept monetary scale;
- EPS and gross margin do not inherit release-table scale;
- for ordinary monetary metrics, parentheses, ASCII minus, or qualifying `negative` wording can set the sign;
- `loss` wording has narrower EPS-specific handling in current code and must not be generalized in the characterization oracle;
- repeated identical scale declarations count as one distinct scale;
- multiple distinct scales skip that candidate but do not necessarily terminate later candidate search;
- explicit-scale and table-scaled monetary values use the implementation's whole-unit rounding behavior.

Separately classify unsafe monetary-loss sign cases as correctness defects for Q3i.

### BR-VAL-004 — ADR ratio detection and share-basis mismatch

**Decision: CORRECT CARD; PRESERVE CURRENT BEHAVIOR FOR REFACTOR.**

The characterization contract must include:

- direct SEC/Yahoo near-whole ADR ratio detection;
- inverse near-whole ratio detection;
- whole-share half-up rounding after ratio adjustment;
- mismatch fallback consequences, including loss of SEC diluted-share usability and fallback to the Yahoo quoted/basic share basis.

The basic ADR-versus-mismatch policy is acceptable for modernization characterization.

### BR-VAL-006 — Historical balances and no-borrowings treatment

**Decision: CORRECT CARD; CHARACTERIZE CURRENT CODE; LOG PARTIAL-GROUP DEBT RISK FOR LATER REPAIR.**

Correct the card to reflect that the implementation selects the first debt concept group with **at least one** tagged component, not necessarily a fully populated group.

The zero-debt route must be documented with all existing conditions, including accession and US-GAAP borrowing-concept requirements; it is not a generic IFRS rule.

Phase 2 should faithfully characterize this current behavior. However, possible debt understatement from a partially populated debt group is not blessed as economically correct and should enter the later correctness-repair queue.

### BR-VAL-022 — Metric source reconciliation status

**Decision: CORRECT CARD; PRESERVE CORE SEMANTICS.**

Target contract:

- `AGREED` requires latest SEC plus at least one independent matching provider;
- without SEC, other sources may still produce `CONFLICT`, but they cannot produce `AGREED`;
- tolerance includes percentage tolerance plus both observation/reference precision components and the implementation's numerical slack;
- a restatement may coexist with `AGREED` and is not itself a conflict;
- two SEC observations do not count as two independent providers;
- preserve the actual status-precedence order.

### BR-VAL-025 — Parent net income from ProfitLoss less NCI

**Decision: CORRECT CARD AND SCHEDULE A BEHAVIORAL REPAIR.**

Correct the characterization contract:

- derivation applies whenever `ProfitLoss` is the chosen US-GAAP concept, not only when it is the only concept present;
- NCI must match the relevant accession, start/end period and unit;
- first-filed and latest derivations require their corresponding matched NCI observations;
- revenue concept tie-breaking remains a separate behavior and should be described independently.

Current fallback to raw `ProfitLoss` when required NCI is missing should be characterized for baseline equivalence, but it should **not** remain target decision-grade parent net income. Target behavior should return an unresolved/limited parent-income derivation rather than silently treating consolidated ProfitLoss as parent net income.

---

# Q2 — URL Fetch Scope and `/mcp` Authentication

## Q2a — Fetch allow-list

**Decision: STRICT HTTPS ALLOW-LIST.**

URL-fetching actions may reach only:

1. SEC Archives / explicitly approved SEC filing endpoints;
2. exact Yahoo hosts required by the supported public transcript/news workflows;
3. exact hosts from IR-registry entries whose state is `approved`.

Requirements:

- revalidate every redirect hop;
- reject private, loopback, link-local and other non-public destinations;
- reject IP-literal bypasses;
- do not use broad wildcard domains merely for convenience;
- do not permit arbitrary caller-provided URL fetches.

**Keep `raw_text` as the escape hatch.** `raw_text` means caller-supplied content only. It must never auto-detect a URL and fetch it.

## Q2b — `/mcp` auth and paid providers

**Decision: STATIC BEARER TOKEN FOR THIS MODERNIZATION, BEHIND A REPLACEABLE AUTH ABSTRACTION.**

Do not mix OAuth or a Cloudflare-Access-specific identity architecture into the same modernization program. A later auth project may replace the implementation without changing the MCP semantic contract.

Rate-limit and provider-budget identity should key from the authenticated credential/subject, not merely source IP.

**Anonymous paid-provider access: NO.**

- `/mcp` requires authentication.
- An unauthenticated health/version endpoint is acceptable only if it performs no provider calls.
- Paid providers require authenticated access **and explicit caller opt-in**.
- Default waterfalls must not silently force paid-provider calls.

Provider budget exhaustion must return a typed failure/non-evidence state such as `PROVIDER_BUDGET_EXHAUSTED`; it must never masquerade as `NOT_FOUND` or clean absence.

---

# Q3 — Python / Worker Differences

## Q3a — `accession_number`

**Decision: CHANGE. BLOCKS PHASE 4 UNTIL FIXED.**

Worker must honor an exact accession request with the same semantics as Python. Exact-accession failure must remain explicit failure/non-evidence rather than silently falling back to another filing.

Implement before Phase 4.

## Q3b — ticker validation and accession validation

**Decision: CHANGE.**

Centralize validation and apply it consistently in both runtimes to every relevant public action. Remove the Worker-only gaps and also eliminate equivalent Python paths that skip the shared validation contract.

## Q3c — zero volume and Worker batch cap

**Decision: CHANGE BOTH.**

Zero-volume completed session:

- ratio = `null`;
- flag = `null`/UNKNOWN;
- emit an explicit zero-volume/no-meaningful-ratio warning.

Do not label zero volume as `LOW` merely because arithmetic produced zero.

Batching:

- remove silent Worker truncation;
- use one public, schema-advertised maximum in both runtimes;
- reject an oversize request explicitly rather than dropping tickers.

Use **10** as the modernization cap unless a measured production constraint justifies another value.

## Q3d — price-target rounding and `dataDate`

**Decision: CHANGE.**

- classification/decision thresholds use the unrounded raw ratio;
- rounding is display-only;
- both runtimes use one deterministic display-rounding function;
- missing `dataDate` remains `null`/unknown;
- never synthesize the current date as a data date.

## Q3e — reconciliation tolerance and `AGREED`

**Decision: CHANGE.**

- NaN and ±infinity are invalid tolerance inputs and fall back to the documented 0.5% default;
- then clamp finite values to the supported range;
- align Worker/Python descriptions and implementation;
- `AGREED` means latest SEC plus at least one independent matching provider.

## Q3f — ITM equality (`>=` vs `>`)

**Decision: CHANGE TO `>=` CONSISTENTLY.**

Use inclusive equality across convertible/preferred/options/warrants. Treasury-stock treatment at exact strike still produces zero incremental option/warrant shares, so this yields one semantic boundary without inventing dilution.

## Q3g — valuation market inputs

**Decision: CHANGE.**

Do not retain two valuation realities based on Python `.info` versus Worker raw `quoteSummary`.

Create a normalized `MarketInputs` contract owned by the shared Yahoo client. Both runtimes must consume that normalized representation. Remove or repurpose the currently unused normalization helper only after parity fixtures prove the replacement.

## Q3h — `sources=[]` substitution drift

**Decision: CHANGE.**

- `sources` omitted or null = documented default source set;
- explicit `sources=[]` = attempt no sources;
- preserve separate `requestedSources`, `attemptedSources`, `skippedSources` and default-substitution metadata;
- `sourceStatus` must describe what actually happened, not what would have happened under defaults.

## Q3i — remaining defects

**Decision: FIX CONFIRMED SEMANTIC / DATA-INTEGRITY DEFECTS; PRESERVE ONLY COSMETIC OR INTENTIONAL COMPATIBILITY DIFFERENCES.**

Do not defer a Doctrine-critical defect to Phase 6 when its domain is being moved earlier.

Confirmed repair set includes at least:

- newest-first earnings beat streak;
- multi-line Form 4 parsing instead of first-line-only;
- valid Feb-29 date arithmetic;
- correct recognition of non-USD dollar currencies such as `NT$` / `A$`;
- incomplete-TTM handling (do not label fewer than four quarters as a normal annual TTM without an explicit partial state);
- loss-making/net-debt credit classification must not become mechanically `CLEAN` from a negative leverage ratio; use an unresolved/negative-earnings-base state;
- short-history CAGR must not masquerade as a five-year CAGR;
- provider-eligibility policy read/parse failure must fail closed rather than enable every ticker;
- valuation EV must not remove convertible debt unless the corresponding equity treatment is valid in the same share basis;
- transcript fallback ordering must reflect sources actually attempted/failed;
- earnings-surprise actuals must use the governed source policy consistently;
- News coverage cannot report clean `FULL` coverage while required sources are in failed states.

**Phase rule:** repair a confirmed Doctrine-critical defect before, or in the same governed release as, the phase that moves that domain.

---

# Q4 — Smoke-Test Ticker Table

**Decision: REMOVE FROM PRODUCTION LOOKUP.**

Keep smoke mappings only as injected test fixtures/test seams. Production identity resolution must never succeed because a ticker exists in a smoke-test fallback table.

---

# Q5 — `extract_china_exposure`

**Decision: GENERALISE; KEEP THE PUBLIC ACTION AS A COMPATIBILITY PRESET.**

Implement `extract_china_exposure` as a governed preset over generic `extract_exposure`.

Issuer-specific subsidiary/entity aliases such as Tongmei/JinMei/BoYu belong in a data registry/configuration layer, not executable function logic.

Do not retire the public action yet because downstream Doctrine and existing callers already understand its semantic purpose.

---

# Q6 — Keep Both Runtimes

**Decision: YES.**

Keep:

- Python local/stdio runtime;
- hosted Worker runtime.

Cross-runtime semantic parity is therefore a first-class release requirement.

Retiring either runtime is a separate rearchitecture decision and is outside this modernization.

---

# Q7 — Deeper Rule Extraction

**Decision: REQUIRED.**

Run targeted extraction before touching each thin domain:

- **News:** before Phase 3;
- **Fundamentals:** before its Phase-5 extraction;
- **Providers:** before its Phase-5 extraction.

The existing extraction stopped after four rounds before running dry, so the current small rule counts are not sufficient evidence that these domains are genuinely simple.

---

# Q8 — MCP SDK Major

**Decision: PIN THE CURRENT PRODUCTION MAJOR; DO NOT UPLIFT MAJOR VERSION INSIDE THIS PROGRAM.**

The supplied modernization brief does not establish whether production is presently on SDK major 1.x or 2.x. Therefore the decision is policy-level rather than a guessed version number:

> Inspect the actual package manifest, pin the currently deployed major and supported public APIs, and keep that major throughout modernization. A major-version migration is a separate dependency-uplift project.

The brief should be edited to record the literal observed major once verified from the repository.

---

# Q9 — Release Cadence for Pure Refactors

**Decision: ONE DOMAIN/UNIT PER PR; MULTIPLE VERIFIED PURE-REFACTOR PRs MAY SHARE ONE RELEASE AFTER THE PILOT.**

Rules:

1. Keep one domain/extraction unit per PR for reviewability and rollback.
2. Security changes, shared-client changes, SEC extraction, valuation and earnings receive isolated releases where blast radius is material.
3. After the News pilot demonstrates the process, multiple small, disjoint, behavior-identical extraction PRs may share one deployed release.
4. Every PR included in a shared release must independently show zero prohibited parity delta.
5. The combined release candidate must rerun the full acceptance/parity/canary suite.
6. Every **deployed release** gets one version bump, release/docs entry, convergence check and live verification.

This reduces release noise without sacrificing bisectability or deployment evidence.

---

# Q10 — Worker Latency Baseline

**Decision: YES. EXPORT PRIVACY-MINIMIZED WORKER TELEMETRY BEFORE PHASE 4.**

Capture at minimum:

- action/domain;
- server/runtime/build identity;
- total latency;
- provider-attempt class/count;
- success/failure/status class;
- response-size bucket;
- timeout/retry class.

Do **not** export:

- auth tokens or credentials;
- request/response payload bodies;
- raw filings, transcripts or news text;
- arbitrary caller-supplied `raw_text`;
- sensitive query contents not required for aggregate latency/error measurement.

Use the baseline to compare pre/post Phase-4 latency and error rates and to detect regressions introduced by shared-client extraction.

---

# Ready-to-Paste §7 Replacement

```markdown
## 7. Open Questions — RESOLVED OWNER DECISIONS (2026-10-15)

- [x] **Q1 (gates Phase 2).** Six Medium-confidence P0 cards reviewed. Correct the behavior cards before Phase 2: BR-PRC-002 keeps CURRENT/STALE/UNKNOWN but target freshness is exchange-session-aware; BR-EXT-015 must describe the narrower current scale/sign logic; BR-VAL-004 must include inverse ADR detection, rounding and mismatch consequences; BR-VAL-006 must describe first-partially-tagged debt-group behavior and conditional US-GAAP zero-debt handling; BR-VAL-022 must reflect actual SEC-baseline/status/tolerance semantics; BR-VAL-025 must reflect chosen-ProfitLoss/NCI-match behavior. BR-VAL-025 raw-ProfitLoss fallback and other named unsafe behaviors are characterized for baseline but queued as correctness repairs.

- [x] **Q2a (gates Phase 1).** URL fetch allow-list = HTTPS SEC Archives/approved SEC endpoints + exact supported Yahoo hosts + exact hosts of `approved` IR-registry entries. Validate every redirect hop; reject private/link-local/loopback/IP-literal bypasses. Keep `raw_text` as caller-supplied-content escape hatch only; it never auto-fetches URLs.

- [x] **Q2b (gates Phase 1).** `/mcp` uses a static bearer token for this modernization behind a replaceable auth abstraction. Rate limits/provider budgets key from authenticated identity. Anonymous `/mcp` provider calls are not allowed. Paid providers require authenticated explicit opt-in; budget exhaustion is typed failure/non-evidence, never NOT_FOUND.

- [x] **Q3a.** CHANGE: Worker must honor exact `accession_number` with Python-equivalent fail-closed semantics before Phase 4.
- [x] **Q3b.** CHANGE: centralize ticker/accession validation and apply it consistently across both runtimes/public actions.
- [x] **Q3c.** CHANGE: zero-volume ratio/flag = null/UNKNOWN with explicit warning. Remove silent Worker truncation; one advertised cross-runtime batch cap, initially 10, with explicit oversize rejection.
- [x] **Q3d.** CHANGE: thresholds use unrounded raw values; display rounding is deterministic and cross-runtime; missing `dataDate` stays null and never falls back to today.
- [x] **Q3e.** CHANGE: NaN/inf tolerance -> documented 0.5% default before finite clamp; align AGREED definition to latest SEC + >=1 independent matching provider.
- [x] **Q3f.** CHANGE: use inclusive `>=` ITM boundary consistently across instrument classes; treasury-stock math still yields zero incremental option/warrant shares at exact strike.
- [x] **Q3g.** CHANGE: replace `.info` vs raw quoteSummary divergence with one normalized shared-client `MarketInputs` contract consumed by both runtimes.
- [x] **Q3h.** CHANGE: omitted/null `sources` uses defaults; explicit `sources=[]` means attempt none. Preserve requested/attempted/skipped/defaulted metadata separately.
- [x] **Q3i.** CHANGE confirmed semantic/data-integrity defects. Preserve only cosmetic/intentional compatibility differences. Doctrine-critical defects are repaired before or in the same governed release as the phase touching their domain.

- [x] **Q4 (Phase 6).** Remove `SMOKE_TICKER_CIK_FALLBACKS` from production lookup; retain only as injected test fixtures.

- [x] **Q5 (Phase 5/6).** Generalise `extract_china_exposure` into a governed preset over `extract_exposure`; move issuer-specific aliases to data/config. Keep the public compatibility action for now.

- [x] **Q6 (whole plan).** Keep both runtimes: Python local/stdio + hosted Worker. Runtime retirement is a separate rearchitecture decision.

- [x] **Q7 (Phase 3, Phase 5).** Run targeted rule extraction for News before Phase 3 and Fundamentals/Providers before their Phase-5 units.

- [x] **Q8 (Phase 6).** Pin the currently deployed MCP SDK major and public APIs; do not perform a major-version uplift inside modernization. Record the literal current major after inspecting the package manifest. Major uplift is a separate project.

- [x] **Q9 (after pilot).** One domain/unit per PR. After the News pilot, multiple small disjoint behavior-identical refactor PRs may share a release if each has zero prohibited parity delta and the combined release reruns full acceptance/parity/canary tests. Every deployed release still receives one version bump, docs/release entry, convergence check and live verification.

- [x] **Q10.** Export privacy-minimized Worker operational telemetry before Phase 4 for latency/error baseline. Log action/domain, build identity, latency, provider-attempt/status class and response-size bucket; never log credentials, payload bodies, raw source content or caller raw_text.
```

---

## Gate State After These Decisions

**Modernization §7 owner decisions:** substantially resolved.  
**Q8 literal SDK-major number:** must be filled from the actual package manifest; policy decision is already resolved.  
**Next governance step:** refine the Sniper Doctrine MCP contract against these decisions before approving modernization execution phases.

