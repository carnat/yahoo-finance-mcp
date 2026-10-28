# 3.0.0 breaking-change release: scope draft

Status: **draft, 2026-10-28.** Written while the owner decides whether the Phase 6 removals ship now as 3.0.0 (with an exception to the "Phase 5 exit" entry criterion) or after the Phase 4 and Phase 5 moves. Nothing here is built yet.

## 1. Why now is possible

- Every removal below already has a notice live: the 2.8.0 `DEPRECATED_FIELD` notices (N1 to N3) and the 2.27.0 notices (F5/F45, F17, F20, F47, F61; docs notices for F59 and F60 part 3). Both runtimes emit them from `worker/src/deprecations.ts` and `yfmcp/deprecations.py`. The window is "at least one minor version"; 2.27.0 is that minor.
- The fresh functional audit, the other Phase 6 entry criterion, is done (2026-10-27, `/mnt/project-files/audits/mcp-audit-2026-10-27.md`).
- The removals do not depend on the domain moves. Doing them first shrinks the code that Phase 4 and Phase 5 move.
- What would be left for later: the Phase 6 cleanup items that are not breaking (shared data dir, stdlib calls, swallowed errors, stale comments, `docs/architecture.md`). They stay after Phase 5.

## 2. Removals with a settled replacement

Each row deletes the field in both runtimes (or in the one named), deletes its notice from `deprecations.*`, and flips its parity cases.

| ID | Removed | Replacement already present | Runtimes |
| --- | --- | --- | --- |
| N1 | `doctrineUse` (meta; data of press releases and section markdown) | `reliabilityClass` | Worker (Python never emits it) |
| N1 | internal `DoctrineToolStatus` type, `TOOL_DOCTRINE_STATUS` table | renamed to neutral names (`ToolReliabilityStatus`, `TOOL_RELIABILITY_STATUS`) | Worker; Python already uses `TOOL_RELIABILITY_CLASS` |
| N2 | `g2`, `opportunity`, `action`, `priceTarget`, `scenarioWeights`, `selectedMethod`, `selectedMultiple` | `decisionUse: EVIDENCE_ONLY` | both |
| N3a | `calculate_price_target_distance` `bracket`, `inferredTag`, `tag`, `tagNote` | `priceAtOrAboveTarget`, `referenceTargetPct` | both |
| N3b | `check_volume_liquidity_threshold` `gatePass`, `gateThresholdUsd`; `get_market_snapshot` `volume.liquidityGatePass` | `meetsMinimum` / `volume.meetsMinimum` with `min_adv_usd` | both |
| N3c | `analyze_position_signals` `t1_inputs`, `t2_inputs`, `t4_inputs`, `t5_inputs` | `analystInputs`, `pricePositionInputs`, `earningsInputs`, `technicalInputs` | both |
| F5/F45 | `meta.decisionGrade` on `extract_sec_filing_fact`, `get_company_press_releases`, `get_sec_filing_section_markdown` | payload `decisionGrade` | Worker |
| F61 | `analyze_options_flow_window` `ivPctile`, `putVolVs10dAvg` | `ivVsRealizedRangePct`, `putVolPer1PctStockAdv` | both |
| F61 | `get_analyst_rating_changes` `upgrades30d`, `downgrades30d`, `initiations30d` | `upgrades`, `downgrades`, `initiations` + `windowDays` | both |
| F17 | Python `get_ownership_holders` yfinance column keys | the Worker's camelCase quoteSummary rows | Python |
| F47 | Python `get_company_events_calendar` yfinance keys | `calendar.earnings.earningsDate`, `calendar.earnings.*`, `calendar.dividendDate`, `calendar.exDividendDate` | Python |

Also: the `tagNote` / `referenceTargetPct` wording notes from the RN status (brief §5, RN) are moot once the fields go. The 2.8.0 fixed $10M gate constant goes with N3b.

## 3. Reshapes that need a planner rule first

These change values or shapes, not just delete keys. Each rule below is a proposal; per `CLAUDE.md` it is fixed by the planner with the owner before any build.

### 3.1 F20: status strings out of `confidence`

Sites today (both runtimes): `EXTRACTION_FAILED`, `NOT_DISCLOSED` (extractors, earnings metrics, guidance, exposure, segment/geo revenue), `FILING_NOT_FOUND_TRY_OTHER_TYPE`, `FILING_TEXT_NOT_AVAILABLE`, `TICKER_NOT_FOUND` (Python empty result), `NOT_DECISION_GRADE` (`extract_sec_filing_fact` failures, section markdown, earnings guidance failure), `NOT_FOUND` (earnings). About 25 literal sites per runtime, plus places that copy a sub-metric's confidence upward (`extractEarningsMetrics` aggregate).

Proposed rule:
- `confidence` is `HIGH`, `MEDIUM`, `LOW` or `null`. It is `null` wherever it held a status string (no value, so no confidence in it).
- If the object already has `status`, nothing else changes (most sites already carry the same string in `status`).
- If it has no `status`, it gains `status` with the old string. Sites without a sibling `status` today: earnings metric defaults (`{value, unit, confidence, evidence}`), the `grossMargin` default, some exposure/segment payloads. This adds a field, so it needs the owner's OK.
- An aggregate confidence computed from sub-metrics ignores `null`s and is `null` when every sub-metric is `null`.
- Alternative the owner may prefer: `LOW` instead of `null`. Rejected in this draft because `LOW` on an absent value reads as "a value exists but is weak".

### 3.2 F59: one earnings envelope shape

Proposed (from the F54-F62 recommendations): the Worker's keys. `sources[0].sourceType` is `sec_8k` with `items`, `isInlineXBRL`, `cikPadded`; add `contentUrl` (the EX-99.1 exhibit that was read) in both runtimes. Python drops top-level `errorCode`, `meta.generatedAt` and the `sec_8k_ex991` source type. `contentUrl` is a new field in the Worker too.

### 3.3 F60 part 3: one period vocabulary

Announced form: `latest`, `FY2026`, `FY2026 Q3`. Refused with `INPUT_VALIDATION_ERROR`: `latest_quarter`, `latest_annual`, `Q3 2026`. Open points:
- `reconcile_metric_sources` today reads `Q3 2026` as the **calendar** quarter of the period end, while `FY2026 Q3` is a **fiscal** quarter. The new form changes the meaning for off-calendar issuers (AAPL, MSFT). Proposal: `FY<yyyy> Q<n>` is the issuer's fiscal quarter from SEC `fy`/`fp`, as `FY<yyyy>` already is.
- `latest_quarter` becomes `latest`; `latest_annual` has no direct form. Proposal: `latest` keeps meaning the latest quarter in reconcile, and the latest annual is asked as `FY<yyyy>`. Alternative: keep `latest_annual` as an exception.
- The bare year (`2025`), accepted today by the extractors, was not in the refused list, so it stays accepted.
- Tools whose `period` is `enum: ["latest"]` are unchanged.

## 4. Guardrails

- **Neutrality CI test** (Phase 6 exit): no `doctrine` term in any public schema, field, enum or description, in both listings; none of the removed field names in any output schema.
- **Evidence hashes:** removing fields from `data` changes `evidenceCutId` for the affected tools. Expected; say so in the docs section.
- **Legacy envelope** (`MCP_ENVELOPE_V2=false`) loses the same fields.
- **Docs:** a "Breaking Changes (3.0.0)" section in `docs/provider-runtime-guidance.md` with a field-by-field table, and the README semver note.
- **Tests:** flip every case in `cases_neutrality.py` and `cases_notices_2270.py`; new cases for each F20 site and for F59/F60. Full `ci.yml` list locally.
- **Rollback:** redeploy 2.27.0.

## 5. Suggested split

One major release, built in this order inside one PR (each step keeps the harness green):
1. Pure deletions (section 2).
2. F20.
3. F59.
4. F60 part 3.
5. Neutrality CI test, docs, version 3.0.0.

Splitting steps 2 to 4 into a later release would make that release breaking too (a 4.0.0), so one PR is preferred even if it is large.
