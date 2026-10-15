# RN inventory: doctrine-specific output fields (2026-10-15, HEAD 958b345)

This is a read-only survey for the RN neutrality-deprecation release (Q14, N1–N3). Line numbers are approximate, because `server.py` was being edited for R3 while the survey ran.

## Planner notes (decisions RN must make or respect)

- **N1 `doctrineUse` is Worker-only.**
  - Python emits it nowhere.
  - The Worker emits it twice for press releases and section markdown: once in `meta`, from the `TOOL_DOCTRINE_STATUS` table, and once in `data`, as an inline literal.
  - RN adds `reliabilityClass` to both runtimes, mapped as follows. That closes the gap.

    | `doctrineUse` | `reliabilityClass` |
    |---|---|
    | ALLOWED | RELIABLE |
    | VERIFY_ONLY | VERIFY_BEFORE_USE |
    | DIAGNOSTICS_ONLY | DIAGNOSTIC |
    | BLOCKED | UNRELIABLE |

  - Python section markdown also differs from the Worker. Python has no `SECTION_MARKDOWN_UNVERIFIED`, no `decisionGrade: false` and no warning. Decide whether RN aligns this or leaves it for later.
- **N2 boundary fields are hashed.**
  - `AUTHORITY_BOUNDARY` (`decisionUse: EVIDENCE_ONLY` plus the seven null fields) is merged at dispatch for 12 actions, and by 10 producers directly.
  - It is inside the canonical JSON of evidence cuts. Adding a `DEPRECATED_FIELD` notice inside the payload would therefore change every new `evidenceCutId`.
  - `decisionUse === "EVIDENCE_ONLY"` also switches off fact enrichment in both envelopes (`envelope.py:216`, `response.ts:158`), so it must stay.
- **N3 targets:**
  - `calculate_price_target_distance` (`bracket`, `inferredTag`).
  - `check_volume_liquidity_threshold` (fixed $10M `gatePass`).
  - `get_market_snapshot` (`volume.liquidityGatePass`).
  - `analyze_position_signals` (`t1/t2/t4/t5_inputs`).
- **Worker schema bug.** The output schemas for `calculate_price_target_distance` and `analyze_position_signals` are never attached to their public names (no `OUTPUT_SCHEMAS.<public>` alias), so the Worker serves `SIMPLE_OBJECT_SCHEMA`. Python attaches them. RN changes these schemas, so fix this in the same release.
- **`analyze_position_signals` runtime asymmetries** (not yet in the harness): `t5.macd` exists in Python only; the analyst-count source differs; `componentStatus` keys differ; `quoteTimestamp` exists in Python only; the Worker accepts batch tickers.
- **Deprecation precedent:**
  - Inline sibling notes exist, such as `tagNote` and the options-flow `fieldNotes`.
  - The `DEPRECATED_TOOL` error and `DEPRECATED_ALIAS` warning codes exist but are not emitted.
  - The policy in `docs/tool-naming.md:69-80` keeps a deprecation for at least one minor version and removes it only in a major version.
- **Tests that pin the old names, to update deliberately:**
  - `scripts/public_tool_contract.py`
  - `test_worker_doctrine_safety.py`
  - `test_data_quality.py`
  - `test_quote_calculations.py`
  - `test_universal_aliases.py`
  - `test_deployed_discovery.py`
  - `test_source_discovery_schema.py`
  - `test_evidence*.py`
  - parity `cases_pricing_evidence_tools.py` (volume gate around lines 265–396, bracket around 1232–1297)

## 1. doctrineUse / DoctrineToolStatus / TOOL_DOCTRINE_STATUS

### Worker

- **Type:** `DoctrineToolStatus` at `tools.ts:1731-1741`.
- **Table:** `TOOL_DOCTRINE_STATUS` at `tools.ts:1743-1774`:
  - `get_sec_filing_section_markdown`: BLOCKED
  - `get_company_press_releases`: ALLOWED
  - `extract_sec_filing_fact`: ALLOWED
- **Lookup:** `doctrineStatusFor()` at `tools.ts:1776`. Its result is spread into `meta` through `metaExtra` (`tools.ts:1934`, `1939`), on success only.
- **Typed field:** `response.ts:55` `doctrineUse?: string`.
- **Inline literals in `data`:**
  - `yahoo-finance.ts:12230` (press releases, ALLOWED)
  - `yahoo-finance.ts:13165` (section markdown, BLOCKED)
- **Unused values:** VERIFY_ONLY and DIAGNOSTICS_ONLY are never emitted.

### Python

Python has no emission, and `ToolMeta` (`envelope.py:52-60`) carries no doctrine fields.

### Tests

`test_worker_doctrine_safety.py`, at 71, 78, 206-207, 228, 243-244 and 254.

## 2. Seven null boundary fields and decisionUse EVIDENCE_ONLY

- **Constant:** `AUTHORITY_BOUNDARY` at `yfmcp/evidence.py:36-45` and `worker/src/evidence.ts:29-38`.
- **Action list:** `EVIDENCE_ONLY_ACTIONS` at `evidence.py:49-62` and `evidence.ts:43-56` (12 actions).
- **Dispatch wrapper:**
  - Python `with_authority_boundary` at `evidence.py:65-82`, called from `envelope.py:498-500`.
  - Worker `withAuthorityBoundary` at `evidence.ts:63-76`, called from `tools.ts:1880`.
- **Direct producers:**
  - consensus curve
  - EPS revisions
  - evidence quality
  - evidence cut document
  - share scenarios
  - metric reconciliation
  - historical valuation
  - driver ledger
  - funding schedule
  - guidance history
  - `get_historical_valuation_context` (`server.py` around 10678-10766, `yahoo-finance.ts` around 15706-15783)
- **Described in prose only:**
  - `server.py:12303` and `tools.ts:559`
  - `tool_catalog.json:40`
  - `yfmcp/app.py:224`
  - `README.md:142-143`
  - `docs/provider-runtime-guidance.md` (422-428, 648-649, 876-890)
- **Tests:**
  - `test_evidence.py`: 188-195, 325-326, 406-407, 592-593, 932-958
  - `test_evidence_tools.py`: 38, 184-185
  - `test_scenarios_and_schedule.py`: 301, 422-423
  - `test_guidance_and_drivers.py`: 516, 611
  - `test_valuation_history_and_reconcile.py`: 774, 1168
- **Same key, other values (keep as they are):** `decisionUse` values such as CONTEXT_ONLY, USE_OFFICIAL_EVIDENCE, CHECK_OFFICIAL_RELEASES, HEDGE_SCREEN_ONLY, MECHANICAL_NOT_CONSENSUS and COMPANY_DISCLOSED_NOT_FORECAST are a different use. So is the capital-structure `priceTarget` evidence object.

## 3. bracket / inferredTag (`calculate_price_target_distance`)

- **Producers:**
  - Python `get_price_target_bracket` at `server.py:7978`; thresholds around 8003-8012, output around 8056-8059.
  - Worker `getPriceTargetBracket` at `yahoo-finance.ts:7851`; thresholds 7869-7877, output 7894-7897.
- **Thresholds,** on `referenceTargetPct` = round(current/target × 100, 1):

  | Field | Rule |
  |---|---|
  | `bracket` | ≤75 STRONG_BUY, ≤90 ACCEPTABLE, ≤100 CAUTION, otherwise AVOID |
  | `inferredTag` | <40 SPECULATIVE, <80 LONG, <100 NEAR, otherwise INVERTED |
  | `invertedFlag` | pct ≥ 100 |

- **Schemas:**
  - Python `yfmcp/schemas.py:569-590`, alias at 811.
  - Worker `tools.ts:1150-1174`, never aliased (the bug above).
- **Tests:**
  - parity, around 1232-1297
  - `test_data_quality.py`: 822, 1067-1080
  - `test_universal_aliases.py:15`
  - `test_deployed_discovery.py:741`
  - `test_source_discovery_schema.py`: 166, 178-183
  - `public_tool_contract.py:30`
- **Different field, same name:** the options-flow `bracket` (UPPER, LOWER, MID).

## 4. Liquidity gate ($10M ADV20)

- **Constant:** `LIQUIDITY_GATE_MIN_ADV_USD`, at `yfmcp/tools/pricing.py:1441` and `yahoo-finance.ts:8036`.
- **Function:**
  - Python `get_volume_gate` at `pricing.py:1492` (gate 1627, note 1629-1633).
  - Worker `getVolumeGate` at `yahoo-finance.ts:8086` (gate 8218).
- **Output fields:**
  - `adv20dTradedValueUsd`
  - `gateBasis`
  - `gateThresholdUsd`
  - `gatePass`
  - `note`, which hard-codes "$10M"
- **Tools:**
  - `check_volume_liquidity_threshold`
  - `get_market_snapshot`, as `volume.liquidityGatePass` (Python `pricing.py` 1750, 1864, 1878, 1908; Worker `yahoo-finance.ts` 8295, 8407, 8421, 8451)
- **Inputs:** no `min_adv_usd` style input exists today. The inputs are `ticker` and `foreign_exchange`, and `foreign_exchange` is ignored.
- **Schemas:**
  - Python `schemas.py:604`, alias at 813.
  - Worker `tools.ts:1193`, alias at 1405.
- **Descriptions:** the Python description is thinner than the Worker's. `app.py:190` is stale.
- **Tests:**
  - parity, around 265-396
  - `test_quote_calculations.py`: 126, 236, 245-246, 287-296
  - `test_data_quality.py`: 184, 268, 434-468, 786-804
  - `test_manifest_diagnostics.py`: 310-374, 593
  - `public_tool_contract.py`: 32, 71-87, 263, 326
  - `test_universal_aliases.py:11`
  - `test_deployed_discovery.py:742`

## 5. t1_inputs / t2_inputs / t4_inputs / t5_inputs (`analyze_position_signals`)

- **Builders:**
  - Python `get_position_score_inputs` at `server.py:8070`, emitted at 8170-8173.
  - Worker `getPositionScoreInputs` at `yahoo-finance.ts:7909`, emitted at 7987-7990.
- **Schemas:**
  - Python `schemas.py:591-603`, alias at 810.
  - Worker `tools.ts:1176-1191`, never aliased.
- **Asymmetries:** listed in the planner notes above.
- **Tests:**
  - `test_data_quality.py`: 1028-1039
  - `test_universal_aliases.py:14`
  - `test_deployed_discovery.py:740`
  - `test_source_discovery_schema.py:149-176`, which forbids the terms T1–T5, "doctrine" and EQF in descriptions
- **Parity coverage:** none.

## 6. Deprecation mechanics in place today

- **Unused codes:**
  - The `DEPRECATED_TOOL` error code (`response.ts:81`, `envelope.py:45`) is never emitted.
  - The `DEPRECATED_ALIAS` warning is policy only, in `docs/tool-naming.md:69-80`.
- **Tool-level deprecation:**
  - Envelope `meta` carries `canonicalTool`, `deprecatedTool` and `useInstead`.
  - Worker `ToolDef` carries `deprecated`, `useInstead` and `deprecationReason`.
- **Field-level precedent:**
  - `tag` with `tagNote` ("Deprecated: … Use inferredTag.")
  - the options-flow `fieldNotes` map
  - No schema uses a `deprecated: true` convention.
