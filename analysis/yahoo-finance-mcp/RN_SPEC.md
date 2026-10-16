# RN: neutrality deprecation release (2.8.0) — implementation spec

Owner decisions: Q14 (N1–N3, with deprecation), Q15 and Q15a names. Inventory: `RN_INVENTORY.md`. This release is additive. Every old field keeps emitting **identical values** for at least one minor version and is removed in Phase 6, in a major release. Both runtimes change alike, and the Worker goes first.

## 0. Deprecation notice (shared mechanism)

- **Where:** a warning in the **envelope** `meta.warnings`, never inside `data`. Evidence payloads are hashed into `evidenceCutId`, and `meta` is outside the hash.
- **Shape:** one warning per (tool, replacement):
  ```json
  {"code": "DEPRECATED_FIELD", "severity": "info",
   "fields": ["bracket", "inferredTag"], "replacement": "priceAtOrAboveTarget",
   "message": "bracket, inferredTag are deprecated and will be removed in the next major version; use priceAtOrAboveTarget and referenceTargetPct."}
  ```
  Match the existing warning-object conventions in each envelope for key order and whether `severity` is used. Use the same text in both runtimes.
- **Emission:** only on success responses of the affected tools, and only when the deprecated field is present in the payload.
- **Legacy envelope (`MCP_ENVELOPE_V2=false`):** emits no notice. The docs carry the mapping.

## 1. N1: `reliabilityClass` replaces `doctrineUse`

- **Mapping** (one shared constant per runtime):

  | `doctrineUse` | `reliabilityClass` |
  |---|---|
  | ALLOWED | RELIABLE |
  | VERIFY_ONLY | VERIFY_BEFORE_USE |
  | DIAGNOSTICS_ONLY | DIAGNOSTIC |
  | BLOCKED | UNRELIABLE |

- **Worker `meta`:**
  - Wherever `TOOL_DOCTRINE_STATUS` puts `doctrineUse` into `meta`, also add `reliabilityClass`. Today that covers `get_sec_filing_section_markdown`, `get_company_press_releases` and `extract_sec_filing_fact`.
  - Add `reliabilityClass?: string` to `ToolMeta` in `response.ts`.
- **Worker `data`:** next to the inline literal `doctrineUse: "ALLOWED"` (press releases) and `doctrineUse: "BLOCKED"` (section markdown), add `reliabilityClass` with the mapped value.
- **Python:**
  - Add `reliabilityClass` to `meta` for the same three tools (`ToolMeta` and the success path).
  - Add `reliabilityClass` to the data payload of `get_company_press_releases` ("RELIABLE") and `get_sec_filing_section_markdown` ("UNRELIABLE").
  - Python does **not** start emitting `doctrineUse`; it never did.
- **Deprecation notice:** a `DEPRECATED_FIELD` warning (fields `["doctrineUse"]`, replacement `reliabilityClass`), on the Worker only, wherever it emits `doctrineUse`.
- **Out of scope:** the other Python/Worker section-markdown differences (status, `decisionGrade`, warning). Record them as a FINDING parity case.

## 2. N2: the seven always-null boundary fields

- **Fields:** `g2`, `opportunity`, `action`, `priceTarget`, `scenarioWeights`, `selectedMethod`, `selectedMultiple`.
- **Payload:** these fields keep emitting null, and `AUTHORITY_BOUNDARY` is unchanged. Hashes and cut documents must stay byte-identical.
- **Replacement:** `decisionUse: "EVIDENCE_ONLY"`, which is already emitted.
- **Notice:** a `DEPRECATED_FIELD` warning in envelope `meta.warnings` for each response of an `EVIDENCE_ONLY_ACTIONS` action.
  - Fields: the seven names. Replacement: `decisionUse`.
  - Message: "…are always null and will be removed in the next major version; decisionUse=EVIDENCE_ONLY marks the evidence boundary."
  - Add it at the same dispatch point that applies `with_authority_boundary` / `withAuthorityBoundary`, or in the envelope builder for those actions. Pick whichever keeps the data payload untouched.

## 3. N3: strategy labels become raw measures and caller parameters

### 3a. `calculate_price_target_distance`
- **New field:** `priceAtOrAboveTarget` (boolean or null) = `currentPrice >= referenceTargetPrice`, computed from the **raw** prices, not the rounded percentage. It is null when either price is missing or not positive.
- **Deprecated fields:** `bracket` and `inferredTag`, plus the already-deprecated `tag` and `tagNote`. They keep their values. Notice replacement: `priceAtOrAboveTarget` (message also points to `referenceTargetPct`).
- **Schemas:** add the field and mark the old ones "Deprecated: …" in their descriptions, in both runtimes.
- **Worker schema bug:** `OUTPUT_SCHEMAS` has no entry for the public names `calculate_price_target_distance` and `analyze_position_signals`, so the Worker serves `SIMPLE_OBJECT_SCHEMA`. Alias them to the existing schemas, as the volume-gate alias does (`tools.ts` around line 1405).

### 3b. `check_volume_liquidity_threshold` and `get_market_snapshot`
- **New input:** optional `min_adv_usd` (number, > 0) on both tools. Add it to the input schema and the grouped/dispatch argument passing in both runtimes.
- **New output, `meetsMinimum`** (boolean or null):

  | Condition | `meetsMinimum` |
  |---|---|
  | `min_adv_usd` given and `adv20dTradedValueUsd` known | `adv20dTradedValueUsd >= min_adv_usd` |
  | `min_adv_usd` given, ADV unknown | null |
  | `min_adv_usd` not given | null |

  Also echo `minAdvUsd` (the caller's value, or null).
  - `check_volume_liquidity_threshold` gets both at top level.
  - `get_market_snapshot` gets them inside `volume`, next to `liquidityGatePass`, as `volume.meetsMinimum` and `volume.minAdvUsd`.
- **Bad input:** a non-numeric or ≤ 0 `min_adv_usd` returns INPUT_VALIDATION_ERROR with the same message in both runtimes.
- **Deprecated fields:** `gatePass`, `gateThresholdUsd` and the snapshot's `volume.liquidityGatePass`.
  - Values stay identical.
  - Notice replacement: `meetsMinimum` (message: "pass min_adv_usd for a caller-chosen threshold").
  - `adv20dTradedValueUsd` and `gateBasis` stay: they describe the measure.
  - `note` stays byte-identical in this window.

### 3c. `analyze_position_signals`
- **New groups**, each holding values **identical** to its source group (the same object contents, including today's runtime asymmetries):

  | New group | Source group |
  |---|---|
  | `analystInputs` | `t1_inputs` |
  | `pricePositionInputs` | `t2_inputs` |
  | `earningsInputs` | `t4_inputs` |
  | `technicalInputs` | `t5_inputs` |

- **Deprecated groups:** the four `t*_inputs` groups. The notice is one warning listing the four groups, with replacement "analystInputs, pricePositionInputs, earningsInputs, technicalInputs". If a per-replacement shape is cleaner, use four warnings, but decide once and use it in both runtimes.
- **Schemas:** update both runtimes.
- **Descriptions:** the tests in `test_source_discovery_schema.py` forbid `T1`–`T5`, "doctrine" and EQF in public descriptions. Keep them passing.

## 4. Tests and harness

- **Pinned cases to update:** the existing parity cases for the volume gate (BR-PRC-003) and the bracket (BR-PRC-022) keep passing. Extend their selects to the new fields with explicit expected values in both runtimes.
- **New parity cases:**
  - `reliabilityClass` in `meta` and `data` for the three tools;
  - `DEPRECATED_FIELD` notices for each tool family;
  - `priceAtOrAboveTarget` (below, equal to and above the target, and missing prices);
  - `meetsMinimum` (above, below, equal, ADV unknown, no input, bad input);
  - snapshot `volume.meetsMinimum`;
  - the four new groups equal to their `t*` twins;
  - the `analyze_position_signals` runtime asymmetries as `FINDING` pinned cases;
  - an evidence action showing the payload unchanged and the `meta.warnings` notice present.
- **Frozen contract:** `scripts/public_tool_contract.py` and the other tests in `RN_INVENTORY.md` gain the new fields. Old fields stay required.
- **Must pass:** `scripts/check_tool_sync.py`, `cd worker && npx tsc --noEmit`, `python scripts/test_parity_harness.py`, and the acceptance scripts listed in `ci.yml`.

## 5. Release

- **Version:** 2.8.0 (minor).
- **Docs:** a section in `docs/provider-runtime-guidance.md` with the field-by-field mapping table and the removal plan (Phase 6, major version). Downstream consumers move within the window.
