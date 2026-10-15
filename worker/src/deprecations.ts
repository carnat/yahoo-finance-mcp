// Field deprecations (2.8.0, RN). Each deprecated field keeps emitting its
// old value; a DEPRECATED_FIELD notice in the envelope's meta.warnings names
// its replacement. The notice never goes into `data`: evidence payloads are
// hashed into evidenceCutId and meta sits outside that hash. The fields are
// removed in the next major version. Python mirrors these texts verbatim.
//
// This module stays import-free, like response.ts.

/** doctrineUse → reliabilityClass (N1). */
export const RELIABILITY_CLASS_BY_DOCTRINE_USE = {
  ALLOWED: "RELIABLE",
  VERIFY_ONLY: "VERIFY_BEFORE_USE",
  DIAGNOSTICS_ONLY: "DIAGNOSTIC",
  BLOCKED: "UNRELIABLE",
} as const;

export type DoctrineUse = keyof typeof RELIABILITY_CLASS_BY_DOCTRINE_USE;
export type ReliabilityClass = (typeof RELIABILITY_CLASS_BY_DOCTRINE_USE)[DoctrineUse];

export function reliabilityClassFor(doctrineUse: DoctrineUse): ReliabilityClass {
  return RELIABILITY_CLASS_BY_DOCTRINE_USE[doctrineUse];
}

export interface DeprecatedFieldWarning {
  code: "DEPRECATED_FIELD";
  message: string;
  severity: "info";
  fields: string[];
  replacement: string;
}

const REMOVAL = "will be removed in the next major version";

function deprecatedFieldWarning(fields: string[], replacement: string, message: string): DeprecatedFieldWarning {
  return { code: "DEPRECATED_FIELD", message, severity: "info", fields, replacement };
}

function fieldList(fields: string[]): string {
  return `${fields.join(", ")} ${fields.length === 1 ? "is" : "are"}`;
}

/** N1: doctrineUse (meta, and data for press releases and section markdown). */
export const DOCTRINE_USE_DEPRECATION = deprecatedFieldWarning(
  ["doctrineUse"],
  "reliabilityClass",
  `${fieldList(["doctrineUse"])} deprecated and ${REMOVAL}; use reliabilityClass.`,
);

/** N2: the seven always-null authority-boundary fields on evidence-only actions. */
const BOUNDARY_NULL_FIELDS = ["g2", "opportunity", "action", "priceTarget", "scenarioWeights", "selectedMethod", "selectedMultiple"];
export const AUTHORITY_BOUNDARY_DEPRECATION = deprecatedFieldWarning(
  BOUNDARY_NULL_FIELDS,
  "decisionUse",
  `${fieldList(BOUNDARY_NULL_FIELDS)} always null and ${REMOVAL}; decisionUse=EVIDENCE_ONLY marks the evidence boundary.`,
);

/** N3a: calculate_price_target_distance strategy labels. */
const PRICE_TARGET_LABEL_FIELDS = ["bracket", "inferredTag", "tag", "tagNote"];
export const PRICE_TARGET_LABEL_DEPRECATION = deprecatedFieldWarning(
  PRICE_TARGET_LABEL_FIELDS,
  "priceAtOrAboveTarget",
  `${fieldList(PRICE_TARGET_LABEL_FIELDS)} deprecated and ${REMOVAL}; use priceAtOrAboveTarget and referenceTargetPct.`,
);

/** N3b: the fixed $10M liquidity gate on check_volume_liquidity_threshold. */
const VOLUME_GATE_FIELDS = ["gatePass", "gateThresholdUsd"];
export const VOLUME_GATE_DEPRECATION = deprecatedFieldWarning(
  VOLUME_GATE_FIELDS,
  "meetsMinimum",
  `${fieldList(VOLUME_GATE_FIELDS)} deprecated and ${REMOVAL}; use meetsMinimum and pass min_adv_usd for a caller-chosen threshold.`,
);

/** N3b: the snapshot's copy of the fixed gate. */
const SNAPSHOT_GATE_FIELDS = ["volume.liquidityGatePass"];
export const SNAPSHOT_GATE_DEPRECATION = deprecatedFieldWarning(
  SNAPSHOT_GATE_FIELDS,
  "volume.meetsMinimum",
  `${fieldList(SNAPSHOT_GATE_FIELDS)} deprecated and ${REMOVAL}; use volume.meetsMinimum and pass min_adv_usd for a caller-chosen threshold.`,
);

/** N3c: analyze_position_signals input groups, one notice for all four. */
const POSITION_INPUT_GROUPS = ["t1_inputs", "t2_inputs", "t4_inputs", "t5_inputs"];
const POSITION_INPUT_REPLACEMENTS = "analystInputs, pricePositionInputs, earningsInputs, technicalInputs";
export const POSITION_INPUT_GROUP_DEPRECATION = deprecatedFieldWarning(
  POSITION_INPUT_GROUPS,
  POSITION_INPUT_REPLACEMENTS,
  `${fieldList(POSITION_INPUT_GROUPS)} deprecated and ${REMOVAL}; use ${POSITION_INPUT_REPLACEMENTS}.`,
);

type Rec = Record<string, unknown>;

function isRecord(value: unknown): value is Rec {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

/** True when `path` (dotted) resolves to an own key of `rec`. */
function hasPath(rec: Rec, path: string): boolean {
  let node: unknown = rec;
  for (const key of path.split(".")) {
    if (!isRecord(node) || !Object.prototype.hasOwnProperty.call(node, key)) return false;
    node = node[key];
  }
  return true;
}

function hasAnyField(rec: Rec, fields: string[]): boolean {
  return fields.some((field) => hasPath(rec, field));
}

/**
 * Whether any listed field is in the payload, either at the top level or, for
 * a batch result, one level down (`{ AAPL: {...}, MSFT: {...} }`) or under
 * `tickers` (`get_market_snapshot`). Only keys are read, never values.
 */
function payloadHasAnyField(data: unknown, fields: string[]): boolean {
  if (!isRecord(data)) return false;
  if (hasAnyField(data, fields)) return true;
  const batch = isRecord(data.tickers) ? data.tickers : data;
  return Object.values(batch).some((entry) => isRecord(entry) && hasAnyField(entry, fields));
}

/**
 * The DEPRECATED_FIELD notices for one successful tool response. `metaFields`
 * are the deprecated keys the envelope's meta carries for this call (from the
 * tool's status table), checked alongside `data`.
 */
export function deprecationWarningsFor(
  tool: string,
  data: unknown,
  opts: { evidenceOnly: boolean; metaFields: readonly string[] },
): DeprecatedFieldWarning[] {
  const out: DeprecatedFieldWarning[] = [];
  const notice = (warning: DeprecatedFieldWarning) =>
    out.push({ ...warning, fields: [...warning.fields] });
  if (opts.metaFields.includes("doctrineUse") || (isRecord(data) && "doctrineUse" in data)) {
    notice(DOCTRINE_USE_DEPRECATION);
  }
  if (opts.evidenceOnly && isRecord(data) && hasAnyField(data, BOUNDARY_NULL_FIELDS)) {
    notice(AUTHORITY_BOUNDARY_DEPRECATION);
  }
  switch (tool) {
    case "calculate_price_target_distance":
      if (payloadHasAnyField(data, PRICE_TARGET_LABEL_FIELDS)) notice(PRICE_TARGET_LABEL_DEPRECATION);
      break;
    case "check_volume_liquidity_threshold":
      if (payloadHasAnyField(data, VOLUME_GATE_FIELDS)) notice(VOLUME_GATE_DEPRECATION);
      break;
    case "get_market_snapshot":
      if (payloadHasAnyField(data, SNAPSHOT_GATE_FIELDS)) notice(SNAPSHOT_GATE_DEPRECATION);
      break;
    case "analyze_position_signals":
      if (payloadHasAnyField(data, POSITION_INPUT_GROUPS)) notice(POSITION_INPUT_GROUP_DEPRECATION);
      break;
  }
  return out;
}

/** meetsMinimum: ADV20 traded value against a caller-chosen floor; null when either is unknown. */
export function meetsMinimum(adv20dTradedValueUsd: number | null | undefined, minAdvUsd: number | null): boolean | null {
  if (minAdvUsd == null || adv20dTradedValueUsd == null) return null;
  return adv20dTradedValueUsd >= minAdvUsd;
}

export const MIN_ADV_USD_ERROR = "min_adv_usd must be a positive number when supplied.";

/**
 * The caller's min_adv_usd: null when not supplied, a number when valid, or
 * `{ error }` for a non-numeric or non-positive value.
 */
export function parseMinAdvUsd(value: unknown): number | null | { error: string } {
  if (value == null) return null;
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return { error: MIN_ADV_USD_ERROR };
  return value;
}
