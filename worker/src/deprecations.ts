// Field deprecations. Each deprecated field keeps emitting its old value; a
// DEPRECATED_FIELD notice in the envelope's meta.warnings names its
// replacement. The notice never goes into `data`: evidence payloads are hashed
// into evidenceCutId and meta sits outside that hash. Python mirrors these
// texts verbatim. The 2.8.0 and 2.27.0 deprecations were removed in 3.0.0;
// see docs/provider-runtime-guidance.md "Breaking Changes (3.0.0)".
//
// This module stays import-free, like response.ts.

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

/** F20 (2.27.0): status strings carried in confidence. */
export const STATUS_CONFIDENCE_DEPRECATION = deprecatedFieldWarning(
  ["confidence"],
  "status",
  "confidence values other than HIGH, MEDIUM and LOW (status strings such as EXTRACTION_FAILED or NOT_DISCLOSED) are deprecated "
    + `and ${REMOVAL}; confidence will hold only HIGH, MEDIUM or LOW, and a status field names the limitation.`,
);

const CONFIDENCE_LEVELS = new Set(["HIGH", "MEDIUM", "LOW"]);

type Rec = Record<string, unknown>;

/** Whether any `confidence` in the payload holds a string other than HIGH, MEDIUM or LOW (F20). */
export function payloadHasStatusConfidence(data: unknown, depth = 0): boolean {
  if (depth > 8 || data == null || typeof data !== "object") return false;
  if (Array.isArray(data)) return data.some((item) => payloadHasStatusConfidence(item, depth + 1));
  for (const [key, value] of Object.entries(data as Rec)) {
    if (key === "confidence" && typeof value === "string" && !CONFIDENCE_LEVELS.has(value)) return true;
    if (value != null && typeof value === "object" && payloadHasStatusConfidence(value, depth + 1)) return true;
  }
  return false;
}

/** The DEPRECATED_FIELD notices for one successful tool response. */
export function deprecationWarningsFor(_tool: string, data: unknown): DeprecatedFieldWarning[] {
  const out: DeprecatedFieldWarning[] = [];
  if (payloadHasStatusConfidence(data)) out.push({ ...STATUS_CONFIDENCE_DEPRECATION, fields: [...STATUS_CONFIDENCE_DEPRECATION.fields] });
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
