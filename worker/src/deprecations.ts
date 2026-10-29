// The caller-chosen liquidity minimum (2.8.0, N3b). The 2.8.0 and 2.27.0
// field deprecations and their DEPRECATED_FIELD notices were removed in
// 3.0.0; see docs/provider-runtime-guidance.md "Breaking Changes (3.0.0)".
//
// This module stays import-free, like response.ts.

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
