// The caller-chosen liquidity minimum (2.8.0, N3b), and the 3.11.0 field
// deprecations (neutrality audit NL1-NL8, NL12-NL13). The 2.8.0 and 2.27.0
// deprecations were removed in 3.0.0; see docs/provider-runtime-guidance.md
// "Breaking Changes (3.0.0)".
//
// Each deprecated field keeps emitting its old value; a DEPRECATED_FIELD
// notice in the envelope's meta.warnings names its replacement. The notice
// never goes into `data`. Python mirrors these texts verbatim
// (yfmcp/deprecations.py).
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

function fieldList(fields: string[]): string {
  return `${fields.join(", ")} ${fields.length === 1 ? "is" : "are"}`;
}

function deprecatedFieldWarning(fields: string[], replacement: string, reason: string): DeprecatedFieldWarning {
  return {
    code: "DEPRECATED_FIELD",
    message: `${fieldList(fields)} deprecated and ${REMOVAL}; ${reason}`,
    severity: "info",
    fields,
    replacement,
  };
}

/** NL1: the moving-average label. */
export const MA_TREND_DEPRECATION = deprecatedFieldWarning(
  ["trend"], "maAlignment",
  "use maAlignment (ABOVE_BOTH, BELOW_BOTH or MIXED: the price against the 50- and 200-day averages).",
);
export const SNAPSHOT_MA_TREND_DEPRECATION = deprecatedFieldWarning(
  ["trend.maTrend"], "trend.maAlignment",
  "use trend.maAlignment (ABOVE_BOTH, BELOW_BOTH or MIXED).",
);
/** NL2: rating-change labels. */
export const RATING_LABEL_DEPRECATION = deprecatedFieldWarning(
  ["netSentiment", "changes[].strengthFlag"], "netUpgrades, changes[].signal, changes[].mixedSignal",
  "use netUpgrades (upgrades minus downgrades); each change's signal and mixedSignal carry what strengthFlag restated.",
);
/** NL3: earnings-momentum labels. */
const EARNINGS_LABELS = [
  "momentumFlag", "revisionDirection", "forwardRevisionSignal", "historicalSurpriseSignal",
  "compositeMomentumSignal", "interpretationNote",
];
export const EARNINGS_LABEL_DEPRECATION = deprecatedFieldWarning(
  EARNINGS_LABELS, "revisionBand, beatRateBand",
  "use revisionBand (UP, FLAT or DOWN at revisionThresholdPct, set with revision_threshold_pct) and beatRateBand, "
    + "with the revision and beatRate numbers.",
);
/** NL4: short-interest labels. */
export const SHORT_LABEL_DEPRECATION = deprecatedFieldWarning(
  ["squeezeRisk", "flag"], "shortPctFloat, daysToCover",
  "they have no replacement label; read shortPctFloat and daysToCover.",
);
/** NL5: the options-flow bracket. */
export const OPTIONS_BRACKET_DEPRECATION = deprecatedFieldWarning(
  ["bracket"], "pcRatio, putVolTrend",
  "read pcRatio and putVolTrend; formattedBlock will drop its Advisory segment.",
);
/** NL6: credit labels. */
export const CREDIT_LABEL_DEPRECATION = deprecatedFieldWarning(
  ["debtTier", "creditStressFlag"], "leverageBand, meetsCreditStressThreshold",
  "use leverageBand (the netDebtToEbitda band) and meetsCreditStressThreshold "
    + "(set with max_net_debt_to_ebitda and min_interest_coverage).",
);
/** NL7: the consensus-target label. */
export const TARGET_LAG_DEPRECATION = deprecatedFieldWarning(
  ["targetLagSignal"], "targetVsPrice",
  "use targetVsPrice (AT_OR_ABOVE or BELOW) with recentUpgradeCount30d.",
);
/** NL1-NL3: analyze_position_signals copies of those labels. */
export const POSITION_LABEL_DEPRECATION = deprecatedFieldWarning(
  ["analystInputs.analystNetSentiment", "earningsInputs.momentumFlag", "technicalInputs.maPosition"],
  "analystInputs.analystNetUpgrades, earningsInputs.revisionBand, technicalInputs.maAlignment",
  "use analystInputs.analystNetUpgrades, earningsInputs.revisionBand and technicalInputs.maAlignment.",
);
/** NL12: calculate_price_target_distance copies under workflow names. */
export const PRICE_TARGET_COPY_DEPRECATION = deprecatedFieldWarning(
  ["ioPt", "eqfPct", "invertedFlag"], "referenceTargetPrice, referenceTargetPct, priceAtOrAboveTarget",
  "use referenceTargetPrice, referenceTargetPct and priceAtOrAboveTarget.",
);
/** NL13: the hedge candidates' emoji IV flag. */
export const HEDGE_IV_FLAG_DEPRECATION = deprecatedFieldWarning(
  ["candidates[].ivFlag"], "candidates[].ivPctile",
  "read each candidate's ivPctile.",
);
/** NL8: the io_pt argument (a payload notice from the tool, since it depends on the arguments). */
export const IO_PT_ARGUMENT_DEPRECATION: DeprecatedFieldWarning = {
  code: "DEPRECATED_FIELD",
  message: `The io_pt argument is deprecated and ${REMOVAL}; pass reference_target_price.`,
  severity: "info",
  fields: ["io_pt"],
  replacement: "reference_target_price",
};

type Rec = Record<string, unknown>;

function isRecord(value: unknown): value is Rec {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

/** True when `path` resolves in `rec`; a `name[]` segment matches when any element of that array has the rest. */
function hasPath(rec: Rec, path: string): boolean {
  const [head, ...rest] = path.split(".");
  if (head.endsWith("[]")) {
    const list = rec[head.slice(0, -2)];
    return Array.isArray(list) && list.some((item) => isRecord(item) && rest.length > 0 && hasPath(item, rest.join(".")));
  }
  if (!Object.prototype.hasOwnProperty.call(rec, head)) return false;
  if (rest.length === 0) return true;
  const next = rec[head];
  return isRecord(next) && hasPath(next, rest.join("."));
}

/**
 * Whether any listed field is in the payload, at the top level or, for a batch
 * result, one level down (`{ AAPL: {...}, MSFT: {...} }`) or under `tickers`
 * (`get_market_snapshot`). Only keys are read, never values.
 */
function payloadHasAnyField(data: unknown, fields: string[]): boolean {
  if (!isRecord(data)) return false;
  const has = (rec: Rec) => fields.some((field) => hasPath(rec, field));
  if (has(data)) return true;
  const batch = isRecord(data.tickers) ? data.tickers : data;
  return Object.values(batch).some((entry) => isRecord(entry) && has(entry));
}

const NOTICES_BY_TOOL: Record<string, DeprecatedFieldWarning[]> = {
  analyze_moving_average_position: [MA_TREND_DEPRECATION],
  get_market_snapshot: [SNAPSHOT_MA_TREND_DEPRECATION],
  get_analyst_rating_changes: [RATING_LABEL_DEPRECATION],
  analyze_earnings_momentum: [EARNINGS_LABEL_DEPRECATION],
  get_short_momentum: [SHORT_LABEL_DEPRECATION],
  analyze_options_flow_window: [OPTIONS_BRACKET_DEPRECATION],
  analyze_credit_health: [CREDIT_LABEL_DEPRECATION],
  get_analyst_consensus: [TARGET_LAG_DEPRECATION],
  analyze_position_signals: [POSITION_LABEL_DEPRECATION],
  calculate_price_target_distance: [PRICE_TARGET_COPY_DEPRECATION],
  find_put_hedge_candidates: [HEDGE_IV_FLAG_DEPRECATION],
};

/** The DEPRECATED_FIELD notices for one successful tool response: one per notice whose fields the payload carries. */
export function deprecationWarningsFor(tool: string, data: unknown): DeprecatedFieldWarning[] {
  return (NOTICES_BY_TOOL[tool] ?? [])
    .filter((notice) => payloadHasAnyField(data, notice.fields))
    .map((notice) => ({ ...notice, fields: [...notice.fields] }));
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
