// Neutral replacement labels (3.11.0, neutrality audit NL1-NL7). Each label is a
// factual classification or a band with a documented threshold the caller can
// override; the labels they replace are deprecated and removed in 4.0.0. Python
// mirrors these rules in yfmcp/labels.py.
//
// This module stays import-free, like deprecations.ts.

/** NL1: price against both moving averages. ABOVE means at or above (pct >= 0), as regime50/regime200. */
export type MaAlignment = "ABOVE_BOTH" | "BELOW_BOTH" | "MIXED";

export function maAlignment(regime50: string | null, regime200: string | null): MaAlignment | null {
  if (regime50 == null || regime200 == null) return null;
  if (regime50 === "ABOVE" && regime200 === "ABOVE") return "ABOVE_BOTH";
  if (regime50 === "BELOW" && regime200 === "BELOW") return "BELOW_BOTH";
  return "MIXED";
}

/** NL3: the default revision band, in percent (the band forwardRevisionSignal has always used). */
export const DEFAULT_REVISION_THRESHOLD_PCT = 3;

export type RevisionBand = "UP" | "FLAT" | "DOWN";

/**
 * NL3: the EPS revision band. 30d is primary; when 30d is inside the band, 7d confirms (driver 30d_neutral_7d);
 * with no 30d, 7d, then 90d as a fallback. A change of at least `thresholdPct` either way is UP or DOWN.
 * band is null when no revision is known (driver "none").
 */
export function revisionBand(
  revision30d: number | null,
  revision7d: number | null,
  revision90d: number | null,
  thresholdPct: number = DEFAULT_REVISION_THRESHOLD_PCT,
): { band: RevisionBand | null; driver: string } {
  const bandOf = (value: number): RevisionBand =>
    value <= -thresholdPct ? "DOWN" : value >= thresholdPct ? "UP" : "FLAT";
  if (revision30d != null) {
    const primary = bandOf(revision30d);
    if (primary !== "FLAT") return { band: primary, driver: "30d" };
    if (revision7d != null) {
      const confirm = bandOf(revision7d);
      if (confirm !== "FLAT") return { band: confirm, driver: "30d_neutral_7d" };
    }
    return { band: "FLAT", driver: "30d" };
  }
  if (revision7d != null) return { band: bandOf(revision7d), driver: "7d" };
  if (revision90d != null) return { band: bandOf(revision90d), driver: "90d_fallback" };
  return { band: null, driver: "none" };
}

/** NL3: beatRate (0-1) in four bands, lower bound inclusive. */
export type BeatRateBand = "75_TO_100PCT" | "55_TO_75PCT" | "40_TO_55PCT" | "0_TO_40PCT";

export function beatRateBand(beatRate: number | null): BeatRateBand | null {
  if (beatRate == null) return null;
  if (beatRate >= 0.75) return "75_TO_100PCT";
  if (beatRate >= 0.55) return "55_TO_75PCT";
  if (beatRate >= 0.4) return "40_TO_55PCT";
  return "0_TO_40PCT";
}

/** NL6: net debt / operational EBITDA in bands (upper bound inclusive above 1x), or NEGATIVE_EARNINGS_BASE for a loss-maker. */
export type LeverageBand = "BELOW_1X" | "1X_TO_2_5X" | "2_5X_TO_4X" | "ABOVE_4X" | "NEGATIVE_EARNINGS_BASE";

export function leverageBand(netDebtToEbitda: number | null, negativeEarningsBase: boolean): LeverageBand | null {
  if (netDebtToEbitda != null) {
    if (netDebtToEbitda < 1) return "BELOW_1X";
    if (netDebtToEbitda <= 2.5) return "1X_TO_2_5X";
    if (netDebtToEbitda <= 4) return "2_5X_TO_4X";
    return "ABOVE_4X";
  }
  return negativeEarningsBase ? "NEGATIVE_EARNINGS_BASE" : null;
}

/** NL6: the default credit-stress thresholds (the fixed ones creditStressFlag has always used). */
export const DEFAULT_MAX_NET_DEBT_TO_EBITDA = 2.5;
export const DEFAULT_MIN_INTEREST_COVERAGE = 3;

/** NL6: netDebtToEbitda above the maximum and EBIT interest coverage below the minimum; null when either is unknown. */
export function meetsCreditStressThreshold(
  netDebtToEbitda: number | null,
  interestCoverageEbit: number | null,
  maxNetDebtToEbitda: number = DEFAULT_MAX_NET_DEBT_TO_EBITDA,
  minInterestCoverage: number = DEFAULT_MIN_INTEREST_COVERAGE,
): boolean | null {
  if (netDebtToEbitda == null || interestCoverageEbit == null) return null;
  return netDebtToEbitda > maxNetDebtToEbitda && interestCoverageEbit < minInterestCoverage;
}

/** NL7: the mean consensus target against the current price. */
export type TargetVsPrice = "AT_OR_ABOVE" | "BELOW";

export function targetVsPrice(targetMean: number | null, lastPrice: number | null): TargetVsPrice | null {
  if (targetMean == null || lastPrice == null) return null;
  return targetMean >= lastPrice ? "AT_OR_ABOVE" : "BELOW";
}

/**
 * An optional positive-number argument: null when not supplied, the number when valid, or `{ error }` for a
 * non-numeric, non-finite or non-positive value (the min_adv_usd rule).
 */
export function parsePositiveNumberArg(name: string, value: unknown): number | null | { error: string } {
  if (value == null) return null;
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return { error: `${name} must be a positive number when supplied.` };
  }
  return value;
}
