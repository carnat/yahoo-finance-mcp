/**
 * Pricing notices (R5-pricing-2, 3.1.0; names from Q11). A tool payload carries its notices under the internal key
 * `_warnings`; the envelope moves them to meta.warnings (liftPayloadWarnings), and get_market_snapshot folds its
 * components' notices into its own data.warnings with the component name. Python: yfmcp/pricing_notices.py.
 */

export const ZERO_VOLUME_NO_RATIO = "ZERO_VOLUME_NO_RATIO";
export const INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY";
export const SESSION_CALENDAR_UNKNOWN = "SESSION_CALENDAR_UNKNOWN";

// The internal payload key and its lift live in response.ts, which node's strip-types tests import on its own.
import { PAYLOAD_WARNINGS_KEY } from "./response.js";
export { PAYLOAD_WARNINGS_KEY, liftPayloadWarnings } from "./response.js";

export type PricingNotice = { code: string; message: string; ticker?: string };

export function zeroVolumeNotice(ticker: string): PricingNotice {
  return {
    code: ZERO_VOLUME_NO_RATIO,
    message: "The latest completed session traded zero volume, so ratio10d, ratio90d and volumeFlag are null.",
    ticker,
  };
}

export function insufficientHistoryNotice(ticker: string, years: number): PricingNotice {
  return {
    code: INSUFFICIENT_HISTORY,
    message: `Price history starts after the start of the ${years}-year window, so cagr${years}y is null.`,
    ticker,
  };
}

export function sessionCalendarNotice(ticker: string): PricingNotice {
  return {
    code: SESSION_CALENDAR_UNKNOWN,
    message:
      "No holiday calendar covers this exchange and date, so the previous session could not be named and freshnessStatus is UNKNOWN.",
    ticker,
  };
}

/** Notices for a payload built from prepared daily history: SESSION_CALENDAR_UNKNOWN when its reason says so. */
export function freshnessNotices(ticker: string, freshnessReason: string | null | undefined): PricingNotice[] {
  return freshnessReason === SESSION_CALENDAR_UNKNOWN ? [sessionCalendarNotice(ticker)] : [];
}

/** Sets `_warnings` on a payload when there are notices; leaves the payload unchanged otherwise. */
export function withNotices<T extends Record<string, unknown>>(payload: T, notices: PricingNotice[]): T {
  if (notices.length > 0) (payload as Record<string, unknown>)[PAYLOAD_WARNINGS_KEY] = notices;
  return payload;
}

/** Notices without repeats (same code, ticker and message), in first-seen order. */
export function uniqueNotices(notices: PricingNotice[]): PricingNotice[] {
  const seen = new Set<string>();
  return notices.filter((notice) => {
    const key = JSON.stringify([notice.code, notice.ticker ?? null, notice.message]);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/** Removes `_warnings` from a payload and returns them (component payloads folded by the snapshot). */
export function takeNotices(payload: Record<string, unknown> | null | undefined): PricingNotice[] {
  if (!payload || typeof payload !== "object") return [];
  const notices = payload[PAYLOAD_WARNINGS_KEY];
  delete payload[PAYLOAD_WARNINGS_KEY];
  return Array.isArray(notices) ? (notices as PricingNotice[]) : [];
}
