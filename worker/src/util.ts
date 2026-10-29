/**
 * Generic helpers shared by the tool domains (Phase 4 unit 5, moved out of yahoo-finance.ts): string and integer
 * coercion, object-only JSON parsing, short excerpts, ISO timestamps and trading-day dates. Imports nothing.
 * Python twin: yfmcp/util.py.
 */

/**
 * Returns the last trading date as a YYYY-MM-DD string.
 * If a unix-seconds timestamps array is provided, uses the last element.
 * Falls back to the last weekday from the UTC system clock.
 * Note: does not account for market holidays — weekday fallback only.
 */
export function getLastTradingDate(timestamps?: number[]): string {
  if (timestamps && timestamps.length > 0) {
    return new Date(timestamps[timestamps.length - 1] * 1000).toISOString().slice(0, 10);
  }
  const d = new Date();
  let safety = 0;
  while ((d.getUTCDay() === 0 || d.getUTCDay() === 6) && safety++ < 7) {
    d.setUTCDate(d.getUTCDate() - 1);
  }
  return d.toISOString().slice(0, 10);
}

export function dailyBarDate(timestamp: number | null, timezone: string): string | null {
  if (timestamp == null) return null;
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(new Date(timestamp * 1000));
    const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return values.year && values.month && values.day
      ? `${values.year}-${values.month}-${values.day}`
      : null;
  } catch {
    return new Date(timestamp * 1000).toISOString().slice(0, 10);
  }
}

export const _str = (v: unknown, fallback = ""): string => (typeof v === "string" ? v : fallback);

export function clampInt(value: number, fallback: number, min: number, max: number): number {
  const n = Number.isFinite(value) ? Math.trunc(value) : fallback;
  return Math.min(max, Math.max(min, n));
}

export function parseObjectJson(raw: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(raw) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

export function compactExcerpt(text: unknown, maxLen = 240): string {
  const cleaned = String(text ?? "").replace(/\s+/g, " ").trim();
  return cleaned.length <= maxLen ? cleaned : `${cleaned.slice(0, maxLen).trimEnd()}...`;
}

export function toIsoUtc(ts: unknown): string | null {
  if (typeof ts !== "string" || !ts.trim()) return null;
  const s = ts.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return `${s}T00:00:00Z`;
  const d = new Date(s);
  return Number.isFinite(d.getTime()) ? d.toISOString() : null;
}

/** The same month and day `years` later (negative: earlier); 29 February becomes 28 February in a common year, as
 * yfmcp.util._add_years does. */
export function addIsoYears(isoDate: string, years: number): string {
  const y = parseInt(isoDate.slice(0, 4), 10) + years;
  const leap = y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0);
  const mmdd = isoDate.slice(5, 10) === "02-29" && !leap ? "02-28" : isoDate.slice(5, 10);
  return `${String(y).padStart(4, "0")}-${mmdd}`;
}

/**
 * Round half away from zero to `digits` decimals, decimal-safe (2.17.0): the number's shortest decimal form is shifted,
 * so 1.005 rounds to 1.01 and -0.125 to -0.13, as Python's Decimal(str(x)).quantize(..., ROUND_HALF_UP) does.
 * Moved from news/shared.ts (R5-pricing-1) so pricing uses the same display rounding.
 */
export function roundHalfAwayFromZero(value: number, digits: number): number {
  // Past 2^52 at this scale a double has no fraction left to round.
  if (!Number.isFinite(value) || Math.abs(value) * 10 ** digits >= 2 ** 52) return value;
  const [mantissa, exponent] = Math.abs(value).toExponential().split("e");
  const shifted = Math.round(Number(`${mantissa}e${Number(exponent) + digits}`));
  const rounded = shifted / 10 ** digits;
  return value < 0 && rounded !== 0 ? -rounded : rounded;
}
