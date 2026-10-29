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
