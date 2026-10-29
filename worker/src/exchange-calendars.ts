/**
 * Exchange holiday calendars (BR-PRC-002, 3.1.0). The closures live in exchange-calendars.json, which Python reads too
 * (yfmcp/exchange_calendars.py), so both runtimes name the same previous session. An exchange without a calendar, or a
 * date outside the covered years, has no known previous session.
 */
import exchangeCalendars from "./exchange-calendars.json";

interface ExchangeCalendar {
  exchanges: string[];
  firstYear: number;
  lastYear: number;
  closures: string[];
}

export const CALENDARS = exchangeCalendars.calendars as Record<string, ExchangeCalendar>;

const BY_EXCHANGE = new Map<string, ExchangeCalendar & { closureSet: Set<string> }>();
for (const calendar of Object.values(CALENDARS)) {
  for (const code of calendar.exchanges) BY_EXCHANGE.set(code, { ...calendar, closureSet: new Set(calendar.closures) });
}

/** The exchange session before an ISO date, or null when the exchange has no calendar or the date is not covered. */
export function previousExchangeSession(exchangeName: unknown, isoDate: string): string | null {
  const calendar = typeof exchangeName === "string" ? BY_EXCHANGE.get(exchangeName) : undefined;
  if (!calendar || !/^\d{4}-\d{2}-\d{2}$/.test(isoDate)) return null;
  const day = new Date(`${isoDate}T00:00:00Z`);
  // A date that does not exist (2026-02-30) rolls over in Date; Python rejects it, so reject it here too.
  if (Number.isNaN(day.getTime()) || day.toISOString().slice(0, 10) !== isoDate) return null;
  for (let i = 0; i < 14; i++) {
    day.setUTCDate(day.getUTCDate() - 1);
    const year = day.getUTCFullYear();
    if (year < calendar.firstYear || year > calendar.lastYear) return null;
    const iso = day.toISOString().slice(0, 10);
    const weekday = day.getUTCDay();
    if (weekday !== 0 && weekday !== 6 && !calendar.closureSet.has(iso)) return iso;
  }
  return null;
}
