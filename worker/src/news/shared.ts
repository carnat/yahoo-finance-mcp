/**
 * Helpers shared by the news providers and the news pipeline (news.ts): ISO and text cutting, keyword
 * event typing, duplicate-group identity, date windows, XML text and exchange normalisation.
 * Moved out of news.ts in 2.15.1 (pure restructure); providers never import news.ts.
 */

import { stripHtmlTags, _str } from "../yahoo-finance.js";

/** Parse a single HTML <table> into a list of rows (each row is a list of plain-text cells). */
export function parseHtmlTable(tableHtml: string): string[][] {
  const rows: string[][] = [];
  const trRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
  let trMatch: RegExpExecArray | null;
  while ((trMatch = trRe.exec(tableHtml)) !== null) {
    const row: string[] = [];
    let tdMatch: RegExpExecArray | null;
    while ((tdMatch = tdRe.exec(trMatch[1])) !== null) {
      row.push(stripHtmlTags(tdMatch[1]));
    }
    if (row.length > 0) rows.push(row);
  }
  return rows;
}
export function sourceDecisionUse(sourceType: unknown): string {
  const src = _str(sourceType);
  if (src === "sec_ex99_found") return "USE_OFFICIAL_EVIDENCE";
  if (["sec_filing", "company_ir", "company_ir_page", "press_release", "yahoo_finance_press_releases", "newswire", "marketaux_wire"].includes(src)) {
    return "CHECK_OFFICIAL_RELEASES";
  }
  return "CONTEXT_ONLY";
}
/** An ISO 8601 UTC string without ".000" when the milliseconds are zero, as Python's isoformat writes it (2.17.0). */
export function withoutZeroMillis(value: string): string {
  return value.replace(/\.000Z$/, "Z");
}
/** The news ISO formatter (2.17.0): `Date.toISOString()` with zero milliseconds dropped ("...:00Z", not "...:00.000Z"). */
export function isoUtc(date: Date): string {
  return withoutZeroMillis(date.toISOString());
}
/** Epoch seconds as a news ISO timestamp (isoUtc). */
export function isoFromEpochSeconds(ts: number): string {
  return isoUtc(new Date(ts * 1000));
}
export function normalizeIso(raw: unknown): string | null {
  if (raw == null) return null;
  if (typeof raw === "number" && Number.isFinite(raw)) return isoFromEpochSeconds(raw);
  if (typeof raw !== "string") return null;
  const v = raw.trim();
  if (!v) return null;
  if (/^\d{14}$/.test(v)) return `${v.slice(0, 4)}-${v.slice(4, 6)}-${v.slice(6, 8)}T${v.slice(8, 10)}:${v.slice(10, 12)}:${v.slice(12, 14)}Z`;
  if (/^\d{8}$/.test(v)) return `${v.slice(0, 4)}-${v.slice(4, 6)}-${v.slice(6, 8)}T00:00:00Z`;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : isoUtc(d);
}
export function shortText(value: unknown, maxLen = 220): string | null {
  const s = _str(value).replace(/\s+/g, " ").trim();
  // Cut at code points, as Python does, so an astral character is never split into a lone surrogate (2.17.0).
  return s ? Array.from(s).slice(0, maxLen).join("") : null;
}
/**
 * Event-type keyword table, shared with Python (yfmcp/news/shared.py): Python's list, the superset (2.17.0).
 * Groups are tested in order and the first group with a whole-word match (optional s/es/d/ed/ing suffix) wins.
 */
const EVENT_TYPE_KEYWORDS: Array<[string, string[]]> = [
  ["earnings", ["earnings", "eps", "quarterly", "10-q", "10-k", "annual report"]],
  ["guidance", ["guidance", "outlook", "forecast", "reaffirm", "raises", "lowers"]],
  ["contract", ["contract", "agreement", "deal", "partnership"]],
  ["financing", ["offering", "financing", "debt", "credit facility", "note"]],
  ["product", ["launch", "product", "introduce", "announces new"]],
  ["analyst", ["analyst", "rating", "upgrade", "downgrade", "price target"]],
  ["macro", ["macro", "inflation", "rates", "fomc", "cpi"]],
  ["litigation", ["lawsuit", "litigation", "court", "settlement"]],
  ["insider", ["insider", "director", "officer", "form 4"]],
  ["regulatory", ["sec", "regulatory", "8-k", "10-q", "10-k", "filing"]],
];
const EVENT_TYPE_PATTERNS: Array<[string, RegExp]> = EVENT_TYPE_KEYWORDS.map(([eventType, keywords]) => [
  eventType,
  // Whole words with an optional inflection on the last word (planner decision 1, 2.17.0): 'launches', 'notes' and
  // 'announced' match; 'sector', 'dealer' and 'steps' do not hit 'sec', 'deal' and 'eps'.
  new RegExp(`\\b(?:${keywords.map(k => k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|")})(?:s|es|d|ed|ing)?\\b`),
]);
export function eventTypeFromKeywords(title: string, summary: string): string {
  const text = `${title} ${summary}`.toLowerCase();
  for (const [eventType, pattern] of EVENT_TYPE_PATTERNS) {
    if (pattern.test(text)) return eventType;
  }
  return "other";
}
/**
 * Whether the ticker appears in the text (case-insensitive). A single-letter ticker must be a whole token, so 'A' is
 * not found inside 'MARKETS' (2.17.0); longer tickers keep the substring test.
 */
export function tickerInText(text: string, ticker: string): boolean {
  const tickerU = ticker.toUpperCase();
  const textU = text.toUpperCase();
  if (tickerU.length === 1) return new RegExp(`\\b${tickerU.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`).test(textU);
  return textU.includes(tickerU);
}
function normalizedEventTitleStem(ticker: string, title: string | null): string {
  const stop = new Set(["yahoo", "finance", "finnhub", "globenewswire", "press", "release", "inc", "corp", "corporation", "company", "plc", "ltd", "llc"]);
  const tickerLower = ticker.toLowerCase();
  // HTML entities are decoded before the stem, so "&amp;" and "&" titles group (2.17.0).
  return decodeXmlText(_str(title))
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter((word) => word && word !== tickerLower && !stop.has(word))
    .join(" ")
    .slice(0, 80);
}
export function makeDupGroupId(ticker: string, title: string | null, publishedAt: string | null, issuer: string | null): string | null {
  const normTitle = normalizedEventTitleStem(ticker, title);
  const day = _str(publishedAt).slice(0, 10);
  const entity = (_str(issuer) || ticker).toUpperCase();
  if (!normTitle && !day) return null;
  let h = 0;
  const s = `${normTitle}|${day}|${entity}`;
  for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  return (h >>> 0).toString(16).padStart(8, "0");
}
/**
 * Round half away from zero to `digits` decimals, decimal-safe (2.17.0): the number's shortest decimal form is shifted,
 * so 1.005 rounds to 1.01 and -0.125 to -0.13, as Python's Decimal(str(x)).quantize(..., ROUND_HALF_UP) does.
 */
export function roundHalfAwayFromZero(value: number, digits: number): number {
  // Past 2^52 at this scale a double has no fraction left to round.
  if (!Number.isFinite(value) || Math.abs(value) * 10 ** digits >= 2 ** 52) return value;
  const [mantissa, exponent] = Math.abs(value).toExponential().split("e");
  const shifted = Math.round(Number(`${mantissa}e${Number(exponent) + digits}`));
  const rounded = shifted / 10 ** digits;
  return value < 0 && rounded !== 0 ? -rounded : rounded;
}
export function withinDateWindow(publishedAt: string | null, startDate = "", endDate = "", lookbackDays?: number): boolean {
  if (!publishedAt) return false;
  const day = publishedAt.slice(0, 10);
  if (startDate && day < startDate) return false;
  if (endDate && day > endDate) return false;
  if (lookbackDays != null) {
    const cutoff = new Date(Date.now() - lookbackDays * 86400000).toISOString().slice(0, 10);
    if (day < cutoff) return false;
  }
  return true;
}
export function decodeXmlText(value: string): string {
  return value
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/gi, "$1")
    .replace(/&(?:amp|#38);/gi, "&")
    .replace(/&(?:lt|#60);/gi, "<")
    .replace(/&(?:gt|#62);/gi, ">")
    .replace(/&(?:quot|#34);/gi, "\"")
    .replace(/&(?:apos|#39);/gi, "'")
    .replace(/&#(\d+);/g, (_m, code) => String.fromCharCode(Number(code)));
}
export function extractXmlTag(xml: string, tagName: string): string {
  const escaped = tagName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const m = xml.match(new RegExp(`<${escaped}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${escaped}>`, "i"));
  return m ? decodeXmlText(m[1]).trim() : "";
}
/**
 * The exchange normaliser shared by every news exchange check (Yahoo ticker prefixes, Marketaux entities, GlobeNewswire
 * stock categories) and by Python (2.17.0): NYSE American / AMEX / exact ASE are tested before NYSE, and GS / NMS / NGM /
 * NCM count only as whole tokens or exact codes.
 */
const NASDAQ_EXCHANGE_CODES = new Set(["GS", "NMS", "NGM", "NCM"]);
export function normalizeExchange(exch: string | null | undefined): string | null {
  if (!exch) return null;
  const e = exch.toUpperCase();
  const tokens = e.split(/[^A-Z0-9]+/).filter(Boolean);
  if (e.includes("NASDAQ") || tokens.some(token => NASDAQ_EXCHANGE_CODES.has(token))) return "NASDAQ";
  if (e.includes("NYSE AMERICAN") || e.includes("AMEX") || e === "ASE") return "NYSEAMERICAN";
  if (e.includes("NYSE") || e === "NYQ") return "NYSE";
  if (e.includes("TSXV") || e.includes("CVE")) return "TSXV";
  if (e.includes("TSX") || e.includes("TOR")) return "TSX";
  return null;
}
