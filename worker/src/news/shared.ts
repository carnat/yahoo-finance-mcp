/**
 * Helpers shared by the news providers and the news pipeline (news.ts): ISO and text cutting, keyword
 * event typing, duplicate-group identity, date windows, XML text and exchange normalisation.
 * Moved out of news.ts in 2.15.1 (pure restructure); providers never import news.ts.
 */

import { iso, stripHtmlTags, _str } from "../yahoo-finance.js";

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
export function normalizeIso(raw: unknown): string | null {
  if (raw == null) return null;
  if (typeof raw === "number" && Number.isFinite(raw)) return iso(raw);
  if (typeof raw !== "string") return null;
  const v = raw.trim();
  if (!v) return null;
  if (/^\d{14}$/.test(v)) return `${v.slice(0, 4)}-${v.slice(4, 6)}-${v.slice(6, 8)}T${v.slice(8, 10)}:${v.slice(10, 12)}:${v.slice(12, 14)}Z`;
  if (/^\d{8}$/.test(v)) return `${v.slice(0, 4)}-${v.slice(4, 6)}-${v.slice(6, 8)}T00:00:00Z`;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}
export function shortText(value: unknown, maxLen = 220): string | null {
  const s = _str(value).replace(/\s+/g, " ").trim();
  return s ? s.slice(0, maxLen) : null;
}
export function eventTypeFromKeywords(title: string, summary: string): string {
  const text = `${title} ${summary}`.toLowerCase();
  if (/earnings|eps|quarterly|10-q|10-k/.test(text)) return "earnings";
  if (/guidance|outlook|forecast|reaffirm|raises|lowers/.test(text)) return "guidance";
  if (/contract|agreement|deal|partnership/.test(text)) return "contract";
  if (/offering|financing|debt|notes?/.test(text)) return "financing";
  if (/launch|product|introduce/.test(text)) return "product";
  if (/analyst|rating|upgrade|downgrade|price target/.test(text)) return "analyst";
  if (/macro|inflation|rates|fomc|cpi/.test(text)) return "macro";
  if (/lawsuit|litigation|court|settlement/.test(text)) return "litigation";
  if (/insider|director|officer|form 4/.test(text)) return "insider";
  if (/sec|regulatory|8-k|filing/.test(text)) return "regulatory";
  return "other";
}
function normalizedEventTitleStem(ticker: string, title: string | null): string {
  const stop = new Set(["yahoo", "finance", "finnhub", "globenewswire", "press", "release", "inc", "corp", "corporation", "company", "plc", "ltd", "llc"]);
  const tickerLower = ticker.toLowerCase();
  return _str(title)
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
export function normalizeExchange(exch: string | null | undefined): string | null {
  if (!exch) return null;
  const e = exch.toUpperCase();
  if (e.includes("NASDAQ") || e.includes("NMS") || e.includes("NGM") || e.includes("NCM") || e.includes("GS")) return "NASDAQ";
  if (e.includes("NYSE") || e === "NYQ") return "NYSE";
  if (e.includes("AMEX") || e.includes("NYSE AMERICAN") || e === "ASE") return "NYSEAMERICAN";
  if (e.includes("TSXV") || e.includes("CVE")) return "TSXV";
  if (e.includes("TSX") || e.includes("TOR")) return "TSX";
  return null;
}
