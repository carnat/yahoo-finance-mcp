/**
 * GlobeNewswire provider: RSS feed fetch with the size guard, category matching and collectGlobeNewswireEvents.
 * Moved out of news.ts in 2.15.1 (pure restructure); providers never import news.ts.
 */

import { setBounded } from "../../cache.js";
import { providerFetch } from "../../request-context.js";
import { UA, stripHtmlTags, _str } from "../../yahoo-finance.js";
import {
  sourceDecisionUse,
  normalizeIso,
  shortText,
  eventTypeFromKeywords,
  makeDupGroupId,
  withinDateWindow,
  decodeXmlText,
  extractXmlTag,
} from "../shared.js";
import type { NewsProviderResult } from "./types.js";

const GLOBENEWSWIRE_RSS_FEEDS: Array<{ name: string; url: string }> = [
  {
    name: "public_companies",
    url: "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
  },
  {
    name: "press_releases",
    url: "https://www.globenewswire.com/RssFeed/subjectcode/72-Press%20Releases/feedTitle/GlobeNewswire%20-%20Press%20Releases",
  },
  {
    name: "earnings",
    url: "https://www.globenewswire.com/RssFeed/subjectcode/13-Earnings%20Releases%20And%20Operating%20Results/feedTitle/GlobeNewswire%20-%20Earnings%20Releases%20And%20Operating%20Results",
  },
  {
    name: "stock_market_news",
    url: "https://www.globenewswire.com/RssFeed/subjectcode/39-Stock%20Market%20News/feedTitle/GlobeNewswire%20-%20Stock%20Market%20News",
  },
  {
    name: "technology",
    url: "https://www.globenewswire.com/RssFeed/industry/9000-Technology/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Technology",
  },
  {
    name: "semiconductors",
    url: "https://www.globenewswire.com/RssFeed/industry/9576-Semiconductors/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Semiconductors",
  },
  {
    name: "telecommunications",
    url: "https://www.globenewswire.com/RssFeed/industry/6000-Telecommunications/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Telecommunications",
  },
  {
    name: "mobile_telecommunications",
    url: "https://www.globenewswire.com/RssFeed/industry/6575-Mobile%20Telecommunications/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Mobile%20Telecommunications",
  },
  {
    name: "telecommunications_equipment",
    url: "https://www.globenewswire.com/RssFeed/industry/9578-Telecommunications%20Equipment/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Telecommunications%20Equipment",
  },
  {
    name: "electronic_equipment",
    url: "https://www.globenewswire.com/RssFeed/industry/2737-Electronic%20Equipment/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Electronic%20Equipment",
  },
];
const GLOBENEWSWIRE_MAX_BYTES = 2 * 1024 * 1024;
const GLOBENEWSWIRE_TTL_MS = 15 * 60 * 1000;
const GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN = "https://www.globenewswire.com/rss/stock";
const GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN = "https://www.globenewswire.com/rss/ISIN";
const GLOBENEWSWIRE_CACHE_MAX = 40;
const globenewswireCache = new Map<string, { value: string; storedAt: number }>();
function normalizedExchangeCode(exchange: string | null | undefined): string | null {
  const e = _str(exchange).toUpperCase();
  if (!e) return null;
  const eUpper = e.toUpperCase();
  if (eUpper.includes("NASDAQ") || eUpper.includes("NMS") || eUpper.includes("NGM") || eUpper.includes("NCM")) return "NASDAQ";
  if (eUpper.includes("NYSE") || eUpper === "NYQ") return "NYSE";
  if (eUpper.includes("AMEX") || eUpper.includes("NYSE AMERICAN") || eUpper === "ASE") return "NYSEAMERICAN";
  return null;
}
function exchangeCompatibleWithStockCategory(exchange: string | null | undefined, category: string): boolean {
  const expected = normalizedExchangeCode(exchange);
  if (!expected) return true;
  const prefix = category.split(":")[0]?.trim().toUpperCase();
  if (!prefix || prefix === category.toUpperCase()) return true;
  if (expected === "NASDAQ") return prefix === "NASDAQ";
  if (expected === "NYSE") return prefix === "NYSE";
  if (expected === "NYSEAMERICAN") return ["NYSEAMERICAN", "NYSE AMERICAN", "AMEX"].includes(prefix);
  return true;
}
function extractXmlTagValues(xml: string, tagName: string): string[] {
  const escaped = tagName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const out: string[] = [];
  const re = new RegExp(`<${escaped}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${escaped}>`, "gi");
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    const value = decodeXmlText(m[1]).trim();
    if (value) out.push(value);
  }
  return out;
}
function extractGlobeNewswireCategories(xml: string, domain: string): string[] {
  const out: string[] = [];
  const domainLower = domain.toLowerCase();
  const re = /<category\b([^>]*)>([\s\S]*?)<\/category>/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    const attrs = m[1].toLowerCase();
    if (!attrs.includes(`domain="${domainLower}"`) && !attrs.includes(`domain='${domainLower}'`)) continue;
    const text = decodeXmlText(m[2]).trim();
    for (const part of text.split(",")) {
      const value = part.trim();
      if (value) out.push(value);
    }
  }
  return out;
}
function globenewswireStockCategoryMatches(ticker: string, stockCategories: string[], exchange?: string | null): boolean {
  const tickerU = ticker.toUpperCase();
  return stockCategories.some(category =>
    category.split(":").pop()?.trim().toUpperCase() === tickerU &&
    exchangeCompatibleWithStockCategory(exchange, category)
  );
}
function globenewswirePlainText(value: string): string {
  return stripHtmlTags(decodeXmlText(value));
}
async function fetchGlobeNewswireFeed(feed: { name: string; url: string }): Promise<string> {
  const cacheKey = `gnw_rss:${feed.name}`;
  const cached = globenewswireCache.get(cacheKey);
  if (cached && Date.now() - cached.storedAt < GLOBENEWSWIRE_TTL_MS) return cached.value;

  const resp = await providerFetch(feed.url, { headers: { "User-Agent": UA } });
  if (!resp.ok) {
    await resp.body?.cancel();
    throw new Error(`HTTP ${resp.status}`);
  }
  const declaredLen = Number(resp.headers.get("content-length") || "0");
  if (declaredLen > GLOBENEWSWIRE_MAX_BYTES) {
    await resp.body?.cancel();
    throw new Error("GlobeNewswire RSS response exceeded size limit");
  }
  const buf = await resp.arrayBuffer();
  if (buf.byteLength > GLOBENEWSWIRE_MAX_BYTES) {
    throw new Error("GlobeNewswire RSS response exceeded size limit");
  }
  const xml = new TextDecoder("utf-8").decode(buf);
  setBounded(globenewswireCache, cacheKey, { value: xml, storedAt: Date.now() }, GLOBENEWSWIRE_CACHE_MAX);
  return xml;
}
export async function collectGlobeNewswireEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number,
  exchange?: string | null
): Promise<NewsProviderResult> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  let parsedAnyFeed = false;
  const tickerU = ticker.toUpperCase();

  for (const feed of GLOBENEWSWIRE_RSS_FEEDS) {
    let xml = "";
    try {
      xml = await fetchGlobeNewswireFeed(feed);
    } catch (e) {
      warnings.push({
        code: "SOURCE_UNAVAILABLE",
        message: `GlobeNewswire RSS feed '${feed.name}' unavailable: ${e instanceof Error ? e.message : String(e)}`,
        severity: "warning",
      });
      continue;
    }

    try {
      if (/<!entity/i.test(xml)) {
        throw new Error("unsupported XML entity declaration in GlobeNewswire RSS");
      }
      if (!/<rss\b/i.test(xml) || !/<channel\b/i.test(xml) || !/<\/rss>/i.test(xml)) {
        throw new Error("invalid GlobeNewswire RSS XML");
      }
      const openItems = xml.match(/<item\b/gi)?.length ?? 0;
      const closeItems = xml.match(/<\/item>/gi)?.length ?? 0;
      if (openItems !== closeItems) {
        throw new Error("malformed GlobeNewswire RSS item XML");
      }
      const itemRe = /<item\b[^>]*>([\s\S]*?)<\/item>/gi;
      let m: RegExpExecArray | null;
      parsedAnyFeed = true;
      while ((m = itemRe.exec(xml)) !== null) {
        const itemXml = m[1];
        const stockCategories = extractGlobeNewswireCategories(itemXml, GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN);
        if (!globenewswireStockCategoryMatches(tickerU, stockCategories, exchange)) continue;

        const title = extractXmlTag(itemXml, "title");
        const descriptionRaw = extractXmlTag(itemXml, "description");
        const description = globenewswirePlainText(descriptionRaw);
        const link = extractXmlTag(itemXml, "link") || null;
        const guid = extractXmlTag(itemXml, "guid") || null;
        const publishedAt = normalizeIso(extractXmlTag(itemXml, "pubDate"));
        if (!withinDateWindow(publishedAt, startDate, endDate, lookbackDays)) continue;

        const issuer = extractXmlTag(itemXml, "dc:contributor") || null;
        const subject = extractXmlTag(itemXml, "dc:subject") || null;
        const language = extractXmlTag(itemXml, "dc:language") || null;
        const globenewswireId = extractXmlTag(itemXml, "dc:identifier") || null;
        const keywords = extractXmlTagValues(itemXml, "dc:keyword");
        const isinValues = extractGlobeNewswireCategories(itemXml, GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN);

        if (!publishedAt) {
          warnings.push({
            code: "PUBLISHED_AT_UNAVAILABLE",
            message: "Published timestamp unavailable for GlobeNewswire item.",
            severity: "warning",
          });
        }

        const duplicateGroupId = makeDupGroupId(tickerU, title, publishedAt, issuer);
        if (!duplicateGroupId) {
          warnings.push({ code: "DEDUPE_WEAK_KEY", message: "Weak dedupe key for at least one GlobeNewswire item.", severity: "warning" });
        }
        const dedupeKey = duplicateGroupId || `${feed.name}:${link || guid}:${title}:${publishedAt}`;
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);

        const item: Record<string, unknown> = {
          title,
          source: "newswire",
          originalSource: "GlobeNewswire",
          sourceType: "newswire",
          provider: "globenewswire",
          discoveredVia: "globenewswire_rss",
          publishedAt,
          retrievedAt,
          url: link || guid,
          issuer,
          tickers: [tickerU],
          eventType: eventTypeFromKeywords(title, `${description} ${subject ?? ""}`),
          summary: shortText(description || title, 240),
          evidenceText: shortText(description || title, 180),
          confidence: link || guid ? "HIGH" : "MEDIUM",
          tickerRelevance: "HIGH",
          decisionUse: sourceDecisionUse("newswire"),
          duplicateGroupId,
          stockCategories,
          feedSource: feed.name,
        };
        if (isinValues.length > 0) item.isin = isinValues[0];
        if (subject) item.subject = subject;
        if (keywords.length > 0) item.keywords = keywords;
        if (language) item.language = language;
        if (globenewswireId) item.globenewswireId = globenewswireId;
        items.push(item);
        if (items.length >= maxResults) return { items, warnings, used: true };
      }
    } catch (e) {
      warnings.push({
        code: "SOURCE_UNAVAILABLE",
        message: `GlobeNewswire RSS feed '${feed.name}' parse error: ${e instanceof Error ? e.message : String(e)}`,
        severity: "warning",
      });
    }
  }

  return { items, warnings, used: warnings.length > 0 && items.length === 0 ? false : parsedAnyFeed };
}
