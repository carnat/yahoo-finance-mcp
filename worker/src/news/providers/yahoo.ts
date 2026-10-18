/**
 * Yahoo Finance news provider: the search feed (getNews), issuer/identity matching and collectYahooEvents.
 * Moved out of news.ts in 2.15.1 (pure restructure); providers never import news.ts.
 */

import { yGet, enc, iso, _str, type NewsCompanyIdentity } from "../../yahoo-finance.js";
import {
  sourceDecisionUse,
  normalizeIso,
  shortText,
  eventTypeFromKeywords,
  makeDupGroupId,
  withinDateWindow,
  normalizeExchange,
} from "../shared.js";
import type { NewsProviderResultWithDiagnostics } from "./types.js";

const YAHOO_ALLOWED_CONTENT_TYPES = new Set(["STORY", "ARTICLE", "PRESS_RELEASE"]);

export async function getNews(ticker: string): Promise<string> {
  const retrievedAt = new Date().toISOString();
  const d = (await yGet(
    `https://query1.finance.yahoo.com/v1/finance/search?q=${enc(ticker)}&quotesCount=0&newsCount=20&enableFuzzyQuery=false`,
    false
  )) as Record<string, unknown>;

  const news = (d?.news as Record<string, unknown>[]) ?? [];
  const tickerUpper = ticker.toUpperCase();
  const items = news.map((n) => {
    const title = String(n.title ?? "");
    return {
      title,
      publisher: String(n.publisher ?? ""),
      url: String(n.link ?? ""),
      publishedAt: n.providerPublishTime ? iso(Number(n.providerPublishTime)) : null,
      retrievedAt,
      sourceType: "yahoo_finance",
      tickerRelevance: title.toUpperCase().includes(tickerUpper) ? "HIGH" : "UNKNOWN",
    };
  });
  return JSON.stringify({
    ticker,
    items,
    meta: {
      source: "yahoo_finance",
      watermark: retrievedAt,
      itemCount: items.length,
    },
  });
}
const YAHOO_NEWS_LEGAL_SUFFIXES = new Set([
  "inc", "incorporated", "corp", "corporation", "ltd", "limited", "llc", "plc",
  "co", "company", "sa", "ag", "nv", "se", "gmbh",
]);
const YAHOO_NEWS_ACRONYM_IGNORED_WORDS = new Set(["the", "and", "of", "for"]);
type YahooNewsIdentity = {
  status: "RESOLVED" | "UNAVAILABLE";
  companyName: string | null;
  aliases: string[];
  acronyms: string[];
  exchange: string | null;
};
type YahooNewsMatch = {
  basis: "TICKER_TOKEN" | "ISSUER_NAME" | "ISSUER_ACRONYM";
  rank: number;
};
function normalizedYahooNewsPhrase(value: unknown): string {
  return _str(value).toLowerCase().replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim();
}
function stripYahooNewsLegalSuffix(value: string): string {
  const words = value.split(" ").filter(Boolean);
  while (words.length > 1 && YAHOO_NEWS_LEGAL_SUFFIXES.has(words[words.length - 1])) words.pop();
  return words.join(" ");
}
export function yahooNewsIdentityFor(identity: NewsCompanyIdentity | null): YahooNewsIdentity {
  const aliases = new Set<string>();
  const acronyms = new Set<string>();
  for (const rawName of [identity?.shortName, identity?.longName, identity?.companyName]) {
    const normalized = normalizedYahooNewsPhrase(rawName);
    if (!normalized) continue;
    const stripped = stripYahooNewsLegalSuffix(normalized);
    for (const alias of [normalized, stripped]) {
      // quoteSummary may fall back shortName to the ticker; it is not issuer identity.
      if (alias.length >= 3 && alias !== identity?.ticker.toLowerCase()) aliases.add(alias);
    }
    const initials = stripped.split(" ")
      .filter(word => word && !YAHOO_NEWS_ACRONYM_IGNORED_WORDS.has(word))
      .map(word => word[0])
      .join("");
    // Two-character acronyms create a large false-positive surface in news text.
    if (initials.length >= 3 && initials.length <= 6) acronyms.add(initials);
  }
  return {
    status: aliases.size > 0 ? "RESOLVED" : "UNAVAILABLE",
    companyName: identity?.companyName ?? identity?.longName ?? identity?.shortName ?? null,
    aliases: [...aliases],
    acronyms: [...acronyms],
    exchange: identity?.exchange ?? null,
  };
}
/**
 * The issuer of a Yahoo press-release item: the company when the headline
 * starts with its name (or ticker), otherwise unknown. News stories have no
 * issuer.
 */
export function yahooItemIssuer(item: Record<string, unknown>, identity: YahooNewsIdentity): string | null {
  if (_str(item.source) !== "yahoo_finance_press_releases") return null;
  const title = normalizedYahooNewsPhrase(item.title);
  const tickers = Array.isArray(item.tickers) ? item.tickers.map((t) => _str(t).toLowerCase()) : [];
  const leads = [...identity.aliases, ...tickers];
  return leads.some((lead) => lead && (title === lead || title.startsWith(`${lead} `))) ? identity.companyName : null;
}
function yahooNewsMatchFor(text: string, ticker: string, identity: YahooNewsIdentity): YahooNewsMatch | null {
  if (isTickerCompatibleWithContext(text, ticker, identity.exchange)) return { basis: "TICKER_TOKEN", rank: 0 };
  if (identity.status !== "RESOLVED") return null;
  const normalizedText = ` ${normalizedYahooNewsPhrase(text)} `;
  if (identity.aliases.some(alias => normalizedText.includes(` ${alias} `))) return { basis: "ISSUER_NAME", rank: 1 };
  if (identity.acronyms.some(acronym => new RegExp(`\\b${acronym}\\b`, "i").test(text))) {
    return { basis: "ISSUER_ACRONYM", rank: 2 };
  }
  return null;
}
export function yahooDecisionUse(eventType: string): "CONTEXT_ONLY" | "CHECK_OFFICIAL_RELEASES" {
  return new Set(["earnings", "guidance", "contract", "financing", "product", "regulatory", "litigation"]).has(eventType)
    ? "CHECK_OFFICIAL_RELEASES"
    : "CONTEXT_ONLY";
}
function buildYfEventItem(
  ticker: string,
  raw: Record<string, unknown>,
  retrievedAt: string,
  feedSource?: string,
): { item: Record<string, unknown>; warnings: Record<string, unknown>[] } {
  const warnings: Record<string, unknown>[] = [];
  const content = (raw.content && typeof raw.content === "object") ? raw.content as Record<string, unknown> : {};
  const title = _str(content.title || raw.title).trim();
  const summary = _str(content.summary || raw.summary).trim();
  const originalSource = _str((content.provider as Record<string, unknown> | undefined)?.displayName || raw.publisher || "Yahoo Finance");
  const url = _str((content.canonicalUrl as Record<string, unknown> | undefined)?.url || raw.link || raw.url) || null;
  // Prefer native Yahoo publish timestamps, then normalized fallback from getNews().
  const publishedAt = normalizeIso(raw.providerPublishTime ?? content.pubDate ?? raw.publishedAt);
  if (!publishedAt) {
    warnings.push({ code: "PUBLISHED_AT_UNAVAILABLE", message: `Published timestamp unavailable for source '${originalSource}'.`, severity: "warning" });
  }

  // Determine the precise source label.
  let sourceKey: string;
  if (feedSource === "yahoo_finance_news" || feedSource === "yahoo_finance_press_releases") {
    sourceKey = feedSource;
  } else {
    const contentType = _str(content.contentType || raw.contentType).toUpperCase();
    sourceKey = contentType === "PRESS_RELEASE" ? "yahoo_finance_press_releases" : "yahoo_finance_news";
  }

  const relevance = `${title} ${summary}`.toUpperCase().includes(ticker.toUpperCase()) ? "HIGH" : "LOW";
  const confidence = !url || relevance !== "HIGH" ? "LOW" : "MEDIUM";
  const duplicateGroupId = makeDupGroupId(ticker, title, publishedAt, null);
  if (!duplicateGroupId) {
    warnings.push({ code: "DEDUPE_WEAK_KEY", message: "Weak dedupe key for at least one item.", severity: "warning" });
  }
  return {
    item: {
      title,
      source: sourceKey,
      originalSource,
      sourceType: sourceKey,
      publishedAt,
      retrievedAt,
      url,
      issuer: null,
      tickers: [ticker.toUpperCase()],
      eventType: eventTypeFromKeywords(title, summary),
      summary: shortText(summary || title, 240),
      evidenceText: shortText(summary || title, 180),
      confidence,
      tickerRelevance: relevance,
      decisionUse: sourceDecisionUse(sourceKey),
      duplicateGroupId,
    },
    warnings,
  };
}
function isTickerCompatibleWithContext(text: string, ticker: string, companyExchange: string | null | undefined): boolean {
  const tickerU = ticker.toUpperCase();
  const tickerEsc = tickerU.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const tickerPat = new RegExp(`\\b${tickerEsc}\\b`, "gi");
  
  const matches = [...text.matchAll(tickerPat)];
  if (matches.length === 0) return false;
  
  const normComp = normalizeExchange(companyExchange);
  if (!normComp) return true;
  
  const prefixPat = /\b([A-Za-z0-9]+)\s*:\s*$/i;
  
  for (const m of matches) {
    const idx = m.index ?? 0;
    const preceding = text.slice(Math.max(0, idx - 20), idx);
    const pm = preceding.match(prefixPat);
    if (pm) {
      const prefix = pm[1].toUpperCase();
      const normPrefix = normalizeExchange(prefix);
      if (normPrefix) {
        if (normPrefix === normComp) return true;
      } else {
        if (["TSXV", "TSX", "NYSE", "NASDAQ", "AMEX", "CVE", "ASX", "LSE", "TSX-V"].includes(prefix)) {
          // Incompatible exchange prefix
        } else {
          return true; // Not a known exchange prefix
        }
      }
    } else {
      return true; // No prefix
    }
  }
  
  return false;
}
export async function collectYahooEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  identity: YahooNewsIdentity,
  startDate = "",
  endDate = "",
  lookbackDays?: number,
  feed: "news" | "press_releases" = "news",
): Promise<NewsProviderResultWithDiagnostics> {
  const warnings: Record<string, unknown>[] = [];
  const accepted: Array<{ item: Record<string, unknown>; rank: number }> = [];
  const diagnostics: Record<string, unknown> = {
    rawCount: 0,
    retrievedCount: 0,
    filteredCount: 0,
    acceptedCount: 0,
    rejectedCount: 0,
    rejectionCounts: {},
    identityStatus: identity.status,
  };
  const reject = (reason: string): void => {
    diagnostics.rejectedCount = Number(diagnostics.rejectedCount) + 1;
    const rejectionCounts = diagnostics.rejectionCounts as Record<string, number>;
    rejectionCounts[reason] = (rejectionCounts[reason] ?? 0) + 1;
  };
  const feedSource = feed === "press_releases" ? "yahoo_finance_press_releases" : "yahoo_finance_news";
  try {
    // Yahoo retired its press-releases tab endpoint (/xhr/ncp?queryRef=pressRelease: HTTP 404 for every ticker,
    // 2.5.33), so both feeds read the news search feed; press releases are the issuer's own wire releases in it.
    if (feed === "press_releases") diagnostics.method = YAHOO_PRESS_RELEASE_METHOD;
    const raw = JSON.parse(await getNews(ticker)) as Record<string, unknown>;
    const newsRaw = (raw.items as Record<string, unknown>[]) ?? [];
    diagnostics.rawCount = newsRaw.length;
    diagnostics.retrievedCount = newsRaw.length;
    for (const n of newsRaw) {
      const content = (n.content && typeof n.content === "object") ? n.content as Record<string, unknown> : {};
      const ct = _str(content.contentType || n.contentType).toUpperCase();
      if (ct && !YAHOO_ALLOWED_CONTENT_TYPES.has(ct)) {
        reject("CONTENT_TYPE");
        continue;
      }
      // For the press-releases feed, Yahoo tab membership is authoritative:
      // valid press-release tab items may still arrive as STORY/ARTICLE.
      const { item, warnings: w } = buildYfEventItem(ticker, n, retrievedAt, feedSource);
      if (!withinDateWindow(_str(item.publishedAt) || null, startDate, endDate, lookbackDays)) {
        reject("DATE_WINDOW");
        continue;
      }
      const match = yahooNewsMatchFor(`${_str(item.title)} ${_str(item.summary)} ${_str(item.evidenceText)}`, ticker, identity);
      if (!match) {
        reject(identity.status === "UNAVAILABLE" ? "IDENTITY_UNAVAILABLE_TICKER_NOT_FOUND" : "IDENTITY_MISMATCH");
        continue;
      }
      // The match names the company, but a release about it may be issued
      // by another company ("Qualcomm renews license with Apple").
      item.mentionedCompany = identity.companyName;
      item.issuer = yahooItemIssuer(item, identity);
      // A press release is the company's own: a wire release whose headline leads with its name or ticker
      // (AEHR's ACCESS Newswire Sonoma order), not another issuer's release that mentions it (QuickLogic's).
      if (feed === "press_releases" && !(PRESS_RELEASE_WIRE_RE.test(_str(item.originalSource)) && item.issuer)) {
        reject("NOT_ISSUER_WIRE_RELEASE");
        continue;
      }
      item.matchBasis = match.basis;
      item.sourceTickerMatch = true;
      item.tickerRelevance = "HIGH";
      item.confidence = item.url ? "MEDIUM" : "LOW";
      item.decisionUse = yahooDecisionUse(_str(item.eventType));
      accepted.push({ item, rank: match.rank });
      warnings.push(...w);
    }
  } catch (e) {
    warnings.push({ code: "SOURCE_UNAVAILABLE", message: `Yahoo Finance source unavailable: ${e instanceof Error ? e.message : String(e)}`, severity: "warning" });
    return { items: [], warnings, used: false, diagnostics };
  }
  accepted.sort((a, b) => a.rank - b.rank || _str(b.item.publishedAt).localeCompare(_str(a.item.publishedAt)));
  diagnostics.filteredCount = accepted.length;
  diagnostics.acceptedCount = accepted.length;
  return { items: accepted.slice(0, maxResults).map(entry => entry.item), warnings, used: true, diagnostics };
}
// Wire services that distribute issuers' own press releases (not news services such as MT Newswires).
const PRESS_RELEASE_WIRE_RE = /^(?:access\s?newswire|accesswire|globe\s?newswire|business\s?wire|pr\s?newswire|newsfile|prweb|eqs(?:\s+newswire)?|cision)\b/i;
const YAHOO_PRESS_RELEASE_METHOD = "ISSUER_WIRE_RELEASES_IN_NEWS_FEED";
