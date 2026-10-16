/**
 * News and public events: company news, press releases, SEC recent events, the event timeline and event
 * verification, with the source collectors and parsers behind them (Phase 3 move out of yahoo-finance.ts).
 * Shared helpers still live in yahoo-finance.ts and are imported back; see analysis/yahoo-finance-mcp/PHASE3_IMPORTS_TS.md.
 */

import { setBounded } from "./cache.js";
import { providerFetch, withBudgetWatch } from "./request-context.js";
import { getWorkerVar } from "./response.js";
import { rankEvidence, stemWord } from "./extraction-rules.js";
import { reliabilityClassFor } from "./deprecations.js";
import registryManifest from "./company-ir-page-registry.json";
import { companyIrPageUrlAllowed } from "./url-policy.js";
import {
  UA,
  type ProviderJsonResult,
  providerCacheKey,
  getProviderCache,
  setProviderCache,
  providerHttpStatus,
  fetchProviderWithTimeout,
  yGet,
  enc,
  iso,
  runPartialBatch,
  edgarGetHtml,
  edgarCikFromAccession,
  getSubmissionsForTicker,
  stripHtmlTags,
  _str,
  clampInt,
  type NewsCompanyIdentity,
  finnhubEligibility,
  resolveNewsCompanyIdentity,
  toIsoUtc,
  resolveEx991Url,
} from "./yahoo-finance.js";

const FINNHUB_COMPANY_NEWS_API = "https://finnhub.io/api/v1/company-news";
const MARKETAUX_NEWS_API = "https://api.marketaux.com/v1/news/all";
const MARKETAUX_WIRE_DOMAINS = ["businesswire.com", "prnewswire.com", "globenewswire.com"] as const;
const MARKETAUX_MAX_ITEMS = 3;
const MARKETAUX_NEWS_TTL_MS = 15 * 60 * 1000;

async function fetchMarketauxJson(
  params: Record<string, unknown>,
  ttlMs = MARKETAUX_NEWS_TTL_MS,
): Promise<ProviderJsonResult> {
  const cacheKey = providerCacheKey("marketaux", "/v1/news/all", params);
  const cached = await getProviderCache(cacheKey, ttlMs);
  if (cached) return cached;
  const token = getWorkerVar("MARKETAUX_API_TOKEN") ?? getWorkerVar("MARKETAUX_API_KEY");
  const publicQuery = new URLSearchParams(
    Object.entries({ ...params, api_token: "REDACTED" }).map(([key, value]): [string, string] => [key, String(value)])
  );
  const publicUrl = `${MARKETAUX_NEWS_API}?${publicQuery.toString()}`;
  if (!token) {
    return { payload: null, status: "SOURCE_UNCONFIGURED", publicUrl, providerAttempted: false, cacheStatus: "MISS" };
  }
  const requestQuery = new URLSearchParams(publicQuery);
  requestQuery.set("api_token", token);
  try {
    const response = await fetchProviderWithTimeout(
      `${MARKETAUX_NEWS_API}?${requestQuery.toString()}`,
      { headers: { "User-Agent": UA } },
      12_000,
    );
    const retryAfter = response.headers.get("Retry-After") ?? undefined;
    if (!response.ok) {
      const body = (await response.text()).replaceAll(token, "REDACTED");
      const status = response.status === 402 || response.status === 429
        ? "RATE_LIMIT"
        : response.status === 401 || response.status === 403
          ? "AUTH_ERROR"
          : providerHttpStatus(response.status, body);
      return {
        payload: null,
        status,
        publicUrl,
        providerAttempted: true,
        cacheStatus: "MISS",
        httpStatus: response.status,
        message: body.slice(0, 500) || undefined,
        retryAfter,
      };
    }
    const rawJson = await response.json();
    if (!rawJson || typeof rawJson !== "object" || Array.isArray(rawJson)) {
      return { payload: null, status: "PROVIDER_ERROR", publicUrl, providerAttempted: true, cacheStatus: "MISS", message: "Marketaux returned a non-object payload." };
    }
    const payload = rawJson as Record<string, unknown>;
    const fetchedAt = new Date().toISOString();
    await setProviderCache(cacheKey, payload, publicUrl, fetchedAt, ttlMs);
    return { payload, status: "OK", publicUrl, providerAttempted: true, cacheStatus: "MISS", fetchedAt };
  } catch (error) {
    const message = error instanceof Error ? error.message.replaceAll(token, "REDACTED") : String(error);
    const status = /timeout|timed out|abort/i.test(message) ? "TIMEOUT" : "PROVIDER_ERROR";
    return { payload: null, status, publicUrl, providerAttempted: true, cacheStatus: "MISS", message };
  }
}
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
const COMPANY_IR_HTML_MAX_BYTES = 750 * 1024;
const COMPANY_IR_BLOCKED_WEBSITE_HOSTS = [
  "bloomberg.com",
  "facebook.com",
  "finance.yahoo.com",
  "google.com",
  "instagram.com",
  "linkedin.com",
  "marketwatch.com",
  "nasdaq.com",
  "reuters.com",
  "sec.gov",
  "twitter.com",
  "wikipedia.org",
  "x.com",
  "yahoo.com",
  "youtube.com",
];

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
const OFFICIAL_SOURCE_TYPES = new Set(["sec_filing", "company_ir", "company_ir_page", "press_release", "newswire", "yahoo_finance_press_releases"]);
const SUPPORTED_EVENT_SOURCES = new Set([
  "sec", "company_ir", "company_ir_page", "newswire",
  "yahoo_finance",                  // legacy: aggregates news + press releases
  "yahoo_finance_news",             // Yahoo Finance general news tab
  "yahoo_finance_press_releases",   // Yahoo Finance press releases tab
  "finnhub", "marketaux",
]);

const SOURCE_PRIORITY: Record<string, number> = {
  sec_filing: 0,
  sec_ex99_found: 0,
  company_ir: 1,
  company_ir_page: 1,
  press_release: 2,
  yahoo_finance_press_releases: 2,
  newswire: 3,
  company_news: 4,
  yahoo_finance: 5,
  yahoo_finance_news: 5,
  marketaux_wire: 6,
  other: 7,
};
function confidenceForSourceType(sourceType: unknown, fallback: unknown = "LOW"): string {
  const src = _str(sourceType);
  if (src === "sec_filing" || src === "company_ir" || src === "company_ir_page") return "HIGH";
  if (src === "press_release" || src === "yahoo_finance_press_releases" || src === "newswire") return "MEDIUM";
  if (src === "company_news" || src === "yahoo_finance_news" || src === "yahoo_finance" || src === "finnhub") return "LOW";
  return _str(fallback, "LOW");
}
// Default source sets the news tools apply when `sources` is omitted or null (Q3h, 2.7.0).
// An explicit array, including [], is used as given: [] attempts no source.
export const DEFAULT_NEWS_SOURCES = ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"];
export const DEFAULT_PRESS_RELEASE_SOURCES = ["sec", "company_ir_page", "yahoo_finance_press_releases"];
export const DEFAULT_EVENT_SOURCES = ["sec", "company_ir_page", "company_ir", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub", "marketaux"];
const NOT_ATTEMPTED_SOURCE_STATUSES = new Set(["NOT_ELIGIBLE", "NOT_NEEDED", "UNCONFIGURED"]);
/** What the caller asked for and what was actually tried (Q3h): requestedSources is null when sources was omitted. */
export function sourceRequestFields(requestedSources: string[] | null, sourceStatus: Record<string, unknown>): Record<string, unknown> {
  const attemptedSources: Record<string, unknown>[] = [];
  for (const [source, value] of Object.entries(sourceStatus)) {
    if (!value || typeof value !== "object") continue;
    const info = value as Record<string, unknown>;
    const status = _str(info.status);
    if (info.attempted === false || NOT_ATTEMPTED_SOURCE_STATUSES.has(status)) continue;
    attemptedSources.push({ sourceType: source, status });
  }
  return { requestedSources, defaultSourcesApplied: requestedSources === null, attemptedSources };
}
function normalizeSources(sources: string[] | undefined, defaults: string[]): { selected: string[]; warnings: Record<string, unknown>[] } {
  const warnings: Record<string, unknown>[] = [];
  const input = (sources ?? defaults).map(s => _str(s).toLowerCase().trim()).filter(Boolean);
  const seen = new Set<string>();
  const selected: string[] = [];
  for (const src of input) {
    if (!SUPPORTED_EVENT_SOURCES.has(src)) {
      warnings.push({ code: "SOURCE_UNSUPPORTED", message: `Source '${src}' is not supported.`, severity: "warning" });
      continue;
    }
    if (!seen.has(src)) {
      seen.add(src);
      selected.push(src);
    }
  }
  // An explicit list is used as given (Q3h): [] or only unsupported names selects no source.
  return { selected, warnings };
}
/**
 * An explicit sources list that selects no supported source attempts nothing (Q3h). The result is
 * neither a provider failure nor confirmed absence, so it reads as source-limited.
 */
function markNoSourceSelected(payload: Record<string, unknown>, requestedSources: string[] | null): Record<string, unknown> {
  if (requestedSources === null || normalizeSources(requestedSources, []).selected.length > 0) return payload;
  payload.status = "SOURCE_LIMITED_NOT_FOUND";
  payload.sourceCoverage = "PARTIAL";
  if (payload.coverage && typeof payload.coverage === "object") {
    payload.coverage = { ...(payload.coverage as Record<string, unknown>), state: "PARTIAL", recommendedNextAction: "CHECK_OFFICIAL_RELEASES" };
  }
  return payload;
}
type CompanyEventCollectionResult = {
  items: Record<string, unknown>[];
  sourcesUsed: string[];
  warnings: Record<string, unknown>[];
  watermark: string;
  sourceDiagnostics: Record<string, unknown>;
  /** Sources whose collection had a fetch refused for the subrequest budget (2.11.0); never serialized. */
  budgetStarvedSources?: string[];
};
function sourceDecisionUse(sourceType: unknown): string {
  const src = _str(sourceType);
  if (src === "sec_ex99_found") return "USE_OFFICIAL_EVIDENCE";
  if (["sec_filing", "company_ir", "company_ir_page", "press_release", "yahoo_finance_press_releases", "newswire", "marketaux_wire"].includes(src)) {
    return "CHECK_OFFICIAL_RELEASES";
  }
  return "CONTEXT_ONLY";
}
function evidenceClassFor(sourceType: unknown): string {
  const source = _str(sourceType);
  if (source === "sec_filing" || source === "sec_ex99_found") return "SEC_FILING";
  if (source === "company_ir_page") return "APPROVED_IR_PAGE";
  if (source === "company_ir") return "OFFICIAL_RSS_ATOM";
  if (["newswire", "press_release", "yahoo_finance_press_releases"].includes(source)) return "WIRE_RELEASE";
  return "CONTEXTUAL_NEWS";
}
function urlProvenanceFor(item: Record<string, unknown>): string {
  const source = _str(item.sourceType);
  if (["sec_filing", "sec_ex99_found", "company_ir", "company_ir_page"].includes(source)) return "OFFICIAL";
  const url = _str(item.url);
  if (!url) return "UNKNOWN";
  try {
    const host = new URL(url).hostname.toLowerCase();
    if (host.endsWith("finance.yahoo.com") || host.endsWith("finnhub.io")) return "PROVIDER";
  } catch { return "UNKNOWN"; }
  return "PUBLISHER";
}
function tickerMatchFor(item: Record<string, unknown>): string {
  if (["TICKER_TOKEN", "ISSUER_NAME", "ISSUER_ACRONYM", "PROVIDER_ENTITY_SYMBOL"].includes(_str(item.matchBasis))) return "EXPLICIT";
  const configuredTicker = Array.isArray(item.tickers) ? item.tickers.map(value => _str(value).toUpperCase()).find(Boolean) ?? "" : "";
  const text = `${_str(item.title)} ${_str(item.summary)} ${_str(item.evidenceText)}`.toUpperCase();
  if (configuredTicker && new RegExp(`\\b${configuredTicker.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`).test(text)) return "EXPLICIT";
  if (configuredTicker) return "SOURCE_SCOPED";
  return "UNVERIFIED";
}
function enrichNewsItemForLlm(item: Record<string, unknown>): Record<string, unknown> {
  const sourceType = _str(item.sourceType);
  const decisionUse = sourceType === "company_ir_page" && _str(item.approvalStatus) === "approved"
    ? "USE_OFFICIAL_EVIDENCE"
    : (sourceType === "yahoo_finance_news" || sourceType === "company_news"
      ? yahooDecisionUse(_str(item.eventType))
      : sourceDecisionUse(sourceType));
  return {
    ...item,
    evidenceClass: evidenceClassFor(item.sourceType),
    tickerMatch: tickerMatchFor(item),
    urlProvenance: urlProvenanceFor(item),
    decisionUse,
  };
}
export function buildCoverage(sourceStatus: Record<string, unknown>, resolvedAction: string | null = null): Record<string, unknown> {
  const failedStatuses = new Set(["UNCONFIGURED", "PROVIDER_ERROR", "AUTH_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_BUDGET_EXHAUSTED", "PROVIDER_CHANGED", "IDENTITY_UNAVAILABLE", "WEBSITE_NOT_AVAILABLE", "DISCOVERY_NOT_FOUND", "DISCOVERY_BUDGET_EXHAUSTED", "FEED_NOT_FOUND", "PARSE_ERROR", "CANDIDATE_AVAILABLE", "NOT_REGISTERED", "DISABLED", "REVALIDATION_DUE", "SOURCE_VALIDATION_FAILED"]);
  const failedSources: Record<string, unknown>[] = [];
  const skippedSources: Record<string, unknown>[] = [];
  const truncatedSources: Record<string, unknown>[] = [];
  for (const [source, value] of Object.entries(sourceStatus)) {
    if (!value || typeof value !== "object") continue;
    const info = value as Record<string, unknown>;
    const status = _str(info.status);
    const entry = { source, status, attempted: info.attempted !== false, reasonCode: info.reasonCode ?? null };
    if (status === "NOT_ELIGIBLE" || status === "NOT_NEEDED") skippedSources.push(entry);
    else if (failedStatuses.has(status)) failedSources.push(entry);
    if (info.hasMoreAccepted === true) {
      truncatedSources.push({
        source,
        acceptedCount: Number(info.acceptedCount ?? 0),
        returnedCount: Number(info.returnedCount ?? 0),
        truncatedCount: Number(info.truncatedCount ?? 0),
      });
    }
  }
  const coverageLimitingSkips = skippedSources.filter(entry => entry.status !== "NOT_NEEDED");
  const recommendedNextAction = resolvedAction ?? (
    failedSources.some(entry => ["RATE_LIMITED", "TIMEOUT", "PROVIDER_BUDGET_EXHAUSTED"].includes(_str(entry.status)))
      ? "RETRY_RETRYABLE_SOURCES"
      : (failedSources.length || coverageLimitingSkips.length
        ? "CHECK_OFFICIAL_RELEASES"
        : (truncatedSources.length ? "RETRY_TRUNCATED_SOURCE" : "USE_RETURNED_CONTEXT"))
  );
  return { state: computeSourceCoverage(sourceStatus), failedSources, skippedSources, truncatedSources, recommendedNextAction };
}
type CompanyIrPageSource = {
  ticker: string;
  issuerName: string;
  status: "candidate" | "approved" | "disabled";
  adapter: "structured" | "html_article";
  canonicalUrl: string;
  allowedHosts: string[];
  allowedPathPrefixes: string[];
  candidateReason?: string | null;
  discoveredAt?: string | null;
  reviewedBy?: string | null;
  reviewedAt?: string | null;
  revalidateAfter?: string | null;
};
type CompanyIrPageRegistry = {
  schemaVersion: string;
  registryVersion: string;
  sources: CompanyIrPageSource[];
};
const companyIrPageRegistry = registryManifest as unknown as CompanyIrPageRegistry;
function companyIrPageEntry(ticker: string): CompanyIrPageSource | null {
  const tickerU = ticker.toUpperCase();
  return companyIrPageRegistry.sources.find(entry => entry.ticker.toUpperCase() === tickerU) ?? null;
}
function registryFresh(entry: CompanyIrPageSource): boolean {
  if (!entry.revalidateAfter) return true;
  const deadline = Date.parse(entry.revalidateAfter);
  return Number.isFinite(deadline) && deadline >= Date.now();
}
function arrayRecords(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.filter((v): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v)) : [];
}
export function hostWithoutWww(host: string): string {
  return host.toLowerCase().replace(/^www\./, "");
}
export function blockedCompanyWebsiteHost(host: string): boolean {
  const h = hostWithoutWww(host);
  return COMPANY_IR_BLOCKED_WEBSITE_HOSTS.some(blocked => h === blocked || h.endsWith(`.${blocked}`));
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
function yahooNewsIdentityFor(identity: NewsCompanyIdentity | null): YahooNewsIdentity {
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
function yahooDecisionUse(eventType: string): "CONTEXT_ONLY" | "CHECK_OFFICIAL_RELEASES" {
  return new Set(["earnings", "guidance", "contract", "financing", "product", "regulatory", "litigation"]).has(eventType)
    ? "CHECK_OFFICIAL_RELEASES"
    : "CONTEXT_ONLY";
}
function htmlAttr(attrs: string, name: string): string {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const m = attrs.match(new RegExp(`${escaped}\\s*=\\s*("([^"]*)"|'([^']*)'|([^\\s"'>]+))`, "i"));
  return decodeXmlText((m?.[2] ?? m?.[3] ?? m?.[4] ?? "").trim());
}
function normalizeLinkedUrl(raw: string, baseUrl: string): string | null {
  const value = decodeXmlText(raw).trim();
  if (!value || /^javascript:|^mailto:|^tel:/i.test(value)) return null;
  try {
    const u = new URL(value, baseUrl);
    if (!["http:", "https:"].includes(u.protocol)) return null;
    u.hash = "";
    return u.toString();
  } catch {
    return null;
  }
}
function looksLikeFeed(text: string): boolean {
  return /<rss\b/i.test(text) || /<feed\b/i.test(text);
}
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
function normalizeIso(raw: unknown): string | null {
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
function shortText(value: unknown, maxLen = 220): string | null {
  const s = _str(value).replace(/\s+/g, " ").trim();
  return s ? s.slice(0, maxLen) : null;
}
function eventTypeFromKeywords(title: string, summary: string): string {
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
function eventTypeFromForm(formType: string): string {
  const f = formType.toUpperCase();
  if (f === "10-Q" || f === "10-K") return "earnings";
  if (f === "S-3" || f === "S-1" || f === "424B") return "financing";
  if (f === "8-K" || f === "DEF14A" || f === "PRE14A") return "regulatory";
  if (f === "4") return "insider";
  return "other";
}
function makeDupGroupId(ticker: string, title: string | null, publishedAt: string | null, issuer: string | null): string | null {
  const normTitle = normalizedEventTitleStem(ticker, title);
  const day = _str(publishedAt).slice(0, 10);
  const entity = (_str(issuer) || ticker).toUpperCase();
  if (!normTitle && !day) return null;
  let h = 0;
  const s = `${normTitle}|${day}|${entity}`;
  for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  return (h >>> 0).toString(16).padStart(8, "0");
}
function xmlTag(xml: string, path: string[]): string | null {
  let scope = xml;
  for (const tag of path) {
    const re = new RegExp(`<(?:\\w+:)?${tag}\\b[^>]*>([\\s\\S]*?)<\\/(?:\\w+:)?${tag}>`, "i");
    const m = scope.match(re);
    if (!m) return null;
    scope = m[1];
  }
  return scope.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim() || null;
}
function xmlBlock(xml: string, tag: string): string | null {
  const m = xml.match(new RegExp(`<(?:\\w+:)?${tag}\\b[^>]*>([\\s\\S]*?)<\\/(?:\\w+:)?${tag}>`, "i"));
  return m ? m[1] : null;
}
const FORM4_TRANSACTION_LABELS: Record<string, string> = {
  P: "Purchase",
  S: "Sale",
  A: "Grant/Award",
  D: "Disposition",
  M: "Option exercise/conversion",
  F: "Tax withholding/payment",
  G: "Gift",
};
function form4Num(value: string | null): number | null {
  if (value == null) return null;
  const text = stripHtmlTags(value);
  const m = text.match(/-?\$?\s*\(?\d[\d,]*(?:\.\d+)?\)?/);
  if (!m) return null;
  let s = m[0].replace(/\$/g, "").replace(/,/g, "").replace(/\s+/g, "");
  const negative = s.startsWith("(") && s.endsWith(")");
  s = s.replace(/[()]/g, "");
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return negative ? -n : n;
}
function form4Date(value: string | null): string | null {
  const text = stripHtmlTags(value ?? "");
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const m = text.match(/\b(\d{1,2})\/(\d{1,2})\/(\d{4})\b/);
  if (!m) return text;
  return `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
}
function form4Payload(
  owner: string | null,
  role: string | null,
  code: string | null,
  shares: number | null,
  price: number | null,
  ownershipForm: string | null,
  transactionDate: string | null,
): Record<string, unknown> {
  return {
    owner,
    role,
    transactionCode: code,
    transactionLabel: FORM4_TRANSACTION_LABELS[_str(code).toUpperCase()] ?? "Other/Unclassified",
    shares,
    price,
    value: shares != null && price != null ? Math.round(shares * price * 100) / 100 : null,
    ownershipForm,
    transactionDate,
  };
}
function form4OwnerFromHtml(html: string): string | null {
  const m = html.match(/Name and Address of Reporting Person[\s\S]*?<a\b[^>]*>([\s\S]*?)<\/a>/i);
  return m ? stripHtmlTags(m[1]) || null : null;
}
function form4RoleFromHtml(html: string): string | null {
  const m = html.match(/Relationship of Reporting Person\(s\) to Issuer([\s\S]*?)Individual or Joint\/Group Filing/i);
  if (!m) return null;
  const roles: string[] = [];
  for (const row of parseHtmlTable(m[1])) {
    for (let i = 0; i < row.length - 1; i++) {
      if (row[i].trim().toUpperCase() !== "X") continue;
      const label = row[i + 1].toLowerCase();
      if (label.includes("director")) roles.push("director");
      else if (label.includes("officer")) roles.push("officer");
      else if (label.includes("10%") || label.includes("owner")) roles.push("ten_percent_owner");
      else if (label.includes("other")) roles.push("other");
    }
  }
  return [...new Set(roles)].join(", ") || null;
}
function parseForm4HtmlTransaction(html: string): Record<string, unknown> | null {
  const owner = form4OwnerFromHtml(html);
  const role = form4RoleFromHtml(html);
  const tableRe = /<table[^>]*>[\s\S]*?<\/table>/gi;
  let tableM: RegExpExecArray | null;
  while ((tableM = tableRe.exec(html)) !== null) {
    const tableHtml = tableM[0];
    const tableText = stripHtmlTags(tableHtml).toLowerCase();
    if (tableText.includes("non-derivative securities")) {
      for (const row of parseHtmlTable(tableHtml)) {
        if (row.length < 10) continue;
        const code = row[3].trim().toUpperCase();
        const shares = form4Num(row[5]);
        if (!code || shares == null) continue;
        return form4Payload(
          owner,
          role,
          code,
          shares,
          form4Num(row[7]),
          row[9].trim() || null,
          form4Date(row[1]),
        );
      }
    }
    if (tableText.includes("derivative securities")) {
      for (const row of parseHtmlTable(tableHtml)) {
        if (row.length < 15) continue;
        const code = row[4].trim().toUpperCase();
        const shares = form4Num(row[6]);
        if (!code || shares == null) continue;
        return form4Payload(
          owner,
          role,
          code,
          shares,
          form4Num(row[1]),
          row[14].trim() || null,
          form4Date(row[2]),
        );
      }
    }
  }
  return null;
}
function parseForm4Transaction(xml: string): Record<string, unknown> | null {
  const tx = xmlBlock(xml, "nonDerivativeTransaction") ?? xmlBlock(xml, "derivativeTransaction");
  if (!tx) return parseForm4HtmlTransaction(xml);
  const owner = xmlTag(xml, ["reportingOwnerId", "rptOwnerName"]);
  const officerTitle = xmlTag(xml, ["reportingOwnerRelationship", "officerTitle"]);
  const roles: string[] = [];
  for (const [tag, label] of [["isDirector", "director"], ["isOfficer", "officer"], ["isTenPercentOwner", "ten_percent_owner"], ["isOther", "other"]]) {
    if (["1", "true"].includes(_str(xmlTag(xml, ["reportingOwnerRelationship", tag])).toLowerCase())) roles.push(label);
  }
  const code = xmlTag(tx, ["transactionCoding", "transactionCode"]);
  const shares = form4Num(xmlTag(tx, ["transactionAmounts", "transactionShares", "value"]));
  const price = form4Num(xmlTag(tx, ["transactionAmounts", "transactionPricePerShare", "value"]));
  return form4Payload(
    owner,
    officerTitle || roles.join(", ") || null,
    code,
    shares,
    price,
    xmlTag(tx, ["ownershipNature", "directOrIndirectOwnership", "value"]),
    form4Date(xmlTag(tx, ["transactionDate", "value"])),
  );
}
async function tryAttachForm4Transaction(item: Record<string, unknown>, filing: Record<string, unknown>, warnings: Record<string, unknown>[]): Promise<void> {
  if (_str(filing.filingType).toUpperCase() !== "4") return;
  const url = _str(item.url);
  if (!url.startsWith("https://www.sec.gov/Archives/")) {
    warnings.push({ code: "FORM4_PARSE_UNAVAILABLE", message: "Form 4 primary document URL is unavailable.", severity: "warning" });
    return;
  }
  const xml = await edgarGetHtml(url, 2_000_000);
  const parsed = parseForm4Transaction(xml ?? "");
  if (!parsed) {
    warnings.push({ code: "FORM4_PARSE_UNAVAILABLE", message: "Form 4 transaction details could not be parsed from the primary document.", severity: "warning" });
    return;
  }
  item.insiderTransaction = parsed;
  const label = _str(parsed.transactionLabel) || "Insider transaction";
  const owner = _str(parsed.owner) || "reporting owner";
  const shares = typeof parsed.shares === "number" ? `${Math.round(parsed.shares).toLocaleString("en-US")} shares` : "shares unavailable";
  const value = typeof parsed.value === "number" ? `, value $${Math.round(parsed.value).toLocaleString("en-US")}` : "";
  item.title = `Form 4: ${label} by ${owner}`;
  item.summary = shortText(`${label} by ${owner}: ${shares}${value}.`);
  item.evidenceText = shortText(`SEC Form 4 transaction code ${_str(parsed.transactionCode) || "unknown"} on ${_str(parsed.transactionDate) || _str(item.filingDate)}.`);
}
function withinDateWindow(publishedAt: string | null, startDate = "", endDate = "", lookbackDays?: number): boolean {
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
function decodeXmlText(value: string): string {
  return value
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/gi, "$1")
    .replace(/&(?:amp|#38);/gi, "&")
    .replace(/&(?:lt|#60);/gi, "<")
    .replace(/&(?:gt|#62);/gi, ">")
    .replace(/&(?:quot|#34);/gi, "\"")
    .replace(/&(?:apos|#39);/gi, "'")
    .replace(/&#(\d+);/g, (_m, code) => String.fromCharCode(Number(code)));
}
function extractXmlTag(xml: string, tagName: string): string {
  const escaped = tagName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const m = xml.match(new RegExp(`<${escaped}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${escaped}>`, "i"));
  return m ? decodeXmlText(m[1]).trim() : "";
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
async function fetchApprovedCompanyIrPage(entry: CompanyIrPageSource): Promise<{ text: string; finalUrl: string; contentType: string }> {
  if (!companyIrPageUrlAllowed(entry, entry.canonicalUrl)) {
    throw new Error("registry canonical URL is outside its allow-list");
  }
  const resp = await providerFetch(entry.canonicalUrl, {
    headers: {
      "User-Agent": UA,
      Accept: "application/json, application/rss+xml, application/atom+xml, application/xml, text/xml, text/html;q=0.9",
    },
  });
  if (!resp.ok) {
    await resp.body?.cancel();
    throw new Error(`HTTP ${resp.status}`);
  }
  const finalUrl = resp.url || entry.canonicalUrl;
  if (!companyIrPageUrlAllowed(entry, finalUrl)) {
    await resp.body?.cancel();
    throw new Error("redirect target is outside registry allow-list");
  }
  const contentType = _str(resp.headers.get("content-type")).toLowerCase();
  if (entry.adapter === "structured" && contentType && !/(json|xml|rss|atom|text\/plain)/i.test(contentType)) {
    await resp.body?.cancel();
    throw new Error(`unexpected structured content type: ${contentType}`);
  }
  if (entry.adapter === "html_article" && contentType && !/(html|text\/plain)/i.test(contentType)) {
    await resp.body?.cancel();
    throw new Error(`unexpected HTML content type: ${contentType}`);
  }
  const declaredLen = Number(resp.headers.get("content-length") || "0");
  if (declaredLen > COMPANY_IR_HTML_MAX_BYTES) {
    await resp.body?.cancel();
    throw new Error("response exceeded size limit");
  }
  const buf = await resp.arrayBuffer();
  if (buf.byteLength > COMPANY_IR_HTML_MAX_BYTES) throw new Error("response exceeded size limit");
  return { text: new TextDecoder("utf-8").decode(buf), finalUrl, contentType };
}
function companyIrPageItemBase(
  ticker: string,
  entry: CompanyIrPageSource,
  title: string,
  summary: string,
  rawUrl: string | null,
  publishedAt: string | null,
  retrievedAt: string,
  finalUrl: string,
): Record<string, unknown> | null {
  const url = rawUrl ? normalizeLinkedUrl(rawUrl, finalUrl) : finalUrl;
  if (!url || !companyIrPageUrlAllowed(entry, url)) return null;
  const tickerU = ticker.toUpperCase();
  const duplicateGroupId = makeDupGroupId(tickerU, title, publishedAt, entry.issuerName);
  return {
    title,
    source: "company_ir_page",
    originalSource: entry.issuerName,
    sourceType: "company_ir_page",
    provider: "company_ir_page_registry",
    discoveredVia: "approved_company_ir_page_registry",
    adapter: entry.adapter,
    canonicalUrl: entry.canonicalUrl,
    registryVersion: companyIrPageRegistry.registryVersion,
    approvalStatus: "approved",
    publishedAt,
    retrievedAt,
    url,
    issuer: entry.issuerName,
    tickers: [tickerU],
    eventType: eventTypeFromKeywords(title, summary),
    summary: shortText(summary || title, 240),
    evidenceText: shortText(summary || title, 180),
    confidence: "HIGH",
    tickerRelevance: "HIGH",
    decisionUse: sourceDecisionUse("company_ir_page"),
    duplicateGroupId,
  };
}
function companyIrPageFeedItems(
  ticker: string,
  entry: CompanyIrPageSource,
  xml: string,
  retrievedAt: string,
  finalUrl: string,
): Record<string, unknown>[] {
  const { itemXmls } = parseCompanyIrFeedItems(xml);
  const items: Record<string, unknown>[] = [];
  for (const feedItem of itemXmls) {
    const title = extractXmlTag(feedItem.xml, "title");
    if (!title) continue;
    const summaryRaw = feedItem.kind === "atom"
      ? (extractXmlTag(feedItem.xml, "summary") || extractXmlTag(feedItem.xml, "content"))
      : (extractXmlTag(feedItem.xml, "description") || extractXmlTag(feedItem.xml, "content:encoded"));
    const summary = stripHtmlTags(summaryRaw);
    const rawLink = feedItem.kind === "atom"
      ? atomEntryLink(feedItem.xml)
      : (extractXmlTag(feedItem.xml, "link") || extractXmlTag(feedItem.xml, "guid"));
    const publishedAt = normalizeIso(
      feedItem.kind === "atom"
        ? (extractXmlTag(feedItem.xml, "published") || extractXmlTag(feedItem.xml, "updated"))
        : extractXmlTag(feedItem.xml, "pubDate")
    );
    const item = companyIrPageItemBase(ticker, entry, title, summary, rawLink || null, publishedAt, retrievedAt, finalUrl);
    if (item) items.push(item);
  }
  return items;
}
function structuredJsonItemArrays(value: unknown): Record<string, unknown>[] {
  const root = value && typeof value === "object" ? value as Record<string, unknown> : {};
  const candidates = [
    root.items,
    root.releases,
    root.pressReleases,
    root.news,
    root.data,
    (root.feed && typeof root.feed === "object" ? (root.feed as Record<string, unknown>).items : null),
  ];
  for (const candidate of candidates) {
    const records = arrayRecords(candidate);
    if (records.length > 0) return records;
  }
  if (root.data && typeof root.data === "object" && !Array.isArray(root.data)) {
    for (const value of Object.values(root.data as Record<string, unknown>)) {
      const records = arrayRecords(value);
      if (records.length > 0) return records;
    }
  }
  return [];
}
function companyIrPageStructuredJsonItems(
  ticker: string,
  entry: CompanyIrPageSource,
  text: string,
  retrievedAt: string,
  finalUrl: string,
): Record<string, unknown>[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return [];
  }
  const items: Record<string, unknown>[] = [];
  for (const raw of structuredJsonItemArrays(parsed)) {
    const title = _str(raw.title || raw.headline || raw.name).trim();
    if (!title) continue;
    const summary = _str(raw.summary || raw.description || raw.excerpt || raw.content_text || raw.content || "").trim();
    const rawUrl = _str(raw.url || raw.link || raw.external_url || raw.permalink || "").trim() || null;
    const publishedAt = normalizeIso(raw.publishedAt || raw.published_at || raw.datePublished || raw.date_published || raw.date || raw.pubDate);
    const item = companyIrPageItemBase(ticker, entry, title, summary, rawUrl, publishedAt, retrievedAt, finalUrl);
    if (item) items.push(item);
  }
  return items;
}
function companyIrPageHtmlItems(
  ticker: string,
  entry: CompanyIrPageSource,
  html: string,
  retrievedAt: string,
  finalUrl: string,
): Record<string, unknown>[] {
  const items: Record<string, unknown>[] = [];
  const blocks: string[] = [];
  const articleRe = /<article\b[^>]*>[\s\S]*?<\/article>/gi;
  let articleM: RegExpExecArray | null;
  while ((articleM = articleRe.exec(html)) !== null && blocks.length < 50) blocks.push(articleM[0]);
  if (blocks.length === 0) {
    const liRe = /<li\b[^>]*>[\s\S]*?<\/li>/gi;
    let liM: RegExpExecArray | null;
    while ((liM = liRe.exec(html)) !== null && blocks.length < 50) {
      const text = stripHtmlTags(liM[0]).toLowerCase();
      if (/(press release|news release|earnings|results|announces|reports)/.test(text)) blocks.push(liM[0]);
    }
  }
  for (const block of blocks) {
    const headingM = block.match(/<h[1-4]\b[^>]*>([\s\S]*?)<\/h[1-4]>/i);
    const anchorM = block.match(/<a\b([^>]*)>([\s\S]*?)<\/a>/i);
    const title = stripHtmlTags(headingM?.[1] || anchorM?.[2] || "").trim();
    if (!title || title.length < 6) continue;
    const href = anchorM ? htmlAttr(anchorM[1], "href") : "";
    const timeM = block.match(/<time\b([^>]*)>([\s\S]*?)<\/time>/i);
    const datetime = timeM ? (htmlAttr(timeM[1], "datetime") || stripHtmlTags(timeM[2])) : "";
    const publishedAt = normalizeIso(datetime);
    const summary = stripHtmlTags(block);
    const item = companyIrPageItemBase(ticker, entry, title, summary, href || null, publishedAt, retrievedAt, finalUrl);
    if (item) items.push(item);
  }
  return items;
}
async function collectCompanyIrPageEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean; diagnostics: Record<string, unknown> }> {
  const tickerU = ticker.toUpperCase();
  const entry = companyIrPageEntry(tickerU);
  const warnings: Record<string, unknown>[] = [];
  const baseDiagnostics: Record<string, unknown> = {
    registryVersion: companyIrPageRegistry.registryVersion,
    registrySchemaVersion: companyIrPageRegistry.schemaVersion,
  };
  if (!entry) {
    return {
      items: [],
      warnings,
      used: false,
      diagnostics: { ...baseDiagnostics, status: "NOT_REGISTERED", approvalStatus: "not_registered", allowedAction: "registry_review_required" },
    };
  }
  const diagnostics: Record<string, unknown> = {
    ...baseDiagnostics,
    status: "EMPTY_RESULT",
    approvalStatus: entry.status,
    adapter: entry.adapter,
    canonicalUrl: entry.canonicalUrl,
    allowedHosts: entry.allowedHosts,
    allowedPathPrefixes: entry.allowedPathPrefixes,
    revalidateAfter: entry.revalidateAfter ?? null,
  };
  if (entry.status === "disabled") {
    return { items: [], warnings, used: false, diagnostics: { ...diagnostics, status: "DISABLED", allowedAction: "none" } };
  }
  if (entry.status === "candidate") {
    diagnostics.status = "CANDIDATE_AVAILABLE";
    diagnostics.allowedAction = "review_and_promote";
    diagnostics.candidates = [{
      ticker: entry.ticker,
      issuerName: entry.issuerName,
      canonicalUrl: entry.canonicalUrl,
      adapter: entry.adapter,
      candidateReason: entry.candidateReason ?? null,
      discoveredAt: entry.discoveredAt ?? null,
      decisionGrade: false,
    }];
    return { items: [], warnings, used: false, diagnostics };
  }
  if (!registryFresh(entry)) {
    warnings.push({
      code: "COMPANY_IR_PAGE_REVALIDATION_DUE",
      message: "Approved company IR page is past its revalidation date; skipped until reviewed.",
      severity: "warning",
      canonicalUrl: entry.canonicalUrl,
    });
    return { items: [], warnings, used: false, diagnostics: { ...diagnostics, status: "REVALIDATION_DUE", allowedAction: "revalidate" } };
  }

  try {
    const { text, finalUrl } = await fetchApprovedCompanyIrPage(entry);
    let parsed = looksLikeFeed(text)
      ? companyIrPageFeedItems(tickerU, entry, text, retrievedAt, finalUrl)
      : entry.adapter === "structured"
        ? companyIrPageStructuredJsonItems(tickerU, entry, text, retrievedAt, finalUrl)
        : companyIrPageHtmlItems(tickerU, entry, text, retrievedAt, finalUrl);
    parsed = parsed.filter(item => withinDateWindow(_str(item.publishedAt) || null, startDate, endDate, lookbackDays)).slice(0, maxResults);
    diagnostics.status = parsed.length > 0 ? "APPROVED_SOURCE_OK" : "EMPTY_RESULT";
    diagnostics.allowedAction = "fetch_configured_source";
    diagnostics.finalUrl = finalUrl;
    diagnostics.rawCount = parsed.length;
    diagnostics.filteredCount = parsed.length;
    return { items: parsed, warnings, used: true, diagnostics };
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    warnings.push({
      code: "COMPANY_IR_PAGE_VALIDATION_FAILED",
      message: `Approved company IR page validation failed: ${message}`,
      severity: "warning",
      canonicalUrl: entry.canonicalUrl,
    });
    return { items: [], warnings, used: false, diagnostics: { ...diagnostics, status: "SOURCE_VALIDATION_FAILED", allowedAction: "review_registry_entry" } };
  }
}
function parseCompanyIrFeedItems(xml: string): { feedTitle: string | null; itemXmls: Array<{ kind: "rss" | "atom"; xml: string }> } {
  if (/<!entity/i.test(xml)) throw new Error("unsupported XML entity declaration in company IR RSS/Atom");
  if (!looksLikeFeed(xml)) throw new Error("invalid company IR RSS/Atom XML");
  const feedTitle = extractXmlTag(xml, "title") || null;
  const itemXmls: Array<{ kind: "rss" | "atom"; xml: string }> = [];
  const rssRe = /<item\b[^>]*>([\s\S]*?)<\/item>/gi;
  let rssM: RegExpExecArray | null;
  while ((rssM = rssRe.exec(xml)) !== null) itemXmls.push({ kind: "rss", xml: rssM[1] });
  const atomRe = /<entry\b[^>]*>([\s\S]*?)<\/entry>/gi;
  let atomM: RegExpExecArray | null;
  while ((atomM = atomRe.exec(xml)) !== null) itemXmls.push({ kind: "atom", xml: atomM[1] });
  return { feedTitle, itemXmls };
}
function atomEntryLink(entryXml: string): string {
  const linkRe = /<link\b([^>]*)\/?>/gi;
  let m: RegExpExecArray | null;
  while ((m = linkRe.exec(entryXml)) !== null) {
    const rel = htmlAttr(m[1], "rel").toLowerCase();
    const href = htmlAttr(m[1], "href");
    if (href && (!rel || rel === "alternate")) return href;
  }
  return extractXmlTag(entryXml, "link");
}
/**
 * The company_ir source (2.10.0): approved company IR registry entries only.
 * Open-web discovery (probing the profile website and its investor/ir hosts,
 * then fetching linked feeds on any host) was removed with the fetch
 * allow-list. Without an approved entry the source is NOT_REGISTERED, the
 * same status an unregistered company_ir_page source reports.
 */
async function collectCompanyIrEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean; diagnostics: Record<string, unknown> }> {
  const entry = companyIrPageEntry(ticker);
  if (!entry || entry.status !== "approved") {
    return {
      items: [],
      warnings: [],
      used: false,
      diagnostics: {
        registryVersion: companyIrPageRegistry.registryVersion,
        registrySchemaVersion: companyIrPageRegistry.schemaVersion,
        status: "NOT_REGISTERED",
        approvalStatus: "not_registered",
        allowedAction: "registry_review_required",
      },
    };
  }
  return collectCompanyIrPageEvents(ticker, maxResults, retrievedAt, startDate, endDate, lookbackDays);
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
function buildSecEventItem(
  ticker: string,
  filing: Record<string, unknown>,
  retrievedAt: string,
  issuer: string | null
): { item: Record<string, unknown>; warnings: Record<string, unknown>[] } {
  const warnings: Record<string, unknown>[] = [];
  const filingType = _str(filing.filingType || filing.formType || filing.form).toUpperCase();
  const filingDate = _str(filing.filingDate);
  const acceptedAt = normalizeIso(filing.acceptedAt ?? filing.acceptanceDateTime);
  let publishedAt = acceptedAt;
  if (!publishedAt && filingDate) {
    publishedAt = `${filingDate}T00:00:00Z`;
    warnings.push({
      code: "PUBLISHED_AT_ESTIMATED",
      message: `acceptedAt unavailable for ${filingType || "SEC filing"}; filingDate used.`,
      severity: "warning",
    });
  }
  const accessionNumber = _str(filing.accessionNumber);
  const cikInt = _str(filing.cikInt || filing.cik);
  const accClean = accessionNumber.replace(/-/g, "");
  const primaryDocument = _str(filing.primaryDocument);
  let url = _str(filing.documentUrl || filing.primaryDocumentUrl);
  if (!url && cikInt && accessionNumber && primaryDocument) {
    url = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${primaryDocument}`;
  }
  if (!url && cikInt && accessionNumber) {
    url = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${accessionNumber}-index.htm`;
  }
  if (!url.startsWith("https://www.sec.gov/Archives/")) {
    url = "";
    warnings.push({ code: "SEC_URL_INVALID", message: "SEC event URL missing or invalid SEC Archives URL.", severity: "warning" });
  }
  const duplicateGroupId = makeDupGroupId(ticker, `${filingType} filed`, publishedAt, issuer);
  if (!duplicateGroupId) {
    warnings.push({ code: "DEDUPE_WEAK_KEY", message: "Weak dedupe key for at least one item.", severity: "warning" });
  }
  const confidence = accessionNumber && acceptedAt && url ? "HIGH" : (accessionNumber && url ? "MEDIUM" : "LOW");
  return {
    item: {
      title: `${filingType || "SEC"} filed`,
      source: "SEC",
      sourceType: "sec_filing",
      filingType: filingType || null,
      filingDate: filingDate || null,
      acceptedAt: acceptedAt || null,
      accessionNumber: accessionNumber || null,
      cikInt: cikInt || null,
      url: url || null,
      publishedAt,
      retrievedAt,
      issuer,
      tickers: [ticker.toUpperCase()],
      eventType: eventTypeFromForm(filingType),
      summary: shortText(`SEC ${filingType} filing for ${ticker.toUpperCase()}`),
      evidenceText: shortText(`${filingType} accepted by SEC on ${acceptedAt || filingDate}`),
      confidence,
      tickerRelevance: "HIGH",
      decisionUse: sourceDecisionUse("sec_filing"),
      duplicateGroupId,
    },
    warnings,
  };
}
function dedupeEventItems(items: Record<string, unknown>[], warnings: Record<string, unknown>[]): Record<string, unknown>[] {
  const grouped = new Map<string, Record<string, unknown>>();
  const passthrough: Record<string, unknown>[] = [];
  for (const item of items) {
    const gid = _str(item.duplicateGroupId);
    if (!gid) {
      passthrough.push(item);
      continue;
    }
    const existing = grouped.get(gid);
    if (!existing) {
      grouped.set(gid, item);
      continue;
    }
    const existingRank = SOURCE_PRIORITY[_str(existing.sourceType, "other")] ?? SOURCE_PRIORITY.other;
    const currentRank = SOURCE_PRIORITY[_str(item.sourceType, "other")] ?? SOURCE_PRIORITY.other;
    let keepNew = currentRank < existingRank;
    if (currentRank === existingRank) {
      keepNew = _str(item.publishedAt) > _str(existing.publishedAt);
    }
    if (_str(existing.publishedAt) !== _str(item.publishedAt)) {
      warnings.push({
        code: "TIMESTAMP_VARIANCE",
        message: `Different source timestamps observed for duplicateGroupId=${gid}; this is dedupe metadata, not factual evidence conflict.`,
        severity: "warning",
        duplicateGroupId: gid,
        statusImpact: false,
      });
    }
    const preferred = keepNew ? item : existing;
    const alternate = keepNew ? existing : item;
    const refs = Array.isArray(preferred.sourceRefs) ? preferred.sourceRefs as Record<string, unknown>[] : [];
    refs.push({
      source: alternate.source,
      sourceType: alternate.sourceType,
      publishedAt: alternate.publishedAt,
      url: alternate.url,
    });
    preferred.sourceRefs = refs;
    grouped.set(gid, preferred);
  }
  const deduped = [...grouped.values(), ...passthrough];
  deduped.sort((a, b) => _str(b.publishedAt).localeCompare(_str(a.publishedAt)));
  return deduped;
}
function collectionStatus(items: Record<string, unknown>[], sourcesUsed: string[], warnings: Record<string, unknown>[]): string | null {
  const hasLimitedSource = warnings.some(w => ["SOURCE_UNAVAILABLE", "SOURCE_NOT_ELIGIBLE", "SOURCE_IDENTITY_UNAVAILABLE"].includes(_str(w.code)));
  if (items.length > 0 && hasLimitedSource) return "PARTIAL";
  if (items.length === 0) {
    if (hasLimitedSource) return "SOURCE_LIMITED_NOT_FOUND";
    if (warnings.some(w => [
      "COMPANY_WEBSITE_NOT_AVAILABLE",
      "COMPANY_IR_FEED_NOT_FOUND",
      "COMPANY_IR_RSS_HOST_REJECTED",
      "COMPANY_IR_RSS_PARSE_ERROR",
      "COMPANY_IR_RSS_UNAVAILABLE",
      "COMPANY_IR_DISCOVERY_BUDGET_EXHAUSTED",
      "COMPANY_IR_PAGE_VALIDATION_FAILED",
      "COMPANY_IR_PAGE_REVALIDATION_DUE",
    ].includes(_str(w.code)))) return "SOURCE_LIMITED_NOT_FOUND";
    if (sourcesUsed.length > 0) return "NOT_FOUND";
    return "PROVIDER_ERROR";
  }
  return null;
}
function computeSourceStatus(
  sourcesUsed: string[],
  warnings: Record<string, unknown>[],
  items: Record<string, unknown>[],
  selectedSources: string[],
  sourceDiagnostics: Record<string, unknown> = {},
  budgetStarvedSources: string[] = [],
): Record<string, unknown> {
  const warningCodes = new Set(warnings.map(w => _str(w.code)));
  const warningMsgs = warnings
    .filter(w => _str(w.code) === "SOURCE_UNAVAILABLE")
    .map(w => _str(w.message).toLowerCase());
  const isYfError = warningMsgs.some(m => m.includes("yahoo finance"));
  const secItems = items.filter(it => _str(it.sourceType).includes("sec"));
  const getItemSource = (it: Record<string, unknown>): string => _str(it.source);
  const getItemSourceType = (it: Record<string, unknown>): string => _str(it.sourceType);
  // Fine-grained Yahoo Finance items
  const yfNewsItems = items.filter(
    it => getItemSource(it) === "yahoo_finance_news" || getItemSourceType(it) === "yahoo_finance_news"
  );
  const yfPrItems = items.filter(
    it =>
      getItemSource(it) === "yahoo_finance_press_releases"
      || getItemSourceType(it) === "yahoo_finance_press_releases"
  );
  // Legacy yahoo_finance aggregates both fine-grained sources plus legacy-tagged items
  const yfLegacyItems = items.filter(
    it =>
      ["yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases"].includes(getItemSource(it))
      || ["yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases"].includes(getItemSourceType(it))
  );
  const newswireItems = items.filter(it => _str(it.sourceType) === "newswire");
  const companyIrItems = items.filter(it => ["company_ir", "press_release"].includes(_str(it.sourceType)));
  const companyIrPageItems = items.filter(it => _str(it.sourceType) === "company_ir_page");
  const finnhubItems = items.filter(it => _str(it.source) === "finnhub");
  const marketauxItems = items.filter(it => _str(it.source) === "marketaux");
  const result: Record<string, unknown> = {};
  const yahooStatus = (source: string, sourceItems: Record<string, unknown>[]): Record<string, unknown> | null => {
    const diagnostic = (sourceDiagnostics[source] && typeof sourceDiagnostics[source] === "object")
      ? sourceDiagnostics[source] as Record<string, unknown>
      : null;
    if (!diagnostic) return null;
    const acceptedCount = Number(diagnostic.acceptedCount ?? diagnostic.filteredCount ?? sourceItems.length);
    return {
      status: diagnostic.completed === false ? "PROVIDER_ERROR" : (acceptedCount > 0 ? "OK" : "EMPTY_RESULT"),
      rawCount: Number(diagnostic.rawCount ?? 0),
      retrievedCount: Number(diagnostic.retrievedCount ?? diagnostic.rawCount ?? 0),
      filteredCount: Number(diagnostic.filteredCount ?? 0),
      acceptedCount,
      returnedCount: sourceItems.length,
      rejectedCount: Number(diagnostic.rejectedCount ?? 0),
      rejectionCounts: diagnostic.rejectionCounts ?? {},
      identityStatus: diagnostic.identityStatus ?? null,
      attempted: diagnostic.attempted !== false,
      ...(diagnostic.method ? { method: diagnostic.method } : {}),
    };
  };
  const identityDiagnostic = (sourceDiagnostics.yahoo_finance_identity && typeof sourceDiagnostics.yahoo_finance_identity === "object")
    ? sourceDiagnostics.yahoo_finance_identity as Record<string, unknown>
    : null;
  if (identityDiagnostic && _str(identityDiagnostic.status) === "IDENTITY_UNAVAILABLE") {
    result.yahoo_finance_identity = identityDiagnostic;
  }
  if (selectedSources.includes("sec")) {
    if (sourcesUsed.includes("sec")) {
      result.sec = { status: secItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: secItems.length, filteredCount: secItems.length };
    } else if (warningMsgs.some(m => m.includes("sec submissions"))) {
      result.sec = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.sec = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  // Fine-grained Yahoo Finance sources
  if (selectedSources.includes("yahoo_finance_news")) {
    const diagnosticStatus = yahooStatus("yahoo_finance_news", yfNewsItems);
    if (diagnosticStatus) {
      result.yahoo_finance_news = diagnosticStatus;
    } else if (sourcesUsed.includes("yahoo_finance_news")) {
      result.yahoo_finance_news = { status: yfNewsItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: yfNewsItems.length, filteredCount: yfNewsItems.length };
    } else if (isYfError) {
      result.yahoo_finance_news = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.yahoo_finance_news = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  if (selectedSources.includes("yahoo_finance_press_releases")) {
    const diagnosticStatus = yahooStatus("yahoo_finance_press_releases", yfPrItems);
    if (diagnosticStatus) {
      result.yahoo_finance_press_releases = diagnosticStatus;
    } else if (sourcesUsed.includes("yahoo_finance_press_releases")) {
      result.yahoo_finance_press_releases = { status: yfPrItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: yfPrItems.length, filteredCount: yfPrItems.length };
    } else if (isYfError) {
      result.yahoo_finance_press_releases = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.yahoo_finance_press_releases = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  // Legacy yahoo_finance aggregate
  if (selectedSources.includes("yahoo_finance")) {
    const diagnosticStatus = yahooStatus("yahoo_finance", yfLegacyItems);
    if (diagnosticStatus) {
      result.yahoo_finance = diagnosticStatus;
    } else if (sourcesUsed.includes("yahoo_finance")) {
      result.yahoo_finance = { status: yfLegacyItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: yfLegacyItems.length, filteredCount: yfLegacyItems.length };
    } else if (isYfError) {
      result.yahoo_finance = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.yahoo_finance = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  if (selectedSources.includes("finnhub")) {
    const diagnostic = (sourceDiagnostics.finnhub && typeof sourceDiagnostics.finnhub === "object")
      ? sourceDiagnostics.finnhub as Record<string, unknown>
      : {};
    if (_str(diagnostic.status) === "NOT_ELIGIBLE") {
      result.finnhub = { status: "NOT_ELIGIBLE", rawCount: 0, filteredCount: 0, ...diagnostic };
    } else if (sourcesUsed.includes("finnhub")) {
      const acceptedCount = Number(diagnostic.acceptedCount ?? finnhubItems.length);
      const returnedCount = finnhubItems.length;
      const truncatedCount = Math.max(0, acceptedCount - returnedCount);
      result.finnhub = {
        status: acceptedCount > 0 ? "OK" : "EMPTY_RESULT",
        rawCount: Number(diagnostic.rawCount ?? acceptedCount),
        retrievedCount: Number(diagnostic.retrievedCount ?? diagnostic.rawCount ?? acceptedCount),
        filteredCount: acceptedCount,
        acceptedCount,
        collectedCount: Number(diagnostic.collectedCount ?? acceptedCount),
        returnedCount,
        rejectedCount: Number(diagnostic.rejectedCount ?? 0),
        rejectionCounts: diagnostic.rejectionCounts ?? {},
        truncatedCount,
        hasMoreAccepted: truncatedCount > 0,
        attempted: diagnostic.attempted !== false,
      };
    } else if (warningMsgs.some(m => m.includes("finnhub company-news source is not configured"))) {
      result.finnhub = { status: "UNCONFIGURED" };
    } else if (warningMsgs.some(m => m.includes("finnhub auth error"))) {
      result.finnhub = { status: "AUTH_ERROR", rawCount: 0, filteredCount: 0 };
    } else if (warningMsgs.some(m => m.includes("finnhub rate limited"))) {
      result.finnhub = { status: "RATE_LIMITED", rawCount: 0, filteredCount: 0 };
    } else if (warningMsgs.some(m => m.includes("finnhub provider changed"))) {
      result.finnhub = { status: "PROVIDER_CHANGED", rawCount: 0, filteredCount: 0 };
    } else if (warningMsgs.some(m => m.includes("finnhub"))) {
      result.finnhub = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.finnhub = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  if (selectedSources.includes("marketaux")) {
    const diagnostic = (sourceDiagnostics.marketaux && typeof sourceDiagnostics.marketaux === "object")
      ? sourceDiagnostics.marketaux as Record<string, unknown>
      : {};
    const acceptedCount = Number(diagnostic.acceptedCount ?? marketauxItems.length);
    const status = _str(diagnostic.status) || (acceptedCount > 0 ? "OK" : "EMPTY_RESULT");
    const allowedAction: Record<string, string> = {
      NOT_NEEDED: "none",
      UNCONFIGURED: "configure_marketaux_api_token",
      AUTH_ERROR: "verify_marketaux_api_token",
      RATE_LIMITED: "retry_after_quota_reset",
      TIMEOUT: "retry_later",
      PROVIDER_CHANGED: "inspect_provider_contract",
      PROVIDER_ERROR: "retry_or_use_official_sources",
    };
    result.marketaux = {
      status,
      rawCount: Number(diagnostic.rawCount ?? 0),
      retrievedCount: Number(diagnostic.retrievedCount ?? diagnostic.rawCount ?? 0),
      filteredCount: acceptedCount,
      acceptedCount,
      returnedCount: marketauxItems.length,
      rejectedCount: Number(diagnostic.rejectedCount ?? 0),
      rejectionCounts: diagnostic.rejectionCounts ?? {},
      attempted: diagnostic.attempted !== false,
      reasonCode: diagnostic.reasonCode ?? null,
      fallbackReason: diagnostic.fallbackReason ?? null,
      quotaPolicy: diagnostic.quotaPolicy ?? null,
      domains: diagnostic.domains ?? null,
      allowedAction: allowedAction[status] ?? (acceptedCount > 0 ? "check_official_releases" : "none"),
    };
  }
  if (selectedSources.includes("company_ir")) {
    const diagnostic = (sourceDiagnostics.company_ir && typeof sourceDiagnostics.company_ir === "object")
      ? sourceDiagnostics.company_ir as Record<string, unknown>
      : {};
    let companyIrStatus = "EMPTY_RESULT";
    if (sourcesUsed.includes("company_ir")) {
      companyIrStatus = companyIrItems.length > 0 ? "OK" : "EMPTY_RESULT";
    } else if (warningCodes.has("COMPANY_WEBSITE_NOT_AVAILABLE")) {
      companyIrStatus = "WEBSITE_NOT_AVAILABLE";
    } else if (warningCodes.has("COMPANY_IR_DISCOVERY_BUDGET_EXHAUSTED")) {
      companyIrStatus = "DISCOVERY_BUDGET_EXHAUSTED";
    } else if (warningCodes.has("COMPANY_IR_FEED_NOT_FOUND")) {
      companyIrStatus = "FEED_NOT_FOUND";
    } else if (warningCodes.has("COMPANY_IR_RSS_PARSE_ERROR")) {
      companyIrStatus = "PARSE_ERROR";
    } else if (warningCodes.has("COMPANY_IR_RSS_UNAVAILABLE")) {
      companyIrStatus = "PROVIDER_ERROR";
    } else if (warningCodes.has("COMPANY_IR_RSS_HOST_REJECTED")) {
      companyIrStatus = "DISCOVERY_NOT_FOUND";
    } else if (isYfError) {
      companyIrStatus = "PROVIDER_ERROR";
    }
    result.company_ir = { status: companyIrStatus, rawCount: companyIrItems.length, filteredCount: companyIrItems.length, ...diagnostic };
  }
  if (selectedSources.includes("company_ir_page")) {
    const diagnostic = (sourceDiagnostics.company_ir_page && typeof sourceDiagnostics.company_ir_page === "object")
      ? sourceDiagnostics.company_ir_page as Record<string, unknown>
      : {};
    let companyIrPageStatus = _str(diagnostic.status) || "EMPTY_RESULT";
    if (sourcesUsed.includes("company_ir_page") && !companyIrPageStatus) {
      companyIrPageStatus = companyIrPageItems.length > 0 ? "APPROVED_SOURCE_OK" : "EMPTY_RESULT";
    } else if (warningCodes.has("COMPANY_IR_PAGE_VALIDATION_FAILED")) {
      companyIrPageStatus = "SOURCE_VALIDATION_FAILED";
    } else if (warningCodes.has("COMPANY_IR_PAGE_REVALIDATION_DUE")) {
      companyIrPageStatus = "REVALIDATION_DUE";
    }
    result.company_ir_page = {
      status: companyIrPageStatus,
      rawCount: companyIrPageItems.length,
      filteredCount: companyIrPageItems.length,
      ...diagnostic,
    };
  }
  if (selectedSources.includes("newswire")) {
    if (sourcesUsed.includes("newswire")) {
      result.newswire = { status: newswireItems.length > 0 ? "OK" : "EMPTY_RESULT", rawCount: newswireItems.length, filteredCount: newswireItems.length };
    } else if (warningMsgs.some(m => m.includes("globenewswire"))) {
      result.newswire = { status: "PROVIDER_ERROR", rawCount: 0, filteredCount: 0 };
    } else {
      result.newswire = { status: "EMPTY_RESULT", rawCount: 0, filteredCount: 0 };
    }
  }
  return markBudgetStarvedSources(result, budgetStarvedSources);
}
/**
 * A source whose collection had a fetch refused for the subrequest budget and
 * did not end OK reports PROVIDER_BUDGET_EXHAUSTED (2.11.0), never
 * PROVIDER_ERROR, EMPTY_RESULT or another absence. Only `status` changes.
 */
const BUDGET_UNAFFECTED_STATUSES = new Set(["OK", "APPROVED_SOURCE_OK"]);
function markBudgetStarvedSources(sourceStatus: Record<string, unknown>, budgetStarvedSources: string[]): Record<string, unknown> {
  for (const source of budgetStarvedSources) {
    const info = sourceStatus[source];
    if (!info || typeof info !== "object") continue;
    const row = info as Record<string, unknown>;
    if (!BUDGET_UNAFFECTED_STATUSES.has(_str(row.status))) row.status = "PROVIDER_BUDGET_EXHAUSTED";
  }
  return sourceStatus;
}
function computeSourceCoverage(sourceStatus: Record<string, unknown>): string {
  const limitedStatuses = new Set([
    "UNCONFIGURED",
    "NOT_ELIGIBLE",
    "PROVIDER_ERROR",
    "AUTH_ERROR",
    "RATE_LIMITED",
    "TIMEOUT",
    "PROVIDER_BUDGET_EXHAUSTED",
    "PROVIDER_CHANGED",
    "IDENTITY_UNAVAILABLE",
    "WEBSITE_NOT_AVAILABLE",
    "DISCOVERY_NOT_FOUND",
    "DISCOVERY_BUDGET_EXHAUSTED",
    "FEED_NOT_FOUND",
    "PARSE_ERROR",
    "CANDIDATE_AVAILABLE",
    "NOT_REGISTERED",
    "DISABLED",
    "REVALIDATION_DUE",
    "SOURCE_VALIDATION_FAILED",
  ]);
  for (const info of Object.values(sourceStatus)) {
    if (info && typeof info === "object" && limitedStatuses.has(_str((info as Record<string, unknown>).status))) {
      return "PARTIAL";
    }
  }
  return "FULL";
}
function verifyCoverageFailureMode(
  sourceStatus: Record<string, unknown>,
  warnings: Record<string, unknown>[]
): string | null {
  const sourceStatuses = Object.values(sourceStatus)
    .filter((info): info is Record<string, unknown> => !!info && typeof info === "object")
    .map(info => _str(info.status));
  const warningText = warnings
    .filter(w => _str(w.code) === "SOURCE_UNAVAILABLE")
    .map(w => _str(w.message).toLowerCase())
    .join(" ");
  if (warningText.includes("too many subrequests") || sourceStatuses.includes("PROVIDER_BUDGET_EXHAUSTED")) return "WORKER_SUBREQUEST_LIMIT";
  if (sourceStatuses.includes("RATE_LIMITED")) return "RATE_LIMITED";
  if (sourceStatuses.includes("TIMEOUT")) return "PROVIDER_TIMEOUT";
  if (sourceStatuses.includes("PROVIDER_CHANGED")) return "PROVIDER_CHANGED";
  if (sourceStatuses.includes("IDENTITY_UNAVAILABLE")) return "YAHOO_IDENTITY_UNAVAILABLE";
  if (sourceStatuses.includes("UNCONFIGURED")) return "SOURCE_UNCONFIGURED";
  if (sourceStatuses.includes("NOT_ELIGIBLE")) return "SOURCE_NOT_ELIGIBLE";
  if (sourceStatuses.includes("PROVIDER_ERROR")) return "PROVIDER_ERROR";
  return null;
}
async function collectSecEvents(
  ticker: string,
  filingTypes: string[],
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean }> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    warnings.push({ code: "SOURCE_UNAVAILABLE", message: "SEC submissions source unavailable.", severity: "warning" });
    return { items, warnings, used: false };
  }
  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form ?? []) as string[];
  const filingDates = (recent.filingDate ?? []) as string[];
  const accepted = (recent.acceptanceDateTime ?? []) as string[];
  const accessions = (recent.accessionNumber ?? []) as string[];
  const primaryDocs = (recent.primaryDocument ?? []) as string[];
  const issuer = _str(submissions.name) || null;
  const wanted = new Set(filingTypes.map(f => f.toUpperCase()));
  const cikInt = String(parseInt(cikPadded, 10));
  for (let i = 0; i < forms.length && items.length < maxResults; i++) {
    const form = _str(forms[i]).toUpperCase();
    if (wanted.size > 0 && !wanted.has(form)) continue;
    const filing = {
      filingType: form,
      filingDate: filingDates[i] ?? "",
      acceptedAt: accepted[i] ?? "",
      accessionNumber: accessions[i] ?? "",
      primaryDocument: primaryDocs[i] ?? "",
      cikInt,
    };
    const { item, warnings: w } = buildSecEventItem(ticker, filing, retrievedAt, issuer);
    await tryAttachForm4Transaction(item, filing, w);
    if (!withinDateWindow(_str(item.publishedAt) || null, startDate, endDate, lookbackDays)) continue;
    items.push(item);
    warnings.push(...w);
  }
  return { items, warnings, used: true };
}
function normalizeExchange(exch: string | null | undefined): string | null {
  if (!exch) return null;
  const e = exch.toUpperCase();
  if (e.includes("NASDAQ") || e.includes("NMS") || e.includes("NGM") || e.includes("NCM") || e.includes("GS")) return "NASDAQ";
  if (e.includes("NYSE") || e === "NYQ") return "NYSE";
  if (e.includes("AMEX") || e.includes("NYSE AMERICAN") || e === "ASE") return "NYSEAMERICAN";
  if (e.includes("TSXV") || e.includes("CVE")) return "TSXV";
  if (e.includes("TSX") || e.includes("TOR")) return "TSX";
  return null;
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
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean; diagnostics: Record<string, unknown> }> {
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
async function collectGlobeNewswireEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number,
  exchange?: string | null
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean }> {
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
async function collectFinnhubEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean; diagnostics: Record<string, unknown> }> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const rejectionCounts: Record<string, number> = {};
  const diagnostics: Record<string, unknown> = {
    rawCount: 0,
    retrievedCount: 0,
    acceptedCount: 0,
    collectedCount: 0,
    rejectedCount: 0,
    rejectionCounts,
    attempted: true,
    completed: false,
  };
  const reject = (reason: string): void => {
    rejectionCounts[reason] = (rejectionCounts[reason] ?? 0) + 1;
    diagnostics.rejectedCount = Number(diagnostics.rejectedCount ?? 0) + 1;
  };
  const token = getWorkerVar("FINNHUB_API_KEY") ?? getWorkerVar("FINNHUB_TOKEN");
  if (!token) {
    warnings.push({ code: "SOURCE_UNAVAILABLE", message: "Finnhub company-news source is not configured; skipped.", severity: "warning" });
    diagnostics.attempted = false;
    return { items, warnings, used: false, diagnostics };
  }
  try {
    const now = new Date();
    const from = new Date(now.getTime() - (lookbackDays ?? 14) * 86400000).toISOString().slice(0, 10);
    const to = now.toISOString().slice(0, 10);
    const url = `${FINNHUB_COMPANY_NEWS_API}?symbol=${encodeURIComponent(ticker.toUpperCase())}&from=${from}&to=${to}`;
    const resp = await providerFetch(url, { headers: { "User-Agent": UA, "X-Finnhub-Token": token } });
    if (!resp.ok) {
      await resp.body?.cancel();
      if (resp.status === 401 || resp.status === 403) {
        throw new Error(`FINNHUB_AUTH_ERROR:${resp.status}`);
      } else if (resp.status === 429) {
        throw new Error("FINNHUB_RATE_LIMITED");
      } else {
        throw new Error(`HTTP ${resp.status}`);
      }
    }
    const rawJson = await resp.json();
    if (!Array.isArray(rawJson)) {
      throw new Error(`FINNHUB_PROVIDER_CHANGED: expected array, got ${typeof rawJson}`);
    }
    const news = rawJson as unknown[];
    diagnostics.rawCount = news.length;
    diagnostics.retrievedCount = news.length;
    const tickerU = ticker.toUpperCase();
    for (const n of news) {
      if (!n || typeof n !== "object" || Array.isArray(n)) {
        reject("INVALID_ROW");
        continue;
      }
      const row = n as Record<string, unknown>;
      const title = _str(row.headline).trim();
      if (!title) {
        reject("MISSING_HEADLINE");
        continue;
      }
      const summary = _str(row.summary).trim();
      const originalSource = _str(row.source).trim() || null;
      const urlStr = _str(row.url).trim() || null;
      const publishedAt = normalizeIso(row.datetime);
      const duplicateGroupId = makeDupGroupId(tickerU, title, publishedAt, null);
      if (!duplicateGroupId) {
        warnings.push({ code: "DEDUPE_WEAK_KEY", message: "Weak dedupe key for at least one item.", severity: "warning" });
      }
      const relevance = `${title} ${summary}`.toUpperCase().includes(tickerU) ? "HIGH" : "LOW";
      const item: Record<string, unknown> = {
        title,
        source: "finnhub",
        originalSource,
        sourceType: "company_news",
        publishedAt,
        retrievedAt,
        url: urlStr,
        issuer: null,
        tickers: [tickerU],
        eventType: eventTypeFromKeywords(title, summary),
        summary: shortText(summary || title, 240),
        evidenceText: shortText(summary || title, 180),
        confidence: urlStr ? "MEDIUM" : "LOW",
        tickerRelevance: relevance,
        decisionUse: sourceDecisionUse("company_news"),
        duplicateGroupId,
      };
      if (!withinDateWindow(_str(item.publishedAt) || null, startDate, endDate, lookbackDays)) {
        reject("DATE_OUTSIDE_WINDOW");
        continue;
      }
      diagnostics.acceptedCount = Number(diagnostics.acceptedCount ?? 0) + 1;
      if (items.length < maxResults) items.push(item);
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (msg.startsWith("FINNHUB_AUTH_ERROR")) {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: `Finnhub auth error: ${msg}`, severity: "warning" });
    } else if (msg.startsWith("FINNHUB_RATE_LIMITED")) {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: "Finnhub rate limited: HTTP 429", severity: "warning" });
    } else if (msg.startsWith("FINNHUB_PROVIDER_CHANGED")) {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: `Finnhub provider changed: ${msg}`, severity: "warning" });
    } else {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: `Finnhub source unavailable: ${msg}`, severity: "warning" });
    }
    return { items, warnings, used: false, diagnostics };
  }
  diagnostics.collectedCount = items.length;
  diagnostics.completed = true;
  return { items, warnings, used: true, diagnostics };
}
async function collectMarketauxEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number,
  fallbackReason = "EXPLICIT_SOURCE_SELECTION",
  searchQuery = "",
  expectedExchange: string | null = null,
): Promise<{ items: Record<string, unknown>[]; warnings: Record<string, unknown>[]; used: boolean; diagnostics: Record<string, unknown> }> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const limit = Math.min(MARKETAUX_MAX_ITEMS, Math.max(1, maxResults));
  const rejectionCounts: Record<string, number> = {};
  const diagnostics: Record<string, unknown> = {
    status: "PROVIDER_ERROR",
    rawCount: 0,
    retrievedCount: 0,
    acceptedCount: 0,
    collectedCount: 0,
    rejectedCount: 0,
    rejectionCounts,
    attempted: true,
    completed: false,
    fallbackReason,
    domains: [...MARKETAUX_WIRE_DOMAINS],
    quotaPolicy: {
      dailyLimit: 100,
      maxArticlesPerRequest: MARKETAUX_MAX_ITEMS,
      pagesRequested: 1,
      automaticPagination: false,
    },
  };
  const reject = (reason: string): void => {
    rejectionCounts[reason] = (rejectionCounts[reason] ?? 0) + 1;
    diagnostics.rejectedCount = Number(diagnostics.rejectedCount ?? 0) + 1;
  };
  const now = new Date();
  const fromDay = startDate || new Date(now.getTime() - (lookbackDays ?? 14) * 86400000).toISOString().slice(0, 10);
  const toDay = endDate || now.toISOString().slice(0, 10);
  const providerParams: Record<string, unknown> = {
    symbols: ticker.toUpperCase(),
    filter_entities: true,
    language: "en",
    domains: MARKETAUX_WIRE_DOMAINS.join(","),
    published_after: `${fromDay}T00:00:00`,
    published_before: `${toDay}T23:59:59`,
    limit,
    page: 1,
  };
  if (searchQuery.trim()) providerParams.search = searchQuery.trim();
  const provider = await fetchMarketauxJson(providerParams);
  diagnostics.providerUrl = provider.publicUrl;
  diagnostics.cacheStatus = provider.cacheStatus;
  diagnostics.attempted = provider.providerAttempted;
  if (provider.status !== "OK" || !provider.payload) {
    const status = provider.status === "SOURCE_UNCONFIGURED"
      ? "UNCONFIGURED"
      : provider.status === "RATE_LIMIT"
        ? "RATE_LIMITED"
        : provider.status;
    diagnostics.status = status;
    const message = status === "UNCONFIGURED"
      ? "Marketaux wire fallback is not configured; set MARKETAUX_API_TOKEN to enable it."
      : `Marketaux wire fallback unavailable (${status}).`;
    warnings.push({ code: "SOURCE_UNAVAILABLE", message, severity: "warning", source: "marketaux" });
    return { items, warnings, used: false, diagnostics };
  }
  const rawItems = provider.payload.data;
  if (!Array.isArray(rawItems)) {
    diagnostics.status = "PROVIDER_CHANGED";
    warnings.push({ code: "SOURCE_UNAVAILABLE", message: "Marketaux provider changed: expected a data array.", severity: "warning", source: "marketaux" });
    return { items, warnings, used: false, diagnostics };
  }
  diagnostics.rawCount = rawItems.length;
  diagnostics.retrievedCount = rawItems.length;
  const tickerU = ticker.toUpperCase();
  for (const candidate of rawItems) {
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
      reject("INVALID_ROW");
      continue;
    }
    const row = candidate as Record<string, unknown>;
    const title = _str(row.title).trim();
    const url = _str(row.url).trim();
    if (!title) {
      reject("MISSING_HEADLINE");
      continue;
    }
    let host = "";
    try {
      const parsed = new URL(url);
      if (parsed.protocol !== "https:") throw new Error("invalid protocol");
      host = parsed.hostname.toLowerCase();
    } catch {
      reject("INVALID_URL");
      continue;
    }
    if (!MARKETAUX_WIRE_DOMAINS.some(domain => host === domain || host.endsWith(`.${domain}`))) {
      reject("DOMAIN_NOT_ALLOWED");
      continue;
    }
    const entities = Array.isArray(row.entities) ? row.entities : [];
    const matchedEntity = entities.find(entity => (
      !!entity && typeof entity === "object" && !Array.isArray(entity)
      && _str((entity as Record<string, unknown>).symbol).toUpperCase() === tickerU
    )) as Record<string, unknown> | undefined;
    if (!matchedEntity) {
      reject("ENTITY_SYMBOL_MISMATCH");
      continue;
    }
    const expectedExchangeNorm = normalizeExchange(expectedExchange);
    const providerExchangeNorm = normalizeExchange(_str(matchedEntity.exchange));
    if (expectedExchangeNorm && providerExchangeNorm && expectedExchangeNorm !== providerExchangeNorm) {
      reject("ENTITY_EXCHANGE_MISMATCH");
      continue;
    }
    const publishedAt = toIsoUtc(row.published_at);
    if (!withinDateWindow(publishedAt, startDate, endDate, lookbackDays)) {
      reject("DATE_OUTSIDE_WINDOW");
      continue;
    }
    const summary = _str(row.snippet) || _str(row.description) || title;
    const duplicateGroupId = makeDupGroupId(tickerU, title, publishedAt, null);
    items.push({
      title,
      source: "marketaux",
      originalSource: _str(row.source) || host,
      sourceType: "marketaux_wire",
      provider: "marketaux",
      discoveredVia: "marketaux_api",
      publishedAt,
      retrievedAt,
      url,
      issuer: _str(matchedEntity.name) || null,
      tickers: [tickerU],
      matchBasis: "PROVIDER_ENTITY_SYMBOL",
      sourceTickerMatch: true,
      eventType: eventTypeFromKeywords(title, summary),
      summary: shortText(summary, 240),
      evidenceText: shortText(summary, 180),
      confidence: "LOW",
      tickerRelevance: "HIGH",
      duplicateGroupId,
      providerArticleId: _str(row.uuid) || null,
      providerRelevanceScore: matchedEntity.match_score ?? null,
    });
    diagnostics.acceptedCount = Number(diagnostics.acceptedCount ?? 0) + 1;
    if (items.length >= limit) break;
  }
  diagnostics.status = Number(diagnostics.acceptedCount ?? 0) > 0 ? "OK" : "EMPTY_RESULT";
  diagnostics.collectedCount = items.length;
  diagnostics.completed = true;
  return { items, warnings, used: true, diagnostics };
}
function marketauxFallbackReason(
  selected: string[],
  items: Record<string, unknown>[],
  warnings: Record<string, unknown>[],
  sourceDiagnostics: Record<string, unknown>,
  searchQuery = "",
): string | null {
  const prioritySources = new Set(["yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"]);
  const selectedPriority = selected.filter(source => prioritySources.has(source));
  if (selectedPriority.length === 0) return "EXPLICIT_SOURCE_SELECTION";
  const query = searchQuery.trim().toLowerCase();
  const hasPriorityItems = items.some(item => {
    const isPriority = prioritySources.has(_str(item.source))
      || prioritySources.has(_str(item.sourceType))
      || _str(item.sourceType) === "company_news";
    if (!isPriority || !query) return isPriority;
    const text = `${_str(item.title)} ${_str(item.summary)} ${_str(item.source)} ${_str(item.eventType)} ${_str(item.evidenceText)}`.toLowerCase();
    return text.includes(query);
  });
  let limited = warnings.some(warning => {
    const code = _str(warning.code);
    const sourceOrMessage = `${_str(warning.source)} ${_str(warning.message)}`.toLowerCase();
    return code === "SOURCE_IDENTITY_UNAVAILABLE"
      || (["SOURCE_UNAVAILABLE", "SOURCE_NOT_ELIGIBLE"].includes(code) && sourceOrMessage.includes("finnhub"))
      || (code === "SOURCE_UNAVAILABLE" && sourceOrMessage.includes("yahoo finance"));
  });
  for (const source of selectedPriority) {
    const value = sourceDiagnostics[source];
    const diagnostic = value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
    if (diagnostic && (
      diagnostic.completed === false
      || ["UNCONFIGURED", "NOT_ELIGIBLE", "PROVIDER_ERROR", "AUTH_ERROR", "RATE_LIMITED", "TIMEOUT", "PROVIDER_CHANGED"].includes(_str(diagnostic.status))
    )) limited = true;
  }
  if (limited) return "HIGHER_PRIORITY_COVERAGE_LIMITED";
  if (!hasPriorityItems) return "HIGHER_PRIORITY_EMPTY";
  return null;
}
async function collectCompanyEvents(
  ticker: string,
  {
    maxResults = 10,
    lookbackDays = 14,
    startDate = "",
    endDate = "",
    sources,
    secFilingTypes = ["8-K", "10-Q", "10-K", "S-3", "DEF14A"],
    searchQuery = "",
    keepAll = false,
  }: {
    maxResults?: number;
    lookbackDays?: number;
    startDate?: string;
    endDate?: string;
    sources?: string[];
    secFilingTypes?: string[];
    searchQuery?: string;
    /** Keep every deduplicated item instead of the newest maxResults (event verification matches before capping). */
    keepAll?: boolean;
  } = {}
): Promise<CompanyEventCollectionResult> {
  const safeMax = clampInt(maxResults, 10, 1, 100);
  const safeLookback = clampInt(lookbackDays, 14, 1, 3650);
  const watermark = new Date().toISOString();
  const { selected, warnings: sourceWarnings } = normalizeSources(sources, [
    "sec", "company_ir_page", "company_ir",
    "yahoo_finance", "yahoo_finance_news", "yahoo_finance_press_releases",
    "finnhub", "marketaux",
  ]);
  const warnings: Record<string, unknown>[] = [...sourceWarnings];
  const items: Record<string, unknown>[] = [];
  const sourcesUsed: string[] = [];
  const sourceDiagnostics: Record<string, unknown> = {};
  // Each source is collected in its own scope, so a fetch the subrequest budget
  // refused is charged to the source that asked for it (2.11.0).
  const budgetStarved = new Set<string>();
  const watched = async <T>(charged: string[], fn: () => Promise<T>): Promise<T> => {
    const { result, starved } = await withBudgetWatch(fn);
    if (starved) for (const source of charged) budgetStarved.add(source);
    return result;
  };
  let eventIdentity: NewsCompanyIdentity | null = null;
  const getEventIdentity = async (): Promise<NewsCompanyIdentity | null> => {
    if (eventIdentity) return eventIdentity;
    try {
      eventIdentity = await watched(["yahoo_finance_identity"], () => resolveNewsCompanyIdentity(ticker));
    } catch {
      eventIdentity = null;
    }
    return eventIdentity;
  };

  if (selected.includes("sec")) {
    const sec = await watched(["sec"], () => collectSecEvents(ticker, secFilingTypes, safeMax, watermark, startDate, endDate, safeLookback));
    if (sec.used) sourcesUsed.push("sec");
    items.push(...sec.items);
    warnings.push(...sec.warnings);
  }

  if (selected.includes("company_ir")) {
    const ir = await watched(["company_ir"], () => collectCompanyIrEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback));
    sourceDiagnostics.company_ir = ir.diagnostics;
    if (ir.used) sourcesUsed.push("company_ir");
    items.push(...ir.items);
    warnings.push(...ir.warnings);
  }

  if (selected.includes("company_ir_page")) {
    const page = await watched(["company_ir_page"], () => collectCompanyIrPageEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback));
    sourceDiagnostics.company_ir_page = page.diagnostics;
    if (page.used) sourcesUsed.push("company_ir_page");
    items.push(...page.items);
    warnings.push(...page.warnings);
  }

  // Yahoo Finance news tab (explicit or via legacy yahoo_finance).
  // company_ir is served by the approved company IR registry above; Yahoo
  // sources remain selectable separately as yahoo_finance_news / _press_releases.
  const needYfNews = selected.includes("yahoo_finance_news")
    || selected.includes("yahoo_finance");
  const needYfPr = selected.includes("yahoo_finance_press_releases") || selected.includes("yahoo_finance");
  const needYahooIdentity = needYfNews || needYfPr;
  const yahooIdentity = needYahooIdentity ? yahooNewsIdentityFor(await getEventIdentity()) : null;
  const yahooDiagnostics: Record<string, unknown>[] = [];
  if (yahooIdentity?.status === "UNAVAILABLE") {
    sourceDiagnostics.yahoo_finance_identity = {
      status: "IDENTITY_UNAVAILABLE",
      attempted: true,
      reasonCode: "YAHOO_PROFILE_IDENTITY_UNAVAILABLE",
      allowedAction: "retry_or_use_explicit_ticker_matches_only",
    };
    warnings.push({
      code: "SOURCE_IDENTITY_UNAVAILABLE",
      message: "Yahoo company identity lookup was unavailable; only exact ticker-token matches were retained.",
      severity: "warning",
      source: "yahoo_finance_identity",
    });
  }
  if (needYfNews) {
    const yf = await watched(["yahoo_finance_news", "yahoo_finance"], () => collectYahooEvents(ticker, safeMax, watermark, yahooIdentity!, startDate, endDate, safeLookback, "news"));
    const diagnostic = { ...yf.diagnostics, attempted: true, completed: yf.used };
    sourceDiagnostics.yahoo_finance_news = diagnostic;
    yahooDiagnostics.push(diagnostic);
    if (yf.used) {
      if (selected.includes("yahoo_finance_news")) sourcesUsed.push("yahoo_finance_news");
      if (selected.includes("yahoo_finance") && !sourcesUsed.includes("yahoo_finance")) sourcesUsed.push("yahoo_finance");
    }
    for (const item of yf.items) {
      const src = _str(item.source);
      if (selected.includes("yahoo_finance_news") && src === "yahoo_finance_news") items.push(item);
      else if (selected.includes("yahoo_finance") && ["yahoo_finance_news", "yahoo_finance_press_releases"].includes(src)) items.push(item);
    }
    warnings.push(...yf.warnings);
  }

  // Yahoo Finance press releases tab (explicit or via legacy yahoo_finance)
  if (needYfPr) {
    const pr = await watched(["yahoo_finance_press_releases", "yahoo_finance"], () => collectYahooEvents(ticker, safeMax, watermark, yahooIdentity!, startDate, endDate, safeLookback, "press_releases"));
    const diagnostic = { ...pr.diagnostics, attempted: true, completed: pr.used };
    sourceDiagnostics.yahoo_finance_press_releases = diagnostic;
    yahooDiagnostics.push(diagnostic);
    if (pr.used && selected.includes("yahoo_finance_press_releases") && !sourcesUsed.includes("yahoo_finance_press_releases")) {
      sourcesUsed.push("yahoo_finance_press_releases");
    }
    items.push(...pr.items);
    warnings.push(...pr.warnings);
  }

  if (selected.includes("yahoo_finance") && yahooDiagnostics.length) {
    const rejectionCounts: Record<string, number> = {};
    for (const diagnostic of yahooDiagnostics) {
      for (const [reason, count] of Object.entries((diagnostic.rejectionCounts as Record<string, number>) ?? {})) {
        rejectionCounts[reason] = (rejectionCounts[reason] ?? 0) + count;
      }
    }
    sourceDiagnostics.yahoo_finance = {
      rawCount: yahooDiagnostics.reduce((sum, diagnostic) => sum + Number(diagnostic.rawCount ?? 0), 0),
      retrievedCount: yahooDiagnostics.reduce((sum, diagnostic) => sum + Number(diagnostic.retrievedCount ?? 0), 0),
      filteredCount: yahooDiagnostics.reduce((sum, diagnostic) => sum + Number(diagnostic.filteredCount ?? 0), 0),
      acceptedCount: yahooDiagnostics.reduce((sum, diagnostic) => sum + Number(diagnostic.acceptedCount ?? 0), 0),
      rejectedCount: yahooDiagnostics.reduce((sum, diagnostic) => sum + Number(diagnostic.rejectedCount ?? 0), 0),
      rejectionCounts,
      identityStatus: yahooIdentity?.status ?? "UNAVAILABLE",
      attempted: true,
      completed: yahooDiagnostics.every(diagnostic => diagnostic.completed === true),
    };
  }

  if (selected.includes("finnhub") && finnhubEligibility(ticker).eligible) {
    const finnhub = await watched(["finnhub"], () => collectFinnhubEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback));
    sourceDiagnostics.finnhub = finnhub.diagnostics;
    if (finnhub.used) sourcesUsed.push("finnhub");
    items.push(...finnhub.items);
    warnings.push(...finnhub.warnings);
  } else if (selected.includes("finnhub")) {
    const eligibility = finnhubEligibility(ticker);
    sourceDiagnostics.finnhub = {
      status: "NOT_ELIGIBLE",
      attempted: false,
      reasonCode: eligibility.reasonCode,
      allowedAction: "use_yahoo_or_official_sources",
    };
    warnings.push({
      code: "SOURCE_NOT_ELIGIBLE",
      message: `Finnhub company-news is intentionally skipped for ${ticker.toUpperCase()} under the deployed market capability policy.`,
      severity: "warning",
      source: "finnhub",
      attempted: false,
      reasonCode: eligibility.reasonCode,
    });
  }

  if (selected.includes("marketaux")) {
    const fallbackReason = marketauxFallbackReason(selected, items, warnings, sourceDiagnostics, searchQuery);
    if (fallbackReason === null) {
      sourceDiagnostics.marketaux = {
        status: "NOT_NEEDED",
        attempted: false,
        completed: true,
        reasonCode: "HIGHER_PRIORITY_EVIDENCE_FOUND",
        fallbackReason: null,
        domains: [...MARKETAUX_WIRE_DOMAINS],
        quotaPolicy: {
          dailyLimit: 100,
          maxArticlesPerRequest: MARKETAUX_MAX_ITEMS,
          pagesRequested: 0,
          automaticPagination: false,
        },
      };
    } else {
      const marketaux = await watched(["marketaux"], () => collectMarketauxEvents(
        ticker,
        safeMax,
        watermark,
        startDate,
        endDate,
        safeLookback,
        fallbackReason,
        searchQuery,
        yahooIdentity?.exchange ?? null,
      ));
      sourceDiagnostics.marketaux = marketaux.diagnostics;
      if (marketaux.used) sourcesUsed.push("marketaux");
      items.push(...marketaux.items);
      warnings.push(...marketaux.warnings);
    }
  }

  // GlobeNewswire RSS is an explicit shallow snapshot and intentionally runs
  // after bounded API sources so cold feed latency cannot delay them.
  if (selected.includes("newswire")) {
    const identity = await getEventIdentity();
    const gnw = await watched(["newswire"], () => collectGlobeNewswireEvents(ticker, safeMax, watermark, startDate, endDate, safeLookback, identity?.exchange ?? null));
    if (gnw.used) sourcesUsed.push("newswire");
    items.push(...gnw.items);
    warnings.push(...gnw.warnings);
  }

  const allDeduped = dedupeEventItems(items, warnings);
  const deduped = (keepAll ? allDeduped : allDeduped.slice(0, safeMax)).map(enrichNewsItemForLlm);
  const uniqueWarnings: Record<string, unknown>[] = [];
  const warningKeys = new Set<string>();
  for (const w of warnings) {
    const key = `${_str(w.code)}|${_str(w.message)}`;
    if (warningKeys.has(key)) continue;
    warningKeys.add(key);
    uniqueWarnings.push(w);
  }
  return { items: deduped, sourcesUsed, warnings: uniqueWarnings, watermark, sourceDiagnostics, budgetStarvedSources: [...budgetStarved] };
}
// ─── Public event / news tools ─────────────────────────────────────────────────

const NEWS_RELEVANCE_RANK: Record<string, number> = { HIGH: 0, MEDIUM: 1, LOW: 2 };
/** Items ordered by tickerRelevance (HIGH, MEDIUM, LOW), keeping their order (newest first) within each level. */
export function rankNewsItemsByRelevance(items: Record<string, unknown>[]): Record<string, unknown>[] {
  const rank = (item: Record<string, unknown>): number => NEWS_RELEVANCE_RANK[_str(item.tickerRelevance).toUpperCase()] ?? 1;
  return items.map((item, index) => ({ item, index }))
    .sort((a, b) => rank(a.item) - rank(b.item) || a.index - b.index)
    .map(({ item }) => item);
}
export async function getCompanyNews(
  ticker: string | string[],
  maxResults = 10,
  lookbackDays = 14,
  requestedSources: string[] | null = null,
): Promise<string> {
  const sources = requestedSources ?? DEFAULT_NEWS_SOURCES;
  if (
    (typeof ticker === "string" && ticker.trim() === "")
    || (Array.isArray(ticker) && (ticker.length === 0 || ticker.some((item) => typeof item !== "string" || item.trim() === "")))
  ) {
    return JSON.stringify({
      error: true,
      code: "INPUT_VALIDATION_ERROR",
      message: "ticker is required and must not contain empty symbols.",
    });
  }
  ticker = Array.isArray(ticker) ? ticker.map((item) => item.trim()) : ticker.trim();
  // Batch path: fetch each ticker independently and return a per-ticker keyed
  // object (a union of results), matching the other multi-ticker tools. News is
  // fetched per ticker; there is no combined query that could zero out the
  // whole batch under low-news conditions.
  if (Array.isArray(ticker)) {
    return runPartialBatch(ticker, (t) => getCompanyNews(t, maxResults, lookbackDays, requestedSources));
  }
  // Collect a wider pool, then keep the most relevant items: an item that
  // only mentions the company in passing should not displace one about it.
  const safeMax = clampInt(maxResults, 10, 1, 100);
  const out = await collectCompanyEvents(ticker, { maxResults: Math.min(100, safeMax * 3), lookbackDays, sources });
  out.items = rankNewsItemsByRelevance(out.items).slice(0, safeMax);
  const status = collectionStatus(out.items, out.sourcesUsed, out.warnings);
  const sourceStatus = computeSourceStatus(out.sourcesUsed, out.warnings, out.items, sources, out.sourceDiagnostics, out.budgetStarvedSources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    items: out.items,
    meta: { sourcesUsed: out.sourcesUsed, deduped: true, watermark: out.watermark },
    warnings: out.warnings,
    sourceCoverage,
    coverage,
    sourceStatus,
    ...sourceRequestFields(requestedSources, sourceStatus),
  };
  if (status) payload.status = status;
  return JSON.stringify(markNoSourceSelected(payload, requestedSources));
}
export async function searchCompanyNews(
  ticker: string,
  query: string,
  startDate = "",
  endDate = "",
  requestedSources: string[] | null = null,
  maxResults = 10
): Promise<string> {
  const sources = requestedSources ?? DEFAULT_NEWS_SOURCES;
  // The query filters the full collected pool; filtering only the first
  // max_results items found almost nothing.
  const out = await collectCompanyEvents(ticker, {
    maxResults: 100,
    lookbackDays: 14,
    startDate,
    endDate,
    sources,
    searchQuery: query,
  });
  const q = query.toLowerCase().trim();
  const matchedAll = q
    ? out.items.filter(item => `${_str(item.title)} ${_str(item.summary)} ${_str(item.source)} ${_str(item.eventType)} ${_str(item.evidenceText)}`.toLowerCase().includes(q))
    : out.items;
  const matched = rankNewsItemsByRelevance(matchedAll).slice(0, clampInt(maxResults, 10, 1, 100));
  const status = collectionStatus(matched, out.sourcesUsed, out.warnings);
  const sourceStatus = computeSourceStatus(out.sourcesUsed, out.warnings, matched, sources, out.sourceDiagnostics, out.budgetStarvedSources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    query,
    matchCount: matchedAll.length,
    items: matched,
    meta: { sourcesUsed: out.sourcesUsed, deduped: true, watermark: out.watermark },
    warnings: out.warnings,
    sourceCoverage,
    coverage,
    sourceStatus,
    ...sourceRequestFields(requestedSources, sourceStatus),
  };
  if (status) payload.status = status;
  return JSON.stringify(markNoSourceSelected(payload, requestedSources));
}
export async function getCompanyPressReleases(
  ticker: string,
  lookbackDays = 90,
  maxResults = 20,
  requestedSources: string[] | null = null
): Promise<string> {
  const sources = requestedSources ?? DEFAULT_PRESS_RELEASE_SOURCES;
  const safeMax = clampInt(maxResults, 20, 1, 100);
  const safeLookback = clampInt(lookbackDays, 90, 1, 3650);
  const { selected, warnings: sourceWarnings } = normalizeSources(sources, ["sec", "company_ir_page", "yahoo_finance_press_releases"]);
  const primarySources = selected.filter(src => src === "sec" || src === "company_ir_page");
  const optionalSources = selected.filter(src => src !== "sec" && src !== "company_ir_page");
  const selectedHasCompanyIr = selected.includes("company_ir");
  const emptyCollection = (): CompanyEventCollectionResult => ({
    items: [],
    sourcesUsed: [],
    warnings: [],
    watermark: new Date().toISOString(),
    sourceDiagnostics: {},
  });
  const primary = primarySources.length > 0
    ? await collectCompanyEvents(ticker, {
      maxResults: safeMax,
      lookbackDays: safeLookback,
      sources: primarySources,
      secFilingTypes: ["8-K"],
    })
    : emptyCollection();
  const releaseTypes = new Set(["company_ir", "company_ir_page", "press_release", "newswire", "marketaux_wire", "sec_filing", "sec_ex99_found", "yahoo_finance_press_releases"]);
  const primaryItems = primary.items.filter(it => releaseTypes.has(_str(it.sourceType)));

  let hasSecEx99Found = false;
  const sec8kEvidence: Record<string, unknown>[] = [];
  let sec8kWithoutEx99Count = 0;
  const processedPrimaryItems: Record<string, unknown>[] = [];
  for (const it of primaryItems) {
    if (_str(it.sourceType) === "sec_filing" && _str(it.filingType) === "8-K") {
      const accession = _str(it.accessionNumber);
      const url = _str(it.url);
      const evidence: Record<string, unknown> = {
        filingType: "8-K",
        filingDate: it.filingDate ?? null,
        acceptedAt: it.acceptedAt ?? null,
        accessionNumber: accession || null,
        cikInt: it.cikInt ?? null,
        documentUrl: url || null,
      };
      sec8kEvidence.push(evidence);
      const cikFromItem = Number.parseInt(_str(it.cikInt), 10);
      const cikFromAccession = accession ? edgarCikFromAccession(accession) : null;
      const cikMatch = /\/data\/(\d+)\//.exec(url);
      const cikFromUrl = cikMatch ? Number.parseInt(cikMatch[1], 10) : null;
      // The accession prefix names the filer agent, so it comes last.
      const cik = [cikFromItem, cikFromUrl, cikFromAccession]
        .find((value): value is number => typeof value === "number" && Number.isFinite(value) && value > 0) ?? null;
      if (accession && cik !== null) {
        const ex991Url = await resolveEx991Url(cik, accession);
        if (ex991Url) {
          hasSecEx99Found = true;
          evidence.ex991Url = ex991Url;
          evidence.ex991Resolved = true;
          processedPrimaryItems.push({
            ...it,
            sourceType: "sec_ex99_found",
            url: ex991Url,
            title: "EX-99.1 exhibit found in 8-K",
            eventType: "press_release",
            evidenceText: "Resolved EX-99.1 press-release exhibit from SEC 8-K.",
            confidence: "HIGH",
            decisionUse: sourceDecisionUse("sec_ex99_found"),
          });
          continue;
        }
      }
      sec8kWithoutEx99Count += 1;
    }
    processedPrimaryItems.push(it);
  }

  const supplemental = optionalSources.length > 0
    ? await collectCompanyEvents(ticker, {
      maxResults: safeMax,
      lookbackDays: safeLookback,
      sources: optionalSources,
      secFilingTypes: ["8-K"],
    })
    : emptyCollection();
  const warnings = [...sourceWarnings, ...primary.warnings, ...supplemental.warnings];
  const warningKeys = new Set<string>();
  const uniqueWarnings: Record<string, unknown>[] = [];
  for (const w of warnings) {
    const key = `${_str(w.code)}|${_str(w.message)}|${_str(w.feedUrl)}|${_str(w.canonicalUrl)}`;
    if (warningKeys.has(key)) continue;
    warningKeys.add(key);
    uniqueWarnings.push(w);
  }
  const supplementalItems = supplemental.items.filter(it => releaseTypes.has(_str(it.sourceType)));
  const processedItems = dedupeEventItems([...processedPrimaryItems, ...supplementalItems], uniqueWarnings);
  const sourcesUsed = [...new Set([...primary.sourcesUsed, ...supplemental.sourcesUsed])];
  const sourceDiagnostics = { ...primary.sourceDiagnostics, ...supplemental.sourceDiagnostics };
  const hasApprovedIrPageResolved = processedItems.some(it =>
    _str(it.sourceType) === "company_ir_page" && _str(it.approvalStatus) === "approved" && !!_str(it.url)
  );
  const irPageEvidence = processedItems
    .filter(it => _str(it.sourceType) === "company_ir_page" && _str(it.approvalStatus) === "approved")
    .slice(0, 10)
    .map(it => ({
      title: it.title ?? null,
      publishedAt: it.publishedAt ?? null,
      url: it.url ?? null,
      canonicalUrl: it.canonicalUrl ?? null,
      adapter: it.adapter ?? null,
      registryVersion: it.registryVersion ?? null,
      basis: "approved_company_ir_page_registry",
    }));
  const companyIrPageStatus = sourceDiagnostics.company_ir_page && typeof sourceDiagnostics.company_ir_page === "object"
    ? sourceDiagnostics.company_ir_page as Record<string, unknown>
    : {};
  const candidateSources = Array.isArray(companyIrPageStatus.candidates) ? companyIrPageStatus.candidates : [];

  if (processedItems.length === 0) {
    uniqueWarnings.push({
      code: "NO_OFFICIAL_RELEASE_SOURCE",
      message: "No company-originated or official release source found in requested window.",
      severity: "warning",
    });
  }
  if (!hasSecEx99Found && sec8kWithoutEx99Count > 0) {
    uniqueWarnings.push({
      code: "SEC_8K_FOUND_EX99_NOT_FOUND",
      message: "SEC 8-K filing(s) were found, but no EX-99.1 press-release exhibit was resolved.",
      severity: "warning",
      filingsSearched: sec8kWithoutEx99Count,
    });
  }

  let status: string | null = null;
  if (hasSecEx99Found) {
    status = "SEC_EX99_FOUND";
  } else if (hasApprovedIrPageResolved) {
    status = "APPROVED_IR_PAGE_FOUND";
  } else if (sec8kWithoutEx99Count > 0) {
    status = "SEC_8K_FOUND_EX99_NOT_FOUND";
  } else if (processedItems.length === 0) {
    if (selected.includes("yahoo_finance_press_releases")) {
      status = "NO_YAHOO_PRESS_RELEASE";
    } else if (selected.includes("company_ir_page")) {
      status = candidateSources.length > 0 ? "COMPANY_IR_PAGE_CANDIDATE_ONLY" : "COMPANY_IR_PAGE_NOT_APPROVED";
    } else if (selectedHasCompanyIr) {
      status = "COMPANY_IR_NOT_FOUND";
    } else {
      status = "NOT_FOUND";
    }
  } else {
    status = collectionStatus(processedItems, sourcesUsed, uniqueWarnings);
  }

  const coverageStatus = hasSecEx99Found ? "SEC_EX99_RESOLVED"
    : hasApprovedIrPageResolved ? "APPROVED_IR_PAGE_RESOLVED"
    : sec8kWithoutEx99Count > 0 ? "SEC_8K_FOUND_EX99_NOT_FOUND"
    : processedItems.length > 0 ? "OFFICIAL_RELEASE_SOURCE_FOUND"
    : "NO_OFFICIAL_RELEASE_SOURCE";
  const payloadDecisionGrade = hasSecEx99Found || hasApprovedIrPageResolved;
  const decisionGradeBasis = payloadDecisionGrade
    ? hasSecEx99Found
      ? "Resolved SEC 8-K EX-99 press-release exhibit for this call."
      : "Resolved an approved, registry-reviewed company IR page source for this call."
    : "No resolved SEC EX-99 press-release exhibit in this call; use for verification/context only.";
  const budgetStarvedSources = [...new Set([...(primary.budgetStarvedSources ?? []), ...(supplemental.budgetStarvedSources ?? [])])];
  const sourceStatus = computeSourceStatus(sourcesUsed, uniqueWarnings, processedItems, selected, sourceDiagnostics, budgetStarvedSources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus, payloadDecisionGrade ? "USE_OFFICIAL_EVIDENCE" : null);

  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    items: processedItems.slice(0, safeMax),
    meta: { sourcesUsed, deduped: true, watermark: primary.watermark || supplemental.watermark },
    warnings: uniqueWarnings,
    sourceCoverage,
    coverage,
    sourceStatus,
    ...sourceRequestFields(requestedSources, sourceStatus),
    coverageStatus,
    decisionGrade: payloadDecisionGrade,
    decisionGradeBasis,
    capabilityStatus: "ACTIVE",
    doctrineUse: "ALLOWED",
    reliabilityClass: reliabilityClassFor("ALLOWED"),
    failureMode: payloadDecisionGrade ? null : coverageStatus,
  };
  if (sec8kEvidence.length > 0) payload.secEvidence = sec8kEvidence.slice(0, 10);
  if (irPageEvidence.length > 0) payload.irPageEvidence = irPageEvidence;
  if (candidateSources.length > 0) payload.candidateSources = candidateSources;
  if (status) payload.status = status;
  return JSON.stringify(markNoSourceSelected(payload, requestedSources));
}
export async function getSecRecentEvents(
  ticker: string,
  filingTypes: string[] = ["8-K", "10-Q", "10-K"],
  lookbackDays = 90,
  maxResults = 20
): Promise<string> {
  const watermark = new Date().toISOString();
  const { result: sec, starved } = await withBudgetWatch(() => collectSecEvents(ticker, filingTypes, clampInt(maxResults, 20, 1, 100), watermark, "", "", clampInt(lookbackDays, 90, 1, 3650)));
  const status = collectionStatus(sec.items, sec.used ? ["sec"] : [], sec.warnings);
  const sourceStatus = markBudgetStarvedSources(
    { sec: { status: sec.used ? (sec.items.length ? "OK" : "EMPTY_RESULT") : "PROVIDER_ERROR", rawCount: sec.items.length, filteredCount: sec.items.length } },
    starved ? ["sec"] : [],
  );
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    items: sec.items.map(enrichNewsItemForLlm),
    meta: { sourcesUsed: sec.used ? ["sec"] : [], watermark },
    warnings: sec.warnings,
    sourceCoverage,
    coverage,
    sourceStatus,
  };
  if (status) payload.status = status;
  return JSON.stringify(payload);
}
export async function getPublicEventTimeline(
  ticker: string,
  startDate = "",
  endDate = "",
  requestedSources: string[] | null = null,
  maxResults = 50,
  newestFirst = false
): Promise<string> {
  const sources = requestedSources ?? DEFAULT_EVENT_SOURCES;
  const out = await collectCompanyEvents(ticker, { maxResults, lookbackDays: 365, startDate, endDate, sources });
  const timeline = out.items
    .filter(item => _str(item.publishedAt))
    .map(item => ({
      timestamp: item.publishedAt,
      eventType: item.eventType,
      title: item.title,
      source: item.source,
      sourceType: item.sourceType,
      url: item.url,
      confidence: item.confidence,
      evidenceClass: item.evidenceClass,
      tickerMatch: item.tickerMatch,
      matchBasis: item.matchBasis ?? null,
      urlProvenance: item.urlProvenance,
      decisionUse: item.decisionUse ?? null,
      duplicateGroupId: item.duplicateGroupId,
      sourceRefs: item.sourceRefs ?? [],
    }))
    .sort((a, b) => newestFirst
      ? _str(b.timestamp).localeCompare(_str(a.timestamp))
      : _str(a.timestamp).localeCompare(_str(b.timestamp)))
    .slice(0, clampInt(maxResults, 50, 1, 100));
  const status = collectionStatus(out.items, out.sourcesUsed, out.warnings);
  const sourceStatus = computeSourceStatus(out.sourcesUsed, out.warnings, out.items, sources, out.sourceDiagnostics, out.budgetStarvedSources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus);
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    timeline,
    meta: { sourcesUsed: out.sourcesUsed, deduped: true, watermark: out.watermark },
    warnings: out.warnings,
    sourceCoverage,
    coverage,
    sourceStatus,
    ...sourceRequestFields(requestedSources, sourceStatus),
  };
  if (status) payload.status = status;
  return JSON.stringify(markNoSourceSelected(payload, requestedSources));
}
export async function verifyCompanyEvent(
  ticker: string,
  eventQuery: string,
  startDate = "",
  endDate = "",
  requestedSources: string[] | null = null
): Promise<string> {
  const sources = requestedSources ?? DEFAULT_EVENT_SOURCES;
  // Match against everything collected: a cap of the newest 50 let a flood of
  // aggregator items push the official release out before matching (ASTS
  // BlueBird launch, 2.4.5).
  const out = await collectCompanyEvents(ticker, { maxResults: 50, lookbackDays: 365, startDate, endDate, sources, keepAll: true });
  const normalizeEventText = (value: unknown): string =>
    _str(value).toLowerCase().replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim();
  const eventQueryStopwords = new Set([
    "announce", "announced", "announcement", "announcements", "announces",
    "company", "corporate", "latest", "news", "official", "press", "release",
    "report", "reported", "reports", "result", "results", "today", "update",
    "updated", "updates",
  ]);
  const normalizedQuery = normalizeEventText(eventQuery);
  const meaningfulTerms = normalizedQuery
    .split(" ")
    .filter(term => term.length >= 3 && !eventQueryStopwords.has(term));
  const requiredTermMatches = meaningfulTerms.length <= 2
    ? meaningfulTerms.length
    : Math.ceil(meaningfulTerms.length * 0.67);
  const queryMatch = (item: Record<string, unknown>): {
    matched: boolean;
    method: "PHRASE" | "TERM_THRESHOLD" | "NONE";
    matchedTerms: string[];
    requiredTermMatches: number;
  } => {
    const text = normalizeEventText(`${_str(item.title)} ${_str(item.summary)} ${_str(item.evidenceText)} ${_str(item.eventType)} ${_str(item.source)}`);
    // Stemmed, so "launch" matches "launches" and "launched" (extraction-rules.ts).
    const words = new Set(text.split(" ").filter(Boolean).map(stemWord));
    const matchedTerms = meaningfulTerms.filter(term => words.has(stemWord(term)));
    if (normalizedQuery && text.includes(normalizedQuery)) {
      return { matched: true, method: "PHRASE", matchedTerms, requiredTermMatches };
    }
    if (requiredTermMatches > 0 && matchedTerms.length >= requiredTermMatches) {
      return { matched: true, method: "TERM_THRESHOLD", matchedTerms, requiredTermMatches };
    }
    return { matched: false, method: "NONE", matchedTerms, requiredTermMatches };
  };
  if (meaningfulTerms.length === 0) {
    out.warnings.push({
      code: "EVENT_QUERY_TOO_GENERIC",
      message: "The event query contains no specific event terms. Add a term such as acquisition, dividend, guidance, contract, or offering.",
      severity: "warning",
    });
  }
  const matched = out.items.filter(item => queryMatch(item).matched);
  const inRange = matched.filter(item => {
    if (!startDate && !endDate) return true;
    return withinDateWindow(_str(item.publishedAt) || null, startDate, endDate);
  });
  const official = inRange.filter(item =>
    OFFICIAL_SOURCE_TYPES.has(_str(item.sourceType)) &&
    !!_str(item.url) &&
    ["HIGH", "MEDIUM"].includes(confidenceForSourceType(item.sourceType, item.confidence))
  );
  const staleCutoff = new Date(Date.now() - 90 * 86400000).toISOString().slice(0, 10);
  const staleOnly = matched.length > 0 && matched.every(item => _str(item.publishedAt).slice(0, 10) < staleCutoff);
  const matchedGroupIds = new Set(matched.map(item => _str(item.duplicateGroupId)).filter(Boolean));
  const conflicts: Record<string, unknown>[] = out.warnings
    .filter(w => _str(w.code) === "EVIDENCE_CONFLICT" && matchedGroupIds.has(_str(w.duplicateGroupId)))
    .map(w => ({
      type: "evidence",
      duplicateGroupId: _str(w.duplicateGroupId),
      message: _str(w.message, "Conflicting evidence observed for the matched event."),
    }));
  let status = "NOT_FOUND";
  if (conflicts.length > 0) status = "CONFLICTING";
  else if (official.length > 0) status = "CONFIRMED";
  else if (inRange.length > 0) status = "PARTIAL";
  else if (staleOnly) status = "STALE";

  let companyName = "";
  try {
    const priceD = await yGet(
      `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=price`
    ) as Record<string, unknown>;
    const priceResult = ((priceD?.quoteSummary as Record<string, unknown> | undefined)
      ?.result as Record<string, unknown>[] | undefined)?.[0]?.price as Record<string, unknown> | undefined;
    companyName = _str(priceResult?.shortName || priceResult?.longName);
  } catch { /* non-fatal */ }

  const extractBaseCompanyName = (name: string): string => {
    if (!name) return "";
    let nameClean = name.toUpperCase();
    const suffixes = [
      " INC", " CORP", " CORPORATION", " LTD", " LIMITED", " PLC",
      " CO", " COMPANY", " S.A.", " AG", " GMBH", " S.A.B. DE C.V.",
      " GROUP", " HOLDINGS", " TRUST"
    ];
    for (const p of [".", ",", "/"]) {
      nameClean = nameClean.split(p).join("");
    }
    for (const suff of suffixes) {
      if (nameClean.endsWith(suff)) {
        nameClean = nameClean.slice(0, -suff.length);
      }
    }
    return nameClean.trim();
  };

  const baseCompanyName = extractBaseCompanyName(companyName);
  const tickerUpper = ticker.toUpperCase();
  const tickerPattern = new RegExp(`\\b${tickerUpper}\\b`, "i");

  const relevanceScore = (ev: Record<string, unknown>): string => {
    // 1. Check explicit tickers list
    const evTickers = Array.isArray(ev.tickers) ? ev.tickers.map(String) : [];
    if (evTickers.some(t => t.toUpperCase() === tickerUpper)) {
      return "HIGH";
    }

    // 2. Check source type
    const sourceType = _str(ev.sourceType);
    if (["sec_filing", "sec", "company_ir", "company_ir_page", "sec_ex99_found"].includes(sourceType)) {
      return "HIGH";
    }

    // 3. Check text content
    const hay = `${_str(ev.title)} ${_str(ev.summary)} ${_str(ev.evidenceText)} ${_str(ev.issuer)}`.toLowerCase();
    if (tickerPattern.test(hay)) {
      return "HIGH";
    }

    if (baseCompanyName && hay.includes(baseCompanyName.toLowerCase())) {
      return "HIGH";
    }
    if (companyName && hay.includes(companyName.toLowerCase())) {
      return "HIGH";
    }

    // Check issuer field explicitly
    const evIssuer = _str(ev.issuer).toUpperCase();
    if (evIssuer.includes(tickerUpper)) {
      return "HIGH";
    }

    return "LOW";
  };

  // Stronger evidence first, then newest: a LOW-confidence aggregator item
  // never outranks a press release that matched the same query.
  const pool = official.length > 0 ? official : (inRange.length > 0 ? inRange : matched);
  const best = rankEvidence(pool, (ev) => confidenceForSourceType(ev.sourceType, ev.confidence)).slice(0, 5).map(ev => {
    const relevance = relevanceScore(ev);
    const match = queryMatch(ev);
    return {
      source: ev.source,
      sourceType: ev.sourceType,
      publishedAt: ev.publishedAt,
      retrievedAt: ev.retrievedAt,
      url: ev.url,
      confidence: confidenceForSourceType(ev.sourceType, ev.confidence),
      relevance,
      entityRelevance: relevance,
      queryMatch: {
        method: match.method,
        matchedTerms: match.matchedTerms,
        requiredTermMatches: match.requiredTermMatches,
      },
      evidenceText: shortText(ev.evidenceText || ev.summary || ev.title, 180),
    };
  });

  // If all best evidence is LOW relevance, downgrade status
  if (best.length > 0 && best.every(e => e.relevance === "LOW")) {
    if (status === "CONFIRMED") {
      status = "PARTIAL";
      out.warnings.push({
        code: "LOW_RELEVANCE_EVIDENCE",
        message: `Evidence found but none contain word-boundary match for ticker '${tickerUpper}'. Confidence downgraded.`,
        severity: "warning",
      });
    }
  }
  const sourceStatus = computeSourceStatus(out.sourcesUsed, out.warnings, out.items, sources, out.sourceDiagnostics, out.budgetStarvedSources);
  const sourceCoverage = computeSourceCoverage(sourceStatus);
  const coverage = buildCoverage(sourceStatus);
  const coverageFailureMode = verifyCoverageFailureMode(sourceStatus, out.warnings);
  if (status === "NOT_FOUND" && coverageFailureMode) {
    status = "SOURCE_LIMITED_NOT_FOUND";
    out.warnings.push({
      code: "SOURCE_LIMITED_NOT_FOUND",
      message: "One or more selected sources were unavailable; this is not confirmed evidence that the event did not occur.",
      severity: "warning",
      failureMode: coverageFailureMode,
    });
  }
  const failureMode = status === "SOURCE_LIMITED_NOT_FOUND" ? coverageFailureMode : null;
  const retryable = ["WORKER_SUBREQUEST_LIMIT", "RATE_LIMITED", "PROVIDER_TIMEOUT"].includes(_str(failureMode));
  const payload: Record<string, unknown> = {
    ticker: ticker.toUpperCase(),
    query: eventQuery,
    queryPolicy: {
      meaningfulTerms,
      requiredTermMatches,
      matchedEvidenceCount: matched.length,
    },
    status,
    bestEvidence: best,
    conflicts,
    meta: { sourcesChecked: out.sourcesUsed, watermark: out.watermark },
    warnings: out.warnings,
    sourceCoverage,
    coverage,
    sourceStatus,
    ...sourceRequestFields(requestedSources, sourceStatus),
    failureMode,
    retryable,
    recommendedAction: failureMode
      ? "Inspect sourceStatus and retry with a narrower sources list that excludes failed providers; do not treat this result as confirmed absence."
      : null,
  };
  return JSON.stringify(markNoSourceSelected(payload, requestedSources));
}
