/**
 * company_ir_page provider: approved company IR page registry entries, the page fetch and the feed /
 * structured JSON / HTML adapters.
 * Moved out of news.ts in 2.15.1 (pure restructure); providers never import news.ts.
 */

import { providerFetch } from "../../request-context.js";
import registryManifest from "../../company-ir-page-registry.json";
import { companyIrPageUrlAllowed } from "../../url-policy.js";
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
import type { NewsProviderResultWithDiagnostics } from "./types.js";

const COMPANY_IR_HTML_MAX_BYTES = 750 * 1024;
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
export const companyIrPageRegistry = registryManifest as unknown as CompanyIrPageRegistry;
export function companyIrPageEntry(ticker: string): CompanyIrPageSource | null {
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
export async function collectCompanyIrPageEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<NewsProviderResultWithDiagnostics> {
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
      // R-news D (2.22.0, F11 rest): the row carries canonicalUrl (null, no entry) beside registrySchemaVersion, as Python's does.
      diagnostics: { ...baseDiagnostics, canonicalUrl: null, status: "NOT_REGISTERED", approvalStatus: "not_registered", allowedAction: "registry_review_required" },
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
  // Registry states that are not fetched raise Python's info warnings (2.19.0, R-news Batch B A-G9 / B-G3), so the empty
  // collection reads as source-limited rather than a provider error.
  const warningBase = { canonicalUrl: entry.canonicalUrl, registryVersion: companyIrPageRegistry.registryVersion, approvalStatus: entry.status };
  if (entry.status === "disabled") {
    warnings.push({ ...warningBase, code: "COMPANY_IR_PAGE_DISABLED", message: "Company IR page registry entry is disabled.", severity: "info" });
    return { items: [], warnings, used: false, diagnostics: { ...diagnostics, status: "DISABLED", allowedAction: "registry_review_required" } };
  }
  if (entry.status === "candidate") {
    const candidate = {
      ticker: entry.ticker,
      issuerName: entry.issuerName,
      canonicalUrl: entry.canonicalUrl,
      adapter: entry.adapter,
      candidateReason: entry.candidateReason ?? null,
      discoveredAt: entry.discoveredAt ?? null,
      decisionGrade: false,
    };
    diagnostics.status = "CANDIDATE_AVAILABLE";
    diagnostics.allowedAction = "review_and_promote";
    diagnostics.candidates = [candidate];
    warnings.push({
      ...warningBase,
      code: "COMPANY_IR_PAGE_CANDIDATE_AVAILABLE",
      message: "Company IR page candidate is available for review but was not fetched.",
      severity: "info",
      candidate,
    });
    return { items: [], warnings, used: false, diagnostics };
  }
  if (!registryFresh(entry)) {
    warnings.push({
      code: "COMPANY_IR_PAGE_REVALIDATION_DUE",
      message: "Approved company IR page is past its revalidation date; skipped until reviewed.",
      severity: "warning",
      canonicalUrl: entry.canonicalUrl,
    });
    return { items: [], warnings, used: false, diagnostics: { ...diagnostics, status: "REVALIDATION_DUE", allowedAction: "registry_review_required" } };
  }

  try {
    const { text, finalUrl } = await fetchApprovedCompanyIrPage(entry);
    // An html_article entry is approved for HTML only: a feed body fails validation (2.19.0, call #17).
    if (entry.adapter === "html_article" && looksLikeFeed(text)) throw new Error("unexpected feed body for html_article adapter");
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
    return { items: [], warnings, used: false, diagnostics: { ...diagnostics, status: "SOURCE_VALIDATION_FAILED", allowedAction: "registry_review_required" } };
  }
}
function parseCompanyIrFeedItems(xml: string): { feedTitle: string | null; itemXmls: Array<{ kind: "rss" | "atom"; xml: string }> } {
  // Same guard as the GlobeNewswire feed (2.17.0): a DOCTYPE or ENTITY declaration is refused.
  if (/<!doctype|<!entity/i.test(xml)) throw new Error("unsupported XML declaration in company IR RSS/Atom");
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
