/**
 * News company identity (Phase 4 unit 5, moved out of yahoo-finance.ts): the Yahoo profile and SEC CIK identity
 * the news providers and company IR-page discovery read, its official-website normalisation, and the Finnhub
 * eligibility policy.
 */

import { setBounded } from "../cache.js";
import { resolveCikForTicker } from "../clients/edgar.js";
import { yahooQuoteSummaryUrl } from "../clients/endpoints.js";
import { enc, yGet } from "../clients/yahoo.js";
import newsSourceCapabilities from "../news-source-capabilities.json";
import { blockedCompanyWebsiteHost, hostWithoutWww } from "../news.js";
import { _str } from "../util.js";

export const COMPANY_IR_DISCOVERY_TTL_MS = 24 * 60 * 60 * 1000;

const COMPANY_IR_IDENTITY_CACHE_MAX = 500;

const companyIrIdentityCache = new Map<string, { value: NewsCompanyIdentity; storedAt: number }>();

export type NewsCompanyIdentity = {
  ticker: string;
  shortName: string | null;
  longName: string | null;
  companyName: string | null;
  website: string | null;
  websiteHost: string | null;
  websiteRoot: string | null;
  exchange: string | null;
  cik: string | null;
  sources: string[];
  confidence: "HIGH" | "MEDIUM" | "LOW";
  warnings: Record<string, unknown>[];
};

export function finnhubEligibility(ticker: string): { eligible: boolean; reasonCode: string | null } {
  const policy = newsSourceCapabilities.providers.finnhub;
  const tickerU = ticker.toUpperCase();
  const ineligible = policy.ineligibleTickers.includes(tickerU)
    || policy.ineligibleTickerSuffixes.some(suffix => tickerU.endsWith(suffix));
  return { eligible: !ineligible, reasonCode: ineligible ? policy.reasonCode : null };
}

function companyHostRoot(host: string): string {
  let root = hostWithoutWww(host);
  for (const prefix of ["investors.", "investor.", "ir.", "newsroom.", "news."]) {
    if (root.startsWith(prefix)) root = root.slice(prefix.length);
  }
  return root;
}

function normalizeCompanyWebsite(raw: unknown): { url: string | null; host: string | null; root: string | null; warning: Record<string, unknown> | null } {
  let text = _str(raw).trim();
  if (!text) {
    return {
      url: null,
      host: null,
      root: null,
      warning: { code: "COMPANY_WEBSITE_NOT_AVAILABLE", message: "No company website was available from public profile data.", severity: "warning" },
    };
  }
  if (!/^https?:\/\//i.test(text)) text = `https://${text}`;
  try {
    const u = new URL(text);
    if (!["http:", "https:"].includes(u.protocol)) {
      return {
        url: null,
        host: null,
        root: null,
        warning: { code: "COMPANY_WEBSITE_NOT_AVAILABLE", message: `Company website uses unsupported scheme: ${u.protocol}`, severity: "warning" },
      };
    }
    if (blockedCompanyWebsiteHost(u.hostname)) {
      return {
        url: null,
        host: null,
        root: null,
        warning: { code: "COMPANY_WEBSITE_NOT_AVAILABLE", message: `Company website host is not an official company domain: ${u.hostname}`, severity: "warning" },
      };
    }
    u.hash = "";
    u.search = "";
    u.pathname = "/";
    const host = hostWithoutWww(u.hostname);
    return { url: u.toString(), host, root: companyHostRoot(host), warning: null };
  } catch {
    return {
      url: null,
      host: null,
      root: null,
      warning: { code: "COMPANY_WEBSITE_NOT_AVAILABLE", message: "Company website URL from public profile data was malformed.", severity: "warning" },
    };
  }
}

export async function resolveNewsCompanyIdentity(ticker: string): Promise<NewsCompanyIdentity> {
  const tickerU = ticker.toUpperCase();
  const cached = companyIrIdentityCache.get(tickerU);
  if (cached && Date.now() - cached.storedAt < COMPANY_IR_DISCOVERY_TTL_MS) return cached.value;

  const warnings: Record<string, unknown>[] = [];
  let shortName: string | null = null;
  let longName: string | null = null;
  let websiteRaw: unknown = null;
  let exchange: string | null = null;
  let cik: string | null = null;
  const sources: string[] = [];

  try {
    const d = (await yGet(
      yahooQuoteSummaryUrl(enc(tickerU), "price,summaryProfile,assetProfile,secFilings")
    )) as Record<string, unknown>;
    const result = (d?.quoteSummary as Record<string, unknown[]> | undefined)?.result?.[0] as Record<string, unknown> | undefined;
    const price = result?.price as Record<string, unknown> | undefined;
    const summaryProfile = result?.summaryProfile as Record<string, unknown> | undefined;
    const assetProfile = result?.assetProfile as Record<string, unknown> | undefined;
    const secFilings = result?.secFilings as Record<string, unknown> | undefined;
    shortName = _str(price?.shortName || price?.displayName || price?.symbol || summaryProfile?.name) || null;
    longName = _str(price?.longName || assetProfile?.longName || summaryProfile?.longName) || null;
    websiteRaw = summaryProfile?.website ?? assetProfile?.website ?? null;
    exchange = _str(price?.exchangeName || price?.fullExchangeName || price?.exchange) || null;
    const cikRaw = secFilings?.cik;
    if (cikRaw != null) cik = String(cikRaw).replace(/\D/g, "").padStart(10, "0");
    sources.push("yahoo_profile");
  } catch (e) {
    warnings.push({
      code: "COMPANY_IDENTITY_PROVIDER_ERROR",
      message: `Yahoo profile identity lookup failed: ${e instanceof Error ? e.message : String(e)}`,
      severity: "warning",
    });
  }

  if (!cik) {
    try {
      cik = await resolveCikForTicker(tickerU);
      if (cik) sources.push("sec_company_tickers");
    } catch {
      // CIK improves diagnostics but is not required for RSS discovery.
    }
  }

  const website = normalizeCompanyWebsite(websiteRaw);
  if (website.warning) warnings.push(website.warning);
  const companyName = longName || shortName || null;
  const confidence: NewsCompanyIdentity["confidence"] = website.url && companyName ? "HIGH" : (companyName || cik ? "MEDIUM" : "LOW");
  const identity: NewsCompanyIdentity = {
    ticker: tickerU,
    shortName,
    longName,
    companyName,
    website: website.url,
    websiteHost: website.host,
    websiteRoot: website.root,
    exchange,
    cik,
    sources,
    confidence,
    warnings,
  };
  setBounded(companyIrIdentityCache, tickerU, { value: identity, storedAt: Date.now() }, COMPANY_IR_IDENTITY_CACHE_MAX);
  return identity;
}
