/**
 * EDGAR client (Phase 4 unit 1, moved out of yahoo-finance.ts): the SEC transport and User-Agent, the SEC document
 * cache, EDGAR JSON/HTML reads, filing and exhibit-index URLs, ticker -> CIK resolution and submissions.
 * Filing selection (resolveSecFiling and its not-found envelopes) stays in yahoo-finance.ts (decision D11).
 * The Yahoo client and provider transport come from clients/yahoo.ts and clients/provider-http.ts (unit 2); the HTML
 * helper stripHtmlTags is imported back from yahoo-finance.ts until unit 3 moves it (decision D4).
 */

import { BoundedTtlCache } from "../cache.js";
import { awaitNotingBudget, consumeSubrequest, countCacheEvent, emptyCacheUsage, providerFetch, type CacheEvent, type CacheUsage } from "../request-context.js";
import { getWorkerVar } from "../response.js";
import { stripHtmlTags } from "../yahoo-finance.js";
import { edgeCache, fetchProviderWithTimeout } from "./provider-http.js";
import { countersSince, enc, yGet } from "./yahoo.js";

// Ticker -> CIK fixtures, read after SEC's ticker index and before the EDGAR atom lookup. Empty in production:
// only tests fill it, through setTickerCikFixturesForTests (2.13.0, R4 Q4; the smoke table it replaces was a
// production fallback).
let tickerCikFixtures: ReadonlyMap<string, string> = new Map();

/** Tests only: replace the ticker -> padded CIK fixtures (null or undefined empties them). */
export function setTickerCikFixturesForTests(map: ReadonlyMap<string, string> | null | undefined): void {
  tickerCikFixtures = new Map([...(map ?? new Map<string, string>())].map(([ticker, cik]) => [ticker.toUpperCase(), cik]));
}

// EDGAR fair-access policy requires a reachable contact in the User-Agent.
// Set the EDGAR_CONTACT_EMAIL Cloudflare var/secret to an operator-owned
// address; the placeholder is used only when it is unset.
const EDGAR_FALLBACK_CONTACT = "contact@example.com";
const EDGAR_FETCH_TIMEOUT_MS = 20_000;

function edgarUserAgent(): string {
  return `yahoo-finance-mcp ${getWorkerVar("EDGAR_CONTACT_EMAIL")?.trim() || EDGAR_FALLBACK_CONTACT}`;
}

function edgarFetch(url: string): Promise<Response> {
  return fetchProviderWithTimeout(url, { headers: { "User-Agent": edgarUserAgent() } }, EDGAR_FETCH_TIMEOUT_MS);
}

/**
 * A caller-supplied SEC page (parse_public_transcript): the generic public-page read (fetchPublicHtml in
 * yahoo-finance.ts: no timeout, whole chunks until maxBytes, UTF-8), sent with SEC's required User-Agent instead of
 * the browser one (2.26.0, D9). Null when the page cannot be read. Not cached: the caller may name any page.
 */
export async function edgarFetchPublicHtml(url: string, maxBytes = 3_000_000): Promise<string | null> {
  try {
    const resp = await providerFetch(url, { headers: { "User-Agent": edgarUserAgent() } });
    if (!resp.ok) return null;
    const reader = resp.body?.getReader();
    if (!reader) return await resp.text();
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      chunks.push(value);
      total += value.byteLength;
      if (total >= maxBytes) { await reader.cancel(); break; }
    }
    const merged = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) { merged.set(c, off); off += c.byteLength; }
    return new TextDecoder().decode(merged);
  } catch {
    return null;
  }
}

// SEC archive documents (https://www.sec.gov/Archives/...) never change once
// filed, and filing tools usually read the same document several times
// (outline, then sections and tables). Documents are kept for 10 minutes in
// the isolate and for 24 hours in the colo's edge cache, and concurrent reads
// of one document share a single SEC request. Failed fetches are not cached.
const SEC_DOCUMENT_EDGE_CACHE_VERSION = "v1";
const SEC_DOCUMENT_MEMORY_TTL_MS = 10 * 60 * 1000;
const SEC_DOCUMENT_EDGE_TTL_MS = 24 * 60 * 60 * 1000;
// Filings reach several MB and may hold two-byte characters, so the
// in-process cache is bounded by characters as well as entries.
const SEC_DOCUMENT_CACHE_MAX_CHARS = 16_000_000;
/** Filing documents are read (and scanned) up to this many characters. */
export const SEC_DOCUMENT_READ_MAX_CHARS = 12_000_000;
const secDocumentBodies = new BoundedTtlCache<string>(12, SEC_DOCUMENT_CACHE_MAX_CHARS, (body) => body.length);
export type SecDocumentResult = { ok: true; text: string } | { ok: false; status: number };
const secDocumentInflight = new Map<string, Promise<SecDocumentResult>>();
const secDocumentCounters: CacheUsage = emptyCacheUsage();

function countSecDocumentCache(event: CacheEvent): void {
  secDocumentCounters[event]++;
  countCacheEvent("sec", event);
}

export function secDocumentCacheStats(): Record<string, unknown> {
  return {
    scope: "isolate",
    since: countersSince,
    ...secDocumentCounters,
    bodyCacheEntries: secDocumentBodies.size,
    bodyCacheChars: secDocumentBodies.weight,
  };
}

function secDocumentEdgeRequest(url: string): Request {
  return new Request(`https://sec-document-cache.invalid/${SEC_DOCUMENT_EDGE_CACHE_VERSION}/${encodeURIComponent(url)}`);
}

async function readSecDocumentEdge(url: string): Promise<string | null> {
  const cache = edgeCache();
  if (!cache || !consumeSubrequest("cache")) return null;
  try {
    const response = await cache.match(secDocumentEdgeRequest(url));
    if (!response) return null;
    const storedAt = Number(response.headers.get("X-Stored-At"));
    if (!Number.isFinite(storedAt) || Date.now() - storedAt >= SEC_DOCUMENT_EDGE_TTL_MS) {
      await response.body?.cancel();
      return null;
    }
    return await response.text();
  } catch {
    return null;
  }
}

async function writeSecDocumentEdge(url: string, text: string): Promise<boolean> {
  const cache = edgeCache();
  if (!cache || !consumeSubrequest("cache")) return false;
  try {
    await cache.put(
      secDocumentEdgeRequest(url),
      new Response(text, {
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": `public, max-age=${Math.floor(SEC_DOCUMENT_EDGE_TTL_MS / 1000)}`,
          "X-Stored-At": String(Date.now()),
        },
      }),
    );
    return true;
  } catch {
    // Edge cache is best effort; the SEC response remains usable.
    return false;
  }
}

async function loadSecDocument(url: string): Promise<SecDocumentResult> {
  const cached = await readSecDocumentEdge(url);
  if (cached !== null) {
    countSecDocumentCache("edgeHits");
    return { ok: true, text: cached };
  }
  countSecDocumentCache("edgeMisses");
  countSecDocumentCache("upstreamFetches");
  const resp = await edgarFetch(url);
  if (!resp.ok) {
    await resp.body?.cancel();
    return { ok: false, status: resp.status };
  }
  const text = await resp.text();
  if (await writeSecDocumentEdge(url, text)) countSecDocumentCache("edgeWrites");
  return { ok: true, text };
}

/** Read an SEC archive document through the process, in-flight and edge caches. */
export async function fetchSecDocument(url: string): Promise<SecDocumentResult> {
  const cached = secDocumentBodies.get(url);
  if (cached !== undefined) {
    countSecDocumentCache("memoryHits");
    return { ok: true, text: cached };
  }
  let pending = secDocumentInflight.get(url);
  if (pending) {
    countSecDocumentCache("sharedInflight");
  } else {
    pending = loadSecDocument(url)
      .then((result) => {
        if (result.ok) secDocumentBodies.set(url, result.text, SEC_DOCUMENT_MEMORY_TTL_MS);
        return result;
      })
      .finally(() => {
        secDocumentInflight.delete(url);
      });
    secDocumentInflight.set(url, pending);
  }
  return awaitNotingBudget(pending);
}

const FILING_CIK_TTL_MS = 24 * 60 * 60 * 1000;
// Submissions list recent filings, so they must expire for new 8-K/10-Q
// filings to become visible within a long-lived isolate.
const FILING_SUBMISSIONS_TTL_MS = 15 * 60 * 1000;
const filingCikCache = new BoundedTtlCache<string>(2_000);
const filingSubmissionsCache = new BoundedTtlCache<Record<string, unknown>>(50);

// ── EDGAR helper utilities ────────────────────────────────────────────────────

/** Fetch an EDGAR JSON endpoint using the required User-Agent header. */
export async function edgarGetJson(url: string): Promise<Record<string, unknown> | null> {
  try {
    const resp = await edgarFetch(url);
    if (!resp.ok) { await resp.body?.cancel(); return null; }
    return await resp.json() as Record<string, unknown>;
  } catch {
    return null;
  }
}

/**
 * An EDGAR JSON read that tells absence from failure (2.5.17): a 404 is SEC saying the resource does not
 * exist (json null); any other non-OK status, a network error or an unreadable body is a failed read.
 */
type EdgarJsonRead = { ok: true; json: Record<string, unknown> | null } | { ok: false; httpStatus: number | null };

export async function edgarReadJson(url: string): Promise<EdgarJsonRead> {
  try {
    const resp = await edgarFetch(url);
    if (resp.status === 404) { await resp.body?.cancel(); return { ok: true, json: null }; }
    if (!resp.ok) { await resp.body?.cancel(); return { ok: false, httpStatus: resp.status }; }
    return { ok: true, json: await resp.json() as Record<string, unknown> };
  } catch {
    return { ok: false, httpStatus: null };
  }
}

/** Fetch an EDGAR HTML/text document. Reads at most maxBytes from the response
 *  stream and cancels the rest, avoiding large memory allocations for big filings. */
export async function edgarGetHtml(url: string, maxBytes = 5_000_000): Promise<string | null> {
  // Filing documents go through the SEC document cache and are read up to
  // SEC_DOCUMENT_READ_MAX_CHARS; large inline-XBRL 10-Ks exceed 5 MB, and a
  // cut-off read hid their later tables. Small reads (index pages) stream.
  if (maxBytes >= 1_000_000 && url.startsWith("https://www.sec.gov/Archives/")) {
    try {
      const doc = await fetchSecDocument(url);
      if (!doc.ok) return null;
      return doc.text.length > SEC_DOCUMENT_READ_MAX_CHARS ? doc.text.slice(0, SEC_DOCUMENT_READ_MAX_CHARS) : doc.text;
    } catch {
      return null;
    }
  }
  return (await edgarStreamHtml(url, maxBytes)).text;
}

/** A streamed EDGAR read of at most maxBytes, with the HTTP status of a failed response (null when none came back). */
async function edgarStreamHtml(url: string, maxBytes: number): Promise<{ text: string | null; httpStatus: number | null }> {
  try {
    const resp = await edgarFetch(url);
    if (!resp.ok || !resp.body) { await resp.body?.cancel(); return { text: null, httpStatus: resp.status }; }
    const reader = resp.body.getReader();
    const chunks: Uint8Array[] = [];
    let totalBytes = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) {
        const remaining = maxBytes - totalBytes;
        if (value.byteLength <= remaining) {
          chunks.push(value);
          totalBytes += value.byteLength;
        } else {
          if (remaining > 0) {
            chunks.push(value.slice(0, remaining));
            totalBytes = maxBytes;
          }
          await reader.cancel();
          break;
        }
      }
    }
    const combined = new Uint8Array(totalBytes);
    let offset = 0;
    for (const chunk of chunks) { combined.set(chunk, offset); offset += chunk.byteLength; }
    return { text: new TextDecoder("utf-8", { fatal: false, ignoreBOM: true }).decode(combined), httpStatus: null };
  } catch {
    return { text: null, httpStatus: null };
  }
}

/** Construct EDGAR filing index URL and primary document URL from CIK + accession + primaryDoc. */
export function edgarBuildFilingUrls(
  cik: number,
  accessionNumber: string,
  primaryDoc: string | null
): { edgarIndexUrl: string; edgarPrimaryDocumentUrl: string | null } {
  const noDash = accessionNumber.replace(/-/g, "");
  const edgarIndexUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${noDash}/${accessionNumber}-index.htm`;
  const edgarPrimaryDocumentUrl = primaryDoc
    ? `https://www.sec.gov/Archives/edgar/data/${cik}/${noDash}/${primaryDoc}`
    : null;
  return { edgarIndexUrl, edgarPrimaryDocumentUrl };
}

function normalizeEdgarDocumentRef(rawRef: string): string | null {
  let ref = rawRef.trim();
  if (!ref) return null;
  const docParam = ref.match(/[?&]doc=([^&#]+)/i);
  if (docParam) {
    try {
      ref = decodeURIComponent(docParam[1]);
    } catch {
      ref = docParam[1];
    }
  }
  ref = ref.split("#", 1)[0].split("?", 1)[0].trim();
  return ref || null;
}

export function edgarDocumentFileName(rawRef: string): string {
  const normalized = normalizeEdgarDocumentRef(rawRef) ?? rawRef.trim();
  return normalized.split("/").pop() ?? normalized;
}

export function edgarDocumentUrlFromRef(cik: number, accessionNumber: string, rawRef: string): string | null {
  const ref = normalizeEdgarDocumentRef(rawRef);
  if (!ref) return null;
  if (/^https?:\/\//i.test(ref)) return ref;
  if (ref.startsWith("/Archives/")) return `https://www.sec.gov${ref}`;
  const noDash = accessionNumber.replace(/-/g, "");
  const fileName = ref.split("/").pop() ?? ref;
  return fileName ? `https://www.sec.gov/Archives/edgar/data/${cik}/${noDash}/${fileName}` : null;
}

function edgarDocumentUrlFromIndexUrl(indexUrl: string, rawRef: string): string | null {
  const ref = normalizeEdgarDocumentRef(rawRef);
  if (!ref) return null;
  if (/^https?:\/\//i.test(ref)) return ref;
  if (ref.startsWith("/Archives/")) return `https://www.sec.gov${ref}`;
  const fileName = ref.split("/").pop() ?? ref;
  const baseUrl = indexUrl.replace(/\/[^/]*$/, "/");
  return fileName ? `${baseUrl}${fileName}` : null;
}

export async function edgarListExhibitsFromIndex(indexUrl: string): Promise<Record<string, unknown>[]> {
  return (await edgarReadExhibitIndex(indexUrl)).exhibits;
}

/**
 * The exhibits a filing index lists, and whether SEC answered the index with HTTP 404 (the accession is not a filing
 * of that CIK). Any other failed read is not notFound (2.15.0, R4-ACC-EXHIBITS).
 */
export async function edgarReadExhibitIndex(indexUrl: string): Promise<{ exhibits: Record<string, unknown>[]; notFound: boolean }> {
  const read = await edgarStreamHtml(indexUrl, 500_000);
  return { exhibits: read.text ? parseExhibitIndexHtml(indexUrl, read.text) : [], notFound: read.httpStatus === 404 };
}

function parseExhibitIndexHtml(indexUrl: string, html: string): Record<string, unknown>[] {
  const exhibits: Record<string, unknown>[] = [];
  for (const rowM of html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const rowHtml = rowM[1];
    const cells = [...rowHtml.matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)].map((m) => m[1]);
    if (cells.length < 3) continue;
    const sequence = stripHtmlTags(cells[0]).trim();
    if (!/^\d/.test(sequence)) continue;
    const hrefMatch = cells[2].match(/href=["']([^"']+)["']/i);
    const rawDocumentRef = hrefMatch
      ? hrefMatch[1].trim()
      : stripHtmlTags(cells[2] ?? "").trim();
    const document = edgarDocumentFileName(rawDocumentRef);
    const documentUrl = edgarDocumentUrlFromIndexUrl(indexUrl, rawDocumentRef);
    exhibits.push({
      sequence,
      description: stripHtmlTags(cells[1] ?? "").trim(),
      document,
      documentUrl,
      type: stripHtmlTags(cells[3] ?? "").trim(),
      size: stripHtmlTags(cells[4] ?? "").trim(),
    });
  }
  return exhibits;
}

/**
 * Derive the EDGAR CIK from an accession number.
 * The first 10 digits of an accession number are the zero-padded filer CIK.
 * e.g. "0000024741-26-000124" → 24741
 */
export function edgarCikFromAccession(accessionNumber: string): number | null {
  try {
    // Strip leading zeros; empty string ("") is falsy so "|| '0'" handles the
    // all-zeros edge case without producing NaN from parseInt("", 10).
    const stripped = accessionNumber.split("-")[0].replace(/^0+/, "") || "0";
    const n = parseInt(stripped, 10);
    return isNaN(n) || n === 0 ? null : n;
  } catch {
    return null;
  }
}

const EDGAR_TICKER_MAP_TTL_MS = 24 * 60 * 60 * 1000;
let edgarTickerMap: { value: Map<string, number>; expiresAt: number } | null = null;
let edgarTickerMapLoad: Promise<Map<string, number> | null> | null = null;

/**
 * Ticker -> CIK lookup built from EDGAR company_tickers.json (~1 MB). The file
 * is downloaded and indexed once per isolate per day instead of per lookup;
 * concurrent callers share one download. A failed download is not cached.
 */
export async function getEdgarTickerCikMap(): Promise<Map<string, number> | null> {
  if (edgarTickerMap && Date.now() < edgarTickerMap.expiresAt) return edgarTickerMap.value;
  edgarTickerMapLoad ??= edgarGetJson("https://www.sec.gov/files/company_tickers.json")
    .then((data) => {
      if (!data) return null;
      const map = new Map<string, number>();
      for (const entry of Object.values(data) as { ticker?: unknown; cik_str?: unknown }[]) {
        const cik = Number(entry?.cik_str);
        if (typeof entry?.ticker !== "string" || !Number.isFinite(cik) || cik <= 0) continue;
        const symbol = entry.ticker.toUpperCase();
        // company_tickers.json is ordered by relevance; keep the first match.
        if (!map.has(symbol)) map.set(symbol, cik);
      }
      edgarTickerMap = { value: map, expiresAt: Date.now() + EDGAR_TICKER_MAP_TTL_MS };
      return map;
    })
    .finally(() => {
      edgarTickerMapLoad = null;
    });
  return edgarTickerMapLoad;
}

/**
 * True when SEC's company_tickers.json index is loaded and current (no fetch). After a CIK lookup came back
 * empty, this tells "SEC has no CIK for the ticker" (the index was read) from "the index could not be read" (2.16.0).
 */
export function edgarTickerMapLoaded(): boolean {
  return !!edgarTickerMap && Date.now() < edgarTickerMap.expiresAt;
}

/** Resolve CIK for a ticker using the EDGAR company_tickers.json index. */
async function edgarResolveCik(ticker: string): Promise<number | null> {
  const map = await getEdgarTickerCikMap();
  return map?.get(ticker.toUpperCase()) ?? null;
}

export async function resolveCikForTicker(ticker: string): Promise<string | null> {
  const key = ticker.toUpperCase();
  const cached = filingCikCache.get(key);
  if (cached) {
    countCacheEvent("sec", "memoryHits");
    return cached;
  }

  try {
    const sec = await yGet(
      `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${enc(ticker)}?modules=secFilings`
    ) as Record<string, unknown>;
    const result = (sec?.quoteSummary as Record<string, unknown[]>)?.result?.[0] as Record<string, unknown> | undefined;
    const secFilings = result?.secFilings as Record<string, unknown> | undefined;
    const cikFromYahoo = secFilings?.cik as string | number | undefined;
    if (cikFromYahoo != null) {
      const cik = String(cikFromYahoo).replace(/\D/g, "").padStart(10, "0");
      filingCikCache.set(key, cik, FILING_CIK_TTL_MS);
      return cik;
    }
  } catch { /* non-fatal */ }

  const cikFromTickerFile = await edgarResolveCik(ticker);
  if (cikFromTickerFile != null) {
    const cik = String(cikFromTickerFile).padStart(10, "0");
    filingCikCache.set(key, cik, FILING_CIK_TTL_MS);
    return cik;
  }

  const fixtureCik = tickerCikFixtures.get(key);
  if (fixtureCik) {
    filingCikCache.set(key, fixtureCik, FILING_CIK_TTL_MS);
    return fixtureCik;
  }

  const extractCik = (text: string): string | null => {
    const patterns = [/CIK=(\d{1,10})/i, /\/CIK0*([1-9]\d{0,9})\.json/i, /\/edgar\/data\/0*([1-9]\d{0,9})\//i];
    for (const p of patterns) {
      const m = text.match(p);
      if (m) return m[1].padStart(10, "0");
    }
    return null;
  };

  // EDGAR's lookup by ticker only. Its company-name search is not a ticker lookup: "BE" matched "BE 2023
  // Irrevocable Trust" (CIK 0001986183) ahead of Bloom Energy and was cached as BE's CIK when Yahoo and SEC's
  // ticker index were both unreadable (2.5.31, F-027).
  const atomUrls = [
    `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${encodeURIComponent(ticker)}&type=&dateb=&owner=include&count=10&output=atom`,
  ];

  try {
    for (const atomUrl of atomUrls) {
      const atom = await edgarFetch(atomUrl);
      if (!atom.ok) {
        await atom.body?.cancel();
        continue;
      }
      const text = await atom.text();
      const cik = extractCik(text);
      if (cik) {
        filingCikCache.set(key, cik, FILING_CIK_TTL_MS);
        return cik;
      }
    }
  } catch { /* non-fatal */ }

  return null;
}

export async function getSubmissionsForTicker(ticker: string): Promise<{ cikPadded: string | null; submissions: Record<string, unknown> | null }> {
  const key = ticker.toUpperCase();
  const cachedSubmissions = filingSubmissionsCache.get(key) ?? null;
  const cikPadded = await resolveCikForTicker(ticker);
  if (!cikPadded) return { cikPadded: null, submissions: null };
  if (cachedSubmissions) {
    countCacheEvent("sec", "memoryHits");
    return { cikPadded, submissions: cachedSubmissions };
  }
  const submissions = await edgarGetJson(`https://data.sec.gov/submissions/CIK${cikPadded}.json`);
  if (submissions) filingSubmissionsCache.set(key, submissions, FILING_SUBMISSIONS_TTL_MS);
  return { cikPadded, submissions };
}

/** Resolve the EX-99.1 exhibit URL from an 8-K filing index page. */
export async function resolveEx991Url(cikInt: number, accessionNumber: string): Promise<string | null> {
  const { edgarIndexUrl: indexUrl } = edgarBuildFilingUrls(cikInt, accessionNumber, null);
  const exhibits = await edgarListExhibitsFromIndex(indexUrl);
  const ex991 = exhibits.find((exhibit) => {
    // One rule in both runtimes (2.14.0): the type names exhibit 99.1 (EX-99.1, EX-99.01 or the bare 99.1 alias),
    // or an EX-99 type carries release wording. A file name alone is not accepted.
    const type = String(exhibit.type ?? "").toUpperCase().replace(/\s+/g, "");
    const description = String(exhibit.description ?? "").toUpperCase();
    if (/^EX-99\.0?1$/.test(type) || /^99\.0?1$/.test(type)) return true;
    return type.startsWith("EX-99") && /PRESS RELEASE|EARNINGS RELEASE|RESULTS RELEASE/.test(description);
  });
  if (ex991?.document) {
    const { edgarPrimaryDocumentUrl } = edgarBuildFilingUrls(cikInt, accessionNumber, String(ex991.document));
    if (edgarPrimaryDocumentUrl) return edgarPrimaryDocumentUrl;
  }

  const html = await edgarGetHtml(indexUrl, 100_000);
  if (!html) return null;
  // Match an EX-99.1 row in the filing index and capture its document link
  const re = /EX-99\.0?1\b[^<]{0,300}<[^>]+href="(\/Archives\/edgar\/data\/[^"]+\.(?:htm|html))"/i;
  const m = re.exec(html);
  if (m) return `https://www.sec.gov${m[1]}`;
  return null;
}
