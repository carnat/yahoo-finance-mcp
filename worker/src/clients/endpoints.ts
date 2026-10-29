/**
 * Upstream endpoint builder (Phase 4 unit 4): every Yahoo Finance API and SEC EDGAR URL template in one place.
 * Each builder returns exactly the string its call sites wrote by hand before (decision D8), so callers keep doing
 * their own path encoding (enc(ticker), yahooTickerPath(...)) and pass the encoded segment in.
 * Python twin: yfmcp/clients/endpoints.py. Pinned by scripts/test_endpoints.py.
 */

// ── Yahoo Finance ────────────────────────────────────────────────────────────

export const YAHOO_QUERY1 = "https://query1.finance.yahoo.com";
export const YAHOO_QUERY2 = "https://query2.finance.yahoo.com";
export const YAHOO_WEB = "https://finance.yahoo.com";

/** quoteSummary for an encoded symbol and a comma-separated module list. */
export function yahooQuoteSummaryUrl(symbolPath: string, modules: string): string {
  return `${YAHOO_QUERY1}/v10/finance/quoteSummary/${symbolPath}?modules=${modules}`;
}

/** v8 chart for an encoded symbol; `query` is the query string without its "?". */
export function yahooChartUrl(symbolPath: string, query: string): string {
  return `${YAHOO_QUERY1}/v8/finance/chart/${symbolPath}?${query}`;
}

/** v7 options for an encoded symbol (no query: the nearest expiry). */
export function yahooOptionsUrl(symbolPath: string): string {
  return `${YAHOO_QUERY2}/v7/finance/options/${symbolPath}`;
}

/** Fundamentals timeseries for an encoded symbol, on the host the call site uses. */
export function yahooTimeseriesUrl(symbolPath: string, host: string = YAHOO_QUERY2): string {
  return `${host}/ws/fundamentals-timeseries/v1/finance/timeseries/${symbolPath}`;
}

/** v1 search; `query` is the query string without its "?". */
export function yahooSearchUrl(query: string): string {
  return `${YAHOO_QUERY1}/v1/finance/search?${query}`;
}

/** v1 quoteType for an encoded symbol path. */
export function yahooQuoteTypeUrl(symbolPath: string): string {
  return `${YAHOO_QUERY1}/v1/finance/quoteType/${symbolPath}?lang=en-US&region=US`;
}

/** The visualization (calendar) POST endpoint. */
export const YAHOO_VISUALIZATION_URL = "https://query1.finance.yahoo.com/v1/finance/visualization?lang=en-US&region=US";

/** Predefined screener; `query` is the query string without its "?". */
export function yahooScreenerUrl(query: string): string {
  return `${YAHOO_QUERY1}/v1/finance/screener/predefined/saved?${query}`;
}

// ── SEC EDGAR ────────────────────────────────────────────────────────────────

export const SEC_WWW = "https://www.sec.gov";
export const SEC_DATA = "https://data.sec.gov";
/** Every SEC archive document starts with this prefix. */
export const SEC_ARCHIVES_PREFIX = "https://www.sec.gov/Archives/";
export const SEC_TICKER_INDEX_URL = "https://www.sec.gov/files/company_tickers.json";

export function secSubmissionsUrl(cikPadded: string): string {
  return `${SEC_DATA}/submissions/CIK${cikPadded}.json`;
}

export function secCompanyFactsUrl(cikPadded: string): string {
  return `${SEC_DATA}/api/xbrl/companyfacts/CIK${cikPadded}.json`;
}

export function secCompanyConceptUrl(cikPadded: string, concept: string): string {
  return `${SEC_DATA}/api/xbrl/companyconcept/CIK${cikPadded}/us-gaap/${concept}.json`;
}

/** A document inside a filing folder: CIK without padding, accession without dashes, file name. */
export function secArchiveDocumentUrl(cik: string | number, accessionNoDash: string, fileName: string): string {
  return `${SEC_WWW}/Archives/edgar/data/${cik}/${accessionNoDash}/${fileName}`;
}

/** A filing's index page: the accession appears without dashes in the folder and with dashes in the file name. */
export function secFilingIndexUrl(cik: string | number, accessionNoDash: string, accessionNumber: string): string {
  return `${SEC_WWW}/Archives/edgar/data/${cik}/${accessionNoDash}/${accessionNumber}-index.htm`;
}

/** EDGAR company browse as an Atom feed, for an encoded ticker or CIK (the CIK fallback in resolveCikForTicker). */
export function secBrowseEdgarAtomUrl(companyParam: string): string {
  return `${SEC_WWW}/cgi-bin/browse-edgar?action=getcompany&CIK=${companyParam}&type=&dateb=&owner=include&count=10&output=atom`;
}

/** The manual EDGAR lookups a filing tool returns when a fact is not XBRL-tagged (moved from yahoo-finance.ts). */
export function filingManualLookup(ticker: string, cikPadded: string | null, filingType: string): Record<string, unknown> {
  const edgarIndexUrl = cikPadded
    ? `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cikPadded}&type=${filingType}&owner=include&count=10`
    : `https://www.sec.gov/cgi-bin/browse-edgar?company=${encodeURIComponent(ticker)}&action=getcompany&type=${filingType}&owner=include&count=10`;
  const eftsSearchUrl = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(ticker)}&forms=${encodeURIComponent(filingType)}`;
  return {
    edgarIndexUrl,
    eftsSearchUrl,
    note: "Fact not XBRL-tagged. Use search_filing_text instead.",
  };
}
