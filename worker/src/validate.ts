const TICKER_RE = /^[A-Z0-9.\-\^=]{1,20}$/;
const SEC_URL_PREFIX = "https://www.sec.gov/Archives/";

export function validateTicker(ticker: string): string | null {
  const t = ticker.trim().toUpperCase();
  if (!TICKER_RE.test(t)) {
    return `Invalid ticker symbol: '${ticker}'. Must be 1-20 uppercase alphanumeric characters.`;
  }
  return null;
}

export function validateBatch(tickers: string[]): string | null {
  if (tickers.length > 5) {
    return `Too many tickers: ${tickers.length}. Maximum is 5 per call.`;
  }
  return null;
}

export function validateSecUrl(url: string): string | null {
  if (!url.startsWith(SEC_URL_PREFIX)) {
    return `Invalid SEC URL: must start with '${SEC_URL_PREFIX}'.`;
  }
  return null;
}
