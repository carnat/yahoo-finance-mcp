const TICKER_RE = /^[A-Z0-9.\-\^=]{1,20}$/;

export function validateTicker(ticker: string): string | null {
  const t = ticker.trim().toUpperCase();
  if (!TICKER_RE.test(t)) {
    return `Invalid ticker symbol: '${ticker}'. Must be 1-20 characters: uppercase letters, digits, or . - ^ =`;
  }
  return null;
}
