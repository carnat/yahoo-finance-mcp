const TICKER_RE = /^[A-Z0-9.\-\^=]{1,20}$/;
// Python's str-pattern \d is any Unicode decimal digit (\p{Nd}); JS \d is ASCII only.
const ACCESSION_RE = /^\p{Nd}{10}-\p{Nd}{2}-\p{Nd}{6}$/u;

export function validateTicker(ticker: string): string | null {
  const t = ticker.trim().toUpperCase();
  if (!TICKER_RE.test(t)) {
    return `Invalid ticker symbol: '${ticker}'. Must be 1-20 characters: uppercase letters, digits, or . - ^ =`;
  }
  return null;
}

/** Mirrors Python yfmcp/validation.py `_validate_accession`, message included. */
export function validateAccession(acc: string): string | null {
  if (!ACCESSION_RE.test(acc.trim())) {
    return `Invalid accession number: '${acc}'. Expected format: XXXXXXXXXX-YY-ZZZZZZ.`;
  }
  return null;
}
