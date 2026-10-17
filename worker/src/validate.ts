const TICKER_RE = /^[A-Z0-9.\-\^=]{1,20}$/;
// ASCII digits only (2.12.0 planner decision; Python's `\d` is to be narrowed to match).
const ACCESSION_RE = /^[0-9]{10}-[0-9]{2}-[0-9]{6}$/;

export function validateTicker(ticker: string): string | null {
  const t = ticker.trim().toUpperCase();
  if (!TICKER_RE.test(t)) {
    return `Invalid ticker symbol: '${ticker}'. Must be 1-20 characters: uppercase letters, digits, or . - ^ =`;
  }
  return null;
}

/** Mirrors Python yfmcp/validation.py `_validate_accession`, message included (ASCII digits only). */
export function validateAccession(acc: string): string | null {
  if (!ACCESSION_RE.test(acc.trim())) {
    return `Invalid accession number: '${acc}'. Expected format: XXXXXXXXXX-YY-ZZZZZZ.`;
  }
  return null;
}
