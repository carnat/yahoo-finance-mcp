"""Input validation helpers for ticker symbols, accession numbers, and SEC URLs.

Extracted from server.py in Phase 1 of the refactoring plan.
"""

import re as _re


# ---------------------------------------------------------------------------
# Input validation helpers
# ---------------------------------------------------------------------------
_TICKER_RE = _re.compile(r'^[A-Z0-9.\-\^=]{1,20}$')
_ACCESSION_RE = _re.compile(r'^\d{10}-\d{2}-\d{6}$')


def _validate_ticker(ticker: str) -> str | None:
    """Returns an error message if the ticker is invalid, else None."""
    t = ticker.strip().upper()
    if not _TICKER_RE.match(t):
        return f"Invalid ticker symbol: '{ticker}'. Must be 1-20 characters: uppercase letters, digits, or . - ^ ="
    return None


def _validate_accession(acc: str) -> str | None:
    """Returns an error message if the accession number is invalid, else None."""
    if not _ACCESSION_RE.match(acc.strip()):
        return f"Invalid accession number: '{acc}'. Expected format: XXXXXXXXXX-YY-ZZZZZZ."
    return None


def _validate_batch_tickers(tickers: list) -> str | None:
    """Returns an error message if the batch is too large, else None."""
    if len(tickers) > 5:
        return f"Too many tickers: {len(tickers)}. Maximum is 5 per call."
    return None


def _validate_sec_url(url: str) -> str | None:
    """Returns an error message if the SEC URL is not from sec.gov/Archives, else None."""
    if not url.startswith("https://www.sec.gov/Archives/"):
        return f"Invalid SEC URL: must start with 'https://www.sec.gov/Archives/'."
    return None


def _sanitize_sec_html(html: str) -> str:
    """Strip script/style tags and event handler attributes from SEC HTML."""
    html = _re.sub(r'<script[^>]*>.*?</script[^>]*>', '', html, flags=_re.DOTALL | _re.IGNORECASE)
    html = _re.sub(r'<style[^>]*>.*?</style[^>]*>', '', html, flags=_re.DOTALL | _re.IGNORECASE)
    html = _re.sub(r'\s+on\w+\s*=\s*(?:"[^"]*"|\'[^\']*\'|[^\s>]+)', '', html, flags=_re.IGNORECASE)
    return html
