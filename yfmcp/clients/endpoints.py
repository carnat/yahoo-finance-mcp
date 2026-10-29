"""Upstream endpoint builder (Phase 4 unit 4): the SEC EDGAR and Yahoo Finance URL templates in one place.

Each builder returns exactly the string its call sites wrote by hand before (decision D8); callers keep doing their
own encoding. Worker twin: worker/src/clients/endpoints.ts. Pinned by scripts/test_endpoints.py.
"""

from __future__ import annotations

import urllib.parse as _urlparse

# ── Yahoo Finance ────────────────────────────────────────────────────────────

YAHOO_QUERY1 = "https://query1.finance.yahoo.com"
YAHOO_QUERY2 = "https://query2.finance.yahoo.com"
# quoteSummary path prefixes; the symbol follows, modules go in the query (the Worker's yahooQuoteSummaryUrl).
YAHOO_QUOTE_SUMMARY_PREFIX = f"{YAHOO_QUERY1}/v10/finance/quoteSummary/"
YAHOO_QUOTE_SUMMARY_PREFIX_QUERY2 = f"{YAHOO_QUERY2}/v10/finance/quoteSummary/"


def yahoo_quote_summary_url(symbol_path: str, modules: str) -> str:
    return f"{YAHOO_QUOTE_SUMMARY_PREFIX}{symbol_path}?modules={modules}"


# ── SEC EDGAR ────────────────────────────────────────────────────────────────

SEC_WWW = "https://www.sec.gov"
SEC_DATA = "https://data.sec.gov"
# Every SEC archive document starts with this prefix.
SEC_ARCHIVES_PREFIX = "https://www.sec.gov/Archives/"
SEC_TICKER_INDEX_URL = "https://www.sec.gov/files/company_tickers.json"


def sec_submissions_url(cik_padded: str) -> str:
    return f"{SEC_DATA}/submissions/CIK{cik_padded}.json"


def sec_company_facts_url(cik_padded: str) -> str:
    return f"{SEC_DATA}/api/xbrl/companyfacts/CIK{cik_padded}.json"


def sec_company_concept_url(cik_padded: str, concept: str) -> str:
    return f"{SEC_DATA}/api/xbrl/companyconcept/CIK{cik_padded}/us-gaap/{concept}.json"


def sec_archive_document_url(cik: object, accession_nodash: str, file_name: object) -> str:
    """A document inside a filing folder: CIK without padding, accession without dashes, file name."""
    return f"{SEC_WWW}/Archives/edgar/data/{cik}/{accession_nodash}/{file_name}"


def sec_filing_index_url(cik: object, accession_nodash: str, accession_number: str) -> str:
    """A filing's index page: the accession appears without dashes in the folder and with dashes in the file name."""
    return f"{SEC_WWW}/Archives/edgar/data/{cik}/{accession_nodash}/{accession_number}-index.htm"


def sec_browse_edgar_atom_url(company_param: str) -> str:
    """EDGAR company browse as an Atom feed, for an encoded ticker or CIK (the CIK fallback)."""
    return f"{SEC_WWW}/cgi-bin/browse-edgar?action=getcompany&CIK={company_param}&type=&dateb=&owner=include&count=10&output=atom"


def _manual_lookup_payload(ticker: str, cik_padded: str | None, filing_type: str, note: str) -> dict:
    edgar_index_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_padded}&type={filing_type}&owner=include&count=10"
        if cik_padded
        else f"https://www.sec.gov/cgi-bin/browse-edgar?company={ticker}&action=getcompany&type={filing_type}&owner=include&count=10"
    )
    efts_url = (
        "https://efts.sec.gov/LATEST/search-index"
        f"?q={_urlparse.quote(ticker)}&forms={_urlparse.quote(filing_type)}"
    )
    return {
        "edgarIndexUrl": edgar_index_url,
        "eftsSearchUrl": efts_url,
        "note": note,
    }
