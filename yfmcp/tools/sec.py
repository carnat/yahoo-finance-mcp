"""SEC EDGAR retrieval MCP tools.

Phase 5 unit 1: extracted from server.py as a pure move (no behaviour change).
Covers filing listing, outline/section/table/index retrieval, filing fact and text
search (``get_filing_data``, ``search_filing_text`` and their canonical ``*_sec_*``
aliases), material-filing lists, the filing-intelligence composite and the
Markdown section reader, together with their helpers and module constants.

Shared dependencies are imported from ``yfmcp.*``; this module never imports
``server`` (no import cycle). ``server.py`` re-exports the names that remaining
server code, grouped routing, scripts and tests still look up as ``server.<name>``.
"""

import datetime
import html as _html_module
import json
import re as _re
import threading
import time
import urllib.error as _urlerror
from typing import Annotated, Any

from pydantic import Field

from yfmcp import companyfacts_coverage as _cfc
from yfmcp import filing_search as _fs
from yfmcp import sec_facts as _sf
from yfmcp.app import yfinance_server
from yfmcp.cache import TTL_EDGAR, _tool_cache
from yfmcp.clients.edgar import (
    EdgarError,
    _edgar_build_filing_urls,
    _edgar_get,
    _edgar_get_company_facts,
    _edgar_get_document,
    _edgar_get_html,
    _edgar_list_exhibits_from_index,
    _edgar_read_json,
    _edgar_read_submissions,
    _get_submissions_for_ticker,
    _load_edgar_tickers,
    _resolve_cik_for_ticker,
)
from yfmcp.clients.endpoints import (
    _manual_lookup_payload,
    sec_archive_document_url,
    sec_company_concept_url,
    sec_company_facts_url,
    sec_filing_index_url,
)
from yfmcp.deprecations import TOOL_RELIABILITY_CLASS as _TOOL_RELIABILITY_CLASS
from yfmcp.envelope import ErrorCode, _classify_error_message, _mcp_failure
from yfmcp.fiscal_calendar import document_fiscal_year_focus, filing_fiscal_year_label
from yfmcp.parsing.extractors import (
    _extract_geo_revenue_from_html,
    _extract_xbrl_latest_annual,
    _geo_region_aliases,
    _js_round,
    _normalize_segment_label,
    _region_matches,
)
from yfmcp.parsing.html import (
    _UNIT_SCALE_ASSUMED_WARNING,
    _clean_filing_display_text,
    _html_to_markdown_fallback,
    _looks_like_filing_markup_text,
    _strip_html_blocks,
    _strip_html_tags,
    _table_unit_scale,
)
from yfmcp.schemas import FilingFactType
from yfmcp.tools.news import _safe_sec_url
from yfmcp.util import _fy_number, _js_json_numbers, _utf16_length
from yfmcp.validation import _sanitize_sec_html, _validate_accession, _validate_sec_url, _validate_ticker


def _is_likely_xbrl_document_url(url: str) -> bool:
    """Same test as the Worker's isLikelyXbrlDocumentUrl."""
    lower = url.split("?", 1)[0].lower()
    return lower.endswith(".xml") or "/xbrl/" in lower


async def list_sec_filings(ticker: str, form_type: str = "10-K", max_filings: int = 5) -> str:
    ALLOWED_FORMS = {"10-K", "10-Q", "8-K", "DEF 14A"}
    if form_type not in ALLOWED_FORMS:
        return _mcp_failure("list_sec_filings", ErrorCode.INPUT_VALIDATION_ERROR,
                            f"Invalid form_type '{form_type}'. Must be one of: {sorted(ALLOWED_FORMS)}")
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_sec_filings", ErrorCode.INPUT_VALIDATION_ERROR, err)
    max_filings = min(max(1, max_filings), 20)

    # Through the EDGAR client (2.26.0, PHASE4_SPEC §3.6): the Worker's full CIK resolution and the 15-minute
    # submissions cache. A miss keeps its TICKER_NOT_FOUND message and a failed submissions read keeps urllib's text.
    try:
        cik_padded = await _resolve_cik_for_ticker(ticker)
        if not cik_padded:
            return _mcp_failure("list_sec_filings", ErrorCode.TICKER_NOT_FOUND,
                                f"Could not find EDGAR CIK for ticker '{ticker}'")
        cik = int(cik_padded)
        try:
            sub_data = await _edgar_read_submissions(cik_padded)
        except EdgarError as sub_err:
            return _mcp_failure("list_sec_filings", ErrorCode.PROVIDER_ERROR, str(sub_err.__cause__ or sub_err))

        filings_data = sub_data.get("filings", {}).get("recent", {})
        forms = filings_data.get("form", [])
        dates = filings_data.get("filingDate", [])
        accessions = filings_data.get("accessionNumber", [])
        primary_docs = filings_data.get("primaryDocument", [])

        results = []
        for i, form in enumerate(forms):
            if form == form_type and len(results) < max_filings:
                acc = accessions[i] if i < len(accessions) else ""
                date = dates[i] if i < len(dates) else ""
                doc = primary_docs[i] if i < len(primary_docs) else ""
                acc_clean = acc.replace("-", "")
                index_url = sec_filing_index_url(cik, acc_clean, acc)
                doc_url = sec_archive_document_url(cik, acc_clean, doc) if doc else None
                results.append({
                    "accessionNumber": acc,
                    "filingDate": date,
                    "formType": form,
                    "primaryDocumentUrl": doc_url,
                    "edgarIndexUrl": index_url,
                })

        return json.dumps({"ticker": ticker, "formType": form_type, "filings": results})
    except Exception as e:
        return _mcp_failure("list_sec_filings", ErrorCode.PROVIDER_ERROR, str(e))


def _outline_read_failure(message: str) -> str:
    """A failed outline read: a 429 stays RATE_LIMIT and a timeout PROVIDER_TIMEOUT (the Worker's classifier), else PROVIDER_ERROR."""
    classified = _classify_error_message(message)
    return _mcp_failure("get_filing_outline", classified[0] if classified else ErrorCode.PROVIDER_ERROR, message)


async def get_filing_outline(ticker: str, accession_number: str | None = None, document_url: str | None = None) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if document_url:
        url_err = _validate_sec_url(document_url)
        if url_err:
            return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR, url_err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)

    try:
        resolved_accession = accession_number or None
        if not document_url and accession_number:
            # An explicit accession without document_url: the outline of that filing's primary document, found by the
            # same resolver as the filing index; a miss is ACCESSION_NOT_FOUND (2.13.0, R4 Q3a). document_url wins.
            resolved_filing, resolve_error = await _resolve_sec_filing(ticker, "10-K", accession_number)
            if resolved_filing is None:
                resolve_error = resolve_error or {}
                if _is_accession_not_found(resolve_error):
                    return _accession_not_found_envelope(resolve_error)
                return json.dumps({"ok": False, "error": resolve_error, **resolve_error})
            document_url = resolved_filing["documentUrl"]
            resolved_accession = resolved_filing.get("accessionNumber")

        if not document_url:
            return _mcp_failure("get_filing_outline", ErrorCode.INPUT_VALIDATION_ERROR,
                                "Either accession_number or document_url is required")

        try:
            html = await _edgar_get_document(document_url)
        except _urlerror.HTTPError as http_err:
            return _outline_read_failure(f"HTTP {http_err.code}")

        html = _sanitize_sec_html(html)
        outline = []
        heading_re = _re.compile(r'<h([1-6])[^>]*>(.*?)</h\1>', _re.IGNORECASE | _re.DOTALL)
        item_re = _re.compile(r'(Part\s+[IVX]+|Item\s+\d+[A-Z]?|Note\s+\d+)', _re.IGNORECASE)
        for m in heading_re.finditer(html):
            level = int(m.group(1))
            # Inner tags become a space, as in the Worker: "Item<b>1</b>" reads "Item 1" (2.26.0, F2).
            text = ' '.join(_re.sub(r'<[^>]+>', ' ', m.group(2)).split())
            if text and (item_re.search(text) or len(text) < 100):
                outline.append({"level": level, "title": text})

        outline = outline[:100]
        if not outline:
            outline = [{"level": h.level, "title": h.title} for h in _fs.filing_item_headings(html)[:100]]
        table_count = len(_re.findall(r'<table\b', html, _re.IGNORECASE))
        return json.dumps({
            "ticker": ticker,
            "accessionNumber": resolved_accession,
            "documentUrl": document_url,
            "outline": outline,
            "status": "OK" if outline else ("OUTLINE_NOT_PARSED" if table_count > 0 else "EMPTY"),
            "tableCount": table_count,
            "warnings": [{
                "code": "TABLES_FOUND_OUTLINE_EMPTY",
                "message": "Filing tables were detected but no section outline headings were parsed.",
                "severity": "warning",
            }] if not outline and table_count > 0 else [],
        })
    except Exception as e:
        return _outline_read_failure(str(e))


async def get_filing_section(ticker: str, section_name: str, document_url: str, context_chars: int = 3000) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_filing_section", ErrorCode.INPUT_VALIDATION_ERROR, err)
    url_err = _validate_sec_url(document_url)
    if url_err:
        return _mcp_failure("get_filing_section", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        html = await _edgar_get_document(document_url)

        html = _sanitize_sec_html(html)
        start_idx, end_idx, found_heading, toc_skipped, err_code = _find_section_bounds(html, section_name, context_chars)
        if err_code == "SECTION_AMBIGUOUS":
            return _mcp_failure("get_filing_section", "SECTION_AMBIGUOUS", "The section heading could not be resolved unambiguously.")

        if start_idx is not None and end_idx is not None:
            section_html = html[start_idx:end_idx]
            plain_section = _html_module.unescape(_re.sub(r'<[^>]+>', ' ', section_html))
            plain_section = ' '.join(plain_section.split())
            return json.dumps({
                "ticker": ticker,
                "sectionName": section_name,
                "status": "OK",
                "found": True,
                "text": plain_section[:context_chars],
                "sectionStartOffset": start_idx,
                "sectionEndOffset": end_idx,
                "matchedHeading": found_heading,
                "tocSkipped": toc_skipped,
            })

        if _section_item_token(section_name):
            return json.dumps({
                "ticker": ticker,
                "sectionName": section_name,
                "status": "SECTION_STRUCTURE_NOT_RESOLVED",
                "found": False,
                "text": None,
                "sectionStartOffset": None,
                "sectionEndOffset": None,
                "matchedHeading": "",
                "tocSkipped": toc_skipped,
                "decisionGrade": False,
                "recommendedNextAction": "GET_FILING_OUTLINE",
            })

        # Non-Item labels retain the legacy plain-text fallback.
        text = _html_module.unescape(_re.sub(r'<[^>]+>', ' ', html))
        text = ' '.join(text.split())

        pattern = _re.compile(_re.escape(section_name), _re.IGNORECASE)
        m = pattern.search(text)
        if not m:
            words = section_name.split()
            if words:
                pattern2 = _re.compile(r'\b' + r'\s+'.join(_re.escape(w) for w in words), _re.IGNORECASE)
                m = pattern2.search(text)

        if not m:
            return json.dumps({
                "ticker": ticker,
                "sectionName": section_name,
                "status": "SECTION_NOT_FOUND",
                "found": False,
                "text": None,
                "sectionStartOffset": None,
                "sectionEndOffset": None,
                "matchedHeading": "",
                "tocSkipped": toc_skipped,
            })

        start = max(0, m.start())
        end = min(len(text), m.start() + context_chars)
        return json.dumps({
            "ticker": ticker,
            "sectionName": section_name,
            "status": "TEXT_FALLBACK",
            "found": True,
            "text": text[start:end],
            "sectionStartOffset": start,
            "sectionEndOffset": end,
            "matchedHeading": section_name,
            "tocSkipped": toc_skipped,
        })
    except Exception as e:
        return _mcp_failure("get_filing_section", ErrorCode.PROVIDER_ERROR, str(e))


def _parse_filing_table_rows(table_html: str, max_rows: int | None = None) -> list[list[str]]:
    tr_re = _re.compile(r'<tr[^>]*>(.*?)</tr>', _re.DOTALL | _re.IGNORECASE)
    td_re = _re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', _re.DOTALL | _re.IGNORECASE)
    parsed: list[list[str]] = []
    for row_html in tr_re.findall(table_html):
        cells = []
        for cell_html in td_re.findall(row_html):
            cell = _html_module.unescape(_re.sub(r'<[^>]+>', ' ', cell_html))
            cells.append(" ".join(cell.replace("\xa0", " ").split()))
        if any(cells):
            parsed.append(cells)
        if max_rows is not None and len(parsed) >= max_rows:
            break
    return parsed


def _filing_table_is_usable(rows: list[list[str]]) -> bool:
    return sum(1 for row in rows for cell in row if cell.strip()) >= 2


async def list_filing_tables(
    ticker: str,
    document_url: str,
    offset: int = 0,
    limit: int = 50,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_filing_tables", ErrorCode.INPUT_VALIDATION_ERROR, err)
    url_err = _validate_sec_url(document_url)
    if url_err:
        return _mcp_failure("list_filing_tables", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        html = await _edgar_get_document(document_url)

        html = _sanitize_sec_html(html)
        tables = []
        table_re = _re.compile(r'<table[^>]*>(.*?)</table>', _re.DOTALL | _re.IGNORECASE)

        raw_tables = list(table_re.finditer(html))
        for i, table_m in enumerate(raw_tables):
            rows = _parse_filing_table_rows(table_m.group(1))
            if not _filing_table_is_usable(rows):
                continue
            tables.append({
                "tableIndex": i,
                "rowCount": len(rows),
                "title": None,
                "headers": rows[0][:6],
                "qualityStatus": "USABLE",
            })

        safe_offset = max(0, int(offset))
        safe_limit = min(100, max(1, int(limit or 50)))
        page = tables[safe_offset:safe_offset + safe_limit]
        return json.dumps({
            "ticker": ticker,
            "documentUrl": document_url,
            "status": "OK" if tables else "NO_USABLE_TABLES",
            "tableCount": len(raw_tables),
            "usableTableCount": len(tables),
            "excludedTableCount": len(raw_tables) - len(tables),
            "returnedCount": len(page),
            "offset": safe_offset,
            "limit": safe_limit,
            "hasMore": safe_offset + len(page) < len(tables),
            "tables": page,
            "recommendedNextAction": "GET_SEC_FILING_TABLE" if tables else "SEARCH_FILING_TEXT",
        })
    except Exception as e:
        return _mcp_failure("list_filing_tables", ErrorCode.PROVIDER_ERROR, str(e))


async def get_filing_table(ticker: str, document_url: str, table_index: int, max_rows: int = 30) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_filing_table", ErrorCode.INPUT_VALIDATION_ERROR, err)
    url_err = _validate_sec_url(document_url)
    if url_err:
        return _mcp_failure("get_filing_table", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        html = await _edgar_get_document(document_url)

        html = _sanitize_sec_html(html)
        table_re = _re.compile(r'<table[^>]*>(.*?)</table>', _re.DOTALL | _re.IGNORECASE)
        tables = list(table_re.finditer(html))
        if table_index < 0 or table_index >= len(tables):
            return _mcp_failure("get_filing_table", ErrorCode.NO_FILING_DATA,
                                f"Table index {table_index} not found. Document has {len(tables)} tables.")

        table_html = tables[table_index].group(1)
        all_rows = _parse_filing_table_rows(table_html)
        if not _filing_table_is_usable(all_rows):
            return json.dumps({
                "ticker": ticker,
                "tableIndex": table_index,
                "status": "UNUSABLE_TABLE",
                "decisionGrade": False,
                "totalRows": len(all_rows),
                "returnedRows": 0,
                "rows": [],
                "recommendedNextAction": "LIST_USABLE_TABLES",
            })
        safe_max_rows = min(100, max(1, int(max_rows)))
        parsed_rows = all_rows[:safe_max_rows]

        return json.dumps({
            "ticker": ticker,
            "tableIndex": table_index,
            "status": "OK",
            "decisionGrade": False,
            "totalRows": len(all_rows),
            "returnedRows": len(parsed_rows),
            "rows": parsed_rows,
            "recommendedNextAction": "VERIFY_TABLE_PERIOD_AND_UNITS",
        })
    except Exception as e:
        return _mcp_failure("get_filing_table", ErrorCode.PROVIDER_ERROR, str(e))


async def extract_filing_fact(
    ticker: str,
    fact_name: str,
    document_url: str | None = None,
    accession_number: str | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("extract_filing_fact", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if document_url:
        url_err = _validate_sec_url(document_url)
        if url_err:
            return _mcp_failure("extract_filing_fact", ErrorCode.INPUT_VALIDATION_ERROR, url_err)

    try:
        result_str = await search_filing_text(
            ticker=ticker,
            search_terms=[fact_name],
            filing_type="10-K",
            accession_number=accession_number,
            context_chars=1000,
            return_tables=True,
        )
        result = json.loads(result_str)
        if isinstance(result, dict) and result.get("ok") is False and _is_accession_not_found(result.get("error")):
            return _accession_not_found_envelope(result["error"])
        return json.dumps({
            "ticker": ticker,
            "factName": fact_name,
            "extractionMethod": "text_search",
            "result": result,
        })
    except Exception as e:
        return _mcp_failure("extract_filing_fact", ErrorCode.PROVIDER_ERROR, str(e))


# ---------------------------------------------------------------------------
# Group 3.4b — get_filing_data / search_filing_text
# ---------------------------------------------------------------------------

# Equivalent concepts are all read; the one filed most recently wins (yfmcp/sec_facts.py).
_FILING_FACT_ALTERNATES: dict[FilingFactType, list[str]] = {
    FilingFactType.geographic_revenue: _sf.REVENUE_CONCEPTS[1:],
    FilingFactType.segment_revenue: _sf.REVENUE_CONCEPTS[1:],
    FilingFactType.total_revenue: _sf.REVENUE_CONCEPTS[1:],
}


_FILING_FACT_CONCEPTS: dict[FilingFactType, tuple[str, str | None]] = {
    FilingFactType.geographic_revenue: (_sf.REVENUE_CONCEPTS[0], None),
    FilingFactType.segment_revenue: (_sf.REVENUE_CONCEPTS[0], None),
    FilingFactType.capex: ("PaymentsToAcquirePropertyPlantAndEquipment", None),
    FilingFactType.rd_expense: ("ResearchAndDevelopmentExpense", None),
    FilingFactType.operating_income: ("OperatingIncomeLoss", None),
    FilingFactType.net_income: ("NetIncomeLoss", None),
    FilingFactType.total_revenue: (_sf.REVENUE_CONCEPTS[0], None),
    FilingFactType.long_term_debt: ("LongTermDebt", None),
    FilingFactType.cash: ("CashAndCashEquivalentsAtCarryingValue", None),
}


def _accession_not_found_error(ticker: str, accession_number: str) -> dict:
    """The Worker's accessionNotFoundError: an explicit accession missing from recent filings fails closed (2.13.0, R4 Q3a)."""
    return {
        "code": ErrorCode.ACCESSION_NOT_FOUND,
        "message": f"Accession {accession_number} was not found in {ticker.upper()}'s recent SEC filings.",
        "requestedAccession": accession_number,
        "retryable": False,
    }


def _is_accession_not_found(error: dict | None) -> bool:
    return isinstance(error, dict) and error.get("code") == ErrorCode.ACCESSION_NOT_FOUND


def _accession_not_found_envelope(error: dict) -> str:
    """The ACCESSION_NOT_FOUND error envelope a public action returns."""
    return json.dumps({"ok": False, "error": error})


async def _legacy_accession_miss_payload(ticker: str, filing_type: str) -> dict:
    """The Worker's legacyAccessionMissPayload: the filing-not-found payload the out-of-scope readers returned before 2.13.0."""
    _cik, subs = await _get_submissions_for_ticker(ticker)
    recent = ((subs or {}).get("filings") or {}).get("recent") or {}
    forms = [str(f) for f in (recent.get("form") or [])]
    available = list(dict.fromkeys(f.upper() for f in forms if f))[:12]
    return _filing_not_found(ticker, (filing_type or "10-K").upper(), available)


async def _resolve_sec_filing(ticker: str, requested_filing_type: str = "10-K", accession_number: str | None = None) -> tuple[dict | None, dict | None]:
    """The Worker's resolveSecFiling: (filing, None), or (None, error) with the Worker's error payloads.

    A 10-K that is not filed falls to the latest 20-F (AUTO_20F_FALLBACK); a pinned accession is matched exactly.
    """
    requested = (requested_filing_type or "10-K").upper()
    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return None, {"status": "TICKER_NOT_FOUND", "code": "TICKER_NOT_FOUND", "ticker": ticker,
                      "message": f"Could not resolve EDGAR submissions for ticker '{ticker}'"}
    recent = (subs.get("filings") or {}).get("recent") or {}
    forms = [str(f) for f in (recent.get("form") or [])]
    accessions = recent.get("accessionNumber") or []
    primary_docs = recent.get("primaryDocument") or []
    filing_dates = recent.get("filingDate") or []
    report_dates = recent.get("reportDate") or []
    accepted = recent.get("acceptanceDateTime") or []
    available = list(dict.fromkeys(f.upper() for f in forms if f))[:12]

    warnings: list[dict] = []
    target: int | None = None
    if accession_number:
        target = next((i for i, a in enumerate(accessions) if a == accession_number), None)
    else:
        target = next((i for i, f in enumerate(forms) if f.upper() == requested), None)
        if target is None and requested == "10-K":
            target = next((i for i, f in enumerate(forms) if f.upper() == "20-F"), None)
            if target is not None:
                warnings.append({
                    "code": "AUTO_20F_FALLBACK",
                    "message": "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
                    "severity": "info",
                })
    if accession_number and target is None:
        return None, _accession_not_found_error(ticker, accession_number)
    if target is None or target >= len(accessions) or not accessions[target]:
        return None, _filing_not_found(ticker, requested, available)

    def _at(values: list, default: object = None) -> object:
        return values[target] if target < len(values) and values[target] is not None else default

    primary_document = str(_at(primary_docs, ""))
    filing_type = str(forms[target]) if target < len(forms) else requested
    if not primary_document:
        return None, {
            "status": "FILING_TEXT_NOT_AVAILABLE", "code": "FILING_TEXT_NOT_AVAILABLE", "ticker": ticker,
            "requestedFilingType": requested, "filingType": filing_type, "filingDate": _at(filing_dates),
            "accessionNumber": accessions[target], "documentUrl": None, "availableFilingTypes": available,
            "suggestedFilingTypes": [],
            "warnings": [{"code": "PRIMARY_DOCUMENT_MISSING", "message": "SEC submissions entry has no primaryDocument.", "severity": "error"}],
        }
    cik_int = int(cik_padded)
    _, document_url = _edgar_build_filing_urls(cik_int, accessions[target], primary_document)
    if not document_url or _is_likely_xbrl_document_url(document_url):
        return None, {
            "status": "FILING_TEXT_NOT_AVAILABLE", "code": "FILING_TEXT_NOT_AVAILABLE", "ticker": ticker,
            "requestedFilingType": requested, "filingType": filing_type, "filingDate": _at(filing_dates),
            "accessionNumber": accessions[target], "documentUrl": document_url, "availableFilingTypes": available,
            "suggestedFilingTypes": [],
            "warnings": [{"code": "PRIMARY_HTML_NOT_FOUND", "message": "Could not resolve a human-readable primary filing HTML document.", "severity": "error"}],
        }
    return {
        "ticker": ticker, "cikPadded": cik_padded, "cikInt": cik_int, "requestedFilingType": requested,
        "filingType": filing_type, "filingDate": _at(filing_dates), "reportDate": (report_dates[target] if target < len(report_dates) else None) or None,
        "acceptedAt": _at(accepted), "accessionNumber": accessions[target], "primaryDocument": primary_document,
        "documentUrl": document_url, "availableFilingTypes": available,
        "suggestedFilingTypes": ["20-F"] if requested == "10-K" and filing_type.upper() == "20-F" else [],
        "warnings": warnings,
    }, None


def _filing_has_relevant_geo_text(html: str, region: str) -> bool:
    """The Worker's filingHasRelevantGeoText: a region mention within 4,000 characters of revenue, sales, geographic or segment."""
    html_lower = html.lower()
    aliases = _geo_region_aliases(region)
    if not aliases:
        return False
    start = 0
    for _ in range(20):
        positions = [pos for pos in (html_lower.find(term, start) for term in aliases) if pos >= 0]
        if not positions:
            return False
        pos = min(positions)
        if _re.search(r"revenue|sales|geographic|segment", html_lower[max(0, pos - 4_000): min(len(html_lower), pos + 4_000)]):
            return True
        start = pos + 1
    return False


_ANNUAL_FILING_TYPE_RE = _re.compile(r"(?:10-K|20-F|40-F)\b", _re.IGNORECASE)


async def _annual_coverage_warning_for(ticker: str, payload: dict) -> dict | None:
    """The Worker's annualCoverageWarningFor (2.5.30, F-015): the LATEST_ANNUAL_NOT_IN_COMPANYFACTS warning for an
    annual-form fact read, looked for only when the read has no value or its value is from another accession than
    the latest annual report. Unreadable SEC data gives none."""
    try:
        cik_padded, subs = await _get_submissions_for_ticker(ticker)
    except Exception:  # noqa: BLE001 - no submissions, no check
        return None
    if not cik_padded or not subs:
        return None
    filings = _cfc.annual_filings_from_submissions(subs)
    latest = _cfc.latest_annual_filing(filings)
    used = str(payload["accessionNumber"]).replace("-", "") if payload.get("value") is not None and isinstance(payload.get("accessionNumber"), str) else None
    if latest is None or used == latest["accessionNumber"].replace("-", ""):
        return None
    try:
        # Read as get_filing_data reads companyfacts; an unreadable file gives no warning.
        facts = await _edgar_get(sec_company_facts_url(cik_padded))
    except Exception:  # noqa: BLE001
        facts = None
    return _cfc.latest_annual_coverage_warning(filings, facts) if isinstance(facts, dict) else None


async def get_filing_data(
    ticker: str,
    fact_type: FilingFactType,
    region: str | None = None,
    filing_type: str = "10-K",
    period: str = "latest",
    period_mode: str = "auto",
    accession_number: str | None = None,
) -> str:
    raw = await _get_filing_data_unchecked(ticker, fact_type, region, filing_type, period, period_mode, accession_number)
    if not _ANNUAL_FILING_TYPE_RE.match(filing_type or ""):
        return raw
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return raw
    if not isinstance(payload, dict):
        return raw
    # A failed read, an unresolved ticker or a rejected period says nothing about what companyfacts holds.
    if payload.get("status") == "PROVIDER_ERROR" or payload.get("code") in ("NO_SEC_REGISTRANT", "SEC_LOOKUP_UNAVAILABLE", "INVALID_PERIOD"):
        return raw
    warning = await _annual_coverage_warning_for(ticker, payload)
    if not warning:
        return raw
    payload["warnings"] = [*(payload.get("warnings") if isinstance(payload.get("warnings"), list) else []), warning]
    return json.dumps(_js_json_numbers(payload))


async def _get_filing_data_unchecked(
    ticker: str,
    fact_type: FilingFactType,
    region: str | None = None,
    filing_type: str = "10-K",
    period: str = "latest",
    period_mode: str = "auto",
    accession_number: str | None = None,
) -> str:
    FLOATING_POINT_EPSILON = 1e-9
    PCT_MULTIPLIER = 100

    def _format_raw_number(n: float | int | None) -> str | None:
        if n is None:
            return None
        try:
            f = float(n)
            if abs(f - round(f)) < FLOATING_POINT_EPSILON:
                return f"{int(round(f)):,}"
            return f"{f:,.2f}"
        except Exception:
            return None

    # Warnings raised while reading SEC facts, carried on every result (2.5.15).
    read_warnings: list[dict] = []

    def _geo_shape(payload: dict, *, warn_denominator: bool = False, add_read_warnings: bool = True) -> str:
        """The Worker's withGeoShape: the geographic_revenue payload, with every key it carries, in its order."""
        if read_warnings and add_read_warnings:
            payload = {**payload, "warnings": [*(payload["warnings"] if isinstance(payload.get("warnings"), list) else []), *read_warnings]}
        if fact_type != FilingFactType.geographic_revenue:
            return json.dumps(_js_json_numbers(payload))

        def _field(key: str, default: object = None) -> object:
            # JavaScript's `payload.key ?? default`: only null/missing falls back, never "" or 0.
            value = payload.get(key)
            return default if value is None else value

        warnings = list(payload["warnings"]) if isinstance(payload.get("warnings"), list) else []
        has_denominator = payload.get("denominator") is not None
        shaped: dict = {
            "ticker": _field("ticker", ticker),
            "factType": _field("factType", FilingFactType.geographic_revenue.value),
            "region": _field("region", region),
            "period": _field("period"),
            "rawValue": _field("rawValue"),
            "rawDenominator": _field("rawDenominator"),
            "unit": _field("unit", "USD"),
            "unitScale": _field("unitScale", "actual"),
            # How a parsed table's scale was known (2.5.21); null for XBRL facts and when nothing was parsed.
            "unitScaleSource": _field("unitScaleSource"),
            "value": _field("value"),
            "denominator": _field("denominator"),
            "valueRatio": _field("valueRatio") if has_denominator else None,
            "valuePct": _field("valuePct") if has_denominator else None,
            "extractionMethod": _field("extractionMethod", "NONE"),
            "source": _field("source", "NOT_DISCLOSED"),
            "confidence": _field("confidence", "NOT_DISCLOSED"),
            "filingType": _field("filingType", filing_type),
            "filingDate": _field("filingDate"),
            "accessionNumber": _field("accessionNumber"),
            "documentUrl": _field("documentUrl"),
            "indexUrl": _field("indexUrl"),
            "primaryDocumentUrl": _field("primaryDocumentUrl"),
            "evidence": _field("evidence", {}),
            "calculation": _field("calculation"),
            "scanCoverage": _field("scanCoverage"),
            "searchedTerms": _field("searchedTerms", []),
            "notDisclosedBasis": _field("notDisclosedBasis"),
            "status": _field("status", "FOUND" if payload.get("value") is not None else _field("confidence", "NOT_DISCLOSED")),
            "code": _field("code"),
            **({"retryable": payload["retryable"]} if isinstance(payload.get("retryable"), bool) else {}),
            "xbrlContext": _field("xbrlContext"),
            "warnings": warnings,
        }
        if (
            shaped["value"] is not None
            and shaped["extractionMethod"] == "XBRL"
            and shaped["xbrlContext"] is None
            and not any(isinstance(w, dict) and w.get("code") == "XBRL_CONTEXT_METADATA_UNAVAILABLE" for w in warnings)
        ):
            warnings.append({
                "code": "XBRL_CONTEXT_METADATA_UNAVAILABLE",
                "message": "XBRL fact value was found, but SEC context metadata was unavailable.",
                "severity": "warning",
            })
        if warn_denominator and shaped["value"] is not None and shaped["denominator"] is None:
            warnings.append({
                "code": "DENOMINATOR_NOT_FOUND",
                "message": "Could not compute geographic revenue percentage due to missing denominator.",
                "severity": "warning",
            })
        return json.dumps(_js_json_numbers(shaped))

    async def _resolve_filing_urls_for_accession(accn: str) -> tuple[str | None, str | None]:
        if not accn:
            return None, None
        if not cik_padded:
            return None, None
        index_url, primary_url = _edgar_build_filing_urls(int(cik_padded), accn, None)
        _, subs = await _get_submissions_for_ticker(ticker)
        if not subs:
            return index_url, primary_url
        recent = subs.get("filings", {}).get("recent", {})
        accessions: list[str] = recent.get("accessionNumber", [])
        primary_docs: list[str] = recent.get("primaryDocument", [])
        try:
            idx = accessions.index(accn)
            primary_doc = primary_docs[idx] if idx < len(primary_docs) else None
            if primary_doc:
                _, primary_url = _edgar_build_filing_urls(int(cik_padded), accn, primary_doc)
        except Exception:
            pass
        return index_url, primary_url

    concept_primary, concept_fallback = _FILING_FACT_CONCEPTS[fact_type]
    # A period this reader cannot resolve is refused, never answered with an arbitrary fact (2.5.16).
    named_period = _sf.parse_filing_period(period)
    if named_period is None:
        return json.dumps({
            "ticker": ticker, "factType": fact_type.value, "value": None, "unit": None, "period": None, "filingType": filing_type,
            "extractionMethod": "NONE", "source": "INPUT", "confidence": "NOT_DECISION_GRADE",
            "status": "INVALID_PERIOD", "code": "INVALID_PERIOD", "decisionGrade": False, "requestedPeriod": period,
            "warnings": [{"code": "INVALID_PERIOD", "message": _sf.FILING_PERIOD_HELP, "severity": "error"}],
        })
    if fact_type == FilingFactType.geographic_revenue and not region:
        return json.dumps({"error": True, "message": "region is required for fact_type='geographic_revenue'"})

    cik_padded = await _resolve_cik_for_ticker(ticker)
    if not cik_padded:
        # No CIK is a lookup outcome, not a disclosure: say whether SEC's ticker
        # index was unreachable (retry) or has no such registrant, and keep the
        # accession a caller pinned.
        ticker_index = await _load_edgar_tickers()
        code = "NO_SEC_REGISTRANT" if ticker_index else "SEC_LOOKUP_UNAVAILABLE"
        requested = accession_number.strip() if accession_number and accession_number.strip() else None
        message = (
            f"SEC has no registrant CIK for ticker {ticker.upper()}." if ticker_index
            else f"SEC's ticker index could not be read, so {ticker.upper()} was not resolved to a CIK; retry."
        )
        return _geo_shape({
            "ticker": ticker,
            "factType": fact_type.value,
            "value": None,
            "denominator": None,
            "valueRatio": None,
            "valuePct": None,
            "filingType": filing_type,
            "accessionNumber": requested,
            **({"requestedAccession": requested} if requested else {}),
            "extractionMethod": "NONE",
            "source": "NONE",
            "confidence": "NOT_DECISION_GRADE",
            # An unreadable ticker index is a failed read, like SEC_READ_FAILED, not a missing fact (2.5.18).
            "status": "PROVIDER_ERROR" if code == "SEC_LOOKUP_UNAVAILABLE" else "SEC_FACT_NOT_AVAILABLE",
            "code": code,
            "retryable": code == "SEC_LOOKUP_UNAVAILABLE",
            "evidence": None,
            "warnings": [{"code": code, "message": message, "severity": "warning"}],
            "_manualLookup": _manual_lookup_payload(
                ticker, None, filing_type, "Fact not XBRL-tagged. Use search_filing_text instead."
            ),
        })

    # A read that failed (not a 404) leaves a fact's absence unproven: the result is SEC_READ_FAILED, never
    # "no facts" (2.5.17). Mirrors the Worker's readSecJson.
    failed_reads: list[dict] = []

    async def _read_sec_json(url: str, endpoint: str, concept_name: str | None) -> dict | None:
        read = await _edgar_read_json(url)
        if read["ok"]:
            return read["json"]
        failed_reads.append({"endpoint": endpoint, "concept": concept_name, "httpStatus": read["httpStatus"]})
        return None

    companyfacts_url = sec_company_facts_url(cik_padded)

    async def _concept_json(concept_name: str) -> dict | None:
        return await _read_sec_json(
            sec_company_concept_url(cik_padded, concept_name),
            "companyconcept",
            concept_name,
        )

    # Every equivalent concept is read; the one with the newest filing of this
    # form wins, so a filer that switched concepts is not read from its old one.
    candidate_names = [concept_primary, *_FILING_FACT_ALTERNATES.get(fact_type, []), *([concept_fallback] if concept_fallback else [])]
    # SEC's companyconcept endpoint has served a concept's USD facts as an empty object instead of a list (BE,
    # 2026-09): those facts are read from companyfacts, which carries them (2.5.15). Fetched at most once.
    malformed_concepts: list[str] = []
    companyfacts_read: list[dict | None] = []

    async def _concept_rows(name: str) -> list:
        data = await _concept_json(name)
        units = data.get("units") if isinstance(data, dict) else None
        usd = units.get("USD") if isinstance(units, dict) else None
        if usd is None or isinstance(usd, list):
            return usd or []
        malformed_concepts.append(name)
        if not companyfacts_read:
            companyfacts_read.append(await _read_sec_json(companyfacts_url, "companyfacts", None))
        cf = companyfacts_read[0]
        facts = cf.get("facts") if isinstance(cf, dict) else None
        us_gaap = facts.get("us-gaap") if isinstance(facts, dict) else None
        entry = us_gaap.get(name) if isinstance(us_gaap, dict) else None
        entry_units = entry.get("units") if isinstance(entry, dict) else None
        rows = entry_units.get("USD") if isinstance(entry_units, dict) else None
        return rows if isinstance(rows, list) else []

    async def _unavailable_structured_fact(
        code: str,
        message: str,
        concept_name: str | None,
        requested_accession: str | None = None,
        extra: dict | None = None,
    ) -> str:
        """The Worker's unavailableStructuredFact: no fact, why, and the latest filing of the form it looked in (2.5.16).

        A pinned accession is echoed as requestedAccession and no other filing is looked up. `extra` is applied last,
        as the Worker spreads it at the end of the payload: an existing key keeps its position, a new one is appended.
        """
        filing: dict | None = None
        filing_warnings: list[dict] = []
        resolved: list = []
        if not requested_accession:
            try:
                resolved, _error = await _resolve_periodic_filings(ticker, filing_type, None)
            except Exception:
                resolved = []
        if resolved:
            filing = resolved[0][1]
            if (filing_type or "").upper() == "10-K" and str(filing.get("filingType") or "").upper() == "20-F":
                filing_warnings.append({
                    "code": "AUTO_20F_FALLBACK",
                    "message": "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
                    "severity": "info",
                })
        payload: dict = {
            "ticker": ticker,
            "factType": fact_type.value,
            "concept": concept_name,
            "value": None,
            "denominator": None,
            "valueRatio": None,
            "valuePct": None,
            "rawValue": None,
            "rawDenominator": None,
            "unit": "USD",
            "unitScale": "actual",
            "period": None,
            "filingType": filing["filingType"] if filing else filing_type,
            "filingDate": filing.get("filingDate") if filing else None,
            "accessionNumber": requested_accession or (filing.get("accessionNumber") if filing else None),
            **({"requestedAccession": requested_accession} if requested_accession else {}),
            "documentUrl": filing.get("documentUrl") if filing else None,
            "indexUrl": None,
            "primaryDocumentUrl": filing.get("documentUrl") if filing else None,
            "extractionMethod": "NONE",
            "source": "SEC_COMPANYCONCEPT",
            "confidence": "NOT_DECISION_GRADE",
            "status": "SEC_FACT_NOT_AVAILABLE",
            "code": code,
            "decisionGrade": False,
            "evidence": {
                "sourceType": "sec_filing",
                "filingType": filing["filingType"],
                "filingDate": filing.get("filingDate"),
                "accessionNumber": filing.get("accessionNumber"),
                "documentUrl": filing.get("documentUrl"),
            } if filing else {},
            "xbrlContext": None,
            "calculation": None,
            "warnings": [*filing_warnings, *read_warnings, {"code": code, "message": message, "severity": "warning"}],
            "_manualLookup": _manual_lookup_payload(ticker, cik_padded, filing_type, "Fact not XBRL-tagged. Use search_filing_text instead."),
        }
        payload.update(extra or {})
        # Unshaped, as the Worker's unavailableStructuredFact: the geographic_revenue shape (_geo_shape) would drop status/code.
        return json.dumps(_js_json_numbers(payload))

    async def _sec_read_failed(concept_name: str | None) -> str:
        """The Worker's secReadFailed (2.5.17): a failed SEC read is not a missing fact; retry."""
        # Reads finish in network order; list them in candidate order, companyfacts last, so the result is stable.
        def _rank(r: dict) -> int:
            if r["endpoint"] != "companyconcept":
                return len(candidate_names)
            return candidate_names.index(r["concept"] or "") if (r["concept"] or "") in candidate_names else -1

        failed_reads.sort(key=_rank)
        reads = ", ".join(
            f"{r['endpoint']}{' ' + r['concept'] if r['concept'] else ''} ({'HTTP ' + str(r['httpStatus']) if r['httpStatus'] is not None else 'no response'})"
            for r in failed_reads
        )
        return await _unavailable_structured_fact(
            "SEC_READ_FAILED",
            f"SEC could not be read for {ticker.upper()}: {reads}. Whether the fact exists is unknown; retry.",
            concept_name,
            accession_number.strip() if accession_number and accession_number.strip() else None,
            {"status": "PROVIDER_ERROR", "retryable": True, "failedReads": list(failed_reads)},
        )

    fetched_concepts = []
    for name in candidate_names:
        fetched_concepts.append({"concept": name, "facts": await _concept_rows(name)})
    if malformed_concepts:
        read_warnings.append({
            "code": "SEC_COMPANYCONCEPT_MALFORMED",
            "message": f"SEC companyconcept returned no fact list for {', '.join(malformed_concepts)}; those facts were read from companyfacts.",
            "severity": "info",
        })
    if failed_reads:
        return await _sec_read_failed(concept_primary)
    pinned_accession = accession_number.strip() if accession_number and accession_number.strip() else None
    chosen = _sf.pick_concept_facts(fetched_concepts, filing_type, pinned_accession, "larger" if fact_type == FilingFactType.total_revenue else "first")
    concept_used = chosen["concept"] if chosen else concept_primary
    filtered = list(chosen["facts"]) if chosen else []
    # A pinned geographic read with no XBRL fact reads that filing's own table below, as the unpinned read does (2.23.0, F4).
    if not filtered and pinned_accession and fact_type != FilingFactType.geographic_revenue:
        # A pinned IFRS filing was searched for us-gaap concepts only: say IFRS was not read, not that the
        # filing has no such fact (2.5.30, F-018).
        if not companyfacts_read:
            companyfacts_read.append(await _read_sec_json(companyfacts_url, "companyfacts", None))
        cf = companyfacts_read[0]
        taxonomies = cf.get("facts") if isinstance(cf, dict) else None
        if failed_reads:
            return await _sec_read_failed(concept_used)
        if isinstance(taxonomies, dict) and taxonomies.get("ifrs-full") and not taxonomies.get("us-gaap"):
            return await _unavailable_structured_fact(
                "SEC_FACTS_IFRS_ONLY",
                f"{ticker.upper()} reports its SEC facts under IFRS (ifrs-full), which this action does not read, so accession "
                f"{pinned_accession} was searched for {' / '.join(candidate_names)} (us-gaap) only and its IFRS facts were not searched; "
                "reconcile_metric_sources reads IFRS facts in the reporting currency.",
                concept_used,
                pinned_accession,
            )
        # A failed pin names the filing that was asked for, never the latest one (the Worker's unavailableStructuredFact).
        return await _unavailable_structured_fact(
            "NO_FACT_FOR_ACCESSION",
            f"SEC companyconcept has no {' / '.join(candidate_names)} fact in accession {pinned_accession} ({filing_type}).",
            concept_used,
            pinned_accession,
        )
    if not filtered:
        if fact_type != FilingFactType.geographic_revenue:
            # An IFRS filer (TSM's 20-F) tags ifrs-full, never us-gaap: say so rather than report the fact
            # missing (2.5.15). Same code, status and warnings as the Worker's unavailableStructuredFact.
            if not companyfacts_read:
                companyfacts_read.append(await _read_sec_json(companyfacts_url, "companyfacts", None))
            cf = companyfacts_read[0]
            taxonomies = cf.get("facts") if isinstance(cf, dict) else None
            if failed_reads:
                return await _sec_read_failed(concept_used)
            if isinstance(taxonomies, dict) and taxonomies.get("ifrs-full") and not taxonomies.get("us-gaap"):
                return await _unavailable_structured_fact(
                    "SEC_FACTS_IFRS_ONLY",
                    f"{ticker.upper()} reports its SEC facts under IFRS (ifrs-full), which this action does not read; "
                    "reconcile_metric_sources reads IFRS facts in the reporting currency.",
                    concept_used,
                )
            # No fact for the form is a lookup outcome, not a non-disclosure: the Worker's payload, with the
            # latest filing of the form it looked in (2.5.16; was NOT_DISCLOSED with no code).
            return await _unavailable_structured_fact(
                "NO_COMPANYCONCEPT_FACT_FOR_FORM",
                f"SEC companyconcept has no {concept_used} facts for filing type {filing_type}.",
                concept_used,
            )
        # For geographic_revenue, fall through to HTML fallback below (picked remains None)

    if filtered and named_period["kind"] == "latest" and not pinned_accession:
        latest_filed = max(str(f.get("filed", "")) for f in filtered)
        filtered = [f for f in filtered if str(f.get("filed", "")) == latest_filed]

    # ── period_mode filtering ─────────────────────────────────────────────
    # XBRL facts include both quarterly and YTD figures for the same filing.
    # Filter by duration to avoid returning 6-month revenue as quarterly.
    resolved_mode = period_mode.lower().strip() if period_mode else "auto"
    if resolved_mode == "auto":
        resolved_mode = "quarter" if filing_type.upper() == "10-Q" else "annual"
    if resolved_mode in ("quarter", "annual", "ytd") and filtered:
        def _duration_days(f: dict) -> int | None:
            """Compute duration in days from XBRL start/end dates."""
            s, e = f.get("start"), f.get("end")
            if not s or not e:
                return None
            try:
                d0 = datetime.date.fromisoformat(str(s))
                d1 = datetime.date.fromisoformat(str(e))
                return (d1 - d0).days
            except (ValueError, TypeError):
                return None
        dur_tagged = [(f, _duration_days(f)) for f in filtered]
        if resolved_mode == "quarter":
            # Keep facts with duration 80-100 days (one quarter ~90 days)
            # Also keep instant facts (no start/end) and untagged duration
            mode_filtered = [f for f, d in dur_tagged if d is None or (60 <= d <= 110)]
        elif resolved_mode == "ytd":
            # Keep facts with year-to-date duration based on fiscal period (fp) or form type
            def _is_ytd_match(f: dict, d: int | None) -> bool:
                if d is None:
                    return True
                fp = str(f.get("fp") or "").upper().strip()
                if fp == "Q1":
                    return 60 <= d <= 110
                elif fp == "Q2":
                    return 150 <= d <= 200
                elif fp == "Q3":
                    return 240 <= d <= 290
                else:  # FY, Q4, or fallback
                    form = str(f.get("form") or "").upper().strip()
                    if form == "10-K":
                        return 340 <= d <= 400
                    return d >= 60
            mode_filtered = [f for f, d in dur_tagged if _is_ytd_match(f, d)]
        else:  # annual
            # Keep facts with duration 350-380 days (one year ~365 days)
            mode_filtered = [f for f, d in dur_tagged if d is None or (340 <= d <= 400)]
        if mode_filtered:
            filtered = mode_filtered
    # A named fiscal year is an annual period: the rows for the period the issuer calls that year (2.5.16).
    named_year_label: str | None = None
    if named_period["kind"] == "fiscalYear":
        if resolved_mode != "annual":
            return await _unavailable_structured_fact(
                "INVALID_PERIOD",
                f"period {period} names a fiscal year, an annual period; request filing_type 10-K or 20-F (or period_mode annual).",
                concept_used,
            )
        selected = _sf.select_fiscal_year_rows(filtered, named_period["year"])
        if not selected["rows"]:
            years_found = ", ".join(str(y) for y in selected["fiscalYears"][-6:]) or "none"
            return await _unavailable_structured_fact(
                "PERIOD_NOT_FOUND",
                f"No annual {concept_used} fact is fiscal year {named_period['year']} (the issuer's stated fiscal year, else the year the period ends); fiscal years found: {years_found}.",
                concept_used,
            )
        filtered = selected["rows"]
        named_year_label = f"FY{named_period['year']}"
    # A filing reports prior-year comparatives too: the newest period end comes
    # first, as in the Worker (ASTS's Q2 2026 10-Q also carries Q2 2025).
    if named_period["kind"] == "latest" and filtered:
        filtered = sorted(filtered, key=lambda f: (str(f.get("end") or ""), _fy_number(f.get("fy")), str(f.get("filed") or "")), reverse=True)

    if fact_type == FilingFactType.segment_revenue:
        seg_rows = []
        for f in filtered:
            seg_label = _normalize_segment_label(f.get("segment"))
            if not seg_label:
                continue
            seg_rows.append({
                "segmentLabel": seg_label,
                "value": f.get("val"),
                "fiscalYear": str(f.get("fy") or ""),
                "fiscalPeriod": str(f.get("fp") or ""),
                "filingDate": str(f.get("filed") or ""),
                "accessionNumber": str(f.get("accn") or ""),
            })
        return json.dumps(_js_json_numbers({
            "ticker": ticker,
            "factType": fact_type.value,
            "concept": concept_used,
            "value": seg_rows[0]["value"] if seg_rows else None,
            "fiscalYear": seg_rows[0]["fiscalYear"] if seg_rows else "",
            "fiscalPeriod": seg_rows[0]["fiscalPeriod"] if seg_rows else "",
            "filingType": filing_type,
            "filingDate": seg_rows[0]["filingDate"] if seg_rows else "",
            "accessionNumber": seg_rows[0]["accessionNumber"] if seg_rows else "",
            "extractionMethod": "XBRL",
            "source": "XBRL",
            "confidence": "HIGH",
            "allSegments": seg_rows,
        }))

    picked: dict | None = None
    value_ratio: float | None = None
    value_pct: float | None = None
    denominator: float | None = None
    segment_label: str | None = None
    if fact_type == FilingFactType.geographic_revenue:
        for f in filtered:
            seg_label = _normalize_segment_label(f.get("segment"))
            if seg_label and _region_matches(seg_label, region or "", include_asia_fallback=False):
                picked = f
                segment_label = seg_label
                break
        if picked is None and (region or "").lower() == "china":
            for f in filtered:
                seg_label = _normalize_segment_label(f.get("segment"))
                if seg_label and _region_matches(seg_label, region or "", include_asia_fallback=True):
                    picked = f
                    segment_label = seg_label
                    break
        if picked is not None:
            accn = str(picked.get("accn") or "")
            total_fact = next(
                (
                    # The total for the same period: a 10-Q also carries the prior year's total (2.5.9, MRVL).
                    f for f in filtered
                    if str(f.get("accn") or "") == accn and not f.get("segment")
                    and str(f.get("start") or "") == str(picked.get("start") or "") and str(f.get("end") or "") == str(picked.get("end") or "")
                ),
                None,
            )
            try:
                picked_val = picked.get("val")
                if total_fact and picked_val is not None and float(total_fact.get("val", 0)) > 0:
                    denominator = float(total_fact.get("val", 0))
                    # JavaScript's Math.round (halves up), as the Worker's ratio and ratioToPct.
                    value_ratio = _js_round((float(picked_val) / denominator) * 10000) / 10000
                    value_pct = _js_round(value_ratio * PCT_MULTIPLIER * 100) / 100
            except Exception:
                denominator = None
                value_ratio = None
                value_pct = None
    else:
        picked = next((f for f in filtered if not f.get("segment")), filtered[0] if filtered else None)

    if picked is None:
        # ── HTML fallback for geographic_revenue ──────────────────────────────
        # Some companies (e.g. GLW) do not XBRL-tag geographic-revenue segments.
        # Fall through to the same HTML-parsing path used by search_filing_text.
        if fact_type == FilingFactType.geographic_revenue:
            # The Worker's path: the filing (the pinned one, else the latest of the form, a 10-K falling to the latest
            # 20-F), its primary HTML, then the table, or why there is none (2.5.18).
            filing, resolve_error = await _resolve_sec_filing(ticker, filing_type, pinned_accession)
            if filing is None:
                resolve_error = resolve_error or {}
                if _is_accession_not_found(resolve_error):
                    # Not an action 2.13.0 fails closed on a miss: the structured fact path reads as before.
                    resolve_error = await _legacy_accession_miss_payload(ticker, filing_type)
                return _geo_shape({
                    **resolve_error,
                    "factType": fact_type.value,
                    "region": region,
                    "value": None,
                    "denominator": None,
                    "valueRatio": None,
                    "valuePct": None,
                    "extractionMethod": "NONE",
                    "source": resolve_error.get("code"),
                    "confidence": resolve_error.get("code"),
                    "filingType": filing_type,
                    "evidence": {},
                })
            html_text = await _edgar_get_html(filing["documentUrl"], max_bytes=_SEC_DOCUMENT_READ_MAX_CHARS + 1)
            # The read is bounded in UTF-16 code units, as the Worker's string length.
            html_length = _utf16_length(html_text) if html_text else 0
            if html_length > _SEC_DOCUMENT_READ_MAX_CHARS:
                html_text = html_text.encode("utf-16-le", "surrogatepass")[: 2 * _SEC_DOCUMENT_READ_MAX_CHARS].decode("utf-16-le", "ignore")
                html_length = _utf16_length(html_text)
            filing_evidence = {
                "sourceType": "sec_filing",
                "filingType": filing["filingType"],
                "filingDate": filing["filingDate"],
                "accessionNumber": filing["accessionNumber"],
                "documentUrl": filing["documentUrl"],
            }
            if html_text:
                geo_ratio, geo_usd, geo_denominator, geo_heading, geo_evidence = _extract_geo_revenue_from_html(html_text, region or "")
                if geo_usd is not None:
                    geo_evidence = geo_evidence if isinstance(geo_evidence, dict) else {}
                    # A quarterly report's table is a quarter or year-to-date, never a fiscal year: its own column
                    # header says which ("Three Months Ended August 1, 2026"), not the filing date (2.5.9, MRVL). An
                    # annual report's year is its period of report's fiscal year, not its filing date's (2.5.11).
                    annual = bool(_re.match(r"(?:10-K|20-F|40-F)", str(filing["filingType"] or ""), _re.IGNORECASE))
                    # The fiscal year the filing states for itself wins over the period-end rule (DG's year ending
                    # January 2026 is its FY2025) (2.5.13).
                    if annual:
                        fiscal_year = filing_fiscal_year_label(document_fiscal_year_focus(html_text), filing.get("reportDate")) or ""
                    else:
                        headers = geo_evidence.get("sourceColumns")
                        # A column header with no year ("Three Months Ended" alone, the date in a row below) names no
                        # period, so the label is null rather than the header (2.26.5, F60).
                        quarter_column = str(headers[0]) if headers else ""
                        fiscal_year = quarter_column if _re.search(r"\b\d{4}\b", quarter_column) else ""
                    raw_value = geo_evidence.get("rawValue") if geo_evidence.get("rawValue") is not None else _format_raw_number(geo_usd)
                    raw_den = geo_evidence.get("rawDenominator") if geo_evidence.get("rawDenominator") is not None else _format_raw_number(geo_denominator)
                    source_cols = geo_evidence.get("sourceColumns") or [fiscal_year]
                    warnings = []
                    if geo_denominator is None and geo_usd is not None:
                        warnings.append({
                            "code": "DENOMINATOR_NOT_FOUND",
                            "message": "Could not compute geographic revenue percentage due to missing denominator.",
                            "severity": "warning",
                        })
                    scale_assumed = geo_evidence.get("unitScaleSource") == "ASSUMED_MILLIONS"
                    if scale_assumed:
                        warnings.append(dict(_UNIT_SCALE_ASSUMED_WARNING))
                    return _geo_shape({
                        "ticker": ticker,
                        "factType": fact_type.value,
                        "region": region,
                        "period": fiscal_year or None,
                        "rawValue": raw_value,
                        "rawDenominator": raw_den,
                        "unit": "USD",
                        "unitScale": geo_evidence.get("unitScale"),
                        "unitScaleSource": geo_evidence.get("unitScaleSource"),
                        "value": geo_usd,
                        "denominator": geo_denominator,
                        "valueRatio": geo_ratio,
                        "valuePct": _js_round(geo_ratio * PCT_MULTIPLIER * 100) / 100 if geo_denominator is not None else None,
                        "extractionMethod": "PARSED_TABLE",
                        "source": "PARSED_TABLE",
                        # An assumed scale leaves the amounts unproven, not the share (2.5.21).
                        "confidence": "LOW" if geo_denominator is None else "MEDIUM" if scale_assumed else "HIGH",
                        "filingType": filing["filingType"],
                        "filingDate": filing["filingDate"],
                        "accessionNumber": filing["accessionNumber"],
                        "documentUrl": filing["documentUrl"],
                        "indexUrl": None,
                        "primaryDocumentUrl": filing["documentUrl"],
                        "evidence": {
                            "sectionHeading": geo_heading or None,
                            "tableTitle": None,
                            "sourceTableId": 1,
                            "sourceRows": geo_evidence.get("sourceRows"),
                            "sourceColumns": source_cols,
                            # The filing the table came from, so the standardised evidence row names its document (2.14.0).
                            "documentUrl": filing["documentUrl"],
                            "filingType": filing["filingType"],
                            "accessionNumber": filing["accessionNumber"],
                            "filingDate": filing["filingDate"],
                        },
                        "calculation": (
                            {
                                "formula": "value / denominator * 100",
                                "valueSource": "sourceRows[0]",
                                "denominatorSource": "sourceRows[1]",
                                "resultPct": _js_round(geo_ratio * PCT_MULTIPLIER * 100) / 100,
                            }
                            if geo_denominator is not None else None
                        ),
                        "warnings": [*warnings, *filing["warnings"]],
                    })
                region_text = str(region or "")
                searched_terms = _geo_region_aliases(region_text)
                relevant_geo_text = _filing_has_relevant_geo_text(html_text, region_text)
                read_truncated = html_length >= _SEC_DOCUMENT_READ_MAX_CHARS
                scan_coverage = {
                    "sourceType": "sec_primary_html",
                    "documentUrl": filing["documentUrl"],
                    "charsScanned": html_length,
                    "maxCharsRequested": _SEC_DOCUMENT_READ_MAX_CHARS,
                    "filingReadTruncated": read_truncated,
                    "relevantGeoTextFound": relevant_geo_text,
                    "searchedTerms": searched_terms,
                }
                if relevant_geo_text or read_truncated:
                    extraction_warnings = list(filing["warnings"])
                    if relevant_geo_text:
                        extraction_warnings.append({"code": "TABLE_NOT_PARSED", "message": "Relevant filing text exists, but no geographic revenue table was parsed.", "severity": "warning"})
                    if read_truncated:
                        extraction_warnings.append({"code": "FILING_READ_TRUNCATED", "message": "Geographic revenue scan read only the bounded filing prefix; relevant tables may appear later in the filing.", "severity": "warning"})
                    return _geo_shape({
                        "ticker": ticker,
                        "factType": fact_type.value,
                        "region": region,
                        "value": None,
                        "denominator": None,
                        "valueRatio": None,
                        "valuePct": None,
                        "extractionMethod": "NONE",
                        "source": "EXTRACTION_FAILED",
                        "confidence": "EXTRACTION_FAILED",
                        "status": "EXTRACTION_FAILED",
                        "code": "EXTRACTION_FAILED",
                        "filingType": filing["filingType"],
                        "filingDate": filing["filingDate"],
                        "accessionNumber": filing["accessionNumber"],
                        "documentUrl": filing["documentUrl"],
                        "primaryDocumentUrl": filing["documentUrl"],
                        "evidence": filing_evidence,
                        "scanCoverage": scan_coverage,
                        "searchedTerms": searched_terms,
                        "warnings": extraction_warnings,
                    })
                return _geo_shape({
                    "ticker": ticker,
                    "factType": fact_type.value,
                    "region": region,
                    "value": None,
                    "denominator": None,
                    "valueRatio": None,
                    "valuePct": None,
                    "extractionMethod": "NONE",
                    "source": "NOT_DISCLOSED",
                    "confidence": "NOT_DISCLOSED",
                    "status": "NOT_DISCLOSED",
                    "filingType": filing["filingType"],
                    "filingDate": filing["filingDate"],
                    "accessionNumber": filing["accessionNumber"],
                    "documentUrl": filing["documentUrl"],
                    "primaryDocumentUrl": filing["documentUrl"],
                    "evidence": filing_evidence,
                    "scanCoverage": scan_coverage,
                    "searchedTerms": searched_terms,
                    "notDisclosedBasis": "Resolved and scanned the primary SEC filing HTML; no matching geographic revenue disclosure text or parseable table was found for the requested region.",
                    "warnings": list(filing["warnings"]),
                })
            return _geo_shape({
                "ticker": ticker,
                "factType": fact_type.value,
                "region": region,
                "value": None,
                "denominator": None,
                "valueRatio": None,
                "valuePct": None,
                "extractionMethod": "NONE",
                "source": "EXTRACTION_FAILED",
                "confidence": "EXTRACTION_FAILED",
                "status": "EXTRACTION_FAILED",
                "code": "FILING_TEXT_NOT_AVAILABLE",
                "filingType": filing["filingType"],
                "filingDate": filing["filingDate"],
                "accessionNumber": filing["accessionNumber"],
                "documentUrl": filing["documentUrl"],
                "primaryDocumentUrl": filing["documentUrl"],
                "evidence": filing_evidence,
                "warnings": [
                    *filing["warnings"],
                    {"code": "FILING_TEXT_NOT_AVAILABLE", "message": "Resolved SEC filing, but primary HTML text could not be fetched for geographic revenue scan.", "severity": "warning"},
                ],
            })
        return _geo_shape({
            "ticker": ticker,
            "factType": fact_type.value,
            "value": None,
            "denominator": None,
            "valueRatio": None,
            "valuePct": None,
            "extractionMethod": "NONE",
            "source": "NOT_DISCLOSED",
            "confidence": "NOT_DISCLOSED",
            "evidence": {},
            "warnings": [],
            "_manualLookup": _manual_lookup_payload(
                ticker, cik_padded, filing_type, "Fact not XBRL-tagged. Use search_filing_text instead."
            ),
        })

    accession_number = str(picked.get("accn") or "")
    index_url, primary_document_url = await _resolve_filing_urls_for_accession(accession_number)
    document_url = primary_document_url or index_url
    value_num = float(picked.get("val", 0)) if picked.get("val") is not None else None
    raw_value = _format_raw_number(value_num)
    raw_denominator = _format_raw_number(denominator)
    period_label = str(picked.get("fy") or "")
    if period_label and not period_label.startswith("FY"):
        period_label = f"FY{period_label}"
    # A named year is labelled as the period it selected, not by the fy of the filing that carried it (2.5.16).
    if named_year_label:
        period_label = named_year_label

    # ── XBRL context metadata ─────────────────────────────────────────────
    # The Worker's full context (2.5.16): without concept and taxonomy, a found fact failed the decision-grade
    # check (ANET's FY2025 revenue read decisionGrade false in Python, true in the Worker).
    xbrl_context: dict = {
        "concept": concept_used,
        "taxonomy": "us-gaap",
        "unit": "USD",
        "periodStart": picked.get("start"),
        "periodEnd": picked.get("end"),
        "instant": None if picked.get("start") else picked.get("end"),
        "durationDays": None,
        "fiscalPeriod": str(picked.get("fp") or ""),
        "fiscalYear": str(picked.get("fy") or ""),
        "form": str(picked.get("form") or ""),
        "frame": picked.get("frame"),
        "accessionNumber": picked.get("accn"),
        "filedAt": picked.get("filed"),
        "periodMode": resolved_mode,
        "dimensions": {
            **({"segment": segment_label} if segment_label else {}),
            **({"rawSegment": picked.get("segment")} if picked.get("segment") is not None else {}),
        },
    }
    if picked.get("start") and picked.get("end"):
        try:
            d0 = datetime.date.fromisoformat(str(picked["start"]))
            d1 = datetime.date.fromisoformat(str(picked["end"]))
            xbrl_context["durationDays"] = (d1 - d0).days
        except (ValueError, TypeError):
            pass

    result_warnings: list[dict] = []
    # Warn if period_mode filtering was requested but couldn't narrow results
    if resolved_mode == "quarter" and xbrl_context.get("durationDays") and xbrl_context["durationDays"] > 110:
        result_warnings.append({
            "code": "PERIOD_MODE_MISMATCH",
            "message": f"Requested quarter but picked fact has {xbrl_context['durationDays']}-day duration. No quarterly fact available.",
            "severity": "warning",
        })

    return _geo_shape({
        "ticker": ticker,
        "factType": fact_type.value,
        "region": region,
        "period": period_label or None,
        "rawValue": raw_value,
        "rawDenominator": raw_denominator,
        "unit": "USD",
        "unitScale": "actual",
        "value": value_num,
        "denominator": denominator if fact_type == FilingFactType.geographic_revenue else None,
        "valueRatio": value_ratio if fact_type == FilingFactType.geographic_revenue else None,
        "valuePct": value_pct if fact_type == FilingFactType.geographic_revenue else None,
        "extractionMethod": "XBRL",
        "source": "XBRL",
        "confidence": "HIGH" if fact_type != FilingFactType.geographic_revenue or denominator is not None else "LOW",
        "filingType": filing_type,
        "filingDate": str(picked.get("filed") or ""),
        "accessionNumber": accession_number or None,
        "documentUrl": document_url,
        "indexUrl": index_url,
        "primaryDocumentUrl": primary_document_url,
        "xbrlContext": xbrl_context,
        "evidence": {
            "sectionHeading": segment_label,
            "tableTitle": None,
            "sourceTableId": None,
            "sourceRows": [
                [segment_label or (region or "Region"), raw_value if raw_value is not None else ""],
                ["Total revenue", raw_denominator if raw_denominator is not None else ""],
            ],
            "sourceColumns": [period_label or str(picked.get("fp") or "")],
        },
        "calculation": (
            {
                "formula": "value / denominator * 100",
                "valueSource": "sourceRows[0]",
                "denominatorSource": "sourceRows[1]",
                "resultPct": value_pct,
            }
            if fact_type == FilingFactType.geographic_revenue and denominator is not None else None
        ),
        "warnings": result_warnings,
    }, warn_denominator=(fact_type == FilingFactType.geographic_revenue and denominator is None))


# ── search_sec_filing_text ────────────────────────────────────────────────────
# Mirrors the Worker's searchFilingText: each document is projected once to
# display text (yfmcp.filing_search) and searched there.

_FILING_TEXT_TTL = 30 * 60


_FILING_TEXT_MAX_CHARS = 12_000_000


_FILING_TEXT_CACHE: dict[str, tuple[dict, float]] = {}


_FILING_TEXT_CACHE_CHARS = 0


_FILING_TEXT_LOCK = threading.Lock()


_FILING_SEARCH_MAX_FILINGS = 5


_FILING_SEARCH_MAX_EXHIBITS = 4


_FILING_SEARCH_TABLE_ROWS = 40


_SEC_DOCUMENT_READ_MAX_CHARS = 12_000_000


async def _projected_filing_document(url: str) -> dict | None:
    """A filing document as searchable display text, projected once per process."""
    global _FILING_TEXT_CACHE_CHARS
    with _FILING_TEXT_LOCK:
        hit = _FILING_TEXT_CACHE.get(url)
        if hit is not None and time.monotonic() - hit[1] < _FILING_TEXT_TTL:
            return hit[0]
    html = await _edgar_get_html(url, max_bytes=_SEC_DOCUMENT_READ_MAX_CHARS + 1)
    if not html:
        return None
    full_html = html
    truncated = len(html) > _SEC_DOCUMENT_READ_MAX_CHARS or len(html.encode("utf-8", "surrogatepass")) > _SEC_DOCUMENT_READ_MAX_CHARS
    if len(html) > _SEC_DOCUMENT_READ_MAX_CHARS:
        html = html[:_SEC_DOCUMENT_READ_MAX_CHARS]
    projection = _fs.project_filing_text(html, _fs.search_headings(html), _fs.filing_table_spans(html))
    # fiscalYearFocus: the fiscal year the document tags for itself (dei:DocumentFiscalYearFocus), read before truncation.
    entry = {"projection": projection, "truncated": truncated, "fiscalYearFocus": document_fiscal_year_focus(full_html)}
    weight = len(projection.text) * 2
    with _FILING_TEXT_LOCK:
        previous = _FILING_TEXT_CACHE.pop(url, None)
        if previous is not None:
            _FILING_TEXT_CACHE_CHARS -= len(previous[0]["projection"].text) * 2
        if weight <= _FILING_TEXT_MAX_CHARS:
            _FILING_TEXT_CACHE[url] = (entry, time.monotonic())
            _FILING_TEXT_CACHE_CHARS += weight
            while (_FILING_TEXT_CACHE_CHARS > _FILING_TEXT_MAX_CHARS or len(_FILING_TEXT_CACHE) > 24) and _FILING_TEXT_CACHE:
                oldest = next(iter(_FILING_TEXT_CACHE))
                _FILING_TEXT_CACHE_CHARS -= len(_FILING_TEXT_CACHE.pop(oldest)[0]["projection"].text) * 2
    return entry


def _near_specs(values: list | None) -> list:
    out = []
    for value in values or []:
        if not isinstance(value, dict):
            continue
        terms = _fs.terms_from_list(value.get("terms") if isinstance(value.get("terms"), list) else [])
        if len(terms) != 2:
            continue
        try:
            within = int(value.get("within_words", value.get("withinWords", 10)))
        except (TypeError, ValueError):
            within = 10
        out.append(_fs.NearSpec(terms[0], terms[1], max(1, min(200, within))))
    return out


def _fiscal_year_of(report_date: str | None, focus: int | None = None) -> str | None:
    """A filing's fiscal year from the year its primary document tags for itself once read (2.5.14), else from its period of report (2.5.11)."""
    return filing_fiscal_year_label(focus, str(report_date) if report_date else None)


async def _exhibit_targets(cik_int: int, accession: str, filing_date: str | None, filing_type: str, primary_url: str) -> list[dict]:
    index_url, _ = _edgar_build_filing_urls(cik_int, accession, None)
    out: list[dict] = []
    for exhibit in await _edgar_list_exhibits_from_index(index_url):
        doc_type = str(exhibit.get("type") or "").upper()
        name = str(exhibit.get("document") or "")
        url = f"{index_url.rsplit('/', 1)[0]}/{name}" if name else ""
        if not doc_type.startswith("EX-99") or not _re.search(r"\.(?:htm|html|txt)$", url, _re.IGNORECASE) or url == primary_url:
            continue
        description = str(exhibit.get("description") or "").strip()
        out.append({
            "key": url, "url": url, "documentType": doc_type,
            # The filing index often repeats the type as the description.
            "defaultSection": f"{doc_type}: {description}" if description and description.upper() != doc_type else doc_type,
            "primary": False, "accessionNumber": accession, "filingDate": filing_date, "filingType": filing_type,
        })
        if len(out) >= _FILING_SEARCH_MAX_EXHIBITS:
            break
    return out


async def search_filing_text(
    ticker: str,
    search_terms: list[str] | None = None,
    section_hint: str | None = None,
    filing_type: str = "10-K",
    accession_number: str | None = None,
    context_chars: int = 1500,
    return_tables: bool = False,
    document_url: str | None = None,
    *,
    search_query: str | None = None,
    exclude_terms: list[str] | None = None,
    near: list[dict] | None = None,
    match: str | None = None,
    order: str | None = None,
    max_matches: int = 10,
    cursor: str | None = None,
    filing_count: int = 1,
    since: str | None = None,
    include_exhibits: bool = False,
) -> str:
    warnings: list[dict] = []
    mode = "substring" if match == "substring" else "word"
    ordering = "document" if order == "document" else "relevance"
    try:
        budget = max(200, min(int(context_chars) or 1500, 4000))
    except (TypeError, ValueError):
        budget = 1500
    try:
        page_size = max(1, min(50, int(max_matches) or 10))
    except (TypeError, ValueError):
        page_size = 10
    try:
        start_at = max(0, int(str(cursor or "0")))
    except ValueError:
        start_at = 0
    parsed = _fs.parse_search_query(search_query) if search_query else _fs.ParsedQuery()
    spec = _fs.SearchSpec(
        terms=[*_fs.terms_from_list(search_terms or []), *parsed.terms],
        near=[*parsed.near, *_near_specs(near)],
        exclude=[*parsed.exclude, *_fs.terms_from_list(exclude_terms or [])],
        mode=mode,
        budget=budget,
    )
    try:
        count = max(1, min(_FILING_SEARCH_MAX_FILINGS, int(filing_count) or 1))
    except (TypeError, ValueError):
        count = 1
    since_date = since if since and _re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", since) else None

    def empty_result(**fields) -> str:
        return json.dumps({
            "ticker": ticker, "accessionNumber": accession_number, "documentUrl": None, "fiscalYear": None,
            "filingType": filing_type, "filingDate": None, "documentKind": "unavailable",
            "matches": [], "matchCount": 0, **fields,
        })

    # ── Documents to search ──
    filings: list[dict] = []
    explicit_target: dict | None = None
    if document_url:
        explicit_url = _safe_sec_url(document_url)
        if explicit_url is None:
            return _mcp_failure("search_sec_filing_text", ErrorCode.INPUT_VALIDATION_ERROR, "document_url must be an https://www.sec.gov/Archives/ URL.")
        if _is_likely_xbrl_document_url(explicit_url):
            if not accession_number:
                return empty_result(
                    documentUrl=explicit_url, documentKind="xbrl_xml", status="FILING_TEXT_NOT_AVAILABLE",
                    code="FILING_TEXT_NOT_AVAILABLE", confidence="FILING_TEXT_NOT_AVAILABLE",
                    warnings=[{"code": "FILING_TEXT_NOT_AVAILABLE", "message": "Provided document_url appears to be XBRL/XML and no accession_number was supplied to resolve primary HTML.", "severity": "error"}],
                )
            warnings.append({"code": "DOCUMENT_URL_REPLACED_WITH_PRIMARY_HTML", "message": "Provided document_url was XBRL/XML; resolved the accession primary HTML document instead.", "severity": "warning"})
        else:
            explicit_target = {
                "key": explicit_url, "url": explicit_url, "documentType": "primary", "defaultSection": None,
                "primary": True, "accessionNumber": accession_number, "filingDate": None, "filingType": filing_type,
            }
    if explicit_target is None:
        cik_padded, subs = await _get_submissions_for_ticker(ticker)
        if not cik_padded or not subs:
            return empty_result(status="TICKER_NOT_FOUND", code="TICKER_NOT_FOUND", confidence="TICKER_NOT_FOUND",
                                message=f"Could not resolve EDGAR submissions for ticker '{ticker}'")
        recent = subs.get("filings", {}).get("recent", {})
        forms = [str(f) for f in recent.get("form", [])]
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        filing_dates = recent.get("filingDate", [])
        report_dates = recent.get("reportDate", [])
        cik_int = int(cik_padded)
        requested = (filing_type or "10-K").upper()
        wanted = requested
        if not accession_number and requested == "10-K" and "10-K" not in (f.upper() for f in forms) and "20-F" in (f.upper() for f in forms):
            wanted = "20-F"
            warnings.append({"code": "AUTO_20F_FALLBACK", "message": "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).", "severity": "info"})

        def filing_at(i: int) -> dict | None:
            primary = primary_docs[i] if i < len(primary_docs) else None
            if not primary or i >= len(accessions):
                return None
            _, url = _edgar_build_filing_urls(cik_int, accessions[i], primary)
            if not url or _is_likely_xbrl_document_url(url):
                return None
            return {
                "accessionNumber": accessions[i], "filingType": forms[i], "cikInt": cik_int, "documentUrl": url,
                "filingDate": filing_dates[i] if i < len(filing_dates) else None,
                "reportDate": report_dates[i] if i < len(report_dates) else None,
            }

        if accession_number:
            index = next((i for i, acc in enumerate(accessions) if acc == accession_number), None)
            if index is None:
                # An explicit accession missing from recent filings fails closed (2.13.0, R4 Q3a).
                return _accession_not_found_envelope(_accession_not_found_error(ticker, accession_number))
            first = filing_at(index)
            if first and (count > 1 or since_date):
                warnings.append({"code": "FILING_COUNT_IGNORED", "message": "accession_number selects one filing; filing_count and since were ignored.", "severity": "info"})
            filings = [first] if first else []
        else:
            limit = _FILING_SEARCH_MAX_FILINGS if since_date and count <= 1 else count
            capped = False
            for i, form in enumerate(forms):
                if form.upper() != wanted:
                    continue
                if since_date and str(filing_dates[i] if i < len(filing_dates) else "") < since_date:
                    continue
                filing = filing_at(i)
                if filing is None:
                    continue
                if len(filings) >= limit:
                    capped = since_date is not None
                    break
                filings.append(filing)
            if capped:
                warnings.append({"code": "FILINGS_CAPPED", "message": f"Searched the latest {len(filings)} matching filings; older ones were not searched.", "severity": "info"})
        if not filings:
            return empty_result(
                status="FILING_NOT_FOUND_TRY_OTHER_TYPE", code="FILING_NOT_FOUND_TRY_OTHER_TYPE",
                confidence="FILING_NOT_FOUND_TRY_OTHER_TYPE", requestedFilingType=requested,
                availableFilingTypes=list(dict.fromkeys(forms))[:12],
            )

    first = filings[0] if filings else None
    primary_url = explicit_target["url"] if explicit_target else first["documentUrl"]
    base = {
        "ticker": ticker,
        "accessionNumber": first["accessionNumber"] if first else accession_number,
        "documentUrl": primary_url,
        "fiscalYear": _fiscal_year_of(first.get("reportDate")) if first else None,
        "filingType": first["filingType"] if first else filing_type,
        "filingDate": first["filingDate"] if first else None,
    }
    if not spec.terms and not spec.near and not section_hint:
        # Nothing to look for: the resolved filing alone, without reading it.
        return json.dumps({
            **base, "documentKind": "primary_html", "matches": [], "matchCount": 0, "totalMatches": 0,
            "confidence": "NOT_DISCLOSED",
            "warnings": [*warnings, {"code": "NO_SEARCH_TERMS", "message": "Pass search_terms, search_query or near to search the filing text.", "severity": "warning"}],
        })

    groups: list[dict] = (
        [{"filing": None, "targets": [explicit_target]}] if explicit_target else [
            {"filing": f, "targets": [{
                "key": f["documentUrl"], "url": f["documentUrl"], "documentType": "primary", "defaultSection": None,
                "primary": True, "accessionNumber": f["accessionNumber"], "filingDate": f["filingDate"], "filingType": f["filingType"],
            }]}
            for f in filings
        ]
    )
    if include_exhibits:
        for group in groups:
            acc = group["filing"]["accessionNumber"] if group["filing"] else accession_number
            cik_match = _re.search(r"/data/([0-9]+)/", primary_url or "")
            cik_value = group["filing"]["cikInt"] if group["filing"] else (int(cik_match.group(1)) if cik_match else None)
            if not acc or cik_value is None:
                continue
            group["targets"].extend(await _exhibit_targets(
                cik_value, acc, group["filing"]["filingDate"] if group["filing"] else None,
                group["filing"]["filingType"] if group["filing"] else filing_type, group["targets"][0]["url"],
            ))

    # ── Search each document in turn ──
    all_matches: list = []
    term_stats: dict[str, list] = {}
    documents_searched: list[dict] = []
    filing_summaries: list[dict] = []
    doc_order: dict[str, int] = {}
    excluded_hit_count = 0
    section_scope: str | None = None
    hint_missing = False
    truncated_documents = 0
    for group in groups:
        filing_matches: list = []
        filing_term_hits: dict[str, int] = {}
        primary_focus: int | None = None
        for target in group["targets"]:
            doc = await _projected_filing_document(target["url"])
            if doc is None:
                if target["primary"] and group is groups[0]:
                    return empty_result(
                        **base, documentKind="primary_html", status="FILING_TEXT_NOT_AVAILABLE",
                        code="FILING_TEXT_NOT_AVAILABLE", confidence="FILING_TEXT_NOT_AVAILABLE",
                        warnings=[*warnings, {"code": "FILING_TEXT_NOT_AVAILABLE", "message": "Unable to fetch primary filing HTML.", "severity": "error"}],
                    )
                documents_searched.append({"documentUrl": target["url"], "documentType": target["documentType"], "accessionNumber": target["accessionNumber"], "filingDate": target["filingDate"], "status": "FETCH_FAILED"})
                continue
            if doc["truncated"]:
                truncated_documents += 1
            if target["primary"]:
                primary_focus = doc["fiscalYearFocus"]
                # The resolved (first) filing's label follows the year its document tags for itself.
                if group is groups[0] and group["filing"] is first:
                    base["fiscalYear"] = _fiscal_year_of(first.get("reportDate") if first else None, primary_focus)
            projection = doc["projection"]
            scope_start, scope_end = 0, len(projection.text)
            scope = None
            if target["primary"] and section_hint:
                scope = _fs.section_hint_scope(projection, section_hint)
                if scope:
                    scope_start, scope_end = scope[0], scope[1]
                    section_scope = section_scope or scope[2]
                else:
                    hint_missing = True
            doc_order[target["key"]] = len(doc_order)
            search_doc = _fs.SearchDocument(target["key"], target["documentType"], target["defaultSection"], projection, scope_start, scope_end)
            result = _fs.search_document(search_doc, spec)
            if not spec.terms and not spec.near and scope:
                # A section hint alone returns the start of that section.
                result.matches.append(_fs.SearchMatch(
                    search_doc, [section_hint], 1, scope[0],
                    _fs.match_context(projection, scope[0], scope[0] + 1, budget), scope[2],
                ))
            excluded_hit_count += result.excluded_count
            doc_hits = 0
            for label, (hits, capped) in result.term_hits.items():
                prior = term_stats.get(label, [0, False])
                term_stats[label] = [prior[0] + hits, prior[1] or capped]
                filing_term_hits[label] = filing_term_hits.get(label, 0) + hits
                doc_hits += hits
            filing_matches.extend(result.matches)
            entry = {
                "documentUrl": target["url"], "documentType": target["documentType"],
                "accessionNumber": target["accessionNumber"], "filingDate": target["filingDate"],
                "hitCount": doc_hits, "matchCount": len(result.matches), "textChars": len(projection.text),
                "truncated": doc["truncated"],
            }
            if scope:
                entry["sectionScope"] = scope[2]
            documents_searched.append(entry)
        all_matches.extend(filing_matches)
        if group["filing"]:
            f = group["filing"]
            filing_summaries.append({
                "accessionNumber": f["accessionNumber"], "filingDate": f["filingDate"], "filingType": f["filingType"],
                "fiscalYear": _fiscal_year_of(f.get("reportDate"), primary_focus), "documentUrl": f["documentUrl"],
                "totalMatches": len(filing_matches), "termHits": filing_term_hits,
                "hitsBySection": _fs.hits_by_section(filing_matches),
            })
    if hint_missing:
        warnings.append({"code": "SECTION_HINT_NOT_FOUND", "message": f"No heading matched section_hint '{section_hint}'; the whole document was searched.", "severity": "warning"})
    if truncated_documents:
        warnings.append({"code": "FILING_READ_TRUNCATED", "message": f"{truncated_documents} document(s) exceeded {_SEC_DOCUMENT_READ_MAX_CHARS} characters; text past that point was not searched.", "severity": "warning"})

    # ── Order, page and describe ──
    ordered = _fs.order_matches(all_matches, ordering, doc_order)
    page = ordered[start_at:start_at + page_size]
    multi_document = len(documents_searched) > 1
    target_by_key = {t["key"]: t for g in groups for t in g["targets"]}
    payloads: list[dict] = []
    for m in page:
        payload = _fs.match_payload(m)
        target = target_by_key.get(m.doc.key)
        if multi_document and target:
            payload.update(documentUrl=target["url"], documentType=target["documentType"],
                           accessionNumber=target["accessionNumber"], filingDate=target["filingDate"])
        if return_tables:
            table_parsed: list[dict] = []
            table = m.context.table
            if table and target:
                html = await _edgar_get_html(target["url"], max_bytes=_SEC_DOCUMENT_READ_MAX_CHARS + 1)
                if html:
                    rows = [_fs.merge_financial_cells(r) for r in _parse_filing_table_rows(html[table.html_start:table.html_end], _FILING_SEARCH_TABLE_ROWS)]
                    table_parsed.append({"tableIndex": table.table_index, "rows": rows, "rowsTruncated": len(rows) >= _FILING_SEARCH_TABLE_ROWS})
            payload["tableParsed"] = table_parsed
        payloads.append(payload)
    stats = [
        {
            "term": label, "hitCount": value[0], "hitCountCapped": value[1],
            "matchCount": sum(1 for m in all_matches if label in m.terms),
            "returnedCount": sum(1 for m in page if label in m.terms),
        }
        for label, value in term_stats.items()
    ]
    next_cursor = str(start_at + len(page)) if start_at + len(page) < len(ordered) else None
    out = {
        **base,
        "documentKind": "primary_html",
        **({"sectionScope": section_scope} if section_hint else {}),
        "query": {
            "terms": [_fs.term_label(t) for t in spec.terms],
            "near": [f"{_fs.term_label(n.a)} NEAR/{n.within_words} {_fs.term_label(n.b)}" for n in spec.near],
            "exclude": [_fs.term_label(t) for t in spec.exclude],
            "match": mode,
            "order": ordering,
        },
        "matches": payloads,
        "matchCount": len(payloads),
        "totalMatches": len(ordered),
        "excludedHitCount": excluded_hit_count,
        "termStats": stats,
    }
    if len(filing_summaries) > 1:
        out["filings"] = filing_summaries
    else:
        out["hitsBySection"] = _fs.hits_by_section(all_matches)
    out.update({
        "documentsSearched": documents_searched,
        "pagination": {"cursor": str(start_at), "nextCursor": next_cursor, "limit": page_size, "returned": len(payloads), "hasMore": next_cursor is not None},
        "confidence": "NOT_DISCLOSED" if not payloads else "MEDIUM",
        "warnings": [*warnings, *([{"code": "RAW_FILING_TEXT", "message": "Returned text is sanitized filing context, not structured fact extraction.", "severity": "info"}] if payloads else [])],
    })
    return json.dumps(out)


@yfinance_server.tool(name="list_sec_company_filings", description="""List SEC filings for a company from EDGAR submissions.

Returns compact metadata for each filing including accession number, filing date, accepted timestamp, and a direct document URL.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type, e.g. "10-K", "10-Q", "8-K". Defaults to "10-K".
    limit: Maximum number of filings to return (1-20). Defaults to 5.
""")
async def list_sec_company_filings(ticker: str, filing_type: str = "10-K", limit: int = 5, form_type: str | None = None, max_filings: int | None = None) -> str:
    resolved_type = form_type or filing_type
    resolved_limit = min(max(1, max_filings or limit), 20)
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_sec_company_filings", ErrorCode.INPUT_VALIDATION_ERROR, err)

    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("list_sec_company_filings", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not find EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms: list[str] = recent.get("form", [])
    dates: list[str] = recent.get("filingDate", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    accepted_dts: list[str] = recent.get("acceptanceDateTime", [])
    items: list[str] = recent.get("items", [])

    results: list[dict] = []
    for i, form in enumerate(forms):
        if len(results) >= resolved_limit:
            break
        if str(form).upper() != resolved_type.upper():
            continue
        acc = accessions[i] if i < len(accessions) else ""
        date = dates[i] if i < len(dates) else ""
        accepted_at = accepted_dts[i] if i < len(accepted_dts) else None
        primary_doc = primary_docs[i] if i < len(primary_docs) else ""
        _, doc_url = _edgar_build_filing_urls(cik_int, acc, primary_doc)
        results.append({
            "filingType": form,
            "filingDate": date,
            "acceptedAt": accepted_at,
            "accessionNumber": acc,
            "primaryDocument": primary_doc,
            "documentUrl": doc_url,
            "items": str(items[i]) if i < len(items) and items[i] is not None else "",
        })

    retrieved_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    return json.dumps({
        "ticker": ticker,
        "cik": cik_padded,
        "filings": results,
        "meta": {
            "source": "sec_submissions",
            "retrievedAt": retrieved_at,
        },
    })


async def _resolve_latest_sec_doc_url(ticker: str, filing_type: str = "10-K") -> str | None:
    listed_raw = await list_sec_filings(ticker=ticker, form_type=filing_type, max_filings=1)
    try:
        listed = json.loads(listed_raw)
        filings = listed.get("filings") if isinstance(listed, dict) else None
        if isinstance(filings, list) and filings:
            first = filings[0] if isinstance(filings[0], dict) else {}
            return first.get("primaryDocumentUrl")
    except Exception:
        return None
    return None


@yfinance_server.tool(name="get_sec_filing_outline", description="Canonical alias for get_filing_outline.")
async def get_sec_filing_outline(ticker: str, filing_type: str = "10-K", period: str = "latest", accession_number: str | None = None, document_url: str | None = None) -> str:
    # Without a document URL the Worker outlines the filing index (secIndexOutlinePayload), which honours a pinned
    # accession; Python read the latest primary document and dropped the accession (2.26.0, F1).
    if document_url is None:
        return _sec_index_outline_payload(await _sec_filing_index_full(ticker, filing_type, period, accession_number))
    return await get_filing_outline(ticker, accession_number, document_url)


def _sec_index_outline_payload(index_result: str) -> str:
    """The Worker's secIndexOutlinePayload: the filing index's sections in the get_sec_filing_outline shape."""
    payload = json.loads(index_result)
    if not isinstance(payload, dict) or payload.get("ok") is False or payload.get("error"):
        return index_result
    index = payload.get("index") if isinstance(payload.get("index"), dict) else {}
    sections = index.get("sections") if isinstance(index.get("sections"), list) else []
    tables = index.get("tables") if isinstance(index.get("tables"), list) else []
    outline = []
    for section in sections:
        title = section.get("heading") if section.get("heading") is not None else section.get("normalizedHeading")
        title = title if title is not None else ""
        if not str(title).strip():
            continue
        outline.append({"level": section.get("level"), "title": title, "sectionId": section.get("sectionId")})
    return json.dumps({
        "ticker": payload.get("ticker"),
        "filingType": payload.get("filingType"),
        "filingDate": payload.get("filingDate"),
        "accessionNumber": payload.get("accessionNumber"),
        "documentUrl": payload.get("documentUrl"),
        "outline": outline,
        "status": "OK" if outline else ("OUTLINE_NOT_PARSED" if tables else "EMPTY"),
        "tableCount": len(tables),
        "warnings": [{
            "code": "TABLES_FOUND_OUTLINE_EMPTY",
            "message": "Filing tables were detected but no section outline headings were parsed.",
            "severity": "warning",
        }] if not outline and tables else [],
    })


@yfinance_server.tool(name="get_sec_filing_section", description="Get structurally resolved SEC filing section text; Item requests fail closed when structure is unresolved.")
async def get_sec_filing_section(
    ticker: str,
    filing_type: str = "10-K",
    selector: dict | None = None,
    section_name: str | None = None,
    document_url: str | None = None,
    context_chars: int = 3000,
) -> str:
    resolved_doc_url = document_url or await _resolve_latest_sec_doc_url(ticker, filing_type)
    section = section_name or (selector or {}).get("item") or "Item 1A"
    return await get_filing_section(ticker, str(section), str(resolved_doc_url), context_chars)


@yfinance_server.tool(name="list_sec_filing_tables", description="List usable SEC filing tables with original indexes and excluded-table counts.")
async def list_sec_filing_tables(
    ticker: str,
    filing_type: str = "10-K",
    document_url: str | None = None,
    offset: int = 0,
    limit: int = 50,
) -> str:
    if document_url:
        return await list_filing_tables(ticker, document_url, offset, limit)
    # Without a document URL the Worker lists the filing index's tables (secIndexTablesPayload), so each row carries
    # its title, row labels, section and unit scale; Python listed the raw document instead (2.25.3 audit, F46).
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_sec_filing_tables", ErrorCode.INPUT_VALIDATION_ERROR, err)
    return _sec_index_tables_payload(await _get_sec_filing_index_impl(ticker, filing_type, None), offset, limit)


def _sec_index_tables_payload(index_result: str, offset: int, limit: int) -> str:
    """The Worker's secIndexTablesPayload: a page of the filing index's tables in the list_sec_filing_tables shape."""
    payload = json.loads(index_result)
    if not isinstance(payload, dict) or payload.get("ok") is False or payload.get("error"):
        return index_result
    safe_offset = max(0, int(offset))
    safe_limit = min(100, max(1, int(limit or 50)))
    index = payload.get("index") if isinstance(payload.get("index"), dict) else {}
    all_tables = index.get("tables") if isinstance(index.get("tables"), list) else []
    raw_count = index.get("rawTableCount")
    raw_count = raw_count if isinstance(raw_count, (int, float)) and not isinstance(raw_count, bool) else len(all_tables)
    excluded = index.get("excludedTableCount")
    excluded = excluded if isinstance(excluded, (int, float)) and not isinstance(excluded, bool) else max(0, raw_count - len(all_tables))
    tables = [{
        "tableIndex": table.get("tableId") if table.get("tableId") is not None else safe_offset + i,
        "title": table.get("title"),
        "headers": table.get("headers") if isinstance(table.get("headers"), list) else [],
        "rowLabels": table.get("rowLabels") if isinstance(table.get("rowLabels"), list) else [],
        "sectionId": table.get("sectionId"),
        "unitScale": table.get("unitScale") if table.get("unitScale") is not None else "unknown",
        "confidence": table.get("confidence") if table.get("confidence") is not None else "LOW",
    } for i, table in enumerate(all_tables[safe_offset:safe_offset + safe_limit])]
    return json.dumps({
        "ticker": payload.get("ticker"),
        "filingType": payload.get("filingType"),
        "filingDate": payload.get("filingDate"),
        "accessionNumber": payload.get("accessionNumber"),
        "documentUrl": payload.get("documentUrl"),
        "status": "OK" if all_tables else "NO_USABLE_TABLES",
        "tableCount": raw_count,
        "usableTableCount": len(all_tables),
        "excludedTableCount": excluded,
        "returnedCount": len(tables),
        "offset": safe_offset,
        "limit": safe_limit,
        "hasMore": safe_offset + len(tables) < len(all_tables),
        "tables": tables,
        "recommendedNextAction": "GET_SEC_FILING_TABLE" if all_tables else "SEARCH_FILING_TEXT",
    })


@yfinance_server.tool(name="get_sec_filing_table", description="Get a selected SEC table; unusable layout tables return an explicit recovery action.")
async def get_sec_filing_table(ticker: str, table_index: int, filing_type: str = "10-K", document_url: str | None = None, max_rows: int = 30) -> str:
    resolved_doc_url = document_url or await _resolve_latest_sec_doc_url(ticker, filing_type)
    return await get_filing_table(ticker, str(resolved_doc_url), table_index, max_rows)


# ---------------------------------------------------------------------------
# Retrieval path and alternative query helpers for extract_sec_filing_fact
# ---------------------------------------------------------------------------

def _map_extraction_to_retrieval_path(extraction_method: str) -> str:
    """Map extractionMethod to a semantic retrieval path label."""
    method_upper = str(extraction_method or "").upper()
    if method_upper in ("XBRL", "COMPANYFACTS", "XBRL_COMPANYFACTS"):
        return "XBRL"
    elif method_upper in ("HTML_TABLE", "INDEXED_TABLE", "TABLE_PARSE"):
        return "INDEXED_TABLE"
    elif method_upper in ("TEXT_SEARCH", "SECTION_TEXT", "TARGETED_TEXT"):
        return "SECTION_TEXT"
    elif method_upper in ("FULL_DOC_SEARCH", "FULL_TEXT", "FALLBACK"):
        return "FULL_DOC_SEARCH"
    elif method_upper == "NONE":
        return "NONE"
    return "UNKNOWN"


def _suggest_alternative_queries(fact_type: str, region: str | None, payload: dict) -> list[str]:
    """Suggest alternative queries when confidence is low or result is NOT_DISCLOSED."""
    source = str(payload.get("source") or "").upper()
    confidence = str(payload.get("confidence") or "").upper()

    if source not in ("NOT_DISCLOSED", "CONFLICTING") and confidence not in ("NOT_DISCLOSED", "LOW"):
        return []

    suggestions: list[str] = []
    if fact_type == "geographic_revenue" and region:
        suggestions.append(f"search_sec_filing_text with search_terms=['{region}', 'revenue'] and section_hint='geographic'")
        suggestions.append("Try extract_revenue_exposure for broader region matching")
    elif fact_type == "segment_revenue":
        suggestions.append("search_sec_filing_text with search_terms=['segment', 'revenue'] and section_hint='segment'")
    elif fact_type in ("total_revenue", "net_income", "operating_income"):
        suggestions.append(f"search_sec_filing_text with search_terms=['{fact_type.replace('_', ' ')}']")
        suggestions.append("Try get_sec_filing_table with the financial statements table")
    elif fact_type in ("long_term_debt", "cash"):
        suggestions.append("search_sec_filing_text with search_terms=['debt', 'borrowings'] and section_hint='balance sheet'")
    elif fact_type in ("capex", "rd_expense"):
        suggestions.append(f"search_sec_filing_text with search_terms=['{fact_type.replace('_', ' ')}']")

    if not suggestions:
        suggestions.append("Use get_sec_filing_intelligence to check available data sources")
        suggestions.append("Try get_sec_filing_section_markdown for the relevant section")

    return suggestions


def _sec_xbrl_source_evidence(payload: dict) -> dict | None:
    context = payload.get("xbrlContext")
    if not isinstance(context, dict):
        return None
    return {
        "sourceType": "sec_xbrl_companyconcept",
        "concept": context.get("concept"),
        "taxonomy": context.get("taxonomy"),
        "unit": context.get("unit") or payload.get("unit"),
        "accessionNumber": payload.get("accessionNumber") or context.get("accessionNumber"),
        "filingType": payload.get("filingType") or context.get("form"),
        "filingDate": payload.get("filingDate") or context.get("filedAt"),
        "periodStart": context.get("periodStart"),
        "periodEnd": context.get("periodEnd") or context.get("instant"),
        "fiscalYear": context.get("fiscalYear"),
        "fiscalPeriod": context.get("fiscalPeriod"),
        "dimensions": context.get("dimensions") or {},
        "documentUrl": payload.get("documentUrl"),
        "indexUrl": payload.get("indexUrl"),
    }


def _is_decision_grade_sec_xbrl_fact(
    payload: dict,
    source_evidence: dict | None,
    status: object,
) -> bool:
    return bool(
        payload.get("value") is not None
        and str(status).upper() == "FOUND"
        and "XBRL" in str(payload.get("extractionMethod") or "").upper()
        and isinstance(payload.get("xbrlContext"), dict)
        and isinstance(source_evidence, dict)
        and all(source_evidence.get(field) is not None and str(source_evidence.get(field)).strip() != "" for field in (
            "sourceType", "concept", "accessionNumber", "periodEnd",
        ))
    )


# The period parameter of the SEC fact extractors, as the Worker's inputSchema describes it (2.5.16).
_FILING_PERIOD_DESCRIPTION = (
    '"latest" or a fiscal year ("FY2025" or "2025"); a fiscal year selects the annual period the issuer calls that year. '
    "Other values are rejected."
)


_FilingPeriodParam = Annotated[str, Field(description=_FILING_PERIOD_DESCRIPTION)]


def _filing_period_failure(tool: str, period: Any) -> str | None:
    """The Worker's _dispatchTool check for the SEC fact tools: a period that is not "latest" or a fiscal year is refused (2.5.16)."""
    if period is not None and _sf.parse_filing_period(str(period)) is None:
        return _mcp_failure(tool, ErrorCode.INPUT_VALIDATION_ERROR, f"{_sf.FILING_PERIOD_HELP} Got '{period}'.")
    return None


# Revenue names accepted as fact, as the Worker's SEC_XBRL_CONCEPT_ALIASES does.
_REVENUE_FACT_ALIASES = {
    "revenue", "revenues", "totalrevenue", "salesrevenuenet",
    "revenuefromcontractwithcustomerexcludingassessedtax", "revenuefromcontractwithcustomerincludingassessedtax",
}


@yfinance_server.tool(name="extract_sec_filing_fact", description="Canonical SEC fact extractor (routes to get_filing_data or extract_filing_fact).")
async def extract_sec_filing_fact(
    ticker: str,
    fact: str | None = None,
    fact_name: str | None = None,
    fact_type: FilingFactType | None = None,
    region: str | None = None,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    period_mode: str = "auto",
    document_url: str | None = None,
    accession_number: str | None = None,
) -> str:
    failure = _filing_period_failure("extract_sec_filing_fact", period)
    if failure:
        return failure
    routed_fact_type = fact_type
    if routed_fact_type is None and fact is not None and _re.sub(r"[^a-z0-9]", "", _re.sub(r"^(?:us-gaap|dei|srt|country):", "", fact, flags=_re.I).lower()) in _REVENUE_FACT_ALIASES:
        routed_fact_type = FilingFactType.total_revenue
    if routed_fact_type is None and fact is not None:
        try:
            routed_fact_type = FilingFactType(fact)
        except Exception:
            routed_fact_type = FilingFactType.geographic_revenue if region is not None else None
    if routed_fact_type is not None or region is not None or fact_name is None:
        routed_fact_type = routed_fact_type or FilingFactType.geographic_revenue
        raw = await get_filing_data(ticker=ticker, fact_type=routed_fact_type, region=region, filing_type=filing_type, period=period, period_mode=period_mode,
                                    accession_number=accession_number)
        parsed_payload: dict = {}
        try:
            parsed_any = json.loads(raw)
            if isinstance(parsed_any, dict) and "ok" in parsed_any and "data" in parsed_any:
                parsed_any = parsed_any.get("data")
            if isinstance(parsed_any, str):
                parsed_any = json.loads(parsed_any)
            if isinstance(parsed_any, dict):
                parsed_payload = parsed_any
        except Exception:
            parsed_payload = {}
        xbrl_context = parsed_payload.get("xbrlContext")
        source_evidence = _sec_xbrl_source_evidence(parsed_payload)
        status = parsed_payload.get("status") or (
            "FOUND" if parsed_payload.get("value") is not None
            else parsed_payload.get("code") or parsed_payload.get("confidence") or "NOT_DISCLOSED"
        )
        decision_grade = _is_decision_grade_sec_xbrl_fact(
            parsed_payload,
            source_evidence,
            status,
        )
        return json.dumps({
            "fact": routed_fact_type.value,
            "region": region,
            "value": parsed_payload.get("value"),
            "denominator": parsed_payload.get("denominator"),
            "valueRatio": parsed_payload.get("valueRatio"),
            "valuePct": parsed_payload.get("valuePct"),
            "rawValue": parsed_payload.get("rawValue"),
            "rawDenominator": parsed_payload.get("rawDenominator"),
            "unit": "USD",
            "unitScale": parsed_payload.get("unitScale"),
            "period": parsed_payload.get("period"),
            "filingType": parsed_payload.get("filingType", filing_type),
            "filingDate": parsed_payload.get("filingDate"),
            "accessionNumber": parsed_payload.get("accessionNumber"),
            **({"requestedAccession": parsed_payload["requestedAccession"]} if parsed_payload.get("requestedAccession") else {}),
            "extractionMethod": parsed_payload.get("extractionMethod", "NONE"),
            "source": parsed_payload.get("source", "NOT_DISCLOSED"),
            "confidence": parsed_payload.get("confidence", "NOT_DISCLOSED"),
            "status": status,
            "code": parsed_payload.get("code"),
            **({"retryable": parsed_payload["retryable"]} if isinstance(parsed_payload.get("retryable"), bool) else {}),
            **({"failedReads": parsed_payload["failedReads"]} if isinstance(parsed_payload.get("failedReads"), list) else {}),
            "decisionGrade": decision_grade,
            "xbrlContext": xbrl_context,
            "retrieval_path": _map_extraction_to_retrieval_path(parsed_payload.get("extractionMethod", "NONE")),
            "documentUrl": parsed_payload.get("documentUrl"),
            "indexUrl": parsed_payload.get("indexUrl"),
            "primaryDocumentUrl": parsed_payload.get("primaryDocumentUrl"),
            "evidence": source_evidence if decision_grade else parsed_payload.get("evidence"),
            "sourceEvidence": source_evidence,
            "calculation": parsed_payload.get("calculation"),
            "warnings": parsed_payload.get("warnings", []),
            "alternative_queries": _suggest_alternative_queries(routed_fact_type.value, region, parsed_payload),
            "ticker": parsed_payload.get("ticker", ticker),
        })
    return await extract_filing_fact(ticker=ticker, fact_name=fact_name, document_url=document_url, accession_number=accession_number)


@yfinance_server.tool(name="search_sec_filing_text", description="Canonical alias for search_filing_text.")
async def search_sec_filing_text(
    ticker: str,
    search_terms: list[str] | None = None,
    search_query: str | None = None,
    exclude_terms: list[str] | None = None,
    near: list[dict] | None = None,
    match: str = "word",
    order: str = "relevance",
    max_matches: int = 10,
    cursor: str | None = None,
    selector: dict | None = None,
    section_hint: str | None = None,
    filing_type: str = "10-K",
    accession_number: str | None = None,
    document_url: str | None = None,
    filing_count: int = 1,
    since: str | None = None,
    include_exhibits: bool = False,
    context_chars: int = 1500,
    return_tables: bool = False,
) -> str:
    hint = section_hint or (selector or {}).get("item") or None
    return await search_filing_text(
        ticker, search_terms or [], hint, filing_type, accession_number, context_chars, return_tables, document_url,
        search_query=search_query, exclude_terms=exclude_terms, near=near, match=match, order=order,
        max_matches=max_matches, cursor=cursor, filing_count=filing_count, since=since, include_exhibits=include_exhibits,
    )


# ---------------------------------------------------------------------------
# SEC Filing Index helpers
# ---------------------------------------------------------------------------

_INDEX_KEYWORDS = [
    "china", "greater china", "prc", "geographic", "segment", "revenue",
    "customers", "long-lived assets", "risk factors", "americas", "europe",
    "japan", "asia", "rest of asia",
]


def _build_filing_index_from_html(html: str) -> dict:
    """Parse an SEC filing HTML and return a structured index (sections, tables, keywordMap)."""
    # Sanitize: remove scripts/styles/event handlers.
    # Apply iteratively until stable to prevent nested/malformed pattern bypass.
    _script_re = _re.compile(r'<script\b[^>]*>[\s\S]*?</\s*script[^>]*>', _re.IGNORECASE)
    _style_re = _re.compile(r'<style\b[^>]*>[\s\S]*?</\s*style[^>]*>', _re.IGNORECASE)
    sanitized = html
    while True:
        next_s = _script_re.sub('<!--removed-->', sanitized)
        next_s = _style_re.sub('<!--removed-->', next_s)
        if next_s == sanitized:
            break
        sanitized = next_s
    sanitized = _re.sub(r'\s+on\w+=(?:"[^"]*"|\'[^\']*\'|[^\s>]+)', ' ', sanitized, flags=_re.IGNORECASE)

    # Section extraction
    sections: list[dict] = []
    heading_re = _re.compile(r'<h([1-6])[^>]*>(.*?)</h\1>', _re.DOTALL | _re.IGNORECASE)
    for h_match in heading_re.finditer(sanitized):
        if len(sections) >= 50:
            break
        level = int(h_match.group(1))
        raw_text = _strip_html_tags(h_match.group(2))
        if not raw_text or len(raw_text) > 200:
            continue
        normalized = raw_text.lower().strip()
        keywords = [kw for kw in _INDEX_KEYWORDS if kw in normalized]
        section_id = _re.sub(r'[^a-z0-9]+', '_', normalized)[:60]
        sections.append({
            "sectionId": section_id,
            "heading": raw_text,
            "normalizedHeading": normalized,
            "level": level,
            "keywords": keywords,
            "startChar": h_match.start(),
            "endChar": h_match.end(),
        })

    # A filing that marks Part and Item headings in bold text instead of <h1>-<h6> (most 10-Ks and 10-Qs) is indexed
    # by those headings, as in the Worker's _buildFilingIndexFromHtml (2.26.1).
    if not sections:
        for heading in _fs.filing_item_headings(sanitized)[:50]:
            normalized = heading.title.lower()
            sections.append({
                "sectionId": _re.sub(r'[^a-z0-9]+', '_', normalized)[:60],
                "heading": heading.title,
                "normalizedHeading": normalized,
                "level": heading.level,
                "keywords": [kw for kw in _INDEX_KEYWORDS if kw in normalized],
                "startChar": heading.start,
                "endChar": heading.end,
            })

    # Table extraction
    tables: list[dict] = []
    raw_table_count = 0
    table_re = _re.compile(r'<table[^>]*>(.*?)</table>', _re.DOTALL | _re.IGNORECASE)

    for table_idx, t_match in enumerate(table_re.finditer(sanitized)):
        if table_idx >= 100:
            break
        raw_table_count = table_idx + 1
        table_start = t_match.start()
        table_html = t_match.group(0)

        # Nearest section starting before this table
        nearby_section_id: str | None = None
        nearby_heading = ""
        for sec in reversed(sections):
            if sec["startChar"] <= table_start:
                nearby_section_id = sec["sectionId"]
                nearby_heading = sec["heading"]
                break

        parsed_rows = _parse_filing_table_rows(t_match.group(1))
        if not _filing_table_is_usable(parsed_rows):
            continue

        # Headers from first row
        headers = parsed_rows[0][:10]

        # Row labels from first column of subsequent rows
        row_labels: list[str] = []
        for row in parsed_rows[1:20]:
            if row:
                label = row[0]
                if label and len(label) < 100:
                    row_labels.append(label)

        # Unit scale: default to "unknown"; detect explicitly from context.
        unit_scale = _table_unit_scale(table_html, sanitized[max(0, table_start - 2000):table_start])

        # Confidence: also lower when unitScale is unknown
        has_year_headers = any(_re.search(r'\b20\d\d\b', h) for h in headers)
        has_row_labels = bool(row_labels)
        if has_year_headers and has_row_labels and unit_scale != "unknown":
            confidence = "HIGH"
        elif has_year_headers or has_row_labels:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        # The last line of the text before the table, one line per block element; the section heading when none
        # fits (below), as the Worker's _buildFilingIndexFromHtml (2.26.2, F49).
        pre_text = _strip_html_blocks(sanitized[max(0, table_start - 500):table_start])
        lines = [ln for ln in (_clean_filing_display_text(raw.strip()) for raw in pre_text.split('\n')) if ln]
        candidate = lines[-1] if lines else ""
        title = candidate if 10 < len(candidate) < 200 and not _looks_like_filing_markup_text(candidate) else ""

        tables.append({
            "tableId": table_idx,
            "sectionId": nearby_section_id,
            "title": title or nearby_heading,
            "headers": headers,
            "rowLabels": row_labels,
            "unit": "USD",
            "unitScale": unit_scale,
            "confidence": confidence,
        })

    # Keyword map
    keyword_map: dict[str, list[str]] = {}
    for kw in _INDEX_KEYWORDS:
        refs: list[str] = []
        for sec in sections:
            if kw in sec["normalizedHeading"]:
                ref = f"sectionId:{sec['sectionId']}"
                if ref not in refs:
                    refs.append(ref)
        for tbl in tables:
            haystack = " ".join(tbl["rowLabels"] + tbl["headers"] + [tbl["title"]]).lower()
            if kw in haystack:
                ref = f"tableId:{tbl['tableId']}"
                if ref not in refs:
                    refs.append(ref)
        if refs:
            keyword_map[kw] = refs

    return {
        "sections": sections,
        "tables": tables,
        "keywordMap": keyword_map,
        "rawTableCount": raw_table_count,
        "excludedTableCount": raw_table_count - len(tables),
    }


async def _get_sec_filing_index_impl(
    ticker: str,
    filing_type: str = "10-K",
    accession_number: str | None = None,
) -> str:
    """Build or retrieve the cached index for get_sec_filing_index (the Worker's getSecFilingIndex)."""
    try:
        return await _build_sec_filing_index(ticker, filing_type, accession_number)
    except Exception as exc:
        return json.dumps({"ok": False, "error": {"code": "PROVIDER_ERROR", "message": str(exc)}})


async def _build_sec_filing_index(ticker: str, filing_type: str, accession_number: str | None) -> str:
    """The Worker's _getSecFilingIndexImpl: the filing resolver, then the filing's index (2.26.3, F50).

    The filing comes from _resolve_sec_filing, as in the Worker: a 10-K that is not filed falls to the latest 20-F,
    and its failures are the Worker's payloads (FILING_NOT_FOUND_TRY_OTHER_TYPE with the available forms, the
    accession envelope for a pinned accession that is not found).
    """
    filing, error = await _resolve_sec_filing(ticker, filing_type, accession_number)
    if error is not None:
        if error.get("code") == "ACCESSION_NOT_FOUND":
            return _accession_not_found_envelope(error)
        return json.dumps({"ok": False, "error": error, **error})

    accession_number = filing["accessionNumber"]
    cache_key = f"secidx:{ticker.upper()}:{accession_number}:{filing['filingType']}"
    cached = _tool_cache.get(cache_key)
    if cached is not None:
        return cached[0]

    # Through the SEC document cache, so the outline, section and table tools reuse the same read.
    try:
        html = await _edgar_get_document(filing["documentUrl"])
    except _urlerror.HTTPError as http_err:
        return json.dumps({"ok": False, "error": {"code": "PROVIDER_ERROR",
                                                  "message": f"Failed to fetch filing: HTTP {http_err.code}"}})
    # The read is bounded in UTF-16 code units, as the Worker's string length.
    if _utf16_length(html) > _SEC_DOCUMENT_READ_MAX_CHARS:
        html = html.encode("utf-16-le", "surrogatepass")[: 2 * _SEC_DOCUMENT_READ_MAX_CHARS].decode("utf-16-le", "ignore")

    index = _build_filing_index_from_html(html)
    indexed_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    result = json.dumps({
        "ticker": ticker,
        "cik": filing["cikPadded"],
        "requestedFilingType": filing["requestedFilingType"],
        "filingType": filing["filingType"],
        "filingDate": filing["filingDate"],
        "acceptedAt": filing["acceptedAt"],
        "accessionNumber": accession_number,
        "documentUrl": filing["documentUrl"],
        "index": index,
        "meta": {
            "indexedAt": indexed_at,
            "source": "sec",
            "cacheKey": f"{ticker.upper()}:{accession_number}",
            "cacheTtlHours": 24,
        },
        "warnings": filing["warnings"],
    })

    _tool_cache.set(cache_key, result, TTL_EDGAR)
    return result


@yfinance_server.tool(
    name="get_sec_filing_index",
    description="""Build or retrieve the cached section/table index for an SEC filing.
Identifies headings, tables, row labels, and units without a duplicate indexing action.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type, e.g. "10-K" or "10-Q". Defaults to "10-K".
    period: Reserved for future multi-period support. Currently only "latest" is supported.
        When accession_number is provided, the specific filing is returned regardless of period.
    accession_number: Optional SEC accession number (format XXXXXXXXXX-YY-ZZZZZZ).
        If omitted, the most recent filing matching filing_type is used.
""",
)
async def get_sec_filing_index(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("get_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)
    return _compact_filing_index_payload(await _get_sec_filing_index_impl(ticker, filing_type, accession_number))


# The public filing index lists each table's first 8 row labels and its label count; the full labels stay in the
# cached index the extractors read. A 10-K index otherwise runs to tens of thousands of characters. The Worker's
# compactFilingIndexPayload (2.26.3, F50).
_INDEX_ROW_LABELS_SHOWN = 8


def _compact_filing_index_payload(raw: str) -> str:
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return raw
    index = parsed.get("index") if isinstance(parsed, dict) else None
    if not isinstance(index, dict) or not isinstance(index.get("tables"), list):
        return raw
    compacted = []
    for table in index["tables"]:
        labels = table.get("rowLabels") if isinstance(table, dict) and isinstance(table.get("rowLabels"), list) else []
        if len(labels) <= _INDEX_ROW_LABELS_SHOWN:
            compacted.append(table)
        else:
            compacted.append({**table, "rowLabels": labels[:_INDEX_ROW_LABELS_SHOWN], "rowLabelCount": len(labels)})
    index["tables"] = compacted
    return json.dumps(parsed)


async def _sec_filing_index_full(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    accession_number: str | None = None,
) -> str:
    """get_sec_filing_index with every table's row labels, for the tools built on the index (as the Worker's
    internal getSecFilingIndex callers)."""
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("get_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)
    return await _get_sec_filing_index_impl(ticker, filing_type, accession_number)


# ---------------------------------------------------------------------------
# SEC Material Filing Forms (non-noisy filings for intelligence layer)
# ---------------------------------------------------------------------------
_SEC_MATERIAL_FORMS_DEFAULT: list[str] = [
    "10-K", "10-Q", "8-K", "S-1", "424B", "DEF 14A", "20-F", "6-K",
]


_SEC_NOISY_FORMS: set[str] = {
    "4", "3", "5", "SC 13G", "SC 13G/A", "SC 13D", "SC 13D/A",
    "144", "SD", "CORRESP", "UPLOAD", "CT ORDER",
}


_SEC_MATERIAL_FILINGS_MAX = 20


@yfinance_server.tool(
    name="list_sec_material_filings",
    description="""List latest material SEC filings for a ticker, filtering out noise (Form 4, 144, SC 13G, etc.).
Returns only significant filings (10-K, 10-Q, 8-K, S-1, 424B, DEF 14A, 20-F, 6-K by default).

Args:
    ticker: Ticker symbol.
    forms: List of form types to include (default: ["10-K", "10-Q", "8-K", "S-1", "424B", "DEF 14A", "20-F", "6-K"]).
    limit: Maximum number of filings to return (default: 5, max: 20).
""",
)
async def list_sec_material_filings(
    ticker: str,
    forms: list[str] | None = None,
    limit: int = 5,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("list_sec_material_filings", ErrorCode.INPUT_VALIDATION_ERROR, err)

    resolved_limit = min(max(1, limit), _SEC_MATERIAL_FILINGS_MAX)
    allowed_forms: set[str] = set(f.upper() for f in (forms or _SEC_MATERIAL_FORMS_DEFAULT))

    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("list_sec_material_filings", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not find EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms_list: list[str] = recent.get("form", [])
    dates: list[str] = recent.get("filingDate", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    accepted_dts: list[str] = recent.get("acceptanceDateTime", [])
    is_xbrl: list = recent.get("isXBRL", [])
    is_inline_xbrl: list = recent.get("isInlineXBRL", [])

    def _flag_at(values: list, index: int) -> bool:
        try:
            return int(values[index]) == 1
        except (IndexError, TypeError, ValueError):
            return False

    def _material(form: Any) -> bool:
        # Must match an allowed form or prefix (e.g. "424B" matches "424B4") and not be in the noisy set.
        form_upper = str(form).upper()
        return form_upper not in _SEC_NOISY_FORMS and any(form_upper == af or form_upper.startswith(af) for af in allowed_forms)

    results: list[dict] = []
    last_index = -1
    for i, form in enumerate(forms_list):
        if len(results) >= resolved_limit:
            break
        if not _material(form):
            continue
        last_index = i
        acc = accessions[i] if i < len(accessions) else ""
        date = dates[i] if i < len(dates) else ""
        accepted_at = accepted_dts[i] if i < len(accepted_dts) else None
        primary_doc = primary_docs[i] if i < len(primary_docs) else ""
        _, doc_url = _edgar_build_filing_urls(cik_int, acc, primary_doc)

        # SEC submissions flag each filing that carries XBRL or inline XBRL.
        xbrl_available = _flag_at(is_xbrl, i) or _flag_at(is_inline_xbrl, i)

        results.append({
            "filingType": form,
            "filingDate": date,
            "acceptedAt": accepted_at,
            "accessionNumber": acc,
            "primaryDocument": primary_doc,
            "documentUrl": doc_url,
            "xbrl_available": xbrl_available,
        })

    # The cap and whether it hid filings are stated (2.5.31, F-028: limit 120 returned 20 rows and said nothing).
    more_available = len(results) == resolved_limit and any(_material(f) for f in forms_list[last_index + 1:])
    warnings: list[dict] = []
    if limit > _SEC_MATERIAL_FILINGS_MAX:
        more = ", and more matching filings exist in SEC's recent submissions" if more_available else ""
        warnings.append({"code": "LIMIT_CAPPED", "message": (
            f"limit {limit} is above the maximum of {_SEC_MATERIAL_FILINGS_MAX}; at most {_SEC_MATERIAL_FILINGS_MAX} filings are returned{more}."), "severity": "warning"})
    retrieved_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    return json.dumps({
        "ticker": ticker,
        "cik": cik_padded,
        "filings": results,
        "limit": {"requested": limit, "applied": resolved_limit, "maximum": _SEC_MATERIAL_FILINGS_MAX},
        "moreAvailable": more_available,
        "meta": {
            "source": "sec_submissions",
            "materialFormsFilter": sorted(allowed_forms),
            "retrievedAt": retrieved_at,
        },
        **({"warnings": warnings} if warnings else {}),
    })


# ---------------------------------------------------------------------------
# SEC Filing Intelligence - composite tool
# ---------------------------------------------------------------------------

# XBRL concept names for the intelligence snapshot
_XBRL_INTELLIGENCE_CONCEPTS: dict[str, list[str]] = {
    # The shared revenue concepts (yfmcp/sec_facts.py); each filing is read from the one it tags.
    "revenue": list(_sf.REVENUE_CONCEPTS),
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "cash": ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsAndShortTermInvestments"],
    "long_term_debt": ["LongTermDebt", "LongTermDebtNoncurrent"],
    "total_assets": ["Assets"],
    "operating_income": ["OperatingIncomeLoss"],
}


@yfinance_server.tool(
    name="get_sec_filing_intelligence",
    description="""Get a comprehensive intelligence map of a company's SEC filing — XBRL facts snapshot, section/table index summary, and recommended queries — in a single call.
Gives the LLM a filing "map" so it knows what data is available and can make targeted follow-up calls.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type (default: "10-K").
    filing_index: 0 = latest, 1 = previous (default: 0).
""",
)
async def get_sec_filing_intelligence(
    ticker: str,
    filing_type: str = "10-K",
    filing_index: int = 0,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_sec_filing_intelligence", ErrorCode.INPUT_VALIDATION_ERROR, err)

    filing_index = max(0, min(filing_index, 9))

    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("get_sec_filing_intelligence", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not find EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms_list: list[str] = recent.get("form", [])
    dates: list[str] = recent.get("filingDate", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    accepted_dts: list[str] = recent.get("acceptanceDateTime", [])

    # Find the Nth matching filing
    match_count = 0
    target_idx: int | None = None
    for i, form in enumerate(forms_list):
        if str(form).upper() == filing_type.upper():
            if match_count == filing_index:
                target_idx = i
                break
            match_count += 1

    if target_idx is None:
        return _mcp_failure("get_sec_filing_intelligence", ErrorCode.NO_FILING_DATA,
                            f"No {filing_type} filing at index {filing_index} for '{ticker}'")

    accession_number = accessions[target_idx] if target_idx < len(accessions) else ""
    filing_date = dates[target_idx] if target_idx < len(dates) else ""
    accepted_at = accepted_dts[target_idx] if target_idx < len(accepted_dts) else None
    primary_doc = primary_docs[target_idx] if target_idx < len(primary_docs) else ""
    _, document_url = _edgar_build_filing_urls(cik_int, accession_number, primary_doc)

    # --- XBRL facts snapshot ---
    xbrl_available = False
    xbrl_facts: dict[str, dict | None] = {}
    xbrl_status = "UNAVAILABLE"
    try:
        facts_data = await _edgar_get_company_facts(cik_padded)
        if facts_data and facts_data.get("facts"):
            xbrl_available = True
            xbrl_status = "OK"
            for fact_key, concepts in _XBRL_INTELLIGENCE_CONCEPTS.items():
                xbrl_facts[fact_key] = _extract_xbrl_latest_annual(
                    facts_data,
                    concepts,
                    accession_number=accession_number,
                    document_url=document_url,
                )
            if not any(xbrl_facts.values()):
                xbrl_status = "AVAILABLE_NO_MATCHING_FACTS"
    except Exception:
        xbrl_status = "ERROR"

    # --- Filing index (sections/tables) ---
    index_status = "UNAVAILABLE"
    sections_list: list[str] = []
    sections_count = 0
    tables_count = 0
    # Exhibits are counted from the filing index under the issuer's CIK; the
    # count had been a constant 0 (2.4.5).
    exhibits_count = None
    try:
        exhibits_index_url, _ = _edgar_build_filing_urls(cik_int, accession_number, None)
        exhibits_count = len(await _edgar_list_exhibits_from_index(exhibits_index_url))
    except Exception:
        exhibits_count = None
    try:
        index_raw = await _get_sec_filing_index_impl(ticker, filing_type, accession_number)
        index_data = json.loads(index_raw)
        if isinstance(index_data, dict) and "index" in index_data:
            idx = index_data["index"]
            sections_count = len(idx.get("sections", []))
            tables_count = len(idx.get("tables", []))
            sections_list = [s.get("heading", "") for s in idx.get("sections", [])[:20]]
            index_status = "OK"
    except Exception:
        index_status = "ERROR"

    # Recommended queries based on filing type
    recommended_queries = [
        "revenue by segment",
        "risk factors",
        "liquidity and capital resources",
        "customer concentration",
        "long-term debt",
    ]
    if filing_type.upper() in ("10-K", "20-F"):
        recommended_queries.extend(["geographic revenue", "R&D expense", "guidance"])
    elif filing_type.upper() == "10-Q":
        recommended_queries.extend(["quarter-over-quarter revenue", "material events"])
    elif filing_type.upper() == "8-K":
        recommended_queries = ["material event", "exhibit content", "financial results"]

    return json.dumps({
        "ticker": ticker,
        "filing": {
            "type": filing_type,
            "accessionNumber": accession_number,
            "filedAt": filing_date,
            "acceptedAt": accepted_at,
            "documentUrl": document_url,
        },
        "xbrl_available": xbrl_available,
        "xbrl_facts": {k: v for k, v in xbrl_facts.items() if v is not None},
        "index": {
            "sections_count": sections_count,
            "tables_count": tables_count,
            "sections": sections_list,
            "exhibits_count": exhibits_count,
        },
        "recommended_queries": recommended_queries,
        "status": {
            "xbrl": xbrl_status,
            "index": index_status,
            "sections": "AVAILABLE" if sections_count > 0 else "EMPTY",
        },
    })


def _is_toc_match(html: str, match_start: int, match_end: int) -> bool:
    if _re.search(r'<a\b[^>]*\bhref\s*=\s*[\'"]#[^\'"]*[\'"]', html[max(0, match_start - 30):min(len(html), match_end + 30)], _re.IGNORECASE):
        return True
    surr_before = html[max(0, match_start - 100):match_start]
    if _re.search(r'<a\b[^>]*\bhref\s*=\s*[\'"]#[^\'"]*[\'"][^>]*>\s*$', surr_before, _re.IGNORECASE):
        return True
    context = html[max(0, match_start - 150):min(len(html), match_end + 150)]
    if "...." in context or ". . ." in context or "&#183;" in context or "&middot;" in context:
        return True
    plain_context = _re.sub(r'<[^>]+>', ' ', context)
    if _re.search(r'\.{3,}\s*\d+|\.\s*\.\s*\.\s*\d+', plain_context):
        return True
    return False


def _section_item_token(value: str) -> str | None:
    match = _re.search(r"\bitem\s+(\d+[a-z]?)\b", value, _re.IGNORECASE)
    return f"item {match.group(1).lower()}" if match else None


def _section_heading_matches(requested: str, heading: str) -> bool:
    requested_text = " ".join(requested.lower().split())
    heading_text = " ".join(heading.lower().split())
    requested_item = _section_item_token(requested_text)
    heading_item = _section_item_token(heading_text)
    if requested_item:
        return requested_item == heading_item
    return requested_text in heading_text or heading_text in requested_text


def _bold_item_headings(html: str) -> list[tuple[int, int, str, str]]:
    """Structurally bold SEC Item headings with offsets and normalized tokens (the Worker's boldItemHeadings).

    The bold pattern matches the style value per quote character, because inline-XBRL filings quote font names
    inside it (style="font-family:'Helvetica';font-weight:700"); the old pattern stopped at the inner quote, so
    no Apple 10-K Item heading matched (2.25.2 audit, F32)."""
    matches: list[tuple[int, int, str, str]] = []
    for match in _fs._BOLD_ITEM_RE.finditer(html):
        text = _fs._heading_block_text(html, match.start()) or " ".join(
            _fs.strip_html_tags(match.group(1)).replace("\xa0", " ").split())
        token = _section_item_token(text)
        if token:
            matches.append((match.start(), match.end(), text, token))
    return matches


def _find_section_bounds(html: str, section: str, max_chars: int = 50000) -> tuple[int | None, int | None, str, bool, str | None]:
    heading_re = _re.compile(r'<h([1-6])[^>]*>(.*?)</h\1>', _re.DOTALL | _re.IGNORECASE)
    candidates = []
    toc_skipped = False

    for occurrence in _re.finditer(_re.escape(section), html, _re.IGNORECASE):
        if _is_toc_match(html, occurrence.start(), occurrence.end()):
            toc_skipped = True

    all_headings = list(heading_re.finditer(html))
    for idx, h_match in enumerate(all_headings):
        heading_text = _html_module.unescape(_strip_html_tags(h_match.group(2))).replace("\xa0", " ")
        heading_text = " ".join(heading_text.split())
        level = int(h_match.group(1))

        if _section_heading_matches(section, heading_text):
            if _is_toc_match(html, h_match.start(), h_match.end()):
                toc_skipped = True
                continue
            candidates.append((h_match.start(), level, heading_text, idx))

    item_matches: list[tuple[int, int, str, str]] = []
    all_bold_items = _bold_item_headings(html)
    if not candidates:
        for item_match in all_bold_items:
            start, end, heading_text, _token = item_match
            if not _section_heading_matches(section, heading_text):
                continue
            if _is_toc_match(html, start, end):
                toc_skipped = True
                continue
            item_matches.append(item_match)

    if not candidates and not item_matches:
        return None, None, "", toc_skipped, None

    if len(candidates) > 1 or len(item_matches) > 1:
        return None, None, "", toc_skipped, "SECTION_AMBIGUOUS"

    if candidates:
        start_pos, level, found_heading, h_idx = candidates[0]
        end_pos = None
        for next_h in all_headings[h_idx + 1:]:
            next_level = int(next_h.group(1))
            if next_level <= level:
                end_pos = next_h.start()
                break
        if end_pos is None:
            end_pos = min(start_pos + max_chars * 3, len(html))
        return start_pos, end_pos, found_heading, toc_skipped, None
    else:
        start_pos, end_pos_match, found_heading, matched_token = item_matches[0]
        end_pos = next((
            item_start
            for item_start, item_end, _heading, item_token in all_bold_items
            if item_start >= end_pos_match
            and item_token != matched_token
            and not _is_toc_match(html, item_start, item_end)
        ), None)
        if end_pos is None:
            end_pos = min(start_pos + max_chars * 3, len(html))
        return start_pos, end_pos, found_heading, toc_skipped, None


@yfinance_server.tool(
    name="get_sec_filing_section_markdown",
    description="""Return a specific SEC filing section as LLM-ready Markdown.
Converts filing HTML to clean Markdown with preserved section headers and pipe-delimited tables.
Use after get_sec_filing_intelligence to drill into a specific section.

Args:
    ticker: Ticker symbol.
    filing_type: SEC form type (default: "10-K").
    section: Section name to extract (e.g. "Risk Factors", "Item 7", "MD&A", "Item 1A").
    filing_index: 0 = latest, 1 = previous (default: 0).
    max_chars: Maximum characters to return (default: 50000).
""",
)
async def get_sec_filing_section_markdown(
    ticker: str,
    section: str = "Item 1A",
    filing_type: str = "10-K",
    filing_index: int = 0,
    max_chars: int = 50000,
) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.INPUT_VALIDATION_ERROR, err)

    filing_index = max(0, min(filing_index, 9))
    max_chars = min(max(1000, max_chars), 100000)

    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.TICKER_NOT_FOUND,
                            f"Could not find EDGAR submissions for ticker '{ticker}'")

    cik_int = int(cik_padded)
    recent = subs.get("filings", {}).get("recent", {})
    forms_list: list[str] = recent.get("form", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])

    # Find the Nth matching filing
    match_count = 0
    target_idx: int | None = None
    for i, form in enumerate(forms_list):
        if str(form).upper() == filing_type.upper():
            if match_count == filing_index:
                target_idx = i
                break
            match_count += 1

    if target_idx is None:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.NO_FILING_DATA,
                            f"No {filing_type} filing at index {filing_index} for '{ticker}'")

    accession_number = accessions[target_idx] if target_idx < len(accessions) else ""
    primary_doc = primary_docs[target_idx] if target_idx < len(primary_docs) else ""
    _, document_url = _edgar_build_filing_urls(cik_int, accession_number, primary_doc)

    if not document_url:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.NO_FILING_DATA,
                            f"No document URL for {accession_number}")

    # Fetch the filing HTML
    html = await _edgar_get_html(document_url, max_bytes=5_000_000)
    if not html:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.PROVIDER_ERROR,
                            f"Failed to fetch filing document: {document_url}")

# Find the section boundaries using heading patterns
    start_idx, end_idx, found_heading, toc_skipped, err_code = _find_section_bounds(html, section, max_chars)
    if err_code == "SECTION_AMBIGUOUS":
        return _mcp_failure("get_sec_filing_section_markdown", "SECTION_AMBIGUOUS", "The section heading could not be resolved unambiguously.")

    if start_idx is None:
        return _mcp_failure("get_sec_filing_section_markdown", ErrorCode.NO_FILING_DATA,
                            f"Section '{section}' not found in filing")

    parser_source = "html_parser_fallback"
    markdown = _html_to_markdown_fallback(html, start_idx, end_idx)

    # Count tables in the section
    section_slice = html[start_idx:end_idx]
    tables_in_section = len(_re.findall(r'<table[^>]*>', section_slice, _re.IGNORECASE))

    # Truncate if needed
    truncated = False
    if len(markdown) > max_chars:
        markdown = markdown[:max_chars].rstrip()
        truncated = True

    word_count = len(markdown.split())
    warnings: list[dict] = [{
        "code": "LIVE_SECTION_EXTRACTION_UNRELIABLE",
        "message": "Section markdown is produced by a lossy Worker HTML fallback and is blocked from decision-grade use.",
        "severity": "warning",
    }]
    if word_count < 50:
        warnings.append({
            "code": "SECTION_MARKDOWN_LOW_CONTENT",
            "message": "Extracted section markdown is unusually short; verify with the source filing.",
            "severity": "warning",
        })

    return json.dumps({
        "ticker": ticker,
        "section": found_heading or section,
        "filingType": filing_type,
        "accessionNumber": accession_number,
        "markdown": markdown,
        "tables_in_section": tables_in_section,
        "word_count": word_count,
        "status": "SECTION_MARKDOWN_UNVERIFIED",
        "confidence": "NOT_DECISION_GRADE",
        "decisionGrade": False,
        "reliabilityClass": _TOOL_RELIABILITY_CLASS["get_sec_filing_section_markdown"],
        "warnings": warnings,
        "source": parser_source,
        "truncated": truncated,
        "sectionStartOffset": start_idx,
        "sectionEndOffset": end_idx,
        "matchedHeading": found_heading,
        "tocSkipped": toc_skipped,
    })


def _filing_not_found(ticker: str, requested: str, available: list[str]) -> dict:
    return {
        "status": "FILING_NOT_FOUND_TRY_OTHER_TYPE",
        "code": "FILING_NOT_FOUND_TRY_OTHER_TYPE",
        "ticker": ticker,
        "requestedFilingType": requested,
        "availableFilingTypes": available,
        "suggestedFilingTypes": ["20-F"] if requested == "10-K" and "20-F" in available else [],
        "accessionNumber": None,
        "filingDate": None,
        "documentUrl": None,
        "warnings": [{"code": "FILING_NOT_FOUND_TRY_OTHER_TYPE", "message": f"No {requested} filing found for '{ticker}'.", "severity": "error"}],
    }


async def _resolve_periodic_filings(ticker: str, filing_type: str, accession_number: str | None) -> tuple[list[tuple[str, dict]], dict | None]:
    """The newest periodic report, plus the latest annual report when the newest is a 10-Q."""
    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not subs:
        return [], {"status": "TICKER_NOT_FOUND", "code": "TICKER_NOT_FOUND", "ticker": ticker, "message": f"Could not resolve EDGAR submissions for ticker '{ticker}'"}
    recent = subs.get("filings", {}).get("recent", {})
    forms = [str(f) for f in recent.get("form", [])]
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])
    filing_dates = recent.get("filingDate", [])
    cik_int = int(cik_padded)
    available = list(dict.fromkeys(f.upper() for f in forms if f))[:12]

    def row(i: int) -> dict | None:
        primary = primary_docs[i] if i < len(primary_docs) else None
        if not primary or i >= len(accessions) or not accessions[i]:
            return None
        _, url = _edgar_build_filing_urls(cik_int, accessions[i], primary)
        if not url or _is_likely_xbrl_document_url(url):
            return None
        return {"filingType": forms[i], "filingDate": filing_dates[i] if i < len(filing_dates) else None, "accessionNumber": accessions[i], "documentUrl": url}

    def first(form: str) -> dict | None:
        index = next((i for i, f in enumerate(forms) if f.upper() == form), None)
        return row(index) if index is not None else None

    def annual() -> dict | None:
        return first("10-K") or first("20-F")

    requested = (filing_type or "latest").upper()
    if accession_number:
        index = next((i for i, acc in enumerate(accessions) if acc == accession_number), None)
        if index is None:
            return [], _accession_not_found_error(ticker, accession_number)
        one = row(index)
        return ([("primary", one)], None) if one else ([], _filing_not_found(ticker, "10-K" if requested == "LATEST" else requested, available))
    if requested not in ("LATEST", "10-Q"):
        one = annual() if requested == "10-K" else first(requested)
        return ([("primary", one)], None) if one else ([], _filing_not_found(ticker, requested, available))
    yearly = annual()
    quarterly = first("10-Q")
    if requested == "10-Q" and not quarterly:
        return [], _filing_not_found(ticker, "10-Q", available)
    if not yearly and not quarterly:
        return [], _filing_not_found(ticker, "10-K", available)
    if quarterly and (not yearly or str(quarterly["filingDate"] or "") > str(yearly["filingDate"] or "") or requested == "10-Q"):
        filings = [("primary", quarterly)]
        if yearly:
            filings.append(("latest_annual_fallback", yearly))
        return filings, None
    return [("primary", yearly)], None
