"""SEC extractor and exposure MCP tools.

Phase 5 unit 2: extracted from server.py as a pure move (no behaviour change).
Covers the SEC extractor tools (geographic, segment and total revenue, revenue exposure, risk-factor
mentions, customer concentration, dilution bridge, capital structure, share-count scenarios, funding
and capex schedule, operating-driver ledger, China exposure, multi-dimensional exposure) and
``query_sec_filing_index``, which routes to them, together with their helpers and module constants.

Shared dependencies are imported from ``yfmcp.*``; this module never imports ``server`` (no import
cycle) and ``yfmcp.tools.sec`` never imports this module. ``server.py`` re-exports the names that
remaining server code, grouped routing, scripts and tests still look up as ``server.<name>``.
"""

import asyncio
import json
import math
import re as _re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from yfmcp import capital_structure as _cs
from yfmcp import driver_ledger as _dl
from yfmcp import extraction_rules as _er
from yfmcp import funding_schedule as _fsched
from yfmcp import sec_facts as _sf
from yfmcp import share_scenarios as _ss
from yfmcp.app import yfinance_server
from yfmcp.clients.edgar import (
    _edgar_get_company_facts,
    _edgar_get_html,
    _get_submissions_for_ticker,
    _warm_sec_submissions,
)
from yfmcp.clients.endpoints import sec_archive_document_url
from yfmcp.envelope import ErrorCode, _mcp_failure
from yfmcp.exposure_presets import CHINA as _CHINA_PRESET
from yfmcp.parsing.extractors import (
    PRODUCT_BREAKDOWN_LEAD_RE as _PRODUCT_BREAKDOWN_LEAD_RE,
    _text_contains_geo_region,
    extract_geo_revenue_from_html as _extract_geo_table,
    extract_segment_table_from_html,
)
from yfmcp.parsing.html import _UNIT_SCALE_ASSUMED_WARNING, _strip_html_blocks
from yfmcp.schemas import FilingFactType
from yfmcp.tools.sec import (
    _FilingPeriodParam,
    _SEC_DOCUMENT_READ_MAX_CHARS,
    _accession_not_found_envelope,
    _filing_period_failure,
    _is_accession_not_found,
    _is_decision_grade_sec_xbrl_fact,
    _legacy_accession_miss_payload,
    _resolve_periodic_filings,
    _resolve_sec_filing,
    _sec_filing_index_full,
    _sec_xbrl_source_evidence,
    get_filing_data,
    search_filing_text,
    search_sec_filing_text,
)
from yfmcp.util import _compact_excerpt, _nn, _safe_json_loads, _unwrap_payload, _utf16_length
from yfmcp.validation import _sanitize_sec_html, _validate_accession, _validate_ticker


def _as_status(source_payload: dict) -> str:
    """The Worker's normalizeStatus: status/code first, then source/confidence, then NOT_DISCLOSED, CONFLICTING, NOT_FOUND."""
    def _text(value: object) -> str:
        return "" if value is None else str(value).upper()

    status = _text(source_payload.get("status") if source_payload.get("status") is not None else source_payload.get("code"))
    for named in ("FILING_NOT_FOUND_TRY_OTHER_TYPE", "FILING_TEXT_NOT_AVAILABLE", "EXTRACTION_FAILED", "TABLE_NOT_PARSED",
                  "PROVIDER_LIMITATION", "NO_DIMENSIONAL_REVENUE_FACT"):
        if status == named:
            return named
    # A failed SEC read is not a missing fact (2.5.17).
    if status == "PROVIDER_ERROR":
        return "PROVIDER_ERROR"
    source = _text(source_payload.get("source"))
    confidence = _text(source_payload.get("confidence"))
    for named in ("FILING_NOT_FOUND_TRY_OTHER_TYPE", "EXTRACTION_FAILED", "TABLE_NOT_PARSED", "PROVIDER_LIMITATION",
                  "NO_DIMENSIONAL_REVENUE_FACT", "NOT_DISCLOSED", "CONFLICTING"):
        if source == named or confidence == named:
            return named
    return "NOT_FOUND"


# The revenue statuses that say why there is no value (the Worker's NON_DECISION_REVENUE_STATUSES).
_NON_DECISION_REVENUE_STATUSES = frozenset({
    "EXTRACTION_FAILED", "TABLE_NOT_PARSED", "PROVIDER_LIMITATION", "NO_DIMENSIONAL_REVENUE_FACT",
    "FILING_NOT_FOUND_TRY_OTHER_TYPE", "FILING_TEXT_NOT_AVAILABLE",
})


def _explicit_revenue_limitation_status(status: object, warnings: list | None = None) -> str | None:
    """The Worker's explicitRevenueLimitationStatus: a named limitation, from the status or from a warning code."""
    normalized = "" if status is None else str(status).upper()
    if normalized in _NON_DECISION_REVENUE_STATUSES:
        return normalized
    codes = {w.get("code") for w in (warnings or []) if isinstance(w, dict)}
    if "TABLE_NOT_PARSED" in codes:
        return "TABLE_NOT_PARSED"
    if "NO_DIMENSIONAL_REVENUE_FACT" in codes:
        return "NO_DIMENSIONAL_REVENUE_FACT"
    return None


async def _may_be_20f_filer(ticker: str) -> bool:
    _, submissions = await _get_submissions_for_ticker(ticker)
    if not isinstance(submissions, dict):
        return False
    forms = (((submissions.get("filings") or {}).get("recent") or {}).get("form") or [])
    return any(str(form or "").upper() == "20-F" for form in forms)


@yfinance_server.tool(
    name="extract_geographic_revenue",
    description="Extract geographic revenue exposure from official SEC data and indexed filing tables, returning explicit parser/provider limitation statuses when no decision-grade value is available.",
)
async def extract_geographic_revenue(
    ticker: str,
    region: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    accession_number: str | None = None,
    detailLevel: str = "compact",
) -> str:
    failure = _filing_period_failure("extract_geographic_revenue", period)
    if failure:
        return failure
    if not region or not str(region).strip():
        return json.dumps({
            "ticker": ticker,
            "factType": "geographic_revenue",
            "region": region,
            "period": None,
            "rawValue": None,
            "rawDenominator": None,
            "unit": "USD",
            "unitScale": "unknown",
            "value": None,
            "denominator": None,
            "valueRatio": None,
            "valuePct": None,
            "extractionMethod": "NONE",
            "confidence": "NOT_DISCLOSED",
            "evidence": {},
            "calculation": None,
            "warnings": [{"code": "INPUT_VALIDATION_ERROR", "message": "region is required", "severity": "error"}],
        })

    # An explicit accession is the filing read: the figure, the evidence and the raw index all come from it, and a
    # miss fails closed with no fallback to the latest filing (2.15.0, R4-ACC-GEO).
    pinned = accession_number is not None and str(accession_number).strip() != ""
    pinned_filing_form = None
    if pinned:
        resolved_filing, resolve_error = await _resolve_sec_filing(ticker, filing_type, accession_number)
        if resolved_filing is None and _is_accession_not_found(resolve_error):
            return _accession_not_found_envelope(resolve_error)
        pinned_filing_form = str(
            resolved_filing.get("filingType") if resolved_filing is not None else (resolve_error or {}).get("filingType") or ""
        ).upper()
    payload = _safe_json_loads(await get_filing_data(
        ticker=ticker, fact_type=FilingFactType.geographic_revenue, region=region, filing_type=filing_type, period=period,
        accession_number=accession_number if pinned else None,
    ))
    detail = str(detailLevel).lower()
    # The filing index is read for detailLevel raw only, as the Worker does (compact evidence comes from the payload).
    idx_payload = _safe_json_loads(await _sec_filing_index_full(ticker, filing_type, period, accession_number)) if detail == "raw" else {}
    warnings = list(payload["warnings"]) if isinstance(payload.get("warnings"), list) else []
    payload_status = str(_nn(payload.get("status"), _nn(payload.get("code"), _nn(payload.get("confidence"), "")))).strip()
    if payload.get("value") is not None and payload.get("denominator") is None and not any(isinstance(w, dict) and w.get("code") == "DENOMINATOR_NOT_FOUND" for w in warnings):
        warnings.append({
            "code": "DENOMINATOR_NOT_FOUND",
            "message": "Could not compute geographic revenue percentage due to missing denominator.",
            "severity": "warning",
        })

    def _evidence(source: dict, index: dict, default_filing_type: str) -> dict:
        found = source.get("evidence") if isinstance(source.get("evidence"), dict) else {}
        return {
            "filingType": _nn(index.get("filingType"), _nn(source.get("filingType"), default_filing_type)),
            "filingDate": _nn(index.get("filingDate"), source.get("filingDate")),
            "acceptedAt": index.get("acceptedAt"),
            "accessionNumber": _nn(index.get("accessionNumber"), source.get("accessionNumber")),
            "documentUrl": _nn(index.get("documentUrl"), source.get("documentUrl")),
            "sectionHeading": found.get("sectionHeading"),
            "tableTitle": found.get("tableTitle"),
            "sourceTableId": found.get("sourceTableId"),
            "sourceRows": found.get("sourceRows") if isinstance(found.get("sourceRows"), list) else [],
            "sourceColumns": found.get("sourceColumns") if isinstance(found.get("sourceColumns"), list) else [],
        }

    has_denominator = payload.get("denominator") is not None
    shaped = {
        "ticker": ticker,
        "factType": "geographic_revenue",
        "region": region,
        "period": payload.get("period"),
        "rawValue": payload.get("rawValue"),
        "rawDenominator": payload.get("rawDenominator"),
        "unit": _nn(payload.get("unit"), "USD"),
        "unitScale": _nn(payload.get("unitScale"), "unknown"),
        "unitScaleSource": payload.get("unitScaleSource"),
        "value": payload.get("value"),
        "denominator": payload.get("denominator"),
        "valueRatio": payload.get("valueRatio") if has_denominator else None,
        "valuePct": payload.get("valuePct") if has_denominator else None,
        "extractionMethod": _nn(payload.get("extractionMethod"), "NONE"),
        "confidence": _nn(payload.get("confidence"), "NOT_DISCLOSED"),
        "evidence": _evidence(payload, idx_payload, filing_type),
        "calculation": payload.get("calculation"),
        "scanCoverage": payload.get("scanCoverage"),
        "searchedTerms": payload["searchedTerms"] if isinstance(payload.get("searchedTerms"), list) else [],
        "notDisclosedBasis": payload.get("notDisclosedBasis"),
        "status": "FOUND" if payload.get("value") is not None else (payload_status or "NOT_DISCLOSED"),
        "code": _nn(payload.get("code"), payload_status if payload_status and payload_status not in ("FOUND", "NOT_DISCLOSED") else None),
        "warnings": warnings,
    }
    if str(filing_type or "").upper() == "10-K" and str(shaped["confidence"] or "").upper() == "NOT_DISCLOSED":
        evidence_filing_type = str(shaped["evidence"].get("filingType") or "").upper()
        maybe_20f = evidence_filing_type == "20-F"
        if pinned:
            # A pinned read stays pinned: the 20-F retry reads the same filing, and only when that filing is a 20-F (2.15.0).
            maybe_20f = pinned_filing_form == "20-F"
        elif not maybe_20f and evidence_filing_type in ("", "10-K"):
            maybe_20f = await _may_be_20f_filer(ticker)
        if maybe_20f:
            # Automatic 20-F fallback: retry extraction with 20-F filing type
            fallback_payload = _safe_json_loads(await get_filing_data(
                ticker=ticker, fact_type=FilingFactType.geographic_revenue, region=region, filing_type="20-F", period=period,
                accession_number=accession_number if pinned else None,
            ))
            fallback_idx = _safe_json_loads(await _sec_filing_index_full(ticker, "20-F", period, accession_number))
            if fallback_payload.get("value") is not None:
                # 20-F extraction succeeded — replace the output with the fallback data
                fallback_denominator = fallback_payload.get("denominator") is not None
                shaped["period"] = fallback_payload.get("period")
                shaped["rawValue"] = fallback_payload.get("rawValue")
                shaped["rawDenominator"] = fallback_payload.get("rawDenominator")
                shaped["unit"] = _nn(fallback_payload.get("unit"), "USD")
                shaped["unitScale"] = _nn(fallback_payload.get("unitScale"), "unknown")
                shaped["unitScaleSource"] = fallback_payload.get("unitScaleSource")
                shaped["value"] = fallback_payload.get("value")
                shaped["denominator"] = fallback_payload.get("denominator")
                shaped["valueRatio"] = fallback_payload.get("valueRatio") if fallback_denominator else None
                shaped["valuePct"] = fallback_payload.get("valuePct") if fallback_denominator else None
                shaped["extractionMethod"] = _nn(fallback_payload.get("extractionMethod"), "NONE")
                shaped["confidence"] = _nn(fallback_payload.get("confidence"), "NOT_DISCLOSED")
                shaped["evidence"] = _evidence(fallback_payload, fallback_idx, "20-F")
                shaped["calculation"] = fallback_payload.get("calculation")
                shaped["scanCoverage"] = fallback_payload.get("scanCoverage")
                shaped["searchedTerms"] = fallback_payload["searchedTerms"] if isinstance(fallback_payload.get("searchedTerms"), list) else []
                shaped["notDisclosedBasis"] = fallback_payload.get("notDisclosedBasis")
                shaped["status"] = "FOUND"
                shaped["code"] = fallback_payload.get("code")
                # Append advisory warning noting automatic 20-F selection
                shaped["warnings"] = [
                    *(fallback_payload["warnings"] if isinstance(fallback_payload.get("warnings"), list) else []),
                    {
                        "code": "AUTO_20F_FALLBACK",
                        "message": "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
                        "severity": "info",
                    },
                ]
            elif not any(isinstance(w, dict) and w.get("code") == "POSSIBLE_20F_FILER" for w in warnings):
                # 20-F extraction also failed — keep the original and add an advisory warning
                warnings.append({
                    "code": "POSSIBLE_20F_FILER",
                    "message": "POSSIBLE_20F_FILER: Ticker may file 20-F. Retry with filing_type='20-F' or use IR web search.",
                    "severity": "warning",
                })
    if detail == "raw":
        shaped["rawContext"] = {"filingIndex": idx_payload}
    return json.dumps(shaped)


@yfinance_server.tool(name="extract_segment_revenue", description="Extract segment revenue rows from official SEC facts and filing tables, returning explicit limitation statuses when no parseable segment data is found.")
async def extract_segment_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    detailLevel: str = "compact",
) -> str:
    return await _extract_segment_revenue(ticker, filing_type, period, detailLevel)


async def _extract_segment_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
    # Internal only (not a tool param): a pinned caller reads the pinned filing (2.24.0, F21).
    accession_number: str | None = None,
) -> str:
    failure = _filing_period_failure("extract_segment_revenue", period)
    if failure:
        return failure
    pin = str(accession_number).strip() if accession_number is not None and str(accession_number).strip() else None
    payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.segment_revenue, filing_type=filing_type, period=period, accession_number=pin))
    segments = payload.get("allSegments") if isinstance(payload.get("allSegments"), list) else []
    rows = []
    for seg in segments:
        if not isinstance(seg, dict):
            continue
        rows.append({
            "label": seg.get("segmentLabel"),
            "value": seg.get("value"),
            "period": f"FY{seg.get('fiscalYear')}" if seg.get("fiscalYear") else None,
            "confidence": "HIGH",
            "evidence": {
                "filingDate": seg.get("filingDate"),
                "accessionNumber": seg.get("accessionNumber"),
            },
        })
    out = {"ticker": ticker, "factType": "segment_revenue", "segments": rows, "status": "FOUND" if rows else "NOT_DISCLOSED"}
    warnings: list[dict] = []
    # Automatic 20-F fallback for foreign private issuers
    if not rows and str(filing_type or "").upper() == "10-K":
        if pin:
            # A pinned read stays pinned: the 20-F retry reads the same filing, and only when that filing is a 20-F (2.24.0, F21).
            pinned_filing, _pin_error = await _resolve_sec_filing(ticker, filing_type, pin)
            maybe_20f = pinned_filing is not None and str(pinned_filing.get("filingType") or "").upper() == "20-F"
        else:
            maybe_20f = await _may_be_20f_filer(ticker)
        if maybe_20f:
            fb_payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.segment_revenue, filing_type="20-F", period=period, accession_number=pin))
            fb_segments = fb_payload.get("allSegments") if isinstance(fb_payload.get("allSegments"), list) else []
            fb_rows = []
            for seg in fb_segments:
                if not isinstance(seg, dict):
                    continue
                fb_rows.append({
                    "label": seg.get("segmentLabel"),
                    "value": seg.get("value"),
                    "period": f"FY{seg.get('fiscalYear')}" if seg.get("fiscalYear") else None,
                    "confidence": "HIGH",
                    "evidence": {
                        "filingDate": seg.get("filingDate"),
                        "accessionNumber": seg.get("accessionNumber"),
                    },
                })
            if fb_rows:
                rows = fb_rows
                out = {"ticker": ticker, "factType": "segment_revenue", "segments": rows, "status": "FOUND"}
                warnings.append({
                    "code": "AUTO_20F_FALLBACK",
                    "message": "Filing type automatically adapted from 10-K to 20-F (foreign private issuer detected).",
                    "severity": "info",
                })
            else:
                warnings.append({
                    "code": "POSSIBLE_20F_FILER",
                    "message": "POSSIBLE_20F_FILER: Ticker may file 20-F. Retry with filing_type='20-F' or use IR web search.",
                    "severity": "warning",
                })
    if not rows and payload.get("code") == "INVALID_PERIOD":
        # A fiscal year with a quarterly filing type: no table read can answer it either (2.25.1, F26).
        invalid = next((w for w in (payload.get("warnings") if isinstance(payload.get("warnings"), list) else [])
                        if isinstance(w, dict) and w.get("code") == "INVALID_PERIOD"), None)
        out.update({"status": "INVALID_PERIOD", "code": "INVALID_PERIOD", "message": invalid.get("message") if invalid else None})
    elif not rows:
        # SEC companyconcept facts carry no segment dimensions, so most filers need the filing's own segment table;
        # the table read honours the period and a pinned filing (2.25.1, F26).
        out.update(await _segment_revenue_from_filing_table(ticker, filing_type, warnings, period, pin))
    if warnings:
        out["warnings"] = warnings
    if str(detailLevel).lower() == "raw":
        out["rawContext"] = payload
    return json.dumps(out)


async def _segment_revenue_from_filing_table(
    ticker: str,
    filing_type: str,
    warnings: list[dict],
    period: str = "latest",
    pin: str | None = None,
) -> dict:
    """The Worker's segmentRevenueFromFilingTable: segment rows parsed from the filing's own table (2.25.1, F26)."""
    named = _sf.parse_filing_period(period)
    fiscal_year = named["year"] if named and named["kind"] == "fiscalYear" else None
    filing, error = await _resolve_sec_filing(ticker, filing_type, pin)
    if filing is None:
        error = error or {}
        return {"status": str(error.get("status") or error.get("code") or "FILING_NOT_FOUND"), "code": error.get("code"), "message": error.get("message")}
    warnings.extend(filing["warnings"])
    filing_meta = {
        "filingType": filing["filingType"],
        "filingDate": filing["filingDate"],
        "accessionNumber": filing["accessionNumber"],
        "documentUrl": filing["documentUrl"],
    }
    html = await _edgar_get_html(filing["documentUrl"], max_bytes=_SEC_DOCUMENT_READ_MAX_CHARS + 1)
    # The read is bounded in UTF-16 code units, as the Worker's string length.
    html_length = _utf16_length(html) if html else 0
    if html and html_length > _SEC_DOCUMENT_READ_MAX_CHARS:
        html = html.encode("utf-16-le", "surrogatepass")[: 2 * _SEC_DOCUMENT_READ_MAX_CHARS].decode("utf-16-le", "ignore")
        html_length = _utf16_length(html)
    if not html:
        return {**filing_meta, "status": "PROVIDER_ERROR", "code": "PROVIDER_ERROR", "message": "The filing document could not be read."}
    parsed = extract_segment_table_from_html(html, fiscal_year)
    result, tables_seen, tables_scanned, years_seen = parsed["result"], parsed["segmentTablesSeen"], parsed["tablesScanned"], parsed["yearsSeen"]
    truncated = html_length >= _SEC_DOCUMENT_READ_MAX_CHARS
    searched_terms = ["segment", "revenue", "net sales"]
    scan_coverage = {
        "sourceType": "sec_primary_html",
        "documentUrl": filing["documentUrl"],
        "charsScanned": html_length,
        "tablesScanned": tables_scanned,
        "segmentTablesSeen": tables_seen,
        "filingReadTruncated": truncated,
        "searchedTerms": searched_terms,
    }
    if result:
        result_period = f"FY{_re.sub(r'[^0-9]', '', result['column'])}" if result["column"] else None
        scale_assumed = result["unitScaleSource"] == "ASSUMED_MILLIONS"
        if scale_assumed:
            warnings.append(_UNIT_SCALE_ASSUMED_WARNING)
        mult = result["unitMultiplier"]
        total = result["total"]
        return {
            **filing_meta,
            "status": "FOUND",
            "extractionMethod": "PARSED_TABLE",
            "decisionGrade": False,
            "unitScale": result["unitScale"],
            "unitScaleSource": result["unitScaleSource"],
            "segments": [{
                "label": row["label"],
                "value": row["value"] * mult,
                "rawValue": row["rawValue"],
                "period": result_period,
                "confidence": "LOW" if scale_assumed else "MEDIUM",
                "evidence": {
                    "documentUrl": filing["documentUrl"],
                    "filingType": filing["filingType"],
                    "accessionNumber": filing["accessionNumber"],
                    "filingDate": filing["filingDate"],
                    "sourceTableId": result["tableIndex"],
                    "tableTitle": result["title"],
                    "sourceRows": [[row["label"], row["rawValue"]], [total["label"], total["rawValue"]]],
                    "sourceColumns": [result["column"]] if result["column"] else [],
                },
            } for row in result["segments"]],
            # A parsed table's total is no surer than its rows (2.5.21).
            "total": {"label": total["label"], "value": total["value"] * mult, "rawValue": total["rawValue"], "confidence": "LOW" if scale_assumed else "MEDIUM"},
            "evidence": {
                "sourceType": "sec_filing_table",
                "documentUrl": filing["documentUrl"],
                "tableIndex": result["tableIndex"],
                "tableTitle": result["title"],
                "column": result["column"],
            },
            "calculation": {"check": "segments sum to the table total within 1%"},
            "recommendedNextAction": "VERIFY_TABLE_PERIOD_AND_UNITS",
        }
    if fiscal_year is not None and years_seen and fiscal_year not in years_seen:
        # The filing's segment tables report other years: say so rather than return another year's rows (2.25.1, F26).
        return {
            **filing_meta,
            "status": "PERIOD_NOT_FOUND",
            "decisionGrade": False,
            "message": f"The segment tables in this {filing['filingType']} (filed {filing['filingDate']}) have columns for {', '.join(str(y) for y in years_seen)}, not fiscal year {fiscal_year}.",
            "fiscalYearsAvailable": years_seen,
            "scanCoverage": scan_coverage,
            "searchedTerms": searched_terms,
        }
    if tables_seen > 0 or truncated:
        if tables_seen > 0:
            warnings.append({"code": "TABLE_NOT_PARSED", "message": "The filing has tables introduced as segment revenue, but none parsed into segments that sum to a total.", "severity": "warning"})
        if truncated:
            warnings.append({"code": "FILING_READ_TRUNCATED", "message": "Only the first part of the filing was scanned; segment tables may appear later.", "severity": "warning"})
        return {**filing_meta, "status": "EXTRACTION_FAILED", "code": "EXTRACTION_FAILED", "decisionGrade": False, "scanCoverage": scan_coverage, "searchedTerms": searched_terms, "recommendedNextAction": "LIST_SEC_FILING_TABLES"}
    return {
        **filing_meta,
        "status": "NOT_DISCLOSED",
        "decisionGrade": False,
        "scanCoverage": scan_coverage,
        "searchedTerms": searched_terms,
        "notDisclosedBasis": "No XBRL segment facts and no table introduced as segment revenue in the full filing.",
    }


@yfinance_server.tool(name="extract_total_revenue", description="Extract total revenue from official SEC facts or filing tables with evidence metadata.")
async def extract_total_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
) -> str:
    return await _extract_total_revenue(ticker, filing_type, period)


async def _extract_total_revenue(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    # Internal only (not a tool param): a pinned caller reads the pinned filing (2.24.0, F21).
    accession_number: str | None = None,
) -> str:
    failure = _filing_period_failure("extract_total_revenue", period)
    if failure:
        return failure
    pin = str(accession_number).strip() if accession_number is not None and str(accession_number).strip() else None
    payload = _safe_json_loads(await get_filing_data(ticker=ticker, fact_type=FilingFactType.total_revenue, filing_type=filing_type, period=period, accession_number=pin))
    val = payload.get("value")
    status = "FOUND" if val is not None else _as_status(payload)
    source_evidence = _sec_xbrl_source_evidence(payload)
    decision_grade = _is_decision_grade_sec_xbrl_fact(
        payload,
        source_evidence,
        status,
    )
    filing_evidence = {
        "filingType": payload.get("filingType", filing_type),
        "filingDate": payload.get("filingDate"),
        "accessionNumber": payload.get("accessionNumber"),
        "documentUrl": payload.get("documentUrl"),
    }
    return json.dumps({
        "ticker": ticker,
        "factType": "total_revenue",
        "value": val,
        "period": payload.get("period"),
        # No value, no unit: a missing fact has no currency (2.5.15).
        "unit": (payload.get("unit") or "USD") if val is not None else None,
        "unitScale": payload.get("unitScale"),
        "confidence": _nn(payload.get("confidence"), "NOT_DISCLOSED" if val is None else "LOW"),
        "extractionMethod": payload.get("extractionMethod", "NONE"),
        "evidence": source_evidence if decision_grade else filing_evidence,
        "sourceEvidence": source_evidence,
        "xbrlContext": payload.get("xbrlContext"),
        "decisionGrade": decision_grade,
        "status": status,
        # Why there is no value (NO_SEC_REGISTRANT, SEC_FACTS_IFRS_ONLY, ...) and the read's warnings (2.5.15).
        "code": None if val is not None else (payload.get("code") if isinstance(payload.get("code"), str) else None),
        "message": None if val is not None else (payload.get("message") if isinstance(payload.get("message"), str) else None),
        # A failed SEC read says so and which reads failed; retry rather than treat the fact as missing (2.5.17).
        **({"retryable": True} if val is None and payload.get("retryable") is True else {}),
        **({"failedReads": payload["failedReads"]} if val is None and isinstance(payload.get("failedReads"), list) else {}),
        "warnings": payload.get("warnings") if isinstance(payload.get("warnings"), list) else [],
    })


@yfinance_server.tool(name="extract_revenue_exposure", description="Extract revenue exposure for a region/customer/segment query, returning explicit parser/provider limitation statuses when no decision-grade value is available.")
async def extract_revenue_exposure(
    ticker: str,
    exposure_query: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    detailLevel: str = "compact",
) -> str:
    return await _extract_revenue_exposure(ticker, exposure_query, filing_type, period, detailLevel)


async def _extract_revenue_exposure(
    ticker: str,
    exposure_query: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
    # Internal only (not a tool param): a pinned caller reads the pinned filing (2.23.0, F3).
    accession_number: str | None = None,
) -> str:
    failure = _filing_period_failure("extract_revenue_exposure", period)
    if failure:
        return failure
    geo = _safe_json_loads(await extract_geographic_revenue(ticker=ticker, region=exposure_query, filing_type=filing_type, period=period, accession_number=accession_number, detailLevel=detailLevel))
    found = geo.get("value") is not None
    status = "FOUND_REVENUE_EXPOSURE" if found else _as_status(geo)
    warnings = geo["warnings"] if isinstance(geo.get("warnings"), list) else []
    # A value read from a product or category table ("iPhone" in "net sales by category") is segment revenue, not
    # geographic revenue (2.25.4 audit, F40). The filing is cached, so re-reading its chosen table costs no request.
    exposure_type = "geographic_revenue"
    parsed_evidence = geo.get("evidence") if isinstance(geo.get("evidence"), dict) else {}
    parsed_url = parsed_evidence.get("documentUrl") or geo.get("documentUrl")
    if found and geo.get("extractionMethod") == "PARSED_TABLE" and isinstance(parsed_url, str) and parsed_url:
        html_text = await _edgar_get_html(parsed_url, max_bytes=5_000_000)
        table = _extract_geo_table(html_text, exposure_query) if html_text else None
        if table and _PRODUCT_BREAKDOWN_LEAD_RE.search(table.get("tableLead") or ""):
            exposure_type = "segment_revenue"
    matches = []
    if found:
        matches.append({
            "exposureType": exposure_type,
            "label": exposure_query,
            "value": geo.get("value"),
            "denominator": geo.get("denominator"),
            "valueRatio": geo.get("valueRatio"),
            "valuePct": geo.get("valuePct"),
            "period": geo.get("period"),
            "confidence": _nn(geo.get("confidence"), "LOW"),
            "evidence": _nn(geo.get("evidence"), {}),
        })
    geo_evidence = geo["evidence"] if isinstance(geo.get("evidence"), dict) else {}
    return json.dumps({
        "ticker": ticker,
        "query": exposure_query,
        "matches": matches,
        "status": status,
        # Why there is no value (SEC_READ_FAILED, FILING_NOT_FOUND_TRY_OTHER_TYPE, ...); a plain NOT_DISCLOSED has no code.
        "code": None if found else _nn(geo.get("code"), None if status == "NOT_DISCLOSED" else status),
        "requestedFilingType": _nn(geo.get("requestedFilingType"), filing_type),
        "filingType": _nn(geo_evidence.get("filingType"), _nn(geo.get("filingType"), filing_type)),
        "filingDate": _nn(geo_evidence.get("filingDate"), geo.get("filingDate")),
        "accessionNumber": _nn(geo_evidence.get("accessionNumber"), geo.get("accessionNumber")),
        "documentUrl": _nn(geo_evidence.get("documentUrl"), geo.get("documentUrl")),
        "availableFilingTypes": _nn(geo.get("availableFilingTypes"), []),
        "suggestedFilingTypes": _nn(geo.get("suggestedFilingTypes"), []),
        "warnings": warnings,
    })


_EXPOSURE_XBRL_TOKEN_RE = _re.compile(
    r"\b(?:us-gaap|srt|country|dei):[A-Za-z0-9_.-]+\b",
    _re.IGNORECASE,
)
_EXPOSURE_COMPETITION_RE = _re.compile(
    r"\b(?:competitors?|competition|compete(?:s|d|ing)?\s+with)\b",
    _re.IGNORECASE,
)
_EXPOSURE_RELATIONSHIP_RE = _re.compile(
    r"\b(?:supplier|vendor|customer|manufactur\w*|assembl\w*|facilit\w*|factory|"
    r"subsidiar\w*|operations?|production|procur\w*|sourc\w*|partner\w*|"
    r"loan|credit|deposit|account|lender|borrow\w*)\b",
    _re.IGNORECASE,
)


def _exposure_term_match(text: str, term: str):
    """First whole-word match of term, plural forms included ("tariff" matches "tariffs")."""
    escaped = _re.escape(str(term).strip())
    if not escaped:
        return None
    return _re.search(rf"(?<![A-Za-z0-9]){escaped}(?:e?s)?(?![A-Za-z0-9])", text, _re.IGNORECASE)


def _exposure_term_present(text: str, term: str) -> bool:
    return _exposure_term_match(text, term) is not None


def _term_centered_excerpt(text: str, terms: list[str], max_len: int) -> str:
    """About max_len characters around the first term, starting at its sentence when that fits."""
    cleaned = " ".join(text.split())
    if len(cleaned) <= max_len:
        return cleaned
    starts = [m.start() for m in (_exposure_term_match(cleaned, term) for term in terms) if m]
    if not starts:
        return _compact_excerpt(cleaned, max_len)
    at = min(starts)
    start = max(0, min(at - max_len // 3, len(cleaned) - max_len))
    sentence = cleaned.rfind(". ", start, at)
    if sentence >= 0:
        start = sentence + 2
    elif start > 0:
        space = cleaned.find(" ", start, at)
        if space >= 0:
            start = space + 1
    end = start + max_len
    if end < len(cleaned):
        space = cleaned.rfind(" ", at, end)
        if space > at:
            end = space
    return f"{'...' if start > 0 else ''}{cleaned[start:end].strip()}{'...' if end < len(cleaned) else ''}"


def _readable_exposure_excerpt(
    value: object,
    terms: list[str],
    *,
    max_len: int = 240,
    min_words: int = 5,
) -> str:
    cleaned = _EXPOSURE_XBRL_TOKEN_RE.sub(" ", str(value or ""))
    excerpt = _term_centered_excerpt(cleaned, terms, max_len)
    if not excerpt or not any(_exposure_term_present(excerpt, term) for term in terms):
        return ""
    if len(_re.findall(r"[A-Za-z]{2,}", excerpt)) < min_words:
        return ""
    return excerpt


def _contextual_entity_excerpt(value: object, entity: str, *, max_len: int = 240) -> str:
    excerpt = _readable_exposure_excerpt(value, [entity], max_len=max_len, min_words=4)
    if not excerpt or _EXPOSURE_COMPETITION_RE.search(excerpt):
        return ""
    return excerpt if _EXPOSURE_RELATIONSHIP_RE.search(excerpt) else ""


@yfinance_server.tool(name="extract_risk_factor_mentions", description="Extract concise risk-factor mentions for explicit terms from a filing.")
async def extract_risk_factor_mentions(
    ticker: str,
    terms: list[str],
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    return await _extract_risk_factor_mentions(ticker, terms, filing_type, period, detailLevel)


async def _extract_risk_factor_mentions(
    ticker: str,
    terms: list[str],
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
    # Internal only (not a tool param): a pinned caller reads the pinned filing (2.24.0, F21).
    accession_number: str | None = None,
) -> str:
    matches: list[dict] = []
    rejected_noise_count = 0
    raw_match_count = 0
    for term in (terms or []):
        search = _safe_json_loads(await search_sec_filing_text(
            ticker=ticker,
            search_terms=[str(term)],
            section_hint="Risk Factors",
            filing_type=filing_type,
            accession_number=accession_number,
        ))
        raw_match_count += int(search.get("matchCount") or 0)
        for m in (search.get("matches") if isinstance(search.get("matches"), list) else [])[:3]:
            if not isinstance(m, dict):
                continue
            row_term = str(m.get("term") or "")
            heading = str(m.get("sectionHeading") or "Risk Factors")
            if row_term and row_term.lower() != str(term).lower():
                rejected_noise_count += 1
                continue
            if m.get("sectionHeading") and "risk" not in heading.lower():
                rejected_noise_count += 1
                continue
            excerpt = _readable_exposure_excerpt(
                m.get("contextText") or m.get("context") or m.get("excerpt") or "",
                [str(term)],
            )
            if not excerpt:
                rejected_noise_count += 1
                continue
            matches.append({
                "term": term,
                "sectionHeading": heading,
                "excerpt": excerpt,
                "excerptAvailable": True,
                "confidence": "MEDIUM",
                "evidence": {
                    "filingDate": search.get("filingDate"),
                    "accessionNumber": search.get("accessionNumber"),
                    "documentUrl": search.get("documentUrl"),
                },
            })
    result = {
        "ticker": ticker,
        "matches": matches,
        "status": "FOUND" if matches else ("FOUND_NO_EXCERPT" if raw_match_count else "NOT_FOUND"),
        "rawMatchCount": raw_match_count,
        "rejectedNoiseCount": rejected_noise_count,
    }
    if str(detailLevel).lower() == "raw":
        result["rawTerms"] = terms or []
    return json.dumps(result)


_CUSTOMER_TABLE_ROW_TERMS = ["Customer A", "Customer B", "Customer C", "Customer 1", "Distributor A", "Distributor B"]


@yfinance_server.tool(name="extract_customer_concentration", description="Extract customer concentration percentages from SEC filing text evidence.")
async def extract_customer_concentration(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
) -> str:
    return await _extract_customer_concentration(ticker, filing_type, period, detailLevel)


async def _extract_customer_concentration(
    ticker: str,
    filing_type: str = "10-K",
    period: str = "latest",
    detailLevel: str = "compact",
    # Internal only (not a tool param): a pinned caller reads the pinned filing (2.24.0, F21).
    accession_number: str | None = None,
) -> str:
    # Mirrors the Worker: revenue-concentration rules live in yfmcp/extraction_rules.py.
    searched_terms = ["major customer", "customers", "customer accounted", "accounted for", "of our revenue", "of total revenue", "of net sales", "percent of revenue"]
    search = _safe_json_loads(await search_sec_filing_text(ticker=ticker, search_terms=searched_terms, filing_type=filing_type, accession_number=accession_number, context_chars=1200))
    matches = [m for m in (search.get("matches") if isinstance(search.get("matches"), list) else []) if isinstance(m, dict)]
    # The rows of a significant-customer table in the same filing, which the prose search does not reach (2.5.32,
    # F-024: MRVL's "Distributor A | 37% | 34% | 24%").
    if search.get("accessionNumber"):
        table_search = _safe_json_loads(await search_sec_filing_text(
            ticker=ticker, search_terms=_CUSTOMER_TABLE_ROW_TERMS, filing_type=filing_type, accession_number=str(search["accessionNumber"]),
            max_matches=12, context_chars=1200))
        matches += [m for m in (table_search.get("matches") if isinstance(table_search.get("matches"), list) else []) if isinstance(m, dict) and m.get("inTable") is True]
    filing_evidence = {
        "filingDate": search.get("filingDate"),
        "accessionNumber": search.get("accessionNumber"),
        "documentUrl": search.get("documentUrl"),
    }
    fiscal_year = str(search.get("fiscalYear") or "")
    fallback_period = (fiscal_year if fiscal_year.upper().startswith("FY") else f"FY{fiscal_year}") if fiscal_year else None
    rules = _er.customer_concentration(matches)

    def _evidence(f: dict) -> dict:
        return {"sectionHeading": f["sectionHeading"], "excerpt": _compact_excerpt(f["sentence"]), **filing_evidence}

    def _period(f: dict) -> str | None:
        return f"FY{f['year']}" if f["year"] is not None else fallback_period

    customers = [{
        "label": f["name"] or "Unnamed customer",
        "name": f["name"],
        "nameDisclosed": f["name"] is not None,
        "description": f["description"],
        "valuePct": f["valuePct"],
        "period": _period(f),
        "confidence": "MEDIUM",
        "evidence": _evidence(f),
    } for f in rules["findings"] if f["kind"] == "customer"][:8]
    aggregates = [{
        "description": f["description"],
        "valuePct": f["valuePct"],
        "period": _period(f),
        "confidence": "MEDIUM",
        "evidence": _evidence(f),
    } for f in rules["findings"] if f["kind"] == "aggregate"][:4]
    negation = rules["negation"]
    match_count = int(search.get("matchCount") or 0)
    scan_coverage = {"sourceType": "sec_primary_html", **filing_evidence, "matchCount": match_count, "searchedTerms": searched_terms}
    result: dict = {"ticker": ticker, "customers": customers, "aggregates": aggregates}
    if customers or aggregates:
        result["status"] = "FOUND"
    elif negation:
        result.update({
            "status": "NOT_DISCLOSED",
            "notDisclosedBasis": "The filing states that no customer reached the disclosure threshold.",
            "evidence": {"sectionHeading": negation["sectionHeading"], "excerpt": _compact_excerpt(negation["sentence"]), **filing_evidence},
            "scanCoverage": scan_coverage,
        })
    elif match_count > 0:
        result.update({
            "status": "EXTRACTION_FAILED",
            "code": "EXTRACTION_FAILED",
            "scanCoverage": scan_coverage,
            "warnings": [{"code": "CUSTOMER_PERCENT_NOT_PARSED", "message": "Customer disclosures were found, but no customer percentage or no-major-customer statement was parsed.", "severity": "warning"}],
        })
    else:
        result.update({"status": "NOT_FOUND", "scanCoverage": scan_coverage})
    if str(detailLevel).lower() == "raw":
        result["rawMatchCount"] = match_count
    return json.dumps(result)


# ── Dilution bridge, capital structure, analyst methods, UK filings (2.3.0) ──
# Mirrors the Worker; the parsing and arithmetic live in yfmcp.capital_structure.

_IXBRL_DOCUMENTS: dict[str, tuple[_cs.IxDocument, bool, float]] = {}
_IXBRL_DOCUMENT_TTL = 30 * 60
_IXBRL_DOCUMENT_MAX = 8
_AWARD_TABLE_SEARCH_TERMS = ["unvested", "nonvested"]
_ATM_SEARCH_TERMS = ["at-the-market", "at the market offering", "ATM program", "equity distribution agreement", "sales agreement"]
_FUNDING_SEARCH_TERMS = ["sufficient to fund", "sufficient to meet", "fully funded", "going concern", "cash runway", "next twelve months", "next 12 months", "at-the-market", "capital expenditures"]


def _resolve_failure(error: dict, ticker: str) -> str:
    """The Worker's resolveFailure: ACCESSION_NOT_FOUND as its error envelope, any other resolver error with the ticker."""
    if _is_accession_not_found(error):
        return _accession_not_found_envelope(error)
    return json.dumps({"ticker": ticker, **error})


async def _ixbrl_source(role: str, filing: dict) -> tuple[_cs.IxSource, bool] | None:
    url = filing["documentUrl"]
    hit = _IXBRL_DOCUMENTS.get(url)
    if hit is not None and time.monotonic() - hit[2] < _IXBRL_DOCUMENT_TTL:
        doc, truncated = hit[0], hit[1]
    else:
        html = await _edgar_get_html(url, max_bytes=_SEC_DOCUMENT_READ_MAX_CHARS + 1)
        if not html:
            return None
        truncated = len(html) > _SEC_DOCUMENT_READ_MAX_CHARS
        doc = _cs.parse_ixbrl(html[:_SEC_DOCUMENT_READ_MAX_CHARS])
        _IXBRL_DOCUMENTS.pop(url, None)
        _IXBRL_DOCUMENTS[url] = (doc, truncated, time.monotonic())
        while len(_IXBRL_DOCUMENTS) > _IXBRL_DOCUMENT_MAX:
            _IXBRL_DOCUMENTS.pop(next(iter(_IXBRL_DOCUMENTS)))
    source = _cs.IxSource(role, filing["filingType"], filing["filingDate"], filing["accessionNumber"], url, doc)
    return source, truncated


async def _filing_text_matches(ticker: str, filing: dict, terms: list[str], max_matches: int, context_chars: int) -> list[_cs.TextMatch]:
    search = _safe_json_loads(await search_filing_text(
        ticker, terms, None, filing["filingType"], filing["accessionNumber"], context_chars, False, None, max_matches=max_matches,
    ))
    return _text_matches_from(search)


async def _share_claim_matches(ticker: str, filings: list[tuple[str, dict]]) -> list[_cs.TextMatch] | None:
    """Share-claim text across the resolved filings; None when a filing's text could not be searched."""
    out: list[_cs.TextMatch] = []
    for _, filing in filings:
        search = _safe_json_loads(await search_filing_text(
            ticker, _cs.SHARE_CLAIM_SEARCH_TERMS, None, filing["filingType"], filing["accessionNumber"], 900, False, None, max_matches=32,
        ))
        if search.get("error") or search.get("code") or not isinstance(search.get("matches"), list):
            return None
        out.extend(_text_matches_from(search))
    return out


async def _warrant_lifecycle_matches(ticker: str, filings: list[tuple[str, dict]], counts: list[float]) -> list[_cs.TextMatch] | None:
    """Exercise, expiry and redemption text for warrant classes counted before the period end (2.5.11).

    Searched by the lifecycle phrases and each class's count; None when a filing's text could not be searched.
    """
    terms = [*_cs.WARRANT_LIFECYCLE_SEARCH_TERMS,
             *(f"{int(n):,}" for n in counts if isinstance(n, (int, float)) and not isinstance(n, bool) and float(n).is_integer() and n >= 1000)]
    return await _searched_matches(ticker, filings, terms)


async def _searched_matches(ticker: str, filings: list[tuple[str, dict]], terms: list[str]) -> list[_cs.TextMatch] | None:
    """Text matches for the terms across the resolved filings; None when a filing's text could not be searched."""
    out: list[_cs.TextMatch] = []
    for _, filing in filings:
        search = _safe_json_loads(await search_filing_text(
            ticker, terms, None, filing["filingType"], filing["accessionNumber"], 900, False, None, max_matches=32,
        ))
        if search.get("error") or search.get("code") or not isinstance(search.get("matches"), list):
            return None
        out.extend(_text_matches_from(search))
    return out


def _text_matches_from(search: dict) -> list[_cs.TextMatch]:
    out = []
    for m in search.get("matches") if isinstance(search.get("matches"), list) else []:
        if not isinstance(m, dict):
            continue
        heading = m.get("sectionHeading")
        out.append(_cs.TextMatch(
            context_text=str(m.get("contextText") or ""),
            section_heading=heading if isinstance(heading, str) and heading else None,
            document_url=_first_present(m.get("documentUrl"), search.get("documentUrl")),
            filing_date=_first_present(m.get("filingDate"), search.get("filingDate")),
            accession_number=_first_present(m.get("accessionNumber"), search.get("accessionNumber")),
            in_table=m.get("inTable") is True,
            table_title=m.get("tableTitle") if isinstance(m.get("tableTitle"), str) else None,
            row_label=m.get("rowLabel") if isinstance(m.get("rowLabel"), str) else None,
        ))
    return out


def _first_present(*values: Any) -> Any:
    return next((v for v in values if v is not None), None)


@yfinance_server.tool(
    name="extract_dilution_bridge",
    description="Basic-to-diluted share bridge at a price you supply, from the filing's inline XBRL: cover-page basic shares, options (treasury-stock method, by exercise-price range when tagged), unvested RSUs/PSUs (gross), warrants per class (treasury stock), convertibles and convertible preferred (if-converted when in the money) and ATM remaining capacity from filing text. Mechanical and company-disclosed, not a consensus diluted share count; never back-solve it into one. status covers tagged instruments only, never a full claim inventory: share claims stated only in filing text (price protection, anti-dilution rights, forward sales, contingent or share-settled obligations, exchangeable interests, SAFEs, equity lines, post-period warrants) are quoted in unquantifiedShareClaims and never counted. filing_type latest uses the newest 10-Q with the last 10-K as fallback.",
)
async def extract_dilution_bridge(
    ticker: str,
    price: float,
    as_of_date: str | None = None,
    currency: str = "USD",
    filing_type: str = "latest",
    accession_number: str | None = None,
    include_atm: bool = True,
) -> str:
    try:
        price_value = float(price)
    except (TypeError, ValueError):
        price_value = math.nan
    if not math.isfinite(price_value) or price_value <= 0:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "price is required and must be a positive number: the bridge is evaluated at the price you supply."})
    filings, error = await _resolve_periodic_filings(ticker, filing_type, accession_number)
    if error:
        return _resolve_failure(error, ticker)
    loaded = [await _ixbrl_source(role, filing) for role, filing in filings]
    sources = [item[0] for item in loaded if item is not None]
    if not sources:
        return json.dumps({"ticker": ticker, "status": "FILING_TEXT_NOT_AVAILABLE", "code": "FILING_TEXT_NOT_AVAILABLE", "message": "The filing document could not be read from SEC."})
    atm_matches: list[_cs.TextMatch] = []
    if include_atm is not False:
        for _, filing in filings:
            atm_matches = await _filing_text_matches(ticker, filing, _ATM_SEARCH_TERMS, 10, 800)
            if atm_matches:
                break
    claim_matches = await _share_claim_matches(ticker, filings)
    bridge_args = (
        ticker.upper(), price_value, (currency or "USD").upper(),
        as_of_date if as_of_date and _re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", as_of_date) else None,
        sources, atm_matches,
    )
    out = _cs.dilution_bridge(*bridge_args, None, claim_matches)
    # A warrant class counted before the period end: read the filing text for its exercise, expiry or redemption (2.5.11).
    lifecycle_matches: list[_cs.TextMatch] | None = None
    warrant_component = next((c for c in out["components"] if c.get("component") == "warrants"), None)
    early_counts = [c.get("outstanding") for c in ((warrant_component or {}).get("classes") or []) if c.get("countBeforePeriodEnd") is True]
    if early_counts:
        lifecycle_matches = await _warrant_lifecycle_matches(ticker, filings, early_counts)
    # Convertible notes: read the filing text for a stated cash settlement of principal (2.5.11).
    settlement_matches: list[_cs.TextMatch] | None = None
    capped_call_matches: list[_cs.TextMatch] | None = None
    has_convertibles = any(c.get("component") == "convertible_debt" for c in out["components"])
    if has_convertibles:
        settlement_matches = await _searched_matches(ticker, filings, list(_cs.CONVERTIBLE_SETTLEMENT_SEARCH_TERMS))
        # And for capped call terms (2.5.12).
        capped_call_matches = await _searched_matches(ticker, filings, list(_cs.CAPPED_CALL_SEARCH_TERMS))
    if early_counts or has_convertibles:
        out = _cs.dilution_bridge(*bridge_args, None, claim_matches, lifecycle_matches, settlement_matches, capped_call_matches)
    # No unvested award count is tagged: read it from the filing's award table.
    if "unvested_share_awards" in out["notDisclosed"]:
        for _, filing in filings:
            table_matches = [m for m in await _filing_text_matches(ticker, filing, _AWARD_TABLE_SEARCH_TERMS, 30, 400) if m.in_table]
            if not table_matches:
                continue
            retried = _cs.dilution_bridge(*bridge_args, table_matches, claim_matches, lifecycle_matches, settlement_matches, capped_call_matches)
            if "unvested_share_awards" not in retried["notDisclosed"]:
                out = retried
                break
    if any(item is not None and item[1] for item in loaded):
        out["warnings"].append({"code": "FILING_READ_TRUNCATED", "message": "A filing exceeded the read limit; facts past that point were not parsed.", "severity": "warning"})
    return json.dumps(out)


@yfinance_server.tool(
    name="extract_capital_structure",
    description="Company-disclosed capital structure at the filing's period end: cash, short-term investments, total debt and net cash; each debt instrument's face amount, carrying amount, coupon, maturity and conversion terms from dimensional inline XBRL (which companyfacts omits); the tagged maturity ladder; and the company's own funding, runway, going-concern and ATM statements quoted from the filing. Disclosed, not forecast.",
)
async def extract_capital_structure(
    ticker: str,
    filing_type: str = "latest",
    accession_number: str | None = None,
    include_funding_statements: bool = True,
) -> str:
    filings, error = await _resolve_periodic_filings(ticker, filing_type, accession_number)
    if error:
        return _resolve_failure(error, ticker)
    _, primary = filings[0]
    loaded = await _ixbrl_source("primary", primary)
    if loaded is None:
        return json.dumps({"ticker": ticker, "status": "FILING_TEXT_NOT_AVAILABLE", "code": "FILING_TEXT_NOT_AVAILABLE", "message": "The filing document could not be read from SEC."})
    funding = await _filing_text_matches(ticker, primary, _FUNDING_SEARCH_TERMS, 20, 700) if include_funding_statements is not False else []
    out = _cs.capital_structure(ticker.upper(), loaded[0], funding)
    # An outstanding instrument with no tagged maturity: read the filing's own maturity sentences (2.5.24).
    if _cs.wants_maturity_text(out):
        maturity_matches = await _filing_text_matches(ticker, primary, _cs.MATURITY_SEARCH_TERMS, 12, 400)
        out = _cs.capital_structure(ticker.upper(), loaded[0], funding, maturity_matches)
    if loaded[1]:
        out["warnings"].append({"code": "FILING_READ_TRUNCATED", "message": "The filing exceeded the read limit; facts past that point were not parsed.", "severity": "warning"})
    return json.dumps(out)


@yfinance_server.tool(
    name="get_share_count_scenarios",
    description="Share counts under scenarios you define: for each scenario's price and treatments (options treasury-stock/gross/exclude, unvested awards gross/exclude, warrants treasury-stock/gross/exclude on vested or all, convertibles if-converted when in the money/all/net-share settlement where the filing states principal is settled in cash/exclude, ATM remaining capacity include/exclude, capped calls ignored or offset where the filing states their strike, cap and covered shares, and known issuance you supply), every instrument from the filing's inline XBRL is listed as included or excluded with its treasury-stock or if-converted mechanics and threshold price; unresolved instruments are listed and left out. Scenarios are reported side by side; no denominator is selected. Evidence only.",
)
async def get_share_count_scenarios(
    ticker: str,
    scenarios: list[dict],
    filing_type: str = "latest",
    accession_number: str | None = None,
    currency: str = "USD",
) -> str:
    parsed = _ss.parse_share_scenarios(scenarios)
    if "error" in parsed:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": parsed["error"]})
    raw = await extract_dilution_bridge(ticker=ticker, price=parsed["scenarios"][0]["price"], currency=currency, filing_type=filing_type,
                                        accession_number=accession_number, include_atm=True)
    bridge = _unwrap_payload(raw)
    if bridge.get("error") is True or bridge.get("basis") != "MECHANICAL_COMPANY_DISCLOSED":
        return raw
    return json.dumps(_ss.share_count_scenarios(ticker, bridge, parsed["scenarios"]))


_SCHEDULE_SEARCH_TERMS = [
    "capital expenditures", "purchase commitments", "purchase obligations", "non-cancelable", "committed to",
    "contractual obligations", "grant agreement", "awarded", "milestone payments", "CHIPS", "incentive agreement",
    "expect to invest", "sufficient to fund", "at-the-market",
]


@yfinance_server.tool(
    name="extract_funding_capex_schedule",
    description="Funding and capex schedule from the latest periodic filing: tagged contractual schedules (debt principal, operating and finance lease payments, purchase and contractual obligations by due period; purchase commitments) and funding or capex statements from the text, each classified CONTRACTUAL, COMPANY_DISCLOSED_COMMITTED, COMPANY_GUIDED, AWARDED_CONTINGENT or UNRESOLVED with amount, timing and the quoted source; plus liquidity sources (cash, undrawn facilities, ATM capacity). Nothing is netted or forecast. Evidence only.",
)
async def extract_funding_capex_schedule(ticker: str, filing_type: str = "latest", accession_number: str | None = None) -> str:
    filings, error = await _resolve_periodic_filings(ticker, filing_type, accession_number)
    if error:
        return _resolve_failure(error, ticker)
    _, primary = filings[0]
    loaded = await _ixbrl_source("primary", primary)
    if loaded is None:
        return json.dumps({"ticker": ticker, "status": "FILING_TEXT_NOT_AVAILABLE", "code": "FILING_TEXT_NOT_AVAILABLE", "message": "The filing document could not be read from SEC."})
    matches = await _filing_text_matches(ticker, primary, _SCHEDULE_SEARCH_TERMS, 30, 700)
    source, truncated = loaded
    capital = _cs.capital_structure(ticker.upper(), source, [])
    remaining = next((e for e in _cs.atm_evidence(matches) if e.get("kind") == "remaining_capacity"), None)
    doc = source.doc
    out = _fsched.funding_capex_schedule(
        ticker=ticker,
        period_end=doc.document_period_end,
        source=capital.get("source"),
        facts=[{"local": f.local, "value": f.value, "periodEnd": f.period_end, "dims": f.dims, "unit": f.unit} for f in doc.facts],
        statements=[{"contextText": m.context_text, "sectionHeading": m.section_heading, "documentUrl": m.document_url,
                     "filingDate": m.filing_date, "accessionNumber": m.accession_number} for m in matches],
        balances=capital.get("balances"),
        atm_remaining_usd=remaining["amountUsd"] if remaining else None,
        atm_evidence=remaining,
    )
    if truncated:
        out["warnings"].append({"code": "FILING_READ_TRUNCATED", "message": "The filing exceeded the read limit; facts past that point were not parsed.", "severity": "warning"})
    return json.dumps(out)


_DRIVER_SEARCH_TERMS = [
    "capacity", "backlog", "bookings", "deliveries", "shipments", "customers", "subscribers", "launched", "deployed",
    "production", "utilization", "average selling price", "employees", "remaining performance obligations",
]


def _earnings_release_picks(subs: dict, cik_int: int, limit: int, include_amendments: bool = False) -> list[dict]:
    """The newest earnings-release 8-Ks (Item 2.02, results of operations) in SEC's recent submissions; 8-K/As too when asked."""
    recent = (subs.get("filings") or {}).get("recent") or {}

    def at(key: str, i: int) -> str:
        values = recent.get(key) or []
        return str(values[i] or "") if i < len(values) else ""

    picks: list[dict] = []
    for i, form in enumerate(recent.get("form") or []):
        if len(picks) >= limit:
            break
        form_upper = str(form or "").upper()
        if not (form_upper == "8-K" or (include_amendments and form_upper == "8-K/A")) or "2.02" not in at("items", i):
            continue
        acc, doc = at("accessionNumber", i), at("primaryDocument", i)
        if not acc or not doc:
            continue
        picks.append({"filingDate": at("filingDate", i), "accessionNumber": acc,
                      "primaryUrl": sec_archive_document_url(cik_int, acc.replace('-', ''), doc), "form": form_upper})
    return picks


async def _read_release_text(cik_int: int, accession_number: str, primary_url: str) -> dict:
    """An earnings-release 8-K's text: EX-99.1 when the index names one, else the 8-K itself."""
    from yfmcp.tools.earnings import _resolve_ex991_url
    try:
        ex991 = await _resolve_ex991_url(accession_number, cik_int)
    except Exception:  # noqa: BLE001 - fall back to the 8-K document
        ex991 = None
    url = ex991 or primary_url
    try:
        html = await _edgar_get_html(url, max_bytes=5_000_000)
    except Exception:  # noqa: BLE001 - reported as unread
        html = None
    if not html:
        return {"url": url, "status": "NOT_READ", "text": None}
    return {"url": url, "status": "READ", "text": _strip_html_blocks(_sanitize_sec_html(html))}


@yfinance_server.tool(
    name="extract_operating_driver_ledger",
    description="Operating drivers as the company discloses them: SEC XBRL series (revenue, gross profit, R&D, capex, remaining performance obligations, contract liabilities; newest filing per period, year-to-date periods kept as filed), the filer's own tagged KPIs from its latest periodic filing, and filing and earnings-release statements with figures by category (capacity, production, deliveries, launches and deployments, customers, backlog and bookings, utilization, pricing, yield, headcount), each marked REPORTED_ACTUAL, TARGET_OR_PLAN or UNCLEAR with timing and the quoted source. Untagged drivers are listed as not reported and unread sources as not read; nothing is derived or annualized. Evidence only.",
)
async def extract_operating_driver_ledger(ticker: str, filing_type: str = "latest", accession_number: str | None = None) -> str:
    filings, error = await _resolve_periodic_filings(ticker, filing_type, accession_number)
    if error:
        return _resolve_failure(error, ticker)
    _, primary = filings[0]
    cik_padded, subs = await _get_submissions_for_ticker(ticker)

    async def release_text() -> dict:
        # The newest results-of-operations 8-K, as in get_guidance_history.
        picks = _earnings_release_picks(subs, int(cik_padded), 1) if cik_padded and subs else []
        if not picks:
            return {"status": "RELEASE_NOT_RESOLVED", "url": None, "filingDate": None, "accessionNumber": None, "text": None}
        read = await _read_release_text(int(cik_padded), picks[0]["accessionNumber"], picks[0]["primaryUrl"])
        return {**read, "filingDate": picks[0]["filingDate"], "accessionNumber": picks[0]["accessionNumber"]}

    async def companyfacts() -> dict | None:
        return await _edgar_get_company_facts(cik_padded) if cik_padded else None

    loaded, matches, facts, release = await asyncio.gather(
        _ixbrl_source("primary", primary),
        _filing_text_matches(ticker, primary, _DRIVER_SEARCH_TERMS, 30, 700),
        companyfacts(),
        release_text(),
        return_exceptions=True,
    )
    loaded = None if isinstance(loaded, BaseException) else loaded
    matches = [] if isinstance(matches, BaseException) else matches
    facts = None if isinstance(facts, BaseException) else facts
    if isinstance(release, BaseException):
        release = {"status": "RELEASE_NOT_RESOLVED", "url": None, "filingDate": None, "accessionNumber": None, "text": None}
    statements = []
    if isinstance(release.get("text"), str):
        statements.append({"contextText": release["text"], "sectionHeading": None, "documentUrl": release.get("url"),
                           "filingDate": release.get("filingDate"), "accessionNumber": release.get("accessionNumber"), "source": "EARNINGS_RELEASE"})
    statements += [{"contextText": m.context_text, "sectionHeading": m.section_heading, "documentUrl": m.document_url,
                    "filingDate": m.filing_date, "accessionNumber": m.accession_number, "source": "PERIODIC_FILING"} for m in matches]
    source, truncated = loaded if loaded else (None, False)
    out = _dl.operating_driver_ledger(
        ticker=ticker,
        companyfacts=facts,
        inline_facts=[{"name": f.name, "unit": f.unit, "value": f.value, "periodStart": f.period_start, "periodEnd": f.period_end, "dims": f.dims}
                      for f in source.doc.facts] if source else None,
        filing={"filingType": primary["filingType"], "filingDate": primary["filingDate"], "accessionNumber": primary["accessionNumber"],
                "documentUrl": primary["documentUrl"], "status": "READ" if source else "NOT_READ", "textMatches": len(matches)},
        release={"filingDate": release.get("filingDate"), "accessionNumber": release.get("accessionNumber"), "url": release.get("url"), "status": release["status"]},
        statements=statements,
    )
    warnings = []
    if not source:
        warnings.append({"code": "FILING_TEXT_NOT_AVAILABLE", "message": "The periodic filing could not be read from SEC, so company-specific series were not read; retry.", "severity": "warning"})
    elif truncated:
        warnings.append({"code": "FILING_READ_TRUNCATED", "message": "The filing exceeded the read limit; facts past that point were not parsed.", "severity": "warning"})
    if not facts:
        warnings.append({"code": "COMPANYFACTS_NOT_AVAILABLE", "message": "SEC companyfacts could not be read, so standard driver series were not read; retry.", "severity": "warning"})
    if release["status"] == "NOT_READ":
        warnings.append({"code": "RELEASE_TEXT_NOT_AVAILABLE", "message": f"The earnings release {release.get('url')} could not be read from SEC; retry.", "severity": "warning"})
    elif release["status"] == "RELEASE_NOT_RESOLVED":
        warnings.append({"code": "RELEASE_NOT_RESOLVED", "message": "No SEC earnings release was resolved, so release statements were not read.", "severity": "warning"})
    out["status"] = "PARTIAL" if any(w["code"] not in ("FILING_READ_TRUNCATED", "RELEASE_NOT_RESOLVED") for w in warnings) else "OK"
    out["retryable"] = not source or not facts or release["status"] == "NOT_READ"
    out["warnings"] = warnings
    return json.dumps(out)


def _china_filing_not_found(ticker: str, filing_type: str, source: dict) -> str:
    """The Worker's extractChinaExposure payload for a filing that is not there: no read past the lookup."""
    return json.dumps({
        "ticker": ticker,
        "exposureType": "china_exposure",
        "filingType": filing_type,
        "filingDate": None,
        "accessionNumber": None,
        "documentUrl": None,
        "revenueExposure": {"status": "FILING_NOT_FOUND_TRY_OTHER_TYPE", "value": None, "denominator": None, "valueRatio": None, "valuePct": None, "confidence": "FILING_NOT_FOUND_TRY_OTHER_TYPE", "evidence": []},
        "manufacturingExposure": {"status": "NOT_FOUND", "confidence": "LOW", "evidence": []},
        "entityExposure": {"status": "NOT_FOUND", "entities": [], "confidence": "LOW", "evidence": []},
        "bankExposure": {"status": "NOT_FOUND", "entities": [], "confidence": "LOW", "evidence": []},
        "riskFactorExposure": {"status": "NOT_FOUND", "confidence": "LOW", "evidence": []},
        "overallStatus": "FILING_NOT_FOUND_TRY_OTHER_TYPE",
        "code": "FILING_NOT_FOUND_TRY_OTHER_TYPE",
        "requestedFilingType": _nn(source.get("requestedFilingType"), filing_type),
        "availableFilingTypes": _nn(source.get("availableFilingTypes"), []),
        "suggestedFilingTypes": _nn(source.get("suggestedFilingTypes"), []),
        "warnings": _nn(source.get("warnings"), []),
    })


@yfinance_server.tool(name="extract_china_exposure", description="Extract China exposure with separate revenue and non-revenue classifications; revenue values are decision-grade only when evidence and status support them.", after="compare_peer_valuations")
async def extract_china_exposure(
    ticker: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    accession_number: str | None = None,
    detailLevel: str = "compact",
) -> str:
    failure = _filing_period_failure("extract_china_exposure", period)
    if failure:
        return failure
    risk_terms = list(_CHINA_PRESET["riskTerms"])
    # Warm the shared submissions cache once, then fetch the three
    # independent filing reads together.
    await _warm_sec_submissions(ticker)
    # The filing the Worker reads: a 10-K not filed falls to the latest 20-F, a pinned accession is exact. One with no
    # such filing is FILING_NOT_FOUND_TRY_OTHER_TYPE before any other read (the Worker's resolveSecFiling).
    resolved_filing: dict | None = None
    resolve_error: dict | None = None
    try:
        resolved_filing, resolve_error = await _resolve_sec_filing(ticker, filing_type, accession_number)
    except Exception:
        pass
    if _is_accession_not_found(resolve_error):
        # Not an action 2.13.0 fails closed on a miss: it reads FILING_NOT_FOUND_TRY_OTHER_TYPE, as before.
        resolve_error = await _legacy_accession_miss_payload(ticker, filing_type)
    if resolve_error and resolve_error.get("status") == "FILING_NOT_FOUND_TRY_OTHER_TYPE":
        return _china_filing_not_found(ticker, filing_type, resolve_error)

    async def _no_index() -> str:
        # The Worker reads the index from the resolved filing; one that could not be resolved (no primary document, an
        # XBRL-only one, no submissions) is its error payload, read from as the index (the Worker's {ok: false, ...error}).
        return json.dumps(resolve_error)

    idx_raw, revenue_raw, risk_raw = await asyncio.gather(
        _sec_filing_index_full(
            ticker=ticker,
            filing_type=resolved_filing["filingType"],
            period=period,
            accession_number=resolved_filing["accessionNumber"],
        ) if resolved_filing else (_no_index() if resolve_error else _sec_filing_index_full(
            ticker=ticker, filing_type=filing_type, period=period, accession_number=accession_number)),
        _extract_revenue_exposure(ticker=ticker, exposure_query=_CHINA_PRESET["region"], filing_type=filing_type, period=period, accession_number=accession_number),
        _extract_risk_factor_mentions(ticker=ticker, terms=risk_terms, filing_type=filing_type, period=period, accession_number=accession_number),
    )
    idx = _safe_json_loads(idx_raw)
    revenue = _safe_json_loads(revenue_raw)
    if revenue.get("status") == "FILING_NOT_FOUND_TRY_OTHER_TYPE":
        return _china_filing_not_found(ticker, filing_type, revenue)
    revenue_status_text = str(_nn(revenue.get("status"), "NOT_FOUND"))
    revenue_warnings = revenue["warnings"] if isinstance(revenue.get("warnings"), list) else []
    explicit_revenue_status = _explicit_revenue_limitation_status(revenue_status_text, revenue_warnings)
    revenue_status = "FOUND" if revenue_status_text == "FOUND_REVENUE_EXPOSURE" else revenue_status_text
    # A revenue read that failed (SEC_READ_FAILED, SEC_LOOKUP_UNAVAILABLE) is not a missing exposure (2.5.18).
    revenue_read_failed = revenue_status_text == "PROVIDER_ERROR"

    index = idx.get("index") if isinstance(idx.get("index"), dict) else {}
    sections = index.get("sections") if isinstance(index.get("sections"), list) else []
    tables = index.get("tables") if isinstance(index.get("tables"), list) else []

    def _collect(term_list: list[str], evidence_kind: str) -> tuple[list[dict], int]:
        found = []
        rejected = 0

        def _scoped_excerpt(value: object, term: str) -> str:
            if evidence_kind == "entity":
                return _contextual_entity_excerpt(value, term)
            excerpt = _readable_exposure_excerpt(value, [term], min_words=3)
            if evidence_kind == "bank" and excerpt and not _EXPOSURE_RELATIONSHIP_RE.search(excerpt):
                return ""
            return excerpt

        for sec in sections:
            if not isinstance(sec, dict):
                continue
            heading = str(sec.get("heading") or "")
            low = heading.lower()
            for term in term_list:
                if term.lower() in low:
                    excerpt = _scoped_excerpt(heading, term)
                    if not excerpt:
                        rejected += 1
                        continue
                    found.append({
                        "source": "section",
                        "term": term,
                        "sectionHeading": heading,
                        "excerpt": excerpt,
                        "excerptAvailable": bool(excerpt),
                    })
        for tbl in tables:
            if not isinstance(tbl, dict):
                continue
            hay_source = " ".join([str(tbl.get("title") or ""), *[str(x) for x in (tbl.get("rowLabels") or [])]])
            hay = hay_source.lower()
            for term in term_list:
                if term.lower() in hay:
                    excerpt = _scoped_excerpt(hay_source, term)
                    if not excerpt:
                        rejected += 1
                        continue
                    found.append({
                        "source": "table",
                        "term": term,
                        "tableTitle": tbl.get("title"),
                        "sourceTableId": tbl.get("tableId"),
                        "sectionId": tbl.get("sectionId"),
                        "excerpt": excerpt,
                        "excerptAvailable": bool(excerpt),
                    })
        return found, rejected

    entity_terms = list(_CHINA_PRESET["entityTerms"])
    bank_terms = list(_CHINA_PRESET["bankTerms"])
    # "manufactured by outsourcing partners located primarily in China mainland" (AAPL) names manufacturing without the
    # word "manufacturing" (2.25.4 audit, F39). New phrases go after the old ones, so an old term still reports first.
    manuf_terms = list(_CHINA_PRESET["manufacturingTerms"])

    entity_evidence, entity_rejected = _collect(entity_terms, "entity")
    bank_evidence, bank_rejected = _collect(bank_terms, "bank")
    manu_evidence, manu_rejected = _collect(manuf_terms, "manufacturing")
    risk_mentions = _safe_json_loads(risk_raw)
    raw_risk_evidence = risk_mentions.get("matches") if isinstance(risk_mentions.get("matches"), list) else []
    risk_evidence = []
    for ev in raw_risk_evidence:
        if isinstance(ev, dict):
            term = str(ev.get("term") or _CHINA_PRESET["region"])
            excerpt = _readable_exposure_excerpt(
                ev.get("excerpt") or ev.get("context") or "",
                [term],
            )
            if excerpt:
                shaped = dict(ev)
                shaped["excerpt"] = excerpt
                shaped["excerptAvailable"] = True
                risk_evidence.append(shaped)

    if not manu_evidence:
        # Headings and table labels rarely name manufacturing locations; the filing text does (as extract_exposure's
        # operational scan reads it).
        china_text = _safe_json_loads(await search_sec_filing_text(
            ticker=ticker, search_terms=[_CHINA_PRESET["region"]], filing_type=filing_type, accession_number=accession_number,
            context_chars=800, return_tables=False,
        ))
        for m in china_text["matches"] if isinstance(china_text.get("matches"), list) else []:
            if not isinstance(m, dict):
                continue
            excerpt = _readable_exposure_excerpt(_nn(m.get("contextText"), ""), [_CHINA_PRESET["region"]], max_len=240, min_words=3)
            term = next((t for t in manuf_terms if t in excerpt.lower()), None) if excerpt else None
            if not excerpt or not term:
                continue
            manu_evidence.append({"source": "text", "term": term, "sectionHeading": m.get("sectionHeading"), "excerpt": excerpt, "excerptAvailable": True})
            if len(manu_evidence) >= 5:
                break

    non_revenue_found = bool(entity_evidence or bank_evidence or manu_evidence or risk_evidence)
    revenue_matches = revenue["matches"] if isinstance(revenue.get("matches"), list) else []
    first_revenue = revenue_matches[0] if revenue_matches and isinstance(revenue_matches[0], dict) else {}
    if revenue_status == "FOUND":
        overall = "FOUND_REVENUE_EXPOSURE"
    elif non_revenue_found:
        overall = "FOUND_NON_REVENUE_EXPOSURE"
    elif explicit_revenue_status:
        overall = explicit_revenue_status
    elif revenue_read_failed:
        overall = "PROVIDER_ERROR"
    elif revenue_status_text == "NOT_DISCLOSED":
        overall = "NOT_DISCLOSED"
    elif revenue_status_text == "CONFLICTING":
        overall = "CONFLICTING"
    else:
        overall = "NOT_FOUND"

    out = {
        "ticker": ticker,
        "exposureType": "china_exposure",
        "filingType": _nn(idx.get("filingType"), filing_type),
        "filingDate": idx.get("filingDate"),
        "accessionNumber": idx.get("accessionNumber"),
        "documentUrl": idx.get("documentUrl"),
        "revenueExposure": {
            "status": revenue_status,
            "value": first_revenue.get("value"),
            "denominator": first_revenue.get("denominator"),
            "valueRatio": first_revenue.get("valueRatio"),
            "valuePct": first_revenue.get("valuePct"),
            "confidence": "HIGH" if revenue_status == "FOUND" else (explicit_revenue_status or ("NOT_DISCLOSED" if revenue_status_text == "NOT_DISCLOSED" else "LOW")),
            "evidence": _nn(first_revenue.get("evidence"), []),
        },
        "manufacturingExposure": {"status": "FOUND" if manu_evidence else "NOT_FOUND", "confidence": "MEDIUM", "evidence": manu_evidence, "rejectedNoiseCount": manu_rejected},
        "entityExposure": {"status": "FOUND" if entity_evidence else "NOT_FOUND", "entities": entity_terms if entity_evidence else [], "confidence": "MEDIUM", "evidence": entity_evidence, "rejectedNoiseCount": entity_rejected},
        "bankExposure": {"status": "FOUND" if bank_evidence else "NOT_FOUND", "entities": bank_terms if bank_evidence else [], "confidence": "MEDIUM", "evidence": bank_evidence, "rejectedNoiseCount": bank_rejected},
        "riskFactorExposure": {"status": "FOUND" if risk_evidence else "NOT_FOUND", "confidence": "MEDIUM", "evidence": risk_evidence},
        "overallStatus": overall,
        "code": explicit_revenue_status or ((revenue["code"] if isinstance(revenue.get("code"), str) else "PROVIDER_ERROR") if revenue_read_failed else None),
        "warnings": revenue_warnings,
    }
    if str(detailLevel).lower() == "raw":
        out["rawContext"] = {"filingIndex": idx}
    return json.dumps(out)


@yfinance_server.tool(
    name="extract_exposure",
    description="Extract multi-dimensional SEC exposure for a geographic region or named entity/topic. Returns revenue, operational, named-entity, and risk evidence with explicit non-decision-grade statuses when parser/provider limits prevent a value.",
    after="extract_china_exposure",
)
async def extract_exposure(
    ticker: str,
    topic: str,
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    include_risk_factors: bool = True,
) -> str:
    failure = _filing_period_failure("extract_exposure", period)
    if failure:
        return failure
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("extract_exposure", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if not topic or not topic.strip():
        return json.dumps({"ticker": ticker, "topic": topic, "overallStatus": "NOT_FOUND", "warnings": [{"code": "INPUT_VALIDATION_ERROR", "message": "topic is required"}]})

    topic_lower = topic.strip().lower()

    EXPOSURE_SYNONYMS = {
        "china": ["china", "greaterchinese", "greaterchina", "prc", "hongkong", "taiwan"],
        "greater china": ["greaterchina", "greaterchinese", "china", "hongkong", "taiwan"],
        "europe": ["europe", "emea", "europeanunion"],
        "japan": ["japan"],
        "americas": ["americas", "northamerica", "unitedstates", "us"],
        "russia": ["russia", "russianfederation"],
        "india": ["india"],
        "rest of asia": ["restofasia", "asiapacific", "asiaxjapan"],
    }
    CHINA_NAMED_ENTITIES = list(_CHINA_PRESET["namedEntities"])
    # The F39 manufacturing phrases apply to every region, not only the China preset (2.26.5, F62).
    OPERATIONAL_TERMS = ["manufacturing", "assembly", "supply chain", "factory", "production facility", "manufactured", "outsourcing partner", "contract manufacturer"]

    is_china = topic_lower in ("china", "greater china")
    synonym_entry = EXPOSURE_SYNONYMS.get(topic_lower)
    region_label = "Greater China" if topic_lower == "china" else topic

    warnings: list[dict] = []

    async def _skipped() -> None:
        return None

    # The filing reads below are independent. Warm the shared submissions
    # cache once, then run them together; each keeps its own error handling.
    await _warm_sec_submissions(ticker)
    idx_raw, geo_raw, ops_raw, ent_raw, risk_raw = await asyncio.gather(
        _sec_filing_index_full(ticker=ticker, filing_type=filing_type, period=period),
        extract_geographic_revenue(ticker=ticker, region=region_label, filing_type=filing_type, period=period, detailLevel="compact"),
        search_sec_filing_text(ticker=ticker, search_terms=[topic_lower], filing_type=filing_type, context_chars=600, return_tables=False),
        search_sec_filing_text(ticker=ticker, search_terms=CHINA_NAMED_ENTITIES[:3], filing_type=filing_type, context_chars=400, return_tables=False)
        if is_china else _skipped(),
        extract_risk_factor_mentions(ticker=ticker, terms=[topic_lower], filing_type=filing_type, period=period, detailLevel="compact")
        if include_risk_factors else _skipped(),
        return_exceptions=True,
    )
    for result in (idx_raw, geo_raw, ops_raw, ent_raw, risk_raw):
        if isinstance(result, BaseException) and not isinstance(result, Exception):
            raise result
    if isinstance(idx_raw, Exception):
        raise idx_raw

    # Filing index for metadata
    idx = _safe_json_loads(idx_raw)

    filing_date = idx.get("filingDate")
    accession_number = idx.get("accessionNumber")
    document_url = idx.get("documentUrl")

    # Revenue extraction
    if isinstance(geo_raw, Exception):
        warnings.append({"code": "REVENUE_EXTRACTION_ERROR", "message": str(geo_raw), "severity": "warning"})
        geo = {}
    else:
        geo = _safe_json_loads(geo_raw)

    geo_value = geo.get("value")
    geo_denominator = geo.get("denominator")
    geo_conf = str(geo.get("confidence") or "LOW").upper()
    geo_method = str(geo.get("extractionMethod") or "NONE").upper()
    geo_evidence = geo.get("evidence") or {}
    if isinstance(geo_evidence, dict):
        pass
    else:
        geo_evidence = {}

    # A named limitation (EXTRACTION_FAILED, TABLE_NOT_PARSED, ...) is the revenue status, as in the Worker. A failed
    # table parse needs a table to have failed on: when the region is only in the filing's text and no indexed table
    # names it, the status is NOT_FOUND (2.25.4 audit, F35: AAPL India).
    geo_warnings = [w for w in geo.get("warnings") or [] if isinstance(w, dict)]
    geo_warning_codes = {w.get("code") for w in geo_warnings}
    explicit_revenue_status = _explicit_revenue_limitation_status(geo.get("status") or geo.get("code"), geo_warnings)
    if (explicit_revenue_status in ("EXTRACTION_FAILED", "TABLE_NOT_PARSED") and "TABLE_NOT_PARSED" in geo_warning_codes
            and "FILING_READ_TRUNCATED" not in geo_warning_codes and _index_tables_name_region(idx, topic_lower) is False):
        explicit_revenue_status = None
        # The parsed tables simply have no row for the region, so no table failed to parse (2.26.5, F62).
        geo_warnings = [w for w in geo_warnings if w.get("code") != "TABLE_NOT_PARSED"]

    if geo_value is not None:
        rev_status = "FOUND"
    elif explicit_revenue_status:
        rev_status = explicit_revenue_status
    elif geo_conf == "NOT_DISCLOSED":
        rev_status = "NOT_DISCLOSED"
    else:
        rev_status = "NOT_FOUND"

    revenue_exposure = {
        "status": rev_status,
        "value": geo_value,
        "denominator": geo_denominator,
        "valuePct": geo.get("valuePct") if geo_denominator is not None else None,
        "valueRatio": geo.get("valueRatio") if geo_denominator is not None else None,
        "unit": geo.get("unit", "USD"),
        "region": geo_evidence.get("sectionHeading") or region_label,
        "period": geo.get("period"),
        "extractionMethod": geo_method,
        "confidence": geo_conf if geo_value is not None else (explicit_revenue_status or "LOW"),
        "code": explicit_revenue_status,
        "evidence": {
            "sectionHeading": geo_evidence.get("sectionHeading"),
            "sourceRows": geo_evidence.get("sourceRows") if isinstance(geo_evidence.get("sourceRows"), list) else [],
            "sourceColumns": geo_evidence.get("sourceColumns") if isinstance(geo_evidence.get("sourceColumns"), list) else [],
        },
    }

    # Operational scan via existing search
    ops_matches = _safe_json_loads(ops_raw).get("matches") or []

    ops_evidence: list[dict] = []
    found_op_terms: set[str] = set()
    rejected_ops_noise = 0
    for m in ops_matches:
        if not isinstance(m, dict):
            continue
        raw_context = m.get("contextText") or m.get("context") or ""
        context_excerpt = _readable_exposure_excerpt(raw_context, [topic_lower], max_len=200)
        if not context_excerpt:
            rejected_ops_noise += 1
            continue
        context_text = context_excerpt.lower()
        for op_term in OPERATIONAL_TERMS:
            if op_term in context_text:
                found_op_terms.add(op_term)
                if len(ops_evidence) < 5:
                    ops_evidence.append({
                        "term": op_term,
                        "excerpt": context_excerpt,
                        "section": str(m.get("sectionHeading") or ""),
                    })
                break
        if len(ops_evidence) >= 5:
            break

    operational_exposure = {
        "status": "FOUND" if ops_evidence else "NOT_FOUND",
        "terms": list(found_op_terms),
        "evidence": ops_evidence,
        "rejectedNoiseCount": rejected_ops_noise,
    }

    # Entity scan (China only)
    if is_china:
        ent_matches = _safe_json_loads(ent_raw).get("matches") or []
        found_entities: set[str] = set()
        ent_evidence: list[dict] = []
        rejected_entity_noise = 0
        for m in ent_matches:
            if not isinstance(m, dict):
                continue
            term_low = str(m.get("term") or "").lower()
            excerpt = _contextual_entity_excerpt(
                m.get("contextText") or m.get("context") or "",
                term_low,
                max_len=200,
            )
            if not term_low or not excerpt:
                rejected_entity_noise += 1
                continue
            if term_low and term_low not in found_entities:
                found_entities.add(term_low)
                if len(ent_evidence) < 5:
                    ent_evidence.append({
                        "entity": str(m.get("term") or ""),
                        "excerpt": excerpt,
                        "section": str(m.get("sectionHeading") or ""),
                    })
            if len(ent_evidence) >= 5:
                break
        entity_exposure = {
            "status": "FOUND" if ent_evidence else "NOT_FOUND",
            "entities": list(found_entities),
            "evidence": ent_evidence,
            "rejectedNoiseCount": rejected_entity_noise,
        }
    else:
        entity_exposure = {"status": "NOT_FOUND", "entities": [], "evidence": [], "rejectedNoiseCount": 0}

    # Risk factor scan
    if include_risk_factors:
        risk_matches = _safe_json_loads(risk_raw).get("matches") or []
        risk_evidence = []
        for m in risk_matches[:5]:
            if not isinstance(m, dict):
                continue
            excerpt = _readable_exposure_excerpt(
                m.get("excerpt") or m.get("context") or m.get("contextText") or "",
                [topic_lower],
                max_len=200,
            )
            if excerpt:
                risk_evidence.append({
                    "excerpt": excerpt,
                    "section": str(m.get("sectionHeading") or "Risk Factors"),
                })
        risk_factor_exposure = {
            "status": "FOUND" if risk_evidence else "NOT_FOUND",
            "mentionCount": len(risk_matches),
            "evidence": risk_evidence,
        }
    else:
        risk_factor_exposure = {"status": "NOT_FOUND", "mentionCount": 0, "evidence": []}

    # Overall status
    non_revenue_found = bool(ops_evidence or entity_exposure.get("evidence") or risk_factor_exposure.get("evidence"))
    if rev_status == "FOUND":
        overall_status = "FOUND_REVENUE_EXPOSURE"
    elif non_revenue_found:
        overall_status = "FOUND_NON_REVENUE_EXPOSURE"
    elif explicit_revenue_status:
        overall_status = explicit_revenue_status
    elif rev_status == "NOT_DISCLOSED":
        overall_status = "NOT_DISCLOSED"
    else:
        overall_status = "NOT_FOUND"

    return json.dumps({
        "ticker": ticker,
        "topic": topic_lower,
        "filingType": idx.get("filingType", filing_type),
        "filingDate": filing_date,
        "accessionNumber": accession_number,
        "documentUrl": document_url,
        "revenueExposure": revenue_exposure,
        "operationalExposure": operational_exposure,
        "entityExposure": entity_exposure,
        "riskFactorExposure": risk_factor_exposure,
        "overallStatus": overall_status,
        "code": explicit_revenue_status,
        # The revenue extraction's warnings, after this tool's own, as in the Worker.
        "warnings": [*warnings, *geo_warnings],
    })


def _index_tables_name_region(index_payload: dict, region: str) -> bool | None:
    """The Worker's indexTablesNameRegion: whether any indexed table names the region; None with no tables to read."""
    index = index_payload.get("index") if isinstance(index_payload.get("index"), dict) else None
    tables = index.get("tables") if index and isinstance(index.get("tables"), list) else None
    if not tables:
        return None
    def words(v):
        return " ".join(str(x) for x in v) if isinstance(v, list) else ""
    return any(isinstance(t, dict) and _text_contains_geo_region(
        f"{t.get('title') or ''} {words(t.get('headers'))} {words(t.get('rowLabels'))}", region) for t in tables)


@dataclass
class _SecIndexContext:
    ticker: str
    filing_type: str
    period: str
    accession_number: str | None
    detail: str
    routed_params: dict
    warnings: list
    shape_evidence: Callable[[dict], dict]
    result: Callable[..., str]
    missing_param: Callable[[str], str]
    accession_miss: Callable[[], Awaitable[str | None]]


def _sec_index_evidence_required(status: str, evidence: list, warnings: list) -> str:
    if status == "ANSWERED" and not evidence:
        warnings.append({"code": "EVIDENCE_REQUIRED", "message": "ANSWERED responses require evidence."})
        return "NOT_FOUND"
    return status


async def _sec_index_geographic_revenue_share(ctx: _SecIndexContext) -> str:
    region = str(ctx.routed_params.get("region") or "").strip()
    if not region:
        return ctx.missing_param("region")
    geo = _safe_json_loads(await extract_geographic_revenue(
        ticker=ctx.ticker,
        region=region,
        filing_type=ctx.filing_type,
        period=ctx.period,
        accession_number=ctx.accession_number,
        detailLevel=ctx.detail,
    ))
    # An explicit accession that is not among the company's recent filings fails closed here too (2.15.0, R4-ACC-QUERY).
    if geo.get("ok") is False and _is_accession_not_found(geo.get("error")):
        return _accession_not_found_envelope(geo["error"])
    evidence_obj = geo.get("evidence") if isinstance(geo.get("evidence"), dict) else {}
    evidence = [ctx.shape_evidence(evidence_obj)] if evidence_obj else []
    status = "ANSWERED" if geo.get("value") is not None else _as_status(geo)
    status = _sec_index_evidence_required(status, evidence, ctx.warnings)
    answer = {
        "region": region,
        "value": geo.get("value"),
        "denominator": geo.get("denominator"),
        "valueRatio": geo.get("valueRatio"),
        "valuePct": geo.get("valuePct"),
        "unit": geo.get("unit", "USD"),
    }
    confidence = str(_nn(geo.get("confidence"), "LOW" if status == "ANSWERED" else "NOT_DISCLOSED"))
    if status == "NOT_FOUND":
        confidence = "NOT_DISCLOSED" if str(geo.get("confidence") or "").upper() == "NOT_DISCLOSED" else "LOW"
    return ctx.result(status, answer, confidence, evidence, ctx.warnings)


async def _sec_index_revenue_exposure(ctx: _SecIndexContext) -> str:
    exposure_query = str(ctx.routed_params.get("exposure_query") or "").strip()
    if not exposure_query:
        return ctx.missing_param("exposure_query")
    rex = _safe_json_loads(await _extract_revenue_exposure(
        ticker=ctx.ticker,
        exposure_query=exposure_query,
        filing_type=ctx.filing_type,
        period=ctx.period,
        detailLevel=ctx.detail,
        accession_number=ctx.accession_number,
    ))
    matches = rex.get("matches") if isinstance(rex.get("matches"), list) else []
    first = matches[0] if matches and isinstance(matches[0], dict) else {}
    evidence_obj = first.get("evidence") if isinstance(first.get("evidence"), dict) else {}
    evidence = [ctx.shape_evidence(evidence_obj)] if evidence_obj else []
    status = "ANSWERED" if bool(matches) and str(rex.get("status")) == "FOUND_REVENUE_EXPOSURE" else str(rex.get("status") or "NOT_FOUND")
    status = _sec_index_evidence_required(status, evidence, ctx.warnings)
    answer = {
        "exposureQuery": exposure_query,
        "value": first.get("value"),
        "denominator": first.get("denominator"),
        "valueRatio": first.get("valueRatio"),
        "valuePct": first.get("valuePct"),
        "period": first.get("period"),
    }
    confidence = str(_nn(first.get("confidence"), "LOW" if status == "ANSWERED" else "NOT_DISCLOSED"))
    return ctx.result(status, answer, confidence, evidence, ctx.warnings)


async def _sec_index_china_exposure(ctx: _SecIndexContext) -> str:
    china = _safe_json_loads(await extract_china_exposure(
        ticker=ctx.ticker,
        filing_type=ctx.filing_type,
        period=ctx.period,
        accession_number=ctx.accession_number,
        detailLevel=ctx.detail,
    ))
    overall = str(china.get("overallStatus") or "NOT_FOUND")
    answer = {
        "revenueExposure": china.get("revenueExposure"),
        "manufacturingExposure": china.get("manufacturingExposure"),
        "entityExposure": china.get("entityExposure"),
        "bankExposure": china.get("bankExposure"),
        "riskFactorExposure": china.get("riskFactorExposure"),
        "overallStatus": overall,
    }
    evidence: list[dict] = []
    for key in ("revenueExposure", "manufacturingExposure", "entityExposure", "bankExposure", "riskFactorExposure"):
        block = china.get(key)
        if not isinstance(block, dict):
            continue
        ev = block.get("evidence")
        if isinstance(ev, dict):
            evidence.append(ctx.shape_evidence(ev))
        elif isinstance(ev, list):
            for item in ev:
                if isinstance(item, dict):
                    evidence.append(ctx.shape_evidence(item))
    status = "ANSWERED" if overall in {"FOUND_REVENUE_EXPOSURE", "FOUND_NON_REVENUE_EXPOSURE"} else overall
    status = _sec_index_evidence_required(status, evidence, ctx.warnings)
    confidence = "HIGH" if status == "ANSWERED" else ("NOT_DISCLOSED" if overall == "NOT_DISCLOSED" else "LOW")
    return ctx.result(status, answer, confidence, evidence, ctx.warnings)


async def _sec_index_risk_factor_mentions(ctx: _SecIndexContext) -> str:
    terms = ctx.routed_params.get("terms")
    terms_list = [str(t) for t in terms] if isinstance(terms, list) else []
    if not terms_list:
        return ctx.missing_param("terms")
    risk_miss = await ctx.accession_miss()
    if risk_miss:
        return risk_miss
    risk = _safe_json_loads(await _extract_risk_factor_mentions(
        ticker=ctx.ticker,
        terms=terms_list,
        filing_type=ctx.filing_type,
        period=ctx.period,
        detailLevel=ctx.detail,
        accession_number=ctx.accession_number,
    ))
    matches = [m for m in (risk.get("matches") if isinstance(risk.get("matches"), list) else []) if isinstance(m, dict)]
    evidence = [ctx.shape_evidence(m.get("evidence")) for m in matches if isinstance(m.get("evidence"), dict)]
    status = "ANSWERED" if matches else str(risk.get("status") or "NOT_FOUND")
    status = _sec_index_evidence_required(status, evidence, ctx.warnings)
    return ctx.result(
        status,
        {"terms": terms_list, "matches": matches},
        "MEDIUM" if status == "ANSWERED" else "LOW",
        evidence,
        ctx.warnings,
    )


async def _sec_index_customer_concentration(ctx: _SecIndexContext) -> str:
    customer_label = str(ctx.routed_params.get("customer_label") or "").strip()
    cust_miss = await ctx.accession_miss()
    if cust_miss:
        return cust_miss
    cust = _safe_json_loads(await _extract_customer_concentration(
        ticker=ctx.ticker,
        filing_type=ctx.filing_type,
        period=ctx.period,
        detailLevel=ctx.detail,
        accession_number=ctx.accession_number,
    ))
    customers = [c for c in (cust.get("customers") if isinstance(cust.get("customers"), list) else []) if isinstance(c, dict)]
    if customer_label:
        customers = [c for c in customers if str(c.get("label") or "").lower() == customer_label.lower()]
    evidence = [ctx.shape_evidence(c.get("evidence")) for c in customers if isinstance(c.get("evidence"), dict)]
    status = "ANSWERED" if customers else str(cust.get("status") or "NOT_FOUND")
    status = _sec_index_evidence_required(status, evidence, ctx.warnings)
    return ctx.result(
        status,
        {"customerLabel": customer_label or None, "customers": customers},
        "HIGH" if status == "ANSWERED" else ("NOT_DISCLOSED" if status == "NOT_DISCLOSED" else "LOW"),
        evidence,
        ctx.warnings,
    )


async def _sec_index_total_revenue(ctx: _SecIndexContext) -> str:
    total_miss = await ctx.accession_miss()
    if total_miss:
        return total_miss
    total = _safe_json_loads(await _extract_total_revenue(
        ticker=ctx.ticker,
        filing_type=ctx.filing_type,
        period=ctx.period,
        accession_number=ctx.accession_number,
    ))
    evidence_obj = total.get("evidence") if isinstance(total.get("evidence"), dict) else {}
    evidence = [ctx.shape_evidence(evidence_obj)] if evidence_obj else []
    status = "ANSWERED" if total.get("value") is not None else str(total.get("status") or "NOT_FOUND")
    status = _sec_index_evidence_required(status, evidence, ctx.warnings)
    return ctx.result(
        status,
        {"value": total.get("value"), "period": total.get("period"), "unit": "USD"},
        str(_nn(total.get("confidence"), "LOW" if status == "ANSWERED" else "NOT_DISCLOSED")),
        evidence,
        ctx.warnings,
    )


async def _sec_index_segment_revenue(ctx: _SecIndexContext) -> str:
    segment_name = str(ctx.routed_params.get("segment") or "").strip()
    seg_miss = await ctx.accession_miss()
    if seg_miss:
        return seg_miss
    seg = _safe_json_loads(await _extract_segment_revenue(
        ticker=ctx.ticker,
        filing_type=ctx.filing_type,
        period=ctx.period,
        detailLevel=ctx.detail,
        accession_number=ctx.accession_number,
    ))
    segments = [s for s in (seg.get("segments") if isinstance(seg.get("segments"), list) else []) if isinstance(s, dict)]
    if segment_name:
        segments = [s for s in segments if str(s.get("label") or "").lower() == segment_name.lower()]
    evidence = [ctx.shape_evidence(s.get("evidence")) for s in segments if isinstance(s.get("evidence"), dict)]
    status = "ANSWERED" if segments else ("NOT_FOUND" if segment_name else str(seg.get("status") or "NOT_FOUND"))
    status = _sec_index_evidence_required(status, evidence, ctx.warnings)
    # The table fallback's rows carry their own confidence (MEDIUM, or LOW when the scale was assumed), as the Worker reads it.
    segment_confidence = str(segments[0].get("confidence") or "LOW") if segments else None
    confidence = (segment_confidence or "LOW") if status == "ANSWERED" else ("NOT_DISCLOSED" if status == "NOT_DISCLOSED" else "LOW")
    # The extractor's warnings (POSSIBLE_20F_FILER, ...) go with the answer, as in the Worker (2.26.4, F23).
    if isinstance(seg.get("warnings"), list):
        ctx.warnings.extend(w for w in seg["warnings"] if isinstance(w, dict))
    return ctx.result(
        status,
        {"segment": segment_name or None, "segments": segments},
        confidence,
        evidence,
        ctx.warnings,
    )


@yfinance_server.tool(
    name="query_sec_filing_index",
    description="Deterministically route supported SEC filing index query types to extractor tools.",
    after="extract_exposure",
)
async def query_sec_filing_index(
    ticker: str,
    # Required, as on the Worker: an omitted query type must not silently
    # become geographic_revenue_share.
    query_type: Literal[
        "geographic_revenue_share",
        "revenue_exposure",
        "china_exposure",
        "risk_factor_mentions",
        "customer_concentration",
        "total_revenue",
        "segment_revenue",
    ],
    filing_type: str = "10-K",
    period: _FilingPeriodParam = "latest",
    accession_number: str | None = None,
    params: dict | None = None,
    return_evidence: bool = True,
    detailLevel: str = "compact",
) -> str:
    failure = _filing_period_failure("query_sec_filing_index", period)
    if failure:
        return failure
    err = _validate_ticker(ticker)
    if err:
        return _mcp_failure("query_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, err)
    if accession_number:
        acc_err = _validate_accession(accession_number)
        if acc_err:
            return _mcp_failure("query_sec_filing_index", ErrorCode.INPUT_VALIDATION_ERROR, acc_err)

    allowed_detail = {"compact", "evidence", "raw"}
    detail = str(detailLevel or "compact").lower()
    if detail not in allowed_detail:
        return json.dumps({
            "status": "INPUT_VALIDATION_ERROR",
            "queryType": query_type,
            "ticker": ticker,
            "filingType": filing_type,
            "period": period,
            "answer": None,
            "confidence": "NOT_DISCLOSED",
            "evidence": [],
            "warnings": [{"code": "INPUT_VALIDATION_ERROR", "message": "detailLevel must be one of: compact, evidence, raw"}],
        })

    query = str(query_type or "").strip()
    routed_params = params if isinstance(params, dict) else {}

    def _shape_evidence(ev: dict) -> dict:
        shaped = {
            "filingDate": ev.get("filingDate"),
            "acceptedAt": ev.get("acceptedAt"),
            "accessionNumber": ev.get("accessionNumber"),
            "documentUrl": ev.get("documentUrl"),
            "sectionHeading": ev.get("sectionHeading"),
            "tableTitle": ev.get("tableTitle"),
            "sourceTableId": ev.get("sourceTableId"),
        }
        if detail in {"evidence", "raw"}:
            shaped["sourceRows"] = ev.get("sourceRows") if isinstance(ev.get("sourceRows"), list) else []
            shaped["sourceColumns"] = ev.get("sourceColumns") if isinstance(ev.get("sourceColumns"), list) else []
            if ev.get("excerpt") is not None:
                shaped["excerpt"] = ev.get("excerpt")
        return shaped

    def _result(status: str, answer: dict | None, confidence: str, evidence_items: list[dict] | None = None, warnings: list[dict] | None = None) -> str:
        return json.dumps({
            "status": status,
            "queryType": query,
            "ticker": ticker,
            "filingType": filing_type,
            "period": period,
            "answer": answer,
            "confidence": confidence,
            "evidence": evidence_items if return_evidence else [],
            "warnings": warnings or [],
        })

    def _missing_param(name: str) -> str:
        return _result(
            "INPUT_VALIDATION_ERROR",
            None,
            "NOT_DISCLOSED",
            [],
            [{"code": "INPUT_VALIDATION_ERROR", "message": f"Missing required params.{name} for query_type={query}"}],
        )

    supported = {
        "geographic_revenue_share",
        "revenue_exposure",
        "china_exposure",
        "risk_factor_mentions",
        "customer_concentration",
        "total_revenue",
        "segment_revenue",
    }
    if query not in supported:
        return _mcp_failure(
            "query_sec_filing_index",
            "UNSUPPORTED_QUERY_TYPE",
            f"Unsupported query type '{query}'. Supported types are: {', '.join(sorted(supported))}",
            meta_extra={
                "supportedQueryTypes": list(supported),
                "error_extra": {
                    "supportedQueryTypes": list(supported)
                }
            }
        )

    warnings: list[dict] = []

    # An explicit accession that is not among the company's filings fails closed for the pinned reads too, as
    # geographic_revenue_share does; checked after each query's own params (2.24.0, F21).
    async def _accession_miss() -> str | None:
        if accession_number is None or not str(accession_number).strip():
            return None
        _resolved, resolve_error = await _resolve_sec_filing(ticker, filing_type, str(accession_number).strip())
        return _accession_not_found_envelope(resolve_error) if _resolved is None and _is_accession_not_found(resolve_error) else None

    ctx = _SecIndexContext(
        ticker=ticker,
        filing_type=filing_type,
        period=period,
        accession_number=accession_number,
        detail=detail,
        routed_params=routed_params,
        warnings=warnings,
        shape_evidence=_shape_evidence,
        result=_result,
        missing_param=_missing_param,
        accession_miss=_accession_miss,
    )
    if query == "geographic_revenue_share":
        return await _sec_index_geographic_revenue_share(ctx)
    if query == "revenue_exposure":
        return await _sec_index_revenue_exposure(ctx)
    if query == "china_exposure":
        return await _sec_index_china_exposure(ctx)
    if query == "risk_factor_mentions":
        return await _sec_index_risk_factor_mentions(ctx)
    if query == "customer_concentration":
        return await _sec_index_customer_concentration(ctx)
    if query == "total_revenue":
        return await _sec_index_total_revenue(ctx)
    return await _sec_index_segment_revenue(ctx)
