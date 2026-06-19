"""EdgarTools-backed SEC structured facts sidecar.

This module is deliberately independent from the FastMCP server.  The
Cloudflare Worker remains the public MCP endpoint and calls this sidecar only
when EDGAR_FACTS_URL is configured.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route


TTL_SECONDS = 24 * 3600
REVENUE_CONCEPTS = (
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
)
GEOGRAPHY_HINTS = ("geograph", "country", "region", "area", "segment")
PRODUCT_HINTS = ("product", "service", "segment")
TOTAL_LABELS = ("total", "net sales", "net revenue", "revenue")
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_LAST_ERROR_CODE: str | None = None


@dataclass(frozen=True)
class ExposureRequest:
    ticker: str
    topic: str
    filing_type: str = "10-K"
    period: str = "latest"
    accession_number: str | None = None
    detail_level: str = "compact"


def _now_iso() -> str:
    return _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _cache_get(key: str) -> dict[str, Any] | None:
    entry = _CACHE.get(key)
    if entry is None:
        return None
    stored_at, value = entry
    if time.monotonic() - stored_at >= TTL_SECONDS:
        _CACHE.pop(key, None)
        return None
    return {**value, "cache": {"hit": True, "ttlSeconds": TTL_SECONDS}}


def _cache_set(key: str, value: dict[str, Any]) -> dict[str, Any]:
    _CACHE[key] = (time.monotonic(), value)
    return {**value, "cache": {"hit": False, "ttlSeconds": TTL_SECONDS}}


def _norm(text: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").lower()).strip()


def _contains_topic(row: dict[str, Any], topic: str) -> bool:
    topic_norm = _norm(topic)
    haystack = _norm(" ".join(str(v) for v in row.values()))
    if topic_norm in haystack:
        return True
    if topic_norm == "china":
        return any(token in haystack for token in ("china", "greater china", "cn", "hong kong", "taiwan"))
    return False


def _numeric(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").replace(",", "").replace("$", "").strip()
    if not text:
        return None
    neg = text.startswith("(") and text.endswith(")")
    if neg:
        text = text[1:-1]
    try:
        parsed = float(text)
    except ValueError:
        return None
    return -parsed if neg else parsed


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _fact_rows_to_dicts(rows: Any) -> list[dict[str, Any]]:
    if rows is None:
        return []
    if hasattr(rows, "to_dict"):
        try:
            records = rows.to_dict("records")
            if isinstance(records, list):
                return [r for r in records if isinstance(r, dict)]
        except Exception:
            pass
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def normalize_exposure_facts(
    request: ExposureRequest,
    rows: list[dict[str, Any]],
    filing_meta: dict[str, Any],
) -> dict[str, Any]:
    topic_rows = [row for row in rows if _contains_topic(row, request.topic)]
    total_rows = [
        row for row in rows
        if any(label in _norm(row.get("label") or row.get("concept")) for label in TOTAL_LABELS)
        and not _contains_topic(row, request.topic)
    ]
    topic_row = max(topic_rows, key=lambda r: abs(_numeric(r.get("value")) or 0), default=None)
    total_row = max(total_rows, key=lambda r: abs(_numeric(r.get("value")) or 0), default=None)

    if topic_row is None:
        return {
            "status": "NO_DIMENSIONAL_REVENUE_FACT",
            "code": "NO_DIMENSIONAL_REVENUE_FACT",
            "provider": "edgartools_sidecar",
            "ticker": request.ticker,
            "topic": request.topic,
            "value": None,
            "valuePct": None,
            "warnings": [{
                "code": "NO_DIMENSIONAL_REVENUE_FACT",
                "message": f"No dimensional revenue fact matched topic '{request.topic}'.",
                "severity": "warning",
            }],
            **filing_meta,
        }

    value = _numeric(topic_row.get("value"))
    denominator = _numeric(total_row.get("value")) if total_row else None
    value_pct = (value / denominator * 100) if value is not None and denominator not in (None, 0) else None
    evidence = {
        "provider": "edgartools",
        "concept": _jsonable(topic_row.get("concept")),
        "label": _jsonable(topic_row.get("label")),
        "periodEnd": _jsonable(topic_row.get("period_end") or topic_row.get("period")),
        "units": _jsonable(topic_row.get("units") or topic_row.get("unit")),
        "dimensions": {
            str(k): _jsonable(v) for k, v in topic_row.items()
            if "axis" in str(k).lower() or "dimension" in str(k).lower() or "member" in str(k).lower()
        },
        "denominatorConcept": _jsonable(total_row.get("concept")) if total_row else None,
        "denominatorLabel": _jsonable(total_row.get("label")) if total_row else None,
        "documentUrl": filing_meta.get("documentUrl"),
        "accessionNumber": filing_meta.get("accessionNumber"),
        "filingDate": filing_meta.get("filingDate"),
    }
    return {
        "status": "FOUND",
        "code": "FOUND",
        "provider": "edgartools_sidecar",
        "ticker": request.ticker,
        "topic": request.topic,
        "value": value,
        "valuePct": round(value_pct, 4) if value_pct is not None else None,
        "currency": _jsonable(topic_row.get("units") or topic_row.get("unit") or "USD"),
        "denominator": denominator,
        "evidence": evidence,
        "warnings": [] if denominator else [{
            "code": "DENOMINATOR_NOT_FOUND",
            "message": "Matched a dimensional revenue fact but could not resolve total revenue denominator.",
            "severity": "warning",
        }],
        **filing_meta,
    }


def _filing_meta(filing: Any, filing_type: str) -> dict[str, Any]:
    accession = getattr(filing, "accession_no", None) or getattr(filing, "accession_number", None)
    filing_date = getattr(filing, "filing_date", None) or getattr(filing, "filed", None)
    document_url = (
        getattr(filing, "filing_url", None)
        or getattr(filing, "document_url", None)
        or getattr(filing, "url", None)
    )
    return {
        "filingType": str(getattr(filing, "form", None) or filing_type),
        "filingDate": str(filing_date) if filing_date else None,
        "fiscalYear": f"FY{str(filing_date)[:4]}" if filing_date else None,
        "accessionNumber": str(accession) if accession else None,
        "documentUrl": str(document_url) if document_url else None,
    }


def _latest_filing(company: Any, requested_type: str) -> tuple[Any, str, list[dict[str, Any]]]:
    warnings: list[dict[str, Any]] = []
    candidates = [requested_type]
    if requested_type.upper() == "10-K":
        candidates.append("20-F")
    for form in candidates:
        try:
            filings = company.get_filings(form=form)
            filing = filings.latest()
            if filing is not None:
                if form != requested_type:
                    warnings.append({
                        "code": "AUTO_20F_FALLBACK",
                        "message": f"No {requested_type} filing found; used {form}.",
                        "severity": "warning",
                    })
                return filing, form, warnings
        except Exception:
            continue
    raise LookupError("FILING_NOT_FOUND_TRY_OTHER_TYPE")


def _query_rows_from_filing(filing: Any) -> list[dict[str, Any]]:
    xbrl = filing.xbrl()
    rows: list[dict[str, Any]] = []
    for concept in REVENUE_CONCEPTS:
        try:
            query = xbrl.query().by_concept(concept)
            rows.extend(_fact_rows_to_dicts(query.to_dataframe()))
        except Exception:
            continue
    if not rows:
        try:
            query = xbrl.query().by_label("revenue", exact=False)
            rows.extend(_fact_rows_to_dicts(query.to_dataframe()))
        except Exception:
            pass
    return rows


def extract_exposure(request: ExposureRequest) -> dict[str, Any]:
    global _LAST_ERROR_CODE
    cache_key = f"exposure:{request.ticker.upper()}:{request.topic.lower()}:{request.filing_type.upper()}:{request.period}:{request.accession_number or ''}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        from edgar import Company, set_identity  # type: ignore
    except Exception as exc:
        _LAST_ERROR_CODE = "STRUCTURED_FACT_PROVIDER_UNAVAILABLE"
        return {
            "status": "STRUCTURED_FACT_PROVIDER_UNAVAILABLE",
            "code": "STRUCTURED_FACT_PROVIDER_UNAVAILABLE",
            "provider": "edgartools_sidecar",
            "message": f"Could not import EdgarTools: {exc}",
            "ticker": request.ticker,
            "topic": request.topic,
            "value": None,
            "valuePct": None,
            "warnings": [],
        }

    identity = os.environ.get("EDGAR_IDENTITY") or os.environ.get("EDGAR_CONTACT_EMAIL")
    if identity:
        set_identity(identity)

    try:
        company = Company(request.ticker.upper())
        filing, actual_type, filing_warnings = _latest_filing(company, request.filing_type.upper())
        rows = _query_rows_from_filing(filing)
        meta = _filing_meta(filing, actual_type)
        result = normalize_exposure_facts(request, rows, meta)
        result["warnings"] = filing_warnings + list(result.get("warnings") or [])
        result["generatedAt"] = _now_iso()
        _LAST_ERROR_CODE = None if result.get("status") == "FOUND" else str(result.get("code"))
        return _cache_set(cache_key, result)
    except LookupError:
        _LAST_ERROR_CODE = "FILING_NOT_FOUND_TRY_OTHER_TYPE"
        return {
            "status": "FILING_NOT_FOUND_TRY_OTHER_TYPE",
            "code": "FILING_NOT_FOUND_TRY_OTHER_TYPE",
            "provider": "edgartools_sidecar",
            "ticker": request.ticker,
            "topic": request.topic,
            "value": None,
            "valuePct": None,
            "requestedFilingType": request.filing_type,
            "suggestedFilingTypes": ["20-F"] if request.filing_type.upper() == "10-K" else [],
            "warnings": [],
        }
    except Exception as exc:
        code = "SEC_RATE_LIMITED" if "429" in str(exc) or "rate" in str(exc).lower() else "PROVIDER_LIMITATION"
        _LAST_ERROR_CODE = code
        return {
            "status": code,
            "code": code,
            "provider": "edgartools_sidecar",
            "message": str(exc),
            "ticker": request.ticker,
            "topic": request.topic,
            "value": None,
            "valuePct": None,
            "warnings": [{"code": code, "message": str(exc), "severity": "error"}],
        }


async def health(_: Request) -> JSONResponse:
    return JSONResponse({
        "status": "ok",
        "provider": "edgartools_sidecar",
        "cacheEntries": len(_CACHE),
        "cacheTtlSeconds": TTL_SECONDS,
        "lastErrorCode": _LAST_ERROR_CODE,
        "generatedAt": _now_iso(),
    })


async def exposure(request: Request) -> JSONResponse:
    body = await request.json()
    req = ExposureRequest(
        ticker=str(body.get("ticker") or "").strip().upper(),
        topic=str(body.get("topic") or body.get("region") or body.get("exposure_query") or "China").strip(),
        filing_type=str(body.get("filing_type") or "10-K").strip().upper(),
        period=str(body.get("period") or "latest").strip(),
        accession_number=str(body["accession_number"]).strip() if body.get("accession_number") else None,
        detail_level=str(body.get("detailLevel") or "compact"),
    )
    if not req.ticker or not req.topic:
        return JSONResponse({
            "status": "INPUT_VALIDATION_ERROR",
            "code": "INPUT_VALIDATION_ERROR",
            "message": "ticker and topic are required.",
        }, status_code=400)
    result = await asyncio.to_thread(extract_exposure, req)
    return JSONResponse(result)


app = Starlette(routes=[
    Route("/health", health, methods=["GET"]),
    Route("/sec/facts/exposure", exposure, methods=["POST"]),
])


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8081"))
    uvicorn.run(app, host="0.0.0.0", port=port)
