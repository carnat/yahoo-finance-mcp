"""MCP response envelope helpers.

Extracted from server.py in Phase 1 of the refactoring plan.
server.py re-imports all public names from this module so that
``server._mcp_success``, ``server.ErrorCode``, etc. keep working.
"""

import datetime
import json
import os
from typing import TypedDict

# ---------------------------------------------------------------------------
# Server version and envelope feature flag
# ---------------------------------------------------------------------------
SERVER_VERSION = os.environ.get("SERVER_VERSION", "0.2.0")
BUILD_DATE = os.environ.get("BUILD_DATE", "unknown")


class _DynamicEnvelopeV2Flag:
    def __bool__(self) -> bool:
        return os.environ.get("MCP_ENVELOPE_V2", "").lower() == "true"
    def __repr__(self) -> str:
        return str(bool(self))
    def __eq__(self, other: object) -> bool:
        return bool(self) == other


_ENVELOPE_V2 = _DynamicEnvelopeV2Flag()



# ---------------------------------------------------------------------------
# Typed domain error codes
# ---------------------------------------------------------------------------
class ErrorCode:
    TICKER_NOT_FOUND = "TICKER_NOT_FOUND"
    NO_OPTIONS_DATA = "NO_OPTIONS_DATA"
    NO_FILING_DATA = "NO_FILING_DATA"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    PROVIDER_TIMEOUT = "PROVIDER_TIMEOUT"
    RATE_LIMIT = "RATE_LIMIT"
    INPUT_VALIDATION_ERROR = "INPUT_VALIDATION_ERROR"
    DEPRECATED_TOOL = "DEPRECATED_TOOL"
    AMBIGUOUS_CONTEXT = "AMBIGUOUS_CONTEXT"


# ---------------------------------------------------------------------------
# McpResponse TypedDicts
# ---------------------------------------------------------------------------
class ToolMeta(TypedDict):
    tool: str
    canonicalTool: str | None
    deprecatedTool: bool
    useInstead: str
    source: str
    dataDate: str | None
    serverVersion: str
    cacheHit: bool
    warnings: list[object]


class ErrorDetail(TypedDict):
    code: str
    message: str


class McpResponse(TypedDict):
    ok: bool
    data: object
    meta: ToolMeta
    error: ErrorDetail | None


# ---------------------------------------------------------------------------
# McpResponse helpers
# ---------------------------------------------------------------------------
def _mcp_success(
    tool: str,
    data: object,
    *,
    canonical_tool: str | None = None,
    deprecated_tool: bool | None = None,
    use_instead: str | None = None,
    source: str = "yahoo_finance",
    data_date: str | None = None,
    cache_hit: bool = False,
    warnings: list[object] | None = None,
) -> str:
    if not _ENVELOPE_V2:
        return data if isinstance(data, str) else json.dumps(data)
    return json.dumps({
        "ok": True,
        "data": data if not isinstance(data, str) else json.loads(data),
        "meta": {
            "tool": tool,
            **({"canonicalTool": canonical_tool} if canonical_tool is not None else {}),
            **({"deprecatedTool": deprecated_tool} if deprecated_tool is not None else {}),
            **({"useInstead": use_instead} if use_instead is not None else {}),
            "source": source,
            "dataDate": data_date,
            "serverVersion": SERVER_VERSION,
            "cacheHit": cache_hit,
            "warnings": warnings or [],
        },
        "error": None,
    })


def _mcp_failure(
    tool: str,
    code: str,
    message: str,
    *,
    source: str = "yahoo_finance",
    data_date: str | None = None,
    meta_extra: dict | None = None,
) -> str:
    error_payload = {
        "code": code,
        "message": message,
    }
    diagnostics = None
    if meta_extra:
        if "error_extra" in meta_extra:
            error_payload.update(meta_extra["error_extra"])
            meta_extra = {k: v for k, v in meta_extra.items() if k != "error_extra"}
        if "diagnostics" in meta_extra:
            diagnostics = meta_extra["diagnostics"]
            meta_extra = {k: v for k, v in meta_extra.items() if k != "diagnostics"}

    payload = {
        "ok": False,
        "data": None,
        "meta": {
            "tool": tool,
            "source": source,
            "dataDate": data_date,
            "serverVersion": SERVER_VERSION,
            "cacheHit": False,
            "warnings": [],
        },
        "error": error_payload,
    }
    if meta_extra:
        payload["meta"].update(meta_extra)
    if diagnostics is not None:
        payload["diagnostics"] = diagnostics

    if not _ENVELOPE_V2:
        ret = {"error": True, "code": code, "message": message}
        if "fallbackSuggested" in error_payload:
            ret["fallbackSuggested"] = error_payload["fallbackSuggested"]
        if "retryable" in error_payload:
            ret["retryable"] = error_payload["retryable"]
        return json.dumps(ret)
    return json.dumps(payload)


def _mcp_warning(
    tool: str,
    data: object,
    message: str,
    *,
    canonical_tool: str | None = None,
    source: str = "yahoo_finance",
    data_date: str | None = None,
) -> str:
    if not _ENVELOPE_V2:
        return data if isinstance(data, str) else json.dumps(data)
    parsed = data if not isinstance(data, str) else json.loads(data)
    return json.dumps({
        "ok": True,
        "data": parsed,
        "meta": {
            "tool": tool,
            **({"canonicalTool": canonical_tool} if canonical_tool is not None else {}),
            "source": source,
            "dataDate": data_date,
            "serverVersion": SERVER_VERSION,
            "cacheHit": False,
            "warnings": [message],
        },
        "error": None,
    })


import re

# ---------------------------------------------------------------------------
# Envelope V2 standardization helper
# ---------------------------------------------------------------------------
def _enrich_facts(val, parent_source_type=None, parent_confidence=None, is_metric=False):
    if isinstance(val, dict):
        is_fact = "value" in val or "low" in val or "high" in val or "valueRatio" in val or "valuePct" in val or is_metric
        
        if is_fact:
            has_explicit_decision_grade = "decisionGrade" in val
            conf = val.get("confidence") or parent_confidence
            if not conf:
                if val.get("value") is not None or val.get("low") is not None or val.get("valueRatio") is not None:
                    conf = "HIGH"
                else:
                    conf = "NOT_DECISION_GRADE"
            
            conf = str(conf).upper()
            if conf not in {"HIGH", "MEDIUM", "LOW", "NOT_DECISION_GRADE"}:
                if conf == "NOT_DISCLOSED":
                    conf = "NOT_DECISION_GRADE"
                else:
                    conf = "LOW"
                    
            val["confidence"] = conf
            
            orig_ev = val.get("evidence")
            ev_list = []
            if isinstance(orig_ev, list):
                ev_list = orig_ev
            elif orig_ev:
                ev_list = [orig_ev]
                
            standardised_ev = []
            urls = []
            for ev in ev_list:
                if isinstance(ev, dict):
                    url = ev.get("url") or ev.get("documentUrl") or None
                    if url:
                        urls.append(str(url))
                    standardised_ev.append({
                        "url": url,
                        "filingType": ev.get("filingType") or None,
                        "accessionNumber": ev.get("accessionNumber") or None,
                        "filingDate": ev.get("filingDate") or None,
                        "tableIndex": ev.get("tableIndex") or None,
                        "rowLabel": ev.get("rowLabel") or None,
                        "columnLabel": ev.get("columnLabel") or None,
                        "rawRow": ev.get("rawRow") or None,
                    })
                elif isinstance(ev, str):
                    if ev.startswith("http"):
                        urls.append(ev)
                    standardised_ev.append({
                        "url": ev if ev.startswith("http") else None,
                        "filingType": None,
                        "accessionNumber": None,
                        "filingDate": None,
                        "tableIndex": None,
                        "rowLabel": None,
                        "columnLabel": None,
                        "rawRow": ev,
                    })
            val["evidence"] = standardised_ev if standardised_ev else None

            inferred_source_type = None
            for url in urls:
                url_lower = url.lower()
                if "sec.gov" in url_lower:
                    if "companyfacts" in url_lower or "submissions" in url_lower or ".xml" in url_lower:
                        inferred_source_type = "sec_xbrl"
                        break
                    elif "ix?doc=" in url_lower or "index" in url_lower:
                        inferred_source_type = "sec_table"
                        break
                    else:
                        inferred_source_type = "sec_filing"
                        break
                elif "yahoo.com" in url_lower:
                    inferred_source_type = "yahoo"
                    break
                elif "ir." in url_lower or "investor" in url_lower:
                    inferred_source_type = "company_ir"
                    break
                elif url_lower.startswith("http"):
                    inferred_source_type = "unknown"
                    break

            if not inferred_source_type:
                inferred_source_type = parent_source_type

            if not inferred_source_type:
                ext_method = str(val.get("extractionMethod") or "").upper()
                if "XBRL" in ext_method or "COMPANYFACTS" in ext_method:
                    inferred_source_type = "sec_xbrl"
                elif "HTML" in ext_method or "TABLE" in ext_method:
                    inferred_source_type = "sec_table"
                elif "TEXT" in ext_method:
                    inferred_source_type = "sec_filing"

            if not inferred_source_type:
                inferred_source_type = "unknown"

            val["sourceType"] = val.get("sourceType") or inferred_source_type
            val["evidenceRequired"] = True
            if not has_explicit_decision_grade:
                val["decisionGrade"] = False
            
        source_type = val.get("source") or val.get("sourceType") or parent_source_type
        confidence = val.get("confidence") or parent_confidence
        
        for k, v in list(val.items()):
            if k in {"confidence", "sourceType", "evidenceRequired", "decisionGrade", "evidence"}:
                continue
            child_is_metric = is_fact or k in {"metrics", "actual", "estimate", "revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex", "eps"}
            val[k] = _enrich_facts(v, parent_source_type=source_type, parent_confidence=confidence, is_metric=child_is_metric)
            
    elif isinstance(val, list):
        return [_enrich_facts(item, parent_source_type, parent_confidence, is_metric) for item in val]
        
    return val


def _wrap_envelope_v2(
    tool_name: str,
    data: dict | list | None,
    *,
    warnings: list[dict] | None = None,
    error: str | None = None,
    error_code: str | None = None,
    meta_extra: dict | None = None,
) -> str:
    """
    Returns a JSON-encoded MCP Envelope V2 string.

    Shape:
    {
      "ok": true | false,
      "data": <payload>,          # present when ok=true
      "error": "<message>",       # present when ok=false
      "errorCode": "<CODE>",      # present when ok=false
      "meta": {
        "tool": "<tool_name>",
        "generatedAt": "<ISO-UTC>",
        "warnings": [ { "code": str, "message": str } ]
      }
    }
    """
    generated_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    meta: dict = {
        "tool": tool_name,
        "generatedAt": generated_at,
        "warnings": warnings or [],
    }
    if meta_extra:
        meta.update(meta_extra)

    if error is not None:
        error_payload = {
            "code": error_code or "UNKNOWN_ERROR",
            "message": error,
        }
        diagnostics = None
        if meta_extra:
            if "error_extra" in meta_extra:
                error_payload.update(meta_extra["error_extra"])
                # Clean up to avoid duplicate keys in meta
                meta_extra = {k: v for k, v in meta_extra.items() if k != "error_extra"}
            if "diagnostics" in meta_extra:
                diagnostics = meta_extra["diagnostics"]
                meta_extra = {k: v for k, v in meta_extra.items() if k != "diagnostics"}
            meta.update(meta_extra)

        resp = {
            "ok": False,
            "error": error_payload,
            "errorCode": error_code or "UNKNOWN_ERROR",
            "data": None,
            "meta": meta,
        }
        if diagnostics is not None:
            resp["diagnostics"] = diagnostics

        return json.dumps(resp)

    enriched_data = _enrich_facts(data)
    return json.dumps({
        "ok": True,
        "data": enriched_data,
        "error": None,
        "errorCode": None,
        "meta": meta,
    })
