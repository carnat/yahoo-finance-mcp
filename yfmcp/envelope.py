"""MCP response envelope helpers.

Extracted from server.py in Phase 1 of the refactoring plan.
server.py re-imports all public names from this module so that
``server._mcp_success``, ``server.ErrorCode``, etc. keep working.
"""

import datetime
import json
import os
from typing import TypedDict

from yfmcp.build_info import BUILD_DATE, SERVER_VERSION

# ---------------------------------------------------------------------------
# Server version and envelope feature flag
# ---------------------------------------------------------------------------
class _DynamicEnvelopeV2Flag:
    def __bool__(self) -> bool:
        # V2 is the default since 2.0.0; set MCP_ENVELOPE_V2 to any other
        # value (e.g. "false") for the legacy 1.x response shape.
        return os.environ.get("MCP_ENVELOPE_V2", "true").lower() == "true"
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
        "meta": _base_meta(
            tool,
            canonical_tool=canonical_tool,
            deprecated_tool=deprecated_tool,
            use_instead=use_instead,
            source=source,
            data_date=data_date,
            cache_hit=cache_hit,
            warnings=warnings,
        ),
        "error": None,
    })


def _base_meta(
    tool: str,
    *,
    canonical_tool: str | None = None,
    deprecated_tool: bool | None = None,
    use_instead: str | None = None,
    source: str = "yahoo_finance",
    data_date: str | None = None,
    cache_hit: bool = False,
    warnings: list[object] | None = None,
) -> dict:
    return {
        "tool": tool,
        **({"canonicalTool": canonical_tool} if canonical_tool is not None else {}),
        **({"deprecatedTool": deprecated_tool} if deprecated_tool is not None else {}),
        **({"useInstead": use_instead} if use_instead is not None else {}),
        "source": source,
        "dataDate": data_date,
        "serverVersion": SERVER_VERSION,
        "cacheHit": cache_hit,
        "warnings": warnings or [],
    }


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


import re

# ---------------------------------------------------------------------------
# Envelope V2 standardization helper
# ---------------------------------------------------------------------------
def _is_price_bar(val: dict) -> bool:
    return "open" in val and "high" in val and "low" in val and "close" in val


def _enrich_facts(val, parent_source_type=None, parent_confidence=None, is_metric=False):
    if isinstance(val, dict):
        # OHLC price bars carry high/low keys but are raw exchange prices, not
        # extracted facts that need evidence tagging.
        if _is_price_bar(val):
            return val
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
            
            urls = []
            source_evidence = val.get("sourceEvidence")
            preserve_decision_grade_xbrl_evidence = (
                val.get("decisionGrade") is True
                and isinstance(source_evidence, dict)
                and "XBRL" in str(val.get("extractionMethod") or "").upper()
            )
            if preserve_decision_grade_xbrl_evidence:
                val["evidence"] = dict(source_evidence)
                url = source_evidence.get("url") or source_evidence.get("documentUrl")
                if url:
                    urls.append(str(url))
            else:
                orig_ev = val.get("evidence")
                ev_list = []
                if isinstance(orig_ev, list):
                    ev_list = orig_ev
                elif orig_ev:
                    ev_list = [orig_ev]

                standardised_ev = []
                for ev in ev_list:
                    if isinstance(ev, dict):
                        url = ev.get("url") or ev.get("documentUrl") or None
                        if url:
                            urls.append(str(url))
                        standardised_ev.append({
                            "url": url,
                            "sourceType": ev.get("sourceType") or None,
                            "filingType": ev.get("filingType") or None,
                            "accessionNumber": ev.get("accessionNumber") or None,
                            "filingDate": ev.get("filingDate") or None,
                            "publishedAt": ev.get("publishedAt") or None,
                            "retrievedAt": ev.get("retrievedAt") or None,
                            "excerpt": ev.get("excerpt") or None,
                            "concept": ev.get("concept") or None,
                            "periodEnd": ev.get("periodEnd") or None,
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
                            "sourceType": None,
                            "filingType": None,
                            "accessionNumber": None,
                            "filingDate": None,
                            "publishedAt": None,
                            "retrievedAt": None,
                            "excerpt": None,
                            "concept": None,
                            "periodEnd": None,
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
            if k in {"confidence", "sourceType", "evidenceRequired", "decisionGrade", "evidence", "sourceEvidence", "xbrlContext", "calculation"}:
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


# ---------------------------------------------------------------------------
# Tool-boundary envelope (parity with the Worker's callTool/mcpSuccess)
# ---------------------------------------------------------------------------
# Matches an HTTP 429 status ("error 429", "HTTP 429", "status: 429"). A bare
# "429" substring test also matched tickers and timestamps inside URLs quoted
# in provider exception text, reporting ordinary failures as rate limits.
_HTTP_429_PATTERN = re.compile(r"\b(?:error|http|status)[:\s]*429\b")

# Legacy {"error": true, ...} fields carried into the V2 error object.
_LEGACY_ERROR_FIELDS = (
    "retryable",
    "fallbackSuggested",
    "recommendedNextAction",
    "missingParams",
    "invalidParams",
    "unexpectedParams",
    "expectedParams",
)


def _legacy_text_failure(tool: str, text: str) -> str | None:
    """Failure envelope for a plain-text legacy error, or None if text is not one."""
    text = text.strip()
    lower = text.lower()
    if not (
        lower.startswith("error")
        or (lower.startswith("company ticker") and "not found" in lower)
    ):
        return None
    if lower.startswith("error: invalid") or " is required" in lower:
        code = ErrorCode.INPUT_VALIDATION_ERROR
    elif "no option" in lower:
        code = ErrorCode.NO_OPTIONS_DATA
    elif "not found" in lower:
        code = ErrorCode.TICKER_NOT_FOUND
    elif "rate limit" in lower or _HTTP_429_PATTERN.search(lower):
        code = ErrorCode.RATE_LIMIT
    elif "timeout" in lower or "timed out" in lower:
        code = ErrorCode.PROVIDER_TIMEOUT
    else:
        code = ErrorCode.PROVIDER_ERROR
    return _mcp_failure(tool, code, text)


def _envelope_tool_result(tool: str, result: object) -> object:
    """Return a tool's raw result in the V2 envelope, as the Worker does.

    Applied once per MCP tool call so local responses share the hosted shape:
    existing envelopes gain any missing base meta, legacy ``{"error": true}``
    payloads and plain-text errors become failure envelopes, and everything
    else becomes ``{"ok": true, "data": ...}`` with fact enrichment. With
    MCP_ENVELOPE_V2 disabled the raw result is returned unchanged.
    """
    if not _ENVELOPE_V2 or not isinstance(result, str):
        return result
    text = result.strip()
    try:
        parsed = json.loads(text)
    except ValueError:
        parsed = text
    if isinstance(parsed, str):
        failure = _legacy_text_failure(tool, parsed)
        if failure is not None:
            return failure
        return json.dumps({"ok": True, "data": parsed, "meta": _base_meta(tool), "error": None})
    if isinstance(parsed, dict):
        if isinstance(parsed.get("ok"), bool) and ("data" in parsed or "error" in parsed):
            inner_meta = parsed.get("meta") if isinstance(parsed.get("meta"), dict) else {}
            parsed["meta"] = {**_base_meta(tool), **inner_meta}
            if parsed["ok"] is True:
                parsed["data"] = _enrich_facts(parsed.get("data"))
            return json.dumps(parsed)
        if parsed.get("error") is True:
            meta_extra: dict = {}
            error_extra = {k: parsed[k] for k in _LEGACY_ERROR_FIELDS if k in parsed}
            if error_extra:
                meta_extra["error_extra"] = error_extra
            if "diagnostics" in parsed:
                meta_extra["diagnostics"] = parsed["diagnostics"]
            return _mcp_failure(
                tool,
                str(parsed.get("code") or ErrorCode.PROVIDER_ERROR),
                str(parsed.get("message") or "Tool returned a legacy error without details."),
                meta_extra=meta_extra or None,
            )
    return json.dumps({"ok": True, "data": _enrich_facts(parsed), "meta": _base_meta(tool), "error": None})
