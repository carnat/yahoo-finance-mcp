"""Field deprecations. Mirrors worker/src/deprecations.ts.

Each deprecated field keeps emitting its old value; a DEPRECATED_FIELD notice in
the envelope's ``meta.warnings`` names its replacement. The notice never goes
into ``data``: evidence payloads are hashed into ``evidenceCutId`` and ``meta``
sits outside that hash. The texts are the Worker's, verbatim. The 2.8.0 and
2.27.0 deprecations were removed in 3.0.0; see docs/provider-runtime-guidance.md
"Breaking Changes (3.0.0)". Only the F20 status-confidence notice remains.

This module stays import-free of the rest of yfmcp, like worker/src/deprecations.ts.
"""

from __future__ import annotations

import math

# The tools whose meta (and, for two of them, data) carry reliabilityClass. The values are the Worker's
# TOOL_RELIABILITY_STATUS reliabilityClass.
TOOL_RELIABILITY_CLASS = {
    "get_sec_filing_section_markdown": "UNRELIABLE",
    "get_company_press_releases": "RELIABLE",
    "extract_sec_filing_fact": "RELIABLE",
}


_REMOVAL = "will be removed in the next major version"


def _deprecated_field_warning(fields: list[str], replacement: str, message: str) -> dict:
    # Key order is the Worker's: code, message, severity, fields, replacement.
    return {"code": "DEPRECATED_FIELD", "message": message, "severity": "info",
            "fields": list(fields), "replacement": replacement}


# F20 (2.27.0): status strings carried in confidence.
STATUS_CONFIDENCE_DEPRECATION = _deprecated_field_warning(
    ["confidence"],
    "status",
    "confidence values other than HIGH, MEDIUM and LOW (status strings such as EXTRACTION_FAILED or NOT_DISCLOSED) are deprecated "
    f"and {_REMOVAL}; confidence will hold only HIGH, MEDIUM or LOW, and a status field names the limitation.",
)
_CONFIDENCE_LEVELS = {"HIGH", "MEDIUM", "LOW"}


def payload_has_status_confidence(data: object, depth: int = 0) -> bool:
    """Whether any ``confidence`` in the payload holds a string other than HIGH, MEDIUM or LOW (F20)."""
    if depth > 8 or not isinstance(data, (dict, list)):
        return False
    if isinstance(data, list):
        return any(payload_has_status_confidence(item, depth + 1) for item in data)
    for key, value in data.items():
        if key == "confidence" and isinstance(value, str) and value not in _CONFIDENCE_LEVELS:
            return True
        if isinstance(value, (dict, list)) and payload_has_status_confidence(value, depth + 1):
            return True
    return False


def deprecation_warnings_for(tool: str, data: object, *, evidence_only: bool) -> list[dict]:
    """The DEPRECATED_FIELD notices for one successful tool response (fresh dicts, safe to mutate).
    ``tool`` and ``evidence_only`` are kept for the callers; since 3.0.0 only the F20 notice remains."""
    out: list[dict] = []
    if payload_has_status_confidence(data):
        out.append({**STATUS_CONFIDENCE_DEPRECATION, "fields": list(STATUS_CONFIDENCE_DEPRECATION["fields"])})
    return out


def meets_minimum(adv20d_traded_value_usd: float | None, min_adv_usd: float | None) -> bool | None:
    """ADV20 traded value against a caller-chosen floor; None when either is unknown."""
    if min_adv_usd is None or adv20d_traded_value_usd is None:
        return None
    return adv20d_traded_value_usd >= min_adv_usd


MIN_ADV_USD_ERROR = "min_adv_usd must be a positive number when supplied."
MIN_ADV_USD_DESCRIPTION = (
    "Optional caller-chosen minimum 20-day average daily traded value in USD; "
    "when supplied, meetsMinimum compares adv20dTradedValueUsd against it."
)


def parse_min_adv_usd(value: object) -> tuple[float | None, str | None]:
    """The caller's min_adv_usd as ``(value, error)``: ``(None, None)`` when not supplied, ``(number, None)``
    when valid, ``(None, message)`` for a non-numeric, non-finite or non-positive value (a bool is not a number)."""
    if value is None:
        return None, None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        return None, MIN_ADV_USD_ERROR
    return value, None
