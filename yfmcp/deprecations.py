"""The caller-chosen liquidity minimum (2.8.0, N3b) and the status table. Mirrors worker/src/deprecations.ts.

The 2.8.0 and 2.27.0 field deprecations and their DEPRECATED_FIELD notices were removed in 3.0.0; see
docs/provider-runtime-guidance.md "Breaking Changes (3.0.0)".

This module stays import-free of the rest of yfmcp, like worker/src/deprecations.ts.
"""

from __future__ import annotations

import math

# The tools whose success meta carries their status-table entry (3.7.0): a copy of the Worker's TOOL_RELIABILITY_STATUS
# (worker/src/tools.ts), same values and key order. reliabilityClass also appears in two tools' data.
TOOL_RELIABILITY_STATUS: dict[str, dict] = {
    "get_sec_filing_section_markdown": {
        "capabilityStatus": "DEGRADED",
        "reliabilityClass": "UNRELIABLE",
        "failureMode": "LIVE_SECTION_EXTRACTION_UNRELIABLE",
        "evidenceRequired": True,
        "sourceType": "sec_filing",
    },
    "get_company_press_releases": {
        "capabilityStatus": "ACTIVE",
        "reliabilityClass": "RELIABLE",
        "failureMode": None,
        "evidenceRequired": True,
        "sourceType": "company_ir_page",
        "successCriteria": ["coverageStatus=SEC_EX99_RESOLVED or APPROVED_IR_PAGE_RESOLVED", "decisionGrade=true", "secEvidence or irPageEvidence present"],
        "limitationStatuses": ["SEC_8K_FOUND_EX99_NOT_FOUND", "NO_OFFICIAL_RELEASE_SOURCE", "NO_YAHOO_PRESS_RELEASE", "COMPANY_IR_PAGE_NOT_APPROVED", "COMPANY_IR_PAGE_CANDIDATE_ONLY", "COMPANY_IR_NOT_FOUND", "NOT_FOUND"],
        "note": "Tool is usable, but gate-clear is payload-level only; candidate, unresolved, RSS-only, newswire, and Yahoo payloads remain non-decision-grade.",
    },
    "extract_sec_filing_fact": {
        "capabilityStatus": "ACTIVE",
        "reliabilityClass": "RELIABLE",
        "failureMode": None,
        "evidenceRequired": True,
        "sourceType": "sec_xbrl",
        "successCriteria": ["value", "xbrlContext", "sourceEvidence"],
        "limitationStatuses": ["SEC_FACT_NOT_AVAILABLE", "NO_COMPANYCONCEPT_FACT_FOR_FORM", "UNSUPPORTED_XBRL_CONCEPT"],
        "note": "Decision-grade only for successful XBRL facts with value, xbrlContext, and sourceEvidence; limitation statuses remain non-decision-grade at payload level.",
    },
}


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
