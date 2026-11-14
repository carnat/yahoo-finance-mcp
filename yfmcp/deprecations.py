"""The caller-chosen liquidity minimum (2.8.0, N3b), the 3.11.0 field deprecations (neutrality audit NL1-NL8,
NL12-NL13) and the status table. Mirrors worker/src/deprecations.ts.

The 2.8.0 and 2.27.0 deprecations were removed in 3.0.0; see docs/provider-runtime-guidance.md "Breaking Changes
(3.0.0)". Each 3.11.0 deprecated field keeps emitting its old value; a DEPRECATED_FIELD notice in the envelope's
meta.warnings names its replacement. The notice never goes into ``data``.

This module stays import-free of the rest of yfmcp, like worker/src/deprecations.ts.
"""

from __future__ import annotations

import math

_REMOVAL = "will be removed in the next major version"


def _field_list(fields: list[str]) -> str:
    return f"{', '.join(fields)} {'is' if len(fields) == 1 else 'are'}"


def _deprecated_field_warning(fields: list[str], replacement: str, reason: str) -> dict:
    return {
        "code": "DEPRECATED_FIELD",
        "message": f"{_field_list(fields)} deprecated and {_REMOVAL}; {reason}",
        "severity": "info",
        "fields": fields,
        "replacement": replacement,
    }


# NL1: the moving-average label.
MA_TREND_DEPRECATION = _deprecated_field_warning(
    ["trend"], "maAlignment",
    "use maAlignment (ABOVE_BOTH, BELOW_BOTH or MIXED: the price against the 50- and 200-day averages).",
)
SNAPSHOT_MA_TREND_DEPRECATION = _deprecated_field_warning(
    ["trend.maTrend"], "trend.maAlignment",
    "use trend.maAlignment (ABOVE_BOTH, BELOW_BOTH or MIXED).",
)
# NL2: rating-change labels.
RATING_LABEL_DEPRECATION = _deprecated_field_warning(
    ["netSentiment", "changes[].strengthFlag"], "netUpgrades, changes[].signal, changes[].mixedSignal",
    "use netUpgrades (upgrades minus downgrades); each change's signal and mixedSignal carry what strengthFlag restated.",
)
# NL3: earnings-momentum labels.
_EARNINGS_LABELS = [
    "momentumFlag", "revisionDirection", "forwardRevisionSignal", "historicalSurpriseSignal",
    "compositeMomentumSignal", "interpretationNote",
]
EARNINGS_LABEL_DEPRECATION = _deprecated_field_warning(
    _EARNINGS_LABELS, "revisionBand, beatRateBand",
    "use revisionBand (UP, FLAT or DOWN at revisionThresholdPct, set with revision_threshold_pct) and beatRateBand, "
    "with the revision and beatRate numbers.",
)
# NL4: short-interest labels.
SHORT_LABEL_DEPRECATION = _deprecated_field_warning(
    ["squeezeRisk", "flag"], "shortPctFloat, daysToCover",
    "they have no replacement label; read shortPctFloat and daysToCover.",
)
# NL5: the options-flow bracket.
OPTIONS_BRACKET_DEPRECATION = _deprecated_field_warning(
    ["bracket"], "pcRatio, putVolTrend",
    "read pcRatio and putVolTrend; formattedBlock will drop its Advisory segment.",
)
# NL6: credit labels.
CREDIT_LABEL_DEPRECATION = _deprecated_field_warning(
    ["debtTier", "creditStressFlag"], "leverageBand, meetsCreditStressThreshold",
    "use leverageBand (the netDebtToEbitda band) and meetsCreditStressThreshold "
    "(set with max_net_debt_to_ebitda and min_interest_coverage).",
)
# NL7: the consensus-target label.
TARGET_LAG_DEPRECATION = _deprecated_field_warning(
    ["targetLagSignal"], "targetVsPrice",
    "use targetVsPrice (AT_OR_ABOVE or BELOW) with recentUpgradeCount30d.",
)
# NL1-NL3: analyze_position_signals copies of those labels.
POSITION_LABEL_DEPRECATION = _deprecated_field_warning(
    ["analystInputs.analystNetSentiment", "earningsInputs.momentumFlag", "technicalInputs.maPosition"],
    "analystInputs.analystNetUpgrades, earningsInputs.revisionBand, technicalInputs.maAlignment",
    "use analystInputs.analystNetUpgrades, earningsInputs.revisionBand and technicalInputs.maAlignment.",
)
# NL12: calculate_price_target_distance copies under workflow names.
PRICE_TARGET_COPY_DEPRECATION = _deprecated_field_warning(
    ["ioPt", "eqfPct", "invertedFlag"], "referenceTargetPrice, referenceTargetPct, priceAtOrAboveTarget",
    "use referenceTargetPrice, referenceTargetPct and priceAtOrAboveTarget.",
)
# NL13: the hedge candidates' emoji IV flag.
HEDGE_IV_FLAG_DEPRECATION = _deprecated_field_warning(
    ["candidates[].ivFlag"], "candidates[].ivPctile",
    "read each candidate's ivPctile.",
)
# NL8: the io_pt argument (a payload notice from the tool, since it depends on the arguments).
IO_PT_ARGUMENT_DEPRECATION: dict = {
    "code": "DEPRECATED_FIELD",
    "message": f"The io_pt argument is deprecated and {_REMOVAL}; pass reference_target_price.",
    "severity": "info",
    "fields": ["io_pt"],
    "replacement": "reference_target_price",
}


def _has_path(rec: dict, path: str) -> bool:
    """True when ``path`` resolves in ``rec``; a ``name[]`` segment matches when any element of that array has the rest."""
    head, *rest = path.split(".")
    if head.endswith("[]"):
        items = rec.get(head[:-2])
        return isinstance(items, list) and any(
            isinstance(item, dict) and len(rest) > 0 and _has_path(item, ".".join(rest)) for item in items
        )
    if head not in rec:
        return False
    if not rest:
        return True
    nxt = rec[head]
    return isinstance(nxt, dict) and _has_path(nxt, ".".join(rest))


def _payload_has_any_field(data: object, fields: list[str]) -> bool:
    """Whether any listed field is in the payload, at the top level or, for a batch result, one level down
    (``{"AAPL": {...}, "MSFT": {...}}``) or under ``tickers`` (get_market_snapshot). Only keys are read."""
    if not isinstance(data, dict):
        return False

    def has(rec: dict) -> bool:
        return any(_has_path(rec, field) for field in fields)

    if has(data):
        return True
    batch = data["tickers"] if isinstance(data.get("tickers"), dict) else data
    return any(isinstance(entry, dict) and has(entry) for entry in batch.values())


_NOTICES_BY_TOOL: dict[str, list[dict]] = {
    "analyze_moving_average_position": [MA_TREND_DEPRECATION],
    "get_market_snapshot": [SNAPSHOT_MA_TREND_DEPRECATION],
    "get_analyst_rating_changes": [RATING_LABEL_DEPRECATION],
    "analyze_earnings_momentum": [EARNINGS_LABEL_DEPRECATION],
    "get_short_momentum": [SHORT_LABEL_DEPRECATION],
    "analyze_options_flow_window": [OPTIONS_BRACKET_DEPRECATION],
    "analyze_credit_health": [CREDIT_LABEL_DEPRECATION],
    "get_analyst_consensus": [TARGET_LAG_DEPRECATION],
    "analyze_position_signals": [POSITION_LABEL_DEPRECATION],
    "calculate_price_target_distance": [PRICE_TARGET_COPY_DEPRECATION],
    "find_put_hedge_candidates": [HEDGE_IV_FLAG_DEPRECATION],
}


def deprecation_warnings_for(tool: str, data: object) -> list[dict]:
    """The DEPRECATED_FIELD notices for one successful tool response: one per notice whose fields the payload carries
    (copies, so a caller cannot change the table)."""
    return [
        {**notice, "fields": list(notice["fields"])}
        for notice in _NOTICES_BY_TOOL.get(tool, [])
        if _payload_has_any_field(data, notice["fields"])
    ]

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
