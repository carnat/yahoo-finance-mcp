"""Field deprecations (2.8.0, RN). Mirrors worker/src/deprecations.ts.

Each deprecated field keeps emitting its old value; a DEPRECATED_FIELD notice in
the envelope's ``meta.warnings`` names its replacement. The notice never goes
into ``data``: evidence payloads are hashed into ``evidenceCutId`` and ``meta``
sits outside that hash. The fields are removed in the next major version. The
texts are the Worker's, verbatim.

Python never emitted ``doctrineUse``, so it emits no N1 notice; it adds
``reliabilityClass`` only.

This module stays import-free of the rest of yfmcp, like worker/src/deprecations.ts.
"""

from __future__ import annotations

import math
import re

# doctrineUse -> reliabilityClass (N1).
RELIABILITY_CLASS_BY_DOCTRINE_USE = {
    "ALLOWED": "RELIABLE",
    "VERIFY_ONLY": "VERIFY_BEFORE_USE",
    "DIAGNOSTICS_ONLY": "DIAGNOSTIC",
    "BLOCKED": "UNRELIABLE",
}

# The tools whose meta (and, for two of them, data) carry reliabilityClass. The values are the Worker's
# TOOL_DOCTRINE_STATUS doctrineUse, mapped through RELIABILITY_CLASS_BY_DOCTRINE_USE.
TOOL_RELIABILITY_CLASS = {
    "get_sec_filing_section_markdown": RELIABILITY_CLASS_BY_DOCTRINE_USE["BLOCKED"],
    "get_company_press_releases": RELIABILITY_CLASS_BY_DOCTRINE_USE["ALLOWED"],
    "extract_sec_filing_fact": RELIABILITY_CLASS_BY_DOCTRINE_USE["ALLOWED"],
}


def reliability_class_for(doctrine_use: str) -> str:
    return RELIABILITY_CLASS_BY_DOCTRINE_USE[doctrine_use]


_REMOVAL = "will be removed in the next major version"


def _field_list(fields: list[str]) -> str:
    return f"{', '.join(fields)} {'is' if len(fields) == 1 else 'are'}"


def _deprecated_field_warning(fields: list[str], replacement: str, message: str) -> dict:
    # Key order is the Worker's: code, message, severity, fields, replacement.
    return {"code": "DEPRECATED_FIELD", "message": message, "severity": "info",
            "fields": list(fields), "replacement": replacement}


# N2: the seven always-null authority-boundary fields on evidence-only actions.
BOUNDARY_NULL_FIELDS = ["g2", "opportunity", "action", "priceTarget", "scenarioWeights", "selectedMethod", "selectedMultiple"]
AUTHORITY_BOUNDARY_DEPRECATION = _deprecated_field_warning(
    BOUNDARY_NULL_FIELDS,
    "decisionUse",
    f"{_field_list(BOUNDARY_NULL_FIELDS)} always null and {_REMOVAL}; decisionUse=EVIDENCE_ONLY marks the evidence boundary.",
)

# N3a: calculate_price_target_distance strategy labels.
PRICE_TARGET_LABEL_FIELDS = ["bracket", "inferredTag", "tag", "tagNote"]
PRICE_TARGET_LABEL_DEPRECATION = _deprecated_field_warning(
    PRICE_TARGET_LABEL_FIELDS,
    "priceAtOrAboveTarget",
    f"{_field_list(PRICE_TARGET_LABEL_FIELDS)} deprecated and {_REMOVAL}; use priceAtOrAboveTarget (direction); referenceTargetPct gives the price/target ratio.",
)

# N3b: the fixed $10M liquidity gate on check_volume_liquidity_threshold.
VOLUME_GATE_FIELDS = ["gatePass", "gateThresholdUsd"]
VOLUME_GATE_DEPRECATION = _deprecated_field_warning(
    VOLUME_GATE_FIELDS,
    "meetsMinimum",
    f"{_field_list(VOLUME_GATE_FIELDS)} deprecated and {_REMOVAL}; use meetsMinimum and pass min_adv_usd for a caller-chosen threshold.",
)

# N3b: the snapshot's copy of the fixed gate.
SNAPSHOT_GATE_FIELDS = ["volume.liquidityGatePass"]
SNAPSHOT_GATE_DEPRECATION = _deprecated_field_warning(
    SNAPSHOT_GATE_FIELDS,
    "volume.meetsMinimum",
    f"{_field_list(SNAPSHOT_GATE_FIELDS)} deprecated and {_REMOVAL}; use volume.meetsMinimum and pass min_adv_usd for a caller-chosen threshold.",
)

# N3c: analyze_position_signals input groups, one notice for all four.
POSITION_INPUT_GROUPS = ["t1_inputs", "t2_inputs", "t4_inputs", "t5_inputs"]
POSITION_INPUT_REPLACEMENTS = "analystInputs, pricePositionInputs, earningsInputs, technicalInputs"
POSITION_INPUT_GROUP_DEPRECATION = _deprecated_field_warning(
    POSITION_INPUT_GROUPS,
    POSITION_INPUT_REPLACEMENTS,
    f"{_field_list(POSITION_INPUT_GROUPS)} deprecated and {_REMOVAL}; use {POSITION_INPUT_REPLACEMENTS}.",
)


# F61 (2.27.0): analyze_options_flow_window's 2.4.4 aliases, until now announced only in fieldNotes.
OPTIONS_FLOW_ALIAS_FIELDS = ["ivPctile", "putVolVs10dAvg"]
OPTIONS_FLOW_ALIAS_DEPRECATION = _deprecated_field_warning(
    OPTIONS_FLOW_ALIAS_FIELDS,
    "ivVsRealizedRangePct, putVolPer1PctStockAdv",
    f"{_field_list(OPTIONS_FLOW_ALIAS_FIELDS)} deprecated and {_REMOVAL}; use ivVsRealizedRangePct, putVolPer1PctStockAdv.",
)

# F61 (2.27.0): get_analyst_rating_changes' 30-day copies, null for any other window.
RATING_WINDOW_FIELDS = ["upgrades30d", "downgrades30d", "initiations30d"]
RATING_WINDOW_DEPRECATION = _deprecated_field_warning(
    RATING_WINDOW_FIELDS,
    "upgrades, downgrades, initiations",
    f"{_field_list(RATING_WINDOW_FIELDS)} deprecated and {_REMOVAL}; use upgrades, downgrades, initiations, which count the windowDays window.",
)

# F20 (2.27.0): status strings carried in confidence.
STATUS_CONFIDENCE_DEPRECATION = _deprecated_field_warning(
    ["confidence"],
    "status",
    "confidence values other than HIGH, MEDIUM and LOW (status strings such as EXTRACTION_FAILED or NOT_DISCLOSED) are deprecated "
    f"and {_REMOVAL}; confidence will hold only HIGH, MEDIUM or LOW, and a status field names the limitation.",
)
_CONFIDENCE_LEVELS = {"HIGH", "MEDIUM", "LOW"}

# Python only (2.27.0). The Worker never emitted these keys, so it has no such notice (as N1 above, the other way round).
# F17: get_ownership_holders rows carry yfinance's column names; the major gives them the Worker's quoteSummary shape.
HOLDER_ROWS_REPLACEMENT = "the Worker's quoteSummary rows (camelCase keys)"
# F47: get_company_events_calendar's calendar carries yfinance's keys; the major gives it the Worker's keys.
CALENDAR_KEYS_REPLACEMENT = "calendar.earnings.earningsDate, calendar.earnings.*, calendar.dividendDate, calendar.exDividendDate"


def _yfinance_keys(keys) -> list[str]:
    """The keys that are not camelCase identifiers (yfinance column names such as "Value" or "Earnings Date")."""
    return sorted({k for k in keys if isinstance(k, str) and not re.fullmatch(r"[a-z][A-Za-z0-9]*", k)})


def holder_rows_deprecation(data: object) -> dict | None:
    if not isinstance(data, list):
        return None
    fields = _yfinance_keys(k for row in data if isinstance(row, dict) for k in row)
    if not fields:
        return None
    return _deprecated_field_warning(fields, HOLDER_ROWS_REPLACEMENT,
                                     f"{_field_list(fields)} deprecated and {_REMOVAL}; rows take {HOLDER_ROWS_REPLACEMENT}.")


def calendar_keys_deprecation(data: object) -> dict | None:
    calendar = data.get("calendar") if isinstance(data, dict) else None
    if not isinstance(calendar, dict):
        return None
    fields = [f"calendar.{k}" for k in _yfinance_keys(calendar)]
    if not fields:
        return None
    return _deprecated_field_warning(fields, CALENDAR_KEYS_REPLACEMENT,
                                     f"{_field_list(fields)} deprecated and {_REMOVAL}; use {CALENDAR_KEYS_REPLACEMENT}.")


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


def _has_path(rec: dict, path: str) -> bool:
    """True when ``path`` (dotted) resolves to a key of ``rec``."""
    node: object = rec
    for key in path.split("."):
        if not isinstance(node, dict) or key not in node:
            return False
        node = node[key]
    return True


def _has_any_field(rec: dict, fields: list[str]) -> bool:
    return any(_has_path(rec, field) for field in fields)


def _payload_has_any_field(data: object, fields: list[str]) -> bool:
    """Whether any listed field is in the payload, at the top level or, for a batch result,
    one level down (``{"AAPL": {...}, "MSFT": {...}}``) or under ``tickers`` (``get_market_snapshot``).
    Only keys are read, never values."""
    if not isinstance(data, dict):
        return False
    if _has_any_field(data, fields):
        return True
    batch = data["tickers"] if isinstance(data.get("tickers"), dict) else data
    return any(isinstance(entry, dict) and _has_any_field(entry, fields) for entry in batch.values())


def deprecation_warnings_for(tool: str, data: object, *, evidence_only: bool) -> list[dict]:
    """The DEPRECATED_FIELD notices for one successful tool response (fresh dicts, safe to mutate)."""
    out: list[dict] = []

    def notice(warning: dict) -> None:
        out.append({**warning, "fields": list(warning["fields"])})

    if evidence_only and isinstance(data, dict) and _has_any_field(data, BOUNDARY_NULL_FIELDS):
        notice(AUTHORITY_BOUNDARY_DEPRECATION)
    if tool == "calculate_price_target_distance":
        if _payload_has_any_field(data, PRICE_TARGET_LABEL_FIELDS):
            notice(PRICE_TARGET_LABEL_DEPRECATION)
    elif tool == "check_volume_liquidity_threshold":
        if _payload_has_any_field(data, VOLUME_GATE_FIELDS):
            notice(VOLUME_GATE_DEPRECATION)
    elif tool == "get_market_snapshot":
        if _payload_has_any_field(data, SNAPSHOT_GATE_FIELDS):
            notice(SNAPSHOT_GATE_DEPRECATION)
    elif tool == "analyze_position_signals":
        if _payload_has_any_field(data, POSITION_INPUT_GROUPS):
            notice(POSITION_INPUT_GROUP_DEPRECATION)
    elif tool == "analyze_options_flow_window":
        if _payload_has_any_field(data, OPTIONS_FLOW_ALIAS_FIELDS):
            notice(OPTIONS_FLOW_ALIAS_DEPRECATION)
    elif tool == "get_analyst_rating_changes":
        if _payload_has_any_field(data, RATING_WINDOW_FIELDS):
            notice(RATING_WINDOW_DEPRECATION)
    elif tool == "get_ownership_holders":
        warning = holder_rows_deprecation(data)
        if warning:
            notice(warning)
    elif tool == "get_company_events_calendar":
        warning = calendar_keys_deprecation(data)
        if warning:
            notice(warning)
    if payload_has_status_confidence(data):
        notice(STATUS_CONFIDENCE_DEPRECATION)
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
