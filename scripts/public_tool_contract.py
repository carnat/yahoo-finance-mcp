"""Shared LLM-facing tool discovery contract checks.

The same semantic checks run against Worker source before merge and against
the deployed ``tools/list`` response after deployment. Keeping one contract
prevents a description or output-schema change from passing CI only to fail
the post-deploy smoke test.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any


# Each inner tuple is an any-of concept group; every group must be represented.
DESCRIPTION_CONCEPTS: dict[str, tuple[tuple[str, ...], ...]] = {
    "get_fund_profile": (("fund", "etf"), ("holdings", "allocation", "sections")),
    "analyze_financial_ratios": (("financial ratios", "valuation"), ("historical", "history")),
    "get_earnings_analysis": (("earnings", "eps"), ("revision",)),
    "analyze_share_count_trend": (("dilution", "shares-outstanding"), ("sec", "filing")),
    "get_company_events_calendar": (("earnings",), ("unverified", "estimate")),
    "get_market_calendar": (("market-wide",), ("earnings", "economic")),
    "calculate_price_target_distance": (("reference price target", "user-supplied"),),
    "analyze_position_signals": (("does not access holdings",),),
    "check_volume_liquidity_threshold": (("liquidity thresholds",),),
}


OUTPUT_SCHEMA_FIELDS: dict[str, tuple[str, ...]] = {
    "get_fund_profile": (
        "ticker", "sectionsRequested", "sectionStatus", "topHoldings",
        "fundOperations", "decisionGrade", "recommendedNextAction",
    ),
    "analyze_financial_ratios": (
        "ticker", "trailingPE", "freeCashflowYield", "valuationHistory",
        "valuationFrequency", "decisionGrade",
    ),
    "get_earnings_analysis": (
        "ticker", "earningsEstimate", "epsTrend", "epsRevisions",
        "earningsHistory", "decisionGrade",
    ),
    "analyze_share_count_trend": (
        "ticker", "currentShares", "changePct", "observations",
        "dataDate", "decisionGrade", "recommendedNextAction",
    ),
    "get_company_events_calendar": (
        "ticker", "mode", "items", "calendar", "confirmationStatus",
        "decisionGrade", "recommendedNextAction",
    ),
    "get_market_calendar": (
        "eventType", "items", "coverage", "confirmationStatus",
        "decisionGrade", "recommendedNextAction",
    ),
}

CONTEXTUAL_BASE_FIELDS = {"source", "evidenceClass", "decisionGrade", "recommendedNextAction"}


WORKER_SCHEMA_CONSTANTS = {
    "get_fund_profile": "FUND_PROFILE_OUTPUT_SCHEMA",
    "analyze_financial_ratios": "FINANCIAL_RATIOS_OUTPUT_SCHEMA",
    "get_earnings_analysis": "EARNINGS_ANALYSIS_OUTPUT_SCHEMA",
    "analyze_share_count_trend": "SHARE_COUNT_TREND_OUTPUT_SCHEMA",
    "get_company_events_calendar": "COMPANY_CALENDAR_OUTPUT_SCHEMA",
    "get_market_calendar": "MARKET_CALENDAR_OUTPUT_SCHEMA",
}

PYTHON_SCHEMA_KEYS = {
    "get_fund_profile": "get_etf_info",
    "analyze_financial_ratios": "get_financial_ratios",
    "get_earnings_analysis": "get_earnings_analysis",
    "analyze_share_count_trend": "analyze_share_count_trend",
    "get_company_events_calendar": "get_calendar",
    "get_market_calendar": "get_market_calendar",
}

PYTHON_SCHEMA_CONSTANTS = {
    "get_fund_profile": "_FUND_PROFILE_OUTPUT_SCHEMA",
    "analyze_financial_ratios": "_FINANCIAL_RATIOS_OUTPUT_SCHEMA",
    "get_earnings_analysis": "_EARNINGS_ANALYSIS_OUTPUT_SCHEMA",
    "analyze_share_count_trend": "_SHARE_COUNT_TREND_OUTPUT_SCHEMA",
    "get_company_events_calendar": "_COMPANY_CALENDAR_OUTPUT_SCHEMA",
    "get_market_calendar": "_MARKET_CALENDAR_OUTPUT_SCHEMA",
}


def _description_errors(descriptions: Mapping[str, str]) -> list[str]:
    errors: list[str] = []
    for name, concept_groups in DESCRIPTION_CONCEPTS.items():
        description = descriptions.get(name, "")
        if not description:
            errors.append(f"Missing description for {name}")
            continue
        lower = description.lower()
        for alternatives in concept_groups:
            if not any(phrase.lower() in lower for phrase in alternatives):
                errors.append(f"{name}: description missing one of {alternatives!r}")
    return errors


def _data_properties(output_schema: Any) -> Mapping[str, Any]:
    if not isinstance(output_schema, Mapping):
        return {}
    properties = output_schema.get("properties")
    if not isinstance(properties, Mapping):
        return {}
    data_schema = properties.get("data")
    if isinstance(data_schema, Mapping):
        direct = data_schema.get("properties")
        if isinstance(direct, Mapping):
            return direct
        alternatives = data_schema.get("anyOf")
        if isinstance(alternatives, Sequence):
            for alternative in alternatives:
                if isinstance(alternative, Mapping) and isinstance(alternative.get("properties"), Mapping):
                    return alternative["properties"]
    return properties


def validate_live_tool_contract(tools: Sequence[Mapping[str, Any]]) -> None:
    """Validate descriptions and exposed output schemas from MCP ``tools/list``."""
    by_name = {str(tool.get("name")): tool for tool in tools}
    errors = _description_errors({name: str(tool.get("description", "")) for name, tool in by_name.items()})
    for name, expected_fields in OUTPUT_SCHEMA_FIELDS.items():
        tool = by_name.get(name)
        if tool is None:
            errors.append(f"Missing tool {name}")
            continue
        fields = _data_properties(tool.get("outputSchema"))
        missing = [field for field in expected_fields if field not in fields]
        if missing:
            errors.append(f"{name}: output schema missing {missing}")
    if errors:
        raise AssertionError("; ".join(errors))


def _worker_description(source: str, name: str) -> str:
    pattern = re.compile(
        rf'name:\s*"{re.escape(name)}"[\s\S]{{0,900}}?description:\s*"((?:\\.|[^"\\])*)"',
    )
    match = pattern.search(source)
    if not match:
        return ""
    try:
        return json.loads(f'"{match.group(1)}"')
    except json.JSONDecodeError:
        return match.group(1)


def _balanced_object_after(source: str, pattern: str) -> str:
    match = re.search(pattern, source)
    if not match:
        return ""
    start = source.find("{", match.end())
    if start < 0:
        return ""
    depth = 0
    quote = ""
    escaped = False
    for index in range(start, len(source)):
        char = source[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char in {'"', "'", "`"}:
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start:index + 1]
    return ""


def validate_worker_source_contract(source: str) -> None:
    """Validate the Worker manifest before deployment."""
    descriptions = {name: _worker_description(source, name) for name in DESCRIPTION_CONCEPTS}
    errors = _description_errors(descriptions)
    for marker in ("LLM_DETAILED_OUTPUT_TOOLS", "envelopedToolOutputSchema", "data: contextualData"):
        if marker not in source:
            errors.append(f"Worker detailed envelope schema path missing {marker!r}")
    detailed_match = re.search(
        r"const\s+LLM_DETAILED_OUTPUT_TOOLS\s*=\s*new\s+Set\s*\(\s*\[([\s\S]*?)\]\s*\)",
        source,
    )
    detailed_block = detailed_match.group(1) if detailed_match else ""
    contextual_block = _balanced_object_after(source, r"function\s+contextualOutputSchema\s*\([^)]*\)")
    missing_contextual = [
        field for field in CONTEXTUAL_BASE_FIELDS
        if re.search(rf'\b{re.escape(field)}\s*:', contextual_block) is None
    ]
    if missing_contextual:
        errors.append(f"Worker contextual output schema missing {missing_contextual}")
    for canonical, expected_fields in OUTPUT_SCHEMA_FIELDS.items():
        schema_constant = WORKER_SCHEMA_CONSTANTS[canonical]
        block = _balanced_object_after(
            source,
            rf"const\s+{re.escape(schema_constant)}\s*=\s*contextualOutputSchema\s*\(",
        )
        if not block:
            errors.append(f"Worker output schema missing for {canonical} ({schema_constant})")
            continue
        missing = [
            field for field in expected_fields
            if field not in CONTEXTUAL_BASE_FIELDS
            and re.search(rf'\b{re.escape(field)}\s*:', block) is None
        ]
        if missing:
            errors.append(f"{canonical}: Worker output schema missing {missing}")
        mapping_pattern = rf"\b{re.escape(canonical)}\s*:\s*{re.escape(schema_constant)}\b"
        if re.search(mapping_pattern, source) is None:
            errors.append(f"{canonical}: Worker schema mapping missing -> {schema_constant}")
        if re.search(rf'["\']{re.escape(canonical)}["\']', detailed_block) is None:
            errors.append(f"{canonical}: missing from Worker detailed envelope tool set")
    if errors:
        raise AssertionError("; ".join(errors))


def validate_python_schema_source(source: str) -> None:
    """Validate the Python mirror statically so lightweight CI needs no MCP imports."""
    errors: list[str] = []
    contextual_block = _balanced_object_after(source, r"def\s+_contextual_output_schema\s*\([^)]*\)")
    missing_contextual = [
        field for field in CONTEXTUAL_BASE_FIELDS
        if re.search(rf'["\']{re.escape(field)}["\']\s*:', contextual_block) is None
    ]
    if missing_contextual:
        errors.append(f"Python contextual output schema missing {missing_contextual}")

    for canonical, expected_fields in OUTPUT_SCHEMA_FIELDS.items():
        schema_constant = PYTHON_SCHEMA_CONSTANTS[canonical]
        block = _balanced_object_after(
            source,
            rf"{re.escape(schema_constant)}\s*=\s*_contextual_output_schema\s*\(",
        )
        if not block:
            errors.append(f"Python output schema missing for {canonical} ({schema_constant})")
            continue
        missing = [
            field for field in expected_fields
            if field not in CONTEXTUAL_BASE_FIELDS
            and re.search(rf'["\']{re.escape(field)}["\']\s*:', block) is None
        ]
        if missing:
            errors.append(f"{canonical}: Python output schema missing {missing}")

        schema_key = PYTHON_SCHEMA_KEYS[canonical]
        mapping_pattern = rf'["\']{re.escape(schema_key)}["\']\s*:\s*{re.escape(schema_constant)}\b'
        if re.search(mapping_pattern, source) is None:
            errors.append(f"{canonical}: Python schema mapping missing {schema_key!r} -> {schema_constant}")

    if errors:
        raise AssertionError("; ".join(errors))
