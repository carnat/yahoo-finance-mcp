"""The caller-chosen liquidity minimum (2.8.0, N3b) and the reliabilityClass table. Mirrors worker/src/deprecations.ts.

The 2.8.0 and 2.27.0 field deprecations and their DEPRECATED_FIELD notices were removed in 3.0.0; see
docs/provider-runtime-guidance.md "Breaking Changes (3.0.0)".

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
