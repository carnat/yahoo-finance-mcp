"""Neutral replacement labels (3.11.0, neutrality audit NL1-NL7). Mirrors worker/src/labels.ts.

Each label is a factual classification or a band with a documented threshold the caller can override; the labels
they replace are deprecated and removed in 4.0.0.

This module stays import-free of the rest of yfmcp, like worker/src/labels.ts.
"""

from __future__ import annotations

import math

# NL1: price against both moving averages. ABOVE means at or above (pct >= 0), as regime50/regime200.


def ma_alignment(regime50: str | None, regime200: str | None) -> str | None:
    if regime50 is None or regime200 is None:
        return None
    if regime50 == "ABOVE" and regime200 == "ABOVE":
        return "ABOVE_BOTH"
    if regime50 == "BELOW" and regime200 == "BELOW":
        return "BELOW_BOTH"
    return "MIXED"


# NL3: the default revision band, in percent (the band forwardRevisionSignal has always used).
DEFAULT_REVISION_THRESHOLD_PCT = 3


def revision_band(
    revision30d: float | None,
    revision7d: float | None,
    revision90d: float | None,
    threshold_pct: float = DEFAULT_REVISION_THRESHOLD_PCT,
) -> tuple[str | None, str]:
    """NL3: the EPS revision band as ``(band, driver)``. 30d is primary; when 30d is inside the band, 7d confirms
    (driver 30d_neutral_7d); with no 30d, 7d, then 90d as a fallback. A change of at least ``threshold_pct`` either
    way is UP or DOWN. band is None when no revision is known (driver "none")."""

    def band_of(value: float) -> str:
        if value <= -threshold_pct:
            return "DOWN"
        return "UP" if value >= threshold_pct else "FLAT"

    if revision30d is not None:
        primary = band_of(revision30d)
        if primary != "FLAT":
            return primary, "30d"
        if revision7d is not None:
            confirm = band_of(revision7d)
            if confirm != "FLAT":
                return confirm, "30d_neutral_7d"
        return "FLAT", "30d"
    if revision7d is not None:
        return band_of(revision7d), "7d"
    if revision90d is not None:
        return band_of(revision90d), "90d_fallback"
    return None, "none"


def beat_rate_band(beat_rate: float | None) -> str | None:
    """NL3: beatRate (0-1) in four bands, lower bound inclusive."""
    if beat_rate is None:
        return None
    if beat_rate >= 0.75:
        return "75_TO_100PCT"
    if beat_rate >= 0.55:
        return "55_TO_75PCT"
    if beat_rate >= 0.4:
        return "40_TO_55PCT"
    return "0_TO_40PCT"


def leverage_band(net_debt_to_ebitda: float | None, negative_earnings_base: bool) -> str | None:
    """NL6: net debt / operational EBITDA in bands (upper bound inclusive above 1x), or NEGATIVE_EARNINGS_BASE for a
    loss-maker."""
    if net_debt_to_ebitda is not None:
        if net_debt_to_ebitda < 1:
            return "BELOW_1X"
        if net_debt_to_ebitda <= 2.5:
            return "1X_TO_2_5X"
        if net_debt_to_ebitda <= 4:
            return "2_5X_TO_4X"
        return "ABOVE_4X"
    return "NEGATIVE_EARNINGS_BASE" if negative_earnings_base else None


# NL6: the default credit-stress thresholds (the fixed ones creditStressFlag has always used).
DEFAULT_MAX_NET_DEBT_TO_EBITDA = 2.5
DEFAULT_MIN_INTEREST_COVERAGE = 3


def meets_credit_stress_threshold(
    net_debt_to_ebitda: float | None,
    interest_coverage_ebit: float | None,
    max_net_debt_to_ebitda: float = DEFAULT_MAX_NET_DEBT_TO_EBITDA,
    min_interest_coverage: float = DEFAULT_MIN_INTEREST_COVERAGE,
) -> bool | None:
    """NL6: netDebtToEbitda above the maximum and EBIT interest coverage below the minimum; None when either is
    unknown."""
    if net_debt_to_ebitda is None or interest_coverage_ebit is None:
        return None
    return net_debt_to_ebitda > max_net_debt_to_ebitda and interest_coverage_ebit < min_interest_coverage


def target_vs_price(target_mean: float | None, last_price: float | None) -> str | None:
    """NL7: the mean consensus target against the current price."""
    if target_mean is None or last_price is None:
        return None
    return "AT_OR_ABOVE" if target_mean >= last_price else "BELOW"


def parse_positive_number_arg(name: str, value: object) -> tuple[float | None, str | None]:
    """An optional positive-number argument as ``(value, error)``: ``(None, None)`` when not supplied, ``(number,
    None)`` when valid, ``(None, message)`` for a non-numeric, non-finite or non-positive value (a bool is not a
    number; the min_adv_usd rule)."""
    if value is None:
        return None, None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        return None, f"{name} must be a positive number when supplied."
    return value, None
