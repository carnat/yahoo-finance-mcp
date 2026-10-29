"""Pricing notices (R5-pricing-2, 3.1.0; names from Q11).

A tool payload carries its notices under the internal key ``_warnings``; the envelope moves them to meta.warnings
(lift_payload_warnings), and get_market_snapshot folds its components' notices into its own data.warnings with the
component name. Worker: worker/src/pricing-notices.ts.
"""

from __future__ import annotations

import json

ZERO_VOLUME_NO_RATIO = "ZERO_VOLUME_NO_RATIO"
INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
SESSION_CALENDAR_UNKNOWN = "SESSION_CALENDAR_UNKNOWN"

PAYLOAD_WARNINGS_KEY = "_warnings"


def zero_volume_notice(ticker: str) -> dict:
    return {
        "code": ZERO_VOLUME_NO_RATIO,
        "message": "The latest completed session traded zero volume, so ratio10d, ratio90d and volumeFlag are null.",
        "ticker": ticker,
    }


def insufficient_history_notice(ticker: str, years: int) -> dict:
    return {
        "code": INSUFFICIENT_HISTORY,
        "message": f"Price history starts after the start of the {years}-year window, so cagr{years}y is null.",
        "ticker": ticker,
    }


def session_calendar_notice(ticker: str) -> dict:
    return {
        "code": SESSION_CALENDAR_UNKNOWN,
        "message": (
            "No holiday calendar covers this exchange and date, so the previous session could not be named and "
            "freshnessStatus is UNKNOWN."
        ),
        "ticker": ticker,
    }


def freshness_notices(ticker: str, freshness_reason: object) -> list[dict]:
    """Notices for a payload built from prepared daily history: SESSION_CALENDAR_UNKNOWN when its reason says so."""
    return [session_calendar_notice(ticker)] if freshness_reason == SESSION_CALENDAR_UNKNOWN else []


def with_notices(payload: dict, notices: list[dict]) -> dict:
    """Sets ``_warnings`` on a payload when there are notices; leaves the payload unchanged otherwise."""
    if notices:
        payload[PAYLOAD_WARNINGS_KEY] = notices
    return payload


def unique_notices(notices: list) -> list:
    """Notices without repeats (same code, ticker and message), in first-seen order."""
    seen: set = set()
    out = []
    for notice in notices:
        key = json.dumps([notice.get("code"), notice.get("ticker"), notice.get("message")]) if isinstance(notice, dict) \
            else json.dumps(notice, default=str)
        if key not in seen:
            seen.add(key)
            out.append(notice)
    return out


def take_notices(payload: object) -> list:
    """Removes ``_warnings`` from a payload and returns them (component payloads folded by the snapshot)."""
    if not isinstance(payload, dict):
        return []
    notices = payload.pop(PAYLOAD_WARNINGS_KEY, None)
    return list(notices) if isinstance(notices, list) else []


def lift_payload_warnings(data: object) -> list:
    """Moves every ``_warnings`` list in a tool's data into one list, in payload order: the data itself, each of its
    object values (a batch keyed by ticker) and each value of those (a batch under ``tickers``)."""
    lifted: list = []

    def visit(value: object, depth: int) -> None:
        if not isinstance(value, dict):
            return
        if PAYLOAD_WARNINGS_KEY in value:
            own = value.pop(PAYLOAD_WARNINGS_KEY)
            if isinstance(own, list):
                lifted.extend(own)
        if depth < 2:
            for child in list(value.values()):
                visit(child, depth + 1)

    visit(data, 0)
    return lifted
