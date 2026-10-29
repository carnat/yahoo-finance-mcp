"""Exchange holiday calendars (BR-PRC-002, 3.1.0).

The closures live in worker/src/exchange-calendars.json, which the Worker compiles in, so both runtimes name the same
previous session. An exchange without a calendar, or a date outside the covered years, has no known previous session.
"""

from __future__ import annotations

import datetime
import json
import re
from pathlib import Path

_PATH = Path(__file__).resolve().parents[1] / "worker" / "src" / "exchange-calendars.json"
CALENDARS: dict = json.loads(_PATH.read_text(encoding="utf-8"))["calendars"]
_BY_EXCHANGE: dict[str, dict] = {
    code: {**calendar, "closureSet": frozenset(calendar["closures"])}
    for calendar in CALENDARS.values()
    for code in calendar["exchanges"]
}


def previous_exchange_session(exchange_name: object, iso_date: str) -> str | None:
    """The exchange session before an ISO date, or None when the exchange has no calendar or the date is not covered."""
    calendar = _BY_EXCHANGE.get(exchange_name) if isinstance(exchange_name, str) else None
    if calendar is None or not isinstance(iso_date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", iso_date):
        return None
    try:
        day = datetime.date.fromisoformat(iso_date)
    except (TypeError, ValueError):
        return None
    for _ in range(14):
        day -= datetime.timedelta(days=1)
        if not calendar["firstYear"] <= day.year <= calendar["lastYear"]:
            return None
        if day.weekday() < 5 and day.isoformat() not in calendar["closureSet"]:
            return day.isoformat()
    return None
