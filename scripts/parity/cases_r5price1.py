"""Parity cases for R5-pricing-1 (3.0.8) that are not tied to one tool.

The CAGR horizon now starts on a calendar date N years back (BR-PRC-021, Q3i G5). Both runtimes compute that date with
a shared helper: Python yfmcp.util._add_years, the Worker util.addIsoYears. This case checks the two against Python's
datetime for 1, 3 and 5 years back from every end of February and the 1st and 15th of each month, 1896 to 2404, so the
29 February rule (28 February in a common year) and the century rules hold in both.
"""

from __future__ import annotations

import datetime as _dt

from scripts.parity.harness import Case
from yfmcp.util import _add_years

CASES: list[Case] = []

_OFFSETS = (-1, -3, -5, 1)


def _dates() -> list[str]:
    out: list[str] = []
    for y in range(1896, 2405):
        for m in range(1, 13):
            out += [f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-15"]
        out += [f"{y:04d}-02-27", f"{y:04d}-02-28", f"{y:04d}-03-01"]
        if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0):
            out.append(f"{y:04d}-02-29")
    return out


_DATES = _dates()


def _oracle(date: str, years: int) -> str:
    d = _dt.date.fromisoformat(date)
    try:
        return d.replace(year=d.year + years).isoformat()
    except ValueError:
        return d.replace(year=d.year + years, day=28).isoformat()


def _mismatches(rows: list[list[str]]) -> dict:
    bad = [[d, n, got] for d, row in zip(_DATES, rows) for n, got in zip(_OFFSETS, row) if got != _oracle(d, n)]
    return {"checked": sum(len(r) for r in rows), "mismatches": len(bad), "first": bad[:5]}


CASES.append(Case(
    name="r5price1.cagr_cutoff_date_matches_calendar", ids=["BR-PRC-021"],
    py=lambda: [[_add_years(d, n) for n in _OFFSETS] for d in _DATES],
    ts={"js": "return args[0].map((d) => args[1].map((n) => m.util.addIsoYears(d, n)));", "args": [_DATES, list(_OFFSETS)]},
    select=_mismatches, expect={"checked": len(_DATES) * len(_OFFSETS), "mismatches": 0, "first": []},
    note="R5-pricing-1: the CAGR cutoff date is a real calendar date, the same in both runtimes",
))


# ── Q3b: a non-numeric `days` is refused by get_price_slope in both runtimes ─────────────────────────────────────
# Python raised ValueError from int("abc"); the Worker silently used 5 days. Both now refuse before any fetch.
import asyncio as _asyncio  # noqa: E402
import json as _json  # noqa: E402

from yfmcp.tools import pricing as _pricing  # noqa: E402


def _failure_view(v) -> dict:
    v = _json.loads(v) if isinstance(v, str) else v
    err = (v or {}).get("error") or {}
    return {"ok": (v or {}).get("ok"), "code": err.get("code"), "message": err.get("message")}


for _label, _days in (("letters", "abc"), ("decimal_string", "2.5"), ("list", [5])):
    CASES.append(Case(
        name=f"r5price1.slope.non_numeric_days_refused_{_label}", ids=["BR-PRC-014", "Q3b"],
        py=(lambda d=_days: _asyncio.run(_pricing.get_price_slope("TST", d))),
        ts={"js": "return await m.tools.callTool('get_price_slope', {ticker: 'TST', days: args[0]});", "args": [_days], "isolate": True},
        select=_failure_view,
        expect={"ok": False, "code": "INPUT_VALIDATION_ERROR", "message": "days must be a whole number of sessions"},
    ))
