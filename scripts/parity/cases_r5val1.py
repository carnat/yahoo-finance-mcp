"""Cases for R5-valuation part 1 (3.2.1) that have no older case to flip.

- BR-VAL-068: a release sentence naming a calendar quarter is matched against the quarter of the date a week before
  the period end (fiscal_calendar's 52/53-week drift rule), so a quarter ending January 1-7 is Q4 of the year before.
  Before, ceil(month / 3) of the raw end made it Q1 of the new year: "fourth quarter of 2026" was rejected and
  "first quarter of 2027" accepted for the quarter ended 2027-01-02.

The tolerance input, AGREED wording and convertible-basis repairs flip their cases in cases_valuation.py.
"""

from __future__ import annotations

from scripts.parity.harness import Case
from yfmcp import metric_reconciliation as mr

CASES: list[Case] = []
add = CASES.append

_RELEASE = {"status": "READ", "url": "https://www.sec.gov/Archives/edgar/data/1/x.htm", "filingDate": "2027-02-01",
            "accessionNumber": "0000000001-27-000001"}
_Q4_53 = {"periodType": "QUARTER", "periodStart": "2026-10-04", "periodEnd": "2027-01-02"}
_Q2 = {"periodType": "QUARTER", "periodStart": "2026-03-29", "periodEnd": "2026-06-27"}
_SENTENCES = [
    ("Revenue for the fourth quarter of 2026 was $1.2 billion.", _Q4_53),
    ("Revenue for Q4 2026 was $1.2 billion.", _Q4_53),
    ("Revenue for the first quarter of 2027 was $1.2 billion.", _Q4_53),
    ("Revenue for the second quarter of 2026 was $1.2 billion.", _Q2),
    ("Revenue for the third quarter of 2026 was $1.2 billion.", _Q2),
]


def _view(rows: list) -> list:
    return [[r["status"], r.get("value")] for r in rows]


add(Case(name="r5val1.br_val_068.calendar_quarter_follows_the_week_drift_rule", ids=["BR-VAL-068"],
         py=lambda: [mr.release_observation({**_RELEASE, "text": t}, "revenue", p, "USD") for t, p in _SENTENCES],
         ts={"js": "return args[0].map(([t, p]) => m.metricReconciliation.releaseObservation({...args[1], text: t}, 'revenue', p, 'USD'));",
             "args": [[list(x) for x in _SENTENCES], _RELEASE]},
         select=_view,
         expect=[["FOUND", 1_200_000_000], ["FOUND", 1_200_000_000], ["NOT_FOUND_IN_TEXT", None],
                 ["FOUND", 1_200_000_000], ["NOT_FOUND_IN_TEXT", None]],
         note="R5-valuation part 1: the quarter ended 2027-01-02 is Q4 2026, not Q1 2027"))


# ── get_valuation_snapshot: a quote-summary failure (the F57 rule of compare_peer_valuations, 3.2.1) ──
import asyncio  # noqa: E402
from unittest.mock import AsyncMock, patch  # noqa: E402

import server  # noqa: E402
from scripts.parity.cases_audit_2265 import YAHOO_AUTH_ROUTES  # noqa: E402
from yfmcp.envelope import _envelope_tool_result  # noqa: E402
from yfmcp.tools import valuation as valuation_tools  # noqa: E402


def _py_snapshot(message: str):
    def run():
        with patch.object(valuation_tools, "_valuation_market_inputs", AsyncMock(side_effect=RuntimeError(message))):
            return _envelope_tool_result("get_valuation_snapshot", asyncio.run(server.get_valuation_snapshot("zzzzqq")))
    return run


def _error_view(v: dict) -> dict:
    return {"ok": v["ok"], "code": v["error"]["code"], "message": v["error"]["message"]}


_TS_SNAPSHOT = ('m.response.setWorkerEnv({}); return (await m.tools.callToolResult("get_valuation_snapshot", '
                '{ticker: "zzzzqq"})).value;')
for _key, _status, _expect in [
    ("unknown_ticker_is_ticker_not_found", 404,
     {"ok": False, "code": "TICKER_NOT_FOUND", "message": "No provider data was found for the requested ticker."}),
    ("provider_failure_has_no_url", 500,
     {"ok": False, "code": "PROVIDER_ERROR", "message": "Yahoo quote summary failed for ZZZZQQ: Yahoo Finance API error 500 for"}),
]:
    add(Case(name=f"r5val1.valuation_snapshot.{_key}", ids=["F57", "BR-VAL-045"],
             py=_py_snapshot(f"Yahoo Finance API error {_status} for: https://query1.finance.yahoo.com/v10/finance/quoteSummary/ZZZZQQ?modules=price"),
             ts={"js": _TS_SNAPSHOT, "isolate": True},
             routes={**YAHOO_AUTH_ROUTES, "https://query1.finance.yahoo.com/v*": {"status": _status, "body": "x"},
                     "https://query2.finance.yahoo.com/v*": {"status": _status, "body": "x"}},
             select=_error_view, expect=_expect,
             note="R5-valuation part 1: get_valuation_snapshot fails as compare_peer_valuations does, with no provider URL"))
