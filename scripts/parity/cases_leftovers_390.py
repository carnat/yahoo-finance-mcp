"""Parity cases: the remaining swallowed reads are reported (3.9.0, swallowed-error leftovers items 1-3).

A failed read and a genuinely empty one are served the same way to both runtimes (a 500 and a 404) and must differ:

- extract_management_commentary: the payload carries a `warnings` list. An SEC release that cannot be read adds
  RELEASE_TEXT_NOT_AVAILABLE; a 404 or a release without the topic leaves `warnings` empty. Topic statuses are unchanged.
- get_filing_data, annual form: a companyfacts read that fails adds an info SEC_READ_FAILED warning with
  failedReads [{endpoint "companyfacts", concept null, httpStatus}]; a 404 adds nothing. (The submissions branch needs a
  read that fails only after a cached success, which no stub can serve; scripts/test_python_only_partial_warnings_390.py
  and the Worker typecheck cover it.)
- get_options_flow_scan (ADV10_UNAVAILABLE, MAX_PAIN_UNAVAILABLE): its Yahoo reads are not served by HTTP routes in both
  runtimes (Python goes through yfinance objects), so the ADV10 cases give each runtime its own failing read. MAX_PAIN_UNAVAILABLE
  needs a failing pure computation that no input can cause in the Worker: Python's branch is tested in
  scripts/test_python_only_partial_warnings_390.py and the Worker's try/catch is covered by the typecheck only.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pandas as pd

from scripts.parity.cases_major_300 import _FLOW_EXPIRY, _FLOW_EXPIRY_TS, QS, _contract
from scripts.parity.cases_pricing_evidence_tools import GATE_NOW, YAHOO_AUTH_ROUTES, _ts_with_clock
from scripts.parity.cases_sec_earnings import _doc_url, _run, _sec_routes, _srv, _unwrap
from scripts.parity.cases_sec_reads_380 import BASE_ROUTES, FACTS_URL, _py as _py380
from scripts.parity.harness import Case

import server  # noqa: E402
from yfmcp.tools import pricing  # noqa: E402

CASES: list[Case] = []
IDS = ["LEFTOVERS-390"]

# ── extract_management_commentary ─────────────────────────────────────────────────────────────────
_CIK, _TKR = 820001, "ZML"
_ACC = f"0000{_CIK:06d}-26-000001"
_DOC = _doc_url(_CIK, _ACC, "r8k.htm")
_HTML = "<html><body><p>Management said margins improved on supply chain gains in the quarter.</p></body></html>"
_UNREAD = {"code": "RELEASE_TEXT_NOT_AVAILABLE", "severity": "warning",
           "message": f"The earnings release {_DOC} could not be read from SEC; retry."}


def _commentary_view(v):
    d = _unwrap(v)
    return {"topics": [[t.get("topic"), t.get("status")] for t in d.get("topics") or []], "warnings": d.get("warnings"),
            "keys": sorted(d.keys())}


def _commentary_case(name: str, doc_route: dict, expect: dict) -> Case:
    routes = _sec_routes(_TKR, _CIK, [{"accession": _ACC, "date": "2026-07-14", "primary": "r8k.htm"}])
    routes[_DOC] = doc_route
    return Case(
        name=f"leftovers_390.commentary.{name}", ids=IDS,
        py=_run(lambda: _srv().extract_management_commentary(_TKR, "latest", ["margins"])),
        ts={"js": "m.response.setWorkerEnv({}); return m.yahooFinance.extractManagementCommentary(args[0], 'latest', ['margins']);",
            "args": [_TKR], "isolate": True},
        routes=routes, select=_commentary_view, expect=expect)


_KEYS = ["period", "ticker", "topics", "warnings"]  # compared sorted: the harness canonicalises key order
CASES += [
    _commentary_case("release_read_has_empty_warnings", {"body": _HTML},
                     {"topics": [["margins", "FOUND"]], "warnings": [], "keys": _KEYS}),
    _commentary_case("release_404_is_silent", {"status": 404},
                     {"topics": [["margins", "NOT_FOUND"]], "warnings": [], "keys": _KEYS}),
    _commentary_case("release_500_warns", {"status": 500},
                     {"topics": [["margins", "NOT_FOUND"]], "warnings": [_UNREAD], "keys": _KEYS}),
]

# ── annual-coverage check on get_filing_data ──────────────────────────────────────────────────────
_FD_ARGS = ("AAPL", "total_revenue", None, "10-K")


def _coverage_view(v):
    d = _unwrap(v)
    return [w for w in d.get("warnings") or [] if w.get("code") in ("SEC_READ_FAILED", "LATEST_ANNUAL_NOT_IN_COMPANYFACTS")]


def _fd_py():
    from yfmcp.schemas import FilingFactType
    return _py380(_srv().get_filing_data, _FD_ARGS[0], FilingFactType.total_revenue, None, "10-K")


# A revenue fact from an older 10-K than the latest one on file, so the fact read succeeds without companyfacts and the
# coverage check is the one to read companyfacts.
_CONCEPT = {"cik": 320193, "taxonomy": "us-gaap", "tag": "Revenues", "units": {"USD": [
    {"start": "2022-10-01", "end": "2023-09-30", "val": 383285000000, "accn": "0000320193-23-000106", "fy": 2023, "fp": "FY",
     "form": "10-K", "filed": "2023-11-03"}]}}


def _fd_case(name: str, facts_route: dict, expect: list) -> Case:
    return Case(
        name=f"leftovers_390.annual_coverage.{name}", ids=IDS, py=_fd_py(),
        ts={"module": "sec", "fn": "getFilingData", "args": list(_FD_ARGS), "isolate": True},
        routes={**BASE_ROUTES, FACTS_URL: facts_route, "https://data.sec.gov/api/xbrl/companyconcept/*": {"json": _CONCEPT}},
        select=_coverage_view, expect=expect)


_READ_FAILED = {"code": "SEC_READ_FAILED", "severity": "info",
                "message": "SEC data could not be read, so the latest-annual coverage check did not run; retry.",
                "failedReads": [{"endpoint": "companyfacts", "concept": None, "httpStatus": 500}]}
CASES += [
    _fd_case("companyfacts_500_adds_info_warning", {"status": 500}, [_READ_FAILED]),
    _fd_case("companyfacts_404_is_silent", {"status": 404}, []),
]


# ── get_options_flow_scan: ADV10_UNAVAILABLE ──────────────────────────────────────────────────────
class _FastInfo(dict):
    """Python's fast_info: the price reads, the 10-day average volume read fails (or is legitimately absent)."""

    def __init__(self, adv10: str):
        super().__init__(lastPrice=100.0)
        self._adv10 = adv10

    def __getitem__(self, key):
        if key == "tenDayAverageVolume":
            if self._adv10 == "fail":
                raise RuntimeError("fast info unavailable")
            return None
        return super().__getitem__(key)


def _adv10_case(name: str, adv10: str, expect: dict) -> Case:
    strikes = [95.0, 100.0, 105.0]
    calls = [_contract(s, "C") for s in strikes]
    puts = [_contract(s, "P") for s in strikes]

    class _Ticker:
        fast_info = _FastInfo(adv10)
        options = (_FLOW_EXPIRY,)

        def option_chain(self, expiry):
            return SimpleNamespace(calls=pd.DataFrame(calls), puts=pd.DataFrame(puts))

    def py():
        with patch.object(server.yf, "Ticker", lambda _t: _Ticker()), \
                patch.object(pricing, "_load_completed_daily_history", AsyncMock(side_effect=RuntimeError("no history"))), \
                patch.object(pricing, "get_last_trading_date", lambda: "2026-10-08"):
            return json.loads(asyncio.run(server.get_options_flow_scan("MJRFLOW", "audit")))

    chain = {"optionChain": {"result": [{"expirationDates": [_FLOW_EXPIRY_TS], "options": [{"expirationDate": _FLOW_EXPIRY_TS,
                                                                                           "calls": calls, "puts": puts}]}]}}
    price = {"quoteSummary": {"result": [{"price": {"regularMarketPrice": {"raw": 100.0}}}]}}
    # The Worker reads the price with modules=price and the 10-day average with the longer module list.
    fast = ({"status": 500} if adv10 == "fail"
            else {"json": {"quoteSummary": {"result": [{"price": {"regularMarketPrice": {"raw": 100.0}}, "summaryDetail": {},
                                                          "defaultKeyStatistics": {}}]}}})
    return Case(
        name=f"leftovers_390.options_flow.{name}", ids=IDS, py=py,
        ts={"js": _ts_with_clock(GATE_NOW, 'return JSON.parse(await m.yahooFinance.getOptionsFlowScan("MJRFLOW", "audit"));'),
            "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, "https://query2.finance.yahoo.com/v7/finance/options/MJRFLOW?*": {"json": chain},
                f"{QS}MJRFLOW?*": {"json": price}, f"{QS}MJRFLOW?modules=price,summaryDetail*": fast},
        select=lambda v: {"warnings": [w for w in v["warnings"] if w.startswith(("ADV10", "MAX_PAIN"))],
                          "putVolPer1PctStockAdv": v["putVolPer1PctStockAdv"]}, expect=expect)


CASES += [
    _adv10_case("adv10_read_failed_warns", "fail", {"warnings": ["ADV10_UNAVAILABLE"], "putVolPer1PctStockAdv": None}),
    _adv10_case("adv10_absent_is_silent", "absent", {"warnings": [], "putVolPer1PctStockAdv": None}),
]
