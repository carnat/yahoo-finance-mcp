"""Parity cases: a skipped option expiry is reported (3.8.2, swallowed-error audit row 3.8.2).

get_put_hedge_candidates reads the nearest two qualifying expiries. A failed read used to be skipped silently:

- some expiries read, some failed: the result as before, status PARTIAL, and the warnings list gains
  OPTION_CHAIN_READ_FAILED once;
- every attempted expiry failed: the tool's bare error {error, message, ticker} "No option chain could be read; retry.";
  it no longer answers "No put options found";
- every read succeeded but no put qualifies: unchanged (no warning; the empty chains already give PARTIAL through
  NO_CONTRACTS_RETURNED).
"""

from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from scripts.parity.cases_pricing_evidence_tools import YAHOO_AUTH_ROUTES
from scripts.parity.harness import Case

import server  # noqa: E402
from yfmcp.tools import pricing  # noqa: E402

CASES: list[Case] = []
IDS = ["OPTION-READS-382"]

QS = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/"
OPT = "https://query2.finance.yahoo.com/v7/finance/options/HDGTST"
EXPIRIES = ["2098-12-18", "2099-01-15", "2099-02-19"]
AFTER = "2099-01-01"  # the two expiries read are 2099-01-15 and 2099-02-19
FAIL = object()
NO_PUTS = object()


def _ts(date: str) -> int:
    return int(pd.Timestamp(date, tz="UTC").timestamp())


def _put(strike: float, expiry: str) -> dict:
    return {"contractSymbol": f"HDG{expiry}P{int(strike)}", "strike": strike, "lastPrice": 2.0, "bid": 1.9, "ask": 2.1,
            "volume": 300, "openInterest": 1000, "impliedVolatility": 0.4, "lastTradeDate": int(time.time()),
            "inTheMoney": False, "currency": "USD", "expiration": _ts(expiry)}


def _puts(expiry: str, mode) -> list[dict]:
    return [] if mode is NO_PUTS else [_put(s, expiry) for s in (90.0, 91.0)]  # 8-12% below 100


def _case(name: str, modes: dict, expect: dict) -> Case:
    """modes maps an expiry to FAIL, NO_PUTS or None (a chain with two qualifying puts)."""

    class _Ticker:
        fast_info = SimpleNamespace(last_price=100.0)
        options = tuple(EXPIRIES)

        def option_chain(self, expiry):
            mode = modes.get(expiry)
            if mode is FAIL:
                raise RuntimeError("yahoo down")
            return SimpleNamespace(calls=pd.DataFrame(), puts=pd.DataFrame(_puts(expiry, mode), columns=list(_put(0.0, expiry))))

    def py():
        with patch.object(server.yf, "Ticker", lambda _t: _Ticker()):
            return json.loads(asyncio.run(pricing.get_put_hedge_candidates("HDGTST", 8.0, 12.0, 500.0, AFTER)))

    routes = {**YAHOO_AUTH_ROUTES,
              f"{QS}HDGTST?*": {"json": {"quoteSummary": {"result": [{"price": {"regularMarketPrice": {"raw": 100.0}}}]}}}}
    base = {"optionChain": {"result": [{"expirationDates": [_ts(e) for e in EXPIRIES], "options": [{"calls": [], "puts": _puts(EXPIRIES[0], None)}]}]}}
    routes[f"{OPT}?*"] = {"json": base}
    for e in EXPIRIES:
        if e == EXPIRIES[0]:
            continue
        mode = modes.get(e)
        routes[f"{OPT}?date={_ts(e)}*"] = (
            {"status": 500, "body": "{}"} if mode is FAIL
            else {"json": {"optionChain": {"result": [{"expirationDates": [_ts(x) for x in EXPIRIES],
                                                       "options": [{"calls": [], "puts": _puts(e, mode)}]}]}}})

    def select(v):
        return {"error": v.get("error"), "message": v.get("message"), "status": v.get("status"),
                "failedWarnings": (v.get("warnings") or []).count("OPTION_CHAIN_READ_FAILED"),
                "expiries": sorted({c["expiry"] for c in v.get("candidates", [])}),
                "next": v.get("recommendedNextAction")}

    return Case(name=f"options_reads_382.put_hedge.{name}", ids=IDS, py=py,
                ts={"js": f'return JSON.parse(await m.yahooFinance.getPutHedgeCandidates("HDGTST", 8, 12, 500, "{AFTER}"));',
                    "isolate": True},
                routes=routes, select=select, expect=expect)


_NONE = {"error": None, "message": None, "failedWarnings": 0}

CASES += [
    _case("one_expiry_failed_one_ok", {EXPIRIES[1]: FAIL},
          {**_NONE, "status": "PARTIAL", "failedWarnings": 1, "expiries": [EXPIRIES[2]], "next": "RETRY"}),
    _case("all_expiries_failed", {EXPIRIES[1]: FAIL, EXPIRIES[2]: FAIL},
          {"error": True, "message": "No option chain could be read; retry.", "status": None, "failedWarnings": 0,
           "expiries": [], "next": None}),
    _case("all_ok_no_puts_unchanged", {EXPIRIES[1]: NO_PUTS, EXPIRIES[2]: NO_PUTS},
          {**_NONE, "status": "PARTIAL", "expiries": [], "next": "RETRY"}),
    _case("all_ok_with_puts_unchanged", {},
          {**_NONE, "status": "OK", "expiries": [EXPIRIES[1], EXPIRIES[2]], "next": "NONE"}),
]
