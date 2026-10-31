"""Parity cases: per-invocation subrequest budget guard (2.11.0, Phase 1 release 3c, spec §5).

The Worker side runs each tool inside a root request scope (`withCacheScope`, as index.ts does for `/mcp`) and
spends part of the 45-unit budget first (`consumeSubrequest("fetch")`), standing in for earlier work in the same
invocation, so the fake upstream needs more fetches than remain. No edge cache is involved (the runner has no
`caches.default`).

The Python side (yfmcp/url_policy.py, spec §4) runs each news call inside `invocation_scope(spent=...)`,
the same pre-spent budget, so both runtimes see the same units left. The news cases are mode "equal". The batch
case stays pinned: a documented divergence (see BATCH_DECISION).
"""

from __future__ import annotations

import asyncio
import json
import types
from unittest.mock import patch

import server
from scripts.parity.cases_news_a import (
    FAKE_FINNHUB,
    FAKE_MARKETAUX,
    FINNHUB_NEWS,
    MARKETAUX_NEWS,
    YAHOO_AUTH_ROUTES,
    YAHOO_QUOTE_SUMMARY,
    news_view,
    tool_case,
)
from scripts.parity.harness import Case
from yfmcp import url_policy

CASES: list[Case] = []

DECISION = "R3C-python-mirror"
BATCH_DECISION = ("Python batch per-ticker work runs through yfinance (uncounted); "
                  "the subrequest limit is a Worker runtime limit")
INTENTIONAL_BATCH_DECISION = "INTENTIONAL (3.7.0): the Worker stops a batch at the Cloudflare subrequest cap; Python has no such cap. " + BATCH_DECISION

# The Worker body: the news cases' frozen clock and fake env, plus a root scope with part of the budget spent.
_BUDGET_TOOL_JS = """
const RealDate = Date;
const FIXED = RealDate.parse(fx.now);
class FrozenDate extends RealDate {
  constructor(...a) { if (a.length === 0) super(FIXED); else super(...a); }
  static now() { return FIXED; }
}
m.response.setWorkerEnv(fx.env || {});
globalThis.Date = FrozenDate;
try {
  const { result } = await m.requestContext.withCacheScope(async () => {
    for (let i = 0; i < fx.spent; i++) m.requestContext.consumeSubrequest("fetch");
    return m.yahooFinance[fx.fn](...args);
  });
  return typeof result === "string" ? JSON.parse(result) : result;
} finally { globalThis.Date = RealDate; m.response.setWorkerEnv({}); }
"""


def budget_case(name: str, ids: list[str], tool: str, args: list, scn: dict, spent: int, **kw) -> Case:
    """tool_case with the Worker call made inside a root scope whose budget has `spent` units used."""
    case = tool_case(name, ids, tool, args, scn, **kw)
    run_python = case.py

    def py():
        with url_policy.invocation_scope(spent=spent):
            return run_python()

    case.py = py
    case.ts = {**case.ts, "js": _BUDGET_TOOL_JS, "fx": {**case.ts["fx"], "spent": spent}}
    return case


FINNHUB_AND_MARKETAUX = ["finnhub", "marketaux"]
KEYS = {**FAKE_FINNHUB, **FAKE_MARKETAUX}
ACME_INFO = {"shortName": "Acme Corp", "longName": "Acme Corporation", "exchange": "NMS"}
MX_EMPTY = {MARKETAUX_NEWS: {"json": {"data": [], "meta": {"found": 0}}}}


def _budget_news_view(v: dict) -> dict:
    view = news_view(v, items=(), sources=("finnhub", "marketaux"))
    view["attempted"] = [[a["sourceType"], a["status"]] for a in v.get("attemptedSources") or []]
    return view


CASES += [
    # Within budget: a call inside a root scope with budget to spare is unchanged (equal in both runtimes).
    budget_case("budget.news.within_budget_is_unchanged", ["R3C-BUDGET-003"], "news",
                ["ACME", 10, 14, FINNHUB_AND_MARKETAUX],
                {"env": KEYS, "info": ACME_INFO, "routes": {FINNHUB_NEWS: {"json": []}, **MX_EMPTY}},
                spent=0, select=_budget_news_view,
                expect={"status": "NOT_FOUND",
                        "coverage": {"sourceCoverage": "FULL", "state": "FULL", "failed": [], "skipped": [], "truncated": [],
                                     "action": "USE_RETURNED_CONTEXT"},
                        "warnings": [],
                        "sourceStatus": {"finnhub": {"status": "EMPTY_RESULT"}, "marketaux": {"status": "EMPTY_RESULT"}},
                        "attempted": [["finnhub", "EMPTY_RESULT"], ["marketaux", "EMPTY_RESULT"]]}),
    # 44 of 45 units spent: Finnhub gets the last unit and answers empty; the Marketaux fallback is refused and
    # reports PROVIDER_BUDGET_EXHAUSTED (failedSources, reasonCode null), coverage PARTIAL, retry the retryable sources.
    budget_case("budget.news.starved_source_reports_budget_exhausted", ["R3C-BUDGET-001"], "news",
                ["ACME", 10, 14, FINNHUB_AND_MARKETAUX],
                {"env": KEYS, "info": ACME_INFO, "routes": {FINNHUB_NEWS: {"json": []}, **MX_EMPTY}},
                spent=44,
                select=lambda v: {
                    **_budget_news_view(v),
                    "failedReasonCodes": [f.get("reasonCode") for f in (v.get("coverage") or {}).get("failedSources") or []],
                },
                expect={"status": "SOURCE_LIMITED_NOT_FOUND",
                           "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL",
                                        "failed": [["marketaux", "PROVIDER_BUDGET_EXHAUSTED"]], "skipped": [], "truncated": [],
                                        "action": "RETRY_RETRYABLE_SOURCES"},
                           "warnings": ["SOURCE_UNAVAILABLE"],
                           "sourceStatus": {"finnhub": {"status": "EMPTY_RESULT"},
                                            "marketaux": {"status": "PROVIDER_BUDGET_EXHAUSTED"}},
                           "attempted": [["finnhub", "EMPTY_RESULT"], ["marketaux", "PROVIDER_BUDGET_EXHAUSTED"]],
                           "failedReasonCodes": [None]}),
]


# A batch that runs out partway: 42 units spent, three left. AAPL needs Yahoo auth (2) + quoteSummary (1) and
# completes; MSFT's quoteSummary is refused and gets the per-ticker budget code.
_PROFILE_ROUTES = {
    **YAHOO_AUTH_ROUTES,
    YAHOO_QUOTE_SUMMARY: {"json": {"quoteSummary": {"result": [{
        "price": {"shortName": "Acme Corp", "longName": "Acme Corporation", "currency": "USD"},
        "assetProfile": {"sector": "Technology", "industry": "Software"},
    }], "error": None}}},
}

_BATCH_JS = """
m.response.setWorkerEnv({});
const { result } = await m.requestContext.withCacheScope(async () => {
  for (let i = 0; i < fx.spent; i++) m.requestContext.consumeSubrequest("fetch");
  return m.yahooFinance.getStockInfo(["AAPL", "MSFT"]);
});
return JSON.parse(result);
"""


def _batch_view(v):
    if isinstance(v, str):
        return v
    out = {}
    for t in ("AAPL", "MSFT"):
        row = v.get(t) or {}
        err = row.get("error") if isinstance(row.get("error"), dict) else (row if row.get("error") is True else None)
        out[t] = {"failed": err is not None,
                  **({"code": err.get("code"), "retryable": err.get("retryable"), "recoveryAction": err.get("recoveryAction")}
                     if err else {})}
    out["batchMeta"] = v.get("__batchMeta")
    return out


class _BatchTicker:
    """yf.Ticker stand-in for the Python batch: yfinance answers, no urllib fetch is made, nothing is counted."""

    def __init__(self, _symbol: str) -> None:
        self.fast_info = types.SimpleNamespace(currency="USD")
        self.info = {"shortName": "Acme Corp", "longName": "Acme Corporation", "currency": "USD", "sector": "Technology",
                     "industry": "Software"}


def _python_batch():
    # The same pre-spent budget as the Worker run (42 of 45); yfinance does not touch it, so no ticker starves.
    with url_policy.invocation_scope(spent=42), patch.object(server.yf, "Ticker", _BatchTicker):
        return json.loads(asyncio.run(server.get_stock_info(["AAPL", "MSFT"])))


CASES += [
    # Documented divergence: BATCH_DECISION. The Worker's per-ticker work is counted fetches, so MSFT starves;
    # the Python batch reads yfinance, which is not counted (spec §4), so both tickers complete and there is no
    # __batchMeta. Python batches cannot starve; nothing here invents a way to make them.
    Case(name="budget.batch.stops_partway_with_per_ticker_code", ids=["R3C-BUDGET-002"],
         py=_python_batch,
         ts={"js": _BATCH_JS, "fx": {"spent": 42}, "isolate": True},
         routes=_PROFILE_ROUTES, mode="pinned", decision=INTENTIONAL_BATCH_DECISION, select=_batch_view,
         expect_py={"AAPL": {"failed": False}, "MSFT": {"failed": False}, "batchMeta": None},
         expect_ts={"AAPL": {"failed": False},
                    "MSFT": {"failed": True, "code": "PROVIDER_BUDGET_EXHAUSTED", "retryable": True, "recoveryAction": "RETRY_LATER"},
                    "batchMeta": {"partialSuccess": True, "successCount": 1, "errorCount": 1}}),
]
