"""Parity cases for the 2.25.2 audit repairs (analysis/yahoo-finance-mcp/F_TRIAGE.md §5).

F33: get_fund_profile on a company is an INPUT_VALIDATION_ERROR naming its quote type, in both runtimes.
F34: the Worker's company profile keeps a field an earlier quoteSummary module set when a later one is empty.
F31: the Python grouped boundary accepts an enum parameter's string value (every holder_type was refused).
F42 lives with the price-target cases in cases_pricing_evidence_tools.py.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

from scripts.parity.cases_pricing_evidence_tools import YAHOO_AUTH_ROUTES
from scripts.parity.harness import Case

import server  # noqa: E402

NOT_A_FUND = ("TST is quote type EQUITY, not an ETF or fund; get_fund_profile reads fund data only. "
              "Use get_company_profile for a company.")


def _envelope_view(v):
    v = json.loads(v) if isinstance(v, str) else v
    return {"ok": v.get("ok"), "error": v.get("error")} if isinstance(v, dict) else v


def _fund_case(name: str, quote_type: str, expect) -> Case:
    def py():
        company = SimpleNamespace(info={"quoteType": quote_type, "shortName": "Test"}, funds_data=None)
        with patch.object(server.yf, "Ticker", lambda _t: company):
            return asyncio.run(server.get_etf_info("TST", ["overview"]))

    summary = {"quoteSummary": {"result": [{"price": {"quoteType": quote_type, "shortName": "Test"}, "summaryDetail": {}}]}}
    return Case(
        name=name, ids=["F33"], py=py,
        ts={"js": 'return await m.yahooFinance.getEtfInfo("TST", ["overview"]);', "isolate": True},
        routes={**YAHOO_AUTH_ROUTES, "https://query1.finance.yahoo.com/v10/finance/quoteSummary/TST?*": {"json": summary}},
        select=_envelope_view, expect=expect)


def _profile_ps_ts() -> dict:
    # summaryDetail states P/S; a later module carries the same key empty.
    summary = {"quoteSummary": {"result": [{
        "summaryDetail": {"priceToSalesTrailing12Months": {"raw": 10.72, "fmt": "10.72"}},
        "defaultKeyStatistics": {"priceToSalesTrailing12Months": {}},
        "price": {"shortName": "Test", "currency": "USD"},
    }]}}
    return {"routes": {**YAHOO_AUTH_ROUTES, "https://query1.finance.yahoo.com/v10/finance/quoteSummary/TST?*": {"json": summary}}}


def _enum_boundary_py():
    import tool_groups
    from yfmcp.schemas import HolderType, RecommendationType

    return {"holder": tool_groups._matches_annotation("insider_transactions", HolderType),
            "recommendation": tool_groups._matches_annotation("recommendations", RecommendationType),
            "member": tool_groups._matches_annotation(HolderType.major_holders, HolderType),
            "number": tool_groups._matches_annotation(3, HolderType)}


CASES = [
    _fund_case("audit2252.fund_profile.equity_is_not_a_fund", "EQUITY",
               {"ok": False, "error": {"code": "INPUT_VALIDATION_ERROR", "message": NOT_A_FUND}}),
    Case(name="audit2252.profile.later_empty_module_keeps_price_to_sales", ids=["F34"],
         py=lambda: {"priceToSalesTrailing12Months": 10.72},
         ts={"js": 'return JSON.parse(await m.yahooFinance.getStockInfo("TST")).priceToSalesTrailing12Months;', "isolate": True},
         routes=_profile_ps_ts()["routes"],
         select=lambda v: v if isinstance(v, dict) else {"priceToSalesTrailing12Months": v},
         expect={"priceToSalesTrailing12Months": 10.72},
         note="Worker-only repair: Python's yfinance info already keeps the value; the Python side states the expected value."),
    Case(name="audit2252.grouped_boundary.enum_value_is_accepted", ids=["F31"],
         py=_enum_boundary_py,
         ts={"js": 'return {holder: true, recommendation: true, member: true, number: false};'},
         expect={"holder": True, "recommendation": True, "member": True, "number": False},
         note="The Worker boundary checks shape only (no enums), so its side is the constant contract the Python boundary must meet."),
]
