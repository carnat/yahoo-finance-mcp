"""Parity cases: R5-shared (2.21.0), the untagged-fact confidence default becomes LOW (BR-ENV-001, R5SHARED_SPEC.md).

Both enrichers (Python `yfmcp/envelope.py` `_enrich_facts`, Worker `worker/src/response.ts` `enrichFacts`) give a fact-shaped
dict with no own or inherited `confidence` the value LOW when `value`, `low` or `valueRatio` is non-null (HIGH before 2.21.0),
and NOT_DECISION_GRADE otherwise. The normalisation (upper-case; NOT_DISCLOSED -> NOT_DECISION_GRADE; any other value -> LOW)
is unchanged.

The producers that reached the default (spec section 1):
  A. get_analyst_consensus priceTargets               -> LOW (both runtimes)
  B. get_earnings_analysis earningsEstimate[] / revenueEstimate[] rows -> LOW (both runtimes)
  C. get_ownership_holders rows with Yahoo's lowercase `value` -> LOW (both runtimes since 3.0.0; F17 aligned Python to the Worker's quoteSummary rows)
  D. extract_dilution_bridge WARRANT_EXERCISE_NOT_OUTSTANDING counts[] -> tagged HIGH at the producer, so unchanged

F19 (fixed in 2.25.0): the Worker's get_analyst_consensus now sets the top-level source / evidenceClass / decisionGrade /
recommendedNextAction that Python always set, so priceTargets.sourceType is "yahoo_finance" in both runtimes.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from scripts import test_capital_structure as T
from scripts.parity.cases_pricing_evidence_tools import GATE_NOW, YAHOO_AUTH_ROUTES, _ts_with_clock
from scripts.parity.harness import Case

import server  # noqa: E402
from yfmcp import capital_structure as cs  # noqa: E402
from yfmcp.envelope import _envelope_tool_result  # noqa: E402

CASES: list[Case] = []



def _worker(body: str) -> str:
    return f"m.response.setWorkerEnv({{}}); {body}"


# ───────────────────────────────────────────────────────────────── the default itself

_DEFAULT_PAYLOAD = {
    "ticker": "TST",
    "untaggedValue": {"value": 5},
    "untaggedLow": {"low": 2, "high": 9},
    "untaggedRatio": {"valueRatio": 0.5},
    "onlyHigh": {"high": 10},
    "nullValue": {"value": None},
    "taggedHigh": {"value": 7, "confidence": "HIGH"},
    "notDisclosed": {"value": 7, "confidence": "not_disclosed"},
    "otherString": {"value": 7, "confidence": "EXTRACTION_FAILED"},
    "parent": {"confidence": "MEDIUM", "child": {"value": 3}},
    "taggedFact": {"value": 1, "confidence": "HIGH", "inner": {"value": 2}},
}


def _default_view(v: dict) -> dict:
    d = v["data"]
    return {"untaggedValue": d["untaggedValue"]["confidence"], "untaggedLow": d["untaggedLow"]["confidence"],
            "untaggedRatio": d["untaggedRatio"]["confidence"], "onlyHigh": d["onlyHigh"]["confidence"],
            "nullValue": d["nullValue"]["confidence"], "taggedHigh": d["taggedHigh"]["confidence"],
            "notDisclosed": d["notDisclosed"]["confidence"], "otherString": d["otherString"]["confidence"],
            "inheritedFromParent": d["parent"]["child"]["confidence"], "inheritedFromFact": d["taggedFact"]["inner"]["confidence"]}


_DEFAULT_TS = {"untaggedValue": "LOW", "untaggedLow": "LOW", "untaggedRatio": "LOW", "onlyHigh": "NOT_DECISION_GRADE",
               "nullValue": "NOT_DECISION_GRADE", "taggedHigh": "HIGH", "notDisclosed": "NOT_DECISION_GRADE", "otherString": "LOW",
               "inheritedFromParent": "MEDIUM", "inheritedFromFact": "HIGH"}

CASES += [
    Case(name="confidence.default.untagged_fact_with_a_value_is_low_only_high_is_not_decision_grade_inherited_wins",
         ids=["BR-ENV-001"],
         py=lambda: _envelope_tool_result("extract_sec_filing_fact", json.dumps(_DEFAULT_PAYLOAD)),
         ts={"js": _worker("return JSON.parse(m.response.mcpSuccess(args[0], args[1]));"),
             "args": ["extract_sec_filing_fact", json.dumps(_DEFAULT_PAYLOAD)]},
         select=_default_view, expect=_DEFAULT_TS),
]


# ───────────────────────────────────────────────────────────────── A and F19: get_analyst_consensus

_TICKER_A = "CNFDA"  # its own ticker: Python's tool cache is keyed by ticker


def _raw(value):
    return {"raw": value}


_CONSENSUS_SUMMARY = {"quoteSummary": {"result": [{
    "price": {"regularMarketPrice": _raw(120.0)},
    "financialData": {"targetMeanPrice": _raw(150.0), "targetLowPrice": _raw(100.0), "targetHighPrice": _raw(200.0),
                      "targetMedianPrice": _raw(150.0), "numberOfAnalystOpinions": _raw(3)},
    "recommendationTrend": {"trend": [{"period": "0m", "strongBuy": 2, "buy": 3, "hold": 1, "sell": 1, "strongSell": 0}]},
    "upgradeDowngradeHistory": {"history": []}}]}}
_CONSENSUS_ROUTES = {**YAHOO_AUTH_ROUTES,
                     f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{_TICKER_A}?*": {"json": _CONSENSUS_SUMMARY}}


def _py_consensus_envelope() -> str:
    trend = pd.DataFrame([{"period": "0m", "strongBuy": 2, "buy": 3, "hold": 1, "sell": 1, "strongSell": 0}])
    company = SimpleNamespace(
        fast_info=SimpleNamespace(last_price=120.0),
        analyst_price_targets={"mean": 150.0, "low": 100.0, "high": 200.0, "median": 150.0},
        upgrades_downgrades=pd.DataFrame(), recommendations_summary=trend, info={"numberOfAnalystOpinions": 3})
    with patch.object(server.yf, "Ticker", lambda _t: company), patch.object(server, "_target_analyst_count", lambda _c: 3):
        raw = asyncio.run(server.get_analyst_consensus(_TICKER_A))
    return _envelope_tool_result("get_analyst_consensus", raw)


def _consensus_case(name: str, select, **kw) -> Case:
    body = f'const r = await m.tools.callToolResult("get_analyst_consensus", {{ticker: "{_TICKER_A}"}}); return r.value;'
    return Case(name=name, ids=["BR-ENV-001"], py=_py_consensus_envelope,
                ts={"js": _ts_with_clock(GATE_NOW, _worker(body)), "isolate": True},
                routes=_CONSENSUS_ROUTES, select=select, **kw)


def _f19_view(v: dict) -> dict:
    d = v["data"]
    return {k: d.get(k) for k in ("source", "evidenceClass", "decisionGrade", "recommendedNextAction")} | {
        "hasSource": "source" in d, "priceTargetsSourceType": d["priceTargets"].get("sourceType")}


CASES += [
    _consensus_case("confidence.analyst_consensus.price_targets_are_low",
                    lambda v: {"priceTargets": v["data"]["priceTargets"]["confidence"],
                               "priceTargetsDecisionGrade": v["data"]["priceTargets"]["decisionGrade"]},
                    expect={"priceTargets": "LOW", "priceTargetsDecisionGrade": False}),
    # F19 (2.25.0): the Worker now sets the top-level provenance Python always set, so priceTargets inherits "yahoo_finance" in both.
    _consensus_case("confidence.analyst_consensus.top_level_provenance_matches", _f19_view,
                    expect={"source": "yahoo_finance", "evidenceClass": "CONTEXTUAL_MARKET_DATA", "decisionGrade": False,
                            "recommendedNextAction": "NONE", "hasSource": True, "priceTargetsSourceType": "yahoo_finance"}),
]


# ───────────────────────────────────────────────────────────────── B: get_earnings_analysis

_TICKER_B = "CNFDB"
_EST_PERIODS = ["0q", "+1q"]
_EPS_EST = [{"avg": 1.5, "low": 1.2, "high": 1.8, "yearAgoEps": 1.1, "numberOfAnalysts": 10, "growth": 0.36},
            {"avg": 1.7, "low": 1.4, "high": 2.0, "yearAgoEps": 1.3, "numberOfAnalysts": 9, "growth": 0.31}]
_REV_EST = [{"avg": 9.0e9, "low": 8.5e9, "high": 9.5e9, "numberOfAnalysts": 8, "yearAgoRevenue": 8.0e9, "growth": 0.125},
            {"avg": 9.5e9, "low": 9.0e9, "high": 1.0e10, "numberOfAnalysts": 7, "yearAgoRevenue": 8.4e9, "growth": 0.13}]
_EARNINGS_SUMMARY = {"quoteSummary": {"result": [{
    "earningsTrend": {"trend": [{"period": p, "earningsEstimate": {k: _raw(x) for k, x in e.items()},
                                 "revenueEstimate": {k: _raw(x) for k, x in r.items()}}
                                for p, e, r in zip(_EST_PERIODS, _EPS_EST, _REV_EST)]},
    "earningsHistory": {"history": []}}]}}
_EARNINGS_ROUTES = {**YAHOO_AUTH_ROUTES,
                    f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{_TICKER_B}?*": {"json": _EARNINGS_SUMMARY}}


def _py_earnings_envelope() -> str:
    def frame(rows):
        return pd.DataFrame(rows, index=pd.Index(_EST_PERIODS, name="period"))
    company = SimpleNamespace(fast_info=SimpleNamespace(currency="USD"), earnings_estimate=frame(_EPS_EST),
                              revenue_estimate=frame(_REV_EST))
    with patch.object(server.yf, "Ticker", lambda _t: company):
        raw = asyncio.run(server.get_earnings_analysis(_TICKER_B))
    return _envelope_tool_result("get_earnings_analysis", raw)


def _estimate_view(v: dict) -> dict:
    d = v["data"]
    return {key: [row.get("confidence") for row in d.get(key) or []] for key in ("earningsEstimate", "revenueEstimate")}


CASES += [
    Case(name="confidence.earnings_analysis.estimate_rows_are_low", ids=["BR-ENV-001"], py=_py_earnings_envelope,
         ts={"js": _ts_with_clock(GATE_NOW, _worker(
             f'const r = await m.tools.callToolResult("get_earnings_analysis", {{ticker: "{_TICKER_B}"}}); return r.value;')),
             "isolate": True},
         routes=_EARNINGS_ROUTES, select=_estimate_view,
         expect={"earningsEstimate": ["LOW", "LOW"], "revenueEstimate": ["LOW", "LOW"]}),
]


# ───────────────────────────────────────────────────────────────── C: get_ownership_holders (Worker only)

_TICKER_C = "CNFDC"
_HOLDERS_SUMMARY = {"quoteSummary": {"result": [{"institutionOwnership": {"maxAge": 1, "ownershipList": [
    {"maxAge": 1, "reportDate": {"raw": 1782777600, "fmt": "2026-06-30"}, "organization": "Fund One",
     "pctHeld": _raw(0.08), "position": _raw(1_000_000), "value": _raw(120_000_000), "pctChange": _raw(0.01)},
    {"maxAge": 1, "reportDate": {"raw": 1782777600, "fmt": "2026-06-30"}, "organization": "Fund Two",
     "pctHeld": _raw(0.05), "position": _raw(600_000), "value": _raw(72_000_000), "pctChange": _raw(-0.02)}]}}]}}
_HOLDERS_ROUTES = {**YAHOO_AUTH_ROUTES,
                   f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{_TICKER_C}?*": {"json": _HOLDERS_SUMMARY}}


def _py_holders_envelope() -> str:
    # 3.0.0 (F17): Python reads the same quoteSummary module as the Worker, through yfinance's crumb-aware fetcher.
    company = SimpleNamespace(fast_info=SimpleNamespace(currency="USD"),
                              _data=SimpleNamespace(get_raw_json=lambda url, params=None, timeout=30: _HOLDERS_SUMMARY))
    with patch.object(server.yf, "Ticker", lambda _t: company):
        raw = asyncio.run(server.get_ownership_holders(_TICKER_C, "institutional_holders"))
    return _envelope_tool_result("get_ownership_holders", raw)


def _holder_rows(data):
    return data.get("ownershipList") if isinstance(data, dict) else data


CASES += [
    # R5-shared decision 3: the rows carry Yahoo's lowercase `value`, so they are facts and fall to LOW. Python's rows were
    # yfinance's institutional_holders frame (`Value`, not fact-shaped, finding F17) until 3.0.0 gave them the Worker's shape.
    Case(name="confidence.ownership_holders.institutional_rows_are_low_in_both_runtimes",
         ids=["BR-ENV-001"], py=_py_holders_envelope,
         ts={"js": _ts_with_clock(GATE_NOW, _worker(
             f'const r = await m.tools.callToolResult("get_ownership_holders", {{ticker: "{_TICKER_C}", holder_type: "institutional_holders"}}); return r.value;')),
             "isolate": True},
         routes=_HOLDERS_ROUTES,
         select=lambda v: [{"keys": sorted(k for k in row if k in ("value", "Value")), "confidence": row.get("confidence")}
                           for row in _holder_rows(v["data"]) or []],
         expect=[{"keys": ["value"], "confidence": "LOW"}, {"keys": ["value"], "confidence": "LOW"}]),
]


# ───────────────────────────────────────────────────────────────── D: extract_dilution_bridge warrant counts

def _bridge_envelope_py(html: str, filing: str, price: float) -> str:
    src = cs.IxSource("primary", filing, "2026-08-10", "0001234568-26-000999", T.Q_URL, cs.parse_ixbrl(html))
    bridge = cs.dilution_bridge("TST", price, "USD", None, [src], [])
    return _envelope_tool_result("extract_dilution_bridge", json.dumps(bridge))


_BRIDGE_ENVELOPE_JS = _worker("""
const doc = m.capitalStructure.parseIxbrl(fx.html);
const src = { role: "primary", filingType: args[0], filingDate: "2026-08-10", accessionNumber: "0001234568-26-000999", documentUrl: fx.url, doc };
const bridge = m.capitalStructure.dilutionBridge({ ticker: "TST", price: args[1], priceCurrency: "USD", asOfDate: null, sources: [src], atmMatches: [] });
return JSON.parse(m.response.mcpSuccess("extract_dilution_bridge", JSON.stringify(bridge)));
""")


def _warrant_counts_view(v: dict) -> list:
    d = v["data"]
    warnings = d.get("warnings") or []
    counts = next((w.get("counts") for w in warnings if isinstance(w, dict) and w.get("code") == "WARRANT_EXERCISE_NOT_OUTSTANDING"), None)
    return sorted(([c["value"], c["reason"], c.get("confidence"),
                    (c.get("exercise") or {}).get("confidence") if c.get("exercise") else None] for c in counts or []),
                  key=lambda r: r[0])


def _bridge_case(name: str, html: str, filing: str, price: float, expect: list) -> Case:
    return Case(name=name, ids=["BR-ENV-001", "BR-EXT-007"], py=lambda: _bridge_envelope_py(html, filing, price),
                ts={"js": _BRIDGE_ENVELOPE_JS, "args": [filing, price], "fx": {"html": html, "url": T.Q_URL}},
                select=_warrant_counts_view, expect=expect)


CASES += [
    # R5-shared decision 4: each count is tagged confidence HIGH at the producer (Worker capital-structure.ts warrantCountEvents),
    # so the counts and the `exercise` child (which inherits it) stay HIGH after the default flips to LOW.
    _bridge_case("confidence.dilution_bridge.warrant_exercised_after_count_and_its_exercise_stay_high", T.BE_Q, "10-Q", 30,
                 [[3_531_073, "EXERCISED_AFTER_COUNT", "HIGH", "HIGH"]]),
    _bridge_case("confidence.dilution_bridge.warrant_exercise_and_equity_movement_counts_stay_high", T.VRT_WARRANT_K, "10-K", 150,
                 [[1_000_000, "WARRANT_EXERCISE", "HIGH", None], [4_812_521, "EQUITY_STATEMENT_MOVEMENT", "HIGH", None]]),
]
