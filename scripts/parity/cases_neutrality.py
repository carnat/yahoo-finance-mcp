"""Parity cases: neutrality, Q14-N1 / Q14-N2 / Q14-N3 (the 2.8.0 deprecations, removed in 3.0.0).

3.0.0 removed the deprecated fields (doctrineUse, the always-null authority fields, the price-target labels, the fixed
$10M gate, the tier-numbered input groups) and with them their DEPRECATED_FIELD notices. The neutral replacements stay.
Both runtimes must produce the same payloads, so these cases compare the envelope each runtime returns from its
tool-boundary entry point (Worker `callToolResult` / `callVisibleToolResult`, Python `_envelope_tool_result` / the
grouped router).

Conventions follow cases_pricing_evidence_tools.py: `equal` cases carry `expect`; `pinned` cases record a known
Python/Worker difference ("FINDING ..." decisions) that RN does not change.

The notice texts below are written out again, on purpose, rather than imported from either runtime's module: a text
that drifts in one runtime fails here.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from unittest.mock import AsyncMock, patch

import pandas as pd

from scripts.parity.cases_pricing_evidence_tools import (
    BRACKET_NOW, QUOTE_TIME, YAHOO_AUTH_ROUTES, _bracket_case, _gate_case, _ts_with_clock,
)
from scripts.parity.cases_accession import _python_sec_state
from scripts.parity.harness import Case

import server  # noqa: F401,E402
from yfmcp.envelope import _envelope_tool_result  # noqa: E402
from yfmcp.tools import news as news_tools  # noqa: E402
from yfmcp.news.providers import yahoo as yahoo_provider  # noqa: E402

CASES: list[Case] = []



MIN_ADV_ERROR = "min_adv_usd must be a positive number when supplied."


def _worker(body: str, *, env: str = "{}") -> str:
    """A Worker case body that runs against the V2 envelope entry points."""
    return f"m.response.setWorkerEnv({env}); {body}"


def _envelope_view(v: dict) -> dict:
    if not isinstance(v, dict):
        return {"notAnEnvelope": v}
    err = v.get("error") if isinstance(v.get("error"), dict) else {}
    return {"ok": v.get("ok"), "code": err.get("code"), "message": err.get("message"),
            "warnings": (v.get("meta") or {}).get("warnings")}


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Q14-N3b: meetsMinimum / minAdvUsd on check_volume_liquidity_threshold (BR-PRC-003)
# Fixture listings (cases_pricing_evidence_tools): EURX.PA trades 200,000,000 USD a day, USDX 500,000, and NORATE.L has
# no usable FX rate, so its average traded value is unknown.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

EURX = dict(symbol="EURX.PA", currency="EUR", close=100.0, volume=1_000_000, fx=("EUR=X", 0.5))
USDX = dict(symbol="USDX", currency="USD", close=5.0, volume=100_000, fx=None)
NORATE = dict(symbol="NORATE.L", currency="GBp", close=250.0, volume=2_000_000, fx=("GBP=X", None))


def _gate_fields(v: dict) -> dict:
    keys = ("status", "meetsMinimum", "minAdvUsd", "adv20dTradedValueUsd", "gateBasis")
    return {k: v.get(k) for k in keys}


EURX_BASE = {"status": "OK", "adv20dTradedValueUsd": 200000000,
             "gateBasis": "ADV20_TRADED_VALUE_USD"}
USDX_BASE = {"status": "OK", "adv20dTradedValueUsd": 500000,
             "gateBasis": "ADV20_TRADED_VALUE_USD"}
NORATE_BASE = {"status": "OK", "adv20dTradedValueUsd": None,
               "gateBasis": "ADV20_TRADED_VALUE_USD"}

CASES += [
    # Direct (the gate function in each runtime). 3.0.0: gatePass and gateThresholdUsd are gone from the select.
    _gate_case("neutral.n3b.meets_minimum_true_when_adv_above_the_minimum", ["BR-PRC-003", "Q14-N3"], **EURX,
               min_adv=150_000_000, select=_gate_fields,
               expect={**EURX_BASE, "meetsMinimum": True, "minAdvUsd": 150000000}),
    _gate_case("neutral.n3b.meets_minimum_false_when_adv_below_the_minimum", ["BR-PRC-003", "Q14-N3"], **EURX,
               min_adv=250_000_000, select=_gate_fields,
               expect={**EURX_BASE, "meetsMinimum": False, "minAdvUsd": 250000000}),
    _gate_case("neutral.n3b.meets_minimum_true_when_adv_equals_the_minimum", ["BR-PRC-003", "Q14-N3"], **USDX,
               min_adv=500_000, select=_gate_fields,
               expect={**USDX_BASE, "meetsMinimum": True, "minAdvUsd": 500000}),
    _gate_case("neutral.n3b.meets_minimum_false_one_cent_below_equal", ["BR-PRC-003", "Q14-N3"], **USDX,
               min_adv=500_000.01, select=_gate_fields,
               expect={**USDX_BASE, "meetsMinimum": False, "minAdvUsd": 500000.01}),
    _gate_case("neutral.n3b.meets_minimum_null_when_adv_is_unknown", ["BR-PRC-003", "Q14-N3"], **NORATE,
               min_adv=1_000_000, select=_gate_fields,
               expect={**NORATE_BASE, "meetsMinimum": None, "minAdvUsd": 1000000}),
    _gate_case("neutral.n3b.meets_minimum_null_without_min_adv_usd", ["BR-PRC-003", "Q14-N3"], **USDX,
               select=_gate_fields, expect={**USDX_BASE, "meetsMinimum": None, "minAdvUsd": None}),
    _gate_case("neutral.n3b.meets_minimum_null_without_min_adv_usd_and_adv_unknown", ["BR-PRC-003", "Q14-N3"], **NORATE,
               select=_gate_fields, expect={**NORATE_BASE, "meetsMinimum": None, "minAdvUsd": None}),
]


def _gate_envelope_view(v: dict) -> dict:
    data = v.get("data") or {}
    return {"ok": v.get("ok"), **_gate_fields(data), "warnings": v["meta"]["warnings"]}


CASES += [
    # Through the V2 envelope: the same values, and no notice (the gate fields were removed in 3.0.0).
    _gate_case("neutral.n3b.envelope_carries_meets_minimum_and_no_notice", ["BR-PRC-003", "Q14-N3"], **EURX,
               min_adv=150_000_000, via="envelope", select=_gate_envelope_view,
               expect={"ok": True, **EURX_BASE, "meetsMinimum": True, "minAdvUsd": 150000000, "warnings": []}),
    _gate_case("neutral.n3b.envelope_without_min_adv_usd_has_no_notice", ["BR-PRC-003", "Q14-N3"], **USDX,
               via="envelope", select=_gate_envelope_view,
               expect={"ok": True, **USDX_BASE, "meetsMinimum": None, "minAdvUsd": None, "warnings": []}),
    # Grouped mode: the same call as an action of stock_pricing.
    _gate_case("neutral.n3b.grouped_carries_meets_minimum_and_no_notice", ["BR-PRC-003", "Q14-N3"], **EURX,
               min_adv=250_000_000, via="grouped", select=_gate_envelope_view,
               expect={"ok": True, **EURX_BASE, "meetsMinimum": False, "minAdvUsd": 250000000, "warnings": []}),
    _gate_case("neutral.n3b.grouped_without_min_adv_usd_has_no_notice", ["BR-PRC-003", "Q14-N3"], **USDX,
               via="grouped", select=_gate_envelope_view,
               expect={"ok": True, **USDX_BASE, "meetsMinimum": None, "minAdvUsd": None, "warnings": []}),
]


def _bad_min_case(name: str, bad, *, tool: str = "check_volume_liquidity_threshold", via: str = "envelope",
                  expect: dict, **kw) -> Case:
    """A min_adv_usd the tool must refuse. `via` "envelope" is the direct tool, "grouped" the stock_pricing action."""
    args = {"ticker": "USDX", "min_adv_usd": bad}

    def py():
        from yfmcp.app import build_handler_registry
        import tool_groups

        async def call():
            if via == "grouped":
                return await tool_groups._route_grouped_call("stock_pricing", tool, args, build_handler_registry(server.yfinance_server))
            fn = getattr(server, tool) if tool == "check_volume_liquidity_threshold" else server.get_market_snapshot
            raw = await (fn("USDX", False, bad) if tool == "check_volume_liquidity_threshold" else fn("USDX", "compact", False, bad))
            return _envelope_tool_result(tool, raw)
        return asyncio.run(call())

    if via == "grouped":
        # A grouped parameter-validation failure comes back as text only, without the parsed value.
        body = (f'const r = await m.tools.callVisibleToolResult("stock_pricing", {{action: {json.dumps(tool)}, '
                f'params: {json.dumps(args)}}}); return r.value ?? JSON.parse(r.text);')
    else:
        body = f'return (await m.tools.callToolResult({json.dumps(tool)}, {json.dumps(args)})).value;'
    return Case(name=name, ids=["BR-PRC-003", "Q14-N3"], py=py, ts={"js": _worker(body), "isolate": True},
                select=_envelope_view, expect=expect, **kw)


def _refused(message: str = MIN_ADV_ERROR) -> dict:
    return {"ok": False, "code": "INPUT_VALIDATION_ERROR", "message": message, "warnings": []}


for _label, _bad in (("string", "abc"), ("numeric_string", "5"), ("zero", 0), ("negative", -5), ("boolean", True)):
    CASES.append(_bad_min_case(f"neutral.n3b.min_adv_usd_{_label}_is_refused_by_the_gate_tool", _bad, expect=_refused()))
    CASES.append(_bad_min_case(f"neutral.n3b.min_adv_usd_{_label}_is_refused_by_the_snapshot_tool", _bad,
                               tool="get_market_snapshot", expect=_refused()))

# Grouped mode checks the parameter type before the tool runs, so a string or boolean is refused by the group router in
# both runtimes with the same text; zero and negative numbers pass that check and reach the tool's own refusal.
_GROUPED_TYPE_ERROR = "Invalid params for 'check_volume_liquidity_threshold': invalid parameter value(s): min_adv_usd."
for _label, _bad, _msg in (("string", "abc", _GROUPED_TYPE_ERROR), ("numeric_string", "5", _GROUPED_TYPE_ERROR),
                           ("boolean", True, _GROUPED_TYPE_ERROR), ("zero", 0, MIN_ADV_ERROR), ("negative", -5, MIN_ADV_ERROR)):
    CASES.append(_bad_min_case(f"neutral.n3b.grouped_min_adv_usd_{_label}_is_refused", _bad, via="grouped",
                               expect=_refused(_msg)))


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Q14-N3b: get_market_snapshot volume.meetsMinimum / volume.minAdvUsd
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _snapshot_volume(v: dict) -> dict:
    volume = v["volume"]
    return {k: volume.get(k) for k in ("meetsMinimum", "minAdvUsd", "avgVolume20d", "ratio20d")}


def _snapshot_envelope_view(v: dict) -> dict:
    return {"ok": v.get("ok"), **_snapshot_volume(v["data"]), "warnings": v["meta"]["warnings"]}


CASES += [
    _gate_case("neutral.n3b.snapshot_meets_minimum_true_above_the_minimum", ["BR-PRC-003", "Q14-N3"], **EURX,
               min_adv=150_000_000, via="snapshot", select=_snapshot_volume,
               expect={"meetsMinimum": True, "minAdvUsd": 150000000, "avgVolume20d": 1000000, "ratio20d": 1}),
    _gate_case("neutral.n3b.snapshot_meets_minimum_false_below_the_minimum", ["BR-PRC-003", "Q14-N3"], **EURX,
               min_adv=250_000_000, via="snapshot", select=_snapshot_volume,
               expect={"meetsMinimum": False, "minAdvUsd": 250000000, "avgVolume20d": 1000000, "ratio20d": 1}),
    _gate_case("neutral.n3b.snapshot_meets_minimum_true_when_equal", ["BR-PRC-003", "Q14-N3"], **USDX,
               min_adv=500_000, via="snapshot", select=_snapshot_volume,
               expect={"meetsMinimum": True, "minAdvUsd": 500000, "avgVolume20d": 100000, "ratio20d": 1}),
    _gate_case("neutral.n3b.snapshot_meets_minimum_null_when_adv_is_unknown", ["BR-PRC-003", "Q14-N3"], **NORATE,
               min_adv=1_000_000, via="snapshot", select=_snapshot_volume,
               expect={"meetsMinimum": None, "minAdvUsd": 1000000, "avgVolume20d": 2000000, "ratio20d": 1}),
    _gate_case("neutral.n3b.snapshot_meets_minimum_null_without_min_adv_usd", ["BR-PRC-003", "Q14-N3"], **USDX,
               via="snapshot", select=_snapshot_volume,
               expect={"meetsMinimum": None, "minAdvUsd": None, "avgVolume20d": 100000, "ratio20d": 1}),
    _gate_case("neutral.n3b.snapshot_envelope_carries_volume_meets_minimum_and_no_notice", ["BR-PRC-003", "Q14-N3"],
               **EURX, min_adv=150_000_000, via="snapshot_envelope", select=_snapshot_envelope_view,
               expect={"ok": True, "meetsMinimum": True, "minAdvUsd": 150000000,
                       "avgVolume20d": 1000000, "ratio20d": 1, "warnings": []}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Q14-N3a: calculate_price_target_distance notice (priceAtOrAboveTarget values: BR-PRC-022 cases)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _bracket_envelope_view(v: dict) -> dict:
    data = v.get("data") or {}
    return {"ok": v.get("ok"), "priceAtOrAboveTarget": data.get("priceAtOrAboveTarget"),
            "labelKeys": sorted(k for k in ("bracket", "inferredTag", "tag", "tagNote") if k in data), "warnings": v["meta"]["warnings"]}


_N3A_LONG = {"ok": True, "priceAtOrAboveTarget": False, "labelKeys": []}
_N3A_AT_TARGET = {"ok": True, "priceAtOrAboveTarget": True, "labelKeys": []}

CASES += [
    _bracket_case("neutral.n3a.envelope_has_the_flag_no_labels_and_no_notice", ["BR-PRC-022", "Q14-N3"], 70.0, 100.0,
                  market_time=QUOTE_TIME, via="envelope", select=_bracket_envelope_view,
                  expect={**_N3A_LONG, "warnings": []}),
    _bracket_case("neutral.n3a.envelope_flag_true_at_the_target", ["BR-PRC-022", "Q14-N3"], 100.0, 100.0,
                  market_time=QUOTE_TIME, via="envelope", select=_bracket_envelope_view,
                  expect={**_N3A_AT_TARGET, "warnings": []}),
    # A failure carries no data and no notice.
    _bracket_case("neutral.n3a.envelope_failure_carries_no_notice", ["BR-PRC-022", "Q14-N3"], None, 100.0,
                  market_time=QUOTE_TIME, via="envelope",
                  select=lambda v: {"ok": v.get("ok"), "warnings": v["meta"]["warnings"]},
                  expect={"ok": False, "warnings": []}),
    # Price 0: Python divides by it for distanceToTargetPct before the flag is reached and raises; the Worker computes
    # Infinity -> null and reports the flag as null. The flag is null for any non-positive price in the Worker.
    _bracket_case("neutral.n3a.finding_zero_price_python_raises_worker_reports_null_flag", ["BR-PRC-022", "Q14-N3"], 0.0, 100.0,
                  market_time=QUOTE_TIME, keys=("referenceTargetPct", "distanceToTargetPct", "priceAtOrAboveTarget"),
                  mode="pinned", decision="FINDING Q14-N3: a zero lastPrice raises ZeroDivisionError in Python (distanceToTargetPct is computed first)",
                  expect_py={"__error__": "ZeroDivisionError: float division by zero"},
                  expect_ts={"referenceTargetPct": 0, "distanceToTargetPct": None, "priceAtOrAboveTarget": None}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Q14-N2: an evidence-only action keeps its payload; the notice is in meta.warnings only
# list_evidence_cuts with no durable store configured returns a fixed payload in both runtimes.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

EVIDENCE_DATA = {
    "ticker": "AAPL", "storageStatus": "UNAVAILABLE", "cuts": [],
    "decisionUse": "EVIDENCE_ONLY",
}


def _py_evidence(legacy: bool = False):
    def py():
        async def call():
            return _envelope_tool_result("list_evidence_cuts", await server.list_evidence_cuts("AAPL"))
        with patch.dict(os.environ, {"MCP_ENVELOPE_V2": "false" if legacy else "true"}):
            return asyncio.run(call())
    return py


def _ts_evidence(legacy: bool = False) -> dict:
    env = '{MCP_ENVELOPE_V2: "false"}' if legacy else "{}"
    read = "JSON.parse(r.text)" if legacy else "r.value"
    return {"js": _worker(f'const r = await m.tools.callToolResult("list_evidence_cuts", {{ticker: "AAPL"}}); return {read};', env=env),
            "isolate": True}


CASES += [
    Case(name="neutral.n2.evidence_action_data_has_only_decision_use_and_no_notice", ids=["Q14-N2"],
         py=_py_evidence(), ts=_ts_evidence(),
         select=lambda v: {"ok": v["ok"], "data": v["data"], "warnings": v["meta"]["warnings"]},
         expect={"ok": True, "data": EVIDENCE_DATA, "warnings": []}),
    # The legacy envelope (MCP_ENVELOPE_V2=false) emits no notice and no meta at all; the payload is the same.
    Case(name="neutral.n2.legacy_envelope_has_no_meta_and_the_same_payload", ids=["Q14-N2"],
         py=_py_evidence(legacy=True), ts=_ts_evidence(legacy=True),
         select=lambda v: {"hasMeta": "meta" in v, "data": v}, expect={"hasMeta": False, "data": EVIDENCE_DATA}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Q14-N1: reliabilityClass replaces doctrineUse
# Both runtimes add reliabilityClass to meta for the three status-table tools and to the data of press releases and
# section markdown. Python never emitted doctrineUse, so it emits no doctrineUse and no N1 notice (pinned FINDING).
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from scripts.parity.cases_pricing_evidence_tools import AAPL_DOC  # noqa: E402

SECTION_BODY = " ".join(["Risk factor sentence number %d describes a business risk in plain words." % i for i in range(1, 9)])
SECTION_HTML = (f"<html><body><h2>Item 1A. Risk Factors</h2><p>{SECTION_BODY}</p>"
                "<h2>Item 1B. Unresolved Staff Comments</h2><p>None.</p></body></html>")
SECTION_ROUTES = {
    "https://www.sec.gov/files/company_tickers.json": {"json": {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}},
    "https://data.sec.gov/submissions/CIK0000320193.json": {"json": {"cik": "320193", "name": "Apple Inc.", "filings": {"recent": {
        "form": ["10-K"], "accessionNumber": ["0000320193-24-000123"], "primaryDocument": ["aapl-20240928.htm"],
        "filingDate": ["2024-11-01"], "reportDate": ["2024-09-28"], "acceptanceDateTime": ["2024-11-01T06:00:00.000Z"]}}}},
    AAPL_DOC: {"body": SECTION_HTML},
}


def _tool_case(name: str, tool: str, args: dict, py_call, select, *, routes: dict | None = None, ids=("Q14-N1",), **kw) -> Case:
    """A tool through the V2 envelope in both runtimes. `py_call` is an async callable returning the tool's raw result."""
    def py():
        async def call():
            return _envelope_tool_result(tool, await py_call())
        with patch.object(server.yf, "Ticker", side_effect=RuntimeError("offline")), \
                patch.object(yahoo_provider, "_yahoo_search_news", return_value=[]), \
                (_python_sec_state() if routes else contextlib.nullcontext()):
            return asyncio.run(call())

    body = f'const r = await m.tools.callToolResult({json.dumps(tool)}, {json.dumps(args)}); return r.value;'
    return Case(name=name, ids=list(ids), py=py, ts={"js": _worker(body), "isolate": True}, routes=routes or {},
                select=select, **kw)


def _reliability_view(v: dict) -> dict:
    data = v.get("data") if isinstance(v.get("data"), dict) else {}
    return {"ok": v.get("ok"), "metaReliabilityClass": v["meta"].get("reliabilityClass"),
            "dataReliabilityClass": data.get("reliabilityClass"), "dataHasReliabilityClass": "reliabilityClass" in data}


def _doctrine_view(v: dict) -> dict:
    data = v.get("data") if isinstance(v.get("data"), dict) else {}
    return {"metaHasDoctrineUse": "doctrineUse" in v["meta"], "dataHasDoctrineUse": "doctrineUse" in data,
            "metaHasDecisionGrade": "decisionGrade" in v["meta"], "warnings": v["meta"]["warnings"]}


PRESS = ("get_company_press_releases", {"ticker": "AAPL"}, lambda: server.get_company_press_releases("AAPL"))
FACT = ("extract_sec_filing_fact", {"ticker": "AAPL", "fact": "total_revenue"},
        lambda: server.extract_sec_filing_fact("AAPL", fact="total_revenue"))
SECTION = ("get_sec_filing_section_markdown", {"ticker": "AAPL"}, lambda: server.get_sec_filing_section_markdown("AAPL"))

CASES += [
    _tool_case("neutral.n1.press_releases_reliability_class_in_meta_and_data", PRESS[0], PRESS[1], PRESS[2], _reliability_view,
               expect={"ok": True, "metaReliabilityClass": "RELIABLE", "dataReliabilityClass": "RELIABLE", "dataHasReliabilityClass": True}),
    _tool_case("neutral.n1.extract_sec_filing_fact_reliability_class_in_meta_only", FACT[0], FACT[1], FACT[2], _reliability_view,
               expect={"ok": True, "metaReliabilityClass": "RELIABLE", "dataReliabilityClass": None, "dataHasReliabilityClass": False}),
    _tool_case("neutral.n1.section_markdown_reliability_class_in_meta_and_data", SECTION[0], SECTION[1], SECTION[2], _reliability_view,
               routes=SECTION_ROUTES, ids=("Q14-N1",),
               expect={"ok": True, "metaReliabilityClass": "UNRELIABLE", "dataReliabilityClass": "UNRELIABLE", "dataHasReliabilityClass": True}),
    # 3.0.0: neither runtime emits doctrineUse or the status table's meta.decisionGrade, and neither gives their notices.
    # F20 (3.0.0): a status string in confidence moves to status; no DEPRECATED_FIELD notice remains.
    _tool_case("neutral.n1.doctrine_use_and_meta_decision_grade_are_gone_press_releases", PRESS[0], PRESS[1], PRESS[2],
               _doctrine_view, expect={"metaHasDoctrineUse": False, "dataHasDoctrineUse": False, "metaHasDecisionGrade": False, "warnings": []}),
    _tool_case("neutral.n1.doctrine_use_and_meta_decision_grade_are_gone_extract_sec_filing_fact", FACT[0], FACT[1], FACT[2],
               _doctrine_view, expect={"metaHasDoctrineUse": False, "dataHasDoctrineUse": False, "metaHasDecisionGrade": False,
                                       "warnings": []}),
    _tool_case("neutral.n1.doctrine_use_and_meta_decision_grade_are_gone_section_markdown", SECTION[0], SECTION[1], SECTION[2],
               _doctrine_view, routes=SECTION_ROUTES,
               expect={"metaHasDoctrineUse": False, "dataHasDoctrineUse": False, "metaHasDecisionGrade": False,
                       "warnings": []}),
]


def _section_view(v: dict) -> dict:
    data = v["data"]
    return {"status": data.get("status"), "decisionGrade": data.get("decisionGrade"), "confidence": data.get("confidence"),
            "warningCodes": [w.get("code") for w in data.get("warnings") or []] if data.get("warnings") is not None else None,
            "hasWarnings": "warnings" in data, "source": data.get("source"), "section": data.get("matchedHeading")}


# 2.14.0 (R4B1_SPEC section 3): Python marks section markdown as the Worker does; doctrineUse was removed from the Worker in 3.0.0.
CASES.append(_tool_case(
    "neutral.n1.finding_section_markdown_status_decision_grade_and_warning_differ", SECTION[0], SECTION[1], SECTION[2],
    _section_view, routes=SECTION_ROUTES,
    expect={"status": "SECTION_MARKDOWN_UNVERIFIED", "decisionGrade": False, "confidence": None,
            "warningCodes": ["LIVE_SECTION_EXTRACTION_UNRELIABLE"], "hasWarnings": True,
            "source": "html_parser_fallback", "section": "Item 1A. Risk Factors"}))


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Q14-N3c: analyze_position_signals carries analystInputs / pricePositionInputs / earningsInputs / technicalInputs;
# the tier-numbered t1/t2/t4/t5_inputs groups were removed in 3.0.0. The runtime asymmetries of the groups
# stay as they are today and are pinned below as FINDING cases.
# Python: the six component tools are stubbed. Worker: one canned Yahoo quoteSummary and chart per ticker.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from scripts.parity.cases_pricing_evidence_tools import GATE_NOW, _epoch, _weekdays  # noqa: E402

_POS_DAYS = _weekdays("2026-10-09", 300)
_POS_STAMPS = [_epoch(d, "20:00", "UTC") for d in _POS_DAYS]
_POS_CLOSES = [100 + i * 0.1 for i in range(300)]
_POS_CHART = {"chart": {"result": [{
    "meta": {"exchangeTimezoneName": "UTC", "regularMarketTime": _POS_STAMPS[-1], "currency": "USD", "symbol": "TST",
             "regularMarketPrice": _POS_CLOSES[-1], "previousClose": _POS_CLOSES[-2]},
    "timestamp": _POS_STAMPS,
    "indicators": {"quote": [{"close": _POS_CLOSES, "open": _POS_CLOSES, "high": _POS_CLOSES, "low": _POS_CLOSES,
                              "volume": [1_000_000] * 300}], "adjclose": [{"adjclose": _POS_CLOSES}]}}]}}


def _raw(value):
    return {"raw": value}


_POS_SUMMARY = {"quoteSummary": {"result": [{
    "price": {"currency": "USD", "quoteType": "EQUITY", "regularMarketPrice": _raw(_POS_CLOSES[-1]), "marketState": "CLOSED",
              "regularMarketTime": _raw(_POS_STAMPS[-1])},
    "summaryDetail": {"fiftyTwoWeekHigh": _raw(max(_POS_CLOSES)), "fiftyTwoWeekLow": _raw(min(_POS_CLOSES))},
    "defaultKeyStatistics": {},
    "financialData": {"targetMeanPrice": _raw(150), "numberOfAnalystOpinions": _raw(3)},
    "recommendationTrend": {"trend": [{"period": "0m", "strongBuy": 2, "buy": 3, "hold": 1, "sell": 1, "strongSell": 0}]},
    "upgradeDowngradeHistory": {"history": []}, "earningsHistory": {"history": []}, "earnings": {}}]}}
POSITION_ROUTES = {
    **YAHOO_AUTH_ROUTES,
    "https://query1.finance.yahoo.com/v10/finance/quoteSummary/*": {"json": _POS_SUMMARY},
    "https://query1.finance.yahoo.com/v8/finance/chart/*": {"json": _POS_CHART},
}

# What the stubbed Python components return: the same market as the canned Worker responses above.
PY_COMPONENTS = {
    "get_analyst_upgrade_radar": {"netSentiment": 0, "upgrades": 0, "downgrades": 0, "initiations": 0},
    "get_analyst_consensus": {"dominantRating": "buy", "totalAnalysts": 7, "numberOfAnalysts": 7},
    "get_price_stats": {"lastPrice": 129.9, "yearHigh": 129.9, "yearLow": 100, "pctFromYearHigh": 0, "pctFromYearLow": 29.9,
                        "priceTimestamp": "2026-10-09T20:00:00Z"},
    "get_earnings_momentum": {"beatRate": None, "currentBeatStreak": 0, "avgSurprisePct": None, "momentumFlag": "NO_HISTORY",
                              "preRevenue": True},
    "get_technical_indicators": {"rsi14": 100, "macd": 0.0, "macdHistogram": 0, "lastClose": 129.9, "dataDate": "2026-10-09"},
    "get_ma_position": {"trend": None, "pctVs50dma": None, "pctVs200dma": None, "dataDate": "2026-10-09"},
}


def _position_case(name: str, select, *, ticker="TST", ids=("Q14-N3",), via: str = "envelope", components=None, **kw) -> Case:
    """analyze_position_signals through the V2 envelope in both runtimes (`via` "expanded" is the Worker's expanded-mode
    callToolResult, which is where a ticker list is accepted)."""
    payloads = {**PY_COMPONENTS, **(components or {})}

    def py():
        async def call():
            stubs = {k: AsyncMock(return_value=json.dumps(v)) for k, v in payloads.items()}
            with patch.multiple(server, **stubs):
                raw = await server.analyze_position_signals(ticker)
            return _envelope_tool_result("analyze_position_signals", raw)
        return asyncio.run(call())

    body = (f'const r = await m.tools.callToolResult("analyze_position_signals", {{ticker: {json.dumps(ticker)}}}); '
            "return r.value;")
    return Case(name=name, ids=list(ids), py=py, ts={"js": _ts_with_clock(GATE_NOW, _worker(body)), "isolate": True},
                routes=POSITION_ROUTES, select=select, **kw)


REMOVED_GROUPS = ("t1_inputs", "t2_inputs", "t4_inputs", "t5_inputs")
GROUPS = ("analystInputs", "pricePositionInputs", "earningsInputs", "technicalInputs")


def _groups_view(v: dict) -> dict:
    data = v["data"]
    return {"ok": v["ok"], "removedGroups": sorted(k for k in REMOVED_GROUPS if k in data),
            "groups": sorted(k for k in GROUPS if k in data), "warnings": v["meta"]["warnings"]}


CASES += [
    _position_case("neutral.n3c.tier_numbered_groups_are_gone_and_there_is_no_notice", _groups_view,
                   expect={"ok": True, "removedGroups": [], "groups": sorted(GROUPS), "warnings": []}),
    # 3.0.0: analystInputs takes its 30-day counts from the radar's plain upgrades/downgrades/initiations.
    _position_case("neutral.n3c.twins_carry_the_source_group_key_sets_t1", lambda v: sorted(v["data"]["analystInputs"]),
                   expect=["analystCount", "analystNetSentiment", "dominantRating", "downgrades30d", "initiations30d", "upgrades30d"]),
    _position_case("neutral.n3c.twins_carry_the_source_group_key_sets_t2", lambda v: sorted(v["data"]["pricePositionInputs"]),
                   expect=["currentPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "pctFromYearHigh", "pctFromYearLow"]),
    _position_case("neutral.n3c.twins_carry_the_source_group_key_sets_t4", lambda v: sorted(v["data"]["earningsInputs"]),
                   expect=["avgSurprisePct", "beatRate", "currentBeatStreak", "momentumFlag", "preRevenue"]),
]

# FINDING: asymmetries of the source groups, unchanged by RN. technicalInputs mirrors t5_inputs, so it inherits them.
_POS_DECISION = "FINDING Q14-N3c: analyze_position_signals runtime asymmetry that RN leaves as it is"

CASES += [
    _position_case("neutral.n3c.finding_t5_macd_is_python_only",
                   lambda v: {"technicalInputs": sorted(v["data"]["technicalInputs"])},
                   mode="pinned", decision=_POS_DECISION,
                   expect_py={"technicalInputs": ["lastClose", "maPosition", "macd", "macdHistogram", "pctFrom200dma", "pctFrom50dma", "rsi14"]},
                   expect_ts={"technicalInputs": ["lastClose", "maPosition", "macdHistogram", "pctFrom200dma", "pctFrom50dma", "rsi14"]}),
    _position_case("neutral.n3c.finding_component_status_keys_and_failed_components_differ",
                   lambda v: {"componentStatusKeys": sorted(v["data"]["componentStatus"]),
                              "hasFailedComponents": "failedComponents" in v["data"]},
                   mode="pinned", decision=_POS_DECISION,
                   expect_py={"componentStatusKeys": ["analystConsensus", "analystUpgrades", "earningsMomentum", "maPosition",
                                                      "priceStats", "technicalIndicators"], "hasFailedComponents": True},
                   expect_ts={"componentStatusKeys": ["movingAveragePosition", "pricePerformance", "technicalIndicators"],
                              "hasFailedComponents": False}),
    _position_case("neutral.n3c.finding_quote_timestamp_is_python_only_and_quote_data_date_comes_from_different_components",
                   lambda v: {"hasQuoteTimestamp": "quoteTimestamp" in v["data"], "quoteDataDate": v["data"]["quoteDataDate"]},
                   components={"get_price_stats": {**PY_COMPONENTS["get_price_stats"], "priceTimestamp": "2026-10-08T20:00:00Z"}},
                   mode="pinned", decision=_POS_DECISION,
                   expect_py={"hasQuoteTimestamp": True, "quoteDataDate": "2026-10-08"},
                   expect_ts={"hasQuoteTimestamp": False, "quoteDataDate": "2026-10-09"}),
    # A ticker list: the Worker's expanded-mode tool batches it into a result per ticker; the Python tool takes a
    # string and passes a list straight through to its components, returning one object whose "ticker" is the list.
    Case(name="neutral.n3c.finding_batch_tickers_are_accepted_by_the_worker_only", ids=["Q14-N3"],
         py=lambda: _py_position_batch(["AAA", "BBB"]),
         ts={"js": _ts_with_clock(GATE_NOW, _worker('const r = await m.tools.callToolResult("analyze_position_signals", '
                                                    '{ticker: ["AAA", "BBB"]}); return r.value;')), "isolate": True},
         routes=POSITION_ROUTES,
         select=lambda v: {"ok": v["ok"], "dataKeys": sorted(v["data"]),
                           "tickerIsList": isinstance(v["data"].get("ticker"), list)},
         mode="pinned", decision=_POS_DECISION,
         expect_py={"ok": True, "tickerIsList": True, "dataKeys": sorted([
             "ticker", "status", "dataDate", "quoteDataDate", "quoteTimestamp", "completedBarDataDate", "analystInputs", "pricePositionInputs", "earningsInputs", "technicalInputs",
             "componentStatus", "failedComponents", "limitedComponents", "recommendedNextAction"])},
         expect_ts={"ok": True, "tickerIsList": False, "dataKeys": ["AAA", "BBB"]}),
]


def _py_position_batch(tickers: list[str]) -> dict:
    async def call():
        stubs = {k: AsyncMock(return_value=json.dumps(v)) for k, v in PY_COMPONENTS.items()}
        with patch.multiple(server, **stubs):
            raw = await server.analyze_position_signals(tickers)
        return _envelope_tool_result("analyze_position_signals", raw)
    return asyncio.run(call())


# analystCount source. Python reads consensus.totalAnalysts, the Worker reads consensus.numberOfAnalysts. Both name the
# rating count today, because each runtime's consensus tool emits the field its own position tool reads; Python emits
# both names, the Worker only numberOfAnalysts. Function level, on a recommendation trend of 2+3+1+1+0 = 7 ratings and
# a price-target contributor count of 3.
from types import SimpleNamespace  # noqa: E402


def _py_consensus() -> dict:
    trend = pd.DataFrame([{"period": "0m", "strongBuy": 2, "buy": 3, "hold": 1, "sell": 1, "strongSell": 0}])
    company = SimpleNamespace(
        fast_info=SimpleNamespace(last_price=129.9), analyst_price_targets={"mean": 150.0, "low": 100.0, "high": 200.0, "median": 150.0},
        upgrades_downgrades=pd.DataFrame(), recommendations_summary=trend, info={"numberOfAnalystOpinions": 3})
    with patch.object(server.yf, "Ticker", lambda _t: company), patch.object(server, "_target_analyst_count", lambda _c: 3):
        return asyncio.run(server.get_analyst_consensus("CNSNS"))


CASES.append(Case(
    name="neutral.n3c.finding_analyst_count_source_totalanalysts_in_python_numberofanalysts_in_the_worker", ids=["Q14-N3"],
    py=_py_consensus,
    ts={"js": _ts_with_clock(GATE_NOW, 'return JSON.parse(await m.yahooFinance.getAnalystConsensus("CNSNS"));'), "isolate": True},
    routes={**YAHOO_AUTH_ROUTES, "https://query1.finance.yahoo.com/v10/finance/quoteSummary/CNSNS?*": {"json": _POS_SUMMARY}},
    select=lambda v: {"ratingCount": v.get("ratingCount"), "numberOfAnalysts": v.get("numberOfAnalysts"),
                      "totalAnalysts": v.get("totalAnalysts"), "targetContributors": v["priceTargets"]["numberOfAnalysts"]},
    mode="pinned", decision=_POS_DECISION,
    expect_py={"ratingCount": 7, "numberOfAnalysts": 7, "totalAnalysts": 7, "targetContributors": 3},
    expect_ts={"ratingCount": 7, "numberOfAnalysts": 7, "totalAnalysts": None, "targetContributors": 3}))


