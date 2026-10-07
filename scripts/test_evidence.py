#!/usr/bin/env python3
"""Evidence composition parity (2.5.1): worker/src/evidence.ts vs yfmcp/evidence.py.

Covers canonical JSON (the bytes an evidence cut is hashed over), the FY0-FY+5
consensus curve with per-provider, per-period coverage states, EPS revision
windows, evidence-quality preflight, the receipt, and the authority boundary
every payload carries.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

from yfmcp import evidence as ev  # noqa: E402

AS_OF = "2026-09-26T08:00:00.000Z"


def _r(v):
    return {"raw": v, "fmt": str(v)}


YAHOO_TREND = [
    {"period": "0q", "endDate": "2026-09-30", "earningsEstimate": {"avg": _r(-0.44), "numberOfAnalysts": _r(6)}},
    {
        "period": "0y", "endDate": "2026-12-31",
        "earningsEstimate": {"avg": _r(-2.27), "low": _r(-2.49), "high": _r(-2.04), "numberOfAnalysts": _r(8)},
        "revenueEstimate": {"avg": _r(168_000_000), "low": _r(150_980_000), "high": _r(189_000_000), "numberOfAnalysts": _r(12)},
        "epsTrend": {"current": _r(-2.27), "7daysAgo": _r(-2.26), "30daysAgo": _r(-1.39), "60daysAgo": _r(-1.5), "90daysAgo": _r(-1.5)},
        "epsRevisions": {"upLast7days": _r(0), "upLast30days": _r(0), "downLast30days": _r(4), "downLast7Days": _r(1)},
    },
    {
        "period": "+1y", "endDate": "2027-12-31",
        "earningsEstimate": {"avg": _r(-1.1), "low": _r(-1.98), "high": _r(1.48), "numberOfAnalysts": _r(9)},
        "revenueEstimate": {"avg": _r(520_000_000), "low": _r(345_057_500), "high": _r(834_100_000), "numberOfAnalysts": _r(11)},
        "epsTrend": {"current": _r(-1.1), "7daysAgo": _r(-1.05), "30daysAgo": _r(-0.69), "60daysAgo": None, "90daysAgo": _r(-0.65)},
        "epsRevisions": {"upLast7days": _r(1), "upLast30days": _r(1), "downLast30days": _r(2)},
    },
]

AV = {"symbol": "ASTS", "estimates": [
    {"date": "2027-12-31", "horizon": "fiscal year", "eps_estimate_average": "-1.1016", "eps_estimate_high": "1.4800", "eps_estimate_low": "-1.9800",
     "eps_estimate_analyst_count": "9.0000", "eps_estimate_average_7_days_ago": "-1.0493", "eps_estimate_average_30_days_ago": "-0.6863",
     "eps_estimate_average_60_days_ago": "-0.6476", "eps_estimate_average_90_days_ago": "-0.6476", "eps_estimate_revision_up_trailing_7_days": "1.0000",
     "eps_estimate_revision_down_trailing_7_days": None, "eps_estimate_revision_up_trailing_30_days": "1.0000", "eps_estimate_revision_down_trailing_30_days": "2.0000",
     "revenue_estimate_average": "650775060.00", "revenue_estimate_high": "834100000.00", "revenue_estimate_low": "345057500.00", "revenue_estimate_analyst_count": "11.00"},
    {"date": "2026-12-31", "horizon": "fiscal year", "eps_estimate_average": "-2.2839", "eps_estimate_high": "-2.0400", "eps_estimate_low": "-2.4900",
     "eps_estimate_analyst_count": "8.0000", "eps_estimate_average_7_days_ago": "-2.2815", "eps_estimate_average_30_days_ago": "-1.3862",
     "eps_estimate_average_60_days_ago": "-1.5051", "eps_estimate_average_90_days_ago": "-1.5051", "eps_estimate_revision_up_trailing_7_days": "0.0000",
     "eps_estimate_revision_down_trailing_7_days": None, "eps_estimate_revision_up_trailing_30_days": "0.0000", "eps_estimate_revision_down_trailing_30_days": "4.0000",
     "revenue_estimate_average": "168843670.00", "revenue_estimate_high": "189000000.00", "revenue_estimate_low": "150980000.00", "revenue_estimate_analyst_count": "12.00"},
    {"date": "2026-09-30", "horizon": "fiscal quarter", "eps_estimate_average": "-0.4463", "eps_estimate_analyst_count": "6.0000"},
    {"date": "2021-12-31", "horizon": "fiscal year", "eps_estimate_average": "0.0000", "eps_estimate_analyst_count": "0.0000",
     "revenue_estimate_average": "0.00", "revenue_estimate_analyst_count": "0.00"},
]}

# 2.5.22: Alpha Vantage rows carrying Yahoo's published figures to the last digit (one upstream feed, not two checks).
AV_IDENTICAL = {"symbol": "ASTS", "estimates": [
    {"date": "2026-12-31", "horizon": "fiscal year", "eps_estimate_average": "-2.27", "eps_estimate_high": "-2.04", "eps_estimate_low": "-2.49",
     "eps_estimate_analyst_count": "8", "revenue_estimate_average": "168000000", "revenue_estimate_high": "189000000", "revenue_estimate_low": "150980000",
     "revenue_estimate_analyst_count": "12"},
    {"date": "2027-12-31", "horizon": "fiscal year", "eps_estimate_average": "-1.1", "eps_estimate_high": "1.48", "eps_estimate_low": "-1.98",
     "eps_estimate_analyst_count": "9", "revenue_estimate_average": "520000000", "revenue_estimate_high": "834100000", "revenue_estimate_low": "345057500",
     "revenue_estimate_analyst_count": "11"},
]}

# 2.5.23 (F-013): the live ANET FY2027 EPS cell, one feed published at two precisions: Yahoo's 5.20199 is Alpha
# Vantage's "5.2020". AV_NEAR differs at Alpha Vantage's fourth place; AV_ZEROS shows trailing zeros count as precision.
F013_TREND = [{"period": "0y", "endDate": "2026-12-31",
               "earningsEstimate": {"avg": _r(5.20199), "high": _r(5.73), "low": _r(4.66), "numberOfAnalysts": _r(29)},
               "revenueEstimate": {"avg": _r(2.54), "high": _r(2.9), "low": _r(2.1), "numberOfAnalysts": _r(29)}}]
F013_AV = {"symbol": "ANET", "estimates": [{"date": "2026-12-31", "horizon": "fiscal year", "eps_estimate_average": "5.2020", "eps_estimate_high": "5.7300",
                                            "eps_estimate_low": "4.6600", "eps_estimate_analyst_count": "29.0000", "revenue_estimate_average": "2.5000",
                                            "revenue_estimate_high": "2.9000", "revenue_estimate_low": "2.1000", "revenue_estimate_analyst_count": "29.00"}]}
F013_AV_NEAR = {"symbol": "ANET", "estimates": [dict(F013_AV["estimates"][0], eps_estimate_average="5.2021")]}

# A 52/53-week filer with two thinly covered years, a TWD ADR, and a provider fiscal-year mismatch.
THIN_TREND = [
    {"period": "0y", "endDate": "2026-09-26", "earningsEstimate": {"avg": 1.2, "numberOfAnalysts": 2}, "revenueEstimate": {"avg": 5e8, "numberOfAnalysts": 2}},
    {"period": "+1y", "endDate": "2027-09-25", "earningsEstimate": {"avg": 1.5, "numberOfAnalysts": 3}, "revenueEstimate": {}},
]
ADR_TREND = [{"period": "0y", "endDate": "2026-12-31", "earningsEstimate": {"avg": 60.1, "numberOfAnalysts": 20, "earningsCurrency": "TWD"},
              "revenueEstimate": {"avg": 3.6e12, "numberOfAnalysts": 20, "revenueCurrency": "TWD"}}]
ADR_OTHER = {"provider": "other", "status": "OK", "retrievedAt": AS_OF, "providerTimestamp": None, "message": None, "periods": [
    {"providerPeriodLabel": "fiscal year", "fiscalYearEnd": "2026-12-31", "fiscalYearEndBasis": "PROVIDER_STATED",
     "eps": {"mean": 9.4, "high": None, "low": None, "analystCount": 20, "currency": "USD", "currencyBasis": "PROVIDER_STATED"},
     "revenue": {"mean": None, "high": None, "low": None, "analystCount": None, "currency": None, "currencyBasis": "NOT_STATED"},
     "epsTrend": None, "epsRevisions": None},
    {"providerPeriodLabel": "fiscal year", "fiscalYearEnd": "2027-09-30", "fiscalYearEndBasis": "PROVIDER_STATED",
     "eps": {"mean": 11.0, "high": None, "low": None, "analystCount": 20, "currency": "USD", "currencyBasis": "PROVIDER_STATED"},
     "revenue": {"mean": None, "high": None, "low": None, "analystCount": None, "currency": None, "currencyBasis": "NOT_STATED"},
     "epsTrend": None, "epsRevisions": None},
]}
ADR_TREND_NEXT = ADR_TREND + [{"period": "+1y", "endDate": "2027-12-31", "earningsEstimate": {"avg": 70.0, "numberOfAnalysts": 20, "earningsCurrency": "TWD"}}]
# yfinance DataFrames carry no endDate; the local server derives it.
DERIVED_TREND = [{"period": "0y", "earningsEstimate": {"avg": 3.0, "numberOfAnalysts": 12}, "revenueEstimate": {"avg": 1e9, "numberOfAnalysts": 12}}]

FILINGS = [
    {"form": "10-Q", "filingDate": "2026-08-10", "reportDate": "2026-06-30", "items": "", "isInlineXBRL": True},
    {"form": "8-K", "filingDate": "2026-08-10", "reportDate": "2026-08-10", "items": "2.02,9.01", "isInlineXBRL": True},
    {"form": "8-K", "filingDate": "2026-08-05", "reportDate": "2026-08-05", "items": "7.01,9.01", "isInlineXBRL": False},
    {"form": "10-K", "filingDate": "2026-03-01", "reportDate": "2025-12-31", "items": "", "isInlineXBRL": True},
    {"form": "8-K", "filingDate": "2026-03-01", "reportDate": "2026-03-01", "items": "2.02", "isInlineXBRL": True},
]
STALE_FILINGS = [{"form": "10-K", "filingDate": "2025-03-01", "reportDate": "2024-12-31", "items": "", "isInlineXBRL": False}]
# Foreign private issuers (2.5.15): one annual report a year and interim results on 6-K; stale only when the next
# 20-F is overdue. AS_OF_LATER is 273 days after the 2025-12-31 period end.
# 2.5.16: DG as Yahoo states its year (a month end) and the naming with the calendar its SEC year ends fit.
DG_TREND = [{"period": "0y", "endDate": "2027-01-31", "earningsEstimate": {"avg": 6.0, "numberOfAnalysts": 20}, "revenueEstimate": {}}]
DG_NAMING = {"offset": -1, "basis": "SEC_STATED_FISCAL_YEAR", "periodEnd": "2026-01-30", "statedFiscalYear": 2025,
             "calendar": {"patterns": ["WEEKDAY_NEAREST_MONTH_END"], "month": 1, "weekday": "Friday", "basis": "SEC_ANNUAL_PERIOD_ENDS",
                          "periodEnds": ["2026-01-30", "2025-01-31", "2024-02-02"]}}
AS_OF_LATER = "2026-09-30T08:00:00.000Z"
FPI_FILINGS = [
    {"form": "20-F", "filingDate": "2026-04-30", "reportDate": "2025-12-31", "items": "", "isInlineXBRL": True},
    {"form": "6-K", "filingDate": "2026-08-20", "reportDate": "2026-08-20", "items": "", "isInlineXBRL": False},
    {"form": "6-K", "filingDate": "2026-07-16", "reportDate": "2026-07-16", "items": "", "isInlineXBRL": False},
]
FPI_STALE_FILINGS = [
    {"form": "20-F", "filingDate": "2025-04-30", "reportDate": "2024-12-31", "items": "", "isInlineXBRL": True},
    {"form": "6-K", "filingDate": "2026-08-20", "reportDate": "2026-08-20", "items": "", "isInlineXBRL": False},
]
FPI_40FA_FILINGS = [{"form": "40-F/A", "filingDate": "2026-03-20", "reportDate": "2025-12-31", "items": "", "isInlineXBRL": False}]
# 2.5.23: ANET-shaped rows. Alpha Vantage FY0 revenue averages above its own high (F-001) and its FY0 EPS is still on
# the pre-split count after a 4-for-1 split 16 days before as-of (F-002); Yahoo's 30-90 day trend spans that split.
ANET_AS_OF = "2026-10-06T04:00:00.000Z"
ANET_TREND = [
    {"period": "0y", "endDate": "2026-12-31",
     "earningsEstimate": {"avg": 2.8, "high": 2.95, "low": 2.7, "numberOfAnalysts": 20, "earningsCurrency": "USD"},
     "revenueEstimate": {"avg": 9.0e9, "high": 9.2e9, "low": 8.8e9, "numberOfAnalysts": 20, "revenueCurrency": "USD"},
     "epsTrend": {"current": 2.8, "7daysAgo": 2.79, "30daysAgo": 11.0, "60daysAgo": 10.9, "90daysAgo": 10.8}},
    {"period": "+1y", "endDate": "2027-12-31",
     "earningsEstimate": {"avg": 3.3, "high": 3.5, "low": 3.1, "numberOfAnalysts": 18},
     "revenueEstimate": {"avg": 10.5e9, "high": 11.5e9, "low": 10e9, "numberOfAnalysts": 18}},
]
ANET_AV = {"symbol": "ANET", "estimates": [
    {"date": "2026-12-31", "horizon": "fiscal year", "eps_estimate_average": "11.2", "eps_estimate_high": "11.8", "eps_estimate_low": "10.8",
     "eps_estimate_analyst_count": "20", "eps_estimate_average_7_days_ago": "11.1", "eps_estimate_average_30_days_ago": "11.0",
     "revenue_estimate_average": "9050000000", "revenue_estimate_high": "8990000000", "revenue_estimate_low": "8800000000", "revenue_estimate_analyst_count": "20"},
    {"date": "2027-12-31", "horizon": "fiscal year", "eps_estimate_average": "3.31", "eps_estimate_high": "3.5", "eps_estimate_low": "3.1",
     "eps_estimate_analyst_count": "18", "revenue_estimate_average": "10600000000", "revenue_estimate_high": "10400000000", "revenue_estimate_low": "10500000000",
     "revenue_estimate_analyst_count": "18"},
]}
ANET_SPLITS = {"status": "OK", "splits": [{"date": "2021-11-18", "ratio": 4}, {"date": "2026-09-20", "ratio": 4}]}
ANET_OLD_SPLITS = {"status": "OK", "splits": [{"date": "2021-11-18", "ratio": 4}]}
SPLITS_FAILED = {"status": "PROVIDER_ERROR", "splits": [], "message": "chart down"}
# A 1-for-10 reverse split: the unadjusted provider shows the smaller loss per share.
REVERSE_SPLITS = {"status": "OK", "splits": [{"date": "2026-08-01", "ratio": 0.1}]}
REVERSE_A = {"provider": "yahoo_finance", "status": "OK", "retrievedAt": ANET_AS_OF, "providerTimestamp": None, "message": None, "periods": [
    {"providerPeriodLabel": "0y", "fiscalYearEnd": "2026-12-31", "fiscalYearEndBasis": "PROVIDER_STATED",
     "eps": {"mean": -5.0, "high": -4.0, "low": -6.0, "analystCount": 5, "currency": "USD", "currencyBasis": "PROVIDER_STATED"},
     "revenue": {"mean": None, "high": None, "low": None, "analystCount": None, "currency": None, "currencyBasis": "NOT_STATED"},
     "epsTrend": None, "epsRevisions": None}]}
REVERSE_B = {**REVERSE_A, "provider": "alpha_vantage", "periods": [{**REVERSE_A["periods"][0], "providerPeriodLabel": "fiscal year",
    "eps": {"mean": -0.52, "high": -0.4, "low": -0.6, "analystCount": 5, "currency": None, "currencyBasis": "NOT_STATED"}}]}
F013_DECIMAL_CASES = ["5.2020", "168843670.00", "-2", "1e-7", " 3.10 ", "abc", "", 5.20199, 168843670, {"raw": "5.20"}, {"raw": 2.54, "fmt": "2.54"}, None, True]
QUOTE_FAILED = {"price": None, "currency": None, "priceTime": None, "status": "PROVIDER_ERROR"}
QUOTE_NO_DATA = {"price": None, "currency": None, "priceTime": None, "status": "NO_DATA"}

CANONICAL_CASES = [
    {"b": [1, 2.5, -0.0, 1e21, 1.5e21, 1e-7, 0.000001, 123.456, 31520000.0, -2.2839, 1e16, 5e-324], "a": "é\n\"q\"\u0001", "c": None, "d": True},
    {"z": {"y": [], "x": {}}, "m": 0.1, "n": 100, "k": "ASTS"},
]

COMPONENT_TEXTS = [
    ("extract_capital_structure", json.dumps({"basis": "COMPANY_DISCLOSED", "source": {"filingDate": "2026-08-10", "periodEnd": "2026-06-30", "form": "10-Q"}, "warnings": [{"code": "X"}]})),
    ("extract_guidance", json.dumps({"status": "GUIDANCE_NOT_AVAILABLE", "reason": "No guidance found"})),
    ("extract_dilution_bridge", json.dumps({"error": True, "code": "TICKER_NOT_FOUND", "message": "no CIK"})),
    ("extract_dilution_bridge_partial", json.dumps({"status": "PARTIAL", "unresolved": ["warrants"]})),
    ("market_series_incomplete", json.dumps({"status": "INCOMPLETE", "message": "missing completed bar"})),
    ("list_sec_material_filings", json.dumps({"ok": False, "data": None, "error": {"code": "RATE_LIMIT", "message": "slow"}})),
    ("get_quote", "not json"),
]

# An evidence-only action's result object always carries AUTHORITY_BOUNDARY (2.5.8).
COMPLETE = json.dumps({"status": "OK", **ev.AUTHORITY_BOUNDARY})
BOUNDARY_CASES = [
    ["reconcile_metric_sources", json.dumps({"ticker": "DG", "metric": "revenue", "status": "PERIOD_NOT_FOUND", "spec": "FY2026"})],
    ["get_historical_valuation_context", json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "At most 5 peers."})],
    ["get_quote", json.dumps({"status": "OK"})],
    ["get_guidance_history", COMPLETE],
    ["get_share_count_scenarios", json.dumps({"status": "OK", "decisionUse": "EVIDENCE_ONLY", "priceTarget": 5})],
    ["list_evidence_cuts", "not json"],
    ["get_evidence_quality", "[1, 2]"],
    ["build_valuation_evidence_pack", json.dumps({"ok": True, "data": {}, "meta": {}, "error": None})],
]


# 2.5.29: a stored Alpha Vantage snapshot standing in for a refused live request.
STALE_FETCHED_AT = "2026-09-25T02:00:00.000Z"
# [stored, live status, live message, asOf, expected ticker]; stored is "good", "bad" (not JSON) or "none".
STALE_CASES = [
    ["good", "RATE_LIMIT", "25 requests per day", AS_OF, "asts"],
    ["good", "TIMEOUT", "", AS_OF, "ASTS"],
    ["good", "PROVIDER_ERROR", None, AS_OF, "asts"],
    ["good", "AUTH_ERROR", "bad key", AS_OF, "asts"],
    ["good", "SOURCE_UNCONFIGURED", None, AS_OF, "asts"],
    ["good", "RATE_LIMIT", None, "2026-10-02T02:00:00.000Z", "asts"],
    ["good", "RATE_LIMIT", None, "2026-10-02T02:00:00.001Z", "asts"],
    ["good", "RATE_LIMIT", None, "2026-09-25T01:59:59.000Z", "asts"],
    ["good", "RATE_LIMIT", None, AS_OF, "msft"],
    ["bad", "RATE_LIMIT", None, AS_OF, "asts"],
    ["none", "RATE_LIMIT", None, AS_OF, "asts"],
]


def _stale_stored(kind: str) -> dict | None:
    key = ev.provider_snapshot_key("alpha_vantage", "EARNINGS_ESTIMATES", "asts", STALE_FETCHED_AT)
    if kind == "good":
        return {"key": key, "text": ev.provider_snapshot_body("alpha_vantage", "EARNINGS_ESTIMATES", "asts", STALE_FETCHED_AT, AV)}
    return {"key": key, "text": "not json"} if kind == "bad" else None


def _stale_outputs(yahoo: dict) -> dict:
    expect = lambda t: {"provider": "alpha_vantage", "operation": "EARNINGS_ESTIMATES", "ticker": t}  # noqa: E731
    cases = [ev.stale_snapshot(_stale_stored(kind), expect(t), {"status": st, "message": msg}, at) for kind, st, msg, at, t in STALE_CASES]
    hit = cases[0]
    av_stale = ev.alpha_vantage_consensus_input(hit["payload"], retrieved_at=hit["fetchedAt"], stale_fallback=hit["staleFallback"])
    return {
        "staleCases": [None if c is None else {"fetchedAt": c["fetchedAt"], "staleFallback": c["staleFallback"], "estimates": len(c["payload"]["estimates"])} for c in cases],
        "staleBody": _stale_stored("good"),
        "staleCandidateKeys": ev.provider_snapshot_candidate_keys("alpha_vantage", "EARNINGS_ESTIMATES", "asts", "2026-10-03T08:00:00.000Z"),
        "curveAvStale": ev.build_consensus_curve("asts", [yahoo, av_stale], AS_OF),
        "revisionsAvStale": ev.build_eps_revisions("asts", [yahoo, av_stale], AS_OF),
    }


def _boundary_out(raw: object) -> object:
    try:
        return json.loads(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return raw


def _python_outputs() -> dict:
    yahoo = ev.yahoo_consensus_input(YAHOO_TREND, retrieved_at=AS_OF, financial_currency="USD")
    av = ev.alpha_vantage_consensus_input(AV, retrieved_at=AS_OF)
    thin = ev.yahoo_consensus_input(THIN_TREND, retrieved_at=AS_OF, financial_currency="USD")
    adr = ev.yahoo_consensus_input(ADR_TREND_NEXT, retrieved_at=AS_OF)
    derived = ev.yahoo_consensus_input(DERIVED_TREND, retrieved_at=AS_OF, financial_currency="EUR",
                                       fiscal_year_ends={"0y": "2026-12-31"}, fiscal_year_end_basis="DERIVED_FROM_NEXT_FISCAL_YEAR_END")
    conflict = ev.alpha_vantage_consensus_input({"estimates": [dict(AV["estimates"][1], revenue_estimate_average="120000000")]}, retrieved_at=AS_OF)
    curve = ev.build_consensus_curve("asts", [yahoo, av], AS_OF)
    quality = ev.evidence_quality(ticker="ASTS", as_of=AS_OF, quote={"price": 81.2, "currency": "USD", "priceTime": "2026-09-25T20:00:00.000Z", "status": "OK"},
                                  filings=FILINGS, filings_status="OK", consensus=curve, storage_available=True)
    failed_yahoo = ev.yahoo_consensus_input([], retrieved_at=AS_OF, status="PROVIDER_ERROR", message="down")
    failed_av = ev.alpha_vantage_consensus_input({}, retrieved_at=AS_OF, status="RATE_LIMIT", message="25 requests per day")
    curve_failed = ev.build_consensus_curve("asts", [failed_yahoo, failed_av], AS_OF)
    curve_failed_one = ev.build_consensus_curve("asts", [failed_yahoo, av], AS_OF)
    curve_no_data = ev.build_consensus_curve("asts", [ev.yahoo_consensus_input([], retrieved_at=AS_OF, status="NO_DATA")], AS_OF)
    av_identical = ev.alpha_vantage_consensus_input(AV_IDENTICAL, retrieved_at=AS_OF)
    av_limited = ev.alpha_vantage_consensus_input({}, retrieved_at=AS_OF, status="RATE_LIMIT", message="25 requests per day")
    av_limited_silent = ev.alpha_vantage_consensus_input({}, retrieved_at=AS_OF, status="RATE_LIMIT", message="")
    anet_yahoo = ev.yahoo_consensus_input(ANET_TREND, retrieved_at=ANET_AS_OF, financial_currency="USD")
    anet_av = ev.alpha_vantage_consensus_input(ANET_AV, retrieved_at=ANET_AS_OF)
    f013_yahoo = ev.yahoo_consensus_input(F013_TREND, retrieved_at=AS_OF, financial_currency="USD")

    def later(**kw: object) -> dict:
        return ev.evidence_quality(ticker="tsm", as_of=AS_OF_LATER, quote=kw.get("quote", {"price": 81.2, "currency": "USD", "priceTime": "2026-09-29T20:00:00.000Z", "status": "OK"}),  # type: ignore[arg-type]
                                   filings=kw.get("filings", FILINGS), filings_status="OK", consensus=kw.get("consensus"), storage_available=True)  # type: ignore[arg-type]

    components = {name: ev.component_from_tool_text(name, text, AS_OF) for name, text in COMPONENT_TEXTS}
    hashes = {name: hashlib.sha256(ev.canonical_json(c).encode()).hexdigest() for name, c in components.items()}
    receipt = ev.build_receipt(ticker="asts", evidence_cutoff=AS_OF, server_version="2.5.0", build_sha="abc", runtime="test",
                               components=components, component_hashes=hashes, components_sha256="0" * 64)
    return {
        "canonical": [ev.canonical_json(c) for c in CANONICAL_CASES],
        "curve": curve,
        "curveOneYear": ev.build_consensus_curve("asts", [yahoo, av], AS_OF, {**ev.DEFAULT_CONSENSUS_POLICY, "horizonYears": 1}),
        "curveThin": ev.build_consensus_curve("thin", [thin], AS_OF),
        "curveAdr": ev.build_consensus_curve("tsm", [adr, ADR_OTHER], AS_OF),
        "curveDerived": ev.build_consensus_curve("sap", [derived], AS_OF),
        "curveConflict": ev.build_consensus_curve("asts", [yahoo, conflict], AS_OF),
        "curveAvOnly": ev.build_consensus_curve("asts", [av], AS_OF),
        "curveIdentical": ev.build_consensus_curve("asts", [yahoo, av_identical], AS_OF),
        "curveAvRateLimited": ev.build_consensus_curve("asts", [yahoo, av_limited], AS_OF),
        "curveAvRateLimitedSilent": ev.build_consensus_curve("asts", [yahoo, av_limited_silent], AS_OF),
        "curveBothFailed": curve_failed,
        "curveDgCalendar": ev.build_consensus_curve("dg", [ev.yahoo_consensus_input(DG_TREND, retrieved_at=AS_OF, financial_currency="USD")], AS_OF, None, DG_NAMING),
        "curveDgNoNaming": ev.build_consensus_curve("dg", [ev.yahoo_consensus_input(DG_TREND, retrieved_at=AS_OF, financial_currency="USD")], AS_OF),
        "curveNone": ev.build_consensus_curve("none", [ev.yahoo_consensus_input([], retrieved_at=AS_OF, status="PROVIDER_ERROR", message="down")], AS_OF),
        "revisions": ev.build_eps_revisions("asts", [yahoo, av], AS_OF),
        "curveAnet": ev.build_consensus_curve("anet", [anet_yahoo, anet_av], ANET_AS_OF, None, None, ANET_SPLITS),
        "curveAnetOldSplit": ev.build_consensus_curve("anet", [anet_yahoo, anet_av], ANET_AS_OF, None, None, ANET_OLD_SPLITS),
        "curveAnetSplitsFailed": ev.build_consensus_curve("anet", [anet_yahoo, anet_av], ANET_AS_OF, None, None, SPLITS_FAILED),
        "curveAnetAvOnly": ev.build_consensus_curve("anet", [anet_av], ANET_AS_OF, None, None, ANET_SPLITS),
        "curveReverse": ev.build_consensus_curve("rev", [REVERSE_A, REVERSE_B], ANET_AS_OF, None, None, REVERSE_SPLITS),
        "curveF013": ev.build_consensus_curve("anet", [f013_yahoo, ev.alpha_vantage_consensus_input(F013_AV, retrieved_at=AS_OF)], AS_OF),
        "curveF013Near": ev.build_consensus_curve("anet", [f013_yahoo, ev.alpha_vantage_consensus_input(F013_AV_NEAR, retrieved_at=AS_OF)], AS_OF),
        "publishedDecimals": [ev.published_decimals(v) for v in F013_DECIMAL_CASES],
        "revisionsAnet": ev.build_eps_revisions("anet", [anet_yahoo, anet_av], ANET_AS_OF, ANET_SPLITS),
        "revisionsAnetSplitsFailed": ev.build_eps_revisions("anet", [anet_yahoo, anet_av], ANET_AS_OF, SPLITS_FAILED),
        "quality": quality,
        "qualityStale": ev.evidence_quality(ticker="old", as_of=AS_OF, quote=None, filings=STALE_FILINGS, filings_status="OK", consensus=None, storage_available=False),
        "qualityNoSec": ev.evidence_quality(ticker="iqe.l", as_of=AS_OF, quote={"price": 12.5, "currency": "GBp", "priceTime": "2026-09-10T16:00:00.000Z", "status": "OK"},
                                            filings=None, filings_status="TICKER_NOT_FOUND", consensus=curve, storage_available=True),
        "qualityFpiReady": later(filings=FPI_FILINGS),
        "qualityFpiStale": later(filings=FPI_STALE_FILINGS),
        "qualityFpi40FA": later(filings=FPI_40FA_FILINGS),
        "qualityQuarterlyLater": later(filings=FILINGS),
        "qualityQuoteFailed": later(quote=QUOTE_FAILED, filings=FPI_FILINGS),
        "qualityQuoteNoData": later(quote=QUOTE_NO_DATA, filings=FPI_FILINGS),
        "qualityConsensusFailed": later(consensus=curve_failed),
        "qualityConsensusOneFailed": later(consensus=curve_failed_one),
        "qualityConsensusNoData": later(consensus=curve_no_data),
        "components": components,
        "receipt": receipt,
        "cutId": ev.evidence_cut_id("asts", AS_OF, "a" * 64),
        "parsed": ev.parse_evidence_cut_id(ev.evidence_cut_id("brk.b", AS_OF, "b" * 64)),
        "parsedBad": ev.parse_evidence_cut_id("ec1_../x_20260926T080000Z_" + "c" * 64),
        "observationKey": ev.consensus_observation_key("asts", AS_OF),
        "boundary": [_boundary_out(ev.with_authority_boundary(a, raw)) for a, raw in BOUNDARY_CASES],
        "boundaryActions": sorted(ev.EVIDENCE_ONLY_ACTIONS),
        **_stale_outputs(yahoo),
    }


_HARNESS = r"""
const [bundleUrl, fixturesPath] = process.argv.slice(-2);
const m = await import(bundleUrl);
const { readFileSync } = await import("node:fs");
const f = JSON.parse(readFileSync(fixturesPath, "utf8"));
const AS_OF = f.asOf;
const yahoo = m.yahooConsensusInput(f.yahooTrend, { retrievedAt: AS_OF, financialCurrency: "USD" });
const av = m.alphaVantageConsensusInput(f.av, { retrievedAt: AS_OF });
const thin = m.yahooConsensusInput(f.thinTrend, { retrievedAt: AS_OF, financialCurrency: "USD" });
const adr = m.yahooConsensusInput(f.adrTrend, { retrievedAt: AS_OF });
const derived = m.yahooConsensusInput(f.derivedTrend, { retrievedAt: AS_OF, financialCurrency: "EUR", fiscalYearEnds: { "0y": "2026-12-31" }, fiscalYearEndBasis: "DERIVED_FROM_NEXT_FISCAL_YEAR_END" });
const conflict = m.alphaVantageConsensusInput({ estimates: [{ ...f.av.estimates[1], revenue_estimate_average: "120000000" }] }, { retrievedAt: AS_OF });
const curve = m.buildConsensusCurve("asts", [yahoo, av], AS_OF);
const quality = m.evidenceQuality({ ticker: "ASTS", asOf: AS_OF, quote: { price: 81.2, currency: "USD", priceTime: "2026-09-25T20:00:00.000Z", status: "OK" }, filings: f.filings, filingsStatus: "OK", consensus: curve, storageAvailable: true });
const failedYahoo = m.yahooConsensusInput([], { retrievedAt: AS_OF, status: "PROVIDER_ERROR", message: "down" });
const failedAv = m.alphaVantageConsensusInput({}, { retrievedAt: AS_OF, status: "RATE_LIMIT", message: "25 requests per day" });
const curveFailed = m.buildConsensusCurve("asts", [failedYahoo, failedAv], AS_OF);
const curveFailedOne = m.buildConsensusCurve("asts", [failedYahoo, av], AS_OF);
const curveNoData = m.buildConsensusCurve("asts", [m.yahooConsensusInput([], { retrievedAt: AS_OF, status: "NO_DATA" })], AS_OF);
const avIdentical = m.alphaVantageConsensusInput(f.avIdentical, { retrievedAt: AS_OF });
const avLimited = m.alphaVantageConsensusInput({}, { retrievedAt: AS_OF, status: "RATE_LIMIT", message: "25 requests per day" });
const avLimitedSilent = m.alphaVantageConsensusInput({}, { retrievedAt: AS_OF, status: "RATE_LIMIT", message: "" });
const anetYahoo = m.yahooConsensusInput(f.anetTrend, { retrievedAt: f.anetAsOf, financialCurrency: "USD" });
const anetAv = m.alphaVantageConsensusInput(f.anetAv, { retrievedAt: f.anetAsOf });
const f013Yahoo = m.yahooConsensusInput(f.f013Trend, { retrievedAt: AS_OF, financialCurrency: "USD" });
const later = (o) => m.evidenceQuality({ ticker: "tsm", asOf: f.asOfLater, quote: "quote" in o ? o.quote : { price: 81.2, currency: "USD", priceTime: "2026-09-29T20:00:00.000Z", status: "OK" }, filings: "filings" in o ? o.filings : f.filings, filingsStatus: "OK", consensus: o.consensus ?? null, storageAvailable: true });
const components = Object.fromEntries(f.componentTexts.map(([name, text]) => [name, m.componentFromToolText(name, text, AS_OF)]));
const { createHash } = await import("node:crypto");
const hashes = Object.fromEntries(Object.entries(components).map(([k, c]) => [k, createHash("sha256").update(m.canonicalJson(c)).digest("hex")]));
const receipt = m.buildReceipt({ ticker: "asts", evidenceCutoff: AS_OF, serverVersion: "2.5.0", buildSha: "abc", runtime: "test", components, componentHashes: hashes, componentsSha256: "0".repeat(64) });
const out = {
  canonical: f.canonical.map((c) => m.canonicalJson(c)),
  curve,
  curveOneYear: m.buildConsensusCurve("asts", [yahoo, av], AS_OF, { ...m.DEFAULT_CONSENSUS_POLICY, horizonYears: 1 }),
  curveThin: m.buildConsensusCurve("thin", [thin], AS_OF),
  curveAdr: m.buildConsensusCurve("tsm", [adr, f.adrOther], AS_OF),
  curveDerived: m.buildConsensusCurve("sap", [derived], AS_OF),
  curveConflict: m.buildConsensusCurve("asts", [yahoo, conflict], AS_OF),
  curveAvOnly: m.buildConsensusCurve("asts", [av], AS_OF),
  curveIdentical: m.buildConsensusCurve("asts", [yahoo, avIdentical], AS_OF),
  curveAvRateLimited: m.buildConsensusCurve("asts", [yahoo, avLimited], AS_OF),
  curveAvRateLimitedSilent: m.buildConsensusCurve("asts", [yahoo, avLimitedSilent], AS_OF),
  curveBothFailed: curveFailed,
  curveDgCalendar: m.buildConsensusCurve("dg", [m.yahooConsensusInput(f.dgTrend, { retrievedAt: AS_OF, financialCurrency: "USD" })], AS_OF, undefined, f.dgNaming),
  curveDgNoNaming: m.buildConsensusCurve("dg", [m.yahooConsensusInput(f.dgTrend, { retrievedAt: AS_OF, financialCurrency: "USD" })], AS_OF),
  curveNone: m.buildConsensusCurve("none", [m.yahooConsensusInput([], { retrievedAt: AS_OF, status: "PROVIDER_ERROR", message: "down" })], AS_OF),
  revisions: m.buildEpsRevisions("asts", [yahoo, av], AS_OF),
  curveAnet: m.buildConsensusCurve("anet", [anetYahoo, anetAv], f.anetAsOf, undefined, null, f.anetSplits),
  curveAnetOldSplit: m.buildConsensusCurve("anet", [anetYahoo, anetAv], f.anetAsOf, undefined, null, f.anetOldSplits),
  curveAnetSplitsFailed: m.buildConsensusCurve("anet", [anetYahoo, anetAv], f.anetAsOf, undefined, null, f.splitsFailed),
  curveAnetAvOnly: m.buildConsensusCurve("anet", [anetAv], f.anetAsOf, undefined, null, f.anetSplits),
  curveReverse: m.buildConsensusCurve("rev", [f.reverseA, f.reverseB], f.anetAsOf, undefined, null, f.reverseSplits),
  curveF013: m.buildConsensusCurve("anet", [f013Yahoo, m.alphaVantageConsensusInput(f.f013Av, { retrievedAt: AS_OF })], AS_OF),
  curveF013Near: m.buildConsensusCurve("anet", [f013Yahoo, m.alphaVantageConsensusInput(f.f013AvNear, { retrievedAt: AS_OF })], AS_OF),
  publishedDecimals: f.f013DecimalCases.map((v) => m.publishedDecimals(v)),
  revisionsAnet: m.buildEpsRevisions("anet", [anetYahoo, anetAv], f.anetAsOf, f.anetSplits),
  revisionsAnetSplitsFailed: m.buildEpsRevisions("anet", [anetYahoo, anetAv], f.anetAsOf, f.splitsFailed),
  quality,
  qualityStale: m.evidenceQuality({ ticker: "old", asOf: AS_OF, quote: null, filings: f.staleFilings, filingsStatus: "OK", consensus: null, storageAvailable: false }),
  qualityNoSec: m.evidenceQuality({ ticker: "iqe.l", asOf: AS_OF, quote: { price: 12.5, currency: "GBp", priceTime: "2026-09-10T16:00:00.000Z", status: "OK" }, filings: null, filingsStatus: "TICKER_NOT_FOUND", consensus: curve, storageAvailable: true }),
  qualityFpiReady: later({ filings: f.fpiFilings }),
  qualityFpiStale: later({ filings: f.fpiStaleFilings }),
  qualityFpi40FA: later({ filings: f.fpi40faFilings }),
  qualityQuarterlyLater: later({ filings: f.filings }),
  qualityQuoteFailed: later({ quote: f.quoteFailed, filings: f.fpiFilings }),
  qualityQuoteNoData: later({ quote: f.quoteNoData, filings: f.fpiFilings }),
  qualityConsensusFailed: later({ consensus: curveFailed }),
  qualityConsensusOneFailed: later({ consensus: curveFailedOne }),
  qualityConsensusNoData: later({ consensus: curveNoData }),
  components,
  receipt,
  cutId: m.evidenceCutId("asts", AS_OF, "a".repeat(64)),
  parsed: m.parseEvidenceCutId(m.evidenceCutId("brk.b", AS_OF, "b".repeat(64))),
  parsedBad: m.parseEvidenceCutId("ec1_../x_20260926T080000Z_" + "c".repeat(64)),
  observationKey: m.consensusObservationKey("asts", AS_OF),
  boundary: f.boundaryCases.map(([a, raw]) => { const o = m.withAuthorityBoundary(a, raw); try { return JSON.parse(o); } catch { return o; } }),
  boundaryActions: [...m.EVIDENCE_ONLY_ACTIONS].sort(),
};
const staleStored = (kind) => {
  const key = m.providerSnapshotKey("alpha_vantage", "EARNINGS_ESTIMATES", "asts", f.staleFetchedAt);
  if (kind === "good") return { key, text: m.providerSnapshotBody("alpha_vantage", "EARNINGS_ESTIMATES", "asts", f.staleFetchedAt, f.av) };
  return kind === "bad" ? { key, text: "not json" } : null;
};
const staleCases = f.staleCases.map(([kind, status, message, at, t]) =>
  m.staleSnapshot(staleStored(kind), { provider: "alpha_vantage", operation: "EARNINGS_ESTIMATES", ticker: t }, { status, message }, at));
const hit = staleCases[0];
const avStale = m.alphaVantageConsensusInput(hit.payload, { retrievedAt: hit.fetchedAt, staleFallback: hit.staleFallback });
Object.assign(out, {
  staleCases: staleCases.map((c) => c === null ? null : { fetchedAt: c.fetchedAt, staleFallback: c.staleFallback, estimates: c.payload.estimates.length }),
  staleBody: staleStored("good"),
  staleCandidateKeys: m.providerSnapshotCandidateKeys("alpha_vantage", "EARNINGS_ESTIMATES", "asts", "2026-10-03T08:00:00.000Z"),
  curveAvStale: m.buildConsensusCurve("asts", [yahoo, avStale], AS_OF),
  revisionsAvStale: m.buildEpsRevisions("asts", [yahoo, avStale], AS_OF),
});
console.log(JSON.stringify(out));
"""


def _worker_outputs() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    fixtures = {
        "asOf": AS_OF, "yahooTrend": YAHOO_TREND, "av": AV, "avIdentical": AV_IDENTICAL, "thinTrend": THIN_TREND, "adrTrend": ADR_TREND_NEXT, "adrOther": ADR_OTHER,
        "derivedTrend": DERIVED_TREND, "filings": FILINGS, "staleFilings": STALE_FILINGS, "componentTexts": COMPONENT_TEXTS,
        "asOfLater": AS_OF_LATER, "fpiFilings": FPI_FILINGS, "fpiStaleFilings": FPI_STALE_FILINGS, "fpi40faFilings": FPI_40FA_FILINGS,
        "quoteFailed": QUOTE_FAILED, "quoteNoData": QUOTE_NO_DATA,
        "dgTrend": DG_TREND, "dgNaming": DG_NAMING,
        "anetAsOf": ANET_AS_OF, "anetTrend": ANET_TREND, "anetAv": ANET_AV, "anetSplits": ANET_SPLITS, "anetOldSplits": ANET_OLD_SPLITS,
        "f013Trend": F013_TREND, "f013Av": F013_AV, "f013AvNear": F013_AV_NEAR, "f013DecimalCases": F013_DECIMAL_CASES,
        "staleFetchedAt": STALE_FETCHED_AT, "staleCases": STALE_CASES,
        "splitsFailed": SPLITS_FAILED, "reverseSplits": REVERSE_SPLITS, "reverseA": REVERSE_A, "reverseB": REVERSE_B,
        # JSON cannot carry -0.0 distinctly from 0 in every parser; both runtimes format it as 0.
        "canonical": CANONICAL_CASES,
        "boundaryCases": BOUNDARY_CASES,
    }
    with tempfile.TemporaryDirectory() as tmp:
        bundle = Path(tmp) / "evidence.mjs"
        harness = Path(tmp) / "harness.mjs"
        fx = Path(tmp) / "fixtures.json"
        fx.write_text(json.dumps(fixtures), encoding="utf-8")
        harness.write_text(_HARNESS, encoding="utf-8")
        subprocess.run([str(ESBUILD), str(WORKER / "src" / "evidence.ts"), "--bundle", "--format=esm", "--platform=neutral", f"--outfile={bundle}", "--log-level=error"],
                       cwd=WORKER, check=True, capture_output=True, text=True, timeout=120)
        result = subprocess.run([node, str(harness), bundle.as_uri(), str(fx)], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


def _cell(curve: dict, label: str, metric: str) -> dict:
    return next(p for p in curve["periods"] if p["label"] == label)["metrics"][metric]


class TestEvidenceParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.py = _python_outputs()
        cls.ts = _worker_outputs()

    def test_outputs_match(self) -> None:
        for key in self.py:
            self.assertEqual(json.loads(json.dumps(self.py[key])), self.ts[key], key)

    def test_canonical_bytes_match(self) -> None:
        self.assertEqual(self.py["canonical"], self.ts["canonical"])
        self.assertTrue(self.py["canonical"][0].startswith('{"a":"é\\n\\"q\\"\\u0001","b":[1,2.5,0,1e+21,1.5e+21,1e-7,0.000001,123.456,31520000,-2.2839,10000000000000000,5e-324]'))


class TestConsensusCurve(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()

    def test_fy0_fy1_are_covered_by_both_providers(self) -> None:
        curve = self.out["curve"]
        self.assertEqual(curve["fiscalYearBasis"], {"fiscalYearEnd": "2026-12-31", "basis": "yahoo_finance:0y"})
        eps = _cell(curve, "FY0", "eps")
        self.assertEqual(eps["coverage"], "PROVIDER_COVERED")
        self.assertEqual([p["provider"] for p in eps["providers"]], ["yahoo_finance", "alpha_vantage"])
        self.assertEqual(eps["agreement"]["status"], "AGREED")
        self.assertEqual(eps["agreement"]["currencyIdentity"], "UNVERIFIED")  # Alpha Vantage states no currency
        rev = _cell(curve, "FY+1", "revenue")
        self.assertEqual(rev["coverage"], "PROVIDER_CONFLICT")  # 520M vs 650.8M
        self.assertEqual(rev["agreement"]["relativeDiffPct"], 20.1)

    def test_identical_figures_are_one_feed_not_two_checks(self) -> None:
        # 2.5.22 (F-005): equal mean, high, low and analyst count is IDENTICAL, and a curve of only those is NOT_INDEPENDENT.
        curve = self.out["curveIdentical"]
        for label, metric in (("FY0", "eps"), ("FY0", "revenue"), ("FY+1", "eps"), ("FY+1", "revenue")):
            agreement = _cell(curve, label, metric)["agreement"]
            self.assertEqual(agreement, {
                "status": "IDENTICAL", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 0, "absoluteDiff": 0,
                "currencyIdentity": "UNVERIFIED", "independence": "NOT_INDEPENDENT"})
            self.assertEqual(list(agreement), ["status", "providersCompared", "relativeDiffPct", "absoluteDiff", "currencyIdentity", "independence"])
            self.assertEqual(_cell(curve, label, metric)["coverage"], "PROVIDER_COVERED")
        self.assertEqual(curve["crossCheck"], {
            "status": "NOT_INDEPENDENT", "providersAnswered": ["yahoo_finance", "alpha_vantage"], "providersFailed": [],
            "cellsCompared": 4, "cellsIdentical": 4})
        self.assertEqual([w["code"] for w in curve["warnings"]], ["PROVIDERS_NOT_INDEPENDENT"])
        self.assertEqual(curve["warnings"][0]["severity"], "warning")
        # Distinct figures keep the numeric agreement and an unverified independence.
        normal = _cell(self.out["curve"], "FY0", "eps")["agreement"]
        self.assertEqual((normal["status"], normal["independence"]), ("AGREED", "UNVERIFIED"))
        self.assertEqual(list(normal), ["status", "providersCompared", "relativeDiffPct", "absoluteDiff", "currencyIdentity", "independence"])
        self.assertEqual(self.out["curve"]["crossCheck"]["status"], "CROSS_CHECKED")
        self.assertEqual(self.out["curve"]["warnings"], [])
        # A cell without an analyst count on every provider is never IDENTICAL.
        entry = {"mean": 1.0, "high": None, "low": None, "analystCount": None, "currency": "USD", "fiscalYearEnd": "2026-12-31"}
        other = {**entry, "provider": "b"}
        self.assertEqual(ev._agreement([{**entry, "provider": "a"}, other], "eps", ev.DEFAULT_CONSENSUS_POLICY)["status"], "AGREED")
        # A field present on both providers must match.
        a = {**entry, "provider": "a", "analystCount": 5, "high": 2.0}
        b = {**entry, "provider": "b", "analystCount": 5, "high": 2.5}
        self.assertEqual(ev._agreement([a, b], "eps", ev.DEFAULT_CONSENSUS_POLICY)["status"], "AGREED")

    def test_identical_at_the_coarser_published_precision(self) -> None:
        # 2.5.23 (F-013): Yahoo 5.20199 and Alpha Vantage "5.2020" are one figure at Alpha Vantage's four places.
        curve = self.out["curveF013"]
        eps = _cell(curve, "FY0", "eps")["agreement"]
        self.assertEqual(eps, {"status": "IDENTICAL", "providersCompared": ["yahoo_finance", "alpha_vantage"], "relativeDiffPct": 0,
                               "absoluteDiff": 1e-05, "currencyIdentity": "UNVERIFIED", "independence": "NOT_INDEPENDENT"})
        # "2.5000" is four places, so Yahoo's 2.54 is not it, though 2.5 would round from it at one place.
        self.assertEqual(_cell(curve, "FY0", "revenue")["agreement"]["status"], "AGREED")
        self.assertEqual(curve["crossCheck"]["cellsIdentical"], 1)
        near = _cell(self.out["curveF013Near"], "FY0", "eps")["agreement"]
        self.assertEqual((near["status"], near["independence"]), ("AGREED", "UNVERIFIED"))
        self.assertEqual(self.out["publishedDecimals"], [4, 2, 0, None, 2, None, None, None, None, 2, None, None, None])
        # Numbers state no rounding: without published text the comparison stays exact.
        entry = {"high": None, "low": None, "analystCount": 9, "currency": "USD", "fiscalYearEnd": "2026-12-31"}
        pair = [{**entry, "provider": "a", "mean": 5.2}, {**entry, "provider": "b", "mean": 5.20199}]
        self.assertEqual(ev._agreement(pair, "eps", ev.DEFAULT_CONSENSUS_POLICY)["status"], "AGREED")

    def test_cross_check_reports_a_failed_provider(self) -> None:
        # 2.5.22 (F-003): a rate-limited Alpha Vantage leaves a single-provider curve with a warning, not silence.
        curve = self.out["curveAvRateLimited"]
        self.assertEqual(curve["crossCheck"], {
            "status": "SINGLE_PROVIDER", "providersAnswered": ["yahoo_finance"],
            "providersFailed": [{"provider": "alpha_vantage", "status": "RATE_LIMIT", "message": "25 requests per day"}],
            "cellsCompared": 0, "cellsIdentical": 0})
        self.assertEqual(len(curve["warnings"]), 1)
        warning = curve["warnings"][0]
        self.assertEqual((warning["code"], warning["severity"]), ("CROSS_CHECK_DEGRADED", "warning"))
        self.assertEqual(warning["message"], "alpha_vantage returned RATE_LIMIT: 25 requests per day. The curve is not cross-checked against it. "
                         "This server's Alpha Vantage key and its quota are its own, separate from any key used directly.")
        self.assertIn("separate from any key used directly", warning["message"])
        # An empty message adds no ": " suffix.
        silent = self.out["curveAvRateLimitedSilent"]["warnings"][0]["message"]
        self.assertTrue(silent.startswith("alpha_vantage returned RATE_LIMIT. The curve is not cross-checked against it."))
        # Both providers failed: NO_PROVIDER and one warning each, the quota sentence on Alpha Vantage only.
        both = self.out["curveBothFailed"]
        self.assertEqual(both["crossCheck"]["status"], "NO_PROVIDER")
        self.assertEqual([w["code"] for w in both["warnings"]], ["CROSS_CHECK_DEGRADED", "CROSS_CHECK_DEGRADED"])
        self.assertNotIn("quota", both["warnings"][0]["message"])
        self.assertIn("quota", both["warnings"][1]["message"])
        # Key order: crossCheck follows asOf, warnings follow notes.
        keys = list(curve)
        self.assertEqual(keys[:3], ["ticker", "asOf", "crossCheck"])
        self.assertEqual(keys[keys.index("notes") + 1], "warnings")

    def test_eps_basis_is_stated_as_unknown(self) -> None:
        # 2.5.31 (F-022): neither provider states GAAP or adjusted; the curve and the revisions say so.
        for out in (self.out["curve"], self.out["revisions"], self.out["curveAvRateLimited"]):
            self.assertEqual(out["epsBasis"], ev.EPS_BASIS)
        self.assertEqual((ev.EPS_BASIS["basis"], ev.EPS_BASIS["determination"]), ("UNKNOWN", "NOT_DISCLOSED_BY_PROVIDER"))
        keys = list(self.out["curve"])
        self.assertEqual(keys[keys.index("fiscalYearNaming") + 1], "epsBasis")

    def test_years_beyond_fy1_stay_visibly_missing(self) -> None:
        curve = self.out["curve"]
        self.assertEqual([p["label"] for p in curve["periods"]], ["FY0", "FY+1", "FY+2", "FY+3", "FY+4", "FY+5"])
        for label in ("FY+2", "FY+3", "FY+4", "FY+5"):
            for metric in ("eps", "revenue"):
                cell = _cell(curve, label, metric)
                self.assertEqual((cell["coverage"], cell["providers"]), ("PROVIDER_NOT_COVERED", []))
        self.assertEqual(next(p for p in curve["periods"] if p["label"] == "FY+3")["fiscalYear"], 2029)
        self.assertEqual({m["metric"] for m in curve["metricsNotCoveredByAnyProvider"]}, {"ebitda", "ebit", "freeCashFlow", "capex", "grossMargin", "ebitdaMargin"})
        self.assertEqual(_cell(curve, "FY0", "eps")["dispersionStatistics"]["median"], {"value": None, "state": "PROVIDER_NOT_COVERED"})
        self.assertEqual(len(self.out["curveOneYear"]["periods"]), 2)

    def test_no_selected_value_and_authority_boundary(self) -> None:
        for key in ("curve", "revisions", "quality"):
            payload = self.out[key]
            self.assertEqual(payload["decisionUse"], "EVIDENCE_ONLY")
            for field in ("selectedMethod", "selectedMultiple", "scenarioWeights", "priceTarget", "g2", "opportunity", "action"):
                self.assertIn(field, payload)
                self.assertIsNone(payload[field])
        eps = _cell(self.out["curve"], "FY0", "eps")
        self.assertNotIn("mean", eps)
        self.assertNotIn("value", eps)

    def test_thin_coverage_and_52_week_years(self) -> None:
        thin = self.out["curveThin"]
        self.assertEqual(_cell(thin, "FY0", "eps")["coverage"], "INSUFFICIENT_ANALYST_COUNT")
        self.assertEqual(_cell(thin, "FY+1", "eps")["coverage"], "PROVIDER_COVERED")  # 2027-09-25 is FY+1 of 2026-09-26
        self.assertEqual(_cell(thin, "FY+1", "revenue")["coverage"], "PROVIDER_NOT_COVERED")

    def test_fiscal_year_ending_in_early_january(self) -> None:
        # 2.5.11: a 52/53-week year ending January 2, 2027 is fiscal 2026, not 2027.
        trend = [{"period": "0y", "endDate": "2027-01-02", "earningsEstimate": {"avg": 1.0, "numberOfAnalysts": 5}, "revenueEstimate": {}}]
        curve = ev.build_consensus_curve("amdx", [ev.yahoo_consensus_input(trend, retrieved_at=AS_OF, financial_currency="USD")], AS_OF)
        self.assertEqual([p["fiscalYear"] for p in curve["periods"][:2]], [2026, 2027])

    def test_company_fiscal_year_naming(self) -> None:
        # 2.5.13: DG's year ending January 29, 2027 is its fiscal 2026 by its own naming.
        trend = [{"period": "0y", "endDate": "2027-01-29", "earningsEstimate": {"avg": 6.0, "numberOfAnalysts": 20}, "revenueEstimate": {}}]
        naming = {"offset": -1, "basis": "SEC_STATED_FISCAL_YEAR", "periodEnd": "2026-01-30", "statedFiscalYear": 2025}
        inp = ev.yahoo_consensus_input(trend, retrieved_at=AS_OF, financial_currency="USD")
        curve = ev.build_consensus_curve("dg", [inp], AS_OF, None, naming)
        self.assertEqual([p["fiscalYear"] for p in curve["periods"][:2]], [2026, 2027])
        self.assertEqual(curve["fiscalYearNaming"], naming)
        self.assertEqual(ev.build_consensus_curve("dg", [inp], AS_OF)["fiscalYearNaming"]["basis"], "PERIOD_END_RULE")

    def test_company_fiscal_year_end(self) -> None:
        # 2.5.16: Yahoo dates DG's year 2027-01-31; its own calendar (Friday nearest January 31) ends it 2027-01-29.
        curve = self.out["curveDgCalendar"]
        fy0 = curve["periods"][0]
        self.assertEqual((fy0["fiscalYearEnds"], fy0["companyFiscalYearEnd"]), (["2027-01-31"], "2027-01-29"))
        self.assertEqual(list(fy0)[:4], ["label", "fiscalYear", "fiscalYearEnds", "companyFiscalYearEnd"])
        self.assertIsNone(self.out["curveDgNoNaming"]["periods"][0]["companyFiscalYearEnd"])
        self.assertIsNone(self.out["curveDgNoNaming"]["fiscalYearNaming"]["calendar"])
        # A naming passed without a calendar behaves as none.
        legacy = {"offset": -1, "basis": "SEC_STATED_FISCAL_YEAR", "periodEnd": "2026-01-30", "statedFiscalYear": 2025}
        self.assertIsNone(ev.build_consensus_curve("dg", [ev.yahoo_consensus_input(DG_TREND, retrieved_at=AS_OF, financial_currency="USD")], AS_OF, None, legacy)["periods"][0]["companyFiscalYearEnd"])

    def test_currency_and_period_identity_conflicts(self) -> None:
        adr = self.out["curveAdr"]
        fy0 = _cell(adr, "FY0", "eps")
        self.assertEqual((fy0["coverage"], fy0["agreement"]["status"], fy0["agreement"]["currencies"]), ("PROVIDER_CONFLICT", "CURRENCY_MISMATCH", ["TWD", "USD"]))
        fy1 = _cell(adr, "FY+1", "eps")
        self.assertEqual((fy1["coverage"], fy1["agreement"]["status"]), ("PROVIDER_CONFLICT", "PERIOD_IDENTITY_MISMATCH"))

    def test_derived_fiscal_year_is_labelled(self) -> None:
        eps = _cell(self.out["curveDerived"], "FY0", "eps")["providers"][0]
        self.assertEqual((eps["fiscalYearEndBasis"], eps["currency"], eps["currencyBasis"]), ("DERIVED_FROM_NEXT_FISCAL_YEAR_END", "EUR", "FINANCIAL_CURRENCY"))

    def test_av_zero_count_rows_are_not_coverage(self) -> None:
        av = self.out["curveAvOnly"]
        self.assertEqual(av["fiscalYearBasis"]["basis"], "earliest_provider_fiscal_year_on_or_after_as_of")
        self.assertNotIn("2021-12-31", json.dumps(av["periods"]))
        none = self.out["curveNone"]
        self.assertEqual(none["coverageSummary"], {"PROVIDER_NOT_COVERED": 12})
        self.assertEqual(none["providers"][0]["status"], "PROVIDER_ERROR")

    def test_eps_revision_windows(self) -> None:
        rev = self.out["revisions"]
        fy0 = rev["periods"][0]["providers"]
        yahoo = next(p for p in fy0 if p["provider"] == "yahoo_finance")
        self.assertEqual(yahoo["windows"]["30d"], {"mean": -1.39, "change": -0.88, "changePct": -63.31, "state": "COMPARED"})
        self.assertEqual(yahoo["state"], "PROVIDER_COVERED")
        self.assertEqual(yahoo["revisionCounts"], {"up7d": 0, "down7d": 1, "up30d": 0, "down30d": 4})
        av1 = next(p for p in rev["periods"][1]["providers"] if p["provider"] == "alpha_vantage")
        self.assertIn("revisionCounts.down7d", av1["notReported"])
        y1 = next(p for p in rev["periods"][1]["providers"] if p["provider"] == "yahoo_finance")
        self.assertEqual(y1["windows"]["60d"], {"mean": None, "change": None, "changePct": None, "state": "NOT_REPORTED"})
        self.assertEqual(rev["revenueRevisions"]["coverage"], "PROVIDER_NOT_COVERED")
        self.assertEqual(rev["splitHistory"], {"status": "NOT_REQUESTED", "lookbackDays": 91, "recentSplits": []})
        self.assertEqual(rev["warnings"], [])


class TestStaleProviderFallback(unittest.TestCase):
    """2.5.29: a refused Alpha Vantage request falls back to its last stored payload, marked stale."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()

    def test_which_refusals_and_snapshots_stand_in(self) -> None:
        key = "provider-snapshots/alpha_vantage/EARNINGS_ESTIMATES/ASTS/2026-09-25.json"
        cases = self.out["staleCases"]
        self.assertEqual(cases[0], {"fetchedAt": STALE_FETCHED_AT, "estimates": 4, "staleFallback": {
            "liveStatus": "RATE_LIMIT", "liveMessage": "25 requests per day", "ageHours": 30, "snapshotKey": key}})
        # A timeout or upstream error passes too; an empty message reads as none.
        self.assertEqual(cases[1]["staleFallback"]["liveMessage"], None)
        self.assertEqual(cases[2]["staleFallback"]["liveStatus"], "PROVIDER_ERROR")
        # A bad key or no key is not something that passes: no stand-in.
        self.assertEqual(cases[3:5], [None, None])
        # Exactly seven days old is in; a millisecond older, or fetched after asOf, is out.
        self.assertEqual(cases[5]["staleFallback"]["ageHours"], 168)
        self.assertEqual(cases[6:8], [None, None])
        # Another ticker's snapshot, an unreadable one or none at all: no stand-in.
        self.assertEqual(cases[8:], [None, None, None])

    def test_snapshot_keys_and_body(self) -> None:
        self.assertEqual(self.out["staleBody"]["key"], "provider-snapshots/alpha_vantage/EARNINGS_ESTIMATES/ASTS/2026-09-25.json")
        body = json.loads(self.out["staleBody"]["text"])
        self.assertEqual({k: body[k] for k in ("schema", "provider", "operation", "ticker", "fetchedAt")}, {
            "schema": "yfmcp.provider-snapshot/1", "provider": "alpha_vantage", "operation": "EARNINGS_ESTIMATES", "ticker": "ASTS", "fetchedAt": STALE_FETCHED_AT})
        # Published text survives the round trip, so F-013's precision rule still sees "5.2020"-style strings.
        self.assertEqual(body["payload"], AV)
        keys = self.out["staleCandidateKeys"]
        self.assertEqual([k.rsplit("/", 1)[-1] for k in keys], [f"2026-10-0{d}.json" for d in (3, 2, 1)] + [f"2026-09-{d}.json" for d in (30, 29, 28, 27, 26)])

    def test_stale_figures_are_compared_and_flagged(self) -> None:
        curve = self.out["curveAvStale"]
        key = "provider-snapshots/alpha_vantage/EARNINGS_ESTIMATES/ASTS/2026-09-25.json"
        stale = {"liveStatus": "RATE_LIMIT", "liveMessage": "25 requests per day", "ageHours": 30, "snapshotKey": key}
        check = curve["crossCheck"]
        self.assertEqual(check["providersAnswered"], ["yahoo_finance", "alpha_vantage"])
        self.assertEqual(check["providersFailed"], [])
        self.assertEqual(check["providersStale"], [{"provider": "alpha_vantage", "retrievedAt": STALE_FETCHED_AT, **stale}])
        self.assertEqual(list(check)[-1], "providersStale")
        av = next(p for p in curve["providers"] if p["provider"] == "alpha_vantage")
        self.assertEqual((av["status"], av["retrievedAt"], av["staleFallback"]), ("OK", STALE_FETCHED_AT, stale))
        self.assertNotIn("staleFallback", next(p for p in curve["providers"] if p["provider"] == "yahoo_finance"))
        entry = next(e for e in _cell(curve, "FY0", "eps")["providers"] if e["provider"] == "alpha_vantage")
        self.assertEqual(entry["retrievedAt"], STALE_FETCHED_AT)
        codes = [w["code"] for w in curve["warnings"]]
        self.assertNotIn("CROSS_CHECK_DEGRADED", codes)
        warning = next(w for w in curve["warnings"] if w["code"] == "PROVIDER_DATA_STALE")
        self.assertEqual((warning["severity"], warning["message"]), ("warning",
            f"alpha_vantage returned RATE_LIMIT: 25 requests per day. Its figures retrieved at {STALE_FETCHED_AT} (30 hours before asOf) stand in. "
            "A difference from another provider may be a revision since then, not a disagreement. "
            "This server's Alpha Vantage key and its quota are its own, separate from any key used directly."))
        # The same comparison as live figures would get.
        self.assertEqual(_cell(curve, "FY0", "eps")["agreement"], _cell(self.out["curve"], "FY0", "eps")["agreement"])
        # A live curve carries no stale fields.
        self.assertNotIn("providersStale", self.out["curve"]["crossCheck"])
        self.assertNotIn("staleFallback", self.out["curve"]["providers"][1])

    def test_stale_revision_windows_are_flagged(self) -> None:
        rev = self.out["revisionsAvStale"]
        av = next(p for p in rev["providers"] if p["provider"] == "alpha_vantage")
        self.assertEqual(av["staleFallback"]["ageHours"], 30)
        self.assertEqual([w["code"] for w in rev["warnings"]], ["PROVIDER_DATA_STALE"])
        self.assertIn("Its windows end at that time, not at asOf.", rev["warnings"][0]["message"])


class TestProviderRowGuards(unittest.TestCase):
    """2.5.23: impossible provider rows (F-001) and EPS figures not adjusted for a split (F-002)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()

    def test_mean_outside_its_own_range_is_excluded(self) -> None:
        cell = _cell(self.out["curveAnet"], "FY0", "revenue")
        av = next(e for e in cell["providers"] if e["provider"] == "alpha_vantage")
        self.assertEqual((av["state"], av["inconsistency"], av["mean"], av["high"]), ("PROVIDER_INCONSISTENT", "MEAN_ABOVE_HIGH", 9050000000, 8990000000))
        self.assertEqual(list(av)[-2:], ["state", "inconsistency"])
        yahoo = next(e for e in cell["providers"] if e["provider"] == "yahoo_finance")
        self.assertEqual(yahoo["state"], "PROVIDER_COVERED")
        self.assertNotIn("inconsistency", yahoo)
        self.assertEqual(cell["agreement"], {"status": "SINGLE_PROVIDER", "providersCompared": ["yahoo_finance"], "relativeDiffPct": None, "absoluteDiff": None,
                                             "providersExcluded": [{"provider": "alpha_vantage", "reason": "MEAN_ABOVE_HIGH"}]})
        self.assertEqual(cell["coverage"], "PROVIDER_COVERED")
        # FY+1: Alpha Vantage's low is above its high.
        fy1 = _cell(self.out["curveAnet"], "FY+1", "revenue")
        self.assertEqual(next(e for e in fy1["providers"] if e["provider"] == "alpha_vantage")["inconsistency"], "LOW_ABOVE_HIGH")
        # The only provider inconsistent: the cell says so rather than claiming coverage.
        only = _cell(self.out["curveAnetAvOnly"], "FY0", "revenue")
        self.assertEqual((only["coverage"], only["agreement"]["status"]), ("PROVIDER_INCONSISTENT", "PROVIDER_INCONSISTENT"))
        codes = [w["code"] for w in self.out["curveAnet"]["warnings"]]
        self.assertEqual(codes.count("PROVIDER_ROW_INCONSISTENT"), 2)
        warning = next(w for w in self.out["curveAnet"]["warnings"] if w["code"] == "PROVIDER_ROW_INCONSISTENT")
        self.assertEqual(warning["message"], "alpha_vantage FY0 revenue (fiscal year ending 2026-12-31): its mean is above its own high "
                         "(mean 9050000000, low 8800000000, high 8990000000). The row is shown as given and left out of the agreement check.")

    def test_range_inconsistency_rule(self) -> None:
        f = ev.range_inconsistency
        self.assertIsNone(f({"mean": 1.0, "high": 1.0, "low": 1.0}))
        self.assertIsNone(f({"mean": 1.0, "high": None, "low": None}))
        self.assertIsNone(f({"mean": None, "high": 2.0, "low": 1.0}))
        self.assertEqual(f({"mean": 0.9, "high": 2.0, "low": 1.0}), "MEAN_BELOW_LOW")
        self.assertEqual(f({"mean": None, "high": 1.0, "low": 2.0}), "LOW_ABOVE_HIGH")
        self.assertEqual(f({"mean": -2.0, "high": -2.1, "low": -3.0}), "MEAN_ABOVE_HIGH")

    def test_eps_gap_matching_a_recent_split_is_not_a_conflict(self) -> None:
        curve = self.out["curveAnet"]
        eps = _cell(curve, "FY0", "eps")
        self.assertEqual(eps["agreement"]["status"], "NOT_SPLIT_ADJUSTED")
        self.assertEqual(eps["agreement"]["split"], {"date": "2026-09-20", "ratio": 4})
        self.assertEqual(eps["agreement"]["providersNotAdjusted"], ["alpha_vantage"])
        self.assertEqual(eps["coverage"], "PROVIDER_CONFLICT")
        self.assertEqual(curve["splitHistory"], {"status": "OK", "lookbackDays": 400, "recentSplits": [{"date": "2026-09-20", "ratio": 4}]})
        self.assertEqual(list(curve)[:4], ["ticker", "asOf", "crossCheck", "splitHistory"])
        w = next(w for w in curve["warnings"] if w["code"] == "PROVIDER_NOT_SPLIT_ADJUSTED")
        self.assertEqual(w["message"], "FY0 EPS: the providers differ by the ratio of the 4-for-1 split on 2026-09-20; alpha_vantage appears not split-adjusted. "
                         "Reported as NOT_SPLIT_ADJUSTED, not as a conflict between views.")
        # The 2021 split is outside the lookback: the same gap is a plain conflict.
        old = self.out["curveAnetOldSplit"]
        self.assertEqual(_cell(old, "FY0", "eps")["agreement"]["status"], "CONFLICT")
        self.assertEqual(old["splitHistory"]["recentSplits"], [])
        # Split history unreadable: no split check, and the curve says so.
        failed = self.out["curveAnetSplitsFailed"]
        self.assertEqual(_cell(failed, "FY0", "eps")["agreement"]["status"], "CONFLICT")
        self.assertEqual(failed["splitHistory"], {"status": "PROVIDER_ERROR", "lookbackDays": 400, "recentSplits": [], "message": "chart down"})
        self.assertEqual(failed["warnings"][-1]["code"], "SPLIT_HISTORY_UNAVAILABLE")
        self.assertEqual(failed["warnings"][-1]["message"], "Split history returned PROVIDER_ERROR: chart down. EPS gaps between providers are not checked against splits.")
        # A reverse split: the smaller loss per share is the unadjusted one.
        rev = _cell(self.out["curveReverse"], "FY0", "eps")["agreement"]
        self.assertEqual((rev["status"], rev["providersNotAdjusted"], rev["split"]), ("NOT_SPLIT_ADJUSTED", ["alpha_vantage"], {"date": "2026-08-01", "ratio": 0.1}))
        self.assertIn("1-for-10 split", next(w for w in self.out["curveReverse"]["warnings"] if w["code"] == "PROVIDER_NOT_SPLIT_ADJUSTED")["message"])

    def test_revision_windows_spanning_a_split_carry_no_change(self) -> None:
        rev = self.out["revisionsAnet"]
        yahoo = next(p for p in rev["periods"][0]["providers"] if p["provider"] == "yahoo_finance")
        self.assertEqual(yahoo["windows"]["7d"], {"mean": 2.79, "change": 0.01, "changePct": 0.36, "state": "COMPARED"})
        for name, mean in (("30d", 11.0), ("60d", 10.9), ("90d", 10.8)):
            self.assertEqual(yahoo["windows"][name], {"mean": mean, "change": None, "changePct": None, "state": "SPLIT_IN_WINDOW",
                                                      "split": {"date": "2026-09-20", "ratio": 4}})
        av = next(p for p in rev["periods"][0]["providers"] if p["provider"] == "alpha_vantage")
        self.assertEqual(av["windows"]["60d"]["state"], "NOT_REPORTED")
        self.assertEqual(rev["splitHistory"], {"status": "OK", "lookbackDays": 91, "recentSplits": [{"date": "2026-09-20", "ratio": 4}]})
        self.assertEqual([w["code"] for w in rev["warnings"]], ["SPLIT_IN_WINDOW"])
        self.assertEqual(rev["warnings"][0]["message"], "A 4-for-1 split on 2026-09-20 falls inside the revision windows; windows spanning it "
                         "compare pre- and post-split estimates and carry no change.")
        failed = self.out["revisionsAnetSplitsFailed"]
        y = next(p for p in failed["periods"][0]["providers"] if p["provider"] == "yahoo_finance")
        self.assertEqual(y["windows"]["30d"]["state"], "COMPARED")
        self.assertEqual([w["code"] for w in failed["warnings"]], ["SPLIT_HISTORY_UNAVAILABLE"])
        self.assertTrue(failed["warnings"][0]["message"].endswith("Revision windows are not checked against splits."))

    def test_inconsistent_eps_row_has_no_window_changes(self) -> None:
        bad = ev.alpha_vantage_consensus_input({"estimates": [{"date": "2026-12-31", "horizon": "fiscal year", "eps_estimate_average": "3.0",
                                                               "eps_estimate_high": "2.9", "eps_estimate_low": "2.5", "eps_estimate_analyst_count": "9",
                                                               "eps_estimate_average_7_days_ago": "2.8"}]}, retrieved_at=ANET_AS_OF)
        rev = ev.build_eps_revisions("x", [bad], ANET_AS_OF, {"status": "OK", "splits": []})
        p = rev["periods"][0]["providers"][0]
        self.assertEqual((p["state"], p["inconsistency"]), ("PROVIDER_INCONSISTENT", "MEAN_ABOVE_HIGH"))
        self.assertEqual(p["windows"]["7d"], {"mean": 2.8, "change": None, "changePct": None, "state": "PROVIDER_INCONSISTENT"})
        self.assertEqual([w["code"] for w in rev["warnings"]], ["PROVIDER_ROW_INCONSISTENT"])


class TestEvidenceQualityAndReceipt(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()

    def test_quality_families(self) -> None:
        q = self.out["quality"]["families"]
        self.assertEqual(q["secPeriodicFiling"]["state"], "READY")
        self.assertEqual((q["guidance"]["state"], q["guidance"]["latestEarningsRelease8k"]), ("READY_TO_EXTRACT", "2026-08-10"))
        self.assertEqual(q["materialEvents"]["eightKCount90d"], 2)
        self.assertEqual(q["consensus"]["state"], "PARTIAL")
        self.assertIn({"family": "consensus", "code": "PROVIDER_CONFLICT", "message": "FY+1.revenue is PROVIDER_CONFLICT."}, self.out["quality"]["blockers"])
        stale = self.out["qualityStale"]["families"]
        self.assertEqual((stale["quote"]["state"], stale["secPeriodicFiling"]["state"], stale["capitalStructure"]["state"], stale["evidenceStorage"]["state"]),
                         ("MISSING", "STALE", "PARTIAL", "UNAVAILABLE"))
        no_sec = self.out["qualityNoSec"]["families"]
        self.assertEqual((no_sec["quote"]["state"], no_sec["secPeriodicFiling"]["state"], no_sec["dilution"]["state"]), ("STALE", "UNAVAILABLE", "UNAVAILABLE"))

    def test_annual_filers_are_stale_only_when_the_next_20f_is_overdue(self) -> None:
        # 2026-09-30 is 273 days after 2025-12-31: STALE for a 135-day quarterly filer, READY for a 20-F filer (2.5.15).
        ready = self.out["qualityFpiReady"]["families"]["secPeriodicFiling"]
        self.assertEqual((ready["state"], ready["cadence"], ready["staleAfterDays"], ready["periodAgeDays"], ready["latestInterim6k"]),
                         ("READY", "ANNUAL", 492, 273, "2026-08-20"))
        self.assertEqual(list(ready), ["state", "form", "filingDate", "periodEnd", "periodAgeDays", "cadence", "staleAfterDays", "latestInterim6k", "inlineXbrl", "sourceStatus"])
        self.assertFalse([b for b in self.out["qualityFpiReady"]["blockers"] if b["family"] == "secPeriodicFiling"])
        stale = self.out["qualityFpiStale"]["families"]["secPeriodicFiling"]
        self.assertEqual((stale["state"], stale["cadence"], stale["periodAgeDays"], stale["latestInterim6k"]), ("STALE", "ANNUAL", 638, "2026-08-20"))
        self.assertIn("SEC_PERIODIC_STALE", [b["code"] for b in self.out["qualityFpiStale"]["blockers"]])
        # 40-F/A is a periodic form; a filer with no 6-K has a null interim date, not a missing key.
        amended = self.out["qualityFpi40FA"]["families"]["secPeriodicFiling"]
        self.assertEqual((amended["form"], amended["state"], amended["cadence"], amended["latestInterim6k"]), ("40-F/A", "READY", "ANNUAL", None))
        # A quarterly filer keeps 135 days and has no interim key.
        quarterly = self.out["qualityQuarterlyLater"]["families"]["secPeriodicFiling"]
        self.assertEqual((quarterly["state"], quarterly["cadence"], quarterly["staleAfterDays"], quarterly["periodAgeDays"]), ("READY", "QUARTERLY", 135, 92))
        self.assertNotIn("latestInterim6k", quarterly)
        old_10k = self.out["qualityStale"]["families"]["secPeriodicFiling"]
        self.assertEqual((old_10k["state"], old_10k["cadence"], old_10k["staleAfterDays"]), ("STALE", "QUARTERLY", 135))
        no_filings = self.out["qualityNoSec"]["families"]["secPeriodicFiling"]
        self.assertEqual((no_filings["state"], no_filings["cadence"], no_filings["staleAfterDays"]), ("UNAVAILABLE", None, None))

    def test_a_failed_quote_is_unavailable_and_retryable(self) -> None:
        failed = self.out["qualityQuoteFailed"]
        self.assertEqual((failed["families"]["quote"]["state"], failed["families"]["quote"]["sourceStatus"]), ("UNAVAILABLE", "PROVIDER_ERROR"))
        self.assertEqual([b for b in failed["blockers"] if b["family"] == "quote"], [{
            "family": "quote", "code": "QUOTE_UNAVAILABLE", "retryable": True,
            "message": "The price request failed (PROVIDER_ERROR); retry. Mechanical dilution at price cannot run without it."}])
        # No data is a missing price, not a failed request.
        no_data = self.out["qualityQuoteNoData"]
        self.assertEqual(no_data["families"]["quote"]["state"], "MISSING")
        self.assertEqual([b["code"] for b in no_data["blockers"] if b["family"] == "quote"], ["QUOTE_MISSING"])
        self.assertEqual(self.out["qualityStale"]["families"]["quote"]["state"], "MISSING")

    def test_consensus_that_every_provider_failed_to_read_is_one_retryable_blocker(self) -> None:
        failed = self.out["qualityConsensusFailed"]
        consensus = failed["families"]["consensus"]
        self.assertEqual((consensus["state"], consensus["cells"]), ("UNAVAILABLE", {}))
        self.assertEqual(consensus["providerStatuses"], [{"provider": "yahoo_finance", "status": "PROVIDER_ERROR"}, {"provider": "alpha_vantage", "status": "RATE_LIMIT"}])
        blockers = [b for b in failed["blockers"] if b["family"] == "consensus"]
        self.assertEqual(len(blockers), 1)
        self.assertEqual((blockers[0]["code"], blockers[0]["retryable"]), ("CONSENSUS_PROVIDER_ERROR", True))
        self.assertEqual(blockers[0]["message"], "No estimates were read: yahoo_finance PROVIDER_ERROR, alpha_vantage RATE_LIMIT; retry. Analyst coverage is unknown, not absent.")
        # One provider read is enough to report coverage cell by cell; no data from anyone is missing coverage, not an error.
        one = self.out["qualityConsensusOneFailed"]["families"]["consensus"]
        self.assertNotEqual(one["state"], "UNAVAILABLE")
        self.assertTrue(one["cells"])
        self.assertNotIn("CONSENSUS_PROVIDER_ERROR", [b["code"] for b in self.out["qualityConsensusOneFailed"]["blockers"]])
        no_data = self.out["qualityConsensusNoData"]["families"]["consensus"]
        self.assertEqual(no_data["providerStatuses"], [{"provider": "yahoo_finance", "status": "NO_DATA"}])
        self.assertNotIn("CONSENSUS_PROVIDER_ERROR", [b["code"] for b in self.out["qualityConsensusNoData"]["blockers"]])
        # providerStatuses is always present, including when consensus was not read at all.
        self.assertEqual(self.out["qualityStale"]["families"]["consensus"]["providerStatuses"], [])
        self.assertEqual(self.out["quality"]["families"]["consensus"]["providerStatuses"], [{"provider": "yahoo_finance", "status": "OK"}, {"provider": "alpha_vantage", "status": "OK"}])

    def test_components_and_receipt(self) -> None:
        c = self.out["components"]
        self.assertEqual({k: v["status"] for k, v in c.items()}, {
            "extract_capital_structure": "OK", "extract_guidance": "LIMITED", "extract_dilution_bridge": "FAILED",
            "extract_dilution_bridge_partial": "LIMITED", "market_series_incomplete": "LIMITED",
            "list_sec_material_filings": "FAILED", "get_quote": "FAILED"})
        receipt = self.out["receipt"]
        self.assertEqual(receipt["coverage"]["state"], "PARTIAL")
        self.assertEqual(receipt["coverage"]["scope"], "COMPONENT_WRAPPER_STATUS")
        self.assertEqual(receipt["coverage"]["evidenceCompleteness"], "NOT_ASSERTED")
        cap = next(r for r in receipt["components"] if r["name"] == "extract_capital_structure")
        self.assertEqual(cap["providerTimestamps"], {"source.filingDate": "2026-08-10", "source.periodEnd": "2026-06-30", "source.form": "10-Q"})
        self.assertEqual(len(cap["sha256"]), 64)

    def test_cut_ids(self) -> None:
        self.assertEqual(self.out["cutId"], "ec1_ASTS_20260926T080000Z_" + "a" * 64)
        self.assertEqual(self.out["parsed"]["key"], "evidence-cuts/BRK.B/20260926T080000Z/" + "b" * 64 + ".json")
        self.assertIsNone(self.out["parsedBad"])
        self.assertEqual(self.out["observationKey"], "consensus-history/ASTS/2026-09-26.json")


class TestAuthorityBoundaryOnEveryResult(unittest.TestCase):
    """Terminal statuses of evidence-only actions carry the same authority fields as full results (2.5.8)."""

    def test_terminal_status_gains_the_boundary(self) -> None:
        out = _python_outputs()["boundary"]
        self.assertEqual({k: out[0][k] for k in ev.AUTHORITY_BOUNDARY}, ev.AUTHORITY_BOUNDARY)
        self.assertEqual((out[0]["status"], out[0]["spec"]), ("PERIOD_NOT_FOUND", "FY2026"))
        # Errors, other tools, envelopes and non-objects are unchanged.
        self.assertNotIn("decisionUse", out[1])
        self.assertNotIn("decisionUse", out[2])
        self.assertEqual((out[5], out[6]), ("not json", [1, 2]))
        self.assertNotIn("decisionUse", out[7])
        # A stray value is overwritten, never passed through as authority.
        self.assertIsNone(out[4]["priceTarget"])
        # A complete payload is returned as is, byte for byte.
        self.assertIs(ev.with_authority_boundary("get_guidance_history", COMPLETE), COMPLETE)

    def test_envelope_carries_it_and_errors_carry_no_data(self) -> None:
        from yfmcp.envelope import _envelope_tool_result

        ok = json.loads(_envelope_tool_result("reconcile_metric_sources", BOUNDARY_CASES[0][1]))
        self.assertTrue(ok["ok"])
        self.assertEqual({k: ok["data"][k] for k in ev.AUTHORITY_BOUNDARY}, ev.AUTHORITY_BOUNDARY)
        failed = json.loads(_envelope_tool_result("get_historical_valuation_context", BOUNDARY_CASES[1][1]))
        self.assertEqual((failed["ok"], failed["data"]), (False, None))

    def test_every_evidence_only_action_is_a_catalog_action_and_worker_applies_it(self) -> None:
        catalog = (ROOT / "tool_catalog.json").read_text(encoding="utf-8")
        for action in ev.EVIDENCE_ONLY_ACTIONS:
            self.assertIn(f'"{action}"', catalog, action)
        tools_ts = (WORKER / "src" / "tools.ts").read_text(encoding="utf-8")
        self.assertIn("raw = withAuthorityBoundary(name, await _dispatchTool(name, args));", tools_ts)


if __name__ == "__main__":
    unittest.main()
