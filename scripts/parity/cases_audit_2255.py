"""Parity cases for the 2.25.5 audit repair F29 (analysis/yahoo-finance-mcp/F_TRIAGE.md §5).

F29: Python extract_earnings_metrics reads the Worker's tier 1, SEC XBRL quarter facts, before the release text. A
three-month fact is used as is; a cash-flow quarter is the year-to-date value less the prior year-to-date value; a
10-Q filed before the release, or a quarter that ended more than 120 days ago, is not used. Dates are relative to
today because the recency window reads the clock.
"""

from __future__ import annotations

import datetime

from scripts.parity.cases_sec_earnings import _doc_url, _run, _sec_routes, _srv
from scripts.parity.harness import Case

TODAY = datetime.date.today()


def _d(days_ago: int) -> str:
    return (TODAY - datetime.timedelta(days=days_ago)).isoformat()


def _fact(val, start: str, end: str, filed: str, form: str = "10-Q") -> dict:
    return {"val": val, "start": start, "end": end, "filed": filed, "form": form}


# The quarter ends 50 days ago; the fiscal year began 50 + 272 days ago (nine months to date), the half 50 + 181.
Q_END, Q_START, PRIOR_END = _d(50), _d(50 + 90), _d(50 + 91)
YTD_START = _d(50 + 272)


def _concepts(filed: str) -> dict:
    return {
        "RevenueFromContractWithCustomerExcludingAssessedTax": ("USD", [
            _fact(1_100_000_000, Q_START, Q_END, filed), _fact(3_000_000_000, YTD_START, Q_END, filed)]),
        "EarningsPerShareDiluted": ("USD/shares", [_fact(0.55, Q_START, Q_END, filed), _fact(1.5, YTD_START, Q_END, filed)]),
        "GrossProfit": ("USD", [_fact(440_000_000, Q_START, Q_END, filed)]),
        "OperatingIncomeLoss": ("USD", [_fact(200_000_000, Q_START, Q_END, filed)]),
        "PaymentsToAcquirePropertyPlantAndEquipment": ("USD", [
            _fact(-90_000_000, YTD_START, Q_END, filed), _fact(-60_000_000, YTD_START, PRIOR_END, _d(130))]),
        "NetCashProvidedByUsedInOperatingActivities": ("USD", [
            _fact(900_000_000, YTD_START, Q_END, filed), _fact(600_000_000, YTD_START, PRIOR_END, _d(130))]),
    }


RELEASE = "Acme Reports Second Quarter Fiscal 2026 Results. Revenue was $1.0 billion for the quarter."


def _case(name: str, tkr: str, cik: int, filed: str, expect: dict) -> Case:
    accn = f"0000{cik:06d}-26-000001"
    doc = _doc_url(cik, accn, "r8k.htm")
    routes = _sec_routes(tkr, cik, [{"accession": accn, "date": _d(20), "primary": "r8k.htm"}], {doc: f"<html><body><p>{RELEASE}</p></body></html>"})
    routes["https://data.sec.gov/api/xbrl/companyconcept/*"] = {"status": 404}
    for concept, (unit, facts) in _concepts(filed).items():
        routes[f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/us-gaap/{concept}.json"] = {"json": {"units": {unit: facts}}}
    return Case(name=name, ids=["F29"], py=lambda: _run(lambda: _srv().extract_earnings_metrics(tkr))(),
                ts={"js": "m.response.setWorkerEnv({}); return m.yahooFinance.extractEarningsMetrics(args[0]);", "args": [tkr], "isolate": True},
                routes=routes, select=_view, expect=expect)


def _view(v: dict) -> dict:
    d = v.get("data") if isinstance(v.get("data"), dict) else v
    metrics = {}
    for key, m in (d.get("metrics") or {}).items():
        # A metric with no value is left out: its confidence label already differs between runtimes on the text
        # path (Python NOT_DECISION_GRADE, Worker NOT_DISCLOSED), which F29 does not change.
        if isinstance(m, dict) and m.get("value", m.get("valuePct")) is not None:
            metrics[key] = [m.get("value", m.get("valuePct")), m.get("confidence"), m.get("derivation")]
    return {"metrics": metrics, "confidence": d.get("confidence"),
            "evidence": [e.get("sourceType") for e in d.get("evidence") or []]}


XBRL = {"revenue": [1_100_000_000, "HIGH", "DIRECT_QUARTER"], "epsDiluted": [0.55, "HIGH", "DIRECT_QUARTER"],
        "grossMargin": [40.0, "HIGH", "DIRECT_QUARTER"], "operatingIncome": [200_000_000, "HIGH", "DIRECT_QUARTER"],
        "freeCashFlow": [270_000_000, "HIGH", "YTD_DIFFERENCE"], "capex": [30_000_000, "HIGH", "YTD_DIFFERENCE"]}
TEXT_ONLY = {"revenue": [1_000_000_000, "LOW", None]}

CASES = [
    _case("audit2255.earnings_metrics.xbrl_quarter_facts_come_first", "ZQX", 840101, _d(10),
          {"metrics": XBRL, "confidence": "HIGH", "evidence": ["sec_xbrl_10q"] * 6}),
    _case("audit2255.earnings_metrics.a_10q_filed_before_the_release_is_not_used", "ZQY", 840102, _d(30),
          {"metrics": TEXT_ONLY, "confidence": "LOW", "evidence": ["sec_8k"]}),
]


# selectQuarterFact on its own: a 10-K fourth quarter is the fiscal year less nine months; per-share amounts are
# never derived; a segment fact or a non-10-Q/10-K form is skipped.
from yfmcp.tools.earnings import _select_quarter_fact  # noqa: E402

_FY = [_fact(400, "2025-01-01", "2025-12-31", "2026-02-20", "10-K"), _fact(290, "2025-01-01", "2025-09-30", "2025-11-01"),
       {**_fact(999, "2025-10-01", "2025-12-31", "2026-02-20", "10-K"), "segment": {"dimension": "x"}},
       _fact(111, "2025-10-01", "2025-12-31", "2026-02-21", "8-K")]
for _key, _derive, _expect in [
    ("fourth_quarter_is_fiscal_year_less_nine_months", True,
     {"value": 110, "start": "2025-10-01", "end": "2025-12-31", "filed": "2026-02-20", "form": "10-K", "method": "YTD_DIFFERENCE"}),
    ("per_share_amount_is_not_derived", False, None),
]:
    CASES.append(Case(
        name=f"audit2255.select_quarter_fact.{_key}", ids=["F29"],
        py=(lambda d=_derive: _select_quarter_fact(_FY, "2026-01-15", d)),
        ts={"module": "yahooFinance", "fn": "selectQuarterFact", "args": [_FY, {"minFiled": "2026-01-15", "derive": _derive}]},
        expect=_expect))
