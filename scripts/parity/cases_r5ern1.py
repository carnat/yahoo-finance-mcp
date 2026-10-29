"""Cases for R5-earnings part 1 (3.3.0) that have no older case to flip.

compare_earnings_actual_vs_estimate:
- Source policy R3: actual EPS is the official release figure, with its own source and confidence; Yahoo's epsActual is
  the fallback only when the release states none, labelled yahoo and LOW. A release EPS is compared only with the
  estimate of the same quarter: when the periods are not MATCHED, the EPS surprise is withheld with the revenue
  surprise and the alignment warning says so.
- Q3i call 5 (alignment half): Python matches a structured quarter to a dated Yahoo row when their period ends are
  within a week, as the Worker does, and reports quarterEnd from the release's period end.
- Surprise figures round half away from zero in both runtimes (Python's round() was half-even).

The zero-estimate, dated-row, release-EPS and source_url-message repairs flip their cases in cases_sec_earnings.py.
"""

from __future__ import annotations

import dataclasses

from scripts.parity.cases_sec_earnings import _REL_Q2, _REVEST, _ROW, _cmp, _compare_case, _compare_view, _unwrap
from scripts.parity.harness import Case

CASES: list[Case] = []

_EPS_TEXT = " Diluted net income per share was $0.60 for the quarter."


def _actual_eps_view(v):
    d = _unwrap(v)
    eps = (d.get("actual") or {}).get("eps") or {}
    # sourceType is added by the envelope layer, which the Worker's direct call does not run, so it is left out.
    return {k: eps.get(k) for k in ("value", "source", "confidence", "period", "decisionGrade")}


CASES += [
    # The release EPS and Yahoo's row name different quarters: the release figure is still the actual, and no EPS
    # surprise is computed against the other quarter's estimate.
    _compare_case("r5ern1.compare.release_eps_is_not_compared_with_another_quarters_estimate", ["BR-ERN-018", "BR-ERN-001"], "ZRA", 820101,
                  _REL_Q2 + _EPS_TEXT, _ROW(0.55, 0.50, "FY2026 Q1"), [{"period": "FY2026 Q1", "avg": 900_000_000}],
                  _cmp(reportedDate="FY2026 Q1", estimatePeriod="FY2026 Q1", periodAlignmentStatus="MISMATCH", actualEps=0.6,
                       estimateRevenue=900_000_000, revenueSurprisePct=None, epsSurprisePct=None, epsDelta=None,
                       confidence="NOT_DISCLOSED", warningCodes=["PERIOD_ALIGNMENT_MISMATCH"])),
    # The same with a Yahoo row named only by its date: UNVERIFIED, the revenue estimate and both surprises withheld.
    _compare_case("r5ern1.compare.release_eps_is_not_compared_with_an_unverified_row", ["BR-ERN-018", "BR-ERN-001"], "ZRB", 820102,
                  _REL_Q2 + _EPS_TEXT, _ROW(0.55, 0.50, "2026-06-30"), [{"period": "2026-06-30", "avg": 1_000_000_000}],
                  _cmp(reportedDate="2026-06-30", estimatePeriod="2026-06-30", periodAlignmentStatus="UNVERIFIED", actualEps=0.6,
                       estimateRevenue=None, revenueSurprisePct=None, epsSurprisePct=None, epsDelta=None,
                       confidence="NOT_DISCLOSED", warningCodes=["PERIOD_ALIGNMENT_UNVERIFIED"])),
    # A revenue surprise of exactly 0.125% rounds to 0.13 in both runtimes (Python's half-even round gave 0.12).
    _compare_case("r5ern1.compare.surprise_rounds_half_away_from_zero", ["BR-ERN-018"], "ZRC", 820103,
                  "Acme Reports Second Quarter Fiscal 2026 Results. Revenue was $801 million for the quarter.", _ROW(0.55, 0.50),
                  [{"period": "FY2026 Q2", "avg": 800_000_000}],
                  _cmp(actualRevenue=801_000_000, estimateRevenue=800_000_000, revenueSurprisePct=0.13)),
]

# The actual EPS row: the release figure carries its own extraction confidence (LOW from text) and the release period;
# a Yahoo fallback is labelled yahoo and LOW with the Yahoo row's period.
CASES += [
    dataclasses.replace(
        _compare_case("r5ern1.compare.release_eps_row_keeps_its_source_and_period", ["BR-ERN-018"], "ZRD", 820104,
                      _REL_Q2 + _EPS_TEXT, _ROW(0.55, 0.50), _REVEST, None),
        select=_actual_eps_view,
        expect={"value": 0.6, "source": None, "confidence": "LOW", "period": "FY2026 Q2", "decisionGrade": False}),
    dataclasses.replace(
        _compare_case("r5ern1.compare.yahoo_eps_is_the_labelled_fallback_when_the_release_has_none", ["BR-ERN-018", "F22"], "ZRE", 820105,
                      _REL_Q2, _ROW(0.55, 0.50), _REVEST, None),
        select=_actual_eps_view,
        expect={"value": 0.55, "source": "yahoo", "confidence": "LOW", "period": "FY2026 Q2", "decisionGrade": False}),
]


# Call 5, alignment: XBRL gives the quarter's period end (2026-06-27); Yahoo dates its row 2026-06-30. Within a week,
# so both runtimes report MATCHED, compare the XBRL EPS with the estimate and give quarterEnd 2026-06-27.
def _fact(val, start, end, filed="2026-07-31"):
    return {"val": val, "start": start, "end": end, "filed": filed, "form": "10-Q"}


_XBRL = {
    "RevenueFromContractWithCustomerExcludingAssessedTax": ("USD", [_fact(1_100_000_000, "2026-03-29", "2026-06-27")]),
    "EarningsPerShareDiluted": ("USD/shares", [_fact(0.55, "2026-03-29", "2026-06-27")]),
}


def _xbrl_case(name, tkr, cik, expect, select):
    base = _compare_case(name, ["BR-ERN-001", "BR-ERN-018"], tkr, cik, _REL_Q2, _ROW(0.55, 0.50, "2026-06-30"),
                         [{"period": "2026-06-30", "avg": 1_000_000_000}], expect)
    routes = dict(base.routes)
    for concept, (unit, facts) in _XBRL.items():
        routes[f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/us-gaap/{concept}.json"] = {"json": {"units": {unit: facts}}}
    return dataclasses.replace(base, routes=routes, select=select)


def _aligned_view(v):
    d = _unwrap(v)
    return {**_compare_view(v), "quarterEnd": d.get("quarterEnd"),
            "actualEpsConfidence": ((d.get("actual") or {}).get("eps") or {}).get("confidence")}


CASES.append(_xbrl_case(
    "r5ern1.compare.period_ends_within_a_week_match_a_dated_row", "ZRF", 820106,
    {**_cmp(reportedDate="2026-06-30", estimatePeriod="2026-06-30", actualRevenue=1_100_000_000, actualEps=0.55),
     "quarterEnd": "2026-06-27", "actualEpsConfidence": "HIGH"},
    _aligned_view))
