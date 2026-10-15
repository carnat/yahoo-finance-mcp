"""Parity cases: SEC fact selection / fiscal calendar and earnings tools (Phase 2, domains "sec" and "earnings").

Pure modules (compared directly, no network):
  yfmcp/fiscal_calendar.py      <-> worker/src/fiscal-calendar.ts       BR-SEC-001, 005, 006 (+ 016-022, 027)
  yfmcp/sec_facts.py            <-> worker/src/sec-facts.ts             BR-SEC-002, 003, 004, 007
  yfmcp/companyfacts_coverage.py<-> worker/src/companyfacts-coverage.ts BR-SEC-023, 024
  yfmcp/guidance_history.py     <-> worker/src/guidance-history.ts      BR-ERN-002, 003, 005
  yfmcp/earnings_period.py      <-> worker/src/earnings-period.ts       BR-ERN-012..014

Tool level (SEC / Yahoo HTTP served from `routes` to both runtimes; Python's yfinance and cached SEC lookups are
reset or patched inside the `py` callable):
  yfmcp/tools/earnings.py       <-> the earnings section of worker/src/yahoo-finance.ts
  BR-ERN-001 / 018 / 031 (compare_earnings_actual_vs_estimate), BR-ERN-004 / 020 (extract_guidance),
  BR-ERN-009 (transcript fallback), BR-ERN-017 (earnings source URL policy).

Expected values come from the rule cards in analysis/yahoo-finance-mcp/BUSINESS_RULES.md. Where both runtimes agree
but differ from the card, or the code looks wrong, the case keeps today's value and carries a
`# Finding (Phase 2, for Q3i triage)` comment; a Python/Worker difference with no owner decision is a `pinned`
case whose decision is `FINDING: ...`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from unittest.mock import AsyncMock, patch

from scripts.parity.harness import FIXTURES, Case

from yfmcp import companyfacts_coverage as cfc
from yfmcp import earnings_period as ep
from yfmcp import fiscal_calendar as fc
from yfmcp import guidance_history as gh
from yfmcp import sec_facts as sf


def _ts(module: str, fn: str, *args) -> dict:
    return {"module": module, "fn": fn, "args": list(args)}


# ── companyfacts builders ───────────────────────────────────────────────────────────────────────

def _row(start, end, fy, accn, *, form="10-K", fp="FY", filed="2026-03-01", val=1_000_000):
    out = {"end": end, "val": val, "accn": accn, "fy": fy, "fp": fp, "form": form, "filed": filed}
    if start is not None:
        out["start"] = start
    return out


def _facts(rows, concept="Revenues", unit="USD", extra=None):
    """us-gaap companyfacts with one concept's rows (plus any `extra` {concept: (unit, rows)})."""
    gaap = {concept: {"units": {unit: list(rows)}}}
    for name, (u, r) in (extra or {}).items():
        gaap[name] = {"units": {u: list(r)}}
    return {"facts": {"us-gaap": gaap}}


# DG-like: 52/53-week year ending the Friday nearest January 31, named for the year it STARTS in
# (BR-SEC-001 card: year ended 2026-01-30 states fy 2025, 2025-01-31 states 2024, 2024-02-02 states 2023).
DG_KS = [
    _row("2025-02-01", "2026-01-30", 2025, "0000000001-26-000010", filed="2026-03-20"),
    _row("2024-02-03", "2025-01-31", 2024, "0000000001-25-000010", filed="2025-03-20"),
    _row("2023-02-04", "2024-02-02", 2023, "0000000001-24-000010", filed="2024-03-20"),
]
DG_FACTS = _facts(DG_KS)


def _naming_fields(v):
    return {k: v[k] for k in ("offset", "basis", "periodEnd", "statedFiscalYear")}


def _naming_case(name, ids, facts, expect, **kw):
    return Case(name=name, ids=ids, py=lambda: fc.fiscal_year_naming(facts),
                ts=_ts("fiscalCalendar", "fiscalYearNaming", facts), select=_naming_fields, expect=expect, **kw)


def _fy_case(name, ids, end, expect):
    return Case(name=name, ids=ids, py=lambda: fc.fiscal_year_of_period_end(end),
                ts=_ts("fiscalCalendar", "fiscalYearOfPeriodEnd", end), expect=expect)


def _file_ts(module: str, fn: str, relpath: str) -> dict:
    """Worker side of a call on a recorded fixture, read from disk inside Node (keeps cases.json small)."""
    return {"js": f"const fs = await import('node:fs'); const d = JSON.parse(fs.readFileSync(fx.path, 'utf8')); return m.{module}.{fn}(d);",
            "fx": {"path": str(FIXTURES / relpath)}}


def _fixture(relpath: str):
    return json.loads((FIXTURES / relpath).read_text(encoding="utf-8"))


# ═══ BR-SEC-005 / BR-SEC-006: fiscal year of a period end (7-day drift) ═══════════════════════════
_SEC005 = ["BR-SEC-005", "BR-SEC-006"]

CASES = [
    _fy_case("sec.fiscal_year_of_period_end.jan_2_belongs_to_prior_year", _SEC005, "2027-01-02", 2026),
    _fy_case("sec.fiscal_year_of_period_end.jan_8_is_new_year", _SEC005, "2027-01-08", 2027),
    # Boundary of the drift window: the 7th still belongs to the prior year, the 8th does not.
    _fy_case("sec.fiscal_year_of_period_end.jan_7_last_day_of_prior_year", _SEC005, "2027-01-07", 2026),
    _fy_case("sec.fiscal_year_of_period_end.jan_1_quarter_end", _SEC005, "2026-01-01", 2025),
    _fy_case("sec.fiscal_year_of_period_end.jan_3_quarter_end", _SEC005, "2026-01-03", 2025),
    _fy_case("sec.fiscal_year_of_period_end.dec_31_calendar_year", _SEC005, "2025-12-31", 2025),
    _fy_case("sec.fiscal_year_of_period_end.dec_26_52_week_year", _SEC005, "2026-12-26", 2026),
    _fy_case("sec.fiscal_year_of_period_end.september_year_end", _SEC005, "2025-09-27", 2025),
    _fy_case("sec.fiscal_year_of_period_end.timestamp_suffix_ignored", _SEC005, "2027-01-02T00:00:00Z", 2026),
    _fy_case("sec.fiscal_year_of_period_end.invalid_calendar_day_is_none", _SEC005, "2027-02-30", None),
    _fy_case("sec.fiscal_year_of_period_end.non_iso_is_none", _SEC005, "20270102", None),
    _fy_case("sec.fiscal_year_of_period_end.empty_is_none", _SEC005, "", None),
    _fy_case("sec.fiscal_year_of_period_end.null_is_none", _SEC005, None, None),
    Case(name="sec.fiscal_year_label.jan_2_is_fy2026", ids=_SEC005, py=lambda: fc.fiscal_year_label("2027-01-02"),
         ts=_ts("fiscalCalendar", "fiscalYearLabel", "2027-01-02"), expect="FY2026"),
    Case(name="sec.fiscal_year_label.jan_8_is_fy2027", ids=_SEC005, py=lambda: fc.fiscal_year_label("2027-01-08"),
         ts=_ts("fiscalCalendar", "fiscalYearLabel", "2027-01-08"), expect="FY2027"),
    Case(name="sec.fiscal_year_label.invalid_is_none", ids=_SEC005, py=lambda: fc.fiscal_year_label("garbage"),
         ts=_ts("fiscalCalendar", "fiscalYearLabel", "garbage"), expect=None),

    # ═══ BR-SEC-001 (+ 010, 011): the company's own fiscal-year naming offset ═════════════════════
    _naming_case("sec.fiscal_year_naming.dg_offset_minus_one", ["BR-SEC-001", "BR-SEC-010", "BR-SEC-011"], DG_FACTS,
                 {"offset": -1, "basis": "SEC_STATED_FISCAL_YEAR", "periodEnd": "2026-01-30", "statedFiscalYear": 2025}),
    _naming_case("sec.fiscal_year_naming.sec_not_read", ["BR-SEC-001"], None,
                 {"offset": 0, "basis": "PERIOD_END_RULE_SEC_NOT_READ", "periodEnd": None, "statedFiscalYear": None}),
    _naming_case("sec.fiscal_year_naming.no_stated_year_empty_facts", ["BR-SEC-001"], {"facts": {}},
                 {"offset": 0, "basis": "PERIOD_END_RULE_NO_STATED_YEAR", "periodEnd": None, "statedFiscalYear": None}),
    # 20-F / 40-F filers are not read for naming (BR-SEC-001 edge case; card SME question).
    _naming_case("sec.fiscal_year_naming.20f_filer_not_read", ["BR-SEC-001"],
                 _facts([_row("2025-04-01", "2026-03-31", 2025, "0000000002-26-000001", form="20-F")]),
                 {"offset": 0, "basis": "PERIOD_END_RULE_NO_STATED_YEAR", "periodEnd": None, "statedFiscalYear": None}),
    _naming_case("sec.fiscal_year_naming.offsets_disagree", ["BR-SEC-001"],
                 _facts([_row("2025-02-01", "2026-01-30", 2025, "A-1"), _row("2024-02-03", "2025-01-31", 2025, "A-2", filed="2025-03-20"),
                         _row("2023-02-04", "2024-02-02", 2023, "A-3", filed="2024-03-20")]),
                 {"offset": 0, "basis": "PERIOD_END_RULE_STATED_YEARS_INCONSISTENT", "periodEnd": "2026-01-30", "statedFiscalYear": 2025}),
    # Offset -3 (stated 2023 for a year ended 2026) is beyond the +-1 cap: the period-end rule is used.
    _naming_case("sec.fiscal_year_naming.offset_beyond_one_year_rejected", ["BR-SEC-001"],
                 _facts([_row("2025-07-01", "2026-06-30", 2023, "B-1"), _row("2024-07-01", "2025-06-30", 2022, "B-2", filed="2025-03-20"),
                         _row("2023-07-01", "2024-06-30", 2021, "B-3", filed="2024-03-20")]),
                 {"offset": 0, "basis": "PERIOD_END_RULE_STATED_YEARS_INCONSISTENT", "periodEnd": "2026-06-30", "statedFiscalYear": 2023}),
    # Only the newest three reports count: an older, disagreeing fourth is ignored.
    _naming_case("sec.fiscal_year_naming.only_newest_three_reports_count", ["BR-SEC-001"],
                 _facts(DG_KS + [_row("2022-02-05", "2023-02-03", 2020, "0000000001-23-000010", filed="2023-03-20")]),
                 {"offset": -1, "basis": "SEC_STATED_FISCAL_YEAR", "periodEnd": "2026-01-30", "statedFiscalYear": 2025}),
    # A fact whose period is not ~one year (300 days) is not an annual report.
    _naming_case("sec.fiscal_year_naming.non_annual_duration_ignored", ["BR-SEC-001"],
                 _facts([_row("2025-04-01", "2026-01-30", 2025, "C-1")]),
                 {"offset": 0, "basis": "PERIOD_END_RULE_NO_STATED_YEAR", "periodEnd": None, "statedFiscalYear": None}),
    Case(name="sec.fiscal_year_naming.aapl_fixture_offset_zero", ids=["BR-SEC-001"], py=lambda: fc.fiscal_year_naming(_fixture("sec/companyfacts_AAPL.json")),
         ts=_file_ts("fiscalCalendar", "fiscalYearNaming", "sec/companyfacts_AAPL.json"), select=_naming_fields,
         expect={"offset": 0, "basis": "SEC_STATED_FISCAL_YEAR", "periodEnd": "2025-09-27", "statedFiscalYear": 2025}),
    Case(name="sec.fiscal_year_naming.cohr_fixture_offset_zero", ids=["BR-SEC-001"], py=lambda: fc.fiscal_year_naming(_fixture("sec/companyfacts_COHR.json")),
         ts=_file_ts("fiscalCalendar", "fiscalYearNaming", "sec/companyfacts_COHR.json"), select=_naming_fields,
         expect={"offset": 0, "basis": "SEC_STATED_FISCAL_YEAR", "periodEnd": "2026-06-30", "statedFiscalYear": 2026}),
]


# ── fiscal quarter, calendar rule and filing labels (BR-SEC-016..022, 027: P1, shared with the P0 year rule) ──────
def _quarter(end, year_end, expect, name, ids):
    return Case(name=name, ids=ids, py=lambda: fc.fiscal_quarter_of(end, year_end), ts=_ts("fiscalCalendar", "fiscalQuarterOf", end, year_end), expect=expect)


CASES += [
    _quarter("2025-09-27", "2025-09-27", 4, "sec.fiscal_quarter_of.year_end_is_q4", ["BR-SEC-019", "BR-SEC-020", "BR-SEC-021"]),
    _quarter("2025-06-28", "2025-09-27", 3, "sec.fiscal_quarter_of.one_quarter_back_is_q3", ["BR-SEC-019", "BR-SEC-020", "BR-SEC-021"]),
    _quarter("2024-12-28", "2025-09-27", 1, "sec.fiscal_quarter_of.three_quarters_back_is_q1", ["BR-SEC-019", "BR-SEC-020", "BR-SEC-021"]),
    # A 52-week year ending 2026-01-02: its third quarter ends 2025-10-04 (90 days before), its first 2025-04-05.
    _quarter("2025-10-04", "2026-01-02", 3, "sec.fiscal_quarter_of.q3_of_year_ending_jan_2", ["BR-SEC-019", "BR-SEC-020"]),
    _quarter("2025-04-05", "2026-01-02", 1, "sec.fiscal_quarter_of.q1_of_year_ending_jan_2", ["BR-SEC-019", "BR-SEC-020"]),
    _quarter("2026-01-08", "2026-01-02", 4, "sec.fiscal_quarter_of.six_days_past_year_end_still_q4", ["BR-SEC-019"]),
    _quarter("2026-01-10", "2026-01-02", None, "sec.fiscal_quarter_of.eight_days_past_year_end_is_none", ["BR-SEC-019"]),
    _quarter("2026-10-31", "2026-06-30", None, "sec.fiscal_quarter_of.after_year_end_is_none", ["BR-SEC-019"]),
    _quarter("2024-09-01", "2025-09-27", None, "sec.fiscal_quarter_of.more_than_280_days_back_is_none", ["BR-SEC-019"]),
    _quarter("2025-08-10", "2025-09-27", None, "sec.fiscal_quarter_of.not_near_a_quarter_boundary_is_none", ["BR-SEC-019"]),
    Case(name="sec.filing_fiscal_year_label.focus_within_one_year_wins", ids=["BR-SEC-012", "BR-SEC-013", "BR-SEC-014"],
         py=lambda: fc.filing_fiscal_year_label(2025, "2026-01-30"), ts=_ts("fiscalCalendar", "filingFiscalYearLabel", 2025, "2026-01-30"), expect="FY2025"),
    Case(name="sec.filing_fiscal_year_label.mistag_beyond_one_year_uses_period_end_rule", ids=["BR-SEC-012", "BR-SEC-014"],
         py=lambda: fc.filing_fiscal_year_label(2020, "2026-01-30"), ts=_ts("fiscalCalendar", "filingFiscalYearLabel", 2020, "2026-01-30"), expect="FY2026"),
    Case(name="sec.filing_fiscal_year_label.untagged_filing_uses_period_end_rule", ids=["BR-SEC-012", "BR-SEC-013"],
         py=lambda: fc.filing_fiscal_year_label(None, "2027-01-02"), ts=_ts("fiscalCalendar", "filingFiscalYearLabel", None, "2027-01-02"), expect="FY2026"),
    Case(name="sec.filing_fiscal_year_label.no_date_and_no_focus_is_none", ids=["BR-SEC-012"],
         py=lambda: fc.filing_fiscal_year_label(None, None), ts=_ts("fiscalCalendar", "filingFiscalYearLabel", None, None), expect=None),
    # BR-SEC-016/017/018/032: calendar rule detection. DG ends: Friday nearest January 31 (2026-01-30, 2025-01-31).
    Case(name="sec.fiscal_calendar.weekday_nearest_month_end_dg", ids=["BR-SEC-016", "BR-SEC-017", "BR-SEC-018", "BR-SEC-032"],
         py=lambda: fc.fiscal_calendar(["2026-01-30", "2025-01-31", "2024-02-02"]),
         ts=_ts("fiscalCalendar", "fiscalCalendar", ["2026-01-30", "2025-01-31", "2024-02-02"]),
         select=lambda v: {k: v[k] for k in ("patterns", "month", "weekday")},
         expect={"patterns": ["WEEKDAY_NEAREST_MONTH_END"], "month": 1, "weekday": "Friday"}),
    Case(name="sec.fiscal_calendar.month_end_calendar_year", ids=["BR-SEC-016", "BR-SEC-018"],
         py=lambda: fc.fiscal_calendar(["2025-12-31", "2024-12-31"]), ts=_ts("fiscalCalendar", "fiscalCalendar", ["2025-12-31", "2024-12-31"]),
         select=lambda v: {k: v[k] for k in ("patterns", "month", "weekday")}, expect={"patterns": ["MONTH_END"], "month": 12, "weekday": None}),
    Case(name="sec.fiscal_calendar.single_period_end_is_none", ids=["BR-SEC-016"],
         py=lambda: fc.fiscal_calendar(["2025-12-31"]), ts=_ts("fiscalCalendar", "fiscalCalendar", ["2025-12-31"]), expect=None),
    Case(name="sec.fiscal_calendar.mixed_months_is_none", ids=["BR-SEC-016"],
         py=lambda: fc.fiscal_calendar(["2025-12-31", "2024-06-30"]), ts=_ts("fiscalCalendar", "fiscalCalendar", ["2025-12-31", "2024-06-30"]), expect=None),
    Case(name="sec.company_fiscal_year_end.projects_provider_month_end_to_friday", ids=["BR-SEC-027", "BR-SEC-018"],
         py=lambda: fc.company_fiscal_year_end(fc.fiscal_calendar(["2026-01-30", "2025-01-31", "2024-02-02"]), "2027-01-31"),
         ts={"js": "const c = m.fiscalCalendar.fiscalCalendar(['2026-01-30','2025-01-31','2024-02-02']); return m.fiscalCalendar.companyFiscalYearEnd(c, '2027-01-31');"},
         expect="2027-01-29"),
    Case(name="sec.company_fiscal_year_end.other_month_is_none", ids=["BR-SEC-027"],
         py=lambda: fc.company_fiscal_year_end(fc.fiscal_calendar(["2026-01-30", "2025-01-31"]), "2027-06-30"),
         ts={"js": "const c = m.fiscalCalendar.fiscalCalendar(['2026-01-30','2025-01-31']); return m.fiscalCalendar.companyFiscalYearEnd(c, '2027-06-30');"},
         expect=None),
]

# ═══ BR-SEC-002 / BR-SEC-003: equivalent-concept selection by newest filing ═══════════════════════════
_EXCL = "RevenueFromContractWithCustomerExcludingAssessedTax"
_INCL = "RevenueFromContractWithCustomerIncludingAssessedTax"


def _cf(concept, *rows):
    return {"concept": concept, "facts": list(rows)}


def _f(filed, accn, *, form="10-K", end="2024-12-31", val=100):
    return {"form": form, "filed": filed, "accn": accn, "end": end, "start": "2024-01-01", "val": val}


def _pick(name, ids, candidates, form, accession=None, on_tie="first", expect=None):
    return Case(name=name, ids=ids, py=lambda: sf.pick_concept_facts(candidates, form, accession, on_tie),
                ts=_ts("secFacts", "pickConceptFacts", candidates, form, accession, on_tie),
                select=lambda v: None if v is None else {"concept": v["concept"], "accns": [f["accn"] for f in v["facts"]]}, expect=expect)


_P23 = ["BR-SEC-002", "BR-SEC-003"]
CASES += [
    # Card example: ExcludingAssessedTax last filed 2023, IncludingAssessedTax filed 2025 -> Including wins.
    _pick("sec.pick_concept.newest_filing_wins_over_listed_order", _P23,
          [_cf(_EXCL, _f("2023-03-01", "A-23")), _cf(_INCL, _f("2025-03-01", "A-25"))], "10-K",
          expect={"concept": _INCL, "accns": ["A-25"]}),
    _pick("sec.pick_concept.card_003_dates", _P23,
          [_cf(_EXCL, _f("2023-02-28", "A-23")), _cf(_INCL, _f("2025-02-27", "A-25"))], "10-K",
          expect={"concept": _INCL, "accns": ["A-25"]}),
    _pick("sec.pick_concept.tie_goes_to_earlier_listed", _P23,
          [_cf(_EXCL, _f("2025-03-01", "A-1", val=10)), _cf(_INCL, _f("2025-03-01", "A-2", val=99))], "10-K",
          expect={"concept": _EXCL, "accns": ["A-1"]}),
    _pick("sec.pick_concept.tie_larger_mode_takes_larger_newest_period", _P23,
          [_cf(_EXCL, _f("2025-03-01", "A-1", val=10)), _cf(_INCL, _f("2025-03-01", "A-2", val=99))], "10-K", on_tie="larger",
          expect={"concept": _INCL, "accns": ["A-2"]}),
    _pick("sec.pick_concept.larger_mode_does_not_override_newer_filing", _P23,
          [_cf(_EXCL, _f("2025-03-01", "A-1", val=10)), _cf(_INCL, _f("2024-03-01", "A-2", val=99))], "10-K", on_tie="larger",
          expect={"concept": _EXCL, "accns": ["A-1"]}),
    _pick("sec.pick_concept.pinned_accession_restricts_facts", _P23,
          [_cf(_EXCL, _f("2025-03-01", "A-NEW"), _f("2023-03-01", "A-OLD")), _cf(_INCL, _f("2025-09-01", "A-OTHER"))], "10-K", accession="A-OLD",
          expect={"concept": _EXCL, "accns": ["A-OLD"]}),
    _pick("sec.pick_concept.pinned_accession_with_whitespace_is_trimmed", _P23,
          [_cf(_EXCL, _f("2023-03-01", "A-OLD"))], "10-K", accession="  A-OLD ", expect={"concept": _EXCL, "accns": ["A-OLD"]}),
    _pick("sec.pick_concept.form_matches_case_insensitively", _P23,
          [_cf(_EXCL, _f("2025-03-01", "A-1", form="10-K"))], "10-k", expect={"concept": _EXCL, "accns": ["A-1"]}),
    _pick("sec.pick_concept.other_forms_do_not_count_toward_recency", _P23,
          [_cf(_EXCL, _f("2025-03-01", "A-1", form="10-K"), _f("2026-05-01", "A-Q", form="10-Q")), _cf(_INCL, _f("2025-09-01", "A-2", form="10-K"))], "10-K",
          expect={"concept": _INCL, "accns": ["A-2"]}),
    _pick("sec.pick_concept.no_fact_for_form_is_none", _P23, [_cf(_EXCL, _f("2025-03-01", "A-1"))], "10-Q", expect=None),
    _pick("sec.pick_concept.non_list_facts_are_skipped", _P23,
          [{"concept": _EXCL, "facts": {}}, _cf(_INCL, _f("2020-01-01", "A-2"))], "10-K", expect={"concept": _INCL, "accns": ["A-2"]}),
    Case(name="sec.revenue_concepts.listed_order", ids=_P23, py=lambda: sf.REVENUE_CONCEPTS, ts={"js": "return m.secFacts.REVENUE_CONCEPTS;"},
         expect=[_EXCL, _INCL, "Revenues", "SalesRevenueNet"]),
]

# ═══ BR-SEC-004: filing period is "latest" or a fiscal year ═══════════════════════════════════════
def _period(raw, expect, name):
    return Case(name=f"sec.parse_filing_period.{name}", ids=["BR-SEC-004"], py=lambda: sf.parse_filing_period(raw),
                ts=_ts("secFacts", "parseFilingPeriod", raw), expect=expect)


_LATEST = {"kind": "latest"}
CASES += [
    _period("latest", _LATEST, "latest"),
    _period(None, _LATEST, "null_is_latest"),
    _period("", _LATEST, "blank_is_latest"),
    _period("  ", _LATEST, "whitespace_is_latest"),
    _period("LATEST", _LATEST, "latest_is_case_insensitive"),
    _period("FY2025", {"kind": "fiscalYear", "year": 2025}, "fy2025"),
    _period("FY 2025", {"kind": "fiscalYear", "year": 2025}, "fy_space_2025"),
    _period("2025", {"kind": "fiscalYear", "year": 2025}, "bare_year"),
    _period("fy2025", {"kind": "fiscalYear", "year": 2025}, "lowercase_fy2025_accepted"),
    _period(" 2025 ", {"kind": "fiscalYear", "year": 2025}, "surrounding_space_trimmed"),
    _period("Q3 2025", None, "quarter_rejected"),
    _period("FY25", None, "two_digit_year_rejected"),
    _period("FY  2025", None, "two_spaces_rejected"),
    _period("20255", None, "five_digits_rejected"),
    _period("FY2025Q3", None, "trailing_text_rejected"),
    _period("junk", None, "junk_rejected"),
    _period("2025-12-31", None, "iso_date_rejected"),
]


def _srv():
    import server as srv
    return srv


def _tool_error(v):
    """The error message of a failed tool call, in either runtime's envelope."""
    return (v.get("error") or {}).get("message") if isinstance(v.get("error"), dict) else v.get("error")


for _raw in ("Q3 2025", "FY25"):
    CASES.append(Case(
        name=f"sec.period_validation.extract_total_revenue_refuses_{_raw.lower().replace(' ', '_')}", ids=["BR-SEC-004"],
        py=lambda raw=_raw: _srv().extract_total_revenue("XYZ", "10-K", raw),
        ts={"js": "m.response.setWorkerEnv({}); return m.tools.callTool('extract_total_revenue', {ticker: 'XYZ', period: args[0]});", "args": [_raw]},
        select=lambda v: {"ok": v.get("ok"), "code": (v.get("error") or {}).get("code"), "message": (v.get("error") or {}).get("message")},
        expect={"ok": False, "code": "INPUT_VALIDATION_ERROR",
                "message": f"period must be \"latest\" or a fiscal year (\"FY2025\" or \"2025\"). Got '{_raw}'."}))

# ═══ BR-SEC-007: fiscal-year assignment for annual SEC facts (52/53-week years) ═════════════════════
def _rows_case(name, ids, rows, year, expect):
    return Case(name=name, ids=ids, py=lambda: sf.select_fiscal_year_rows(rows, year),
                ts=_ts("secFacts", "selectFiscalYearRows", rows, year),
                select=lambda v: {"accns": [r["accn"] for r in v["rows"]], "fiscalYears": v["fiscalYears"]}, expect=expect)


_52W = ("2025-01-04", "2026-01-03")   # a 52-week year ending Saturday 2026-01-03
_P7 = ["BR-SEC-007"]
CASES += [
    # Card example: no fp=FY on the first filing, so the year is the year of (end - 7 days) = 2025.
    _rows_case("sec.fiscal_year_rows.jan_3_year_end_without_fp_is_fiscal_2025", _P7,
               [_row(*_52W, 2025, "R-1", fp="Q4", filed="2026-02-20")], 2025, {"accns": ["R-1"], "fiscalYears": [2025]}),
    _rows_case("sec.fiscal_year_rows.jan_3_year_end_is_not_fiscal_2026", _P7,
               [_row(*_52W, 2025, "R-1", fp="Q4", filed="2026-02-20")], 2026, {"accns": [], "fiscalYears": [2025]}),
    _rows_case("sec.fiscal_year_rows.stated_fy_equal_to_end_year_minus_one_accepted", _P7,
               [_row(*_52W, 2025, "R-1", filed="2026-02-20")], 2025, {"accns": ["R-1"], "fiscalYears": [2025]}),
    # fy 2026 is neither the shifted end year (2025) nor the year before it: the issuer's tag is ignored.
    _rows_case("sec.fiscal_year_rows.stated_fy_outside_window_falls_back_to_end_year", _P7,
               [_row(*_52W, 2026, "R-1", filed="2026-02-20")], 2026, {"accns": [], "fiscalYears": [2025]}),
    # DG: year ended 2026-01-30 states fy 2025 (end year 2026 minus one): the issuer's naming is kept.
    _rows_case("sec.fiscal_year_rows.dg_year_ended_jan_30_is_fiscal_2025", _P7,
               [_row("2025-02-01", "2026-01-30", 2025, "R-DG", filed="2026-03-20")], 2025, {"accns": ["R-DG"], "fiscalYears": [2025]}),
    # The earliest-filed row decides the year; the comparative restated later carries a later fy but stays in 2024.
    _rows_case("sec.fiscal_year_rows.earliest_filing_decides_year_and_latest_filed_row_comes_first", _P7,
               [_row("2024-01-01", "2024-12-31", 2024, "R-OLD", filed="2025-02-20", val=5),
                _row("2024-01-01", "2024-12-31", 2025, "R-NEW", filed="2026-02-20", val=6)], 2024,
               {"accns": ["R-NEW", "R-OLD"], "fiscalYears": [2024]}),
    _rows_case("sec.fiscal_year_rows.fp_is_case_insensitive_and_fy_string_is_numeric", _P7,
               [_row(*_52W, "2025", "R-1", fp="fy", filed="2026-02-20")], 2025, {"accns": ["R-1"], "fiscalYears": [2025]}),
    _rows_case("sec.fiscal_year_rows.fiscal_years_listed_for_a_miss", _P7,
               [_row("2023-01-01", "2023-12-31", 2023, "R-23", filed="2024-02-20"), _row("2024-01-01", "2024-12-31", 2024, "R-24", filed="2025-02-20")],
               2030, {"accns": [], "fiscalYears": [2023, 2024]}),
    _rows_case("sec.fiscal_year_rows.non_string_end_is_skipped", _P7,
               [{"start": "2024-01-01", "end": None, "fy": 2024, "fp": "FY", "accn": "R-X", "filed": "2025-02-20"}], 2024, {"accns": [], "fiscalYears": []}),
    _rows_case("sec.fiscal_year_rows.day_32_is_unparseable_and_skipped", _P7,
               [_row("2024-01-01", "2024-12-32", 2024, "R-X", filed="2025-02-20")], 2024, {"accns": [], "fiscalYears": []}),
    # An invalid day-of-month (31 in a 30-day month) rolls over like JavaScript's Date (card edge case).
    _rows_case("sec.fiscal_year_rows.invalid_day_31_in_30_day_month_rolls_over", _P7,
               [_row("2024-07-01", "2025-06-31", 2025, "R-ROLL", filed="2025-08-20")], 2025, {"accns": ["R-ROLL"], "fiscalYears": [2025]}),
    Case(name="sec.fiscal_year_rows.aapl_fixture_fy2025_revenue", ids=_P7,
         py=lambda: sf.select_fiscal_year_rows(
             _fixture("sec/companyfacts_AAPL.json")["facts"]["us-gaap"]["RevenueFromContractWithCustomerExcludingAssessedTax"]["units"]["USD"], 2025),
         ts={"js": "const fs = await import('node:fs'); const d = JSON.parse(fs.readFileSync(fx.path, 'utf8')); "
                   "return m.secFacts.selectFiscalYearRows(d.facts['us-gaap'].RevenueFromContractWithCustomerExcludingAssessedTax.units.USD, 2025);",
             "fx": {"path": str(FIXTURES / "sec/companyfacts_AAPL.json")}},
         # The selector does not itself restrict to annual periods: AAPL's Q1 FY2026 quarter (ended 2025-12-27, fp Q1) is
         # grouped under calendar 2025 as well. Its callers (server.py:5793, yahoo-finance.ts:6659) pass annual rows only.
         select=lambda v: {"includes2025": 2025 in v["fiscalYears"], "annualFyEnds": sorted({r["end"] for r in v["rows"] if r.get("fp") == "FY"})},
         expect={"includes2025": True, "annualFyEnds": ["2025-09-27"]}),
]

# ═══ BR-SEC-023 / BR-SEC-024: latest annual report missing from SEC companyfacts ══════════════════════
A24, A25 = "0001628280-25-000024", "0001628280-26-025362"


def _cov_row(start, end, val, accn, form, filed, fy=2024):
    return {**({"start": start} if start else {}), "end": end, "val": val, "accn": accn, "fy": fy, "fp": "FY", "form": form, "filed": filed}


def _subs(*rows):
    """rows: (form, accession, filingDate, reportDate)."""
    return {"cik": "1", "filings": {"recent": {"form": [r[0] for r in rows], "accessionNumber": [r[1] for r in rows], "filingDate": [r[2] for r in rows],
                                                 "reportDate": [r[3] for r in rows], "primaryDocument": ["d.htm" for _ in rows]}}}


_TSM = {"facts": {
    "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [_cov_row(None, "2026-03-31", 25932000000, A25, "20-F", "2026-04-16", 2025)]}}},
    "ifrs-full": {"Revenue": {"units": {"TWD": [_cov_row("2024-01-01", "2024-12-31", 2894307699000, A24, "20-F", "2025-04-17")]}}},
}}
_TSM_COVERED = json.loads(json.dumps(_TSM))
_TSM_COVERED["facts"]["ifrs-full"]["Revenue"]["units"]["TWD"].append(_cov_row("2025-01-01", "2025-12-31", 3809054000000, A25, "20-F", "2026-04-16", 2025))
_SUBS = _subs(("6-K", "0001628280-26-030000", "2026-07-16", ""), ("20-F", A25, "2026-04-16", "2025-12-31"), ("20-F", A24, "2025-04-17", "2024-12-31"))
_COV_JS = ("const c = m.companyfactsCoverage; const filings = c.annualFilingsFromSubmissions(args[0]); const latest = c.latestAnnualFiling(filings, args[2]); "
           "return {latest: latest ? latest.accessionNumber : null, inCompanyfacts: latest ? c.accessionInCompanyfacts(args[1], latest.accessionNumber) : null, "
           "warning: c.latestAnnualCoverageWarning(filings, args[1], args[2])};")


def _coverage(name, ids, subs, facts, as_of, expect):
    def py():
        filings = cfc.annual_filings_from_submissions(subs)
        latest = cfc.latest_annual_filing(filings, as_of)
        return {"latest": latest["accessionNumber"] if latest else None,
                "inCompanyfacts": cfc.accession_in_companyfacts(facts, latest["accessionNumber"]) if latest else None,
                "warning": cfc.latest_annual_coverage_warning(filings, facts, as_of)}
    return Case(name=name, ids=ids, py=py, ts={"js": _COV_JS, "args": [subs, facts, as_of]}, expect=expect)


_P23_24 = ["BR-SEC-023", "BR-SEC-024"]
CASES += [
    _coverage("sec.companyfacts_coverage.tsm_fy2025_20f_missing_warns", _P23_24, _SUBS, _TSM, "9999-12-31",
              {"latest": A25, "inCompanyfacts": False, "warning": {
                  "code": "LATEST_ANNUAL_NOT_IN_COMPANYFACTS", "severity": "warning", "form": "20-F", "accessionNumber": A25, "filed": "2026-04-16",
                  "reportDate": "2025-12-31", "latestCompanyfactsAnnualPeriodEnd": "2024-12-31",
                  "message": f"The latest annual report (20-F {A25}, filed 2026-04-16, period 2025-12-31) has no us-gaap or ifrs-full fact in SEC companyfacts; "
                             "the newest annual period companyfacts holds ends 2024-12-31. Figures here come from earlier reports, not from that filing."}}),
    _coverage("sec.companyfacts_coverage.covered_report_gives_no_warning", _P23_24, _SUBS, _TSM_COVERED, "9999-12-31",
              {"latest": A25, "inCompanyfacts": True, "warning": None}),
    # as_of before the FY2025 20-F was filed: FY2024 is the latest annual report and companyfacts holds it.
    _coverage("sec.companyfacts_coverage.as_of_before_filing_uses_prior_report", _P23_24, _SUBS, _TSM, "2026-04-15",
              {"latest": A24, "inCompanyfacts": True, "warning": None}),
    _coverage("sec.companyfacts_coverage.no_annual_report_on_file_gives_none", _P23_24, _subs(("10-Q", "0000000001-26-000002", "2026-05-01", "2026-03-31")),
              _TSM, "9999-12-31", {"latest": None, "inCompanyfacts": None, "warning": None}),
    _coverage("sec.companyfacts_coverage.no_companyfacts_holds_no_annual_period", _P23_24, _SUBS, None, "9999-12-31",
              {"latest": A25, "inCompanyfacts": False, "warning": {
                  "code": "LATEST_ANNUAL_NOT_IN_COMPANYFACTS", "severity": "warning", "form": "20-F", "accessionNumber": A25, "filed": "2026-04-16",
                  "reportDate": "2025-12-31", "latestCompanyfactsAnnualPeriodEnd": None,
                  "message": f"The latest annual report (20-F {A25}, filed 2026-04-16, period 2025-12-31) has no us-gaap or ifrs-full fact in SEC companyfacts; "
                             "companyfacts holds no annual period. Figures here come from earlier reports, not from that filing."}}),
]

# ═══ BR-ERN-002 / BR-ERN-003: guidance amount scaling ═════════════════════════════════════════════
def _amount(text, fallback, expect, name):
    return Case(name=f"ern.parse_amount.{name}", ids=["BR-ERN-002", "BR-ERN-003"], py=lambda: gh.parse_amount(text, fallback),
                ts=_ts("guidanceHistory", "parseAmount", text, fallback), expect=expect)


def _unit_of(text, expect, name):
    return Case(name=f"ern.unit_of.{name}", ids=["BR-ERN-002"], py=lambda: gh.unit_of(text), ts=_ts("guidanceHistory", "unitOf", text), expect=expect)


CASES += [
    _amount("2.0", "billion", {"value": 2_000_000_000, "unit": "billion"}, "unitless_low_takes_other_ends_billion"),
    _amount("2.4 billion", None, {"value": 2_400_000_000, "unit": "billion"}, "billion_word"),
    # Card: '2.05 billion' becomes 2,050,000,000 exactly (not 2049999999.9999998).
    _amount("2.05 billion", None, {"value": 2_050_000_000, "unit": "billion"}, "two_point_oh_five_billion_is_exact"),
    _amount("150.0 million", None, {"value": 150_000_000, "unit": "million"}, "million_word"),
    _amount("1,250 mn", None, {"value": 1_250_000_000, "unit": "mn"}, "mn_abbreviation_with_thousands_separator"),
    _amount("500 k", None, {"value": 500_000, "unit": "k"}, "k_abbreviation_is_thousand"),
    _amount("750 thousand", None, {"value": 750_000, "unit": "thousand"}, "thousand_word"),
    _amount("3 bn", None, {"value": 3_000_000_000, "unit": "bn"}, "bn_abbreviation"),
    _amount("7B", None, {"value": 7_000_000_000, "unit": "b"}, "b_abbreviation_uppercase_is_lowered"),
    _amount("12 m", None, {"value": 12_000_000, "unit": "m"}, "m_abbreviation"),
    _amount("0.0005 k", None, {"value": 1, "unit": "k"}, "rounds_half_up_to_whole_units"),
    _amount("1.0000004 million", None, {"value": 1_000_000, "unit": "million"}, "fraction_below_half_rounds_down"),
    _amount("7.5", None, {"value": 7.5, "unit": None}, "no_unit_keeps_the_raw_number"),
    _amount("2.4 billion", "million", {"value": 2_400_000_000, "unit": "billion"}, "own_unit_beats_fallback"),
    _amount("$2.0 billion", None, {"value": None, "unit": None}, "dollar_sign_not_accepted"),
    _amount("abc", "billion", {"value": None, "unit": None}, "non_numeric_is_none"),
    _unit_of("$2.4 billion", "billion", "billion_word"),
    _unit_of("$150 MM", "m", "mm_reads_as_m"),
    _unit_of("$2.4", None, "no_unit"),
    _unit_of("$2.4 Bn ", "bn", "trailing_space_and_case"),
]


def _entries(text, name, ids, expect, status="READ"):
    release = {"filingDate": "2026-08-10", "accessionNumber": "0001-26-000300", "url": "https://www.sec.gov/r.htm", "status": status, "text": text}
    return Case(name=name, ids=ids, py=lambda: gh.guidance_entries(release), ts=_ts("guidanceHistory", "guidanceEntries", release),
                select=lambda v: [{k: e.get(k) for k in ("metric", "low", "high", "midpoint", "unit", "event")} for e in v], expect=expect)


_E23 = ["BR-ERN-002", "BR-ERN-003"]
_REV = lambda lo, hi: {"metric": "revenue", "low": lo, "high": hi, "midpoint": (lo + hi) / 2, "unit": "USD", "event": None}  # noqa: E731
CASES += [
    # Card: '$2.0 to $2.4 billion' -> 2,000,000,000 / 2,400,000,000, midpoint 2,200,000,000 (the low end takes the high end's unit).
    _entries("The company expects revenue guidance of $2.0 to $2.4 billion for the third quarter of 2026.", "ern.guidance_entries.range_unit_shared_from_high_end",
             _E23, [_REV(2_000_000_000, 2_400_000_000)]),
    _entries("The company expects revenue between $2.05 billion and $2.15 billion in fiscal 2026.", "ern.guidance_entries.two_point_oh_five_billion_exact", _E23,
             [_REV(2_050_000_000, 2_150_000_000)]),
    _entries("The company expects revenue between $950 million and $1.05 billion in fiscal 2026.", "ern.guidance_entries.each_end_keeps_its_own_unit", _E23,
             [_REV(950_000_000, 1_050_000_000)]),
    _entries("Full year 2026 revenue guidance of $150.0 million to $200.0 million.", "ern.guidance_entries.millions_on_both_ends", _E23,
             [_REV(150_000_000, 200_000_000)]),
    _entries("Full year 2026 revenue guidance of $750 thousand to $900 thousand.", "ern.guidance_entries.thousands_on_both_ends", _E23,
             [_REV(750_000, 900_000)]),
    _entries("The company expects EPS of $0.10 to $0.20 for fiscal 2027.", "ern.guidance_entries.eps_is_not_scaled", _E23,
             [{"metric": "eps", "low": 0.1, "high": 0.2, "midpoint": 0.15000000000000002, "unit": "USD/share", "event": None}]),
    _entries("For the first quarter of 2026, the company expects gross margin of 41% to 43%.", "ern.guidance_entries.gross_margin_percent_is_not_scaled", _E23,
             [{"metric": "grossMargin", "low": 41, "high": 43, "midpoint": 42, "unit": "percent", "event": None}]),
    _entries("Results were strong this quarter.", "ern.guidance_entries.no_guidance_text_gives_no_entries", _E23, []),
    _entries("The company expects revenue between $2.0 billion and $2.4 billion.", "ern.guidance_entries.unread_release_gives_no_entries", _E23, [], status="NOT_READ"),
    # BR-ERN-004 (lifecycle): a withdrawal is an event, never a range.
    _entries("Given the uncertainty, the company is withdrawing its full year 2025 guidance.", "ern.guidance_entries.withdrawn_guidance_is_an_event",
             ["BR-ERN-004", "BR-ERN-026", "BR-ERN-027"], [{"metric": "any", "low": None, "high": None, "midpoint": None, "unit": None, "event": "WITHDRAWN"}]),
]

# ═══ BR-ERN-005 (+ 029, 030): reported actuals labelled by period ═════════════════════════════════════
_E5 = ["BR-ERN-005", "BR-ERN-029", "BR-ERN-030"]


def _qrow(start, end, val, form="10-Q", accn="Q-1", filed="2025-08-01", fy=None, fp=None):
    out = {"start": start, "end": end, "val": val, "form": form, "accn": accn, "filed": filed}
    if fy is not None:
        out["fy"], out["fp"] = fy, fp
    return out


def _actuals_fields(v):
    return {"read": v["read"], "calendarFiscalYear": v["calendarFiscalYear"], "fiscalQuarterMapping": v["fiscalQuarterMapping"],
            "revenueLabels": sorted(v["revenue"]), "epsLabels": sorted(v["eps"])}


def _actuals(name, ids, facts, expect, select=_actuals_fields):
    return Case(name=name, ids=ids, py=lambda: gh.actuals_from_company_facts(facts), ts=_ts("guidanceHistory", "actualsFromCompanyFacts", facts),
                select=select, expect=expect)


# December year-end: a 91-day and a 181-day fact both ending 2025-06-30 (card example) plus the 365-day year.
_DEC_FACTS = _facts([
    _qrow("2024-01-01", "2024-12-31", 4_000, "10-K", "K-24", "2025-02-20", 2024, "FY"),
    _qrow("2025-03-31", "2025-06-30", 1_100, "10-Q", "Q-2"),
    _qrow("2024-12-31", "2025-06-30", 2_100, "10-Q", "Q-2"),
])
# Non-calendar (June year end) with no 10-K that states a fiscal year: no quarter labels, FY by the period-end rule.
_JUN_NO_STATED = _facts([
    _qrow("2024-07-01", "2025-06-28", 9_000, "10-K", "K-25", "2025-08-20"),
    _qrow("2025-06-29", "2025-09-27", 2_000, "10-Q", "Q-1", "2025-11-01"),
])
# DG-like: the 10-K for the year ended 2026-01-30 states fy 2025; the quarter ended 2026-05-01 is after the latest annual end.
_DG_ACTUALS = _facts(DG_KS + [_qrow("2026-02-01", "2026-05-01", 7_000, "10-Q", "Q-DG-1", "2026-06-05")])
_DG_NO_Q = _facts([_row("2025-02-01", "2026-01-30", None, "K-NO-FY", filed="2026-03-20") | {"fy": "x"}, _qrow("2026-02-01", "2026-05-01", 7_000, "10-Q", "Q-DG-1", "2026-06-05")])
CASES += [
    _actuals("ern.actuals.calendar_filer_labels_q2_h1_and_fy", _E5, _DEC_FACTS,
             {"read": True, "calendarFiscalYear": True, "fiscalQuarterMapping": "CALENDAR", "revenueLabels": ["FY2024", "H1 2025", "Q2 2025"], "epsLabels": []}),
    _actuals("ern.actuals.non_calendar_without_stated_year_has_no_quarter_labels", _E5, _JUN_NO_STATED,
             {"read": True, "calendarFiscalYear": False, "fiscalQuarterMapping": "NONE", "revenueLabels": ["FY2025"], "epsLabels": []}),
    _actuals("ern.actuals.quarter_after_latest_annual_end_uses_next_fy_when_10k_states_fy", _E5, _DG_ACTUALS,
             {"read": True, "calendarFiscalYear": False, "fiscalQuarterMapping": "FILING_STATED_FISCAL_YEAR",
              "revenueLabels": ["FY2023", "FY2024", "FY2025", "Q1 2026"], "epsLabels": []}),
    _actuals("ern.actuals.companyfacts_not_read", _E5, None,
             {"read": False, "calendarFiscalYear": False, "fiscalQuarterMapping": "NONE", "revenueLabels": [], "epsLabels": []}),
    # Second halves are never derived, and a first half is only labelled for a calendar year (H1 needs June nominal end).
    _actuals("ern.actuals.second_half_is_never_derived", _E5,
             _facts([_qrow("2024-01-01", "2024-12-31", 4_000, "10-K", "K-24", "2025-02-20", 2024, "FY"),
                     _qrow("2024-07-01", "2024-12-31", 2_000, "10-K", "K-24", "2025-02-20", 2024, "FY")]),
             {"read": True, "calendarFiscalYear": True, "fiscalQuarterMapping": "CALENDAR", "revenueLabels": ["FY2024"], "epsLabels": []}),
    # A 52-week year ending 2027-01-02 is FY2026 by the period-end rule (7-day drift) when no 10-K states its year.
    _actuals("ern.actuals.fy_label_uses_week_drift_rule_when_no_stated_year", _E5,
             _facts([_qrow("2026-01-04", "2027-01-02", 8_000, "10-K", "K-W", "2027-02-20")]),
             {"read": True, "calendarFiscalYear": True, "fiscalQuarterMapping": "CALENDAR", "revenueLabels": ["FY2026"], "epsLabels": []}),
    # The 10-K's stated fiscal year wins over the period-end rule (DG: year ended 2026-01-30 is fiscal 2025).
    _actuals("ern.actuals.fy_label_prefers_the_year_the_10k_states", _E5, _facts(DG_KS),
             {"read": True, "calendarFiscalYear": False, "fiscalQuarterMapping": "FILING_STATED_FISCAL_YEAR", "revenueLabels": ["FY2023", "FY2024", "FY2025"], "epsLabels": []}),
    # A non-10 form is ignored (8-K), and an amended filing of the same period wins by filed date.
    _actuals("ern.actuals.newest_filing_of_a_period_wins", _E5,
             _facts([_qrow("2025-03-31", "2025-06-30", 1_100, "10-Q", "Q-2", "2025-08-01"), _qrow("2025-03-31", "2025-06-30", 1_150, "10-Q/A", "Q-2A", "2025-09-01"),
                     _qrow("2025-03-31", "2025-06-30", 9_999, "8-K", "Q-8K", "2025-10-01"),
                     _qrow("2024-12-31", "2024-12-31", 1, "10-K", "K", "2025-02-20", 2024, "FY") | {"start": "2024-01-01"}]),
             {"revenue": {"Q2 2025": 1_150, "FY2024": 1}, "eps": {}},
             select=lambda v: {"revenue": {k: r["value"] for k, r in v["revenue"].items()}, "eps": {k: r["value"] for k, r in v["eps"].items()}}),
    # Recorded companyfacts (AAPL 52/53-week September year end).
    Case(name="ern.actuals.aapl_fixture_labels_fiscal_quarters_from_stated_year", ids=_E5, py=lambda: gh.actuals_from_company_facts(_fixture("sec/companyfacts_AAPL.json")),
         ts=_file_ts("guidanceHistory", "actualsFromCompanyFacts", "sec/companyfacts_AAPL.json"),
         select=lambda v: {"mapping": v["fiscalQuarterMapping"], "fy2025": v["revenue"]["FY2025"]["value"], "fy2025End": v["revenue"]["FY2025"]["periodEnd"],
                           "q1_2026": v["revenue"]["Q1 2026"]["value"], "q1_2026End": v["revenue"]["Q1 2026"]["periodEnd"], "eps_fy2025": v["eps"]["FY2025"]["value"],
                           "q4_2025_derived": "Q4 2025" in v["revenue"]},
         expect={"mapping": "FILING_STATED_FISCAL_YEAR", "fy2025": 416_161_000_000, "fy2025End": "2025-09-27", "q1_2026": 143_756_000_000,
                 "q1_2026End": "2025-12-27", "eps_fy2025": 7.46, "q4_2025_derived": False}),
    # Finding (Phase 2, for Q3i triage): COHR's 10-K for the year ended 2021-06-30 carries SEC fy 2020 (a mistagged
    # companyfacts year). Both runtimes label it FY2020, so it overwrites the real FY2020 (ended 2020-06-30) and there
    # is no FY2021 label at all. The card says "labelled FY by the fiscal year the 10-K states", so this is the rule
    # working on bad source data; recorded as current behaviour. See yfmcp/guidance_history.py:361-366 and
    # worker/src/guidance-history.ts (statedFy), and the fiscal-year naming offset the COHR fixture exercises.
    Case(name="ern.actuals.cohr_fixture_mistagged_fy_overwrites_prior_year", ids=_E5, py=lambda: gh.actuals_from_company_facts(_fixture("sec/companyfacts_COHR.json")),
         ts=_file_ts("guidanceHistory", "actualsFromCompanyFacts", "sec/companyfacts_COHR.json"),
         select=lambda v: {"fy2020End": v["revenue"]["FY2020"]["periodEnd"], "fy2021": "FY2021" in v["revenue"], "fy2026": v["revenue"]["FY2026"]["value"],
                           "fy2026End": v["revenue"]["FY2026"]["periodEnd"], "q3_2026": v["revenue"]["Q3 2026"]["value"]},
         expect={"fy2020End": "2021-06-30", "fy2021": False, "fy2026": 7_118_181_000, "fy2026End": "2026-06-30", "q3_2026": 1_805_641_000}),
]
