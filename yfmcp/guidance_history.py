"""Guidance history (2.5.3), mirroring worker/src/guidance-history.ts.

Parity is tested in scripts/test_guidance_and_drivers.py. Pure.

Every guidance range is read from an earnings-release exhibit with its
target period and source; revisions compare consecutive releases for the
same metric and target period; outcomes compare the company's later
reported actual (XBRL) with the first and last range. Nothing is inferred:
a range whose target period the release does not state is kept but not
compared, and an actual that cannot be matched to the period is not
evaluated.
"""

from __future__ import annotations

import datetime as _dt
import math
import re
from typing import Any

from yfmcp.evidence import AUTHORITY_BOUNDARY
from yfmcp.extraction_rules import guidance_ranges
from yfmcp.fiscal_calendar import TEXT_DATE_SOURCE, fiscal_quarter_of, fiscal_year_naming, fiscal_year_of_period_end, nominal_period_end, text_date
from yfmcp.sec_facts import REVENUE_CONCEPTS

_F = re.I | re.A

_SCALE = {"billion": 1e9, "bn": 1e9, "b": 1e9, "million": 1e6, "mn": 1e6, "m": 1e6, "thousand": 1e3, "k": 1e3}
_AMOUNT_RE = re.compile(r"\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(billion|million|thousand|bn|mn|b|m|k)?\s*", _F)
_UNIT_RE = re.compile(r"(billion|million|thousand|bn|mn|b|m|k)\s*$", _F)


def parse_amount(text: str, fallback_unit: str | None = None) -> dict:
    """"150.0 million" -> 150000000; the scale of the other end of a range applies when this end has none."""
    m = _AMOUNT_RE.fullmatch(text)
    if not m:
        return {"value": None, "unit": None}
    unit = (m.group(2) or fallback_unit or "").lower() or None
    base = float(m.group(1).replace(",", ""))
    # Whole units once scaled: 2.05 billion is 2050000000, not 2049999999.9999998.
    return {"value": math.floor(base * _SCALE.get(unit, 1) + 0.5) if unit else base, "unit": unit}


def unit_of(text: str) -> str | None:
    m = _UNIT_RE.search(text.strip())
    return m.group(1).lower() if m else None


_ORDINAL = {"first": 1, "second": 2, "third": 3, "fourth": 4}
_FY_RE = re.compile(r"\b(?:full[- ]year|fiscal(?: year)?|FY)\s*'?(20\d\d|\d\d)\b", _F)
_YEAR_FIRST_RE = re.compile(r"\b(20\d\d)\s+(?:full[- ]year|annual)\b", _F)
_ORDINAL_QUARTER_RE = re.compile(r"\b(first|second|third|fourth) quarter(?: of)?(?: fiscal)?(?: year)?\s*(20\d\d)?", _F)
_Q_RE = re.compile(r"\bQ([1-4])\s*(?:of\s+)?(?:FY)?\s*'?(20\d\d)?\b", re.A)
_HALF_WORD_RE = re.compile(r"\b(first|second)[- ]half(?: of)?(?: fiscal)?(?: year)?\s*'?(20\d\d)?\b", _F)
_HALF_LEAD_RE = re.compile(r"\b([12])H\s?'?(20\d\d|\d\d)\b", re.A)
_HALF_TRAIL_RE = re.compile(r"\bH([12])\s*'?(20\d\d|\d\d)?\b", re.A)
# "the fiscal year ending June 25, 2027" and "... quarter ended October 31, 2026" (2.5.11).
_FISCAL_YEAR_ENDING_RE = re.compile(r"\bfiscal year (?:ending|ended|that (?:ends|ended)|which (?:ends|ended))(?: on)?\s+" + TEXT_DATE_SOURCE + r"\b", _F)
_PERIOD_END_AFTER_RE = re.compile(r"\s*,?\s*(?:ending|ended)(?: on)?\s+" + TEXT_DATE_SOURCE + r"\b", _F)


def _year4(text: str | None) -> int | None:
    if not text:
        return None
    n = int(text)
    return 2000 + n if len(text) == 2 else n


def guidance_target_period(context: str, anchor: int | None = None) -> dict:
    """The target period a context names for the guidance ending at anchor.

    The closest named before the anchor, else the first after it. Quarters
    and halves ("first quarter of fiscal 2026", "second-half 2025", "2H25")
    win over the fiscal year inside them.
    """
    anchor = len(context) if anchor is None else anchor
    found: list[dict] = []

    def add(m: re.Match, fiscal_year: int | None, quarter: int | None, half: int | None) -> None:
        # "fiscal 2027 ending June 25, 2027", "third quarter ended October 31, 2026": the stated end date (2.5.11).
        d = _PERIOD_END_AFTER_RE.match(context[m.end():m.end() + 60])
        found.append({"index": m.start(), "end": m.end(), "fiscalYear": fiscal_year, "quarter": quarter, "half": half,
                      "periodEnd": text_date(d.group(1), d.group(2), d.group(3)) if d else None, "fromDate": False})

    # "the fiscal year ending June 25, 2027" names a 52/53-week year by its end date only: its year is the
    # fiscal year of that date (fiscal_calendar.py) (2.5.11, AEHR).
    for m in _FISCAL_YEAR_ENDING_RE.finditer(context):
        period_end = text_date(m.group(1), m.group(2), m.group(3))
        if period_end:
            found.append({"index": m.start(), "end": m.end(), "fiscalYear": fiscal_year_of_period_end(period_end), "quarter": None,
                          "half": None, "periodEnd": period_end, "fromDate": True})
    for m in _FY_RE.finditer(context):
        add(m, _year4(m.group(1)), None, None)
    for m in _YEAR_FIRST_RE.finditer(context):
        add(m, _year4(m.group(1)), None, None)
    for m in _ORDINAL_QUARTER_RE.finditer(context):
        add(m, _year4(m.group(2)), _ORDINAL[m.group(1).lower()], None)
    for m in _Q_RE.finditer(context):
        add(m, _year4(m.group(2)), int(m.group(1)), None)
    for m in _HALF_WORD_RE.finditer(context):
        add(m, _year4(m.group(2)), None, _ORDINAL[m.group(1).lower()])
    for m in _HALF_LEAD_RE.finditer(context):
        add(m, _year4(m.group(2)), None, int(m.group(1)))
    for m in _HALF_TRAIL_RE.finditer(context):
        add(m, _year4(m.group(2)), None, int(m.group(1)))
    parts = [h for h in found if h["quarter"] is not None or h["half"] is not None]
    hits = [h for h in found if h["quarter"] is not None or h["half"] is not None
            or not any(q["index"] <= h["index"] < q["end"] for q in parts)]
    if not hits:
        return {"label": None, "fiscalYear": None, "quarter": None, "half": None, "periodEnd": None, "basis": "NOT_STATED"}
    before = [h for h in hits if h["index"] < anchor]
    if before:
        best = before[0]
        for h in before[1:]:
            if h["index"] >= best["index"]:
                best = h
    else:
        best = hits[0]
        for h in hits[1:]:
            if h["index"] < best["index"]:
                best = h
    year = f" {best['fiscalYear']}" if best["fiscalYear"] is not None else ""
    if best["quarter"] is not None:
        label = f"Q{best['quarter']}{year}"
    elif best["half"] is not None:
        label = f"H{best['half']}{year}"
    else:
        label = f"FY{best['fiscalYear']}"
    return {
        "label": label,
        "fiscalYear": best["fiscalYear"],
        "quarter": best["quarter"],
        "half": best["half"],
        "periodEnd": best["periodEnd"],
        "basis": "TEXT_YEAR_NOT_STATED" if best["fiscalYear"] is None else "TEXT_PERIOD_END" if best["fromDate"] else "TEXT",
    }


# Sentence and bullet boundaries, including the " o " bullets SEC-rendered releases carry.
_WS = "[\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]"
_BOUNDARY_RE = re.compile(f"[.!?]{_WS}|{_WS}[\u2022\u25cf\u25aa\u25e6\u00b7]{_WS}|{_WS}o{_WS}(?=[A-Z])")


def sentence_bounds(text: str, at: int, length: int) -> tuple[int, int]:
    """[start, end) of the sentence or bullet around an excerpt at [at, at + length)."""
    window_start = max(0, at - 300)
    start = window_start
    for m in _BOUNDARY_RE.finditer(text[window_start:at]):
        start = window_start + m.end()
    tail = text[at + length: at + length + 200]
    m = _BOUNDARY_RE.search(tail)
    return start, at + length + (m.start() if m else len(tail))


def period_for_excerpt(text: str, at: int, length: int) -> dict:
    """The period for an excerpt at [at, at + length).

    From its own sentence first ("expects EPS of $0.10 to $0.20 for fiscal
    2027"), else from the 200 characters before it (a heading or lead-in).
    scope says which.
    """
    start, end = sentence_bounds(text, at, length)
    in_sentence = guidance_target_period(text[start:end], at + length - start)
    if in_sentence["basis"] != "NOT_STATED":
        return {**in_sentence, "scope": "SENTENCE"}
    # The nearest guidance or outlook heading above, read whole: a fixed look-back can cut "third
    # quarter of fiscal 2027" to "fiscal 2027" (NVDA), and bullets can sit far below it (MRVL) (2.5.10).
    # Nearest mention first; one that names no period ("... in its outlook") gives way to the next.
    base = max(0, at - _HEADING_LOOKBACK)
    for heading in reversed([base + m.start() for m in _HEADING_WORD_RE.finditer(text[base:at])]):
        start_h = max(0, heading - 100)
        from_heading = guidance_target_period(text[start_h:min(at, heading + 100)], heading - start_h)
        if from_heading["basis"] != "NOT_STATED":
            return {**from_heading, "scope": "GUIDANCE_MENTION"}
    preceding = guidance_target_period(text[max(0, at - 200): at + length])
    return {**preceding, "scope": None if preceding["basis"] == "NOT_STATED" else "PRECEDING_TEXT"}


_HEADING_LOOKBACK = 1500
_HEADING_WORD_RE = re.compile(r"\b(?:outlook|guidance)\b", _F)


_WITHDRAWN_RE = re.compile(
    r"\bwithdr[ae]w(?:s|n|ing)?\b[^.]{0,60}\b(?:guidance|outlook)\b|\b(?:guidance|outlook)\b[^.]{0,60}\bwithdrawn\b"
    r"|\bsuspend(?:s|ed|ing)?\b[^.]{0,40}\b(?:guidance|outlook)\b",
    _F,
)
_REAFFIRM_RE = re.compile(
    r"\breaffirm(?:s|ed|ing)?\b|\breiterat(?:e|es|ed|ing)\b|\bmaintain(?:s|ed|ing)?\b[^.]{0,30}\b(?:guidance|outlook)\b", _F
)


def _number(text: str) -> float | None:
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def guidance_entries(release: dict) -> list[dict]:
    """Guidance ranges stated in one release, each with its target period."""
    text = release.get("text")
    if release.get("status") != "READ" or not text:
        return []
    ranges = guidance_ranges(text, lambda at, n: period_for_excerpt(text, at, n).get("label"))
    out: list[dict] = []
    for metric in ("revenue", "grossMargin", "eps"):
        r = ranges.get(metric)
        if not r:
            continue
        at = text.find(r["excerpt"])
        s_start, s_end = sentence_bounds(text, at, len(r["excerpt"]))
        sentence = text[s_start:s_end]
        if metric == "revenue":
            fallback = unit_of(r["high"]) or unit_of(r["low"])
            low = parse_amount(r["low"], fallback)["value"]
            high = parse_amount(r["high"], fallback)["value"]
            unit = "USD"
        else:
            low = _number(r["low"])
            high = _number(r["high"])
            unit = "USD/share" if metric == "eps" else "percent"
        if low is None or high is None or not math.isfinite(low) or not math.isfinite(high):
            continue
        out.append({
            "metric": metric,
            "targetPeriod": period_for_excerpt(text, at, len(r["excerpt"])),
            "low": low,
            "high": high,
            "midpoint": (low + high) / 2,
            "unit": unit,
            "basis": r["basis"],
            "statedAction": "REAFFIRMED_IN_TEXT" if _REAFFIRM_RE.search(sentence) else None,
            "releaseDate": release.get("filingDate"),
            "accessionNumber": release.get("accessionNumber"),
            "sourceUrl": release.get("url"),
            "excerpt": r["excerpt"][:300],
        })
    withdrawn = _WITHDRAWN_RE.search(text)
    if withdrawn:
        at = withdrawn.start()
        out.append({
            "metric": "any",
            "targetPeriod": period_for_excerpt(text, at, len(withdrawn.group(0))),
            "event": "WITHDRAWN",
            "releaseDate": release.get("filingDate"),
            "accessionNumber": release.get("accessionNumber"),
            "sourceUrl": release.get("url"),
            "excerpt": withdrawn.group(0)[:300],
        })
    return out


def _eq(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-9 * max(1, abs(a))


def _compare(prev: dict, cur: dict) -> str:
    if _eq(prev["low"], cur["low"]) and _eq(prev["high"], cur["high"]):
        return "REAFFIRMED"
    pm, cm = prev["midpoint"], cur["midpoint"]
    if not _eq(pm, cm):
        return "RAISED" if cm > pm else "LOWERED"
    return "NARROWED" if (cur["high"] - cur["low"]) < (prev["high"] - prev["low"]) else "WIDENED"


def _groups(entries: list[dict]) -> dict[tuple[str, str], list[dict]]:
    groups: dict[tuple[str, str], list[dict]] = {}
    for e in entries:
        tp = e["targetPeriod"]
        if e.get("event") or tp.get("label") is None or tp.get("fiscalYear") is None:
            continue
        groups.setdefault((e["metric"], tp["label"]), []).append(e)
    return groups


def guidance_revisions(entries: list[dict]) -> list[dict]:
    """Changes between consecutive releases for the same metric and target period."""
    out: list[dict] = []
    for (metric, label), items in _groups(entries).items():
        ordered = sorted(items, key=lambda e: str(e["releaseDate"]))
        for i, cur in enumerate(ordered):
            prev = ordered[i - 1] if i > 0 else None
            out.append({
                "metric": metric,
                "targetPeriod": label,
                "releaseDate": cur["releaseDate"],
                "change": _compare(prev, cur) if prev else "INITIATED",
                "from": {"low": prev["low"], "high": prev["high"], "releaseDate": prev["releaseDate"]} if prev else None,
                "to": {"low": cur["low"], "high": cur["high"]},
                "accessionNumber": cur["accessionNumber"],
            })
    for e in entries:
        if e.get("event") != "WITHDRAWN":
            continue
        out.append({"metric": "any", "targetPeriod": e["targetPeriod"]["label"], "releaseDate": e["releaseDate"], "change": "WITHDRAWN",
                    "from": None, "to": None, "accessionNumber": e["accessionNumber"]})
    return sorted(out, key=lambda r: (str(r["releaseDate"]), str(r["metric"])))


def _days(start: str, end: str) -> int:
    return (_dt.date.fromisoformat(end[:10]) - _dt.date.fromisoformat(start[:10])).days


def actuals_from_company_facts(companyfacts: Any) -> dict:
    """Reported actuals by period label from companyfacts.

    FY<fiscal year> for ~1-year durations: the fiscal year the annual report
    states (companyfacts fy of the 10-K whose own year it is), else the fiscal
    year of the period end (fiscal_calendar.py). Quarters: calendar quarters
    when every annual period ends in December (a 52/53-week year ending in the
    first week of January counts); otherwise fiscal quarters of a year whose
    annual report states its fiscal year, counted back from that year's end.
    The newest filing of each period wins.
    """
    facts = (companyfacts or {}).get("facts") if isinstance(companyfacts, dict) else None
    usgaap = (facts or {}).get("us-gaap") or {}

    def raw(concepts: list[str], unit: str) -> list[dict]:
        out: list[dict] = []
        for concept in concepts:
            units = (usgaap.get(concept) or {}).get("units") or {}
            for f in units.get(unit) or []:
                val = f.get("val")
                if not isinstance(f.get("start"), str) or not isinstance(f.get("end"), str):
                    continue
                if isinstance(val, bool) or not isinstance(val, (int, float)):
                    continue
                if not re.match(r"10-[KQ]", str(f.get("form") or "")):
                    continue
                out.append({**f, "concept": concept})
        return out

    def collect(items: list[dict]) -> list[dict]:
        by_period: dict[str, dict] = {}
        for f in items:
            key = f"{f['start']}|{f['end']}"
            prev = by_period.get(key)
            if prev is None or str(f.get("filed") or "") > str(prev.get("filed") or ""):
                by_period[key] = f
        return list(by_period.values())

    raw_revenue = raw(list(REVENUE_CONCEPTS), "USD")
    raw_eps = raw(["EarningsPerShareDiluted"], "USD/shares")
    revenue = collect(raw_revenue)
    eps = collect(raw_eps)

    def is_annual(f: dict) -> bool:
        return 350 <= _days(f["start"], f["end"]) <= 380

    # The fiscal year an annual report states for its own year: the fy of the 10-K's latest annual period (2.5.11).
    newest_by_accession: dict[str, dict] = {}
    for f in raw_revenue + raw_eps:
        fy = f.get("fy")
        if (not is_annual(f) or not str(f.get("form") or "").startswith("10-K") or f.get("fp") != "FY"
                or isinstance(fy, bool) or not isinstance(fy, int) or not isinstance(f.get("accn"), str)):
            continue
        prev = newest_by_accession.get(f["accn"])
        if prev is None or str(f["end"]) > str(prev["end"]):
            newest_by_accession[f["accn"]] = f
    stated_fy: dict[str, int] = {}
    for f in sorted(newest_by_accession.values(), key=lambda x: str(x.get("filed") or "")):
        stated_fy[str(f["end"])] = f["fy"]
    annual_ends = sorted({str(f["end"]) for f in revenue + eps if is_annual(f)})
    calendar_fy = bool(annual_ends) and all((nominal_period_end(e) or "")[5:7] == "12" for e in annual_ends)
    mapping = "CALENDAR" if calendar_fy else "FILING_STATED_FISCAL_YEAR" if stated_fy else "NONE"

    def fiscal_year(end: str) -> int | None:
        return stated_fy.get(end, fiscal_year_of_period_end(end))

    # The fiscal year a non-calendar quarter falls in: the first annual end on or after it, else (the year in
    # progress) a year after the latest, only when an annual report states that year's number.
    def fiscal_quarter(end: str) -> str | None:
        year_end = next((a for a in annual_ends if -7 <= _days(end, a) <= 280), None)
        if year_end:
            fy = stated_fy.get(year_end)
            q = fiscal_quarter_of(end, year_end)
            return f"Q{q} {fy}" if fy is not None and q is not None else None
        latest = annual_ends[-1] if annual_ends else None
        fy = stated_fy.get(latest) if latest else None
        if not latest or fy is None or end <= latest:
            return None
        projected = (_dt.date.fromisoformat(latest) + _dt.timedelta(days=364)).isoformat()
        q = fiscal_quarter_of(end, projected)
        return f"Q{q} {fy + 1}" if q is not None else None

    def label(f: dict) -> str | None:
        d = _days(f["start"], f["end"])
        end = f["end"]
        nominal = nominal_period_end(end) or end
        if 350 <= d <= 380:
            return f"FY{fiscal_year(end)}"
        if 80 <= d <= 100:
            if calendar_fy:
                return f"Q{math.ceil(int(nominal[5:7]) / 3)} {nominal[:4]}"
            return fiscal_quarter(end) if mapping == "FILING_STATED_FISCAL_YEAR" else None
        # A first half is filed as the six-month year-to-date period; a second half is never filed as a period.
        if 170 <= d <= 190 and calendar_fy and nominal[5:7] == "06":
            return f"H1 {nominal[:4]}"
        return None

    def table(items: list[dict]) -> dict:
        out: dict = {}
        for f in items:
            lab = label(f)
            if lab:
                out[lab] = {"value": f["val"], "concept": f["concept"], "periodStart": f["start"], "periodEnd": f["end"],
                            "form": f.get("form"), "filed": f.get("filed"), "accessionNumber": f.get("accn")}
        return out

    # An unread companyfacts is not an unreported actual.
    return {"read": companyfacts is not None, "revenue": table(revenue), "eps": table(eps), "calendarFiscalYear": calendar_fy,
            "fiscalQuarterMapping": mapping, "statedFiscalYears": dict(sorted(stated_fy.items()))}


def guidance_outcomes(entries: list[dict], actuals: dict) -> list[dict]:
    """First and last guidance for each metric and target period against the reported actual."""
    out: list[dict] = []
    for (metric, label), items in _groups(entries).items():
        ordered = sorted(items, key=lambda e: str(e["releaseDate"]))
        first, last = ordered[0], ordered[-1]
        table = actuals.get(metric)
        actual = table.get(label) if table else None

        def position(g: dict, actual: dict | None = actual) -> str | None:
            if not actual or g.get("basis") == "NON_GAAP":
                return None
            v = actual["value"]
            return "BELOW" if v < g["low"] else "ABOVE" if v > g["high"] else "WITHIN"

        # Reported actuals are GAAP: a non-GAAP range is never scored against them (2.5.9).
        non_gaap = any(g.get("basis") == "NON_GAAP" for g in ordered)
        status = "EVALUATED"
        if actuals.get("read") is False and metric != "grossMargin":
            status = "ACTUALS_NOT_READ"
        # An empty table is a metric read with no actual yet, as in the Worker; only a missing one is not evaluated (2.5.13).
        elif table is None:
            status = "NOT_EVALUATED_METRIC"
        elif non_gaap:
            status = "NOT_EVALUATED_NON_GAAP_BASIS"
        elif not actual:
            if label.startswith("H") and (label.startswith("H2") or actuals.get("calendarFiscalYear") is not True):
                status = "NOT_EVALUATED_HALF_YEAR"
            elif label.startswith("Q") and (actuals.get("fiscalQuarterMapping") or "NONE") == "NONE":
                status = "NOT_EVALUATED_FISCAL_QUARTER_MAPPING"
            else:
                status = "ACTUAL_NOT_YET_REPORTED"
        out.append({
            "metric": metric,
            "targetPeriod": label,
            "status": status,
            "actual": actual,
            "initialGuidance": {"low": first["low"], "high": first["high"], "releaseDate": first["releaseDate"]},
            "lastGuidance": {"low": last["low"], "high": last["high"], "releaseDate": last["releaseDate"]},
            "basis": last.get("basis") or "NOT_STATED",
            "positionVsInitial": position(first),
            "positionVsLast": position(last),
        })
    return sorted(out, key=lambda r: (str(r["targetPeriod"]), str(r["metric"])))


def _with_naming(e: dict, naming: dict) -> dict:
    """A year named only by its end date takes the company's own naming (DG's year ending January 2027 is its fiscal 2026) (2.5.13)."""
    tp = e.get("targetPeriod")
    if not isinstance(tp, dict) or tp.get("basis") != "TEXT_PERIOD_END" or naming["offset"] == 0 or not isinstance(tp.get("fiscalYear"), int):
        return e
    fiscal_year = tp["fiscalYear"] + naming["offset"]
    return {**e, "targetPeriod": {**tp, "fiscalYear": fiscal_year, "label": f"FY{fiscal_year}", "namingBasis": naming["basis"]}}


def guidance_history(ticker: str, releases: list[dict], companyfacts: Any) -> dict:
    naming = fiscal_year_naming(companyfacts)
    entries = [_with_naming(e, naming) for r in releases for e in guidance_entries(r)]
    actuals = actuals_from_company_facts(companyfacts)
    return {
        "ticker": ticker.upper(),
        "basis": "COMPANY_DISCLOSED",
        "releases": [
            {
                "filingDate": r.get("filingDate"),
                "accessionNumber": r.get("accessionNumber"),
                "url": r.get("url"),
                "status": r.get("status"),
                "guidanceFound": len([e for e in guidance_entries(r) if not e.get("event")]),
            }
            for r in releases
        ],
        "guidance": [e for e in entries if not e.get("event")],
        "withdrawals": [e for e in entries if e.get("event") == "WITHDRAWN"],
        "revisions": guidance_revisions(entries),
        "outcomes": guidance_outcomes(entries, actuals),
        "actualsBasis": {
            "revenue": "SEC XBRL revenue concepts, newest filing per period",
            "eps": "us-gaap:EarningsPerShareDiluted, newest filing per period",
            "grossMargin": "not evaluated",
            "periodLabels": ("FY<the fiscal year the annual report states, else the year of the period end less a week (a 52/53-week year"
                             " ending in early January is the prior year's)>; calendar quarters and first halves for calendar fiscal years;"
                             " fiscal quarters counted back from a year end whose fiscal year an annual report states; second halves are"
                             " not filed as a period and are not derived"),
            "calendarFiscalYear": actuals["calendarFiscalYear"],
            "fiscalQuarterMapping": actuals["fiscalQuarterMapping"],
            "read": actuals["read"],
        },
        "notes": [
            "Guidance is read from earnings-release exhibits as written; a range whose target period the release does not state is listed but not compared.",
            "Revisions compare consecutive releases for the same metric and target period by midpoint, then width.",
            "Outcomes compare the reported actual with the first and last guidance; nothing is estimated.",
            "guidanceFound counts ranges stated in text; guidance given only in a table is not read, and a release with none found is not a withdrawal.",
        ],
        **AUTHORITY_BOUNDARY,
    }
