"""Metric source reconciliation (2.5.4), mirroring worker/src/metric-reconciliation.ts.

Parity is tested in scripts/test_valuation_history_and_reconcile.py. Pure.

One metric for one period, as each source states it:
- SEC XBRL as first filed and as most recently filed for that period (a
  difference is a restatement, reported as such);
- the issuer's earnings release, read only from sentences that name the
  metric, a dollar amount and the period's scope (quarter or full year);
- Yahoo's quarterly or annual statement row for the same period end.
Each found value is compared with the latest SEC value (the baseline) with
its difference and percentage; a release figure's stated precision widens
the tolerance ("$31.5 million" is +/- $50,000). Nothing is averaged or
chosen: the comparison is the output.
"""

from __future__ import annotations

import datetime as _dt
import math
import re
from typing import Any

from yfmcp.capital_structure import _collapse
from yfmcp.capital_structure import round_half_up as _round
from yfmcp.evidence import AUTHORITY_BOUNDARY
from yfmcp.sec_facts import REVENUE_CONCEPTS

METRICS: dict[str, dict] = {
    "revenue": {"kind": "duration", "unit": "money", "gaap": list(REVENUE_CONCEPTS), "ifrs": ["Revenue", "RevenueFromContractsWithCustomers"],
                "yahoo": ["totalRevenue"], "releaseLabel": r"\b(?:net sales|revenues?)\b"},
    "net_income": {"kind": "duration", "unit": "money", "gaap": ["NetIncomeLoss", "ProfitLoss"], "ifrs": ["ProfitLossAttributableToOwnersOfParent", "ProfitLoss"],
                   "yahoo": ["netIncome", "netIncomeCommonStockholders"], "releaseLabel": r"\bnet (?:income|loss)\b"},
    "operating_income": {"kind": "duration", "unit": "money", "gaap": ["OperatingIncomeLoss"], "ifrs": ["ProfitLossFromOperatingActivities"],
                         "yahoo": ["operatingIncome"], "releaseLabel": r"\b(?:operating (?:income|loss)|(?:income|loss) from operations)\b"},
    "eps_diluted": {"kind": "duration", "unit": "per_share", "gaap": ["EarningsPerShareDiluted"], "ifrs": ["DilutedEarningsLossPerShare"],
                    "yahoo": ["dilutedEPS"],
                    "releaseLabel": r"\b(?:diluted (?:earnings|loss|net loss|net income) per share|diluted eps|(?:earnings|loss|net loss|net income) per diluted share)\b"},
    "cash_and_equivalents": {"kind": "instant", "unit": "money", "gaap": ["CashAndCashEquivalentsAtCarryingValue"], "ifrs": ["CashAndCashEquivalents"],
                             "yahoo": ["cashAndCashEquivalents"],
                             # "cash, cash equivalents, and restricted cash" (ASTS) is a different total.
                             "releaseLabel": r"\bcash,? (?:and )?cash equivalents\b(?!,? (?:and )?(?:restricted cash|short-term investments|marketable securities|investments))"},
}

DEFAULT_TOLERANCE_PCT = 0.5
_PERIODIC_FORM_RE = re.compile(r"(?:10-K|10-Q|20-F|40-F)")
_F = re.I | re.A


def _days(start: str, end: str) -> int:
    return (_dt.date.fromisoformat(end[:10]) - _dt.date.fromisoformat(start[:10])).days


def _num(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def metric_facts(companyfacts: Any, metric: str) -> tuple[str | None, str | None, list[dict]]:
    """The taxonomy, reporting unit and facts for a metric (all periodic filings, every filing kept)."""
    spec = METRICS[metric]
    facts = companyfacts.get("facts") if isinstance(companyfacts, dict) else None
    for taxonomy, concepts in (("us-gaap", spec["gaap"]), ("ifrs-full", spec["ifrs"])):
        tax = (facts or {}).get(taxonomy)
        if not tax:
            continue
        by_unit: dict[str, list[dict]] = {}
        for concept in concepts:
            for unit, rows in ((tax.get(concept) or {}).get("units") or {}).items():
                ok = re.fullmatch(r"[A-Z]{3}/shares", unit) if spec["unit"] == "per_share" else re.fullmatch(r"[A-Z]{3}", unit)
                if not ok:
                    continue
                for f in rows:
                    if (not isinstance(f.get("end"), str) or not _num(f.get("val")) or not isinstance(f.get("filed"), str)
                            or not _PERIODIC_FORM_RE.match(str(f.get("form") or ""))):
                        continue
                    start = f.get("start") if isinstance(f.get("start"), str) else None
                    if (spec["kind"] == "instant") != (start is None):
                        continue
                    by_unit.setdefault(unit, []).append({"concept": concept, "start": start, "end": f["end"], "val": f["val"], "filed": f["filed"],
                                                         "form": str(f["form"]), "accn": f.get("accn") if isinstance(f.get("accn"), str) else None})
        if not by_unit:
            continue
        unit, rows = sorted(by_unit.items(), key=lambda kv: (-len(kv[1]), kv[0]))[0]
        return taxonomy, unit, rows
    return None, None, []


def _is_quarter(r: dict) -> bool:
    return r["start"] is not None and 80 <= _days(r["start"], r["end"]) <= 100


def _is_annual(r: dict) -> bool:
    return r["start"] is not None and 350 <= _days(r["start"], r["end"]) <= 380


def resolve_period(companyfacts: Any, metric: str, spec: str) -> dict:
    """The period a spec names.

    latest_quarter, latest_annual, FY<year> (the fiscal year ending in that
    year) or Q<n> <year> (the quarter ending in that calendar quarter).
    Instants take the end of the matching revenue period.
    """
    # The company's reporting periods come from its revenue (ASTS stopped tagging undimensioned EPS in 2022,
    # so its own latest EPS quarter is not the latest quarter); without revenue, the metric's own or net income's.
    _, _, rows = metric_facts(companyfacts, "revenue")
    if not rows:
        _, _, rows = metric_facts(companyfacts, metric if METRICS[metric]["kind"] == "duration" else "net_income")
    text = spec.strip()
    if re.fullmatch(r"latest_quarter", text, re.I):
        period_type = "QUARTER"
        candidates = [r for r in rows if _is_quarter(r)]
    elif re.fullmatch(r"latest_annual", text, re.I):
        period_type = "ANNUAL"
        candidates = [r for r in rows if _is_annual(r)]
    elif (m := re.fullmatch(r"FY\s?(\d{4})", text, re.I)):
        period_type = "ANNUAL"
        year = m.group(1)
        candidates = [r for r in rows if _is_annual(r) and r["end"][:4] == year]
    elif (m := re.fullmatch(r"Q([1-4])\s?(\d{4})", text, re.I)):
        period_type = "QUARTER"
        q = int(m.group(1))
        year = m.group(2)
        candidates = [r for r in rows if _is_quarter(r) and r["end"][:4] == year and math.ceil(int(r["end"][5:7]) / 3) == q]
    else:
        return {"status": "INVALID_PERIOD", "message": "period must be latest_quarter, latest_annual, FY<yyyy> or Q<n> <yyyy>."}
    if not candidates:
        return {"status": "PERIOD_NOT_FOUND", "spec": text, "periodType": period_type,
                "message": f"No {period_type.lower()} period in companyfacts matches {text} (20-F and 40-F filers tag annual periods only)."}
    best = sorted(candidates, key=lambda r: (r["end"], r["start"] or ""))[-1]
    kind = METRICS[metric]["kind"]
    return {
        "status": "OK",
        "spec": text,
        "periodType": period_type,
        "periodStart": None if kind == "instant" else best["start"],
        "periodEnd": best["end"],
        "labelBasis": "Q<n> is the calendar quarter of the period end; FY<yyyy> is the fiscal year ending in that year",
    }


def _row_ref(r: dict) -> dict:
    return {"concept": r["concept"], "value": r["val"], "form": r["form"], "filed": r["filed"], "accessionNumber": r["accn"]}


def sec_observations(companyfacts: Any, metric: str, period: dict) -> list[dict]:
    """SEC values for the period: as first filed and as most recently filed, with every distinct value filed."""
    taxonomy, unit, rows = metric_facts(companyfacts, metric)
    spec = METRICS[metric]
    all_rows = [r for r in rows if r["end"] == period.get("periodEnd") and (spec["kind"] == "instant" or r["start"] == period.get("periodStart"))]
    if not all_rows:
        return [{"source": "SEC_XBRL_LATEST", "provider": "SEC", "status": "NOT_FOUND", "value": None, "taxonomy": taxonomy, "unit": unit}]
    # One concept: the one filed most recently for the period (the earlier-listed on a tie), so NetIncomeLoss
    # is not compared with ProfitLoss (which includes noncontrolling interests) from the same filing.
    concepts = spec["ifrs"] if taxonomy == "ifrs-full" else spec["gaap"]

    def newest_filed(c: str) -> str:
        filed = ""
        for r in all_rows:
            if r["concept"] == c and r["filed"] > filed:
                filed = r["filed"]
        return filed

    present = [c for c in concepts if newest_filed(c) != ""]
    concept = present[0]
    for c in present[1:]:
        if newest_filed(c) > newest_filed(concept):
            concept = c

    def ordered(c: str) -> list[dict]:
        return sorted([r for r in all_rows if r["concept"] == c], key=lambda r: (r["filed"], r["accn"] or ""))

    matching = ordered(concept)
    first, last = matching[0], matching[-1]
    distinct = list(dict.fromkeys(r["val"] for r in matching))
    other_concepts = [_row_ref(ordered(c)[-1]) for c in present if c != concept]
    return [
        {"source": "SEC_XBRL_LATEST", "provider": "SEC", "status": "FOUND", "value": last["val"], "unit": unit, "taxonomy": taxonomy, "precision": 0,
         "evidence": _row_ref(last), "filedValues": distinct, "otherConcepts": other_concepts},
        {"source": "SEC_XBRL_AS_FIRST_FILED", "provider": "SEC", "status": "FOUND", "value": first["val"], "unit": unit, "taxonomy": taxonomy, "precision": 0,
         "evidence": _row_ref(first)},
    ]


# Sentences and bullets, including the " o " bullets SEC-rendered releases carry.
_PIECE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\s+[•●▪◦·]\s+|\s+o\s+(?=[A-Z])")
_GUIDANCE_RE = re.compile(r"\b(?:guidance|outlook|expect(?:s|ed|ation|ations)?|forecast|project(?:s|ed|ion|ions)?|target|range)\b", _F)
_NON_RESULT_RE = re.compile(r"\b(?:awards?|awarded|contract value|aggregate value|backlog|bookings|orders?|pipeline|contracted)\b", _F)
_QUARTER_SCOPE_RE = re.compile(r"\b(?:quarter(?:ly)?|three months|Q[1-4])\b", _F)
_ANNUAL_SCOPE_RE = re.compile(r"\b(?:full[- ]year|fiscal (?:year )?20\d\d|years? ended|twelve months|for (?:the )?(?:fiscal )?year|annual)\b", _F)
_INSTANT_SCOPE_RE = re.compile(r"\b(?:as of|ended (?:the )?(?:quarter|year|period)|at (?:the )?(?:end|close) of|balance)\b", _F)
_MONEY = r"(\(?)\s?(-?)\s?\$\s?(\(?)(-?)([0-9][0-9,]*)(\.[0-9]+)?\)?(?:\s?(billion|million|thousand)\b)?"
_QUARTER_ORDINAL = ("first", "second", "third", "fourth")


def _explicit_period_matches(sentence: str, amount_start: int, period: dict) -> bool:
    before_amount = sentence[:amount_start]
    if period.get("periodType") == "QUARTER":
        end = str(period.get("periodEnd") or "")
        q = math.ceil(int(end[5:7]) / 3)
        year = end[:4]
        mentions = list(re.finditer(r"\b(?:Q([1-4])|(?:first|second|third|fourth) quarter)\b", before_amount, re.I))
        for m in mentions:
            token = m.group(0).lower()
            observed_q = int(m.group(1)) if m.group(1) else next((i + 1 for i, x in enumerate(_QUARTER_ORDINAL) if token.startswith(x)), 0)
            if observed_q != q:
                return False
            nearby = before_amount[m.start():m.start() + 60]
            ym = re.search(r"\b(20\d{2})\b", nearby)
            if ym and ym.group(1) != year:
                return False
        if not mentions:
            after = sentence[amount_start:]
            first_period = re.search(r"\b(?:Q([1-4])|(?:first|second|third|fourth) quarter)\b", after, re.I)
            if first_period:
                prefix = after[:first_period.start()]
                if not re.search(r"\b(?:compared (?:with|to)|versus|vs\.?|from)\b", prefix, re.I):
                    token = first_period.group(0).lower()
                    observed_q = int(first_period.group(1)) if first_period.group(1) else next((i + 1 for i, x in enumerate(_QUARTER_ORDINAL) if token.startswith(x)), 0)
                    if observed_q != q:
                        return False
                    nearby = after[first_period.start():first_period.start() + 60]
                    ym = re.search(r"\b(20\d{2})\b", nearby)
                    if ym and ym.group(1) != year:
                        return False
        return True
    if period.get("periodType") == "ANNUAL":
        requested_year = str(period.get("periodEnd") or "")[:4]
        before_years = re.findall(r"\b(20\d{2})\b", before_amount)
        if any(y != requested_year for y in before_years):
            return False
        after = sentence[amount_start:]
        later_year = re.search(r"\b(20\d{2})\b", after)
        if not before_years and later_year:
            prefix = after[:later_year.start()]
            if not re.search(r"\b(?:compared (?:with|to)|versus|vs\.?|from)\b", prefix, re.I) and later_year.group(1) != requested_year:
                return False
        return True
    return True
_SCALE = {"billion": 1e9, "million": 1e6, "thousand": 1e3}


def release_observation(release: dict | None, metric: str, period: dict, reporting_unit: str | None) -> dict:
    """A release sentence's figure for the metric and period, or why none was read."""
    base = {"source": "ISSUER_RELEASE", "provider": "ISSUER_RELEASE", "url": (release or {}).get("url"), "filingDate": (release or {}).get("filingDate"),
            "accessionNumber": (release or {}).get("accessionNumber")}
    if not release or release.get("status") == "NOT_RESOLVED":
        return {**base, "status": "NOT_RESOLVED", "value": None}
    if release.get("status") != "READ" or not isinstance(release.get("text"), str):
        return {**base, "status": "NOT_READ", "value": None}
    if reporting_unit and not reporting_unit.startswith("USD"):
        return {**base, "status": "NOT_COMPARED_CURRENCY", "value": None, "reportingUnit": reporting_unit}
    spec = METRICS[metric]
    label = re.compile(spec["releaseLabel"], _F)
    anchored = re.compile(f"({spec['releaseLabel']})[^$]{{0,80}}?{_MONEY}", _F)
    unscoped = 0
    for raw in _PIECE_SPLIT_RE.split(_collapse(release["text"])):
        sentence = raw.strip()
        if not sentence or len(sentence) > 600 or not label.search(sentence):
            continue
        if _GUIDANCE_RE.search(sentence) or _NON_RESULT_RE.search(sentence):
            continue
        m = anchored.search(sentence)
        if not m:
            continue
        quarter = _QUARTER_SCOPE_RE.search(sentence) is not None
        annual = _ANNUAL_SCOPE_RE.search(sentence) is not None
        amount_start = m.start() + m.group(0).find("$")
        if spec["kind"] == "instant":
            scoped = _INSTANT_SCOPE_RE.search(sentence) is not None
        elif period.get("periodType") == "QUARTER":
            scoped = quarter and not annual and _explicit_period_matches(sentence, amount_start, period)
        else:
            scoped = annual and not quarter and _explicit_period_matches(sentence, amount_start, period)
        if not scoped:
            unscoped += 1
            continue
        label_text, open_paren, minus, inner_paren, inner_minus, int_part, frac, scale_word = m.groups()
        scale = _SCALE[scale_word.lower()] if scale_word else 1
        magnitude = float(int_part.replace(",", "") + (frac or "")) * scale
        negative = open_paren == "(" or inner_paren == "(" or minus == "-" or inner_minus == "-" or re.search(r"\bloss\b", label_text, _F) is not None
        decimals = len(frac) - 1 if frac else 0
        precision = 0.5 * 10 ** -decimals * scale
        as_written = re.sub(r"^[^$(-]*", "", m.group(0)[len(label_text):]).strip()
        return {**base, "status": "FOUND", "value": -magnitude if negative else magnitude, "precision": precision, "asWritten": as_written,
                "sentence": sentence[:400]}
    return {**base, "status": "NOT_FOUND_IN_TEXT", "value": None, "unscopedCandidates": unscoped}


def pick_release_observation(observations: list[dict]) -> dict:
    """Of the releases read for the period (oldest first), the first with a scoped figure; else the first read.

    Every release considered is listed.
    """
    considered = [{"filingDate": o.get("filingDate"), "accessionNumber": o.get("accessionNumber"), "status": o.get("status")} for o in observations]
    found = [o for o in observations if o.get("status") == "FOUND"]
    chosen = found[-1] if found else (observations[0] if observations else
                  {"source": "ISSUER_RELEASE", "provider": "ISSUER_RELEASE", "status": "NOT_RESOLVED", "value": None})
    return {**chosen, "releasesConsidered": considered}


def yahoo_observation(rows: list[dict] | None, metric: str, period: dict, frequency: str) -> dict:
    """Yahoo's statement row for the period end (within 7 days)."""
    base = {"source": "YAHOO", "provider": "YAHOO", "frequency": frequency}
    if rows is None:
        return {**base, "status": "NOT_READ", "value": None}
    end = str(period.get("periodEnd"))
    row = next((r for r in rows if isinstance(r.get("date"), str) and abs(_days(r["date"], end)) <= 7), None)
    if row is None:
        return {**base, "status": "NOT_FOUND", "value": None, "datesAvailable": [r.get("date") for r in rows][:8]}
    for field in METRICS[metric]["yahoo"]:
        v = row.get(field)
        if _num(v) and math.isfinite(v):
            return {**base, "status": "FOUND", "value": v, "precision": 0, "field": field, "periodEnd": row["date"]}
    return {**base, "status": "FIELD_EMPTY", "value": None, "periodEnd": row["date"], "fields": METRICS[metric]["yahoo"]}


def reconcile_observations(observations: list[dict], tolerance_pct: float) -> tuple[list[dict], str, bool]:
    """Each found value against the baseline, and the overall agreement."""
    found = [o for o in observations if o.get("status") == "FOUND" and _num(o.get("value"))]
    baseline = next((o for o in found if o["source"] == "SEC_XBRL_LATEST"), None)
    comparisons: list[dict] = []
    conflict = False
    restated = False
    if baseline is not None:
        b = baseline["value"]
        for o in found:
            if o is baseline:
                continue
            v = o["value"]
            difference = v - b
            tolerance = max(abs(b) * tolerance_pct / 100, float(o.get("precision") or 0), float(baseline.get("precision") or 0))
            match = abs(difference) <= tolerance + 1e-9 * max(1, abs(b))
            row = {
                "source": o["source"],
                "against": baseline["source"],
                "value": v,
                "baselineValue": b,
                "difference": _round(difference, 6),
                "differencePct": _round(difference / abs(b) * 100, 4) if b != 0 else None,
                "tolerance": _round(tolerance, 6),
                "result": "MATCH" if match else "MISMATCH",
            }
            if o["source"] == "SEC_XBRL_AS_FIRST_FILED":
                row["result"] = "MATCH" if match else "RESTATED"
                restated = not match
            elif not match:
                conflict = True
            comparisons.append(row)
    providers = {o["provider"] for o in found}
    status = "NOT_FOUND" if not found else "PARTIAL" if baseline is None else "CONFLICT" if conflict else "AGREED" if len(providers) >= 2 else "PARTIAL"
    return comparisons, status, restated


def metric_reconciliation(*, ticker: str, metric: str, period: dict, companyfacts: Any, releases: list[dict], yahoo_rows: list[dict] | None,
                          tolerance_pct: float) -> dict:
    """releases: the releases read for the period, oldest first; empty when none was filed in the window."""
    taxonomy, unit, _ = metric_facts(companyfacts, metric)
    frequency = "annual" if period.get("periodType") == "ANNUAL" else "quarterly"
    observations = [
        *sec_observations(companyfacts, metric, period),
        pick_release_observation([release_observation(r, metric, period, unit) for r in releases]),
        yahoo_observation(yahoo_rows, metric, period, frequency),
    ]
    comparisons, status, restated = reconcile_observations(observations, tolerance_pct)
    found = {o["provider"] for o in observations if o.get("status") == "FOUND"}
    return {
        "ticker": ticker.upper(),
        "metric": metric,
        "period": period,
        "taxonomy": taxonomy,
        "unit": unit,
        "status": status,
        "restated": restated,
        "tolerancePct": tolerance_pct,
        "observations": observations,
        "comparisons": comparisons,
        "missingProviders": [p for p in ("SEC", "ISSUER_RELEASE", "YAHOO") if p not in found],
        "sourcesNotCompared": [
            {"source": "SEC_FILING_TABLES", "reason": "The filing's statement tables are the inline XBRL values companyfacts carries per filing; both as-first-filed and latest are compared."},
            {"source": "ALPHA_VANTAGE", "reason": "Not read: its free-tier quota is reserved for consensus estimates."},
            {"source": "COMPANIES_HOUSE", "reason": "UK statutory filings; not applicable to SEC registrants and not tagged for these metrics."},
        ],
        "notes": [
            "AGREED: at least two providers found the value and all agree within tolerance. PARTIAL: one provider found it. CONFLICT: a provider differs from the latest SEC value beyond tolerance. NOT_FOUND: none found it.",
            "A difference between the SEC value as first filed and as latest filed is a restatement (restated: true), not a conflict.",
            "The tolerance is the larger of tolerancePct of the SEC value and half the last stated digit of a release figure.",
            "Release figures are read only from sentences scoped to the period (quarter or full year); unscopedCandidates counts sentences skipped for scope.",
        ],
        **AUTHORITY_BOUNDARY,
    }
