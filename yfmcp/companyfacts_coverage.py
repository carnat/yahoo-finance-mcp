"""Whether the filer's latest annual report is in SEC companyfacts (2.5.30, F-015/F-018).

SEC's companyfacts file can lack a filed annual report's facts: TSM's FY2025 20-F (0001628280-26-025362) added
only two administrative facts, so every companyfacts-backed figure fell back to FY2024. Each path that reads
companyfacts raises the same warning when that happens, rather than each discovering it differently.
Shared with worker/src/companyfacts-coverage.ts; parity is tested in scripts/test_companyfacts_coverage.py.
"""

from __future__ import annotations

from typing import Any, Callable

LATEST_ANNUAL_NOT_IN_COMPANYFACTS = "LATEST_ANNUAL_NOT_IN_COMPANYFACTS"
_ANNUAL_FORMS = frozenset({"10-K", "20-F", "40-F"})
# The taxonomies this server reads financial figures from; dei and srt carry administrative facts.
_FINANCIAL_TAXONOMIES = ("us-gaap", "ifrs-full")


def _bare(accession: str) -> str:
    return accession.replace("-", "")


def annual_filings_from_submissions(submissions: Any) -> list[dict]:
    """The annual reports (10-K, 20-F, 40-F; amendments excluded) in SEC submissions' recent filings."""
    recent = ((submissions or {}).get("filings") or {}).get("recent") or {} if isinstance(submissions, dict) else {}
    forms = recent.get("form") or []
    accessions = recent.get("accessionNumber") or []
    filed = recent.get("filingDate") or []
    reports = recent.get("reportDate") or []
    out = []
    for i, form in enumerate(forms):
        form = str(form or "").upper()
        accession = str(accessions[i] if i < len(accessions) and accessions[i] is not None else "")
        if form not in _ANNUAL_FORMS or not accession:
            continue
        report = reports[i] if i < len(reports) else None
        out.append({"accessionNumber": accession, "form": form, "filed": str(filed[i] if i < len(filed) and filed[i] is not None else ""),
                    "reportDate": str(report) if report else None})
    return out


def latest_annual_filing(filings: list[dict], as_of: str = "9999-12-31") -> dict | None:
    """The newest annual report filed on or before as_of's day."""
    day = as_of[:10]
    best = None
    for f in filings:
        filed = str(f.get("filed") or "")
        if str(f.get("form") or "").upper() not in _ANNUAL_FORMS or not filed or filed[:10] > day:
            continue
        if best is None or filed > best["filed"] or (filed == best["filed"] and f["accessionNumber"] > best["accessionNumber"]):
            best = f
    return best


def _each_fact(companyfacts: Any, visit: Callable[[dict], bool]) -> None:
    facts = (companyfacts or {}).get("facts") if isinstance(companyfacts, dict) else None
    if not isinstance(facts, dict):
        return
    for taxonomy in _FINANCIAL_TAXONOMIES:
        concepts = facts.get(taxonomy)
        if not isinstance(concepts, dict):
            continue
        for concept in concepts.values():
            units = concept.get("units") if isinstance(concept, dict) else None
            if not isinstance(units, dict):
                continue
            for rows in units.values():
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    if isinstance(row, dict) and visit(row):
                        return


def accession_in_companyfacts(companyfacts: Any, accession: str) -> bool:
    """Whether companyfacts holds any us-gaap or ifrs-full fact from the accession."""
    target = _bare(accession)
    found = [False]

    def visit(row: dict) -> bool:
        found[0] = isinstance(row.get("accn"), str) and _bare(row["accn"]) == target
        return found[0]

    _each_fact(companyfacts, visit)
    return found[0]


def _latest_annual_period_end(companyfacts: Any, as_of: str) -> str | None:
    """The newest period end of an annual-report fact companyfacts holds, filed on or before as_of's day."""
    day = as_of[:10]
    latest = [None]

    def visit(row: dict) -> bool:
        form = str(row.get("form") or "").upper()
        form = form[:-2] if form.endswith("/A") else form
        end = row.get("end")
        if form in _ANNUAL_FORMS and isinstance(end, str) and str(row.get("filed") or "") <= day and (latest[0] is None or end > latest[0]):
            latest[0] = end
        return False

    _each_fact(companyfacts, visit)
    return latest[0]


def latest_annual_coverage_warning(filings: list[dict], companyfacts: Any, as_of: str = "9999-12-31") -> dict | None:
    """The LATEST_ANNUAL_NOT_IN_COMPANYFACTS warning when the latest annual report filed by as_of contributes no
    us-gaap or ifrs-full fact to companyfacts and no annual period companyfacts holds reaches its period end, else
    None. A filer with no annual report on file gets none."""
    latest = latest_annual_filing(filings, as_of)
    if latest is None or accession_in_companyfacts(companyfacts, latest["accessionNumber"]):
        return None
    held = _latest_annual_period_end(companyfacts, as_of)
    # An amendment filed since may have put the report's year into companyfacts: the year is there, so no warning.
    if latest.get("reportDate") and held and held >= latest["reportDate"]:
        return None
    by = "" if as_of.startswith("9999") else f" filed by {as_of[:10]}"
    period = f", period {latest['reportDate']}" if latest.get("reportDate") else ""
    holds = f"the newest annual period companyfacts holds ends {held}" if held else "companyfacts holds no annual period"
    return {
        "code": LATEST_ANNUAL_NOT_IN_COMPANYFACTS,
        "message": f"The latest annual report{by} ({latest['form']} {latest['accessionNumber']}, filed {latest['filed']}{period}) has no us-gaap or ifrs-full fact in SEC companyfacts; "
                   f"{holds}. Figures here come from earlier reports, not from that filing.",
        "severity": "warning",
        "form": latest["form"],
        "accessionNumber": latest["accessionNumber"],
        "filed": latest["filed"],
        "reportDate": latest.get("reportDate"),
        "latestCompanyfactsAnnualPeriodEnd": held,
    }
