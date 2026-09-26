"""SEC companyconcept fact selection across equivalent concepts (2.4.4).

Filers move between equivalent us-gaap concepts: ASTS reported revenue as
RevenueFromContractWithCustomerExcludingAssessedTax until 2023 and as
...IncludingAssessedTax since. Reading the first concept that has any facts
returned a 2022 quarter as "latest". The concept with the newest filing of
the requested form wins instead, and a pinned accession must be matched.

Mirrors worker/src/sec-facts.ts; scripts/test_sec_facts.py requires identical
output from both.
"""

from __future__ import annotations

REVENUE_CONCEPTS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
]


def pick_concept_facts(candidates: list[dict], form: str, accession: str | None = None) -> dict | None:
    """Of several concepts for one fact, the one whose facts for the form were filed most recently.

    The earlier-listed concept wins a tie. With a pinned accession only facts
    from that filing count, so the first concept tagged in it wins. The
    returned facts are already limited to the form and accession.
    """
    want_form = form.upper()
    want_accession = accession.strip() if accession else ""
    best = None
    best_filed = ""
    for candidate in candidates:
        rows = [f for f in candidate["facts"]
                if str(f.get("form") or "").upper() == want_form
                and (not want_accession or str(f.get("accn") or "") == want_accession)]
        if not rows:
            continue
        filed = max(str(f.get("filed") or "") for f in rows)
        if best is None or filed > best_filed:
            best = {"concept": candidate["concept"], "facts": rows}
            best_filed = filed
    return best


def _duration_days(fact: dict) -> float:
    import datetime
    start = fact.get("start") if isinstance(fact.get("start"), str) else ""
    end = fact.get("end") if isinstance(fact.get("end"), str) else ""
    if not start:
        return 0
    try:
        return (datetime.date.fromisoformat(end) - datetime.date.fromisoformat(start)).days
    except ValueError:
        return 1e9


def filing_fact_in_accession(candidates: list[dict], accession: str) -> dict | None:
    """A filing's own value for a fact (2.4.5).

    The first concept tagged in the accession, at the latest period end it
    reports, and the shortest period there, so a 10-Q gives its quarter rather
    than the year to date or the prior-year comparative. Any form counts; the
    old snapshot read 10-K forms only.
    """
    want = accession.strip()
    for candidate in candidates:
        rows = [f for f in candidate["facts"]
                if str(f.get("accn") or "") == want and isinstance(f.get("end"), str) and f.get("end") and f.get("val") is not None]
        if not rows:
            continue
        latest_end = max(str(f["end"]) for f in rows)
        best = None
        for f in rows:
            if str(f["end"]) != latest_end:
                continue
            if best is None or _duration_days(f) < _duration_days(best):
                best = f
        if best is not None:
            return {"concept": candidate["concept"], "fact": best}
    return None
