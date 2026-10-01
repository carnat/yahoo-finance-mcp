"""Operating-driver ledger (2.5.3), mirroring worker/src/driver-ledger.ts.

Parity is tested in scripts/test_guidance_and_drivers.py. Pure.

Three sources, each reported as disclosed:
- standard driver series from SEC companyfacts (revenue, gross profit, R&D,
  capex, remaining performance obligations, contract liabilities), newest
  filing per period; year-to-date cash-flow periods stay year-to-date;
- company-specific inline XBRL facts (the filer's own taxonomy) from the
  latest periodic filing (non-monetary, financing concepts left out);
- operating statements from the filing and the latest earnings release with
  a figure, a driver category, whether the sentence reports an actual or
  states a target, timing, and the quoted source.
A series the company does not tag is NOT_REPORTED; nothing is derived,
annualized or filled.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
from typing import Any

from yfmcp.capital_structure import _collapse
from yfmcp.evidence import AUTHORITY_BOUNDARY
from yfmcp.funding_schedule import timing_of
from yfmcp.sec_facts import REVENUE_CONCEPTS

_F = re.I | re.A

DRIVER_SERIES: list[dict] = [
    {"driver": "revenue", "concepts": list(REVENUE_CONCEPTS), "unit": "USD", "periodKind": "duration"},
    {"driver": "gross_profit", "concepts": ["GrossProfit"], "unit": "USD", "periodKind": "duration"},
    {"driver": "research_and_development", "concepts": ["ResearchAndDevelopmentExpense"], "unit": "USD", "periodKind": "duration"},
    {"driver": "capital_expenditure", "concepts": ["PaymentsToAcquirePropertyPlantAndEquipment"], "unit": "USD", "periodKind": "duration"},
    {"driver": "remaining_performance_obligation", "concepts": ["RevenueRemainingPerformanceObligation"], "unit": "USD", "periodKind": "instant"},
    {"driver": "contract_liabilities", "concepts": ["ContractWithCustomerLiability"], "unit": "USD", "periodKind": "instant"},
    {"driver": "contract_liabilities_current", "concepts": ["ContractWithCustomerLiabilityCurrent"], "unit": "USD", "periodKind": "instant"},
    {"driver": "contract_liabilities_noncurrent", "concepts": ["ContractWithCustomerLiabilityNoncurrent"], "unit": "USD", "periodKind": "instant"},
]

SERIES_POINT_LIMIT = 16
COMPANY_SERIES_LIMIT = 40
TEXT_DRIVER_LIMIT = 40


def _days(start: str, end: str) -> int:
    return (_dt.date.fromisoformat(end[:10]) - _dt.date.fromisoformat(start[:10])).days


def period_type(start: str | None, end: str) -> str:
    if start is None:
        return "INSTANT"
    d = _days(start, end)
    if 80 <= d <= 100:
        return "QUARTER"
    if 350 <= d <= 380:
        return "ANNUAL"
    if 100 < d < 350:
        return "YEAR_TO_DATE"
    return "OTHER"


def xbrl_driver_series(companyfacts: Any) -> list[dict]:
    """Standard driver series from companyfacts, newest filing per period, last points by period end."""
    facts = companyfacts.get("facts") if isinstance(companyfacts, dict) else None
    usgaap = (facts or {}).get("us-gaap") or {}
    out: list[dict] = []
    for spec in DRIVER_SERIES:
        by_period: dict[str, dict] = {}
        for concept in spec["concepts"]:
            units = (usgaap.get(concept) or {}).get("units") or {}
            for f in units.get(spec["unit"]) or []:
                val = f.get("val")
                if not isinstance(f.get("end"), str) or isinstance(val, bool) or not isinstance(val, (int, float)):
                    continue
                start = f.get("start") if isinstance(f.get("start"), str) else None
                if (spec["periodKind"] == "instant") != (start is None):
                    continue
                if not re.match(r"10-[KQ]", str(f.get("form") or "")):
                    continue
                key = f"{start or ''}|{f['end']}"
                prev = by_period.get(key)
                if prev is None or str(f.get("filed") or "") > str(prev.get("filed") or ""):
                    by_period[key] = {**f, "start": start, "concept": concept}
        ordered = sorted(by_period.values(), key=lambda f: (str(f["end"]), f["start"] or ""))[-SERIES_POINT_LIMIT:]
        points = [
            {
                "periodStart": f["start"],
                "periodEnd": f["end"],
                "periodType": period_type(f["start"], f["end"]),
                "value": f["val"],
                "concept": f["concept"],
                "form": f.get("form"),
                "filed": f.get("filed"),
                "accessionNumber": f.get("accn"),
            }
            for f in ordered
        ]
        out.append({
            "driver": spec["driver"],
            "concepts": [f"us-gaap:{c}" for c in spec["concepts"]],
            "unit": spec["unit"],
            # A series the company stopped tagging shows its last period here.
            "latestPeriodEnd": points[-1]["periodEnd"] if points else None,
            # An unread companyfacts is not an untagged driver.
            "status": "NOT_READ" if companyfacts is None else "REPORTED" if points else "NOT_REPORTED",
            "points": points,
        })
    return out


_STANDARD_PREFIXES = {"us-gaap", "dei", "srt", "ifrs-full", "country", "currency", "exch", "stpr", "naics", "sic", "ecd", "cyd", "invest", "xbrli", "iso4217", "utr"}


def _is_monetary(unit: str | None) -> bool:
    return unit is not None and re.match(r"[A-Z]{3}(?:/|$)", unit) is not None


# Financing, equity and acquisition terms belong to the capital-structure and dilution tools, not the operating ledger.
CAPITAL_CONCEPT_RE = re.compile(
    r"Stock|Share|Warrant|Convertible|Note|LineOfCredit|Credit|Loan|Debt|Borrowing|Interest|Fee|Ownership|Voting|Equity|Tax|Lease"
    r"|Principal|Installment|Acquisition|BusinessCombination|Dividend|Option|Award|Vesting|Compensation|Seller|Commission|Premium|Restructuring"
)
_CAPITAL_AXIS_RE = re.compile(r"ClassOfStock|Warrant|Debt|LineOfCredit|CreditFacility|Equity|Stock")


def _unit_rank(unit: str | None) -> int:
    """Custom units (satellites, patents, customers) first, then ratios and counts."""
    if unit is not None and re.fullmatch(r"shares", unit, re.I):
        return 2
    if unit is None or re.fullmatch(r"pure", unit, re.I):
        return 1
    return 0


def concept_label(local: str) -> str:
    return re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", local))


def company_specific_series(facts: list[dict]) -> tuple[list[dict], int, int]:
    """The filer's own tagged non-monetary operating figures, grouped by concept and dimensions.

    Monetary extension concepts are accounting line items, and financing
    concepts belong to the capital tools; both are counted, not listed.
    Returns the series and the two counts.
    """
    groups: dict[str, dict] = {}
    capital: set[str] = set()
    monetary: set[str] = set()
    for f in facts:
        if f.get("value") is None or f.get("periodEnd") is None:
            continue
        name = f["name"]
        colon = name.find(":")
        prefix = name[:colon] if colon > 0 else ""
        if not prefix or prefix.lower() in _STANDARD_PREFIXES:
            continue
        local = name[colon + 1:]
        raw_dims = f.get("dims") or {}
        dim_keys = sorted(raw_dims)
        if CAPITAL_CONCEPT_RE.search(local) or any(_CAPITAL_AXIS_RE.search(k[k.find(":") + 1:]) for k in dim_keys):
            capital.add(name)
            continue
        if _is_monetary(f.get("unit")):
            monetary.add(name)
            continue
        dims = {k: raw_dims[k] for k in dim_keys}
        key = f"{name}|{f.get('unit') or ''}|{','.join(f'{k}={raw_dims[k]}' for k in dim_keys)}"
        g = groups.get(key)
        if g is None:
            g = {"concept": name, "label": concept_label(local), "unit": f.get("unit"), "dims": dims, "points": [], "seen": set()}
            groups[key] = g
        period_key = f"{f.get('periodStart') or ''}|{f['periodEnd']}"
        if period_key in g["seen"]:
            continue
        g["seen"].add(period_key)
        g["points"].append({"periodStart": f.get("periodStart"), "periodEnd": f["periodEnd"],
                            "periodType": period_type(f.get("periodStart"), f["periodEnd"]), "value": f["value"]})
    out = [
        {
            "concept": g["concept"],
            "label": g["label"],
            "unit": g["unit"],
            "dims": g["dims"],
            "points": sorted(g["points"], key=lambda p: (str(p["periodEnd"]), p["periodStart"] or "")),
        }
        for g in groups.values()
    ]
    # Custom units first; within each, undimensioned before dimensioned, then by concept.
    out.sort(key=lambda s: (_unit_rank(s["unit"]), len(s["dims"]), s["concept"], json.dumps(s["dims"], separators=(",", ":"), ensure_ascii=False)))
    return out[:COMPANY_SERIES_LIMIT], len(capital), len(monetary)


DRIVER_CATEGORIES: list[tuple[str, re.Pattern]] = [
    ("capacity", re.compile(r"\bcapacity\b|\bmegawatts?\b|\bgigawatts?\b|\b[0-9.,]+\s?(?:MW|GW)\b", _F)),
    ("production", re.compile(r"\bproduc(?:e|ed|es|ing|tion)\b|\bmanufactur(?:e|ed|es|ing)\b|\boutput\b", _F)),
    ("deliveries", re.compile(r"\bdeliver(?:ed|ies|y)\b|\bshipments?\b|\bshipped\b", _F)),
    ("launches_deployments", re.compile(r"\blaunch(?:es|ed|ing)?\b|\bdeploy(?:s|ed|ing|ment|ments)?\b|\bin orbit\b|\bsatellites?\b", _F)),
    ("customers", re.compile(r"\bcustomers?\b|\bsubscribers?\b|\busers\b|\bmembers\b", _F)),
    ("backlog_bookings", re.compile(r"\bbacklog\b|\bbookings?\b|\bremaining performance obligations?\b|\bbook-to-bill\b|\border(?:s| book)\b", _F)),
    ("utilization", re.compile(r"\butiliz(?:ation|ed)\b|\boccupancy\b|\bload factor\b", _F)),
    ("pricing", re.compile(r"\baverage (?:selling |sales )?prices?\b|\bASPs?\b|\bpricing\b|\bprice increases?\b|\bARPU\b", _F)),
    ("yield", re.compile(r"\byields?\b", _F)),
    ("headcount", re.compile(r"\bemployees\b|\bheadcount\b|\bfull-time\b", _F)),
]

# A figure: a dollar amount, a percentage, or a number with a scale or unit; bare years are not figures.
# "$2.05B" keeps its "B" (2.5.20).
_FIGURE_RE = re.compile(
    r"(\$\s?)?\b([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(\.[0-9]+)?"
    r"(?:\s?(%|percent\b|billion\b|million\b|thousand\b|bn\b|mn\b|[BMK]\b|MW\b|GW\b|megawatts?\b|gigawatts?\b|square (?:feet|foot|meters?)\b|[a-z][a-z-]{1,19}[a-z]\b))?",
    re.A,
)
_NON_UNIT_WORDS = {"and", "or", "to", "of", "in", "the", "for", "from", "with", "at", "on", "as", "by", "per", "compared", "versus", "vs", "year",
                   "years", "quarter", "quarters", "month", "months", "days", "day", "was", "were", "is", "are", "will", "would", "through", "into",
                   "over", "under", "within", "than", "more", "less", "higher", "lower", "increase", "increases", "decrease", "decreases",
                   "respectively", "marks", "which", "that", "this", "these", "each", "both", "after", "before", "during", "since", "until",
                   "including"}


def figures_in(sentence: str) -> list[dict]:
    out: list[dict] = []
    for m in _FIGURE_RE.finditer(sentence):
        dollar = m.group(1) is not None
        if not dollar:
            # "Block 2", "BlueBird 8-13", "New Glenn 3": a number in a name is not a figure.
            pre = sentence[:m.start()]
            name = re.search(r"([A-Z][A-Za-z-]*)\s?$", pre)
            # A sentence-initial reporting/quantity word can legitimately precede a
            # number ("Reported 5 satellites", "Approximately 5 million"). Product/
            # model names ("Block 2", "BlueBird 8-13", "New Glenn 3") are not figures
            # even when the name begins at offset zero.
            sentence_start_quantity_prefix = bool(
                name
                and name.start() == 0
                and re.fullmatch(
                    r"Reported|Launched|Shipped|Delivered|Produced|Added|Reached|Had|Was|Were|Signed|Completed|Increased|Decreased|Grew|Approximately|About|Nearly|Over|Under|More|Less",
                    name.group(1),
                )
            )
            if (name and not sentence_start_quantity_prefix) or re.search(r"[0-9]-$", pre):
                continue
        unit_word = m.group(4)
        unit = None if unit_word is not None and unit_word.lower() in _NON_UNIT_WORDS else unit_word
        int_part = m.group(2)
        number = float(int_part.replace(",", "") + (m.group(3) or ""))
        # A year is not a figure, bare or before a word ("2026 revenue"); "2025 MW" or "2025%" still is.
        if (not dollar and m.group(3) is None and re.fullmatch(r"(?:19|20)[0-9]{2}", int_part)
                and not (unit is not None and re.fullmatch(r"%|percent|billion|million|thousand|bn|MW|GW|megawatts?|gigawatts?", unit))):
            continue
        if not dollar and unit is None:
            continue
        as_written = m.group(0) if unit is not None else f"{m.group(1) or ''}{int_part}{m.group(3) or ''}"
        out.append({"asWritten": as_written.strip(), "number": number, "currency": "USD" if dollar else None, "unitAsWritten": unit})
        if len(out) >= 6:
            break
    return out


_PLAN_RE = re.compile(
    r"\b(?:expects?|expected to|plans?|planned|planning|targets?|targeted|targeting|anticipates?|anticipated|aims?|intends?|will|goal|guidance"
    r"|outlook|forecasts?|projects?|projected|on track to|scheduled to|by (?:the )?end of)\b",
    _F,
)
_ACTUAL_RE = re.compile(
    r"\b(?:delivered|shipped|launched|deployed|produced|completed|reached|achieved|ended|added|installed|signed|totaled|totalled|grew|increased"
    r"|decreased|declined|had|was|were|reported|recorded|generated)\b",
    _F,
)


def driver_basis(sentence: str) -> str:
    plan = _PLAN_RE.search(sentence) is not None
    actual = _ACTUAL_RE.search(sentence) is not None
    if plan and not actual:
        return "TARGET_OR_PLAN"
    if actual and not plan:
        return "REPORTED_ACTUAL"
    return "UNCLEAR"


# Sentences and bullets, including the " o " bullets SEC-rendered releases carry.
_PIECE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\s+[\u2022\u25cf\u25aa\u25e6\u00b7]\s+|\s+o\s+(?=[A-Z])")


def pieces(text: str) -> list[str]:
    """Whole sentences of a context: a search window's cut-off first and last pieces are dropped."""
    # SEC exhibit headers ("EX-99.1 2 asts-ex99_1.htm") are not sentences.
    parts = [p.strip() for p in _PIECE_SPLIT_RE.split(_collapse(text))]
    parts = [p for p in parts if p and not re.search(r"\.htm", p, re.I)]
    return [p for i, p in enumerate(parts)
            if not (i == 0 and re.match(r"[a-z,;:)]", p)) and not (i == len(parts) - 1 and not re.search(r"[.!?][\"\u201d\u2019)]?$", p))]


def text_drivers(statements: list[dict]) -> list[dict]:
    """Operating statements with a figure and a driver category, deduplicated, release first."""
    out: list[dict] = []
    seen: set[str] = set()
    for st in statements:
        for sentence in pieces(st.get("contextText") or ""):
            if len(sentence) < 30 or len(sentence) > 600:
                continue
            categories = [c for c, pattern in DRIVER_CATEGORIES if pattern.search(sentence)]
            if not categories:
                continue
            figures = figures_in(sentence)
            if not figures:
                continue
            # Overlapping search windows repeat a sentence; its opening identifies it.
            key = sentence.lower()[:120]
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "categories": categories,
                "basis": driver_basis(sentence),
                "figures": figures,
                "timing": timing_of(sentence),
                "sentence": sentence,
                "source": st.get("source"),
                "sectionHeading": st.get("sectionHeading"),
                "documentUrl": st.get("documentUrl"),
                "filingDate": st.get("filingDate"),
                "accessionNumber": st.get("accessionNumber"),
            })
            if len(out) >= TEXT_DRIVER_LIMIT:
                return out
    return out


def operating_driver_ledger(*, ticker: str, companyfacts: Any, inline_facts: list[dict] | None, filing: dict | None, release: dict | None,
                            statements: list[dict]) -> dict:
    series = xbrl_driver_series(companyfacts)
    # inline_facts is None when the periodic filing could not be read.
    company, excluded_capital, excluded_monetary = ([], 0, 0) if inline_facts is None else company_specific_series(inline_facts)
    text = text_drivers(statements)
    by_category = {c: 0 for c, _ in DRIVER_CATEGORIES}
    for t in text:
        for c in t["categories"]:
            by_category[c] += 1
    by_basis = {"REPORTED_ACTUAL": 0, "TARGET_OR_PLAN": 0, "UNCLEAR": 0}
    for t in text:
        by_basis[t["basis"]] += 1
    return {
        "ticker": ticker.upper(),
        "basis": "COMPANY_DISCLOSED",
        "xbrlSeries": series,
        "notReported": [s["driver"] for s in series if s["status"] == "NOT_REPORTED"],
        "notRead": [s["driver"] for s in series if s["status"] == "NOT_READ"],
        "companySpecificStatus": "NOT_READ" if inline_facts is None else "READ",
        "companySpecificSeries": company,
        "excludedCapitalStructureConcepts": excluded_capital,
        "excludedMonetaryConcepts": excluded_monetary,
        "textDrivers": text,
        "summary": {"byCategory": by_category, "byBasis": by_basis, "textDriverCount": len(text), "companySpecificSeriesCount": len(company)},
        "sources": {"periodicFiling": filing, "earningsRelease": release},
        "notes": [
            "Standard series are SEC companyfacts as filed (10-K/10-Q), newest filing per period; year-to-date periods are not converted to quarters.",
            "Company-specific series are the filer's own non-monetary inline XBRL concepts in the latest periodic filing, custom units first; monetary extension line items and financing or equity concepts are counted, not listed.",
            "Text drivers are sentences with a figure; basis says whether the sentence reports an actual or states a target or plan, and UNCLEAR when it does both or neither.",
            "A driver the company does not tag or state is listed as not reported; nothing is derived, annualized or filled.",
        ],
        **AUTHORITY_BOUNDARY,
    }
