"""Funding and capex schedule (2.5.2), mirroring worker/src/funding-schedule.ts.

Parity is tested in scripts/test_funding_schedule.py. Pure.

Every amount keeps its classification, timing and source:
- CONTRACTUAL: obligations the filing tags by due period (debt principal,
  lease payments, purchase and other contractual obligations);
- COMPANY_DISCLOSED_COMMITTED: amounts the company says it has committed or
  is obligated to spend, stated in text;
- COMPANY_GUIDED: amounts the company expects, plans or budgets;
- AWARDED_CONTINGENT: grants, awards, incentives and milestone payments that
  depend on conditions;
- UNRESOLVED: a funding or capex statement whose amount or type cannot be
  classified from its wording.
Liquidity sources (cash, ATM capacity, undrawn facilities) are listed
separately; nothing is netted, forecast or filled in.
"""

from __future__ import annotations

import re
from typing import Any

from yfmcp.capital_structure import _collapse, _money_value, _sentences, member_label
from yfmcp.evidence import AUTHORITY_BOUNDARY

_F = re.I | re.A

CLASSIFICATIONS: dict[str, str] = {
    "CONTRACTUAL": "Obligations the filing tags by due period: debt principal, lease payments, purchase and other contractual obligations.",
    "COMPANY_DISCLOSED_COMMITTED": "Amounts the company states it has committed or is obligated to spend, from filing text.",
    "COMPANY_GUIDED": "Amounts the company expects, plans, estimates or budgets; forward-looking and not binding.",
    "AWARDED_CONTINGENT": "Grants, awards, incentives and milestone payments that depend on conditions being met.",
    "UNRESOLVED": "A funding or capex statement whose amount or type cannot be classified from its wording.",
}

_FAMILIES: list[dict] = [
    {"family": "debt_principal", "buckets": [
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalRemainderOfFiscalYear", "remainder_of_fiscal_year", 0),
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalInNextTwelveMonths", "next_12_months", 1),
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearTwo", "year_2", 2),
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearThree", "year_3", 3),
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFour", "year_4", 4),
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFive", "year_5", 5),
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalAfterYearFive", "after_year_5", 6),
    ], "totals": []},
    {"family": "operating_lease_payments", "buckets": [
        ("LesseeOperatingLeaseLiabilityPaymentsRemainderOfFiscalYear", "remainder_of_fiscal_year", 0),
        ("LesseeOperatingLeaseLiabilityPaymentsDueNextTwelveMonths", "next_12_months", 1),
        ("LesseeOperatingLeaseLiabilityPaymentsDueYearTwo", "year_2", 2),
        ("LesseeOperatingLeaseLiabilityPaymentsDueYearThree", "year_3", 3),
        ("LesseeOperatingLeaseLiabilityPaymentsDueYearFour", "year_4", 4),
        ("LesseeOperatingLeaseLiabilityPaymentsDueYearFive", "year_5", 5),
        ("LesseeOperatingLeaseLiabilityPaymentsDueAfterYearFive", "after_year_5", 6),
    ], "totals": ["LesseeOperatingLeaseLiabilityPaymentsDue"]},
    {"family": "finance_lease_payments", "buckets": [
        ("FinanceLeaseLiabilityPaymentsRemainderOfFiscalYear", "remainder_of_fiscal_year", 0),
        ("FinanceLeaseLiabilityPaymentsDueNextTwelveMonths", "next_12_months", 1),
        ("FinanceLeaseLiabilityPaymentsDueYearTwo", "year_2", 2),
        ("FinanceLeaseLiabilityPaymentsDueYearThree", "year_3", 3),
        ("FinanceLeaseLiabilityPaymentsDueYearFour", "year_4", 4),
        ("FinanceLeaseLiabilityPaymentsDueYearFive", "year_5", 5),
        ("FinanceLeaseLiabilityPaymentsDueAfterYearFive", "after_year_5", 6),
    ], "totals": ["FinanceLeaseLiabilityPaymentsDue"]},
    {"family": "purchase_obligations", "buckets": [
        ("PurchaseObligationDueInNextTwelveMonths", "next_12_months", 1),
        ("PurchaseObligationDueInSecondYear", "year_2", 2),
        ("PurchaseObligationDueInThirdYear", "year_3", 3),
        ("PurchaseObligationDueInFourthYear", "year_4", 4),
        ("PurchaseObligationDueInFifthYear", "year_5", 5),
        ("PurchaseObligationDueAfterFifthYear", "after_year_5", 6),
        ("UnrecordedUnconditionalPurchaseObligationBalanceOnFirstAnniversary", "next_12_months", 1),
        ("UnrecordedUnconditionalPurchaseObligationBalanceOnSecondAnniversary", "year_2", 2),
        ("UnrecordedUnconditionalPurchaseObligationBalanceOnThirdAnniversary", "year_3", 3),
        ("UnrecordedUnconditionalPurchaseObligationBalanceOnFourthAnniversary", "year_4", 4),
        ("UnrecordedUnconditionalPurchaseObligationBalanceOnFifthAnniversary", "year_5", 5),
        ("UnrecordedUnconditionalPurchaseObligationDueAfterFiveYears", "after_year_5", 6),
    ], "totals": ["PurchaseObligation", "UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount"]},
    {"family": "contractual_obligations", "buckets": [
        ("ContractualObligationDueInNextTwelveMonths", "next_12_months", 1),
        ("ContractualObligationDueInSecondYear", "year_2", 2),
        ("ContractualObligationDueInThirdYear", "year_3", 3),
        ("ContractualObligationDueInFourthYear", "year_4", 4),
        ("ContractualObligationDueInFifthYear", "year_5", 5),
        ("ContractualObligationDueAfterFifthYear", "after_year_5", 6),
    ], "totals": ["ContractualObligation"]},
]

_PURCHASE_COMMITMENT = "LongTermPurchaseCommitmentAmount"
_UNDRAWN_FACILITY = "LineOfCreditFacilityRemainingBorrowingCapacity"


def _add_years(date: str, years: int) -> str:
    return f"{int(date[:4]) + years}{date[4:]}"


def _plain_at(facts: list[dict], local: str, at: str | None) -> dict | None:
    hits = [f for f in facts if f["local"] == local and f["value"] is not None and not f["dims"] and (at is None or f["periodEnd"] == at)]
    return hits[-1] if hits else None


# ── Text classification ──────────────────────────────────────────────────────

_MONEY_RE = re.compile(r"(?:US)?\$\s?(\d[\d,]*(?:\.\d+)?)\s*(billion|million|thousand|bn|mm|m|k)?\b", _F)
_EQUITY_AWARD_RE = re.compile(r"\b(?:stock|share|equity|RSU|option|incentive)[- ](?:based )?(?:awards?|compensation|grants?)\b|\brestricted stock\b|\bRSUs?\b|\bPSUs?\b|\bvest(?:s|ed|ing)?\b", _F)
# Without an amount, a sentence is kept only when it names a funding or capex obligation outright.
_STRONG_RE = re.compile(r"\bcapital expenditures?\b|\bcapex\b|\bpurchase (?:commitments?|obligations?)\b|\bcommitments?\b|\bcommitted\b|\bnon-?cancell?able\b|\bobligated\b|\bgrants?\b|\bmilestones?\b|\bCHIPS\b|\bincentive agreement\b", _F)
_CONTINGENT_RE = re.compile(r"\bgrants?\b|\bawarded\b|\baward agreement\b|\bCHIPS\b|\bincentives?\b|\bsubsid(?:y|ies)\b|\bmilestones?\b|\bpreliminary memorandum of terms\b|\bcontingent\b|\bsubject to (?:the )?(?:achievement|completion|satisfaction|conditions?|approval)\b", _F)
_COMMITTED_RE = re.compile(r"\bcommitted\b|\bcommitments?\b|\bnon-?cancell?able\b|\bobligated\b|\bfirm purchase\b", _F)
_GUIDED_RE = re.compile(r"\bexpects?\b|\bexpected\b|\banticipates?\b|\banticipated\b|\bplans?\b|\bplanned\b|\bintends?\b|\bestimates?\b|\bestimated\b|\bprojects?\b|\bbudget(?:s|ed)?\b|\bguidance\b|\bforecasts?\b", _F)
_CAPEX_RE = re.compile(r"\bcapital expenditures?\b|\bcapex\b|\bconstruction\b|\bpurchase (?:commitments?|obligations?)\b|\binvest(?:ment)?s? (?:of|in)\b|\bspend(?:ing)?\b|\bfacilit(?:y|ies)\b|\bmanufactur\w*\b|\bsatellites?\b|\bdeploy\w*\b", _F)
_FUNDING_RE = re.compile(r"\bfund(?:s|ed|ing)?\b|\bfinanc\w*|\bliquidity\b|\bcash\b|\bproceeds\b", _F)
_RANGE_JOIN_RE = re.compile(r"^\s*(?:to|-|–|—|and)\s*$", re.I)


_PER_UNIT_RE = re.compile(r"^\s*per (?:share|unit|warrant|note)\b", _F)


def _amounts(sentence: str) -> dict:
    """The first dollar amount or range in a sentence.

    A figure below $100,000 with no scale word ("$550.0" in a filing that
    reports in millions) keeps its wording and is SCALE_NOT_STATED rather than
    read at face value.
    """
    matches = [(m.start(), m.end(), m.group(0), m.group(1), m.group(2)) for m in _MONEY_RE.finditer(sentence)
               if not _PER_UNIT_RE.match(sentence[m.end():])]
    if not matches:
        return {"low": None, "high": None, "qualifier": None, "amountStatus": "NOT_STATED", "asWritten": None}
    a = matches[0]
    b = matches[1] if len(matches) > 1 else None
    is_range = b is not None and bool(_RANGE_JOIN_RE.match(sentence[a[1]:b[0]]))
    unit = (a[4] if a[4] is not None else b[4]) if is_range else a[4]
    low = _money_value(a[3], unit)
    high = _money_value(b[3], b[4] if b[4] is not None else unit) if is_range else low
    as_written = (sentence[a[0]:b[1]] if is_range else a[2]).strip()
    before = sentence[max(0, a[0] - 24):a[0]].lower()
    if re.search(r"up to\s*$", before):
        qualifier = "up_to"
    elif re.search(r"(?:approximately|about|around|roughly|~)\s*$", before):
        qualifier = "approximately"
    else:
        qualifier = "range" if low != high else "stated"
    if unit is None and high < 100000:
        return {"low": None, "high": None, "qualifier": qualifier, "amountStatus": "SCALE_NOT_STATED", "asWritten": as_written}
    return {"low": low, "high": high, "qualifier": qualifier, "amountStatus": "STATED", "asWritten": as_written}


_ORDINAL = {"first": 1, "second": 2, "third": 3, "fourth": 4}


def timing_of(sentence: str) -> dict:
    """When a sentence says the amount falls due or is spent; UNSTATED when it does not."""
    m = re.search(r"\b(first|second|third|fourth) quarter of (?:fiscal )?(20\d\d)\b", sentence, _F)
    if m:
        return {"basis": "TEXT", "horizon": "quarter", "year": int(m.group(2)), "quarter": _ORDINAL[m.group(1).lower()], "phrase": m.group(0)}
    m = re.search(r"\bQ([1-4])\s?(?:FY)?(20\d\d)\b", sentence, re.A)
    if m:
        return {"basis": "TEXT", "horizon": "quarter", "year": int(m.group(2)), "quarter": int(m.group(1)), "phrase": m.group(0)}
    m = re.search(r"\bnext (?:12|twelve) months\b", sentence, _F)
    if m:
        return {"basis": "TEXT", "horizon": "next_12_months", "phrase": m.group(0)}
    m = re.search(r"\b(?:remainder|rest) of (?:the )?(?:fiscal )?(?:year|20\d\d)\b", sentence, _F)
    if m:
        return {"basis": "TEXT", "horizon": "remainder_of_fiscal_year", "phrase": m.group(0)}
    m = re.search(r"\b(?:fiscal )?(20\d\d) (?:and|through|to|-) (?:fiscal )?(20\d\d)\b", sentence, _F)
    if m:
        return {"basis": "TEXT", "horizon": "years", "fromYear": int(m.group(1)), "toYear": int(m.group(2)), "phrase": m.group(0)}
    m = re.search(r"\b(?:through|by|until) (?:the end of )?(?:fiscal )?(?:year )?(20\d\d)\b", sentence, _F)
    if m:
        return {"basis": "TEXT", "horizon": "through_year", "year": int(m.group(1)), "phrase": m.group(0)}
    m = re.search(r"\b(?:in|during|for) (?:the )?(?:full )?(?:fiscal )?(?:year )?(20\d\d)\b", sentence, _F)
    if m:
        return {"basis": "TEXT", "horizon": "year", "year": int(m.group(1)), "phrase": m.group(0)}
    return {"basis": "UNSTATED"}


def classify_sentence(sentence: str) -> dict | None:
    """A funding or capex sentence's classification, or None when it speaks to neither."""
    contingent = bool(_CONTINGENT_RE.search(sentence)) and not _EQUITY_AWARD_RE.search(sentence)
    capex = bool(_CAPEX_RE.search(sentence))
    committed = bool(_COMMITTED_RE.search(sentence))
    if not contingent and not capex and not committed:
        return None
    if not contingent and not committed and not _FUNDING_RE.search(sentence) and not re.search(r"\$\s?\d", sentence):
        return None
    category = "award_or_incentive" if contingent else "capital_expenditure" if capex else "commitment"
    if contingent:
        return {"classification": "AWARDED_CONTINGENT", "category": category}
    # A stated commitment stays a commitment even when the sentence also uses a forward-looking word.
    if committed:
        return {"classification": "COMPANY_DISCLOSED_COMMITTED", "category": category}
    if _GUIDED_RE.search(sentence):
        return {"classification": "COMPANY_GUIDED", "category": category}
    return {"classification": "UNRESOLVED", "category": category}


def _text_items(statements: list[dict], limit: int) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for st in statements:
        for sentence in _sentences(_collapse(st["contextText"])):
            # A sentence ending in ";" is an item of a list, usually forward-looking boilerplate.
            if len(sentence) < 40 or len(sentence) > 1200 or sentence.endswith(";"):
                continue
            cls = classify_sentence(sentence)
            if not cls:
                continue
            amt = _amounts(sentence)
            if amt["amountStatus"] == "NOT_STATED" and not _STRONG_RE.search(sentence):
                continue
            key = sentence[:200].lower()
            if key in seen:
                continue
            seen.add(key)
            classification = "UNRESOLVED" if amt["amountStatus"] == "NOT_STATED" else cls["classification"]
            out.append({
                "classification": classification,
                "statedType": cls["classification"],
                "category": cls["category"],
                "amountLow": amt["low"],
                "amountHigh": amt["high"],
                "amountQualifier": amt["qualifier"],
                "amountStatus": amt["amountStatus"],
                "amountAsWritten": amt["asWritten"],
                "currency": None if amt["amountStatus"] == "NOT_STATED" else "USD",
                "timing": timing_of(sentence),
                "unresolvedReason": ("no amount stated" if amt["amountStatus"] == "NOT_STATED" else "type not stated") if classification == "UNRESOLVED" else None,
                "evidence": {
                    "sentence": sentence[:600],
                    "sectionHeading": st.get("sectionHeading"),
                    "documentUrl": st.get("documentUrl"),
                    "filingDate": st.get("filingDate"),
                    "accessionNumber": st.get("accessionNumber"),
                },
            })
            if len(out) >= limit:
                return out
    return out


# ── Schedule ─────────────────────────────────────────────────────────────────

def funding_capex_schedule(*, ticker: str, period_end: str | None, source: dict | None, facts: list[dict], statements: list[dict],
                           balances: dict | None, atm_remaining_usd: float | None, atm_evidence: dict | None) -> dict:
    contractual: list[dict] = []
    totals: list[dict] = []
    for fam in _FAMILIES:
        seen_buckets: set[str] = set()
        for concept, bucket, offset in fam["buckets"]:
            if bucket in seen_buckets:
                continue
            hit = _plain_at(facts, concept, period_end)
            if not hit:
                continue
            seen_buckets.add(bucket)
            contractual.append({
                "classification": "CONTRACTUAL",
                "family": fam["family"],
                "bucket": bucket,
                "periodThrough": _add_years(period_end, offset) if period_end and 0 < offset < 6 else None,
                "amount": hit["value"],
                "unit": hit["unit"],
                "concept": concept,
                "periodEnd": hit["periodEnd"],
            })
        for concept in fam["totals"]:
            hit = _plain_at(facts, concept, period_end)
            if hit:
                totals.append({"family": fam["family"], "concept": concept, "amount": hit["value"], "unit": hit["unit"], "periodEnd": hit["periodEnd"]})
    # Purchase commitments per category; two values for one category are the low and high of a stated range.
    groups: dict[str, list[dict]] = {}
    for f in facts:
        if f["local"] != _PURCHASE_COMMITMENT or f["value"] is None or (period_end and f["periodEnd"] != period_end):
            continue
        # A range axis (srt:RangeAxis Minimum/Maximum) splits one commitment into its low and high.
        key = "&".join(f"{a}={m}" for a, m in sorted(f["dims"].items()) if not a.endswith("RangeAxis"))
        groups.setdefault(key, []).append(f)
    for group in groups.values():
        values = sorted(set(f["value"] for f in group))
        members = [m for a, m in group[0]["dims"].items() if not a.endswith("RangeAxis")]
        contractual.append({
            "classification": "CONTRACTUAL",
            "family": "purchase_commitment",
            "bucket": "unstated",
            "periodThrough": None,
            "amount": values[0] if len(values) == 1 else None,
            **({"amountLow": values[0], "amountHigh": values[-1], "amountBasis": "tagged_range"} if len(values) > 1 else {}),
            "unit": group[0]["unit"],
            "concept": _PURCHASE_COMMITMENT,
            "counterpartyOrCategory": " / ".join(member_label(m) for m in members) if members else None,
            "periodEnd": group[0]["periodEnd"],
        })

    items = _text_items(statements, 25)
    by_classification = {key: 0 for key in CLASSIFICATIONS}
    for row in [*contractual, *items]:
        by_classification[row["classification"]] += 1

    liquidity: list[dict] = []
    bal = balances or {}
    for key, label in (("cashAndEquivalents", "cash_and_equivalents"), ("shortTermInvestments", "short_term_investments")):
        if isinstance(bal.get(key), (int, float)) and not isinstance(bal.get(key), bool):
            liquidity.append({"source": label, "classification": "COMPANY_DISCLOSED_BALANCE", "amount": bal[key], "asOf": period_end})
    undrawn = _plain_at(facts, _UNDRAWN_FACILITY, period_end)
    if undrawn:
        liquidity.append({"source": "undrawn_credit_facility", "classification": "CONTRACTUAL_AVAILABILITY", "amount": undrawn["value"], "asOf": undrawn["periodEnd"], "concept": _UNDRAWN_FACILITY})
    if atm_remaining_usd is not None:
        liquidity.append({"source": "atm_remaining_capacity", "classification": "AVAILABLE_AT_COMPANY_DISCRETION", "amount": atm_remaining_usd, "asOf": None, "evidence": atm_evidence})

    warnings: list[dict] = []
    if not facts:
        warnings.append({"code": "NO_INLINE_XBRL", "message": "The filing carries no inline XBRL facts; contractual schedules need tagged due periods.", "severity": "warning"})
    if not contractual and facts:
        warnings.append({"code": "NO_TAGGED_SCHEDULES", "message": "No debt, lease, purchase or contractual obligation is tagged by due period at the period end.", "severity": "info"})

    return {
        "ticker": ticker.upper(),
        "status": "COMPUTED" if contractual or items else "NOT_FOUND",
        "basis": "COMPANY_DISCLOSED",
        "periodEnd": period_end,
        "source": source,
        "classifications": CLASSIFICATIONS,
        "contractual": contractual,
        "contractualTotals": totals,
        "textItems": items,
        "byClassification": by_classification,
        "liquiditySources": liquidity,
        "notes": [
            "Amounts are reported as the filing states them, each with its classification, timing and source; nothing is netted, summed across classifications, forecast or filled in.",
            "A classification of text is by the sentence's own wording; UNRESOLVED means the amount or type could not be read from it, not that nothing is owed.",
            "Liquidity sources are availability, not commitments: an ATM is capacity at the company's discretion.",
        ],
        "warnings": warnings,
        **AUTHORITY_BOUNDARY,
    }
