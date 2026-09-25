"""Company-disclosed capital structure from inline XBRL (2.3.0).

Mirrors worker/src/capital-structure.ts; scripts/test_capital_structure.py
requires identical output from both runtimes. See that file for the design
notes: the parser reads dimensional facts that companyfacts omits, and the
dilution bridge, capital-structure timeline and analyst-method extraction
built on it are mechanical views of company or published disclosures, never
forecasts.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Callable

_F = re.I | re.A
_ENTITY_RE = re.compile(r"&(#x[0-9a-f]+|#\d+|amp|lt|gt|quot|apos|nbsp);", _F)
# An explicit whitespace class, so both runtimes collapse the same characters.
_WS_RUN_RE = re.compile("[\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+")


def _collapse(text: str) -> str:
    return _WS_RUN_RE.sub(" ", text).strip(" ")


def _decode_entities(text: str) -> str:
    def sub(m: re.Match) -> str:
        lower = m.group(1).lower()
        simple = {"amp": "&", "lt": "<", "gt": ">", "quot": "\"", "apos": "'", "nbsp": " "}
        if lower in simple:
            return simple[lower]
        n = int(lower[2:], 16) if lower.startswith("#x") else int(lower[1:], 10)
        if n <= 0 or n > 0x10FFFF:
            return " "
        return " " if n == 0xA0 else chr(n)

    return _ENTITY_RE.sub(sub, text)


def _plain_text(html: str) -> str:
    return _collapse(_decode_entities(re.sub(r"<[^>]*>", " ", html)))


def _attr(attrs: str, name: str) -> str | None:
    m = re.search(rf"(?:^|\s){name}\s*=\s*(?:\"([^\"]*)\"|'([^']*)')", attrs, _F)
    if not m:
        return None
    return m.group(1) if m.group(1) is not None else m.group(2)


def _local_name(qname: str) -> str:
    i = qname.find(":")
    return qname[i + 1:] if i >= 0 else qname


_MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
    "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def _pad2(value: str) -> str:
    return f"0{value}" if len(value) == 1 else value


def normalize_ix_date(text: str | None) -> str | None:
    """A disclosed date as ISO text: yyyy-mm-dd, or yyyy-mm / yyyy when that is all the text gives."""
    if not text:
        return None
    t = re.sub(r"(\d)(?:st|nd|rd|th)\b", r"\1", _collapse(text), flags=_F)
    if re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", t, _F):
        return t
    m = re.fullmatch(r"([A-Za-z]{3,})\.? (\d{1,2}),? (\d{4})", t, _F)
    if m and _MONTHS.get(m.group(1)[:3].lower()):
        return f"{m.group(3)}-{_MONTHS[m.group(1)[:3].lower()]}-{_pad2(m.group(2))}"
    m = re.fullmatch(r"(\d{1,2}) ([A-Za-z]{3,})\.?,? (\d{4})", t, _F)
    if m and _MONTHS.get(m.group(2)[:3].lower()):
        return f"{m.group(3)}-{_MONTHS[m.group(2)[:3].lower()]}-{_pad2(m.group(1))}"
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", t, _F)
    if m:
        return f"{m.group(3)}-{_pad2(m.group(1))}-{_pad2(m.group(2))}"
    m = re.fullmatch(r"([A-Za-z]{3,})\.?,? (\d{4})", t, _F)
    if m and _MONTHS.get(m.group(1)[:3].lower()):
        return f"{m.group(2)}-{_MONTHS[m.group(1)[:3].lower()]}"
    if re.fullmatch(r"(\d{4})", t, _F):
        return t
    return None


_NUMBER_WORDS = {
    "no": 0, "none": 0, "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
    "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20, "thirty": 30, "forty": 40,
    "fifty": 50, "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
}


def _words_number(text: str) -> float | None:
    words = [w for w in re.split(r"\s+", text.lower().replace("-", " ")) if w and w != "and"]
    if not words:
        return None
    total = 0
    current = 0
    for word in words:
        if word in _NUMBER_WORDS:
            current += _NUMBER_WORDS[word]
        elif word == "hundred":
            current *= 100
        elif word == "thousand":
            total += current * 1000
            current = 0
        elif word == "million":
            total += current * 1000000
            current = 0
        else:
            return None
    return total + current


def ix_number(text: str, fmt_attr: str | None) -> float | None:
    """The number an ix:nonFraction displays, before scale and sign."""
    fmt = _local_name(fmt_attr or "").lower()
    t = _collapse(text)
    # ixt:fixed-zero and ixt:zerodash display a dash for zero.
    if "zero" in fmt:
        return 0
    if "word" in fmt:
        return _words_number(t)
    digits = re.sub("[ $\u20ac\u00a3%()]", "", t)
    if "comma-decimal" in fmt or "numcommadecimal" in fmt:
        digits = digits.replace(".", "").replace(",", ".")
    else:
        digits = digits.replace(",", "")
    if not re.fullmatch(r"\d+(?:\.\d+)?", digits, _F):
        return None
    return float(digits)


def _apply_scale(value: float, scale: str | None) -> float:
    try:
        n = int(scale) if scale else 0
    except ValueError:
        n = 0
    if n == 0:
        return value
    return value * 10 ** n if n > 0 else value / 10 ** -n


@dataclass
class IxFact:
    name: str
    local: str
    context_ref: str
    unit: str | None
    value: float | None
    text: str | None
    period_end: str | None
    period_start: str | None
    dims: dict[str, str]
    order: int


@dataclass
class IxDocument:
    facts: list[IxFact]
    context_count: int
    document_period_end: str | None
    document_type: str | None


@dataclass
class IxSource:
    role: str
    filing_type: str
    filing_date: str | None
    accession_number: str | None
    document_url: str | None
    doc: IxDocument


def _parse_contexts(html: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for m in re.finditer(r"<((?:xbrli:)?context)\b([^>]*)>(.*?)</\1\s*>", html, _F | re.S):
        ctx_id = _attr(m.group(2), "id")
        if not ctx_id:
            continue
        body = m.group(3)
        instant = re.search(r"<(?:xbrli:)?instant\s*>\s*([^<\s]+)\s*<", body, _F)
        start = re.search(r"<(?:xbrli:)?startDate\s*>\s*([^<\s]+)\s*<", body, _F)
        end = re.search(r"<(?:xbrli:)?endDate\s*>\s*([^<\s]+)\s*<", body, _F)
        dims: dict[str, str] = {}
        for d in re.finditer(r"<xbrldi:explicitMember\b([^>]*)>\s*([^<\s]+)\s*</xbrldi:explicitMember\s*>", body, _F):
            axis = _attr(d.group(1), "dimension")
            if axis:
                dims[_local_name(axis)] = d.group(2)
        for d in re.finditer(r"<xbrldi:typedMember\b([^>]*)>(.*?)</xbrldi:typedMember\s*>", body, _F | re.S):
            axis = _attr(d.group(1), "dimension")
            if axis:
                dims[_local_name(axis)] = _plain_text(d.group(2))
        out[ctx_id] = {
            "instant": instant.group(1)[:10] if instant else None,
            "start": start.group(1)[:10] if start else None,
            "end": end.group(1)[:10] if end else None,
            "dims": dims,
        }
    return out


def _parse_units(html: str) -> dict[str, str]:
    out: dict[str, str] = {}

    def measures(part: str) -> list[str]:
        return [_local_name(x.group(1)) for x in re.finditer(r"<(?:xbrli:)?measure\s*>\s*([^<\s]+)\s*<", part, _F)]

    for m in re.finditer(r"<((?:xbrli:)?unit)\b([^>]*)>(.*?)</\1\s*>", html, _F | re.S):
        unit_id = _attr(m.group(2), "id")
        if not unit_id:
            continue
        num = re.search(r"<(?:xbrli:)?unitNumerator\b[^>]*>(.*?)</(?:xbrli:)?unitNumerator\s*>", m.group(3), _F | re.S)
        den = re.search(r"<(?:xbrli:)?unitDenominator\b[^>]*>(.*?)</(?:xbrli:)?unitDenominator\s*>", m.group(3), _F | re.S)
        out[unit_id] = f"{'*'.join(measures(num.group(1)))}/{'*'.join(measures(den.group(1)))}" if num and den else "*".join(measures(m.group(3)))
    return out


_NON_NUMERIC_MAX_CHARS = 300
_NUM_OPEN = re.compile(r"<ix:nonFraction\b([^>]*?)(/?)>", _F)
_NUM_CLOSE = re.compile(r"</ix:nonFraction\s*>", _F)
_TEXT_OPEN = re.compile(r"<ix:nonNumeric\b([^>]*?)(/?)>", _F)
_TEXT_CLOSE = re.compile(r"</ix:nonNumeric\s*>", _F)
_TEXT_NESTED = re.compile(r"<ix:nonNumeric\b", _F)


def _fmt_key(value: Any) -> str:
    return "" if value is None else repr(value)


def parse_ixbrl(html: str) -> IxDocument:
    """Every numeric fact and short text fact in an inline XBRL document, with its period and dimensions."""
    contexts = _parse_contexts(html)
    units = _parse_units(html)
    facts: list[IxFact] = []
    seen: set[str] = set()

    def push(name: str, context_ref: str, unit: str | None, value: float | None, text: str | None) -> None:
        ctx = contexts.get(context_ref)
        key = f"{name}|{context_ref}|{unit or ''}|{_fmt_key(value)}|{text or ''}"
        if key in seen:
            return
        seen.add(key)
        facts.append(IxFact(
            name=name, local=_local_name(name), context_ref=context_ref, unit=unit, value=value, text=text,
            period_end=(ctx["instant"] if ctx["instant"] is not None else ctx["end"]) if ctx else None,
            period_start=ctx["start"] if ctx else None,
            dims=ctx["dims"] if ctx else {},
            order=len(facts),
        ))

    for m in _NUM_OPEN.finditer(html):
        if m.group(2) == "/":
            continue
        attrs = m.group(1)
        name = _attr(attrs, "name")
        context_ref = _attr(attrs, "contextRef")
        if not name or not context_ref:
            continue
        close = _NUM_CLOSE.search(html, m.end())
        if not close:
            continue
        value = ix_number(_plain_text(html[m.end():close.start()]), _attr(attrs, "format"))
        if value is None:
            continue
        value = _apply_scale(value, _attr(attrs, "scale"))
        if _attr(attrs, "sign") == "-":
            value = -value
        unit_ref = _attr(attrs, "unitRef")
        push(name, context_ref, units.get(unit_ref, unit_ref) if unit_ref else None, value, None)

    for m in _TEXT_OPEN.finditer(html):
        if m.group(2) == "/":
            continue
        attrs = m.group(1)
        name = _attr(attrs, "name")
        context_ref = _attr(attrs, "contextRef")
        if not name or not context_ref or re.search(r"TextBlock$|Policy", name, _F):
            continue
        close = _TEXT_CLOSE.search(html, m.end())
        if not close:
            continue
        nested = _TEXT_NESTED.search(html, m.end())
        if nested and nested.start() < close.start():
            continue
        text = _plain_text(html[m.end():close.start()])
        if not text or len(text) > _NON_NUMERIC_MAX_CHARS:
            continue
        push(name, context_ref, None, None, text)

    def dei(local: str) -> str | None:
        return next((f.text for f in facts if f.local == local and f.text is not None), None)

    period_text = dei("DocumentPeriodEndDate")
    return IxDocument(
        facts=facts,
        context_count=len(contexts),
        document_period_end=normalize_ix_date(period_text) or _latest_period_end(facts),
        document_type=dei("DocumentType"),
    )


def _latest_period_end(facts: list[IxFact]) -> str | None:
    best = None
    for f in facts:
        if f.value is None or f.dims or not f.period_end or not f.name.startswith("us-gaap:"):
            continue
        if best is None or f.period_end > best:
            best = f.period_end
    return best


# ── Fact selection ──────────────────────────────────────────────────────────

def _newest(facts: list[IxFact]) -> IxFact | None:
    best = None
    for f in facts:
        if best is None or (f.period_end or "") > (best.period_end or ""):
            best = f
    return best


def _picked(f: IxFact | None) -> dict | None:
    return {"value": f.value, "periodEnd": f.period_end, "concept": f.name, "unit": f.unit} if f is not None and f.value is not None else None


def _total(doc: IxDocument, local: str, at: str | None = None) -> dict | None:
    return _picked(_newest([f for f in doc.facts if f.local == local and f.value is not None and not f.dims and (at is None or f.period_end == at)]))


def _first_total(doc: IxDocument, locals_: list[str], at: str | None = None) -> dict | None:
    for local in locals_:
        hit = _total(doc, local, at)
        if hit:
            return hit
    return None


def _dims_key(dims: dict[str, str]) -> str:
    return "&".join(f"{k}={dims[k]}" for k in sorted(dims))


def member_label(member: str) -> str:
    """"aapl:ConvertibleSeniorNotesDue2029Member" -> "Convertible Senior Notes Due 2029"."""
    label = re.sub(r"Member$", "", _local_name(member))
    label = re.sub(r"([a-z])([A-Z])", r"\1 \2", label)
    # "ClassBCommonStock" -> "Class B Common Stock"
    label = re.sub(r"([A-Z])([A-Z][a-z])", r"\1 \2", label)
    label = re.sub(r"([A-Za-z])(\d)", r"\1 \2", label)
    label = re.sub(r"(\d)([A-Za-z])", r"\1 \2", label)
    return re.sub(r"\s+", " ", label).strip()


def round_half_up(value: float, digits: int = 0) -> float:
    f = 10 ** digits
    return math.floor(value * f + 0.5) / f


def _source_ref(source: IxSource, period_end: str | None) -> dict:
    return {
        "filingType": source.filing_type,
        "filingDate": source.filing_date,
        "accessionNumber": source.accession_number,
        "documentUrl": source.document_url,
        "periodEnd": period_end,
    }


def _find_in_sources(sources: list[IxSource], fn: Callable[[IxDocument], Any]) -> tuple[Any, IxSource] | None:
    for source in sources:
        value = fn(source.doc)
        if value is not None:
            return value, source
    return None


def _max_period(facts: list[IxFact]) -> str:
    best = ""
    for f in facts:
        if (f.period_end or "") > best:
            best = f.period_end or ""
    return best


# ── Dilution bridge ─────────────────────────────────────────────────────────

_OPTIONS_OUTSTANDING = "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingNumber"
_OPTIONS_STRIKE = "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingWeightedAverageExercisePrice"
_OPTIONS_EXERCISABLE = "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsExercisableNumber"
_RANGE_OUTSTANDING = "ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeOutstandingOptions"
_RANGE_STRIKE = "ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeOutstandingOptionsWeightedAverageExercisePrice"
_UNVESTED_AWARDS = "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber"
_AWARD_AXIS_RE = re.compile(r"Award|PlanName|Plan\b|Grant|Vesting", _F)
# The outstanding count, else the number of shares the warrants are exercisable for.
_WARRANT_COUNT_CONCEPTS = ["ClassOfWarrantOrRightOutstanding", "ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights"]
_WARRANT_STRIKE = "ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1"
_CONVERSION_PRICE = "DebtInstrumentConvertibleConversionPrice1"
_CONVERSION_RATIO = "DebtInstrumentConvertibleConversionRatio1"
_FACE_AMOUNT = "DebtInstrumentFaceAmount"
_PRINCIPAL_FALLBACK_CONCEPTS = ["DebtInstrumentCarryingAmount", "LongTermDebt", "ConvertibleNotesPayable", "ConvertibleLongTermNotesPayable", "LongTermDebtNoncurrent", "SeniorNotes"]
_DEBT_AXES = ["DebtInstrumentAxis", "LongtermDebtTypeAxis"]


def _treasury_stock(count: float, strike: float, price: float) -> float:
    return count * (1 - strike / price) if price > strike else 0


def _basic_shares(doc: IxDocument) -> dict | None:
    cover = [f for f in doc.facts if f.local == "EntityCommonStockSharesOutstanding" and f.value is not None]
    if cover:
        date = _max_period(cover)
        at_date = [f for f in cover if (f.period_end or "") == date]
        plain = next((f for f in at_date if not f.dims), None)
        classes = [{"class": member_label(next(iter(f.dims.values()), "")), "shares": f.value} for f in at_date if f.dims]
        value = plain.value if plain is not None else sum((f.value or 0) for f in at_date)
        return {"shares": value, "asOf": date or None, "concept": "dei:EntityCommonStockSharesOutstanding", "basis": "cover_page", "classes": classes}
    bs = _total(doc, "CommonStockSharesOutstanding")
    return {"shares": bs["value"], "asOf": bs["periodEnd"], "concept": bs["concept"], "basis": "balance_sheet", "classes": []} if bs else None


def _option_tranches(doc: IxDocument, at: str | None) -> list[dict]:
    out = []
    for f in doc.facts:
        if f.local != _RANGE_OUTSTANDING or f.value is None or not f.dims.get("ExercisePriceRangeAxis") or (at and f.period_end != at):
            continue
        strike = next((g for g in doc.facts if g.local == _RANGE_STRIKE and g.value is not None and g.period_end == f.period_end and _dims_key(g.dims) == _dims_key(f.dims)), None)
        if strike is None:
            continue
        out.append({"count": f.value, "strike": strike.value, "label": member_label(f.dims["ExercisePriceRangeAxis"])})
    return out


def _options_component(sources: list[IxSource], price: float) -> dict | None:
    found = _find_in_sources(sources, lambda doc: _total(doc, _OPTIONS_OUTSTANDING))
    if not found:
        return None
    outstanding, source = found
    strike = _total(source.doc, _OPTIONS_STRIKE, outstanding["periodEnd"])
    exercisable = _total(source.doc, _OPTIONS_EXERCISABLE, outstanding["periodEnd"])
    tranches = _option_tranches(source.doc, outstanding["periodEnd"])
    out: dict = {
        "component": "stock_options",
        "outstanding": outstanding["value"],
        "exercisable": exercisable["value"] if exercisable else None,
        "weightedAverageExercisePrice": strike["value"] if strike else None,
        "strikeUnit": strike["unit"] if strike else None,
        "source": _source_ref(source, outstanding["periodEnd"]),
    }
    if tranches:
        inc = sum(_treasury_stock(t["count"], t["strike"], price) for t in tranches)
        out["method"] = "treasury_stock_by_exercise_price_range"
        out["tranches"] = [{
            "range": t["label"],
            "outstanding": t["count"],
            "weightedAverageExercisePrice": t["strike"],
            "inTheMoney": price > t["strike"],
            "incrementalShares": round_half_up(_treasury_stock(t["count"], t["strike"], price)),
        } for t in tranches]
        out["incrementalShares"] = round_half_up(inc)
    elif strike:
        out["method"] = "treasury_stock_on_weighted_average_strike"
        out["inTheMoney"] = price > strike["value"]
        out["incrementalShares"] = round_half_up(_treasury_stock(outstanding["value"], strike["value"], price))
        out["note"] = "One weighted-average strike stands in for every tranche; tranche-level strikes can give a different count."
    else:
        out["method"] = "not_computed"
        out["incrementalShares"] = None
        out["note"] = "No weighted-average exercise price was tagged for the same date."
    return out


# Unvested award counts, most specific first: the us-gaap concept, then the
# same count under a company prefix, then outstanding or vested-and-expected-
# to-vest counts (AAOI tags only the last, under its own prefix).
_AWARD_COUNT_CONCEPTS = [
    (re.compile(f"^{_UNVESTED_AWARDS}$"), "nonvested"),
    (re.compile(r"OtherThanOptionsNonvestedNumber$", re.I), "nonvested"),
    (re.compile(r"(?:OtherThanOptions|Nonoption)EquityInstrumentsOutstandingNumber$", re.I), "outstanding"),
    (re.compile(r"(?:OtherThanOptions|Nonoption)EquityInstrumentsVestedAndExpectedToVest(?:Number|OutstandingNumber)?$", re.I), "vested_and_expected_to_vest"),
]


def _is_share_count(f: IxFact) -> bool:
    return f.value is not None and (f.unit is None or bool(re.search(r"shares", f.unit, re.I))) and not re.search(r"USD|EUR|GBP", f.unit or "", re.I)


def _awards_component(sources: list[IxSource], table_matches: list | None = None) -> dict | None:
    def find(doc: IxDocument):
        facts: list[IxFact] = []
        basis = "nonvested"
        for rx, label in _AWARD_COUNT_CONCEPTS:
            facts = [f for f in doc.facts if rx.search(f.local) and _is_share_count(f)]
            basis = label
            if facts:
                break
        if not facts:
            return None
        date = _max_period(facts)
        at_date = [f for f in facts if (f.period_end or "") == date]
        plain = next((f for f in at_date if not f.dims), None)
        # Without a total, sum the breakdown on the fewest award/plan axes, so a
        # type x plan split is not also counted by type alone.
        award_facts = [f for f in at_date if f.dims and all(_AWARD_AXIS_RE.search(axis) for axis in f.dims)]
        fewest = min((len(f.dims) for f in award_facts), default=None)
        axis_set = next((f for f in award_facts if len(f.dims) == fewest), None)
        set_key = "&".join(sorted(axis_set.dims)) if axis_set is not None else ""
        by_type = [f for f in award_facts if "&".join(sorted(f.dims)) == set_key]
        if plain is None and not by_type:
            return None
        return date, plain, by_type, basis, (plain or by_type[0]).name

    found = _find_in_sources(sources, find)
    if not found:
        return awards_from_table(table_matches or [])
    (date, plain, by_type, basis, concept), source = found
    breakdown = [{"awardType": " / ".join(member_label(v) for v in f.dims.values()), "unvested": f.value} for f in by_type]
    summed = sum((f.value or 0) for f in by_type)
    return {
        "component": "unvested_share_awards",
        "unvested": plain.value if plain is not None else summed,
        "breakdown": breakdown,
        "concept": concept,
        "countBasis": basis,
        "method": "gross_unvested",
        "incrementalShares": round_half_up(plain.value if plain is not None else summed),
        "note": "Unvested RSUs/PSUs are counted in full; the treasury-stock method on unrecognized compensation would count fewer, and unearned performance awards may never vest.",
        "source": _source_ref(source, date or None),
    }


_AWARD_TABLE_RE = re.compile(r"restricted stock|\bRSUs?\b|stock units?|share units?|\bPSUs?\b", _F)
_AWARD_ROW_RE = re.compile(r"^(?:unvested|nonvested|outstanding|balance)\b[^|]*?\b(?:at|as of)\s+(.+)$", _F)
_TABLE_NUMBER_RE = re.compile(r"\(?(\d{1,3}(?:,\d{3})+|\d{4,})\)?", _F)


def awards_from_table(matches: list) -> dict | None:
    """Unvested RSU count from the equity-award table rows, when the filing tags none."""
    best = None
    for match in matches:
        if not match.in_table:
            continue
        scope = f"{match.table_title or ''} {match.section_heading or ''} {match.context_text}"
        if not _AWARD_TABLE_RE.search(scope) or re.search(r"\boptions?\b", match.table_title or "", _F):
            continue
        cells = [c.strip() for c in _collapse(match.context_text).split(" | ")]
        label = str(match.row_label) if re.match(r"(?:unvested|nonvested|outstanding|balance)\b", match.row_label or "", _F) else cells[0]
        row = _AWARD_ROW_RE.match(label)
        if not row:
            continue
        date = normalize_ix_date(re.sub(r"[,.:;]+$", "", row.group(1)))
        if not date:
            continue
        number_cell = next((c for c in (re.sub(r"^\$\s*", "", c) for c in cells[1:]) if _TABLE_NUMBER_RE.fullmatch(c)), None)
        if number_cell is None:
            continue
        value = float(re.sub(r"[(),]", "", number_cell))
        if re.search(r"in thousands", scope, _F):
            value *= 1000
        if best is None or date > best["date"]:
            best = {"date": date, "value": value, "match": match, "row": match.context_text[:300]}
    if best is None:
        return None
    m = best["match"]
    return {
        "component": "unvested_share_awards",
        "unvested": best["value"],
        "breakdown": [],
        "concept": None,
        "countBasis": "filing_table_text",
        "method": "gross_unvested",
        "incrementalShares": round_half_up(best["value"]),
        "note": "Read from the filing's award table because no unvested count is tagged; check the quoted row. Counted in full, as tagged awards are.",
        "evidence": {"row": best["row"], "tableTitle": m.table_title, "sectionHeading": m.section_heading, "documentUrl": m.document_url, "filingDate": m.filing_date},
        "source": {"filingType": None, "filingDate": m.filing_date, "accessionNumber": m.accession_number, "documentUrl": m.document_url, "periodEnd": best["date"]},
    }


# Unvested warrant shares (e.g. a customer warrant that vests with purchases).
_WARRANT_UNVESTED_RE = re.compile(r"Unvested\w*NumberOfSecuritiesCalledByWarrantsOrRights$|ClassOfWarrantOrRightUnvested\w*$", re.I)


def _warrants_component(sources: list[IxSource], price: float) -> dict | None:
    def find(doc: IxDocument):
        concept = next((c for c in _WARRANT_COUNT_CONCEPTS if any(f.local == c and f.value is not None for f in doc.facts)), None)
        facts = [f for f in doc.facts if f.local == concept and f.value is not None]
        if not facts:
            return None
        groups: dict[str, IxFact] = {}
        for f in facts:
            key = _dims_key(f.dims)
            prev = groups.get(key)
            if prev is None or (f.period_end or "") > (prev.period_end or ""):
                groups[key] = f
        dimmed = [f for key, f in groups.items() if key != ""]
        return dimmed if dimmed else [groups[""]]

    found = _find_in_sources(sources, find)
    if not found:
        return None
    facts, source = found
    unvested_facts = [g for g in source.doc.facts if _WARRANT_UNVESTED_RE.search(g.local) and _is_share_count(g)]
    classes = []
    for f in facts:
        strike = _newest([g for g in source.doc.facts if g.local == _WARRANT_STRIKE and g.value is not None and _dims_key(g.dims) == _dims_key(f.dims)])
        label = " / ".join(member_label(v) for v in f.dims.values()) if f.dims else "Warrants (not itemized)"
        # Only vested warrant shares can be exercised now; the rest count in the gross total.
        unvested = _newest([g for g in unvested_facts if _dims_key(g.dims) == _dims_key(f.dims)])
        if unvested is None and len(facts) == 1:
            unvested = _newest(unvested_facts)
        exercisable = max(0, f.value - unvested.value) if unvested is not None else f.value
        classes.append({
            "class": label,
            "concept": f.name,
            "outstanding": f.value,
            "asOf": f.period_end,
            "unvested": unvested.value if unvested is not None else None,
            "unvestedAsOf": unvested.period_end if unvested is not None else None,
            "exercisable": exercisable,
            "exercisePrice": strike.value if strike else None,
            "inTheMoney": price > strike.value if strike else None,
            "incrementalShares": round_half_up(_treasury_stock(exercisable, strike.value, price)) if strike else None,
        })
    unresolved = len([c for c in classes if c["incrementalShares"] is None])
    return {
        "component": "warrants",
        "outstanding": sum((c["outstanding"] or 0) for c in classes),
        "classes": classes,
        "method": "treasury_stock_per_class_on_vested",
        "incrementalShares": None if unresolved == len(classes) else sum((c["incrementalShares"] or 0) for c in classes),
        "unresolvedClasses": unresolved,
        "source": _source_ref(source, facts[0].period_end),
    }


@dataclass
class _DebtGroup:
    key: str
    axis: str | None
    member: str | None
    facts: list[IxFact] = field(default_factory=list)


def _debt_groups(doc: IxDocument) -> list[_DebtGroup]:
    groups: dict[str, _DebtGroup] = {}
    for f in doc.facts:
        axis = next((a for a in _DEBT_AXES if f.dims.get(a) is not None), None)
        if axis is None:
            continue
        member = f.dims[axis]
        key = f"{axis}={member}"
        group = groups.get(key)
        if group is None:
            group = _DebtGroup(key, axis, member)
            groups[key] = group
        group.facts.append(f)
    by_instrument = [g for g in groups.values() if g.axis == "DebtInstrumentAxis"]
    return by_instrument if by_instrument else list(groups.values())


def _group_value(group: _DebtGroup, locals_: list[str], at: str | None = None) -> dict | None:
    for local in locals_:
        hit = _newest([f for f in group.facts if f.local == local and f.value is not None and (at is None or f.period_end == at)])
        if hit is not None:
            return _picked(hit)
    return None


def _group_text(group: _DebtGroup, local: str) -> str | None:
    best = _newest([f for f in group.facts if f.local == local and f.text is not None])
    return best.text if best is not None else None


def _convertibles_component(sources: list[IxSource], price: float) -> dict | None:
    def find(doc: IxDocument):
        groups = [g for g in _debt_groups(doc) if _group_value(g, [_CONVERSION_PRICE, _CONVERSION_RATIO]) is not None]
        if groups:
            return groups
        plain_price = _total(doc, _CONVERSION_PRICE) or _total(doc, _CONVERSION_RATIO)
        if not plain_price:
            return None
        return [_DebtGroup("", None, None, [f for f in doc.facts if not f.dims])]

    found = _find_in_sources(sources, find)
    if not found:
        return None
    groups, source = found
    instruments = []
    for g in groups:
        face_tagged = _group_value(g, [_FACE_AMOUNT])
        # Some issuers tag each issue's principal only under a carrying-amount
        # concept, often at the issue date; use it and say so.
        face = face_tagged or _group_value(g, _PRINCIPAL_FALLBACK_CONCEPTS)
        conv_price = _group_value(g, [_CONVERSION_PRICE])
        ratio = _group_value(g, [_CONVERSION_RATIO])
        implied = conv_price["value"] if conv_price else (1000 / ratio["value"] if ratio and ratio["value"] > 0 else None)
        shares = None
        basis = None
        if face and ratio and ratio["value"] > 0:
            shares = (face["value"] / 1000) * ratio["value"]
            basis = "principal / 1000 * conversion_ratio"
        elif face and conv_price and conv_price["value"] > 0:
            shares = face["value"] / conv_price["value"]
            basis = "principal / conversion_price"
        in_the_money = price >= implied if implied is not None else None
        instruments.append({
            "instrument": member_label(g.member) if g.member else "Convertible notes (not itemized)",
            "member": g.member,
            "faceAmount": face_tagged["value"] if face_tagged else None,
            "principal": face["value"] if face else None,
            "principalConcept": face["concept"] if face else None,
            "principalDate": face["periodEnd"] if face else None,
            "principalBasis": "face_amount" if face_tagged else ("tagged_amount_fallback" if face else None),
            "conversionPrice": conv_price["value"] if conv_price else (round_half_up(implied, 4) if implied is not None else None),
            "conversionPriceBasis": "tagged" if conv_price else ("1000 / conversion_ratio" if implied is not None else None),
            "conversionRatioPer1000": ratio["value"] if ratio else None,
            "maturityDate": normalize_ix_date(_group_text(g, "DebtInstrumentMaturityDate")),
            "ifConvertedShares": round_half_up(shares) if shares is not None else None,
            "ifConvertedBasis": basis,
            "inTheMoney": in_the_money,
            "incrementalShares": round_half_up(shares) if shares is not None and in_the_money else (0 if shares is not None else None),
        })
    unresolved = len([i for i in instruments if i["ifConvertedShares"] is None])
    return {
        "component": "convertible_debt",
        "instruments": instruments,
        "method": "if_converted_when_in_the_money",
        "ifConvertedShares": sum((i["ifConvertedShares"] or 0) for i in instruments),
        "incrementalShares": None if unresolved == len(instruments) else sum((i["incrementalShares"] or 0) for i in instruments),
        "unresolvedInstruments": unresolved,
        "note": "Principal is the tagged face amount, else the issue's tagged carrying amount (principalBasis says which); either can be the original principal before repurchases. Net-share or cash settlement, capped calls and make-whole adjustments are not modeled.",
        "source": _source_ref(source, None),
    }


# ── Text evidence (ATM programs, funding statements) ────────────────────────

@dataclass
class TextMatch:
    context_text: str
    section_heading: str | None
    document_url: str | None
    filing_date: str | None
    accession_number: str | None
    in_table: bool = False
    table_title: str | None = None
    row_label: str | None = None


_ATM_RE = re.compile(r"\bat[- ]the[- ]market\b|\bATM (?:program|offering|facility|agreement)\b|\b(?:equity distribution|open market sale|controlled equity offering|sales) agreement\b", _F)
_MONEY_RE = re.compile(r"(?:US)?\$\s?(\d[\d,]*(?:\.\d+)?)\s*(billion|million|thousand|bn|mm|m|k)?\b", _F)


def _money_value(amount: str, unit: str | None) -> float:
    base = float(amount.replace(",", ""))
    u = (unit or "").lower()
    if u in ("billion", "bn"):
        return base * 1e9
    if u in ("million", "mm", "m"):
        return base * 1e6
    if u in ("thousand", "k"):
        return base * 1e3
    return base


def _sentences(text: str) -> list[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+(?=[A-Z(\u201c\"])", text) if s.strip()]


def _classify_atm_clause(clause: str) -> str:
    if re.search(r"\bremain|\bavailable (?:for|to be)\b|\bunsold\b|\byet to be sold\b|\bunused\b", clause, _F):
        return "remaining_capacity"
    if re.search(r"\bsold\b|\bissued\b|\bproceeds\b", clause, _F):
        return "sold_to_date"
    if re.search(r"\bup to\b|\baggregate (?:offering|gross|sales) price\b|\baggregate of\b", clause, _F):
        return "program_size"
    return "other"


def atm_evidence(matches: list[TextMatch]) -> list[dict]:
    """ATM program amounts stated in filing text, each with the sentence it came from."""
    out: list[dict] = []
    seen: set[str] = set()
    for match in matches:
        for sentence in _sentences(_collapse(match.context_text)):
            if not _ATM_RE.search(sentence):
                continue
            for clause in re.split(r"[;,]\s+", sentence):
                for money in _MONEY_RE.finditer(clause):
                    amount = _money_value(money.group(1), money.group(2))
                    if not amount >= 100000:
                        continue
                    kind = _classify_atm_clause(clause)
                    key = f"{kind}|{amount!r}|{sentence}"
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append({
                        "kind": kind,
                        "amountUsd": amount,
                        "sentence": sentence[:600],
                        "sectionHeading": match.section_heading,
                        "documentUrl": match.document_url,
                        "filingDate": match.filing_date,
                    })
    return out


def _atm_component(evidence: list[dict], price: float) -> dict | None:
    if not evidence:
        return None
    remaining = next((e for e in evidence if e["kind"] == "remaining_capacity"), None)
    size = next((e for e in evidence if e["kind"] == "program_size"), None)
    out: dict = {
        "component": "atm_program",
        "remainingCapacityUsd": remaining["amountUsd"] if remaining else None,
        "programSizeUsd": size["amountUsd"] if size else None,
        "evidence": evidence[:6],
    }
    if remaining:
        out["method"] = "remaining_capacity / price"
        out["potentialShares"] = round_half_up(remaining["amountUsd"] / price)
        out["note"] = "Shares if the whole remaining capacity were sold at the supplied price. Issuance is at the company's discretion; this is capacity, not a plan."
    else:
        out["method"] = "not_computed"
        out["potentialShares"] = None
        out["note"] = "The filing states the program size but not the unsold remainder, so no share count is derived."
    return out


_ANTIDILUTIVE = "AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount"
_DILUTION_CONCEPT_RE = re.compile(r"Warrant|Option|Nonvested|RestrictedStock|Convertible|Antidilutive|EarningsPerShare|SharesOutstanding", _F)


def _latest_period_facts(facts: list[IxFact]) -> list[IxFact]:
    """Facts over the shortest period ending at the latest end date: the quarter in a 10-Q, the year in a 10-K."""
    end = _max_period(facts)
    at_end = [f for f in facts if (f.period_end or "") == end]
    start = ""
    for f in at_end:
        if (f.period_start or "") > start:
            start = f.period_start or ""
    return [f for f in at_end if (f.period_start or "") == start]


def _reported_eps_dilution(doc: IxDocument) -> dict | None:
    """The company's own EPS share counts and the securities it excluded as antidilutive."""
    def plain(local: str) -> IxFact | None:
        facts = _latest_period_facts([f for f in doc.facts if f.local == local and f.value is not None and not f.dims])
        return facts[0] if facts else None

    basic = plain("WeightedAverageNumberOfSharesOutstandingBasic")
    diluted = plain("WeightedAverageNumberOfDilutedSharesOutstanding")
    # Itemized on AntidilutiveSecuritiesAxis, else on whatever single axis the filer used.
    antidilutive = [f for f in doc.facts if f.local == _ANTIDILUTIVE and f.value is not None]
    on_standard_axis = [f for f in antidilutive if f.dims.get("AntidilutiveSecuritiesAxis") is not None]
    excluded = _latest_period_facts(on_standard_axis if on_standard_axis else [f for f in antidilutive if len(f.dims) == 1])
    if basic is None and diluted is None and not excluded:
        return None
    period = basic or diluted or excluded[0]
    return {
        "periodStart": period.period_start,
        "periodEnd": period.period_end,
        "weightedBasicShares": basic.value if basic else None,
        "weightedDilutedShares": diluted.value if diluted else None,
        "antidilutiveExcluded": [{"security": member_label(f.dims.get("AntidilutiveSecuritiesAxis") or next(iter(f.dims.values()))), "shares": f.value} for f in excluded],
        "note": "The company's own weighted-average EPS counts for the period, and the securities it left out as antidilutive. A cross-check on the bridge's instrument list, not a point-in-time count.",
    }


def _tagged_dilution_concepts(sources: list[IxSource]) -> list[str]:
    """Share-count concepts the filing tags, with their axes: "concept [Axis, ...]"."""
    out: list[str] = []
    for s in sources:
        for f in s.doc.facts:
            if not _is_share_count(f) or not (_DILUTION_CONCEPT_RE.search(f.local) or re.search(r"Nonoption|Unvested|Vested", f.local, re.I)):
                continue
            axes = sorted(f.dims)
            key = f"{f.name} [{', '.join(axes)}]" if axes else f.name
            if key not in out:
                out.append(key)
    return sorted(out)[:60]


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def dilution_bridge(ticker: str, price: float, price_currency: str, as_of_date: str | None,
                    sources: list[IxSource], atm_matches: list[TextMatch], award_table_matches: list[TextMatch] | None = None) -> dict:
    """Basic to diluted shares at a supplied price, from company disclosures only."""
    primary = sources[0] if sources else None
    basic_found = _find_in_sources(sources, _basic_shares)
    options = _options_component(sources, price)
    awards = _awards_component(sources, award_table_matches or [])
    warrants = _warrants_component(sources, price)
    convertibles = _convertibles_component(sources, price)
    atm = _atm_component(atm_evidence(atm_matches), price)
    components = [c for c in (options, awards, warrants, convertibles) if c is not None]
    not_disclosed = [name for name, c in (
        ("stock_options", options), ("unvested_share_awards", awards), ("warrants", warrants),
        ("convertible_debt", convertibles), ("atm_program", atm),
    ) if c is None]
    unresolved = [c["component"] for c in components if c.get("incrementalShares") is None]
    partial = [
        c["component"] for c in components
        if c.get("incrementalShares") is not None
        and float(c.get("unresolvedClasses") if c.get("unresolvedClasses") is not None else (c.get("unresolvedInstruments") or 0)) > 0
    ]
    warnings: list[dict] = []
    currency_units: list[str] = []
    for c in components:
        unit = c.get("strikeUnit")
        if isinstance(unit, str) and unit and unit.split("/")[0] not in currency_units:
            currency_units.append(unit.split("/")[0])
    for unit in currency_units:
        if unit != price_currency:
            warnings.append({"code": "PRICE_CURRENCY_MISMATCH", "message": f"Strikes are reported in {unit}; the supplied price is treated as {price_currency}.", "severity": "warning"})
    if all(not s.doc.facts for s in sources):
        warnings.append({"code": "NO_INLINE_XBRL", "message": "The filing carries no inline XBRL facts; the bridge needs tagged share and instrument counts.", "severity": "warning"})

    out: dict = {
        "ticker": ticker,
        "basis": "MECHANICAL_COMPANY_DISCLOSED",
        "decisionUse": "MECHANICAL_NOT_CONSENSUS",
        "price": {"amount": price, "currency": price_currency, "asOfDate": as_of_date, "source": "caller_supplied"},
        "sources": [{"role": s.role, **_source_ref(s, s.doc.document_period_end), "factCount": len(s.doc.facts)} for s in sources],
        "basicShares": {**basic_found[0], "source": _source_ref(basic_found[1], basic_found[0]["asOf"])} if basic_found else None,
        "components": components,
        "atmProgram": atm,
        "reportedEpsDilution": (_find_in_sources(sources, _reported_eps_dilution) or (None,))[0],
        "notDisclosed": not_disclosed,
        "unresolved": unresolved,
        "partiallyResolved": partial,
    }
    if not basic_found:
        out["status"] = "NOT_FOUND"
        out["bridge"] = None
        warnings.append({"code": "BASIC_SHARES_NOT_FOUND", "message": "No cover-page or balance-sheet share count was tagged.", "severity": "error"})
    else:
        basic = basic_found[0]["shares"]

        def inc(c: dict | None) -> float:
            return c["incrementalShares"] if c and _is_number(c.get("incrementalShares")) else 0

        diluted = basic + inc(options) + inc(awards) + inc(warrants) + inc(convertibles)
        gross = (basic
                 + (float(options.get("outstanding") or 0) if options else 0)
                 + (float(awards.get("unvested") or 0) if awards else 0)
                 + (float(warrants.get("outstanding") or 0) if warrants else 0)
                 + (float(convertibles.get("ifConvertedShares") or 0) if convertibles else 0))
        atm_shares = atm["potentialShares"] if atm and _is_number(atm.get("potentialShares")) else None
        out["bridge"] = {
            "basicShares": basic,
            "stockOptions": options["incrementalShares"] if options else None,
            "unvestedShareAwards": awards["incrementalShares"] if awards else None,
            "warrants": warrants["incrementalShares"] if warrants else None,
            "convertibleDebt": convertibles["incrementalShares"] if convertibles else None,
            "dilutedSharesAtPrice": round_half_up(diluted),
            "dilutionPctAtPrice": round_half_up(((diluted - basic) / basic) * 100, 2) if basic > 0 else None,
            "grossSharesAllInstruments": round_half_up(gross),
            "grossDilutionPct": round_half_up(((gross - basic) / basic) * 100, 2) if basic > 0 else None,
            "atmPotentialShares": atm_shares,
            "dilutedSharesAtPriceWithAtm": round_half_up(diluted + atm_shares) if atm_shares is not None else None,
            "formula": "basic + options (treasury stock) + unvested awards (gross) + warrants (treasury stock) + convertibles (if-converted when in the money)",
        }
        out["status"] = "PARTIAL" if unresolved or partial else "COMPUTED"
    out["methodology"] = [
        "Every count is a company disclosure tagged in the filing's inline XBRL; the only external input is the price you supplied.",
        "This is a mechanical bridge, not a consensus or forecast diluted share count, and must not be back-solved into one.",
        "A component missing from notDisclosed was not tagged in the filing; that is not proof the instrument does not exist.",
    ]
    if not_disclosed or unresolved:
        # What the filing does tag, so a missing component can be traced to a concept this bridge does not read.
        out["taggedDilutionConcepts"] = _tagged_dilution_concepts(sources)
    out["warnings"] = warnings
    if primary is not None and primary.doc.document_period_end:
        out["periodEnd"] = primary.doc.document_period_end
    return out


# ── Capital structure timeline ──────────────────────────────────────────────

_CASH_CONCEPTS = ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents", "Cash"]
_SHORT_TERM_INVESTMENT_CONCEPTS = ["ShortTermInvestments", "MarketableSecuritiesCurrent", "AvailableForSaleSecuritiesDebtSecuritiesCurrent"]
_SHORT_TERM_BORROWING_CONCEPTS = ["ShortTermBorrowings", "CommercialPaper"]
_DEBT_LINE_CONCEPTS = [
    "ConvertibleNotesPayableCurrent", "ConvertibleLongTermNotesPayable", "LongTermNotesPayable", "NotesPayableCurrent",
    "SeniorLongTermNotes", "LineOfCredit", "LoansPayableCurrent", "LongTermLoansPayable", "OtherLongTermDebtNoncurrent",
]
_CARRYING_CONCEPTS = ["LongTermDebt", "DebtInstrumentCarryingAmount", "LongTermDebtNoncurrent", "ConvertibleNotesPayable", "ConvertibleLongTermNotesPayable", "SeniorNotes", "NotesPayable"]
_LADDER = [
    ("LongTermDebtMaturitiesRepaymentsOfPrincipalRemainderOfFiscalYear", "remainder_of_fiscal_year", 0),
    ("LongTermDebtMaturitiesRepaymentsOfPrincipalInNextTwelveMonths", "next_12_months", 1),
    ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearTwo", "year_2", 2),
    ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearThree", "year_3", 3),
    ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFour", "year_4", 4),
    ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFive", "year_5", 5),
    ("LongTermDebtMaturitiesRepaymentsOfPrincipalAfterYearFive", "after_year_5", 6),
]


_CONVERTIBLE_TOTAL_CONCEPTS = ["ConvertibleNotesPayable", "ConvertibleDebt"]
_CONVERTIBLE_PART_CONCEPTS = ["ConvertibleNotesPayableCurrent", "ConvertibleLongTermNotesPayable", "ConvertibleDebtCurrent", "ConvertibleDebtNoncurrent"]


def _convertible_balance(doc: IxDocument, at: str | None) -> tuple[float, list[dict]] | None:
    """Convertible notes tagged as balance-sheet lines at a date: a total, else current + noncurrent."""
    whole = _first_total(doc, _CONVERTIBLE_TOTAL_CONCEPTS, at)
    if whole:
        return whole["value"], [whole]
    parts = [p for p in (_total(doc, c, at) for c in _CONVERTIBLE_PART_CONCEPTS) if p is not None]
    return (sum(p["value"] for p in parts), parts) if parts else None


def _total_debt(doc: IxDocument, at: str | None) -> dict | None:
    short_term = [p for p in (_total(doc, c, at) for c in _SHORT_TERM_BORROWING_CONCEPTS) if p is not None]

    def with_short(base: float, parts: list[dict], basis: str) -> dict:
        return {
            "value": base + sum(p["value"] for p in short_term),
            "components": [{"concept": p["concept"], "amount": p["value"]} for p in [*parts, *short_term]],
            "basis": basis,
        }

    # Convertible notes on their own balance-sheet line (AAOI) are outside the
    # long-term debt lines when they exceed them; add them so debt is complete.
    def with_convertibles(base: float, parts: list[dict], basis: str) -> dict:
        conv = _convertible_balance(doc, at)
        if conv and conv[0] > base:
            return with_short(base + conv[0], [*parts, *conv[1]], f"{basis}, plus separately reported convertible notes")
        return with_short(base, parts, basis)

    all_debt = _total(doc, "LongTermDebt", at)
    if all_debt:
        return with_convertibles(all_debt["value"], [all_debt], "LongTermDebt (current and noncurrent) plus short-term borrowings")
    cur = _total(doc, "LongTermDebtCurrent", at)
    non = _total(doc, "LongTermDebtNoncurrent", at)
    if cur or non:
        parts = [p for p in (cur, non) if p is not None]
        return with_convertibles(sum(p["value"] for p in parts), parts, "LongTermDebtCurrent + LongTermDebtNoncurrent plus short-term borrowings")
    lines = [p for p in (_total(doc, c, at) for c in _DEBT_LINE_CONCEPTS) if p is not None]
    if lines:
        return with_short(sum(p["value"] for p in lines), lines, "Sum of tagged debt lines plus short-term borrowings")
    fallback = _first_total(doc, ["ConvertibleNotesPayable", "NotesPayable", "SeniorNotes"], at)
    if fallback:
        return with_short(fallback["value"], [fallback], f"{fallback['concept']} plus short-term borrowings")
    if short_term:
        return with_short(0, [], "Short-term borrowings only")
    return None


def _add_years(date: str, years: int) -> str:
    return f"{int(date[:4]) + years}{date[4:]}"


def _instruments(doc: IxDocument, period_end: str | None) -> list[dict]:
    out: list[dict] = []
    for g in _debt_groups(doc):
        face = _group_value(g, [_FACE_AMOUNT])
        # A carrying amount is a balance only at the period end; an amount tagged on
        # another date (often the issue date) is reported as such.
        carrying = _group_value(g, _CARRYING_CONCEPTS, period_end)
        tagged = None if carrying else _group_value(g, _CARRYING_CONCEPTS)
        coupon = _group_value(g, ["DebtInstrumentInterestRateStatedPercentage"])
        effective = _group_value(g, ["DebtInstrumentInterestRateEffectivePercentage"])
        conv_price = _group_value(g, [_CONVERSION_PRICE])
        ratio = _group_value(g, [_CONVERSION_RATIO])
        maturity = normalize_ix_date(_group_text(g, "DebtInstrumentMaturityDate"))
        # A coupon alone is often a duplicate member of an instrument listed elsewhere.
        if not face and not carrying and not tagged and not maturity:
            continue
        latest = _max_period(g.facts)
        status = "reported"
        if maturity and period_end and len(maturity) == 10 and maturity < period_end:
            status = "matured_before_period_end"
        elif period_end and carrying and carrying["periodEnd"] == period_end:
            status = "outstanding_at_period_end"
        out.append({
            "instrument": member_label(g.member) if g.member else "",
            "member": g.member,
            "axis": g.axis,
            "faceAmount": face["value"] if face else None,
            "carryingAmount": carrying["value"] if carrying else None,
            "carryingAmountConcept": carrying["concept"] if carrying else None,
            "taggedAmount": tagged["value"] if tagged else None,
            "taggedAmountDate": tagged["periodEnd"] if tagged else None,
            "taggedAmountConcept": tagged["concept"] if tagged else None,
            "couponPct": round_half_up(coupon["value"] * 100, 4) if coupon else None,
            "effectiveRatePct": round_half_up(effective["value"] * 100, 4) if effective else None,
            "maturityDate": maturity,
            "convertible": conv_price is not None or ratio is not None,
            "conversionPrice": conv_price["value"] if conv_price else None,
            "conversionRatioPer1000": ratio["value"] if ratio else None,
            "latestFactDate": latest or None,
            "status": status,
        })
    out.sort(key=lambda r: (r["maturityDate"] if r["maturityDate"] is not None else "9999", str(r["instrument"])))
    return out


# (category, trigger, context the sentence must also have). The context
# requirement keeps out "next 12 months" revenue recognition, stock-award
# valuation, and capex mentioned only in passing.
_CASH_CONTEXT_RE = re.compile(r"\bcash\b|\bliquidity\b|\bcapital resources\b|\bfund(?:s|ed|ing)?\b|\bfinanc\w*|\brunway\b|\bborrowings?\b", _F)
_CAPEX_CONTEXT_RE = re.compile(r"\$\s?\d|\b(?:expects?|expected|plans?|planned|anticipates?|anticipated|intends?|budget(?:ed)?)\b", _F)
_FUNDING_CATEGORIES = [
    ("going_concern", re.compile(r"\bgoing concern\b|\bsubstantial doubt\b", _F), None),
    ("liquidity_sufficiency", re.compile(r"\bsufficient to (?:fund|meet|satisfy|finance)\b|\badequate to (?:fund|meet)\b|\bfully[- ]funded\b|\bcash runway\b|\brunway\b|\bnext (?:12|twelve) months\b", _F), _CASH_CONTEXT_RE),
    ("atm_program", _ATM_RE, None),
    ("capital_expenditure", re.compile(r"\bcapital expenditures?\b|\bcapex\b|\bpurchase commitments?\b", _F), _CAPEX_CONTEXT_RE),
    ("financing_activity", re.compile(r"\bcredit (?:facility|agreement)\b|\brevolving\b|\bterm loan\b|\bnotes due\b|\bindenture\b", _F), None),
]


def funding_statements(matches: list[TextMatch], limit: int = 10) -> list[dict]:
    """Company statements on liquidity, funding and going concern, classified by what they speak to."""
    out: list[dict] = []
    seen: set[str] = set()
    for match in matches:
        for sentence in _sentences(_collapse(match.context_text)):
            # A sentence ending in ";" is an item of a list, usually forward-looking boilerplate.
            if len(sentence) < 40 or sentence.endswith(";"):
                continue
            categories = [name for name, rx, context in _FUNDING_CATEGORIES if rx.search(sentence) and (context is None or context.search(sentence))]
            if not categories:
                continue
            key = sentence[:200].lower()
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "categories": categories,
                "statement": sentence[:600],
                "sectionHeading": match.section_heading,
                "documentUrl": match.document_url,
                "filingDate": match.filing_date,
                "accessionNumber": match.accession_number,
            })
            if len(out) >= limit:
                return out
    return out


def capital_structure(ticker: str, source: IxSource, funding_matches: list[TextMatch]) -> dict:
    """Cash, debt, instruments and maturities at the filing's period end, with the company's own funding statements."""
    doc = source.doc
    period_end = doc.document_period_end
    cash = _first_total(doc, _CASH_CONCEPTS, period_end)
    short_term = _first_total(doc, _SHORT_TERM_INVESTMENT_CONCEPTS, period_end)
    long_term_securities = _total(doc, "MarketableSecuritiesNoncurrent", period_end)
    debt = _total_debt(doc, period_end)
    instrument_rows = _instruments(doc, period_end)
    ladder = []
    for concept, bucket, offset in _LADDER:
        hit = _total(doc, concept, period_end)
        if not hit:
            continue
        ladder.append({
            "bucket": bucket,
            "periodThrough": _add_years(period_end, offset) if period_end and 0 < offset < 6 else None,
            "amount": hit["value"],
            "concept": hit["concept"],
        })
    by_year: dict[str, dict] = {}
    for row in instrument_rows:
        maturity = row["maturityDate"]
        if not maturity or row["status"] == "matured_before_period_end":
            continue
        year = maturity[:4]
        entry = by_year.get(year) or {"year": year, "faceAmount": 0, "instruments": []}
        amount = _first_not_none(row["faceAmount"], row["carryingAmount"], row["taggedAmount"])
        entry["faceAmount"] += float(amount if amount is not None else 0)
        entry["instruments"].append(str(row["instrument"]))
        by_year[year] = entry
    instrument_ladder = sorted(by_year.values(), key=lambda e: e["year"])
    liquid = (cash["value"] if cash else 0) + (short_term["value"] if short_term else 0)
    warnings: list[dict] = []
    if not doc.facts:
        warnings.append({"code": "NO_INLINE_XBRL", "message": "The filing carries no inline XBRL facts.", "severity": "warning"})
    if cash and cash["concept"].endswith("RestrictedCashEquivalents"):
        warnings.append({"code": "CASH_INCLUDES_RESTRICTED", "message": "Only a cash figure that includes restricted cash was tagged.", "severity": "info"})
    funding = funding_statements(funding_matches)
    status = ("COMPUTED" if cash and debt else "PARTIAL") if (cash or debt or instrument_rows) else "NOT_FOUND"
    return {
        "ticker": ticker,
        "status": status,
        "basis": "COMPANY_DISCLOSED",
        "decisionUse": "COMPANY_DISCLOSED_NOT_FORECAST",
        "source": _source_ref(source, period_end),
        "periodEnd": period_end,
        "balances": {
            "cashAndEquivalents": cash["value"] if cash else None,
            "cashConcept": cash["concept"] if cash else None,
            "shortTermInvestments": short_term["value"] if short_term else None,
            "shortTermInvestmentsConcept": short_term["concept"] if short_term else None,
            "marketableSecuritiesNoncurrent": long_term_securities["value"] if long_term_securities else None,
            "totalDebt": debt["value"] if debt else None,
            "totalDebtBasis": debt["basis"] if debt else None,
            "totalDebtComponents": debt["components"] if debt else [],
            "netCash": liquid - debt["value"] if cash and debt else None,
            "netCashFormula": "cash and equivalents + short-term investments - total debt (carrying amounts; leases excluded)",
        },
        "instruments": instrument_rows,
        "maturityLadder": ladder,
        "instrumentMaturitiesByYear": instrument_ladder,
        "fundingStatements": funding,
        "methodology": [
            "Balances are the filing's own tagged values at its period end; nothing is projected forward.",
            "Instrument rows come from facts dimensioned by debt instrument, which companyfacts omits; face amounts can be original principal.",
            "Funding statements are quoted from the filing so the company's own runway and sufficiency claims can be read in context.",
        ],
        "warnings": warnings,
    }


# ── Analyst valuation methods ───────────────────────────────────────────────

_BROKERS = [
    "Morgan Stanley", "Goldman Sachs", "JPMorgan", "J.P. Morgan", "Jefferies", "Needham", "Rosenblatt", "Craig-Hallum",
    "B. Riley", "Cantor Fitzgerald", "Barclays", "Citi", "Citigroup", "UBS", "BofA", "Bank of America", "Wells Fargo",
    "Deutsche Bank", "Mizuho", "Stifel", "Piper Sandler", "Raymond James", "KeyBanc", "Oppenheimer", "TD Cowen",
    "Evercore", "Bernstein", "Wedbush", "Northland", "Benchmark", "Loop Capital", "Susquehanna", "Truist", "Baird",
    "HSBC", "Macquarie", "Nomura", "Canaccord", "Roth", "Lake Street", "H.C. Wainwright", "D.A. Davidson",
    "Berenberg", "Redburn", "Peel Hunt", "Carnegie", "ABG Sundal Collier", "Pareto", "SEB", "Danske", "Handelsbanken",
    "DNB", "Liberum", "Panmure", "Shore Capital", "Cavendish", "Investec", "Numis", "Guggenheim", "BMO", "RBC",
    "Scotiabank", "CIBC", "Bernstein SocGen", "Exane", "Kepler", "Jyske", "Nordea", "Arctic",
]

_CURRENCY = "(\\$|US\\$|USD\\s?|\u00a3|GBP\\s?|GBp\\s?|GBX\\s?|\u20ac|EUR\\s?|SEK\\s?|kr\\s?|NT\\$|TWD\\s?)?"
_NUM = r"(\d[\d,]*(?:\.\d+)?)"
_SUFFIX = r"(?:\s?(p|pence|kr|SEK)\b)?"
_TARGET_FROM_TO = re.compile(rf"(?:price target|target price|\bPT\b|price objective)[^.]{{0,40}}?\bfrom\s+{_CURRENCY}\s?{_NUM}{_SUFFIX}\s+to\s+{_CURRENCY}\s?{_NUM}{_SUFFIX}", _F)
_TARGET_TO_FROM = re.compile(rf"(?:price target|target price|\bPT\b|price objective)[^.\d$\u00a3\u20ac]{{0,40}}?\bto\s+{_CURRENCY}\s?{_NUM}{_SUFFIX}\s+from\s+{_CURRENCY}\s?{_NUM}{_SUFFIX}", _F)
_TARGET_BEFORE = re.compile(r"(\$|US\$|\u00a3|\u20ac|NT\$)\s?(\d[\d,]*(?:\.\d+)?)\s+(?:price\s+)?target\b", _F)
# A number followed by "%" is a move or a rate, never a target.
_TARGET_TO = re.compile(rf"(?:price target|target price|\bPT\b|price objective)[^.\d$\u00a3\u20ac]{{0,40}}?{_CURRENCY}\s?{_NUM}(?![\d.,]*\s?%){_SUFFIX}", _F)
_PERIOD_RE = re.compile(r"\b(?:FY|CY|F)\s?'?\d{2,4}E?\b|\b[12]H\s?'?\d{2,4}E?\b|\b(?:19|20)\d\dE?\b|\bNTM\b|\bnext[- ]twelve[- ]months\b|\bforward\b", _F)
_METRIC = r"(EV\s?/\s?EBITDA|EV\s?/\s?sales|EV\s?/\s?revenue|EV\s?/\s?EBIT|P\s?/\s?E|price[- ]to[- ]earnings|price[- ]to[- ]sales|EBITDA|EBIT|sales|revenue|earnings|EPS|free cash flow|FCF|gross profit|book value|NAV)"
_MULTIPLE_FIRST = re.compile(rf"\b(\d{{1,3}}(?:\.\d+)?)\s?(?:x|times)\b([^.;]{{0,40}}?)\b{_METRIC}\b", _F)
_NOT_A_MULTIPLE_RE = re.compile(r"\b(?:than|grew|growth|increase[sd]?|faster|more)\b", _F)
_METRIC_FIRST = re.compile(rf"\b{_METRIC}(?:\s+multiple)?\s+(?:of|at)\s+(?:about\s+|roughly\s+|approximately\s+|~)?(\d{{1,3}}(?:\.\d+)?)\s?(?:x|times)\b([^.;]{{0,30}})", _F)


def _currency_code(symbol: str | None, suffix: str | None = None) -> str | None:
    s = (symbol or "").strip()
    if not s:
        x = (suffix or "").lower()
        if x in ("p", "pence"):
            return "GBp"
        if x in ("kr", "sek"):
            return "SEK"
        return None
    if s in ("$", "US$", "USD"):
        return "USD"
    if s in ("\u00a3", "GBP"):
        return "GBP"
    if s in ("GBp", "GBX"):
        return "GBp"
    if s in ("\u20ac", "EUR"):
        return "EUR"
    if s in ("SEK", "kr"):
        return "SEK"
    if s in ("NT$", "TWD"):
        return "TWD"
    return None


def _number_of(text: str) -> float:
    return float(text.replace(",", ""))


def _normalized_metric(metric: str) -> tuple[str, bool]:
    m = re.sub(r"\s+", "", metric).lower()
    if m.startswith("ev/"):
        rest = m[3:]
        return ("EBITDA" if rest == "ebitda" else "EBIT" if rest == "ebit" else "sales"), True
    if m in ("p/e", "price-to-earnings", "pricetoearnings", "earnings", "eps"):
        return "earnings", False
    if m in ("price-to-sales", "pricetosales", "revenue", "sales"):
        return "sales", False
    if m in ("fcf", "freecashflow"):
        return "free cash flow", False
    if m == "ebitda":
        return "EBITDA", False
    if m == "ebit":
        return "EBIT", False
    if m == "grossprofit":
        return "gross profit", False
    if m == "bookvalue":
        return "book value", False
    return ("NAV" if metric.upper() == "NAV" else metric), False


def _periods(text: str) -> list[str]:
    out: list[str] = []
    for m in _PERIOD_RE.finditer(text):
        v = re.sub(r"\s+", "", m.group(0)).upper()
        if v.startswith("NEXT"):
            v = "NTM"
        if v not in out:
            out.append(v)
    return out


def _percent_after(sentence: str, pattern: str) -> float | None:
    m = re.search(pattern, sentence, _F)
    return _number_of(m.group(1)) if m else None


def _first_not_none(*values: Any) -> Any:
    return next((v for v in values if v is not None), None)


def _sentence_methods(sentence: str) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()

    def add_multiple(value: str, metric: str, window: str) -> None:
        if _NOT_A_MULTIPLE_RE.search(window):
            return
        norm_metric, ev = _normalized_metric(metric)
        multiple = _number_of(value)
        key = f"{multiple!r}|{norm_metric}|{ev}"
        if key in seen or not multiple > 0:
            return
        seen.add(key)
        ev_window = bool(re.search(r"\bEV\b|enterprise value", window, _F)) or ev
        out.append({"method": "multiple", "multiple": multiple, "metric": norm_metric, "enterpriseValue": ev_window, "periods": _periods(window)})

    for m in _MULTIPLE_FIRST.finditer(sentence):
        add_multiple(m.group(1), m.group(3), f"{m.group(2)} {m.group(3)}")
    for m in _METRIC_FIRST.finditer(sentence):
        add_multiple(m.group(2), m.group(1), f"{m.group(1)} {m.group(3)}")
    if re.search(r"\bdiscounted cash[- ]flow\b|\bDCF\b", sentence, _F):
        out.append({
            "method": "DCF",
            "waccPct": _first_not_none(_percent_after(sentence, r"\bWACC\b[^%\d]{0,20}(\d{1,2}(?:\.\d+)?)\s?%"), _percent_after(sentence, r"(\d{1,2}(?:\.\d+)?)\s?%\s+WACC\b")),
            "discountRatePct": _first_not_none(_percent_after(sentence, r"\bdiscount rate\b[^%\d]{0,20}(\d{1,2}(?:\.\d+)?)\s?%"), _percent_after(sentence, r"(\d{1,2}(?:\.\d+)?)\s?%\s+discount rate\b")),
            "terminalGrowthPct": _first_not_none(_percent_after(sentence, r"\bterminal (?:growth )?(?:rate )?[^%\d]{0,20}(\d{1,2}(?:\.\d+)?)\s?%"), _percent_after(sentence, r"(\d{1,2}(?:\.\d+)?)\s?%\s+terminal growth\b")),
            "exitMultiple": _percent_after(sentence, r"\b(?:terminal|exit) multiple\b[^\d]{0,20}(\d{1,3}(?:\.\d+)?)\s?x\b"),
        })
    if re.search(r"\bsum[- ]of[- ](?:the[- ])?parts\b|\bSOTP\b", sentence, _F):
        out.append({"method": "sum_of_the_parts"})
    if re.search(r"\brisk[- ]adjusted NPV\b|\brNPV\b", sentence, _F):
        out.append({"method": "risk_adjusted_npv"})
    if re.search(r"\bprobability[- ]weighted\b|\bscenario[- ]weighted\b", sentence, _F):
        out.append({"method": "probability_weighted_scenarios"})
    if re.search(r"\bpeer (?:group )?(?:multiple|average|median)\b|\bcomparable compan(?:y|ies)\b|\bcomps\b", sentence, _F):
        out.append({"method": "peer_comparison"})
    return out


def _price_target(sentence: str) -> dict | None:
    from_to = _TARGET_FROM_TO.search(sentence)
    if from_to:
        return {
            "target": _number_of(from_to.group(5)),
            "prior": _number_of(from_to.group(2)),
            "currency": _currency_code(_first_not_none(from_to.group(4), from_to.group(1)), _first_not_none(from_to.group(6), from_to.group(3))),
        }
    to_from = _TARGET_TO_FROM.search(sentence)
    if to_from:
        return {
            "target": _number_of(to_from.group(2)),
            "prior": _number_of(to_from.group(5)),
            "currency": _currency_code(_first_not_none(to_from.group(1), to_from.group(4)), _first_not_none(to_from.group(3), to_from.group(6))),
        }
    # "$92 price target" names its number outright; try it before scanning past the phrase.
    before = _TARGET_BEFORE.search(sentence)
    if before:
        return {"target": _number_of(before.group(2)), "prior": None, "currency": _currency_code(before.group(1))}
    to = _TARGET_TO.search(sentence)
    if to:
        return {"target": _number_of(to.group(2)), "prior": None, "currency": _currency_code(to.group(1), to.group(3))}
    return None


def _firm_in(text: str, firms: list[str]) -> str | None:
    best: tuple[str, int] | None = None
    for firm in firms:
        if not firm:
            continue
        # Case-sensitive: firm names are proper nouns ("Benchmark", not "benchmark").
        m = re.search(rf"(?:^|[^A-Za-z]){re.escape(firm)}(?![A-Za-z])", text)
        if m and (best is None or m.start() < best[1] or (m.start() == best[1] and len(firm) > len(best[0]))):
            best = (firm, m.start())
    return best[0] if best else None


def _to_number(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def analyst_valuation_methods(ticker: str, items: list[dict], changes: list[dict]) -> dict:
    """Valuation methods named in news headlines and summaries: context, never model inputs."""
    change_firms = list(dict.fromkeys(f for f in (str(c.get("firm") if c.get("firm") is not None else "").strip() for c in changes) if f))
    firms = [*change_firms, *[b for b in _BROKERS if not any(c.lower() == b.lower() for c in change_firms)]]
    evidence: list[dict] = []
    firms_with_method: set[str] = set()
    seen_urls: list[str] = []
    for item in items:
        title = str(item.get("title") if item.get("title") is not None else "").strip()
        summary = str(item.get("summary") if item.get("summary") is not None else "").strip()
        url = item.get("url") if isinstance(item.get("url"), str) else None
        key = url if url is not None else title
        if not key or key in seen_urls:
            continue
        seen_urls.append(key)
        text = _collapse(f"{title}. {summary}" if summary and not summary.startswith(title) else (summary or title))
        firm = _firm_in(text, firms)
        for sentence in _sentences(text):
            methods = _sentence_methods(sentence)
            target = _price_target(sentence)
            if not methods and not (target and firm):
                continue
            if methods and firm:
                firms_with_method.add(firm)
            evidence.append({
                "firm": firm,
                "publishedAt": item.get("publishedAt"),
                "publisher": _first_not_none(item.get("originalSource"), item.get("publisher"), item.get("source")),
                "url": url,
                "title": title[:240],
                "sentence": sentence[:400],
                "priceTarget": target,
                "methods": methods,
                "methodDisclosed": bool(methods),
            })
    method_not_disclosed: list[dict] = []
    noted: set[str] = set()
    for e in evidence:
        if e["methodDisclosed"] or not e["firm"] or str(e["firm"]) in firms_with_method:
            continue
        k = f"news|{e['firm']}"
        if k in noted:
            continue
        noted.add(k)
        method_not_disclosed.append({"firm": e["firm"], "priceTarget": e["priceTarget"], "date": e["publishedAt"], "source": "news", "url": e["url"]})
    for c in changes:
        firm = str(c.get("firm") if c.get("firm") is not None else "").strip()
        target = _to_number(c.get("ptTo"))
        if not firm or not target > 0 or firm in firms_with_method or f"rating|{firm}" in noted:
            continue
        noted.add(f"rating|{firm}")
        prior = _to_number(c.get("ptFrom"))
        method_not_disclosed.append({
            "firm": firm,
            "priceTarget": {"target": target, "prior": prior if prior > 0 else None, "currency": None},
            "date": c.get("date"),
            "source": "rating_changes",
            "url": None,
        })
    method_counts: dict[str, int] = {}
    for e in evidence:
        for m in e["methods"]:
            name = str(m["method"])
            method_counts[name] = method_counts.get(name, 0) + 1
    if any(e["methodDisclosed"] for e in evidence):
        status = "FOUND"
    elif evidence or method_not_disclosed:
        status = "METHOD_NOT_DISCLOSED"
    else:
        status = "NOT_FOUND"
    return {
        "ticker": ticker,
        "status": status,
        "basis": "NEWS_TEXT_EXTRACTION",
        "decisionUse": "CONTEXT_ONLY",
        "itemsScanned": len(seen_urls),
        "ratingChangesScanned": len(changes),
        "methodCounts": method_counts,
        "evidence": evidence[:40],
        "methodNotDisclosed": method_not_disclosed,
        "caveats": [
            "Read from headlines and short summaries, not the research notes; a method named here may be one of several the analyst used.",
            "Multiples and rates are the analyst's, quoted as published. Do not treat them as consensus or back-solve targets into forecasts.",
        ],
    }


# ── Companies House ─────────────────────────────────────────────────────────

_CH_CATEGORY_LABELS = {
    "accounts": "Accounts",
    "capital": "Share capital (allotments, buybacks)",
    "mortgage": "Charges (secured lending)",
    "confirmation-statement": "Confirmation statement",
    "resolution": "Resolutions (incl. allotment authority)",
    "incorporation": "Incorporation",
    "officers": "Officers",
    "persons-with-significant-control": "Persons with significant control",
    "address": "Registered address",
    "annotation": "Annotation",
    "change-of-name": "Change of name",
    "miscellaneous": "Miscellaneous",
}


def companies_house_filings(company_number: str, payload: dict) -> list[dict]:
    """Companies House filing-history items as dated evidence rows with document links."""
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    out = []
    for item in items:
        links = item.get("links") if isinstance(item.get("links"), dict) else {}
        meta = links.get("document_metadata") if isinstance(links.get("document_metadata"), str) else None
        category = str(item.get("category") if item.get("category") is not None else "")
        values = item.get("description_values") if isinstance(item.get("description_values"), dict) else {}
        description = str(item.get("description") if item.get("description") is not None else "").replace("-", " ").strip()
        transaction_id = item.get("transaction_id") if isinstance(item.get("transaction_id"), str) else None
        pages = item.get("pages")
        out.append({
            "date": item.get("date"),
            "type": item.get("type"),
            "category": category,
            "categoryLabel": _CH_CATEGORY_LABELS.get(category, category),
            "description": description,
            "descriptionValues": values,
            "pages": pages if _is_number(pages) else None,
            "documentMetadataUrl": meta,
            "documentContentUrl": f"{meta}/content" if meta else None,
            "viewerUrl": (
                f"https://find-and-update.company-information.service.gov.uk/company/{company_number}/filing-history/{transaction_id}/document?format=pdf&download=0"
                if transaction_id else None
            ),
        })
    return out


def _normalized_company_name(name: str) -> str:
    s = re.sub(r"[^A-Z0-9 ]", " ", name.upper().replace("&", " AND "))
    s = re.sub(r"\b(?:PUBLIC LIMITED COMPANY|PLC|LIMITED|LTD|HOLDINGS?|GROUP|THE)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def pick_companies_house_match(issuer_name: str, payload: dict) -> dict | None:
    """The search result that names the issuer, preferring active companies; None when none does."""
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    wanted = _normalized_company_name(issuer_name)
    if not wanted:
        return None
    exact = [i for i in items if _normalized_company_name(str(i.get("title") if i.get("title") is not None else "")) == wanted]
    active = next((i for i in exact if str(i.get("company_status") or "") == "active"), None)
    return active or (exact[0] if exact else None)
