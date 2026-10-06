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
from dataclasses import dataclass, field, replace
from typing import Any, Callable

from yfmcp.fiscal_calendar import TEXT_DATE_SOURCE, text_date

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
    # The fact's decimals attribute; None when INF or absent (exact).
    decimals: int | None = None
    # For investment concepts outside a table, the sentence the fact sits in; else None.
    sentence: str | None = None


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


def _js_number(value: Any) -> str:
    """A number as JavaScript prints it, so messages match the Worker."""
    if isinstance(value, float) and value.is_integer() and abs(value) < 1e21:
        return str(int(value))
    return str(value)


def _parse_decimals(raw: str | None) -> int | None:
    """An ix decimals attribute as a number; INF, absent or malformed is None (exact)."""
    if raw is None:
        return None
    text = raw.strip()
    return int(text) if re.fullmatch(r"-?\d+", text, re.A) else None


# Investment facts whose surrounding sentence is kept, so one that restates
# part of cash ("classified as cash equivalents") can be recognised.
_SENTENCE_CONCEPTS = {
    "ShortTermInvestments", "MarketableSecuritiesCurrent", "AvailableForSaleSecuritiesDebtSecuritiesCurrent", "MarketableSecuritiesNoncurrent",
    "DebtSecuritiesHeldToMaturityAmortizedCostAfterAllowanceForCreditLossCurrent", "HeldToMaturitySecuritiesCurrent",
    "DebtSecuritiesHeldToMaturityExcludingAccruedInterestAfterAllowanceForCreditLossCurrent", "OtherShortTermInvestments",
}
_SENTENCE_WINDOW = 1500
_SENTENCE_MAX_CHARS = 500
_TABLE_RE = re.compile(r"<table\b[\s\S]*?</table\s*>", _F)
_SENTENCE_BREAK_RE = re.compile(r"[.!?]\s", re.A)
_SENTENCE_END_RE = re.compile(r"[.!?](?:\s|$)", re.A)


def _last_sentence_start(text: str) -> int:
    start = 0
    for m in _SENTENCE_BREAK_RE.finditer(text):
        start = m.end()
    return start


def _fact_sentence(html: str, open_: int, body_start: int, body_end: int, close_end: int, tables: list[tuple[int, int]]) -> str | None:
    """The sentence around a fact outside any table, from the raw HTML either side of it."""
    if any(a <= open_ < b for a, b in tables):
        return None
    before = html[max(0, open_ - _SENTENCE_WINDOW):open_]
    gt = before.find(">")
    lt = before.find("<")
    if gt >= 0 and (lt < 0 or gt < lt):
        before = before[gt + 1:]
    after = html[close_end:close_end + _SENTENCE_WINDOW]
    last_lt = after.rfind("<")
    if last_lt > after.rfind(">"):
        after = after[:last_lt]
    before_text = _plain_text(before)
    after_text = _plain_text(after)
    end = _SENTENCE_END_RE.search(after_text)
    head = after_text[:end.start() + 1] if end else after_text
    sentence = _collapse(f"{before_text[_last_sentence_start(before_text):]} {_plain_text(html[body_start:body_end])} {head}")
    return sentence[:_SENTENCE_MAX_CHARS] or None


def parse_ixbrl(html: str) -> IxDocument:
    """Every numeric fact and short text fact in an inline XBRL document, with its period and dimensions."""
    contexts = _parse_contexts(html)
    units = _parse_units(html)
    facts: list[IxFact] = []
    seen: set[str] = set()

    tables: list[tuple[int, int]] | None = None

    def table_spans() -> list[tuple[int, int]]:
        nonlocal tables
        if tables is None:
            tables = [(m.start(), m.end()) for m in _TABLE_RE.finditer(html)]
        return tables

    def push(name: str, context_ref: str, unit: str | None, value: float | None, text: str | None, decimals: int | None = None, sentence: str | None = None) -> None:
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
            decimals=decimals,
            sentence=sentence,
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
        sentence = (_fact_sentence(html, m.start(), m.end(), close.start(), close.end(), table_spans())
                    if _local_name(name) in _SENTENCE_CONCEPTS else None)
        push(name, context_ref, units.get(unit_ref, unit_ref) if unit_ref else None, value, None, _parse_decimals(_attr(attrs, "decimals")), sentence)

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

def _precision(decimals: int | None) -> float:
    """Decimal places a fact is accurate to; an exact (INF) fact ranks above any rounding."""
    return float("inf") if decimals is None else decimals


def _newest(facts: list[IxFact]) -> IxFact | None:
    """The latest fact; on the same date the most precise, so a statement line beats a rounded narrative figure."""
    best = None
    for f in facts:
        end = f.period_end or ""
        best_end = (best.period_end or "") if best is not None else ""
        if best is None or end > best_end or (end == best_end and _precision(f.decimals) > _precision(best.decimals)):
            best = f
    return best


def _picked(f: IxFact | None) -> dict | None:
    return {"value": f.value, "periodEnd": f.period_end, "concept": f.name, "unit": f.unit, "decimals": f.decimals, "sentence": f.sentence} if f is not None and f.value is not None else None


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
    # "December282028" -> "December 28, 2028" (2.5.24, F-011): a day and a year run together after a month name.
    label = _MONTH_DAY_YEAR_RUN_RE.sub(r"\1 \2, \3", label)
    return re.sub(r"\s+", " ", label).strip()


_MONTH_DAY_YEAR_RUN_RE = re.compile(r"\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sept?|Oct|Nov|Dec) (\d{1,2})((?:19|20)\d{2})\b", re.A)
_LABEL_DATE_RE = re.compile(r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}\b", re.A)


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


def _award_axis_options(doc: IxDocument) -> dict | None:
    """Options tagged only on award or plan axes, with no undimensioned total (AEHR tags its 316,000
    outstanding options at $5.11 only under aehr:OutstandingOptionsStockOptionTransactionsMember on
    AwardTypeAxis) (2.5.10). The latest date, on the fewest such axes, one entry per member."""
    facts = [f for f in doc.facts if f.local == _OPTIONS_OUTSTANDING and f.value is not None and f.dims
             and all(_AWARD_AXIS_RE.search(axis) for axis in f.dims)]
    if not facts:
        return None
    date = max((f.period_end or "") for f in facts)
    at_date = [f for f in facts if (f.period_end or "") == date]
    fewest = min(len(f.dims) for f in at_date)
    axes = "|".join(sorted(next(f for f in at_date if len(f.dims) == fewest).dims))
    chosen = [f for f in at_date if "|".join(sorted(f.dims)) == axes]

    def same(local: str, f: IxFact) -> IxFact | None:
        return next((g for g in doc.facts if g.local == local and g.value is not None and g.period_end == f.period_end and _dims_key(g.dims) == _dims_key(f.dims)), None)

    members = []
    for f in chosen:
        exercisable = same(_OPTIONS_EXERCISABLE, f)
        members.append({"label": " / ".join(member_label(v) for v in f.dims.values()), "count": f.value,
                        "strike": _picked(same(_OPTIONS_STRIKE, f)), "exercisable": exercisable.value if exercisable is not None else None})
    return {"periodEnd": date, "members": members}


def _options_component(sources: list[IxSource], price: float) -> dict | None:
    def find(doc: IxDocument):
        plain = _total(doc, _OPTIONS_OUTSTANDING)
        if plain:
            return {"plain": plain, "axis": None}
        axis = _award_axis_options(doc)
        return {"plain": None, "axis": axis} if axis else None

    found = _find_in_sources(sources, find)
    if not found:
        return None
    hit, source = found
    plain, axis = hit["plain"], hit["axis"]
    members = axis["members"] if axis else []
    period_end = plain["periodEnd"] if plain else axis["periodEnd"]
    outstanding_count = plain["value"] if plain else sum(m["count"] for m in members)
    strike = _total(source.doc, _OPTIONS_STRIKE, period_end) if plain else (members[0]["strike"] if len(members) == 1 else None)
    if plain:
        ex = _total(source.doc, _OPTIONS_EXERCISABLE, period_end)
        exercisable = ex["value"] if ex else None
    else:
        exercisable = sum(m["exercisable"] for m in members) if all(m["exercisable"] is not None for m in members) else None
    # Several members, each with its own strike, are priced like exercise-price ranges.
    if plain:
        tranches = _option_tranches(source.doc, period_end)
    elif len(members) > 1 and all(m["strike"] for m in members):
        tranches = [{"count": m["count"], "strike": m["strike"]["value"], "label": m["label"]} for m in members]
    else:
        tranches = []
    unit = strike["unit"] if strike else next((m["strike"]["unit"] for m in members if m["strike"]), None)
    outstanding = {"value": outstanding_count}
    out: dict = {
        "component": "stock_options",
        "outstanding": outstanding_count,
        "exercisable": exercisable,
        "weightedAverageExercisePrice": strike["value"] if strike else None,
        "strikeUnit": unit,
        **({"countBasis": "award_axis_members", "members": [
            {"member": m["label"], "outstanding": m["count"], "weightedAverageExercisePrice": m["strike"]["value"] if m["strike"] else None,
             "exercisable": m["exercisable"]} for m in members]} if axis else {}),
        "source": _source_ref(source, period_end),
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
# Vested warrant shares at the period end (MRVL tags ClassOfWarrantOrRightSharesVested
# per customer warrant: 1.2M of 4.2M, and 0 of 1.0M). Case-sensitive, so "Unvested" never matches (2.5.9).
_WARRANT_VESTED_RE = re.compile(r"^ClassOfWarrantOrRight\w*Vested(?:Number)?$")
# A vesting term tagged for a class says its shares vest on conditions; without a vested or
# unvested count, how many are exercisable is unknown, never assumed to be all of them.
_WARRANT_VESTING_TERM = "WarrantsAndRightsOutstandingVestingTerm"


# A warrant's expiry: a tagged maturity or expiration date closes a class that expired before the
# period end; a tagged term from a count dated before the period end that has since elapsed is flagged
# (the term can run from a later exercisability date, so it never closes a class) (2.5.10).
_WARRANT_EXPIRY_RE = re.compile(r"^(?:WarrantsAndRightsOutstandingMaturityDate|ClassOfWarrant\w*Expir\w*Date|Warrant\w*Expir\w*Date)$")
_WARRANT_TERM = "WarrantsAndRightsOutstandingTerm"
_TERM_WORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10}


def term_years(text: str | None) -> int | None:
    """Whole years in a tagged term: "P5Y", "5 years", "five years"; None when it is not whole years."""
    if not text:
        return None
    t = _collapse(text).lower()
    iso = re.fullmatch(r"p(\d+)y", t)
    if iso:
        return int(iso.group(1))
    m = re.fullmatch(r"(\d+|one|two|three|four|five|six|seven|eight|nine|ten)(?:\s*|-)years?", t)
    if not m:
        return None
    return int(m.group(1)) if m.group(1).isdigit() else _TERM_WORDS[m.group(1)]


def _after_period_end(doc: IxDocument, f: IxFact) -> bool:
    """A count dated after the report's period end, or tagged as a subsequent event: not a period-end instrument."""
    end = doc.document_period_end
    return any(re.search(r"SubsequentEventTypeAxis$", axis) for axis in f.dims) or (end is not None and f.period_end is not None and f.period_end > end)


# A warrant count can be an event rather than warrants outstanding (2.5.9). VRT's 2025 10-K tags
# ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights = 4,812,521 on 2024-12-06, the shares
# issued when its private placement warrants were exercised cashlessly (5,266,667 warrants exercised, tagged
# on the same date and class), in the equity statement; none of those warrants remained at 2025-12-31.
# A count on the equity-statement axis, or with a warrants-exercised count for the same class and date, is
# not read as outstanding. A count dated before the period end is still read (AAOI tags its outstanding
# Amazon warrant only at issuance) and is flagged.
_EQUITY_STATEMENT_AXIS = "StatementEquityComponentsAxis"
_WARRANT_EXERCISED_CONCEPTS = ("ClassOfWarrantOrRightNumberOfWarrantsExercised", "ClassOfWarrantOrRightExercised")


def _dims_within(inner: dict[str, str], outer: dict[str, str]) -> bool:
    return all(outer.get(axis) == member for axis, member in inner.items())


# An exercise of the class: warrants exercised or shares issued on exercise (BE tags Oracle's cashless exercise
# on 2026-05-01 as StockIssuedDuringPeriodSharesExerciseOfWarrants on the warrant's class member).
_WARRANT_EXERCISE_EVENT_RE = re.compile(r"WarrantsExercised|ExerciseOfWarrants")
_CLASS_OF_WARRANT_AXIS = "ClassOfWarrantOrRightAxis"


def _later_exercise(doc: IxDocument, f: IxFact) -> IxFact | None:
    """An exercise of the same warrant class after the count and by the period end: the count no longer describes what is outstanding."""
    axis = next((a for a in f.dims if a.endswith(_CLASS_OF_WARRANT_AXIS)), None)
    if not axis or not f.period_end:
        return None
    member = f.dims[axis]
    end = doc.document_period_end
    hits = [g for g in doc.facts if _WARRANT_EXERCISE_EVENT_RE.search(g.local) and g.value is not None and g.value > 0 and g.dims.get(axis) == member
            and g.period_end is not None and g.period_end > f.period_end and (end is None or g.period_end <= end)]
    return _newest(hits)


def _warrant_count_event(doc: IxDocument, f: IxFact) -> str | None:
    """Why a tagged warrant count is an exercise or equity movement, not warrants outstanding; None when it is a count."""
    if any(axis.endswith(_EQUITY_STATEMENT_AXIS) for axis in f.dims):
        return "EQUITY_STATEMENT_MOVEMENT"
    exercised = any(g.local in _WARRANT_EXERCISED_CONCEPTS and g.value is not None and g.period_end == f.period_end and _dims_within(g.dims, f.dims)
                    for g in doc.facts)
    if exercised:
        return "WARRANT_EXERCISE"
    return "EXERCISED_AFTER_COUNT" if _later_exercise(doc, f) else None


def warrant_count_events(sources: list[IxSource]) -> list[dict]:
    """Warrant counts not read as outstanding because they record an exercise or an equity movement."""
    out: dict[str, dict] = {}
    for source in sources:
        for f in source.doc.facts:
            if f.local not in _WARRANT_COUNT_CONCEPTS or f.value is None:
                continue
            reason = _warrant_count_event(source.doc, f)
            if not reason:
                continue
            key = f"{f.name}|{_dims_key(f.dims)}|{f.period_end}|{f.value}"
            if key in out:
                continue
            exercise = _later_exercise(source.doc, f) if reason == "EXERCISED_AFTER_COUNT" else None
            out[key] = {
                "class": " / ".join(member_label(v) for v in f.dims.values()) if f.dims else "Warrants (not itemized)",
                "concept": f.name,
                "value": f.value,
                "asOf": f.period_end,
                "reason": reason,
                **({"exercise": {"concept": exercise.name, "value": exercise.value, "date": exercise.period_end}} if exercise else {}),
                "filingType": source.filing_type,
                "accessionNumber": source.accession_number,
            }
    return list(out.values())


def _warrant_class_key(f: IxFact) -> str:
    """A warrant class's identity across filings: its class-of-warrant member, else its dimensions."""
    axis = next((a for a in f.dims if a.endswith(_CLASS_OF_WARRANT_AXIS)), None)
    return f"{axis}={f.dims[axis]}" if axis else _dims_key(f.dims)


# ── Warrant lifecycle stated in text (2.5.11) ───────────────────────────────
#
# A warrant count tagged before the period end (an issuance, a prior year end) says nothing of what
# happened since; filers often state an exercise, expiry or redemption only in text. ASTS's 10-Q counts
# 122,000 Private Placement Warrants as of 2025-12-31 and says "the remaining 122,000 Private Placement
# Warrants were exercised" in the quarter ended March 31, 2026; RKLB's 10-K counts 728,835 warrants
# issued on 2023-12-29 and says "On November 14, 2024, all 728,835 common stock warrants were exercised".
# A sentence retires a class only when it names the class (its tagged count, or its class name of two or
# more words), states the whole class exercised, expired or redeemed in the past tense, and dates that
# after the tagged count and by the period end. Anything less leaves the class counted.
WARRANT_LIFECYCLE_SEARCH_TERMS = [
    "warrants were exercised", "warrant was exercised", "were fully exercised", "was fully exercised", "exercised in full",
    "warrants expired", "warrant expired", "warrants were redeemed", "redemption of all", "redeemed all",
]
_LIFECYCLE_EVENTS = [
    ("EXERCISED", re.compile(r"\bwarrants?\b[^.]{0,160}?\b(?:were|was|have been|has been|had been)\s+(?:fully\s+|all\s+)?exercised\b"
                             r"|\bexercised\s+(?:all|the remaining|in full)\b[^.]{0,100}?\bwarrants?\b", _F)),
    ("REDEEMED", re.compile(r"\bwarrants?\b[^.]{0,160}?\b(?:were|was|have been|has been|had been)\s+(?:fully\s+)?redeemed\b"
                            r"|\bredeemed\s+(?:all|the remaining)\b[^.]{0,100}?\bwarrants?\b|\bredemption of all\b[^.]{0,100}?\bwarrants?\b", _F)),
    ("EXPIRED", re.compile(r"\bwarrants?\b[^.]{0,160}?\b(?:expired|lapsed)\b", _F)),
]
# A negated, future or conditional sentence states no event ("No Private Placement Warrants were exercised").
# Case-sensitive past the first letter, so the month "May" is not the verb "may".
_LIFECYCLE_SKIP_RE = re.compile(r"\b(?:[Nn]o|[Nn]one|[Nn]ot|[Nn]either|nor|will|would|may|might|could|shall|[Uu]nless|[Ii]f)\b", re.A)
# The whole class: all or the remaining warrants, in full; an expiry or redemption ends every unexercised warrant.
_WHOLE_CLASS_RE = re.compile(r"\b(?:all|remaining|fully|in full|in their entirety|each of the)\b", _F)
_TEXT_DATE_RE = re.compile(TEXT_DATE_SOURCE, _F)


def warrant_lifecycle_sentences(matches: list[TextMatch]) -> list[dict]:
    """Past-tense exercise, expiry and redemption sentences about warrants, each with the dates it states."""
    out: list[dict] = []
    seen: set[str] = set()
    for match in matches:
        for raw in _CLAIM_SENTENCE_SPLIT_RE.split(_collapse(match.context_text)):
            sentence = raw.strip()
            if not sentence or sentence in seen or _LIFECYCLE_SKIP_RE.search(sentence):
                continue
            event = next((name for name, rx in _LIFECYCLE_EVENTS if rx.search(sentence)), None)
            if not event:
                continue
            dates = [d for d in (text_date(m.group(1), m.group(2), m.group(3)) for m in _TEXT_DATE_RE.finditer(sentence)) if d is not None]
            if not dates:
                continue
            seen.add(sentence)
            out.append({
                "event": event,
                "sentence": sentence[:600],
                "dates": dates,
                "sectionHeading": match.section_heading,
                "documentUrl": match.document_url,
                "filingDate": match.filing_date,
                "accessionNumber": match.accession_number,
            })
    return out


def _states_count(sentence: str, count: float) -> bool:
    """"728,835" as a whole number in the text, not part of a longer one."""
    if isinstance(count, bool) or not float(count).is_integer() or count < 1000:
        return False
    return re.search(r"(?<![\d,.])" + re.escape(f"{int(count):,}") + r"(?!\d|,\d)", sentence) is not None


def _specific_class_name(f: IxFact) -> str | None:
    """The class-of-warrant member's name when it is specific ("Private Placement Warrants"), never a bare "Warrants"."""
    axis = next((a for a in f.dims if a.endswith(_CLASS_OF_WARRANT_AXIS)), None)
    if axis is None:
        return None
    name = re.sub(r"\s+", " ", member_label(f.dims[axis]).lower()).strip()
    return re.sub(r"s$", "", name) if len(name.split(" ")) >= 2 else None


def _names_class(sentence: str, f: IxFact) -> str | None:
    """How a sentence names the class of a tagged count: by the count itself or by the class name; None when it does not."""
    if f.value is not None and _states_count(sentence, f.value):
        return "STATED_COUNT"
    name = _specific_class_name(f)
    return "CLASS_NAME" if name and name in _collapse(sentence).lower() else None


def _text_retirement(f: IxFact, sentences: list[dict], period_end: str | None) -> dict | None:
    """The stated event that retired a class counted before the period end.

    A sentence naming the class that states it exercised, expired or redeemed in whole, dated after the
    count and by the period end. The earliest such event wins (ASTS's warrants were exercised in the
    first quarter, then expired in April).
    """
    if period_end is None or f.period_end is None or f.period_end >= period_end:
        return None
    best: dict | None = None
    for s in sentences:
        matched_by = _names_class(s["sentence"], f)
        if not matched_by:
            continue
        if s["event"] == "EXERCISED" and matched_by != "STATED_COUNT" and not _WHOLE_CLASS_RE.search(s["sentence"]):
            continue
        # The latest date the sentence states by the period end: "During the three months ended March 31, 2026".
        dated = sorted(d for d in s["dates"] if d <= period_end)
        event_date = dated[-1] if dated else None
        if not event_date or event_date <= f.period_end:
            continue
        if best is not None and str(best["eventDate"]) <= event_date:
            continue
        best = {"event": s["event"], "eventDate": event_date, "matchedBy": matched_by, "sentence": s["sentence"],
                "sectionHeading": s["sectionHeading"], "documentUrl": s["documentUrl"], "filingDate": s["filingDate"],
                "accessionNumber": s["accessionNumber"]}
    return best


def _unnest_warrant_counts(facts: list[IxFact]) -> tuple[list[IxFact], list[dict]]:
    """Counts whose dimensions extend another counted class's (the class and a tranche or holder axis) are parts of it.

    Parts that add up to the class (within 0.5%) replace it, keeping their own terms; parts that do not
    ("of which 104,157") are left out and the class total is counted.
    """
    def label(f: IxFact) -> str:
        return " / ".join(member_label(v) for v in f.dims.values()) if f.dims else "Warrants (not itemized)"

    def parts_of(p: IxFact) -> list[IxFact]:
        return [c for c in facts if c is not p and p.dims and len(c.dims) > len(p.dims) and _dims_within(p.dims, c.dims)]

    dropped: dict[int, dict] = {}
    # Widest classes first, so a part of a part is judged against its own class.
    for p in sorted(facts, key=lambda f: len(f.dims)):
        if id(p) in dropped:
            continue
        parts = [c for c in parts_of(p) if id(c) not in dropped]
        if not parts:
            continue
        total = sum((c.value or 0) for c in parts)
        if p.value is not None and p.value > 0 and abs(total - p.value) <= p.value * 0.005:
            dropped[id(p)] = {"class": label(p), "count": p.value, "asOf": p.period_end, "reason": "SUM_OF_COUNTED_PARTS", "parts": [label(c) for c in parts]}
        else:
            for c in parts:
                dropped[id(c)] = {"class": label(c), "count": c.value, "asOf": c.period_end, "reason": "PART_OF_COUNTED_CLASS", "partOf": label(p)}
    return [f for f in facts if id(f) not in dropped], [dropped[id(f)] for f in facts if id(f) in dropped]


def _warrants_component(sources: list[IxSource], price: float, lifecycle: list[dict] | None = None) -> dict | None:
    # Classes a newer filing shows exercised or retired; an older fallback filing must not restore them
    # (BE's 2025 10-K still counts the Oracle warrant its 2026 10-Q shows exercised).
    retired: set[str] = set()
    found = None
    for source in sources:
        doc = source.doc
        counts = [f for f in doc.facts if f.value is not None and f.local in _WARRANT_COUNT_CONCEPTS and _warrant_count_event(doc, f) is None
                  and not _after_period_end(doc, f) and _warrant_class_key(f) not in retired]
        concept = next((c for c in _WARRANT_COUNT_CONCEPTS if any(f.local == c for f in counts)), None)
        facts = [f for f in counts if f.local == concept]
        if facts:
            groups: dict[str, IxFact] = {}
            for f in facts:
                key = _dims_key(f.dims)
                prev = groups.get(key)
                if prev is None or (f.period_end or "") > (prev.period_end or ""):
                    groups[key] = f
            dimmed = [f for key, f in groups.items() if key != ""]
            found = (dimmed if dimmed else [groups[""]], source)
            break
        for f in doc.facts:
            if f.value is not None and f.local in _WARRANT_COUNT_CONCEPTS and _warrant_count_event(doc, f) is not None:
                retired.add(_warrant_class_key(f))
    if not found:
        return None
    tagged, source = found
    # A class tagged both as a total and in parts is counted once (2.5.13): JOBY tags its Delta Warrants
    # (12,833,333) and the two tranches (7,000,000 and 5,833,333); LUNR its 541,667 preferred investor
    # warrants and the 104,157 of them a related party holds.
    facts, nested = _unnest_warrant_counts(tagged)
    unvested_facts = [g for g in source.doc.facts if _WARRANT_UNVESTED_RE.search(g.local) and _is_share_count(g)]
    vested_facts = [g for g in source.doc.facts if _WARRANT_VESTED_RE.search(g.local) and g.value is not None and not _after_period_end(source.doc, g)]
    doc_end = source.doc.document_period_end

    def expiry_of(f: IxFact) -> str | None:
        hit = _newest([g for g in source.doc.facts if _WARRANT_EXPIRY_RE.search(g.local) and g.text is not None and _dims_key(g.dims) == _dims_key(f.dims)])
        return normalize_ix_date(hit.text if hit else None)

    expired = [f for f in facts if (lambda e: e is not None and doc_end is not None and e < doc_end)(expiry_of(f))]
    # A count tagged before the period end that the filing text says was since exercised, expired or redeemed (2.5.11).
    retired_in_text = [(f, stated) for f, stated in
                       ((f, _text_retirement(f, lifecycle, doc_end) if lifecycle is not None else None) for f in facts if f not in expired)
                       if stated is not None]
    live = [f for f in facts if f not in expired and not any(r[0] is f for r in retired_in_text)]
    classes = []
    for f in live:
        strike = _newest([g for g in source.doc.facts if g.local == _WARRANT_STRIKE and g.value is not None and _dims_key(g.dims) == _dims_key(f.dims)])
        label = " / ".join(member_label(v) for v in f.dims.values()) if f.dims else "Warrants (not itemized)"
        # Only vested warrant shares can be exercised now; the rest count in the gross total.
        vested = _newest([g for g in vested_facts if _dims_key(g.dims) == _dims_key(f.dims)])
        unvested = None
        if vested is None:
            unvested = _newest([g for g in unvested_facts if _dims_key(g.dims) == _dims_key(f.dims)])
            if unvested is None and len(facts) == 1:
                unvested = _newest(unvested_facts)
        vesting_term = any(g.local == _WARRANT_VESTING_TERM and _dims_key(g.dims) == _dims_key(f.dims) for g in source.doc.facts)
        if vested is not None:
            exercisable, exercisable_basis = min(f.value, vested.value), "vested_count_tagged"
        elif unvested is not None:
            exercisable, exercisable_basis = max(0, f.value - unvested.value), "outstanding_less_unvested_tagged"
        elif vesting_term:
            exercisable, exercisable_basis = None, "vesting_terms_without_vested_count"
        else:
            exercisable, exercisable_basis = f.value, "no_vesting_terms_tagged"
        period_end = source.doc.document_period_end
        expiry = expiry_of(f)
        term_hit = _newest([g for g in source.doc.facts if g.local == _WARRANT_TERM and _dims_key(g.dims) == _dims_key(f.dims)])
        years = term_years(term_hit.text if term_hit else None)
        term_end = _add_years(f.period_end, years) if years is not None and f.period_end else None
        classes.append({
            "class": label,
            "concept": f.name,
            "outstanding": f.value,
            "asOf": f.period_end,
            # Tagged at an earlier date (an issuance) and not restated at the period end.
            "countBeforePeriodEnd": period_end is not None and f.period_end is not None and f.period_end < period_end,
            # Whether the filing text was read for this earlier count's exercise, expiry or redemption (2.5.11).
            **({"lifecycleText": "NO_EVENT_STATED" if lifecycle is not None else "NOT_READ"}
               if period_end is not None and f.period_end is not None and f.period_end < period_end else {}),
            "expirationDate": expiry,
            # A term counted from the tagged date that ended before the period end: possibly expired unexercised.
            **({"termElapsedBy": term_end} if expiry is None and term_end is not None and period_end is not None and f.period_end < period_end
               and term_end < period_end else {}),
            "unvested": max(0, f.value - vested.value) if vested is not None else unvested.value if unvested is not None else None,
            "unvestedAsOf": vested.period_end if vested is not None else unvested.period_end if unvested is not None else None,
            "exercisable": exercisable,
            "exercisableBasis": exercisable_basis,
            "exercisePrice": strike.value if strike else None,
            "inTheMoney": price > strike.value if strike else None,
            # No warrants exercisable adds no shares, whatever the strike; an unknown exercisable count adds an unknown number.
            "incrementalShares": (None if exercisable is None
                                  else round_half_up(_treasury_stock(exercisable, strike.value, price)) if strike
                                  else 0 if exercisable == 0 else None),
        })
    unresolved = len([c for c in classes if c["incrementalShares"] is None])
    return {
        "component": "warrants",
        "outstanding": sum((c["outstanding"] or 0) for c in classes),
        "classes": classes,
        **({"expiredClasses": [{"class": " / ".join(member_label(v) for v in f.dims.values()) if f.dims else "Warrants (not itemized)",
                                "count": f.value, "asOf": f.period_end, "expirationDate": expiry_of(f)} for f in expired]} if expired else {}),
        **({"retiredInText": [{"class": " / ".join(member_label(v) for v in f.dims.values()) if f.dims else "Warrants (not itemized)",
                               "count": f.value, "asOf": f.period_end, **stated} for f, stated in retired_in_text]} if retired_in_text else {}),
        **({"nestedCounts": nested} if nested else {}),
        "method": "treasury_stock_per_class_on_vested",
        "incrementalShares": 0 if not classes else None if unresolved == len(classes) else sum((c["incrementalShares"] or 0) for c in classes),
        "unresolvedClasses": unresolved,
        "source": _source_ref(source, facts[0].period_end),
    }


def post_period_warrants(source: IxSource | None) -> list[dict]:
    """Warrants the primary report tags after its period end (MRVL's 59.0M customer warrant at $206.58,
    issued after the quarter): claims to quote, never counted as outstanding at the period end."""
    if source is None:
        return []
    doc = source.doc
    by_key: dict[str, IxFact] = {}
    for f in doc.facts:
        if f.value is None or f.local not in _WARRANT_COUNT_CONCEPTS or not _after_period_end(doc, f) or _warrant_count_event(doc, f) is not None:
            continue
        prev = by_key.get(_dims_key(f.dims))
        if prev is None or (f.period_end or "") > (prev.period_end or ""):
            by_key[_dims_key(f.dims)] = f
    out = []
    for f in by_key.values():
        strike = _newest([g for g in doc.facts if g.local == _WARRANT_STRIKE and g.value is not None and _dims_key(g.dims) == _dims_key(f.dims)])
        class_member = f.dims.get(_CLASS_OF_WARRANT_AXIS)
        out.append({
            "kind": "WARRANT_AFTER_PERIOD_END",
            "status": "UNQUANTIFIED",
            "class": member_label(class_member) if class_member else None,
            "shares": f.value,
            "exercisePrice": strike.value if strike else None,
            "asOf": f.period_end,
            "periodEnd": doc.document_period_end,
            "concept": f.name,
            "sentences": [],
            "leadIn": None,
            "leadOut": None,
            "documentUrl": source.document_url,
            "filingDate": source.filing_date,
            "accessionNumber": source.accession_number,
        })
    return out


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


def _group_texts(group: _DebtGroup, local: str) -> list[str]:
    """Every distinct text a concept carries on the group's newest date for it, in document order (2.5.24, F-010)."""
    hits = [f for f in group.facts if f.local == local and f.text is not None]
    best = _newest(hits)
    if best is None:
        return []
    return list(dict.fromkeys(f.text for f in hits if (f.period_end or "") == (best.period_end or "")))


def _group_values(group: _DebtGroup, local: str) -> list[float]:
    """Every distinct value a concept carries on the group's newest date for it."""
    hits = [f for f in group.facts if f.local == local and f.value is not None]
    best = _newest(hits)
    if best is None:
        return []
    return list(dict.fromkeys(f.value for f in hits if (f.period_end or "") == (best.period_end or "")))


def _group_text(group: _DebtGroup, local: str) -> str | None:
    best = _newest([f for f in group.facts if f.local == local and f.text is not None])
    return best.text if best is not None else None


# The filing's own count of shares issuable on conversion at the period end (BE tags the maximum, make-whole
# included, per note), and principal outstanding then: after conversions and repurchases the issue's face
# amount overstates both (BE's 2028 notes: $632.5M issued, $0.787M left at 2026-06-30) (2.5.9).
_SHARES_ISSUABLE_CONCEPTS = ["DebtInstrumentConvertibleNumberOfSharesAvailableForConversion", "DebtInstrumentConvertibleNumberOfEquityInstruments"]
# Principal at the period end: a face amount tagged then, else the instrument's carrying amount when it is well
# below the issue's face (conversions or repurchases), not a balance-sheet line net of discount and costs
# (LongTermDebt $290M against $300M issued is the same notes, net).
_PERIOD_END_FACE_CONCEPTS = ["DebtInstrumentFaceAmount"]
_PERIOD_END_CARRYING_CONCEPTS = ["DebtInstrumentCarryingAmount"]
_REDUCED_PRINCIPAL_SHARE = 0.95
_INSTRUMENT_AXES_RE = re.compile(r"(?:DebtInstrumentAxis|LongtermDebtTypeAxis)$")
_SUBSEQUENT_EVENT_AXIS_RE = re.compile(r"SubsequentEventTypeAxis$")
_REDEMPTION_RE = re.compile(r"Redemption|Redeem|Repurchase|Extinguish|Repaid|Repayment")
# A tagged conversion ratio that disagrees with the tagged conversion price is not the conversion rate
# (BE tags only the make-whole increase, e.g. 2.6926 against $194.97, whose rate is 5.1290 per $1,000).
_RATIO_PRICE_TOLERANCE = 0.02


def _instrument_value(group: _DebtGroup, locals_: list[str], at: str) -> dict | None:
    """A value tagged only on the instrument's own axes, at a date."""
    for local in locals_:
        hit = _newest([f for f in group.facts if f.local == local and f.value is not None and f.period_end == at
                       and all(_INSTRUMENT_AXES_RE.search(axis) for axis in f.dims)])
        if hit:
            return _picked(hit)
    return None


def normalized_ratio(ratio: float | None, conv_price: float | None) -> dict:
    """A tagged conversion ratio in shares per $1,000 of principal. Some filers tag it per $1 (LITE's
    0.0076319 against a $131.03 price is 7.6319 per $1,000): a ratio whose product with the conversion
    price is ~1 is scaled by 1,000 (RATIO_PER_1_PRINCIPAL_SCALED). One that agrees with neither scale is
    not the conversion rate (RATIO_INCONSISTENT_WITH_PRICE); without a price, a ratio below 1 has no
    provable unit (RATIO_UNIT_UNCERTAIN). The tagged value stays visible (2.5.10)."""
    if ratio is None or not ratio > 0:
        return {"per1000": None, "usable": False, "note": None}
    if conv_price is not None:
        if abs(ratio * conv_price / 1000 - 1) <= _RATIO_PRICE_TOLERANCE:
            return {"per1000": ratio, "usable": True, "note": None}
        if abs(ratio * conv_price - 1) <= _RATIO_PRICE_TOLERANCE:
            return {"per1000": round_half_up(ratio * 1000, 6), "usable": True, "note": "RATIO_PER_1_PRINCIPAL_SCALED"}
        return {"per1000": ratio, "usable": False, "note": "RATIO_INCONSISTENT_WITH_PRICE"}
    return {"per1000": ratio, "usable": True, "note": None} if ratio >= 1 else {"per1000": None, "usable": False, "note": "RATIO_UNIT_UNCERTAIN"}


# ── Principal settled in cash (2.5.11, LITE) ────────────────────────────────
#
# LITE's 10-K: "The principal amounts of all of our outstanding convertible notes must be settled in cash."
# When principal is settled in cash, conversion delivers shares only for the conversion value above
# principal: max(0, if-converted shares - principal / price) at the price. The if-converted count (the
# EPS basis) stays the bridge's count; the net-share count is reported beside it, only for notes whose
# cash settlement of principal the filing states, never assumed. Capped calls are not modeled.
CONVERTIBLE_SETTLEMENT_SEARCH_TERMS = [
    "settled in cash", "pay cash up to", "principal amount in cash", "principal amounts of all", "settle the principal", "cash equal to the aggregate principal",
]
_CASH_PRINCIPAL_RE = re.compile(
    r"\bprincipal(?: amounts?)?\b[^.]{0,160}?\b(?:must|will|shall|is required to|are required to) be (?:settled|paid) (?:solely |only )?in cash\b"
    r"|\b(?:pay|paying|deliver|delivering) cash (?:equal to|up to) the (?:aggregate )?principal amount\b"
    r"|\belected to (?:settle|pay) (?:the )?(?:aggregate )?principal(?: amounts?)?\b[^.]{0,80}?\bin cash\b", _F)
# A settlement method the issuer may still choose is not a stated cash settlement.
_SETTLEMENT_OPTION_RE = re.compile(r"\bmay\b|\bcan\b|\bat (?:our|its|the company['’]s) (?:option|election)\b", re.A)
_ALL_NOTES_RE = re.compile(r"\ball (?:of )?(?:our |the )?(?:outstanding )?(?:convertible )?(?:senior )?notes\b"
                           r"|\beach (?:series|issue) of (?:our |the )?(?:convertible )?(?:senior )?notes\b", _F)
_NOTE_YEAR_RE = re.compile(r"\b(20\d\d) (?:convertible )?(?:senior )?notes\b|\bnotes due (20\d\d)\b", _F)


def principal_cash_settlement(matches: list[TextMatch]) -> list[dict]:
    """Sentences stating convertible principal is settled in cash, with the notes they name."""
    out: list[dict] = []
    seen: set[str] = set()
    for match in matches:
        for raw in _CLAIM_SENTENCE_SPLIT_RE.split(_collapse(match.context_text)):
            sentence = raw.strip()
            if not sentence or sentence in seen or not _CASH_PRINCIPAL_RE.search(sentence) or _SETTLEMENT_OPTION_RE.search(sentence):
                continue
            seen.add(sentence)
            years: list[str] = []
            for m in _NOTE_YEAR_RE.finditer(sentence):
                y = m.group(1) or m.group(2)
                if y not in years:
                    years.append(y)
            out.append({
                "sentence": sentence[:600],
                "scope": "ALL_NOTES" if _ALL_NOTES_RE.search(sentence) else "NAMED_NOTES" if years else "UNNAMED_NOTES",
                "years": years,
                "sectionHeading": match.section_heading,
                "documentUrl": match.document_url,
                "filingDate": match.filing_date,
                "accessionNumber": match.accession_number,
            })
    return out


def _settlement_for(instrument: str, count: int, sentences: list[dict]) -> dict | None:
    """The sentence that states cash settlement of this instrument's principal: every note, its year, or the only note."""
    hit = next((s for s in sentences if s["scope"] == "ALL_NOTES"), None)
    if hit is None:
        hit = next((s for s in sentences if s["scope"] == "NAMED_NOTES" and any(y in instrument for y in s["years"])), None)
    if hit is None and count == 1:
        hit = next((s for s in sentences if s["scope"] == "UNNAMED_NOTES"), None)
    return hit


# ── Capped calls (2.5.12) ───────────────────────────────────────────────────
#
# A capped call bought with a convertible delivers to the company, at settlement, the value of the
# covered shares between the strike and the cap: covered x (min(price, cap) - strike) / price shares at
# the price. BE's 10-K: "The Capped Calls have an initial strike price of approximately $18.85 per share
# … The number of shares underlying the Capped Calls is 33,549,508 shares … The cap price of the Capped
# Calls is initially $26.46 per share", and they "were not impacted by the induced conversion" of the
# notes. The offset is economic, not the EPS count (capped calls are antidilutive and excluded from
# diluted EPS); it is computed only when the strike, the cap and the covered shares are all stated.
# LITE states the cap ($268.24) and that its 2032 capped calls cover the shares that initially underlie
# the 2032 Notes, but no strike: that capped call is reported and left unresolved.
CAPPED_CALL_SEARCH_TERMS = ["cap price", "capped call"]
_CAPPED_CALL_RE = re.compile(r"\bcapped calls?\b", _F)
_CAP_PRICE_RE = re.compile(r"\bcap price\b[^.$]{0,80}?\$\s?(\d[\d,]*(?:\.\d+)?)", _F)
_CC_STRIKE_RE = re.compile(r"\bstrike price\b[^.$]{0,80}?\$\s?(\d[\d,]*(?:\.\d+)?)", _F)
_CC_STRIKE_IS_CONVERSION_RE = re.compile(r"\bstrike price\b[^.]{0,120}?\bcorrespond(?:s|ing)? to the (?:initial )?conversion price\b", _F)
# "The number of shares underlying the Capped Calls is 33,549,508 shares" (BE); "covering approximately 69.3 million shares" (RKLB).
_CC_COUNT_RE = re.compile(r"\bnumber of shares\b[^.\d]{0,80}?\bunderlying the capped calls?\b\D{0,30}?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)? million)"
                          r"|\bcover(?:s|ing|ed)?\b\D{0,60}?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)? million) shares\b", _F)
# The capped calls outlast conversions of their notes: "were not impacted by the induced conversion" (BE).
_CC_SURVIVE_RE = re.compile(r"\bcapped calls?\b[^.]{0,80}?\b(?:were|was|have|has)(?: not been| not)\s+(?:impacted|affected|terminated|unwound|settled)\b"
                            r"|\bcapped calls?\b[^.]{0,80}?\bremain(?:s|ed)? outstanding\b", _F)
_CC_UNDERLIE_RE = re.compile(r"\bcover\w*\b[^.]{0,200}?\b(?:initially )?underl(?:ie|ies|ying)\b[^.]{0,60}?\bnotes\b", _F)
_CC_YEAR_RE = re.compile(r"\b(20\d\d) capped calls?\b|\bnotes due (?:[A-Z][a-z]+ )?(20\d\d)\b|\b(20\d\d) (?:convertible )?(?:senior )?notes\b", _F)


def _dollars(m: re.Match | None) -> float | None:
    return float(m.group(1).replace(",", "")) if m else None


def _shares_stated(text: str) -> float:
    """"33,549,508" -> 33549508; "69.3 million" -> 69300000."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?) million", text, _F)
    return round_half_up(float(m.group(1)) * 1_000_000) if m else int(text.replace(",", ""))


def capped_call_terms(matches: list[TextMatch]) -> list[dict]:
    """Capped call terms stated in filing text, merged per set of notes named (by year) in the passage."""
    groups: dict[str, dict] = {}
    for match in matches:
        text = _collapse(match.context_text)
        if not _CAPPED_CALL_RE.search(text):
            continue
        years = sorted({next(y for y in m.groups() if y) for m in _CC_YEAR_RE.finditer(text)})
        key = "+".join(years)
        g = groups.get(key)
        if g is None:
            g = {"years": years, "strikePrice": None, "strikeIsConversionPrice": False, "capPrice": None, "coveredShares": None,
                 "coversNoteShares": False, "survivesConversions": False, "sentences": [], "documentUrl": match.document_url,
                 "filingDate": match.filing_date, "accessionNumber": match.accession_number}
            groups[key] = g
        # Each term is read from one sentence about the capped calls (or the sentence after one that names them).
        items = [x.strip() for x in _CLAIM_SENTENCE_SPLIT_RE.split(text)]
        about = [x for i, x in enumerate(items) if _CAPPED_CALL_RE.search(x) or (i > 0 and _CAPPED_CALL_RE.search(items[i - 1]))]

        def apply(field: str, m: re.Match, g: dict = g) -> None:
            if field == "strikePrice" and g["strikePrice"] is None:
                g["strikePrice"] = _dollars(m)
            elif field == "strikeIsConversionPrice":
                g["strikeIsConversionPrice"] = True
            elif field == "capPrice" and g["capPrice"] is None:
                g["capPrice"] = _dollars(m)
            elif field == "coveredShares" and g["coveredShares"] is None:
                g["coveredShares"] = _shares_stated(m.group(1) or m.group(2))
            elif field == "coversNoteShares":
                g["coversNoteShares"] = True
            elif field == "survivesConversions":
                g["survivesConversions"] = True

        for field, rx in (("strikePrice", _CC_STRIKE_RE), ("strikeIsConversionPrice", _CC_STRIKE_IS_CONVERSION_RE), ("capPrice", _CAP_PRICE_RE),
                          ("coveredShares", _CC_COUNT_RE), ("coversNoteShares", _CC_UNDERLIE_RE), ("survivesConversions", _CC_SURVIVE_RE)):
            for sentence in about:
                m = rx.search(sentence)
                if not m:
                    continue
                apply(field, m)
                # The sentence that states it, quoted once.
                if sentence not in g["sentences"] and len(g["sentences"]) < 5:
                    g["sentences"].append(sentence[:600])
                break
    return [g for g in groups.values()
            if g["capPrice"] is not None or g["strikePrice"] is not None or g["coveredShares"] is not None or g["coversNoteShares"]]


def _capped_call_for(instrument: str, count: int, terms: list[dict]) -> dict | None:
    """The capped call terms for a note: the passage naming its year, else the only passage when there is one note."""
    hit = next((t for t in terms if len(t["years"]) == 1 and t["years"][0] in instrument), None)
    if hit is None and count == 1:
        hit = next((t for t in terms if not t["years"]), None)
    return hit


def _capped_call_at(t: dict, note: dict, price: float) -> dict:
    """A note's capped call at the price: its stated terms and the shares its value between strike and cap offsets."""
    conversion = note.get("conversionPrice") if _is_number(note.get("conversionPrice")) else None
    strike = t["strikePrice"] if t["strikePrice"] is not None else (conversion if t["strikeIsConversionPrice"] else None)
    strike_basis = "STATED" if t["strikePrice"] is not None else "STATED_AS_CONVERSION_PRICE" if strike is not None else "NOT_STATED"
    note_shares = note.get("ifConvertedShares") if _is_number(note.get("ifConvertedShares")) else None
    # A stated count above the notes' shares today (issue-date coverage after conversions) is used only when the
    # filing says the capped calls outlasted those conversions (BE); otherwise the notes' shares bound it (RKLB).
    stated_exceeds = t["coveredShares"] is not None and note_shares is not None and t["coveredShares"] > note_shares * 1.01
    if t["coveredShares"] is not None and not (stated_exceeds and not t["survivesConversions"]):
        covered = t["coveredShares"]
    elif t["coversNoteShares"] or stated_exceeds:
        covered = note_shares
    else:
        covered = None
    if covered is None:
        coverage_basis = "NOT_STATED"
    elif covered == t["coveredShares"]:
        coverage_basis = "STATED_COUNT_SURVIVES_CONVERSIONS" if stated_exceeds else "STATED_COUNT"
    else:
        coverage_basis = "SHARES_UNDERLYING_NOTES_AT_PERIOD_END_BELOW_STATED_COUNT" if stated_exceeds else "SHARES_UNDERLYING_NOTES_AT_PERIOD_END"
    # Stable enums: unresolvedReason is CAPPED_CALL_TERMS_INCOMPLETE and missingTerms names each term not stated.
    missing = [x for x, v in (("STRIKE_PRICE", strike), ("CAP_PRICE", t["capPrice"]), ("COVERED_SHARES", covered)) if v is None]
    offset = None if missing else round_half_up(covered * max(0, min(price, t["capPrice"]) - strike) / price)
    out = {
        "strikePrice": strike,
        "strikeBasis": strike_basis,
        "capPrice": t["capPrice"],
        "coveredShares": covered,
        "coverageBasis": coverage_basis,
        "statedCoveredShares": t["coveredShares"],
        "offsetSharesAtPrice": offset,
    }
    if missing:
        out["unresolvedReason"] = "CAPPED_CALL_TERMS_INCOMPLETE"
        out["missingTerms"] = missing
    out.update({"sentences": t["sentences"], "documentUrl": t["documentUrl"], "filingDate": t["filingDate"], "accessionNumber": t["accessionNumber"]})
    return out


def _convertibles_component(sources: list[IxSource], price: float, settlement: list[dict] | None = None,
                            capped_calls: list[dict] | None = None) -> dict | None:
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
    end = source.doc.document_period_end
    instruments = []
    for g in groups:
        face_tagged = _group_value(g, [_FACE_AMOUNT])
        face_at_end = _instrument_value(g, _PERIOD_END_FACE_CONCEPTS, end) if end and g.member else None
        carrying_at_end = _instrument_value(g, _PERIOD_END_CARRYING_CONCEPTS, end) if end and g.member and not face_at_end else None
        current = face_at_end or (carrying_at_end if carrying_at_end and (face_tagged is None or carrying_at_end["value"] < _REDUCED_PRINCIPAL_SHARE * face_tagged["value"])
                                  else None)
        # Some issuers tag each issue's principal only under a carrying-amount
        # concept, often at the issue date; use it and say so.
        face = current or face_tagged or _group_value(g, _PRINCIPAL_FALLBACK_CONCEPTS)
        issuable = _instrument_value(g, _SHARES_ISSUABLE_CONCEPTS, end) if end and g.member else None
        conv_price = _group_value(g, [_CONVERSION_PRICE])
        ratio = _group_value(g, [_CONVERSION_RATIO])
        norm = normalized_ratio(ratio["value"] if ratio else None, conv_price["value"] if conv_price else None)
        usable_ratio = {**ratio, "value": norm["per1000"]} if norm["usable"] else None
        implied = conv_price["value"] if conv_price else (1000 / usable_ratio["value"] if usable_ratio else None)
        shares = None
        basis = None
        if issuable:
            shares = issuable["value"]
            basis = "shares_issuable_tagged_at_period_end"
        elif face and usable_ratio:
            shares = (face["value"] / 1000) * usable_ratio["value"]
            basis = "principal / 1000 * conversion_ratio"
        elif face and conv_price and conv_price["value"] > 0:
            shares = face["value"] / conv_price["value"]
            basis = "principal / conversion_price"
        redemption = _newest([f for f in g.facts if _REDEMPTION_RE.search(f.local) and any(_SUBSEQUENT_EVENT_AXIS_RE.search(axis) for axis in f.dims)])
        in_the_money = price >= implied if implied is not None else None
        inst = {
            "instrument": member_label(g.member) if g.member else "Convertible notes (not itemized)",
            "member": g.member,
            "faceAmount": face_tagged["value"] if face_tagged else None,
            "principal": face["value"] if face else None,
            "principalConcept": face["concept"] if face else None,
            "principalDate": face["periodEnd"] if face else None,
            "principalBasis": "outstanding_at_period_end" if current else ("face_amount" if face_tagged else ("tagged_amount_fallback" if face else None)),
            "conversionPrice": conv_price["value"] if conv_price else (round_half_up(implied, 4) if implied is not None else None),
            "conversionPriceBasis": "tagged" if conv_price else ("1000 / conversion_ratio" if implied is not None else None),
            "conversionRatioPer1000": norm["per1000"],
            "conversionRatioTagged": ratio["value"] if ratio else None,
            "conversionRatioUsed": usable_ratio is not None and not issuable,
        }
        if norm["note"]:
            inst["conversionRatioNote"] = norm["note"]
        inst.update({
            "maturityDate": normalize_ix_date(_group_text(g, "DebtInstrumentMaturityDate")),
            "ifConvertedShares": round_half_up(shares) if shares is not None else None,
            "ifConvertedBasis": basis,
            "sharesIssuableDate": issuable["periodEnd"] if issuable else None,
        })
        if redemption:
            inst["afterPeriodEnd"] = {"concept": redemption.name, "date": redemption.period_end}
        inst.update({
            "inTheMoney": in_the_money,
            "incrementalShares": round_half_up(shares) if shares is not None and in_the_money else (0 if shares is not None else None),
        })
        instruments.append(inst)
    # Stated cash settlement of principal: the shares for the conversion value above principal at the price (2.5.11).
    if settlement is not None:
        for inst in instruments:
            stated = _settlement_for(str(inst["instrument"]), len(instruments), settlement)
            if not stated or inst["ifConvertedShares"] is None:
                nss = None
            elif not inst["inTheMoney"]:
                nss = 0
            else:
                nss = round_half_up(max(0, inst["ifConvertedShares"] - inst["principal"] / price)) if inst["principal"] is not None else None
            inst["principalSettlement"] = ({"stated": "PRINCIPAL_IN_CASH", "scope": stated["scope"], "sentence": stated["sentence"],
                                            "sectionHeading": stated["sectionHeading"], "documentUrl": stated["documentUrl"],
                                            "filingDate": stated["filingDate"], "accessionNumber": stated["accessionNumber"]} if stated else None)
            inst["netShareSettlementShares"] = nss
    # Capped call terms the filing states for each note (2.5.12).
    if capped_calls is not None:
        for inst in instruments:
            terms = _capped_call_for(str(inst["instrument"]), len(instruments), capped_calls)
            inst["cappedCall"] = _capped_call_at(terms, inst, price) if terms else None
    calls = [i["cappedCall"] for i in instruments if i.get("cappedCall") is not None]
    resolved_calls = [c for c in calls if _is_number(c.get("offsetSharesAtPrice"))]
    unresolved = len([i for i in instruments if i["ifConvertedShares"] is None])
    cash_settled = [i for i in instruments if i.get("principalSettlement") is not None]
    nss_unresolved = any(i.get("netShareSettlementShares") is None for i in cash_settled)
    return {
        "component": "convertible_debt",
        "instruments": instruments,
        "method": "if_converted_when_in_the_money",
        "ifConvertedShares": sum((i["ifConvertedShares"] or 0) for i in instruments),
        "incrementalShares": None if unresolved == len(instruments) else sum((i["incrementalShares"] or 0) for i in instruments),
        "unresolvedInstruments": unresolved,
        "settlementText": "NOT_READ" if settlement is None else "READ",
        # The same count with each note whose principal the filing says is settled in cash at its net shares; None when none is.
        "incrementalSharesNetShareSettlement": (
            None if not cash_settled or unresolved == len(instruments) or nss_unresolved
            else sum((i["netShareSettlementShares"] if i.get("principalSettlement") is not None else (i["incrementalShares"] or 0)) for i in instruments)),
        "cappedCallText": "NOT_READ" if capped_calls is None else "READ",
        # Shares the stated capped calls deliver back at the price; None when none is fully stated (2.5.12).
        "cappedCallOffsetShares": sum(c["offsetSharesAtPrice"] for c in resolved_calls) if resolved_calls else None,
        "cappedCallsUnresolved": len(calls) - len(resolved_calls),
        "note": ("Shares are the filing's count issuable on conversion at the period end when tagged (it can be the maximum, make-whole included); "
                 "else principal outstanding at the period end (a face amount then, or a carrying amount well below the issue's face), else the face "
                 "amount or an issue-date carrying amount (principalBasis says which), "
                 "times a conversion ratio that agrees with the conversion price, else divided by the price. Where the filing states principal is "
                 "settled in cash, netShareSettlementShares is the conversion value above principal in shares at the price. Where the filing states "
                 "a capped call's strike, cap and covered shares, cappedCall.offsetSharesAtPrice is covered x (min(price, cap) - strike) / price: an "
                 "economic offset, not part of the EPS count."),
        "source": _source_ref(source, None),
    }


# ── Convertible preferred stock (2.5.9, MRVL) ───────────────────────────────

_PREFERRED_SHARES_ISSUABLE = "PreferredStockConvertibleSharesIssuable"
_PREFERRED_CONVERSION_PRICE = "PreferredStockConvertibleConversionPrice"
_PREFERRED_LIQUIDATION_AGGREGATE = ("PreferredStockLiquidationPreferenceValue", "TemporaryEquityLiquidationPreference")
_PREFERRED_LIQUIDATION_PER_SHARE = ("PreferredStockLiquidationPreference", "TemporaryEquityLiquidationPreferencePerShare")
_PREFERRED_DIVIDEND_RATE = ("PreferredStockDividendRatePercentage",)


# A preferred conversion the filing states in words, in two exact forms only (2.5.10): a per-share
# ratio ("The Preferred Stock will convert on a one-for-one basis into shares of our common stock",
# LITE; "each share of Preferred Stock is convertible into 10 shares of common stock") or an aggregate
# ("convertible in the aggregate into a maximum of approximately 21.8 million shares of our common stock").
_PREF_ONE_FOR_ONE_RE = re.compile(r"\bpreferred stock\b[^.]{0,120}\bconvert(?:s|ible)?\b[^.]{0,40}\bon a (?:one[- ](?:for|to)[- ]one|1[- ]for[- ]1|1:1) basis\b", _F)
# "each share of Series A Preferred Stock is convertible, at the option of the holder, into 10 shares of common stock" (2.5.11: text between).
_PREF_PER_SHARE_RE = re.compile(r"\beach share of (?:the |our )?(?:series [a-z0-9-]+ )?(?:convertible )?preferred stock\b[^.]{0,80}\bconvertible\b[^.]{0,60}?\binto "
                                r"([0-9][0-9,]*(?:\.[0-9]+)?) shares of (?:our |the company['’]s )?(?:class [a-z] )?common stock\b", _F)
# "a conversion rate of 10 shares of common stock for each share of Series A Preferred Stock" (2.5.11).
_PREF_RATE_RE = re.compile(r"\bconversion (?:rate|ratio) of ([0-9][0-9,]*(?:\.[0-9]+)?) shares of (?:our |the company['’]s )?(?:class [a-z] )?common stock "
                           r"(?:for each|per) share of (?:the |our )?(?:series [a-z0-9-]+ )?(?:convertible )?preferred stock\b", _F)
_PREF_AGGREGATE_RE = re.compile(r"\bpreferred stock\b[^.]{0,120}\bconvertible (?:in the aggregate into|into an aggregate of) (?:a maximum of |up to )?(?:approximately )?"
                                r"([0-9][0-9,]*(?:\.[0-9]+)?)( million)? shares of (?:our |the company['’]s )?(?:class [a-z] )?common stock\b", _F)


def preferred_conversion_terms(matches: list[TextMatch]) -> dict | None:
    """The first stated preferred conversion in filing text, quoted."""
    for match in matches:
        for sentence in (x.strip() for x in _CLAIM_SENTENCE_SPLIT_RE.split(_collapse(match.context_text))):
            one = _PREF_ONE_FOR_ONE_RE.search(sentence)
            per = _PREF_PER_SHARE_RE.search(sentence) or _PREF_RATE_RE.search(sentence)
            agg = _PREF_AGGREGATE_RE.search(sentence)
            if not one and not per and not agg:
                continue
            aggregate = float(agg.group(1).replace(",", "")) * (1_000_000 if agg.group(2) else 1) if agg else None
            return {
                "ratio": 1 if one else float(per.group(1).replace(",", "")) if per else None,
                "aggregateShares": round_half_up(aggregate) if aggregate is not None else None,
                "approximate": agg is not None and re.search(r"approximately", agg.group(0), re.I) is not None,
                "sentence": sentence[:600],
                "documentUrl": match.document_url,
            }
    return None


def _convertible_preferred_component(sources: list[IxSource], price: float, stated: dict | None = None) -> dict | None:
    """Convertible preferred outstanding at the period end, if-converted when in the money: the tagged
    common shares issuable on conversion and the conversion price. MRVL's Series A (2.0M preferred
    shares, issued to NVIDIA) converts into up to 21.8M common shares at $91.84."""
    def find(doc: IxDocument):
        end = doc.document_period_end
        if not end:
            return None
        outstanding = [f for f in doc.facts if f.local in _PREFERRED_OUTSTANDING_CONCEPTS and f.value is not None and f.period_end == end
                       and not any(re.search(r"StatementEquityComponentsAxis$", axis) for axis in f.dims)]
        # The same shares can be tagged under both concepts (LITE: 2.9M preferred and 2.9M temporary
        # equity): the larger concept count, never their sum.
        per_concept = []
        for c in _PREFERRED_OUTSTANDING_CONCEPTS:
            facts = [f for f in outstanding if f.local == c]
            plain = [f for f in facts if not f.dims]
            per_concept.append(sum(f.value for f in (plain or facts)))
        shares = max(per_concept)
        if not shares > 0:
            return None
        issuable = _newest([f for f in doc.facts if f.local == _PREFERRED_SHARES_ISSUABLE and f.value is not None and not _after_period_end(doc, f)])
        conv = _newest([f for f in doc.facts if f.local == _PREFERRED_CONVERSION_PRICE and f.value is not None and not _after_period_end(doc, f)])
        if issuable is None and conv is None and stated is None:
            return None

        # Preferred economics as tagged (2.5.10): the liquidation preference, aggregate or per share, and the dividend rate.
        def tagged(locals_: tuple[str, ...]) -> IxFact | None:
            return _newest([f for f in doc.facts if f.local in locals_ and f.value is not None and not _after_period_end(doc, f)
                            and not any(re.search(r"StatementEquityComponentsAxis$", axis) for axis in f.dims)])

        return {"shares": shares, "asOf": end, "issuable": issuable, "conv": conv, "liqAggregate": tagged(_PREFERRED_LIQUIDATION_AGGREGATE),
                "liqPerShare": tagged(_PREFERRED_LIQUIDATION_PER_SHARE), "dividend": tagged(_PREFERRED_DIVIDEND_RATE)}

    found = _find_in_sources(sources, find)
    if not found:
        return None
    v, source = found
    conv_price = v["conv"].value if v["conv"] is not None else None
    # Tags first; else the conversion the filing states in words (quoted in statedConversion).
    from_text = v["issuable"] is None and stated is not None
    if v["issuable"] is not None:
        if_converted, basis = v["issuable"].value, "shares_issuable_tagged"
    elif stated is not None and stated["ratio"] is not None:
        if_converted, basis = round_half_up(v["shares"] * stated["ratio"]), "ratio_stated_in_text"
    elif stated is not None and stated["aggregateShares"] is not None:
        if_converted, basis = stated["aggregateShares"], "aggregate_stated_in_text"
    else:
        if_converted, basis = None, None
    # A per-share ratio with no conversion price converts without payment: the preferred is
    # common-equivalent at any price (LITE's one-for-one Series A participates as converted).
    as_converted = conv_price is None and basis == "ratio_stated_in_text"
    in_the_money = price >= conv_price if conv_price is not None else None
    if if_converted is None:
        incremental = None
    elif as_converted:
        incremental = round_half_up(if_converted)
    else:
        incremental = None if in_the_money is None else round_half_up(if_converted) if in_the_money else 0
    instrument = {
        "instrument": "Convertible preferred stock",
        "preferredSharesOutstanding": v["shares"],
        "asOf": v["asOf"],
        "ifConvertedShares": if_converted,
        "ifConvertedBasis": basis,
        "sharesIssuableDate": v["issuable"].period_end if v["issuable"] is not None else None,
        # The issuable count was tagged at issuance and not restated at the period end.
        "countBeforePeriodEnd": v["issuable"] is not None and v["issuable"].period_end is not None and v["issuable"].period_end < v["asOf"],
        "conversionPrice": conv_price,
        "inTheMoney": in_the_money,
        "incrementalShares": incremental,
        "liquidationPreference": ({"amount": v["liqAggregate"].value, "basis": "aggregate_tagged", "asOf": v["liqAggregate"].period_end} if v["liqAggregate"] is not None
                                  else {"amount": round_half_up(v["liqPerShare"].value * v["shares"], 2), "basis": "per_share_tagged_x_shares_outstanding",
                                        "perShare": v["liqPerShare"].value, "asOf": v["liqPerShare"].period_end} if v["liqPerShare"] is not None else None),
        "dividendRatePct": round_half_up(v["dividend"].value * 100, 4) if v["dividend"] is not None else None,
        **({"statedConversion": {k: stated[k] for k in ("ratio", "aggregateShares", "approximate", "sentence", "documentUrl")}} if from_text else {}),
        **({"method": "as_converted_no_conversion_price"} if as_converted else {}),
    }
    return {
        "component": "convertible_preferred",
        "instruments": [instrument],
        "method": "if_converted_when_in_the_money",
        "ifConvertedShares": if_converted if if_converted is not None else 0,
        "incrementalShares": instrument["incrementalShares"],
        "unresolvedInstruments": 1 if instrument["incrementalShares"] is None else 0,
        "note": ("Common shares issuable on conversion as tagged (often the maximum at issuance) and the tagged conversion price; if-converted when "
                 "the price is at or above it. The liquidation preference and dividend rate are reported as tagged; redemption is not modeled."),
        "source": _source_ref(source, v["asOf"]),
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


# ── Untagged share claims (2.5.9, COHR) ─────────────────────────────────────

# Claims on the equity that the bridge's tagged components do not model: a
# price-protection or anti-dilution right granted with a share sale, a forward
# sale, shares issuable as contingent consideration, and convertible preferred
# stock. They are read from filing text, quoted, and never quantified.
SHARE_CLAIM_SEARCH_TERMS = [
    "price protection", "anti-dilution", "antidilution", "forward sale agreement", "earnout shares", "earn-out shares",
    "contingent consideration", "contingently issuable", "convertible preferred",
    "subsequent to quarter end", "subsequent to year end", "subsequent to the end of the quarter",
    "one-for-one basis", "convertible in the aggregate",
    "exchangeable for shares", "exchangeable into shares", "redeemable for shares of", "simple agreement for future equity",
    "payable in shares", "settled in shares", "standby equity purchase agreement", "equity line of credit", "committed equity facility",
    # 2.5.11: holdback and milestone shares, share-settled CVRs, issuance commitments, more preferred conversion wordings.
    "holdback shares", "escrow shares", "milestone shares", "contingent value right", "obligated to issue", "committed to issue", "required to issue",
    "one-to-one basis", "shares of common stock for each share of", "convertible into an aggregate of",
]
_SHARE_CLAIM_KINDS = [
    ("PRICE_PROTECTION", re.compile(r"\bprice[- ]protection\b", _F)),
    ("ANTI_DILUTION_RIGHT", re.compile(r"\banti-?dilution (?:rights?|protections?|provisions?)\b", _F)),
    ("FORWARD_SALE", re.compile(r"\bforward (?:sale|equity sale) agreements?\b", _F)),
    ("CONTINGENT_SHARES", re.compile(r"\bearn-?out shares\b|\bcontingently issuable (?:shares|common stock)\b|\bcontingent consideration\b[^.]{0,120}\b(?:in|of) (?:shares|common stock)\b"
                                  r"|\b(?:holdback|escrow(?:ed)?|milestone) shares\b|\bshares (?:held|placed) in escrow\b", _F)),
    ("CONVERTIBLE_PREFERRED", re.compile(r"\bconvertible preferred (?:stock|shares)\b", _F)),
    # Up-C units or exchangeable shares (2.5.10).
    ("EXCHANGEABLE_INTERESTS", re.compile(r"\b(?:units?|interests?|shares|stock)\b[^.]{0,100}\b(?:exchangeable|redeemable) (?:for|into) (?:an equal number of )?(?:newly[- ]issued )?(?:shares of )?(?:our |the company['’]s )?(?:class [a-z] )?common stock\b", _F)),
    ("SAFE", re.compile(r"\bsimple agreements? for future equity\b|\bSAFEs?\b")),
    ("SHARE_SETTLED_OBLIGATION", re.compile(r"\b(?:payable|settled|settleable|issuable)\b[^.]{0,30}\bin (?:shares of )?(?:our |the company['’]s )?(?:class [a-z] )?common stock\b", _F)),
    # Contingent value rights settled in shares, and commitments to issue shares (2.5.11).
    ("CONTINGENT_VALUE_RIGHT", re.compile(r"\bcontingent value rights?\b", _F)),
    ("SHARE_ISSUANCE_COMMITMENT", re.compile(r"\b(?:obligated|committed|required) to issue\b[^.]{0,80}\b(?:shares|common stock)\b", _F)),
    ("EQUITY_LINE", re.compile(r"\b(?:standby equity purchase agreement|equity line of credit|committed equity facility|equity purchase facility)\b", _F)),
    ("WARRANT_AFTER_PERIOD_END", re.compile(r"\bsubsequent to (?:the )?(?:quarter|year|period)[- ]end\b[^.]{0,200}\bwarrants?\b|\bsubsequent to the end of the (?:quarter|year|period)\b[^.]{0,200}\bwarrants?\b", _F)),
]
# A customer or distributor price-protection term is a revenue reduction, not a
# claim on shares (COHR's variable-consideration policy).
_REVENUE_TERM_RE = re.compile(r"\b(?:distributors?|customers?|revenues?|variable consideration|product returns?|sales price|price reductions?|inventor(?:y|ies)|rebates?)\b", _F)
# Anti-dilution adjustments of a warrant's or note's own terms belong to an
# instrument the bridge already reads.
_INSTRUMENT_TERM_RE = re.compile(r"\b(?:warrants?|convertible (?:senior )?notes?|conversion (?:rate|price)|exercise price|debentures?)\b", _F)
_AWARD_OR_NOTE_RE = re.compile(r"\b(?:restricted stock|RSUs?|PSUs?|awards?|stock options?|employees?|compensation|ESPP|dividend reinvestment|convertible|notes|debentures?|warrants?)\b", _F)
_PRESENT_OR_FUTURE_RE = re.compile(r"\b(?:is|are|may|will|would|can|could|remain|remains)\b", _F)
_EQUITY_TERM_RE = re.compile(r"\b(?:shares?|stock|equity|securities purchase agreement|purchase agreement|investors?|stockholders?|shareholders?)\b", _F)
_EXTINGUISHED_RE = re.compile(r"\bwere (?:all )?converted\b|\bno shares of\b[^.]{0,120}\b(?:are|were|remain)\b[^.]{0,30}\boutstanding\b|\b(?:was|were) redeemed in full\b|\b(?:expired|terminated) (?:unexercised|without)\b", _F)
# Sentence breaks, except after an initialism such as "U.S." ("applicable U.S. GAAP").
_CLAIM_SENTENCE_SPLIT_RE = re.compile(r"(?<![A-Z]\.[A-Z]\.)(?<=[.!?])\s+(?=[A-Z(\u201c\"])")
_PREFERRED_OUTSTANDING_CONCEPTS = ("PreferredStockSharesOutstanding", "TemporaryEquitySharesOutstanding")


def share_claim_signals(matches: list[TextMatch]) -> list[dict]:
    """Share claims stated in filing text that no tagged component covers, one per
    kind and document, each with up to four quoted sentences and the lead-in
    before the first. Every claim is UNQUANTIFIED; text saying an instrument was
    converted or redeemed is flagged (extinguishmentStated) but never closes the
    claim, since the sentence can describe another series."""
    groups: dict[str, dict] = {}
    seen: set[str] = set()
    for match in matches:
        items = [x.strip() for x in _CLAIM_SENTENCE_SPLIT_RE.split(_collapse(match.context_text)) if x.strip()]
        # A context window cuts its first and last sentences mid-way.
        if items and not re.match(r"[A-Z(\u201c\"]", items[0]):
            items.pop(0)
        if items and not re.search(r"[.!?\u201d\")]$", items[-1]):
            items.pop()
        for i, sentence in enumerate(items):
            kind = next((k for k, pattern in _SHARE_CLAIM_KINDS if pattern.search(sentence)), None)
            if kind is None or f"{kind}|{sentence}" in seen:
                continue
            if kind == "PRICE_PROTECTION" and _REVENUE_TERM_RE.search(sentence):
                continue
            if kind == "ANTI_DILUTION_RIGHT" and _INSTRUMENT_TERM_RE.search(sentence):
                continue
            # Awards, notes and dividends settled in shares belong elsewhere; a claim is present or future, not past.
            if kind in ("SHARE_SETTLED_OBLIGATION", "EXCHANGEABLE_INTERESTS", "SHARE_ISSUANCE_COMMITMENT") and (
                    _AWARD_OR_NOTE_RE.search(sentence) or not _PRESENT_OR_FUTURE_RE.search(sentence)):
                continue
            # A contingent value right is a share claim only when the sentence says it can be paid in shares.
            if kind == "CONTINGENT_VALUE_RIGHT" and not re.search(r"\b(?:shares|common stock)\b", sentence, _F):
                continue
            lead_in = " ".join(items[max(0, i - 2):i])
            if not _EQUITY_TERM_RE.search(f"{lead_in} {sentence}"):
                continue
            seen.add(f"{kind}|{sentence}")
            key = f"{kind}|{match.document_url or ''}"
            group = groups.get(key)
            if group is None:
                group = {
                    "kind": kind,
                    "status": "UNQUANTIFIED",
                    "sentences": [],
                    "leadIn": lead_in[-600:] or None,
                    # The sentence after the first quoted one ("The warrant is eligible for vesting ...").
                    "leadOut": items[i + 1][:600] if i + 1 < len(items) else None,
                    "extinguishmentStated": False,
                    "sectionHeading": match.section_heading,
                    "documentUrl": match.document_url,
                    "filingDate": match.filing_date,
                    "accessionNumber": match.accession_number,
                }
                groups[key] = group
            if len(group["sentences"]) < 4:
                group["sentences"].append(sentence[:600])
            if _EXTINGUISHED_RE.search(sentence):
                group["extinguishmentStated"] = True
    return list(groups.values())


def _preferred_claims_with_tags(claims: list[dict], sources: list[IxSource], component: dict | None = None) -> list[dict]:
    """Convertible preferred claims checked against the preferred and temporary
    equity share counts tagged at a filing's period end: all zero closes the
    claim (TAGGED_NONE_OUTSTANDING); a positive count keeps it open and quotes it."""
    tagged = None
    for s in sources:
        end = s.doc.document_period_end
        facts = [f for f in s.doc.facts if f.local in _PREFERRED_OUTSTANDING_CONCEPTS and f.value is not None and end is not None and f.period_end == end]
        if facts:
            tagged = facts
            break
    out = []
    for c in claims:
        if c["kind"] != "CONVERTIBLE_PREFERRED" or tagged is None:
            out.append(c)
            continue
        rows = [{"concept": f.name, "class": member_label(next(iter(f.dims.values()))) if f.dims else None, "shares": f.value, "asOf": f.period_end}
                for f in tagged]
        # Outstanding and resolved by the convertible_preferred component: counted, not open (2.5.9).
        status = ("TAGGED_NONE_OUTSTANDING" if all(f.value == 0 for f in tagged)
                  else "MODELED_IN_BRIDGE" if component is not None and component.get("incrementalShares") is not None else "UNQUANTIFIED")
        out.append({**c, "status": status, "taggedOutstanding": rows})
    return out


def _with_post_period_warrants(claims: list[dict], tagged: list[dict]) -> list[dict]:
    """Post-period warrants from the tags, each quoting the filing text's subsequent-event warrant
    sentences when the scan found them (which then stop being a separate claim)."""
    if not tagged:
        return claims
    text = next((c for c in claims if c["kind"] == "WARRANT_AFTER_PERIOD_END"), None)
    rest = [c for c in claims if c is not text]
    merged = [{**t, "sentences": text["sentences"], "leadIn": text["leadIn"], "leadOut": text["leadOut"], "sectionHeading": text["sectionHeading"]}
              if text is not None and i == 0 else t for i, t in enumerate(tagged)]
    return rest + merged


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


# ── The company's antidilutive-securities table (2.5.13) ────────────────────
#
# The EPS note lists every class of potentially dilutive security the company left out of diluted EPS,
# with its own share count for the period. Each row is matched to a bridge component by its label. A row
# no component models becomes a share claim with the company's count (REPORTED_NOT_MODELED): ASTS's
# Class B and Class C common stock exchangeable for Class A (89.4M), LUNR's escrow shares, SOUN's
# contingently issuable shares, RKLB's collared forward transactions. The table's warrant total is also
# set against the bridge's (LUNR reports 4,857,302; the bridge counts 541,667). The counts can be
# weighted averages over the period, so they are evidence beside the bridge, never added into it.
_ANTIDILUTIVE_CATEGORIES = [
    ("warrants", "warrants", "WARRANTS_NOT_TAGGED", re.compile(r"warrant", _F)),
    ("convertible_preferred", "convertible_preferred", "CONVERTIBLE_PREFERRED", re.compile(r"preferred", _F)),
    ("convertible_debt", "convertible_debt", "CONVERTIBLE_DEBT_NOT_TAGGED", re.compile(r"convertible|\bnotes?\b|debentures?", _F)),
    ("forward_sale", None, "FORWARD_SALE", re.compile(r"forward", _F)),
    ("contingent_shares", None, "CONTINGENT_SHARES", re.compile(r"contingent|earn-?out|escrow|holdback|milestone", _F)),
    ("stock_options", "stock_options", "EQUITY_COMPENSATION_NOT_TAGGED", re.compile(r"option", _F)),
    ("share_awards", "unvested_share_awards", "EQUITY_COMPENSATION_NOT_TAGGED",
     re.compile(r"restricted|\bRSUs?\b|\bPSUs?\b|stock units?|share units?|award|performance|unvested|nonvested", _F)),
    ("equity_compensation", "equity_compensation", "EQUITY_COMPENSATION_NOT_TAGGED",
     re.compile(r"compensation|incentive|equity plan|employee stock|\bESPP\b|purchase plan", _F)),
    ("exchangeable_interests", None, "EXCHANGEABLE_INTERESTS",
     re.compile(r"\bunits?\b|class [a-z]\b|common stock|common shares|exchangeable|noncontrolling|\bLLC\b|partnership", _F)),
]
_EQUITY_COMPENSATION_COMPONENTS = ("stock_options", "unvested_share_awards")


def _antidilutive_reconciliation(eps: dict | None, components: list[dict], source: IxSource | None) -> tuple[dict | None, list[dict], dict | None]:
    """The antidilutive rows matched to components, the claims no component models, and the warrant totals set side by side."""
    rows = [r for r in ((eps or {}).get("antidilutiveExcluded") or []) if _is_number(r.get("shares")) and r["shares"] > 0]
    if not eps or not rows:
        return None, [], None
    present = {str(c.get("component")) for c in components}

    def modeled(component: str | None) -> bool:
        if component is None:
            return False
        if component == "equity_compensation":
            return any(c in present for c in _EQUITY_COMPENSATION_COMPONENTS)
        return component in present or (component in _EQUITY_COMPENSATION_COMPONENTS and any(c in present for c in _EQUITY_COMPENSATION_COMPONENTS))

    matched = []
    for r in rows:
        cat = next((c for c in _ANTIDILUTIVE_CATEGORIES if c[3].search(str(r["security"]))), None)
        matched.append({"security": r["security"], "shares": r["shares"], "category": cat[0] if cat else "other",
                        "modeledBy": cat[1] if cat and modeled(cat[1]) else None, "kind": cat[2] if cat else "OTHER_REPORTED_SECURITY"})
    groups: dict[str, list[dict]] = {}
    for m in matched:
        if m["modeledBy"] is None:
            groups.setdefault(m["kind"], []).append(m)
    claims = [{
        "kind": kind,
        "status": "REPORTED_NOT_MODELED",
        "reportedShares": sum(m["shares"] for m in items),
        "reportedSecurities": [{"security": m["security"], "shares": m["shares"]} for m in items],
        "reportedPeriod": {"start": eps.get("periodStart"), "end": eps.get("periodEnd")},
        "evidence": "ANTIDILUTIVE_TABLE",
        "sentences": [],
        "leadIn": None,
        "leadOut": None,
        "documentUrl": source.document_url if source else None,
        "filingDate": source.filing_date if source else None,
        "accessionNumber": source.accession_number if source else None,
    } for kind, items in groups.items()]
    reported_warrants = sum(m["shares"] for m in matched if m["category"] == "warrants")
    bridge_warrants = next((c for c in components if c.get("component") == "warrants"), None)
    reconciliation = {
        "periodStart": eps.get("periodStart"),
        "periodEnd": eps.get("periodEnd"),
        "rows": [{k: v for k, v in m.items() if k != "kind"} for m in matched],
        "note": ("The company's own antidilutive-securities counts for the period (possibly weighted averages), matched to bridge components by "
                 "label. A row no component models is listed in unquantifiedShareClaims with the company's count; nothing here is added to the bridge."),
    }
    warrants = ({"reported": reported_warrants, "bridge": bridge_warrants.get("outstanding") if _is_number(bridge_warrants.get("outstanding")) else None}
                if reported_warrants > 0 and bridge_warrants else None)
    return reconciliation, claims, warrants


def _merge_reported_claims(claims: list[dict], reported: list[dict]) -> list[dict]:
    """Text claims of a kind the antidilutive table also reports carry its count; the table's other rows are their own claims."""
    out = [dict(c) for c in claims]
    for r in reported:
        text = next((c for c in out if c["kind"] == r["kind"] and c["status"] == "UNQUANTIFIED"), None)
        if text is not None:
            text.update({"status": "REPORTED_NOT_MODELED", "reportedShares": r["reportedShares"], "reportedSecurities": r["reportedSecurities"],
                         "reportedPeriod": r["reportedPeriod"]})
        else:
            out.append(r)
    return out


def is_open_claim(c: dict) -> bool:
    """A claim the bridge leaves out: quoted only in text, or reported with a count no component models."""
    return c.get("status") in ("UNQUANTIFIED", "REPORTED_NOT_MODELED")


def dilution_bridge(ticker: str, price: float, price_currency: str, as_of_date: str | None,
                    sources: list[IxSource], atm_matches: list[TextMatch], award_table_matches: list[TextMatch] | None = None,
                    claim_matches: list[TextMatch] | None = None, warrant_lifecycle_matches: list[TextMatch] | None = None,
                    convertible_settlement_matches: list[TextMatch] | None = None, capped_call_matches: list[TextMatch] | None = None) -> dict:
    """Basic to diluted shares at a supplied price, from company disclosures only.

    warrant_lifecycle_matches is the filing text read for the exercise, expiry or redemption of warrants
    counted before the period end (2.5.11); convertible_settlement_matches the text read for a stated cash
    settlement of convertible principal (2.5.11); capped_call_matches the text read for capped call terms
    (2.5.12). Each is None when it was not read.
    """
    primary = sources[0] if sources else None
    basic_found = _find_in_sources(sources, _basic_shares)
    options = _options_component(sources, price)
    awards = _awards_component(sources, award_table_matches or [])
    warrants = _warrants_component(sources, price, warrant_lifecycle_sentences(warrant_lifecycle_matches) if warrant_lifecycle_matches is not None else None)
    convertibles = _convertibles_component(sources, price, principal_cash_settlement(convertible_settlement_matches) if convertible_settlement_matches is not None else None,
                                           capped_call_terms(capped_call_matches) if capped_call_matches is not None else None)
    preferred = _convertible_preferred_component(sources, price, preferred_conversion_terms(claim_matches) if claim_matches is not None else None)
    atm = _atm_component(atm_evidence(atm_matches), price)
    components = [c for c in (options, awards, warrants, convertibles, preferred) if c is not None]
    not_disclosed = [name for name, c in (
        ("stock_options", options), ("unvested_share_awards", awards), ("warrants", warrants),
        ("convertible_debt", convertibles), ("convertible_preferred", preferred), ("atm_program", atm),
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
    warrant_events = warrant_count_events(sources)
    if warrant_events:
        listed = "; ".join(
            f"{w['class']} {_js_number(w['value'])} on {w['asOf']}"
            + (f" (exercised: {_js_number(w['exercise']['value'])} on {w['exercise']['date']})" if w.get("exercise") else "")
            for w in warrant_events)
        warnings.append({
            "code": "WARRANT_EXERCISE_NOT_OUTSTANDING",
            "message": (f"{len(warrant_events)} tagged warrant count(s) record an exercise or an equity-statement movement, or predate an exercise of "
                        f"the same class, so they are not counted as warrants outstanding: {listed}."),
            "severity": "info",
            "counts": warrant_events,
        })
    notes = (convertibles or {}).get("instruments") or []
    face_only = [i for i in notes if i.get("ifConvertedBasis") != "shares_issuable_tagged_at_period_end" and i.get("principalBasis") != "outstanding_at_period_end"
                 and i.get("principal") is not None]
    if face_only:
        names = ", ".join(i["instrument"] for i in face_only)
        used = ", ".join(f"{i['principalBasis']} {_js_number(i['principal'])} ({i['principalDate']})" for i in face_only)
        warnings.append({"code": "CONVERTIBLE_PRINCIPAL_NOT_AT_PERIOD_END", "message": (
            f"No principal or shares issuable is tagged at the period end for {names}; the {used} is used and can include notes since converted "
            "or repurchased."), "severity": "warning"})
    bad_ratios = [i for i in notes if i.get("conversionRatioNote") == "RATIO_INCONSISTENT_WITH_PRICE"]
    if bad_ratios:
        listed = ", ".join(f"{i['instrument']} ({_js_number(i['conversionRatioPer1000'])} per 1,000 against a ${_js_number(i['conversionPrice'])} price)" for i in bad_ratios)
        warnings.append({"code": "CONVERSION_RATIO_INCONSISTENT", "message": (
            f"The tagged conversion ratio of {listed} is not the conversion rate (often a make-whole increase) and is not used."), "severity": "info"})
    cash_principal = [i for i in notes if i.get("principalSettlement") is not None]
    if cash_principal:
        counts = ", ".join(f"{'unresolved' if i.get('netShareSettlementShares') is None else _js_number(i['netShareSettlementShares'])} for {i['instrument']}"
                           for i in cash_principal)
        warnings.append({
            "code": "CONVERTIBLE_PRINCIPAL_SETTLED_IN_CASH",
            "message": (f"The filing states the principal of {', '.join(str(i['instrument']) for i in cash_principal)} is settled in cash, so conversion "
                        "delivers shares only for the value above principal. dilutedSharesAtPrice counts them if-converted (the EPS basis); "
                        f"dilutedSharesAtPriceNetShareSettlement counts {counts} instead."),
            "severity": "info",
        })
    capped = [(i["instrument"], i["cappedCall"]) for i in notes if i.get("cappedCall") is not None]
    capped_resolved = [(n, c) for n, c in capped if _is_number(c.get("offsetSharesAtPrice"))]
    if capped_resolved:
        listed = "; ".join(f"{n} (strike ${_js_number(c['strikePrice'])}, cap ${_js_number(c['capPrice'])}, {_js_number(c['coveredShares'])} shares: "
                           f"{_js_number(c['offsetSharesAtPrice'])} shares back at the price)" for n, c in capped_resolved)
        warnings.append({
            "code": "CAPPED_CALL_OFFSET",
            "message": (f"The filing states capped call terms for {listed}. dilutedSharesAtPrice does not net them (diluted EPS excludes capped calls "
                        "as antidilutive); dilutedSharesAtPriceNetOfCappedCalls does."),
            "severity": "info",
        })
    capped_open = [(n, c) for n, c in capped if not _is_number(c.get("offsetSharesAtPrice"))]
    if capped_open:
        listed = "; ".join(f"{n} (not stated: {', '.join(c['missingTerms'])})" for n, c in capped_open)
        warnings.append({"code": "CAPPED_CALL_TERMS_INCOMPLETE",
                         "message": f"The filing describes capped calls for {listed}; no offset is computed for them.", "severity": "info"})
    redeemed = [i for i in notes if i.get("afterPeriodEnd") is not None]
    if redeemed:
        listed = ", ".join(f"{i['instrument']} ({i['afterPeriodEnd']['date']})" for i in redeemed)
        warnings.append({"code": "CONVERTIBLE_REDEMPTION_AFTER_PERIOD_END", "message": (
            f"A redemption or repurchase after the period end is tagged for {listed}; its shares are counted as of the period end."), "severity": "warning"})
    nested_counts = (warrants or {}).get("nestedCounts") or []
    if nested_counts:
        listed = "; ".join(
            f"{c['class']} ({_js_number(c['count'])}) is counted through its parts {', '.join(c['parts'])}" if c["reason"] == "SUM_OF_COUNTED_PARTS"
            else f"{c['class']} ({_js_number(c['count'])}) is part of {c['partOf']} and not counted again" for c in nested_counts)
        warnings.append({"code": "WARRANT_NESTED_COUNT", "message": f"{listed}.", "severity": "info"})
    retired_text = (warrants or {}).get("retiredInText") or []
    if retired_text:
        listed = "; ".join(f"{c['class']} ({_js_number(c['count'])} as of {c['asOf']}: {str(c['event']).lower()} by {c['eventDate']})" for c in retired_text)
        warnings.append({
            "code": "WARRANT_RETIRED_IN_TEXT",
            "message": (f"{listed}: the filing text states the class was exercised, expired or redeemed after its tagged count, so it is not "
                        "counted; the sentence is quoted in retiredInText."),
            "severity": "info",
        })
    early_counts = [c for c in ((warrants or {}).get("classes") or []) if c.get("countBeforePeriodEnd") is True]
    if early_counts:
        listed = "; ".join(f"{c['class']} {_js_number(c['outstanding'])} as of {c['asOf']}" for c in early_counts)
        read = all(c.get("lifecycleText") == "NO_EVENT_STATED" for c in early_counts)
        state = ("the filing text was read and states no exercise, expiry or redemption of them, which does not prove they remain outstanding"
                 if read else "the filing text should confirm they remain outstanding")
        warnings.append({
            "code": "WARRANT_COUNT_BEFORE_PERIOD_END",
            "message": (f"{len(early_counts)} warrant class(es) are counted from a figure tagged before the report's period end and not restated at "
                        f"it; {state}: {listed}."),
            "severity": "warning",
        })
    # claim_matches is the filing text read for untagged share claims; None when it was not read.
    claims_read = claim_matches is not None
    eps_found = _find_in_sources(sources, _reported_eps_dilution)
    eps_value, eps_source = eps_found if eps_found else (None, None)
    reconciliation, reported_claims, reported_warrants = _antidilutive_reconciliation(eps_value, components, eps_source)
    claims = _merge_reported_claims(_with_post_period_warrants(
        _preferred_claims_with_tags(share_claim_signals(claim_matches), sources, preferred) if claim_matches is not None else [],
        post_period_warrants(primary),
    ), reported_claims)
    open_claims = [c for c in claims if is_open_claim(c)]
    if open_claims:
        listed = ", ".join(f"{c['kind']}: {_js_number(c['reportedShares'])} reported" if _is_number(c.get("reportedShares")) else c["kind"] for c in open_claims)
        warnings.append({
            "code": "UNQUANTIFIED_SHARE_CLAIMS",
            "message": (f"The filing states {len(open_claims)} share claim(s) no tagged component covers ({listed}); "
                        "they are listed in unquantifiedShareClaims and are not in any share count."),
            "severity": "warning",
        })
    # The company's warrant total against the bridge's: a stale, missing or double count shows as a gap (2.5.13).
    aw = reported_warrants
    if aw and aw["bridge"] is not None and abs(aw["reported"] - aw["bridge"]) > max(aw["reported"] * 0.05, 100_000):
        warnings.append({
            "code": "WARRANT_COUNT_DIFFERS_FROM_REPORTED",
            "message": (f"The bridge counts {_js_number(aw['bridge'])} warrant shares; the company's antidilutive-securities table reports "
                        f"{_js_number(aw['reported'])} for the period {eps_value.get('periodStart')} to {eps_value.get('periodEnd')}. The table can be a "
                        "weighted average, but a gap this size usually means a class is stale, untagged or tagged only in part; check the warrant note."),
            "severity": "warning",
        })
    expired_classes = (warrants or {}).get("expiredClasses") or []
    if expired_classes:
        listed = "; ".join(f"{c['class']} ({_js_number(c['count'])}, expired {c['expirationDate']})" for c in expired_classes)
        warnings.append({"code": "WARRANT_EXPIRED_BEFORE_PERIOD_END", "message": (
            f"{listed} expired before the period end by their tagged expiration date and are not counted."), "severity": "info"})
    elapsed = [c for c in ((warrants or {}).get("classes") or []) if c.get("termElapsedBy") is not None]
    if elapsed:
        listed = "; ".join(f"{c['class']} ({_js_number(c['outstanding'])} as of {c['asOf']}, term through {c['termElapsedBy']})" for c in elapsed)
        warnings.append({"code": "WARRANT_TERM_ELAPSED", "message": (
            f"{listed}: the tagged term, counted from the tagged date, ended before the period end. They may have expired unexercised; they are still "
            "counted until the filing says otherwise."), "severity": "warning"})
    vesting_unknown = [c for c in ((warrants or {}).get("classes") or []) if c.get("exercisableBasis") == "vesting_terms_without_vested_count"]
    if vesting_unknown:
        warnings.append({"code": "WARRANT_VESTING_NOT_TAGGED", "message": (
            f"{', '.join(str(c['class']) for c in vesting_unknown)} vest on conditions, and no vested or unvested count is tagged; their exercisable "
            f"shares are unresolved, not assumed to be all {', '.join(_js_number(c['outstanding']) for c in vesting_unknown)}."), "severity": "warning"})
    if not claims_read:
        warnings.append({"code": "SHARE_CLAIM_TEXT_NOT_READ", "message": (
            "The filing text was not read for untagged share claims (the kinds in claimCoverage.textScanKinds); retry."), "severity": "warning"})
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
        "reportedEpsDilution": eps_value,
        "antidilutiveReconciliation": reconciliation,
        "notDisclosed": not_disclosed,
        "unresolved": unresolved,
        "partiallyResolved": partial,
        "unquantifiedShareClaims": claims,
        "claimCoverage": {
            "scope": "TAGGED_INSTRUMENTS",
            "completeClaimInventory": False,
            "modeledComponents": ["stock_options", "unvested_share_awards", "warrants", "convertible_debt", "convertible_preferred", "atm_program"],
            "textScan": "READ" if claims_read else "NOT_READ",
            # The company's antidilutive-securities table, read from the inline XBRL (2.5.13).
            "antidilutiveTable": "READ" if reconciliation else "NOT_TAGGED",
            "textScanKinds": [k for k, _ in _SHARE_CLAIM_KINDS],
            "unquantifiedClaims": len(open_claims),
            "note": ("status describes the tagged instruments only. COMPUTED means every tagged instrument was resolved, never that every claim on the "
                     "equity was found; the text scan covers a fixed list of claim kinds, and a claim it does not find is not proof none exists. The "
                     "company's antidilutive-securities table adds any class of potentially dilutive security it reports that no component models, "
                     "with its count."),
        },
    }
    if not basic_found:
        out["status"] = "NOT_FOUND"
        out["bridge"] = None
        warnings.append({"code": "BASIC_SHARES_NOT_FOUND", "message": "No cover-page or balance-sheet share count was tagged.", "severity": "error"})
    else:
        basic = basic_found[0]["shares"]

        def inc(c: dict | None) -> float:
            return c["incrementalShares"] if c and _is_number(c.get("incrementalShares")) else 0

        diluted = basic + inc(options) + inc(awards) + inc(warrants) + inc(convertibles) + inc(preferred)
        gross = (basic
                 + (float(options.get("outstanding") or 0) if options else 0)
                 + (float(awards.get("unvested") or 0) if awards else 0)
                 + (float(warrants.get("outstanding") or 0) if warrants else 0)
                 + (float(convertibles.get("ifConvertedShares") or 0) if convertibles else 0)
                 + (float(preferred.get("ifConvertedShares") or 0) if preferred else 0))
        atm_shares = atm["potentialShares"] if atm and _is_number(atm.get("potentialShares")) else None
        out["bridge"] = {
            "basicShares": basic,
            "stockOptions": options["incrementalShares"] if options else None,
            "unvestedShareAwards": awards["incrementalShares"] if awards else None,
            "warrants": warrants["incrementalShares"] if warrants else None,
            "convertibleDebt": convertibles["incrementalShares"] if convertibles else None,
            "convertiblePreferred": preferred["incrementalShares"] if preferred else None,
            "dilutedSharesAtPrice": round_half_up(diluted),
            "dilutionPctAtPrice": round_half_up(((diluted - basic) / basic) * 100, 2) if basic > 0 else None,
            "grossSharesAllInstruments": round_half_up(gross),
            "grossDilutionPct": round_half_up(((gross - basic) / basic) * 100, 2) if basic > 0 else None,
            # Convertibles whose principal the filing says is settled in cash at their net shares; None when none is (2.5.11).
            "convertibleDebtNetShareSettlement": (convertibles["incrementalSharesNetShareSettlement"]
                                                  if convertibles and convertibles.get("incrementalSharesNetShareSettlement") is not None else None),
            "dilutedSharesAtPriceNetShareSettlement": (
                round_half_up(diluted - inc(convertibles) + convertibles["incrementalSharesNetShareSettlement"])
                if convertibles and _is_number(convertibles.get("incrementalSharesNetShareSettlement")) else None),
            # Stated capped calls netted at the price: an economic view beside the EPS count (2.5.12).
            "cappedCallOffsetShares": convertibles["cappedCallOffsetShares"] if convertibles and _is_number(convertibles.get("cappedCallOffsetShares")) else None,
            "dilutedSharesAtPriceNetOfCappedCalls": (round_half_up(diluted - convertibles["cappedCallOffsetShares"])
                                                     if convertibles and _is_number(convertibles.get("cappedCallOffsetShares")) else None),
            "dilutedSharesAtPriceNetShareSettlementNetOfCappedCalls": (
                round_half_up(diluted - inc(convertibles) + convertibles["incrementalSharesNetShareSettlement"] - convertibles["cappedCallOffsetShares"])
                if convertibles and _is_number(convertibles.get("cappedCallOffsetShares")) and _is_number(convertibles.get("incrementalSharesNetShareSettlement"))
                else None),
            "atmPotentialShares": atm_shares,
            "dilutedSharesAtPriceWithAtm": round_half_up(diluted + atm_shares) if atm_shares is not None else None,
            "formula": ("basic + options (treasury stock) + unvested awards (gross) + warrants (treasury stock, vested) + convertibles and "
                        "convertible preferred (if-converted when in the money)"),
        }
        # A share claim the filing states but no tagged component covers leaves the count partial (2.5.9).
        out["status"] = "PARTIAL" if unresolved or partial or open_claims else "COMPUTED"
    out["methodology"] = [
        "Every count is a company disclosure tagged in the filing's inline XBRL; the only external input is the price you supplied.",
        "This is a mechanical bridge, not a consensus or forecast diluted share count, and must not be back-solved into one.",
        "A component missing from notDisclosed was not tagged in the filing; that is not proof the instrument does not exist.",
        ("COMPUTED covers the tagged instruments only; it is not a full claim inventory. Share claims found in the filing text are quoted in "
         "unquantifiedShareClaims and left out of every count."),
    ]
    # Convertible preferred is rare; its absence alone does not call for the concept list.
    if any(c != "convertible_preferred" for c in not_disclosed) or unresolved:
        # What the filing does tag, so a missing component can be traced to a concept this bridge does not read.
        out["taggedDilutionConcepts"] = _tagged_dilution_concepts(sources)
    out["warnings"] = warnings
    if primary is not None and primary.doc.document_period_end:
        out["periodEnd"] = primary.doc.document_period_end
    return out


# ── Capital structure timeline ──────────────────────────────────────────────

_CASH_CONCEPTS = ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents", "Cash"]
# A short-term investments total when tagged; otherwise the current securities
# categories, which are disjoint, are added. VRT tags only its held-to-maturity
# Treasury bills (2.4.3).
_SHORT_TERM_INVESTMENT_TOTALS = ["ShortTermInvestments", "MarketableSecuritiesCurrent"]
_SHORT_TERM_INVESTMENT_PARTS = [
    ["AvailableForSaleSecuritiesDebtSecuritiesCurrent"],
    ["DebtSecuritiesHeldToMaturityAmortizedCostAfterAllowanceForCreditLossCurrent", "HeldToMaturitySecuritiesCurrent", "DebtSecuritiesHeldToMaturityExcludingAccruedInterestAfterAllowanceForCreditLossCurrent"],
    ["OtherShortTermInvestments"],
]
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


def _short_term_investments(doc: IxDocument, at: str | None) -> dict | None:
    """Short-term investments at a date: the tagged total, else the sum of the current securities categories."""
    whole = _first_total(doc, _SHORT_TERM_INVESTMENT_TOTALS, at)
    if whole:
        return whole
    parts = [p for p in (_first_total(doc, group, at) for group in _SHORT_TERM_INVESTMENT_PARTS) if p is not None]
    if len(parts) <= 1:
        return parts[0] if parts else None
    decimals = None
    for p in parts:
        if p["decimals"] is not None:
            decimals = p["decimals"] if decimals is None else min(decimals, p["decimals"])
    return {
        "value": sum(p["value"] for p in parts),
        "periodEnd": at,
        "concept": " + ".join(p["concept"] for p in parts),
        "unit": parts[0]["unit"],
        "decimals": decimals,
        "sentence": None,
    }


# Debt totals that include finance leases (2.5.27: MU, XOM, CMCSA and MPC tag their debt only this way), and the
# finance-lease liabilities that take them back to debt alone.
_LEASE_INCLUSIVE_TOTAL = "DebtAndCapitalLeaseObligations"
_LEASE_INCLUSIVE_PARTS = ["DebtCurrent", "LongTermDebtAndCapitalLeaseObligationsCurrent", "LongTermDebtAndCapitalLeaseObligations"]
_FINANCE_LEASE_TOTAL = "FinanceLeaseLiability"
_FINANCE_LEASE_PARTS = ["FinanceLeaseLiabilityCurrent", "FinanceLeaseLiabilityNoncurrent"]

# Every concept _total_debt or the instrument rows read; a filing with none of
# them at any date or dimension reports no borrowings.
_BORROWING_CONCEPTS = {
    "LongTermDebt", "LongTermDebtCurrent", "LongTermDebtNoncurrent", "NotesPayable", "SeniorNotes", "DebtInstrumentFaceAmount",
    _LEASE_INCLUSIVE_TOTAL, *_LEASE_INCLUSIVE_PARTS,
    *_SHORT_TERM_BORROWING_CONCEPTS, *_DEBT_LINE_CONCEPTS, *_CARRYING_CONCEPTS, *_CONVERTIBLE_TOTAL_CONCEPTS, *_CONVERTIBLE_PART_CONCEPTS,
}


def _tags_no_borrowings(doc: IxDocument) -> bool:
    return not any(f.local in _BORROWING_CONCEPTS for f in doc.facts)


# A balance tagged 100x coarser than the cash line (decimals two or more
# lower) is a rounded narrative figure, such as "approximately $2.3 billion
# ... classified as cash equivalents", not a balance-sheet line.
_ROUNDED_DECIMALS_GAP = 2


# A sentence that says the amount is cash equivalents or money-market funds
# restates part of cash; adding it again would count it twice.
_CASH_OVERLAP_RE = re.compile(r"\bcash equivalents?\b|\bmoney[- ]market\b", re.I | re.A)


def _balance_exclusion(value: float | None, period_end: str | None, decimals: int | None, sentence: str | None, cash: dict | None, at: str | None) -> str | None:
    """Why a period-end fact is not a balance-sheet amount: "rounded", "overlaps_cash", or None to keep it."""
    if not cash or value is None or period_end != at:
        return None
    if cash["decimals"] is not None and decimals is not None and decimals <= cash["decimals"] - _ROUNDED_DECIMALS_GAP:
        return "rounded"
    if sentence is not None and _CASH_OVERLAP_RE.search(sentence) and value <= cash["value"]:
        return "overlaps_cash"
    return None


def _balance_facts(doc: IxDocument, cash: dict | None, at: str | None) -> IxDocument:
    """The document without period-end facts that are rounded note figures or restate cash."""
    if not cash:
        return doc
    kept = [f for f in doc.facts if f.dims or _balance_exclusion(f.value, f.period_end, f.decimals, f.sentence, cash, at) is None]
    return replace(doc, facts=kept)


# Within half a percent, two totals are the same amount.
_AGGREGATE_TOLERANCE = 0.005


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
    lease_inclusive = _lease_inclusive_debt(doc, at)
    if lease_inclusive:
        return lease_inclusive
    lines = [p for p in (_total(doc, c, at) for c in _DEBT_LINE_CONCEPTS) if p is not None]
    if lines:
        return with_short(sum(p["value"] for p in lines), lines, "Sum of tagged debt lines plus short-term borrowings")
    fallback = _first_total(doc, ["ConvertibleNotesPayable", "NotesPayable", "SeniorNotes"], at)
    if fallback:
        return with_short(fallback["value"], [fallback], f"{fallback['concept']} plus short-term borrowings")
    if short_term:
        return with_short(0, [], "Short-term borrowings only")
    return None


def _lease_inclusive_debt(doc: IxDocument, at: str | None) -> dict | None:
    """Debt from totals that include finance leases, less the finance-lease liabilities when the filing tags them
    (2.5.27: MU tags DebtAndCapitalLeaseObligations 5,722 and FinanceLeaseLiability 2,670, so debt is 3,052, the sum
    of its notes). Current debt in these totals already holds short-term borrowings, so none are added. `leases`
    says whether the leases were taken out or could not be."""
    whole = _total(doc, _LEASE_INCLUSIVE_TOTAL, at)
    parts = [whole] if whole else [p for p in (_total(doc, c, at) for c in _LEASE_INCLUSIVE_PARTS) if p is not None]
    if not parts:
        return None
    # DebtCurrent and the current lease-inclusive line are alternatives for the same current debt.
    used = [p for p in parts if _local_name(p["concept"]) != "LongTermDebtAndCapitalLeaseObligationsCurrent"] if any(_local_name(p["concept"]) == "DebtCurrent" for p in parts) else parts
    gross = sum((p["value"] for p in used), 0)
    lease_whole = _total(doc, _FINANCE_LEASE_TOTAL, at)
    lease_parts = [lease_whole] if lease_whole else [p for p in (_total(doc, c, at) for c in _FINANCE_LEASE_PARTS) if p is not None]
    lease = sum((p["value"] for p in lease_parts), 0)
    gross_basis = " + ".join(_local_name(p["concept"]) for p in used)
    if lease_parts and lease <= gross:
        return {
            "value": gross - lease,
            "components": [*({"concept": p["concept"], "amount": p["value"]} for p in used), *({"concept": p["concept"], "amount": -p["value"]} for p in lease_parts)],
            "basis": f"{gross_basis} (debt including finance leases) less {' + '.join(_local_name(p['concept']) for p in lease_parts)}",
            "leases": "SUBTRACTED",
        }
    return {"value": gross, "components": [{"concept": p["concept"], "amount": p["value"]} for p in used], "basis": f"{gross_basis} (debt including finance leases; no finance-lease liability tagged to take out)", "leases": "INCLUDED"}


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
        # One member can carry two tagged maturities and coupons (AAOI's China bank revolver and equipment term loan,
        # one amount): all are reported and the row is placed at the earliest (2.5.24, F-010).
        maturity_dates = sorted({d for d in (normalize_ix_date(t) for t in _group_texts(g, "DebtInstrumentMaturityDate")) if d is not None})
        maturity = maturity_dates[0] if maturity_dates else None
        coupons = sorted(round_half_up(v * 100, 4) for v in _group_values(g, "DebtInstrumentInterestRateStatedPercentage"))
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
            # The date the face amount is tagged at: an issue date before the period end is original principal (F-007).
            "faceAmountDate": face["periodEnd"] if face else None,
            "carryingAmount": carrying["value"] if carrying else None,
            "carryingAmountConcept": carrying["concept"] if carrying else None,
            "taggedAmount": tagged["value"] if tagged else None,
            "taggedAmountDate": tagged["periodEnd"] if tagged else None,
            "taggedAmountConcept": tagged["concept"] if tagged else None,
            "couponPct": round_half_up(coupon["value"] * 100, 4) if coupon else None,
            "effectiveRatePct": round_half_up(effective["value"] * 100, 4) if effective else None,
            "maturityDate": maturity,
            "maturityDateSource": "XBRL" if maturity else None,
            **({"maturityDates": maturity_dates} if len(maturity_dates) > 1 else {}),
            **({"couponPcts": coupons} if len(coupons) > 1 else {}),
            "convertible": conv_price is not None or ratio is not None,
            "conversionPrice": conv_price["value"] if conv_price else None,
            "conversionRatioPer1000": normalized_ratio(ratio["value"] if ratio else None, conv_price["value"] if conv_price else None)["per1000"],
            "conversionRatioTagged": ratio["value"] if ratio else None,
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


# Filing text stating when instruments mature, read only when an outstanding row has no tagged maturity (2.5.24).
MATURITY_SEARCH_TERMS = ["will mature on", "mature on", "matures on"]
_MATURITY_TEXT_RE = re.compile(r"\bmatur(?:e|es)\s+on\s+((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})", _F)


def _maturity_statements(matches: list[TextMatch]) -> list[dict]:
    """"... will mature on January 15, 2030 ..." sentences, each with the ISO date it states."""
    out: list[dict] = []
    seen: set[str] = set()
    for match in matches:
        for sentence in _sentences(_collapse(match.context_text)):
            for m in _MATURITY_TEXT_RE.finditer(sentence):
                date = normalize_ix_date(m.group(1))
                if not date or len(date) != 10 or f"{date}|{sentence}" in seen:
                    continue
                seen.add(f"{date}|{sentence}")
                out.append({"date": date, "sentence": sentence[:400]})
    return out


def _unplaced_rows(rows: list[dict]) -> list[dict]:
    """Rows still outstanding with a non-zero amount but no maturity: what a maturity ladder cannot place."""
    return [r for r in rows if r.get("maturityDate") is None and r.get("status") != "matured_before_period_end" and not r.get("aggregateOf") and (_ladder_amount(r) or {"amount": 0})["amount"] != 0]


def wants_maturity_text(out: dict) -> bool:
    """Whether the filing text should be searched for maturity sentences: an outstanding row with an amount is dated
    neither by XBRL nor by the text (a date read from its name is only a year or month)."""
    return any(
        r.get("status") != "matured_before_period_end" and not r.get("aggregateOf") and (_ladder_amount(r) or {"amount": 0})["amount"] != 0
        and (r.get("maturityDateSource") is None or r.get("maturityDateSource") == "INSTRUMENT_NAME")
        for r in (out.get("instruments") or [])
    )


_NAME_MATURITY_RE = re.compile(r"\b(?:Due|Maturing|Matures)\s+(?:In\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sept?|Oct|Nov|Dec)\.?\s+(?:\d{1,2},\s+)?(?:19|20)\d{2}|(?:19|20)\d{2})\b", re.A)


def _maturity_from_name(rows: list[dict], period_end: str | None) -> None:
    """Last resort for a row with no tagged or stated maturity (2.5.25): the date its name gives after "Due" or
    "Maturing" ("Due January 2031" -> 2031-01, "Due 2036" -> 2036), only when that is not before the period end.
    The ladder needs only the year."""
    for row in rows:
        if row.get("maturityDate") is not None or row.get("status") == "matured_before_period_end":
            continue
        m = _NAME_MATURITY_RE.search(str(row["instrument"]))
        date = normalize_ix_date(m.group(1)) if m else None
        if not date or (period_end and date < period_end[:len(date)]):
            continue
        row["maturityDate"] = date
        row["maturityDateSource"] = "INSTRUMENT_NAME"


_AGGREGATE_MAX_ROWS = 16


def _mark_aggregates(rows: list[dict]) -> list[dict]:
    """An undated row whose amount is, within half a percent, the sum of exactly one set of two or more other rows on
    the same basis, each named for the same kind of instrument, is their aggregate (2.5.27: VRT's "Senior Unsecured
    Notes" 2,100.0 is its 2036, 2046, 2056 and 2066 notes, 600 + 500 + 500 + 500; the filing tags them side by side
    with no link). Each such row is marked aggregateOf and returned."""
    marked: list[dict] = []
    live = [r for r in rows if r.get("status") != "matured_before_period_end" and (_ladder_amount(r) or {"amount": 0})["amount"] > 0]
    for row in live:
        if row.get("maturityDate") is not None:
            continue
        own = _ladder_amount(row)
        words = re.split(r"\s+", str(row["instrument"]).strip())
        kind = words[-1].lower() if words else ""
        others = [r for r in live if r is not row and not r.get("aggregateOf") and _ladder_amount(r)["basis"] == own["basis"]
                  and kind in re.split(r"\s+", str(r["instrument"]).lower())]
        if len(others) < 2 or len(others) > _AGGREGATE_MAX_ROWS:
            continue
        matches: list[int] = []
        for mask in range(1, 1 << len(others)):
            if len(matches) >= 2:
                break
            if mask & (mask - 1) == 0:
                continue
            total = 0
            for i, other in enumerate(others):
                if mask & (1 << i):
                    total += _ladder_amount(other)["amount"]
            if abs(total - own["amount"]) <= _AGGREGATE_TOLERANCE * own["amount"]:
                matches.append(mask)
        if len(matches) != 1:
            continue
        row["aggregateOf"] = [r["instrument"] for i, r in enumerate(others) if matches[0] & (1 << i)]
        marked.append(row)
    return marked


def _maturity_from_text(rows: list[dict], statements: list[dict]) -> None:
    """A row with no tagged maturity takes the one date the filing text gives for its year (2.5.24, F-008: "The 2030
    Notes will mature on January 15, 2030" for "Convertible Notes Maturing 2030"). The year must appear in the
    instrument's name and the text must give exactly one date in it."""
    for row in _unplaced_rows(rows):
        years = set(re.findall(r"\b(?:19|20)\d{2}\b", str(row["instrument"]), re.A))
        hits = [s for s in statements if s["date"][:4] in years]
        dates = list(dict.fromkeys(h["date"] for h in hits))
        if len(dates) != 1:
            continue
        row["maturityDate"] = dates[0]
        row["maturityDateSource"] = "FILING_TEXT"
        row["maturityStatement"] = hits[0]["sentence"]


def _ladder_amount(row: dict) -> dict | None:
    """The amount a row adds to the year ladder and on what basis: the period-end carrying amount, else face, else a tagged amount."""
    if _is_number(row.get("carryingAmount")):
        return {"amount": row["carryingAmount"], "basis": "CARRYING"}
    if _is_number(row.get("faceAmount")):
        return {"amount": row["faceAmount"], "basis": "FACE"}
    if _is_number(row.get("taggedAmount")):
        return {"amount": row["taggedAmount"], "basis": "TAGGED"}
    return None


def capital_structure(ticker: str, source: IxSource, funding_matches: list[TextMatch], maturity_matches: list[TextMatch] | None = None) -> dict:
    """Cash, debt, instruments and maturities at the filing's period end, with the company's own funding statements."""
    doc = source.doc
    period_end = doc.document_period_end
    cash = _first_total(doc, _CASH_CONCEPTS, period_end)
    warnings: list[dict] = []
    # Investments and debt are read without rounded note figures or restated cash.
    balance_doc = _balance_facts(doc, cash, period_end)
    short_term = _short_term_investments(balance_doc, period_end)
    long_term_securities = _total(balance_doc, "MarketableSecuritiesNoncurrent", period_end)
    debt = _total_debt(balance_doc, period_end)
    ignored = [
        (_short_term_investments(doc, period_end), short_term),
        (_total(doc, "MarketableSecuritiesNoncurrent", period_end), long_term_securities),
    ]
    for raw, kept in ignored:
        if not raw or (kept and kept["value"] == raw["value"] and kept["concept"] == raw["concept"]):
            continue
        if _balance_exclusion(raw["value"], raw["periodEnd"], raw["decimals"], raw["sentence"], cash, period_end) == "overlaps_cash":
            warnings.append({"code": "OVERLAPS_CASH_EQUIVALENTS", "message": f"{raw['concept']} {_js_number(raw['value'])} is tagged in a sentence describing cash equivalents or money-market funds; it is part of cash, not added to it.", "severity": "info", "sentence": raw["sentence"]})
        else:
            warnings.append({"code": "ROUNDED_FACT_IGNORED", "message": f"{raw['concept']} {_js_number(raw['value'])} is rounded to {raw['decimals']} decimals against cash at {cash['decimals']}; read as a narrative figure, not a balance-sheet line.", "severity": "info"})
    # The filing's own cash-plus-investments total, when tagged, must agree.
    aggregate = _total(balance_doc, "CashCashEquivalentsAndShortTermInvestments", period_end)
    if aggregate and cash and short_term and abs(cash["value"] + short_term["value"] - aggregate["value"]) > _AGGREGATE_TOLERANCE * abs(aggregate["value"]):
        if abs(aggregate["value"] - cash["value"]) <= _AGGREGATE_TOLERANCE * abs(aggregate["value"]):
            warnings.append({"code": "CASH_AGGREGATE_MISMATCH", "message": f"The filing's cash and short-term investments total {_js_number(aggregate['value'])} equals cash alone, so {short_term['concept']} {_js_number(short_term['value'])} is already inside cash and is not added.", "severity": "info"})
            short_term = None
        else:
            warnings.append({"code": "CASH_AGGREGATE_MISMATCH", "message": f"Cash {_js_number(cash['value'])} plus {short_term['concept']} {_js_number(short_term['value'])} differs from the filing's cash and short-term investments total {_js_number(aggregate['value'])}.", "severity": "warning"})
    raw_debt = _total_debt(doc, period_end)
    if raw_debt and (not debt or debt["value"] != raw_debt["value"]):
        warnings.append({"code": "ROUNDED_FACT_IGNORED", "message": f"Total debt {_js_number(raw_debt['value'])} includes figures rounded far more coarsely than cash; they were left out.", "severity": "info"})
    if not debt and cash and _tags_no_borrowings(doc):
        debt = {"value": 0, "components": [], "basis": "No borrowing concepts tagged in the filing"}
        warnings.append({"code": "NO_BORROWINGS_TAGGED", "message": "The filing tags no borrowings at any date, so total debt is taken as zero (leases excluded).", "severity": "info"})
    if debt and debt.get("leases") == "SUBTRACTED":
        warnings.append({"code": "FINANCE_LEASES_SUBTRACTED", "message": f"The filing tags debt only with finance leases included; total debt is {debt['basis']} = {_js_number(debt['value'])}.", "severity": "info"})
    elif debt and debt.get("leases") == "INCLUDED":
        warnings.append({"code": "TOTAL_DEBT_INCLUDES_FINANCE_LEASES", "message": f"The filing tags debt only with finance leases included and tags no finance-lease liability to take out; total debt {_js_number(debt['value'])} includes finance leases.", "severity": "warning"})
    instrument_rows = _instruments(doc, period_end)
    if maturity_matches:
        _maturity_from_text(instrument_rows, _maturity_statements(maturity_matches))
    _maturity_from_name(instrument_rows, period_end)
    for row in _mark_aggregates(instrument_rows):
        warnings.append({"code": "AGGREGATE_ROW_EXCLUDED", "message": f"{row['instrument']} ({_js_number(_ladder_amount(row)['amount'])}) equals the sum of {', '.join(row['aggregateOf'])}; it is left out of the ladder, coverage and reconciliation so it is not counted twice.", "severity": "info"})
    from_name = [r for r in instrument_rows if r.get("maturityDateSource") == "INSTRUMENT_NAME"]
    if from_name:
        listed_names = ", ".join(f"{r['instrument']} ({r['maturityDate']})" for r in from_name)
        warnings.append({"code": "MATURITY_FROM_INSTRUMENT_NAME", "message": f"No tagged or stated maturity date for {listed_names}; dated from the instrument name, to the year or month it gives.", "severity": "info"})
    for row in instrument_rows:
        if isinstance(row.get("maturityDates"), list):
            warnings.append({"code": "MULTIPLE_MATURITIES", "message": f"{row['instrument']} carries {len(row['maturityDates'])} tagged maturities ({', '.join(row['maturityDates'])}) on one amount; the year ladder places it at the earliest.", "severity": "info"})
        named = _LABEL_DATE_RE.search(str(row["instrument"]))
        named_date = normalize_ix_date(named.group(0)) if named else None
        if named_date and row.get("maturityDateSource") == "XBRL" and named_date not in (row.get("maturityDates") or [row["maturityDate"]]):
            warnings.append({"code": "LABEL_DATE_DIFFERS_FROM_MATURITY", "message": f"{row['instrument']} is named for {named.group(0)} but its tagged maturity is {row['maturityDate']}; the tagged date is used.", "severity": "info"})
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
    # Each bucket states its amount's basis; faceAmount sums only rows tagged with a face amount (2.5.24, F-009:
    # it summed carrying amounts under the face name).
    by_year: dict[str, dict] = {}
    laddered = 0
    laddered_any = 0
    laddered_bases: list[str] = []
    for row in instrument_rows:
        maturity = row["maturityDate"]
        amount = _ladder_amount(row)
        if not maturity or not amount or row["status"] == "matured_before_period_end" or row.get("aggregateOf"):
            continue
        year = maturity[:4]
        entry = by_year.get(year) or {"year": year, "amount": 0, "bases": [], "faceAmount": None, "instruments": []}
        entry["amount"] += amount["amount"]
        if amount["basis"] not in entry["bases"]:
            entry["bases"].append(amount["basis"])
        if _is_number(row["faceAmount"]):
            entry["faceAmount"] = (entry["faceAmount"] or 0) + row["faceAmount"]
        entry["instruments"].append(str(row["instrument"]))
        by_year[year] = entry
        if _is_number(row["carryingAmount"]):
            laddered += row["carryingAmount"]
        laddered_any += amount["amount"]
        if amount["basis"] not in laddered_bases:
            laddered_bases.append(amount["basis"])
    instrument_ladder = [
        {"year": e["year"], "amount": e["amount"], "amountBasis": e["bases"][0] if len(e["bases"]) == 1 else "MIXED", "faceAmount": e["faceAmount"], "instruments": e["instruments"]}
        for e in sorted(by_year.values(), key=lambda e: e["year"])
    ]
    # How much of total debt the year ladder places, at carrying amounts like total debt, and what it cannot
    # (2.5.24, F-008: AAOI's ladder held 58.9M of 188.1M with nothing said).
    debt_value = debt["value"] if debt and _is_number(debt["value"]) else None
    unplaced = [{"instrument": r["instrument"], "amount": _ladder_amount(r)["amount"], "amountBasis": _ladder_amount(r)["basis"], "reason": "NO_MATURITY_DATE"} for r in _unplaced_rows(instrument_rows)]
    ladder_coverage = {
        "totalDebt": debt_value,
        "ladderedCarryingAmount": laddered,
        "coveragePct": round_half_up((laddered / debt_value) * 100, 2) if debt_value and debt_value > 0 else None,
        # Every bucket amount on whatever basis it has (face-only ladders like VRT's carry no carrying amounts).
        "ladderedAmount": laddered_any,
        "ladderedAmountBasis": None if not laddered_bases else laddered_bases[0] if len(laddered_bases) == 1 else "MIXED",
        "notLaddered": unplaced,
    }
    # When an outstanding instrument has no maturity and the filing tags no maturity ladder of its own.
    if unplaced and not ladder:
        listed = ", ".join(f"{u['instrument']} ({_js_number(u['amount'])})" for u in unplaced)
        # A ladder on face or tagged amounts says so, rather than "places 0" at carrying amounts (2.5.26: VRT).
        basis = ladder_coverage["ladderedAmountBasis"]
        if basis is None or basis == "CARRYING":
            of_total = f" of total debt {_js_number(debt_value)} ({'null' if ladder_coverage['coveragePct'] is None else _js_number(ladder_coverage['coveragePct'])}%)" if debt_value else ""
            placed = f"the year ladder places {_js_number(laddered)}{of_total}."
        else:
            against = f" against total debt {_js_number(debt_value)}" if debt_value else ""
            placed = f"the year ladder places {_js_number(laddered_any)} on a {basis} basis ({_js_number(laddered)} at carrying amounts){against}."
        warnings.append({
            "code": "MATURITY_LADDER_INCOMPLETE",
            "message": f"No maturity date for {listed}; {placed}",
            "severity": "warning",
        })
    # Instrument rows' period-end carrying amounts against total debt (2.5.24, F-007: AAOI tags 124.9M principal as
    # the 2030 Notes' carrying amount; the balance sheet carries 129.1M).
    outstanding = [r for r in instrument_rows if r["status"] != "matured_before_period_end" and not r.get("aggregateOf") and _ladder_amount(r) is not None]
    carried = [r for r in outstanding if _is_number(r["carryingAmount"])]
    carried_total = sum((r["carryingAmount"] for r in carried), 0)
    without_carrying = len(outstanding) - len(carried)
    # Compared only when every outstanding row has a period-end carrying amount (2.5.25: VRT's notes carry only face
    # amounts, so its rows summed to 0 against 2.94B and read as a gap).
    if not debt_value or not carried or without_carrying > 0:
        reconciliation = {"status": "NOT_COMPARABLE", "instrumentsCarryingTotal": None, "totalDebt": debt_value, "difference": None, "rowsWithoutCarryingAmount": without_carrying}
    else:
        reconciliation = {
            "status": "RECONCILED" if abs(carried_total - debt_value) <= _AGGREGATE_TOLERANCE * abs(debt_value) else "NOT_RECONCILED",
            "instrumentsCarryingTotal": carried_total,
            "totalDebt": debt_value,
            "difference": debt_value - carried_total,
            "rowsWithoutCarryingAmount": without_carrying,
        }
    if reconciliation["status"] == "NOT_RECONCILED":
        warnings.append({
            "code": "INSTRUMENTS_DO_NOT_RECONCILE",
            "message": f"Instrument rows carry {_js_number(carried_total)} at the period end against total debt {_js_number(debt_value)} (difference {_js_number(reconciliation['difference'])}): "
                       "an instrument may be untagged, or a tagged amount may be principal rather than carrying value.",
            "severity": "warning",
        })
    # Convertible rows against the balance sheet's own convertible line (2.5.26, F-007: AAOI tags the 2030 Notes'
    # "approximately $124.9 million" principal as DebtInstrumentCarryingAmount; the balance sheet carries them at
    # 129,142,000). The rows are marked, not rewritten: the balance-sheet line is not tagged to the instrument.
    convertible_rows = [r for r in carried if r.get("convertible") is True]
    convertible_line = _convertible_balance(balance_doc, period_end) if convertible_rows else None
    if convertible_line:
        line_total, line_parts = convertible_line
        rows_total = sum((r["carryingAmount"] for r in convertible_rows), 0)
        line_concept = " + ".join(part["concept"] for part in line_parts)
        matches = abs(rows_total - line_total) <= _AGGREGATE_TOLERANCE * abs(line_total)
        reconciliation["convertibleBalanceSheet"] = {
            "status": "RECONCILED" if matches else "NOT_RECONCILED",
            "concept": line_concept,
            "balanceSheetAmount": line_total,
            "instrumentRowsTotal": rows_total,
            "difference": line_total - rows_total,
        }
        if not matches:
            for row in convertible_rows:
                row["carryingAmountBasis"] = "MAY_BE_PRINCIPAL"
            carried_list = "; ".join(f"{r['instrument']} carries {_js_number(r['carryingAmount'])} under {r['carryingAmountConcept']}" for r in convertible_rows)
            warnings.append({
                "code": "CARRYING_AMOUNT_MAY_BE_PRINCIPAL",
                "message": f"{carried_list}, "
                           f"but the balance sheet carries convertible notes at {_js_number(line_total)} ({line_concept}); the row amount may be principal, not carrying value.",
                "severity": "warning",
            })
    # What separates the ladder from total debt: rows it cannot place, and rows whose amounts do not add up to it.
    if debt_value is not None:
        unplaced_total = sum((u["amount"] for u in unplaced), 0)
        ladder_coverage["gap"] = {
            "amount": debt_value - laddered,
            "inNotLaddered": unplaced_total,
            "inReconciliationDifference": reconciliation["difference"] if reconciliation["status"] == "NOT_RECONCILED" else None,
        }
    liquid = (cash["value"] if cash else 0) + (short_term["value"] if short_term else 0)
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
            "currency": cash["unit"] if cash else None,
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
        "ladderCoverage": ladder_coverage,
        "instrumentReconciliation": reconciliation,
        "fundingStatements": funding,
        "methodology": [
            "Balances are the filing's own tagged values at its period end; nothing is projected forward.",
            "Instrument rows come from facts dimensioned by debt instrument, which companyfacts omits; face amounts can be original principal (faceAmountDate says when it was tagged).",
            "The year ladder sums each row's period-end carrying amount, else its face amount, else a tagged amount (amountBasis); a row with no tagged maturity takes one only when the filing text states a single maturity date in the year its name carries, else the year or month its name gives after Due or Maturing (maturityDateSource INSTRUMENT_NAME).",
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


# Target attribution (2.4.4): a headline such as "Rocket Lab climbs as Cantor
# reiterates $122 target; ... AST SpaceMobile rises" names several companies,
# and the target belongs to the one in its own clause.
_LEGAL_SUFFIXES = {"inc", "incorporated", "corp", "corporation", "ltd", "limited", "llc", "plc", "co", "company", "sa", "ag", "nv", "se", "gmbh", "holdings", "group"}


def _norm_phrase(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def subject_aliases(ticker: str, names: list[str]) -> list[str]:
    """Lowercase phrases that name the subject: the ticker (3+ letters) and the company name with and without its legal suffix."""
    out: set[str] = set()
    base = _norm_phrase(ticker.split(".")[0])
    if len(base) >= 3:
        out.add(base)
    for name in names:
        norm = _norm_phrase(name)
        if not norm:
            continue
        words = norm.split(" ")
        while len(words) > 1 and words[-1] in _LEGAL_SUFFIXES:
            words.pop()
        for alias in (norm, " ".join(words)):
            if len(alias) >= 3:
                out.add(alias)
    return sorted(out)


def _mentions_subject(text: str, aliases: list[str]) -> bool:
    padded = f" {_norm_phrase(text)} "
    return any(f" {a} " in padded for a in aliases)


def _target_clause(sentence: str) -> str:
    """The clause of a sentence (split at "; " and ", ") that carries its price target, else the sentence."""
    return next((clause for clause in re.split(r";\s*|,\s+", sentence) if _price_target(clause) is not None), sentence)


def analyst_valuation_methods(ticker: str, items: list[dict], changes: list[dict], issuer_names: list[str] | None = None) -> dict:
    """Valuation methods named in news headlines and summaries: context, never model inputs.

    With issuer_names, each target and method must belong to the subject: its
    clause names the subject, or no sentence of the item names another
    company's target while the item names the subject. Without issuer_names
    nothing is checked (subjectMatch NOT_CHECKED).
    """
    aliases = subject_aliases(ticker, issuer_names) if issuer_names is not None else None
    rejected_other_company = 0
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
        item_sentences = _sentences(text)
        # An item is conflicted when a sentence names the subject but puts a
        # target in a clause about another company.
        conflicted = aliases is not None and any(
            _price_target(s) is not None and _mentions_subject(s, aliases) and not _mentions_subject(_target_clause(s), aliases)
            for s in item_sentences)
        item_names_subject = aliases is not None and _mentions_subject(text, aliases)
        for sentence in item_sentences:
            methods = _sentence_methods(sentence)
            target = _price_target(sentence)
            if not methods and not (target and firm):
                continue
            subject_match = "NOT_CHECKED"
            if aliases is not None:
                clause = _target_clause(sentence) if target else sentence
                if _mentions_subject(clause, aliases):
                    subject_match = "CLAUSE"
                elif not _mentions_subject(sentence, aliases) and item_names_subject and not conflicted:
                    subject_match = "ITEM"
                else:
                    rejected_other_company += 1
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
                "subjectMatch": subject_match,
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
        "attribution": ({"checked": True, "subjectAliases": aliases, "rejectedForOtherCompany": rejected_other_company}
                        if aliases is not None else {"checked": False, "subjectAliases": [], "rejectedForOtherCompany": 0}),
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
