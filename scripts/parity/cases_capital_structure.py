"""Parity cases: SEC extractors (yfmcp/capital_structure.py, share_scenarios.py, funding_schedule.py,
parsing/extractors.py  <->  worker/src/capital-structure.ts, share-scenarios.ts, funding-schedule.ts, yahoo-finance.ts).

Covers the P0 rules of the `extractors` domain not already covered by cases_extraction_rules.py:
BR-EXT-001/002 (conversion ratio units), 003/011 (convertible if-converted shares and principal basis),
004 (convertible preferred), 005/013/082/083 (scenario convertible treatments, net-share settlement),
006 (debt instrument status), 007/008 (dilution bridge totals and status), 009 (geographic revenue table),
012 (instrument-to-total-debt reconciliation), 018-021/024 (share-count scenarios), 022 (cash aggregate),
023 (total debt hierarchy), 025 (unvested awards), 026 (warrant retirement in text), 027 (warrant exercisable
count), and the scheduled-repair rules BR-EXT-003/082/083 (Q3f, in-the-money boundary) and
BR-EXT-044/045/059/064/110 (Q3i, Feb-29 `_add_years`, foreign dollars labelled USD) as pinned cases.

Expected values are derived from the rule cards (analysis/yahoo-finance-mcp/BUSINESS_RULES.md) and the literal
inputs, then run against both runtimes. Where a runtime result differs from the card the comment says so.
The iXBRL fixtures are the ones scripts/test_capital_structure.py and scripts/test_scenarios_and_schedule.py
already build (imported, not copied), plus the small purpose-built filings below.
"""

from __future__ import annotations

from scripts import test_capital_structure as T
from scripts import test_scenarios_and_schedule as S
from scripts.parity.harness import Case
from yfmcp import capital_structure as cs
from yfmcp import funding_schedule as fs
from yfmcp import share_scenarios as ss
from yfmcp.parsing import extractors as ex

PE = "2026-06-30"
URL = T.Q_URL


# ───────────────────────────────────────────────────────────────── small iXBRL filings

def _filing(ctxs: list[str], body: str, *, end: str = PE, end_text: str = "June 30, 2026", start: str = "2026-04-01",
            doc_type: str = "10-Q", shares: str | None = "100,000,000") -> str:
    """A minimal inline-XBRL 10-Q/10-K: a duration context `dur`, an instant `end` at the period end, a cover date `cov`."""
    base = [T._context("dur", f"{start}..{end}"), T._context("end", end), T._context("cov", "2026-08-01")]
    cover = f"<p>Shares outstanding: {T._num('dei:EntityCommonStockSharesOutstanding', 'cov', 'shares', shares)}</p>" if shares else ""
    return (f'<html><body><div style="display:none"><ix:header><ix:hidden>'
            f'{T._text("dei:DocumentType", "dur", doc_type)}'
            f'{T._text("dei:DocumentPeriodEndDate", "dur", end_text, "ixt:date-monthname-day-year-en")}'
            f'</ix:hidden><ix:resources>{"".join(base + ctxs)}{T.UNITS}</ix:resources></ix:header></div>'
            f'{cover}{body}</body></html>')


def _note(tag: str, member: str, *, face: float | None = None, face_at: str = "iss", price: float | None = None,
          ratio: float | None = None, carrying: float | None = None, issuable: float | None = None,
          maturity: str | None = None) -> tuple[list[str], str]:
    """Contexts and body for one convertible note: face at issue (`iss`) or at the period end (`end`), price/ratio at the period end."""
    ctxs = [T._context(f"{tag}iss", "2025-03-01", {T.DEBT: member}), T._context(f"{tag}end", PE, {T.DEBT: member})]
    parts = []
    if face is not None:
        parts.append(f"face {T._num('us-gaap:DebtInstrumentFaceAmount', tag + face_at, 'usd', f'{face:,.0f}')}")
    if price is not None:
        parts.append(f"price {T._num('us-gaap:DebtInstrumentConvertibleConversionPrice1', tag + 'end', 'usdPerShare', f'{price}')}")
    if ratio is not None:
        parts.append(f"ratio {T._num('us-gaap:DebtInstrumentConvertibleConversionRatio1', tag + 'end', 'pure', f'{ratio}')}")
    if carrying is not None:
        parts.append(f"carrying {T._num('us-gaap:DebtInstrumentCarryingAmount', tag + 'end', 'usd', f'{carrying:,.0f}')}")
    if issuable is not None:
        parts.append(f"issuable {T._num('us-gaap:DebtInstrumentConvertibleNumberOfSharesAvailableForConversion', tag + 'end', 'shares', f'{issuable:,.0f}')}")
    if maturity is not None:
        parts.append(f"maturing {T._text('us-gaap:DebtInstrumentMaturityDate', tag + 'end', maturity, 'ixt:date-monthname-day-year-en')}")
    return ctxs, f"<p>{tag}: " + "; ".join(parts) + ".</p>"


def _conv_filing(*notes: tuple[list[str], str]) -> str:
    return _filing([c for ctxs, _ in notes for c in ctxs], "".join(b for _, b in notes))


# ───────────────────────────────────────────────────────────────── bridge / capital structure runners

def _tm(texts: list[str] | None, heading: str = "Equity") -> list | None:
    return None if texts is None else [cs.TextMatch(t, heading, T.K_URL, "2026-08-20", None) for t in texts]


def _bridge_py(html: str, price: float, filing: str, claims, lifecycle, settlement, capped, currency: str, tables=None) -> dict:
    src = cs.IxSource("primary", filing, "2026-08-10", "0001234568-26-000999", URL, cs.parse_ixbrl(html))
    table_matches = None if tables is None else [cs.TextMatch(t["contextText"], t["sectionHeading"], t["documentUrl"], t["filingDate"],
                                                              t["accessionNumber"], t["inTable"], t["tableTitle"], t["rowLabel"]) for t in tables]
    return cs.dilution_bridge("TST", price, currency, None, [src], [], table_matches, _tm(claims), _tm(lifecycle), _tm(settlement), _tm(capped))


_BRIDGE_JS = """
const tm = (l) => l == null ? null : l.map((t) => ({ contextText: t, sectionHeading: "Equity", documentUrl: fx.kUrl, filingDate: "2026-08-20", accessionNumber: null }));
const doc = m.capitalStructure.parseIxbrl(fx.html);
const src = { role: "primary", filingType: args[1], filingDate: "2026-08-10", accessionNumber: "0001234568-26-000999", documentUrl: fx.url, doc };
return m.capitalStructure.dilutionBridge({ ticker: "TST", price: args[0], priceCurrency: args[6], asOfDate: null, sources: [src], atmMatches: [],
  awardTableMatches: args[7] ?? undefined, claimMatches: tm(args[2]), warrantLifecycleMatches: tm(args[3]),
  convertibleSettlementMatches: tm(args[4]), cappedCallMatches: tm(args[5]) });
"""


def _bridge(name: str, ids: list[str], html: str, price: float, *, filing: str = "10-Q", claims=None, lifecycle=None, settlement=None,
            capped=None, currency: str = "USD", tables=None, **kw) -> Case:
    """dilution_bridge on one iXBRL document in both runtimes; text inputs are lists of sentences (None = not read)."""
    return Case(name=name, ids=ids,
                py=lambda: _bridge_py(html, price, filing, claims, lifecycle, settlement, capped, currency, tables),
                ts={"js": _BRIDGE_JS, "args": [price, filing, claims, lifecycle, settlement, capped, currency, tables],
                    "fx": {"html": html, "url": URL, "kUrl": T.K_URL}}, **kw)


def _capital_py(html: str, filing: str, maturity) -> dict:
    src = cs.IxSource("primary", filing, "2026-08-10", "0001234568-26-000999", URL, cs.parse_ixbrl(html))
    return cs.capital_structure("TST", src, [], _tm(maturity))


_CAPITAL_JS = """
const tm = (l) => l == null ? undefined : l.map((t) => ({ contextText: t, sectionHeading: "Equity", documentUrl: fx.kUrl, filingDate: "2026-08-20", accessionNumber: null }));
const doc = m.capitalStructure.parseIxbrl(fx.html);
const src = { role: "primary", filingType: args[0], filingDate: "2026-08-10", accessionNumber: "0001234568-26-000999", documentUrl: fx.url, doc };
return m.capitalStructure.capitalStructure({ ticker: "TST", source: src, fundingMatches: [], maturityMatches: tm(args[1]) });
"""


def _capital(name: str, ids: list[str], html: str, *, filing: str = "10-Q", maturity=None, **kw) -> Case:
    """capital_structure on one iXBRL document in both runtimes."""
    return Case(name=name, ids=ids, py=lambda: _capital_py(html, filing, maturity),
                ts={"js": _CAPITAL_JS, "args": [filing, maturity], "fx": {"html": html, "url": URL, "kUrl": T.K_URL}}, **kw)


def comp(v: dict, name: str) -> dict | None:
    return next((c for c in (v.get("components") or []) if c["component"] == name), None)


def pick(d: dict | None, *keys: str) -> dict | None:
    return None if d is None else {k: d.get(k) for k in keys}


def codes(v: dict) -> list[str]:
    return [w["code"] for w in v.get("warnings") or []]


# ───────────────────────────────────────────────────────────────── share scenario runners

_SCEN_JS = """
const out = {};
for (const [label, run] of Object.entries(args[0])) {
  out[label] = m.shareScenarios.shareCountScenarios("tst", run.bridge, m.shareScenarios.parseShareScenarios(run.scenarios).scenarios);
}
return out;
"""


def _scen(name: str, ids: list[str], runs: dict, sel, **kw) -> Case:
    """share_count_scenarios for each labelled {bridge, scenarios} run; `sel` is applied to each run's result."""
    def py():
        return {k: ss.share_count_scenarios("tst", r["bridge"], ss.parse_share_scenarios(r["scenarios"])["scenarios"]) for k, r in runs.items()}
    return Case(name=name, ids=ids, py=py, ts={"js": _SCEN_JS, "args": [runs]},
                select=lambda v: {k: sel(r) for k, r in v.items()}, **kw)


def _inv(components: list[dict], basic: float | None = 100_000_000, *, atm: dict | None = None, claims: list | None = None,
         text_scan: str | None = "READ") -> dict:
    """A dilution-bridge inventory as share_count_scenarios reads it."""
    b: dict = {"basicShares": None if basic is None else {"shares": basic}, "components": components, "atmProgram": atm,
               "unquantifiedShareClaims": claims or []}
    if text_scan:
        b["claimCoverage"] = {"scope": "TAGGED_INSTRUMENTS", "completeClaimInventory": False, "textScan": text_scan}
    return b


def lines(*names: str, fields=("instrument", "incrementalShares", "included")):
    """Select: per scenario in `names` (all when empty), the chosen fields of each instrument line."""
    def sel(r: dict) -> dict:
        return {s["name"]: [{k: ln.get(k) for k in fields} for ln in s["instruments"]] for s in r["scenarios"] if not names or s["name"] in names}
    return sel


def totals(r: dict) -> dict:
    return {s["name"]: {"resultingShares": s["totals"]["resultingShares"], "dilutionPct": s["totals"]["dilutionPct"],
                        "knownIssuanceShares": s["totals"]["knownIssuanceShares"], "completeness": s["completeness"]} for s in r["scenarios"]} | {"status": r["status"]}


_NOTE_ID = "principal / conversion_price"
_CASH = {"stated": "PRINCIPAL_IN_CASH", "scope": "ALL_NOTES"}


def _notes(shares: float | None, conv: float | None, principal: float | None = None, settled: bool = False, name: str = "Notes") -> dict:
    inst = {"instrument": name, "principal": principal, "ifConvertedShares": shares, "conversionPrice": conv, "ifConvertedBasis": _NOTE_ID,
            "principalSettlement": _CASH if settled else None}
    return {"component": "convertible_debt", "instruments": [inst]}


def _sc(name: str, price: float, **treat) -> dict:
    return {"name": name, "price": price, **treat}


CASES: list[Case] = []
add = CASES.append

# ═════════════════════════════════════════════════════ BR-EXT-001 / 002: conversion ratio units

def _ratio(name: str, ids: list[str], ratio, price, expect: dict, **kw) -> Case:
    return Case(name=name, ids=ids, py=lambda: cs.normalized_ratio(ratio, price),
                ts={"module": "capitalStructure", "fn": "normalizedRatio", "args": [ratio, price]}, expect=expect, **kw)


_R12 = ["BR-EXT-001", "BR-EXT-002"]
add(_ratio("cap.ratio.per_1_scaled_to_per_1000", _R12, 0.0076319, 131.03,
           {"per1000": 7.6319, "usable": True, "note": "RATIO_PER_1_PRINCIPAL_SCALED"}))
add(_ratio("cap.ratio.inconsistent_with_price_not_used", _R12, 2.6926, 194.97,
           {"per1000": 2.6926, "usable": False, "note": "RATIO_INCONSISTENT_WITH_PRICE"}))
add(_ratio("cap.ratio.per_1000_agrees_with_price", _R12, 50, 20, {"per1000": 50, "usable": True, "note": None}))
add(_ratio("cap.ratio.per_1000_within_two_percent", _R12, 50.9, 20, {"per1000": 50.9, "usable": True, "note": None}))
add(_ratio("cap.ratio.per_1000_beyond_two_percent_rejected", _R12, 51.5, 20,
           {"per1000": 51.5, "usable": False, "note": "RATIO_INCONSISTENT_WITH_PRICE"}))
add(_ratio("cap.ratio.no_price_ratio_at_least_1_used_as_is", _R12, 12.5, None, {"per1000": 12.5, "usable": True, "note": None}))
add(_ratio("cap.ratio.no_price_ratio_below_1_unit_uncertain", _R12, 0.5, None, {"per1000": None, "usable": False, "note": "RATIO_UNIT_UNCERTAIN"}))
add(_ratio("cap.ratio.zero_unusable", _R12, 0, 20, {"per1000": None, "usable": False, "note": None}))
add(_ratio("cap.ratio.negative_unusable", _R12, -3, 20, {"per1000": None, "usable": False, "note": None}))
add(_ratio("cap.ratio.missing_unusable", _R12, None, 20, {"per1000": None, "usable": False, "note": None}))

_NOTE_FIELDS = ("conversionPrice", "conversionPriceBasis", "conversionRatioPer1000", "conversionRatioTagged", "conversionRatioUsed", "conversionRatioNote",
                "ifConvertedShares", "ifConvertedBasis", "principal", "principalBasis", "inTheMoney", "incrementalShares")


def conv_view(v: dict) -> dict:
    """The first convertible-note instrument, the component's incremental shares and the bridge's convertibleDebt line."""
    c = comp(v, "convertible_debt")
    return {"instrument": pick((c or {}).get("instruments", [None])[0], *_NOTE_FIELDS) if c else None,
            "component": None if c is None else c["incrementalShares"], "bridge": (v.get("bridge") or {}).get("convertibleDebt")}


CONV = "tst:ConvertibleNotesDue2030Member"

# LITE (card example): ratio tagged per $1 of principal, 0.0076319 x 131.03 = 1.0000.
add(_bridge("cap.ratio.lite_per_1_ratio_end_to_end", _R12, T.LITE_K, 700, filing="10-K", claims=[], select=conv_view,
            expect={"instrument": {"conversionPrice": 131.03, "conversionPriceBasis": "tagged", "conversionRatioPer1000": 7.6319, "conversionRatioTagged": 0.0076319,
                                   "conversionRatioUsed": True, "conversionRatioNote": "RATIO_PER_1_PRINCIPAL_SCALED",
                                   # 179,600,000 / 1000 x 7.6319 = 1,370,689.24
                                   "ifConvertedShares": 1_370_689, "ifConvertedBasis": "principal / 1000 * conversion_ratio",
                                   "principal": 179_600_000, "principalBasis": "outstanding_at_period_end", "inTheMoney": True, "incrementalShares": 1_370_689},
                    "component": 1_370_689, "bridge": 1_370_689}))
# BE (card example): the tagged 2.6926 is a make-whole increase, product with $194.97 is 525/1000, so the shares fall back to principal / price.
add(_bridge("cap.ratio.be_make_whole_ratio_rejected_falls_back_to_price", _R12, T.BE_Q, 300, claims=[],
            select=lambda v: {"notes": {i["instrument"]: pick(i, "conversionRatioTagged", "conversionRatioPer1000", "conversionRatioUsed", "conversionRatioNote",
                                                              "ifConvertedBasis", "ifConvertedShares", "principal")
                                        for i in comp(v, "convertible_debt")["instruments"]},
                              "codes": [c for c in codes(v) if c == "CONVERSION_RATIO_INCONSISTENT"]},
            expect={"notes": {
                "Convertible Senior Notes Due November 2030": {
                    "conversionRatioTagged": 2.6926, "conversionRatioPer1000": 2.6926, "conversionRatioUsed": False,
                    "conversionRatioNote": "RATIO_INCONSISTENT_WITH_PRICE", "ifConvertedBasis": "principal / conversion_price",
                    # 2,500,000,000 / 194.97 = 12,822,485.5
                    "ifConvertedShares": 12_822_486, "principal": 2_500_000_000},
                "Green Convertible Senior Notes Due June 2028": {
                    "conversionRatioTagged": 22.543, "conversionRatioPer1000": 22.543, "conversionRatioUsed": False,
                    "conversionRatioNote": "RATIO_INCONSISTENT_WITH_PRICE", "ifConvertedBasis": "shares_issuable_tagged_at_period_end",
                    "ifConvertedShares": 59_486, "principal": 787_000}},
                "codes": ["CONVERSION_RATIO_INCONSISTENT"]}))

# ═════════════════════════════════════════════════════ BR-EXT-003 / 011: convertible note if-converted shares
_R3 = ["BR-EXT-003", "BR-EXT-011"]
_FACE_ONLY = _conv_filing(_note("a", CONV, face=300_000_000, price=50.0))

add(_bridge("cap.ifconv.principal_over_conversion_price_in_the_money", _R3, _FACE_ONLY, 60, claims=[], select=conv_view,
            expect={"instrument": {"conversionPrice": 50, "conversionPriceBasis": "tagged", "conversionRatioPer1000": None, "conversionRatioTagged": None,
                                   "conversionRatioUsed": False, "conversionRatioNote": None, "ifConvertedShares": 6_000_000, "ifConvertedBasis": _NOTE_ID,
                                   "principal": 300_000_000, "principalBasis": "face_amount", "inTheMoney": True, "incrementalShares": 6_000_000},
                    "component": 6_000_000, "bridge": 6_000_000}))
add(_bridge("cap.ifconv.below_conversion_price_adds_nothing", _R3, _FACE_ONLY, 49.99, claims=[],
            select=lambda v: pick(conv_view(v)["instrument"], "ifConvertedShares", "inTheMoney", "incrementalShares"),
            expect={"ifConvertedShares": 6_000_000, "inTheMoney": False, "incrementalShares": 0}))
# Scheduled repair Q3f (BR-EXT-003 card, "Suspected defect"): the convertible test is price >= conversion price while options and
# warrants need price > strike, so at exactly the conversion price a note counts all its shares and an option counts none.
add(Case(name="cap.ifconv.price_equal_conversion_price_counts_all_shares", ids=_R3, mode="pinned", decision="Q3f",
         py=lambda: _bridge_py(_FACE_ONLY, 50, "10-Q", [], None, None, None, "USD"),
         ts={"js": _BRIDGE_JS, "args": [50, "10-Q", [], None, None, None, "USD", None], "fx": {"html": _FACE_ONLY, "url": URL, "kUrl": T.K_URL}},
         select=lambda v: pick(conv_view(v)["instrument"], "ifConvertedShares", "inTheMoney", "incrementalShares"),
         expect={"ifConvertedShares": 6_000_000, "inTheMoney": True, "incrementalShares": 6_000_000},
         note="repair: boundary rule for in-the-money (>= for convertibles, > for options/warrants) to be decided by Q3f"))

# Card 003 example: $300M face, $250M carrying (< 95% of face), $50 conversion price, price $60.
add(_bridge("cap.ifconv.reduced_carrying_amount_is_the_principal", _R3,
            _conv_filing(_note("a", CONV, face=300_000_000, price=50.0, carrying=250_000_000)), 60, claims=[],
            select=lambda v: pick(conv_view(v)["instrument"], "principal", "principalBasis", "ifConvertedShares", "incrementalShares"),
            expect={"principal": 250_000_000, "principalBasis": "outstanding_at_period_end", "ifConvertedShares": 5_000_000, "incrementalShares": 5_000_000}))
add(_bridge("cap.ifconv.carrying_near_face_keeps_face_amount", _R3,
            _conv_filing(_note("a", CONV, face=300_000_000, price=50.0, carrying=290_000_000)), 60, claims=[],
            select=lambda v: pick(conv_view(v)["instrument"], "principal", "principalBasis", "ifConvertedShares"),
            expect={"principal": 300_000_000, "principalBasis": "face_amount", "ifConvertedShares": 6_000_000}))
add(_bridge("cap.ifconv.tagged_shares_issuable_come_first", _R3,
            _conv_filing(_note("a", CONV, face=300_000_000, price=50.0, issuable=7_000_000)), 60, claims=[],
            select=lambda v: pick(conv_view(v)["instrument"], "ifConvertedShares", "ifConvertedBasis", "incrementalShares"),
            expect={"ifConvertedShares": 7_000_000, "ifConvertedBasis": "shares_issuable_tagged_at_period_end", "incrementalShares": 7_000_000}))
add(_bridge("cap.ifconv.ratio_beats_price_when_consistent", _R3,
            _conv_filing(_note("a", CONV, face=300_000_000, price=25.0, ratio=40.0)), 30, claims=[],
            select=lambda v: pick(conv_view(v)["instrument"], "ifConvertedShares", "ifConvertedBasis", "conversionRatioUsed", "conversionPrice"),
            expect={"ifConvertedShares": 12_000_000, "ifConvertedBasis": "principal / 1000 * conversion_ratio", "conversionRatioUsed": True, "conversionPrice": 25}))
add(_bridge("cap.ifconv.ratio_only_implies_conversion_price", _R3,
            _conv_filing(_note("a", CONV, face=300_000_000, ratio=40.0)), 30, claims=[],
            select=lambda v: pick(conv_view(v)["instrument"], "ifConvertedShares", "conversionPrice", "conversionPriceBasis", "inTheMoney", "incrementalShares"),
            expect={"ifConvertedShares": 12_000_000, "conversionPrice": 25, "conversionPriceBasis": "1000 / conversion_ratio", "inTheMoney": True,
                    "incrementalShares": 12_000_000}))
add(_bridge("cap.ifconv.no_principal_is_unresolved", _R3, _conv_filing(_note("a", CONV, price=50.0)), 60, claims=[],
            select=lambda v: {**conv_view(v), "status": v["status"], "unresolved": v["unresolved"]},
            expect={"instrument": {"conversionPrice": 50, "conversionPriceBasis": "tagged", "conversionRatioPer1000": None, "conversionRatioTagged": None,
                                   "conversionRatioUsed": False, "conversionRatioNote": None, "ifConvertedShares": None, "ifConvertedBasis": None,
                                   "principal": None, "principalBasis": None, "inTheMoney": True, "incrementalShares": None},
                    "component": None, "bridge": None, "status": "PARTIAL", "unresolved": ["convertible_debt"]}))
# "Component incrementalShares is null only when every instrument is unresolved": one resolved note keeps the component's count.
_TWO = _conv_filing(_note("a", CONV, face=300_000_000, price=50.0), _note("b", "tst:ConvertibleNotesDue2032Member", price=40.0))
add(_bridge("cap.ifconv.one_unresolved_note_leaves_component_resolved", _R3, _TWO, 60, claims=[],
            select=lambda v: {"component": comp(v, "convertible_debt")["incrementalShares"], "unresolvedInstruments": comp(v, "convertible_debt")["unresolvedInstruments"],
                              "partiallyResolved": v["partiallyResolved"], "unresolved": v["unresolved"], "status": v["status"]},
            expect={"component": 6_000_000, "unresolvedInstruments": 1, "partiallyResolved": ["convertible_debt"], "unresolved": [], "status": "PARTIAL"}))
# BE (card edge case): a redemption after the period end is only a warning; the 59,486 shares still count as of the period end.
add(_bridge("cap.ifconv.redemption_after_period_end_still_counted", _R3, T.BE_Q, 30, claims=[],
            select=lambda v: {"note": pick(next(i for i in comp(v, "convertible_debt")["instruments"] if "June 2028" in i["instrument"]),
                                          "ifConvertedShares", "incrementalShares", "afterPeriodEnd"),
                              "warned": "CONVERTIBLE_REDEMPTION_AFTER_PERIOD_END" in codes(v)},
            expect={"note": {"ifConvertedShares": 59_486, "incrementalShares": 59_486,
                             "afterPeriodEnd": {"concept": "us-gaap:DebtInstrumentRedemptionPricePercentage", "date": "2026-07-10"}}, "warned": True}))
