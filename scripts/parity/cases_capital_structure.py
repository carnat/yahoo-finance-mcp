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
# Finding (Phase 2, for Q3i triage): the card says "within 2%" (<= 0.02), but a ratio exactly 2% off (51 x $20 / 1000 = 1.02) is rejected in
# both runtimes because abs(1.02 - 1) evaluates to 0.020000000000000018 in IEEE doubles. Recorded as current behaviour.
add(_ratio("cap.ratio.exactly_two_percent_off_rejected_by_float_error", _R12, 51, 20,
           {"per1000": 51, "usable": False, "note": "RATIO_INCONSISTENT_WITH_PRICE"}))
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

# ═════════════════════════════════════════════════════ share-count scenarios (BR-EXT-018/019/020/021/024/005/013/082/083)
_OPT = {"component": "stock_options", "outstanding": 1_000_000, "weightedAverageExercisePrice": 10.0}
_F4 = ("instrument", "incrementalShares", "included")

# BR-EXT-018 / 021 card example: 1,000,000 options at $10, price $25.
add(_scen("cap.scen.options_treasury_stock_gross_exclude", ["BR-EXT-018", "BR-EXT-021"],
          {"r": {"bridge": _inv([_OPT]), "scenarios": [_sc("ts", 25), _sc("gross", 25, options="gross"), _sc("excl", 25, options="exclude")]}},
          lines(fields=("incrementalShares", "included", "thresholdPrice")),
          expect={"r": {"ts": [{"incrementalShares": 600_000, "included": True, "thresholdPrice": 10}],
                        "gross": [{"incrementalShares": 1_000_000, "included": True, "thresholdPrice": None}],
                        "excl": [{"incrementalShares": 0, "included": False, "thresholdPrice": None}]}}))
add(_scen("cap.scen.options_at_or_below_strike_add_nothing", ["BR-EXT-018", "BR-EXT-021"],
          {"r": {"bridge": _inv([_OPT]), "scenarios": [_sc("at-strike", 10), _sc("below", 8), _sc("gross-at-strike", 10, options="gross")]}},
          lines(fields=("incrementalShares", "included")),
          expect={"r": {"at-strike": [{"incrementalShares": 0, "included": True}], "below": [{"incrementalShares": 0, "included": True}],
                        "gross-at-strike": [{"incrementalShares": 1_000_000, "included": True}]}}))
_UNRES = _inv([{"component": "warrants", "classes": [
    {"class": "No Count", "outstanding": None, "exercisable": None, "exercisePrice": 10.0},
    {"class": "No Strike", "outstanding": 1_000_000, "exercisable": 1_000_000, "exercisePrice": None}]}])
add(_scen("cap.scen.untagged_count_or_strike_is_unresolved", ["BR-EXT-018", "BR-EXT-021", "BR-EXT-024"],
          {"r": {"bridge": _UNRES, "scenarios": [_sc("ts", 25), _sc("gross", 25, warrants="gross")]}},
          lambda r: {"lines": lines(fields=("instrument", "incrementalShares", "included", "unresolvedReason"))(r),
                     "totals": {s["name"]: [s["totals"]["resultingShares"], s["completeness"]] for s in r["scenarios"]}},
          expect={"r": {"lines": {
              "ts": [{"instrument": "No Count", "incrementalShares": None, "included": False, "unresolvedReason": "count not tagged"},
                     {"instrument": "No Strike", "incrementalShares": None, "included": False, "unresolvedReason": "exercise price not tagged"}],
              "gross": [{"instrument": "No Count", "incrementalShares": None, "included": False, "unresolvedReason": "count not tagged"},
                        {"instrument": "No Strike", "incrementalShares": 1_000_000, "included": True, "unresolvedReason": None}]},
              "totals": {"ts": [100_000_000, "EXCLUDES_UNRESOLVED_INSTRUMENTS"], "gross": [101_000_000, "EXCLUDES_UNRESOLVED_INSTRUMENTS"]}}}))
_TR = {"component": "stock_options", "outstanding": 2_000_000, "weightedAverageExercisePrice": 10.0,
       "tranches": [{"range": "$0.00 - $10.00", "outstanding": 1_000_000, "weightedAverageExercisePrice": 5.0},
                    {"range": "$10.01 - $30.00", "outstanding": 1_000_000, "weightedAverageExercisePrice": 15.0}]}
add(_scen("cap.scen.options_use_tranches_when_tagged", ["BR-EXT-021"],
          {"tranches": {"bridge": _inv([_TR]), "scenarios": [_sc("p25", 25)]},
           "weighted": {"bridge": _inv([{k: v for k, v in _TR.items() if k != "tranches"}]), "scenarios": [_sc("p25", 25)]}},
          lines(fields=("instrument", "incrementalShares", "countBasis")),
          expect={"tranches": {"p25": [{"instrument": "Options $0.00 - $10.00", "incrementalShares": 800_000, "countBasis": "outstanding_in_exercise_price_range"},
                                       {"instrument": "Options $10.01 - $30.00", "incrementalShares": 400_000, "countBasis": "outstanding_in_exercise_price_range"}]},
                  "weighted": {"p25": [{"instrument": "Options (weighted-average exercise price)", "incrementalShares": 1_200_000, "countBasis": "outstanding"}]}}))

# BR-EXT-019 / 020 card examples.
add(_scen("cap.scen.resulting_shares_basic_plus_included_plus_known_issuance", ["BR-EXT-019"],
          {"r": {"bridge": _inv([{"component": "unvested_share_awards", "unvested": 5_000_000, "countBasis": "nonvested"}]),
                 "scenarios": [_sc("s", 25, known_issuance=[{"label": "Offering", "shares": 1_000_000}])]}},
          totals, expect={"r": {"s": {"resultingShares": 106_000_000, "dilutionPct": 6, "knownIssuanceShares": 1_000_000,
                                      "completeness": "TAGGED_INSTRUMENTS_RESOLVED"}, "status": "COMPUTED"}}))
add(_scen("cap.scen.totals_options_atm_excluded_known_issuance", ["BR-EXT-020"],
          {"r": {"bridge": _inv([_OPT], atm={"remainingCapacityUsd": 50_000_000}),
                 "scenarios": [_sc("s", 25, known_issuance=[{"label": "Awarded", "shares": 400_000}])]}},
          lambda r: {**totals(r), "incremental": r["scenarios"][0]["totals"]["incrementalByComponent"]},
          expect={"r": {"s": {"resultingShares": 101_000_000, "dilutionPct": 1, "knownIssuanceShares": 400_000, "completeness": "TAGGED_INSTRUMENTS_RESOLVED"},
                        "status": "COMPUTED", "incremental": {"stock_options": 600_000}}}))
_CLAIM = {"kind": "PRICE_PROTECTION", "status": "UNQUANTIFIED", "sentences": ["x"]}
_COV = {"component": "convertible_debt", "instruments": [{"instrument": "Unpriced", "ifConvertedShares": None, "conversionPrice": None}]}
add(_scen("cap.scen.completeness_precedence_and_status", ["BR-EXT-019", "BR-EXT-020"],
          {"no_basic": {"bridge": _inv([_OPT], basic=None), "scenarios": [_sc("s", 25)]},
           "unresolved_over_claims": {"bridge": _inv([_COV], claims=[_CLAIM]), "scenarios": [_sc("s", 25)]},
           "unquantified_claim": {"bridge": _inv([_OPT], claims=[_CLAIM]), "scenarios": [_sc("s", 25)]},
           "reported_not_modeled_claim": {"bridge": _inv([_OPT], claims=[{**_CLAIM, "status": "REPORTED_NOT_MODELED"}]), "scenarios": [_sc("s", 25)]},
           "closed_claim_ignored": {"bridge": _inv([_OPT], claims=[{**_CLAIM, "status": "RETIRED"}]), "scenarios": [_sc("s", 25)]},
           "text_not_read": {"bridge": _inv([_OPT], text_scan=None), "scenarios": [_sc("s", 25)]},
           "resolved": {"bridge": _inv([_OPT]), "scenarios": [_sc("s", 25)]}},
          totals,
          expect={"no_basic": {"s": {"resultingShares": None, "dilutionPct": None, "knownIssuanceShares": 0, "completeness": "NO_BASIC_SHARES"}, "status": "NOT_FOUND"},
                  "unresolved_over_claims": {"s": {"resultingShares": 100_000_000, "dilutionPct": 0, "knownIssuanceShares": 0, "completeness": "EXCLUDES_UNRESOLVED_INSTRUMENTS"}, "status": "PARTIAL"},
                  "unquantified_claim": {"s": {"resultingShares": 100_600_000, "dilutionPct": 0.6, "knownIssuanceShares": 0, "completeness": "EXCLUDES_UNQUANTIFIED_CLAIMS"}, "status": "PARTIAL"},
                  "reported_not_modeled_claim": {"s": {"resultingShares": 100_600_000, "dilutionPct": 0.6, "knownIssuanceShares": 0, "completeness": "EXCLUDES_UNQUANTIFIED_CLAIMS"}, "status": "PARTIAL"},
                  "closed_claim_ignored": {"s": {"resultingShares": 100_600_000, "dilutionPct": 0.6, "knownIssuanceShares": 0, "completeness": "TAGGED_INSTRUMENTS_RESOLVED"}, "status": "COMPUTED"},
                  "text_not_read": {"s": {"resultingShares": 100_600_000, "dilutionPct": 0.6, "knownIssuanceShares": 0, "completeness": "CLAIM_TEXT_NOT_READ"}, "status": "PARTIAL"},
                  "resolved": {"s": {"resultingShares": 100_600_000, "dilutionPct": 0.6, "knownIssuanceShares": 0, "completeness": "TAGGED_INSTRUMENTS_RESOLVED"}, "status": "COMPUTED"}}))
_W = {"component": "warrants", "classes": [
    {"class": "Vested", "outstanding": 4_000_000, "exercisable": 1_000_000, "exercisePrice": 25.0},
    {"class": "Untagged Vesting", "outstanding": 500_000, "exercisable": None, "exercisableBasis": "vesting_terms_without_vested_count", "exercisePrice": 30.0}]}
add(_scen("cap.scen.warrant_vesting_treatments", ["BR-EXT-019", "BR-EXT-024"],
          {"r": {"bridge": _inv([_W]), "scenarios": [_sc("vested", 40), _sc("all", 40, warrant_vesting="all")]}},
          lines(fields=("instrument", "count", "countBasis", "incrementalShares", "unresolvedReason")),
          expect={"r": {
              # 1,000,000 x (1 - 25/40) = 375,000; the class vesting on untagged conditions is never taken as all 500,000.
              "vested": [{"instrument": "Vested", "count": 1_000_000, "countBasis": "vested_exercisable", "incrementalShares": 375_000, "unresolvedReason": None},
                         {"instrument": "Untagged Vesting", "count": None, "countBasis": "vested_exercisable", "incrementalShares": None, "unresolvedReason": "count not tagged"}],
              # 4,000,000 x (1 - 25/40) = 1,500,000; 500,000 x (1 - 30/40) = 125,000
              "all": [{"instrument": "Vested", "count": 4_000_000, "countBasis": "outstanding", "incrementalShares": 1_500_000, "unresolvedReason": None},
                      {"instrument": "Untagged Vesting", "count": 500_000, "countBasis": "outstanding", "incrementalShares": 125_000, "unresolvedReason": None}]}}))
add(_scen("cap.scen.atm_full_remaining_capacity", ["BR-EXT-019", "BR-EXT-024"],
          {"r": {"bridge": _inv([], atm={"remainingCapacityUsd": 400_000_000}), "scenarios": [_sc("full", 40, atm="full_remaining_capacity"), _sc("excl", 40)]},
           "unstated": {"bridge": _inv([], atm={"remainingCapacityUsd": None}), "scenarios": [_sc("full", 40, atm="full_remaining_capacity")]}},
          lines(fields=("incrementalShares", "included", "unresolvedReason")),
          expect={"r": {"full": [{"incrementalShares": 10_000_000, "included": True, "unresolvedReason": None}],
                        "excl": [{"incrementalShares": 0, "included": False, "unresolvedReason": None}]},
                  "unstated": {"full": [{"incrementalShares": None, "included": False, "unresolvedReason": "remaining capacity not stated"}]}}))

# BR-EXT-005 / 013 / 024 / 082 / 083 card examples: convertible treatments and net-share settlement.
_N10 = _inv([_notes(10_000_000, 50.0, 500_000_000, settled=True)])
_CF = ("incrementalShares", "inTheMoney")
add(_scen("cap.scen.convertible_net_share_settlement_card_examples", ["BR-EXT-005", "BR-EXT-024", "BR-EXT-082", "BR-EXT-083"],
          {"r": {"bridge": _N10, "scenarios": [
              _sc("net75", 75, convertibles="net_share_settlement_when_stated"), _sc("net60", 60, convertibles="net_share_settlement_when_stated"),
              _sc("net45", 45, convertibles="net_share_settlement_when_stated"), _sc("default75", 75), _sc("default45", 45),
              _sc("all45", 45, convertibles="if_converted_all"), _sc("excl75", 75, convertibles="exclude")]}},
          lines(fields=_CF),
          expect={"r": {
              # 10,000,000 - 500,000,000 / 75 = 3,333,333.3 ; at $60: 1,666,666.7
              "net75": [{"incrementalShares": 3_333_333, "inTheMoney": True}], "net60": [{"incrementalShares": 1_666_667, "inTheMoney": True}],
              "net45": [{"incrementalShares": 0, "inTheMoney": False}], "default75": [{"incrementalShares": 10_000_000, "inTheMoney": True}],
              "default45": [{"incrementalShares": 0, "inTheMoney": False}], "all45": [{"incrementalShares": 10_000_000, "inTheMoney": False}],
              "excl75": [{"incrementalShares": 0, "inTheMoney": True}]}}))
add(_scen("cap.scen.convertible_cash_principal_card_13_and_24", ["BR-EXT-013", "BR-EXT-024"],
          {"lite": {"bridge": _inv([_notes(6_000_000, 50.0, 300_000_000, settled=True)]),
                    "scenarios": [_sc("p75", 75, convertibles="net_share_settlement_when_stated"), _sc("p40", 40, convertibles="net_share_settlement_when_stated")]},
           "mix": {"bridge": _inv([_OPT, _notes(2_000_000, 20.0, 40_000_000, settled=True)]),
                   "scenarios": [_sc("net", 25, convertibles="net_share_settlement_when_stated"), _sc("default", 25)]}},
          lines(fields=("component", "incrementalShares")),
          expect={"lite": {"p75": [{"component": "convertible_debt", "incrementalShares": 2_000_000}], "p40": [{"component": "convertible_debt", "incrementalShares": 0}]},
                  # options 1,000,000 x (1 - 10/25) = 600,000; notes 2,000,000 - 40,000,000 / 25 = 400,000, default 2,000,000
                  "mix": {"net": [{"component": "stock_options", "incrementalShares": 600_000}, {"component": "convertible_debt", "incrementalShares": 400_000}],
                          "default": [{"component": "stock_options", "incrementalShares": 600_000}, {"component": "convertible_debt", "incrementalShares": 2_000_000}]}}))
add(_scen("cap.scen.convertible_net_share_not_stated_falls_back_to_if_converted", ["BR-EXT-005", "BR-EXT-083"],
          {"r": {"bridge": _inv([_notes(10_000_000, 50.0, 500_000_000, settled=False)]),
                 "scenarios": [_sc("net", 75, convertibles="net_share_settlement_when_stated"), _sc("plain", 75)]}},
          lines(fields=("incrementalShares", "method")),
          expect={"r": {"net": [{"incrementalShares": 10_000_000, "method": "if_converted when price >= conversion price; cash settlement of principal not stated"}],
                        "plain": [{"incrementalShares": 10_000_000, "method": "if_converted when price >= conversion price"}]}}))
add(_scen("cap.scen.convertible_unresolved_reasons", ["BR-EXT-082", "BR-EXT-083"],
          {"no_shares": {"bridge": _inv([_notes(None, 50.0, 500_000_000)]), "scenarios": [_sc("def", 75), _sc("all", 75, convertibles="if_converted_all")]},
           "no_price": {"bridge": _inv([_notes(10_000_000, None, 500_000_000, settled=True)]),
                        "scenarios": [_sc("def", 75), _sc("all", 75, convertibles="if_converted_all"), _sc("net", 75, convertibles="net_share_settlement_when_stated")]},
           "no_principal": {"bridge": _inv([_notes(10_000_000, 50.0, None, settled=True)]),
                            "scenarios": [_sc("net", 75, convertibles="net_share_settlement_when_stated")]}},
          lines(fields=("incrementalShares", "unresolvedReason")),
          expect={"no_shares": {"def": [{"incrementalShares": None, "unresolvedReason": "principal or conversion terms not tagged"}],
                                "all": [{"incrementalShares": None, "unresolvedReason": "principal or conversion terms not tagged"}]},
                  "no_price": {"def": [{"incrementalShares": None, "unresolvedReason": "conversion price not tagged"}],
                               "all": [{"incrementalShares": 10_000_000, "unresolvedReason": None}],
                               "net": [{"incrementalShares": None, "unresolvedReason": "conversion price not tagged"}]},
                  "no_principal": {"net": [{"incrementalShares": None, "unresolvedReason": "principal not tagged"}]}}))
_PREF = {"component": "convertible_preferred", "instruments": [{"instrument": "Convertible preferred stock", "ifConvertedShares": 21_800_000,
                                                                 "conversionPrice": 91.84, "ifConvertedBasis": "shares_issuable_tagged"}]}
add(_scen("cap.scen.convertible_preferred_same_treatment", ["BR-EXT-005", "BR-EXT-024"],
          {"r": {"bridge": _inv([_PREF]), "scenarios": [_sc("p80", 80), _sc("p95", 95), _sc("all80", 80, convertibles="if_converted_all")]}},
          lines(fields=_CF),
          expect={"r": {"p80": [{"incrementalShares": 0, "inTheMoney": False}], "p95": [{"incrementalShares": 21_800_000, "inTheMoney": True}],
                        "all80": [{"incrementalShares": 21_800_000, "inTheMoney": False}]}}))
# BR-EXT-024 edge: capped call offset = -covered x max(0, min(price, cap) - strike) / price, only with offset_when_stated.
_CC = _inv([{"component": "convertible_debt", "instruments": [{
    "instrument": "Notes", "principal": 1_000_000_000, "ifConvertedShares": 20_000_000, "conversionPrice": 50.0, "ifConvertedBasis": _NOTE_ID, "principalSettlement": None,
    "cappedCall": {"strikePrice": 50.0, "capPrice": 100.0, "coveredShares": 20_000_000, "coverageBasis": "STATED_COUNT"}}]}])
add(_scen("cap.scen.capped_call_offset_between_and_above_cap", ["BR-EXT-024"],
          {"r": {"bridge": _CC, "scenarios": [_sc("mid", 80, capped_calls="offset_when_stated"), _sc("above", 120, capped_calls="offset_when_stated"),
                                              _sc("at-strike", 50, capped_calls="offset_when_stated"), _sc("ignored", 80)]}},
          lines(fields=("component", "incrementalShares")),
          expect={"r": {
              # 20,000,000 x (80 - 50) / 80 = 7,500,000 ; above the $100 cap: 20,000,000 x 50 / 120 = 8,333,333.3
              "mid": [{"component": "convertible_debt", "incrementalShares": 20_000_000}, {"component": "capped_call", "incrementalShares": -7_500_000}],
              "above": [{"component": "convertible_debt", "incrementalShares": 20_000_000}, {"component": "capped_call", "incrementalShares": -8_333_333}],
              "at-strike": [{"component": "convertible_debt", "incrementalShares": 20_000_000}, {"component": "capped_call", "incrementalShares": 0}],
              "ignored": [{"component": "convertible_debt", "incrementalShares": 20_000_000}]}}))

# Scheduled repair Q3f: at price == strike an option or warrant is not in the money (> strictly), at price == conversion price a note is (>=).
add(_scen("cap.scen.in_the_money_boundary_options_vs_convertibles", ["BR-EXT-003", "BR-EXT-005", "BR-EXT-018", "BR-EXT-021", "BR-EXT-024", "BR-EXT-082", "BR-EXT-083"],
          {"r": {"bridge": _inv([_OPT, {"component": "warrants", "classes": [{"class": "W", "outstanding": 1_000_000, "exercisable": 1_000_000, "exercisePrice": 10.0}]},
                                 _notes(1_000_000, 10.0, 10_000_000)]),
                 "scenarios": [_sc("equal", 10)]}},
          lines(fields=("component", "inTheMoney", "thresholdPrice", "incrementalShares")),
          mode="pinned", decision="Q3f",
          expect={"r": {"equal": [{"component": "stock_options", "inTheMoney": False, "thresholdPrice": 10, "incrementalShares": 0},
                                  {"component": "warrants", "inTheMoney": False, "thresholdPrice": 10, "incrementalShares": 0},
                                  {"component": "convertible_debt", "inTheMoney": True, "thresholdPrice": 10, "incrementalShares": 1_000_000}]}},
          note="repair: one in-the-money boundary rule across instruments (Q3f)"))

# 2.23.0 (BR-EXT-084): a class with no `exercisable` key at all is untagged exactly like a null one: under vested_only its count is
# unknown ("count not tagged"), never its outstanding count; under warrant_vesting "all" the outstanding count is used.
_PLAIN_WARRANT = lambda: _inv([{"component": "warrants", "classes": [{"class": "Plain", "outstanding": 1_000_000, "exercisePrice": 10.0}]}])
add(_scen("cap.scen.warrant_without_exercisable_key_is_unresolved", ["BR-EXT-019", "BR-EXT-084"],
          {"r": {"bridge": _PLAIN_WARRANT(), "scenarios": [_sc("p25", 25)]}},
          lines(fields=("count", "countBasis", "incrementalShares")),
          expect={"r": {"p25": [{"count": None, "countBasis": "vested_exercisable", "incrementalShares": None}]}}))
add(_scen("cap.scen.warrant_without_exercisable_key_all_vesting_uses_outstanding", ["BR-EXT-019", "BR-EXT-084"],
          {"r": {"bridge": _PLAIN_WARRANT(), "scenarios": [_sc("p25", 25, warrant_vesting="all")]}},
          lines(fields=("count", "countBasis", "incrementalShares")),
          expect={"r": {"p25": [{"count": 1_000_000, "countBasis": "outstanding", "incrementalShares": 600_000}]}}))

# ═════════════════════════════════════════════════════ dilution bridge (BR-EXT-007 / 008 / 025 / 027 / 004 / 026 / 013)
_OPT_N = "us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingNumber"
_OPT_K = "us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingWeightedAverageExercisePrice"
_NONVESTED = "us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber"
_WCOUNT, _WSTRIKE = "us-gaap:ClassOfWarrantOrRightOutstanding", "us-gaap:ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1"
_WAXIS, _AAXIS = "us-gaap:ClassOfWarrantOrRightAxis", "us-gaap:AwardTypeAxis"


def _std(*, options: tuple[int, float] | None = None, awards: dict[str, int] | None = None, warrants: list[tuple[str, int, float | None]] | None = None,
         notes: list[tuple[list[str], str]] | None = None, extra_ctx: list[str] | None = None, extra_body: str = "", shares: str | None = "100,000,000") -> str:
    ctxs = list(extra_ctx or [])
    body = []
    if options:
        body.append(f"<p>Options {T._num(_OPT_N, 'end', 'shares', f'{options[0]:,}')} at {T._num(_OPT_K, 'end', 'usdPerShare', f'{options[1]}')}.</p>")
    for i, (kind, n) in enumerate((awards or {}).items()):
        ctxs.append(T._context(f"aw{i}", PE, {_AAXIS: kind}))
        body.append(f"<p>Unvested {kind} {T._num(_NONVESTED, f'aw{i}', 'shares', f'{n:,}')}.</p>")
    for i, (member, n, strike) in enumerate(warrants or []):
        ctxs.append(T._context(f"w{i}", PE, {_WAXIS: member}))
        k = f" at {T._num(_WSTRIKE, f'w{i}', 'usdPerShare', f'{strike}')}" if strike is not None else ""
        body.append(f"<p>Warrants {T._num(_WCOUNT, f'w{i}', 'shares', f'{n:,}')}{k}.</p>")
    for c, b in notes or []:
        ctxs += c
        body.append(b)
    return _filing(ctxs, "".join(body) + extra_body, shares=shares)


def bridge_numbers(v: dict) -> dict:
    return {"status": v["status"], **{k: (v.get("bridge") or {}).get(k) for k in (
        "basicShares", "stockOptions", "unvestedShareAwards", "warrants", "convertibleDebt", "convertiblePreferred",
        "dilutedSharesAtPrice", "dilutionPctAtPrice", "grossSharesAllInstruments", "grossDilutionPct")}}


# BR-EXT-007 card example: basic 100,000,000, options +2,000,000, warrants +3,000,000, no open claims.
_B7 = _std(options=(5_000_000, 15.0), warrants=[("tst:PublicWarrantsMember", 7_500_000, 15.0)])
add(_bridge("cap.bridge.diluted_shares_basic_plus_options_plus_warrants", ["BR-EXT-007", "BR-EXT-008", "BR-EXT-027"], _B7, 25, claims=[], select=bridge_numbers,
            expect={"status": "COMPUTED", "basicShares": 100_000_000, "stockOptions": 2_000_000, "unvestedShareAwards": None, "warrants": 3_000_000,
                    "convertibleDebt": None, "convertiblePreferred": None, "dilutedSharesAtPrice": 105_000_000, "dilutionPctAtPrice": 5,
                    # gross adds the full 5,000,000 options and 7,500,000 warrants
                    "grossSharesAllInstruments": 112_500_000, "grossDilutionPct": 12.5}))
# BR-EXT-008 card example: options +600,000, awards +2,500,000, warrants +900,000, convertibles +5,000,000 on 100,000,000 basic.
_B8 = _std(options=(1_000_000, 10.0), awards={"us-gaap:RestrictedStockUnitsRSUMember": 2_000_000, "us-gaap:PerformanceSharesMember": 500_000},
           warrants=[("tst:PublicWarrantsMember", 1_500_000, 10.0)], notes=[_note("a", CONV, face=100_000_000, price=20.0)])
add(_bridge("cap.bridge.all_components_card_example", ["BR-EXT-007", "BR-EXT-008", "BR-EXT-025", "BR-EXT-027"], _B8, 25, claims=[],
            select=lambda v: {**bridge_numbers(v), "tagged": "taggedDilutionConcepts" in v, "notDisclosed": v["notDisclosed"],
                              "awardBasis": pick(comp(v, "unvested_share_awards"), "unvested", "method", "countBasis")},
            expect={"status": "COMPUTED", "basicShares": 100_000_000, "stockOptions": 600_000, "unvestedShareAwards": 2_500_000, "warrants": 900_000,
                    "convertibleDebt": 5_000_000, "convertiblePreferred": None, "dilutedSharesAtPrice": 109_000_000, "dilutionPctAtPrice": 9,
                    # gross: 100M + 1,000,000 options + 2,500,000 awards + 1,500,000 warrants + 5,000,000 if-converted
                    "grossSharesAllInstruments": 110_000_000, "grossDilutionPct": 10, "tagged": True, "notDisclosed": ["convertible_preferred", "atm_program"],
                    "awardBasis": {"unvested": 2_500_000, "method": "gross_unvested", "countBasis": "nonvested"}}))
add(_bridge("cap.bridge.open_claim_makes_status_partial", ["BR-EXT-007", "BR-EXT-008"], _B8, 25, claims=T.COHR_CLAIM_TEXT[:1],
            select=lambda v: {"status": v["status"], "diluted": v["bridge"]["dilutedSharesAtPrice"], "kinds": [c["kind"] for c in v["unquantifiedShareClaims"]],
                              "code": "UNQUANTIFIED_SHARE_CLAIMS" in codes(v)},
            expect={"status": "PARTIAL", "diluted": 109_000_000, "kinds": ["PRICE_PROTECTION"], "code": True}))
_B_UNRES = _std(options=(5_000_000, 15.0), warrants=[("tst:PublicWarrantsMember", 4_000_000, 11.5), ("tst:SeriesBWarrantsMember", 500_000, None)])
add(_bridge("cap.bridge.unpriced_warrant_class_makes_status_partial", ["BR-EXT-007", "BR-EXT-008", "BR-EXT-027"], _B_UNRES, 25, claims=[],
            select=lambda v: {"status": v["status"], "partiallyResolved": v["partiallyResolved"], "unresolved": v["unresolved"],
                              "classes": {c["class"]: c["incrementalShares"] for c in comp(v, "warrants")["classes"]},
                              "component": comp(v, "warrants")["incrementalShares"], "unresolvedClasses": comp(v, "warrants")["unresolvedClasses"]},
            expect={"status": "PARTIAL", "partiallyResolved": ["warrants"], "unresolved": [],
                    # 4,000,000 x (1 - 11.5/25) = 2,160,000 ; no tagged strike and exercisable > 0 is unresolved (null), not 0
                    "classes": {"Public Warrants": 2_160_000, "Series B Warrants": None}, "component": 2_160_000, "unresolvedClasses": 1}))
add(_bridge("cap.bridge.no_basic_shares_not_found", ["BR-EXT-007", "BR-EXT-008"], _std(options=(5_000_000, 15.0), shares=None), 25, claims=[],
            select=lambda v: {"status": v["status"], "bridge": v["bridge"], "basicShares": v["basicShares"],
                              "error": [w["code"] for w in v["warnings"] if w["severity"] == "error"], "tagged": "taggedDilutionConcepts" in v},
            expect={"status": "NOT_FOUND", "bridge": None, "basicShares": None, "error": ["BASIC_SHARES_NOT_FOUND"], "tagged": True}))
add(_bridge("cap.bridge.strike_currency_differs_from_price_currency", ["BR-EXT-007", "BR-EXT-008"], _B7, 25, claims=[], currency="EUR",
            select=lambda v: [(w["code"], w["severity"]) for w in v["warnings"] if w["code"] == "PRICE_CURRENCY_MISMATCH"],
            expect=[["PRICE_CURRENCY_MISMATCH", "warning"]]))
add(_bridge("cap.bridge.cover_page_classes_summed_when_no_total", ["BR-EXT-008"], T.TEN_K, 25, filing="10-K", claims=[],
            select=lambda v: pick(v["basicShares"], "shares", "basis", "asOf"),
            expect={"shares": 100_000_000, "basis": "cover_page", "asOf": "2025-02-10"}))

# BR-EXT-027: exercisable warrants and treasury stock (card: 1,000,000 warrants, no vesting, strike $10, price $25 / $8).
_W27 = _std(warrants=[("tst:PublicWarrantsMember", 1_000_000, 10.0)])
_WSEL = lambda v: {"warrants": v["bridge"]["warrants"], "class": pick(comp(v, "warrants")["classes"][0], "exercisable", "exercisableBasis", "incrementalShares", "inTheMoney")}
add(_bridge("cap.warrants.no_vesting_all_exercisable_price_25", ["BR-EXT-027"], _W27, 25, claims=[], select=_WSEL,
            expect={"warrants": 600_000, "class": {"exercisable": 1_000_000, "exercisableBasis": "no_vesting_terms_tagged", "incrementalShares": 600_000, "inTheMoney": True}}))
add(_bridge("cap.warrants.price_below_strike_adds_nothing", ["BR-EXT-027"], _W27, 8, claims=[], select=_WSEL,
            expect={"warrants": 0, "class": {"exercisable": 1_000_000, "exercisableBasis": "no_vesting_terms_tagged", "incrementalShares": 0, "inTheMoney": False}}))
add(_bridge("cap.warrants.price_equal_strike_adds_nothing", ["BR-EXT-027"], _W27, 10, claims=[], select=lambda v: _WSEL(v)["warrants"], expect=0))
_VCT = "us-gaap:ClassOfWarrantOrRightSharesVested"
_UNV = "us-gaap:ClassOfWarrantOrRightUnvestedNumberOfSecuritiesCalledByWarrantsOrRights"
_VT = "us-gaap:WarrantsAndRightsOutstandingVestingTerm"
_WCTX = [T._context("wx", PE, {_WAXIS: "tst:CustomerWarrantMember"})]
_WB = f"<p>Customer warrant {T._num(_WCOUNT, 'wx', 'shares', '4,000,000')} at {T._num(_WSTRIKE, 'wx', 'usdPerShare', '10.00')}; "
for _n, _tail, _exp in (
        ("vested_count_tagged", f"{T._num(_VCT, 'wx', 'shares', '1,000,000')} vested.</p>", (1_000_000, "vested_count_tagged", 600_000)),
        ("outstanding_less_unvested", f"{T._num(_UNV, 'wx', 'shares', '3,000,000')} unvested.</p>", (1_000_000, "outstanding_less_unvested_tagged", 600_000)),
        ("vesting_terms_without_count_unknown", f"vesting over {T._text(_VT, 'wx', 'five years')}.</p>", (None, "vesting_terms_without_vested_count", None))):
    add(_bridge(f"cap.warrants.exercisable_{_n}", ["BR-EXT-027"], _filing(_WCTX, _WB + _tail), 25, claims=[],
                select=lambda v: {"warrants": v["bridge"]["warrants"], "unresolved": v["unresolved"],
                                  "class": pick(comp(v, "warrants")["classes"][0], "exercisable", "exercisableBasis", "incrementalShares")},
                expect={"warrants": _exp[2], "unresolved": [] if _exp[2] is not None else ["warrants"],
                        "class": {"exercisable": _exp[0], "exercisableBasis": _exp[1], "incrementalShares": _exp[2]}}))

# BR-EXT-025: unvested awards counted gross.
add(_bridge("cap.awards.rsu_and_psu_on_award_axis_summed", ["BR-EXT-025"],
            _std(awards={"us-gaap:RestrictedStockUnitsRSUMember": 2_000_000, "us-gaap:PerformanceSharesMember": 500_000}), 25, claims=[],
            select=lambda v: pick(comp(v, "unvested_share_awards"), "unvested", "incrementalShares", "method", "countBasis"),
            expect={"unvested": 2_500_000, "incrementalShares": 2_500_000, "method": "gross_unvested", "countBasis": "nonvested"}))
add(_bridge("cap.awards.fewest_axes_not_double_counted", ["BR-EXT-025"], T.AWARDS_Q, 25, claims=[],
            select=lambda v: pick(comp(v, "unvested_share_awards"), "unvested", "countBasis"),
            expect={"unvested": 1_000_000, "countBasis": "nonvested"}))
add(_bridge("cap.awards.company_prefixed_vested_and_expected_concept", ["BR-EXT-025"], T.AAOI_Q, 30, claims=[],
            select=lambda v: pick(comp(v, "unvested_share_awards"), "unvested", "countBasis"),
            expect={"unvested": 3_000_000, "countBasis": "vested_and_expected_to_vest"}))
_TBL = lambda title, label, ctx: {"contextText": ctx, "sectionHeading": "Stock-Based Compensation", "documentUrl": "https://www.sec.gov/Archives/x.htm",
                                  "filingDate": "2026-08-06", "accessionNumber": None, "inTable": True, "tableTitle": title, "rowLabel": label}
add(_bridge("cap.awards.table_row_in_thousands_when_nothing_tagged", ["BR-EXT-025"], T.BARE_Q, 30, claims=[],
            tables=[_TBL("Restricted stock unit activity (in thousands)", "Unvested at December 31, 2025", "Unvested at December 31, 2025 | 1,234")],
            select=lambda v: pick(comp(v, "unvested_share_awards"), "unvested", "incrementalShares", "countBasis"),
            expect={"unvested": 1_234_000, "incrementalShares": 1_234_000, "countBasis": "filing_table_text"}))
add(_bridge("cap.awards.table_latest_row_wins_and_option_table_skipped", ["BR-EXT-025"], T.BARE_Q, 30, claims=[], tables=T.TABLE_MATCHES,
            select=lambda v: pick(comp(v, "unvested_share_awards"), "unvested", "countBasis"),
            expect={"unvested": 2_750_000, "countBasis": "filing_table_text"}))

# BR-EXT-004: convertible preferred.
_PU = "us-gaap:PreferredStockConvertibleSharesIssuable"
_PP = "us-gaap:PreferredStockConvertibleConversionPrice"
_PREF_BODY = (f"<p>Preferred outstanding {T._num('us-gaap:PreferredStockSharesOutstanding', 'end', 'shares', '2,000,000')}, convertible into "
              f"{T._num(_PU, 'end', 'shares', '21,800,000')} shares at {T._num(_PP, 'end', 'usdPerShare', '91.84')}; liquidation preference "
              f"{T._num('us-gaap:PreferredStockLiquidationPreference', 'end', 'usdPerShare', '1,000')} per share.</p>")
_PREF_F = _filing([], _PREF_BODY)
_PSEL = lambda v: {"component": v["bridge"]["convertiblePreferred"], "instrument": pick(comp(v, "convertible_preferred")["instruments"][0],
                   "preferredSharesOutstanding", "ifConvertedShares", "ifConvertedBasis", "conversionPrice", "inTheMoney", "incrementalShares", "liquidationPreference")}
_LIQ = {"amount": 2_000_000_000, "basis": "per_share_tagged_x_shares_outstanding", "perShare": 1000, "asOf": PE}
add(_bridge("cap.pref.below_conversion_price_keeps_liquidation_preference", ["BR-EXT-004"], _PREF_F, 80, claims=[], select=_PSEL,
            expect={"component": 0, "instrument": {"preferredSharesOutstanding": 2_000_000, "ifConvertedShares": 21_800_000, "ifConvertedBasis": "shares_issuable_tagged",
                                                   "conversionPrice": 91.84, "inTheMoney": False, "incrementalShares": 0, "liquidationPreference": _LIQ}}))
add(_bridge("cap.pref.above_conversion_price_if_converted", ["BR-EXT-004"], _PREF_F, 95, claims=[], select=_PSEL,
            expect={"component": 21_800_000, "instrument": {"preferredSharesOutstanding": 2_000_000, "ifConvertedShares": 21_800_000, "ifConvertedBasis": "shares_issuable_tagged",
                                                            "conversionPrice": 91.84, "inTheMoney": True, "incrementalShares": 21_800_000, "liquidationPreference": _LIQ}}))
add(_bridge("cap.pref.price_equal_conversion_price_counts_all_shares", ["BR-EXT-004", "BR-EXT-082"], _PREF_F, 91.84, claims=[], mode="pinned", decision="Q3f",
            select=lambda v: _PSEL(v)["instrument"]["incrementalShares"], expect=21_800_000,
            note="repair: preferred uses >= like notes today; Q3f decides the single boundary rule"))
add(_bridge("cap.pref.dividend_rate_and_aggregate_liquidation_preference", ["BR-EXT-004"], T.PREFLIQ_Q, 10, claims=[],
            select=lambda v: pick(comp(v, "convertible_preferred")["instruments"][0], "dividendRatePct", "liquidationPreference", "incrementalShares", "inTheMoney"),
            expect={"dividendRatePct": 6, "liquidationPreference": {"amount": 100_000_000, "basis": "per_share_tagged_x_shares_outstanding", "perShare": 100, "asOf": "2026-06-30"},
                    "incrementalShares": 0, "inTheMoney": False}))
add(_bridge("cap.pref.preferred_and_temporary_equity_not_summed_one_for_one_text", ["BR-EXT-004"], T.LITE_K, 700, filing="10-K", claims=T.LITE_CLAIM_TEXT,
            select=lambda v: pick(comp(v, "convertible_preferred")["instruments"][0], "preferredSharesOutstanding", "ifConvertedShares", "ifConvertedBasis", "conversionPrice",
                                  "incrementalShares", "method"),
            expect={"preferredSharesOutstanding": 2_900_000, "ifConvertedShares": 2_900_000, "ifConvertedBasis": "ratio_stated_in_text", "conversionPrice": None,
                    "incrementalShares": 2_900_000, "method": "as_converted_no_conversion_price"}))
add(_bridge("cap.pref.no_conversion_price_unresolved", ["BR-EXT-004"],
            _filing([], f"<p>Preferred outstanding {T._num('us-gaap:PreferredStockSharesOutstanding', 'end', 'shares', '2,000,000')}, convertible into "
                        f"{T._num(_PU, 'end', 'shares', '21,800,000')} shares.</p>"), 95, claims=[],
            select=lambda v: {"component": v["bridge"]["convertiblePreferred"], "unresolved": v["unresolved"], "inTheMoney": comp(v, "convertible_preferred")["instruments"][0]["inTheMoney"]},
            expect={"component": None, "unresolved": ["convertible_preferred"], "inTheMoney": None}))
add(_bridge("cap.pref.equity_component_axis_facts_excluded", ["BR-EXT-004"],
            _filing([T._context("eqc", PE, {"us-gaap:StatementEquityComponentsAxis": "us-gaap:PreferredStockMember"})],
                    f"<p>Preferred {T._num('us-gaap:PreferredStockSharesOutstanding', 'eqc', 'shares', '2,000,000')} at {T._num(_PP, 'end', 'usdPerShare', '91.84')}.</p>"),
            95, claims=[], select=lambda v: {"present": comp(v, "convertible_preferred") is not None, "notDisclosed": "convertible_preferred" in v["notDisclosed"]},
            expect={"present": False, "notDisclosed": True}))
add(Case(name="cap.pref.text_wordings_ratio_and_aggregate", ids=["BR-EXT-004"],
         py=lambda: [cs.preferred_conversion_terms(_tm(t)) for t in [*T.PREFERRED_WORDINGS, T.LITE_CLAIM_TEXT]],
         ts={"js": "const tm = (l) => l.map((t) => ({ contextText: t, sectionHeading: 'Equity', documentUrl: fx.kUrl, filingDate: '2026-08-20', accessionNumber: null }));"
                   "return args[0].map((t) => m.capitalStructure.preferredConversionTerms(tm(t)));",
             "args": [[*T.PREFERRED_WORDINGS, T.LITE_CLAIM_TEXT]], "fx": {"kUrl": T.K_URL}},
         select=lambda v: [pick(x, "ratio", "aggregateShares", "approximate") for x in v],
         expect=[{"ratio": 12.5, "aggregateShares": None, "approximate": False}, {"ratio": None, "aggregateShares": 4_200_000, "approximate": False},
                 {"ratio": 3, "aggregateShares": None, "approximate": False}, {"ratio": 1, "aggregateShares": None, "approximate": False},
                 {"ratio": 1, "aggregateShares": None, "approximate": False}]))

# BR-EXT-013 at the bridge: principal stated as cash-settled gives net shares beside the EPS-basis diluted count.
_NSS_F = _conv_filing(_note("a", CONV, face=300_000_000, price=50.0, issuable=6_000_000))
_NSS_TEXT = ["Upon conversion of the 2030 Notes, we will pay cash up to the aggregate principal amount of the notes being converted and pay or deliver shares for the remainder. "
             "For the 2031 Notes, we may settle conversions in cash, shares or a combination, at our election."]
_NSS_SEL = lambda v: {"settlement": pick(comp(v, "convertible_debt")["instruments"][0], "netShareSettlementShares", "incrementalShares"),
                      "scope": (comp(v, "convertible_debt")["instruments"][0].get("principalSettlement") or {}).get("scope"),
                      "bridge": pick(v["bridge"], "dilutedSharesAtPrice", "convertibleDebtNetShareSettlement", "dilutedSharesAtPriceNetShareSettlement")}
add(_bridge("cap.nss.cash_principal_net_shares_beside_if_converted", ["BR-EXT-013"], _NSS_F, 75, claims=[], settlement=_NSS_TEXT, select=_NSS_SEL,
            expect={"settlement": {"netShareSettlementShares": 2_000_000, "incrementalShares": 6_000_000}, "scope": "NAMED_NOTES",
                    "bridge": {"dilutedSharesAtPrice": 106_000_000, "convertibleDebtNetShareSettlement": 2_000_000, "dilutedSharesAtPriceNetShareSettlement": 102_000_000}}))
add(_bridge("cap.nss.unnamed_notes_used_when_exactly_one_note", ["BR-EXT-013"], _NSS_F, 75, claims=[],
            settlement=["Upon conversion, we will pay cash up to the aggregate principal amount of the notes being converted and deliver shares for the remainder."],
            select=_NSS_SEL, expect={"settlement": {"netShareSettlementShares": 2_000_000, "incrementalShares": 6_000_000}, "scope": "UNNAMED_NOTES",
                                     "bridge": {"dilutedSharesAtPrice": 106_000_000, "convertibleDebtNetShareSettlement": 2_000_000, "dilutedSharesAtPriceNetShareSettlement": 102_000_000}}))
add(_bridge("cap.nss.unnamed_notes_ignored_when_two_notes", ["BR-EXT-013"], _conv_filing(_note("a", CONV, face=300_000_000, price=50.0, issuable=6_000_000),
                                                                                         _note("b", "tst:ConvertibleNotesDue2032Member", face=100_000_000, price=50.0, issuable=2_000_000)), 75, claims=[],
            settlement=["Upon conversion, we will pay cash up to the aggregate principal amount of the notes being converted and deliver shares for the remainder."],
            select=lambda v: [i.get("principalSettlement") for i in comp(v, "convertible_debt")["instruments"]], expect=[None, None]))
add(_bridge("cap.nss.out_of_the_money_gives_zero", ["BR-EXT-013"], _NSS_F, 40, claims=[], settlement=_NSS_TEXT,
            select=lambda v: _NSS_SEL(v)["settlement"], expect={"netShareSettlementShares": 0, "incrementalShares": 0}))
add(_bridge("cap.nss.optional_wording_is_not_a_stated_cash_settlement", ["BR-EXT-013"], _NSS_F, 75, claims=[],
            settlement=["We may settle conversions of the 2030 Notes in cash, shares of our common stock or a combination, at our election, and the principal amount may be paid in cash."],
            select=_NSS_SEL, expect={"settlement": {"netShareSettlementShares": None, "incrementalShares": 6_000_000}, "scope": None,
                                     "bridge": {"dilutedSharesAtPrice": 106_000_000, "convertibleDebtNetShareSettlement": None, "dilutedSharesAtPriceNetShareSettlement": None}}))
add(_bridge("cap.nss.all_notes_scope_and_untagged_principal_unresolved", ["BR-EXT-013"], T.LITE_K, 700, filing="10-K", claims=T.LITE_CLAIM_TEXT, settlement=T.LITE_SETTLEMENT_TEXT,
            select=lambda v: {"scope": comp(v, "convertible_debt")["instruments"][0]["principalSettlement"]["scope"],
                              "net": comp(v, "convertible_debt")["instruments"][0]["netShareSettlementShares"], "basis": v["bridge"]["dilutedSharesAtPrice"] is not None},
            expect={"scope": "ALL_NOTES",
                    # 1,370,689 if-converted - 179,600,000 / 700 = 1,370,689 - 256,571.4
                    "net": 1_114_118, "basis": True}))

# BR-EXT-026: warrant class retirement by filing text.
add(_bridge("cap.warrants.retired_in_text_exercised_in_whole", ["BR-EXT-026"], T.STALE_Q, 20, claims=[], lifecycle=T.LIFECYCLE_TEXT,
            select=lambda v: {"retired": [pick(r, "class", "count", "asOf", "event", "eventDate", "matchedBy") for r in comp(v, "warrants")["retiredInText"]],
                              "live": {c["class"]: [c["outstanding"], c["incrementalShares"]] for c in comp(v, "warrants")["classes"]},
                              "warrants": v["bridge"]["warrants"], "warned": "WARRANT_RETIRED_IN_TEXT" in codes(v)},
            expect={"retired": [{"class": "Private Placement Warrants", "count": 122_000, "asOf": "2025-12-31", "event": "EXERCISED", "eventDate": "2026-03-31", "matchedBy": "STATED_COUNT"},
                                {"class": "Lender Warrant", "count": 728_835, "asOf": "2023-12-29", "event": "EXERCISED", "eventDate": "2024-11-14", "matchedBy": "STATED_COUNT"}],
                    # Delta: negated, partial and post-period sentences retire nothing; public: no sentence. 7,000,000 x (1 - 0.01/20); 3,000,000 x (1 - 11.5/20)
                    "live": {"Delta Warrants": [7_000_000, 6_996_500], "Public Warrants": [3_000_000, 1_275_000]},
                    "warrants": 8_271_500, "warned": True}))
add(_bridge("cap.warrants.text_not_read_keeps_stale_counts", ["BR-EXT-026"], T.STALE_Q, 20, claims=[], lifecycle=None,
            select=lambda v: {"classes": sorted(c["class"] for c in comp(v, "warrants")["classes"]), "retired": "retiredInText" in comp(v, "warrants")},
            expect={"classes": ["Delta Warrants", "Lender Warrant", "Private Placement Warrants", "Public Warrants"], "retired": False}))
add(_bridge("cap.warrants.tagged_expiry_before_period_end_moves_to_expired", ["BR-EXT-026"], T.LIFE_K, 20, filing="10-K", claims=[], lifecycle=[],
            select=lambda v: {"expired": [pick(c, "class", "count", "expirationDate") for c in comp(v, "warrants").get("expiredClasses", [])],
                              "live": [c["class"] for c in comp(v, "warrants")["classes"]]},
            expect={"expired": [{"class": "Series A Warrants", "count": 2_000_000, "expirationDate": "2025-06-30"}], "live": ["Series B Warrants", "Public Warrants"]}))

# ═════════════════════════════════════════════════════ capital structure: BR-EXT-006 / 012 / 022 / 023
def _debt_filing(rows: list[tuple[str, dict]], total: float | None, costs: float | None = None, cash: float = 500_000_000) -> str:
    notes = [_note(f"r{i}", member, **kw) for i, (member, kw) in enumerate(rows)]
    parts = [f"<p>Cash {T._num('us-gaap:CashAndCashEquivalentsAtCarryingValue', 'end', 'usd', f'{cash:,.0f}')}."]
    if total is not None:
        parts.append(f" Debt {T._num('us-gaap:LongTermDebt', 'end', 'usd', f'{total:,.0f}')}.")
    if costs is not None:
        parts.append(f" Less unamortized costs {T._num('us-gaap:UnamortizedDebtIssuanceExpense', 'end', 'usd', f'{costs:,.0f}')}.")
    parts.append("</p>")
    return _filing([c for cs_, _ in notes for c in cs_], "".join(parts) + "".join(b for _, b in notes))


def rows_view(v: dict) -> dict:
    return {r["instrument"]: pick(r, "status", "maturityDate", "carryingAmount", "carryingAmountBasis") for r in v["instruments"]}


def recon_view(v: dict) -> dict:
    return {"reconciliation": pick(v["instrumentReconciliation"], "status", "instrumentsCarryingTotal", "totalDebt", "difference", "rowsWithoutCarryingAmount"),
            "codes": [c for c in codes(v) if c in ("INSTRUMENTS_DO_NOT_RECONCILE", "INSTRUMENT_AMOUNTS_BEFORE_UNAMORTIZED_COSTS", "ISSUED_AFTER_PERIOD_END")]}


_MATURED = ("tst:NotesDue2026Member", dict(face=100_000_000, carrying=100_000_000, maturity="March 15, 2026"))
_LIVE = ("tst:NotesDue2029Member", dict(face=130_000_000, carrying=130_000_000, maturity="January 15, 2029"))
# BR-EXT-006 card example: period end 2026-06-30 and a note maturing 2026-03-15.
add(_capital("cap.status.matured_note_excluded_from_reconciliation", ["BR-EXT-006", "BR-EXT-012"],
             _debt_filing([_MATURED, _LIVE], 130_000_000),
             select=lambda v: {"rows": rows_view(v), **recon_view(v)},
             expect={"rows": {"Notes Due 2026": {"status": "matured_before_period_end", "maturityDate": "2026-03-15", "carryingAmount": 100_000_000, "carryingAmountBasis": None},
                              "Notes Due 2029": {"status": "outstanding_at_period_end", "maturityDate": "2029-01-15", "carryingAmount": 130_000_000, "carryingAmountBasis": None}},
                     "reconciliation": {"status": "RECONCILED", "instrumentsCarryingTotal": 130_000_000, "totalDebt": 130_000_000, "difference": 0, "rowsWithoutCarryingAmount": 0},
                     "codes": []}))
add(_capital("cap.status.ten_k_matured_outstanding_and_reported", ["BR-EXT-006"], T.TEN_K, filing="10-K",
             select=lambda v: {k: x["status"] for k, x in rows_view(v).items()},
             # Senior notes due 2023 matured before 2024-12-31; term loan and 2029 notes carry amounts at the period end; the 2031 notes
             # are tagged only at their 2024-06-01 issue date, so they are reported without a period-end balance; the coupon-only member is dropped.
             expect={"Senior Notes Due 2023": "matured_before_period_end", "Term Loan": "outstanding_at_period_end",
                     "Convertible Senior Notes Due 2029": "outstanding_at_period_end", "Convertible Notes Due 2031": "reported"}))
add(_capital("cap.status.subsequent_event_note_issued_after_period_end", ["BR-EXT-006"], T.ASTS_DEBT_Q,
             select=lambda v: {"statuses": {k: x["status"] for k, x in rows_view(v).items() if "Thirty Four" in k},
                               "later": [c for c in codes(v) if c == "ISSUED_AFTER_PERIOD_END"]},
             expect={"statuses": {"Two Thousand Thirty Four One Point Six Two Five Percent Convertible Notes": "issued_after_period_end"},
                     "later": ["ISSUED_AFTER_PERIOD_END"]}))
add(_capital("cap.status.two_maturities_place_row_at_the_earliest", ["BR-EXT-006"], T.AAOI_DEBT_Q,
             select=lambda v: [pick(r, "instrument", "maturityDate", "maturityDates") for r in v["instruments"] if r.get("maturityDates")],
             expect=[{"instrument": "Loan Agreement", "maturityDate": "2027-06-25", "maturityDates": ["2027-06-25", "2032-06-12"]}]))

# BR-EXT-012: instrument rows against total debt, within 0.5%.
_N130 = ("tst:NotesDue2029Member", dict(face=130_000_000, carrying=130_000_000, maturity="January 15, 2029"))
add(_capital("cap.recon.net_of_unamortized_costs", ["BR-EXT-012"], _debt_filing([_N130], 129_142_000, costs=858_000),
             select=lambda v: {**recon_view(v), "rows": rows_view(v)},
             # Gross 130,000,000 vs 129,142,000 is 0.66% > 0.5%; less 858,000 it matches.
             expect={"reconciliation": {"status": "RECONCILED_NET_OF_UNAMORTIZED_COSTS", "instrumentsCarryingTotal": 130_000_000, "totalDebt": 129_142_000,
                                        "difference": -858_000, "rowsWithoutCarryingAmount": 0},
                     "codes": ["INSTRUMENT_AMOUNTS_BEFORE_UNAMORTIZED_COSTS"],
                     "rows": {"Notes Due 2029": {"status": "outstanding_at_period_end", "maturityDate": "2029-01-15", "carryingAmount": 130_000_000,
                                                 "carryingAmountBasis": "BEFORE_UNAMORTIZED_COSTS"}}}))
add(_capital("cap.recon.gap_without_costs_not_reconciled", ["BR-EXT-012"], _debt_filing([_N130], 129_142_000), select=recon_view,
             expect={"reconciliation": {"status": "NOT_RECONCILED", "instrumentsCarryingTotal": 130_000_000, "totalDebt": 129_142_000, "difference": -858_000,
                                        "rowsWithoutCarryingAmount": 0}, "codes": ["INSTRUMENTS_DO_NOT_RECONCILE"]}))
add(_capital("cap.recon.within_half_percent_reconciled", ["BR-EXT-012"], _debt_filing([_N130], 129_800_000), select=recon_view,
             expect={"reconciliation": {"status": "RECONCILED", "instrumentsCarryingTotal": 130_000_000, "totalDebt": 129_800_000, "difference": -200_000,
                                        "rowsWithoutCarryingAmount": 0}, "codes": []}))
add(_capital("cap.recon.row_without_carrying_amount_not_comparable", ["BR-EXT-012"],
             _debt_filing([_N130, ("tst:NotesDue2031Member", dict(face=50_000_000, maturity="January 15, 2031"))], 130_000_000), select=recon_view,
             expect={"reconciliation": {"status": "NOT_COMPARABLE", "instrumentsCarryingTotal": None, "totalDebt": 130_000_000, "difference": None,
                                        "rowsWithoutCarryingAmount": 1}, "codes": []}))
add(_capital("cap.recon.no_total_debt_not_comparable", ["BR-EXT-012"], _debt_filing([_N130], None), select=recon_view,
             expect={"reconciliation": {"status": "NOT_COMPARABLE", "instrumentsCarryingTotal": None, "totalDebt": None, "difference": None,
                                        "rowsWithoutCarryingAmount": 0}, "codes": []}))
add(_capital("cap.recon.convertible_rows_vs_balance_sheet_line_may_be_principal", ["BR-EXT-012"], T.AAOI_DEBT_Q,
             select=lambda v: {"convertibleBalanceSheet": pick(v["instrumentReconciliation"]["convertibleBalanceSheet"], "status", "balanceSheetAmount", "instrumentRowsTotal", "difference"),
                               "basis": {r["instrument"]: r.get("carryingAmountBasis") for r in v["instruments"] if r.get("convertible")},
                               "code": "CARRYING_AMOUNT_MAY_BE_PRINCIPAL" in codes(v)},
             expect={"convertibleBalanceSheet": {"status": "NOT_RECONCILED", "balanceSheetAmount": 129_142_000, "instrumentRowsTotal": 124_900_000, "difference": 4_242_000},
                     "basis": {"Convertible Notes Maturing 2030": "MAY_BE_PRINCIPAL"}, "code": True}))

# BR-EXT-022: short-term investments and the filing's cash aggregate.
def _cash_filing(cash: int, parts: dict[str, int], agg: int | None) -> str:
    rows = [f"<tr><td>Cash</td><td>{T._num('us-gaap:CashAndCashEquivalentsAtCarryingValue', 'end', 'usd', f'{cash:,}')}</td></tr>"]
    rows += [f"<tr><td>{c}</td><td>{T._num('us-gaap:' + c, 'end', 'usd', f'{v:,}')}</td></tr>" for c, v in parts.items()]
    if agg is not None:
        rows.append(f"<tr><td>Cash and investments</td><td>{T._num('us-gaap:CashCashEquivalentsAndShortTermInvestments', 'end', 'usd', f'{agg:,}')}</td></tr>")
    rows.append(f"<tr><td>Debt</td><td>{T._num('us-gaap:LongTermDebt', 'end', 'usd', '10,000,000')}</td></tr>")
    return _filing([], "<table>" + "".join(rows) + "</table>")


_AFS = "AvailableForSaleSecuritiesDebtSecuritiesCurrent"
_CSEL = lambda v: {"sti": v["balances"]["shortTermInvestments"], "concept": v["balances"]["shortTermInvestmentsConcept"],
                   "warn": [[w["code"], w["severity"]] for w in v["warnings"] if w["code"] == "CASH_AGGREGATE_MISMATCH"]}
add(_capital("cap.cash.investments_already_inside_cash_dropped", ["BR-EXT-022"], _cash_filing(500_000_000, {_AFS: 200_000_000}, 500_000_000), select=_CSEL,
             expect={"sti": None, "concept": None, "warn": [["CASH_AGGREGATE_MISMATCH", "info"]]}))
add(_capital("cap.cash.aggregate_mismatch_warns_and_keeps_investments", ["BR-EXT-022"], _cash_filing(500_000_000, {_AFS: 200_000_000}, 650_000_000), select=_CSEL,
             expect={"sti": 200_000_000, "concept": "us-gaap:" + _AFS, "warn": [["CASH_AGGREGATE_MISMATCH", "warning"]]}))
add(_capital("cap.cash.aggregate_agrees_no_warning", ["BR-EXT-022"], _cash_filing(500_000_000, {_AFS: 200_000_000}, 700_000_000), select=_CSEL,
             expect={"sti": 200_000_000, "concept": "us-gaap:" + _AFS, "warn": []}))
add(_capital("cap.cash.tagged_total_beats_category_parts", ["BR-EXT-022"],
             _cash_filing(500_000_000, {"ShortTermInvestments": 150_000_000, _AFS: 40_000_000}, None), select=_CSEL,
             expect={"sti": 150_000_000, "concept": "us-gaap:ShortTermInvestments", "warn": []}))
add(_capital("cap.cash.disjoint_categories_summed_without_total", ["BR-EXT-022"], T.PARTS_Q, select=_CSEL,
             expect={"sti": 100_000_000, "concept": "us-gaap:AvailableForSaleSecuritiesDebtSecuritiesCurrent + us-gaap:HeldToMaturitySecuritiesCurrent", "warn": []}))
add(_capital("cap.cash.single_category_returned_as_is", ["BR-EXT-022"], T.VRT_Q, select=_CSEL,
             expect={"sti": 300_000_000, "concept": "us-gaap:DebtSecuritiesHeldToMaturityAmortizedCostAfterAllowanceForCreditLossCurrent", "warn": []}))

# BR-EXT-023: total debt concept hierarchy and finance-lease removal.
_TDSEL = lambda v: {"totalDebt": v["balances"]["totalDebt"], "basis": v["balances"]["totalDebtBasis"], "codes": [c for c in codes(v) if "LEASE" in c or "BORROWINGS" in c]}
add(_capital("cap.debt.finance_lease_subtracted_from_lease_inclusive_total", ["BR-EXT-023"], T.LEASE_TOTAL_Q, select=_TDSEL,
             expect={"totalDebt": 3_052_000_000, "basis": "DebtAndCapitalLeaseObligations (debt including finance leases) less FinanceLeaseLiability",
                     "codes": ["FINANCE_LEASES_SUBTRACTED"]}))
add(_capital("cap.debt.lease_parts_less_lease_parts", ["BR-EXT-023"], T.LEASE_PARTS_Q, select=_TDSEL,
             expect={"totalDebt": 3_052_000_000, "codes": ["FINANCE_LEASES_SUBTRACTED"],
                     "basis": "DebtCurrent + LongTermDebtAndCapitalLeaseObligations (debt including finance leases) less FinanceLeaseLiabilityCurrent + FinanceLeaseLiabilityNoncurrent"}))
add(_capital("cap.debt.no_finance_lease_tagged_leases_included", ["BR-EXT-023"], T.LEASE_UNSPLIT_Q, select=_TDSEL,
             expect={"totalDebt": 42_368_000_000, "codes": ["TOTAL_DEBT_INCLUDES_FINANCE_LEASES"],
                     "basis": "DebtCurrent + LongTermDebtAndCapitalLeaseObligations (debt including finance leases; no finance-lease liability tagged to take out)"}))
add(_capital("cap.debt.lease_larger_than_gross_is_not_subtracted", ["BR-EXT-023"],
             T._lease_q([("DebtAndCapitalLeaseObligations", "1,000"), ("FinanceLeaseLiability", "2,000")]), select=_TDSEL,
             expect={"totalDebt": 1_000_000_000, "codes": ["TOTAL_DEBT_INCLUDES_FINANCE_LEASES"],
                     "basis": "DebtAndCapitalLeaseObligations (debt including finance leases; no finance-lease liability tagged to take out)"}))
add(_capital("cap.debt.current_debt_takes_precedence_over_current_lease_inclusive_line", ["BR-EXT-023"],
             T._lease_q([("DebtCurrent", "582"), ("LongTermDebtAndCapitalLeaseObligationsCurrent", "600"), ("LongTermDebtAndCapitalLeaseObligations", "5,140"),
                         ("FinanceLeaseLiabilityCurrent", "582"), ("FinanceLeaseLiabilityNoncurrent", "2,088")]), select=lambda v: _TDSEL(v)["totalDebt"], expect=3_052_000_000))
add(_capital("cap.debt.long_term_debt_concept_comes_first", ["BR-EXT-023"], T.TEN_K, filing="10-K", select=_TDSEL,
             expect={"totalDebt": 385_000_000, "basis": "LongTermDebt (current and noncurrent) plus short-term borrowings", "codes": []}))
add(_capital("cap.debt.current_plus_noncurrent_when_no_long_term_debt", ["BR-EXT-023"], T.ASTS_Q, select=lambda v: {k: _TDSEL(v)[k] for k in ("totalDebt", "basis")},
             expect={"totalDebt": 2_971_916_000, "basis": "LongTermDebtCurrent + LongTermDebtNoncurrent plus short-term borrowings"}))
add(_capital("cap.debt.separately_reported_convertibles_added_when_larger", ["BR-EXT-023"], T.AAOI_Q, select=lambda v: {k: _TDSEL(v)[k] for k in ("totalDebt", "basis")},
             # bank loans 57,258 + 1,657 = 58,915 (thousands) ; convertible line 124,900 is larger, so it is added
             expect={"totalDebt": 183_815_000, "basis": "LongTermDebtCurrent + LongTermDebtNoncurrent plus short-term borrowings, plus separately reported convertible notes"}))
add(_capital("cap.debt.no_borrowing_concept_means_zero_debt", ["BR-EXT-023"], T.DEBT_FREE_K, filing="10-K", select=_TDSEL,
             expect={"totalDebt": 0, "basis": "No borrowing concepts tagged in the filing", "codes": ["NO_BORROWINGS_TAGGED"]}))

# ═════════════════════════════════════════════════════ BR-EXT-009: geographic revenue table selection and share check
def _tbl(rows: list[list[str]], lead: str = "") -> str:
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return (f"<p>{lead}</p>" if lead else "") + f"<table>{body}</table>"


def _geo(name: str, html: str, region: str, expect, **kw) -> Case:
    return Case(name=name, ids=["BR-EXT-009"], py=lambda: ex.extract_geo_revenue_from_html(html, region),
                ts={"module": "yahooFinance", "fn": "extractGeoRevenueFromHtml", "args": [html, region]},
                select=lambda v: None if v is None else pick(v, "pct", "usd", "denominator", "statedPct", "unitScale"), expect=expect, **kw)


_GEO_LEAD = "Revenue by geographic region (in millions)"
_HDR_PCT = ["Region", "Revenue", "% of total"]
_G = lambda china, total, pct="": [_HDR_PCT, ["China", china, pct], ["Other", "850", ""], ["Total", total, "100%" if pct else ""]]
add(_geo("cap.geo.stated_share_off_by_more_than_one_point_rejected", _tbl(_G("150", "1,000", "16.5%"), _GEO_LEAD), "China", None))
add(_geo("cap.geo.table_without_stated_column_pct_usd_denominator", _tbl([["Region", "Revenue"], ["China", "150"], ["Other", "850"], ["Total", "1,000"]], _GEO_LEAD), "China",
         {"pct": 0.15, "usd": 150_000_000, "denominator": 1_000_000_000, "statedPct": None, "unitScale": "millions"}))
add(_geo("cap.geo.stated_share_within_one_point_accepted", _tbl(_G("150", "1,000", "15.5%"), _GEO_LEAD), "China",
         {"pct": 0.15, "usd": 150_000_000, "denominator": 1_000_000_000, "statedPct": 15.5, "unitScale": "millions"}))
add(_geo("cap.geo.percent_under_change_column_is_not_a_share", _tbl([["Region", "Revenue", "% Change"], ["China", "150", "12.0%"], ["Other", "850", ""], ["Total", "1,000", ""]], _GEO_LEAD),
         "China", {"pct": 0.15, "usd": 150_000_000, "denominator": 1_000_000_000, "statedPct": None, "unitScale": "millions"}))
add(_geo("cap.geo.region_above_total_rejected", _tbl([["Region", "Revenue"], ["China", "1,002"], ["Total", "1,000"]], _GEO_LEAD), "China", None))
add(_geo("cap.geo.square_footage_table_skipped", _tbl([["Region", "Square feet"], ["China", "150"], ["Total", "1,000"]], "Properties and revenue facilities (square feet)"), "China", None))
add(_geo("cap.geo.geographic_table_preferred_over_earlier_generic_table",
         _tbl([["Customer", "Revenue"], ["China", "50"], ["Total", "500"]], "Net sales by customer (in millions)")
         + _tbl([["Region", "Revenue"], ["China", "150"], ["Total", "1,000"]], _GEO_LEAD), "China",
         {"pct": 0.15, "usd": 150_000_000, "denominator": 1_000_000_000, "statedPct": None, "unitScale": "millions"}))
add(_geo("cap.geo.prc_spelling_matches_china_region", _tbl([["Region", "Revenue"], ["PRC", "150"], ["Total", "1,000"]], _GEO_LEAD), "China",
         {"pct": 0.15, "usd": 150_000_000, "denominator": 1_000_000_000, "statedPct": None, "unitScale": "millions"}))
add(_geo("cap.geo.dash_cell_reads_as_zero_and_column_pairs_by_position",
         _tbl([["Region", "2026", "2025"], ["China", "&#8212;", "120"], ["Total", "1,000", "900"]], _GEO_LEAD), "China",
         {"pct": 0.1333, "usd": 120_000_000, "denominator": 900_000_000, "statedPct": None, "unitScale": "millions"}))
add(_geo("cap.geo.total_row_without_amounts_falls_back_to_last_numeric_row",
         _tbl([["Region", "Amount"], ["Revenues", ""], ["China", "150"], ["Other", "850"], ["Sum", "1,000"]], _GEO_LEAD), "China",
         {"pct": 0.15, "usd": 150_000_000, "denominator": 1_000_000_000, "statedPct": None, "unitScale": "millions"}))
add(_geo("cap.geo.unit_scale_thousands_from_lead_in", _tbl([["Region", "Revenue"], ["China", "150"], ["Total", "1,000"]], "Revenue by geographic region (in thousands)"), "China",
         {"pct": 0.15, "usd": 150_000, "denominator": 1_000_000, "statedPct": None, "unitScale": "thousands"}))
add(_geo("cap.geo.no_table_naming_region_is_none", _tbl([["Region", "Revenue"], ["Japan", "150"], ["Total", "1,000"]], _GEO_LEAD), "China", None))

# ═════════════════════════════════════════════════════ funding schedule: BR-EXT-044 / 045 / 059 / 064 / 110
def _plain(local: str, value: float, pe: str, dims=None) -> dict:
    return {"local": local, "value": value, "periodEnd": pe, "dims": dims or {}, "unit": "USD"}


_SCHED_JS = ("return m.fundingSchedule.fundingCapexSchedule({ ticker: 'tst', periodEnd: args[0], source: null, facts: args[1], statements: args[2],"
             " balances: null, atmRemainingUsd: null, atmEvidence: null });")


def _sched(name: str, ids: list[str], pe: str | None, facts: list[dict], statements: list[dict], select, **kw) -> Case:
    return Case(name=name, ids=ids,
                py=lambda: fs.funding_capex_schedule(ticker="tst", period_end=pe, source=None, facts=facts, statements=statements, balances=None,
                                                     atm_remaining_usd=None, atm_evidence=None),
                ts={"js": _SCHED_JS, "args": [pe, facts, statements]}, select=select, **kw)


def _stmt(text: str) -> dict:
    return {"contextText": text, "sectionHeading": "Liquidity", "documentUrl": "https://www.sec.gov/x/10q.htm", "filingDate": "2026-08-10", "accessionNumber": None}


def buckets(v: dict) -> list:
    return [[r["family"], r["bucket"], r["periodThrough"], r.get("amount")] for r in v["contractual"]]


_DEBT = "LongTermDebtMaturitiesRepaymentsOfPrincipal"
# BR-EXT-044 / 045 / 110 card examples.
add(_sched("cap.funding.bucket_period_through_is_period_end_plus_offset", ["BR-EXT-044", "BR-EXT-045", "BR-EXT-110"], "2025-12-31",
           [_plain(_DEBT + "InYearThree", 500_000_000, "2025-12-31"), _plain(_DEBT + "RemainderOfFiscalYear", 10, "2025-12-31"),
            _plain(_DEBT + "AfterYearFive", 20, "2025-12-31"), _plain(_DEBT + "InYearTwo", 7, "2024-12-31")], [],
           lambda v: {"rows": buckets(v), "status": v["status"]},
           expect={"rows": [["debt_principal", "remainder_of_fiscal_year", None, 10], ["debt_principal", "year_3", "2028-12-31", 500_000_000],
                            ["debt_principal", "after_year_5", None, 20]], "status": "COMPUTED"}))
add(_sched("cap.funding.year_two_bucket_card_example", ["BR-EXT-045"], PE, [_plain(_DEBT + "InYearTwo", 500_000_000, PE)], [],
           lambda v: {"rows": buckets(v), "status": v["status"], "first": v["contractual"][0]["classification"]},
           expect={"rows": [["debt_principal", "year_2", "2028-06-30", 500_000_000]], "status": "COMPUTED", "first": "CONTRACTUAL"}))
add(_sched("cap.funding.first_concept_per_bucket_wins_and_ranges_split", ["BR-EXT-044", "BR-EXT-045"], PE,
           [_plain("PurchaseObligationDueInNextTwelveMonths", 250, PE), _plain("UnrecordedUnconditionalPurchaseObligationBalanceOnFirstAnniversary", 99, PE),
            _plain("LongTermPurchaseCommitmentAmount", 775, PE, {"srt:RangeAxis": "srt:MinimumMember"}),
            _plain("LongTermPurchaseCommitmentAmount", 795, PE, {"srt:RangeAxis": "srt:MaximumMember"})], [],
           lambda v: {"rows": buckets(v), "range": [pick(r, "amountLow", "amountHigh", "amountBasis") for r in v["contractual"] if r["family"] == "purchase_commitment"]},
           expect={"rows": [["purchase_obligations", "next_12_months", "2027-06-30", 250], ["purchase_commitment", "unstated", None, None]],
                   "range": [{"amountLow": 775, "amountHigh": 795, "amountBasis": "tagged_range"}]}))
add(_sched("cap.funding.no_facts_and_no_schedules_warnings", ["BR-EXT-044", "BR-EXT-045"], PE, [], [],
           lambda v: {"status": v["status"], "codes": [w["code"] for w in v["warnings"]]}, expect={"status": "NOT_FOUND", "codes": ["NO_INLINE_XBRL"]}))
add(_sched("cap.funding.facts_but_no_schedule_info_warning", ["BR-EXT-044", "BR-EXT-045"], PE, [_plain("Cash", 5, PE)], [],
           lambda v: {"status": v["status"], "warnings": [[w["code"], w["severity"]] for w in v["warnings"]]},
           expect={"status": "NOT_FOUND", "warnings": [["NO_TAGGED_SCHEDULES", "info"]]}))

# Scheduled repair Q3i (BR-EXT-044 / 045 / 110): _add_years replaces the year only, so a Feb-29 period end gives invalid dates
# in the funding schedule and in capital_structure's maturityLadder (server.py:11955-11960 handles Feb 29 correctly).
_LEAP = "2024-02-29"
add(_sched("cap.funding.leap_day_period_through_invalid_date", ["BR-EXT-044", "BR-EXT-045", "BR-EXT-110"], _LEAP,
           [_plain(_DEBT + f, 1, _LEAP) for f in ("InNextTwelveMonths", "InYearTwo", "InYearThree", "InYearFour", "InYearFive")], [],
           lambda v: [r["periodThrough"] for r in v["contractual"]], mode="pinned", decision="Q3i",
           expect=["2025-02-29", "2026-02-29", "2027-02-29", "2028-02-29", "2029-02-29"],
           note="repair: roll Feb 29 to a valid date (Feb 28 or Mar 1: SME question)"))
_LEAP_K = _filing([], "<p>" + "".join(T._num(f"us-gaap:{_DEBT}{f}", "end", "usd", "1") for f in ("InNextTwelveMonths", "InYearTwo")) + "</p>",
                  end=_LEAP, end_text="February 29, 2024", start="2023-03-01", doc_type="10-K")
add(_capital("cap.funding.leap_day_maturity_ladder_period_through_invalid_date", ["BR-EXT-044", "BR-EXT-045", "BR-EXT-110"], _LEAP_K, filing="10-K",
             select=lambda v: [[r["bucket"], r["periodThrough"]] for r in v["maturityLadder"]], mode="pinned", decision="Q3i",
             expect=[["next_12_months", "2025-02-29"], ["year_2", "2026-02-29"]]))

# BR-EXT-059: the first dollar amount, ranges only low to high, no-scale figures under $100,000.
def items(v: dict) -> list:
    return [pick(i, "classification", "amountLow", "amountHigh", "amountQualifier", "amountStatus", "amountAsWritten", "currency") for i in v["textItems"]]


def _text(name: str, ids: list[str], text: str, expect, **kw) -> Case:
    return _sched(name, ids, PE, [], [_stmt(text)], items, expect=expect, **kw)


add(_text("cap.funding.text_two_amounts_high_below_low_is_not_a_range", ["BR-EXT-059"],
          "We have committed $1.2 billion and $300 million to two facility expansions through 2028.",
          [{"classification": "COMPANY_DISCLOSED_COMMITTED", "amountLow": 1_200_000_000, "amountHigh": 1_200_000_000, "amountQualifier": "stated",
            "amountStatus": "STATED", "amountAsWritten": "$1.2 billion", "currency": "USD"}]))
add(_text("cap.funding.text_unscaled_small_amounts_scale_not_stated", ["BR-EXT-059"],
          "We expect capital expenditures of between $550.0 and $600.0 for the full year 2026 to support capacity.",
          [{"classification": "COMPANY_GUIDED", "amountLow": None, "amountHigh": None, "amountQualifier": "range", "amountStatus": "SCALE_NOT_STATED",
            "amountAsWritten": "$550.0 and $600.0", "currency": "USD"}]))
add(_text("cap.funding.text_range_borrows_other_sides_unit_and_qualifier", ["BR-EXT-059"],
          "We expect capital expenditures of $300 to $350 million in 2026, primarily for satellite manufacturing.",
          [{"classification": "COMPANY_GUIDED", "amountLow": 300_000_000, "amountHigh": 350_000_000, "amountQualifier": "range", "amountStatus": "STATED",
            "amountAsWritten": "$300 to $350 million", "currency": "USD"}]))
add(_text("cap.funding.text_up_to_and_approximately_qualifiers", ["BR-EXT-059"],
          "The Company was awarded a grant of up to $43.5 million under the CHIPS Act, subject to the achievement of construction milestones. "
          "As of June 30, 2026, we had non-cancelable purchase commitments of approximately $1.2 billion for launch services.",
          [{"classification": "AWARDED_CONTINGENT", "amountLow": 43_500_000, "amountHigh": 43_500_000, "amountQualifier": "up_to", "amountStatus": "STATED",
            "amountAsWritten": "$43.5 million", "currency": "USD"},
           {"classification": "COMPANY_DISCLOSED_COMMITTED", "amountLow": 1_200_000_000, "amountHigh": 1_200_000_000, "amountQualifier": "approximately",
            "amountStatus": "STATED", "amountAsWritten": "$1.2 billion", "currency": "USD"}]))
add(_text("cap.funding.text_per_unit_price_is_not_the_amount", ["BR-EXT-059"],
          "We committed to purchase equipment at $2.50 per unit under the long-term supply agreement.",
          [{"classification": "UNRESOLVED", "amountLow": None, "amountHigh": None, "amountQualifier": None, "amountStatus": "NOT_STATED",
            "amountAsWritten": None, "currency": None}]))
# Scheduled repair Q3i (BR-EXT-059 / 064): "$" inside NT$, A$ or C$ is read as dollars and the currency is always written USD.
for _cur, _tag in (("NT$", "ntd"), ("A$", "aud"), ("C$", "cad")):
    add(_text(f"cap.funding.text_{_tag}_dollar_labelled_usd", ["BR-EXT-059", "BR-EXT-064"],
              f"We have committed {_cur}5 billion to the new fabrication facility expansion through 2028.",
              [{"classification": "COMPANY_DISCLOSED_COMMITTED", "amountLow": 5_000_000_000, "amountHigh": 5_000_000_000, "amountQualifier": "stated",
                "amountStatus": "STATED", "amountAsWritten": "$5 billion", "currency": "USD"}], mode="pinned", decision="Q3i",
              note="repair: foreign-dollar amounts are excluded or carry their own currency (SME question)"))

# BR-EXT-064: eligibility of funding text items and the unresolved reason.
add(_text("cap.funding.text_commitment_without_amount_is_unresolved", ["BR-EXT-064"],
          "We have non-cancelable commitments to purchase equipment. Capex was $5 million.",
          [{"classification": "UNRESOLVED", "amountLow": None, "amountHigh": None, "amountQualifier": None, "amountStatus": "NOT_STATED",
            "amountAsWritten": None, "currency": None}]))
add(_sched("cap.funding.text_item_reasons_and_statedtype", ["BR-EXT-064"], PE, [],
           [_stmt("We have non-cancelable commitments to purchase equipment. Capex was $5 million. Capital expenditures of $60 million relate to the new facility.")],
           lambda v: [pick(i, "classification", "statedType", "unresolvedReason") for i in v["textItems"]],
           expect=[{"classification": "UNRESOLVED", "statedType": "COMPANY_DISCLOSED_COMMITTED", "unresolvedReason": "no amount stated"},
                   {"classification": "UNRESOLVED", "statedType": "UNRESOLVED", "unresolvedReason": "type not stated"}]))
add(_sched("cap.funding.text_list_items_and_overlong_sentences_skipped_and_duplicates_removed", ["BR-EXT-064"], PE, [],
           [_stmt("We expect capital expenditures of $40 million in 2025 to fund our expansion plans;"),
            _stmt("We expect capital expenditures of $41 million in 2025 to fund our expansion plans. We expect capital expenditures of $41 million in 2025 to fund our expansion plans."),
            _stmt("We expect capital expenditures of $42 million in 2025 " + "to fund the buildout " * 70 + "of our network.")],
           lambda v: [[i["amountLow"], len(i["evidence"]["sentence"])] for i in v["textItems"]],
           expect=[[41_000_000, 82]]))
add(_sched("cap.funding.text_items_capped_at_25", ["BR-EXT-064"], PE, [],
           [_stmt(" ".join(f"We expect capital expenditures of ${n} million in 2026 for facility number {n} buildout." for n in range(10, 10 + 30)))],
           lambda v: [len(v["textItems"]), v["textItems"][-1]["amountLow"]], expect=[25, 34_000_000]))
