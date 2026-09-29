"""Caller-parameterized share-count scenarios (2.5.2), mirroring worker/src/share-scenarios.ts.

Parity is tested in scripts/test_share_scenarios.py. Pure.

The instrument inventory is the dilution bridge's (company-disclosed inline
XBRL); each scenario applies the caller's price and treatments to it. Every
instrument line reports its treatment, whether it is included, the
treasury-stock or if-converted mechanics, and why it is unresolved when it is.
Scenarios are reported side by side: MCP never selects a denominator.
"""

from __future__ import annotations

import math
from typing import Any

from yfmcp.evidence import AUTHORITY_BOUNDARY

SHARE_SCENARIO_LIMIT = 8
_KNOWN_ISSUANCE_LIMIT = 10

TREATMENTS: dict[str, list[str]] = {
    "options": ["treasury_stock", "gross", "exclude"],
    "unvested_awards": ["gross", "exclude"],
    "warrants": ["treasury_stock", "gross", "exclude"],
    "warrant_vesting": ["vested_only", "all"],
    "convertibles": ["if_converted_when_in_the_money", "if_converted_all", "net_share_settlement_when_stated", "exclude"],
    "atm": ["exclude", "full_remaining_capacity"],
}

DEFAULTS: dict[str, str] = {
    "options": "treasury_stock",
    "unvested_awards": "gross",
    "warrants": "treasury_stock",
    "warrant_vesting": "vested_only",
    "convertibles": "if_converted_when_in_the_money",
    "atm": "exclude",
}


def _round(value: float, digits: int = 0) -> float:
    f = 10 ** digits
    return math.floor(value * f + 0.5) / f


def _num(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if math.isfinite(value) else None


def _first(*values: Any) -> Any:
    for v in values:
        if v is not None:
            return v
    return None


def parse_share_scenarios(raw: Any) -> dict:
    """Validate caller scenarios; every treatment not supplied takes the documented default and is listed as defaulted."""
    if not isinstance(raw, list) or not raw:
        return {"error": "scenarios must be a non-empty array."}
    if len(raw) > SHARE_SCENARIO_LIMIT:
        return {"error": f"At most {SHARE_SCENARIO_LIMIT} scenarios per call."}
    names: set[str] = set()
    scenarios = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            return {"error": f"scenarios[{i}] must be an object."}
        name = item.get("name").strip() if isinstance(item.get("name"), str) else ""
        if not name or len(name) > 60:
            return {"error": f"scenarios[{i}].name must be 1 to 60 characters."}
        if name in names:
            return {"error": f"Scenario name '{name}' is repeated."}
        names.add(name)
        price = _num(item.get("price"))
        if price is None or price <= 0:
            return {"error": f"scenarios[{i}].price must be a positive number."}
        treatments: dict[str, str] = {}
        defaulted: list[str] = []
        for key, allowed in TREATMENTS.items():
            value = item.get(key)
            if value is None:
                treatments[key] = DEFAULTS[key]
                defaulted.append(key)
            elif isinstance(value, str) and value in allowed:
                treatments[key] = value
            else:
                return {"error": f"scenarios[{i}].{key} must be one of {', '.join(allowed)}."}
        issuance = item.get("known_issuance") if item.get("known_issuance") is not None else []
        if not isinstance(issuance, list) or len(issuance) > _KNOWN_ISSUANCE_LIMIT:
            return {"error": f"scenarios[{i}].known_issuance must be an array of at most {_KNOWN_ISSUANCE_LIMIT} items."}
        known = []
        for j, row in enumerate(issuance):
            r = row if isinstance(row, dict) else {}
            label = r.get("label").strip() if isinstance(r.get("label"), str) else ""
            shares = _num(r.get("shares"))
            if not label or shares is None or shares <= 0:
                return {"error": f"scenarios[{i}].known_issuance[{j}] needs a label and positive shares."}
            known.append({"label": label[:120], "shares": shares, "source": r["source"][:300] if isinstance(r.get("source"), str) else None})
        scenarios.append({"name": name, "price": price, "treatments": treatments, "defaulted": defaulted, "knownIssuance": known})
    return {"scenarios": scenarios}


def _line(component: str, instrument: str, treatment: str, count: float | None, count_basis: str, exercise_price: float | None,
          price: float, in_the_money: bool | None, threshold_price: float | None, method: str, incremental: float | None,
          unresolved_reason: str | None = None) -> dict:
    return {
        "component": component,
        "instrument": instrument,
        "treatment": treatment,
        "count": count,
        "countBasis": count_basis,
        "exercisePrice": exercise_price,
        "price": price,
        "inTheMoney": in_the_money,
        "thresholdPrice": threshold_price,
        "method": method,
        "incrementalShares": incremental,
        "unresolvedReason": unresolved_reason,
        "included": treatment != "exclude" and unresolved_reason is None,
    }


def _treasury(count: float, strike: float, price: float) -> float:
    return count * (1 - strike / price) if price > strike else 0


def _exercisable(component: str, instrument: str, count: float | None, count_basis: str, strike: float | None, treatment: str, price: float) -> dict:
    itm = price > strike if strike is not None else None
    if treatment == "exclude":
        return _line(component, instrument, treatment, count, count_basis, strike, price, itm, None, "excluded by scenario", 0)
    if count is None:
        return _line(component, instrument, treatment, count, count_basis, strike, price, None, None, treatment, None, "count not tagged")
    if treatment == "gross":
        return _line(component, instrument, treatment, count, count_basis, strike, price, itm, None, "gross: every share counted", _round(count))
    if strike is None:
        return _line(component, instrument, treatment, count, count_basis, strike, price, None, None, "treasury_stock", None, "exercise price not tagged")
    return _line(component, instrument, treatment, count, count_basis, strike, price, price > strike, strike,
                 "treasury_stock: count x (1 - exercise price / price) when price > exercise price", _round(_treasury(count, strike, price)))


def _scenario_lines(bridge: dict, s: dict) -> list[dict]:
    lines: list[dict] = []
    components = bridge.get("components") if isinstance(bridge.get("components"), list) else []

    def by_name(name: str) -> dict | None:
        return next((c for c in components if isinstance(c, dict) and c.get("component") == name), None)

    t = s["treatments"]
    price = s["price"]

    options = by_name("stock_options")
    if options:
        tranches = options.get("tranches") if isinstance(options.get("tranches"), list) else []
        if tranches:
            for tr in tranches:
                label = f"Options {str(_first(tr.get('range'), ''))}".strip()
                lines.append(_exercisable("stock_options", label, _num(tr.get("outstanding")), "outstanding_in_exercise_price_range",
                                          _num(tr.get("weightedAverageExercisePrice")), t["options"], price))
        else:
            lines.append(_exercisable("stock_options", "Options (weighted-average exercise price)", _num(options.get("outstanding")), "outstanding",
                                      _num(options.get("weightedAverageExercisePrice")), t["options"], price))

    awards = by_name("unvested_share_awards")
    if awards:
        count = _num(awards.get("unvested"))
        basis = str(_first(awards.get("countBasis"), "nonvested"))
        args = ("unvested_share_awards", "Unvested RSUs/PSUs", t["unvested_awards"], count, basis, None, price, None, None)
        if t["unvested_awards"] == "exclude":
            lines.append(_line(*args, "excluded by scenario", 0))
        elif count is None:
            lines.append(_line(*args, "gross", None, "count not tagged"))
        else:
            lines.append(_line(*args, "gross: every unvested award counted", _round(count)))

    warrants = by_name("warrants")
    if warrants:
        for cls in warrants.get("classes") if isinstance(warrants.get("classes"), list) else []:
            all_ = t["warrant_vesting"] == "all"
            # A class that vests on untagged conditions has no known exercisable count; it is never taken as all outstanding (2.5.9).
            count = _num(cls.get("outstanding")) if all_ or "exercisable" not in cls else _num(cls.get("exercisable"))
            lines.append(_exercisable("warrants", str(_first(cls.get("class"), "Warrants")), count, "outstanding" if all_ else "vested_exercisable",
                                      _num(cls.get("exercisePrice")), t["warrants"], price))

    # Convertible notes and convertible preferred stock (2.5.9) take the same if-converted treatment.
    for name in ("convertible_debt", "convertible_preferred"):
        convertibles = by_name(name)
        if not convertibles:
            continue
        for inst in convertibles.get("instruments") if isinstance(convertibles.get("instruments"), list) else []:
            shares = _num(inst.get("ifConvertedShares"))
            conv = _num(inst.get("conversionPrice"))
            itm = price >= conv if conv is not None else None
            args = (name, str(_first(inst.get("instrument"), "Convertible notes")), t["convertibles"], shares,
                    str(_first(inst.get("ifConvertedBasis"), "if_converted")), conv, price)
            if t["convertibles"] == "exclude":
                lines.append(_line(*args, itm, None, "excluded by scenario", 0))
            elif shares is None:
                lines.append(_line(*args, itm, None, t["convertibles"], None, "principal or conversion terms not tagged"))
            elif t["convertibles"] == "if_converted_all":
                lines.append(_line(*args, itm, None, "if_converted: every note converted", _round(shares)))
            elif conv is None:
                lines.append(_line(*args, None, None, t["convertibles"], None, "conversion price not tagged"))
            elif t["convertibles"] == "net_share_settlement_when_stated" and inst.get("principalSettlement") is not None:
                # The filing states principal is settled in cash: shares only for the conversion value above principal (2.5.11).
                principal = _num(inst.get("principal"))
                if principal is None:
                    lines.append(_line(*args, itm, conv, t["convertibles"], None, "principal not tagged"))
                else:
                    lines.append(_line(*args, itm, conv, "net share settlement: (if-converted shares - principal / price) when price >= conversion price; "
                                       "principal in cash as the filing states", _round(max(0, shares - principal / price)) if itm else 0))
            else:
                unstated = "; cash settlement of principal not stated" if t["convertibles"] == "net_share_settlement_when_stated" else ""
                lines.append(_line(*args, itm, conv, f"if_converted when price >= conversion price{unstated}", _round(shares) if itm else 0))

    atm = bridge.get("atmProgram") if isinstance(bridge.get("atmProgram"), dict) else None
    if atm:
        remaining = _num(atm.get("remainingCapacityUsd"))
        args = ("atm_program", "At-the-market program", t["atm"], None, "remaining_capacity_usd", None, price, None, None)
        if t["atm"] == "exclude":
            lines.append(_line(*args, "excluded by scenario", 0))
        elif remaining is None:
            lines.append(_line(*args, "full_remaining_capacity", None, "remaining capacity not stated"))
        else:
            lines.append(_line(*args, "remaining capacity / price (capacity at the company's discretion, not a plan)", _round(remaining / price)))
    return lines


def share_count_scenarios(ticker: str, bridge: dict, scenarios: list[dict]) -> dict:
    """Share counts under each caller scenario, from the dilution bridge's price-independent inventory."""
    basic_rec = bridge.get("basicShares") if isinstance(bridge.get("basicShares"), dict) else None
    basic = _num(basic_rec.get("shares")) if basic_rec else None
    # Claims the filing text states but no tagged instrument covers (2.5.9).
    claims = bridge.get("unquantifiedShareClaims") if isinstance(bridge.get("unquantifiedShareClaims"), list) else []
    open_claims = [c for c in claims if isinstance(c, dict) and c.get("status") == "UNQUANTIFIED"]
    coverage = bridge.get("claimCoverage") if isinstance(bridge.get("claimCoverage"), dict) else None
    claim_text_read = bool(coverage) and coverage.get("textScan") == "READ"
    results = []
    for s in scenarios:
        lines = _scenario_lines(bridge, s)
        by_component: dict[str, float] = {}
        for ln in lines:
            if ln["included"] and ln["incrementalShares"] is not None:
                by_component[ln["component"]] = by_component.get(ln["component"], 0) + ln["incrementalShares"]
        issuance = sum(k["shares"] for k in s["knownIssuance"])
        unresolved = [ln for ln in lines if ln["unresolvedReason"] is not None and ln["treatment"] != "exclude"]
        incremental = sum(by_component.values())
        resulting = _round(basic + incremental + issuance) if basic is not None else None
        results.append({
            "name": s["name"],
            "parameters": {"price": s["price"], **s["treatments"], "knownIssuance": s["knownIssuance"], "defaulted": s["defaulted"]},
            "basicShares": basic,
            "instruments": lines,
            "knownIssuance": s["knownIssuance"],
            "totals": {
                "basicShares": basic,
                "incrementalByComponent": by_component,
                "knownIssuanceShares": issuance,
                "resultingShares": resulting,
                "dilutionPct": _round((resulting - basic) / basic * 100, 2) if basic is not None and basic > 0 and resulting is not None else None,
            },
            "unresolvedInstruments": [{"component": ln["component"], "instrument": ln["instrument"], "reason": ln["unresolvedReason"]} for ln in unresolved],
            # Resolved tagged instruments are never a full claim inventory.
            "completeness": ("NO_BASIC_SHARES" if basic is None
                             else "EXCLUDES_UNRESOLVED_INSTRUMENTS" if unresolved
                             else "EXCLUDES_UNQUANTIFIED_CLAIMS" if open_claims
                             else "CLAIM_TEXT_NOT_READ" if not claim_text_read
                             else "TAGGED_INSTRUMENTS_RESOLVED"),
            "priceSensitiveInstruments": [
                {"component": ln["component"], "instrument": ln["instrument"], "thresholdPrice": ln["thresholdPrice"], "inTheMoney": ln["inTheMoney"]}
                for ln in lines if ln["thresholdPrice"] is not None
            ],
        })
    return {
        "ticker": ticker.upper(),
        "basis": "MECHANICAL_COMPANY_DISCLOSED",
        "status": "NOT_FOUND" if basic is None else "PARTIAL" if any(r["completeness"] != "TAGGED_INSTRUMENTS_RESOLVED" for r in results) else "COMPUTED",
        "periodEnd": bridge.get("periodEnd"),
        "basicShares": basic_rec,
        "sources": _first(bridge.get("sources"), []),
        "scenarios": results,
        "notDisclosed": _first(bridge.get("notDisclosed"), []),
        "unquantifiedShareClaims": claims,
        "claimCoverage": coverage or {"scope": "TAGGED_INSTRUMENTS", "completeClaimInventory": False, "textScan": "NOT_READ"},
        "reportedEpsDilution": bridge.get("reportedEpsDilution"),
        "bridgeWarnings": bridge.get("warnings") if isinstance(bridge.get("warnings"), list) else [],
        "treatmentOptions": TREATMENTS,
        "treatmentDefaults": DEFAULTS,
        "methodology": [
            "Counts, exercise prices and conversion terms are the company's inline XBRL disclosures (the dilution bridge's inventory); prices and treatments are the caller's.",
            "Each scenario is reported as computed; no scenario is recommended and no denominator is selected.",
            "An unresolved instrument is left out of resultingShares and listed; an instrument in notDisclosed was not tagged, which is not proof it does not exist.",
            "Net-share or cash settlement, capped calls, make-whole adjustments and performance conditions are not modeled.",
            ("TAGGED_INSTRUMENTS_RESOLVED means every tagged instrument was resolved, not that every claim on the equity was found; claims quoted in "
             "unquantifiedShareClaims are in no scenario's resultingShares."),
        ],
        "selectedScenario": None,
        "selectedDenominator": None,
        **AUTHORITY_BOUNDARY,
    }
