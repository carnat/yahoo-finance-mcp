"""Historical valuation context (2.5.4), mirroring worker/src/valuation-history.ts.

Parity is tested in scripts/test_valuation_history_and_reconcile.py. Pure.

For each requested date, market capitalization, enterprise value and
multiples as they could have been computed on that date:
- the price is the close on or before the date, with Yahoo's later split
  adjustments undone so it matches the share count reported at the time;
- shares, balances and results are SEC companyfacts filed on or before the
  date (point in time: no later amendment or restatement is used);
- denominators are the last fiscal year (LFY) and the last twelve months
  (LTM = LFY + current year-to-date - prior year-to-date), each naming its
  periods, concepts and filings;
- reporting-currency figures are converted at the date's FX close, and an
  ADR's ordinary shares are divided by the ordinary shares per ADS.
A figure that is not tagged or not filed yet is reported as such; the
multiples it feeds are null. No multiple is selected or weighted.
"""

from __future__ import annotations

import datetime as _dt
import re
from typing import Any

from yfmcp.capital_structure import round_half_up as _round
from yfmcp.evidence import AUTHORITY_BOUNDARY
from yfmcp.sec_facts import REVENUE_CONCEPTS
from yfmcp.valuation import _major_price

US_GAAP: dict = {
    "revenue": list(REVENUE_CONCEPTS),
    "operatingIncome": ["OperatingIncomeLoss"],
    "netIncome": ["NetIncomeLoss", "ProfitLoss"],
    "depreciationAmortization": [["DepreciationDepletionAndAmortization"], ["DepreciationAndAmortization"], ["DepreciationAmortizationAndAccretionNet"],
                                 ["Depreciation", "AmortizationOfIntangibleAssets"]],
    "cash": ["CashAndCashEquivalentsAtCarryingValue", "Cash"],
    "shortTermInvestments": ["ShortTermInvestments", "MarketableSecuritiesCurrent", "AvailableForSaleSecuritiesDebtSecuritiesCurrent"],
    "debtGroups": [["LongTermDebt"], ["LongTermDebtCurrent", "LongTermDebtNoncurrent"],
                   ["ConvertibleNotesPayableCurrent", "ConvertibleNotesPayable", "ConvertibleLongTermNotesPayable"], ["NotesPayableCurrent", "NotesPayable"]],
    "debtAdditions": ["ShortTermBorrowings", "CommercialPaper"],
    "sharesInstant": ["CommonStockSharesOutstanding"],
    "sharesWeighted": ["WeightedAverageNumberOfSharesOutstandingBasic"],
}

IFRS: dict = {
    "revenue": ["Revenue", "RevenueFromContractsWithCustomers"],
    "operatingIncome": ["ProfitLossFromOperatingActivities"],
    "netIncome": ["ProfitLossAttributableToOwnersOfParent", "ProfitLoss"],
    "depreciationAmortization": [["DepreciationAndAmortisationExpense"], ["DepreciationExpense", "AmortisationExpense"]],
    "cash": ["CashAndCashEquivalents"],
    "shortTermInvestments": [],
    "debtGroups": [["Borrowings"], ["CurrentBorrowingsAndCurrentPortionOfNoncurrentBorrowings", "NoncurrentPortionOfNoncurrentBorrowings"],
                   ["ShorttermBorrowings", "CurrentPortionOfLongtermBorrowings", "LongtermBorrowings"]],
    "debtAdditions": [],
    "sharesInstant": [],
    "sharesWeighted": ["WeightedAverageShares"],
}

_PERIODIC_FORM_RE = re.compile(r"(?:10-K|10-Q|20-F|40-F)")
MAX_DATES = 12


def days(start: str, end: str) -> int:
    return (_dt.date.fromisoformat(end[:10]) - _dt.date.fromisoformat(start[:10])).days


def _num(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def taxonomy_of(companyfacts: Any) -> tuple[str | None, str | None]:
    """us-gaap when it carries revenue, else ifrs-full; the reporting currency is the revenue unit with the most facts."""
    facts = companyfacts.get("facts") if isinstance(companyfacts, dict) else None
    for taxonomy, mapping in (("us-gaap", US_GAAP), ("ifrs-full", IFRS)):
        tax = (facts or {}).get(taxonomy)
        if not tax:
            continue
        counts: dict[str, int] = {}
        for concept in mapping["revenue"]:
            for unit, rows in ((tax.get(concept) or {}).get("units") or {}).items():
                if re.fullmatch(r"[A-Z]{3}", unit):
                    counts[unit] = counts.get(unit, 0) + len(rows)
        if not counts:
            continue
        currency = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        return taxonomy, currency
    return None, None


def _facts_for(tax: dict | None, concepts: list[str], unit: str, as_of: str) -> list[dict]:
    """Periodic-report facts for concepts in one unit, filed on or before as_of; the newest filing of each period wins."""
    by_period: dict[str, dict] = {}
    for concept in concepts:
        rows = (((tax or {}).get(concept) or {}).get("units") or {}).get(unit) or []
        for f in rows:
            if not isinstance(f.get("end"), str) or not _num(f.get("val")) or not isinstance(f.get("filed"), str):
                continue
            if f["filed"] > as_of or not _PERIODIC_FORM_RE.match(str(f.get("form") or "")):
                continue
            start = f.get("start") if isinstance(f.get("start"), str) else None
            key = f"{start or ''}|{f['end']}"
            prev = by_period.get(key)
            candidate = {"concept": concept, "start": start, "end": f["end"], "val": f["val"], "filed": f["filed"], "form": str(f["form"]),
                         "accn": f.get("accn") if isinstance(f.get("accn"), str) else None}
            candidate_priority = concepts.index(concept)
            previous_priority = concepts.index(prev["concept"]) if prev is not None else 10**9
            # Concept order is semantic precedence. A later filing may update the
            # same concept, but a lower-priority alternate concept must not silently replace it.
            if prev is None or candidate_priority < previous_priority or (candidate_priority == previous_priority and f["filed"] > prev["filed"]):
                by_period[key] = candidate
    return list(by_period.values())


def _component(f: dict, sign: int = 1) -> dict:
    return {"concept": f["concept"], "periodStart": f["start"], "periodEnd": f["end"], "value": f["val"], "sign": sign, "form": f["form"],
            "filed": f["filed"], "accessionNumber": f["accn"]}


def _is_annual(f: dict) -> bool:
    return f["start"] is not None and 350 <= days(f["start"], f["end"]) <= 380


def flow_bases(facts: list[dict]) -> dict:
    """A flow metric on the LFY and LTM bases.

    LTM is the last fiscal year when it is the latest period filed, else
    LFY + current year-to-date - prior year-to-date.
    """
    durations = [f for f in facts if f["start"] is not None]
    annual = sorted([f for f in durations if _is_annual(f)], key=lambda f: f["end"])
    lfy = annual[-1] if annual else None
    if lfy is None:
        missing = {"status": "NOT_AVAILABLE", "value": None, "reason": "NO_ANNUAL_PERIOD_FILED"}
        return {"LFY": missing, "LTM": missing}
    lfy_out = {"status": "OK", "value": lfy["val"], "periodStart": lfy["start"], "periodEnd": lfy["end"], "method": "REPORTED_FISCAL_YEAR",
               "components": [_component(lfy)]}
    later = [f for f in durations if f["end"] > lfy["end"]]
    if not later:
        return {"LFY": lfy_out, "LTM": {**lfy_out, "method": "LAST_FISCAL_YEAR_IS_LATEST"}}
    latest_end = max(f["end"] for f in later)
    # The year-to-date period that starts the day after the fiscal year ended.
    candidates = [f for f in later if f["end"] == latest_end and 0 <= days(lfy["end"], f["start"]) <= 4]
    candidates.sort(key=lambda f: -days(f["start"], f["end"]))
    ytd = candidates[0] if candidates else None
    if ytd is None:
        return {"LFY": lfy_out, "LTM": {"status": "NOT_AVAILABLE", "value": None, "reason": "YEAR_TO_DATE_NOT_FILED", "latestPeriodEnd": latest_end}}
    span = days(ytd["start"], ytd["end"])
    prior = next((f for f in durations
                  if abs(days(lfy["start"], f["start"])) <= 4 and 358 <= days(f["end"], ytd["end"]) <= 372
                  and abs(days(f["start"], f["end"]) - span) <= 7), None)
    if prior is None:
        return {"LFY": lfy_out, "LTM": {"status": "NOT_AVAILABLE", "value": None, "reason": "PRIOR_YEAR_TO_DATE_NOT_FILED", "latestPeriodEnd": latest_end}}
    return {
        "LFY": lfy_out,
        "LTM": {
            "status": "OK",
            "value": lfy["val"] + ytd["val"] - prior["val"],
            "periodStart": None,
            "periodEnd": ytd["end"],
            "method": "LFY_PLUS_YTD_MINUS_PRIOR_YTD",
            "components": [_component(lfy), _component(ytd), _component(prior, -1)],
        },
    }


def rate_at(bars: list[dict], date: str) -> dict | None:
    best = None
    for b in bars:
        if b["date"] <= date and days(b["date"], date) <= 7 and (best is None or b["date"] > best["date"]):
            best = b
    return best


def _latest(facts: list[dict]) -> dict | None:
    if not facts:
        return None
    ordered = sorted(facts, key=lambda f: (f["end"], -days(f["start"] or f["end"], f["end"])))
    return ordered[-1]


def _shares_at(dei: dict | None, tax: dict | None, mapping: dict, as_of: str) -> dict | None:
    cover = _latest([f for f in _facts_for(dei, ["EntityCommonStockSharesOutstanding"], "shares", as_of) if f["start"] is None])
    if cover:
        return {"value": cover["val"], "asOf": cover["end"], "basis": "COVER_PAGE", **_component(cover)}
    balance = _latest([f for f in _facts_for(tax, mapping["sharesInstant"], "shares", as_of) if f["start"] is None])
    if balance:
        return {"value": balance["val"], "asOf": balance["end"], "basis": "BALANCE_SHEET", **_component(balance)}
    weighted = _latest([f for f in _facts_for(tax, mapping["sharesWeighted"], "shares", as_of) if f["start"] is not None])
    if weighted:
        return {"value": weighted["val"], "asOf": weighted["end"], "basis": "WEIGHTED_AVERAGE_BASIC", **_component(weighted)}
    return None


def _balances_at(tax: dict | None, mapping: dict, currency: str, as_of: str) -> dict:
    cash_facts = sorted([f for f in _facts_for(tax, mapping["cash"], currency, as_of) if f["start"] is None], key=lambda f: f["end"])
    if not cash_facts:
        return {"status": "NOT_AVAILABLE", "reason": "CASH_NOT_TAGGED"}
    at = cash_facts[-1]["end"]

    # Every balance is read at the cash balance's date; a concept last tagged earlier is not carried forward.
    def tagged(concept: str) -> dict | None:
        rows = [f for f in _facts_for(tax, [concept], currency, as_of) if f["start"] is None and f["end"] == at]
        return rows[0] if rows else None

    cash = [f for f in cash_facts if f["end"] == at][0]
    sti = next((f for f in (tagged(c) for c in mapping["shortTermInvestments"]) if f is not None), None)
    debt: dict = {"status": "NOT_TAGGED", "value": None, "concepts": [c for g in mapping["debtGroups"] for c in g]}
    for group in mapping["debtGroups"]:
        parts = [f for f in (tagged(c) for c in group) if f is not None]
        if not parts:
            continue
        additions = [f for f in (tagged(c) for c in mapping["debtAdditions"]) if f is not None]
        all_parts = parts + additions
        debt = {"status": "OK", "value": sum_values(all_parts), "components": [_component(f) for f in all_parts]}
        break
    return {
        "status": "OK",
        "balanceDate": at,
        "currency": currency,
        "cash": {"value": cash["val"], **_component(cash)},
        "shortTermInvestments": ({"status": "OK", "value": sti["val"], **_component(sti)} if sti else
                                 {"status": "NOT_TAGGED" if mapping["shortTermInvestments"] else "NOT_MAPPED", "value": None,
                                  "concepts": mapping["shortTermInvestments"]}),
        "debt": debt,
    }


def sum_values(facts: list[dict]) -> float:
    total = 0
    for f in facts:
        total += f["val"]
    return total


def _multiple(numerator: float | None, denominator: dict | None, fx: float | None, numerator_name: str, basis: str, metric: str) -> dict:
    den = denominator["value"] if denominator and denominator.get("status") == "OK" else None
    ref = {"numerator": numerator_name, "denominator": metric, "basis": basis,
           "denominatorPeriodEnd": denominator.get("periodEnd") if denominator else None}
    if numerator is None:
        return {"value": None, "status": f"{numerator_name.upper()}_NOT_AVAILABLE", **ref}
    if den is None:
        return {"value": None, "status": "DENOMINATOR_NOT_AVAILABLE", **ref}
    if fx is None:
        return {"value": None, "status": "FX_NOT_AVAILABLE", **ref}
    # A negative enterprise value (net cash above market cap) has no meaningful multiple.
    if not numerator > 0:
        return {"value": None, "status": "NOT_MEANINGFUL_NONPOSITIVE_NUMERATOR", **ref}
    converted = den * fx
    if not converted > 0:
        return {"value": None, "status": "NOT_MEANINGFUL_NONPOSITIVE_DENOMINATOR", **ref}
    return {"value": _round(numerator / converted, 2), "status": "OK", **ref}


def _ebitda(oi: dict, da: dict, da_concepts: list[str] | None) -> dict:
    """EBITDA as operating income plus depreciation and amortization on the same basis and periods."""
    if oi["status"] != "OK":
        return {"status": "NOT_AVAILABLE", "value": None, "reason": "OPERATING_INCOME_NOT_AVAILABLE"}
    if da["status"] != "OK":
        return {"status": "NOT_AVAILABLE", "value": None, "reason": "DEPRECIATION_AMORTIZATION_NOT_AVAILABLE"}
    if oi.get("periodEnd") != da.get("periodEnd"):
        return {"status": "NOT_AVAILABLE", "value": None, "reason": "PERIODS_DIFFER", "operatingIncomePeriodEnd": oi.get("periodEnd"),
                "depreciationPeriodEnd": da.get("periodEnd")}
    return {
        "status": "OK",
        "value": oi["value"] + da["value"],
        "periodStart": oi.get("periodStart"),
        "periodEnd": oi.get("periodEnd"),
        "method": f"operating income + {' + '.join(da_concepts or [])} (computed, not a reported figure)",
        "operatingIncome": oi,
        "depreciationAmortization": da,
    }


def _da_for(tax: dict | None, mapping: dict, currency: str, as_of: str, oi: dict) -> dict:
    """Depreciation and amortization per basis.

    The first concept group with that basis for operating income's period (VRT
    tags the total only in 10-Ks and depreciation and amortization separately
    in 10-Qs). Separate parts are summed when all are there for the same
    periods.
    """
    groups = []
    for group in mapping["depreciationAmortization"]:
        parts = [flow_bases(_facts_for(tax, [c], currency, as_of)) for c in group]
        if len(parts) == 1:
            groups.append((group, parts[0]))
            continue

        def summed(key: str, parts: list[dict] = parts) -> dict:
            rows = [p[key] for p in parts]
            if any(r["status"] != "OK" for r in rows) or any(r.get("periodEnd") != rows[0].get("periodEnd") for r in rows):
                return {"status": "NOT_AVAILABLE", "value": None, "reason": "PARTS_NOT_ALIGNED"}
            total = 0
            for r in rows:
                total += r["value"]
            return {**rows[0], "value": total, "components": [c for r in rows for c in r["components"]]}

        groups.append((group, {"LFY": summed("LFY"), "LTM": summed("LTM")}))

    def pick(key: str) -> dict:
        ok = [(g, b) for g, b in groups if b[key]["status"] == "OK"]
        aligned = next(((g, b) for g, b in ok if b[key].get("periodEnd") == oi[key].get("periodEnd")), ok[0] if ok else None)
        if aligned is None:
            return {"value": {"status": "NOT_AVAILABLE", "value": None, "reason": "NOT_TAGGED"}, "concepts": None}
        return {"value": aligned[1][key], "concepts": aligned[0]}

    return {"LFY": pick("LFY"), "LTM": pick("LTM")}


def staleness_limits(companyfacts: Any) -> dict:
    """Staleness limits in days.

    Annual-only (20-F/40-F) filers publish balances and share counts once a
    year, about four months after year end, so a quarterly filer's 200-day
    balance limit would leave their EV null for most of each year.
    """
    if foreign_filer(companyfacts):
        return {"cadence": "ANNUAL", "shareCountDays": 500, "balancesDays": 500, "resultsDays": 500}
    return {"cadence": "QUARTERLY", "shareCountDays": 400, "balancesDays": 200, "resultsDays": 500}


def valuation_at_date(inp: dict, date: str, taxonomy: str, currency: str) -> dict:
    """Market value, balances, denominators and multiples as they stood on one date."""
    limits = staleness_limits(inp["companyfacts"])
    facts = inp["companyfacts"]["facts"]
    tax = facts.get(taxonomy)
    dei = facts.get("dei")
    mapping = IFRS if taxonomy == "ifrs-full" else US_GAAP
    warnings: list[dict] = []

    bar = rate_at(inp["bars"], date)
    if bar is None:
        return {"date": date, "status": "PRICE_UNAVAILABLE",
                "warnings": [{"code": "PRICE_UNAVAILABLE", "message": f"No close within 7 days on or before {date}.", "severity": "warning"}]}
    # Yahoo's closes are adjusted for every later split; undo that so the price matches the share count reported then.
    later_splits = [s for s in inp["splits"] if s["date"] > bar["date"] and s["ratio"] > 0]
    split_factor = 1
    for s in later_splits:
        split_factor *= s["ratio"]
    major_price, price_currency = _major_price(bar["close"] * split_factor, inp.get("priceCurrency"))
    price = {"tradingDate": bar["date"], "closeAsAdjusted": bar["close"], "laterSplitFactor": split_factor, "close": _round(major_price, 4),
             "currency": price_currency, "laterSplits": later_splits}

    shares = _shares_at(dei, tax, mapping, date)
    if shares is None:
        market_cap: dict = {"status": "SHARES_NOT_AVAILABLE", "value": None}
    else:
        if shares["basis"] == "WEIGHTED_AVERAGE_BASIC":
            warnings.append({"code": "SHARE_COUNT_WEIGHTED_AVERAGE", "message": (
                "No undimensioned cover-page or balance-sheet share count is in companyfacts (companies with several share classes tag them "
                "per class), so the latest weighted-average basic count is used; it may cover only the listed class."), "severity": "warning"})
        share_count_stale = days(str(shares["asOf"]), date) > limits["shareCountDays"]
        if share_count_stale:
            warnings.append({"code": "SHARE_COUNT_STALE", "message": f"The latest share count filed by {date} is as of {shares['asOf']}.", "severity": "warning"})
        split_after_count = next((s for s in inp["splits"] if str(shares["asOf"]) < s["date"] <= bar["date"]), None)
        ads = inp.get("adsRatio")
        quoted = shares["value"] / ads if ads is not None and ads > 0 else shares["value"]
        if ads is not None:
            shares["quotedShareEquivalent"] = _round(quoted)
        market_cap = (
            {"status": "SPLIT_AFTER_SHARE_COUNT", "value": None, "split": split_after_count} if split_after_count else
            {"status": "SHARE_COUNT_STALE", "value": None, "asOf": shares["asOf"]} if share_count_stale else
            {"status": "OK", "value": _round(major_price * quoted), "currency": price_currency}
        )

    fx_rate: float | None = None
    fx: dict | None = None
    if price_currency is None or price_currency == currency:
        fx_rate = 1
    else:
        fx_in = inp.get("fx")
        r = rate_at(fx_in["bars"], date) if fx_in else None
        if r:
            fx_rate = r["close"]
            fx = {"pair": fx_in.get("pair") if fx_in else None, "date": r["date"], "rate": r["close"]}
        else:
            fx = {"pair": fx_in.get("pair") if fx_in else None, "status": "NOT_AVAILABLE"}
            warnings.append({"code": "FX_NOT_AVAILABLE", "message": (
                f"No {currency} to {price_currency} rate within 7 days on or before {date}; figures in {currency} are not converted and the "
                "multiples are null."), "severity": "warning"})

    balances = _balances_at(tax, mapping, currency, date)
    mcap = market_cap["value"] if market_cap["status"] == "OK" else None
    if mcap is None:
        enterprise_value: dict = {"status": "MARKET_CAP_NOT_AVAILABLE", "value": None}
    elif balances["status"] != "OK":
        enterprise_value = {"status": "BALANCES_NOT_AVAILABLE", "value": None}
    elif balances["debt"]["status"] != "OK":
        enterprise_value = {"status": "DEBT_NOT_TAGGED", "value": None}
    elif fx_rate is None:
        enterprise_value = {"status": "FX_NOT_AVAILABLE", "value": None}
    else:
        cash = balances["cash"]["value"]
        sti = balances["shortTermInvestments"]["value"]
        sti = 0 if sti is None else sti
        debt = balances["debt"]["value"]
        balances_stale = days(str(balances["balanceDate"]), date) > limits["balancesDays"]
        enterprise_value = (
            {"status": "BALANCES_STALE", "value": None, "balanceDate": balances["balanceDate"], "currency": price_currency}
            if balances_stale
            else {
                "status": "OK",
                "value": _round(mcap + (debt - cash - sti) * fx_rate),
                "currency": price_currency,
                "formula": "market cap + debt - cash - short-term investments (untagged short-term investments are left out)",
                "balanceDate": balances["balanceDate"],
            }
        )
        if balances["shortTermInvestments"]["status"] != "OK":
            warnings.append({"code": "SHORT_TERM_INVESTMENTS_NOT_TAGGED", "message": (
                "No short-term investment concept is tagged at the balance date; enterprise value subtracts cash only."), "severity": "info"})
        if balances_stale:
            warnings.append({"code": "BALANCES_STALE", "message": f"The latest balance sheet filed by {date} is as of {balances['balanceDate']}.",
                             "severity": "warning"})

    revenue = flow_bases(_facts_for(tax, mapping["revenue"], currency, date))
    oi = flow_bases(_facts_for(tax, mapping["operatingIncome"], currency, date))
    ni = flow_bases(_facts_for(tax, mapping["netIncome"], currency, date))
    da = _da_for(tax, mapping, currency, date, oi)
    ltm_end = str(revenue["LTM"]["periodEnd"]) if revenue["LTM"]["status"] == "OK" else None
    if ltm_end and days(ltm_end, date) > limits["resultsDays"]:
        warnings.append({"code": "RESULTS_STALE", "message": (
            f"The latest results in SEC companyfacts filed by {date} end {ltm_end}."), "severity": "warning"})
    denominators: dict = {}
    multiples: dict = {}
    ev = enterprise_value["value"] if enterprise_value["status"] == "OK" else None
    for basis in ("LTM", "LFY"):
        e = _ebitda(oi[basis], da[basis]["value"], da[basis]["concepts"])
        denominators[basis] = {"revenue": revenue[basis], "ebitda": e, "netIncome": ni[basis]}
        def with_freshness(m: dict, denominator: dict) -> dict:
            period_end = denominator.get("periodEnd")
            if isinstance(period_end, str) and days(period_end, date) > limits["resultsDays"]:
                return {**m, "value": None, "status": "RESULTS_STALE", "denominatorPeriodEnd": period_end}
            return m

        multiples[basis] = {
            "evToRevenue": with_freshness(_multiple(ev, revenue[basis], fx_rate, "enterpriseValue", basis, "revenue"), revenue[basis]),
            "evToEbitda": with_freshness(_multiple(ev, e, fx_rate, "enterpriseValue", basis, "ebitda"), e),
            "priceToEarnings": with_freshness(_multiple(mcap, ni[basis], fx_rate, "marketCap", basis, "netIncome"), ni[basis]),
            "priceToSales": with_freshness(_multiple(mcap, revenue[basis], fx_rate, "marketCap", basis, "revenue"), revenue[basis]),
        }
    all_ok = market_cap["status"] == "OK" and enterprise_value["status"] == "OK"
    return {
        "date": date,
        "status": "OK" if all_ok else "PARTIAL",
        "price": price,
        "shares": shares,
        "marketCap": market_cap,
        "fx": fx,
        "balances": balances,
        "enterpriseValue": enterprise_value,
        "denominators": denominators,
        "multiples": multiples,
        "warnings": warnings,
    }


def foreign_filer(companyfacts: Any) -> bool:
    """Whether the latest annual report is a 20-F or 40-F: a foreign private issuer, whose quote may be for ADSs."""
    taxonomy, currency = taxonomy_of(companyfacts)
    if not taxonomy or not currency:
        return False
    tax = companyfacts["facts"].get(taxonomy)
    mapping = IFRS if taxonomy == "ifrs-full" else US_GAAP
    annual = sorted([f for f in _facts_for(tax, mapping["revenue"], currency, "9999-12-31") if _is_annual(f)], key=lambda f: f["end"])
    return bool(annual) and re.match(r"(?:20|40)-F", annual[-1]["form"]) is not None


def latest_share_count(companyfacts: Any) -> float | None:
    """The newest share count companyfacts holds (cover page, balance sheet or weighted average), for the ADS ratio."""
    taxonomy, _ = taxonomy_of(companyfacts)
    if not taxonomy:
        return None
    facts = companyfacts.get("facts") or {}
    shares = _shares_at(facts.get("dei"), facts.get(taxonomy), IFRS if taxonomy == "ifrs-full" else US_GAAP, "9999-12-31")
    return shares["value"] if shares else None


def _leap(n: int) -> bool:
    return (n % 4 == 0 and n % 100 != 0) or n % 400 == 0


def valuation_dates(requested: list[str] | None, latest_trading_date: str | None) -> tuple[list[str], str | None]:
    """Requested dates, or the latest close and its anniversaries over five years."""
    if requested:
        if len(requested) > MAX_DATES:
            return [], f"At most {MAX_DATES} dates."
        for d in requested:
            try:
                ok = bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(d))) and _dt.date.fromisoformat(str(d)) is not None
            except ValueError:
                ok = False
            if not ok:
                return [], f"Invalid date {d}; use YYYY-MM-DD."
        return sorted(set(requested)), None
    if not latest_trading_date:
        return [], "No latest close to anchor default dates."
    y = int(latest_trading_date[:4])
    md = latest_trading_date[4:]
    out = []
    for k in range(5, 0, -1):
        # 29 February falls back to the 28th in years without it.
        out.append(f"{y - k}-02-28" if md == "-02-29" and not _leap(y - k) else f"{y - k}{md}")
    out.append(latest_trading_date)
    return out, None


def historical_valuation(inp: dict) -> dict:
    taxonomy, currency = taxonomy_of(inp["companyfacts"])
    base = {
        "ticker": inp["ticker"].upper(),
        "basis": "POINT_IN_TIME_SEC_FACTS",
        "taxonomy": taxonomy,
        "reportingCurrency": currency,
        "priceCurrency": _major_price(1, inp.get("priceCurrency"))[1] if inp.get("priceCurrency") else None,
        "adsRatio": inp.get("adsRatio"),
    }
    if not taxonomy or not currency:
        return {**base, "status": "FUNDAMENTALS_NOT_AVAILABLE", "points": [], "notes": ["No us-gaap or ifrs-full revenue facts are in companyfacts."],
                **AUTHORITY_BOUNDARY}
    points = [valuation_at_date(inp, d, taxonomy, currency) for d in inp["dates"]]
    limits = staleness_limits(inp["companyfacts"])
    return {
        **base,
        "reportingCadence": limits["cadence"],
        "stalenessLimitsDays": {"shareCount": limits["shareCountDays"], "balances": limits["balancesDays"], "results": limits["resultsDays"]},
        "status": "OK" if all(p["status"] == "OK" for p in points) else "PARTIAL",
        "points": points,
        "notes": [
            "Each date uses only SEC facts filed on or before it; the price is that day's close (or the last within 7 days) with later split adjustments undone.",
            "LTM = last fiscal year + current year-to-date - prior year-to-date, from the filings named in components; LFY is the last reported fiscal year.",
            "EBITDA is computed as operating income plus depreciation and amortization; it is not a reported figure.",
            "Balances are read at the latest cash balance date filed by the date; a concept not tagged at that date is not carried forward.",
            "Multiples with a missing or non-positive denominator are null with a status; none is selected or weighted.",
        ],
        **AUTHORITY_BOUNDARY,
    }


_MULTIPLE_KEYS = ["evToRevenue", "evToEbitda", "priceToEarnings", "priceToSales"]


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    s = sorted(values)
    mid = len(s) // 2
    return _round(s[mid] if len(s) % 2 == 1 else (s[mid - 1] + s[mid]) / 2, 2)


def peer_medians(dates: list[str], peers: list[dict]) -> list[dict]:
    """Unweighted medians of the peers' multiples on each date and basis, with the peers counted."""
    out = []
    for date in dates:
        row_out: dict = {"date": date}
        for basis in ("LTM", "LFY"):
            row: dict = {}
            for key in _MULTIPLE_KEYS:
                values: list[float] = []
                tickers: list[str] = []
                for peer in peers:
                    point = next((p for p in peer.get("points") or [] if p.get("date") == date), None)
                    m = (((point or {}).get("multiples") or {}).get(basis) or {}).get(key)
                    if m and m.get("status") == "OK" and _num(m.get("value")):
                        values.append(m["value"])
                        tickers.append(str(peer.get("ticker")))
                row[key] = {"median": _median(values), "count": len(values), "tickers": tickers}
            row_out[basis] = row
        out.append(row_out)
    return out
