"""Valuation snapshot and peer multiples (2.4.0).

Mirrors worker/src/valuation.ts; scripts/test_valuation.py requires identical
output from both runtimes. Mechanical context only: enterprise value from a
price, a share count and disclosed balances, and multiples over reported
results and Yahoo's consensus. No implied price, no forecast, nothing
back-solved from price targets.
"""

from __future__ import annotations

import datetime
import math
from typing import Any

from yfmcp.capital_structure import round_half_up


def _num(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if math.isfinite(value) else None


def _raw_num(value: Any) -> float | None:
    return _num(value.get("raw") if isinstance(value, dict) else value)


def _text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def empty_market_inputs(ticker: str) -> dict:
    return {
        "ticker": ticker.upper(), "name": None, "currency": None, "financialCurrency": None,
        "price": None, "priceTime": None, "sharesOutstanding": None, "marketCap": None,
        "totalCash": None, "totalDebt": None, "ttmRevenue": None, "ttmEbitda": None,
        "grossMarginPct": None, "trailingEps": None, "estimates": [],
    }


def market_inputs_from_quote_summary(ticker: str, result: dict) -> dict:
    """Yahoo quoteSummary (price, financialData, defaultKeyStatistics, earningsTrend) as valuation inputs."""
    price = result.get("price") or {}
    fd = result.get("financialData") or {}
    ks = result.get("defaultKeyStatistics") or {}
    trend = (result.get("earningsTrend") or {}).get("trend") or []
    time = _raw_num(price.get("regularMarketTime"))
    gross = _raw_num(fd.get("grossMargins"))
    estimates = []
    for t in trend:
        if not isinstance(t.get("period"), str):
            continue
        rev = t.get("revenueEstimate") or {}
        eps = t.get("earningsEstimate") or {}
        estimates.append({
            "period": t["period"],
            "revenueAvg": _raw_num(rev.get("avg")),
            "revenueAnalysts": _raw_num(rev.get("numberOfAnalysts")),
            "epsAvg": _raw_num(eps.get("avg")),
            "epsAnalysts": _raw_num(eps.get("numberOfAnalysts")),
        })
    return {
        "ticker": ticker.upper(),
        "name": _text(price.get("longName")) or _text(price.get("shortName")),
        "currency": _text(price.get("currency")),
        "financialCurrency": _text(fd.get("financialCurrency")),
        "price": _raw_num(price.get("regularMarketPrice")),
        "priceTime": _iso_time(time),
        "sharesOutstanding": _raw_num(ks.get("sharesOutstanding")),
        "marketCap": _raw_num(price.get("marketCap")),
        "totalCash": _raw_num(fd.get("totalCash")),
        "totalDebt": _raw_num(fd.get("totalDebt")),
        "ttmRevenue": _raw_num(fd.get("totalRevenue")),
        "ttmEbitda": _raw_num(fd.get("ebitda")),
        "grossMarginPct": round_half_up(gross * 100, 2) if gross is not None else None,
        "trailingEps": _raw_num(ks.get("trailingEps")),
        "estimates": estimates,
    }


def _iso_time(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    moment = datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def market_inputs_from_yfinance(ticker: str, info: dict, revenue_rows: list[dict], earnings_rows: list[dict]) -> dict:
    """yfinance info plus revenue_estimate / earnings_estimate rows as valuation inputs."""
    out = empty_market_inputs(ticker)
    gross = _num(info.get("grossMargins"))
    out.update({
        "name": _text(info.get("longName")) or _text(info.get("shortName")),
        "currency": _text(info.get("currency")),
        "financialCurrency": _text(info.get("financialCurrency")),
        "price": _num(info.get("currentPrice")) if _num(info.get("currentPrice")) is not None else _num(info.get("regularMarketPrice")),
        "priceTime": _iso_time(_num(info.get("regularMarketTime"))),
        "sharesOutstanding": _num(info.get("sharesOutstanding")),
        "marketCap": _num(info.get("marketCap")),
        "totalCash": _num(info.get("totalCash")),
        "totalDebt": _num(info.get("totalDebt")),
        "ttmRevenue": _num(info.get("totalRevenue")),
        "ttmEbitda": _num(info.get("ebitda")),
        "grossMarginPct": round_half_up(gross * 100, 2) if gross is not None else None,
        "trailingEps": _num(info.get("trailingEps")),
    })
    eps_by_period = {str(r.get("period")): r for r in earnings_rows if isinstance(r, dict)}
    for row in revenue_rows:
        if not isinstance(row, dict) or not isinstance(row.get("period"), str):
            continue
        eps = eps_by_period.get(row["period"], {})
        out["estimates"].append({
            "period": row["period"],
            "revenueAvg": _num(row.get("avg")),
            "revenueAnalysts": _num(row.get("numberOfAnalysts")),
            "epsAvg": _num(eps.get("avg")),
            "epsAnalysts": _num(eps.get("numberOfAnalysts")),
        })
    return out


# Listings quoted in a minor unit, with the major currency their financials use.
_MINOR_UNITS = {"GBp": ("GBP", 100), "GBX": ("GBP", 100), "ZAc": ("ZAR", 100), "ILA": ("ILS", 100)}
_PERIOD_LABELS = {"0y": "current_fiscal_year", "+1y": "next_fiscal_year"}


def _major_price(price: float, currency: str | None) -> tuple[float, str | None]:
    unit = _MINOR_UNITS.get(currency) if currency else None
    return (price / unit[1], unit[0]) if unit else (price, currency)


def _estimate(market: dict, period: str) -> dict | None:
    return next((e for e in market["estimates"] if e["period"] == period), None)


def _ratio(numerator: float | None, denominator: float | None, digits: int = 2) -> float | None:
    return round_half_up(numerator / denominator, digits) if numerator is not None and denominator is not None and denominator > 0 else None


def _multiples(ev: float | None, price: float | None, market: dict, comparable: bool) -> list[dict]:
    rows: list[dict] = []

    def add(metric: str, basis: str, numerator: float | None, denominator: float | None, source: str, analysts: float | None) -> None:
        multiple = None
        if not comparable:
            note = "Financials are reported in a different currency from the quote; not computed."
        elif numerator is None:
            note = "Numerator unavailable."
        elif denominator is None:
            note = "Denominator unavailable."
        elif denominator <= 0:
            note = "Denominator is zero or negative; a multiple is not meaningful."
        else:
            note = None
            multiple = _ratio(numerator, denominator)
        rows.append({"metric": metric, "basis": basis, "multiple": multiple, "denominator": denominator,
                     "denominatorSource": source, "analysts": analysts, "note": note})

    add("EV/Revenue", "trailing_12_months", ev, market["ttmRevenue"], "yahoo_financial_data_ttm", None)
    for period in ("0y", "+1y"):
        e = _estimate(market, period)
        add("EV/Revenue", _PERIOD_LABELS[period], ev, e["revenueAvg"] if e else None, "yahoo_consensus_estimate", e["revenueAnalysts"] if e else None)
    add("EV/EBITDA", "trailing_12_months", ev, market["ttmEbitda"], "yahoo_financial_data_ttm", None)
    add("P/E", "trailing_12_months", price, market["trailingEps"], "yahoo_trailing_eps", None)
    for period in ("0y", "+1y"):
        e = _estimate(market, period)
        add("P/E", _PERIOD_LABELS[period], price, e["epsAvg"] if e else None, "yahoo_consensus_estimate", e["epsAnalysts"] if e else None)
    return rows


def _growth(market: dict) -> dict:
    fy0 = _estimate(market, "0y")
    fy1 = _estimate(market, "+1y")

    def pct(a: float | None, b: float | None) -> float | None:
        return round_half_up((a / b - 1) * 100, 2) if a is not None and b is not None and b > 0 else None

    return {
        "currentFiscalYearVsTtmPct": pct(fy0["revenueAvg"] if fy0 else None, market["ttmRevenue"]),
        "nextVsCurrentFiscalYearPct": pct(fy1["revenueAvg"] if fy1 else None, fy0["revenueAvg"] if fy0 else None),
    }


def _yahoo_basis(market: dict, price: float | None) -> dict:
    if price is None:
        return {"marketCap": None, "enterpriseValue": None}
    major, _ = _major_price(price, market["currency"])
    market_cap = major * market["sharesOutstanding"] if market["sharesOutstanding"] is not None else market["marketCap"]
    ev = market_cap + market["totalDebt"] - market["totalCash"] if market_cap is not None and market["totalDebt"] is not None and market["totalCash"] is not None else None
    return {"marketCap": round_half_up(market_cap) if market_cap is not None else None, "enterpriseValue": round_half_up(ev) if ev is not None else None}


def _comparable_currency(market: dict) -> bool:
    currency = market["currency"]
    quote = (_MINOR_UNITS[currency][0] if currency in _MINOR_UNITS else currency) if currency else None
    return not market["financialCurrency"] or not quote or market["financialCurrency"] == quote


def valuation_snapshot(ticker: str, market: dict, supplied_price: float | None, bridge: dict | None, capital: dict | None,
                       sec_warnings: list[dict]) -> dict:
    """Price, diluted shares, disclosed balances and multiples for one ticker."""
    warnings = list(sec_warnings)
    quote = supplied_price if supplied_price is not None else market["price"]
    if quote is None:
        return {"ticker": ticker, "status": "PRICE_UNAVAILABLE", "code": "PRICE_UNAVAILABLE", "message": "No price was supplied and Yahoo returned none.", "warnings": warnings}
    major, major_currency = _major_price(quote, market["currency"])
    bridge_core = (bridge or {}).get("bridge") if bridge else None
    bridge_basic = (bridge or {}).get("basicShares") if bridge else None
    sec_basic = _num((bridge_basic or {}).get("shares"))
    basic = sec_basic if sec_basic is not None else market["sharesOutstanding"]
    diluted = _num((bridge_core or {}).get("dilutedSharesAtPrice"))
    share_basis = "sec_cover_page" if sec_basic is not None else ("yahoo_shares_outstanding" if market["sharesOutstanding"] is not None else None)
    value_shares = diluted if diluted is not None else basic

    # Balances: the filing's period-end values when read, else Yahoo's totals.
    balances = (capital or {}).get("balances") if capital else None
    sec_balances = balances is not None and _num(balances.get("totalDebt")) is not None and _num(balances.get("cashAndEquivalents")) is not None
    cash = _num(balances["cashAndEquivalents"]) if sec_balances else market["totalCash"]
    short_term = (_num(balances.get("shortTermInvestments")) or 0) if sec_balances else 0
    debt = _num(balances["totalDebt"]) if sec_balances else market["totalDebt"]
    balance_basis = "sec_filing_period_end" if sec_balances else ("yahoo_financial_data" if market["totalDebt"] is not None else None)
    if not sec_balances:
        warnings.append({"code": "YAHOO_BALANCES", "message": "Cash and debt are Yahoo's totals (debt can include leases), not the filing's period-end balances.", "severity": "info"})

    # Convertibles counted as shares leave the debt, so they are not counted twice.
    convertible_adjustment = 0
    convertibles = next((c for c in ((bridge or {}).get("components") or []) if c.get("component") == "convertible_debt"), None)
    for inst in (convertibles or {}).get("instruments") or []:
        if (_num(inst.get("incrementalShares")) or 0) > 0:
            convertible_adjustment += _num(inst.get("principal")) or 0
    if debt is not None:
        convertible_adjustment = min(convertible_adjustment, debt)

    equity_value = major * value_shares if value_shares is not None else None
    enterprise_value = (equity_value + debt - convertible_adjustment - cash - short_term
                        if equity_value is not None and debt is not None and cash is not None else None)
    if enterprise_value is None:
        warnings.append({"code": "ENTERPRISE_VALUE_INCOMPLETE", "message": "Shares, cash or debt were unavailable, so enterprise value was not computed.", "severity": "warning"})
    comparable = _comparable_currency(market)
    if not comparable:
        warnings.append({"code": "FINANCIAL_CURRENCY_MISMATCH", "message": f"Financials are in {market['financialCurrency']}; the quote is in {market['currency']}.", "severity": "warning"})

    atm = (bridge or {}).get("atmProgram") if bridge else None
    yahoo = _yahoo_basis(market, quote)
    shares_diff = (round_half_up((basic / market["sharesOutstanding"] - 1) * 100, 2)
                   if basic is not None and market["sharesOutstanding"] and market["sharesOutstanding"] > 0 and share_basis == "sec_cover_page" else None)
    return {
        "ticker": ticker,
        "name": market["name"],
        "status": "COMPUTED" if enterprise_value is not None else "PARTIAL",
        "basis": "MECHANICAL_VALUATION_CONTEXT",
        "decisionUse": "CONTEXT_ONLY_NOT_A_PRICE_TARGET",
        "price": {
            "amount": quote,
            "currency": market["currency"],
            "majorUnitAmount": major,
            "majorCurrency": major_currency,
            "source": "caller_supplied" if supplied_price is not None else "yahoo_regular_market",
            "asOf": None if supplied_price is not None else market["priceTime"],
        },
        "shares": {
            "basic": basic,
            "diluted": diluted,
            "usedForValue": value_shares,
            "basis": share_basis,
            "dilutionPctAtPrice": _num((bridge_core or {}).get("dilutionPctAtPrice")),
            "bridgeStatus": (bridge or {}).get("status") if bridge else None,
            "yahooSharesOutstanding": market["sharesOutstanding"],
            "secVsYahooSharesDiffPct": shares_diff,
        },
        "balances": {
            "cash": cash,
            "shortTermInvestments": short_term if sec_balances else None,
            "totalDebt": debt,
            "convertibleDebtCountedAsShares": round_half_up(convertible_adjustment),
            "basis": balance_basis,
            "periodEnd": (capital or {}).get("periodEnd") if sec_balances else None,
        },
        "equityValue": round_half_up(equity_value) if equity_value is not None else None,
        "enterpriseValue": round_half_up(enterprise_value) if enterprise_value is not None else None,
        "enterpriseValueFormula": "price x diluted shares + total debt - convertible debt counted as shares - cash - short-term investments",
        "peerComparableBasis": {**yahoo, "note": "Price x Yahoo shares outstanding + Yahoo total debt - Yahoo total cash: the basis compare_peer_valuations uses for every peer."},
        "multiples": _multiples(enterprise_value, major, market, comparable),
        "revenueGrowth": _growth(market),
        "grossMarginPct": market["grossMarginPct"],
        "atmCapacity": {
            "remainingCapacity": atm.get("remainingCapacityUsd"),
            "programSize": atm.get("programSizeUsd"),
            "potentialSharesAtPrice": atm.get("potentialShares"),
            "note": "Possible issuance at the company's discretion; not included in the share count above.",
        } if atm else None,
        "methodology": [
            "Enterprise value uses the dilution bridge's share count at this price and the filing's period-end cash and debt, falling back to Yahoo when no SEC filing is read.",
            "Multiples divide by Yahoo's trailing results and consensus estimates as published; the analyst counts say how many estimates stand behind each.",
            "This is context for a valuation, not one: no implied price, no forecast, and nothing back-solved from price targets.",
        ],
        "warnings": warnings,
    }


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 == 1 else (ordered[mid - 1] + ordered[mid]) / 2


def peer_row(market: dict) -> dict:
    """One peer row on the Yahoo basis."""
    yahoo = _yahoo_basis(market, market["price"])
    comparable = _comparable_currency(market)
    major = _major_price(market["price"], market["currency"])[0] if market["price"] is not None else None
    rows = _multiples(yahoo["enterpriseValue"], major, market, comparable)

    def value(metric: str, basis: str) -> float | None:
        return next((r["multiple"] for r in rows if r["metric"] == metric and r["basis"] == basis), None)

    return {
        "ticker": market["ticker"],
        "name": market["name"],
        "price": market["price"],
        "currency": market["currency"],
        "marketCap": yahoo["marketCap"],
        "enterpriseValue": yahoo["enterpriseValue"],
        "evToRevenueTtm": value("EV/Revenue", "trailing_12_months"),
        "evToRevenueCurrentFy": value("EV/Revenue", "current_fiscal_year"),
        "evToRevenueNextFy": value("EV/Revenue", "next_fiscal_year"),
        "evToEbitdaTtm": value("EV/EBITDA", "trailing_12_months"),
        "peCurrentFy": value("P/E", "current_fiscal_year"),
        "peNextFy": value("P/E", "next_fiscal_year"),
        "revenueGrowthNextFyPct": _growth(market)["nextVsCurrentFiscalYearPct"],
        "grossMarginPct": market["grossMarginPct"],
        "comparableCurrency": comparable,
    }


_PEER_METRICS = ["evToRevenueTtm", "evToRevenueCurrentFy", "evToRevenueNextFy", "evToEbitdaTtm", "peCurrentFy", "peNextFy", "revenueGrowthNextFyPct", "grossMarginPct"]


def peer_valuations(subject: str | None, markets: list[dict], errors: list[dict]) -> dict:
    """Multiples for a peer set on one basis, with peer medians and the subject against them."""
    rows = [peer_row(m) for m in markets]
    subject_u = subject.upper() if subject else None
    peers = [r for r in rows if r["ticker"] != subject_u]
    subject_row = next((r for r in rows if r["ticker"] == subject_u), None)
    summary: dict = {}
    versus: dict = {}
    for metric in _PEER_METRICS:
        values = [v for v in (_num(r[metric]) for r in peers) if v is not None]
        med = _median(values)
        summary[metric] = {
            "median": round_half_up(med, 2) if med is not None else None,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "count": len(values),
        }
        if subject_row:
            own = _num(subject_row[metric])
            versus[metric] = {
                "subject": own,
                "peerMedian": round_half_up(med, 2) if med is not None else None,
                "premiumPct": round_half_up((own / med - 1) * 100, 2) if own is not None and med is not None and med > 0 and not metric.endswith("Pct") else None,
            }
    return {
        "subject": subject_u,
        "basis": "YAHOO_MARKET_AND_CONSENSUS",
        "decisionUse": "CONTEXT_ONLY_NOT_A_PRICE_TARGET",
        "rows": rows,
        "peerSummary": summary,
        "subjectVsPeerMedian": versus if subject_row else None,
        "notes": [
            "Every row uses the same basis: Yahoo price x shares outstanding + total debt - total cash, over Yahoo's trailing results and consensus.",
            "Peer medians exclude the subject. A premium or discount is context, not a signal; peers differ in growth, margins and risk.",
            "Multiples on zero or negative denominators are left empty rather than shown as meaningless numbers.",
        ],
        "errors": errors,
    }
