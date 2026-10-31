"""Parity cases: valuation domain (BR-VAL-001 ... BR-VAL-036 and the valuation rules with a scheduled repair).

Python module          <->  Worker module
yfmcp/valuation.py          worker/src/valuation.ts            snapshot, ADR ratio, balances, currency, EV
yfmcp/valuation_history.py  worker/src/valuation-history.ts    point-in-time market cap, EV, LTM, price, share counts
yfmcp/metric_reconciliation.py  worker/src/metric-reconciliation.ts   source agreement, tolerance, parent net income

Inputs are the cards' worked examples, hand-built companyfacts / market dicts, and a few recorded fixtures
(fixtures/parity/sec, fixtures/parity/yahoo). Expected values come from the rule cards; where a card and the code
disagree the code wins and the case says so. "pinned" cases record behaviour an owner decision schedules for repair.
"""

from __future__ import annotations

import copy
import datetime as _dt
import re
from typing import Any

from scripts.parity.harness import Case, fixture
from yfmcp import metric_reconciliation as mr
from yfmcp import valuation as vl
from yfmcp import valuation_history as vh


# ───────────────────────────────────────────────────────────────── selection helpers

def _get(v: Any, path: str) -> Any:
    for seg in path.split("."):
        if v is None:
            return None
        if isinstance(v, list):
            i = int(seg)
            v = v[i] if i < len(v) else None
        elif isinstance(v, dict):
            v = v.get(seg)
        else:
            return None
    return v


def _sel(*paths: str, codes: str | None = "warnings", prefix: str = ""):
    """select: the listed dotted paths (optionally under `prefix`) plus the warning codes found at `codes`."""
    def pick(v):
        out = {p: _get(v, prefix + p) for p in paths}
        if codes:
            out["warningCodes"] = [w.get("code") for w in (_get(v, prefix + codes) or [])]
        return out
    return pick


def _psel(*paths: str, codes: str | None = "warnings"):
    """_sel on the first point of a historical valuation."""
    return _sel(*paths, codes=codes, prefix="points.0.")


# ───────────────────────────────────────────────────────────────── snapshot builders

def _mk(**over) -> dict:
    m = {"ticker": "TEST", "name": "Test Co", "currency": "USD", "financialCurrency": "USD", "price": 25.0, "priceTime": None,
         "sharesOutstanding": 100_000_000, "impliedSharesOutstanding": None, "marketCap": None, "totalCash": None, "totalDebt": None,
         "ttmRevenue": None, "ttmEbitda": None, "grossMarginPct": None, "trailingEps": None, "mostRecentQuarter": None, "estimates": []}
    m.update(over)
    return m


def _bridge(basic, diluted="same", filing="10-K", components=None, claims=None, dilution=0) -> dict:
    b: dict = {"status": "COMPUTED", "basicShares": {"shares": basic, "source": {"filingType": filing}}, "bridge": {"dilutionPctAtPrice": dilution}}
    if diluted == "same":
        b["bridge"]["dilutedSharesAtPrice"] = basic
    elif diluted is not None:
        b["bridge"]["dilutedSharesAtPrice"] = diluted
    if components is not None:
        b["components"] = components
    if claims is not None:
        b["unquantifiedShareClaims"] = claims
    return b


def _cap(cash, debt, sti=None, period_end=None, currency=None, warnings=None) -> dict:
    bal: dict = {"cashAndEquivalents": cash, "shortTermInvestments": sti, "totalDebt": debt}
    if currency:
        bal["currency"] = currency
    c: dict = {"basis": "COMPANY_DISCLOSED", "balances": bal}
    if period_end:
        c["periodEnd"] = period_end
    if warnings:
        c["warnings"] = warnings
    return c


def _conv(principal, incremental) -> dict:
    return {"component": "convertible_debt", "instruments": [{"principal": principal, "incrementalShares": incremental}]}


def _pref(shares=1_000_000, incremental=0, liquidation=None) -> dict:
    inst: dict = {"preferredSharesOutstanding": shares, "ifConvertedShares": 5_000_000, "incrementalShares": incremental}
    if liquidation is not None:
        inst["liquidationPreference"] = {"amount": liquidation, "basis": "per_share_tagged_x_shares_outstanding"}
    return {"component": "convertible_preferred", "instruments": [inst]}


def _snap(name: str, ids: list[str], market: dict, *, price=None, bridge=None, capital=None, secw=None, **kw) -> Case:
    secw = list(secw or [])
    return Case(
        name=name, ids=ids,
        py=lambda: vl.valuation_snapshot("TEST", copy.deepcopy(market), price, copy.deepcopy(bridge), copy.deepcopy(capital), list(secw)),
        ts={"module": "valuation", "fn": "valuationSnapshot",
            "args": [{"ticker": "TEST", "market": market, "suppliedPrice": price, "bridge": bridge, "capital": capital, "secWarnings": secw}]},
        **kw)


def _peer(name: str, ids: list[str], market: dict, **kw) -> Case:
    return Case(name=name, ids=ids, py=lambda: vl.peer_row(copy.deepcopy(market)),
                ts={"module": "valuation", "fn": "peerRow", "args": [market]}, **kw)


_SHARES = ("shares.basic", "shares.diluted", "shares.usedForValue", "shares.basis", "shares.secOrdinaryShares",
           "shares.ordinarySharesPerQuotedShare", "shares.dilutionPctAtPrice", "shares.secVsYahooSharesDiffPct")


def _mult(v: dict) -> list:
    return [[r["metric"], r["basis"], r["multiple"], r["note"]] for r in v["multiples"]]


# ───────────────────────────────────────────────────────────────── ADR ratio and share basis

ADS_PAIRS = [[25e6, 5e6], [25.3e6, 5e6], [25.5e6, 5e6], [25.6e6, 5e6], [9.5e6, 5e6], [10e6, 5e6], [10.8e6, 5e6], [10.08e6, 5e6],
             [5e6, 10e6], [4.9e6, 10e6], [5e6, 15e6], [12e6, 10e6], [0, 5e6], [5e6, 0], [-25e6, 5e6]]

# Yahoo's quoted-share count for an ADS-quoted 20-F filer; Yahoo balances in USD.
_ADR_MKT = _mk(price=100.0, totalCash=10_000_000_000, totalDebt=5_000_000_000)

ADR_CASES = [
    Case(name="val.adr.ads_ratio_function", ids=["BR-VAL-001", "BR-VAL-002", "BR-VAL-003", "BR-VAL-004"],
         py=lambda: [vl.ads_ratio(a, b) for a, b in ADS_PAIRS],
         ts={"js": "return args[0].map(([a, b]) => m.valuation.adsRatio(a, b));", "args": [ADS_PAIRS]},
         # 5x, 5.06x and 5.1x (within 2% of 5) -> 5; 5.12x, 1.9x (|1.9-2| = 0.1 > 0.04), 2.16x -> none; exactly 2x and 2.016x -> 2;
         # inverse 0.5 -> 1/2, 0.3333 -> 1/3, 0.49 (1/r = 2.04 is 2% of 2 away: 0.0408 > 0.04) -> none; 1.2x -> none; zero / negative -> none.
         expect=[5, 5, 5, None, None, 2, None, 2, 0.5, None, 1 / 3, None, None, None, None]),
    _snap("val.adr.20f_ratio_5_card_001", ["BR-VAL-001", "BR-VAL-004"], _mk(sharesOutstanding=5_186_076_000, price=100.0, totalCash=1, totalDebt=1),
          bridge=_bridge(25_930_380_000, 26_000_000_000, "20-F", dilution=0.27),
          select=_sel(*_SHARES),
          expect={"shares.basic": 5_186_076_000, "shares.diluted": 5_200_000_000, "shares.usedForValue": 5_200_000_000, "shares.basis": "sec_cover_page",
                  "shares.secOrdinaryShares": 25_930_380_000, "shares.ordinarySharesPerQuotedShare": 5, "shares.dilutionPctAtPrice": 0.27,
                  "shares.secVsYahooSharesDiffPct": 0, "warningCodes": ["ADR_RATIO_APPLIED", "YAHOO_BALANCES"]}),
    _snap("val.adr.40f_ratio_5_card_002", ["BR-VAL-002"], _mk(sharesOutstanding=5_186_400_000, price=100.0, totalCash=1, totalDebt=1),
          bridge=_bridge(25_932_000_000, 25_932_000_000, "40-F"),
          select=_sel("shares.basic", "shares.diluted", "shares.ordinarySharesPerQuotedShare", "shares.basis"),
          expect={"shares.basic": 5_186_400_000, "shares.diluted": 5_186_400_000, "shares.ordinarySharesPerQuotedShare": 5,
                  "shares.basis": "sec_cover_page", "warningCodes": ["ADR_RATIO_APPLIED", "YAHOO_BALANCES"]}),
    _snap("val.adr.20f_amendment_form_counts_as_foreign_card_003", ["BR-VAL-003"], _mk(sharesOutstanding=5_186_000_000, price=100.0, totalCash=1, totalDebt=1),
          bridge=_bridge(25_930_000_000, 25_930_000_000, "20-F/A"),
          select=_sel("shares.basic", "shares.ordinarySharesPerQuotedShare"),
          expect={"shares.basic": 5_186_000_000, "shares.ordinarySharesPerQuotedShare": 5, "warningCodes": ["ADR_RATIO_APPLIED", "YAHOO_BALANCES"]}),
    # 25.9B / 5.186B = 4.994: still ratio 5 (owner-corrected card BR-VAL-004); counts divide by 5.
    _snap("val.adr.ratio_4_994_rounds_to_5_card_004", ["BR-VAL-004"], _mk(sharesOutstanding=5_186_000_000, price=100.0, totalCash=1, totalDebt=1),
          bridge=_bridge(25_900_000_000, 25_900_000_000, "20-F"),
          select=_sel("shares.basic", "shares.ordinarySharesPerQuotedShare", "shares.secVsYahooSharesDiffPct"),
          expect={"shares.basic": 5_180_000_000, "shares.ordinarySharesPerQuotedShare": 5, "shares.secVsYahooSharesDiffPct": -0.12,
                  "warningCodes": ["ADR_RATIO_APPLIED", "YAHOO_BALANCES"]}),
    # Divided counts are rounded half-up to whole shares (card BR-VAL-004 edge case).
    _snap("val.adr.divided_counts_rounded_half_up", ["BR-VAL-004"], _mk(sharesOutstanding=5_186_400_000, price=100.0, totalCash=1, totalDebt=1),
          bridge=_bridge(25_932_000_002, 26_000_000_003, "20-F"),
          select=_sel("shares.basic", "shares.diluted"),
          expect={"shares.basic": 5_186_400_000, "shares.diluted": 5_200_000_001, "warningCodes": ["ADR_RATIO_APPLIED", "YAHOO_BALANCES"]}),
    # Inverse: Yahoo counts 3 per SEC share -> ratio 1/3, SEC counts are multiplied by 3 (card BR-VAL-004 edge case).
    _snap("val.adr.inverse_ratio_multiplies_sec_counts", ["BR-VAL-001", "BR-VAL-004"], _mk(sharesOutstanding=30_000_000, price=10.0, totalCash=1, totalDebt=1),
          bridge=_bridge(10_000_000, 10_500_000, "20-F"),
          select=_sel("shares.basic", "shares.diluted", "shares.ordinarySharesPerQuotedShare"),
          expect={"shares.basic": 30_000_000, "shares.diluted": 31_500_000, "shares.ordinarySharesPerQuotedShare": 1 / 3,
                  "warningCodes": ["ADR_RATIO_APPLIED", "YAHOO_BALANCES"]}),
    # The ratio is taken against the Yahoo count the engine uses: the implied all-class count when >2% above the listed one.
    _snap("val.adr.ratio_against_yahoo_implied_count", ["BR-VAL-004", "BR-VAL-086", "BR-VAL-087"],
          _mk(sharesOutstanding=5_000_000_000, impliedSharesOutstanding=5_186_000_000, price=100.0, totalCash=1, totalDebt=1),
          bridge=_bridge(25_930_000_000, 25_930_000_000, "20-F"),
          select=_sel("shares.basic", "shares.ordinarySharesPerQuotedShare", "shares.yahooSharesOutstanding", "shares.yahooImpliedSharesOutstanding"),
          expect={"shares.basic": 5_186_000_000, "shares.ordinarySharesPerQuotedShare": 5, "shares.yahooSharesOutstanding": 5_000_000_000,
                  "shares.yahooImpliedSharesOutstanding": 5_186_000_000, "warningCodes": ["ADR_RATIO_APPLIED", "YAHOO_BALANCES"]}),
    # A domestic (10-K) filer at exactly 2x is never read as an ADR: share-basis mismatch, Yahoo's count used.
    _snap("val.adr.domestic_exact_2x_is_not_an_adr", ["BR-VAL-001", "BR-VAL-002", "BR-VAL-003", "BR-VAL-004"],
          _mk(sharesOutstanding=20_000_000, totalCash=1, totalDebt=1), bridge=_bridge(40_000_000, 41_000_000, "10-K", dilution=2.5),
          select=_sel(*_SHARES),
          expect={"shares.basic": 20_000_000, "shares.diluted": None, "shares.usedForValue": 20_000_000, "shares.basis": "yahoo_shares_outstanding",
                  "shares.secOrdinaryShares": 40_000_000, "shares.ordinarySharesPerQuotedShare": None, "shares.dilutionPctAtPrice": None,
                  "shares.secVsYahooSharesDiffPct": 100, "warningCodes": ["SHARE_BASIS_MISMATCH", "YAHOO_BALANCES"]}),
    # No filing type on the share source: not a foreign filer, so no ADR ratio.
    _snap("val.adr.unknown_filing_type_is_not_an_adr", ["BR-VAL-004"], _mk(sharesOutstanding=5_186_000_000, totalCash=1, totalDebt=1),
          bridge={"status": "COMPUTED", "basicShares": {"shares": 25_930_000_000}, "bridge": {"dilutedSharesAtPrice": 25_930_000_000, "dilutionPctAtPrice": 0}},
          select=_sel("shares.basic", "shares.ordinarySharesPerQuotedShare", "shares.basis"),
          expect={"shares.basic": 5_186_000_000, "shares.ordinarySharesPerQuotedShare": None, "shares.basis": "yahoo_shares_outstanding",
                  "warningCodes": ["SHARE_BASIS_MISMATCH", "YAHOO_BALANCES"]}),
    # A foreign filer between 1.5x and 1.9x, or at 2.5x, has no whole multiple: mismatch.
    _snap("val.adr.foreign_without_whole_multiple_is_mismatch", ["BR-VAL-001", "BR-VAL-004"], _mk(sharesOutstanding=5_000_000, totalCash=1, totalDebt=1),
          bridge=_bridge(12_500_000, 12_500_000, "20-F"),
          select=_sel("shares.basic", "shares.ordinarySharesPerQuotedShare", "shares.basis"),
          expect={"shares.basic": 5_000_000, "shares.ordinarySharesPerQuotedShare": None, "shares.basis": "yahoo_shares_outstanding",
                  "warningCodes": ["SHARE_BASIS_MISMATCH", "YAHOO_BALANCES"]}),
    _snap("val.adr.foreign_1_6x_is_mismatch", ["BR-VAL-004"], _mk(sharesOutstanding=5_000_000, totalCash=1, totalDebt=1),
          bridge=_bridge(8_000_000, 8_000_000, "20-F"),
          select=_sel("shares.basic", "shares.ordinarySharesPerQuotedShare"),
          expect={"shares.basic": 5_000_000, "shares.ordinarySharesPerQuotedShare": None, "warningCodes": ["SHARE_BASIS_MISMATCH", "YAHOO_BALANCES"]}),
]

SHARE_BASIS_CASES = [
    _snap("val.share_basis.domestic_400m_vs_200m_card_035", ["BR-VAL-035"], _mk(sharesOutstanding=200_000_000, totalCash=1, totalDebt=1),
          bridge=_bridge(400_000_000, 410_000_000, "10-K", dilution=2.5), select=_sel(*_SHARES),
          expect={"shares.basic": 200_000_000, "shares.diluted": None, "shares.usedForValue": 200_000_000, "shares.basis": "yahoo_shares_outstanding",
                  "shares.secOrdinaryShares": 400_000_000, "shares.ordinarySharesPerQuotedShare": None, "shares.dilutionPctAtPrice": None,
                  "shares.secVsYahooSharesDiffPct": 100, "warningCodes": ["SHARE_BASIS_MISMATCH", "YAHOO_BALANCES"]}),
    _snap("val.share_basis.domestic_300m_vs_150m_card_036", ["BR-VAL-036"], _mk(sharesOutstanding=150_000_000, totalCash=1, totalDebt=1),
          bridge=_bridge(300_000_000, 310_000_000, "10-K", dilution=3.3), select=_sel(*_SHARES),
          expect={"shares.basic": 150_000_000, "shares.diluted": None, "shares.usedForValue": 150_000_000, "shares.basis": "yahoo_shares_outstanding",
                  "shares.secOrdinaryShares": 300_000_000, "shares.ordinarySharesPerQuotedShare": None, "shares.dilutionPctAtPrice": None,
                  "shares.secVsYahooSharesDiffPct": 100, "warningCodes": ["SHARE_BASIS_MISMATCH", "YAHOO_BALANCES"]}),
    # Ratio 1.4 keeps the SEC count, with no warning (card BR-VAL-036 edge case).
    _snap("val.share_basis.ratio_1_4_keeps_sec_count", ["BR-VAL-035", "BR-VAL-036"], _mk(sharesOutstanding=200_000_000, totalCash=1, totalDebt=1),
          bridge=_bridge(280_000_000, 290_000_000, "10-K", dilution=3.5), select=_sel(*_SHARES),
          expect={"shares.basic": 280_000_000, "shares.diluted": 290_000_000, "shares.usedForValue": 290_000_000, "shares.basis": "sec_cover_page",
                  "shares.secOrdinaryShares": 280_000_000, "shares.ordinarySharesPerQuotedShare": None, "shares.dilutionPctAtPrice": 3.5,
                  "shares.secVsYahooSharesDiffPct": 40, "warningCodes": ["YAHOO_BALANCES"]}),
    # Exactly 1.5x is not "more than 1.5x apart".
    _snap("val.share_basis.exactly_1_5x_keeps_sec_count", ["BR-VAL-035", "BR-VAL-036"], _mk(sharesOutstanding=200_000_000, totalCash=1, totalDebt=1),
          bridge=_bridge(300_000_000, 300_000_000, "10-K"), select=_sel("shares.basic", "shares.basis"),
          expect={"shares.basic": 300_000_000, "shares.basis": "sec_cover_page", "warningCodes": ["YAHOO_BALANCES"]}),
    # Just over 1.5x: mismatch.
    _snap("val.share_basis.just_over_1_5x_is_mismatch", ["BR-VAL-035", "BR-VAL-036"], _mk(sharesOutstanding=200_000_000, totalCash=1, totalDebt=1),
          bridge=_bridge(300_000_001, 300_000_001, "10-K"), select=_sel("shares.basic", "shares.basis"),
          expect={"shares.basic": 200_000_000, "shares.basis": "yahoo_shares_outstanding", "warningCodes": ["SHARE_BASIS_MISMATCH", "YAHOO_BALANCES"]}),
    # SEC count half of Yahoo's (domestic): below 1/1.5 -> mismatch (card BR-VAL-001 edge case).
    _snap("val.share_basis.sec_half_of_yahoo_is_mismatch", ["BR-VAL-001", "BR-VAL-035"], _mk(sharesOutstanding=200_000_000, totalCash=1, totalDebt=1),
          bridge=_bridge(100_000_000, 100_000_000, "10-K"), select=_sel("shares.basic", "shares.basis", "shares.secVsYahooSharesDiffPct"),
          expect={"shares.basic": 200_000_000, "shares.basis": "yahoo_shares_outstanding", "shares.secVsYahooSharesDiffPct": -50,
                  "warningCodes": ["SHARE_BASIS_MISMATCH", "YAHOO_BALANCES"]}),
    # Nothing to compare against: SEC count stays usable.
    _snap("val.share_basis.no_yahoo_count_keeps_sec_count", ["BR-VAL-035"], _mk(sharesOutstanding=None, totalCash=1, totalDebt=1),
          bridge=_bridge(400_000_000, 410_000_000, "10-K"), select=_sel("shares.basic", "shares.diluted", "shares.basis"),
          expect={"shares.basic": 400_000_000, "shares.diluted": 410_000_000, "shares.basis": "sec_cover_page", "warningCodes": ["YAHOO_BALANCES"]}),
    # When the SEC count is discarded: no unquantified-claims warning, no preferred liquidation preference in EV (card BR-VAL-035 edge case).
    _snap("val.share_basis.mismatch_drops_preferred_and_claims", ["BR-VAL-035"], _mk(sharesOutstanding=200_000_000, totalCash=1, totalDebt=1),
          bridge=_bridge(400_000_000, 410_000_000, "10-K", components=[_pref(liquidation=100_000_000)],
                         claims=[{"kind": "PRICE_PROTECTION", "status": "UNQUANTIFIED"}]),
          capital=_cap(140_000_000, 380_000_000),
          select=_sel("balances.preferredLiquidationPreferenceInEv", "enterpriseValue", "shares.unquantifiedShareClaims"),
          expect={"balances.preferredLiquidationPreferenceInEv": 0, "enterpriseValue": 25 * 200_000_000 + 380_000_000 - 140_000_000,
                  "shares.unquantifiedShareClaims": ["PRICE_PROTECTION"], "warningCodes": ["SHARE_BASIS_MISMATCH"]}),
    # Yahoo's implied all-class count replaces the listed class only when more than 2% larger.
    _peer("val.share_basis.implied_exactly_2pct_not_used", ["BR-VAL-086", "BR-VAL-087", "BR-VAL-088", "BR-VAL-089"],
          _mk(sharesOutstanding=100_000_000, impliedSharesOutstanding=102_000_000, totalCash=0, totalDebt=0),
          select=_sel("sharesUsed", "shareBasis", codes=None), expect={"sharesUsed": 100_000_000, "shareBasis": "yahoo_shares_outstanding"}),
    _peer("val.share_basis.implied_over_2pct_used", ["BR-VAL-086", "BR-VAL-087", "BR-VAL-088", "BR-VAL-089"],
          _mk(sharesOutstanding=100_000_000, impliedSharesOutstanding=102_000_001, totalCash=0, totalDebt=0),
          select=_sel("sharesUsed", "shareBasis", codes=None), expect={"sharesUsed": 102_000_001, "shareBasis": "yahoo_implied_shares_outstanding"}),
]


# ───────────────────────────────────────────────────────────────── balances: filing vs Yahoo

# Quote and financials in USD; Yahoo has cash and debt and a latest quarter end to compare with the filing's period end.
_BAL_MKT = _mk(price=25.0, sharesOutstanding=101_000_000, totalCash=190_000_000, totalDebt=400_000_000, mostRecentQuarter="2025-06-30")
_BAL_SEL = _sel("balances.basis", "balances.cash", "balances.totalDebt", "balances.periodEnd", "balances.shortTermInvestments", "enterpriseValue")
_YAHOO_USED = {"balances.basis": "yahoo_financial_data", "balances.cash": 190_000_000, "balances.totalDebt": 400_000_000,
               "balances.periodEnd": None, "balances.shortTermInvestments": None, "enterpriseValue": 25 * 101_000_000 + 400_000_000 - 190_000_000}
_SEC_USED = {"balances.basis": "sec_filing_period_end", "balances.cash": 140_000_000, "balances.totalDebt": 380_000_000, "balances.shortTermInvestments": 0}


def _bal_expect(used: dict, codes: list, **over) -> dict:
    return {**used, **over, "warningCodes": codes}


BALANCE_CASES = [
    # Card BR-VAL-005 / BR-VAL-033: 2024-12-31 vs Yahoo 2025-06-30 = 181 days > 45, Yahoo has cash and debt.
    _snap("val.balances.stale_181_days_yahoo_used_card_005_033", ["BR-VAL-005", "BR-VAL-033", "BR-VAL-070", "BR-VAL-081"], _BAL_MKT,
          capital=_cap(140_000_000, 380_000_000, period_end="2024-12-31", currency="USD"), select=_BAL_SEL,
          expect=_bal_expect(_YAHOO_USED, ["SEC_BALANCES_STALE", "YAHOO_BALANCES"])),
    # Card BR-VAL-032 / BR-VAL-034: 2025-12-31 vs 2026-03-31 = 90 days.
    _snap("val.balances.stale_90_days_yahoo_used_card_032_034", ["BR-VAL-032", "BR-VAL-034"],
          {**_BAL_MKT, "mostRecentQuarter": "2026-03-31"}, capital=_cap(140_000_000, 380_000_000, period_end="2025-12-31", currency="USD"),
          select=_sel("balances.basis", "balances.cash", "balances.totalDebt", "enterpriseValue"),
          expect={"balances.basis": "yahoo_financial_data", "balances.cash": 190_000_000, "balances.totalDebt": 400_000_000,
                  "enterpriseValue": 25 * 101_000_000 + 400_000_000 - 190_000_000, "warningCodes": ["SEC_BALANCES_STALE", "YAHOO_BALANCES"]}),
    # 45 days is the limit: 2025-05-16 -> 2025-06-30 keeps the filing; 46 days gives way.
    _snap("val.balances.exactly_45_days_keeps_filing", ["BR-VAL-032", "BR-VAL-034", "BR-VAL-070"], _BAL_MKT,
          capital=_cap(140_000_000, 380_000_000, period_end="2025-05-16", currency="USD"), select=_BAL_SEL,
          expect=_bal_expect({**_SEC_USED, "balances.periodEnd": "2025-05-16", "enterpriseValue": 25 * 101_000_000 + 380_000_000 - 140_000_000}, [])),
    _snap("val.balances.46_days_gives_way_to_yahoo", ["BR-VAL-032", "BR-VAL-034", "BR-VAL-070"], _BAL_MKT,
          capital=_cap(140_000_000, 380_000_000, period_end="2025-05-15", currency="USD"), select=_BAL_SEL,
          expect=_bal_expect(_YAHOO_USED, ["SEC_BALANCES_STALE", "YAHOO_BALANCES"])),
    # Stale but Yahoo lacks cash or debt: the older filing balances are kept, with a warning.
    _snap("val.balances.stale_but_yahoo_cash_missing_keeps_filing", ["BR-VAL-005", "BR-VAL-032", "BR-VAL-033", "BR-VAL-034"],
          {**_BAL_MKT, "totalCash": None}, capital=_cap(140_000_000, 380_000_000, period_end="2024-12-31", currency="USD"),
          select=_sel("balances.basis", "balances.cash", "balances.totalDebt", "balances.periodEnd", "enterpriseValue"),
          expect={"balances.basis": "sec_filing_period_end", "balances.cash": 140_000_000, "balances.totalDebt": 380_000_000, "balances.periodEnd": "2024-12-31",
                  "enterpriseValue": 25 * 101_000_000 + 380_000_000 - 140_000_000, "warningCodes": ["SEC_BALANCES_STALE"]}),
    _snap("val.balances.stale_but_yahoo_debt_missing_keeps_filing", ["BR-VAL-005", "BR-VAL-032", "BR-VAL-033", "BR-VAL-034"],
          {**_BAL_MKT, "totalDebt": None}, capital=_cap(140_000_000, 380_000_000, period_end="2024-12-31", currency="USD"),
          select=_sel("balances.basis", "balances.periodEnd", "enterpriseValue"),
          expect={"balances.basis": "sec_filing_period_end", "balances.periodEnd": "2024-12-31",
                  "enterpriseValue": 25 * 101_000_000 + 380_000_000 - 140_000_000, "warningCodes": ["SEC_BALANCES_STALE"]}),
    # The filing's balances need both a numeric debt and a numeric cash.
    _snap("val.balances.filing_without_debt_not_used", ["BR-VAL-005", "BR-VAL-033", "BR-VAL-034"], {**_BAL_MKT, "mostRecentQuarter": None},
          capital=_cap(140_000_000, None, period_end="2025-06-30"), select=_BAL_SEL, expect=_bal_expect(_YAHOO_USED, ["YAHOO_BALANCES"])),
    _snap("val.balances.filing_without_cash_not_used", ["BR-VAL-005", "BR-VAL-033", "BR-VAL-034"], {**_BAL_MKT, "mostRecentQuarter": None},
          capital=_cap(None, 380_000_000, period_end="2025-06-30"), select=_BAL_SEL, expect=_bal_expect(_YAHOO_USED, ["YAHOO_BALANCES"])),
    # Another currency than the quote: not used, with an info warning; checked before staleness.
    _snap("val.balances.filing_in_other_currency_not_used", ["BR-VAL-005", "BR-VAL-031", "BR-VAL-032", "BR-VAL-033", "BR-VAL-034"],
          {**_BAL_MKT, "mostRecentQuarter": None}, capital=_cap(100_000_000, 50_000_000, period_end="2025-06-30", currency="EUR"),
          select=_BAL_SEL, expect=_bal_expect(_YAHOO_USED, ["SEC_BALANCES_CURRENCY", "YAHOO_BALANCES"])),
    # A pence quote is compared in its major currency: balances in GBP are used.
    _snap("val.balances.pence_quote_uses_gbp_filing", ["BR-VAL-023", "BR-VAL-031", "BR-VAL-032", "BR-VAL-033"],
          _mk(currency="GBp", financialCurrency="GBP", price=250.0, sharesOutstanding=100_000_000, totalCash=1, totalDebt=1),
          capital=_cap(30_000_000, 60_000_000, currency="GBP"),
          select=_sel("balances.basis", "price.majorUnitAmount", "price.majorCurrency", "enterpriseValue"),
          expect={"balances.basis": "sec_filing_period_end", "price.majorUnitAmount": 2.5, "price.majorCurrency": "GBP",
                  "enterpriseValue": 250_000_000 + 60_000_000 - 30_000_000, "warningCodes": []}),
    # Capital-structure warnings travel with the filing's balances only (cards BR-VAL-005, 033, 034 edge case).
    _snap("val.balances.capital_warnings_travel_with_filing_balances", ["BR-VAL-005", "BR-VAL-033", "BR-VAL-034"], {**_BAL_MKT, "mostRecentQuarter": None},
          capital=_cap(140_000_000, 0, period_end="2025-06-30", warnings=[{"code": "NO_BORROWINGS_TAGGED", "message": "m", "severity": "info"}]),
          select=_sel("warnings", codes=None),
          expect={"warnings": [{"code": "NO_BORROWINGS_TAGGED", "message": "m", "severity": "info", "source": "extract_capital_structure"}]}),
    _snap("val.balances.capital_warnings_dropped_with_yahoo_balances", ["BR-VAL-033", "BR-VAL-034"],
          _BAL_MKT, capital=_cap(140_000_000, 0, period_end="2024-12-31", warnings=[{"code": "NO_BORROWINGS_TAGGED", "message": "m", "severity": "info"}]),
          select=_sel("balances.basis"), expect={"balances.basis": "yahoo_financial_data", "warningCodes": ["SEC_BALANCES_STALE", "YAHOO_BALANCES"]}),

    # ── cash cross-checks (cards BR-VAL-032, 038, 039): warn only, never change the value ──
    _snap("val.cash_check.filing_cash_matches_yahoo_but_investments_on_top", ["BR-VAL-032", "BR-VAL-038", "BR-VAL-039", "BR-VAL-072", "BR-VAL-073"],
          _mk(price=25.0, sharesOutstanding=101_000_000, totalCash=190_000_000, totalDebt=400_000_000),
          capital=_cap(190_000_000, 380_000_000, sti=150_000_000, period_end="2025-03-31"),
          select=lambda v: {"warning": next((w for w in v["warnings"] if w["code"] == "SEC_YAHOO_CASH_MISMATCH"), None), "ev": v["enterpriseValue"]},
          expect={"warning": {"code": "SEC_YAHOO_CASH_MISMATCH", "severity": "warning", "secCash": 190_000_000, "secShortTermInvestments": 150_000_000,
                              "yahooTotalCash": 190_000_000,
                              "message": "The filing's cash alone matches Yahoo's total cash and short-term investments, but cash plus the filing's short-term investments is well above it; the investments may already be inside cash. Check the filing before relying on enterprise value."},
                  "ev": 25 * 101_000_000 + 380_000_000 - 190_000_000 - 150_000_000}),
    # Investments of 15M take cash plus investments to 205M, under 110% of 190M: no warning; 25M -> 215M: warning.
    _snap("val.cash_check.investments_under_10pct_excess_no_warning", ["BR-VAL-032", "BR-VAL-038"],
          _mk(sharesOutstanding=101_000_000, totalCash=190_000_000, totalDebt=400_000_000),
          capital=_cap(190_000_000, 380_000_000, sti=15_000_000), select=_sel(), expect={"warningCodes": []}),
    _snap("val.cash_check.investments_over_10pct_excess_warning", ["BR-VAL-032", "BR-VAL-038"],
          _mk(sharesOutstanding=101_000_000, totalCash=190_000_000, totalDebt=400_000_000),
          capital=_cap(190_000_000, 380_000_000, sti=25_000_000), select=_sel(), expect={"warningCodes": ["SEC_YAHOO_CASH_MISMATCH"]}),
    # Cash 185M is 2.6% from Yahoo's 190M: not "alone within 2%", so no mismatch warning; 187M (1.6%) is.
    _snap("val.cash_check.cash_2_6pct_from_yahoo_no_warning", ["BR-VAL-032", "BR-VAL-038"],
          _mk(sharesOutstanding=101_000_000, totalCash=190_000_000, totalDebt=400_000_000),
          capital=_cap(185_000_000, 380_000_000, sti=150_000_000), select=_sel(), expect={"warningCodes": []}),
    _snap("val.cash_check.cash_1_6pct_from_yahoo_warning", ["BR-VAL-032", "BR-VAL-038"],
          _mk(sharesOutstanding=101_000_000, totalCash=190_000_000, totalDebt=400_000_000),
          capital=_cap(187_000_000, 380_000_000, sti=150_000_000), select=_sel(), expect={"warningCodes": ["SEC_YAHOO_CASH_MISMATCH"]}),
    # Shortfall: same quarter (within 7 days), Yahoo cash more than 5% above the filing's cash plus investments.
    _snap("val.cash_check.yahoo_cash_above_filing_same_quarter", ["BR-VAL-032", "BR-VAL-038", "BR-VAL-039", "BR-VAL-072", "BR-VAL-073"],
          _mk(sharesOutstanding=101_000_000, totalCash=190_000_000, totalDebt=400_000_000, mostRecentQuarter="2025-03-31"),
          capital=_cap(100_000_000, 380_000_000, period_end="2025-03-31", currency="USD"),
          select=lambda v: {"warning": next((w for w in v["warnings"] if w["code"] == "SEC_YAHOO_CASH_SHORTFALL"), None), "basis": v["balances"]["basis"]},
          expect={"warning": {"code": "SEC_YAHOO_CASH_SHORTFALL", "severity": "warning", "secCash": 100_000_000, "secShortTermInvestments": 0,
                              "yahooTotalCash": 190_000_000,
                              "message": "Yahoo's cash and short-term investments are more than 5% above the filing's cash plus the investments read from it; an investment line may be tagged under a concept not read. Check the balance sheet before relying on enterprise value."},
                  "basis": "sec_filing_period_end"}),
    _snap("val.cash_check.shortfall_quarter_ends_7_days_apart_warns", ["BR-VAL-032", "BR-VAL-038"],
          _mk(sharesOutstanding=101_000_000, totalCash=190_000_000, totalDebt=400_000_000, mostRecentQuarter="2025-04-07"),
          capital=_cap(100_000_000, 380_000_000, period_end="2025-03-31"), select=_sel(), expect={"warningCodes": ["SEC_YAHOO_CASH_SHORTFALL"]}),
    _snap("val.cash_check.shortfall_quarter_ends_8_days_apart_silent", ["BR-VAL-032", "BR-VAL-038"],
          _mk(sharesOutstanding=101_000_000, totalCash=190_000_000, totalDebt=400_000_000, mostRecentQuarter="2025-04-08"),
          capital=_cap(100_000_000, 380_000_000, period_end="2025-03-31"), select=_sel(), expect={"warningCodes": []}),
    # 104M is within 5% of the filing's 100M; 106M is not.
    _snap("val.cash_check.shortfall_4pct_silent", ["BR-VAL-032", "BR-VAL-038"],
          _mk(sharesOutstanding=101_000_000, totalCash=104_000_000, totalDebt=400_000_000, mostRecentQuarter="2025-03-31"),
          capital=_cap(100_000_000, 380_000_000, period_end="2025-03-31"), select=_sel(), expect={"warningCodes": []}),
    _snap("val.cash_check.shortfall_6pct_warns", ["BR-VAL-032", "BR-VAL-038"],
          _mk(sharesOutstanding=101_000_000, totalCash=106_000_000, totalDebt=400_000_000, mostRecentQuarter="2025-03-31"),
          capital=_cap(100_000_000, 380_000_000, period_end="2025-03-31"), select=_sel(), expect={"warningCodes": ["SEC_YAHOO_CASH_SHORTFALL"]}),
]


# ───────────────────────────────────────────────────────────────── minor units and currency comparability

_MULTI_SEL = lambda v: {"majorUnitAmount": v["price"]["majorUnitAmount"], "majorCurrency": v["price"]["majorCurrency"], "ev": v["enterpriseValue"],
                        "status": v["status"], "multiples": _mult(v), "codes": [w["code"] for w in v["warnings"]]}
_NOT_COMPARABLE_NOTE = "Financials are reported in a different currency from the quote; not computed."
_PEER_SEL = _sel("price", "currency", "marketCap", "enterpriseValue", "evToRevenueTtm", "comparableCurrency", codes=None)

CURRENCY_CASES = [
    # Card BR-VAL-023: 1,250 GBp with GBP financials -> GBP 12.50, comparable.
    _snap("val.currency.gbp_pence_1250_card_023", ["BR-VAL-023", "BR-VAL-031"],
          _mk(currency="GBp", financialCurrency="GBP", price=1250.0, sharesOutstanding=100_000_000, totalCash=30_000_000, totalDebt=60_000_000, ttmRevenue=110_000_000),
          select=lambda v: {"major": v["price"]["majorUnitAmount"], "majorCurrency": v["price"]["majorCurrency"], "ev": v["enterpriseValue"],
                            "evToRevenue": _mult(v)[0][2], "peerEv": v["peerComparableBasis"]["enterpriseValue"]},
          expect={"major": 12.5, "majorCurrency": "GBP", "ev": 1_280_000_000, "evToRevenue": 11.64, "peerEv": 1_280_000_000}),
    # Card BR-VAL-024: 250 GBp, EBITDA -10M: EV/EBITDA null on a non-positive denominator; the others compute.
    _snap("val.currency.gbp_pence_negative_ebitda_card_024", ["BR-VAL-024", "BR-VAL-055", "BR-VAL-075", "BR-VAL-079", "BR-VAL-080"],
          _mk(currency="GBp", financialCurrency="GBP", price=250.0, sharesOutstanding=100_000_000, totalCash=30_000_000, totalDebt=60_000_000,
              ttmRevenue=110_000_000, ttmEbitda=-10_000_000, trailingEps=0.1),
          select=lambda v: {"major": v["price"]["majorUnitAmount"], "multiples": _mult(v)},
          expect={"major": 2.5, "multiples": [
              ["EV/Revenue", "trailing_12_months", 2.55, None], ["EV/Revenue", "current_fiscal_year", None, "Denominator unavailable."],
              ["EV/Revenue", "next_fiscal_year", None, "Denominator unavailable."],
              ["EV/EBITDA", "trailing_12_months", None, "Denominator is zero or negative; a multiple is not meaningful."],
              ["P/E", "trailing_12_months", 25, None], ["P/E", "current_fiscal_year", None, "Denominator unavailable."],
              ["P/E", "next_fiscal_year", None, "Denominator unavailable."]]}),
    # Card BR-VAL-024: with financials in USD against a GBp quote every multiple is null with the currency note.
    _snap("val.currency.gbp_quote_usd_financials_all_multiples_null_card_024", ["BR-VAL-024", "BR-VAL-031", "BR-VAL-055", "BR-VAL-080"],
          _mk(currency="GBp", financialCurrency="USD", price=250.0, sharesOutstanding=100_000_000, totalCash=30_000_000, totalDebt=60_000_000,
              ttmRevenue=110_000_000, ttmEbitda=20_000_000, trailingEps=0.1),
          select=_MULTI_SEL,
          expect={"majorUnitAmount": 2.5, "majorCurrency": "GBP", "ev": None, "status": "PARTIAL",
                  "multiples": [[m, b, None, _NOT_COMPARABLE_NOTE] for m, b in [("EV/Revenue", "trailing_12_months"), ("EV/Revenue", "current_fiscal_year"),
                                                                                  ("EV/Revenue", "next_fiscal_year"), ("EV/EBITDA", "trailing_12_months"),
                                                                                  ("P/E", "trailing_12_months"), ("P/E", "current_fiscal_year"), ("P/E", "next_fiscal_year")]],
                  "codes": ["YAHOO_BALANCES", "ENTERPRISE_VALUE_CURRENCY_MISMATCH", "FINANCIAL_CURRENCY_MISMATCH"]}),
    # USD quote on TWD financials (TSM-like): EV null, both warnings.
    _snap("val.currency.usd_quote_twd_financials_ev_null_card_031", ["BR-VAL-031", "BR-VAL-023"],
          _mk(currency="USD", financialCurrency="TWD", price=450.0, sharesOutstanding=5_000_000, totalCash=3_500_000_000, totalDebt=1_000_000_000, ttmRevenue=4_000_000_000),
          select=_MULTI_SEL,
          expect={"majorUnitAmount": 450, "majorCurrency": "USD", "ev": None, "status": "PARTIAL",
                  "multiples": [[m, b, None, _NOT_COMPARABLE_NOTE] for m, b in [("EV/Revenue", "trailing_12_months"), ("EV/Revenue", "current_fiscal_year"),
                                                                                  ("EV/Revenue", "next_fiscal_year"), ("EV/EBITDA", "trailing_12_months"),
                                                                                  ("P/E", "trailing_12_months"), ("P/E", "current_fiscal_year"), ("P/E", "next_fiscal_year")]],
                  "codes": ["YAHOO_BALANCES", "ENTERPRISE_VALUE_CURRENCY_MISMATCH", "FINANCIAL_CURRENCY_MISMATCH"]}),
    # Card BR-VAL-031: SEC balances in the quote currency still give an EV against Yahoo's other-currency financials;
    # the Yahoo-denominated multiples stay "not computed".
    _snap("val.currency.sec_balances_in_quote_currency_give_ev_but_no_multiples_card_031", ["BR-VAL-031"],
          _mk(currency="USD", financialCurrency="TWD", price=450.0, sharesOutstanding=5_000_000, totalCash=3_500_000_000, totalDebt=1_000_000_000, ttmRevenue=4_000_000_000),
          capital=_cap(100_000_000, 50_000_000, currency="USD"),
          select=lambda v: {"ev": v["enterpriseValue"], "status": v["status"], "evToRevenue": _mult(v)[0], "basis": v["balances"]["basis"],
                            "codes": [w["code"] for w in v["warnings"]]},
          expect={"ev": 450 * 5_000_000 + 50_000_000 - 100_000_000, "status": "COMPUTED", "basis": "sec_filing_period_end",
                  "evToRevenue": ["EV/Revenue", "trailing_12_months", None, _NOT_COMPARABLE_NOTE], "codes": ["FINANCIAL_CURRENCY_MISMATCH"]}),
    # Unknown currency on either side is treated as comparable.
    _snap("val.currency.unknown_financial_currency_is_comparable", ["BR-VAL-023", "BR-VAL-024", "BR-VAL-031"],
          _mk(currency="GBp", financialCurrency=None, price=250.0, sharesOutstanding=100_000_000, totalCash=30_000_000, totalDebt=60_000_000, ttmRevenue=110_000_000),
          select=lambda v: {"ev": v["enterpriseValue"], "evToRevenue": _mult(v)[0][2]}, expect={"ev": 280_000_000, "evToRevenue": 2.55}),
    _snap("val.currency.unknown_quote_currency_is_comparable", ["BR-VAL-023", "BR-VAL-024", "BR-VAL-031"],
          _mk(currency=None, financialCurrency="GBP", price=2.5, sharesOutstanding=100_000_000, totalCash=30_000_000, totalDebt=60_000_000, ttmRevenue=110_000_000),
          select=lambda v: {"ev": v["enterpriseValue"], "major": v["price"]["majorUnitAmount"], "majorCurrency": v["price"]["majorCurrency"]},
          expect={"ev": 280_000_000, "major": 2.5, "majorCurrency": None}),
    # Other minor-unit quotes on the peer basis: GBX, ZAc, ILA.
    _peer("val.currency.gbx_peer_row", ["BR-VAL-023", "BR-VAL-031"],
          _mk(currency="GBX", financialCurrency="GBP", price=250.0, sharesOutstanding=100_000_000, totalCash=30_000_000, totalDebt=60_000_000, ttmRevenue=110_000_000),
          select=_PEER_SEL, expect={"price": 250, "currency": "GBX", "marketCap": 250_000_000, "enterpriseValue": 280_000_000, "evToRevenueTtm": 2.55,
                                    "comparableCurrency": True}),
    _peer("val.currency.zac_peer_row", ["BR-VAL-023", "BR-VAL-031"],
          _mk(currency="ZAc", financialCurrency="ZAR", price=5000.0, sharesOutstanding=10_000_000, totalCash=100_000_000, totalDebt=200_000_000, ttmRevenue=500_000_000),
          select=_PEER_SEL, expect={"price": 5000, "currency": "ZAc", "marketCap": 500_000_000, "enterpriseValue": 600_000_000, "evToRevenueTtm": 1.2,
                                    "comparableCurrency": True}),
    _peer("val.currency.ila_peer_row", ["BR-VAL-023", "BR-VAL-031"],
          _mk(currency="ILA", financialCurrency="ILS", price=3000.0, sharesOutstanding=10_000_000, totalCash=100_000_000, totalDebt=200_000_000, ttmRevenue=500_000_000),
          select=_PEER_SEL, expect={"price": 3000, "currency": "ILA", "marketCap": 300_000_000, "enterpriseValue": 400_000_000, "evToRevenueTtm": 0.8,
                                    "comparableCurrency": True}),
    # A minor-unit quote whose financials are in a different major currency is not comparable.
    _peer("val.currency.zac_quote_usd_financials_not_comparable", ["BR-VAL-023", "BR-VAL-031"],
          _mk(currency="ZAc", financialCurrency="USD", price=5000.0, sharesOutstanding=10_000_000, totalCash=100_000_000, totalDebt=200_000_000, ttmRevenue=500_000_000),
          select=_PEER_SEL, expect={"price": 5000, "currency": "ZAc", "marketCap": 500_000_000, "enterpriseValue": None, "evToRevenueTtm": None,
                                    "comparableCurrency": False}),
    _peer("val.currency.peer_row_usd_vs_twd_not_comparable", ["BR-VAL-031", "BR-VAL-023"],
          _mk(currency="USD", financialCurrency="TWD", price=10.0, sharesOutstanding=50_000_000, totalCash=900_000_000, totalDebt=100_000_000, ttmRevenue=3_000_000_000),
          select=_PEER_SEL, expect={"price": 10, "currency": "USD", "marketCap": 500_000_000, "enterpriseValue": None, "evToRevenueTtm": None,
                                    "comparableCurrency": False}),
    _snap("val.snapshot.no_price_is_unavailable", ["BR-VAL-084", "BR-VAL-082"], _mk(price=None, totalCash=1, totalDebt=1),
          select=lambda v: {"status": v["status"], "code": v["code"], "warnings": v["warnings"]},
          expect={"status": "PRICE_UNAVAILABLE", "code": "PRICE_UNAVAILABLE", "warnings": []}),
    _snap("val.snapshot.supplied_price_overrides_yahoo", ["BR-VAL-084"], _mk(price=25.0, priceTime="2026-10-01T00:00:00.000Z", totalCash=0, totalDebt=0),
          price=40.0, select=_sel("price.amount", "price.source", "price.asOf", "equityValue", codes=None),
          expect={"price.amount": 40, "price.source": "caller_supplied", "price.asOf": None, "equityValue": 4_000_000_000}),
]


# ───────────────────────────────────────────────────────────────── enterprise value and convertible debt (Q3i)

_EV_SEL = _sel("equityValue", "enterpriseValue", "status", "balances.convertibleDebtCountedAsShares", "balances.preferredLiquidationPreferenceInEv", "shares.usedForValue")
_SAME_SIZE_MKT = dict(sharesOutstanding=100_000_000)

EV_CASES = [
    # Card BR-VAL-046: price 25, diluted 108M, debt 500M, in-the-money convertible 300M, cash 400M, STI 100M -> 2,400M.
    _snap("val.ev.card_046_convertible_counted_as_shares", ["BR-VAL-046", "BR-VAL-045", "BR-VAL-083"], _mk(**_SAME_SIZE_MKT),
          bridge=_bridge(100_000_000, 108_000_000, components=[_conv(300_000_000, 12_000_000)]),
          capital=_cap(400_000_000, 500_000_000, sti=100_000_000), select=_EV_SEL,
          expect={"equityValue": 2_700_000_000, "enterpriseValue": 2_400_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 300_000_000,
                  "balances.preferredLiquidationPreferenceInEv": 0, "shares.usedForValue": 108_000_000, "warningCodes": []}),
    # Card BR-VAL-074: out-of-the-money convertible preferred joins EV at its $50M liquidation preference -> 1,950M.
    _snap("val.ev.card_074_convertible_and_preferred", ["BR-VAL-074", "BR-VAL-045", "BR-VAL-083"], _mk(price=20.0, **_SAME_SIZE_MKT),
          bridge=_bridge(100_000_000, 100_000_000, components=[_conv(300_000_000, 15_000_000), _pref(liquidation=50_000_000)]),
          capital=_cap(200_000_000, 500_000_000, sti=100_000_000), select=_EV_SEL,
          expect={"equityValue": 2_000_000_000, "enterpriseValue": 1_950_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 300_000_000,
                  "balances.preferredLiquidationPreferenceInEv": 50_000_000, "shares.usedForValue": 100_000_000, "warningCodes": []}),
    # Card BR-VAL-082: price 20, diluted 110M, debt 600M with a 300M convertible, cash 200M, STI 100M -> 2,200M.
    _snap("val.ev.card_082_status_computed", ["BR-VAL-082", "BR-VAL-046", "BR-VAL-083"], _mk(price=20.0, **_SAME_SIZE_MKT),
          bridge=_bridge(100_000_000, 110_000_000, components=[_conv(300_000_000, 15_000_000)]),
          capital=_cap(200_000_000, 600_000_000, sti=100_000_000), select=_EV_SEL,
          expect={"equityValue": 2_200_000_000, "enterpriseValue": 2_200_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 300_000_000,
                  "balances.preferredLiquidationPreferenceInEv": 0, "shares.usedForValue": 110_000_000, "warningCodes": []}),
    # The convertible adjustment is capped at total debt.
    _snap("val.ev.convertible_adjustment_capped_at_debt", ["BR-VAL-046", "BR-VAL-074", "BR-VAL-082"], _mk(price=10.0, **_SAME_SIZE_MKT),
          bridge=_bridge(100_000_000, 100_000_000, components=[_conv(700_000_000, 20_000_000)]), capital=_cap(100_000_000, 500_000_000), select=_EV_SEL,
          expect={"equityValue": 1_000_000_000, "enterpriseValue": 900_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 500_000_000,
                  "balances.preferredLiquidationPreferenceInEv": 0, "shares.usedForValue": 100_000_000, "warningCodes": []}),
    # Notes with no incremental shares at the price stay in debt (the card BRIDGE example: 300M counted, 200M not).
    _snap("val.ev.out_of_money_convertible_stays_in_debt", ["BR-VAL-046", "BR-VAL-082"], _mk(price=10.0, **_SAME_SIZE_MKT),
          bridge=_bridge(100_000_000, 100_000_000, components=[{"component": "convertible_debt", "instruments": [
              {"principal": 300_000_000, "incrementalShares": 15_000_000}, {"principal": 200_000_000, "incrementalShares": 0}]}]),
          capital=_cap(100_000_000, 500_000_000), select=_EV_SEL,
          expect={"equityValue": 1_000_000_000, "enterpriseValue": 1_100_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 300_000_000,
                  "balances.preferredLiquidationPreferenceInEv": 0, "shares.usedForValue": 100_000_000, "warningCodes": []}),
    # Preferred without a tagged preference is left out and said so; preferred that converts at the price adds nothing.
    _snap("val.ev.preferred_without_liquidation_preference_warns", ["BR-VAL-046", "BR-VAL-074", "BR-VAL-082"], _mk(price=10.0, **_SAME_SIZE_MKT),
          bridge=_bridge(100_000_000, 100_000_000, components=[_pref(shares=1_000_000)]), capital=_cap(100_000_000, 500_000_000), select=_EV_SEL,
          expect={"equityValue": 1_000_000_000, "enterpriseValue": 1_400_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 0,
                  "balances.preferredLiquidationPreferenceInEv": 0, "shares.usedForValue": 100_000_000, "warningCodes": ["PREFERRED_NOT_IN_ENTERPRISE_VALUE"]}),
    _snap("val.ev.preferred_converting_at_price_adds_nothing", ["BR-VAL-046", "BR-VAL-074"], _mk(price=10.0, **_SAME_SIZE_MKT),
          bridge=_bridge(100_000_000, 105_000_000, components=[_pref(incremental=5_000_000, liquidation=100_000_000)]), capital=_cap(100_000_000, 500_000_000),
          select=_EV_SEL, expect={"equityValue": 1_050_000_000, "enterpriseValue": 1_450_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 0,
                                  "balances.preferredLiquidationPreferenceInEv": 0, "shares.usedForValue": 105_000_000, "warningCodes": []}),
    # Missing shares, cash or debt: PARTIAL with ENTERPRISE_VALUE_INCOMPLETE.
    _snap("val.ev.incomplete_without_yahoo_cash", ["BR-VAL-046", "BR-VAL-074", "BR-VAL-082"], _mk(totalCash=None, totalDebt=400_000_000),
          select=_sel("status", "enterpriseValue", "equityValue"),
          expect={"status": "PARTIAL", "enterpriseValue": None, "equityValue": 2_500_000_000, "warningCodes": ["YAHOO_BALANCES", "ENTERPRISE_VALUE_INCOMPLETE"]}),
    _snap("val.ev.incomplete_without_any_share_count", ["BR-VAL-046", "BR-VAL-082"], _mk(sharesOutstanding=None, totalCash=1, totalDebt=1),
          select=_sel("status", "enterpriseValue", "equityValue", "shares.basis"),
          expect={"status": "PARTIAL", "enterpriseValue": None, "equityValue": None, "shares.basis": None,
                  "warningCodes": ["YAHOO_BALANCES", "ENTERPRISE_VALUE_INCOMPLETE"]}),

    # ── Q3i (3.2.1): the convertible principal leaves debt only when the bridge's diluted count is the share count used ──
    # SHARE_BASIS_MISMATCH puts Yahoo's 200M basic shares in the equity value, so the 300M convertible stays in debt:
    # EV = 25 x 200M + 380M - 140M = 5,240M. Before, the principal left debt too (4,940M) and the convertible was in neither.
    _snap("val.ev.q3i_convertible_stays_in_debt_when_sec_share_count_rejected", ["BR-VAL-046", "BR-VAL-074", "BR-VAL-082"],
          _mk(sharesOutstanding=200_000_000), bridge=_bridge(400_000_000, 410_000_000, "10-K", components=[_conv(300_000_000, 15_000_000)]),
          capital=_cap(140_000_000, 380_000_000, period_end="2025-03-31"), select=_EV_SEL,
          expect={"equityValue": 5_000_000_000, "enterpriseValue": 5_240_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 0,
                  "balances.preferredLiquidationPreferenceInEv": 0, "shares.usedForValue": 200_000_000, "warningCodes": ["SHARE_BASIS_MISMATCH"]}),
    # A usable SEC count but no diluted count in the bridge: equity uses the basic count, so the principal stays in debt
    # (20 x 100M + 500M - 200M - 100M = 2,200M; before 1,900M).
    _snap("val.ev.q3i_convertible_stays_in_debt_when_bridge_has_no_diluted_count", ["BR-VAL-046", "BR-VAL-074", "BR-VAL-082"],
          _mk(price=20.0, **_SAME_SIZE_MKT), bridge=_bridge(100_000_000, None, components=[_conv(300_000_000, 15_000_000)]),
          capital=_cap(200_000_000, 500_000_000, sti=100_000_000), select=_EV_SEL,
          expect={"equityValue": 2_000_000_000, "enterpriseValue": 2_200_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 0,
                  "balances.preferredLiquidationPreferenceInEv": 0, "shares.usedForValue": 100_000_000, "warningCodes": []}),
]



# ═══════════════════════════════════════════════════════════════════ historical valuation (valuation_history)

def _f(start, end, val, form, filed, accn) -> dict:
    row = {"end": end, "val": val, "form": form, "filed": filed, "accn": accn}
    if start is not None:
        row["start"] = start
    return row


def _usd(*rows) -> dict:
    return {"units": {"USD": list(rows)}}


K24 = ("10-K", "2025-02-15", "k24")
Q125 = ("10-Q", "2025-05-05", "q125")
Q225 = ("10-Q", "2025-08-05", "q225")
M = 1_000_000

# FY2024 1,000M; six months 2025 600M; six months 2024 450M: LTM to 2025-06-30 = 1,150M (cards BR-VAL-007, 014).
_STD_REV = (_f("2024-01-01", "2024-12-31", 1000 * M, *K24), _f("2025-01-01", "2025-06-30", 600 * M, *Q225), _f("2024-01-01", "2024-06-30", 450 * M, *Q225))
_COVER = lambda val=100 * M, end="2025-07-30", src=Q225: [_f(None, end, val, *src)]  # noqa: E731


def _facts(us=None, dei=None, ifrs=None) -> dict:
    facts: dict = {}
    if dei is not None:
        facts["dei"] = {"EntityCommonStockSharesOutstanding": {"units": {"shares": dei}}}
    if us is not None:
        facts["us-gaap"] = us
    if ifrs is not None:
        facts["ifrs-full"] = ifrs
    return {"facts": facts}


def _us(rev=None, **concepts) -> dict:
    out = {"Revenues": _usd(*(_STD_REV if rev is None else rev))}
    for name, rows in concepts.items():
        out[name] = _usd(*rows)
    return out


def _inp(facts, *, date="2025-09-02", close=20.0, bars=None, splits=None, currency="USD", fx=None, ads=None, dates=None, **extra) -> dict:
    return {"ticker": "tst", "dates": dates or [date], "companyfacts": facts, "bars": bars if bars is not None else [{"date": date, "close": close}],
            "priceCurrency": currency, "splits": splits or [], "fx": fx, "adsRatio": ads, **extra}


def _hv(name: str, ids: list[str], inp: dict, **kw) -> Case:
    return Case(name=name, ids=ids, py=lambda: vh.historical_valuation(copy.deepcopy(inp)),
                ts={"module": "valuationHistory", "fn": "historicalValuation", "args": [inp]}, **kw)


def _scene(*, close=20.0, shares=100 * M, cash=100 * M, sti=50 * M, debt=300 * M, ni=None, **kw) -> dict:
    """Balances at 2025-06-30 from the 10-Q, a cover-page count, standard revenue, priced on 2025-09-02."""
    us = _us(CashAndCashEquivalentsAtCarryingValue=[_f(None, "2025-06-30", cash, *Q225)],
             ShortTermInvestments=[_f(None, "2025-06-30", sti, *Q225)], LongTermDebt=[_f(None, "2025-06-30", debt, *Q225)])
    if ni:
        us["NetIncomeLoss"] = _usd(_f("2024-01-01", "2024-12-31", ni[0], *K24), _f("2025-01-01", "2025-06-30", ni[1], *Q225), _f("2024-01-01", "2024-06-30", ni[2], *Q225))
    return _inp(_facts(us, _COVER(shares)), close=close, **kw)


_CORE = _psel("marketCap.status", "marketCap.value", "enterpriseValue.status", "enterpriseValue.value", "multiples.LTM.evToRevenue.status",
              "multiples.LTM.evToRevenue.value", "multiples.LTM.priceToSales.value", "multiples.LTM.priceToEarnings.status", "multiples.LTM.priceToEarnings.value",
              "denominators.LTM.revenue.value", "denominators.LTM.revenue.method")


def _core(mcap, ev, ev_rev, p_s, rev=1150 * M, **over) -> dict:
    e = {"marketCap.status": "OK", "marketCap.value": mcap, "enterpriseValue.status": "OK", "enterpriseValue.value": ev,
         "multiples.LTM.evToRevenue.status": "OK", "multiples.LTM.evToRevenue.value": ev_rev, "multiples.LTM.priceToSales.value": p_s,
         "multiples.LTM.priceToEarnings.status": None, "multiples.LTM.priceToEarnings.value": None,
         "denominators.LTM.revenue.value": rev, "denominators.LTM.revenue.method": "LFY_PLUS_YTD_MINUS_PRIOR_YTD", "warningCodes": []}
    e.update(over)
    return e


EV_CASES_HIST = [
    # Card BR-VAL-007: market cap 2,000M, debt 300M, cash 100M, STI 50M, LTM revenue 1,150M -> EV 2,150M, EV/Revenue 1.87.
    _hv("val.hist.ev.card_007_ev_and_multiples", ["BR-VAL-007", "BR-VAL-010", "BR-VAL-014", "BR-VAL-049"], _scene(), select=_CORE,
        expect=_core(2000 * M, 2150 * M, 1.87, 1.74, **{"multiples.LTM.priceToEarnings.status": "DENOMINATOR_NOT_AVAILABLE"})),
    # Card BR-VAL-010: price 120, 50M shares, debt 800M, cash 300M, STI 100M -> market cap 6,000M, EV 6,400M, EV/Revenue 5.57.
    _hv("val.hist.ev.card_010_price_120", ["BR-VAL-010", "BR-VAL-007"], _scene(close=120.0, shares=50 * M, cash=300 * M, sti=100 * M, debt=800 * M, ni=(100 * M, 70 * M, 40 * M)),
        select=_CORE, expect=_core(6000 * M, 6400 * M, 5.57, 5.22, **{"multiples.LTM.priceToEarnings.status": "OK", "multiples.LTM.priceToEarnings.value": 46.15})),
    # Negative EV (net cash above market cap): the EV is shown, its multiples are not meaningful.
    _hv("val.hist.ev.negative_ev_multiples_not_meaningful", ["BR-VAL-007", "BR-VAL-010"], _scene(close=1.0, cash=5000 * M),
        select=_psel("marketCap.value", "enterpriseValue.status", "enterpriseValue.value", "multiples.LTM.evToRevenue.status", "multiples.LTM.evToRevenue.value",
                     "multiples.LTM.priceToSales.value"),
        expect={"marketCap.value": 100 * M, "enterpriseValue.status": "OK", "enterpriseValue.value": 100 * M + (300 - 5000 - 50) * M,
                "multiples.LTM.evToRevenue.status": "NOT_MEANINGFUL_NONPOSITIVE_NUMERATOR", "multiples.LTM.evToRevenue.value": None,
                "multiples.LTM.priceToSales.value": 0.09, "warningCodes": []}),
    # A net loss: P/E has a non-positive denominator.
    _hv("val.hist.ev.net_loss_pe_not_meaningful", ["BR-VAL-010"], _scene(ni=(-100 * M, -70 * M, -40 * M)),
        select=_psel("multiples.LTM.priceToEarnings.status", "multiples.LTM.priceToEarnings.value", "multiples.LFY.priceToEarnings.status",
                     "denominators.LTM.netIncome.value"),
        expect={"multiples.LTM.priceToEarnings.status": "NOT_MEANINGFUL_NONPOSITIVE_DENOMINATOR", "multiples.LTM.priceToEarnings.value": None,
                "multiples.LFY.priceToEarnings.status": "NOT_MEANINGFUL_NONPOSITIVE_DENOMINATOR", "denominators.LTM.netIncome.value": -130 * M, "warningCodes": []}),
    # No FX within 7 days on or before the date: figures in the reporting currency are not converted, multiples null.
    _hv("val.hist.ev.fx_8_days_old_not_available", ["BR-VAL-007", "BR-VAL-008"],
        {**_scene(close=2000.0, currency="GBp"), "fx": {"pair": "USDGBP=X", "bars": [{"date": "2025-08-25", "close": 0.8}]}},
        select=_psel("enterpriseValue.status", "enterpriseValue.value", "fx.status", "multiples.LTM.evToRevenue.status", "multiples.LTM.priceToSales.status", "price.close",
                     "price.currency", "marketCap.value"),
        expect={"enterpriseValue.status": "FX_NOT_AVAILABLE", "enterpriseValue.value": None, "fx.status": "NOT_AVAILABLE",
                "multiples.LTM.evToRevenue.status": "ENTERPRISEVALUE_NOT_AVAILABLE", "multiples.LTM.priceToSales.status": "FX_NOT_AVAILABLE", "price.close": 20,
                "price.currency": "GBP", "marketCap.value": 2000 * M, "warningCodes": ["FX_NOT_AVAILABLE"]}),
    _hv("val.hist.ev.fx_7_days_old_is_used", ["BR-VAL-007", "BR-VAL-008"],
        {**_scene(close=2000.0, currency="GBp"), "fx": {"pair": "USDGBP=X", "bars": [{"date": "2025-08-26", "close": 0.8}]}},
        select=_psel("enterpriseValue.status", "enterpriseValue.value", "fx.rate", "fx.date", "multiples.LTM.evToRevenue.value", "multiples.LTM.priceToSales.value"),
        expect={"enterpriseValue.status": "OK", "enterpriseValue.value": 2000 * M + round((300 - 100 - 50) * M * 0.8), "fx.rate": 0.8, "fx.date": "2025-08-26",
                "multiples.LTM.evToRevenue.value": 2.3, "multiples.LTM.priceToSales.value": 2.17, "warningCodes": []}),
]


def _bsel(v: dict) -> dict:
    p = v["points"][0]
    b = p["balances"]
    debt = b.get("debt") or {}
    return {"balances.status": b["status"], "balances.reason": b.get("reason"), "balanceDate": b.get("balanceDate"), "debt.status": debt.get("status"),
            "debt.value": debt.get("value"), "debt.basis": debt.get("basis"), "debt.concepts": [c["concept"] for c in debt.get("components") or []],
            "sti.status": (b.get("shortTermInvestments") or {}).get("status"), "marketCap.status": p["marketCap"]["status"],
            "ev.status": p["enterpriseValue"]["status"], "ev.value": p["enterpriseValue"]["value"], "warningCodes": [w["code"] for w in p["warnings"]]}


def _cash(v=300 * M, end="2025-06-30", src=Q225):
    return [_f(None, end, v, *src)]


BAL_HIST_CASES = [
    # Card BR-VAL-006: LongTermDebt 500M + ShortTermBorrowings 50M, cash 300M, STI untagged, market cap 2,000M -> EV 2,250M.
    _hv("val.hist.balances.card_006_debt_plus_short_term_borrowings", ["BR-VAL-006", "BR-VAL-008", "BR-VAL-010"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebt=[_f(None, "2025-06-30", 500 * M, *Q225)],
                        ShortTermBorrowings=[_f(None, "2025-06-30", 50 * M, *Q225)]), _COVER())), select=_bsel,
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "OK", "debt.value": 550 * M, "debt.basis": None,
                "debt.concepts": ["LongTermDebt", "ShortTermBorrowings"], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "OK",
                "ev.value": 2250 * M, "warningCodes": ["SHORT_TERM_INVESTMENTS_NOT_TAGGED"]}),
    # Card BR-VAL-008: no borrowing concept anywhere in the filing that reported the cash -> debt 0, EV = 2,000M - 300M.
    _hv("val.hist.balances.card_008_no_borrowings_means_zero_debt", ["BR-VAL-006", "BR-VAL-008", "BR-VAL-010", "BR-VAL-047"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash()), _COVER())), select=_bsel,
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "OK", "debt.value": 0, "debt.basis": "NO_BORROWINGS_TAGGED",
                "debt.concepts": [], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "OK", "ev.value": 1700 * M,
                "warningCodes": ["NO_BORROWINGS_TAGGED", "SHORT_TERM_INVESTMENTS_NOT_TAGGED"]}),
    # 3.4.0 (Q1 BR-VAL-006, owner 2026-10-30 "Withhold the figure"): only LongTermDebtCurrent (30M) is tagged at 2025-06-30;
    # the same 10-Q tags LongTermDebtNoncurrent (280M) only at the prior year end, so the group is incomplete at the balance
    # date. Debt is INCOMPLETE and EV is DEBT_INCOMPLETE. Before, debt read 30M and EV was understated by the missing 280M.
    _hv("val.hist.balances.partial_debt_group_withholds_debt_and_ev", ["BR-VAL-006", "BR-VAL-008"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebtCurrent=[_f(None, "2025-06-30", 30 * M, *Q225)],
                        LongTermDebtNoncurrent=[_f(None, "2024-12-31", 280 * M, *Q225)]), _COVER())), select=_bsel,
        decision="Q1 BR-VAL-006, owner 2026-10-30",
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "INCOMPLETE", "debt.value": None, "debt.basis": None,
                "debt.concepts": ["LongTermDebtCurrent"], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "DEBT_INCOMPLETE",
                "ev.value": None, "warningCodes": []}),
    # A component the cash filing never tags (here LongTermDebtNoncurrent appears nowhere) does not make the group incomplete.
    _hv("val.hist.balances.group_component_never_tagged_by_the_filing_is_complete", ["BR-VAL-006"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebtCurrent=[_f(None, "2025-06-30", 30 * M, *Q225)]), _COVER())),
        select=_bsel, decision="Q1 BR-VAL-006, owner 2026-10-30",
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "OK", "debt.value": 30 * M, "debt.basis": None,
                "debt.concepts": ["LongTermDebtCurrent"], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "OK",
                "ev.value": 2000 * M + (30 - 300) * M, "warningCodes": ["SHORT_TERM_INVESTMENTS_NOT_TAGGED"]}),
    # A component tagged only by another filing (the 10-K) is not this balance sheet's: the group stays complete.
    _hv("val.hist.balances.group_component_tagged_only_by_another_filing_is_complete", ["BR-VAL-006"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebtCurrent=[_f(None, "2025-06-30", 30 * M, *Q225)],
                        LongTermDebtNoncurrent=[_f(None, "2024-12-31", 280 * M, *K24)]), _COVER())),
        select=_bsel, decision="Q1 BR-VAL-006, owner 2026-10-30",
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "OK", "debt.value": 30 * M, "debt.basis": None,
                "debt.concepts": ["LongTermDebtCurrent"], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "OK",
                "ev.value": 2000 * M + (30 - 300) * M, "warningCodes": ["SHORT_TERM_INVESTMENTS_NOT_TAGGED"]}),
    # The first group with any tagged component wins; later groups are not added.
    _hv("val.hist.balances.first_matching_group_wins", ["BR-VAL-006"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebt=[_f(None, "2025-06-30", 100 * M, *Q225)],
                        LongTermDebtCurrent=[_f(None, "2025-06-30", 30 * M, *Q225)], LongTermDebtNoncurrent=[_f(None, "2025-06-30", 280 * M, *Q225)]), _COVER())),
        select=_bsel, expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "OK", "debt.value": 100 * M, "debt.basis": None,
                              "debt.concepts": ["LongTermDebt"], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "OK", "ev.value": 1800 * M,
                              "warningCodes": ["SHORT_TERM_INVESTMENTS_NOT_TAGGED"]}),
    # Group 2 sums both of its components when both are tagged, plus commercial paper.
    _hv("val.hist.balances.group_two_sums_components_and_commercial_paper", ["BR-VAL-006"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebtCurrent=[_f(None, "2025-06-30", 30 * M, *Q225)],
                        LongTermDebtNoncurrent=[_f(None, "2025-06-30", 280 * M, *Q225)], CommercialPaper=[_f(None, "2025-06-30", 5 * M, *Q225)]), _COVER())),
        select=_bsel, expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "OK", "debt.value": 315 * M, "debt.basis": None,
                              "debt.concepts": ["LongTermDebtCurrent", "LongTermDebtNoncurrent", "CommercialPaper"], "sti.status": "NOT_TAGGED",
                              "marketCap.status": "OK", "ev.status": "OK", "ev.value": 2000 * M + (315 - 300) * M, "warningCodes": ["SHORT_TERM_INVESTMENTS_NOT_TAGGED"]}),
    # Short-term borrowings are added only once a group has matched; alone they leave debt untagged (and block the zero-debt rule).
    _hv("val.hist.balances.short_term_borrowings_alone_do_not_make_debt", ["BR-VAL-006", "BR-VAL-008"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), ShortTermBorrowings=[_f(None, "2025-06-30", 50 * M, *Q225)]), _COVER())), select=_bsel,
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "NOT_TAGGED", "debt.value": None, "debt.basis": None,
                "debt.concepts": [], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "DEBT_NOT_TAGGED", "ev.value": None, "warningCodes": []}),
    # Nothing is carried forward: debt tagged only at the prior year end (a comparative in the same 10-Q) is not the balance-date debt.
    _hv("val.hist.balances.debt_at_earlier_date_not_carried_forward", ["BR-VAL-006", "BR-VAL-008", "BR-VAL-047"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebt=[_f(None, "2024-12-31", 500 * M, *Q225)]), _COVER())), select=_bsel,
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "NOT_TAGGED", "debt.value": None, "debt.basis": None,
                "debt.concepts": [], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "DEBT_NOT_TAGGED", "ev.value": None, "warningCodes": []}),
    _hv("val.hist.balances.no_cash_tagged_balances_not_available", ["BR-VAL-006", "BR-VAL-008"],
        _inp(_facts(_us(LongTermDebt=[_f(None, "2025-06-30", 500 * M, *Q225)]), _COVER())), select=_bsel,
        expect={"balances.status": "NOT_AVAILABLE", "balances.reason": "CASH_NOT_TAGGED", "balanceDate": None, "debt.status": None, "debt.value": None, "debt.basis": None,
                "debt.concepts": [], "sti.status": None, "marketCap.status": "OK", "ev.status": "BALANCES_NOT_AVAILABLE", "ev.value": None, "warningCodes": []}),
    # The zero-debt rule needs the cash fact's accession.
    _hv("val.hist.balances.cash_without_accession_blocks_zero_debt", ["BR-VAL-006", "BR-VAL-008", "BR-VAL-047"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=[_f(None, "2025-06-30", 300 * M, "10-Q", "2025-08-05", None)]), _COVER())), select=_bsel,
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "NOT_TAGGED", "debt.value": None, "debt.basis": None,
                "debt.concepts": [], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "DEBT_NOT_TAGGED", "ev.value": None, "warningCodes": []}),
    # The first tagged short-term investment concept is used.
    _hv("val.hist.balances.first_tagged_short_term_investment_concept", ["BR-VAL-006", "BR-VAL-010"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebt=[_f(None, "2025-06-30", 300 * M, *Q225)],
                        ShortTermInvestments=[_f(None, "2025-06-30", 20 * M, *Q225)], MarketableSecuritiesCurrent=[_f(None, "2025-06-30", 40 * M, *Q225)]), _COVER())),
        select=_bsel, expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "OK", "debt.value": 300 * M, "debt.basis": None,
                              "debt.concepts": ["LongTermDebt"], "sti.status": "OK", "marketCap.status": "OK", "ev.status": "OK", "ev.value": 2000 * M + (300 - 300 - 20) * M,
                              "warningCodes": []}),
    # IFRS has no borrowing-concept mapping: the zero-debt rule never applies, debt stays untagged; short-term investments are not mapped.
    _hv("val.hist.balances.ifrs_never_zero_debt", ["BR-VAL-006", "BR-VAL-008", "BR-VAL-047"],
        _inp(_facts(None, [_f(None, "2024-12-31", 500, "20-F", "2025-04-15", "f24")], ifrs={
            "Revenue": {"units": {"TWD": [_f("2024-01-01", "2024-12-31", 20000, "20-F", "2025-04-15", "f24")]}},
            "CashAndCashEquivalents": {"units": {"TWD": [_f(None, "2024-12-31", 3000, "20-F", "2025-04-15", "f24")]}}}),
             date="2025-06-02", close=10.0, currency="TWD"), select=_bsel,
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2024-12-31", "debt.status": "NOT_TAGGED", "debt.value": None, "debt.basis": None,
                "debt.concepts": [], "sti.status": "NOT_MAPPED", "marketCap.status": "OK", "ev.status": "DEBT_NOT_TAGGED", "ev.value": None, "warningCodes": []}),

    # Finding (Phase 2, for Q3i triage): the zero-debt rule looks only at the accession that reported the cash. Debt tagged in the 10-K (k24,
    # 500M at 2024-12-31) but not in the 10-Q that reported the later cash balance (q225) reads as "no borrowings": debt 0, EV 1,700M. Both runtimes
    # agree; the SYN fixture of scripts/test_valuation_history_and_reconcile.py shows the same shape when a debt line is tagged at a different date.
    _hv("val.hist.balances.finding_debt_only_in_older_filing_reads_as_zero_debt", ["BR-VAL-006", "BR-VAL-008", "BR-VAL-047"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebt=[_f(None, "2024-12-31", 500 * M, *K24)]), _COVER())), select=_bsel,
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "OK", "debt.value": 0, "debt.basis": "NO_BORROWINGS_TAGGED",
                "debt.concepts": [], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "OK", "ev.value": 1700 * M,
                "warningCodes": ["NO_BORROWINGS_TAGGED", "SHORT_TERM_INVESTMENTS_NOT_TAGGED"]}),

    # ── EV status ladder: MARKET_CAP_NOT_AVAILABLE > BALANCES_NOT_AVAILABLE > DEBT_NOT_TAGGED > FX_NOT_AVAILABLE > BALANCES_STALE > OK ──
    _hv("val.hist.ev_status.market_cap_unavailable_beats_missing_balances", ["BR-VAL-008", "BR-VAL-009"],
        _inp(_facts(_us(), None)), select=_bsel,
        expect={"balances.status": "NOT_AVAILABLE", "balances.reason": "CASH_NOT_TAGGED", "balanceDate": None, "debt.status": None, "debt.value": None, "debt.basis": None,
                "debt.concepts": [], "sti.status": None, "marketCap.status": "SHARES_NOT_AVAILABLE", "ev.status": "MARKET_CAP_NOT_AVAILABLE", "ev.value": None,
                "warningCodes": []}),
    _hv("val.hist.ev_status.debt_not_tagged_beats_fx_not_available", ["BR-VAL-008"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebt=[_f(None, "2024-12-31", 500 * M, *Q225)]), _COVER()), close=2000.0, currency="GBp"),
        select=_bsel, expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2025-06-30", "debt.status": "NOT_TAGGED", "debt.value": None,
                              "debt.basis": None, "debt.concepts": [], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "DEBT_NOT_TAGGED",
                              "ev.value": None, "warningCodes": ["FX_NOT_AVAILABLE"]}),
    # Balances at 2024-12-31 are 245 days old on 2025-09-02: past 200, but FX is checked first.
    _hv("val.hist.ev_status.fx_not_available_beats_balances_stale", ["BR-VAL-008"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(300 * M, "2024-12-31", K24), LongTermDebt=[_f(None, "2024-12-31", 500 * M, *K24)]), _COVER()),
             close=2000.0, currency="GBp"),
        select=_bsel, expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2024-12-31", "debt.status": "OK", "debt.value": 500 * M,
                              "debt.basis": None, "debt.concepts": ["LongTermDebt"], "sti.status": "NOT_TAGGED", "marketCap.status": "OK",
                              "ev.status": "FX_NOT_AVAILABLE", "ev.value": None, "warningCodes": ["FX_NOT_AVAILABLE"]}),
    _hv("val.hist.ev_status.balances_stale_after_200_days", ["BR-VAL-008", "BR-VAL-050", "BR-VAL-053", "BR-VAL-076", "BR-VAL-077"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(300 * M, "2024-12-31", K24), LongTermDebt=[_f(None, "2024-12-31", 500 * M, *K24)]), _COVER())),
        select=_bsel, expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2024-12-31", "debt.status": "OK", "debt.value": 500 * M,
                              "debt.basis": None, "debt.concepts": ["LongTermDebt"], "sti.status": "NOT_TAGGED", "marketCap.status": "OK",
                              "ev.status": "BALANCES_STALE", "ev.value": None, "warningCodes": ["SHORT_TERM_INVESTMENTS_NOT_TAGGED", "BALANCES_STALE"]}),
    # 200 days exactly (2025-03-31 -> 2025-10-17) is not stale; 201 is.
    _hv("val.hist.ev_status.balances_200_days_old_is_fresh", ["BR-VAL-008", "BR-VAL-050", "BR-VAL-053"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(300 * M, "2025-03-31", Q125), LongTermDebt=[_f(None, "2025-03-31", 500 * M, *Q125)]), _COVER()),
             date="2025-10-17"), select=_psel("enterpriseValue.status", "enterpriseValue.value", "balances.balanceAgeDays", "balances.stale"),
        expect={"enterpriseValue.status": "OK", "enterpriseValue.value": 2000 * M + 200 * M, "balances.balanceAgeDays": 200, "balances.stale": False,
                "warningCodes": ["SHORT_TERM_INVESTMENTS_NOT_TAGGED"]}),
    _hv("val.hist.ev_status.balances_201_days_old_is_stale", ["BR-VAL-008", "BR-VAL-050", "BR-VAL-053"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(300 * M, "2025-03-31", Q125), LongTermDebt=[_f(None, "2025-03-31", 500 * M, *Q125)]), _COVER()),
             date="2025-10-18"), select=_psel("enterpriseValue.status", "enterpriseValue.value", "balances.balanceAgeDays", "balances.stale"),
        expect={"enterpriseValue.status": "BALANCES_STALE", "enterpriseValue.value": None, "balances.balanceAgeDays": 201, "balances.stale": True,
                "warningCodes": ["SHORT_TERM_INVESTMENTS_NOT_TAGGED", "BALANCES_STALE"]}),
]


# ───────────────────────────────────────────────────────────────── price, share count and market cap (cards 009, 011, 012, 013, 028, 030)

def _days_before(date: str, n: int) -> str:
    return (_dt.date.fromisoformat(date) - _dt.timedelta(days=n)).isoformat()


def _days_after(date: str, n: int) -> str:
    return (_dt.date.fromisoformat(date) + _dt.timedelta(days=n)).isoformat()


_PRICE_SEL = _psel("price.tradingDate", "price.closeAsAdjusted", "price.laterSplitFactor", "price.close", "price.currency", codes=None)
_MCAP_SEL = _psel("marketCap.status", "marketCap.value", "shares.basis", "shares.value", "shares.asOf", "shares.ageDays", "enterpriseValue.status")
_STD_FACTS = _facts(_us(), _COVER())


def _price(name, ids, bars, splits, date, expect, currency="USD", **kw) -> Case:
    return _hv(name, ids, _inp(_STD_FACTS, date=date, bars=bars, splits=splits, currency=currency), select=_PRICE_SEL, expect=expect, **kw)


PRICE_CASES = [
    # Card BR-VAL-012: 50 adjusted on 2023-06-30, 4-for-1 on 2024-07-01 -> 200.
    _price("val.hist.price.card_012_later_split_undone", ["BR-VAL-012", "BR-VAL-013", "BR-VAL-028"], [{"date": "2023-06-30", "close": 50.0}],
           [{"date": "2024-07-01", "ratio": 4.0}], "2023-06-30",
           {"price.tradingDate": "2023-06-30", "price.closeAsAdjusted": 50, "price.laterSplitFactor": 4, "price.close": 200, "price.currency": "USD"}),
    # Card BR-VAL-013: 50 on 2020-06-30, 4:1 on 2022-01-15 -> 200.0000.
    _price("val.hist.price.card_013_four_to_one_2022", ["BR-VAL-013", "BR-VAL-012"], [{"date": "2020-06-30", "close": 50.0}],
           [{"date": "2022-01-15", "ratio": 4.0}], "2020-06-30",
           {"price.tradingDate": "2020-06-30", "price.closeAsAdjusted": 50, "price.laterSplitFactor": 4, "price.close": 200, "price.currency": "USD"}),
    # Card BR-VAL-028: 30 on 2023-06-30, 4-for-1 on 2024-06-10 -> 120.
    _price("val.hist.price.card_028_30_times_4", ["BR-VAL-028", "BR-VAL-012"], [{"date": "2023-06-30", "close": 30.0}], [{"date": "2024-06-10", "ratio": 4.0}], "2023-06-30",
           {"price.tradingDate": "2023-06-30", "price.closeAsAdjusted": 30, "price.laterSplitFactor": 4, "price.close": 120, "price.currency": "USD"}),
    # Every later split multiplies; a split on the bar date or before, and a non-positive ratio, do not.
    _price("val.hist.price.later_splits_multiply_earlier_and_invalid_ignored", ["BR-VAL-012", "BR-VAL-013", "BR-VAL-028"], [{"date": "2023-06-30", "close": 10.0}],
           [{"date": "2022-01-01", "ratio": 5.0}, {"date": "2023-06-30", "ratio": 7.0}, {"date": "2023-12-01", "ratio": 2.0}, {"date": "2024-03-01", "ratio": 3.0},
            {"date": "2024-04-01", "ratio": 0.0}, {"date": "2024-05-01", "ratio": -2.0}], "2023-06-30",
           {"price.tradingDate": "2023-06-30", "price.closeAsAdjusted": 10, "price.laterSplitFactor": 6, "price.close": 60, "price.currency": "USD"}),
    # The close is rounded to 4 decimals after the split factor.
    _price("val.hist.price.rounded_to_four_decimals", ["BR-VAL-012", "BR-VAL-013"], [{"date": "2023-06-30", "close": 12.345678}], [], "2023-06-30",
           {"price.tradingDate": "2023-06-30", "price.closeAsAdjusted": 12.345678, "price.laterSplitFactor": 1, "price.close": 12.3457, "price.currency": "USD"}),
    _price("val.hist.price.split_product_rounded_to_four_decimals", ["BR-VAL-012", "BR-VAL-013"], [{"date": "2023-06-30", "close": 33.333333}],
           [{"date": "2024-01-01", "ratio": 3.0}], "2023-06-30",
           {"price.tradingDate": "2023-06-30", "price.closeAsAdjusted": 33.333333, "price.laterSplitFactor": 3, "price.close": 100, "price.currency": "USD"}),
    # Minor-unit quote: 1234.5678 GBp -> GBP 12.3457.
    _price("val.hist.price.pence_quote_converted_to_pounds", ["BR-VAL-012", "BR-VAL-024"], [{"date": "2023-06-30", "close": 1234.5678}], [], "2023-06-30",
           {"price.tradingDate": "2023-06-30", "price.closeAsAdjusted": 1234.5678, "price.laterSplitFactor": 1, "price.close": 12.3457, "price.currency": "GBP"}, currency="GBp"),
    # The latest bar on or before the date, up to 7 days back; a bar after the date is ignored, and the bar on the date wins.
    _price("val.hist.price.latest_bar_on_or_before_date", ["BR-VAL-012", "BR-VAL-013"],
           [{"date": "2023-06-22", "close": 1.0}, {"date": "2023-06-28", "close": 2.0}, {"date": "2023-06-30", "close": 3.0}, {"date": "2023-07-03", "close": 4.0}], [], "2023-06-30",
           {"price.tradingDate": "2023-06-30", "price.closeAsAdjusted": 3, "price.laterSplitFactor": 1, "price.close": 3, "price.currency": "USD"}),
    _price("val.hist.price.bar_7_days_back_is_used", ["BR-VAL-012", "BR-VAL-013"], [{"date": "2023-06-23", "close": 2.0}, {"date": "2023-07-03", "close": 4.0}], [], "2023-06-30",
           {"price.tradingDate": "2023-06-23", "price.closeAsAdjusted": 2, "price.laterSplitFactor": 1, "price.close": 2, "price.currency": "USD"}),
    _hv("val.hist.price.bar_8_days_back_is_price_unavailable", ["BR-VAL-012", "BR-VAL-013", "BR-VAL-028"],
        _inp(_STD_FACTS, date="2023-06-30", bars=[{"date": "2023-06-22", "close": 2.0}]), select=_psel("status", "coreStatus", "marketCap", "enterpriseValue"),
        expect={"status": "PRICE_UNAVAILABLE", "coreStatus": "PRICE_UNAVAILABLE", "marketCap": None, "enterpriseValue": None, "warningCodes": ["PRICE_UNAVAILABLE"]}),
]


def _sh(name, ids, rows, *, date="2025-09-02", expect, close=20.0, splits=None, ads=None, us=None, **kw) -> Case:
    facts = _facts(us if us is not None else _us(), rows)
    return _hv(name, ids, _inp(facts, date=date, close=close, splits=splits, ads=ads, **kw.pop("inp", {})), select=kw.pop("select", _MCAP_SEL), expect=expect, **kw)


MCAP_CASES = [
    # Card BR-VAL-009: close 10 adjusted for a later 4-for-1 split = 40; count 50,000,000 as of 90 days earlier -> market cap 2,000M.
    _sh("val.hist.market_cap.card_009_unadjusted_close_times_cover_count", ["BR-VAL-009", "BR-VAL-012", "BR-VAL-028"],
        [_f(None, "2025-06-04", 50 * M, "10-Q", "2025-06-10", "q125b")], close=10.0, splits=[{"date": "2025-12-01", "ratio": 4.0}],
        expect={"marketCap.status": "OK", "marketCap.value": 2000 * M, "shares.basis": "COVER_PAGE", "shares.value": 50 * M, "shares.asOf": "2025-06-04",
                "shares.ageDays": 90, "enterpriseValue.status": "BALANCES_NOT_AVAILABLE", "warningCodes": []}),
    _sh("val.hist.market_cap.no_share_count_shares_not_available", ["BR-VAL-009", "BR-VAL-011"], None,
        expect={"marketCap.status": "SHARES_NOT_AVAILABLE", "marketCap.value": None, "shares.basis": None, "shares.value": None, "shares.asOf": None, "shares.ageDays": None,
                "enterpriseValue.status": "MARKET_CAP_NOT_AVAILABLE", "warningCodes": []}),
    # A count of zero (or less) is shown but gives no market cap.
    _sh("val.hist.market_cap.zero_count_not_positive", ["BR-VAL-009", "BR-VAL-011", "BR-VAL-030"], [_f(None, "2025-07-30", 0, *Q225)],
        expect={"marketCap.status": "SHARE_COUNT_NOT_POSITIVE", "marketCap.value": None, "shares.basis": "COVER_PAGE", "shares.value": 0, "shares.asOf": "2025-07-30",
                "shares.ageDays": 34, "enterpriseValue.status": "MARKET_CAP_NOT_AVAILABLE", "warningCodes": ["SHARE_COUNT_NOT_POSITIVE"]}),
    _sh("val.hist.market_cap.negative_count_not_positive", ["BR-VAL-009", "BR-VAL-011"], [_f(None, "2025-07-30", -5 * M, *Q225)],
        expect={"marketCap.status": "SHARE_COUNT_NOT_POSITIVE", "marketCap.value": None, "shares.basis": "COVER_PAGE", "shares.value": -5 * M, "shares.asOf": "2025-07-30",
                "shares.ageDays": 34, "enterpriseValue.status": "MARKET_CAP_NOT_AVAILABLE", "warningCodes": ["SHARE_COUNT_NOT_POSITIVE"]}),
    # Card BR-VAL-030: only a weighted-average basic count (period ending 2023-03-31), date 2023-06-30 -> unresolved, value null, warning.
    _hv("val.hist.market_cap.card_030_weighted_average_only_is_unresolved", ["BR-VAL-009", "BR-VAL-011", "BR-VAL-030"],
        _inp(_facts({"Revenues": _usd(_f("2022-01-01", "2022-12-31", 1000 * M, "10-K", "2023-02-15", "k22")),
                     "WeightedAverageNumberOfSharesOutstandingBasic": {"units": {"shares": [_f("2023-01-01", "2023-03-31", 50 * M, "10-Q", "2023-05-05", "q123")]}}}),
             date="2023-06-30", close=10.0),
        select=_psel("marketCap.status", "marketCap.value", "shares.basis", "shares.value", "shares.pointInTime", "shares.asOf", "shares.coverPageRead.status",
                     "enterpriseValue.status", "status", "coreStatus"),
        expect={"marketCap.status": "POINT_IN_TIME_SHARES_UNRESOLVED", "marketCap.value": None, "shares.basis": "WEIGHTED_AVERAGE_BASIC", "shares.value": 50 * M,
                "shares.pointInTime": False, "shares.asOf": "2023-03-31", "shares.coverPageRead.status": "NO_PERIODIC_FILING",
                "enterpriseValue.status": "MARKET_CAP_NOT_AVAILABLE", "status": "PARTIAL", "coreStatus": "PARTIAL", "warningCodes": ["POINT_IN_TIME_SHARES_UNRESOLVED"]}),
    # A split between the count date and the trading date: no market cap.
    _sh("val.hist.market_cap.split_after_share_count", ["BR-VAL-009", "BR-VAL-013", "BR-VAL-028", "BR-VAL-030"], [_f(None, "2025-06-04", 50 * M, "10-Q", "2025-06-10", "q125b")],
        splits=[{"date": "2025-07-15", "ratio": 2.0}],
        select=_psel("marketCap.status", "marketCap.value", "marketCap.split.date", "marketCap.split.ratio", "price.laterSplitFactor", "price.close", "enterpriseValue.status"),
        expect={"marketCap.status": "SPLIT_AFTER_SHARE_COUNT", "marketCap.value": None, "marketCap.split.date": "2025-07-15", "marketCap.split.ratio": 2,
                "price.laterSplitFactor": 1, "price.close": 20, "enterpriseValue.status": "MARKET_CAP_NOT_AVAILABLE", "warningCodes": []}),
    # Precedence: SPLIT_AFTER_SHARE_COUNT before SHARE_COUNT_STALE.
    _sh("val.hist.market_cap.split_after_count_beats_stale_count", ["BR-VAL-009"], [_f(None, "2024-01-31", 50 * M, "10-Q", "2024-02-10", "q423")],
        splits=[{"date": "2024-06-01", "ratio": 2.0}],
        select=_psel("marketCap.status", "marketCap.value", "marketCap.split.date", "shares.ageDays"),
        expect={"marketCap.status": "SPLIT_AFTER_SHARE_COUNT", "marketCap.value": None, "marketCap.split.date": "2024-06-01", "shares.ageDays": 580,
                "warningCodes": ["SHARE_COUNT_STALE"]}),
    # Quarterly cadence: a count 400 days old is current, 401 is stale.
    _sh("val.hist.market_cap.count_400_days_old_is_current", ["BR-VAL-009", "BR-VAL-050", "BR-VAL-077"],
        [_f(None, _days_before("2025-09-02", 400), 50 * M, "10-Q", "2024-08-05", "q224b")],
        expect={"marketCap.status": "OK", "marketCap.value": 1000 * M, "shares.basis": "COVER_PAGE", "shares.value": 50 * M, "shares.asOf": "2024-07-29", "shares.ageDays": 400,
                "enterpriseValue.status": "BALANCES_NOT_AVAILABLE", "warningCodes": []}),
    _sh("val.hist.market_cap.count_401_days_old_is_stale", ["BR-VAL-009", "BR-VAL-050", "BR-VAL-077"],
        [_f(None, _days_before("2025-09-02", 401), 50 * M, "10-Q", "2024-08-05", "q224b")],
        expect={"marketCap.status": "SHARE_COUNT_STALE", "marketCap.value": None, "shares.basis": "COVER_PAGE", "shares.value": 50 * M, "shares.asOf": "2024-07-28",
                "shares.ageDays": 401, "enterpriseValue.status": "MARKET_CAP_NOT_AVAILABLE", "warningCodes": ["SHARE_COUNT_STALE"]}),
    # Share count preference: cover page over a newer balance-sheet count; the balance-sheet count when there is no cover page.
    _sh("val.hist.market_cap.cover_page_beats_newer_balance_sheet_count", ["BR-VAL-011", "BR-VAL-030", "BR-VAL-060"], [_f(None, "2025-02-10", 10 * M, *K24)],
        us={**_us(), "CommonStockSharesOutstanding": {"units": {"shares": [_f(None, "2025-06-30", 60 * M, *Q225)]}}},
        expect={"marketCap.status": "OK", "marketCap.value": 200 * M, "shares.basis": "COVER_PAGE", "shares.value": 10 * M, "shares.asOf": "2025-02-10", "shares.ageDays": 204,
                "enterpriseValue.status": "BALANCES_NOT_AVAILABLE", "warningCodes": []}),
    _sh("val.hist.market_cap.balance_sheet_count_without_cover_page", ["BR-VAL-011", "BR-VAL-030", "BR-VAL-060"], None,
        us={**_us(), "CommonStockSharesOutstanding": {"units": {"shares": [_f(None, "2025-06-30", 60 * M, *Q225)]}}},
        expect={"marketCap.status": "OK", "marketCap.value": 1200 * M, "shares.basis": "BALANCE_SHEET", "shares.value": 60 * M, "shares.asOf": "2025-06-30", "shares.ageDays": 64,
                "enterpriseValue.status": "BALANCES_NOT_AVAILABLE", "warningCodes": []}),
    # A count filed after the date is not known on it.
    _sh("val.hist.market_cap.count_filed_after_date_ignored", ["BR-VAL-011", "BR-VAL-026", "BR-VAL-030"],
        [_f(None, "2025-02-10", 10 * M, *K24), _f(None, "2025-09-20", 99 * M, "10-Q", "2025-09-25", "q325")],
        expect={"marketCap.status": "OK", "marketCap.value": 200 * M, "shares.basis": "COVER_PAGE", "shares.value": 10 * M, "shares.asOf": "2025-02-10", "shares.ageDays": 204,
                "enterpriseValue.status": "BALANCES_NOT_AVAILABLE", "warningCodes": []}),
    # An ADR's ordinary shares are divided by the ADS ratio before the quoted close is applied.
    _sh("val.hist.market_cap.ads_ratio_divides_ordinary_shares", ["BR-VAL-009", "BR-VAL-010", "BR-VAL-030"], _COVER(500 * M), ads=5,
        select=_psel("marketCap.status", "marketCap.value", "shares.value", "shares.quotedShareEquivalent"),
        expect={"marketCap.status": "OK", "marketCap.value": 2000 * M, "shares.value": 500 * M, "shares.quotedShareEquivalent": 100 * M, "warningCodes": []}),
]

_FILINGS = [{"accessionNumber": "q225", "form": "10-Q", "filed": "2025-08-05", "reportDate": "2025-06-30"}]
_CLASS_SUM_READ = {"status": "OK", "value": 25, "asOf": "2025-08-01", "basis": "COVER_PAGE_CLASS_SUM", "documentUrl": "https://www.sec.gov/q225.htm",
                   "classes": [{"class": "Common Class A", "member": "us-gaap:CommonClassAMember", "shares": 20},
                               {"class": "Common Class B", "member": "us-gaap:CommonClassBMember", "shares": 5}]}


def _cover_case(name, ids, read, expect, **kw) -> Case:
    return _hv(name, ids, _inp(_facts(_us(), [_f(None, "2025-02-10", 10, *K24)]), coverCounts={"q225": read}, periodicFilings=_FILINGS),
               select=_psel("shares.basis", "shares.value", "shares.asOf", "shares.accessionNumber", "marketCap.value"), expect=expect, **kw)


COVER_CASES = [
    # The latest periodic filing's per-class cover counts, summed, replace a companyfacts count that is older than that cover page.
    _cover_case("val.hist.shares.class_sum_replaces_older_companyfacts_count", ["BR-VAL-011", "BR-VAL-030", "BR-VAL-060"], _CLASS_SUM_READ,
                {"shares.basis": "COVER_PAGE_CLASS_SUM", "shares.value": 25, "shares.asOf": "2025-08-01", "shares.accessionNumber": "q225", "marketCap.value": 500,
                 "warningCodes": ["SHARE_CLASSES_SUMMED"]}),
    _cover_case("val.hist.shares.older_class_sum_does_not_replace_companyfacts_count", ["BR-VAL-011", "BR-VAL-030", "BR-VAL-060"], {**_CLASS_SUM_READ, "asOf": "2025-01-01"},
                {"shares.basis": "COVER_PAGE", "shares.value": 10, "shares.asOf": "2025-02-10", "shares.accessionNumber": "k24", "marketCap.value": 200, "warningCodes": []}),
    _cover_case("val.hist.shares.unreadable_cover_leaves_companyfacts_count", ["BR-VAL-011", "BR-VAL-030", "BR-VAL-060"],
                {"status": "OTHER_DIMENSIONS", "value": None, "asOf": "2025-08-01", "dimensions": ["dei:LegalEntityAxis"]},
                {"shares.basis": "COVER_PAGE", "shares.value": 10, "shares.asOf": "2025-02-10", "shares.accessionNumber": "k24", "marketCap.value": 200, "warningCodes": []}),
]


def _ctx(cid, date, members=()):
    segment = "".join(f'<xbrldi:explicitMember dimension="{d}">{m}</xbrldi:explicitMember>' for d, m in members)
    entity = f"<xbrli:entity><xbrli:identifier>1</xbrli:identifier>{f'<xbrli:segment>{segment}</xbrli:segment>' if segment else ''}</xbrli:entity>"
    return f'<xbrli:context id="{cid}">{entity}<xbrli:period><xbrli:instant>{date}</xbrli:instant></xbrli:period></xbrli:context>'


def _cvr(cid, text, fmt="ixt:num-dot-decimal", scale=None):
    scale_attr = f' scale="{scale}"' if scale is not None else ""
    return (f'<ix:nonFraction contextRef="{cid}" name="dei:EntityCommonStockSharesOutstanding" unitRef="shares" decimals="INF" format="{fmt}"{scale_attr}>'
            f"{text}</ix:nonFraction>")


_CA = ("us-gaap:StatementClassOfStockAxis", "us-gaap:CommonClassAMember")
_CB = ("us-gaap:StatementClassOfStockAxis", "us-gaap:CommonClassBMember")
COVER_HTML = [
    "<ix:header><ix:resources>" + _ctx("a", "2026-08-06", [_CA]) + _ctx("b", "2026-08-06", [_CB]) + _ctx("old", "2025-08-06", [_CA])
    + "</ix:resources></ix:header><p>" + _cvr("a", "299,789,305") + _cvr("b", "<span>11,215,111</span>") + _cvr("a", "299,789,305") + _cvr("old", "1") + "</p>",
    _ctx("t", "2026-08-06") + _ctx("a", "2026-08-06", [_CA]) + _cvr("t", "311,004,416") + _cvr("a", "299,789,305"),
    _ctx("a", "2026-08-06", [_CA, ("dei:LegalEntityAxis", "x:SubMember")]) + _cvr("a", "5"),
    _ctx("a", "2026-08-06", [_CA]) + _cvr("a", "10") + _cvr("missing", "3"),
    "<p>No cover count.</p>",
    _ctx("a", "2026-08-06", [_CA]) + _ctx("b", "2026-08-06", [_CB]) + _cvr("a", "1.234,00", fmt="ixt:num-comma-decimal", scale=3) + _cvr("b", "—", fmt="ixt:fixed-zero"),
    _ctx("a", "2026-08-06", [_CA]) + _cvr("a", "n/a"),
]


def _cover_summary(results):
    return [[r["status"], r.get("basis"), r.get("value"), r.get("asOf"), [c["class"] for c in r.get("classes") or []]] for r in results]


COVER_PARSE_CASES = [
    Case(name="val.hist.shares.cover_page_html_statuses", ids=["BR-VAL-011", "BR-VAL-030", "BR-VAL-042", "BR-VAL-043", "BR-VAL-044"],
         py=lambda: [vh.cover_share_counts(h) for h in COVER_HTML],
         ts={"js": "return args[0].map((h) => m.valuationHistory.coverShareCounts(h));", "args": [COVER_HTML]},
         select=_cover_summary,
         # Per-class counts at the latest cover date are summed (the repeated and older facts are ignored); an undimensioned total wins;
         # a second dimension is not summed; an unresolved context fails the read; no count; comma-decimal with scale and a fixed-zero class; unparsed value.
         expect=[["OK", "COVER_PAGE_CLASS_SUM", 311_004_416, "2026-08-06", ["Common Class A", "Common Class B"]],
                 ["OK", "COVER_PAGE", 311_004_416, "2026-08-06", []],
                 ["OTHER_DIMENSIONS", None, None, "2026-08-06", []],
                 ["CONTEXT_NOT_READ", None, None, None, []],
                 ["NOT_TAGGED", None, None, None, []],
                 ["OK", "COVER_PAGE_CLASS_SUM", 1_234_000, "2026-08-06", ["Common Class A", "Common Class B"]],
                 ["VALUE_NOT_PARSED", None, None, None, []]]),
]


# ───────────────────────────────────────────────────────────────── LTM = LFY + YTD - prior YTD (cards BR-VAL-014 ... 018)

def _ff(start, end, val, concept="Revenues", form="10-K", filed="2025-02-15", accn="a1") -> dict:
    return {"concept": concept, "start": start, "end": end, "val": val, "filed": filed, "form": form, "accn": accn}


def _flow(v: dict) -> dict:
    out: dict = {}
    for k in ("LFY", "LTM"):
        r = v[k]
        out[k] = [r["status"], r.get("value"), r.get("method") or r.get("reason"), r.get("periodStart"), r.get("periodEnd"), r.get("latestPeriodEnd")]
    out["signs"] = [c["sign"] for c in v["LTM"].get("components", [])]
    out["componentValues"] = [c["value"] for c in v["LTM"].get("components", [])]
    return out


def _flows(name: str, ids: list[str], facts: list[dict], expect: dict, **kw) -> Case:
    return Case(name=name, ids=ids, py=lambda: vh.flow_bases(copy.deepcopy(facts)), ts={"module": "valuationHistory", "fn": "flowBases", "args": [facts]},
                select=_flow, expect=expect, **kw)


def _ok_ltm(lfy_start, lfy_end, lfy_val, ltm_val, ltm_end, comps) -> dict:
    return {"LFY": ["OK", lfy_val, "REPORTED_FISCAL_YEAR", lfy_start, lfy_end, None],
            "LTM": ["OK", ltm_val, "LFY_PLUS_YTD_MINUS_PRIOR_YTD", None, ltm_end, None], "signs": [1, 1, -1], "componentValues": comps}


_NO_ANNUAL = {"LFY": ["NOT_AVAILABLE", None, "NO_ANNUAL_PERIOD_FILED", None, None, None], "LTM": ["NOT_AVAILABLE", None, "NO_ANNUAL_PERIOD_FILED", None, None, None],
              "signs": [], "componentValues": []}
_FY24 = _ff("2024-01-01", "2024-12-31", 1000)
_YTD25 = _ff("2025-01-01", "2025-06-30", 600, form="10-Q", filed="2025-08-05", accn="q2")
_PRIOR24 = _ff("2024-01-01", "2024-06-30", 450, form="10-Q", filed="2025-08-05", accn="q2")


def _prior_ending(gap: int, start: str = "2024-01-01") -> dict:
    return _ff(start, _days_before("2025-06-30", gap), 450, form="10-Q", filed="2025-08-05", accn="q2")


FLOW_CASES = [
    _flows("val.hist.ltm.card_014_lfy_plus_ytd_minus_prior_ytd", ["BR-VAL-014", "BR-VAL-017", "BR-VAL-018"], [_FY24, _YTD25, _PRIOR24],
           _ok_ltm("2024-01-01", "2024-12-31", 1000, 1150, "2025-06-30", [1000, 600, 450])),
    _flows("val.hist.ltm.card_015_nine_month_ytd", ["BR-VAL-015", "BR-VAL-014"],
           [_ff("2025-01-01", "2025-12-31", 1000), _ff("2026-01-01", "2026-09-30", 900, form="10-Q", accn="q3"), _ff("2025-01-01", "2025-09-30", 750, form="10-Q", accn="q3")],
           _ok_ltm("2025-01-01", "2025-12-31", 1000, 1150, "2026-09-30", [1000, 900, 750])),
    _flows("val.hist.ltm.card_016_six_month_ytd", ["BR-VAL-016", "BR-VAL-014"],
           [_ff("2025-01-01", "2025-12-31", 400), _ff("2026-01-01", "2026-06-30", 230, form="10-Q", accn="q2"), _ff("2025-01-01", "2025-06-30", 190, form="10-Q", accn="q2")],
           _ok_ltm("2025-01-01", "2025-12-31", 400, 440, "2026-06-30", [400, 230, 190])),
    _flows("val.hist.ltm.card_017_fy2023_nine_months", ["BR-VAL-017", "BR-VAL-014"],
           [_ff("2023-01-01", "2023-12-31", 1000), _ff("2024-01-01", "2024-09-30", 800, form="10-Q", accn="q3"), _ff("2023-01-01", "2023-09-30", 700, form="10-Q", accn="q3")],
           _ok_ltm("2023-01-01", "2023-12-31", 1000, 1100, "2024-09-30", [1000, 800, 700])),
    # Card BR-VAL-018 plus "the longest YTD wins": two YTD periods end 2025-09-30 (one starting 3 days late, listed first); a standalone Q3 is not a YTD.
    _flows("val.hist.ltm.card_018_longest_ytd_wins", ["BR-VAL-018", "BR-VAL-014", "BR-VAL-017"],
           [_FY24, _ff("2025-01-04", "2025-09-30", 300, form="10-Q", accn="q3"), _ff("2025-07-01", "2025-09-30", 400, form="10-Q", accn="q3"),
            _ff("2025-01-01", "2025-09-30", 900, form="10-Q", accn="q3"), _ff("2024-01-01", "2024-09-30", 700, form="10-Q", accn="q3")],
           _ok_ltm("2024-01-01", "2024-12-31", 1000, 1200, "2025-09-30", [1000, 900, 700])),
    _flows("val.hist.ltm.fiscal_year_is_latest_period", ["BR-VAL-014", "BR-VAL-015", "BR-VAL-016", "BR-VAL-017", "BR-VAL-018"],
           [_FY24, _ff("2024-01-01", "2024-06-30", 450, form="10-Q", accn="q2")],
           {"LFY": ["OK", 1000, "REPORTED_FISCAL_YEAR", "2024-01-01", "2024-12-31", None], "LTM": ["OK", 1000, "LAST_FISCAL_YEAR_IS_LATEST", "2024-01-01", "2024-12-31", None],
            "signs": [1], "componentValues": [1000]}),
    _flows("val.hist.ltm.no_annual_period_filed", ["BR-VAL-014", "BR-VAL-016", "BR-VAL-018"], [_ff("2025-01-01", "2025-03-31", 260, form="10-Q", accn="q1"), _YTD25], _NO_ANNUAL),
    _flows("val.hist.ltm.instant_facts_are_not_periods", ["BR-VAL-014"], [_ff(None, "2024-12-31", 1000)], _NO_ANNUAL),
    _flows("val.hist.ltm.latest_annual_period_is_lfy", ["BR-VAL-015", "BR-VAL-016"], [_ff("2024-01-01", "2024-12-31", 1000), _ff("2023-01-01", "2023-12-31", 800)],
           {"LFY": ["OK", 1000, "REPORTED_FISCAL_YEAR", "2024-01-01", "2024-12-31", None], "LTM": ["OK", 1000, "LAST_FISCAL_YEAR_IS_LATEST", "2024-01-01", "2024-12-31", None],
            "signs": [1], "componentValues": [1000]}),
    # Only a standalone quarter follows the fiscal year: no year-to-date period starts within 4 days of its end.
    _flows("val.hist.ltm.year_to_date_not_filed", ["BR-VAL-014", "BR-VAL-015", "BR-VAL-016", "BR-VAL-017", "BR-VAL-018"],
           [_FY24, _ff("2025-04-01", "2025-06-30", 300, form="10-Q", accn="q2"), _ff("2024-04-01", "2024-06-30", 240, form="10-Q", accn="q2")],
           {"LFY": ["OK", 1000, "REPORTED_FISCAL_YEAR", "2024-01-01", "2024-12-31", None],
            "LTM": ["NOT_AVAILABLE", None, "YEAR_TO_DATE_NOT_FILED", None, None, "2025-06-30"], "signs": [], "componentValues": []}),
    _flows("val.hist.ltm.prior_year_to_date_not_filed", ["BR-VAL-014", "BR-VAL-015", "BR-VAL-016", "BR-VAL-017", "BR-VAL-018"], [_FY24, _YTD25],
           {"LFY": ["OK", 1000, "REPORTED_FISCAL_YEAR", "2024-01-01", "2024-12-31", None],
            "LTM": ["NOT_AVAILABLE", None, "PRIOR_YEAR_TO_DATE_NOT_FILED", None, None, "2025-06-30"], "signs": [], "componentValues": []}),
    # Year-to-date start gap: 4 days after the fiscal year end is accepted (2025-01-04), 5 days is not.
    _flows("val.hist.ltm.ytd_starting_4_days_after_lfy_end_is_used", ["BR-VAL-014", "BR-VAL-015", "BR-VAL-017"],
           [_FY24, _ff("2025-01-04", "2025-06-30", 600, form="10-Q", accn="q2"), _PRIOR24],
           _ok_ltm("2024-01-01", "2024-12-31", 1000, 1150, "2025-06-30", [1000, 600, 450])),
    _flows("val.hist.ltm.ytd_starting_5_days_after_lfy_end_is_not", ["BR-VAL-014", "BR-VAL-015", "BR-VAL-017"],
           [_FY24, _ff("2025-01-05", "2025-06-30", 600, form="10-Q", accn="q2"), _PRIOR24],
           {"LFY": ["OK", 1000, "REPORTED_FISCAL_YEAR", "2024-01-01", "2024-12-31", None],
            "LTM": ["NOT_AVAILABLE", None, "YEAR_TO_DATE_NOT_FILED", None, None, "2025-06-30"], "signs": [], "componentValues": []}),
    # Prior-year period must end 358 to 372 days before the current YTD end.
    _flows("val.hist.ltm.prior_ytd_ending_358_days_before_is_used", ["BR-VAL-014", "BR-VAL-018"], [_FY24, _YTD25, _prior_ending(358, "2024-01-05")],
           _ok_ltm("2024-01-01", "2024-12-31", 1000, 1150, "2025-06-30", [1000, 600, 450])),
    _flows("val.hist.ltm.prior_ytd_ending_357_days_before_is_not", ["BR-VAL-014", "BR-VAL-018"], [_FY24, _YTD25, _prior_ending(357, "2024-01-05")],
           {"LFY": ["OK", 1000, "REPORTED_FISCAL_YEAR", "2024-01-01", "2024-12-31", None],
            "LTM": ["NOT_AVAILABLE", None, "PRIOR_YEAR_TO_DATE_NOT_FILED", None, None, "2025-06-30"], "signs": [], "componentValues": []}),
    _flows("val.hist.ltm.prior_ytd_ending_372_days_before_is_used", ["BR-VAL-014", "BR-VAL-018"], [_FY24, _YTD25, _prior_ending(372)],
           _ok_ltm("2024-01-01", "2024-12-31", 1000, 1150, "2025-06-30", [1000, 600, 450])),
    _flows("val.hist.ltm.prior_ytd_ending_373_days_before_is_not", ["BR-VAL-014", "BR-VAL-018"], [_FY24, _YTD25, _prior_ending(373)],
           {"LFY": ["OK", 1000, "REPORTED_FISCAL_YEAR", "2024-01-01", "2024-12-31", None],
            "LTM": ["NOT_AVAILABLE", None, "PRIOR_YEAR_TO_DATE_NOT_FILED", None, None, "2025-06-30"], "signs": [], "componentValues": []}),
    Case(name="val.hist.ltm.annual_period_length_350_to_380_days", ids=["BR-VAL-015", "BR-VAL-016"],
         py=lambda: [vh.flow_bases([_ff("2024-01-01", _days_after("2024-01-01", n), 1000)])["LFY"]["status"] for n in (349, 350, 380, 381)],
         ts={"js": "return args[0].map((f) => m.valuationHistory.flowBases([f]).LFY.status);",
             "args": [[_ff("2024-01-01", _days_after("2024-01-01", n), 1000) for n in (349, 350, 380, 381)]]},
         expect=["NOT_AVAILABLE", "OK", "OK", "NOT_AVAILABLE"]),
]


# ───────────────────────────────────────────────────────────────── point-in-time fact selection (cards BR-VAL-026, 027, 029)

_EX = "RevenueFromContractWithCustomerExcludingAssessedTax"
_FY24_ROW = lambda val, form, filed, accn: _f("2024-01-01", "2024-12-31", val, form, filed, accn)  # noqa: E731
_REV_SEL = _psel("denominators.LFY.revenue.status", "denominators.LFY.revenue.reason", "denominators.LFY.revenue.value", "denominators.LFY.revenue.components.0.concept",
                 "denominators.LFY.revenue.components.0.form", "denominators.LFY.revenue.components.0.filed", "denominators.LFY.revenue.components.0.accessionNumber", codes=None)


def _pit_rev(name, ids, us, date, expect) -> Case:
    facts = _facts(us, [_f(None, "2025-02-10", 100 * M, *K24)])
    return _hv(name, ids, _inp(facts, date=date), select=_REV_SEL, expect=expect)


def _rev_expect(concept, val, form, filed, accn) -> dict:
    return {"denominators.LFY.revenue.status": "OK", "denominators.LFY.revenue.reason": None, "denominators.LFY.revenue.value": val,
            "denominators.LFY.revenue.components.0.concept": concept, "denominators.LFY.revenue.components.0.form": form,
            "denominators.LFY.revenue.components.0.filed": filed, "denominators.LFY.revenue.components.0.accessionNumber": accn}


_NI_SEL = _psel("denominators.LFY.netIncome.status", "denominators.LFY.netIncome.value", "denominators.LFY.netIncome.components.0.concept",
                "denominators.LFY.netIncome.components.0.filed", codes=None)


def _pit_ni(name, ids, ni_rows, date, expect) -> Case:
    us = {"Revenues": _usd(_f("2022-01-01", "2022-12-31", 1000 * M, "10-K", "2023-02-01", "k22")), **{c: _usd(*rows) for c, rows in ni_rows.items()}}
    facts = _facts(us, [_f(None, "2023-01-20", 100 * M, "10-K", "2023-02-01", "k22")])
    return _hv(name, ids, _inp(facts, date=date), select=_NI_SEL, expect=expect)


_NI22 = lambda val, filed, accn, form="10-K": _f("2022-01-01", "2022-12-31", val, form, filed, accn)  # noqa: E731

PIT_CASES = [
    # Card BR-VAL-026: contract revenue (listed first) filed 2025-02-15; Revenues re-filed in a 10-K/A on 2025-04-01 does not displace it;
    # a later filing of the first concept (2025-06-15) is after the 2025-05-01 date and ignored.
    _pit_rev("val.hist.pit.card_026_listed_first_concept_beats_newer_10ka", ["BR-VAL-026", "BR-VAL-027", "BR-VAL-029"],
             {_EX: _usd(_FY24_ROW(1000 * M, *K24), _FY24_ROW(9999 * M, "10-K", "2025-06-15", "kx")), "Revenues": _usd(_FY24_ROW(1100 * M, "10-K/A", "2025-04-01", "k24a"))},
             "2025-05-01", _rev_expect(_EX, 1000 * M, "10-K", "2025-02-15", "k24")),
    # Card BR-VAL-029: contract revenue 2025-02-01, Revenues 2025-05-01, read as of 2025-06-01: the first-listed concept still wins.
    _pit_rev("val.hist.pit.card_029_first_listed_concept_despite_later_revenues_filing", ["BR-VAL-029", "BR-VAL-026", "BR-VAL-027"],
             {_EX: _usd(_FY24_ROW(1000 * M, "10-K", "2025-02-01", "k24")), "Revenues": _usd(_FY24_ROW(1100 * M, "10-K/A", "2025-05-01", "k24a"))},
             "2025-06-01", _rev_expect(_EX, 1000 * M, "10-K", "2025-02-01", "k24")),
    # Within one concept the newest filing by the date wins (a restatement known on the date), not one filed later.
    _pit_rev("val.hist.pit.same_concept_newer_filing_replaces", ["BR-VAL-026", "BR-VAL-027", "BR-VAL-029"],
             {_EX: _usd(_FY24_ROW(1000 * M, *K24), _FY24_ROW(1010 * M, "10-K/A", "2025-04-01", "k24a"))},
             "2025-05-01", _rev_expect(_EX, 1010 * M, "10-K/A", "2025-04-01", "k24a")),
    _pit_rev("val.hist.pit.restatement_filed_after_date_is_not_known", ["BR-VAL-026", "BR-VAL-029"],
             {_EX: _usd(_FY24_ROW(1000 * M, *K24), _FY24_ROW(1010 * M, "10-K/A", "2025-04-01", "k24a"))},
             "2025-03-01", _rev_expect(_EX, 1000 * M, "10-K", "2025-02-15", "k24")),
    # The same filing date does not replace: the first row stays.
    _pit_rev("val.hist.pit.same_filing_date_keeps_first_row", ["BR-VAL-026", "BR-VAL-029"],
             {_EX: _usd(_FY24_ROW(1000 * M, *K24), _FY24_ROW(1005 * M, "10-K", "2025-02-15", "k24b"))},
             "2025-05-01", _rev_expect(_EX, 1000 * M, "10-K", "2025-02-15", "k24")),
    # Only 10-K, 10-Q, 20-F and 40-F filings count: an 8-K row is no fiscal-year revenue.
    _pit_rev("val.hist.pit.non_periodic_forms_ignored", ["BR-VAL-026", "BR-VAL-029"], {"Revenues": _usd(_FY24_ROW(777 * M, "8-K", "2025-02-15", "e1"))}, "2025-05-01",
             {"denominators.LFY.revenue.status": "NOT_AVAILABLE", "denominators.LFY.revenue.reason": "NO_ANNUAL_PERIOD_FILED", "denominators.LFY.revenue.value": None,
              "denominators.LFY.revenue.components.0.concept": None, "denominators.LFY.revenue.components.0.form": None, "denominators.LFY.revenue.components.0.filed": None,
              "denominators.LFY.revenue.components.0.accessionNumber": None}),
    _pit_rev("val.hist.pit.20f_and_40f_forms_count", ["BR-VAL-026", "BR-VAL-029"], {"Revenues": _usd(_FY24_ROW(500 * M, "20-F", "2025-02-15", "f24"))}, "2025-05-01",
             _rev_expect("Revenues", 500 * M, "20-F", "2025-02-15", "f24")),
    # Card BR-VAL-027: NetIncomeLoss filed 2023-02-01 beats ProfitLoss filed 2023-05-01 (listed first), as of 2023-06-01.
    _pit_ni("val.hist.pit.card_027_net_income_loss_beats_newer_profit_loss", ["BR-VAL-027", "BR-VAL-026", "BR-VAL-029"],
            {"NetIncomeLoss": [_NI22(100 * M, "2023-02-01", "k22")], "ProfitLoss": [_NI22(110 * M, "2023-05-01", "k22a", "10-K/A")]}, "2023-06-01",
            {"denominators.LFY.netIncome.status": "OK", "denominators.LFY.netIncome.value": 100 * M, "denominators.LFY.netIncome.components.0.concept": "NetIncomeLoss",
             "denominators.LFY.netIncome.components.0.filed": "2023-02-01"}),
    # The lower-priority concept is used only while the higher one has nothing filed by the date.
    _pit_ni("val.hist.pit.lower_priority_concept_used_until_listed_first_is_filed", ["BR-VAL-027", "BR-VAL-026"],
            {"NetIncomeLoss": [_NI22(100 * M, "2023-07-01", "k22b", "10-K/A")], "ProfitLoss": [_NI22(110 * M, "2023-05-01", "k22a", "10-K/A")]}, "2023-06-01",
            {"denominators.LFY.netIncome.status": "OK", "denominators.LFY.netIncome.value": 110 * M, "denominators.LFY.netIncome.components.0.concept": "ProfitLoss",
             "denominators.LFY.netIncome.components.0.filed": "2023-05-01"}),
]


# ───────────────────────────────────────────────────────────────── recorded fixtures: NVDA split, AEHR zero debt, TSM IFRS ADR

def _chart(name: str) -> tuple[list[dict], list[dict]]:
    """Split-adjusted, dividend-unadjusted daily closes and split events from a recorded Yahoo chart, as server._daily_history_sync builds them."""
    result = fixture(f"yahoo/chart_{name}_5y.json")["chart"]["result"][0]
    offset = result["meta"].get("gmtoffset", 0)
    closes = result["indicators"]["quote"][0]["close"]
    bars = []
    for ts, close in zip(result["timestamp"], closes):
        if close is not None:
            bars.append({"date": (_dt.datetime(1970, 1, 1) + _dt.timedelta(seconds=ts + offset)).strftime("%Y-%m-%d"), "close": close})
    splits = [{"date": (_dt.datetime(1970, 1, 1) + _dt.timedelta(seconds=e["date"] + offset)).strftime("%Y-%m-%d"), "ratio": e["numerator"] / e["denominator"]}
              for e in (result.get("events", {}).get("splits") or {}).values()]
    return bars, splits


_NVDA_BARS, _NVDA_SPLITS = _chart("NVDA")


def _nvda_points(v: dict) -> list:
    return [[p["date"], p["price"]["tradingDate"], p["price"]["laterSplitFactor"], p["price"]["close"], p["shares"]["value"], p["shares"]["asOf"],
             p["marketCap"]["status"], p["marketCap"]["value"], (p["marketCap"].get("split") or {}).get("date")] for p in v["points"]]


RECORDED_CASES = [
    # NVDA's 10-for-1 split on 2024-06-10. On 2023-06-30 Yahoo's split-adjusted close (42.30) is undone to the 423.02 the share count was filed against.
    # On 2024-06-28 the newest count (the 10-Q cover as of 2024-05-24, filed 2024-05-29) predates the split, so market cap is withheld.
    _hv("val.hist.recorded.nvda_split_price_and_market_cap", ["BR-VAL-012", "BR-VAL-013", "BR-VAL-028", "BR-VAL-009"],
        _inp(fixture("sec/companyfacts_NVDA.json"), bars=_NVDA_BARS, splits=_NVDA_SPLITS, dates=["2023-06-30", "2024-06-28"]),
        select=_nvda_points,
        # Market cap multiplies the unrounded unadjusted close (423.0199813...) by the count, not the 4-decimal price shown.
        expect=[["2023-06-30", "2023-06-30", 10, 423.02, 2_470_000_000, "2023-05-19", "OK", 1_044_859_354_019, None],
                ["2024-06-28", "2024-06-28", 1, 123.54, 2_460_000_000, "2024-05-24", "SPLIT_AFTER_SHARE_COUNT", None, "2024-06-10"]]),
    # AEHR files 10-Ks and 10-Qs that tag no borrowing concept at any date: debt is zero, not "not tagged" (zero-debt route).
    # The close is a synthetic 20.00 (no recorded AEHR chart).
    _hv("val.hist.recorded.aehr_no_borrowings_means_zero_debt", ["BR-VAL-006", "BR-VAL-008", "BR-VAL-047"],
        _inp(fixture("sec/companyfacts_AEHR.json"), date="2026-08-14", close=20.0), select=_bsel,
        # Market cap 20.00 x 32,620,450 (10-K cover, 2026-07-20) = 652,409,000; EV = that + 0 debt - 116,358,000 cash (2026-05-29).
        expect={"balances.status": "OK", "balances.reason": None, "balanceDate": "2026-05-29", "debt.status": "OK", "debt.value": 0, "debt.basis": "NO_BORROWINGS_TAGGED",
                "debt.concepts": [], "sti.status": "NOT_TAGGED", "marketCap.status": "OK", "ev.status": "OK", "ev.value": 536_051_000,
                "warningCodes": ["NO_BORROWINGS_TAGGED", "SHORT_TERM_INVESTMENTS_NOT_TAGGED"]}),
    # TSM (20-F, IFRS, TWD): ordinary shares divided by 5 per ADS, FX at the 20-F date; IFRS maps no short-term investments.
    _hv("val.hist.recorded.tsm_ifrs_adr_fx_and_ads_ratio", ["BR-VAL-009", "BR-VAL-010", "BR-VAL-030", "BR-VAL-008"],
        {**_inp(fixture("sec/companyfacts_TSM.json"), date="2025-06-02", close=180.0, ads=5), "fx": {"pair": "TWDUSD=X", "bars": [{"date": "2025-05-30", "close": 0.0305}]}},
        select=_psel("marketCap.value", "shares.value", "shares.quotedShareEquivalent", "shares.basis", "fx.rate", "reportingCurrency", "secCompanyfactsCadence",
                     "enterpriseValue.status", "enterpriseValue.value", "balances.balanceDate", "balances.shortTermInvestments.status", "balances.debt.value"),
        # Market cap 180 x 25,932,733,242 / 5 = 933,578,396,712 USD; EV adds (91,682,300,000 debt - 2,127,627,000,000 cash) TWD x 0.0305.
        expect={"marketCap.value": 933_578_396_712, "shares.value": 25_932_733_242, "shares.quotedShareEquivalent": 5_186_546_648, "shares.basis": "COVER_PAGE",
                "fx.rate": 0.0305, "reportingCurrency": "TWD", "secCompanyfactsCadence": "ANNUAL", "enterpriseValue.status": "OK", "enterpriseValue.value": 871_482_083_362,
                "balances.balanceDate": "2024-12-31", "balances.shortTermInvestments.status": "NOT_MAPPED", "balances.debt.value": 91_682_300_000,
                "warningCodes": ["SHORT_TERM_INVESTMENTS_NOT_TAGGED"]}),
]


# ═══════════════════════════════════════════════════════════════════ metric reconciliation (metric_reconciliation)

def _o(source: str, value, precision=0, provider: str | None = None, status: str = "FOUND") -> dict:
    return {"source": source, "provider": provider or ("SEC" if source.startswith("SEC") else source), "status": status, "value": value, "precision": precision}


def _cmp(source, against, value, baseline, difference, pct, tolerance, result) -> dict:
    return {"source": source, "against": against, "value": value, "baselineValue": baseline, "difference": difference, "differencePct": pct, "tolerance": tolerance, "result": result}


_SEC = "SEC_XBRL_LATEST"
_SEC1 = "SEC_XBRL_AS_FIRST_FILED"
_REL = "ISSUER_RELEASE"
_YAH = "YAHOO"


def _rec(name: str, ids: list[str], obs: list[dict], tol: float, comparisons: list, status: str, restated: bool, **kw) -> Case:
    return Case(name=name, ids=ids, py=lambda: mr.reconcile_observations(copy.deepcopy(obs), tol),
                ts={"js": "const r = m.metricReconciliation.reconcileObservations(args[0], args[1]); return [r.comparisons, r.status, r.restated];", "args": [obs, tol]},
                expect=[comparisons, status, restated], **kw)


RECON_OBS_CASES = [
    # Card BR-VAL-019: SEC 31,500,000, release "$31.5 million" (precision 50,000), tolerance 0.5% = 157,500 -> MATCH, AGREED.
    _rec("val.recon.card_019_release_matches_within_tolerance", ["BR-VAL-019", "BR-VAL-020", "BR-VAL-021", "BR-VAL-022"],
         [_o(_SEC, 31_500_000), _o(_SEC1, 31_500_000), _o(_REL, 31_500_000, 50_000)], 0.5,
         [_cmp(_SEC1, _SEC, 31_500_000, 31_500_000, 0, 0, 157_500, "MATCH"), _cmp(_REL, _SEC, 31_500_000, 31_500_000, 0, 0, 157_500, "MATCH")], "AGREED", False),
    # Card BR-VAL-021: SEC 31,512,000 vs release 31,500,000 (precision 50,000): tolerance max(157,560, 50,000); difference -12,000 = -0.0381%.
    _rec("val.recon.card_021_difference_pct_and_tolerance", ["BR-VAL-021", "BR-VAL-019", "BR-VAL-022"], [_o(_SEC, 31_512_000), _o(_REL, 31_500_000, 50_000)], 0.5,
         [_cmp(_REL, _SEC, 31_500_000, 31_512_000, -12_000, -0.0381, 157_560, "MATCH")], "AGREED", False),
    # A release's stated precision widens the tolerance when the percentage is smaller (tolerance 0%).
    _rec("val.recon.release_precision_widens_tolerance", ["BR-VAL-019", "BR-VAL-021"], [_o(_SEC, 31_500_000), _o(_REL, 31_540_000, 50_000)], 0.0,
         [_cmp(_REL, _SEC, 31_540_000, 31_500_000, 40_000, 0.127, 50_000, "MATCH")], "AGREED", False),
    _rec("val.recon.release_beyond_precision_is_conflict", ["BR-VAL-019", "BR-VAL-021"], [_o(_SEC, 31_500_000), _o(_REL, 31_560_000, 50_000)], 0.0,
         [_cmp(_REL, _SEC, 31_560_000, 31_500_000, 60_000, 0.1905, 50_000, "MISMATCH")], "CONFLICT", False),
    # Card BR-VAL-022: SEC 1,000,000,000, Yahoo 1,004,000,000 at 0.5% -> MATCH, AGREED; 1,006,000,000 -> CONFLICT; exactly 5,000,000 apart still matches.
    _rec("val.recon.card_022_yahoo_within_tolerance_agreed", ["BR-VAL-022", "BR-VAL-020", "BR-VAL-021"], [_o(_SEC, 1_000_000_000), _o(_YAH, 1_004_000_000)], 0.5,
         [_cmp(_YAH, _SEC, 1_004_000_000, 1_000_000_000, 4_000_000, 0.4, 5_000_000, "MATCH")], "AGREED", False),
    _rec("val.recon.card_022_yahoo_beyond_tolerance_conflict", ["BR-VAL-022", "BR-VAL-020", "BR-VAL-021"], [_o(_SEC, 1_000_000_000), _o(_YAH, 1_006_000_000)], 0.5,
         [_cmp(_YAH, _SEC, 1_006_000_000, 1_000_000_000, 6_000_000, 0.6, 5_000_000, "MISMATCH")], "CONFLICT", False),
    _rec("val.recon.difference_exactly_at_tolerance_matches", ["BR-VAL-021", "BR-VAL-022"], [_o(_SEC, 1_000_000_000), _o(_YAH, 1_005_000_000)], 0.5,
         [_cmp(_YAH, _SEC, 1_005_000_000, 1_000_000_000, 5_000_000, 0.5, 5_000_000, "MATCH")], "AGREED", False),
    _rec("val.recon.wider_tolerance_turns_conflict_into_agreed", ["BR-VAL-021", "BR-VAL-022"], [_o(_SEC, 1000), _o(_YAH, 1099)], 10,
         [_cmp(_YAH, _SEC, 1099, 1000, 99, 9.9, 100, "MATCH")], "AGREED", False),
    # Without SEC the first found source is the reference: a disagreement is CONFLICT (not PARTIAL), agreement never AGREED.
    _rec("val.recon.no_sec_agreeing_sources_are_partial", ["BR-VAL-019", "BR-VAL-020", "BR-VAL-021", "BR-VAL-022"], [_o(_REL, 100.0), _o(_YAH, 100.0)], 0.5,
         [_cmp(_YAH, _REL, 100, 100, 0, 0, 0.5, "MATCH")], "PARTIAL", False),
    _rec("val.recon.no_sec_disagreeing_sources_are_conflict", ["BR-VAL-019", "BR-VAL-020", "BR-VAL-021", "BR-VAL-022"], [_o(_REL, 100.0), _o(_YAH, 120.0)], 0.5,
         [_cmp(_YAH, _REL, 120, 100, 20, 20, 0.5, "MISMATCH")], "CONFLICT", False),
    # The reference's own stated precision widens the tolerance too.
    _rec("val.recon.reference_precision_widens_tolerance", ["BR-VAL-021", "BR-VAL-022"], [_o(_REL, 31_500_000, 50_000), _o(_YAH, 31_540_000)], 0.0,
         [_cmp(_YAH, _REL, 31_540_000, 31_500_000, 40_000, 0.127, 50_000, "MATCH")], "PARTIAL", False),
    # SEC as first filed and as latest filed are one provider: they cannot make AGREED, and a difference is a restatement, not a conflict.
    _rec("val.recon.two_sec_observations_are_one_provider", ["BR-VAL-019", "BR-VAL-020", "BR-VAL-022"], [_o(_SEC, 100), _o(_SEC1, 100)], 0.5,
         [_cmp(_SEC1, _SEC, 100, 100, 0, 0, 0.5, "MATCH")], "PARTIAL", False),
    _rec("val.recon.restatement_alone_is_partial_not_conflict", ["BR-VAL-020", "BR-VAL-022"], [_o(_SEC, 110), _o(_SEC1, 100)], 0.5,
         [_cmp(_SEC1, _SEC, 100, 110, -10, -9.0909, 0.55, "RESTATED")], "PARTIAL", True),
    # A restatement can coexist with AGREED when an independent provider matches the latest value.
    _rec("val.recon.restatement_with_independent_match_is_agreed", ["BR-VAL-020", "BR-VAL-022"], [_o(_SEC, 110), _o(_SEC1, 100), _o(_YAH, 110)], 0.5,
         [_cmp(_SEC1, _SEC, 100, 110, -10, -9.0909, 0.55, "RESTATED"), _cmp(_YAH, _SEC, 110, 110, 0, 0, 0.55, "MATCH")], "AGREED", True),
    _rec("val.recon.one_conflicting_provider_beats_agreeing_one", ["BR-VAL-020", "BR-VAL-022"], [_o(_SEC, 1000), _o(_REL, 1000), _o(_YAH, 2000)], 0.5,
         [_cmp(_REL, _SEC, 1000, 1000, 0, 0, 5, "MATCH"), _cmp(_YAH, _SEC, 2000, 1000, 1000, 100, 5, "MISMATCH")], "CONFLICT", False),
    _rec("val.recon.conflict_and_restatement_together", ["BR-VAL-020", "BR-VAL-022"], [_o(_SEC, 100), _o(_SEC1, 90), _o(_YAH, 120)], 0.5,
         [_cmp(_SEC1, _SEC, 90, 100, -10, -10, 0.5, "RESTATED"), _cmp(_YAH, _SEC, 120, 100, 20, 20, 0.5, "MISMATCH")], "CONFLICT", True),
    _rec("val.recon.nothing_found_is_not_found", ["BR-VAL-020", "BR-VAL-022"],
         [_o(_SEC, None, status="NOT_FOUND"), _o(_REL, None, status="NOT_FOUND_IN_TEXT"), _o(_YAH, None, status="NOT_READ")], 0.5, [], "NOT_FOUND", False),
    _rec("val.recon.unfound_providers_are_ignored", ["BR-VAL-020", "BR-VAL-022"], [_o(_SEC, 100), _o(_REL, None, status="NOT_FOUND_IN_TEXT"), _o(_YAH, None, status="FIELD_EMPTY")], 0.5,
         [], "PARTIAL", False),
    # Zero baseline: percentage is null; no tolerance, so any difference is a mismatch.
    _rec("val.recon.zero_baseline_has_no_percentage", ["BR-VAL-021", "BR-VAL-022"], [_o(_SEC, 0), _o(_YAH, 0)], 0.5, [_cmp(_YAH, _SEC, 0, 0, 0, None, 0, "MATCH")], "AGREED", False),
    _rec("val.recon.zero_baseline_any_difference_conflicts", ["BR-VAL-021", "BR-VAL-022"], [_o(_SEC, 0), _o(_YAH, 5)], 0.5, [_cmp(_YAH, _SEC, 5, 0, 5, None, 0, "MISMATCH")], "CONFLICT", False),
]


# Full path: companyfacts + release text + Yahoo rows. A restated Q2 2025 revenue (20.0M first filed, 21.0M latest) and the Q2 2026 quarter.
_RECON_FACTS = {"facts": {"us-gaap": {"RevenueFromContractWithCustomerIncludingAssessedTax": _usd(
    _f("2025-04-01", "2025-06-30", 20_000_000, "10-Q", "2025-08-11", "q225"),
    _f("2025-04-01", "2025-06-30", 21_000_000, "10-Q", "2026-08-10", "q226"),
    _f("2026-04-01", "2026-06-30", 31_512_000, "10-Q", "2026-08-10", "q226"),
    # The prior fiscal year end that dates the 2025 quarter as FY2025 Q2 (3.0.0, F60: no calendar-quarter form).
    _f("2024-01-01", "2024-12-31", 44_000_000, "10-K", "2025-03-03", "k24"))}}}
_RELEASE = {"status": "READ", "url": "https://www.sec.gov/r.htm", "filingDate": "2026-08-10", "accessionNumber": "r1",
            "text": "Second quarter 2026 revenue was $31.5 million from government and commercial customers."}
_RELEASE_JS = ("const [facts, metric, spec, releases, rows, tol] = args; const mrm = m.metricReconciliation; "
               "return mrm.metricReconciliation({ ticker: 'tst', metric, period: mrm.resolvePeriod(facts, metric, spec), companyfacts: facts, releases, yahooRows: rows, tolerancePct: tol });")


def _rsel(v: dict) -> dict:
    basis = v["agreementBasis"]
    return {"status": v["status"], "restated": v["restated"], "agreeing": basis["providersAgreeingWithSec"], "independentChecks": basis["independentChecks"],
            "found": basis["providersFound"], "missing": v["missingProviders"], "notRead": basis["notRead"], "tolerancePct": v["tolerancePct"],
            "comparisons": [[c["source"], c["result"], c["value"], c["difference"], c["differencePct"], c["tolerance"]] for c in v["comparisons"]]}


def _recon(name: str, ids: list[str], spec: str, releases: list, rows, tol: float, expect: dict, facts: dict = _RECON_FACTS, metric: str = "revenue", **kw) -> Case:
    return Case(name=name, ids=ids,
                py=lambda: mr.metric_reconciliation(ticker="tst", metric=metric, period=mr.resolve_period(facts, metric, spec), companyfacts=facts, releases=copy.deepcopy(releases),
                                                    yahoo_rows=copy.deepcopy(rows), tolerance_pct=tol),
                ts={"js": _RELEASE_JS, "args": [facts, metric, spec, releases, rows, tol]}, select=_rsel, expect=expect, **kw)


def _recon_expect(status, comparisons, *, restated=False, agreeing=(), found=("SEC",), notread=("ALPHA_VANTAGE",), tol=0.5) -> dict:
    missing = [p for p in ("SEC", "ISSUER_RELEASE", "YAHOO") if p not in found]
    return {"status": status, "restated": restated, "agreeing": list(agreeing), "independentChecks": len(agreeing), "found": list(found), "missing": missing,
            "notRead": list(notread), "tolerancePct": tol, "comparisons": comparisons}


RECON_FLOW_CASES = [
    # Card BR-VAL-020: SEC and Yahoo both found revenue within tolerance, release not found -> AGREED, providersAgreeingWithSec ['YAHOO'], independentChecks 1.
    _recon("val.recon.card_020_sec_and_yahoo_agree_release_not_found", ["BR-VAL-020", "BR-VAL-019", "BR-VAL-022"], "latest", [],
           [{"date": "2026-06-30", "totalRevenue": 31_520_000.0}], 0.5,
           _recon_expect("AGREED", [[_SEC1, "MATCH", 31_512_000, 0, 0, 157_560], [_YAH, "MATCH", 31_520_000, 8_000, 0.0254, 157_560]], agreeing=["YAHOO"], found=("SEC", "YAHOO"))),
    # Release text read through the whole path: "$31.5 million" has precision 50,000 and matches 31,512,000 (card BR-VAL-021).
    _recon("val.recon.release_text_precision_through_full_path", ["BR-VAL-021", "BR-VAL-019", "BR-VAL-022"], "latest", [_RELEASE], None, 0.5,
           _recon_expect("AGREED", [[_SEC1, "MATCH", 31_512_000, 0, 0, 157_560], [_REL, "MATCH", 31_500_000, -12_000, -0.0381, 157_560]], agreeing=["ISSUER_RELEASE"], found=("SEC", "ISSUER_RELEASE"))),
    _recon("val.recon.release_and_yahoo_both_agree", ["BR-VAL-019", "BR-VAL-020", "BR-VAL-022"], "latest", [_RELEASE], [{"date": "2026-06-30", "totalRevenue": 31_512_000.0}], 0.5,
           _recon_expect("AGREED", [[_SEC1, "MATCH", 31_512_000, 0, 0, 157_560], [_REL, "MATCH", 31_500_000, -12_000, -0.0381, 157_560], [_YAH, "MATCH", 31_512_000, 0, 0, 157_560]],
                         agreeing=["ISSUER_RELEASE", "YAHOO"], found=("SEC", "ISSUER_RELEASE", "YAHOO"))),
    _recon("val.recon.yahoo_dates_within_7_days_matched", ["BR-VAL-022"], "latest", [], [{"date": "2026-07-07", "totalRevenue": 31_512_000.0}], 0.5,
           _recon_expect("AGREED", [[_SEC1, "MATCH", 31_512_000, 0, 0, 157_560], [_YAH, "MATCH", 31_512_000, 0, 0, 157_560]], agreeing=["YAHOO"], found=("SEC", "YAHOO"))),
    _recon("val.recon.yahoo_row_8_days_off_not_found", ["BR-VAL-022"], "latest", [], [{"date": "2026-07-08", "totalRevenue": 31_512_000.0}], 0.5,
           _recon_expect("PARTIAL", [[_SEC1, "MATCH", 31_512_000, 0, 0, 157_560]])),
    # Two SEC observations are one provider; a restated value alone is PARTIAL and restated, never a conflict.
    _recon("val.recon.restated_sec_value_alone_is_partial", ["BR-VAL-019", "BR-VAL-020", "BR-VAL-022"], "FY2025 Q2", [], None, 0.5,
           _recon_expect("PARTIAL", [[_SEC1, "RESTATED", 20_000_000, -1_000_000, -4.7619, 105_000]], restated=True)),
    # Restatement + AGREED: Yahoo matches the latest (21.0M) value, so the restated first-filed value does not stop AGREED.
    _recon("val.recon.restatement_with_yahoo_match_is_agreed", ["BR-VAL-019", "BR-VAL-020", "BR-VAL-022"], "FY2025 Q2", [], [{"date": "2025-06-30", "totalRevenue": 21_000_000.0}], 0.5,
           _recon_expect("AGREED", [[_SEC1, "RESTATED", 20_000_000, -1_000_000, -4.7619, 105_000], [_YAH, "MATCH", 21_000_000, 0, 0, 105_000]],
                         restated=True, agreeing=["YAHOO"], found=("SEC", "YAHOO"))),
    # Yahoo holding the first-filed value (20.0M) disagrees with the latest SEC value: a conflict as well as a restatement.
    _recon("val.recon.yahoo_with_first_filed_value_conflicts", ["BR-VAL-020", "BR-VAL-022"], "FY2025 Q2", [], [{"date": "2025-06-30", "totalRevenue": 20_000_000.0}], 0.5,
           _recon_expect("CONFLICT", [[_SEC1, "RESTATED", 20_000_000, -1_000_000, -4.7619, 105_000], [_YAH, "MISMATCH", 20_000_000, -1_000_000, -4.7619, 105_000]],
                         restated=True, found=("SEC", "YAHOO"))),
    _recon("val.recon.release_mismatch_beats_yahoo_match", ["BR-VAL-020", "BR-VAL-022"], "latest",
           [{**_RELEASE, "text": "Second quarter 2026 revenue was $33.0 million."}], [{"date": "2026-06-30", "totalRevenue": 31_512_000.0}], 0.5,
           _recon_expect("CONFLICT", [[_SEC1, "MATCH", 31_512_000, 0, 0, 157_560], [_REL, "MISMATCH", 33_000_000, 1_488_000, 4.722, 157_560], [_YAH, "MATCH", 31_512_000, 0, 0, 157_560]],
                         agreeing=["YAHOO"], found=("SEC", "ISSUER_RELEASE", "YAHOO"))),
    _recon("val.recon.nothing_found_net_income_is_not_found", ["BR-VAL-020", "BR-VAL-022"], "latest", [], None, 0.5,
           _recon_expect("NOT_FOUND", [], found=()), metric="net_income"),
]


# ───────────────────────────────────────────────────────────────── parent net income from ProfitLoss (cards BR-VAL-025, 056)

_BE_FILED = ("10-K", "2026-02-26", "be26")
_NCI = "NetIncomeLossAttributableToNoncontrollingInterest"


def _ni_facts(**concepts) -> dict:
    us = {"Revenues": _usd(_f("2025-01-01", "2025-12-31", 2000 * M, *_BE_FILED))}
    us.update({c: _usd(*rows) for c, rows in concepts.items()})
    return {"facts": {"us-gaap": us}}


def _ni_row(val, form="10-K", filed="2026-02-26", accn="be26", start="2025-01-01", end="2025-12-31") -> dict:
    return _f(start, end, val, form, filed, accn)


def _nsel(v: list) -> list:
    out = []
    for o in v:
        ev = o.get("evidence") or {}
        out.append([o["source"], o["status"], o["value"], o.get("basis"), ev.get("concept"), ev.get("profitLoss"), ev.get("noncontrollingInterest"), ev.get("derivation")])
    return out


def _secobs(name: str, ids: list[str], facts: dict, expect: list, metric: str = "net_income", **kw) -> Case:
    return Case(name=name, ids=ids,
                py=lambda: mr.sec_observations(facts, metric, mr.resolve_period(facts, metric, "FY2025")),
                ts={"js": "const p = m.metricReconciliation.resolvePeriod(args[0], args[1], 'FY2025'); return m.metricReconciliation.secObservations(args[0], args[1], p);",
                    "args": [facts, metric]},
                select=_nsel, expect=expect, **kw)


_DERIVATION = "ProfitLoss - NetIncomeLossAttributableToNoncontrollingInterest"


def _derived(value, profit_loss, nci) -> list:
    return [["SEC_XBRL_LATEST", "FOUND", value, "PARENT_DERIVED_FROM_PROFITLOSS", "ProfitLoss", profit_loss, nci, _DERIVATION],
            ["SEC_XBRL_AS_FIRST_FILED", "FOUND", value, "PARENT_DERIVED_FROM_PROFITLOSS", "ProfitLoss", profit_loss, nci, _DERIVATION]]


def _raw(concept, latest, first) -> list:
    return [["SEC_XBRL_LATEST", "FOUND", latest, None, concept, None, None, None], ["SEC_XBRL_AS_FIRST_FILED", "FOUND", first, None, concept, None, None, None]]


_BE_DECIDED = dict(decision="Q1 BR-VAL-025, owner 2026-10-30")


def _unresolved() -> list:
    """3.4.0: no matched NCI, so the parent's net income is NOT_RESOLVED; the ProfitLoss row stays as evidence."""
    return [["SEC_XBRL_LATEST", "NOT_RESOLVED", None, None, "ProfitLoss", None, None, None],
            ["SEC_XBRL_AS_FIRST_FILED", "NOT_RESOLVED", None, None, "ProfitLoss", None, None, None]]


NET_INCOME_CASES = [
    # Card BR-VAL-025: ProfitLoss 120M is the chosen concept, NCI 5M in the same accession, period and unit -> 115M, PARENT_DERIVED_FROM_PROFITLOSS.
    _secobs("val.recon.net_income.card_025_profit_loss_less_nci", ["BR-VAL-025", "BR-VAL-056"],
            _ni_facts(ProfitLoss=[_ni_row(120 * M)], **{_NCI: [_ni_row(5 * M)]}), _derived(115 * M, 120 * M, 5 * M)),
    # Also when NetIncomeLoss is tagged, if ProfitLoss was filed more recently (a 10-K/A with its own NCI row).
    _secobs("val.recon.net_income.profit_loss_filed_later_than_net_income_loss", ["BR-VAL-025", "BR-VAL-056"],
            _ni_facts(NetIncomeLoss=[_ni_row(100 * M)], ProfitLoss=[_ni_row(110 * M, "10-K/A", "2026-03-15", "be26a")], **{_NCI: [_ni_row(5 * M, "10-K/A", "2026-03-15", "be26a")]}),
            _derived(105 * M, 110 * M, 5 * M)),
    # A tie on filing date goes to the earlier-listed concept (NetIncomeLoss): no derivation.
    _secobs("val.recon.net_income.same_filing_tie_goes_to_net_income_loss", ["BR-VAL-025", "BR-VAL-056"],
            _ni_facts(NetIncomeLoss=[_ni_row(100 * M)], ProfitLoss=[_ni_row(110 * M)], **{_NCI: [_ni_row(5 * M)]}), _raw("NetIncomeLoss", 100 * M, 100 * M)),
    # The first-filed and latest derivations each use their own filing's NCI: ProfitLoss restated 118M -> 120M, NCI 4M -> 5M.
    _secobs("val.recon.net_income.restated_profit_loss_uses_each_filings_nci", ["BR-VAL-025", "BR-VAL-056"],
            _ni_facts(ProfitLoss=[_ni_row(118 * M), _ni_row(120 * M, "10-K/A", "2026-03-15", "be26a")],
                      **{_NCI: [_ni_row(4 * M), _ni_row(5 * M, "10-K/A", "2026-03-15", "be26a")]}),
            [["SEC_XBRL_LATEST", "FOUND", 115 * M, "PARENT_DERIVED_FROM_PROFITLOSS", "ProfitLoss", 120 * M, 5 * M, _DERIVATION],
             ["SEC_XBRL_AS_FIRST_FILED", "FOUND", 114 * M, "PARENT_DERIVED_FROM_PROFITLOSS", "ProfitLoss", 118 * M, 4 * M, _DERIVATION]]),
    # 3.4.0 (Q1 BR-VAL-025, owner 2026-10-30 "Withhold the figure"): with no NCI tagged, ProfitLoss includes the
    # noncontrolling interest, so the parent's net income is NOT_RESOLVED in both rows. Before, raw ProfitLoss was reported.
    _secobs("val.recon.net_income.missing_nci_leaves_parent_net_income_unresolved", ["BR-VAL-025", "BR-VAL-056"], _ni_facts(ProfitLoss=[_ni_row(120 * M)]),
            _unresolved(), **_BE_DECIDED),
    # Both the latest and the first-filed row need a matched NCI: here only the latest has one, so both are unresolved.
    _secobs("val.recon.net_income.nci_missing_for_first_filed_row_leaves_both_unresolved", ["BR-VAL-025", "BR-VAL-056"],
            _ni_facts(ProfitLoss=[_ni_row(118 * M), _ni_row(120 * M, "10-K/A", "2026-03-15", "be26a")], **{_NCI: [_ni_row(5 * M, "10-K/A", "2026-03-15", "be26a")]}),
            _unresolved(), **_BE_DECIDED),
    # NCI must come from the same accession (a different filing's figure is not matched).
    _secobs("val.recon.net_income.nci_from_other_filing_leaves_parent_net_income_unresolved", ["BR-VAL-025", "BR-VAL-056"],
            _ni_facts(ProfitLoss=[_ni_row(120 * M)], **{_NCI: [_ni_row(5 * M, accn="other")]}), _unresolved(), **_BE_DECIDED),
    # ... and for the same period start and end.
    _secobs("val.recon.net_income.nci_for_other_period_leaves_parent_net_income_unresolved", ["BR-VAL-025", "BR-VAL-056"],
            _ni_facts(ProfitLoss=[_ni_row(120 * M)], **{_NCI: [_ni_row(5 * M, end="2025-09-30")]}), _unresolved(), **_BE_DECIDED),
    # Finding (Phase 2, for Q3i triage): IFRS ProfitLoss (which includes non-controlling interests) is reported as net income without any
    # derivation when the owners-of-parent concept is not tagged; the derivation is US-GAAP only. Both runtimes agree; recorded as current behaviour.
    _secobs("val.recon.net_income.ifrs_profit_loss_is_not_derived", ["BR-VAL-025", "BR-VAL-056"],
            {"facts": {"ifrs-full": {"Revenue": {"units": {"TWD": [_ni_row(2000 * M)]}}, "ProfitLoss": {"units": {"TWD": [_ni_row(120 * M)]}}}}},
            _raw("ProfitLoss", 120 * M, 120 * M)),
    # Revenue concept tie-break (separate behaviour in the same function): same-day concepts, the larger magnitude wins.
    _secobs("val.recon.net_income.revenue_same_filing_larger_magnitude_wins", ["BR-VAL-025"],
            {"facts": {"us-gaap": {"RevenueFromContractWithCustomerExcludingAssessedTax": _usd(_ni_row(2_001_614_000)), "Revenues": _usd(_ni_row(2_023_994_000))}}},
            [["SEC_XBRL_LATEST", "FOUND", 2_023_994_000, None, "Revenues", None, None, None], ["SEC_XBRL_AS_FIRST_FILED", "FOUND", 2_023_994_000, None, "Revenues", None, None, None]],
            metric="revenue"),
]

# Recorded BE (Bloom Energy) FY2025 10-K: net income tagged only as ProfitLoss (-87,140,000) with +1,294,000 attributable to noncontrolling interests.
NET_INCOME_CASES.append(
    _secobs("val.recon.net_income.recorded_be_profit_loss_less_nci", ["BR-VAL-025", "BR-VAL-056"], fixture("sec/companyfacts_BE.json"),
            [["SEC_XBRL_LATEST", "FOUND", -88_434_000, "PARENT_DERIVED_FROM_PROFITLOSS", "ProfitLoss", -87_140_000, 1_294_000, _DERIVATION],
             ["SEC_XBRL_AS_FIRST_FILED", "FOUND", -88_434_000, "PARENT_DERIVED_FROM_PROFITLOSS", "ProfitLoss", -87_140_000, 1_294_000, _DERIVATION]]))


# ───────────────────────────────────────────────────────────────── reconcile_metric_sources tolerance input (Q3e: cards BR-VAL-019, 020, 063)
# The clamp lives in the tool wrappers (server.py reconcile_metric_sources, yahoo-finance.ts reconcileMetricSources), so these cases run the whole
# tool against canned SEC responses and compare the tolerance the result echoes. Yahoo statements are unavailable on both sides (only SEC is read).

_TOL_ROUTES = {
    "https://www.sec.gov/files/company_tickers.json": {"json": {"0": {"cik_str": 1234567, "ticker": "TST", "title": "Test Co"}}},
    "https://data.sec.gov/submissions/CIK0001234567.json": {"json": {"cik": "1234567", "filings": {"recent": {
        "accessionNumber": [], "filingDate": [], "form": [], "primaryDocument": [], "items": [], "reportDate": []}}}},
    "https://data.sec.gov/api/xbrl/companyfacts/CIK0001234567.json": {"json": _RECON_FACTS},
}
_NAN, _INF = float("nan"), float("inf")
_OMIT = "__omit__"


def _tolerances_py(values: list) -> list:
    """The tolerancePct echoed by server.reconcile_metric_sources for each caller value (no network: CIK cached, Yahoo statements unavailable)."""
    import asyncio
    import json
    from unittest.mock import patch

    import server
    from yfmcp.clients import edgar
    from yfmcp.tools import valuation as valuation_tools

    def no_yahoo(*_a, **_k):
        raise RuntimeError("Yahoo statements unavailable")

    out = []
    with patch.dict(edgar._FILING_CIK_CACHE, {"TST": "0001234567"}), patch.object(valuation_tools, "_yahoo_statement_rows_sync", no_yahoo):
        for v in values:
            args = ("TST", "revenue", "latest") + (() if v == _OMIT else (v,))
            result = json.loads(asyncio.run(server.reconcile_metric_sources(*args)))
            out.append(result.get("tolerancePct", result))
    return out


_TOLERANCES_JS = ("const out = []; for (const v of args[0]) { const t = v === '__omit__' ? [] : [v === 'NaN' ? NaN : v === 'Infinity' ? Infinity : v === '-Infinity' ? -Infinity : v]; "
                  "const r = JSON.parse(await m.yahooFinance.reconcileMetricSources('TST', 'revenue', 'latest', ...t)); out.push(r.tolerancePct ?? r); } return out;")


def _tolerances(name: str, ids: list[str], values: list, **kw) -> Case:
    js_values = [("NaN" if v != v else "Infinity" if v == _INF else "-Infinity" if v == -_INF else v) if isinstance(v, float) else v for v in values]
    return Case(name=name, ids=ids, py=lambda: _tolerances_py(values), ts={"js": _TOLERANCES_JS, "args": [js_values], "isolate": True}, routes=_TOL_ROUTES, **kw)


TOLERANCE_CASES = [
    # In range, out of range, default (argument omitted), and values that are not numbers: clamped to 0-10, else 0.5.
    _tolerances("val.recon.tolerance_clamp_finite_and_unusable_values", ["BR-VAL-021", "BR-VAL-022", "BR-VAL-063"], [15.0, -1.0, 3.0, 10.0, 0.0, _OMIT, "abc", None],
                expect=[10, 0, 3, 10, 0, 0.5, 0.5, 0.5]),
    # Q3e (3.2.1): NaN and both infinities are the 0.5 default in both runtimes. Python used to clamp before its finiteness check
    # (NaN -> 0, +inf -> 10).
    _tolerances("val.recon.q3e_tolerance_nan_and_infinity_are_the_default", ["BR-VAL-019", "BR-VAL-063", "BR-VAL-021"], [_NAN, _INF, -_INF],
                expect=[0.5, 0.5, 0.5]),
    # Q3i triage: a numeric string is not a number, so it is the 0.5 default in both (Python's float() used to coerce "5" to 5.0).
    _tolerances("val.recon.q3i_tolerance_numeric_string_is_the_default", ["BR-VAL-063"], ["5", True],
                expect=[0.5, 0.5]),
]


def _agreed_wording_py() -> str:
    import asyncio

    import server
    tool = next(t for t in asyncio.run(server.yfinance_server.list_tools()) if t.name == "reconcile_metric_sources")
    return tool.description


_AGREED_PHRASE = re.compile(r"Status AGREED.*?with source evidence")

DESCRIPTION_CASES = [
    # Q3e (card BR-VAL-020, 3.2.1): both descriptions say what both code paths do: AGREED needs the latest SEC value plus at least
    # one independent matching provider. The Worker used to say "two or more providers agree".
    Case(name="val.recon.q3e_tool_description_agreed_wording", ids=["BR-VAL-020", "BR-VAL-019"],
         py=_agreed_wording_py,
         ts={"js": "return m.tools.TOOLS.find((t) => t.name === 'reconcile_metric_sources').description;"},
         select=lambda v: _AGREED_PHRASE.search(v).group(0),
         expect="Status AGREED requires latest SEC plus at least one independent matching provider; PARTIAL covers a missing SEC baseline or only one provider; "
                "otherwise CONFLICT or NOT_FOUND, with source evidence"),
]

CASES = (ADR_CASES + SHARE_BASIS_CASES + BALANCE_CASES + CURRENCY_CASES + EV_CASES + EV_CASES_HIST + BAL_HIST_CASES + PRICE_CASES + MCAP_CASES + COVER_CASES
         + COVER_PARSE_CASES + FLOW_CASES + PIT_CASES + RECORDED_CASES + RECON_OBS_CASES + RECON_FLOW_CASES + NET_INCOME_CASES + TOLERANCE_CASES + DESCRIPTION_CASES)
