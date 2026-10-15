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

    # ── Q3i: the convertible principal leaves debt even when the SEC share count is not used (owner decision, repair scheduled) ──
    # Today: SHARE_BASIS_MISMATCH puts Yahoo's 200M basic shares in the equity value, yet the 300M convertible principal still leaves debt
    # (380M - 300M) although no conversion shares were added. EV = 25 x 200M + 380M - 300M - 140M = 4,940M; with the conversion shares
    # missing and the debt reduced, the convertible is in neither.
    _snap("val.ev.pinned_convertible_deducted_when_sec_share_count_rejected", ["BR-VAL-046", "BR-VAL-074", "BR-VAL-082"],
          _mk(sharesOutstanding=200_000_000), bridge=_bridge(400_000_000, 410_000_000, "10-K", components=[_conv(300_000_000, 15_000_000)]),
          capital=_cap(140_000_000, 380_000_000, period_end="2025-03-31"), select=_EV_SEL,
          mode="pinned", decision="Q3i",
          expect={"equityValue": 5_000_000_000, "enterpriseValue": 4_940_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 300_000_000,
                  "balances.preferredLiquidationPreferenceInEv": 0, "shares.usedForValue": 200_000_000, "warningCodes": ["SHARE_BASIS_MISMATCH"]}),
    # Same defect with a usable SEC count but no diluted count in the bridge: equity uses the basic count, principal still leaves debt.
    _snap("val.ev.pinned_convertible_deducted_when_bridge_has_no_diluted_count", ["BR-VAL-046", "BR-VAL-074", "BR-VAL-082"],
          _mk(price=20.0, **_SAME_SIZE_MKT), bridge=_bridge(100_000_000, None, components=[_conv(300_000_000, 15_000_000)]),
          capital=_cap(200_000_000, 500_000_000, sti=100_000_000), select=_EV_SEL,
          mode="pinned", decision="Q3i",
          expect={"equityValue": 2_000_000_000, "enterpriseValue": 1_900_000_000, "status": "COMPUTED", "balances.convertibleDebtCountedAsShares": 300_000_000,
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
         "multiples.LTM.priceToEarnings.status": "NOT_MEANINGFUL_NONPOSITIVE_DENOMINATOR" if False else None, "multiples.LTM.priceToEarnings.value": None,
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
    # Q1 BR-VAL-006 (owner decision): debt is the FIRST group with at least one component tagged at the cash date, even when the group
    # is only partly populated. Only LongTermDebtCurrent (30M) is tagged at 2025-06-30; LongTermDebtNoncurrent (280M) is tagged only
    # at the prior year end, so debt reads 30M and EV is understated by the missing 280M.
    _hv("val.hist.balances.pinned_partial_debt_group_uses_tagged_component_only", ["BR-VAL-006", "BR-VAL-008"],
        _inp(_facts(_us(CashAndCashEquivalentsAtCarryingValue=_cash(), LongTermDebtCurrent=[_f(None, "2025-06-30", 30 * M, *Q225)],
                        LongTermDebtNoncurrent=[_f(None, "2024-12-31", 280 * M, *Q225)]), _COVER())), select=_bsel,
        mode="pinned", decision="Q1 BR-VAL-006",
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
        us=_us(CommonStockSharesOutstanding=None) if False else {**_us(), "CommonStockSharesOutstanding": {"units": {"shares": [_f(None, "2025-06-30", 60 * M, *Q225)]}}},
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

CASES = ADR_CASES + SHARE_BASIS_CASES + BALANCE_CASES + CURRENCY_CASES + EV_CASES + EV_CASES_HIST + BAL_HIST_CASES + PRICE_CASES + MCAP_CASES + COVER_CASES + COVER_PARSE_CASES
