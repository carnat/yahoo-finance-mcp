#!/usr/bin/env python3
"""Options data quality unit tests.

Tests for:
- _compute_data_quality: ASTS-like synthetic chain with zero OI/bid/ask/placeholder IV
- _sort_by_relevance: relevance sort does not return deep ITM strikes first
- get_options_summary/get_options_flow_scan helpers for maxPainStrike and atmIV guards

All offline — no network calls required.
Run: PYTHONPATH=. python scripts/test_options_quality.py
"""

import os
import sys
import json
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Patch FastMCP.tool to accept output_schema (not in mcp>=1.9)
from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402
_orig_tool = _FastMCP.tool
def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
    return _orig_tool(self, name=name, **kwargs)
_FastMCP.tool = _patched_tool  # type: ignore[method-assign]

import server as srv


def _make_contract(
    strike: float,
    bid: float = 0.0,
    ask: float = 0.0,
    open_interest: int = 0,
    volume: int = 0,
    implied_volatility: float = 0.0,
    last_trade_date: str | None = None,
    in_the_money: bool = False,
) -> dict:
    c: dict = {
        "strike": strike,
        "bid": bid,
        "ask": ask,
        "openInterest": open_interest,
        "volume": volume,
        "impliedVolatility": implied_volatility,
        "inTheMoney": in_the_money,
    }
    if last_trade_date is not None:
        c["lastTradeDate"] = last_trade_date
    return c


class TestComputeDataQuality(unittest.TestCase):
    """Tests for _compute_data_quality."""

    def test_empty_contracts_returns_low(self):
        dq = srv._compute_data_quality([], "2025-01-10")
        self.assertEqual(dq["quality"], "LOW")
        self.assertIn("NO_CONTRACTS_RETURNED", dq["warnings"])
        self.assertEqual(dq["returnedContracts"], 0)

    def test_asts_like_chain_zero_oi_bid_ask_placeholder_iv_is_low(self):
        """ASTS-like chain: all zero bid/ask, zero OI, placeholder IV => quality LOW."""
        contracts = [
            _make_contract(strike=s, bid=0, ask=0, open_interest=0, implied_volatility=0.00001)
            for s in range(10, 60, 5)
        ]
        dq = srv._compute_data_quality(contracts, "2025-05-15")
        self.assertEqual(dq["quality"], "LOW")
        self.assertEqual(dq["zeroBidAskCount"], len(contracts))
        self.assertEqual(dq["zeroOpenInterestCount"], len(contracts))
        self.assertEqual(dq["placeholderIvCount"], len(contracts))
        self.assertIn("MAJORITY_ZERO_BID_ASK", dq["warnings"])
        self.assertIn("MAJORITY_ZERO_OPEN_INTEREST", dq["warnings"])
        self.assertIn("MAJORITY_PLACEHOLDER_IV", dq["warnings"])

    def test_high_quality_chain(self):
        """All contracts have real bid/ask, OI, and valid IV => quality HIGH."""
        contracts = [
            _make_contract(strike=s, bid=1.0, ask=1.1, open_interest=500, implied_volatility=0.45)
            for s in range(20, 30)
        ]
        dq = srv._compute_data_quality(contracts, "2025-05-15")
        self.assertEqual(dq["quality"], "HIGH")
        self.assertEqual(dq["zeroBidAskCount"], 0)
        self.assertEqual(dq["zeroOpenInterestCount"], 0)
        self.assertEqual(dq["placeholderIvCount"], 0)
        self.assertEqual(dq["warnings"], [])

    def test_medium_quality_chain(self):
        """~40% of contracts have issues => quality MEDIUM."""
        good = [
            _make_contract(strike=float(s), bid=1.0, ask=1.1, open_interest=100, implied_volatility=0.3)
            for s in range(1, 7)  # 6 good
        ]
        bad = [
            _make_contract(strike=float(s), bid=0, ask=0, open_interest=0, implied_volatility=0.0)
            for s in range(7, 11)  # 4 bad
        ]
        # Each bad contract has zeroBidAsk=1, zeroOI=1, placeholderIv=1
        # Fractions: zeroBidAsk=4/10=0.40, zeroOI=4/10=0.40, placeholderIV=4/10=0.40
        # Each > 0.30 (MEDIUM threshold) but < 0.50 (LOW threshold) => MEDIUM quality
        dq = srv._compute_data_quality(good + bad, "2025-05-15")
        self.assertEqual(dq["quality"], "MEDIUM")

    def test_stale_trade_count(self):
        """Contracts older than threshold => staleLastTradeCount increases."""
        contracts = [
            _make_contract(strike=10.0, bid=1.0, ask=1.1, open_interest=100,
                           implied_volatility=0.4, last_trade_date="2025-01-01")
        ]
        # data_date is 2025-05-15, so age = ~134 days >> 5 day threshold
        dq = srv._compute_data_quality(contracts, "2025-05-15", stale_days_threshold=5)
        self.assertEqual(dq["staleLastTradeCount"], 1)

    def test_stale_majority_warning(self):
        """Majority of stale trades triggers warning."""
        contracts = [
            _make_contract(strike=float(i), bid=1.0, ask=1.1, open_interest=100,
                           implied_volatility=0.4, last_trade_date="2025-01-01")
            for i in range(10)
        ]
        dq = srv._compute_data_quality(contracts, "2025-05-15", stale_days_threshold=5)
        self.assertIn("MAJORITY_STALE_LAST_TRADE", dq["warnings"])

    def test_epoch_seconds_lasttradedate_not_falsely_stale(self):
        """Yahoo Finance returns epoch seconds (e.g. 1746057600).
        Must NOT be treated as 1970-era stale after the /1000 bug."""
        import datetime as dt
        # 2026-04-01 in epoch seconds
        epoch_seconds = int(dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc).timestamp())
        self.assertLess(epoch_seconds, 1e10, "Epoch-seconds fixture must be < 1e10")
        c = _make_contract(strike=10.0, bid=1.0, ask=1.1, open_interest=100, implied_volatility=0.4)
        c["lastTradeDate"] = epoch_seconds
        # data_date 2026-05-15; age ~44 days; stale threshold 60 => NOT stale
        dq = srv._compute_data_quality([c], "2026-05-15", stale_days_threshold=60)
        self.assertEqual(dq["staleLastTradeCount"], 0,
            "Epoch-second lastTradeDate (< 1e10) must not be treated as 1970-era stale")

    def test_epoch_milliseconds_lasttradedate_not_falsely_stale(self):
        """Some sources return epoch milliseconds (> 1e10).
        Must be divided by 1000 before converting to a date."""
        import datetime as dt
        # 2026-04-01 in epoch milliseconds
        epoch_ms = int(dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc).timestamp()) * 1000
        self.assertGreater(epoch_ms, 1e10, "Epoch-ms fixture must be > 1e10")
        c = _make_contract(strike=10.0, bid=1.0, ask=1.1, open_interest=100, implied_volatility=0.4)
        c["lastTradeDate"] = epoch_ms
        # data_date 2026-05-15; age ~44 days; stale threshold 60 => NOT stale
        dq = srv._compute_data_quality([c], "2026-05-15", stale_days_threshold=60)
        self.assertEqual(dq["staleLastTradeCount"], 0,
            "Epoch-ms lastTradeDate (> 1e10) must be divided by 1000 before date conversion")


class TestMaxPainZeroOI(unittest.TestCase):
    """maxPainStrike should be null when all OI is zero."""

    def _run_max_pain(self, contracts_calls, contracts_puts):
        """Simulate the max pain logic from get_options_summary/get_options_flow_scan."""
        import pandas as pd
        calls = pd.DataFrame(contracts_calls)
        puts = pd.DataFrame(contracts_puts)
        call_oi = float(calls["openInterest"].sum()) if "openInterest" in calls.columns else 0
        put_oi = float(puts["openInterest"].sum()) if "openInterest" in puts.columns else 0
        flow_warnings = []
        max_pain_strike = None
        if call_oi + put_oi <= 0:
            flow_warnings.append("MAX_PAIN_UNAVAILABLE_ZERO_OI")
        else:
            all_strikes = sorted(set(calls["strike"].tolist() + puts["strike"].tolist()))
            min_pain = float("inf")
            for s in all_strikes:
                cp = float(((s - calls["strike"]).clip(lower=0) * calls.get("openInterest", 0)).sum())
                pp = float(((puts["strike"] - s).clip(lower=0) * puts.get("openInterest", 0)).sum())
                if cp + pp < min_pain:
                    min_pain = cp + pp
                    max_pain_strike = s
        return max_pain_strike, flow_warnings

    def test_all_zero_oi_produces_null_max_pain(self):
        calls = [_make_contract(s, open_interest=0) for s in [10.0, 12.0, 15.0, 20.0]]
        puts = [_make_contract(s, open_interest=0) for s in [10.0, 12.0, 15.0, 20.0]]
        max_pain, warnings = self._run_max_pain(calls, puts)
        self.assertIsNone(max_pain)
        self.assertIn("MAX_PAIN_UNAVAILABLE_ZERO_OI", warnings)

    def test_nonzero_oi_produces_max_pain(self):
        calls = [_make_contract(s, open_interest=100 if s == 15.0 else 10) for s in [10.0, 12.0, 15.0, 20.0]]
        puts = [_make_contract(s, open_interest=50) for s in [10.0, 12.0, 15.0, 20.0]]
        max_pain, warnings = self._run_max_pain(calls, puts)
        self.assertIsNotNone(max_pain)
        self.assertNotIn("MAX_PAIN_UNAVAILABLE_ZERO_OI", warnings)


class TestAtmIVPlaceholder(unittest.TestCase):
    """atmIV should be null when placeholder IV dominates ATM contract."""

    def test_placeholder_atm_iv_returns_none(self):
        """Contract with IV <= 0.0001 should produce atmIV=None."""
        atm_iv_raw = 0.00001  # placeholder
        atm_iv = atm_iv_raw if atm_iv_raw > srv._PLACEHOLDER_IV_THRESHOLD else None
        self.assertIsNone(atm_iv)

    def test_valid_atm_iv_passes(self):
        atm_iv_raw = 0.85
        atm_iv = atm_iv_raw if atm_iv_raw > srv._PLACEHOLDER_IV_THRESHOLD else None
        self.assertIsNotNone(atm_iv)
        self.assertAlmostEqual(atm_iv, 0.85)

    def test_boundary_iv_exactly_threshold_is_placeholder(self):
        atm_iv_raw = srv._PLACEHOLDER_IV_THRESHOLD
        atm_iv = atm_iv_raw if atm_iv_raw > srv._PLACEHOLDER_IV_THRESHOLD else None
        self.assertIsNone(atm_iv)


class TestSortByRelevance(unittest.TestCase):
    """Relevance sort should not put deep ITM strikes first by default."""

    def _make_deep_itm_chain(self, underlying: float) -> list[dict]:
        """Simulate a chain where deep ITM contracts are first alphabetically by strike."""
        contracts = []
        # Deep ITM: low strike, zero bid/ask/OI/IV (like Yahoo Finance for illiquid deep ITM)
        for s in [1.0, 2.0, 3.0, 4.0, 5.0]:
            contracts.append(_make_contract(
                strike=s, bid=0.0, ask=0.0, open_interest=0, volume=0,
                implied_volatility=0.0, in_the_money=True
            ))
        # ATM/near-money: good contracts
        for s in [underlying - 2, underlying - 1, underlying, underlying + 1, underlying + 2]:
            contracts.append(_make_contract(
                strike=s, bid=1.5, ask=1.6, open_interest=500, volume=200,
                implied_volatility=0.55, in_the_money=(s < underlying)
            ))
        return contracts

    def test_relevance_sort_puts_valid_quotes_first(self):
        """Relevance sort: valid-quote contracts rank above zero-bid-ask contracts."""
        underlying = 30.0
        contracts = self._make_deep_itm_chain(underlying)
        sorted_contracts = srv._sort_by_relevance(contracts, underlying)
        # First contract should NOT be strike=1.0 (deep ITM zero-liquid)
        first_strike = sorted_contracts[0]["strike"]
        self.assertNotEqual(first_strike, 1.0,
            "Deep ITM zero-liquid contract should not be ranked first by relevance")
        # First contract should have valid bid/ask
        self.assertGreater(sorted_contracts[0]["bid"], 0,
            "First contract after relevance sort should have bid > 0")

    def test_relevance_sort_deep_itm_zero_liquid_at_end(self):
        """Deep ITM zero-liquid contracts should rank after valid contracts."""
        underlying = 30.0
        contracts = self._make_deep_itm_chain(underlying)
        sorted_contracts = srv._sort_by_relevance(contracts, underlying)
        # All zero-bid-ask, zero-OI contracts should be after all valid ones
        valid_indices = [i for i, c in enumerate(sorted_contracts) if c["bid"] > 0]
        invalid_indices = [i for i, c in enumerate(sorted_contracts) if c["bid"] == 0]
        if valid_indices and invalid_indices:
            self.assertLess(max(valid_indices), min(invalid_indices),
                "All valid-bid contracts should come before zero-bid contracts in relevance sort")

    def test_relevance_sort_among_valid_quotes_closer_atm_first(self):
        """Among valid-quote contracts, closer-to-ATM should rank first."""
        underlying = 25.0
        contracts = [
            _make_contract(strike=10.0, bid=15.0, ask=15.5, open_interest=50, volume=10, implied_volatility=0.3, in_the_money=True),
            _make_contract(strike=24.0, bid=1.5, ask=1.6, open_interest=500, volume=200, implied_volatility=0.5),
            _make_contract(strike=26.0, bid=1.2, ask=1.3, open_interest=300, volume=150, implied_volatility=0.5),
        ]
        sorted_contracts = srv._sort_by_relevance(contracts, underlying)
        # Near ATM should come first among valid-bid contracts
        # strike 24 or 26 should beat strike 10 (deep ITM)
        self.assertIn(sorted_contracts[0]["strike"], [24.0, 26.0])

    def test_sort_by_strike_gives_ascending_order(self):
        """When sort_by=strike, contracts return in ascending strike order."""
        contracts = [
            _make_contract(50.0), _make_contract(10.0), _make_contract(30.0),
        ]
        # Simulate the strike sort from get_option_chain
        sorted_contracts = sorted(contracts, key=lambda c: c["strike"])
        self.assertEqual([c["strike"] for c in sorted_contracts], [10.0, 30.0, 50.0])

    def test_default_50_contracts_not_all_deep_itm(self):
        """With 60 deep-ITM zero-liquid + 10 near-money good contracts,
        relevance sort top-50 should include the near-money good contracts."""
        underlying = 100.0
        contracts = []
        # 60 deep ITM zero-liquid
        for s in range(1, 61):
            contracts.append(_make_contract(float(s), bid=0, ask=0, open_interest=0, implied_volatility=0.0))
        # 10 near-money good
        for s in range(95, 106):
            contracts.append(_make_contract(float(s), bid=2.0, ask=2.1, open_interest=300, volume=100, implied_volatility=0.4))

        sorted_contracts = srv._sort_by_relevance(contracts, underlying)
        top50 = sorted_contracts[:50]
        top50_strikes = [c["strike"] for c in top50]
        # Near-money good contracts (95-105) should all appear in top 50
        for s in range(95, 106):
            self.assertIn(float(s), top50_strikes,
                f"Near-money strike {s} should be in top 50 after relevance sort")


class TestIVPctilePlaceholder(unittest.TestCase):
    """IV percentile should be null when placeholder IV dominates."""

    def test_placeholder_iv_pctile_suppression(self):
        """When majority of IVs are placeholder, IV_PERCENTILE_UNAVAILABLE_PLACEHOLDER_IV warning."""
        n = 20
        # 18 placeholder IV contracts + 2 valid
        contracts = [
            _make_contract(float(i), bid=0, ask=0, open_interest=0, implied_volatility=0.00001)
            for i in range(18)
        ] + [
            _make_contract(float(i), bid=1.0, ask=1.1, open_interest=100, implied_volatility=0.5)
            for i in range(18, 20)
        ]
        dq = srv._compute_data_quality(contracts, "2025-05-15")
        # Majority placeholder IV
        self.assertGreater(dq["placeholderIvCount"], n * 0.5)
        self.assertIn("MAJORITY_PLACEHOLDER_IV", dq["warnings"])


class TestDataQualityDimensionThresholds(unittest.TestCase):
    """Stronger per-dimension classification tests."""

    def test_100pct_zero_oi_but_good_bid_ask_and_iv_is_still_low(self):
        """100% zero OI (> 0.80 threshold) should be LOW even if bid/ask and IV are fine."""
        contracts = [
            _make_contract(float(i), bid=1.0, ask=1.1, open_interest=0, implied_volatility=0.4)
            for i in range(10)
        ]
        dq = srv._compute_data_quality(contracts, "2025-05-15")
        self.assertEqual(dq["quality"], "LOW",
            "100% zero OI should be LOW quality (zeroOI/n=1.0 > 0.80 threshold)")

    def test_60pct_zero_oi_is_medium(self):
        """60% zero OI (> 0.50 but ≤ 0.80) should be MEDIUM."""
        good = [_make_contract(float(i), bid=1.0, ask=1.1, open_interest=100, implied_volatility=0.4) for i in range(4)]
        bad = [_make_contract(float(i + 100), bid=1.0, ask=1.1, open_interest=0, implied_volatility=0.4) for i in range(6)]
        dq = srv._compute_data_quality(good + bad, "2025-05-15")
        self.assertEqual(dq["quality"], "MEDIUM",
            "60% zero OI (> 0.50 threshold) should be MEDIUM quality")

    def test_55pct_zero_bid_ask_is_low(self):
        """55% zero bid/ask (> 0.50 threshold) should be LOW."""
        good = [_make_contract(float(i), bid=1.0, ask=1.1, open_interest=100, implied_volatility=0.4) for i in range(9)]
        bad = [_make_contract(float(i + 100), bid=0, ask=0, open_interest=100, implied_volatility=0.4) for i in range(11)]
        dq = srv._compute_data_quality(good + bad, "2025-05-15")
        self.assertEqual(dq["quality"], "LOW",
            "55% zero bid/ask should be LOW quality")

    def test_35pct_placeholder_iv_with_good_oi_is_medium(self):
        """35% placeholder IV (> 0.30 threshold) with good OI/bid should be MEDIUM."""
        good = [_make_contract(float(i), bid=1.0, ask=1.1, open_interest=100, implied_volatility=0.4) for i in range(13)]
        placeholder = [_make_contract(float(i + 100), bid=1.0, ask=1.1, open_interest=100, implied_volatility=0.00001) for i in range(7)]
        dq = srv._compute_data_quality(good + placeholder, "2025-05-15")
        self.assertEqual(dq["quality"], "MEDIUM",
            "35% placeholder IV (> 0.30 threshold) should be MEDIUM quality")


class TestIncludeIlliquid(unittest.TestCase):
    """Tests for include_illiquid filter behavior."""

    def _filter_illiquid(self, contracts: list[dict], include_illiquid: bool) -> list[dict]:
        """Simulate the include_illiquid filter from get_option_chain."""
        if include_illiquid:
            return contracts
        return [
            c for c in contracts
            if not (c.get("bid", 0) == 0 and c.get("ask", 0) == 0 and c.get("openInterest", 0) == 0)
        ]

    def test_include_illiquid_false_removes_zero_liquid(self):
        """include_illiquid=False should remove contracts with zero bid/ask AND zero OI."""
        contracts = [
            _make_contract(10.0, bid=0, ask=0, open_interest=0),   # illiquid — removed
            _make_contract(15.0, bid=1.0, ask=1.1, open_interest=100),  # liquid — kept
            _make_contract(20.0, bid=0, ask=0, open_interest=50),   # zero bid/ask but has OI — kept
            _make_contract(25.0, bid=0.5, ask=0.6, open_interest=0), # has bid/ask but zero OI — kept
        ]
        filtered = self._filter_illiquid(contracts, include_illiquid=False)
        strikes = [c["strike"] for c in filtered]
        self.assertNotIn(10.0, strikes, "Zero bid+ask+OI should be removed when include_illiquid=False")
        self.assertIn(15.0, strikes)
        self.assertIn(20.0, strikes, "Contract with OI>0 should be kept even if bid/ask=0")
        self.assertIn(25.0, strikes, "Contract with bid/ask>0 should be kept even if OI=0")

    def test_include_illiquid_true_keeps_all(self):
        """include_illiquid=True should keep all contracts including zero liquid ones."""
        contracts = [
            _make_contract(10.0, bid=0, ask=0, open_interest=0),
            _make_contract(15.0, bid=1.0, ask=1.1, open_interest=100),
        ]
        filtered = self._filter_illiquid(contracts, include_illiquid=True)
        self.assertEqual(len(filtered), 2, "include_illiquid=True should keep all contracts")
        strikes = [c["strike"] for c in filtered]
        self.assertIn(10.0, strikes)
        self.assertIn(15.0, strikes)

    def test_filters_applied_reflect_include_illiquid(self):
        """filtersApplied in get_option_chain response should include include_illiquid setting."""
        # We test the server-level function integration via a stub that checks the filtersApplied structure
        # by inspecting the server defaults directly.
        import inspect
        sig = inspect.signature(srv.get_option_chain)
        defaults = {
            k: v.default
            for k, v in sig.parameters.items()
            if v.default is not inspect.Parameter.empty
        }
        self.assertIn("include_illiquid", defaults,
            "get_option_chain must have include_illiquid parameter")
        self.assertFalse(defaults["include_illiquid"],
            "include_illiquid default must be False (Robot-safe)")
        self.assertEqual(defaults.get("moneyness"), "near_money",
            "moneyness default must be near_money (Robot-safe)")
        self.assertEqual(defaults.get("sort_by"), "relevance",
            "sort_by default must be relevance (Robot-safe)")


class TestDiscoverySchema(unittest.TestCase):
    """Validate that get_option_chain schema has the required new parameters."""

    def test_get_option_chain_schema_has_new_params(self):
        """The TOOL_DEFINITIONS or server.py must expose moneyness_window_pct, include_illiquid,
        and sort_by=relevance.  This test checks the server.py function signature directly."""
        import inspect
        sig = inspect.signature(srv.get_option_chain)
        params = list(sig.parameters.keys())
        self.assertIn("moneyness_window_pct", params,
            "get_option_chain must have moneyness_window_pct parameter")
        self.assertIn("include_illiquid", params,
            "get_option_chain must have include_illiquid parameter")
        # Confirm default values
        defaults = {k: v.default for k, v in sig.parameters.items()
                    if v.default is not inspect.Parameter.empty}
        self.assertEqual(defaults.get("sort_by"), "relevance",
            "sort_by default must be 'relevance'")
        self.assertEqual(defaults.get("moneyness"), "near_money",
            "moneyness default must be 'near_money'")
        self.assertEqual(defaults.get("moneyness_window_pct"), 20,
            "moneyness_window_pct default must be 20")
        self.assertFalse(defaults.get("include_illiquid"),
            "include_illiquid default must be False")


if __name__ == "__main__":
    unittest.main(verbosity=2)
