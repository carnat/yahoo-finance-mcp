#!/usr/bin/env python3
"""Validate canonical/alias routing invariants on deployed MCP.

Asserts routing metadata (ok, meta.canonicalTool, DEPRECATED_ALIAS warning,
stable key presence) WITHOUT exact-comparing volatile live payload values
(prices, timestamps, volume). This avoids false failures when the market is open
or when two sequential calls return slightly different quotes.

Stable keys checked per tool (only keys that are structurally guaranteed):
  get_market_quote / get_fast_info   → currency, quoteType, lastPrice, previousClose
  check_volume_liquidity_threshold    → ticker, gatePass, dataDate
  summarize_options_flow              → ticker, dataQuality
  analyze_options_flow_window         → ticker, dataQuality (or windowLabel)
  analyze_position_signals            → t1_inputs, t2_inputs, t4_inputs, t5_inputs
  calculate_price_target_distance     → ticker, currentPrice, bracket

Note: get_market_quote/get_fast_info returns yfinance fast_info fields directly
and does not inject a "ticker" key, so "ticker" is not a stable key for that tool.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

URL = "https://yahoo-finance-mcp.artinatw.workers.dev/mcp"
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-universal-aliases/1.0)"

# Set ALLOW_NETWORK_SKIP=1 (or "true"/"yes") to skip gracefully when the
# deployed worker is unreachable or behind (e.g. pre-deployment CI runs).
# The deployed smoke-test (deploy-worker.yml) always sets ALLOW_NETWORK_SKIP=0.
_ALLOW_SKIP = os.environ.get("ALLOW_NETWORK_SKIP", "0").lower() in ("1", "true", "yes")

# (canonical, args, alias, required_stable_keys_in_data, expect_deprecated_warning)
ALIAS_PAIRS: list[tuple[str, dict, str, set[str], bool]] = [
    # get_fast_info returns yfinance fast_info fields directly (no "ticker" key injected)
    ("get_market_quote", {"ticker": "AAPL"}, "get_fast_info", {"currency", "quoteType", "lastPrice", "previousClose"}, False),
    ("check_volume_liquidity_threshold", {"ticker": "AAPL"}, "get_adv_gate", {"ticker", "gatePass", "dataDate"}, True),
    ("summarize_options_flow", {"ticker": "AAPL"}, "get_options_summary", {"ticker", "dataQuality"}, False),
    ("summarize_options_flow", {"ticker": "AAPL"}, "get_options_flow_summary", {"ticker", "dataQuality"}, False),
    ("analyze_options_flow_window", {"ticker": "AAPL", "window_label": "audit"}, "get_dc134_options_scan", {"ticker", "dataQuality"}, True),
    ("analyze_position_signals", {"ticker": "AAPL"}, "get_tps_inputs", {"t1_inputs", "t2_inputs", "t4_inputs", "t5_inputs"}, True),
    ("calculate_price_target_distance", {"ticker": "AAPL", "io_pt": 200}, "get_eqf_bracket", {"ticker", "currentPrice", "bracket"}, True),
]


def rpc(name: str, args: dict, req_id: int) -> dict:
    payload = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/call",
        "params": {"name": name, "arguments": args},
    }
    req = urllib.request.Request(
        URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": UA},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        body = json.loads(resp.read())
    if "error" in body:
        raise AssertionError(f"{name} JSON-RPC error: {body['error']}")
    text = ((((body.get("result") or {}).get("content") or [{}])[0]) or {}).get("text", "")
    return json.loads(text)


def data_of(payload: dict) -> dict:
    if isinstance(payload, dict) and "ok" in payload and "data" in payload:
        return payload.get("data") or {}
    # Fallback: if no V2 envelope, payload is the data directly
    return payload if isinstance(payload, dict) else {}


def meta_of(payload: dict) -> dict:
    if isinstance(payload, dict) and "ok" in payload:
        return payload.get("meta") or {}
    return {}


def main() -> int:
    failures: list[str] = []
    for i, (canonical, args, alias, stable_keys, expect_warning) in enumerate(ALIAS_PAIRS, start=1):
        try:
            can_payload = rpc(canonical, args, i)
            ali_payload = rpc(alias, args, i + 100)
        except urllib.error.URLError as exc:
            if _ALLOW_SKIP:
                print(f"SKIP universal alias tests: worker unreachable ({exc})")
                return 0
            failures.append(f"{alias}: network error — {exc}")
            continue
        try:
            can_data = data_of(can_payload)
            ali_data = data_of(ali_payload)

            # Check V2 envelope on canonical
            if isinstance(can_payload, dict) and "ok" in can_payload:
                if can_payload.get("ok") is not True:
                    failures.append(f"{canonical}: ok != true")
                    continue

            # Check stable keys present in both (not comparing volatile values)
            for key in stable_keys:
                if key not in can_data:
                    failures.append(f"{canonical}: missing stable key {key!r}")
                if key not in ali_data:
                    failures.append(f"{alias}: missing stable key {key!r}")

            # Check alias routing metadata
            meta = meta_of(ali_payload)
            if meta:
                canon_tool = meta.get("canonicalTool")
                if canon_tool != canonical:
                    failures.append(f"{alias}: meta.canonicalTool={canon_tool!r}, expected {canonical!r}")
                warnings = meta.get("warnings") or []
                has_deprecated_alias_warning = any(
                    isinstance(w, dict) and w.get("code") == "DEPRECATED_ALIAS" for w in warnings
                )
                if expect_warning and not has_deprecated_alias_warning:
                    failures.append(f"{alias}: missing DEPRECATED_ALIAS warning in meta.warnings")
                if (not expect_warning) and has_deprecated_alias_warning:
                    failures.append(f"{alias}: unexpected DEPRECATED_ALIAS warning in meta.warnings")
                deprecated_tool = meta.get("deprecatedTool")
                use_instead = meta.get("useInstead")
                if expect_warning:
                    if deprecated_tool is not True:
                        failures.append(f"{alias}: expected meta.deprecatedTool=true, got {deprecated_tool!r}")
                    if not isinstance(use_instead, str) or not use_instead:
                        failures.append(f"{alias}: expected non-empty meta.useInstead, got {use_instead!r}")
                else:
                    if deprecated_tool is True:
                        failures.append(f"{alias}: unexpected meta.deprecatedTool=true")

            # Check structure parity (same top-level keys, ignoring deprecation markers)
            _alias_extra_keys = {"_deprecatedAlias", "_canonicalTool"}
            can_keys = set(can_data.keys())
            ali_keys = set(ali_data.keys()) - _alias_extra_keys
            missing = can_keys - ali_keys
            if missing:
                print(f"  [!] {alias}: alias missing keys vs canonical: {missing} (may vary when market is open)", file=sys.stderr)

            print(f"  PASS  {alias} → {canonical}")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{alias}: {exc}")

    if failures:
        print("\nFAILURES:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        return 1

    print("\nPASS — all universal aliases routing invariants verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
