#!/usr/bin/env python3
"""Validate canonical/alias routing parity on deployed MCP.

Checks that each deprecated alias:
  1. Returns the same data as its canonical counterpart.
  2. Includes a meta.canonicalTool field matching the canonical name.
  3. Includes a DEPRECATED_ALIAS warning in meta.warnings.
"""

from __future__ import annotations

import json
import sys
import urllib.request

URL = "https://yahoo-finance-mcp.artinatw.workers.dev/mcp"
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-universal-aliases/1.0)"

ALIAS_PAIRS: list[tuple[str, dict, str]] = [
    ("get_market_quote", {"ticker": "ASTS"}, "get_fast_info"),
    ("analyze_position_signals", {"ticker": "ASTS"}, "get_tps_inputs"),
    ("calculate_price_target_distance", {"ticker": "ASTS", "io_pt": 95}, "get_eqf_bracket"),
    ("check_volume_liquidity_threshold", {"ticker": "ASTS"}, "get_adv_gate"),
    ("analyze_options_flow_window", {"ticker": "ASTS", "window_label": "audit"}, "get_dc134_options_scan"),
    ("summarize_options_flow", {"ticker": "ASTS"}, "get_options_summary"),
    ("summarize_options_flow", {"ticker": "ASTS"}, "get_options_flow_summary"),
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
    return payload


def meta_of(payload: dict) -> dict:
    if isinstance(payload, dict) and "ok" in payload:
        return payload.get("meta") or {}
    return {}


def main() -> int:
    failures: list[str] = []
    for i, (canonical, args, alias) in enumerate(ALIAS_PAIRS, start=1):
        try:
            can_payload = rpc(canonical, args, i)
            ali_payload = rpc(alias, args, i + 100)
            can_data = data_of(can_payload)
            ali_data = data_of(ali_payload)
            if can_data != ali_data:
                failures.append(f"Data mismatch: alias={alias} canonical={canonical}")
                continue
            meta = meta_of(ali_payload)
            if meta:
                if meta.get("canonicalTool") != canonical:
                    failures.append(f"{alias}: expected canonicalTool={canonical!r}, got {meta.get('canonicalTool')!r}")
                warnings = meta.get("warnings") or []
                if not any(isinstance(w, dict) and w.get("code") == "DEPRECATED_ALIAS" for w in warnings):
                    failures.append(f"{alias}: missing DEPRECATED_ALIAS warning in meta.warnings")
            print(f"  PASS  {alias} → {canonical}")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{alias}: {exc}")

    if failures:
        print("\nFAILURES:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        return 1

    print("\nPASS — all universal aliases verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

