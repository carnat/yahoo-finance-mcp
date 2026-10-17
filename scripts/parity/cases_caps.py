"""Parity cases: per-tool batch ticker caps (2.12.0, R4 Q3c, spec §3 and §5).

The cap table is tool_catalog.json `tickerCaps`, read by both runtimes. An array longer than the action's cap is
rejected by the tool-call gate with BATCH_LIMIT_EXCEEDED before any fetch; nothing is truncated any more.
get_company_news is capped at 1 when `sources` names company_ir, company_ir_page or newswire, and
get_market_snapshot uses its "get_market_snapshot.full" entry when mode is "full".

Worker side: `validateToolArgs` (worker/src/tools.ts) and the expanded tool listing. Python side: `validate_tool_args`
(see cases_validation.py `_py_gate`) and Python's own expanded listing, whose ticker array branch carries maxItems.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from scripts.parity.cases_validation import PASS, _py_gate, gate_view
from scripts.parity.harness import Case

CASES: list[Case] = []

TICKER_CAPS: dict[str, int] = json.loads(
    (Path(__file__).resolve().parents[2] / "tool_catalog.json").read_text(encoding="utf-8"))["tickerCaps"]
POOL = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BRK-B", "AVGO", "TSLA", "JPM", "LLY", "V", "UNH", "XOM",
        "MA", "JNJ", "WMT", "PG", "HD", "COST", "ORCL"]


def over(action: str, n: int, cap: int) -> dict:
    return {"code": "BATCH_LIMIT_EXCEEDED", "message": f"Too many tickers: {n}. Maximum is {cap} per call for {action}.",
            "tool": action, "retryable": False}


def caps_case(name: str, ids: list[str], rows: list[tuple[str, str, dict]], expect: dict) -> Case:
    js = ("const out = {};\n"
          f"for (const [label, action, a] of {json.dumps([list(r) for r in rows])}) out[label] = m.tools.validateToolArgs(action, a);\n"
          "return out;")
    return Case(name=name, ids=ids, py=lambda: _py_gate(rows), ts={"js": js},
                select=lambda v: {k: gate_view(x) for k, x in v.items()}, expect=expect)


def _action_args(key: str, n: int) -> tuple[str, dict]:
    action, _, variant = key.partition(".")
    args: dict = {"ticker": POOL[:n]}
    if variant == "full":
        args["mode"] = "full"
    return action, args


# ── Every capped action: the cap passes, cap + 1 is BATCH_LIMIT_EXCEEDED ─────────────────────────────────────────
CAP_ROWS, CAP_EXPECT = [], {}
for key, cap in sorted(TICKER_CAPS.items()):
    action, at_cap = _action_args(key, cap)
    _, over_cap = _action_args(key, cap + 1)
    CAP_ROWS += [(f"{key}@{cap}", action, at_cap), (f"{key}@{cap + 1}", action, over_cap)]
    CAP_EXPECT[f"{key}@{cap}"] = PASS
    CAP_EXPECT[f"{key}@{cap + 1}"] = over(action, cap + 1, cap)
CASES += [caps_case("caps.every_capped_action_cap_passes_and_cap_plus_one_is_rejected", ["Q3c"], CAP_ROWS, CAP_EXPECT)]

# ── get_company_news conditional cap, snapshot modes, ordering and what is not capped ──────────────────────────
NEWS = "get_company_news"
COND_ROWS = [
    ("company_ir_two", NEWS, {"ticker": ["AAPL", "MSFT"], "sources": ["company_ir"]}),
    ("company_ir_one", NEWS, {"ticker": ["AAPL"], "sources": ["company_ir"]}),
    ("company_ir_string", NEWS, {"ticker": "AAPL", "sources": ["company_ir", "sec"]}),
    ("company_ir_page_two", NEWS, {"ticker": ["AAPL", "MSFT"], "sources": ["yahoo_finance_news", "company_ir_page"]}),
    ("newswire_two", NEWS, {"ticker": ["AAPL", "MSFT"], "sources": ["newswire"]}),
    # Source names are matched as the handler normalises them (trimmed, lower-case).
    ("newswire_mixed_case_two", NEWS, {"ticker": ["AAPL", "MSFT"], "sources": [" NewsWire "]}),
    ("other_sources_five", NEWS, {"ticker": POOL[:5], "sources": ["yahoo_finance_news", "finnhub", "sec"]}),
    ("default_sources_five", NEWS, {"ticker": POOL[:5]}),
    ("snapshot_mode_other_than_full_uses_compact", "get_market_snapshot", {"ticker": POOL[:5], "mode": "FULL"}),
    # The cap is checked before the ticker format.
    ("oversize_with_bad_symbols", "get_market_quote", {"ticker": ["BAD$"] * 11}),
    # A comma string is never split: it is one (invalid) symbol, not a batch.
    ("comma_string_is_not_a_batch", "get_market_quote", {"ticker": ",".join(POOL[:11])}),
    # Peer tools keep their own rules and messages; actions without a tickerCaps entry are not capped by the gate.
    ("compare_peer_valuations_eleven", "compare_peer_valuations", {"tickers": POOL[:11]}),
    ("historical_valuation_six_peers", "get_historical_valuation_context", {"ticker": "AAPL", "peers": POOL[1:7]}),
    ("uncapped_action_array", "analyze_position_signals", {"ticker": POOL[:6]}),
]
CASES += [
    caps_case("caps.company_news_conditional_cap_snapshot_modes_and_uncapped_actions", ["Q3c"], COND_ROWS, {
        "company_ir_two": over(NEWS, 2, 1),
        "company_ir_one": PASS,
        "company_ir_string": PASS,
        "company_ir_page_two": over(NEWS, 2, 1),
        "newswire_two": over(NEWS, 2, 1),
        "newswire_mixed_case_two": over(NEWS, 2, 1),
        "other_sources_five": PASS,
        "default_sources_five": PASS,
        "snapshot_mode_other_than_full_uses_compact": PASS,
        "oversize_with_bad_symbols": over("get_market_quote", 11, 10),
        "comma_string_is_not_a_batch": {"code": "INPUT_VALIDATION_ERROR", "tool": "get_market_quote", "retryable": None,
                                        "message": f"Invalid ticker symbol: '{','.join(POOL[:11])}'. Must be 1-20 characters: uppercase letters, digits, or . - ^ ="},
        "compare_peer_valuations_eleven": PASS,
        "historical_valuation_six_peers": PASS,
        "uncapped_action_array": PASS,
    }),
]


# ── Schemas: the ticker array branch advertises the cap as maxItems ─────────────────────────────────────────────
def _schema_max_items(schema: dict) -> int | None:
    for branch in (schema or {}).get("anyOf", []) + (schema or {}).get("oneOf", []):
        if branch.get("type") == "array" and "maxItems" in branch:
            return branch["maxItems"]
    return None


def _py_listing_caps():
    import server

    out = {}
    for tool in asyncio.run(server.yfinance_server.list_tools()):
        schema = getattr(tool, "input_schema", None) or getattr(tool, "inputSchema", None) or {}
        cap = _schema_max_items(schema.get("properties", {}).get("ticker"))
        if cap is not None:
            out[tool.name] = cap
    return out


_TS_LISTING = """
const out = {};
for (const tool of m.tools.TOOLS) {
  const branch = (tool.inputSchema.properties.ticker?.oneOf ?? []).find((b) => b.type === "array");
  if (branch && typeof branch.maxItems === "number") out[tool.name] = branch.maxItems;
}
return out;
"""
SCHEMA_CAPS = {k: v for k, v in TICKER_CAPS.items() if "." not in k}
CASES += [
    Case(name="caps.schema_max_items_match_ticker_caps", ids=["Q3c"], py=_py_listing_caps, ts={"js": _TS_LISTING},
         expect=SCHEMA_CAPS),
]
