"""Parity cases: release 2.25.1 audit findings F25 to F28.

F25  share-count scale read as the table's monetary scale   yfmcp/parsing/html.py   <-> tableUnitScale, statedUnitScales
F26  extract_segment_revenue ignores the period             yfmcp/parsing/extractors.py <-> extractSegmentTableFromHtml
F27  a future calendar date reported as the latest release  yfmcp/tools/earnings.py <-> resolveLatestEarningsRelease
F28  latest-only period not enforced                        yfmcp/validation.py     <-> LATEST_ONLY_PERIOD_TOOLS in tools.ts

F27 and F28 run the public tool in both runtimes (the Worker's resolver is not exported); SEC and Yahoo HTTP is
served from `routes`, so nothing here touches the network.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

from scripts.parity.cases_sec_earnings import _python_env, _sec_routes
from scripts.parity.harness import Case
from yfmcp.parsing.extractors import extract_segment_table_from_html
from yfmcp.parsing.html import _table_unit_scale

CASES: list[Case] = []

# ── F25: tableUnitScale ───────────────────────────────────────────────────────────────────────────
APPLE_CAPTION = "(In millions, except number of shares, which are reflected in thousands, and per-share amounts)"
TABLE = "<table><tr><td>Net sales</td><td>1</td></tr></table>"

for _name, _caption, _scale in (
    ("apple_caption_except_shares", APPLE_CAPTION, "millions"),
    ("shares_in_thousands_clause", "(Dollars in millions, shares in thousands)", "millions"),
    ("per_share_except_clause", "(in thousands, except per share data)", "thousands"),
    ("last_statement_wins", "(in millions) ... (in thousands)", "thousands"),
):
    _before = f"<p>{_caption}</p>"
    CASES.append(Case(
        name=f"audit2251.f25.table_unit_scale.{_name}", ids=["F25"],
        py=lambda b=_before: _table_unit_scale(TABLE, b),
        ts={"module": "yahooFinance", "fn": "tableUnitScale", "args": [TABLE, _before]},
        expect=_scale))


def _segment_html(lead: str) -> str:
    return (f"<p>{lead}</p><table>"
            "<tr><td></td><td>2025</td><td>2024</td><td>2023</td></tr>"
            "<tr><td>Americas</td><td>$ 178,353</td><td>$ 167,045</td><td>$ 162,560</td></tr>"
            "<tr><td>Europe</td><td>111,032</td><td>101,328</td><td>94,294</td></tr>"
            "<tr><td>Greater China</td><td>64,377</td><td>66,952</td><td>72,559</td></tr>"
            "<tr><td>Total net sales</td><td>$ 353,762</td><td>$ 335,325</td><td>$ 329,413</td></tr>"
            "</table>")


def _seg_view(v: dict) -> dict:
    r = v.get("result")
    return {
        "yearsSeen": v.get("yearsSeen"), "segmentTablesSeen": v.get("segmentTablesSeen"),
        "result": None if r is None else {
            "column": r["column"], "unitScale": r["unitScale"], "unitScaleSource": r["unitScaleSource"],
            "rawValues": [s["rawValue"] for s in r["segments"]], "total": r["total"]["rawValue"],
        },
    }


def _seg_expect(column: str | None, raw: list[str], total: str) -> dict:
    result = None if column is None else {"column": column, "unitScale": "millions", "unitScaleSource": "STATED_BEFORE_TABLE",
                                          "rawValues": raw, "total": total}
    return {"yearsSeen": [2025, 2024, 2023], "segmentTablesSeen": 1, "result": result}


def _seg_case(name: str, ids: list[str], lead: str, year: int | None, expect: dict) -> Case:
    html = _segment_html(lead)
    return Case(name=name, ids=ids, py=lambda: extract_segment_table_from_html(html, year),
                ts={"module": "yahooFinance", "fn": "extractSegmentTableFromHtml", "args": [html, year]},
                select=_seg_view, expect=expect)


_LEAD = "Net sales by reportable segment for 2025, 2024 and 2023 were as follows (dollars in millions):"

# detectUnitScale on a segment table whose lead-in is Apple's caption: millions, not the share scale.
CASES.append(_seg_case("audit2251.f25.segment_table_scale_with_apple_caption", ["F25"],
                       f"Net sales by reportable segment {APPLE_CAPTION}:", None,
                       _seg_expect("2025", ["$ 178,353", "111,032", "64,377"], "$ 353,762")))

# ── F26: extractSegmentTableFromHtml reads the named year's column ────────────────────────────────
CASES += [
    _seg_case("audit2251.f26.latest_column_without_a_year", ["F26"], _LEAD, None,
              _seg_expect("2025", ["$ 178,353", "111,032", "64,377"], "$ 353,762")),
    _seg_case("audit2251.f26.named_year_2024", ["F26"], _LEAD, 2024,
              _seg_expect("2024", ["$ 167,045", "101,328", "66,952"], "$ 335,325")),
    _seg_case("audit2251.f26.year_not_in_any_table_is_no_result", ["F26"], _LEAD, 2022, _seg_expect(None, [], "")),
]

# ── F27: a calendar date still ahead is not the latest release ────────────────────────────────────
_EPOCH_FUTURE = 4_070_908_800   # 2099-01-01T00:00:00Z
_EPOCH_PAST = 1_577_836_800     # 2020-01-01T00:00:00Z


def _iso(epoch: int) -> str:
    import datetime
    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _release_view(v: dict) -> dict:
    return {"confidence": v.get("confidence"), "reportedAt": (v.get("reportedAt") or "")[:10] or None,
            "sourceTypes": [s.get("sourceType") for s in v.get("sources") or []],
            "warningCodes": [w.get("code") for w in v.get("warnings") or []]}


def _f27_case(name: str, tkr: str, cik: int, epochs: list[int], expect: dict) -> Case:
    routes = _sec_routes(tkr, cik, [])
    routes[f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{tkr}?modules=calendarEvents*"] = {"json": {"quoteSummary": {"result": [
        {"calendarEvents": {"earnings": {"earningsDate": [{"raw": e} for e in epochs]}}}], "error": None}}}
    calendar = json.dumps({"calendar": {"earnings": {"earningsDate": [_iso(e) for e in epochs]}}})

    def py():
        from yfmcp.tools.earnings import _resolve_latest_earnings_release
        with _python_env(calendar=calendar):
            return asyncio.run(_resolve_latest_earnings_release(tkr))

    return Case(name=name, ids=["F27"], routes=routes, py=py, select=_release_view, expect=expect,
                ts={"js": "m.response.setWorkerEnv({}); return m.yahooFinance.getLatestEarningsRelease(args[0]);", "args": [tkr], "isolate": True},
                note="Python's get_calendar output has flat 'Earnings Date' keys, so its calendar path is stubbed here with the Worker's shape.")


CASES += [
    _f27_case("audit2251.f27.only_a_future_date_is_not_found", "ZQF", 840001, [_EPOCH_FUTURE],
              {"confidence": "NOT_FOUND", "reportedAt": None, "sourceTypes": [], "warningCodes": []}),
    _f27_case("audit2251.f27.past_date_wins_over_a_future_one", "ZQG", 840002, [_EPOCH_FUTURE, _EPOCH_PAST],
              {"confidence": "MEDIUM", "reportedAt": "2020-01-01", "sourceTypes": ["yahoo_estimate"], "warningCodes": ["SEC_8K_NOT_FOUND"]}),
]

# ── F28: a latest-only earnings tool refuses another period before any fetch ──────────────────────
LATEST_ONLY = ["get_latest_earnings_release", "index_earnings_release", "extract_earnings_metrics", "extract_guidance",
               "extract_management_commentary", "compare_earnings_actual_vs_estimate", "get_earnings_call_transcript"]


def _grouped_py(action: str, args: dict, calendar: str = "{}"):
    def run():
        import server
        import tool_groups
        from yfmcp.app import build_handler_registry
        with _python_env(calendar=calendar):
            return asyncio.run(tool_groups._route_grouped_call("earnings_intelligence", action, args, build_handler_registry(server.yfinance_server)))
    return run


def _grouped_ts(action: str, args: dict) -> dict:
    body = (f'm.response.setWorkerEnv({{}}); const r = await m.tools.callVisibleToolResult("earnings_intelligence", '
            f'{{action: {json.dumps(action)}, params: {json.dumps(args)}}}); return r.value ?? JSON.parse(r.text);')
    return {"js": body, "isolate": True}


def _gate_view(v: dict) -> dict:
    err = v.get("error") if isinstance(v.get("error"), dict) else {}
    return {"ok": v.get("ok"), "code": err.get("code"), "message": err.get("message"), "tool": (v.get("meta") or {}).get("tool")}


def _refusal(action: str, period: str) -> dict:
    return {"ok": False, "code": "INPUT_VALIDATION_ERROR", "tool": action,
            "message": f"period supports only \"latest\" for {action}; it reads the latest earnings release. Got '{period}'."}


def _period_gate_view(v: dict) -> bool:
    """Whether the call was refused by the latest-only gate."""
    err = v.get("error") if isinstance(v.get("error"), dict) else {}
    return 'supports only "latest"' in str(err.get("message") or "")


CASES.append(Case(
    name="audit2251.f28.extract_earnings_metrics_fiscal_quarter_period_is_refused", ids=["F28"],
    py=_grouped_py("extract_earnings_metrics", {"ticker": "ZQH", "period": "FY2025 Q4"}),
    ts=_grouped_ts("extract_earnings_metrics", {"ticker": "ZQH", "period": "FY2025 Q4"}),
    routes=_sec_routes("ZQH", 840003, []), select=_gate_view, expect=_refusal("extract_earnings_metrics", "FY2025 Q4")))
for _i, _action in enumerate(LATEST_ONLY):
    _args = {"ticker": f"ZR{chr(65 + _i)}", "period": "2025"}
    CASES.append(Case(
        name=f"audit2251.f28.{_action}_refuses_a_year_period", ids=["F28"],
        py=_grouped_py(_action, _args), ts=_grouped_ts(_action, _args),
        routes=_sec_routes(_args["ticker"], 841000 + _i, []), select=_gate_view, expect=_refusal(_action, "2025")))
for _label, _period in (("latest", "latest"), ("upper_case_latest", "LATEST"), ("padded_latest", " Latest ")):
    CASES.append(Case(
        name=f"audit2251.f28.period_{_label}_passes_the_gate", ids=["F28"],
        py=_grouped_py("get_latest_earnings_release", {"ticker": "ZQI", "period": _period}),
        ts=_grouped_ts("get_latest_earnings_release", {"ticker": "ZQI", "period": _period}),
        routes=_sec_routes("ZQI", 840004, []), select=_period_gate_view, expect=False))
