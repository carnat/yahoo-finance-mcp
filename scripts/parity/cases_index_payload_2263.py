"""Parity cases: the filing index payload through the Worker's filing resolver (2.26.3, F_TRIAGE.md F50).

Python's get_sec_filing_index goes through _resolve_sec_filing, as the Worker's getSecFilingIndex goes through
resolveSecFiling: the payload carries requestedFilingType and warnings, a 10-K that is not filed falls to the latest
20-F (AUTO_20F_FALLBACK), and failures are the Worker's payloads. The public tool lists each table's first 8 row
labels and its rowLabelCount (the Worker's compactFilingIndexPayload); the tools built on the index read them all.

The canned SEC responses are those of cases_accession.py, plus the foreign private issuer's 20-F document.
"""

from __future__ import annotations

import json

import server  # noqa: E402
from scripts.parity.cases_accession import MISSING_ACC, ROUTES, TEN_K_ACC, TEN_K_DOC, _py
from scripts.parity.cases_routing_2260 import _strip_meta, _tool_ts
from scripts.parity.harness import Case

CASES: list[Case] = []

FPI_DOC = "https://www.sec.gov/Archives/edgar/data/999001/000099900125000001/fpi-20f.htm"
INDEX_ROUTES = {**ROUTES, FPI_DOC: {"body": "<html><body><h2>Item 3. Key Information</h2><p>Risk.</p></body></html>"}}


def _without_indexed_at(v):
    """The payload without meta.indexedAt, the time the index was built."""
    if isinstance(v, dict) and isinstance(v.get("meta"), dict) and "indexedAt" in v["meta"]:
        v = {**v, "meta": {k: m for k, m in v["meta"].items() if k != "indexedAt"}}
    return v


INDEX_CASES = [
    ("pinned_10k", "AAPL", "10-K", TEN_K_ACC),
    ("latest_10q", "AAPL", "10-Q", None),
    ("fpi_10k_falls_to_20f", "FPI", "10-K", None),
    ("form_not_filed", "AAPL", "S-1", None),
    ("accession_not_found", "AAPL", "10-K", MISSING_ACC),
    ("ticker_not_found", "NOPE", "10-K", None),
]
for name, ticker, form, acc in INDEX_CASES:
    CASES.append(Case(
        name=f"f50.index.{name}", ids=["F50"], routes=INDEX_ROUTES, select=_without_indexed_at,
        py=_py(server.get_sec_filing_index, ticker, form, "latest", acc),
        ts={"js": "return await m.yahooFinance.getSecFilingIndex(...args);",
            "args": [ticker, form, "latest", acc], "isolate": True},
    ))

# The 20-F fallback is stated, not silent.
CASES.append(Case(
    name="f50.index.fpi_10k_fallback_is_stated", ids=["F50"], routes=INDEX_ROUTES,
    py=lambda: {k: json.loads(_py(server.get_sec_filing_index, "FPI", "10-K", "latest", None)()).get(k)
                for k in ("requestedFilingType", "filingType")}
    | {"warnings": [w["code"] for w in json.loads(_py(server.get_sec_filing_index, "FPI", "10-K", "latest", None)())
                    .get("warnings", [])]},
    ts={"js": "const v = JSON.parse(await m.yahooFinance.getSecFilingIndex(...args));"
              " return { requestedFilingType: v.requestedFilingType, filingType: v.filingType,"
              " warnings: (v.warnings ?? []).map((w) => w.code) };",
        "args": ["FPI", "10-K", "latest", None], "isolate": True},
    expect={"requestedFilingType": "10-K", "filingType": "20-F", "warnings": ["AUTO_20F_FALLBACK"]},
))

# A failed document read keeps its HTTP status.
CASES.append(Case(
    name="f50.index.document_read_fails", ids=["F50"], routes={**ROUTES, TEN_K_DOC: {"status": 404, "body": "gone"}},
    py=_py(server.get_sec_filing_index, "AAPL", "10-K", "latest", TEN_K_ACC),
    ts={"js": "return await m.yahooFinance.getSecFilingIndex(...args);",
        "args": ["AAPL", "10-K", "latest", TEN_K_ACC], "isolate": True},
    expect={"ok": False, "error": {"code": "PROVIDER_ERROR", "message": "Failed to fetch filing: HTTP 404"}},
))

# ── The public tool: 8 row labels and the count ─────────────────────────────────────────────────────────────────
LONG_TABLE_HTML = (
    "<html><body><h2>Item 8. Financial Statements</h2><p>Balance sheet items as follows (in millions):</p><table>"
    "<tr><td>Item</td><td>2025</td></tr>"
    + "".join(f"<tr><td>Line {i}</td><td>{i},000</td></tr>" for i in range(1, 13))
    + "</table></body></html>"
)
LONG_ROUTES = {**ROUTES, TEN_K_DOC: {"body": LONG_TABLE_HTML}}

CASES.append(Case(
    name="f50.tool.index_lists_eight_row_labels_and_the_count", ids=["F50"], routes=LONG_ROUTES,
    py=lambda: [{k: t.get(k) for k in ("rowLabels", "rowLabelCount")} for t in
                json.loads(_py(server.get_sec_filing_index, "AAPL", "10-K", "latest", TEN_K_ACC)())["index"]["tables"]],
    ts={"js": "const r = JSON.parse(await m.tools.callTool(args[0], args[1])); const v = r.data ?? r;"
              " return v.index.tables.map((t) => ({ rowLabels: t.rowLabels, rowLabelCount: t.rowLabelCount ?? null }));",
        "args": ["get_sec_filing_index", {"ticker": "AAPL", "filing_type": "10-K", "accession_number": TEN_K_ACC}],
        "isolate": True},
    expect=[{"rowLabels": [f"Line {i}" for i in range(1, 9)], "rowLabelCount": 12}],
))

# The tables tool, built on the index, still reads every row label.
CASES.append(Case(
    name="f50.tables.index_tables_keep_every_row_label", ids=["F50"], routes=LONG_ROUTES, select=_strip_meta,
    py=_py(server.list_sec_filing_tables, "AAPL", "10-K", None, 0, 50),
    ts=_tool_ts("list_sec_filing_tables", {"ticker": "AAPL", "filing_type": "10-K"}),
))

# ── Tools built on the index, through the new failures ─────────────────────────────────────────────────────────
def _error_of(v):
    """A failure's error object; the Worker's callTool puts it in its {ok, data, meta, error} envelope."""
    if isinstance(v, str):
        v = json.loads(v)
    return v.get("error") if isinstance(v, dict) and v.get("ok") is False else _strip_meta(v)


for name, ticker, form in [("form_not_filed", "AAPL", "S-1"), ("fpi_10k_falls_to_20f", "FPI", "10-K"),
                           ("ticker_not_found", "NOPE", "10-K")]:
    CASES.append(Case(
        name=f"f50.outline.{name}", ids=["F50"], routes=INDEX_ROUTES, select=_error_of,
        py=_py(server.get_sec_filing_outline, ticker, form, "latest", None, None),
        ts=_tool_ts("get_sec_filing_outline", {"ticker": ticker, "filing_type": form}),
    ))
    CASES.append(Case(
        name=f"f50.tables.{name}", ids=["F50"], routes=INDEX_ROUTES, select=_error_of,
        py=_py(server.list_sec_filing_tables, ticker, form, None, 0, 50),
        ts=_tool_ts("list_sec_filing_tables", {"ticker": ticker, "filing_type": form}),
    ))
