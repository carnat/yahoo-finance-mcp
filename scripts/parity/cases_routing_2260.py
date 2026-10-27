"""Parity cases: the Python EDGAR routing release (2.26.0; PHASE4_SPEC.md §3.6 and D9, F_TRIAGE.md F1, F2).

- F1: get_sec_filing_outline without a document_url outlines the filing index in both runtimes (the Worker's
  secIndexOutlinePayload), so a pinned accession is honoured. Python read the latest primary document instead.
- F2: outline titles turn inner tags into a space in both runtimes ("Item<b>1A.</b>" reads "Item 1A.").
- D9: parse_public_transcript fetches an SEC page through the EDGAR client, with SEC's User-Agent, in both runtimes.

The canned SEC responses are those of cases_accession.py: a 10-Q, an 8-K and an older 10-K for AAPL.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import server  # noqa: E402
from scripts.parity.cases_accession import BASE, ROUTES, TEN_K_ACC, TEN_K_DOC, _py, _ts
from scripts.parity.harness import Case
from yfmcp import url_policy

CASES: list[Case] = []

SEC_UA = "yahoo-finance-mcp contact@example.com"


def _strip_meta(v):
    """The tool payload: the Worker's callTool wraps it in the {ok, data, meta, error} envelope, Python's function does not."""
    if isinstance(v, dict) and "meta" in v and "data" in v and v.get("ok") is True:
        return v["data"]
    return v


def _tool_ts(name: str, args: dict) -> dict:
    """The Worker's public tool dispatch (tools.ts callTool), as the grouped server calls it."""
    return {"js": "return await m.tools.callTool(args[0], args[1]);", "args": [name, args], "isolate": True}


# ── F1: the index outline, honouring a pinned accession ─────────────────────────────────────────────────────────
F1_CASES = [
    ("f1.outline.pinned_10k_accession", {"filing_type": "10-K", "accession_number": TEN_K_ACC}),
    ("f1.outline.latest_10k_without_accession", {"filing_type": "10-K"}),
    ("f1.outline.latest_10q_without_accession", {"filing_type": "10-Q"}),
]
for name, extra in F1_CASES:
    args = {"ticker": "AAPL", "period": "latest", **extra}
    CASES.append(Case(
        name=name, ids=["F1"], routes=ROUTES, select=_strip_meta,
        py=_py(server.get_sec_filing_outline, "AAPL", extra.get("filing_type", "10-K"), "latest",
               extra.get("accession_number"), None),
        ts=_tool_ts("get_sec_filing_outline", args),
    ))

# The pinned filing is the one the outline names (the 10-K document), in both runtimes.
CASES.append(Case(
    name="f1.outline.pinned_accession_names_the_pinned_document", ids=["F1"], routes=ROUTES,
    py=lambda: {k: json.loads(_py(server.get_sec_filing_outline, "AAPL", "10-Q", "latest", TEN_K_ACC, None)()).get(k)
                for k in ("accessionNumber", "documentUrl")},
    ts={"js": "const r = JSON.parse(await m.tools.callTool(args[0], args[1])); const v = r.data ?? r;"
              " return { accessionNumber: v.accessionNumber, documentUrl: v.documentUrl };",
        "args": ["get_sec_filing_outline", {"ticker": "AAPL", "filing_type": "10-Q", "accession_number": TEN_K_ACC}],
        "isolate": True},
    expect={"accessionNumber": TEN_K_ACC, "documentUrl": TEN_K_DOC},
))

# ── F2: inner tags in a heading become a space ──────────────────────────────────────────────────────────────────
SPLIT_HEADING_DOC = f"{BASE}/000032019325000086/split-heading.htm"
F2_ROUTES = {**ROUTES, SPLIT_HEADING_DOC: {"body": "<html><body><h2><b>Item</b><b>1A.</b>Risk Factors</h2>"
                                                     "<h3>Part<span>II</span></h3></body></html>"}}
CASES.append(Case(
    name="f2.outline.inner_tags_become_a_space", ids=["F2"], routes=F2_ROUTES,
    py=lambda: json.loads(_py(server.get_filing_outline, "AAPL", None, SPLIT_HEADING_DOC)())["outline"],
    ts={"js": "return JSON.parse(await m.yahooFinance.getFilingOutline(...args)).outline;",
        "args": ["AAPL", None, SPLIT_HEADING_DOC], "isolate": True},
    expect=[{"level": 2, "title": "Item 1A. Risk Factors"}, {"level": 3, "title": "Part II"}],
))

# ── D9: an SEC page in parse_public_transcript carries SEC's User-Agent ─────────────────────────────────────────
SEC_PAGE = f"{BASE}/000032019325000070/aapl-ex99.htm"
D9_ROUTES = {**ROUTES, SEC_PAGE: {"body": "<html><body><p>Operator: Welcome to the call.</p></body></html>"}}


def _py_user_agents():
    seen: list[str] = []
    original = url_policy.urlopen

    def recording(req, *a, **k):
        seen.append(req.get_header("User-agent") if hasattr(req, "get_header") else None)
        return original(req, *a, **k)

    with patch.object(url_policy, "urlopen", recording):
        out = json.loads(_py(server.parse_public_transcript, SEC_PAGE, None, None)())
    return {"userAgents": seen, "source": (out.get("data") or out).get("source")}


CASES.append(Case(
    name="d9.parse_public_transcript.sec_page_uses_the_sec_user_agent", ids=["D9"], routes=D9_ROUTES,
    py=_py_user_agents,
    ts={"js": (
        "const seen = []; const orig = globalThis.fetch;"
        " globalThis.fetch = async (input, init) => {"
        "   const h = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));"
        "   seen.push(h.get('User-Agent')); return orig(input, init); };"
        " try { const v = JSON.parse(await m.yahooFinance.parsePublicTranscript(args[0], null, null));"
        "   return { userAgents: seen, source: v.source }; }"
        " finally { globalThis.fetch = orig; }"),
        "args": [SEC_PAGE], "isolate": True},
    expect={"userAgents": [SEC_UA], "source": "public_url"},
))

# ── F48 (2.26.2, owner 2026-10-27: match the Worker): a pinned accession of another form than filing_type ────────
# get_sec_filing_index labels filingType with the pinned filing's own form in both runtimes. Python used the form
# the caller asked for. The outline inherits the label because it is built from the index.
CASES.append(Case(
    name="f48.index.pinned_accession_of_another_form_filing_type_label", ids=["F48"], routes=ROUTES,
    py=lambda: json.loads(_py(server.get_sec_filing_index, "AAPL", "10-Q", "latest", TEN_K_ACC)()).get("filingType"),
    ts={"js": "return JSON.parse(await m.yahooFinance.getSecFilingIndex(...args)).filingType;",
        "args": ["AAPL", "10-Q", "latest", TEN_K_ACC], "isolate": True},
    expect="10-K",
))
CASES.append(Case(
    name="f48.outline.pinned_accession_of_another_form_filing_type_label", ids=["F48"], routes=ROUTES,
    py=lambda: json.loads(_py(server.get_sec_filing_outline, "AAPL", "10-Q", "latest", TEN_K_ACC, None)()).get("filingType"),
    ts={"js": "const r = JSON.parse(await m.tools.callTool(args[0], args[1])); return (r.data ?? r).filingType;",
        "args": ["get_sec_filing_outline", {"ticker": "AAPL", "filing_type": "10-Q", "accession_number": TEN_K_ACC}],
        "isolate": True},
    expect="10-K",
))
