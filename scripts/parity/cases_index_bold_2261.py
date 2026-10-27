"""Parity cases: the filing index of a filing with bold Part and Item headings (2.26.1).

Most 10-Ks and 10-Qs mark Part and Item headings in bold text, not <h1>-<h6>. The Worker's index falls back to those
headings (filingItemHeadings); Python's index had none, so get_sec_filing_outline without a document_url (which
outlines the index since 2.26.0, F1) returned an empty outline in Python and the Item outline in the Worker.

The canned SEC responses are those of cases_accession.py, with the 10-K document replaced by a bold-heading one.
"""

from __future__ import annotations

import json

import server  # noqa: E402
from scripts.parity.cases_accession import ROUTES, TEN_K_ACC, TEN_K_DOC, _py
from scripts.parity.harness import Case
from scripts.parity.cases_routing_2260 import _strip_meta, _tool_ts

CASES: list[Case] = []

BOLD_10K_HTML = (
    "<html><body>"
    # The table of contents: rows that end with a page number are not headings.
    "<table><tr><td><b>Item 1. Business</b> 1</td></tr><tr><td><b>Item 1A. Risk Factors</b> 5</td></tr></table>"
    "<p><b>PART I</b></p>"
    "<p><span style=\"font-weight:700\">Item 1.&#160;&#160;&#160;&#160;Business</span></p>"
    "<p>Annual revenue grew in fiscal 2024.</p>"
    "<p><b>Item 1A. Risk Factors</b></p><p>Supply risk.</p>"
    "<table><tr><td>Net sales</td><td>391,035</td></tr></table>"
    "<p><b>PART II</b></p>"
    "<p><strong>Item 7. Management&#8217;s Discussion and Analysis</strong></p><p>Revenue rose.</p>"
    "</body></html>"
)
BOLD_ROUTES = {**ROUTES, TEN_K_DOC: {"body": BOLD_10K_HTML}}

CASES.append(Case(
    name="idx2261.index.bold_headings_are_sections", ids=["F1"], routes=BOLD_ROUTES,
    py=lambda: json.loads(_py(server.get_sec_filing_index, "AAPL", "10-K", "latest", TEN_K_ACC)())["index"]["sections"],
    ts={"js": "return JSON.parse(await m.yahooFinance.getSecFilingIndex(...args)).index.sections;",
        "args": ["AAPL", "10-K", "latest", TEN_K_ACC], "isolate": True},
))

# A table takes the bold heading before it as its section.
CASES.append(Case(
    name="idx2261.index.table_section_is_the_bold_heading_before_it", ids=["F1"], routes=BOLD_ROUTES,
    py=lambda: [t["sectionId"] for t in
                json.loads(_py(server.get_sec_filing_index, "AAPL", "10-K", "latest", TEN_K_ACC)())["index"]["tables"]],
    ts={"js": "return JSON.parse(await m.yahooFinance.getSecFilingIndex(...args)).index.tables.map((t) => t.sectionId);",
        "args": ["AAPL", "10-K", "latest", TEN_K_ACC], "isolate": True},
    expect=[None, "item_1a_risk_factors"],
))

# ── Noticed, not fixed in 2.26.1 (F49, unclassified): an index table's title ─────────────────────────────────────
# The Worker takes the last line of the text before the table (block tags break lines; markup-like text is
# dropped; the section heading when nothing fits). Python strips the same text without line breaks, so its title is
# all of it, and it has no heading fallback. The keyword map, built from titles, differs with it. Pinned until the
# owner classifies F49.
CASES.append(Case(
    name="f49.index.table_title_from_preceding_text", ids=["F49"], routes=BOLD_ROUTES,
    mode="pinned", decision="F49 (unclassified; F_TRIAGE.md §6)",
    py=lambda: [t["title"] for t in
                json.loads(_py(server.get_sec_filing_index, "AAPL", "10-K", "latest", TEN_K_ACC)())["index"]["tables"]],
    ts={"js": "return JSON.parse(await m.yahooFinance.getSecFilingIndex(...args)).index.tables.map((t) => t.title);",
        "args": ["AAPL", "10-K", "latest", TEN_K_ACC], "isolate": True},
    expect_py=["", "Item 1. Business 1 Item 1A. Risk Factors 5 PART I Item 1. Business Annual revenue grew in fiscal"
                   " 2024. Item 1A. Risk Factors Supply risk."],
    expect_ts=["", "Supply risk."],
))

CASES.append(Case(
    name="idx2261.outline.bold_headings_without_document_url", ids=["F1"], routes=BOLD_ROUTES, select=_strip_meta,
    py=_py(server.get_sec_filing_outline, "AAPL", "10-K", "latest", TEN_K_ACC, None),
    ts=_tool_ts("get_sec_filing_outline", {"ticker": "AAPL", "filing_type": "10-K", "period": "latest",
                                           "accession_number": TEN_K_ACC}),
))

# The outline names the bold Item headings in order, not an empty list.
CASES.append(Case(
    name="idx2261.outline.bold_headings_titles", ids=["F1"], routes=BOLD_ROUTES,
    py=lambda: [(e["level"], e["title"]) for e in
                json.loads(_py(server.get_sec_filing_outline, "AAPL", "10-K", "latest", TEN_K_ACC, None)())["outline"]],
    ts={"js": "const r = JSON.parse(await m.tools.callTool(args[0], args[1])); const v = r.data ?? r;"
              " return v.outline.map((e) => [e.level, e.title]);",
        "args": ["get_sec_filing_outline", {"ticker": "AAPL", "filing_type": "10-K", "accession_number": TEN_K_ACC}],
        "isolate": True},
    expect=[[1, "PART I"], [2, "Item 1. Business"], [2, "Item 1A. Risk Factors"], [1, "PART II"],
            [2, "Item 7. Management’s Discussion and Analysis"]],
))
