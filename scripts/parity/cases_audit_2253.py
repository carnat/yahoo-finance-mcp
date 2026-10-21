"""Parity cases for the 2.25.3 audit repairs (analysis/yahoo-finance-mcp/F_TRIAGE.md §5).

F32: get_sec_filing_section_markdown finds an inline-XBRL bold Item heading whose style quotes a font name
(Python returned NO_FILING_DATA), and the Worker decodes entities in the markdown (it kept &#160; and &#8217;).
"""

from __future__ import annotations

import json

from scripts.parity.cases_neutrality import AAPL_DOC, SECTION_ROUTES, _tool_case
from scripts.parity.harness import Case

import server  # noqa: E402

BOLD = "<span style=\"color:#000000;font-family:'Helvetica',sans-serif;font-size:9pt;font-weight:700\">"
PLAIN = "<span style=\"font-family:'Helvetica',sans-serif;font-size:9pt;font-weight:400\">"
BODY = " ".join("The Company&#8217;s net sales&#160;grew in fiscal year %d &amp; services rose." % y for y in range(2010, 2022))
IXBRL_HTML = (
    "<html><body>"
    # Table of contents: the same headings as links, which both runtimes skip.
    "<table><tr><td><a href=\"#i7\">Item 7.</a></td><td><a href=\"#i7\">Management&#8217;s Discussion</a></td><td>20</td></tr>"
    "<tr><td><a href=\"#i7a\">Item 7A.</a></td><td>Market Risk</td><td>31</td></tr></table>"
    f"<div id=\"i7\">{BOLD}Item 7.&#160;&#160;&#160;&#160;Management&#8217;s Discussion and Analysis</span></div>"
    f"<div>{PLAIN}{BODY}</span></div>"
    f"<div id=\"i7a\">{BOLD}Item 7A.&#160;&#160;&#160;&#160;Quantitative and Qualitative Disclosures About Market Risk</span></div>"
    f"<div>{PLAIN}Market risk text.</span></div>"
    "</body></html>"
)
IXBRL_ROUTES = {**SECTION_ROUTES, AAPL_DOC: {"body": IXBRL_HTML}}
DECODED_BODY = BODY.replace("&#8217;", "’").replace("&#160;", " ").replace("&amp;", "&")


def _markdown_view(v: dict) -> dict:
    data = v.get("data") if isinstance(v.get("data"), dict) else {}
    markdown = data.get("markdown") or ""
    return {"ok": v.get("ok"), "errorCode": (v.get("error") or {}).get("code"), "section": data.get("section"),
            "text": " ".join(markdown.split()), "hasRawEntity": "&#" in markdown or "&amp;" in markdown}


CASES = [
    _tool_case("audit2253.section_markdown.ixbrl_bold_item_with_quoted_font_is_found_and_decoded",
               "get_sec_filing_section_markdown", {"ticker": "AAPL", "section": "Item 7"},
               lambda: server.get_sec_filing_section_markdown("AAPL", section="Item 7"), _markdown_view,
               routes=IXBRL_ROUTES, ids=("F32",),
               expect={"ok": True, "errorCode": None,
                       "section": "Item 7. Management’s Discussion and Analysis",
                       "text": "Item 7. Management’s Discussion and Analysis " + " ".join(DECODED_BODY.split()),
                       "hasRawEntity": False}),
]


# F46: without document_url, list_sec_filing_tables lists the filing index's tables in both runtimes.
TABLE_HTML = (
    "<html><body><h2>Item 8. Financial Statements and Supplementary Data</h2>"
    "<p>CONSOLIDATED STATEMENTS OF OPERATIONS (In millions, except number of shares, which are reflected in thousands)</p>"
    "<table><tr><td></td><td>2025</td><td>2024</td></tr>"
    "<tr><td>Net sales</td><td>416,161</td><td>391,035</td></tr>"
    "<tr><td>Cost of sales</td><td>220,960</td><td>210,352</td></tr>"
    "<tr><td>Gross margin</td><td>195,201</td><td>180,683</td></tr></table>"
    "<table><tr><td>&#160;</td></tr></table>"
    "</body></html>"
)
TABLE_ROUTES = {**SECTION_ROUTES, AAPL_DOC: {"body": TABLE_HTML}}


def _tables_view(v: dict) -> dict:
    data = v.get("data") if isinstance(v.get("data"), dict) else {}
    keep = ("status", "tableCount", "usableTableCount", "excludedTableCount", "returnedCount", "hasMore", "accessionNumber", "filingType")
    return {**{k: data.get(k) for k in keep},
            "rowKeys": [sorted(t) for t in data.get("tables") or []],
            "rows": [{k: t.get(k) for k in ("tableIndex", "headers", "rowLabels", "unitScale", "confidence")} for t in data.get("tables") or []]}


CASES.append(_tool_case(
    "audit2253.list_sec_filing_tables.without_document_url_lists_index_tables",
    "list_sec_filing_tables", {"ticker": "AAPL"}, lambda: server.list_sec_filing_tables("AAPL"), _tables_view,
    routes=TABLE_ROUTES, ids=("F46",),
    expect={"status": "OK", "tableCount": 2, "usableTableCount": 1, "excludedTableCount": 1, "returnedCount": 1, "hasMore": False,
            "accessionNumber": "0000320193-24-000123", "filingType": "10-K",
            "rowKeys": [["confidence", "headers", "rowLabels", "sectionId", "tableIndex", "title", "unitScale"]],
            "rows": [{"tableIndex": 0, "headers": ["", "2025", "2024"], "rowLabels": ["Net sales", "Cost of sales", "Gross margin"],
                      "unitScale": "millions", "confidence": "HIGH"}]}))


# F32: the bold-heading title is the heading's whole block in both runtimes. Here the bold span sits in no block of
# its own, so the block runs to the paragraph's close (scripts/test_earnings_tools.py states the same).
LOOSE_SPAN_HTML = ('<html><body><div><a href="#risk">Item 1A. Risk Factors</a></div>'
                   '<span style="font-weight:700">Item 1A. Risk Factors</span><p>This is the actual risk factor disclosure.</p>'
                   '<span style="font-weight:700">Item 1B. Unresolved Staff Comments</span><p>Next section.</p></body></html>')


def _py_bounds():
    start, end, heading, toc, err = server._find_section_bounds(LOOSE_SPAN_HTML, "Item 1A", 1000)
    return {"start": start, "end": end, "heading": heading, "tocSkipped": toc, "errCode": err}


CASES.append(Case(
    name="audit2253.section_bounds.loose_bold_span_heading_reads_its_block", ids=["F32"], py=_py_bounds,
    ts={"js": f"const b = m.yahooFinance.findSectionBounds({json.dumps(LOOSE_SPAN_HTML)}, 'Item 1A', 1000); "
              "return {start: b.startIdx, end: b.endIdx, heading: b.foundHeading, tocSkipped: b.tocSkipped, errCode: b.errCode};"},
    expect={"start": LOOSE_SPAN_HTML.index('<span style="font-weight:700">Item 1A'),
            "end": LOOSE_SPAN_HTML.index('<span style="font-weight:700">Item 1B'),
            "heading": "Item 1A. Risk Factors This is the actual risk factor disclosure.", "tocSkipped": True, "errCode": None}))
