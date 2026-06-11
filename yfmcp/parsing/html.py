"""HTML table parsing helpers used by the SEC filing fallback layer.

Extracted from server.py in Phase 1 of the refactoring plan.
"""

import html as _html_module
import re as _re


# ---------------------------------------------------------------------------
# HTML table parsing helpers
# ---------------------------------------------------------------------------

def _strip_html_tags(html_str: str) -> str:
    """Remove HTML tags and decode entities to produce plain text."""
    text = _re.sub(r"<[^>]+>", " ", html_str)
    text = _html_module.unescape(text)
    return _re.sub(r"\s+", " ", text).strip()


def _parse_html_table(table_html: str) -> list[list[str]]:
    """Parse an HTML table into a list of rows, each a list of plain-text cell strings."""
    rows: list[list[str]] = []
    tr_pat = _re.compile(r"<tr[^>]*>(.*?)</tr>", _re.IGNORECASE | _re.DOTALL)
    td_pat = _re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", _re.IGNORECASE | _re.DOTALL)
    for tr_m in tr_pat.finditer(table_html):
        row = [_strip_html_tags(td_m.group(1)) for td_m in td_pat.finditer(tr_m.group(1))]
        if row:
            rows.append(row)
    return rows


def _parse_numeric_cell(text: str) -> float | None:
    """Parse a table cell's text as a number. Returns None when not parseable."""
    s = text.strip().replace(",", "").replace(" ", "").replace("$", "").replace("%", "")
    # Parentheses → negative value: (123) → -123
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    multiplier = 1.0
    if s.upper().endswith("B"):
        multiplier, s = 1_000_000_000.0, s[:-1]
    elif s.upper().endswith("M"):
        multiplier, s = 1_000_000.0, s[:-1]
    elif s.upper().endswith("K"):
        multiplier, s = 1_000.0, s[:-1]
    try:
        return float(s) * multiplier
    except ValueError:
        return None


def _detect_unit_multiplier(table_html: str, context_html: str) -> float:
    """Detect the monetary unit scale from a table caption/nearby headings.

    Returns 1_000_000 (millions) if not found — the most common 10-K scale.
    """
    combined = (table_html + context_html).lower()
    if "in billions" in combined or "$ billions" in combined:
        return 1_000_000_000.0
    if "in thousands" in combined or "$ thousands" in combined or "in thousands)" in combined:
        return 1_000.0
    if "in millions" in combined or "$ millions" in combined or "in millions)" in combined:
        return 1_000_000.0
    # Default assumption for 10-K financials: millions
    return 1_000_000.0


# ---------------------------------------------------------------------------
# Markdown conversion helpers (used by get_sec_filing_section_markdown)
# ---------------------------------------------------------------------------

_MD_MAX_CELL_CHARS = 60


def _html_table_to_markdown(table_html: str) -> str:
    """Convert an HTML table to a pipe-delimited Markdown table."""
    rows = _parse_html_table(table_html)
    if not rows:
        return ""
    # Build pipe-separated rows
    md_lines: list[str] = []
    for i, row in enumerate(rows[:50]):  # Limit to 50 rows
        line = "| " + " | ".join(cell[:_MD_MAX_CELL_CHARS] for cell in row) + " |"
        md_lines.append(line)
        if i == 0:
            # Add separator after header
            md_lines.append("| " + " | ".join("---" for _ in row) + " |")
    return "\n".join(md_lines)


def _html_to_markdown_fallback(html: str, section_start: int, section_end: int) -> str:
    """Convert a section of HTML to basic Markdown using built-in parser."""
    section_html = html[section_start:section_end]

    # Remove scripts/styles iteratively to prevent nested/malformed pattern bypass
    _script_re = _re.compile(r'<script\b[^>]*>[\s\S]*?</\s*script[^>]*>', _re.IGNORECASE)
    _style_re = _re.compile(r'<style\b[^>]*>[\s\S]*?</\s*style[^>]*>', _re.IGNORECASE)
    while True:
        next_s = _script_re.sub('', section_html)
        next_s = _style_re.sub('', next_s)
        if next_s == section_html:
            break
        section_html = next_s

    # Convert headers
    for level in range(1, 7):
        prefix = "#" * level
        section_html = _re.sub(
            rf'<h{level}[^>]*>(.*?)</h{level}>',
            lambda m, p=prefix: f"\n{p} {_strip_html_tags(m.group(1))}\n",
            section_html,
            flags=_re.IGNORECASE | _re.DOTALL,
        )

    # Convert tables to markdown
    table_re = _re.compile(r'<table[^>]*>[\s\S]*?</table>', _re.IGNORECASE)
    for t_match in table_re.finditer(section_html):
        md_table = _html_table_to_markdown(t_match.group(0))
        if md_table:
            section_html = section_html.replace(t_match.group(0), f"\n{md_table}\n")

    # Convert list items
    section_html = _re.sub(r'<li[^>]*>(.*?)</li>', r'- \1', section_html, flags=_re.IGNORECASE | _re.DOTALL)

    # Convert paragraphs/divs to newlines
    section_html = _re.sub(r'<(?:p|div|br)[^>]*/?\s*>', '\n', section_html, flags=_re.IGNORECASE)
    section_html = _re.sub(r'</(?:p|div)>', '\n', section_html, flags=_re.IGNORECASE)

    # Strip remaining HTML tags
    text = _strip_html_tags(section_html)

    # Clean up whitespace
    text = _re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()
