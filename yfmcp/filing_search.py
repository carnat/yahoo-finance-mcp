"""SEC filing text search over the text a reader sees.

Python mirror of worker/src/filing-search.ts; the two produce the same
projection, matches and payloads for the same HTML (scripts/test_filing_search.py
runs both on shared fixtures).

A filing's HTML is projected once into display text: hidden inline-XBRL data
(ix:header), scripts, styles and display:none blocks are dropped, entities are
decoded, whitespace is collapsed, each paragraph or table row ends in "\\n" and
table cells are joined by " | ". A folded copy of the same length (lowercase,
one apostrophe, one double quote, one dash) is what queries match against, so
match positions index both.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# ── Projection ────────────────────────────────────────────────────────────────

_SKIP_CONTENT_TAGS = {"script", "style", "head", "title", "noscript", "template", "ix:header"}
_BLOCK_TAGS = {
    "p", "div", "br", "li", "ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6", "section", "article",
    "blockquote", "hr", "dl", "dt", "dd", "center", "pre", "caption", "body", "html", "header",
    "footer", "nav", "aside", "figure", "figcaption", "page",
}
_TOKEN_RE = re.compile(
    r"<!--[\s\S]*?-->|<(/?)([A-Za-z][A-Za-z0-9_:.-]*)((?:[^>\"']|\"[^\"]*\"|'[^']*')*)>|[^<]+|<"
)
_ZERO_WIDTH_RE = re.compile("[\u200b-\u200d\u2060\ufeff\u00ad]")
# Whitespace spelled out, as the Worker does, so both collapse the same characters.
_WS_RUN_RE = re.compile("[\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+")
_ENTITY_RE = re.compile(r"&(#[xX][0-9a-fA-F]+|#[0-9]+|[A-Za-z][A-Za-z0-9]*);")
_NAMED_ENTITIES = {
    "nbsp": " ", "amp": "&", "lt": "<", "gt": ">", "quot": '"', "apos": "'", "rsquo": "\u2019", "lsquo": "\u2018",
    "rdquo": "\u201d", "ldquo": "\u201c", "sbquo": "\u201a", "bdquo": "\u201e", "ndash": "\u2013", "mdash": "\u2014",
    "hellip": "\u2026", "bull": "\u2022", "middot": "\u00b7", "reg": "\u00ae", "trade": "\u2122", "copy": "\u00a9",
    "sect": "\u00a7", "para": "\u00b6", "shy": "", "ensp": " ", "emsp": " ", "thinsp": " ", "zwj": "", "zwnj": "",
    "deg": "\u00b0", "times": "\u00d7", "euro": "\u20ac", "pound": "\u00a3", "yen": "\u00a5", "cent": "\u00a2",
    "frac12": "\u00bd", "frac14": "\u00bc", "frac34": "\u00be", "prime": "\u2032", "minus": "\u2212",
}
_APOSTROPHES = re.compile("[\u2018\u2019\u201a\u201b\u2032\u02bc\uff07]")
_QUOTES = re.compile("[\u201c\u201d\u201e\u201f\u2033\uff02]")
_DASHES = re.compile("[\u2010-\u2015\u2212\ufe58\ufe63\uff0d]")


def _collapse_spaces(value: str) -> str:
    return _WS_RUN_RE.sub(" ", value)


def decode_html_entities(text: str) -> str:
    def replace(m: re.Match) -> str:
        body = m.group(1)
        if body[0] == "#":
            is_hex = body[1] in "xX"
            try:
                code = int(body[2:] if is_hex else body[1:], 16 if is_hex else 10)
            except ValueError:
                return " "
            if code <= 0 or code > 0x10FFFF or 0xD800 <= code <= 0xDFFF:
                return " "
            return chr(code)
        named = _NAMED_ENTITIES.get(body.lower())
        return m.group(0) if named is None else named

    return _ENTITY_RE.sub(replace, text)


def fold_search_text(text: str) -> str:
    """Lowercase with one apostrophe, one double quote and one dash; the length never changes."""
    mapped = _DASHES.sub("-", _QUOTES.sub('"', _APOSTROPHES.sub("'", text)))
    lower = mapped.lower()
    if len(lower) == len(mapped):
        return lower
    return "".join(ch.lower() if len(ch.lower()) == 1 else ch for ch in mapped)


def _skip_element(html: str, name: str, start: int) -> int:
    """End of the element that opens just before start, counting nested elements of the same name."""
    tag_re = re.compile(rf"<(/?){re.escape(name)}(?=[\s/>])(?:[^>\"']|\"[^\"]*\"|'[^']*')*>", re.IGNORECASE)
    depth = 1
    for m in tag_re.finditer(html, start):
        if m.group(1):
            depth -= 1
            if depth == 0:
                return m.end()
        elif not m.group(0).endswith("/>"):
            depth += 1
    return len(html)


@dataclass
class HeadingAnchor:
    start: int
    level: int
    title: str


@dataclass
class TableSpan:
    table_index: int
    html_start: int
    html_end: int


@dataclass
class ProjectedSection:
    title: str
    level: int
    text_start: int


@dataclass
class ProjectedTable:
    table_index: int
    text_start: int
    text_end: int
    html_start: int
    html_end: int


@dataclass
class FilingTextProjection:
    text: str
    folded: str
    sections: list[ProjectedSection]
    tables: list[ProjectedTable]


def filing_table_spans(html: str) -> list[TableSpan]:
    """Table spans numbered as list_sec_filing_tables numbers them."""
    return [
        TableSpan(i, m.start(), m.end())
        for i, m in enumerate(re.finditer(r"<table[^>]*>[\s\S]*?</table>", html, re.IGNORECASE))
    ]


def project_filing_text(html: str, headings: list[HeadingAnchor], table_spans: list[TableSpan]) -> FilingTextProjection:
    parts: list[str] = []
    st = {
        "length": 0, "last": "\n", "pending_space": False, "table_depth": 0, "in_cell": False,
        "row_has_content": False, "pending_cell_sep": False, "cell_text": "", "prev_cell_text": "",
    }

    def write(s: str) -> None:
        if not s:
            return
        parts.append(s)
        st["length"] += len(s)
        st["last"] = s[-1]

    def line_break() -> None:
        st["pending_space"] = False
        if st["last"] != "\n":
            write("\n")

    def emit_text(raw: str) -> None:
        decoded = _collapse_spaces(_ZERO_WIDTH_RE.sub("", decode_html_entities(raw)))
        if not decoded:
            return
        trimmed = decoded.strip(" ")
        if not trimmed:
            if st["last"] != "\n":
                st["pending_space"] = True
            return
        if st["in_cell"] and st["pending_cell_sep"]:
            # "$" and "%" sit in cells of their own in financial tables.
            join = bool(re.fullmatch("[$\u20ac\u00a3\u00a5]", st["prev_cell_text"])) or trimmed[0] in "%)"
            write("" if join else " | ")
        elif (st["pending_space"] or decoded[0] == " ") and st["last"] != "\n":
            write(" ")
        st["pending_cell_sep"] = False
        write(trimmed)
        if st["in_cell"]:
            st["cell_text"] += (" " if st["cell_text"] else "") + trimmed
            st["row_has_content"] = True
        st["pending_space"] = decoded[-1] == " "

    sections: list[ProjectedSection] = []
    sorted_headings = sorted(headings, key=lambda h: h.start)
    tables: list[ProjectedTable] = []
    anchors = {"heading": 0, "table": 0, "open": None}

    def advance_anchors(pos: int) -> None:
        while anchors["heading"] < len(sorted_headings) and sorted_headings[anchors["heading"]].start <= pos:
            heading = sorted_headings[anchors["heading"]]
            anchors["heading"] += 1
            sections.append(ProjectedSection(heading.title, heading.level, st["length"]))
        open_table = anchors["open"]
        if open_table is not None and pos >= open_table.html_end:
            open_table.text_end = st["length"]
            tables.append(open_table)
            anchors["open"] = None
        while anchors["open"] is None and anchors["table"] < len(table_spans) and table_spans[anchors["table"]].html_start <= pos:
            span = table_spans[anchors["table"]]
            anchors["table"] += 1
            if pos >= span.html_end:
                continue  # inside skipped content
            anchors["open"] = ProjectedTable(span.table_index, st["length"], st["length"], span.html_start, span.html_end)

    lower_html = html.lower()
    pos = 0
    while True:
        m = _TOKEN_RE.search(html, pos)
        if m is None:
            break
        pos = m.end()
        advance_anchors(m.start())
        token = m.group(0)
        if token.startswith("<!--"):
            continue
        name = (m.group(2) or "").lower()
        if not name:
            emit_text(token)
            continue
        closing = m.group(1) == "/"
        attrs = m.group(3) or ""
        if not closing and name in _SKIP_CONTENT_TAGS:
            close = lower_html.find(f"</{name}", pos)
            close_end = -1 if close < 0 else lower_html.find(">", close)
            pos = len(html) if close_end < 0 else close_end + 1
            continue
        if not closing and not attrs.endswith("/") and re.search(r"display\s*:\s*none", attrs, re.IGNORECASE):
            pos = _skip_element(html, name, pos)
            continue
        if name == "table":
            st["table_depth"] = max(0, st["table_depth"] + (-1 if closing else 1))
            st["in_cell"] = False
            line_break()
        elif name == "tr":
            line_break()
            st.update(in_cell=False, row_has_content=False, pending_cell_sep=False, cell_text="", prev_cell_text="")
        elif name in ("td", "th"):
            if not closing:
                st["prev_cell_text"] = st["cell_text"]
                st["cell_text"] = ""
                st["in_cell"] = True
                if st["row_has_content"]:
                    st["pending_cell_sep"] = True
            else:
                st["in_cell"] = False
        elif name in _BLOCK_TAGS:
            if st["table_depth"] > 0 and st["in_cell"]:
                if st["last"] != "\n":
                    st["pending_space"] = True
            else:
                line_break()
    advance_anchors(len(html))
    if anchors["open"] is not None:
        anchors["open"].text_end = st["length"]
        tables.append(anchors["open"])
    text = "".join(parts)
    return FilingTextProjection(text, fold_search_text(text), sections, tables)


# ── Headings (as the Worker finds them) ───────────────────────────────────────

_WORKER_ENTITY_MAP = {"&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"', "&apos;": "'"}


def strip_html_tags(html: str) -> str:
    """The Worker's stripHtmlTags: tags removed, a few entities decoded, whitespace collapsed."""
    text = re.sub(r"<!--[\s\S]*?-->", " ", html)
    text = re.sub(r"<script\b[^>]*>[\s\S]*?</script[^>]*>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<style\b[^>]*>[\s\S]*?</style[^>]*>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)

    def replace(m: re.Match) -> str:
        entity = m.group(0)
        if entity in _WORKER_ENTITY_MAP:
            return _WORKER_ENTITY_MAP[entity]
        if entity.startswith("&#"):
            try:
                code = int(entity[2:-1], 10)
            except ValueError:
                return " "
            return chr(code) if 0 < code < 0x110000 and not 0xD800 <= code <= 0xDFFF else " "
        return " "

    text = re.sub(r"&(?:nbsp|amp|lt|gt|quot|apos|#[0-9]+|[a-z]+);", replace, text, flags=re.IGNORECASE)
    return _collapse_spaces(re.sub(r"<[^>]+>", " ", text)).strip(" ")


def is_toc_match(html: str, start: int, end: int) -> bool:
    sl = html[max(0, start - 30):min(len(html), end + 30)]
    if re.search(r"<a\b[^>]*\bhref\s*=\s*['\"]#[^'\"]*['\"]", sl, re.IGNORECASE):
        return True
    before = html[max(0, start - 100):start]
    if re.search(r"<a\b[^>]*\bhref\s*=\s*['\"]#[^'\"]*['\"][^>]*>\s*$", before, re.IGNORECASE):
        return True
    context = html[max(0, start - 150):min(len(html), end + 150)]
    if "...." in context or ". . ." in context or "&#183;" in context or "&middot;" in context:
        return True
    plain = re.sub(r"<[^>]+>", " ", context)
    return bool(re.search(r"\.{3,}\s*[0-9]+|\.\s*\.\s*\.\s*[0-9]+", plain))


_BOLD_OPEN = (
    r"(?:<b\b[^>]*>|<strong\b[^>]*>|<span\b[^>]*style\s*=\s*"
    r"(?:\"[^\"]*font-weight\s*:\s*(?:bold|[6-9]00)[^\"]*\"|'[^']*font-weight\s*:\s*(?:bold|[6-9]00)[^']*')[^>]*>)"
)
_BOLD_ITEM_RE = re.compile(
    _BOLD_OPEN + r"\s*((?:<[^>]+>\s*)*Item(?:\s|&nbsp;|&#160;|\u00a0)+[0-9]+[A-Z]?(?:\s*\([^)]+\))?[^<]{0,160})",
    re.IGNORECASE,
)
_BOLD_PART_RE = re.compile(
    _BOLD_OPEN + r"\s*((?:<[^>]+>\s*)*Part(?:\s|&nbsp;|&#160;|\u00a0)+[IVX]+\b[^<]{0,80})",
    re.IGNORECASE,
)


def _section_item_token(value: str) -> str | None:
    m = re.search(r"\bitem\s+([0-9]+[a-z]?)\b", value, re.IGNORECASE)
    return f"item {m.group(1).lower()}" if m else None


def _heading_block_text(html: str, pos: int) -> str:
    rest = html[pos:pos + 2000]
    close = re.search(r"</(?:p|div|tr|h[1-6])>", rest, re.IGNORECASE)
    block = rest[:close.start()] if close else rest[:600]
    return _collapse_spaces(strip_html_tags(block).replace("\u00a0", " ")).strip(" ")[:200]


def filing_item_headings(html: str) -> list[HeadingAnchor]:
    """Part and Item headings in bold text, outside the table of contents (the Worker's filingItemHeadings)."""
    headings: list[HeadingAnchor] = []
    seen: set[str] = set()

    def add(pattern: re.Pattern, level: int, token_of) -> None:
        for m in pattern.finditer(html):
            if is_toc_match(html, m.start(), m.end()):
                continue
            title = _heading_block_text(html, m.start())
            if not title or re.search(r"\s[0-9]{1,3}$", title):
                continue
            token = token_of(title)
            if not token or token in seen:
                continue
            seen.add(token)
            headings.append(HeadingAnchor(m.start(), level, title))

    def part_token(text: str) -> str | None:
        m = re.match(r"part\s+([ivx]+)\b", text, re.IGNORECASE)
        return f"part {m.group(1).lower()}" if m else None

    add(_BOLD_PART_RE, 1, part_token)
    add(_BOLD_ITEM_RE, 2, _section_item_token)
    return sorted(headings, key=lambda h: h.start)


def search_headings(html: str) -> list[HeadingAnchor]:
    """Item and Part headings; filings that mark them with <h1>-<h6> instead of bold text use those."""
    bold = filing_item_headings(html)
    if bold:
        return bold
    out: list[HeadingAnchor] = []
    for m in re.finditer(r"<h([1-6])[^>]*>([\s\S]*?)</h\1>", html, re.IGNORECASE):
        title = strip_html_tags(m.group(2))
        level = 1 if re.match(r"part\s+[ivx]+\b", title, re.IGNORECASE) else 2 if re.match(r"item\s+[0-9]+[a-z]?\b", title, re.IGNORECASE) else 0
        if level == 0 or re.search(r"\s[0-9]{1,3}$", title) or is_toc_match(html, m.start(), m.end()):
            continue
        out.append(HeadingAnchor(m.start(), level, title[:200]))
    return out


def section_heading_matches(requested: str, heading: str) -> bool:
    requested_text = " ".join(requested.lower().split())
    heading_text = " ".join(heading.lower().split())
    requested_item = _section_item_token(requested_text)
    if requested_item:
        return requested_item == _section_item_token(heading_text)
    return requested_text in heading_text or heading_text in requested_text


def merge_financial_cells(cells: list[str]) -> list[str]:
    """The Worker's mergeFinancialCells: "$", "%" and ")" cells join the adjacent value."""
    out: list[str] = []
    prefix = ""
    for index, raw in enumerate(cells):
        cell = _collapse_spaces(raw).strip(" ")
        if not cell:
            if index == 0:
                out.append("")
            continue
        if re.fullmatch("[$\u20ac\u00a3\u00a5]", cell):
            prefix += cell
            continue
        if re.fullmatch(r"%|\)|\)%|%\)", cell) and out:
            out[-1] += cell
            continue
        cell = re.sub(r"\s+\)$", ")", re.sub(r"^\(\s+", "(", cell))
        out.append(prefix + cell)
        prefix = ""
    if prefix:
        out.append(prefix)
    return out


# ── Queries ───────────────────────────────────────────────────────────────────

@dataclass
class QueryTerm:
    text: str
    exact: bool = False


@dataclass
class NearSpec:
    a: QueryTerm
    b: QueryTerm
    within_words: int


@dataclass
class ParsedQuery:
    terms: list[QueryTerm] = field(default_factory=list)
    near: list[NearSpec] = field(default_factory=list)
    exclude: list[QueryTerm] = field(default_factory=list)


def _query_term(raw: str, exact: bool = False) -> QueryTerm | None:
    text = _collapse_spaces(raw).strip(" ")
    if len(text) >= 2 and text.startswith('"') and text.endswith('"'):
        text = text[1:-1].strip(" ")
        exact = True
    return QueryTerm(text, exact) if text else None


def parse_search_query(query: str) -> ParsedQuery:
    """search_query syntax: "exact phrase", A NEAR/n B (single words or quoted phrases),
    -excluded or -"excluded phrase"; other consecutive words form one phrase."""
    tokens: list[dict] = []
    for m in re.finditer(r'(-?)"([^"]*)"|(\S+)', query):
        if m.group(3) is not None:
            bare = m.group(3)
            neg = len(bare) > 1 and bare.startswith("-")
            tokens.append({"neg": neg, "quoted": False, "text": bare[1:] if neg else bare, "used": False})
        else:
            tokens.append({"neg": m.group(1) == "-", "quoted": True, "text": m.group(2), "used": False})
    parsed = ParsedQuery()
    i = 1
    while i + 1 < len(tokens):
        op = tokens[i]
        near = re.fullmatch(r"NEAR/([0-9]{1,3})", op["text"], re.IGNORECASE) if not op["quoted"] and not op["neg"] else None
        left, right = tokens[i - 1], tokens[i + 1]
        if near and not left["used"] and not left["neg"] and not right["neg"]:
            a = _query_term(left["text"], left["quoted"])
            b = _query_term(right["text"], right["quoted"])
            if a and b:
                parsed.near.append(NearSpec(a, b, max(1, min(200, int(near.group(1))))))
                left["used"] = op["used"] = right["used"] = True
                i += 2
                continue
        i += 1
    bare: list[str] = []

    def flush() -> None:
        term = _query_term(" ".join(bare))
        if term:
            parsed.terms.append(term)
        bare.clear()

    for token in tokens:
        if token["used"]:
            flush()
            continue
        if token["neg"]:
            flush()
            term = _query_term(token["text"], token["quoted"])
            if term:
                parsed.exclude.append(term)
        elif token["quoted"]:
            flush()
            term = _query_term(token["text"], True)
            if term:
                parsed.terms.append(term)
        else:
            bare.append(token["text"])
    flush()
    return parsed


def terms_from_list(values: list | None) -> list[QueryTerm]:
    """Terms from search_terms: each entry is one phrase; a quoted entry is exact."""
    out = []
    for value in values or []:
        term = _query_term(str(value if value is not None else ""))
        if term:
            out.append(term)
    return out


def term_label(term: QueryTerm) -> str:
    return f'"{term.text}"' if term.exact else term.text


def term_regex(term: QueryTerm, mode: str) -> re.Pattern | None:
    """Whole words by default: no letter or digit may touch either end, spaces in a
    phrase also match hyphens, and a term ending in a letter also matches its plural
    and possessive unless it is exact."""
    folded = fold_search_text(_collapse_spaces(term.text).strip(" "))
    if not folded:
        return None
    if mode == "substring":
        return re.compile(re.escape(folded))
    body = "[ -]".join(re.escape(part) for part in folded.split(" "))
    lead = r"(?<![^\W_])" if folded[0].isalnum() else ""
    plural = "(?:'s|es|s)?" if not term.exact and folded[-1].isalpha() else ""
    tail = r"(?![^\W_])" if folded[-1].isalnum() else ""
    return re.compile(f"{lead}{body}{plural}{tail}")


@dataclass
class Hit:
    label: str
    terms: list[str]
    start: int
    end: int


MAX_HITS_PER_TERM = 5000


def find_hits(folded: str, pattern: re.Pattern, label: str, scope_start: int, scope_end: int) -> tuple[list[Hit], int, bool]:
    hits: list[Hit] = []
    count = 0
    for m in pattern.finditer(folded, scope_start):
        if m.start() >= scope_end:
            break
        if m.end() == m.start():
            continue
        count += 1
        if len(hits) < MAX_HITS_PER_TERM:
            hits.append(Hit(label, [label], m.start(), m.end()))
        else:
            return hits, count, True
    return hits, count, False


def _words_between(folded: str, start: int, end: int) -> int:
    return 0 if end <= start else len(re.findall(r"[^\W_]+", folded[start:end]))


def near_hits(folded: str, a_hits: list[Hit], b_hits: list[Hit], within_words: int, label: str, terms: list[str]) -> list[Hit]:
    """First hit of a with a hit of b at most within_words words away, in either order."""
    out: list[Hit] = []
    max_chars = within_words * 30 + 60
    lo = 0
    for a in a_hits:
        while lo < len(b_hits) and b_hits[lo].end < a.start - max_chars:
            lo += 1
        best = None
        best_words = float("inf")
        i = lo
        while i < len(b_hits) and b_hits[i].start <= a.end + max_chars:
            b = b_hits[i]
            i += 1
            if b.start < a.end and a.start < b.end:
                continue  # the same words
            words = _words_between(folded, a.end, b.start) if b.start >= a.end else _words_between(folded, b.end, a.start)
            if words <= within_words and words < best_words:
                best, best_words = b, words
        if best is not None:
            out.append(Hit(label, list(terms), min(a.start, best.start), max(a.end, best.end)))
    return out


# ── Context ───────────────────────────────────────────────────────────────────

_ABBREVIATIONS = {
    "inc", "corp", "co", "ltd", "llc", "no", "nos", "mr", "mrs", "ms", "dr", "st", "vs", "etc", "jan", "feb",
    "mar", "apr", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec", "approx", "fig", "incl", "avg", "ref",
}
_CLOSERS = "\"'\u201d\u2019)]"


def _sentence_ends_at(text: str, i: int) -> bool:
    ch = text[i]
    if ch not in ".!?":
        return False
    j = i + 1
    while j < len(text) and text[j] in _CLOSERS:
        j += 1
    if j < len(text) and text[j] not in " \n":
        return False
    if ch != ".":
        return True
    k = i
    while k > 0 and (text[k - 1].isalpha() or text[k - 1] == "."):
        k -= 1
    word = text[k:i]
    if not word:
        return True
    if "." in word:
        return False  # U.S. and e.g.
    if len(word) == 1 and word.isupper():
        return False  # an initial
    return word.lower() not in _ABBREVIATIONS


def _sentence_start_before(text: str, pos: int, floor: int) -> int:
    for i in range(pos - 1, floor, -1):
        if _sentence_ends_at(text, i):
            s = i + 1
            while s < pos and text[s] in _CLOSERS + " ":
                s += 1
            return s
    return floor


def _sentence_end_after(text: str, pos: int, ceiling: int) -> int:
    for i in range(pos, ceiling):
        if _sentence_ends_at(text, i):
            e = i + 1
            while e < ceiling and text[e] in _CLOSERS:
                e += 1
            return e
    return ceiling


@dataclass
class MatchContext:
    text: str
    start: int
    end: int
    table: ProjectedTable | None = None
    row_label: str | None = None
    table_title: str | None = None


def _line_bounds(text: str, pos: int) -> tuple[int, int]:
    start = text.rfind("\n", 0, pos) + 1
    nl = text.find("\n", pos)
    return start, (len(text) if nl < 0 else nl)


def _window_around(text: str, lo: int, hi: int, pos: int, budget: int) -> tuple[int, int, bool, bool]:
    start = max(lo, min(pos - budget // 3, hi - budget))
    end = min(hi, start + budget)
    if start > lo:
        space = text.find(" ", start)
        if 0 <= space < pos:
            start = space + 1
    if end < hi:
        space = text.rfind(" ", 0, end + 1)
        if space > pos:
            end = space
    return start, end, start > lo, end < hi


def table_at(tables: list[ProjectedTable], pos: int) -> ProjectedTable | None:
    lo, hi = 0, len(tables) - 1
    while lo <= hi:
        mid = (lo + hi) >> 1
        t = tables[mid]
        if pos < t.text_start:
            hi = mid - 1
        elif pos >= t.text_end:
            lo = mid + 1
        else:
            return t
    return None


def _table_title_before(text: str, table_start: int) -> str | None:
    end = table_start
    for _ in range(3):
        if end <= 0:
            break
        if text[end - 1] == "\n":
            end -= 1
        start = text.rfind("\n", 0, end) + 1
        line = text[start:end].strip(" ")
        if 8 <= len(line) <= 300 and not re.fullmatch(r"[0-9]+", line) and line.lower() != "table of contents":
            return line
        end = start
    return None


def hit_sentence(p: FilingTextProjection, start: int, end: int) -> tuple[int, int]:
    """The sentence holding a hit, or its row in a table."""
    line_start, line_end = _line_bounds(p.text, start)
    if table_at(p.tables, start):
        return line_start, line_end
    return _sentence_start_before(p.text, start, line_start), _sentence_end_after(p.text, max(end, start), line_end)


def match_context(p: FilingTextProjection, start: int, end: int, budget: int) -> MatchContext:
    """The passage around a match. Text: the sentence holding it, grown by whole
    neighbouring sentences of its paragraph while they fit the budget; a short
    heading-like line takes the paragraphs that follow it. Tables: the row."""
    text = p.text
    line_start, line_end = _line_bounds(text, start)
    table = table_at(p.tables, start)
    if table:
        if line_end - line_start <= budget:
            s, e, pre, post = line_start, line_end, False, False
        else:
            s, e, pre, post = _window_around(text, line_start, line_end, start, budget)
        row = text[line_start:line_end]
        label = row.split(" | ")[0].strip(" ")
        keep_label = " | " in row and label and not re.fullmatch(r"[0-9$\u20ac\u00a3\u00a5(),.%\s-]+", label)
        return MatchContext(
            f"{'...' if pre else ''}{text[s:e].strip(' ')}{'...' if post else ''}",
            line_start, max(line_end, end), table, label if keep_label else None,
            _table_title_before(text, table.text_start),
        )
    lo, hi = line_start, line_end
    line = text[line_start:line_end].strip(" ")
    if len(line) < 120 and not re.search("[.!?:;][\"'\u201d\u2019)]?$", line):
        # A heading: its passage is the text that follows.
        while hi < len(text) and hi - lo < budget:
            next_start, next_end = _line_bounds(text, hi + 1)
            if next_start <= hi or table_at(p.tables, next_start):
                break
            hi = next_end
            if next_end - next_start >= 120:
                break
    pre = post = False
    if hi - lo <= budget:
        s, e = lo, hi
    else:
        s = _sentence_start_before(text, start, lo)
        e = _sentence_end_after(text, max(end, s), hi)
        if e - s > budget:
            s, e, pre, post = _window_around(text, s, e, start, budget)
            pre = pre or s > lo
            post = post or e < hi
        else:
            grew = True
            while grew:
                grew = False
                if e < hi:
                    nxt = _sentence_end_after(text, e + 1, hi)
                    if nxt > e and nxt - s <= budget:
                        e = nxt
                        grew = True
                if s > lo:
                    # Step back over the previous sentence's closing punctuation first.
                    k = s
                    while k > lo and (text[k - 1] in _CLOSERS + ".!?" or text[k - 1] in " \n"):
                        k -= 1
                    prev = _sentence_start_before(text, k, lo)
                    if prev < s and e - prev <= budget:
                        s = prev
                        grew = True
            pre = s > lo
            post = e < hi
    body = text[s:e].replace("\n", " ").strip(" ")
    return MatchContext(f"{'...' if pre else ''}{body}{'...' if post else ''}", s, max(e, end))


# ── Sections ──────────────────────────────────────────────────────────────────

def section_at(sections: list[ProjectedSection], pos: int) -> ProjectedSection | None:
    lo, hi = 0, len(sections) - 1
    found = None
    while lo <= hi:
        mid = (lo + hi) >> 1
        if sections[mid].text_start <= pos:
            found = sections[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    return found


def section_weight(title: str | None, document_type: str) -> int:
    """Weight of a section in relevance ranking: risk factors and MD&A first."""
    if document_type != "primary":
        return 2
    t = (title or "").lower()
    if re.search(r"risk factors|management.s discussion", t):
        return 3
    if re.search(r"quantitative and qualitative|\bitem\s+1\.?\s+business\b", t):
        return 2
    if re.search(r"financial statements|supplementary data|legal proceedings", t):
        return 1
    return 0


# ── Matches ───────────────────────────────────────────────────────────────────

@dataclass
class SearchDocument:
    key: str
    document_type: str
    default_section: str | None
    projection: FilingTextProjection
    scope_start: int
    scope_end: int


@dataclass
class SearchMatch:
    doc: SearchDocument
    terms: list[str]
    hit_count: int
    start: int
    context: MatchContext
    section: str | None
    score: int = 0


@dataclass
class SearchSpec:
    terms: list[QueryTerm]
    near: list[NearSpec]
    exclude: list[QueryTerm]
    mode: str
    budget: int


@dataclass
class DocumentSearchResult:
    matches: list[SearchMatch]
    term_hits: dict[str, tuple[int, bool]]
    # Hits dropped by excluded terms.
    excluded_count: int


def search_document(doc: SearchDocument, spec: SearchSpec) -> DocumentSearchResult:
    folded = doc.projection.folded
    term_hits: dict[str, tuple[int, bool]] = {}
    hits: list[Hit] = []
    for term in spec.terms:
        pattern = term_regex(term, spec.mode)
        label = term_label(term)
        if pattern is None or label in term_hits:
            continue
        found, count, capped = find_hits(folded, pattern, label, doc.scope_start, doc.scope_end)
        term_hits[label] = (count, capped)
        hits.extend(found)
    for near in spec.near:
        pattern_a = term_regex(near.a, spec.mode)
        pattern_b = term_regex(near.b, spec.mode)
        label = f"{term_label(near.a)} NEAR/{near.within_words} {term_label(near.b)}"
        if pattern_a is None or pattern_b is None or label in term_hits:
            continue
        a_hits, _, a_capped = find_hits(folded, pattern_a, term_label(near.a), doc.scope_start, doc.scope_end)
        b_hits, _, b_capped = find_hits(folded, pattern_b, term_label(near.b), doc.scope_start, doc.scope_end)
        found = near_hits(folded, a_hits, b_hits, near.within_words, label, [label])
        term_hits[label] = (len(found), a_capped or b_capped)
        hits.extend(found)
    hits.sort(key=lambda h: (h.start, h.end))
    # An excluded term drops each hit whose sentence (or table row) holds it.
    exclude_patterns = [p for p in (term_regex(t, spec.mode) for t in spec.exclude) if p is not None]
    if exclude_patterns:
        kept = []
        for hit in hits:
            s, e = hit_sentence(doc.projection, hit.start, hit.end)
            if not any(p.search(folded[s:e]) for p in exclude_patterns):
                kept.append(hit)
    else:
        kept = hits
    excluded_count = len(hits) - len(kept)
    matches: list[SearchMatch] = []
    current: SearchMatch | None = None
    for hit in kept:
        if current and current.context.start <= hit.start < current.context.end:
            for term in hit.terms:
                if term not in current.terms:
                    current.terms.append(term)
            current.hit_count += 1
            continue
        if current:
            matches.append(current)
        context = match_context(doc.projection, hit.start, hit.end, spec.budget)
        section = section_at(doc.projection.sections, hit.start)
        current = SearchMatch(doc, list(hit.terms), 1, hit.start, context, section.title if section else doc.default_section)
    if current:
        matches.append(current)
    for match in matches:
        match.score = (
            3 * len(match.terms)
            + section_weight(match.section, doc.document_type)
            + (0 if match.context.table else 1)
            + min(match.hit_count - 1, 2)
        )
    return DocumentSearchResult(matches, term_hits, excluded_count)


def order_matches(matches: list[SearchMatch], order: str, doc_order: dict[str, int]) -> list[SearchMatch]:
    """Relevance: each term's matches best first, taken one per term in turn so that
    one common term cannot fill the page. Document: filing order."""
    def position(m: SearchMatch) -> tuple[int, int]:
        return doc_order.get(m.doc.key, 0), m.start

    if order == "document":
        return sorted(matches, key=position)
    groups: dict[str, list[SearchMatch]] = {}
    for match in matches:
        groups.setdefault(match.terms[0], []).append(match)
    queues = [sorted(group, key=lambda m: (-m.score, *position(m))) for group in groups.values()]
    out: list[SearchMatch] = []
    round_ = 0
    while len(out) < len(matches):
        for queue in queues:
            if round_ < len(queue):
                out.append(queue[round_])
        round_ += 1
    return out


def hits_by_section(matches: list[SearchMatch]) -> list[dict]:
    out: dict[str, dict] = {}
    for match in matches:
        key = match.section or ""
        entry = out.get(key)
        if entry:
            entry["matchCount"] += 1
            entry["hitCount"] += match.hit_count
            entry["first"] = min(entry["first"], match.start)
        else:
            out[key] = {"section": match.section, "matchCount": 1, "hitCount": match.hit_count, "first": match.start}
    rows = sorted(out.values(), key=lambda r: r["first"])
    return [{k: v for k, v in row.items() if k != "first"} for row in rows]


def section_hint_scope(p: FilingTextProjection, hint: str) -> tuple[int, int, str] | None:
    """Scope of a section hint: the matching heading up to the next heading of the same or a higher level."""
    for idx, heading in enumerate(p.sections):
        if section_heading_matches(hint, heading.title) or hint.lower() in heading.title.lower():
            nxt = next((s for s in p.sections[idx + 1:] if s.level <= heading.level), None)
            return heading.text_start, (nxt.text_start if nxt else len(p.text)), heading.title
    # No heading: a line that starts with the hint and is no table-of-contents row.
    folded = fold_search_text(_collapse_spaces(hint).strip(" "))
    if not folded:
        return None
    pattern = re.compile(r"(^|\n)(?:item [0-9]+[a-z]?\.? )?" + re.escape(folded) + r"[^\n]{0,80}(?=\n)")
    for m in pattern.finditer(p.folded):
        start = m.start() + len(m.group(1))
        line = p.text[start:m.end()]
        if re.search(r"\s[0-9]{1,3}$", line) or table_at(p.tables, start):
            continue
        nxt = next((s for s in p.sections if s.text_start > start), None)
        return start, (nxt.text_start if nxt else min(len(p.text), start + 300_000)), line.strip(" ")
    return None


def match_payload(match: SearchMatch) -> dict:
    words = len(re.findall(r"[^\W\d_]{2,}", match.context.text))
    out: dict = {
        "term": match.terms[0],
        "terms": match.terms,
        "hitCount": match.hit_count,
        "sectionHeading": match.section or "",
        "contextText": match.context.text,
        "inTable": match.context.table is not None,
        "textOffset": match.start,
        "confidence": "MEDIUM" if words >= 5 else "LOW",
    }
    if match.context.table:
        out["tableIndex"] = match.context.table.table_index
        out["rowLabel"] = match.context.row_label
        out["tableTitle"] = match.context.table_title
    return out
