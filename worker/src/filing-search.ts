/**
 * SEC filing text search over the text a reader sees.
 *
 * A filing's HTML is projected once into display text: hidden inline-XBRL
 * data (ix:header), scripts, styles and display:none blocks are dropped,
 * entities are decoded, whitespace is collapsed, each paragraph or table row
 * ends in "\n" and table cells are joined by " | ". A folded copy of the same
 * length (lowercase, one apostrophe, one double quote, one dash) is what
 * queries match against, so match positions index both.
 *
 * The Python runtime mirrors this module in yfmcp/filing_search.py; the two
 * are checked against the same fixtures for identical output.
 */

export interface HeadingAnchor {
  /** Position of the heading in the HTML. */
  start: number;
  level: number;
  title: string;
}

export interface TableSpan {
  tableIndex: number;
  htmlStart: number;
  htmlEnd: number;
}

export interface ProjectedSection {
  title: string;
  level: number;
  textStart: number;
}

export interface ProjectedTable {
  tableIndex: number;
  textStart: number;
  textEnd: number;
  htmlStart: number;
  htmlEnd: number;
}

export interface FilingTextProjection {
  text: string;
  folded: string;
  sections: ProjectedSection[];
  tables: ProjectedTable[];
}

export type MatchMode = "word" | "substring";
export type MatchOrder = "relevance" | "document";

export interface QueryTerm {
  text: string;
  /** Exact terms do not match plural forms. */
  exact: boolean;
}

export interface NearSpec {
  a: QueryTerm;
  b: QueryTerm;
  withinWords: number;
}

export interface ParsedQuery {
  terms: QueryTerm[];
  near: NearSpec[];
  exclude: QueryTerm[];
}

// ── Projection ────────────────────────────────────────────────────────────────

const SKIP_CONTENT_TAGS = new Set(["script", "style", "head", "title", "noscript", "template", "ix:header"]);
const BLOCK_TAGS = new Set([
  "p", "div", "br", "li", "ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6", "section", "article",
  "blockquote", "hr", "dl", "dt", "dd", "center", "pre", "caption", "body", "html", "header",
  "footer", "nav", "aside", "figure", "figcaption", "page",
]);
const TOKEN_RE = /<!--[\s\S]*?-->|<(\/?)([A-Za-z][\w:.-]*)((?:[^>"']|"[^"]*"|'[^']*')*)>|[^<]+|</g;
const ZERO_WIDTH_RE = /[\u200B-\u200D\u2060\uFEFF\u00AD]/g;
// Whitespace spelled out, so that the Python mirror collapses the same characters.
const WS_RUN_RE = /[\t\n\v\f\r \u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+/g;

function collapseSpaces(value: string): string {
  return value.replace(WS_RUN_RE, " ");
}
const NAMED_ENTITIES: Record<string, string> = {
  nbsp: " ", amp: "&", lt: "<", gt: ">", quot: "\"", apos: "'", rsquo: "\u2019", lsquo: "\u2018",
  rdquo: "\u201D", ldquo: "\u201C", sbquo: "\u201A", bdquo: "\u201E", ndash: "\u2013", mdash: "\u2014",
  hellip: "\u2026", bull: "\u2022", middot: "\u00B7", reg: "\u00AE", trade: "\u2122", copy: "\u00A9",
  sect: "\u00A7", para: "\u00B6", shy: "", ensp: " ", emsp: " ", thinsp: " ", zwj: "", zwnj: "",
  deg: "\u00B0", times: "\u00D7", euro: "\u20AC", pound: "\u00A3", yen: "\u00A5", cent: "\u00A2",
  frac12: "\u00BD", frac14: "\u00BC", frac34: "\u00BE", prime: "\u2032", minus: "\u2212",
};

export function decodeHtmlEntities(text: string): string {
  return text.replace(/&(#[xX][0-9a-fA-F]+|#\d+|[A-Za-z][A-Za-z0-9]*);/g, (entity, body: string) => {
    if (body[0] === "#") {
      const hex = body[1] === "x" || body[1] === "X";
      const code = parseInt(body.slice(hex ? 2 : 1), hex ? 16 : 10);
      if (!Number.isFinite(code) || code <= 0 || code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) return " ";
      return String.fromCodePoint(code);
    }
    const named = NAMED_ENTITIES[body.toLowerCase()];
    return named ?? entity;
  });
}

/** Lowercase with one apostrophe, one double quote and one dash; the length never changes. */
export function foldSearchText(text: string): string {
  const mapped = text
    .replace(/[\u2018\u2019\u201A\u201B\u2032\u02BC\uFF07]/g, "'")
    .replace(/[\u201C\u201D\u201E\u201F\u2033\uFF02]/g, "\"")
    .replace(/[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]/g, "-");
  const lower = mapped.toLowerCase();
  if (lower.length === mapped.length) return lower;
  let out = "";
  for (const ch of mapped) {
    const low = ch.toLowerCase();
    out += low.length === ch.length ? low : ch;
  }
  return out;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** End of the element that opens just before `from`, counting nested elements of the same name. */
function skipElement(html: string, name: string, from: number): number {
  const re = new RegExp(`<(/?)${escapeRegExp(name)}(?=[\\s/>])(?:[^>"']|"[^"]*"|'[^']*')*>`, "gi");
  re.lastIndex = from;
  let depth = 1;
  let m: RegExpExecArray | null;
  while ((m = re.exec(html)) !== null) {
    if (m[1]) {
      depth -= 1;
      if (depth === 0) return re.lastIndex;
    } else if (!m[0].endsWith("/>")) {
      depth += 1;
    }
  }
  return html.length;
}

/** Table spans numbered as list_sec_filing_tables numbers them. */
export function filingTableSpans(html: string): TableSpan[] {
  const spans: TableSpan[] = [];
  let index = 0;
  for (const m of html.matchAll(/<table[^>]*>[\s\S]*?<\/table>/gi)) {
    const start = m.index ?? 0;
    spans.push({ tableIndex: index++, htmlStart: start, htmlEnd: start + m[0].length });
  }
  return spans;
}

export function projectFilingText(html: string, headings: HeadingAnchor[], tableSpans: TableSpan[]): FilingTextProjection {
  const parts: string[] = [];
  let length = 0;
  let last = "\n";
  let pendingSpace = false;
  let tableDepth = 0;
  let inCell = false;
  let rowHasContent = false;
  let pendingCellSep = false;
  let cellText = "";
  let prevCellText = "";

  const write = (s: string) => {
    if (!s) return;
    parts.push(s);
    length += s.length;
    last = s[s.length - 1];
  };
  const lineBreak = () => {
    pendingSpace = false;
    if (last !== "\n") write("\n");
  };
  const emitText = (raw: string) => {
    const decoded = collapseSpaces(decodeHtmlEntities(raw).replace(ZERO_WIDTH_RE, ""));
    if (!decoded) return;
    const trimmed = decoded.trim();
    if (!trimmed) {
      if (last !== "\n") pendingSpace = true;
      return;
    }
    if (inCell && pendingCellSep) {
      // "$" and "%" sit in cells of their own in financial tables.
      const join = /^[$\u20AC\u00A3\u00A5]$/.test(prevCellText) || /^[%)]/.test(trimmed);
      write(join ? "" : " | ");
    } else if ((pendingSpace || decoded[0] === " ") && last !== "\n") {
      write(" ");
    }
    pendingCellSep = false;
    write(trimmed);
    if (inCell) {
      cellText += (cellText ? " " : "") + trimmed;
      rowHasContent = true;
    }
    pendingSpace = decoded[decoded.length - 1] === " ";
  };

  const sections: ProjectedSection[] = [];
  const sortedHeadings = [...headings].sort((x, y) => x.start - y.start);
  let headingIdx = 0;
  const tables: ProjectedTable[] = [];
  let tableIdx = 0;
  let openTable: ProjectedTable | null = null;
  const advanceAnchors = (pos: number) => {
    while (headingIdx < sortedHeadings.length && sortedHeadings[headingIdx].start <= pos) {
      const heading = sortedHeadings[headingIdx++];
      sections.push({ title: heading.title, level: heading.level, textStart: length });
    }
    if (openTable && pos >= openTable.htmlEnd) {
      openTable.textEnd = length;
      tables.push(openTable);
      openTable = null;
    }
    while (!openTable && tableIdx < tableSpans.length && tableSpans[tableIdx].htmlStart <= pos) {
      const span = tableSpans[tableIdx++];
      if (pos >= span.htmlEnd) continue; // inside skipped content
      openTable = { tableIndex: span.tableIndex, textStart: length, textEnd: length, htmlStart: span.htmlStart, htmlEnd: span.htmlEnd };
    }
  };

  const lowerHtml = html.toLowerCase();
  TOKEN_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = TOKEN_RE.exec(html)) !== null) {
    advanceAnchors(m.index);
    const token = m[0];
    if (token.startsWith("<!--")) continue;
    const name = m[2]?.toLowerCase();
    if (!name) {
      emitText(token);
      continue;
    }
    const closing = m[1] === "/";
    const attrs = m[3] ?? "";
    if (!closing && SKIP_CONTENT_TAGS.has(name)) {
      const close = lowerHtml.indexOf(`</${name}`, TOKEN_RE.lastIndex);
      const closeEnd = close < 0 ? -1 : lowerHtml.indexOf(">", close);
      TOKEN_RE.lastIndex = closeEnd < 0 ? html.length : closeEnd + 1;
      continue;
    }
    if (!closing && !attrs.endsWith("/") && /display\s*:\s*none/i.test(attrs)) {
      TOKEN_RE.lastIndex = skipElement(html, name, TOKEN_RE.lastIndex);
      continue;
    }
    if (name === "table") {
      tableDepth += closing ? -1 : 1;
      if (tableDepth < 0) tableDepth = 0;
      inCell = false;
      lineBreak();
    } else if (name === "tr") {
      lineBreak();
      inCell = false;
      rowHasContent = false;
      pendingCellSep = false;
      cellText = "";
      prevCellText = "";
    } else if (name === "td" || name === "th") {
      if (!closing) {
        prevCellText = cellText;
        cellText = "";
        inCell = true;
        if (rowHasContent) pendingCellSep = true;
      } else {
        inCell = false;
      }
    } else if (BLOCK_TAGS.has(name)) {
      if (tableDepth > 0 && inCell) {
        if (last !== "\n") pendingSpace = true;
      } else {
        lineBreak();
      }
    }
  }
  advanceAnchors(html.length);
  if (openTable) {
    (openTable as ProjectedTable).textEnd = length;
    tables.push(openTable);
  }
  const text = parts.join("");
  return { text, folded: foldSearchText(text), sections, tables };
}

// ── Queries ───────────────────────────────────────────────────────────────────

function queryTerm(raw: string, exact = false): QueryTerm | null {
  let text = collapseSpaces(raw).trim();
  if (text.length >= 2 && text.startsWith("\"") && text.endsWith("\"")) {
    text = text.slice(1, -1).trim();
    exact = true;
  }
  return text ? { text, exact } : null;
}

/**
 * search_query syntax: "exact phrase", A NEAR/n B (single words or quoted
 * phrases), -excluded or -"excluded phrase"; other consecutive words form
 * one phrase. A query without operators is a single phrase.
 */
export function parseSearchQuery(query: string): ParsedQuery {
  const tokens: { neg: boolean; quoted: boolean; text: string; used: boolean }[] = [];
  for (const m of query.matchAll(/(-?)"([^"]*)"|(\S+)/g)) {
    if (m[3] !== undefined) {
      const bare = m[3];
      const neg = bare.length > 1 && bare.startsWith("-");
      tokens.push({ neg, quoted: false, text: neg ? bare.slice(1) : bare, used: false });
    } else {
      tokens.push({ neg: m[1] === "-", quoted: true, text: m[2], used: false });
    }
  }
  const parsed: ParsedQuery = { terms: [], near: [], exclude: [] };
  for (let i = 1; i + 1 < tokens.length; i++) {
    const op = tokens[i];
    const near = !op.quoted && !op.neg ? op.text.match(/^NEAR\/(\d{1,3})$/i) : null;
    const left = tokens[i - 1];
    const right = tokens[i + 1];
    if (!near || left.used || left.neg || right.neg) continue;
    const a = queryTerm(left.text, left.quoted);
    const b = queryTerm(right.text, right.quoted);
    if (!a || !b) continue;
    parsed.near.push({ a, b, withinWords: Math.max(1, Math.min(200, parseInt(near[1], 10))) });
    left.used = op.used = right.used = true;
    i += 1;
  }
  let bare: string[] = [];
  const flush = () => {
    const term = queryTerm(bare.join(" "));
    if (term) parsed.terms.push(term);
    bare = [];
  };
  for (const token of tokens) {
    if (token.used) {
      flush();
      continue;
    }
    if (token.neg) {
      flush();
      const term = queryTerm(token.text, token.quoted);
      if (term) parsed.exclude.push(term);
    } else if (token.quoted) {
      flush();
      const term = queryTerm(token.text, true);
      if (term) parsed.terms.push(term);
    } else {
      bare.push(token.text);
    }
  }
  flush();
  return parsed;
}

/** Terms from search_terms: each entry is one phrase; a quoted entry is exact. */
export function termsFromList(values: unknown[]): QueryTerm[] {
  const out: QueryTerm[] = [];
  for (const value of values) {
    const term = queryTerm(String(value ?? ""));
    if (term) out.push(term);
  }
  return out;
}

export function termLabel(term: QueryTerm): string {
  return term.exact ? `"${term.text}"` : term.text;
}

/**
 * Regex over folded text. Whole words by default: no letter or digit may
 * touch either end, spaces in a phrase also match hyphens, and a term ending
 * in a letter also matches its plural and possessive unless it is exact.
 */
export function termRegex(term: QueryTerm, mode: MatchMode): RegExp | null {
  const folded = foldSearchText(collapseSpaces(term.text).trim());
  if (!folded) return null;
  if (mode === "substring") return new RegExp(escapeRegExp(folded), "gu");
  const body = folded.split(" ").map(escapeRegExp).join("[ -]");
  const lead = /^[\p{L}\p{N}]/u.test(folded) ? "(?<![\\p{L}\\p{N}])" : "";
  const plural = !term.exact && /\p{L}$/u.test(folded) ? "(?:'s|es|s)?" : "";
  const tail = /[\p{L}\p{N}]$/u.test(folded) ? "(?![\\p{L}\\p{N}])" : "";
  return new RegExp(`${lead}${body}${plural}${tail}`, "gu");
}

export interface Hit {
  label: string;
  terms: string[];
  start: number;
  end: number;
}

export const MAX_HITS_PER_TERM = 5000;

export function findHits(
  folded: string,
  re: RegExp,
  label: string,
  scopeStart: number,
  scopeEnd: number,
): { hits: Hit[]; count: number; capped: boolean } {
  const hits: Hit[] = [];
  let count = 0;
  re.lastIndex = scopeStart;
  let m: RegExpExecArray | null;
  while ((m = re.exec(folded)) !== null) {
    if (m.index >= scopeEnd) break;
    if (m[0].length === 0) {
      re.lastIndex += 1;
      continue;
    }
    count += 1;
    if (hits.length < MAX_HITS_PER_TERM) hits.push({ label, terms: [label], start: m.index, end: m.index + m[0].length });
    else return { hits, count, capped: true };
  }
  return { hits, count, capped: false };
}

function wordsBetween(folded: string, from: number, to: number): number {
  if (to <= from) return 0;
  return (folded.slice(from, to).match(/[\p{L}\p{N}]+/gu) ?? []).length;
}

/** First hit of `a` with a hit of `b` at most withinWords words away, in either order. */
export function nearHits(folded: string, aHits: Hit[], bHits: Hit[], withinWords: number, label: string, terms: string[]): Hit[] {
  const out: Hit[] = [];
  const maxChars = withinWords * 30 + 60;
  let lo = 0;
  for (const a of aHits) {
    while (lo < bHits.length && bHits[lo].end < a.start - maxChars) lo += 1;
    let best: Hit | null = null;
    let bestWords = Infinity;
    for (let i = lo; i < bHits.length && bHits[i].start <= a.end + maxChars; i++) {
      const b = bHits[i];
      if (b.start < a.end && a.start < b.end) continue; // the same words
      const words = b.start >= a.end ? wordsBetween(folded, a.end, b.start) : wordsBetween(folded, b.end, a.start);
      if (words <= withinWords && words < bestWords) {
        best = b;
        bestWords = words;
      }
    }
    if (best) out.push({ label, terms, start: Math.min(a.start, best.start), end: Math.max(a.end, best.end) });
  }
  return out;
}

// ── Context ───────────────────────────────────────────────────────────────────

const ABBREVIATIONS = new Set([
  "inc", "corp", "co", "ltd", "llc", "no", "nos", "mr", "mrs", "ms", "dr", "st", "vs", "etc", "jan", "feb",
  "mar", "apr", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec", "approx", "fig", "incl", "avg", "ref",
]);

/** A "." "!" or "?" at i ends a sentence: a space or line end follows and it closes no abbreviation. */
function sentenceEndsAt(text: string, i: number): boolean {
  const ch = text[i];
  if (ch !== "." && ch !== "!" && ch !== "?") return false;
  let j = i + 1;
  while (j < text.length && /["'\u201D\u2019)\]]/.test(text[j])) j += 1;
  if (j < text.length && text[j] !== " " && text[j] !== "\n") return false;
  if (ch !== ".") return true;
  let k = i;
  while (k > 0 && /[\p{L}.]/u.test(text[k - 1])) k -= 1;
  const word = text.slice(k, i);
  if (!word) return true;
  if (word.includes(".")) return false; // U.S. and e.g.
  if (/^\p{Lu}$/u.test(word)) return false; // an initial
  return !ABBREVIATIONS.has(word.toLowerCase());
}

function sentenceStartBefore(text: string, pos: number, floor: number): number {
  for (let i = pos - 1; i > floor; i--) {
    if (sentenceEndsAt(text, i)) {
      let s = i + 1;
      while (s < pos && /["'\u201D\u2019)\] ]/.test(text[s])) s += 1;
      return s;
    }
  }
  return floor;
}

function sentenceEndAfter(text: string, pos: number, ceiling: number): number {
  for (let i = pos; i < ceiling; i++) {
    if (sentenceEndsAt(text, i)) {
      let e = i + 1;
      while (e < ceiling && /["'\u201D\u2019)\]]/.test(text[e])) e += 1;
      return e;
    }
  }
  return ceiling;
}

/** The sentence holding a hit, or its row in a table. */
export function hitSentence(p: FilingTextProjection, hit: { start: number; end: number }): [number, number] {
  const [lineStart, lineEnd] = lineBounds(p.text, hit.start);
  if (tableAt(p.tables, hit.start)) return [lineStart, lineEnd];
  return [sentenceStartBefore(p.text, hit.start, lineStart), sentenceEndAfter(p.text, Math.max(hit.end, hit.start), lineEnd)];
}

export interface MatchContext {
  text: string;
  start: number;
  end: number;
  table: ProjectedTable | null;
  rowLabel: string | null;
  tableTitle: string | null;
}

function lineBounds(text: string, pos: number): [number, number] {
  const start = text.lastIndexOf("\n", pos - 1) + 1;
  const nl = text.indexOf("\n", pos);
  return [start, nl < 0 ? text.length : nl];
}

function windowAround(text: string, lo: number, hi: number, pos: number, budget: number): [number, number, boolean, boolean] {
  let start = Math.max(lo, Math.min(pos - Math.floor(budget / 3), hi - budget));
  let end = Math.min(hi, start + budget);
  if (start > lo) {
    const space = text.indexOf(" ", start);
    if (space >= 0 && space < pos) start = space + 1;
  }
  if (end < hi) {
    const space = text.lastIndexOf(" ", end);
    if (space > pos) end = space;
  }
  return [start, end, start > lo, end < hi];
}

export function tableAt(tables: ProjectedTable[], pos: number): ProjectedTable | null {
  let lo = 0;
  let hi = tables.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const t = tables[mid];
    if (pos < t.textStart) hi = mid - 1;
    else if (pos >= t.textEnd) lo = mid + 1;
    else return t;
  }
  return null;
}

function tableTitleBefore(text: string, tableStart: number): string | null {
  let end = tableStart;
  for (let n = 0; n < 3 && end > 0; n++) {
    if (text[end - 1] === "\n") end -= 1;
    const start = text.lastIndexOf("\n", end - 1) + 1;
    const line = text.slice(start, end).trim();
    if (line.length >= 8 && line.length <= 300 && !/^\d+$/.test(line) && !/^table of contents$/i.test(line)) return line;
    end = start;
  }
  return null;
}

/**
 * The passage around a match. Text: the sentence holding it, grown by whole
 * neighbouring sentences of its paragraph while they fit the budget; a short
 * heading-like line takes the paragraphs that follow it. Tables: the row.
 */
export function matchContext(p: FilingTextProjection, start: number, end: number, budget: number): MatchContext {
  const text = p.text;
  const [lineStart, lineEnd] = lineBounds(text, start);
  const table = tableAt(p.tables, start);
  if (table) {
    const [s, e, pre, post] = lineEnd - lineStart <= budget
      ? [lineStart, lineEnd, false, false]
      : windowAround(text, lineStart, lineEnd, start, budget);
    const row = text.slice(lineStart, lineEnd);
    const label = row.split(" | ")[0].trim();
    return {
      text: `${pre ? "..." : ""}${text.slice(s, e).trim()}${post ? "..." : ""}`,
      start: lineStart,
      end: Math.max(lineEnd, end),
      table,
      rowLabel: row.includes(" | ") && label && !/^[\d$\u20AC\u00A3\u00A5(),.%\s-]+$/.test(label) ? label : null,
      tableTitle: tableTitleBefore(text, table.textStart),
    };
  }
  let lo = lineStart;
  let hi = lineEnd;
  const line = text.slice(lineStart, lineEnd).trim();
  if (line.length < 120 && !/[.!?:;]["'\u201D\u2019)]?$/.test(line)) {
    // A heading: its passage is the text that follows.
    while (hi < text.length && hi - lo < budget) {
      const [nextStart, nextEnd] = lineBounds(text, hi + 1);
      if (nextStart <= hi || tableAt(p.tables, nextStart)) break;
      hi = nextEnd;
      if (nextEnd - nextStart >= 120) break;
    }
  }
  let s: number;
  let e: number;
  let pre = false;
  let post = false;
  if (hi - lo <= budget) {
    s = lo;
    e = hi;
  } else {
    s = sentenceStartBefore(text, start, lo);
    e = sentenceEndAfter(text, Math.max(end, s), hi);
    if (e - s > budget) {
      [s, e, pre, post] = windowAround(text, s, e, start, budget);
      pre = pre || s > lo;
      post = post || e < hi;
    } else {
      let grew = true;
      while (grew) {
        grew = false;
        if (e < hi) {
          const next = sentenceEndAfter(text, e + 1, hi);
          if (next > e && next - s <= budget) {
            e = next;
            grew = true;
          }
        }
        if (s > lo) {
          // Step back over the previous sentence's closing punctuation first.
          let k = s;
          while (k > lo && /[\s"'\u201D\u2019)\].!?]/.test(text[k - 1])) k -= 1;
          const prev = sentenceStartBefore(text, k, lo);
          if (prev < s && e - prev <= budget) {
            s = prev;
            grew = true;
          }
        }
      }
      pre = s > lo;
      post = e < hi;
    }
  }
  const body = text.slice(s, e).replace(/\n/g, " ").trim();
  return {
    text: `${pre ? "..." : ""}${body}${post ? "..." : ""}`,
    start: s,
    end: Math.max(e, end),
    table: null,
    rowLabel: null,
    tableTitle: null,
  };
}

// ── Sections ──────────────────────────────────────────────────────────────────

export function sectionAt(sections: ProjectedSection[], pos: number): ProjectedSection | null {
  let lo = 0;
  let hi = sections.length - 1;
  let found: ProjectedSection | null = null;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (sections[mid].textStart <= pos) {
      found = sections[mid];
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return found;
}

/** Weight of a section in relevance ranking: risk factors and MD&A first. */
export function sectionWeight(title: string | null, documentType: string): number {
  if (documentType !== "primary") return 2;
  const t = (title ?? "").toLowerCase();
  if (/risk factors|management.s discussion/.test(t)) return 3;
  if (/quantitative and qualitative|\bitem\s+1\.?\s+business\b/.test(t)) return 2;
  if (/financial statements|supplementary data|legal proceedings/.test(t)) return 1;
  return 0;
}

// ── Matches ───────────────────────────────────────────────────────────────────

export interface SearchDocument {
  key: string;
  documentType: string;
  /** Section title for a document without Item headings, such as an exhibit. */
  defaultSection: string | null;
  projection: FilingTextProjection;
  scopeStart: number;
  scopeEnd: number;
}

export interface SearchMatch {
  doc: SearchDocument;
  terms: string[];
  hitCount: number;
  start: number;
  context: MatchContext;
  section: string | null;
  score: number;
}

export interface TermStat {
  term: string;
  hitCount: number;
  hitCountCapped: boolean;
  matchCount: number;
  returnedCount: number;
}

export interface DocumentSearchResult {
  matches: SearchMatch[];
  termHits: Map<string, { count: number; capped: boolean }>;
  /** Hits dropped by excluded terms. */
  excludedCount: number;
}

export interface SearchSpec {
  terms: QueryTerm[];
  near: NearSpec[];
  exclude: QueryTerm[];
  mode: MatchMode;
  budget: number;
}

export function searchDocument(doc: SearchDocument, spec: SearchSpec): DocumentSearchResult {
  const { folded } = doc.projection;
  const termHits = new Map<string, { count: number; capped: boolean }>();
  const hits: Hit[] = [];
  for (const term of spec.terms) {
    const re = termRegex(term, spec.mode);
    const label = termLabel(term);
    if (!re || termHits.has(label)) continue;
    const found = findHits(folded, re, label, doc.scopeStart, doc.scopeEnd);
    termHits.set(label, { count: found.count, capped: found.capped });
    hits.push(...found.hits);
  }
  for (const near of spec.near) {
    const reA = termRegex(near.a, spec.mode);
    const reB = termRegex(near.b, spec.mode);
    const label = `${termLabel(near.a)} NEAR/${near.withinWords} ${termLabel(near.b)}`;
    if (!reA || !reB || termHits.has(label)) continue;
    const a = findHits(folded, reA, termLabel(near.a), doc.scopeStart, doc.scopeEnd);
    const b = findHits(folded, reB, termLabel(near.b), doc.scopeStart, doc.scopeEnd);
    const found = nearHits(folded, a.hits, b.hits, near.withinWords, label, [label]);
    termHits.set(label, { count: found.length, capped: a.capped || b.capped });
    hits.push(...found);
  }
  hits.sort((x, y) => x.start - y.start || x.end - y.end);
  // An excluded term drops each hit whose sentence (or table row) holds it.
  const excludeRes = spec.exclude.map((term) => termRegex(term, spec.mode)).filter((re): re is RegExp => re != null);
  const kept = excludeRes.length === 0 ? hits : hits.filter((hit) => {
    const [s, e] = hitSentence(doc.projection, hit);
    const slice = folded.slice(s, e);
    return !excludeRes.some((re) => {
      re.lastIndex = 0;
      return re.test(slice);
    });
  });
  const excludedCount = hits.length - kept.length;
  const matches: SearchMatch[] = [];
  let current: SearchMatch | null = null;
  for (const hit of kept) {
    if (current && hit.start >= current.context.start && hit.start < current.context.end) {
      for (const term of hit.terms) if (!current.terms.includes(term)) current.terms.push(term);
      current.hitCount += 1;
      continue;
    }
    if (current) matches.push(current);
    const context = matchContext(doc.projection, hit.start, hit.end, spec.budget);
    const section = sectionAt(doc.projection.sections, hit.start)?.title ?? doc.defaultSection;
    current = { doc, terms: [...hit.terms], hitCount: 1, start: hit.start, context, section, score: 0 };
  }
  if (current) matches.push(current);
  for (const match of matches) {
    match.score = 3 * match.terms.length
      + sectionWeight(match.section, doc.documentType)
      + (match.context.table ? 0 : 1)
      + Math.min(match.hitCount - 1, 2);
  }
  return { matches, termHits, excludedCount };
}

/**
 * Relevance: each term's matches best first, taken one per term in turn so
 * that one common term cannot fill the page. Document: filing order.
 */
export function orderMatches(matches: SearchMatch[], order: MatchOrder, docOrder: Map<string, number>): SearchMatch[] {
  const byPosition = (x: SearchMatch, y: SearchMatch) =>
    (docOrder.get(x.doc.key) ?? 0) - (docOrder.get(y.doc.key) ?? 0) || x.start - y.start;
  if (order === "document") return [...matches].sort(byPosition);
  const groups = new Map<string, SearchMatch[]>();
  for (const match of matches) {
    const key = match.terms[0];
    const group = groups.get(key);
    if (group) group.push(match);
    else groups.set(key, [match]);
  }
  const queues = [...groups.values()].map((group) => group.sort((x, y) => y.score - x.score || byPosition(x, y)));
  const out: SearchMatch[] = [];
  for (let round = 0; out.length < matches.length; round++) {
    for (const queue of queues) if (round < queue.length) out.push(queue[round]);
  }
  return out;
}

export function hitsBySection(matches: SearchMatch[]): { section: string | null; matchCount: number; hitCount: number }[] {
  const out = new Map<string, { section: string | null; matchCount: number; hitCount: number; first: number }>();
  for (const match of matches) {
    const key = match.section ?? "";
    const entry = out.get(key);
    if (entry) {
      entry.matchCount += 1;
      entry.hitCount += match.hitCount;
      entry.first = Math.min(entry.first, match.start);
    } else {
      out.set(key, { section: match.section, matchCount: 1, hitCount: match.hitCount, first: match.start });
    }
  }
  return [...out.values()].sort((x, y) => x.first - y.first).map(({ first: _first, ...rest }) => rest);
}

/** Scope of a section hint: the matching heading up to the next heading of the same or a higher level. */
export function sectionHintScope(p: FilingTextProjection, hint: string, matches: (hint: string, title: string) => boolean): { start: number; end: number; title: string } | null {
  const idx = p.sections.findIndex((s) => matches(hint, s.title));
  if (idx >= 0) {
    const heading = p.sections[idx];
    const next = p.sections.slice(idx + 1).find((s) => s.level <= heading.level);
    return { start: heading.textStart, end: next?.textStart ?? p.text.length, title: heading.title };
  }
  // No heading: a line that starts with the hint and is no table-of-contents row.
  const folded = foldSearchText(collapseSpaces(hint).trim());
  if (!folded) return null;
  const re = new RegExp(`(^|\\n)(?:item [0-9]+[a-z]?\\.? )?${escapeRegExp(folded)}[^\\n]{0,80}(?=\\n)`, "g");
  let m: RegExpExecArray | null;
  while ((m = re.exec(p.folded)) !== null) {
    const start = m.index + m[1].length;
    const line = p.text.slice(start, start + m[0].length - m[1].length);
    if (/\s\d{1,3}$/.test(line) || tableAt(p.tables, start)) continue;
    const next = p.sections.find((s) => s.textStart > start);
    return { start, end: next?.textStart ?? Math.min(p.text.length, start + 300_000), title: line.trim() };
  }
  return null;
}

export function matchPayload(match: SearchMatch, includeDocument: boolean): Record<string, unknown> {
  const words = (match.context.text.match(/[\p{L}]{2,}/gu) ?? []).length;
  const out: Record<string, unknown> = {
    term: match.terms[0],
    terms: match.terms,
    hitCount: match.hitCount,
    sectionHeading: match.section ?? "",
    contextText: match.context.text,
    inTable: match.context.table != null,
    textOffset: match.start,
    confidence: words >= 5 ? "MEDIUM" : "LOW",
  };
  if (match.context.table) {
    out.tableIndex = match.context.table.tableIndex;
    out.rowLabel = match.context.rowLabel;
    out.tableTitle = match.context.tableTitle;
  }
  if (includeDocument) out.documentKey = match.doc.key;
  return out;
}
