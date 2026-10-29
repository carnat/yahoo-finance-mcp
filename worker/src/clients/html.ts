/**
 * HTML and filing-table parsing (Phase 4 unit 3, moved out of yahoo-finance.ts and news/shared.ts): tag stripping and
 * entity decoding, readable text and topic paragraphs, HTML table rows with financial cells merged, numeric cells,
 * and the stated monetary unit scale of a filing table.
 */

/** Parse a single HTML <table> into a list of rows (each row is a list of plain-text cells). */
export function parseHtmlTable(tableHtml: string): string[][] {
  const rows: string[][] = [];
  const trRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
  let trMatch: RegExpExecArray | null;
  while ((trMatch = trRe.exec(tableHtml)) !== null) {
    const row: string[] = [];
    let tdMatch: RegExpExecArray | null;
    while ((tdMatch = tdRe.exec(trMatch[1])) !== null) {
      row.push(stripHtmlTags(tdMatch[1]));
    }
    if (row.length > 0) rows.push(row);
  }
  return rows;
}

export function stripHtmlTags(html: string): string {
  const sanitizedHtml = html
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script[^>]*>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style[^>]*>/gi, " ")
    .replace(/\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, " ");
  // Remove all HTML tags first, then decode entities in a single pass to avoid
  // double-unescaping. Inline tags render without a break: "FINANC</span><span>IAL"
  // is one word (ASTS 10-Q headings, 2.4.5); whitespace in the HTML still separates.
  const noTags = sanitizedHtml.replace(/<\/?(?:span|font|b|i|u|em|strong|a|sup|sub|small|ix:[a-z]+)\b[^>]*>/gi, "").replace(/<[^>]+>/g, " ");
  const decoded = decodeHtmlEntities(noTags);
  return decoded.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

const HTML_ENTITY_MAP: Record<string, string> = {
  "&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"', "&apos;": "'",
  // Bullets and punctuation releases use, decoded as Python's html.unescape does (2.5.4).
  "&bull;": "\u2022", "&middot;": "\u00b7", "&rsquo;": "\u2019", "&lsquo;": "\u2018", "&ldquo;": "\u201c", "&rdquo;": "\u201d",
  "&ndash;": "\u2013", "&mdash;": "\u2014", "&hellip;": "\u2026",
};

/** Decode HTML entities in one pass; other named entities become a space. */
export function decodeHtmlEntities(text: string): string {
  return text.replace(/&(?:nbsp|amp|lt|gt|quot|apos|#\d+|#x[0-9a-f]+|[a-z]+);/gi, (entity) => {
    if (entity in HTML_ENTITY_MAP) return HTML_ENTITY_MAP[entity];
    if (/^&#x/i.test(entity)) {
      // "&#x2022;" (ASTS release bullets) was left undecoded, so bullets did not split.
      const code = parseInt(entity.slice(3, -1), 16);
      return Number.isFinite(code) && code > 0 && code <= 0x10ffff ? String.fromCodePoint(code) : " ";
    }
    if (entity.startsWith("&#")) {
      const code = parseInt(entity.slice(2, -1), 10);
      return isNaN(code) ? " " : String.fromCharCode(code);
    }
    return " ";
  });
}

export function sanitizeFilingHtml(html: string): string {
  return html
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script[^>]*>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style[^>]*>/gi, " ")
    .replace(/\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, " ");
}

export function htmlToReadableText(html: string): string {
  const blockBroken = sanitizeFilingHtml(html)
    .replace(/<(?:br|\/p|\/div|\/li|\/tr|\/h[1-6]|\/section)\b[^>]*>/gi, "\n")
    .replace(/<(?:p|div|li|tr|h[1-6]|section)\b[^>]*>/gi, "\n");
  const withoutTags = blockBroken.replace(/<[^>]+>/g, " ");
  const entityMap: Record<string, string> = {
    "&nbsp;": " ",
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&apos;": "'",
  };
  const decoded = withoutTags.replace(/&(?:nbsp|amp|lt|gt|quot|apos|#\d+|[a-z]+);/gi, (entity) => {
    if (entity in entityMap) return entityMap[entity];
    if (entity.startsWith("&#")) {
      const code = parseInt(entity.slice(2, -1), 10);
      return Number.isNaN(code) ? " " : String.fromCharCode(code);
    }
    return " ";
  });
  return decoded
    .replace(/[^\S\n]+/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

export function filterParagraphsByTopics(text: string, topics: string[], maxChars = 1000): Record<string, unknown>[] {
  if (!topics.length) return [];
  const paragraphs = text
    .split(/\n\s*\n|\n/)
    .map((paragraph) => paragraph.trim())
    .filter((paragraph) => paragraph.length > 20);
  const topicPatterns = topics.map((topic) => ({
    topic,
    pattern: new RegExp(topic.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i"),
  }));
  const results: Record<string, unknown>[] = [];
  for (const paragraph of paragraphs) {
    const matchedTopics = topicPatterns
      .filter(({ pattern }) => pattern.test(paragraph))
      .map(({ topic }) => topic);
    if (matchedTopics.length) {
      results.push({
        paragraph: paragraph.length > maxChars ? `${paragraph.slice(0, maxChars).trimEnd()}...` : paragraph,
        matchedTopics,
      });
    }
  }
  return results;
}

/**
 * Financial statement tables put "$", "%" and a closing ")" in cells of
 * their own, so the same period sits in a different column on each row.
 * Merge those cells into the adjacent value and drop empty cells:
 * ["Americas", "$", "178,353", "7", "%"] -> ["Americas", "$178,353", "7%"].
 */
export function mergeFinancialCells(cells: string[]): string[] {
  const out: string[] = [];
  let prefix = "";
  for (const [index, raw] of cells.entries()) {
    let cell = raw.replace(/\s+/g, " ").trim();
    // An empty first cell is the row's (missing) label; keep its position so
    // an unlabeled total row stays aligned with the labeled rows.
    if (!cell) {
      if (index === 0) out.push("");
      continue;
    }
    if (/^[$€£¥]$/.test(cell)) {
      prefix += cell;
      continue;
    }
    if (/^(?:%|\)|\)%|%\))$/.test(cell) && out.length > 0) {
      out[out.length - 1] += cell;
      continue;
    }
    // "( 95,699 )" -> "(95,699)"
    cell = cell.replace(/^\(\s+/, "(").replace(/\s+\)$/, ")");
    out.push(prefix + cell);
    prefix = "";
  }
  if (prefix) out.push(prefix);
  return out;
}

/** Table rows with financial cells merged (see mergeFinancialCells); empty rows dropped. */
export function parseFinancialTableRows(tableHtml: string): string[][] {
  return parseHtmlTable(tableHtml).map(mergeFinancialCells).filter((row) => row.some((cell) => cell !== ""));
}

/** Parse a numeric cell value, handling commas, parens, and $/%/scale suffixes. */
export function parseNumericCell(text: string): number | null {
  let s = text.replace(/,/g, "").replace(/\s/g, "").replace(/[$€£¥]/g, "").replace(/%/g, "");
  if (s.startsWith("(") && s.endsWith(")")) s = "-" + s.slice(1, -1);
  let mult = 1;
  if (/b$/i.test(s)) { mult = 1e9; s = s.slice(0, -1); }
  else if (/m$/i.test(s)) { mult = 1e6; s = s.slice(0, -1); }
  else if (/k$/i.test(s)) { mult = 1e3; s = s.slice(0, -1); }
  const n = parseFloat(s);
  return isNaN(n) ? null : n * mult;
}

// Amount cells of a table row: numbers and dash placeholders, never percent
// cells. A total row often carries no label and no "% of total" cells, so a
// cell index taken from a region row can land on another period's total
// (MRVL: China $1,161.5 against the prior year's $2,006.1 read 57.9%; the
// table states 42%) (2.5.9).
export const DASH_CELL_RE = /^[\s$]*[-\u2013\u2014]+[\s]*$/;

// A table's stated scale: "(in thousands)", "$ millions", "thousands of dollars", "($000)", "(000s)" (2.5.20:
// COHR's "by market ($000):" was read as millions, 1000x too large).
const UNIT_SCALE_RE = /\bin\s+(billions|millions|thousands)\b|\$\s*(billions|millions|thousands)\b|\b(billions|millions|thousands)\s+of\s+(?:u\.?s\.?\s+)?dollars\b|\(\s*(?:in\s+)?\$?\s*(000)'?s?\s*\)|\$\s*(000)'?s\b/gi;
const UNIT_SCALE_WORD: Record<string, number> = { billions: 1e9, millions: 1e6, thousands: 1e3, "000": 1e3 };

/**
 * A scale statement about share counts, not amounts: "shares in thousands", or a scale after "except" in the same
 * parenthetical, as in Apple's "(In millions, except number of shares, which are reflected in thousands ...)"
 * (2.25.1, F25: the share scale was read as the table's monetary scale, 1000x too small).
 */
export function isShareScaleStatement(text: string, index: number): boolean {
  const lead = text.slice(Math.max(0, index - 80), index);
  const clause = lead.slice(Math.max(lead.lastIndexOf("("), lead.lastIndexOf(")"), lead.lastIndexOf(";")) + 1);
  return /\b(?:except|shares?)\b/i.test(clause);
}

function statedUnitScales(html: string): number[] {
  const text = html.replace(/<[^>]+>/g, " ").replace(/&nbsp;|&#160;|&#xa0;/gi, " ").replace(/&#36;|&dollar;/gi, "$").replace(/\s+/g, " ");
  return [...text.matchAll(UNIT_SCALE_RE)]
    .filter((m) => !isShareScaleStatement(text, m.index ?? 0))
    .map((m) => UNIT_SCALE_WORD[(m.slice(1).find(Boolean) ?? "").toLowerCase()]);
}

// Where a table's scale came from (2.5.21). ASSUMED_MILLIONS: neither the table nor its lead-in states one, so
// amounts are read in millions (the most common 10-K scale) and carry UNIT_SCALE_ASSUMED; a share is unaffected.
// STATED_IN_DOCUMENT (2.5.22): the table and its lead-in state none, but every scale statement in the filing
// agrees (VRT's 10-K says "(Dollars in millions ...)" fourteen times and nothing else).
export type UnitScaleSource = "STATED_IN_TABLE" | "STATED_BEFORE_TABLE" | "STATED_IN_DOCUMENT" | "ASSUMED_MILLIONS";

export const UNIT_SCALE_ASSUMED_WARNING = {
  code: "UNIT_SCALE_ASSUMED",
  message: "Neither the table nor the text before it states a scale; amounts are read in millions. Check the filing before using the amounts (a percentage share is not affected).",
  severity: "warning",
};

/** The one scale every statement in a filing names, or null when it states none or several. */
function documentUnitScale(html: string): number | null {
  const scales = new Set(statedUnitScales(html));
  return scales.size === 1 ? [...scales][0] : null;
}

/**
 * The monetary unit multiplier for a table: the scale the table itself states, else the statement in its
 * lead-in nearest the table, else the filing's single stated scale, else millions, marked as assumed.
 */
export function detectUnitScale(tableHtml: string, contextHtml: string, documentScale: () => number | null = () => null): { multiplier: number; source: UnitScaleSource } {
  const own = statedUnitScales(tableHtml);
  if (own.length > 0) return { multiplier: own[0], source: "STATED_IN_TABLE" };
  const lead = statedUnitScales(contextHtml);
  if (lead.length > 0) return { multiplier: lead[lead.length - 1], source: "STATED_BEFORE_TABLE" };
  const doc = documentScale();
  return doc != null ? { multiplier: doc, source: "STATED_IN_DOCUMENT" } : { multiplier: 1e6, source: "ASSUMED_MILLIONS" };
}

/** A filing's single stated scale, computed once per document on first use. */
export function lazyDocumentScale(html: string): () => number | null {
  let done = false;
  let value: number | null = null;
  return () => {
    if (!done) {
      value = documentUnitScale(html);
      done = true;
    }
    return value;
  };
}

export const TOTAL_LABELS = new Set([
  "total", "consolidated", "total revenues", "total net revenues",
  "net revenues", "revenues", "total revenue", "total net sales", "net sales",
  "total net revenue",
]);

/**
 * The unit a filing table states, such as "(in millions)" in its caption or
 * "(dollars in millions)" in the sentence before it. The statement nearest
 * the table wins; an amount like "$4.2 billion" in nearby prose is not a
 * unit statement. "unknown" when none is stated.
 */
export function tableUnitScale(tableHtml: string, beforeHtml: string): "thousands" | "millions" | "billions" | "unknown" {
  const unitRe = /\bin\s+(thousands|millions|billions)\b|\$\s*(thousands|millions|billions)\b|\b(thousands|millions|billions)\s+of\s+(?:u\.s\.\s+)?dollars\b/gi;
  const scaleOf = (m: RegExpMatchArray): "thousands" | "millions" | "billions" =>
    (m[1] ?? m[2] ?? m[3]).toLowerCase() as "thousands" | "millions" | "billions";
  // A share-count scale is not the table's monetary scale (2.25.1, F25).
  const tableText = stripHtmlTags(tableHtml).slice(0, 1_500);
  const inTable = [...tableText.matchAll(unitRe)].filter((m) => !isShareScaleStatement(tableText, m.index ?? 0));
  if (inTable.length > 0) return scaleOf(inTable[0]);
  const beforeText = stripHtmlTags(beforeHtml).slice(-800);
  const before = [...beforeText.matchAll(unitRe)].filter((m) => !isShareScaleStatement(beforeText, m.index ?? 0));
  if (before.length > 0) return scaleOf(before[before.length - 1]);
  return "unknown";
}

export function _stripHtmlTagsIdx(html: string): string {
  const sanitizedHtml = html
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script[^>]*>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style[^>]*>/gi, " ")
    .replace(/\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, " ");
  const blockBroken = sanitizedHtml
    .replace(/<(?:br|\/p|\/div|\/li|\/tr|\/h[1-6]|\/section)\b[^>]*>/gi, "\n")
    .replace(/<(?:p|div|li|tr|h[1-6]|section)\b[^>]*>/gi, "\n");
  // Inline tags render without a break: "FINANC</span><span>IAL" is one word
  // (ASTS 10-Q headings, 2.4.5). Whitespace written in the HTML still separates.
  const noTags = blockBroken.replace(/<\/?(?:span|font|b|i|u|em|strong|a|sup|sub|small|ix:[a-z]+)\b[^>]*>/gi, "").replace(/<[^>]+>/g, " ");
  const decoded = decodeHtmlEntities(noTags);
  // A no-break space is a space (2.5.20): Python's ASCII \s would not match it in the mirrored readers.
  const lines = decoded.split("\n").map(line => line.replace(/[ \t\u00a0]+/g, " ").trim());
  return lines.filter(Boolean).join("\n");
}

export function _sanitizeFilingHtml(html: string): string {
  return sanitizeFilingHtml(html);
}
