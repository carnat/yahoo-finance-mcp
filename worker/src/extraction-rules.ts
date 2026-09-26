// Text extraction rules shared by both runtimes (2.4.4).
//
// Customer concentration, guidance ranges and reported release metrics read
// filing and press-release prose. Each rule proves what a number belongs to
// before returning it: a customer percentage must be a share of revenue for a
// named or explicitly unnamed customer, a guidance range must follow a
// guidance keyword, and a reported metric must follow its own label outside
// award, backlog and outlook wording.
//
// yfmcp/extraction_rules.py mirrors this file; scripts/test_extraction_rules.py
// requires identical output from both.

// One whitespace class for both runtimes (JavaScript's \s); text is collapsed
// with it first, so every later pattern sees plain spaces.
const WS_RUN_RE = /[\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+/g;

function collapseWs(text: string): string {
  return text.replace(WS_RUN_RE, " ").trim();
}

// ── Customer concentration ──────────────────────────────────────────────────

const CUSTOMER_NEGATION_RE = /\bno (?:single |one )?customer\b|\bnone of (?:the|its|our) customers\b|did not have any (?:single )?customer|no customers? (?:that )?(?:individually )?accounted/i;
// "<subject> accounted for 53.1%, 34.1% and 11.3% of our revenue": the first
// percentage is the first period listed. Receivables and other bases never match.
// A threshold ("more than 10%", "10% or more") states the disclosure rule, not
// a value, and is skipped.
const CONCENTRATION_RE = /\b(?:accounted\s+for|represented|comprised|contributed)\s+(?:approximately\s+|about\s+|nearly\s+|roughly\s+|(more\s+than|over|greater\s+than|at\s+least|in\s+excess\s+of|exceeding)\s+)?(\d{1,3}(?:\.\d+)?)\s*%(\s*or\s+(?:more|greater|higher))?(?:(?:\s*,\s*|\s*,?\s*and\s+)\d{1,3}(?:\.\d+)?\s*%)*\s+of\s+(?:(?:our|its|the\s+company'?s|the|total|net|consolidated)\s+){0,3}(?:revenues?|net\s+(?:sales|revenues?)|sales)\b/gi;
// "... of our revenue in 2025": a year straight after a match belongs to it.
const TRAILING_YEAR_RE = /^\s*(?:in|for|during)\s+(?:fiscal\s+(?:year\s+)?)?((?:19|20)\d{2})\b/i;
const YEAR_RE = /\b(?:19|20)\d{2}\b/g;
const SUBJECT_BREAK_RE = /,\s+|;\s+|\band\s+/gi;
const AGGREGATE_SUBJECT_RE = /\b(?:customers|clients|distributors)\b/i;
const UNNAMED_SUBJECT_RE = /^(?:(?:our|the|its)\s+)?(?:one|a|single|a single|another|largest)\b.*\b(?:customer|client|distributor)$/i;

export type ConcentrationFinding = {
  kind: "customer" | "aggregate";
  name: string | null;
  description: string;
  valuePct: number;
  year: number | null;
  sectionHeading: string | null;
  sentence: string;
};

function concentrationSubject(segment: string): string {
  let start = 0;
  for (const m of segment.matchAll(SUBJECT_BREAK_RE)) start = (m.index ?? 0) + m[0].length;
  return segment.slice(start).trim()
    .replace(/^(?:in|during|for)\s+(?:fiscal\s+)?(?:19|20)\d{2}\s*/i, "")
    .replace(/[\s,;:]+$/, "");
}

/**
 * Revenue-concentration statements in filing text matches: named or unnamed
 * customers, and aggregates such as "our top ten customers", each for the
 * first period its sentence lists. Only the latest year found is kept.
 */
export function customerConcentration(matches: Record<string, unknown>[]): { findings: ConcentrationFinding[]; negation: { sectionHeading: string | null; sentence: string } | null } {
  const findings: ConcentrationFinding[] = [];
  let negation: { sectionHeading: string | null; sentence: string } | null = null;
  for (const item of matches) {
    const ctx = collapseWs(String(item.contextText ?? item.context ?? ""));
    const sectionHeading = typeof item.sectionHeading === "string" ? item.sectionHeading : null;
    for (const sentence of ctx.split(/(?<=[.;])\s+/)) {
      if (!/customer|client|distributor/i.test(sentence) && !/\b(?:accounted\s+for|represented)\b/i.test(sentence)) continue;
      if (CUSTOMER_NEGATION_RE.test(sentence)) {
        negation ??= { sectionHeading, sentence };
        continue;
      }
      let prevEnd = 0;
      let lastYear: number | null = null;
      for (const m of sentence.matchAll(CONCENTRATION_RE)) {
        const index = m.index ?? 0;
        const segment = sentence.slice(prevEnd, index);
        prevEnd = index + m[0].length;
        const trailing = TRAILING_YEAR_RE.exec(sentence.slice(prevEnd));
        if (trailing) prevEnd += trailing[0].length;
        const years = [...segment.matchAll(YEAR_RE)].map((y) => Number(y[0]));
        const year: number | null = trailing ? Number(trailing[1]) : (years.length > 0 ? years[0] : lastYear);
        lastYear = year;
        if (m[1] || m[3]) continue;
        const pct = Number(m[2]);
        if (!(pct > 0 && pct <= 100)) continue;
        const subject = concentrationSubject(segment);
        if (!subject) continue;
        if (AGGREGATE_SUBJECT_RE.test(subject)) {
          findings.push({ kind: "aggregate", name: null, description: subject, valuePct: pct, year, sectionHeading, sentence });
        } else if (UNNAMED_SUBJECT_RE.test(subject) || !/^[A-Z]/.test(subject) || subject.length > 60) {
          findings.push({ kind: "customer", name: null, description: subject, valuePct: pct, year, sectionHeading, sentence });
        } else {
          findings.push({ kind: "customer", name: subject.replace(/^(?:our|the|its)\s+/i, ""), description: subject, valuePct: pct, year, sectionHeading, sentence });
        }
      }
    }
  }
  const years = findings.map((f) => f.year).filter((y): y is number => y != null);
  const latest = years.length > 0 ? Math.max(...years) : null;
  const seen = new Set<string>();
  const kept: ConcentrationFinding[] = [];
  for (const f of findings) {
    if (latest != null && f.year != null && f.year !== latest) continue;
    const key = f.kind === "aggregate" ? `a|${f.description.toLowerCase()}` : (f.name ? `n|${f.name.toLowerCase()}` : `u|${f.valuePct}`);
    if (seen.has(key)) continue;
    seen.add(key);
    kept.push(f);
  }
  return { findings: kept.slice(0, 12), negation };
}

// ── Guidance ranges ─────────────────────────────────────────────────────────

const AMOUNT = "([0-9][0-9.,]*(?:\\s*(?:billion|million|thousand|bn|m|k))?)";
const RANGE_SEP = "\\s*(?:to|and|-|\\u2013|\\u2014)\\s*";
// "full year 2026 revenue guidance of $150.0 million to $200.0 million" (ASTS)
const REVENUE_FIRST_RE = new RegExp(`\\brevenues?\\s+(?:guidance|outlook|forecast)\\b[^$.]{0,40}\\$\\s*${AMOUNT}${RANGE_SEP}\\$?\\s*${AMOUNT}`, "i");
// "expects revenue between $X and $Y" / "guidance: revenue of $X to $Y"
const KEYWORD_FIRST_RE = new RegExp(`(?:expects|guidance|outlook)[^.\\n]{0,120}revenue[^$]{0,25}\\$?\\s*${AMOUNT}${RANGE_SEP}\\$?\\s*${AMOUNT}`, "i");
const GROSS_MARGIN_RE = /gross margin[^0-9]{0,20}([0-9]{1,2}(?:\.[0-9]+)?)\s*%\s*(?:to|and|-|–|—)\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%/i;
const EPS_RE = /(?:expects|guidance|outlook)[^.\n]{0,120}(?:eps|earnings per share)[^$]{0,25}\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|and|-|–|—)\s*\$?\s*([0-9]+(?:\.[0-9]+)?)/i;

export type RangeMatch = { excerpt: string; low: string; high: string } | null;

/** Guidance ranges stated in release text; low and high are the number text as written. */
export function guidanceRanges(text: string): { revenue: RangeMatch; grossMargin: RangeMatch; eps: RangeMatch } {
  const pick = (m: RegExpMatchArray | null): RangeMatch => (m ? { excerpt: m[0], low: m[1], high: m[2] } : null);
  return {
    revenue: pick(text.match(REVENUE_FIRST_RE)) ?? pick(text.match(KEYWORD_FIRST_RE)),
    grossMargin: pick(text.match(GROSS_MARGIN_RE)),
    eps: pick(text.match(EPS_RE)),
  };
}

// ── Reported release metrics ────────────────────────────────────────────────

// Sentences end at . ! ? and at bullet markers, including the " o " bullets
// SEC-rendered press releases carry, so one bullet's value is never read for
// another's label.
const METRIC_SPLIT_RE = /(?<=[.!?])\s+|\s+[•●▪◦·]\s+|\s+o\s+(?=[A-Z])/;
const GUIDANCE_CONTEXT_RE = /\b(?:guidance|outlook|expect(?:s|ed|ation)?|forecast|project(?:s|ed)?|target|range)\b/i;
const REPORTED_CONTEXT_RE = /\b(?:reported|was|were|totaled|totalled|generated|delivered|achieved)\b/i;
// "$125 million" of government awards is not revenue (ASTS Q2 2026).
const NON_RESULT_CONTEXT_RE = /\b(?:awards?|awarded|contract value|aggregate value|backlog|bookings|orders?|pipeline|contracted)\b/i;

export const REVENUE_LABEL = "\\b(?:net sales|revenues?)\\b";
export const USD_AMOUNT = "(\\$\\s*[-+]?[0-9][0-9,.\\s]*(?:billion|million|thousand|bn|m|k)?)";
export const EPS_LABEL = "\\b(?:diluted (?:earnings per share|eps)|eps \\(diluted\\))\\b";
export const EPS_AMOUNT = "(\\$\\s*\\(?[-+]?[0-9]+(?:\\.[0-9]+)?\\)?)";
export const PCT_AMOUNT = "([0-9]{1,2}(?:\\.[0-9]+)?\\s*%)";

/**
 * The first explicitly reported value for a label: a sentence with a result
 * verb, no guidance or award wording, and the value within 100 non-digit
 * characters after the label.
 */
export function reportedTextMetric(text: string, labelSource: string, valueSource: string): { rawValue: string; sentence: string } | null {
  const label = new RegExp(labelSource, "i");
  const anchored = new RegExp(`(?:${labelSource})\\D{0,100}?${valueSource}`, "i");
  for (const raw of collapseWs(text).split(METRIC_SPLIT_RE)) {
    const sentence = raw.trim();
    if (!sentence || !label.test(sentence)) continue;
    if (GUIDANCE_CONTEXT_RE.test(sentence) || NON_RESULT_CONTEXT_RE.test(sentence) || !REPORTED_CONTEXT_RE.test(sentence)) continue;
    const m = anchored.exec(sentence);
    if (m && m[1]) return { rawValue: m[1].trim(), sentence };
  }
  return null;
}

// ── Event query terms ───────────────────────────────────────────────────────

/**
 * A light suffix stem for matching event query words: "launch", "launches",
 * "launched" and "launching" all become "launch"; "release" and "released"
 * become "releas". Applied to both the query and the evidence words.
 */
export function stemWord(word: string): string {
  let w = word.toLowerCase();
  if (w.length > 5 && w.endsWith("ing")) w = w.slice(0, -3);
  else if (w.length > 4 && w.endsWith("ed")) w = w.slice(0, -2);
  else if (w.length > 4 && w.endsWith("es")) w = w.slice(0, -2);
  else if (w.length > 3 && w.endsWith("s") && !w.endsWith("ss")) w = w.slice(0, -1);
  if (w.length > 4 && w.endsWith("e")) w = w.slice(0, -1);
  return w;
}

const CONFIDENCE_RANK: Record<string, number> = { HIGH: 0, MEDIUM: 1, LOW: 2 };

/** Evidence ordered by confidence (HIGH first), then newest first; stable otherwise. */
export function rankEvidence<T extends Record<string, unknown>>(items: T[], confidenceOf: (item: T) => string): T[] {
  return items
    .map((item, index) => ({ item, index }))
    .sort((a, b) => {
      const ca = CONFIDENCE_RANK[confidenceOf(a.item)] ?? 3;
      const cb = CONFIDENCE_RANK[confidenceOf(b.item)] ?? 3;
      if (ca !== cb) return ca - cb;
      const pa = String(a.item.publishedAt ?? "");
      const pb = String(b.item.publishedAt ?? "");
      if (pa !== pb) return pa < pb ? 1 : -1;
      return a.index - b.index;
    })
    .map(({ item }) => item);
}
