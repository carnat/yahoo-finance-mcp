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
// "expects revenue between $X and $Y" / "guidance: revenue of $X to $Y" / "expectations for revenue of $X to $Y" (ASTS)
const KEYWORD_FIRST_RE = new RegExp(`(?:expects|expectations?|guidance|outlook)[^.\\n]{0,120}revenue[^$]{0,25}\\$?\\s*${AMOUNT}${RANGE_SEP}\\$?\\s*${AMOUNT}`, "i");
const GROSS_MARGIN_RE = /gross margin[^0-9]{0,20}([0-9]{1,2}(?:\.[0-9]+)?)\s*%\s*(?:to|and|-|–|—)\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%/i;
// "net income per share" is EPS too (2.5.10, LITE: "Non-GAAP diluted net income per share of $4.05 to $4.35").
const EPS_RE = /(?:expects|guidance|outlook)[^.\n]{0,120}(?:eps|earnings per share|net (?:income|earnings|loss) per share)[^$]{0,25}\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|and|-|–|—)\s*\$?\s*([0-9]+(?:\.[0-9]+)?)/i;
// Metric first, forward verb after it (2.5.9, COHR): "Revenue for the first
// quarter of fiscal 2027 is expected to be between $2.2 billion and $2.4
// billion." No period, dollar sign or percent sign may sit between the metric
// and the verb, so a reported value is never read as the range.
const FORWARD_VERB = "\\b(?:expected|projected|forecast(?:ed)?|anticipated|estimated)\\s+to\\s+(?:be|range|total)\\b";
const METRIC_FIRST_REVENUE_RE = new RegExp(`\\brevenues?\\b[^.$%]{0,120}?${FORWARD_VERB}[^$.%]{0,30}\\$\\s*${AMOUNT}${RANGE_SEP}\\$?\\s*${AMOUNT}`, "i");
const METRIC_FIRST_GROSS_MARGIN_RE = new RegExp(`\\bgross margins?\\b[^.$%]{0,120}?${FORWARD_VERB}[^$.%0-9]{0,30}([0-9]{1,2}(?:\\.[0-9]+)?)\\s*%${RANGE_SEP}([0-9]{1,2}(?:\\.[0-9]+)?)\\s*%`, "i");
const EPS_LABEL_SOURCE = "\\b(?:eps|earnings per share|net (?:income|earnings|loss) per share)\\b";
const METRIC_FIRST_EPS_RE = new RegExp(`${EPS_LABEL_SOURCE}[^.$%]{0,120}?${FORWARD_VERB}[^$.%]{0,30}\\$\\s*([0-9]+(?:\\.[0-9]+)?)${RANGE_SEP}\\$?\\s*([0-9]+(?:\\.[0-9]+)?)`, "i");
// A midpoint and a tolerance (2.5.9, MRVL): "Net revenue is expected to be
// $3.150 billion +/- 5%", "... net income per share is expected to be $0.53
// +/- $0.05 per share". The bounds are computed exactly (pmBounds).
const PLUS_MINUS = "(?:\\+\\s*/\\s*[-\\u2212]|\\u00b1|plus or minus)";
const PM_NUMBER = "([0-9][0-9,]*(?:\\.[0-9]+)?)";
const PM_UNIT = "(?:\\s*(billion|million|thousand|bn|m|k)\\b)?";
// A comma may precede the tolerance (NVDA: "$108.0 billion, plus or minus 2%").
const PM_TOLERANCE = `\\s*,?\\s*${PLUS_MINUS}\\s*(\\$)?\\s*${PM_NUMBER}\\s*(%|(?:billion|million|thousand|bn|m|k)\\b)?`;
const METRIC_FIRST_REVENUE_PM_RE = new RegExp(`\\brevenues?\\b[^.$%]{0,120}?${FORWARD_VERB}[^$.%]{0,30}\\$\\s*${PM_NUMBER}${PM_UNIT}${PM_TOLERANCE}`, "i");
const METRIC_FIRST_EPS_PM_RE = new RegExp(`${EPS_LABEL_SOURCE}[^.$%]{0,120}?${FORWARD_VERB}[^$.%]{0,30}\\$\\s*${PM_NUMBER}${PM_UNIT}${PM_TOLERANCE}`, "i");
const UNIT_EXP: Record<string, number> = { billion: 9, bn: 9, million: 6, m: 6, thousand: 3, k: 3 };
// A margin and a tolerance in points (NVDA: "gross margins are expected to be 74.0%, plus or minus 50 basis points").
const METRIC_FIRST_GROSS_MARGIN_PM_RE = new RegExp(`\\bgross margins?\\b[^.$%]{0,120}?${FORWARD_VERB}[^$.%0-9]{0,30}([0-9]{1,2}(?:\\.[0-9]+)?)\\s*%\\s*,?\\s*${PLUS_MINUS}\\s*([0-9]+(?:\\.[0-9]+)?)\\s*(basis points?|bps|percentage points?|%)`, "i");
// Release tables, read as flattened text (2.5.10, VRT): "Third Quarter 2026 Guidance Net sales $3,650M -
// $3,850M ... Adjusted diluted EPS (1) $1.77 - $1.83". A row is a label, an optional footnote marker and
// the range, under a guidance or outlook heading with no sentence break between them.
const TABLE_FOOTNOTE = "(?:\\s*\\(\\d\\))?";
// An outlook bullet puts "of" or "in the range of" between label and range (LITE: "Non-GAAP diluted net
// income per share of $4.05 to $4.35").
const ROW_LEAD = "\\s*(?:of\\s+|in the range of\\s+|:\\s*)?";
// "... diluted EPS of $5.82 to $5.92 and adjusted diluted EPS of $6.65 to $6.75" (VRT): the second range.
const EPS_CONTINUATION_RE = /\band (?:adjusted|non-GAAP|GAAP) (?:diluted )?(?:eps|earnings per share|net income per share) of \$\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|-|–|—)\s*\$?\s*([0-9]+(?:\.[0-9]+)?)/i;
const TABLE_REVENUE_RE = new RegExp(`\\b(?:net sales|(?:total )?(?:net )?revenues?)${TABLE_FOOTNOTE}${ROW_LEAD}\\$\\s*${AMOUNT}${RANGE_SEP}\\$?\\s*${AMOUNT}`, "i");
const TABLE_GROSS_MARGIN_RE = new RegExp(`\\b(?:(?:adjusted|non-GAAP|GAAP)\\s+)?gross margins?${TABLE_FOOTNOTE}${ROW_LEAD}([0-9]{1,2}(?:\\.[0-9]+)?)\\s*%${RANGE_SEP}([0-9]{1,2}(?:\\.[0-9]+)?)\\s*%`, "i");
const TABLE_EPS_RE = new RegExp(`\\b(?:(?:adjusted|non-GAAP|GAAP)\\s+)?(?:diluted\\s+)?(?:eps|earnings per share|net income per share)${TABLE_FOOTNOTE}${ROW_LEAD}\\$\\s*([0-9]+(?:\\.[0-9]+)?)${RANGE_SEP}\\$?\\s*([0-9]+(?:\\.[0-9]+)?)`, "i");
const TABLE_HEADING_RE = /\b(?:guidance|outlook)\b/gi;

/** A table row sits under a guidance or outlook heading within 400 characters, with no sentence break between. */
function underGuidanceHeading(text: string, at: number): boolean {
  const before = text.slice(Math.max(0, at - 400), at);
  let last = -1;
  for (const m of before.matchAll(TABLE_HEADING_RE)) last = (m.index ?? 0) + m[0].length;
  return last >= 0 && !/[.!?]\s+[A-Z]/.test(before.slice(last));
}

type Dec = { n: bigint; exp: number };

function dec(text: string): Dec {
  const t = text.replace(/,/g, "");
  const dot = t.indexOf(".");
  return dot < 0 ? { n: BigInt(t), exp: 0 } : { n: BigInt(t.replace(".", "")), exp: -(t.length - dot - 1) };
}

function decAt(d: Dec, exp: number): bigint {
  return d.n * 10n ** BigInt(d.exp - exp);
}

/** An exact decimal n x 10^exp written in units of 10^unitExp, trailing zeros dropped. */
function decText(n: bigint, exp: number, unitExp: number): string {
  const places = unitExp - exp;
  const neg = n < 0n;
  let digits = (neg ? -n : n).toString();
  if (places > 0) {
    digits = digits.padStart(places + 1, "0");
    digits = `${digits.slice(0, -places)}.${digits.slice(-places)}`.replace(/\.?0+$/, "");
  } else if (places < 0) {
    digits = digits + "0".repeat(-places);
  }
  return `${neg ? "-" : ""}${digits}`;
}

/**
 * The low and high of "midpoint +/- tolerance", in the midpoint's unit: a
 * percentage of the midpoint, or an amount in its own unit (the midpoint's
 * when it names none). A bare tolerance with no $, % or unit is not read.
 */
/** The low and high of a margin "N% +/- M basis points" (or percentage points), exactly. */
function pointBounds(mid: string, tol: string, unit: string): { low: string; high: string } {
  const m = dec(mid);
  const t = dec(tol);
  const tExp = /basis|bps/i.test(unit) ? t.exp - 2 : t.exp;
  const exp = Math.min(m.exp, tExp);
  const mn = decAt(m, exp);
  const tn = decAt({ n: t.n, exp: tExp }, exp);
  return { low: decText(mn - tn, exp, 0), high: decText(mn + tn, exp, 0) };
}

function pmBounds(mid: string, midUnit: string | undefined, dollar: string | undefined, tol: string, tolUnit: string | undefined): { low: string; high: string } | null {
  const m = dec(mid);
  const t = dec(tol);
  const midExp = UNIT_EXP[(midUnit ?? "").toLowerCase()] ?? 0;
  const suffix = midUnit ? ` ${midUnit}` : "";
  if (tolUnit === "%") {
    // mid x (100 -/+ p) / 100
    const hundred = 100n * 10n ** BigInt(-t.exp);
    const exp = m.exp + t.exp - 2;
    return { low: decText(m.n * (hundred - t.n), exp, 0) + suffix, high: decText(m.n * (hundred + t.n), exp, 0) + suffix };
  }
  if (!dollar && !tolUnit) return null;
  const tolExp = tolUnit ? UNIT_EXP[tolUnit.toLowerCase()] ?? 0 : midExp;
  const exp = Math.min(m.exp + midExp, t.exp + tolExp);
  const mn = decAt({ n: m.n, exp: m.exp + midExp }, exp);
  const tn = decAt({ n: t.n, exp: t.exp + tolExp }, exp);
  return { low: decText(mn - tn, exp, midExp) + suffix, high: decText(mn + tn, exp, midExp) + suffix };
}

// The basis a range is stated on, read from its own clause: the sentence up
// to the range, and the words after it up to the next value or clause break
// ("... between $1.85 and $2.05 on a non-GAAP basis.").
const CLAUSE_START_RE = /(?:[.;!?]\s|•)/g;
const CLAUSE_TAIL_RE = /^[^.;,$%•]{0,80}?(?=[.;,$%•]|\sand\s|$)/;
const NON_GAAP_RE = /\bnon-?\s?GAAP\b|\badjusted\b/i;
const GAAP_RE = /\bGAAP\b/i;
const BOTH_BASES_RE = /\bGAAP and non-?\s?GAAP\b|\bnon-?\s?GAAP and GAAP\b/i;

export type RangeBasis = "NON_GAAP" | "GAAP" | "GAAP_AND_NON_GAAP" | "NOT_STATED";
export type RangeMatch = {
  excerpt: string;
  low: string;
  high: string;
  basis: RangeBasis;
  // RANGE: "$X to $Y"; MIDPOINT_PLUS_MINUS: "$X +/- 5%", bounds computed exactly;
  // OUTLOOK_ROW: a release-table row or outlook bullet under a guidance heading.
  statedAs: "RANGE" | "MIDPOINT_PLUS_MINUS" | "OUTLOOK_ROW";
  // The same metric stated on another basis ("GAAP ... ; non-GAAP ..."), one per basis.
  alternates: Omit<NonNullable<RangeMatch>, "alternates">[];
} | null;

function clauseBasis(clause: string): RangeBasis {
  // One range for both (NVDA: "GAAP and non-GAAP gross margins are expected to be 74.0% ...") (2.5.10).
  if (BOTH_BASES_RE.test(clause)) return "GAAP_AND_NON_GAAP";
  if (NON_GAAP_RE.test(clause)) return "NON_GAAP";
  return GAAP_RE.test(clause) ? "GAAP" : "NOT_STATED";
}

function rangeBasis(text: string, at: number, len: number): RangeBasis {
  const before = text.slice(Math.max(0, at - 200), at);
  let start = 0;
  for (const m of before.matchAll(CLAUSE_START_RE)) start = (m.index ?? 0) + m[0].length;
  const tail = CLAUSE_TAIL_RE.exec(text.slice(at + len))?.[0] ?? "";
  return clauseBasis(`${before.slice(start)}${text.slice(at, at + len)}${tail}`);
}

type Pattern = { re: RegExp; kind: "range" | "pm_amount" | "pm_points" | "table" };

/**
 * Guidance ranges stated in release text; low and high are the number text as
 * written (computed exactly for a midpoint and tolerance). Keyword-first
 * wording wins; metric-first wording ("revenue ... is expected to be
 * between") is read when there is none, then release-table rows under a
 * guidance heading. Ranges for the same metric on another basis are kept as
 * alternates.
 */
export function guidanceRanges(text: string, periodOf: ((at: number, len: number) => string | null) | null = null): { revenue: RangeMatch; grossMargin: RangeMatch; eps: RangeMatch } {
  const pick = (...patterns: Pattern[]): RangeMatch => {
    const found: Omit<NonNullable<RangeMatch>, "alternates">[] = [];
    const periods: (string | null)[] = [];
    for (const { re, kind } of patterns) {
      for (const m of text.matchAll(new RegExp(re.source, `${re.flags}g`))) {
        const at = m.index ?? 0;
        if (kind === "table" && !underGuidanceHeading(text, at)) continue;
        const bounds = kind === "pm_amount" ? pmBounds(m[1], m[2], m[3], m[4], m[5])
          : kind === "pm_points" ? pointBounds(m[1], m[2], m[3])
          : { low: m[1], high: m[2] };
        if (!bounds) continue;
        found.push({
          excerpt: m[0],
          low: bounds.low,
          high: bounds.high,
          // A table row's basis is its own label: the rows above it belong to other metrics.
          basis: kind === "table" ? clauseBasis(m[0]) : rangeBasis(text, at, m[0].length),
          statedAs: kind === "table" ? "OUTLOOK_ROW" : kind === "range" ? "RANGE" : "MIDPOINT_PLUS_MINUS",
        });
        periods.push(periodOf ? periodOf(at, m[0].length) : null);
      }
    }
    if (found.length === 0) return null;
    const [primary, ...rest] = found;
    const alternates: typeof found = [];
    // Another basis for the same target period only: a quarter's row is not a year's alternate (2.5.10).
    rest.forEach((r, i) => {
      const samePeriod = periods[0] == null || periods[i + 1] == null || periods[i + 1] === periods[0];
      if (samePeriod && r.basis !== primary.basis && !alternates.some((a) => a.basis === r.basis)) alternates.push(r);
    });
    return { ...primary, alternates };
  };
  return {
    revenue: pick({ re: REVENUE_FIRST_RE, kind: "range" }, { re: KEYWORD_FIRST_RE, kind: "range" }, { re: METRIC_FIRST_REVENUE_RE, kind: "range" },
      { re: METRIC_FIRST_REVENUE_PM_RE, kind: "pm_amount" }, { re: TABLE_REVENUE_RE, kind: "table" }),
    grossMargin: pick({ re: GROSS_MARGIN_RE, kind: "range" }, { re: METRIC_FIRST_GROSS_MARGIN_RE, kind: "range" },
      { re: METRIC_FIRST_GROSS_MARGIN_PM_RE, kind: "pm_points" }, { re: TABLE_GROSS_MARGIN_RE, kind: "table" }),
    eps: pick({ re: EPS_RE, kind: "range" }, { re: EPS_CONTINUATION_RE, kind: "range" }, { re: METRIC_FIRST_EPS_RE, kind: "range" }, { re: METRIC_FIRST_EPS_PM_RE, kind: "pm_amount" },
      { re: TABLE_EPS_RE, kind: "table" }),
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
