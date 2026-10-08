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
// A lower-case subject naming no customer is a revenue category, not a customer (2.5.32, F-024: AEHR's "EV and power
// semiconductor revenues accounted for 17%").
const CUSTOMER_WORD_RE = /\b(?:customers?|clients?|distributors?|resellers?)\b/i;
// "three customers accounted for approximately 26%, 14% and 11%": as many shares as customers counted, in a sentence
// naming fewer years, are one share per customer, not a total (2.5.32, F-024: AEHR, ANET).
const COUNT_WORDS: Record<string, number> = { two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9, ten: 10 };
const COUNTED_SUBJECT_RE = /^(?:(?:these|the|our|its)\s+)?(two|three|four|five|six|seven|eight|nine|ten|\d{1,2})\s+(?:of\s+(?:our|its|the)\s+)?(?:\w+\s+)?(?:customers|clients|distributors)$/i;
const RANKED_SUBJECT_RE = /\b(?:largest|top|biggest|major)\b/i;
const PCT_RE = /(\d{1,3}(?:\.\d+)?)\s*%/g;
// A significant-customer table row (2.5.32, F-024: MRVL's "Distributor A | 37% | 34% | 24%" under a title stating
// the 10%-of-net-revenue rule); the first column is the latest period.
const TABLE_CUSTOMER_LABEL_RE = /^(?:(?:direct|end)\s+)?(?:customer|distributor|client|reseller)\s+(?:[A-Z]|\d{1,2})$/i;
const TABLE_PCT_CELL_RE = /^(\d{1,3}(?:\.\d+)?)\s*%$/;

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
  // An "inclusive of ..." clause and trailing punctuation are no subject break (MRVL's "net revenue from our ten (10)
  // largest customers, inclusive of our distributor and direct customers, represented 82%"), and "sales to" or
  // "revenue from" is not part of the customer (ANET's "Sales to one end customer") (2.5.32, F-024).
  const trimmed = segment.replace(/,\s*(?:inclusive\s+of|including|excluding)\b[^,]*,?\s*$/i, "").replace(/[\s,;:]+$/, "");
  let start = 0;
  for (const m of trimmed.matchAll(SUBJECT_BREAK_RE)) start = (m.index ?? 0) + m[0].length;
  return trimmed.slice(start).trim()
    .replace(/^(?:in|during|for)\s+(?:fiscal\s+)?(?:19|20)\d{2}\s*/i, "")
    .replace(/^(?:net\s+)?(?:revenues?|sales)\s+(?:from|to)\s+/i, "")
    .replace(/[\s,;:]+$/, "");
}

function tableRowFinding(item: Record<string, unknown>, sectionHeading: string | null): ConcentrationFinding | null {
  const label = typeof item.rowLabel === "string" ? collapseWs(item.rowLabel) : "";
  const title = typeof item.tableTitle === "string" ? collapseWs(item.tableTitle) : "";
  if (!TABLE_CUSTOMER_LABEL_RE.test(label) || !/\b(?:revenues?|sales)\b/i.test(title) || /receivable/i.test(title)) return null;
  const cells = collapseWs(String(item.contextText ?? "")).split("|").map((c) => c.trim());
  const m = cells[0] === label ? TABLE_PCT_CELL_RE.exec(cells[1] ?? "") : null;
  if (!m) return null;
  const pct = Number(m[1]);
  if (!(pct > 0 && pct <= 100)) return null;
  return { kind: "customer", name: null, description: label, valuePct: pct, year: null, sectionHeading, sentence: `${title} ${cells.join(" | ")}` };
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
    if (item.inTable === true) {
      const row = tableRowFinding(item, sectionHeading);
      if (row) findings.push(row);
      continue;
    }
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
        const counted = COUNTED_SUBJECT_RE.exec(subject);
        const shares = [...m[0].matchAll(PCT_RE)].map((p) => Number(p[1]));
        const count = counted ? (COUNT_WORDS[counted[1].toLowerCase()] ?? Number(counted[1])) : 0;
        if (counted && !RANKED_SUBJECT_RE.test(subject) && shares.length === count && new Set(sentence.match(YEAR_RE) ?? []).size < count) {
          for (const share of shares) {
            if (share > 0 && share <= 100) findings.push({ kind: "customer", name: null, description: `one of ${subject}`, valuePct: share, year, sectionHeading, sentence });
          }
          continue;
        }
        if (AGGREGATE_SUBJECT_RE.test(subject)) {
          findings.push({ kind: "aggregate", name: null, description: subject, valuePct: pct, year, sectionHeading, sentence });
        } else if (!/^[A-Z]/.test(subject) && !CUSTOMER_WORD_RE.test(subject)) {
          continue;
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
  // A significant-customer table states the same customers more precisely than the prose ("three customers accounted
  // for approximately 26%, 14% and 11%" against AEHR's Customer A-C at 26.3%, 14.2% and 10.9%): the prose share
  // within half a point of a table row is that row (2.5.32, F-024).
  const tableShares = findings.filter((f) => f.kind === "customer" && f.name == null && TABLE_CUSTOMER_LABEL_RE.test(f.description)).map((f) => f.valuePct);
  const seen = new Set<string>();
  const kept: ConcentrationFinding[] = [];
  for (const f of findings) {
    if (latest != null && f.year != null && f.year !== latest) continue;
    if (f.kind === "customer" && f.name == null && !TABLE_CUSTOMER_LABEL_RE.test(f.description) && tableShares.some((t) => Math.abs(t - f.valuePct) <= 0.5)) continue;
    // "our five largest customers" and "the Company's five largest customers" are one aggregate.
    const key = f.kind === "aggregate" ? `a|${f.description.toLowerCase().replace(/^(?:our|its|the company['’]s|the)\s+/, "")}`
      : f.name ? `n|${f.name.toLowerCase()}` : TABLE_CUSTOMER_LABEL_RE.test(f.description) ? `l|${f.description.toLowerCase()}` : `u|${f.valuePct}`;
    if (seen.has(key)) continue;
    seen.add(key);
    kept.push(f);
  }
  return { findings: kept.slice(0, 12), negation };
}

// ── Guidance ranges ─────────────────────────────────────────────────────────

// "$3.9B" carries its unit; "$1,234 more" carries none (2.5.20).
const AMOUNT = "([0-9][0-9.,]*(?:\\s*(?:billion|million|thousand|bn|mn|b|m|k)\\b)?)";
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
const PM_UNIT = "(?:\\s*(billion|million|thousand|bn|mn|b|m|k)\\b)?";
// A comma may precede the tolerance (NVDA: "$108.0 billion, plus or minus 2%").
const PM_TOLERANCE = `\\s*,?\\s*${PLUS_MINUS}\\s*(\\$)?\\s*${PM_NUMBER}\\s*(%|(?:billion|million|thousand|bn|mn|b|m|k)\\b)?`;
const METRIC_FIRST_REVENUE_PM_RE = new RegExp(`\\brevenues?\\b[^.$%]{0,120}?${FORWARD_VERB}[^$.%]{0,30}\\$\\s*${PM_NUMBER}${PM_UNIT}${PM_TOLERANCE}`, "i");
const METRIC_FIRST_EPS_PM_RE = new RegExp(`${EPS_LABEL_SOURCE}[^.$%]{0,120}?${FORWARD_VERB}[^$.%]{0,30}\\$\\s*${PM_NUMBER}${PM_UNIT}${PM_TOLERANCE}`, "i");
const UNIT_EXP: Record<string, number> = { billion: 9, bn: 9, b: 9, million: 6, mn: 6, m: 6, thousand: 3, k: 3 };
// A margin and a tolerance in points (NVDA: "gross margins are expected to be 74.0%, plus or minus 50 basis points").
const METRIC_FIRST_GROSS_MARGIN_PM_RE = new RegExp(`\\bgross margins?\\b[^.$%]{0,120}?${FORWARD_VERB}[^$.%0-9]{0,30}([0-9]{1,2}(?:\\.[0-9]+)?)\\s*%\\s*,?\\s*${PLUS_MINUS}\\s*([0-9]+(?:\\.[0-9]+)?)\\s*(basis points?|bps|percentage points?|%)`, "i");
// Release tables, read as flattened text (2.5.10, VRT): "Third Quarter 2026 Guidance Net sales $3,650M -
// $3,850M ... Adjusted diluted EPS (1) $1.77 - $1.83". A row is a label, an optional footnote marker and
// the range, under a guidance or outlook heading with no sentence break between them.
const TABLE_FOOTNOTE = "(?:\\s*\\(\\d\\))?";
// An outlook bullet puts "of" or "in the range of" between label and range (LITE: "Non-GAAP diluted net
// income per share of $4.05 to $4.35"); a two-column table may put an "N/A" first cell there (SNDK, 2.5.20).
const ROW_LEAD = "\\s*(?:of\\s+|in the range of\\s+|:\\s*)?(?:N/A\\s+)?";
// "... diluted EPS of $5.82 to $5.92 and adjusted diluted EPS of $6.65 to $6.75" (VRT): the second range.
const EPS_CONTINUATION_RE = /\band (?:adjusted|non-GAAP|GAAP) (?:diluted )?(?:eps|earnings per share|net income per share) of \$\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|-|–|—)\s*\$?\s*([0-9]+(?:\.[0-9]+)?)/i;
const TABLE_REVENUE_RE = new RegExp(`\\b(?:net sales|(?:total )?(?:net )?revenues?)${TABLE_FOOTNOTE}${ROW_LEAD}\\$\\s*${AMOUNT}${RANGE_SEP}\\$?\\s*${AMOUNT}`, "i");
const TABLE_GROSS_MARGIN_RE = new RegExp(`\\b(?:(?:adjusted|non-GAAP|GAAP)\\s+)?gross margins?${TABLE_FOOTNOTE}${ROW_LEAD}([0-9]{1,2}(?:\\.[0-9]+)?)\\s*%${RANGE_SEP}([0-9]{1,2}(?:\\.[0-9]+)?)\\s*%`, "i");
const TABLE_EPS_RE = new RegExp(`\\b(?:(?:adjusted|non-GAAP|GAAP)\\s+)?(?:diluted\\s+)?(?:eps|earnings per share|net income per share)${TABLE_FOOTNOTE}${ROW_LEAD}\\$\\s*([0-9]+(?:\\.[0-9]+)?)${RANGE_SEP}\\$?\\s*([0-9]+(?:\\.[0-9]+)?)`, "i");
// An outlook row stated as a midpoint and tolerance, or as an approximate point (2.5.21, MU: "Revenue $61.5 billion
// ± $1.5 billion", "Diluted earnings per share $37.84 ± $1.00", "Gross margin Approximately 85.95%").
const TABLE_REVENUE_PM_RE = new RegExp(`\\b(?:net sales|(?:total )?(?:net )?revenues?)${TABLE_FOOTNOTE}${ROW_LEAD}\\$\\s*${PM_NUMBER}${PM_UNIT}${PM_TOLERANCE}`, "i");
const TABLE_EPS_PM_RE = new RegExp(`\\b(?:(?:adjusted|non-GAAP|GAAP)\\s+)?(?:diluted\\s+)?(?:eps|earnings per share|net income per share)${TABLE_FOOTNOTE}${ROW_LEAD}\\$\\s*${PM_NUMBER}${PM_UNIT}${PM_TOLERANCE}`, "i");
const APPROX = "(?:approximately|about|~)\\s*";
const TABLE_GROSS_MARGIN_POINT_RE = new RegExp(`\\b(?:(?:adjusted|non-GAAP|GAAP)\\s+)?gross margins?${TABLE_FOOTNOTE}${ROW_LEAD}${APPROX}([0-9]{1,2}(?:\\.[0-9]+)?)\\s*%`, "i");
const TABLE_HEADING_RE = /\b(?:guidance|outlook)\b/gi;

// An outlook table's scale and columns (2.5.20, SNDK: "Business Outlook ... (in millions, except per share
// amounts) GAAP Non-GAAP (1) Revenue $10,300 - $10,800 $10,300 - $10,800 Gross Margin 83.0% - 84.9% 83.0% -
// 85.0% ... Diluted Net Income Per Share N/A $44.00 - $46.00"). Both are read only between the outlook heading
// and the row, with no sentence break after them.
const OUTLOOK_SCALE_RE = /\(\s*(?:\$|US\$|dollars|amounts)?\s*in\s+(thousands|millions|billions)\b/gi;
// A header may name its columns "GAAP(1) Outlook Non-GAAP(2) Outlook", with an "Adjustments" column between (MU, 2.5.21).
const OUTLOOK_COLUMNS_RE = /\b(Non-?\s?GAAP|GAAP)(?:\s*\(\d\))?(?:\s+Outlook)?(?:\s+Adjustments)?\s+(Non-?\s?GAAP|GAAP)(?:\s*\(\d\))?(?:\s+Outlook)?(?=\s+[A-Z])/gi;
const SENTENCE_BREAK_RE = /[.!?]\s+[A-Z]/;
const SECOND_AMOUNT_CELL_RE = new RegExp(`^\\s*\\$\\s*${AMOUNT}${RANGE_SEP}\\$?\\s*${AMOUNT}`, "i");
const SECOND_PCT_CELL_RE = /^\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%\s*(?:to|and|-|–|—)\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%/i;
const SECOND_PM_CELL_RE = new RegExp(`^\\s*\\$\\s*${PM_NUMBER}${PM_UNIT}${PM_TOLERANCE}`, "i");
const SECOND_POINT_CELL_RE = new RegExp(`^\\s*${APPROX}([0-9]{1,2}(?:\\.[0-9]+)?)\\s*%`, "i");

/** The last statement a pattern finds in the 400 characters before `at`, if no sentence break follows it. */
function outlookStatement(text: string, at: number, re: RegExp): RegExpMatchArray | null {
  const before = text.slice(Math.max(0, at - 400), at);
  let last: RegExpMatchArray | null = null;
  for (const m of before.matchAll(re)) last = m;
  return last && !SENTENCE_BREAK_RE.test(before.slice((last.index ?? 0) + last[0].length)) ? last : null;
}

function columnBasis(word: string): RangeBasis {
  return NON_GAAP_RE.test(word) ? "NON_GAAP" : "GAAP";
}

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
  // OUTLOOK_ROW: a release-table row or outlook bullet under a guidance heading;
  // POINT_ESTIMATE: an outlook row stated as one approximate value ("Approximately 85.95%"), low equal to high.
  statedAs: "RANGE" | "MIDPOINT_PLUS_MINUS" | "OUTLOOK_ROW" | "POINT_ESTIMATE";
  // The same metric stated on another basis ("GAAP ... ; non-GAAP ..."), one per basis.
  alternates: Omit<NonNullable<RangeMatch>, "alternates">[];
} | null;

function clauseBasis(clause: string): RangeBasis {
  // One range for both (NVDA: "GAAP and non-GAAP gross margins are expected to be 74.0% ...") (2.5.10).
  if (BOTH_BASES_RE.test(clause)) return "GAAP_AND_NON_GAAP";
  if (NON_GAAP_RE.test(clause)) return "NON_GAAP";
  return GAAP_RE.test(clause) ? "GAAP" : "NOT_STATED";
}

/**
 * The basis of the clause holding a range: from the last clause break before its first amount (`anchor`) to the
 * words after it. A break inside the match ends an earlier clause: BE's "...GAAP to Non-GAAP financial measures
 * ... Guidance ... • Revenue: $3.4B - $3.8B" is not a non-GAAP range (2.5.21).
 */
function rangeBasis(text: string, at: number, len: number, anchor: number = at): RangeBasis {
  const before = text.slice(Math.max(0, anchor - 200), anchor);
  let start = 0;
  for (const m of before.matchAll(CLAUSE_START_RE)) start = (m.index ?? 0) + m[0].length;
  const tail = CLAUSE_TAIL_RE.exec(text.slice(at + len))?.[0] ?? "";
  return clauseBasis(`${before.slice(start)}${text.slice(anchor, at + len)}${tail}`);
}

type Pattern = { re: RegExp; kind: "range" | "pm_amount" | "pm_points" | "table" | "table_pm" | "table_point" };

/**
 * Guidance ranges stated in release text; low and high are the number text as
 * written (computed exactly for a midpoint and tolerance). Keyword-first
 * wording wins; metric-first wording ("revenue ... is expected to be
 * between") is read when there is none, then release-table rows under a
 * guidance heading. Ranges for the same metric on another basis are kept as
 * alternates.
 */
export function guidanceRanges(text: string, periodOf: ((at: number, len: number) => string | null) | null = null): { revenue: RangeMatch; grossMargin: RangeMatch; eps: RangeMatch } {
  const pick = (money: boolean, ...patterns: Pattern[]): RangeMatch => {
    const found: Omit<NonNullable<RangeMatch>, "alternates">[] = [];
    const periods: (string | null)[] = [];
    for (const { re, kind } of patterns) {
      for (const m of text.matchAll(new RegExp(re.source, `${re.flags.replace("d", "")}gd`))) {
        const at = m.index ?? 0;
        const tableRow = kind === "table" || kind === "table_pm" || kind === "table_point";
        if (tableRow && !underGuidanceHeading(text, at)) continue;
        const bounds = kind === "pm_amount" || kind === "table_pm" ? pmBounds(m[1], m[2], m[3], m[4], m[5])
          : kind === "pm_points" ? pointBounds(m[1], m[2], m[3])
          : kind === "table_point" ? { low: m[1], high: m[1] }
          : { low: m[1], high: m[2] };
        if (!bounds) continue;
        const firstAt = m.indices?.[1]?.[0] ?? at;
        // An unscaled amount takes the outlook table's stated scale: "$10,300 - $10,800" (in millions).
        const scaleWord = money && !/[a-z]\s*$/i.test(bounds.low) && !/[a-z]\s*$/i.test(bounds.high)
          ? outlookStatement(text, firstAt, OUTLOOK_SCALE_RE)?.[1].toLowerCase().replace(/s$/, "") ?? null : null;
        const scaled = (t: string): string => (scaleWord ? `${t} ${scaleWord}` : t);
        // Under a "GAAP Non-GAAP" header a range takes its column's basis; an "N/A" first cell puts it in the second.
        const columns = outlookStatement(text, firstAt, OUTLOOK_COLUMNS_RE);
        const twoColumns = columns && columnBasis(columns[1]) !== columnBasis(columns[2]) ? [columnBasis(columns[1]), columnBasis(columns[2])] : null;
        const column = twoColumns && /\bN\/A\s*\$?\s*$/i.test(text.slice(Math.max(0, firstAt - 12), firstAt)) ? 1 : 0;
        const statedAs: NonNullable<RangeMatch>["statedAs"] = kind === "table" ? "OUTLOOK_ROW" : kind === "range" ? "RANGE" : kind === "table_point" ? "POINT_ESTIMATE" : "MIDPOINT_PLUS_MINUS";
        found.push({
          excerpt: m[0],
          low: scaled(bounds.low),
          high: scaled(bounds.high),
          // A table row's basis is its own label: the rows above it belong to other metrics.
          basis: twoColumns ? twoColumns[column] : tableRow ? clauseBasis(m[0]) : rangeBasis(text, at, m[0].length, firstAt),
          statedAs,
        });
        periods.push(periodOf ? periodOf(at, m[0].length) : null);
        if (twoColumns && column === 0 && kind !== "pm_amount" && kind !== "pm_points") {
          const rest = text.slice(at + m[0].length);
          let second: { text: string; low: string; high: string } | null = null;
          if (kind === "table_pm") {
            const c = SECOND_PM_CELL_RE.exec(rest);
            const b = c ? pmBounds(c[1], c[2], c[3], c[4], c[5]) : null;
            second = c && b ? { text: c[0], ...b } : null;
          } else if (kind === "table_point") {
            const c = SECOND_POINT_CELL_RE.exec(rest);
            second = c ? { text: c[0], low: c[1], high: c[1] } : null;
          } else {
            const c = (/%\s*$/.test(m[0]) ? SECOND_PCT_CELL_RE : SECOND_AMOUNT_CELL_RE).exec(rest);
            second = c ? { text: c[0], low: c[1], high: c[2] } : null;
          }
          if (second) {
            found.push({ excerpt: `${m[0]}${second.text}`, low: scaled(second.low), high: scaled(second.high), basis: twoColumns[1], statedAs });
            periods.push(periodOf ? periodOf(at, m[0].length) : null);
          }
        }
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
    revenue: pick(true, { re: REVENUE_FIRST_RE, kind: "range" }, { re: KEYWORD_FIRST_RE, kind: "range" }, { re: METRIC_FIRST_REVENUE_RE, kind: "range" },
      { re: METRIC_FIRST_REVENUE_PM_RE, kind: "pm_amount" }, { re: TABLE_REVENUE_RE, kind: "table" }, { re: TABLE_REVENUE_PM_RE, kind: "table_pm" }),
    grossMargin: pick(false, { re: GROSS_MARGIN_RE, kind: "range" }, { re: METRIC_FIRST_GROSS_MARGIN_RE, kind: "range" },
      { re: METRIC_FIRST_GROSS_MARGIN_PM_RE, kind: "pm_points" }, { re: TABLE_GROSS_MARGIN_RE, kind: "table" }, { re: TABLE_GROSS_MARGIN_POINT_RE, kind: "table_point" }),
    eps: pick(false, { re: EPS_RE, kind: "range" }, { re: EPS_CONTINUATION_RE, kind: "range" }, { re: METRIC_FIRST_EPS_RE, kind: "range" }, { re: METRIC_FIRST_EPS_PM_RE, kind: "pm_amount" },
      { re: TABLE_EPS_RE, kind: "table" }, { re: TABLE_EPS_PM_RE, kind: "table_pm" }),
  };
}

// ── Reported release metrics (2.5.20) ───────────────────────────────────────
//
// extract_earnings_metrics' text fallback read MU's FQ4 2026 revenue as 54229 USD: the highlights bullet
// "Revenue of $54.23 billion versus…" has no result verb, so the statement table's "$ 54,229" (in millions)
// was read as written. A figure is read for a metric only when the sentence proves it is that metric's
// result for the quarter:
// - a sentence with guidance, award, backlog or ± wording is never read, nor one that names only an annual
//   period ("in 2025", "fiscal 2026 revenue", "full year");
// - the label leads the sentence or follows a period or GAAP qualifier: "Gaming revenue" and "Services
//   revenues" are segments, not the total;
// - a change verb ("increased 5.2% to") reads the figure after "to", never the change itself;
// - a non-GAAP or adjusted figure, a per-share figure for a total, and a figure attributed to another metric
//   ("$13.7 billion of free cash flow" after "capital expenditures") are never read;
// - an unscaled figure takes the release's table scale only when the release declares exactly one.

// Sentences end at . ! ? (also before a closing quote) and at bullet markers ("•Revenue" needs no space after
// the bullet, MU 2.5.20), including the " o " bullets
// SEC-rendered press releases carry, so one bullet's value is never read for another's label.
const RELEASE_SPLIT_RE = /(?<=[.!?])\s+|(?<=[.!?]["”’])\s+|\s+[•●▪◦]\s*|\s+·\s+|\s+o\s+(?=[A-Z])/;
const GUIDANCE_CONTEXT_RE = /\b(?:guidance|outlook|expect(?:s|ed|ation)?|forecast(?:s|ing)?|project(?:s|ed)?|target|range)\b/i;
const REPORTED_CONTEXT_RE = /\b(?:reported|was|were|totaled|totalled|generated|delivered|achieved|increased|decreased|grew|rose|declined|fell)\b/i;
// "$125 million" of government awards is not revenue (ASTS Q2 2026).
const NON_RESULT_CONTEXT_RE = /\b(?:awards?|awarded|contract value|aggregate value|backlog|bookings|orders?|pipeline|contracted)\b/i;
const ANNUAL_CONTEXT_RE = /\b(?:full[\s-]year|(?:full )?fiscal year|year[\s-]to[\s-]date|twelve months|12 months|annual|in (?:fiscal )?(?:19|20)[0-9]{2}|fiscal (?:19|20)[0-9]{2}|FY ?(?:19|20)?[0-9]{2})\b/i;
const QUARTER_CONTEXT_RE = /\b(?:quarter(?:ly)?|Q[1-4]|three months|13 weeks)\b/i;

export type ReleaseMetricName = "revenue" | "epsDiluted" | "grossMargin" | "operatingIncome" | "freeCashFlow" | "capex";
export type ReleaseMetricHit = { value: number; rawValue: string; scaleBasis: string | null; sentence: string };

const RELEASE_LABELS: Record<ReleaseMetricName, string> = {
  revenue: "\\b(?:net sales|net revenues?|total revenues?|revenues?)\\b",
  epsDiluted: "\\b(?:diluted (?:earnings|net income|net loss|income|loss)(?: \\(loss\\))? per (?:common )?share|diluted eps|eps \\(diluted\\))(?![\\w(])",
  grossMargin: "\\bgross margin\\b",
  operatingIncome: "\\boperating income\\b",
  freeCashFlow: "\\bfree cash flow\\b",
  capex: "\\b(?:capital expenditures|capex)\\b",
};
// Between a label and its figure: words, and the period tokens a release puts there ("for the fourth quarter
// of fiscal 2026 was", "ended July 27, 2025, of", "in Q1 FY27") or a change ("increased 5.2% to"). Never another figure.
const RELEASE_GAP = "(?:[^$0-9%]|\\b(?:19|20)[0-9]{2}\\b|\\b[0-3]?[0-9], (?:19|20)[0-9]{2}\\b|\\bQ[1-4]\\b|\\bFY ?[0-9]{2,4}\\b|\\b[0-9]{1,3}(?:\\.[0-9]+)?\\s*(?:%|percent)(?=\\s+to\\b))";
// Groups: "(" before the $, "(" after it, "-", integer, fraction, scale word. "($0.12)" and "$ (0.12)" are negative.
const RELEASE_MONEY = "(\\(\\s*)?\\$\\s*(\\()?\\s*(-)?\\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(\\.[0-9]+)?\\s*\\)?(?:\\s*(billion|million|thousand|bn|mn|mm|b|m|k)\\b)?";
const RELEASE_PCT = "()()(-)?([0-9]{1,2})(\\.[0-9]+)?\\s*%()";
const RELEASE_SCALE: Record<string, number> = { billion: 1e9, bn: 1e9, b: 1e9, million: 1e6, mn: 1e6, mm: 1e6, m: 1e6, thousand: 1e3, k: 1e3 };
const RELEASE_TABLE_SCALE_RE = /\(\s*(?:\$|US\$|dollars|amounts)?\s*in\s+(thousands|millions|billions)\b/gi;
const RELEASE_NON_GAAP_RE = /\bnon-?\s?GAAP\b|\badjusted\b/i;
const RELEASE_PLUS_MINUS_RE = /±|\+\/-|\bplus or minus\b/i;
const RELEASE_PER_SHARE_AFTER_RE = /^\s*(?:per|a)\s+(?:diluted\s+|basic\s+)?(?:common\s+)?(?:share|ADS|ADR)\b/i;
const RELEASE_PER_DILUTED_WORDING_RE = /\bper\s+diluted\s+(?:common\s+)?share\b/i;
const RELEASE_CHANGE_RE = /\b(?:increase[sd]?|decrease[sd]?|grew|grow(?:s|th)?|rose|declined?|fell|up|down|improved?)\b/i;
const RELEASE_ATTRIBUTED_RE = /^\s*(?:of|in)\s+(?:[A-Za-z-]+\s+){0,2}?(free cash flow|operating cash flow|cash|revenues?|net sales|net income|net loss|operating income|capital expenditures|gross margin|earnings)\b/i;
// The word before a label: a period or GAAP qualifier, a result verb, or a sentence, clause or heading boundary.
const RELEASE_QUALIFIER_RE = /^(?:gaap|total|record|quarterly|consolidated|reported|delivered|achieved|generated|posted|preliminary|the|our|its|with|and|of|in|for|q[1-4]|fy[0-9]{2,4}|(?:19|20)[0-9]{2}|first|second|third|fourth|(?:first|second|third|fourth)-quarter|quarter|fiscal)$/i;

function releaseScales(text: string): string[] {
  return [...new Set([...text.matchAll(RELEASE_TABLE_SCALE_RE)].map((m) => m[1].toLowerCase().replace(/s$/, "")))];
}

function qualifiedLabel(before: string): boolean {
  const raw = before.match(/(\S+)\s*$/)?.[1] ?? "";
  if (/[:;,]$/.test(raw)) return true;
  const token = raw.replace(/^\W+|\W+$/g, "");
  return token === "" || RELEASE_QUALIFIER_RE.test(token);
}

/** The first reported release figure for a metric under the 2.5.20 rules, or null. */
export function releaseTextMetric(text: string, metric: ReleaseMetricName): ReleaseMetricHit | null {
  const collapsed = collapseWs(text);
  const tableScales = releaseScales(collapsed);
  const labelSource = RELEASE_LABELS[metric];
  const money = metric !== "grossMargin" && metric !== "epsDiluted";
  const amountSource = metric === "grossMargin" ? RELEASE_PCT : RELEASE_MONEY;
  const label = new RegExp(labelSource, "i");
  const resultLead = new RegExp(`^(?:GAAP\\s+|total\\s+)?(?:${labelSource})\\s*(?:of|:|was|were|totaled|totalled)\\b`, "i");
  const labelFirst = new RegExp(`(${labelSource})(${RELEASE_GAP}{0,100}?)(${amountSource})`, "gi");
  // "net income of $37.70 billion, or $32.87 per diluted share"
  const perDilutedShare = new RegExp(`${RELEASE_MONEY.replace("(?:\\s*(billion|million|thousand|bn|mn|mm|b|m|k)\\b)?", "()")}\\s+per\\s+diluted\\s+(?:common\\s+)?share\\b`, "gi");
  for (const raw of collapsed.split(RELEASE_SPLIT_RE)) {
    const piece = raw.trim();
    if (!piece || piece.length > 600) continue;
    if (GUIDANCE_CONTEXT_RE.test(piece) || NON_RESULT_CONTEXT_RE.test(piece) || RELEASE_PLUS_MINUS_RE.test(piece)) continue;
    if (ANNUAL_CONTEXT_RE.test(piece) && !QUARTER_CONTEXT_RE.test(piece)) continue;
    const labelled = label.test(piece);
    const perShareForm = metric === "epsDiluted" && RELEASE_PER_DILUTED_WORDING_RE.test(piece);
    if (!labelled && !perShareForm) continue;
    if (!REPORTED_CONTEXT_RE.test(piece) && !resultLead.test(piece) && !perShareForm) continue;
    const candidates: { kind: "label" | "perShare"; m: RegExpExecArray }[] = [];
    if (labelled) {
      labelFirst.lastIndex = 0;
      for (let m = labelFirst.exec(piece); m; m = labelFirst.exec(piece)) candidates.push({ kind: "label", m });
    }
    if (metric === "epsDiluted") {
      perDilutedShare.lastIndex = 0;
      for (let m = perDilutedShare.exec(piece); m; m = perDilutedShare.exec(piece)) candidates.push({ kind: "perShare", m });
    }
    candidates.sort((x, y) => x.m.index - y.m.index);
    for (const { kind, m } of candidates) {
      const [outerParen, innerParen, minus, intPart, frac, word] = kind === "label" ? m.slice(4) : m.slice(1);
      const amountStart = kind === "label" ? m.index + m[0].length - m[3].length : m.index;
      const gap = kind === "label" ? m[2] : "";
      if (kind === "label") {
        if (!qualifiedLabel(piece.slice(0, m.index))) continue;
        if (RELEASE_CHANGE_RE.test(gap) && !/\bto\s*$/i.test(gap)) continue;
      }
      // A label-first figure: its label and the 40 characters before it; a per-share figure: the piece before it.
      const lead = piece.slice(kind === "label" ? Math.max(0, m.index - 40) : 0, amountStart);
      if (RELEASE_NON_GAAP_RE.test(lead)) continue;
      const after = piece.slice(m.index + m[0].length);
      if (money && RELEASE_PER_SHARE_AFTER_RE.test(after)) continue;
      const attributed = after.match(RELEASE_ATTRIBUTED_RE);
      if (kind === "label" && attributed && !label.test(attributed[1])) continue;
      let scale = 1;
      let scaleBasis: string | null = null;
      if (money) {
        if (word) {
          scale = RELEASE_SCALE[word.toLowerCase()];
          scaleBasis = "AS_WRITTEN";
        } else if (tableScales.length > 1) {
          continue;
        } else if (tableScales.length === 1) {
          scale = RELEASE_SCALE[tableScales[0]];
          scaleBasis = `RELEASE_TABLE_IN_${tableScales[0].toUpperCase()}S`;
        }
      }
      const magnitude = Number(`${intPart.replace(/,/g, "")}${frac ?? ""}`);
      if (!Number.isFinite(magnitude)) continue;
      const scaled = scale === 1 ? magnitude : Math.round(magnitude * scale);
      // An unsigned figure stated as a loss is negative: "diluted net loss per share of $0.12", "net loss of $0.12 per
      // diluted share", "free cash flow was negative $5 billion".
      const statedLoss = /\bnegative\s*$/i.test(gap) || (metric === "epsDiluted" && (kind === "label"
        ? /\bloss\b/i.test(m[1]) && !/\b(?:income|earnings)\b/i.test(m[1])
        : /\bnet loss\b/i.test(lead.match(/\bnet (?:income|earnings|loss)\b/gi)?.pop() ?? "")));
      const negative = !!outerParen || innerParen === "(" || minus === "-" || statedLoss;
      const value = negative && scaled !== 0 ? -scaled : scaled;
      const rawValue = (kind === "label" ? m[3] : m[0]).trim();
      return { value, rawValue, scaleBasis, sentence: piece };
    }
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
