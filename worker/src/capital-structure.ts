// Company-disclosed capital structure from inline XBRL (2.3.0).
//
// The SEC companyfacts API drops dimensional facts, so per-instrument debt
// terms (face amount, coupon, maturity, conversion price) and per-class
// warrant counts are only available in the filing's own inline XBRL. This
// module parses those facts and builds two mechanical views from them:
// a dilution bridge at a caller-supplied price, and a capital-structure
// timeline at the filing's period end. Nothing here forecasts; every number
// is a company disclosure or a stated formula over company disclosures.
//
// yfmcp/capital_structure.py mirrors this file; scripts/test_capital_structure.py
// requires identical output from both.

import { TEXT_DATE_SOURCE, textDate } from "./fiscal-calendar.js";

export type IxContext = {
  id: string;
  instant: string | null;
  start: string | null;
  end: string | null;
  dims: Record<string, string>;
};

export type IxFact = {
  name: string;
  local: string;
  contextRef: string;
  unit: string | null;
  value: number | null;
  text: string | null;
  periodEnd: string | null;
  periodStart: string | null;
  dims: Record<string, string>;
  /** The fact's decimals attribute; null when INF or absent (exact). */
  decimals: number | null;
  /** For investment concepts outside a table, the sentence the fact sits in; else null. */
  sentence: string | null;
  order: number;
};

export type IxDocument = {
  facts: IxFact[];
  contextCount: number;
  documentPeriodEnd: string | null;
  documentType: string | null;
};

export type IxSource = {
  role: string;
  filingType: string;
  filingDate: string | null;
  accessionNumber: string | null;
  documentUrl: string | null;
  doc: IxDocument;
};

const ENTITY_RE = /&(#x[0-9a-f]+|#\d+|amp|lt|gt|quot|apos|nbsp);/gi;
// An explicit whitespace class, so both runtimes collapse the same characters.
const WS_RUN_RE = /[\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+/g;

export function collapse(text: string): string {
  return text.replace(WS_RUN_RE, " ").trim();
}

function decodeEntities(text: string): string {
  return text.replace(ENTITY_RE, (_m, code: string) => {
    const lower = code.toLowerCase();
    if (lower === "amp") return "&";
    if (lower === "lt") return "<";
    if (lower === "gt") return ">";
    if (lower === "quot") return "\"";
    if (lower === "apos") return "'";
    if (lower === "nbsp") return " ";
    const n = lower.startsWith("#x") ? parseInt(lower.slice(2), 16) : parseInt(lower.slice(1), 10);
    if (!Number.isFinite(n) || n <= 0 || n > 0x10ffff) return " ";
    return n === 0xa0 ? " " : String.fromCodePoint(n);
  });
}

function plainText(html: string): string {
  return collapse(decodeEntities(html.replace(/<[^>]*>/g, " ")));
}

function attr(attrs: string, name: string): string | null {
  const m = new RegExp(`(?:^|\\s)${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)')`, "i").exec(attrs);
  if (!m) return null;
  return m[1] ?? m[2] ?? null;
}

function localName(qname: string): string {
  const i = qname.indexOf(":");
  return i >= 0 ? qname.slice(i + 1) : qname;
}

const MONTHS: Record<string, string> = {
  jan: "01", feb: "02", mar: "03", apr: "04", may: "05", jun: "06",
  jul: "07", aug: "08", sep: "09", oct: "10", nov: "11", dec: "12",
};

function pad2(value: string): string {
  return value.length === 1 ? `0${value}` : value;
}

/** A disclosed date as ISO text: yyyy-mm-dd, or yyyy-mm / yyyy when that is all the text gives. */
export function normalizeIxDate(text: string | null): string | null {
  if (!text) return null;
  const t = collapse(text).replace(/(\d)(?:st|nd|rd|th)\b/gi, "$1");
  let m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(t);
  if (m) return t;
  m = /^([A-Za-z]{3,})\.? (\d{1,2}),? (\d{4})$/.exec(t);
  if (m && MONTHS[m[1].slice(0, 3).toLowerCase()]) return `${m[3]}-${MONTHS[m[1].slice(0, 3).toLowerCase()]}-${pad2(m[2])}`;
  m = /^(\d{1,2}) ([A-Za-z]{3,})\.?,? (\d{4})$/.exec(t);
  if (m && MONTHS[m[2].slice(0, 3).toLowerCase()]) return `${m[3]}-${MONTHS[m[2].slice(0, 3).toLowerCase()]}-${pad2(m[1])}`;
  m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(t);
  if (m) return `${m[3]}-${pad2(m[1])}-${pad2(m[2])}`;
  m = /^([A-Za-z]{3,})\.?,? (\d{4})$/.exec(t);
  if (m && MONTHS[m[1].slice(0, 3).toLowerCase()]) return `${m[2]}-${MONTHS[m[1].slice(0, 3).toLowerCase()]}`;
  m = /^(\d{4})$/.exec(t);
  if (m) return t;
  return null;
}

const NUMBER_WORDS: Record<string, number> = {
  no: 0, none: 0, zero: 0, one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9,
  ten: 10, eleven: 11, twelve: 12, thirteen: 13, fourteen: 14, fifteen: 15, sixteen: 16, seventeen: 17,
  eighteen: 18, nineteen: 19, twenty: 20, thirty: 30, forty: 40, fifty: 50, sixty: 60, seventy: 70,
  eighty: 80, ninety: 90,
};

function wordsNumber(text: string): number | null {
  const words = text.toLowerCase().replace(/-/g, " ").split(/\s+/).filter((w) => w && w !== "and");
  if (words.length === 0) return null;
  let total = 0;
  let current = 0;
  for (const word of words) {
    if (word in NUMBER_WORDS) current += NUMBER_WORDS[word];
    else if (word === "hundred") current *= 100;
    else if (word === "thousand") { total += current * 1000; current = 0; }
    else if (word === "million") { total += current * 1000000; current = 0; }
    else return null;
  }
  return total + current;
}

/** The number an ix:nonFraction displays, before scale and sign. */
export function ixNumber(text: string, format: string | null): number | null {
  const fmt = localName(format ?? "").toLowerCase();
  const t = collapse(text);
  // ixt:fixed-zero and ixt:zerodash display a dash for zero.
  if (fmt.includes("zero")) return 0;
  if (fmt.includes("word")) return wordsNumber(t);
  let digits = t.replace(/[ $\u20ac\u00a3%()]/g, "");
  if (fmt.includes("comma-decimal") || fmt.includes("numcommadecimal")) {
    digits = digits.replace(/\./g, "").replace(/,/g, ".");
  } else {
    digits = digits.replace(/,/g, "");
  }
  if (!/^\d+(?:\.\d+)?$/.test(digits)) return null;
  return parseFloat(digits);
}

function applyScale(value: number, scale: string | null): number {
  const n = scale ? parseInt(scale, 10) : 0;
  if (!Number.isFinite(n) || n === 0) return value;
  return n > 0 ? value * 10 ** n : value / 10 ** -n;
}

function parseContexts(html: string): Map<string, IxContext> {
  const out = new Map<string, IxContext>();
  const re = /<((?:xbrli:)?context)\b([^>]*)>([\s\S]*?)<\/\1\s*>/gi;
  for (const m of html.matchAll(re)) {
    const id = attr(m[2], "id");
    if (!id) continue;
    const body = m[3];
    const instant = /<(?:xbrli:)?instant\s*>\s*([^<\s]+)\s*</i.exec(body);
    const start = /<(?:xbrli:)?startDate\s*>\s*([^<\s]+)\s*</i.exec(body);
    const end = /<(?:xbrli:)?endDate\s*>\s*([^<\s]+)\s*</i.exec(body);
    const dims: Record<string, string> = {};
    for (const d of body.matchAll(/<xbrldi:explicitMember\b([^>]*)>\s*([^<\s]+)\s*<\/xbrldi:explicitMember\s*>/gi)) {
      const axis = attr(d[1], "dimension");
      if (axis) dims[localName(axis)] = d[2];
    }
    for (const d of body.matchAll(/<xbrldi:typedMember\b([^>]*)>([\s\S]*?)<\/xbrldi:typedMember\s*>/gi)) {
      const axis = attr(d[1], "dimension");
      if (axis) dims[localName(axis)] = plainText(d[2]);
    }
    out.set(id, {
      id,
      instant: instant ? instant[1].slice(0, 10) : null,
      start: start ? start[1].slice(0, 10) : null,
      end: end ? end[1].slice(0, 10) : null,
      dims,
    });
  }
  return out;
}

function parseUnits(html: string): Map<string, string> {
  const out = new Map<string, string>();
  const re = /<((?:xbrli:)?unit)\b([^>]*)>([\s\S]*?)<\/\1\s*>/gi;
  for (const m of html.matchAll(re)) {
    const id = attr(m[2], "id");
    if (!id) continue;
    const measures = (part: string) => [...part.matchAll(/<(?:xbrli:)?measure\s*>\s*([^<\s]+)\s*</gi)].map((x) => localName(x[1]));
    const num = /<(?:xbrli:)?unitNumerator\b[^>]*>([\s\S]*?)<\/(?:xbrli:)?unitNumerator\s*>/i.exec(m[3]);
    const den = /<(?:xbrli:)?unitDenominator\b[^>]*>([\s\S]*?)<\/(?:xbrli:)?unitDenominator\s*>/i.exec(m[3]);
    out.set(id, num && den ? `${measures(num[1]).join("*")}/${measures(den[1]).join("*")}` : measures(m[3]).join("*"));
  }
  return out;
}

const NON_NUMERIC_MAX_CHARS = 300;

/** An ix decimals attribute as a number; INF, absent or malformed is null (exact). */
function parseDecimals(raw: string | null): number | null {
  if (raw == null) return null;
  const text = raw.trim();
  return /^-?\d+$/.test(text) ? parseInt(text, 10) : null;
}

// Investment facts whose surrounding sentence is kept, so one that restates
// part of cash ("classified as cash equivalents") can be recognised.
const SENTENCE_CONCEPTS = new Set([
  "ShortTermInvestments", "MarketableSecuritiesCurrent", "AvailableForSaleSecuritiesDebtSecuritiesCurrent", "MarketableSecuritiesNoncurrent",
  "DebtSecuritiesHeldToMaturityAmortizedCostAfterAllowanceForCreditLossCurrent", "HeldToMaturitySecuritiesCurrent",
  "DebtSecuritiesHeldToMaturityExcludingAccruedInterestAfterAllowanceForCreditLossCurrent", "OtherShortTermInvestments",
]);
const SENTENCE_WINDOW = 1500;
const SENTENCE_MAX_CHARS = 500;

function lastSentenceStart(text: string): number {
  let start = 0;
  for (const m of text.matchAll(/[.!?]\s/g)) start = (m.index ?? 0) + m[0].length;
  return start;
}

/** The sentence around a fact outside any table, from the raw HTML either side of it. */
function factSentence(html: string, open: number, bodyStart: number, bodyEnd: number, closeEnd: number, tables: [number, number][]): string | null {
  if (tables.some(([a, b]) => open >= a && open < b)) return null;
  let before = html.slice(Math.max(0, open - SENTENCE_WINDOW), open);
  const gt = before.indexOf(">");
  const lt = before.indexOf("<");
  if (gt >= 0 && (lt < 0 || gt < lt)) before = before.slice(gt + 1);
  let after = html.slice(closeEnd, closeEnd + SENTENCE_WINDOW);
  const lastLt = after.lastIndexOf("<");
  if (lastLt > after.lastIndexOf(">")) after = after.slice(0, lastLt);
  const beforeText = plainText(before);
  const afterText = plainText(after);
  const end = /[.!?](?:\s|$)/.exec(afterText);
  const sentence = collapse(`${beforeText.slice(lastSentenceStart(beforeText))} ${plainText(html.slice(bodyStart, bodyEnd))} ${end ? afterText.slice(0, end.index + 1) : afterText}`);
  return sentence.slice(0, SENTENCE_MAX_CHARS) || null;
}

/** Every numeric fact and short text fact in an inline XBRL document, with its period and dimensions. */
export function parseIxbrl(html: string): IxDocument {
  const contexts = parseContexts(html);
  const units = parseUnits(html);
  const facts: IxFact[] = [];
  const seen = new Set<string>();
  let order = 0;
  let tables: [number, number][] | null = null;
  const tableSpans = (): [number, number][] => {
    tables ??= [...html.matchAll(/<table\b[\s\S]*?<\/table\s*>/gi)].map((m) => [m.index ?? 0, (m.index ?? 0) + m[0].length] as [number, number]);
    return tables;
  };
  const push = (name: string, contextRef: string, unit: string | null, value: number | null, text: string | null, decimals: number | null = null, sentence: string | null = null) => {
    const ctx = contexts.get(contextRef);
    const key = `${name}|${contextRef}|${unit ?? ""}|${value ?? ""}|${text ?? ""}`;
    if (seen.has(key)) return;
    seen.add(key);
    facts.push({
      name,
      local: localName(name),
      contextRef,
      unit,
      value,
      text,
      periodEnd: ctx ? (ctx.instant ?? ctx.end) : null,
      periodStart: ctx ? ctx.start : null,
      dims: ctx ? ctx.dims : {},
      decimals,
      sentence,
      order: order++,
    });
  };

  const numOpen = /<ix:nonFraction\b([^>]*?)(\/?)>/gi;
  const numClose = /<\/ix:nonFraction\s*>/gi;
  for (const m of html.matchAll(numOpen)) {
    if (m[2] === "/") continue;
    const attrs = m[1];
    const name = attr(attrs, "name");
    const contextRef = attr(attrs, "contextRef");
    if (!name || !contextRef) continue;
    const bodyStart = (m.index ?? 0) + m[0].length;
    numClose.lastIndex = bodyStart;
    const close = numClose.exec(html);
    if (!close) continue;
    let value = ixNumber(plainText(html.slice(bodyStart, close.index)), attr(attrs, "format"));
    if (value == null) continue;
    value = applyScale(value, attr(attrs, "scale"));
    if (attr(attrs, "sign") === "-") value = -value;
    const unitRef = attr(attrs, "unitRef");
    const sentence = SENTENCE_CONCEPTS.has(localName(name))
      ? factSentence(html, m.index ?? 0, bodyStart, close.index, close.index + close[0].length, tableSpans())
      : null;
    push(name, contextRef, unitRef ? (units.get(unitRef) ?? unitRef) : null, value, null, parseDecimals(attr(attrs, "decimals")), sentence);
  }

  const textOpen = /<ix:nonNumeric\b([^>]*?)(\/?)>/gi;
  const textClose = /<\/ix:nonNumeric\s*>/gi;
  const textNested = /<ix:nonNumeric\b/gi;
  for (const m of html.matchAll(textOpen)) {
    if (m[2] === "/") continue;
    const attrs = m[1];
    const name = attr(attrs, "name");
    const contextRef = attr(attrs, "contextRef");
    if (!name || !contextRef || /TextBlock$|Policy/i.test(name)) continue;
    const bodyStart = (m.index ?? 0) + m[0].length;
    textClose.lastIndex = bodyStart;
    const close = textClose.exec(html);
    if (!close) continue;
    textNested.lastIndex = bodyStart;
    const nested = textNested.exec(html);
    if (nested && nested.index < close.index) continue;
    const text = plainText(html.slice(bodyStart, close.index));
    if (!text || text.length > NON_NUMERIC_MAX_CHARS) continue;
    push(name, contextRef, null, null, text);
  }

  const dei = (local: string) => facts.find((f) => f.local === local && f.text != null)?.text ?? null;
  const periodText = dei("DocumentPeriodEndDate");
  return {
    facts,
    contextCount: contexts.size,
    documentPeriodEnd: normalizeIxDate(periodText) ?? latestPeriodEnd(facts),
    documentType: dei("DocumentType"),
  };
}

function latestPeriodEnd(facts: IxFact[]): string | null {
  let best: string | null = null;
  for (const f of facts) {
    if (f.value == null || Object.keys(f.dims).length > 0 || !f.periodEnd || !f.name.startsWith("us-gaap:")) continue;
    if (best == null || f.periodEnd > best) best = f.periodEnd;
  }
  return best;
}

// ── Fact selection ──────────────────────────────────────────────────────────

type Picked = { value: number; periodEnd: string | null; concept: string; unit: string | null; decimals: number | null; sentence: string | null };

function hasDims(f: IxFact): boolean {
  return Object.keys(f.dims).length > 0;
}

/** Decimal places a fact is accurate to; an exact (INF) fact ranks above any rounding. */
function precision(decimals: number | null): number {
  return decimals ?? Number.POSITIVE_INFINITY;
}

/** The latest fact; on the same date the most precise, so a statement line beats a rounded narrative figure. */
function newest(facts: IxFact[]): IxFact | null {
  let best: IxFact | null = null;
  for (const f of facts) {
    const end = f.periodEnd ?? "";
    const bestEnd = best ? (best.periodEnd ?? "") : "";
    if (best == null || end > bestEnd || (end === bestEnd && precision(f.decimals) > precision(best.decimals))) best = f;
  }
  return best;
}

function picked(f: IxFact | null): Picked | null {
  return f && f.value != null ? { value: f.value, periodEnd: f.periodEnd, concept: f.name, unit: f.unit, decimals: f.decimals, sentence: f.sentence } : null;
}

/** The newest undimensioned value of a concept, optionally at one date. */
function total(doc: IxDocument, local: string, at: string | null = null): Picked | null {
  return picked(newest(doc.facts.filter((f) => f.local === local && f.value != null && !hasDims(f) && (at == null || f.periodEnd === at))));
}

function firstTotal(doc: IxDocument, locals: string[], at: string | null = null): Picked | null {
  for (const local of locals) {
    const hit = total(doc, local, at);
    if (hit) return hit;
  }
  return null;
}

function dimsKey(dims: Record<string, string>): string {
  return Object.keys(dims).sort().map((k) => `${k}=${dims[k]}`).join("&");
}

/** "aapl:ConvertibleSeniorNotesDue2029Member" -> "Convertible Senior Notes Due 2029". */
export function memberLabel(member: string): string {
  return localName(member)
    .replace(/Member$/, "")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    // "ClassBCommonStock" -> "Class B Common Stock"
    .replace(/([A-Z])([A-Z][a-z])/g, "$1 $2")
    .replace(/([A-Za-z])(\d)/g, "$1 $2")
    .replace(/(\d)([A-Za-z])/g, "$1 $2")
    // "December282028" -> "December 28, 2028" (2.5.24, F-011): a day and a year run together after a month name.
    .replace(MONTH_DAY_YEAR_RUN_RE, "$1 $2, $3")
    .replace(/\s+/g, " ")
    .trim();
}

const MONTH_DAY_YEAR_RUN_RE = /\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sept?|Oct|Nov|Dec) (\d{1,2})((?:19|20)\d{2})\b/g;
const LABEL_DATE_RE = /\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}\b/;

export function round(value: number, digits = 0): number {
  const f = 10 ** digits;
  return Math.floor(value * f + 0.5) / f;
}

function sourceRef(source: IxSource, periodEnd: string | null): Record<string, unknown> {
  return {
    filingType: source.filingType,
    filingDate: source.filingDate,
    accessionNumber: source.accessionNumber,
    documentUrl: source.documentUrl,
    periodEnd,
  };
}

function findInSources<T>(sources: IxSource[], fn: (doc: IxDocument) => T | null): { value: T; source: IxSource } | null {
  for (const source of sources) {
    const value = fn(source.doc);
    if (value != null) return { value, source };
  }
  return null;
}

// ── Dilution bridge ─────────────────────────────────────────────────────────

const OPTIONS_OUTSTANDING = "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingNumber";
const OPTIONS_STRIKE = "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingWeightedAverageExercisePrice";
const OPTIONS_EXERCISABLE = "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsExercisableNumber";
const RANGE_OUTSTANDING = "ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeOutstandingOptions";
const RANGE_STRIKE = "ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeOutstandingOptionsWeightedAverageExercisePrice";
const UNVESTED_AWARDS = "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber";
const AWARD_AXIS_RE = /Award|PlanName|Plan\b|Grant|Vesting/i;
// The outstanding count, else the number of shares the warrants are exercisable for.
const WARRANT_COUNT_CONCEPTS = ["ClassOfWarrantOrRightOutstanding", "ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights"];
const WARRANT_STRIKE = "ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1";
const CONVERSION_PRICE = "DebtInstrumentConvertibleConversionPrice1";
const CONVERSION_RATIO = "DebtInstrumentConvertibleConversionRatio1";
const FACE_AMOUNT = "DebtInstrumentFaceAmount";
const PRINCIPAL_FALLBACK_CONCEPTS = ["DebtInstrumentCarryingAmount", "LongTermDebt", "ConvertibleNotesPayable", "ConvertibleLongTermNotesPayable", "LongTermDebtNoncurrent", "SeniorNotes"];
const DEBT_AXES = ["DebtInstrumentAxis", "LongtermDebtTypeAxis"];

function treasuryStock(count: number, strike: number, price: number): number {
  return price > strike ? count * (1 - strike / price) : 0;
}

function basicShares(doc: IxDocument): Record<string, unknown> | null {
  const cover = doc.facts.filter((f) => f.local === "EntityCommonStockSharesOutstanding" && f.value != null);
  if (cover.length > 0) {
    const date = cover.reduce((best, f) => ((f.periodEnd ?? "") > best ? (f.periodEnd ?? "") : best), "");
    const atDate = cover.filter((f) => (f.periodEnd ?? "") === date);
    const plain = atDate.find((f) => !hasDims(f));
    const classes = atDate.filter((f) => hasDims(f)).map((f) => ({
      class: memberLabel(Object.values(f.dims)[0] ?? ""),
      shares: f.value,
    }));
    const value = plain ? plain.value! : atDate.reduce((sum, f) => sum + (f.value ?? 0), 0);
    return { shares: value, asOf: date || null, concept: "dei:EntityCommonStockSharesOutstanding", basis: "cover_page", classes };
  }
  const bs = total(doc, "CommonStockSharesOutstanding");
  return bs ? { shares: bs.value, asOf: bs.periodEnd, concept: bs.concept, basis: "balance_sheet", classes: [] } : null;
}

function optionTranches(doc: IxDocument, at: string | null): { count: number; strike: number; label: string }[] {
  const out: { count: number; strike: number; label: string }[] = [];
  for (const f of doc.facts) {
    if (f.local !== RANGE_OUTSTANDING || f.value == null || !f.dims.ExercisePriceRangeAxis || (at && f.periodEnd !== at)) continue;
    const strike = doc.facts.find((g) => g.local === RANGE_STRIKE && g.value != null && g.periodEnd === f.periodEnd && dimsKey(g.dims) === dimsKey(f.dims));
    if (!strike) continue;
    out.push({ count: f.value, strike: strike.value!, label: memberLabel(f.dims.ExercisePriceRangeAxis) });
  }
  return out;
}

/**
 * Options tagged only on award or plan axes, with no undimensioned total (AEHR tags its 316,000
 * outstanding options at $5.11 only under aehr:OutstandingOptionsStockOptionTransactionsMember on
 * AwardTypeAxis) (2.5.10). The latest date, on the fewest such axes, one entry per member.
 */
function awardAxisOptions(doc: IxDocument): { periodEnd: string; members: { label: string; count: number; strike: Picked | null; exercisable: number | null }[] } | null {
  const facts = doc.facts.filter((f) => f.local === OPTIONS_OUTSTANDING && f.value != null && hasDims(f)
    && Object.keys(f.dims).every((axis) => AWARD_AXIS_RE.test(axis)));
  if (facts.length === 0) return null;
  const date = facts.reduce((best, f) => ((f.periodEnd ?? "") > best ? (f.periodEnd ?? "") : best), "");
  const atDate = facts.filter((f) => (f.periodEnd ?? "") === date);
  const fewest = atDate.reduce((min, f) => Math.min(min, Object.keys(f.dims).length), Infinity);
  const axes = Object.keys(atDate.find((f) => Object.keys(f.dims).length === fewest)!.dims).sort().join("|");
  const chosen = atDate.filter((f) => Object.keys(f.dims).sort().join("|") === axes);
  const same = (local: string, f: IxFact) => doc.facts.find((g) => g.local === local && g.value != null && g.periodEnd === f.periodEnd && dimsKey(g.dims) === dimsKey(f.dims)) ?? null;
  return {
    periodEnd: date,
    members: chosen.map((f) => ({
      label: Object.values(f.dims).map(memberLabel).join(" / "),
      count: f.value!,
      strike: picked(same(OPTIONS_STRIKE, f)),
      exercisable: same(OPTIONS_EXERCISABLE, f)?.value ?? null,
    })),
  };
}

function optionsComponent(sources: IxSource[], price: number): Record<string, unknown> | null {
  const found = findInSources(sources, (doc) => {
    const plain = total(doc, OPTIONS_OUTSTANDING);
    if (plain) return { plain, axis: null };
    const axis = awardAxisOptions(doc);
    return axis ? { plain: null, axis } : null;
  });
  if (!found) return null;
  const { value: { plain, axis }, source } = found;
  const members = axis ? axis.members : [];
  const periodEnd = plain ? plain.periodEnd : axis!.periodEnd;
  const outstanding = plain ? plain.value : members.reduce((sum, m) => sum + m.count, 0);
  const strike = plain ? total(source.doc, OPTIONS_STRIKE, periodEnd) : members.length === 1 ? members[0].strike : null;
  const exercisable = plain ? total(source.doc, OPTIONS_EXERCISABLE, periodEnd)?.value ?? null
    : members.every((m) => m.exercisable != null) ? members.reduce((sum, m) => sum + m.exercisable!, 0) : null;
  // Several members, each with its own strike, are priced like exercise-price ranges.
  const tranches = plain ? optionTranches(source.doc, periodEnd)
    : members.length > 1 && members.every((m) => m.strike) ? members.map((m) => ({ count: m.count, strike: m.strike!.value, label: m.label })) : [];
  const out: Record<string, unknown> = {
    component: "stock_options",
    outstanding,
    exercisable,
    weightedAverageExercisePrice: strike ? strike.value : null,
    strikeUnit: strike ? strike.unit : (members.find((m) => m.strike)?.strike?.unit ?? null),
    ...(axis ? { countBasis: "award_axis_members", members: members.map((m) => ({ member: m.label, outstanding: m.count, weightedAverageExercisePrice: m.strike ? m.strike.value : null, exercisable: m.exercisable })) } : {}),
    source: sourceRef(source, periodEnd),
  };
  if (tranches.length > 0) {
    const inc = tranches.reduce((sum, t) => sum + treasuryStock(t.count, t.strike, price), 0);
    out.method = "treasury_stock_by_exercise_price_range";
    out.tranches = tranches.map((t) => ({
      range: t.label,
      outstanding: t.count,
      weightedAverageExercisePrice: t.strike,
      inTheMoney: price > t.strike,
      incrementalShares: round(treasuryStock(t.count, t.strike, price)),
    }));
    out.incrementalShares = round(inc);
  } else if (strike) {
    out.method = "treasury_stock_on_weighted_average_strike";
    out.inTheMoney = price > strike.value;
    out.incrementalShares = round(treasuryStock(outstanding, strike.value, price));
    out.note = "One weighted-average strike stands in for every tranche; tranche-level strikes can give a different count.";
  } else {
    out.method = "not_computed";
    out.incrementalShares = null;
    out.note = "No weighted-average exercise price was tagged for the same date.";
  }
  return out;
}

// Unvested award counts, most specific first: the us-gaap concept, then the
// same count under a company prefix, then outstanding or vested-and-expected-
// to-vest counts (AAOI tags only the last, under its own prefix).
const AWARD_COUNT_CONCEPTS: [RegExp, string][] = [
  [new RegExp(`^${UNVESTED_AWARDS}$`), "nonvested"],
  [/OtherThanOptionsNonvestedNumber$/i, "nonvested"],
  [/(?:OtherThanOptions|Nonoption)EquityInstrumentsOutstandingNumber$/i, "outstanding"],
  [/(?:OtherThanOptions|Nonoption)EquityInstrumentsVestedAndExpectedToVest(?:Number|OutstandingNumber)?$/i, "vested_and_expected_to_vest"],
];

function isShareCount(f: IxFact): boolean {
  return f.value != null && (f.unit == null || /shares/i.test(f.unit)) && !/USD|EUR|GBP/i.test(f.unit ?? "");
}

function awardsComponent(sources: IxSource[], tableMatches: TextMatch[] = []): Record<string, unknown> | null {
  const found = findInSources(sources, (doc) => {
    let facts: IxFact[] = [];
    let basis = "nonvested";
    for (const [re, label] of AWARD_COUNT_CONCEPTS) {
      facts = doc.facts.filter((f) => re.test(f.local) && isShareCount(f));
      basis = label;
      if (facts.length > 0) break;
    }
    if (facts.length === 0) return null;
    const date = facts.reduce((best, f) => ((f.periodEnd ?? "") > best ? (f.periodEnd ?? "") : best), "");
    const atDate = facts.filter((f) => (f.periodEnd ?? "") === date);
    const plain = atDate.find((f) => !hasDims(f));
    // Without a total, sum the breakdown on the fewest award/plan axes, so a
    // type x plan split is not also counted by type alone.
    const awardFacts = atDate.filter((f) => hasDims(f) && Object.keys(f.dims).every((axis) => AWARD_AXIS_RE.test(axis)));
    const fewest = awardFacts.reduce((min, f) => Math.min(min, Object.keys(f.dims).length), Infinity);
    const axisSet = awardFacts.find((f) => Object.keys(f.dims).length === fewest);
    const setKey = axisSet ? Object.keys(axisSet.dims).sort().join("&") : "";
    const byType = awardFacts.filter((f) => Object.keys(f.dims).sort().join("&") === setKey);
    if (!plain && byType.length === 0) return null;
    return { date, plain, byType, basis, concept: (plain ?? byType[0]).name };
  });
  if (!found) return awardsFromTable(tableMatches);
  const { value: { date, plain, byType, basis, concept }, source } = found;
  const breakdown = byType.map((f) => ({ awardType: Object.values(f.dims).map(memberLabel).join(" / "), unvested: f.value }));
  return {
    component: "unvested_share_awards",
    unvested: plain ? plain.value : byType.reduce((sum, f) => sum + (f.value ?? 0), 0),
    breakdown,
    concept,
    countBasis: basis,
    method: "gross_unvested",
    incrementalShares: round(plain ? plain.value! : byType.reduce((sum, f) => sum + (f.value ?? 0), 0)),
    note: "Unvested RSUs/PSUs are counted in full; the treasury-stock method on unrecognized compensation would count fewer, and unearned performance awards may never vest.",
    source: sourceRef(source, date || null),
  };
}

const AWARD_TABLE_RE = /restricted stock|\bRSUs?\b|stock units?|share units?|\bPSUs?\b/i;
const AWARD_ROW_RE = /^(?:unvested|nonvested|outstanding|balance)\b[^|]*?\b(?:at|as of)\s+(.+)$/i;
const TABLE_NUMBER_RE = /^\(?(\d{1,3}(?:,\d{3})+|\d{4,})\)?$/;

/** Unvested RSU count from the equity-award table rows, when the filing tags none. */
export function awardsFromTable(matches: TextMatch[]): Record<string, unknown> | null {
  let best: { date: string; value: number; match: TextMatch; row: string } | null = null;
  for (const match of matches) {
    if (!match.inTable) continue;
    const scope = `${match.tableTitle ?? ""} ${match.sectionHeading ?? ""} ${match.contextText}`;
    if (!AWARD_TABLE_RE.test(scope) || /\boptions?\b/i.test(match.tableTitle ?? "")) continue;
    const cells = collapse(match.contextText).split(" | ").map((c) => c.trim());
    const label = /^(?:unvested|nonvested|outstanding|balance)\b/i.test(match.rowLabel ?? "") ? String(match.rowLabel) : cells[0];
    const row = AWARD_ROW_RE.exec(label);
    if (!row) continue;
    const date = normalizeIxDate(row[1].replace(/[,.:;]+$/, ""));
    if (!date) continue;
    const numberCell = cells.slice(1).map((c) => c.replace(/^\$\s*/, "")).find((c) => TABLE_NUMBER_RE.test(c));
    if (!numberCell) continue;
    let value = parseFloat(numberCell.replace(/[(),]/g, ""));
    if (/in thousands/i.test(scope)) value *= 1000;
    if (best == null || date > best.date) best = { date, value, match, row: match.contextText.slice(0, 300) };
  }
  if (!best) return null;
  return {
    component: "unvested_share_awards",
    unvested: best.value,
    breakdown: [],
    concept: null,
    countBasis: "filing_table_text",
    method: "gross_unvested",
    incrementalShares: round(best.value),
    note: "Read from the filing's award table because no unvested count is tagged; check the quoted row. Counted in full, as tagged awards are.",
    evidence: { row: best.row, tableTitle: best.match.tableTitle, sectionHeading: best.match.sectionHeading, documentUrl: best.match.documentUrl, filingDate: best.match.filingDate },
    source: { filingType: null, filingDate: best.match.filingDate, accessionNumber: best.match.accessionNumber, documentUrl: best.match.documentUrl, periodEnd: best.date },
  };
}

// Unvested warrant shares (e.g. a customer warrant that vests with purchases).
const WARRANT_UNVESTED_RE = /Unvested\w*NumberOfSecuritiesCalledByWarrantsOrRights$|ClassOfWarrantOrRightUnvested\w*$/i;
// Vested warrant shares at the period end (MRVL tags ClassOfWarrantOrRightSharesVested
// per customer warrant: 1.2M of 4.2M, and 0 of 1.0M). Case-sensitive, so "Unvested" never matches (2.5.9).
const WARRANT_VESTED_RE = /^ClassOfWarrantOrRight\w*Vested(?:Number)?$/;
// A vesting term tagged for a class says its shares vest on conditions; without a vested or
// unvested count, how many are exercisable is unknown, never assumed to be all of them.
const WARRANT_VESTING_TERM = "WarrantsAndRightsOutstandingVestingTerm";

// A warrant's expiry: a tagged maturity or expiration date closes a class that expired before the
// period end; a tagged term from a count dated before the period end that has since elapsed is flagged
// (the term can run from a later exercisability date, so it never closes a class) (2.5.10).
const WARRANT_EXPIRY_RE = /^(?:WarrantsAndRightsOutstandingMaturityDate|ClassOfWarrant\w*Expir\w*Date|Warrant\w*Expir\w*Date)$/;
const WARRANT_TERM = "WarrantsAndRightsOutstandingTerm";
const TERM_WORDS: Record<string, number> = { one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9, ten: 10 };

/** Whole years in a tagged term: "P5Y", "5 years", "five years"; null when it is not whole years. */
export function termYears(text: string | null): number | null {
  if (!text) return null;
  const t = collapse(text).toLowerCase();
  const iso = /^p(\d+)y$/.exec(t);
  if (iso) return Number(iso[1]);
  const m = /^(\d+|one|two|three|four|five|six|seven|eight|nine|ten)(?:\s*|-)years?$/.exec(t);
  if (!m) return null;
  return /^\d+$/.test(m[1]) ? Number(m[1]) : TERM_WORDS[m[1]];
}

/** A count dated after the report's period end, or tagged as a subsequent event: not a period-end instrument. */
function afterPeriodEnd(doc: IxDocument, f: IxFact): boolean {
  const end = doc.documentPeriodEnd;
  return Object.keys(f.dims).some((axis) => SUBSEQUENT_EVENT_AXIS_RE.test(axis)) || (end != null && f.periodEnd != null && f.periodEnd > end);
}

/**
 * Warrants the primary report tags after its period end (MRVL's 59.0M customer warrant at $206.58,
 * issued after the quarter): claims to quote, never counted as outstanding at the period end.
 */
export function postPeriodWarrants(source: IxSource | undefined): Record<string, unknown>[] {
  if (!source) return [];
  const doc = source.doc;
  const counts = doc.facts.filter((f) => f.value != null && WARRANT_COUNT_CONCEPTS.includes(f.local) && afterPeriodEnd(doc, f) && warrantCountEvent(doc, f) == null);
  const byKey = new Map<string, IxFact>();
  for (const f of counts) {
    const prev = byKey.get(dimsKey(f.dims));
    if (!prev || (f.periodEnd ?? "") > (prev.periodEnd ?? "")) byKey.set(dimsKey(f.dims), f);
  }
  return [...byKey.values()].map((f) => {
    const strike = newest(doc.facts.filter((g) => g.local === WARRANT_STRIKE && g.value != null && dimsKey(g.dims) === dimsKey(f.dims)));
    const classMember = f.dims[CLASS_OF_WARRANT_AXIS];
    return {
      kind: "WARRANT_AFTER_PERIOD_END",
      status: "UNQUANTIFIED",
      class: classMember ? memberLabel(classMember) : null,
      shares: f.value,
      exercisePrice: strike ? strike.value : null,
      asOf: f.periodEnd,
      periodEnd: doc.documentPeriodEnd,
      concept: f.name,
      sentences: [] as string[],
      leadIn: null,
      leadOut: null,
      documentUrl: source.documentUrl,
      filingDate: source.filingDate,
      accessionNumber: source.accessionNumber,
    };
  });
}

// A warrant count can be an event rather than warrants outstanding (2.5.9). VRT's 2025 10-K tags
// ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights = 4,812,521 on 2024-12-06, the shares
// issued when its private placement warrants were exercised cashlessly (5,266,667 warrants exercised, tagged
// on the same date and class), in the equity statement; none of those warrants remained at 2025-12-31.
// A count on the equity-statement axis, or with a warrants-exercised count for the same class and date, is
// not read as outstanding. A count dated before the period end is still read (AAOI tags its outstanding
// Amazon warrant only at issuance) and is flagged.
const EQUITY_STATEMENT_AXIS = "StatementEquityComponentsAxis";
const WARRANT_EXERCISED_CONCEPTS = ["ClassOfWarrantOrRightNumberOfWarrantsExercised", "ClassOfWarrantOrRightExercised"];

function dimsWithin(inner: Record<string, string>, outer: Record<string, string>): boolean {
  return Object.entries(inner).every(([axis, member]) => outer[axis] === member);
}

// An exercise of the class: warrants exercised or shares issued on exercise (BE tags Oracle's cashless exercise
// on 2026-05-01 as StockIssuedDuringPeriodSharesExerciseOfWarrants on the warrant's class member).
const WARRANT_EXERCISE_EVENT_RE = /WarrantsExercised|ExerciseOfWarrants/;
const CLASS_OF_WARRANT_AXIS = "ClassOfWarrantOrRightAxis";

/** An exercise of the same warrant class after the count and by the period end: the count no longer describes what is outstanding. */
function laterExercise(doc: IxDocument, f: IxFact): IxFact | null {
  const axis = Object.keys(f.dims).find((a) => a.endsWith(CLASS_OF_WARRANT_AXIS));
  if (!axis || !f.periodEnd) return null;
  const member = f.dims[axis];
  const end = doc.documentPeriodEnd;
  const hits = doc.facts.filter((g) => WARRANT_EXERCISE_EVENT_RE.test(g.local) && g.value != null && g.value > 0 && g.dims[axis] === member
    && g.periodEnd != null && g.periodEnd > f.periodEnd! && (end == null || g.periodEnd <= end));
  return newest(hits);
}

/** Why a tagged warrant count is an exercise or equity movement, not warrants outstanding; null when it is a count. */
function warrantCountEvent(doc: IxDocument, f: IxFact): string | null {
  if (Object.keys(f.dims).some((axis) => axis.endsWith(EQUITY_STATEMENT_AXIS))) return "EQUITY_STATEMENT_MOVEMENT";
  const exercised = doc.facts.some((g) => WARRANT_EXERCISED_CONCEPTS.includes(g.local) && g.value != null && g.periodEnd === f.periodEnd && dimsWithin(g.dims, f.dims));
  if (exercised) return "WARRANT_EXERCISE";
  return laterExercise(doc, f) ? "EXERCISED_AFTER_COUNT" : null;
}

/** Warrant counts not read as outstanding because they record an exercise or an equity movement. */
export function warrantCountEvents(sources: IxSource[]): Record<string, unknown>[] {
  const out = new Map<string, Record<string, unknown>>();
  for (const source of sources) {
    for (const f of source.doc.facts) {
      if (!WARRANT_COUNT_CONCEPTS.includes(f.local) || f.value == null) continue;
      const reason = warrantCountEvent(source.doc, f);
      if (!reason) continue;
      const key = `${f.name}|${dimsKey(f.dims)}|${f.periodEnd}|${f.value}`;
      if (out.has(key)) continue;
      const exercise = reason === "EXERCISED_AFTER_COUNT" ? laterExercise(source.doc, f) : null;
      out.set(key, {
        class: hasDims(f) ? Object.values(f.dims).map(memberLabel).join(" / ") : "Warrants (not itemized)",
        concept: f.name,
        value: f.value,
        asOf: f.periodEnd,
        reason,
        ...(exercise ? { exercise: { concept: exercise.name, value: exercise.value, date: exercise.periodEnd } } : {}),
        filingType: source.filingType,
        accessionNumber: source.accessionNumber,
      });
    }
  }
  return [...out.values()];
}

/** A warrant class's identity across filings: its class-of-warrant member, else its dimensions. */
function warrantClassKey(f: IxFact): string {
  const axis = Object.keys(f.dims).find((a) => a.endsWith(CLASS_OF_WARRANT_AXIS));
  return axis ? `${axis}=${f.dims[axis]}` : dimsKey(f.dims);
}

// ── Warrant lifecycle stated in text (2.5.11) ───────────────────────────────
//
// A warrant count tagged before the period end (an issuance, a prior year end) says nothing of what
// happened since; filers often state an exercise, expiry or redemption only in text. ASTS's 10-Q counts
// 122,000 Private Placement Warrants as of 2025-12-31 and says "the remaining 122,000 Private Placement
// Warrants were exercised" in the quarter ended March 31, 2026; RKLB's 10-K counts 728,835 warrants
// issued on 2023-12-29 and says "On November 14, 2024, all 728,835 common stock warrants were exercised".
// A sentence retires a class only when it names the class (its tagged count, or its class name of two or
// more words), states the whole class exercised, expired or redeemed in the past tense, and dates that
// after the tagged count and by the period end. Anything less leaves the class counted.
export const WARRANT_LIFECYCLE_SEARCH_TERMS = [
  "warrants were exercised", "warrant was exercised", "were fully exercised", "was fully exercised", "exercised in full",
  "warrants expired", "warrant expired", "warrants were redeemed", "redemption of all", "redeemed all",
];
const LIFECYCLE_EVENTS: { event: string; re: RegExp }[] = [
  { event: "EXERCISED", re: /\bwarrants?\b[^.]{0,160}?\b(?:were|was|have been|has been|had been)\s+(?:fully\s+|all\s+)?exercised\b|\bexercised\s+(?:all|the remaining|in full)\b[^.]{0,100}?\bwarrants?\b/i },
  { event: "REDEEMED", re: /\bwarrants?\b[^.]{0,160}?\b(?:were|was|have been|has been|had been)\s+(?:fully\s+)?redeemed\b|\bredeemed\s+(?:all|the remaining)\b[^.]{0,100}?\bwarrants?\b|\bredemption of all\b[^.]{0,100}?\bwarrants?\b/i },
  { event: "EXPIRED", re: /\bwarrants?\b[^.]{0,160}?\b(?:expired|lapsed)\b/i },
];
// A negated, future or conditional sentence states no event ("No Private Placement Warrants were exercised").
// Case-sensitive past the first letter, so the month "May" is not the verb "may".
const LIFECYCLE_SKIP_RE = /\b(?:[Nn]o|[Nn]one|[Nn]ot|[Nn]either|nor|will|would|may|might|could|shall|[Uu]nless|[Ii]f)\b/;
// The whole class: all or the remaining warrants, in full; an expiry or redemption ends every unexercised warrant.
const WHOLE_CLASS_RE = /\b(?:all|remaining|fully|in full|in their entirety|each of the)\b/i;
const TEXT_DATE_RE = new RegExp(TEXT_DATE_SOURCE, "gi");

export type WarrantLifecycleSentence = {
  event: string;
  sentence: string;
  dates: string[];
  sectionHeading: string | null;
  documentUrl: string | null;
  filingDate: string | null;
  accessionNumber: string | null;
};

/** Past-tense exercise, expiry and redemption sentences about warrants, each with the dates it states. */
export function warrantLifecycleSentences(matches: TextMatch[]): WarrantLifecycleSentence[] {
  const out: WarrantLifecycleSentence[] = [];
  const seen = new Set<string>();
  for (const match of matches) {
    for (const raw of collapse(match.contextText).split(CLAIM_SENTENCE_SPLIT_RE)) {
      const sentence = raw.trim();
      if (!sentence || seen.has(sentence) || LIFECYCLE_SKIP_RE.test(sentence)) continue;
      const event = LIFECYCLE_EVENTS.find((e) => e.re.test(sentence))?.event;
      if (!event) continue;
      const dates = [...sentence.matchAll(TEXT_DATE_RE)].map((m) => textDate(m[1], m[2], m[3])).filter((d): d is string => d != null);
      if (dates.length === 0) continue;
      seen.add(sentence);
      out.push({
        event,
        sentence: sentence.slice(0, 600),
        dates,
        sectionHeading: match.sectionHeading,
        documentUrl: match.documentUrl,
        filingDate: match.filingDate,
        accessionNumber: match.accessionNumber,
      });
    }
  }
  return out;
}

/** 728835 -> "728,835". */
export function groupedCount(count: number): string {
  return String(count).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/** "728,835" as a whole number in the text, not part of a longer one. */
function statesCount(sentence: string, count: number): boolean {
  if (!Number.isInteger(count) || count < 1000) return false;
  const text = groupedCount(count);
  return new RegExp(`(?<![\\d,.])${text}(?!\\d|,\\d)`).test(sentence);
}

/** The class-of-warrant member's name when it is specific ("Private Placement Warrants"), never a bare "Warrants". */
function specificClassName(f: IxFact): string | null {
  const axis = Object.keys(f.dims).find((a) => a.endsWith(CLASS_OF_WARRANT_AXIS));
  if (!axis) return null;
  const name = memberLabel(f.dims[axis]).toLowerCase().replace(/\s+/g, " ").trim();
  return name.split(" ").length >= 2 ? name.replace(/s$/, "") : null;
}

/** How a sentence names the class of a tagged count: by the count itself or by the class name; null when it does not. */
function namesClass(sentence: string, f: IxFact): string | null {
  if (f.value != null && statesCount(sentence, f.value)) return "STATED_COUNT";
  const name = specificClassName(f);
  return name && collapse(sentence).toLowerCase().includes(name) ? "CLASS_NAME" : null;
}

/**
 * The stated event that retired a class counted before the period end: a sentence naming the class
 * that states it exercised, expired or redeemed in whole, dated after the count and by the period end.
 * The earliest such event wins (ASTS's warrants were exercised in the first quarter, then expired in April).
 */
function textRetirement(f: IxFact, sentences: WarrantLifecycleSentence[], periodEnd: string | null): Record<string, unknown> | null {
  if (periodEnd == null || f.periodEnd == null || f.periodEnd >= periodEnd) return null;
  let best: Record<string, unknown> | null = null;
  for (const s of sentences) {
    const matchedBy = namesClass(s.sentence, f);
    if (!matchedBy) continue;
    if (s.event === "EXERCISED" && matchedBy !== "STATED_COUNT" && !WHOLE_CLASS_RE.test(s.sentence)) continue;
    // The latest date the sentence states by the period end: "During the three months ended March 31, 2026".
    const dated = s.dates.filter((d) => d <= periodEnd).sort();
    const eventDate = dated[dated.length - 1];
    if (!eventDate || eventDate <= f.periodEnd) continue;
    if (best && String(best.eventDate) <= eventDate) continue;
    best = { event: s.event, eventDate, matchedBy, sentence: s.sentence, sectionHeading: s.sectionHeading, documentUrl: s.documentUrl, filingDate: s.filingDate, accessionNumber: s.accessionNumber };
  }
  return best;
}

/**
 * Counts whose dimensions extend another counted class's (the class and a tranche or holder axis) are
 * parts of it. Parts that add up to the class (within 0.5%) replace it, keeping their own terms; parts
 * that do not ("of which 104,157") are left out and the class total is counted.
 */
function unnestWarrantCounts(facts: IxFact[]): { kept: IxFact[]; nested: Record<string, unknown>[] } {
  const label = (f: IxFact) => (hasDims(f) ? Object.values(f.dims).map(memberLabel).join(" / ") : "Warrants (not itemized)");
  const partsOf = (p: IxFact) => facts.filter((c) => c !== p && hasDims(p) && Object.keys(c.dims).length > Object.keys(p.dims).length && dimsWithin(p.dims, c.dims));
  const dropped = new Map<IxFact, Record<string, unknown>>();
  // Widest classes first, so a part of a part is judged against its own class.
  for (const p of [...facts].sort((a, b) => Object.keys(a.dims).length - Object.keys(b.dims).length)) {
    if (dropped.has(p)) continue;
    const parts = partsOf(p).filter((c) => !dropped.has(c));
    if (parts.length === 0) continue;
    const sum = parts.reduce((t, c) => t + (c.value ?? 0), 0);
    if (p.value != null && p.value > 0 && Math.abs(sum - p.value) <= p.value * 0.005) {
      dropped.set(p, { class: label(p), count: p.value, asOf: p.periodEnd, reason: "SUM_OF_COUNTED_PARTS", parts: parts.map(label) });
    } else {
      for (const c of parts) dropped.set(c, { class: label(c), count: c.value, asOf: c.periodEnd, reason: "PART_OF_COUNTED_CLASS", partOf: label(p) });
    }
  }
  return { kept: facts.filter((f) => !dropped.has(f)), nested: facts.filter((f) => dropped.has(f)).map((f) => dropped.get(f)!) };
}

function warrantsComponent(sources: IxSource[], price: number, lifecycle: WarrantLifecycleSentence[] | null = null): Record<string, unknown> | null {
  // Classes a newer filing shows exercised or retired; an older fallback filing must not restore them
  // (BE's 2025 10-K still counts the Oracle warrant its 2026 10-Q shows exercised).
  const retired = new Set<string>();
  let found: { value: IxFact[]; source: IxSource } | null = null;
  for (const source of sources) {
    const doc = source.doc;
    const counts = doc.facts.filter((f) => f.value != null && WARRANT_COUNT_CONCEPTS.includes(f.local) && warrantCountEvent(doc, f) == null
      && !afterPeriodEnd(doc, f) && !retired.has(warrantClassKey(f)));
    const concept = WARRANT_COUNT_CONCEPTS.find((c) => counts.some((f) => f.local === c));
    const facts = counts.filter((f) => f.local === concept);
    if (facts.length > 0) {
      const groups = new Map<string, IxFact>();
      for (const f of facts) {
        const key = dimsKey(f.dims);
        const prev = groups.get(key);
        if (!prev || (f.periodEnd ?? "") > (prev.periodEnd ?? "")) groups.set(key, f);
      }
      const dimmed = [...groups.entries()].filter(([key]) => key !== "");
      found = { value: dimmed.length > 0 ? dimmed.map(([, f]) => f) : [groups.get("")!], source };
      break;
    }
    for (const f of doc.facts) {
      if (f.value != null && WARRANT_COUNT_CONCEPTS.includes(f.local) && warrantCountEvent(doc, f) != null) retired.add(warrantClassKey(f));
    }
  }
  if (!found) return null;
  const { value: tagged, source } = found;
  // A class tagged both as a total and in parts is counted once (2.5.13): JOBY tags its Delta Warrants
  // (12,833,333) and the two tranches (7,000,000 and 5,833,333); LUNR its 541,667 preferred investor
  // warrants and the 104,157 of them a related party holds.
  const { kept: facts, nested } = unnestWarrantCounts(tagged);
  const unvestedFacts = source.doc.facts.filter((g) => WARRANT_UNVESTED_RE.test(g.local) && isShareCount(g));
  const vestedFacts = source.doc.facts.filter((g) => WARRANT_VESTED_RE.test(g.local) && g.value != null && !afterPeriodEnd(source.doc, g));
  const docEnd = source.doc.documentPeriodEnd;
  const expiryOf = (f: IxFact) => normalizeIxDate(newest(source.doc.facts.filter((g) => WARRANT_EXPIRY_RE.test(g.local) && g.text != null && dimsKey(g.dims) === dimsKey(f.dims)))?.text ?? null);
  const expired = facts.filter((f) => { const e = expiryOf(f); return e != null && docEnd != null && e < docEnd; });
  // A count tagged before the period end that the filing text says was since exercised, expired or redeemed (2.5.11).
  const retiredInText = facts.filter((f) => !expired.includes(f)).map((f) => ({ f, stated: lifecycle ? textRetirement(f, lifecycle, docEnd) : null })).filter((r) => r.stated != null);
  const live = facts.filter((f) => !expired.includes(f) && !retiredInText.some((r) => r.f === f));
  const classes = live.map((f) => {
    const strike = newest(source.doc.facts.filter((g) => g.local === WARRANT_STRIKE && g.value != null && dimsKey(g.dims) === dimsKey(f.dims)));
    const label = hasDims(f) ? Object.values(f.dims).map(memberLabel).join(" / ") : "Warrants (not itemized)";
    // Only vested warrant shares can be exercised now; the rest count in the gross total.
    const vested = newest(vestedFacts.filter((g) => dimsKey(g.dims) === dimsKey(f.dims)));
    const unvested = vested ? null : newest(unvestedFacts.filter((g) => dimsKey(g.dims) === dimsKey(f.dims)))
      ?? (facts.length === 1 ? newest(unvestedFacts) : null);
    const vestingTerm = source.doc.facts.some((g) => g.local === WARRANT_VESTING_TERM && dimsKey(g.dims) === dimsKey(f.dims));
    const exercisable = vested ? Math.min(f.value!, vested.value!)
      : unvested ? Math.max(0, f.value! - unvested.value!)
      : vestingTerm ? null : f.value!;
    const exercisableBasis = vested ? "vested_count_tagged" : unvested ? "outstanding_less_unvested_tagged"
      : vestingTerm ? "vesting_terms_without_vested_count" : "no_vesting_terms_tagged";
    const periodEnd = source.doc.documentPeriodEnd;
    const expiry = expiryOf(f);
    const years = termYears(newest(source.doc.facts.filter((g) => g.local === WARRANT_TERM && dimsKey(g.dims) === dimsKey(f.dims)))?.text ?? null);
    const termEnd = years != null && f.periodEnd ? addYears(f.periodEnd, years) : null;
    return {
      class: label,
      concept: f.name,
      outstanding: f.value,
      asOf: f.periodEnd,
      // Tagged at an earlier date (an issuance) and not restated at the period end.
      countBeforePeriodEnd: periodEnd != null && f.periodEnd != null && f.periodEnd < periodEnd,
      // Whether the filing text was read for this earlier count's exercise, expiry or redemption (2.5.11).
      ...(periodEnd != null && f.periodEnd != null && f.periodEnd < periodEnd ? { lifecycleText: lifecycle ? "NO_EVENT_STATED" : "NOT_READ" } : {}),
      expirationDate: expiry,
      // A term counted from the tagged date that ended before the period end: possibly expired unexercised.
      ...(expiry == null && termEnd != null && periodEnd != null && f.periodEnd! < periodEnd && termEnd < periodEnd ? { termElapsedBy: termEnd } : {}),
      unvested: vested ? Math.max(0, f.value! - vested.value!) : unvested ? unvested.value : null,
      unvestedAsOf: vested ? vested.periodEnd : unvested ? unvested.periodEnd : null,
      exercisable,
      exercisableBasis,
      exercisePrice: strike ? strike.value : null,
      inTheMoney: strike ? price > strike.value! : null,
      // No warrants exercisable adds no shares, whatever the strike; an unknown exercisable count adds an unknown number.
      incrementalShares: exercisable == null ? null : strike ? round(treasuryStock(exercisable, strike.value!, price)) : exercisable === 0 ? 0 : null,
    };
  });
  const unresolved = classes.filter((c) => c.incrementalShares == null).length;
  return {
    component: "warrants",
    outstanding: classes.reduce((sum, c) => sum + (c.outstanding ?? 0), 0),
    classes,
    ...(expired.length > 0 ? { expiredClasses: expired.map((f) => ({ class: hasDims(f) ? Object.values(f.dims).map(memberLabel).join(" / ") : "Warrants (not itemized)", count: f.value, asOf: f.periodEnd, expirationDate: expiryOf(f) })) } : {}),
    ...(retiredInText.length > 0 ? { retiredInText: retiredInText.map(({ f, stated }) => ({ class: hasDims(f) ? Object.values(f.dims).map(memberLabel).join(" / ") : "Warrants (not itemized)", count: f.value, asOf: f.periodEnd, ...stated })) } : {}),
    ...(nested.length > 0 ? { nestedCounts: nested } : {}),
    method: "treasury_stock_per_class_on_vested",
    incrementalShares: classes.length === 0 ? 0 : unresolved === classes.length ? null : classes.reduce((sum, c) => sum + (c.incrementalShares ?? 0), 0),
    unresolvedClasses: unresolved,
    source: sourceRef(source, facts[0].periodEnd),
  };
}

type DebtGroup = { key: string; axis: string | null; member: string | null; facts: IxFact[] };

function debtGroups(doc: IxDocument): DebtGroup[] {
  const groups = new Map<string, DebtGroup>();
  for (const f of doc.facts) {
    const axis = DEBT_AXES.find((a) => f.dims[a] != null) ?? null;
    if (!axis) continue;
    const member = f.dims[axis];
    const key = `${axis}=${member}`;
    let group = groups.get(key);
    if (!group) {
      group = { key, axis, member, facts: [] };
      groups.set(key, group);
    }
    group.facts.push(f);
  }
  const byInstrument = [...groups.values()].filter((g) => g.axis === "DebtInstrumentAxis");
  return byInstrument.length > 0 ? byInstrument : [...groups.values()];
}

function groupValue(group: DebtGroup, locals: string[], at: string | null = null): Picked | null {
  for (const local of locals) {
    const hit = newest(group.facts.filter((f) => f.local === local && f.value != null && (at == null || f.periodEnd === at)));
    if (hit) return picked(hit);
  }
  return null;
}

/** Every distinct text a concept carries on the group's newest date for it, in document order (2.5.24, F-010). */
function groupTexts(group: DebtGroup, local: string): string[] {
  const hits = group.facts.filter((f) => f.local === local && f.text != null);
  const best = newest(hits);
  if (!best) return [];
  return [...new Set(hits.filter((f) => (f.periodEnd ?? "") === (best.periodEnd ?? "")).map((f) => f.text as string))];
}

/** Every distinct value a concept carries on the group's newest date for it. */
function groupValues(group: DebtGroup, local: string): number[] {
  const hits = group.facts.filter((f) => f.local === local && f.value != null);
  const best = newest(hits);
  if (!best) return [];
  return [...new Set(hits.filter((f) => (f.periodEnd ?? "") === (best.periodEnd ?? "")).map((f) => f.value as number))];
}

function groupText(group: DebtGroup, local: string): string | null {
  const hits = group.facts.filter((f) => f.local === local && f.text != null);
  const best = newest(hits);
  return best ? best.text : null;
}

// The filing's own count of shares issuable on conversion at the period end (BE tags the maximum, make-whole
// included, per note), and principal outstanding then: after conversions and repurchases the issue's face
// amount overstates both (BE's 2028 notes: $632.5M issued, $0.787M left at 2026-06-30) (2.5.9).
const SHARES_ISSUABLE_CONCEPTS = ["DebtInstrumentConvertibleNumberOfSharesAvailableForConversion", "DebtInstrumentConvertibleNumberOfEquityInstruments"];
// Principal at the period end: a face amount tagged then, else the instrument's carrying amount when it is well
// below the issue's face (conversions or repurchases), not a balance-sheet line net of discount and costs
// (LongTermDebt $290M against $300M issued is the same notes, net).
const PERIOD_END_FACE_CONCEPTS = ["DebtInstrumentFaceAmount"];
const PERIOD_END_CARRYING_CONCEPTS = ["DebtInstrumentCarryingAmount"];
const REDUCED_PRINCIPAL_SHARE = 0.95;
const INSTRUMENT_AXES_RE = /(?:DebtInstrumentAxis|LongtermDebtTypeAxis)$/;
const SUBSEQUENT_EVENT_AXIS_RE = /SubsequentEventTypeAxis$/;
const REDEMPTION_RE = /Redemption|Redeem|Repurchase|Extinguish|Repaid|Repayment/;
// A tagged conversion ratio that disagrees with the tagged conversion price is not the conversion rate
// (BE tags only the make-whole increase, e.g. 2.6926 against $194.97, whose rate is 5.1290 per $1,000).
const RATIO_PRICE_TOLERANCE = 0.02;

/**
 * A tagged conversion ratio in shares per $1,000 of principal. Some filers tag it per $1 (LITE's
 * 0.0076319 against a $131.03 price is 7.6319 per $1,000): a ratio whose product with the conversion
 * price is ~1 is scaled by 1,000 (RATIO_PER_1_PRINCIPAL_SCALED). One that agrees with neither scale is
 * not the conversion rate (RATIO_INCONSISTENT_WITH_PRICE); without a price, a ratio below 1 has no
 * provable unit (RATIO_UNIT_UNCERTAIN). The tagged value stays visible (2.5.10).
 */
export function normalizedRatio(ratio: number | null, convPrice: number | null): { per1000: number | null; usable: boolean; note: string | null } {
  if (ratio == null || !(ratio > 0)) return { per1000: null, usable: false, note: null };
  if (convPrice != null) {
    if (Math.abs((ratio * convPrice) / 1000 - 1) <= RATIO_PRICE_TOLERANCE) return { per1000: ratio, usable: true, note: null };
    if (Math.abs(ratio * convPrice - 1) <= RATIO_PRICE_TOLERANCE) return { per1000: round(ratio * 1000, 6), usable: true, note: "RATIO_PER_1_PRINCIPAL_SCALED" };
    return { per1000: ratio, usable: false, note: "RATIO_INCONSISTENT_WITH_PRICE" };
  }
  return ratio >= 1 ? { per1000: ratio, usable: true, note: null } : { per1000: null, usable: false, note: "RATIO_UNIT_UNCERTAIN" };
}

/** A value tagged only on the instrument's own axes, at a date. */
function instrumentValue(group: DebtGroup, locals: string[], at: string): Picked | null {
  for (const local of locals) {
    const hit = newest(group.facts.filter((f) => f.local === local && f.value != null && f.periodEnd === at
      && Object.keys(f.dims).every((axis) => INSTRUMENT_AXES_RE.test(axis))));
    if (hit) return picked(hit);
  }
  return null;
}

// ── Principal settled in cash (2.5.11, LITE) ────────────────────────────────
//
// LITE's 10-K: "The principal amounts of all of our outstanding convertible notes must be settled in cash."
// When principal is settled in cash, conversion delivers shares only for the conversion value above
// principal: max(0, if-converted shares - principal / price) at the price. The if-converted count (the
// EPS basis) stays the bridge's count; the net-share count is reported beside it, only for notes whose
// cash settlement of principal the filing states, never assumed. Capped calls are not modeled.
export const CONVERTIBLE_SETTLEMENT_SEARCH_TERMS = [
  "settled in cash", "pay cash up to", "principal amount in cash", "principal amounts of all", "settle the principal", "cash equal to the aggregate principal",
];
const CASH_PRINCIPAL_RE = /\bprincipal(?: amounts?)?\b[^.]{0,160}?\b(?:must|will|shall|is required to|are required to) be (?:settled|paid) (?:solely |only )?in cash\b|\b(?:pay|paying|deliver|delivering) cash (?:equal to|up to) the (?:aggregate )?principal amount\b|\belected to (?:settle|pay) (?:the )?(?:aggregate )?principal(?: amounts?)?\b[^.]{0,80}?\bin cash\b/i;
// A settlement method the issuer may still choose is not a stated cash settlement.
const SETTLEMENT_OPTION_RE = /\bmay\b|\bcan\b|\bat (?:our|its|the company['’]s) (?:option|election)\b/;
const ALL_NOTES_RE = /\ball (?:of )?(?:our |the )?(?:outstanding )?(?:convertible )?(?:senior )?notes\b|\beach (?:series|issue) of (?:our |the )?(?:convertible )?(?:senior )?notes\b/i;
const NOTE_YEAR_RE = /\b(20\d\d) (?:convertible )?(?:senior )?notes\b|\bnotes due (20\d\d)\b/gi;

export type SettlementSentence = { sentence: string; scope: string; years: string[]; sectionHeading: string | null; documentUrl: string | null; filingDate: string | null; accessionNumber: string | null };

/** Sentences stating convertible principal is settled in cash, with the notes they name. */
export function principalCashSettlement(matches: TextMatch[]): SettlementSentence[] {
  const out: SettlementSentence[] = [];
  const seen = new Set<string>();
  for (const match of matches) {
    for (const raw of collapse(match.contextText).split(CLAIM_SENTENCE_SPLIT_RE)) {
      const sentence = raw.trim();
      if (!sentence || seen.has(sentence) || !CASH_PRINCIPAL_RE.test(sentence) || SETTLEMENT_OPTION_RE.test(sentence)) continue;
      seen.add(sentence);
      const years = [...new Set([...sentence.matchAll(NOTE_YEAR_RE)].map((m) => m[1] ?? m[2]))];
      out.push({
        sentence: sentence.slice(0, 600),
        scope: ALL_NOTES_RE.test(sentence) ? "ALL_NOTES" : years.length > 0 ? "NAMED_NOTES" : "UNNAMED_NOTES",
        years,
        sectionHeading: match.sectionHeading,
        documentUrl: match.documentUrl,
        filingDate: match.filingDate,
        accessionNumber: match.accessionNumber,
      });
    }
  }
  return out;
}

/** The sentence that states cash settlement of this instrument's principal: every note, its year, or the only note. */
function settlementFor(instrument: string, count: number, sentences: SettlementSentence[]): SettlementSentence | null {
  return sentences.find((s) => s.scope === "ALL_NOTES")
    ?? sentences.find((s) => s.scope === "NAMED_NOTES" && s.years.some((y) => instrument.includes(y)))
    ?? (count === 1 ? sentences.find((s) => s.scope === "UNNAMED_NOTES") ?? null : null);
}

// ── Capped calls (2.5.12) ───────────────────────────────────────────────────
//
// A capped call bought with a convertible delivers to the company, at settlement, the value of the
// covered shares between the strike and the cap: covered x (min(price, cap) - strike) / price shares at
// the price. BE's 10-K: "The Capped Calls have an initial strike price of approximately $18.85 per share
// … The number of shares underlying the Capped Calls is 33,549,508 shares … The cap price of the Capped
// Calls is initially $26.46 per share", and they "were not impacted by the induced conversion" of the
// notes. The offset is economic, not the EPS count (capped calls are antidilutive and excluded from
// diluted EPS); it is computed only when the strike, the cap and the covered shares are all stated.
// LITE states the cap ($268.24) and that its 2032 capped calls cover the shares that initially underlie
// the 2032 Notes, but no strike: that capped call is reported and left unresolved.
export const CAPPED_CALL_SEARCH_TERMS = ["cap price", "capped call"];
const CAPPED_CALL_RE = /\bcapped calls?\b/i;
const CAP_PRICE_RE = /\bcap price\b[^.$]{0,80}?\$\s?(\d[\d,]*(?:\.\d+)?)/i;
const CC_STRIKE_RE = /\bstrike price\b[^.$]{0,80}?\$\s?(\d[\d,]*(?:\.\d+)?)/i;
const CC_STRIKE_IS_CONVERSION_RE = /\bstrike price\b[^.]{0,120}?\bcorrespond(?:s|ing)? to the (?:initial )?conversion price\b/i;
// "The number of shares underlying the Capped Calls is 33,549,508 shares" (BE); "covering approximately 69.3 million shares" (RKLB).
const CC_COUNT_RE = /\bnumber of shares\b[^.\d]{0,80}?\bunderlying the capped calls?\b\D{0,30}?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)? million)|\bcover(?:s|ing|ed)?\b\D{0,60}?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)? million) shares\b/i;
// The capped calls outlast conversions of their notes: "were not impacted by the induced conversion" (BE).
const CC_SURVIVE_RE = /\bcapped calls?\b[^.]{0,80}?\b(?:were|was|have|has)(?: not been| not)\s+(?:impacted|affected|terminated|unwound|settled)\b|\bcapped calls?\b[^.]{0,80}?\bremain(?:s|ed)? outstanding\b/i;
const CC_UNDERLIE_RE = /\bcover\w*\b[^.]{0,200}?\b(?:initially )?underl(?:ie|ies|ying)\b[^.]{0,60}?\bnotes\b/i;
const CC_YEAR_RE = /\b(20\d\d) capped calls?\b|\bnotes due (?:[A-Z][a-z]+ )?(20\d\d)\b|\b(20\d\d) (?:convertible )?(?:senior )?notes\b/gi;

export type CappedCallTerms = {
  years: string[];
  strikePrice: number | null;
  strikeIsConversionPrice: boolean;
  capPrice: number | null;
  coveredShares: number | null;
  coversNoteShares: boolean;
  survivesConversions: boolean;
  sentences: string[];
  documentUrl: string | null;
  filingDate: string | null;
  accessionNumber: string | null;
};

const dollars = (m: RegExpExecArray | null): number | null => (m ? parseFloat(m[1].replace(/,/g, "")) : null);
/** "33,549,508" -> 33549508; "69.3 million" -> 69300000. */
function sharesStated(text: string): number {
  const m = /^(\d+(?:\.\d+)?) million$/i.exec(text);
  return m ? round(parseFloat(m[1]) * 1_000_000) : parseInt(text.replace(/,/g, ""), 10);
}

/** Capped call terms stated in filing text, merged per set of notes named (by year) in the passage. */
export function cappedCallTerms(matches: TextMatch[]): CappedCallTerms[] {
  const groups = new Map<string, CappedCallTerms>();
  for (const match of matches) {
    const text = collapse(match.contextText);
    if (!CAPPED_CALL_RE.test(text)) continue;
    const years = [...new Set([...text.matchAll(CC_YEAR_RE)].map((m) => m[1] ?? m[2] ?? m[3]))].sort();
    const key = years.join("+");
    let g = groups.get(key);
    if (!g) {
      g = { years, strikePrice: null, strikeIsConversionPrice: false, capPrice: null, coveredShares: null, coversNoteShares: false, survivesConversions: false, sentences: [], documentUrl: match.documentUrl, filingDate: match.filingDate, accessionNumber: match.accessionNumber };
      groups.set(key, g);
    }
    // Each term is read from one sentence about the capped calls (or the sentence after one that names them).
    const list = text.split(CLAIM_SENTENCE_SPLIT_RE).map((x) => x.trim());
    const about = list.filter((x, i) => CAPPED_CALL_RE.test(x) || (i > 0 && CAPPED_CALL_RE.test(list[i - 1])));
    const facts: [RegExp, (m: RegExpExecArray) => void][] = [
      [CC_STRIKE_RE, (m) => { g!.strikePrice ??= dollars(m); }],
      [CC_STRIKE_IS_CONVERSION_RE, () => { g!.strikeIsConversionPrice = true; }],
      [CAP_PRICE_RE, (m) => { g!.capPrice ??= dollars(m); }],
      [CC_COUNT_RE, (m) => { g!.coveredShares ??= sharesStated(m[1] ?? m[2]); }],
      [CC_UNDERLIE_RE, () => { g!.coversNoteShares = true; }],
      [CC_SURVIVE_RE, () => { g!.survivesConversions = true; }],
    ];
    for (const [re, apply] of facts) {
      for (const sentence of about) {
        const m = re.exec(sentence);
        if (!m) continue;
        apply(m);
        // The sentence that states it, quoted once.
        if (!g.sentences.includes(sentence) && g.sentences.length < 5) g.sentences.push(sentence.slice(0, 600));
        break;
      }
    }
  }
  return [...groups.values()].filter((g) => g.capPrice != null || g.strikePrice != null || g.coveredShares != null || g.coversNoteShares);
}

/** The capped call terms for a note: the passage naming its year, else the only passage when there is one note. */
function cappedCallFor(instrument: string, count: number, terms: CappedCallTerms[]): CappedCallTerms | null {
  return terms.find((t) => t.years.length === 1 && instrument.includes(t.years[0]))
    ?? (count === 1 ? terms.find((t) => t.years.length === 0) ?? null : null);
}

/** A note's capped call at the price: its stated terms and the shares its value between strike and cap offsets. */
function cappedCallAt(t: CappedCallTerms, note: Record<string, unknown>, price: number): Record<string, unknown> {
  const conversion = typeof note.conversionPrice === "number" ? note.conversionPrice : null;
  const strike = t.strikePrice ?? (t.strikeIsConversionPrice ? conversion : null);
  const strikeBasis = t.strikePrice != null ? "STATED" : strike != null ? "STATED_AS_CONVERSION_PRICE" : "NOT_STATED";
  const noteShares = typeof note.ifConvertedShares === "number" ? note.ifConvertedShares : null;
  // A stated count above the notes' shares today (issue-date coverage after conversions) is used only when the
  // filing says the capped calls outlasted those conversions (BE); otherwise the notes' shares bound it (RKLB).
  const statedExceeds = t.coveredShares != null && noteShares != null && t.coveredShares > noteShares * 1.01;
  const covered = t.coveredShares != null && !(statedExceeds && !t.survivesConversions) ? t.coveredShares
    : (t.coversNoteShares || statedExceeds) ? noteShares : null;
  const coverageBasis = covered == null ? "NOT_STATED"
    : covered === t.coveredShares ? (statedExceeds ? "STATED_COUNT_SURVIVES_CONVERSIONS" : "STATED_COUNT")
    : statedExceeds ? "SHARES_UNDERLYING_NOTES_AT_PERIOD_END_BELOW_STATED_COUNT" : "SHARES_UNDERLYING_NOTES_AT_PERIOD_END";
  // Stable enums: unresolvedReason is CAPPED_CALL_TERMS_INCOMPLETE and missingTerms names each term not stated.
  const missing = [strike == null ? "STRIKE_PRICE" : null, t.capPrice == null ? "CAP_PRICE" : null, covered == null ? "COVERED_SHARES" : null].filter((x): x is string => x != null);
  const offset = missing.length > 0 ? null : round((covered! * Math.max(0, Math.min(price, t.capPrice!) - strike!)) / price);
  return {
    strikePrice: strike,
    strikeBasis,
    capPrice: t.capPrice,
    coveredShares: covered,
    coverageBasis,
    statedCoveredShares: t.coveredShares,
    offsetSharesAtPrice: offset,
    ...(missing.length > 0 ? { unresolvedReason: "CAPPED_CALL_TERMS_INCOMPLETE", missingTerms: missing } : {}),
    sentences: t.sentences,
    documentUrl: t.documentUrl,
    filingDate: t.filingDate,
    accessionNumber: t.accessionNumber,
  };
}

function convertiblesComponent(sources: IxSource[], price: number, settlement: SettlementSentence[] | null = null, cappedCalls: CappedCallTerms[] | null = null): Record<string, unknown> | null {
  const found = findInSources(sources, (doc) => {
    const groups = debtGroups(doc).filter((g) => groupValue(g, [CONVERSION_PRICE, CONVERSION_RATIO]) != null);
    if (groups.length > 0) return groups;
    const plainPrice = total(doc, CONVERSION_PRICE) ?? total(doc, CONVERSION_RATIO);
    if (!plainPrice) return null;
    return [{ key: "", axis: null, member: null, facts: doc.facts.filter((f) => !hasDims(f)) }];
  });
  if (!found) return null;
  const { value: groups, source } = found;
  const end = source.doc.documentPeriodEnd;
  const instruments = groups.map((g) => {
    const faceTagged = groupValue(g, [FACE_AMOUNT]);
    const faceAtEnd = end && g.member ? instrumentValue(g, PERIOD_END_FACE_CONCEPTS, end) : null;
    const carryingAtEnd = end && g.member && !faceAtEnd ? instrumentValue(g, PERIOD_END_CARRYING_CONCEPTS, end) : null;
    const current = faceAtEnd
      ?? (carryingAtEnd && (faceTagged == null || carryingAtEnd.value < REDUCED_PRINCIPAL_SHARE * faceTagged.value) ? carryingAtEnd : null);
    // Some issuers tag each issue's principal only under a carrying-amount
    // concept, often at the issue date; use it and say so.
    const face = current ?? faceTagged ?? groupValue(g, PRINCIPAL_FALLBACK_CONCEPTS);
    const issuable = end && g.member ? instrumentValue(g, SHARES_ISSUABLE_CONCEPTS, end) : null;
    const convPrice = groupValue(g, [CONVERSION_PRICE]);
    const ratio = groupValue(g, [CONVERSION_RATIO]);
    const norm = normalizedRatio(ratio ? ratio.value : null, convPrice ? convPrice.value : null);
    const usableRatio = norm.usable ? { ...ratio!, value: norm.per1000! } : null;
    const impliedPrice = convPrice ? convPrice.value : (usableRatio ? 1000 / usableRatio.value : null);
    let shares: number | null = null;
    let basis: string | null = null;
    if (issuable) {
      shares = issuable.value;
      basis = "shares_issuable_tagged_at_period_end";
    } else if (face && usableRatio) {
      shares = (face.value / 1000) * usableRatio.value;
      basis = "principal / 1000 * conversion_ratio";
    } else if (face && convPrice && convPrice.value > 0) {
      shares = face.value / convPrice.value;
      basis = "principal / conversion_price";
    }
    const redemption = newest(g.facts.filter((f) => REDEMPTION_RE.test(f.local) && Object.keys(f.dims).some((axis) => SUBSEQUENT_EVENT_AXIS_RE.test(axis))));
    const inTheMoney = impliedPrice != null ? price >= impliedPrice : null;
    return {
      instrument: g.member ? memberLabel(g.member) : "Convertible notes (not itemized)",
      member: g.member,
      faceAmount: faceTagged ? faceTagged.value : null,
      principal: face ? face.value : null,
      principalConcept: face ? face.concept : null,
      principalDate: face ? face.periodEnd : null,
      principalBasis: current ? "outstanding_at_period_end" : faceTagged ? "face_amount" : (face ? "tagged_amount_fallback" : null),
      conversionPrice: convPrice ? convPrice.value : (impliedPrice != null ? round(impliedPrice, 4) : null),
      conversionPriceBasis: convPrice ? "tagged" : (impliedPrice != null ? "1000 / conversion_ratio" : null),
      conversionRatioPer1000: norm.per1000,
      conversionRatioTagged: ratio ? ratio.value : null,
      conversionRatioUsed: usableRatio != null && !issuable,
      ...(norm.note ? { conversionRatioNote: norm.note } : {}),
      maturityDate: normalizeIxDate(groupText(g, "DebtInstrumentMaturityDate")),
      ifConvertedShares: shares != null ? round(shares) : null,
      ifConvertedBasis: basis,
      sharesIssuableDate: issuable ? issuable.periodEnd : null,
      ...(redemption ? { afterPeriodEnd: { concept: redemption.name, date: redemption.periodEnd } } : {}),
      inTheMoney,
      incrementalShares: shares != null && inTheMoney ? round(shares) : (shares != null ? 0 : null),
    };
  });
  // Stated cash settlement of principal: the shares for the conversion value above principal at the price (2.5.11).
  const settled = instruments.map((i) => {
    let out: Record<string, unknown> = i;
    if (settlement != null) {
      const stated = settlementFor(String(i.instrument), instruments.length, settlement);
      const nss = !stated || i.ifConvertedShares == null ? null
        : !i.inTheMoney ? 0
        : i.principal != null ? round(Math.max(0, i.ifConvertedShares - i.principal / price)) : null;
      out = {
        ...out,
        principalSettlement: stated ? { stated: "PRINCIPAL_IN_CASH", scope: stated.scope, sentence: stated.sentence, sectionHeading: stated.sectionHeading, documentUrl: stated.documentUrl, filingDate: stated.filingDate, accessionNumber: stated.accessionNumber } : null,
        netShareSettlementShares: nss,
      };
    }
    // Capped call terms the filing states for this note (2.5.12).
    if (cappedCalls != null) {
      const terms = cappedCallFor(String(i.instrument), instruments.length, cappedCalls);
      out = { ...out, cappedCall: terms ? cappedCallAt(terms, i, price) : null };
    }
    return out as typeof i & Record<string, unknown>;
  });
  const calls = settled.map((i) => (i as Record<string, unknown>).cappedCall as Record<string, unknown> | null | undefined).filter((c): c is Record<string, unknown> => c != null);
  const resolvedCalls = calls.filter((c) => typeof c.offsetSharesAtPrice === "number");
  const unresolved = settled.filter((i) => i.ifConvertedShares == null).length;
  const cashSettled = settled.filter((i) => (i as Record<string, unknown>).principalSettlement != null);
  const nssUnresolved = cashSettled.some((i) => (i as Record<string, unknown>).netShareSettlementShares == null);
  return {
    component: "convertible_debt",
    instruments: settled,
    method: "if_converted_when_in_the_money",
    ifConvertedShares: settled.reduce((sum, i) => sum + (i.ifConvertedShares ?? 0), 0),
    incrementalShares: unresolved === settled.length ? null : settled.reduce((sum, i) => sum + (i.incrementalShares ?? 0), 0),
    unresolvedInstruments: unresolved,
    settlementText: settlement == null ? "NOT_READ" : "READ",
    // The same count with each note whose principal the filing says is settled in cash at its net shares; null when none is.
    incrementalSharesNetShareSettlement: cashSettled.length === 0 || unresolved === settled.length || nssUnresolved ? null
      : settled.reduce((sum, i) => sum + Number((i as Record<string, unknown>).principalSettlement != null ? (i as Record<string, unknown>).netShareSettlementShares : (i.incrementalShares ?? 0)), 0),
    cappedCallText: cappedCalls == null ? "NOT_READ" : "READ",
    // Shares the stated capped calls deliver back at the price; null when none is fully stated (2.5.12).
    cappedCallOffsetShares: resolvedCalls.length === 0 ? null : resolvedCalls.reduce((sum, c) => sum + Number(c.offsetSharesAtPrice), 0),
    cappedCallsUnresolved: calls.length - resolvedCalls.length,
    note: "Shares are the filing's count issuable on conversion at the period end when tagged (it can be the maximum, make-whole included); else principal outstanding at the period end (a face amount then, or a carrying amount well below the issue's face), else the face amount or an issue-date carrying amount (principalBasis says which), times a conversion ratio that agrees with the conversion price, else divided by the price. Where the filing states principal is settled in cash, netShareSettlementShares is the conversion value above principal in shares at the price. Where the filing states a capped call's strike, cap and covered shares, cappedCall.offsetSharesAtPrice is covered x (min(price, cap) - strike) / price: an economic offset, not part of the EPS count.",
    source: sourceRef(source, null),
  };
}

// ── Convertible preferred stock (2.5.9, MRVL) ───────────────────────────────

const PREFERRED_SHARES_ISSUABLE = "PreferredStockConvertibleSharesIssuable";
const PREFERRED_CONVERSION_PRICE = "PreferredStockConvertibleConversionPrice";
const PREFERRED_LIQUIDATION_AGGREGATE = ["PreferredStockLiquidationPreferenceValue", "TemporaryEquityLiquidationPreference"];
const PREFERRED_LIQUIDATION_PER_SHARE = ["PreferredStockLiquidationPreference", "TemporaryEquityLiquidationPreferencePerShare"];
const PREFERRED_DIVIDEND_RATE = ["PreferredStockDividendRatePercentage"];

/**
 * Convertible preferred outstanding at the period end, if-converted when in the money: the tagged
 * common shares issuable on conversion and the conversion price. MRVL's Series A (2.0M preferred
 * shares, issued to NVIDIA) converts into up to 21.8M common shares at $91.84.
 */
// A preferred conversion the filing states in words (2.5.10; more wordings in 2.5.11): a per-share
// ratio ("The Preferred Stock will convert on a one-for-one basis into shares of our common stock",
// LITE; "each share of Preferred Stock is convertible into 10 shares of common stock") or an aggregate
// ("convertible in the aggregate into a maximum of approximately 21.8 million shares of our common stock").
const PREF_ONE_FOR_ONE_RE = /\bpreferred stock\b[^.]{0,120}\bconvert(?:s|ible)?\b[^.]{0,40}\bon a (?:one[- ](?:for|to)[- ]one|1[- ]for[- ]1|1:1) basis\b/i;
// "each share of Series A Preferred Stock is convertible, at the option of the holder, into 10 shares of common stock" (2.5.11: text between).
const PREF_PER_SHARE_RE = /\beach share of (?:the |our )?(?:series [a-z0-9-]+ )?(?:convertible )?preferred stock\b[^.]{0,80}\bconvertible\b[^.]{0,60}?\binto ([0-9][0-9,]*(?:\.[0-9]+)?) shares of (?:our |the company['’]s )?(?:class [a-z] )?common stock\b/i;
// "a conversion rate of 10 shares of common stock for each share of Series A Preferred Stock" (2.5.11).
const PREF_RATE_RE = /\bconversion (?:rate|ratio) of ([0-9][0-9,]*(?:\.[0-9]+)?) shares of (?:our |the company['’]s )?(?:class [a-z] )?common stock (?:for each|per) share of (?:the |our )?(?:series [a-z0-9-]+ )?(?:convertible )?preferred stock\b/i;
const PREF_AGGREGATE_RE = /\bpreferred stock\b[^.]{0,120}\bconvertible (?:in the aggregate into|into an aggregate of) (?:a maximum of |up to )?(?:approximately )?([0-9][0-9,]*(?:\.[0-9]+)?)( million)? shares of (?:our |the company['’]s )?(?:class [a-z] )?common stock\b/i;

export type PreferredConversionText = { ratio: number | null; aggregateShares: number | null; approximate: boolean; sentence: string; documentUrl: string | null };

/** The first stated preferred conversion in filing text, quoted. */
export function preferredConversionTerms(matches: TextMatch[]): PreferredConversionText | null {
  for (const match of matches) {
    for (const sentence of collapse(match.contextText).split(CLAIM_SENTENCE_SPLIT_RE).map((x) => x.trim())) {
      const one = PREF_ONE_FOR_ONE_RE.test(sentence);
      const per = PREF_PER_SHARE_RE.exec(sentence) ?? PREF_RATE_RE.exec(sentence);
      const agg = PREF_AGGREGATE_RE.exec(sentence);
      if (!one && !per && !agg) continue;
      const aggregate = agg ? parseFloat(agg[1].replace(/,/g, "")) * (agg[2] ? 1_000_000 : 1) : null;
      return {
        ratio: one ? 1 : per ? parseFloat(per[1].replace(/,/g, "")) : null,
        aggregateShares: aggregate != null ? round(aggregate) : null,
        approximate: agg != null && /approximately/i.test(agg[0]),
        sentence: sentence.slice(0, 600),
        documentUrl: match.documentUrl,
      };
    }
  }
  return null;
}

function convertiblePreferredComponent(sources: IxSource[], price: number, stated: PreferredConversionText | null = null): Record<string, unknown> | null {
  const found = findInSources(sources, (doc) => {
    const end = doc.documentPeriodEnd;
    if (!end) return null;
    const outstanding = doc.facts.filter((f) => PREFERRED_OUTSTANDING_CONCEPTS.includes(f.local) && f.value != null && f.periodEnd === end
      && !Object.keys(f.dims).some((axis) => /StatementEquityComponentsAxis$/.test(axis)));
    // The same shares can be tagged under both concepts (LITE: 2.9M preferred and 2.9M temporary
    // equity): the larger concept count, never their sum.
    const perConcept = PREFERRED_OUTSTANDING_CONCEPTS.map((c) => {
      const facts = outstanding.filter((f) => f.local === c);
      const plain = facts.filter((f) => !hasDims(f));
      return (plain.length > 0 ? plain : facts).reduce((sum, f) => sum + f.value!, 0);
    });
    const shares = Math.max(...perConcept);
    if (!(shares > 0)) return null;
    const issuable = newest(doc.facts.filter((f) => f.local === PREFERRED_SHARES_ISSUABLE && f.value != null && !afterPeriodEnd(doc, f)));
    const conv = newest(doc.facts.filter((f) => f.local === PREFERRED_CONVERSION_PRICE && f.value != null && !afterPeriodEnd(doc, f)));
    if (!issuable && !conv && !stated) return null;
    // Preferred economics as tagged (2.5.10): the liquidation preference, aggregate or per share, and the dividend rate.
    const tagged = (locals: string[]) => newest(doc.facts.filter((f) => locals.includes(f.local) && f.value != null && !afterPeriodEnd(doc, f)
      && !Object.keys(f.dims).some((axis) => /StatementEquityComponentsAxis$/.test(axis))));
    const liqAggregate = tagged(PREFERRED_LIQUIDATION_AGGREGATE);
    const liqPerShare = tagged(PREFERRED_LIQUIDATION_PER_SHARE);
    const dividend = tagged(PREFERRED_DIVIDEND_RATE);
    return { shares, asOf: end, issuable, conv, liqAggregate, liqPerShare, dividend };
  });
  if (!found) return null;
  const { value: v, source } = found;
  const convPrice = v.conv ? v.conv.value! : null;
  // Tags first; else the conversion the filing states in words (quoted in statedConversion).
  const fromText = !v.issuable && stated != null;
  const ifConverted = v.issuable ? v.issuable.value!
    : stated?.ratio != null ? round(v.shares * stated.ratio)
    : stated?.aggregateShares ?? null;
  const basis = v.issuable ? "shares_issuable_tagged"
    : stated?.ratio != null ? "ratio_stated_in_text"
    : stated?.aggregateShares != null ? "aggregate_stated_in_text" : null;
  // A per-share ratio with no conversion price converts without payment: the preferred is
  // common-equivalent at any price (LITE's one-for-one Series A participates as converted).
  const asConverted = convPrice == null && basis === "ratio_stated_in_text";
  const inTheMoney = convPrice != null ? price >= convPrice : null;
  const instrument = {
    instrument: "Convertible preferred stock",
    preferredSharesOutstanding: v.shares,
    asOf: v.asOf,
    ifConvertedShares: ifConverted,
    ifConvertedBasis: basis,
    sharesIssuableDate: v.issuable ? v.issuable.periodEnd : null,
    // The issuable count was tagged at issuance and not restated at the period end.
    countBeforePeriodEnd: v.issuable != null && v.issuable.periodEnd != null && v.issuable.periodEnd < v.asOf,
    conversionPrice: convPrice,
    inTheMoney,
    incrementalShares: ifConverted == null ? null : asConverted ? round(ifConverted) : inTheMoney == null ? null : inTheMoney ? round(ifConverted) : 0,
    liquidationPreference: v.liqAggregate ? { amount: v.liqAggregate.value, basis: "aggregate_tagged", asOf: v.liqAggregate.periodEnd }
      : v.liqPerShare ? { amount: round(v.liqPerShare.value! * v.shares, 2), basis: "per_share_tagged_x_shares_outstanding", perShare: v.liqPerShare.value, asOf: v.liqPerShare.periodEnd }
      : null,
    dividendRatePct: v.dividend ? round(v.dividend.value! * 100, 4) : null,
    ...(fromText ? { statedConversion: { ratio: stated!.ratio, aggregateShares: stated!.aggregateShares, approximate: stated!.approximate, sentence: stated!.sentence, documentUrl: stated!.documentUrl } } : {}),
    ...(asConverted ? { method: "as_converted_no_conversion_price" } : {}),
  };
  return {
    component: "convertible_preferred",
    instruments: [instrument],
    method: "if_converted_when_in_the_money",
    ifConvertedShares: ifConverted ?? 0,
    incrementalShares: instrument.incrementalShares,
    unresolvedInstruments: instrument.incrementalShares == null ? 1 : 0,
    note: "Common shares issuable on conversion as tagged (often the maximum at issuance) and the tagged conversion price; if-converted when the price is at or above it. The liquidation preference and dividend rate are reported as tagged; redemption is not modeled.",
    source: sourceRef(source, v.asOf),
  };
}

// ── Text evidence (ATM programs, funding statements) ────────────────────────

export type TextMatch = {
  contextText: string;
  sectionHeading: string | null;
  documentUrl: string | null;
  filingDate: string | null;
  accessionNumber: string | null;
  inTable?: boolean;
  tableTitle?: string | null;
  rowLabel?: string | null;
};

const ATM_RE = /\bat[- ]the[- ]market\b|\bATM (?:program|offering|facility|agreement)\b|\b(?:equity distribution|open market sale|controlled equity offering|sales) agreement\b/i;
const MONEY_RE = /(?:US)?\$\s?(\d[\d,]*(?:\.\d+)?)\s*(billion|million|thousand|bn|mm|m|k)?\b/gi;

export function moneyValue(amount: string, unit: string | undefined): number {
  const base = parseFloat(amount.replace(/,/g, ""));
  const u = (unit ?? "").toLowerCase();
  if (u === "billion" || u === "bn") return base * 1e9;
  if (u === "million" || u === "mm" || u === "m") return base * 1e6;
  if (u === "thousand" || u === "k") return base * 1e3;
  return base;
}

export function sentences(text: string): string[] {
  return text.split(/(?<=[.!?])\s+(?=[A-Z(\u201c"])/).map((s) => s.trim()).filter(Boolean);
}

function classifyAtmClause(clause: string): string {
  if (/\bremain|\bavailable (?:for|to be)\b|\bunsold\b|\byet to be sold\b|\bunused\b/i.test(clause)) return "remaining_capacity";
  if (/\bsold\b|\bissued\b|\bproceeds\b/i.test(clause)) return "sold_to_date";
  if (/\bup to\b|\baggregate (?:offering|gross|sales) price\b|\baggregate of\b/i.test(clause)) return "program_size";
  return "other";
}

/** ATM program amounts stated in filing text, each with the sentence it came from. */
export function atmEvidence(matches: TextMatch[]): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  for (const match of matches) {
    for (const sentence of sentences(collapse(match.contextText))) {
      if (!ATM_RE.test(sentence)) continue;
      for (const clause of sentence.split(/[;,]\s+/)) {
        for (const money of clause.matchAll(MONEY_RE)) {
          const amountUsd = moneyValue(money[1], money[2]);
          if (!(amountUsd >= 100000)) continue;
          const kind = classifyAtmClause(clause);
          const key = `${kind}|${amountUsd}|${sentence}`;
          if (seen.has(key)) continue;
          seen.add(key);
          out.push({
            kind,
            amountUsd,
            sentence: sentence.slice(0, 600),
            sectionHeading: match.sectionHeading,
            documentUrl: match.documentUrl,
            filingDate: match.filingDate,
          });
        }
      }
    }
  }
  return out;
}

function atmComponent(evidence: Record<string, unknown>[], price: number): Record<string, unknown> | null {
  if (evidence.length === 0) return null;
  const remaining = evidence.find((e) => e.kind === "remaining_capacity");
  const size = evidence.find((e) => e.kind === "program_size");
  const out: Record<string, unknown> = {
    component: "atm_program",
    remainingCapacityUsd: remaining ? remaining.amountUsd : null,
    programSizeUsd: size ? size.amountUsd : null,
    evidence: evidence.slice(0, 6),
  };
  if (remaining) {
    out.method = "remaining_capacity / price";
    out.potentialShares = round((remaining.amountUsd as number) / price);
    out.note = "Shares if the whole remaining capacity were sold at the supplied price. Issuance is at the company's discretion; this is capacity, not a plan.";
  } else {
    out.method = "not_computed";
    out.potentialShares = null;
    out.note = "The filing states the program size but not the unsold remainder, so no share count is derived.";
  }
  return out;
}

// ── Untagged share claims (2.5.9, COHR) ─────────────────────────────────────

// Claims on the equity that the bridge's tagged components do not model: a
// price-protection or anti-dilution right granted with a share sale, a forward
// sale, shares issuable as contingent consideration, and convertible preferred
// stock. They are read from filing text, quoted, and never quantified.
export const SHARE_CLAIM_SEARCH_TERMS = [
  "price protection", "anti-dilution", "antidilution", "forward sale agreement", "earnout shares", "earn-out shares",
  "contingent consideration", "contingently issuable", "convertible preferred",
  "subsequent to quarter end", "subsequent to year end", "subsequent to the end of the quarter",
  "one-for-one basis", "convertible in the aggregate",
  "exchangeable for shares", "exchangeable into shares", "redeemable for shares of", "simple agreement for future equity",
  "payable in shares", "settled in shares", "standby equity purchase agreement", "equity line of credit", "committed equity facility",
  // 2.5.11: holdback and milestone shares, share-settled CVRs, issuance commitments, more preferred conversion wordings.
  "holdback shares", "escrow shares", "milestone shares", "contingent value right", "obligated to issue", "committed to issue", "required to issue",
  "one-to-one basis", "shares of common stock for each share of", "convertible into an aggregate of",
];
const SHARE_CLAIM_KINDS: { kind: string; re: RegExp }[] = [
  { kind: "PRICE_PROTECTION", re: /\bprice[- ]protection\b/i },
  { kind: "ANTI_DILUTION_RIGHT", re: /\banti-?dilution (?:rights?|protections?|provisions?)\b/i },
  { kind: "FORWARD_SALE", re: /\bforward (?:sale|equity sale) agreements?\b/i },
  { kind: "CONTINGENT_SHARES", re: /\bearn-?out shares\b|\bcontingently issuable (?:shares|common stock)\b|\bcontingent consideration\b[^.]{0,120}\b(?:in|of) (?:shares|common stock)\b|\b(?:holdback|escrow(?:ed)?|milestone) shares\b|\bshares (?:held|placed) in escrow\b/i },
  { kind: "CONVERTIBLE_PREFERRED", re: /\bconvertible preferred (?:stock|shares)\b/i },
  // Up-C units or exchangeable shares (2.5.10).
  { kind: "EXCHANGEABLE_INTERESTS", re: /\b(?:units?|interests?|shares|stock)\b[^.]{0,100}\b(?:exchangeable|redeemable) (?:for|into) (?:an equal number of )?(?:newly[- ]issued )?(?:shares of )?(?:our |the company['’]s )?(?:class [a-z] )?common stock\b/i },
  { kind: "SAFE", re: /\bsimple agreements? for future equity\b|\bSAFEs?\b/ },
  { kind: "SHARE_SETTLED_OBLIGATION", re: /\b(?:payable|settled|settleable|issuable)\b[^.]{0,30}\bin (?:shares of )?(?:our |the company['’]s )?(?:class [a-z] )?common stock\b/i },
  // Contingent value rights settled in shares, and commitments to issue shares (2.5.11).
  { kind: "CONTINGENT_VALUE_RIGHT", re: /\bcontingent value rights?\b/i },
  { kind: "SHARE_ISSUANCE_COMMITMENT", re: /\b(?:obligated|committed|required) to issue\b[^.]{0,80}\b(?:shares|common stock)\b/i },
  { kind: "EQUITY_LINE", re: /\b(?:standby equity purchase agreement|equity line of credit|committed equity facility|equity purchase facility)\b/i },
  { kind: "WARRANT_AFTER_PERIOD_END", re: /\bsubsequent to (?:the )?(?:quarter|year|period)[- ]end\b[^.]{0,200}\bwarrants?\b|\bsubsequent to the end of the (?:quarter|year|period)\b[^.]{0,200}\bwarrants?\b/i },
];
// A customer or distributor price-protection term is a revenue reduction, not a
// claim on shares (COHR's variable-consideration policy).
const REVENUE_TERM_RE = /\b(?:distributors?|customers?|revenues?|variable consideration|product returns?|sales price|price reductions?|inventor(?:y|ies)|rebates?)\b/i;
// Anti-dilution adjustments of a warrant's or note's own terms belong to an
// instrument the bridge already reads.
const INSTRUMENT_TERM_RE = /\b(?:warrants?|convertible (?:senior )?notes?|conversion (?:rate|price)|exercise price|debentures?)\b/i;
const AWARD_OR_NOTE_RE = /\b(?:restricted stock|RSUs?|PSUs?|awards?|stock options?|employees?|compensation|ESPP|dividend reinvestment|convertible|notes|debentures?|warrants?)\b/i;
const PRESENT_OR_FUTURE_RE = /\b(?:is|are|may|will|would|can|could|remain|remains)\b/i;
const EQUITY_TERM_RE = /\b(?:shares?|stock|equity|securities purchase agreement|purchase agreement|investors?|stockholders?|shareholders?)\b/i;
const EXTINGUISHED_RE = /\bwere (?:all )?converted\b|\bno shares of\b[^.]{0,120}\b(?:are|were|remain)\b[^.]{0,30}\boutstanding\b|\b(?:was|were) redeemed in full\b|\b(?:expired|terminated) (?:unexercised|without)\b/i;
// Sentence breaks, except after an initialism such as "U.S." ("applicable U.S. GAAP").
const CLAIM_SENTENCE_SPLIT_RE = /(?<![A-Z]\.[A-Z]\.)(?<=[.!?])\s+(?=[A-Z(\u201c"])/;
const PREFERRED_OUTSTANDING_CONCEPTS = ["PreferredStockSharesOutstanding", "TemporaryEquitySharesOutstanding"];

/**
 * Share claims stated in filing text that no tagged component covers, one per
 * kind and document, each with up to four quoted sentences and the lead-in
 * before the first. Every claim is UNQUANTIFIED; text saying an instrument was
 * converted or redeemed is flagged (extinguishmentStated) but never closes the
 * claim, since the sentence can describe another series.
 */
export function shareClaimSignals(matches: TextMatch[]): Record<string, unknown>[] {
  const groups = new Map<string, Record<string, unknown>>();
  const seen = new Set<string>();
  for (const match of matches) {
    const list = collapse(match.contextText).split(CLAIM_SENTENCE_SPLIT_RE).map((x) => x.trim()).filter(Boolean);
    // A context window cuts its first and last sentences mid-way.
    if (list.length > 0 && !/^[A-Z(\u201c"]/.test(list[0])) list.shift();
    if (list.length > 0 && !/[.!?\u201d")]$/.test(list[list.length - 1])) list.pop();
    list.forEach((sentence, i) => {
      const kind = SHARE_CLAIM_KINDS.find((k) => k.re.test(sentence))?.kind;
      if (!kind || seen.has(`${kind}|${sentence}`)) return;
      if (kind === "PRICE_PROTECTION" && REVENUE_TERM_RE.test(sentence)) return;
      if (kind === "ANTI_DILUTION_RIGHT" && INSTRUMENT_TERM_RE.test(sentence)) return;
      // Awards, notes and dividends settled in shares belong elsewhere; a claim is present or future, not past.
      if ((kind === "SHARE_SETTLED_OBLIGATION" || kind === "EXCHANGEABLE_INTERESTS" || kind === "SHARE_ISSUANCE_COMMITMENT") && (AWARD_OR_NOTE_RE.test(sentence) || !PRESENT_OR_FUTURE_RE.test(sentence))) return;
      // A contingent value right is a share claim only when the sentence says it can be paid in shares.
      if (kind === "CONTINGENT_VALUE_RIGHT" && !/\b(?:shares|common stock)\b/i.test(sentence)) return;
      const leadIn = list.slice(Math.max(0, i - 2), i).join(" ");
      if (!EQUITY_TERM_RE.test(`${leadIn} ${sentence}`)) return;
      seen.add(`${kind}|${sentence}`);
      const key = `${kind}|${match.documentUrl ?? ""}`;
      let group = groups.get(key);
      if (!group) {
        group = {
          kind,
          status: "UNQUANTIFIED",
          sentences: [] as string[],
          leadIn: leadIn.slice(-600) || null,
          // The sentence after the first quoted one ("The warrant is eligible for vesting ...").
          leadOut: i + 1 < list.length ? list[i + 1].slice(0, 600) : null,
          extinguishmentStated: false,
          sectionHeading: match.sectionHeading,
          documentUrl: match.documentUrl,
          filingDate: match.filingDate,
          accessionNumber: match.accessionNumber,
        };
        groups.set(key, group);
      }
      const quoted = group.sentences as string[];
      if (quoted.length < 4) quoted.push(sentence.slice(0, 600));
      if (EXTINGUISHED_RE.test(sentence)) group.extinguishmentStated = true;
    });
  }
  return [...groups.values()];
}

/**
 * Convertible preferred claims checked against the preferred and temporary
 * equity share counts tagged at a filing's period end: all zero closes the
 * claim (TAGGED_NONE_OUTSTANDING); a positive count keeps it open and quotes it.
 */
function preferredClaimsWithTags(claims: Record<string, unknown>[], sources: IxSource[], component: Record<string, unknown> | null = null): Record<string, unknown>[] {
  let tagged: IxFact[] | null = null;
  for (const s of sources) {
    const end = s.doc.documentPeriodEnd;
    const facts = s.doc.facts.filter((f) => PREFERRED_OUTSTANDING_CONCEPTS.includes(f.local) && f.value != null && end != null && f.periodEnd === end);
    if (facts.length > 0) {
      tagged = facts;
      break;
    }
  }
  return claims.map((c) => {
    if (c.kind !== "CONVERTIBLE_PREFERRED" || tagged == null) return c;
    const rows = tagged.map((f) => ({ concept: f.name, class: Object.values(f.dims)[0] ? memberLabel(Object.values(f.dims)[0]) : null, shares: f.value, asOf: f.periodEnd }));
    // Outstanding and resolved by the convertible_preferred component: counted, not open (2.5.9).
    const status = tagged.every((f) => f.value === 0) ? "TAGGED_NONE_OUTSTANDING"
      : component && component.incrementalShares != null ? "MODELED_IN_BRIDGE" : "UNQUANTIFIED";
    return { ...c, status, taggedOutstanding: rows };
  });
}

/**
 * Post-period warrants from the tags, each quoting the filing text's subsequent-event warrant
 * sentences when the scan found them (which then stop being a separate claim).
 */
function withPostPeriodWarrants(claims: Record<string, unknown>[], tagged: Record<string, unknown>[]): Record<string, unknown>[] {
  if (tagged.length === 0) return claims;
  const text = claims.find((c) => c.kind === "WARRANT_AFTER_PERIOD_END");
  const rest = claims.filter((c) => c !== text);
  const merged = tagged.map((t, i) => (text && i === 0 ? { ...t, sentences: text.sentences, leadIn: text.leadIn, leadOut: text.leadOut, sectionHeading: text.sectionHeading } : t));
  return [...rest, ...merged];
}

const ANTIDILUTIVE = "AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount";
const DILUTION_CONCEPT_RE = /Warrant|Option|Nonvested|RestrictedStock|Convertible|Antidilutive|EarningsPerShare|SharesOutstanding/i;

/** Facts over the shortest period ending at the latest end date: the quarter in a 10-Q, the year in a 10-K. */
function latestPeriodFacts(facts: IxFact[]): IxFact[] {
  const end = facts.reduce((best, f) => ((f.periodEnd ?? "") > best ? (f.periodEnd ?? "") : best), "");
  const atEnd = facts.filter((f) => (f.periodEnd ?? "") === end);
  const start = atEnd.reduce((best, f) => ((f.periodStart ?? "") > best ? (f.periodStart ?? "") : best), "");
  return atEnd.filter((f) => (f.periodStart ?? "") === start);
}

/** The company's own EPS share counts and the securities it excluded as antidilutive. */
function reportedEpsDilution(doc: IxDocument): Record<string, unknown> | null {
  const plain = (local: string) => latestPeriodFacts(doc.facts.filter((f) => f.local === local && f.value != null && !hasDims(f)))[0] ?? null;
  const basic = plain("WeightedAverageNumberOfSharesOutstandingBasic");
  const diluted = plain("WeightedAverageNumberOfDilutedSharesOutstanding");
  // Itemized on AntidilutiveSecuritiesAxis, else on whatever single axis the filer used.
  const antidilutive = doc.facts.filter((f) => f.local === ANTIDILUTIVE && f.value != null);
  const onStandardAxis = antidilutive.filter((f) => f.dims.AntidilutiveSecuritiesAxis != null);
  const excluded = latestPeriodFacts(onStandardAxis.length > 0 ? onStandardAxis : antidilutive.filter((f) => Object.keys(f.dims).length === 1));
  if (!basic && !diluted && excluded.length === 0) return null;
  const period = basic ?? diluted ?? excluded[0];
  return {
    periodStart: period.periodStart,
    periodEnd: period.periodEnd,
    weightedBasicShares: basic ? basic.value : null,
    weightedDilutedShares: diluted ? diluted.value : null,
    antidilutiveExcluded: excluded.map((f) => ({ security: memberLabel(f.dims.AntidilutiveSecuritiesAxis ?? Object.values(f.dims)[0]), shares: f.value })),
    note: "The company's own weighted-average EPS counts for the period, and the securities it left out as antidilutive. A cross-check on the bridge's instrument list, not a point-in-time count.",
  };
}

/** Share-count concepts the filing tags, with their axes: "concept [Axis, ...]". */
function taggedDilutionConcepts(sources: IxSource[]): string[] {
  const out: string[] = [];
  for (const s of sources) {
    for (const f of s.doc.facts) {
      if (!isShareCount(f) || !(DILUTION_CONCEPT_RE.test(f.local) || /Nonoption|Unvested|Vested/i.test(f.local))) continue;
      const axes = Object.keys(f.dims).sort();
      const key = axes.length > 0 ? `${f.name} [${axes.join(", ")}]` : f.name;
      if (!out.includes(key)) out.push(key);
    }
  }
  return out.sort().slice(0, 60);
}

// ── The company's antidilutive-securities table (2.5.13) ────────────────────
//
// The EPS note lists every class of potentially dilutive security the company left out of diluted EPS,
// with its own share count for the period. Each row is matched to a bridge component by its label. A row
// no component models becomes a share claim with the company's count (REPORTED_NOT_MODELED): ASTS's
// Class B and Class C common stock exchangeable for Class A (89.4M), LUNR's escrow shares, SOUN's
// contingently issuable shares, RKLB's collared forward transactions. The table's warrant total is also
// set against the bridge's (LUNR reports 4,857,302; the bridge counts 541,667). The counts can be
// weighted averages over the period, so they are evidence beside the bridge, never added into it.
const ANTIDILUTIVE_CATEGORIES: { category: string; component: string | null; kind: string; re: RegExp }[] = [
  { category: "warrants", component: "warrants", kind: "WARRANTS_NOT_TAGGED", re: /warrant/i },
  { category: "convertible_preferred", component: "convertible_preferred", kind: "CONVERTIBLE_PREFERRED", re: /preferred/i },
  { category: "convertible_debt", component: "convertible_debt", kind: "CONVERTIBLE_DEBT_NOT_TAGGED", re: /convertible|\bnotes?\b|debentures?/i },
  { category: "forward_sale", component: null, kind: "FORWARD_SALE", re: /forward/i },
  { category: "contingent_shares", component: null, kind: "CONTINGENT_SHARES", re: /contingent|earn-?out|escrow|holdback|milestone/i },
  { category: "stock_options", component: "stock_options", kind: "EQUITY_COMPENSATION_NOT_TAGGED", re: /option/i },
  { category: "share_awards", component: "unvested_share_awards", kind: "EQUITY_COMPENSATION_NOT_TAGGED", re: /restricted|\bRSUs?\b|\bPSUs?\b|stock units?|share units?|award|performance|unvested|nonvested/i },
  { category: "equity_compensation", component: "equity_compensation", kind: "EQUITY_COMPENSATION_NOT_TAGGED", re: /compensation|incentive|equity plan|employee stock|\bESPP\b|purchase plan/i },
  { category: "exchangeable_interests", component: null, kind: "EXCHANGEABLE_INTERESTS", re: /\bunits?\b|class [a-z]\b|common stock|common shares|exchangeable|noncontrolling|\bLLC\b|partnership/i },
];
const EQUITY_COMPENSATION_COMPONENTS = ["stock_options", "unvested_share_awards"];

/** The antidilutive rows matched to components, the claims no component models, and the warrant totals set side by side. */
function antidilutiveReconciliation(eps: Record<string, unknown> | null, components: Record<string, unknown>[], source: IxSource | null): { reconciliation: Record<string, unknown> | null; claims: Record<string, unknown>[]; warrants: { reported: number; bridge: number | null } | null } {
  const rows = ((eps?.antidilutiveExcluded ?? []) as Record<string, unknown>[]).filter((r) => typeof r.shares === "number" && (r.shares as number) > 0);
  if (!eps || rows.length === 0) return { reconciliation: null, claims: [], warrants: null };
  const present = new Set(components.map((c) => String(c.component)));
  const modeled = (component: string | null) => component != null && (component === "equity_compensation" ? EQUITY_COMPENSATION_COMPONENTS.some((c) => present.has(c)) : present.has(component)
    || (EQUITY_COMPENSATION_COMPONENTS.includes(component) && EQUITY_COMPENSATION_COMPONENTS.some((c) => present.has(c))));
  const matched = rows.map((r) => {
    const cat = ANTIDILUTIVE_CATEGORIES.find((c) => c.re.test(String(r.security)));
    return { security: r.security, shares: r.shares, category: cat?.category ?? "other", modeledBy: cat && modeled(cat.component) ? cat.component : null, kind: cat?.kind ?? "OTHER_REPORTED_SECURITY" };
  });
  const groups = new Map<string, typeof matched>();
  for (const m of matched.filter((m) => m.modeledBy == null)) groups.set(m.kind, [...(groups.get(m.kind) ?? []), m]);
  const claims = [...groups.entries()].map(([kind, list]) => ({
    kind,
    status: "REPORTED_NOT_MODELED",
    reportedShares: list.reduce((t, m) => t + Number(m.shares), 0),
    reportedSecurities: list.map((m) => ({ security: m.security, shares: m.shares })),
    reportedPeriod: { start: eps.periodStart ?? null, end: eps.periodEnd ?? null },
    evidence: "ANTIDILUTIVE_TABLE",
    sentences: [] as string[],
    leadIn: null,
    leadOut: null,
    documentUrl: source?.documentUrl ?? null,
    filingDate: source?.filingDate ?? null,
    accessionNumber: source?.accessionNumber ?? null,
  }));
  const reportedWarrants = matched.filter((m) => m.category === "warrants").reduce((t, m) => t + Number(m.shares), 0);
  const bridgeWarrants = components.find((c) => c.component === "warrants");
  return {
    reconciliation: {
      periodStart: eps.periodStart ?? null,
      periodEnd: eps.periodEnd ?? null,
      rows: matched.map(({ kind: _kind, ...m }) => m),
      note: "The company's own antidilutive-securities counts for the period (possibly weighted averages), matched to bridge components by label. A row no component models is listed in unquantifiedShareClaims with the company's count; nothing here is added to the bridge.",
    },
    claims,
    warrants: reportedWarrants > 0 && bridgeWarrants ? { reported: reportedWarrants, bridge: typeof bridgeWarrants.outstanding === "number" ? bridgeWarrants.outstanding : null } : null,
  };
}

/** Text claims of a kind the antidilutive table also reports carry its count; the table's other rows are their own claims. */
function mergeReportedClaims(claims: Record<string, unknown>[], reported: Record<string, unknown>[]): Record<string, unknown>[] {
  const out = claims.map((c) => ({ ...c }));
  for (const r of reported) {
    const text = out.find((c) => c.kind === r.kind && c.status === "UNQUANTIFIED");
    if (text) Object.assign(text, { status: "REPORTED_NOT_MODELED", reportedShares: r.reportedShares, reportedSecurities: r.reportedSecurities, reportedPeriod: r.reportedPeriod });
    else out.push(r);
  }
  return out;
}

/** A claim the bridge leaves out: quoted only in text, or reported with a count no component models. */
export function isOpenClaim(c: Record<string, unknown>): boolean {
  return c.status === "UNQUANTIFIED" || c.status === "REPORTED_NOT_MODELED";
}

export type DilutionInput = {
  ticker: string;
  price: number;
  priceCurrency: string;
  asOfDate: string | null;
  sources: IxSource[];
  atmMatches: TextMatch[];
  awardTableMatches?: TextMatch[];
  // Filing text read for untagged share claims; null when it was not read.
  claimMatches?: TextMatch[] | null;
  // Filing text read for the exercise, expiry or redemption of warrants counted before the period end (2.5.11).
  warrantLifecycleMatches?: TextMatch[] | null;  // Filing text read for a stated cash settlement of convertible principal (2.5.11).
  convertibleSettlementMatches?: TextMatch[] | null;
  // Filing text read for capped call terms (2.5.12).
  cappedCallMatches?: TextMatch[] | null;
};

/** Basic to diluted shares at a supplied price, from company disclosures only. */
export function dilutionBridge(input: DilutionInput): Record<string, unknown> {
  const { price, sources } = input;
  const primary = sources[0];
  const basicFound = findInSources(sources, basicShares);
  const options = optionsComponent(sources, price);
  const awards = awardsComponent(sources, input.awardTableMatches ?? []);
  const warrants = warrantsComponent(sources, price, Array.isArray(input.warrantLifecycleMatches) ? warrantLifecycleSentences(input.warrantLifecycleMatches) : null);
  const convertibles = convertiblesComponent(sources, price, Array.isArray(input.convertibleSettlementMatches) ? principalCashSettlement(input.convertibleSettlementMatches) : null,
    Array.isArray(input.cappedCallMatches) ? cappedCallTerms(input.cappedCallMatches) : null);
  const preferred = convertiblePreferredComponent(sources, price, Array.isArray(input.claimMatches) ? preferredConversionTerms(input.claimMatches) : null);
  const atm = atmComponent(atmEvidence(input.atmMatches), price);
  const components = [options, awards, warrants, convertibles, preferred].filter((c): c is Record<string, unknown> => c != null);
  const notDisclosed = [
    options ? null : "stock_options",
    awards ? null : "unvested_share_awards",
    warrants ? null : "warrants",
    convertibles ? null : "convertible_debt",
    preferred ? null : "convertible_preferred",
    atm ? null : "atm_program",
  ].filter((c): c is string => c != null);
  const unresolved = components.filter((c) => c.incrementalShares == null).map((c) => c.component as string);
  const partial = components
    .filter((c) => c.incrementalShares != null && Number(c.unresolvedClasses ?? c.unresolvedInstruments ?? 0) > 0)
    .map((c) => c.component as string);
  const warnings: Record<string, unknown>[] = [];
  const currencyUnits = new Set<string>();
  for (const c of components) {
    const unit = c.strikeUnit;
    if (typeof unit === "string" && unit) currencyUnits.add(unit.split("/")[0]);
  }
  for (const unit of currencyUnits) {
    if (unit !== input.priceCurrency) {
      warnings.push({ code: "PRICE_CURRENCY_MISMATCH", message: `Strikes are reported in ${unit}; the supplied price is treated as ${input.priceCurrency}.`, severity: "warning" });
    }
  }
  const warrantEvents = warrantCountEvents(sources);
  if (warrantEvents.length > 0) {
    warnings.push({
      code: "WARRANT_EXERCISE_NOT_OUTSTANDING",
      message: `${warrantEvents.length} tagged warrant count(s) record an exercise or an equity-statement movement, or predate an exercise of the same class, so they are not counted as warrants outstanding: ${warrantEvents.map((w) => `${w.class} ${w.value} on ${w.asOf}${w.exercise ? ` (exercised: ${(w.exercise as Record<string, unknown>).value} on ${(w.exercise as Record<string, unknown>).date})` : ""}`).join("; ")}.`,
      severity: "info",
      counts: warrantEvents,
    });
  }
  const notes = (convertibles?.instruments ?? []) as Record<string, unknown>[];
  const faceOnly = notes.filter((i) => i.ifConvertedBasis !== "shares_issuable_tagged_at_period_end" && i.principalBasis !== "outstanding_at_period_end" && i.principal != null);
  if (faceOnly.length > 0) {
    warnings.push({ code: "CONVERTIBLE_PRINCIPAL_NOT_AT_PERIOD_END", message: `No principal or shares issuable is tagged at the period end for ${faceOnly.map((i) => i.instrument).join(", ")}; the ${faceOnly.map((i) => `${i.principalBasis} ${i.principal} (${i.principalDate})`).join(", ")} is used and can include notes since converted or repurchased.`, severity: "warning" });
  }
  const badRatios = notes.filter((i) => i.conversionRatioNote === "RATIO_INCONSISTENT_WITH_PRICE");
  if (badRatios.length > 0) {
    warnings.push({ code: "CONVERSION_RATIO_INCONSISTENT", message: `The tagged conversion ratio of ${badRatios.map((i) => `${i.instrument} (${i.conversionRatioPer1000} per 1,000 against a $${i.conversionPrice} price)`).join(", ")} is not the conversion rate (often a make-whole increase) and is not used.`, severity: "info" });
  }
  const cashPrincipal = notes.filter((i) => i.principalSettlement != null);
  if (cashPrincipal.length > 0) {
    warnings.push({
      code: "CONVERTIBLE_PRINCIPAL_SETTLED_IN_CASH",
      message: `The filing states the principal of ${cashPrincipal.map((i) => i.instrument).join(", ")} is settled in cash, so conversion delivers shares only for the value above principal. dilutedSharesAtPrice counts them if-converted (the EPS basis); dilutedSharesAtPriceNetShareSettlement counts ${cashPrincipal.map((i) => `${i.netShareSettlementShares ?? "unresolved"} for ${i.instrument}`).join(", ")} instead.`,
      severity: "info",
    });
  }
  const capped = notes.filter((i) => i.cappedCall != null).map((i) => ({ instrument: i.instrument, call: i.cappedCall as Record<string, unknown> }));
  const cappedResolved = capped.filter((c) => typeof c.call.offsetSharesAtPrice === "number");
  if (cappedResolved.length > 0) {
    warnings.push({
      code: "CAPPED_CALL_OFFSET",
      message: `The filing states capped call terms for ${cappedResolved.map((c) => `${c.instrument} (strike $${c.call.strikePrice}, cap $${c.call.capPrice}, ${c.call.coveredShares} shares: ${c.call.offsetSharesAtPrice} shares back at the price)`).join("; ")}. dilutedSharesAtPrice does not net them (diluted EPS excludes capped calls as antidilutive); dilutedSharesAtPriceNetOfCappedCalls does.`,
      severity: "info",
    });
  }
  const cappedOpen = capped.filter((c) => typeof c.call.offsetSharesAtPrice !== "number");
  if (cappedOpen.length > 0) {
    warnings.push({ code: "CAPPED_CALL_TERMS_INCOMPLETE", message: `The filing describes capped calls for ${cappedOpen.map((c) => `${c.instrument} (not stated: ${(c.call.missingTerms as string[]).join(", ")})`).join("; ")}; no offset is computed for them.`, severity: "info" });
  }
  const redeemed = notes.filter((i) => i.afterPeriodEnd != null);
  if (redeemed.length > 0) {
    warnings.push({ code: "CONVERTIBLE_REDEMPTION_AFTER_PERIOD_END", message: `A redemption or repurchase after the period end is tagged for ${redeemed.map((i) => `${i.instrument} (${(i.afterPeriodEnd as Record<string, unknown>).date})`).join(", ")}; its shares are counted as of the period end.`, severity: "warning" });
  }
  const nestedCounts = (warrants?.nestedCounts ?? []) as Record<string, unknown>[];
  if (nestedCounts.length > 0) {
    warnings.push({
      code: "WARRANT_NESTED_COUNT",
      message: `${nestedCounts.map((c) => (c.reason === "SUM_OF_COUNTED_PARTS" ? `${c.class} (${c.count}) is counted through its parts ${(c.parts as string[]).join(", ")}` : `${c.class} (${c.count}) is part of ${c.partOf} and not counted again`)).join("; ")}.`,
      severity: "info",
    });
  }
  const retiredText = (warrants?.retiredInText ?? []) as Record<string, unknown>[];
  if (retiredText.length > 0) {
    warnings.push({
      code: "WARRANT_RETIRED_IN_TEXT",
      message: `${retiredText.map((c) => `${c.class} (${c.count} as of ${c.asOf}: ${String(c.event).toLowerCase()} by ${c.eventDate})`).join("; ")}: the filing text states the class was exercised, expired or redeemed after its tagged count, so it is not counted; the sentence is quoted in retiredInText.`,
      severity: "info",
    });
  }
  const earlyCounts = ((warrants?.classes ?? []) as Record<string, unknown>[]).filter((c) => c.countBeforePeriodEnd === true);
  if (earlyCounts.length > 0) {
    const read = earlyCounts.every((c) => c.lifecycleText === "NO_EVENT_STATED");
    warnings.push({
      code: "WARRANT_COUNT_BEFORE_PERIOD_END",
      message: `${earlyCounts.length} warrant class(es) are counted from a figure tagged before the report's period end and not restated at it; ${read ? "the filing text was read and states no exercise, expiry or redemption of them, which does not prove they remain outstanding" : "the filing text should confirm they remain outstanding"}: ${earlyCounts.map((c) => `${c.class} ${c.outstanding} as of ${c.asOf}`).join("; ")}.`,
      severity: "warning",
    });
  }
  const claimsRead = Array.isArray(input.claimMatches);
  const epsFound = findInSources(sources, reportedEpsDilution);
  const antidilutive = antidilutiveReconciliation(epsFound?.value ?? null, components, epsFound?.source ?? null);
  const claims = mergeReportedClaims(withPostPeriodWarrants(
    claimsRead ? preferredClaimsWithTags(shareClaimSignals(input.claimMatches as TextMatch[]), sources, preferred) : [],
    postPeriodWarrants(primary),
  ), antidilutive.claims);
  const openClaims = claims.filter(isOpenClaim);
  if (openClaims.length > 0) {
    warnings.push({
      code: "UNQUANTIFIED_SHARE_CLAIMS",
      message: `The filing states ${openClaims.length} share claim(s) no tagged component covers (${openClaims.map((c) => (typeof c.reportedShares === "number" ? `${c.kind}: ${c.reportedShares} reported` : c.kind)).join(", ")}); they are listed in unquantifiedShareClaims and are not in any share count.`,
      severity: "warning",
    });
  }
  // The company's warrant total against the bridge's: a stale, missing or double count shows as a gap (2.5.13).
  const aw = antidilutive.warrants;
  if (aw && aw.bridge != null && Math.abs(aw.reported - aw.bridge) > Math.max(aw.reported * 0.05, 100_000)) {
    warnings.push({
      code: "WARRANT_COUNT_DIFFERS_FROM_REPORTED",
      message: `The bridge counts ${aw.bridge} warrant shares; the company's antidilutive-securities table reports ${aw.reported} for the period ${epsFound?.value.periodStart} to ${epsFound?.value.periodEnd}. The table can be a weighted average, but a gap this size usually means a class is stale, untagged or tagged only in part; check the warrant note.`,
      severity: "warning",
    });
  }
  const expiredClasses = (warrants?.expiredClasses ?? []) as Record<string, unknown>[];
  if (expiredClasses.length > 0) {
    warnings.push({ code: "WARRANT_EXPIRED_BEFORE_PERIOD_END", message: `${expiredClasses.map((c) => `${c.class} (${c.count}, expired ${c.expirationDate})`).join("; ")} expired before the period end by their tagged expiration date and are not counted.`, severity: "info" });
  }
  const elapsed = ((warrants?.classes ?? []) as Record<string, unknown>[]).filter((c) => c.termElapsedBy != null);
  if (elapsed.length > 0) {
    warnings.push({ code: "WARRANT_TERM_ELAPSED", message: `${elapsed.map((c) => `${c.class} (${c.outstanding} as of ${c.asOf}, term through ${c.termElapsedBy})`).join("; ")}: the tagged term, counted from the tagged date, ended before the period end. They may have expired unexercised; they are still counted until the filing says otherwise.`, severity: "warning" });
  }
  const vestingUnknown = ((warrants?.classes ?? []) as Record<string, unknown>[]).filter((c) => c.exercisableBasis === "vesting_terms_without_vested_count");
  if (vestingUnknown.length > 0) {
    warnings.push({ code: "WARRANT_VESTING_NOT_TAGGED", message: `${vestingUnknown.map((c) => c.class).join(", ")} vest on conditions, and no vested or unvested count is tagged; their exercisable shares are unresolved, not assumed to be all ${vestingUnknown.map((c) => c.outstanding).join(", ")}.`, severity: "warning" });
  }
  if (!claimsRead) {
    warnings.push({ code: "SHARE_CLAIM_TEXT_NOT_READ", message: "The filing text was not read for untagged share claims (the kinds in claimCoverage.textScanKinds); retry.", severity: "warning" });
  }
  if (sources.every((s) => s.doc.facts.length === 0)) {
    warnings.push({ code: "NO_INLINE_XBRL", message: "The filing carries no inline XBRL facts; the bridge needs tagged share and instrument counts.", severity: "warning" });
  }

  const out: Record<string, unknown> = {
    ticker: input.ticker,
    basis: "MECHANICAL_COMPANY_DISCLOSED",
    decisionUse: "MECHANICAL_NOT_CONSENSUS",
    price: { amount: price, currency: input.priceCurrency, asOfDate: input.asOfDate, source: "caller_supplied" },
    sources: sources.map((s) => ({ role: s.role, ...sourceRef(s, s.doc.documentPeriodEnd), factCount: s.doc.facts.length })),
    basicShares: basicFound ? { ...basicFound.value, source: sourceRef(basicFound.source, basicFound.value.asOf as string | null) } : null,
    components,
    atmProgram: atm,
    reportedEpsDilution: epsFound?.value ?? null,
    antidilutiveReconciliation: antidilutive.reconciliation,
    notDisclosed,
    unresolved,
    partiallyResolved: partial,
    unquantifiedShareClaims: claims,
    claimCoverage: {
      scope: "TAGGED_INSTRUMENTS",
      completeClaimInventory: false,
      modeledComponents: ["stock_options", "unvested_share_awards", "warrants", "convertible_debt", "convertible_preferred", "atm_program"],
      textScan: claimsRead ? "READ" : "NOT_READ",
      // The company's antidilutive-securities table, read from the inline XBRL (2.5.13).
      antidilutiveTable: antidilutive.reconciliation ? "READ" : "NOT_TAGGED",
      textScanKinds: SHARE_CLAIM_KINDS.map((k) => k.kind),
      unquantifiedClaims: openClaims.length,
      note: "status describes the tagged instruments only. COMPUTED means every tagged instrument was resolved, never that every claim on the equity was found; the text scan covers a fixed list of claim kinds, and a claim it does not find is not proof none exists. The company's antidilutive-securities table adds any class of potentially dilutive security it reports that no component models, with its count.",
    },
  };
  if (!basicFound) {
    out.status = "NOT_FOUND";
    out.bridge = null;
    warnings.push({ code: "BASIC_SHARES_NOT_FOUND", message: "No cover-page or balance-sheet share count was tagged.", severity: "error" });
  } else {
    const basic = basicFound.value.shares as number;
    const inc = (c: Record<string, unknown> | null) => (c && typeof c.incrementalShares === "number" ? c.incrementalShares : 0);
    const diluted = basic + inc(options) + inc(awards) + inc(warrants) + inc(convertibles) + inc(preferred);
    const gross = basic
      + (options ? Number(options.outstanding ?? 0) : 0)
      + (awards ? Number(awards.unvested ?? 0) : 0)
      + (warrants ? Number(warrants.outstanding ?? 0) : 0)
      + (convertibles ? Number(convertibles.ifConvertedShares ?? 0) : 0)
      + (preferred ? Number(preferred.ifConvertedShares ?? 0) : 0);
    const atmShares = atm && typeof atm.potentialShares === "number" ? atm.potentialShares : null;
    out.bridge = {
      basicShares: basic,
      stockOptions: options ? options.incrementalShares : null,
      unvestedShareAwards: awards ? awards.incrementalShares : null,
      warrants: warrants ? warrants.incrementalShares : null,
      convertibleDebt: convertibles ? convertibles.incrementalShares : null,
      convertiblePreferred: preferred ? preferred.incrementalShares : null,
      dilutedSharesAtPrice: round(diluted),
      dilutionPctAtPrice: basic > 0 ? round(((diluted - basic) / basic) * 100, 2) : null,
      grossSharesAllInstruments: round(gross),
      grossDilutionPct: basic > 0 ? round(((gross - basic) / basic) * 100, 2) : null,
      // Convertibles whose principal the filing says is settled in cash at their net shares; null when none is (2.5.11).
      convertibleDebtNetShareSettlement: convertibles && convertibles.incrementalSharesNetShareSettlement != null ? convertibles.incrementalSharesNetShareSettlement : null,
      dilutedSharesAtPriceNetShareSettlement: convertibles && typeof convertibles.incrementalSharesNetShareSettlement === "number" ? round(diluted - inc(convertibles) + convertibles.incrementalSharesNetShareSettlement) : null,
      // Stated capped calls netted at the price: an economic view beside the EPS count (2.5.12).
      cappedCallOffsetShares: convertibles && typeof convertibles.cappedCallOffsetShares === "number" ? convertibles.cappedCallOffsetShares : null,
      dilutedSharesAtPriceNetOfCappedCalls: convertibles && typeof convertibles.cappedCallOffsetShares === "number" ? round(diluted - convertibles.cappedCallOffsetShares) : null,
      dilutedSharesAtPriceNetShareSettlementNetOfCappedCalls: convertibles && typeof convertibles.cappedCallOffsetShares === "number" && typeof convertibles.incrementalSharesNetShareSettlement === "number"
        ? round(diluted - inc(convertibles) + convertibles.incrementalSharesNetShareSettlement - convertibles.cappedCallOffsetShares) : null,
      atmPotentialShares: atmShares,
      dilutedSharesAtPriceWithAtm: atmShares != null ? round(diluted + atmShares) : null,
      formula: "basic + options (treasury stock) + unvested awards (gross) + warrants (treasury stock, vested) + convertibles and convertible preferred (if-converted when in the money)",
    };
    // A share claim the filing states but no tagged component covers leaves the count partial (2.5.9).
    out.status = unresolved.length > 0 || partial.length > 0 || openClaims.length > 0 ? "PARTIAL" : "COMPUTED";
  }
  out.methodology = [
    "Every count is a company disclosure tagged in the filing's inline XBRL; the only external input is the price you supplied.",
    "This is a mechanical bridge, not a consensus or forecast diluted share count, and must not be back-solved into one.",
    "A component missing from notDisclosed was not tagged in the filing; that is not proof the instrument does not exist.",
    "COMPUTED covers the tagged instruments only; it is not a full claim inventory. Share claims found in the filing text are quoted in unquantifiedShareClaims and left out of every count.",
  ];
  // Convertible preferred is rare; its absence alone does not call for the concept list.
  if (notDisclosed.some((c) => c !== "convertible_preferred") || unresolved.length > 0) {
    // What the filing does tag, so a missing component can be traced to a concept this bridge does not read.
    out.taggedDilutionConcepts = taggedDilutionConcepts(sources);
  }
  out.warnings = warnings;
  if (primary && primary.doc.documentPeriodEnd) out.periodEnd = primary.doc.documentPeriodEnd;
  return out;
}

// ── Capital structure timeline ──────────────────────────────────────────────

const CASH_CONCEPTS = ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents", "Cash"];
// A short-term investments total when tagged; otherwise the current securities
// categories, which are disjoint, are added. VRT tags only its held-to-maturity
// Treasury bills (2.4.3).
const SHORT_TERM_INVESTMENT_TOTALS = ["ShortTermInvestments", "MarketableSecuritiesCurrent"];
const SHORT_TERM_INVESTMENT_PARTS = [
  ["AvailableForSaleSecuritiesDebtSecuritiesCurrent"],
  ["DebtSecuritiesHeldToMaturityAmortizedCostAfterAllowanceForCreditLossCurrent", "HeldToMaturitySecuritiesCurrent", "DebtSecuritiesHeldToMaturityExcludingAccruedInterestAfterAllowanceForCreditLossCurrent"],
  ["OtherShortTermInvestments"],
];
const SHORT_TERM_BORROWING_CONCEPTS = ["ShortTermBorrowings", "CommercialPaper"];
const DEBT_LINE_CONCEPTS = [
  "ConvertibleNotesPayableCurrent", "ConvertibleLongTermNotesPayable", "LongTermNotesPayable", "NotesPayableCurrent",
  "SeniorLongTermNotes", "LineOfCredit", "LoansPayableCurrent", "LongTermLoansPayable", "OtherLongTermDebtNoncurrent",
];
const CARRYING_CONCEPTS = ["LongTermDebt", "DebtInstrumentCarryingAmount", "LongTermDebtNoncurrent", "ConvertibleNotesPayable", "ConvertibleLongTermNotesPayable", "SeniorNotes", "NotesPayable"];
const LADDER: [string, string, number][] = [
  ["LongTermDebtMaturitiesRepaymentsOfPrincipalRemainderOfFiscalYear", "remainder_of_fiscal_year", 0],
  ["LongTermDebtMaturitiesRepaymentsOfPrincipalInNextTwelveMonths", "next_12_months", 1],
  ["LongTermDebtMaturitiesRepaymentsOfPrincipalInYearTwo", "year_2", 2],
  ["LongTermDebtMaturitiesRepaymentsOfPrincipalInYearThree", "year_3", 3],
  ["LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFour", "year_4", 4],
  ["LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFive", "year_5", 5],
  ["LongTermDebtMaturitiesRepaymentsOfPrincipalAfterYearFive", "after_year_5", 6],
];

const CONVERTIBLE_TOTAL_CONCEPTS = ["ConvertibleNotesPayable", "ConvertibleDebt"];
const CONVERTIBLE_PART_CONCEPTS = ["ConvertibleNotesPayableCurrent", "ConvertibleLongTermNotesPayable", "ConvertibleDebtCurrent", "ConvertibleDebtNoncurrent"];

/** Convertible notes tagged as balance-sheet lines at a date: a total, else current + noncurrent. */
function convertibleBalance(doc: IxDocument, at: string | null): { total: number; parts: Picked[] } | null {
  const whole = firstTotal(doc, CONVERTIBLE_TOTAL_CONCEPTS, at);
  if (whole) return { total: whole.value, parts: [whole] };
  const parts = CONVERTIBLE_PART_CONCEPTS.map((c) => total(doc, c, at)).filter((p): p is Picked => p != null);
  return parts.length > 0 ? { total: parts.reduce((sum, p) => sum + p.value, 0), parts } : null;
}

/** Short-term investments at a date: the tagged total, else the sum of the current securities categories. */
function shortTermInvestments(doc: IxDocument, at: string | null): Picked | null {
  const whole = firstTotal(doc, SHORT_TERM_INVESTMENT_TOTALS, at);
  if (whole) return whole;
  const parts = SHORT_TERM_INVESTMENT_PARTS.map((group) => firstTotal(doc, group, at)).filter((p): p is Picked => p != null);
  if (parts.length <= 1) return parts[0] ?? null;
  let decimals: number | null = null;
  for (const p of parts) if (p.decimals != null) decimals = decimals == null ? p.decimals : Math.min(decimals, p.decimals);
  return {
    value: parts.reduce((sum, p) => sum + p.value, 0),
    periodEnd: at,
    concept: parts.map((p) => p.concept).join(" + "),
    unit: parts[0].unit,
    decimals,
    sentence: null,
  };
}

// Every concept totalDebt or the instrument rows read; a filing with none of
// them at any date or dimension reports no borrowings.
const BORROWING_CONCEPTS = new Set([
  "LongTermDebt", "LongTermDebtCurrent", "LongTermDebtNoncurrent", "NotesPayable", "SeniorNotes", "DebtInstrumentFaceAmount",
  ...SHORT_TERM_BORROWING_CONCEPTS, ...DEBT_LINE_CONCEPTS, ...CARRYING_CONCEPTS, ...CONVERTIBLE_TOTAL_CONCEPTS, ...CONVERTIBLE_PART_CONCEPTS,
]);

function tagsNoBorrowings(doc: IxDocument): boolean {
  return !doc.facts.some((f) => BORROWING_CONCEPTS.has(f.local));
}

// A balance tagged 100x coarser than the cash line (decimals two or more
// lower) is a rounded narrative figure, such as "approximately $2.3 billion
// ... classified as cash equivalents", not a balance-sheet line.
const ROUNDED_DECIMALS_GAP = 2;

// A sentence that says the amount is cash equivalents or money-market funds
// restates part of cash; adding it again would count it twice.
const CASH_OVERLAP_RE = /\bcash equivalents?\b|\bmoney[- ]market\b/i;

type BalanceFact = { value: number | null; periodEnd: string | null; decimals: number | null; sentence: string | null };

/** Why a period-end fact is not a balance-sheet amount: "rounded", "overlaps_cash", or null to keep it. */
function balanceExclusion(f: BalanceFact, cash: Picked | null, at: string | null): "rounded" | "overlaps_cash" | null {
  if (!cash || f.value == null || f.periodEnd !== at) return null;
  if (cash.decimals != null && f.decimals != null && f.decimals <= cash.decimals - ROUNDED_DECIMALS_GAP) return "rounded";
  if (f.sentence != null && CASH_OVERLAP_RE.test(f.sentence) && f.value <= cash.value) return "overlaps_cash";
  return null;
}

/** The document without period-end facts that are rounded note figures or restate cash. */
function balanceFacts(doc: IxDocument, cash: Picked | null, at: string | null): IxDocument {
  if (!cash) return doc;
  return { ...doc, facts: doc.facts.filter((f) => hasDims(f) || balanceExclusion(f, cash, at) == null) };
}

// Within half a percent, two totals are the same amount.
const AGGREGATE_TOLERANCE = 0.005;

function totalDebt(doc: IxDocument, at: string | null): Record<string, unknown> | null {
  const shortTerm = SHORT_TERM_BORROWING_CONCEPTS.map((c) => total(doc, c, at)).filter((p): p is Picked => p != null);
  const withShort = (base: number, parts: Picked[], basis: string) => ({
    value: base + shortTerm.reduce((sum, p) => sum + p.value, 0),
    components: [...parts, ...shortTerm].map((p) => ({ concept: p.concept, amount: p.value })),
    basis,
  });
  // Convertible notes on their own balance-sheet line (AAOI) are outside the
  // long-term debt lines when they exceed them; add them so debt is complete.
  const withConvertibles = (base: number, parts: Picked[], basis: string) => {
    const conv = convertibleBalance(doc, at);
    if (conv && conv.total > base) {
      return withShort(base + conv.total, [...parts, ...conv.parts], `${basis}, plus separately reported convertible notes`);
    }
    return withShort(base, parts, basis);
  };
  const all = total(doc, "LongTermDebt", at);
  if (all) return withConvertibles(all.value, [all], "LongTermDebt (current and noncurrent) plus short-term borrowings");
  const cur = total(doc, "LongTermDebtCurrent", at);
  const non = total(doc, "LongTermDebtNoncurrent", at);
  if (cur || non) {
    const parts = [cur, non].filter((p): p is Picked => p != null);
    return withConvertibles(parts.reduce((sum, p) => sum + p.value, 0), parts, "LongTermDebtCurrent + LongTermDebtNoncurrent plus short-term borrowings");
  }
  const lines = DEBT_LINE_CONCEPTS.map((c) => total(doc, c, at)).filter((p): p is Picked => p != null);
  if (lines.length > 0) return withShort(lines.reduce((sum, p) => sum + p.value, 0), lines, "Sum of tagged debt lines plus short-term borrowings");
  const fallback = firstTotal(doc, ["ConvertibleNotesPayable", "NotesPayable", "SeniorNotes"], at);
  if (fallback) return withShort(fallback.value, [fallback], `${fallback.concept} plus short-term borrowings`);
  if (shortTerm.length > 0) return withShort(0, [], "Short-term borrowings only");
  return null;
}

function addYears(date: string, years: number): string {
  const y = parseInt(date.slice(0, 4), 10) + years;
  return `${y}${date.slice(4)}`;
}

function instruments(doc: IxDocument, periodEnd: string | null): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  for (const g of debtGroups(doc)) {
    const face = groupValue(g, [FACE_AMOUNT]);
    // A carrying amount is a balance only at the period end; an amount tagged on
    // another date (often the issue date) is reported as such.
    const carrying = groupValue(g, CARRYING_CONCEPTS, periodEnd);
    const tagged = carrying ? null : groupValue(g, CARRYING_CONCEPTS);
    const coupon = groupValue(g, ["DebtInstrumentInterestRateStatedPercentage"]);
    const effective = groupValue(g, ["DebtInstrumentInterestRateEffectivePercentage"]);
    const convPrice = groupValue(g, [CONVERSION_PRICE]);
    const ratio = groupValue(g, [CONVERSION_RATIO]);
    // One member can carry two tagged maturities and coupons (AAOI's China bank revolver and equipment term loan,
    // one amount): all are reported and the row is placed at the earliest (2.5.24, F-010).
    const maturityDates = [...new Set(groupTexts(g, "DebtInstrumentMaturityDate").map(normalizeIxDate).filter((d): d is string => d != null))].sort();
    const maturityDate = maturityDates[0] ?? null;
    const coupons = groupValues(g, "DebtInstrumentInterestRateStatedPercentage").map((v) => round(v * 100, 4)).sort((a, b) => a - b);
    // A coupon alone is often a duplicate member of an instrument listed elsewhere.
    if (!face && !carrying && !tagged && !maturityDate) continue;
    const latest = g.facts.reduce((best, f) => ((f.periodEnd ?? "") > best ? (f.periodEnd ?? "") : best), "");
    let status = "reported";
    if (maturityDate && periodEnd && maturityDate.length === 10 && maturityDate < periodEnd) status = "matured_before_period_end";
    else if (periodEnd && carrying && carrying.periodEnd === periodEnd) status = "outstanding_at_period_end";
    out.push({
      instrument: g.member ? memberLabel(g.member) : "",
      member: g.member,
      axis: g.axis,
      faceAmount: face ? face.value : null,
      // The date the face amount is tagged at: an issue date before the period end is original principal (F-007).
      faceAmountDate: face ? face.periodEnd : null,
      carryingAmount: carrying ? carrying.value : null,
      carryingAmountConcept: carrying ? carrying.concept : null,
      taggedAmount: tagged ? tagged.value : null,
      taggedAmountDate: tagged ? tagged.periodEnd : null,
      taggedAmountConcept: tagged ? tagged.concept : null,
      couponPct: coupon ? round(coupon.value * 100, 4) : null,
      effectiveRatePct: effective ? round(effective.value * 100, 4) : null,
      maturityDate,
      maturityDateSource: maturityDate ? "XBRL" : null,
      ...(maturityDates.length > 1 ? { maturityDates } : {}),
      ...(coupons.length > 1 ? { couponPcts: coupons } : {}),
      convertible: convPrice != null || ratio != null,
      conversionPrice: convPrice ? convPrice.value : null,
      conversionRatioPer1000: normalizedRatio(ratio ? ratio.value : null, convPrice ? convPrice.value : null).per1000,
      conversionRatioTagged: ratio ? ratio.value : null,
      latestFactDate: latest || null,
      status,
    });
  }
  out.sort((a, b) => {
    const am = (a.maturityDate as string | null) ?? "9999";
    const bm = (b.maturityDate as string | null) ?? "9999";
    return am < bm ? -1 : am > bm ? 1 : String(a.instrument) < String(b.instrument) ? -1 : String(a.instrument) > String(b.instrument) ? 1 : 0;
  });
  return out;
}

// [category, trigger, context the sentence must also have]. The context
// requirement keeps out "next 12 months" revenue recognition, stock-award
// valuation, and capex mentioned only in passing.
const CASH_CONTEXT_RE = /\bcash\b|\bliquidity\b|\bcapital resources\b|\bfund(?:s|ed|ing)?\b|\bfinanc\w*|\brunway\b|\bborrowings?\b/i;
const CAPEX_CONTEXT_RE = /\$\s?\d|\b(?:expects?|expected|plans?|planned|anticipates?|anticipated|intends?|budget(?:ed)?)\b/i;
const FUNDING_CATEGORIES: [string, RegExp, RegExp | null][] = [
  ["going_concern", /\bgoing concern\b|\bsubstantial doubt\b/i, null],
  ["liquidity_sufficiency", /\bsufficient to (?:fund|meet|satisfy|finance)\b|\badequate to (?:fund|meet)\b|\bfully[- ]funded\b|\bcash runway\b|\brunway\b|\bnext (?:12|twelve) months\b/i, CASH_CONTEXT_RE],
  ["atm_program", ATM_RE, null],
  ["capital_expenditure", /\bcapital expenditures?\b|\bcapex\b|\bpurchase commitments?\b/i, CAPEX_CONTEXT_RE],
  ["financing_activity", /\bcredit (?:facility|agreement)\b|\brevolving\b|\bterm loan\b|\bnotes due\b|\bindenture\b/i, null],
];

/** Company statements on liquidity, funding and going concern, classified by what they speak to. */
export function fundingStatements(matches: TextMatch[], limit = 10): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  for (const match of matches) {
    for (const sentence of sentences(collapse(match.contextText))) {
      // A sentence ending in ";" is an item of a list, usually forward-looking boilerplate.
      if (sentence.length < 40 || sentence.endsWith(";")) continue;
      const categories = FUNDING_CATEGORIES.filter(([, re, context]) => re.test(sentence) && (!context || context.test(sentence))).map(([name]) => name);
      if (categories.length === 0) continue;
      const key = sentence.slice(0, 200).toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        categories,
        statement: sentence.slice(0, 600),
        sectionHeading: match.sectionHeading,
        documentUrl: match.documentUrl,
        filingDate: match.filingDate,
        accessionNumber: match.accessionNumber,
      });
      if (out.length >= limit) return out;
    }
  }
  return out;
}

export type CapitalStructureInput = {
  ticker: string;
  source: IxSource;
  fundingMatches: TextMatch[];
  // Filing text stating when instruments mature, read only when an outstanding row has no tagged maturity (2.5.24).
  maturityMatches?: TextMatch[];
};

export const MATURITY_SEARCH_TERMS = ["will mature on", "mature on", "matures on"];
const MATURITY_TEXT_RE = /\bmatur(?:e|es)\s+on\s+((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})/gi;

/** "... will mature on January 15, 2030 ..." sentences, each with the ISO date it states. */
function maturityStatements(matches: TextMatch[]): { date: string; sentence: string }[] {
  const out: { date: string; sentence: string }[] = [];
  const seen = new Set<string>();
  for (const match of matches) {
    for (const sentence of sentences(collapse(match.contextText))) {
      for (const m of sentence.matchAll(MATURITY_TEXT_RE)) {
        const date = normalizeIxDate(m[1]);
        if (!date || date.length !== 10 || seen.has(`${date}|${sentence}`)) continue;
        seen.add(`${date}|${sentence}`);
        out.push({ date, sentence: sentence.slice(0, 400) });
      }
    }
  }
  return out;
}

/** Rows still outstanding with a non-zero amount but no maturity: what a maturity ladder cannot place. */
function unplacedRows(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  return rows.filter((r) => r.maturityDate == null && r.status !== "matured_before_period_end" && (ladderAmount(r)?.amount ?? 0) !== 0);
}

/**
 * Whether the filing text should be searched for maturity sentences: an outstanding row with an amount is dated
 * neither by XBRL nor by the text (a date read from its name is only a year or month).
 */
export function wantsMaturityText(out: Record<string, unknown>): boolean {
  return ((out.instruments ?? []) as Record<string, unknown>[]).some((r) =>
    r.status !== "matured_before_period_end" && (ladderAmount(r)?.amount ?? 0) !== 0
    && (r.maturityDateSource == null || r.maturityDateSource === "INSTRUMENT_NAME"));
}

const NAME_MATURITY_RE = /\b(?:Due|Maturing|Matures)\s+(?:In\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sept?|Oct|Nov|Dec)\.?\s+(?:\d{1,2},\s+)?(?:19|20)\d{2}|(?:19|20)\d{2})\b/;

/**
 * Last resort for a row with no tagged or stated maturity (2.5.25): the date its name gives after "Due" or
 * "Maturing" ("Due January 2031" -> 2031-01, "Due 2036" -> 2036), only when that is not before the period end.
 * The ladder needs only the year.
 */
function maturityFromName(rows: Record<string, unknown>[], periodEnd: string | null): void {
  for (const row of rows) {
    if (row.maturityDate != null || row.status === "matured_before_period_end") continue;
    const m = NAME_MATURITY_RE.exec(String(row.instrument));
    const date = m ? normalizeIxDate(m[1]) : null;
    if (!date || (periodEnd && date < periodEnd.slice(0, date.length))) continue;
    row.maturityDate = date;
    row.maturityDateSource = "INSTRUMENT_NAME";
  }
}

/**
 * A row with no tagged maturity takes the one date the filing text gives for its year (2.5.24, F-008: "The 2030
 * Notes will mature on January 15, 2030" for "Convertible Notes Maturing 2030"). The year must appear in the
 * instrument's name and the text must give exactly one date in it.
 */
function maturityFromText(rows: Record<string, unknown>[], statements: { date: string; sentence: string }[]): void {
  for (const row of unplacedRows(rows)) {
    const years = new Set(String(row.instrument).match(/\b(?:19|20)\d{2}\b/g) ?? []);
    const hits = statements.filter((s) => years.has(s.date.slice(0, 4)));
    const dates = [...new Set(hits.map((h) => h.date))];
    if (dates.length !== 1) continue;
    row.maturityDate = dates[0];
    row.maturityDateSource = "FILING_TEXT";
    row.maturityStatement = hits[0].sentence;
  }
}

/** The amount a row adds to the year ladder and on what basis: the period-end carrying amount, else face, else a tagged amount. */
function ladderAmount(row: Record<string, unknown>): { amount: number; basis: string } | null {
  if (typeof row.carryingAmount === "number") return { amount: row.carryingAmount, basis: "CARRYING" };
  if (typeof row.faceAmount === "number") return { amount: row.faceAmount, basis: "FACE" };
  if (typeof row.taggedAmount === "number") return { amount: row.taggedAmount, basis: "TAGGED" };
  return null;
}

/** Cash, debt, instruments and maturities at the filing's period end, with the company's own funding statements. */
export function capitalStructure(input: CapitalStructureInput): Record<string, unknown> {
  const { source } = input;
  const doc = source.doc;
  const periodEnd = doc.documentPeriodEnd;
  const cash = firstTotal(doc, CASH_CONCEPTS, periodEnd);
  const warnings: Record<string, unknown>[] = [];
  // Investments and debt are read without rounded note figures or restated cash.
  const balanceDoc = balanceFacts(doc, cash, periodEnd);
  let shortTerm = shortTermInvestments(balanceDoc, periodEnd);
  const longTermSecurities = total(balanceDoc, "MarketableSecuritiesNoncurrent", periodEnd);
  let debt = totalDebt(balanceDoc, periodEnd);
  const ignored = [
    [shortTermInvestments(doc, periodEnd), shortTerm],
    [total(doc, "MarketableSecuritiesNoncurrent", periodEnd), longTermSecurities],
  ] as [Picked | null, Picked | null][];
  for (const [raw, kept] of ignored) {
    if (!raw || (kept && kept.value === raw.value && kept.concept === raw.concept)) continue;
    if (balanceExclusion(raw, cash, periodEnd) === "overlaps_cash") {
      warnings.push({ code: "OVERLAPS_CASH_EQUIVALENTS", message: `${raw.concept} ${raw.value} is tagged in a sentence describing cash equivalents or money-market funds; it is part of cash, not added to it.`, severity: "info", sentence: raw.sentence });
    } else {
      warnings.push({ code: "ROUNDED_FACT_IGNORED", message: `${raw.concept} ${raw.value} is rounded to ${raw.decimals} decimals against cash at ${cash!.decimals}; read as a narrative figure, not a balance-sheet line.`, severity: "info" });
    }
  }
  // The filing's own cash-plus-investments total, when tagged, must agree.
  const aggregate = total(balanceDoc, "CashCashEquivalentsAndShortTermInvestments", periodEnd);
  if (aggregate && cash && shortTerm && Math.abs(cash.value + shortTerm.value - aggregate.value) > AGGREGATE_TOLERANCE * Math.abs(aggregate.value)) {
    if (Math.abs(aggregate.value - cash.value) <= AGGREGATE_TOLERANCE * Math.abs(aggregate.value)) {
      warnings.push({ code: "CASH_AGGREGATE_MISMATCH", message: `The filing's cash and short-term investments total ${aggregate.value} equals cash alone, so ${shortTerm.concept} ${shortTerm.value} is already inside cash and is not added.`, severity: "info" });
      shortTerm = null;
    } else {
      warnings.push({ code: "CASH_AGGREGATE_MISMATCH", message: `Cash ${cash.value} plus ${shortTerm.concept} ${shortTerm.value} differs from the filing's cash and short-term investments total ${aggregate.value}.`, severity: "warning" });
    }
  }
  const rawDebt = totalDebt(doc, periodEnd);
  if (rawDebt && (!debt || debt.value !== rawDebt.value)) {
    warnings.push({ code: "ROUNDED_FACT_IGNORED", message: `Total debt ${rawDebt.value} includes figures rounded far more coarsely than cash; they were left out.`, severity: "info" });
  }
  if (!debt && cash && tagsNoBorrowings(doc)) {
    debt = { value: 0, components: [], basis: "No borrowing concepts tagged in the filing" };
    warnings.push({ code: "NO_BORROWINGS_TAGGED", message: "The filing tags no borrowings at any date, so total debt is taken as zero (leases excluded).", severity: "info" });
  }
  const instrumentRows = instruments(doc, periodEnd);
  if (input.maturityMatches?.length) maturityFromText(instrumentRows, maturityStatements(input.maturityMatches));
  maturityFromName(instrumentRows, periodEnd);
  const named = instrumentRows.filter((r) => r.maturityDateSource === "INSTRUMENT_NAME");
  if (named.length > 0) {
    warnings.push({ code: "MATURITY_FROM_INSTRUMENT_NAME", message: `No tagged or stated maturity date for ${named.map((r) => `${r.instrument} (${r.maturityDate})`).join(", ")}; dated from the instrument name, to the year or month it gives.`, severity: "info" });
  }
  for (const row of instrumentRows) {
    if (Array.isArray(row.maturityDates)) {
      warnings.push({ code: "MULTIPLE_MATURITIES", message: `${row.instrument} carries ${row.maturityDates.length} tagged maturities (${row.maturityDates.join(", ")}) on one amount; the year ladder places it at the earliest.`, severity: "info" });
    }
    const labelled = LABEL_DATE_RE.exec(String(row.instrument));
    const labelDate = labelled ? normalizeIxDate(labelled[0]) : null;
    if (labelDate && row.maturityDateSource === "XBRL" && !(row.maturityDates as string[] | undefined ?? [row.maturityDate]).includes(labelDate)) {
      warnings.push({ code: "LABEL_DATE_DIFFERS_FROM_MATURITY", message: `${row.instrument} is named for ${labelled![0]} but its tagged maturity is ${row.maturityDate}; the tagged date is used.`, severity: "info" });
    }
  }
  const ladder = LADDER.map(([concept, bucket, offset]) => {
    const hit = total(doc, concept, periodEnd);
    if (!hit) return null;
    return {
      bucket,
      periodThrough: periodEnd && offset > 0 && offset < 6 ? addYears(periodEnd, offset) : null,
      amount: hit.value,
      concept: hit.concept,
    };
  }).filter((r): r is NonNullable<typeof r> => r != null);
  // Each bucket states its amount's basis; faceAmount sums only rows tagged with a face amount (2.5.24, F-009:
  // it summed carrying amounts under the face name).
  const byYear = new Map<string, { year: string; amount: number; bases: Set<string>; faceAmount: number | null; instruments: string[] }>();
  let laddered = 0;
  let ladderedAny = 0;
  const ladderedBases = new Set<string>();
  for (const row of instrumentRows) {
    const maturity = row.maturityDate as string | null;
    const amount = ladderAmount(row);
    if (!maturity || !amount || row.status === "matured_before_period_end") continue;
    const year = maturity.slice(0, 4);
    const entry = byYear.get(year) ?? { year, amount: 0, bases: new Set<string>(), faceAmount: null, instruments: [] };
    entry.amount += amount.amount;
    entry.bases.add(amount.basis);
    if (typeof row.faceAmount === "number") entry.faceAmount = (entry.faceAmount ?? 0) + row.faceAmount;
    entry.instruments.push(String(row.instrument));
    byYear.set(year, entry);
    if (typeof row.carryingAmount === "number") laddered += row.carryingAmount;
    ladderedAny += amount.amount;
    ladderedBases.add(amount.basis);
  }
  const instrumentLadder = [...byYear.values()]
    .sort((a, b) => (a.year < b.year ? -1 : a.year > b.year ? 1 : 0))
    .map((e) => ({ year: e.year, amount: e.amount, amountBasis: e.bases.size === 1 ? [...e.bases][0] : "MIXED", faceAmount: e.faceAmount, instruments: e.instruments }));
  // How much of total debt the year ladder places, at carrying amounts like total debt, and what it cannot
  // (2.5.24, F-008: AAOI's ladder held 58.9M of 188.1M with nothing said).
  const debtValue = debt && typeof debt.value === "number" ? debt.value : null;
  const unplaced = unplacedRows(instrumentRows).map((r) => ({ instrument: r.instrument, amount: ladderAmount(r)!.amount, amountBasis: ladderAmount(r)!.basis, reason: "NO_MATURITY_DATE" }));
  const ladderCoverage = {
    totalDebt: debtValue,
    ladderedCarryingAmount: laddered,
    coveragePct: debtValue && debtValue > 0 ? round((laddered / debtValue) * 100, 2) : null,
    // Every bucket amount on whatever basis it has (face-only ladders like VRT's carry no carrying amounts).
    ladderedAmount: ladderedAny,
    ladderedAmountBasis: ladderedBases.size === 0 ? null : ladderedBases.size === 1 ? [...ladderedBases][0] : "MIXED",
    notLaddered: unplaced,
  };
  // When an outstanding instrument has no maturity and the filing tags no maturity ladder of its own.
  if (unplaced.length > 0 && ladder.length === 0) {
    warnings.push({
      code: "MATURITY_LADDER_INCOMPLETE",
      message: `No maturity date for ${unplaced.map((u) => `${u.instrument} (${u.amount})`).join(", ")}; the year ladder places ${laddered}${debtValue ? ` of total debt ${debtValue} (${ladderCoverage.coveragePct}%)` : ""}.`,
      severity: "warning",
    });
  }
  // Instrument rows' period-end carrying amounts against total debt (2.5.24, F-007: AAOI tags 124.9M principal as
  // the 2030 Notes' carrying amount; the balance sheet carries 129.1M).
  const outstanding = instrumentRows.filter((r) => r.status !== "matured_before_period_end" && ladderAmount(r) != null);
  const carried = outstanding.filter((r) => typeof r.carryingAmount === "number");
  const carriedTotal = carried.reduce((sum, r) => sum + (r.carryingAmount as number), 0);
  const withoutCarrying = outstanding.length - carried.length;
  // Compared only when every outstanding row has a period-end carrying amount (2.5.25: VRT's notes carry only face
  // amounts, so its rows summed to 0 against 2.94B and read as a gap).
  const instrumentReconciliation = !debtValue || carried.length === 0 || withoutCarrying > 0
    ? { status: "NOT_COMPARABLE", instrumentsCarryingTotal: null, totalDebt: debtValue, difference: null, rowsWithoutCarryingAmount: withoutCarrying }
    : {
      status: Math.abs(carriedTotal - debtValue) <= AGGREGATE_TOLERANCE * Math.abs(debtValue) ? "RECONCILED" : "NOT_RECONCILED",
      instrumentsCarryingTotal: carriedTotal,
      totalDebt: debtValue,
      difference: debtValue - carriedTotal,
      rowsWithoutCarryingAmount: withoutCarrying,
    };
  if (instrumentReconciliation.status === "NOT_RECONCILED") {
    warnings.push({
      code: "INSTRUMENTS_DO_NOT_RECONCILE",
      message: `Instrument rows carry ${carriedTotal} at the period end against total debt ${debtValue} (difference ${instrumentReconciliation.difference}): `
        + "an instrument may be untagged, or a tagged amount may be principal rather than carrying value.",
      severity: "warning",
    });
  }
  // Convertible rows against the balance sheet's own convertible line (2.5.26, F-007: AAOI tags the 2030 Notes'
  // "approximately $124.9 million" principal as DebtInstrumentCarryingAmount; the balance sheet carries them at
  // 129,142,000). The rows are marked, not rewritten: the balance-sheet line is not tagged to the instrument.
  const convertibleRows = carried.filter((r) => r.convertible === true);
  const convertibleLine = convertibleRows.length > 0 ? convertibleBalance(balanceDoc, periodEnd) : null;
  if (convertibleLine) {
    const rowsTotal = convertibleRows.reduce((sum, r) => sum + (r.carryingAmount as number), 0);
    const lineConcept = convertibleLine.parts.map((part) => part.concept).join(" + ");
    const matches = Math.abs(rowsTotal - convertibleLine.total) <= AGGREGATE_TOLERANCE * Math.abs(convertibleLine.total);
    (instrumentReconciliation as Record<string, unknown>).convertibleBalanceSheet = {
      status: matches ? "RECONCILED" : "NOT_RECONCILED",
      concept: lineConcept,
      balanceSheetAmount: convertibleLine.total,
      instrumentRowsTotal: rowsTotal,
      difference: convertibleLine.total - rowsTotal,
    };
    if (!matches) {
      for (const row of convertibleRows) row.carryingAmountBasis = "MAY_BE_PRINCIPAL";
      warnings.push({
        code: "CARRYING_AMOUNT_MAY_BE_PRINCIPAL",
        message: `${convertibleRows.map((r) => `${r.instrument} carries ${r.carryingAmount} under ${r.carryingAmountConcept}`).join("; ")}, `
          + `but the balance sheet carries convertible notes at ${convertibleLine.total} (${lineConcept}); the row amount may be principal, not carrying value.`,
        severity: "warning",
      });
    }
  }
  // What separates the ladder from total debt: rows it cannot place, and rows whose amounts do not add up to it.
  if (debtValue != null) {
    const unplacedTotal = unplaced.reduce((sum, u) => sum + (u.amount as number), 0);
    (ladderCoverage as Record<string, unknown>).gap = {
      amount: debtValue - laddered,
      inNotLaddered: unplacedTotal,
      inReconciliationDifference: instrumentReconciliation.status === "NOT_RECONCILED" ? instrumentReconciliation.difference : null,
    };
  }
  const liquid = (cash ? cash.value : 0) + (shortTerm ? shortTerm.value : 0);
  if (doc.facts.length === 0) {
    warnings.push({ code: "NO_INLINE_XBRL", message: "The filing carries no inline XBRL facts.", severity: "warning" });
  }
  if (cash && cash.concept.endsWith("RestrictedCashEquivalents")) {
    warnings.push({ code: "CASH_INCLUDES_RESTRICTED", message: "Only a cash figure that includes restricted cash was tagged.", severity: "info" });
  }
  const funding = fundingStatements(input.fundingMatches);
  const status = cash || debt || instrumentRows.length > 0 ? (cash && debt ? "COMPUTED" : "PARTIAL") : "NOT_FOUND";
  return {
    ticker: input.ticker,
    status,
    basis: "COMPANY_DISCLOSED",
    decisionUse: "COMPANY_DISCLOSED_NOT_FORECAST",
    source: sourceRef(source, periodEnd),
    periodEnd,
    balances: {
      cashAndEquivalents: cash ? cash.value : null,
      currency: cash ? cash.unit : null,
      cashConcept: cash ? cash.concept : null,
      shortTermInvestments: shortTerm ? shortTerm.value : null,
      shortTermInvestmentsConcept: shortTerm ? shortTerm.concept : null,
      marketableSecuritiesNoncurrent: longTermSecurities ? longTermSecurities.value : null,
      totalDebt: debt ? debt.value : null,
      totalDebtBasis: debt ? debt.basis : null,
      totalDebtComponents: debt ? debt.components : [],
      netCash: cash && debt ? liquid - (debt.value as number) : null,
      netCashFormula: "cash and equivalents + short-term investments - total debt (carrying amounts; leases excluded)",
    },
    instruments: instrumentRows,
    maturityLadder: ladder,
    instrumentMaturitiesByYear: instrumentLadder,
    ladderCoverage,
    instrumentReconciliation,
    fundingStatements: funding,
    methodology: [
      "Balances are the filing's own tagged values at its period end; nothing is projected forward.",
      "Instrument rows come from facts dimensioned by debt instrument, which companyfacts omits; face amounts can be original principal (faceAmountDate says when it was tagged).",
      "The year ladder sums each row's period-end carrying amount, else its face amount, else a tagged amount (amountBasis); a row with no tagged maturity takes one only when the filing text states a single maturity date in the year its name carries, else the year or month its name gives after Due or Maturing (maturityDateSource INSTRUMENT_NAME).",
      "Funding statements are quoted from the filing so the company's own runway and sufficiency claims can be read in context.",
    ],
    warnings,
  };
}

// ── Analyst valuation methods ───────────────────────────────────────────────

const BROKERS = [
  "Morgan Stanley", "Goldman Sachs", "JPMorgan", "J.P. Morgan", "Jefferies", "Needham", "Rosenblatt", "Craig-Hallum",
  "B. Riley", "Cantor Fitzgerald", "Barclays", "Citi", "Citigroup", "UBS", "BofA", "Bank of America", "Wells Fargo",
  "Deutsche Bank", "Mizuho", "Stifel", "Piper Sandler", "Raymond James", "KeyBanc", "Oppenheimer", "TD Cowen",
  "Evercore", "Bernstein", "Wedbush", "Northland", "Benchmark", "Loop Capital", "Susquehanna", "Truist", "Baird",
  "HSBC", "Macquarie", "Nomura", "Canaccord", "Roth", "Lake Street", "H.C. Wainwright", "D.A. Davidson",
  "Berenberg", "Redburn", "Peel Hunt", "Carnegie", "ABG Sundal Collier", "Pareto", "SEB", "Danske", "Handelsbanken",
  "DNB", "Liberum", "Panmure", "Shore Capital", "Cavendish", "Investec", "Numis", "Guggenheim", "BMO", "RBC",
  "Scotiabank", "CIBC", "Bernstein SocGen", "Exane", "Kepler", "Jyske", "Nordea", "Arctic",
];

const CURRENCY = "(\\$|US\\$|USD\\s?|\\u00a3|GBP\\s?|GBp\\s?|GBX\\s?|\\u20ac|EUR\\s?|SEK\\s?|kr\\s?|NT\\$|TWD\\s?)?";
const NUM = "(\\d[\\d,]*(?:\\.\\d+)?)";
const SUFFIX = "(?:\\s?(p|pence|kr|SEK)\\b)?";
const TARGET_FROM_TO = new RegExp(`(?:price target|target price|\\bPT\\b|price objective)[^.]{0,40}?\\bfrom\\s+${CURRENCY}\\s?${NUM}${SUFFIX}\\s+to\\s+${CURRENCY}\\s?${NUM}${SUFFIX}`, "i");
const TARGET_TO_FROM = new RegExp(`(?:price target|target price|\\bPT\\b|price objective)[^.\\d$\\u00a3\\u20ac]{0,40}?\\bto\\s+${CURRENCY}\\s?${NUM}${SUFFIX}\\s+from\\s+${CURRENCY}\\s?${NUM}${SUFFIX}`, "i");
const TARGET_BEFORE = /(\$|US\$|\u00a3|\u20ac|NT\$)\s?(\d[\d,]*(?:\.\d+)?)\s+(?:price\s+)?target\b/i;
// A number followed by "%" is a move or a rate, never a target.
const TARGET_TO = new RegExp(`(?:price target|target price|\\bPT\\b|price objective)[^.\\d$\\u00a3\\u20ac]{0,40}?${CURRENCY}\\s?${NUM}(?![\\d.,]*\\s?%)${SUFFIX}`, "i");
const PERIOD_RE = /\b(?:FY|CY|F)\s?'?\d{2,4}E?\b|\b[12]H\s?'?\d{2,4}E?\b|\b(?:19|20)\d\dE?\b|\bNTM\b|\bnext[- ]twelve[- ]months\b|\bforward\b/gi;
const METRIC = "(EV\\s?\\/\\s?EBITDA|EV\\s?\\/\\s?sales|EV\\s?\\/\\s?revenue|EV\\s?\\/\\s?EBIT|P\\s?\\/\\s?E|price[- ]to[- ]earnings|price[- ]to[- ]sales|EBITDA|EBIT|sales|revenue|earnings|EPS|free cash flow|FCF|gross profit|book value|NAV)";
const MULTIPLE_FIRST = new RegExp(`\\b(\\d{1,3}(?:\\.\\d+)?)\\s?(?:x|times)\\b([^.;]{0,40}?)\\b${METRIC}\\b`, "gi");
const NOT_A_MULTIPLE_RE = /\b(?:than|grew|growth|increase[sd]?|faster|more)\b/i;
const METRIC_FIRST = new RegExp(`\\b${METRIC}(?:\\s+multiple)?\\s+(?:of|at)\\s+(?:about\\s+|roughly\\s+|approximately\\s+|~)?(\\d{1,3}(?:\\.\\d+)?)\\s?(?:x|times)\\b([^.;]{0,30})`, "gi");

function currencyCode(symbol: string | undefined, suffix: string | undefined = undefined): string | null {
  const s = (symbol ?? "").trim();
  if (!s) {
    const x = (suffix ?? "").toLowerCase();
    if (x === "p" || x === "pence") return "GBp";
    if (x === "kr" || x === "sek") return "SEK";
    return null;
  }
  if (s === "$" || s === "US$" || s === "USD") return "USD";
  if (s === "\u00a3" || s === "GBP") return "GBP";
  if (s === "GBp" || s === "GBX") return "GBp";
  if (s === "\u20ac" || s === "EUR") return "EUR";
  if (s === "SEK" || s === "kr") return "SEK";
  if (s === "NT$" || s === "TWD") return "TWD";
  return null;
}

function numberOf(text: string): number {
  return parseFloat(text.replace(/,/g, ""));
}

function normalizedMetric(metric: string): { metric: string; enterpriseValue: boolean } {
  const m = metric.replace(/\s+/g, "").toLowerCase();
  if (m.startsWith("ev/")) return { metric: m.slice(3) === "ebitda" ? "EBITDA" : m.slice(3) === "ebit" ? "EBIT" : "sales", enterpriseValue: true };
  if (m === "p/e" || m === "price-to-earnings" || m === "pricetoearnings" || m === "earnings" || m === "eps") return { metric: "earnings", enterpriseValue: false };
  if (m === "price-to-sales" || m === "pricetosales" || m === "revenue" || m === "sales") return { metric: "sales", enterpriseValue: false };
  if (m === "fcf" || m === "freecashflow") return { metric: "free cash flow", enterpriseValue: false };
  if (m === "ebitda") return { metric: "EBITDA", enterpriseValue: false };
  if (m === "ebit") return { metric: "EBIT", enterpriseValue: false };
  if (m === "grossprofit") return { metric: "gross profit", enterpriseValue: false };
  if (m === "bookvalue") return { metric: "book value", enterpriseValue: false };
  return { metric: metric.toUpperCase() === "NAV" ? "NAV" : metric, enterpriseValue: false };
}

function periods(text: string): string[] {
  const out: string[] = [];
  for (const m of text.matchAll(PERIOD_RE)) {
    let v = m[0].replace(/\s+/g, "").toUpperCase();
    if (v.startsWith("NEXT")) v = "NTM";
    if (!out.includes(v)) out.push(v);
  }
  return out;
}

function percentAfter(sentence: string, re: RegExp): number | null {
  const m = re.exec(sentence);
  return m ? numberOf(m[1]) : null;
}

function sentenceMethods(sentence: string): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  const addMultiple = (value: string, metric: string, window: string) => {
    if (NOT_A_MULTIPLE_RE.test(window)) return;
    const norm = normalizedMetric(metric);
    const multiple = numberOf(value);
    const key = `${multiple}|${norm.metric}|${norm.enterpriseValue}`;
    if (seen.has(key) || !(multiple > 0)) return;
    seen.add(key);
    const evWindow = /\bEV\b|enterprise value/i.test(window) || norm.enterpriseValue;
    out.push({ method: "multiple", multiple, metric: norm.metric, enterpriseValue: evWindow, periods: periods(window) });
  };
  for (const m of sentence.matchAll(MULTIPLE_FIRST)) addMultiple(m[1], m[3], `${m[2]} ${m[3]}`);
  for (const m of sentence.matchAll(METRIC_FIRST)) addMultiple(m[2], m[1], `${m[1]} ${m[3]}`);
  if (/\bdiscounted cash[- ]flow\b|\bDCF\b/i.test(sentence)) {
    out.push({
      method: "DCF",
      waccPct: percentAfter(sentence, /\bWACC\b[^%\d]{0,20}(\d{1,2}(?:\.\d+)?)\s?%/i) ?? percentAfter(sentence, /(\d{1,2}(?:\.\d+)?)\s?%\s+WACC\b/i),
      discountRatePct: percentAfter(sentence, /\bdiscount rate\b[^%\d]{0,20}(\d{1,2}(?:\.\d+)?)\s?%/i) ?? percentAfter(sentence, /(\d{1,2}(?:\.\d+)?)\s?%\s+discount rate\b/i),
      terminalGrowthPct: percentAfter(sentence, /\bterminal (?:growth )?(?:rate )?[^%\d]{0,20}(\d{1,2}(?:\.\d+)?)\s?%/i) ?? percentAfter(sentence, /(\d{1,2}(?:\.\d+)?)\s?%\s+terminal growth\b/i),
      exitMultiple: percentAfter(sentence, /\b(?:terminal|exit) multiple\b[^\d]{0,20}(\d{1,3}(?:\.\d+)?)\s?x\b/i),
    });
  }
  if (/\bsum[- ]of[- ](?:the[- ])?parts\b|\bSOTP\b/i.test(sentence)) out.push({ method: "sum_of_the_parts" });
  if (/\brisk[- ]adjusted NPV\b|\brNPV\b/i.test(sentence)) out.push({ method: "risk_adjusted_npv" });
  if (/\bprobability[- ]weighted\b|\bscenario[- ]weighted\b/i.test(sentence)) out.push({ method: "probability_weighted_scenarios" });
  if (/\bpeer (?:group )?(?:multiple|average|median)\b|\bcomparable compan(?:y|ies)\b|\bcomps\b/i.test(sentence)) out.push({ method: "peer_comparison" });
  return out;
}

function priceTarget(sentence: string): Record<string, unknown> | null {
  const fromTo = TARGET_FROM_TO.exec(sentence);
  if (fromTo) {
    return { target: numberOf(fromTo[5]), prior: numberOf(fromTo[2]), currency: currencyCode(fromTo[4] ?? fromTo[1], fromTo[6] ?? fromTo[3]) };
  }
  const toFrom = TARGET_TO_FROM.exec(sentence);
  if (toFrom) {
    return { target: numberOf(toFrom[2]), prior: numberOf(toFrom[5]), currency: currencyCode(toFrom[1] ?? toFrom[4], toFrom[3] ?? toFrom[6]) };
  }
  // "$92 price target" names its number outright; try it before scanning past the phrase.
  const before = TARGET_BEFORE.exec(sentence);
  if (before) return { target: numberOf(before[2]), prior: null, currency: currencyCode(before[1]) };
  const to = TARGET_TO.exec(sentence);
  if (to) return { target: numberOf(to[2]), prior: null, currency: currencyCode(to[1], to[3]) };
  return null;
}

function firmIn(text: string, firms: string[]): string | null {
  let best: { firm: string; index: number } | null = null;
  for (const firm of firms) {
    if (!firm) continue;
    const escaped = firm.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    // Case-sensitive: firm names are proper nouns ("Benchmark", not "benchmark").
    const m = new RegExp(`(?:^|[^A-Za-z])${escaped}(?![A-Za-z])`).exec(text);
    if (m && (best == null || m.index < best.index || (m.index === best.index && firm.length > best.firm.length))) best = { firm, index: m.index };
  }
  return best ? best.firm : null;
}

export type NewsItem = { title?: unknown; summary?: unknown; url?: unknown; publishedAt?: unknown; source?: unknown; originalSource?: unknown; publisher?: unknown };
export type RatingChange = { date?: unknown; firm?: unknown; toGrade?: unknown; ptTo?: unknown; ptFrom?: unknown };

/** Valuation methods named in news headlines and summaries: context, never model inputs. */
// Target attribution (2.4.4): a headline such as "Rocket Lab climbs as Cantor
// reiterates $122 target; ... AST SpaceMobile rises" names several companies,
// and the target belongs to the one in its own clause.
const LEGAL_SUFFIXES = new Set(["inc", "incorporated", "corp", "corporation", "ltd", "limited", "llc", "plc", "co", "company", "sa", "ag", "nv", "se", "gmbh", "holdings", "group"]);

function normPhrase(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

/** Lowercase phrases that name the subject: the ticker (3+ letters) and the company name with and without its legal suffix. */
export function subjectAliases(ticker: string, names: string[]): string[] {
  const out = new Set<string>();
  const base = normPhrase(ticker.split(".")[0]);
  if (base.length >= 3) out.add(base);
  for (const name of names) {
    const norm = normPhrase(name);
    if (!norm) continue;
    const words = norm.split(" ");
    while (words.length > 1 && LEGAL_SUFFIXES.has(words[words.length - 1])) words.pop();
    for (const alias of [norm, words.join(" ")]) if (alias.length >= 3) out.add(alias);
  }
  return [...out].sort();
}

function mentionsSubject(text: string, aliases: string[]): boolean {
  const padded = ` ${normPhrase(text)} `;
  return aliases.some((a) => padded.includes(` ${a} `));
}

/** The clause of a sentence (split at "; " and ", ") that carries its price target, else the sentence. */
function targetClause(sentence: string): string {
  return sentence.split(/;\s*|,\s+/).find((clause) => priceTarget(clause) != null) ?? sentence;
}

/**
 * Valuation methods and targets named in news. With issuerNames, each target
 * and method must belong to the subject: its clause names the subject, or no
 * sentence of the item names another company's target while the item names
 * the subject. Without issuerNames nothing is checked (subjectMatch NOT_CHECKED).
 */
export function analystValuationMethods(ticker: string, items: NewsItem[], changes: RatingChange[], issuerNames: string[] | null = null): Record<string, unknown> {
  const aliases = issuerNames ? subjectAliases(ticker, issuerNames) : null;
  let rejectedOtherCompany = 0;
  const changeFirms = [...new Set(changes.map((c) => String(c.firm ?? "").trim()).filter(Boolean))];
  const firms = [...changeFirms, ...BROKERS.filter((b) => !changeFirms.some((c) => c.toLowerCase() === b.toLowerCase()))];
  const evidence: Record<string, unknown>[] = [];
  const firmsWithMethod = new Set<string>();
  const seenUrls = new Set<string>();
  for (const item of items) {
    const title = String(item.title ?? "").trim();
    const summary = String(item.summary ?? "").trim();
    const url = typeof item.url === "string" ? item.url : null;
    const key = url ?? title;
    if (!key || seenUrls.has(key)) continue;
    seenUrls.add(key);
    const text = collapse(summary && !summary.startsWith(title) ? `${title}. ${summary}` : (summary || title));
    const firm = firmIn(text, firms);
    const itemSentences = sentences(text);
    // An item is conflicted when a sentence names the subject but puts a
    // target in a clause about another company.
    const conflicted = aliases != null && itemSentences.some((s) =>
      priceTarget(s) != null && mentionsSubject(s, aliases) && !mentionsSubject(targetClause(s), aliases));
    const itemNamesSubject = aliases != null && mentionsSubject(text, aliases);
    for (const sentence of itemSentences) {
      const methods = sentenceMethods(sentence);
      const target = priceTarget(sentence);
      if (methods.length === 0 && !(target && firm)) continue;
      let subjectMatch = "NOT_CHECKED";
      if (aliases != null) {
        const clause = target ? targetClause(sentence) : sentence;
        if (mentionsSubject(clause, aliases)) subjectMatch = "CLAUSE";
        else if (!mentionsSubject(sentence, aliases) && itemNamesSubject && !conflicted) subjectMatch = "ITEM";
        else {
          rejectedOtherCompany += 1;
          continue;
        }
      }
      if (methods.length > 0 && firm) firmsWithMethod.add(firm);
      evidence.push({
        firm,
        publishedAt: item.publishedAt ?? null,
        publisher: item.originalSource ?? item.publisher ?? item.source ?? null,
        url,
        title: title.slice(0, 240),
        sentence: sentence.slice(0, 400),
        priceTarget: target,
        methods,
        methodDisclosed: methods.length > 0,
        subjectMatch,
      });
    }
  }
  const methodNotDisclosed: Record<string, unknown>[] = [];
  const noted = new Set<string>();
  for (const e of evidence) {
    if (e.methodDisclosed || !e.firm || firmsWithMethod.has(String(e.firm))) continue;
    const k = `news|${e.firm}`;
    if (noted.has(k)) continue;
    noted.add(k);
    methodNotDisclosed.push({ firm: e.firm, priceTarget: e.priceTarget, date: e.publishedAt, source: "news", url: e.url });
  }
  for (const c of changes) {
    const firm = String(c.firm ?? "").trim();
    const target = Number(c.ptTo ?? 0);
    if (!firm || !(target > 0) || firmsWithMethod.has(firm) || noted.has(`rating|${firm}`)) continue;
    noted.add(`rating|${firm}`);
    methodNotDisclosed.push({ firm, priceTarget: { target, prior: Number(c.ptFrom ?? 0) > 0 ? Number(c.ptFrom) : null, currency: null }, date: c.date ?? null, source: "rating_changes", url: null });
  }
  const methodCounts: Record<string, number> = {};
  for (const e of evidence) {
    for (const m of e.methods as Record<string, unknown>[]) {
      const name = String(m.method);
      methodCounts[name] = (methodCounts[name] ?? 0) + 1;
    }
  }
  return {
    ticker,
    status: evidence.some((e) => e.methodDisclosed) ? "FOUND" : (evidence.length > 0 || methodNotDisclosed.length > 0 ? "METHOD_NOT_DISCLOSED" : "NOT_FOUND"),
    basis: "NEWS_TEXT_EXTRACTION",
    decisionUse: "CONTEXT_ONLY",
    itemsScanned: seenUrls.size,
    ratingChangesScanned: changes.length,
    methodCounts,
    evidence: evidence.slice(0, 40),
    methodNotDisclosed,
    attribution: aliases != null
      ? { checked: true, subjectAliases: aliases, rejectedForOtherCompany: rejectedOtherCompany }
      : { checked: false, subjectAliases: [], rejectedForOtherCompany: 0 },
    caveats: [
      "Read from headlines and short summaries, not the research notes; a method named here may be one of several the analyst used.",
      "Multiples and rates are the analyst's, quoted as published. Do not treat them as consensus or back-solve targets into forecasts.",
    ],
  };
}

// ── Companies House ─────────────────────────────────────────────────────────

const CH_CATEGORY_LABELS: Record<string, string> = {
  accounts: "Accounts",
  capital: "Share capital (allotments, buybacks)",
  mortgage: "Charges (secured lending)",
  "confirmation-statement": "Confirmation statement",
  resolution: "Resolutions (incl. allotment authority)",
  incorporation: "Incorporation",
  officers: "Officers",
  "persons-with-significant-control": "Persons with significant control",
  address: "Registered address",
  annotation: "Annotation",
  "change-of-name": "Change of name",
  "miscellaneous": "Miscellaneous",
};

/** Companies House filing-history items as dated evidence rows with document links. */
export function companiesHouseFilings(companyNumber: string, payload: Record<string, unknown>): Record<string, unknown>[] {
  const items = Array.isArray(payload.items) ? payload.items as Record<string, unknown>[] : [];
  return items.map((item) => {
    const links = (item.links && typeof item.links === "object" ? item.links : {}) as Record<string, unknown>;
    const meta = typeof links.document_metadata === "string" ? links.document_metadata : null;
    const category = String(item.category ?? "");
    const values = (item.description_values && typeof item.description_values === "object" ? item.description_values : {}) as Record<string, unknown>;
    const description = String(item.description ?? "").replace(/-/g, " ").trim();
    const transactionId = typeof item.transaction_id === "string" ? item.transaction_id : null;
    return {
      date: item.date ?? null,
      type: item.type ?? null,
      category,
      categoryLabel: CH_CATEGORY_LABELS[category] ?? category,
      description,
      descriptionValues: values,
      pages: typeof item.pages === "number" ? item.pages : null,
      documentMetadataUrl: meta,
      documentContentUrl: meta ? `${meta}/content` : null,
      viewerUrl: transactionId
        ? `https://find-and-update.company-information.service.gov.uk/company/${companyNumber}/filing-history/${transactionId}/document?format=pdf&download=0`
        : null,
    };
  });
}

function normalizedCompanyName(name: string): string {
  return name.toUpperCase().replace(/&/g, " AND ").replace(/[^A-Z0-9 ]/g, " ")
    .replace(/\b(?:PUBLIC LIMITED COMPANY|PLC|LIMITED|LTD|HOLDINGS?|GROUP|THE)\b/g, " ")
    .replace(/\s+/g, " ").trim();
}

/** The search result that names the issuer, preferring active companies; null when none does. */
export function pickCompaniesHouseMatch(issuerName: string, payload: Record<string, unknown>): Record<string, unknown> | null {
  const items = Array.isArray(payload.items) ? payload.items as Record<string, unknown>[] : [];
  const wanted = normalizedCompanyName(issuerName);
  if (!wanted) return null;
  const exact = items.filter((i) => normalizedCompanyName(String(i.title ?? "")) === wanted);
  const active = exact.find((i) => String(i.company_status ?? "") === "active");
  return active ?? exact[0] ?? null;
}
