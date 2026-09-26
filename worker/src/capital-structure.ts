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

function collapse(text: string): string {
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
    .replace(/\s+/g, " ")
    .trim();
}

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

function optionsComponent(sources: IxSource[], price: number): Record<string, unknown> | null {
  const found = findInSources(sources, (doc) => total(doc, OPTIONS_OUTSTANDING));
  if (!found) return null;
  const { value: outstanding, source } = found;
  const strike = total(source.doc, OPTIONS_STRIKE, outstanding.periodEnd);
  const exercisable = total(source.doc, OPTIONS_EXERCISABLE, outstanding.periodEnd);
  const tranches = optionTranches(source.doc, outstanding.periodEnd);
  const out: Record<string, unknown> = {
    component: "stock_options",
    outstanding: outstanding.value,
    exercisable: exercisable ? exercisable.value : null,
    weightedAverageExercisePrice: strike ? strike.value : null,
    strikeUnit: strike ? strike.unit : null,
    source: sourceRef(source, outstanding.periodEnd),
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
    out.incrementalShares = round(treasuryStock(outstanding.value, strike.value, price));
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

function warrantsComponent(sources: IxSource[], price: number): Record<string, unknown> | null {
  const found = findInSources(sources, (doc) => {
    const concept = WARRANT_COUNT_CONCEPTS.find((c) => doc.facts.some((f) => f.local === c && f.value != null));
    const facts = doc.facts.filter((f) => f.local === concept && f.value != null);
    if (facts.length === 0) return null;
    const groups = new Map<string, IxFact>();
    for (const f of facts) {
      const key = dimsKey(f.dims);
      const prev = groups.get(key);
      if (!prev || (f.periodEnd ?? "") > (prev.periodEnd ?? "")) groups.set(key, f);
    }
    const dimmed = [...groups.entries()].filter(([key]) => key !== "");
    return dimmed.length > 0 ? dimmed.map(([, f]) => f) : [groups.get("")!];
  });
  if (!found) return null;
  const { value: facts, source } = found;
  const unvestedFacts = source.doc.facts.filter((g) => WARRANT_UNVESTED_RE.test(g.local) && isShareCount(g));
  const classes = facts.map((f) => {
    const strike = newest(source.doc.facts.filter((g) => g.local === WARRANT_STRIKE && g.value != null && dimsKey(g.dims) === dimsKey(f.dims)));
    const label = hasDims(f) ? Object.values(f.dims).map(memberLabel).join(" / ") : "Warrants (not itemized)";
    // Only vested warrant shares can be exercised now; the rest count in the gross total.
    const unvested = newest(unvestedFacts.filter((g) => dimsKey(g.dims) === dimsKey(f.dims)))
      ?? (facts.length === 1 ? newest(unvestedFacts) : null);
    const exercisable = unvested ? Math.max(0, f.value! - unvested.value!) : f.value!;
    return {
      class: label,
      concept: f.name,
      outstanding: f.value,
      asOf: f.periodEnd,
      unvested: unvested ? unvested.value : null,
      unvestedAsOf: unvested ? unvested.periodEnd : null,
      exercisable,
      exercisePrice: strike ? strike.value : null,
      inTheMoney: strike ? price > strike.value! : null,
      incrementalShares: strike ? round(treasuryStock(exercisable, strike.value!, price)) : null,
    };
  });
  const unresolved = classes.filter((c) => c.incrementalShares == null).length;
  return {
    component: "warrants",
    outstanding: classes.reduce((sum, c) => sum + (c.outstanding ?? 0), 0),
    classes,
    method: "treasury_stock_per_class_on_vested",
    incrementalShares: unresolved === classes.length ? null : classes.reduce((sum, c) => sum + (c.incrementalShares ?? 0), 0),
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

function groupText(group: DebtGroup, local: string): string | null {
  const hits = group.facts.filter((f) => f.local === local && f.text != null);
  const best = newest(hits);
  return best ? best.text : null;
}

function convertiblesComponent(sources: IxSource[], price: number): Record<string, unknown> | null {
  const found = findInSources(sources, (doc) => {
    const groups = debtGroups(doc).filter((g) => groupValue(g, [CONVERSION_PRICE, CONVERSION_RATIO]) != null);
    if (groups.length > 0) return groups;
    const plainPrice = total(doc, CONVERSION_PRICE) ?? total(doc, CONVERSION_RATIO);
    if (!plainPrice) return null;
    return [{ key: "", axis: null, member: null, facts: doc.facts.filter((f) => !hasDims(f)) }];
  });
  if (!found) return null;
  const { value: groups, source } = found;
  const instruments = groups.map((g) => {
    const faceTagged = groupValue(g, [FACE_AMOUNT]);
    // Some issuers tag each issue's principal only under a carrying-amount
    // concept, often at the issue date; use it and say so.
    const face = faceTagged ?? groupValue(g, PRINCIPAL_FALLBACK_CONCEPTS);
    const convPrice = groupValue(g, [CONVERSION_PRICE]);
    const ratio = groupValue(g, [CONVERSION_RATIO]);
    const impliedPrice = convPrice ? convPrice.value : (ratio && ratio.value > 0 ? 1000 / ratio.value : null);
    let shares: number | null = null;
    let basis: string | null = null;
    if (face && ratio && ratio.value > 0) {
      shares = (face.value / 1000) * ratio.value;
      basis = "principal / 1000 * conversion_ratio";
    } else if (face && convPrice && convPrice.value > 0) {
      shares = face.value / convPrice.value;
      basis = "principal / conversion_price";
    }
    const inTheMoney = impliedPrice != null ? price >= impliedPrice : null;
    return {
      instrument: g.member ? memberLabel(g.member) : "Convertible notes (not itemized)",
      member: g.member,
      faceAmount: faceTagged ? faceTagged.value : null,
      principal: face ? face.value : null,
      principalConcept: face ? face.concept : null,
      principalDate: face ? face.periodEnd : null,
      principalBasis: faceTagged ? "face_amount" : (face ? "tagged_amount_fallback" : null),
      conversionPrice: convPrice ? convPrice.value : (impliedPrice != null ? round(impliedPrice, 4) : null),
      conversionPriceBasis: convPrice ? "tagged" : (impliedPrice != null ? "1000 / conversion_ratio" : null),
      conversionRatioPer1000: ratio ? ratio.value : null,
      maturityDate: normalizeIxDate(groupText(g, "DebtInstrumentMaturityDate")),
      ifConvertedShares: shares != null ? round(shares) : null,
      ifConvertedBasis: basis,
      inTheMoney,
      incrementalShares: shares != null && inTheMoney ? round(shares) : (shares != null ? 0 : null),
    };
  });
  const unresolved = instruments.filter((i) => i.ifConvertedShares == null).length;
  return {
    component: "convertible_debt",
    instruments,
    method: "if_converted_when_in_the_money",
    ifConvertedShares: instruments.reduce((sum, i) => sum + (i.ifConvertedShares ?? 0), 0),
    incrementalShares: unresolved === instruments.length ? null : instruments.reduce((sum, i) => sum + (i.incrementalShares ?? 0), 0),
    unresolvedInstruments: unresolved,
    note: "Principal is the tagged face amount, else the issue's tagged carrying amount (principalBasis says which); either can be the original principal before repurchases. Net-share or cash settlement, capped calls and make-whole adjustments are not modeled.",
    source: sourceRef(source, null),
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

function moneyValue(amount: string, unit: string | undefined): number {
  const base = parseFloat(amount.replace(/,/g, ""));
  const u = (unit ?? "").toLowerCase();
  if (u === "billion" || u === "bn") return base * 1e9;
  if (u === "million" || u === "mm" || u === "m") return base * 1e6;
  if (u === "thousand" || u === "k") return base * 1e3;
  return base;
}

function sentences(text: string): string[] {
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

export type DilutionInput = {
  ticker: string;
  price: number;
  priceCurrency: string;
  asOfDate: string | null;
  sources: IxSource[];
  atmMatches: TextMatch[];
  awardTableMatches?: TextMatch[];
};

/** Basic to diluted shares at a supplied price, from company disclosures only. */
export function dilutionBridge(input: DilutionInput): Record<string, unknown> {
  const { price, sources } = input;
  const primary = sources[0];
  const basicFound = findInSources(sources, basicShares);
  const options = optionsComponent(sources, price);
  const awards = awardsComponent(sources, input.awardTableMatches ?? []);
  const warrants = warrantsComponent(sources, price);
  const convertibles = convertiblesComponent(sources, price);
  const atm = atmComponent(atmEvidence(input.atmMatches), price);
  const components = [options, awards, warrants, convertibles].filter((c): c is Record<string, unknown> => c != null);
  const notDisclosed = [
    options ? null : "stock_options",
    awards ? null : "unvested_share_awards",
    warrants ? null : "warrants",
    convertibles ? null : "convertible_debt",
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
    reportedEpsDilution: findInSources(sources, reportedEpsDilution)?.value ?? null,
    notDisclosed,
    unresolved,
    partiallyResolved: partial,
  };
  if (!basicFound) {
    out.status = "NOT_FOUND";
    out.bridge = null;
    warnings.push({ code: "BASIC_SHARES_NOT_FOUND", message: "No cover-page or balance-sheet share count was tagged.", severity: "error" });
  } else {
    const basic = basicFound.value.shares as number;
    const inc = (c: Record<string, unknown> | null) => (c && typeof c.incrementalShares === "number" ? c.incrementalShares : 0);
    const diluted = basic + inc(options) + inc(awards) + inc(warrants) + inc(convertibles);
    const gross = basic
      + (options ? Number(options.outstanding ?? 0) : 0)
      + (awards ? Number(awards.unvested ?? 0) : 0)
      + (warrants ? Number(warrants.outstanding ?? 0) : 0)
      + (convertibles ? Number(convertibles.ifConvertedShares ?? 0) : 0);
    const atmShares = atm && typeof atm.potentialShares === "number" ? atm.potentialShares : null;
    out.bridge = {
      basicShares: basic,
      stockOptions: options ? options.incrementalShares : null,
      unvestedShareAwards: awards ? awards.incrementalShares : null,
      warrants: warrants ? warrants.incrementalShares : null,
      convertibleDebt: convertibles ? convertibles.incrementalShares : null,
      dilutedSharesAtPrice: round(diluted),
      dilutionPctAtPrice: basic > 0 ? round(((diluted - basic) / basic) * 100, 2) : null,
      grossSharesAllInstruments: round(gross),
      grossDilutionPct: basic > 0 ? round(((gross - basic) / basic) * 100, 2) : null,
      atmPotentialShares: atmShares,
      dilutedSharesAtPriceWithAtm: atmShares != null ? round(diluted + atmShares) : null,
      formula: "basic + options (treasury stock) + unvested awards (gross) + warrants (treasury stock) + convertibles (if-converted when in the money)",
    };
    out.status = unresolved.length > 0 || partial.length > 0 ? "PARTIAL" : "COMPUTED";
  }
  out.methodology = [
    "Every count is a company disclosure tagged in the filing's inline XBRL; the only external input is the price you supplied.",
    "This is a mechanical bridge, not a consensus or forecast diluted share count, and must not be back-solved into one.",
    "A component missing from notDisclosed was not tagged in the filing; that is not proof the instrument does not exist.",
  ];
  if (notDisclosed.length > 0 || unresolved.length > 0) {
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
    const maturityDate = normalizeIxDate(groupText(g, "DebtInstrumentMaturityDate"));
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
      carryingAmount: carrying ? carrying.value : null,
      carryingAmountConcept: carrying ? carrying.concept : null,
      taggedAmount: tagged ? tagged.value : null,
      taggedAmountDate: tagged ? tagged.periodEnd : null,
      taggedAmountConcept: tagged ? tagged.concept : null,
      couponPct: coupon ? round(coupon.value * 100, 4) : null,
      effectiveRatePct: effective ? round(effective.value * 100, 4) : null,
      maturityDate,
      convertible: convPrice != null || ratio != null,
      conversionPrice: convPrice ? convPrice.value : null,
      conversionRatioPer1000: ratio ? ratio.value : null,
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
};

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
  const byYear = new Map<string, { year: string; faceAmount: number; instruments: string[] }>();
  for (const row of instrumentRows) {
    const maturity = row.maturityDate as string | null;
    if (!maturity || row.status === "matured_before_period_end") continue;
    const year = maturity.slice(0, 4);
    const entry = byYear.get(year) ?? { year, faceAmount: 0, instruments: [] };
    entry.faceAmount += Number(row.faceAmount ?? row.carryingAmount ?? row.taggedAmount ?? 0);
    entry.instruments.push(String(row.instrument));
    byYear.set(year, entry);
  }
  const instrumentLadder = [...byYear.values()].sort((a, b) => (a.year < b.year ? -1 : a.year > b.year ? 1 : 0));
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
    fundingStatements: funding,
    methodology: [
      "Balances are the filing's own tagged values at its period end; nothing is projected forward.",
      "Instrument rows come from facts dimensioned by debt instrument, which companyfacts omits; face amounts can be original principal.",
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
export function analystValuationMethods(ticker: string, items: NewsItem[], changes: RatingChange[]): Record<string, unknown> {
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
    for (const sentence of sentences(text)) {
      const methods = sentenceMethods(sentence);
      const target = priceTarget(sentence);
      if (methods.length === 0 && !(target && firm)) continue;
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
