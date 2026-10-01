/**
 * Funding and capex schedule (2.5.2), shared with yfmcp/funding_schedule.py
 * (parity in scripts/test_funding_schedule.py). Pure.
 *
 * Every amount keeps its classification, timing and source:
 * - CONTRACTUAL: obligations the filing tags by due period (debt principal,
 *   lease payments, purchase and other contractual obligations);
 * - COMPANY_DISCLOSED_COMMITTED: amounts the company says it has committed
 *   or is obligated to spend, stated in text;
 * - COMPANY_GUIDED: amounts the company expects, plans or budgets;
 * - AWARDED_CONTINGENT: grants, awards, incentives and milestone payments
 *   that depend on conditions;
 * - UNRESOLVED: a funding or capex statement whose amount or type cannot be
 *   classified from its wording.
 * Liquidity sources (cash, ATM capacity, undrawn facilities) are listed
 * separately; nothing is netted, forecast or filled in.
 */

import { collapse, memberLabel, moneyValue, sentences } from "./capital-structure.js";
import { AUTHORITY_BOUNDARY } from "./evidence.js";

type Rec = Record<string, unknown>;

export interface PlainFact {
  local: string;
  value: number | null;
  periodEnd: string | null;
  dims: Record<string, string>;
  unit: string | null;
}

export interface Statement {
  contextText: string;
  sectionHeading: string | null;
  documentUrl: string | null;
  filingDate: string | null;
  accessionNumber: string | null;
}

export const CLASSIFICATIONS: Record<string, string> = {
  CONTRACTUAL: "Obligations the filing tags by due period: debt principal, lease payments, purchase and other contractual obligations.",
  COMPANY_DISCLOSED_COMMITTED: "Amounts the company states it has committed or is obligated to spend, from filing text.",
  COMPANY_GUIDED: "Amounts the company expects, plans, estimates or budgets; forward-looking and not binding.",
  AWARDED_CONTINGENT: "Grants, awards, incentives and milestone payments that depend on conditions being met.",
  UNRESOLVED: "A funding or capex statement whose amount or type cannot be classified from its wording.",
};

// [concept, bucket, years after period end (0 = rest of the fiscal year, 6 = after year five)]
type Bucket = [string, string, number];
const FAMILIES: { family: string; buckets: Bucket[]; totals: string[] }[] = [
  {
    family: "debt_principal",
    buckets: [
      ["LongTermDebtMaturitiesRepaymentsOfPrincipalRemainderOfFiscalYear", "remainder_of_fiscal_year", 0],
      ["LongTermDebtMaturitiesRepaymentsOfPrincipalInNextTwelveMonths", "next_12_months", 1],
      ["LongTermDebtMaturitiesRepaymentsOfPrincipalInYearTwo", "year_2", 2],
      ["LongTermDebtMaturitiesRepaymentsOfPrincipalInYearThree", "year_3", 3],
      ["LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFour", "year_4", 4],
      ["LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFive", "year_5", 5],
      ["LongTermDebtMaturitiesRepaymentsOfPrincipalAfterYearFive", "after_year_5", 6],
    ],
    totals: [],
  },
  {
    family: "operating_lease_payments",
    buckets: [
      ["LesseeOperatingLeaseLiabilityPaymentsRemainderOfFiscalYear", "remainder_of_fiscal_year", 0],
      ["LesseeOperatingLeaseLiabilityPaymentsDueNextTwelveMonths", "next_12_months", 1],
      ["LesseeOperatingLeaseLiabilityPaymentsDueYearTwo", "year_2", 2],
      ["LesseeOperatingLeaseLiabilityPaymentsDueYearThree", "year_3", 3],
      ["LesseeOperatingLeaseLiabilityPaymentsDueYearFour", "year_4", 4],
      ["LesseeOperatingLeaseLiabilityPaymentsDueYearFive", "year_5", 5],
      ["LesseeOperatingLeaseLiabilityPaymentsDueAfterYearFive", "after_year_5", 6],
    ],
    totals: ["LesseeOperatingLeaseLiabilityPaymentsDue"],
  },
  {
    family: "finance_lease_payments",
    buckets: [
      ["FinanceLeaseLiabilityPaymentsRemainderOfFiscalYear", "remainder_of_fiscal_year", 0],
      ["FinanceLeaseLiabilityPaymentsDueNextTwelveMonths", "next_12_months", 1],
      ["FinanceLeaseLiabilityPaymentsDueYearTwo", "year_2", 2],
      ["FinanceLeaseLiabilityPaymentsDueYearThree", "year_3", 3],
      ["FinanceLeaseLiabilityPaymentsDueYearFour", "year_4", 4],
      ["FinanceLeaseLiabilityPaymentsDueYearFive", "year_5", 5],
      ["FinanceLeaseLiabilityPaymentsDueAfterYearFive", "after_year_5", 6],
    ],
    totals: ["FinanceLeaseLiabilityPaymentsDue"],
  },
  {
    family: "purchase_obligations",
    buckets: [
      ["PurchaseObligationDueInNextTwelveMonths", "next_12_months", 1],
      ["PurchaseObligationDueInSecondYear", "year_2", 2],
      ["PurchaseObligationDueInThirdYear", "year_3", 3],
      ["PurchaseObligationDueInFourthYear", "year_4", 4],
      ["PurchaseObligationDueInFifthYear", "year_5", 5],
      ["PurchaseObligationDueAfterFifthYear", "after_year_5", 6],
      ["UnrecordedUnconditionalPurchaseObligationBalanceOnFirstAnniversary", "next_12_months", 1],
      ["UnrecordedUnconditionalPurchaseObligationBalanceOnSecondAnniversary", "year_2", 2],
      ["UnrecordedUnconditionalPurchaseObligationBalanceOnThirdAnniversary", "year_3", 3],
      ["UnrecordedUnconditionalPurchaseObligationBalanceOnFourthAnniversary", "year_4", 4],
      ["UnrecordedUnconditionalPurchaseObligationBalanceOnFifthAnniversary", "year_5", 5],
      ["UnrecordedUnconditionalPurchaseObligationDueAfterFiveYears", "after_year_5", 6],
    ],
    totals: ["PurchaseObligation", "UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount"],
  },
  {
    family: "contractual_obligations",
    buckets: [
      ["ContractualObligationDueInNextTwelveMonths", "next_12_months", 1],
      ["ContractualObligationDueInSecondYear", "year_2", 2],
      ["ContractualObligationDueInThirdYear", "year_3", 3],
      ["ContractualObligationDueInFourthYear", "year_4", 4],
      ["ContractualObligationDueInFifthYear", "year_5", 5],
      ["ContractualObligationDueAfterFifthYear", "after_year_5", 6],
    ],
    totals: ["ContractualObligation"],
  },
];

const PURCHASE_COMMITMENT = "LongTermPurchaseCommitmentAmount";
const UNDRAWN_FACILITY = "LineOfCreditFacilityRemainingBorrowingCapacity";

function addYears(date: string, years: number): string {
  return `${parseInt(date.slice(0, 4), 10) + years}${date.slice(4)}`;
}

function plainAt(facts: PlainFact[], local: string, at: string | null): PlainFact | null {
  const hits = facts.filter((f) => f.local === local && f.value != null && Object.keys(f.dims).length === 0 && (at == null || f.periodEnd === at));
  return hits.length > 0 ? hits[hits.length - 1] : null;
}

// ── Text classification ──────────────────────────────────────────────────────

const MONEY_RE = /(?:US)?\$\s?(\d[\d,]*(?:\.\d+)?)\s*(billion|million|thousand|bn|mm|m|k)?\b/gi;
const EQUITY_AWARD_RE = /\b(?:stock|share|equity|RSU|option|incentive)[- ](?:based )?(?:awards?|compensation|grants?)\b|\brestricted stock\b|\bRSUs?\b|\bPSUs?\b|\bvest(?:s|ed|ing)?\b/i;
// Without an amount, a sentence is kept only when it names a funding or capex obligation outright.
const STRONG_RE = /\bcapital expenditures?\b|\bcapex\b|\bpurchase (?:commitments?|obligations?)\b|\bcommitments?\b|\bcommitted\b|\bnon-?cancell?able\b|\bobligated\b|\bgrants?\b|\bmilestones?\b|\bCHIPS\b|\bincentive agreement\b/i;
const CONTINGENT_RE = /\bgrants?\b|\bawarded\b|\baward agreement\b|\bCHIPS\b|\bincentives?\b|\bsubsid(?:y|ies)\b|\bmilestones?\b|\bpreliminary memorandum of terms\b|\bcontingent\b|\bsubject to (?:the )?(?:achievement|completion|satisfaction|conditions?|approval)\b/i;
const COMMITTED_RE = /\bcommitted\b|\bcommitments?\b|\bnon-?cancell?able\b|\bobligated\b|\bfirm purchase\b/i;
const GUIDED_RE = /\bexpects?\b|\bexpected\b|\banticipates?\b|\banticipated\b|\bplans?\b|\bplanned\b|\bintends?\b|\bestimates?\b|\bestimated\b|\bprojects?\b|\bbudget(?:s|ed)?\b|\bguidance\b|\bforecasts?\b/i;
const CAPEX_RE = /\bcapital expenditures?\b|\bcapex\b|\bconstruction\b|\bpurchase (?:commitments?|obligations?)\b|\binvest(?:ment)?s? (?:of|in)\b|\bspend(?:ing)?\b|\bfacilit(?:y|ies)\b|\bmanufactur\w*\b|\bsatellites?\b|\bdeploy\w*\b/i;
const FUNDING_RE = /\bfund(?:s|ed|ing)?\b|\bfinanc\w*|\bliquidity\b|\bcash\b|\bproceeds\b/i;
const RANGE_JOIN_RE = /^\s*(?:to|-|–|—|and)\s*$/i;

interface Amount {
  low: number | null;
  high: number | null;
  qualifier: string | null;
  amountStatus: "STATED" | "SCALE_NOT_STATED" | "NOT_STATED";
  asWritten: string | null;
}

const PER_UNIT_RE = /^\s*per (?:share|unit|warrant|note)\b/i;

/**
 * The first dollar amount or range in a sentence. A figure below $100,000
 * with no scale word ("$550.0" in a filing that reports in millions) keeps
 * its wording and is SCALE_NOT_STATED rather than read at face value.
 */
function amounts(sentence: string): Amount {
  const matches = [...sentence.matchAll(MONEY_RE)]
    .map((m) => ({ index: m.index ?? 0, end: (m.index ?? 0) + m[0].length, text: m[0], amount: m[1], unit: m[2] }))
    .filter((m) => !PER_UNIT_RE.test(sentence.slice(m.end)));
  const none: Amount = { low: null, high: null, qualifier: null, amountStatus: "NOT_STATED", asWritten: null };
  if (matches.length === 0) return none;
  const a = matches[0];
  const b = matches[1];
  // Two amounts are a range only when they run low to high: "committed $1.2 billion and $300 million" is two (2.5.20).
  const isRange = b != null && RANGE_JOIN_RE.test(sentence.slice(a.end, b.index)) && moneyValue(a.amount, a.unit ?? b.unit) <= moneyValue(b.amount, b.unit ?? a.unit);
  const unit = isRange ? (a.unit ?? b.unit) : a.unit;
  const low = moneyValue(a.amount, unit);
  const high = isRange ? moneyValue(b.amount, b.unit ?? unit) : low;
  const asWritten = (isRange ? sentence.slice(a.index, b.end) : a.text).trim();
  const before = sentence.slice(Math.max(0, a.index - 24), a.index).toLowerCase();
  const qualifier = /up to\s*$/.test(before) ? "up_to" : /(?:approximately|about|around|roughly|~)\s*$/.test(before) ? "approximately" : low !== high ? "range" : "stated";
  if (unit == null && high < 100000) return { low: null, high: null, qualifier, amountStatus: "SCALE_NOT_STATED", asWritten };
  return { low, high, qualifier, amountStatus: "STATED", asWritten };
}

const ORDINAL: Record<string, number> = { first: 1, second: 2, third: 3, fourth: 4 };

/** When a sentence says the amount falls due or is spent; UNSTATED when it does not. */
export function timingOf(sentence: string): Rec {
  let m = /\b(first|second|third|fourth) quarter of (?:fiscal )?(20\d\d)\b/i.exec(sentence);
  if (m) return { basis: "TEXT", horizon: "quarter", year: Number(m[2]), quarter: ORDINAL[m[1].toLowerCase()], phrase: m[0] };
  m = /\bQ([1-4])\s?(?:FY)?(20\d\d)\b/.exec(sentence);
  if (m) return { basis: "TEXT", horizon: "quarter", year: Number(m[2]), quarter: Number(m[1]), phrase: m[0] };
  m = /\bnext (?:12|twelve) months\b/i.exec(sentence);
  if (m) return { basis: "TEXT", horizon: "next_12_months", phrase: m[0] };
  m = /\b(?:remainder|rest) of (?:the )?(?:fiscal )?(?:year|20\d\d)\b/i.exec(sentence);
  if (m) return { basis: "TEXT", horizon: "remainder_of_fiscal_year", phrase: m[0] };
  m = /\b(?:fiscal )?(20\d\d) (?:and|through|to|-) (?:fiscal )?(20\d\d)\b/i.exec(sentence);
  if (m) return { basis: "TEXT", horizon: "years", fromYear: Number(m[1]), toYear: Number(m[2]), phrase: m[0] };
  m = /\b(?:through|by|until) (?:the end of )?(?:fiscal )?(?:year )?(20\d\d)\b/i.exec(sentence);
  if (m) return { basis: "TEXT", horizon: "through_year", year: Number(m[1]), phrase: m[0] };
  m = /\b(?:in|during|for) (?:the )?(?:full )?(?:fiscal )?(?:year )?(20\d\d)\b/i.exec(sentence);
  if (m) return { basis: "TEXT", horizon: "year", year: Number(m[1]), phrase: m[0] };
  return { basis: "UNSTATED" };
}

/** A funding or capex sentence's classification, or null when it speaks to neither. */
export function classifySentence(sentence: string): { classification: string; category: string } | null {
  const contingent = CONTINGENT_RE.test(sentence) && !EQUITY_AWARD_RE.test(sentence);
  const capex = CAPEX_RE.test(sentence);
  const committed = COMMITTED_RE.test(sentence);
  if (!contingent && !capex && !committed) return null;
  if (!contingent && !committed && !FUNDING_RE.test(sentence) && !/\$\s?\d/.test(sentence)) return null;
  const category = contingent ? "award_or_incentive" : capex ? "capital_expenditure" : "commitment";
  if (contingent) return { classification: "AWARDED_CONTINGENT", category };
  // A stated commitment stays a commitment even when the sentence also uses a forward-looking word.
  if (committed) return { classification: "COMPANY_DISCLOSED_COMMITTED", category };
  if (GUIDED_RE.test(sentence)) return { classification: "COMPANY_GUIDED", category };
  return { classification: "UNRESOLVED", category };
}

function textItems(statements: Statement[], limit: number): Rec[] {
  const out: Rec[] = [];
  const seen = new Set<string>();
  for (const st of statements) {
    for (const sentence of sentences(collapse(st.contextText))) {
      // A sentence ending in ";" is an item of a list, usually forward-looking boilerplate.
      if (sentence.length < 40 || sentence.length > 1200 || sentence.endsWith(";")) continue;
      const cls = classifySentence(sentence);
      if (!cls) continue;
      const amt = amounts(sentence);
      if (amt.amountStatus === "NOT_STATED" && !STRONG_RE.test(sentence)) continue;
      const key = sentence.slice(0, 200).toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      const classification = amt.amountStatus === "NOT_STATED" ? "UNRESOLVED" : cls.classification;
      out.push({
        classification,
        statedType: cls.classification,
        category: cls.category,
        amountLow: amt.low,
        amountHigh: amt.high,
        amountQualifier: amt.qualifier,
        amountStatus: amt.amountStatus,
        amountAsWritten: amt.asWritten,
        currency: amt.amountStatus === "NOT_STATED" ? null : "USD",
        timing: timingOf(sentence),
        unresolvedReason: classification === "UNRESOLVED" ? (amt.amountStatus === "NOT_STATED" ? "no amount stated" : "type not stated") : null,
        evidence: {
          sentence: sentence.slice(0, 600),
          sectionHeading: st.sectionHeading,
          documentUrl: st.documentUrl,
          filingDate: st.filingDate,
          accessionNumber: st.accessionNumber,
        },
      });
      if (out.length >= limit) return out;
    }
  }
  return out;
}

// ── Schedule ─────────────────────────────────────────────────────────────────

export function fundingCapexSchedule(input: {
  ticker: string;
  periodEnd: string | null;
  source: Rec | null;
  facts: PlainFact[];
  statements: Statement[];
  balances: Rec | null;
  atmRemainingUsd: number | null;
  atmEvidence: Rec | null;
}): Rec {
  const { facts, periodEnd } = input;
  const contractual: Rec[] = [];
  const totals: Rec[] = [];
  for (const fam of FAMILIES) {
    const seenBuckets = new Set<string>();
    for (const [concept, bucket, offset] of fam.buckets) {
      if (seenBuckets.has(bucket)) continue;
      const hit = plainAt(facts, concept, periodEnd);
      if (!hit) continue;
      seenBuckets.add(bucket);
      contractual.push({
        classification: "CONTRACTUAL",
        family: fam.family,
        bucket,
        periodThrough: periodEnd && offset > 0 && offset < 6 ? addYears(periodEnd, offset) : null,
        amount: hit.value,
        unit: hit.unit,
        concept,
        periodEnd: hit.periodEnd,
      });
    }
    for (const concept of fam.totals) {
      const hit = plainAt(facts, concept, periodEnd);
      if (hit) totals.push({ family: fam.family, concept, amount: hit.value, unit: hit.unit, periodEnd: hit.periodEnd });
    }
  }
  // Purchase commitments per category; two values for one category are the low and high of a stated range.
  const commitmentGroups = new Map<string, PlainFact[]>();
  for (const f of facts) {
    if (f.local !== PURCHASE_COMMITMENT || f.value == null || (periodEnd && f.periodEnd !== periodEnd)) continue;
    // A range axis (srt:RangeAxis Minimum/Maximum) splits one commitment into its low and high.
    const key = Object.entries(f.dims).filter(([a]) => !/RangeAxis$/.test(a)).sort().map(([a, m]) => `${a}=${m}`).join("&");
    commitmentGroups.set(key, [...(commitmentGroups.get(key) ?? []), f]);
  }
  for (const group of commitmentGroups.values()) {
    const values = [...new Set(group.map((f) => f.value as number))].sort((a, b) => a - b);
    const members = Object.entries(group[0].dims).filter(([a]) => !/RangeAxis$/.test(a)).map(([, m]) => m);
    contractual.push({
      classification: "CONTRACTUAL",
      family: "purchase_commitment",
      bucket: "unstated",
      periodThrough: null,
      amount: values.length === 1 ? values[0] : null,
      ...(values.length > 1 ? { amountLow: values[0], amountHigh: values[values.length - 1], amountBasis: "tagged_range" } : {}),
      unit: group[0].unit,
      concept: PURCHASE_COMMITMENT,
      counterpartyOrCategory: members.length > 0 ? members.map(memberLabel).join(" / ") : null,
      periodEnd: group[0].periodEnd,
    });
  }

  const items = textItems(input.statements, 25);
  const byClassification: Record<string, number> = {};
  for (const key of Object.keys(CLASSIFICATIONS)) byClassification[key] = 0;
  for (const row of [...contractual, ...items]) byClassification[String(row.classification)] += 1;

  const liquidity: Rec[] = [];
  const bal = input.balances ?? {};
  for (const [key, label] of [["cashAndEquivalents", "cash_and_equivalents"], ["shortTermInvestments", "short_term_investments"]] as const) {
    if (typeof bal[key] === "number") liquidity.push({ source: label, classification: "COMPANY_DISCLOSED_BALANCE", amount: bal[key], asOf: periodEnd });
  }
  const undrawn = plainAt(facts, UNDRAWN_FACILITY, periodEnd);
  if (undrawn) liquidity.push({ source: "undrawn_credit_facility", classification: "CONTRACTUAL_AVAILABILITY", amount: undrawn.value, asOf: undrawn.periodEnd, concept: UNDRAWN_FACILITY });
  if (input.atmRemainingUsd != null) {
    liquidity.push({ source: "atm_remaining_capacity", classification: "AVAILABLE_AT_COMPANY_DISCRETION", amount: input.atmRemainingUsd, asOf: null, evidence: input.atmEvidence });
  }

  const warnings: Rec[] = [];
  if (facts.length === 0) warnings.push({ code: "NO_INLINE_XBRL", message: "The filing carries no inline XBRL facts; contractual schedules need tagged due periods.", severity: "warning" });
  if (contractual.length === 0 && facts.length > 0) warnings.push({ code: "NO_TAGGED_SCHEDULES", message: "No debt, lease, purchase or contractual obligation is tagged by due period at the period end.", severity: "info" });

  return {
    ticker: input.ticker.toUpperCase(),
    status: contractual.length > 0 || items.length > 0 ? "COMPUTED" : "NOT_FOUND",
    basis: "COMPANY_DISCLOSED",
    periodEnd,
    source: input.source,
    classifications: CLASSIFICATIONS,
    contractual,
    contractualTotals: totals,
    textItems: items,
    byClassification,
    liquiditySources: liquidity,
    notes: [
      "Amounts are reported as the filing states them, each with its classification, timing and source; nothing is netted, summed across classifications, forecast or filled in.",
      "A classification of text is by the sentence's own wording; UNRESOLVED means the amount or type could not be read from it, not that nothing is owed.",
      "Liquidity sources are availability, not commitments: an ATM is capacity at the company's discretion.",
    ],
    warnings,
    ...AUTHORITY_BOUNDARY,
  };
}
