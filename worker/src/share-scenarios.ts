/**
 * Caller-parameterized share-count scenarios (2.5.2), shared with
 * yfmcp/share_scenarios.py (parity in scripts/test_share_scenarios.py). Pure.
 *
 * The instrument inventory is the dilution bridge's (company-disclosed inline
 * XBRL); each scenario applies the caller's price and treatments to it. Every
 * instrument line reports its treatment, whether it is included, the
 * treasury-stock or if-converted mechanics, and why it is unresolved when it
 * is. Scenarios are reported side by side: MCP never selects a denominator.
 */

import { AUTHORITY_BOUNDARY } from "./evidence.js";

type Rec = Record<string, unknown>;

export const SHARE_SCENARIO_LIMIT = 8;
const KNOWN_ISSUANCE_LIMIT = 10;

export const TREATMENTS = {
  options: ["treasury_stock", "gross", "exclude"],
  unvested_awards: ["gross", "exclude"],
  warrants: ["treasury_stock", "gross", "exclude"],
  warrant_vesting: ["vested_only", "all"],
  convertibles: ["if_converted_when_in_the_money", "if_converted_all", "net_share_settlement_when_stated", "exclude"],
  atm: ["exclude", "full_remaining_capacity"],
  capped_calls: ["ignore", "offset_when_stated"],
} as const;

type TreatmentKey = keyof typeof TREATMENTS;

const DEFAULTS: Record<TreatmentKey, string> = {
  options: "treasury_stock",
  unvested_awards: "gross",
  warrants: "treasury_stock",
  warrant_vesting: "vested_only",
  convertibles: "if_converted_when_in_the_money",
  atm: "exclude",
  capped_calls: "ignore",
};

export interface KnownIssuance {
  label: string;
  shares: number;
  source: string | null;
}

export interface ShareScenario {
  name: string;
  price: number;
  treatments: Record<TreatmentKey, string>;
  defaulted: TreatmentKey[];
  knownIssuance: KnownIssuance[];
}

function round(value: number, digits = 0): number {
  const f = 10 ** digits;
  return Math.round(value * f) / f;
}

function num(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/** Validate caller scenarios; every treatment not supplied takes the documented default and is listed as defaulted. */
export function parseShareScenarios(raw: unknown): { scenarios: ShareScenario[] } | { error: string } {
  if (!Array.isArray(raw) || raw.length === 0) return { error: "scenarios must be a non-empty array." };
  if (raw.length > SHARE_SCENARIO_LIMIT) return { error: `At most ${SHARE_SCENARIO_LIMIT} scenarios per call.` };
  const names = new Set<string>();
  const scenarios: ShareScenario[] = [];
  for (const [i, item] of raw.entries()) {
    if (!item || typeof item !== "object" || Array.isArray(item)) return { error: `scenarios[${i}] must be an object.` };
    const s = item as Rec;
    const name = typeof s.name === "string" ? s.name.trim() : "";
    if (!name || name.length > 60) return { error: `scenarios[${i}].name must be 1 to 60 characters.` };
    if (names.has(name)) return { error: `Scenario name '${name}' is repeated.` };
    names.add(name);
    const price = num(s.price);
    if (price == null || price <= 0) return { error: `scenarios[${i}].price must be a positive number.` };
    const treatments = {} as Record<TreatmentKey, string>;
    const defaulted: TreatmentKey[] = [];
    for (const key of Object.keys(TREATMENTS) as TreatmentKey[]) {
      const value = s[key];
      if (value == null) {
        treatments[key] = DEFAULTS[key];
        defaulted.push(key);
      } else if (typeof value === "string" && (TREATMENTS[key] as readonly string[]).includes(value)) {
        treatments[key] = value;
      } else {
        return { error: `scenarios[${i}].${key} must be one of ${TREATMENTS[key].join(", ")}.` };
      }
    }
    const issuance = s.known_issuance ?? [];
    if (!Array.isArray(issuance) || issuance.length > KNOWN_ISSUANCE_LIMIT) {
      return { error: `scenarios[${i}].known_issuance must be an array of at most ${KNOWN_ISSUANCE_LIMIT} items.` };
    }
    const knownIssuance: KnownIssuance[] = [];
    for (const [j, row] of issuance.entries()) {
      const r = (row && typeof row === "object" ? row : {}) as Rec;
      const label = typeof r.label === "string" ? r.label.trim() : "";
      const shares = num(r.shares);
      if (!label || shares == null || shares <= 0) return { error: `scenarios[${i}].known_issuance[${j}] needs a label and positive shares.` };
      knownIssuance.push({ label: label.slice(0, 120), shares, source: typeof r.source === "string" ? r.source.slice(0, 300) : null });
    }
    scenarios.push({ name, price, treatments, defaulted, knownIssuance });
  }
  return { scenarios };
}

interface Line {
  component: string;
  instrument: string;
  treatment: string;
  count: number | null;
  countBasis: string;
  exercisePrice: number | null;
  price: number;
  inTheMoney: boolean | null;
  thresholdPrice: number | null;
  method: string;
  incrementalShares: number | null;
  included: boolean;
  unresolvedReason: string | null;
}

function line(partial: Omit<Line, "included" | "unresolvedReason"> & { unresolvedReason?: string | null }): Line {
  const unresolvedReason = partial.unresolvedReason ?? null;
  return { ...partial, unresolvedReason, included: partial.treatment !== "exclude" && unresolvedReason == null };
}

function treasury(count: number, strike: number, price: number): number {
  return price > strike ? count * (1 - strike / price) : 0;
}

/** One exercisable instrument (option tranche or warrant class) under a treatment. */
function exercisable(component: string, instrument: string, count: number | null, countBasis: string, strike: number | null, treatment: string, price: number): Line {
  const base = { component, instrument, treatment, count, countBasis, exercisePrice: strike, price };
  if (treatment === "exclude") {
    return line({ ...base, inTheMoney: strike != null ? price > strike : null, thresholdPrice: null, method: "excluded by scenario", incrementalShares: 0 });
  }
  if (count == null) return line({ ...base, inTheMoney: null, thresholdPrice: null, method: treatment, incrementalShares: null, unresolvedReason: "count not tagged" });
  if (treatment === "gross") {
    return line({ ...base, inTheMoney: strike != null ? price > strike : null, thresholdPrice: null, method: "gross: every share counted", incrementalShares: round(count) });
  }
  if (strike == null) return line({ ...base, inTheMoney: null, thresholdPrice: null, method: "treasury_stock", incrementalShares: null, unresolvedReason: "exercise price not tagged" });
  return line({
    ...base,
    inTheMoney: price > strike,
    thresholdPrice: strike,
    method: "treasury_stock: count x (1 - exercise price / price) when price > exercise price",
    incrementalShares: round(treasury(count, strike, price)),
  });
}

function scenarioLines(bridge: Rec, s: ShareScenario): Line[] {
  const lines: Line[] = [];
  const components = (Array.isArray(bridge.components) ? bridge.components : []) as Rec[];
  const byName = (name: string) => components.find((c) => c.component === name) ?? null;
  const t = s.treatments;
  const price = s.price;

  const options = byName("stock_options");
  if (options) {
    const tranches = Array.isArray(options.tranches) ? options.tranches as Rec[] : [];
    if (tranches.length > 0) {
      for (const tr of tranches) {
        lines.push(exercisable("stock_options", `Options ${String(tr.range ?? "")}`.trim(), num(tr.outstanding), "outstanding_in_exercise_price_range", num(tr.weightedAverageExercisePrice), t.options, price));
      }
    } else {
      lines.push(exercisable("stock_options", "Options (weighted-average exercise price)", num(options.outstanding), "outstanding", num(options.weightedAverageExercisePrice), t.options, price));
    }
  }

  const awards = byName("unvested_share_awards");
  if (awards) {
    const count = num(awards.unvested);
    const base = {
      component: "unvested_share_awards", instrument: "Unvested RSUs/PSUs", treatment: t.unvested_awards, count,
      countBasis: String(awards.countBasis ?? "nonvested"), exercisePrice: null, price, inTheMoney: null, thresholdPrice: null,
    };
    if (t.unvested_awards === "exclude") lines.push(line({ ...base, method: "excluded by scenario", incrementalShares: 0 }));
    else if (count == null) lines.push(line({ ...base, method: "gross", incrementalShares: null, unresolvedReason: "count not tagged" }));
    else lines.push(line({ ...base, method: "gross: every unvested award counted", incrementalShares: round(count) }));
  }

  const warrants = byName("warrants");
  if (warrants) {
    for (const cls of (Array.isArray(warrants.classes) ? warrants.classes : []) as Rec[]) {
      const all = t.warrant_vesting === "all";
      // A class that vests on untagged conditions has no known exercisable count; it is never taken as all outstanding (2.5.9).
      const count = all ? num(cls.outstanding) : cls.exercisable === undefined ? num(cls.outstanding) : num(cls.exercisable);
      lines.push(exercisable("warrants", String(cls.class ?? "Warrants"), count, all ? "outstanding" : "vested_exercisable", num(cls.exercisePrice), t.warrants, price));
    }
  }

  // Convertible notes and convertible preferred stock (2.5.9) take the same if-converted treatment.
  for (const name of ["convertible_debt", "convertible_preferred"]) {
    const convertibles = byName(name);
    if (!convertibles) continue;
    for (const inst of (Array.isArray(convertibles.instruments) ? convertibles.instruments : []) as Rec[]) {
      const shares = num(inst.ifConvertedShares);
      const conv = num(inst.conversionPrice);
      const base = {
        component: name, instrument: String(inst.instrument ?? "Convertible notes"), treatment: t.convertibles,
        count: shares, countBasis: String(inst.ifConvertedBasis ?? "if_converted"), exercisePrice: conv, price,
      };
      const itm = conv != null ? price >= conv : null;
      if (t.convertibles === "exclude") {
        lines.push(line({ ...base, inTheMoney: itm, thresholdPrice: null, method: "excluded by scenario", incrementalShares: 0 }));
      } else if (shares == null) {
        lines.push(line({ ...base, inTheMoney: itm, thresholdPrice: null, method: t.convertibles, incrementalShares: null, unresolvedReason: "principal or conversion terms not tagged" }));
      } else if (t.convertibles === "if_converted_all") {
        lines.push(line({ ...base, inTheMoney: itm, thresholdPrice: null, method: "if_converted: every note converted", incrementalShares: round(shares) }));
      } else if (conv == null) {
        lines.push(line({ ...base, inTheMoney: null, thresholdPrice: null, method: t.convertibles, incrementalShares: null, unresolvedReason: "conversion price not tagged" }));
      } else if (t.convertibles === "net_share_settlement_when_stated" && inst.principalSettlement != null) {
        // The filing states principal is settled in cash: shares only for the conversion value above principal (2.5.11).
        const principal = num(inst.principal);
        if (principal == null) lines.push(line({ ...base, inTheMoney: itm, thresholdPrice: conv, method: t.convertibles, incrementalShares: null, unresolvedReason: "principal not tagged" }));
        else lines.push(line({ ...base, inTheMoney: itm, thresholdPrice: conv, method: "net share settlement: (if-converted shares - principal / price) when price >= conversion price; principal in cash as the filing states", incrementalShares: itm ? round(Math.max(0, shares - principal / price)) : 0 }));
      } else {
        const unstated = t.convertibles === "net_share_settlement_when_stated" ? "; cash settlement of principal not stated" : "";
        lines.push(line({ ...base, inTheMoney: itm, thresholdPrice: conv, method: `if_converted when price >= conversion price${unstated}`, incrementalShares: itm ? round(shares) : 0 }));
      }
    }
  }

  // Capped calls the filing states for convertible notes, netted only when the caller asks (2.5.12).
  if (t.capped_calls === "offset_when_stated") {
    for (const inst of (Array.isArray(byName("convertible_debt")?.instruments) ? byName("convertible_debt")!.instruments : []) as Rec[]) {
      const call = inst.cappedCall && typeof inst.cappedCall === "object" ? inst.cappedCall as Rec : null;
      if (!call) continue;
      const strike = num(call.strikePrice);
      const cap = num(call.capPrice);
      const covered = num(call.coveredShares);
      const base = {
        component: "capped_call", instrument: `${String(inst.instrument ?? "Convertible notes")} capped call`, treatment: t.capped_calls,
        count: covered, countBasis: String(call.coverageBasis ?? "NOT_STATED"), exercisePrice: strike, price,
        inTheMoney: strike != null ? price > strike : null, thresholdPrice: strike,
      };
      if (strike == null || cap == null || covered == null) {
        lines.push(line({ ...base, method: t.capped_calls, incrementalShares: null, unresolvedReason: String(call.unresolvedReason ?? "capped call terms not stated") }));
      } else {
        lines.push(line({ ...base, method: "capped call: minus covered x (min(price, cap) - strike) / price, delivered back to the company (economic, not the EPS count)", incrementalShares: 0 - round((covered * Math.max(0, Math.min(price, cap) - strike)) / price) }));
      }
    }
  }

  const atm = bridge.atmProgram && typeof bridge.atmProgram === "object" ? bridge.atmProgram as Rec : null;
  if (atm) {
    const remaining = num(atm.remainingCapacityUsd);
    const base = {
      component: "atm_program", instrument: "At-the-market program", treatment: t.atm, count: null,
      countBasis: "remaining_capacity_usd", exercisePrice: null, price, inTheMoney: null, thresholdPrice: null,
    };
    if (t.atm === "exclude") lines.push(line({ ...base, method: "excluded by scenario", incrementalShares: 0 }));
    else if (remaining == null) lines.push(line({ ...base, method: "full_remaining_capacity", incrementalShares: null, unresolvedReason: "remaining capacity not stated" }));
    else lines.push(line({ ...base, method: "remaining capacity / price (capacity at the company's discretion, not a plan)", incrementalShares: round(remaining / price) }));
  }
  return lines;
}

/**
 * Share counts under each caller scenario. `bridge` is the dilution bridge
 * payload; only its price-independent inventory is read (counts, exercise and
 * conversion prices, ATM remaining capacity).
 */
export function shareCountScenarios(ticker: string, bridge: Rec, scenarios: ShareScenario[]): Rec {
  const basicRec = (bridge.basicShares && typeof bridge.basicShares === "object" ? bridge.basicShares : null) as Rec | null;
  const basic = basicRec ? num(basicRec.shares) : null;
  // Claims the filing text states but no tagged instrument covers (2.5.9).
  const claims = (Array.isArray(bridge.unquantifiedShareClaims) ? bridge.unquantifiedShareClaims : []) as Rec[];
  const openClaims = claims.filter((c) => c.status === "UNQUANTIFIED");
  const coverage = (bridge.claimCoverage && typeof bridge.claimCoverage === "object" ? bridge.claimCoverage : null) as Rec | null;
  const claimTextRead = coverage?.textScan === "READ";
  const results = scenarios.map((s) => {
    const lines = scenarioLines(bridge, s);
    const byComponent: Record<string, number> = {};
    for (const l of lines) {
      if (l.included && l.incrementalShares != null) byComponent[l.component] = (byComponent[l.component] ?? 0) + l.incrementalShares;
    }
    const issuance = s.knownIssuance.reduce((sum, k) => sum + k.shares, 0);
    const unresolved = lines.filter((l) => l.unresolvedReason != null && l.treatment !== "exclude");
    const incremental = Object.values(byComponent).reduce((sum, v) => sum + v, 0);
    const resulting = basic != null ? round(basic + incremental + issuance) : null;
    return {
      name: s.name,
      parameters: { price: s.price, ...s.treatments, knownIssuance: s.knownIssuance, defaulted: s.defaulted },
      basicShares: basic,
      instruments: lines,
      knownIssuance: s.knownIssuance,
      totals: {
        basicShares: basic,
        incrementalByComponent: byComponent,
        knownIssuanceShares: issuance,
        resultingShares: resulting,
        dilutionPct: basic != null && basic > 0 && resulting != null ? round(((resulting - basic) / basic) * 100, 2) : null,
      },
      unresolvedInstruments: unresolved.map((l) => ({ component: l.component, instrument: l.instrument, reason: l.unresolvedReason })),
      // Resolved tagged instruments are never a full claim inventory.
      completeness: basic == null ? "NO_BASIC_SHARES"
        : unresolved.length > 0 ? "EXCLUDES_UNRESOLVED_INSTRUMENTS"
        : openClaims.length > 0 ? "EXCLUDES_UNQUANTIFIED_CLAIMS"
        : !claimTextRead ? "CLAIM_TEXT_NOT_READ"
        : "TAGGED_INSTRUMENTS_RESOLVED",
      priceSensitiveInstruments: lines
        .filter((l) => l.thresholdPrice != null)
        .map((l) => ({ component: l.component, instrument: l.instrument, thresholdPrice: l.thresholdPrice, inTheMoney: l.inTheMoney })),
    };
  });
  return {
    ticker: ticker.toUpperCase(),
    basis: "MECHANICAL_COMPANY_DISCLOSED",
    status: basic == null ? "NOT_FOUND" : results.some((r) => r.completeness !== "TAGGED_INSTRUMENTS_RESOLVED") ? "PARTIAL" : "COMPUTED",
    periodEnd: bridge.periodEnd ?? null,
    basicShares: basicRec,
    sources: bridge.sources ?? [],
    scenarios: results,
    notDisclosed: bridge.notDisclosed ?? [],
    unquantifiedShareClaims: claims,
    claimCoverage: coverage ?? { scope: "TAGGED_INSTRUMENTS", completeClaimInventory: false, textScan: "NOT_READ" },
    reportedEpsDilution: bridge.reportedEpsDilution ?? null,
    bridgeWarnings: Array.isArray(bridge.warnings) ? bridge.warnings : [],
    treatmentOptions: TREATMENTS,
    treatmentDefaults: DEFAULTS,
    methodology: [
      "Counts, exercise prices and conversion terms are the company's inline XBRL disclosures (the dilution bridge's inventory); prices and treatments are the caller's.",
      "Each scenario is reported as computed; no scenario is recommended and no denominator is selected.",
      "An unresolved instrument is left out of resultingShares and listed; an instrument in notDisclosed was not tagged, which is not proof it does not exist.",
      "Net-share settlement applies only to notes whose principal the filing says is settled in cash (convertibles: net_share_settlement_when_stated); capped calls offset only with capped_calls: offset_when_stated and only when their strike, cap and covered shares are stated. Make-whole adjustments and performance conditions are not modeled.",
      "TAGGED_INSTRUMENTS_RESOLVED means every tagged instrument was resolved, not that every claim on the equity was found; claims quoted in unquantifiedShareClaims are in no scenario's resultingShares.",
    ],
    selectedScenario: null,
    selectedDenominator: null,
    ...AUTHORITY_BOUNDARY,
  };
}
