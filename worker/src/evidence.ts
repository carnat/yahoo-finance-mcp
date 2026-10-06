/**
 * Evidence composition primitives, shared with yfmcp/evidence.py (parity is
 * tested in scripts/test_evidence.py). Pure: no I/O and no clock.
 *
 * MCP supplies reproducible facts and mechanical transformations; valuation
 * method, multiple, scenario weights, price target, G2, opportunity and
 * action belong to the caller. Every payload built here carries
 * AUTHORITY_BOUNDARY, and missing provider evidence stays visibly missing:
 * nothing is interpolated, extrapolated or derived and called consensus.
 */

import { companyFiscalYearEnd, fiscalYearOfPeriodEnd, type FiscalYearNaming } from "./fiscal-calendar.js";

export const EVIDENCE_CUT_SCHEMA = "yfmcp.evidence-cut/1";
export const CONSENSUS_OBSERVATION_SCHEMA = "yfmcp.consensus-observation/1";
export const CANONICALIZATION = "yfmcp-canonical-json/1: sorted keys, no whitespace, ECMAScript number formatting, UTF-8";

export const AUTHORITY_BOUNDARY = {
  decisionUse: "EVIDENCE_ONLY",
  selectedMethod: null,
  selectedMultiple: null,
  scenarioWeights: null,
  priceTarget: null,
  g2: null,
  opportunity: null,
  action: null,
} as const;

type Rec = Record<string, unknown>;

/** Actions whose payloads are evidence only; each result they return carries AUTHORITY_BOUNDARY (2.5.8). */
export const EVIDENCE_ONLY_ACTIONS: ReadonlySet<string> = new Set([
  "build_valuation_evidence_pack",
  "extract_funding_capex_schedule",
  "extract_operating_driver_ledger",
  "get_consensus_forecast_curve",
  "get_eps_revisions",
  "get_evidence_cut",
  "get_evidence_quality",
  "get_guidance_history",
  "get_historical_valuation_context",
  "get_share_count_scenarios",
  "list_evidence_cuts",
  "reconcile_metric_sources",
]);

/**
 * An evidence-only action's result object with AUTHORITY_BOUNDARY, so a terminal status such as
 * PERIOD_NOT_FOUND or NO_SEC_REGISTRANT has the same shape as a full result. Errors ({ error: true })
 * become failure envelopes with no data and are left alone, as are other tools and non-object results.
 */
export function withAuthorityBoundary(action: string, raw: string): string {
  if (!EVIDENCE_ONLY_ACTIONS.has(action)) return raw;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return raw;
  }
  if (parsed == null || typeof parsed !== "object" || Array.isArray(parsed)) return raw;
  const rec = parsed as Rec;
  if (rec.error === true || typeof rec.ok === "boolean") return raw;
  if (Object.entries(AUTHORITY_BOUNDARY).every(([k, v]) => rec[k] === v)) return raw;
  return JSON.stringify({ ...rec, ...AUTHORITY_BOUNDARY });
}

// ── Canonical JSON ───────────────────────────────────────────────────────────

/** Canonical JSON: the bytes an evidence cut's SHA-256 is taken over. */
export function canonicalJson(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  if (typeof value === "string") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((v) => (v === undefined ? "null" : canonicalJson(v))).join(",")}]`;
  if (typeof value === "object") {
    const obj = value as Rec;
    const keys = Object.keys(obj).filter((k) => obj[k] !== undefined).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalJson(obj[k])}`).join(",")}}`;
  }
  return "null";
}

// ── Evidence cut identity ────────────────────────────────────────────────────

const CUT_ID_RE = /^ec1_([A-Z0-9.^=-]{1,20})_(\d{8}T\d{6}Z)_([0-9a-f]{64})$/;

/** 2026-09-26T08:02:09.101Z -> 20260926T080209Z */
export function compactTimestamp(iso: string): string {
  return iso.replace(/\.\d+/, "").replace(/[-:]/g, "");
}

export function evidenceCutId(ticker: string, cutoffIso: string, sha256: string): string {
  return `ec1_${ticker.toUpperCase()}_${compactTimestamp(cutoffIso)}_${sha256}`;
}

export function evidenceCutKey(ticker: string, compactTs: string, sha256: string): string {
  return `evidence-cuts/${ticker.toUpperCase()}/${compactTs}/${sha256}.json`;
}

export function parseEvidenceCutId(id: string): { ticker: string; timestamp: string; sha256: string; key: string } | null {
  const m = CUT_ID_RE.exec(id.trim());
  if (!m) return null;
  return { ticker: m[1], timestamp: m[2], sha256: m[3], key: evidenceCutKey(m[1], m[2], m[3]) };
}

export function consensusObservationKey(ticker: string, isoDate: string): string {
  return `consensus-history/${ticker.toUpperCase()}/${isoDate.slice(0, 10)}.json`;
}

// ── Numbers ──────────────────────────────────────────────────────────────────

function num(value: unknown): number | null {
  const v = value && typeof value === "object" && !Array.isArray(value) ? (value as Rec).raw : value;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string" && v.trim() && !Number.isNaN(Number(v))) return Number(v);
  return null;
}

function round(value: number, digits: number): number {
  const f = 10 ** digits;
  return Math.round(value * f) / f;
}

function str(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

// ── Consensus provider inputs ────────────────────────────────────────────────

export type ConsensusMetric = "eps" | "revenue";
export const CONSENSUS_METRICS: ConsensusMetric[] = ["eps", "revenue"];
/** Requested metrics no configured provider supplies as consensus. */
export const METRICS_NOT_COVERED = ["ebitda", "ebit", "freeCashFlow", "capex", "grossMargin", "ebitdaMargin"];

export interface MetricEstimate {
  mean: number | null;
  high: number | null;
  low: number | null;
  analystCount: number | null;
  currency: string | null;
  currencyBasis: string;
}

export interface EpsTrend {
  current: number | null;
  d7: number | null;
  d30: number | null;
  d60: number | null;
  d90: number | null;
}

export interface EpsRevisionCounts {
  up7d: number | null;
  down7d: number | null;
  up30d: number | null;
  down30d: number | null;
}

export interface ProviderPeriod {
  providerPeriodLabel: string;
  fiscalYearEnd: string | null;
  fiscalYearEndBasis: string;
  eps: MetricEstimate;
  revenue: MetricEstimate;
  epsTrend: EpsTrend | null;
  epsRevisions: EpsRevisionCounts | null;
}

export interface ProviderConsensusInput {
  provider: string;
  status: string;
  retrievedAt: string | null;
  providerTimestamp: string | null;
  message: string | null;
  periods: ProviderPeriod[];
}

/** A stock split: `ratio` is new shares per old share (4 for a 4-for-1, 0.1 for a 1-for-10 reverse split). */
export interface SplitEvent {
  date: string;
  ratio: number;
}

/** The ticker's split history as fetched; `status` is not OK when it could not be read and no split check ran. */
export interface SplitHistory {
  status: string;
  splits: SplitEvent[];
  message?: string | null;
}

/** How far back a split can explain a provider EPS gap (2.5.23, F-002): stale rows outlive a split by months. */
export const SPLIT_LOOKBACK_DAYS = 400;
/** A provider EPS ratio within this fraction of a split's ratio is read as an unadjusted row, not a conflict. */
export const SPLIT_RATIO_TOLERANCE = 0.15;

/** "4-for-1" for a forward split, "1-for-10" for a reverse one. */
export function splitText(ratio: number): string {
  return ratio >= 1 ? `${round(ratio, 4)}-for-1` : `1-for-${round(1 / ratio, 4)}`;
}

/** Splits dated on or before asOf and within `days` of it, oldest first. */
export function splitsWithin(history: SplitHistory | null, asOf: string, days: number): SplitEvent[] {
  if (!history || history.status !== "OK") return [];
  const end = dayNumber(asOf.slice(0, 10));
  return history.splits
    .filter((s) => typeof s.ratio === "number" && Number.isFinite(s.ratio) && s.ratio > 0 && s.ratio !== 1 && /^\d{4}-\d{2}-\d{2}/.test(s.date))
    .filter((s) => { const d = dayNumber(s.date.slice(0, 10)); return d <= end && end - d <= days; })
    .sort((a, b) => a.date.localeCompare(b.date));
}

/**
 * A provider row whose mean lies outside its own high-low range, or whose low is above its high, cannot be a
 * consensus (2.5.23, F-001: Alpha Vantage ANET revenue averaged 667.6M against a 652.7M high).
 */
export function rangeInconsistency(e: { mean: number | null; high: number | null; low: number | null }): string | null {
  const tol = (v: number) => 1e-9 * Math.max(1, Math.abs(v));
  if (e.high != null && e.low != null && e.low - e.high > tol(e.high)) return "LOW_ABOVE_HIGH";
  if (e.mean == null) return null;
  if (e.high != null && e.mean - e.high > tol(e.high)) return "MEAN_ABOVE_HIGH";
  if (e.low != null && e.low - e.mean > tol(e.low)) return "MEAN_BELOW_LOW";
  return null;
}

function estimate(block: unknown, currency: string | null, currencyBasis: string, allowZeroCount = false): MetricEstimate {
  const b = (block && typeof block === "object" ? block : {}) as Rec;
  const analystCount = num(b.numberOfAnalysts ?? b.analystCount);
  const empty = !allowZeroCount && analystCount === 0;
  return {
    mean: empty ? null : num(b.avg ?? b.mean),
    high: empty ? null : num(b.high),
    low: empty ? null : num(b.low),
    analystCount,
    currency,
    currencyBasis,
  };
}

/**
 * Yahoo earningsTrend rows (0y, +1y) as provider periods. Values may be
 * {raw, fmt} objects or numbers. Yahoo states the fiscal period end
 * (`endDate`); when a caller cannot supply it, `fiscalYearEnds` maps the
 * period label to an end date it derived, and the basis says so.
 */
export function yahooConsensusInput(
  trend: unknown[],
  opts: {
    retrievedAt: string | null;
    status?: string;
    message?: string | null;
    financialCurrency?: string | null;
    fiscalYearEnds?: Record<string, string>;
    fiscalYearEndBasis?: string;
  },
): ProviderConsensusInput {
  const periods: ProviderPeriod[] = [];
  for (const raw of Array.isArray(trend) ? trend : []) {
    const t = (raw && typeof raw === "object" ? raw : {}) as Rec;
    const label = str(t.period);
    if (label !== "0y" && label !== "+1y") continue;
    const stated = str(t.endDate);
    const fiscalYearEnd = stated ?? opts.fiscalYearEnds?.[label] ?? null;
    const basis = stated ? "PROVIDER_STATED" : fiscalYearEnd ? (opts.fiscalYearEndBasis ?? "DERIVED") : "NOT_STATED";
    const ee = (t.earningsEstimate ?? {}) as Rec;
    const re = (t.revenueEstimate ?? {}) as Rec;
    const epsCurrency = str(ee.earningsCurrency);
    const revCurrency = str(re.revenueCurrency);
    const fallback = str(opts.financialCurrency ?? null);
    const trendBlock = (t.epsTrend ?? null) as Rec | null;
    const revBlock = (t.epsRevisions ?? null) as Rec | null;
    periods.push({
      providerPeriodLabel: label,
      fiscalYearEnd,
      fiscalYearEndBasis: basis,
      eps: estimate(ee, epsCurrency ?? fallback, epsCurrency ? "PROVIDER_STATED" : fallback ? "FINANCIAL_CURRENCY" : "NOT_STATED"),
      revenue: estimate(re, revCurrency ?? fallback, revCurrency ? "PROVIDER_STATED" : fallback ? "FINANCIAL_CURRENCY" : "NOT_STATED"),
      epsTrend: trendBlock ? {
        current: num(trendBlock.current),
        d7: num(trendBlock["7daysAgo"]),
        d30: num(trendBlock["30daysAgo"]),
        d60: num(trendBlock["60daysAgo"]),
        d90: num(trendBlock["90daysAgo"]),
      } : null,
      epsRevisions: revBlock ? {
        up7d: num(revBlock.upLast7days ?? revBlock.upLast7Days),
        down7d: num(revBlock.downLast7days ?? revBlock.downLast7Days),
        up30d: num(revBlock.upLast30days ?? revBlock.upLast30Days),
        down30d: num(revBlock.downLast30days ?? revBlock.downLast30Days),
      } : null,
    });
  }
  return {
    provider: "yahoo_finance",
    status: opts.status ?? (periods.length ? "OK" : "NO_DATA"),
    retrievedAt: opts.retrievedAt,
    providerTimestamp: null,
    message: opts.message ?? null,
    periods,
  };
}

/** Alpha Vantage EARNINGS_ESTIMATES fiscal-year rows as provider periods. */
export function alphaVantageConsensusInput(
  payload: unknown,
  opts: { retrievedAt: string | null; status?: string; message?: string | null },
): ProviderConsensusInput {
  const rows = ((payload && typeof payload === "object" ? (payload as Rec).estimates : null) ?? []) as Rec[];
  const periods: ProviderPeriod[] = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    if (str(row.horizon) !== "fiscal year") continue;
    const fiscalYearEnd = str(row.date);
    if (!fiscalYearEnd) continue;
    periods.push({
      providerPeriodLabel: "fiscal year",
      fiscalYearEnd,
      fiscalYearEndBasis: "PROVIDER_STATED",
      eps: estimate({
        avg: row.eps_estimate_average, high: row.eps_estimate_high, low: row.eps_estimate_low,
        numberOfAnalysts: row.eps_estimate_analyst_count,
      }, null, "NOT_STATED"),
      revenue: estimate({
        avg: row.revenue_estimate_average, high: row.revenue_estimate_high, low: row.revenue_estimate_low,
        numberOfAnalysts: row.revenue_estimate_analyst_count,
      }, null, "NOT_STATED"),
      epsTrend: {
        current: num(row.eps_estimate_average),
        d7: num(row.eps_estimate_average_7_days_ago),
        d30: num(row.eps_estimate_average_30_days_ago),
        d60: num(row.eps_estimate_average_60_days_ago),
        d90: num(row.eps_estimate_average_90_days_ago),
      },
      epsRevisions: {
        up7d: num(row.eps_estimate_revision_up_trailing_7_days),
        down7d: num(row.eps_estimate_revision_down_trailing_7_days),
        up30d: num(row.eps_estimate_revision_up_trailing_30_days),
        down30d: num(row.eps_estimate_revision_down_trailing_30_days),
      },
    });
  }
  periods.sort((a, b) => String(a.fiscalYearEnd).localeCompare(String(b.fiscalYearEnd)));
  return {
    provider: "alpha_vantage",
    status: opts.status ?? (periods.length ? "OK" : "NO_DATA"),
    retrievedAt: opts.retrievedAt,
    providerTimestamp: null,
    message: opts.message ?? null,
    periods,
  };
}

// ── Fiscal period identity ───────────────────────────────────────────────────

function dayNumber(isoDate: string): number {
  return Math.floor(Date.parse(`${isoDate.slice(0, 10)}T00:00:00Z`) / 86_400_000);
}

/** FY0 is Yahoo's current fiscal year when stated, else the first provider fiscal year ending on or after the as-of date. */
export function resolveFy0(inputs: ProviderConsensusInput[], asOfDate: string): { fiscalYearEnd: string; basis: string } | null {
  for (const input of inputs) {
    const p = input.periods.find((x) => x.providerPeriodLabel === "0y" && x.fiscalYearEnd);
    if (p?.fiscalYearEnd) return { fiscalYearEnd: p.fiscalYearEnd, basis: `${input.provider}:0y` };
  }
  const today = dayNumber(asOfDate);
  const ends = inputs.flatMap((i) => i.periods.map((p) => p.fiscalYearEnd)).filter((d): d is string => !!d && dayNumber(d) >= today).sort();
  return ends.length ? { fiscalYearEnd: ends[0], basis: "earliest_provider_fiscal_year_on_or_after_as_of" } : null;
}

/** Years after FY0, from fiscal year ends (52/53-week years stay within a few days of a whole year). */
export function fiscalYearOffset(fy0End: string, fiscalYearEnd: string): number {
  return Math.round((dayNumber(fiscalYearEnd) - dayNumber(fy0End)) / 365.25);
}

// ── Consensus curve ──────────────────────────────────────────────────────────

export interface ConsensusPolicy {
  horizonYears: number;
  minAnalystCount: number;
  conflictTolerancePct: number;
  epsAbsoluteTolerance: number;
}

export const DEFAULT_CONSENSUS_POLICY: ConsensusPolicy = {
  horizonYears: 5,
  minAnalystCount: 3,
  conflictTolerancePct: 10,
  epsAbsoluteTolerance: 0.02,
};

function providerEntry(input: ProviderConsensusInput, period: ProviderPeriod, metric: ConsensusMetric, policy: ConsensusPolicy): Rec {
  const e = period[metric];
  const range = e.high != null && e.low != null ? e.high - e.low : null;
  const inconsistency = rangeInconsistency(e);
  let state = "PROVIDER_COVERED";
  if (e.mean == null) state = "PROVIDER_NOT_COVERED";
  else if (inconsistency) state = "PROVIDER_INCONSISTENT";
  else if (e.analystCount == null || e.analystCount < policy.minAnalystCount) state = "INSUFFICIENT_ANALYST_COUNT";
  return {
    provider: input.provider,
    providerPeriodLabel: period.providerPeriodLabel,
    fiscalYearEnd: period.fiscalYearEnd,
    fiscalYearEndBasis: period.fiscalYearEndBasis,
    mean: e.mean,
    high: e.high,
    low: e.low,
    analystCount: e.analystCount,
    currency: e.currency,
    currencyBasis: e.currencyBasis,
    highLowRange: range != null ? round(range, 6) : null,
    highLowRangePctOfMean: range != null && e.mean != null && e.mean !== 0 ? round((range / Math.abs(e.mean)) * 100, 2) : null,
    providerTimestamp: input.providerTimestamp,
    retrievedAt: input.retrievedAt,
    state,
    ...(inconsistency ? { inconsistency } : {}),
  };
}

function agreement(entries: Rec[], metric: ConsensusMetric, policy: ConsensusPolicy, splits: SplitEvent[] = []): Rec {
  // An impossible row is not a second opinion (2.5.23, F-001): it is left out of the comparison and named.
  const inconsistent = entries.filter((e) => typeof e.mean === "number" && e.state === "PROVIDER_INCONSISTENT");
  const excluded = inconsistent.length ? { providersExcluded: inconsistent.map((e) => ({ provider: e.provider, reason: e.inconsistency })) } : {};
  const valued = entries.filter((e) => typeof e.mean === "number" && e.state !== "PROVIDER_INCONSISTENT");
  const providersCompared = valued.map((e) => e.provider);
  if (valued.length === 0 && inconsistent.length) return { status: "PROVIDER_INCONSISTENT", providersCompared, relativeDiffPct: null, absoluteDiff: null, ...excluded };
  if (valued.length === 0) return { status: "NO_PROVIDER", providersCompared, relativeDiffPct: null, absoluteDiff: null };
  if (valued.length === 1) return { status: "SINGLE_PROVIDER", providersCompared, relativeDiffPct: null, absoluteDiff: null, ...excluded };
  const ends = valued.map((e) => String(e.fiscalYearEnd ?? ""));
  const endDays = ends.filter(Boolean).map(dayNumber);
  if (endDays.length === valued.length && Math.max(...endDays) - Math.min(...endDays) > 10) {
    return { status: "PERIOD_IDENTITY_MISMATCH", providersCompared, fiscalYearEnds: ends, relativeDiffPct: null, absoluteDiff: null, ...excluded };
  }
  const currencies = [...new Set(valued.map((e) => e.currency).filter((c): c is string => typeof c === "string"))];
  if (currencies.length > 1) return { status: "CURRENCY_MISMATCH", providersCompared, currencies, relativeDiffPct: null, absoluteDiff: null, ...excluded };
  const currencyIdentity = currencies.length === 1 && valued.every((e) => e.currency === currencies[0]) ? "VERIFIED" : "UNVERIFIED";
  // The same mean, high, low and analyst count wherever both publish them: one upstream feed carried twice, not
  // two checks (2.5.22, F-005: Yahoo and Alpha Vantage matched to the last published digit).
  if (identicalAtPublishedPrecision(valued)) {
    return { status: "IDENTICAL", providersCompared, relativeDiffPct: 0, absoluteDiff: 0, currencyIdentity, independence: "NOT_INDEPENDENT", ...excluded };
  }
  const means = valued.map((e) => e.mean as number);
  const hi = Math.max(...means);
  const lo = Math.min(...means);
  const absoluteDiff = round(hi - lo, 6);
  const scale = Math.max(Math.abs(hi), Math.abs(lo));
  const relativeDiffPct = scale > 0 ? round(((hi - lo) / scale) * 100, 2) : 0;
  const withinAbsolute = metric === "eps" && absoluteDiff <= policy.epsAbsoluteTolerance;
  const status = withinAbsolute || relativeDiffPct <= policy.conflictTolerancePct ? "AGREED" : "CONFLICT";
  if (status === "CONFLICT" && metric === "eps") {
    // An EPS gap that is a recent split's ratio is one provider not split-adjusted, not two views (2.5.23, F-002).
    const split = splitExplains(valued, splits);
    if (split) {
      return { status: "NOT_SPLIT_ADJUSTED", providersCompared, relativeDiffPct, absoluteDiff, currencyIdentity, independence: "UNVERIFIED",
        split: split.split, providersNotAdjusted: split.notAdjusted, ...excluded };
    }
  }
  return { status, providersCompared, relativeDiffPct, absoluteDiff, currencyIdentity, independence: "UNVERIFIED", ...excluded };
}

/**
 * The recent split (or run of recent splits) whose ratio matches the gap between two same-sign EPS means, and the
 * provider still on the pre-split basis: the larger magnitude after a forward split, the smaller after a reverse one.
 */
function splitExplains(valued: Rec[], splits: SplitEvent[]): { split: Rec; notAdjusted: string[] } | null {
  if (valued.length !== 2 || !splits.length) return null;
  const [a, b] = valued.map((e) => e.mean as number);
  if (a === 0 || b === 0 || Math.sign(a) !== Math.sign(b)) return null;
  const gap = Math.max(Math.abs(a), Math.abs(b)) / Math.min(Math.abs(a), Math.abs(b));
  const candidates: { split: Rec; ratio: number }[] = splits.map((s) => ({ split: { date: s.date.slice(0, 10), ratio: s.ratio }, ratio: s.ratio }));
  if (splits.length > 1) {
    const ratio = splits.reduce((p, s) => p * s.ratio, 1);
    candidates.push({ split: { date: splits[splits.length - 1].date.slice(0, 10), ratio: round(ratio, 6), cumulativeOf: splits.length }, ratio });
  }
  for (const c of candidates) {
    const factor = Math.max(c.ratio, 1 / c.ratio);
    if (factor < 1.5 || Math.abs(gap / factor - 1) > SPLIT_RATIO_TOLERANCE) continue;
    const larger = Math.abs(a) > Math.abs(b) ? valued[0] : valued[1];
    const smaller = larger === valued[0] ? valued[1] : valued[0];
    return { split: c.split, notAdjusted: [String((c.ratio > 1 ? larger : smaller).provider)] };
  }
  return null;
}

function identicalAtPublishedPrecision(valued: Rec[]): boolean {
  for (const field of ["mean", "high", "low", "analystCount"]) {
    const values = valued.map((e) => e[field]).filter((v): v is number => typeof v === "number");
    if (values.length >= 2 && new Set(values).size > 1) return false;
  }
  return valued.every((e) => typeof e.analystCount === "number");
}

const COMPARED = new Set(["AGREED", "CONFLICT", "IDENTICAL", "NOT_SPLIT_ADJUSTED"]);

/**
 * Whether the curve is a cross-check at all (2.5.22, F-003/F-005): which providers answered, which failed and
 * why, and whether the ones that answered are independent. A failed provider or identical feeds is a warning,
 * not a quiet single-source curve.
 */
function crossCheck(inputs: ProviderConsensusInput[], periods: Rec[]): { summary: Rec; warnings: Rec[] } {
  const answered = inputs.filter((i) => i.status === "OK").map((i) => i.provider);
  const failed = inputs.filter((i) => i.status !== "OK" && i.status !== "NO_DATA").map((i) => ({ provider: i.provider, status: i.status, message: i.message }));
  let compared = 0;
  let identical = 0;
  for (const period of periods) {
    for (const cell of Object.values(period.metrics as Rec)) {
      const status = String(((cell as Rec).agreement as Rec).status);
      if (COMPARED.has(status)) compared += 1;
      if (status === "IDENTICAL") identical += 1;
    }
  }
  const status = answered.length === 0 ? "NO_PROVIDER"
    : answered.length === 1 ? "SINGLE_PROVIDER"
    : compared === 0 ? "NOT_COMPARED"
    : identical === compared ? "NOT_INDEPENDENT"
    : "CROSS_CHECKED";
  const warnings: Rec[] = [];
  for (const f of failed) {
    const quota = f.provider === "alpha_vantage"
      ? " This server's Alpha Vantage key and its quota are its own, separate from any key used directly."
      : "";
    warnings.push({
      code: "CROSS_CHECK_DEGRADED",
      message: `${f.provider} returned ${f.status}${f.message ? `: ${f.message}` : ""}. The curve is not cross-checked against it.${quota}`,
      severity: "warning",
    });
  }
  if (status === "NOT_INDEPENDENT") {
    warnings.push({
      code: "PROVIDERS_NOT_INDEPENDENT",
      message: "Every compared figure is identical at the providers' published precision: they carry one upstream feed, so their agreement is one source, not two.",
      severity: "warning",
    });
  }
  return {
    summary: { status, providersAnswered: answered, providersFailed: failed, cellsCompared: compared, cellsIdentical: identical },
    warnings,
  };
}

function metricCoverage(entries: Rec[], agreementStatus: string): string {
  if (!entries.some((e) => typeof e.mean === "number")) return "PROVIDER_NOT_COVERED";
  if (agreementStatus === "PROVIDER_INCONSISTENT") return "PROVIDER_INCONSISTENT";
  if (["CONFLICT", "PERIOD_IDENTITY_MISMATCH", "CURRENCY_MISMATCH", "NOT_SPLIT_ADJUSTED"].includes(agreementStatus)) return "PROVIDER_CONFLICT";
  if (!entries.some((e) => e.state === "PROVIDER_COVERED")) return "INSUFFICIENT_ANALYST_COUNT";
  return "PROVIDER_COVERED";
}

const INCONSISTENCY_TEXT: Record<string, string> = {
  MEAN_ABOVE_HIGH: "its mean is above its own high",
  MEAN_BELOW_LOW: "its mean is below its own low",
  LOW_ABOVE_HIGH: "its low is above its own high",
};

/** One warning per impossible provider row (F-001) and per EPS gap read as a missed split adjustment (F-002). */
function cellWarnings(label: string, metric: string, cell: Rec): Rec[] {
  const out: Rec[] = [];
  for (const e of cell.providers as Rec[]) {
    if (e.state !== "PROVIDER_INCONSISTENT") continue;
    out.push({
      code: "PROVIDER_ROW_INCONSISTENT",
      message: `${e.provider} ${label} ${metric} (fiscal year ending ${e.fiscalYearEnd ?? "not stated"}): ${INCONSISTENCY_TEXT[String(e.inconsistency)]} `
        + `(mean ${e.mean}, low ${e.low}, high ${e.high}). The row is shown as given and left out of the agreement check.`,
      severity: "warning",
    });
  }
  const agree = cell.agreement as Rec;
  if (agree.status === "NOT_SPLIT_ADJUSTED") {
    const split = agree.split as Rec;
    out.push({
      code: "PROVIDER_NOT_SPLIT_ADJUSTED",
      message: `${label} EPS: the providers differ by the ratio of the ${splitText(split.ratio as number)} split on ${split.date}; `
        + `${(agree.providersNotAdjusted as string[]).join(", ")} appears not split-adjusted. Reported as NOT_SPLIT_ADJUSTED, not as a conflict between views.`,
      severity: "warning",
    });
  }
  return out;
}

const UNAVAILABLE_STATISTIC = { value: null, state: "PROVIDER_NOT_COVERED" };

/**
 * FY0 to FY+horizon consensus, per provider, per fiscal period and metric,
 * with each cell's coverage state. Periods no provider covers are listed as
 * PROVIDER_NOT_COVERED rather than filled; no provider value is selected.
 */
export function buildConsensusCurve(
  ticker: string,
  inputs: ProviderConsensusInput[],
  asOf: string,
  policy: ConsensusPolicy = DEFAULT_CONSENSUS_POLICY,
  // How the company names its fiscal years, from its annual reports (2.5.13); the period-end rule without it.
  naming: FiscalYearNaming | null = null,
  // The ticker's split history (2.5.23); without it EPS gaps are not checked against splits.
  splitHistory: SplitHistory | null = null,
): Rec {
  const recentSplits = splitsWithin(splitHistory, asOf, SPLIT_LOOKBACK_DAYS);
  const cellWarningList: Rec[] = [];
  const horizon = Math.max(1, Math.min(5, Math.trunc(policy.horizonYears)));
  const yearNaming: FiscalYearNaming = naming ?? { offset: 0, basis: "PERIOD_END_RULE", periodEnd: null, statedFiscalYear: null, calendar: null };
  const fy0 = resolveFy0(inputs, asOf);
  const periods: Rec[] = [];
  const summary: Record<string, number> = {};
  for (let k = 0; k <= horizon; k++) {
    const label = k === 0 ? "FY0" : `FY+${k}`;
    const matching = fy0
      ? inputs.flatMap((input) => input.periods
        .filter((p) => p.fiscalYearEnd && fiscalYearOffset(fy0.fiscalYearEnd, p.fiscalYearEnd) === k)
        .map((p) => ({ input, p })))
      : [];
    const fiscalYearEnds = [...new Set(matching.map(({ p }) => p.fiscalYearEnd as string))].sort();
    const metrics: Rec = {};
    for (const metric of CONSENSUS_METRICS) {
      const entries = matching.map(({ input, p }) => providerEntry(input, p, metric, policy));
      const agree = agreement(entries, metric, policy, recentSplits);
      const coverage = metricCoverage(entries, String(agree.status));
      summary[coverage] = (summary[coverage] ?? 0) + 1;
      metrics[metric] = {
        coverage,
        providers: entries,
        agreement: agree,
        dispersionStatistics: { highLowRange: "PER_PROVIDER", median: UNAVAILABLE_STATISTIC, standardDeviation: UNAVAILABLE_STATISTIC },
      };
      cellWarningList.push(...cellWarnings(label, metric, metrics[metric] as Rec));
    }
    periods.push({
      label,
      // A 52/53-week year ending in early January is the prior year's (2.5.11); the company's own naming
      // (DG's year ending January 2026 is its fiscal 2025) shifts it (2.5.13).
      fiscalYear: fy0 && fiscalYearOfPeriodEnd(fy0.fiscalYearEnd) != null ? (fiscalYearOfPeriodEnd(fy0.fiscalYearEnd) as number) + yearNaming.offset + k : null,
      fiscalYearEnds,
      // The company's own year end for that year, from its SEC fiscal calendar; fiscalYearEnds stays as the
      // providers state it (2.5.16: DG's 2027-01-31 from Yahoo is its Friday 2027-01-29).
      companyFiscalYearEnd: companyFiscalYearEnd(yearNaming.calendar, fiscalYearEnds[0] ?? null),
      metrics,
    });
  }
  const check = crossCheck(inputs, periods);
  return {
    ticker: ticker.toUpperCase(),
    asOf,
    crossCheck: check.summary,
    splitHistory: splitHistorySummary(splitHistory, recentSplits),
    fiscalYearBasis: fy0 ?? { fiscalYearEnd: null, basis: "NO_PROVIDER_FISCAL_YEAR" },
    fiscalYearNaming: yearNaming,
    policy: { ...policy, horizonYears: horizon },
    providers: inputs.map((i) => ({
      provider: i.provider,
      status: i.status,
      retrievedAt: i.retrievedAt,
      providerTimestamp: i.providerTimestamp,
      message: i.message,
      fiscalYearsReturned: i.periods.map((p) => p.fiscalYearEnd).filter(Boolean),
    })),
    periods,
    coverageSummary: summary,
    metricsNotCoveredByAnyProvider: METRICS_NOT_COVERED.map((metric) => ({ metric, coverage: "PROVIDER_NOT_COVERED" })),
    notes: [
      "Each provider's figures are reported as given; no provider value is selected, blended or averaged.",
      "Periods and metrics no provider covers are PROVIDER_NOT_COVERED; nothing is interpolated or extended from long-term growth rates.",
      "Median and standard deviation are not published by the configured providers; the high-low range per provider is the only dispersion measure.",
      "Agreement between providers does not establish independence: they may redistribute the same underlying estimates. IDENTICAL marks figures equal to the last published digit, which is one feed carried twice.",
    ],
    warnings: [...check.warnings, ...cellWarningList, ...splitHistoryWarnings(splitHistory, "EPS gaps between providers are not checked against splits.")],
    ...AUTHORITY_BOUNDARY,
  };
}

function splitHistorySummary(history: SplitHistory | null, recent: SplitEvent[]): Rec {
  return {
    status: history ? history.status : "NOT_REQUESTED",
    lookbackDays: SPLIT_LOOKBACK_DAYS,
    recentSplits: recent.map((s) => ({ date: s.date.slice(0, 10), ratio: s.ratio })),
    ...(history?.message ? { message: history.message } : {}),
  };
}

function splitHistoryWarnings(history: SplitHistory | null, consequence: string): Rec[] {
  if (!history || history.status === "OK") return [];
  return [{
    code: "SPLIT_HISTORY_UNAVAILABLE",
    message: `Split history returned ${history.status}${history.message ? `: ${history.message}` : ""}. ${consequence}`,
    severity: "warning",
  }];
}

// ── EPS revision windows ─────────────────────────────────────────────────────

const WINDOWS: [string, keyof EpsTrend, number][] = [["7d", "d7", 7], ["30d", "d30", 30], ["60d", "d60", 60], ["90d", "d90", 90]];

/**
 * Each window's change, unless it cannot mean anything (2.5.23): the row is impossible (F-001), or a split falls
 * inside the window so the past mean may be on the pre-split share count (F-002: ANET 2.73 → 0.73 at its 4-for-1).
 * A day of margin covers providers that adjust the day after the split.
 */
function revisionProvider(input: ProviderConsensusInput, p: ProviderPeriod, label: string, asOf: string, splits: SplitEvent[], warnings: Rec[]): Rec {
  const trend = p.epsTrend;
  const current = trend?.current ?? null;
  const inconsistency = rangeInconsistency(p.eps);
  if (inconsistency) {
    warnings.push({
      code: "PROVIDER_ROW_INCONSISTENT",
      message: `${input.provider} ${label} eps (fiscal year ending ${p.fiscalYearEnd ?? "not stated"}): ${INCONSISTENCY_TEXT[inconsistency]} `
        + `(mean ${p.eps.mean}, low ${p.eps.low}, high ${p.eps.high}). No window change is computed from it.`,
      severity: "warning",
    });
  }
  const windows: Rec = {};
  const notReported: string[] = [];
  for (const [name, key, days] of WINDOWS) {
    const past = trend ? trend[key] : null;
    if (past == null) notReported.push(`${name}.mean`);
    const inWindow = splitsWithin({ status: "OK", splits }, asOf, days + 1);
    const split = inWindow.length ? inWindow[inWindow.length - 1] : null;
    const state = past == null || current == null ? "NOT_REPORTED"
      : inconsistency ? "PROVIDER_INCONSISTENT"
      : split ? "SPLIT_IN_WINDOW"
      : "COMPARED";
    const compared = state === "COMPARED";
    windows[name] = {
      mean: past,
      change: compared ? round((current as number) - (past as number), 6) : null,
      changePct: compared && past !== 0 ? round((((current as number) - (past as number)) / Math.abs(past as number)) * 100, 2) : null,
      state,
      ...(state === "SPLIT_IN_WINDOW" && split ? { split: { date: split.date.slice(0, 10), ratio: split.ratio } } : {}),
    };
  }
  const counts = p.epsRevisions ?? { up7d: null, down7d: null, up30d: null, down30d: null };
  for (const [k, v] of Object.entries(counts)) if (v == null) notReported.push(`revisionCounts.${k}`);
  return {
    provider: input.provider,
    providerPeriodLabel: p.providerPeriodLabel,
    fiscalYearEnd: p.fiscalYearEnd,
    state: inconsistency ? "PROVIDER_INCONSISTENT" : "PROVIDER_COVERED",
    ...(inconsistency ? { inconsistency } : {}),
    current,
    windows,
    revisionCounts: counts,
    notReported,
    retrievedAt: input.retrievedAt,
    providerTimestamp: input.providerTimestamp,
  };
}

/** EPS estimate windows (7/30/60/90 days) and revision counts as each provider reports them for FY0 and FY+1. */
export function buildEpsRevisions(ticker: string, inputs: ProviderConsensusInput[], asOf: string, splitHistory: SplitHistory | null = null): Rec {
  const fy0 = resolveFy0(inputs, asOf);
  const windowSplits = splitsWithin(splitHistory, asOf, 91);
  const warnings: Rec[] = [];
  const periods: Rec[] = [];
  for (const k of [0, 1]) {
    const label = k === 0 ? "FY0" : "FY+1";
    const providers = fy0
      ? inputs.flatMap((input) => input.periods
        .filter((p) => p.fiscalYearEnd && fiscalYearOffset(fy0.fiscalYearEnd, p.fiscalYearEnd) === k && (p.epsTrend || p.epsRevisions))
        .map((p) => revisionProvider(input, p, label, asOf, windowSplits, warnings)))
      : [];
    periods.push({ label, coverage: providers.length ? "PROVIDER_COVERED" : "PROVIDER_NOT_COVERED", providers });
  }
  if (windowSplits.length) {
    const last = windowSplits[windowSplits.length - 1];
    warnings.push({
      code: "SPLIT_IN_WINDOW",
      message: `A ${splitText(last.ratio)} split on ${last.date.slice(0, 10)} falls inside the revision windows; windows spanning it compare pre- and post-split estimates and carry no change.`,
      severity: "warning",
    });
  }
  warnings.push(...splitHistoryWarnings(splitHistory, "Revision windows are not checked against splits."));
  return {
    ticker: ticker.toUpperCase(),
    asOf,
    fiscalYearBasis: fy0 ?? { fiscalYearEnd: null, basis: "NO_PROVIDER_FISCAL_YEAR" },
    periods,
    revenueRevisions: { coverage: "PROVIDER_NOT_COVERED", note: "No configured provider publishes revenue estimate history." },
    analystCountChanges: { coverage: "PROVIDER_NOT_COVERED", note: "No configured provider publishes analyst adds or drops." },
    providers: inputs.map((i) => ({ provider: i.provider, status: i.status, retrievedAt: i.retrievedAt, message: i.message })),
    splitHistory: { ...splitHistorySummary(splitHistory, windowSplits), lookbackDays: 91 },
    warnings,
    ...AUTHORITY_BOUNDARY,
  };
}

// ── Evidence quality ─────────────────────────────────────────────────────────

export function daysBetween(fromIso: string, toIso: string): number {
  return dayNumber(toIso) - dayNumber(fromIso);
}

export interface FilingRow {
  form: string;
  filingDate: string;
  reportDate: string | null;
  items: string | null;
  isInlineXBRL: boolean | null;
}

const PERIODIC_FORMS = new Set(["10-K", "10-Q", "20-F", "40-F", "10-K/A", "10-Q/A", "20-F/A", "40-F/A"]);
const QUARTERLY_REPORT_STALE_DAYS = 135;
// A year plus the four months a 20-F has after year end, plus a week of 52/53-week drift.
const ANNUAL_REPORT_STALE_DAYS = 365 + 120 + 7;
// Provider statuses that mean the request failed, as opposed to the provider having nothing (NO_DATA).
const PROVIDER_FAILURE_STATUSES = new Set(["PROVIDER_ERROR", "RATE_LIMIT", "PROVIDER_TIMEOUT", "ERROR", "TIMEOUT"]);

/**
 * Status, freshness and coverage per doctrine-relevant evidence family, from
 * light inputs only (quote, SEC submissions, consensus coverage, storage).
 * Families that need a full extraction report readiness, not results.
 */
export function evidenceQuality(input: {
  ticker: string;
  asOf: string;
  quote: { price: number | null; currency: string | null; priceTime: string | null; status: string } | null;
  filings: FilingRow[] | null;
  filingsStatus: string;
  consensus: Rec | null;
  storageAvailable: boolean;
}): Rec {
  const families: Rec = {};
  const blockers: Rec[] = [];
  const block = (family: string, code: string, message: string) => blockers.push({ family, code, message });

  const q = input.quote;
  const quoteAge = q?.priceTime ? daysBetween(q.priceTime, input.asOf) : null;
  // A quote request that failed is not a missing price: it is unavailable and worth a retry (2.5.15).
  const quoteFailed = q?.price == null && PROVIDER_FAILURE_STATUSES.has(String(q?.status ?? ""));
  families.quote = {
    state: q?.price != null ? (quoteAge != null && quoteAge > 4 ? "STALE" : "READY") : quoteFailed ? "UNAVAILABLE" : "MISSING",
    price: q?.price ?? null,
    currency: q?.currency ?? null,
    priceTime: q?.priceTime ?? null,
    ageDays: quoteAge,
    sourceStatus: q?.status ?? "NOT_RETRIEVED",
  };
  if (quoteFailed) blockers.push({ family: "quote", code: "QUOTE_UNAVAILABLE", message: `The price request failed (${q?.status}); retry. Mechanical dilution at price cannot run without it.`, retryable: true });
  else if (q?.price == null) block("quote", "QUOTE_MISSING", "No current price; mechanical dilution at price cannot run.");

  const filings = input.filings ?? [];
  const periodic = filings.filter((f) => PERIODIC_FORMS.has(f.form)).sort((a, b) => b.filingDate.localeCompare(a.filingDate))[0] ?? null;
  const periodEnd = periodic?.reportDate ?? null;
  const periodAge = periodEnd ? daysBetween(periodEnd, input.asOf) : null;
  // A foreign private issuer files one periodic report a year (20-F within four months of year end, 40-F
  // likewise) and furnishes interim results on 6-K; its periodic report is stale only once the next one is
  // overdue (2.5.15: TSM, TSEM, NBIS were STALE at 273 days).
  const annualCadence = periodic != null && /^(?:20-F|40-F)/.test(periodic.form);
  const staleAfterDays = annualCadence ? ANNUAL_REPORT_STALE_DAYS : QUARTERLY_REPORT_STALE_DAYS;
  const periodicState = !input.filings ? "UNAVAILABLE" : !periodic ? "MISSING" : periodAge != null && periodAge > staleAfterDays ? "STALE" : "READY";
  const latest6k = annualCadence
    ? filings.filter((f) => f.form === "6-K").map((f) => f.filingDate).sort().slice(-1)[0] ?? null
    : undefined;
  families.secPeriodicFiling = {
    state: periodicState,
    form: periodic?.form ?? null,
    filingDate: periodic?.filingDate ?? null,
    periodEnd,
    periodAgeDays: periodAge,
    cadence: periodic == null ? null : annualCadence ? "ANNUAL" : "QUARTERLY",
    staleAfterDays: periodic == null ? null : staleAfterDays,
    ...(latest6k !== undefined ? { latestInterim6k: latest6k } : {}),
    inlineXbrl: periodic?.isInlineXBRL ?? null,
    sourceStatus: input.filingsStatus,
  };
  if (periodicState !== "READY") block("secPeriodicFiling", `SEC_PERIODIC_${periodicState}`, periodic ? `Latest periodic filing covers ${periodEnd}, ${periodAge} days ago.` : "No SEC periodic filing found.");

  const extractable = periodicState === "READY" || periodicState === "STALE";
  for (const family of ["capitalStructure", "dilution"]) {
    families[family] = {
      state: extractable ? (periodic?.isInlineXBRL === false ? "PARTIAL" : "READY_TO_EXTRACT") : "UNAVAILABLE",
      evaluated: false,
      basis: extractable ? `${periodic?.form} for ${periodEnd}` : null,
      note: "Preflight checks inputs only; the extraction runs in build_valuation_evidence_pack.",
    };
  }

  const earnings8k = filings
    .filter((f) => f.form === "8-K" && (f.items ?? "").split(",").map((s) => s.trim()).includes("2.02"))
    .sort((a, b) => b.filingDate.localeCompare(a.filingDate))[0] ?? null;
  const guidanceAge = earnings8k ? daysBetween(earnings8k.filingDate, input.asOf) : null;
  families.guidance = {
    state: !input.filings ? "UNAVAILABLE" : !earnings8k ? "MISSING" : guidanceAge != null && guidanceAge > 120 ? "STALE" : "READY_TO_EXTRACT",
    latestEarningsRelease8k: earnings8k?.filingDate ?? null,
    ageDays: guidanceAge,
    evaluated: false,
  };

  const recent8k = filings.filter((f) => f.form === "8-K" && daysBetween(f.filingDate, input.asOf) <= 90);
  families.materialEvents = {
    state: !input.filings ? "UNAVAILABLE" : "READY",
    eightKCount90d: recent8k.length,
    latest8k: recent8k.map((f) => f.filingDate).sort().slice(-1)[0] ?? null,
  };

  // No provider returned estimates because every request failed (Yahoo errored, Alpha Vantage rate-limited):
  // that says nothing about analyst coverage, so the family is UNAVAILABLE with one retryable blocker (2.5.15).
  const providerStatuses = ((input.consensus?.providers ?? []) as Rec[]).map((p) => ({ provider: String(p.provider ?? ""), status: String(p.status ?? "") }));
  const consensusFailed = providerStatuses.length > 0
    && !providerStatuses.some((p) => p.status === "OK")
    && providerStatuses.some((p) => PROVIDER_FAILURE_STATUSES.has(p.status));
  const periodsOut = ((input.consensus?.periods ?? []) as Rec[]).slice(0, 2);
  const cells: Rec = {};
  for (const p of periodsOut) {
    const m = (p.metrics ?? {}) as Record<string, Rec>;
    for (const metric of CONSENSUS_METRICS) cells[`${p.label}.${metric}`] = m[metric]?.coverage ?? "PROVIDER_NOT_COVERED";
  }
  const covered = Object.values(cells).filter((c) => c === "PROVIDER_COVERED").length;
  families.consensus = {
    state: !input.consensus || consensusFailed ? "UNAVAILABLE" : covered === Object.keys(cells).length && covered > 0 ? "READY" : covered > 0 ? "PARTIAL" : "MISSING",
    cells: consensusFailed ? {} : cells,
    beyondFy1: "PROVIDER_NOT_COVERED",
    providerStatuses,
  };
  if (consensusFailed) {
    blockers.push({
      family: "consensus",
      code: "CONSENSUS_PROVIDER_ERROR",
      message: `No estimates were read: ${providerStatuses.map((p) => `${p.provider} ${p.status}`).join(", ")}; retry. Analyst coverage is unknown, not absent.`,
      retryable: true,
    });
  } else {
    for (const [cell, coverage] of Object.entries(cells)) {
      if (coverage !== "PROVIDER_COVERED") block("consensus", String(coverage), `${cell} is ${coverage}.`);
    }
  }

  families.evidenceStorage = { state: input.storageAvailable ? "READY" : "UNAVAILABLE", durable: input.storageAvailable };

  return {
    ticker: input.ticker.toUpperCase(),
    asOf: input.asOf,
    families,
    blockers,
    readyFamilies: Object.entries(families).filter(([, v]) => ["READY", "READY_TO_EXTRACT"].includes(String((v as Rec).state))).map(([k]) => k),
    ...AUTHORITY_BOUNDARY,
  };
}

// ── Evidence cut receipt ─────────────────────────────────────────────────────

export interface ComponentRecord {
  status: string;
  sourceTool: string;
  retrievedAt: string | null;
  data: unknown;
  warnings: unknown[];
  error: { code: string; message: string } | null;
}

function warningCodes(warnings: unknown[]): string[] {
  return warnings
    .map((w) => (w && typeof w === "object" ? (w as Rec).code : null))
    .filter((c): c is string => typeof c === "string");
}

/** Provider timestamps a component states (market time, filing dates, provider as-of). */
function providerTimestamps(data: unknown): Rec {
  const d = (data && typeof data === "object" ? data : {}) as Rec;
  const out: Rec = {};
  for (const key of ["priceTime", "filingDate", "periodEnd", "asOf", "acceptedAt", "reportDate"]) {
    if (typeof d[key] === "string") out[key] = d[key];
  }
  const source = (d.source && typeof d.source === "object" ? d.source : {}) as Rec;
  for (const key of ["filingDate", "periodEnd", "accessionNumber", "form", "filingType"]) {
    if (typeof source[key] === "string") out[`source.${key}`] = source[key];
  }
  return out;
}

/**
 * The receipt embedded in an evidence cut. `componentHashes` are SHA-256 of
 * each component's canonical JSON, computed by the caller (hashing is async
 * in the Worker).
 */
export function buildReceipt(input: {
  ticker: string;
  evidenceCutoff: string;
  serverVersion: string;
  buildSha: string | null;
  runtime: string;
  components: Record<string, ComponentRecord>;
  componentHashes: Record<string, string>;
  componentsSha256: string;
}): Rec {
  const names = Object.keys(input.components).sort();
  const failures = names.filter((n) => input.components[n].status !== "OK");
  return {
    ticker: input.ticker.toUpperCase(),
    evidenceCutoff: input.evidenceCutoff,
    serverVersion: input.serverVersion,
    buildSha: input.buildSha,
    runtime: input.runtime,
    hashAlgorithm: "sha256",
    canonicalization: CANONICALIZATION,
    componentsSha256: input.componentsSha256,
    components: names.map((name) => {
      const c = input.components[name];
      return {
        name,
        sourceTool: c.sourceTool,
        status: c.status,
        sha256: input.componentHashes[name],
        retrievedAt: c.retrievedAt,
        providerTimestamps: providerTimestamps(c.data),
        warningCodes: warningCodes(c.warnings),
        failure: c.error,
      };
    }),
    coverage: {
      componentCount: names.length,
      okCount: names.length - failures.length,
      failedOrLimited: failures,
      state: failures.length === 0 ? "COMPLETE" : failures.length === names.length ? "FAILED" : "PARTIAL",
      scope: "COMPONENT_WRAPPER_STATUS",
      evidenceCompleteness: "NOT_ASSERTED",
    },
  };
}

/**
 * The document an evidence cut stores: the authority boundary at top level,
 * one record per component, and the receipt as `provenance`. Its SHA-256 over
 * canonicalJson(document) is the cut's identity.
 */
export function evidenceCutDocument(input: {
  ticker: string;
  evidenceCutoff: string;
  components: Record<string, ComponentRecord>;
  receipt: Rec;
}): Rec {
  return {
    schema: EVIDENCE_CUT_SCHEMA,
    ticker: input.ticker.toUpperCase(),
    evidenceCutoff: input.evidenceCutoff,
    ...AUTHORITY_BOUNDARY,
    ...input.components,
    provenance: input.receipt,
  };
}

/** Classify a sub-tool's JSON text as a component record without altering its payload. */
export function componentFromToolText(sourceTool: string, text: string | null, retrievedAt: string, error: unknown = null): ComponentRecord {
  if (error != null || text == null) {
    const message = error instanceof Error ? error.message : String(error ?? "no response");
    return { status: "FAILED", sourceTool, retrievedAt, data: null, warnings: [], error: { code: "COMPONENT_FAILED", message } };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { status: "FAILED", sourceTool, retrievedAt, data: text, warnings: [], error: { code: "NON_JSON_RESPONSE", message: text.slice(0, 300) } };
  }
  return componentFromValue(sourceTool, parsed, retrievedAt);
}

export function componentFromValue(sourceTool: string, parsed: unknown, retrievedAt: string): ComponentRecord {
  let value = parsed;
  if (value && typeof value === "object" && typeof (value as Rec).ok === "boolean" && "data" in (value as Rec)) {
    const env = value as Rec;
    if (env.ok !== true) {
      const err = (env.error ?? {}) as Rec;
      return { status: "FAILED", sourceTool, retrievedAt, data: null, warnings: [], error: { code: String(err.code ?? "PROVIDER_ERROR"), message: String(err.message ?? "") } };
    }
    value = env.data;
  }
  const obj = (value && typeof value === "object" && !Array.isArray(value) ? value : {}) as Rec;
  const warnings = Array.isArray(obj.warnings) ? obj.warnings : [];
  if (obj.error === true || (obj.ok === false && obj.error)) {
    const err = (obj.error && typeof obj.error === "object" ? obj.error : obj) as Rec;
    return { status: "FAILED", sourceTool, retrievedAt, data: value, warnings, error: { code: String(err.code ?? "PROVIDER_ERROR"), message: String(err.message ?? "") } };
  }
  const limited = typeof obj.status === "string" && /PARTIAL|INCOMPLETE|STALE|NOT_AVAILABLE|NOT_FOUND|UNAVAILABLE|UNSUPPORTED|NO_DATA|NOT_DISCLOSED|UNCONFIGURED|FAILED/.test(obj.status);
  return {
    status: limited ? "LIMITED" : "OK",
    sourceTool,
    retrievedAt,
    data: value,
    warnings,
    error: limited ? { code: String(obj.status), message: String(obj.message ?? obj.reason ?? "") } : null,
  };
}
