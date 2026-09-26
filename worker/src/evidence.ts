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
  let state = "PROVIDER_COVERED";
  if (e.mean == null) state = "PROVIDER_NOT_COVERED";
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
  };
}

function agreement(entries: Rec[], metric: ConsensusMetric, policy: ConsensusPolicy): Rec {
  const valued = entries.filter((e) => typeof e.mean === "number");
  const providersCompared = valued.map((e) => e.provider);
  if (valued.length === 0) return { status: "NO_PROVIDER", providersCompared, relativeDiffPct: null, absoluteDiff: null };
  if (valued.length === 1) return { status: "SINGLE_PROVIDER", providersCompared, relativeDiffPct: null, absoluteDiff: null };
  const ends = valued.map((e) => String(e.fiscalYearEnd ?? ""));
  const endDays = ends.filter(Boolean).map(dayNumber);
  if (endDays.length === valued.length && Math.max(...endDays) - Math.min(...endDays) > 10) {
    return { status: "PERIOD_IDENTITY_MISMATCH", providersCompared, fiscalYearEnds: ends, relativeDiffPct: null, absoluteDiff: null };
  }
  const currencies = [...new Set(valued.map((e) => e.currency).filter((c): c is string => typeof c === "string"))];
  if (currencies.length > 1) return { status: "CURRENCY_MISMATCH", providersCompared, currencies, relativeDiffPct: null, absoluteDiff: null };
  const means = valued.map((e) => e.mean as number);
  const hi = Math.max(...means);
  const lo = Math.min(...means);
  const absoluteDiff = round(hi - lo, 6);
  const scale = Math.max(Math.abs(hi), Math.abs(lo));
  const relativeDiffPct = scale > 0 ? round(((hi - lo) / scale) * 100, 2) : 0;
  const withinAbsolute = metric === "eps" && absoluteDiff <= policy.epsAbsoluteTolerance;
  const status = withinAbsolute || relativeDiffPct <= policy.conflictTolerancePct ? "AGREED" : "CONFLICT";
  return {
    status,
    providersCompared,
    relativeDiffPct,
    absoluteDiff,
    currencyIdentity: currencies.length === 1 && valued.every((e) => e.currency === currencies[0]) ? "VERIFIED" : "UNVERIFIED",
  };
}

function metricCoverage(entries: Rec[], agreementStatus: string): string {
  if (!entries.some((e) => typeof e.mean === "number")) return "PROVIDER_NOT_COVERED";
  if (["CONFLICT", "PERIOD_IDENTITY_MISMATCH", "CURRENCY_MISMATCH"].includes(agreementStatus)) return "PROVIDER_CONFLICT";
  if (!entries.some((e) => e.state === "PROVIDER_COVERED")) return "INSUFFICIENT_ANALYST_COUNT";
  return "PROVIDER_COVERED";
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
): Rec {
  const horizon = Math.max(1, Math.min(5, Math.trunc(policy.horizonYears)));
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
      const agree = agreement(entries, metric, policy);
      const coverage = metricCoverage(entries, String(agree.status));
      summary[coverage] = (summary[coverage] ?? 0) + 1;
      metrics[metric] = {
        coverage,
        providers: entries,
        agreement: agree,
        dispersionStatistics: { highLowRange: "PER_PROVIDER", median: UNAVAILABLE_STATISTIC, standardDeviation: UNAVAILABLE_STATISTIC },
      };
    }
    periods.push({
      label,
      fiscalYear: fy0 ? Number(fy0.fiscalYearEnd.slice(0, 4)) + k : null,
      fiscalYearEnds,
      metrics,
    });
  }
  return {
    ticker: ticker.toUpperCase(),
    asOf,
    fiscalYearBasis: fy0 ?? { fiscalYearEnd: null, basis: "NO_PROVIDER_FISCAL_YEAR" },
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
      "Agreement between providers does not establish independence: they may redistribute the same underlying estimates.",
    ],
    ...AUTHORITY_BOUNDARY,
  };
}

// ── EPS revision windows ─────────────────────────────────────────────────────

const WINDOWS: [string, keyof EpsTrend][] = [["7d", "d7"], ["30d", "d30"], ["60d", "d60"], ["90d", "d90"]];

function revisionProvider(input: ProviderConsensusInput, p: ProviderPeriod): Rec {
  const trend = p.epsTrend;
  const current = trend?.current ?? null;
  const windows: Rec = {};
  const notReported: string[] = [];
  for (const [name, key] of WINDOWS) {
    const past = trend ? trend[key] : null;
    if (past == null) notReported.push(`${name}.mean`);
    windows[name] = {
      mean: past,
      change: past != null && current != null ? round(current - past, 6) : null,
      changePct: past != null && current != null && past !== 0 ? round(((current - past) / Math.abs(past)) * 100, 2) : null,
    };
  }
  const counts = p.epsRevisions ?? { up7d: null, down7d: null, up30d: null, down30d: null };
  for (const [k, v] of Object.entries(counts)) if (v == null) notReported.push(`revisionCounts.${k}`);
  return {
    provider: input.provider,
    providerPeriodLabel: p.providerPeriodLabel,
    fiscalYearEnd: p.fiscalYearEnd,
    current,
    windows,
    revisionCounts: counts,
    notReported,
    retrievedAt: input.retrievedAt,
    providerTimestamp: input.providerTimestamp,
  };
}

/** EPS estimate windows (7/30/60/90 days) and revision counts as each provider reports them for FY0 and FY+1. */
export function buildEpsRevisions(ticker: string, inputs: ProviderConsensusInput[], asOf: string): Rec {
  const fy0 = resolveFy0(inputs, asOf);
  const periods: Rec[] = [];
  for (const k of [0, 1]) {
    const providers = fy0
      ? inputs.flatMap((input) => input.periods
        .filter((p) => p.fiscalYearEnd && fiscalYearOffset(fy0.fiscalYearEnd, p.fiscalYearEnd) === k && (p.epsTrend || p.epsRevisions))
        .map((p) => revisionProvider(input, p)))
      : [];
    periods.push({ label: k === 0 ? "FY0" : "FY+1", coverage: providers.length ? "PROVIDER_COVERED" : "PROVIDER_NOT_COVERED", providers });
  }
  return {
    ticker: ticker.toUpperCase(),
    asOf,
    fiscalYearBasis: fy0 ?? { fiscalYearEnd: null, basis: "NO_PROVIDER_FISCAL_YEAR" },
    periods,
    revenueRevisions: { coverage: "PROVIDER_NOT_COVERED", note: "No configured provider publishes revenue estimate history." },
    analystCountChanges: { coverage: "PROVIDER_NOT_COVERED", note: "No configured provider publishes analyst adds or drops." },
    providers: inputs.map((i) => ({ provider: i.provider, status: i.status, retrievedAt: i.retrievedAt, message: i.message })),
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

const PERIODIC_FORMS = new Set(["10-K", "10-Q", "20-F", "40-F", "10-K/A", "10-Q/A", "20-F/A"]);

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
  families.quote = {
    state: q?.price != null ? (quoteAge != null && quoteAge > 4 ? "STALE" : "READY") : "MISSING",
    price: q?.price ?? null,
    currency: q?.currency ?? null,
    priceTime: q?.priceTime ?? null,
    ageDays: quoteAge,
    sourceStatus: q?.status ?? "NOT_RETRIEVED",
  };
  if (q?.price == null) block("quote", "QUOTE_MISSING", "No current price; mechanical dilution at price cannot run.");

  const filings = input.filings ?? [];
  const periodic = filings.filter((f) => PERIODIC_FORMS.has(f.form)).sort((a, b) => b.filingDate.localeCompare(a.filingDate))[0] ?? null;
  const periodEnd = periodic?.reportDate ?? null;
  const periodAge = periodEnd ? daysBetween(periodEnd, input.asOf) : null;
  const periodicState = !input.filings ? "UNAVAILABLE" : !periodic ? "MISSING" : periodAge != null && periodAge > 135 ? "STALE" : "READY";
  families.secPeriodicFiling = {
    state: periodicState,
    form: periodic?.form ?? null,
    filingDate: periodic?.filingDate ?? null,
    periodEnd,
    periodAgeDays: periodAge,
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

  const periodsOut = ((input.consensus?.periods ?? []) as Rec[]).slice(0, 2);
  const cells: Rec = {};
  for (const p of periodsOut) {
    const m = (p.metrics ?? {}) as Record<string, Rec>;
    for (const metric of CONSENSUS_METRICS) cells[`${p.label}.${metric}`] = m[metric]?.coverage ?? "PROVIDER_NOT_COVERED";
  }
  const covered = Object.values(cells).filter((c) => c === "PROVIDER_COVERED").length;
  families.consensus = {
    state: !input.consensus ? "UNAVAILABLE" : covered === Object.keys(cells).length && covered > 0 ? "READY" : covered > 0 ? "PARTIAL" : "MISSING",
    cells,
    beyondFy1: "PROVIDER_NOT_COVERED",
  };
  for (const [cell, coverage] of Object.entries(cells)) {
    if (coverage !== "PROVIDER_COVERED") block("consensus", String(coverage), `${cell} is ${coverage}.`);
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
  const limited = typeof obj.status === "string" && /NOT_AVAILABLE|NOT_FOUND|UNAVAILABLE|UNSUPPORTED|NO_DATA|NOT_DISCLOSED|UNCONFIGURED|FAILED/.test(obj.status);
  return {
    status: limited ? "LIMITED" : "OK",
    sourceTool,
    retrievedAt,
    data: value,
    warnings,
    error: limited ? { code: String(obj.status), message: String(obj.message ?? obj.reason ?? "") } : null,
  };
}
