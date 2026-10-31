/**
 * Valuation tools: historical valuation context, metric source reconciliation and the valuation snapshot, with their
 * helpers (daily history, periodic reports, cover-page share counts) (Phase 5 move out of yahoo-finance.ts). The
 * arithmetic stays in valuation.ts, valuation-history.ts and metric-reconciliation.ts. Helpers shared with other
 * domains live in their own modules and are imported; yahoo-finance.ts re-exports every moved name, so tools.ts and
 * the parity bundle are unchanged.
 */

import { adsRatio, majorPrice, valuationSnapshot, type MarketInputs } from "./valuation.js";
import { coverReadsNeeded, coverShareCounts, foreignFiler, historicalValuation, latestShareCount, peerMedians, reportingCurrencies, taxonomyOf, valuationDates, type Bar, type PeriodicFiling, type Split } from "./valuation-history.js";
import { annualFilingsFromSubmissions, latestAnnualCoverageWarning } from "./companyfacts-coverage.js";
import { DEFAULT_TOLERANCE_PCT, METRICS as RECONCILE_METRICS, RECONCILE_PERIOD_HELP, isReconcilePeriod, metricReconciliation, resolvePeriod } from "./metric-reconciliation.js";
import { AUTHORITY_BOUNDARY } from "./evidence.js";
import { edgarGetHtml, edgarGetJson, getEdgarTickerCikMap, getSubmissionsForTicker, resolveCikForTicker } from "./clients/edgar.js";
import { secArchiveDocumentUrl, secCompanyFactsUrl, yahooChartUrl } from "./clients/endpoints.js";
import { enc, fetchTimeseriesByDate, valuationMarketInputs, yGet } from "./clients/yahoo.js";
import { dailyBarDate, parseObjectJson } from "./util.js";
import { extractDilutionBridge, extractCapitalStructure } from "./exposure.js";
import { peerValuationError } from "./fundamentals.js";
import { earningsReleasePicks, readReleaseText } from "./earnings.js";

// ── Historical valuation context and metric reconciliation (2.5.4) ────────

/** Daily closes (Yahoo split-adjusted), split events and the quote currency from fromDate to now. */
async function dailyHistory(ticker: string, fromDate: string): Promise<{ bars: Bar[]; splits: Split[]; currency: string | null } | null> {
  const p1 = Math.floor(Date.parse(`${fromDate}T00:00:00Z`) / 1000);
  const p2 = Math.floor(Date.now() / 1000);
  const d = (await yGet(yahooChartUrl(enc(ticker), `period1=${p1}&period2=${p2}&interval=1d&events=split`), false)) as Record<string, unknown>;
  const result = (d?.chart as Record<string, unknown[]> | undefined)?.result?.[0] as Record<string, unknown> | undefined;
  if (!result) return null;
  const meta = (result.meta as Record<string, unknown> | undefined) ?? {};
  const tz = typeof meta.exchangeTimezoneName === "string" ? meta.exchangeTimezoneName : "UTC";
  const timestamps = (result.timestamp as number[]) ?? [];
  const closes = (((result.indicators as Record<string, unknown[]>)?.quote?.[0] as Record<string, (number | null)[]>) ?? {}).close ?? [];
  const bars: Bar[] = [];
  timestamps.forEach((t, i) => {
    const date = dailyBarDate(t, tz);
    const close = closes[i];
    if (date && typeof close === "number" && Number.isFinite(close)) bars.push({ date, close });
  });
  const rawSplits = (((result.events as Record<string, unknown> | undefined)?.splits ?? {}) as Record<string, Record<string, unknown>>);
  const splits: Split[] = Object.values(rawSplits)
    .map((e) => ({ date: dailyBarDate(Number(e.date), tz) ?? "", ratio: Number(e.numerator) / Number(e.denominator) }))
    .filter((e) => e.date !== "" && Number.isFinite(e.ratio) && e.ratio > 0)
    .sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
  return { bars, splits, currency: typeof meta.currency === "string" ? meta.currency : null };
}

// Cover pages sit at the top of a periodic report with the header contexts; the first 900 KB is read, not
// the whole report (ASTS's 10-Q is 3.5 MB, its class counts end near 194 KB).
const COVER_READ_MAX_BYTES = 900_000;

const PERIODIC_REPORT_FORMS = new Set(["10-K", "10-Q", "20-F", "40-F"]);

/** The filer's periodic reports in SEC's recent submissions, with their primary documents. */
function periodicReports(submissions: Record<string, unknown> | null, cikPadded: string | null): (PeriodicFiling & { documentUrl: string })[] {
  if (!submissions || !cikPadded) return [];
  const recent = ((submissions.filings as Record<string, unknown> | undefined)?.recent ?? {}) as Record<string, unknown[]>;
  const out: (PeriodicFiling & { documentUrl: string })[] = [];
  const forms = (recent.form ?? []) as string[];
  for (let i = 0; i < forms.length; i++) {
    const form = String(forms[i] ?? "").toUpperCase();
    const acc = String(recent.accessionNumber?.[i] ?? "");
    const doc = String(recent.primaryDocument?.[i] ?? "");
    if (!PERIODIC_REPORT_FORMS.has(form) || !acc || !doc) continue;
    out.push({
      accessionNumber: acc,
      form,
      filed: String(recent.filingDate?.[i] ?? ""),
      reportDate: recent.reportDate?.[i] ? String(recent.reportDate[i]) : null,
      documentUrl: secArchiveDocumentUrl(parseInt(cikPadded, 10), acc.replace(/-/g, ""), doc),
    });
  }
  return out;
}

/** Per-class cover-page share counts from the periodic reports companyfacts cannot give a point-in-time count for. */
async function coverCountsFor(companyfacts: unknown, reports: (PeriodicFiling & { documentUrl: string })[], dates: string[]): Promise<Record<string, Record<string, unknown>>> {
  const out: Record<string, Record<string, unknown>> = {};
  for (const n of coverReadsNeeded(companyfacts, reports, dates)) {
    const url = (n as PeriodicFiling & { documentUrl: string }).documentUrl;
    const html = await edgarGetHtml(url, COVER_READ_MAX_BYTES).catch(() => null);
    out[n.accessionNumber] = html ? { ...coverShareCounts(html), documentUrl: url } : { status: "NOT_READ", value: null, documentUrl: url };
  }
  return out;
}

async function valuationHistoryFor(ticker: string, dates: string[] | null, fromDate: string): Promise<Record<string, unknown>> {
  const upper = ticker.toUpperCase();
  const cikPadded = await resolveCikForTicker(ticker);
  if (!cikPadded) {
    const tickerIndex = await getEdgarTickerCikMap().catch(() => null);
    const code = tickerIndex ? "NO_SEC_REGISTRANT" : "SEC_LOOKUP_UNAVAILABLE";
    return { ticker: upper, status: code, code, retryable: !tickerIndex, ...AUTHORITY_BOUNDARY };
  }
  const [facts, history] = await Promise.all([
    edgarGetJson(secCompanyFactsUrl(cikPadded)).catch(() => null),
    dailyHistory(ticker, fromDate).catch(() => null),
  ]);
  if (!facts) return { ticker: upper, status: "COMPANYFACTS_NOT_AVAILABLE", code: "COMPANYFACTS_NOT_AVAILABLE", retryable: true, ...AUTHORITY_BOUNDARY };
  if (!history || history.bars.length === 0) return { ticker: upper, status: "PRICE_HISTORY_NOT_AVAILABLE", code: "PRICE_HISTORY_NOT_AVAILABLE", retryable: true, ...AUTHORITY_BOUNDARY };
  const useDates = dates ?? valuationDates(null, history.bars[history.bars.length - 1].date).dates;
  const { currency } = taxonomyOf(facts);
  const priceCurrency = history.currency ? majorPrice(1, history.currency).currency : null;
  const warnings: Record<string, unknown>[] = [];
  // One FX series per reporting currency the dates use (2.5.32, F-029: NBIS reported in RUB, then USD).
  const fxByCurrency: Record<string, { pair: string; bars: Bar[] }> = {};
  if (priceCurrency) {
    for (const c of reportingCurrencies(facts, useDates)) {
      if (c === priceCurrency) continue;
      const pair = `${c}${priceCurrency}=X`;
      const fxHistory = await dailyHistory(pair, fromDate).catch(() => null);
      fxByCurrency[c] = { pair, bars: fxHistory?.bars ?? [] };
    }
  }
  const fx = currency ? fxByCurrency[currency] ?? null : null;
  let ratio: number | null = null;
  if (foreignFiler(facts)) {
    const market = await valuationMarketInputs(ticker).catch(() => null);
    const sec = latestShareCount(facts);
    const quoted = market?.sharesOutstanding ?? null;
    const r = sec != null && quoted != null ? adsRatio(sec, quoted) : null;
    if (r != null && r !== 1) {
      ratio = r;
      warnings.push({ code: "ADR_RATIO_APPLIED", message: `SEC counts ${sec} ordinary shares; Yahoo counts ${quoted} quoted shares, ${r} ordinary shares each. Share counts are divided by ${r} at every date.`, severity: "info" });
    }
  }
  const { submissions } = await getSubmissionsForTicker(ticker).catch(() => ({ submissions: null }));
  const reports = periodicReports(submissions, cikPadded);
  const coverCounts = await coverCountsFor(facts, reports, useDates);
  const periodicFilings = reports.map(({ accessionNumber, form, filed, reportDate }) => ({ accessionNumber, form, filed, reportDate }));
  const out = historicalValuation({ ticker, dates: useDates, companyfacts: facts, bars: history.bars, priceCurrency: history.currency, splits: history.splits, fx, fxByCurrency, adsRatio: ratio, coverCounts, periodicFilings });
  if (!submissions) warnings.push({ code: "SUBMISSIONS_NOT_AVAILABLE", message: "SEC submissions could not be read, so no cover page was read; filers without an undimensioned share count have no market cap. Retry.", severity: "warning" });
  out.warnings = warnings;
  return out;
}

/** Historical valuation context (2.5.4): point-in-time market value, EV and multiples at dates, for a ticker and caller-named peers. */
export async function getHistoricalValuationContext(ticker: string, dates: string[] | null = null, peers: string[] | null = null): Promise<string> {
  let requested: string[] | null = null;
  if (dates && dates.length > 0) {
    const check = valuationDates(dates.map((d) => String(d).trim()), null);
    if (check.error) return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: check.error });
    requested = check.dates;
  }
  const peerList = [...new Set((peers ?? []).map((p) => String(p).trim().toUpperCase()).filter((p) => p !== "" && p !== ticker.toUpperCase()))];
  if (peerList.length > 5) return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "At most 5 peers." });
  const today = new Date().toISOString().slice(0, 10);
  const earliestRaw = requested ? requested[0] : `${Number(today.slice(0, 4)) - 5}${today.slice(4)}`;
  const earliest = earliestRaw.slice(4) === "-02-29" ? `${earliestRaw.slice(0, 4)}-02-28` : earliestRaw;
  const fromDate = new Date(Date.parse(`${earliest}T00:00:00Z`) - 15 * 86_400_000).toISOString().slice(0, 10);
  const subject = await valuationHistoryFor(ticker, requested, fromDate);
  const finalDates = requested ?? ((subject.points ?? []) as Record<string, unknown>[]).map((p) => String(p.date));
  const peerResults: Record<string, unknown>[] = [];
  // One peer at a time: each parsed companyfacts document can be tens of megabytes, and the
  // Worker's memory is bounded; only one is held at once.
  for (const p of peerList) {
    peerResults.push(await valuationHistoryFor(p, finalDates.length > 0 ? finalDates : null, fromDate));
  }
  return JSON.stringify({
    ticker: ticker.toUpperCase(),
    status: String(subject.status ?? "NOT_AVAILABLE"),
    dates: finalDates,
    subject,
    peers: peerResults,
    peerMedians: peerList.length > 0 && finalDates.length > 0 ? peerMedians(finalDates, peerResults.filter((p) => Array.isArray(p.points))) : [],
    notes: [
      "Peers are the caller's; their medians are unweighted and count only peers with that multiple on that date. Each peer's denominators are its own fiscal periods, named in its points.",
      "The latest date's close can be intraday while the market is open.",
    ],
    ...AUTHORITY_BOUNDARY,
  });
}

const RELEASE_READ_MAX = 4;
const RECONCILE_INCOME_TYPES = ["TotalRevenue", "NetIncome", "NetIncomeCommonStockholders", "OperatingIncome", "DilutedEPS"];

/** Metric source reconciliation (2.5.4): one metric for one period across SEC XBRL, the issuer release and Yahoo. */
export async function reconcileMetricSources(ticker: string, metric: string, period = "latest", tolerancePct: number = DEFAULT_TOLERANCE_PCT): Promise<string> {
  const upper = ticker.toUpperCase();
  if (!(metric in RECONCILE_METRICS)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: `metric must be one of: ${Object.keys(RECONCILE_METRICS).join(", ")}` });
  }
  // A period it cannot read is an input error before any fetch, not ok:true with INVALID_PERIOD (2.26.5, F60).
  if (!isReconcilePeriod(period)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: RECONCILE_PERIOD_HELP });
  }
  const tolerance = Number.isFinite(tolerancePct) ? Math.min(10, Math.max(0, tolerancePct)) : DEFAULT_TOLERANCE_PCT;
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded) {
    const tickerIndex = await getEdgarTickerCikMap().catch(() => null);
    const code = tickerIndex ? "NO_SEC_REGISTRANT" : "SEC_LOOKUP_UNAVAILABLE";
    return JSON.stringify({ ticker: upper, metric, status: code, code, retryable: !tickerIndex });
  }
  const facts = await edgarGetJson(secCompanyFactsUrl(cikPadded)).catch(() => null);
  if (!facts) return JSON.stringify({ ticker: upper, metric, status: "COMPANYFACTS_NOT_AVAILABLE", code: "COMPANYFACTS_NOT_AVAILABLE", retryable: true });
  // The latest annual report may be missing from companyfacts (2.5.30, F-015): said on every result, found or not.
  const coverageWarning = latestAnnualCoverageWarning(annualFilingsFromSubmissions(submissions), facts);
  const resolved = resolvePeriod(facts, metric, period);
  if (resolved.status !== "OK") return JSON.stringify({ ticker: upper, metric, ...resolved, ...(coverageWarning ? { warnings: [coverageWarning] } : {}) });
  const cikInt = parseInt(cikPadded, 10);
  const end = String(resolved.periodEnd);
  // Results releases (Item 2.02 8-Ks and 8-K/As) filed within 100 days after the period ended. The newest
  // RELEASE_READ_MAX are read, so a later correction is never crowded out by earlier filings; they are kept
  // oldest first, and the latest with a scoped figure wins (an early Item 2.02 8-K, as ASTS filed on
  // 2026-07-15, is not always the results release).
  const inWindow = submissions
    ? earningsReleasePicks(submissions, cikInt, 40, true)
      .filter((p) => p.filingDate > end && (Date.parse(`${p.filingDate}T00:00:00Z`) - Date.parse(`${end}T00:00:00Z`)) / 86_400_000 <= 100)
      .sort((a, b) => (a.filingDate < b.filingDate ? -1 : a.filingDate > b.filingDate ? 1 : a.accessionNumber < b.accessionNumber ? -1 : a.accessionNumber > b.accessionNumber ? 1 : 0))
    : [];
  const candidates = inWindow.slice(-RELEASE_READ_MAX);
  const notRead = inWindow.slice(0, inWindow.length - candidates.length);
  const annual = resolved.periodType === "ANNUAL";
  const spec = RECONCILE_METRICS[metric];
  const readReleases = async (): Promise<Record<string, unknown>[]> => {
    const read: Record<string, unknown>[] = [];
    for (const c of candidates) {
      const r: Record<string, unknown> = await readReleaseText(cikInt, c.accessionNumber, c.primaryUrl)
        .then((x): Record<string, unknown> => ({ ...x, filingDate: c.filingDate, accessionNumber: c.accessionNumber }))
        .catch((): Record<string, unknown> => ({ status: "NOT_READ", url: c.primaryUrl, filingDate: c.filingDate, accessionNumber: c.accessionNumber, text: null }));
      read.push(r);
    }
    return read;
  };
  const [releases, yahooRows] = await Promise.all([
    readReleases(),
    fetchTimeseriesByDate(ticker, annual ? "annual" : "quarterly", spec.kind === "instant" ? ["CashAndCashEquivalents"] : RECONCILE_INCOME_TYPES).catch(() => null),
  ]);
  const out = metricReconciliation({ ticker, metric, period: resolved, companyfacts: facts, releases, yahooRows, tolerancePct: tolerance });
  const warnings: Record<string, unknown>[] = [];
  const unread = releases.filter((r) => r.status === "NOT_READ");
  if (unread.length > 0) warnings.push({ code: "RELEASE_TEXT_NOT_AVAILABLE", message: `${unread.length} earnings release(s) could not be read from SEC; retry.`, severity: "warning" });
  if (yahooRows == null) warnings.push({ code: "YAHOO_NOT_AVAILABLE", message: "Yahoo's statements could not be read; retry.", severity: "warning" });
  out.releaseCandidates = { inWindow: inWindow.length, read: candidates.length, notRead: notRead.map((p) => ({ filingDate: p.filingDate, accessionNumber: p.accessionNumber, form: p.form })) };
  if (notRead.length > 0) warnings.push({ code: "OLDER_RELEASE_CANDIDATES_NOT_READ", message: `${notRead.length} older Item 2.02 filing(s) in the 100-day window were not read; the ${candidates.length} newest were.`, severity: "info" });
  if (coverageWarning) warnings.push(coverageWarning);
  out.retryable = unread.length > 0 || yahooRows == null;
  out.warnings = warnings;
  return JSON.stringify(out);
}

// ── Valuation snapshot and peer multiples (2.4.0) ─────────────────────────

const SEC_UNAVAILABLE_STATUSES = new Set(["TICKER_NOT_FOUND", "FILING_NOT_FOUND_TRY_OTHER_TYPE", "FILING_TEXT_NOT_AVAILABLE"]);

export async function getValuationSnapshot(
  ticker: string,
  price: number | null = null,
  filingType = "latest",
  includeSecFilings = true,
): Promise<string> {
  if (price != null && !(Number.isFinite(price) && price > 0)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "price must be a positive number when supplied." });
  }
  let market: MarketInputs;
  try {
    market = await valuationMarketInputs(ticker);
  } catch (e) {
    // As compare_peer_valuations (F57): an unknown ticker is TICKER_NOT_FOUND, and no provider URL is shown (3.2.1).
    const failure = peerValuationError(ticker.toUpperCase(), e instanceof Error ? e.message : String(e));
    return JSON.stringify({
      error: true,
      code: failure.code,
      message: failure.code === "TICKER_NOT_FOUND" ? failure.message : `Yahoo quote summary failed for ${ticker.toUpperCase()}: ${failure.message}`,
    });
  }
  const quote = price ?? market.price;
  const secWarnings: Record<string, unknown>[] = [];
  let bridge: Record<string, unknown> | null = null;
  let capital: Record<string, unknown> | null = null;
  if (quote != null && includeSecFilings) {
    const major = majorPrice(quote, market.currency);
    if (major.currency == null || major.currency === "USD") {
      const [b, c] = await Promise.all([
        extractDilutionBridge(ticker, major.price, null, "USD", filingType, null, true).then(parseObjectJson),
        extractCapitalStructure(ticker, filingType, null, false).then(parseObjectJson),
      ]);
      if (b.basis === "MECHANICAL_COMPANY_DISCLOSED") bridge = b;
      if (c.basis === "COMPANY_DISCLOSED") capital = c;
      const status = String(b.status ?? b.code ?? c.status ?? "");
      if (!bridge || !capital) {
        secWarnings.push({
          code: "SEC_FILINGS_UNAVAILABLE",
          message: SEC_UNAVAILABLE_STATUSES.has(status) ? `SEC filings were not read (${status}); Yahoo figures are used.` : "SEC filings could not be read in full; Yahoo figures fill the gaps.",
          severity: "info",
        });
      }
    } else {
      secWarnings.push({ code: "NON_USD_LISTING", message: `SEC filings are read only for USD listings; ${ticker.toUpperCase()} is quoted in ${market.currency}, so Yahoo figures are used.`, severity: "info" });
    }
  }
  return JSON.stringify(valuationSnapshot({ ticker: ticker.toUpperCase(), market, suppliedPrice: price, bridge, capital, secWarnings }));
}


