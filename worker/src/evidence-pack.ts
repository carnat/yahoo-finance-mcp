/**
 * Evidence tools (2.5.0): consensus forecast curve, EPS revision windows,
 * evidence-quality preflight, the valuation evidence pack and its
 * content-addressed evidence cuts. The pack composes the canonical tools
 * (capital structure, dilution bridge, guidance, material filings) and keeps
 * each one's status, warnings and failure visible; it is an evidence
 * composition layer, not a valuation engine. Shared logic lives in evidence.ts.
 */

import {
  alphaVantageConsensusInput,
  buildConsensusCurve,
  buildEpsRevisions,
  buildReceipt,
  canonicalJson,
  compactTimestamp,
  componentFromToolText,
  componentFromValue,
  CONSENSUS_OBSERVATION_SCHEMA,
  consensusObservationKey,
  DEFAULT_CONSENSUS_POLICY,
  evidenceCutDocument,
  evidenceCutId,
  evidenceCutKey,
  evidenceQuality,
  parseEvidenceCutId,
  yahooConsensusInput,
  type ComponentRecord,
  type ConsensusPolicy,
  type FilingRow,
  type ProviderConsensusInput,
} from "./evidence.js";
import { getEvidenceStore, putOnce, sha256Hex } from "./evidence-store.js";
import { getServerVersion, getWorkerVar } from "./response.js";
import { majorPrice } from "./valuation.js";
import {
  extractCapitalStructure,
  extractDilutionBridge,
  extractGuidance,
  fetchAlphaVantageJson,
  getSubmissionsForTicker,
  listSecMaterialFilings,
  yGet,
} from "./yahoo-finance.js";

type Rec = Record<string, unknown>;

const ALPHA_VANTAGE_ESTIMATES_TTL_MS = 6 * 60 * 60 * 1000;

interface MarketQuote {
  price: number | null;
  currency: string | null;
  priceTime: string | null;
  status: string;
  message: string | null;
}

function rawNum(value: unknown): number | null {
  const v = value && typeof value === "object" ? (value as Rec).raw : value;
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function buildSha(): string | null {
  return getWorkerVar("BUILD_SHA")?.trim() || null;
}

/** Yahoo earningsTrend + quote and Alpha Vantage EARNINGS_ESTIMATES, fetched together; each provider fails on its own. */
async function consensusProviders(ticker: string): Promise<{ inputs: ProviderConsensusInput[]; quote: MarketQuote }> {
  const symbol = ticker.toUpperCase();
  const [yahoo, alpha] = await Promise.all([
    yGet(`https://query1.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(symbol)}?modules=earningsTrend,financialData,price`)
      .then((d) => ({ ok: true as const, d, at: new Date().toISOString() }), (e) => ({ ok: false as const, e, at: new Date().toISOString() })),
    fetchAlphaVantageJson("EARNINGS_ESTIMATES", { symbol }, ALPHA_VANTAGE_ESTIMATES_TTL_MS)
      .then((r) => ({ r, at: new Date().toISOString() })),
  ]);

  let yahooInput: ProviderConsensusInput;
  let quote: MarketQuote;
  const result = yahoo.ok ? ((yahoo.d as Rec)?.quoteSummary as Rec | undefined)?.result as Rec[] | undefined : undefined;
  const summary = result?.[0];
  if (summary) {
    const fd = (summary.financialData ?? {}) as Rec;
    const price = (summary.price ?? {}) as Rec;
    const trend = (((summary.earningsTrend ?? {}) as Rec).trend ?? []) as unknown[];
    yahooInput = yahooConsensusInput(trend, {
      retrievedAt: yahoo.at,
      financialCurrency: typeof fd.financialCurrency === "string" ? fd.financialCurrency : null,
    });
    const time = rawNum(price.regularMarketTime);
    quote = {
      price: rawNum(price.regularMarketPrice),
      currency: typeof price.currency === "string" ? price.currency : null,
      priceTime: time != null ? new Date(time * 1000).toISOString() : null,
      status: "OK",
      message: null,
    };
  } else {
    const message = yahoo.ok ? "Yahoo returned no quote summary." : (yahoo.e instanceof Error ? yahoo.e.message : String(yahoo.e));
    yahooInput = yahooConsensusInput([], { retrievedAt: yahoo.at, status: yahoo.ok ? "NO_DATA" : "PROVIDER_ERROR", message });
    quote = { price: null, currency: null, priceTime: null, status: yahoo.ok ? "NO_DATA" : "PROVIDER_ERROR", message };
  }

  const r = alpha.r;
  const alphaInput = alphaVantageConsensusInput(r.payload, {
    retrievedAt: r.fetchedAt ?? alpha.at,
    status: r.status === "OK" ? undefined : r.status,
    message: r.message ?? null,
  });
  return { inputs: [yahooInput, alphaInput], quote };
}

function consensusPolicy(horizonYears: number, minAnalystCount: number, conflictTolerancePct: number): ConsensusPolicy | string {
  if (!Number.isInteger(horizonYears) || horizonYears < 1 || horizonYears > 5) return "horizon_years must be an integer from 1 to 5.";
  if (!Number.isInteger(minAnalystCount) || minAnalystCount < 1 || minAnalystCount > 50) return "min_analyst_count must be an integer from 1 to 50.";
  if (!Number.isFinite(conflictTolerancePct) || conflictTolerancePct < 0 || conflictTolerancePct > 100) return "conflict_tolerance_pct must be from 0 to 100.";
  return { ...DEFAULT_CONSENSUS_POLICY, horizonYears, minAnalystCount, conflictTolerancePct };
}

async function writeConsensusObservation(ticker: string, curve: Rec, observedAt: string): Promise<Rec> {
  const key = consensusObservationKey(ticker, observedAt);
  const body = canonicalJson({
    schema: CONSENSUS_OBSERVATION_SCHEMA,
    ticker: ticker.toUpperCase(),
    observedAt,
    serverVersion: getServerVersion(),
    buildSha: buildSha(),
    curve,
  });
  const written = await putOnce(key, body, { ticker: ticker.toUpperCase(), kind: "consensus-observation" });
  return { status: written.status, key: written.status === "UNAVAILABLE" ? null : key, ...(written.message ? { message: written.message } : {}) };
}

export async function getConsensusForecastCurve(
  ticker: string,
  horizonYears = 5,
  minAnalystCount = DEFAULT_CONSENSUS_POLICY.minAnalystCount,
  conflictTolerancePct = DEFAULT_CONSENSUS_POLICY.conflictTolerancePct,
): Promise<string> {
  const policy = consensusPolicy(horizonYears, minAnalystCount, conflictTolerancePct);
  if (typeof policy === "string") return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: policy });
  const asOf = new Date().toISOString();
  const { inputs } = await consensusProviders(ticker);
  const curve = buildConsensusCurve(ticker, inputs, asOf, policy);
  // The first observation of the day is kept; later calls report ALREADY_STORED.
  const observation = await writeConsensusObservation(ticker, curve, asOf);
  return JSON.stringify({ ...curve, storage: { consensusObservation: observation } });
}

export async function getEpsRevisions(ticker: string): Promise<string> {
  const asOf = new Date().toISOString();
  const { inputs } = await consensusProviders(ticker);
  const revisions = buildEpsRevisions(ticker, inputs, asOf);
  const store = getEvidenceStore();
  let stored: Rec = { storageStatus: "UNAVAILABLE", observationDates: [] };
  if (store) {
    try {
      const keys = await store.list(`consensus-history/${ticker.toUpperCase()}/`, 400);
      stored = { storageStatus: "AVAILABLE", observationDates: keys.map((k) => k.split("/").pop()!.replace(/\.json$/, "")) };
    } catch (e) {
      stored = { storageStatus: "FAILED", observationDates: [], message: e instanceof Error ? e.message : String(e) };
    }
  }
  return JSON.stringify({ ...revisions, storedConsensusObservations: stored });
}

async function secFilingRows(ticker: string): Promise<{ rows: FilingRow[] | null; status: string }> {
  try {
    const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
    if (!cikPadded || !submissions) return { rows: null, status: "TICKER_NOT_FOUND" };
    const recent = ((submissions.filings as Rec)?.recent ?? {}) as Record<string, unknown[]>;
    const forms = (recent.form ?? []) as string[];
    const rows: FilingRow[] = forms.map((form, i) => ({
      form: String(form),
      filingDate: String(recent.filingDate?.[i] ?? ""),
      reportDate: recent.reportDate?.[i] ? String(recent.reportDate[i]) : null,
      items: recent.items?.[i] != null ? String(recent.items[i]) : null,
      isInlineXBRL: recent.isInlineXBRL?.[i] != null ? Number(recent.isInlineXBRL[i]) === 1 : null,
    })).filter((r) => r.filingDate);
    return { rows, status: "OK" };
  } catch (e) {
    return { rows: null, status: e instanceof Error ? `PROVIDER_ERROR: ${e.message}` : "PROVIDER_ERROR" };
  }
}

export async function getEvidenceQuality(ticker: string): Promise<string> {
  const asOf = new Date().toISOString();
  const [{ inputs, quote }, filings] = await Promise.all([consensusProviders(ticker), secFilingRows(ticker)]);
  const curve = buildConsensusCurve(ticker, inputs, asOf);
  return JSON.stringify(evidenceQuality({
    ticker,
    asOf,
    quote,
    filings: filings.rows,
    filingsStatus: filings.status,
    consensus: curve,
    storageAvailable: getEvidenceStore() != null,
  }));
}

async function runComponent(sourceTool: string, run: () => Promise<string>): Promise<ComponentRecord> {
  try {
    const text = await run();
    return componentFromToolText(sourceTool, text, new Date().toISOString());
  } catch (e) {
    return componentFromToolText(sourceTool, null, new Date().toISOString(), e);
  }
}

function notApplicable(sourceTool: string, code: string, message: string, retrievedAt: string): ComponentRecord {
  return { status: "NOT_APPLICABLE", sourceTool, retrievedAt, data: null, warnings: [], error: { code, message } };
}

/**
 * The valuation evidence pack: quote, consensus curve, EPS revisions,
 * evidence quality, current capital structure, dilution at the current
 * price, latest guidance and material filings, as one evidence cut. Its
 * receipt (`provenance`) hashes every component; the cut is stored in R2
 * when available and returned in full either way.
 */
export async function buildValuationEvidencePack(ticker: string, horizonYears = 5, persist = true): Promise<string> {
  const policy = consensusPolicy(horizonYears, DEFAULT_CONSENSUS_POLICY.minAnalystCount, DEFAULT_CONSENSUS_POLICY.conflictTolerancePct);
  if (typeof policy === "string") return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: policy });
  const symbol = ticker.toUpperCase();
  const cutoff = new Date().toISOString();

  const [{ inputs, quote }, filings] = await Promise.all([consensusProviders(symbol), secFilingRows(symbol)]);
  const curve = buildConsensusCurve(symbol, inputs, cutoff, policy);
  const major = quote.price != null ? majorPrice(quote.price, quote.currency) : null;

  const dilutionRun = (): Promise<ComponentRecord> => {
    if (!major) return Promise.resolve(notApplicable("extract_dilution_bridge", "PRICE_UNAVAILABLE", "No current price to run the dilution bridge at.", cutoff));
    if (major.currency !== "USD") {
      return Promise.resolve(notApplicable("extract_dilution_bridge", "NON_USD_LISTING", `The dilution bridge reads SEC filings for USD listings; ${symbol} is quoted in ${quote.currency}.`, cutoff));
    }
    return runComponent("extract_dilution_bridge", () => extractDilutionBridge(symbol, major.price, null, "USD", "latest", null, true));
  };
  const [capital, dilution, guidance, events] = await Promise.all([
    runComponent("extract_capital_structure", () => extractCapitalStructure(symbol, "latest", null, true)),
    dilutionRun(),
    runComponent("extract_guidance", () => extractGuidance(symbol, "latest")),
    runComponent("list_sec_material_filings", () => listSecMaterialFilings(symbol, null, 10)),
  ]);

  const components: Record<string, ComponentRecord> = {
    quote: {
      status: quote.price != null ? "OK" : "FAILED",
      sourceTool: "yahoo_quote_summary",
      retrievedAt: inputs[0].retrievedAt,
      data: { ...quote, majorUnitPrice: major?.price ?? null, majorUnitCurrency: major?.currency ?? null },
      warnings: [],
      error: quote.price != null ? null : { code: quote.status, message: quote.message ?? "No price." },
    },
    evidenceQuality: componentFromValue("get_evidence_quality", evidenceQuality({
      ticker: symbol, asOf: cutoff, quote, filings: filings.rows, filingsStatus: filings.status, consensus: curve, storageAvailable: getEvidenceStore() != null,
    }), cutoff),
    consensus: componentFromValue("get_consensus_forecast_curve", curve, cutoff),
    epsRevisions: componentFromValue("get_eps_revisions", buildEpsRevisions(symbol, inputs, cutoff), cutoff),
    currentCapitalStructure: capital,
    currentDilution: dilution,
    latestGuidance: guidance,
    materialEvents: events,
  };
  // A pack whose providers all failed is still evidence of that; its components say FAILED.
  if (inputs.every((i) => i.periods.length === 0)) components.consensus.status = "LIMITED";

  const names = Object.keys(components);
  const hashes = Object.fromEntries(await Promise.all(names.map(async (n) => [n, await sha256Hex(canonicalJson(components[n]))] as const)));
  const receipt = buildReceipt({
    ticker: symbol,
    evidenceCutoff: cutoff,
    serverVersion: getServerVersion(),
    buildSha: buildSha(),
    runtime: "cloudflare_worker",
    components,
    componentHashes: hashes,
    componentsSha256: await sha256Hex(canonicalJson(components)),
  });
  const document = evidenceCutDocument({ ticker: symbol, evidenceCutoff: cutoff, components, receipt });
  const bytes = canonicalJson(document);
  const sha = await sha256Hex(bytes);
  const key = evidenceCutKey(symbol, compactTimestamp(cutoff), sha);

  const stored = persist
    ? await putOnce(key, bytes, { ticker: symbol, sha256: sha, serverVersion: getServerVersion() })
    : { status: "SKIPPED" as const };
  const observation = persist ? await writeConsensusObservation(symbol, curve, cutoff) : { status: "SKIPPED", key: null };

  return JSON.stringify({
    ...document,
    evidenceCut: {
      evidenceCutId: evidenceCutId(symbol, cutoff, sha),
      contentSha256: sha,
      storageStatus: stored.status,
      storageKey: stored.status === "STORED" || stored.status === "ALREADY_STORED" ? key : null,
      ...(stored.message ? { storageMessage: stored.message } : {}),
      consensusObservation: observation,
      verification: "contentSha256 is SHA-256 over canonical JSON (sorted keys, no whitespace, ECMAScript numbers) of this payload without the evidenceCut field.",
    },
  });
}

export async function getEvidenceCut(id: string): Promise<string> {
  const parsed = parseEvidenceCutId(id);
  if (!parsed) return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "evidence_cut_id must look like ec1_<TICKER>_<YYYYMMDDTHHMMSSZ>_<sha256>." });
  const store = getEvidenceStore();
  if (!store) return JSON.stringify({ error: true, code: "STORAGE_UNAVAILABLE", message: "Durable evidence storage is not configured; evidence cuts cannot be retrieved." });
  let text: string | null;
  try {
    text = await store.get(parsed.key);
  } catch (e) {
    return JSON.stringify({ error: true, code: "STORAGE_ERROR", message: e instanceof Error ? e.message : String(e) });
  }
  if (text == null) return JSON.stringify({ error: true, code: "NOT_FOUND", message: `No evidence cut is stored under ${id}.` });
  const actual = await sha256Hex(text);
  let document: unknown = null;
  try {
    document = JSON.parse(text);
  } catch {
    document = null;
  }
  return JSON.stringify({
    evidenceCutId: id,
    storageKey: parsed.key,
    integrity: actual === parsed.sha256 && document != null ? "VERIFIED" : "MISMATCH",
    expectedSha256: parsed.sha256,
    actualSha256: actual,
    document,
  });
}

export async function listEvidenceCuts(ticker: string, limit = 20): Promise<string> {
  const store = getEvidenceStore();
  const symbol = ticker.toUpperCase();
  if (!store) return JSON.stringify({ ticker: symbol, storageStatus: "UNAVAILABLE", cuts: [] });
  const cap = Math.max(1, Math.min(100, Math.trunc(limit)));
  try {
    const keys = await store.list(`evidence-cuts/${symbol}/`, 1000);
    const cuts = keys
      .map((key) => /^evidence-cuts\/([^/]+)\/(\d{8}T\d{6}Z)\/([0-9a-f]{64})\.json$/.exec(key))
      .filter((m): m is RegExpExecArray => m !== null)
      .map((m) => ({ evidenceCutId: `ec1_${m[1]}_${m[2]}_${m[3]}`, cutoff: m[2], storageKey: m[0] }))
      .sort((a, b) => b.cutoff.localeCompare(a.cutoff))
      .slice(0, cap);
    return JSON.stringify({ ticker: symbol, storageStatus: "AVAILABLE", cuts });
  } catch (e) {
    return JSON.stringify({ ticker: symbol, storageStatus: "FAILED", cuts: [], message: e instanceof Error ? e.message : String(e) });
  }
}
