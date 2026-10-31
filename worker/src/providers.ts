/**
 * Scarce-provider and Companies House tools: Alpha Vantage and Finnhub fetchers with provider denial memory, expanded
 * institutional ownership, the historical put/call ratio and UK Companies House filings (Phase 5 move out of
 * yahoo-finance.ts). Helpers shared with other domains still live in yahoo-finance.ts and are imported back;
 * yahoo-finance.ts re-exports every moved name, so tools.ts and the parity bundle are unchanged.
 */

import { BoundedTtlCache } from "./cache.js";
import { countCacheEvent } from "./request-context.js";
import { getWorkerVar, mcpFailure, ErrorCode } from "./response.js";
import { pickCompaniesHouseMatch, companiesHouseFilings } from "./capital-structure.js";
import {
  fetchProviderWithTimeout,
  type ProviderJsonResult,
  providerCacheKey,
  getProviderCache,
  UA,
  providerHttpStatus,
  providerMessageStatus,
  setProviderCache,
  providerCacheName,
} from "./clients/provider-http.js";
import { _str, clampInt } from "./util.js";
import { finnhubEligibility, resolveNewsCompanyIdentity } from "./news/identity.js";

const ALPHA_VANTAGE_API = "https://www.alphavantage.co/query";
const FINNHUB_API = "https://finnhub.io/api/v1";
const ALPHA_HISTORICAL_PUT_CALL_TTL_MS = 365 * 24 * 60 * 60 * 1000;
const PROVIDER_OWNERSHIP_TTL_MS = 7 * 24 * 60 * 60 * 1000;

// Provider refusals that repeat until something changes (a plan entitlement,
// a daily quota) are remembered per endpoint, so each call does not spend a
// request, and quota, only to be refused again.
const PROVIDER_ENTITLEMENT_DENIAL_TTL_MS = 6 * 60 * 60 * 1000;
const providerDenials = new BoundedTtlCache<ProviderJsonResult>(100);

function rememberedProviderDenial(denialKey: string): ProviderJsonResult | null {
  const denial = providerDenials.get(denialKey);
  if (!denial) return null;
  countCacheEvent(providerCacheName(denialKey), "memoryHits");
  const until = denial.deniedUntil ? ` until ${denial.deniedUntil}` : "";
  return { ...denial, providerAttempted: false, cacheStatus: "HIT_PROCESS", message: `${denial.message ?? denial.status} (remembered${until}; not retried)` };
}

function rememberProviderDenial(denialKey: string, result: ProviderJsonResult): ProviderJsonResult {
  const now = Date.now();
  if (result.status === "ENTITLEMENT_REQUIRED" || result.status === "AUTH_ERROR") {
    const deniedUntil = new Date(now + PROVIDER_ENTITLEMENT_DENIAL_TTL_MS).toISOString();
    providerDenials.set(denialKey, { ...result, deniedUntil }, PROVIDER_ENTITLEMENT_DENIAL_TTL_MS);
    return { ...result, deniedUntil };
  } else if (result.status === "RATE_LIMIT" && /per day|daily/i.test(result.message ?? "")) {
    const d = new Date(now);
    const ttl = Math.max(60_000, Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1) - now);
    const deniedUntil = new Date(now + ttl).toISOString();
    providerDenials.set(denialKey, { ...result, deniedUntil }, ttl);
    return { ...result, deniedUntil };
  }
  return result;
}

export async function fetchAlphaVantageJson(
  operation: string,
  params: Record<string, unknown>,
  ttlMs: number,
): Promise<ProviderJsonResult> {
  const safeParams = { function: operation, ...params };
  const cacheKey = providerCacheKey("alpha_vantage", operation, safeParams);
  const cached = await getProviderCache(cacheKey, ttlMs);
  if (cached) return cached;
  const denialKey = "alpha_vantage:quota";
  const denied = rememberedProviderDenial(denialKey);
  if (denied) return denied;
  const apiKey = getWorkerVar("ALPHA_VANTAGE_API_KEY") ?? getWorkerVar("ALPHAVANTAGE_API_KEY");
  const publicQuery = new URLSearchParams({
    ...Object.fromEntries(Object.entries(safeParams).map(([key, value]) => [key, String(value)])),
    apikey: "REDACTED",
  });
  const publicUrl = `${ALPHA_VANTAGE_API}?${publicQuery.toString()}`;
  if (!apiKey) return { payload: null, status: "SOURCE_UNCONFIGURED", publicUrl, providerAttempted: false, cacheStatus: "MISS" };
  const requestQuery = new URLSearchParams(publicQuery);
  requestQuery.set("apikey", apiKey);
  try {
    const response = await fetchProviderWithTimeout(
      `${ALPHA_VANTAGE_API}?${requestQuery.toString()}`,
      { headers: { "User-Agent": UA } },
      30_000,
    );
    const retryAfter = response.headers.get("Retry-After") ?? undefined;
    if (!response.ok) {
      const body = (await response.text()).replaceAll(apiKey, "REDACTED");
      return {
        payload: null,
        status: providerHttpStatus(response.status, body),
        publicUrl,
        providerAttempted: true,
        cacheStatus: "MISS",
        httpStatus: response.status,
        message: body.slice(0, 500) || undefined,
        retryAfter,
      };
    }
    const rawJson = await response.json();
    if (!rawJson || typeof rawJson !== "object" || Array.isArray(rawJson)) {
      return { payload: null, status: "PROVIDER_ERROR", publicUrl, providerAttempted: true, cacheStatus: "MISS", message: "Alpha Vantage returned a non-object payload." };
    }
    const json = rawJson as Record<string, unknown>;
    const providerMessage = String(json.Note ?? json.Information ?? json["Error Message"] ?? "").replaceAll(apiKey, "REDACTED");
    if (providerMessage) {
      return rememberProviderDenial(denialKey, { payload: null, status: providerMessageStatus(providerMessage), publicUrl, providerAttempted: true, cacheStatus: "MISS", message: providerMessage });
    }
    const fetchedAt = new Date().toISOString();
    await setProviderCache(cacheKey, json, publicUrl, fetchedAt, ttlMs);
    return { payload: json, status: "OK", publicUrl, providerAttempted: true, cacheStatus: "MISS", fetchedAt };
  } catch (error) {
    const message = error instanceof Error ? error.message.replaceAll(apiKey, "REDACTED") : String(error);
    const status = /timeout|timed out|abort/i.test(message) ? "TIMEOUT" : "PROVIDER_ERROR";
    return { payload: null, status, publicUrl, providerAttempted: true, cacheStatus: "MISS", message };
  }
}

// Python parity: a JSON object body reports its error/message/Information/Note text; any other body is used as is.
function finnhubErrorMessage(body: string): string {
  try {
    const parsed: unknown = JSON.parse(body);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      const record = parsed as Record<string, unknown>;
      return String(record.error || record.message || record.Information || record.Note || body);
    }
  } catch {
    // not JSON: fall through to the raw body
  }
  return body;
}

async function fetchFinnhubJson(
  path: string,
  params: Record<string, unknown>,
  ttlMs: number,
): Promise<ProviderJsonResult> {
  const cleanPath = `/${path.replace(/^\/+/, "")}`;
  const cacheKey = providerCacheKey("finnhub", cleanPath, params);
  const cached = await getProviderCache(cacheKey, ttlMs);
  if (cached) return cached;
  // Entitlements are per endpoint, not per symbol.
  const denialKey = `finnhub:${cleanPath}`;
  const denied = rememberedProviderDenial(denialKey);
  if (denied) return { ...denied, publicUrl: `${FINNHUB_API}${cleanPath}?${new URLSearchParams(Object.entries(params).map(([key, value]): [string, string] => [key, String(value)])).toString()}` };
  const token = getWorkerVar("FINNHUB_API_KEY") ?? getWorkerVar("FINNHUB_TOKEN");
  const query = new URLSearchParams(
    Object.entries(params).map(([key, value]): [string, string] => [key, String(value)])
  );
  const publicUrl = `${FINNHUB_API}${cleanPath}?${query.toString()}`;
  if (!token) return { payload: null, status: "SOURCE_UNCONFIGURED", publicUrl, providerAttempted: false, cacheStatus: "MISS" };
  try {
    const response = await fetchProviderWithTimeout(
      publicUrl,
      { headers: { "User-Agent": UA, "X-Finnhub-Token": token } },
      20_000,
    );
    const retryAfter = response.headers.get("Retry-After") ?? undefined;
    if (!response.ok) {
      const body = (await response.text()).replaceAll(token, "REDACTED");
      const errorMessage = finnhubErrorMessage(body);
      return rememberProviderDenial(denialKey, {
        payload: null,
        status: providerHttpStatus(response.status, errorMessage),
        publicUrl,
        providerAttempted: true,
        cacheStatus: "MISS",
        httpStatus: response.status,
        message: errorMessage.slice(0, 500) || undefined,
        retryAfter,
      });
    }
    const json = await response.json();
    if (!json || typeof json !== "object" || Array.isArray(json)) {
      return { payload: null, status: "PROVIDER_ERROR", publicUrl, providerAttempted: true, cacheStatus: "MISS", message: "Finnhub returned a non-object payload." };
    }
    const payload = json as Record<string, unknown>;
    const fetchedAt = new Date().toISOString();
    await setProviderCache(cacheKey, payload, publicUrl, fetchedAt, ttlMs);
    return { payload, status: "OK", publicUrl, providerAttempted: true, cacheStatus: "MISS", fetchedAt };
  } catch (error) {
    const message = error instanceof Error ? error.message.replaceAll(token, "REDACTED") : String(error);
    const status = /timeout|timed out|abort/i.test(message) ? "TIMEOUT" : "PROVIDER_ERROR";
    return { payload: null, status, publicUrl, providerAttempted: true, cacheStatus: "MISS", message };
  }
}

function providerNumber(value: unknown): number | null {
  if (value == null || typeof value === "boolean") return null;
  const text = String(value).replaceAll(",", "");
  if (text.trim() === "") return null;
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

function providerAttempt(provider: string, result: ProviderJsonResult): Record<string, unknown> {
  return Object.fromEntries(Object.entries({
    provider,
    status: result.status,
    attempted: result.providerAttempted,
    cacheStatus: result.cacheStatus,
    url: result.publicUrl,
    httpStatus: result.httpStatus,
    message: result.message,
    retryAfter: result.retryAfter,
  }).filter(([, value]) => value != null));
}

function providerRecovery(status: string, alphaAllowed = false): string {
  if (status === "SOURCE_UNCONFIGURED") return "CONFIGURE_PROVIDER";
  if (status === "AUTH_ERROR") return "CHECK_PROVIDER_CREDENTIALS";
  if (status === "ENTITLEMENT_REQUIRED") return alphaAllowed ? "USE_ALPHA_VANTAGE_EXPLICITLY" : "CHECK_PROVIDER_ENTITLEMENT";
  if (status === "RATE_LIMIT") return "RETRY_AFTER_PROVIDER_RESET";
  if (status === "TIMEOUT") return "RETRY_PROVIDER";
  if (status === "NOT_ELIGIBLE") return "USE_YAHOO_OR_OFFICIAL_FILINGS";
  if (status === "NOT_FOUND") return "VERIFY_TICKER_AND_DATE";
  return "RETRY_OR_USE_PRIMARY_PROVIDER";
}

export async function getHistoricalPutCallRatio(ticker: string, date: string): Promise<string> {
  const tool = "get_historical_put_call_ratio";
  if (!String(ticker ?? "").trim()) {
    return mcpFailure(tool, ErrorCode.INPUT_VALIDATION_ERROR, "ticker is required.");
  }
  const normalizedDate = String(date ?? "").trim();
  const parsedDate = /^\d{4}-\d{2}-\d{2}$/.test(normalizedDate)
    ? new Date(`${normalizedDate}T00:00:00.000Z`)
    : new Date(Number.NaN);
  const today = new Date().toISOString().slice(0, 10);
  if (
    !Number.isFinite(parsedDate.getTime())
    || parsedDate.toISOString().slice(0, 10) !== normalizedDate
    || normalizedDate <= "2008-01-01"
    || normalizedDate > today
  ) {
    return mcpFailure(
      tool,
      ErrorCode.INPUT_VALIDATION_ERROR,
      "date must be a valid YYYY-MM-DD after 2008-01-01 and not in the future. Use summarize_options_flow for a current Yahoo options snapshot.",
    );
  }
  const tickerU = ticker.toUpperCase();
  const result = await fetchAlphaVantageJson(
    "HISTORICAL_PUT_CALL_RATIO",
    { symbol: tickerU, date: normalizedDate },
    ALPHA_HISTORICAL_PUT_CALL_TTL_MS,
  );
  const attempts = [providerAttempt("alpha_vantage", result)];
  if (result.status !== "OK" || !result.payload) {
    return JSON.stringify({
      ticker: tickerU,
      status: result.status,
      source: null,
      providerGapFilled: false,
      capacityClass: "SCARCE_SHARED_QUOTA",
      dataDate: normalizedDate,
      evidenceClass: "CONTEXTUAL_PROVIDER_DATA",
      decisionGrade: false,
      providerAttempts: attempts,
      recommendedNextAction: providerRecovery(result.status),
    });
  }
  const responseSymbol = (_str(result.payload.symbol) || tickerU).toUpperCase();
  const responseDate = _str(result.payload.date) || normalizedDate;
  if (responseSymbol !== tickerU || responseDate !== normalizedDate) {
    attempts[0].validationStatus = "PROVIDER_IDENTITY_MISMATCH";
    return JSON.stringify({
      ticker: tickerU,
      status: "PROVIDER_IDENTITY_MISMATCH",
      source: null,
      providerGapFilled: false,
      capacityClass: "SCARCE_SHARED_QUOTA",
      dataDate: normalizedDate,
      evidenceClass: "CONTEXTUAL_PROVIDER_DATA",
      decisionGrade: false,
      providerAttempts: attempts,
      recommendedNextAction: "RETRY_OR_VERIFY_PROVIDER_RESPONSE",
    });
  }
  const fullChain = providerNumber(result.payload.put_call_ratio_full_chain);
  const byExpiration = (Array.isArray(result.payload.put_call_ratio_by_expiration)
    ? result.payload.put_call_ratio_by_expiration
    : [])
    .filter((row): row is Record<string, unknown> => !!row && typeof row === "object" && !Array.isArray(row))
    .map((row) => ({
      expirationDate: _str(row.date),
      putCallRatio: providerNumber(row.value),
    }))
    .filter((row) => row.expirationDate && row.putCallRatio != null);
  if (fullChain == null && byExpiration.length === 0) {
    return JSON.stringify({
      ticker: tickerU,
      status: "PROVIDER_ERROR",
      source: null,
      providerGapFilled: false,
      capacityClass: "SCARCE_SHARED_QUOTA",
      dataDate: normalizedDate,
      evidenceClass: "CONTEXTUAL_PROVIDER_DATA",
      decisionGrade: false,
      providerAttempts: attempts,
      recommendedNextAction: "RETRY_OR_USE_PRIMARY_PROVIDER",
    });
  }
  return JSON.stringify({
    ticker: tickerU,
    status: "OK",
    source: "alpha_vantage",
    providerGapFilled: true,
    capacityClass: "SCARCE_SHARED_QUOTA",
    dataDate: responseDate,
    freshnessStatus: "HISTORICAL_OBSERVATION",
    putCallRatioFullChain: fullChain,
    byExpiration,
    expirationCount: byExpiration.length,
    evidenceClass: "CONTEXTUAL_OPTIONS_DATA",
    decisionGrade: false,
    cacheStatus: result.cacheStatus,
    fetchedAt: result.fetchedAt ?? null,
    providerAttempts: attempts,
    recommendedNextAction: "NONE",
  });
}

function ownershipRows(payload: Record<string, unknown>, provider: string): Record<string, unknown>[] {
  const candidates = [payload.holdings, payload.ownership, payload.data].find(Array.isArray);
  if (!Array.isArray(candidates)) return [];
  return candidates
    .filter((row): row is Record<string, unknown> => !!row && typeof row === "object" && !Array.isArray(row))
    .map((row) => {
      const normalized = provider === "alpha_vantage"
        ? {
            holderName: row.holder_name,
            sharesHeld: providerNumber(row.shares_held),
            sharesChanged: providerNumber(row.shares_changed),
            sharesChangedPct: providerNumber(row.shares_changed_percentage),
            changeType: row.change_type,
            reportDate: row.last_reported,
          }
        : {
            holderName: row.name ?? row.holder ?? row.holderName,
            sharesHeld: providerNumber(row.share ?? row.shares ?? row.sharesHeld),
            sharesChanged: providerNumber(row.change ?? row.sharesChanged),
            sharesChangedPct: providerNumber(row.changePercent ?? row.sharesChangedPct),
            reportDate: row.filingDate ?? row.reportDate ?? row.date,
          };
      return Object.fromEntries(Object.entries(normalized).filter(([, value]) => value != null && value !== ""));
    })
    .filter((row) => Object.keys(row).length > 0);
}

function ownershipDataDate(payload: Record<string, unknown>, rows: Record<string, unknown>[]): string | null {
  const direct = payload.date ?? payload.last_reported ?? payload.lastReported;
  if (direct) return String(direct).slice(0, 10);
  const dates = rows.map(row => _str(row.reportDate).slice(0, 10)).filter(Boolean).sort().reverse();
  return dates[0] ?? null;
}

export async function getExpandedInstitutionalOwnership(
  ticker: string,
  allowScarceFallback = false,
  maxHolders = 50,
): Promise<string> {
  const tool = "get_expanded_institutional_ownership";
  if (!String(ticker ?? "").trim()) {
    return mcpFailure(tool, ErrorCode.INPUT_VALIDATION_ERROR, "ticker is required.");
  }
  if (!Number.isInteger(maxHolders) || maxHolders < 1 || maxHolders > 100) {
    return mcpFailure(tool, ErrorCode.INPUT_VALIDATION_ERROR, "max_holders must be an integer from 1 through 100.");
  }
  const tickerU = ticker.toUpperCase();
  const attempts: Record<string, unknown>[] = [];
  const eligibility = finnhubEligibility(tickerU);
  let provider = "finnhub";
  let result: ProviderJsonResult;
  if (eligibility.eligible) {
    result = await fetchFinnhubJson("/stock/ownership", { symbol: tickerU, limit: maxHolders }, PROVIDER_OWNERSHIP_TTL_MS);
    attempts.push(providerAttempt("finnhub", result));
  } else {
    result = {
      payload: null,
      status: "NOT_ELIGIBLE",
      publicUrl: "",
      providerAttempted: false,
      cacheStatus: "MISS",
      message: eligibility.reasonCode ?? undefined,
    };
    attempts.push({ provider: "finnhub", status: "NOT_ELIGIBLE", attempted: false, reasonCode: eligibility.reasonCode });
  }
  let rows = result.payload ? ownershipRows(result.payload, "finnhub") : [];
  if (result.payload && (_str(result.payload.symbol) || tickerU).toUpperCase() !== tickerU) {
    attempts[attempts.length - 1].validationStatus = "PROVIDER_IDENTITY_MISMATCH";
    result = {
      ...result,
      payload: null,
      status: "PROVIDER_IDENTITY_MISMATCH",
      message: "Provider response symbol did not match the requested ticker.",
    };
    rows = [];
  }
  if (rows.length === 0 && allowScarceFallback) {
    provider = "alpha_vantage";
    result = await fetchAlphaVantageJson("INSTITUTIONAL_HOLDINGS", { symbol: tickerU }, PROVIDER_OWNERSHIP_TTL_MS);
    attempts.push(providerAttempt("alpha_vantage", result));
    rows = result.payload ? ownershipRows(result.payload, "alpha_vantage") : [];
    if (result.payload && (_str(result.payload.symbol) || tickerU).toUpperCase() !== tickerU) {
      attempts[attempts.length - 1].validationStatus = "PROVIDER_IDENTITY_MISMATCH";
      result = {
        ...result,
        payload: null,
        status: "PROVIDER_IDENTITY_MISMATCH",
        message: "Provider response symbol did not match the requested ticker.",
      };
      rows = [];
    }
  }
  const capacityClass = provider === "alpha_vantage" ? "SCARCE_SHARED_QUOTA" : "ENTITLEMENT_DEPENDENT";
  if (result.status !== "OK" || !result.payload || rows.length === 0) {
    const status = result.status === "OK" ? "NOT_FOUND" : result.status;
    const recommendedNextAction = status === "NOT_FOUND" && !allowScarceFallback
      ? "VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK"
      : providerRecovery(status, !allowScarceFallback);
    return JSON.stringify({
      ticker: tickerU,
      status,
      source: null,
      providerGapFilled: false,
      capacityClass,
      evidenceClass: "CONTEXTUAL_PROVIDER_DATA",
      decisionGrade: false,
      providerAttempts: attempts,
      alphaFallbackAllowed: allowScarceFallback,
      recommendedNextAction,
    });
  }
  const totalCount = rows.length;
  const holders = rows.slice(0, maxHolders);
  const dataDate = ownershipDataDate(result.payload, holders);
  const aggregate: Record<string, number> = {};
  if (provider === "alpha_vantage") {
    const mappings: Array<[string, string]> = [
      ["total_institutional_holders", "totalInstitutionalHolders"],
      ["total_institutional_shares", "totalInstitutionalShares"],
      ["total_institutional_ownership_percentage", "totalInstitutionalOwnershipPct"],
      ["holders_with_increased_holdings", "holdersIncreased"],
      ["holders_with_decreased_holdings", "holdersDecreased"],
      ["holders_with_unchanged_holdings", "holdersUnchanged"],
      ["shares_with_increased_holdings", "sharesIncreased"],
      ["shares_with_decreased_holdings", "sharesDecreased"],
      ["shares_with_unchanged_holdings", "sharesUnchanged"],
    ];
    for (const [input, output] of mappings) {
      const value = providerNumber(result.payload[input]);
      if (value != null) aggregate[output] = value;
    }
  }
  return JSON.stringify({
    ticker: tickerU,
    status: "OK",
    source: provider,
    providerGapFilled: true,
    capacityClass,
    scope: "PROVIDER_AGGREGATED_INSTITUTIONAL_OWNERSHIP",
    dataDate,
    freshnessStatus: dataDate ? "DATED" : "DATE_NOT_DISCLOSED",
    holders,
    returnedCount: holders.length,
    providerRowCount: totalCount,
    truncated: totalCount > holders.length,
    aggregate,
    evidenceClass: "CONTEXTUAL_OWNERSHIP_DATA",
    decisionGrade: false,
    cacheStatus: result.cacheStatus,
    fetchedAt: result.fetchedAt ?? null,
    providerAttempts: attempts,
    recommendedNextAction: "VERIFY_SEC_13F_FOR_MATERIAL_USE",
  });
}

const COMPANIES_HOUSE_API = "https://api.company-information.service.gov.uk";
const COMPANIES_HOUSE_TIMEOUT_MS = 15_000;
const COMPANIES_HOUSE_CATEGORIES = new Set(["accounts", "capital", "mortgage", "confirmation-statement", "resolution", "incorporation", "officers", "persons-with-significant-control", "address", "annotation", "change-of-name", "miscellaneous"]);

async function companiesHouseGet(path: string, key: string): Promise<{ status: number; body: Record<string, unknown> | null }> {
  const resp = await fetchProviderWithTimeout(`${COMPANIES_HOUSE_API}${path}`, {
    headers: { Authorization: `Basic ${btoa(`${key}:`)}`, Accept: "application/json" },
  }, COMPANIES_HOUSE_TIMEOUT_MS);
  if (!resp.ok) {
    await resp.body?.cancel();
    return { status: resp.status, body: null };
  }
  return { status: resp.status, body: await resp.json() as Record<string, unknown> };
}

function companiesHouseError(status: number, step: string): string {
  if (status === 429) return JSON.stringify({ error: true, message: `Companies House ${step}: HTTP 429 rate limited (600 requests per 5 minutes).` });
  if (status === 401 || status === 403) return JSON.stringify({ error: true, code: "PROVIDER_ERROR", status: "AUTH_ERROR", message: `Companies House ${step}: HTTP ${status}; check COMPANIES_HOUSE_API_KEY.` });
  return JSON.stringify({ error: true, code: "PROVIDER_ERROR", message: `Companies House ${step}: HTTP ${status}.` });
}

function chargeRows(payload: Record<string, unknown>): Record<string, unknown>[] {
  const items = Array.isArray(payload.items) ? payload.items as Record<string, unknown>[] : [];
  return items.map((c) => {
    const classification = (c.classification && typeof c.classification === "object" ? c.classification : {}) as Record<string, unknown>;
    const particulars = (c.particulars && typeof c.particulars === "object" ? c.particulars : {}) as Record<string, unknown>;
    const persons = Array.isArray(c.persons_entitled) ? c.persons_entitled as Record<string, unknown>[] : [];
    return {
      chargeCode: c.charge_code ?? null,
      status: c.status ?? null,
      createdOn: c.created_on ?? null,
      deliveredOn: c.delivered_on ?? null,
      satisfiedOn: c.satisfied_on ?? null,
      classification: classification.description ?? null,
      personsEntitled: persons.map((p) => String(p.name ?? "")).filter(Boolean),
      particulars: typeof particulars.description === "string" ? particulars.description.slice(0, 400) : null,
    };
  });
}

export async function getUkCompanyFilings(
  ticker: string | null,
  companyNumber: string | null = null,
  companyName: string | null = null,
  category: string | null = null,
  limit = 25,
  includeCharges = false,
): Promise<string> {
  const key = getWorkerVar("COMPANIES_HOUSE_API_KEY")?.trim();
  const base = { source: "companies_house", sourceType: "uk_companies_house" };
  if (!key) {
    return JSON.stringify({
      ...base,
      ticker,
      status: "SOURCE_UNCONFIGURED",
      code: "SOURCE_UNCONFIGURED",
      message: "Companies House is not configured; set the COMPANIES_HOUSE_API_KEY secret (a free key from the Companies House developer hub).",
      filings: [],
    });
  }
  const cat = category ? category.trim().toLowerCase() : null;
  if (cat && !COMPANIES_HOUSE_CATEGORIES.has(cat)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: `category must be one of: ${[...COMPANIES_HOUSE_CATEGORIES].join(", ")}.` });
  }
  const size = clampInt(limit, 25, 1, 100);
  const warnings: Record<string, unknown>[] = [];
  let number = companyNumber ? companyNumber.trim().toUpperCase() : null;
  if (number && /^\d{1,8}$/.test(number)) number = number.padStart(8, "0");
  if (number && !/^[A-Z0-9]{8}$/.test(number)) {
    return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "company_number must be an 8-character Companies House number, e.g. 01234567 or SC123456." });
  }
  let matchedBy = "company_number";
  let company: Record<string, unknown> | null = null;
  try {
    if (!number) {
      let name = companyName?.trim() || null;
      matchedBy = "company_name";
      if (!name && ticker) {
        const identity = await resolveNewsCompanyIdentity(ticker);
        name = identity.longName ?? identity.companyName ?? identity.shortName;
        matchedBy = "ticker_issuer_name";
        if (!/\.(?:L|IL)$/i.test(ticker)) {
          warnings.push({ code: "NON_UK_LISTING", message: `${ticker} is not a London listing; the match is by issuer name only.`, severity: "info" });
        }
      }
      if (!name) {
        return JSON.stringify({ error: true, code: "INPUT_VALIDATION_ERROR", message: "Provide company_number, company_name, or a ticker whose issuer name can be resolved." });
      }
      const search = await companiesHouseGet(`/search/companies?q=${encodeURIComponent(name)}&items_per_page=20`, key);
      if (!search.body) return companiesHouseError(search.status, "company search");
      const match = pickCompaniesHouseMatch(name, search.body);
      if (!match) {
        const items = Array.isArray(search.body.items) ? search.body.items as Record<string, unknown>[] : [];
        return JSON.stringify({
          ...base,
          ticker,
          status: "COMPANY_NOT_MATCHED",
          searchedName: name,
          candidates: items.slice(0, 5).map((i) => ({ companyNumber: i.company_number ?? null, title: i.title ?? null, companyStatus: i.company_status ?? null, addressSnippet: i.address_snippet ?? null })),
          message: "No registered company name matched the issuer exactly; pass company_number from the candidates.",
          filings: [],
          warnings,
        });
      }
      number = String(match.company_number ?? "");
      company = { companyNumber: number, name: match.title ?? null, status: match.company_status ?? null };
    }
    if (!company) {
      const profile = await companiesHouseGet(`/company/${number}`, key);
      if (profile.status === 404) return JSON.stringify({ ...base, ticker, status: "COMPANY_NOT_FOUND", companyNumber: number, filings: [] });
      if (!profile.body) return companiesHouseError(profile.status, "company profile");
      company = { companyNumber: number, name: profile.body.company_name ?? null, status: profile.body.company_status ?? null };
    }
    const history = await companiesHouseGet(`/company/${number}/filing-history?items_per_page=${size}${cat ? `&category=${cat}` : ""}`, key);
    if (!history.body) return companiesHouseError(history.status, "filing history");
    const out: Record<string, unknown> = {
      ...base,
      ticker,
      status: "OK",
      decisionUse: "USE_OFFICIAL_EVIDENCE",
      company: { ...company, matchedBy },
      category: cat,
      filings: companiesHouseFilings(String(number), history.body),
      totalFilings: typeof history.body.total_count === "number" ? history.body.total_count : null,
      filingHistoryUrl: `https://find-and-update.company-information.service.gov.uk/company/${number}/filing-history`,
      notes: [
        "Companies House holds statutory filings: accounts, share allotments (SH01), charges (MR01), resolutions and confirmation statements.",
        "Market announcements (RNS) such as results, loan-note terms and trading updates are not filed here; read them on the company's investor site.",
        "Documents are PDFs; documentContentUrl needs the same API key, viewerUrl opens the public viewer.",
      ],
      warnings,
    };
    if (includeCharges) {
      const charges = await companiesHouseGet(`/company/${number}/charges`, key);
      if (charges.body) out.charges = chargeRows(charges.body);
      else if (charges.status === 404) out.charges = [];
      else warnings.push({ code: "CHARGES_UNAVAILABLE", message: `Companies House charges: HTTP ${charges.status}.`, severity: "warning" });
    }
    return JSON.stringify(out);
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    if (/abort|timed? ?out/i.test(message)) return JSON.stringify({ error: true, message: "Companies House request timed out." });
    return JSON.stringify({ error: true, code: "PROVIDER_ERROR", message: `Companies House request failed: ${message}` });
  }
}
