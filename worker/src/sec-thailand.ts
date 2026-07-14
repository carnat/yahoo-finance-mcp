/** Bounded Thai SEC Open Data fund workflows. No cache, crawling, or PDF fetching. */

import { ErrorCode, getWorkerVar, mcpFailure, mcpSuccess } from "./response.js";

const SOURCE = "sec_thailand_open_data";
const EVIDENCE_CLASS = "OFFICIAL_REGULATORY_DATA";
const BASE_URL = "https://api.sec.or.th/v2/fund";
const TIMEZONE = "Asia/Bangkok";
const TIMEOUT_MS = 12_000;
const MAX_PAGE_SIZE = 100;
const DEFAULT_NAV_LOOKBACK_DAYS = 45;
const MAX_NAV_LOOKBACK_DAYS = 90;
const FACTSHEET_SECTIONS = new Set(["statistics", "top_holdings", "urls"]);

type RecordValue = Record<string, unknown>;

interface ProviderError {
  code: string;
  message: string;
  recoveryAction: string;
}

interface SecResponse {
  items: RecordValue[];
  next_cursor?: string | null;
}

interface FundIdentity {
  fundClassName: string;
  projId: string;
  uniqueId: string | null;
  companyNameTh: string | null;
  companyNameEn: string | null;
  projectNameTh: string | null;
  projectNameEn: string | null;
}

interface Resolution {
  status: string;
  identity?: FundIdentity;
  candidates?: FundIdentity[];
  message?: string;
}

function optionalString(value: unknown): string | null {
  if (value == null) return null;
  const text = String(value).trim();
  return text || null;
}

function basePayload(status: string): RecordValue {
  return { status, source: SOURCE, evidenceClass: EVIDENCE_CLASS, decisionGrade: false };
}

function recovery(action: string, detail: string): RecordValue {
  return { action, detail };
}

function providerError(code: string, message: string, recoveryAction: string): ProviderError {
  return { code, message, recoveryAction };
}

function isIsoDate(value: unknown): value is string {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

function addDays(isoDate: string, days: number): string {
  const parsed = new Date(`${isoDate}T00:00:00.000Z`);
  parsed.setUTCDate(parsed.getUTCDate() + days);
  return parsed.toISOString().slice(0, 10);
}

function bangkokToday(): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function errorResult(tool: string, error: ProviderError): string {
  return mcpFailure(tool, error.code, error.message, {
    source: SOURCE,
    metaExtra: {
      recoveryAction: error.recoveryAction,
      evidenceClass: EVIDENCE_CLASS,
      decisionGrade: false,
    },
  });
}

function inputError(tool: string, message: string): string {
  return mcpFailure(tool, ErrorCode.INPUT_VALIDATION_ERROR, message, { source: SOURCE });
}

async function secGet(path: string, params: Record<string, unknown>): Promise<SecResponse> {
  const apiKey = (getWorkerVar("SEC_OPEN_DATA_API_KEY") ?? "").trim();
  if (!apiKey) {
    throw providerError("SOURCE_UNCONFIGURED", "SEC Open Data is not configured for this runtime.", "CONFIGURE_SEC_OPEN_DATA_API_KEY");
  }
  const url = new URL(`${BASE_URL}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value != null && String(value) !== "") url.searchParams.set(key, typeof value === "boolean" ? String(value).toLowerCase() : String(value));
  }
  const controller = new AbortController();
  let timedOut = false;
  const timeout = setTimeout(() => { timedOut = true; controller.abort(); }, TIMEOUT_MS);
  let response: Response;
  try {
    response = await fetch(url.toString(), {
      method: "GET",
      headers: { Accept: "application/json", "Ocp-Apim-Subscription-Key": apiKey },
      signal: controller.signal,
    });
  } catch {
    throw timedOut
      ? providerError("PROVIDER_TIMEOUT", "SEC Open Data did not respond before the request timeout.", "RETRY_LATER")
      : providerError("PROVIDER_ERROR", "SEC Open Data request failed before a response was received.", "RETRY_LATER");
  } finally {
    clearTimeout(timeout);
  }
  if (response.status === 401 || response.status === 403) {
    throw providerError("AUTH_ERROR", "SEC Open Data rejected the configured subscription key.", "VERIFY_SEC_OPEN_DATA_API_KEY");
  }
  if (response.status === 429) {
    throw providerError("RATE_LIMIT", "SEC Open Data rate limited this request.", "RETRY_LATER");
  }
  if (!response.ok) {
    throw providerError("PROVIDER_ERROR", `SEC Open Data returned HTTP ${response.status}.`, "RETRY_LATER");
  }
  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    throw providerError("PROVIDER_ERROR", "SEC Open Data returned an invalid JSON response.", "RETRY_LATER");
  }
  if (!payload || typeof payload !== "object" || !Array.isArray((payload as RecordValue).items)) {
    throw providerError("PROVIDER_ERROR", "SEC Open Data returned an unexpected response shape.", "RETRY_LATER");
  }
  const body = payload as RecordValue;
  return {
    items: (body.items as unknown[]).filter((item): item is RecordValue => item != null && typeof item === "object" && !Array.isArray(item)),
    next_cursor: optionalString(body.next_cursor),
  };
}

function identityFromProfile(row: RecordValue): FundIdentity {
  return {
    fundClassName: String(row.fund_class_name ?? ""),
    projId: String(row.proj_id ?? ""),
    uniqueId: optionalString(row.unique_id),
    companyNameTh: optionalString(row.comp_name_th),
    companyNameEn: optionalString(row.comp_name_en),
    projectNameTh: optionalString(row.proj_name_th),
    projectNameEn: optionalString(row.proj_name_en),
  };
}

async function resolveFund(fundClassName: string, projId: string | null): Promise<Resolution> {
  const payload = await secGet("/general-info/profiles", {
    fund_class_name: fundClassName,
    page_size: MAX_PAGE_SIZE,
    ...(projId ? { project_info: projId } : {}),
  });
  const exact = payload.items
    .filter((row) => String(row.fund_class_name ?? "") === fundClassName && (!projId || String(row.proj_id ?? "") === projId))
    .map(identityFromProfile);
  if (projId && exact.length === 0) {
    const classMatches = payload.items
      .filter((row) => String(row.fund_class_name ?? "") === fundClassName)
      .map(identityFromProfile)
      .slice(0, 10);
    return { status: "FUND_IDENTITY_MISMATCH", candidates: classMatches, message: "The supplied proj_id does not match the requested fund_class_name." };
  }
  if (exact.length === 0) {
    return { status: "FUND_NOT_FOUND", message: "No exact active share class matched fund_class_name." };
  }
  if (exact.length !== 1 || Boolean(payload.next_cursor)) {
    return {
      status: "AMBIGUOUS_SHARE_CLASS",
      candidates: exact.slice(0, 10),
      message: "Provide proj_id to select an exact share class; no automatic class selection was made.",
    };
  }
  return { status: "OK", identity: exact[0] };
}

function resolutionResponse(tool: string, fundClassName: string, projId: string | null, resolution: Resolution): string {
  const payload = basePayload(resolution.status);
  Object.assign(payload, {
    scope: "SHARE_CLASS",
    requestedIdentity: { fundClassName, projId },
    identity: resolution.identity ?? null,
    candidates: resolution.candidates ?? [],
    recovery: recovery(
      resolution.status === "AMBIGUOUS_SHARE_CLASS" ? "PROVIDE_PROJ_ID" : "CHECK_FUND_CLASS_NAME",
      resolution.message ?? "Resolve an exact Thai SEC share class before requesting fund data.",
    ),
  });
  return mcpSuccess(tool, JSON.stringify(payload), { source: SOURCE });
}

async function resolveOrResponse(tool: string, fundClassNameRaw: unknown, projIdRaw: unknown): Promise<{ identity?: FundIdentity; response?: string }> {
  const fundClassName = optionalString(fundClassNameRaw);
  if (!fundClassName) return { response: inputError(tool, "fund_class_name is required.") };
  const projId = optionalString(projIdRaw);
  try {
    const resolution = await resolveFund(fundClassName, projId);
    if (resolution.status !== "OK" || !resolution.identity) return { response: resolutionResponse(tool, fundClassName, projId, resolution) };
    return { identity: resolution.identity };
  } catch (error) {
    return { response: errorResult(tool, error as ProviderError) };
  }
}

function freshness(dataDate: string | null, asOfDate: string): RecordValue {
  const days = dataDate && isIsoDate(dataDate) && isIsoDate(asOfDate)
    ? Math.round((Date.parse(`${asOfDate}T00:00:00Z`) - Date.parse(`${dataDate}T00:00:00Z`)) / 86_400_000)
    : null;
  return { asOfDate, dataDate, calendarDaysFromAsOf: days, timezone: TIMEZONE };
}

export async function getThaiFundNav(
  fundClassNameRaw: unknown,
  projIdRaw: unknown,
  asOfDateRaw: unknown,
  lookbackDaysRaw: unknown,
): Promise<string> {
  const tool = "get_thai_fund_nav";
  const resolved = await resolveOrResponse(tool, fundClassNameRaw, projIdRaw);
  if (resolved.response) return resolved.response;
  const identity = resolved.identity!;
  const asOfDate = asOfDateRaw == null || String(asOfDateRaw).trim() === "" ? bangkokToday() : String(asOfDateRaw).trim();
  if (!isIsoDate(asOfDate)) return inputError(tool, "as_of_date must be YYYY-MM-DD.");
  const lookbackDays = lookbackDaysRaw == null ? DEFAULT_NAV_LOOKBACK_DAYS : Number(lookbackDaysRaw);
  if (!Number.isInteger(lookbackDays) || lookbackDays < 1 || lookbackDays > MAX_NAV_LOOKBACK_DAYS) {
    return inputError(tool, "lookback_days must be an integer from 1 through 90.");
  }
  const startDate = addDays(asOfDate, -(lookbackDays - 1));
  let response: SecResponse;
  try {
    response = await secGet("/daily-info/nav", {
      proj_id: identity.projId,
      fund_class_name: identity.fundClassName,
      start_nav_date: startDate,
      end_nav_date: asOfDate,
      page_size: MAX_PAGE_SIZE,
    });
  } catch (error) {
    return errorResult(tool, error as ProviderError);
  }
  const rows = response.items.filter((row) => isIsoDate(row.nav_date));
  const latest = rows.length ? rows.reduce((best, row) => String(row.nav_date) > String(best.nav_date) ? row : best) : null;
  const dataDate = latest ? optionalString(latest.nav_date) : null;
  const payload = basePayload(latest ? "OK" : "NAV_NOT_FOUND_IN_WINDOW");
  Object.assign(payload, {
    scope: "SHARE_CLASS",
    identity,
    requestedWindow: { startDate, endDate: asOfDate, lookbackCalendarDays: lookbackDays, timezone: TIMEZONE },
    dataDate,
    freshness: freshness(dataDate, asOfDate),
    nav: latest ? {
      navDate: latest.nav_date, netAsset: latest.net_asset, lastValue: latest.last_val,
      sellPrice: latest.sell_price, buyPrice: latest.buy_price, sellSwapPrice: latest.sell_swap_price,
      buySwapPrice: latest.buy_swap_price, lastUpdatedAt: latest.last_upd_date,
    } : null,
    nextCursor: optionalString(response.next_cursor),
    hasMore: Boolean(response.next_cursor),
    recovery: recovery(
      latest ? "NONE" : "EXPAND_WINDOW_UP_TO_90_DAYS",
      latest ? "Latest returned NAV selected by nav_date, not provider row order." : "No NAV was returned inside this requested window; try a later as_of_date or a wider window.",
    ),
  });
  return mcpSuccess(tool, JSON.stringify(payload), { source: SOURCE, dataDate });
}

function factsheetSectionError(section: string, error: ProviderError): RecordValue {
  return {
    status: error.code,
    scope: section === "top_holdings" ? "PROJECT" : "SHARE_CLASS",
    asOfDate: null,
    data: null,
    recovery: recovery(error.recoveryAction, error.message),
  };
}

async function factsheetStatistics(identity: FundIdentity): Promise<RecordValue> {
  try {
    const response = await secGet("/factsheet/statistics", { proj_id: identity.projId, fund_class_name: identity.fundClassName, latest: true, page_size: 1 });
    const row = response.items[0];
    if (!row) return { status: "EMPTY_RESULT", scope: "SHARE_CLASS", asOfDate: null, data: null, recovery: recovery("CHECK_LATER", "No dated statistics record is currently available for this share class.") };
    const metrics = ["portfolio_turnover_ratio", "recovering_period", "portfolio_duration_period", "maximum_drawdown", "sharpe_ratio", "beta", "alpha", "fx_hedging", "tracking_error", "yield_to_maturity"];
    return {
      status: "OK", scope: "SHARE_CLASS", asOfDate: row.end_date ?? row.start_date ?? null,
      period: { startDate: row.start_date ?? null, endDate: row.end_date ?? null, prospectusType: row.prospectus_type ?? null },
      data: Object.fromEntries(metrics.map((key) => [key, row[key] ?? null])),
      recovery: recovery("NONE", "Dated factsheet statistics returned."),
    };
  } catch (error) {
    return factsheetSectionError("statistics", error as ProviderError);
  }
}

async function factsheetTopHoldings(identity: FundIdentity): Promise<RecordValue> {
  try {
    const response = await secGet("/factsheet/top5-holdings", { proj_id: identity.projId, latest: true, page_size: 5 });
    const asOfDate = response.items.map((row) => row.end_date ?? row.start_date).find((value) => value != null) ?? null;
    const data = response.items.map((row) => ({
      assetName: row.asset_name ?? null, assetRatio: row.asset_ratio ?? null, assetSequence: row.asset_seq ?? null,
      startDate: row.start_date ?? null, endDate: row.end_date ?? null, prospectusType: row.prospectus_type ?? null,
      lastUpdatedAt: row.last_upd_date ?? null,
    }));
    return {
      status: data.length ? "OK" : "EMPTY_RESULT", scope: "PROJECT", asOfDate, data,
      recovery: recovery(data.length ? "NONE" : "CHECK_LATER", "Top holdings are dated project-level factsheet evidence, not current share-class holdings."),
    };
  } catch (error) {
    return factsheetSectionError("top_holdings", error as ProviderError);
  }
}

async function factsheetUrls(identity: FundIdentity): Promise<RecordValue> {
  try {
    const response = await secGet("/factsheet/urls", { proj_id: identity.projId, fund_class_name: identity.fundClassName, page_size: MAX_PAGE_SIZE });
    const asOfDate = response.items.map((row) => row.as_of_date).find((value) => value != null) ?? null;
    const data = response.items.map((row) => ({
      prospectusType: row.prospectus_type ?? null, amcUrlFactsheet: row.amc_url_factsheet ?? null,
      pdfFactsheet: row.pdf_factsheet ?? null, asOfDate: row.as_of_date ?? null, lastUpdatedAt: row.last_upd_date ?? null,
    }));
    return {
      status: data.length ? "OK" : "EMPTY_RESULT", scope: "SHARE_CLASS", asOfDate, data,
      recovery: recovery(data.length ? "NONE" : "CHECK_LATER", "URLs are returned as official references only; this tool does not fetch or parse PDFs."),
    };
  } catch (error) {
    return factsheetSectionError("urls", error as ProviderError);
  }
}

export async function getThaiFundFactsheet(
  fundClassNameRaw: unknown,
  projIdRaw: unknown,
  sectionsRaw: unknown,
): Promise<string> {
  const tool = "get_thai_fund_factsheet";
  const resolved = await resolveOrResponse(tool, fundClassNameRaw, projIdRaw);
  if (resolved.response) return resolved.response;
  const identity = resolved.identity!;
  const selected = sectionsRaw == null ? ["statistics", "top_holdings", "urls"] : Array.isArray(sectionsRaw) ? sectionsRaw.map(String) : [];
  if (!selected.length || selected.some((section) => !FACTSHEET_SECTIONS.has(section))) {
    return inputError(tool, "sections must be a non-empty subset of: statistics, top_holdings, urls.");
  }
  const loaders: Record<string, (item: FundIdentity) => Promise<RecordValue>> = {
    statistics: factsheetStatistics, top_holdings: factsheetTopHoldings, urls: factsheetUrls,
  };
  const loaded = await Promise.all(selected.map(async (section) => [section, await loaders[section](identity)] as const));
  const sections = Object.fromEntries(loaded) as Record<string, RecordValue>;
  const sectionStatus = Object.fromEntries(Object.entries(sections).map(([name, section]) => [name, section.status]));
  const failures = Object.values(sectionStatus).some((status) => status !== "OK" && status !== "EMPTY_RESULT");
  const payload = basePayload(failures ? "PARTIAL" : "OK");
  Object.assign(payload, {
    scope: "MIXED", identity, sections, sectionStatus, partialSuccess: failures,
    recovery: recovery(failures ? "RETRY_FAILED_SECTIONS" : "NONE", failures ? "Retry only the listed failed factsheet sections; successful dated evidence remains valid." : "All requested factsheet sections completed."),
  });
  return mcpSuccess(tool, JSON.stringify(payload), { source: SOURCE });
}

export async function getThaiFundDividendHistory(
  fundClassNameRaw: unknown,
  projIdRaw: unknown,
  maxResultsRaw: unknown,
  nextCursorRaw: unknown,
): Promise<string> {
  const tool = "get_thai_fund_dividend_history";
  const resolved = await resolveOrResponse(tool, fundClassNameRaw, projIdRaw);
  if (resolved.response) return resolved.response;
  const identity = resolved.identity!;
  const maxResults = maxResultsRaw == null ? MAX_PAGE_SIZE : Number(maxResultsRaw);
  if (!Number.isInteger(maxResults) || maxResults < 1 || maxResults > MAX_PAGE_SIZE) {
    return inputError(tool, "max_results must be an integer from 1 through 100.");
  }
  let response: SecResponse;
  try {
    response = await secGet("/daily-info/dividend-history", {
      proj_id: identity.projId, page_size: maxResults, next_cursor: optionalString(nextCursorRaw),
    });
  } catch (error) {
    return errorResult(tool, error as ProviderError);
  }
  const dividends = response.items
    .sort((a, b) => String(b.dividend_date ?? "").localeCompare(String(a.dividend_date ?? "")))
    .map((row) => ({
      projId: row.proj_id ?? null, uniqueId: row.unique_id ?? null, classAbbrName: row.class_abbr_name ?? null,
      bookCloseDate: row.book_close_date ?? null, dividendDate: row.dividend_date ?? null,
      dividendValue: row.dividend_value ?? null, lastUpdatedAt: row.last_upd_date ?? null,
    }));
  const dataDate = dividends.map((row) => optionalString(row.dividendDate)).filter((value): value is string => value != null).sort().at(-1) ?? null;
  const nextCursor = optionalString(response.next_cursor);
  const payload = basePayload(dividends.length ? "OK" : "EMPTY_RESULT");
  Object.assign(payload, {
    scope: "PROJECT", identity, dataDate, dividends, nextCursor, hasMore: Boolean(nextCursor),
    recovery: recovery(nextCursor ? "FETCH_NEXT_PAGE" : "NONE", "Dividend history is project scoped and this response covers only the returned page."),
  });
  return mcpSuccess(tool, JSON.stringify(payload), { source: SOURCE, dataDate });
}
