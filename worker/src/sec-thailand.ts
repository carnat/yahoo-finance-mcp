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
const DEFAULT_SEARCH_PAGE_SIZE = 10;
const MAX_SEARCH_PAGE_SIZE = 20;
const MAX_NAV_BATCH_FUNDS = 20;
const ACTIVE_FUND_STATUSES = ["Registered", "IPO"] as const;
const FACTSHEET_SECTIONS = new Set(["statistics", "top_holdings", "urls"]);

type RecordValue = Record<string, unknown>;

interface ProviderError {
  code: string;
  message: string;
  recoveryAction: string;
  diagnostics?: RecordValue;
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
  requestedFundClassName?: string | null;
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

function searchCandidate(row: RecordValue): RecordValue {
  return {
    projId: optionalString(row.proj_id),
    fundClassName: optionalString(row.fund_class_name),
    projectNameTh: optionalString(row.proj_name_th),
    projectNameEn: optionalString(row.proj_name_en),
    projectAbbreviation: optionalString(row.proj_abbr_name),
    companyNameTh: optionalString(row.comp_name_th),
    companyNameEn: optionalString(row.comp_name_en),
    uniqueId: optionalString(row.unique_id),
    fundStatus: optionalString(row.fund_status),
    lastUpdatedAt: optionalString(row.last_upd_date),
  };
}

function normalizeSearchCursors(value: unknown): Record<string, string | null> | null {
  if (value == null) return null;
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("next_cursors must be an object returned by a prior search_thai_funds response.");
  }
  const cursors: Record<string, string | null> = {};
  for (const [status, cursor] of Object.entries(value as Record<string, unknown>)) {
    if (!(ACTIVE_FUND_STATUSES as readonly string[]).includes(status)) {
      throw new Error("next_cursors may contain only Registered and IPO keys.");
    }
    if (cursor != null && typeof cursor !== "string") {
      throw new Error("next_cursors values must be strings or null.");
    }
    cursors[status] = optionalString(cursor);
  }
  if (Object.keys(cursors).length && Object.values(cursors).every((cursor) => cursor == null)) {
    throw new Error("next_cursors has no remaining page; start a new search without it.");
  }
  return cursors;
}

function providerError(code: string, message: string, recoveryAction: string, diagnostics?: RecordValue): ProviderError {
  return { code, message, recoveryAction, diagnostics };
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
    diagnostics: error.diagnostics,
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
  const queryParams = { ...params };
  if (queryParams.next_cursor == null) queryParams.next_cursor = "";
  for (const [key, value] of Object.entries(queryParams)) {
    if (value != null && (String(value) !== "" || key === "next_cursor")) {
      url.searchParams.set(key, typeof value === "boolean" ? String(value).toLowerCase() : String(value));
    }
  }
  const controller = new AbortController();
  let timedOut = false;
  const timeout = setTimeout(() => { timedOut = true; controller.abort(); }, TIMEOUT_MS);
  let response: Response;
  try {
    response = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "Ocp-Apim-Subscription-Key": apiKey,
      },
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
  const raw = await response.text();
  let payload: unknown;
  try {
    payload = JSON.parse(raw);
  } catch {
    const bodyBytes = new TextEncoder().encode(raw);
    const digest = await crypto.subtle.digest("SHA-256", bodyBytes);
    const bodySha256 = Array.from(new Uint8Array(digest)).map((value) => value.toString(16).padStart(2, "0")).join("");
    throw providerError("PROVIDER_ERROR", "SEC Open Data returned an invalid JSON response.", "RETRY_LATER", {
      httpStatus: response.status,
      contentType: response.headers.get("content-type"),
      bodyBytes: bodyBytes.byteLength,
      bodySha256,
    });
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

function identityFromNav(
  row: RecordValue,
  projId: string,
  requestedFundClassName: string,
): FundIdentity {
  const sourceFundClassName = optionalString(row.fund_class_name) ?? requestedFundClassName;
  return {
    fundClassName: sourceFundClassName,
    projId,
    uniqueId: optionalString(row.unique_id),
    companyNameTh: null,
    companyNameEn: null,
    projectNameTh: null,
    projectNameEn: null,
    ...(sourceFundClassName !== requestedFundClassName ? { requestedFundClassName } : {}),
  };
}

function resolveProfilePayload(
  payload: SecResponse,
  fundClassName: string,
  projId: string | null,
): Resolution {
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

async function resolveFund(
  fundClassName: string,
  projId: string | null,
  projectInfo: string | null,
): Promise<Resolution> {
  const projectLookup = projId ?? projectInfo;
  const profilesParams = {
    fund_class_name: fundClassName,
    fund_status: "Registered",
    page_size: MAX_PAGE_SIZE,
    next_cursor: "",
    ...(projectLookup ? { project_info: projectLookup } : {}),
  };
  const registered = await secGet("/general-info/profiles", profilesParams);
  const registeredClassMatches = registered.items.filter((row) => String(row.fund_class_name ?? "") === fundClassName);
  if (registeredClassMatches.length || registered.next_cursor) {
    return resolveProfilePayload(registered, fundClassName, projId);
  }
  const ipo = await secGet("/general-info/profiles", { ...profilesParams, fund_status: "IPO" });
  return resolveProfilePayload(ipo, fundClassName, projId);
}

function resolutionResponse(
  tool: string,
  fundClassName: string,
  projId: string | null,
  projectInfo: string | null,
  resolution: Resolution,
): string {
  const payload = basePayload(resolution.status);
  Object.assign(payload, {
    scope: "SHARE_CLASS",
    requestedIdentity: { fundClassName, projId, projectInfo },
    identity: resolution.identity ?? null,
    candidates: resolution.candidates ?? [],
    recovery: recovery(
      resolution.status === "AMBIGUOUS_SHARE_CLASS" ? "PROVIDE_PROJ_ID" : "CHECK_FUND_CLASS_NAME",
      resolution.message ?? "Resolve an exact Thai SEC share class before requesting fund data.",
    ),
  });
  return mcpSuccess(tool, JSON.stringify(payload), { source: SOURCE });
}

async function resolveOrResponse(
  tool: string,
  fundClassNameRaw: unknown,
  projIdRaw: unknown,
  projectInfoRaw: unknown,
): Promise<{ identity?: FundIdentity; response?: string }> {
  const fundClassName = optionalString(fundClassNameRaw);
  if (!fundClassName) return { response: inputError(tool, "fund_class_name is required.") };
  const projId = optionalString(projIdRaw);
  const projectInfo = optionalString(projectInfoRaw);
  try {
    const resolution = await resolveFund(fundClassName, projId, projectInfo);
    if (resolution.status !== "OK" || !resolution.identity) return { response: resolutionResponse(tool, fundClassName, projId, projectInfo, resolution) };
    return { identity: resolution.identity };
  } catch (error) {
    return { response: errorResult(tool, error as ProviderError) };
  }
}

export async function searchThaiFunds(
  projectInfoRaw: unknown,
  companyInfoRaw: unknown,
  fundClassNameRaw: unknown,
  pageSizeRaw: unknown,
  nextCursorsRaw: unknown,
): Promise<string> {
  const tool = "search_thai_funds";
  const projectInfo = optionalString(projectInfoRaw);
  const companyInfo = optionalString(companyInfoRaw);
  const fundClassName = optionalString(fundClassNameRaw);
  if (!projectInfo && !companyInfo && !fundClassName) {
    return inputError(tool, "Provide at least one of project_info, company_info, or fund_class_name.");
  }
  const pageSize = pageSizeRaw == null ? DEFAULT_SEARCH_PAGE_SIZE : Number(pageSizeRaw);
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > MAX_SEARCH_PAGE_SIZE) {
    return inputError(tool, "page_size must be an integer from 1 through 20 per active fund status.");
  }
  let cursors: Record<string, string | null> | null;
  try {
    cursors = normalizeSearchCursors(nextCursorsRaw);
  } catch (error) {
    return inputError(tool, (error as Error).message);
  }

  const resultsByFundStatus: Record<string, RecordValue> = {};
  try {
    for (const fundStatus of ACTIVE_FUND_STATUSES) {
      if (cursors && Object.prototype.hasOwnProperty.call(cursors, fundStatus) && cursors[fundStatus] == null) continue;
      const response = await secGet("/general-info/profiles", {
        project_info: projectInfo,
        company_info: companyInfo,
        fund_class_name: fundClassName,
        fund_status: fundStatus,
        next_cursor: cursors == null ? "" : cursors[fundStatus] ?? "",
        page_size: pageSize,
      });
      const nextCursor = optionalString(response.next_cursor);
      resultsByFundStatus[fundStatus] = {
        candidates: response.items.map(searchCandidate),
        nextCursor,
        hasMore: Boolean(nextCursor),
      };
    }
  } catch (error) {
    return errorResult(tool, error as ProviderError);
  }
  const candidates = Object.values(resultsByFundStatus)
    .flatMap((result) => result.candidates as RecordValue[]);
  const nextCursors = Object.fromEntries(
    ACTIVE_FUND_STATUSES.map((status) => [
      status,
      (resultsByFundStatus[status]?.nextCursor as string | null | undefined) ?? null,
    ]),
  );
  const payload = basePayload(candidates.length ? "OK" : "FUND_NOT_FOUND");
  Object.assign(payload, {
    scope: "PROFILE_CATALOG",
    requestedFilters: {
      projectInfo,
      companyInfo,
      fundClassName,
      fundStatuses: Object.keys(resultsByFundStatus),
      pageSizePerStatus: pageSize,
    },
    candidates,
    candidateCount: candidates.length,
    resultsByFundStatus,
    nextCursors,
    hasMore: Object.values(nextCursors).some((cursor) => cursor != null),
    recovery: recovery(
      candidates.length ? "USE_CANDIDATE_PROJ_ID" : "CHECK_PROJECT_OR_COMPANY_NAME",
      candidates.length
        ? "Select an exact candidate explicitly; no candidate has been promoted into fund evidence."
        : "No active SEC profile candidate matched these filters. Try an official project name, abbreviation, or company name.",
    ),
  });
  return mcpSuccess(tool, JSON.stringify(payload), { source: SOURCE });
}

function freshness(dataDate: string | null, asOfDate: string): RecordValue {
  const days = dataDate && isIsoDate(dataDate) && isIsoDate(asOfDate)
    ? Math.round((Date.parse(`${asOfDate}T00:00:00Z`) - Date.parse(`${dataDate}T00:00:00Z`)) / 86_400_000)
    : null;
  return { asOfDate, dataDate, calendarDaysFromAsOf: days, timezone: TIMEZONE };
}

function navPayload(
  identity: FundIdentity,
  rows: RecordValue[],
  nextCursor: string | null,
  startDate: string,
  asOfDate: string,
  lookbackDays: number,
  identityResolution?: RecordValue,
): RecordValue {
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
    nextCursor,
    hasMore: Boolean(nextCursor),
    recovery: recovery(
      latest ? "NONE" : "EXPAND_WINDOW_UP_TO_90_DAYS",
      latest ? "Latest returned NAV selected by nav_date, not provider row order." : "No NAV was returned inside this requested window; try a later as_of_date or a wider window.",
    ),
    ...(identityResolution ? { identityResolution } : {}),
  });
  return payload;
}

function navClassAmbiguityPayload(
  requestedFundClassName: string,
  projId: string,
  classes: string[],
  nextCursor: string | null,
): RecordValue {
  const payload = basePayload("AMBIGUOUS_SHARE_CLASS");
  Object.assign(payload, {
    scope: "PROJECT",
    requestedIdentity: { fundClassName: requestedFundClassName, projId },
    candidates: classes.map((fundClassName) => ({ fundClassName, projId })),
    nextCursor,
    hasMore: Boolean(nextCursor),
    recovery: recovery(
      "PROVIDE_SEC_FUND_CLASS_NAME",
      "The explicit proj_id returned multiple SEC fund classes; retry with one returned SEC fund_class_name.",
    ),
  });
  return payload;
}

async function explicitProjectNavPayload(
  requestedFundClassName: string,
  projId: string,
  startDate: string,
  asOfDate: string,
  lookbackDays: number,
): Promise<RecordValue> {
  const response = await secGet("/daily-info/nav", {
    proj_id: projId,
    start_nav_date: startDate,
    end_nav_date: asOfDate,
    page_size: MAX_PAGE_SIZE,
  });
  const rows = response.items.filter((row) => isIsoDate(row.nav_date));
  const nextCursor = optionalString(response.next_cursor);
  const sourceClasses = [...new Set(rows.map((row) => optionalString(row.fund_class_name)).filter((value): value is string => Boolean(value)))].sort();
  const exactRows = rows.filter((row) => String(row.fund_class_name ?? "") === requestedFundClassName);
  let selectedRows: RecordValue[];
  let identityStatus: string;
  if (exactRows.length) {
    selectedRows = exactRows;
    identityStatus = "NAV_PROJECT_ID_AND_SEC_CLASS_CONFIRMED";
  } else if (sourceClasses.length === 1 && !nextCursor) {
    selectedRows = rows;
    identityStatus = "NAV_PROJECT_ID_CONFIRMED_CLASS_ALIAS";
  } else if (rows.length) {
    return navClassAmbiguityPayload(requestedFundClassName, projId, sourceClasses, nextCursor);
  } else {
    selectedRows = [];
    identityStatus = "NAV_PROJECT_ID_UNVERIFIED_NO_ROWS";
  }
  const identity = identityFromNav(selectedRows[0] ?? {}, projId, requestedFundClassName);
  return navPayload(
    identity,
    selectedRows,
    nextCursor,
    startDate,
    asOfDate,
    lookbackDays,
    {
      status: identityStatus,
      method: "EXPLICIT_PROJ_ID_DIRECT_NAV",
      requestedFundClassName,
      sourceFundClassName: selectedRows.length ? identity.fundClassName : null,
    },
  );
}

export async function getThaiFundNav(
  fundClassNameRaw: unknown,
  projIdRaw: unknown,
  asOfDateRaw: unknown,
  lookbackDaysRaw: unknown,
  projectInfoRaw: unknown,
): Promise<string> {
  const tool = "get_thai_fund_nav";
  const requestedFundClassName = optionalString(fundClassNameRaw);
  if (!requestedFundClassName) return inputError(tool, "fund_class_name is required.");
  const explicitProjId = optionalString(projIdRaw);
  const asOfDate = asOfDateRaw == null || String(asOfDateRaw).trim() === "" ? bangkokToday() : String(asOfDateRaw).trim();
  if (!isIsoDate(asOfDate)) return inputError(tool, "as_of_date must be YYYY-MM-DD.");
  const lookbackDays = lookbackDaysRaw == null ? DEFAULT_NAV_LOOKBACK_DAYS : Number(lookbackDaysRaw);
  if (!Number.isInteger(lookbackDays) || lookbackDays < 1 || lookbackDays > MAX_NAV_LOOKBACK_DAYS) {
    return inputError(tool, "lookback_days must be an integer from 1 through 90.");
  }
  const startDate = addDays(asOfDate, -(lookbackDays - 1));

  if (explicitProjId) {
    try {
      const payload = await explicitProjectNavPayload(
        requestedFundClassName,
        explicitProjId,
        startDate,
        asOfDate,
        lookbackDays,
      );
      return mcpSuccess(tool, JSON.stringify(payload), { source: SOURCE, dataDate: payload.dataDate as string | null });
    } catch (error) {
      return errorResult(tool, error as ProviderError);
    }
  }

  const resolved = await resolveOrResponse(tool, requestedFundClassName, null, projectInfoRaw);
  if (resolved.response) return resolved.response;
  const identity = resolved.identity!;
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
  const payload = navPayload(identity, rows, optionalString(response.next_cursor), startDate, asOfDate, lookbackDays);
  return mcpSuccess(tool, JSON.stringify(payload), { source: SOURCE, dataDate: payload.dataDate as string | null });
}

interface NavBatchFund {
  reference: string;
  fundClassName: string;
  projId: string;
}

function normalizeNavBatchFunds(value: unknown): NavBatchFund[] {
  if (!Array.isArray(value) || !value.length) {
    throw new Error("funds must be a non-empty list of explicit fund_class_name and proj_id pairs.");
  }
  if (value.length > MAX_NAV_BATCH_FUNDS) {
    throw new Error(`funds supports at most ${MAX_NAV_BATCH_FUNDS} entries per request.`);
  }
  return value.map((item, index) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) {
      throw new Error(`funds[${index}] must be an object.`);
    }
    const entry = item as Record<string, unknown>;
    const fundClassName = optionalString(entry.fund_class_name);
    const projId = optionalString(entry.proj_id);
    if (!fundClassName || !projId) {
      throw new Error(`funds[${index}] requires fund_class_name and proj_id.`);
    }
    return { reference: optionalString(entry.reference) ?? fundClassName, fundClassName, projId };
  });
}

function navBatchItemFailure(
  fund: NavBatchFund,
  error: ProviderError,
  startDate: string,
  asOfDate: string,
  lookbackDays: number,
): RecordValue {
  const payload = basePayload(error.code);
  Object.assign(payload, {
    reference: fund.reference,
    scope: "SHARE_CLASS",
    requestedIdentity: { fundClassName: fund.fundClassName, projId: fund.projId },
    requestedWindow: { startDate, endDate: asOfDate, lookbackCalendarDays: lookbackDays, timezone: TIMEZONE },
    dataDate: null,
    freshness: freshness(null, asOfDate),
    nav: null,
    recovery: recovery(error.recoveryAction, error.message),
  });
  return payload;
}

export async function getThaiFundNavBatch(
  fundsRaw: unknown,
  asOfDateRaw: unknown,
  lookbackDaysRaw: unknown,
): Promise<string> {
  const tool = "get_thai_fund_nav_batch";
  let funds: NavBatchFund[];
  try {
    funds = normalizeNavBatchFunds(fundsRaw);
  } catch (error) {
    return inputError(tool, (error as Error).message);
  }
  const asOfDate = asOfDateRaw == null || String(asOfDateRaw).trim() === "" ? bangkokToday() : String(asOfDateRaw).trim();
  if (!isIsoDate(asOfDate)) return inputError(tool, "as_of_date must be YYYY-MM-DD.");
  const lookbackDays = lookbackDaysRaw == null ? DEFAULT_NAV_LOOKBACK_DAYS : Number(lookbackDaysRaw);
  if (!Number.isInteger(lookbackDays) || lookbackDays < 1 || lookbackDays > MAX_NAV_LOOKBACK_DAYS) {
    return inputError(tool, "lookback_days must be an integer from 1 through 90.");
  }
  const startDate = addDays(asOfDate, -(lookbackDays - 1));
  const items: RecordValue[] = [];
  for (const fund of funds) {
    let item: RecordValue;
    try {
      item = await explicitProjectNavPayload(fund.fundClassName, fund.projId, startDate, asOfDate, lookbackDays);
    } catch (error) {
      const provider = error as ProviderError;
      if (provider.code === "SOURCE_UNCONFIGURED" || provider.code === "AUTH_ERROR") return errorResult(tool, provider);
      item = navBatchItemFailure(fund, provider, startDate, asOfDate, lookbackDays);
    }
    item.reference = fund.reference;
    items.push(item);
  }
  const incompleteReferences = items
    .filter((item) => item.status !== "OK")
    .map((item) => item.reference as string);
  const dataDates = items
    .map((item) => item.dataDate)
    .filter((value): value is string => typeof value === "string");
  const payload = basePayload(incompleteReferences.length ? "PARTIAL" : "OK");
  Object.assign(payload, {
    scope: "VAULT_BATCH",
    requestedWindow: { startDate, endDate: asOfDate, lookbackCalendarDays: lookbackDays, timezone: TIMEZONE },
    items,
    itemCount: items.length,
    incompleteReferences,
    dataDate: dataDates.length ? [...dataDates].sort()[dataDates.length - 1] : null,
    recovery: recovery(
      incompleteReferences.length ? "RETRY_INCOMPLETE_FUNDS" : "NONE",
      incompleteReferences.length
        ? "Retry only incompleteReferences after reviewing each item recovery action."
        : "All requested direct-project NAV lookups completed.",
    ),
  });
  return mcpSuccess(tool, JSON.stringify(payload), { source: SOURCE, dataDate: payload.dataDate as string | null });
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
  projectInfoRaw: unknown,
): Promise<string> {
  const tool = "get_thai_fund_factsheet";
  const resolved = await resolveOrResponse(tool, fundClassNameRaw, projIdRaw, projectInfoRaw);
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
  projectInfoRaw: unknown,
): Promise<string> {
  const tool = "get_thai_fund_dividend_history";
  const resolved = await resolveOrResponse(tool, fundClassNameRaw, projIdRaw, projectInfoRaw);
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
