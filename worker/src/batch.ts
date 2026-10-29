/**
 * Multi-ticker batches (Phase 4 unit 5, moved out of yahoo-finance.ts): run a per-ticker handler a few symbols at a
 * time, each in its own subrequest-budget scope, and shape every symbol's result or error into one batch payload.
 */

import { isBudgetExhaustedError, withBudgetWatch } from "./request-context.js";
import { ErrorCode } from "./response.js";

/**
 * Batch ticker caps. Cloudflare Workers Free allows 50 subrequests per
 * invocation (Cache API calls and redirect hops included), and since 2.11.0 new
 * upstream work stops at a budget of 45 (request-context.ts). The per-tool caps
 * live in tool_catalog.json `tickerCaps` (2.12.0): each action's schema
 * maxItems is generated from them, and the tool-call gate in tools.ts rejects a
 * longer array with BATCH_LIMIT_EXCEEDED before any fetch. Nothing is truncated.
 * MAX_TICKERS is only the default for runPartialBatch callers without a catalog
 * cap (analyze_position_signals).
 */
const MAX_TICKERS = 5;

/** Parse a JSON string returned by a single-ticker handler, falling back to a
 *  structured error object if the string is not valid JSON (e.g. plain error messages). */
export function safeJsonParse(s: string, ticker: string): Record<string, unknown> {
  try {
    return JSON.parse(s) as Record<string, unknown>;
  } catch {
    return { error: true, message: s, ticker };
  }
}

function toBatchError(message: string, code = "PROVIDER_ERROR", retryable = false): Record<string, unknown> {
  return { code, message, retryable };
}

function normalizeBatchSymbolResult(parsed: unknown, ticker: string): Record<string, unknown> {
  if (parsed != null && typeof parsed === "object") {
    const p = parsed as Record<string, unknown>;
    if (typeof p.ok === "boolean" && ("data" in p || "error" in p)) {
      if (p.ok === true) {
        return { ok: true, data: p.data ?? null };
      }
      const errObj = p.error as Record<string, unknown> | null | undefined;
      const code = typeof errObj?.code === "string" ? errObj.code : "PROVIDER_ERROR";
      const message = typeof errObj?.message === "string" ? errObj.message : `Error for ${ticker}`;
      const retryable = errObj?.retryable === true || code === "PROVIDER_TIMEOUT";
      return { ok: false, data: null, error: toBatchError(message, code, retryable) };
    }
    if (p.error === true || p.error != null) {
      const message = typeof p.message === "string" ? p.message : (typeof p.error === "string" ? p.error : `Error for ${ticker}`);
      // R-news D (2.22.0, F13c): a legacy-envelope entry (MCP_ENVELOPE_V2=false) keeps its own code, read as the V2 branch above
      // reads error.code; the message heuristics below only classify entries without one.
      if (typeof p.code === "string") {
        return { ok: false, data: null, error: toBatchError(message, p.code, p.retryable === true || p.code === "PROVIDER_TIMEOUT") };
      }
      const lower = message.toLowerCase();
      if (lower.includes("no data found for ticker") || lower.includes("not found") || lower.includes("api error 404")) {
        return { ok: false, data: null, error: toBatchError(message, "TICKER_NOT_FOUND", false) };
      }
      if (lower.includes("timeout")) {
        return { ok: false, data: null, error: toBatchError(message, "PROVIDER_TIMEOUT", true) };
      }
      return { ok: false, data: null, error: toBatchError(message, "PROVIDER_ERROR", false) };
    }
    return { ok: true, data: p, error: null };
  }
  return { ok: false, data: null, error: toBatchError(`Malformed response for ${ticker}`, "PROVIDER_ERROR", false) };
}

// Per-ticker work in a batch runs a few symbols at a time: enough overlap to
// cut latency without multiplying simultaneous Yahoo connections.
export const BATCH_TICKER_CONCURRENCY = 3;

/** Map items with at most `limit` pending calls; results keep input order. */
export async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < items.length) {
      const index = next++;
      results[index] = await fn(items[index]);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}

export async function runPartialBatch(
  tickers: string[],
  perTicker: (ticker: string) => Promise<string>,
  maxTickers: number = MAX_TICKERS,
): Promise<string> {
  // Capped actions are rejected earlier, by the tool-call gate (tools.ts), with
  // BATCH_LIMIT_EXCEEDED; this check only guards direct callers.
  if (tickers.length > maxTickers) {
    return JSON.stringify({
      error: true,
      code: "INPUT_VALIDATION_ERROR",
      message: `Too many tickers: ${tickers.length}. Maximum is ${maxTickers} per call.`,
    });
  }
  const out: Record<string, unknown> = {};
  let successCount = 0;
  let errorCount = 0;
  // Each ticker runs in its own scope, so a fetch the subrequest budget refused
  // is charged to the ticker that asked for it (2.11.0).
  const settled = await mapWithConcurrency(tickers, BATCH_TICKER_CONCURRENCY, (t) =>
    withBudgetWatch(() => perTicker(t).then(
      (raw) => ({ raw }),
      (error: unknown) => ({ error }),
    )).then(({ result, starved }) => ({ ...result, starved })));
  for (const [index, t] of tickers.entries()) {
    const starved = settled[index].starved;
    try {
      const result = settled[index];
      if ("error" in result) throw result.error;
      const raw = result.raw;
      const parsed = safeJsonParse(raw, t);
      const shaped = normalizeBatchSymbolResult(parsed, t);
      if (shaped.ok === true) successCount += 1;
      else errorCount += 1;
      out[t] = shaped.ok === true
        ? (shaped.data ?? null)
        : starved
          ? {
              error: true,
              code: ErrorCode.PROVIDER_BUDGET_EXHAUSTED,
              message: typeof (shaped.error as Record<string, unknown> | undefined)?.message === "string"
                ? (shaped.error as Record<string, unknown>).message
                : `Error for ${t}`,
              retryable: true,
              recoveryAction: "RETRY_LATER",
            }
          : {
            error: true,
            code: typeof (shaped.error as Record<string, unknown> | undefined)?.code === "string"
              ? (shaped.error as Record<string, unknown>).code
              : "PROVIDER_ERROR",
            message: typeof (shaped.error as Record<string, unknown> | undefined)?.message === "string"
              ? (shaped.error as Record<string, unknown>).message
              : `Error for ${t}`,
            retryable: (shaped.error as Record<string, unknown> | undefined)?.retryable === true,
          };
    } catch (e) {
      errorCount += 1;
      const message = e instanceof Error ? e.message : String(e);
      const lower = message.toLowerCase();
      if (starved || isBudgetExhaustedError(e)) {
        out[t] = {
          ok: false,
          data: null,
          error: { ...toBatchError(message, ErrorCode.PROVIDER_BUDGET_EXHAUSTED, true), recoveryAction: "RETRY_LATER" },
        };
        continue;
      }
      const code = lower.includes("timeout") ? "PROVIDER_TIMEOUT"
        : lower.includes("api error 404") ? "TICKER_NOT_FOUND"
        : "PROVIDER_ERROR";
      out[t] = {
        ok: false,
        data: null,
        error: toBatchError(message, code, lower.includes("timeout")),
      };
    }
  }
  out.__batchMeta = {
    partialSuccess: successCount > 0 && errorCount > 0,
    successCount,
    errorCount,
  };
  return JSON.stringify(out);
}
