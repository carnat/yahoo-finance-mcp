/**
 * Finnhub company-news provider: the API fetch and collectFinnhubEvents.
 * Moved out of news.ts in 2.15.1 (pure restructure); providers never import news.ts.
 */

import { providerFetch } from "../../request-context.js";
import { getWorkerVar } from "../../response.js";
import { UA } from "../../clients/provider-http.js";
import { _str } from "../../yahoo-finance.js";
import {
  sourceDecisionUse,
  normalizeIso,
  shortText,
  eventTypeFromKeywords,
  makeDupGroupId,
  withinDateWindow,
  tickerInText,
} from "../shared.js";
import type { NewsProviderResultWithDiagnostics } from "./types.js";

const FINNHUB_COMPANY_NEWS_API = "https://finnhub.io/api/v1/company-news";
export async function collectFinnhubEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<NewsProviderResultWithDiagnostics> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const rejectionCounts: Record<string, number> = {};
  const diagnostics: Record<string, unknown> = {
    rawCount: 0,
    retrievedCount: 0,
    acceptedCount: 0,
    collectedCount: 0,
    rejectedCount: 0,
    rejectionCounts,
    attempted: true,
    completed: false,
  };
  const reject = (reason: string): void => {
    rejectionCounts[reason] = (rejectionCounts[reason] ?? 0) + 1;
    diagnostics.rejectedCount = Number(diagnostics.rejectedCount ?? 0) + 1;
  };
  const token = getWorkerVar("FINNHUB_API_KEY") ?? getWorkerVar("FINNHUB_TOKEN");
  if (!token) {
    warnings.push({ code: "SOURCE_UNAVAILABLE", message: "Finnhub company-news source is not configured; skipped.", severity: "warning" });
    diagnostics.attempted = false;
    return { items, warnings, used: false, diagnostics };
  }
  try {
    const now = new Date();
    const from = new Date(now.getTime() - (lookbackDays ?? 14) * 86400000).toISOString().slice(0, 10);
    const to = now.toISOString().slice(0, 10);
    const url = `${FINNHUB_COMPANY_NEWS_API}?symbol=${encodeURIComponent(ticker.toUpperCase())}&from=${from}&to=${to}`;
    const resp = await providerFetch(url, { headers: { "User-Agent": UA, "X-Finnhub-Token": token } });
    if (!resp.ok) {
      await resp.body?.cancel();
      if (resp.status === 401 || resp.status === 403) {
        throw new Error(`FINNHUB_AUTH_ERROR:${resp.status}`);
      } else if (resp.status === 429) {
        throw new Error("FINNHUB_RATE_LIMITED:429");
      } else {
        throw new Error(`HTTP ${resp.status}`);
      }
    }
    const rawJson = await resp.json();
    if (!Array.isArray(rawJson)) {
      throw new Error("FINNHUB_PROVIDER_CHANGED");
    }
    const news = rawJson as unknown[];
    diagnostics.rawCount = news.length;
    diagnostics.retrievedCount = news.length;
    const tickerU = ticker.toUpperCase();
    for (const n of news) {
      if (!n || typeof n !== "object" || Array.isArray(n)) {
        reject("INVALID_ROW");
        continue;
      }
      const row = n as Record<string, unknown>;
      const title = _str(row.headline).trim();
      if (!title) {
        reject("MISSING_HEADLINE");
        continue;
      }
      const summary = _str(row.summary).trim();
      const originalSource = _str(row.source).trim() || null;
      const urlStr = _str(row.url).trim() || null;
      const publishedAt = normalizeIso(row.datetime);
      const duplicateGroupId = makeDupGroupId(tickerU, title, publishedAt, null);
      if (!duplicateGroupId) {
        warnings.push({ code: "DEDUPE_WEAK_KEY", message: "Weak dedupe key for at least one item.", severity: "warning" });
      }
      const relevance = tickerInText(`${title} ${summary}`, tickerU) ? "HIGH" : "LOW";
      const item: Record<string, unknown> = {
        title,
        source: "finnhub",
        originalSource,
        sourceType: "company_news",
        publishedAt,
        retrievedAt,
        url: urlStr,
        issuer: null,
        tickers: [tickerU],
        eventType: eventTypeFromKeywords(title, summary),
        summary: shortText(summary || title, 240),
        evidenceText: shortText(summary || title, 180),
        confidence: urlStr ? "MEDIUM" : "LOW",
        tickerRelevance: relevance,
        decisionUse: sourceDecisionUse("company_news"),
        duplicateGroupId,
      };
      if (!withinDateWindow(_str(item.publishedAt) || null, startDate, endDate, lookbackDays)) {
        reject("DATE_OUTSIDE_WINDOW");
        continue;
      }
      diagnostics.acceptedCount = Number(diagnostics.acceptedCount ?? 0) + 1;
      if (items.length < maxResults) items.push(item);
    }
  } catch (e) {
    // R-news Batch C (decision 11): one message template per status, Python's `HTTP <n>` wording. The internal FINNHUB_* tags
    // never reach the message; sourceStatus infers AUTH_ERROR / RATE_LIMITED / PROVIDER_CHANGED from these message prefixes.
    const msg = e instanceof Error ? e.message : String(e);
    if (msg.startsWith("FINNHUB_AUTH_ERROR:")) {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: `Finnhub auth error: HTTP ${msg.slice("FINNHUB_AUTH_ERROR:".length)}`, severity: "warning" });
    } else if (msg.startsWith("FINNHUB_RATE_LIMITED")) {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: "Finnhub rate limited: HTTP 429", severity: "warning" });
    } else if (msg.startsWith("FINNHUB_PROVIDER_CHANGED")) {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: "Finnhub provider changed: unexpected response format", severity: "warning" });
    } else {
      warnings.push({ code: "SOURCE_UNAVAILABLE", message: `Finnhub source unavailable: ${msg}`, severity: "warning" });
    }
    return { items, warnings, used: false, diagnostics };
  }
  diagnostics.collectedCount = items.length;
  diagnostics.completed = true;
  return { items, warnings, used: true, diagnostics };
}
