/**
 * company_ir provider (registry-only, 2.10.0): delegates to the approved company IR page collector.
 * Moved out of news.ts in 2.15.1 (pure restructure); providers never import news.ts.
 */

import { companyIrPageRegistry, companyIrPageEntry, collectCompanyIrPageEvents } from "./company-ir-page.js";
import type { NewsProviderResultWithDiagnostics } from "./types.js";

/**
 * The company_ir source (2.10.0): approved company IR registry entries only.
 * Open-web discovery (probing the profile website and its investor/ir hosts,
 * then fetching linked feeds on any host) was removed with the fetch
 * allow-list. Without an approved entry the source is NOT_REGISTERED, the
 * same status an unregistered company_ir_page source reports.
 */
export async function collectCompanyIrEvents(
  ticker: string,
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<NewsProviderResultWithDiagnostics> {
  const entry = companyIrPageEntry(ticker);
  if (!entry || entry.status !== "approved") {
    return {
      items: [],
      warnings: [],
      used: false,
      diagnostics: {
        registryVersion: companyIrPageRegistry.registryVersion,
        registrySchemaVersion: companyIrPageRegistry.schemaVersion,
        status: "NOT_REGISTERED",
        approvalStatus: "not_registered",
        allowedAction: "registry_review_required",
      },
    };
  }
  return collectCompanyIrPageEvents(ticker, maxResults, retrievedAt, startDate, endDate, lookbackDays);
}
