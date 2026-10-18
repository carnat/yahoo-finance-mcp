/**
 * The adapter result every news provider collector resolves to (2.15.1). This describes the shape the
 * collect<Source>Events functions already returned before the provider move; it changes no shape.
 *
 * - `items`: the provider's accepted event items, at most the caller's maxResults.
 * - `warnings`: the provider's warnings; the pipeline appends them to the response's warning list.
 * - `used`: true when the provider answered (its fetch and parse succeeded, even with no items); false when it
 *   was not reached, not eligible or failed. The pipeline reads it into sourcesUsed and the source status.
 * - `diagnostics`: per-source status details (counts, rejections, registry state) for the providers that
 *   report them: Yahoo, company_ir, company_ir_page, Finnhub and Marketaux. SEC and GlobeNewswire do not.
 *
 * There is no provider registry yet; that and the off switch are part 2.
 */
export type NewsProviderResult = {
  items: Record<string, unknown>[];
  warnings: Record<string, unknown>[];
  used: boolean;
  /**
   * SEC only (2.16.0): true when no CIK was found although SEC's ticker index was read, so the ticker has no SEC
   * filings. The news pipeline reports it as NOT_ELIGIBLE; get_sec_recent_events keeps its own reporting. Never serialized.
   */
  noCik?: boolean;
};

/** A NewsProviderResult that also carries the provider's per-source diagnostics. */
export type NewsProviderResultWithDiagnostics = NewsProviderResult & { diagnostics: Record<string, unknown> };
