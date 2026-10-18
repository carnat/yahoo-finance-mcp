# R-news part 2 (2.16.0): news source registry, operator off switch, SEC in the defaults

**Source:** the approved Q3i triage, §3a part 2 (owner, 2026-10-18), and the owner's default-sources decision of the same date.
**Version:** 2.16.0 (minor). The default sources change, and a new operator setting is added.

**Rule zero:** apart from the effects listed in §4, every output is byte-identical. That covers explicit `sources` requests and every news tool when no source is disabled.

## 1. Source registry (data, both runtimes)

**New file `worker/src/news-source-registry.json`.** Python reads it from there, just as it already reads `news-source-capabilities.json` (Phase 6 moves shared data). Shape:

```json
{
  "schemaVersion": "2026-10-18",
  "defaultSources": ["yahoo_finance_news", "yahoo_finance_press_releases", "sec", "finnhub", "marketaux"],
  "sources": {
    "<name>": { "tier": "official" | "aggregator", "requiresKey": ["ENV_NAME", ...] | [], "approxSubrequests": <int>, "description": "<one line>" }
  }
}
```

- **One entry per source name** that `normalizeSources` / `_normalize_sources` accept today:
  - `sec`, `company_ir`, `company_ir_page`, `newswire`: tier `official`;
  - `yahoo_finance`, `yahoo_finance_news`, `yahoo_finance_press_releases`, `finnhub`, `marketaux`: tier `aggregator`.
  - `requiresKey` is taken from the code: the Finnhub key name and the Marketaux token names.
  - `approxSubrequests` holds a measured cold count for one ticker (see §5); round it up.
- **Defaults come from the registry:** `DEFAULT_NEWS_SOURCES` / `_DEFAULT_NEWS_SOURCES` are read from `defaultSources`. The two runtime constants are removed or derived from it.
- **The `sec` position** in the list is right after `yahoo_finance_press_releases` (planner decision).
- **Consistency test:** the registry names equal each runtime's accepted source names, and both runtimes load the same defaults.

## 2. Operator off switch

**Setting: `NEWS_SOURCES_DISABLED`.**
- **Where it is set:** a Worker var read with `getWorkerVar`, or a Python environment variable.
- **Value:** a comma-separated list of names, each trimmed and lower-cased. Unknown names are ignored. Empty or unset means nothing is disabled.
- **Not set in `wrangler.toml` by default.** The docs show how to add it.

**A disabled source is applied** in the shared source selection that every news tool uses (`get_company_news`, `search_company_news`, `get_company_press_releases`, `get_public_event_timeline`, `verify_company_event`, `get_sec_recent_events` where it selects sources), whether the source came from the defaults or from an explicit `sources` list:
- It is never fetched and costs no subrequests.
- `sourceStatus[<name>] = { status: "SOURCE_UNCONFIGURED", attempted: false, reasonCode: null, ...the same zero counters that an unconfigured provider reports today }`. Reuse the existing unconfigured shape for that source family.
- One warning per disabled source that was selected: `{ code: "SOURCE_UNAVAILABLE", severity: "info", source: "<name>", message: "Source <name> is disabled by the server operator." }`. Use the existing warning shape.
- `coverage.failedSources` gains `{ source, status: "SOURCE_UNCONFIGURED", attempted: false, reasonCode: null }`, so `coverage.state` reads PARTIAL. A disabled source never yields NOT_FOUND on its own: the collection status follows the existing source-limited path.
- The fallback order is unchanged for the remaining sources. If `marketaux` is disabled, the fallback is skipped and reported as above.

**No new code:** no new status, warning code, reasonCode or field (Q3i rule P4).

## 3. Telemetry

Disabled sources are not recorded. Telemetry stays as it is.

## 4. Intended output changes

1. **Default calls include SEC.** `get_company_news` and `search_company_news` with no `sources` now include `sec`: SEC items, `sourceStatus.sec`, `sourcesUsed`, `attemptedSources` and `requestedSources` handling as for an explicit `sec`.
2. **Disabled sources** are reported as described in §2, only when the operator sets the variable.

## 5. Budget guard

- **Measure first:** run the Worker bundle locally against live upstream with an empty cache stub and a fetch counter, as the 2.12.0 snapshot measurement did. Measure the cold subrequests for default `get_company_news` (one ticker), before and after adding SEC.
- **STOP condition:** if the after count exceeds **40** (the 45 budget minus headroom), stop and report.
- **Recording:** put the numbers in the registry (`approxSubrequests`) and in the PR body.

## 6. Tests (both runtimes, mode equal in the parity harness)

- The defaults include `sec`, and a default call shows `sourceStatus.sec`.
- One disabled default source (`finnhub`): no fetch (fetch spy), `SOURCE_UNCONFIGURED`, the warning, coverage PARTIAL.
- An explicitly requested disabled source gets the same reporting.
- Every requested source disabled: no fetch, source-limited status, never NOT_FOUND.
- Unknown names in the setting are ignored, and an empty setting changes nothing.
- The registry consistency test.
- Update the existing default-source cases to the new defaults, listing each changed case.

## 7. Release

- **Docs section "News Source Registry and Off Switch (2.16.0)":** the registry, how to disable a source in `wrangler.toml` `[vars]` and locally, the new defaults, and the budget numbers.
- **Live checks:**
  - `health_check` reports 2.16.0;
  - default `get_company_news` AAPL shows `sourceStatus.sec`;
  - an explicit `sources:["finnhub"]` call still works;
  - the canaries pass.
  - The off switch is checked offline only: production does not set it.

## 8. Planner decisions taken during execution (2026-10-18)

1. **Status value.** A disabled source's `sourceStatus` row uses the news pipeline's existing `"UNCONFIGURED"` status, not `SOURCE_UNCONFIGURED`. The existing coverage sets already treat `UNCONFIGURED` as failed, which gives `failedSources` and PARTIAL. `failedSources[].status` is `UNCONFIGURED` as well. This replaces `SOURCE_UNCONFIGURED` throughout §2.
2. **One row shape for every disabled source:** `{ status: "UNCONFIGURED", rawCount: 0, filteredCount: 0, attempted: false }`. All four fields already exist in the news rows. Do not copy the family-specific unconfigured rows: Marketaux's `configure_marketaux_api_token` advice would be wrong for an operator disable.
3. **Message matching.**
   - A disabled source's row is always the row in point 2. Message-based status inference, such as "finnhub" in a `SOURCE_UNAVAILABLE` message giving `PROVIDER_ERROR`, must not override it.
   - Disabling `finnhub` counts as limited for `marketauxFallbackReason`, so the Marketaux fallback runs unless `marketaux` is disabled too.
4. **`get_sec_recent_events` is not affected.** It is an explicit SEC filing tool and selects no news sources. The docs say so.
5. **`get_company_press_releases`:** the disabled report (row, warning, failedSources entry) is attached once, in the tool's final payload, keyed by the source name. The inner `collectCompanyEvents` call must not add a second copy.
6. **Advertised defaults.** The `get_company_news` and `search_company_news` schemas update their `default` lists and description text to the new defaults (sec included) in both runtimes, keeping `check_tool_sync` green. The `tools/list` change is expected in this minor release.
7. **Tickers SEC does not cover (revised 2026-10-18; supersedes the first version of this point).**
   - Reusing the Finnhub skip shape always gives PARTIAL: `NOT_ELIGIBLE` is a limiting status, as pinned by `news.coverage.not_eligible_source_makes_coverage_partial`. The planner accepts that rather than special-casing coverage.
   - **Rule:** when the ticker has no SEC CIK, `sec` is reported as not eligible, whether it came from the defaults or was requested explicitly:
     - `sourceStatus.sec.status = "NOT_ELIGIBLE"` (the existing status) with `reasonCode: null`;
     - the existing `SOURCE_NOT_ELIGIBLE` warning, with the message "SEC has no filings for <TICKER> (no SEC CIK).";
     - the existing coverage, action and failureMode handling for `NOT_ELIGIBLE` (PARTIAL, `CHECK_OFFICIAL_RELEASES`, failureMode `SOURCE_NOT_ELIGIBLE`).
   - **Splitting the warning:** `collectSecEvents` / `_collect_sec_events` split "no CIK" (this rule) from "submissions fetch failed", which stays `PROVIDER_ERROR` with "SEC submissions source unavailable.".
   - **Why:** foreign listings already read PARTIAL through Finnhub ineligibility, so this is consistent and never overstates. It also stops reporting a missing CIK as a provider error.
   - **Accepted from the Worker step:**
     - press-release status branches use only the active (not disabled) sources, and all-disabled gives `SOURCE_LIMITED_NOT_FOUND`;
     - a disabled Yahoo is not counted as limited by the Marketaux gate, because Marketaux already runs on `HIGHER_PRIORITY_EMPTY`.
