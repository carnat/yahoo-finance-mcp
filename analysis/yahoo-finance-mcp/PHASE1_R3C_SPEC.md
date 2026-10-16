# Phase 1, release 3c (2.11.0): per-invocation subrequest budget guard

This release implements brief §Phase 1, "Per-invocation subrequest budget guard (V3)" and "News budget follow-through". It is the last Phase 1 release.

**Owner decision (2026-10-16):** Finnhub, Alpha Vantage and Marketaux are quota-limited free tiers, not paid providers. They stay in the defaults. `PROVIDER_OPT_IN_REQUIRED` is reserved for a provider the owner pays for later, and nothing emits it in this release. That settles the paid-provider half of Q2b with no contract change.

**Order:** the Worker goes first, then the `python-mirror` port, the parity tests, docs, `release-clerk` and `live-verifier`.

**Version: 2.11.0** (minor). `PROVIDER_BUDGET_EXHAUSTED` enters the public contract. The name was accepted under Q11 on 2026-10-15.

## 1. Why

- **The limit:** Workers Free allows **50 subrequests per invocation**. Cache API calls and redirect hops count toward it, and the limit cannot be raised (V3).
- **What happens today when a call needs more:** Cloudflare throws "Too many subrequests". The tool then reports sources as `PROVIDER_ERROR`, or as a clean absence, under a top-level `ok: true`. In V3, MSFT returned 0 items this way.
- **Calls measured over the limit:**
  - `analyze_credit_health` at n=10: about 62;
  - `get_company_news` with defaults: about 61, before 2.10.0 removed IR discovery.
- **What this release does:** stop starting new upstream work before the limit, and say so in a typed state.

## 2. Counting (Worker, `worker/src/request-context.ts`)

- **Scope:** the invocation is the `/mcp` request. `withCacheScope` already wraps it (index.ts), and scopes nest, so the root scope is the invocation.
- **Budget:** add `subrequests: number` to the **root** scope only, through a helper that walks to the root.
  - `SUBREQUEST_LIMIT = 50` and `SUBREQUEST_RESERVE = 5` mean new work stops at **45** (`SUBREQUEST_BUDGET`).
  - The reserve covers the response path and any `waitUntil` cache writes.
- **What counts:**
  - every `fetch` that `providerFetch` makes, **including each redirect hop** (one per hop, as `upstreamRequests` already does);
  - every Cache API call: `match`, `put` and `delete` in the `edgeCache` wrappers (yahoo-finance.ts ~149–221, ~943–960, ~4967–4984).
  - Count at the call site, through one exported `consumeSubrequest(kind: "fetch" | "cache"): boolean`.
- **Fetches:**
  - `providerFetch` calls `consumeSubrequest("fetch")` before each hop.
  - When the call returns false, it throws `BudgetExhaustedError`, a new class exported from `request-context.ts`, with `name = "BudgetExhaustedError"`. It does not call `fetch`.
- **Cache calls:**
  - When the budget is spent, a cache call is **skipped, not thrown**: `match` returns a miss and `put`/`delete` are no-ops.
  - A skipped cache read must never turn a cache hit into an error. Skipped cache calls do not consume budget.
- **Outside a request scope** (unit tests, scheduled code): counting is a no-op and never throws.
- **Real "Too many subrequests" errors** still get classified, as a backstop: Cloudflare's own error text maps to the same typed state as `BudgetExhaustedError`. See §3.

## 3. Reporting

**Per-source status `PROVIDER_BUDGET_EXHAUSTED`** (Q11):
- **Where a source row exists, it gets this status:**
  - news and events (`sourceStatus`, `attemptedSources`, coverage);
  - the transcript waterfall (`attemptedSources`);
  - consensus provider lists in the evidence pack.
- **Rules for that row:**
  - It is never `PROVIDER_ERROR`, `EMPTY_RESULT` or a clean absence.
  - In news coverage, a starved source goes in `failedSources` with `status: "PROVIDER_BUDGET_EXHAUSTED"`. The status itself states the cause, so `reasonCode` stays `null`; no new reason code.
  - Coverage `state` is `PARTIAL` or worse, never `FULL`.
  - `recommendedNextAction` is the existing `RETRY_RETRYABLE_SOURCES`: add `PROVIDER_BUDGET_EXHAUSTED` to the retryable statuses in `buildCoverage` (news.ts ~376, next to `RATE_LIMITED` and `TIMEOUT`).
- **Existing news classifier** (news.ts ~1618, `failureMode` `WORKER_SUBREQUEST_LIMIT`): keep `WORKER_SUBREQUEST_LIMIT` as the `failureMode`, because it is a frozen value. Set the source **status** to `PROVIDER_BUDGET_EXHAUSTED` wherever that failure mode, or `BudgetExhaustedError`, is detected. It stays retryable.

**Whole-tool failure.** Sometimes a tool has no per-source rows and its main fetch is refused for budget, for example a ticker batch that runs out partway.
- **A batch tool** keeps its existing per-ticker failure envelope, with error code `PROVIDER_BUDGET_EXHAUSTED`, `recoveryAction: "RETRY_LATER"` and `retryable: true`. The tickers that completed are returned as normal.
- **A single tool call** returns `ok: false` with that error code and recovery action.
- **The planner widens the Q11 kind for this:** `PROVIDER_BUDGET_EXHAUSTED` is also allowed as an error code. It already has the same meaning. Add it to the Worker's error-code type and to Python `ErrorCode`.

**Warning.** Every response where the guard refused at least one fetch carries a `meta.warnings` row:
`{ code: "PROVIDER_BUDGET_EXHAUSTED", message: "Stopped starting new upstream requests at the per-request subrequest budget (45 of 50); some sources were not queried.", severity: "warning" }`
- One row per response, added where `meta` is built (response.ts / mcp.ts), driven by a `budgetExhausted` flag on the root scope.

## 4. Python mirror (stdio has no subrequest limit)

- **Mirror the guard** so outputs stay equal under the harness. Use a `contextvars` counter per tool invocation in `yfmcp/url_policy.py`. It is the single fetch seam since 2.10.0.
  - `open_checked` and `urlopen` consume one unit per request and per redirect hop.
  - At 45 they raise `BudgetExhaustedError`, a subclass of `UrlPolicyError`, so it lands in each call site's existing failure path. Map it to the same statuses and codes as §3.
- **Python has no Cache API.** Its in-process caches do not count.
  - **Documented divergence:** in the Worker, edge-cache calls also consume budget. A Worker call can therefore run out slightly earlier than the same Python call.
  - The harness stubs caches, so parity cases are unaffected. Record this in the docs.
- **Python-only paths** (yfinance via curl_cffi) do not count, the same scope as the 2.10.0 allow-list.
- **The invocation boundary** is the tool-call wrapper. Reset the counter per call; grouped dispatch counts as one call.

## 5. Tests

- **Worker unit tests** (`scripts/test_worker_budget.py`, new; node bundle with fetch and cache spies):
  - 45 fetches succeed and the 46th throws without calling `fetch`;
  - redirect hops count;
  - cache `match`/`put` count, and are skipped (miss or no-op) once the budget is spent;
  - the root scope is shared by nested scopes;
  - there is no counting outside a scope;
  - one `meta.warnings` row appears when the guard fired.
- **Parity cases:**
  - a news call whose fake upstream would need more than 45 fetches reports the starved sources as `PROVIDER_BUDGET_EXHAUSTED`, with coverage `PARTIAL`;
  - a batch tool stops partway with the per-ticker code;
  - a call within budget is unchanged.
  - Mode is equal in both runtimes, and no cache is involved.
- **Regression:** the existing "Too many subrequests" classification tests now expect status `PROVIDER_BUDGET_EXHAUSTED`. List every expectation edit.
- **CI:** wire the new test into `ci.yml`.

## 6. Release and live verification

- **Docs section "Subrequest Budget Guard (2.11.0)":**
  - the 45/50 rule;
  - what counts;
  - the status, error code and warning;
  - the Python divergence;
  - that Finnhub, Alpha Vantage and Marketaux stay default (owner decision), and that `PROVIDER_OPT_IN_REQUIRED` is reserved.
- **Live checks:**
  - `analyze_credit_health` with 10 tickers returns `PROVIDER_BUDGET_EXHAUSTED` rows or tickers plus the warning, with no Cloudflare error and no `PROVIDER_ERROR` for the starved part;
  - `get_company_news` defaults for MSFT is unchanged (well under budget after 2.10.0);
  - the deploy canaries pass;
  - the connector `health_check` reports 2.11.0.
- **Brief:** tick the exit item "Anonymous or no-opt-in calls make zero paid-provider requests…". Its opt-in half is settled as not applicable by the owner's decision, and exhaustion returns the typed state.
