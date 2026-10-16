# R4, first release (2.11.1): Q10 privacy-minimised Worker telemetry

**Owner decision Q10 (2026-10-15):** export privacy-minimised Worker operational telemetry before Phase 4, to get a latency and error baseline.

**Order (A3, accepted):** Q10 goes first in R4, so the baseline has time to accumulate before any Phase 4 merge.

**Version: 2.11.1** (patch). No tool output, schema or header changes. The Worker gains a telemetry binding and one structured log line per request. Python is unchanged: it is a local stdio server with nothing to export. Parity is unaffected; the harness proves this.

## 1. Where telemetry goes

### Workers Analytics Engine
- **Plan limits (Free):** 100,000 data points written per day, which matches the 100,000 requests/day cap. 10,000 read queries per day. 3 months retention. Up to 20 blobs and 20 doubles per data point.
- **Binding:** add to `worker/wrangler.toml`:
  ```toml
  [[analytics_engine_datasets]]
  binding = "TELEMETRY"
  dataset = "yfmcp_telemetry"
  ```
- **How writes happen:** `env.TELEMETRY.writeDataPoint(...)` is a synchronous, non-blocking binding call. It is not a fetch, does not count as a subrequest and is never awaited. When the binding is absent (tests, local dev), telemetry is a no-op.

### Workers Logs, for CPU and resource kills only
- **The problem:** an invocation killed for CPU (Error 1102, outcome `exceededCpu` or `exceededResources`) never reaches the end-of-request write, so the outcome is only visible in Workers Logs (already on: `[observability.logs] persist = true`, `invocation_logs = true`).
- **The fix:** emit **one** structured line at dispatch start, so a killed invocation still carries its action:
  `console.log(JSON.stringify({ t: "yfmcp", v: 1, action, domain, build: <short sha> }))`
- **What the line excludes:** arguments, tickers, ids and URLs.
- **Retention:** Workers Logs keeps data for only 3 days on Free, so the export (§4) must pull it daily.

## 2. The data point (one per `/mcp` request)

The point is written once, in `worker/src/index.ts`, after the response is built. That includes the early rejections: 401, 405, 413, 400 batch, 400 invalid request.

**index1 = `action`:** one of
- `tools/call:<tool name as called>` (the grouped domain tool plus the action, e.g. `tools/call:stock_pricing.get_stock_price`);
- the JSON-RPC method (`initialize`, `tools/list`, …);
- for rejections: `rejected:auth`, `rejected:method`, `rejected:body_size`, `rejected:batch`, `rejected:invalid`.

**Blobs, in this order:**

| # | Field | Value |
|---|---|---|
| 1 | `domain` | the grouped tool name, or `-` |
| 2 | `serverVersion` | |
| 3 | `buildShaShort` | 7 characters |
| 4 | `outcome` | one of `ok`, `tool_error`, `rpc_error`, `rejected`, `exception` |
| 5 | `errorCode` | the envelope `error.code`, or the HTTP-level code (`AUTH_REQUIRED`, `PAYLOAD_TOO_LARGE`, `BATCH_LIMIT_EXCEEDED`), or empty |
| 6 | `sizeBucket` | response bytes, one of `<1K`, `1-10K`, `10-100K`, `100K-1M`, `>=1M` |
| 7 | `credentialTransport` | `header`, `path` or `none` |
| 8 | `budgetExhausted` | `1` or `0` (2.11.0 root-scope flag) |
| 9 | `cacheSource` | from `currentCacheSummary`, or empty |
| 10 | `toolMode` | |
| 11 | `timeoutClass` | `none`, `some` or `all`, from the provider timeout counter below |

**Doubles, in this order:**

| # | Field | Value |
|---|---|---|
| 1 | `latencyMs` | wall clock from request start to response built |
| 2 | `responseBytes` | |
| 3 | `httpStatus` | |
| 4 | `upstreamRequests` | root scope |
| 5 | `subrequestsUsed` | 2.11.0 budget counter |
| 6 | `budgetRefusals` | |
| 7 | `providerTimeouts` | new counter, below |
| 8 | `edgeCacheHits` | |
| 9 | `edgeCacheMisses` | |
| 10 | `providerRetries` | new counter, below |

**New counters** (root scope, in `request-context.ts`, next to `upstreamRequests`):
- `providerTimeouts` is incremented where a provider fetch is aborted by its timeout (`fetchProviderWithTimeout` and the other `AbortController`/timeout sites).
- `providerRetries` is incremented at existing retry sites (Yahoo crumb refresh or 429 retry, SEC retry). It counts only; no behaviour changes.

## 3. Privacy rules (Q10, Q13). These are tested.

- **Never record:**
  - the token, the `Authorization` header or the request URL/path (`/mcp/<token>`);
  - request or response bodies, tool arguments, tickers, accession numbers, `raw_text`;
  - any provider content, the caller's IP, or `cf` geo fields.
- **Values are restricted:**
  - `action` and `domain` come only from the server's own tool registry, never from caller strings. An unknown tool name records `tools/call:unknown`. A non-string or unknown JSON-RPC method records `rpc:other`.
  - `errorCode` is taken from the server's own envelope.
- **Test:** for every recorded string, it is in an allow-set or matches `^[A-Za-z0-9_./:<>=-]{0,64}$`. Also, no blob or log line contains the configured `MCP_TOKEN` or any argument value from the request. Use a fake token and fake args.

## 4. Export (baseline capture)

- **`scripts/export_telemetry.py` (new).** It needs `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_TELEMETRY_TOKEN`, a token with **Account Analytics: Read** plus **Workers Observability: Read**. It queries:
  1. **The Analytics Engine SQL API**, hourly per action, for the last 24 h: count, `outcome` counts, p50/p95/p99 `latencyMs`, mean `upstreamRequests` and `subrequestsUsed`, the sum of `budgetRefusals`, `providerTimeouts` and `providerRetries`, the `sizeBucket` histogram, and the error-code counts. Use `_sample_interval` weighting.
  2. **The Workers Observability telemetry query API**, for the last 24 h: invocation outcome counts (`ok`, `exceededCpu`, `exceededResources`, `exception`, …), joined to `action` via the §1 log line. Only `action`/`domain`/`build` and the outcome are read.

  It writes one JSON file: `telemetry/<date>.json` with aggregates only, no raw rows.
- **`.github/workflows/telemetry-export.yml` (new).**
  - Runs daily and on dispatch, with `permissions: contents: read` and SHA-pinned actions (S-09).
  - Runs the script and uploads the JSON as a workflow artifact (90-day retention).
  - Skips cleanly, with a notice, if the secrets are absent.
- **Baseline window:** at least **7 consecutive days** of exports before the first Phase 4 merge. That becomes a Phase 4 entry check in the brief. The live verifier checks the first export.

## 5. Tests

- **`scripts/test_worker_telemetry.py` (new; node bundle, fake `TELEMETRY` binding that records calls, fake env with a token).** It covers:
  - one data point per request for each shape: an OK tool call, a tool error, `tools/list`, 401, 413, batch 400, invalid request;
  - the exact blob and double order, with the expected values;
  - `latencyMs ≥ 0`;
  - `budgetExhausted` set when the guard fired;
  - the privacy assertions from §3;
  - one dispatch-start log line with the §1 shape and no arguments;
  - no binding means no throw and identical responses;
  - the response bodies and headers are byte-identical with and without the binding.
- **Python script tests:** `scripts/test_export_telemetry.py` covers aggregation over canned API responses, with no network.
- **Parity harness:** green and unchanged.
- **CI:** wire both tests into `ci.yml`.

## 6. Owner actions (one-time, at release)

1. Create a Cloudflare API token with **Account Analytics: Read** and **Workers Observability: Read**, scoped to this account. Store it as the GitHub secret `CLOUDFLARE_TELEMETRY_TOKEN`. `CLOUDFLARE_ACCOUNT_ID` already exists as a secret; the deploy workflow uses it. The planner gives the exact steps; the token value never enters the repo or chat.
2. Nothing else. The dataset is created on first write.

## 7. Docs and live verification

- **Docs section "Telemetry (2.11.1)":**
  - what is recorded and what never is;
  - where it lives;
  - the daily export;
  - how to query a single day by hand;
  - the 7-day baseline rule.
- **Live checks:**
  - after deploy, a few connector calls appear in the dataset within minutes (the export script run by hand, or a dispatch of the export workflow, once the owner secret exists);
  - the dispatch log line appears in Workers Logs without arguments;
  - responses are unchanged;
  - the canaries pass.
