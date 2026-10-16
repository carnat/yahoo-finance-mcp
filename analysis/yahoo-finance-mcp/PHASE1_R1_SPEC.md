# Phase 1, release 1 (2.9.0): credential support in grace mode, plus request limits

Approved under brief v3, §8 (2026-10-16). The owner decisions this release implements:

| Decision | What it settles |
|---|---|
| Q2b | Static credential behind a replaceable auth abstraction |
| Q13 | The credential is accepted as `Authorization: Bearer <token>` or as the path `/mcp/<token>` |
| Q12 / S-03 | A JSON-RPC batch carries at most 1 tool call |
| Q11 | The names `AUTH_REQUIRED` and `BATCH_LIMIT_EXCEEDED` |
| A1 | The grace-check field `health_check.credentialTransport` |

The Worker goes first. Python changes only for the parity field (§5).

Phase 1 ships in three releases:
1. **This release:** credentials are accepted but not yet required (grace mode), plus the batch and body limits.
2. **Enforcement:** a config-only flip of `MCP_AUTH_MODE` to `enforce`. It ships after the owner re-adds the claude.ai connector with the secret URL and the live verifier sees `credentialTransport: "path"`. It also restricts CORS (S-13).
3. **The rest of Phase 1:** the SSRF allow-list, the budget guard, paid-provider opt-in, the evidence-store guard and the supply-chain items.

## 1. Auth abstraction (`worker/src/auth.ts`, new)

Export `resolveCredential(request: Request, pathname: string, env)`. It returns one of:
- `{ route: "mcp", transport: "header" | "path" | "none", status: "valid" | "invalid" | "absent" }`
- `{ route: "other" }`

**How the credential is read:**
- **Path form:** `pathname` is `/mcp/<segment>` with exactly one non-empty segment and no trailing segments. The segment is the presented token.
- **Header form:** `Authorization: Bearer <token>`. The scheme match is case-insensitive, and leading and trailing whitespace is trimmed.
- **Both present:** the header wins. Report `transport: "header"`.

**How it is checked:**
- The presented token is compared with `env.MCP_TOKEN` in constant time. Reuse the existing `timingSafeEqual` in `index.ts`, moving it to `auth.ts` if that is cleaner.
- **`MCP_TOKEN` unset or empty:** any presented credential is `invalid`, and `/mcp/<segment>` returns 404 (so we never reveal that the path form exists). A request with no credential is `absent`.
- **Only the Worker sees tokens.** The token never appears in any response, log line, error message, telemetry or `/health` output. The Worker logs no URLs today; keep it that way. Any future logging must go through a `redactPath()` helper, exported from `auth.ts`, which replaces the segment with `<redacted>`.

## 2. Enforcement mode

`env.MCP_AUTH_MODE` is a plain var, not a secret. Values are `grace` (the default when unset) and `enforce`.

| Credential | grace | enforce |
|---|---|---|
| valid (header or path) | serve | serve |
| absent | serve, `credentialTransport: "none"` | **401** `AUTH_REQUIRED` |
| invalid | **401** `AUTH_REQUIRED` | **401** `AUTH_REQUIRED` |

**The 401 response:**
- HTTP 401, with the `WWW-Authenticate: Bearer realm="yahoo-finance-mcp"` header and the CORS headers.
- Body: `{"jsonrpc":"2.0","id":null,"error":{"code":-32001,"message":"AUTH_REQUIRED: a valid credential is required (Authorization: Bearer <token> or /mcp/<token>).","data":{"code":"AUTH_REQUIRED"}}}`.
- No other information is given, and the message never says whether a token was close.

**Unchanged routes:**
- `/health` and `/` stay unauthenticated, make no provider calls (assert this in a test with a fetch spy) and add no auth fields.
- `/audit/mcp` keeps its own `AUDIT_TOKEN` check.
- `OPTIONS` preflight is answered for `/mcp/<segment>` as for `/mcp`.

## 3. Request limits (S-03, Q12)

- **Body cap:** **1 MiB** (1,048,576 bytes). Check `Content-Length` when present, and the size of the text actually read in every case: read `request.text()`, then `JSON.parse`. A larger body returns HTTP 413 with a JSON-RPC error. The message is "PAYLOAD_TOO_LARGE: request body exceeds 1048576 bytes." and `data.code` is `"PAYLOAD_TOO_LARGE"`.
  - *The planner chose this cap, with headroom for `raw_text` transcripts, which run to a few hundred KB. No new name enters the public contract: this is an HTTP-level error and lists no tool field.*
- **Batch cap:** a JSON-RPC batch array may contain **at most 1 `tools/call`** request.
  - Other messages in a batch are still allowed: `initialize`, `tools/list`, notifications and so on.
  - With more than one `tools/call`, return HTTP 400 with a single JSON-RPC error, `id: null`. The message is "BATCH_LIMIT_EXCEEDED: at most 1 tools/call per request." and `data.code` is `"BATCH_LIMIT_EXCEEDED"`.
  - Nothing is executed in that case: check before dispatching.
  - An empty batch array is an Invalid Request (`-32600`), per the JSON-RPC spec.
- **Order of checks:** auth first, then the body cap, then parsing, then the batch cap. In enforce mode an unauthenticated client therefore learns nothing about limits.

## 4. `health_check` field (A1)

- **Worker:** the MCP `health_check` tool output gains `credentialTransport`: `"header"`, `"path"` or `"none"` for the calling request (only valid or absent credentials reach the tool).
  - Pass it through the existing `withRequestFacts` request context, next to `authorizationHeaderPresent`, which stays unchanged.
  - Schema: `{ type: ["string","null"], enum: ["header","path","none",null] }`.
  - Description: append " credentialTransport reports how the calling request presented its credential (header, path or none), never the credential itself."
- **Python** (stdio has no HTTP credential): `health_check` returns `credentialTransport: null`, with the same schema entry and description in both runtimes (see `authorizationHeaderPresent` for the pattern; there are about 4 places).

## 5. Scripts and CI

- **Callers to update:** every script that posts to the deployed Worker. The brief counted 9: `scripts/live_smoke_utils.py` (`rpc`), `test_deployed_canaries`, `test_deployed_extractors`, `test_geographic_revenue_schema`, `test_triangulation`, `test_universal_aliases` and the rest. Find them all by grepping for `MCP_URL`, `workers.dev` and `/mcp`.
  - Each sends `Authorization: Bearer $MCP_TOKEN` **when the `MCP_TOKEN` env var is set**, and sends nothing otherwise, so grace keeps working.
  - The token is never printed.
- **Deploy workflow:** `.github/workflows/deploy-worker.yml` passes `MCP_TOKEN: ${{ secrets.MCP_TOKEN }}` to the canary and smoke steps' environment. This is a deploy-workflow change, so `release-clerk` makes it in the release PR.

## 6. Tests

New `scripts/test_worker_auth.py` (node via the esbuild bundle, like the other worker tests, calling the Worker's default `fetch` with a fake env). It covers:
- grace mode:
  - absent credential is served;
  - valid header is served;
  - valid path is served;
  - invalid header returns 401;
  - invalid path returns 401;
- `MCP_TOKEN` unset: the path form returns 404;
- enforce mode:
  - absent credential returns 401;
  - valid header or path is served;
- the 401 body and headers, exactly;
- no token text appears in any response body or header;
- the body cap: just under and just over 1 MiB, with and without `Content-Length`;
- the batch cap:
  - 2 `tools/call` return 400 and nothing executes (assert with a dispatch spy);
  - 1 `tools/call` plus notifications is served;
  - an empty array returns -32600;
- `health_check` `credentialTransport` for header, path and none;
- `/health` makes zero fetches.

Add a parity case for `credentialTransport` (Python `null` and the Worker under no request context; mirror the existing `authorizationHeaderPresent` case). Wire the new test into `ci.yml`.

## 7. Release

- **Version:** 2.9.0. The new `credentialTransport` field and the new request limits are a minor change.
- **Docs:** a section covering grace mode, how to configure the connector (path URL), header usage for scripts, the 1 MiB and 1-tool-call limits, and that enforcement follows in the next release.
- **Live verification after deploy:**
  - `/health` works with no auth;
  - `/mcp` with no credential still works (grace);
  - `/mcp/<wrong>` returns 401;
  - `health_check` reports `credentialTransport` "none" through the current connector;
  - a 2-call batch returns 400.
