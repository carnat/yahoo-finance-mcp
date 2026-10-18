# Phase 1b (2.18.0): per-person access keys (Worker only)

**Approval:** owner, 2026-10-18 ("1b approved"). The names and the sharing constraints were accepted as listed in the brief's Phase 1b section.
**Version:** 2.18.0 (minor). It adds a new error code, `KEY_RATE_LIMITED`. The local Python server is stdio and single-user, so it is unchanged and parity is unaffected.
**Rule zero:** the owner's existing `MCP_TOKEN` behaves exactly as today, in both header and path form. Every tool output is unchanged.

## 1. Storage

### Keys
- **Where:** the KV namespace `yfmcp_access_keys`, binding `ACCESS_KEYS`.
- **Record:** KV key `key:<sha256 hex of the full key>`. The JSON value is:
  `{ "id": "<first 8 hex of the hash>", "label": "<non-secret name>", "createdAt": "<ISO>", "status": "active"|"revoked", "dailyLimit": <int>, "perMinuteLimit": <int>, "paidProviders": <bool> }`
- **Never stored:** the full key exists only in the issuing script's output, shown once.
- **Key format:** `yfk_` followed by 43 base64url characters (32 random bytes).
- **Defaults:** `dailyLimit` 2000, `perMinuteLimit` 30, `paidProviders` false. The docs tell the owner to keep the sum of daily limits well under the shared Free cap of 100,000 per day.

### Usage counters
- **Planner decision:** a Durable Object class `KeyUsage` on the SQLite backend, which the Free plan supports, with one instance per key id.
- **Why not KV:** KV is eventually consistent, and its Free write quota is about 1,000 writes per day, so it cannot count requests.
- **What the instance does:** it holds a per-minute window and a per-UTC-day counter. One call, `check-and-increment`, returns `{allowed, retryAfterSeconds}`.
- **Owner key:** never calls it.
- **Binding:** `USAGE` (class `KeyUsage`) in `wrangler.toml`, with the `new_sqlite_classes` migration.
- **Abstraction:** both the store and the counter sit behind small interfaces (`AccessKeyStore`, `UsageCounter`). Tests inject in-memory versions, following the `setEvidenceStoreForTests` pattern.

## 2. Request flow (`worker/src/auth.ts`, `worker/src/index.ts`)

1. **Read the credential** exactly as today: header first, then path; the transport is recorded as now.
2. **Owner check:** if the credential matches `MCP_TOKEN` (constant-time comparison), it is the owner. There are no limits and no KV or Durable Object call.
3. **Personal key:** otherwise, if the credential has the `yfk_` shape and the `ACCESS_KEYS` binding exists:
   - Hash it with SHA-256 (Web Crypto) and look up `key:<hash>`. Cache positive and negative results in the isolate for 60 seconds, and never log the key.
   - No record, or a record with `status` other than `active`, gives the existing `AUTH_REQUIRED` 401, with an identical response.
   - An active record calls `USAGE.checkAndIncrement(id, perMinuteLimit, dailyLimit)`.
     - Not allowed gives HTTP **429** with a `Retry-After` header and the body `{ "jsonrpc": "2.0", "id": null, "error": { "code": -32029, "message": "Key rate limit reached.", "data": { "code": "KEY_RATE_LIMITED", "retryAfterSeconds": N } } }`.
     - Mirror the shape of the existing 401 body (`AUTH_REQUIRED`); if that body differs, use it as the template and STOP to report the difference.
4. **Anything else:** the existing 401.
5. **No binding:** when `ACCESS_KEYS` is absent, only the owner key works, exactly as today. This is the deploy-safe default until the owner creates the namespace.

## 3. Per-key provider access

- **`paidProviders` false:** for that request, the key-gated provider credentials read the same as unconfigured. These are the Finnhub, Marketaux and Alpha Vantage key names that `getWorkerVar` reads, so every existing "unconfigured provider" path applies unchanged and no new output appears.
- **How:** a request-scoped flag in `request-context.ts`, checked by one wrapper around those env reads. Do not scatter the check.
- **Owner key:** always has provider access.

## 4. Telemetry (Q10)

- **New blob 12 `keyId`:**
  - `owner` for the owner key;
  - the record's 8-character id for a personal key;
  - `-` for rejected or anonymous requests.

  It is never the key or the label.
- **New rejection action:** `rejected:key_limit`, with `errorCode` `KEY_RATE_LIMITED`.
- **Export:** update `scripts/export_telemetry.py` to aggregate requests by `keyId`.
- **Privacy tests:** extend them so that no blob or log line contains a key or a label.

## 5. Admin script `scripts/manage_access_keys.py`

- **Commands:**
  - `issue --label NAME [--daily N] [--per-minute N] [--paid-providers]`: prints the new key once, with a reminder to share it privately, and writes the record.
  - `list`: shows id, label, status, limits and createdAt. It never shows hashes or keys.
  - `revoke --id ID`.
  - `set-limits --id ID [--daily N] [--per-minute N] [--paid-providers/--no-paid-providers]`.
- **Implementation:** it calls `npx wrangler kv key put|get|list` with `--binding ACCESS_KEYS --remote` from `worker/`, through subprocess with argument lists and no shell. To find a record by id it lists `key:` keys and reads each one.
- **Never logged:** no key and no hash in any error or exception message.
- **Tests:** `scripts/test_manage_access_keys.py` with a fake wrangler runner, covering argument building, the key format, one-time printing and the non-leaking error paths.

## 6. Owner action (once, at deploy)

1. Create the namespace: Cloudflare dashboard → Storage & Databases → KV → Create → `yfmcp_access_keys`.
2. Give the planner its **namespace ID**. The ID is not a secret.
3. The planner adds `[[kv_namespaces]] binding = "ACCESS_KEYS"  id = "<ID>"` to `wrangler.toml` in a small follow-up commit.

Until step 3 lands, the release ships with the code inactive. The Durable Object binding needs no owner action.

## 7. Tests (`scripts/test_worker_access_keys.py`, node bundle)

- The owner key works with no store or counter call (spies).
- A valid personal key works in header and path form.
- An unknown key and a revoked key each get the same 401 as a wrong token.
- A key over its per-minute limit gets 429 `KEY_RATE_LIMITED` with `Retry-After`; over its daily limit, the same.
- Revoking one key leaves another working.
- With `paidProviders` false, a Finnhub-using call reports Finnhub as unconfigured. With true, it is attempted.
- Without the binding, only the owner key works.
- The 60-second isolate cache serves repeat lookups.
- Telemetry carries `keyId` and never the key or label.
- Existing auth, transport and telemetry tests stay green.
- The parity harness is unchanged.

## 8. Docs

Section "Per-Person Access Keys (2.18.0)":
- how to issue, list, rotate (issue a new key, then revoke the old) and revoke keys;
- the limits;
- what a user must not do: no redistribution, non-commercial and personal use only, and the daily capacity is shared;
- the telemetry key id;
- the owner action in §6.

## 9. Decisions taken during execution (2026-10-18)

1. **Applying the Durable Object migration (owner: "One-time workflow").**
   - **Why:** the release pipeline only runs `wrangler versions upload` / `versions deploy`, and those never apply Durable Object migrations. Cloudflare requires `wrangler deploy` for that.
   - **Two steps:**
     - **2.18.0 (this release):**
       - The `KeyUsage` class is exported and all 1b code ships.
       - `wrangler.toml` is unchanged: no `USAGE` binding, no migration, no KV binding.
       - Personal keys need both `ACCESS_KEYS` and `USAGE`. With either missing, only the owner key works.
       - The normal pipeline deploys it as usual.
     - **Activation (follow-up PR, once the owner gives the KV namespace ID):**
       - Add the `[[kv_namespaces]]` `ACCESS_KEYS` entry, the `[[durable_objects.bindings]]` `USAGE` entry and the `new_sqlite_classes` migration.
       - Add `.github/workflows/apply-do-migration.yml`: `workflow_dispatch` only, input `ref`. It checks out that ref, runs `npx wrangler deploy`, then the production canaries and the live discovery audit, with the same secrets and SHA-pinned actions as the deploy workflow.
       - **Order:**
         1. The PR is green.
         2. The planner dispatches the migration workflow on the PR's head ref.
         3. It succeeds with canaries green.
         4. Merge. The normal pipeline then re-deploys the same code through `versions upload`.
       - `deploy-worker.yml` and `test_smoke_architecture.py` are unchanged, and a test asserts the new workflow is dispatch-only.
2. **Cached provider data:** a key without `paidProviders` may be served provider data already in cache. The flag protects the shared free quotas, and a cache hit spends no quota. Documented; no change to the cache sites.
3. **Telemetry `keyId`:** once a personal key is identified, its id is recorded on every outcome for that request, including 429 and any later rejection. `-` only when no key is identified: anonymous requests, 401, or rejections before auth.
4. **The counter's subrequest** comes from the 5-request reserve outside the 45 budget. Documented.
5. **Failures:** if KV or the Durable Object throws, or a record is malformed, the request fails closed through the existing exception path: status 500, telemetry outcome `exception`. It never fails open. The owner key is unaffected.
6. **Path form with `MCP_TOKEN` unset:** stays 404 as today. Production always sets `MCP_TOKEN`.
