# Phase 1, release 3b (2.10.0): fetch allow-list, CORS, evidence-store guard

This release implements these owner decisions, approved under brief v3, §8:

| Decision | What it settles |
|---|---|
| Q2a | Fetch allow-list |
| S-13 | CORS restriction, now that auth exists |
| S-05 | Evidence-store `list()` guard |

V2 was acknowledged on 2026-10-16: with 0 `approved` IR registry entries, company-IR URL fetches are refused.

**Inputs:** the planner's release-3 inventory, summarized in §1. Release 3 is split into three releases:
- **3a (#282):** CI supply chain;
- **3b:** this document;
- **3c:** subrequest budget guard and paid-provider opt-in.

**Order:** the Worker goes first, then `python-mirror` ports it, then the parity tests, docs, `release-clerk` and `live-verifier`.

**Version: 2.10.0** (minor). Caller URLs that worked before are now refused, and the Worker's `company_ir` stops discovering pages on the open web. No new output field, enum value or error code is added: refusals reuse `INPUT_VALIDATION_ERROR`, and source statuses reuse `NOT_REGISTERED`.

## 1. Surfaces (from the inventory)

| Surface | Worker | Python | Today |
|---|---|---|---|
| `parse_public_transcript.url` | `parsePublicTranscript` (yahoo-finance.ts ~9837), then `fetchPublicHtml` | `earnings.py` ~1034, then `_fetch_page` ~1045 | Only checks that the URL starts with `https://`. Fetches any host and follows redirects. |
| `index_earnings_release.source_url` and resolved release URLs | `classifyEarningsSourceUrl` (~13362), then `fetchPublicHtml` (~13378) | `_classify_earnings_source_url` (~116), then `_fetch_public_html` (~129) | Checks https and a paywall denylist. Any other host is treated as `company_ir` and fetched. |
| `company_ir` news and event source | `fetchCompanyIrText` (news.ts ~947): discovery probes on the Yahoo-profile website and its investor/ir variants, plus up to 8 linked feeds on any host | n/a (Python `company_ir` reuses yfinance news) | Fetches arbitrary hosts. The final URL is not re-checked. |
| Approved IR registry page | `fetchApprovedCompanyIrPage` (news.ts ~967) | `_fetch_company_ir_page` (news.py ~957) | Already checks the host and path, and re-checks the final URL. |
| `document_url` (6 SEC filing tools) | inline `startsWith("https://www.sec.gov/Archives/")` | `_validate_sec_url` | Prefix check only. Redirects are followed and not re-checked. |
| All other provider fetches | constant hosts, through `providerFetch` (request-context.ts ~49) | constant hosts, through urllib | Redirects are followed without checks. |

## 2. URL policy module (new; the same rules in both runtimes)

- **Worker:** `worker/src/url-policy.ts`, exporting `checkFetchUrl(url: string): { ok: true; url: URL } | { ok: false; reason: string }`.
- **Python:** `yfmcp/url_policy.py`, exporting `check_fetch_url(url) -> tuple[bool, str]`. The reason strings are identical in both runtimes.

A URL is allowed only if **all** of the following hold. The listed reason is returned for the first rule that fails.

| # | Rule | Reason |
|---|---|---|
| 1 | Parses as an absolute URL | `invalid URL` |
| 2 | Scheme is `https:` | `URL must use https` |
| 3 | No username or password | `URL must not contain credentials` |
| 4 | Port is empty or 443 | `URL port not allowed` |
| 5 | Hostname is not an IP literal (dotted IPv4, any `[…]` IPv6, or an integer or hex IPv4 form) and is not `localhost`. It does not end in `.localhost`, `.local`, `.internal` or `.invalid`. | `URL host not allowed` |
| 6 | Hostname, lowercased with any trailing dot stripped, is **exactly** in the allow-list | `URL host not allowed` |

**The allow-list.** Exact hosts, no wildcards:
- SEC: `www.sec.gov`, `data.sec.gov`, `efts.sec.gov`.
- Yahoo: `finance.yahoo.com`, `query1.finance.yahoo.com`, `query2.finance.yahoo.com`, `fc.yahoo.com`.
- News and providers:
  - `www.globenewswire.com`;
  - `finnhub.io`, `www.alphavantage.co`, `api.marketaux.com`;
  - `api.sec.or.th`, `api.company-information.service.gov.uk`.
- Registry: every host in `allowedHosts` of a registry entry whose `status` is `approved`, computed at module load from `company-ir-page-registry.json` (Python: the same file through its existing loader). There are 0 such entries today.

**Path rules for caller-supplied URLs only.** Applied in §4, on top of the host check:
- an SEC URL must start with `https://www.sec.gov/Archives/`;
- a Yahoo URL must pass the existing transcript URL parser;
- a registry-host URL must pass `companyIrPageUrlAllowed` (that entry's path prefix).

DNS resolution is not checked: Workers cannot resolve names. The exact-host allow-list makes a DNS-based bypass impossible except through a host we already trust.

## 3. Redirects: re-validate every hop

- **Worker:** `providerFetch` becomes the single choke point.
  - It calls `fetch(url, { ...init, redirect: "manual" })`.
  - On a 301, 302, 303, 307 or 308 response, it resolves `Location` against the current URL and runs `checkFetchUrl` on it. If the check fails, it stops and throws `UrlPolicyError(reason)`.
  - It follows at most **5 hops**. Each hop counts as an upstream request in the scope, as today.
  - A 303 switches to GET.
  - Explicit `redirect: "follow"` arguments (yahoo-finance.ts ~705, ~2993) are dropped, because the wrapper now follows.
  - `checkFetchUrl` also runs on the initial URL. Constant hosts pass by construction, and this catches regressions.
- **Python:** one `HTTPRedirectHandler` subclass in `yfmcp/url_policy.py`, whose `redirect_request` runs `check_fetch_url` and raises `UrlPolicyError`.
  - It is installed in every opener used for the surfaces in §1 and for the SEC, provider and news urllib fetches listed in the inventory.
  - yfinance (curl_cffi) is out of scope. It only talks to Yahoo hosts it chooses itself.
- **How callers treat `UrlPolicyError`:** a refused hop is a fetch failure for that source.
  - For caller URLs (§4) it is reported as the tool error below.
  - For provider and source fetches it maps to the source's existing failure status.

## 4. Caller-supplied URLs

### `parse_public_transcript`

- When `raw_text` is non-empty, nothing changes: it is never fetched, even if it contains URLs. Add a test for this.
- Otherwise, run `checkFetchUrl(url)` plus the caller path rule (§2). On refusal return:
  `{ ok: false, error: { code: "INPUT_VALIDATION_ERROR", message: "url is not on the fetch allow-list (<reason>). Allowed: SEC Archives, finance.yahoo.com transcript pages, and approved company IR pages. Paste the text as raw_text instead." } }`
- The envelope shape is unchanged in both runtimes (Worker inline JSON, Python `_wrap_envelope_v2`).

### `index_earnings_release`

- `classifyEarningsSourceUrl` keeps its first three checks (non-empty, valid URL, https), then calls `checkFetchUrl` and the path rule.
- **Mapping:**
  - SEC Archives → `sec_8k`, as today.
  - An approved registry host → `company_ir`.
  - Anything else → `error: "source_url is not on the fetch allow-list (<reason>)"`, through the tool's existing error path.
- The paywall denylist stays as is. It is now redundant, because those hosts aren't on the list, but its message remains for those hosts.
- Release URLs resolved from SEC search go through the same check before `fetchPublicHtml`. A refused resolved URL is treated as a failed fetch, the existing path.

### `document_url` (6 SEC filing tools)

The prefix checks stay as they are. Redirect re-validation (§3) covers the rest. Only the error text changes; codes and paths stay.

## 5. Worker `company_ir` news source: approved registry only

- **Remove open-web discovery from the request path.** `fetchCompanyIrText` discovery probes and linked-feed fetches are no longer called.
- **The `company_ir` source uses only the approved-registry path** (`fetchApprovedCompanyIrPage`).
- **When no approved entry exists for the ticker,** the source reports status `NOT_REGISTERED`, an existing value already in the failed-status set (news.ts ~388). Coverage gets the same `skippedSources` row that an unregistered company produces today.
- **`COMPANY_IR_DISCOVERY_BUDGET_EXHAUSTED` can no longer occur.** Keep the code in the enums, because it is a frozen contract value.
- **Delete the discovery functions only if nothing else calls them.** Otherwise leave them unreferenced and list them in the report.
- **Parity:** Python `company_ir` is unchanged (yfinance news).
  - Any parity case that pinned the Worker's discovery behaviour as a `FINDING` (BR-NWS-107, `company_ir` meaning) is updated to the new Worker output. Each such edit is listed in the report.
  - This is a deliberate behaviour change, not a fix-by-accident.

## 6. CORS (S-13)

- **`/mcp` and `/mcp/<token>`:** no `Access-Control-Allow-Origin` unless the request `Origin` is exactly `https://claude.ai`. In that case, reflect it, add `Vary: Origin`, and include `Authorization` in `Access-Control-Allow-Headers`.
  - Server-side MCP clients (the claude.ai connector backend, scripts, CI) send no `Origin` and are unaffected.
  - A browser on any other origin can no longer read responses.
- **`OPTIONS` on `/mcp*`:** 204, with the headers above only for the allowed origin.
- **`/`, `/health`:** keep `Access-Control-Allow-Origin: *`. They are public and make no provider calls.
- **`/audit/mcp`:** no CORS headers.
- **Unknown-path 404:** unchanged (no CORS).
- **Rule for the code:** one helper, `corsHeadersFor(request, route)`, replaces the global `CORS_HEADERS` spread.

## 7. Evidence-store `list()` guard (S-05)

- **Worker:**
  - `readConsensusObservation`, `getEpsRevisions` and `listEvidenceCuts` validate the symbol with the existing ticker validator before building the prefix. An invalid ticker returns the tool's existing invalid-ticker error.
  - `evidence-store.ts` `list()` rejects a prefix that is empty, does not end in `/`, or contains `..`, `//` or a backslash.
  - The hard key cap stays at 1000.
- **Python:**
  - `LocalDirStore.list` validates the prefix with the same rules as `_path` (`_KEY_RE`, no `..`). It resolves the path and confirms it stays under `root`; otherwise it raises `ValueError`. `MemoryStore.list` applies the same prefix check.
  - `get_eps_revisions` and `list_evidence_cuts` call `_validate_ticker` first (the consensus path at server.py ~8909 too).
  - Error envelopes match the Worker's.

## 8. Tests

| Test | What it covers |
|---|---|
| `scripts/test_url_policy.py` (new; both runtimes, table-driven, same cases) | Every rule in §2. IPv4, IPv6, decimal and hex IP literals; `localhost`; `.internal`; a port; credentials; http; a trailing-dot host; an uppercase host; an allowed host. |
| Worker redirect tests (fake fetch) | An allowed-to-allowed hop follows. An allowed-to-disallowed hop is refused. An allowed-to-IP-literal hop is refused. A 6-hop chain is refused. A 303 becomes GET. Each hop counts as one upstream request. |
| Python redirect tests | The handler refuses a disallowed `Location`. |
| SSRF exit criteria (brief §Phase 1) | An http redirect. A non-allow-listed host. Loopback. Link-local (`169.254.169.254`). An IP literal. A `raw_text` containing a URL is not fetched (fetch spy shows 0). |
| `parse_public_transcript` and `index_earnings_release` | The refusal envelopes. Parity cases for the refusal in both runtimes, mode equal. |
| `company_ir` | With 0 approved entries the source is `NOT_REGISTERED`, and a fetch spy records zero non-allow-listed hosts during `get_company_news` defaults. |
| CORS | No `Origin`. `Origin: https://claude.ai`. `Origin: https://evil.example`. `OPTIONS` for each. `/health` keeps `*`. |
| Evidence | An invalid ticker is rejected before `list()`. Prefix rules apply in both stores. Python path containment. |
| CI | Wire `test_url_policy.py` into `ci.yml`. |

## 9. Release and live verification

- **Docs section "Fetch Allow-list, CORS and Evidence Guard (2.10.0)":**
  - the allow-list;
  - how to get a company IR site added (registry entry with status `approved`);
  - `raw_text` as the escape hatch;
  - the CORS rule.
- **Live checks after deploy:**
  - `parse_public_transcript` with `https://example.com/` is refused with `INPUT_VALIDATION_ERROR`;
  - `raw_text` still works;
  - an SEC Archives `document_url` still works;
  - `get_earnings_call_transcript` for AAPL still returns;
  - `get_company_news` for MSFT reports `company_ir` `NOT_REGISTERED` and still returns Yahoo/SEC items;
  - `curl` with `Origin: https://evil.example` gets no ACAO header;
  - the connector `health_check` works.

## 10. Release 3c (outline; it gets its own spec after 3b)

- **Per-invocation subrequest budget:**
  - `providerFetch` refuses new upstream work once the scope reaches **45** (Free limit 50, with 5 kept in reserve), throwing `BudgetExhaustedError`.
  - Sources starved this way report `PROVIDER_BUDGET_EXHAUSTED` (Q11 name), never `PROVIDER_ERROR`, and coverage states the budget cause.
  - Cache API calls count too: count them in `edgeCache` wrappers.
- **Paid-provider opt-in (Q2b):**
  - Finnhub, Alpha Vantage and Marketaux leave every default source list and waterfall.
  - An explicit request (for example `sources` naming the provider) opts in.
  - A provider skipped for lack of opt-in reports `PROVIDER_OPT_IN_REQUIRED` in `skippedSources`.
  - **Open planner question for 3c:** tools with no `sources` parameter, such as the consensus tools (Alpha Vantage estimates) and the transcript waterfall, have no way to opt in. A new input field is a contract change, so 3c will propose one name to the owner rather than choose it during execution.
