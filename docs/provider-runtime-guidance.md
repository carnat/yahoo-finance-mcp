# Provider And Runtime Guidance

This project is Worker-first for the public MCP endpoint. Before adding a data
provider, parser dependency, cache path, or deployment path, check the current
official provider/runtime docs and record the consequence in the PR.

Sources checked on 2026-07-09:

- SEC EDGAR APIs: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
- SEC fair access guidance: https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data
- Cloudflare remote MCP server guide: https://developers.cloudflare.com/agents/model-context-protocol/guides/remote-mcp-server/
- Cloudflare Workers limits: https://developers.cloudflare.com/workers/platform/limits/

## SEC EDGAR Rules

- `data.sec.gov` is keyless and public. Do not add API-key or paid-provider
  requirements for structured SEC facts unless a concrete fixture proves the
  official path cannot support the needed contract.
- SEC `companyfacts` and `companyconcept` APIs aggregate standardized
  non-custom taxonomy facts that apply to the whole filing entity. Use them for
  total revenue and other comparable entity-level facts.
- Do not expect official XBRL JSON alone to solve company-specific geography,
  product, customer, or segment tables. Use the Worker filing index / HTML table
  fallback for those cases, and return explicit limitation statuses when parsing
  fails.
- Do not collapse provider or parser limitations into clean `NOT_DISCLOSED`.
  Clean `NOT_DISCLOSED` requires filing metadata, scan coverage, searched terms,
  a non-disclosure basis, and no `TABLE_NOT_PARSED` warning.
- Use a declared SEC User-Agent for scripted access. Keep request patterns
  efficient and cache repeated submissions/companyfacts/index fetches.
- `data.sec.gov` does not support CORS. Browser/dashboard features should call
  the MCP/backend surface, not fetch SEC APIs directly from client JavaScript.
- Treat SEC fair-access guidance as a hard design constraint. The public
  guidance currently lists a maximum request rate of 10 requests/second; code
  should stay comfortably below that and avoid retry storms on `429`.
- For broad or repeated SEC data sweeps, prefer cached data or SEC bulk archives
  over repeated live per-company API calls.

## Cloudflare Worker Rules

- Keep the public MCP endpoint stateless unless a feature truly needs per-user
  session state. Cloudflare's Remote MCP guidance lists `createMcpHandler()` as
  the simplest fit for stateless tools; Durable Object-backed `McpAgent` is for
  stateful/session use.
- Do not add sidecars or alternate public deployment paths by default. Add them
  only when Worker limits or provider rules block a verified, decision-grade
  contract.
- Design SEC parsing for Worker limits: avoid large in-memory DOMs, avoid broad
  fanout in one tool call, and do not assume many simultaneous upstream fetches.
- Treat Cloudflare limits as runtime design inputs, especially CPU time, 128 MB
  memory, subrequest limits, and 6 simultaneous outgoing connections/request.
- Prefer compact contract checks in deploy canaries. Keep broad parser-quality
  sweeps as audit jobs unless the response contract itself is at risk.

## PR Preflight

Provider/runtime PRs should answer these before implementation:

- Which official provider/runtime docs were checked?
- Does the change alter public MCP tool names, schemas, response envelopes, or
  diagnostic fields?
- Does it increase live SEC or Yahoo request volume?
- What is cached, for how long, and what happens on rate limit or provider
  outage?
- Which blocking canary or audit smoke would catch a broken deploy?
