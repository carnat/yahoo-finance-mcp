# SEC Facts Provider

The public MCP endpoint remains the Cloudflare Worker. Structured SEC fact
tools use the official keyless SEC JSON APIs at `data.sec.gov`.

See also `docs/provider-runtime-guidance.md` for the official SEC and
Cloudflare constraints that govern provider/runtime changes.

Provider endpoints used by the Worker:

- `https://www.sec.gov/files/company_tickers.json`
- `https://data.sec.gov/submissions/CIK##########.json`
- `https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`

No separate Python service, Fly app, paid EdgarTools API, or API key is required.

## Worker Configuration

- `STRUCTURED_FACT_PROVIDER`: default `official_sec_data_api`; set to
  `disabled` to roll back structured fact tools.
- `SEC_USER_AGENT`: optional SEC-compliant User-Agent string. Defaults to a
  project identifier.
- `EDGAR_FACTS_LAST_SMOKE_STATUS`: optional deploy metadata set by automation
  after the live smoke.

When disabled, structured fact tools return
`STRUCTURED_FACT_PROVIDER_UNCONFIGURED` instead of using legacy Worker
heuristics.

## Limits

The official SEC `companyfacts` API is best for standardized entity-level XBRL
facts such as total revenue. It may not expose company-specific geographic or
segment dimensions such as "Greater China" in a normalized way.

SEC's XBRL APIs are designed around non-custom taxonomy facts that apply to the
whole filing entity. Geography, product, customer, and segment disclosures are
often company-specific filing/table problems, so the Worker filing index and
HTML table fallback remain part of the structured exposure path.

Do not add a paid parser, sidecar, or extra provider until a concrete fixture
proves the official SEC JSON plus Worker filing-index path cannot meet the
tool's stated contract.

When a filing and total revenue exist but no matching dimensional fact or table
can be parsed, the Worker returns an explicit non-decision-grade status such as
`PROVIDER_LIMITATION`, `NO_DIMENSIONAL_REVENUE_FACT`, `EXTRACTION_FAILED`, or
`TABLE_NOT_PARSED`. This is intentional: do not collapse provider or parser
limitations into `NOT_DISCLOSED`.

## Required Post-Deploy Smoke

The smoke script is `scripts/test_deployed_sec_facts_provider.py`. It calls the
deployed Worker, verifies
`health_check.structuredFactProvider == "official_sec_data_api"`, then calls:

```text
extract_total_revenue(AAPL, 10-K, latest)
extract_geographic_revenue(AAPL, Greater China, 10-K, latest)
```

The total revenue call must return `FOUND`. The geographic call may return
`FOUND` or an explicit limitation/failure status such as `PROVIDER_LIMITATION`,
`EXTRACTION_FAILED`, `TABLE_NOT_PARSED`, or `NO_DIMENSIONAL_REVENUE_FACT`, but
must not return fake `NOT_DISCLOSED` when the filing exists and the parser or
official SEC JSON cannot expose a dimensional fact.

## Rollback

Set `STRUCTURED_FACT_PROVIDER=disabled` as a Worker secret. The Worker will keep
the public tools callable, but structured fact tools will return
`STRUCTURED_FACT_PROVIDER_UNCONFIGURED`.

## Provider Access Discipline

- Use a declared SEC User-Agent for scripted access.
- Cache repeated submissions/companyfacts/index fetches.
- Avoid retry storms on SEC `429`; pass through explicit rate-limit status.
- Keep broad extraction-quality sweeps out of blocking deploy gates unless the
  response contract itself changed.
