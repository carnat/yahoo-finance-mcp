# SEC Facts Provider

The public MCP endpoint remains the Cloudflare Worker. Structured SEC fact
tools use the official keyless SEC JSON APIs at `data.sec.gov`.

Provider endpoints used by the Worker:

- `https://www.sec.gov/files/company_tickers.json`
- `https://data.sec.gov/submissions/CIK##########.json`
- `https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`

No Python sidecar, Fly app, paid EdgarTools API, or API key is required.

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

When a filing and total revenue exist but no matching dimensional fact is
available from official SEC JSON, the Worker returns `PROVIDER_LIMITATION`.
This is intentional: do not collapse provider limitations into
`NOT_DISCLOSED`.

## Required Post-Deploy Smoke

The smoke script is `scripts/test_deployed_sec_facts_sidecar.py` for continuity
with earlier PR wiring. It calls the deployed Worker, verifies
`health_check.structuredFactProvider == "official_sec_data_api"`, then calls:

```text
extract_total_revenue(AAPL, 10-K, latest)
extract_geographic_revenue(AAPL, Greater China, 10-K, latest)
```

The total revenue call must return `FOUND`. The geographic call may return
`FOUND` or `PROVIDER_LIMITATION`, but must not return fake `NOT_DISCLOSED`
when official SEC JSON cannot expose a dimensional fact.

## Rollback

Set `STRUCTURED_FACT_PROVIDER=disabled` as a Worker secret. The Worker will keep
the public tools callable, but structured fact tools will return
`STRUCTURED_FACT_PROVIDER_UNCONFIGURED`.
