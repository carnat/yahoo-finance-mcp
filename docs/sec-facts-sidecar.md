# SEC Facts Sidecar

The public MCP endpoint remains the Cloudflare Worker. Structured SEC fact
tools call a separate Python sidecar only when `EDGAR_FACTS_URL` is configured.

## Run

```bash
python -m yfmcp.sec_facts_sidecar
```

Environment:

- `PORT`: HTTP port, default `8081`.
- `EDGAR_IDENTITY` or `EDGAR_CONTACT_EMAIL`: SEC identity used by EdgarTools.

Endpoints:

- `GET /health`
- `POST /sec/facts/exposure`

## Worker Configuration

Set these Worker vars/secrets after the sidecar is deployed:

- `EDGAR_FACTS_URL`: base URL for the sidecar, for example `https://sec-facts.example.com`.
- `STRUCTURED_FACT_PROVIDER`: optional; set to `disabled` to force rollback.
- `EDGAR_FACTS_LAST_SMOKE_STATUS`: optional deploy metadata set by automation after the live smoke.

When `EDGAR_FACTS_URL` is absent or disabled, structured fact tools return
`STRUCTURED_FACT_PROVIDER_UNCONFIGURED` instead of using the legacy Worker
heuristics.

## Required Smoke Gate

The blocking smoke is `scripts/test_deployed_sec_facts_sidecar.py`. It calls
the deployed Worker, verifies sidecar diagnostics in `health_check`, then calls:

```text
extract_geographic_revenue(AAPL, Greater China, 10-K, latest)
```

The smoke has no network-skip mode. It must pass before merging or treating a
deployment as active.

## Rollback

Unset `EDGAR_FACTS_URL` or set `STRUCTURED_FACT_PROVIDER=disabled`. The Worker
will keep the public tools callable, but structured fact tools will return
`STRUCTURED_FACT_PROVIDER_UNCONFIGURED` until the sidecar is restored.

