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

## Deploy Target

This repo uses Fly.io for the first sidecar deployment target. The sidecar is a
plain Docker service described by `Dockerfile.sec-facts` and
`fly.sec-facts.toml`.

Required GitHub settings:

- Secret `FLY_API_TOKEN`: Fly deploy token.
- Secret `EDGAR_IDENTITY`: SEC Edgar identity string, usually a contact email.
- Variable `SEC_FACTS_FLY_APP`: Fly app name. Defaults to `yahoo-finance-sec-facts`.
- Variable `EDGAR_FACTS_URL`: deployed sidecar base URL, for example
  `https://yahoo-finance-sec-facts.fly.dev`.
- Variable `STRUCTURED_FACT_PROVIDER`: optional. Use `edgartools_sidecar` when active.
- Variable `SEC_FACTS_GATE_MCP_URL`: Worker MCP URL used by the hard PR gate.

Deploy order:

1. Create the Fly app once if it does not exist:

   ```bash
   flyctl apps create yahoo-finance-sec-facts
   ```

2. Run the `Deploy SEC Facts Sidecar` workflow.
3. Set repository variable `EDGAR_FACTS_URL` to the deployed sidecar URL.
4. Run the `Deploy Cloudflare Worker` workflow so it wires `EDGAR_FACTS_URL`
   into the Worker.
5. Rerun CI. The `Live SEC facts sidecar smoke` check must pass before merge.

Pre-merge note: GitHub only exposes newly added `workflow_dispatch` workflows
from the default branch in many repository setups. Before this workflow exists
on `main`, deploy the sidecar from this branch with:

```bash
flyctl deploy --remote-only --config fly.sec-facts.toml --app yahoo-finance-sec-facts
```

Then deploy this PR branch's Worker once with `EDGAR_FACTS_URL` wired through
`wrangler secret put EDGAR_FACTS_URL`, and rerun the hard smoke. That is the
only way for the PR gate to prove the branch before the workflow itself has
landed on `main`.

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
