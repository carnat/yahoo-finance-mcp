# Live Smoke Test Policy

Live smoke tests protect the deployed MCP contract. They should prove that the
Worker is reachable, tools are discoverable, responses are JSON envelopes, and
known unsafe failure modes stay explicit.

They should not depend on stale market dates or exact provider data unless the
test first discovers the current valid input.

## Blocking Contract Checks

Keep these as deploy-blocking checks:

- deployed Worker is reachable when `ALLOW_NETWORK_SKIP=0`;
- `tools/list` exposes the expected expanded or grouped surface;
- tool calls return JSON payloads, not opaque platform text;
- inner `ok:false` tool failures are not wrapped as top-level success;
- provider, entitlement, rate-limit, and parser failures use explicit status
  codes instead of fake success data;
- required diagnostic/quarantine metadata is present.

## Non-Blocking Audit Checks

Keep broad live parser and provider-quality sweeps as audit steps. They are
useful signal, but should not block deploy after the core MCP contract is
healthy:

- broad SEC extractor matrices across many tickers;
- exact geographic/parser quality expectations beyond the stable AAOI positive
  fixture;
- end-to-end all-tool live probes;
- grouped-mode live smokes unless the deployed Worker is actually configured
  with grouped discovery.

## Volatile Data Rules

- Do not hardcode option expiration dates in live smokes. Resolve current
  expirations with `scripts/live_smoke_utils.py`.
- Do not hardcode latest filing accession numbers unless the test uses a
  recorded fixture. Live filing smokes should resolve current filings first.
- Do not assert exact live prices, volumes, news counts, SEC extraction values,
  or provider freshness unless that exact value is the feature under test.
- SEC/Yahoo/provider unavailability may be tolerated only when the response is
  still a standard JSON envelope with an explicit status or error code.

## When A Tool Changes

Any PR that changes a public tool, schema, envelope, provider, fallback, or
diagnostic field should update the relevant smoke expectation in the same PR:

- `scripts/deployed_canaries.json`
- `scripts/test_deployed_canaries.py`
- `scripts/test_deployed_discovery.py`
- `scripts/test_deployed_extractors.py`
- `scripts/test_deployed_sec_facts_provider.py`
- `scripts/test_tools.py`
- `scripts/test_live_discovery.py`

If a live data-quality assertion is too brittle, move the exact assertion to a
fixture/unit test and keep the deploy smoke focused on the truthful runtime
contract.

Provider-facing changes should cite the current official provider docs in the
PR description or update the relevant docs in-repo. Community repos are useful
implementation references, but they should not define the deployed contract by
themselves.
