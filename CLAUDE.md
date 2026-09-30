# yahoo-finance-mcp

Two runtimes with strict output parity: the Python MCP server (`server.py`, `yfmcp/`) and the Cloudflare Worker (`worker/src/*.ts`). Every behaviour change lands in both, with parity tests, a section in `docs/provider-runtime-guidance.md`, and a version bump.

## Who does what

The main session plans, decides and reviews. Scoped execution goes to the Sonnet subagents in `.claude/agents/`:

- `sec-investigator`: read filings and current tool output before designing a rule.
- `python-mirror`: port a finished TS change to Python and run the parity tests.
- `live-verifier`: check the deployed release against written expectations.
- `release-clerk`: version bump, docs section, local CI, PR body.

Subagents do not see this conversation. Each handoff states the files, the functions, the expected output and the tests to run.

## Decisions that stay with the planner

These are never made during execution. Stop and report them instead:

- a new or changed output field, enum value, warning code or `unresolvedReason`;
- an evidence threshold, coverage guard or fallback order;
- a Python/TS divergence that needs a behaviour choice to resolve.

When running on Sonnet and one of these comes up, switch to Opus (`/model opus`) or hand it to the planner rather than choosing.
