# Preflight — yahoo-finance-mcp

Run: 2026-10-14, branch `claude/laughing-pasteur-c8a1hh` @ `f122d0e` (release 2.5.33).

**Invocation note:** `/modernize-preflight` was run with no system argument and there is no `legacy/` directory. This preflight treats the **whole repository root** as the system under check (`$1 = yahoo-finance-mcp`). Downstream commands expect `legacy/<system>`. Either symlink it (`mkdir -p legacy && ln -s .. legacy/yahoo-finance-mcp`, and add `legacy/` to `.gitignore`), or pass the repo root explicitly.

---

## Check 0 — Human answers (verbatim)

This was a non-interactive cloud run, so nobody answered during the checks. **All five questions are OPEN. The human must fill them in before `/modernize-brief`.**

1. **Scope:** Is `legacy/yahoo-finance-mcp` the complete system, or one slice of a larger codebase? If a slice, what *outside* it depends on code *inside* it, and is breaking those consumers acceptable?
   > **OPEN — unanswered.** (Check 6 found a standalone repository. See below.)

2. **Build & test locally:** Can this environment restore, build, and run the tests? Roughly how long does the full CI pipeline take?
   > **OPEN — unanswered.** (Measured here: yes. The full acceptance suite ran in about 3.5 min locally. See Check 3.)

3. **Bespoke build infrastructure:** Is there organization-specific build or dependency-resolution machinery (an internal package feed, a custom binary store, a code generator, a wrapper around the standard build tool) that someone new to this codebase would not guess? Where is it documented?
   > **OPEN — unanswered.**

4. **Prior attempts:** Has anyone tried to modernize any of this before? What went wrong?
   > **OPEN — unanswered.**

5. **Off limits:** Is anything under `legacy/yahoo-finance-mcp` not allowed to change in this pass (a component another team owns, a frozen branch, generated code)?
   > **OPEN — unanswered.** (Candidates from the source: `worker/src/company-ir-page-registry.json` is rewritten by the `ir-source-discovery` bot workflow. Build-stamped fields in `worker/wrangler.toml` `[vars]` are written by `scripts/stamp_worker_build.py` at deploy time.)

## Check 6 — Scope boundary

**Standalone repository.** The git top level is the repo root. There is no parent repo, workspace or solution above it.

- **Outbound:** none outside the repo. The only `../..` import is `worker/src/tool-catalog.ts` → `tool_catalog.json`, which is inside the repo. CI guards it (`check_tool_sync.py`, `test_worker_grouped_mode.py`).
- **Inbound:** no in-tree consumers. The **external contract** is the deployed MCP surface at `https://yahoo-finance-mcp.artinatw.workers.dev/mcp`, plus the local stdio server. Live MCP clients, including the `Yahoo_Finance_MCP` connector in this session, depend on the tool names, parameters and envelope shape. `tool_catalog.json`, `scripts/public_tool_contract.py` and `scripts/deployed_canaries.json` pin that shape. Any transformation must preserve them, or treat a change as a versioned breaking change.
- **Internal coupling that matters:** the repo holds **two runtimes held to strict output parity** (Python `server.py` + `yfmcp/`, TypeScript `worker/src/`). Per `CLAUDE.md`, any behaviour change lands in both, with parity tests, a docs section and a version bump in four places. A transformation of one side alone breaks the parity suites.

---

## Status table

| # | Check | Status | Found | Fix |
|---|---|---|---|---|
| 0 | Human answers | ⚠️ | All 5 questions open (headless run) | Human fills in the answers above |
| 1 | Stack | ✅ | Python 3.11 MCP server (FastMCP/`mcp[cli]`, `yfinance`) plus a TypeScript Cloudflare Worker (wrangler 4, TS 7). Lines in git: `server.py`+`tool_groups.py` 12.8k; `yfmcp/` 20.3k (39 files); `worker/src` 32.7k (31 files); `scripts/` tests 31.8k (80 files); docs 3.0k | — |
| 2 | Analysis tooling | ⚠️ | `scc`, `cloc`, `lizard`, `glow`, `delta` all missing. `ruff` 0.16.8 and `mypy` 2.3.1 are present | `apt install cloc` (or `go install github.com/boyter/scc/v3@latest`); `pip install lizard`; `apt install glow git-delta` (or the GitHub releases) |
| 3a | Build definition | ✅ | `.github/workflows/ci.yml` (tool sync, `tsc --noEmit`, transcript contract, 65-step acceptance suite) and `deploy-worker.yml` (stamp → `wrangler versions upload` → candidate canaries → promote → production smoke). Pins **Python 3.11**, **Node 24**. Dependencies come from public PyPI (`uv.lock`) and npm (`package-lock.json`). No private feed | — |
| 3b | Smoke test (L1+L2) | ✅ | L1: `py_compile` of all Python passes. L2: `npm ci` + `tsc --noEmit` exit 0; transcript contract passes; a py3.11 venv with `pip install -e . pandas pydantic` imports `server`; **all 65 acceptance steps pass (3m22s)** | Local defaults differ from CI: `python3` is **3.13** (3.11 exists at `/usr/bin/python3.11`) and `node` is **22** (CI uses 24). Run validation under 3.11. Node 22 passed every Worker-driven test |
| 3c | Deploy toolchain | ⚠️ | `wrangler` 4.138.0 is available via `npx`. No `CLOUDFLARE_API_TOKEN` or provider API keys here (expected) | Deploy and live verification only run through CI or the `live-verifier` agent |
| 4 | Source completeness | ✅ | All imports resolve (`import server` ok, `tsc` clean). Config descriptors are present: `wrangler.toml`, `tool_catalog.json`, `tool_groups.py`, registries (`company-ir-page-registry.json`, `news-source-capabilities.json`). Schemas are in `yfmcp/schemas.py` and `worker/src/validate.ts`. Fixtures are in `fixtures/` and `scripts/*.json`. **No binary artifacts** (the two "binary" hits were empty `__init__.py` files) | — |
| 5a | Telemetry | ⚠️ | No APM/observability MCP connected. Worker `observability.logs` are enabled in Cloudflare but not reachable from here. GitHub Actions deployed-smoke summaries are the closest runtime signal | Optional: export Cloudflare Workers logs, or give the live-verifier the production URL |
| 5b | VCS history | ⚠️ | Git is present but the clone is **shallow**: 168 commits visible, 2026-09-25 → 2026-10-08. Hotspots in the last 90 days: `yahoo-finance.ts` (40), `server.py` (42), `capital-structure.ts`/`.py` (26/25), `tools.ts` (27) | `git fetch --unshallow` before `/modernize-assess` for full churn data |
| 6 | Scope boundary | ✅ | Standalone repo. The external MCP contract and the Python↔TS parity are the real boundaries | — |

## Readiness by command

| Command | Verdict | Why |
|---|---|---|
| `assess` + `map` + `extract-rules` | **Ready-with-gaps** | No `scc`/`lizard`, so LOC and complexity fall back to `find`+`wc` and keyword counts. History is shallow, so churn data is truncated. No missing includes |
| `brief` | **Ready-with-gaps** | Needs the three discovery artifacts, and **the five Check 0 answers are open** |
| `transform` + `reimagine` | **Ready (target unspecified)** | Both legacy toolchains are green and the full test suite runs locally, so dual execution is possible. No `[target-stack]` was given; re-run preflight with it to verify the target |
| `harden` | **Ready-with-gaps** | `ruff`/`mypy` are present. No SAST tools (`semgrep`, `bandit`, `pip-audit`, `npm audit` is available via npm). Install: `pip install semgrep bandit pip-audit` |
| `uplift` | **Ready-with-gaps (if a version bump is intended)** | Source runtimes are present (Py 3.11, plus 3.12/3.13 for a Python uplift; Node 22 locally vs 24 in CI). No migration tool (`pyupgrade`, `ruff --select UP` works as a substitute). No inbound in-tree consumers, but every change must keep the Python/TS parity and the public MCP contract |

## Most important fix

Nothing is red. The biggest gap is **Check 0**: the five human answers, especially *prior attempts* and *off limits*, must be filled in before `/modernize-brief`. After that: install `scc` + `lizard` and unshallow the clone before `/modernize-assess`.
