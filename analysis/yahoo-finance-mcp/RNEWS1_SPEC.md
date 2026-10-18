# R-news part 1 (2.15.1): news provider adapters (pure restructure)

**Source:** the approved Q3i triage, §3a part 1 (owner, 2026-10-18).
**Version:** 2.15.1 (patch). There is no behaviour change; the patch exists so the deploy carries a new build identity.

**Rule zero: every output is byte-identical.** That covers every news tool in both runtimes, in expanded and grouped mode, including warnings, ordering, counts and timestamps.
- **Proof:** the parity harness is unchanged and green with no case edited, and the news test scripts pass.
- **Required before/after dump:** capture `tools/list` for both runtimes and a fixed set of offline news calls (the news parity cases' outputs serialised) at the base commit and after the change. Their diff must be empty.

## 1. Target layout

### Worker
```
worker/src/news/providers/yahoo.ts            collectYahooEvents + getNews + Yahoo-only helpers
worker/src/news/providers/sec.ts              collectSecEvents + SEC-news helpers
worker/src/news/providers/company-ir.ts       collectCompanyIrEvents (registry-only company_ir)
worker/src/news/providers/company-ir-page.ts  collectCompanyIrPageEvents + IR-page adapters
worker/src/news/providers/globenewswire.ts    collectGlobeNewswireEvents + feed guard
worker/src/news/providers/finnhub.ts          collectFinnhubEvents + Finnhub fetch
worker/src/news/providers/marketaux.ts        collectMarketauxEvents + Marketaux fetch
worker/src/news/providers/types.ts            the shared adapter result type (see §2)
worker/src/news.ts                            the shared pipeline (identity, dedupe, event typing, ranking, coverage,
                                              collection status, public tool functions); re-exports every symbol
                                              that other modules or tests import today
```

### Python
```
yfmcp/news/__init__.py
yfmcp/news/providers/{yahoo,sec,company_ir,company_ir_page,globenewswire,finnhub,marketaux}.py
yfmcp/tools/news.py                           shared pipeline and the tool functions; re-imports the provider functions
                                              under their current names
```

**Shared helpers:** helpers used by more than one provider and by the pipeline go into one shared module per runtime, `worker/src/news/shared.ts` and `yfmcp/news/shared.py`. Examples are exchange normalisation, text cutting, ISO formatting, identity matching and the provider HTTP helpers. Providers import the shared module; the pipeline imports providers and the shared module. **No cycles:** providers never import `news.ts` or `tools/news.py`.

## 2. Adapter contract

**Today's behaviour, kept:** each `collect<Source>Events` keeps its current signature and return value. Today these are items plus the per-source status/diagnostics, and warnings, appended to the caller's lists.
- **Worker:** add `types.ts` with a `NewsProviderResult` type that describes that existing shape. Annotate the collectors with it, and change no shape.
- **Python:** add a docstring describing the same contract.
- **No registry yet:** the registry and the off switch are part 2.

## 3. Compatibility

- **Names keep resolving:**
  - Every public or test-used name keeps resolving from where it is imported today: `worker/src/news.ts` exports, and `yfmcp.tools.news` attributes, including `server.*` aliases.
  - Tests that `patch()` a name the pipeline calls must still affect the call. Where a patch target now lives in a provider module, update the test's patch target to that module.
  - A patch must not silently stop applying. Add an assertion or check the harness case that relies on it.
- **Source-text helpers:** `scripts/source_text.py` already covers every module, and the direct-read guard must stay green.
- **Bundle entry points:** the parity harness bundle and esbuild test entry points that import from `./src/news.ts` keep working through the re-exports.

## 4. Work split

1. **Worker first:** the TS move, the harness green, and the before/after dump for the Worker.
2. **Python second:** the Python move, the harness green, and the before/after dump for Python.
3. **Release:** the docs section "News Provider Adapters (2.15.1)" stating the layout and the no-behaviour-change proof, then the version bump.

## 5. Live check

- `health_check` reports 2.15.1.
- `get_company_news` AAPL, `get_company_press_releases` AAPL and `verify_company_event` AAPL "earnings" return the same shape as on 2.15.0.
- The canaries pass.
