# R5-shared (2.21.0): the untagged-fact confidence default becomes LOW (BR-ENV-001)

**Source:** approved Q3i triage, §2 call 21 (owner, 2026-10-18): "First audit the producers and tag explicitly every fact that is genuinely HIGH. Then the default for anything still untagged becomes LOW, in both runtimes."
**Audit:** done 2026-10-19 (static, both runtimes). Its findings are summarised in §1.
**Version:** 2.21.0 (minor). The `confidence` value on some facts changes from HIGH to LOW. No new field, enum value or warning code.

## 1. Audit summary

**The default** is in `yfmcp/envelope.py` (~246-251) and `worker/src/response.ts` (~206-213); the two runtimes are identical.
- A fact-shaped dict with no own or inherited `confidence` gets `"HIGH"` when `value`, `low` or `valueRatio` is non-null, and `"NOT_DECISION_GRADE"` otherwise.
- The value it gets is written onto the fact, and child dicts inherit it.

**Producers that reach it today (four):**

| # | Tool | Fact | Runtime |
|---|---|---|---|
| A | `get_analyst_consensus` | `priceTargets` (Yahoo consensus targets) | both |
| B | `get_earnings_analysis` | `earningsEstimate[]`, `revenueEstimate[]` rows (Yahoo consensus estimates) | both |
| C | `get_ownership_holders` | institutional, fund and insider rows with Yahoo's lowercase `value` | Worker only (Python rows use `Value`, so they are not fact-shaped) |
| D | `extract_dilution_bridge` | `WARRANT_EXERCISE_NOT_OUTSTANDING` warning `counts[]` (inline-XBRL tagged warrant counts) | both |

**Nothing downstream reads the defaulted value.**
- No status, `recommendedNextAction`, decisionGrade predicate or `LOW_RELEVANCE_EVIDENCE` check reads the post-enrichment confidence.
- No parity case, acceptance test or canary asserts a defaulted value.

## 2. Planner decisions (2026-10-19)

1. **The default becomes LOW** in both runtimes for a fact with `value`, `low` or `valueRatio` non-null and no own or inherited confidence. The `NOT_DECISION_GRADE` branch and the normalisation (upper-case; `NOT_DISCLOSED` → `NOT_DECISION_GRADE`; any other value → `LOW`) stay as they are.
2. **A and B fall to LOW** (no tag).
   - The producers already label themselves contextual market data (`evidenceClass: CONTEXTUAL_MARKET_DATA`, `decisionGrade: false`).
   - The news convention rates contextual sources LOW.
3. **C falls to LOW, Worker only.** The Python/Worker row-shape difference (`Value` vs `value`) is recorded as finding F17 and is not aligned here (rule P5).
4. **D is tagged HIGH explicitly** at the producer, in both runtimes.
   - A tagged inline-XBRL fact read straight from the filing has always been HIGH; the precedents are 8-K iXBRL and `server.py` ~3076.
   - So D's output does not change.
5. **Out of scope, recorded as findings:**
   - **F16:** `compare_earnings_actual_vs_estimate` gives Yahoo `estimate.*` rows the comparison result's HIGH/MEDIUM, while `get_earnings_analysis` will rate the same estimates LOW.
   - **F18:** producer-level `?? "HIGH"` / `.get("confidence", "HIGH")` fallbacks, which the envelope flip does not reach.
     - Python: `server.py` ~6500, 6630, 6672, 8615, 8647, 8751.
     - Worker: `yahoo-finance.ts` ~11420, 11792, 11829, 13392, 13416, 13496, 13514.
     - Python keeps an explicit `None`; TS `??` replaces it.
   - **F19:** Python `get_analyst_consensus` sets a top-level `source`, `evidenceClass`, `decisionGrade` and `recommendedNextAction`, and the Worker sets none of them, so `priceTargets.sourceType` may differ. This is inferred from code; verify it with a parity case in this release and pin it if it diverges.
   - **F20:** status-like strings (`EXTRACTION_FAILED`, `FILING_NOT_FOUND_TRY_OTHER_TYPE`) stored in `confidence` normalise to LOW.
   - **Minor:** Python `or` treats `{}` and `[]` as empty and JS `||` does not. No producer puts an object in `confidence`, so there is no case for it.

## 3. Work

**Worker first:**
- `response.ts`: the default changes to `"LOW"`.
- `capital-structure.ts`: the D producer (the warrant counts added to the warning) gets an explicit `confidence: "HIGH"` on each count.

**Then Python:**
- `envelope.py`: the same default change.
- `capital_structure.py`: the same D tag.

**Parity cases (equal mode), new:**
- A unit case for the default: an untagged fact with `value` gives LOW; one with only `high` gives NOT_DECISION_GRADE; an inherited parent confidence wins.
- One case each for A, B and D, asserting the confidence on the affected facts: A and B LOW, D HIGH.
- C: a Worker-only pinned case showing the LOW rows, with Python's different shape documented.
- F19: a case comparing `priceTargets.sourceType` (and the top-level fields) between runtimes. Pin it if it differs, and record the values in the F19 note.

**Docs:** a section "Untagged Fact Confidence Defaults to LOW (2.21.0)", listing the affected tools and fields, and stating that status and next actions are unchanged.

## 4. Live checks

- `health_check` reports 2.21.0.
- `get_analyst_consensus` AAPL: `priceTargets.confidence` is LOW.
- `get_earnings_analysis` AAPL: estimate rows are LOW.
- `get_ownership_holders` AAPL institutional: rows are LOW.
- An unrelated tagged tool, `extract_sec_filing_fact`, is unchanged.

## 5. Status (2026-10-19)

Released as 2.21.0 (#299). Deploy run #278 was delayed about 10 minutes in the GitHub runner queue, then went green. Live checks at 15:40Z on build 2.21.0+git.f42b93d:
- `get_analyst_consensus` AAPL: `priceTargets.confidence` is LOW. `sourceType` is `unknown` (F19, as pinned).
- `get_earnings_analysis` AAPL: all 8 estimate rows are LOW.
- `get_ownership_holders` AAPL institutional: all 10 rows are LOW (Worker).
- `manifestHash` and `schemaHash` are unchanged from 2.20.1.

R5-shared is complete.
