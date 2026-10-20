# 2.25.0: consensus provenance in the Worker (F19), and Yahoo estimate rows rated LOW in compare (F16)

**Source:** the approved F triage rows F19 (R5-fundamentals) and F16 (R5-earnings).
**Owner decision (2026-10-20):** "ok yes" to shipping them together as 2.25.0, after 2.24.0 (F21), ahead of the Phase 4 gate.
**Version:** 2.25.0 (minor). No new field name or enum value: every value already exists in the other runtime or tool (P2).

## F19: `get_analyst_consensus` provenance (Worker follows Python)

- In the Worker's `getAnalystConsensus` output (`yahoo-finance.ts` ~2655), set the four keys Python sets (`server.py` ~1319-1325), in the same order:
  - `source: "yahoo_finance"`
  - `evidenceClass: "CONTEXTUAL_MARKET_DATA"`
  - `decisionGrade: false`
  - `recommendedNextAction: "NONE"`
- `priceTargets.sourceType` then inherits `yahoo_finance`, as in Python. `priceTargets.confidence` stays LOW (2.21.0).
- **Parity:** flip `confidence.analyst_consensus.finding_f19_...` (pinned) to an equal case. Expect Python's values (`"hasSource": true`, `"priceTargetsSourceType": "yahoo_finance"`) and rename it `confidence.analyst_consensus.top_level_provenance_matches`.
- **Docs:** state that the F17 deprecation notice (Python holder rows renamed in Phase 6) belongs to R5-fundamentals. It is announced in this docs section only; there is no runtime notice, because none is approved yet.

## F16: Yahoo estimate rows inside `compare_earnings_actual_vs_estimate`

- `estimate.revenue` and `estimate.eps` get an explicit `confidence: "LOW"` in both runtimes:
  - Python: `yfmcp/tools/earnings.py` ~795-797.
  - Worker: `yahoo-finance.ts` ~14543.
  - This matches `get_earnings_analysis`, which rates the same Yahoo consensus rows LOW since 2.21.0.
- The comparison result keeps its own `confidence` (HIGH / MEDIUM / NOT_DISCLOSED), unchanged.
- **Out of scope, recorded as finding F22:** `actual.eps` is also a Yahoo row (`source: "yahoo"`) and inherits the result's confidence. Rating it LOW would need its own decision (it is an actual, not a consensus estimate).
- **Parity:** a new equal case on a stubbed compare call. It asserts that `estimate.revenue.confidence` and `estimate.eps.confidence` are LOW while the top-level `confidence` is HIGH (aligned periods, non-zero estimates).

## Live checks

- `health_check` reports 2.25.0.
- `get_analyst_consensus` AAPL: top-level `source` is `yahoo_finance`, `evidenceClass` is `CONTEXTUAL_MARKET_DATA`, `decisionGrade` is false, `priceTargets.sourceType` is `yahoo_finance`, and `priceTargets.confidence` is LOW.
- `compare_earnings_actual_vs_estimate` AAPL: the `estimate.*` confidence is LOW, and the top-level confidence is unchanged from 2.24.0.
