# R5-pricing-2 (3.1.0): zero volume, short-history CAGR, US session calendar

**Source:** the brief's R5 pricing list (`MODERNIZATION_BRIEF.md`, R5-‹unit› Pricing), decisions Q1 (BR-PRC-002 exchange-session freshness), Q3c (zero volume) and the binding Q3i triage (short-history CAGR), and the Q11 names `ZERO_VOLUME_NO_RATIO`, `INSUFFICIENT_HISTORY` and `SESSION_CALENDAR_UNKNOWN`.

**Owner cards (2026-10-29):** the new rows go in `meta.warnings`, and a snapshot carries its components' rows in `data.warnings`; the holiday calendar covers US exchanges only.

**Version:** 3.1.0 (minor: three new warning codes). No output field or enum value is added; `schemaHash` and `manifestHash` do not change.

## 1. Rules

| Item | Tool | Before | Now |
|---|---|---|---|
| Q3c zero volume | `analyze_volume_ratio` | Python null ratio and flag, no reason; Worker ratio 0, flag `LOW` | `ratio10d`, `ratio90d`, `volumeFlag` null and `ZERO_VOLUME_NO_RATIO` |
| Q3i short history | `analyze_price_performance` | the whole series stood in for a longer window (3 years reported as `cagr5y` 5.8853) | a horizon whose start is more than 10 calendar days before the first bar is null, one `INSUFFICIENT_HISTORY` row per horizon, in horizon order |
| BR-PRC-002 calendar | every tool on the completed daily series (`analyze_price_performance`, `get_price_slope`, `analyze_volume_ratio`, `check_volume_liquidity_threshold`, `get_technical_indicators`, `analyze_options_flow_window`) | in session without `previousClose`, the previous weekday, for every exchange | the previous session on the exchange calendar; no calendar or an uncovered date gives `UNKNOWN`, `expectedCompletedDate` null and `SESSION_CALENDAR_UNKNOWN`; a missing bar is `STALE` against the named session |

Calendar data: `worker/src/exchange-calendars.json`, NYSE full closures 2024 to 2028 (nyse.com Holidays and Trading Hours, including the 2025-01-09 day of mourning; early closes are sessions), for NMS, NGM, NCM, NYQ, ASE and PCX. The Worker compiles it in (`worker/src/exchange-calendars.ts`, `previousExchangeSession`); Python reads it from `worker/src` (`yfmcp/exchange_calendars.py`, `previous_exchange_session`). The walk back is at most 14 days and stops at the first day outside the covered years. A malformed or non-existent date (`2026-02-30`) gives null in both.

## 2. Where the rows go

- Tools put rows in an internal `_warnings` list on their payload (`worker/src/pricing-notices.ts`, `yfmcp/pricing_notices.py`: `withNotices` / `with_notices`). Rows are `{code, message, ticker}`.
- The V2 envelope (`mcpSuccessFromValue` with `liftPayloadWarnings` in `worker/src/response.ts`, which imports no other module so node's strip-types tests can load it alone; `_envelope_tool_result` with `lift_payload_warnings`) removes every `_warnings` list from the data (the data itself, its values, and their values, so batch results are covered) and appends the rows to `meta.warnings`, top level first, then in payload order. Only successful inner envelopes are lifted.
- `get_market_snapshot` takes each component's rows (`takeNotices` / `take_notices`) into its `data.warnings` as `{code, component, message}`.
- With `MCP_ENVELOPE_V2` off nothing is lifted, in both runtimes.

Known gap: `get_position_score_inputs` drops its components' rows; R5-pricing part 3 passes them on.

## 3. Tests

- `scripts/parity/cases_pricing_evidence_tools.py`: the zero-volume, short-CAGR and Monday-holiday cases move from pinned to equal; new cases for an exchange without a calendar (XYZ), a date past 2028, a 6-month history (three rows) and the coverage edge (first bar 10 days after the start covers it, 11 does not). The Tokyo open-session case names `JPX`.
- `scripts/parity/cases_r5price2.py`: each US exchange code checked for every day from 2023-12-01 to 2029-01-31 against an oracle built from the NYSE holiday rules; boundary and malformed inputs; the zero-volume row through the V2 envelope and the grouped route; the lift order; the snapshot row.
- `scripts/test_completed_session_freshness.py`: scenarios carry `exchangeName` (NMS, or JPX for Tokyo); `us_open_after_holiday_unproven` becomes `us_open_after_holiday_named_by_calendar` (CURRENT, 2026-09-04), and `open_after_holiday_without_calendar` (XYZ) stays UNKNOWN. A `previousClose` that does not match is unchanged (STALE, no expected date).
- `scripts/test_exchange_calendars.py` (CI): sorted unique weekday closures inside the covered years, one calendar per code, Python reads the same file, and a failure 90 days before the last covered year ends.

## 4. Exit

- [ ] Full parity harness and every `ci.yml` test command.
- [ ] PR, CI green, owner merge.
- [ ] Live check: `health_check` 3.1.0, schemaHash and manifestHash unchanged; AAPL `analyze_price_performance` and `analyze_volume_ratio` succeed with no new rows; a recent listing shows `INSUFFICIENT_HISTORY` in `meta.warnings` with `cagr5y` null.
