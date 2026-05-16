# Tool Naming Migration

## Naming principles

- Canonical tools use clear, general financial verbs: `get_`, `list_`, `search_`, `screen_`, `analyze_`, `summarize_`, `check_`, `find_`, `calculate_`.
- Canonical names avoid doctrine/internal terms (TPS, EQF, ADV gate, DC-*).
- Aliases remain available for backward compatibility.
- Inputs must be public-data oriented and must not require holdings, cost basis, position size, or private workflow state.
- Output contracts should keep provider values, computed ratios, confidence/data-quality flags, warnings, and evidence/source metadata clearly separated.

## Public naming and description policy

1. Tool names must describe public financial data access or public computations.
2. Tool names must not encode private workflow labels.
3. Tool descriptions must be understandable by any external caller.
4. Inputs must not ask for holdings, cost basis, position size, or private workflow state.
5. Caller-defined thresholds are allowed via generic parameter names only (no private rule labels).
6. Output should separate raw provider values, computed public ratios, confidence/data quality, warnings, and evidence/source metadata.

## Canonical tool groups

- Market: `get_market_quote`, `get_historical_prices`, `analyze_price_performance`, `analyze_moving_average_position`, `analyze_volume_ratio`, `check_volume_liquidity_threshold`
- Fundamentals: `get_company_profile`, `get_fund_profile`, `analyze_financial_ratios`, `analyze_credit_health`, `get_corporate_actions`, `get_ownership_holders`
- Analyst/earnings: `get_analyst_consensus`, `get_analyst_recommendations`, `get_analyst_rating_changes`, `get_earnings_analysis`, `analyze_earnings_momentum`, `get_company_events_calendar`
- Options: `get_option_expiration_dates`, `get_option_chain`, `summarize_options_flow`, `analyze_options_flow_window`, `find_put_hedge_candidates`
- SEC: `list_sec_company_filings`, `get_sec_filing_outline`, `get_sec_filing_section`, `list_sec_filing_tables`, `get_sec_filing_table`, `extract_sec_filing_fact`, `search_sec_filing_text`
- Position: `analyze_position_signals`, `calculate_price_target_distance`

## Alias map

- `get_fast_info` → `get_market_quote`
- `get_tps_inputs` → `analyze_position_signals`
- `get_eqf_bracket` → `calculate_price_target_distance`
- `get_adv_gate` → `check_volume_liquidity_threshold`
- `get_dc134_options_scan` → `analyze_options_flow_window`
- `get_options_summary` / `get_options_flow_summary` → `summarize_options_flow`
- `list_sec_filings` → `list_sec_company_filings`
- `get_filing_outline` → `get_sec_filing_outline`
- `get_filing_section` → `get_sec_filing_section`
- `list_filing_tables` → `list_sec_filing_tables`
- `get_filing_table` → `get_sec_filing_table`
- `extract_filing_fact` / `get_filing_data` → `extract_sec_filing_fact`
- `search_filing_text` → `search_sec_filing_text`

## Deprecation policy

Canonical tool names are designed for general financial callers. Doctrine-specific names remain supported as aliases but are not preferred. Aliases may be removed in a future major version after migration notice.

- Canonical names are first-class in discovery and documentation.
- Alias calls remain callable. Legacy doctrine aliases emit `DEPRECATED_ALIAS` warnings in V2 envelope mode.
- Deprecated aliases should expose standardized manifest metadata:
  - `deprecated: true`
  - `useInstead: "<canonical_or_preferred_public_tool_name>"`
  - `deprecationReason: "Use the canonical public tool name."`

## Examples

- Use `get_market_quote` instead of `get_fast_info`.
- Use `analyze_position_signals` instead of `get_tps_inputs`.
- Use `calculate_price_target_distance` instead of `get_eqf_bracket`.
- Use `check_volume_liquidity_threshold` instead of `get_adv_gate`.
- Use `analyze_options_flow_window` instead of `get_dc134_options_scan`.
