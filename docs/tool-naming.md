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

- Market: `get_market_quote`, `get_historical_prices`, `analyze_price_performance`, `analyze_moving_average_position`, `analyze_volume_ratio`, `check_volume_liquidity_threshold`, `get_technical_indicators`, `get_price_slope`, `get_short_interest`, `get_short_momentum`, `get_overnight_quote`
- Snapshot: `get_market_snapshot`, `get_manifest_diagnostics`, `health_check`
- Fundamentals: `get_company_profile`, `get_fund_profile`, `get_financial_statement`, `analyze_financial_ratios`, `analyze_credit_health`, `get_corporate_actions`, `get_ownership_holders`
- Analyst/earnings: `get_analyst_consensus`, `get_analyst_recommendations`, `get_analyst_rating_changes`, `get_earnings_analysis`, `analyze_earnings_momentum`, `get_company_events_calendar`
- Options: `get_option_expiration_dates`, `get_option_chain`, `summarize_options_flow`, `analyze_options_flow_window`, `find_put_hedge_candidates`
- SEC: `list_sec_company_filings`, `get_sec_filing_outline`, `get_sec_filing_section`, `list_sec_filing_tables`, `get_sec_filing_table`, `extract_sec_filing_fact`, `search_sec_filing_text`, `index_sec_filing`, `get_sec_filing_index`, `query_sec_filing_index`
- SEC extractors: `extract_geographic_revenue`, `extract_segment_revenue`, `extract_total_revenue`, `extract_revenue_exposure`, `extract_china_exposure`, `extract_risk_factor_mentions`, `extract_customer_concentration`
- News/events: `get_company_news`, `search_company_news`, `get_company_press_releases`, `get_sec_recent_events`, `get_public_event_timeline`, `verify_company_event`
- Earnings intelligence: `get_latest_earnings_release`, `index_earnings_release`, `extract_earnings_metrics`, `extract_guidance`, `extract_management_commentary`, `compare_earnings_actual_vs_estimate`, `get_earnings_call_transcript`
- Position/discovery: `analyze_position_signals`, `calculate_price_target_distance`, `search_ticker`, `screen_stocks`

## Alias map

- `get_fast_info` → `get_market_quote`
- `get_position_score_inputs` → `analyze_position_signals`
- `get_price_target_bracket` → `calculate_price_target_distance`
- `get_volume_gate` → `check_volume_liquidity_threshold`
- `get_options_flow_scan` → `analyze_options_flow_window`
- `get_options_summary` / `get_options_flow_summary` → `summarize_options_flow`
- `list_sec_filings` → `list_sec_company_filings`
- `get_filing_outline` → `get_sec_filing_outline`
- `get_filing_section` → `get_sec_filing_section`
- `list_filing_tables` → `list_sec_filing_tables`
- `get_filing_table` → `get_sec_filing_table`
- `extract_filing_fact` / `get_filing_data` → `extract_sec_filing_fact`
- `search_filing_text` → `search_sec_filing_text`

## Deprecation policy

Canonical tool names are designed for general financial callers. Public aliases remain supported for compatibility. Private doctrine-style aliases are removed from discovery and routing.

- Canonical names are first-class in discovery and documentation.
- Public alias calls remain callable. Deprecated public aliases emit `DEPRECATED_ALIAS` warnings in V2 envelope mode when metadata is present.
- Deprecated aliases should expose standardized manifest metadata:
  - `deprecated: true`
  - `useInstead: "<canonical_or_preferred_public_tool_name>"`
  - `deprecationReason: "Use the canonical public tool name."`

## Examples

- Use `get_market_quote` instead of `get_fast_info`.
- Use `analyze_position_signals` instead of `get_position_score_inputs`.
- Use `calculate_price_target_distance` instead of `get_price_target_bracket`.
- Use `check_volume_liquidity_threshold` instead of `get_volume_gate`.
- Use `analyze_options_flow_window` instead of `get_options_flow_scan`.
