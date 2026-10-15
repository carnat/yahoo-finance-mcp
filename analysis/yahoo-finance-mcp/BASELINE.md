# Phase 2 baseline: behaviour-contract parity harness

Status: the Phase 2 exit criteria were met on 2026-10-15. This file is the reference that later phases are checked against.

## What the harness is

`scripts/parity/` runs the same behaviour in both runtimes and compares the results:
- Python: `yfmcp/` and `server.py`.
- Worker: one esbuild bundle of `worker/src/*.ts`, run by `runner.mjs`.

The parts:
- `harness.py` defines `Case`, the canonical JSON comparison, the network stubs and the Worker bundle.
- There is one `cases_*.py` file per domain.
- `scripts/test_parity_harness.py` runs one unittest per case, plus self-tests that prove the harness can fail.
- `--report FILE` writes the coverage JSON this file was built from.
- `fixtures/parity/` holds recorded public inputs: SEC companyfacts for 8 issuers, and Yahoo 5-year charts for NVDA and AAPL.
- Fixtures are re-recorded by hand only, with `record_fixtures.py`.

Neither runtime reaches the network:
- On the Worker side, `fetch` is stubbed.
- On the Python side, `urlopen` and `OpenerDirector.open` are stubbed, and raw socket connects are refused.
- Any request that no route matches fails with `UNSTUBBED_FETCH`.

There are two case modes:
- **equal:** both runtimes must produce the same value, and the expected value when one is given.
- **pinned:** today's behaviour is held under the decision that schedules its repair. This includes known Python/Worker divergences. The repair release flips the case to `equal`.

## Results

| Measure | Value |
|---|---|
| Cases | 696, all passing (702 tests including the 6 self-tests) |
| Pinned cases | 55 |
| Rule IDs exercised | 173 |
| P0 rules covered | 84 of 84 |
| Repair-scheduled rules with a pinned case | 39 of 39 |
| Run time | about 9 s for the unittest run; about 15 s wall time, including the esbuild bundle |
| Phase 2 budget | under 2 min |
| Stability | 3 full runs in a row, all clean |

| File | Cases | Pinned |
|---|---|---|
| `cases_valuation.py` | 195 | 10 |
| `cases_sec_earnings.py` | 173 | 9 |
| `cases_pricing_evidence_tools.py` | 166 | 27 |
| `cases_capital_structure.py` | 132 | 8 |
| `cases_extraction_rules.py` | 30 | 1 |

### Mutation spot-check

For each domain I changed one rule constant, in each runtime separately. After each change I ran the full harness, then restored the file.

| Domain | Mutation | Python failures | Worker failures |
|---|---|---|---|
| Extraction | `AS_WRITTEN` scale basis renamed | 7 | 7 |
| Capital structure | ADR ratio/price tolerance 0.02 → 0.05 | 2 | 2 |
| Valuation | stale-balance threshold 45 → 60 days | 1 | 1 |
| SEC / fiscal calendar | 52/53-week drift 7 → 3 days | 2 | 2 |
| Pricing | ZAc minor-unit divisor 100 → 10 | 1 | 1 |
| Evidence | EPS absolute band 0.02 → 0.05 | 1 | 1 |

The evidence mutation first survived. Every band case passed an explicit policy, so the code's own default, which the tools use, was never read. I added two default-policy cases (`evd.agreement.code_default_policy_*`), and the mutation is now caught.

Three more fixes came out of the review:
- **Live network hit:** the Python stub missed `build_opener` openers. One transcript-waterfall case was reaching live Yahoo and failed intermittently with a 429.
  - The stub now routes `OpenerDirector.open` too, and refuses sockets.
  - A self-test covers both.
- **API keys:** the SEC/earnings case environment now strips provider API keys, so a developer's keys cannot change results.
- **Report attribution:** `Case.source` records the case file separately from `note`, so the report attributes cases correctly.

## P0 coverage

| P0 rule | Name | Cases | Example cases |
|---|---|---|---|
| BR-PRC-001 | Completed-session bar rule (exclude today's unfinished bar) | 9 | `prc.completed_bar.open_session_drops_todays_row`, `prc.completed_bar.open_session_without_todays_row_excludes_nothing` |
| BR-PRC-002 | Daily price freshness classification (CURRENT / STALE / UNKNOWN) | 12 | `prc.freshness.closed_market_stale_card_example`, `prc.freshness.closed_market_current` |
| BR-PRC-003 | Listing currency to USD conversion with minor units | 9 | `prc.currency.gbp_pence_listing_card_example`, `prc.currency.gbx_alias_behaves_like_gbp_pence` |
| BR-SEC-001 | Company fiscal-year naming offset | 10 | `sec.fiscal_year_naming.dg_offset_minus_one`, `sec.fiscal_year_naming.sec_not_read` |
| BR-SEC-002 | Equivalent SEC concept selection by newest filing | 12 | `sec.pick_concept.newest_filing_wins_over_listed_order`, `sec.pick_concept.card_003_dates` |
| BR-SEC-003 | Equivalent-concept fact selection by newest filing | 12 | `sec.pick_concept.newest_filing_wins_over_listed_order`, `sec.pick_concept.card_003_dates` |
| BR-SEC-004 | Filing period must be 'latest' or a fiscal year | 19 | `sec.parse_filing_period.latest`, `sec.parse_filing_period.null_is_latest` |
| BR-SEC-005 | Fiscal year of a period end (52/53-week drift) | 16 | `sec.fiscal_year_of_period_end.jan_2_belongs_to_prior_year`, `sec.fiscal_year_of_period_end.jan_8_is_new_year` |
| BR-SEC-006 | Fiscal year of a period end (one-week drift rule) | 16 | `sec.fiscal_year_of_period_end.jan_2_belongs_to_prior_year`, `sec.fiscal_year_of_period_end.jan_8_is_new_year` |
| BR-SEC-007 | Fiscal-year assignment for SEC facts (52/53-week years) | 12 | `sec.fiscal_year_rows.jan_3_year_end_without_fp_is_fiscal_2025`, `sec.fiscal_year_rows.jan_3_year_end_is_not_fiscal_2026` |
| BR-EXT-001 | Conversion ratio unit normalisation (per $1,000 vs per $1) | 13 | `cap.ratio.per_1_scaled_to_per_1000`, `cap.ratio.inconsistent_with_price_not_used` |
| BR-EXT-002 | Conversion ratio unit normalization | 13 | `cap.ratio.per_1_scaled_to_per_1000`, `cap.ratio.inconsistent_with_price_not_used` |
| BR-EXT-003 | Convertible note if-converted shares and principal basis | 12 | `cap.ifconv.principal_over_conversion_price_in_the_money`, `cap.ifconv.below_conversion_price_adds_nothing` |
| BR-EXT-004 | Convertible preferred if-converted and liquidation preference | 8 | `cap.pref.below_conversion_price_keeps_liquidation_preference`, `cap.pref.above_conversion_price_if_converted` |
| BR-EXT-005 | Convertible scenario treatments and net share settlement | 4 | `cap.scen.convertible_net_share_settlement_card_examples`, `cap.scen.convertible_net_share_not_stated_falls_back_to_if_converted` |
| BR-EXT-006 | Debt instrument period-end status | 4 | `cap.status.matured_note_excluded_from_reconciliation`, `cap.status.ten_k_matured_outstanding_and_reported` |
| BR-EXT-007 | Dilution bridge status and diluted share formula | 6 | `cap.bridge.diluted_shares_basic_plus_options_plus_warrants`, `cap.bridge.all_components_card_example` |
| BR-EXT-008 | Dilution bridge totals, dilution percentages and status | 7 | `cap.bridge.diluted_shares_basic_plus_options_plus_warrants`, `cap.bridge.all_components_card_example` |
| BR-EXT-009 | Geographic revenue table selection and share check | 12 | `cap.geo.stated_share_off_by_more_than_one_point_rejected`, `cap.geo.table_without_stated_column_pct_usd_denominator` |
| BR-EXT-010 | Guidance basis (GAAP / non-GAAP) classification | 6 | `ext.guidance_basis.non_gaap`, `ext.guidance_basis.gaap` |
| BR-EXT-011 | If-converted shares for convertible notes | 11 | `cap.ifconv.principal_over_conversion_price_in_the_money`, `cap.ifconv.below_conversion_price_adds_nothing` |
| BR-EXT-012 | Instrument-to-total-debt reconciliation | 7 | `cap.status.matured_note_excluded_from_reconciliation`, `cap.recon.net_of_unamortized_costs` |
| BR-EXT-013 | Net-share settlement when principal is stated as cash-settled | 7 | `cap.scen.convertible_cash_principal_card_13_and_24`, `cap.nss.cash_principal_net_shares_beside_if_converted` |
| BR-EXT-014 | Outlook table scale for unscaled amounts | 3 | `ext.outlook_scale.millions`, `ext.outlook_scale.own_scale_kept` |
| BR-EXT-015 | Release figure scale and loss sign | 12 | `ext.release.single_table_scale`, `ext.release.repeated_scale_counts_once` |
| BR-EXT-016 | Reported release metric acceptance rules | 10 | `ext.release.single_table_scale`, `ext.release.two_scales_skipped` |
| BR-EXT-017 | Reported release metric eligibility | 11 | `ext.release.single_table_scale`, `ext.release.repeated_scale_counts_once` |
| BR-EXT-018 | Scenario instrument line inclusion and treasury-stock increment | 4 | `cap.scen.options_treasury_stock_gross_exclude`, `cap.scen.options_at_or_below_strike_add_nothing` |
| BR-EXT-019 | Scenario resulting shares, dilution and completeness | 5 | `cap.scen.resulting_shares_basic_plus_included_plus_known_issuance`, `cap.scen.completeness_precedence_and_status` |
| BR-EXT-020 | Scenario totals, dilution % and completeness | 2 | `cap.scen.totals_options_atm_excluded_known_issuance`, `cap.scen.completeness_precedence_and_status` |
| BR-EXT-021 | Scenario treasury-stock dilution for options and warrants | 5 | `cap.scen.options_treasury_stock_gross_exclude`, `cap.scen.options_at_or_below_strike_add_nothing` |
| BR-EXT-022 | Short-term investments and cash aggregate cross-check | 6 | `cap.cash.investments_already_inside_cash_dropped`, `cap.cash.aggregate_mismatch_warns_and_keeps_investments` |
| BR-EXT-023 | Total debt concept hierarchy and finance-lease removal | 9 | `cap.debt.finance_lease_subtracted_from_lease_inclusive_total`, `cap.debt.lease_parts_less_lease_parts` |
| BR-EXT-024 | Treasury-stock and if-converted dilution mechanics | 8 | `cap.scen.untagged_count_or_strike_is_unresolved`, `cap.scen.warrant_vesting_treatments` |
| BR-EXT-025 | Unvested RSU/PSU awards counted gross | 6 | `cap.bridge.all_components_card_example`, `cap.awards.rsu_and_psu_on_award_axis_summed` |
| BR-EXT-026 | Warrant class retirement by filing text | 3 | `cap.warrants.retired_in_text_exercised_in_whole`, `cap.warrants.text_not_read_keeps_stale_counts` |
| BR-EXT-027 | Warrant exercisable count and treasury-stock dilution | 9 | `cap.bridge.diluted_shares_basic_plus_options_plus_warrants`, `cap.bridge.all_components_card_example` |
| BR-VAL-001 | ADR ordinary-shares-per-quoted-share ratio | 6 | `val.adr.ads_ratio_function`, `val.adr.20f_ratio_5_card_001` |
| BR-VAL-002 | ADR ratio applied only for foreign filers when share counts are a whole multiple apart | 3 | `val.adr.ads_ratio_function`, `val.adr.40f_ratio_5_card_002` |
| BR-VAL-003 | ADR ratio between SEC ordinary shares and Yahoo quoted shares | 3 | `val.adr.ads_ratio_function`, `val.adr.20f_amendment_form_counts_as_foreign_card_003` |
| BR-VAL-004 | ADR ratio detection and share-basis mismatch | 10 | `val.adr.ads_ratio_function`, `val.adr.20f_ratio_5_card_001` |
| BR-VAL-005 | Filing balances vs Yahoo balances selection (currency and staleness) | 7 | `val.balances.stale_181_days_yahoo_used_card_005_033`, `val.balances.stale_but_yahoo_cash_missing_keeps_filing` |
| BR-VAL-006 | Historical balances read at the cash date; no-borrowings means zero debt | 13 | `val.hist.balances.card_006_debt_plus_short_term_borrowings`, `val.hist.balances.card_008_no_borrowings_means_zero_debt` |
| BR-VAL-007 | Historical enterprise value and multiples | 5 | `val.hist.ev.card_007_ev_and_multiples`, `val.hist.ev.card_010_price_120` |
| BR-VAL-008 | Historical EV status precedence and no-borrowings rule | 19 | `val.hist.ev.fx_8_days_old_not_available`, `val.hist.ev.fx_7_days_old_is_used` |
| BR-VAL-009 | Historical market cap status precedence | 13 | `val.hist.ev_status.market_cap_unavailable_beats_missing_balances`, `val.hist.market_cap.card_009_unadjusted_close_times_cover_count` |
| BR-VAL-010 | Historical market cap, EV and multiples | 9 | `val.hist.ev.card_007_ev_and_multiples`, `val.hist.ev.card_010_price_120` |
| BR-VAL-011 | Historical point-in-time share count selection | 11 | `val.hist.market_cap.no_share_count_shares_not_available`, `val.hist.market_cap.zero_count_not_positive` |
| BR-VAL-012 | Historical price on or before date with later splits undone | 12 | `val.hist.price.card_012_later_split_undone`, `val.hist.price.card_013_four_to_one_2022` |
| BR-VAL-013 | Historical price: close within 7 days with later splits undone | 10 | `val.hist.price.card_012_later_split_undone`, `val.hist.price.card_013_four_to_one_2022` |
| BR-VAL-014 | LTM = LFY + YTD - prior YTD | 17 | `val.hist.ev.card_007_ev_and_multiples`, `val.hist.ltm.card_014_lfy_plus_ytd_minus_prior_ytd` |
| BR-VAL-015 | LTM and LFY flow figures | 8 | `val.hist.ltm.card_015_nine_month_ytd`, `val.hist.ltm.fiscal_year_is_latest_period` |
| BR-VAL-016 | LTM flow derivation | 7 | `val.hist.ltm.card_016_six_month_ytd`, `val.hist.ltm.fiscal_year_is_latest_period` |
| BR-VAL-017 | LTM flow metric = last fiscal year + YTD - prior YTD | 8 | `val.hist.ltm.card_014_lfy_plus_ytd_minus_prior_ytd`, `val.hist.ltm.card_017_fy2023_nine_months` |
| BR-VAL-018 | LTM flow metric = LFY + YTD - prior YTD | 10 | `val.hist.ltm.card_014_lfy_plus_ytd_minus_prior_ytd`, `val.hist.ltm.card_018_longest_ytd_wins` |
| BR-VAL-019 | Metric reconciliation agreement status | 14 | `val.recon.card_019_release_matches_within_tolerance`, `val.recon.card_021_difference_pct_and_tolerance` |
| BR-VAL-020 | Metric reconciliation status | 20 | `val.recon.card_019_release_matches_within_tolerance`, `val.recon.card_022_yahoo_within_tolerance_agreed` |
| BR-VAL-021 | Metric reconciliation tolerance and match | 16 | `val.recon.card_019_release_matches_within_tolerance`, `val.recon.card_021_difference_pct_and_tolerance` |
| BR-VAL-022 | Metric source reconciliation status | 29 | `val.recon.card_019_release_matches_within_tolerance`, `val.recon.card_021_difference_pct_and_tolerance` |
| BR-VAL-023 | Minor-unit quote conversion and currency comparability | 10 | `val.balances.pence_quote_uses_gbp_filing`, `val.currency.gbp_pence_1250_card_023` |
| BR-VAL-024 | Minor-unit quote conversion and currency comparability for multiples | 5 | `val.currency.gbp_pence_negative_ebitda_card_024`, `val.currency.gbp_quote_usd_financials_all_multiples_null_card_024` |
| BR-VAL-025 | Parent net income derived from ProfitLoss less NCI | 11 | `val.recon.net_income.card_025_profit_loss_less_nci`, `val.recon.net_income.profit_loss_filed_later_than_net_income_loss` |
| BR-VAL-026 | Point-in-time fact selection and concept precedence | 10 | `val.hist.market_cap.count_filed_after_date_ignored`, `val.hist.pit.card_026_listed_first_concept_beats_newer_10ka` |
| BR-VAL-027 | Point-in-time fact selection: concept priority beats newer filing | 5 | `val.hist.pit.card_026_listed_first_concept_beats_newer_10ka`, `val.hist.pit.card_029_first_listed_concept_despite_later_revenues_filing` |
| BR-VAL-028 | Point-in-time price with later splits undone | 7 | `val.hist.price.card_012_later_split_undone`, `val.hist.price.card_028_30_times_4` |
| BR-VAL-029 | Point-in-time SEC fact precedence | 8 | `val.hist.pit.card_026_listed_first_concept_beats_newer_10ka`, `val.hist.pit.card_029_first_listed_concept_despite_later_revenues_filing` |
| BR-VAL-030 | Point-in-time share count precedence for historical market cap | 12 | `val.hist.market_cap.zero_count_not_positive`, `val.hist.market_cap.card_030_weighted_average_only_is_unresolved` |
| BR-VAL-031 | Quote vs financial currency comparability | 13 | `val.balances.filing_in_other_currency_not_used`, `val.balances.pence_quote_uses_gbp_filing` |
| BR-VAL-032 | SEC balance usage guards (currency, staleness, cash cross-checks) | 17 | `val.balances.stale_90_days_yahoo_used_card_032_034`, `val.balances.exactly_45_days_keeps_filing` |
| BR-VAL-033 | SEC balances replaced by Yahoo when stale or in another currency | 9 | `val.balances.stale_181_days_yahoo_used_card_005_033`, `val.balances.stale_but_yahoo_cash_missing_keeps_filing` |
| BR-VAL-034 | SEC vs Yahoo balance-sheet source selection and staleness | 10 | `val.balances.stale_90_days_yahoo_used_card_032_034`, `val.balances.exactly_45_days_keeps_filing` |
| BR-VAL-035 | Share basis mismatch falls back to Yahoo count | 7 | `val.share_basis.domestic_400m_vs_200m_card_035`, `val.share_basis.ratio_1_4_keeps_sec_count` |
| BR-VAL-036 | Share basis mismatch falls back to Yahoo share count | 4 | `val.share_basis.domestic_300m_vs_150m_card_036`, `val.share_basis.ratio_1_4_keeps_sec_count` |
| BR-ERN-001 | Earnings actual vs estimate period alignment and surprise | 5 | `ern.compare.matched_period_gives_revenue_and_eps_surprise`, `ern.compare.different_labels_mismatch_omits_revenue_surprise_keeps_eps` |
| BR-ERN-002 | Guidance amount scaling | 31 | `ern.parse_amount.unitless_low_takes_other_ends_billion`, `ern.parse_amount.billion_word` |
| BR-ERN-003 | Guidance amount scaling | 26 | `ern.parse_amount.unitless_low_takes_other_ends_billion`, `ern.parse_amount.billion_word` |
| BR-ERN-004 | Guidance extraction status (FOUND / NOT_DISCLOSED / NOT_READ) | 6 | `ern.guidance_entries.withdrawn_guidance_is_an_event`, `ern.extract_guidance.found_revenue_range_shared_billion_unit` |
| BR-ERN-005 | Reported actuals period labelling for guidance outcomes | 10 | `ern.actuals.calendar_filer_labels_q2_h1_and_fy`, `ern.actuals.non_calendar_without_stated_year_has_no_quarter_labels` |
| BR-EVD-001 | Consensus provider agreement | 7 | `evd.agreement.eps_relative_within_tolerance_agreed`, `evd.agreement.eps_9_57_pct_agreed_denominator_is_larger_magnitude` |
| BR-EVD-002 | Consensus provider agreement classification | 15 | `evd.agreement.eps_relative_within_tolerance_agreed`, `evd.agreement.eps_within_two_cents_agreed_despite_tight_pct_tolerance` |
| BR-EVD-003 | Cross-provider consensus agreement status | 19 | `evd.agreement.eps_relative_within_tolerance_agreed`, `evd.agreement.eps_9_57_pct_agreed_denominator_is_larger_magnitude` |
| BR-EVD-004 | Evidence cut integrity verification | 8 | `evd.cut.stored_text_hashing_to_the_id_is_verified`, `evd.cut.any_changed_byte_is_a_mismatch` |
| BR-EVD-005 | Evidence storage write-once lifecycle | 9 | `evd.cut.no_store_is_storage_unavailable`, `evd.store.first_write_is_stored` |
| BR-SHR-001 | Decision-grade SEC XBRL fact | 17 | `shr.decision_grade.complete_xbrl_fact_is_decision_grade`, `shr.decision_grade.status_comparison_ignores_case` |

## Pinned behaviour

Each pinned case names the owner decision that schedules its repair. Cases marked `FINDING` are Python/Worker divergences that no decision covers yet; they are listed below.

| Pinned case | Rule IDs | Decision |
|---|---|---|
| `cap.ifconv.price_equal_conversion_price_counts_all_shares` | BR-EXT-003, BR-EXT-011 | Q3f |
| `cap.scen.in_the_money_boundary_options_vs_convertibles` | BR-EXT-003, BR-EXT-005, BR-EXT-018, BR-EXT-021, BR-EXT-024, BR-EXT-082, BR-EXT-083 | Q3f |
| `cap.pref.price_equal_conversion_price_counts_all_shares` | BR-EXT-004, BR-EXT-082 | Q3f |
| `cap.funding.leap_day_period_through_invalid_date` | BR-EXT-044, BR-EXT-045, BR-EXT-110 | Q3i |
| `cap.funding.leap_day_maturity_ladder_period_through_invalid_date` | BR-EXT-044, BR-EXT-045, BR-EXT-110 | Q3i |
| `cap.funding.text_ntd_dollar_labelled_usd` | BR-EXT-059, BR-EXT-064 | Q3i |
| `cap.funding.text_aud_dollar_labelled_usd` | BR-EXT-059, BR-EXT-064 | Q3i |
| `cap.funding.text_cad_dollar_labelled_usd` | BR-EXT-059, BR-EXT-064 | Q3i |
| `ext.sign.monetary_loss_wording_unsigned` | BR-EXT-015 | Q1 BR-EXT-015 / Q3i |
| `prc.freshness.open_after_monday_holiday_weekday_fallback_unknown` | BR-PRC-002 | Q1 BR-PRC-002 |
| `shr.decision_grade.array_xbrl_context_diverges` | BR-SHR-001 | FINDING: Worker accepts an array xbrlContext as decision-grade, Python requires a dict |
| `shr.decision_grade.whitespace_only_evidence_field_diverges` | BR-SHR-001 | FINDING: Python accepts a whitespace-only evidence field, the Worker trims it and rejects |
| `tool.q3b.outline_invalid_ticker_python_rejects_worker_proceeds` | BR-ENV-007 | Q3b |
| `tool.q3b.outline_invalid_accession_python_rejects_worker_proceeds` | BR-ENV-005 | Q3b |
| `tool.q3a.outline_accession_only_python_resolves_worker_ignores` | Q3a | Q3a |
| `tool.q4.smoke_table_resolves_listed_ticker_when_yahoo_and_sec_fail` | Q4 | Q4 |
| `tool.q4.smoke_table_covers_aaoi` | Q4 | Q4 |
| `tool.nws.coverage_website_not_available_failed_source` | BR-NWS-007 | Q3i |
| `tool.q3h.empty_sources_python_substitutes_defaults_worker_attempts_none` | Q3h | Q3h |
| `prc.slope.q3b_invalid_ticker_is_not_rejected_by_either_runtime` | BR-PRC-014, BR-ENV-007 | Q3b |
| `prc.slope.q3b_non_numeric_days_python_raises_worker_defaults_to_five` | BR-PRC-014 | Q3b |
| `prc.volume_ratio.q3c_zero_volume_session_python_null_worker_zero_low` | BR-PRC-033 | Q3c |
| `prc.volume_ratio.q3c_average_rounding_differs_between_runtimes` | BR-PRC-033 | Q3c |
| `prc.batch.q3c_seven_tickers_python_processes_all_worker_truncates_to_five` | BR-SHR-006 | Q3c |
| `prc.cagr.q3i_short_history_five_year_cagr_uses_nominal_five_years` | BR-PRC-019, BR-PRC-021 | Q3i |
| `prc.cagr.start_bar_selection_differs_on_the_horizon_boundary` | BR-PRC-021 | FINDING: Python (calendar DateOffset) and Worker (365.25-day years) pick different CAGR start bars at the horizon boundary |
| `prc.bracket.q3d_threshold_uses_the_rounded_percentage` | BR-PRC-022 | Q3d |
| `prc.bracket.q3d_rounding_differs_python_round_vs_ts_tofixed` | BR-PRC-022 | Q3d |
| `prc.bracket.q3d_missing_quote_time_python_null_worker_today` | BR-PRC-022 | Q3d |
| `fnd.credit.q3i_loss_maker_with_net_debt_is_tiered_clean` | BR-FND-007 | Q3i |
| `fnd.credit.q3i_three_quarter_ttm_python_partial_worker_ok` | BR-FND-004, BR-FND-006 | Q3i |
| `fnd.credit.q3i_long_term_debt_fallback_exists_only_in_python` | BR-FND-004, BR-FND-006 | Q3i |
| `shr.momentum.q3i_beat_streak_counts_from_the_first_provider_row` | BR-SHR-010, BR-SHR-012 | Q3i |
| `shr.momentum.q3i_beat_streak_reversed_rows_give_a_different_streak` | BR-SHR-010 | Q3i |
| `prv.eligibility.q3i_unreadable_policy_fails_open_in_python` | BR-PRV-002, BR-PRV-003 | Q3i |
| `shr.form4.q3i_multi_line_filing_reports_only_the_first_transaction` | BR-SHR-016 | Q3i |
| `ern.source_url.paywalled_subdomain_markets_wsj_not_blocked` | BR-ERN-017 | Q2a |
| `ern.source_url.paywalled_subdomain_on_wsj_not_blocked` | BR-ERN-017 | Q2a |
| `ern.source_url.any_https_host_accepted_as_company_ir` | BR-ERN-017 | Q2a |
| `ern.source_url.unparseable_url_message_differs` | BR-ERN-017 | FINDING: unparseable source_url message differs (Python 'must use https', Worker 'must be a valid URL') |
| `ern.compare.zero_eps_estimate_gives_no_delta` | BR-ERN-031, BR-ERN-018 | Q3i |
| `ern.compare.date_identified_row_is_unverified_runtimes_differ` | BR-ERN-001, BR-ERN-018 | FINDING: dated Yahoo row gives estimatePeriod '2026-06-30T00:00:00' in Python, '2026-06-30' in the Worker, and only the Worker finds the revenue estimate |
| `ern.compare.actual_eps_comes_from_yahoo_not_the_release` | BR-ERN-018, BR-ERN-031, BR-ERN-001 | Q3i |
| `ern.transcript_waterfall.next_fallback_is_company_ir_when_nothing_found` | BR-ERN-009 | Q3i |
| `ern.transcript_waterfall.next_fallback_is_company_ir_even_with_fiscal_quarter_and_alpha_vantage_tried` | BR-ERN-009 | Q3i |
| `val.ev.pinned_convertible_deducted_when_sec_share_count_rejected` | BR-VAL-046, BR-VAL-074, BR-VAL-082 | Q3i |
| `val.ev.pinned_convertible_deducted_when_bridge_has_no_diluted_count` | BR-VAL-046, BR-VAL-074, BR-VAL-082 | Q3i |
| `val.hist.balances.pinned_partial_debt_group_uses_tagged_component_only` | BR-VAL-006, BR-VAL-008 | Q1 BR-VAL-006 |
| `val.recon.net_income.pinned_missing_nci_reports_raw_profit_loss` | BR-VAL-025, BR-VAL-056 | Q1 BR-VAL-025 |
| `val.recon.net_income.pinned_nci_missing_for_first_filed_row_reports_raw` | BR-VAL-025, BR-VAL-056 | Q1 BR-VAL-025 |
| `val.recon.net_income.pinned_nci_from_other_filing_reports_raw` | BR-VAL-025, BR-VAL-056 | Q1 BR-VAL-025 |
| `val.recon.net_income.pinned_nci_for_other_period_reports_raw` | BR-VAL-025, BR-VAL-056 | Q1 BR-VAL-025 |
| `val.recon.pinned_tolerance_nan_and_infinity` | BR-VAL-019, BR-VAL-063, BR-VAL-021 | Q3e |
| `val.recon.pinned_tolerance_numeric_string` | BR-VAL-063 | FINDING: numeric-string tolerance_pct is 5.0 in Python (float() coerces) but the 0.5 default in the Worker (Number.isFinite does not coerce) |
| `val.recon.pinned_tool_description_agreed_wording` | BR-VAL-020, BR-VAL-019 | Q3e |

## Findings for the planner

None of these were resolved during Phase 2. Under CLAUDE.md, a divergence that needs a behaviour choice is the planner's call. Each is held by a case at today's behaviour.

### A. Python and the Worker give different answers

1. **BR-ERN-018, estimate period.**
   - For a dated Yahoo row, `estimatePeriod` is `2026-06-30T00:00:00` in Python and `2026-06-30` in the Worker.
   - As a result, only the Worker finds the revenue estimate.
2. **BR-ERN-017, unparseable `source_url`.**
   - Python's message is "must use https".
   - The Worker's message is "must be a valid URL".
3. **BR-VAL-063, `tolerance_pct` as a numeric string.**
   - Python reads 5.0, because `float()` coerces.
   - The Worker uses the 0.5 default.
   - This sits beside the Q3e NaN/infinity divergence, which is already pinned.
4. **BR-PRC-021, CAGR start bar.**
   - Python uses a calendar `DateOffset`.
   - The Worker uses 365.25-day years.
   - So they pick different start bars at the horizon boundary.
5. **BR-SHR-001, decision-grade facts.**
   - A whitespace-only evidence field passes in Python and is rejected by the Worker.
   - An array `xbrlContext` passes in the Worker and is rejected by Python.
6. **BR-NWS-007, source coverage.** Pinned under Q3i.
   - Python's coverage omits WEBSITE_NOT_AVAILABLE, DISCOVERY_*, FEED_NOT_FOUND and PARSE_ERROR, so it reports FULL beside a failed source.
   - The Worker reports PARTIAL.
7. **BR-FND-006/007, credit health.** Pinned under Q3i.
   - With fewer than 4 income quarters, Python gives `dataQuality` PARTIAL and the Worker does not.
   - Python falls back to Long Term Debt; the Worker reads TotalDebt only.

### B. Both runtimes agree, but the behaviour looks wrong (for Q3i triage)

- **BR-EXT-015:** "-$5 billion" reads positive, because a minus sign before the dollar sign is dropped.
- **BR-EXT-010:** "on both a GAAP and non-GAAP basis" after a range reads as GAAP only.
- **BR-EXT-001/002:** a ratio exactly 2% off is rejected through float error. The card says "within 2%".
- **BR-EXT-019/027:** a warrant class with no `exercisable` key uses its full outstanding count but is labelled `vested_exercisable`.
- **BR-VAL-006/047:** debt tagged only in an older 10-K reads as NO_BORROWINGS_TAGGED with debt 0, which can overstate EV.
- **BR-VAL-025:** IFRS `ProfitLoss`, which includes NCI, is reported raw. This is the same defect class as the scheduled US-GAAP repair.
- **Q3i convertible deduction:** it also fires when the bridge has no diluted share count, a trigger the cards do not name.
- **Feb 29 (Q3i):** the Feb 29 `_add_years` path also reaches warrant `termElapsedBy`, not only the ladder and the funding schedule.

### C. The cards are wrong; the code was followed

- **BR-EXT-024:** a missing conversion price leaves a line unresolved only under the default and `net_share_settlement_when_stated` treatments.
- **BR-EXT-020:** an excluded ATM line has no `incrementalByComponent` key at all; the card implies the key holds 0.
- **BR-EXT-008:** a missing `atm_program` also triggers `taggedDilutionConcepts`.
- **BR-VAL-033:** `balances.shortTermInvestments` is null, not 0.
- **Reconciliation:** full-path reconciliation always includes an `SEC_XBRL_AS_FIRST_FILED` row.
- **Historical market cap:** it uses the unrounded close, not the 4-decimal `price.close`.
- **SEC/earnings:**
  - The Worker has an XBRL tier that Python lacks.
  - The EPS fallback is unreachable.
  - COHR's mistagged `fy` overwrites FY2020.
  - `select_fiscal_year_rows` is not annual-only.
  - `unit_of("$150 MM")` returns "m".

## Not exercised, and harness follow-ups

- **Python-only store key check:** the store key validation (BR-EVD-027) exists only in Python, so there is no Worker behaviour to compare.
- **Unreadable policy file:** the Worker's news-source policy is compiled in, so this path is Python-only.
- **Q3h with `sources` omitted:** this branch needs live providers. Only an explicit `[]` is pinned.
- **Private TS functions:** `prepareCompletedDailySeries`, `buildCoverage`, `computeSourceCoverage` and `parseForm4Transaction` are reached only through tool-level cases.
- **Possible harness additions:**
  - a `clock` option, since `Date.now` overrides do not affect `new Date()`;
  - a Python cache reset between cases;
  - per-runtime environment variables;
  - query-string route matching;
  - one shared home for the iXBRL builders that are now imported from `scripts/test_*.py`;
  - fixtures referenced by path instead of embedded in each case.
