# Phase 3 pilot: Python imports record (`yfmcp/tools/news.py`)

Python half of the imports record required by `PHASE3_SPEC.md` (the planner merges it with the Worker half into `PHASE3_IMPORTS.md`).
Scope: 79 functions (2,839 NLOC by `phase3_news_region.json`) and 33 module constants moved from `server.py`
to `yfmcp/tools/news.py`; every moved function is AST-identical to its original, and the only text changes
in moved code are the two `Path(__file__)` constants (below).

## 1. What `yfmcp/tools/news.py` imports

`news.py` never imports `server` (no cycle) and has no `_server_attr(...)` back-calls. Standard-library and
third-party modules keep the aliases `server.py` uses, so names the moved code reads (and tests patch) are
unchanged: `asyncio, datetime, hashlib, json, math, os, functools`, `html as _html_module`, `re as _re`,
`urllib.parse as _urlparse`, `urllib.request as _urlrequest`, `urllib.error as _urlerror`,
`xml.etree.ElementTree as _ET`, `pathlib.Path`, `typing.Annotated`, `yfinance as yf`, `pydantic.Field`.

| Names | Imported from | Why |
|---|---|---|
| `yfinance_server` | `yfmcp.app` | the five tool decorators in this module (`@yfinance_server.tool`) |
| `_TOOL_OUTPUT_SCHEMAS` | `yfmcp.schemas` | `output_schema=` of the six tools |
| `ErrorCode`, `_mcp_failure` | `yfmcp.envelope` | input-validation failures |
| `_validate_ticker` | `yfmcp.validation` | ticker checks |
| `_tool_cache`, `TTL_NEWS` | `yfmcp.cache` | GlobeNewswire / feed caching |
| `_utc_now_iso`, `_to_iso_utc`, `_parse_rss_date` | `yfmcp.util` | timestamps |
| `_safe_parse` | `yfmcp.clients.yahoo` | batch result parsing in `get_company_news` |
| `_edgar_get_html`, `_get_submissions_for_ticker` | `yfmcp.clients.edgar` | SEC event collectors |
| `_PROVIDER_USER_AGENT` | `yfmcp.clients.market_providers` | non-SEC provider requests (moved there, see 2) |
| `_parse_html_table`, `_strip_html_tags` | `yfmcp.parsing.html` | IR-page / Form 4 parsing |
| `extraction_rules` as `_er` | `yfmcp` | event/period rules |
| `TOOL_RELIABILITY_CLASS` as `_TOOL_RELIABILITY_CLASS` | `yfmcp.deprecations` | `reliabilityClass` on `get_company_press_releases` |
| `_run_ticker_batch` | `yfmcp.tools.pricing` | per-ticker batch path of `get_company_news` |

Nothing is imported from `server.py`. Imports from other `yfmcp.tools.*` modules: only `_run_ticker_batch`
(pricing). This is the shared-helper debt Phase 4 can hoist (`_run_ticker_batch` into a neutral module).

## 2. Constants

* Moved to `news.py` (33): `_DEDUP_TITLE_MAX_LEN, _STALE_EVENT_DAYS, _PHASE6B_SUPPORTED_SOURCES, _OFFICIAL_SOURCE_TYPES,
  _SOURCE_PRIORITY, _YAHOO_NEWS_LEGAL_SUFFIXES, _YAHOO_NEWS_ACRONYM_IGNORED_WORDS, _YAHOO_ALLOWED_CONTENT_TYPES,
  _FINNHUB_NEWS_API, _MARKETAUX_NEWS_API, _MARKETAUX_WIRE_DOMAINS, _MARKETAUX_MAX_ITEMS, _GLOBENEWSWIRE_RSS_FEEDS,
  _GLOBENEWSWIRE_MAX_BYTES, _GLOBENEWSWIRE_BLOCKED_XML_MARKERS, _GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN,
  _GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN, _COMPANY_IR_PAGE_REGISTRY_PATH, _NEWS_SOURCE_CAPABILITIES_PATH,
  _COMPANY_IR_PAGE_MAX_BYTES, _DEFAULT_NEWS_SOURCES, _DEFAULT_PRESS_RELEASE_SOURCES, _DEFAULT_EVENT_SOURCES,
  _GET_COMPANY_NEWS_SOURCES_DESCRIPTION, _SOURCES_DEFAULT_DESCRIPTION, _NOT_ATTEMPTED_SOURCE_STATUSES,
  _FORM4_TRANSACTION_LABELS, _PRESS_RELEASE_WIRE_RE, _YAHOO_PRESS_RELEASE_METHOD, _NEWS_RELEVANCE_RANK`, plus three
  constants that nothing in the repo reads (dead code) but that sit inside the news block and belong to it:
  `_NEWSWIRE_HINTS`, `_COMPANY_IR_URL_MARKERS`, `_GLOBENEWSWIRE_RSS_URL` (`_GLOBENEWSWIRE_RSS_URL = _GLOBENEWSWIRE_RSS_FEEDS[0][1]`,
  so it had to follow `_GLOBENEWSWIRE_RSS_FEEDS`). Planner: say if you prefer the two unreferenced ones left in `server.py`.
* `_COMPANY_IR_PAGE_REGISTRY_PATH` and `_NEWS_SOURCE_CAPABILITIES_PATH`: `Path(__file__).resolve().parent / "worker" / ...`
  became `Path(__file__).resolve().parents[2] / "worker" / ...` because the module moved from the repo root to
  `yfmcp/tools/`. The resolved absolute path is identical (verified for both files).
* Shared, so moved to a leaf module: `_PROVIDER_USER_AGENT = "yahoo-finance-mcp"` (with its comment) now lives in
  `yfmcp/clients/market_providers.py`. News (`_collect_globenewswire_events`, `_collect_finnhub_events`,
  `_collect_marketaux_events`) and `server._companies_house_get` both use it; `server.py` imports it, so
  `server._PROVIDER_USER_AGENT` still resolves.
* Stayed in `server.py`: `_PRE_REVENUE_EPS_EPSILON` (used only by `get_earnings_momentum`, though it sat in the news block).

## 3. Imports back into `server.py` (re-exports kept)

`server.py` has `import yfmcp.tools.news  # noqa: F401` plus `from yfmcp.tools.news import (...)` at the former start of the
news block, and `from yfmcp.clients.market_providers import _PROVIDER_USER_AGENT`. 43 names are re-exported (41 functions
and 2 constants). A name is re-exported only if `server.py` itself or a script/test/parity case reads it as `server.<name>`.
The six tool handlers are kept because tests and callers use `server.<tool>`; grouped routing resolves handlers from the
FastMCP registry, not from `server` globals.

| Re-export | Read by |
|---|---|
| `_finnhub_policy` | `test_edgar_client_cache.py` |
| `_finnhub_eligibility` | `test_edgar_client_cache.py`, `test_news_events.py` |
| `_enrich_news_item_for_llm` | `test_news_events.py` |
| `_build_coverage` | `cases_pricing_evidence_tools.py`, `test_marketaux_news.py`, `test_news_events.py`, `test_provider_gap_routing.py` |
| `_yahoo_item_issuer` | `cases_news_b.py`, `test_local_data_accuracy.py` |
| `_coerce_max_results` | `test_helpers.py` |
| `_coerce_lookback_days` | `test_helpers.py` |
| `_normalize_event_sources` | `test_helpers.py` |
| `_source_request_fields` | `cases_news_a.py`, `cases_pricing_evidence_tools.py` |
| `_event_type_from_keywords` | `test_helpers.py` |
| `_event_type_from_form` | `test_helpers.py` |
| `_short_text` | `test_helpers.py` |
| `_make_duplicate_group_id` | `test_data_quality.py`, `test_helpers.py` |
| `_parse_form4_transaction` | `test_data_quality.py` |
| `_source_rank` | `test_helpers.py` |
| `_safe_sec_url` | `server.py` itself (search_filing_text); `cases_news_b.py`, `test_helpers.py` |
| `_within_date_window` | `test_helpers.py` |
| `_load_company_ir_page_registry` | `cases_news_a.py` |
| `_company_ir_page_item` | `test_news_events.py` |
| `_parse_company_ir_page_json` | `test_news_events.py` |
| `_parse_company_ir_page_html` | `test_news_events.py` |
| `_collect_company_ir_page_events` | `test_news_events.py` |
| `_build_yahoo_event_item` | `test_news_events.py` |
| `_collect_sec_events` | `test_news_events.py` |
| `_collect_yahoo_events` | `cases_news_b.py`, `test_marketaux_news.py`, `test_news_events.py`, `test_yahoo_press_releases.py` |
| `_yahoo_search_news` | `cases_neutrality.py`, `cases_news_a.py`, `cases_news_b.py`, `test_news_events.py`, `test_yahoo_press_releases.py` |
| `_collect_globenewswire_events` | `test_edgar_client_cache.py`, `test_news_events.py` |
| `_collect_finnhub_events` | `test_edgar_client_cache.py`, `test_marketaux_news.py`, `test_news_events.py`, `test_provider_gap_routing.py` |
| `_collect_marketaux_events` | `test_edgar_client_cache.py`, `test_marketaux_news.py` |
| `_marketaux_fallback_reason` | `test_marketaux_news.py` |
| `_dedupe_event_items` | `test_data_quality.py`, `test_helpers.py` |
| `_build_collection_status` | `test_helpers.py` |
| `_compute_source_status` | `cases_news_a.py`, `test_helpers.py`, `test_marketaux_news.py`, `test_news_events.py`, `test_provider_gap_routing.py` |
| `_compute_source_coverage` | `test_helpers.py`, `test_news_events.py` |
| `_collect_company_events` | `cases_pricing_evidence_tools.py`, `test_earnings_tools.py`, `test_local_data_accuracy.py`, `test_marketaux_news.py`, `test_news_events.py` |
| `search_company_news` | `cases_news_a.py`, `cases_news_b.py`, `cases_pricing_evidence_tools.py`, `test_local_data_accuracy.py`, `test_news_events.py`, `test_triangulation.py` (tool handler) |
| `get_company_press_releases` | `cases_neutrality.py`, `cases_news_a.py`, `cases_news_b.py`, `cases_pricing_evidence_tools.py`, `test_earnings_tools.py`, `test_news_events.py`, `test_triangulation.py`, `test_worker_doctrine_safety.py` (tool handler) |
| `get_sec_recent_events` | `cases_news_a.py`, `cases_news_b.py`, `cases_pricing_evidence_tools.py`, `test_news_events.py`, `test_triangulation.py` (tool handler) |
| `get_public_event_timeline` | `cases_news_a.py`, `cases_news_b.py`, `cases_pricing_evidence_tools.py`, `test_news_events.py`, `test_triangulation.py` (tool handler) |
| `verify_company_event` | `cases_news_a.py`, `cases_news_b.py`, `cases_pricing_evidence_tools.py`, `test_news_events.py`, `test_triangulation.py` (tool handler) |
| `get_company_news` | `server.py` itself (extract_analyst_valuation_methods, get_yahoo_finance_news); `audit_arsenal_news.py`, `cases_news_a.py`, `cases_news_b.py`, `cases_pricing_evidence_tools.py`, `test_grouped_registry.py`, `test_local_data_accuracy.py`, `test_news_events.py`, `test_tools.py` (tool handler) |
| `_GLOBENEWSWIRE_RSS_FEEDS` | `cases_news_a.py`, `test_news_events.py` |
| `_GLOBENEWSWIRE_MAX_BYTES` | `test_news_events.py` |

`server.py` code that still calls moved names: `get_yahoo_finance_news` and `extract_analyst_valuation_methods` call
`get_company_news`; `search_filing_text` calls `_safe_sec_url`.

Not re-exported (38 functions, nothing outside `news.py` reads them): _evidence_class_for, _url_provenance_for, _normalize_event_text, _strip_yahoo_news_legal_suffix, _yahoo_news_identity_from_info, _yahoo_news_match_for, _yahoo_decision_use, _globenewswire_xml_is_safe, _xml_local_name, _xml_child_texts, _xml_first_child_text, _globenewswire_category_values, _globenewswire_stock_category_matches, _globenewswire_plain_text, _mark_no_source_selected, _normalized_event_title_stem, _xml_text, _strip_xml_namespaces, _form4_num, _form4_date, _form4_owner_from_html, _form4_role_from_html, _parse_form4_html_transaction, _form4_transaction_payload, _try_attach_form4_transaction, _company_ir_page_entry, _host_without_www, _company_ir_page_url_allowed, _company_ir_page_registry_fresh, _company_ir_page_json_records, _html_attr, _fetch_company_ir_page, _build_sec_event_item, _normalize_exchange, _is_ticker_compatible_with_context, _verify_coverage_failure_mode, _unpack_company_event_result, _rank_news_items_by_relevance.

Deliberately not re-exported: `_COMPANY_IR_PAGE_REGISTRY_PATH`. It is only ever patched; a copy in `server` would make a
`patch("server._COMPANY_IR_PAGE_REGISTRY_PATH")` silently do nothing. Patch `yfmcp.tools.news._COMPANY_IR_PAGE_REGISTRY_PATH`.

## 4. Tool registration order

The news tools were registered at two places in `server.py`: five contiguous handlers (`search_company_news` ...
`verify_company_event`, tools/list positions 14-18, 1-based, directly after the `provider_gaps` tools) and `get_company_news`
(position 44, 1-based, between the `get_company_events_calendar` and `summarize_options_flow` aliases). One side-effect import
placed before the first `server.py` registration reproduces the first five. `get_company_news`, registered by the same
import, would land at position 19, so `server.py` calls the new `yfmcp.app.reseat_tool_registration(yfinance_server,
"get_company_news", after="get_company_events_calendar")` at its original position. The helper rebuilds the FastMCP
tool-manager dict with the tool directly after the named one (idempotent, so `importlib.reload(server)` keeps the same
order). Expanded and grouped `list_tools` (names, order, descriptions, input and output schemas) and the handler-registry
order are byte-identical before and after.

Why not a call-form registration in `server.py`: `scripts/check_tool_sync.py` finds tools with the regex
`@yfinance_server\.tool\(\s*name\s*=`, so a tool registered as `yfinance_server.tool(...)(fn)` would disappear from the sync check.

## 5. Left alone on purpose

* `server.py` imports that only the moved code used are still there (`functools`, `hashlib`, `Path`, `_ET`, `TTL_NEWS`,
  `_parse_html_table`, `_parse_rss_date`, `_to_iso_utc`, `_utc_now_iso`): pyflakes now reports them unused. They stay
  importable as `server.<name>`; remove them in Phase 4.
* Generic helpers such as `_xml_*`, `_short_text` and `_html_attr` moved too, because the topology assigns them to `#news`.
