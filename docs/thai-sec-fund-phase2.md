# Thai SEC Fund API — Phase 2 Roadmap

Thai SEC Fund MCP intentionally exposes only bounded workflows:

- `search_thai_funds` — active-profile discovery that returns candidates without selecting one.
- `get_thai_fund_nav` — exact share-class NAV in a caller-bounded Bangkok-time window.
- `get_thai_fund_nav_batch` — sequential direct-project NAV refresh for a caller-owned list.
- `get_thai_fund_factsheet` — dated statistics, project-scoped top-five holdings, and official URL references.
- `get_thai_fund_dividend_history` — one project-scoped payout page with explicit pagination.

All v1 results are `source: sec_thailand_open_data`,
`evidenceClass: OFFICIAL_REGULATORY_DATA`, and `decisionGrade: false`. The
provider must never infer a share class, claim a project-scoped record is
share-class scoped, fetch a factsheet PDF, or crawl AMC websites.

## Verified v1 Contract Notes

- Base URL: `https://api.sec.or.th/v2/fund`; authentication uses the
  `Ocp-Apim-Subscription-Key` header from `SEC_OPEN_DATA_API_KEY`.
- API filters and pagination are **query-string parameters**. A GET body is not
  part of this provider contract.
- `/general-info/profiles` resolves exact `fund_class_name`; `project_info`
  narrows that profile search. Ambiguous profile matches require the caller to
  supply a project ID.
- `/daily-info/nav` accepts `proj_id`, `fund_class_name`,
  `start_nav_date`, and `end_nav_date`. An explicit NAV `proj_id` is queried
  directly without requiring profile lookup: one returned SEC class (including
  `main`) is accepted as a source-class alias, while multiple classes remain
  an explicit ambiguity. The v1 result selects the maximum returned `nav_date`
  rather than trusting row order. Since 2.25.6 the explicit-project path asks
  for the requested `fund_class_name` first and reads the project unfiltered
  only when that returns no row for the class, so a multi-class project's
  first page can no longer hide the requested class. Non-class funds appear as
  `main` in the data dictionary and `-` in the spec sample; both reach the
  alias path.
- Factsheet records with `latest=true` still retain their own historical
  period/as-of date. They are dated evidence, not live holdings.
- `/factsheet/top5-holdings` and `/daily-info/dividend-history` are project
  scoped. `class_abbr_name` is retained on dividend rows but does not make
  the endpoint share-class scoped.
- URL records are returned only as official references. PDF fetching/parsing is
  intentionally out of scope. `/factsheet/urls` has no `latest` filter, so the
  section is dated by its newest `as_of_date` and lists rows newest first.
  `amc_url_factsheet` is per share class, but `pdf_factsheet` is one document
  for the whole project (the spec says it consolidates every class), so since
  2.25.7 the section's scope is `MIXED`.
- Dividend history row order is undocumented. Since 2.25.7 `dataDate` is
  reported only when one page holds the whole history (no cursor sent and none
  returned); a page of a longer history has `dataDate: null`.
- Top-five holdings are listed by `asset_seq`, since provider row order is not
  documented.
- `PROVIDER_TIMEOUT` and `RATE_LIMIT` failures carry `retryable: true`, as other
  providers' transient failures do.

## Spec Source and Endpoint Inventory Check (2026-10-21)

- The official developer portal is `secopendata.sec.or.th`; the earlier
  `api-portal.sec.or.th` was migrated there and announced to close on
  30 June 2026. The portal's firewall blocks automated and cloud clients,
  including a headless browser, so this contract was checked against an
  unofficial mirror of the portal's spec
  (`github.com/Sitthinut/sec-open-data-api-spec`, captured 2026-05-26).
- The live gateway answers 401 (route exists, key required) for each of the
  21 fund endpoints in that spec and 404 for paths outside it, so the mirror
  matched the gateway on 2026-10-21. Six endpoints are in use; the other 15
  are listed below.

## Verified Discovery and Batch Notes

- `/general-info/profiles` powers bounded `search_thai_funds` with documented
  `project_info`, `company_info`, and `fund_class_name` filters. It returns
  Registered and IPO results separately, with a cursor per status. A search
  candidate never becomes evidence until a caller explicitly supplies its
  `projId` and intended class to a later tool.
- `get_thai_fund_nav_batch` accepts up to 20 explicit `fund_class_name` /
  `proj_id` pairs and makes sequential direct NAV calls. It neither profile-
  searches nor derives portfolio value, wrapper eligibility, tax treatment,
  units, cost, or P&L.

## Deferred Endpoint Inventory

| Endpoint family | Deferred workflows | Why deferred |
| --- | --- | --- |
| `/factsheet/performance`, `/factsheet/benchmarks`, `/factsheet/risk-spectrum`, `/factsheet/asset-allocation` | Performance, benchmark comparison, risk, allocation | Period, benchmark, and freshness semantics need live fixtures before an LLM-facing contract is safe. |
| `/factsheet/dividend-policy`, `/factsheet/fees` | Product comparison | Policy/fee disclosure is distinct from actual dividend history. |
| `/general-info/specifications`, `/general-info/mutual-fund-fees`, `/factsheet/subscription-redemption-minimums`, `/factsheet/subscription-redemption-periods`, `/factsheet/ipos`, `/general-info/involve-parties` | Product discovery and transaction terms | Terms must be modeled independently from market-data/NAV evidence. |
| `/outstanding/portfolio`, `/outstanding/portfolio-asset-type` | Broader portfolio exposure | Pagination, reporting date, and stale-record handling require validation first. |
| `/general-info/amcs` | Fund-house directory | Search already accepts `company_info`; a separate AMC list adds no fund evidence on its own. |

## Promotion Criteria

No phase-two endpoint may become an MCP tool until all of the following are
committed in the same reviewable change:

1. A bounded live canary uses an exact known project/share class and verifies
   response, pagination, date, and scope behavior without printing secrets.
2. A redacted regression fixture covers normal, empty, malformed/provider
   failure, and scope/freshness behavior in both Python and Worker paths.
3. The tool description specifies the exact identity scope, time basis,
   pagination/completeness limit, and an LLM recovery action.
4. Worker/Python schema, grouped catalog, type checks, tool sync, and deployed
   canaries pass.

There is **no automatic promotion** from the SEC API inventory to a public MCP
tool. A documented endpoint is not evidence that its data is current, complete,
class-specific, or suitable for investment decisions.
