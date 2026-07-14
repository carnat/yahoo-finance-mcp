# Thai SEC Fund API — Phase 2 Roadmap

Thai SEC Fund MCP v1 intentionally exposes only three bounded workflows:

- `get_thai_fund_nav` — exact share-class NAV in a caller-bounded Bangkok-time window.
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
  narrows an explicitly supplied `proj_id`. Ambiguous classes require the
  caller to supply a project ID.
- `/daily-info/nav` accepts `proj_id`, `fund_class_name`,
  `start_nav_date`, and `end_nav_date`; the v1 result selects the maximum
  returned `nav_date` rather than trusting row order.
- Factsheet records with `latest=true` still retain their own historical
  period/as-of date. They are dated evidence, not live holdings.
- `/factsheet/top5-holdings` and `/daily-info/dividend-history` are project
  scoped. `class_abbr_name` is retained on dividend rows but does not make
  the endpoint share-class scoped.
- URL records are returned only as official references. PDF fetching/parsing is
  intentionally out of scope.

## Deferred Endpoint Inventory

| Endpoint family | Deferred workflows | Why deferred |
| --- | --- | --- |
| `/factsheet/performance`, `/factsheet/benchmarks`, `/factsheet/risk-spectrum`, `/factsheet/asset-allocation` | Performance, benchmark comparison, risk, allocation | Period, benchmark, and freshness semantics need live fixtures before an LLM-facing contract is safe. |
| `/factsheet/dividend-policy`, `/factsheet/fees` | Product comparison | Policy/fee disclosure is distinct from actual dividend history. |
| `/general-info/specifications`, `/general-info/subscription-redemption-minimums`, `/general-info/subscription-redemption-periods`, `/general-info/ipos`, `/general-info/involve-parties` | Product discovery and transaction terms | Terms must be modeled independently from market-data/NAV evidence. |
| `/outstanding/portfolio`, `/outstanding/portfolio-asset-type` | Broader portfolio exposure | Pagination, reporting date, and stale-record handling require validation first. |

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
