"""company_ir provider (registry-only, 2.10.0; ported to Python in 2.19.0).

Mirrors ``worker/src/news/providers/company-ir.ts``: the ``company_ir`` source reads approved company IR registry
entries only. There is no Yahoo-feed pass-through, no open-web discovery and no "used" marking without an approved
entry. Without one the source is NOT_REGISTERED, the same status an unregistered ``company_ir_page`` source reports;
with one it delegates to the approved company IR page collector.
"""

from yfmcp.news.providers import company_ir_page as _ir_page
from yfmcp.news.providers.company_ir_page import _CompanyIrPageResult


async def _collect_company_ir_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
) -> _CompanyIrPageResult:
    entry = _ir_page._company_ir_page_entry(ticker)
    if not entry or str(entry.get("status") or "") != "approved":
        registry = _ir_page._load_company_ir_page_registry()
        return _CompanyIrPageResult(([], [], False), {
            "registryVersion": registry.get("registryVersion"),
            "registrySchemaVersion": registry.get("schemaVersion"),
            "status": "NOT_REGISTERED",
            "approvalStatus": "not_registered",
            "allowedAction": "registry_review_required",
        })
    return await _ir_page._collect_company_ir_page_events(
        ticker,
        retrieved_at=retrieved_at,
        max_results=max_results,
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
    )
