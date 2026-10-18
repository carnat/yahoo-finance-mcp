"""company_ir provider (registry-only, 2.10.0).

The Python runtime has no separate company_ir collector: ``yfmcp.tools.news._collect_company_events``
serves a requested ``company_ir`` source from the Yahoo feed and the approved company IR page
collector (``yfmcp.news.providers.company_ir_page``), and reports its status in
``_compute_source_status``. That logic is pipeline code, so it stays in ``yfmcp/tools/news.py``
(a pure restructure moves no function body). This module exists to mirror
``worker/src/news/providers/company-ir.ts`` and is where a Python ``company_ir`` collector would live.
"""
