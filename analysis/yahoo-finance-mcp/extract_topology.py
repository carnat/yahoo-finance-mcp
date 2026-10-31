#!/usr/bin/env python3
"""Extract the dependency/topology map of yahoo-finance-mcp.

Re-run from the repo root:  python3 analysis/yahoo-finance-mcp/extract_topology.py

Writes analysis/yahoo-finance-mcp/topology.json and prints a summary.

How edges are found (both runtimes):
  * call      - Python `import`/`from` (ast) and TS `import {..} from` statements,
                resolved to the *function* that is imported so edges into a god
                file land on the right region; plus intra-file calls between
                top-level functions of the god files.
  * dispatch  - the MCP router. Python: tool_catalog.json action -> handler name
                -> def in server.py / yfmcp/tools/*. TS: `case "<action>":` arms
                of _dispatchTool (worker/src/tools.ts) -> imported functions
                called inside the arm. Also earnings.py's lazy
                `_server_attr("<name>")` back-calls into server.py (string literal
                resolved), and GitHub Actions jobs -> the scripts they run.
  * read/write - JSON registries, the evidence store (R2 / local dir), the
                Cloudflare edge cache, and upstream HTTP hosts (literal URLs or
                module constants that hold them).

God files (server.py, worker/src/yahoo-finance.ts) are split into per-domain
regions: each tool handler is seeded with its catalog group's domain, the domain
propagates down the intra-file call graph, helpers reached from more than one
domain go to a "shared" region, unreached helpers inherit their nearest
preceding neighbour's domain.
"""
from __future__ import annotations

import ast
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "topology.json"

GOD_FILES = {"server.py": "py", "worker/src/yahoo-finance.ts": "ts"}

DOMAINS = {
    "transport": "Transport & tool registry",
    "envelope": "Envelope, schemas & cross-cutting",
    "pricing": "Yahoo pricing, technicals, options & screening",
    "fundamentals": "Yahoo fundamentals, analyst & calendar",
    "providers": "Third-party fallbacks (key-gated)",
    "sec": "SEC EDGAR retrieval",
    "extractors": "SEC extractors & capital structure",
    "valuation": "Valuation & reconciliation",
    "earnings": "Earnings intelligence & transcripts",
    "news": "News & public events",
    "evidence": "Evidence layer",
    "thai": "Thai SEC funds",
    "shared": "Shared god-file helpers",
    "surface": "MCP tool surface (entry)",
    "ops": "Release & batch operations",
    "data": "Data stores & registries",
    "upstream": "Upstream data providers",
}

GROUP_DOMAIN = {
    "stock_pricing": "pricing", "options_analysis": "pricing", "screening": "pricing",
    "stock_fundamentals": "fundamentals", "analyst_data": "fundamentals",
    "earnings_intelligence": "earnings", "evidence": "evidence",
    "news_events": "news", "sec_filings": "sec", "sec_extractors": "extractors",
    "thai_funds": "thai", "system": "transport",
}

# Per-tool overrides where the catalog group is a presentation choice and the
# implementation clearly belongs to another domain (from /modernize-assess).
ACTION_DOMAIN = {
    "get_uk_company_filings": "providers",
    "get_historical_put_call_ratio": "providers",
    "get_expanded_institutional_ownership": "providers",
    "get_valuation_snapshot": "valuation",
    "get_historical_valuation_context": "valuation",
    "reconcile_metric_sources": "valuation",
}

FILE_DOMAIN = {
    # Python
    "tool_groups.py": "transport", "yfmcp/app.py": "transport", "yfmcp/tools/system.py": "transport",
    "yfmcp/__init__.py": "transport", "yfmcp/tools/__init__.py": "transport",
    "yfmcp/envelope.py": "envelope", "yfmcp/schemas.py": "envelope", "yfmcp/cache.py": "envelope",
    "yfmcp/util.py": "envelope", "yfmcp/validation.py": "envelope", "yfmcp/build_info.py": "envelope",
    "yfmcp/version.py": "envelope",
    "yfmcp/tools/pricing.py": "pricing", "yfmcp/clients/yahoo.py": "pricing",
    "yfmcp/exchange_calendars.py": "pricing", "yfmcp/pricing_notices.py": "pricing",
    "yfmcp/clients/market_providers.py": "providers", "yfmcp/tools/provider_gaps.py": "providers",
    "yfmcp/clients/edgar.py": "sec", "yfmcp/clients/endpoints.py": "sec", "yfmcp/parsing/html.py": "sec", "yfmcp/filing_search.py": "sec",
    "yfmcp/sec_facts.py": "sec", "yfmcp/fiscal_calendar.py": "sec", "yfmcp/companyfacts_coverage.py": "sec",
    "yfmcp/parsing/extractors.py": "extractors", "yfmcp/extraction_rules.py": "extractors",
    "yfmcp/capital_structure.py": "extractors", "yfmcp/share_scenarios.py": "extractors",
    "yfmcp/funding_schedule.py": "extractors", "yfmcp/driver_ledger.py": "extractors",
    "yfmcp/exposure_presets.py": "extractors", "yfmcp/tools/exposure.py": "extractors",
    "yfmcp/valuation.py": "valuation", "yfmcp/valuation_history.py": "valuation",
    "yfmcp/metric_reconciliation.py": "valuation",
    "yfmcp/tools/earnings.py": "earnings", "yfmcp/clients/yahoo_transcripts.py": "earnings",
    "yfmcp/tools/news.py": "news", "yfmcp/tools/sec.py": "sec",
    "yfmcp/news/__init__.py": "news", "yfmcp/news/shared.py": "news", "yfmcp/news/providers/__init__.py": "news",
    "yfmcp/news/providers/company_ir.py": "news", "yfmcp/news/providers/company_ir_page.py": "news",
    "yfmcp/news/providers/finnhub.py": "news", "yfmcp/news/providers/globenewswire.py": "news",
    "yfmcp/news/providers/marketaux.py": "news", "yfmcp/news/providers/sec.py": "news",
    "yfmcp/news/providers/yahoo.py": "news", "yfmcp/url_policy.py": "envelope",
    "yfmcp/earnings_period.py": "earnings", "yfmcp/guidance_history.py": "earnings",
    "yfmcp/evidence.py": "evidence", "yfmcp/evidence_store.py": "evidence",
    "yfmcp/tools/thai_funds.py": "thai", "yfmcp/tools/fundamentals.py": "fundamentals",
    # TypeScript
    "worker/src/index.ts": "transport", "worker/src/mcp.ts": "transport", "worker/src/tools.ts": "transport",
    "worker/src/tool-catalog.ts": "transport", "worker/src/access-keys.ts": "transport",
    "worker/src/auth.ts": "transport", "worker/src/telemetry.ts": "transport", "worker/src/url-policy.ts": "envelope",
    "worker/src/response.ts": "envelope", "worker/src/request-context.ts": "envelope",
    "worker/src/cache.ts": "envelope", "worker/src/validate.ts": "envelope",
    "worker/src/filing-search.ts": "sec", "worker/src/sec-facts.ts": "sec",
    "worker/src/fiscal-calendar.ts": "sec", "worker/src/companyfacts-coverage.ts": "sec",
    "worker/src/sec.ts": "sec",
    "worker/src/extraction-rules.ts": "extractors", "worker/src/capital-structure.ts": "extractors",
    "worker/src/share-scenarios.ts": "extractors", "worker/src/funding-schedule.ts": "extractors",
    "worker/src/driver-ledger.ts": "extractors", "worker/src/exposure.ts": "extractors",
    "worker/src/valuation.ts": "valuation", "worker/src/valuation-history.ts": "valuation",
    "worker/src/metric-reconciliation.ts": "valuation",
    "worker/src/guidance-history.ts": "earnings", "worker/src/earnings-period.ts": "earnings",
    "worker/src/transcript-contract.ts": "earnings",
    "worker/src/evidence.ts": "evidence", "worker/src/evidence-store.ts": "evidence",
    "worker/src/news.ts": "news", "worker/src/deprecations.ts": "envelope", "yfmcp/deprecations.py": "envelope",
    "worker/src/evidence-pack.ts": "evidence",
    "worker/src/sec-thailand.ts": "thai",
    "worker/src/clients/edgar.ts": "sec",
    "worker/src/clients/yahoo.ts": "pricing", "worker/src/clients/provider-http.ts": "providers",
    "worker/src/exchange-calendars.ts": "pricing", "worker/src/pricing-notices.ts": "pricing",
    "worker/src/pricing.ts": "pricing", "worker/src/fundamentals.ts": "fundamentals", "worker/src/providers.ts": "providers",
    "worker/src/clients/html.ts": "sec", "worker/src/clients/endpoints.ts": "sec",
    "worker/src/util.ts": "envelope", "worker/src/batch.ts": "envelope", "worker/src/news/identity.ts": "news",
    "worker/src/news/shared.ts": "news", "worker/src/news/providers/types.ts": "news",
    "worker/src/news/providers/company-ir.ts": "news", "worker/src/news/providers/company-ir-page.ts": "news",
    "worker/src/news/providers/finnhub.ts": "news", "worker/src/news/providers/globenewswire.ts": "news",
    "worker/src/news/providers/marketaux.ts": "news", "worker/src/news/providers/sec.ts": "news",
    "worker/src/news/providers/yahoo.ts": "news",
    # Ops scripts that run in production workflows (tests are excluded)
    "scripts/stamp_worker_build.py": "ops", "scripts/worker_version_promotion.py": "ops",
    "scripts/discover_ir_page_candidates.py": "ops", "scripts/audit_arsenal_news.py": "ops",
    "scripts/check_tool_sync.py": "ops",
}

# Upstream hosts -> logical datastore id (hostnames only; never URLs with params).
HOSTS = {
    "www.sec.gov": "sec-edgar", "data.sec.gov": "sec-edgar", "efts.sec.gov": "sec-edgar-fts",
    "query1.finance.yahoo.com": "yahoo-finance", "query2.finance.yahoo.com": "yahoo-finance",
    "finance.yahoo.com": "yahoo-finance", "fc.yahoo.com": "yahoo-finance",
    "www.globenewswire.com": "newswire-rss", "news.cision.com": "newswire-rss",
    "finnhub.io": "finnhub", "www.alphavantage.co": "alpha-vantage", "api.marketaux.com": "marketaux",
    "api.company-information.service.gov.uk": "uk-companies-house",
    "find-and-update.company-information.service.gov.uk": "uk-companies-house",
    "api.sec.or.th": "thai-sec-open-data",
}
UPSTREAM_NAMES = {
    "sec-edgar": "SEC EDGAR (www/data.sec.gov)", "sec-edgar-fts": "SEC full-text search (efts)",
    "yahoo-finance": "Yahoo Finance", "newswire-rss": "Newswire RSS (GlobeNewswire/Cision)",
    "finnhub": "Finnhub", "alpha-vantage": "Alpha Vantage", "marketaux": "Marketaux",
    "uk-companies-house": "UK Companies House", "thai-sec-open-data": "Thai SEC Open Data",
    "company-ir-sites": "Company IR sites / caller URLs (open fetch)",
}
HOST_RE = re.compile(r"https?://([a-zA-Z0-9.-]+)")

REGISTRIES = {
    "tool_catalog.json": ("ds:tool_catalog", "tool_catalog.json"),
    "company-ir-page-registry.json": ("ds:ir_registry", "company-ir-page-registry.json"),
    "news-source-capabilities.json": ("ds:news_capabilities", "news-source-capabilities.json"),
    "exposure-presets.json": ("ds:exposure_presets", "exposure-presets.json"),
    "exchange-calendars.json": ("ds:exchange_calendars", "exchange-calendars.json"),
}

# ----------------------------------------------------------------- helpers

def rel(p: Path) -> str:
    return p.relative_to(ROOT).as_posix()


def nloc(text: str) -> int:
    return sum(1 for ln in text.splitlines() if ln.strip())


nodes: dict[str, dict] = {}        # id -> leaf
edges: set[tuple[str, str, str]] = set()
unresolved: list[str] = []


def add_node(nid, name, kind, domain, **kw):
    if nid not in nodes:
        nodes[nid] = {"id": nid, "name": name, "kind": kind, "_domain": domain, **kw}
    return nid


def add_edge(s, t, kind):
    if s and t and s != t:
        edges.add((s, t, kind))


def hosts_in(text: str) -> set[str]:
    """Upstream hosts and JSON registries referenced by a piece of source."""
    out = {dsid for key, (dsid, _) in REGISTRIES.items() if key in text}
    for h in HOST_RE.findall(text):
        if h in HOSTS:
            out.add("ds:" + HOSTS[h])
    return out


# --------------------------------------------------- catalog / dispatch map
catalog = json.loads((ROOT / "tool_catalog.json").read_text())["groups"]
action_group: dict[str, str] = {}
action_handler: dict[str, str] = {}
for g, spec in catalog.items():
    for action, handler in spec["actions"].items():
        action_group[action] = g
        action_handler[action] = handler


def action_domain(action: str) -> str:
    return ACTION_DOMAIN.get(action) or GROUP_DOMAIN[action_group[action]]


for g in catalog:
    add_node(f"tool:{g}", g, "screen", "surface")

# ------------------------------------------------------------- source scan
py_files = sorted([ROOT / "server.py", ROOT / "tool_groups.py", *ROOT.glob("yfmcp/**/*.py")])
py_files += [ROOT / p for p in FILE_DOMAIN if p.startswith("scripts/")]
# Recursive: worker/src/news/** and worker/src/clients/** (Phase 4) are domain modules too.
ts_files = sorted(p for p in (ROOT / "worker/src").rglob("*.ts") if not p.name.endswith(".d.ts"))


# -- function-level model of a file: name -> (start, body)
def py_functions(src: str, tree: ast.Module):
    funcs = {}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            funcs[n.name] = (n.lineno, ast.get_source_segment(src, n) or "", n)
    return funcs


TS_DEF = re.compile(r"^(?:export\s+)?(?:async\s+)?function\s+([A-Za-z_$][\w$]*)|^(?:export\s+)?const\s+([A-Za-z_$][\w$]*)\s*(?::[^=]+)?=\s*(?:async\s*)?(?:\([^)]*\)|[A-Za-z_$][\w$]*)\s*(?::[^=]+)?=>", re.M)


def ts_functions(src: str):
    lines = src.splitlines()
    starts = []
    for i, ln in enumerate(lines):
        m = TS_DEF.match(ln)
        if m:
            starts.append((i, m.group(1) or m.group(2)))
    funcs = {}
    for k, (i, name) in enumerate(starts):
        j = starts[k + 1][0] if k + 1 < len(starts) else len(lines)
        funcs[name] = (i + 1, "\n".join(lines[i:j]), None)
    return funcs


IDENT = re.compile(r"\b[A-Za-z_$][\w$]*\b")
TS_IMPORT = re.compile(r"^import\s+(type\s+)?(?:\{([^}]*)\}|([A-Za-z_$][\w$]*))\s+from\s+\"([^\"]+)\"", re.M)


def ts_resolve(from_file: Path, spec: str) -> str | None:
    if not spec.startswith("."):
        return None
    tgt = (from_file.parent / spec).resolve()
    for cand in (tgt, tgt.with_suffix(".ts"), Path(str(tgt).removesuffix(".js") + ".ts")):
        if cand.exists() and cand.is_file():
            return rel(cand)
    return None


# Per file: imported local name -> (target file, original symbol)
imports: dict[str, dict[str, tuple[str, str | None]]] = defaultdict(dict)
functions: dict[str, dict] = {}
sources: dict[str, str] = {}
consts_hosts: dict[str, dict[str, set[str]]] = defaultdict(dict)

for p in py_files:
    if not p.exists():
        continue
    f = rel(p)
    src = p.read_text(encoding="utf-8-sig")
    sources[f] = src
    tree = ast.parse(src)
    functions[f] = py_functions(src, tree)
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module and (n.module.startswith("yfmcp") or n.module in ("server", "tool_groups")):
            base = n.module.replace(".", "/")
            for a in n.names:
                sub = ROOT / base / f"{a.name}.py"
                if sub.exists():   # from yfmcp import evidence as _ev
                    imports[f][a.asname or a.name] = (rel(sub), None)
                else:
                    tgt = ROOT / f"{base}.py"
                    if not tgt.exists():
                        tgt = ROOT / base / "__init__.py"
                    imports[f][a.asname or a.name] = (rel(tgt), a.name)
        elif isinstance(n, ast.Import):
            for a in n.names:
                if a.name.startswith("yfmcp") or a.name in ("server", "tool_groups"):
                    tgt = ROOT / (a.name.replace(".", "/") + ".py")
                    if tgt.exists():
                        imports[f][a.asname or a.name.split(".")[0]] = (rel(tgt), None)
    for n in tree.body:   # module constants holding upstream URLs
        if isinstance(n, ast.Assign):
            seg = ast.get_source_segment(src, n) or ""
            hs = hosts_in(seg)
            if hs:
                for t in n.targets:
                    if isinstance(t, ast.Name):
                        consts_hosts[f][t.id] = hs

for p in ts_files:
    f = rel(p)
    src = p.read_text(encoding="utf-8")
    sources[f] = src
    functions[f] = ts_functions(src)
    for m in TS_IMPORT.finditer(src):
        if m.group(1):       # import type - erased at runtime
            continue
        tgt = ts_resolve(p, m.group(4))
        if not tgt:
            continue
        if m.group(3):
            imports[f][m.group(3)] = (tgt, None)
        for part in (m.group(2) or "").split(","):
            part = part.strip()
            if not part or part.startswith("type "):
                continue
            orig, _, alias = part.partition(" as ")
            imports[f][(alias or orig).strip()] = (tgt, orig.strip())
    for m in re.finditer(r"^(?:export\s+)?const\s+([A-Za-z_$][\w$]*)\b[^\n]*", src, re.M):
        hs = hosts_in(m.group(0))
        if hs:
            consts_hosts[f][m.group(1)] = hs

# -------------------------------------------------- god-file region split
func_region: dict[tuple[str, str], str] = {}   # (file, func) -> node id


def region_id(f: str, dom: str) -> str:
    return f"{f}#{dom}"


# Planner decision D1 (c), PHASE4_SPEC.md §11a: a tool handler or tool-only helper that the call-graph rule put in
# "shared" only because two or more domains call it belongs to its catalog domain. It is domain code, not hub code,
# and moves with its Phase 5 unit. Each entry seeds that function's domain; helpers only it calls follow it.
DOMAIN_OVERRIDES: dict[str, dict[str, str]] = {
    "server.py": {
        # The SEC seeds left with their code for yfmcp/tools/sec.py and worker/src/sec.ts (Phase 5 unit 1, 3.6.1).
        # The SEC extractor seeds left for yfmcp/tools/exposure.py (Phase 5 unit 2, 3.6.2). The earnings-release reads
        # (_earnings_release_picks, _read_release_text) moved there too, since the driver ledger calls them; they join
        # the earnings module in unit 6.
        # The analyst_data / stock_fundamentals seeds (get_earnings_momentum, get_analyst_upgrade_radar and the consensus
        # helpers) left for yfmcp/tools/fundamentals.py (Phase 5 unit 4).
    },
    "worker/src/yahoo-finance.ts": {
        # The SEC extractor seeds left with their code for worker/src/exposure.ts (Phase 5 unit 2).
        # snapshotComponentMessage left with the pricing code for worker/src/pricing.ts (Phase 5 unit 3).
        # fetchAlphaVantageJson left with the provider code for worker/src/providers.ts (Phase 5 unit 5).
        "earningsReleasePicks": "earnings", "readReleaseText": "earnings",
    },
}


def seed_handlers(f: str) -> dict[str, str]:
    seeds = {}
    if GOD_FILES[f] == "py":
        for action, handler in action_handler.items():
            if handler in functions[f]:
                seeds[handler] = action_domain(action)
        if "get_server" in functions[f]:
            seeds["get_server"] = "transport"
    for name, dom in DOMAIN_OVERRIDES.get(f, {}).items():
        if name not in functions[f]:
            raise SystemExit(f"DOMAIN_OVERRIDES names {name}, which {f} no longer defines")
        seeds[name] = dom
    return seeds


def ts_dispatch_arms():
    """action -> set of (file, symbol) called in its _dispatchTool case arm."""
    src = sources["worker/src/tools.ts"]
    body = functions["worker/src/tools.ts"]["_dispatchTool"][1]
    arms = re.split(r"^\s*case\s+\"", body, flags=re.M)[1:]
    out = defaultdict(set)
    imp = imports["worker/src/tools.ts"]
    local = functions["worker/src/tools.ts"]
    for arm in arms:
        action = arm.split('"', 1)[0]
        ids = set(IDENT.findall(arm))
        for i in ids:
            if i in imp:
                if FILE_DOMAIN.get(imp[i][0]) == "envelope":
                    continue    # failure/validation helpers, not the handler
                out[action].add(imp[i])
            elif i in local and i != "_dispatchTool":
                out[action].add(("worker/src/tools.ts", i))
    return out, src


arms, _ = ts_dispatch_arms()

for f, lang in GOD_FILES.items():
    funcs = functions[f]
    names = set(funcs)
    calls = {n: (set(IDENT.findall(body)) & names) - {n} for n, (_, body, _) in funcs.items()}
    seeds = seed_handlers(f)
    if lang == "ts":
        for action, targets in arms.items():
            for tf, sym in targets:
                if tf == f and sym in funcs and action in action_group:
                    seeds.setdefault(sym, action_domain(action))
        # functions imported by sibling modules are entry points into the god file
    # Domain = domain of the *direct* callers (fixpoint from the seeded handlers).
    callers = defaultdict(set)
    for n, cs in calls.items():
        for c in cs:
            callers[c].add(n)
    dom_of: dict[str, str] = dict(seeds)
    for _ in range(60):
        changed = False
        for n in funcs:
            if n in seeds:
                continue
            cd = {dom_of[c] for c in callers[n] if c in dom_of}
            if not cd:
                continue
            real = cd - {"shared"}
            d = next(iter(real)) if len(real) == 1 else "shared"
            if dom_of.get(n) != d:
                dom_of[n] = d
                changed = True
        if not changed:
            break
    order = sorted(funcs, key=lambda n: funcs[n][0])
    last = "transport" if lang == "py" else "pricing"
    for n in order:
        dom = dom_of.get(n, last)   # unreached helpers inherit their neighbour
        if dom != "shared":
            last = dom
        func_region[(f, n)] = region_id(f, dom)

    loc_by_region = defaultdict(int)
    for n, (_, body, _) in funcs.items():
        loc_by_region[func_region[(f, n)]] += nloc(body)
    for rid, loc in loc_by_region.items():
        dom = rid.split("#", 1)[1]
        add_node(rid, f"{Path(f).name} · {DOMAINS[dom].split(' (')[0]}", "module", dom,
                 language="python" if lang == "py" else "typescript", loc=loc, file=f)
    # intra-file region calls
    for n, cs in calls.items():
        for c in cs:
            add_edge(func_region[(f, n)], func_region[(f, c)], "call")

# ----------------------------------------------------- plain module nodes
for f, src in sources.items():
    if f in GOD_FILES:
        continue
    dom = FILE_DOMAIN.get(f)
    if dom is None:
        if f.endswith("__init__.py") and not src.strip():
            continue
        unresolved.append(f"no domain for {f}")
        dom = "shared"
    if not src.strip():
        continue
    add_node(f, Path(f).name, "module", dom,
             language="python" if f.endswith(".py") else "typescript", loc=nloc(src), file=f)


def target_node(tfile: str, sym: str | None) -> str | None:
    if tfile in GOD_FILES:
        if sym and (tfile, sym) in func_region:
            return func_region[(tfile, sym)]
        return region_id(tfile, "transport") if region_id(tfile, "transport") in nodes else None
    return tfile if tfile in nodes else None


def owner_nodes(f: str):
    """yield (node id, text, used identifiers) per ownership unit of a file."""
    if f in GOD_FILES:
        for n, (_, body, _) in functions[f].items():
            yield func_region[(f, n)], body, set(IDENT.findall(body))
    elif f in nodes:
        src = sources[f]
        yield f, src, set(IDENT.findall(src))


# ----------------------------------------------------------- call edges
for f in sources:
    imp = imports.get(f, {})
    for nid, text, used in owner_nodes(f):
        for local_name, (tfile, sym) in imp.items():
            if local_name in used:
                reg = REGISTRIES.get(Path(tfile).name)
                if reg:                      # import x from "./registry.json"
                    add_edge(nid, reg[0], "read")
                    continue
                if sym is None and tfile in GOD_FILES:
                    # module alias into a god file: resolve attribute uses alias.fn
                    for attr in re.findall(rf"\b{re.escape(local_name)}\.([A-Za-z_]\w*)", text):
                        add_edge(nid, target_node(tfile, attr), "call")
                else:
                    add_edge(nid, target_node(tfile, sym), "call")

# earnings.py lazy back-calls into server.py, resolved from string literals
for nid, text, _ in owner_nodes("yfmcp/tools/earnings.py"):
    for sym in re.findall(r"_server_attr\(\"([A-Za-z_]\w*)\"\)", text):
        t = target_node("server.py", sym)
        if t:
            add_edge(nid, t, "dispatch")
        else:
            unresolved.append(f"_server_attr('{sym}') not found in server.py")

# ------------------------------------------------------- dispatch edges
py_handler_files = ["server.py"] + [rel(p) for p in sorted(ROOT.glob("yfmcp/tools/*.py"))]
for action, handler in action_handler.items():
    g = f"tool:{action_group[action]}"
    found = False
    for f in py_handler_files:
        if handler in functions.get(f, {}):
            add_edge(g, func_region.get((f, handler), f), "dispatch")
            found = True
            break
    if not found:
        unresolved.append(f"python handler '{handler}' for {action}")
    targets = arms.get(action, set())
    if not targets:
        unresolved.append(f"worker dispatch arm for {action}")
    for tf, sym in targets:
        t = target_node(tf, sym)
        if t and not (tf == "worker/src/tools.ts"):
            add_edge(g, t, "dispatch")
        elif tf == "worker/src/tools.ts":
            add_edge(g, "worker/src/tools.ts", "dispatch")

# routers hold the surface
for g in catalog:
    add_edge("tool_groups.py", f"tool:{g}", "dispatch")
    add_edge("worker/src/tools.ts", f"tool:{g}", "dispatch")

# ------------------------------------------------------ data stores
for key, (dsid, name) in REGISTRIES.items():
    add_node(dsid, name, "datastore", "data")
add_node("ds:evidence_r2", "R2 bucket yfmcp-evidence", "datastore", "data")
add_node("ds:evidence_localdir", "Local evidence dir (YFMCP_EVIDENCE_DIR)", "datastore", "data")
add_node("ds:edge_cache", "Cloudflare edge cache (caches.default)", "datastore", "data")
add_node("ds:wrangler_vars", "wrangler.toml [vars] build stamp", "datastore", "data")
for uid, uname in UPSTREAM_NAMES.items():
    add_node(f"ds:{uid}", uname, "datastore", "upstream")

for f in sources:
    for nid, text, used in owner_nodes(f):
        if nid.startswith("scripts/"):
            pass
        for key, (dsid, _) in REGISTRIES.items():
            if key in text or (key == "company-ir-page-registry.json" and "company_ir_page_registry" in text.lower()):
                add_edge(nid, dsid, "read")
        hs = hosts_in(text)
        for cname, chs in consts_hosts.get(f, {}).items():
            if cname in used:
                hs |= chs
        for h in hs:
            add_edge(nid, h, "read")
        if f.endswith(".py") and nid not in ("yfmcp/clients/yahoo.py",) and re.search(r"\byf\.(Ticker|download|Search|Tickers|Screener|screen)\b", text):
            add_edge(nid, "ds:yahoo-finance", "read")
        # evidence store
        if f == "worker/src/evidence-store.ts":
            add_edge(nid, "ds:evidence_r2", "read"); add_edge(nid, "ds:evidence_r2", "write")
        elif f == "yfmcp/evidence_store.py":
            add_edge(nid, "ds:evidence_localdir", "read"); add_edge(nid, "ds:evidence_localdir", "write")
        else:
            store = "ds:evidence_r2" if f.endswith(".ts") else "ds:evidence_localdir"
            if re.search(r"\b(putOnce|put_once)\b", text):
                add_edge(nid, store, "write")
            if re.search(r"\b(getEvidenceStore|get_store)\s*\(", text) and re.search(r"\.(get|list)\(", text):
                add_edge(nid, store, "read")
        # edge cache
        if re.search(r"\bcache\.match\(|\bedgeCache\(\)", text):
            add_edge(nid, "ds:edge_cache", "read")
        if re.search(r"\bcache\.put\(", text):
            add_edge(nid, "ds:edge_cache", "write")
        # open fetchers: caller- or registry-supplied URLs
        if re.search(r"\b(fetchPublicHtml|_fetch_public_html|fetchCompanyIrText)\s*\(", text) and not re.search(r"function (fetchPublicHtml|fetchCompanyIrText)|def _fetch_public_html", text[:200]):
            add_edge(nid, "ds:company-ir-sites", "read")

# writers of build/registry state
add_edge("scripts/stamp_worker_build.py", "ds:wrangler_vars", "write")
add_edge("scripts/discover_ir_page_candidates.py", "ds:ir_registry", "write")
for f in ("worker/src/index.ts",):
    add_edge(f, "ds:wrangler_vars", "read")

# ------------------------------------------------------ entry points: jobs
entry = {"worker/src/index.ts", region_id("server.py", "transport")}
entry |= {f"tool:{g}" for g in catalog}
for wf in sorted((ROOT / ".github/workflows").glob("*.yml")):
    text = wf.read_text()
    jid = f"job:{wf.stem}"
    trig = "cron" if "schedule:" in text else ("push/PR" if "pull_request" in text or "push:" in text else "manual")
    add_node(jid, f"{wf.stem} ({trig})", "job", "ops", file=rel(wf))
    entry.add(jid)
    for s in sorted(set(re.findall(r"python3? (?:\.\./)?scripts/([A-Za-z_]+\.py)", text))):
        sp = f"scripts/{s}"
        if sp in nodes:
            add_edge(jid, sp, "dispatch")
    if "wrangler versions upload" in text or "wrangler deploy" in text:
        add_edge(jid, "worker/src/index.ts", "dispatch")
        add_edge(jid, "ds:wrangler_vars", "read")

# ------------------------------------------------------ prune & dead ends
for s, t, _ in list(edges):
    for x in (s, t):
        if x not in nodes:
            unresolved.append(f"edge endpoint not a node: {x}")
edges = {e for e in edges if e[0] in nodes and e[1] in nodes}

inbound = defaultdict(int)
for s, t, _ in edges:
    inbound[t] += 1
dead = sorted(n for n, v in nodes.items()
              if v["kind"] == "module" and n not in entry and inbound[n] == 0
              and not n.endswith("__init__.py"))   # package inits load implicitly
# Suppression: anything reachable only by string-keyed dispatch we could not resolve
dyn_suspects = {u.split("'")[1] for u in unresolved if "'" in u}
dead = [d for d in dead if not any(s in d for s in dyn_suspects)]

# ---------------------------------------------------- tree & observations
tree = {"id": "sys", "name": "yahoo-finance-mcp", "kind": "system", "children": []}
by_dom = defaultdict(list)
for n in nodes.values():
    leaf = {k: v for k, v in n.items() if not k.startswith("_")}
    by_dom[n["_domain"]].append(leaf)
for dom, label in DOMAINS.items():
    if by_dom.get(dom):
        tree["children"].append({"id": f"dom:{dom}", "name": label, "kind": "domain",
                                 "children": sorted(by_dom[dom], key=lambda x: x["id"])})

def degree(nid, kinds=("call", "dispatch")):
    return sum(1 for s, t, k in edges if t == nid and k in kinds)

writers = defaultdict(set)
for s, t, k in edges:
    if k == "write":
        writers[t].add(s)

shared_py = nodes.get(region_id("server.py", "shared"), {}).get("loc", 0)
shared_ts = nodes.get(region_id("worker/src/yahoo-finance.ts", "shared"), {}).get("loc", 0)
sec_readers = sorted({s for s, t, k in edges if t == "ds:sec-edgar"})
open_fetch = sorted({s for s, t, k in edges if t == "ds:company-ir-sites"})
back_calls = sorted({t for s, t, k in edges if s == "yfmcp/tools/earnings.py" and k == "dispatch"})
py_reads_worker = sorted({s for s, t, k in edges if t in ("ds:ir_registry", "ds:news_capabilities", "ds:exposure_presets", "ds:exchange_calendars") and s.endswith(".py") or (t in ("ds:ir_registry", "ds:news_capabilities", "ds:exposure_presets", "ds:exchange_calendars") and "server.py" in s)})

observations = [
    f"Both god files are hubs, not layers: their 'shared' regions (server.py {shared_py} NLOC, yahoo-finance.ts {shared_ts} NLOC) are helpers reached from 2+ domains (Yahoo client, EDGAR fetch, HTML/table parsing). Extract these first — every domain module split out later will otherwise import back into the god file.",
    f"SEC EDGAR is read from {len(sec_readers)} separate nodes across both runtimes (direct URL use, not through one client). It is the single most important upstream and a single point of failure; consolidating behind yfmcp/clients/edgar.py and one TS edgar module would make fair-access/User-Agent policy enforceable in one place.",
    f"Open-fetch surface: {len(open_fetch)} nodes fetch caller- or registry-supplied URLs ({', '.join(open_fetch)}). These are the S-01/S-04 SSRF paths from ASSESSMENT.md and should sit behind one allow-listed fetcher per runtime.",
    f"Circular dependency: yfmcp/tools/earnings.py back-calls server.py through string-keyed _server_attr(...) — resolved here to {', '.join(back_calls) or 'none'}. These edges are invisible to static import analysis and block extracting earnings without first moving those targets out of server.py.",
    "The Python runtime reads the Worker's JSON registries (company-ir-page-registry.json, news-source-capabilities.json, exposure-presets.json, exchange-calendars.json) from worker/src at run time, so Python packaging depends on the Worker tree; the IR registry has a second writer (the daily ir-source-discovery job).",
    "Extraction candidates (cleanest seams): News & public events (one region per god file, ~3.3k lines each, no dedicated module either side) and the Thai SEC funds pair (already isolated in both runtimes, only touched by the router).",
]
if unresolved:
    observations.append(f"Unresolved during extraction ({len(unresolved)}): " + "; ".join(sorted(set(unresolved))[:6]))

# ---------------------------------------------------------------- flows
def pick(*cands):
    return [c for c in cands if c in nodes]

YT = "worker/src/yahoo-finance.ts"
flows = [
    {"name": "An investor checks a company's latest earnings and guidance",
     "persona": "Investor using Claude with the hosted connector",
     "description": "The investor asks for the latest quarterly results and management's outlook; the Worker finds the 8-K press release on SEC EDGAR, extracts the figures and guidance, and returns them with provenance.",
     "steps": [
         {"label": "Request arrives at the public MCP endpoint", "nodes": pick("worker/src/index.ts", "worker/src/mcp.ts")},
         {"label": "Router picks the earnings tool", "nodes": pick("worker/src/tools.ts", "tool:earnings_intelligence")},
         {"label": "Find the latest earnings press release", "nodes": pick(region_id(YT, "earnings"), region_id(YT, "sec"), "ds:sec-edgar")},
         {"label": "Pin the fiscal period it reports", "nodes": pick("worker/src/earnings-period.ts", "worker/src/fiscal-calendar.ts")},
         {"label": "Extract reported metrics and guidance", "nodes": pick("worker/src/extraction-rules.ts", "worker/src/guidance-history.ts")},
         {"label": "Return the answer with source and warnings", "nodes": pick("worker/src/response.ts")},
     ]},
    {"name": "An analyst builds a valuation evidence pack",
     "persona": "Equity analyst preparing a valuation",
     "description": "The analyst asks for a citable evidence pack; the system assembles capital structure, dilution, guidance and consensus, stores an immutable evidence cut, and returns its id.",
     "steps": [
         {"label": "Router picks the evidence tool", "nodes": pick("worker/src/tools.ts", "tool:evidence")},
         {"label": "Gather capital structure and dilution from filings", "nodes": pick("worker/src/evidence-pack.ts", "worker/src/capital-structure.ts", region_id(YT, "extractors"), "ds:sec-edgar")},
         {"label": "Add analyst consensus and estimates", "nodes": pick(region_id(YT, "fundamentals"), region_id(YT, "providers"), "ds:yahoo-finance", "ds:alpha-vantage")},
         {"label": "Grade evidence quality and authority", "nodes": pick("worker/src/evidence.ts")},
         {"label": "Store an immutable evidence cut", "nodes": pick("worker/src/evidence-store.ts", "ds:evidence_r2")},
         {"label": "Return the pack and cut id", "nodes": pick("worker/src/response.ts")},
     ]},
    {"name": "A desktop user asks what's new about a company",
     "persona": "Claude Desktop user running the local server",
     "description": "On their own machine the user asks for recent news; the local Python server gathers Yahoo, newswire, registry-approved IR pages and optional Finnhub/Marketaux items, de-duplicates them and reports coverage.",
     "steps": [
         {"label": "Local server receives the request over stdio", "nodes": pick(region_id("server.py", "transport"), "yfmcp/app.py")},
         {"label": "Grouped router resolves news_events", "nodes": pick("tool_groups.py", "tool:news_events")},
         {"label": "Collect items from each source", "nodes": pick(region_id("server.py", "news"), "ds:yahoo-finance", "ds:newswire-rss", "ds:finnhub", "ds:marketaux")},
         {"label": "Check approved company IR pages", "nodes": pick("ds:ir_registry", "ds:company-ir-sites")},
         {"label": "Wrap results in the standard envelope", "nodes": pick("yfmcp/envelope.py")},
     ]},
    {"name": "A maintainer ships a new release",
     "persona": "Repository owner merging to main",
     "description": "A merge to main uploads an immutable candidate Worker version, verifies it with contract canaries, then promotes it to production and re-verifies.",
     "steps": [
         {"label": "Deploy job starts on merge", "nodes": pick("job:deploy-worker")},
         {"label": "Check tool lists and version contract", "nodes": pick("scripts/check_tool_sync.py")},
         {"label": "Stamp build identity", "nodes": pick("scripts/stamp_worker_build.py", "ds:wrangler_vars")},
         {"label": "Upload candidate and verify canaries", "nodes": pick("scripts/worker_version_promotion.py", "worker/src/index.ts")},
         {"label": "Promote to 100% and re-verify production", "nodes": pick("scripts/worker_version_promotion.py")},
     ]},
]

topology = {
    "system": "yahoo-finance-mcp",
    "root": tree,
    "edges": [{"source": s, "target": t, "kind": k} for s, t, k in sorted(edges)],
    "entryPoints": sorted(e for e in entry if e in nodes),
    "deadEnds": dead,
    "observations": observations,
    "flows": flows,
}
OUT.write_text(json.dumps(topology, indent=1) + "\n")

# ---------------------------------------------------------------- summary
kinds = defaultdict(int)
for _, _, k in edges:
    kinds[k] += 1
print(f"wrote {rel(OUT)}")
print(f"nodes: {len(nodes)}  ({sum(1 for n in nodes.values() if n['kind']=='module')} modules/regions, "
      f"{sum(1 for n in nodes.values() if n['kind']=='datastore')} datastores, "
      f"{sum(1 for n in nodes.values() if n['kind']=='screen')} tool groups, "
      f"{sum(1 for n in nodes.values() if n['kind']=='job')} jobs)")
print("edges: " + ", ".join(f"{k}={v}" for k, v in sorted(kinds.items())))
print(f"entry points: {len(topology['entryPoints'])}")
print("\nGod-file regions (NLOC):")
for n in sorted(nodes.values(), key=lambda x: (x.get("file", ""), -x.get("loc", 0))):
    if "#" in n["id"]:
        print(f"  {n['id']:<45} {n['loc']:>6}")
print("\nTop inbound (call+dispatch):")
for nid in sorted(nodes, key=lambda x: -degree(x))[:12]:
    print(f"  {degree(nid):>3}  {nid}")
print("\nData stores — readers / writers:")
for nid, n in sorted(nodes.items()):
    if n["kind"] == "datastore":
        r = sum(1 for s, t, k in edges if t == nid and k == "read")
        w = sum(1 for s, t, k in edges if t == nid and k == "write")
        print(f"  {nid:<28} read={r:<3} write={w}")
print(f"\nDead-end candidates ({len(dead)}): " + (", ".join(dead) or "none"))
print(f"\nUnresolved ({len(set(unresolved))}):")
for u in sorted(set(unresolved))[:20]:
    print("  - " + u)
print("\nObservations:")
for o in observations:
    print("  * " + o)
sys.exit(0)
