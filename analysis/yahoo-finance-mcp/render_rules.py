#!/usr/bin/env python3
"""Render BUSINESS_RULES.md and DATA_OBJECTS.md from rules.json.

rules.json is the verbatim return value of the modernize-extract-rules workflow
(run wf_36b55a43-b0d, 2026-10-14). Re-run from the repo root:

    python3 analysis/yahoo-finance-mcp/render_rules.py

Rule IDs are deterministic: BR-<DOMAIN>-<NNN>, ordered by priority then name
within each domain. Domains come from the topology extractor (god-file rules are
placed by the region that owns the cited function).
"""
from __future__ import annotations

import ast
import contextlib
import html
import io
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
data = json.loads((HERE / "rules.json").read_text())

# Reuse the topology extractor's domain model (it rewrites topology.json identically).
topo: dict = {"__file__": str(HERE / "extract_topology.py"), "__name__": "extract_topology"}
with contextlib.redirect_stdout(io.StringIO()):
    try:
        exec(compile((HERE / "extract_topology.py").read_text(), topo["__file__"], "exec"), topo)
    except SystemExit:      # the extractor ends with sys.exit(0)
        pass
FILE_DOMAIN, DOMAINS, func_region = topo["FILE_DOMAIN"], topo["DOMAINS"], topo["func_region"]

ABBR = {"transport": "TRN", "envelope": "ENV", "pricing": "PRC", "fundamentals": "FND", "providers": "PRV",
        "sec": "SEC", "extractors": "EXT", "valuation": "VAL", "earnings": "ERN", "news": "NWS",
        "evidence": "EVD", "thai": "THA", "shared": "SHR"}

# server.py: line -> owning top-level function -> region domain
_srv = (ROOT / "server.py").read_text()
_spans = [(n.lineno, n.end_lineno, n.name) for n in ast.parse(_srv).body
          if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]


def clean(s) -> str:
    return html.unescape(str(s or "")).strip()


def cell(s, n=None) -> str:
    s = re.sub(r"\s+", " ", clean(s)).replace("|", "\\|")
    return s if n is None or len(s) <= n else s[: n - 1] + "…"


def src_file(source: str) -> str:
    s = clean(source).replace("legacy/yahoo-finance-mcp/", "").replace("/home/user/yahoo-finance-mcp/", "")
    return re.split(r"[:\s(;,]", s, maxsplit=1)[0]


def domain_of(rule) -> str:
    f = src_file(rule["source"])
    if f == "server.py":
        m = re.search(r"server\.py:(\d+)", clean(rule["source"]))
        if m:
            ln = int(m.group(1))
            for a, b, name in _spans:
                if a <= ln <= b:
                    return func_region[("server.py", name)].split("#", 1)[1]
        return "shared"
    if f == "worker/src/yahoo-finance.ts":
        return "shared"
    return FILE_DOMAIN.get(f, "shared")


PRI = {"P0": 0, "P1": 1, "P2": 2}
rules = data["confirmedRules"]

# Owner corrections (rule_overrides.json) layered over the verbatim workflow output.
overrides = json.loads((HERE / "rule_overrides.json").read_text())
for r in rules:
    card = overrides["cards"].get(r["name"]) if r["priority"] == "P0" else None
    if card:
        r["_panelNote"] = r.pop("smeQuestion", None)
        r.update({k: v for k, v in card.items() if k not in ("ownerDecision", "repair")})
        r["_ownerDecision"] = card["ownerDecision"]
        if card.get("repair"):
            r["_repairRef"] = card["repair"]["ref"]
            r["_repairWhat"] = card["repair"]["what"]
    if r.get("suspectedDefect"):
        r["_repairRef"] = r.get("_repairRef") or overrides["repairs"].get(r["name"])

# When a scheduled repair must ship: before the phase that moves the rule's domain (owner decision Q3i phase rule).
REPAIR_WINDOW = {"news": "before Phase 3", "sec": "before Phase 4", "shared": "before Phase 4", "envelope": "before Phase 4",
                 "transport": "before Phase 4"}


def repair_window(r) -> str:
    if (r.get("_repairRef") or "").startswith("Q2a"):
        return "Phase 1"
    return REPAIR_WINDOW.get(r["_dom"], "before its Phase 5 unit")
by_dom = defaultdict(list)
for r in rules:
    r["_dom"] = domain_of(r)
    by_dom[r["_dom"]].append(r)
dom_order = [d for d in DOMAINS if d in by_dom]
for d in dom_order:
    by_dom[d].sort(key=lambda r: (PRI[r["priority"]], clean(r["name"]).lower()))
    for i, r in enumerate(by_dom[d], 1):
        r["_id"] = f"BR-{ABBR[d]}-{i:03d}"
ordered = [r for d in dom_order for r in by_dom[d]]

st = data["stats"]
p0 = [r for r in ordered if r["priority"] == "P0"]
p0_blockers = [r for r in p0 if r["confidence"] != "High"]
defects = [r for r in ordered if r.get("suspectedDefect")]
sme = [r for r in ordered if r["confidence"] != "High"]
demoted = [r for r in ordered if r.get("p0PanelStatus") and r["priority"] != "P0"]
py_cited = sum(1 for r in ordered if src_file(r["source"]).endswith(".py"))

L = []
w = L.append
w("# Business Rules — yahoo-finance-mcp\n")
w("Generated 2026-10-14 by `/modernize-extract-rules` (workflow run `wf_36b55a43-b0d`, 710 agents, 4 extraction rounds) "
  "and rendered by `render_rules.py` from the verbatim workflow output in `rules.json`. Do not hand-edit rule cards; "
  "owner corrections go in `rule_overrides.json`; anything else means re-running the extraction.\n")
w("## How these rules were produced\n")
w("- **Extract:** each round, three extractors read `legacy/yahoo-finance-mcp/` (symlinks to `server.py`, `tool_groups.py`, `yfmcp/`, `worker/src/`, `docs/`, `tool_catalog.json`), each through a different lens (calculations, validations and policies, lifecycle and status). Duplicates across rounds were dropped.")
w("- **Verify:** a separate referee read each rule's cited code. "
  f"{st['confirmed']} were confirmed (wrong citations were corrected and the rule downgraded to Medium confidence), and {st['rejected']} were refuted (Appendix B).")
w(f"- **P0 panel:** two independent judges, one on compliance and one on fidelity, reviewed each P0 candidate. Of {len(p0) + len(demoted)} candidates, **{len(p0)} stayed P0**. "
  f"{len(demoted)} were downgraded to P1 because the judges did not agree they move money, carry a regulatory duty or guard data integrity. Their reasoning is kept verbatim in each card's SME note.")
w("- **Coverage caveats:**")
w("  - Extraction hit the 4-round cap before it ran dry. Rounds 3 and 4 still found 88 and 119 new rules, so the long tail is incomplete.")
w(f"  - Most rules cite Python: {py_cited} of {len(ordered)}. The Worker (`worker/src/*.ts`) is the parity mirror, and its behaviour is assumed equal unless a card or a suspected defect says otherwise.")
w("  - Suspected defects (§3) are the places where that assumption is already known to fail.")
w("  - No instruction-shaped text was found in the source." if not data["injectionFlags"] else
  f"- **Injection suspects:** {len(data['injectionFlags'])} — see Appendix C.")
w("")
w("**Reading priorities in this system.** This is an analytics MCP server. It files and certifies nothing, and no money moves through it. "
  "Here P0 means **data integrity of published figures**: share counts, dilution, enterprise value, fiscal periods, evidence integrity, and the "
  "rules that decide whether a figure is presented as decision-grade. A silent change to any of these would mislead every downstream analysis. "
  "P1 covers the core calculations and policies. P2 is presentation and convenience.\n")

w("## 1. Summary\n")
w("| Domain | P0 | P1 | P2 | Total | Need SME | Suspected defects |")
w("|---|---:|---:|---:|---:|---:|---:|")
for d in dom_order:
    rs = by_dom[d]
    c = Counter(r["priority"] for r in rs)
    w(f"| {DOMAINS[d]} | {c['P0']} | {c['P1']} | {c['P2']} | {len(rs)} | "
      f"{sum(1 for r in rs if r['confidence'] != 'High')} | {sum(1 for r in rs if r.get('suspectedDefect'))} |")
c = Counter(r["priority"] for r in ordered)
w(f"| **Total** | **{c['P0']}** | **{c['P1']}** | **{c['P2']}** | **{len(ordered)}** | **{len(sme)}** | **{len(defects)}** |\n")
w("By category: " + ", ".join(f"{k} {v}" for k, v in Counter(r["category"] for r in ordered).most_common()) + ".\n")

repairs_sched = [r for r in ordered if r.get("_repairRef")]
w("## Owner decisions applied (2026-10-15)\n")
w("`OWNER_DECISIONS_20261015.md` resolved the brief's open questions. Its corrections live in `rule_overrides.json` and are applied here; "
  "`rules.json` stays the verbatim workflow output. Effects on this document:\n")
w(f"- The 6 P0 cards that were below High confidence were corrected and confirmed by the owner (Q1); each carries an **Owner decision** line.")
w(f"- {len(repairs_sched)} rules carry a **Scheduled repair**: today's behaviour is characterised as the Phase 2 baseline, then repaired "
  "in a separate release that ships before the phase that moves the rule's domain (Q3 / Q3i phase rule). "
  "Suspected defects without a decision are **Q3i triage**: classify as semantic/data-integrity (repair) or cosmetic/intentional (preserve) before their domain moves.\n")

w("## 2. P0 behaviour contract\n")
w(f"These {len(p0)} rules must be proven equivalent, in both runtimes, before any phase that touches their code ships. "
  f"**{len(p0_blockers)}** have confidence below High and need SME confirmation before their phase starts (marked ⚠).\n")
w("| ID | Rule | Category | Source | Confidence |")
w("|---|---|---|---|---|")
for r in p0:
    flag = " ⚠" if r["confidence"] != "High" else ""
    w(f"| {r['_id']} | {cell(r['name'])} | {r['category']} | `{cell(r['source'], 90)}` | {r['confidence']}{flag} |")
w("")

w("## 3. Suspected defects found during extraction\n")
w("While reading the code, the referees and judges noted places where the code contradicts its own documentation, the other runtime, or itself. "
  "Under `CLAUDE.md`, resolving a Python/TS divergence, changing an output field or changing a threshold is a planner decision; the owner made those decisions on 2026-10-15. "
  "A pure-restructuring PR still preserves today's behaviour; each decided repair ships as its own release before the phase that moves its domain. Rows marked **parity** are cases where the two runtimes already disagree.\n")
w("Decision column: the owner decision that governs each item (`OWNER_DECISIONS_20261015.md`) and when its repair ships; **Q3i triage** = not yet classified.\n")
w("| ID | Pri | Rule | Suspected defect | Decision |")
w("|---|---|---|---|---|")
for r in defects:
    d = clean(r["suspectedDefect"])
    tag = "**parity** " if re.search(r"parity|Worker|TS |worker/src|diverge", d, re.I) else ""
    dec = f"{r['_repairRef']}: repair {repair_window(r)}" if r.get("_repairRef") else "Q3i triage"
    w(f"| {r['_id']} | {r['priority']} | {cell(r['name'])} | {tag}{cell(d, 400)} | {dec} |")
w("")

w("## 4. Rule cards\n")
for d in dom_order:
    w(f"### {DOMAINS[d]}\n")
    for r in by_dom[d]:
        w(f"#### {r['_id']} · {clean(r['name'])}\n")
        meta = f"**{r['priority']}** · {r['category']} · Confidence **{r['confidence']}**"
        if r.get("p0PanelStatus"):
            meta += f" · P0 panel {r['p0PanelStatus']}"
        w(meta + f"  \nSource: `{clean(r['source'])}`\n")
        w(clean(r["plainEnglish"]) + "\n")
        w(f"- **Given** {clean(r['given'])}")
        w(f"- **When** {clean(r['when'])}")
        w(f"- **Then** {clean(r['then'])}")
        if r.get("and"):
            w(f"- **And** {clean(r['and'])}")
        if r.get("parameters"):
            w(f"- **Parameters:** {clean(r['parameters'])}")
        if r.get("edgeCases"):
            w("- **Edge cases:**")
            for e in r["edgeCases"]:
                w(f"  - {clean(e)}")
        if r.get("suspectedDefect"):
            w(f"- **Suspected defect:** {clean(r['suspectedDefect'])}")
        if r.get("_ownerDecision"):
            w(f"- **Owner decision (2026-10-15):** {r['_ownerDecision']}")
        if r.get("_repairRef"):
            what = f" {r['_repairWhat']}" if r.get("_repairWhat") else ""
            w(f"- **Scheduled repair ({r['_repairRef']}, ships {repair_window(r)}):**{what or ' see the suspected defect above.'}")
        if r.get("_panelNote"):
            w(f"\n<details><summary>P0 panel note (superseded by the owner decision)</summary>\n\n{clean(r['_panelNote'])}\n\n</details>")
        if r.get("smeQuestion"):
            q = clean(r["smeQuestion"])
            if len(q) > 600:
                w(f"\n<details><summary>SME note (panel reasoning, verbatim)</summary>\n\n{q}\n\n</details>")
            else:
                w(f"- **SME question:** {q}")
        w("")

w("## Appendix A — Rules needing SME confirmation\n")
w(f"{len(sme)} rules have confidence below High. Each one's card states the question to answer.\n")
w("| ID | Pri | Rule | Why |")
w("|---|---|---|---|")
for r in sme:
    w(f"| {r['_id']} | {r['priority']} | {cell(r['name'])} | {cell(r.get('smeQuestion'), 220)} |")
w("")
w("## Appendix B — Rules refuted by the referee\n")
for r in data["rejectedRules"]:
    w(f"- **{clean(r['name'])}** (`{clean(r['source'])}`): {cell(r.get('rejectionReason'), 700)}")
w("")
if data["injectionFlags"]:
    w("## Appendix C — Injection suspects\n")
    for s in data["injectionFlags"]:
        w(f"- {cell(s)}")
(HERE / "BUSINESS_RULES.md").write_text("\n".join(L) + "\n")

# Machine-readable index for the Phase 2 parity harness (scripts/parity/).
index = [{"id": r["_id"], "name": clean(r["name"]), "priority": r["priority"], "category": r["category"],
          "domain": r["_dom"], "source": clean(r["source"]), "repair": r.get("_repairRef"),
          "confidence": r["confidence"]} for r in ordered]
(HERE / "rule_index.json").write_text(json.dumps(index, indent=1) + "\n")

# ------------------------------------------------------------- DATA_OBJECTS
name_to_id = {clean(r["name"]).lower(): r["_id"] for r in ordered}
D = ["# Data Objects — yahoo-finance-mcp\n",
     f"{len(data['dataObjects'])} core records/DTOs catalogued by the extraction workflow's data-object pass (run `wf_36b55a43-b0d`). "
     "Rule references link to `BUSINESS_RULES.md` IDs where the name matched exactly. Unmatched names are shown as text.\n",
     "| Object | Source | Fields |", "|---|---|---:|"]
for o in data["dataObjects"]:
    D.append(f"| [{cell(o['name'])}](#{re.sub(r'[^a-z0-9]+', '-', clean(o['name']).lower()).strip('-')}) | "
             f"`{cell(clean(o['source']).replace('legacy/yahoo-finance-mcp/', ''))}` | {len(o.get('fields', []))} |")
D.append("")
for o in data["dataObjects"]:
    D.append(f"## {clean(o['name'])}\n")
    D.append(f"Source: `{clean(o['source']).replace('legacy/yahoo-finance-mcp/', '')}`\n")
    D.append("| Field | Type | Note |")
    D.append("|---|---|---|")
    for f in o.get("fields", []):
        D.append(f"| `{cell(f.get('name'))}` | `{cell(f.get('type'))}` | {cell(f.get('note'))} |")
    refs = []
    for n in o.get("consumedBy", []) or []:
        rid = name_to_id.get(clean(n).lower())
        refs.append(f"{rid} ({cell(n)})" if rid else cell(n))
    if refs:
        D.append("\n**Used by rules:** " + "; ".join(refs))
    D.append("")
(HERE / "DATA_OBJECTS.md").write_text("\n".join(D) + "\n")

print(f"BUSINESS_RULES.md: {len(ordered)} rules, {len(p0)} P0 ({len(p0_blockers)} below High), "
      f"{len(defects)} suspected defects, {len(sme)} need SME; DATA_OBJECTS.md: {len(data['dataObjects'])} objects")
print("by domain:", {d: len(by_dom[d]) for d in dom_order})
