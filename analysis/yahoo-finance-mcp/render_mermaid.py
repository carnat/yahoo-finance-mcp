#!/usr/bin/env python3
"""Render call-graph.mmd, data-lineage.mmd and critical-path.mmd from topology.json.

Run after extract_topology.py:  python3 analysis/yahoo-finance-mcp/render_mermaid.py
"""
import json
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
t = json.loads((HERE / "topology.json").read_text())
leaf, dom = {}, {}


def walk(n, d=None):
    if n["kind"] == "domain":
        d = n
    if "children" in n:
        for c in n["children"]:
            walk(c, d)
    elif n["kind"] != "system":
        leaf[n["id"]] = n
        dom[n["id"]] = d


walk(t["root"])
dn = {d["id"]: d["name"] for d in t["root"]["children"]}
key = lambda i: dom[i]["id"].split(":")[1]

# call-graph.mmd — domain level, capped at 40 edges, entry domains highlighted
agg, kinds = Counter(), defaultdict(set)
for e in t["edges"]:
    if e["kind"] in ("call", "dispatch") and leaf[e["target"]]["kind"] != "datastore":
        a, b = key(e["source"]), key(e["target"])
        if a != b:
            agg[(a, b)] += 1
            kinds[(a, b)].add(e["kind"])
top = sorted(agg.items(), key=lambda x: (-x[1], x[0]))[:40]
used = {x for (a, b), _ in top for x in (a, b)}
L = ["%% Domain-level call/dispatch graph (both runtimes). Generated from topology.json.",
     "%% Edge label = number of module/region-level edges (top 40). Dotted = dispatch only (router, jobs, string-keyed calls).",
     "graph TD"]
L += [f'  {d}["{dn["dom:" + d]}"]' for d in sorted(used)]
for (a, b), v in top:
    L.append(f"  {a} {'-.->' if kinds[(a, b)] == {'dispatch'} else '-->'}|{v}| {b}")
entry = sorted({key(e) for e in t["entryPoints"]} & used)
L += ["  classDef entry fill:#cc785c,stroke:#8a4a34,color:#fff", "  class " + ",".join(entry) + " entry"]
(HERE / "call-graph.mmd").write_text("\n".join(L) + "\n")

# data-lineage.mmd — domains -> stores, read vs write
# Collapse closely related stores so the diagram stays near 40 edges.
MERGE = {"ds:sec-edgar-fts": "ds:sec-edgar", "ds:evidence_localdir": "ds:evidence_r2", "ds:news_capabilities": "ds:ir_registry"}
MERGED_NAME = {"ds:sec-edgar": "SEC EDGAR (incl. full-text search)", "ds:evidence_r2": "Evidence store (R2 / local dir)",
               "ds:ir_registry": "IR-page + news-source registries"}
for k, v in MERGED_NAME.items():
    leaf[k] = {**leaf[k], "name": v}
c2 = Counter((key(e["source"]), MERGE.get(e["target"], e["target"]), e["kind"]) for e in t["edges"] if e["kind"] in ("read", "write"))
sid = lambda s: s.replace("ds:", "ds_").replace("-", "_")
L = ["%% Domains -> data stores and upstream providers. --> read, ==> write. Generated from topology.json.", "graph LR",
     "  subgraph Code"]
L += [f'    {d}["{dn["dom:" + d]}"]' for d in sorted({x[0] for x in c2})]
L += ["  end", "  subgraph Stores"]
L += [f'    {sid(s)}[("{leaf[s]["name"]}")]' for s in sorted({x[1] for x in c2})]
L.append("  end")
pairs = defaultdict(set)   # collapse read+write into one edge per (domain, store)
for (a, s, k) in c2:
    pairs[(a, s)].add(k)
for (a, s), ks in sorted(pairs.items()):
    lab = "read/write" if len(ks) == 2 else next(iter(ks))
    L.append(f"  {a} {'==>' if 'write' in ks else '-->'}|{lab}| {sid(s)}")
(HERE / "data-lineage.mmd").write_text("\n".join(L) + "\n")

# critical-path.mmd — primary persona flow
f = t["flows"][0]
L = [f"%% Critical path: {f['name']} ({f['persona']}).",
     "%% No production telemetry is available (see ASSESSMENT.md) - p50/p99 not annotated.", "flowchart TD"]
for i, s in enumerate(f["steps"], 1):
    names = "<br/>".join(leaf[n]["name"] for n in s["nodes"])
    L.append(f'  S{i}["{i}. {s["label"]}<br/><small>{names}</small>"]')
    if i > 1:
        L.append(f"  S{i-1} --> S{i}")
(HERE / "critical-path.mmd").write_text("\n".join(L) + "\n")
print(f"call-graph: {len(top)} edges, data-lineage: {len(pairs)} edges, critical-path: {len(f['steps'])} steps")
