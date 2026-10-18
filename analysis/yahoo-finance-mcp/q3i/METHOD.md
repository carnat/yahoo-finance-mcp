# Q3i triage — shared method for inventory agents (READ-ONLY task)

Repo: /home/user/yahoo-finance-mcp (two runtimes with strict parity: Python `server.py` + `yfmcp/`, Worker `worker/src/*.ts`).
Parity cases live in `scripts/parity/cases_*.py`; a pinned divergence is a case with `mode="pinned"` whose `decision` starts with `FINDING:`.
Context docs: `analysis/yahoo-finance-mcp/BASELINE.md`, `BUSINESS_RULES.md` (§3 table ~line 140), `NEWS_RULES.md`, `MODERNIZATION_BRIEF.md`.

DO NOT edit any repo file. Do not commit. Only write your output file in the scratchpad triage dir.

Owner-approved classification method (A2):
- REPAIR: a divergence (or defect) that changes a published figure, status, `decisionGrade`, confidence, or coverage claim. Pick the SAFER behaviour (the one that never overstates evidence) and align both runtimes to it.
- PRESERVE: text-only divergence (warning wording, timestamp precision, id format). Mark "align-free" if aligning to one runtime is trivial and changes no figure/status; otherwise "document as intentional".

For each item produce one row with:
| id (case name or BR id) | file:line | rule | Python behaviour | Worker behaviour | kind (figure/status/decisionGrade/confidence/coverage/text/input-handling) | proposed class (REPAIR→target behaviour, or PRESERVE align-free / PRESERVE document) | why (one line) | effort S/M/L | release (R-news for news; R4 for SEC/shared/envelope; R5-<unit> for pricing/fundamentals/extractors/valuation) |

Group rows sharing one root cause under a group heading, and give the group a single fix description. Verify each case against current code (cases may have been flipped to `equal` in recent releases 2.12.0/2.13.0 — skip those and say so). Where the safer choice is genuinely unclear, or the fix would add a NEW output field, enum value, warning code or unresolvedReason, mark the row `PLANNER-DECISION` and state the options — do not choose.

End your file with totals: rows, REPAIR, PRESERVE align-free, PRESERVE document, PLANNER-DECISION.
