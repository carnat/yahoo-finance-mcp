# Triage of the post-approval findings (F1–F20) and the open ops items

**Status:** APPROVED 2026-10-19 (owner; §4). The planner calls in §3 carry recommendations; the owner approves the table as a whole, as with Q3i.
**Source:** `Q3I_TRIAGE.md` §5 (F1–F20) and `q3i/defects_and_inputs.md` (OPS-TEL-2…4).
**Rules:** the Q3i planner rules P1–P5 apply unchanged.
- P1: never overstate.
- P2: a value that exists in either runtime is not "new".
- P3: deprecated fields are not spread to the other runtime.
- P4: existing codes before new ones.
- P5: no new capability inside a repair.

**Already closed:**

| Finding | Closed by |
|---|---|
| F8, F9, the F11 main part | 2.20.0 |
| F15 | 2.20.1 |
| R4-TICKER-AMP | Q3i classified it document-only; it is not reopened here |
| OPS-TEL-1 | 2.14.0 |
| OPS-TEL-2 | 2.14.0 |

## 1. Classification

| # | Finding | Class | Target behaviour | Release |
|---|---|---|---|---|
| F1 | Python `get_sec_filing_outline` without `document_url` resolves the latest primary document, and ignores `accession_number` when `period=latest`. The Worker uses the index outline. | REPAIR → Worker | Python follows the Worker: the index-backed outline, honouring `accession_number`. Ignoring a pinned accession fails open (P1). | R5-sec |
| F2 | Outline titles: the Worker inserts a space when stripping inner tags ("Item 1"); Python inserts nothing ("Item1") | REPAIR → Worker (align-free text) | Python inserts the space | R5-sec |
| F3 | `query_sec_filing_index` with an older pinned accession: the evidence names the latest 10-K, because the revenue read is not pinned | REPAIR | The revenue read uses the pinned accession. Naming the wrong filing overstates provenance (P1). Both runtimes. | R5-extractors |
| F4 | A pinned geographic read with no XBRL revenue fact of the requested form returns `NO_FACT_FOR_ACCESSION` without reading that filing's table | REPAIR | Fall through to the existing table read for the pinned accession. This uses an existing path, so it is not a new capability (P5). Both runtimes. | R5-extractors |
| F5 | `extract_sec_filing_fact`: `meta.decisionGrade` (a static capability flag) contradicts the payload's per-call `decisionGrade` | PLANNER CALL | Recommended: retire the meta flag in the Phase 6 major release, with a deprecation notice from R5-extractors on | Phase 6 |
| F6 | Telemetry: preview-canary requests (about 15 per deploy) plus production canaries and deploy audits appear in Analytics Engine | REPAIR (scripts only) | Every CI canary and audit authenticates with an `Authorization: Bearer` header (`scripts/live_smoke_utils.py`). The owner's connector uses the path transport. Blob 7 (`credentialTransport`) is recorded on every row, including the baseline window, so `credentialTransport = 'header'` identifies synthetic traffic. Add a window-comparison script that queries Analytics Engine for the baseline and comparison windows with header-transport rows excluded, and applies the Phase 4 D10 bounds. The daily export gains the same filtered view. No runtime change. Caveat: once Phase 1b personal keys exist, a real user could use header transport; revisit then. | Phase 4 prep (before 2026-10-24) |
| F7 | Python EDGAR ticker index: a failed load caches `{}` and is not retried for up to 24 h | REPAIR | An empty map counts as not cached, so the next call retries. Check the Worker's `getEdgarTickerCikMap` and align it. | Phase 4 Python routing release (it matters for D2b) |
| F10 | The Worker parses RSS/Atom bodies for structured IR-page entries; Python does not | PLANNER CALL | Recommended: port it to Python (4 functions plus fixtures shared with the Worker), so a registry entry behaves the same locally. Alternative: document it as Worker-only while no registry entry uses a structured feed. | R-news D |
| F11 rest | Unregistered IR-page row keys: the Worker has `registrySchemaVersion` and no `canonicalUrl`; Python has the opposite | REPAIR (align-free) | Both rows carry both keys. Python adds `registrySchemaVersion`; the Worker adds `canonicalUrl: null`. Both keys exist already (P2). | R-news D |
| F12 | Verify stop words: English function words count as meaningful terms, so "the and of" escapes the `INPUT_VALIDATION_ERROR` guard | PLANNER CALL (matching threshold) | Recommended: add a fixed function-word list (a, an, and, are, as, at, by, for, from, in, is, it, its, of, on, or, the, to, with) to the shared stop list in both runtimes. A query of only those words then gets `INPUT_VALIDATION_ERROR`, and matching ignores them. | R-news D |
| F13 | Marketaux statuses: HTTP 404 (Worker `NOT_FOUND`, Python `PROVIDER_ERROR`); non-object body (Worker `PROVIDER_ERROR`, Python `PROVIDER_CHANGED`); legacy-envelope batch error code | REPAIR | **404:** `PROVIDER_ERROR` in both. A provider 404 is not absence of evidence; `NOT_FOUND` would overstate absence (P1). **Non-object body:** `PROVIDER_CHANGED` in both, matching the Finnhub non-array rule. **Legacy envelope:** `normalizeBatchSymbolResult` keeps the entry's `code`. | R-news D |
| F14 | 25 actions in 8 groups accept params their `tool_catalog.json` Params line omits; 2 actions have no Params line | REPAIR (catalog text) | Add the missing params and the 2 Params lines, and empty `_KNOWN_UNADVERTISED_PARAMS`. This is a versioned contract change to a frozen file; text only, with no runtime behaviour change. | R-news D (rides along) |
| F16 | `compare_earnings_actual_vs_estimate` rates Yahoo `estimate.*` rows by the comparison's confidence, while `get_earnings_analysis` rates them LOW | REPAIR | Yahoo estimate rows inside compare get LOW (source-based). The comparison result keeps its own confidence. Both runtimes. | R5-earnings |
| F17 | `get_ownership_holders` row shape: Worker camelCase `value`, Python yfinance `Value` | PLANNER CALL | Recommended: align Python to the Worker shape in the Phase 6 major release. It renames Python output keys, so it is breaking; a deprecation notice goes out in R5-fundamentals. | Phase 6 |
| F18 | Producer-level `?? "HIGH"` / `.get("confidence", "HIGH")` fallbacks (Python 6, Worker 7) | REPAIR (with audit) | Each site either gets an explicit HIGH (a direct tagged read) or falls to LOW, following the R5-shared rules. Also align Python's explicit-`None` handling with TS `??`. | R5-extractors (the sites sit in extractor and exposure code) |
| F19 | `get_analyst_consensus` provenance: Python sets `source`, `evidenceClass`, `decisionGrade`, `recommendedNextAction` and `priceTargets.sourceType: yahoo_finance`; the Worker sets none of them (`sourceType: unknown`) | REPAIR → Python | The Worker sets the same four fields and the source type. The values exist (P2). The pinned case flips to equal. | R5-fundamentals |
| F20 | Status strings (`EXTRACTION_FAILED`, `FILING_NOT_FOUND_TRY_OTHER_TYPE`) stored in `confidence` | DEFER | A data-model change: move them to a status field. This needs new or renamed fields, so it is Phase 6. | Phase 6 |
| OPS-TEL-2 | The export has no coverage guard for Analytics Engine counting requests while Workers Logs counts 0 | CLOSED | Already done in 2.14.0 (`coverage.warnings` `LOGS_ZERO_WITH_REQUESTS`) | — |
| OPS-TEL-3 | `[observability] enabled = false` sits beside `[observability.logs] enabled = true` | REPAIR (config) | Set the flags consistently, or add a comment saying why. It changes runtime logging, so it waits until after the Phase 4 exit comparison, to keep the telemetry windows comparable. | After the Phase 4 exit |
| OPS-TEL-4 | Logged invocations are fewer than Analytics Engine requests | DOCUMENT | Explained (2026-10-19): preview canaries (F6) plus rejected-auth requests, which never log. Recorded in the telemetry docs. | Phase 4 prep docs |

## 2. Release plan

| Release | Contents | Timing |
|---|---|---|
| **Phase 4 prep (scripts only)** | F6, OPS-TEL-4 docs | Before 2026-10-24. No runtime change, no version bump; a tooling PR. |
| **R-news D (2.22.0)** | F11 rest, F13, F12*, F10*, F14 | Now; gates nothing |
| **Phase 4 Python routing release** | F7 | Phase 4, after unit 1 (D2a) |
| **R5-sec** | F1, F2 | Before Phase 5 unit 1 |
| **R5-extractors** | F3, F4, F18 | Before Phase 5 unit 2 |
| **R5-fundamentals** | F19, plus the F17 deprecation notice | Before Phase 5 unit 4 |
| **R5-earnings** | F16 | Before Phase 5 unit 6 |
| **Phase 6 (major)** | F5, F17, F20; OPS-TEL-3 any time after the Phase 4 exit | — |

\* subject to the planner calls in §3.

## 3. Planner calls for the owner

1. **F12:** add the function-word list to the verify stop list (recommended), or keep the current list.
2. **F10:** port RSS/Atom structured parsing to Python (recommended), or document it as Worker-only.
3. **F17:** align the Python holder shape in the Phase 6 major release, with a deprecation notice first (recommended), or keep the two shapes and document them.
4. **F5:** retire `meta.decisionGrade` in Phase 6 (recommended), or derive it per call now.

Everything else follows the existing rules and needs no separate decision.

## 4. Approval

Approved by the owner on 2026-10-19, after "continue with the next triage while waiting". The four planner calls were answered as follows:
- F12: "Add filler words (Recommended)"
- F10: "Port to Python (Recommended)"
- F17: "Align in Phase 6 (Recommended)"
- F5: "Retire in Phase 6 (Recommended)"

**Progress:**
- **F6:** done as a Phase 4 prep tooling commit (`scripts/compare_telemetry_windows.py` and the export's `excludingHeaderTransport` view). No runtime change.
- **R-news D: released as 2.22.0 (#300, from branch `claude/rnews-d`) and live-verified 2026-10-20 03:11Z on build 2.22.0+git.bcc5274.**
  - `verify_company_event` AAPL "the and of" returns `INPUT_VALIDATION_ERROR`.
  - "earnings results" returns CONFIRMED with meaningfulTerms ["earnings"].
  - The company_ir_page row carries `canonicalUrl: null` and `registrySchemaVersion`.
  - `schemaHash` changed, as expected from the F14 catalog text.
  - F10, F11, F12, F13 and F14 are closed. A double-decoding defect in the shared XML decoder was also fixed.
- **Remaining:** F7 (Phase 4 routing release); F1, F2, F3, F4, F16, F18, F19 (R5 releases); F5, F17, F20 (Phase 6); OPS-TEL-3 (after the Phase 4 exit).
