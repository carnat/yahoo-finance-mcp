# R4b-1 (2.14.0): evidence, decision-grade and telemetry tidy-ups

**Source:** the approved Q3i triage (`Q3I_TRIAGE.md`, 2026-10-18), §3 rows tagged R4b. Per-row code sites and values are in `q3i/*.md`.
**Version:** 2.14.0 (minor). Some results lose `decisionGrade`, and Worker evidence rows gain 6 keys.
**Rule zero:** anything not listed below stays byte-identical in both runtimes, as the parity harness proves.
**Not here:** R4b-2 (2.15.0) carries the accession readers, the outline shape and the evidence-store listing.

## 1. EX-99.1 press-release exhibit resolver (news_a G6a/b/c, triage call #7)

Sites: Python `_resolve_ex991_url` (`yfmcp/tools/earnings.py` ~180) and Worker `resolveEx991Url` (`worker/src/yahoo-finance.ts` ~13567). Both runtimes use one identical rule over the filing-index exhibit rows, taken in index order.

**What is accepted:**
1. The type, upper-cased with whitespace removed, matches `^EX-99\.0?1$` (`EX-99.1` or `EX-99.01`). Python also keeps its existing `99.1` alias, normalised the same way; the Worker adds `^99\.0?1$` to match.
2. The type starts with `EX-99` and the description, upper-cased, matches `PRESS RELEASE|EARNINGS RELEASE|RESULTS RELEASE`.

**What is no longer accepted:**
- a bare `EX-99` whose description lacks that release wording (Python change);
- a match on the document file name alone (Worker change).

**Index-HTML fallback:**
- The Worker keeps its fallback for when no exhibit row matches, tightened to `EX-99\.0?1\b`.
- Python gains the same fallback, reading the index HTML with the existing EDGAR helper and the same regex and byte cap (100,000).

**Tests:**
- Flip the three pinned cases (bare EX-99 with another description, file name only, zero-padded) to `equal` with the values above.
- Add a case: bare `EX-99` with the description "Press Release" is accepted in both runtimes.

## 2. Decision-grade XBRL predicate (other_cases G4, BR-SHR-001)

Sites: `server.py` ~5353 and `worker/src/yahoo-finance.ts` ~11687.
- `xbrlContext` must be a plain object, not an array. The Worker adds the `!Array.isArray` check.
- Every required evidence field must be non-blank after trimming. Python adds `str(v).strip() != ""`.
- The equal-mode cases (an empty `{}` context, a numeric `0` field) are unchanged; they are not part of this release.
- Flip `shr.decision_grade.array_xbrl_context_diverges` and `…whitespace_only_evidence_field_diverges` to `equal`.

## 3. Section-markdown marking, Python only (other_cases G2)

Python's `get_sec_filing_section_markdown` (`server.py` ~6250) marks its output exactly as the Worker does (`yahoo-finance.ts` ~9714–9740):
- `status: "SECTION_MARKDOWN_UNVERIFIED"`;
- `confidence: "NOT_DECISION_GRADE"`;
- `decisionGrade: false`;
- warning `LIVE_SECTION_EXTRACTION_UNRELIABLE`;
- plus `SECTION_MARKDOWN_LOW_CONTENT` when the text is under 50 words.

Use the same keys, positions, messages and severities as the Worker. `doctrineUse` stays Worker-only (triage call #18 and P3). Flip the pinned status/decisionGrade case to `equal`, keeping a pinned remainder for `doctrineUse` only.

## 4. Evidence rows (defects B1, triage call #28)

**4a. One row shape.** The Worker enricher (`worker/src/response.ts` ~209) emits the same 14 keys, in the same order, as Python (`yfmcp/envelope.py` ~273):
`url, sourceType, filingType, accessionNumber, filingDate, publishedAt, retrievedAt, excerpt, concept, periodEnd, tableIndex, rowLabel, columnLabel, rawRow`.
- The string-evidence branch gets the same 14 keys, with nulls.
- Keep today's falsy semantics (`||` / `or`) for every key.

**4b. Table evidence is kept.** In both enrichers, when a raw evidence object has no `tableIndex`, `rowLabel` or `rawRow`:
- `tableIndex` ← `sourceTableId`;
- `rowLabel` ← the first element of `sourceRows` when it is a string;
- `rawRow` ← `sourceRows` joined with `" | "` when it is a non-empty list of strings.

**4c. The producer names its document.** `extract_sec_filing_fact`'s geographic PARSED_TABLE evidence object (`server.py` ~2858, `yahoo-finance.ts` ~6684) adds `documentUrl`, `filingType`, `accessionNumber` and `filingDate`. Take them from the filing the table came from; they are already in scope at that site.

**Result:** a HIGH figure from a parsed table carries a non-null `evidence` row that names its document, table and row.

**Tests:** both runtimes, mode equal.
- table evidence survives standardisation;
- the 14-key order holds;
- string evidence gets 14 keys;
- AAOI-shaped fixture: `extract_sec_filing_fact` on the PARSED_TABLE path returns non-null evidence with `url` set.

## 5. Analyst mixed signal (defects A8, BR-SHR-003/004)

Sites: `server.py` ~4333–4343 and `worker/src/yahoo-finance.ts` ~4859–4864. Test `mixedSignal` before DOWNGRADE.
- A downgrade with a raised target gives `MIXED`; an upgrade with a cut target gives `MIXED`.
- `DOWNGRADE` without a mixed signal gives `BEARISH`. Everything else is unchanged.
- `MIXED` already exists in both runtimes.
- Add equal-mode cases for both mixed directions.

## 6. Stale Python status enum (news_b G19)

`yfmcp/schemas.py` ~214 declares the status enum `[OK, PARTIAL]` in an internal schema that has not been advertised since 2.13.1. Remove that `enum` constraint. No output changes.

## 7. RN wording notes (other_cases G9)

Change the text only, in both runtimes, the parity notice constants (`cases_neutrality.py` ~49–51) and the docs:
- **Price-target deprecation message** (`yfmcp/deprecations.py` ~66, `worker/src/deprecations.ts` ~62): "…use priceAtOrAboveTarget (direction); referenceTargetPct gives the price/target ratio." `replacement` stays `priceAtOrAboveTarget`.
- **`tagNote`** (`server.py` ~4892, `yahoo-finance.ts` ~7755): end with "Use priceAtOrAboveTarget." instead of "Use inferredTag."

## 8. Telemetry ops (defects B6)

- **OPS-TEL-1:** `logDispatchStart` (`worker/src/telemetry.ts` ~159) logs the object itself, not `JSON.stringify(...)`. The fields stay the same: `t, v, action, domain, build`. `test_worker_telemetry.py` asserts the object form and still applies the privacy checks. The export script already parses both forms.
- **OPS-TEL-2:** `scripts/export_telemetry.py` adds `coverage.warnings` with the code `LOGS_ZERO_WITH_REQUESTS` when Analytics Engine counts more than 0 requests but Workers Logs counts 0 invocations. It reports this and exits 0; it is an ops signal, not a failure. Add a test.
- **OPS-TEL-3:** in `worker/wrangler.toml`, add a comment on `[observability] enabled = false` saying that logs are enabled explicitly under `[observability.logs]` and traces stay off. No value changes.
- **OPS-TEL-4:** investigate why logs show 45 invocations against 68 Analytics Engine requests, and the 3 `unknown` outcomes. Report the cause in the PR body. Change the code only if the export mis-joins rows, and keep any such fix inside `export_telemetry.py`.

## 9. Release

- **Docs section "Evidence and Decision-Grade Repairs (2.14.0)":**
  - the exhibit rule;
  - the decision-grade predicate;
  - section-markdown marking;
  - the 14-key evidence row, including the 6 new Worker keys;
  - table evidence;
  - analyst MIXED;
  - the wording changes;
  - telemetry ops.
- **Live checks:**
  - `health_check` reports 2.14.0;
  - `extract_sec_filing_fact` for AAOI on the PARSED_TABLE path returns non-null evidence (when the data path is taken; otherwise report the path that was taken);
  - `get_company_press_releases` for AAPL still finds its EX-99.1 and stays decision-grade;
  - an evidence row from any SEC tool shows the 14 keys;
  - the canaries pass;
  - the telemetry export shows the dispatch line read as an object.
