# R4b-2 (2.15.0): requested accessions honoured, outline shape, evidence-store newest listing

**Source:** the approved Q3i triage (`Q3I_TRIAGE.md`, 2026-10-18), the R4b rows still open after 2.14.0. Per-row detail is in `q3i/defects_and_inputs.md` (B3, B4, A7).
**Version:** 2.15.0 (minor):
- explicit accessions are honoured where they were ignored;
- a missing filing fails closed;
- Python's outline shape changes.

**Rule zero:** anything not listed below stays byte-identical in both runtimes, as the parity harness proves.

## 1. Accession readers (B3: R4-ACC-GEO, R4-ACC-QUERY, R4-ACC-EXHIBITS)

### 1a. `extract_geographic_revenue`

Sites: Python `server.py` ~6355–6430 and Worker `worker/src/yahoo-finance.ts` ~11260–11330.

**When a non-empty `accession_number` is given:**
- **Pass it into the filing read:** Python `get_filing_data(..., accession_number=...)`; Worker `getFilingData(..., pinAccession)`, which already has the parameter. The figure, the evidence and the optional index (`detailLevel=raw`) then all come from that filing.
- **The 20-F retry also stays pinned.** If the pinned filing is not a 20-F, skip the 20-F retry.
- **On a miss, fail closed.** If the accession is not among the company's recent filings, return the Q3a envelope with no fallback to the latest filing:
  `{ ok:false, error:{ code:"ACCESSION_NOT_FOUND", message:"Accession <acc> was not found in <TICKER>'s recent SEC filings.", requestedAccession:"<acc>", retryable:false } }`
  Reuse the existing Q3a helpers (`isAccessionNotFound`, the resolver miss), not new ones.

**Unchanged:**
- The internal `getFilingData` pinned-miss path used by **`extract_sec_filing_fact`** keeps its legacy payload, per the 2.13.0 decision. Only `extract_geographic_revenue`, and the `query_sec_filing_index` geographic branch that calls it, turn the miss into the error.
- With no accession given, behaviour is unchanged.

### 1b. `query_sec_filing_index`

The geographic query type goes through 1a, so it inherits the fix. The china query already applies the Q3a check. Add one parity case per query type with an accession: one hit and one miss.

### 1c. `list_sec_filing_exhibits` and `get_sec_filing_exhibit_content`

Sites: Python `yfmcp/tools/earnings.py` ~854–880 with `yfmcp/clients/edgar.py` ~186–194; Worker `yahoo-finance.ts` ~9750–9775.
- **When the filing index for (CIK, accession) returns HTTP 404,** return `ACCESSION_NOT_FOUND` with the message "Accession <acc> was not found in <TICKER>'s SEC filings." plus the same `requestedAccession` and `retryable:false`. Today the result is an empty success.
- **Do not use the recent-filings resolver here.** Older real accessions must keep working.
- **Other index-fetch failures** (timeouts, 5xx) keep their current behaviour.

## 2. `get_sec_filing_outline` shape (B4, triage call #29)

Sites: Python `server.py` ~905–929; Worker `yahoo-finance.ts` ~8500–8540. Both runtimes return one shape. Keys, in order:

`ticker, accessionNumber, documentUrl, outline, status, tableCount, warnings`

- **`status`:** `OK` (headings found), `OUTLINE_NOT_PARSED` (no headings but tables found), or `EMPTY` (neither). These are the Worker's existing values.
- **`tableCount`** and the warning `TABLES_FOUND_OUTLINE_EMPTY`, as the Worker emits them today.
- **Item-heading fallback:** Python gains the Worker's `filingItemHeadings` fallback, so filings without `<hN>` headings still get an outline.
- **Sanitising:** Python keeps its HTML sanitising of outline titles. The Worker adopts it if its titles can carry markup today. If it doesn't sanitise and the outputs differ, mirror Python's sanitiser.
- **`accessionNumber`:** the resolved filing's accession, or null when only a `document_url` was given.
- **`documentUrl`:** the document actually read.
- **Errors:** both runtimes use the standard V2 `ok:false` envelope with the error codes Python already uses for the same failures (an accession miss stays `ACCESSION_NOT_FOUND`). The Worker's bare `{error:true, message}` goes away. If a Worker failure has no matching Python code, STOP and report it; do not invent a code.

Flip any pinned outline cases to `equal`, and remove the outline-shape masking in `cases_accession.py` (`outline_view` ~134) so the full shape is compared.

## 3. Evidence-store listing newest first (A7, BR-EVD-041)

Sites: `yfmcp/evidence_store.py` ~62–90 and `worker/src/evidence-store.ts` ~66–75. Callers: `server.py` ~8941/9003/9186 and `worker/src/evidence-pack.ts` ~220/274/460.
- **Today:** a list stops at `limit` (1,000 cuts / 400 history days) in key order (ascending, oldest first). Beyond the cap, the newest entries disappear while the listing claims newest-first.
- **Fix:** page through the full listing (R2 `cursor` / local directory), then keep the newest `limit` entries in the order the callers already expect.
- **Safety cap:** read at most 10 pages of 1,000 keys. If entries remain after the cap, keep the newest of those read. That cap is far beyond current volumes. If the code already has a truncation or partial flag for listings, set it. If it doesn't, STOP and report rather than add a field.
- **Tests:** a store holding more than `limit` entries returns the newest `limit`, in both runtimes.

## 4. Release

- **Docs section "Requested Accessions, Outline Shape and Evidence Listing (2.15.0)".**
- **Live checks:**
  - `health_check` reports 2.15.0;
  - `extract_geographic_revenue` for AAOI with its 10-K accession `0001437749-26-005875` reads that filing;
  - the same call with `0000320193-99-999999` returns `ACCESSION_NOT_FOUND`;
  - `list_sec_filing_exhibits` for AAPL with that fake accession returns `ACCESSION_NOT_FOUND`;
  - with a real 8-K accession, it still lists the exhibits;
  - `get_sec_filing_outline` for AAPL returns the new key set;
  - the canaries pass.
