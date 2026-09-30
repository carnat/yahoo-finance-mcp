// SEC companyconcept fact selection across equivalent concepts (2.4.4).
//
// Filers move between equivalent us-gaap concepts: ASTS reported revenue as
// RevenueFromContractWithCustomerExcludingAssessedTax until 2023 and as
// ...IncludingAssessedTax since. Reading the first concept that has any facts
// returned a 2022 quarter as "latest". The concept with the newest filing of
// the requested form wins instead, and a pinned accession must be matched.
//
// yfmcp/sec_facts.py mirrors this file; scripts/test_sec_facts.py requires
// identical output from both.

export const REVENUE_CONCEPTS = [
  "RevenueFromContractWithCustomerExcludingAssessedTax",
  "RevenueFromContractWithCustomerIncludingAssessedTax",
  "Revenues",
  "SalesRevenueNet",
];

export type ConceptFacts = { concept: string; facts: Record<string, unknown>[] };

/**
 * Of several concepts for one fact, the one whose facts for the form were
 * filed most recently (the earlier-listed concept on a tie). With a pinned
 * accession only facts from that filing count, so the first concept tagged in
 * it wins. The returned facts are already limited to the form and accession.
 */
export function pickConceptFacts(
  candidates: ConceptFacts[],
  form: string,
  accession: string | null = null,
  onTie: "first" | "larger" = "first",
): ConceptFacts | null {
  const wantForm = form.toUpperCase();
  const wantAccession = accession ? accession.trim() : "";
  let best: ConceptFacts | null = null;
  let bestFiled = "";
  let bestSize = -1;
  for (const candidate of candidates) {
    if (!Array.isArray(candidate.facts)) continue;
    const rows = candidate.facts.filter((f) =>
      String(f.form ?? "").toUpperCase() === wantForm
      && (!wantAccession || String(f.accn ?? "") === wantAccession));
    if (rows.length === 0) continue;
    const filed = rows.reduce((latest, f) => (String(f.filed ?? "") > latest ? String(f.filed ?? "") : latest), "");
    const size = newestPeriodMagnitude(rows, filed);
    if (best == null || filed > bestFiled || (onTie === "larger" && filed === bestFiled && size > bestSize)) {
      best = { concept: candidate.concept, facts: rows };
      bestFiled = filed;
      bestSize = size;
    }
  }
  return best;
}

/**
 * The largest magnitude a concept reports for the latest period end in its newest filing. For revenue, a
 * filer tagging both Revenues and RevenueFromContractWithCustomer... in one filing (BE: 2,023,994,000 and
 * 2,001,614,000) reports its total under the larger; contract revenue is a part of it (2.5.15).
 */
function newestPeriodMagnitude(rows: Record<string, unknown>[], filed: string): number {
  const inFiling = rows.filter((f) => String(f.filed ?? "") === filed && typeof f.val === "number");
  const end = inFiling.reduce((m, f) => (String(f.end ?? "") > m ? String(f.end ?? "") : m), "");
  return inFiling.filter((f) => String(f.end ?? "") === end).reduce((m, f) => Math.max(m, Math.abs(f.val as number)), -1);
}

function durationDays(fact: Record<string, unknown>): number {
  const start = typeof fact.start === "string" ? fact.start : "";
  const end = typeof fact.end === "string" ? fact.end : "";
  if (!start) return 0;
  const s = Date.parse(`${start}T00:00:00Z`);
  const e = Date.parse(`${end}T00:00:00Z`);
  return Number.isFinite(s) && Number.isFinite(e) ? Math.round((e - s) / 86_400_000) : 1e9;
}

/**
 * A filing's own value for a fact (2.4.5): the first concept tagged in the
 * accession, at the latest period end it reports, and the shortest period
 * there, so a 10-Q gives its quarter rather than the year to date or the
 * prior-year comparative. Any form counts; the old snapshot read 10-K forms only.
 */
export function filingFactInAccession(candidates: ConceptFacts[], accession: string): { concept: string; fact: Record<string, unknown> } | null {
  const want = accession.trim();
  for (const candidate of candidates) {
    if (!Array.isArray(candidate.facts)) continue;
    const rows = candidate.facts.filter((f) => String(f.accn ?? "") === want && typeof f.end === "string" && f.end && f.val != null);
    if (rows.length === 0) continue;
    const latestEnd = rows.reduce((m, f) => (String(f.end) > m ? String(f.end) : m), "");
    let best: Record<string, unknown> | null = null;
    for (const f of rows) {
      if (String(f.end) !== latestEnd) continue;
      if (best == null || durationDays(f) < durationDays(best)) best = f;
    }
    if (best) return { concept: candidate.concept, fact: best };
  }
  return null;
}

// ── Named fiscal-year periods (2.5.16) ──────────────────────────────────────
//
// `period` was honoured only as "latest"; any other string ("FY2025", "2025", a typo) left the rows
// unselected, so the first fact came back: BE's 2016 revenue labelled FY2018 for "FY2025". A period is now
// "latest" or the issuer's fiscal year ("FY2025" or "2025"); anything else is refused.

export type FilingPeriod = { kind: "latest" } | { kind: "fiscalYear"; year: number };

export const FILING_PERIOD_HELP = 'period must be "latest" or a fiscal year ("FY2025" or "2025").';

/** The period a caller named, or null when it is not one this reader resolves. */
export function parseFilingPeriod(raw: string | null | undefined): FilingPeriod | null {
  const text = String(raw ?? "latest").trim();
  if (text === "" || /^latest$/i.test(text)) return { kind: "latest" };
  const m = /^(?:FY\s?)?(\d{4})$/i.exec(text);
  return m ? { kind: "fiscalYear", year: Number(m[1]) } : null;
}

function periodEndYear(end: string): number | null {
  const day = Date.parse(`${end.slice(0, 10)}T00:00:00Z`);
  if (!Number.isFinite(day)) return null;
  // A 52/53-week year ending in the first week of January belongs to the year before.
  return Number(new Date(day - 7 * 86_400_000).toISOString().slice(0, 4));
}

/**
 * The rows for the annual period the issuer calls fiscal `year`: its fy (fp FY) as the filing that first
 * reported the period states it, within a year of the period end, else the year the period ends in (the rule
 * reconcile_metric_sources uses). The latest filed rows come first (a restated value wins). The fiscal years
 * found are listed for a miss.
 */
export function selectFiscalYearRows(rows: Record<string, unknown>[], year: number): { rows: Record<string, unknown>[]; fiscalYears: number[] } {
  const groups = new Map<string, Record<string, unknown>[]>();
  for (const f of rows) {
    if (typeof f.end !== "string") continue;
    const key = `${String(f.start ?? "")}|${f.end}`;
    groups.set(key, [...(groups.get(key) ?? []), f]);
  }
  const years = new Set<number>();
  const matched: Record<string, unknown>[] = [];
  for (const group of groups.values()) {
    const first = [...group].sort((a, b) => String(a.filed ?? "").localeCompare(String(b.filed ?? "")))[0];
    const endYear = periodEndYear(String(first.end));
    if (endYear == null) continue;
    const fy = typeof first.fy === "number" ? first.fy : Number(first.fy);
    const issuerYear = String(first.fp ?? "").toUpperCase() === "FY" && Number.isFinite(fy) && (fy === endYear || fy === endYear - 1) ? fy : endYear;
    years.add(issuerYear);
    if (issuerYear === year) matched.push(...group);
  }
  matched.sort((a, b) => String(b.filed ?? "").localeCompare(String(a.filed ?? "")) || String(b.end ?? "").localeCompare(String(a.end ?? "")));
  return { rows: matched, fiscalYears: [...years].sort((a, b) => a - b) };
}
