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
export function pickConceptFacts(candidates: ConceptFacts[], form: string, accession: string | null = null): ConceptFacts | null {
  const wantForm = form.toUpperCase();
  const wantAccession = accession ? accession.trim() : "";
  let best: ConceptFacts | null = null;
  let bestFiled = "";
  for (const candidate of candidates) {
    const rows = candidate.facts.filter((f) =>
      String(f.form ?? "").toUpperCase() === wantForm
      && (!wantAccession || String(f.accn ?? "") === wantAccession));
    if (rows.length === 0) continue;
    const filed = rows.reduce((latest, f) => (String(f.filed ?? "") > latest ? String(f.filed ?? "") : latest), "");
    if (best == null || filed > bestFiled) {
      best = { concept: candidate.concept, facts: rows };
      bestFiled = filed;
    }
  }
  return best;
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
