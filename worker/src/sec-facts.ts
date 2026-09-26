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
