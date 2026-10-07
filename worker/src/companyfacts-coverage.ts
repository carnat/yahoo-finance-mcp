/**
 * Whether the filer's latest annual report is in SEC companyfacts (2.5.30, F-015/F-018).
 *
 * SEC's companyfacts file can lack a filed annual report's facts: TSM's FY2025 20-F (0001628280-26-025362) added
 * only two administrative facts, so every companyfacts-backed figure fell back to FY2024. Each path that reads
 * companyfacts raises the same warning when that happens, rather than each discovering it differently.
 * Shared with yfmcp/companyfacts_coverage.py; parity is tested in scripts/test_companyfacts_coverage.py.
 */

type Rec = Record<string, unknown>;

export interface AnnualFilingRef {
  accessionNumber: string;
  form: string;
  filed: string;
  reportDate?: string | null;
}

export const LATEST_ANNUAL_NOT_IN_COMPANYFACTS = "LATEST_ANNUAL_NOT_IN_COMPANYFACTS";
const ANNUAL_FORMS = new Set(["10-K", "20-F", "40-F"]);
// The taxonomies this server reads financial figures from; dei and srt carry administrative facts.
const FINANCIAL_TAXONOMIES = ["us-gaap", "ifrs-full"];

const bare = (accession: string): string => accession.replace(/-/g, "");

/** The annual reports (10-K, 20-F, 40-F; amendments excluded) in SEC submissions' recent filings. */
export function annualFilingsFromSubmissions(submissions: unknown): AnnualFilingRef[] {
  const recent = ((((submissions ?? {}) as Rec).filings as Rec | undefined)?.recent ?? {}) as Record<string, unknown[]>;
  const forms = (recent.form ?? []) as unknown[];
  const out: AnnualFilingRef[] = [];
  for (let i = 0; i < forms.length; i++) {
    const form = String(forms[i] ?? "").toUpperCase();
    const accessionNumber = String(recent.accessionNumber?.[i] ?? "");
    if (!ANNUAL_FORMS.has(form) || !accessionNumber) continue;
    out.push({ accessionNumber, form, filed: String(recent.filingDate?.[i] ?? ""), reportDate: recent.reportDate?.[i] ? String(recent.reportDate[i]) : null });
  }
  return out;
}

/** The newest annual report filed on or before asOf's day. */
export function latestAnnualFiling(filings: AnnualFilingRef[], asOf = "9999-12-31"): AnnualFilingRef | null {
  const day = asOf.slice(0, 10);
  let best: AnnualFilingRef | null = null;
  for (const f of filings) {
    if (!ANNUAL_FORMS.has(String(f.form).toUpperCase()) || !f.filed || f.filed.slice(0, 10) > day) continue;
    if (!best || f.filed > best.filed || (f.filed === best.filed && f.accessionNumber > best.accessionNumber)) best = f;
  }
  return best;
}

function eachFact(companyfacts: unknown, visit: (fact: Rec) => boolean): void {
  const facts = (((companyfacts ?? {}) as Rec).facts ?? {}) as Rec;
  for (const taxonomy of FINANCIAL_TAXONOMIES) {
    for (const concept of Object.values((facts[taxonomy] ?? {}) as Rec)) {
      for (const rows of Object.values((((concept ?? {}) as Rec).units ?? {}) as Rec)) {
        if (!Array.isArray(rows)) continue;
        for (const row of rows) if (row && typeof row === "object" && visit(row as Rec)) return;
      }
    }
  }
}

/** Whether companyfacts holds any us-gaap or ifrs-full fact from the accession. */
export function accessionInCompanyfacts(companyfacts: unknown, accession: string): boolean {
  const target = bare(accession);
  let found = false;
  eachFact(companyfacts, (row) => (found = typeof row.accn === "string" && bare(row.accn) === target));
  return found;
}

/** The newest period end of an annual-report fact companyfacts holds, filed on or before asOf's day. */
function latestAnnualPeriodEnd(companyfacts: unknown, asOf: string): string | null {
  const day = asOf.slice(0, 10);
  let latest: string | null = null;
  eachFact(companyfacts, (row) => {
    const form = String(row.form ?? "").toUpperCase().replace(/\/A$/, "");
    if (ANNUAL_FORMS.has(form) && typeof row.end === "string" && String(row.filed ?? "") <= day && (latest == null || row.end > latest)) latest = row.end;
    return false;
  });
  return latest;
}

/**
 * The LATEST_ANNUAL_NOT_IN_COMPANYFACTS warning when the latest annual report filed by asOf contributes no
 * us-gaap or ifrs-full fact to companyfacts and no annual period companyfacts holds reaches its period end, else
 * null. A filer with no annual report on file gets none.
 */
export function latestAnnualCoverageWarning(filings: AnnualFilingRef[], companyfacts: unknown, asOf = "9999-12-31"): Rec | null {
  const latest = latestAnnualFiling(filings, asOf);
  if (!latest || accessionInCompanyfacts(companyfacts, latest.accessionNumber)) return null;
  const held = latestAnnualPeriodEnd(companyfacts, asOf);
  // An amendment filed since may have put the report's year into companyfacts: the year is there, so no warning.
  if (latest.reportDate && held && held >= latest.reportDate) return null;
  const by = asOf.startsWith("9999") ? "" : ` filed by ${asOf.slice(0, 10)}`;
  const period = latest.reportDate ? `, period ${latest.reportDate}` : "";
  return {
    code: LATEST_ANNUAL_NOT_IN_COMPANYFACTS,
    message: `The latest annual report${by} (${latest.form} ${latest.accessionNumber}, filed ${latest.filed}${period}) has no us-gaap or ifrs-full fact in SEC companyfacts; `
      + `${held ? `the newest annual period companyfacts holds ends ${held}` : "companyfacts holds no annual period"}. Figures here come from earlier reports, not from that filing.`,
    severity: "warning",
    form: latest.form,
    accessionNumber: latest.accessionNumber,
    filed: latest.filed,
    reportDate: latest.reportDate ?? null,
    latestCompanyfactsAnnualPeriodEnd: held,
  };
}
