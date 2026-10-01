// The fiscal period an earnings release states for itself (2.5.19).
//
// A period is read only from one phrase that joins a quarter to a year: "fourth quarter and full year of
// fiscal 2026", "fiscal 2026 fourth quarter", "Q4 FY2026", "FOURTH-QUARTER AND FULL-YEAR 2026". The quarter
// and the year may not be joined across a sentence, headline or dash: MU's release headline "...position
// Micron for a record fiscal 2027" sits 140 characters before "results for its fourth quarter", and the
// earlier 180-character window read that as FY2027 Q4. The first phrase in the release wins (comparative
// prior-year figures come later; AEHR is a concrete case). Mirrored by yfmcp/earnings_period.py.

export type EarningsPeriodInfo = { period: string | null; periodStatus: "EX99_TEXT_RESOLVED" | "UNRESOLVED"; periodEvidence: string | null };

const QUARTER_WORDS: Record<string, string> = {
  first: "Q1", "1st": "Q1", second: "Q2", "2nd": "Q2", third: "Q3", "3rd": "Q3", fourth: "Q4", "4th": "Q4",
};
const QW = "(first|1st|second|2nd|third|3rd|fourth|4th)";

const PATTERNS: { re: RegExp; quarter: number; year: number; numeric: boolean }[] = [
  // fourth quarter [and full (fiscal) year] [of] [fiscal (year) | FY] 2026
  {
    re: new RegExp(`\\b${QW}[\\s-]+quarter(?:\\s+(?:and|&)\\s+(?:full[\\s-]+)?(?:fiscal[\\s-]+)?year)?(?:\\s+of)?(?:\\s+(?:fiscal(?:\\s+year)?|FY))?\\s*(20\\d{2})\\b`, "i"),
    quarter: 1, year: 2, numeric: false,
  },
  // fiscal (year) 2026 [up to 40 characters, no sentence, headline or dash break] fourth quarter
  {
    re: new RegExp(`\\b(?:fiscal\\s+(?:year\\s+)?|FY\\s*)(20\\d{2})[^.!?;:\\u2022\\u2013\\u2014|]{0,40}?\\b${QW}[\\s-]+quarter\\b`, "i"),
    quarter: 2, year: 1, numeric: false,
  },
  // Q4 [of] fiscal (year) 2026 | Q4 FY2026
  { re: /\bQ([1-4])\s*(?:of\s+)?(?:fiscal\s+(?:year\s+)?|FY\s*)(20\d{2})\b/i, quarter: 1, year: 2, numeric: true },
  // fiscal Q4 [of] 2026
  { re: /\bfiscal\s+Q([1-4])\s*(?:of\s+)?(20\d{2})\b/i, quarter: 1, year: 2, numeric: true },
];

function compactExcerpt(text: string, maxLen: number): string {
  const t = text.replace(/\s+/g, " ").trim();
  return t.length <= maxLen ? t : `${t.slice(0, maxLen).trimEnd()}...`;
}

export function extractEarningsPeriodFromText(text: string): EarningsPeriodInfo {
  const normalized = String(text ?? "").replace(/\s+/g, " ").trim();
  let best: { match: RegExpMatchArray; quarter: string; year: string } | null = null;
  for (const p of PATTERNS) {
    const match = normalized.match(p.re);
    if (!match || match.index == null) continue;
    if (best && (best.match.index ?? 0) <= match.index) continue;
    const q = p.numeric ? `Q${match[p.quarter]}` : QUARTER_WORDS[match[p.quarter].toLowerCase()];
    best = { match, quarter: q, year: match[p.year] };
  }
  if (!best) return { period: null, periodStatus: "UNRESOLVED", periodEvidence: null };
  return {
    period: `FY${best.year} ${best.quarter}`,
    periodStatus: "EX99_TEXT_RESOLVED",
    periodEvidence: compactExcerpt(best.match[0], 220),
  };
}
