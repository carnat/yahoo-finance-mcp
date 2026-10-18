/**
 * SEC provider for the news tools: recent filings from submissions, with the Form 4 transaction parser.
 * Moved out of news.ts in 2.15.1 (pure restructure); providers never import news.ts.
 */

import { edgarGetHtml, edgarTickerMapLoaded, getSubmissionsForTicker, stripHtmlTags, _str } from "../../yahoo-finance.js";
import {
  parseHtmlTable,
  sourceDecisionUse,
  normalizeIso,
  shortText,
  makeDupGroupId,
  withinDateWindow,
} from "../shared.js";
import type { NewsProviderResult } from "./types.js";

function eventTypeFromForm(formType: string): string {
  const f = formType.toUpperCase();
  if (f === "10-Q" || f === "10-K") return "earnings";
  if (f === "S-3" || f === "S-1" || f === "424B") return "financing";
  if (f === "8-K" || f === "DEF14A" || f === "PRE14A") return "regulatory";
  if (f === "4") return "insider";
  return "other";
}
function xmlTag(xml: string, path: string[]): string | null {
  let scope = xml;
  for (const tag of path) {
    const re = new RegExp(`<(?:\\w+:)?${tag}\\b[^>]*>([\\s\\S]*?)<\\/(?:\\w+:)?${tag}>`, "i");
    const m = scope.match(re);
    if (!m) return null;
    scope = m[1];
  }
  return scope.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim() || null;
}
function xmlBlock(xml: string, tag: string): string | null {
  const m = xml.match(new RegExp(`<(?:\\w+:)?${tag}\\b[^>]*>([\\s\\S]*?)<\\/(?:\\w+:)?${tag}>`, "i"));
  return m ? m[1] : null;
}
const FORM4_TRANSACTION_LABELS: Record<string, string> = {
  P: "Purchase",
  S: "Sale",
  A: "Grant/Award",
  D: "Disposition",
  M: "Option exercise/conversion",
  F: "Tax withholding/payment",
  G: "Gift",
};
function form4Num(value: string | null): number | null {
  if (value == null) return null;
  const text = stripHtmlTags(value);
  const m = text.match(/-?\$?\s*\(?\d[\d,]*(?:\.\d+)?\)?/);
  if (!m) return null;
  let s = m[0].replace(/\$/g, "").replace(/,/g, "").replace(/\s+/g, "");
  const negative = s.startsWith("(") && s.endsWith(")");
  s = s.replace(/[()]/g, "");
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return negative ? -n : n;
}
function form4Date(value: string | null): string | null {
  const text = stripHtmlTags(value ?? "");
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const m = text.match(/\b(\d{1,2})\/(\d{1,2})\/(\d{4})\b/);
  if (!m) return text;
  return `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
}
function form4Payload(
  owner: string | null,
  role: string | null,
  code: string | null,
  shares: number | null,
  price: number | null,
  ownershipForm: string | null,
  transactionDate: string | null,
): Record<string, unknown> {
  return {
    owner,
    role,
    transactionCode: code,
    transactionLabel: FORM4_TRANSACTION_LABELS[_str(code).toUpperCase()] ?? "Other/Unclassified",
    shares,
    price,
    value: shares != null && price != null ? Math.round(shares * price * 100) / 100 : null,
    ownershipForm,
    transactionDate,
  };
}
function form4OwnerFromHtml(html: string): string | null {
  const m = html.match(/Name and Address of Reporting Person[\s\S]*?<a\b[^>]*>([\s\S]*?)<\/a>/i);
  return m ? stripHtmlTags(m[1]) || null : null;
}
function form4RoleFromHtml(html: string): string | null {
  const m = html.match(/Relationship of Reporting Person\(s\) to Issuer([\s\S]*?)Individual or Joint\/Group Filing/i);
  if (!m) return null;
  const roles: string[] = [];
  for (const row of parseHtmlTable(m[1])) {
    for (let i = 0; i < row.length - 1; i++) {
      if (row[i].trim().toUpperCase() !== "X") continue;
      const label = row[i + 1].toLowerCase();
      if (label.includes("director")) roles.push("director");
      else if (label.includes("officer")) roles.push("officer");
      else if (label.includes("10%") || label.includes("owner")) roles.push("ten_percent_owner");
      else if (label.includes("other")) roles.push("other");
    }
  }
  return [...new Set(roles)].join(", ") || null;
}
function parseForm4HtmlTransaction(html: string): Record<string, unknown> | null {
  const owner = form4OwnerFromHtml(html);
  const role = form4RoleFromHtml(html);
  const tableRe = /<table[^>]*>[\s\S]*?<\/table>/gi;
  let tableM: RegExpExecArray | null;
  while ((tableM = tableRe.exec(html)) !== null) {
    const tableHtml = tableM[0];
    const tableText = stripHtmlTags(tableHtml).toLowerCase();
    if (tableText.includes("non-derivative securities")) {
      for (const row of parseHtmlTable(tableHtml)) {
        if (row.length < 10) continue;
        const code = row[3].trim().toUpperCase();
        const shares = form4Num(row[5]);
        if (!code || shares == null) continue;
        return form4Payload(
          owner,
          role,
          code,
          shares,
          form4Num(row[7]),
          row[9].trim() || null,
          form4Date(row[1]),
        );
      }
    }
    if (tableText.includes("derivative securities")) {
      for (const row of parseHtmlTable(tableHtml)) {
        if (row.length < 15) continue;
        const code = row[4].trim().toUpperCase();
        const shares = form4Num(row[6]);
        if (!code || shares == null) continue;
        return form4Payload(
          owner,
          role,
          code,
          shares,
          form4Num(row[1]),
          row[14].trim() || null,
          form4Date(row[2]),
        );
      }
    }
  }
  return null;
}
function parseForm4Transaction(xml: string): Record<string, unknown> | null {
  const tx = xmlBlock(xml, "nonDerivativeTransaction") ?? xmlBlock(xml, "derivativeTransaction");
  if (!tx) return parseForm4HtmlTransaction(xml);
  const owner = xmlTag(xml, ["reportingOwnerId", "rptOwnerName"]);
  const officerTitle = xmlTag(xml, ["reportingOwnerRelationship", "officerTitle"]);
  const roles: string[] = [];
  for (const [tag, label] of [["isDirector", "director"], ["isOfficer", "officer"], ["isTenPercentOwner", "ten_percent_owner"], ["isOther", "other"]]) {
    if (["1", "true"].includes(_str(xmlTag(xml, ["reportingOwnerRelationship", tag])).toLowerCase())) roles.push(label);
  }
  const code = xmlTag(tx, ["transactionCoding", "transactionCode"]);
  const shares = form4Num(xmlTag(tx, ["transactionAmounts", "transactionShares", "value"]));
  const price = form4Num(xmlTag(tx, ["transactionAmounts", "transactionPricePerShare", "value"]));
  return form4Payload(
    owner,
    officerTitle || roles.join(", ") || null,
    code,
    shares,
    price,
    xmlTag(tx, ["ownershipNature", "directOrIndirectOwnership", "value"]),
    form4Date(xmlTag(tx, ["transactionDate", "value"])),
  );
}
async function tryAttachForm4Transaction(item: Record<string, unknown>, filing: Record<string, unknown>, warnings: Record<string, unknown>[]): Promise<void> {
  if (_str(filing.filingType).toUpperCase() !== "4") return;
  const url = _str(item.url);
  if (!url.startsWith("https://www.sec.gov/Archives/")) {
    warnings.push({ code: "FORM4_PARSE_UNAVAILABLE", message: "Form 4 primary document URL is unavailable.", severity: "warning" });
    return;
  }
  const xml = await edgarGetHtml(url, 2_000_000);
  const parsed = parseForm4Transaction(xml ?? "");
  if (!parsed) {
    warnings.push({ code: "FORM4_PARSE_UNAVAILABLE", message: "Form 4 transaction details could not be parsed from the primary document.", severity: "warning" });
    return;
  }
  item.insiderTransaction = parsed;
  const label = _str(parsed.transactionLabel) || "Insider transaction";
  const owner = _str(parsed.owner) || "reporting owner";
  const shares = typeof parsed.shares === "number" ? `${Math.round(parsed.shares).toLocaleString("en-US")} shares` : "shares unavailable";
  const value = typeof parsed.value === "number" ? `, value $${Math.round(parsed.value).toLocaleString("en-US")}` : "";
  item.title = `Form 4: ${label} by ${owner}`;
  item.summary = shortText(`${label} by ${owner}: ${shares}${value}.`);
  item.evidenceText = shortText(`SEC Form 4 transaction code ${_str(parsed.transactionCode) || "unknown"} on ${_str(parsed.transactionDate) || _str(item.filingDate)}.`);
}
function buildSecEventItem(
  ticker: string,
  filing: Record<string, unknown>,
  retrievedAt: string,
  issuer: string | null
): { item: Record<string, unknown>; warnings: Record<string, unknown>[] } {
  const warnings: Record<string, unknown>[] = [];
  const filingType = _str(filing.filingType || filing.formType || filing.form).toUpperCase();
  const filingDate = _str(filing.filingDate);
  const acceptedAt = normalizeIso(filing.acceptedAt ?? filing.acceptanceDateTime);
  let publishedAt = acceptedAt;
  if (!publishedAt && filingDate) {
    publishedAt = `${filingDate}T00:00:00Z`;
    warnings.push({
      code: "PUBLISHED_AT_ESTIMATED",
      message: `acceptedAt unavailable for ${filingType || "SEC filing"}; filingDate used.`,
      severity: "warning",
    });
  }
  const accessionNumber = _str(filing.accessionNumber);
  const cikInt = _str(filing.cikInt || filing.cik);
  const accClean = accessionNumber.replace(/-/g, "");
  const primaryDocument = _str(filing.primaryDocument);
  let url = _str(filing.documentUrl || filing.primaryDocumentUrl);
  if (!url && cikInt && accessionNumber && primaryDocument) {
    url = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${primaryDocument}`;
  }
  if (!url && cikInt && accessionNumber) {
    url = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${accClean}/${accessionNumber}-index.htm`;
  }
  if (!url.startsWith("https://www.sec.gov/Archives/")) {
    url = "";
    warnings.push({ code: "SEC_URL_INVALID", message: "SEC event URL missing or invalid SEC Archives URL.", severity: "warning" });
  }
  const duplicateGroupId = makeDupGroupId(ticker, `${filingType} filed`, publishedAt, issuer);
  if (!duplicateGroupId) {
    warnings.push({ code: "DEDUPE_WEAK_KEY", message: "Weak dedupe key for at least one item.", severity: "warning" });
  }
  const confidence = accessionNumber && acceptedAt && url ? "HIGH" : (accessionNumber && url ? "MEDIUM" : "LOW");
  return {
    item: {
      title: `${filingType || "SEC"} filed`,
      source: "SEC",
      sourceType: "sec_filing",
      filingType: filingType || null,
      filingDate: filingDate || null,
      acceptedAt: acceptedAt || null,
      accessionNumber: accessionNumber || null,
      cikInt: cikInt || null,
      url: url || null,
      publishedAt,
      retrievedAt,
      issuer,
      tickers: [ticker.toUpperCase()],
      eventType: eventTypeFromForm(filingType),
      summary: shortText(`SEC ${filingType} filing for ${ticker.toUpperCase()}`),
      evidenceText: shortText(`${filingType} accepted by SEC on ${acceptedAt || filingDate}`),
      confidence,
      tickerRelevance: "HIGH",
      decisionUse: sourceDecisionUse("sec_filing"),
      duplicateGroupId,
    },
    warnings,
  };
}
export async function collectSecEvents(
  ticker: string,
  filingTypes: string[],
  maxResults: number,
  retrievedAt: string,
  startDate = "",
  endDate = "",
  lookbackDays?: number
): Promise<NewsProviderResult> {
  const warnings: Record<string, unknown>[] = [];
  const items: Record<string, unknown>[] = [];
  const { cikPadded, submissions } = await getSubmissionsForTicker(ticker);
  if (!cikPadded || !submissions) {
    warnings.push({ code: "SOURCE_UNAVAILABLE", message: "SEC submissions source unavailable.", severity: "warning" });
    // No CIK although SEC's ticker index was read: the ticker has no SEC filings (2.16.0). A failed lookup or
    // submissions fetch stays a provider failure.
    return { items, warnings, used: false, ...(!cikPadded && edgarTickerMapLoaded() ? { noCik: true } : {}) };
  }
  const recent = ((submissions.filings as Record<string, unknown>)?.recent as Record<string, unknown[]>) ?? {};
  const forms = (recent.form ?? []) as string[];
  const filingDates = (recent.filingDate ?? []) as string[];
  const accepted = (recent.acceptanceDateTime ?? []) as string[];
  const accessions = (recent.accessionNumber ?? []) as string[];
  const primaryDocs = (recent.primaryDocument ?? []) as string[];
  const issuer = _str(submissions.name) || null;
  const wanted = new Set(filingTypes.map(f => f.toUpperCase()));
  const cikInt = String(parseInt(cikPadded, 10));
  for (let i = 0; i < forms.length && items.length < maxResults; i++) {
    const form = _str(forms[i]).toUpperCase();
    if (wanted.size > 0 && !wanted.has(form)) continue;
    const filing = {
      filingType: form,
      filingDate: filingDates[i] ?? "",
      acceptedAt: accepted[i] ?? "",
      accessionNumber: accessions[i] ?? "",
      primaryDocument: primaryDocs[i] ?? "",
      cikInt,
    };
    const { item, warnings: w } = buildSecEventItem(ticker, filing, retrievedAt, issuer);
    await tryAttachForm4Transaction(item, filing, w);
    if (!withinDateWindow(_str(item.publishedAt) || null, startDate, endDate, lookbackDays)) continue;
    items.push(item);
    warnings.push(...w);
  }
  return { items, warnings, used: true };
}
