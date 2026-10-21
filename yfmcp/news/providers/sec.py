"""SEC provider for the news tools: recent filings from submissions, with the Form 4 transaction parser.

Adapter contract (unchanged by the move): each ``_collect_*_events`` keeps its current signature and
return value. The return value is the items plus the per-source status/diagnostics; warnings are
appended to the caller's ``warnings`` list. There is no provider registry yet (2.15.1 is a pure
restructure; the registry and the off switch are part 2).
"""

import re as _re
import xml.etree.ElementTree as _ET

from yfmcp.util import _to_iso_utc
from yfmcp.clients.edgar import _edgar_get_html
from yfmcp.clients.edgar import _edgar_ticker_map_loaded
from yfmcp.clients.edgar import _get_submissions_for_ticker
from yfmcp.clients.edgar import _load_edgar_tickers
from yfmcp.parsing.html import _parse_html_table
from yfmcp.parsing.html import _strip_html_tags
from yfmcp.news.shared import (
    _event_type_from_keywords,
    _make_duplicate_group_id,
    _round_half_away_from_zero,
    _short_text,
    _within_date_window,
)


def _event_type_from_form(form_type: str) -> str:
    # An amendment (8-K/A) types as the form it amends (2.25.4, F43).
    ft = (form_type or "").upper().removesuffix("/A")
    if ft in ("10-Q", "10-K"):
        return "earnings"
    if ft in ("S-3", "S-1", "424B"):
        return "financing"
    if ft in ("8-K", "DEF14A", "PRE14A"):
        return "regulatory"
    if ft == "4":
        return "insider"
    return "other"


def _xml_text(root: _ET.Element, path: str) -> str | None:
    found = root.find(path)
    if found is not None and found.text:
        text = found.text.strip()
        return text or None
    return None


def _strip_xml_namespaces(xml_text: str) -> str:
    return _re.sub(r'\sxmlns(:\w+)?="[^"]*"', "", xml_text)


_FORM4_TRANSACTION_LABELS = {
    "P": "Purchase",
    "S": "Sale",
    "A": "Grant/Award",
    "D": "Disposition",
    "M": "Option exercise/conversion",
    "F": "Tax withholding/payment",
    "G": "Gift",
}


def _form4_num(value: str | None) -> float | None:
    if value is None:
        return None
    text = _strip_html_tags(value)
    match = _re.search(r"-?\$?\s*\(?\d[\d,]*(?:\.\d+)?\)?", text)
    if not match:
        return None
    s = match.group(0).replace("$", "").replace(",", "").replace(" ", "")
    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    try:
        n = float(s)
    except Exception:
        return None
    return -n if negative else n


def _form4_date(value: str | None) -> str | None:
    text = _strip_html_tags(value or "")
    if not text:
        return None
    if _re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    m = _re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text)
    if not m:
        return text
    month, day, year = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return f"{year:04d}-{month:02d}-{day:02d}"


def _form4_owner_from_html(html_text: str) -> str | None:
    m = _re.search(
        r"Name and Address of Reporting Person[\s\S]*?<a\b[^>]*>([\s\S]*?)</a>",
        html_text,
        _re.IGNORECASE,
    )
    return _strip_html_tags(m.group(1)) if m else None


def _form4_role_from_html(html_text: str) -> str | None:
    m = _re.search(
        r"Relationship of Reporting Person\(s\) to Issuer([\s\S]*?)Individual or Joint/Group Filing",
        html_text,
        _re.IGNORECASE,
    )
    if not m:
        return None
    roles: list[str] = []
    for row in _parse_html_table(m.group(1)):
        for i, cell in enumerate(row[:-1]):
            if cell.strip().upper() == "X":
                label = row[i + 1].strip().lower()
                if "director" in label:
                    roles.append("director")
                elif "officer" in label:
                    roles.append("officer")
                elif "10%" in label or "owner" in label:
                    roles.append("ten_percent_owner")
                elif "other" in label:
                    roles.append("other")
    return ", ".join(dict.fromkeys(roles)) or None


def _parse_form4_html_transaction(html_text: str) -> dict | None:
    owner_name = _form4_owner_from_html(html_text)
    role = _form4_role_from_html(html_text)
    for table_m in _re.finditer(r"<table[^>]*>([\s\S]*?)</table>", html_text, _re.IGNORECASE):
        table_html = table_m.group(0)
        table_text = _strip_html_tags(table_html).lower()
        if "non-derivative securities" in table_text:
            for row in _parse_html_table(table_html):
                if len(row) < 10:
                    continue
                code = row[3].strip().upper()
                shares = _form4_num(row[5])
                if not code or shares is None:
                    continue
                price = _form4_num(row[7])
                return _form4_transaction_payload(
                    owner_name=owner_name,
                    role=role,
                    code=code,
                    shares=shares,
                    price=price,
                    ownership_form=row[9].strip() or None,
                    transaction_date=_form4_date(row[1]),
                )
        if "derivative securities" in table_text:
            for row in _parse_html_table(table_html):
                if len(row) < 15:
                    continue
                code = row[4].strip().upper()
                shares = _form4_num(row[6])
                if not code or shares is None:
                    continue
                return _form4_transaction_payload(
                    owner_name=owner_name,
                    role=role,
                    code=code,
                    shares=shares,
                    price=_form4_num(row[1]),
                    ownership_form=row[14].strip() or None,
                    transaction_date=_form4_date(row[2]),
                )
    return None


def _form4_transaction_payload(
    *,
    owner_name: str | None,
    role: str | None,
    code: str | None,
    shares: float | None,
    price: float | None,
    ownership_form: str | None,
    transaction_date: str | None,
) -> dict:
    return {
        "owner": owner_name,
        "role": role,
        "transactionCode": code,
        "transactionLabel": _FORM4_TRANSACTION_LABELS.get(str(code or "").upper(), "Other/Unclassified"),
        "shares": shares,
        "price": price,
        "value": _round_half_away_from_zero(shares * price, 2) if shares is not None and price is not None else None,
        "ownershipForm": ownership_form,
        "transactionDate": transaction_date,
    }


def _parse_form4_transaction(xml_text: str) -> dict | None:
    try:
        root = _ET.fromstring(_strip_xml_namespaces(xml_text))
    except Exception:
        return _parse_form4_html_transaction(xml_text)
    owner_name = _xml_text(root, ".//reportingOwnerId/rptOwnerName")
    officer_title = _xml_text(root, ".//reportingOwnerRelationship/officerTitle")
    roles: list[str] = []
    for flag, label in (
        ("isDirector", "director"),
        ("isOfficer", "officer"),
        ("isTenPercentOwner", "ten_percent_owner"),
        ("isOther", "other"),
    ):
        if (_xml_text(root, f".//reportingOwnerRelationship/{flag}") or "").lower() in {"1", "true"}:
            roles.append(label)
    tx = root.find(".//nonDerivativeTransaction")
    if tx is None:
        tx = root.find(".//derivativeTransaction")
    if tx is None:
        # A well-formed XHTML primary document has no XML transaction node: read it as HTML (Worker behaviour, 2.17.0).
        return _parse_form4_html_transaction(xml_text)
    code = _xml_text(tx, ".//transactionCoding/transactionCode")
    shares_text = _xml_text(tx, ".//transactionAmounts/transactionShares/value")
    price_text = _xml_text(tx, ".//transactionAmounts/transactionPricePerShare/value")
    shares = _form4_num(shares_text)
    price = _form4_num(price_text)
    return _form4_transaction_payload(
        owner_name=owner_name,
        role=officer_title or ", ".join(roles) or None,
        code=code,
        shares=shares,
        price=price,
        ownership_form=_xml_text(tx, ".//ownershipNature/directOrIndirectOwnership/value"),
        transaction_date=_form4_date(_xml_text(tx, ".//transactionDate/value")),
    )


async def _try_attach_form4_transaction(item: dict, filing: dict, warnings: list[dict]) -> None:
    if str(filing.get("filingType") or "").upper() != "4":
        return
    url = _safe_sec_url(item.get("url"))
    if not url:
        warnings.append({
            "code": "FORM4_PARSE_UNAVAILABLE",
            "message": "Form 4 primary document URL is unavailable.",
            "severity": "warning",
        })
        return
    xml_text = await _edgar_get_html(url, max_bytes=2_000_000)
    parsed = _parse_form4_transaction(xml_text or "")
    if not parsed:
        warnings.append({
            "code": "FORM4_PARSE_UNAVAILABLE",
            "message": "Form 4 transaction details could not be parsed from the primary document.",
            "severity": "warning",
        })
        return
    item["insiderTransaction"] = parsed
    label = parsed.get("transactionLabel") or "Insider transaction"
    owner = parsed.get("owner") or "reporting owner"
    shares = parsed.get("shares")
    value = parsed.get("value")
    # The summary rounds with the same helper as the value; int() keeps a result of zero as "0", never "-0" (2.17.0).
    value_part = f", value ${int(_round_half_away_from_zero(value, 0)):,}" if isinstance(value, (int, float)) else ""
    shares_part = f"{int(_round_half_away_from_zero(shares, 0)):,} shares" if isinstance(shares, (int, float)) else "shares unavailable"
    item["title"] = f"Form 4: {label} by {owner}"
    item["summary"] = _short_text(f"{label} by {owner}: {shares_part}{value_part}.")
    item["evidenceText"] = _short_text(f"SEC Form 4 transaction code {parsed.get('transactionCode') or 'unknown'} on {parsed.get('transactionDate') or item.get('filingDate')}.")


def _safe_sec_url(candidate: object) -> str | None:
    url = str(candidate or "").strip()
    return url if url.startswith("https://www.sec.gov/Archives/") else None


def _build_sec_event_item(ticker: str, filing: dict, retrieved_at: str, issuer: str | None = None) -> tuple[dict, list[dict]]:
    warnings: list[dict] = []
    filing_type = str(filing.get("filingType") or filing.get("formType") or filing.get("form") or "").upper()
    filing_date = str(filing.get("filingDate") or "").strip()
    accepted_at = _to_iso_utc(filing.get("acceptedAt") or filing.get("acceptanceDateTime"))
    published_at = accepted_at
    if not published_at and filing_date:
        published_at = f"{filing_date}T00:00:00Z"
        warnings.append({
            "code": "PUBLISHED_AT_ESTIMATED",
            "message": f"acceptedAt unavailable for {filing_type or 'SEC filing'}; filingDate used.",
            "severity": "warning",
        })
    accession = str(filing.get("accessionNumber") or "").strip()
    cik_int = str(filing.get("cikInt") or filing.get("cik") or "").strip()
    acc_clean = accession.replace("-", "")
    primary_document = str(filing.get("primaryDocument") or "").strip()
    url = _safe_sec_url(filing.get("documentUrl")) or _safe_sec_url(filing.get("primaryDocumentUrl"))
    if not url and cik_int and accession and primary_document:
        url = _safe_sec_url(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{primary_document}")
    if not url and cik_int and accession:
        url = _safe_sec_url(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{accession}-index.htm")
    if not url:
        warnings.append({
            "code": "SEC_URL_INVALID",
            "message": "SEC event URL missing or invalid SEC Archives URL.",
            "severity": "warning",
        })
    confidence = "HIGH" if (accession and accepted_at and url) else ("MEDIUM" if accession and url else "LOW")
    title = f"{filing_type} filed" if filing_type else "SEC filing"
    event_type = _event_type_from_form(filing_type)
    duplicate_group_id = _make_duplicate_group_id(ticker, title, published_at, issuer, url)
    if duplicate_group_id is None:
        warnings.append({"code": "DEDUPE_WEAK_KEY", "message": "Weak dedupe key for at least one item.", "severity": "warning"})
    item = {
        "title": title,
        "source": "SEC",
        "sourceType": "sec_filing",
        "filingType": filing_type or None,
        "filingDate": filing_date or None,
        "acceptedAt": accepted_at,
        "accessionNumber": accession or None,
        "url": url,
        "publishedAt": published_at,
        "retrievedAt": retrieved_at,
        "issuer": issuer,
        "tickers": [ticker.upper()],
        "eventType": event_type or _event_type_from_keywords(title),
        "summary": _short_text(f"SEC {filing_type} filing for {ticker.upper()}"),
        "evidenceText": _short_text(f"{filing_type} accepted by SEC on {accepted_at or filing_date}"),
        "confidence": confidence,
        "tickerRelevance": "HIGH",
        "duplicateGroupId": duplicate_group_id,
    }
    return item, warnings


class _SecEventsResult(tuple):
    """The ``(items, warnings, used)`` triple plus ``no_cik`` (2.16.0; Worker ``noCik``): True when no CIK was found
    although SEC's ticker index was read, so the ticker has no SEC filings. The news pipeline reports it as
    NOT_ELIGIBLE; get_sec_recent_events keeps unpacking the triple and its own reporting. Never serialized."""

    no_cik: bool = False


async def _collect_sec_events(
    ticker: str,
    *,
    filing_types: list[str],
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
) -> tuple[list[dict], list[dict], bool]:
    warnings: list[dict] = []
    events: list[dict] = []
    cik_padded, submissions = await _get_submissions_for_ticker(ticker)
    if not cik_padded or not submissions:
        warnings.append({"code": "SOURCE_UNAVAILABLE", "message": "SEC submissions source unavailable.", "severity": "warning"})
        result = _SecEventsResult((events, warnings, False))
        # Index read but no CIK: no SEC filings for the ticker. A failed lookup or submissions fetch stays a provider failure.
        if not cik_padded:
            # Cached after the CIK lookup read it; a fetch happens only when the lookup never reached the index.
            await _load_edgar_tickers()
            result.no_cik = _edgar_ticker_map_loaded()
        return result
    recent = ((submissions.get("filings") or {}).get("recent") or {}) if isinstance(submissions, dict) else {}
    forms = list(recent.get("form") or [])
    filing_dates = list(recent.get("filingDate") or [])
    accepted = list(recent.get("acceptanceDateTime") or [])
    accessions = list(recent.get("accessionNumber") or [])
    primary_docs = list(recent.get("primaryDocument") or [])
    issuer = str(submissions.get("name") or "").strip() or None if isinstance(submissions, dict) else None
    desired = {f.upper() for f in filing_types}
    cik_int = str(int(cik_padded))
    for i, form in enumerate(forms):
        form_str = str(form or "").upper()
        if desired and form_str not in desired:
            continue
        filing_date = str(filing_dates[i] or "") if i < len(filing_dates) else ""
        accepted_at = str(accepted[i] or "") if i < len(accepted) else ""
        accession = str(accessions[i] or "") if i < len(accessions) else ""
        primary_doc = str(primary_docs[i] or "") if i < len(primary_docs) else ""
        filing_obj = {
            "filingType": form_str,
            "filingDate": filing_date,
            "acceptedAt": accepted_at,
            "accessionNumber": accession,
            "primaryDocument": primary_doc,
            "cikInt": cik_int,
        }
        item, item_warnings = _build_sec_event_item(ticker, filing_obj, retrieved_at, issuer=issuer)
        await _try_attach_form4_transaction(item, filing_obj, item_warnings)
        if not _within_date_window(item.get("publishedAt"), start_date=start_date, end_date=end_date, lookback_days=lookback_days):
            continue
        events.append(item)
        warnings.extend(item_warnings)
        if len(events) >= max_results:
            break
    return events, warnings, True
