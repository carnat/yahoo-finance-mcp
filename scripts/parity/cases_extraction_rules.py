"""Parity cases: release-text extraction rules (yfmcp/extraction_rules.py ↔ worker/src/extraction-rules.ts).

Covers BR-EXT-010 (guidance basis), BR-EXT-014 (outlook table scale), BR-EXT-015 (release figure scale
and loss sign; owner-corrected card, monetary-loss repair scheduled), BR-EXT-016/017 (reported release
metric acceptance and eligibility).
"""

from __future__ import annotations

from scripts.parity.harness import Case
from yfmcp import extraction_rules as er


def _guidance(text: str, name: str, ids: list[str], metric: str, expect: dict | None, **kw) -> Case:
    """guidance_ranges in both runtimes, compared on one metric's low/high/basis/statedAs."""
    def pick(v):
        r = (v or {}).get(metric)
        return None if r is None else {k: r.get(k) for k in ("low", "high", "basis", "statedAs")}
    return Case(name=name, ids=ids, py=lambda: er.guidance_ranges(text),
                ts={"module": "extractionRules", "fn": "guidanceRanges", "args": [text]},
                select=pick, expect=expect, **kw)


def _release(text: str, metric: str, name: str, ids: list[str], expect, **kw) -> Case:
    """release_text_metric in both runtimes, compared on value and scaleBasis."""
    def pick(v):
        return None if v is None else {"value": v.get("value"), "scaleBasis": v.get("scaleBasis")}
    return Case(name=name, ids=ids, py=lambda: er.release_text_metric(text, metric),
                ts={"module": "extractionRules", "fn": "releaseTextMetric", "args": [text, metric]},
                select=pick, expect=expect, **kw)


CASES = [
    # ── BR-EXT-010: a range's basis is read from its own clause ───────────────────────────────────
    _guidance("The company expects diluted EPS between $1.85 and $2.05 on a non-GAAP basis.",
              "ext.guidance_basis.non_gaap", ["BR-EXT-010"], "eps",
              {"low": "1.85", "high": "2.05", "basis": "NON_GAAP", "statedAs": "RANGE"}),
    _guidance("The company expects GAAP diluted EPS between $0.40 and $0.44 for the third quarter.",
              "ext.guidance_basis.gaap", ["BR-EXT-010"], "eps",
              {"low": "0.40", "high": "0.44", "basis": "GAAP", "statedAs": "RANGE"}),
    _guidance("GAAP and non-GAAP revenue is expected to be between $10.0 billion and $10.4 billion.",
              "ext.guidance_basis.both", ["BR-EXT-010"], "revenue",
              {"low": "10.0 billion", "high": "10.4 billion", "basis": "GAAP_AND_NON_GAAP", "statedAs": "RANGE"}),
    # Finding (Phase 2, for Q3i triage): "on both a GAAP and non-GAAP basis" after the range is read as GAAP only,
    # because the clause tail stops before "and non-GAAP". Both runtimes agree; recorded as current behaviour.
    _guidance("The company expects revenue between $10.0 billion and $10.4 billion on both a GAAP and non-GAAP basis.",
              "ext.guidance_basis.both_trailing_phrase_reads_gaap", ["BR-EXT-010"], "revenue",
              {"low": "10.0 billion", "high": "10.4 billion", "basis": "GAAP", "statedAs": "RANGE"}),
    _guidance("The company expects revenue between $10.0 billion and $10.4 billion for the third quarter.",
              "ext.guidance_basis.not_stated", ["BR-EXT-010"], "revenue",
              {"low": "10.0 billion", "high": "10.4 billion", "basis": "NOT_STATED", "statedAs": "RANGE"}),
    _guidance("The company expects adjusted diluted EPS between $1.10 and $1.20.",
              "ext.guidance_basis.adjusted", ["BR-EXT-010"], "eps",
              {"low": "1.10", "high": "1.20", "basis": "NON_GAAP", "statedAs": "RANGE"}),

    # ── BR-EXT-014: an unscaled outlook amount takes the table's scale within 400 chars ───────────
    # "Outlook ... Revenue $X - $Y" matches the keyword-first pattern, which wins over a table row, so these
    # read as RANGE; the scale rule applies the same way to both.
    _guidance("Business Outlook (in millions, except per share amounts) Revenue $10,300 - $10,800",
              "ext.outlook_scale.millions", ["BR-EXT-014"], "revenue",
              {"low": "10,300 million", "high": "10,800 million", "basis": "NOT_STATED", "statedAs": "RANGE"}),
    _guidance("Business Outlook (in millions, except per share amounts) Revenue $10.3 billion - $10.8 billion",
              "ext.outlook_scale.own_scale_kept", ["BR-EXT-014"], "revenue",
              {"low": "10.3 billion", "high": "10.8 billion", "basis": "NOT_STATED", "statedAs": "RANGE"}),
    _guidance("Business Outlook (in thousands) Revenue $10,300 - $10,800",
              "ext.outlook_scale.thousands", ["BR-EXT-014"], "revenue",
              {"low": "10,300 thousand", "high": "10,800 thousand", "basis": "NOT_STATED", "statedAs": "RANGE"}),

    # ── BR-EXT-016 / BR-EXT-017: reported release metric acceptance and eligibility ────────────────
    _release("Condensed statements (in millions). Revenue was $ 54,229 for the fourth quarter.", "revenue",
             "ext.release.single_table_scale", ["BR-EXT-016", "BR-EXT-017", "BR-EXT-015"],
             {"value": 54229000000, "scaleBasis": "RELEASE_TABLE_IN_MILLIONS"}),
    _release("Revenue was $54,229 for the fourth quarter. (in millions) (in millions)", "revenue",
             "ext.release.repeated_scale_counts_once", ["BR-EXT-015", "BR-EXT-017"],
             {"value": 54229000000, "scaleBasis": "RELEASE_TABLE_IN_MILLIONS"}),
    _release("Revenue was $54,229 for the fourth quarter. (in millions) (in thousands)", "revenue",
             "ext.release.two_scales_skipped", ["BR-EXT-015", "BR-EXT-016", "BR-EXT-017"], None),
    _release("Revenue of $54.23 billion for the fourth quarter. (in millions)", "revenue",
             "ext.release.as_written_scale_wins", ["BR-EXT-016"], {"value": 54230000000, "scaleBasis": "AS_WRITTEN"}),
    _release("Revenue was $54,229 for the fourth quarter.", "revenue",
             "ext.release.no_scale_declared", ["BR-EXT-015", "BR-EXT-017"], {"value": 54229, "scaleBasis": None}),
    _release("Revenue increased 5.2% to $1.2 billion in the third quarter.", "revenue",
             "ext.release.change_verb_reads_after_to", ["BR-EXT-016", "BR-EXT-017"],
             {"value": 1200000000, "scaleBasis": "AS_WRITTEN"}),
    _release("Revenue is expected to be $1.2 billion next quarter, the outlook says.", "revenue",
             "ext.release.guidance_sentence_skipped", ["BR-EXT-016", "BR-EXT-017"], None),
    _release("Revenue was $125 million of awards in the quarter.", "revenue",
             "ext.release.award_sentence_skipped", ["BR-EXT-016", "BR-EXT-017"], None),
    _release("Non-GAAP revenue was $300 million for the quarter.", "revenue",
             "ext.release.non_gaap_rejected", ["BR-EXT-016", "BR-EXT-017"], None),
    _release("Revenue was $2.9 billion for the full year 2025.", "revenue",
             "ext.release.annual_only_skipped", ["BR-EXT-016", "BR-EXT-017"], None),
    _release("Revenue was $2.9 billion ± $0.1 billion for the quarter.", "revenue",
             "ext.release.plus_minus_skipped", ["BR-EXT-016"], None),
    _release("Gaming revenue was $2.9 billion for the quarter.", "revenue",
             "ext.release.segment_label_rejected", ["BR-EXT-017"], None),
    _release("Total revenue was $2.9 billion for the quarter.", "revenue",
             "ext.release.qualified_label_accepted", ["BR-EXT-017"], {"value": 2900000000, "scaleBasis": "AS_WRITTEN"}),

    # ── BR-EXT-015: sign rules (owner-corrected card) ───────────────────────────────────────────
    _release("Diluted net loss per share of $0.12 for the quarter.", "epsDiluted",
             "ext.sign.eps_loss_wording_negative", ["BR-EXT-015", "BR-EXT-016"], {"value": -0.12, "scaleBasis": None}),
    _release("Diluted income (loss) per share of $0.12 for the quarter.", "epsDiluted",
             "ext.sign.eps_both_outcomes_label_not_signed", ["BR-EXT-015"], {"value": 0.12, "scaleBasis": None}),
    _release("Operating income was $(1,234) for the quarter. (in millions)", "operatingIncome",
             "ext.sign.parentheses_negative", ["BR-EXT-015"], {"value": -1234000000, "scaleBasis": "RELEASE_TABLE_IN_MILLIONS"}),
    _release("Free cash flow was negative $5 billion for the quarter.", "freeCashFlow",
             "ext.sign.negative_wording", ["BR-EXT-015"], {"value": -5000000000, "scaleBasis": "AS_WRITTEN"}),
    _release("Free cash flow was $-5 billion for the quarter.", "freeCashFlow",
             "ext.sign.ascii_minus", ["BR-EXT-015"], {"value": -5000000000, "scaleBasis": "AS_WRITTEN"}),
    # R5-extractors-3 (Q1 BR-EXT-015 / Q3i): a figure stated as a loss, or with a minus before the dollar sign, is negative.
    _release("Free cash flow was -$5 billion for the quarter.", "freeCashFlow",
             "ext.sign.minus_before_dollar_negative", ["BR-EXT-015"], {"value": -5000000000, "scaleBasis": "AS_WRITTEN"}),
    _release("Free cash flow was \u2212$5 billion for the quarter.", "freeCashFlow",
             "ext.sign.unicode_minus_before_dollar_negative", ["BR-EXT-015"], {"value": -5000000000, "scaleBasis": "AS_WRITTEN"}),
    _release("Free cash flow was a loss of $5 billion for the quarter.", "freeCashFlow",
             "ext.sign.monetary_loss_wording_negative", ["BR-EXT-015"], {"value": -5000000000, "scaleBasis": "AS_WRITTEN"}),
    _release("Operating income was a loss of $5 million for the quarter.", "operatingIncome",
             "ext.sign.operating_income_loss_wording_negative", ["BR-EXT-015"], {"value": -5000000, "scaleBasis": "AS_WRITTEN"}),
    _release("Diluted EPS was a loss of $0.12 for the quarter.", "epsDiluted",
             "ext.sign.eps_loss_of_wording_negative", ["BR-EXT-015"], {"value": -0.12, "scaleBasis": None}),
    _release("The company reported -$0.12 per diluted share for the quarter.", "epsDiluted",
             "ext.sign.per_share_minus_before_dollar_negative", ["BR-EXT-015"], {"value": -0.12, "scaleBasis": None}),
    # Wording that names both outcomes, or a spaced dash used as a separator, signs nothing.
    _release("We reported operating income (loss) of $5 million for the quarter.", "operatingIncome",
             "ext.sign.both_outcomes_loss_in_parentheses_not_signed", ["BR-EXT-015"], {"value": 5000000, "scaleBasis": "AS_WRITTEN"}),
    _release("We reported operating income/loss of $5 million for the quarter.", "operatingIncome",
             "ext.sign.both_outcomes_slash_not_signed", ["BR-EXT-015"], {"value": 5000000, "scaleBasis": "AS_WRITTEN"}),
    _release("Total revenue for the quarter - $94.9 billion, reported today.", "revenue",
             "ext.sign.spaced_dash_separator_not_signed", ["BR-EXT-015"], {"value": 94900000000, "scaleBasis": "AS_WRITTEN"}),
    # EPS never inherits a table scale (scale applies to money metrics only).
    _release("Diluted net income per share was $1.25 for the quarter. (in millions)", "epsDiluted",
             "ext.scale.eps_never_table_scaled", ["BR-EXT-015"], {"value": 1.25, "scaleBasis": None}),
]
