# R5-extractors-3 (3.0.7): monetary loss sign, China preset as data

**Source:** the rest of the brief's R5 extractor list (`MODERNIZATION_BRIEF.md`, R5-‹unit› Extractors: "BR-EXT-015 monetary loss sign … Q5 China preset with aliases moved to data"). 3.0.6 (`R5EXTRACTORS2_SPEC.md`) shipped the other items.
- BR-EXT-015 scheduled repair (owner, 2026-10-15): "Monetary metrics stated as a loss must not be returned as positive values."
- Q5 (§7): "move issuer-specific aliases to data/config. Keep the public compatibility action for now."

F39 (the outsourcing-partner phrases) already shipped in 2.25.4 and F62 in 2.26.5. Their phrases move with the data here.

**Version:** 3.0.7 (patch). No new field, enum value, warning code or `unresolvedReason`. Values change only for the loss sign.

## 1. Items

### BR-EXT-015: a figure stated as a loss is negative

`release_text_metric` (`yfmcp/extraction_rules.py`) and `releaseTextMetric` (`worker/src/extraction-rules.ts`) feed `extract_earnings_metrics` and, through it, `compare_earnings_actual_vs_estimate`.

The "stated loss" test now runs on the text right before the figure: the gap after the label for a label-first match, and the text before the amount for the per-diluted-share form. It uses one pattern in both runtimes:

`(?:\bnegative|(?<![(/])\bloss(?:\s+of)?)\s*$|(?:^|[\s(])[-−]$` (case-insensitive)

| Text before the figure | Before | Now |
|---|---|---|
| "was negative $5 billion" | negative | negative |
| "was a loss of $5 billion" / "operating income was a loss of $5 million" / "diluted EPS was a loss of $0.12" | positive | **negative** |
| "was -$5 billion", "was −$5 billion" (U+2212), "reported -$0.12 per diluted share" | positive | **negative** |
| "income (loss) of $5 million", "income/loss of $5 million" | positive | positive (both outcomes named) |
| "revenue for the quarter - $94.9 billion" (spaced dash) | positive | positive (separator) |

- **Planner call:** a figure stated as a loss is returned negative, not dropped, in line with the existing "negative" wording, parentheses and "$-5" rules.
- The minus must touch the dollar sign and follow a space, "(" or the start. A hyphen joined to a word or figure ("Q3-$5") or a spaced dash is not a sign.
- The existing EPS rules are unchanged: loss in the label, and the last net result before a per-share figure.
- **Out of scope:** a figure whose gap names a different metric ("revenue … net loss of $5") was wrong before as a revenue read and stays wrong, now with a minus. It is a label-matching defect, not a sign defect.

### Q5: the China preset is data

- New `worker/src/exposure-presets.json` (`presets.china`): `region`, `riskTerms`, `entityTerms` (the AXT aliases Tongmei, JinMei and BoYu, with a note naming the issuer), `bankTerms`, `manufacturingTerms` and `namedEntities`.
- The Worker imports it in `yahoo-finance.ts` (`CHINA_PRESET`, exported for the parity case). Python loads it in `yfmcp/exposure_presets.py` from `worker/src`, as `yfmcp/tools/news.py` already does for the news registries.
- `extract_china_exposure` (both runtimes) reads these terms in place of its inline lists:
  - the revenue query and the operational text search ("China");
  - the risk terms;
  - the entity, bank and manufacturing terms.
- `extract_exposure` with topic `china` reads `namedEntities` in place of `CHINA_NAMED_ENTITIES`.
- Content and order are unchanged, so output is unchanged. The aliases are still searched for every issuer, as before the move.
  - Scoping them to AXTI would be a behaviour change. It is left for the Phase 5 extractors move, where Q5's governed preset over `extract_exposure` is built.
- The topology script lists `yfmcp/exposure_presets.py` under extractors and the JSON as a data source Python reads from the Worker tree.

## 2. Tests

- `scripts/parity/cases_extraction_rules.py`:
  - `ext.sign.monetary_loss_wording_unsigned` (pinned) becomes `ext.sign.monetary_loss_wording_negative` (equal, −5,000,000,000).
  - `ext.sign.minus_before_dollar_dropped` becomes `ext.sign.minus_before_dollar_negative`.
  - New cases: `unicode_minus_before_dollar_negative`, `operating_income_loss_wording_negative`, `eps_loss_of_wording_negative`, `per_share_minus_before_dollar_negative`, `both_outcomes_loss_in_parentheses_not_signed`, `both_outcomes_slash_not_signed`, `spaced_dash_separator_not_signed`.
- `scripts/parity/cases_r5ext3.py`: `r5ext3.china_preset.terms_as_before_the_move` pins the lists each runtime loads to the pre-move literals.
- The existing China exposure cases (`cases_audit_2254.py`, F39) pass unchanged.

## 3. Exit

- [ ] Full parity harness and every `ci.yml` test command.
- [ ] PR, CI green, owner merge.
- [ ] Live check: hashes unchanged, `health_check` 3.0.7, AAPL `extract_china_exposure` unchanged from 3.0.6 (manufacturingExposure FOUND), AAPL `extract_earnings_metrics` latest values unchanged (positive figures).
