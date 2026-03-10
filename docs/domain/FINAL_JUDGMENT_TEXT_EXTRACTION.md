# Final Judgment Text-First Extraction

The final-judgment extractor now uses a text-first path instead of relying only
on page-image prompting.

## Why

The worst judgment failures were not random:

- multi-page image extraction duplicated money lines across pages, producing
  impossible totals
- page-merging could pull the wrong address or legal block from a different
  section of the document
- local OpenAI-compatible endpoints were capable of structured output, but the
  old path was still image-first and expensive to retry

Those failures were showing up in `BAD` / `CRITICAL` judgment triage as:

- itemized amounts exceeding the stated judgment total
- property identity mismatches
- fee-order / wrong-document contamination

## Current Pipeline

`src/services/final_judgment_processor.py` now does this:

1. Render the PDF pages to images.
2. Run Tesseract OCR on those page images.
3. Build page-marked OCR text (`--- PAGE N ---`).
4. Send the OCR text to the LLM with the strict `JudgmentExtraction` JSON
   schema.
5. Validate the result with Pydantic hard gates.
6. Fall back to image-based extraction only if the text pass is incomplete or
   invalid.
7. Cache the extracted JSON with validation metadata.

## Design Rules

- OCR text is the primary evidence source for final judgments.
- Structured output is required on both local and cloud OpenAI-compatible
  endpoints.
- The pipeline computes money residuals itself; the model is not trusted to do
  arithmetic correctly.
- The validator recovers document-stated credit adjustments from OCR text
  (`less payments`, `escrow credits`, `unapplied funds`, similar subtractive
  lines) before enforcing the hard amount gate. This keeps the arithmetic check
  strict without trusting the model to carry subtractive math correctly.
- The validator also normalizes recurring mortgage fee-table layouts where
  `Corporate Advances` is a subtotal and later filing/service detail lines are
  embedded inside that subtotal. In those cases the pipeline suppresses the
  duplicated `court_costs` read and carries the residual math itself.
- OCR text can misread an accrued `Per Diem Interest good through ...` amount as
  a daily `per_diem_rate`. The model now moves those accrued amounts into
  `per_diem_interest` before enforcing arithmetic.
- The validator also de-duplicates obvious amount collisions that come from OCR
  misreads, such as a `late_charges` value that is identical to `court_costs`
  when the judgment text does not actually mention late fees.
- Old caches should not block the new path forever, so the cache format version
  is bumped when the extraction contract changes materially.
- No new database tables or columns are required for this workflow. Validated
  judgments still persist to `foreclosures.judgment_data`.

## Triage Rules

`scripts/triage_judgments.py` is not just a schema checker. It now applies
three separate lenses:

1. Internal judgment consistency.
2. Property identity against HCPA.
3. Cross-document consistency against ORI / foreclosure state.

Two rules matter for interpreting the output correctly:

- A judgment is not treated as a bad extraction merely because it conflicts
  with the foreclosure row's current strap. If the extracted address /
  subdivision / lot / block are clearly grounded in the OCR text, the script
  downgrades that to a linkage warning and explicitly reports that the document
  likely belongs to an alternate parcel or that the foreclosure linkage is
  wrong.
- The triage output now separates live extraction failures from
  `LINKAGE_REVIEW`, `ARCHIVED_REVIEW`, and `ORPHANED_REVIEW`. The target for
  final-judgment extraction quality is `0` live `SUSPECT` / `BAD` /
  `CRITICAL` files; linkage-review cases are real data problems, but they are
  not counted as bad OCR/LLM extraction.
- Arithmetic triage uses the same OCR-derived credit recovery as the live
  validator, so old judgments with omitted `less payments` lines are not
  incorrectly kept in the `BAD` bucket.

## Operational Use

- Re-run extraction on `BAD` / `CRITICAL` judgments first.
- Re-run `scripts/triage_judgments.py` after bulk re-extraction.
- Treat the triage result as an extraction-quality report, not a persistence
  schema.
- When loading cached extracted judgments into PostgreSQL, revalidate against
  the current contract instead of trusting stale embedded `_validation` blobs
  from older cache files.
