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
- Old caches should not block the new path forever, so the cache format version
  is bumped when the extraction contract changes materially.
- No new database tables or columns are required for this workflow. Validated
  judgments still persist to `foreclosures.judgment_data`.

## Operational Use

- Re-run extraction on `BAD` / `CRITICAL` judgments first.
- Re-run `scripts/triage_judgments.py` after bulk re-extraction.
- Treat the triage result as an extraction-quality report, not a persistence
  schema.
