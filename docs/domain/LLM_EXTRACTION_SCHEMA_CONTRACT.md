# LLM Extraction Schema Contract

The extraction models in `src/models/` are the hard contract between OCR text,
the LLM, and PostgreSQL persistence. They are not documentation-only types.
They now define what the model must emit, what validation rejects, and which
final-judgment extracts are considered unusable.

## Core Contract

- Every declared JSON key must be present in the LLM response.
- Unknown keys are forbidden.
- Nullable fields must be emitted as `null` when unknown, not omitted.
- `raw_text` is pipeline-supplied context and is excluded from the LLM schema.
- Nested objects follow the same rule as root objects: all declared keys must
  be present and no extra keys are allowed.

This is enforced in two places:

- `StrictExtractionModel.model_validate(...)` rejects partial objects before
  field coercion.
- `StrictExtractionModel.model_json_schema()` marks object properties as
  required and sets `additionalProperties: false`, so constrained decoding
  backends see the same contract the validator enforces.

## Enum Handling

Enum validators may normalize known aliases, but they do not silently collapse
unknown values into catch-all buckets like `OTHER` or `UNKNOWN`.

That means:

- `FIRST` may normalize to `FIRST MORTGAGE`
- `DEED OF TRUST` may normalize to `DOT`
- `bankish` or `totally custom mortgage` will fail validation

This is intentional. Silent fallback hides prompt failures and makes post-run
quality review impossible.

## Final Judgment Hard Gates

`JudgmentExtraction` now fails validation when any of these conditions hold:

- missing `case_number`
- missing `judgment_date`
- missing `total_judgment_amount`
- missing `foreclosure_sale_date`
- missing both `legal_description` and `property_address`
- empty `defendants` without an explicit explanation in `unclear_sections`
- known itemized financial fields exceed `total_judgment_amount`
- `sale_location` clearly indicates an online sale while `is_online_sale` is
  `false`

## Final Judgment Arithmetic

The local model is not trusted to add money fields correctly. The pipeline
computes the rollup itself:

- `principal_amount`, `interest_amount`, `per_diem_interest`,
  `late_charges`, `escrow_advances`, `title_search_costs`, `court_costs`, and
  `attorney_fees` are treated as the known line items
- if those known items sum to less than the stated `total_judgment_amount`,
  the validator recomputes `other_costs` as the residual
- if the known items exceed the stated total, validation fails
- if the residual is unusually large, validation emits a warning because a
  major line item may still be missing or misread

This lets the pipeline reject impossible arithmetic without failing merely
because the model did not do the subtraction itself.

## Canonical Persistence

Only validated final-judgment caches are allowed to become canonical
`foreclosures.judgment_data`.

- invalid or partial caches may still exist on disk for review/retry
- canonical PG persistence is reserved for caches whose `_validation.is_valid`
  is true
- legacy v1 caches that do not yet have `_validation` metadata are
  revalidated during PG load instead of being silently skipped
- canonicalization strips private metadata keys before persistence and writes
  the normalized Pydantic payload, not the raw model response

## Prompt Guidance Safety

Prompt descriptions embedded in the models carry Hillsborough-specific
normalization rules, but those rules must not erase real parties. Example:

- judges are never parties
- the Clerk of Court must still be preserved if the document actually names
  the clerk as a party

Model descriptions are therefore part of the extraction logic and must be
reviewed with the same care as validators.

## Expected Usage Pattern

1. Generate the schema with `SomeExtractionModel.model_json_schema()`.
2. Send OCR text plus that schema to the LLM using structured output /
   constrained decoding. This applies to local OpenAI-compatible endpoints as
   well as cloud endpoints; do not silently drop `response_format` on the
   local path.
3. Run `SomeExtractionModel.model_validate(...)` on the response.
4. If validation fails, store the raw response and mark the extraction for
   retry or review. Do not silently persist a partial dict.
5. For final judgments, treat validation failure as extraction failure. The
   downstream title, survival, and bidding analysis depends on those fields
   being trustworthy.
