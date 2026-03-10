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
- itemized financial fields do not reconcile to `total_judgment_amount`
- `sale_location` clearly indicates an online sale while `is_online_sale` is
  `false`

The amount reconciliation is intentionally strict. Final judgments are
itemization-heavy court orders, so a material drift is more likely to mean
bad extraction than acceptable fuzz.

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
   constrained decoding.
3. Run `SomeExtractionModel.model_validate(...)` on the response.
4. If validation fails, store the raw response and mark the extraction for
   retry or review. Do not silently persist a partial dict.
5. For final judgments, treat validation failure as extraction failure. The
   downstream title, survival, and bidding analysis depends on those fields
   being trustworthy.
