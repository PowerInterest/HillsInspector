# Fix Judgment Extraction Logic

## Goal Description
The `FinalJudgmentProcessor` logic in `src/orchestrator.py` currently skips auctions with empty `parcel_id`s. This is incorrect because judgment extraction primarily relies on the `case_number` to locate the PDF file and run OCR/Vision analysis. The `parcel_id` is often extracted *from* the judgment or enriched later, so requiring it beforehand creates a catch-22 for some records.

## Proposed Changes

### Orchestrator
#### [MODIFY] [src/orchestrator.py](file:///mnt/c/code/HillsInspector/src/orchestrator.py)
- Remove `invalid_parcel_ids` set definition.
- Remove `AND a.parcel_id IS NOT NULL` from the SQL query in `process_judgment_extraction`.
- Remove the loop condition `if parcel_id.lower() in invalid_parcel_ids: continue`.
- Add `logger.debug` when `pdf_paths` is empty to improve observability.

## Verification Plan

### Automated Tests
- None specific for this change, but we will rely on manual verification of the code flow.

### Manual Verification
- Review the code changes to ensure the logic no longer relies on `parcel_id` for this step.
