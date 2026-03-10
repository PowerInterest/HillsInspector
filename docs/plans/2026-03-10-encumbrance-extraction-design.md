# Encumbrance Extraction Design

**Date:** 2026-03-10
**Status:** Approved

## Goal

Extract structured data from all ORI document PDFs (mortgages, deeds, liens, lis pendens, satisfactions, assignments, NOCs) using pytesseract OCR + Qwen 3.5 9B, validate with Pydantic schemas, cache locally, and persist to PostgreSQL.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage column | Single `extracted_data` JSONB on `ori_encumbrances` | `encumbrance_type` already identifies schema; one column, one pattern |
| Column migration | Rename `mortgage_data` → `extracted_data` | No backfill needed, existing data preserved |
| Service architecture | Single unified service with dispatch dict | Only difference between types is vision method + Pydantic model |
| PDF download strategy | On-demand during extraction | Matches mortgage service pattern, avoids storing unneeded PDFs |
| Processing model | Sequential with `--extraction-limit N` | Vision endpoints are bottleneck; avoids timeout/suspension risk |
| Pipeline position | Between ORI search and survival analysis | Extraction data available to survival logic |
| Doc types in scope | All 7: mortgage, deed, lien, lis_pendens, satisfaction, assignment, noc | Full extraction coverage |
| NOCs | Included | Valuable for mechanics' lien priority, contractor/owner/timeline data |
| Existing mortgage service | Replaced by unified service | Eliminates duplication |

## Database Changes

Single Alembic migration:

```sql
ALTER TABLE ori_encumbrances RENAME COLUMN mortgage_data TO extracted_data;
```

## New File: `src/services/pg_encumbrance_extraction_service.py`

Unified extraction service (~500-700 lines). Core design:

### Dispatch Table

```python
EXTRACTION_DISPATCH: dict[str, tuple[str, type[BaseDocumentExtraction]]] = {
    "mortgage":    ("extract_mortgage",        MortgageExtraction),
    "judgment":    ("extract_final_judgment",   JudgmentExtraction),
    "lis_pendens": ("extract_lis_pendens",      LisPendensExtraction),
    "lien":        ("extract_lien",             LienExtraction),
    "satisfaction":("extract_satisfaction",      SatisfactionExtraction),
    "assignment":  ("extract_assignment",        AssignmentExtraction),
    "noc":         ("extract_noc",              NOCExtraction),
    "easement":    ("extract_deed",             DeedExtraction),
    "other":       ("extract_deed",             DeedExtraction),
}
```

### Per-Document Flow

1. Query `ori_encumbrances WHERE extracted_data IS NULL AND ori_id IS NOT NULL`
2. Dispatch by `encumbrance_type`
3. Check `{stem}_extracted.json` cache → load if exists, skip vision
4. Download PDF via PAV API (`/PAVDirectSearch/api/Document/{encoded_id}/?OverlayMode=View`)
5. Render first 3 pages to PNG (PyMuPDF, 150 DPI)
6. Send each page to dispatched vision method, merge partial results
7. Validate via `PydanticModel.model_validate(raw_dict)`
8. Write `{stem}_extracted.json` cache next to PDF
9. `UPDATE ori_encumbrances SET extracted_data = validated_dict WHERE id = ?`

### Key Methods

- `extract_encumbrances(limit: int | None)` — main entry point
- `_download_pdf(encumbrance) -> Path` — PAV API download via Playwright
- `_extract_document(pdf_path, encumbrance_type) -> dict` — render + vision + validate
- `_load_or_extract(encumbrance) -> dict | None` — cache-first logic
- `_merge_page_results(pages: list[dict]) -> dict` — fill missing fields from subsequent pages

## Controller Integration

- New step in `PgPipelineController` between ORI search and survival analysis
- New CLI flags: `--extraction-limit N`, `--skip-encumbrance-extraction`
- Replaces existing mortgage extraction step and its flags

## File Changes

| File | Change |
|------|--------|
| `src/services/pg_encumbrance_extraction_service.py` | **NEW** — unified extraction service |
| `alembic/versions/XXXX_rename_mortgage_data.py` | **NEW** — rename column migration |
| `src/services/pg_pipeline_controller.py` | Add extraction step, remove mortgage extraction step |
| `Controller.py` | Add `--extraction-limit`, `--skip-encumbrance-extraction` flags |
| `src/services/pg_mortgage_extraction_service.py` | **DEPRECATE** — replaced by unified service |
| `src/services/pg_survival_service.py` | Update `mortgage_data` references → `extracted_data` |
| `src/services/pg_ori_service.py` | Update `mortgage_data` references → `extracted_data` |
| `app/` (web routes/templates) | Update `mortgage_data` references → `extracted_data` |
| `sunbiz/models.py` | Rename column in ORM model |
| `tests/` | New tests for unified service; update existing mortgage tests |

## Testing

- Dispatch table coverage: each `encumbrance_type` maps to correct vision method + model
- Cache hit/miss logic with fixture `_extracted.json` files
- Pydantic validation per doc type with sample extracted JSON
- Integration: mock vision service, verify full flow from query → cache → DB write
