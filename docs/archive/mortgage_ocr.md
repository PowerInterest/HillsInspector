# Mortgage OCR Extraction

## Overview

**Module**: `src/services/pg_mortgage_extraction_service.py`  
**Pipeline Step**: Phase B Step 5 (`mortgage_extract`) in `Controller.py`

After `ori_search` discovers encumbrances and saves them into `ori_encumbrances`, this step downloads the actual mortgage deed PDFs from the Hills Clerk and uses Vision AI to extract structured loan data.

## Architecture

```
ori_encumbrances (mortgage, ori_id IS NOT NULL)
         │
         ▼
PgMortgageExtractionService.run()
         │
         ├── Playwright browser session
         │       └── download PDF via PAV document URL:
         │           https://publicaccess.hillsclerk.com/PAVDirectSearch/api/Document/{ori_id}/?OverlayMode=View
         │
         ├── ScraperStorage.save_document()  → data/properties/{storage_id}/mortgage_{instrument}.pdf
         │
         └── VisionService.extract_json(pdf, MORTGAGE_PROMPT)
                 └── Returns JSON: { principal_amount, interest_rate, loan_type, seniority, ... }
                         │
                         ▼
                UPDATE ori_encumbrances
                SET mortgage_data = JSONB,
                    amount = principal_amount
```

## Key Design Decisions

### Using `ori_id` (not Instrument Number search)
The PAV `/PAVDirectSearch/api/Document/{id}` endpoint requires an internal opaque document ID, not the instrument number. This ID is acquired in two ways:

1. **During `ori_search`** — `PgOriService._parse_pav_rows()` extracts `ID` from each PAV API result row and stores it in `ori_encumbrances.ori_id` on insert/upsert.
2. **Backfill** — `scripts/backfill_mortgage_ori_id.py` queries the PAV "Instrument #" search (`QueryID=320`, `KeywordId=1006`) for each existing mortgage that has `ori_id = NULL`.

### Why mortgages lack `ori_id` initially
`PgOriService` has two document discovery paths:
- **PAV API path** (`_pav_search → _parse_pav_rows`): Returns `ID` in each result row ✅
- **Local PG path** (`_search_official_records_pg`): Queries `official_records_daily_instruments` table which has no PAV document IDs ❌

Most mortgages are initially discovered via the local PG path. The backfill script corrects this.

## Database Schema

```sql
-- Column added by migration
ALTER TABLE ori_encumbrances ADD COLUMN IF NOT EXISTS mortgage_data JSONB;

-- ori_id already existed; populated by PgOriService on API discovery
-- The mortgage extractor requires this field: WHERE ori_id IS NOT NULL
```

## Extracted Fields (MORTGAGE_PROMPT)

| Field | Description |
|-------|-------------|
| `principal_amount` | Original loan amount in USD |
| `interest_rate` | Annual interest rate (fixed or initial ARM) |
| `loan_type` | CONVENTIONAL, FHA, VA, etc. |
| `seniority` | "1st", "2nd", "3rd" lien priority |
| `maturity_date` | Loan maturity date |
| `lender_name` | Name of the lender |
| `borrower_name` | Name(s) of the borrower(s) |

## Pipeline Flags

| Flag | Effect |
|------|--------|
| `--skip-mortgage-extract` | Skip this step in `Controller.py` |
| `--mortgage-limit N` | Process only N mortgages (testing) |

## Running Standalone

```bash
# Backfill ori_id for existing mortgages (required before extraction)
uv run python scripts/backfill_mortgage_ori_id.py

# Run just the extraction step (leave ori_search running first)
uv run python Controller.py \
  --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr \
  --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits \
  --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain \
  --skip-title-breaks --skip-market-data --skip-auction-scrape \
  --skip-judgment-extract --skip-identifier-recovery --skip-ori-search \
  --skip-survival --mortgage-limit 5
```

## Validation Queries

```sql
-- How many mortgages have been extracted?
SELECT
  COUNT(*) AS total_mortgages,
  COUNT(*) FILTER (WHERE ori_id IS NOT NULL) AS have_ori_id,
  COUNT(*) FILTER (WHERE mortgage_data IS NOT NULL) AS extracted
FROM ori_encumbrances
WHERE encumbrance_type = 'mortgage';

-- Sample extracted data
SELECT instrument_number, amount,
       mortgage_data->>'interest_rate' AS rate,
       mortgage_data->>'seniority' AS seniority,
       mortgage_data->>'loan_type' AS loan_type
FROM ori_encumbrances
WHERE mortgage_data IS NOT NULL
LIMIT 10;
```
