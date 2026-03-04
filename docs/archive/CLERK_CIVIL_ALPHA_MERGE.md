# Clerk Civil Alpha Index Merge

## Problem

The Hillsborough County Clerk publishes civil case data in two overlapping sources:

1. **Monthly Bulk Data** (`/Civil/bulkdata/`): CSV files covering cases filed in the last ~12 months. Loaded into `clerk_civil_cases` (73K rows, 2025-2026) and `clerk_civil_parties`.

2. **Civil Alphabetical Index** (`/Civil/alpha_index/{Circuit,County}/`): 1.5 GB of pipe-delimited files containing **1.86M+ party rows going back to 1958**. Previously loaded into a **separate** `clerk_name_index` table with no automated download.

### Bugs Fixed

- **UCN case number mismatch**: `clerk_name_index.case_number` stored UCN prefix format (`292008CA009351`) while `clerk_civil_cases` uses `08-CA-009351`. JOINs between these tables **never matched**.
- **No automated download**: Criminal name index had automated download; civil did not.
- **Denormalised duplicate**: `clerk_name_index` duplicated case+party data in its own table instead of using the normalised schema.

## Solution

### UCN Conversion

New function `_ucn_to_human_case_number()` converts UCN prefix to human format:

```
292008CA009351  →  strip county '29'  →  2008CA009351
                   year[2:] + '-' + court + '-' + seq
                   →  08-CA-009351
```

### Schema Changes (Alembic migration 004)

**`clerk_civil_cases`** — new columns:
- `court_type TEXT` — "Circuit" or "County"
- `status_date DATE` — current status date from alpha index

**`clerk_civil_parties`** — new columns:
- `suffix TEXT`, `business_name TEXT`
- `disposition_code TEXT`, `disposition_desc TEXT`, `disposition_date DATE`
- `amount_paid TEXT`, `date_paid DATE`, `akas TEXT`

**New indexes on `clerk_civil_parties`**:
- GIN trigram on `last_name`, `first_name`, `business_name`
- B-tree on `disposition_code`

### Loader Strategy

`load_civil_alpha_index()` reads the same pipe-delimited files but writes two upserts per row:

1. **Case upsert** → `clerk_civil_cases`: `ON CONFLICT (case_number) DO UPDATE` using `COALESCE(existing, new)` so richer bulk CSV data (style, judgment_*, cause_of_action) is never overwritten.

2. **Party upsert** → `clerk_civil_parties`: `ON CONFLICT (case_number, party_type, name) DO UPDATE` to fill in disposition/akas/address fields.

### New Service

`PgClerkCivilAlphaService` follows the `PgClerkCriminalService` pattern:
```
update() → init_db() → download_clerk_civil_alpha_index() → load_civil_alpha_index() → return stats
```

### Pipeline Integration

- Registered as `clerk_civil_alpha` step in `PgPipelineController` (runs after `clerk_criminal`)
- Registered as `clerk_civil_alpha` scheduled job (weekly, 2h max, singleton)
- CLI: `--skip-clerk-civil-alpha` flag
- `load-all` now routes civil alpha data through `load_civil_alpha_index()` instead of the removed legacy loader

### Web Query Changes

Both `properties.py` and `database_view.py` switched from `clerk_name_index` → `clerk_civil_parties JOIN clerk_civil_cases`.

## Usage

```bash
# Run migration
uv run alembic upgrade head

# Download civil alpha index files (~1.5 GB)
uv run python -m src.services.pg_loader_clerk download-civil-alpha-index

# Load into normalised tables
uv run python -m src.services.pg_loader_clerk load-civil-alpha-index

# Or run as scheduled job
uv run python -m src.tools.run_scheduled_job --job clerk_civil_alpha --force

# Full pipeline (includes new step)
uv run Controller.py
```

## Validation

```sql
-- Cases with court_type set (from alpha index)
SELECT COUNT(*) FROM clerk_civil_cases WHERE court_type IS NOT NULL;

-- Parties from alpha index
SELECT COUNT(*) FROM clerk_civil_parties WHERE source_file LIKE 'alpha:%';

-- Date range should span 1958-2026
SELECT MIN(filing_date), MAX(filing_date) FROM clerk_civil_cases;
```

## Rollback

```bash
# Rollback table drop + recreate clerk_name_index
uv run alembic downgrade 004_merge_civil_alpha

# Full rollback (also removes new columns, indexes, job config)
uv run alembic downgrade 003_seed_scheduled_jobs
```

## Table Removal

`clerk_name_index` has been **dropped** (migration 005). All alpha index data now lives in `clerk_civil_cases` + `clerk_civil_parties`. The `ClerkNameIndex` ORM model and `load_clerk_name_index()` loader function have been removed.
