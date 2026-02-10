# Pipeline Status Tracking

## Overview

The `status` table provides centralized tracking of pipeline progress for each auction case. It enables:

- **Resume capability** - Pick up exactly where we left off after crashes/interrupts
- **Progress visibility** - Real-time summary of pipeline state
- **Error tracking** - Know why cases failed and how many retries
- **Performance insights** - Timing data for each step

## Design Principles

1. **Source of truth** - Status table is authoritative for pipeline state
2. **Real-time updates** - Updated as each step completes (not batch)
3. **Case-level granularity** - Keyed by `case_number` (same property can have multiple cases)
4. **Idempotent** - Re-running a step on a completed case is safe (skipped)

## Schema

```sql
CREATE TABLE status (
    -- Primary key
    case_number VARCHAR PRIMARY KEY,
    parcel_id VARCHAR,
    auction_date DATE,

    -- Step completion timestamps (NULL = not started, timestamp = completed)
    step_auction_scraped TIMESTAMP,      -- Step 1: Auction data scraped
    step_pdf_downloaded TIMESTAMP,       -- Step 1: Final judgment PDF downloaded
    step_judgment_extracted TIMESTAMP,   -- Step 2: Judgment data extracted via vision
    step_bulk_enriched TIMESTAMP,        -- Step 3: Matched with HCPA bulk data
    step_homeharvest_enriched TIMESTAMP, -- Step 3.5: HomeHarvest MLS data
    step_hcpa_enriched TIMESTAMP,        -- Step 4: HCPA GIS sales history
    step_ori_ingested TIMESTAMP,         -- Step 5: ORI documents ingested
    step_survival_analyzed TIMESTAMP,    -- Step 6: Lien survival analysis
    step_permits_checked TIMESTAMP,      -- Step 7: Building permits
    step_flood_checked TIMESTAMP,        -- Step 8: FEMA flood zone
    step_market_fetched TIMESTAMP,       -- Step 9/10: Zillow/Realtor data
    step_tax_checked TIMESTAMP,          -- Step 12: Tax payment status

    -- Current state
    current_step INTEGER DEFAULT 0,      -- Last completed step number (0 = not started)
    status VARCHAR DEFAULT 'pending',    -- pending, processing, completed, failed, skipped

    -- Error tracking
    last_error VARCHAR,                  -- Most recent error message
    error_step INTEGER,                  -- Step that caused the error
    retry_count INTEGER DEFAULT 0,       -- Number of retry attempts

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP               -- When status became 'completed'
);

-- Index for date range queries
CREATE INDEX idx_status_auction_date ON status(auction_date);
CREATE INDEX idx_status_status ON status(status);
```

## Step Mapping

| Step | Column | Description |
|------|--------|-------------|
| 1 | `step_auction_scraped` | Foreclosure/tax deed auction data scraped |
| 1 | `step_pdf_downloaded` | Final judgment PDF downloaded |
| 2 | `step_judgment_extracted` | Vision/OCR extraction of judgment data |
| 3 | `step_bulk_enriched` | HCPA bulk parcel data matched |
| 3.5 | `step_homeharvest_enriched` | HomeHarvest MLS data fetched |
| 4 | `step_hcpa_enriched` | HCPA GIS sales history scraped |
| 5 | `step_ori_ingested` | ORI documents searched and ingested |
| 6 | `step_survival_analyzed` | Lien survival analysis completed |
| 7 | `step_permits_checked` | Building permits scraped |
| 8 | `step_flood_checked` | FEMA flood zone checked |
| 9/10 | `step_market_fetched` | Market data (Zillow/Realtor) fetched |
| 12 | `step_tax_checked` | Tax payment status checked |

## Status Values

| Status | Meaning |
|--------|---------|
| `pending` | Not yet started processing |
| `processing` | Currently being processed |
| `completed` | All applicable steps finished |
| `failed` | Failed with error (check `last_error`) |
| `skipped` | Intentionally skipped (e.g., invalid parcel_id) |

## Startup Summary

When `--update` runs, display a summary before processing:

```
============================================================
PIPELINE STATUS SUMMARY
============================================================
Date Range: 2025-01-02 to 2025-02-10 (40 days)

Total Auctions: 156
  Foreclosures: 142
  Tax Deeds:     14

By Status:
  Completed:    89 (57%)  ████████████████░░░░░░░░░░░░
  In Progress:  12 (8%)   ██░░░░░░░░░░░░░░░░░░░░░░░░░░
  Pending:      45 (29%)  ████████░░░░░░░░░░░░░░░░░░░░
  Failed:       10 (6%)   ██░░░░░░░░░░░░░░░░░░░░░░░░░░

Step Progress (of 156 auctions):
  Step 1  - Auction Scraped:     156/156 (100%)
  Step 1  - PDF Downloaded:      148/156 (95%)
  Step 2  - Judgment Extracted:  142/156 (91%)
  Step 3  - Bulk Enriched:       156/156 (100%)
  Step 3.5- HomeHarvest:         134/156 (86%)
  Step 4  - HCPA Enriched:       128/156 (82%)
  Step 5  - ORI Ingested:        115/156 (74%)
  Step 6  - Survival Analyzed:   108/156 (69%)
  Step 7  - Permits Checked:      98/156 (63%)
  Step 8  - Flood Checked:       102/156 (65%)
  Step 9  - Market Fetched:       95/156 (61%)
  Step 12 - Tax Checked:          89/156 (57%)

Recent Failures (showing 5 of 10):
  2024-CA-12345 @ Step 2: Vision extraction timeout
  2024-CA-12346 @ Step 5: ORI rate limited (retry 3/3)
  2024-CA-12347 @ Step 6: No encumbrances found
  ...

============================================================
Resuming pipeline...
============================================================
```

## CLI Commands

### Show Status Only
```bash
uv run main.py --status
uv run main.py --status --start-date 2025-01-02 --end-date 2025-02-10
```

### Retry Failed Cases
```bash
uv run main.py --update --retry-failed
uv run main.py --update --retry-failed --max-retries 3
```

### Verify Status Against Files
```bash
uv run main.py --verify
```
Scans `data/properties/*` and reconciles status table with actual files on disk.

## Implementation

### Updating Status

```python
# In orchestrator.py - after each step completes
await db.update_status(
    case_number=case_number,
    step="step_judgment_extracted",
    status="processing"  # or "completed" / "failed"
)

# On error
await db.update_status(
    case_number=case_number,
    status="failed",
    error_step=2,
    last_error="Vision extraction timeout after 30s"
)
```

### Querying Status

```python
# Get summary for date range
summary = db.get_status_summary(start_date, end_date)

# Get failed cases for retry
failed = db.get_failed_cases(start_date, end_date, max_retries=3)

# Check if step is complete
if db.is_step_complete(case_number, "step_ori_ingested"):
    logger.debug("Skipping ORI - already complete")
```

## Migration

For existing databases, run:
```python
db.initialize_status_from_auctions()
```

This populates the `status` table by inferring state from:
- `extracted_judgment_data IS NOT NULL` → `step_judgment_extracted`
- Existence of files in `data/properties/{folio}/documents/`
- Existing `needs_*` flags on auctions table

## Future Enhancements

1. **Progress bar** - Real-time progress bar during `--update`
2. **Webhooks** - Notify external systems on completion/failure
3. **Metrics export** - Prometheus/StatsD metrics for monitoring
4. **Batch status** - Track progress of batch runs, not just individual cases
