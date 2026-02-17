# Auto-Archive Past Auctions

## Problem

The pipeline processes ALL incomplete auctions regardless of whether their auction date has passed. Steps 2-12 have no date-based cutoff — only Step 1 (scraping) uses the date window. This wastes hours per run on auctions that are no longer actionable.

## Solution

After Step 1 completes (and before Step 2 begins), bulk-mark past-date auctions as `archived` in the `status` table. The existing step-level selectors were updated to also exclude `'archived'` alongside `'completed'` and `'skipped'`.

## How It Works

### Status Value

Archived auctions get `pipeline_status = 'archived'` — a value distinct from:
- `'skipped'` — used for data-quality issues (invalid parcel, bad data)
- `'completed'` — fully processed
- `'failed'` — step failure, eligible for retry

This separation preserves signal about why a case was excluded.

### Grace Window

Archival uses a 7-day grace period by default:

```
archive if: normalize_date(auction_date) < (today_local() - 7 days)
```

`today_local()` matches the timezone used by the rest of the pipeline (from `src/utils/time`). The 7-day window allows late-arriving PDFs, judgment data, and downstream enrichment to complete before a case is archived.

### Archive Criteria

A case is archived when ALL of these are true:
- `auction_date` is more than 7 days in the past
- `auction_date` is parseable (see NULL date policy below)
- `pipeline_status` is NOT already `completed`, `skipped`, or `archived`

This means `pending`, `processing`, and `failed` past auctions all get archived. For previously `failed` cases, the original `last_error` value is preserved (not overwritten with `'auction_date_passed'`) so diagnostic info is retained.

### NULL / Unparseable Auction Dates

Cases where `normalize_date(auction_date)` returns NULL (unparseable or missing dates) are **left active** — they are never archived automatically. This is intentional: these cases need manual attention (the date should be fixed), not silent archival. If they accumulate, investigate why the scraper produced bad dates.

### Where It Runs

The archive/un-archive logic runs **outside** the `if start_step <= 1:` guard in `run_full_update()`, so it applies even when resuming with `--start-step 2`.

### Step Query Changes

The following selectors were updated to exclude `'archived'`:

| Location | Original Filter | Updated Filter |
|----------|----------------|----------------|
| `get_auctions_for_processing()` (operations.py) | `NOT IN ('completed', 'skipped')` | `NOT IN ('completed', 'skipped', 'archived')` |
| Step 2 judgment query (orchestrator.py) | `!= 'skipped'` | `NOT IN ('skipped', 'archived')` |
| Step 3 bulk enrichment (orchestrator.py) | `NOT IN ('completed', 'skipped')` | `NOT IN ('completed', 'skipped', 'archived')` |

## Override: `--process-past-auctions`

To reprocess past auctions (backfills, re-extraction after code changes, debugging):

```powershell
uv run main.py --update --process-past-auctions
```

This flag does two things:
1. **Un-archives** all previously archived cases (restoring `failed` cases to `'failed'` with their original `last_error`, and others to `'pending'`)
2. **Skips** the archival step for this run

After the run completes, a subsequent normal run (without the flag) will re-archive any past cases that are still incomplete.

## Manual Un-Archive (SQL)

For targeted restoration without running the full pipeline:

```sql
-- Un-archive all
UPDATE status SET
    pipeline_status = CASE
        WHEN last_error IS NOT NULL AND last_error != 'auction_date_passed' THEN 'failed'
        ELSE 'pending'
    END,
    last_error = CASE
        WHEN last_error = 'auction_date_passed' THEN NULL
        ELSE last_error
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE pipeline_status = 'archived';

-- Un-archive specific case
UPDATE status SET pipeline_status = 'pending', last_error = NULL
WHERE case_number = '292024CA012345XXXXXX';
```

## Pipeline Completeness Validation

The success thresholds in CLAUDE.md measure data completeness. With archiving, the denominators should scope to **active (non-archived) auctions** to avoid counting intentionally-excluded past cases as failures. Archived cases are by definition no longer actionable — they don't reduce the quality of the current run's output.

## Verification

```sql
-- Count archived auctions
SELECT COUNT(*) FROM status WHERE pipeline_status = 'archived';

-- Archived with preserved failure info
SELECT COUNT(*) FROM status
WHERE pipeline_status = 'archived'
  AND last_error IS NOT NULL
  AND last_error != 'auction_date_passed';

-- Verify no stale past auctions remain pending
SELECT s.case_number, normalize_date(a.auction_date) AS auction_date
FROM status s
JOIN auctions a ON a.case_number = s.case_number
WHERE normalize_date(a.auction_date) < date('now', '-7 days')
  AND s.pipeline_status NOT IN ('completed', 'skipped', 'archived');

-- NULL-date cases that need attention
SELECT s.case_number, a.auction_date
FROM status s
JOIN auctions a ON a.case_number = s.case_number
WHERE normalize_date(a.auction_date) IS NULL
  AND s.pipeline_status NOT IN ('completed', 'skipped', 'archived');
```

## Files Modified

| File | Change |
|------|--------|
| `src/db/operations.py` | `archive_past_auctions()` + `unarchive_past_auctions()` methods |
| `src/db/operations.py` | Updated `get_auctions_for_processing()` filter to exclude `'archived'` |
| `src/orchestrator.py` | `skip_past_auctions` param, archive/un-archive call, updated Step 2+3 filters |
| `main.py` | `--process-past-auctions` CLI flag threaded to orchestrator |
