-- One-time recovery: reset retry_count for auctions stuck due to non-critical step failures.
--
-- USAGE: Run manually via sqlite3 CLI or a Python script. NOT auto-run.
--   sqlite3 /path/to/property_master_sqlite.db < scripts/recovery_reset_stuck_auctions.sql
--
-- SAFETY:
--   - Only targets non-critical error_step values (3,4,7,8,9,12)
--   - Does NOT touch critical failures (step 2=judgment, 5=ORI, 6=survival)
--   - Scoped to pipeline_status IN ('processing', 'failed') with retry_count >= 3
--   - Sets status back to 'processing' so they re-enter the pipeline
--
-- Non-critical step numbers:
--   3  = homeharvest
--   4  = HCPA GIS
--   7  = permits
--   8  = FEMA flood
--   9  = market data
--   12 = tax

-- Step 1: Audit — see what will be affected
SELECT
    pipeline_status,
    error_step,
    retry_count,
    COUNT(*) AS cnt
FROM status
WHERE pipeline_status IN ('processing', 'failed')
  AND retry_count >= 3
  AND error_step IN (3, 4, 7, 8, 9, 12)
GROUP BY pipeline_status, error_step, retry_count
ORDER BY error_step, pipeline_status;

-- Step 2: Reset non-critical stuck rows
UPDATE status
SET
    retry_count = 0,
    pipeline_status = 'processing',
    last_error = NULL,
    error_step = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE pipeline_status IN ('processing', 'failed')
  AND retry_count >= 3
  AND error_step IN (3, 4, 7, 8, 9, 12);

-- Step 3: Verify — count remaining stuck rows (should be critical-only)
SELECT
    pipeline_status,
    error_step,
    retry_count,
    COUNT(*) AS cnt
FROM status
WHERE pipeline_status IN ('processing', 'failed')
  AND retry_count >= 3
GROUP BY pipeline_status, error_step, retry_count
ORDER BY error_step, pipeline_status;
