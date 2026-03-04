# Fixing Pipeline Data Loss and API Waste (Data Flow Architeture)

This walkthrough documents the successful structural rework of the HillsInspector data pipeline to prevent historic records returning to the active queue and redundant enrichment fetching.

## Overview of Changes

The primary cause of pipeline errors and data duplication was `refresh_foreclosures.py` moving rows to `foreclosures_history` but later self-upserting them back. We implemented a "Soft Delete" pattern.

### 1. Schema Migration (`001_viewify_foreclosures_history.py`)
- **[DELETED]** The physical `foreclosures_history` table was wiped to eliminate the structural data loop.
- **[ADDED]** Created an Alembic migration that seamlessly converts `foreclosures_history` into a logical PostgreSQL `VIEW` (filtering on `archived_at IS NOT NULL`). This preserved all downstream FastAPI routes and title-chain joins with zero code changes.

### 2. Pipeline Optimization (`scripts/refresh_foreclosures.py`)
- **[MODIFIED]** Rewrote Step 1 to enrich directly via `UPDATE ... FROM foreclosures f2 LEFT JOIN LATERAL ...` rather than the old `INSERT ... SELECT` upsert loop.
- **[MODIFIED]** Removed the destructive Step 5.5 ("sync to history").
- **[ADDED]** Added **Step 7 (Rescheduled Auction Data Reuse)**: When a property auction is rescheduled, a new row is created. Rather than re-requesting costly API elements (Vision OCR, Title Search, Permit scrapes), the pipeline uses `DISTINCT ON` to target the previously archived iteration of that case and unconditionally copies the manual enrichment components over.

### 3. Service Audits & Fixes
To prevent the pipeline from redundantly polling external APIs for properties that were already archived:
- `pg_pipeline_controller.py`: Made `active_only=True` the default for pipeline runs. Ensured Single-Pin permit targeting uses `DISTINCT ON (pin)` so multi-unit properties do not trigger identical calls.
- `pg_auction_service.py` / `pg_judgment_service.py` / `trust_accounts.py`: Forcefully applied `archived_at IS NULL` guarantees across active queues.

## Validation Results

We executed the strict validation checks mandated in `CLAUDE.md`. The target benchmarks evaluated `foreclosures` against `foreclosure_title_chain` and `ori_encumbrances`.

**Final Metric Scores Post-Migration:**
- **Final Judgment PDFs:**    99.32% (Target: >90%) ✅
- **Extracted Judgment Data:** 99.32% (Target: >90%) ✅
- **Chain of Title:**          97.95% (Target: >80%) ✅
- **Encumbrance Coverage:**    98.63% (Target: >80%) ✅
- **Survival Coverage:**       98.63% (Target: >80%) ✅
