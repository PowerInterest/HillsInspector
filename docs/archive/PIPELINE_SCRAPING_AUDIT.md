# Pipeline Scraping Audit — Redundancy, archived_at, and Rescheduled Auctions

**Date:** 2026-02-27
**Scope:** All Phase A bulk services, Phase A per-foreclosure services, Phase B enrichment services, and controller dispatch logic.

---

## Executive Summary

The pipeline has **strong idempotency** at the DB layer (UPSERTs, step flags, SHA dedup) but has **gaps in scope filtering** that cause unnecessary external API calls and processing. The key issues:

| Category | Severity | Count |
|----------|----------|-------|
| Missing `archived_at` filter | HIGH | 4 services |
| Redundant external scraping on reschedule | MEDIUM | 3 services |
| Unnecessary full re-fetch | LOW | 2 services (already fixed) |

---

## Table of Contents

1. [Rescheduled Auction Lifecycle](#1-rescheduled-auction-lifecycle)
2. [Phase A Bulk Services](#2-phase-a-bulk-services)
3. [Phase A Per-Foreclosure Services](#3-phase-a-per-foreclosure-services)
4. [Phase B Enrichment Services](#4-phase-b-enrichment-services)
5. [Summary Matrix](#5-summary-matrix)
6. [Recommended Fixes](#6-recommended-fixes)

---

## 1. Rescheduled Auction Lifecycle

### How It Works Today

The `foreclosures` table has `UNIQUE (case_number_raw, auction_date)`. When an auction is rescheduled:

1. **Old row** (case 25-1234, date 2026-03-15) keeps its `foreclosure_id`, step flags, and enrichment data
2. **New row** (case 25-1234, date 2026-04-01) gets a **new** `foreclosure_id` with all step flags NULL
3. When 2026-03-15 passes, `refresh_foreclosures.py` Step 5 sets `archived_at = now()` on the old row
4. Phase B services filter `WHERE archived_at IS NULL`, so the old row stops being processed

### What This Means for Each Service

The new row has all `step_*` flags NULL, so **every Phase B enrichment step re-runs from scratch** — even though the legal case, PDFs, ORI documents, and survival analysis are identical. The only thing that changed is the auction date.

**Data NOT migrated to new row:**

| Data | Storage | Re-fetched? |
|------|---------|-------------|
| Judgment PDF | Disk: `data/Foreclosure/{case}/documents/` | Same path (shared by case_number) |
| judgment_data JSON | `foreclosures.judgment_data` column | Vision re-extraction NOT needed (JSON cache on disk) |
| ORI encumbrances | `ori_encumbrances` table (keyed by strap) | API re-called (7-day PAV cache helps) |
| Survival status | `ori_encumbrances.survival_status` | Re-analyzed (no external API, just PG reads) |
| Market data | `property_market` table (keyed by strap) | NOT re-fetched (strap-level dedup works) |

### The Window of Waste

Between when the new row is created and when the old row is archived (i.e., its auction_date passes), **both rows are active** (`archived_at IS NULL`). During this window, Phase B services process BOTH rows — doubling Vision API calls, ORI searches, and survival analysis for the same property.

---

## 2. Phase A Bulk Services

These are reference data loaders. They pull data for the entire county, not per-foreclosure.

### 2.1 HCPA Suite

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | HCPA FTP (parcels, sales, subdivisions) | |
| **Redundant scraping** | LOW | SHA-256 in `ingest_files` prevents re-processing same ZIP. FTP is re-synced every run but file-level dedup catches it. |
| **archived_at** | N/A | Parcel-level data, not foreclosure-scoped |
| **Reschedule** | N/A | Not case-linked |

**File:** `sunbiz/pg_loader.py` (load_hcpa_suite)

### 2.2 Clerk Bulk

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | Clerk civil bulk CSVs | |
| **Redundant scraping** | NONE | SHA-256 dedup; skips unchanged files |
| **archived_at** | N/A | Case-level data, not foreclosure-scoped |
| **Reschedule** | SAFE | UPSERT by case_number; idempotent |

**File:** `src/services/pg_clerk_bulk_service.py`

### 2.3 DOR NAL

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | Florida DOR FTP (annual tax rolls) | |
| **Redundant scraping** | NONE | Annual by tax year; SHA dedup |
| **archived_at** | N/A | Parcel-level |
| **Reschedule** | N/A | Not case-linked |

**File:** `src/services/pg_nal_service.py`

### 2.4 Sunbiz FLR

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | Florida DOS SFTP (quarterly UCC filings) | |
| **Redundant scraping** | NONE | SHA dedup on quarterly ZIPs |
| **archived_at** | N/A | Debtor-level |
| **Reschedule** | N/A | Not case-linked |

**File:** `src/services/pg_flr_service.py`

### 2.5 Sunbiz Entity

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | Florida DOS SFTP (quarterly entity filings) | |
| **Redundant scraping** | NONE | SHA dedup on quarterly manifests |
| **archived_at** | N/A | Entity-level |
| **Reschedule** | N/A | Not case-linked |

**File:** `sunbiz/pg_loader.py` (load_sunbiz_entity)

### 2.6 County Permits (ArcGIS)

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | ArcGIS FeatureServer (Building Permits) | |
| **Redundant scraping** | FIXED | Now uses `OBJECTID > MAX` for incremental fetch |
| **archived_at** | N/A | Property-level permit data, not foreclosure-scoped |
| **Reschedule** | SAFE | Permits keyed by folio, not auction date |

**File:** `src/services/CountyPermit.py`

### 2.7 Tampa Permits (Accela)

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | Tampa Accela (Playwright export) | |
| **Redundant scraping** | FIXED | Now uses `MAX(record_date)` for incremental date window |
| **archived_at** | N/A | Property-level permit data, not foreclosure-scoped |
| **Reschedule** | SAFE | Permits keyed by record_number; UPSERT idempotent |

**File:** `src/services/TampaPermit.py`

### Phase A Verdict

Phase A services are **reference data loaders** — they pull county/state data, not foreclosure-specific data. `archived_at` filtering is not applicable here because these tables serve ALL properties, not just active foreclosures. The two time-window redundancies (County, Tampa) have been fixed.

---

## 3. Phase A Per-Foreclosure Services

### 3.1 Foreclosure Refresh

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | None (PG only) | |
| **Redundant work** | HIGH | Re-computes ALL derived fields for ALL active rows every run |
| **archived_at** | SETS IT | Step 5 archives rows where `auction_date < CURRENT_DATE` |
| **Reschedule** | SAFE | Old row archived independently; new row processed normally |

**Concern:** Refresh runs unconditionally. Steps 1-4 (normalize, cross-fill, compute equity, etc.) touch every active row even if nothing changed. Not a scraping concern (no external API) but wastes PG compute.

**File:** `scripts/refresh_foreclosures.py`

### 3.2 Trust Accounts

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | Hills Clerk trust balance PDFs | |
| **Redundant scraping** | LOW | Smart date-based gap-fill (only fetches new report dates) |
| **archived_at** | **MISSING** | Trust balance matching does NOT filter `WHERE archived_at IS NULL` |
| **Reschedule** | **ISSUE** | Same case in trust reports for two auction dates → ambiguous prior-amount lookup |

**Fix needed:** Add `archived_at IS NULL` filter when matching trust balances to foreclosures.

**File:** `src/services/pg_trust_accounts.py`

### 3.3 Title Chain

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | None (PG reads only) | |
| **Redundant work** | LOW | Idempotent rebuild (truncate + reinsert) |
| **archived_at** | PARTIAL | Has `active_only` param but **defaults to False** in controller call |
| **Reschedule** | SAFE | Property-level (strap), not case-linked |

**Fix needed:** Controller should pass `active_only=True`.

**File:** `src/services/pg_title_chain_controller.py`

### 3.4 Single-Pin Permits

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | HCPA ParcelData API + ArcGIS + Tampa Accela | |
| **Redundant scraping** | MEDIUM | Same PIN fetched for BOTH old and new foreclosure rows if both active |
| **archived_at** | **MISSING** | Default `active_only=False`; processes archived foreclosures too |
| **Reschedule** | **ISSUE** | Same property scraped twice (once per active foreclosure_id) |

**Fix needed:** Add `archived_at IS NULL` to candidate selection. Consider dedup by strap.

**File:** `src/services/pg_permit_single_pin_service.py`

---

## 4. Phase B Enrichment Services

### 4.1 Auction Scrape

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | `hillsborough.realforeclose.com` (Playwright) | |
| **Redundant scraping** | LOW | Scrapes by auction_date, UPSERTs by (case_number_raw, auction_date) |
| **archived_at** | **MISSING** | `_dates_with_auctions()` does NOT filter `WHERE archived_at IS NULL` |
| **Reschedule** | **ISSUE** | If an archived row exists for a date, that date may be skipped entirely, preventing new auctions on that date from being scraped |

**Fix needed:** Add `AND archived_at IS NULL` to `_dates_with_auctions()` query.

**File:** `src/services/pg_auction_service.py`, lines 54-65

### 4.2 Judgment Extract

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | Vision API (local GLM or Gemini cloud) | |
| **Redundant scraping** | LOW | `{pdf_stem}_extracted.json` cache on disk; skips if JSON exists |
| **archived_at** | PARTIAL | Filesystem scan has NO filter; PG upload DOES filter `WHERE archived_at IS NULL` |
| **Reschedule** | MITIGATED | Same case_number → same disk path → JSON cache hit. But case_map lookup may target wrong foreclosure_id if old row still active |

**Concern:** The filesystem scan (`_find_unextracted_pdfs`) processes any PDF on disk regardless of whether the foreclosure is archived. Vision API is NOT called redundantly (JSON cache), but the case_map used for PG upload resolves to the **first** matching case_number, which could be the wrong (old) foreclosure_id during the overlap window.

**File:** `src/services/pg_judgment_service.py`, lines 50-82 (scan), 115 (PG filter)

### 4.3 ORI Search

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | PAV CustomQuery API (Official Records) | |
| **Redundant scraping** | MITIGATED | 7-day PAV cache in `data/cache/pav_api/`; step flag prevents re-run |
| **archived_at** | **CORRECT** | `WHERE f.step_ori_searched IS NULL AND f.archived_at IS NULL` |
| **Reschedule** | MITIGATED | New row triggers fresh ORI search, but PAV cache serves same responses for same strap. Encumbrances written to `ori_encumbrances` by strap (shared across foreclosure_ids). |

**Minor concern:** Both old and new foreclosure rows link to the same encumbrances via strap. Not a data correctness issue, but web UI should filter by active foreclosure only.

**File:** `src/services/pg_ori_service.py`, line 325-326

### 4.4 Survival Analysis

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | None (PG reads only) | |
| **Redundant work** | LOW | Idempotent writes to `ori_encumbrances.survival_status` |
| **archived_at** | **CORRECT** | `WHERE f.step_survival_analyzed IS NULL AND f.archived_at IS NULL` |
| **Reschedule** | SAFE | Encumbrances are strap-level; survival_status written once per encumbrance ID |

**File:** `src/services/pg_survival_service.py`, line 107-109

### 4.5 Market Data

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | Redfin, Zillow, HomeHarvest (Playwright) | |
| **Redundant scraping** | NONE | Keyed by strap in `property_market` table; gap-fill query only selects missing |
| **archived_at** | **CORRECT** | `WHERE f.archived_at IS NULL` |
| **Reschedule** | SAFE | Strap-level dedup; both foreclosure rows see same market data |

**File:** `src/services/market_data_worker.py`, line 26

### 4.6 Final Refresh

| Aspect | Status | Details |
|--------|--------|---------|
| **Endpoint** | None (PG only) | |
| **Redundant work** | LOW | Idempotent UPSERT from foreclosures_history |
| **archived_at** | CORRECT | Sets archived_at; respects it in sub-queries |
| **Reschedule** | SAFE | Independent rows per (case_number, auction_date) |

**File:** `scripts/refresh_foreclosures.py`

---

## 5. Summary Matrix

### archived_at Filtering

| Service | Filters archived_at? | Impact |
|---------|---------------------|--------|
| Auction Scrape | **NO** | May skip valid dates if archived row exists |
| Judgment Extract (scan) | **NO** | Processes PDFs for archived cases (cache saves Vision calls) |
| Trust Accounts | **NO** | Matches trust balances to archived foreclosures |
| Single-Pin Permits | **NO** (default) | Scrapes permits for archived properties |
| Title Chain | **NO** (default) | Rebuilds chain for archived properties |
| ORI Search | YES | |
| Survival Analysis | YES | |
| Market Data | YES | |
| Final Refresh | YES (sets it) | |

### Rescheduled Auction Impact

| Service | External API Waste | Data Correctness |
|---------|-------------------|-----------------|
| Auction Scrape | LOW (same clerk page) | SAFE (UPSERT) |
| Judgment Extract | NONE (JSON cache on disk) | **RISKY** (case_map may target wrong row) |
| ORI Search | LOW (PAV 7-day cache) | SAFE (strap-level) |
| Single-Pin Permits | **HIGH** (same PIN scraped 2x) | SAFE (UPSERT) |
| Trust Accounts | LOW (date gap-fill) | **RISKY** (ambiguous prior-amount) |
| Survival Analysis | NONE (PG only) | SAFE (idempotent) |
| Market Data | NONE (strap dedup) | SAFE |

---

## 6. Recommended Fixes

### Priority 1 — Add archived_at Filters (4 services)

These are the highest-value fixes: prevent processing of foreclosures that are no longer active.

#### 1a. Auction Scrape — `pg_auction_service.py`
```python
# _dates_with_auctions() — add filter
SELECT DISTINCT auction_date::date FROM foreclosures
WHERE auction_date BETWEEN :start AND :end
  AND archived_at IS NULL  -- ADD THIS
```

#### 1b. Single-Pin Permits — `pg_permit_single_pin_service.py`
```python
# Candidate selection query — add filter or change default
WHERE f.archived_at IS NULL  -- ADD THIS
# OR: change active_only default from False to True in controller call
```

#### 1c. Trust Accounts — `pg_trust_accounts.py`
```python
# When matching trust balances to foreclosures — add filter
WHERE f.archived_at IS NULL  -- ADD THIS
```

#### 1d. Title Chain — Controller call in `pg_pipeline_controller.py`
```python
# Change default in _run_title_chain()
svc.run(active_only=True)  # was active_only=False
```

### Priority 2 — Rescheduled Auction Data Reuse

When a new foreclosure row is created for a rescheduled case (same case_number, new auction_date), copy enrichment data from the old row instead of re-processing from scratch.

#### 2a. Judgment Data Migration
When creating a new foreclosure row for a known case_number:
- Copy `judgment_data` JSON from the most recent existing row with the same case_number
- Set `step_judgment_extracted` to preserve the flag
- The JSON cache on disk (`_extracted.json`) already shares the path, so Vision API won't be called regardless — but the DB column needs to be populated

#### 2b. Step Flag Awareness for ORI
ORI search writes to `ori_encumbrances` by strap (shared). When a new foreclosure row is created for the same strap:
- Check if encumbrances already exist for that strap
- If so, set `step_ori_searched = now()` on the new row to skip redundant PAV API calls
- Survival analysis should similarly check existing survival_status values

**Suggested implementation:** Add a trigger or post-UPSERT hook in the auction scrape service:
```sql
-- After inserting new foreclosure row, copy enrichment from prior row
UPDATE foreclosures new_f SET
    judgment_data = old_f.judgment_data,
    step_judgment_extracted = old_f.step_judgment_extracted,
    step_ori_searched = CASE
        WHEN EXISTS (SELECT 1 FROM ori_encumbrances WHERE strap = new_f.strap)
        THEN now() ELSE NULL END,
    step_survival_analyzed = CASE
        WHEN NOT EXISTS (SELECT 1 FROM ori_encumbrances WHERE strap = new_f.strap AND survival_status IS NULL)
        THEN now() ELSE NULL END
FROM foreclosures old_f
WHERE new_f.case_number_raw = old_f.case_number_raw
  AND new_f.foreclosure_id != old_f.foreclosure_id
  AND old_f.judgment_data IS NOT NULL
  AND new_f.judgment_data IS NULL;
```

### Priority 3 — Single-Pin Dedup by Strap

Single-pin permits scrape the same property APIs for each active foreclosure_id, even if multiple rows share the same strap. Fix by deduplicating candidates:

```sql
-- Instead of selecting per foreclosure_id, select per strap
SELECT DISTINCT ON (f.strap) f.strap, f.folio, f.property_address, ...
FROM foreclosures f
WHERE f.archived_at IS NULL
  AND f.strap IS NOT NULL
  AND <permit gap condition>
ORDER BY f.strap, f.auction_date DESC
```

### Priority 4 — Judgment Extract Case Map Fix

The `_find_unextracted_pdfs()` filesystem scan should prefer the **newest active** foreclosure_id when multiple rows share a case_number:

```python
# When building case_map, prefer newest active row
SELECT case_number_raw, foreclosure_id FROM foreclosures
WHERE archived_at IS NULL
ORDER BY auction_date DESC  -- newest first
```

This ensures that during the overlap window (both old and new rows active), extracted judgment data is loaded to the new (rescheduled) foreclosure.

---

## Not Recommended to Fix

### Phase A Bulk Services — No archived_at Filter Needed
Phase A services (HCPA, Clerk, NAL, FLR, Sunbiz, County Permits, Tampa Permits) are **reference data loaders**. They pull county/state data for ALL properties, not foreclosure-specific data. Adding `archived_at` filters would break their purpose. These tables are used by multiple consumers beyond active foreclosures.

### Foreclosure Refresh — Full Recompute is By Design
Steps 1-4 (normalize, cross-fill, equity) are lightweight PG operations on the full active set. The cost of tracking per-row dirtiness would exceed the compute saved.
