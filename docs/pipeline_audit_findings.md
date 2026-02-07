# Pipeline Documentation & Logic Audit
**Date:** 2025-12-31 | **Updated:** 2026-01-03  
**Status:** RESOLVED (Items 1-2), ACTION REQUIRED (Items 3-7)

This document summarizes conflicts, logic flaws, and redundancies identified during a review of the `docs/steps/*` documentation and `src/orchestrator.py`.

---

## ✅ Resolved Issues

### 1. Dual Scraping of Realtor.com ~~(CRITICAL)~~
**Status:** ✅ FIXED (2026-01-01)

**Problem:** Pipeline ran two scrapers for Realtor.com in parallel (HomeHarvest + MarketScraper).

**Fix Applied:** Modified `src/scrapers/market_scraper.py` to remove Realtor fallback. MarketScraper now hits **Zillow only**; HomeHarvest handles Realtor exclusively.

### 2. ORI Ingestion Versioning ~~(HIGH)~~
**Status:** ✅ FIXED (2026-01-01)

**Problem:** Conflicting V1/V2 documentation for ORI ingestion.

**Fix Applied:**
- Moved `05_ori_ingestion.md` → `docs/archive/05_ori_ingestion_v1_DEPRECATED.md`
- Renamed `04_ori_ingestion_v2.md` → `docs/steps/05_ori_ingestion_v2.md`

---

## 🔧 Action Required

### 3. HCPA Vision Scraper (Dead Code)
**Severity:** LOW | **Decision:** MOVE TO SCRIPTS

**Current State:** `docs/steps/12_hcpa_fallback.md` documents a Vision-based HCPA scraper that is not wired into the pipeline as a fallback.

**Action Plan:**
1. Move `src/scrapers/hcpa_scraper.py` → `scripts/hcpa_vision_scraper.py`
2. Update `12_hcpa_fallback.md` to indicate it's a standalone utility, not a pipeline step
3. Remove Step 12 from pipeline documentation

### 4. Geocoding: Fail Fast if Missing
**Severity:** MEDIUM | **Decision:** FAIL PIPELINE

**Current State:** Flood Zone check may fail if Bulk doesn't provide coordinates, even though HomeHarvest might later.

**New Requirement:** Bulk should **always** produce geocodes. If a property doesn't have coordinates after Bulk enrichment, the pipeline should **halt** (fail fast) rather than silently proceeding.

**Action Plan:**
1. Add validation in `orchestrator.py` after Bulk enrichment: if `lat/lon` is null, skip enrichment for that property with a clear error
2. Remove the race condition concern - coordinates are expected from Bulk, not optional

### 5. Tax Deed Scraper (Not Implemented)
**Severity:** LOW | **Decision:** MOVE TO SCRIPTS

**Current State:** Tax Deed scraping (`docs/steps/01_5_tax_deed_auctions.md`) was never fully implemented.

**Action Plan:**
1. Move `src/scrapers/tax_deed_scraper.py` → `scripts/tax_deed_scraper.py`
2. Archive documentation: `01_5_tax_deed_auctions.md` → `docs/archive/`
3. Remove Step 1.5 references from pipeline overview

### 6. Step Numbering Alignment
**Severity:** MEDIUM | **Decision:** RENUMBER TO WHOLE NUMBERS

**Current State:** Documentation uses fractional steps (1.5, 3.5, 4v2) that don't match code phases.

**Action Plan:**
1. Audit `orchestrator.py` to extract actual execution order
2. Renumber documentation to use **whole integers** (1, 2, 3, ...)
3. Align step numbers with Orchestrator phases:
   - **Steps 1-2:** Auction Scraping & PDF Extraction
   - **Steps 3-8:** Phase 1 Parallel Enrichment  
   - **Step 9:** ORI Ingestion (Phase 2)
   - **Steps 10-11:** Analysis (Phase 3)

### 7. Semaphore Documentation Mismatch
**Severity:** LOW | **Verified:** REAL BUT MINOR

**Findings:**

| Semaphore | Code | Docs | Issue |
|-----------|------|------|-------|
| `realtor_semaphore` | ❌ Doesn't exist | `= 2` | Docs reference non-existent semaphore |
| `homeharvest_semaphore` | `= 1` | ❌ Missing | Not documented |
| `v2_db_semaphore` | `= 1` | ❌ Missing | Not documented |

**Action Plan:**
1. Remove `realtor_semaphore` from `00_pipeline_overview.md`
2. Add `homeharvest_semaphore = 1` and `v2_db_semaphore = 1` to docs

---

## 📋 Summary of Work

| # | Issue | Status | Action |
|---|-------|--------|--------|
| 1 | Dual Realtor Scraping | ✅ Fixed | N/A |
| 2 | ORI Versioning | ✅ Fixed | N/A |
| 3 | HCPA Vision Fallback | 🔧 Pending | Move to `scripts/` |
| 4 | Geocoding Race | 🔧 Pending | Fail fast if no coords |
| 5 | Tax Deed Scraper | 🔧 Pending | Move to `scripts/` |
| 6 | Step Numbering | 🔧 Pending | Renumber to whole ints |
| 7 | Semaphore Docs | 🔧 Pending | Update `00_pipeline_overview.md` |
