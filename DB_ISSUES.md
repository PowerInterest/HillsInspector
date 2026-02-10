# DB Issues Report (Refreshed)

**Scope:** Second-pass SQL audit and schema/logic cross-checks against the active SQLite DB.  
**DB Path:** `/home/user/hills_data/property_master_sqlite.db` (from `.env` `HILLS_SQLITE_DB`)  
**Date (local):** 2026-02-10  
**Status:** Refreshed after partial fixes; counts below reflect the snapshot at refresh time.

## Summary
Resolved since last report: bulk parcels are loaded; parcels schema now includes `raw_legal2/3/4/strap`; `property_sources` schema migrated; SQLite DDL issue for `judgment_legal_description` fixed; sales history duplicate groups removed; `has_valid_parcel_id` inconsistency resolved; `mrta_status` populated in `chain_of_title`.

Remaining gaps: 60 auctions still missing `parcel_id`; 1 case missing `judgment_legal_description` (thin CC judgment); `encumbrances.survival_status` still NULL for all rows; `sales_history.sale_date` still non‑ISO; status flags still inconsistent; market data / scraper outputs / property sources tables remain empty.

## Table Counts (Current)
```
auctions: 186
parcels: 120
bulk_parcels: 530124
documents: 429
sales_history: 60
chain_of_title: 77
encumbrances: 88
liens: 0
permits: 43
home_harvest: 58
market_data: 0
scraper_outputs: 0
property_sources: 0
status: 186
```

## Resolved / No Longer Issues
1. `parcels` missing `raw_legal2/3/4/strap`
   - Resolved: columns now exist in DB schema.
2. `property_sources` schema mismatch
   - Resolved: migration maps `property_id → folio`, `source_type → source_name`, `source_url → url`, adds `description`.
3. SQLite‑incompatible `ALTER TABLE ... IF NOT EXISTS` in judgment backfill
   - Resolved: DDL moved to migrations; backfill is now a guarded UPDATE.
4. `bulk_parcels` empty
   - Resolved: 530,124 rows present.
5. `sales_history` duplicate groups
   - Resolved: no duplicate groups remain by `(folio, book, page)`.
6. `has_valid_parcel_id` inconsistency
   - Resolved: 0 cases where `has_valid_parcel_id=1` but `parcel_id` is missing.
7. `chain_of_title.mrta_status` missing
   - Resolved: 0 missing.
8. “Documents duplicates by book/page”
   - Not a true duplicate issue: 22 groups are NULL `book/page` with distinct instruments. Unique indexes cover `(folio, instrument_number)` and `(ori_uuid)`; no rows lack both keys.

## Remaining Issues / Gaps
1. Missing parcel linkage
   - 60 auctions still have `parcel_id` NULL/empty.
   - Impact: downstream Steps 3–12 skip; Step 2.5 should address most.
2. Judgment legal description missing (thin CC judgment)
   - 1 case remains: `292024CC016095A001HC` (parcel `19312176U000000000090U`).
   - The Final Judgment PDF contains no property identifiers; recovery must use related ORI docs (e.g., liens, LP).
3. Encumbrances survival status missing
   - 88/88 encumbrances have `survival_status` NULL.
   - Impact: core deliverable incomplete even when encumbrances exist.
4. Sales history dates not normalized
   - 60/60 rows are non‑ISO (e.g., `08/1995`), so `ORDER BY sale_date` is lexicographically wrong.
5. Status flag mismatches
   - `step_pdf_downloaded` set but `needs_judgment_extraction=1`: 72 cases.
   - `step_judgment_extracted` set but `needs_judgment_extraction=1`: 18 cases.
   - `step_bulk_enriched` NULL but `needs_hcpa_enrichment=0`: 63 cases.
6. Empty enrichment tables (data‑coverage gap)
   - `market_data`: 0 rows.
   - `scraper_outputs`: 0 rows (inbox pattern; UI shows empty).
   - `property_sources`: 0 rows (no writer populates yet).

## Residual Risk (Design / Execution)
1. Direct `sqlite3.connect()` usage can bypass migrations if a script runs before `PropertyDB` is constructed.
   - Mitigated during normal `--update` runs (PropertyDB initialized early).
   - Still a risk for standalone scripts or ad‑hoc usage.

## Supporting Queries (Current Snapshot)
```
-- Table counts
SELECT COUNT(*) FROM auctions;
SELECT COUNT(*) FROM parcels;
SELECT COUNT(*) FROM bulk_parcels;
SELECT COUNT(*) FROM documents;
SELECT COUNT(*) FROM sales_history;
SELECT COUNT(*) FROM chain_of_title;
SELECT COUNT(*) FROM encumbrances;
SELECT COUNT(*) FROM market_data;
SELECT COUNT(*) FROM scraper_outputs;
SELECT COUNT(*) FROM property_sources;

-- Auctions missing parcels
SELECT COUNT(*) AS missing_parcel_id
FROM auctions
WHERE parcel_id IS NULL OR parcel_id = '';

-- Inconsistent parcel_id flags
SELECT
  SUM(CASE WHEN parcel_id IS NULL OR parcel_id = '' THEN 1 ELSE 0 END) AS missing_parcel_id,
  SUM(CASE WHEN has_valid_parcel_id = 1 AND (parcel_id IS NULL OR parcel_id = '') THEN 1 ELSE 0 END) AS inconsistent_flag
FROM auctions;

-- Judgment legal missing
SELECT a.case_number, a.parcel_id
FROM auctions a
JOIN parcels p ON p.folio = a.parcel_id
WHERE a.extracted_judgment_data IS NOT NULL
  AND (p.judgment_legal_description IS NULL OR p.judgment_legal_description = '');

-- Missing file paths
SELECT COUNT(*) AS total,
       SUM(CASE WHEN file_path IS NULL OR file_path = '' THEN 1 ELSE 0 END) AS missing_file_path
FROM documents;

-- Survival and MRTA status missing
SELECT COUNT(*) AS total,
       SUM(CASE WHEN survival_status IS NULL OR survival_status = '' THEN 1 ELSE 0 END) AS missing_status
FROM encumbrances;

SELECT COUNT(*) AS total,
       SUM(CASE WHEN mrta_status IS NULL OR mrta_status = '' THEN 1 ELSE 0 END) AS missing_mrta
FROM chain_of_title;

-- Flag mismatches
SELECT COUNT(*)
FROM status s
JOIN auctions a ON a.case_number = s.case_number
WHERE s.step_pdf_downloaded IS NOT NULL AND a.needs_judgment_extraction = 1;

SELECT COUNT(*)
FROM status s
JOIN auctions a ON a.case_number = s.case_number
WHERE s.step_judgment_extracted IS NOT NULL AND a.needs_judgment_extraction = 1;

SELECT COUNT(*)
FROM status s
JOIN auctions a ON a.case_number = s.case_number
WHERE s.step_bulk_enriched IS NULL AND a.needs_hcpa_enrichment = 0;

-- Duplicate patterns (for diagnostics only)
SELECT folio, book, page, COUNT(*) AS cnt
FROM documents
GROUP BY folio, book, page
HAVING cnt > 1
ORDER BY cnt DESC;

SELECT folio, book, page, COUNT(*) AS cnt
FROM sales_history
GROUP BY folio, book, page
HAVING cnt > 1
ORDER BY cnt DESC;

-- Non‑ISO sale_date check
SELECT COUNT(*) AS non_iso_sale_dates
FROM sales_history
WHERE sale_date IS NOT NULL
  AND sale_date != ''
  AND sale_date NOT GLOB '____-__-__*';
```
