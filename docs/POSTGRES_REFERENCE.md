# PostgreSQL Reference Database — `hills_sunbiz`

This document describes the PostgreSQL reference database used for property resolution, fuzzy name matching, and sales history lookups. This database supplements the primary SQLite pipeline database with bulk county data that is too large or query-intensive for SQLite.

## Connection

```
Host:     localhost:5432
Database: hills_sunbiz
User:     hills
Password: hills_dev
DSN:      postgresql+psycopg://hills:hills_dev@localhost:5432/hills_sunbiz
Size:     ~2.9 GB
```

**Python connection** (via `sunbiz/db.py`):
```python
from sunbiz.db import get_engine
engine = get_engine()  # SQLAlchemy engine
```

**CLI**:
```bash
PGPASSWORD=hills_dev psql -h localhost -U hills -d hills_sunbiz
```

## Extensions

| Extension | Version | Purpose |
|-----------|---------|---------|
| `pg_trgm` | 1.6 | Trigram-based fuzzy string matching (`similarity()`, `%` operator) |
| `fuzzystrmatch` | 1.2 | Phonetic matching (`soundex()`, `dmetaphone()`, `levenshtein()`) |

---

## Tables

### `hcpa_allsales` — 2,416,808 rows

Every recorded property sale in Hillsborough County (1901–2026). This is the primary table for resolving defendant names to property folios via sales history.

| Column | Type | Description |
|--------|------|-------------|
| `id` | bigint PK | Auto-increment |
| `pin` | varchar | Parcel identification number |
| `folio` | varchar | 10-digit folio number (joins to `hcpa_bulk_parcels.folio`) |
| `sale_date` | date | Date of sale |
| `sale_type` | varchar | Deed type: `WD` (warranty), `QC` (quit claim), `FD` (foreclosure), `TD` (tax deed), `CT`, `DD`, `TR`, `PR`, etc. |
| `sale_amount` | numeric | Sale price |
| `qualification_code` | varchar | `Q` = arm's-length, `U` = unqualified |
| `grantor` | text | Seller name(s) |
| `grantee` | text | Buyer name(s) — **search this to find defendant's property** |
| `or_book` / `or_page` | varchar | Official Records book/page (cross-ref to ORI) |
| `doc_num` | varchar | Document number |
| `sub_code` | varchar | Subdivision code (joins to `hcpa_parcel_sub_names`) |
| `dor_code` | varchar | Dept of Revenue land use code |
| `grantee_dmetaphone` | text | Pre-computed double metaphone of first word in grantee |
| `grantor_dmetaphone` | text | Pre-computed double metaphone of first word in grantor |

**Key sale types**: `WD` = 1.7M, `QC` = 334K, `CT` = 100K, `FD` (foreclosure deed) = 29K, `TD` (tax deed) = 7K.

#### Indexes

| Index | Type | Column(s) | Purpose |
|-------|------|-----------|---------|
| `hcpa_allsales_pkey` | btree | `id` | Primary key |
| `idx_hcpa_allsales_folio` | btree | `folio` | Lookup all sales for a property |
| `idx_hcpa_allsales_sale_date` | btree | `sale_date` | Date range queries |
| `idx_hcpa_allsales_doc_num` | btree | `doc_num` | Cross-reference to ORI documents |
| `idx_hcpa_allsales_pin` | btree | `pin` | Parcel ID lookup |
| `idx_allsales_saletype_date` | btree | `(sale_type, sale_date DESC)` | Filter by deed type + date |
| **`idx_allsales_grantee_trgm`** | **GIN** | `grantee gin_trgm_ops` | **Fuzzy name search on buyer** |
| **`idx_allsales_grantor_trgm`** | **GIN** | `grantor gin_trgm_ops` | **Fuzzy name search on seller** |
| `idx_allsales_grantee_dmetaphone` | btree | `grantee_dmetaphone` | Phonetic surname match on buyer |
| `idx_allsales_grantor_dmetaphone` | btree | `grantor_dmetaphone` | Phonetic surname match on seller |

---

### `hcpa_bulk_parcels` — 530,324 rows

Every parcel in Hillsborough County with current owner, address, specs, valuations, and legal description. Superset of the pipeline's SQLite `bulk_parcels` table.

| Column | Type | Description |
|--------|------|-------------|
| `folio` | varchar PK | 10-digit folio number |
| `pin` | varchar | Parcel identification number |
| `strap` | varchar | HCPA strap format (e.g., `203216D5N000000000090U`) — **matches pipeline's `auctions.parcel_id`** |
| `owner_name` | text | Current owner |
| `property_address` | text | Street address |
| `city` / `zip_code` | varchar | Location |
| `land_use` / `land_use_desc` | varchar/text | DOR use code + description (e.g., `SINGLE FAMILY`, `MULTI-FAMILY`) |
| `year_built` | integer | Construction year |
| `beds` / `baths` / `stories` | numeric | Property specs |
| `units` | integer | **Number of units — use for multi-unit detection** |
| `buildings` | integer | Number of buildings |
| `heated_area` / `lot_size` | numeric | Square footage |
| `assessed_value` / `market_value` / `just_value` | numeric | Tax valuations |
| `land_value` / `building_value` / `extra_features_value` | numeric | Value breakdown |
| `taxable_value` | numeric | Taxable value |
| `last_sale_date` | date | Most recent sale |
| `last_sale_price` | numeric | Most recent sale price |
| `raw_legal1` through `raw_legal4` | text | **Full legal description (4 lines)** |
| `latitude` / `longitude` | double precision | Currently NULL — use `hcpa_latlon` join |
| `owner_dmetaphone` | text | Pre-computed double metaphone of first word in owner_name |
| `owner_soundex` | varchar(4) | Pre-computed soundex of first word in owner_name |

**Important**: The `strap` column matches the pipeline's `auctions.parcel_id` format. The `folio` column is a different 10-digit format. Always join pipeline data on `strap`, not `folio`.

#### Indexes

| Index | Type | Column(s) | Purpose |
|-------|------|-----------|---------|
| `hcpa_bulk_parcels_pkey` | btree | `folio` | Primary key |
| `idx_hcpa_bulk_parcels_strap` | btree | `strap` | **Join to pipeline parcel_id** |
| `idx_hcpa_bulk_parcels_owner` | btree | `owner_name` | Exact owner lookup |
| `idx_hcpa_bulk_parcels_address` | btree | `property_address` | Address lookup |
| **`idx_bulk_parcels_owner_trgm`** | **GIN** | `owner_name gin_trgm_ops` | **Fuzzy owner name search** |
| `idx_bulk_parcels_owner_dmetaphone` | btree | `owner_dmetaphone` | Phonetic surname match |
| `idx_bulk_parcels_owner_soundex` | btree | `owner_soundex` | Soundex phonetic match |

---

### `hcpa_latlon` — 44,000 rows

Parcel lat/lon coordinates (8.3% coverage). Join on `folio`.

### `hcpa_parcel_sub_names` — 11,491 rows

Subdivision code → name + plat book/page. Useful for matching HOA names to subdivisions.

| Column | Type | Description |
|--------|------|-------------|
| `sub_code` | varchar PK | Subdivision code |
| `sub_name` | text | Subdivision name (e.g., `HUNTER'S GREEN PARCEL 18B PHASE 2B`) |
| `plat_bk` / `page` | varchar | Plat book and page references |

### `hcpa_subdivisions` — 11,280 rows

Subdivision geometry/area records with legal descriptions and plat references.

### Special District Tables (reference data)

| Table | Rows | Description |
|-------|------|-------------|
| `hcpa_special_district_cdds` | 178 | Community Development Districts |
| `hcpa_special_district_lds` | 69 | Lighting Districts |
| `hcpa_special_district_sd` | 55 | Special Districts |
| `hcpa_special_district_sd2` | 7 | Special Districts variant 2 |
| `hcpa_special_district_tifs` | 15 | Tax Increment Financing districts |

### Sunbiz Tables

| Table | Rows | Description |
|-------|------|-------------|
| `sunbiz_raw_records` | 23,183 | Raw fixed-width lines from Sunbiz SFTP (single daily file, Jan 2022) |
| `sunbiz_flr_filings` | 21,267 | Structured UCC financing statements |
| `sunbiz_flr_parties` | 0 | Debtor/secured party details (schema only — not yet loaded) |
| `sunbiz_flr_events` | 0 | Amendment/continuation events (schema only — not yet loaded) |

### `ingest_files` — 15 rows

Tracks loaded source files with SHA-256 checksums and status.

---

## Fuzzy Search Functions

### `resolve_property_by_name()`

Combines all three matching strategies into a single function call.

```sql
SELECT * FROM resolve_property_by_name(
    'ELIZABETH BROWER',     -- defendant name
    'HUNTER'                -- optional: HOA/subdivision hint (filters on raw_legal1)
);
```

**Parameters:**
- `defendant_name TEXT` — name to search for
- `plaintiff_or_hoa TEXT DEFAULT NULL` — optional subdivision/HOA filter (ILIKE against `raw_legal1` and `property_address`)
- `similarity_threshold REAL DEFAULT 0.3` — trigram threshold (lower = more results, more noise)

**Returns** (ranked by `match_score` DESC):

| Column | Description |
|--------|-------------|
| `folio` | 10-digit folio |
| `strap` | HCPA strap (matches pipeline `parcel_id`) |
| `property_address` | Street address |
| `city` | City |
| `owner_name` | Matched name (current owner or historical grantee) |
| `legal_description` | `raw_legal1` from bulk_parcels |
| `match_method` | `owner_trigram`, `owner_metaphone`, or `sales_trigram` |
| `match_score` | 0.0–1.0 (higher = better match) |

**Strategies executed (in parallel):**
1. **owner_trigram**: Fuzzy match against current property owners (`hcpa_bulk_parcels.owner_name`)
2. **owner_metaphone**: Phonetic match on surname against current owners
3. **sales_trigram**: Fuzzy match against all historical buyers (`hcpa_allsales.grantee`)

### Direct Fuzzy Queries

**Trigram similarity** (requires GIN index):
```sql
SET pg_trgm.similarity_threshold = 0.3;
SELECT *, similarity(owner_name, 'JOHN SMITH') as score
FROM hcpa_bulk_parcels
WHERE owner_name % 'JOHN SMITH'
ORDER BY score DESC;
```

**Double metaphone** (phonetic — catches spelling variants):
```sql
SELECT * FROM hcpa_bulk_parcels
WHERE owner_dmetaphone = dmetaphone('SMITH')
  AND owner_name ILIKE '%JOHN%';
```

**Soundex** (simpler phonetic):
```sql
SELECT * FROM hcpa_bulk_parcels
WHERE owner_soundex = soundex('SMITH');
```

**Levenshtein distance** (edit distance):
```sql
SELECT *, levenshtein(upper(owner_name), 'JOHN SMITH') as edit_dist
FROM hcpa_bulk_parcels
WHERE owner_name ILIKE '%SMITH%'
ORDER BY edit_dist
LIMIT 10;
```

---

## Common Pipeline Queries

### Resolve invalid-folio defendant to property
```sql
-- Given: defendant name + HOA/plaintiff from auction listing
SELECT * FROM resolve_property_by_name('DEFENDANT NAME', 'HOA OR SUBDIVISION HINT')
LIMIT 5;
```

### Check if property is multi-unit
```sql
SELECT folio, strap, property_address, units, buildings, land_use_desc
FROM hcpa_bulk_parcels
WHERE strap = '<pipeline_parcel_id>'
  AND units > 1;
```

### Build deed chain from sales history alone
```sql
SELECT sale_date, sale_type, sale_amount, grantor, grantee, or_book, or_page
FROM hcpa_allsales
WHERE folio = '<10_digit_folio>'
ORDER BY sale_date;
```

### Find all foreclosure deeds for a time period
```sql
SELECT a.folio, a.sale_date, a.grantor, a.grantee,
       bp.property_address, bp.just_value
FROM hcpa_allsales a
JOIN hcpa_bulk_parcels bp USING (folio)
WHERE a.sale_type = 'FD'
  AND a.sale_date >= '2025-01-01'
ORDER BY a.sale_date DESC;
```

### Cross-reference pipeline strap to PG folio
```sql
SELECT folio, strap, property_address, owner_name, raw_legal1
FROM hcpa_bulk_parcels
WHERE strap = '<pipeline_parcel_id>';
```

### Find subdivision from HOA name
```sql
SELECT sub_code, sub_name, plat_bk, page
FROM hcpa_parcel_sub_names
WHERE sub_name ILIKE '%KEYWORD%';
```

---

## Data Loader

The PostgreSQL database is loaded via `sunbiz/pg_loader.py`. Source data comes from HCPA bulk downloads and Sunbiz SFTP files. The `ingest_files` table tracks what has been loaded with SHA-256 checksums to prevent duplicate loads.

```bash
# Load/refresh data (idempotent — skips already-loaded files)
uv run python -m sunbiz.pg_loader
```

## Key Identifiers — Pipeline ↔ PostgreSQL

| Pipeline (SQLite) | PostgreSQL | Join Column |
|-------------------|-----------|-------------|
| `auctions.parcel_id` | `hcpa_bulk_parcels.strap` | strap format (e.g., `203216D5N000000000090U`) |
| `parcels.folio` | `hcpa_bulk_parcels.strap` | same strap format |
| — | `hcpa_bulk_parcels.folio` | 10-digit folio (e.g., `0594010642`) |
| — | `hcpa_allsales.folio` | 10-digit folio |
| `documents.instrument` | `hcpa_allsales.doc_num` | ORI document number |
| `documents.book` / `page` | `hcpa_allsales.or_book` / `or_page` | Official Records book/page |
