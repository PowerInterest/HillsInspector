# Consolidation Document

This document is a consolidated reference composed of the following historical files:
- `PG_Schema.md`
- `POSTGRES_REFERENCE.md`
- `schema.md`
- `pg_tables_columns.md`

---



## Source: PG_Schema.md

# PostgreSQL Schema Reference — `hills_sunbiz`

**Connection**: `localhost:5432`, user `hills`, database `hills_sunbiz`
**Extensions**: `pg_trgm` 1.6 (trigram similarity), `fuzzystrmatch` 1.2 (soundex/metaphone/dmetaphone), `plpgsql`
**Last updated**: 2026-02-18

---

## Table Summary

| Table | Rows | Purpose |
|-------|-----:|---------|
| `hcpa_allsales` | 2,422,679 | Every property sale in Hillsborough County (1901–present) |
| `clerk_civil_events` | 783,288 | Court docket events per civil case |
| `hcpa_bulk_parcels` | 528,243 | Current parcel data (owner, specs, valuations) |
| `dor_nal_parcels` | 524,226 | FL Dept of Revenue NAL — tax exemptions, millage, just value |
| `clerk_civil_parties` | 271,542 | Plaintiffs, defendants, attorneys per case |
| `sunbiz_flr_events` | 96,091 | UCC financing statement lifecycle events |
| `clerk_civil_cases` | 72,882 | Civil case header (2025–2026 only) |
| `sunbiz_flr_parties` | 44,298 | Debtors and secured parties on UCC filings |
| `hcpa_latlon` | 44,000 | HCPA parcel centroids (lat/lon) |
| `sunbiz_raw_records` | 23,183 | Raw Sunbiz UCC fixed-width source lines |
| `sunbiz_flr_filings` | 21,267 | UCC financing statement headers |
| `hcpa_parcel_sub_names` | 11,491 | Subdivision code → name + plat book/page |
| `hcpa_subdivisions` | 11,280 | Subdivision polygons (GIS shape area/perimeter) |
| `TrustAccount` | 2,850 | Clerk trust/escrow account movements per case |
| `historical_auctions` | 1,242 | Foreclosure auction history (2014–present) |
| `hcpa_parcel_dor_names` | 305 | DOR land-use code → description lookup |
| `hcpa_special_district_cdds` | 178 | Community Development Districts |
| `ingest_files` | 94 | Source file manifest (ETL tracking) |
| `hcpa_special_district_lds` | 69 | Land Development Special Districts |
| `hcpa_special_district_sd` | 55 | Special Districts (type 1) |
| `TrustAccountSummary` | 15 | Daily rollup of trust account totals |
| `hcpa_special_district_tifs` | 15 | Tax Increment Finance districts |
| `property_market` | 10 | Consolidated best-of-sources market snapshot (per pipeline run) |
| `hcpa_special_district_sd2` | 7 | Special Districts (type 2) |
| `ori_encumbrances` | 0 | PG mirror of ORI encumbrances (not yet populated) |
| `ori_encumbrance_assignments` | 0 | Assignment chain for ORI encumbrances |
| `ori_encumbrance_satisfactions` | 0 | Satisfaction linkages for ORI encumbrances |
| `clerk_disposed_cases` | 0 | Disposed civil cases (not yet loaded) |

---

## Detailed Table Reference

---

### `hcpa_allsales`
**2,422,679 rows** — Every arm's-length and non-arm's-length property transfer recorded by HCPA since 1901.
Sales range from $1 to $938M. Average arm's-length sale ≈ $813K. Data through 2026-02-10.

**Key join**: `folio` (10-digit) → `hcpa_bulk_parcels.folio`, `historical_auctions.folio`

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | bigint | NOT NULL | PK, auto-increment |
| `pin` | varchar(64) | YES | Alternate parcel pin |
| `folio` | varchar(32) | YES | **10-digit folio** — primary property key |
| `dor_code` | varchar(16) | YES | Land use code (joins `hcpa_parcel_dor_names`) |
| `nbhc` | varchar(32) | YES | Neighborhood code |
| `sale_date` | date | YES | Date of transfer |
| `vacant_improved` | varchar(16) | YES | `V` = vacant, `I` = improved |
| `qualification_code` | varchar(16) | YES | HCPA arm's-length qualification |
| `reason_code` | varchar(16) | YES | Reason for disqualification (if any) |
| `sale_amount` | numeric(18,2) | YES | Dollar value of transfer |
| `sub_code` | varchar(16) | YES | Subdivision code (joins `hcpa_parcel_sub_names`) |
| `street_code` | varchar(32) | YES | Street identifier |
| `sale_type` | varchar(16) | YES | **Deed type** — see sale types table below |
| `or_book` | varchar(32) | YES | Official Records book |
| `or_page` | varchar(32) | YES | Official Records page |
| `grantor` | text | YES | **Seller name** (auction winner for WD/QC after foreclosure) |
| `grantee` | text | YES | **Buyer name** (auction winner for CT deeds) |
| `doc_num` | varchar(32) | YES | Document number (links to ORI) |
| `source_file_id` | bigint | NOT NULL | FK → `ingest_files.id` |
| `source_line_number` | integer | NOT NULL | Source file line |
| `loaded_at` | timestamptz | NOT NULL | ETL load timestamp |
| `grantee_dmetaphone` | text | YES | Double Metaphone of grantee (fuzzy search) |
| `grantor_dmetaphone` | text | YES | Double Metaphone of grantor (fuzzy search) |

**Sale Type Codes** (volume in dataset):

| Code | Count | Meaning | Who is the auction buyer? |
|------|------:|---------|--------------------------|
| `WD` | 1,736,468 | Warranty Deed | **`grantor`** (they bought at auction, now selling) |
| `QC` | 334,125 | Quit Claim Deed | **`grantor`** (same logic) |
| `CT` | 99,863 | Certificate of Title (foreclosure) | **`grantee`** (directly issued to auction winner) |
| ` ` | 81,024 | No type recorded | — |
| `DD` | 38,522 | Deed | **`grantor`** |
| `TR` | 36,907 | Transfer | **`grantor`** |
| `FD` | 29,434 | Fee / Final Deed | **`grantor`** |
| `CD` | 24,516 | Certificate of Deed | **`grantor`** |
| `AD` | 9,499 | Administrator's Deed | — |
| `PR` | 8,560 | Personal Representative Deed | — |
| `AG` | 8,483 | Agreement for Deed | — |
| `TD` | 7,160 | Tax Deed | — |

**Indexes**: `folio`, `sale_date`, `sale_type+sale_date`, `doc_num`, `pin`; GIN trigram on `grantor`/`grantee`; dmetaphone btree on both

---

### `hcpa_bulk_parcels`
**528,243 rows** — Current HCPA parcel snapshot (all Hillsborough County parcels). Just values range $4–$944M, avg $496K. 11,700 multi-unit parcels.

**Key join**: `strap` ↔ pipeline `parcel_id` ↔ `property_market.strap`; `folio` ↔ `hcpa_allsales.folio`

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `folio` | varchar(32) | NOT NULL | **PK** — 10-digit parcel ID |
| `pin` | varchar(64) | YES | Alternate PIN |
| `strap` | varchar(64) | YES | **HCPA strap** = pipeline `parcel_id` |
| `owner_name` | text | YES | Current owner of record |
| `property_address` | text | YES | Situs address |
| `city` | varchar(128) | YES | City |
| `zip_code` | varchar(16) | YES | ZIP |
| `land_use` | varchar(64) | YES | DOR land use code |
| `land_use_desc` | text | YES | Human-readable land use description |
| `year_built` | integer | YES | Year of construction |
| `beds` | numeric(12,3) | YES | Bedrooms |
| `baths` | numeric(12,3) | YES | Bathrooms |
| `stories` | numeric(12,3) | YES | Stories |
| `units` | integer | YES | Dwelling units (>1 = multi-family) |
| `buildings` | integer | YES | Number of structures on parcel |
| `heated_area` | numeric(18,3) | YES | Heated square footage |
| `lot_size` | numeric(18,3) | YES | Lot area (sq ft) |
| `assessed_value` | numeric(18,2) | YES | SOH-capped assessed value |
| `market_value` | numeric(18,2) | YES | Market value |
| `just_value` | numeric(18,2) | YES | Just (appraiser fair market) value |
| `land_value` | numeric(18,2) | YES | Land component value |
| `building_value` | numeric(18,2) | YES | Improvement component value |
| `extra_features_value` | numeric(18,2) | YES | Pool, dock, etc. |
| `taxable_value` | numeric(18,2) | YES | Taxable value after exemptions |
| `last_sale_date` | date | YES | Most recent sale date from HCPA |
| `last_sale_price` | numeric(18,2) | YES | Most recent sale amount |
| `raw_type` | varchar(32) | YES | Raw DOR type code |
| `raw_sub` | varchar(64) | YES | Raw subdivision code |
| `raw_taxdist` | varchar(64) | YES | Tax district code |
| `raw_muni` | varchar(64) | YES | Municipality code |
| `raw_legal1`–`raw_legal4` | text | YES | Legal description lines 1–4 |
| `latitude` | float8 | YES | Parcel centroid latitude |
| `longitude` | float8 | YES | Parcel centroid longitude |
| `source_file_id` | bigint | NOT NULL | FK → `ingest_files.id` |
| `updated_at` | timestamptz | NOT NULL | Last ETL update |
| `owner_dmetaphone` | text | YES | Double Metaphone of owner (fuzzy search) |
| `owner_soundex` | varchar(4) | YES | Soundex of owner (fuzzy search) |

**Indexes**: PK `folio`; btree `strap`, `owner_name`, `property_address`; GIN trigram on `owner_name`; btree dmetaphone + soundex

---

### `dor_nal_parcels`
**524,226 rows** — Florida Department of Revenue NAL (Name/Address List) for tax year 2025. Provides detailed tax exemption breakdown, millage rates, and estimated taxes per parcel.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | bigint | NOT NULL | PK |
| `county_code` | varchar(4) | NOT NULL | FL county FIPS |
| `parcel_id` | varchar(40) | NOT NULL | DOR parcel ID format |
| `folio` | varchar(32) | YES | 10-digit folio |
| `strap` | varchar(64) | YES | HCPA strap |
| `tax_year` | integer | NOT NULL | Currently 2025 |
| `owner_name` | text | YES | Owner of record |
| `owner_address1`/`2` | text | YES | Mailing address |
| `owner_city`/`state`/`zip` | text/varchar | YES | Mailing city/state/ZIP |
| `property_address` | text | YES | Situs address |
| `city` | text | YES | Property city |
| `zip_code` | varchar(16) | YES | Property ZIP |
| `property_use_code` | varchar(8) | YES | DOR use code |
| `just_value` | numeric(18,2) | YES | Just value |
| `just_value_homestead` | numeric(18,2) | YES | Just value under homestead |
| `assessed_value_school` | numeric(18,2) | YES | Assessed for school district |
| `assessed_value_nonschool` | numeric(18,2) | YES | Assessed for non-school |
| `assessed_value_homestead` | numeric(18,2) | YES | Homestead assessed value |
| `taxable_value_school` | numeric(18,2) | YES | School taxable value |
| `taxable_value_nonschool` | numeric(18,2) | YES | Non-school taxable value |
| `homestead_exempt` | boolean | YES | Homestead exemption flag |
| `homestead_exempt_value` | numeric(18,2) | YES | Value of homestead exemption |
| `widow_exempt` | boolean | YES | Widow/widower exemption |
| `widow_exempt_value` | numeric(18,2) | YES | Exemption amount |
| `disability_exempt` | boolean | YES | Disability exemption |
| `disability_exempt_value` | numeric(18,2) | YES | Exemption amount |
| `veteran_exempt` | boolean | YES | Veteran exemption |
| `veteran_exempt_value` | numeric(18,2) | YES | Exemption amount |
| `ag_exempt` | boolean | YES | Agricultural exemption |
| `ag_exempt_value` | numeric(18,2) | YES | Exemption amount |
| `soh_differential` | numeric(18,2) | YES | Save Our Homes cap differential |
| `total_millage` | numeric(12,6) | YES | Combined millage rate |
| `county_millage` | numeric(12,6) | YES | County millage |
| `school_millage` | numeric(12,6) | YES | School millage |
| `city_millage` | numeric(12,6) | YES | Municipal millage |
| `estimated_annual_tax` | numeric(18,2) | YES | Estimated annual tax bill |
| `legal_description` | text | YES | Full legal description |
| `source_file_id` | bigint | NOT NULL | FK → `ingest_files.id` |
| `loaded_at` | timestamptz | NOT NULL | ETL load timestamp |

**Indexes**: PK `id`; unique `(county_code, parcel_id, tax_year)`; btree `folio`, `strap`, `tax_year`, `zip_code`; partial on `homestead_exempt`

---

### `clerk_civil_cases`
**72,882 rows** — Civil case headers from Hillsborough County Clerk. **Coverage: Feb 2025 – Jan 2026 only** (12 monthly files). 2,355 flagged as foreclosure cases.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `case_number` | varchar(32) | NOT NULL | **PK** — e.g. `29-2025-CA-012345` |
| `ucn` | varchar(64) | YES | Unified Case Number (unique) |
| `style` | text | YES | Case style (parties) |
| `case_type` | text | YES | Detailed case type description |
| `division` | varchar(16) | YES | Court division |
| `judge` | text | YES | Assigned judge |
| `cause_of_action` | text | YES | Cause code |
| `cause_description` | text | YES | Cause description |
| `case_status` | text | YES | Open/Closed/etc. |
| `filing_date` | date | YES | Date case filed |
| `judgment_code` | text | YES | Judgment type code |
| `judgment_description` | text | YES | Judgment description |
| `judgment_date` | date | YES | Date of judgment |
| `is_foreclosure` | boolean | YES | Computed: true if case_type contains foreclosure |
| `source_file` | text | YES | Source filename |
| `loaded_at` | timestamptz | NOT NULL | ETL load timestamp |

**Indexes**: PK `case_number`; unique `ucn`; btree `case_type`, `case_status`, `filing_date`, `judgment_date`; partial on `is_foreclosure`

---

### `clerk_civil_events`
**783,288 rows** — Individual docket events per civil case (filings, hearings, judgments, service). Covers same window as `clerk_civil_cases`.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | bigint | NOT NULL | PK |
| `case_number` | varchar(32) | NOT NULL | FK → `clerk_civil_cases.case_number` |
| `event_code` | text | YES | Event type code |
| `event_description` | text | YES | Full event description |
| `event_date` | date | YES | Date of event |
| `party_first_name` | text | YES | Associated party first name |
| `party_middle_name` | text | YES | Associated party middle name |
| `party_last_name` | text | YES | Associated party last name |
| `source_file` | text | YES | Source filename |
| `loaded_at` | timestamptz | NOT NULL | ETL load timestamp |

**Indexes**: PK `id`; btree `case_number`, `event_code`, `event_date`; unique `(case_number, event_code, event_date, party_last_name)`

---

### `clerk_civil_parties`
**271,542 rows** — All parties on civil cases: Plaintiff (94,867), Defendant (103,650), Attorney (72,637), plus minor types. Trigram index enables fuzzy defendant name searches.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | bigint | NOT NULL | PK |
| `case_number` | varchar(32) | NOT NULL | FK → `clerk_civil_cases.case_number` |
| `party_type` | text | YES | Plaintiff / Defendant / Attorney / Decedent / etc. |
| `name` | text | YES | Full name (trigram indexed) |
| `first_name` | text | YES | First name |
| `middle_name` | text | YES | Middle name |
| `last_name` | text | YES | Last name |
| `address1`/`address2` | text | YES | Street address |
| `city`/`state`/`zip` | text | YES | City/state/ZIP |
| `bar_number` | text | YES | Florida bar number (attorneys) |
| `phone` | text | YES | Phone number |
| `email` | text | YES | Email |
| `source_file` | text | YES | Source filename |
| `loaded_at` | timestamptz | NOT NULL | ETL load timestamp |

**Indexes**: PK `id`; btree `case_number`, `party_type`; GIN trigram on `name`; unique `(case_number, party_type, name)`

---

### `clerk_disposed_cases`
**0 rows** — Schema loaded, no data yet. Intended for cases with disposition/closure records.

| Column | Type | Notes |
|--------|------|-------|
| `case_number` | varchar(32) PK | |
| `style` | text | |
| `case_type` | text | |
| `case_subtype` | text | |
| `closure_date` | date | |
| `statistical_closure` | text | |
| `closure_comment` | text | |
| `status_date` | date | |
| `current_status` | text | |
| `source_file` | text | |
| `loaded_at` | timestamptz | |

---

### `historical_auctions`
**1,242 rows** — Foreclosure auction results from HillsForeclosures.com, 2014–present. 760 Third Party buys, 482 Plaintiff take-backs. Source HTML scraped and parsed per property.

**Key trigger**: `trg_resolve_buyer` (BEFORE INSERT OR UPDATE) — auto-resolves placeholder buyer names from `hcpa_allsales` using CT→grantee / WD+QC→grantor logic.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | integer | NOT NULL | PK |
| `listing_id` | varchar | NOT NULL | Unique listing ID from auction site |
| `case_number` | varchar | YES | Court case number (unmasked from HTML) |
| `auction_date` | date | YES | Date of auction |
| `auction_status` | varchar | YES | Sold / Cancelled / etc. |
| `folio` | varchar | YES | 10-digit folio |
| `strap` | varchar | YES | HCPA strap |
| `property_address` | text | YES | Property address |
| `winning_bid` | numeric(18,2) | YES | Winning bid amount |
| `final_judgment_amount` | numeric(18,2) | YES | Total debt amount (Final Judgment) |
| `appraised_value` | numeric(18,2) | YES | HCPA appraised value at auction time |
| `previous_sale_price` | numeric(18,2) | YES | Prior sale price |
| `previous_sale_date` | date | YES | Prior sale date |
| `latitude` | float8 | YES | Property latitude |
| `longitude` | float8 | YES | Property longitude |
| `photo_urls` | jsonb | YES | Array of CDN photo URLs from listing |
| `bedrooms` | numeric | YES | Bed count |
| `bathrooms` | numeric | YES | Bath count |
| `sqft_total` | integer | YES | Total square footage |
| `year_built` | integer | YES | Year built |
| `sold_to` | text | YES | **Buyer name** — auto-resolved by trigger from `hcpa_allsales` |
| `buyer_type` | varchar | YES | `Third Party` / `Plaintiff` — set by trigger |
| `html_path` | text | YES | Local path to original scraped HTML file |
| `created_at` | timestamptz | YES | Row creation time |
| `updated_at` | timestamptz | YES | Last update |

**Indexes**: PK `id`; unique `listing_id`; btree `auction_date`, `case_number`, `folio`, `strap`

---

### `property_market`
**10 rows** — Consolidated best-of-sources market snapshot written by the pipeline's `PropertyMarketService`. One row per HCPA strap. Joins to `hcpa_bulk_parcels` via `strap`, to `hcpa_allsales` via `folio`.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `strap` | varchar(64) | NOT NULL | **PK** = pipeline `parcel_id` = HCPA strap |
| `folio` | varchar(32) | YES | 10-digit folio (for `hcpa_allsales` joins) |
| `case_number` | varchar(32) | YES | Foreclosure case number |
| `zestimate` | numeric(18,2) | YES | Best valuation: Zillow > Redfin > HomeHarvest |
| `rent_zestimate` | numeric(18,2) | YES | Rent estimate: Zillow > HomeHarvest |
| `list_price` | numeric(18,2) | YES | Active list price: Redfin > Zillow > HomeHarvest |
| `tax_assessed_value` | numeric(18,2) | YES | Tax assessed value: Zillow > Redfin |
| `beds` | integer | YES | Bedrooms (first non-null across sources) |
| `baths` | numeric(5,2) | YES | Bathrooms |
| `sqft` | integer | YES | Heated square footage |
| `year_built` | integer | YES | Year built |
| `lot_size` | text | YES | Lot size string |
| `property_type` | varchar(64) | YES | Single family / condo / etc. |
| `listing_status` | varchar(32) | YES | Active / Off Market / etc. |
| `detail_url` | text | YES | Zillow/Redfin listing URL |
| `photo_local_paths` | jsonb | YES | Array of relative local paths: `Foreclosure/{case}/photos/NNN_hash.ext` |
| `photo_cdn_urls` | jsonb | YES | Array of original CDN URLs (fallback if local missing) |
| `zillow_json` | jsonb | YES | Full raw Zillow listing JSON |
| `redfin_json` | jsonb | YES | Full raw Redfin listing JSON |
| `homeharvest_json` | jsonb | YES | Full raw HomeHarvest record JSON |
| `primary_source` | varchar(16) | YES | Source contributing most data (`zillow`/`redfin`/`homeharvest`) |
| `created_at` | timestamptz | NOT NULL | First consolidated |
| `updated_at` | timestamptz | NOT NULL | Last consolidated |

**Indexes**: PK `strap`; btree `folio`, `case_number`

---

### `TrustAccount`
**2,850 rows** — Clerk's trust/escrow account daily snapshots per case. Coverage: 2026-02-11 to 2026-02-17 (1 week). Tracks escrow balances for pending foreclosure auctions.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | bigint | NOT NULL | PK |
| `source` | text | NOT NULL | Data source identifier |
| `report_date` | date | NOT NULL | Snapshot date |
| `case_number` | text | NOT NULL | Court case number |
| `movement_type` | text | NOT NULL | `stable` / `entered` / `dropped` / `changed` |
| `amount` | float8 | YES | Current escrow balance |
| `previous_amount` | float8 | YES | Balance on prior report |
| `delta_amount` | float8 | YES | Change (entered = new; dropped = removed; changed = delta) |
| `in_escrow_since` | date | YES | Date funds entered escrow |
| `multiple_recipients` | integer | YES | Flag: multiple payee accounts |
| `has_negative` | integer | YES | Flag: any negative line items |
| `has_offset_pair` | integer | YES | Flag: offsetting entries present |
| `max_abs_amount` | float8 | YES | Largest absolute line item |
| `division_codes` | text | YES | Court divisions associated |
| `registry_net_sum` | float8 | YES | Net registry balance |
| `plaintiff_name` | text | YES | Plaintiff extracted from case style |
| `counterparty_type` | text | YES | `bank` / `unknown` |
| `match_upcoming_auction` | integer | YES | Flag: matched to an upcoming auction date |
| `upcoming_auction_date` | date | YES | Next auction date for this case |
| `winning_bid_date` | date | YES | Date winning bid was placed |
| `winning_bid_match_count` | integer | YES | Number of matching auction bids |
| `winning_bid_amount` | float8 | YES | Matched winning bid amount |
| `days_before_winning_auction` | integer | YES | Days between escrow entry and auction |
| `is_pre_auction_signal` | integer | YES | Flag: deposit received before auction (bidder signal) |
| `raw_payload` | text | YES | Original raw data for debugging |
| `created_at` / `updated_at` | timestamptz | YES | Timestamps |

**Movement types**: `stable` (2,242) = funds held unchanged; `entered` (578) = new deposits; `dropped` (19) = funds withdrawn; `changed` (11) = balance adjusted

**Unique key**: `(source, report_date, case_number, movement_type)`

---

### `TrustAccountSummary`
**15 rows** — Daily aggregate totals from `TrustAccount`, broken out by counterparty type. One row per `(source, report_date, scope, counterparty_type)`.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `source` | text | `real` or test source |
| `report_date` | date | Snapshot date |
| `scope` | text | `all_cases` or filtered scope |
| `counterparty_type` | text | `bank` / `unknown` |
| `case_count` | integer | Number of cases in escrow |
| `total_amount` | float8 | Total escrow balance |
| `avg_amount` | float8 | Average per case |
| `max_amount` | float8 | Largest single case balance |
| `created_at` / `updated_at` | timestamptz | |

---

### `sunbiz_flr_filings`
**21,267 rows** — UCC-1 Financing Statement headers from Florida Secured Transaction Registry (Sunbiz). All 21,267 are type `F` (financing statement), status `A` (active). Filed 2007–2026, expirations out to 9211 (perpetual/error values).

| Column | Type | Notes |
|--------|------|-------|
| `doc_number` | varchar(32) PK | UCC document number |
| `filing_date` | date | Filing date |
| `pages` / `total_pages` | integer | Page counts |
| `filing_status` | varchar(8) | `A` = active |
| `filing_type` | varchar(8) | `F` = financing statement |
| `assessment_date` | date | Assessment date |
| `cancellation_date` | date | Cancellation date |
| `expiration_date` | date | Lapse/expiration date |
| `trans_utility` | boolean | Transmitting utility flag |
| `filing_event_count` | integer | Number of events on filing |
| `total_debtor_count` / `current_debtor_count` | integer | Debtor party counts |
| `total_secured_count` / `current_secured_count` | integer | Secured party counts |
| `source_file_id` | bigint | FK → `ingest_files.id` |
| `source_member` | text | Archive member |
| `source_line_number` | integer | Line in source file |
| `updated_at` | timestamptz | |

---

### `sunbiz_flr_parties`
**44,298 rows** — Debtors (23,025) and secured parties (21,273) on UCC filings.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `doc_number` | varchar(32) | FK → `sunbiz_flr_filings.doc_number` |
| `party_role` | varchar(8) | `debtor` / `secured` |
| `filing_type` | varchar(8) | Filing type code |
| `name` | text | Party name (trigram indexed) |
| `name_format` | varchar(8) | Name format code |
| `address1` / `address2` | text | Street address |
| `city` / `state` / `zip_code` / `country` | text/varchar | Location |
| `sequence_number` | integer | Position in filing |
| `relation_to_filing` | varchar(8) | Relationship code |
| `original_party` | varchar(8) | Original vs amended party flag |
| `filing_status` | varchar(8) | Party's status on filing |
| `source_file_id` | bigint | |
| `source_member` / `source_line_number` | text/int | |
| `loaded_at` | timestamptz | |

---

### `sunbiz_flr_events`
**96,091 rows** — Individual events/actions on UCC financing statements (amendments, continuations, terminations, etc.).

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `event_doc_number` | varchar(32) | UCC event doc number |
| `event_orig_doc_number` | varchar(32) | Original filing being amended |
| `event_action_count` | integer | Number of actions in event |
| `event_sequence_number` | integer | Event sequence |
| `event_pages` | integer | Pages in event filing |
| `event_date` | date | Date of event |
| `action_sequence_number` | integer | Action within event |
| `action_code` | varchar(16) | Action type code |
| `action_verbage` | text | Legal verbiage |
| `action_name` | text | Action description |
| `action_address1/2` | text | Address for action |
| `action_city/state/zip/country` | text/varchar | Location |
| `action_old_name_seq` / `action_new_name_seq` | integer | Name change sequences |
| `action_name_type` | varchar(8) | Name change type |
| `source_file_id` | bigint | |
| `source_member` / `source_line_number` | text/int | |
| `loaded_at` | timestamptz | |

---

### `sunbiz_raw_records`
**23,183 rows** — Fixed-width raw source lines from Sunbiz UCC data files, preserved for audit/reprocessing.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `file_id` | bigint | FK → `ingest_files.id` |
| `source_member` | text | Archive member name |
| `line_number` | integer | Line number in source |
| `record_type` | varchar(8) | Record type code |
| `doc_number` | varchar(32) | UCC document number |
| `raw_line` | text | Original fixed-width data line |
| `loaded_at` | timestamptz | |

---

### `ori_encumbrances`
**0 rows** — PG mirror schema for ORI encumbrance data (populated in SQLite pipeline, not yet synced to PG). Full schema defined with enum types.

**encumbrance_type enum**: `mortgage`, `judgment`, `lis_pendens`, `lien`, `satisfaction`, `release`, `assignment`, `other`

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `folio` | varchar(32) NOT NULL | 10-digit folio |
| `strap` | varchar(64) | HCPA strap |
| `instrument_number` | varchar(64) | ORI instrument number |
| `book` / `page` | varchar(16) | OR book/page |
| `book_type` | varchar(8) | Default `OR` |
| `ori_uuid` / `ori_id` | varchar | ORI internal identifiers |
| `raw_document_type` | text | Raw ORI type string e.g. `(MTG) MORTGAGE` |
| `encumbrance_type` | enum | Normalized type |
| `party1` / `party2` | text | Grantor / grantee names |
| `parties_one_json` / `parties_two_json` | jsonb | Full party arrays |
| `party1_dmetaphone` / `party2_dmetaphone` | text | Phonetic codes |
| `amount` | numeric(18,2) | Dollar amount of encumbrance |
| `amount_confidence` | varchar(16) | `high`/`medium`/`low`/`unknown` |
| `amount_source` | varchar(32) | Source of amount data |
| `recording_date` / `effective_date` | date | Dates |
| `case_number` | varchar(32) | Associated case number |
| `legal_description` | text | Legal description |
| `is_satisfied` | boolean | Default false |
| `satisfaction_date` | date | |
| `satisfaction_instrument/book/page` | varchar | |
| `satisfaction_method` | enum | Method of satisfaction |
| `satisfies_encumbrance_id` | bigint | FK to satisfied encumbrance |
| `survival_status` | varchar(16) | `SURVIVED`/`EXTINGUISHED`/etc. |
| `survival_reason` | text | |
| `survival_analyzed_at` | timestamptz | |
| `survival_case_number` | varchar(32) | Foreclosure case |
| `current_holder` | text | Current lien holder (after assignments) |
| `assignment_count` | integer | Default 0 |
| `mrta_expiration_date` | date | Marketable Record Title Act expiration |
| `source_file_id` | integer | |
| `discovered_at` / `updated_at` | timestamptz | |

---

### `ori_encumbrance_assignments`
**0 rows** — Tracks assignment chain for each ORI encumbrance (who the lien was assigned to and when).

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `encumbrance_id` | bigint NOT NULL | FK → `ori_encumbrances.id` |
| `instrument_number` | varchar(64) | Assignment instrument |
| `book` / `page` | varchar(16) | OR book/page |
| `recording_date` | date | |
| `assignor` | text | Party assigning the lien |
| `assignee` | text | Party receiving the lien (trigram indexed) |
| `assignee_dmetaphone` | text | Phonetic code |
| `ori_uuid` | varchar(128) | |
| `source_file_id` | integer | |
| `discovered_at` | timestamptz | Default now() |

---

### `ori_encumbrance_satisfactions`
**0 rows** — Links encumbrances to their satisfaction documents (many-to-many).

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `encumbrance_id` | bigint NOT NULL | FK → `ori_encumbrances.id` |
| `satisfaction_id` | bigint NOT NULL | FK → `ori_encumbrances.id` (the satisfaction doc) |
| `link_method` | enum NOT NULL | How the link was determined |
| `is_partial` | boolean | Default false |
| `partial_amount` | numeric(18,2) | If partial satisfaction |
| `notes` | text | |
| `linked_at` | timestamptz | Default now() |

---

### `hcpa_latlon`
**44,000 rows** — HCPA-provided parcel centroid coordinates. Subset of `hcpa_bulk_parcels` (which also has lat/lon embedded).

| Column | Type | Notes |
|--------|------|-------|
| `folio` | varchar(32) PK | 10-digit folio |
| `latitude` | float8 | Parcel centroid |
| `longitude` | float8 | Parcel centroid |
| `source_file_id` | bigint | |
| `updated_at` | timestamptz | |

---

### `hcpa_parcel_dor_names`
**305 rows** — Lookup: DOR land-use code → description. e.g. `0000` = "VACANT RESIDENTIAL < 20 AC", `2700` = "AUTOMOTIVE".

| Column | Type | Notes |
|--------|------|-------|
| `dor_code` | varchar(16) PK | DOR land-use code |
| `description` | text | Human-readable description |
| `source_file_id` | bigint | |
| `updated_at` | timestamptz | |

---

### `hcpa_parcel_sub_names`
**11,491 rows** — Lookup: subdivision code → subdivision name + plat book/page. e.g. `001` = "KEYSTONE PARK COLONY SUB", plat 5/55.

| Column | Type | Notes |
|--------|------|-------|
| `sub_code` | varchar(16) PK | Subdivision code |
| `sub_name` | text | Full subdivision name |
| `plat_bk` | varchar(32) | Plat book number |
| `page` | varchar(32) | Plat page |
| `source_file_id` | bigint | |
| `updated_at` | timestamptz | |

---

### `hcpa_subdivisions`
**11,280 rows** — GIS polygon data for each subdivision: legal name, subdivision code, plat reference, and shape measurements.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `object_id` | integer | GIS object ID |
| `legal1` | text | Legal subdivision name |
| `sub_code` | varchar(16) | Subdivision code |
| `plat_bk` | varchar(32) | Plat book |
| `page` | varchar(32) | Plat page |
| `area` | numeric(20,4) | Polygon area (sq ft) |
| `shape_star` | numeric(20,6) | Shape area |
| `shape_stle` | numeric(20,6) | Shape perimeter |
| `source_file_id` | bigint | |
| `source_line_number` | integer | |
| `loaded_at` | timestamptz | |

---

### Special District Tables

All share the same structure: `id` PK, a district code, `name`, `area`, `perimeter`, `source_file_id`, `source_line_number`, `loaded_at`.

| Table | Rows | Code Column | Purpose |
|-------|-----:|-------------|---------|
| `hcpa_special_district_cdds` | 178 | `cdd_code` | Community Development Districts (e.g. Stonebrier, Heritage Harbor) |
| `hcpa_special_district_lds` | 69 | `ld_code` | Land Development Districts |
| `hcpa_special_district_sd` | 55 | `dist_num` | Special Districts type 1 (has `sp_name`, `ord_value`, `dist_type`, `dist_tp`) |
| `hcpa_special_district_sd2` | 7 | `sd_code` | Special Districts type 2 |
| `hcpa_special_district_tifs` | 15 | `tif_code` | Tax Increment Finance Districts |

---

### `ingest_files`
**94 rows** — ETL manifest tracking every source file loaded into PG, with SHA256, size, status, and row count.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigint PK | |
| `source_system` | varchar(32) | `hcpa` / `clerk_civil` / `sunbiz` |
| `category` | varchar(64) | e.g. `allsales`, `bulk_parcels`, `cases`, `flr_structured` |
| `relative_path` | text | Path to source file |
| `file_sha256` | varchar(64) | SHA256 hash for dedup |
| `file_size_bytes` | bigint | File size |
| `file_modified_at` | timestamptz | File modification time |
| `discovered_at` | timestamptz | When file was found |
| `loaded_at` | timestamptz | When ETL completed |
| `loader_version` | varchar(32) | Loader version string |
| `status` | varchar(16) | `loaded` / `error` / `pending` |
| `row_count` | integer | Rows successfully loaded |
| `error_message` | text | Error detail if failed |

**Unique key**: `(source_system, relative_path)`

---

## Triggers

| Trigger | Table | Fires | Function |
|---------|-------|-------|----------|
| `trg_resolve_buyer` | `historical_auctions` | BEFORE INSERT OR UPDATE | `resolve_buyer_name()` — auto-fills `sold_to`/`buyer_type` from `hcpa_allsales` using CT→grantee / WD+QC→grantor logic |
| `trg_ori_enc_computed` | `ori_encumbrances` | BEFORE INSERT OR UPDATE | `ori_encumbrance_computed_fields()` — computes dmetaphone codes and derived fields |

## Key Functions

| Function | Purpose |
|----------|---------|
| `resolve_property_by_name(name text)` | Fuzzy property lookup: trigram + dmetaphone + sales history → returns folio/strap matches |
| `resolve_buyer_name()` | Trigger: resolves placeholder auction buyer names |
| `ori_encumbrance_computed_fields()` | Trigger: fills phonetic columns on ORI encumbrances |
| `dmetaphone()` / `dmetaphone_alt()` | Double Metaphone phonetic encoding |
| `soundex()` / `metaphone()` | Standard phonetic encodings |
| `similarity()` / `word_similarity()` | Trigram similarity functions (pg_trgm) |

## Common Join Patterns

```sql
-- Property detail: parcel + market snapshot + sales history
SELECT p.owner_name, p.just_value, m.zestimate, s.sale_date, s.sale_amount
FROM hcpa_bulk_parcels p
LEFT JOIN property_market m ON m.strap = p.strap
LEFT JOIN hcpa_allsales s ON s.folio = p.folio
WHERE p.folio = '1234560000'
ORDER BY s.sale_date DESC;

-- Auction + buyer resolution (trigger already handled) + parcel specs
SELECT a.auction_date, a.winning_bid, a.sold_to, a.buyer_type,
       p.beds, p.baths, p.heated_area, p.just_value
FROM historical_auctions a
LEFT JOIN hcpa_bulk_parcels p ON p.folio = a.folio
WHERE a.auction_date >= '2023-01-01';

-- Find all foreclosure auction buyers by name (fuzzy)
SELECT a.case_number, a.auction_date, a.sold_to, a.winning_bid
FROM historical_auctions a
WHERE a.sold_to % 'BLACKSTONE'   -- trigram similarity
ORDER BY similarity(a.sold_to, 'BLACKSTONE') DESC;

-- Upcoming auctions with trust account signal (pre-auction deposits)
SELECT t.case_number, t.amount, t.is_pre_auction_signal, t.upcoming_auction_date
FROM "TrustAccount" t
WHERE t.is_pre_auction_signal = 1
ORDER BY t.upcoming_auction_date;

-- UCC liens on a property owner (by fuzzy name)
SELECT f.doc_number, f.filing_date, f.expiration_date, p.name AS debtor, ps.name AS secured_party
FROM sunbiz_flr_filings f
JOIN sunbiz_flr_parties p ON p.doc_number = f.doc_number AND p.party_role = 'debtor'
JOIN sunbiz_flr_parties ps ON ps.doc_number = f.doc_number AND ps.party_role = 'secured'
WHERE p.name % 'SMITH JOHN'
  AND f.filing_status = 'A';
```


## Source: POSTGRES_REFERENCE.md

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


## Source: schema.md

# Database Schema

**SQLite DB (default):** `data/property_master_sqlite.db` (override via `HILLS_SQLITE_DB`).

**Generated:** 2026-02-14 16:44 UTC from `/home/user/hills_data/property_master_sqlite.db`.

## Tables

- [analysis_results](#analysis_results)
- [auction_scrape_log](#auction_scrape_log)
- [auctions](#auctions)
- [bulk_parcels](#bulk_parcels)
- [chain_of_title](#chain_of_title)
- [documents](#documents)
- [encumbrances](#encumbrances)
- [history_auctions](#history_auctions)
- [history_property_details](#history_property_details)
- [history_resales](#history_resales)
- [history_scraped_dates](#history_scraped_dates)
- [home_harvest](#home_harvest)
- [legal_variations](#legal_variations)
- [liens](#liens)
- [linked_identities](#linked_identities)
- [market_data](#market_data)
- [ori_search_queue](#ori_search_queue)
- [parcels](#parcels)
- [permits](#permits)
- [property_parties](#property_parties)
- [property_sources](#property_sources)
- [sales_history](#sales_history)
- [scraper_outputs](#scraper_outputs)
- [status](#status)

## analysis_results

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | YES |  |  |
| `case_number` | `TEXT` | YES |  |  |
| `market_value` | `REAL` | YES |  |  |
| `realtor_estimate` | `REAL` | YES |  |  |
| `zillow_estimate` | `REAL` | YES |  |  |
| `rehab_cost` | `REAL` | YES |  |  |
| `surviving_liens_total` | `REAL` | YES |  |  |
| `auction_bid` | `REAL` | YES |  |  |
| `net_equity` | `REAL` | YES |  |  |
| `roi_percentage` | `REAL` | YES |  |  |
| `risk_score` | `REAL` | YES |  |  |
| `has_hoa_lien` | `INTEGER` | YES |  | 0 |
| `has_surviving_mortgage` | `INTEGER` | YES |  | 0 |
| `has_code_violations` | `INTEGER` | YES |  | 0 |
| `has_tax_certificate` | `INTEGER` | YES |  | 0 |
| `analyzed_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_analysis_folio` (INDEX) on (`folio`)

## auction_scrape_log

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `auction_date` | `DATE` | NO | PRI |  |
| `auction_type` | `TEXT` | NO | PRI |  |
| `auction_count` | `INTEGER` | YES |  |  |
| `scraped_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `sqlite_autoindex_auction_scrape_log_1` (UNIQUE) on (`auction_date`, `auction_type`)

## auctions

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `case_number` | `TEXT` | YES | UNI |  |
| `folio` | `TEXT` | YES |  |  |
| `parcel_id` | `TEXT` | YES |  |  |
| `certificate_number` | `TEXT` | YES |  |  |
| `auction_type` | `TEXT` | YES |  |  |
| `auction_date` | `TEXT` | YES |  |  |
| `property_address` | `TEXT` | YES |  |  |
| `assessed_value` | `REAL` | YES |  |  |
| `final_judgment_amount` | `REAL` | YES |  |  |
| `opening_bid` | `REAL` | YES |  |  |
| `plaintiff_max_bid` | `TEXT` | YES |  |  |
| `lien_position` | `TEXT` | YES |  |  |
| `est_surviving_debt` | `REAL` | YES |  |  |
| `is_toxic_title` | `INTEGER` | YES |  | 0 |
| `final_judgment_content` | `TEXT` | YES |  |  |
| `plaintiff` | `TEXT` | YES |  |  |
| `defendant` | `TEXT` | YES |  |  |
| `foreclosure_type` | `TEXT` | YES |  |  |
| `judgment_date` | `TEXT` | YES |  |  |
| `lis_pendens_date` | `TEXT` | YES |  |  |
| `foreclosure_sale_date` | `TEXT` | YES |  |  |
| `total_judgment_amount` | `REAL` | YES |  |  |
| `principal_amount` | `REAL` | YES |  |  |
| `interest_amount` | `REAL` | YES |  |  |
| `attorney_fees` | `REAL` | YES |  |  |
| `court_costs` | `REAL` | YES |  |  |
| `original_mortgage_amount` | `REAL` | YES |  |  |
| `original_mortgage_date` | `TEXT` | YES |  |  |
| `monthly_payment` | `REAL` | YES |  |  |
| `default_date` | `TEXT` | YES |  |  |
| `extracted_judgment_data` | `TEXT` | YES |  |  |
| `raw_judgment_text` | `TEXT` | YES |  |  |
| `judgment_extracted_at` | `TEXT` | YES |  |  |
| `status` | `TEXT` | YES |  | 'PENDING' |
| `needs_judgment_extraction` | `INTEGER` | YES |  | 1 |
| `needs_hcpa_enrichment` | `INTEGER` | YES |  | 1 |
| `needs_ori_ingestion` | `INTEGER` | YES |  | 1 |
| `needs_lien_survival` | `INTEGER` | YES |  | 1 |
| `needs_sunbiz_search` | `INTEGER` | YES |  | 1 |
| `needs_permit_check` | `INTEGER` | YES |  | 1 |
| `needs_flood_check` | `INTEGER` | YES |  | 1 |
| `needs_market_data` | `INTEGER` | YES |  | 1 |
| `needs_tax_check` | `INTEGER` | YES |  | 1 |
| `needs_homeharvest_enrichment` | `INTEGER` | YES |  | 1 |
| `hcpa_scrape_failed` | `INTEGER` | YES |  | 0 |
| `hcpa_scrape_error` | `TEXT` | YES |  |  |
| `has_valid_parcel_id` | `INTEGER` | YES |  | 1 |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |
| `ori_party_fallback_used` | `INTEGER` | YES |  | 0 |
| `ori_party_fallback_note` | `TEXT` | YES |  |  |

**Indexes**
- `idx_auctions_status` (INDEX) on (`status`)
- `idx_auctions_type` (INDEX) on (`auction_type`)
- `idx_auctions_date` (INDEX) on (`auction_date`)
- `idx_auctions_folio` (INDEX) on (`folio`)
- `sqlite_autoindex_auctions_1` (UNIQUE) on (`case_number`)

## bulk_parcels

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `folio` | `TEXT` | NO | PRI |  |
| `pin` | `TEXT` | YES |  |  |
| `strap` | `TEXT` | YES |  |  |
| `owner_name` | `TEXT` | YES |  |  |
| `property_address` | `TEXT` | YES |  |  |
| `city` | `TEXT` | YES |  |  |
| `zip_code` | `TEXT` | YES |  |  |
| `land_use` | `TEXT` | YES |  |  |
| `land_use_desc` | `TEXT` | YES |  |  |
| `year_built` | `INTEGER` | YES |  |  |
| `beds` | `REAL` | YES |  |  |
| `baths` | `REAL` | YES |  |  |
| `stories` | `REAL` | YES |  |  |
| `units` | `INTEGER` | YES |  |  |
| `buildings` | `INTEGER` | YES |  |  |
| `heated_area` | `REAL` | YES |  |  |
| `lot_size` | `REAL` | YES |  |  |
| `assessed_value` | `REAL` | YES |  |  |
| `market_value` | `REAL` | YES |  |  |
| `just_value` | `REAL` | YES |  |  |
| `land_value` | `REAL` | YES |  |  |
| `building_value` | `REAL` | YES |  |  |
| `extra_features_value` | `REAL` | YES |  |  |
| `taxable_value` | `REAL` | YES |  |  |
| `last_sale_date` | `TEXT` | YES |  |  |
| `last_sale_price` | `REAL` | YES |  |  |
| `raw_type` | `TEXT` | YES |  |  |
| `raw_sub` | `TEXT` | YES |  |  |
| `raw_taxdist` | `TEXT` | YES |  |  |
| `raw_muni` | `TEXT` | YES |  |  |
| `raw_legal1` | `TEXT` | YES |  |  |
| `raw_legal2` | `TEXT` | YES |  |  |
| `raw_legal3` | `TEXT` | YES |  |  |
| `raw_legal4` | `TEXT` | YES |  |  |
| `latitude` | `REAL` | YES |  |  |
| `longitude` | `REAL` | YES |  |  |
| `ingest_date` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_bulk_parcels_landuse` (INDEX) on (`land_use`)
- `idx_bulk_parcels_owner` (INDEX) on (`owner_name`)
- `idx_bulk_parcels_address` (INDEX) on (`property_address`)
- `idx_bulk_parcels_strap` (INDEX) on (`strap`)
- `sqlite_autoindex_bulk_parcels_1` (UNIQUE) on (`folio`)

## chain_of_title

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | YES |  |  |
| `owner_name` | `TEXT` | YES |  |  |
| `acquired_from` | `TEXT` | YES |  |  |
| `acquisition_date` | `TEXT` | YES |  |  |
| `disposition_date` | `TEXT` | YES |  |  |
| `acquisition_instrument` | `TEXT` | YES |  |  |
| `acquisition_doc_type` | `TEXT` | YES |  |  |
| `acquisition_price` | `REAL` | YES |  |  |
| `link_status` | `TEXT` | YES |  |  |
| `confidence_score` | `REAL` | YES |  |  |
| `mrta_status` | `TEXT` | YES |  |  |
| `years_covered` | `REAL` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

## documents

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | YES |  |  |
| `case_number` | `TEXT` | YES |  |  |
| `document_type` | `TEXT` | YES |  |  |
| `file_path` | `TEXT` | YES |  |  |
| `ocr_text` | `TEXT` | YES |  |  |
| `extracted_data` | `TEXT` | YES |  |  |
| `recording_date` | `TEXT` | YES |  |  |
| `book` | `TEXT` | YES |  |  |
| `page` | `TEXT` | YES |  |  |
| `instrument_number` | `TEXT` | YES |  |  |
| `party1` | `TEXT` | YES |  |  |
| `party2` | `TEXT` | YES |  |  |
| `legal_description` | `TEXT` | YES |  |  |
| `sales_price` | `REAL` | YES |  |  |
| `page_count` | `INTEGER` | YES |  |  |
| `ori_uuid` | `TEXT` | YES | UNI |  |
| `ori_id` | `TEXT` | YES |  |  |
| `book_type` | `TEXT` | YES |  |  |
| `party2_resolution_method` | `TEXT` | YES |  |  |
| `is_self_transfer` | `INTEGER` | YES |  | 0 |
| `self_transfer_type` | `TEXT` | YES |  |  |
| `party2_confidence` | `REAL` | YES |  | 1.0 |
| `party2_resolved_at` | `TEXT` | YES |  |  |
| `triggered_by_search_id` | `INTEGER` | YES |  |  |
| `parties_one` | `TEXT` | YES |  |  |
| `parties_two` | `TEXT` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_documents_ori_uuid` (UNIQUE) on (`ori_uuid`)
- `idx_documents_folio_instrument` (UNIQUE) on (`folio`, `instrument_number`)
- `idx_documents_instrument` (INDEX) on (`instrument_number`)
- `idx_documents_case` (INDEX) on (`case_number`)
- `idx_documents_folio` (INDEX) on (`folio`)

## encumbrances

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | YES |  |  |
| `chain_period_id` | `INTEGER` | YES |  |  |
| `encumbrance_type` | `TEXT` | YES |  |  |
| `creditor` | `TEXT` | YES |  |  |
| `debtor` | `TEXT` | YES |  |  |
| `amount` | `REAL` | YES |  |  |
| `amount_confidence` | `TEXT` | YES |  |  |
| `amount_flags` | `TEXT` | YES |  |  |
| `recording_date` | `TEXT` | YES |  |  |
| `instrument` | `TEXT` | YES |  |  |
| `book` | `TEXT` | YES |  |  |
| `page` | `TEXT` | YES |  |  |
| `is_satisfied` | `INTEGER` | YES |  | 0 |
| `satisfaction_instrument` | `TEXT` | YES |  |  |
| `satisfaction_date` | `TEXT` | YES |  |  |
| `survival_status` | `TEXT` | YES |  |  |
| `survival_reason` | `TEXT` | YES |  |  |
| `party2_resolution_method` | `TEXT` | YES |  |  |
| `is_self_transfer` | `INTEGER` | YES |  | 0 |
| `self_transfer_type` | `TEXT` | YES |  |  |
| `is_joined` | `INTEGER` | YES |  | 0 |
| `is_inferred` | `INTEGER` | YES |  | 0 |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

## history_auctions

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `auction_id` | `TEXT` | NO | PRI |  |
| `auction_date` | `TEXT` | YES |  |  |
| `case_number` | `TEXT` | YES |  |  |
| `parcel_id` | `TEXT` | YES |  |  |
| `property_address` | `TEXT` | YES |  |  |
| `winning_bid` | `REAL` | YES |  |  |
| `final_judgment_amount` | `REAL` | YES |  |  |
| `assessed_value` | `REAL` | YES |  |  |
| `sold_to` | `TEXT` | YES |  |  |
| `buyer_normalized` | `TEXT` | YES |  |  |
| `buyer_type` | `TEXT` | YES |  |  |
| `auction_url` | `TEXT` | YES |  |  |
| `pdf_url` | `TEXT` | YES |  |  |
| `pdf_path` | `TEXT` | YES |  |  |
| `status` | `TEXT` | YES |  |  |
| `scraped_at` | `TEXT` | YES |  | datetime('now') |
| `last_resale_scan_at` | `TEXT` | YES |  |  |
| `last_judgment_scan_at` | `TEXT` | YES |  |  |
| `pdf_judgment_amount` | `REAL` | YES |  |  |
| `pdf_principal_amount` | `REAL` | YES |  |  |
| `pdf_interest_amount` | `REAL` | YES |  |  |
| `pdf_attorney_fees` | `REAL` | YES |  |  |
| `pdf_court_costs` | `REAL` | YES |  |  |
| `judgment_red_flags` | `TEXT` | YES |  |  |
| `judgment_data_json` | `TEXT` | YES |  |  |

**Indexes**
- `sqlite_autoindex_history_auctions_1` (UNIQUE) on (`auction_id`)

## history_property_details

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `parcel_id` | `TEXT` | NO | PRI |  |
| `est_market_value` | `REAL` | YES |  |  |
| `est_resale_value` | `REAL` | YES |  |  |
| `value_delta` | `REAL` | YES |  |  |
| `primary_image_url` | `TEXT` | YES |  |  |
| `gallery_json` | `TEXT` | YES |  |  |
| `description` | `TEXT` | YES |  |  |
| `updated_at` | `TEXT` | YES |  | datetime('now') |

**Indexes**
- `sqlite_autoindex_history_property_details_1` (UNIQUE) on (`parcel_id`)

## history_resales

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `resale_id` | `TEXT` | NO | PRI |  |
| `parcel_id` | `TEXT` | YES |  |  |
| `auction_id` | `TEXT` | YES |  |  |
| `sale_date` | `TEXT` | YES |  |  |
| `sale_price` | `REAL` | YES |  |  |
| `sale_type` | `TEXT` | YES |  |  |
| `hold_time_days` | `INTEGER` | YES |  |  |
| `gross_profit` | `REAL` | YES |  |  |
| `roi` | `REAL` | YES |  |  |
| `source` | `TEXT` | YES |  |  |

**Indexes**
- `sqlite_autoindex_history_resales_1` (UNIQUE) on (`resale_id`)

**Foreign Keys**
- `auction_id` -> `history_auctions`.`auction_id` (on_update=NO ACTION, on_delete=NO ACTION)

## history_scraped_dates

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `auction_date` | `TEXT` | NO | PRI |  |
| `scraped_at` | `TEXT` | YES |  | datetime('now') |
| `status` | `TEXT` | YES |  |  |

**Indexes**
- `sqlite_autoindex_history_scraped_dates_1` (UNIQUE) on (`auction_date`)

## home_harvest

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | YES |  |  |
| `property_url` | `TEXT` | YES |  |  |
| `property_id` | `TEXT` | YES |  |  |
| `listing_id` | `TEXT` | YES |  |  |
| `mls` | `TEXT` | YES |  |  |
| `mls_id` | `TEXT` | YES |  |  |
| `mls_status` | `TEXT` | YES |  |  |
| `status` | `TEXT` | YES |  |  |
| `permalink` | `TEXT` | YES |  |  |
| `street` | `TEXT` | YES |  |  |
| `unit` | `TEXT` | YES |  |  |
| `city` | `TEXT` | YES |  |  |
| `state` | `TEXT` | YES |  |  |
| `zip_code` | `TEXT` | YES |  |  |
| `formatted_address` | `TEXT` | YES |  |  |
| `style` | `TEXT` | YES |  |  |
| `beds` | `REAL` | YES |  |  |
| `full_baths` | `REAL` | YES |  |  |
| `half_baths` | `REAL` | YES |  |  |
| `sqft` | `REAL` | YES |  |  |
| `year_built` | `INTEGER` | YES |  |  |
| `stories` | `REAL` | YES |  |  |
| `garage` | `REAL` | YES |  |  |
| `lot_sqft` | `REAL` | YES |  |  |
| `text_description` | `TEXT` | YES |  |  |
| `property_type` | `TEXT` | YES |  |  |
| `days_on_mls` | `INTEGER` | YES |  |  |
| `list_price` | `REAL` | YES |  |  |
| `list_price_min` | `REAL` | YES |  |  |
| `list_price_max` | `REAL` | YES |  |  |
| `list_date` | `TEXT` | YES |  |  |
| `pending_date` | `TEXT` | YES |  |  |
| `sold_price` | `REAL` | YES |  |  |
| `last_sold_date` | `TEXT` | YES |  |  |
| `last_status_change_date` | `TEXT` | YES |  |  |
| `last_update_date` | `TEXT` | YES |  |  |
| `last_sold_price` | `REAL` | YES |  |  |
| `price_per_sqft` | `REAL` | YES |  |  |
| `new_construction` | `INTEGER` | YES |  |  |
| `hoa_fee` | `REAL` | YES |  |  |
| `monthly_fees` | `TEXT` | YES |  |  |
| `one_time_fees` | `TEXT` | YES |  |  |
| `estimated_value` | `REAL` | YES |  |  |
| `tax_assessed_value` | `REAL` | YES |  |  |
| `tax_history` | `TEXT` | YES |  |  |
| `latitude` | `REAL` | YES |  |  |
| `longitude` | `REAL` | YES |  |  |
| `neighborhoods` | `TEXT` | YES |  |  |
| `county` | `TEXT` | YES |  |  |
| `fips_code` | `TEXT` | YES |  |  |
| `parcel_number` | `TEXT` | YES |  |  |
| `nearby_schools` | `TEXT` | YES |  |  |
| `agent_uuid` | `TEXT` | YES |  |  |
| `agent_name` | `TEXT` | YES |  |  |
| `agent_email` | `TEXT` | YES |  |  |
| `agent_phone` | `TEXT` | YES |  |  |
| `agent_state_license` | `TEXT` | YES |  |  |
| `broker_uuid` | `TEXT` | YES |  |  |
| `broker_name` | `TEXT` | YES |  |  |
| `office_uuid` | `TEXT` | YES |  |  |
| `office_name` | `TEXT` | YES |  |  |
| `office_email` | `TEXT` | YES |  |  |
| `office_phones` | `TEXT` | YES |  |  |
| `estimated_monthly_rental` | `REAL` | YES |  |  |
| `tags` | `TEXT` | YES |  |  |
| `flags` | `TEXT` | YES |  |  |
| `photos` | `TEXT` | YES |  |  |
| `primary_photo` | `TEXT` | YES |  |  |
| `alt_photos` | `TEXT` | YES |  |  |
| `open_houses` | `TEXT` | YES |  |  |
| `units` | `TEXT` | YES |  |  |
| `pet_policy` | `TEXT` | YES |  |  |
| `parking` | `TEXT` | YES |  |  |
| `terms` | `TEXT` | YES |  |  |
| `current_estimates` | `TEXT` | YES |  |  |
| `estimates` | `TEXT` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_homeharvest_folio` (INDEX) on (`folio`)

## legal_variations

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | NO |  |  |
| `variation_text` | `TEXT` | NO |  |  |
| `source_instrument` | `TEXT` | YES |  |  |
| `source_type` | `TEXT` | NO |  |  |
| `is_canonical` | `INTEGER` | YES |  | 0 |
| `priority` | `INTEGER` | YES |  | 99 |
| `search_attempted` | `INTEGER` | YES |  | 0 |
| `search_operator` | `TEXT` | YES |  |  |
| `search_result_count` | `INTEGER` | YES |  |  |
| `last_searched_at` | `TEXT` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_legal_variations_folio` (INDEX) on (`folio`)
- `sqlite_autoindex_legal_variations_1` (UNIQUE) on (`folio`, `variation_text`)

## liens

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | YES |  |  |
| `case_number` | `TEXT` | YES |  |  |
| `recording_date` | `TEXT` | YES |  |  |
| `document_type` | `TEXT` | YES |  |  |
| `book` | `TEXT` | YES |  |  |
| `page` | `TEXT` | YES |  |  |
| `amount` | `REAL` | YES |  |  |
| `grantor` | `TEXT` | YES |  |  |
| `grantee` | `TEXT` | YES |  |  |
| `description` | `TEXT` | YES |  |  |
| `instrument_number` | `TEXT` | YES |  |  |
| `survives_foreclosure` | `INTEGER` | YES |  |  |
| `is_surviving` | `INTEGER` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_liens_date` (INDEX) on (`recording_date`)
- `idx_liens_case` (INDEX) on (`case_number`)
- `idx_liens_folio` (INDEX) on (`folio`)

## linked_identities

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `canonical_name` | `TEXT` | NO |  |  |
| `entity_type` | `TEXT` | YES |  |  |
| `link_type` | `TEXT` | YES |  |  |
| `confidence` | `REAL` | YES |  | 1.0 |
| `sunbiz_doc_number` | `TEXT` | YES |  |  |
| `sunbiz_status` | `TEXT` | YES |  |  |
| `notes` | `TEXT` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_linked_identities_canonical` (INDEX) on (`canonical_name`)

## market_data

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | YES |  |  |
| `source` | `TEXT` | YES |  |  |
| `capture_date` | `TEXT` | YES |  |  |
| `listing_status` | `TEXT` | YES |  |  |
| `list_price` | `REAL` | YES |  |  |
| `zestimate` | `REAL` | YES |  |  |
| `rent_estimate` | `REAL` | YES |  |  |
| `hoa_monthly` | `REAL` | YES |  |  |
| `days_on_market` | `INTEGER` | YES |  |  |
| `price_history` | `TEXT` | YES |  |  |
| `raw_json` | `TEXT` | YES |  |  |
| `screenshot_path` | `TEXT` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

## ori_search_queue

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | NO |  |  |
| `search_type` | `TEXT` | NO |  |  |
| `search_term` | `TEXT` | NO |  |  |
| `search_operator` | `TEXT` | YES |  | '' |
| `priority` | `INTEGER` | YES |  | 50 |
| `status` | `TEXT` | YES |  | 'pending' |
| `attempt_count` | `INTEGER` | YES |  | 0 |
| `max_attempts` | `INTEGER` | YES |  | 3 |
| `date_from` | `TEXT` | YES |  |  |
| `date_to` | `TEXT` | YES |  |  |
| `triggered_by_instrument` | `TEXT` | YES |  |  |
| `triggered_by_search_id` | `INTEGER` | YES |  |  |
| `result_count` | `INTEGER` | YES |  |  |
| `new_documents_found` | `INTEGER` | YES |  |  |
| `error_message` | `TEXT` | YES |  |  |
| `queued_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |
| `started_at` | `TEXT` | YES |  |  |
| `completed_at` | `TEXT` | YES |  |  |
| `next_retry_at` | `TEXT` | YES |  |  |

**Indexes**
- `idx_search_queue_folio` (INDEX) on (`folio`)
- `idx_search_queue_status` (INDEX) on (`status`, `priority`)
- `sqlite_autoindex_ori_search_queue_1` (UNIQUE) on (`folio`, `search_type`, `search_term`, `search_operator`)

## parcels

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `folio` | `TEXT` | NO | PRI |  |
| `parcel_id` | `TEXT` | YES |  |  |
| `owner_name` | `TEXT` | YES |  |  |
| `property_address` | `TEXT` | YES |  |  |
| `city` | `TEXT` | YES |  |  |
| `zip_code` | `TEXT` | YES |  |  |
| `land_use` | `TEXT` | YES |  |  |
| `year_built` | `INTEGER` | YES |  |  |
| `beds` | `REAL` | YES |  |  |
| `baths` | `REAL` | YES |  |  |
| `heated_area` | `REAL` | YES |  |  |
| `lot_size` | `REAL` | YES |  |  |
| `assessed_value` | `REAL` | YES |  |  |
| `market_value` | `REAL` | YES |  |  |
| `last_sale_date` | `TEXT` | YES |  |  |
| `last_sale_price` | `REAL` | YES |  |  |
| `image_url` | `TEXT` | YES |  |  |
| `market_analysis_content` | `TEXT` | YES |  |  |
| `legal_description` | `TEXT` | YES |  |  |
| `latitude` | `REAL` | YES |  |  |
| `longitude` | `REAL` | YES |  |  |
| `tax_status` | `TEXT` | YES |  |  |
| `tax_warrant` | `INTEGER` | YES |  |  |
| `last_analyzed_case_number` | `TEXT` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |
| `bulk_folio` | `TEXT` | YES |  |  |
| `raw_legal1` | `TEXT` | YES |  |  |
| `flood_zone` | `TEXT` | YES |  |  |
| `judgment_legal_description` | `TEXT` | YES |  |  |
| `raw_legal2` | `TEXT` | YES |  |  |
| `raw_legal3` | `TEXT` | YES |  |  |
| `raw_legal4` | `TEXT` | YES |  |  |
| `strap` | `TEXT` | YES |  |  |
| `flood_zone_subtype` | `TEXT` | YES |  |  |
| `flood_risk` | `TEXT` | YES |  |  |
| `flood_risk_level` | `TEXT` | YES |  |  |
| `flood_insurance_required` | `INTEGER` | YES |  |  |
| `flood_base_elevation` | `REAL` | YES |  |  |

**Indexes**
- `idx_parcels_parcel_id` (INDEX) on (`parcel_id`)
- `idx_parcels_owner` (INDEX) on (`owner_name`)
- `sqlite_autoindex_parcels_1` (UNIQUE) on (`folio`)

## permits

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | YES |  |  |
| `permit_number` | `TEXT` | YES | UNI |  |
| `issue_date` | `TEXT` | YES |  |  |
| `status` | `TEXT` | YES |  |  |
| `permit_type` | `TEXT` | YES |  |  |
| `description` | `TEXT` | YES |  |  |
| `contractor` | `TEXT` | YES |  |  |
| `estimated_cost` | `REAL` | YES |  |  |
| `url` | `TEXT` | YES |  |  |
| `noc_instrument` | `TEXT` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_permits_folio` (INDEX) on (`folio`)
- `sqlite_autoindex_permits_1` (UNIQUE) on (`permit_number`)

## property_parties

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | NO |  |  |
| `party_name` | `TEXT` | NO |  |  |
| `party_name_normalized` | `TEXT` | YES |  |  |
| `party_role` | `TEXT` | YES |  |  |
| `linked_identity_id` | `INTEGER` | YES |  |  |
| `active_from` | `TEXT` | YES |  |  |
| `active_to` | `TEXT` | YES |  |  |
| `source_instrument` | `TEXT` | YES |  |  |
| `source_document_type` | `TEXT` | YES |  |  |
| `recording_date` | `TEXT` | YES |  |  |
| `search_attempted` | `INTEGER` | YES |  | 0 |
| `search_result_count` | `INTEGER` | YES |  |  |
| `last_searched_at` | `TEXT` | YES |  |  |
| `is_generic` | `INTEGER` | YES |  | 0 |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_property_parties_folio` (INDEX) on (`folio`)
- `sqlite_autoindex_property_parties_1` (UNIQUE) on (`folio`, `party_name`, `source_instrument`)

## property_sources

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | YES |  |  |
| `source_name` | `TEXT` | YES |  |  |
| `url` | `TEXT` | YES |  |  |
| `description` | `TEXT` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `sqlite_autoindex_property_sources_1` (UNIQUE) on (`folio`, `url`)

## sales_history

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `folio` | `TEXT` | YES |  |  |
| `strap` | `TEXT` | YES |  |  |
| `book` | `TEXT` | YES |  |  |
| `page` | `TEXT` | YES |  |  |
| `instrument` | `TEXT` | YES |  |  |
| `sale_date` | `TEXT` | YES |  |  |
| `doc_type` | `TEXT` | YES |  |  |
| `qualified` | `TEXT` | YES |  |  |
| `vacant_improved` | `TEXT` | YES |  |  |
| `sale_price` | `REAL` | YES |  |  |
| `ori_link` | `TEXT` | YES |  |  |
| `pdf_path` | `TEXT` | YES |  |  |
| `grantor` | `TEXT` | YES |  |  |
| `grantee` | `TEXT` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_sales_history_unique` (UNIQUE) CREATE UNIQUE INDEX idx_sales_history_unique ON sales_history(folio, COALESCE(book, ''), COALESCE(page, ''), COALESCE(instrument, ''))
- `idx_sales_history_instrument` (INDEX) on (`folio`, `instrument`)
- `idx_sales_history_strap` (INDEX) on (`strap`)
- `idx_sales_history_folio` (INDEX) on (`folio`)
- `sqlite_autoindex_sales_history_1` (UNIQUE) on (`folio`, `book`, `page`)

## scraper_outputs

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `id` | `INTEGER` | NO | PRI |  |
| `property_id` | `TEXT` | NO |  |  |
| `scraper` | `TEXT` | NO |  |  |
| `scraped_at` | `TEXT` | YES |  |  |
| `processed_at` | `TEXT` | YES |  |  |
| `screenshot_path` | `TEXT` | YES |  |  |
| `vision_output_path` | `TEXT` | YES |  |  |
| `raw_data_path` | `TEXT` | YES |  |  |
| `source_url` | `TEXT` | YES |  |  |
| `prompt_version` | `TEXT` | YES |  |  |
| `extraction_success` | `INTEGER` | YES |  | 0 |
| `error_message` | `TEXT` | YES |  |  |
| `extracted_summary` | `TEXT` | YES |  |  |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |

**Indexes**
- `idx_scraper_outputs_lookup` (INDEX) on (`property_id`, `scraper`)
- `idx_scraper_outputs_property` (INDEX) on (`property_id`)

## status

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :---: | :---: | :--- |
| `case_number` | `TEXT` | NO | PRI |  |
| `parcel_id` | `TEXT` | YES |  |  |
| `auction_date` | `TEXT` | YES |  |  |
| `auction_type` | `TEXT` | YES |  |  |
| `step_auction_scraped` | `TEXT` | YES |  |  |
| `step_pdf_downloaded` | `TEXT` | YES |  |  |
| `step_judgment_extracted` | `TEXT` | YES |  |  |
| `step_bulk_enriched` | `TEXT` | YES |  |  |
| `step_homeharvest_enriched` | `TEXT` | YES |  |  |
| `step_hcpa_enriched` | `TEXT` | YES |  |  |
| `step_ori_ingested` | `TEXT` | YES |  |  |
| `step_survival_analyzed` | `TEXT` | YES |  |  |
| `step_permits_checked` | `TEXT` | YES |  |  |
| `step_flood_checked` | `TEXT` | YES |  |  |
| `step_market_fetched` | `TEXT` | YES |  |  |
| `step_tax_checked` | `TEXT` | YES |  |  |
| `current_step` | `INTEGER` | YES |  | 0 |
| `pipeline_status` | `TEXT` | YES |  | 'pending' |
| `last_error` | `TEXT` | YES |  |  |
| `error_step` | `INTEGER` | YES |  |  |
| `retry_count` | `INTEGER` | YES |  | 0 |
| `created_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TEXT` | YES |  | CURRENT_TIMESTAMP |
| `completed_at` | `TEXT` | YES |  |  |

**Indexes**
- `idx_status_parcel` (INDEX) on (`parcel_id`)
- `idx_status_pipeline_status` (INDEX) on (`pipeline_status`)
- `idx_status_auction_date` (INDEX) on (`auction_date`)
- `sqlite_autoindex_status_1` (UNIQUE) on (`case_number`)



## Source: pg_tables_columns.md

# PostgreSQL Tables and Columns

Total tables: 28

## public.TrustAccount

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('"TrustAccount_id_seq"'::regclass) |
| 2 | source | text | NO |  |
| 3 | report_date | date | NO |  |
| 4 | case_number | text | NO |  |
| 5 | movement_type | text | NO |  |
| 6 | amount | double precision | YES |  |
| 7 | previous_amount | double precision | YES |  |
| 8 | delta_amount | double precision | YES |  |
| 9 | in_escrow_since | date | YES |  |
| 10 | multiple_recipients | integer | YES |  |
| 11 | has_negative | integer | YES |  |
| 12 | has_offset_pair | integer | YES |  |
| 13 | max_abs_amount | double precision | YES |  |
| 14 | division_codes | text | YES |  |
| 15 | registry_net_sum | double precision | YES |  |
| 16 | plaintiff_name | text | YES |  |
| 17 | counterparty_type | text | YES |  |
| 18 | match_upcoming_auction | integer | YES |  |
| 19 | upcoming_auction_date | date | YES |  |
| 20 | winning_bid_date | date | YES |  |
| 21 | winning_bid_match_count | integer | YES |  |
| 22 | winning_bid_amount | double precision | YES |  |
| 23 | days_before_winning_auction | integer | YES |  |
| 24 | is_pre_auction_signal | integer | YES |  |
| 25 | raw_payload | text | YES |  |
| 26 | created_at | timestamp with time zone | YES | now() |
| 27 | updated_at | timestamp with time zone | YES | now() |

## public.TrustAccountSummary

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('"TrustAccountSummary_id_seq"'::regclass) |
| 2 | source | text | NO |  |
| 3 | report_date | date | NO |  |
| 4 | scope | text | NO |  |
| 5 | counterparty_type | text | NO |  |
| 6 | case_count | integer | NO |  |
| 7 | total_amount | double precision | NO |  |
| 8 | avg_amount | double precision | YES |  |
| 9 | max_amount | double precision | YES |  |
| 10 | created_at | timestamp with time zone | YES | now() |
| 11 | updated_at | timestamp with time zone | YES | now() |

## public.clerk_civil_cases

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | case_number | character varying | NO |  |
| 2 | ucn | character varying | YES |  |
| 3 | style | text | YES |  |
| 4 | case_type | text | YES |  |
| 5 | division | character varying | YES |  |
| 6 | judge | text | YES |  |
| 7 | cause_of_action | text | YES |  |
| 8 | cause_description | text | YES |  |
| 9 | case_status | text | YES |  |
| 10 | filing_date | date | YES |  |
| 11 | judgment_code | text | YES |  |
| 12 | judgment_description | text | YES |  |
| 13 | judgment_date | date | YES |  |
| 14 | is_foreclosure | boolean | YES |  |
| 15 | source_file | text | YES |  |
| 16 | loaded_at | timestamp with time zone | NO |  |

## public.clerk_civil_events

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('clerk_civil_events_id_seq'::regclass) |
| 2 | case_number | character varying | NO |  |
| 3 | event_code | text | YES |  |
| 4 | event_description | text | YES |  |
| 5 | event_date | date | YES |  |
| 6 | party_first_name | text | YES |  |
| 7 | party_middle_name | text | YES |  |
| 8 | party_last_name | text | YES |  |
| 9 | source_file | text | YES |  |
| 10 | loaded_at | timestamp with time zone | NO |  |

## public.clerk_civil_parties

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('clerk_civil_parties_id_seq'::regclass) |
| 2 | case_number | character varying | NO |  |
| 3 | party_type | text | YES |  |
| 4 | name | text | YES |  |
| 5 | first_name | text | YES |  |
| 6 | middle_name | text | YES |  |
| 7 | last_name | text | YES |  |
| 8 | address1 | text | YES |  |
| 9 | address2 | text | YES |  |
| 10 | city | text | YES |  |
| 11 | state | text | YES |  |
| 12 | zip | text | YES |  |
| 13 | bar_number | text | YES |  |
| 14 | phone | text | YES |  |
| 15 | email | text | YES |  |
| 16 | source_file | text | YES |  |
| 17 | loaded_at | timestamp with time zone | NO |  |

## public.clerk_disposed_cases

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | case_number | character varying | NO |  |
| 2 | style | text | YES |  |
| 3 | case_type | text | YES |  |
| 4 | case_subtype | text | YES |  |
| 5 | closure_date | date | YES |  |
| 6 | statistical_closure | text | YES |  |
| 7 | closure_comment | text | YES |  |
| 8 | status_date | date | YES |  |
| 9 | current_status | text | YES |  |
| 10 | source_file | text | YES |  |
| 11 | loaded_at | timestamp with time zone | NO |  |

## public.dor_nal_parcels

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('dor_nal_parcels_id_seq'::regclass) |
| 2 | county_code | character varying | NO |  |
| 3 | parcel_id | character varying | NO |  |
| 4 | folio | character varying | YES |  |
| 5 | strap | character varying | YES |  |
| 6 | tax_year | integer | NO |  |
| 7 | owner_name | text | YES |  |
| 8 | owner_address1 | text | YES |  |
| 9 | owner_address2 | text | YES |  |
| 10 | owner_city | text | YES |  |
| 11 | owner_state | character varying | YES |  |
| 12 | owner_zip | character varying | YES |  |
| 13 | property_address | text | YES |  |
| 14 | city | text | YES |  |
| 15 | zip_code | character varying | YES |  |
| 16 | property_use_code | character varying | YES |  |
| 17 | just_value | numeric | YES |  |
| 18 | just_value_homestead | numeric | YES |  |
| 19 | assessed_value_school | numeric | YES |  |
| 20 | assessed_value_nonschool | numeric | YES |  |
| 21 | assessed_value_homestead | numeric | YES |  |
| 22 | taxable_value_school | numeric | YES |  |
| 23 | taxable_value_nonschool | numeric | YES |  |
| 24 | homestead_exempt | boolean | YES |  |
| 25 | homestead_exempt_value | numeric | YES |  |
| 26 | widow_exempt | boolean | YES |  |
| 27 | widow_exempt_value | numeric | YES |  |
| 28 | disability_exempt | boolean | YES |  |
| 29 | disability_exempt_value | numeric | YES |  |
| 30 | veteran_exempt | boolean | YES |  |
| 31 | veteran_exempt_value | numeric | YES |  |
| 32 | ag_exempt | boolean | YES |  |
| 33 | ag_exempt_value | numeric | YES |  |
| 34 | soh_differential | numeric | YES |  |
| 35 | total_millage | numeric | YES |  |
| 36 | county_millage | numeric | YES |  |
| 37 | school_millage | numeric | YES |  |
| 38 | city_millage | numeric | YES |  |
| 39 | estimated_annual_tax | numeric | YES |  |
| 40 | legal_description | text | YES |  |
| 41 | source_file | text | YES |  |
| 42 | source_file_id | bigint | NO |  |
| 43 | loaded_at | timestamp with time zone | NO |  |

## public.hcpa_allsales

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('hcpa_allsales_id_seq'::regclass) |
| 2 | pin | character varying | YES |  |
| 3 | folio | character varying | YES |  |
| 4 | dor_code | character varying | YES |  |
| 5 | nbhc | character varying | YES |  |
| 6 | sale_date | date | YES |  |
| 7 | vacant_improved | character varying | YES |  |
| 8 | qualification_code | character varying | YES |  |
| 9 | reason_code | character varying | YES |  |
| 10 | sale_amount | numeric | YES |  |
| 11 | sub_code | character varying | YES |  |
| 12 | street_code | character varying | YES |  |
| 13 | sale_type | character varying | YES |  |
| 14 | or_book | character varying | YES |  |
| 15 | or_page | character varying | YES |  |
| 16 | grantor | text | YES |  |
| 17 | grantee | text | YES |  |
| 18 | doc_num | character varying | YES |  |
| 19 | source_file_id | bigint | NO |  |
| 20 | source_line_number | integer | NO |  |
| 21 | loaded_at | timestamp with time zone | NO |  |
| 22 | grantee_dmetaphone | text | YES |  |
| 23 | grantor_dmetaphone | text | YES |  |

## public.hcpa_bulk_parcels

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | folio | character varying | NO |  |
| 2 | pin | character varying | YES |  |
| 3 | strap | character varying | YES |  |
| 4 | owner_name | text | YES |  |
| 5 | property_address | text | YES |  |
| 6 | city | character varying | YES |  |
| 7 | zip_code | character varying | YES |  |
| 8 | land_use | character varying | YES |  |
| 9 | land_use_desc | text | YES |  |
| 10 | year_built | integer | YES |  |
| 11 | beds | numeric | YES |  |
| 12 | baths | numeric | YES |  |
| 13 | stories | numeric | YES |  |
| 14 | units | integer | YES |  |
| 15 | buildings | integer | YES |  |
| 16 | heated_area | numeric | YES |  |
| 17 | lot_size | numeric | YES |  |
| 18 | assessed_value | numeric | YES |  |
| 19 | market_value | numeric | YES |  |
| 20 | just_value | numeric | YES |  |
| 21 | land_value | numeric | YES |  |
| 22 | building_value | numeric | YES |  |
| 23 | extra_features_value | numeric | YES |  |
| 24 | taxable_value | numeric | YES |  |
| 25 | last_sale_date | date | YES |  |
| 26 | last_sale_price | numeric | YES |  |
| 27 | raw_type | character varying | YES |  |
| 28 | raw_sub | character varying | YES |  |
| 29 | raw_taxdist | character varying | YES |  |
| 30 | raw_muni | character varying | YES |  |
| 31 | raw_legal1 | text | YES |  |
| 32 | raw_legal2 | text | YES |  |
| 33 | raw_legal3 | text | YES |  |
| 34 | raw_legal4 | text | YES |  |
| 35 | latitude | double precision | YES |  |
| 36 | longitude | double precision | YES |  |
| 37 | source_file_id | bigint | NO |  |
| 38 | updated_at | timestamp with time zone | NO |  |
| 39 | owner_dmetaphone | text | YES |  |
| 40 | owner_soundex | character varying | YES |  |

## public.hcpa_latlon

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | folio | character varying | NO |  |
| 2 | latitude | double precision | YES |  |
| 3 | longitude | double precision | YES |  |
| 4 | source_file_id | bigint | NO |  |
| 5 | updated_at | timestamp with time zone | NO |  |

## public.hcpa_parcel_dor_names

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | dor_code | character varying | NO |  |
| 2 | description | text | YES |  |
| 3 | source_file_id | bigint | NO |  |
| 4 | updated_at | timestamp with time zone | NO |  |

## public.hcpa_parcel_sub_names

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | sub_code | character varying | NO |  |
| 2 | sub_name | text | YES |  |
| 3 | plat_bk | character varying | YES |  |
| 4 | page | character varying | YES |  |
| 5 | source_file_id | bigint | NO |  |
| 6 | updated_at | timestamp with time zone | NO |  |

## public.hcpa_special_district_cdds

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('hcpa_special_district_cdds_id_seq'::regclass) |
| 2 | cdd_code | character varying | YES |  |
| 3 | name | text | YES |  |
| 4 | area | numeric | YES |  |
| 5 | perimeter | numeric | YES |  |
| 6 | source_file_id | bigint | NO |  |
| 7 | source_line_number | integer | NO |  |
| 8 | loaded_at | timestamp with time zone | NO |  |

## public.hcpa_special_district_lds

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('hcpa_special_district_lds_id_seq'::regclass) |
| 2 | ld_code | character varying | YES |  |
| 3 | name | text | YES |  |
| 4 | area | numeric | YES |  |
| 5 | perimeter | numeric | YES |  |
| 6 | source_file_id | bigint | NO |  |
| 7 | source_line_number | integer | NO |  |
| 8 | loaded_at | timestamp with time zone | NO |  |

## public.hcpa_special_district_sd

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('hcpa_special_district_sd_id_seq'::regclass) |
| 2 | sp_name | text | YES |  |
| 3 | ord_value | character varying | YES |  |
| 4 | dist_type | character varying | YES |  |
| 5 | dist_num | integer | YES |  |
| 6 | dist_tp | character varying | YES |  |
| 7 | area | numeric | YES |  |
| 8 | perimeter | numeric | YES |  |
| 9 | source_file_id | bigint | NO |  |
| 10 | source_line_number | integer | NO |  |
| 11 | loaded_at | timestamp with time zone | NO |  |

## public.hcpa_special_district_sd2

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('hcpa_special_district_sd2_id_seq'::regclass) |
| 2 | sd_code | character varying | YES |  |
| 3 | sp_name | text | YES |  |
| 4 | area | numeric | YES |  |
| 5 | perimeter | numeric | YES |  |
| 6 | source_file_id | bigint | NO |  |
| 7 | source_line_number | integer | NO |  |
| 8 | loaded_at | timestamp with time zone | NO |  |

## public.hcpa_special_district_tifs

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('hcpa_special_district_tifs_id_seq'::regclass) |
| 2 | tif_code | character varying | YES |  |
| 3 | name | text | YES |  |
| 4 | area | numeric | YES |  |
| 5 | perimeter | numeric | YES |  |
| 6 | source_file_id | bigint | NO |  |
| 7 | source_line_number | integer | NO |  |
| 8 | loaded_at | timestamp with time zone | NO |  |

## public.hcpa_subdivisions

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('hcpa_subdivisions_id_seq'::regclass) |
| 2 | object_id | integer | YES |  |
| 3 | legal1 | text | YES |  |
| 4 | sub_code | character varying | YES |  |
| 5 | plat_bk | character varying | YES |  |
| 6 | page | character varying | YES |  |
| 7 | area | numeric | YES |  |
| 8 | shape_star | numeric | YES |  |
| 9 | shape_stle | numeric | YES |  |
| 10 | source_file_id | bigint | NO |  |
| 11 | source_line_number | integer | NO |  |
| 12 | loaded_at | timestamp with time zone | NO |  |

## public.historical_auctions

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | integer | NO | nextval('historical_auctions_id_seq'::regclass) |
| 2 | listing_id | character varying | NO |  |
| 3 | case_number | character varying | YES |  |
| 4 | auction_date | date | YES |  |
| 5 | auction_status | character varying | YES |  |
| 6 | folio | character varying | YES |  |
| 7 | strap | character varying | YES |  |
| 8 | property_address | text | YES |  |
| 9 | winning_bid | numeric | YES |  |
| 10 | final_judgment_amount | numeric | YES |  |
| 11 | appraised_value | numeric | YES |  |
| 12 | previous_sale_price | numeric | YES |  |
| 13 | previous_sale_date | date | YES |  |
| 14 | latitude | double precision | YES |  |
| 15 | longitude | double precision | YES |  |
| 16 | photo_urls | jsonb | YES |  |
| 17 | bedrooms | numeric | YES |  |
| 18 | bathrooms | numeric | YES |  |
| 19 | sqft_total | integer | YES |  |
| 20 | year_built | integer | YES |  |
| 21 | sold_to | text | YES |  |
| 22 | buyer_type | character varying | YES |  |
| 23 | html_path | text | YES |  |
| 24 | created_at | timestamp with time zone | YES | now() |
| 25 | updated_at | timestamp with time zone | YES | now() |

## public.ingest_files

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('ingest_files_id_seq'::regclass) |
| 2 | source_system | character varying | NO |  |
| 3 | category | character varying | NO |  |
| 4 | relative_path | text | NO |  |
| 5 | file_sha256 | character varying | YES |  |
| 6 | file_size_bytes | bigint | YES |  |
| 7 | file_modified_at | timestamp with time zone | YES |  |
| 8 | discovered_at | timestamp with time zone | NO |  |
| 9 | loaded_at | timestamp with time zone | YES |  |
| 10 | loader_version | character varying | NO |  |
| 11 | status | character varying | NO |  |
| 12 | row_count | integer | YES |  |
| 13 | error_message | text | YES |  |

## public.ori_encumbrance_assignments

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('ori_encumbrance_assignments_id_seq'::regclass) |
| 2 | encumbrance_id | bigint | NO |  |
| 3 | instrument_number | character varying | YES |  |
| 4 | book | character varying | YES |  |
| 5 | page | character varying | YES |  |
| 6 | recording_date | date | YES |  |
| 7 | assignor | text | YES |  |
| 8 | assignee | text | YES |  |
| 9 | assignee_dmetaphone | text | YES |  |
| 10 | ori_uuid | character varying | YES |  |
| 11 | source_file_id | integer | YES |  |
| 12 | discovered_at | timestamp with time zone | NO | now() |

## public.ori_encumbrance_satisfactions

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('ori_encumbrance_satisfactions_id_seq'::regclass) |
| 2 | encumbrance_id | bigint | NO |  |
| 3 | satisfaction_id | bigint | NO |  |
| 4 | link_method | USER-DEFINED | NO |  |
| 5 | is_partial | boolean | NO | false |
| 6 | partial_amount | numeric | YES |  |
| 7 | notes | text | YES |  |
| 8 | linked_at | timestamp with time zone | NO | now() |

## public.ori_encumbrances

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('ori_encumbrances_id_seq'::regclass) |
| 2 | folio | character varying | NO |  |
| 3 | strap | character varying | YES |  |
| 4 | instrument_number | character varying | YES |  |
| 5 | book | character varying | YES |  |
| 6 | page | character varying | YES |  |
| 7 | book_type | character varying | YES | 'OR'::character varying |
| 8 | ori_uuid | character varying | YES |  |
| 9 | ori_id | character varying | YES |  |
| 10 | raw_document_type | text | YES |  |
| 11 | encumbrance_type | USER-DEFINED | NO | 'other'::encumbrance_type_enum |
| 12 | party1 | text | YES |  |
| 13 | party2 | text | YES |  |
| 14 | parties_one_json | jsonb | YES |  |
| 15 | parties_two_json | jsonb | YES |  |
| 16 | party1_dmetaphone | text | YES |  |
| 17 | party2_dmetaphone | text | YES |  |
| 18 | amount | numeric | YES |  |
| 19 | amount_confidence | character varying | YES | 'unknown'::character varying |
| 20 | amount_source | character varying | YES |  |
| 21 | recording_date | date | YES |  |
| 22 | effective_date | date | YES |  |
| 23 | case_number | character varying | YES |  |
| 24 | legal_description | text | YES |  |
| 25 | is_satisfied | boolean | NO | false |
| 26 | satisfaction_date | date | YES |  |
| 27 | satisfaction_instrument | character varying | YES |  |
| 28 | satisfaction_book | character varying | YES |  |
| 29 | satisfaction_page | character varying | YES |  |
| 30 | satisfaction_method | USER-DEFINED | YES |  |
| 31 | satisfies_encumbrance_id | bigint | YES |  |
| 32 | survival_status | character varying | YES |  |
| 33 | survival_reason | text | YES |  |
| 34 | survival_analyzed_at | timestamp with time zone | YES |  |
| 35 | survival_case_number | character varying | YES |  |
| 36 | current_holder | text | YES |  |
| 37 | assignment_count | integer | YES | 0 |
| 38 | mrta_expiration_date | date | YES |  |
| 39 | source_file_id | integer | YES |  |
| 40 | discovered_at | timestamp with time zone | NO | now() |
| 41 | updated_at | timestamp with time zone | NO | now() |

## public.property_market

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | strap | character varying | NO |  |
| 2 | folio | character varying | YES |  |
| 3 | case_number | character varying | YES |  |
| 4 | zestimate | numeric | YES |  |
| 5 | rent_zestimate | numeric | YES |  |
| 6 | list_price | numeric | YES |  |
| 7 | tax_assessed_value | numeric | YES |  |
| 8 | beds | integer | YES |  |
| 9 | baths | numeric | YES |  |
| 10 | sqft | integer | YES |  |
| 11 | year_built | integer | YES |  |
| 12 | lot_size | text | YES |  |
| 13 | property_type | character varying | YES |  |
| 14 | listing_status | character varying | YES |  |
| 15 | detail_url | text | YES |  |
| 16 | photo_local_paths | jsonb | YES | '[]'::jsonb |
| 17 | photo_cdn_urls | jsonb | YES | '[]'::jsonb |
| 18 | zillow_json | jsonb | YES |  |
| 19 | redfin_json | jsonb | YES |  |
| 20 | homeharvest_json | jsonb | YES |  |
| 21 | primary_source | character varying | YES |  |
| 22 | created_at | timestamp with time zone | NO |  |
| 23 | updated_at | timestamp with time zone | NO |  |

## public.sunbiz_flr_events

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('sunbiz_flr_events_id_seq'::regclass) |
| 2 | event_doc_number | character varying | YES |  |
| 3 | event_orig_doc_number | character varying | YES |  |
| 4 | event_action_count | integer | YES |  |
| 5 | event_sequence_number | integer | YES |  |
| 6 | event_pages | integer | YES |  |
| 7 | event_date | date | YES |  |
| 8 | action_sequence_number | integer | YES |  |
| 9 | action_code | character varying | YES |  |
| 10 | action_verbage | text | YES |  |
| 11 | action_name | text | YES |  |
| 12 | action_address1 | text | YES |  |
| 13 | action_address2 | text | YES |  |
| 14 | action_city | text | YES |  |
| 15 | action_state | character varying | YES |  |
| 16 | action_zip | character varying | YES |  |
| 17 | action_country | character varying | YES |  |
| 18 | action_old_name_seq | integer | YES |  |
| 19 | action_new_name_seq | integer | YES |  |
| 20 | action_name_type | character varying | YES |  |
| 21 | source_file_id | bigint | NO |  |
| 22 | source_member | text | NO |  |
| 23 | source_line_number | integer | NO |  |
| 24 | loaded_at | timestamp with time zone | NO |  |

## public.sunbiz_flr_filings

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | doc_number | character varying | NO |  |
| 2 | filing_date | date | YES |  |
| 3 | pages | integer | YES |  |
| 4 | total_pages | integer | YES |  |
| 5 | filing_status | character varying | YES |  |
| 6 | filing_type | character varying | YES |  |
| 7 | assessment_date | date | YES |  |
| 8 | cancellation_date | date | YES |  |
| 9 | expiration_date | date | YES |  |
| 10 | trans_utility | boolean | YES |  |
| 11 | filing_event_count | integer | YES |  |
| 12 | total_debtor_count | integer | YES |  |
| 13 | total_secured_count | integer | YES |  |
| 14 | current_debtor_count | integer | YES |  |
| 15 | current_secured_count | integer | YES |  |
| 16 | source_file_id | bigint | NO |  |
| 17 | source_member | text | NO |  |
| 18 | source_line_number | integer | NO |  |
| 19 | updated_at | timestamp with time zone | NO |  |

## public.sunbiz_flr_parties

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('sunbiz_flr_parties_id_seq'::regclass) |
| 2 | doc_number | character varying | NO |  |
| 3 | party_role | character varying | NO |  |
| 4 | filing_type | character varying | YES |  |
| 5 | name | text | YES |  |
| 6 | name_format | character varying | YES |  |
| 7 | address1 | text | YES |  |
| 8 | address2 | text | YES |  |
| 9 | city | text | YES |  |
| 10 | state | character varying | YES |  |
| 11 | zip_code | character varying | YES |  |
| 12 | country | character varying | YES |  |
| 13 | sequence_number | integer | YES |  |
| 14 | relation_to_filing | character varying | YES |  |
| 15 | original_party | character varying | YES |  |
| 16 | filing_status | character varying | YES |  |
| 17 | source_file_id | bigint | NO |  |
| 18 | source_member | text | NO |  |
| 19 | source_line_number | integer | NO |  |
| 20 | loaded_at | timestamp with time zone | NO |  |

## public.sunbiz_raw_records

| # | Column | Type | Nullable | Default |
|---:|---|---|---|---|
| 1 | id | bigint | NO | nextval('sunbiz_raw_records_id_seq'::regclass) |
| 2 | file_id | bigint | NO |  |
| 3 | source_member | text | NO |  |
| 4 | line_number | integer | NO |  |
| 5 | record_type | character varying | YES |  |
| 6 | doc_number | character varying | YES |  |
| 7 | raw_line | text | NO |  |
| 8 | loaded_at | timestamp with time zone | NO |  |
