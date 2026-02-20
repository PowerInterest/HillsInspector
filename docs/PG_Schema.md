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
