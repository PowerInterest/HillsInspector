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

