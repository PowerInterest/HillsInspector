# Database Schema

**Database File:** `data/property_master.db`

## Tables

- [analysis_results](#analysis_results)
- [auctions](#auctions)
- [bulk_parcels](#bulk_parcels)
- [chain_of_title](#chain_of_title)
- [documents](#documents)
- [encumbrances](#encumbrances)
- [home_harvest](#home_harvest)
- [legal_variations](#legal_variations)
- [liens](#liens)
- [market_data](#market_data)
- [parcels](#parcels)
- [permits](#permits)
- [sales_history](#sales_history)
- [scraper_outputs](#scraper_outputs)

### analysis_results

Final analysis results combining data from all pipeline steps.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('seq_analysis_id') |
| `folio` | `VARCHAR` | YES |  |  |
| `case_number` | `VARCHAR` | YES |  |  |
| `market_value` | `FLOAT` | YES |  |  |
| `realtor_estimate` | `FLOAT` | YES |  |  |
| `zillow_estimate` | `FLOAT` | YES |  |  |
| `rehab_cost` | `FLOAT` | YES |  |  |
| `surviving_liens_total` | `FLOAT` | YES |  |  |
| `auction_bid` | `FLOAT` | YES |  |  |
| `net_equity` | `FLOAT` | YES |  |  |
| `roi_percentage` | `FLOAT` | YES |  |  |
| `risk_score` | `FLOAT` | YES |  |  |
| `has_hoa_lien` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `has_surviving_mortgage` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `has_code_violations` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `has_tax_certificate` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `analyzed_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### auctions

Foreclosure and tax deed auction listings with pipeline step tracking flags.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('seq_auctions_id') |
| `case_number` | `VARCHAR` | YES | UNI |  |
| `folio` | `VARCHAR` | YES |  |  |
| `parcel_id` | `VARCHAR` | YES |  |  |
| `certificate_number` | `VARCHAR` | YES |  |  |
| `auction_type` | `VARCHAR` | YES |  |  |
| `auction_date` | `DATE` | YES |  |  |
| `property_address` | `VARCHAR` | YES |  |  |
| `assessed_value` | `FLOAT` | YES |  |  |
| `final_judgment_amount` | `FLOAT` | YES |  |  |
| `opening_bid` | `FLOAT` | YES |  |  |
| `plaintiff_max_bid` | `VARCHAR` | YES |  |  |
| `lien_position` | `VARCHAR` | YES |  |  |
| `est_surviving_debt` | `FLOAT` | YES |  |  |
| `is_toxic_title` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `final_judgment_content` | `VARCHAR` | YES |  |  |
| `plaintiff` | `VARCHAR` | YES |  |  |
| `defendant` | `VARCHAR` | YES |  |  |
| `foreclosure_type` | `VARCHAR` | YES |  |  |
| `judgment_date` | `DATE` | YES |  |  |
| `lis_pendens_date` | `DATE` | YES |  |  |
| `foreclosure_sale_date` | `DATE` | YES |  |  |
| `total_judgment_amount` | `FLOAT` | YES |  |  |
| `principal_amount` | `FLOAT` | YES |  |  |
| `interest_amount` | `FLOAT` | YES |  |  |
| `attorney_fees` | `FLOAT` | YES |  |  |
| `court_costs` | `FLOAT` | YES |  |  |
| `original_mortgage_amount` | `FLOAT` | YES |  |  |
| `original_mortgage_date` | `DATE` | YES |  |  |
| `monthly_payment` | `FLOAT` | YES |  |  |
| `default_date` | `DATE` | YES |  |  |
| `extracted_judgment_data` | `JSON` | YES |  |  |
| `raw_judgment_text` | `VARCHAR` | YES |  |  |
| `judgment_extracted_at` | `TIMESTAMP` | YES |  |  |
| `status` | `VARCHAR` | YES |  | 'PENDING' |
| `needs_judgment_extraction` | `BOOLEAN` | YES |  | CAST('t' AS BOOLEAN) |
| `needs_hcpa_enrichment` | `BOOLEAN` | YES |  | CAST('t' AS BOOLEAN) |
| `needs_ori_ingestion` | `BOOLEAN` | YES |  | CAST('t' AS BOOLEAN) |
| `needs_lien_survival` | `BOOLEAN` | YES |  | CAST('t' AS BOOLEAN) |
| `needs_sunbiz_search` | `BOOLEAN` | YES |  | CAST('t' AS BOOLEAN) |
| `needs_permit_check` | `BOOLEAN` | YES |  | CAST('t' AS BOOLEAN) |
| `needs_flood_check` | `BOOLEAN` | YES |  | CAST('t' AS BOOLEAN) |
| `needs_market_data` | `BOOLEAN` | YES |  | CAST('t' AS BOOLEAN) |
| `needs_tax_check` | `BOOLEAN` | YES |  | CAST('t' AS BOOLEAN) |
| `needs_homeharvest_enrichment` | `BOOLEAN` | YES |  | CAST('t' AS BOOLEAN) |
| `hcpa_scrape_failed` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `hcpa_scrape_error` | `VARCHAR` | YES |  |  |

### bulk_parcels

HCPA parcel data loaded from bulk export files. Join to auctions via `strap = folio`.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `folio` | `VARCHAR` | NO | PRI |  |
| `pin` | `VARCHAR` | YES |  |  |
| `strap` | `VARCHAR` | YES |  |  |
| `owner_name` | `VARCHAR` | YES |  |  |
| `property_address` | `VARCHAR` | YES |  |  |
| `city` | `VARCHAR` | YES |  |  |
| `zip_code` | `VARCHAR` | YES |  |  |
| `land_use` | `VARCHAR` | YES |  |  |
| `land_use_desc` | `VARCHAR` | YES |  |  |
| `year_built` | `INTEGER` | YES |  |  |
| `beds` | `FLOAT` | YES |  |  |
| `baths` | `FLOAT` | YES |  |  |
| `stories` | `FLOAT` | YES |  |  |
| `units` | `INTEGER` | YES |  |  |
| `buildings` | `INTEGER` | YES |  |  |
| `heated_area` | `FLOAT` | YES |  |  |
| `lot_size` | `FLOAT` | YES |  |  |
| `assessed_value` | `FLOAT` | YES |  |  |
| `market_value` | `FLOAT` | YES |  |  |
| `just_value` | `FLOAT` | YES |  |  |
| `land_value` | `FLOAT` | YES |  |  |
| `building_value` | `FLOAT` | YES |  |  |
| `extra_features_value` | `FLOAT` | YES |  |  |
| `taxable_value` | `FLOAT` | YES |  |  |
| `last_sale_date` | `DATE` | YES |  |  |
| `last_sale_price` | `FLOAT` | YES |  |  |
| `raw_type` | `VARCHAR` | YES |  |  |
| `raw_sub` | `VARCHAR` | YES |  |  |
| `raw_taxdist` | `VARCHAR` | YES |  |  |
| `raw_muni` | `VARCHAR` | YES |  |  |
| `raw_legal1` | `VARCHAR` | YES |  |  |
| `raw_legal2` | `VARCHAR` | YES |  |  |
| `raw_legal3` | `VARCHAR` | YES |  |  |
| `raw_legal4` | `VARCHAR` | YES |  |  |
| `ingest_date` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### chain_of_title

Ownership timeline derived from ORI documents by TitleChainService.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('chain_of_title_seq') |
| `folio` | `VARCHAR` | YES |  |  |
| `owner_name` | `VARCHAR` | YES |  |  |
| `acquired_from` | `VARCHAR` | YES |  |  |
| `acquisition_date` | `DATE` | YES |  |  |
| `disposition_date` | `DATE` | YES |  |  |
| `acquisition_instrument` | `VARCHAR` | YES |  |  |
| `acquisition_doc_type` | `VARCHAR` | YES |  |  |
| `acquisition_price` | `FLOAT` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `link_status` | `VARCHAR` | YES |  |  |
| `confidence_score` | `FLOAT` | YES |  |  |
| `mrta_status` | `VARCHAR` | YES |  |  |
| `years_covered` | `FLOAT` | YES |  |  |

### documents

ORI document metadata from clerk searches.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('seq_documents_id') |
| `folio` | `VARCHAR` | YES |  |  |
| `case_number` | `VARCHAR` | YES |  |  |
| `document_type` | `VARCHAR` | YES |  |  |
| `file_path` | `VARCHAR` | YES |  |  |
| `ocr_text` | `VARCHAR` | YES |  |  |
| `extracted_data` | `JSON` | YES |  |  |
| `recording_date` | `DATE` | YES |  |  |
| `book` | `VARCHAR` | YES |  |  |
| `page` | `VARCHAR` | YES |  |  |
| `instrument_number` | `VARCHAR` | YES |  |  |
| `party1` | `VARCHAR` | YES |  |  |
| `party2` | `VARCHAR` | YES |  |  |
| `legal_description` | `VARCHAR` | YES |  |  |
| `sales_price` | `FLOAT` | YES |  |  |
| `page_count` | `INTEGER` | YES |  |  |
| `ori_uuid` | `VARCHAR` | YES |  |  |
| `ori_id` | `VARCHAR` | YES |  |  |
| `book_type` | `VARCHAR` | YES |  |  |
| `party2_resolution_method` | `VARCHAR` | YES |  |  |
| `is_self_transfer` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `self_transfer_type` | `VARCHAR` | YES |  |  |
| `party2_confidence` | `FLOAT` | YES |  | 1.0 |
| `party2_resolved_at` | `TIMESTAMP` | YES |  |  |
| `triggered_by_search_id` | `INTEGER` | YES |  |  |
| `parties_one` | `TEXT` | YES |  |  |
| `parties_two` | `TEXT` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### encumbrances

Liens and mortgages with survival status from LienSurvivalAnalyzer.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('encumbrances_seq') |
| `folio` | `VARCHAR` | YES |  |  |
| `chain_period_id` | `INTEGER` | YES |  |  |
| `encumbrance_type` | `VARCHAR` | YES |  |  |
| `creditor` | `VARCHAR` | YES |  |  |
| `debtor` | `VARCHAR` | YES |  |  |
| `amount` | `FLOAT` | YES |  |  |
| `amount_confidence` | `VARCHAR` | YES |  |  |
| `amount_flags` | `VARCHAR` | YES |  |  |
| `recording_date` | `DATE` | YES |  |  |
| `instrument` | `VARCHAR` | YES |  |  |
| `book` | `VARCHAR` | YES |  |  |
| `page` | `VARCHAR` | YES |  |  |
| `is_satisfied` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `satisfaction_instrument` | `VARCHAR` | YES |  |  |
| `satisfaction_date` | `DATE` | YES |  |  |
| `survival_status` | `VARCHAR` | YES |  |  |
| `party2_resolution_method` | `VARCHAR` | YES |  |  |
| `is_self_transfer` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `self_transfer_type` | `VARCHAR` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `is_joined` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `is_inferred` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |

### home_harvest

MLS data and photos from HomeHarvest library (Realtor.com).

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `BIGINT` | NO | PRI | nextval('homeharvest_id_seq') |
| `folio` | `VARCHAR` | YES |  |  |
| `property_url` | `VARCHAR` | YES |  |  |
| `property_id` | `VARCHAR` | YES |  |  |
| `listing_id` | `VARCHAR` | YES |  |  |
| `mls` | `VARCHAR` | YES |  |  |
| `mls_id` | `VARCHAR` | YES |  |  |
| `mls_status` | `VARCHAR` | YES |  |  |
| `status` | `VARCHAR` | YES |  |  |
| `permalink` | `VARCHAR` | YES |  |  |
| `street` | `VARCHAR` | YES |  |  |
| `unit` | `VARCHAR` | YES |  |  |
| `city` | `VARCHAR` | YES |  |  |
| `state` | `VARCHAR` | YES |  |  |
| `zip_code` | `VARCHAR` | YES |  |  |
| `formatted_address` | `VARCHAR` | YES |  |  |
| `style` | `VARCHAR` | YES |  |  |
| `beds` | `DOUBLE` | YES |  |  |
| `full_baths` | `DOUBLE` | YES |  |  |
| `half_baths` | `DOUBLE` | YES |  |  |
| `sqft` | `DOUBLE` | YES |  |  |
| `year_built` | `INTEGER` | YES |  |  |
| `stories` | `DOUBLE` | YES |  |  |
| `garage` | `DOUBLE` | YES |  |  |
| `lot_sqft` | `DOUBLE` | YES |  |  |
| `text_description` | `VARCHAR` | YES |  |  |
| `property_type` | `VARCHAR` | YES |  |  |
| `days_on_mls` | `INTEGER` | YES |  |  |
| `list_price` | `DOUBLE` | YES |  |  |
| `list_price_min` | `DOUBLE` | YES |  |  |
| `list_price_max` | `DOUBLE` | YES |  |  |
| `list_date` | `TIMESTAMP` | YES |  |  |
| `pending_date` | `TIMESTAMP` | YES |  |  |
| `sold_price` | `DOUBLE` | YES |  |  |
| `last_sold_date` | `TIMESTAMP` | YES |  |  |
| `last_status_change_date` | `TIMESTAMP` | YES |  |  |
| `last_update_date` | `TIMESTAMP` | YES |  |  |
| `last_sold_price` | `DOUBLE` | YES |  |  |
| `price_per_sqft` | `DOUBLE` | YES |  |  |
| `new_construction` | `BOOLEAN` | YES |  |  |
| `hoa_fee` | `DOUBLE` | YES |  |  |
| `monthly_fees` | `JSON` | YES |  |  |
| `one_time_fees` | `JSON` | YES |  |  |
| `estimated_value` | `DOUBLE` | YES |  |  |
| `tax_assessed_value` | `DOUBLE` | YES |  |  |
| `tax_history` | `JSON` | YES |  |  |
| `latitude` | `DOUBLE` | YES |  |  |
| `longitude` | `DOUBLE` | YES |  |  |
| `neighborhoods` | `VARCHAR` | YES |  |  |
| `county` | `VARCHAR` | YES |  |  |
| `fips_code` | `VARCHAR` | YES |  |  |
| `parcel_number` | `VARCHAR` | YES |  |  |
| `nearby_schools` | `JSON` | YES |  |  |
| `agent_uuid` | `VARCHAR` | YES |  |  |
| `agent_name` | `VARCHAR` | YES |  |  |
| `agent_email` | `VARCHAR` | YES |  |  |
| `agent_phone` | `JSON` | YES |  |  |
| `agent_state_license` | `VARCHAR` | YES |  |  |
| `broker_uuid` | `VARCHAR` | YES |  |  |
| `broker_name` | `VARCHAR` | YES |  |  |
| `office_uuid` | `VARCHAR` | YES |  |  |
| `office_name` | `VARCHAR` | YES |  |  |
| `office_email` | `VARCHAR` | YES |  |  |
| `office_phones` | `JSON` | YES |  |  |
| `estimated_monthly_rental` | `DOUBLE` | YES |  |  |
| `tags` | `JSON` | YES |  |  |
| `flags` | `JSON` | YES |  |  |
| `photos` | `JSON` | YES |  |  |
| `primary_photo` | `VARCHAR` | YES |  |  |
| `alt_photos` | `JSON` | YES |  |  |
| `open_houses` | `JSON` | YES |  |  |
| `units` | `JSON` | YES |  |  |
| `pet_policy` | `VARCHAR` | YES |  |  |
| `parking` | `VARCHAR` | YES |  |  |
| `terms` | `VARCHAR` | YES |  |  |
| `current_estimates` | `JSON` | YES |  |  |
| `estimates` | `JSON` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### legal_variations (Legacy)
> **Note:** Part of the deprecated V2 ORI ingestion path. Preserved for data history but no longer populated.

Alternative legal description formats found in ORI documents.
| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('legal_variations_seq') |
| `folio` | `VARCHAR` | YES |  |  |
| `variation_text` | `VARCHAR` | YES |  |  |
| `source_instrument` | `VARCHAR` | YES |  |  |
| `source_type` | `VARCHAR` | YES |  |  |
| `is_canonical` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### property_parties (Legacy)
> **Note:** Part of the deprecated V2 ORI ingestion path. Preserved for data history but no longer populated.

### linked_identities (Legacy)
> **Note:** Part of the deprecated V2 ORI ingestion path. Preserved for data history but no longer populated.

### ori_search_queue (Legacy)
> **Note:** Part of the deprecated V2 ORI ingestion path. Preserved for data history but no longer populated.

### liens

Legacy liens table (predecessor to encumbrances). Still used for some lookups.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('seq_liens_id') |
| `folio` | `VARCHAR` | YES |  |  |
| `case_number` | `VARCHAR` | YES |  |  |
| `recording_date` | `DATE` | YES |  |  |
| `document_type` | `VARCHAR` | YES |  |  |
| `book` | `VARCHAR` | YES |  |  |
| `page` | `VARCHAR` | YES |  |  |
| `amount` | `FLOAT` | YES |  |  |
| `grantor` | `VARCHAR` | YES |  |  |
| `grantee` | `VARCHAR` | YES |  |  |
| `description` | `VARCHAR` | YES |  |  |
| `instrument_number` | `VARCHAR` | YES |  |  |
| `survives_foreclosure` | `BOOLEAN` | YES |  |  |
| `is_surviving` | `BOOLEAN` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### market_data

Zillow and Realtor.com market data from scrapers.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('market_data_id_seq') |
| `folio` | `VARCHAR` | YES |  |  |
| `source` | `VARCHAR` | YES |  |  |
| `capture_date` | `DATE` | YES |  |  |
| `listing_status` | `VARCHAR` | YES |  |  |
| `list_price` | `FLOAT` | YES |  |  |
| `zestimate` | `FLOAT` | YES |  |  |
| `rent_estimate` | `FLOAT` | YES |  |  |
| `hoa_monthly` | `FLOAT` | YES |  |  |
| `days_on_market` | `INTEGER` | YES |  |  |
| `price_history` | `VARCHAR` | YES |  |  |
| `raw_json` | `VARCHAR` | YES |  |  |
| `screenshot_path` | `VARCHAR` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### parcels

Property details from HCPA GIS scraper. Enriched during pipeline.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `folio` | `VARCHAR` | NO | PRI |  |
| `parcel_id` | `VARCHAR` | YES |  |  |
| `owner_name` | `VARCHAR` | YES |  |  |
| `property_address` | `VARCHAR` | YES |  |  |
| `city` | `VARCHAR` | YES |  |  |
| `zip_code` | `VARCHAR` | YES |  |  |
| `land_use` | `VARCHAR` | YES |  |  |
| `year_built` | `INTEGER` | YES |  |  |
| `beds` | `FLOAT` | YES |  |  |
| `baths` | `FLOAT` | YES |  |  |
| `heated_area` | `FLOAT` | YES |  |  |
| `lot_size` | `FLOAT` | YES |  |  |
| `assessed_value` | `FLOAT` | YES |  |  |
| `market_value` | `FLOAT` | YES |  |  |
| `last_sale_date` | `DATE` | YES |  |  |
| `last_sale_price` | `FLOAT` | YES |  |  |
| `image_url` | `VARCHAR` | YES |  |  |
| `market_analysis_content` | `VARCHAR` | YES |  |  |
| `latitude` | `DOUBLE` | YES |  |  |
| `longitude` | `DOUBLE` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `last_analyzed_case_number` | `VARCHAR` | YES |  |  |
| `legal_description` | `VARCHAR` | YES |  |  |
| `tax_status` | `VARCHAR` | YES |  |  |
| `judgment_legal_description` | `VARCHAR` | YES |  |  |
| `tax_warrant` | `BOOLEAN` | YES |  |  |

### permits

Building permits from Hillsborough County.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('seq_permits_id') |
| `folio` | `VARCHAR` | YES |  |  |
| `permit_number` | `VARCHAR` | YES | UNI |  |
| `issue_date` | `DATE` | YES |  |  |
| `status` | `VARCHAR` | YES |  |  |
| `permit_type` | `VARCHAR` | YES |  |  |
| `description` | `VARCHAR` | YES |  |  |
| `contractor` | `VARCHAR` | YES |  |  |
| `estimated_cost` | `FLOAT` | YES |  |  |
| `url` | `VARCHAR` | YES |  |  |
| `noc_instrument` | `VARCHAR` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### sales_history

Sales history from HCPA GIS scraper.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('sales_history_seq') |
| `folio` | `VARCHAR` | YES | UNI |  |
| `strap` | `VARCHAR` | YES |  |  |
| `book` | `VARCHAR` | YES | UNI |  |
| `page` | `VARCHAR` | YES | UNI |  |
| `instrument` | `VARCHAR` | YES |  |  |
| `sale_date` | `VARCHAR` | YES |  |  |
| `doc_type` | `VARCHAR` | YES |  |  |
| `qualified` | `VARCHAR` | YES |  |  |
| `vacant_improved` | `VARCHAR` | YES |  |  |
| `sale_price` | `FLOAT` | YES |  |  |
| `ori_link` | `VARCHAR` | YES |  |  |
| `pdf_path` | `VARCHAR` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `grantor` | `VARCHAR` | YES |  |  |
| `grantee` | `VARCHAR` | YES |  |  |

### scraper_outputs

Raw scraper output storage for debugging and reprocessing.

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('scraper_outputs_id_seq') |
| `property_id` | `VARCHAR` | NO |  |  |
| `scraper` | `VARCHAR` | NO |  |  |
| `scraped_at` | `TIMESTAMP` | YES |  |  |
| `processed_at` | `TIMESTAMP` | YES |  |  |
| `screenshot_path` | `VARCHAR` | YES |  |  |
| `vision_output_path` | `VARCHAR` | YES |  |  |
| `raw_data_path` | `VARCHAR` | YES |  |  |
| `source_url` | `VARCHAR` | YES |  |  |
| `prompt_version` | `VARCHAR` | YES |  |  |
| `extraction_success` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `error_message` | `VARCHAR` | YES |  |  |
| `extracted_summary` | `VARCHAR` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

## Key Relationships

```
auctions.folio ──────────────┬──────────────> bulk_parcels.strap
                             │
                             ├──────────────> parcels.folio
                             │
                             ├──────────────> chain_of_title.folio
                             │
                             ├──────────────> encumbrances.folio
                             │
                             ├──────────────> documents.folio
                             │
                             ├──────────────> home_harvest.folio
                             │
                             ├──────────────> market_data.folio
                             │
                             ├──────────────> sales_history.strap
                             │
                             └──────────────> permits.folio

auctions.case_number ───────> liens.case_number
```

## Pipeline Step Flags

The `auctions` table contains boolean flags to track pipeline progress:

| Flag | Pipeline Step | Description |
| :--- | :--- | :--- |
| `needs_judgment_extraction` | Step 2 | Final Judgment PDF extraction |
| `needs_hcpa_enrichment` | Step 3 | HCPA parcel data enrichment |
| `needs_homeharvest_enrichment` | Step 3.5 | HomeHarvest MLS/photos |
| `needs_ori_ingestion` | Phase 2 | ORI document search & chain |
| `needs_lien_survival` | Phase 3 | Lien survival analysis |
| `needs_sunbiz_search` | Phase 1 | Sunbiz entity lookup |
| `needs_permit_check` | Phase 1 | Building permits check |
| `needs_flood_check` | Phase 1 | FEMA flood zone check |
| `needs_market_data` | Phase 1 | Zillow/Realtor scraping |
| `needs_tax_check` | Phase 1 | Tax payment status |
| `hcpa_scrape_failed` | Error | HCPA scrape failed (needs review) |

## Survival Status Values

The `encumbrances.survival_status` field uses these values:

| Status | Description |
| :--- | :--- |
| `SURVIVED` | Lien survives foreclosure |
| `FORECLOSING` | This is the foreclosing lien |
| `SATISFIED` | Lien was satisfied/released |
| `EXPIRED` | Lien expired (statute of limitations) |
| `HISTORICAL` | Pre-dates foreclosing lien (wiped out) |
