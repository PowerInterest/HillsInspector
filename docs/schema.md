# Database Schema

**Database File:** `data/property_master.db`

## Tables

- [analysis_results](#analysis_results)
- [auctions](#auctions)
- [bulk_parcels](#bulk_parcels)
- [documents](#documents)
- [dor_codes](#dor_codes)
- [liens](#liens)
- [parcels](#parcels)
- [permits](#permits)
- [properties](#properties)
- [property_sources](#property_sources)
- [sales_history](#sales_history)
- [scraper_outputs](#scraper_outputs)
- [subdivisions](#subdivisions)

### analysis_results

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
| `status` | `VARCHAR` | YES |  | 'PENDING' |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
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

### bulk_parcels

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
| `ingest_date` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `source_file` | `VARCHAR` | YES |  |  |

### documents

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('seq_documents_id') |
| `folio` | `VARCHAR` | YES |  |  |
| `case_number` | `VARCHAR` | YES |  |  |
| `document_type` | `VARCHAR` | YES |  |  |
| `file_path` | `VARCHAR` | YES |  |  |
| `ocr_text` | `VARCHAR` | YES |  |  |
| `extracted_data` | `JSON` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `recording_date` | `DATE` | YES |  |  |
| `book` | `VARCHAR` | YES |  |  |
| `page` | `VARCHAR` | YES |  |  |
| `instrument_number` | `VARCHAR` | YES |  |  |
| `party1` | `VARCHAR` | YES |  |  |
| `party2` | `VARCHAR` | YES |  |  |
| `legal_description` | `VARCHAR` | YES |  |  |
| `doc_date` | `DATE` | YES |  |  |

### dor_codes

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `dor_code` | `VARCHAR` | NO | PRI |  |
| `description` | `VARCHAR` | YES |  |  |
| `ingest_date` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### liens

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('liens_id_seq') |
| `case_number` | `VARCHAR` | YES |  |  |
| `document_type` | `VARCHAR` | YES |  |  |
| `recording_date` | `DATE` | YES |  |  |
| `amount` | `DECIMAL(12,2)` | YES |  |  |
| `grantor` | `VARCHAR` | YES |  |  |
| `grantee` | `VARCHAR` | YES |  |  |
| `book` | `VARCHAR` | YES |  |  |
| `page` | `VARCHAR` | YES |  |  |
| `instrument_number` | `VARCHAR` | YES |  |  |
| `is_surviving` | `BOOLEAN` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### parcels

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
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `market_analysis_content` | `VARCHAR` | YES |  |  |
| `image_url` | `VARCHAR` | YES |  |  |

### permits

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
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### properties

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `folio` | `VARCHAR` | NO | PRI |  |
| `address` | `VARCHAR` | YES |  |  |
| `owner` | `VARCHAR` | YES |  |  |
| `value` | `DOUBLE` | YES |  |  |
| `auction_date` | `DATE` | YES |  |  |
| `status` | `VARCHAR` | YES |  |  |

### property_sources

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | NO | PRI | nextval('property_sources_id_seq') |
| `folio` | `VARCHAR` | YES | UNI |  |
| `source_name` | `VARCHAR` | YES |  |  |
| `url` | `VARCHAR` | YES | UNI |  |
| `description` | `VARCHAR` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

### sales_history

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

### scraper_outputs

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
| `prompt_version` | `VARCHAR` | YES |  |  |
| `extraction_success` | `BOOLEAN` | YES |  | CAST('f' AS BOOLEAN) |
| `error_message` | `VARCHAR` | YES |  |  |
| `extracted_summary` | `VARCHAR` | YES |  |  |
| `created_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `updated_at` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |
| `source_url` | `VARCHAR` | YES |  |  |

### subdivisions

| Column | Type | Nullable | Key | Default |
| :--- | :--- | :--- | :--- | :--- |
| `sub_code` | `VARCHAR` | NO | PRI |  |
| `sub_name` | `VARCHAR` | YES |  |  |
| `plat_book` | `VARCHAR` | YES |  |  |
| `plat_page` | `VARCHAR` | YES |  |  |
| `ingest_date` | `TIMESTAMP` | YES |  | CURRENT_TIMESTAMP |

