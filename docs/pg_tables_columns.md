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
