"""
Create v2 database with Step4v2 schema enhancements.

This script:
1. Creates property_master_v2.db with full schema
2. Migrates data from v1 (auctions, parcels, bulk_parcels, sales_history, permits, market_data, home_harvest)
3. Leaves chain/documents/encumbrances empty for Step4v2 to rebuild

Usage:
    uv run python -m src.db.migrations.create_v2_database
"""

import duckdb
from pathlib import Path
from loguru import logger

from src.utils.time import ensure_duckdb_utc

V1_DB_PATH = "data/property_master.db"
V2_DB_PATH = "data/property_master_v2.db"


def create_sequences(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all sequences for auto-increment IDs."""
    sequences = [
        "seq_auctions_id",
        "seq_liens_id",
        "seq_permits_id",
        "seq_documents_id",
        "seq_analysis_id",
        "liens_id_seq",
        "sales_history_seq",
        "chain_of_title_seq",
        "encumbrances_seq",
        "legal_variations_seq",
        "market_data_id_seq",
        "homeharvest_id_seq",
        "scraper_outputs_id_seq",
        # New Step4v2 sequences
        "property_parties_seq",
        "linked_identities_seq",
        "ori_search_queue_seq",
    ]
    for seq in sequences:
        conn.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq} START 1")


def create_existing_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all existing tables (from v1 schema)."""

    # parcels table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS parcels (
            folio VARCHAR PRIMARY KEY,
            parcel_id VARCHAR,
            owner_name VARCHAR,
            property_address VARCHAR,
            city VARCHAR,
            zip_code VARCHAR,
            land_use VARCHAR,
            year_built INTEGER,
            beds FLOAT,
            baths FLOAT,
            heated_area FLOAT,
            lot_size FLOAT,
            assessed_value FLOAT,
            market_value FLOAT,
            last_sale_date DATE,
            last_sale_price FLOAT,
            image_url VARCHAR,
            market_analysis_content VARCHAR,
            legal_description VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            tax_status VARCHAR,
            tax_warrant BOOLEAN,
            last_analyzed_case_number VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # auctions table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS auctions (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_auctions_id'),
            case_number VARCHAR UNIQUE,
            folio VARCHAR,
            parcel_id VARCHAR,
            certificate_number VARCHAR,
            auction_type VARCHAR,
            auction_date DATE,
            property_address VARCHAR,
            assessed_value FLOAT,
            final_judgment_amount FLOAT,
            opening_bid FLOAT,
            plaintiff_max_bid VARCHAR,
            lien_position VARCHAR,
            est_surviving_debt FLOAT,
            is_toxic_title BOOLEAN DEFAULT FALSE,
            final_judgment_content VARCHAR,
            plaintiff VARCHAR,
            defendant VARCHAR,
            foreclosure_type VARCHAR,
            judgment_date DATE,
            lis_pendens_date DATE,
            foreclosure_sale_date DATE,
            total_judgment_amount FLOAT,
            principal_amount FLOAT,
            interest_amount FLOAT,
            attorney_fees FLOAT,
            court_costs FLOAT,
            original_mortgage_amount FLOAT,
            original_mortgage_date DATE,
            monthly_payment FLOAT,
            default_date DATE,
            extracted_judgment_data JSON,
            raw_judgment_text VARCHAR,
            judgment_extracted_at TIMESTAMP,
            status VARCHAR DEFAULT 'PENDING',
            needs_judgment_extraction BOOLEAN DEFAULT TRUE,
            needs_hcpa_enrichment BOOLEAN DEFAULT TRUE,
            needs_ori_ingestion BOOLEAN DEFAULT TRUE,
            needs_lien_survival BOOLEAN DEFAULT TRUE,
            needs_sunbiz_search BOOLEAN DEFAULT TRUE,
            needs_permit_check BOOLEAN DEFAULT TRUE,
            needs_flood_check BOOLEAN DEFAULT TRUE,
            needs_market_data BOOLEAN DEFAULT TRUE,
            needs_tax_check BOOLEAN DEFAULT TRUE,
            needs_homeharvest_enrichment BOOLEAN DEFAULT TRUE,
            hcpa_scrape_failed BOOLEAN DEFAULT FALSE,
            hcpa_scrape_error VARCHAR,
            has_valid_parcel_id BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # bulk_parcels table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bulk_parcels (
            folio VARCHAR PRIMARY KEY,
            pin VARCHAR,
            strap VARCHAR,
            owner_name VARCHAR,
            property_address VARCHAR,
            city VARCHAR,
            zip_code VARCHAR,
            land_use VARCHAR,
            land_use_desc VARCHAR,
            year_built INTEGER,
            beds FLOAT,
            baths FLOAT,
            stories FLOAT,
            units INTEGER,
            buildings INTEGER,
            heated_area FLOAT,
            lot_size FLOAT,
            assessed_value FLOAT,
            market_value FLOAT,
            just_value FLOAT,
            land_value FLOAT,
            building_value FLOAT,
            extra_features_value FLOAT,
            taxable_value FLOAT,
            last_sale_date DATE,
            last_sale_price FLOAT,
            raw_type VARCHAR,
            raw_sub VARCHAR,
            raw_taxdist VARCHAR,
            raw_muni VARCHAR,
            raw_legal1 VARCHAR,
            raw_legal2 VARCHAR,
            raw_legal3 VARCHAR,
            raw_legal4 VARCHAR,
            ingest_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # liens table (legacy)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS liens (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_liens_id'),
            folio VARCHAR,
            case_number VARCHAR,
            recording_date DATE,
            document_type VARCHAR,
            book VARCHAR,
            page VARCHAR,
            amount FLOAT,
            grantor VARCHAR,
            grantee VARCHAR,
            description TEXT,
            instrument_number VARCHAR,
            survives_foreclosure BOOLEAN,
            is_surviving BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # permits table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS permits (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_permits_id'),
            folio VARCHAR,
            permit_number VARCHAR UNIQUE,
            issue_date DATE,
            status VARCHAR,
            permit_type VARCHAR,
            description TEXT,
            contractor VARCHAR,
            estimated_cost FLOAT,
            url VARCHAR,
            noc_instrument VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # documents table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_documents_id'),
            folio VARCHAR,
            case_number VARCHAR,
            document_type VARCHAR,
            file_path VARCHAR,
            ocr_text TEXT,
            extracted_data JSON,
            recording_date DATE,
            book VARCHAR,
            page VARCHAR,
            instrument_number VARCHAR,
            party1 VARCHAR,
            party2 VARCHAR,
            legal_description VARCHAR,
            sales_price FLOAT,
            page_count INTEGER,
            ori_uuid VARCHAR,
            ori_id VARCHAR,
            book_type VARCHAR,
            party2_resolution_method VARCHAR,
            is_self_transfer BOOLEAN DEFAULT FALSE,
            self_transfer_type VARCHAR,
            party2_confidence FLOAT DEFAULT 1.0,
            party2_resolved_at TIMESTAMP,
            -- New Step4v2 fields
            triggered_by_search_id INTEGER,
            parties_one JSON,
            parties_two JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # analysis_results table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analysis_results (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_analysis_id'),
            folio VARCHAR,
            case_number VARCHAR,
            market_value FLOAT,
            realtor_estimate FLOAT,
            zillow_estimate FLOAT,
            rehab_cost FLOAT,
            surviving_liens_total FLOAT,
            auction_bid FLOAT,
            net_equity FLOAT,
            roi_percentage FLOAT,
            risk_score FLOAT,
            has_hoa_lien BOOLEAN DEFAULT FALSE,
            has_surviving_mortgage BOOLEAN DEFAULT FALSE,
            has_code_violations BOOLEAN DEFAULT FALSE,
            has_tax_certificate BOOLEAN DEFAULT FALSE,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # sales_history table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sales_history (
            id INTEGER PRIMARY KEY DEFAULT nextval('sales_history_seq'),
            folio VARCHAR,
            strap VARCHAR,
            book VARCHAR,
            page VARCHAR,
            instrument VARCHAR,
            sale_date VARCHAR,
            doc_type VARCHAR,
            qualified VARCHAR,
            vacant_improved VARCHAR,
            sale_price FLOAT,
            ori_link VARCHAR,
            pdf_path VARCHAR,
            grantor VARCHAR,
            grantee VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(folio, book, page)
        )
    """)

    # chain_of_title table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chain_of_title (
            id INTEGER PRIMARY KEY DEFAULT nextval('chain_of_title_seq'),
            folio VARCHAR,
            owner_name VARCHAR,
            acquired_from VARCHAR,
            acquisition_date DATE,
            disposition_date DATE,
            acquisition_instrument VARCHAR,
            acquisition_doc_type VARCHAR,
            acquisition_price FLOAT,
            link_status VARCHAR,
            confidence_score FLOAT,
            mrta_status VARCHAR,
            years_covered FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # encumbrances table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS encumbrances (
            id INTEGER PRIMARY KEY DEFAULT nextval('encumbrances_seq'),
            folio VARCHAR,
            chain_period_id INTEGER,
            encumbrance_type VARCHAR,
            creditor VARCHAR,
            debtor VARCHAR,
            amount FLOAT,
            amount_confidence VARCHAR,
            amount_flags VARCHAR,
            recording_date DATE,
            instrument VARCHAR,
            book VARCHAR,
            page VARCHAR,
            is_satisfied BOOLEAN DEFAULT FALSE,
            satisfaction_instrument VARCHAR,
            satisfaction_date DATE,
            survival_status VARCHAR,
            survival_reason VARCHAR,
            party2_resolution_method VARCHAR,
            is_self_transfer BOOLEAN DEFAULT FALSE,
            self_transfer_type VARCHAR,
            is_joined BOOLEAN DEFAULT FALSE,
            is_inferred BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # market_data table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY DEFAULT nextval('market_data_id_seq'),
            folio VARCHAR,
            source VARCHAR,
            capture_date DATE,
            listing_status VARCHAR,
            list_price FLOAT,
            zestimate FLOAT,
            rent_estimate FLOAT,
            hoa_monthly FLOAT,
            days_on_market INTEGER,
            price_history VARCHAR,
            raw_json VARCHAR,
            screenshot_path VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # home_harvest table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS home_harvest (
            id BIGINT PRIMARY KEY DEFAULT nextval('homeharvest_id_seq'),
            folio VARCHAR,
            property_url VARCHAR,
            property_id VARCHAR,
            listing_id VARCHAR,
            mls VARCHAR,
            mls_id VARCHAR,
            mls_status VARCHAR,
            status VARCHAR,
            permalink VARCHAR,
            street VARCHAR,
            unit VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip_code VARCHAR,
            formatted_address VARCHAR,
            style VARCHAR,
            beds DOUBLE,
            full_baths DOUBLE,
            half_baths DOUBLE,
            sqft DOUBLE,
            year_built INTEGER,
            stories DOUBLE,
            garage DOUBLE,
            lot_sqft DOUBLE,
            text_description VARCHAR,
            property_type VARCHAR,
            days_on_mls INTEGER,
            list_price DOUBLE,
            list_price_min DOUBLE,
            list_price_max DOUBLE,
            list_date TIMESTAMP,
            pending_date TIMESTAMP,
            sold_price DOUBLE,
            last_sold_date TIMESTAMP,
            last_status_change_date TIMESTAMP,
            last_update_date TIMESTAMP,
            last_sold_price DOUBLE,
            price_per_sqft DOUBLE,
            new_construction BOOLEAN,
            hoa_fee DOUBLE,
            monthly_fees JSON,
            one_time_fees JSON,
            estimated_value DOUBLE,
            tax_assessed_value DOUBLE,
            tax_history JSON,
            latitude DOUBLE,
            longitude DOUBLE,
            neighborhoods VARCHAR,
            county VARCHAR,
            fips_code VARCHAR,
            parcel_number VARCHAR,
            nearby_schools JSON,
            agent_uuid VARCHAR,
            agent_name VARCHAR,
            agent_email VARCHAR,
            agent_phone JSON,
            agent_state_license VARCHAR,
            broker_uuid VARCHAR,
            broker_name VARCHAR,
            office_uuid VARCHAR,
            office_name VARCHAR,
            office_email VARCHAR,
            office_phones JSON,
            estimated_monthly_rental DOUBLE,
            tags JSON,
            flags JSON,
            photos JSON,
            primary_photo VARCHAR,
            alt_photos JSON,
            open_houses JSON,
            units JSON,
            pet_policy VARCHAR,
            parking VARCHAR,
            terms VARCHAR,
            current_estimates JSON,
            estimates JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # scraper_outputs table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scraper_outputs (
            id INTEGER PRIMARY KEY DEFAULT nextval('scraper_outputs_id_seq'),
            property_id VARCHAR NOT NULL,
            scraper VARCHAR NOT NULL,
            scraped_at TIMESTAMP,
            processed_at TIMESTAMP,
            screenshot_path VARCHAR,
            vision_output_path VARCHAR,
            raw_data_path VARCHAR,
            source_url VARCHAR,
            prompt_version VARCHAR,
            extraction_success BOOLEAN DEFAULT FALSE,
            error_message VARCHAR,
            extracted_summary VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # status table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS status (
            case_number VARCHAR PRIMARY KEY,
            parcel_id VARCHAR,
            auction_date DATE,
            auction_type VARCHAR,
            step_auction_scraped TIMESTAMP,
            step_pdf_downloaded TIMESTAMP,
            step_judgment_extracted TIMESTAMP,
            step_bulk_enriched TIMESTAMP,
            step_homeharvest_enriched TIMESTAMP,
            step_hcpa_enriched TIMESTAMP,
            step_ori_ingested TIMESTAMP,
            step_survival_analyzed TIMESTAMP,
            step_permits_checked TIMESTAMP,
            step_flood_checked TIMESTAMP,
            step_market_fetched TIMESTAMP,
            step_tax_checked TIMESTAMP,
            current_step INTEGER DEFAULT 0,
            pipeline_status VARCHAR DEFAULT 'pending',
            last_error VARCHAR,
            error_step INTEGER,
            retry_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP
        )
    """)


def create_step4v2_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create new Step4v2 tables for iterative discovery."""

    # Enhanced legal_variations table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legal_variations (
            id INTEGER PRIMARY KEY DEFAULT nextval('legal_variations_seq'),
            folio VARCHAR NOT NULL,
            variation_text VARCHAR NOT NULL,
            source_instrument VARCHAR,
            source_type VARCHAR NOT NULL,
            is_canonical BOOLEAN DEFAULT FALSE,
            priority INTEGER DEFAULT 99,
            search_attempted BOOLEAN DEFAULT FALSE,
            search_operator VARCHAR,
            search_result_count INTEGER,
            last_searched_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(folio, variation_text)
        )
    """)

    # property_parties table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS property_parties (
            id INTEGER PRIMARY KEY DEFAULT nextval('property_parties_seq'),
            folio VARCHAR NOT NULL,
            party_name VARCHAR NOT NULL,
            party_name_normalized VARCHAR,
            party_role VARCHAR,
            linked_identity_id INTEGER,
            active_from DATE,
            active_to DATE,
            source_instrument VARCHAR,
            source_document_type VARCHAR,
            recording_date DATE,
            search_attempted BOOLEAN DEFAULT FALSE,
            search_result_count INTEGER,
            last_searched_at TIMESTAMP,
            is_generic BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(folio, party_name, source_instrument)
        )
    """)

    # linked_identities table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS linked_identities (
            id INTEGER PRIMARY KEY DEFAULT nextval('linked_identities_seq'),
            canonical_name VARCHAR NOT NULL,
            entity_type VARCHAR,
            link_type VARCHAR,
            confidence FLOAT DEFAULT 1.0,
            sunbiz_doc_number VARCHAR,
            sunbiz_status VARCHAR,
            notes VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ori_search_queue table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ori_search_queue (
            id INTEGER PRIMARY KEY DEFAULT nextval('ori_search_queue_seq'),
            folio VARCHAR NOT NULL,
            search_type VARCHAR NOT NULL,
            search_term VARCHAR NOT NULL,
            search_operator VARCHAR DEFAULT '',
            priority INTEGER DEFAULT 50,
            status VARCHAR DEFAULT 'pending',
            attempt_count INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 3,
            date_from DATE,
            date_to DATE,
            triggered_by_instrument VARCHAR,
            triggered_by_search_id INTEGER,
            result_count INTEGER,
            new_documents_found INTEGER,
            error_message VARCHAR,
            queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            next_retry_at TIMESTAMP,
            UNIQUE(folio, search_type, search_term, search_operator)
        )
    """)


def create_indices(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all indices for fast lookups."""
    indices = [
        # Existing indices
        ("idx_parcels_owner", "parcels(owner_name)"),
        ("idx_parcels_parcel_id", "parcels(parcel_id)"),
        ("idx_auctions_folio", "auctions(folio)"),
        ("idx_auctions_date", "auctions(auction_date)"),
        ("idx_auctions_type", "auctions(auction_type)"),
        ("idx_auctions_status", "auctions(status)"),
        ("idx_liens_folio", "liens(folio)"),
        ("idx_liens_case", "liens(case_number)"),
        ("idx_liens_date", "liens(recording_date)"),
        ("idx_liens_survives", "liens(survives_foreclosure)"),
        ("idx_permits_folio", "permits(folio)"),
        ("idx_documents_folio", "documents(folio)"),
        ("idx_documents_case", "documents(case_number)"),
        ("idx_documents_instrument", "documents(instrument_number)"),
        ("idx_analysis_folio", "analysis_results(folio)"),
        ("idx_analysis_case", "analysis_results(case_number)"),
        ("idx_sales_history_folio", "sales_history(folio)"),
        ("idx_sales_history_strap", "sales_history(strap)"),
        ("idx_homeharvest_folio", "home_harvest(folio)"),
        ("idx_homeharvest_address", "home_harvest(formatted_address)"),
        ("idx_status_auction_date", "status(auction_date)"),
        ("idx_status_pipeline_status", "status(pipeline_status)"),
        ("idx_status_parcel", "status(parcel_id)"),
        ("idx_bulk_parcels_strap", "bulk_parcels(strap)"),
        # New Step4v2 indices
        ("idx_legal_variations_folio", "legal_variations(folio)"),
        ("idx_legal_variations_priority", "legal_variations(folio, priority)"),
        ("idx_property_parties_folio", "property_parties(folio)"),
        ("idx_property_parties_linked", "property_parties(linked_identity_id)"),
        ("idx_property_parties_dates", "property_parties(folio, active_from, active_to)"),
        ("idx_linked_identities_canonical", "linked_identities(canonical_name)"),
        ("idx_search_queue_status", "ori_search_queue(status, priority)"),
        ("idx_search_queue_folio", "ori_search_queue(folio)"),
    ]

    for name, definition in indices:
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {definition}")
        except Exception as e:
            logger.warning(f"Could not create index {name}: {e}")


def migrate_data(v1_conn: duckdb.DuckDBPyConnection, v2_conn: duckdb.DuckDBPyConnection) -> None:
    """Migrate data from v1 to v2 database."""

    # Tables to migrate (Step4v2 will rebuild chain/documents/encumbrances)
    tables_to_migrate = [
        "auctions",
        "parcels",
        "bulk_parcels",
        "sales_history",
        "permits",
        "market_data",
        "home_harvest",
        "scraper_outputs",
        "status",
    ]

    for table in tables_to_migrate:
        try:
            # Check if table exists in v1
            check = v1_conn.execute(
                f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' LIMIT 1"
            ).fetchone()
            if not check:
                logger.info(f"Table {table} does not exist in v1, skipping")
                continue

            # Get count
            count = v1_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if count == 0:
                logger.info(f"Table {table} is empty, skipping")
                continue

            # Get column names from v2 schema
            v2_cols = v2_conn.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
            ).fetchall()
            v2_col_names = {col[0] for col in v2_cols}

            # Get column names from v1 data
            v1_cols = v1_conn.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
            ).fetchall()
            v1_col_names = [col[0] for col in v1_cols]

            # Only select columns that exist in both
            common_cols = [c for c in v1_col_names if c in v2_col_names]
            cols_str = ", ".join(common_cols)

            # Export to parquet and import
            logger.info(f"Migrating {table} ({count} rows, {len(common_cols)} columns)...")

            # Use parquet as intermediate format for reliability
            parquet_path = f"/tmp/{table}_migration.parquet"  # noqa: S108
            v1_conn.execute(f"COPY (SELECT {cols_str} FROM {table}) TO '{parquet_path}' (FORMAT PARQUET)")

            # Import into v2
            v2_conn.execute(f"INSERT INTO {table} ({cols_str}) SELECT {cols_str} FROM '{parquet_path}'")

            # Verify
            v2_count = v2_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"  {table}: {v2_count} rows migrated")

            # Cleanup
            Path(parquet_path).unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"Error migrating {table}: {e}")
            raise


def update_sequences(conn: duckdb.DuckDBPyConnection) -> None:
    """Update sequences to start after max IDs in migrated data."""
    sequence_table_map = [
        ("seq_auctions_id", "auctions", "id"),
        ("seq_permits_id", "permits", "id"),
        ("market_data_id_seq", "market_data", "id"),
        ("homeharvest_id_seq", "home_harvest", "id"),
        ("sales_history_seq", "sales_history", "id"),
        ("scraper_outputs_id_seq", "scraper_outputs", "id"),
    ]

    for seq_name, table, col in sequence_table_map:
        try:
            max_id = conn.execute(f"SELECT COALESCE(MAX({col}), 0) FROM {table}").fetchone()[0]
            if max_id > 0:
                # DuckDB sequences need to be recreated to set new start value
                conn.execute(f"DROP SEQUENCE IF EXISTS {seq_name}")
                conn.execute(f"CREATE SEQUENCE {seq_name} START {max_id + 1}")
                logger.info(f"Updated {seq_name} to start at {max_id + 1}")
        except Exception as e:
            logger.warning(f"Could not update sequence {seq_name}: {e}")


def reset_step4_flags(conn: duckdb.DuckDBPyConnection) -> None:
    """Reset Step 4 flags so Step4v2 will process all properties."""
    conn.execute("""
        UPDATE auctions
        SET needs_ori_ingestion = TRUE,
            needs_lien_survival = TRUE
        WHERE folio IS NOT NULL
    """)
    count = conn.execute("SELECT COUNT(*) FROM auctions WHERE needs_ori_ingestion = TRUE").fetchone()[0]
    logger.info(f"Reset Step 4 flags for {count} auctions")


def create_v2_database() -> str:
    """Main function to create and populate v2 database."""
    logger.info("=" * 60)
    logger.info("Creating v2 database with Step4v2 schema")
    logger.info("=" * 60)

    # Check v1 exists
    if not Path(V1_DB_PATH).exists():
        raise FileNotFoundError(f"V1 database not found: {V1_DB_PATH}")

    # Remove existing v2 if present
    v2_path = Path(V2_DB_PATH)
    if v2_path.exists():
        logger.warning(f"Removing existing v2 database: {V2_DB_PATH}")
        v2_path.unlink()

    # Create v2 database
    logger.info(f"Creating v2 database: {V2_DB_PATH}")
    v2_conn = duckdb.connect(V2_DB_PATH)
    ensure_duckdb_utc(v2_conn)

    # Create schema
    logger.info("Creating sequences...")
    create_sequences(v2_conn)

    logger.info("Creating existing tables...")
    create_existing_tables(v2_conn)

    logger.info("Creating Step4v2 tables...")
    create_step4v2_tables(v2_conn)

    logger.info("Creating indices...")
    create_indices(v2_conn)

    # Migrate data from v1
    logger.info("Opening v1 database for migration...")
    v1_conn = duckdb.connect(V1_DB_PATH, read_only=True)
    ensure_duckdb_utc(v1_conn)

    logger.info("Migrating data...")
    migrate_data(v1_conn, v2_conn)

    v1_conn.close()

    # Update sequences
    logger.info("Updating sequences...")
    update_sequences(v2_conn)

    # Reset Step 4 flags
    logger.info("Resetting Step 4 flags...")
    reset_step4_flags(v2_conn)

    # Checkpoint
    v2_conn.execute("CHECKPOINT")

    # Print summary
    logger.info("=" * 60)
    logger.info("V2 Database Summary")
    logger.info("=" * 60)

    tables = v2_conn.execute("SHOW TABLES").fetchall()
    for (table,) in tables:
        count = v2_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info(f"  {table}: {count} rows")

    v2_conn.close()

    logger.info("=" * 60)
    logger.info(f"V2 database created successfully: {V2_DB_PATH}")
    logger.info("=" * 60)

    return V2_DB_PATH


if __name__ == "__main__":
    create_v2_database()
