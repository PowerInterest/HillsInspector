"""
SQLite Schema Creation Script

Creates the primary SQLite database with WAL mode.
Defines tables for auctions, parcels, documents, chain of title, and enrichment data.

Usage:
    uv run python -m src.db.migrations.create_sqlite_database
"""

import sqlite3
from pathlib import Path
from loguru import logger

from src.db.sqlite_paths import resolve_sqlite_db_path_str

DUCKDB_PATH = "data/property_master.db"
SQLITE_PATH = resolve_sqlite_db_path_str()


def setup_wal_mode(conn: sqlite3.Connection) -> None:
    """Configure SQLite for optimal concurrent write performance."""
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")  # 30s wait on locks
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    conn.execute("PRAGMA temp_store=MEMORY")
    logger.info("SQLite WAL mode enabled with optimized settings")


def create_tables(conn: sqlite3.Connection) -> None:
    """Create all tables with SQLite-compatible schema."""
    
    # parcels table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS parcels (
            folio TEXT PRIMARY KEY,
            parcel_id TEXT,
            owner_name TEXT,
            property_address TEXT,
            city TEXT,
            zip_code TEXT,
            land_use TEXT,
            year_built INTEGER,
            beds REAL,
            baths REAL,
            heated_area REAL,
            lot_size REAL,
            assessed_value REAL,
            market_value REAL,
            last_sale_date TEXT,
            last_sale_price REAL,
            image_url TEXT,
            market_analysis_content TEXT,
            legal_description TEXT,
            latitude REAL,
            longitude REAL,
            tax_status TEXT,
            tax_warrant INTEGER,
            last_analyzed_case_number TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # auctions table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS auctions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_number TEXT UNIQUE,
            folio TEXT,
            parcel_id TEXT,
            certificate_number TEXT,
            auction_type TEXT,
            auction_date TEXT,
            property_address TEXT,
            assessed_value REAL,
            final_judgment_amount REAL,
            opening_bid REAL,
            plaintiff_max_bid TEXT,
            lien_position TEXT,
            est_surviving_debt REAL,
            is_toxic_title INTEGER DEFAULT 0,
            final_judgment_content TEXT,
            plaintiff TEXT,
            defendant TEXT,
            foreclosure_type TEXT,
            judgment_date TEXT,
            lis_pendens_date TEXT,
            foreclosure_sale_date TEXT,
            total_judgment_amount REAL,
            principal_amount REAL,
            interest_amount REAL,
            attorney_fees REAL,
            court_costs REAL,
            original_mortgage_amount REAL,
            original_mortgage_date TEXT,
            monthly_payment REAL,
            default_date TEXT,
            extracted_judgment_data TEXT,
            raw_judgment_text TEXT,
            judgment_extracted_at TEXT,
            status TEXT DEFAULT 'PENDING',
            needs_judgment_extraction INTEGER DEFAULT 1,
            needs_hcpa_enrichment INTEGER DEFAULT 1,
            needs_ori_ingestion INTEGER DEFAULT 1,
            needs_lien_survival INTEGER DEFAULT 1,
            needs_sunbiz_search INTEGER DEFAULT 1,
            needs_permit_check INTEGER DEFAULT 1,
            needs_flood_check INTEGER DEFAULT 1,
            needs_market_data INTEGER DEFAULT 1,
            needs_tax_check INTEGER DEFAULT 1,
            needs_homeharvest_enrichment INTEGER DEFAULT 1,
            hcpa_scrape_failed INTEGER DEFAULT 0,
            hcpa_scrape_error TEXT,
            has_valid_parcel_id INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # bulk_parcels table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bulk_parcels (
            folio TEXT PRIMARY KEY,
            pin TEXT,
            strap TEXT,
            owner_name TEXT,
            property_address TEXT,
            city TEXT,
            zip_code TEXT,
            land_use TEXT,
            land_use_desc TEXT,
            year_built INTEGER,
            beds REAL,
            baths REAL,
            stories REAL,
            units INTEGER,
            buildings INTEGER,
            heated_area REAL,
            lot_size REAL,
            assessed_value REAL,
            market_value REAL,
            just_value REAL,
            land_value REAL,
            building_value REAL,
            extra_features_value REAL,
            taxable_value REAL,
            last_sale_date TEXT,
            last_sale_price REAL,
            raw_type TEXT,
            raw_sub TEXT,
            raw_taxdist TEXT,
            raw_muni TEXT,
            raw_legal1 TEXT,
            raw_legal2 TEXT,
            raw_legal3 TEXT,
            raw_legal4 TEXT,
            latitude REAL,
            longitude REAL,
            ingest_date TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # liens table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS liens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT,
            case_number TEXT,
            recording_date TEXT,
            document_type TEXT,
            book TEXT,
            page TEXT,
            amount REAL,
            grantor TEXT,
            grantee TEXT,
            description TEXT,
            instrument_number TEXT,
            survives_foreclosure INTEGER,
            is_surviving INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # permits table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS permits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT,
            permit_number TEXT UNIQUE,
            issue_date TEXT,
            status TEXT,
            permit_type TEXT,
            description TEXT,
            contractor TEXT,
            estimated_cost REAL,
            url TEXT,
            noc_instrument TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # documents table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT,
            case_number TEXT,
            document_type TEXT,
            file_path TEXT,
            ocr_text TEXT,
            extracted_data TEXT,
            recording_date TEXT,
            book TEXT,
            page TEXT,
            instrument_number TEXT,
            party1 TEXT,
            party2 TEXT,
            legal_description TEXT,
            sales_price REAL,
            page_count INTEGER,
            ori_uuid TEXT,
            ori_id TEXT,
            book_type TEXT,
            party2_resolution_method TEXT,
            is_self_transfer INTEGER DEFAULT 0,
            self_transfer_type TEXT,
            party2_confidence REAL DEFAULT 1.0,
            party2_resolved_at TEXT,
            triggered_by_search_id INTEGER,
            parties_one TEXT,
            parties_two TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # sales_history table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sales_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT,
            strap TEXT,
            book TEXT,
            page TEXT,
            instrument TEXT,
            sale_date TEXT,
            doc_type TEXT,
            qualified TEXT,
            vacant_improved TEXT,
            sale_price REAL,
            ori_link TEXT,
            pdf_path TEXT,
            grantor TEXT,
            grantee TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(folio, book, page)
        )
    """)

    # chain_of_title table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chain_of_title (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT,
            owner_name TEXT,
            acquired_from TEXT,
            acquisition_date TEXT,
            disposition_date TEXT,
            acquisition_instrument TEXT,
            acquisition_doc_type TEXT,
            acquisition_price REAL,
            link_status TEXT,
            confidence_score REAL,
            mrta_status TEXT,
            years_covered REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # encumbrances table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS encumbrances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT,
            chain_period_id INTEGER,
            encumbrance_type TEXT,
            creditor TEXT,
            debtor TEXT,
            amount REAL,
            amount_confidence TEXT,
            amount_flags TEXT,
            recording_date TEXT,
            instrument TEXT,
            book TEXT,
            page TEXT,
            is_satisfied INTEGER DEFAULT 0,
            satisfaction_instrument TEXT,
            satisfaction_date TEXT,
            survival_status TEXT,
            survival_reason TEXT,
            party2_resolution_method TEXT,
            is_self_transfer INTEGER DEFAULT 0,
            self_transfer_type TEXT,
            is_joined INTEGER DEFAULT 0,
            is_inferred INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # market_data table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT,
            source TEXT,
            capture_date TEXT,
            listing_status TEXT,
            list_price REAL,
            zestimate REAL,
            rent_estimate REAL,
            hoa_monthly REAL,
            days_on_market INTEGER,
            price_history TEXT,
            raw_json TEXT,
            screenshot_path TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # home_harvest table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS home_harvest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT,
            property_url TEXT,
            property_id TEXT,
            listing_id TEXT,
            mls TEXT,
            mls_id TEXT,
            mls_status TEXT,
            status TEXT,
            permalink TEXT,
            street TEXT,
            unit TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            formatted_address TEXT,
            style TEXT,
            beds REAL,
            full_baths REAL,
            half_baths REAL,
            sqft REAL,
            year_built INTEGER,
            stories REAL,
            garage REAL,
            lot_sqft REAL,
            text_description TEXT,
            property_type TEXT,
            days_on_mls INTEGER,
            list_price REAL,
            list_price_min REAL,
            list_price_max REAL,
            list_date TEXT,
            pending_date TEXT,
            sold_price REAL,
            last_sold_date TEXT,
            last_status_change_date TEXT,
            last_update_date TEXT,
            last_sold_price REAL,
            price_per_sqft REAL,
            new_construction INTEGER,
            hoa_fee REAL,
            monthly_fees TEXT,
            one_time_fees TEXT,
            estimated_value REAL,
            tax_assessed_value REAL,
            tax_history TEXT,
            latitude REAL,
            longitude REAL,
            neighborhoods TEXT,
            county TEXT,
            fips_code TEXT,
            parcel_number TEXT,
            nearby_schools TEXT,
            agent_uuid TEXT,
            agent_name TEXT,
            agent_email TEXT,
            agent_phone TEXT,
            agent_state_license TEXT,
            broker_uuid TEXT,
            broker_name TEXT,
            office_uuid TEXT,
            office_name TEXT,
            office_email TEXT,
            office_phones TEXT,
            estimated_monthly_rental REAL,
            tags TEXT,
            flags TEXT,
            photos TEXT,
            primary_photo TEXT,
            alt_photos TEXT,
            open_houses TEXT,
            units TEXT,
            pet_policy TEXT,
            parking TEXT,
            terms TEXT,
            current_estimates TEXT,
            estimates TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # scraper_outputs table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scraper_outputs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            property_id TEXT NOT NULL,
            scraper TEXT NOT NULL,
            scraped_at TEXT,
            processed_at TEXT,
            screenshot_path TEXT,
            vision_output_path TEXT,
            raw_data_path TEXT,
            source_url TEXT,
            prompt_version TEXT,
            extraction_success INTEGER DEFAULT 0,
            error_message TEXT,
            extracted_summary TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # status table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS status (
            case_number TEXT PRIMARY KEY,
            parcel_id TEXT,
            auction_date TEXT,
            auction_type TEXT,
            step_auction_scraped TEXT,
            step_pdf_downloaded TEXT,
            step_judgment_extracted TEXT,
            step_bulk_enriched TEXT,
            step_homeharvest_enriched TEXT,
            step_hcpa_enriched TEXT,
            step_ori_ingested TEXT,
            step_survival_analyzed TEXT,
            step_permits_checked TEXT,
            step_flood_checked TEXT,
            step_market_fetched TEXT,
            step_tax_checked TEXT,
            current_step INTEGER DEFAULT 0,
            pipeline_status TEXT DEFAULT 'pending',
            last_error TEXT,
            error_step INTEGER,
            retry_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT
        )
    """)

    # analysis_results table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analysis_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT,
            case_number TEXT,
            market_value REAL,
            realtor_estimate REAL,
            zillow_estimate REAL,
            rehab_cost REAL,
            surviving_liens_total REAL,
            auction_bid REAL,
            net_equity REAL,
            roi_percentage REAL,
            risk_score REAL,
            has_hoa_lien INTEGER DEFAULT 0,
            has_surviving_mortgage INTEGER DEFAULT 0,
            has_code_violations INTEGER DEFAULT 0,
            has_tax_certificate INTEGER DEFAULT 0,
            analyzed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Step4v2 tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legal_variations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT NOT NULL,
            variation_text TEXT NOT NULL,
            source_instrument TEXT,
            source_type TEXT NOT NULL,
            is_canonical INTEGER DEFAULT 0,
            priority INTEGER DEFAULT 99,
            search_attempted INTEGER DEFAULT 0,
            search_operator TEXT,
            search_result_count INTEGER,
            last_searched_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(folio, variation_text)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS property_parties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT NOT NULL,
            party_name TEXT NOT NULL,
            party_name_normalized TEXT,
            party_role TEXT,
            linked_identity_id INTEGER,
            active_from TEXT,
            active_to TEXT,
            source_instrument TEXT,
            source_document_type TEXT,
            recording_date TEXT,
            search_attempted INTEGER DEFAULT 0,
            search_result_count INTEGER,
            last_searched_at TEXT,
            is_generic INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(folio, party_name, source_instrument)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS linked_identities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL,
            entity_type TEXT,
            link_type TEXT,
            confidence REAL DEFAULT 1.0,
            sunbiz_doc_number TEXT,
            sunbiz_status TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS ori_search_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT NOT NULL,
            search_type TEXT NOT NULL,
            search_term TEXT NOT NULL,
            search_operator TEXT DEFAULT '',
            priority INTEGER DEFAULT 50,
            status TEXT DEFAULT 'pending',
            attempt_count INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 3,
            date_from TEXT,
            date_to TEXT,
            triggered_by_instrument TEXT,
            triggered_by_search_id INTEGER,
            result_count INTEGER,
            new_documents_found INTEGER,
            error_message TEXT,
            queued_at TEXT DEFAULT CURRENT_TIMESTAMP,
            started_at TEXT,
            completed_at TEXT,
            next_retry_at TEXT,
            UNIQUE(folio, search_type, search_term, search_operator)
        )
    """)

    # property_sources table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS property_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT NOT NULL,
            source_name TEXT NOT NULL,
            url TEXT,
            description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(folio, url)
        )
    """)

    logger.info("All tables created successfully")


def create_indices(conn: sqlite3.Connection) -> None:
    """Create indices for fast lookups."""
    indices = [
        ("idx_parcels_owner", "parcels(owner_name)"),
        ("idx_parcels_parcel_id", "parcels(parcel_id)"),
        ("idx_auctions_folio", "auctions(folio)"),
        ("idx_auctions_date", "auctions(auction_date)"),
        ("idx_auctions_type", "auctions(auction_type)"),
        ("idx_auctions_status", "auctions(status)"),
        ("idx_liens_folio", "liens(folio)"),
        ("idx_liens_case", "liens(case_number)"),
        ("idx_liens_date", "liens(recording_date)"),
        ("idx_permits_folio", "permits(folio)"),
        ("idx_documents_folio", "documents(folio)"),
        ("idx_documents_case", "documents(case_number)"),
        ("idx_documents_instrument", "documents(instrument_number)"),
        ("idx_analysis_folio", "analysis_results(folio)"),
        ("idx_sales_history_folio", "sales_history(folio)"),
        ("idx_sales_history_strap", "sales_history(strap)"),
        ("idx_homeharvest_folio", "home_harvest(folio)"),
        ("idx_status_auction_date", "status(auction_date)"),
        ("idx_status_pipeline_status", "status(pipeline_status)"),
        ("idx_bulk_parcels_strap", "bulk_parcels(strap)"),
        ("idx_legal_variations_folio", "legal_variations(folio)"),
        ("idx_property_parties_folio", "property_parties(folio)"),
        ("idx_linked_identities_canonical", "linked_identities(canonical_name)"),
        ("idx_search_queue_status", "ori_search_queue(status, priority)"),
        ("idx_search_queue_folio", "ori_search_queue(folio)"),
        ("idx_scraper_outputs_property", "scraper_outputs(property_id)"),
        ("idx_scraper_outputs_lookup", "scraper_outputs(property_id, scraper)"),
    ]

    for name, definition in indices:
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {definition}")
        except Exception as e:
            logger.warning(f"Could not create index {name}: {e}")

    logger.info(f"Created {len(indices)} indices")


def migrate_data_from_duckdb(sqlite_conn: sqlite3.Connection) -> None:
    """Migrate data from existing DuckDB database to SQLite."""
    try:
        import duckdb
    except ImportError:
        logger.info("duckdb not installed, skipping DuckDB migration")
        return

    duckdb_path = Path(DUCKDB_PATH)
    if not duckdb_path.exists():
        logger.info("No existing DuckDB database found, starting fresh")
        return
    
    logger.info(f"Migrating data from {DUCKDB_PATH}...")
    
    duck_conn = duckdb.connect(str(duckdb_path), read_only=True)
    
    tables_to_migrate = [
        "parcels", "auctions", "bulk_parcels", "liens", "permits",
        "documents", "sales_history", "chain_of_title", "encumbrances",
        "market_data", "home_harvest", "scraper_outputs", "status",
        "analysis_results", "legal_variations", "property_parties",
        "linked_identities", "ori_search_queue"
    ]
    
    for table in tables_to_migrate:
        try:
            # Check if table exists in DuckDB
            check = duck_conn.execute(
                f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' LIMIT 1"
            ).fetchone()
            if not check:
                continue
            
            # Get data
            rows = duck_conn.execute(f"SELECT * FROM {table}").fetchall()
            if not rows:
                continue
            
            # Get column names
            cols = duck_conn.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
            ).fetchall()
            col_names = [c[0] for c in cols]
            
            # Filter to columns that exist in SQLite schema
            sqlite_cols = sqlite_conn.execute(
                f"PRAGMA table_info({table})"
            ).fetchall()
            sqlite_col_names = {c[1] for c in sqlite_cols}
            
            # Build insert statement with matching columns
            common_cols = [c for c in col_names if c in sqlite_col_names]
            if not common_cols:
                continue
                
            placeholders = ", ".join(["?" for _ in common_cols])
            cols_str = ", ".join(common_cols)
            
            # Get column indices for extraction
            col_indices = [col_names.index(c) for c in common_cols]
            
            # Insert data
            for row in rows:
                values = [row[i] for i in col_indices]
                # Convert any non-serializable types
                values = [str(v) if isinstance(v, (list, dict)) else v for v in values]
                try:
                    sqlite_conn.execute(
                        f"INSERT OR IGNORE INTO {table} ({cols_str}) VALUES ({placeholders})",
                        values
                    )
                except Exception as e:
                    logger.debug(f"Row insert failed for {table}: {e}")
            
            sqlite_conn.commit()
            logger.info(f"  Migrated {table}: {len(rows)} rows")
            
        except Exception as e:
            logger.warning(f"Could not migrate {table}: {e}")
    
    duck_conn.close()
    logger.info("Data migration complete")


def create_sqlite_database() -> str:
    """Main function to create SQLite database with WAL mode."""
    logger.info("=" * 60)
    logger.info("Creating SQLite database with WAL mode")
    logger.info("=" * 60)
    
    # Remove existing SQLite db if present
    sqlite_path = Path(SQLITE_PATH)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    if sqlite_path.exists():
        logger.warning(f"Removing existing SQLite database: {SQLITE_PATH}")
        sqlite_path.unlink()
        # Also remove WAL and SHM files
        for ext in ["-wal", "-shm"]:
            wal_path = Path(str(sqlite_path) + ext)
            if wal_path.exists():
                wal_path.unlink()
    
    # Create database
    logger.info(f"Creating SQLite database: {SQLITE_PATH}")
    conn = sqlite3.connect(SQLITE_PATH)
    
    # Enable WAL mode
    setup_wal_mode(conn)
    
    # Create schema
    logger.info("Creating tables...")
    create_tables(conn)
    
    logger.info("Creating indices...")
    create_indices(conn)
    
    # Migrate data from DuckDB
    migrate_data_from_duckdb(conn)
    
    conn.commit()
    conn.close()
    
    logger.info("=" * 60)
    logger.info(f"SQLite database created: {SQLITE_PATH}")
    logger.info("=" * 60)
    
    return SQLITE_PATH


if __name__ == "__main__":
    create_sqlite_database()
