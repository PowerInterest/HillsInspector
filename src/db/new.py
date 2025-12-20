"""
Database initialization and setup.
"""
import duckdb
from pathlib import Path

def create_database(db_path: str = "data/property_master.db"):
    """
    Initialize the DuckDB database with all necessary tables and indices.
    """
    # Ensure data directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(db_path)
    
    # Create sequences
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_auctions_id START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_liens_id START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_permits_id START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_documents_id START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_analysis_id START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS liens_id_seq START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS sales_history_seq START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS chain_of_title_seq START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS encumbrances_seq START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS legal_variations_seq START 1")
    
    # Create parcels table (from HCPA bulk data)
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
            latitude DOUBLE,
            longitude DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create auctions table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS auctions (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_auctions_id'),
            case_number VARCHAR UNIQUE,
            folio VARCHAR,
            parcel_id VARCHAR,
            certificate_number VARCHAR,
            auction_type VARCHAR,  -- 'FORECLOSURE' or 'TAX_DEED'
            auction_date DATE,
            property_address VARCHAR,
            assessed_value FLOAT,
            final_judgment_amount FLOAT,
            opening_bid FLOAT,
            plaintiff_max_bid VARCHAR,
            
            -- Lien Analysis Fields (Critical)
            lien_position VARCHAR,  -- '1st', '2nd', 'HOA', 'UNKNOWN'
            est_surviving_debt FLOAT,
            is_toxic_title BOOLEAN DEFAULT FALSE,
            
            -- Final Judgment Data
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
            
            -- Status
            status VARCHAR DEFAULT 'PENDING',  -- 'PENDING', 'ANALYZED', 'FLAGGED'

            -- Pipeline Flags (tracks which steps need to run)
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

            -- HCPA Scrape Status
            hcpa_scrape_failed BOOLEAN DEFAULT FALSE,
            
            -- Parcel ID Validation (FALSE = mobile home or unresolved, limited analysis only)
            has_valid_parcel_id BOOLEAN DEFAULT TRUE,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create liens table (from Official Records)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS liens (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_liens_id'),
            folio VARCHAR,
            case_number VARCHAR,
            recording_date DATE,
            document_type VARCHAR,  -- 'MTG', 'LIS_PENDENS', 'JUDGMENT', etc.
            book VARCHAR,
            page VARCHAR,
            amount FLOAT,
            grantor VARCHAR,  -- Who owes the money
            grantee VARCHAR,  -- Who is owed the money
            description TEXT,
            instrument_number VARCHAR,
            survives_foreclosure BOOLEAN,
            is_surviving BOOLEAN,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create permits table
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
    
    # Create documents table (for PDFs and evidence)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_documents_id'),
            folio VARCHAR,
            case_number VARCHAR,
            document_type VARCHAR,  -- 'FINAL_JUDGMENT', 'LIS_PENDENS', 'MORTGAGE', etc.
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

            -- ORI API Additional Fields
            sales_price FLOAT,          -- Sale price or loan amount from ORI
            page_count INTEGER,         -- Number of pages in document
            ori_uuid VARCHAR,           -- ORI unique document identifier
            ori_id VARCHAR,             -- ORI encrypted ID for PDF download
            book_type VARCHAR,          -- Book type (OR, etc.)

            -- Party 2 Resolution Fields
            party2_resolution_method VARCHAR,  -- 'cqid_326', 'ocr_extraction', NULL if original
            is_self_transfer BOOLEAN DEFAULT FALSE,
            self_transfer_type VARCHAR,  -- 'exact_match', 'trust_transfer', 'name_variation'
            party2_confidence FLOAT DEFAULT 1.0,  -- Confidence in party2 data (lower for OCR)
            party2_resolved_at TIMESTAMP,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create analysis_results table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analysis_results (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_analysis_id'),
            folio VARCHAR,
            case_number VARCHAR,
            
            -- Market Data
            market_value FLOAT,
            realtor_estimate FLOAT,
            zillow_estimate FLOAT,
            
            -- Costs
            rehab_cost FLOAT,
            surviving_liens_total FLOAT,
            auction_bid FLOAT,
            
            -- Result
            net_equity FLOAT,
            roi_percentage FLOAT,
            risk_score FLOAT,  -- 0-100, higher = riskier
            
            -- Flags
            has_hoa_lien BOOLEAN DEFAULT FALSE,
            has_surviving_mortgage BOOLEAN DEFAULT FALSE,
            has_code_violations BOOLEAN DEFAULT FALSE,
            has_tax_certificate BOOLEAN DEFAULT FALSE,
            
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create sales_history table
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(folio, book, page)
        )
    """)

    # Create legal_variations table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legal_variations (
            id INTEGER PRIMARY KEY DEFAULT nextval('legal_variations_seq'),
            folio VARCHAR,
            variation_text VARCHAR,
            source_instrument VARCHAR,
            source_type VARCHAR,
            is_canonical BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create chain_of_title table
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create encumbrances table (enhanced)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS encumbrances (
            id INTEGER PRIMARY KEY DEFAULT nextval('encumbrances_seq'),
            folio VARCHAR,
            chain_period_id INTEGER,
            encumbrance_type VARCHAR,
            creditor VARCHAR,
            debtor VARCHAR,  -- Party 1 / grantor
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

            -- Party 2 Resolution Fields
            party2_resolution_method VARCHAR,  -- 'cqid_326', 'ocr_extraction', NULL if original
            is_self_transfer BOOLEAN DEFAULT FALSE,
            self_transfer_type VARCHAR,  -- 'exact_match', 'trust_transfer', 'name_variation'

            is_joined BOOLEAN DEFAULT FALSE,
            is_inferred BOOLEAN DEFAULT FALSE,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create market_data table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY,
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

    # Create home_harvest table (from HomeHarvest enrichment)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS homeharvest_id_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS home_harvest (
            -- Primary Key & Links
            id BIGINT PRIMARY KEY DEFAULT nextval('homeharvest_id_seq'),
            folio VARCHAR, -- Foreign key to our main parcels table
            
            -- Basic Information
            property_url VARCHAR,
            property_id VARCHAR,
            listing_id VARCHAR,
            mls VARCHAR,
            mls_id VARCHAR,
            mls_status VARCHAR,
            status VARCHAR,
            permalink VARCHAR,

            -- Address Details
            street VARCHAR,
            unit VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip_code VARCHAR,
            formatted_address VARCHAR,

            -- Property Description
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

            -- Property Listing Details
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

            -- Tax Information
            tax_assessed_value DOUBLE,
            tax_history JSON,

            -- Location Details
            latitude DOUBLE,
            longitude DOUBLE,
            neighborhoods VARCHAR,
            county VARCHAR,
            fips_code VARCHAR,
            parcel_number VARCHAR,
            nearby_schools JSON,

            -- Agent/Broker/Office Info
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

            -- Additional Fields
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

            -- Metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_homeharvest_folio ON home_harvest(folio)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_homeharvest_address ON home_harvest(formatted_address)")
    
    # Create indices for fast lookups
    print("Creating indices...")
    
    conn.execute("CREATE INDEX IF NOT EXISTS idx_parcels_owner ON parcels(owner_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_parcels_parcel_id ON parcels(parcel_id)")
    
    conn.execute("CREATE INDEX IF NOT EXISTS idx_auctions_folio ON auctions(folio)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_auctions_date ON auctions(auction_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_auctions_type ON auctions(auction_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_auctions_status ON auctions(status)")
    
    conn.execute("CREATE INDEX IF NOT EXISTS idx_liens_folio ON liens(folio)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_liens_case ON liens(case_number)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_liens_date ON liens(recording_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_liens_survives ON liens(survives_foreclosure)")
    
    conn.execute("CREATE INDEX IF NOT EXISTS idx_permits_folio ON permits(folio)")
    
    conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_folio ON documents(folio)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_case ON documents(case_number)")
    
    conn.execute("CREATE INDEX IF NOT EXISTS idx_analysis_folio ON analysis_results(folio)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_analysis_case ON analysis_results(case_number)")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_history_folio ON sales_history(folio)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_history_strap ON sales_history(strap)")
    
    print(f"Database created successfully at {db_path}")
    
    # Print table info
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f"\nCreated {len(tables)} tables:")
    for table in tables:
        print(f"  - {table[0]}")
    
    conn.close()
    return db_path

if __name__ == "__main__":
    create_database()
