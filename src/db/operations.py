"""
Database operations for property data.
Provides high-level functions for inserting and querying data.
"""
import duckdb
import threading
from contextlib import suppress
from datetime import date, datetime
from typing import List, Optional, Dict, Any, Any as AnyType
import json
from loguru import logger

from src.models.property import Property, Lien
from src.utils.time import ensure_duckdb_utc, now_utc_naive, parse_date, today_local

class PropertyDB:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or "data/property_master.db"
        self._local = threading.local()
        self._local.conn = None
        self._schema_migrations_applied = False

    def _get_conn(self):
        return getattr(self._local, "conn", None)

    @property
    def conn(self):
        conn = self._get_conn()
        if conn is None:
            conn = self.connect()
        return conn

    @conn.setter
    def conn(self, value):
        self._local.conn = value
    
    def connect(self):
        """Open database connection."""
        conn = self._get_conn()
        if conn is None:
            conn = duckdb.connect(self.db_path)
            ensure_duckdb_utc(conn)
            self._local.conn = conn
        self._apply_schema_migrations(conn)
        return conn

    def checkpoint(self) -> None:
        """
        Force WAL checkpoint - flushes all pending writes to the main database file.

        This is critical for data durability. Without checkpointing, all changes
        remain in the WAL file and can be lost if the process crashes or the
        WAL replay fails on next open.

        Call this after completing each major pipeline step to ensure data is
        persisted to disk.
        """
        conn = self._get_conn()
        if conn is None:
            # No existing connection - create one for checkpoint
            conn = self.connect()
        try:
            conn.execute("CHECKPOINT")
            logger.info("Database checkpoint complete - WAL flushed to disk")
        except Exception as e:
            logger.warning(f"Checkpoint failed (non-fatal): {e}")

    def _apply_schema_migrations(self, conn) -> None:
        """
        Apply lightweight, idempotent schema migrations.

        Keep this narrow and safe: only `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
        """
        if self._schema_migrations_applied or conn is None:
            return

        def table_exists(table_name: str) -> bool:
            with suppress(Exception):
                row = conn.execute(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = ?
                    LIMIT 1
                    """,
                    [table_name],
                ).fetchone()
                return row is not None
            return False

        if not table_exists("parcels"):
            conn.execute("""
                CREATE TABLE parcels (
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

        if table_exists("parcels"):
            conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS tax_status VARCHAR")
            conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS tax_warrant BOOLEAN")

        if table_exists("encumbrances"):
            conn.execute(
                "ALTER TABLE encumbrances ADD COLUMN IF NOT EXISTS is_joined BOOLEAN DEFAULT FALSE"
            )
            conn.execute(
                "ALTER TABLE encumbrances ADD COLUMN IF NOT EXISTS is_inferred BOOLEAN DEFAULT FALSE"
            )

        if not table_exists("market_data"):
            conn.execute("CREATE SEQUENCE IF NOT EXISTS market_data_seq START 1")
            # Drop if exists (in case it was created incorrectly without data)
            conn.execute("DROP TABLE IF EXISTS market_data") 
            conn.execute("""
                CREATE TABLE market_data (
                    id INTEGER PRIMARY KEY DEFAULT nextval('market_data_seq'),
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

        if table_exists("chain_of_title"):
            conn.execute(
                "ALTER TABLE chain_of_title ADD COLUMN IF NOT EXISTS link_status VARCHAR"
            )
            conn.execute(
                "ALTER TABLE chain_of_title ADD COLUMN IF NOT EXISTS confidence_score FLOAT"
            )
            conn.execute(
                "ALTER TABLE chain_of_title ADD COLUMN IF NOT EXISTS mrta_status VARCHAR"
            )
            conn.execute(
                "ALTER TABLE chain_of_title ADD COLUMN IF NOT EXISTS years_covered FLOAT"
            )

        self._schema_migrations_applied = True
        # Note: We intentionally do NOT checkpoint here. Checkpointing requires
        # exclusive access and will spin-wait if another connection has pending
        # transactions. The orchestrator handles checkpointing after each step.

    @staticmethod
    def normalize_folio(value: str | None) -> str | None:
        """Normalize a folio by stripping non-digit characters."""
        if not value:
            return None
        digits = "".join(ch for ch in str(value) if ch.isdigit())
        return digits or None

    def get_folio_from_strap(self, strap: str) -> str | None:
        """Lookup numeric folio for a STRAP (parcel_id) using bulk_parcels/parcels."""
        if not strap:
            return None
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT folio FROM bulk_parcels WHERE strap = ? LIMIT 1",
                [strap],
            ).fetchone()
            if row and row[0]:
                return str(row[0])
        except Exception:
            pass
        try:
            row = conn.execute(
                "SELECT bulk_folio FROM parcels WHERE folio = ? LIMIT 1",
                [strap],
            ).fetchone()
            if row and row[0]:
                return str(row[0])
        except Exception:
            pass
        return None
    
    def close(self):
        """Close database connection."""
        conn = self._get_conn()
        if conn:
            conn.close()
            self._local.conn = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def initialize_pipeline_flags(self):
        """
        Initialize boolean flags on the auctions table to track pipeline progress.
        Backfills state based on existing data.
        """
        conn = self.connect()
        
        # 1. Add columns if they don't exist
        flags = [
            "needs_judgment_extraction",
            "needs_hcpa_enrichment",
            "needs_ori_ingestion",
            "needs_lien_survival",
            "needs_sunbiz_search",
            "needs_permit_check",
            "needs_flood_check",
            "needs_market_data",
            "needs_tax_check",
            "needs_homeharvest_enrichment",
            "has_valid_parcel_id"
        ]
        
        for flag in flags:
            conn.execute(f"ALTER TABLE auctions ADD COLUMN IF NOT EXISTS {flag} BOOLEAN DEFAULT TRUE")
            
        # 2. Backfill state based on existing data
        
        # Step 2: Judgment Extraction
        # If we have extracted data, we don't need to run it again
        conn.execute("""
            UPDATE auctions 
            SET needs_judgment_extraction = FALSE 
            WHERE extracted_judgment_data IS NOT NULL
        """)
        
        # Step 4/12: HCPA Enrichment
        # If we have an owner name in parcels table, we likely ran enrichment
        # Note: We join on parcel_id/folio
        conn.execute("""
            UPDATE auctions
            SET needs_hcpa_enrichment = FALSE
            WHERE parcel_id IN (
                SELECT folio FROM parcels WHERE owner_name IS NOT NULL
            )
        """)
        
        # Step 5: ORI Ingestion
        # If we have documents for this folio, we ran ORI ingestion
        conn.execute("""
            UPDATE auctions
            SET needs_ori_ingestion = FALSE
            WHERE parcel_id IN (
                SELECT DISTINCT folio FROM documents
            )
        """)
        
        # Step 6: Lien Survival
        # If status is ANALYZED or FLAGGED, we ran analysis
        conn.execute("""
            UPDATE auctions
            SET needs_lien_survival = FALSE
            WHERE status IN ('ANALYZED', 'FLAGGED')
        """)
        
        # Step 8: Permits
        # If we have permits for this folio
        conn.execute("""
            UPDATE auctions
            SET needs_permit_check = FALSE
            WHERE parcel_id IN (
                SELECT DISTINCT folio FROM permits
            )
        """)
        
        # Step 9: Flood Check
        # Flood data would be stored separately - for now, skip this backfill
        # as the flood_zone column doesn't exist yet in parcels
        # TODO: Create flood_data table or add column when implementing FEMA lookup
        
        # Step 10/11: Market Data
        # If we have market data rows
        conn.execute("""
            UPDATE auctions
            SET needs_market_data = FALSE
            WHERE parcel_id IN (
                SELECT DISTINCT folio FROM market_data
            )
        """)

        # Step 14: HomeHarvest Enrichment
        conn.execute("""
            UPDATE auctions
            SET needs_homeharvest_enrichment = FALSE
            WHERE folio IN (
                SELECT DISTINCT folio FROM home_harvest
                WHERE created_at >= CURRENT_DATE - INTERVAL 7 DAY
            )
        """)
        
        # Step 13: Tax Check
        # If we have tax liens
        conn.execute("""
            UPDATE auctions
            SET needs_tax_check = FALSE
            WHERE parcel_id IN (
                SELECT DISTINCT folio FROM liens WHERE document_type LIKE 'TAX%'
            )
        """)
        
        print("Pipeline flags initialized and backfilled.")

    def mark_step_complete(self, case_number: str, step_flag: str):
        """
        Mark a specific pipeline step as complete for an auction.
        
        Args:
            case_number: Case number of the auction
            step_flag: Name of the flag column (e.g., 'needs_permit_check')
        """
        conn = self.connect()
        # Sanitize input to prevent injection (though internal use only)
        valid_flags = {
            "needs_judgment_extraction",
            "needs_hcpa_enrichment",
            "needs_ori_ingestion",
            "needs_lien_survival",
            "needs_sunbiz_search",
            "needs_permit_check",
            "needs_flood_check",
            "needs_market_data",
            "needs_tax_check",
            "needs_homeharvest_enrichment" # New flag
        }
        if step_flag not in valid_flags:
            raise ValueError(f"Invalid flag name: {step_flag}")
            
        conn.execute(f"""
            UPDATE auctions
            SET {step_flag} = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
        """, [case_number])

    def mark_step_complete_by_folio(self, folio: str, step_flag: str):
        """
        Mark a specific pipeline step as complete for ALL auctions with this folio.

        This prevents duplicate scraping when the same property appears in multiple
        auctions (e.g., same folio with different case numbers).

        Args:
            folio: Parcel ID / folio number
            step_flag: Name of the flag column (e.g., 'needs_hcpa_enrichment')
        """
        conn = self.connect()
        # Sanitize input to prevent injection (though internal use only)
        valid_flags = {
            "needs_judgment_extraction",
            "needs_hcpa_enrichment",
            "needs_ori_ingestion",
            "needs_lien_survival",
            "needs_sunbiz_search",
            "needs_permit_check",
            "needs_flood_check",
            "needs_market_data",
            "needs_tax_check",
            "needs_homeharvest_enrichment"
        }
        if step_flag not in valid_flags:
            raise ValueError(f"Invalid flag name: {step_flag}")

        conn.execute(f"""
            UPDATE auctions
            SET {step_flag} = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE parcel_id = ?
        """, [folio])

    def mark_hcpa_scrape_failed(self, case_number: str, error: str) -> None:
        """Record an HCPA scrape failure on the auction row."""
        conn = self.connect()
        conn.execute(
            "ALTER TABLE auctions ADD COLUMN IF NOT EXISTS hcpa_scrape_failed BOOLEAN DEFAULT FALSE"
        )
        conn.execute("ALTER TABLE auctions ADD COLUMN IF NOT EXISTS hcpa_scrape_error VARCHAR")
        conn.execute(
            """
            UPDATE auctions
            SET hcpa_scrape_failed = TRUE,
                hcpa_scrape_error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
            """,
            [error, case_number],
        )

    def mark_ori_party_fallback_used(self, case_number: str, note: str = "") -> None:
        """Record that ORI party-search fallback was used (for review)."""
        conn = self.connect()
        conn.execute(
            "ALTER TABLE auctions ADD COLUMN IF NOT EXISTS ori_party_fallback_used BOOLEAN DEFAULT FALSE"
        )
        conn.execute("ALTER TABLE auctions ADD COLUMN IF NOT EXISTS ori_party_fallback_note VARCHAR")
        conn.execute(
            """
            UPDATE auctions
            SET ori_party_fallback_used = TRUE,
                ori_party_fallback_note = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
            """,
            [note, case_number],
        )

    def upsert_auction(self, prop: Property) -> int:
        """
        Insert or update an auction property.

        Args:
            prop: Property object from scraper

        Returns:
            Auction ID
        """
        conn = self.connect()

        # Ensure column exists (dynamic schema update)
        conn.execute("ALTER TABLE auctions ADD COLUMN IF NOT EXISTS has_valid_parcel_id BOOLEAN DEFAULT TRUE")

        # Check if auction already exists
        existing = conn.execute(
            "SELECT id FROM auctions WHERE case_number = ?",
            [prop.case_number]
        ).fetchone()

        if existing:
            # Update existing record
            conn.execute("""
                UPDATE auctions SET
                    folio = ?,
                    parcel_id = ?,
                    certificate_number = ?,
                    auction_type = ?,
                    auction_date = ?,
                    property_address = ?,
                    assessed_value = ?,
                    final_judgment_amount = ?,
                    opening_bid = ?,
                    plaintiff = COALESCE(?, plaintiff),
                    defendant = COALESCE(?, defendant),
                    has_valid_parcel_id = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE case_number = ?
            """, [
                prop.parcel_id,
                prop.parcel_id,
                prop.certificate_number,
                prop.auction_type,
                prop.auction_date,
                prop.address,
                prop.assessed_value,
                prop.final_judgment_amount,
                prop.opening_bid,
                getattr(prop, 'plaintiff', None),
                getattr(prop, 'defendant', None),
                getattr(prop, 'has_valid_parcel_id', True),
                prop.case_number
            ])
            return existing[0]
        conn.execute("""
                INSERT INTO auctions (
                    case_number, folio, parcel_id, certificate_number,
                    auction_type, auction_date, property_address,
                    assessed_value, final_judgment_amount, opening_bid,
                    plaintiff, defendant, has_valid_parcel_id,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, [
            prop.case_number,
            prop.parcel_id,
            prop.parcel_id,
            prop.certificate_number,
            prop.auction_type,
            prop.auction_date,
            prop.address,
            prop.assessed_value,
            prop.final_judgment_amount,
            prop.opening_bid,
            getattr(prop, 'plaintiff', None),
            getattr(prop, 'defendant', None),
            getattr(prop, 'has_valid_parcel_id', True),
        ])

        # Fetch the new ID
        result = conn.execute(
            "SELECT id FROM auctions WHERE case_number = ?",
            [prop.case_number]
        ).fetchone()

        return result[0] if result else 0
    
    def update_parcel_tax_status(self, folio: str, tax_status: str, tax_warrant: bool):
        """Update tax status and warrant info for a parcel."""
        conn = self.connect()
        conn.execute("INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [folio])
        conn.execute("""
            UPDATE parcels 
            SET tax_status = ?, tax_warrant = ?
            WHERE parcel_id = ? OR folio = ?
        """, [tax_status, tax_warrant, folio, folio])

    def upsert_parcel(self, prop: Property) -> str:
        """
        Insert or update parcel data from enriched property.

        Args:
            prop: Property object with enriched data

        Returns:
            Folio (parcel_id)
        """
        conn = self.connect()

        folio = prop.parcel_id

        # Ensure columns exist (DuckDB supports IF NOT EXISTS natively)
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS market_analysis_content VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS legal_description VARCHAR")

        # Use ON CONFLICT for atomic upsert
        # 1. Try to insert (ignore if exists)
        conn.execute("""
            INSERT OR IGNORE INTO parcels (
                folio, parcel_id, owner_name, property_address,
                city, zip_code, year_built, beds, baths,
                heated_area, assessed_value, image_url, market_analysis_content,
                legal_description, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            folio,
            prop.parcel_id,
            prop.owner_name,
            prop.address,
            prop.city,
            prop.zip_code,
            prop.year_built,
            prop.beds,
            prop.baths,
            prop.heated_area,
            prop.assessed_value,
            prop.image_url,
            prop.market_analysis_content,
            prop.legal_description,
            now_utc_naive(),
        ])

        # 2. Update (in case it already existed and we have new data)
        conn.execute("""
            UPDATE parcels SET
                owner_name = COALESCE(?, owner_name),
                property_address = COALESCE(?, property_address),
                city = COALESCE(?, city),
                zip_code = COALESCE(?, zip_code),
                year_built = COALESCE(?, year_built),
                beds = COALESCE(?, beds),
                baths = COALESCE(?, baths),
                heated_area = COALESCE(?, heated_area),
                assessed_value = COALESCE(?, assessed_value),
                image_url = COALESCE(?, image_url),
                market_analysis_content = COALESCE(?, market_analysis_content),
                legal_description = COALESCE(?, legal_description),
                updated_at = ?
            WHERE folio = ?
        """, [
            prop.owner_name,
            prop.address,
            prop.city,
            prop.zip_code,
            prop.year_built,
            prop.beds,
            prop.baths,
            prop.heated_area,
            prop.assessed_value,
            prop.image_url,
            prop.market_analysis_content,
            prop.legal_description,
            now_utc_naive(),
            folio
        ])

        # Save sales history if available
        if prop.sales_history:
            self.save_sales_history_from_hcpa(folio, prop.sales_history)

        return folio

    def save_sales_history_from_hcpa(self, folio: str, sales: List[Dict]):
        """
        Save sales history records from HCPA enrichment (vision-extracted).

        The vision prompt extracts: date, price, instrument, deed_type, grantor, grantee
        """
        conn = self.connect()

        # Ensure table exists and has grantor/grantee columns
        self.create_sales_history_table()
        conn.execute("ALTER TABLE sales_history ADD COLUMN IF NOT EXISTS grantor VARCHAR")
        conn.execute("ALTER TABLE sales_history ADD COLUMN IF NOT EXISTS grantee VARCHAR")

        # Create index on instrument for faster lookups
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_history_instrument ON sales_history(folio, instrument)")

        saved_count = 0
        for sale in sales:
            try:
                # Parse sale price - handle both 'price' and 'sale_price' keys
                price_str = str(sale.get('price', sale.get('sale_price', ''))).replace('$', '').replace(',', '')
                try:
                    sale_price = float(price_str) if price_str else None
                except (ValueError, TypeError):
                    sale_price = None

                # Get instrument number
                instrument = sale.get('instrument', '')

                # Skip if no instrument number (can't dedupe without it)
                if not instrument:
                    continue

                # Check if record already exists by folio + instrument
                existing = conn.execute("""
                    SELECT id FROM sales_history
                    WHERE folio = ? AND instrument = ?
                """, [folio, instrument]).fetchone()

                if existing:
                    # Update existing record
                    conn.execute("""
                        UPDATE sales_history SET
                            sale_date = ?,
                            doc_type = ?,
                            sale_price = ?,
                            grantor = ?,
                            grantee = ?
                        WHERE folio = ? AND instrument = ?
                    """, [
                        sale.get('date'),
                        sale.get('deed_type', sale.get('doc_type')),
                        sale_price,
                        sale.get('grantor'),
                        sale.get('grantee'),
                        folio,
                        instrument
                    ])
                else:
                    # Insert new record
                    conn.execute("""
                        INSERT INTO sales_history (
                            folio, instrument, sale_date, doc_type,
                            sale_price, grantor, grantee
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [
                        folio,
                        instrument,
                        sale.get('date'),
                        sale.get('deed_type', sale.get('doc_type')),
                        sale_price,
                        sale.get('grantor'),
                        sale.get('grantee')
                    ])
                saved_count += 1
            except Exception as e:
                logger.warning(f"Error saving HCPA sale record for {folio}: {e}")

        if saved_count > 0:
            logger.info(f"Saved {saved_count} sales history records for {folio}")
    
    def get_auctions_by_date(self, auction_date: date) -> List[Dict[str, Any]]:
        """Get all auctions for a specific date."""
        conn = self.connect()
        
        results = conn.execute("""
            SELECT * FROM auctions
            WHERE auction_date = ?
            ORDER BY case_number
        """, [auction_date]).fetchall()
        
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in results]
    
    def get_pending_analysis(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get auctions that need lien analysis."""
        conn = self.connect()
        
        results = conn.execute("""
            SELECT * FROM auctions
            WHERE status = 'PENDING'
            ORDER BY auction_date
            LIMIT ?
        """, [limit]).fetchall()
        
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in results]
    
    def mark_as_analyzed(self, case_number: str):
        """Mark an auction as analyzed."""
        conn = self.connect()
        conn.execute("""
            UPDATE auctions
            SET status = 'ANALYZED', updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
        """, [case_number])
    
    def mark_as_toxic(self, case_number: str, reason: str = ""):
        """Flag an auction as toxic title."""
        conn = self.connect()
        conn.execute("""
            UPDATE auctions
            SET is_toxic_title = TRUE, status = 'FLAGGED', updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
        """, [case_number])

    def save_judgment_text(self, case_number: str, text: str):
        """Save the OCR'd text of the Final Judgment."""
        conn = self.connect()
        
        # Ensure column exists (DuckDB supports IF NOT EXISTS natively)
        conn.execute("ALTER TABLE auctions ADD COLUMN IF NOT EXISTS final_judgment_content VARCHAR")
            
        conn.execute("""
            UPDATE auctions
            SET final_judgment_content = ?, updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
        """, [text, case_number])

    def update_judgment_data(self, case_number: str, data: Dict[str, Any]):
        """Update auction row with extracted Final Judgment data."""
        conn = self.connect()

        fields = {
            "plaintiff": data.get("plaintiff"),
            "defendant": data.get("defendant"),
            "foreclosure_type": data.get("foreclosure_type"),
            "judgment_date": parse_date(data.get("judgment_date")),
            "lis_pendens_date": parse_date(data.get("lis_pendens_date")),
            "foreclosure_sale_date": parse_date(data.get("foreclosure_sale_date")),
            "total_judgment_amount": data.get("total_judgment_amount"),
            "principal_amount": data.get("principal_amount"),
            "interest_amount": data.get("interest_amount"),
            "attorney_fees": data.get("attorney_fees"),
            "court_costs": data.get("court_costs"),
            "original_mortgage_amount": data.get("original_mortgage_amount"),
            "original_mortgage_date": parse_date(data.get("original_mortgage_date")),
            "monthly_payment": data.get("monthly_payment"),
            "default_date": parse_date(data.get("default_date")),
            "extracted_judgment_data": data.get("extracted_judgment_data"),
            "raw_judgment_text": data.get("raw_judgment_text"),
            "judgment_extracted_at": now_utc_naive(),
        }

        set_parts = []
        params = []
        for key, value in fields.items():
            if value is not None:
                set_parts.append(f"{key} = ?")
                params.append(value)

        if not set_parts:
            return False

        params.append(case_number)
        sql = f"""
            UPDATE auctions
            SET {', '.join(set_parts)}, updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
        """
        conn.execute(sql, params)
        return True

    @staticmethod
    def _parse_recording_date(value: Any) -> Optional[date]:
        """Parse recording_date from various formats."""
        return parse_date(value)

    def save_liens(self, folio: str, liens: List[AnyType], case_number: str | None = None):
        """Save identified liens to the database.

        Args:
            folio: Property folio (primary key for grouping liens)
            liens: List of Lien objects or dicts
            case_number: Optional case number for reference
        """
        conn = self.connect()

        try:
            # Create sequence and table with folio as primary grouping
            conn.execute("CREATE SEQUENCE IF NOT EXISTS liens_id_seq")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS liens (
                    id INTEGER PRIMARY KEY DEFAULT nextval('liens_id_seq'),
                    folio VARCHAR,
                    case_number VARCHAR,
                    document_type VARCHAR,
                    recording_date DATE,
                    amount DECIMAL(12, 2),
                    grantor VARCHAR,
                    grantee VARCHAR,
                    book VARCHAR,
                    page VARCHAR,
                    description VARCHAR,
                    instrument_number VARCHAR,
                    survives_foreclosure BOOLEAN,
                    is_surviving BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Add folio column if it doesn't exist (migration)
            import contextlib
            with contextlib.suppress(Exception):
                conn.execute("ALTER TABLE liens ADD COLUMN IF NOT EXISTS folio VARCHAR")

            # Insert new liens (don't delete - allow accumulation from multiple sources)
            for lien in liens:
                # Support dicts or Lien models
                if isinstance(lien, dict):
                    document_type = lien.get("document_type", "")
                    rec_date = self._parse_recording_date(lien.get("recording_date"))
                    amount = lien.get("amount")
                    grantor = lien.get("grantor")
                    grantee = lien.get("grantee")
                    book = lien.get("book")
                    page = lien.get("page")
                    description = lien.get("description")
                    instrument_number = lien.get("instrument_number")
                    is_surviving = lien.get("is_surviving")
                else:
                    document_type = lien.document_type
                    rec_date = self._parse_recording_date(lien.recording_date)
                    amount = lien.amount
                    grantor = lien.grantor
                    grantee = lien.grantee
                    book = lien.book
                    page = lien.page
                    description = getattr(lien, "description", None)
                    instrument_number = getattr(lien, "instrument_number", None)
                    is_surviving = getattr(lien, "is_surviving", None)

                # Check for duplicate before inserting
                existing = conn.execute("""
                    SELECT id FROM liens
                    WHERE folio = ? AND document_type = ? AND
                          ((book = ? AND page = ?) OR instrument_number = ?)
                """, [folio, document_type, book, page, instrument_number]).fetchone()

                if existing:
                    # Update existing record
                    conn.execute("""
                        UPDATE liens SET
                            amount = COALESCE(?, amount),
                            grantor = COALESCE(?, grantor),
                            grantee = COALESCE(?, grantee),
                            is_surviving = COALESCE(?, is_surviving)
                        WHERE id = ?
                    """, [amount, grantor, grantee, is_surviving, existing[0]])
                else:
                    conn.execute("""
                        INSERT INTO liens (
                            folio, case_number, document_type, recording_date,
                            amount, grantor, grantee, book, page, description,
                            instrument_number, is_surviving
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        folio,
                        case_number,
                        document_type,
                        rec_date,
                        amount,
                        grantor,
                        grantee,
                        book,
                        page,
                        description,
                        instrument_number,
                        is_surviving
                    ])

        except Exception as e:
            print(f"Error in save_liens: {e}")
            raise

    def get_liens_by_case(self, case_number: str) -> List[Dict[str, Any]]:
        """Fetch liens by case number."""
        conn = self.connect()
        rows = conn.execute("""
            SELECT * FROM liens
            WHERE case_number = ?
        """, [case_number]).fetchall()
        if not rows:
            return []
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, r, strict=True)) for r in rows]

    @staticmethod
    def _dict_to_lien(data: Dict[str, Any]) -> Lien:
        """Convert a liens table row dict to Lien model (best effort)."""
        rec_date = data.get("recording_date")
        if isinstance(rec_date, str) and rec_date:
            try:
                rec_date = datetime.strptime(rec_date, "%Y-%m-%d").date()
            except ValueError:
                try:
                    rec_date = datetime.strptime(rec_date, "%m/%d/%Y").date()
                except ValueError:
                    rec_date = None
        return Lien(
            recording_date=rec_date,
            document_type=data.get("document_type", ""),
            book=data.get("book"),
            page=data.get("page"),
            amount=data.get("amount"),
            grantor=data.get("grantor"),
            grantee=data.get("grantee"),
            description=None,
            is_surviving=data.get("is_surviving"),
        )

    def ensure_geocode_columns(self):
        """Add latitude/longitude to parcels if missing."""
        conn = self.connect()
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS latitude DOUBLE")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS longitude DOUBLE")

    def update_legal_description(self, folio: str, legal_description: str):
        """Update the legal description for a parcel."""
        conn = self.connect()
        conn.execute("""
            UPDATE parcels
            SET legal_description = ?, updated_at = CURRENT_TIMESTAMP
            WHERE folio = ?
        """, [legal_description, folio])

    def update_flood_data(self, folio: str, flood_data: Dict[str, Any]):
        """Update flood zone information for a parcel."""
        conn = self.connect()
        
        # Ensure columns exist
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_zone VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_zone_subtype VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_risk_level VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_insurance_required BOOLEAN")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_base_elevation FLOAT")
        
        conn.execute("""
            UPDATE parcels SET
                flood_zone = ?,
                flood_zone_subtype = ?,
                flood_risk_level = ?,
                flood_insurance_required = ?,
                flood_base_elevation = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE folio = ?
        """, [
            flood_data.get("flood_zone"),
            flood_data.get("zone_subtype"),
            flood_data.get("risk_level"),
            flood_data.get("insurance_required"),
            flood_data.get("static_bfe"),
            folio
        ])
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS latitude DOUBLE")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS longitude DOUBLE")

    def update_parcel_coordinates(self, parcel_id: str, latitude: float, longitude: float):
        """Update parcel lat/lon."""
        if parcel_id is None or latitude is None or longitude is None:
            return
        self.ensure_geocode_columns()
        conn = self.connect()
        conn.execute(
            """
            UPDATE parcels
            SET latitude = ?, longitude = ?, updated_at = CURRENT_TIMESTAMP
            WHERE parcel_id = ? OR folio = ?
            """,
            [latitude, longitude, parcel_id, parcel_id],
        )

    def create_chain_tables(self):
        """Create tables for chain of title and encumbrances."""
        conn = self.connect()

        # Legal variations table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS legal_variations (
                id INTEGER PRIMARY KEY,
                folio VARCHAR,
                variation_text VARCHAR,
                source_instrument VARCHAR,
                source_type VARCHAR,
                is_canonical BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create sequences
        conn.execute("CREATE SEQUENCE IF NOT EXISTS chain_id_seq")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS encumbrance_id_seq")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS market_id_seq")

        # Chain of Title table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chain_of_title (
                id INTEGER PRIMARY KEY DEFAULT nextval('chain_id_seq'),
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Encumbrances table (enhanced)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS encumbrances (
                id INTEGER PRIMARY KEY DEFAULT nextval('encumbrance_id_seq'),
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
                
                -- Party 2 Resolution Fields
                party2_resolution_method VARCHAR,  -- 'cqid_326', 'ocr_extraction', NULL if original
                is_self_transfer BOOLEAN DEFAULT FALSE,
                self_transfer_type VARCHAR,  -- 'exact_match', 'trust_transfer', 'name_variation'
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_joined BOOLEAN DEFAULT FALSE,
                is_inferred BOOLEAN DEFAULT FALSE
            )
        """)

        # Migration: Add columns if not exists
        conn.execute("ALTER TABLE encumbrances ADD COLUMN IF NOT EXISTS is_joined BOOLEAN DEFAULT FALSE")
        conn.execute("ALTER TABLE encumbrances ADD COLUMN IF NOT EXISTS is_inferred BOOLEAN DEFAULT FALSE")

        # Market data table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY DEFAULT nextval('market_id_seq'),
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

        print("Chain of title tables created successfully")

    def save_legal_variation(self, folio: str, variation_text: str,
                             source_instrument: str, source_type: str,
                             is_canonical: bool = False):
        """Save a legal description variation."""
        conn = self.connect()

        # Check if already exists
        existing = conn.execute("""
            SELECT id FROM legal_variations
            WHERE folio = ? AND variation_text = ?
        """, [folio, variation_text]).fetchone()

        if not existing:
            conn.execute("""
                INSERT INTO legal_variations (folio, variation_text, source_instrument, source_type, is_canonical)
                VALUES (?, ?, ?, ?, ?)
            """, [folio, variation_text, source_instrument, source_type, is_canonical])

    def save_document(self, folio: str, doc_data: Dict[str, Any]) -> int:
        """
        Save a document to the documents table.
        """
        conn = self.connect()

        # Migration: Add new ORI API fields if they don't exist
        conn.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS sales_price FLOAT")
        conn.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS page_count INTEGER")
        conn.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS ori_uuid VARCHAR")
        conn.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS ori_id VARCHAR")
        conn.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS book_type VARCHAR")

        # Check if exists by instrument number
        inst = doc_data.get("instrument_number")
        existing = None
        if inst:
            existing = conn.execute("""
                SELECT id FROM documents 
                WHERE folio = ? AND instrument_number = ?
            """, [folio, inst]).fetchone()
        
        if existing:
            # Update file_path, ocr_text, and Party 2 resolution data if provided
            updates = []
            params = []
            if doc_data.get("file_path"):
                updates.append("file_path = ?")
                params.append(doc_data.get("file_path"))
            if doc_data.get("ocr_text"):
                updates.append("ocr_text = ?")
                params.append(doc_data.get("ocr_text"))
            extracted_data = doc_data.get("extracted_data") or doc_data.get("vision_extracted_data")
            if extracted_data is not None:
                updates.append("extracted_data = ?")
                params.append(json.dumps(extracted_data))
            # Update Party 2 resolution data if provided
            if doc_data.get("party2") and not existing:  # Only update party2 if not already set
                updates.append("party2 = ?")
                params.append(doc_data.get("party2"))
            if doc_data.get("party2_resolution_method"):
                updates.append("party2_resolution_method = ?")
                params.append(doc_data.get("party2_resolution_method"))
            if doc_data.get("is_self_transfer") is not None:
                updates.append("is_self_transfer = ?")
                params.append(doc_data.get("is_self_transfer"))
            if doc_data.get("self_transfer_type"):
                updates.append("self_transfer_type = ?")
                params.append(doc_data.get("self_transfer_type"))

            if updates:
                params.append(existing[0])
                conn.execute(f"UPDATE documents SET {', '.join(updates)} WHERE id = ?", params)
            return existing[0]
            
        conn.execute("""
            INSERT INTO documents (
                folio, case_number, document_type, file_path, ocr_text,
                extracted_data, recording_date, book, page,
                instrument_number, party1, party2, legal_description,
                party2_resolution_method, is_self_transfer, self_transfer_type,
                sales_price, page_count, ori_uuid, ori_id, book_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            folio,
            doc_data.get("case_number"),
            doc_data.get("document_type"),
            doc_data.get("file_path"),
            doc_data.get("ocr_text"),
            json.dumps(
                doc_data.get("extracted_data")
                or doc_data.get("vision_extracted_data")
                or {}
            ),
            doc_data.get("recording_date"),
            doc_data.get("book"),
            doc_data.get("page"),
            doc_data.get("instrument_number"),
            doc_data.get("party1"),
            doc_data.get("party2"),
            doc_data.get("legal_description"),
            doc_data.get("party2_resolution_method"),
            doc_data.get("is_self_transfer", False),
            doc_data.get("self_transfer_type"),
            doc_data.get("sales_price"),
            doc_data.get("page_count"),
            doc_data.get("ori_uuid"),
            doc_data.get("ori_id"),
            doc_data.get("book_type"),
        ])

        # Get the inserted ID (DuckDB compatible)
        result = conn.execute("""
            SELECT id FROM documents
            WHERE folio = ? AND instrument_number = ?
            ORDER BY id DESC LIMIT 1
        """, [folio, doc_data.get("instrument_number")]).fetchone()
        return result[0] if result else 0

    def save_chain_of_title(self, folio: str, chain_data: Dict[str, Any]):
        """
        Save chain of title data for a property.

        Args:
            folio: Property folio
            chain_data: Dict from chain_to_dict()
        """
        conn = self.connect()

        # Schema migrations (idempotent)
        conn.execute("ALTER TABLE chain_of_title ADD COLUMN IF NOT EXISTS link_status VARCHAR")
        conn.execute("ALTER TABLE chain_of_title ADD COLUMN IF NOT EXISTS confidence_score FLOAT")
        conn.execute("ALTER TABLE chain_of_title ADD COLUMN IF NOT EXISTS mrta_status VARCHAR")
        conn.execute("ALTER TABLE chain_of_title ADD COLUMN IF NOT EXISTS years_covered FLOAT")

        # Preserve existing lien survival annotations across chain rebuilds (best-effort).
        prior_survival: dict[str, dict[str, Any]] = {}
        with suppress(Exception):
            rows = conn.execute(
                """
                SELECT instrument, book, page, survival_status, is_joined, is_inferred
                FROM encumbrances
                WHERE folio = ?
                """,
                [folio],
            ).fetchall()
            cols = [desc[0] for desc in conn.description]
            for row in rows:
                rec = dict(zip(cols, row, strict=True))
                inst = (rec.get("instrument") or "").strip()
                book = (rec.get("book") or "").strip()
                page = (rec.get("page") or "").strip()
                if inst:
                    prior_survival[f"INST:{inst}"] = rec
                if book and page:
                    prior_survival[f"BKPG:{book}/{page}"] = rec

        # Delete existing chain data for this folio
        conn.execute("DELETE FROM chain_of_title WHERE folio = ?", [folio])
        conn.execute("DELETE FROM encumbrances WHERE folio = ?", [folio])

        # Insert ownership periods
        mrta_status = chain_data.get("mrta_status")
        years_covered = chain_data.get("years_covered")

        for period in chain_data.get("ownership_timeline", []):
            # Insert chain record and get the ID using RETURNING clause
            result = conn.execute("""
                INSERT INTO chain_of_title (
                    folio, owner_name, acquired_from, acquisition_date,
                    disposition_date, acquisition_instrument, acquisition_doc_type,
                    acquisition_price, link_status, confidence_score,
                    mrta_status, years_covered
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
            """, [
                folio,
                period.get("owner"),
                period.get("acquired_from"),
                period.get("acquisition_date"),
                period.get("disposition_date"),
                period.get("acquisition_instrument"),
                period.get("acquisition_doc_type"),
                period.get("acquisition_price"),
                period.get("link_status"),
                period.get("confidence_score"),
                mrta_status,
                years_covered
            ])

            # Get the chain period ID from the RETURNING clause
            chain_id = result.fetchone()[0]

            # Insert encumbrances for this period
            for enc in period.get("encumbrances", []):
                # Re-apply prior survival status if the new record doesn't have one yet.
                survival_status = enc.get("survival_status")
                is_joined = enc.get("is_joined")
                is_inferred = enc.get("is_inferred")

                inst = (enc.get("instrument") or "").strip()
                book = (enc.get("book") or "").strip()
                page = (enc.get("page") or "").strip()
                match = None
                if inst:
                    match = prior_survival.get(f"INST:{inst}")
                if match is None and book and page:
                    match = prior_survival.get(f"BKPG:{book}/{page}")

                if match:
                    if not survival_status:
                        survival_status = match.get("survival_status")
                    if is_joined is None:
                        is_joined = match.get("is_joined")
                    if is_inferred is None:
                        is_inferred = match.get("is_inferred")

                conn.execute("""
                    INSERT INTO encumbrances (
                        folio, chain_period_id, encumbrance_type, creditor,
                        debtor, amount, amount_confidence, amount_flags, recording_date,
                        instrument, book, page, is_satisfied, satisfaction_instrument,
                        satisfaction_date, survival_status, is_joined, is_inferred,
                        party2_resolution_method, is_self_transfer, self_transfer_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    folio,
                    chain_id,
                    enc.get("type"),
                    enc.get("creditor"),
                    enc.get("debtor"), # Now capturing debtor
                    enc.get("amount"),
                    enc.get("amount_confidence", "HIGH"),
                    str(enc.get("amount_flags", [])),
                    enc.get("recording_date"),
                    enc.get("instrument"),
                    enc.get("book"),
                    enc.get("page"),
                    enc.get("is_satisfied", False),
                    enc.get("satisfaction_instrument"),
                    enc.get("satisfaction_date"),
                    survival_status,
                    bool(is_joined) if is_joined is not None else False,
                    bool(is_inferred) if is_inferred is not None else False,
                    enc.get("party2_resolution_method"),
                    enc.get("is_self_transfer", False),
                    enc.get("self_transfer_type"),
                ])

        # Chain/encumbrances changed; ensure lien survival can be re-run even if it was previously marked complete.
        try:
            conn.execute(
                "UPDATE auctions SET needs_lien_survival = TRUE WHERE parcel_id = ? OR folio = ?",
                [folio, folio],
            )
        except Exception as exc:
            logger.warning(f"Failed to mark needs_lien_survival for {folio}: {exc}")

    def update_encumbrance_survival(
        self,
        encumbrance_id: int,
        status: str,
        is_joined: bool | None = None,
        is_inferred: bool | None = None,
    ):
        """Update survival status of an encumbrance."""
        conn = self.connect()
        updates = ["survival_status = ?"]
        params = [status]
        
        if is_joined is not None:
            updates.append("is_joined = ?")
            params.append(is_joined)
        
        if is_inferred is not None:
            updates.append("is_inferred = ?")
            params.append(is_inferred)
            
        params.append(encumbrance_id)
        sql = f"UPDATE encumbrances SET {', '.join(updates)} WHERE id = ?"
        conn.execute(sql, params)

    def encumbrance_exists(self, folio: str, book: str, page: str) -> bool:
        """Check if an encumbrance with the given book/page already exists for a folio."""
        conn = self.connect()
        result = conn.execute(
            "SELECT 1 FROM encumbrances WHERE folio = ? AND book = ? AND page = ? LIMIT 1",
            [folio, book, page]
        ).fetchone()
        return result is not None

    def insert_encumbrance(
        self,
        folio: str,
        encumbrance_type: str,
        creditor: str | None = None,
        amount: float | None = None,
        recording_date: str | None = None,
        book: str | None = None,
        page: str | None = None,
        instrument: str | None = None,
        survival_status: str | None = None,
        chain_period_id: int | None = None,
        is_joined: bool = False,
        is_inferred: bool = False,
    ) -> int:
        """
        Insert a single encumbrance record.
        Returns the ID of the inserted encumbrance.
        """
        conn = self.connect()
        result = conn.execute("""
            INSERT INTO encumbrances (
                folio, chain_period_id, encumbrance_type, creditor,
                amount, recording_date, instrument, book, page,
                survival_status, is_joined, is_inferred
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
        """, [
            folio,
            chain_period_id,
            encumbrance_type,
            creditor,
            amount,
            recording_date,
            instrument,
            book,
            page,
            survival_status,
            is_joined,
            is_inferred
        ])
        return result.fetchone()[0]

    def get_legal_description(self, parcel_id: str) -> Optional[str]:
        """Get legal description for a parcel."""
        conn = self.connect()
        # Try parcels table first, then auctions as fallback (though auctions usually stores it in parcels table during ingest)
        row = conn.execute(
            "SELECT legal_description FROM parcels WHERE parcel_id = ?",
            [parcel_id]
        ).fetchone()
        return row[0] if row else None

    def get_chain_of_title(self, folio: str) -> Dict[str, Any]:
        """
        Get chain of title for a property.

        Args:
            folio: Property folio

        Returns:
            Chain of title data
        """
        conn = self.connect()

        # Get ownership periods
        periods = conn.execute("""
            SELECT * FROM chain_of_title
            WHERE folio = ?
            ORDER BY acquisition_date
        """, [folio]).fetchall()

        columns = [desc[0] for desc in conn.description]
        ownership_timeline = []

        for row in periods:
            period = dict(zip(columns, row, strict=True))
            period_id = period["id"]

            # Get encumbrances for this period
            encumbrances = conn.execute("""
                SELECT * FROM encumbrances
                WHERE chain_period_id = ?
                ORDER BY recording_date
            """, [period_id]).fetchall()

            enc_columns = [desc[0] for desc in conn.description]
            period["encumbrances"] = [dict(zip(enc_columns, e, strict=True)) for e in encumbrances]

            ownership_timeline.append(period)

        return {
            "folio": folio,
            "ownership_timeline": ownership_timeline,
            "current_owner": ownership_timeline[-1]["owner_name"] if ownership_timeline else None,
            "total_transfers": len(ownership_timeline)
        }

    def create_sources_table(self):
        """Create table for tracking data sources."""
        conn = self.connect()
        conn.execute("CREATE SEQUENCE IF NOT EXISTS property_sources_id_seq")
        with suppress(Exception):
            conn.execute("SELECT nextval('property_sources_id_seq')")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS property_sources (
                id INTEGER PRIMARY KEY DEFAULT nextval('property_sources_id_seq'),
                folio VARCHAR,
                source_name VARCHAR,
                url VARCHAR,
                description VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(folio, url)
            )
        """)

        with suppress(Exception):
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info('property_sources')").fetchall()
            }
            if "source_name" not in columns:
                conn.execute("ALTER TABLE property_sources ADD COLUMN source_name VARCHAR")
                if "source_type" in columns:
                    conn.execute("""
                        UPDATE property_sources
                        SET source_name = source_type
                        WHERE source_name IS NULL
                    """)
            if "description" not in columns:
                conn.execute("ALTER TABLE property_sources ADD COLUMN description VARCHAR")

    def save_market_data(self, folio: str, source: str, data: Dict[str, Any],
                         screenshot_path: Optional[str] = None):
        """Save market data from Zillow/Realtor."""
        conn = self.connect()

        conn.execute("""
            INSERT INTO market_data (
                folio, source, capture_date, listing_status, list_price,
                zestimate, rent_estimate, hoa_monthly, days_on_market,
                price_history, raw_json, screenshot_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            folio,
            source,
            today_local(),
            data.get("listing_status"),
            data.get("list_price") or data.get("price"),
            data.get("zestimate"),
            data.get("rent_zestimate") or data.get("rent_estimate"),
            data.get("hoa_fee") or data.get("hoa_monthly"),
            data.get("days_on_market"),
            json.dumps(data.get("price_history", [])),
            json.dumps(data),
            screenshot_path
        ])


    def create_sales_history_table(self):
        """Create sales_history table for storing deeds/transactions from HCPA."""
        conn = self.connect()

        # Create sequence for auto-increment
        conn.execute("CREATE SEQUENCE IF NOT EXISTS sales_history_seq")

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

        # Create index for faster lookups
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sales_history_folio
            ON sales_history(folio)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sales_history_strap
            ON sales_history(strap)
        """)

        print("sales_history table created successfully")

    def get_property(self, folio: str) -> Optional[Dict[str, Any]]:
        """Retrieve parcel data by folio as a dict.

        Returns dict instead of Property because parcels table doesn't have
        case_number which is required for Property model.
        """
        conn = self.connect()
        row = conn.execute(
            "SELECT * FROM parcels WHERE folio = ?", [folio]
        ).fetchone()

        if not row:
            return None

        cols = [desc[0] for desc in conn.description]
        data = dict(zip(cols, row, strict=False))

        return {
            "parcel_id": data.get("folio"),
            "address": data.get("address"),
            "owner_name": data.get("owner_name"),
            "legal_description": data.get("legal_description"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "year_built": data.get("year_built"),
            "beds": data.get("beds"),
            "baths": data.get("baths"),
            "heated_area": data.get("heated_area"),
        }

    def get_encumbrances_by_folio(self, folio: str) -> List[Dict[str, Any]]:
        """Get all encumbrances for a folio."""
        conn = self.connect()
        rows = conn.execute("""
            SELECT * FROM encumbrances
            WHERE folio = ?
            ORDER BY recording_date DESC
        """, [folio]).fetchall()
        
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def get_auction_by_case(self, case_number: str) -> Optional[Dict[str, Any]]:
        """Get an auction by case number."""
        conn = self.connect()
        row = conn.execute("""
            SELECT * FROM auctions
            WHERE case_number = ?
        """, [case_number]).fetchone()
        
        if not row:
            return None
            
        columns = [desc[0] for desc in conn.description]
        return dict(zip(columns, row, strict=True))

    def save_source(self, folio: str, source_name: str, url: str, description: str = ""):
        """
        Save a data source URL for a property.
        
        Args:
            folio: Property folio/ID
            source_name: Name of source (e.g. "Permits", "Tax Deed")
            url: The URL used or found
            description: Optional description
        """
        conn = self.connect()
        
        # Ensure table exists
        self.create_sources_table()
        
        exists = conn.execute(
            """
            SELECT 1 FROM property_sources
            WHERE folio = ? AND url = ?
            """,
            [folio, url],
        ).fetchone()
        if exists:
            conn.execute(
                """
                UPDATE property_sources
                SET source_name = ?, description = ?, created_at = ?
                WHERE folio = ? AND url = ?
                """,
                [source_name, description, now_utc_naive(), folio, url],
            )
        else:
            conn.execute(
                """
                INSERT INTO property_sources (folio, source_name, url, description, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                [folio, source_name, url, description, now_utc_naive()],
            )
        
    def get_sources(self, folio: str) -> List[Dict[str, Any]]:
        """Get all sources for a property."""
        conn = self.connect()
        
        # Ensure table exists
        self.create_sources_table()
        
        results = conn.execute("""
            SELECT * FROM property_sources
            WHERE folio = ?
            ORDER BY created_at DESC
        """, [folio]).fetchall()

        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in results]

    def save_sales_history(self, folio: str, strap: str, sales: List[Dict]):
        """
        Save sales history records from HCPA GIS scraper.

        Args:
            folio: Numeric folio (e.g., '1895490296')
            strap: Strap/parcel ID (e.g., '192918863000000053150A')
            sales: List of sale dicts from hcpa_gis_scraper
        """
        conn = self.connect()

        # Ensure table exists
        self.create_sales_history_table()

        for sale in sales:
            try:
                # Parse sale price
                price_str = sale.get('sale_price', '').replace('$', '').replace(',', '')
                try:
                    sale_price = float(price_str) if price_str else None
                except (ValueError, TypeError):
                    sale_price = None

                # Use INSERT with ON CONFLICT for DuckDB
                conn.execute("""
                    INSERT INTO sales_history (
                        folio, strap, book, page, instrument,
                        sale_date, doc_type, qualified, vacant_improved,
                        sale_price, ori_link
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (folio, book, page) DO UPDATE SET
                        instrument = EXCLUDED.instrument,
                        sale_date = EXCLUDED.sale_date,
                        doc_type = EXCLUDED.doc_type,
                        qualified = EXCLUDED.qualified,
                        vacant_improved = EXCLUDED.vacant_improved,
                        sale_price = EXCLUDED.sale_price,
                        ori_link = EXCLUDED.ori_link
                """, [
                    folio,
                    strap,
                    sale.get('book'),
                    sale.get('page'),
                    sale.get('instrument'),
                    sale.get('date'),
                    sale.get('doc_type'),
                    sale.get('qualified'),
                    sale.get('vacant_improved'),
                    sale_price,
                    sale.get('book_page_link')
                ])
            except Exception as e:
                print(f"Error saving sale record: {e}")

        print(f"Saved {len(sales)} sales history records for {folio}")

    def get_sales_history(self, folio: str | None = None, strap: str | None = None) -> List[Dict]:
        """Get sales history for a property by folio or strap."""
        conn = self.connect()

        if folio:
            results = conn.execute("""
                SELECT * FROM sales_history WHERE folio = ?
                ORDER BY sale_date DESC
            """, [folio]).fetchall()
        elif strap:
            results = conn.execute("""
                SELECT * FROM sales_history WHERE strap = ?
                ORDER BY sale_date DESC
            """, [strap]).fetchall()
        else:
            return []

        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in results]

    # ------------------------------------------------------------------
    # Restriction / Easement helpers
    # ------------------------------------------------------------------
    def get_restriction_documents(self, folio: str | None = None) -> List[Dict[str, Any]]:
        """
        Find documents that look like restrictions or easements.

        Searches document_type, OCR text, and legal_description for the keywords
        "easement" or "restriction".
        """
        conn = self.connect()

        conditions = [
            "LOWER(COALESCE(document_type, '')) LIKE '%easement%'",
            "LOWER(COALESCE(document_type, '')) LIKE '%restriction%'",
            "LOWER(COALESCE(ocr_text, '')) LIKE '%easement%'",
            "LOWER(COALESCE(ocr_text, '')) LIKE '%restriction%'",
            "LOWER(COALESCE(legal_description, '')) LIKE '%easement%'",
            "LOWER(COALESCE(legal_description, '')) LIKE '%restriction%'",
        ]

        where_clauses = ["(" + " OR ".join(conditions) + ")"]
        params: list[Any] = []
        if folio:
            where_clauses.append("folio = ?")
            params.append(folio)

        where_sql = " AND ".join(where_clauses)
        rows = conn.execute(
            f"""
            SELECT folio, instrument_number, document_type, recording_date,
                   book, page, file_path, legal_description, ocr_text
            FROM documents
            WHERE {where_sql}
            ORDER BY recording_date NULLS LAST, created_at DESC
            """,
            params,
        ).fetchall()

        if not rows:
            return []
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def folio_has_restrictions(self, folio: str) -> bool:
        """Quick existence check for restriction/easement docs for a folio."""
        conn = self.connect()
        result = conn.execute(
            """
            SELECT COUNT(*) FROM documents
            WHERE folio = ? AND (
                LOWER(COALESCE(document_type, '')) LIKE '%easement%' OR
                LOWER(COALESCE(document_type, '')) LIKE '%restriction%' OR
                LOWER(COALESCE(ocr_text, '')) LIKE '%easement%' OR
                LOWER(COALESCE(ocr_text, '')) LIKE '%restriction%' OR
                LOWER(COALESCE(legal_description, '')) LIKE '%easement%' OR
                LOWER(COALESCE(legal_description, '')) LIKE '%restriction%'
            )
            """,
            [folio],
        ).fetchone()
        return bool(result and result[0] > 0)

    # ------------------------------------------------------------------
    # Tax helpers
    # ------------------------------------------------------------------
    def get_tax_liens(self, folio: str) -> List[Dict[str, Any]]:
        """Return tax-related liens for a folio (document_type starts with 'TAX')."""
        conn = self.connect()
        rows = conn.execute(
            """
            SELECT * FROM liens
            WHERE folio = ? AND UPPER(COALESCE(document_type, '')) LIKE 'TAX%'
            ORDER BY recording_date
            """,
            [folio],
        ).fetchall()
        if not rows:
            return []
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def get_tax_status(self, folio: str) -> Dict[str, Any]:
        """
        Summarize tax liens for a folio.

        Returns:
            {
                "has_tax_liens": bool,
                "total_amount_due": float | None,
                "liens": [ ... ]
            }
        """
        liens = self.get_tax_liens(folio)
        amounts = [lien.get("amount") for lien in liens if lien.get("amount") is not None]
        total_amount = sum(amounts) if amounts else None
        return {
            "has_tax_liens": len(liens) > 0,
            "total_amount_due": total_amount,
            "liens": liens,
        }

    # ------------------------------------------------------------------
    # Pipeline Optimization Helpers (Migrated from PipelineDB)
    # ------------------------------------------------------------------
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute a raw query and return dict results."""
        conn = self.connect()
        results = conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=False)) for row in results]

    def ensure_last_analyzed_column(self):
        """Add last_analyzed_case_number column if missing."""
        conn = self.connect()
        conn.execute(
            "ALTER TABLE parcels ADD COLUMN IF NOT EXISTS last_analyzed_case_number VARCHAR"
        )

    def get_auction_count_by_date(self, auction_date: date) -> int:
        """Get count of auctions we have for a specific date."""
        conn = self.connect()
        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM auctions
            WHERE COALESCE(
                TRY_CAST(auction_date AS DATE),
                CAST(TRY_STRPTIME(CAST(auction_date AS VARCHAR), '%m/%d/%Y') AS DATE),
                CAST(TRY_STRPTIME(CAST(auction_date AS VARCHAR), '%m/%d/%Y %H:%M:%S') AS DATE)
            ) = ?
            """,
            [auction_date],
        ).fetchone()
        return result[0] if result else 0

    def ensure_auction_scrape_log_table(self) -> None:
        """Ensure auction scrape log table exists."""
        conn = self.connect()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_scrape_log (
                auction_date DATE,
                auction_type VARCHAR,
                auction_count INTEGER,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (auction_date, auction_type)
            )
            """
        )

    def was_auction_scraped(self, auction_date: date, auction_type: str) -> bool:
        """Check if a scrape has already been recorded for a date/type."""
        conn = self.connect()
        self.ensure_auction_scrape_log_table()
        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM auction_scrape_log
            WHERE auction_date = ? AND auction_type = ?
            """,
            [auction_date, auction_type],
        ).fetchone()
        return result[0] > 0 if result else False

    def record_auction_scrape(self, auction_date: date, auction_type: str, auction_count: int) -> None:
        """Record an auction scrape attempt, even if zero results."""
        conn = self.connect()
        self.ensure_auction_scrape_log_table()
        conn.execute(
            """
            INSERT INTO auction_scrape_log (auction_date, auction_type, auction_count, scraped_at)
            VALUES (?, ?, ?, NOW())
            ON CONFLICT (auction_date, auction_type)
            DO UPDATE SET auction_count = excluded.auction_count, scraped_at = NOW()
            """,
            [auction_date, auction_type, auction_count],
        )

    def get_auctions_by_date_range(self, start_date: date, end_date: date) -> List[dict]:
        """Fetch auctions within a date range."""
        conn = self.connect()
        query = """
            SELECT
                a.case_number,
                COALESCE(a.parcel_id, a.folio) AS parcel_id,
                COALESCE(a.property_address, p.property_address) AS address,
                a.auction_date
            FROM auctions a
            LEFT JOIN parcels p
                ON p.folio = COALESCE(a.parcel_id, a.folio)
            WHERE COALESCE(
                TRY_CAST(a.auction_date AS DATE),
                CAST(TRY_STRPTIME(CAST(a.auction_date AS VARCHAR), '%m/%d/%Y') AS DATE),
                CAST(TRY_STRPTIME(CAST(a.auction_date AS VARCHAR), '%m/%d/%Y %H:%M:%S') AS DATE)
            ) BETWEEN ? AND ?
        """
        results = conn.execute(query, [start_date, end_date]).fetchall()
        return [
            {
                "case_number": row[0],
                "parcel_id": row[1],
                "address": row[2],
                "auction_date": row[3],
            }
            for row in results
        ]

    def get_auctions_for_processing(
        self,
        start_date: date,
        end_date: date,
        include_failed: bool = False,
        max_retries: int = 3,
        skip_tax_deeds: bool = False,
    ) -> List[dict]:
        """Return auctions to process based on status table and retry policy."""
        conn = self.connect()
        auction_type_filter = ""
        params: list = [start_date, end_date, include_failed, max_retries]
        if skip_tax_deeds:
            auction_type_filter = (
                "AND COALESCE(UPPER(REPLACE(n.auction_type, ' ', '_')), '') != 'TAX_DEED'"
            )

        query = f"""
            WITH normalized AS (
                SELECT
                    a.*,
                    COALESCE(a.parcel_id, a.folio) AS parcel_id_norm,
                    COALESCE(a.property_address, p.property_address) AS address,
                    p.owner_name AS owner_name,
                    p.legal_description AS legal_description,
                    COALESCE(
                        TRY_CAST(a.auction_date AS DATE),
                        CAST(TRY_STRPTIME(CAST(a.auction_date AS VARCHAR), '%m/%d/%Y') AS DATE),
                        CAST(TRY_STRPTIME(CAST(a.auction_date AS VARCHAR), '%m/%d/%Y %H:%M:%S') AS DATE)
                    ) AS auction_date_norm
                FROM auctions a
                LEFT JOIN parcels p
                    ON p.folio = COALESCE(a.parcel_id, a.folio)
            )
            SELECT
                n.case_number,
                n.parcel_id_norm AS parcel_id,
                n.address,
                n.auction_date_norm AS auction_date,
                n.owner_name,
                n.legal_description,
                n.plaintiff,
                n.defendant,
                n.auction_type,
                n.property_address
            FROM normalized n
            LEFT JOIN status s ON s.case_number = n.case_number
            WHERE n.auction_date_norm BETWEEN ? AND ?
              AND COALESCE(s.pipeline_status, 'pending') NOT IN ('completed', 'skipped')
              AND (COALESCE(s.pipeline_status, 'pending') != 'failed' OR ?)
              AND COALESCE(s.retry_count, 0) < ?
              {auction_type_filter}
            ORDER BY n.auction_date_norm, n.case_number
        """
        results = conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=False)) for row in results]

    def folio_has_sales_history(self, folio: str) -> bool:
        """Check if folio has sales history data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM sales_history WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_chain_of_title(self, folio: str) -> bool:
        """Check if folio has chain of title data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM chain_of_title WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_encumbrances(self, folio: str) -> bool:
        """Check if folio has encumbrances."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM encumbrances WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_survival_analysis(self, folio: str) -> bool:
        """Check if folio has survival status set on encumbrances."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM encumbrances WHERE folio = ? AND survival_status IS NOT NULL AND survival_status != 'UNKNOWN'",
            [folio],
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_unanalyzed_encumbrances(self, folio: str) -> bool:
        """Check if folio has encumbrances missing survival status."""
        conn = self.connect()
        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM encumbrances
            WHERE folio = ?
              AND (survival_status IS NULL OR survival_status = 'UNKNOWN')
            """,
            [folio],
        ).fetchone()
        return result[0] > 0 if result else False

    def get_last_analyzed_case(self, folio: str) -> Optional[str]:
        """Get the last analyzed case number for a folio."""
        conn = self.connect()
        self.ensure_last_analyzed_column()
        result = conn.execute(
            "SELECT last_analyzed_case_number FROM parcels WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] if result else None

    def set_last_analyzed_case(self, folio: str, case_number: str):
        """Set the last analyzed case number for a folio."""
        conn = self.connect()
        self.ensure_last_analyzed_column()
        conn.execute(
            "INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [folio]
        )
        conn.execute(
            "UPDATE parcels SET last_analyzed_case_number = ?, updated_at = CURRENT_TIMESTAMP WHERE folio = ?",
            [case_number, folio],
        )

    def folio_has_permits(self, folio: str) -> bool:
        """Check if folio has permit data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM permits WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] > 0 if result else False

    def save_permits(self, folio: str, permits: List[Any]):
        """Save permits to database."""
        conn = self.connect()
        
        saved_count = 0
        for p in permits:
            # Handle both Permit objects and dicts
            if isinstance(p, dict):
                permit_number = p.get("permit_number")
                issue_date = p.get("issue_date")
                status = p.get("status")
                permit_type = p.get("permit_type")
                description = p.get("description")
                contractor = p.get("contractor")
                estimated_cost = p.get("estimated_cost")
                url = p.get("url")
                noc_instrument = p.get("noc_instrument")
            else:
                permit_number = p.permit_number
                issue_date = p.issue_date
                status = p.status
                permit_type = p.permit_type
                description = p.description
                contractor = p.contractor
                estimated_cost = p.estimated_cost
                url = p.url
                noc_instrument = p.noc_instrument

            try:
                conn.execute(
                    """INSERT OR IGNORE INTO permits
                       (folio, permit_number, issue_date, status, permit_type,
                        description, contractor, estimated_cost, url, noc_instrument)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        folio, permit_number, issue_date, status, permit_type,
                        description, contractor, estimated_cost, url, noc_instrument
                    ],
                )
                saved_count += 1
            except Exception as e:
                logger.error(f"Failed to save permit {permit_number}: {e}")
        
        return saved_count

    def folio_has_flood_data(self, folio: str) -> bool:
        """Check if parcel has flood zone data."""
        conn = self.connect()
        try:
            result = conn.execute(
                """SELECT COUNT(*) FROM parcels
                   WHERE folio = ? AND flood_zone IS NOT NULL""",
                [folio],
            ).fetchone()
            return result[0] > 0 if result else False
        except Exception:
            return False

    def save_flood_data(self, folio: str, flood_zone: str, flood_risk: str, insurance_required: bool):
        """Save flood zone data to parcels table."""
        conn = self.connect()
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_zone VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_risk VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_risk_level VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_insurance_required BOOLEAN")

        conn.execute("INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [folio])
        conn.execute(
            """UPDATE parcels SET
               flood_zone = ?, flood_risk = ?, flood_risk_level = ?, flood_insurance_required = ?,
               updated_at = CURRENT_TIMESTAMP
               WHERE folio = ?""",
            [flood_zone, flood_risk, flood_risk, insurance_required, folio],
        )

    def folio_has_realtor_data(self, folio: str) -> bool:
        """Check if folio has realtor.com data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM market_data WHERE folio = ? AND source = 'Realtor'",
            [folio],
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_zillow_data(self, folio: str) -> bool:
        """Check if folio has Zillow data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM market_data WHERE folio = ? AND source = 'Zillow'",
            [folio],
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_market_data(self, folio: str) -> bool:
        """Check if folio has consolidated market data or both Zillow/Realtor."""
        conn = self.connect()
        consolidated = conn.execute(
            "SELECT COUNT(*) FROM market_data WHERE folio = ? AND source = 'Consolidated'",
            [folio],
        ).fetchone()
        if consolidated and consolidated[0] > 0:
            return True
        has_realtor = self.folio_has_realtor_data(folio)
        has_zillow = self.folio_has_zillow_data(folio)
        return has_realtor and has_zillow

    def folio_has_owner_name(self, folio: str) -> bool:
        """Check if folio has owner name in parcels."""
        conn = self.connect()
        result = conn.execute(
            "SELECT owner_name FROM parcels WHERE folio = ?", [folio]
        ).fetchone()
        return result is not None and result[0] is not None

    def folio_has_tax_data(self, folio: str) -> bool:
        """Check if folio has tax data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM liens WHERE folio = ? AND document_type = 'TAX'",
            [folio],
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_sunbiz_data(self, folio: str) -> bool:
        """Check if folio has sunbiz entity data (stored as recent source check or result)."""
        # Note: ScraperStorage dependency avoided here; simplest check is if we marked step complete.
        # But for pipeline "skip" logic, we usually check `needs_sunbiz_search`.
        # This method was in PipelineDB using ScraperStorage.
        # For PropertyDB, we'll check if we have entity parties or if we just rely on flags.
        # Or checking property_sources table?
        # Let's check property_sources for 'sunbiz'
        conn = self.connect()
        try:
             result = conn.execute(
                "SELECT COUNT(*) FROM property_sources WHERE folio = ? AND source_name = 'sunbiz' AND created_at > CURRENT_DATE - INTERVAL 30 DAY", 
                [folio]
             ).fetchone()
             return result[0] > 0 if result else False
        except Exception:
             return False

    def folio_has_homeharvest_data(self, folio: str) -> bool:
        """Check if folio has recent HomeHarvest data (7-day cache)."""
        conn = self.connect()
        try:
            # Match 7-day logic from HomeHarvestService
            result = conn.execute(
                "SELECT COUNT(*) FROM home_harvest WHERE folio = ? AND created_at > CURRENT_DATE - INTERVAL 7 DAY",
                [folio]
            ).fetchone()
            return result[0] > 0 if result else False
        except Exception:
            return False

    # -------------------------------------------------------------------------
    # Status Table Methods (Pipeline Progress Tracking)
    # -------------------------------------------------------------------------

    def _normalize_auction_type(self, auction_type: str | None) -> str | None:
        if not auction_type:
            return None
        normalized = auction_type.strip().upper().replace(" ", "_")
        if normalized == "TAXDEED":
            normalized = "TAX_DEED"
        return normalized

    def ensure_status_table(self) -> None:
        """Create the status table if it doesn't exist."""
        conn = self.connect()
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
        # Legacy migration: rename old `status` column to `pipeline_status` if present.
        try:
            cols = [row[1] for row in conn.execute("PRAGMA table_info('status')").fetchall()]
        except Exception:
            cols = []
        if "status" in cols and "pipeline_status" not in cols:
            conn.execute("ALTER TABLE status RENAME COLUMN status TO pipeline_status")
            cols = [row[1] for row in conn.execute("PRAGMA table_info('status')").fetchall()]
        if "status" in cols and "pipeline_status" in cols:
            conn.execute("UPDATE status SET pipeline_status = COALESCE(pipeline_status, status)")
            try:
                conn.execute("ALTER TABLE status DROP COLUMN status")
            except Exception:
                pass

        conn.execute("CREATE INDEX IF NOT EXISTS idx_status_auction_date ON status(auction_date)")
        if "pipeline_status" in cols:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status_pipeline_status ON status(pipeline_status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status_parcel ON status(parcel_id)")

    def upsert_status(
        self,
        case_number: str,
        parcel_id: str | None = None,
        auction_date: date | None = None,
        auction_type: str | None = None,
    ) -> None:
        """Create or update a status record for a case."""
        self.ensure_status_table()
        auction_type = self._normalize_auction_type(auction_type)
        conn = self.connect()
        # Column renamed to pipeline_status to avoid DuckDB ambiguity with table name 'status'
        # Use NOW() instead of CURRENT_TIMESTAMP to avoid DuckDB parsing issues
        conn.execute(
            """
            INSERT INTO status (case_number, parcel_id, auction_date, auction_type, pipeline_status, step_auction_scraped, updated_at)
            VALUES (?, ?, ?, ?, 'pending', NOW(), NOW())
            ON CONFLICT (case_number) DO UPDATE SET
                parcel_id = COALESCE(excluded.parcel_id, parcel_id),
                auction_date = COALESCE(excluded.auction_date, auction_date),
                auction_type = COALESCE(excluded.auction_type, auction_type),
                step_auction_scraped = COALESCE(step_auction_scraped, excluded.step_auction_scraped),
                updated_at = NOW()
            """,
            [case_number, parcel_id, auction_date, auction_type],
        )

    def mark_status_step_complete(
        self,
        case_number: str,
        step_column: str,
        step_number: int | None = None,
    ) -> None:
        """Mark a specific step as complete for a case."""
        self.ensure_status_table()
        conn = self.connect()

        valid_steps = [
            "step_auction_scraped",
            "step_pdf_downloaded",
            "step_judgment_extracted",
            "step_bulk_enriched",
            "step_homeharvest_enriched",
            "step_hcpa_enriched",
            "step_ori_ingested",
            "step_survival_analyzed",
            "step_permits_checked",
            "step_flood_checked",
            "step_market_fetched",
            "step_tax_checked",
        ]

        if step_column not in valid_steps:
            raise ValueError(f"Invalid step column: {step_column}")

        # Update the step timestamp and current_step
        # First get current status to avoid ambiguous column reference in DuckDB
        current_status = self.get_status_state(case_number)
        new_status = current_status if current_status in ("completed", "skipped") else "processing"

        if step_number:
            conn.execute(
                f"""
                UPDATE status SET
                    {step_column} = NOW(),
                    current_step = CASE WHEN current_step < ? THEN ? ELSE current_step END,
                    pipeline_status = ?,
                    last_error = NULL,
                    error_step = NULL,
                    updated_at = NOW()
                WHERE case_number = ?
                """,
                [step_number, step_number, new_status, case_number],
            )
        else:
            conn.execute(
                f"""
                UPDATE status SET
                    {step_column} = NOW(),
                    pipeline_status = ?,
                    last_error = NULL,
                    error_step = NULL,
                    updated_at = NOW()
                WHERE case_number = ?
                """,
                [new_status, case_number],
            )
        self._maybe_mark_status_completed(case_number)

    def get_status_state(self, case_number: str) -> str:
        """Return current status state for a case (defaults to 'pending')."""
        self.ensure_status_table()
        conn = self.connect()
        result = conn.execute(
            "SELECT pipeline_status FROM status WHERE case_number = ?",
            [case_number],
        ).fetchone()
        if not result or not result[0]:
            return "pending"
        return result[0]

    def _get_applicable_steps(self, auction_type: str | None) -> list[str]:
        """Return status steps required for completion for the given auction type."""
        steps = [
            "step_auction_scraped",
            "step_pdf_downloaded",
            "step_judgment_extracted",
            "step_bulk_enriched",
            "step_homeharvest_enriched",
            "step_hcpa_enriched",
            "step_ori_ingested",
            "step_survival_analyzed",
            "step_permits_checked",
            "step_flood_checked",
            "step_market_fetched",
            "step_tax_checked",
        ]
        normalized = self._normalize_auction_type(auction_type)
        if normalized == "TAX_DEED":
            steps = [s for s in steps if s not in {"step_pdf_downloaded", "step_judgment_extracted"}]
        return steps

    def _maybe_mark_status_completed(self, case_number: str) -> None:
        """Mark status completed if all applicable steps are done."""
        self.ensure_status_table()
        conn = self.connect()
        row = conn.execute(
            """
            SELECT auction_type, pipeline_status,
                   step_auction_scraped, step_pdf_downloaded, step_judgment_extracted,
                   step_bulk_enriched, step_homeharvest_enriched, step_hcpa_enriched,
                   step_ori_ingested, step_survival_analyzed, step_permits_checked,
                   step_flood_checked, step_market_fetched, step_tax_checked
            FROM status
            WHERE case_number = ?
            """,
            [case_number],
        ).fetchone()
        if not row:
            return
        auction_type = row[0]
        status_state = row[1]
        if status_state == "skipped":
            return
        step_values = {
            "step_auction_scraped": row[2],
            "step_pdf_downloaded": row[3],
            "step_judgment_extracted": row[4],
            "step_bulk_enriched": row[5],
            "step_homeharvest_enriched": row[6],
            "step_hcpa_enriched": row[7],
            "step_ori_ingested": row[8],
            "step_survival_analyzed": row[9],
            "step_permits_checked": row[10],
            "step_flood_checked": row[11],
            "step_market_fetched": row[12],
            "step_tax_checked": row[13],
        }
        applicable_steps = self._get_applicable_steps(auction_type)
        if all(step_values.get(step) is not None for step in applicable_steps):
            conn.execute(
                """
                UPDATE status SET
                    pipeline_status = 'completed',
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE case_number = ?
                """,
                [case_number],
            )

    def mark_status_failed(
        self,
        case_number: str,
        error_message: str,
        error_step: int | None = None,
    ) -> None:
        """Mark a case as failed with error details."""
        self.ensure_status_table()
        conn = self.connect()
        conn.execute(
            """
            UPDATE status SET
                pipeline_status = 'failed',
                last_error = ?,
                error_step = ?,
                retry_count = retry_count + 1,
                updated_at = NOW()
            WHERE case_number = ?
            """,
            [error_message, error_step, case_number],
        )

    def mark_status_completed(self, case_number: str) -> None:
        """Mark a case as fully completed."""
        self.ensure_status_table()
        conn = self.connect()
        conn.execute(
            """
            UPDATE status SET
                pipeline_status = 'completed',
                completed_at = NOW(),
                updated_at = NOW()
            WHERE case_number = ?
            """,
            [case_number],
        )

    def mark_status_skipped(self, case_number: str, reason: str = "") -> None:
        """Mark a case as skipped (e.g., invalid parcel_id)."""
        self.ensure_status_table()
        conn = self.connect()
        conn.execute(
            """
            UPDATE status SET
                pipeline_status = 'skipped',
                last_error = ?,
                updated_at = NOW()
            WHERE case_number = ?
            """,
            [reason, case_number],
        )

    def is_status_step_complete(self, case_number: str, step_column: str) -> bool:
        """Check if a specific step is complete for a case."""
        self.ensure_status_table()
        conn = self.connect()
        result = conn.execute(
            f"SELECT {step_column} FROM status WHERE case_number = ?",
            [case_number],
        ).fetchone()
        return result is not None and result[0] is not None

    def get_status_summary(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict:
        """
        Get a summary of pipeline status for a date range.

        Returns a dict with counts and percentages for each status and step.
        """
        self.ensure_status_table()
        conn = self.connect()

        # Build date filter
        date_filter = ""
        params: list = []
        if start_date and end_date:
            date_filter = "WHERE auction_date >= ? AND auction_date <= ?"
            params = [start_date, end_date]
        elif start_date:
            date_filter = "WHERE auction_date >= ?"
            params = [start_date]
        elif end_date:
            date_filter = "WHERE auction_date <= ?"
            params = [end_date]

        # Get total count
        total_result = conn.execute(
            f"SELECT COUNT(*) FROM status {date_filter}", params
        ).fetchone()
        total = total_result[0] if total_result else 0

        # Get counts by status
        status_counts = conn.execute(
            f"""
            SELECT pipeline_status, COUNT(*) as count
            FROM status {date_filter}
            GROUP BY pipeline_status
            """,
            params,
        ).fetchall()

        # Get counts by auction type
        type_counts = conn.execute(
            f"""
            SELECT auction_type, COUNT(*) as count
            FROM status {date_filter}
            GROUP BY auction_type
            """,
            params,
        ).fetchall()

        # Get step completion counts
        step_columns = [
            "step_auction_scraped",
            "step_pdf_downloaded",
            "step_judgment_extracted",
            "step_bulk_enriched",
            "step_homeharvest_enriched",
            "step_hcpa_enriched",
            "step_ori_ingested",
            "step_survival_analyzed",
            "step_permits_checked",
            "step_flood_checked",
            "step_market_fetched",
            "step_tax_checked",
        ]

        step_counts = {}
        for step in step_columns:
            result = conn.execute(
                f"""
                SELECT COUNT(*) FROM status
                {date_filter}
                {"AND" if date_filter else "WHERE"} {step} IS NOT NULL
                """,
                params,
            ).fetchone()
            step_counts[step] = result[0] if result else 0

        # Get recent failures
        failures = conn.execute(
            f"""
            SELECT case_number, error_step, last_error, retry_count
            FROM status
            {date_filter}
            {"AND" if date_filter else "WHERE"} pipeline_status = 'failed'
            ORDER BY updated_at DESC
            LIMIT 10
            """,
            params,
        ).fetchall()

        # Get date range from actual data
        date_range = conn.execute(
            f"""
            SELECT MIN(auction_date), MAX(auction_date)
            FROM status {date_filter}
            """,
            params,
        ).fetchone()

        return {
            "total": total,
            "start_date": start_date or (date_range[0] if date_range else None),
            "end_date": end_date or (date_range[1] if date_range else None),
            "actual_start_date": date_range[0] if date_range else None,
            "actual_end_date": date_range[1] if date_range else None,
            "by_status": {row[0] or "pending": row[1] for row in status_counts},
            "by_type": {row[0] or "unknown": row[1] for row in type_counts},
            "step_counts": step_counts,
            "failures": [
                {
                    "case_number": row[0],
                    "error_step": row[1],
                    "last_error": row[2],
                    "retry_count": row[3],
                }
                for row in failures
            ],
        }

    def get_failed_cases(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        max_retries: int = 3,
    ) -> list[dict]:
        """Get failed cases that haven't exceeded max retries."""
        self.ensure_status_table()
        conn = self.connect()

        date_filter = ""
        params: list = [max_retries]
        if start_date and end_date:
            date_filter = "AND auction_date >= ? AND auction_date <= ?"
            params.extend([start_date, end_date])

        results = conn.execute(
            f"""
            SELECT case_number, parcel_id, auction_date, error_step, last_error, retry_count
            FROM status
            WHERE pipeline_status = 'failed' AND retry_count < ?
            {date_filter}
            ORDER BY auction_date
            """,
            params,
        ).fetchall()

        return [
            {
                "case_number": row[0],
                "parcel_id": row[1],
                "auction_date": row[2],
                "error_step": row[3],
                "last_error": row[4],
                "retry_count": row[5],
            }
            for row in results
        ]

    def backfill_status_steps(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        """Backfill status step timestamps based on existing data."""
        self.ensure_status_table()
        conn = self.connect()

        def table_exists(table_name: str) -> bool:
            result = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result[0] > 0 if result else False

        # Ensure columns used for backfill exist when optional migrations haven't run.
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS bulk_folio VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS raw_legal1 VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_zone VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS tax_status VARCHAR")

        date_clause = ""
        params: list = []
        if start_date:
            date_clause += " AND s.auction_date >= ?"
            params.append(start_date)
        if end_date:
            date_clause += " AND s.auction_date <= ?"
            params.append(end_date)

        # Step 1: Auction scraped (default for known cases)
        conn.execute(
            f"""
            UPDATE status s SET step_auction_scraped = COALESCE(s.step_auction_scraped, s.created_at, NOW())
            WHERE s.step_auction_scraped IS NULL
            {date_clause}
            """,
            params,
        )

        # Step 2: Judgment extracted
        conn.execute(
            f"""
            UPDATE status s SET step_judgment_extracted = NOW()
            FROM auctions a
            WHERE s.case_number = a.case_number
              AND s.step_judgment_extracted IS NULL
              AND a.extracted_judgment_data IS NOT NULL
              {date_clause}
            """,
            params,
        )

        # Step 1 PDF downloaded: if judgment extracted, PDF existed
        conn.execute(
            f"""
            UPDATE status s SET step_pdf_downloaded = NOW()
            WHERE s.step_pdf_downloaded IS NULL
              AND s.step_judgment_extracted IS NOT NULL
              {date_clause}
            """,
            params,
        )

        # Step 3: Bulk enrichment (best-effort using bulk-parcel markers)
        conn.execute(
            f"""
            UPDATE status s SET step_bulk_enriched = NOW()
            FROM auctions a
            JOIN parcels p ON COALESCE(a.parcel_id, a.folio) = p.folio
            WHERE s.case_number = a.case_number
              AND s.step_bulk_enriched IS NULL
              AND (p.bulk_folio IS NOT NULL OR p.raw_legal1 IS NOT NULL)
              {date_clause}
            """,
            params,
        )

        # Step 3.5: HomeHarvest
        if table_exists("home_harvest"):
            conn.execute(
                f"""
                UPDATE status s SET step_homeharvest_enriched = NOW()
                WHERE s.step_homeharvest_enriched IS NULL
                  AND s.case_number IN (
                    SELECT case_number FROM auctions
                    WHERE COALESCE(parcel_id, folio) IN (SELECT DISTINCT folio FROM home_harvest)
                  )
                  {date_clause}
                """,
                params,
            )

        # Step 4: HCPA enrichment (owner_name or sales_history present)
        sales_history_filter = ""
        if table_exists("sales_history"):
            sales_history_filter = "OR COALESCE(a.parcel_id, a.folio) IN (SELECT DISTINCT folio FROM sales_history)"
        conn.execute(
            f"""
            UPDATE status s SET step_hcpa_enriched = NOW()
            WHERE s.step_hcpa_enriched IS NULL
              AND s.case_number IN (
                SELECT a.case_number
                FROM auctions a
                WHERE COALESCE(a.parcel_id, a.folio) IN (
                    SELECT folio FROM parcels WHERE owner_name IS NOT NULL
                )
                {sales_history_filter}
              )
              {date_clause}
            """,
            params,
        )

        # Step 5: ORI ingestion (documents table)
        if table_exists("documents"):
            conn.execute(
                f"""
                UPDATE status s SET step_ori_ingested = NOW()
                WHERE s.step_ori_ingested IS NULL
                  AND s.case_number IN (
                    SELECT case_number FROM auctions
                    WHERE COALESCE(parcel_id, folio) IN (SELECT DISTINCT folio FROM documents)
                  )
                  {date_clause}
                """,
                params,
            )

        # Step 6: Survival analysis (auctions status ANALYZED/FLAGGED)
        conn.execute(
            f"""
            UPDATE status s SET step_survival_analyzed = NOW()
            FROM auctions a
            WHERE s.case_number = a.case_number
              AND s.step_survival_analyzed IS NULL
              AND a.status IN ('ANALYZED', 'FLAGGED')
              {date_clause}
            """,
            params,
        )

        # Step 7: Permits
        if table_exists("permits"):
            conn.execute(
                f"""
                UPDATE status s SET step_permits_checked = NOW()
                WHERE s.step_permits_checked IS NULL
                  AND s.case_number IN (
                    SELECT case_number FROM auctions
                    WHERE COALESCE(parcel_id, folio) IN (SELECT DISTINCT folio FROM permits)
                  )
                  {date_clause}
                """,
                params,
            )

        # Step 8: Flood check
        conn.execute(
            f"""
            UPDATE status s SET step_flood_checked = NOW()
            WHERE s.step_flood_checked IS NULL
              AND s.case_number IN (
                SELECT case_number FROM auctions
                WHERE COALESCE(parcel_id, folio) IN (
                    SELECT folio FROM parcels WHERE flood_zone IS NOT NULL
                )
              )
              {date_clause}
            """,
            params,
        )

        # Step 9: Market data
        if table_exists("market_data"):
            conn.execute(
                f"""
                UPDATE status s SET step_market_fetched = NOW()
                WHERE s.step_market_fetched IS NULL
                  AND s.case_number IN (
                    SELECT case_number FROM auctions
                    WHERE COALESCE(parcel_id, folio) IN (SELECT DISTINCT folio FROM market_data)
                  )
                  {date_clause}
                """,
                params,
            )

        # Step 12: Tax check
        conn.execute(
            f"""
            UPDATE status s SET step_tax_checked = NOW()
            WHERE s.step_tax_checked IS NULL
              AND s.case_number IN (
                SELECT case_number FROM auctions
                WHERE COALESCE(parcel_id, folio) IN (
                    SELECT folio FROM parcels WHERE tax_status IS NOT NULL
                )
              )
              {date_clause}
            """,
            params,
        )

        # Backfill from needs_* flags when present
        step_flag_map = {
            "needs_judgment_extraction": "step_judgment_extracted",
            "needs_hcpa_enrichment": "step_hcpa_enriched",
            "needs_ori_ingestion": "step_ori_ingested",
            "needs_lien_survival": "step_survival_analyzed",
            "needs_permit_check": "step_permits_checked",
            "needs_flood_check": "step_flood_checked",
            "needs_market_data": "step_market_fetched",
            "needs_tax_check": "step_tax_checked",
            "needs_homeharvest_enrichment": "step_homeharvest_enriched",
        }

        for flag, step in step_flag_map.items():
            conn.execute(
                f"""
                UPDATE status s SET {step} = NOW()
                FROM auctions a
                WHERE s.case_number = a.case_number
                  AND s.{step} IS NULL
                  AND a.{flag} = FALSE
                  {date_clause}
                """,
                params,
            )

    def refresh_status_completion_for_range(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        """Recompute completion state for cases in a date range."""
        self.ensure_status_table()
        conn = self.connect()
        date_filter = ""
        params: list = []
        if start_date and end_date:
            date_filter = "WHERE auction_date >= ? AND auction_date <= ?"
            params = [start_date, end_date]
        elif start_date:
            date_filter = "WHERE auction_date >= ?"
            params = [start_date]
        elif end_date:
            date_filter = "WHERE auction_date <= ?"
            params = [end_date]
        rows = conn.execute(
            f"SELECT case_number FROM status {date_filter}",
            params,
        ).fetchall()
        for row in rows:
            self._maybe_mark_status_completed(row[0])

    def initialize_status_from_auctions(self) -> int:
        """
        Populate status table from existing auctions table.
        Returns the number of records created.
        """
        self.ensure_status_table()
        conn = self.connect()

        # Insert missing records from auctions
        conn.execute(
            """
            INSERT INTO status (case_number, parcel_id, auction_date, auction_type, step_auction_scraped)
            SELECT
                a.case_number,
                COALESCE(a.parcel_id, a.folio),
                a.auction_date,
                a.auction_type,
                NOW()
            FROM auctions a
            WHERE a.case_number IS NOT NULL
            AND a.case_number NOT IN (SELECT case_number FROM status)
            """
        )

        # Normalize auction types and backfill missing fields from auctions
        conn.execute(
            """
            UPDATE status SET
                parcel_id = COALESCE(status.parcel_id, a.parcel_id, a.folio),
                auction_date = COALESCE(status.auction_date, a.auction_date),
                auction_type = COALESCE(status.auction_type, a.auction_type),
                step_auction_scraped = COALESCE(status.step_auction_scraped, status.created_at, NOW()),
                updated_at = NOW()
            FROM auctions a
            WHERE status.case_number = a.case_number
            """
        )
        conn.execute(
            """
            UPDATE status SET auction_type = CASE
                WHEN auction_type IS NULL THEN NULL
                WHEN UPPER(REPLACE(auction_type, ' ', '_')) = 'TAXDEED' THEN 'TAX_DEED'
                ELSE UPPER(REPLACE(auction_type, ' ', '_'))
            END
            """
        )

        # Backfill step completion based on existing data
        self.backfill_status_steps()
        self.refresh_status_completion_for_range()

        count_result = conn.execute("SELECT COUNT(*) FROM status").fetchone()
        return count_result[0] if count_result else 0


if __name__ == "__main__":
    # Test database operations
    with PropertyDB() as db:
        # Create new tables
        db.create_chain_tables()
        db.create_sales_history_table()

        # Test query
        pending = db.get_pending_analysis(limit=5)
        print(f"Found {len(pending)} pending auctions")
