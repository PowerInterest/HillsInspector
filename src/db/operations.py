"""
Database operations for property data.
Provides high-level functions for inserting and querying data.
"""
import os
import duckdb
from datetime import date, datetime
from typing import List, Optional, Dict, Any, Any as AnyType
import json

from src.models.property import Property, Lien

class PropertyDB:
    def __init__(self, db_path: Optional[str] = None):
        # Allow overriding via env for test/debug runs
        self.db_path = db_path or os.environ.get("HILLS_DB_PATH", "data/property_master.db")
        self.conn = None
    
    def connect(self):
        """Open database connection."""
        if self.conn is None:
            self.conn = duckdb.connect(self.db_path)
        return self.conn
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def upsert_auction(self, prop: Property) -> int:
        """
        Insert or update an auction property.

        Args:
            prop: Property object from scraper

        Returns:
            Auction ID
        """
        conn = self.connect()

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
                prop.case_number
            ])
            return existing[0]
        # Insert new record
        conn.execute("""
                INSERT INTO auctions (
                    case_number, folio, parcel_id, certificate_number,
                    auction_type, auction_date, property_address,
                    assessed_value, final_judgment_amount, opening_bid,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
            prop.opening_bid
        ])

        # Fetch the new ID
        result = conn.execute(
            "SELECT id FROM auctions WHERE case_number = ?",
            [prop.case_number]
        ).fetchone()

        return result[0] if result else 0
    
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
        
        # Ensure column exists (DuckDB supports IF NOT EXISTS natively)
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS market_analysis_content VARCHAR")

        # Use ON CONFLICT for atomic upsert
        # 1. Try to insert (ignore if exists)
        conn.execute("""
            INSERT OR IGNORE INTO parcels (
                folio, parcel_id, owner_name, property_address,
                city, zip_code, year_built, beds, baths,
                heated_area, assessed_value, image_url, market_analysis_content, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            datetime.now()
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
            datetime.now(),
            folio
        ])
        
        return folio
    
    def get_auctions_by_date(self, auction_date: date) -> List[Dict[str, Any]]:
        """Get all auctions for a specific date."""
        conn = self.connect()
        
        results = conn.execute("""
            SELECT * FROM auctions
            WHERE auction_date = ?
            ORDER BY case_number
        """, [auction_date]).fetchall()
        
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row)) for row in results]
    
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
        return [dict(zip(columns, row)) for row in results]
    
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

        def _parse_date(value):
            """Normalize various date formats to ISO date string."""
            if value is None:
                return None
            if isinstance(value, date):
                return value.isoformat()
            if isinstance(value, datetime):
                return value.date().isoformat()
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return None
                for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                    try:
                        return datetime.strptime(value, fmt).date().isoformat()
                    except ValueError:
                        continue
            return None

        fields = {
            "plaintiff": data.get("plaintiff"),
            "defendant": data.get("defendant"),
            "foreclosure_type": data.get("foreclosure_type"),
            "judgment_date": _parse_date(data.get("judgment_date")),
            "lis_pendens_date": _parse_date(data.get("lis_pendens_date")),
            "foreclosure_sale_date": _parse_date(data.get("foreclosure_sale_date")),
            "total_judgment_amount": data.get("total_judgment_amount"),
            "principal_amount": data.get("principal_amount"),
            "interest_amount": data.get("interest_amount"),
            "attorney_fees": data.get("attorney_fees"),
            "court_costs": data.get("court_costs"),
            "original_mortgage_amount": data.get("original_mortgage_amount"),
            "original_mortgage_date": _parse_date(data.get("original_mortgage_date")),
            "monthly_payment": data.get("monthly_payment"),
            "default_date": _parse_date(data.get("default_date")),
            "extracted_judgment_data": data.get("extracted_judgment_data"),
            "raw_judgment_text": data.get("raw_judgment_text"),
            "judgment_extracted_at": datetime.now(),
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
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            value = value.strip()
            for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
        return None

    def save_liens(self, case_number: str, liens: List[AnyType]):
        """Save identified liens to the database."""
        conn = self.connect()
        
        try:
            # Create sequence and table
            conn.execute("CREATE SEQUENCE IF NOT EXISTS liens_id_seq")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS liens (
                    id INTEGER PRIMARY KEY DEFAULT nextval('liens_id_seq'),
                    case_number VARCHAR,
                    document_type VARCHAR,
                    recording_date DATE,
                    amount DECIMAL(12, 2),
                    grantor VARCHAR,
                    grantee VARCHAR,
                    book VARCHAR,
                    page VARCHAR,
                    instrument_number VARCHAR,
                    is_surviving BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Delete existing liens for this case (replace strategy)
            conn.execute("DELETE FROM liens WHERE case_number = ?", [case_number])
            
            # Insert new liens
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
                    instrument_number = getattr(lien, "instrument_number", None)
                    is_surviving = getattr(lien, "is_surviving", None)
                
                conn.execute("""
                    INSERT INTO liens (
                        case_number, document_type, recording_date, 
                        amount, grantor, grantee, book, page, instrument_number, is_surviving
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    case_number,
                    document_type,
                    rec_date,
                    amount,
                    grantor,
                    grantee,
                    book,
                    page,
                    instrument_number,
                    is_surviving
                ])
            
        except Exception as e:
            print(f"Error in save_liens: {e}")
            raise e

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
        return [dict(zip(columns, r)) for r in rows]

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

        # Drop existing tables to ensure schema update
        conn.execute("DROP TABLE IF EXISTS chain_of_title")
        conn.execute("DROP TABLE IF EXISTS encumbrances")
        conn.execute("DROP TABLE IF EXISTS market_data")
        
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

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
        
        # Check if exists by instrument number
        inst = doc_data.get("instrument_number")
        existing = None
        if inst:
            existing = conn.execute("""
                SELECT id FROM documents 
                WHERE folio = ? AND instrument_number = ?
            """, [folio, inst]).fetchone()
        
        if existing:
            # Update file_path or ocr_text if provided
            updates = []
            params = []
            if doc_data.get("file_path"):
                updates.append("file_path = ?")
                params.append(doc_data.get("file_path"))
            if doc_data.get("ocr_text"):
                updates.append("ocr_text = ?")
                params.append(doc_data.get("ocr_text"))
                
            if updates:
                params.append(existing[0])
                conn.execute(f"UPDATE documents SET {', '.join(updates)} WHERE id = ?", params)
            return existing[0]
            
        conn.execute("""
            INSERT INTO documents (
                folio, case_number, document_type, file_path, ocr_text,
                extracted_data, recording_date, book, page,
                instrument_number, party1, party2, legal_description
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            folio,
            doc_data.get("case_number"),
            doc_data.get("document_type"),
            doc_data.get("file_path"),
            doc_data.get("ocr_text"),
            json.dumps(doc_data.get("extracted_data", {})),
            doc_data.get("recording_date"),
            doc_data.get("book"),
            doc_data.get("page"),
            doc_data.get("instrument_number"),
            doc_data.get("party1"),
            doc_data.get("party2"),
            doc_data.get("legal_description")
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

        # Delete existing chain data for this folio
        conn.execute("DELETE FROM chain_of_title WHERE folio = ?", [folio])
        conn.execute("DELETE FROM encumbrances WHERE folio = ?", [folio])

        # Insert ownership periods
        for period in chain_data.get("ownership_timeline", []):
            # Insert chain record
            conn.execute("""
                INSERT INTO chain_of_title (
                    folio, owner_name, acquired_from, acquisition_date,
                    disposition_date, acquisition_instrument, acquisition_doc_type,
                    acquisition_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                folio,
                period.get("owner"),
                period.get("acquired_from"),
                period.get("acquisition_date"),
                period.get("disposition_date"),
                period.get("acquisition_instrument"),
                period.get("acquisition_doc_type"),
                period.get("acquisition_price")
            ])

            # Get the chain period ID
            chain_id = conn.execute(
                "SELECT currval('chain_id_seq')"
            ).fetchone()[0]

            # Insert encumbrances for this period
            for enc in period.get("encumbrances", []):
                conn.execute("""
                    INSERT INTO encumbrances (
                        folio, chain_period_id, encumbrance_type, creditor,
                        amount, amount_confidence, amount_flags, recording_date,
                        instrument, book, page, is_satisfied, satisfaction_instrument,
                        satisfaction_date, survival_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    folio,
                    chain_id,
                    enc.get("type"),
                    enc.get("creditor"),
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
                    enc.get("survival_status")
                ])

    def update_encumbrance_survival(self, encumbrance_id: int, status: str):
        """Update survival status of an encumbrance."""
        conn = self.connect()
        conn.execute("UPDATE encumbrances SET survival_status = ? WHERE id = ?", [status, encumbrance_id])

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
            period = dict(zip(columns, row))
            period_id = period["id"]

            # Get encumbrances for this period
            encumbrances = conn.execute("""
                SELECT * FROM encumbrances
                WHERE chain_period_id = ?
                ORDER BY recording_date
            """, [period_id]).fetchall()

            enc_columns = [desc[0] for desc in conn.description]
            period["encumbrances"] = [dict(zip(enc_columns, e)) for e in encumbrances]

            ownership_timeline.append(period)

        return {
            "folio": folio,
            "ownership_timeline": ownership_timeline,
            "current_owner": ownership_timeline[-1]["owner_name"] if ownership_timeline else None,
            "total_transfers": len(ownership_timeline)
        }

    def save_market_data(self, folio: str, source: str, data: Dict[str, Any],
                         screenshot_path: Optional[str] = None):
        """Save market data from Zillow/Realtor."""
        conn = self.connect()

        import json
        from datetime import date

        conn.execute("""
            INSERT INTO market_data (
                folio, source, capture_date, listing_status, list_price,
                zestimate, rent_estimate, hoa_monthly, days_on_market,
                price_history, raw_json, screenshot_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            folio,
            source,
            date.today(),
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

    def create_sources_table(self):
        """Create table for tracking data sources."""
        conn = self.connect()
        
        conn.execute("CREATE SEQUENCE IF NOT EXISTS property_sources_id_seq")
        
        # Check if table exists and recreate if needed (for dev)
        # In prod we'd migrate, but here we just want it to work
        try:
            conn.execute("SELECT nextval('property_sources_id_seq')")
        except:
            pass

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
        
        conn.execute("""
            INSERT INTO property_sources (folio, source_name, url, description, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (folio, url) DO UPDATE SET
                source_name = excluded.source_name,
                description = excluded.description,
                created_at = excluded.created_at
        """, [folio, source_name, url, description, datetime.now()])
        
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
        return [dict(zip(columns, row)) for row in results]

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

    def get_sales_history(self, folio: str = None, strap: str = None) -> List[Dict]:
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
        return [dict(zip(columns, row)) for row in results]


if __name__ == "__main__":
    # Test database operations
    with PropertyDB() as db:
        # Create new tables
        db.create_chain_tables()
        db.create_sales_history_table()

        # Test query
        pending = db.get_pending_analysis(limit=5)
        print(f"Found {len(pending)} pending auctions")
