"""
Database operations for property data.
Provides high-level functions for inserting and querying data.
Uses SQLite with WAL mode for concurrent write support.
"""

import os
import re
import sqlite3
import threading
from contextlib import suppress
from datetime import date, datetime
from typing import List, Optional, Dict, Any
import json
from loguru import logger

from src.models.property import Property, Lien
from src.utils.time import now_utc_naive, parse_date, today_local
from src.db.sqlite_paths import resolve_sqlite_db_path_str


class PropertyDB:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or resolve_sqlite_db_path_str()
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
        """Open database connection with SQLite WAL mode."""
        conn = self._get_conn()
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            # Enable WAL mode for concurrent writes
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")

            def _sqlite_normalize_date(date_val):
                if not date_val:
                    return None
                date_str = str(date_val).strip()
                formats = ["%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"]
                for fmt in formats:
                    try:
                        return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                    except ValueError:
                        continue
                return None

            conn.create_function("normalize_date", 1, _sqlite_normalize_date)
            self._local.conn = conn
        self._apply_schema_migrations(conn)
        return conn

    def checkpoint(self) -> None:
        """
        Force WAL checkpoint - flushes all pending writes to the main database file.

        This is critical for data durability. Without checkpointing, all changes
        remain in the WAL file and can be lost if the process crashes.

        Call this after completing each major pipeline step to ensure data is
        persisted to disk.
        """
        conn = self._get_conn()
        if conn is not None:
            try:
                conn.commit()
            except Exception as e:
                logger.warning(f"Commit before WAL checkpoint failed: {e}")
            try:
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except Exception as e:
                logger.warning(f"WAL checkpoint failed: {e}")

    def _safe_exec(self, conn, sql, params=()):
        """Execute SQL ignoring errors (for schema updates)."""
        try:
            conn.execute(sql, params)
        except Exception as e:
            snippet = " ".join(line.strip() for line in str(sql).splitlines()[:3])
            if "duplicate column name" in str(e):
                logger.debug(f"_safe_exec: {e} (sql='{snippet}')")
            else:
                logger.warning(f"_safe_exec failed: {e} (sql='{snippet}', params={params})")

    def _apply_schema_migrations(self, conn) -> None:
        """
        Apply lightweight, idempotent schema migrations for SQLite.
        Uses try/except since SQLite doesn't support ADD COLUMN IF NOT EXISTS.
        """
        if self._schema_migrations_applied or conn is None:
            return

        # Ensure migrations can wait for concurrent writers instead of failing
        # with "database is locked" immediately.
        with suppress(Exception):
            conn.execute("PRAGMA busy_timeout = 5000")

        def table_exists(table_name: str) -> bool:
            try:
                row = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table_name,),
                ).fetchone()
                return row is not None
            except Exception as e:
                logger.warning(f"table_exists({table_name}) failed: {e}")
                return False

        def add_column_if_not_exists(table: str, column: str, col_type: str, default: str | None = None):
            """Helper to add column if it doesn't exist (SQLite compatible)."""
            try:
                if default is not None:
                    self._safe_exec(conn, f"ALTER TABLE {table} ADD COLUMN {column} {col_type} DEFAULT {default}")
                else:
                    self._safe_exec(conn, f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    raise

        if not table_exists("parcels"):
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
                    latitude REAL,
                    longitude REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

        if table_exists("parcels"):
            add_column_if_not_exists("parcels", "tax_status", "TEXT")
            add_column_if_not_exists("parcels", "tax_warrant", "INTEGER")
            add_column_if_not_exists("parcels", "legal_description", "TEXT")
            add_column_if_not_exists("parcels", "judgment_legal_description", "TEXT")
            add_column_if_not_exists("parcels", "last_analyzed_case_number", "TEXT")
            add_column_if_not_exists("parcels", "flood_zone", "TEXT")
            add_column_if_not_exists("parcels", "flood_zone_subtype", "TEXT")
            add_column_if_not_exists("parcels", "flood_risk", "TEXT")
            add_column_if_not_exists("parcels", "flood_risk_level", "TEXT")
            add_column_if_not_exists("parcels", "flood_insurance_required", "INTEGER")
            add_column_if_not_exists("parcels", "flood_base_elevation", "REAL")
            add_column_if_not_exists("parcels", "bulk_folio", "TEXT")
            add_column_if_not_exists("parcels", "raw_legal1", "TEXT")

        if table_exists("encumbrances"):
            add_column_if_not_exists("encumbrances", "is_joined", "INTEGER", "0")
            add_column_if_not_exists("encumbrances", "is_inferred", "INTEGER", "0")

        if not table_exists("market_data"):
            conn.execute("DROP TABLE IF EXISTS market_data")
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

        if table_exists("chain_of_title"):
            add_column_if_not_exists("chain_of_title", "link_status", "TEXT")
            add_column_if_not_exists("chain_of_title", "confidence_score", "REAL")
            add_column_if_not_exists("chain_of_title", "mrta_status", "TEXT")
            add_column_if_not_exists("chain_of_title", "years_covered", "REAL")

        if table_exists("encumbrances"):
            add_column_if_not_exists("encumbrances", "survival_reason", "TEXT")

        if table_exists("auctions"):
            add_column_if_not_exists("auctions", "hcpa_scrape_failed", "INTEGER", "0")
            add_column_if_not_exists("auctions", "hcpa_scrape_error", "TEXT")
            add_column_if_not_exists("auctions", "ori_party_fallback_used", "INTEGER", "0")
            add_column_if_not_exists("auctions", "ori_party_fallback_note", "TEXT")

        if table_exists("status"):
            add_column_if_not_exists("status", "completed_at", "TIMESTAMP")

        # Migrate sales_history unique index to include instrument column.
        # The old index (folio, book, page) caused collisions for HCPA vision-extracted
        # records that have instrument but no book/page — they all collapsed to
        # (folio, '', '') and triggered UNIQUE constraint failures.
        if table_exists("sales_history"):
            # Check if old 3-column index exists (needs upgrade to 4-column)
            old_idx = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_sales_history_unique' LIMIT 1"
            ).fetchone()
            needs_rebuild = False
            if old_idx is None:
                needs_rebuild = True
            elif "instrument" not in (old_idx[0] or ""):
                # Old index exists but doesn't include instrument — drop and rebuild
                conn.execute("DROP INDEX IF EXISTS idx_sales_history_unique")
                needs_rebuild = True

            if needs_rebuild:
                # Remove duplicate rows keeping only the one with the lowest id
                conn.execute("""
                    DELETE FROM sales_history
                    WHERE id NOT IN (
                        SELECT MIN(id)
                        FROM sales_history
                        GROUP BY folio, COALESCE(book, ''), COALESCE(page, ''), COALESCE(instrument, '')
                    )
                """)
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_history_unique
                    ON sales_history(folio, COALESCE(book, ''), COALESCE(page, ''), COALESCE(instrument, ''))
                """)

        # Normalize non-ISO sale_date values in sales_history (e.g. "08/1995" → "1995-08-01")
        if table_exists("sales_history"):
            try:
                # Fix MM/YYYY format
                conn.execute("""
                    UPDATE sales_history
                    SET sale_date = SUBSTR(sale_date, 4, 4) || '-' || SUBSTR(sale_date, 1, 2) || '-01'
                    WHERE sale_date LIKE '__/____'
                """)
                # Fix MM/DD/YYYY format
                conn.execute("""
                    UPDATE sales_history
                    SET sale_date = SUBSTR(sale_date, 7, 4) || '-' || SUBSTR(sale_date, 1, 2) || '-' || SUBSTR(sale_date, 4, 2)
                    WHERE sale_date LIKE '__/__/____'
                """)
            except Exception as e:
                logger.warning(f"Migration: sales_history date normalization failed: {e}")

        # Fix has_valid_parcel_id inconsistency: auctions with empty parcel_id
        # should have has_valid_parcel_id=0
        if table_exists("auctions"):
            try:
                conn.execute("""
                    UPDATE auctions
                    SET has_valid_parcel_id = 0
                    WHERE (parcel_id IS NULL OR parcel_id = '')
                      AND has_valid_parcel_id = 1
                """)
            except Exception as e:
                logger.warning(f"Migration: auctions has_valid_parcel_id fix failed: {e}")

        # Backfill judgment_legal_description from extracted_judgment_data
        # (was blocked by invalid ALTER TABLE IF NOT EXISTS syntax, now fixed)
        if table_exists("parcels") and table_exists("auctions"):
            try:
                conn.execute("""
                    UPDATE parcels
                    SET judgment_legal_description = json_extract(a.extracted_judgment_data, '$.legal_description')
                    FROM auctions a
                    WHERE (parcels.folio = a.parcel_id OR parcels.folio = a.folio)
                      AND a.extracted_judgment_data IS NOT NULL
                      AND parcels.judgment_legal_description IS NULL
                      AND json_extract(a.extracted_judgment_data, '$.legal_description') IS NOT NULL
                """)
            except Exception as e:
                logger.warning(f"Migration: parcels judgment_legal_description backfill failed: {e}")

        # Migrate property_sources from old schema (property_id, source_type,
        # source_url) to new schema (folio, source_name, url, description).
        # SQLite 3.25+ supports ALTER TABLE RENAME COLUMN.
        if table_exists("property_sources"):
            columns = {row[1] for row in conn.execute("PRAGMA table_info('property_sources')").fetchall()}
            if "property_id" in columns and "folio" not in columns:
                with suppress(Exception):
                    conn.execute("ALTER TABLE property_sources RENAME COLUMN property_id TO folio")
            if "source_type" in columns and "source_name" not in columns:
                with suppress(Exception):
                    conn.execute("ALTER TABLE property_sources RENAME COLUMN source_type TO source_name")
            if "source_url" in columns and "url" not in columns:
                with suppress(Exception):
                    conn.execute("ALTER TABLE property_sources RENAME COLUMN source_url TO url")
            if "description" not in columns:
                self._safe_exec(conn, "ALTER TABLE property_sources ADD COLUMN description TEXT")

        # Unique index on documents to prevent duplicate inserts by instrument_number
        if table_exists("documents"):
            with suppress(Exception):
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_folio_instrument
                    ON documents(folio, instrument_number)
                    WHERE instrument_number IS NOT NULL AND instrument_number != ''
                """)
            # Unique index on ori_uuid to prevent duplicates from ORI API results
            with suppress(Exception):
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_ori_uuid
                    ON documents(ori_uuid)
                    WHERE ori_uuid IS NOT NULL AND ori_uuid != ''
                """)
            # Dedup documents with NULL/empty instrument_number using ori_uuid
            idx_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_documents_ori_uuid' LIMIT 1"
            ).fetchone()
            if idx_exists:
                # Remove dupes where ori_uuid is populated but duplicated
                try:
                    conn.execute("""
                        DELETE FROM documents
                        WHERE id NOT IN (
                            SELECT MIN(id)
                            FROM documents
                            WHERE ori_uuid IS NOT NULL AND ori_uuid != ''
                            GROUP BY ori_uuid
                        )
                        AND ori_uuid IS NOT NULL AND ori_uuid != ''
                    """)
                except Exception as e:
                    logger.error(f"Document dedup (ori_uuid) failed: {e}")
                # Remove dupes with NULL instrument AND NULL ori_uuid:
                # dedup on (folio, document_type, recording_date, book, page)
                try:
                    conn.execute("""
                        DELETE FROM documents
                        WHERE id NOT IN (
                            SELECT MIN(id)
                            FROM documents
                            WHERE (instrument_number IS NULL OR instrument_number = '')
                              AND (ori_uuid IS NULL OR ori_uuid = '')
                            GROUP BY folio, COALESCE(document_type, ''),
                                     COALESCE(recording_date, ''),
                                     COALESCE(book, ''), COALESCE(page, '')
                        )
                        AND (instrument_number IS NULL OR instrument_number = '')
                        AND (ori_uuid IS NULL OR ori_uuid = '')
                    """)
                except Exception as e:
                    logger.error(f"Document dedup (composite key) failed: {e}")

        # Migrate ori_search_queue: remove inline UNIQUE(folio, search_type, search_term, search_operator)
        # and replace with a unique index that includes date bounds.
        # This allows the same search term with different date ranges (gap-bounded name searches).
        if table_exists("ori_search_queue"):
            # Check if inline UNIQUE still exists (table SQL contains "UNIQUE(folio")
            tbl_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='ori_search_queue' LIMIT 1"
            ).fetchone()
            if tbl_sql and "UNIQUE(folio" in (tbl_sql[0] or ""):
                try:
                    conn.execute("ALTER TABLE ori_search_queue RENAME TO ori_search_queue_old")
                    conn.execute("""
                        CREATE TABLE ori_search_queue (
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
                            next_retry_at TEXT
                        )
                    """)
                    conn.execute("""
                        INSERT INTO ori_search_queue (
                            id, folio, search_type, search_term, search_operator, priority,
                            status, attempt_count, max_attempts, date_from, date_to,
                            triggered_by_instrument, triggered_by_search_id, result_count,
                            new_documents_found, error_message, queued_at, started_at,
                            completed_at, next_retry_at
                        )
                        SELECT id, folio, search_type, search_term, search_operator, priority,
                               status, attempt_count, max_attempts, date_from, date_to,
                               triggered_by_instrument, triggered_by_search_id, result_count,
                               new_documents_found, error_message, queued_at, started_at,
                               completed_at, next_retry_at
                        FROM ori_search_queue_old
                    """)
                    conn.execute("DROP TABLE ori_search_queue_old")
                    conn.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_ori_search_queue_unique
                        ON ori_search_queue(folio, search_type, search_term, search_operator,
                                            COALESCE(date_from, ''), COALESCE(date_to, ''))
                    """)
                    logger.info("Migration: ori_search_queue UNIQUE constraint updated to include date bounds")
                except Exception as e:
                    logger.warning(f"Migration: ori_search_queue rebuild failed: {e}")
                    # Try to recover if rename happened but create failed
                    if not table_exists("ori_search_queue") and table_exists("ori_search_queue_old"):
                        conn.execute("ALTER TABLE ori_search_queue_old RENAME TO ori_search_queue")
            else:
                # Table already migrated, just ensure the index exists
                with suppress(Exception):
                    conn.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_ori_search_queue_unique
                        ON ori_search_queue(folio, search_type, search_term, search_operator,
                                            COALESCE(date_from, ''), COALESCE(date_to, ''))
                    """)

        # Normalize raw ORI encumbrance types to standard lowercase format
        if table_exists("encumbrances"):
            try:
                conn.execute("""
                    UPDATE encumbrances SET encumbrance_type = 'mortgage'
                    WHERE (UPPER(encumbrance_type) LIKE '%MORTGAGE%' OR UPPER(encumbrance_type) LIKE '%MTG%')
                      AND encumbrance_type != 'mortgage'
                """)
                conn.execute("""
                    UPDATE encumbrances SET encumbrance_type = 'judgment'
                    WHERE (UPPER(encumbrance_type) LIKE '%JUDGMENT%' OR UPPER(encumbrance_type) LIKE '%JUD%')
                      AND encumbrance_type != 'judgment'
                """)
                conn.execute("""
                    UPDATE encumbrances SET encumbrance_type = 'lis_pendens'
                    WHERE (UPPER(encumbrance_type) LIKE '%LIS PENDENS%' OR UPPER(encumbrance_type) LIKE '%(LP)%')
                      AND encumbrance_type != 'lis_pendens'
                """)
                conn.execute("""
                    UPDATE encumbrances SET encumbrance_type = 'lien'
                    WHERE (UPPER(encumbrance_type) LIKE '%(LN)%LIEN%' OR UPPER(encumbrance_type) LIKE '%LIEN%')
                      AND encumbrance_type NOT IN ('lien', 'mortgage', 'judgment', 'lis_pendens')
                """)
                conn.execute("""
                    UPDATE encumbrances SET encumbrance_type = 'satisfaction'
                    WHERE (UPPER(encumbrance_type) LIKE '%SATISFACTION%' OR UPPER(encumbrance_type) LIKE '%SAT%')
                      AND encumbrance_type NOT IN ('satisfaction', 'mortgage', 'judgment', 'lis_pendens', 'lien')
                """)
                conn.execute("""
                    UPDATE encumbrances SET encumbrance_type = 'release'
                    WHERE (UPPER(encumbrance_type) LIKE '%RELEASE%' OR UPPER(encumbrance_type) LIKE '%REL%')
                      AND encumbrance_type NOT IN ('release', 'satisfaction', 'mortgage', 'judgment', 'lis_pendens', 'lien')
                """)
                conn.execute("""
                    UPDATE encumbrances SET encumbrance_type = 'assignment'
                    WHERE (UPPER(encumbrance_type) LIKE '%ASSIGNMENT%' OR UPPER(encumbrance_type) LIKE '%ASG%')
                      AND encumbrance_type NOT IN ('assignment', 'release', 'satisfaction', 'mortgage', 'judgment', 'lis_pendens', 'lien')
                """)
                conn.execute("""
                    UPDATE encumbrances SET encumbrance_type = 'other'
                    WHERE encumbrance_type NOT IN ('mortgage', 'judgment', 'lis_pendens', 'lien',
                                                   'satisfaction', 'release', 'assignment', 'other')
                      AND encumbrance_type IS NOT NULL
                """)
            except Exception as e:
                logger.warning(f"Migration: encumbrance type normalization failed: {e}")

            # SQLite triggers to enforce normalized encumbrance_type values
            try:
                conn.execute("""
                    CREATE TRIGGER IF NOT EXISTS trg_normalize_encumbrance_type_insert
                    BEFORE INSERT ON encumbrances
                    BEGIN
                        SELECT RAISE(ABORT, 'encumbrance_type must be normalized')
                        WHERE NEW.encumbrance_type NOT IN (
                            'mortgage', 'judgment', 'lis_pendens', 'lien',
                            'satisfaction', 'release', 'assignment', 'other'
                        ) AND NEW.encumbrance_type IS NOT NULL;
                    END
                """)
                conn.execute("""
                    CREATE TRIGGER IF NOT EXISTS trg_normalize_encumbrance_type_update
                    BEFORE UPDATE OF encumbrance_type ON encumbrances
                    BEGIN
                        SELECT RAISE(ABORT, 'encumbrance_type must be normalized')
                        WHERE NEW.encumbrance_type NOT IN (
                            'mortgage', 'judgment', 'lis_pendens', 'lien',
                            'satisfaction', 'release', 'assignment', 'other'
                        ) AND NEW.encumbrance_type IS NOT NULL;
                    END
                """)
            except Exception as e:
                logger.warning(f"Migration: encumbrance type triggers failed: {e}")

        # Commit all pending DML from migrations (DELETE dedup, UPDATE backfills).
        # Python sqlite3's default isolation_level="" wraps DML in implicit
        # transactions that only auto-commit before DDL.  If the final migration
        # step was DML (no trailing DDL), changes stay uncommitted without this.
        try:
            conn.commit()
        except Exception as e:
            logger.error(f"Migration commit failed on {self.db_path}: {e}")

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
        except Exception as e:
            logger.debug(f"get_folio_from_strap({strap}) bulk_parcels query failed: {e}")
        try:
            row = conn.execute(
                "SELECT bulk_folio FROM parcels WHERE folio = ? LIMIT 1",
                [strap],
            ).fetchone()
            if row and row[0]:
                return str(row[0])
        except Exception as e:
            logger.debug(f"get_folio_from_strap({strap}) parcels query failed: {e}")
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
            "has_valid_parcel_id",
        ]

        for flag in flags:
            self._safe_exec(conn, f"ALTER TABLE auctions ADD COLUMN {flag} INTEGER DEFAULT 1")

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
                SELECT folio FROM parcels WHERE owner_name IS NOT NULL AND owner_name != ''
            )
        """)

        # Step 5: ORI Ingestion
        # Mark complete if chain_of_title exists in SQLite
        try:
            folios_with_chain = conn.execute("SELECT DISTINCT folio FROM chain_of_title").fetchall()
            if folios_with_chain:
                folio_list = [f[0] for f in folios_with_chain]
                placeholders = ",".join(["?"] * len(folio_list))

                conn.execute(
                    f"""
                    UPDATE auctions
                    SET needs_ori_ingestion = FALSE
                    WHERE parcel_id IN ({placeholders})
                    """,
                    folio_list,
                )
        except Exception as e:
            logger.debug(f"Chain check failed in initialize_pipeline_flags: {e}")

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
                WHERE created_at >= date('now', '-7 days')
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

        logger.info("Pipeline flags initialized and backfilled.")

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
            "needs_homeharvest_enrichment",  # New flag
        }
        if step_flag not in valid_flags:
            raise ValueError(f"Invalid flag name: {step_flag}")

        conn.execute(
            f"""
            UPDATE auctions
            SET {step_flag} = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
        """,
            [case_number],
        )

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
            "needs_homeharvest_enrichment",
        }
        if step_flag not in valid_flags:
            raise ValueError(f"Invalid flag name: {step_flag}")

        conn.execute(
            f"""
            UPDATE auctions
            SET {step_flag} = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE parcel_id = ?
        """,
            [folio],
        )

    def mark_hcpa_scrape_failed(self, case_number: str, error: str) -> None:
        """Record an HCPA scrape failure on the auction row."""
        conn = self.connect()
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
        self._safe_exec(conn, "ALTER TABLE auctions ADD COLUMN has_valid_parcel_id INTEGER DEFAULT 1")

        # Check if auction already exists
        existing = conn.execute("SELECT id FROM auctions WHERE case_number = ?", [prop.case_number]).fetchone()

        if existing:
            # Update existing record
            conn.execute(
                """
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
            """,
                [
                    prop.parcel_id,
                    prop.parcel_id,
                    prop.certificate_number,
                    prop.auction_type,
                    prop.auction_date,
                    prop.address,
                    prop.assessed_value,
                    prop.final_judgment_amount,
                    prop.opening_bid,
                    getattr(prop, "plaintiff", None),
                    getattr(prop, "defendant", None),
                    getattr(prop, "has_valid_parcel_id", True),
                    prop.case_number,
                ],
            )
            return existing[0]
        conn.execute(
            """
                INSERT INTO auctions (
                    case_number, folio, parcel_id, certificate_number,
                    auction_type, auction_date, property_address,
                    assessed_value, final_judgment_amount, opening_bid,
                    plaintiff, defendant, has_valid_parcel_id,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            [
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
                getattr(prop, "plaintiff", None),
                getattr(prop, "defendant", None),
                getattr(prop, "has_valid_parcel_id", True),
            ],
        )

        # Fetch the new ID
        result = conn.execute("SELECT id FROM auctions WHERE case_number = ?", [prop.case_number]).fetchone()

        conn.commit()
        return result[0] if result else 0

    def update_parcel_tax_status(self, folio: str, tax_status: str, tax_warrant: bool):
        """Update tax status and warrant info for a parcel."""
        conn = self.connect()
        conn.execute("INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [folio])
        conn.execute(
            """
            UPDATE parcels 
            SET tax_status = ?, tax_warrant = ?
            WHERE parcel_id = ? OR folio = ?
        """,
            [tax_status, tax_warrant, folio, folio],
        )

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

        # Use ON CONFLICT for atomic upsert
        # 1. Try to insert (ignore if exists)
        conn.execute(
            """
            INSERT OR IGNORE INTO parcels (
                folio, parcel_id, owner_name, property_address,
                city, zip_code, year_built, beds, baths,
                heated_area, assessed_value, image_url, market_analysis_content,
                legal_description, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            [
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
            ],
        )

        # 2. Update (in case it already existed and we have new data)
        conn.execute(
            """
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
        """,
            [
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
                folio,
            ],
        )

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

        # Ensure table exists (no-op after first init)
        self.create_sales_history_table()

        saved_count = 0
        for sale in sales:
            try:
                # Parse sale price - handle both 'price' and 'sale_price' keys
                price_str = str(sale.get("price", sale.get("sale_price", ""))).replace("$", "").replace(",", "")
                try:
                    sale_price = float(price_str) if price_str else None
                except (ValueError, TypeError):
                    sale_price = None

                # Get instrument number
                instrument = sale.get("instrument", "")
                sale_date = sale.get("date")

                # Dedupe: by instrument if available, else by folio+date+price
                if instrument:
                    existing = conn.execute(
                        "SELECT id FROM sales_history WHERE folio = ? AND instrument = ?",
                        [folio, instrument],
                    ).fetchone()
                else:
                    existing = conn.execute(
                        "SELECT id FROM sales_history WHERE folio = ? AND sale_date = ? AND sale_price = ?",
                        [folio, sale_date, sale_price],
                    ).fetchone()

                if existing:
                    # Update existing record
                    existing_id = dict(existing)["id"]
                    conn.execute(
                        """
                        UPDATE sales_history SET
                            sale_date = ?,
                            doc_type = ?,
                            sale_price = ?,
                            grantor = ?,
                            grantee = ?,
                            instrument = COALESCE(NULLIF(?, ''), instrument)
                        WHERE id = ?
                    """,
                        [
                            sale_date,
                            sale.get("deed_type", sale.get("doc_type")),
                            sale_price,
                            sale.get("grantor"),
                            sale.get("grantee"),
                            instrument,
                            existing_id,
                        ],
                    )
                else:
                    # Insert new record
                    conn.execute(
                        """
                        INSERT INTO sales_history (
                            folio, instrument, sale_date, doc_type,
                            sale_price, grantor, grantee
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        [
                            folio,
                            instrument or None,
                            sale_date,
                            sale.get("deed_type", sale.get("doc_type")),
                            sale_price,
                            sale.get("grantor"),
                            sale.get("grantee"),
                        ],
                    )
                saved_count += 1
            except Exception as e:
                logger.warning(f"Error saving HCPA sale record for {folio}: {e}")

        if saved_count > 0:
            logger.info(f"Saved {saved_count} sales history records for {folio}")

    def get_auctions_by_date(self, auction_date: date) -> List[Dict[str, Any]]:
        """Get all auctions for a specific date."""
        conn = self.connect()

        results = conn.execute(
            """
            SELECT * FROM auctions
            WHERE auction_date = ?
            ORDER BY case_number
        """,
            [auction_date],
        ).fetchall()

        return [dict(row) for row in results]

    def get_pending_analysis(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get auctions that need lien analysis."""
        conn = self.connect()

        results = conn.execute(
            """
            SELECT * FROM auctions
            WHERE status = 'PENDING'
            ORDER BY auction_date
            LIMIT ?
        """,
            [limit],
        ).fetchall()

        return [dict(row) for row in results]

    def mark_as_analyzed(self, case_number: str):
        """Mark an auction as analyzed."""
        conn = self.connect()
        conn.execute(
            """
            UPDATE auctions
            SET status = 'ANALYZED', updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
        """,
            [case_number],
        )

    def mark_as_toxic(self, case_number: str, reason: str = ""):
        """Flag an auction as toxic title."""
        conn = self.connect()
        conn.execute(
            """
            UPDATE auctions
            SET is_toxic_title = TRUE, status = 'FLAGGED', updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
        """,
            [case_number],
        )

    def save_judgment_text(self, case_number: str, text: str):
        """Save the OCR'd text of the Final Judgment."""
        conn = self.connect()

        # Ensure column exists (DuckDB supportsnatively)
        self._safe_exec(conn, "ALTER TABLE auctions ADD COLUMN final_judgment_content TEXT")

        conn.execute(
            """
            UPDATE auctions
            SET final_judgment_content = ?, updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
        """,
            [text, case_number],
        )

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
            SET {", ".join(set_parts)}, updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
        """
        conn.execute(sql, params)
        return True

    @staticmethod
    def _parse_recording_date(value: Any) -> Optional[date]:
        """Parse recording_date from various formats."""
        return parse_date(value)

    def save_liens(self, folio: str, liens: List[Any], case_number: str | None = None):
        """Save identified liens to the database.

        Args:
            folio: Property folio (primary key for grouping liens)
            liens: List of Lien objects or dicts
            case_number: Optional case number for reference
        """
        conn = self.connect()

        try:
            # Create table with folio as primary grouping
            conn.execute("""
                CREATE TABLE IF NOT EXISTS liens (
                    id INTEGER PRIMARY KEY,
                    folio TEXT,
                    case_number TEXT,
                    document_type TEXT,
                    recording_date DATE,
                    amount DECIMAL(12, 2),
                    grantor TEXT,
                    grantee TEXT,
                    book TEXT,
                    page TEXT,
                    description TEXT,
                    instrument_number TEXT,
                    survives_foreclosure INTEGER,
                    is_surviving INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Add folio column if it doesn't exist (migration)
            import contextlib

            with contextlib.suppress(Exception):
                self._safe_exec(conn, "ALTER TABLE liens ADD COLUMN folio TEXT")

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
                existing = conn.execute(
                    """
                    SELECT id FROM liens
                    WHERE folio = ? AND document_type = ? AND
                          ((book = ? AND page = ?) OR instrument_number = ?)
                """,
                    [folio, document_type, book, page, instrument_number],
                ).fetchone()

                if existing:
                    # Update existing record
                    conn.execute(
                        """
                        UPDATE liens SET
                            amount = COALESCE(?, amount),
                            grantor = COALESCE(?, grantor),
                            grantee = COALESCE(?, grantee),
                            is_surviving = COALESCE(?, is_surviving)
                        WHERE id = ?
                    """,
                        [amount, grantor, grantee, is_surviving, existing[0]],
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO liens (
                            folio, case_number, document_type, recording_date,
                            amount, grantor, grantee, book, page, description,
                            instrument_number, is_surviving
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        [
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
                            is_surviving,
                        ],
                    )

        except Exception as e:
            logger.error(f"Error in save_liens: {e}")
            raise

    def get_liens_by_case(self, case_number: str) -> List[Dict[str, Any]]:
        """Fetch liens by case number."""
        conn = self.connect()
        rows = conn.execute(
            """
            SELECT * FROM liens
            WHERE case_number = ?
        """,
            [case_number],
        ).fetchall()
        if not rows:
            return []
        return [dict(r) for r in rows]

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
        """Add latitude/longitude to parcels if missing. No-op: columns in CREATE TABLE."""

    def update_legal_description(self, folio: str, legal_description: str):
        """Update (or insert) the legal description for a parcel."""
        conn = self.connect()
        # Ensure row exists first (UPSERT pattern)
        conn.execute("INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [folio])
        conn.execute(
            """
            UPDATE parcels
            SET legal_description = ?, updated_at = CURRENT_TIMESTAMP
            WHERE folio = ?
        """,
            [legal_description, folio],
        )

    def save_hcpa_to_parcel(self, folio: str, hcpa_result: dict):
        """Save HCPA GIS scrape data to the parcels table (UPSERT)."""
        conn = self.connect()
        prop_info = hcpa_result.get("property_info") or {}
        building = hcpa_result.get("building_info") or {}

        address = prop_info.get("site_address")
        year_built = building.get("year_built")
        image_url = hcpa_result.get("image_url")
        legal_desc = hcpa_result.get("legal_description")

        # Guard against non-address strings (e.g. "Mailing Address", owner names)
        if address and not re.search(r'\d', address):
            address = None

        # Ensure row exists
        conn.execute("INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [folio])
        # Update with available HCPA data (COALESCE preserves existing non-null values)
        conn.execute(
            """
            UPDATE parcels SET
                property_address = COALESCE(?, property_address),
                year_built = COALESCE(?, year_built),
                image_url = COALESCE(?, image_url),
                legal_description = COALESCE(?, legal_description),
                updated_at = CURRENT_TIMESTAMP
            WHERE folio = ?
        """,
            [address, year_built, image_url, legal_desc, folio],
        )
        conn.commit()

    def update_flood_data(self, folio: str, flood_data: Dict[str, Any]):
        """Update flood zone information for a parcel."""
        conn = self.connect()

        conn.execute(
            """
            UPDATE parcels SET
                flood_zone = ?,
                flood_zone_subtype = ?,
                flood_risk_level = ?,
                flood_insurance_required = ?,
                flood_base_elevation = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE folio = ?
        """,
            [
                flood_data.get("flood_zone"),
                flood_data.get("zone_subtype"),
                flood_data.get("risk_level"),
                flood_data.get("insurance_required"),
                flood_data.get("static_bfe"),
                folio,
            ],
        )
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
        conn.commit()

    def create_chain_tables(self):
        """Create tables for chain of title and encumbrances."""
        conn = self.connect()

        # Documents table (ORI documents discovered by step4v2)
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

        # Legal variations table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS legal_variations (
                id INTEGER PRIMARY KEY,
                folio TEXT,
                variation_text TEXT,
                source_instrument TEXT,
                source_type TEXT,
                priority INTEGER,
                is_canonical INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Chain of Title table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chain_of_title (
                id INTEGER PRIMARY KEY,
                folio TEXT,
                owner_name TEXT,
                acquired_from TEXT,
                acquisition_date DATE,
                disposition_date DATE,
                acquisition_instrument TEXT,
                acquisition_doc_type TEXT,
                acquisition_price REAL,
                link_status TEXT,
                confidence_score REAL,
                mrta_status TEXT DEFAULT 'pending',
                years_covered REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Encumbrances table (enhanced)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS encumbrances (
                id INTEGER PRIMARY KEY,
                folio TEXT,
                chain_period_id INTEGER,
                encumbrance_type TEXT,
                creditor TEXT,
                debtor TEXT,
                amount REAL,
                amount_confidence TEXT,
                amount_flags TEXT,
                recording_date DATE,
                instrument TEXT,
                book TEXT,
                page TEXT,
                is_satisfied INTEGER DEFAULT 0,
                satisfaction_instrument TEXT,
                satisfaction_date DATE,
                survival_status TEXT,
                survival_reason TEXT,
                party2_resolution_method TEXT,
                is_self_transfer INTEGER DEFAULT 0,
                self_transfer_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_joined INTEGER DEFAULT 0,
                is_inferred INTEGER DEFAULT 0
            )
        """)

        # Market data table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY,
                folio TEXT,
                source TEXT,
                capture_date DATE,
                listing_status TEXT,
                list_price REAL,
                zestimate REAL,
                rent_estimate REAL,
                hoa_monthly REAL,
                days_on_market INTEGER,
                price_history TEXT,
                raw_json TEXT,
                screenshot_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ORI Search Queue table
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
                next_retry_at TEXT
            )
        """)

        # Unique index with date bounds — allows same search term with different date ranges
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ori_search_queue_unique
            ON ori_search_queue(folio, search_type, search_term, search_operator,
                                COALESCE(date_from, ''), COALESCE(date_to, ''))
        """)

        # Linked Identities table
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

        # Property Parties table
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

        # Add unique constraint on legal_variations if missing
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_legal_variations_folio_text
            ON legal_variations(folio, variation_text)
        """)

        logger.info("Chain of title tables created successfully")

    def save_legal_variation(
        self, folio: str, variation_text: str, source_instrument: str, source_type: str, is_canonical: bool = False
    ):
        """Save a legal description variation."""
        conn = self.connect()

        # Check if already exists
        existing = conn.execute(
            """
            SELECT id FROM legal_variations
            WHERE folio = ? AND variation_text = ?
        """,
            [folio, variation_text],
        ).fetchone()

        if not existing:
            conn.execute(
                """
                INSERT INTO legal_variations (folio, variation_text, source_instrument, source_type, is_canonical)
                VALUES (?, ?, ?, ?, ?)
            """,
                [folio, variation_text, source_instrument, source_type, is_canonical],
            )

    def save_document(self, folio: str, doc_data: Dict[str, Any]) -> int:
        """
        Save a document to the documents table.
        """
        conn = self.connect()

        # Check if exists by instrument number, then by ori_uuid
        inst = doc_data.get("instrument_number")
        ori_uuid = doc_data.get("ori_uuid")
        existing = None
        if inst:
            existing = conn.execute(
                "SELECT id FROM documents WHERE folio = ? AND instrument_number = ?",
                [folio, inst],
            ).fetchone()
        if not existing and ori_uuid:
            existing = conn.execute(
                "SELECT id FROM documents WHERE ori_uuid = ?",
                [ori_uuid],
            ).fetchone()

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
            if doc_data.get("party2"):  # Update party2 if provided
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

        conn.execute(
            """
            INSERT INTO documents (
                folio, case_number, document_type, file_path, ocr_text,
                extracted_data, recording_date, book, page,
                instrument_number, party1, party2, legal_description,
                party2_resolution_method, is_self_transfer, self_transfer_type,
                sales_price, page_count, ori_uuid, ori_id, book_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            [
                folio,
                doc_data.get("case_number"),
                doc_data.get("document_type"),
                doc_data.get("file_path"),
                doc_data.get("ocr_text"),
                json.dumps(doc_data.get("extracted_data") or doc_data.get("vision_extracted_data") or {}),
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
            ],
        )

        # Get the inserted ID (DuckDB compatible)
        result = conn.execute(
            """
            SELECT id FROM documents
            WHERE folio = ? AND instrument_number = ?
            ORDER BY id DESC LIMIT 1
        """,
            [folio, doc_data.get("instrument_number")],
        ).fetchone()
        return result[0] if result else 0

    @staticmethod
    def _classify_encumbrance_type(doc_type: str) -> str:
        """Normalize ORI doc type to standard encumbrance type."""
        from src.db.type_normalizer import normalize_encumbrance_type
        return normalize_encumbrance_type(doc_type)

    def save_chain_of_title(self, folio: str, chain_data: Dict[str, Any]):
        """
        Save chain of title data for a property.

        Args:
            folio: Property folio
            chain_data: Dict from chain_to_dict()
        """
        conn = self.connect()



        # Preserve existing lien survival annotations across chain rebuilds (best-effort).
        prior_survival: dict[str, dict[str, Any]] = {}
        try:
            rows = conn.execute(
                """
                SELECT instrument, book, page, survival_status, is_joined, is_inferred
                FROM encumbrances
                WHERE folio = ?
                """,
                [folio],
            ).fetchall()
            # cols = [desc[0] for desc in conn.description]
            for row in rows:
                rec = dict(row)
                inst = (rec.get("instrument") or "").strip()
                book = (rec.get("book") or "").strip()
                page = (rec.get("page") or "").strip()
                if inst:
                    prior_survival[f"INST:{inst}"] = rec
                if book and page:
                    prior_survival[f"BKPG:{book}/{page}"] = rec
        except Exception as e:
            logger.error(f"Failed to preserve prior survival data for {folio}: {e}")

        # Delete existing chain data for this folio
        conn.execute("DELETE FROM chain_of_title WHERE folio = ?", [folio])
        conn.execute("DELETE FROM encumbrances WHERE folio = ?", [folio])

        # Insert ownership periods
        # Compute per-period years_covered and chain-level mrta_status if not provided
        timeline = chain_data.get("ownership_timeline", [])
        mrta_status = chain_data.get("mrta_status")

        if not mrta_status and timeline:
            total_years = 0.0
            for p in timeline:
                acq = p.get("acquisition_date")
                disp = p.get("disposition_date")
                if acq:
                    try:
                        from datetime import UTC as _UTC
                        from datetime import date as _date
                        from datetime import datetime as _dt

                        acq_d = _date.fromisoformat(acq) if isinstance(acq, str) else acq
                        if disp:
                            end_d = _date.fromisoformat(disp) if isinstance(disp, str) else disp
                        else:
                            end_d = _dt.now(tz=_UTC).date()
                        yrs = max(0, (end_d - acq_d).days / 365.25)
                        p["_years_covered"] = yrs
                        total_years += yrs
                    except (ValueError, TypeError):
                        p["_years_covered"] = None
            mrta_status = "complete" if total_years >= 30 else "incomplete"

        for period in timeline:
            per_years = period.pop("_years_covered", None) or chain_data.get("years_covered")
            result = conn.execute(
                """
                INSERT INTO chain_of_title (
                    folio, owner_name, acquired_from, acquisition_date,
                    disposition_date, acquisition_instrument, acquisition_doc_type,
                    acquisition_price, link_status, confidence_score,
                    mrta_status, years_covered
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
            """,
                [
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
                    per_years,
                ],
            )

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

                conn.execute(
                    """
                    INSERT INTO encumbrances (
                        folio, chain_period_id, encumbrance_type, creditor,
                        debtor, amount, amount_confidence, amount_flags, recording_date,
                        instrument, book, page, is_satisfied, satisfaction_instrument,
                        satisfaction_date, survival_status, is_joined, is_inferred,
                        party2_resolution_method, is_self_transfer, self_transfer_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    [
                        folio,
                        chain_id,
                        self._classify_encumbrance_type(enc.get("type", "")),
                        enc.get("creditor"),
                        enc.get("debtor"),  # Now capturing debtor
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
                    ],
                )

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
        survival_reason: str | None = None,
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

        if survival_reason is not None:
            updates.append("survival_reason = ?")
            params.append(survival_reason)

        params.append(encumbrance_id)
        sql = f"UPDATE encumbrances SET {', '.join(updates)} WHERE id = ?"
        conn.execute(sql, params)
        conn.commit()

    def encumbrance_exists(self, folio: str, book: str, page: str) -> bool:
        """Check if an encumbrance with the given book/page already exists for a folio."""
        conn = self.connect()
        result = conn.execute(
            "SELECT 1 FROM encumbrances WHERE folio = ? AND COALESCE(book, '') = COALESCE(?, '') AND COALESCE(page, '') = COALESCE(?, '') LIMIT 1",
            [folio, book, page],
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
        result = conn.execute(
            """
            INSERT INTO encumbrances (
                folio, chain_period_id, encumbrance_type, creditor,
                amount, recording_date, instrument, book, page,
                survival_status, is_joined, is_inferred
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
        """,
            [
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
                is_inferred,
            ],
        )
        return result.fetchone()[0]

    def get_legal_description(self, parcel_id: str) -> Optional[str]:
        """Get legal description for a parcel."""
        conn = self.connect()
        # Try parcels table first, then auctions as fallback (though auctions usually stores it in parcels table during ingest)
        row = conn.execute("SELECT legal_description FROM parcels WHERE parcel_id = ?", [parcel_id]).fetchone()
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
        periods = conn.execute(
            """
            SELECT * FROM chain_of_title
            WHERE folio = ?
            ORDER BY acquisition_date
        """,
            [folio],
        ).fetchall()

        ownership_timeline = []

        for row in periods:
            period = dict(row)
            period_id = period["id"]

            # Get encumbrances for this period
            encumbrances = conn.execute(
                """
                SELECT * FROM encumbrances
                WHERE chain_period_id = ?
                ORDER BY recording_date
            """,
                [period_id],
            ).fetchall()

            period["encumbrances"] = [dict(e) for e in encumbrances]

            ownership_timeline.append(period)

        return {
            "folio": folio,
            "ownership_timeline": ownership_timeline,
            "current_owner": ownership_timeline[-1]["owner_name"] if ownership_timeline else None,
            "total_transfers": len(ownership_timeline),
        }

    def create_sources_table(self):
        """Create table for tracking data sources."""
        conn = self.connect()

        # Migrate old schema (property_id/source_type/source_url) → new (folio/source_name/url)
        columns = {row[1] for row in conn.execute("PRAGMA table_info('property_sources')").fetchall()}
        if columns and "folio" not in columns:
            # Old schema — rebuild (table is typically empty)
            row_count = conn.execute("SELECT COUNT(*) FROM property_sources").fetchone()[0]
            if row_count == 0:
                conn.execute("DROP TABLE property_sources")
            else:
                # Migrate existing data
                self._safe_exec(conn, "ALTER TABLE property_sources ADD COLUMN folio TEXT")
                self._safe_exec(conn, "ALTER TABLE property_sources ADD COLUMN source_name TEXT")
                self._safe_exec(conn, "ALTER TABLE property_sources ADD COLUMN url TEXT")
                self._safe_exec(conn, "ALTER TABLE property_sources ADD COLUMN description TEXT")
                conn.execute(
                    "UPDATE property_sources SET folio = property_id, source_name = source_type, url = source_url WHERE folio IS NULL"
                )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS property_sources (
                id INTEGER PRIMARY KEY,
                folio TEXT,
                source_name TEXT,
                url TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(folio, url)
            )
        """)

    def save_market_data(self, folio: str, source: str, data: Dict[str, Any], screenshot_path: Optional[str] = None):
        """Save market data from Zillow/Realtor."""
        conn = self.connect()

        conn.execute(
            """
            INSERT INTO market_data (
                folio, source, capture_date, listing_status, list_price,
                zestimate, rent_estimate, hoa_monthly, days_on_market,
                price_history, raw_json, screenshot_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            [
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
                screenshot_path,
            ],
        )

    def create_sales_history_table(self):
        """Create sales_history table for storing deeds/transactions from HCPA."""
        conn = self.connect()

        # Create sales_history table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sales_history (
                id INTEGER PRIMARY KEY,
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Expression-based unique index: COALESCE handles NULL values which SQLite
        # treats as distinct in plain UNIQUE constraints, causing duplicate rows.
        # Includes instrument so HCPA vision-extracted records (which have instrument
        # but no book/page) don't all collapse to (folio, '', '') and collide.
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_history_unique
            ON sales_history(folio, COALESCE(book, ''), COALESCE(page, ''), COALESCE(instrument, ''))
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
        row = conn.execute("SELECT * FROM parcels WHERE folio = ?", [folio]).fetchone()

        if not row:
            return None

        # cols = [desc[0] for desc in conn.description]
        data = dict(row)

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

    def get_parcel_by_folio(self, folio: str) -> Optional[Dict[str, Any]]:
        """Get combined parcel data from parcels + bulk_parcels tables.

        Prefers parcels data (HCPA-enriched) over bulk_parcels. Returns
        a dict with address, owner_name, latitude, longitude, etc.
        Returns None if folio is not found in either table.
        """
        conn = self.connect()

        # Primary: parcels table (HCPA-enriched data)
        row = conn.execute("SELECT * FROM parcels WHERE folio = ?", [folio]).fetchone()
        parcel = dict(row) if row else {}

        # Secondary: bulk_parcels table (bulk county data)
        bulk_row = conn.execute("SELECT * FROM bulk_parcels WHERE folio = ? OR strap = ? LIMIT 1", [folio, folio]).fetchone()
        bulk = dict(bulk_row) if bulk_row else {}

        if not parcel and not bulk:
            return None

        # Merge: parcels wins over bulk for every field
        address = parcel.get("property_address") or bulk.get("property_address")
        city = parcel.get("city") or bulk.get("city")
        zip_code = parcel.get("zip_code") or bulk.get("zip_code")
        if address and city:
            full_address = f"{address}, {city}, FL"
            if zip_code:
                full_address += f" {zip_code}"
        elif address:
            full_address = address
        else:
            full_address = None

        return {
            "folio": folio,
            "address": full_address,
            "owner_name": parcel.get("owner_name") or bulk.get("owner_name"),
            "latitude": parcel.get("latitude") or bulk.get("latitude"),
            "longitude": parcel.get("longitude") or bulk.get("longitude"),
            "year_built": parcel.get("year_built") or bulk.get("year_built"),
            "beds": parcel.get("beds") or bulk.get("beds"),
            "baths": parcel.get("baths") or bulk.get("baths"),
            "heated_area": parcel.get("heated_area") or bulk.get("heated_area"),
            "lot_size": parcel.get("lot_size") or bulk.get("lot_size"),
            "assessed_value": parcel.get("assessed_value") or bulk.get("assessed_value"),
            "market_value": parcel.get("market_value") or bulk.get("market_value"),
            "land_use": parcel.get("land_use") or bulk.get("land_use"),
        }

    def get_encumbrances_by_folio(self, folio: str) -> List[Dict[str, Any]]:
        """Get all encumbrances for a folio."""
        conn = self.connect()
        rows = conn.execute(
            """
            SELECT * FROM encumbrances
            WHERE folio = ?
            ORDER BY recording_date DESC
        """,
            [folio],
        ).fetchall()

        return [dict(row) for row in rows]

    def get_auction_by_case(self, case_number: str) -> Optional[Dict[str, Any]]:
        """Get an auction by case number."""
        conn = self.connect()
        row = conn.execute(
            """
            SELECT * FROM auctions
            WHERE case_number = ?
        """,
            [case_number],
        ).fetchone()

        if not row:
            return None

        return dict(row)

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

        results = conn.execute(
            """
            SELECT * FROM property_sources
            WHERE folio = ?
            ORDER BY created_at DESC
        """,
            [folio],
        ).fetchall()

        return [dict(row) for row in results]

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
                price_str = sale.get("sale_price", "").replace("$", "").replace(",", "")
                try:
                    sale_price = float(price_str) if price_str else None
                except (ValueError, TypeError):
                    sale_price = None

                book = sale.get("book")
                page = sale.get("page")

                # Check for existing record using COALESCE to match the unique index
                # (SQLite treats NULLs as distinct in UNIQUE constraints, so we use
                # an expression-based unique index with COALESCE)
                existing = conn.execute(
                    """
                    SELECT id FROM sales_history
                    WHERE folio = ? AND COALESCE(book, '') = ? AND COALESCE(page, '') = ?
                """,
                    [folio, book or "", page or ""],
                ).fetchone()

                if existing:
                    conn.execute(
                        """
                        UPDATE sales_history SET
                            strap = ?,
                            instrument = ?,
                            sale_date = ?,
                            doc_type = ?,
                            qualified = ?,
                            vacant_improved = ?,
                            sale_price = ?,
                            ori_link = ?
                        WHERE id = ?
                    """,
                        [
                            strap,
                            sale.get("instrument"),
                            sale.get("date"),
                            sale.get("doc_type"),
                            sale.get("qualified"),
                            sale.get("vacant_improved"),
                            sale_price,
                            sale.get("book_page_link"),
                            existing[0],
                        ],
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO sales_history (
                            folio, strap, book, page, instrument,
                            sale_date, doc_type, qualified, vacant_improved,
                            sale_price, ori_link
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        [
                            folio,
                            strap,
                            book,
                            page,
                            sale.get("instrument"),
                            sale.get("date"),
                            sale.get("doc_type"),
                            sale.get("qualified"),
                            sale.get("vacant_improved"),
                            sale_price,
                            sale.get("book_page_link"),
                        ],
                    )
            except Exception as e:
                logger.error(
                    f"Failed to save sale record for {folio}: book={book}, page={page}, instrument={sale.get('instrument')}: {e}"
                )

        logger.info(f"Saved {len(sales)} sales history records for {folio}")

    def get_sales_history(self, folio: str | None = None, strap: str | None = None) -> List[Dict]:
        """Get sales history for a property by folio or strap."""
        conn = self.connect()

        if folio:
            results = conn.execute(
                """
                SELECT * FROM sales_history WHERE folio = ?
                ORDER BY sale_date DESC
            """,
                [folio],
            ).fetchall()
        elif strap:
            results = conn.execute(
                """
                SELECT * FROM sales_history WHERE strap = ?
                ORDER BY sale_date DESC
            """,
                [strap],
            ).fetchall()
        else:
            return []

        return [dict(row) for row in results]

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
            ORDER BY (recording_date IS NULL), recording_date ASC, created_at DESC
            """,
            params,
        ).fetchall()

        if not rows:
            return []
        return [dict(row) for row in rows]

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
        return [dict(row) for row in rows]

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
        return [dict(row) for row in results]

    def ensure_last_analyzed_column(self):
        """Add last_analyzed_case_number column if missing. No-op: column added at init."""

    def get_auction_count_by_date(self, auction_date: date) -> int:
        """Get count of auctions we have for a specific date."""
        conn = self.connect()
        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM auctions
            WHERE normalize_date(auction_date) = ?
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
                auction_type TEXT,
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
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (auction_date, auction_type)
            DO UPDATE SET auction_count = excluded.auction_count, scraped_at = CURRENT_TIMESTAMP
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
            WHERE date(a.auction_date) BETWEEN ? AND ?
        """
        results = conn.execute(query, [str(start_date), str(end_date)]).fetchall()
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
        start_date: date | None = None,
        end_date: date | None = None,
        include_failed: bool = False,
        max_retries: int = 3,
        skip_tax_deeds: bool = False,
    ) -> List[dict]:
        """Return auctions to process based on status table and retry policy.

        When start_date/end_date are None, processes all incomplete auctions.
        """
        conn = self.connect()
        auction_type_filter = ""
        if skip_tax_deeds:
            auction_type_filter = "AND COALESCE(UPPER(REPLACE(n.auction_type, ' ', '_')), '') != 'TAX_DEED'"

        # Build date clause conditionally
        date_clause = ""
        params: list = []
        if start_date is not None and end_date is not None:
            date_clause = "AND n.auction_date_norm BETWEEN ? AND ?"
            params.extend([start_date, end_date])
        elif start_date is not None:
            date_clause = "AND n.auction_date_norm >= ?"
            params.append(start_date)
        elif end_date is not None:
            date_clause = "AND n.auction_date_norm <= ?"
            params.append(end_date)

        params.extend([include_failed, max_retries])

        # Retry gating: "processing" auctions are always picked up (they have
        # incomplete steps but haven't been declared failed).  Only "failed"
        # auctions are gated by retry_count — they represent cases where a
        # critical step set pipeline_status='failed'.
        query = f"""
            WITH normalized AS (
                SELECT
                    a.*,
                    COALESCE(a.parcel_id, a.folio) AS parcel_id_norm,
                    COALESCE(a.property_address, p.property_address) AS address,
                    p.owner_name AS owner_name,
                    p.legal_description AS legal_description,
                    normalize_date(a.auction_date) AS auction_date_norm
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
            WHERE COALESCE(s.pipeline_status, 'pending') NOT IN ('completed', 'skipped', 'archived')
              {date_clause}
              AND (COALESCE(s.pipeline_status, 'pending') != 'failed' OR ?)
              AND COALESCE(s.retry_count, 0) < ?
              {auction_type_filter}
            ORDER BY n.auction_date_norm, n.case_number
        """
        results = conn.execute(query, params).fetchall()
        return [dict(row) for row in results]

    def folio_has_sales_history(self, folio: str) -> bool:
        """Check if folio has sales history data."""
        conn = self.connect()
        result = conn.execute("SELECT COUNT(*) FROM sales_history WHERE folio = ?", [folio]).fetchone()
        return result[0] > 0 if result else False

    def folio_has_chain_of_title(self, folio: str) -> bool:
        """Check if folio has chain of title data."""
        conn = self.connect()
        result = conn.execute("SELECT COUNT(*) FROM chain_of_title WHERE folio = ?", [folio]).fetchone()
        return result[0] > 0 if result else False

    def folio_has_encumbrances(self, folio: str) -> bool:
        """Check if folio has encumbrances."""
        conn = self.connect()
        result = conn.execute("SELECT COUNT(*) FROM encumbrances WHERE folio = ?", [folio]).fetchone()
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
        result = conn.execute("SELECT last_analyzed_case_number FROM parcels WHERE folio = ?", [folio]).fetchone()
        return result[0] if result else None

    def set_last_analyzed_case(self, folio: str, case_number: str):
        """Set the last analyzed case number for a folio."""
        conn = self.connect()
        self.ensure_last_analyzed_column()
        conn.execute("INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [folio])
        conn.execute(
            "UPDATE parcels SET last_analyzed_case_number = ?, updated_at = CURRENT_TIMESTAMP WHERE folio = ?",
            [case_number, folio],
        )

    def folio_has_permits(self, folio: str) -> bool:
        """Check if folio has permit data."""
        conn = self.connect()
        result = conn.execute("SELECT COUNT(*) FROM permits WHERE folio = ?", [folio]).fetchone()
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
                        folio,
                        permit_number,
                        issue_date,
                        status,
                        permit_type,
                        description,
                        contractor,
                        estimated_cost,
                        url,
                        noc_instrument,
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
        except Exception as e:
            logger.debug(f"folio_has_flood_data({folio}) query failed: {e}")
            return False

    def save_flood_data(self, folio: str, flood_zone: str, flood_risk: str, insurance_required: bool):
        """Save flood zone data to parcels table."""
        conn = self.connect()

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

    def folio_has_redfin_data(self, folio: str) -> bool:
        """Check if folio has Redfin data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM market_data WHERE folio = ? AND source = 'Redfin'",
            [folio],
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_market_data(self, folio: str) -> bool:
        """Check if folio has consolidated market data, Redfin data, or both Zillow/Realtor."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM market_data WHERE folio = ? AND source IN ('Consolidated', 'Redfin')",
            [folio],
        ).fetchone()
        if result and result[0] > 0:
            return True
        has_realtor = self.folio_has_realtor_data(folio)
        has_zillow = self.folio_has_zillow_data(folio)
        return has_realtor and has_zillow

    def folio_has_owner_name(self, folio: str) -> bool:
        """Check if folio has owner name in parcels."""
        conn = self.connect()
        result = conn.execute("SELECT owner_name FROM parcels WHERE folio = ?", [folio]).fetchone()
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
                "SELECT COUNT(*) FROM property_sources WHERE folio = ? AND source_name = 'sunbiz' AND created_at > date('now', '-30 days')",
                [folio],
            ).fetchone()
            return result[0] > 0 if result else False
        except Exception as e:
            logger.debug(f"folio_has_sunbiz_data({folio}) query failed: {e}")
            return False

    def folio_has_homeharvest_data(self, folio: str) -> bool:
        """Check if folio has recent HomeHarvest data (7-day cache)."""
        conn = self.connect()
        try:
            # Match 7-day logic from HomeHarvestService
            result = conn.execute(
                "SELECT COUNT(*) FROM home_harvest WHERE folio = ? AND created_at > date('now', '-7 days')", [folio]
            ).fetchone()
            return result[0] > 0 if result else False
        except Exception as e:
            logger.debug(f"folio_has_homeharvest_data({folio}) query failed: {e}")
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
                case_number TEXT PRIMARY KEY,
                parcel_id TEXT,
                auction_date DATE,
                auction_type TEXT,
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
                pipeline_status TEXT DEFAULT 'pending',
                last_error TEXT,
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
            with suppress(Exception):
                conn.execute("ALTER TABLE status DROP COLUMN status")

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
        # Use CURRENT_TIMESTAMP instead of CURRENT_TIMESTAMP to avoid DuckDB parsing issues
        conn.execute(
            """
            INSERT INTO status (case_number, parcel_id, auction_date, auction_type, pipeline_status, step_auction_scraped, updated_at)
            VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (case_number) DO UPDATE SET
                parcel_id = COALESCE(excluded.parcel_id, parcel_id),
                auction_date = COALESCE(excluded.auction_date, auction_date),
                auction_type = COALESCE(excluded.auction_type, auction_type),
                step_auction_scraped = COALESCE(step_auction_scraped, excluded.step_auction_scraped),
                updated_at = CURRENT_TIMESTAMP
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
                    {step_column} = CURRENT_TIMESTAMP,
                    current_step = CASE WHEN current_step < ? THEN ? ELSE current_step END,
                    pipeline_status = ?,
                    last_error = NULL,
                    error_step = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE case_number = ?
                """,
                [step_number, step_number, new_status, case_number],
            )
        else:
            conn.execute(
                f"""
                UPDATE status SET
                    {step_column} = CURRENT_TIMESTAMP,
                    pipeline_status = ?,
                    last_error = NULL,
                    error_step = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE case_number = ?
                """,
                [new_status, case_number],
            )
        self._maybe_mark_status_completed(case_number)
        conn.commit()

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

    # All known step columns (used for validation)
    ALL_STEP_COLUMNS: set[str] = {
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
    }

    @staticmethod
    def _get_disabled_steps() -> set[str]:
        """Parse HILLS_DISABLED_STEPS from env. Default: step_market_fetched."""
        raw = os.environ.get("HILLS_DISABLED_STEPS", "step_market_fetched")
        if not raw.strip():
            return set()
        configured = {s.strip() for s in raw.split(",") if s.strip()}
        valid = configured & PropertyDB.ALL_STEP_COLUMNS
        unknown = configured - PropertyDB.ALL_STEP_COLUMNS
        if unknown:
            logger.warning(f"HILLS_DISABLED_STEPS: ignoring unknown step names: {unknown}")
        if valid:
            logger.debug(f"Disabled steps (excluded from completion): {valid}")
        return valid

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
        disabled = self._get_disabled_steps()
        if disabled:
            steps = [s for s in steps if s not in disabled]
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
                    completed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
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
                updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
            """,
            [error_message, error_step, case_number],
        )

    def mark_status_step_failed(
        self,
        case_number: str,
        error_message: str,
        error_step: int | None = None,
    ) -> None:
        """Record a non-critical step error without poisoning global retry state.

        Updates last_error and error_step for debugging visibility, but does NOT
        increment retry_count or change pipeline_status.  This prevents transient
        non-critical failures (FEMA timeout, tax scraper 503, etc.) from blocking
        the auction's progress through critical steps like ORI and survival.

        If pipeline_status is already 'failed', 'completed', or 'skipped', those
        states are preserved — this method never overwrites authoritative status.
        """
        self.ensure_status_table()
        conn = self.connect()
        conn.execute(
            """
            UPDATE status SET
                last_error = ?,
                error_step = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
              AND pipeline_status NOT IN ('failed', 'completed', 'skipped')
            """,
            [error_message, error_step, case_number],
        )

    def mark_status_retriable_error(
        self,
        case_number: str,
        error_message: str,
        error_step: int | None = None,
    ) -> None:
        """Record error + increment retry_count WITHOUT changing pipeline_status.

        Use for step failures that should be retried but shouldn't block other steps.
        Unlike mark_status_failed: does NOT set pipeline_status='failed'.
        Unlike mark_status_step_failed: DOES increment retry_count.

        Once retry_count reaches max_retries, get_auctions_for_processing will
        stop selecting this case — effectively quarantining it without needing
        pipeline_status='failed'.
        """
        self.ensure_status_table()
        conn = self.connect()
        conn.execute(
            """
            UPDATE status SET
                last_error = ?,
                error_step = ?,
                retry_count = retry_count + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
            """,
            [error_message, error_step, case_number],
        )

    def reset_pipeline_status(self, case_number: str) -> None:
        """Reset a 'failed' case back to 'processing' after successful re-processing.

        Only resets if current status is 'failed'. Does not touch completed/skipped/archived.
        """
        self.ensure_status_table()
        conn = self.connect()
        conn.execute(
            """
            UPDATE status SET
                pipeline_status = 'processing',
                last_error = NULL,
                error_step = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ? AND pipeline_status = 'failed'
            """,
            [case_number],
        )

    def mark_status_completed(self, case_number: str) -> None:
        """Mark a case as fully completed."""
        self.ensure_status_table()
        conn = self.connect()
        conn.execute(
            """
            UPDATE status SET
                pipeline_status = 'completed',
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
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
                updated_at = CURRENT_TIMESTAMP
            WHERE case_number = ?
            """,
            [reason, case_number],
        )

    def archive_past_auctions(self, as_of_date: date | None = None, grace_days: int = 7) -> int:
        """Bulk-archive auctions whose auction_date is before (as_of_date - grace_days).

        Uses pipeline_status='archived' (distinct from 'skipped') so these are
        clearly distinguishable from cases skipped for data-quality reasons.

        Preserves existing last_error for failed cases so diagnostic info is
        not lost.

        Args:
            as_of_date: Reference date (default: today in local/auction tz).
            grace_days: Number of days after auction_date before archiving.
                        Default 7 allows late-arriving PDFs/data to be processed.

        Returns:
            Count of newly archived cases.
        """
        from datetime import timedelta

        from src.utils.time import today_local

        self.ensure_status_table()
        conn = self.connect()
        cutoff = ((as_of_date or today_local()) - timedelta(days=grace_days)).isoformat()

        cursor = conn.execute("""
            UPDATE status SET
                pipeline_status = 'archived',
                last_error = CASE
                    WHEN last_error IS NULL THEN 'auction_date_passed'
                    ELSE last_error
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE case_number IN (
                SELECT s.case_number
                FROM status s
                JOIN auctions a ON a.case_number = s.case_number
                WHERE normalize_date(a.auction_date) < ?
                  AND normalize_date(a.auction_date) IS NOT NULL
                  AND COALESCE(s.pipeline_status, 'pending')
                      NOT IN ('completed', 'skipped', 'archived')
            )
        """, [cutoff])
        count = cursor.rowcount
        conn.commit()
        return count

    def unarchive_past_auctions(self) -> int:
        """Restore archived-due-to-date cases back to their pre-archive state.

        Cases that were 'failed' before archiving (identifiable by last_error
        != 'auction_date_passed') are restored to 'failed'.
        Cases whose only archive reason was date passage are restored to 'pending'.

        Returns:
            Count of un-archived cases.
        """
        self.ensure_status_table()
        conn = self.connect()
        cursor = conn.execute("""
            UPDATE status SET
                pipeline_status = CASE
                    WHEN last_error IS NOT NULL AND last_error != 'auction_date_passed'
                        THEN 'failed'
                    ELSE 'pending'
                END,
                last_error = CASE
                    WHEN last_error = 'auction_date_passed' THEN NULL
                    ELSE last_error
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_status = 'archived'
        """)
        count = cursor.rowcount
        conn.commit()
        return count

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
        total_result = conn.execute(f"SELECT COUNT(*) FROM status {date_filter}", params).fetchone()
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
            try:
                result = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    [table_name],
                ).fetchone()
                return result is not None
            except Exception as e:
                logger.exception(
                    f"backfill_status_steps table_exists({table_name}) failed: {e}"
                )
                raise

        # Ensure columns used for backfill exist when optional migrations haven't run.
        for col, col_type in [("bulk_folio", "TEXT"), ("raw_legal1", "TEXT"), ("flood_zone", "TEXT"), ("tax_status", "TEXT")]:
            with suppress(Exception):
                self._safe_exec(conn, f"ALTER TABLE parcels ADD COLUMN {col} {col_type}")

        date_clause = ""
        params: list = []
        if start_date:
            date_clause += " AND status.auction_date >= ?"
            params.append(start_date)
        if end_date:
            date_clause += " AND status.auction_date <= ?"
            params.append(end_date)

        # Step 1: Auction scraped (default for known cases)
        conn.execute(
            f"""
            UPDATE status 
            SET step_auction_scraped = COALESCE(status.step_auction_scraped, status.created_at, CURRENT_TIMESTAMP)
            WHERE status.step_auction_scraped IS NULL
            {date_clause}
            """,
            params,
        )

        # Step 2: Judgment extracted
        conn.execute(
            f"""
            UPDATE status 
            SET step_judgment_extracted = CURRENT_TIMESTAMP
            FROM auctions AS a
            WHERE status.case_number = a.case_number
              AND status.step_judgment_extracted IS NULL
              AND a.extracted_judgment_data IS NOT NULL
              {date_clause}
            """,
            params,
        )

        # Step 1 PDF downloaded: if judgment extracted, PDF existed
        conn.execute(
            f"""
            UPDATE status 
            SET step_pdf_downloaded = CURRENT_TIMESTAMP
            WHERE status.step_pdf_downloaded IS NULL
              AND status.step_judgment_extracted IS NOT NULL
              {date_clause}
            """,
            params,
        )

        # Step 3: Bulk enrichment (best-effort using bulk-parcel markers)
        conn.execute(
            f"""
            UPDATE status 
            SET step_bulk_enriched = CURRENT_TIMESTAMP
            FROM auctions AS a
            JOIN parcels AS p ON COALESCE(a.parcel_id, a.folio) = p.folio
            WHERE status.case_number = a.case_number
              AND status.step_bulk_enriched IS NULL
              AND (p.bulk_folio IS NOT NULL OR p.raw_legal1 IS NOT NULL)
              {date_clause}
            """,
            params,
        )

        # Step 3.5: HomeHarvest
        if table_exists("home_harvest"):
            conn.execute(
                f"""
                UPDATE status 
                SET step_homeharvest_enriched = CURRENT_TIMESTAMP
                WHERE status.step_homeharvest_enriched IS NULL
                  AND status.case_number IN (
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
            UPDATE status 
            SET step_hcpa_enriched = CURRENT_TIMESTAMP
            WHERE status.step_hcpa_enriched IS NULL
              AND status.case_number IN (
                SELECT a.case_number
                FROM auctions a
                WHERE COALESCE(a.parcel_id, a.folio) IN (
                    SELECT folio FROM parcels WHERE owner_name IS NOT NULL AND owner_name != ''
                )
                {sales_history_filter}
              )
              {date_clause}
            """,
            params,
        )

        # Step 5: ORI ingestion — intentionally NOT backfilled from chain_of_title.
        # Chain data is folio-global and a new case on the same folio must run ORI
        # independently. ORI completion is set only by actual execution or the
        # case-specific needs_ori_ingestion flag (handled in the flag backfill below).

        # Step 6: Survival analysis (auctions status ANALYZED/FLAGGED)
        conn.execute(
            f"""
            UPDATE status 
            SET step_survival_analyzed = CURRENT_TIMESTAMP
            FROM auctions AS a
            WHERE status.case_number = a.case_number
              AND status.step_survival_analyzed IS NULL
              AND a.status IN ('ANALYZED', 'FLAGGED')
              {date_clause}
            """,
            params,
        )

        # Step 7: Permits
        if table_exists("permits"):
            conn.execute(
                f"""
                UPDATE status 
                SET step_permits_checked = CURRENT_TIMESTAMP
                WHERE status.step_permits_checked IS NULL
                  AND status.case_number IN (
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
            UPDATE status 
            SET step_flood_checked = CURRENT_TIMESTAMP
            WHERE status.step_flood_checked IS NULL
              AND status.case_number IN (
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
                UPDATE status 
                SET step_market_fetched = CURRENT_TIMESTAMP
                WHERE status.step_market_fetched IS NULL
                  AND status.case_number IN (
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
            UPDATE status 
            SET step_tax_checked = CURRENT_TIMESTAMP
            WHERE status.step_tax_checked IS NULL
              AND status.case_number IN (
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
                UPDATE status 
                SET {step} = CURRENT_TIMESTAMP
                FROM auctions AS a
                WHERE status.case_number = a.case_number
                  AND status.{step} IS NULL
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
                CURRENT_TIMESTAMP
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
                step_auction_scraped = COALESCE(status.step_auction_scraped, status.created_at, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP
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
