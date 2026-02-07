"""
V2 Database (DuckDB) operations for chain of title and encumbrances.

Encapsulates all DuckDB V2 connection management and table creation
that was previously embedded in the PipelineOrchestrator.
"""
import duckdb
from typing import Optional, Dict, Any, List
from loguru import logger

from config.step4v2 import V2_DB_PATH
from src.utils.time import ensure_duckdb_utc


class V2Database:
    """Manages the V2 DuckDB database for ORI documents, chain of title, and encumbrances."""

    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or V2_DB_PATH
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Get or create a connection to the V2 database."""
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            ensure_duckdb_utc(self._conn)
            self._ensure_tables(self._conn)
        return self._conn

    def _ensure_tables(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Ensure V2 DuckDB tables exist (idempotent)."""
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
                acquisition_price DOUBLE,
                link_status TEXT,
                confidence_score DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS encumbrances (
                id INTEGER PRIMARY KEY,
                folio TEXT,
                chain_period_id INTEGER,
                encumbrance_type TEXT,
                creditor TEXT,
                debtor TEXT,
                amount DOUBLE,
                amount_confidence TEXT,
                amount_flags TEXT,
                recording_date DATE,
                instrument TEXT,
                book TEXT,
                page TEXT,
                is_satisfied BOOLEAN DEFAULT FALSE,
                satisfaction_instrument TEXT,
                satisfaction_date DATE,
                survival_status TEXT,
                party2_resolution_method TEXT,
                is_self_transfer BOOLEAN DEFAULT FALSE,
                self_transfer_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_joined BOOLEAN DEFAULT FALSE,
                is_inferred BOOLEAN DEFAULT FALSE
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY,
                folio TEXT,
                doc_type TEXT,
                instrument TEXT,
                book TEXT,
                page TEXT,
                recording_date DATE,
                party1 TEXT,
                party2 TEXT,
                legal_description TEXT,
                consideration DOUBLE,
                source_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ori_search_queue (
                id INTEGER PRIMARY KEY,
                folio TEXT,
                search_type TEXT,
                search_value TEXT,
                priority INTEGER DEFAULT 50,
                status TEXT DEFAULT 'pending',
                result_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS linked_identities (
                id INTEGER PRIMARY KEY,
                folio TEXT,
                name_a TEXT,
                name_b TEXT,
                link_type TEXT,
                confidence DOUBLE,
                source_instrument TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS legal_variations (
                id INTEGER PRIMARY KEY,
                folio TEXT,
                variation_text TEXT,
                source_instrument TEXT,
                source_type TEXT,
                is_canonical BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def close(self) -> None:
        """Close the V2 database connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                logger.debug("Error while closing V2 connection")
            self._conn = None

    def folio_has_chain_of_title(self, folio: str) -> bool:
        """Check if folio has chain of title data in V2."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM chain_of_title WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] > 0 if result else False

    def get_chain_of_title(self, folio: str) -> Dict[str, Any]:
        """Get chain of title records from V2."""
        conn = self.connect()
        periods = conn.execute(
            """SELECT id, folio, owner_name, acquired_from, acquisition_date,
                      disposition_date, acquisition_instrument, acquisition_doc_type,
                      acquisition_price, link_status, confidence_score, mrta_status,
                      years_covered
               FROM chain_of_title WHERE folio = ?
               ORDER BY acquisition_date""",
            [folio]
        ).fetchall()
        cols = [desc[0] for desc in conn.description]
        ownership_timeline = []

        for row in periods:
            period = dict(zip(cols, row, strict=False))
            period_id = period["id"]

            encs = conn.execute(
                """SELECT * FROM encumbrances
                   WHERE chain_period_id = ?
                   ORDER BY recording_date""",
                [period_id]
            ).fetchall()
            enc_cols = [desc[0] for desc in conn.description]
            period["encumbrances"] = [dict(zip(enc_cols, e, strict=False)) for e in encs]
            ownership_timeline.append(period)

        return {
            "folio": folio,
            "ownership_timeline": ownership_timeline,
            "current_owner": ownership_timeline[-1]["owner_name"] if ownership_timeline else None,
            "total_transfers": len(ownership_timeline)
        }

    def get_encumbrances_by_folio(self, folio: str) -> List[dict]:
        """Get encumbrances for folio from V2."""
        conn = self.connect()
        result = conn.execute(
            """SELECT id, folio, chain_period_id, encumbrance_type, creditor, debtor,
                      amount, amount_confidence, amount_flags, recording_date, instrument,
                      book, page, is_satisfied, satisfaction_instrument, satisfaction_date,
                      survival_status, is_joined, is_inferred
               FROM encumbrances WHERE folio = ?
               ORDER BY recording_date""",
            [folio]
        ).fetchall()
        cols = [desc[0] for desc in conn.description]
        return [dict(zip(cols, row, strict=False)) for row in result]

    def encumbrance_exists(self, folio: str, book: str, page: str) -> bool:
        """Check if encumbrance exists by book/page in V2."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM encumbrances WHERE folio = ? AND book = ? AND page = ?",
            [folio, book, page]
        ).fetchone()
        return result[0] > 0 if result else False
