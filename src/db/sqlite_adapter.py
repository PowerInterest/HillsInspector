"""
SQLite compatibility adapter for HillsInspector.

Provides helper functions to make SQLite work more like DuckDB,
minimizing changes needed in existing code.
"""
import sqlite3
from contextlib import suppress
from loguru import logger


def get_sqlite_connection(db_path: str, timeout: float = 30.0) -> sqlite3.Connection:
    """
    Create an optimized SQLite connection with WAL mode.
    
    Args:
        db_path: Path to SQLite database file
        timeout: Busy timeout in seconds
        
    Returns:
        Configured SQLite connection
    """
    conn = sqlite3.connect(db_path, timeout=timeout, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    
    # Enable WAL mode for concurrent writes
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    conn.execute("PRAGMA temp_store=MEMORY")
    
    return conn


def add_column_safe(conn: sqlite3.Connection, table: str, column: str, 
                    col_type: str, default=None) -> bool:
    """
    Add a column to a table if it doesn't already exist.
    
    SQLite doesn't support ADD COLUMN IF NOT EXISTS, so we catch the error.
    
    Args:
        conn: SQLite connection
        table: Table name
        column: Column name
        col_type: Column type (TEXT, INTEGER, REAL, etc.)
        default: Optional default value
        
    Returns:
        True if column was added, False if it already existed
    """
    try:
        if default is not None:
            if isinstance(default, str) and default not in ('NULL', 'CURRENT_TIMESTAMP'):
                default = f"'{default}'"
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type} DEFAULT {default}")
        else:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        return True
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            return False
        raise


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the database."""
    with suppress(Exception):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None
    return False


def execute_with_retry(conn: sqlite3.Connection, sql: str, params=None, 
                       max_retries: int = 3) -> sqlite3.Cursor:
    """
    Execute SQL with automatic retry on lock errors.
    
    Args:
        conn: SQLite connection
        sql: SQL statement
        params: Query parameters
        max_retries: Maximum retry attempts
        
    Returns:
        Cursor from successful execution
    """
    import time
    
    last_error = None
    for attempt in range(max_retries):
        try:
            if params:
                return conn.execute(sql, params)
            return conn.execute(sql)
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                last_error = e
                time.sleep(0.1 * (2 ** attempt))
            else:
                raise
    
    raise last_error


def interval_to_date(interval_str: str) -> str:
    """
    Convert DuckDB INTERVAL syntax to SQLite date arithmetic.
    
    DuckDB: CURRENT_DATE - INTERVAL 7 DAY
    SQLite: date('now', '-7 days')
    
    Args:
        interval_str: String like "7 DAY" or "30 DAY"
        
    Returns:
        SQLite date string
    """
    # Parse common patterns
    parts = interval_str.strip().upper().split()
    if len(parts) == 2:
        num = int(parts[0])
        unit = parts[1].rstrip('S')  # Remove plural 's'
        return f"date('now', '-{num} {unit.lower()}s')"
    return "date('now')"
