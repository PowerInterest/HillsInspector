# Production Fixes: Remove DuckDB, Consolidate to SQLite

> **Goal:** Eliminate all DuckDB dependencies. SQLite is the single database.

## Scope Summary

| Category | Files | Status |
|---|---|---|
| **Dependency** | `pyproject.toml` | Remove `duckdb` |
| **Dead code (delete)** | `src/db/new.py`, `src/db/v2_database.py`, `src/db/migrations/create_v2_database.py`, `config/step4v2.py`, `pipelinev2/step4_patch.py` | Original DuckDB schema + disabled v2 |
| **Web app (convert)** | `app/web/database.py`, `app/web/routers/history.py` | Switch from DuckDB → SQLite |
| **History module (convert)** | `src/history/db_init.py`, `scrape_history.py`, `judgment_pipeline.py`, `resale_scanner.py`, `buyer_enricher.py` | Separate DuckDB-backed scraper history |
| **Utils (clean)** | `src/utils/time.py` (`ensure_duckdb_utc`), `src/utils/db_snapshot.py` | Remove DuckDB helpers |
| **Tools (clean)** | `src/tools/db_audit.py`, `src/analysis/db_audit.py`, `src/db/migrate_permits.py`, `src/db/check_missing_data.py` | Remove DuckDB imports |
| **Misc (clean)** | `src/db/writer.py` (docstring), `src/services/ingestion_service.py` (comments), `src/db/sqlite_adapter.py` | Update comments/docstrings |
| **Archive (leave)** | `docs/archive/scripts/*.py` | Dead scripts, don't touch |
| **Tests** | `tests/test_step6v2_real_data.py`, `tests/scripts/test_discovery_features.py` | Remove/update |

---

## Phase 1: Delete Dead DuckDB Code

These files are unused or disabled (`USE_STEP4_V2 = False`). Safe to delete:

#### [DELETE] [new.py](file:///mnt/c/code/HillsInspector/src/db/new.py)
Original DuckDB schema creator. Superseded by `create_sqlite_database.py`.

#### [DELETE] [v2_database.py](file:///mnt/c/code/HillsInspector/src/db/v2_database.py)  
V2 DuckDB connection manager. `USE_STEP4_V2 = False` — never active.

#### [DELETE] [create_v2_database.py](file:///mnt/c/code/HillsInspector/src/db/migrations/create_v2_database.py)
V2 database migration. Unused.

#### [DELETE] [step4v2.py](file:///mnt/c/code/HillsInspector/config/step4v2.py)
V2 config. All constants are only read by v2 code paths.

#### [DELETE] [step4_patch.py](file:///mnt/c/code/HillsInspector/pipelinev2/step4_patch.py)
V2 pipeline patch. Unused.

#### [DELETE] [test_step6v2_real_data.py](file:///mnt/c/code/HillsInspector/tests/test_step6v2_real_data.py)
V2 test. Dead.

---

## Phase 2: Convert Web App to SQLite

The web app reads from `property_master_web.db` (DuckDB format). It needs to use the SQLite database instead.

#### [MODIFY] [database.py](file:///mnt/c/code/HillsInspector/app/web/database.py)
- Replace `import duckdb` → `import sqlite3`
- Replace `duckdb.connect(str(DB_PATH), read_only=True)` → `sqlite3.connect(str(DB_PATH))`
- Remove `ensure_duckdb_utc(conn)` calls
- Update `DB_PATH` resolution to use `HILLS_SQLITE_DB` or `HILLS_WEB_DB` env var pointing to the SQLite file
- Replace DuckDB-specific SQL syntax (e.g., `ILIKE` → `LIKE`, `INTERVAL X DAY` → date arithmetic)
- Change return type annotations from `duckdb.DuckDBPyConnection` → `sqlite3.Connection`

> [!WARNING]
> **SQL dialect differences to watch for:**
> - `ILIKE` → `LIKE` (SQLite is case-insensitive by default for ASCII)
> - `CURRENT_DATE + INTERVAL 60 DAY` → use Python `date` parameters
> - `SHOW TABLES` → `SELECT name FROM sqlite_master WHERE type='table'`
> - No sequences (use `INTEGER PRIMARY KEY AUTOINCREMENT`)
> - `strict=True` in `zip()` works fine, no change needed

#### [MODIFY] [history.py](file:///mnt/c/code/HillsInspector/app/web/routers/history.py)
- Replace `import duckdb` → `import sqlite3`
- Same DuckDB → SQLite connection pattern

---

## Phase 3: Convert History Module to SQLite

The `src/history/` module has its own DuckDB-backed database for scraper history data.

> [!IMPORTANT]
> **Decision needed:** Does the history module use the **same** SQLite database as the pipeline, or a **separate** SQLite file? Using the same DB simplifies things but the existing schema may conflict with `create_sqlite_database.py` tables.

#### [MODIFY] [db_init.py](file:///mnt/c/code/HillsInspector/src/history/db_init.py)
- Replace `duckdb.connect()` → `sqlite3.connect()`
- Convert DuckDB DDL to SQLite DDL (sequences → autoincrement, etc.)

#### [MODIFY] [scrape_history.py](file:///mnt/c/code/HillsInspector/src/history/scrape_history.py)
- 5 instances of `duckdb.connect()` → `sqlite3.connect()`
- Remove `ensure_duckdb_utc()` calls

#### [MODIFY] [judgment_pipeline.py](file:///mnt/c/code/HillsInspector/src/history/judgment_pipeline.py)
- 3 instances of `duckdb.connect()` → `sqlite3.connect()`

#### [MODIFY] [resale_scanner.py](file:///mnt/c/code/HillsInspector/src/history/resale_scanner.py)
- 3 instances of `duckdb.connect()` → `sqlite3.connect()`
- Replace `executemany` pattern if DuckDB-specific

#### [MODIFY] [buyer_enricher.py](file:///mnt/c/code/HillsInspector/src/history/buyer_enricher.py)
- 2 instances of `duckdb.connect()` → `sqlite3.connect()`

---

## Phase 4: Clean Up Utils & Tools

#### [MODIFY] [time.py](file:///mnt/c/code/HillsInspector/src/utils/time.py)
- Remove or deprecate `ensure_duckdb_utc()` function

#### [MODIFY] [db_snapshot.py](file:///mnt/c/code/HillsInspector/src/utils/db_snapshot.py)
- Remove `import duckdb`, convert to SQLite if still useful

#### [MODIFY] [db_audit.py](file:///mnt/c/code/HillsInspector/src/tools/db_audit.py)
- Remove `import duckdb`, convert to SQLite

#### [MODIFY] [db_audit.py](file:///mnt/c/code/HillsInspector/src/analysis/db_audit.py)
- Remove `import duckdb`, convert to SQLite

#### [MODIFY] [migrate_permits.py](file:///mnt/c/code/HillsInspector/src/db/migrate_permits.py)
- Remove `import duckdb`, convert or delete if one-time migration

#### [MODIFY] [check_missing_data.py](file:///mnt/c/code/HillsInspector/src/db/check_missing_data.py)
- Remove `import duckdb`, convert to SQLite

#### [MODIFY] [sqlite_adapter.py](file:///mnt/c/code/HillsInspector/src/db/sqlite_adapter.py)
- Remove any DuckDB references

#### [MODIFY] [writer.py](file:///mnt/c/code/HillsInspector/src/db/writer.py)
- Update docstring: "Single-writer queue for DuckDB operations" → "Single-writer queue for SQLite operations"

#### [MODIFY] [ingestion_service.py](file:///mnt/c/code/HillsInspector/src/services/ingestion_service.py)
- Update comments mentioning DuckDB formatting

#### [MODIFY] [create_sqlite_database.py](file:///mnt/c/code/HillsInspector/src/db/migrations/create_sqlite_database.py)
- Remove `import duckdb` on line 629 (inside a function)

---

## Phase 5: Remove Dependency

#### [MODIFY] [pyproject.toml](file:///mnt/c/code/HillsInspector/pyproject.toml)
- Remove `"duckdb"` from `dependencies`

#### [DELETE] DuckDB database files
- `data/property_master_v2.db` (v2 database, unused)
- `data/property_master_web.db` (DuckDB web snapshot) — replace with SQLite copy

---

## Verification Plan

1. `grep -rni duckdb src/ app/ config/ tests/ pyproject.toml` → should return 0 results
2. `uv run python -c "from src.db.operations import PropertyDB; db = PropertyDB(); print(db.connect())"` → confirms SQLite works
3. Run the web app and verify it loads from SQLite
4. Run the pipeline for a small date range and check for import errors

---

## Decision Points for User

1. **History module DB:** Same SQLite file or separate? (Recommendation: same file, add tables to `create_sqlite_database.py`)
2. **Archive scripts:** Leave as-is or delete entirely? (Recommendation: leave, they're in `docs/archive/`)
3. **`src/db/new.py`:** This was the original DuckDB schema — confirm it's fully superseded by `create_sqlite_database.py`?
