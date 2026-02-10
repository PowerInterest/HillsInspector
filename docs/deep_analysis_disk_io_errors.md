# Deep Analysis: "disk I/O error" and "database is locked" Root Causes

## Executive Summary

**The "disk I/O error" is a SQLite problem, not DuckDB.** Despite initial assumptions, the stack traces prove every single `disk I/O error` originates from `PropertyDB.connect()` (SQLite) — specifically at `conn.execute("PRAGMA journal_mode=WAL")` on line 46 of `src/db/operations.py`. The root cause is a combination of:

1. **Excessive concurrent SQLite connections** from thread pool executors
2. **NTFS-over-WSL filesystem** where POSIX file locking (`fcntl.flock`) is unreliable
3. **Thread-local connection management** that creates new connections prolifically

---

## The Evidence

### Stack Trace (from `hills_inspector_2026-02-09.log:821-895`)

```
Survival analysis failed for 1932012WC000004000110U: disk I/O error

File ".../src/orchestrator.py", line 1888, in _run_survival_analysis
    result = await loop.run_in_executor(None, self._gather_and_analyze_survival, prop)

File ".../src/orchestrator.py", line 339, in _gather_and_analyze_survival
    auction = self.db.get_auction_by_case(case_number)

File ".../src/db/operations.py", line 1732, in get_auction_by_case
    conn = self.connect()

File ".../src/db/operations.py", line 46, in connect
    conn.execute("PRAGMA journal_mode=WAL")

sqlite3.OperationalError: disk I/O error
```

**Key observation:** The error class is `sqlite3.OperationalError`, not a DuckDB error. Every single instance follows this exact path.

---

## Root Cause: Multi-Threaded SQLite Overload on NTFS/WSL

### How the Connection Architecture Works

```
PropertyDB.__init__():
    self._local = threading.local()     # Per-thread storage
    self._local.conn = None

PropertyDB.connect():
    conn = self._get_conn()             # Check thread-local
    if conn is None:
        conn = sqlite3.connect(...)     # NEW connection per thread
        conn.execute("PRAGMA journal_mode=WAL")  # ← CRASHES HERE
```

### Why It Happens

The pipeline runs with `property_semaphore = 15`, meaning up to 15 properties are enriched concurrently. Here's the timeline for each property:

```
_enrich_property():
    Phase 1: Parallel scrapers (6 concurrent tasks, same asyncio thread)
    Phase 2: ORI Ingestion (main asyncio thread)
    Phase 3: asyncio.TaskGroup()
        → _run_permit_scraper()
        → _run_survival_analysis()
            → loop.run_in_executor(None, self._gather_and_analyze_survival, prop)
              ↑ SPAWNS A NEW THREAD
```

The critical line is `loop.run_in_executor(None, ...)` — this runs the survival analysis in Python's default thread pool. Since `PropertyDB` uses `threading.local()`, each new thread gets `conn = None` and must open a **new SQLite connection**.

With 15 properties concurrent:
- Up to 15 survival analysis threads opening new SQLite connections simultaneously
- Plus the main asyncio thread's connection
- Plus the `DatabaseWriter` thread's connection  
- Plus possibly permit scraper threads

**That's potentially 17+ simultaneous SQLite connections** to the same `.db` file.

### Why NTFS/WSL Makes It Catastrophic

SQLite's WAL mode relies on shared-memory (`-shm`) files and POSIX advisory locks. On WSL2 with an NTFS-mounted filesystem (`/mnt/c/`):

| Feature | Linux (ext4) | WSL2 + NTFS |
|---|---|---|
| `fcntl.flock()` | Reliable | **Unreliable/emulated** |
| `-shm` mmap | Fast, direct | **Translated through 9P** |
| WAL concurrent reads | Works well | **Lock contention** |
| Multiple writer attempts | Queued properly | **I/O errors** |

When multiple threads try to initialize WAL mode simultaneously on NTFS, the file lock acquisitions can fail at the OS level, which SQLite surfaces as `disk I/O error` rather than `database is locked`.

---

## The "database is locked" Connection

The `database is locked` errors are the **same root cause, different symptom**:

```python
# src/orchestrator.py line 2390-2406
# Retry bulk enrichment with backoff (often fails transiently on locked DB)
for _attempt in range(3):
    try:
        enrichment_stats = enrich_auctions_from_bulk(conn=db.conn)
        break
    except Exception as _e:
        if "locked" in str(_e).lower() and _attempt < 2:
            wait = 2 ** _attempt
            logger.warning(f"Bulk enrichment locked (attempt {_attempt+1}/3)")
```

The code already acknowledges this is a known problem with a retry loop. But retries don't fix the fundamental issue of too many concurrent connections.

---

## Proposed Fixes (Ordered by Impact)

### Fix 1: Pre-fetch Auction Data Before Executor (Quick Win)

The v2 path already does this correctly:

```python
# V2 path (CORRECT - pre-fetches in main thread):
if USE_STEP4_V2:
    auction = self.db.get_auction_by_case(case_number)  # Main thread
    async with self.v2_db_semaphore:
        result = await loop.run_in_executor(
            None, self._gather_and_analyze_survival_v2, prop, auction
        )

# V1 path (BROKEN - fetches inside executor thread):
else:
    result = await loop.run_in_executor(
        None, self._gather_and_analyze_survival, prop  # Thread opens NEW conn
    )
```

**Fix**: Refactor `_gather_and_analyze_survival` to accept pre-fetched `auction` data, matching the v2 pattern.

### Fix 2: Connection Pooling / Reuse

Replace `threading.local()` with a bounded connection pool:

```python
class PropertyDB:
    def __init__(self, db_path=None, max_connections=3):
        self._pool = queue.Queue(maxsize=max_connections)
        for _ in range(max_connections):
            conn = sqlite3.connect(db_path, timeout=30.0)
            # ... configure WAL, etc.
            self._pool.put(conn)
    
    @contextmanager
    def get_connection(self):
        conn = self._pool.get(timeout=30)
        try:
            yield conn
        finally:
            self._pool.put(conn)
```

This caps the total number of SQLite connections regardless of thread count.

### Fix 3: Reduce Concurrency for Survival Analysis

Add a dedicated semaphore for survival to prevent thread explosion:

```python
self.survival_semaphore = asyncio.Semaphore(3)  # Max 3 concurrent

async def _run_survival_analysis(self, ...):
    async with self.survival_semaphore:
        # ... existing code
```

### Fix 4: Move SQLite Database to Linux Filesystem (Nuclear Option)

If I/O errors persist after connection management fixes, move the **SQLite** database
off the NTFS mount to avoid the unreliable file-locking layer:

```bash
# Move SQLite DB to WSL2 native filesystem (ext4)
mkdir -p ~/hills_data
cp /mnt/c/code/HillsInspector/data/property_master_sqlite.db ~/hills_data/
# Update db_path in PropertyDB default or config
```

This eliminates the NTFS/9P translation layer that breaks POSIX advisory locks.

---

## Recommended Implementation Order

1. **Fix 1** (5 min) — Pre-fetch auction data for v1 survival path. Eliminates the most common trigger.
2. **Fix 3** (2 min) — Add survival semaphore. Limits concurrent thread spawning.
3. **Fix 2** (30 min) — Connection pooling. The proper long-term solution.
4. **Fix 4** (5 min) — Filesystem migration. If fixes 1-3 don't fully resolve it.

---

> **Note (2026-02-10):** DuckDB has been completely removed from the codebase. The project now uses SQLite only. The V2 DuckDB path discussed in this analysis no longer exists.
