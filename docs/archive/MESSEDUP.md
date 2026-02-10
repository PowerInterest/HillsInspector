# Pipeline Consolidation Review Plan

## The Problem

We had **three separate pipeline implementations** with overlapping logic:

| File | Lines | What it did |
|------|-------|-------------|
| `main.py` | 552 | `handle_update`: Inline Steps 1-3.5, then delegated to orchestrator |
| `pipeline.py` | 1826 | `run_full_pipeline`: Complete standalone Steps 1-14 |
| `orchestrator.py` | 707 | `PipelineOrchestrator`: Parallel property enrichment (Steps 4+) |

### Symptoms
- `--update` ran main.py's inline code + orchestrator
- `--test` and `--debug` ran pipeline.py's code
- Fixes applied to one didn't apply to the other
- Different LLMs modified different files unknowingly
- Database locking issues when multiple runs started (each created own `PropertyDB()`)

### Root Cause
main.py was supposed to be a thin stub calling orchestrator, but grew inline Steps 1-3.5.

---

## The Fix (User is implementing)

1. **Rename** `pipeline.py` → `pipeline_OLD.py` (legacy reference)
2. **Move** Steps 1, 1.5, 2, 3, 3.5 from `main.py::handle_update` → `orchestrator.py::run_full_update`
3. **Simplify** main.py to thin CLI stub
4. **Remove** `--test` and `--debug` handlers
5. **Single** `PropertyDB` instance passed through (no duplicate instantiation)

---

## Review Checklist

### Architecture
- [ ] `main.py` is thin stub (just CLI parsing → orchestrator call)
- [ ] `orchestrator.py` has new `run_full_update()` method with Steps 1-3.5
- [ ] **Scrapers return data only** (no DB access, no `PropertyDB` instances)
- [ ] **Orchestrator handles ALL DB writes** via `DatabaseWriter` queue
- [ ] Single `PropertyDB()` in orchestrator only (not passed to scrapers)
- [ ] Scrapers receive only what they need (addresses, folios) - return Property objects/dicts

### Step Logic Preserved
- [ ] **Step 1**: Foreclosure scraping with date-by-date skip logic
- [ ] **Step 1.5**: Tax deed scraping
- [ ] **Step 2**: Final Judgment download + extraction
- [ ] **Step 3**: Bulk enrichment from HCPA parcel dump
- [ ] **Step 3.5**: HomeHarvest MLS data

### Skip Logic
- [ ] Auction dates with `count > 0` are skipped
- [ ] Dates with 0 auctions tracked (new `scraped_auction_dates` table?) to prevent re-scraping holidays
- [ ] `extracted_judgment_data` check for Step 2
- [ ] `last_analyzed_case_number` check for ORI/Survival steps

### The 6 Findings (Still Fixed?)
1. [ ] **HCPA GIS**: Marks complete on any successful response (not just when sales_history exists)
2. [ ] **foreclosing_refs**: Set to `None` unless at least one identifier exists
3. [ ] **HomeHarvest**: Uses `COALESCE(a.parcel_id, a.folio)` and date range filter
4. [ ] **Market-data flag**: Only cleared if BOTH Zillow and Realtor exist
5. [ ] **Judgment legal**: Does `INSERT OR IGNORE` before UPDATE
6. [ ] **Lien dedupe**: Proper parentheses `((book = ? AND page = ?) OR instrument_number = ?)`

### Database Safety
- [ ] No concurrent `PropertyDB` connections fighting for write lock
- [ ] All writes serialized through `DatabaseWriter` queue
- [ ] Graceful handling if pipeline already running (lock file or error message)

### CLI Arguments
- [ ] `--update` works with optional `--start-date`, `--end-date`, `--auction-limit`
- [ ] `--start-step` and `--end-step` preserved (resume after failure)
- [ ] `--skip-tax-deeds` preserved
- [ ] `--web` and `--new` unchanged

### Datetime/Timezone
- [ ] Consistent `datetime` imports (not mixing `from datetime import datetime` with `import datetime`)
- [ ] UTC used throughout (`datetime.now(tz=UTC)`)
- [ ] `date` vs `datetime` types handled correctly

### Error Handling
- [ ] Failures don't silently continue
- [ ] Errors logged with context (case_number, folio, step)
- [ ] Partial failures don't corrupt database state

---

## Files to Check

| File | Expected Changes |
|------|------------------|
| `main.py` | Thin stub, ~100 lines max |
| `src/orchestrator.py` | New `run_full_update()`, Steps 1-3.5 logic, ALL DB writes |
| `src/pipeline.py` | Renamed to `pipeline_OLD.py` |
| `src/scrapers/auction_scraper.py` | **Remove** `self.db = PropertyDB()`, just return data |
| `src/scrapers/*.py` | **Remove** all DB writes, just return data |
| `src/services/homeharvest_service.py` | **Remove** DB access, return data to orchestrator |
| `src/db/operations.py` | Possibly new `scraped_auction_dates` table |

### Scraper Cleanup Pattern

**Before (bad):**
```python
class AuctionScraper:
    def __init__(self):
        self.db = PropertyDB()  # BAD: creates connection

    async def scrape_date(self, date):
        props = await self._scrape(date)
        for p in props:
            self.db.upsert_parcel(p)  # BAD: writes during scrape
        return props
```

**After (good):**
```python
class AuctionScraper:
    def __init__(self):
        self.storage = ScraperStorage()  # OK: just file storage

    async def scrape_date(self, date) -> List[Property]:
        props = await self._scrape(date)
        return props  # GOOD: just return data
```

**Orchestrator handles writes:**
```python
# In orchestrator.run_full_update()
props = await scraper.scrape_date(date)
for p in props:
    await self.db_writer.enqueue("upsert_auction", p.to_dict())
    await self.db_writer.enqueue("upsert_parcel", p.to_dict())
```

---

## Testing

After review, run:
```bash
# Single date test
uv run main.py --update --start-date 2026-01-02 --end-date 2026-01-02 --auction-limit 1

# Check no duplicate scraping
uv run main.py --update --start-date 2026-01-02 --end-date 2026-01-02
# Should show "Skipping 2026-01-02: X auctions already in DB"
```
