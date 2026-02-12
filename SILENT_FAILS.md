# Silent Failures in HillsInspector Codebase

## HIGH SEVERITY (Critical Data Loss / Pipeline Stall)

### 1. Orchestrator: JSON Parse Failure Falls Through Silently
**File:** `src/orchestrator.py:372-374`
```python
except (json.JSONDecodeError, TypeError) as e:
    logger.warning(f"Could not parse judgment data for survival analysis on {folio}: {e}")

foreclosed_mtg = judgment_data.get("foreclosed_mortgage", {})
```
**Issue:** When judgment data fails to parse, the code continues with an empty `judgment_data = {}` dict. Survival analysis runs with no foreclosing mortgage data, producing incorrect results. Pipeline reports success but with garbage.

### 2. Ingestion Service: No Legal Description = Silent Bail
**File:** `src/services/ingestion_service.py:150-152`
```python
if not search_terms:
    logger.warning(f"No valid legal description for {prop.case_number}")
    return
```
**Issue:** Returns `None` with only a warning. No status marked, no retry triggered, property silently dropped from pipeline. No chain of title, no encumbrances, no survival analysis.

### 3. Orchestrator: Party-Based Ingestion Failure Hidden
**File:** `src/orchestrator.py:989-994`
```python
except Exception as e:
    logger.error(f"Party-based ORI search failed for {case_number}: {e}")
    await self.db_writer.enqueue("generic_call", {
        "func": self.db.mark_status_failed,
        "args": [case_number, f"Party search failed: {str(e)[:150]}", 5]
    })
```
**Issue:** Marks status failed but then *continues execution* instead of returning. Enrichment proceeds with incomplete data.

### 4. Orchestrator: Empty Parcel ID Silently Skipped
**File:** `src/orchestrator.py:756-762`
```python
if not parcel_id:
    logger.info(f"Skipping enrichment: No parcel_id for case {case_number}")
    ...
```
**Issue:** Uses `logger.info` (not warning/error) for a data completeness failure. 65 auctions had empty parcel_id strings silently skipped.

### 5. Operations: Table Existence Check Swallows All Exceptions
**File:** `src/db/operations.py:3306-3310`
```python
except Exception:
    return False
```
**Issue:** Returns `False` for *any* exception. DB corruption, connection errors, permission issues all look like "table doesn't exist".

---

## MEDIUM SEVERITY (Debugging Difficulty / Data Quality)

### 6. Auction Scraper: Date Failure Continues Loop
**File:** `src/scrapers/auction_scraper.py:83-86`
```python
except Exception as e:
    logger.error(f"Failed to scrape {current}: {e}")
current += timedelta(days=1)
```
**Issue:** Date scrape failure logs error but continues. Entire days of auctions silently missing. No retry, no status tracking.

### 7. Auction Scraper: Page Close Failures Ignored
**File:** `src/scrapers/auction_scraper.py:1110-1113`
```python
if hcpa_page:
    try:
        await hcpa_page.close()
    except Exception:
        pass
```
**Issue:** Browser cleanup failures completely silent. Resource leaks, zombie browser processes.

### 8. Homeharvest: Type Conversion Failures Return None
**File:** `src/services/homeharvest_service.py:448-449`
```python
except Exception:
    return None
```
**Issue:** Type conversion errors silently return None. One bad value poisons the entire field.

### 9. Homeharvest: Date Parse Failures Return String
**File:** `src/services/homeharvest_service.py:460-461`
```python
except Exception:
    return str(v)
```
**Issue:** Date parsing failures silently fall back to string representation. DB expects ISO format, gets unpredictable strings.

### 10. Data Linker: NOC Date Parse Skips Record
**File:** `src/services/data_linker.py:49-51`
```python
except Exception as e:
    logger.debug(f"Failed to parse NOC date '{rec_date_str}': {e}")
    continue
```
**Issue:** Uses `logger.debug` for data quality failures. NOC records with unparseable dates silently dropped from linking.

### 11. ORI Scraper: Date Format Fallback Hides Issues
**File:** `src/scrapers/ori_api_scraper.py:560-561`
```python
except Exception:
    return raw_date.replace("/", "").replace(" ", "")[:8]
```
**Issue:** Date parsing failures silently fall back to string manipulation. Returns first 8 chars of scrubbed date which could be gibberish.

### 12. Parcel Resolver: Row Count Check Swallows Exception
**File:** `src/services/parcel_resolver.py:73-74`
```python
except Exception:
    logger.warning(f"{TAG} Could not verify bulk_parcels row count")
```
**Issue:** DB query exception logged as warning but otherwise ignored. Resolver continues without knowing bulk data status.

---

## LOW SEVERITY (Cosmetic / Expected Errors)

### 13. Vision Service: Error Body Read Failures
**File:** `src/services/vision_service.py:1320-1321` — `err_body = "(unreadable)"` on any exception.

### 14. Vision Service: Image Analysis Failures
**File:** `src/services/vision_service.py:1478-1480` — Returns None on all exceptions (does log via `logger.exception`).

### 15. Tax Scraper: Timeout Fallback to Sleep
**File:** `src/scrapers/tax_scraper.py:93-95` — Broad catch falls back to 8s blind sleep.

### 16. Tax Scraper: No View Buttons Found
**File:** `src/scrapers/tax_scraper.py:189-190` — Warning only, doesn't distinguish "no results" from "scraper broken".

### 17. HCPA Scraper: Page Load Timeout Warnings
**File:** `src/scrapers/hcpa_scraper.py:95-96` — Timeout logged as warning, scraping continues with possibly incomplete data.

### 18. HCPA Scraper: Text Extraction Failure
**File:** `src/scrapers/hcpa_gis_scraper.py:297-298` — Warning only, card_text supplementary.

### 19. HCPA Scraper: Browser Kill Failure
**File:** `src/scrapers/hcpa_gis_scraper.py:578-579` — `except Exception: pass`.

### 20. Hover Scraper: Dropdown Selection Fallthrough
**File:** `src/scrapers/hover_scraper.py:76-79` — Catches exception, comment admits "risky", no warning logged.

### 21. Tax Deed Scraper: Empty Page Detection
**File:** `src/scrapers/tax_deed_scraper.py:77-79` — Uses `info` for what could be scraper failure.

### 22. Orchestrator: No Logging on Owner Name Guard
**File:** `src/orchestrator.py:1524` — `if not owner_name: return` with zero logging.

### 23. DB Audit Tool: Metric Calculation Failures
**File:** `src/tools/db_audit.py:515-516` — Hides calculation errors as "N/A".

### 24. History Visual Inspect: Timeout Screenshot Debug
**File:** `src/history/visual_inspect.py:35-37` — Debug script, low risk.

---

## Summary

| Severity | Count |
|----------|-------|
| HIGH | 5 |
| MEDIUM | 7 |
| LOW | 12 |
| **Total** | **24** |
