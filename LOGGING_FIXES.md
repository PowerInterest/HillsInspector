# Logging Fixes (2026-02-10)

Goal: **Never fail silently**. These entries capture the current ground-truth errors seen in `logs/hills_inspector_2026-02-10.log` and the exact missing context to add.

## 1) Step4v2: Failed to save document (SQLite binding list)
**Error text:** `Failed to save document: Error binding parameter 16: type 'list' is not supported`  
**Count:** 4,305  
**Ground truth:** `discovery._save_document` is inserting a Python `list` into SQLite. Parameter 16 is `parties_one` (17 is `parties_two`). This blocks ORI docs from being saved, which cascades into chain/encumbrance failure.  
**Add to log:** `folio`, `search_id`, `instrument`, `doc_type`, `type(parties_one/parties_two)`, and a truncated sample (first 1–2 names). Also include source doc keys to distinguish API vs browser format.

## 2) Step4v2: Search failed due to `.strftime` on string
**Error text:** `Search error: 'str' object has no attribute 'strftime'`  
**Count:** 60  
**Ground truth:** `_run_search` is passing a string into `_format_date`, which expects a date object. This terminates the search and causes “All searches exhausted”.  
**Add to log:** `search_id`, `folio`, `search_type`, `search_term`, `date_from`, `date_to`, and `type(date_from/date_to)`. Include `triggered_by_search_id` and `triggered_by_instrument` to trace origin.

## 3) ORI API: 400 Bad Request
**Error text:** `Error searching ORI ... 400 Client Error: Bad Request`  
**Count:** many (recurring bursts)  
**Ground truth:** ORI API rejected the payload; current logs do not include the payload or response body.  
**Add to log:** payload summary for each 400 (at least `search_type`, `search_term`, `date_from`, `date_to`, and doc type filter flag). Include response body if available.

## 4) Bulk enrichment: database locked
**Error text:** `Bulk enrichment failed: database is locked`  
**Count:** 2  
**Ground truth:** SQLite lock prevented bulk enrichment from writing after retry.  
**Add to log:** DB path, busy_timeout, connection mode (WAL/tx state), and any active writer context if available.

## 5) Bulk enrichment: SQL syntax error
**Error text:** `Bulk enrichment failed: near "AS": syntax error`  
**Count:** 1  
**Ground truth:** A specific SQL statement inside `bulk_parcel_ingest.enrich_auctions_from_bulk` is not SQLite-compatible. Current log does not identify which statement.  
**Add to log:** label/name of the failing SQL block and the SQL string (or a hash + snippet).

## 6) Market scraper: Zillow blocked by CAPTCHA
**Error text:** `Zillow scrape failed: Bot detection triggered on zillow: CAPTCHA/block detected`  
**Count:** 28  
**Ground truth:** Zillow is blocking the scraper; run skips Realtor fallback.  
**Add to log:** URL, folio/case, and screenshot path.

## 7) Geocoding: “Mailing Address” no result
**Error text:** `Geocode: no result for Mailing Address, Tampa, FL`  
**Count:** 17  
**Ground truth:** Geocoder is being fed a mailing address rather than a site address.  
**Add to log:** folio/case and address source (auction, parcels, judgment, bulk).

## 8) Survival analysis: Could not identify foreclosing lien
**Error text:** `Could not identify foreclosing lien for {folio}`
**Count:** several (recurring)
**Ground truth:** Survival analysis can't match foreclosing instrument among encumbrances.
**Add to log:** `foreclosing_refs` (instrument/book/page), encumbrance count, and which matching strategy failed.

---

# Silent Failure Audit (2026-02-10)

Goal: Eliminate every `except: pass`, `suppress(Exception)`, `except: return`, and `print()` pattern that hides pipeline failures. Grouped by severity and file.

---

## CRITICAL

### 9) operations.py: `conn.commit()` suppressed in `_apply_schema_migrations`
**Location:** `src/db/operations.py:335-336`
**Pattern:** `with suppress(Exception): conn.commit()`
**Impact:** If commit fails, ALL preceding DML in `_apply_schema_migrations` (date normalization, document dedup DELETEs, parcel_id flag fixes, legal description backfills) is silently rolled back. This is a data integrity time bomb — the pipeline believes migrations ran, but the DB is unchanged.
**Available at failure:** `conn` (the SQLite connection), `self.db_path` (DB file path). The exception itself (e.g., `OperationalError: database is locked`, `IntegrityError`) is lost because `suppress` discards it.
**Add to log:** `logger.error(f"CRITICAL: Migration commit failed on {self.db_path}: {e}")` with the exception type and message. Also log how many DML statements were pending (count of UPDATE/DELETE blocks above).

### 10) operations.py: DML UPDATEs suppressed in `_apply_schema_migrations`
**Location:** `src/db/operations.py:220-257` (3 `with suppress(Exception):` blocks)
**Pattern:** `with suppress(Exception):` wrapping UPDATE statements for: (a) sales_history date normalization, (b) auctions has_valid_parcel_id fix, (c) parcels judgment_legal_description backfill
**Impact:** If any UPDATE fails (schema mismatch, corrupt data, wrong column name), the fix silently doesn't apply. Bad date formats persist → downstream date comparisons break. Wrong parcel_id flags → properties incorrectly skipped/included. Missing legal descriptions → ORI ingestion has no legal to search.
**Available at failure:** `conn`, `table_exists()` results are already checked, but the SQL itself may reference columns that don't exist yet. The UPDATE target table name is hardcoded.
**Add to log:** For each block, wrap in explicit `try/except Exception as e: logger.warning(f"Migration UPDATE on {table} failed: {e}")`. Include `table` name and a label like `"date_normalization"`, `"parcel_id_flag"`, `"legal_desc_backfill"`.

### 11) operations.py: DML DELETEs suppressed in `_apply_schema_migrations`
**Location:** `src/db/operations.py:302-329` (2 `with suppress(Exception):` blocks)
**Pattern:** `with suppress(Exception):` wrapping DELETE FROM documents dedup
**Impact:** If dedup DELETE fails, duplicate documents persist in the `documents` table. Duplicate documents corrupt chain-of-title construction (double-counted encumbrances, duplicate ownership periods). This is the **core deliverable** of the pipeline.
**Available at failure:** `conn`, `idx_exists` (bool — whether the ori_uuid unique index exists). The DELETE SQL references complex subqueries with GROUP BY.
**Add to log:** `logger.error(f"Document dedup DELETE failed: {e}")`. Also log `conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]` before and after to quantify impact.

### 12) auction_scraper.py: Pagination `except Exception: break` silently stops
**Location:** `src/scrapers/auction_scraper.py:163`
**Pattern:** `except Exception: break` — no logging, no `as e`
**Impact:** If clicking the "Next" pagination button fails (network error, DOM change, timeout), remaining pages of auctions are **silently lost**. On a day with 50+ auctions across multiple pages, dozens of auction records disappear with zero trace.
**Available at failure:** `page_num` (current page number), `date_str` (auction date being scraped), `len(properties)` (auctions collected so far), `max_properties` (limit if set). The `next_btn` Playwright locator is in scope.
**Add to log:** `logger.warning(f"Pagination failed on page {page_num} for {date_str}, collected {len(properties)} auctions before stop: {e}")`.

### 13) app/web/database.py: `safe_connection` commit suppressed
**Location:** `app/web/database.py:176-179`
**Pattern:** `with suppress(Exception): if for_write: conn.commit(); conn.close()`
**Impact:** If `conn.commit()` raises (DB corruption, constraint violation, locked), the error is **completely swallowed**. A failed write appears to succeed to the caller. The web UI thinks data was saved when it wasn't.
**Available at failure:** `conn` (the connection object), `for_write` (bool — whether this was a write connection). The exception itself is discarded.
**Add to log:** Split into explicit `try/except`: `logger.error(f"safe_connection commit failed (for_write={for_write}): {e}")`. Include `self.db_path` or the module-level DB path.

### 14) operations.py: `save_sales_history` uses `print()` and swallows per-row errors
**Location:** `src/db/operations.py:2243-2244`
**Pattern:** `except Exception as e: print(f"Error saving sale record: {e}")` — uses `print()`, does not re-raise, loop continues
**Impact:** Individual sales_history rows are silently dropped. The method reports a successful count that may be inflated. `print()` output won't appear in log files.
**Available at failure:** `folio`, `strap`, `book`, `page`, `sale.get("instrument")`, `sale.get("date")`, `sale.get("doc_type")`, `sale_price`, and the full `sale` dict. The exception `e` has the error message.
**Add to log:** `logger.error(f"Failed to save sale record for {folio}: book={book}, page={page}, instrument={sale.get('instrument')}, error={e}")`. Also change `print(f"Saved {len(sales)} ...")` on line 2246 to `logger.info(...)`.

---

## HIGH

### 15) operations.py: WAL checkpoint suppressed
**Location:** `src/db/operations.py:82-83`
**Pattern:** `with suppress(Exception): conn.execute("PRAGMA wal_checkpoint(PASSIVE)")`
**Impact:** If WAL checkpoint fails (locked DB, corrupt WAL), data stays in WAL and may be lost on crash. The orchestrator calls `checkpoint()` after every step, so repeated silent failures accumulate risk.
**Available at failure:** `conn` (connection), `self.db_path`. PRAGMA wal_checkpoint returns a result row with `(busy, log, checkpointed)` counts.
**Add to log:** `logger.warning(f"WAL checkpoint failed on {self.db_path}: {e}")`. Also log the checkpoint result on success at `debug` level.

### 16) operations.py: `get_folio_from_strap` double `except Exception: pass`
**Location:** `src/db/operations.py:356-374`
**Pattern:** Two `try/except Exception: pass` blocks — first queries `bulk_parcels`, second queries `parcels`
**Impact:** If either query fails for reasons other than "table doesn't exist" (e.g., locked DB, corrupt index), folio lookup silently returns `None`. This means properties cannot be matched to bulk data for pre-hydration.
**Available at failure:** `strap` (the input parcel ID), `conn` (connection). The first query targets `bulk_parcels.strap`, the second targets `parcels.folio`.
**Add to log:** `logger.debug(f"get_folio_from_strap({strap}) query failed: {e}")` in each block.

### 17) operations.py: `save_chain_of_title` encumbrance SELECT suppressed
**Location:** `src/db/operations.py:1605-1623`
**Pattern:** `with suppress(Exception):` wrapping SELECT from encumbrances
**Impact:** If the SELECT fails, `prior_survival` dict is empty. The DELETE on line 1627 then wipes all existing encumbrances. When the chain is rebuilt, all prior lien survival annotations (survival_status, is_joined, is_inferred) are **permanently lost**. A full re-run of Step 6 (survival analysis) is needed but nothing triggers it.
**Available at failure:** `folio` (the property being rebuilt), `conn`. The query targets `encumbrances WHERE folio = ?`.
**Add to log:** `logger.error(f"Failed to preserve prior survival data for {folio}: {e} — survival annotations will be lost on rebuild")`.

### 18) hcpa_gis_scraper.py: `_wait_for_content` returns False without logging
**Location:** `src/scrapers/hcpa_gis_scraper.py:86-90`
**Pattern:** `except Exception:` — cancels tasks, returns `False`, no `as e`, no logging
**Impact:** `_wait_for_content` tells `scrape_hcpa_property` that content didn't load, but gives zero visibility into why. The caller proceeds as if content simply timed out, potentially producing empty/incomplete HCPA data.
**Available at failure:** `tasks` (list of asyncio tasks for each selector), the selectors being waited on (defined in the caller). The exception type and message are lost.
**Add to log:** `logger.error(f"_wait_for_content failed: {e}")` with the exception. Also log which selectors were being waited on.

### 19) hcpa_gis_scraper.py: Sales history error uses `print()` instead of `logger`
**Location:** `src/scrapers/hcpa_gis_scraper.py:385-386`
**Pattern:** `except Exception as e: print(f"Error extracting sales history: {e}")`
**Impact:** This is inside `scrape_hcpa_property()`, a core production function called by the orchestrator. Sales history extraction failure means missing data for chain-of-title analysis. Error is invisible in log files.
**Available at failure:** `e` (the exception), `result` (the partial result dict built so far, including `result.get("property_id")` / `result.get("folio")`), `page` (Playwright page object with URL).
**Add to log:** `logger.error(f"Error extracting sales history for {result.get('folio', 'unknown')}: {e}")`.

### 20) step4v2/discovery.py: Gap-search RateLimitError and Exception without logging
**Location:** `src/services/step4v2/discovery.py:350-353`
**Pattern:** `except RateLimitError:` and `except Exception as e:` — both mark DB state but produce **zero log output**
**Impact:** When gap searches fail or get rate-limited, there is zero visibility. The non-gap search block at lines 315-321 does log, but the gap-search variant is silent.
**Available at failure:** `search.id`, `folio`, `search` object (has `search_type`, `search_term`, `date_from`, `date_to`), `iteration` count, `e` (for Exception).
**Add to log:** `logger.warning(f"Rate limited on gap-search {search.id} for {folio}")` and `logger.error(f"Gap-search {search.id} failed for {folio}: {e}")`.

### 21) step4v2/search_queue.py: `_get_sales_history()` catches all exceptions
**Location:** `src/services/step4v2/search_queue.py:199-215`
**Pattern:** `except Exception: return []` — no `as e`, no logging
**Impact:** This catches ALL exceptions, not just `sqlite3.OperationalError` for a missing table. A corrupt database, connection issue, or column mismatch would all be silently swallowed. Returns empty list, so the search queue generates no history-based searches.
**Available at failure:** `folio` (input), `self.conn` (connection). The query targets `sales_history WHERE folio = ? OR strap = ?`.
**Add to log:** Narrow to `except sqlite3.OperationalError as e: logger.debug(...)`. Re-raise or log at `warning` for all other exception types.

### 22) orchestrator.py: `bulk_parcels` query `except Exception: pass`
**Location:** `src/orchestrator.py:1708-1715`
**Pattern:** `except Exception: pass` on `SELECT * FROM bulk_parcels WHERE folio = ? OR strap = ?`
**Impact:** If bulk_parcels exists but query fails (locked DB, corrupt data), ORI ingestion proceeds without bulk data. Missing bulk data means no owner/address pre-hydration.
**Available at failure:** `folio` (property folio), `conn` (the SQLite connection). Comment says "bulk_parcels may not exist" but `except Exception` is too broad.
**Add to log:** `logger.debug(f"bulk_parcels query failed for {folio}: {e}")`.

### 23) bulk_parcel_ingest.py: `before_count` query silently returns 0
**Location:** `src/ingest/bulk_parcel_ingest.py:340-341`
**Pattern:** `except Exception: before_count = 0` — no `as e`, no logging
**Impact:** Silently hides DB errors (missing table, connection issues). The next line (`DELETE FROM bulk_parcels`) may then fail with an unhandled error anyway. The `before_count` is used for logging the import delta.
**Available at failure:** `conn` (connection). The query is `SELECT COUNT(*) FROM bulk_parcels`.
**Add to log:** `logger.debug(f"bulk_parcels count query failed (table may not exist): {e}")`.

### 24) bulk_parcel_ingest.py: DB fallback to parquet without logging
**Location:** `src/ingest/bulk_parcel_ingest.py:603`
**Pattern:** `except Exception:` — falls back to parquet file, no logging
**Impact:** If the DB connection is broken (corruption, permission issue), the error is hidden. The fallback to parquet produces different data than the DB.
**Available at failure:** `conn` (connection), the query, and the DB path.
**Add to log:** `logger.warning(f"bulk_parcels DB query failed, falling back to parquet: {e}")`.

---

## MEDIUM

### 25) hcpa_gis_scraper.py: Sales history link `href` extraction suppressed
**Location:** `src/scrapers/hcpa_gis_scraper.py:356,363`
**Pattern:** `with suppress(Exception):` on `await book_page_link.get_attribute("href")` and `await instrument_link.get_attribute("href")`
**Impact:** These links connect sales to ORI documents. If extraction fails silently, `link_href`/`instrument_href` stay `None` and the ORI connection for that sale is lost.
**Available at failure:** `cells` (table row cells), `book_page_link` and `instrument_link` (Playwright locators), `sale_record` being built (has book, page, date).
**Add to log:** `logger.debug(f"Failed to extract link href from sales history row: {e}")`.

### 26) hcpa_gis_scraper.py: Permit link extraction suppressed
**Location:** `src/scrapers/hcpa_gis_scraper.py:515-521`
**Pattern:** `with suppress(Exception):` wrapping entire permit link extraction in loop
**Impact:** If any single permit fails to extract, it's silently skipped. Can cause undercounting of permits.
**Available at failure:** `permit_link` (Playwright locator), loop index, `result["permits"]` (collected so far).
**Add to log:** `logger.debug(f"Failed to extract permit link: {e}")`.

### 27) tax_scraper.py: Tax detail page selector timeout with no logging
**Location:** `src/scrapers/tax_scraper.py:211`
**Pattern:** `except Exception: await asyncio.sleep(5)` — no `as e`, no logging
**Impact:** After clicking the detail "View" link, waits for tax selector. If it never appears, silently falls through. Subsequent extraction may run on a page that didn't load.
**Available at failure:** `page` (Playwright page, has `.url`), the selector string being waited for, `folio` (available in outer scope).
**Add to log:** `logger.debug(f"Tax detail page selector not found for {folio}: {e}")`.

### 28) operations.py: `folio_has_flood_data` returns False on error
**Location:** `src/db/operations.py:2640-2651`
**Pattern:** `except Exception: return False` — no `as e`, no logging
**Impact:** Query error returns "no data" instead of flagging the error. Property may be re-scraped unnecessarily, or worse, the error indicates a broken connection that will cascade.
**Available at failure:** `folio` (input), `conn` (connection).
**Add to log:** `logger.debug(f"folio_has_flood_data({folio}) query failed: {e}")`.

### 29) operations.py: `folio_has_sunbiz_data` returns False on error
**Location:** `src/db/operations.py:2725-2732`
**Pattern:** `except Exception: return False` — same as above
**Available at failure:** `folio`, `conn`.
**Add to log:** `logger.debug(f"folio_has_sunbiz_data({folio}) query failed: {e}")`.

### 30) operations.py: `folio_has_homeharvest_data` returns False on error
**Location:** `src/db/operations.py:2734-2744`
**Pattern:** `except Exception: return False` — same as above
**Available at failure:** `folio`, `conn`.
**Add to log:** `logger.debug(f"folio_has_homeharvest_data({folio}) query failed: {e}")`.

### 31) operations.py: `save_liens` uses `print()` instead of `logger`
**Location:** `src/db/operations.py:1123-1124`
**Pattern:** `except Exception as e: print(f"Error in save_liens: {e}"); raise`
**Impact:** Does re-raise (so not truly silent), but error is invisible in log files.
**Available at failure:** `e` (exception), method parameters (folio, liens data, case_number).
**Add to log:** Replace `print(...)` with `logger.error(f"Error in save_liens for {folio}: {e}")`.

### 32) step4v2/discovery.py: `_parse_ori_date()` silently returns None
**Location:** `src/services/step4v2/discovery.py:632-649`
**Pattern:** `except Exception: return None` — no `as e`, no logging
**Impact:** Timestamp parsing failure means document's recording date is silently set to None. Affects chain-of-title ordering and lien priority.
**Available at failure:** `date_val` (the input — could be int, float, or other), `self` (has folio context from the calling method).
**Add to log:** `logger.debug(f"_parse_ori_date failed for value {date_val!r} (type={type(date_val).__name__}): {e}")`.

### 33) vision_service.py: `check_server()` double-silent exception
**Location:** `src/services/vision_service.py:1407-1423`
**Pattern:** Two nested `except Exception: return False` — no `as e`, no logging
**Impact:** Health check returns False but nobody knows why. Connection errors, DNS failures, SSL errors are completely hidden.
**Available at failure:** `self.API_URL`, `self.active_model`, `self.session` (requests Session).
**Add to log:** `logger.debug(f"Vision health check failed for {self.API_URL}: {e}")` in both except blocks.

### 34) lien_survival/priority_engine.py: Instrument number comparison silently falls through
**Location:** `src/services/lien_survival/priority_engine.py:74-80`
**Pattern:** `except (ValueError, TypeError): pass` — falls through to less reliable date comparison
**Impact:** When instrument numbers can't be parsed as integers (alphanumeric IDs), lien priority falls back to LP date comparison. Affects same-day recording tie-breaking.
**Available at failure:** `target_inst`, `foreclosing_inst` (the string instrument numbers being compared).
**Add to log:** `logger.debug(f"Could not compare instruments as integers: {target_inst!r} vs {foreclosing_inst!r}")`.

### 35) geocoder.py: Corrupt cache silently discarded
**Location:** `src/services/geocoder.py:18-23`
**Pattern:** `except json.JSONDecodeError: return {}` — no logging
**Impact:** Corrupt geocode cache is silently discarded. All cached geocode data is lost.
**Available at failure:** `CACHE_PATH` (the file path to the cache JSON).
**Add to log:** `logger.warning(f"Corrupt geocode cache at {CACHE_PATH}, starting fresh")`.

### 36) ingestion_service.py: Timestamp-to-date conversion silently fails
**Location:** `src/services/ingestion_service.py:2006-2010`
**Pattern:** `except (ValueError, OSError): pass` — no logging
**Impact:** Recording date for document set to None. Affects chain ordering.
**Available at failure:** `date_val` (the input int/float timestamp), the document being built (has `instrument`, `doc_type`).
**Add to log:** `logger.debug(f"Could not parse timestamp {date_val!r} for document")`.

### 37) batch_title_search.py: Recording date parse suppressed (2 instances)
**Location:** `src/services/batch_title_search.py:209,585`
**Pattern:** `with suppress(Exception):` on `datetime.strptime(date_str.split()[0], "%m/%d/%Y")`
**Impact:** Recording date parsing failure means ORI documents saved with NULL recording dates. These dates are critical for chain-of-title ordering and lien priority analysis.
**Available at failure:** `date_str` (the raw date string from ORI), the document being built (has `instrument`, `doc_type`).
**Add to log:** Replace `suppress` with `try/except: logger.debug(f"Could not parse ORI date: {date_str!r}")`.

### 38) orchestrator.py: judgment JSON parse without logging
**Location:** `src/orchestrator.py:1721-1724`
**Pattern:** `except json.JSONDecodeError: final_judgment = {}` — no logging
**Impact:** Corrupted judgment JSON is silently treated as empty dict. ORI ingestion proceeds without judgment data (no legal description, no party names to seed searches).
**Available at failure:** `final_judgment` (the raw string), `folio`, `case_number` (from `auction` dict).
**Add to log:** `logger.warning(f"Corrupt judgment JSON for {folio}/{case_number}: {final_judgment[:100]}")`.

### 39) orchestrator.py: Survival analysis judgment JSON parse suppressed (2 instances)
**Location:** `src/orchestrator.py:370-371,598-599`
**Pattern:** `with suppress(json.JSONDecodeError, TypeError):` on `json.loads(raw_judgment)`
**Impact:** Corrupted judgment data silently treated as empty dict → empty survival analysis for the property.
**Available at failure:** `raw_judgment` (the string), `folio` (from auction).
**Add to log:** `logger.warning(f"Could not parse judgment data for survival analysis on {folio}")`.

### 40) title_chain_service.py: `_safe_datetime()` returns `datetime.min` sentinel
**Location:** `src/services/title_chain_service.py:778-787`
**Pattern:** `except ValueError: return datetime.min.replace(tzinfo=UTC)` — no logging
**Impact:** Unparseable dates silently become `datetime.min`, placed at the beginning of all chronological sorts. Can misorder chain-of-title entries.
**Available at failure:** `date_val` (the unparseable input).
**Add to log:** `logger.debug(f"Unparseable date '{date_val}' in chain, using datetime.min sentinel")`.

### 41) parcel_resolver.py: `_parse_judgment()` silent JSON failure
**Location:** `src/services/parcel_resolver.py:447-451`
**Pattern:** `except (json.JSONDecodeError, TypeError): return None` — no logging
**Impact:** Judgment data stored in DB is corrupted/unparseable. Resolver proceeds without judgment data. If many records are affected, resolution rate drops silently.
**Available at failure:** `raw` (the raw JSON string, first 100 chars useful for diagnosis).
**Add to log:** `logger.debug(f"Could not parse judgment JSON: {raw[:100] if raw else 'None'}")`.

### 42) app/web/database.py: Column existence checks too broad (4 instances)
**Location:** `app/web/database.py:578,862,1270,1314`
**Pattern:** `except Exception: return []` / `return 0` — meant to check if column/table exists
**Impact:** These catch `sqlite3.OperationalError` for missing columns, but also catch connection errors, permission issues, and corruption. Returns empty/zero, making the web UI show no data.
**Available at failure:** `conn` (connection), the specific SQL query being tested.
**Add to log:** Narrow to `except sqlite3.OperationalError as e: logger.debug(...)`. Re-raise or log other exceptions.

### 43) document_analyzer.py: PDF OCR error uses `print()` instead of `logger`
**Location:** `src/analyzers/document_analyzer.py:62-64`
**Pattern:** `except Exception as e: print(f"Error processing PDF {pdf_path}: {e}")`
**Impact:** Errors during PDF OCR extraction are not captured in log files, only visible in console.
**Available at failure:** `pdf_path` (the file path), `e` (exception).
**Add to log:** `logger.error(f"Error processing PDF {pdf_path}: {e}")`.

### 44) bulk_parcel_ingest.py: `get_bulk_parcel_for_folio` returns None on error
**Location:** `src/ingest/bulk_parcel_ingest.py:765-767`
**Pattern:** `except sqlite3.OperationalError: conn.close(); return None` — no logging
**Impact:** Callers think the folio simply doesn't exist in bulk_parcels, but the real issue may be a DB problem.
**Available at failure:** `folio` (input), `conn`.
**Add to log:** `logger.debug(f"bulk_parcels lookup failed for {folio}: {e}")`.

### 45) bulk_parcel_ingest.py: `get_count()` and distribution queries return 0/empty on error
**Location:** `src/ingest/bulk_parcel_ingest.py:797-798,836-838`
**Pattern:** `except sqlite3.OperationalError: return 0` / `= []` — no logging
**Impact:** Validation statistics become misleading (reports 0 records when the issue is a missing table or bad query).
**Available at failure:** `conn`, the query being executed.
**Add to log:** `logger.debug(f"Validation query failed: {e}")`.

### 46) permit_scraper.py: Date parsing uses broad `suppress(Exception)`
**Location:** `src/scrapers/permit_scraper.py:576`
**Pattern:** `with suppress(Exception):` on date parsing (compare line 431 which correctly uses `suppress(ValueError, TypeError)`)
**Impact:** Could hide non-date-related errors like `AttributeError` if `date_str` is unexpectedly non-string.
**Available at failure:** `date_str` (the raw date string), permit data context.
**Add to log:** Narrow to `suppress(ValueError, TypeError)` to match line 431 pattern, or replace with explicit `try/except` with `logger.debug`.

### 47) app/web/database.py: Judgment JSON parse suppressed (display layer)
**Location:** `app/web/database.py:461,1365`
**Pattern:** `with suppress(Exception):` on `json.loads(auction["extracted_judgment_data"])`
**Impact:** If judgment JSON is malformed, the raw string is kept for display. Low risk since display-only, but corruption would be invisible.
**Available at failure:** `auction` dict (has `case_number`), the raw JSON string.
**Add to log:** `logger.debug(f"Could not parse judgment JSON for display: case={auction.get('case_number')}")`.

---

## LOW (Acceptable — documented for completeness)

These patterns are acceptable by design (type conversion defaults, date format fallback chains, DDL idempotency, file cleanup, asyncio cancellation). Documented so future audits can skip them.

- **operations.py:85-90** `_safe_exec`: DDL-only silent executor (ALTER TABLE ADD COLUMN). Duplicate column errors are expected.
- **operations.py:100-107** `table_exists()`: DDL introspection. Returns False on error = safe default.
- **operations.py:270-277** RENAME COLUMN suppress: DDL idempotency. Already-renamed columns are expected.
- **operations.py:283-295** CREATE INDEX suppress: DDL with `IF NOT EXISTS`. Edge case guard.
- **operations.py:2790-2793** PRAGMA table_info suppress: DDL introspection. Empty cols = safe default.
- **operations.py:2799-2802** DROP COLUMN suppress: SQLite <3.35 doesn't support DROP COLUMN.
- **operations.py:3210-3223** `backfill_status_steps` DDL: DDL idempotency helpers.
- **writer.py:46-47** `suppress(CancelledError)`: Standard asyncio shutdown pattern.
- **writer.py:92-93** `except TimeoutError: continue`: Intentional queue polling.
- **hcpa_gis_scraper.py:83-84** Cancelled task cleanup: Expected `CancelledError` from asyncio.
- **hcpa_gis_scraper.py:572-573** Force-kill browser after timeout: Timeout already logged.
- **auction_scraper.py:1016-1017** `_parse_amount`: Type conversion with None default.
- **auction_scraper.py:1092** `suppress(ValueError, TypeError)` on year_built: Narrow scope.
- **auction_scraper.py:1112-1113** Page close cleanup: Primary error already logged.
- **tax_scraper.py:455,548** `except ValueError: continue` in amount loops: Format fallback chain.
- **tax_deed_scraper.py:182** `_parse_amount`: Type conversion with None default.
- **sunbiz_scraper.py:376** `suppress(ValueError, TypeError)` on date: Narrow scope.
- **permit_scraper.py:360** `suppress(Exception)` on error screenshot: Primary error already logged.
- **permit_scraper.py:431** `suppress(ValueError, TypeError)` on date: Narrow scope.
- **ori_api_scraper.py:444** `except RuntimeError` for event loop detection: Standard pattern.
- **ori_api_scraper.py:551,600** Date format fallback chain: Outer handler logs.
- **vision_service.py:125-152** JSON repair chain: Final attempt logs.
- **vision_service.py:1325-1328** Error body reading guard: Parent logs.
- **homeharvest_service.py:438-449** `val()` type conversion: Returns None for bad values.
- **homeharvest_service.py:452-461** `date_val()` fallback: Preserves value as string.
- **ingestion_service.py:2013-2019** Date format chain: Outer handler logs at line 2070.
- **orchestrator.py:324-334,434-455** `suppress(ValueError, TypeError)` on dates: Narrow scope.
- **document_analyzer.py:85,95** `suppress(Exception)` on amount/rate parsing: Intentional filtering.
- **chain_builder.py:72-73,209-210,247-248** Type conversions: `ValueError` returns None.
- **bulk_parcel_ingest.py:164-165** `safe_float()`: Type conversion utility.
- **bulk_parcel_ingest.py:645-646** ALTER TABLE ADD COLUMN: Standard SQLite idempotency.
- **utils/time.py:38-42,58-64** `suppress(ValueError)` on dates: Multi-format parsing.
- **utils/db_lock.py:59-60** `except OSError: pass` on lock cleanup: Best-effort.
- **utils/amount_validator.py:38-39** `parse_amount()`: Type conversion utility.
- **utils/legal_description.py:774-775,784-785** `except ValueError: pass`: Numeric detection.
- **app/web/database.py:922-923** `_safe_float()`: Type conversion utility.
- **app/web/database.py:932-933** `_safe_json()`: Display-layer JSON parsing.
- **app/web/database.py:1149-1151** JSON parse in enrichment batch: Display-only.
- **services/document_analyzer.py:209-211** Temp file cleanup: `suppress(Exception)` on `unlink()`.
- **services/final_judgment_processor.py:121-123** Temp file cleanup: Same.
- **services/encumbrance_amount_extractor.py:163-165** Temp file cleanup: Same.
- **services/party2_resolution_service.py:286-288** Temp file cleanup: Same.
- **services/final_judgment_processor.py:271-277** `_parse_amount()`: Type conversion utility.
