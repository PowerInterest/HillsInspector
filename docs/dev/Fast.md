# High-Performance Multi-Threaded Architecture Plan (`Fast.md`)

## 1. Executive Summary
The current `HillsInspector` pipeline operates sequentially (Step 1 → Step 13), processing the entire batch of properties through each step before moving to the next. While logical, this is inefficient for I/O-bound tasks (scraping) and leaves CPU resources idle during network waits. 

**Objective:** Refactor the pipeline to maximize throughput by running independent tasks in parallel, utilizing `asyncio` for I/O-bound scrapers and `ProcessPoolExecutor` for CPU-bound analysis (OCR/Vision), while respecting strict data dependencies.

## 2. Data Dependency Graph

The pipeline organizes into three distinct phases based on data availability.

### Phase 1: Seed Generation (Sequential Start)
*Must complete before any other phase begins.*

| Component | Input | Output | Dependencies |
| :--- | :--- | :--- | :--- |
| **Auction Scraper** | Date Range | List of `AuctionProperty` objects<br>(Case #, Auction Date, Address, Pre-Assigned Folio) | None |

### Phase 2: Property Enrichment (Parallel Fan-Out)
*Can start immediately after Phase 1. All tasks in this phase are independent of each other and can run concurrently per property.*

| Component | Input Requirements | Data Source | Output Data |
| :--- | :--- | :--- | :--- |
| **HCPA Scraper** | `address` OR `folio` | Phase 1 | Owner Name, Legal Description, Verified Folio, Market Value |
| **Tax Scraper** | `folio` | Phase 1 OR HCPA | Tax Status, Delinquent Years, Total Due |
| **Permit Scraper** | `address` | Phase 1 | Open/Expired Permits, Violations |
| **Market Scraper** | `address` | Phase 1 | Zestimate, Rent Est, Listing Status, Photos |
| **FEMA Scraper** | `address` OR `folio` | Phase 1 | Flood Zone Code |

**Constraint:** The **HCPA Scraper** is a "soft dependency" for others because it verifies the Folio and Address. Ideally, run HCPA first, then fan out to Tax/Permit/Market/FEMA using the verified data.

### Phase 3: Title & Legal Analysis (Sequential Deep Dive)
*Requires "Owner Name" and "Legal Description" from Phase 2 (HCPA).*

| Step | Component | Input Requirements | Output |
| :--- | :--- | :--- | :--- |
| 3.1 | **ORI Scraper** | `owner_name` (Current & Prior), `legal_desc` | List of Document Metadata (Book/Page, Type) |
| 3.2 | **Doc Downloader** | Document Metadata | PDF/Image Files on Disk |
| 3.3 | **Vision Service** | PDF/Image Files | Extracted Text/JSON (Parties, Dates, Amounts) |
| 3.4 | **FJ Processor** | Final Judgment PDF | Validated Judgment Amount, Defendants, Case # |
| 3.5 | **Chain Builder** | All Extracted Data | Title Chain, Surviving Liens, Equity Analysis |

## 3. Proposed Architecture

### 3.1 The "Property Processor" Unit
Instead of running "Step 4 for ALL properties", we will define a `process_property(property_id)` workflow that handles a single property through Phases 2 and 3.

```python
async def process_property(property):
    # Phase 2: Enrichment (Fan-out)
    hcpa_task = asyncio.create_task(hcpa_scraper.fetch(property))
    await hcpa_task # Wait for verified data
    
    # Parallelize independent scrapers
    enrichment_results = await asyncio.gather(
        tax_scraper.fetch(property),
        permit_scraper.fetch(property),
        market_scraper.fetch(property),
        fema_scraper.fetch(property)
    )
    
    # Phase 3: Legal (Sequential)
    if property.is_worth_analyzing(): # Early exit check
        docs = await ori_scraper.search(property.owner)
        files = await downloader.download(docs)
        data = await vision_service.extract_batch(files) # GPU/API bound
        analysis = chain_builder.analyze(data)
```

### 3.2 Concurrency & Rate Limiting
*   **Global Semaphore:** Limit concurrent processing of properties (e.g., `5` properties at a time) to control overall resource usage.
*   **Service Semaphores:** Specific rate limits for sensitive sites (e.g., `HCPA_LIMIT = 10`, `ORI_LIMIT = 3`).

### 3.3 Database Interaction (Thread-Safety)
DuckDB is not write-safe across multiple threads/processes by default.
*   **Solution:** **Single-Writer Queue**.
*   All worker tasks push results to an `asyncio.Queue`.
*   A dedicated `db_writer` task pulls from the queue and commits to DuckDB.

## 4. Refactoring Plan

### Step 1: Database Abstraction
*   Create `src/db/writer.py` containing the `DBQueue` class.
*   Update all scrapers to return Pydantic models instead of writing directly to DB.

### Step 2: Unify "Phase 2" Scrapers
*   Create `src/workflows/enrichment.py`.
*   Implement `enrich_property(property)` function that calls HCPA, Tax, Permit, Market, FEMA.
*   Use `asyncio.gather` with semaphores.

### Step 3: Optimization of Vision/ORI
*   The **Vision Service** is the slowest component.
*   Implement batching: Send images to the local LLM server in optimized batches or ensure the queue keeps the GPU fed without overloading it (as seen in previous VRAM issues).

### Step 4: Master Orchestrator
*   Rewrite `src/pipeline.py` to:
    1.  Run Auction Scraper (Seed).
    2.  Initialize DB Queue.
    3.  Create a `WorkerPool` (e.g., 5-10 workers).
    4.  Feed Auction Properties into the pool.
    5.  Wait for completion.

## 5. Scraper Data Requirements

| Scraper | Required Fields | Optional Fields | Criticality |
| :--- | :--- | :--- | :--- |
| **Auction** | Date Range | | **Blocking** |
| **HCPA** | `address` or `folio` | | **High** (Source of Truth) |
| **Tax** | `folio` | `owner_name` | Med |
| **Permit** | `address` | | Low |
| **Market** | `address` | `zip_code` | Med |
| **ORI (Official Records)** | `owner_name` (First, Last) | `legal_description` | **High** (Title Chain) |
| **Vision/OCR** | `file_path` | `document_type` | **High** |

## 6. Immediate Next Steps
1.  **Fix Vision Service Stability:** Ensure the local LLM server is stable (done via image resizing fix).
2.  **Refactor DB:** Implement the Queue-based writer.
3.  **Parallelize Phase 2:** Speed up the scraping of ancillary data.
