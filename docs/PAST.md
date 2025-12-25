# Historical Auction Analysis Plan

**Objective**: Systematically gather and analyze historical foreclosure auction data to understand market trends. The primary focus is identifying properties sold to **third-party bidders** (investors) versus those reverting to the Plaintiff (banks), and comparing the **Winning Bid** against the **Final Judgment Amount**.

## Phase 1: Range Discovery & Scouting
*   **Goal**: Establish the "Event Horizon"—the earliest date for which records are accessible.
*   **Status**: **COMPLETED**.
*   **Result**: Earliest confirmed data is **Mid-2023** (found Oct 2023, missing Oct 2022).
*   **Action**: We will target the scraping range from **2023-06-01 to Present**.

## Phase 2: High-Velocity Skeleton Scraping
*   **Goal**: Rapidly ingest basic auction metadata for every date in the target range.
*   **Assumptions**: The target website currently has low rate-limiting protection.
*   **Strategy**:
    *   Run a lightweight scraper loop that only hits the daily index pages (e.g., `?zaction=AUCTION&AUCTIONDATE=...`).
    *   Use high concurrency (async `TaskGroup` or `gather`).
*   **Data Points to Capture** (into `auctions` table):
    *   `auction_date`
    *   `case_number`
    *   `status` (Sold, Cancelled, etc.)
    *   `sold_to` (Name of winner)
    *   `winning_bid` / `sale_amount`
    *   `final_judgment_amount` (as reported on the summary card)
    *   `property_address`
    *   `parcel_id`
*   **Observations/Fixes (2025-12)**:
    *   List view shows the winning bid under the label **"Amount"** inside `AUCTION_STATS` for sold auctions (not always "Winning Bid").
    *   Direct PREVIEW URLs can land on the calendar/login; loading the calendar and clicking the date reliably yields the list view.
    *   Upserts should not overwrite existing `sold_to`/`winning_bid` with NULL from a failed re-scrape (fixed in code).

## Phase 3: Identification of Third-Party Sales
*   **Goal**: Filter the dataset to finding "True Sales" (investor purchases).
*   **Logic**:
    *   **Exclude**: Rows where `sold_to` contains "Plaintiff", "Bank", "Mortgage", "Financial", "Lending", or specific servicing company names.
    *   **Exclude**: Rows where `status` is "Cancelled" or "Redeemed".
    *   **Include**: Rows where `winning_bid` > `100` (nominal bids often indicate admin transfers).

## Phase 4: Final Judgment & Depth Processing (Paused)
*   **Goal**: The nominal path would be to fetch PDFs and confirm the debt load, but we are temporarily pausing this for now.
*   **Approach**:
    *   Skip the Vision/PDF download step and instead focus on the resale lifecycle data we can gather more reliably.
    *   Judgment recovery checks (`Winning Bid / Final Judgment Amount`) remain a future enhancement once the PDFs are back on the roadmap.

## Phase 5: Flip Analysis (Investor Performance)
*   **Goal**: Track the full lifecycle of a 3rd party purchase with concrete ROI metrics.
*   **Metrics**:
    *   **Hold Time**: Days between `Auction Date` and `Resale Date`.
    *   **Gross Profit**: `Resale Price` - `Auction Winning Bid`.
    *   **ROI**: `Gross Profit / Winning Bid` (when the winning bid is available).
*   **Method**:
    1.  Identify 3rd party winner.
    2.  Monitor Property Appraiser / MLS data for a *subsequent* sale after the auction date.
    3.  Link the "Sold To" entity from auction to the "Grantor" in the resale deed (to confirm it's the same investor selling).

    ### [New] Flip Analysis (Investor Performance)
    *   **Goal**: Track the full lifecycle of a 3rd party purchase.
    *   **Metrics**:
        *   **Hold Time**: Days between `Auction Date` and `Resale Date`.
        *   **Gross Profit**: `Resale Price` - `Auction Winning Bid`.
        *   **ROI**: `Gross Profit / Winning Bid`.
    *   **Method**:
        1.  Identify 3rd party winner.
        2.  Monitor Property Appraiser / MLS data for a *subsequent* sale after the auction date.
        3.  Link the "Sold To" entity from auction to the "Grantor" in the resale deed (to confirm it's the same investor selling).

## Technical Decisions (Confirmed)
1.  **Database**: **Separate DB** (`data/history.db`).
    *   Keeps production lean.
    *   Allows specialized schema for historical analysis.
2.  **Scope**:
    *   **Skeleton Scrape**: All auctions in range.
    *   **Full Enrichment**: Only for **3rd Party Purchased Foreclosures**.
    *   **Enrichment Sources**: Property Appraiser (HCPA), HomeHarvest (MLS/Sold Data), Final Judgments.
3.  **Scraper Logic**:
    *   **Confirmed**: "Sold To" (e.g., "3rd Party Bidder") and "Sale Amount" are visible on the **List View**.
    *   **Implication**: We can do a high-speed "Skeleton Scrape" without visiting detail pages.
    *   We will only enter detail pages for confirmed 3rd Party Sales to fetch PDFs.
4.  **Concurrency**:
    *   High concurrency allowed for skeleton scrape, but throttled for deep enrichment.

## Final Database Schema (Confirmed)
```sql
-- 1. THE BUY: Auction Results
CREATE TABLE auctions (
    auction_id VARCHAR PRIMARY KEY, -- (case_number + date)
    auction_date DATE,
    case_number VARCHAR,
    parcel_id VARCHAR,
    property_address VARCHAR,
    
    -- Financials at Auction
    winning_bid DOUBLE,             -- Acquisition Cost
    final_judgment_amount DOUBLE,   -- Debt Load
    assessed_value DOUBLE,          -- Gov Value at Auction Time
    
    -- The Buyer
    sold_to VARCHAR,                -- Raw Name
    buyer_normalized VARCHAR,       -- Cleaned Name
    buyer_type VARCHAR,             -- 'Third Party', 'Plaintiff', 'Individual'
    
    -- Source Data
    auction_url VARCHAR,
    pdf_path VARCHAR,               -- NULL (Link Only Strategy)
    
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. THE EXIT: Resale Events (MLS & Deed)
CREATE TABLE resales (
    resale_id VARCHAR PRIMARY KEY,
    parcel_id VARCHAR,
    auction_id VARCHAR REFERENCES auctions(auction_id),
    
    -- Sale Details
    sale_date DATE,
    sale_price DOUBLE,
    sale_type VARCHAR,
    
    -- Performance Metrics
    hold_time_days INTEGER,
    gross_profit DOUBLE,
    roi DOUBLE,
    
    -- Validation
    source VARCHAR                  -- 'MLS', 'Deed'
);

-- 3. THE ASSET: Market Data & Media
CREATE TABLE property_details (
    parcel_id VARCHAR PRIMARY KEY,
    
    -- Comparables
    est_market_value DOUBLE,        -- AVM at time of Auction
    est_resale_value DOUBLE,        -- AVM at time of Resale
    value_delta DOUBLE,             -- (sale_price - est_resale_value)
    
    -- Media
    primary_image_url VARCHAR,
    gallery_json JSON,
    description TEXT,
    
    updated_at TIMESTAMP
);
```

## Phase 6: Web Interface Integration (NEW)
*   **Goal**: Visualize the historical analysis on the existing web app.
*   **Requirements**:
    *   New Route: `/history`
    *   **Dashboard View**: High-level charts (Profitable Flips vs Failed Flips, Top Investors).
    *   **Data Grid**: Searchable table of the `auctions` joined with `resales`.
    *   **Drill-down**: Click a row to see the "Lifecycle Card" (Images + Timeline of Buy/Sell).
