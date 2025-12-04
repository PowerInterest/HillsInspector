# ORI Deep Search Strategy & Implementation

## Overview
The "Deep Search" strategy is a comprehensive approach to analyzing property title chains in Hillsborough County. It was developed to overcome the limitations of the main Official Records Index (ORI) search page, specifically:
1.  **Rate Limiting**: The main page (`/oripublicaccess/`) is heavily rate-limited and prone to blocking automated scrapers.
2.  **Complexity**: The main page uses complex dynamic loading (iframes, `jsgrid`) that is difficult to scrape reliably.
3.  **Completeness**: A simple "Legal Description" search often fails due to formatting inconsistencies and result truncation (server-side limit of ~100 rows).

Our solution leverages **Direct Search Endpoints (`PAVDirectSearch`)** identified by specific `CQID` (Custom Query ID) parameters. These endpoints are faster, simpler (standard HTML tables), and less prone to rate limiting.

> **Note**: The backend is confirmed to be **Hyland OnBase Public Sector Constituency Web Access**.
> *   `CQID` = **Custom Query ID** (Configured in OnBase Studio).
> *   `OBKey__<ID>_1` = **Dynamic Keyword Value** for Keyword Type `<ID>`.
> *   Reference: [OnBase Configuration Documentation](https://support.hyland.com/r/OnBase/Public-Sector-Constituency-Web-Access/English/Foundation-22.1/Public-Sector-Constituency-Web-Access/Configuration/Front-End-Client-Configuration/Search-Panel-Settings/Configuring-Custom-Queries/Predefine-Keyword-Values-to-Search/Dynamic-Keyword-Values)

## Identified Search Endpoints (CQIDs)

We identified these endpoints by systematically scanning `CQID` values from 0 to 500 and inspecting the returned HTML forms.

| CQID | Search Type | Key Parameters (`OBKey_*`) | Underlying Input ID (`obpa_kw_*`) | Notes |
|------|-------------|----------------------------|-----------------------------------|-------|
| **319** | **Book / Page** | `OBKey__573_1` (Book)<br>`OBKey__1049_1` (Page) | `obpa_kw_573`<br>`obpa_kw_1049` | **Primary Seed Method.** Used to find the initial Deed or Mortgage from HCPA Sales History. |
| **326** | **Name (Cross-Party)** | `OBKey__486_1` (Name) | `obpa_kw_486` | **Best for Owners.** Returns a rich table with Party Name, Date, Doc Type, and Instrument. Finds both Grantors and Grantees. |
| **318** | **Marriage Records** | `OBKey__486_1` (Name) | `obpa_kw_486` | **Name Change Detection.** Used to find marriage licenses to track name changes (e.g., Maiden Name -> Married Name). |
| **321** | **Legal Description** | `OBKey__1011_1` (Legal) | `obpa_kw_1011` | **Fallback.** Search by Subdivision Name + Wildcard (`*`). Prone to result truncation for large subdivisions. |
| **320** | **Instrument #** | `OBKey__1006_1` (Instr) | `obpa_kw_1006` | **Direct Lookup.** Fetch a specific document by its Instrument Number (e.g., found via Name Search). |
| **316** | **Master Search** | Various | Various | Contains all filters but returns a complex, virtualized grid that is hard to parse. We use **326** instead. |
| **324-348**| **Court Cases** | `OBKey__106_1` (Year)<br>`OBKey__107_1` (Seq) | `obpa_kw_106`<br>`obpa_kw_107` | **Future Use.** For finding Foreclosure, Probate, and Civil cases linked to owners. |

### How We Reverse-Engineered the Keys
1.  **Scanning**: We ran a script (`scan_all_cqids.py`) to visit `CQID=0` to `500`.
2.  **Inspection**: For each valid page, we extracted the `<input>` element IDs (e.g., `obpa_kw_486`).
3.  **Mapping**: We mapped these internal IDs to URL parameters:
    *   `obpa_kw_486` -> `OBKey__486_1`
    *   `obpa_kw_573` -> `OBKey__573_1`
    *   `obpa_kw_1049` -> `OBKey__1049_1`
    *   `obpa_kw_1011` -> `OBKey__1011_1`

## Chain of Title Analysis Flow

The `run_chain_analysis.py` script orchestrates the following flow:

### 1. Seed Document Retrieval (Book/Page)
*   **Source**: We start with a known Book/Page from the Property Appraiser's (HCPA) Sales History.
*   **Action**: Call `search_by_book_page(book, page)` (CQID 319).
*   **Goal**: Retrieve the actual Deed (or other recording) to identify the **Grantor** (Seller) and **Grantee** (Buyer).
*   **Note**: We also capture **NOCs (Notice of Commencement)** here if they appear in the book/page search (rare, but possible if referenced).

### 2. Owner Identification
*   **Extraction**: Parse the `party_name` from the seed documents.
*   **Logic**: Identify the current owner and previous owners.

### 3. Name Change Detection (Marriage/Divorce)
*   **Problem**: An owner might buy property as "Jane Doe" and later mortgage it as "Jane Smith" after marriage. A simple name search for "Jane Doe" might miss the mortgage.
*   **Action**: Call `search_marriage_records("Jane Doe")` (CQID 318).
*   **Logic**: If a marriage record is found linking "Jane Doe" to "John Smith", we add "Jane Smith" (and potentially "John Smith") to our search list.
*   **Divorce**: Similarly, divorce decrees (found via Court Search or Name Search) can trigger name reversions.

### 4. Comprehensive Name Search (Deep Scan)
*   **Action**: For *every* identified owner (and alias), call `search_by_name(name)` (CQID 326).
*   **Scope**: This finds **ALL** recorded documents for that person:
    *   **Deeds**: Transfers of ownership.
    *   **Mortgages**: Loans against the property.
    *   **Liens**: HOA liens, Contractor liens, Tax liens.
    *   **Lis Pendens**: Notices of pending lawsuits (Foreclosure).
    *   **NOCs**: Notices of Commencement (Construction).

### 5. Analysis & Categorization
*   **Service**: `TitleChainService` processes the raw list of documents.
*   **Chain of Title**: Sorts Deeds by date to reconstruct the ownership history.
*   **Encumbrances**: Identifies Mortgages and Liens. Matches "Satisfactions" and "Releases" to "Open" items to determine what is still active.
*   **NOCs**: Filters for `NOTICE OF COMMENCEMENT`.

## The Role of NOCs (Notice of Commencement)
*   **What is it?**: A document recorded by a property owner before starting construction improvements (roof, pool, remodel).
*   **Why it matters**:
    1.  **Permit Verification**: Every NOC *should* have a corresponding Building Permit. If we find an NOC but no Permit (via the Permit Scraper), it indicates **Unpermitted Work**.
    2.  **Lien Risk**: Open NOCs can lead to Mechanic's Liens if contractors aren't paid.
    3.  **Timeline**: Helps establish when major work was done on the property.

## Summary of Key Files
*   `src/scrapers/ori_scraper.py`: The core scraper implementing the Direct Search methods.
*   `run_chain_analysis.py`: The orchestrator script for the Deep Search flow.
*   `src/services/title_chain_service.py`: The logic for analyzing the chain and encumbrances.
