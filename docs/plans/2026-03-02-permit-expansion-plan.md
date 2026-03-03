# Permit Data Expansion Plan

Currently, the HillsInspector pipeline covers building permits for the two largest jurisdictions in the county via the Accela platform:
- **Hillsborough County (Unincorporated)** (`HCFL`)
- **City of Tampa** (`TAMPA`)

To achieve 100% geographic coverage within the county, we must integrate the remaining incorporated municipalities:
- **City of Plant City**
- **City of Temple Terrace**

## 1. Research & Discovery Phase

Unlike Tampa and Hillsborough County which share the Accela regional platform, Plant City and Temple Terrace maintain independent municipal building departments and IT infrastructure.

### Plant City
*   **Goal**: Identify the online permitting portal used by Plant City (e.g., Tyler Technologies, OpenGov, Energov, or a custom system).
*   **Action**: Investigate the plantcitygov.com building department page to find the public record search portal.
*   **API Recon**: Use browser network tools to identify if they expose a structured JSON API or if we need to parse HTML/use Playwright.

### Temple Terrace
*   **Goal**: Identify the permitting portal for Temple Terrace.
*   **Action**: Locate the community development/building permit search tool on templeterrace.com.
*   **API Recon**: Trace network requests during an address search to capture the API endpoints.

## 2. Abstraction & Schema Mapping

Our existing permit extractors (`permit_scraper.py`, `TampaPermit.py`) likely produce a standardized dictionary or ORM model for a permit:
*   `permit_number`
*   `permit_type`
*   `status`
*   `issue_date`
*   `description`
*   `jurisdiction`

*   **Action**: Create an interface (e.g., `BasePermitScraper`) if one doesn't exist, ensuring that the new scrapers for Plant City and Temple Terrace map seamlessly to our PostgreSQL schema.

## 3. Implementation of Scrapers

### `src/services/PlantCityPermit.py`
*   Build the request chain required to pass a property address (or parcel ID) into the Plant City search endpoint.
*   Handle pagination, rate limits, and authentication tokens (if the site uses CSRF/session tokens like Accela does).

### `src/services/TempleTerracePermit.py`
*   Replicate the search process for Temple Terrace.

## 4. Integration into the Pipeline Controller

*   **Action**: Update the main execution path (likely in `Controller.py` or the property enrichment pipeline module).
*   **Logic**: 
    1. Check a property's `city` or tax district code.
    2. Route the permit search dynamically:
        *   If Tampa $\rightarrow$ Use Tampa Scraper
        *   If Temple Terrace $\rightarrow$ Use Temple Terrace Scraper
        *   If Plant City $\rightarrow$ Use Plant City Scraper
        *   Else $\rightarrow$ Use Hillsborough County (HCFL) Scraper
    3. Ensure exceptions in one municipal scraper don't crash the pipeline for the property.

## 5. Verification
*   Identify 2-3 sample properties actively undergoing construction in Plant City and Temple Terrace.
*   Run the new scrapers locally via `uv run` to ensure they successfully fetch historical and active permits.
*   Verify the data lands perfectly in the API endpoints (e.g., `/property/{folio}/permits`).
