# Sunbiz Scraper

## Overview
The `SunbizScraper` searches the Florida Division of Corporations (Sunbiz) to identify business entities associated with property owners.

## Source
- **URL**: `https://search.sunbiz.org`
- **Type**: Web Scraping (Playwright)

## Inputs
- **Entity Name**: Business name to search.
- **Officer Name**: Name of officer/registered agent (property owner).
- **Document Number**: Specific entity ID.

## Outputs
- **BusinessEntity Objects**: List containing:
    - Name, Document Number, Status
    - Filing Date, State, Addresses
    - Officers/Registered Agents
- **Files Stored via ScraperStorage**:
    - **Raw Data**: Search results saved to `data/properties/{property_id}/raw/sunbiz/officer_search.json`

## Key Methods
- `search_entity(name)`: Search by business name.
- `search_by_officer(name)`: Search by officer name (useful for finding LLCs owned by a person).
- `search_for_property(...)`: Wrapper for property owner search with caching via `ScraperStorage`.
