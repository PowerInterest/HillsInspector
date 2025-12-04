# Permit Scraper

## Overview
The `PermitScraper` collects building permit information from the Accela Citizen Access portals for both the City of Tampa and Hillsborough County.

## Source
- **City of Tampa URL**: `https://aca-prod.accela.com/TAMPA/Default.aspx`
- **Hillsborough County URL**: `https://aca-prod.accela.com/HCFL/Default.aspx`
- **Type**: Web Scraping (Playwright) + Vision API

## Inputs
- **Address**: Street address, city.
- **Property ID**: For storage.

## Outputs
- **Permit List**: List of `PermitDetail` objects (Number, Type, Status, Date, Description).
- **Files Stored via ScraperStorage**:
    - **Screenshots**: Search result screenshots saved to `data/properties/{property_id}/screenshots/permits/permit_{source}_{timestamp}.png`
    - **Vision Output**: Extracted permit data saved to `data/properties/{property_id}/vision/permits/{context}.json`
    - **Raw Data**: Scraped data recorded in database via `record_scrape`.

## Key Methods
- `get_permits(address, city)`: Queries both City and County portals based on location.
- `get_permits_for_property(...)`: Wrapper with caching support via `ScraperStorage`.
