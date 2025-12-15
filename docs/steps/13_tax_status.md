# Tax Scraper

## Overview
The `TaxScraper` searches the Hillsborough County Tax Collector's website for unpaid property taxes and liens.

## Source
- **URL**: `https://hillsborough.county-taxes.com/public`
- **Type**: Web Scraping (Playwright)

## Inputs
- **Parcel ID**: Property Parcel ID / Folio.

## Outputs
- **Lien Objects**: List of `Lien` objects representing unpaid taxes.
- **Files Stored via ScraperStorage**:
    - **Screenshots**: Search result screenshots saved to `data/properties/{property_id}/screenshots/tax_collector/search_results.png`

## Key Methods
- `get_tax_liens(parcel_id)`: Searches for tax records and identifies outstanding amounts.
