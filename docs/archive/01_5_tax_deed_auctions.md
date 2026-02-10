# Tax Deed Scraper

## Overview
The `TaxDeedScraper` scrapes tax deed auction data from the Hillsborough County RealTaxDeed website.

## Source
- **URL**: `https://hillsborough.realtaxdeed.com`
- **Type**: Web Scraping (Playwright)

## Inputs
- **Target Date**: Date to scrape auctions for.

## Outputs
- **Property Objects**: List of `Property` objects containing:
    - Case Number
    - Certificate Number
    - Parcel ID
    - Opening Bid
    - Assessed Value
- **Files Stored via ScraperStorage**:
    - **Screenshots**: Error screenshots saved to current directory (should be updated to use `ScraperStorage`).

## Key Methods
- `scrape_date(target_date)`: Scrapes all tax deed auctions for a specific date.
