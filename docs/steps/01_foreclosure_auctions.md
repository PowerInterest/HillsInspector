# Auction Scraper

## Overview
The `AuctionScraper` scrapes foreclosure auction data from the Hillsborough County RealForeclose website. It collects property details, auction dates, judgment amounts, and downloads Final Judgment PDFs.

## Source
- **URL**: `https://hillsborough.realforeclose.com`
- **Type**: Web Scraping (Playwright)

## Inputs
- **Date Range**: Start and end dates to scrape auctions for.
- **Target Date**: Specific date to scrape.

## Outputs
- **Property Objects**: List of `Property` objects containing:
    - Case Number
    - Parcel ID
    - Address
    - Assessed Value
    - Final Judgment Amount
    - Auction Date
    - Auction Type
- **Files Stored via ScraperStorage**:
    - **Final Judgment PDFs**: Saved to `data/properties/{property_id}/documents/final_judgment_{doc_id}.pdf`
    - **Vision Output**: Extracted data from Final Judgment PDFs saved to `data/properties/{property_id}/vision/final_judgment/{context}.json`
    - **Screenshots**: Error screenshots saved to `logs/` or current directory on failure.

## Key Methods
- `scrape_date(target_date)`: Scrapes all auctions for a specific date.
- `scrape_all(start_date, end_date)`: Scrapes a range of dates.
- `_download_final_judgment(...)`: Downloads the Final Judgment PDF from the Clerk's OnBase system.
- `_process_final_judgment(prop)`: Extracts structured data from the downloaded PDF using `VisionService`.
