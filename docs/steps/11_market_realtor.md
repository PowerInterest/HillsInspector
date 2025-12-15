# Realtor Scraper

## Overview
The `RealtorScraper` gathers detailed property data from Realtor.com, supplementing Zillow data with better HOA info, price history, and descriptions. It uses `VisionService` and `playwright-stealth`.

## Source
- **URL**: `https://www.realtor.com`
- **Type**: Web Scraping (Playwright Stealth) + Vision API

## Inputs
- **Address**: Street address, city, state, zip code.
- **Property ID**: For storage.

## Outputs
- **RealtorListing**: Object containing:
    - Price / Status
    - HOA Fees & Frequency
    - Price History
    - Property Details (Beds, Baths, Sqft, Year Built)
- **Files Stored via ScraperStorage**:
    - **Screenshots**: Full page screenshots saved to `data/properties/{property_id}/screenshots/realtor/realtor_{address}_{timestamp}.png`
    - **Vision Output**: Extracted data saved to `data/properties/{property_id}/vision/realtor/{context}.json`

## Key Methods
- `get_listing_details(...)`: Navigates to the listing page, simulates human behavior, takes screenshots, and extracts data.
- `get_listing_for_property(...)`: Wrapper with caching support via `ScraperStorage`.
