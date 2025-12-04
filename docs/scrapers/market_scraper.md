# Market Scraper

## Overview
The `MarketScraper` gathers market data (estimates, status, photos) from Zillow and Realtor.com. It relies heavily on `VisionService` and `playwright-stealth` to bypass bot detection.

## Source
- **URLs**: 
    - `https://www.zillow.com`
    - `https://www.realtor.com`
- **Type**: Web Scraping (Playwright Stealth) + Vision API

## Inputs
- **Address**: Street address, city, state, zip code.
- **Property ID**: For storage.

## Outputs
- **ListingDetails**: Object containing:
    - Price / Estimates (Zestimate, Rent Zestimate)
    - Status (For Sale, Sold, Off Market)
    - Description
- **Files Stored via ScraperStorage**:
    - **Screenshots**: Listing page screenshots saved to `data/properties/{property_id}/screenshots/market_zillow/listing.png`
    - **CAPTCHA Screenshots**: Saved if CAPTCHA is encountered.
    - **Vision Output**: Extracted data saved to `data/properties/{property_id}/vision/market_zillow/listing.json`

## Key Methods
- `get_listing_details(...)`: Navigates to the listing page, handles CAPTCHAs (via Vision), takes a screenshot, and extracts data.
- `get_listing_with_captcha_handling(...)`: Wrapper with explicit CAPTCHA solving logic.
