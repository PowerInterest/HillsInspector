# HCPA Scraper

## Overview
The `HCPAScraper` is an alternative/supplementary scraper for the Property Appraiser's website. It uses VisionService to extract data from screenshots of the property details page, which can be more robust against layout changes than DOM parsing.

## Source
- **URL**: `https://gis.hcpafl.org/propertysearch/`
- **Type**: Web Scraping (Playwright) + Vision API

## Inputs
- **Property Object**: Requires `parcel_id` and `address`.

## Outputs
- **Enriched Property Object**: Updates the input property with:
    - Owner Name
    - Year Built
    - Beds/Baths
    - Heated Area
    - Image URL
- **Files Stored via ScraperStorage**:
    - **Screenshots**: Details page screenshot saved to `data/properties/{property_id}/screenshots/hcpa/details.png`
    - **Vision Output**: JSON extracted by VisionService saved to `data/properties/{property_id}/vision/hcpa/details.json`

## Key Methods
- `enrich_property(prop)`: Navigates to the property page, takes a screenshot, and uses `VisionService` to extract details.
