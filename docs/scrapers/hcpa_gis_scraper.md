# HCPA GIS Scraper

## Overview
The `hcpa_gis_scraper` scrapes detailed property information from the Hillsborough County Property Appraiser's GIS portal. It is the primary source for sales history, building characteristics, and legal descriptions.

## Source
- **URL**: `https://gis.hcpafl.org/propertysearch/`
- **Type**: Web Scraping (Playwright)

## Inputs
- **Parcel ID**: 19-digit URL-formatted parcel ID (e.g., `1829134XZ000012000090A`).
- **Folio**: Property Folio number (e.g., `1918870000`).

## Outputs
- **Dictionary**: Contains:
    - Sales History (Book/Page, Date, Price, Instrument)
    - Building Info (Year Built, Type)
    - Legal Description
    - Tax Collector Link
    - Permits (Basic info)
- **Files Stored via ScraperStorage**:
    - **Screenshots**: Full page screenshot saved to `data/properties/{property_id}/screenshots/hcpa_gis/property_details.png`
    - **Raw Data**: Scraped data dictionary saved to `data/properties/{property_id}/raw/hcpa_gis/property_details.json`
    - **Documents**: Downloaded deeds/instruments from sales history saved to `data/properties/{property_id}/documents/deed_{doc_id}.pdf`

## Key Methods
- `scrape_hcpa_property(parcel_id, folio)`: Main scraping function.
- `fetch_sales_documents(hcpa_result)`: Follows links in sales history to download documents from the Clerk's PAV system.
