# ORI API Scraper

## Overview
The `ORIApiScraper` interacts with the hidden API of the Hillsborough County Official Records Index (ORI) to search for recorded documents by legal description or party name.

## Source
- **URL**: `https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search`
- **Type**: API (Reverse Engineered)

## Inputs
- **Legal Description**: Text to search within the legal description field.
- **Party Name**: Name of a party involved in documents.
- **Date Range**: Optional start/end dates.

## Outputs
- **Document List**: List of dictionaries containing document metadata (Instrument #, Book/Page, Date, Type, Parties).
- **Files Stored via ScraperStorage**:
    - **PDFs**: Can download document PDFs (implementation in `download_pdf` saves to local path, should be integrated with `ScraperStorage`).

## Key Methods
- `search_by_legal(legal_description)`: Searches using the API.
- `search_by_legal_browser(legal_desc)`: Fallback using browser automation for `CQID=321` endpoint (bypasses API limits).
- `get_property_documents(folio, legal1, legal2)`: Orchestrates search for a specific property.
