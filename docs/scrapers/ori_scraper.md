# ORI Scraper

## Overview
The `ORIScraper` uses the "PAV Direct Search" endpoints of the Official Records Index to perform specific lookups (Book/Page, Instrument, Name). It parses the HTML table results.

## Source
- **URL**: `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html`
- **Type**: Web Scraping (Playwright)

## Inputs
- **Book/Page**: For specific document lookup.
- **Instrument Number**: For specific document lookup.
- **Name**: For cross-party search.
- **Legal Description**: For subdivision search.

## Outputs
- **Result List**: List of dictionaries representing the rows in the results table.
- **Files Stored via ScraperStorage**:
    - This scraper primarily returns metadata. Document downloading is handled by `hcpa_gis_scraper` or `ori_api_scraper`.

## Key Methods
- `search_by_book_page(book, page)`: Lookup by Book/Page (CQID=319).
- `search_by_name(name)`: Lookup by Name (CQID=326).
- `search_by_legal(legal_desc)`: Lookup by Legal Description (CQID=321).
- `fetch_instrument(instrument_number)`: Lookup by Instrument Number (CQID=320).
