# TODO

## High Priority

### OnBase API Discovery
The backend is **Hyland OnBase**. Leverage [OnBase Documentation](https://support.hyland.com/r/OnBase/Public-Sector-Constituency-Web-Access/English/Foundation-22.1/Public-Sector-Constituency-Web-Access/Configuration/Front-End-Client-Configuration/Search-Panel-Settings/Configuring-Custom-Queries/Predefine-Keyword-Values-to-Search/Dynamic-Keyword-Values) to discover more advanced search capabilities and potential API endpoints.

### Court Case Search
Implement scraping for CQIDs 324-348 to find foreclosure and probate cases.

### Permit Analysis
Integrate `HillsGovHubScraper` to verify NOCs (Notice of Commencement) against actual permits.

## Medium Priority

### Avoid Rework
If data (judgments, liens, geocodes) already exists, skip reprocessing. Only fill gaps:
- Geocode missing lat/lon
- Skip PDFs already extracted
- Don't re-download existing documents

## Low Priority

### Bot Detection Mitigation
Some sources (HOVER, Realtor.com) have aggressive bot detection. Current mitigations:
- Stealth User-Agent
- Headed mode option (`headless=False`)
- IP rotation (manual)

### HOVER Scraper
Investigate `hover_scraper.py` - determine if it's still useful or should be removed. Currently not integrated into pipeline.
