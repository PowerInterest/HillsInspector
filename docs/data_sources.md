# Hillsborough County Property Data Sources

This document outlines the primary data sources for the Hillsborough County property data acquisition project, along with the challenges and the proposed technical approach for each.

| Data Source | URL | Available Data | Challenges | Technical Approach |
| --- | --- | --- | --- | --- |
| **Property Appraiser GIS Search** | `https://gis.hcpafl.org/propertysearch/` | Detailed property information, including ownership, value, and structural details. | Blocked by `robots.txt`. | Browser automation (Playwright) to simulate user interaction and bypass scraping restrictions. |
| **Property Appraiser Downloads** | `https://downloads.hcpafl.org/` | Bulk data files, including a comprehensive parcel spreadsheet and GIS shapefiles. | Links are dynamically generated with JavaScript, preventing direct downloads. | Browser automation (Playwright) to click the download links and save the files. |
| **Clerk of the Court** | `https://hillsclerk.com/taxdeeds` | General information about the tax deed process and links to the auction site. | None. This site is easily accessible. | Standard HTTP requests for any general information. |
| **RealAuction Platform** | `https://hillsborough.realtaxdeed.com/` | Live auction data, including opening bids, auction schedules, and property details. | Blocks requests based on user-agent. | Browser automation (Playwright) with a modified user agent to mimic a real browser. |
