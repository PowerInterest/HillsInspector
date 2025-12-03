# Hillsborough County Property Data Sources

This document outlines the primary data sources for the Hillsborough County property data acquisition project, along with the challenges and the proposed technical approach for each.

| Data Source | URL | Available Data | Challenges | Technical Approach |
| --- | --- | --- | --- | --- |
| **Property Appraiser GIS Search** | `https://gis.hcpafl.org/propertysearch/` | Detailed property information, including ownership, value, and structural details. | Blocked by `robots.txt`. Dynamic content. | Browser automation (Playwright) + **Qwen3vl AI** to simulate user interaction and bypass scraping restrictions. |
| **Property Appraiser Downloads** | `https://downloads.hcpafl.org/` | Bulk data files, including a comprehensive parcel spreadsheet and GIS shapefiles. | Links are dynamically generated with JavaScript, preventing direct downloads. | Browser automation (Playwright) + **Qwen3vl AI** to identify and click download links. |
| **Clerk of the Court** | `https://hillsclerk.com/taxdeeds` | General information about the tax deed process and links to the auction site. | None. This site is easily accessible. | Standard HTTP requests or Playwright. |
| **RealAuction Platform** | `https://hillsborough.realtaxdeed.com/` | Live auction data, including opening bids, auction schedules, and property details. | Blocks requests based on user-agent. Complex UI. | Browser automation (Playwright) with modified user agent + **Qwen3vl AI** to parse auction listings. |

## AI Strategy
To mitigate the fragility of traditional scraping (which breaks when HTML structures change), this project employs **Qwen3vl**.
*   **Visual Understanding**: The model can "see" the page (via screenshots) to find buttons and data fields even if IDs or classes change.
*   **Semantic Parsing**: Instead of writing complex RegEx, we feed the raw text/HTML to the model and ask it to extract JSON objects.
