# Brainstorming for Hillsborough County Property Data Project

## Goal:
Acquire data on properties in Hillsborough County, Florida that are in foreclosure, have tax liens, or are up for tax deeds.

## Initial thoughts on data sources:
*   **Hillsborough County Clerk of the Circuit Court:** This is likely the primary source for foreclosure records. We'll need to investigate their website for any public access portals or data download options.
*   **Hillsborough County Tax Collector:** This office will have information on tax liens and tax deeds. Their website should be checked for auction schedules, property lists, and data access methods.
*   **Third-party data aggregators:** Companies like RealtyTrac, Auction.com, or even Zillow might have this data, but it may come with a cost or be less direct.

## Technical approach:
*   **Web scraping:** This is a likely candidate if the data is available on public websites but not in a downloadable format. We'll need to be careful about the terms of service of the websites.
*   **APIs:** If the county offices provide APIs for data access, this would be the ideal solution. It's worth investigating if such services exist.
*   **Public records requests:** If all else fails, a formal public records request could be an option, but this is a slower, more manual process.

## Next steps:
1.  Research the websites for the Hillsborough County Clerk of the Circuit Court and the Tax Collector.
2.  Identify the specific web pages or portals that contain the target data.
3.  Analyze the structure of the data and the feasibility of scraping it.
4.  Look for any developer portals or API documentation.
