# Hillsborough County Property Data Sources

This document outlines the primary data sources for the Hillsborough County property data acquisition project. 

**Core Strategy Update (Visual Extraction):** Due to the low volume of target properties (approx. 50/week) and high anti-scraping complexity on certain sites, we will utilize **Visual Language Models (VLMs)**. 
* **Model:** Qwen-VL-32B (or similar).
* **Method:** Use Playwright to capture a full-page screenshot (or specific element screenshot) -> Pass image to Qwen-VL-32B -> Prompt for structured JSON output.

## 1. Official Government Sources (The "Legal Truth")
Use these sources to determine ownership, debt, and legal boundaries.

| Data Source | URL | Available Data | Challenges | Technical Approach |
| :--- | :--- | :--- | :--- | :--- |
| **Property Appraiser (Bulk)** | `https://downloads.hcpafl.org/` | **Source of Truth.** Massive Excel dump (`PARCEL_SPREADSHEET.xls`) containing every property, owner, address, and base value. | Download links are JS-generated. | **Native Download.** Use Playwright to access site, click download, and ingest the XLS daily. No VLM needed. |
| **Property Appraiser (API)** | `https://gis.tpcmaps.org/arcgis/rest/services/Parcels/MapServer` | Live spatial data, polygon coordinates, and zoning codes. | Rate limiting. | **Native API.** Query via REST API using `f=json` and `where=FOLIO='xxxx'`. VLM not efficient here. |
| **Permits (City of Tampa)** | `https://aca-prod.accela.com/TAMPA/` | Building permits, code violations, and zoning history for properties within **City Limits**. | Heavy .NET cookies; Captcha; Nested tables. | **Screenshot & VLM.** Navigate to "Building" module -> Search Address -> Screenshot the "Permit History" table -> Qwen-VL extract to JSON. |
| **Permits (County)** | `https://aca-prod.accela.com/HCFL/` | Building permits for **Unincorporated** areas (Brandon, Lutz, etc.). | Same Accela platform challenges as City. | **Screenshot & VLM.** Same strategy as City. Screenshot results page -> Qwen-VL extract. |
| **Clerk (Official Records)** | `https://publicrec.hillsclerk.com` | **Deeds & Mortgages.** Legal chain of title, liens, and mortgage documents. | Anti-scraping; "Daily Index" files are messy. | **FTP/Bulk preferred.** If scraping individual records: Screenshot the search result list -> Qwen-VL extract. |
| **Auction: Foreclosure** | `https://hillsborough.realforeclose.com/` | **Bank Foreclosures.** Properties sold due to mortgage default. | Calendar-based UI; Grid layouts are hard to parse code-wise. | **Screenshot & VLM.** Screenshot the "Calendar View" or individual "Auction Details" modal -> Qwen-VL extract: `{Date, Case#, JudgmentAmount}`. |
| **Auction: Tax Deed** | `https://hillsborough.realtaxdeed.com/` | **Tax Default.** Properties sold due to unpaid property taxes. | Different bidding rules/UI. | **Screenshot & VLM.** Screenshot the "Auction Calendar" -> Qwen-VL extract list of properties. |
| **Clerk (Court Cases)** | `https://hover.hillsclerk.com` | **Lawsuits.** Full docket of foreclosure lawsuits or probate cases. | Search by Party Name or Case Number only. | **Screenshot & VLM.** Search by Owner Name -> Screenshot the Docket list -> Qwen-VL extract active case numbers. |

## 2. Market Data Sources (The "Physical Truth")
Use these sources to determine condition, sentiment, and actual market value.

| Data Source | URL | Available Data | Challenges | Technical Approach |
| :--- | :--- | :--- | :--- | :--- |
| **Realtor.com** | `https://www.realtor.com` | **Visuals & Listings.** Interior photos, explicit HOA fees, and price history. | **Extreme Difficulty.** PerimeterX/Datadome class obfuscation. | **Screenshot & VLM.** Load page -> Screenshot "Property Details" & "Price History" sections -> Qwen-VL extract: `{PriceHistory: [], HOA: value, Description: text}`. |
| **Zillow** | `https://www.zillow.com` | **Rent Estimates & Auction Flags.** "Zestimates" and "Foreclosure / Auction" flags. | Aggressive IP blocking; Shadow DOM. | **Screenshot & VLM.** Screenshot the "Facts and Features" and "Zestimate" chart -> Qwen-VL extract. |
| **FEMA Flood Map** | `msc.fema.gov` | **Risk Factor.** Official Flood Zone designations (AE, X, VE). | Map-based UI is hard to scrape. | **Native API/GIS.** Better to use County GIS overlay. If manual: Screenshot map panel -> Qwen-VL read Zone Code. |
| **Tax Collector** | `https://www.hillstaxfl.gov/` | **Debt Check.** Real-time status of current year tax bills. | None. Open search. | **Native Scraping.** HTML is simple enough to parse directly, but VLM can be used as fallback if layout changes. |

## 3. Integration Logic (Pseudo-Code)

1.  **Ingest:** `PARCEL_SPREADSHEET.xls` is located in the `data/county` directory. THis provides address, folio, and owner
2.  **Filter:** Select target properties (e.g., "Single Family," "Value < $500k").
3.  **Visual Extraction Loop (For difficult sources):**
    * *For each target property:*
    * **Permits:** Playwright navigates to Accela -> Enters Address -> **Screenshot** -> Qwen-VL parses active permits.
    * **Market:** Playwright navigates to Realtor.com -> **Screenshot** -> Qwen-VL parses "Price History" and "Agent Remarks".
    * **Auctions:** Playwright navigates to Auction Calendar -> **Screenshot** -> Qwen-VL checks for date match.
4.  **Enrich (Legal):**
    * Query **HOVER** (Clerk) -> **Screenshot** docket -> Qwen-VL checks for "Lis Pendens".
5.  **Output:** Master Database for Agent Review.
