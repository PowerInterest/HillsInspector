# OnBase Deep Search & Court Case Integration

## Findings
1.  **Court Case Search (CQID 324-348):**
    *   We extensively scanned CQIDs 0-500 and analyzed inputs.
    *   CQIDs 324-348 appear to be related to "Supplier" / "Finance" records, not Court Cases.
    *   We **failed** to identify a direct public CQID for "Court Case" or "Case Number" search that mimics the HOVER functionality.
    *   The HOVER site itself is protected by PerimeterX and does not expose its CQID easily.

2.  **Auction Site Integration (The Breakthrough):**
    *   The user's tip was correct: The Auction website (`hillsborough.realforeclose.com`) links directly to documents.
    *   Crucially, the "Case #" link on the Auction site redirects to an **OnBase Instrument Search (CQID 320)**.
    *   Example Link: `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=320&OBKey__1006_1=2025060477`
    *   `CQID 320` is "Instrument #" search.
    *   `OBKey__1006_1` corresponds to the **Instrument Number**.

3.  **Final Judgment Retrieval:**
    *   The Auction site provides the **Instrument Number** (e.g., `2025060477`) implicitly via the link.
    *   By following this link to OnBase (CQID 320), we land on a search result page.
    *   The search result contains a hidden **Document ID** (e.g., `AfcYeO1Fasr5...`).
    *   This Document ID can be used to construct a direct download URL:
        `https://publicaccess.hillsclerk.com/PAVDirectSearch/api/Document/{DOC_ID}/?OverlayMode=View`
    *   We successfully downloaded a valid "Final Judgment" PDF using this method.

## Strategy Update
1.  **Abandon `CourtCaseScraper` (for now):** Since we cannot find a direct "Case Number" search CQID, and we have a working path via the Auction site, we will deprioritize the standalone `CourtCaseScraper`.
2.  **Enhance `AuctionScraper`:**
    *   The `AuctionScraper` should be updated to:
        *   Extract the **Instrument Number** (or the full OnBase link) from the "Case #" column.
        *   Navigate to the OnBase CQID 320 page.
        *   Intercept the `KeywordSearch` API response to get the **Document ID**.
        *   Download the PDF using the constructed API URL.
    *   This effectively solves the "Final Judgment PDF" requirement.

## Next Steps
1.  Modify `src/scrapers/auction_scraper.py` to implement the PDF retrieval logic.
2.  Update `docs/Lien_research.md` to reflect this new finding (Auction -> Instrument Search -> PDF).
3.  Clean up the investigation scripts.
