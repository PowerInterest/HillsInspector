# Step 2: Final Judgment Extraction

## Overview
This step downloads the Final Judgment of Foreclosure PDF and extracts structured data using the `VisionService` (GLM-4.6v-flash). The Final Judgment is the authoritative source for the total debt amount, the foreclosure type, and the list of defendants whose liens will be wiped out.

## Source
- **URL**: `https://publicaccess.hillsclerk.com` (OnBase / PAVDirectSearch + ORI Public Access)
- **Method**: Playwright (Download) + Vision API (Extraction)

## Process Flow

1.  **Discovery (Primary — Instrument Search)**:
    - The Auction Scraper (Step 1) captures the "Case #" link from the auction site.
    - This link points to a PAVDirectSearch URL (CQID 320 - Instrument Search).
    - Example: `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=320&OBKey__1006_1=2025060477`
    - The `instrument_number` is extracted from the URL parameter `OBKey__1006_1`.

2.  **Discovery (Fallback — ORI Case Number Search)**:
    - ~30% of auction listings have **empty instrument numbers** (`OBKey__1006_1=` with no value).
    - In this case, `_search_judgment_by_case_number()` queries the ORI case search API:
      ```
      POST https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search
      Body: {"CaseNum": "292024CA003499A001HC"}
      ```
    - The API returns all recorded documents for the case (judgments, lis pendens, orders, etc.).
    - We filter for `(JUD) JUDGMENT` in `DocType` to find the Final Judgment.
    - The response includes the document `ID` (for download) and `PartiesOne`/`PartiesTwo` (plaintiff/defendant).
    - **Important**: The browser must first navigate to `publicaccess.hillsclerk.com/oripublicaccess/` before calling the API (same-origin CORS requirement).

3.  **Download**:
    - Using either the intercepted KeywordSearch response (primary) or the ORI search result (fallback), we obtain the internal **Document ID**.
    - The PDF is downloaded via: `.../PAVDirectSearch/api/Document/{encoded_doc_id}/?OverlayMode=View`.
    - The PDF is saved to `data/Foreclosure/{case_number}/documents/final_judgment_{instrument}.pdf`.

4.  **Backfill (Step 2 Pre-pass)**:
    - At the start of Step 2 in the orchestrator, cases without `step_pdf_downloaded` are identified.
    - For each, the ORI case search fallback is attempted to download the missing PDF.
    - This ensures cases scraped before the fallback was implemented still get their PDFs.

5.  **Extraction**:
    - `VisionService` converts the PDF pages to images.
    - All pages are sent in a single batch to the vision endpoint with the `FINAL_JUDGMENT_PROMPT`.
    - The LLM extracts amounts, parties, dates, and legal descriptions into JSON.

## Extracted Data

The following data is extracted and stored in the `auctions` table:

### Financials
- `total_judgment_amount`: The total debt owed to the plaintiff.
- `principal_amount`: The original unpaid principal balance.
- `interest_amount`: Accrued interest.
- `attorney_fees`, `court_costs`: Legal costs added to the judgment.

### Parties
- **Plaintiff**: The foreclosing entity (Bank, HOA, etc.).
- **Defendants**: List of all parties named in the lawsuit. Critical for determining which liens are extinguished.
- **Red Flags**: Detection of Federal Defendants (IRS, USA) which trigger extended redemption periods.

### Property & Procedural
- `foreclosure_type`: "FIRST MORTGAGE", "HOA", "TAX", etc.
- `lis_pendens_date`: The cutoff date for junior liens.
- `sale_date`: Scheduled date of the auction.
- `legal_description`: Verbatim text from the judgment.

## Technical Details

### OnBase Integration
We previously attempted to scrape the HOVER system (`hover.hillsclerk.com`), but it is protected by PerimeterX (returns 403 to headless browsers). The Auction site links directly to OnBase (PAVDirectSearch), bypassing the need for a general case search. We use "Instrument Search" (CQID 320) as the primary path.

When the instrument number is missing, we use the ORI Public Access case search, which is a separate Angular SPA at `/oripublicaccess/`. The underlying API (`/Public/ORIUtilities/DocumentSearch/api/Search`) accepts a JSON body with `CaseNum` and returns a `ResultList` array. Each result has:
- `Instrument` — the ORI instrument number
- `DocType` — e.g. `(JUD) JUDGMENT`, `(LP) LIS PENDENS`, `(ORD) ORDER`
- `ID` — URL-safe document ID for the PAV download API
- `PartiesOne` / `PartiesTwo` — plaintiff and defendant name arrays
- `PageCount`, `RecordDate`, `UUID`

### Coverage
As of 2026-02-08: 180/186 cases (96.8%) have Final Judgment PDFs. The remaining 6 cases have no judgment recorded in the ORI system yet (only Lis Pendens or Orders exist).

### Vision Prompt
The extraction uses `zai-org/glm-4.6v-flash` (local vLLM endpoint). The prompt is designed to be extremely precise, instructing the model to transcribe legal descriptions verbatim and capture every defendant. Cloud fallbacks (OpenAI gpt-4o, Gemini 2.0 flash) are available if the local endpoint is down.

See `src/services/vision_service.py` for the full prompt text.
