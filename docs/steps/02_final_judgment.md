# Step 2: Final Judgment Extraction

## Overview
This step downloads the Final Judgment of Foreclosure PDF and extracts structured data using the `VisionService` (Qwen-VL). The Final Judgment is the authoritative source for the total debt amount, the foreclosure type, and the list of defendants whose liens will be wiped out.

## Source
- **URL**: `https://publicaccess.hillsclerk.com` (OnBase / PAVDirectSearch)
- **Method**: Playwright (Download) + Vision API (Extraction)

## Process Flow

1.  **Discovery**:
    - The Auction Scraper (Step 1) captures the "Case #" link from the auction site.
    - This link points to a PAVDirectSearch URL (CQID 320 - Instrument Search).
    - Example: `https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=320&OBKey__1006_1=2025060477`

2.  **Download**:
    - `FinalJudgmentProcessor` navigates to the PAV URL.
    - It intercepts the `KeywordSearch` API response to find the internal **Document ID**.
    - It constructs a direct download URL: `.../api/Document/{DOC_ID}/?OverlayMode=View`.
    - The PDF is downloaded to `data/properties/{folio}/documents/final_judgment_{instrument}.pdf`.

3.  **Extraction**:
    - `VisionService` converts the PDF pages to images.
    - It sends the images to the vLLM server with the `FINAL_JUDGMENT_PROMPT`.
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
We previously attempted to scrape the HOVER system (`hover.hillsclerk.com`), but it is protected by PerimeterX. We found that the Auction site links directly to OnBase (PAVDirectSearch), bypassing the need for a general case search. We use "Instrument Search" (CQID 320) to find the specific Final Judgment document.

### Vision Prompt
The extraction uses `Qwen/Qwen3-VL-8B-Instruct`. The prompt is designed to be extremely precise, instructing the model to transcribe legal descriptions verbatim and capture every defendant.

See `src/services/vision_service.py` for the full prompt text.
