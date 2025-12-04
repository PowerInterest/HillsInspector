# Final Judgment PDF Extraction - Implementation Guide

## Overview
We've implemented automated extraction of structured data from Final Judgment PDFs using the existing `VisionService` (Qwen Vision LLM).

## What Data We Extract

### Critical Fields for Lien Analysis

The `FINAL_JUDGMENT_PROMPT` in `vision_service.py` extracts the following fields:

#### 1. **Case Information**
- `case_number`: Full court case number
- `judgment_date`: Date the judgment was entered
- `recording_date`: Date judgment was recorded

#### 2. **Parties**
- `plaintiff`: Name of foreclosing party (bank, HOA, etc.)
- `defendant`: Name(s) of property owner(s)

#### 3. **Property Details**
- `property_address`: Street address
- `legal_description`: Full legal description
- `parcel_id`: Parcel/Folio number

#### 4. **Financial Amounts** (Most Critical)
- `total_judgment_amount`: Total amount awarded
- `principal_amount`: Original loan/debt amount
- `interest_amount`: Accrued interest
- `attorney_fees`: Attorney fees
- `court_costs`: Court costs
- `original_mortgage_amount`: Original mortgage principal
- `monthly_payment`: Monthly payment amount

#### 5. **Foreclosure Details**
- `foreclosure_type`: "FIRST MORTGAGE" | "SECOND MORTGAGE" | "HOA" | "TAX" | "OTHER"
- `lis_pendens_date`: Date Lis Pendens was filed
- `foreclosure_sale_date`: Scheduled auction date
- `original_mortgage_date`: Date of original mortgage
- `default_date`: Date of default

## Why These Fields Matter

### For Lien Survival Analysis:

1. **`foreclosure_type`** → Determines which liens survive
   - First Mortgage foreclosure: Wipes out junior liens
   - HOA foreclosure: First mortgage survives (senior position)

2. **`original_mortgage_amount`** → Required for HOA Safe Harbor calculation
   - Safe Harbor = min(12 months × HOA dues, 1% × original mortgage)

3. **`lis_pendens_date`** → Critical cutoff date
   - Liens recorded AFTER Lis Pendens are wiped out
   - Liens recorded BEFORE may survive (if senior)

4. **`plaintiff`** → Identifies foreclosing party
   - Regex check for "HOA", "Association", "Bank", etc.
   - Determines foreclosure type if not explicitly stated

5. **`total_judgment_amount`** → Used in equity calculation
   - True Equity = Assessed Value - Judgment - Surviving Liens

## Implementation Status

### ✅ Completed:
1. **`vision_service.py`** - Added `FINAL_JUDGMENT_PROMPT` and `extract_final_judgment()` method
2. **`final_judgment_processor.py`** - Service to convert PDFs to images and extract data
3. **Test Script** - `test_final_judgment_extraction.py` to demonstrate extraction

### ⚠️ Dependency Required:
**Poppler** must be installed for PDF-to-image conversion:

#### Windows Installation:
```powershell
# Option 1: Using Chocolatey
choco install poppler

# Option 2: Manual Download
# Download from: https://github.com/oschwartz10612/poppler-windows/releases
# Extract to C:\Program Files\poppler
# Add C:\Program Files\poppler\Library\bin to PATH
```

#### Linux/Mac:
```bash
# Ubuntu/Debian
sudo apt-get install poppler-utils

# Mac
brew install poppler
```

### 📝 Next Steps:

1. **Install Poppler** (see above)

2. **Test Extraction**:
   ```bash
   uv run python test_final_judgment_extraction.py
   ```

3. **Integrate into Pipeline**:
   - Modify `auction_scraper.py` to call `FinalJudgmentProcessor` after downloading PDF
   - Store extracted data in database (new columns in `auctions` table)

4. **Database Schema Update**:
   ```sql
   ALTER TABLE auctions ADD COLUMN plaintiff TEXT;
   ALTER TABLE auctions ADD COLUMN foreclosure_type TEXT;
   ALTER TABLE auctions ADD COLUMN lis_pendens_date DATE;
   ALTER TABLE auctions ADD COLUMN original_mortgage_amount REAL;
   ALTER TABLE auctions ADD COLUMN extracted_data JSON; -- Store full extraction
   ```

5. **Lien Analysis Integration**:
   - Use `foreclosure_type` in `LienSurvivalAnalyzer.does_lien_survive()`
   - Use `original_mortgage_amount` for HOA Safe Harbor calculation
   - Use `lis_pendens_date` as cutoff for junior liens

## Example Usage

```python
from src.services.final_judgment_processor import FinalJudgmentProcessor

processor = FinalJudgmentProcessor()

# Process a PDF
pdf_path = "data/pdfs/final_judgments/292012CA015084A001HC_final_judgment.pdf"
case_number = "292012CA015084A001HC"

result = processor.process_pdf(pdf_path, case_number)

if result:
    # Get cleaned amounts
    amounts = processor.extract_key_amounts(result)
    
    # Use in lien analysis
    foreclosure_type = result.get('foreclosure_type')
    original_mtg_amount = amounts.get('original_mortgage_amount')
    lis_pendens_date = result.get('lis_pendens_date')
    
    # Calculate HOA Safe Harbor if needed
    if foreclosure_type == "FIRST MORTGAGE" and original_mtg_amount:
        safe_harbor = min(12 * monthly_hoa_dues, 0.01 * original_mtg_amount)
```

## Prompt Engineering Notes

The `FINAL_JUDGMENT_PROMPT` is designed to:
- Extract structured JSON data
- Handle variations in document format
- Identify foreclosure type from context
- Parse dollar amounts with various formats ($123,456.78 or 123456.78)
- Extract dates in MM/DD/YYYY format

If extraction quality is poor, you can:
1. Increase DPI in `final_judgment_processor.py` (currently 200)
2. Process more pages (currently first 3 pages)
3. Refine the prompt with more specific instructions
4. Add examples to the prompt (few-shot learning)

## Cost/Performance

- **Vision API**: ~2-3 seconds per page
- **PDF Conversion**: ~1 second per page
- **Total**: ~10-15 seconds per Final Judgment PDF
- **Batch Processing**: Can process 5 PDFs in ~1 minute

For 100 auction properties:
- Download PDFs: ~5 minutes
- Extract data: ~15 minutes
- **Total**: ~20 minutes end-to-end
