# Full Pipeline Integration Plan
## Bringing It All Together - End-to-End Property Analysis

**Date**: 2025-11-27  
**Objective**: Integrate Final Judgment extraction into the complete property analysis pipeline

---

## Current State Assessment

### ✅ What We Have Working:

1. **Auction Scrapers**
   - `src/scrapers/auction_scraper.py` - RealForeclose scraper
   - `src/scrapers/tax_deed_scraper.py` - RealTaxDeed scraper
   - Downloads Final Judgment PDFs to `data/pdfs/final_judgments/`

2. **Property Data Collection**
   - `src/scrapers/hcpa_scraper.py` - Property Appraiser data
   - `src/scrapers/ori_scraper.py` - Official Records (liens, deeds, mortgages)
   - `src/scrapers/hover_scraper.py` - Clerk of Court documents

3. **Final Judgment Extraction** ✅ NEW!
   - `src/services/final_judgment_processor.py` - PDF to structured data
   - `src/services/vision_service.py` - Qwen Vision LLM integration
   - Extracts: amounts, foreclosure type, dates, parties, raw OCR text

4. **Lien Analysis**
   - `src/services/lien_survival_analyzer.py` - Determines which liens survive
   - HOA Safe Harbor calculation
   - Priority-based lien ordering

5. **Database**
   - DuckDB schema with auctions, properties, liens, documents tables
   - `setup_db.py` - Database initialization

---

## Integration Plan - 4 Steps

### Step 1: Update Database Schema ⚙️

**Add columns to `auctions` table for extracted Final Judgment data:**

```sql
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS plaintiff TEXT;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS defendant TEXT;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS foreclosure_type TEXT;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS judgment_date DATE;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS lis_pendens_date DATE;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS foreclosure_sale_date DATE;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS total_judgment_amount REAL;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS principal_amount REAL;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS interest_amount REAL;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS attorney_fees REAL;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS court_costs REAL;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS original_mortgage_amount REAL;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS original_mortgage_date DATE;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS extracted_judgment_data JSON;
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS raw_judgment_text TEXT;
```

**File to create**: `scripts/update_schema_for_judgments.py`

---

### Step 2: Integrate Extraction into Auction Scraper 🔗

**Modify `src/scrapers/auction_scraper.py`:**

```python
from src.services.final_judgment_processor import FinalJudgmentProcessor

class AuctionScraper:
    def __init__(self):
        # ... existing code ...
        self.judgment_processor = FinalJudgmentProcessor()
    
    async def scrape_auction(self, auction_url):
        # ... existing scraping code ...
        
        # After downloading Final Judgment PDF:
        if final_judgment_path:
            logger.info(f"Extracting data from Final Judgment: {case_number}")
            extracted_data = self.judgment_processor.process_pdf(
                final_judgment_path, 
                case_number
            )
            
            if extracted_data:
                # Store in database
                await self._store_judgment_data(case_number, extracted_data)
            else:
                logger.warning(f"Failed to extract data from {final_judgment_path}")
```

**Key Changes:**
- Import `FinalJudgmentProcessor`
- Call `process_pdf()` after downloading each Final Judgment
- Store extracted data in new database columns
- Log extraction success/failure

---

### Step 3: Enhanced Lien Survival Analysis 🎯

**Update `src/services/lien_survival_analyzer.py` to use extracted data:**

```python
class LienSurvivalAnalyzer:
    def analyze_property(self, parcel_id: str) -> Dict[str, Any]:
        # Get auction data with extracted judgment info
        auction = self.db.get_auction_by_parcel(parcel_id)
        
        # Use extracted foreclosure_type instead of guessing
        foreclosure_type = auction.get('foreclosure_type', 'UNKNOWN')
        
        if foreclosure_type == 'FIRST MORTGAGE':
            # First mortgage foreclosure wipes out junior liens
            surviving_liens = self._filter_senior_liens(all_liens, auction)
        
        elif foreclosure_type == 'HOA':
            # HOA foreclosure - first mortgage survives
            # Calculate Safe Harbor using extracted original_mortgage_amount
            original_mtg_amt = auction.get('original_mortgage_amount')
            if original_mtg_amt:
                safe_harbor = self._calculate_hoa_safe_harbor(
                    original_mtg_amt, 
                    monthly_hoa_dues
                )
        
        # Use lis_pendens_date as cutoff for junior liens
        lis_pendens_date = auction.get('lis_pendens_date')
        if lis_pendens_date:
            surviving_liens = [
                lien for lien in surviving_liens 
                if lien.recording_date < lis_pendens_date
            ]
        
        return {
            'foreclosure_type': foreclosure_type,
            'total_judgment': auction.get('total_judgment_amount'),
            'surviving_liens': surviving_liens,
            'estimated_equity': self._calculate_equity(auction, surviving_liens)
        }
```

**Benefits:**
- No more guessing foreclosure type from plaintiff name
- Accurate HOA Safe Harbor calculation using actual mortgage amount
- Precise lien cutoff date using Lis Pendens date
- Better equity calculations

---

### Step 4: Create Full Pipeline Runner 🚀

**New file**: `run_full_pipeline.py`

```python
"""
Full end-to-end pipeline for property analysis.

Steps:
1. Scrape auctions (foreclosure + tax deed)
2. Download Final Judgment PDFs
3. Extract structured data from PDFs
4. Scrape property data (HCPA, ORI, HOVER)
5. Analyze lien survival
6. Calculate net equity
7. Generate reports
"""

import asyncio
from loguru import logger
from src.scrapers.auction_scraper import AuctionScraper
from src.scrapers.tax_deed_scraper import TaxDeedScraper
from src.scrapers.hcpa_scraper import HCPAScraper
from src.scrapers.ori_scraper import ORIScraper
from src.services.final_judgment_processor import FinalJudgmentProcessor
from src.services.lien_survival_analyzer import LienSurvivalAnalyzer
from src.database.db_manager import DatabaseManager

async def run_full_pipeline(
    max_auctions: int = 10,
    skip_existing: bool = True
):
    """Run the complete property analysis pipeline."""
    
    logger.info("=" * 60)
    logger.info("STARTING FULL PIPELINE")
    logger.info("=" * 60)
    
    db = DatabaseManager()
    
    # Step 1: Scrape Auctions
    logger.info("\n[1/7] Scraping Foreclosure Auctions...")
    foreclosure_scraper = AuctionScraper()
    foreclosure_count = await foreclosure_scraper.scrape_all(
        max_pages=max_auctions // 10
    )
    logger.success(f"✓ Scraped {foreclosure_count} foreclosure auctions")
    
    logger.info("\n[2/7] Scraping Tax Deed Auctions...")
    tax_deed_scraper = TaxDeedScraper()
    tax_deed_count = await tax_deed_scraper.scrape_all(
        max_pages=max_auctions // 10
    )
    logger.success(f"✓ Scraped {tax_deed_count} tax deed auctions")
    
    # Step 2: Extract Final Judgments
    logger.info("\n[3/7] Extracting Final Judgment Data...")
    judgment_processor = FinalJudgmentProcessor()
    
    # Get auctions without extracted data
    auctions_to_process = db.get_auctions_without_judgment_data()
    
    extracted_count = 0
    for auction in auctions_to_process:
        pdf_path = auction.get('final_judgment_path')
        if pdf_path and Path(pdf_path).exists():
            result = judgment_processor.process_pdf(
                pdf_path, 
                auction['case_number']
            )
            if result:
                db.update_auction_judgment_data(auction['id'], result)
                extracted_count += 1
    
    logger.success(f"✓ Extracted data from {extracted_count} Final Judgments")
    
    # Step 3: Enrich Property Data
    logger.info("\n[4/7] Enriching Property Data (HCPA)...")
    hcpa_scraper = HCPAScraper()
    
    parcels_to_enrich = db.get_parcels_without_hcpa_data()
    enriched_count = 0
    
    for parcel in parcels_to_enrich[:max_auctions]:
        property_data = await hcpa_scraper.scrape_property(parcel['parcel_id'])
        if property_data:
            db.update_property_data(parcel['parcel_id'], property_data)
            enriched_count += 1
    
    logger.success(f"✓ Enriched {enriched_count} properties with HCPA data")
    
    # Step 4: Scrape Official Records (Liens)
    logger.info("\n[5/7] Scraping Official Records (Liens, Deeds, Mortgages)...")
    ori_scraper = ORIScraper()
    
    parcels_for_ori = db.get_parcels_without_ori_data()
    ori_count = 0
    
    for parcel in parcels_for_ori[:max_auctions]:
        documents = await ori_scraper.scrape_parcel(parcel['parcel_id'])
        if documents:
            db.store_ori_documents(parcel['parcel_id'], documents)
            ori_count += 1
    
    logger.success(f"✓ Scraped ORI data for {ori_count} properties")
    
    # Step 5: Analyze Lien Survival
    logger.info("\n[6/7] Analyzing Lien Survival...")
    analyzer = LienSurvivalAnalyzer(db)
    
    analyzed_count = 0
    for auction in db.get_auctions_with_complete_data():
        analysis = analyzer.analyze_property(auction['parcel_id'])
        db.store_lien_analysis(auction['id'], analysis)
        analyzed_count += 1
    
    logger.success(f"✓ Analyzed {analyzed_count} properties for lien survival")
    
    # Step 6: Generate Summary Report
    logger.info("\n[7/7] Generating Summary Report...")
    
    summary = db.get_pipeline_summary()
    
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE - SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total Auctions: {summary['total_auctions']}")
    logger.info(f"With Final Judgment Data: {summary['with_judgment_data']}")
    logger.info(f"With Property Data: {summary['with_property_data']}")
    logger.info(f"With Lien Analysis: {summary['with_lien_analysis']}")
    logger.info(f"\nHigh Equity Opportunities: {summary['high_equity_count']}")
    logger.info(f"High Risk (HOA/Surviving Liens): {summary['high_risk_count']}")
    logger.info("=" * 60)
    
    return summary

if __name__ == "__main__":
    asyncio.run(run_full_pipeline(max_auctions=10))
```

---

## Execution Order

### Option A: Full Fresh Run (Recommended for Testing)
```bash
# 1. Reset database
uv run python setup_db.py --reset

# 2. Run full pipeline
uv run python run_full_pipeline.py

# 3. View results
uv run python view_db_data.py
```

### Option B: Incremental Run (For Production)
```bash
# 1. Update schema (one-time)
uv run python scripts/update_schema_for_judgments.py

# 2. Extract from existing PDFs
uv run python scripts/batch_extract_judgments.py

# 3. Run analysis on updated data
uv run python analyze_property.py --all

# 4. View results
uv run python view_db_data.py
```

---

## Files to Create/Modify

### New Files:
1. ✅ `src/services/final_judgment_processor.py` (DONE)
2. ✅ `test_final_judgment_extraction.py` (DONE)
3. ⚙️ `scripts/update_schema_for_judgments.py` (TODO)
4. ⚙️ `scripts/batch_extract_judgments.py` (TODO)
5. ⚙️ `run_full_pipeline.py` (TODO)

### Files to Modify:
1. ⚙️ `src/scrapers/auction_scraper.py` - Add judgment extraction
2. ⚙️ `src/services/lien_survival_analyzer.py` - Use extracted data
3. ⚙️ `setup_db.py` - Add new columns to schema
4. ⚙️ `view_db_data.py` - Display judgment data

---

## Success Metrics

After running the full pipeline, we should have:

- ✅ **100% of auctions** with Final Judgment PDFs downloaded
- ✅ **90%+ extraction success rate** from PDFs
- ✅ **Accurate foreclosure type** identification (no more guessing)
- ✅ **Precise lien survival** calculations using Lis Pendens dates
- ✅ **HOA Safe Harbor** calculated with actual mortgage amounts
- ✅ **Net equity** calculated for each property
- ✅ **Risk flags** (HOA foreclosure, surviving liens, etc.)

---

## Next Steps - Priority Order

1. **IMMEDIATE**: Create `scripts/update_schema_for_judgments.py`
2. **IMMEDIATE**: Create `scripts/batch_extract_judgments.py` to process existing PDFs
3. **HIGH**: Modify `auction_scraper.py` to integrate extraction
4. **HIGH**: Update `lien_survival_analyzer.py` to use extracted data
5. **MEDIUM**: Create `run_full_pipeline.py` for end-to-end execution
6. **MEDIUM**: Update `view_db_data.py` to show judgment data
7. **LOW**: Create dashboard/web interface (Phase 5 from implementation plan)

---

## Estimated Timeline

- **Schema Update**: 15 minutes
- **Batch Extraction Script**: 30 minutes
- **Auction Scraper Integration**: 1 hour
- **Lien Analyzer Updates**: 1 hour
- **Full Pipeline Script**: 1.5 hours
- **Testing & Debugging**: 2 hours

**Total**: ~6 hours for complete integration

---

## Risk Mitigation

1. **Vision API Downtime**: Cache extracted data, implement retry logic
2. **PDF Format Changes**: Log extraction failures, manual review process
3. **Database Errors**: Transaction rollback, data validation
4. **Scraper Blocks**: Rotate user agents, implement delays
5. **Missing Data**: Graceful degradation, flag incomplete records

---

## Testing Strategy

1. **Unit Tests**: Test judgment extraction on known PDFs
2. **Integration Tests**: Run pipeline on 5 sample properties
3. **Validation**: Compare extracted amounts with manual PDF review
4. **Edge Cases**: Test HOA foreclosures, tax liens, missing data
5. **Performance**: Measure processing time per property

---

## Questions to Answer

- [ ] Should we re-extract existing PDFs or only new ones?
- [ ] How to handle extraction failures? (retry, manual review, skip?)
- [ ] Store raw JSON in database or just key fields?
- [ ] What's the acceptable extraction success rate? (90%? 95%?)
- [ ] Should we validate extracted amounts against opening bids?

---

**Ready to proceed?** Let's start with Step 1: Schema Update!
