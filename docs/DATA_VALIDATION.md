# Data Validation & Quality Control Guide

## Purpose
This document establishes the validation framework for verifying scraped data quality. **The scrapers frequently fail silently** - they return success but capture incomplete or wrong data. This guide defines how to detect and diagnose these failures.

---

## 1. Data Quality Dashboard Queries

Run these queries against `property_master.db` after each scrape to identify gaps.

### 1.1 Auction Coverage Check
```sql
-- How many auctions have been scraped vs enriched?
SELECT
    COUNT(*) as total_auctions,
    COUNT(CASE WHEN folio IS NOT NULL THEN 1 END) as has_folio,
    COUNT(CASE WHEN final_judgment_amount IS NOT NULL THEN 1 END) as has_judgment,
    COUNT(CASE WHEN opening_bid IS NOT NULL THEN 1 END) as has_opening_bid,
    COUNT(CASE WHEN final_judgment_content IS NOT NULL THEN 1 END) as has_ocr_text
FROM auctions;
```

**Expected:** All counts should be equal (100% coverage). If `has_folio` < `total_auctions`, the auction scraper is not extracting parcel IDs.

### 1.2 Enrichment Coverage Check
```sql
-- Are parcels being created for all auctions?
SELECT
    a.auction_type,
    COUNT(DISTINCT a.folio) as auction_folios,
    COUNT(DISTINCT p.folio) as enriched_parcels,
    ROUND(100.0 * COUNT(DISTINCT p.folio) / NULLIF(COUNT(DISTINCT a.folio), 0), 1) as pct_enriched
FROM auctions a
LEFT JOIN parcels p ON a.folio = p.folio
GROUP BY a.auction_type;
```

**Expected:** `pct_enriched` should be 100%. If < 100%, the enrichment loop is skipping properties.

### 1.3 Lien Coverage Check
```sql
-- Do auctions have associated liens?
SELECT
    a.case_number,
    a.property_address,
    COUNT(l.id) as lien_count,
    SUM(CASE WHEN l.amount IS NOT NULL THEN 1 ELSE 0 END) as liens_with_amount
FROM auctions a
LEFT JOIN liens l ON a.case_number = l.case_number
GROUP BY a.case_number, a.property_address
ORDER BY lien_count ASC
LIMIT 20;
```

**Expected:** Most foreclosures should have at least 1 lien (the mortgage being foreclosed). Zero liens = scraper failure.

### 1.4 Field Completeness Matrix
```sql
-- Which fields are populated across all parcels?
SELECT
    COUNT(*) as total,
    COUNT(owner_name) as has_owner,
    COUNT(year_built) as has_year_built,
    COUNT(beds) as has_beds,
    COUNT(heated_area) as has_sqft,
    COUNT(market_value) as has_market_val,
    COUNT(image_url) as has_image
FROM parcels;
```

---

## 2. Validation Rules (Sanity Checks)

### 2.1 Auction Data Validation
| Field | Validation Rule | Failure Action |
|-------|-----------------|----------------|
| `case_number` | Must match pattern `^\d{2}20\d{2}CA\d+` (foreclosure) or similar | Flag as invalid |
| `folio` | Must be 20+ characters alphanumeric | Re-scrape from auction detail page |
| `final_judgment_amount` | Must be > $1,000 and < $10,000,000 | Manual review |
| `auction_date` | Must be in the future | Skip (auction already passed) |
| `property_address` | Must contain at least one number and one letter | Flag for manual fix |

### 2.2 Parcel Data Validation
| Field | Validation Rule | Failure Action |
|-------|-----------------|----------------|
| `owner_name` | Not NULL, not "UNKNOWN", length > 3 | Re-scrape from HCPA |
| `year_built` | Between 1800 and current year | Flag as suspicious |
| `beds` | Between 0 and 20 | Flag as suspicious |
| `assessed_value` | > $10,000 | Flag as suspicious |
| `heated_area` | > 100 sqft if beds > 0 | Flag mismatch |

### 2.3 Lien Data Validation
| Field | Validation Rule | Failure Action |
|-------|-----------------|----------------|
| `document_type` | Must be in allowed list (MORTGAGE, LIEN, LIS_PENDENS, etc.) | Flag unknown type |
| `recording_date` | Must be < today and > 1950 | Flag as suspicious |
| `amount` | If present, must be > $0 | OCR extraction failed |
| `instrument_number` | If in logs but not DB, mapping error | Fix save_liens() |

---

## 3. Known Failure Modes

### 3.1 Silent Failures (Scraper Returns OK but Data Missing)

| Symptom | Root Cause | Diagnosis |
|---------|------------|-----------|
| `final_judgment_content` always NULL | PDF not downloading OR OCR not running OR save not committing | Check if PDFs exist in `/data/properties/{folio}/documents/` |
| `opening_bid` always NULL | Field only appears close to auction date OR selector changed | Manual check of auction site for recent auctions |
| Only 1 parcel for 63 auctions | `upsert_parcel()` only called during "deep analysis" | Review enrichment loop logic |
| Lien `amount` always NULL | OCR running but regex not matching dollar amounts | Check OCR output in logs |
| `instrument_number` in logs but not DB | Field name mismatch in Lien model vs DB insert | Compare model fields to INSERT statement |

### 3.2 Blocked/Timeout Failures

**IMPORTANT: No Retry Policy**
All scraping failures should be logged with loguru and NOT retried automatically. This prevents:
- IP bans from aggressive retrying
- Wasted resources on blocked sites
- Masking of systematic failures

| Source | Symptom | Mitigation |
|--------|---------|------------|
| HOVER (Court Records) | PerimeterX block, no data returned | Use ORI scraper as alternative for lien research |
| Zillow/Realtor | Timeout or CAPTCHA | **No retry.** Log failure, use HCPA assessed value as fallback |
| HCPA | Occasional timeouts | **No retry.** Log failure, flag for manual review |
| ORI | Occasional blocks | **No retry.** Log failure, property marked as `needs_lien_research` |

**Logging Protocol:**
```python
from loguru import logger

# Log all scraper failures with context
logger.error(
    "Scraper failed",
    source="zillow",
    folio="123456",
    error_type="timeout",
    url="https://...",
    extra={"response_code": 403}
)
```

---

## 4. Manual Verification Checklist

For each scraped property, validate against source:

### 4.1 Auction Verification (vs RealForeclose)
- [ ] Case number matches exactly
- [ ] Parcel ID / Folio matches exactly
- [ ] Judgment amount matches (within $1)
- [ ] Auction date matches
- [ ] Address matches (street name and number)

### 4.2 Parcel Verification (vs HCPA Website)
- [ ] Owner name matches current owner
- [ ] Year built matches
- [ ] Beds/Baths match
- [ ] Assessed value within 10% of site
- [ ] Address matches normalized format

### 4.3 Lien Verification (vs Official Records)
- [ ] All mortgages on property are captured
- [ ] Recording dates match
- [ ] Lien amounts match (if parsed)
- [ ] Lis Pendens date captured for priority calc

---

## 5. Automated Validation Script

Create a validation script that runs after each ingestion:

```python
# src/validation/data_quality.py

def run_validation_report(db_path: str) -> dict:
    """Generate data quality report after scraping."""

    checks = {
        "auction_coverage": check_auction_coverage(),
        "enrichment_rate": check_enrichment_rate(),
        "lien_coverage": check_lien_coverage(),
        "field_completeness": check_field_completeness(),
        "sanity_failures": check_sanity_rules(),
    }

    # Flag critical issues
    if checks["enrichment_rate"] < 50:
        logger.error("CRITICAL: Less than 50% of auctions enriched")

    if checks["field_completeness"]["final_judgment_content"] == 0:
        logger.error("CRITICAL: No OCR content captured - PDF pipeline broken")

    return checks
```

---

