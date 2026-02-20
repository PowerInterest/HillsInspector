# Tax Data Research: Replacing Web Scraping with Bulk Data

**Date:** 2026-02-18
**Status:** Research complete -- implementation not yet started

---

## Executive Summary

The current pipeline (Step 12) scrapes `county-taxes.net/hillsborough` using Playwright to obtain tax payment status per property. This is slow (~30-60s per property), unreliable (vision extraction often returns null), and fragile (JavaScript SPA, bot detection, accessibility tree parsing). This document evaluates whether bulk data sources can replace or supplement web scraping.

**Key finding:** No single bulk source provides real-time tax payment status (paid/unpaid, amount due, delinquent status). However, a combination of HCPA bulk data (assessments + taxable values) plus the Florida DOR NAL file (detailed exemptions, classified use, taxable values by district) provides 90% of what the pipeline needs. The remaining 10% (current-year payment status, delinquent amount, tax certificates) is only available through the Tax Collector's individual-property web interface -- no bulk download exists.

**Recommendation:** Use bulk data for assessment/valuation enrichment (eliminates ~80% of scraping needs), and keep targeted scraping only for properties that need current payment status confirmation (delinquent/certificate checks for foreclosure risk analysis).

---

## 1. Data Sources Evaluated

### 1.1 HCPA Bulk Parcel Download (Already Loaded)

**URL:** `https://downloads.hcpafl.org/`
**Files:**
- `parcel_MM_DD_YYYY.zip` (91 MB) -- Shapefile with 530K parcels
- `HCparcel_4_public_MM_DD_YYYY.zip` (97 MB) -- Extended parcel shapefile
- `PARCEL_SPREADSHEET.xls` (536 MB) -- Excel with full parcel data
- `allsales_MM_DD_YYYY.zip` (67 MB) -- All sales history

**Format:** DBF (dBASE III+) inside zip, also XLS
**Update Frequency:** Bi-weekly (most recent: 02/13/2026)
**Authentication:** None (public download, ASP.NET postback for file selection)
**Already in PostgreSQL:** Yes -- `hcpa_bulk_parcels` table (530K rows)

**Fields available (47 fields in parcel.dbf):**

| Field | Type | Description | Tax-Relevant |
|-------|------|-------------|-------------|
| `FOLIO` | C(10) | 10-digit folio number | Join key |
| `STRAP` | C(22) | HCPA strap (matches pipeline parcel_id) | Join key |
| `JUST` | N(19,5) | Just (market) value | Yes -- basis for tax calculation |
| `ASD_VAL` | N(19,5) | Assessed value (after SOH cap) | Yes -- basis for tax bill |
| `TAX_VAL` | N(19,5) | Taxable value (after exemptions) | Yes -- basis for actual tax |
| `LAND` | N(19,5) | Land value component | Supplemental |
| `BLDG` | N(19,5) | Building value component | Supplemental |
| `EXF` | N(19,5) | Extra features value | Supplemental |
| `TAXDIST` | C(3) | Tax district code | Yes -- determines millage rate |
| `MUNI` | C(1) | Municipality flag | Yes -- affects taxing authorities |
| `SD1` | C(3) | Special district 1 | Yes -- additional assessments |
| `SD2` | C(3) | Special district 2 | Yes -- additional assessments |
| `TIF` | C(1) | Tax Increment Financing flag | Yes -- TIF district |
| `OWNER` | C(75) | Current owner name | Supplemental |
| `DOR_C` | C(4) | DOR land use code | Yes -- use classification |
| `S_DATE` | D(8) | Last sale date | Supplemental |
| `S_AMT` | N(19,5) | Last sale price | Supplemental |

**What this provides:**
- Assessed value, just value, taxable value for every parcel
- Tax district codes (can be used to compute approximate tax bill with millage rates)
- Property characteristics, owner, legal description

**What this does NOT provide:**
- Actual tax amount billed
- Payment status (paid/unpaid/delinquent)
- Tax certificate information
- Homestead exemption flag (not in the DBF -- would need NAL file)
- Detailed exemption breakdown
- Non-ad valorem assessments (fire, solid waste, etc.)

### 1.2 Florida DOR NAL File (Assessment Roll)

**URL:** `https://floridarevenue.com/property/dataportal/`
**File:** `Hillsborough 39 Final NAL 2025.zip` (in the Tax Roll Data Files/NAL/2025F directory)
**Format:** CSV (comma-delimited)
**Update Frequency:** Annual (Preliminary in July, Final after certification ~October-January)
**Authentication:** None (free public download)
**Currently Loaded:** No

**The NAL (Name-Address-Legal) file is the official Florida property tax assessment roll, submitted by each county's Property Appraiser to the Department of Revenue.** Hillsborough County is county code **39** (not 29).

**Key field groups (~200+ fields):**

| Field Group | Fields | Description |
|------------|--------|-------------|
| Parcel Identification | 1-4 | County code, parcel ID, STRAP, census tract |
| Stratification | 5-7 | DOR use code, property class |
| Use Information | 8-10 | Actual use, current use, effective year |
| **Parcel Values** | **11-17** | **Just value, assessed value (school & non-school), taxable value (school & non-school), deferred value, homestead flag** |
| **Classified Use Values** | **18-35** | **Just/assessed value breakdowns for agricultural, non-homestead, homestead, mixed use** |
| Sale Data | 36-55 | Last 3 sales: date, amount, qualification, deed type |
| Name/Address | 56-80 | Owner name, mailing address, site address |
| Legal Description | 81-95 | Full legal description (multiple lines) |
| Property Characteristics | 96-109 | Year built, beds, baths, sq ft, lot size, etc. |
| **Exemptions** | **110-153** | **44 exemption fields: homestead ($25K + additional $25K), widow/widower, disability, veteran, senior, institutional, government, conservation, historical, etc. Each with school/non-school applicability** |
| **Millage/Tax District** | **154+** | **Tax district codes, millage rates by authority** |

**What this provides (beyond HCPA bulk):**
- Detailed exemption breakdown (44 exemption types)
- Homestead exemption flag and amount
- Separate school vs. non-school taxable values
- Classified use value breakdowns (agricultural, etc.)
- Complete sale history (last 3 sales)
- Full legal description

**What this does NOT provide:**
- Actual tax payment status (paid/unpaid)
- Delinquent amount
- Tax certificates
- Non-ad valorem assessment amounts (these are billed separately)
- Current year payment date

**Important limitation:** The NAL file is the **assessment roll** (what you owe), not the **collection roll** (what you've paid). The Tax Collector maintains payment/delinquency data separately.

### 1.3 Hillsborough County Tax Collector (hillstaxfl.gov)

**URL:** `https://www.hillstaxfl.gov/records-search/`
**Search Portal:** `https://county-taxes.net/hillsborough/property-tax` (powered by Grant Street Group / GovHub)
**Format:** JavaScript SPA (no API, no bulk download, no CSV export)
**Authentication:** None for search; Cloudflare Turnstile on `hillsborough.county-taxes.com`
**Currently Used:** Yes -- Step 12 scrapes this via Playwright

**Data available per property (web only):**
- Account number (e.g., A0380975910)
- Owner name and situs address
- Amount due (current year)
- Payment status (paid in full / amount outstanding)
- Most recent payment date and amount
- Tax certificates (certificate number, face value, year)
- Account history (multi-year payment records)

**No bulk data access:**
- No API documented or discoverable
- No CSV/data export
- No bulk download
- County-taxes.net is a JavaScript SPA that requires full browser rendering
- The `hillsborough.county-taxes.com` version uses Cloudflare Turnstile (harder to scrape)
- The `county-taxes.net` version (used by current scraper) does not have Cloudflare but is still a JS SPA
- Contact email for data requests: `data@hillstaxfl.gov`

**Delinquent property list:**
- Published annually in May (newspaper advertisement, per Florida Statute Ch. 197)
- Available on LienHub (`lienhub.com/county/hillsborough`) during certificate sale season
- Not available as a downloadable file year-round

### 1.4 LienHub (Tax Certificate Auction Platform)

**URL:** `https://lienhub.com/county/hillsborough`
**Format:** Web interface with Cloudflare protection
**Authentication:** Registration required for bidding; browsing may require login
**Currently Used:** No (checked but Cloudflare blocks)

**Data available:**
- Delinquent property list (during certificate sale period, typically May)
- Tax certificate sale results
- Certificate holder information

**Limitations:**
- Cloudflare Turnstile protection
- Data only available during/after annual certificate sale
- No bulk download or API

### 1.5 Florida Statewide Parcels (FloridaGIO)

**URL:** `https://geodata.floridagio.gov/datasets/FGIO::florida-statewide-parcels/`
**Format:** CSV, KML, Shapefile, GeoJSON, File Geodatabase
**Records:** 10.8M parcels statewide
**Update Frequency:** Annual (from DOR July submission)
**Authentication:** None

**This is a GIS-oriented subset of the NAL data.** Fields include parcel ID, owner, address, valuations, land use, legal description, and sale info. It does NOT include exemption details or tax payment status.

**Verdict:** Redundant with HCPA bulk data already loaded. No additional tax-relevant fields.

### 1.6 Third-Party Data Providers

| Provider | Product | Price | Fields |
|----------|---------|-------|--------|
| TaxNetUSA | Bulk data + API | Quote-based | Assessment + collector data |
| MapWise | Parcels API | Subscription | Parcel + assessment data |
| Regrid | Parcel data | Subscription | Parcel + basic valuation |
| PropertyRadar | API | Subscription | Tax + sales + liens |

**Not evaluated further** -- we prefer free public data sources.

---

## 2. What We Already Have vs. What We Need

### Currently in PostgreSQL (`hcpa_bulk_parcels`)

| Field | Available | Source |
|-------|-----------|--------|
| Just value | Yes | HCPA bulk |
| Assessed value | Yes | HCPA bulk |
| Taxable value | Yes | HCPA bulk |
| Tax district | Yes | HCPA bulk |
| Owner name | Yes | HCPA bulk |
| Property address | Yes | HCPA bulk |
| Homestead exemption flag | **No** | Need NAL file |
| Exemption amount breakdown | **No** | Need NAL file |
| Millage rate | **No** | Need DOR millage data |
| Estimated tax bill | **No** | Computable from taxable_value + millage |
| Paid/unpaid status | **No** | Tax Collector only |
| Delinquent amount | **No** | Tax Collector only |
| Tax certificates | **No** | Tax Collector only |

### What the Pipeline Actually Uses Tax Data For

Looking at `src/orchestrator.py` Step 12 and `src/scrapers/tax_scraper.py`:

1. **Payment status** (`paid_in_full`) -- Is property current on taxes?
2. **Amount due** (`amount_due`) -- How much is owed?
3. **Tax certificates** (`certificates`) -- Are there outstanding tax liens?
4. **Account number** -- Cross-reference identifier
5. **Last payment** -- When was last payment made?

For foreclosure analysis, the critical questions are:
- **Are taxes delinquent?** (affects bid calculation -- buyer inherits tax liens)
- **Are there outstanding tax certificates?** (tax cert holders can force tax deed sale)
- **What is the annual tax burden?** (affects equity calculation)

---

## 3. Proposed Strategy: Hybrid Approach

### Phase 1: Compute Tax Estimates from Bulk Data (No Scraping Needed)

Using HCPA bulk data already in PostgreSQL, we can compute an estimated annual tax bill:

```
estimated_tax = taxable_value * combined_millage_rate
```

Hillsborough County 2025 combined millage rates (approximate):
- County: ~5.5 mills
- School: ~7.5 mills
- City of Tampa: ~6.5 mills (if applicable)
- Special districts: varies

**This gives us 80% of the tax picture** without any scraping:
- Estimated annual tax amount
- Whether property has homestead (if NAL loaded -- big assessment cap difference)
- Taxable value vs. market value gap (indicates exemptions)

### Phase 2: Load Florida DOR NAL File

Download and load the Hillsborough County NAL file into PostgreSQL for:
- Detailed exemption breakdown (44 types)
- Homestead flag
- Classified use values
- School vs. non-school taxable values

**Proposed schema addition:**

```sql
CREATE TABLE dor_nal_parcels (
    county_code       SMALLINT NOT NULL,     -- 39 for Hillsborough
    parcel_id         VARCHAR(32) NOT NULL,  -- County parcel identifier
    strap             VARCHAR(32),           -- HCPA strap format (if derivable)
    roll_year         SMALLINT NOT NULL,      -- Tax year (e.g. 2025)

    -- Values
    just_value              NUMERIC(18,2),
    assessed_value_school   NUMERIC(18,2),
    assessed_value_county   NUMERIC(18,2),
    taxable_value_school    NUMERIC(18,2),
    taxable_value_county    NUMERIC(18,2),

    -- Exemptions
    homestead_flag          BOOLEAN,
    homestead_value         NUMERIC(18,2),
    exemption_total         NUMERIC(18,2),
    -- (additional exemption columns as needed)

    -- Classification
    dor_use_code            VARCHAR(8),
    actual_year_built       SMALLINT,

    -- Metadata
    source_file_id   BIGINT REFERENCES ingest_files(id),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (parcel_id, roll_year)
);

CREATE INDEX idx_dor_nal_strap ON dor_nal_parcels (strap);
CREATE INDEX idx_dor_nal_year ON dor_nal_parcels (roll_year);
```

### Phase 3: Targeted Scraping for Payment Status

Keep Playwright scraping ONLY for properties where we need to know:
1. Is the tax currently paid or delinquent? (relevant for foreclosure bid calculation)
2. Are there outstanding tax certificates? (critical risk factor)

This reduces scraping from 145+ properties to only those in active foreclosure consideration (~20-30 at a time), saving 80%+ of scraping time.

### Phase 4: Annual Delinquent List (Optional)

During May each year, the Tax Collector publishes the delinquent property list. If we can capture this list (even as a one-time newspaper scan or LienHub data), it gives us a point-in-time delinquency snapshot for the entire county.

---

## 4. Comparison: Bulk Data vs. Current Scraping

| Capability | Current (Scraping) | Bulk Data (HCPA+NAL) | Notes |
|-----------|-------------------|---------------------|-------|
| Assessed value | Via screenshot/vision | Direct from DB | Bulk is faster & more reliable |
| Taxable value | Via screenshot/vision | Direct from DB | Bulk is faster & more reliable |
| Estimated annual tax | Not computed | Computable (taxable * millage) | New capability |
| Homestead status | Not captured | Via NAL file | New capability |
| Exemption details | Not captured | Via NAL file (44 types) | New capability |
| Tax district/millage | Not captured | Via HCPA bulk + DOR | New capability |
| **Payment status** | **Yes (unreliable)** | **No** | Still need targeted scraping |
| **Amount due** | **Yes (unreliable)** | **No** | Still need targeted scraping |
| **Tax certificates** | **Yes (unreliable)** | **No** | Still need targeted scraping |
| **Account number** | **Yes** | **No** | Low value -- just a cross-reference |
| Speed per property | 30-60 seconds | Instant (DB lookup) | 50-100x faster for bulk fields |
| Reliability | ~60% (vision failures) | 100% (structured data) | Major improvement |
| Coverage | 530K parcels | 530K parcels | Same |
| Update frequency | Real-time | Bi-weekly (HCPA) / Annual (NAL) | Bulk is stale for payments |

---

## 5. Implementation Roadmap

### Step 1: Compute Tax Estimates (Immediate, No New Data Needed)

- Add `estimated_annual_tax` to the pipeline's equity calculation
- Use: `taxable_value * 0.019` (approximate combined Hillsborough millage ~19 mills)
- Source: existing `hcpa_bulk_parcels.taxable_value` in PostgreSQL
- Effort: 1-2 hours

### Step 2: Download and Load NAL File

- Download `Hillsborough 39 Final NAL 2025.zip` from DOR data portal
- Parse CSV and load into `dor_nal_parcels` table
- Add to `pg_loader.py` as a new dataset type
- Effort: 4-8 hours

### Step 3: Refactor Step 12 (Tax Scraper)

- For all properties: look up tax data from bulk tables first
- Only scrape county-taxes.net for properties where payment status is critical
- Cache scraped payment status in SQLite `liens` table (already done)
- Skip scraping if: tax_status already known AND data is <30 days old
- Effort: 4-6 hours

### Step 4: Millage Rate Table (Optional Enhancement)

- Scrape or manually enter current millage rates by tax district
- Compute exact tax bill: `taxable_value * millage_rate / 1000`
- Effort: 2-4 hours

---

## 6. Data Source URLs and Downloads

| Source | URL | Format | Cost |
|--------|-----|--------|------|
| HCPA Bulk Parcels | `https://downloads.hcpafl.org/` | DBF in ZIP | Free |
| DOR NAL Files | `https://floridarevenue.com/property/dataportal/` | CSV in ZIP | Free |
| DOR NAL User Guide (2025) | [PDF](https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/User%20Guides/2025%20Users%20guide%20and%20quick%20reference/2025_NAL_SDF_NAP_Users_Guide.pdf) | PDF | Free |
| NAL Field Summary (2025) | [PDF](https://floridarevenue.com/property/Documents/2025NALSummaryTable.pdf) | PDF | Free |
| Tax Collector Records | `https://www.hillstaxfl.gov/records-search/` | Web only | Free |
| Tax Collector Data Requests | `data@hillstaxfl.gov` | Custom | Varies |
| LienHub (Certificates) | `https://lienhub.com/county/hillsborough` | Web only | Free (browse) |
| FL Statewide Parcels | `https://geodata.floridagio.gov/` | CSV/SHP/GeoJSON | Free |

---

## 7. Conclusion

The pipeline can eliminate ~80% of tax-related web scraping by leveraging free bulk data already available:

1. **HCPA bulk parcels** (already loaded): taxable value, assessed value, tax district
2. **Florida DOR NAL file** (needs loading): homestead flag, 44 exemption types, classified use values, school/non-school taxable values

The remaining 20% -- actual payment status, delinquent amounts, and tax certificate data -- is only available through the Tax Collector's web interface. This should be targeted scraping (only for properties under active analysis) rather than blanket scraping of all 145+ auction properties.

**Net effect:**
- Step 12 runtime drops from ~2 hours (145 properties x 30-60s each) to ~15 minutes (20-30 properties needing payment status)
- Data reliability improves from ~60% (vision extraction failures) to ~95% (bulk data + targeted scraping)
- New capabilities: estimated tax bill, homestead detection, exemption analysis
